--- Message pool: the transport between pregel instances.
--
-- A pool holds one bucket per worker. Every message is addressed to a vertex
-- name, and guava_name() picks the bucket that owns it, so the sharding is a
-- pure function of the name and every instance agrees on it without talking.
--
-- Buckets batch: put() accumulates and flush() sends the batch in one RPC.
-- Two flavours:
--
--   * instant -- the batch lives in memory and a background fiber pushes it as
--     soon as there is anything to push;
--   * delayed -- the batch lives in a space, so a preload that produces more
--     messages than fit in memory can still be sent (flushed explicitly).
--
-- RPC goes through conn:call() against the global registry that worker.lua and
-- master.lua populate (_G.pregel.worker.*), not through conn:eval(): eval needs
-- a universe execute grant, while a call needs only the per-function lua_call
-- grant that pregel.worker.grant() hands out.

local fun    = require('fun')
local log    = require('log')
local json   = require('json')
local uri    = require('uri')
local clock  = require('clock')
local fiber  = require('fiber')
local digest = require('digest')
local remote = require('net.box')

local table_new = require('table.new')

local strict      = require('pregel.utils.strict')
local utils       = require('pregel.utils')
local xpcall_tb   = utils.xpcall_tb
local is_callable = utils.is_callable
local error       = utils.error

-- How long net.box waits before retrying a failed connection. Short on
-- purpose: this is the interval of the *first* retry too, and the first
-- attempt of a cluster start regularly fails with "Instance bootstrap hasn't
-- finished yet" -- with the 5 s this used to be, every instance paid five
-- seconds for a peer that was ready in a few hundred milliseconds.
local RECONNECT_AFTER  = 0.1
local CONNECT_TIMEOUT  = 30
-- One step of the wait_connected() poll. The wait is a poll rather than a
-- single net.box wait so that a failure no retry can fix -- a rejected
-- authentication -- is noticed while it happens instead of after the whole
-- connect timeout.
local CONNECT_POLL     = 0.1
-- How long a producer blocked on a full bucket sleeps before re-checking. The
-- flush broadcasts, so this is only a backstop against a lost wakeup.
local FULL_POLL        = 0.1
-- How long send_wait waits for one bucket's answer before checking that the
-- fiber that owes it is still alive. A superstep can legitimately take very
-- much longer than this: the check is about liveness, not about a deadline.
local HANDLER_POLL     = 1

local WORKER_DELIVER       = 'pregel.worker.deliver'
local WORKER_DELIVER_BATCH = 'pregel.worker.deliver_batch'

local function bench_monotonic(func, ...)
    local start_time = clock.monotonic()
    local res = {0, func(...)}
    res[1] = clock.monotonic() - start_time
    return res
end

--- A URI with any credentials removed, safe to log.
local function safe_uri(u)
    if type(u) ~= 'string' then
        return tostring(u)
    end
    local ok, parsed = pcall(uri.parse, u)
    if not ok or parsed == nil or parsed.login == nil then
        return u
    end
    -- uri.format would put the login back; rebuild the authority by hand.
    return u:gsub('^.*@', '')
end

--- One entry of the `servers` array, as a {uri, params} pair.
--
-- A plain URI string is the common case. The table form is what a Tarantool 3
-- cluster config hands out for a listener that has parameters -- `transport:
-- ssl` and the client-side ssl_* files -- and net.box accepts those only as
-- part of the URI argument ({uri = ..., params = ...}); there is no `params`
-- connection option (measured on 3.9: "unexpected option 'params'").
local function normalize_server(srv)
    if type(srv) == 'string' then
        return {uri = srv}
    end
    if type(srv) == 'table' and type(srv.uri) == 'string' then
        return {uri = srv.uri, params = srv.params}
    end
    error(0, 'mpool: a server must be a URI string or a {uri = ..., ' ..
             'params = ...} table, got %s', type(srv))
end

--- Connection failures that no amount of retrying will fix.
--
-- With reconnect_after set, net.box retries a rejected authentication for as
-- long as it is allowed to, so without this a wrong password costs the whole
-- connect timeout and is then reported as a timeout rather than as a refusal.
-- The state cannot be used to tell the two apart: measured on 3.9, a wrong
-- password, an unknown user and a peer that is simply down all leave
-- conn.state == 'error_reconnect'. conn.error does say which.
local FATAL_CONNECT_ERRORS = {
    'User not found or supplied credentials are invalid',
    'Session access is denied',
}

local function is_fatal_connect_error(err)
    if err == nil then
        return false
    end
    err = tostring(err)
    for _, text in ipairs(FATAL_CONNECT_ERRORS) do
        if err:find(text, 1, true) ~= nil then
            return true
        end
    end
    return false
end

-------------------------------------------------------------------------------
-- Sharding
-------------------------------------------------------------------------------

local crc32 = digest.crc32.new()

--- Bucket owning `name`, in 1..server_cnt.
--
-- Jump consistent hashing (digest.guava) over a crc32 of the name, so adding a
-- worker moves only the names it must move, and every instance computes the
-- same answer from the name alone.
local function guava_name(name, server_cnt)
    if type(name) == 'table' then
        for _, el in ipairs(name) do
            crc32:update(el)
        end
        name = crc32:result()
        crc32:clear()
    elseif type(name) ~= 'number' then
        name = digest.crc32(name)
    end
    return 1 + digest.guava(name, server_cnt)
end

-------------------------------------------------------------------------------
-- Buckets
-------------------------------------------------------------------------------

--- Resolve a dotted name against _G, e.g. 'pregel.worker.deliver'.
local function registry_lookup(path)
    local node = _G
    for part in path:gmatch('[^%.]+') do
        if type(node) ~= 'table' then
            return nil
        end
        node = node[part]
    end
    return node
end

local bucket_common_methods = {
    --- Invoke `path` with `args` on the bucket's instance.
    --
    -- A local bucket calls the registry function in this process: there is no
    -- reason to pay for a loopback connection, and net.box.self would still
    -- serialise everything through msgpack.
    rpc = function(self, path, args)
        if self.is_local then
            local func = registry_lookup(path)
            if not is_callable(func) then
                error("mpool: '%s' is not registered on this instance", path)
            end
            return func(unpack(args))
        end
        return self.connection:call(path, args)
    end,
    --- Send one message and wait for its result.
    send = function(self, msg, args)
        return self:rpc(WORKER_DELIVER, {self.name, msg, args})
    end,
    deliver_batch = function(self, msgs)
        return self:rpc(WORKER_DELIVER_BATCH, {self.name, msgs})
    end,
    --- Re-raise a delivery failure that happened out of the caller's sight.
    --
    -- A batch that fails inside the background pusher is gone -- flush() has
    -- already reset the counter -- and the fiber that lost it is not the one
    -- running the superstep. Recording it on the bucket is what lets the next
    -- put() or flush() tell the producer that the run is no longer sound,
    -- instead of reporting 'ok' over a lost batch.
    check_failure = function(self)
        if self.failure ~= nil then
            error("mpool: bucket %d (%s) failed to deliver: %s", self.id,
                  safe_uri(self.uri), self.failure)
        end
    end,
    --- Wait for this bucket's connection, or raise saying why it never came.
    --
    -- The message carries net.box's own conn.error verbatim, because that is
    -- the only place the difference between "nothing is listening there" and
    -- "the credentials were refused" exists.
    wait_connected = function(self, timeout)
        local conn = self.connection
        if conn == nil then
            return
        end
        local deadline = clock.monotonic() + timeout
        repeat
            local left = deadline - clock.monotonic()
            if conn:wait_connected(left < CONNECT_POLL and left or
                                   CONNECT_POLL) then
                return
            end
            if is_fatal_connect_error(conn.error) then
                error(0, "mpool: cannot connect to '%s': %s",
                      safe_uri(self.uri), tostring(conn.error))
            end
        until clock.monotonic() >= deadline
        error(0, "mpool: cannot connect to '%s' within %s seconds: %s",
              safe_uri(self.uri), tostring(timeout),
              tostring(conn.error or 'no error reported'))
    end,
    --- Note whether this bucket is in fact this very instance.
    --
    -- The greeting carries the peer's uuid, so telling "this is me" apart from
    -- "this is another instance" costs no RPC -- and box.info.server.uuid,
    -- which the 1.6 version compared against, no longer exists. It needs a
    -- live connection, which is why it happens here and not in bucket_new().
    resolve_local = function(self)
        local conn = self.connection
        if conn == nil then
            return
        end
        self.uuid = conn.peer_uuid
        if self.uuid ~= box.info.uuid then
            return
        end
        -- A local bucket calls the registry in this process; there is no
        -- reason to pay for a loopback connection.
        self.is_local = true
        conn:close()
        self.connection = nil
    end,
    stop = function(self)
        self.stopped = true
        local worker = self.worker
        self.worker = nil
        if worker ~= nil then
            -- Ask the pusher to leave rather than cancelling it: a cancel
            -- lands as an exception inside whatever it is doing, which at
            -- worst is a flush in flight and at best is a stack trace in the
            -- log on every shutdown. Its longest wait is 10ms.
            local deadline = clock.monotonic() + 1
            while worker:status() ~= 'dead' and clock.monotonic() < deadline do
                fiber.sleep(0.005)
            end
            if worker:status() ~= 'dead' then
                worker:cancel()
            end
        end
        if self.connection ~= nil then
            self.connection:close()
            self.connection = nil
        end
    end,
}

--- Background pusher for an instant bucket.
local function pusher_handler(bucket)
    local function handler(self)
        -- Named for the bucket alone: whether it is the local one is not known
        -- until the connections have been resolved, and the pusher starts
        -- before that.
        fiber.self():name(string.format('mpool_pusher_handler-%02d', self.id),
                          {truncate = true})
        log.verbose('<mpool, %s> pusher fiber started', tostring(self.name))
        while not self.stopped do
            -- Nothing left to carry once a delivery has failed: the failure is
            -- recorded on the bucket and put()/flush() raise it. Ending the
            -- fiber here instead is what used to leave a producer waiting on a
            -- flush that nobody would ever perform.
            if self.count > 0 and self.failure == nil then
                -- flush() records the failure and re-raises it; the pusher is
                -- not the one that can act on it, so it logs and carries on.
                local ok, err = pcall(self.flush, self)
                if not ok then
                    log.error('<mpool, %s> pusher flush failed: %s',
                              tostring(self.name), tostring(err))
                end
                fiber.yield()
            else
                fiber.sleep(0.01)
            end
        end
        log.verbose('<mpool, %s> pusher fiber stopped', tostring(self.name))
    end

    return function()
        xpcall_tb(handler, bucket)
    end
end

local bucket_instant_methods = {
    put = function(self, msg, args)
        self:check_failure()
        while self.count >= self.max_count do
            -- Bounded wait: the flush broadcasts, and the loop re-checks, so a
            -- broadcast that lands between the check and the wait costs one
            -- poll interval instead of a hang.
            self.not_full:wait(FULL_POLL)
            -- A pusher that failed will not drain this bucket, so a producer
            -- waiting here has to be let out with the error rather than left
            -- to poll forever.
            self:check_failure()
        end
        self.count = self.count + 1
        local slot = self.msg_pool[self.count]
        if slot == nil then
            slot = table_new(2, 0)
            self.msg_pool[self.count] = slot
        end
        slot[1] = msg
        slot[2] = args
    end,
    flush = function(self)
        self:check_failure()
        -- One sender at a time. The background pusher and a caller's own
        -- flush() would otherwise be in flight together, and the batch started
        -- second can arrive first: a preload's edge.store overtaking its
        -- vertex.store is refused outright ("vertex does not exist"). Checking
        -- and setting the flag never yields in between, so this is a lock.
        while self.sending do
            self.idle:wait(FULL_POLL)
            self:check_failure()
        end
        -- Copy the batch out of the accumulation buffer and reset the buffer
        -- before the first yield. The slots are reused by the next put(), so a
        -- batch that merely referenced them would be rewritten underneath the
        -- RPC; and a second flush() entering while this one is in flight must
        -- see an empty bucket rather than send the same messages again.
        local count = self.count
        if count == 0 then
            return
        end
        local msgs = table_new(count, 0)
        for i = 1, count do
            local slot = self.msg_pool[i]
            msgs[i] = {slot[1], slot[2]}
        end
        self.count = 0
        self.not_full:broadcast()

        self.sending = true
        local ok, err = pcall(self.deliver_batch, self, msgs)
        self.sending = false
        self.idle:broadcast()
        if not ok then
            -- The batch is lost with the exception -- it was copied out of a
            -- buffer that put() has been free to reuse since the line above.
            -- What must not be lost is the fact that it happened: record it so
            -- the next put()/flush() raises rather than reporting a superstep
            -- as 'ok', and release any producer waiting on the bucket.
            self.failure = self.failure or tostring(err)
            self.not_full:broadcast()
            error(tostring(err))
        end
    end,
    --- Everything this bucket was given is on the far side, acknowledged.
    --
    -- Not what flush() alone does: a batch the pusher has taken has already
    -- reset the counter, so a bucket with nothing accumulated may still have
    -- one travelling. That is the whole BSP barrier -- run_superstep, preload
    -- and the master's inform_workers all end here -- and skipping a bucket
    -- because its count is 0 is what let a superstep's messages land in the
    -- next one.
    drain = function(self)
        while true do
            self:flush()
            if self.count == 0 and not self.sending then
                break
            end
            -- Only reachable if something is still producing into this bucket
            -- while the phase is supposed to be over; wait for it and re-check
            -- rather than returning on a bucket that is not actually empty.
            self.idle:wait(FULL_POLL)
        end
        self:check_failure()
    end,
    start = function(self)
        self.worker = fiber.create(pusher_handler(self))
    end
}

local bucket_delayed_methods = {
    put = function(self, msg, args)
        self.space:insert{box.NULL, msg, args == nil and box.NULL or args}
        self.count = self.count + 1
    end,
    flush = function(self)
        while true do
            local tuples = self.space:select(nil, {
                iterator = 'GE',
                limit = self.max_count
            })
            if #tuples == 0 then
                break
            end
            local msgs = table_new(#tuples, 0)
            for i, tuple in ipairs(tuples) do
                msgs[i] = {tuple[2], tuple[3]}
            end
            local rv = bench_monotonic(self.deliver_batch, self, msgs)
            log.verbose('<mpool, %s> flushed %d delayed message(s) in %010.6f',
                        tostring(self.name), #tuples, rv[1])
            -- Delete only once the batch is through, so a failed send leaves
            -- the messages to be retried instead of dropping them.
            for _, tuple in ipairs(tuples) do
                self.space:delete{tuple[1]}
            end
        end
        self.count = 0
    end,
    --- Nothing travels behind a delayed bucket's back: it has no pusher fiber
    -- and flush() only returns once every batch has been acknowledged, so the
    -- barrier is already flush()'s own doing.
    drain = function(self)
        if self.count > 0 then
            self:flush()
        end
    end,
    start = function(self)
        self.space_name = string.format('pregel_mpool_%s_%02d', self.name,
                                        self.id)
        local space = box.schema.space.create(self.space_name, {
            if_not_exists = true,
            format = {
                {name = 'id',   type = 'unsigned'},
                {name = 'msg',  type = 'string'  },
                {name = 'args', type = 'any'     },
            }
        })
        space:create_index('primary', {
            type          = 'TREE',
            parts         = {{field = 1, type = 'unsigned'}},
            sequence      = true,
            if_not_exists = true
        })
        self.space = space
        self.count = space:len()
    end,
    --- Drop the backing space. Not part of stop(): a delayed bucket is meant
    -- to survive a restart with its pending messages.
    drop = function(self)
        if self.space ~= nil then
            self.space:drop()
            self.space = nil
        end
    end,
}

local function methods_of(engine_methods)
    local result = {}
    for k, v in pairs(bucket_common_methods) do result[k] = v end
    for k, v in pairs(engine_methods)        do result[k] = v end
    return result
end

local bucket_instant_mt = { __index = methods_of(bucket_instant_methods) }
local bucket_delayed_mt = { __index = methods_of(bucket_delayed_methods) }

local function bucket_new(id, name, srv, options)
    options = options or {}
    local msg_count  = options.msg_count or 1000
    local is_delayed = options.is_delayed
    if is_delayed == nil then is_delayed = false end

    -- Never waits: whether the peer is up is the pool's business (see
    -- mpool:wait_connected), and a role's apply() must be able to build the
    -- whole pool without blocking the instance's config startup.
    local conn = remote.new(srv.params ~= nil and
                            {uri = srv.uri, params = srv.params} or srv.uri, {
        user            = options.user,
        password        = options.password,
        reconnect_after = options.reconnect_after or RECONNECT_AFTER,
        wait_connected  = false
    })

    local self = {
        id           = id,
        uri          = srv.uri,
        params       = srv.params,
        name         = name,
        -- Both are answered by resolve_local() once the connection is up. A
        -- bucket used before that is treated as remote, which is correct if
        -- slower: it reaches this instance over its own listener.
        uuid         = nil,
        is_local     = false,
        count        = 0,
        connection   = conn,
        not_full     = fiber.cond(),
        -- A batch is in flight: set around the RPC, waited on by flush() and
        -- drain(). The cond is broadcast when it clears.
        sending      = false,
        idle         = fiber.cond(),
        is_delayed   = is_delayed,
        max_count    = msg_count,
        stopped      = false,
        worker       = nil,
        -- Set by flush() when a delivery raises, read by check_failure().
        failure      = nil,
    }

    if is_delayed then
        setmetatable(self, bucket_delayed_mt)
    else
        self.msg_pool = table_new(msg_count, 0)
        setmetatable(self, bucket_instant_mt)
    end

    return self
end

-------------------------------------------------------------------------------
-- waitpool: one message to every bucket, in parallel, wait for all
-------------------------------------------------------------------------------

local function waitpool_handler(id, bucket)
    local function handler(self)
        fiber.self():name(string.format('waitpool_handler-%02d', id),
                          {truncate = true})
        while true do
            local status = self.channel_in:get()
            if status == nil or status == false then
                return
            end
            -- A handler that died would never answer on channel_out and
            -- __call would wait for it forever, so failures travel back as
            -- values instead of unwinding this fiber.
            local ok, rv = pcall(bench_monotonic, bucket.send, bucket,
                                 self.msg, self.args)
            if ok then
                self.rval[id] = rv
            else
                self.errors[id] = tostring(rv)
            end
            self.channel_out:put(true)
        end
    end

    return function(self)
        xpcall_tb(handler, self)
    end
end

local waitpool_mt = {
    __index = {
        -- Closing the input channel makes every handler's get() return nil,
        -- which is how they are told to finish -- no cancel, no stack trace.
        stop = function(self)
            self.channel_in:close()
            self.fpool = {}
        end,
        --- Refuse to keep waiting for a handler that is gone.
        --
        -- The pcall in waitpool_handler is what normally keeps one alive, but
        -- a fiber can also be cancelled, and a handler that dies holding its
        -- task never answers on channel_out. __call used to wait for that
        -- answer forever -- in production, and in the very test that covers
        -- the pcall, where it hung the whole suite instead of failing it.
        check_handlers = function(self)
            for id, handler in ipairs(self.fpool) do
                if handler:status() == 'dead' then
                    error('mpool: waitpool handler %d is dead, bucket %d ' ..
                          'will never answer', id, id)
                end
            end
        end
    },
    __call = function(self, msg, args)
        self.rval   = {}
        self.errors = {}
        self.msg    = msg
        self.args   = args
        -- An answer left over from a call that gave up on a dead handler would
        -- otherwise be read as one of this call's.
        while self.channel_out:get(0) ~= nil do end
        for i = 1, self.fpool_cnt do
            self.channel_in:put(i)
        end
        for _ = 1, self.fpool_cnt do
            while self.channel_out:get(HANDLER_POLL) == nil do
                self:check_handlers()
            end
        end
        local failures = {}
        for id, err in pairs(self.errors) do
            table.insert(failures, string.format('bucket %d: %s', id, err))
        end
        if #failures > 0 then
            table.sort(failures)
            error("mpool: '%s' failed on %d bucket(s): %s", tostring(msg),
                  #failures, table.concat(failures, '; '))
        end
        return self.rval
    end,
}

local function waitpool_new(pool)
    local self = setmetatable({
        fpool_cnt   = pool.bucket_cnt,
        -- Capacity equal to the fan-out, so handing out the tasks cannot
        -- block: a zero-capacity put() waits for a receiver, and a handler
        -- that is gone is never going to be one -- __call would then hang in
        -- put() and never reach the check_handlers() that exists to catch
        -- exactly that. Measured: with fiber.channel(0) here,
        -- test_send_wait_does_not_wait_for_a_dead_handler waits out its 15s
        -- bound instead of getting an error.
        channel_in  = fiber.channel(pool.bucket_cnt),
        channel_out = fiber.channel(pool.bucket_cnt),
        rval        = {},
        errors      = {},
        msg         = nil,
        args        = nil
    }, waitpool_mt)
    self.fpool = fun.iter(pool.buckets):enumerate():map(function(id, bucket)
        return fiber.create(waitpool_handler(id, bucket), self)
    end):totable()
    return self
end

-------------------------------------------------------------------------------
-- Pool
-------------------------------------------------------------------------------

local mpool_mt = {
    __index = {
        --- Id of the bucket owning `name`.
        id = function(self, name)
            return guava_name(name, self.bucket_cnt)
        end,
        --- Bucket owning `name`.
        by_id = function(self, name)
            return self.buckets[guava_name(name, self.bucket_cnt)]
        end,
        --- The BSP barrier: return only once every bucket's messages are on
        -- the far side.
        --
        -- Every phase boundary in the library is a call to this. It used to
        -- skip a bucket whose count was 0, which is exactly what a bucket the
        -- background pusher has taken a batch from looks like -- so the phase
        -- ended with messages still travelling.
        flush = function(self)
            for _, bucket in ipairs(self.buckets) do
                bucket:drain()
            end
        end,
        --- Send one message to every bucket and wait for all the answers.
        send_wait = function(self, message, args)
            log.verbose('<mpool, %s> send_wait %s <%s>', self.name, message,
                        json.encode(args))
            return self.waitpool(message, args)
        end,
        --- Queue one message on every bucket, to go out with the next flush.
        send = function(self, message, args)
            for _, bucket in ipairs(self.buckets) do
                bucket:put(message, args)
            end
        end,
        --- Wait until every bucket's connection is up, then work out which
        -- bucket is this instance.
        --
        -- Separate from mpool.new() because the caller that must not block is
        -- the one that matters: a Tarantool 3 role's apply() holds up the
        -- instance's whole config startup, so it builds the pool here and
        -- waits in a fiber of its own. mpool.new() calls this itself unless
        -- options.connect_async says otherwise.
        --
        -- Raises the first bucket that never came up, with net.box's own
        -- reason in the message.
        wait_connected = function(self, timeout)
            timeout = timeout or CONNECT_TIMEOUT
            local deadline = clock.monotonic() + timeout
            for _, bucket in ipairs(self.buckets) do
                local left = deadline - clock.monotonic()
                bucket:wait_connected(left > 0 and left or 0)
            end
            for idx, bucket in ipairs(self.buckets) do
                bucket:resolve_local()
                if bucket.is_local then
                    self.self_idx = idx
                end
            end
            self.connected = true
            return self
        end,
        --- Block until the pool is connected, whoever does the connecting.
        --
        -- Not wait_connected() itself, and deliberately: with connect_async
        -- the owner of the waiting is the role's fiber, and a second fiber
        -- running wait_connected() concurrently would race it -- resolving the
        -- pool closes the local bucket's connection, and a fiber waiting on
        -- that very connection sees the close as a failure. This is what a
        -- worker asked to preload waits on, because self_idx -- the shard a
        -- worker-side loader reads -- is one of the things being resolved.
        wait_ready = function(self, timeout)
            local deadline = clock.monotonic() + timeout
            while not self.connected do
                if clock.monotonic() > deadline then
                    error(0, "mpool: '%s' did not reach all %d peer(s) " ..
                          'within %s seconds', tostring(self.name),
                          self.bucket_cnt, tostring(timeout))
                end
                fiber.sleep(0.01)
            end
            return self
        end,
        stop = function(self)
            self.waitpool:stop()
            for _, bucket in ipairs(self.buckets) do
                bucket:stop()
            end
        end,
    }
}

--- Build a pool over `servers`.
--
-- servers is an array of net.box URIs; each may carry its own credentials as
-- 'user:password@host:port', and options.user / options.password apply to the
-- ones that do not. An entry may also be a {uri = ..., params = ...} table,
-- which is how a listener's transport parameters (`transport: ssl` and the
-- client-side ssl_* files) reach net.box.
--
-- options.msg_count       -- messages per batch (default 1000)
-- options.is_delayed      -- back the buckets with spaces (default false)
-- options.user            -- net.box user for every connection
-- options.password        -- net.box password for every connection
-- options.connect_timeout -- seconds to wait for all the peers (default 30)
-- options.reconnect_after -- net.box retry interval (default 0.1)
-- options.connect_async   -- return without waiting for any peer; the caller
--                            is then the one that calls pool:wait_connected()
local function mpool_new(name, servers, options)
    options = options or {}
    local msg_count = options.msg_count or 1000

    local is_delayed = options.is_delayed
    if is_delayed == nil then is_delayed = false end

    assert(type(servers) == 'table' and #servers > 0,
           'mpool needs at least one server')

    local self = setmetatable({
        name       = name,
        buckets    = {},
        bucket_cnt = 0,
        is_delayed = is_delayed,
        self_idx   = 0,
        connected  = false,
    }, mpool_mt)

    local sorted = {}
    for _, server in ipairs(servers) do
        table.insert(sorted, normalize_server(server))
    end
    -- Every instance orders the buckets the same way, so bucket N means the
    -- same worker everywhere and guava_name() agrees across the cluster
    -- regardless of the order the URIs were listed in. The key is the URI
    -- string: the peer uuid the previous version sorted by is only known once
    -- a connection has been made, and the order has to exist before that --
    -- the whole point of connect_async is a pool that is complete before any
    -- peer has answered. Every participant reads the same list out of the same
    -- config, so the strings agree.
    table.sort(sorted, function(a, b) return a.uri < b.uri end)

    for k, server in ipairs(sorted) do
        table.insert(self.buckets, bucket_new(k, name, server, {
            is_delayed      = is_delayed,
            msg_count       = msg_count,
            user            = options.user,
            password        = options.password,
            reconnect_after = options.reconnect_after,
        }))
        self.bucket_cnt = self.bucket_cnt + 1
    end

    if not options.connect_async then
        local ok, err = pcall(self.wait_connected, self,
                              options.connect_timeout or CONNECT_TIMEOUT)
        if not ok then
            -- Nothing has been started yet, so closing the connections is the
            -- whole cleanup.
            for _, bucket in ipairs(self.buckets) do
                if bucket.connection ~= nil then
                    bucket.connection:close()
                    bucket.connection = nil
                end
            end
            error(0, tostring(err))
        end
    end

    for _, bucket in ipairs(self.buckets) do
        bucket:start()
    end

    self.waitpool = waitpool_new(self)

    return self
end

return strict.strictify({
    new        = mpool_new,
    guava_name = guava_name,
})
