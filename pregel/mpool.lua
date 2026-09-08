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

local RECONNECT_AFTER  = 5
local CONNECT_TIMEOUT  = 30
-- How long a producer blocked on a full bucket sleeps before re-checking. The
-- flush broadcasts, so this is only a backstop against a lost wakeup.
local FULL_POLL        = 0.1

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
        fiber.self():name(string.format('%6s_pusher_handler-%02d',
                                        self.is_local and 'local' or 'remote',
                                        self.id), {truncate = true})
        log.verbose('<mpool, %s> pusher fiber started', tostring(self.name))
        while not self.stopped do
            if self.count > 0 then
                self:flush()
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
        while self.count >= self.max_count do
            -- Bounded wait: the flush broadcasts, and the loop re-checks, so a
            -- broadcast that lands between the check and the wait costs one
            -- poll interval instead of a hang.
            self.not_full:wait(FULL_POLL)
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

        self:deliver_batch(msgs)
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

    local conn = remote.new(srv, {
        user            = options.user,
        password        = options.password,
        reconnect_after = RECONNECT_AFTER,
        wait_connected  = false
    })
    local timeout = options.connect_timeout or CONNECT_TIMEOUT
    if not conn:wait_connected(timeout) then
        conn:close()
        error("mpool: cannot connect to '%s' within %s seconds",
              safe_uri(srv), tostring(timeout))
    end

    -- The greeting carries the peer's uuid, so telling "this is me" apart from
    -- "this is another instance" costs no RPC -- and box.info.server.uuid,
    -- which the 1.6 version compared against, no longer exists.
    local uuid = conn.peer_uuid
    local is_local = (uuid == box.info.uuid)
    if is_local then
        conn:close()
        conn = nil
    end

    local self = {
        id           = id,
        uri          = srv,
        name         = name,
        uuid         = uuid,
        count        = 0,
        connection   = conn,
        not_full     = fiber.cond(),
        is_local     = is_local,
        is_delayed   = is_delayed,
        max_count    = msg_count,
        stopped      = false,
        worker       = nil,
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
        end
    },
    __call = function(self, msg, args)
        self.rval   = {}
        self.errors = {}
        self.msg    = msg
        self.args   = args
        for i = 1, self.fpool_cnt do
            self.channel_in:put(i)
        end
        for _ = 1, self.fpool_cnt do
            self.channel_out:get()
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
        -- Capacity equal to the fan-out: __call must be able to hand out every
        -- task without first knowing that a handler fiber has reached its
        -- get(), which a zero-capacity channel would require.
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
        flush = function(self)
            for _, bucket in ipairs(self.buckets) do
                if bucket.count > 0 then
                    bucket:flush()
                end
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
-- ones that do not.
--
-- options.msg_count       -- messages per batch (default 1000)
-- options.is_delayed      -- back the buckets with spaces (default false)
-- options.user            -- net.box user for every connection
-- options.password        -- net.box password for every connection
-- options.connect_timeout -- seconds to wait per worker (default 30)
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
    }, mpool_mt)

    for k, server in ipairs(servers) do
        table.insert(self.buckets, bucket_new(k, name, server, {
            is_delayed      = is_delayed,
            msg_count       = msg_count,
            user            = options.user,
            password        = options.password,
            connect_timeout = options.connect_timeout,
        }))
        self.bucket_cnt = self.bucket_cnt + 1
    end

    -- Every instance orders the buckets by peer uuid, so bucket N means the
    -- same worker everywhere and guava_name() agrees across the cluster
    -- regardless of the order the URIs were listed in.
    table.sort(self.buckets, function(b1, b2)
        return b1.uuid < b2.uuid
    end)
    for idx, bucket in ipairs(self.buckets) do
        if bucket.is_local then
            self.self_idx = idx
        end
        bucket.id = idx
        bucket:start()
    end

    self.waitpool = waitpool_new(self)

    return self
end

return strict.strictify({
    new        = mpool_new,
    guava_name = guava_name,
})
