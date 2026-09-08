--- The pregel worker.
--
-- A worker owns a shard of the graph -- the vertices whose names hash to its
-- bucket -- and runs the compute function over them once per superstep. The
-- master drives the supersteps; everything a worker is told to do arrives as
-- one of the protocol messages in `info_functions` below.
--
-- Spaces per instance `name`:
--   data_<name>               the graph shard
--   topology_mutation_<name>  edge/vertex additions and deletions, applied
--                             between supersteps
--   pregel_tube_mqueue_*_<name>  the two message queues (see pregel.queue)

local fun    = require('fun')
local log    = require('log')
local fiber  = require('fiber')
local remote = require('net.box')

local queue      = require('pregel.queue')
local vertex     = require('pregel.vertex')
local aggregator = require('pregel.aggregator')
local mpool      = require('pregel.mpool')

local utils       = require('pregel.utils')
local xpcall_tb   = utils.xpcall_tb
local is_callable = utils.is_callable
local error       = utils.error

local vertex_compute = vertex.vertex_private_methods.compute

local workers = {}

-- Short for the same reason as mpool's: this is also the interval before the
-- first retry, and the first attempt at a peer that is still bootstrapping
-- always fails.
local RECONNECT_AFTER = 0.1
local WAIT_TIMEOUT    = 60

local TOPMT_EDGE_DELETE   = 0
local TOPMT_VERTEX_DELETE = 1
local TOPMT_VERTEX_STORE  = 2
local TOPMT_EDGE_STORE    = 3

-- The registry names this module publishes. Everything that talks to a worker
-- goes through conn:call on one of these, so pregel.worker.grant(user) is the
-- whole privilege story -- no universe grant, no eval.
local RPC_DELIVER       = 'pregel.worker.deliver'
local RPC_DELIVER_BATCH = 'pregel.worker.deliver_batch'
local RPC_WAIT          = 'pregel.worker.wait'

local function count_active(acc, tuple)
    return acc + (tuple[2] == false and 1 or 0)
end

local info_functions = setmetatable({
    ['vertex.store'] = function(instance, args)
        return instance:vertex_store(args)
    end,
    ['edge.store'] = function(instance, args)
        return instance:edge_store(args[1], args[2])
    end,
    ['vertex.store.delayed'] = function(instance, args)
        return instance:vertex_store_delayed(args)
    end,
    ['edge.store.delayed'] = function(instance, args)
        return instance:edge_store_delayed(args[1], args[2], args[3])
    end,
    ['vertex.delete.delayed'] = function(instance, args)
        return instance:vertex_delete_delayed(args[1])
    end,
    ['edge.delete.delayed'] = function(instance, args)
        return instance:edge_delete_delayed(args[1], args[2])
    end,
    ['snapshot'] = function()
        return box.snapshot()
    end,
    ['message.deliver'] = function(instance, args)
        -- args[1] - receiver, args[2] - message, args[3] - sender
        return instance.mqueue_next:put(args[1], args[2])
    end,
    ['aggregator.inform'] = function(instance, args)
        -- args[1] - aggregator name, args[2] - the master's merged value.
        -- This used to assign straight to .value, which left the merged value
        -- sitting in the very accumulator the next superstep contributes to --
        -- so every worker reported it back and the master added it once per
        -- worker. See pregel/aggregator.lua.
        instance.aggregators[args[1]]:receive_global(args[2])
    end,
    ['superstep'] = function(instance, args)
        return instance:run_superstep(args)
    end,
    ['superstep.after'] = function(instance, args)
        return instance:after_superstep(args)
    end,
    ['count'] = function(instance)
        instance.in_progress = instance.data_space:pairs():reduce(count_active, 0)
        log.info('<count> found %d active vertices', instance.in_progress)
        return instance.in_progress
    end,
    ['preload'] = function(instance)
        return instance:preload()
    end
}, {
    __index = function(_, op)
        return function()
            error('unknown message type: %s', op)
        end
    end
})

--- Wait until the instance called `name` exists here, has reached its master
-- and has reached the other workers. This is the first thing the master asks
-- of every worker, and it is the barrier the whole run rests on: the shard
-- index a worker-side loader is handed (mpool.self_idx) is only known once the
-- message pool has resolved its connections, and a preload that ran before
-- that would give every worker the same share of the graph.
local function wait_ready(name)
    local deadline = fiber.clock() + WAIT_TIMEOUT
    while workers[name] == nil do
        if fiber.clock() > deadline then
            error("pregel worker '%s' was not created within %d seconds",
                  tostring(name), WAIT_TIMEOUT)
        end
        fiber.sleep(0.01)
    end
    local instance = workers[name]
    if not instance.master:wait_connected(WAIT_TIMEOUT) then
        error("pregel worker '%s' cannot reach its master within %d seconds",
              tostring(name), WAIT_TIMEOUT)
    end
    instance.mpool:wait_ready(WAIT_TIMEOUT)
    return true
end

local function deliver_msg(name, msg, args)
    if msg == 'wait' then
        return wait_ready(name)
    end
    local rv = {xpcall_tb(function()
        local instance = workers[name]
        assert(instance, 'no pregel instance found')
        return info_functions[msg](instance, args)
    end)}
    local status = table.remove(rv, 1)
    if status == false then
        error(tostring(rv[1]))
    end
    return unpack(rv)
end

local function deliver_batch(name, msgs)
    local status, err = xpcall_tb(function()
        local instance = workers[name]
        assert(instance, 'no pregel instance found')
        for _, msg in ipairs(msgs) do
            info_functions[msg[1]](instance, msg[2])
        end
        return #msgs
    end)
    if status == false then
        error(tostring(err))
    end
    return #msgs
end

-------------------------------------------------------------------------------
-- Topology mutation
-------------------------------------------------------------------------------

--- Group pending mutations of one type by the vertex they act on.
--
-- Everything is read out before anything is applied: applying deletes from the
-- mutation space, and mutating a space underneath its own iterator is not
-- something to rely on.
local function collect_mutations(index, tmtype)
    local groups, order = {}, {}
    for _, tuple in index:pairs({tmtype}) do
        local id, _, name, dest, value = tuple:unpack()
        local group = groups[name]
        if group == nil then
            group = {}
            groups[name] = group
            table.insert(order, name)
        end
        table.insert(group, {id = id, dest = dest, value = value})
    end
    return groups, order
end

-------------------------------------------------------------------------------
-- Worker
-------------------------------------------------------------------------------

local worker_mt = {
    __index = {
        run_superstep = function(self, superstep)
            local function tuple_filter(tuple)
                local id, halt = tuple:unpack(1, 2)
                -- A halted vertex with no messages has nothing to do.
                return not (self.mqueue:len(id) == 0 and halt == true)
            end

            local function tuple_process(acc, tuple)
                if acc % 1000 == 0 then
                    fiber.yield()
                end
                if acc % 10000 == 0 then
                    log.info('processed %d/%d vertices', acc,
                             self.data_space:len())
                end
                local vertex_object = self.vertex_pool:pop(tuple)
                vertex_object.__superstep = superstep
                vertex_object:vote_halt(false)
                vertex_compute(vertex_object)
                self.mqueue:delete(vertex_object.__id)
                self.vertex_pool:push(vertex_object)
                return acc + 1
            end

            log.info('starting superstep %d', superstep)

            self.data_space:pairs()
                           :filter(tuple_filter)
                           :reduce(tuple_process, 0)

            while self.vertex_pool.count > 0 do
                fiber.yield()
            end

            self.mpool:flush()

            log.info('ending superstep %d', superstep)
            return 'ok'
        end,
        after_superstep = function(self)
            -- Swap the message queues: what was delivered during the superstep
            -- becomes what the next one reads.
            self.mqueue, self.mqueue_next = self.mqueue_next, self.mqueue

            local left = self.mqueue_next:len()
            if left > 0 then
                log.warn('%d message(s) left unread from the last superstep',
                         left)
                for receiver in self.mqueue_next:receiver_closure() do
                    log.warn('  unread message(s) for %s', tostring(receiver))
                end
            end
            self.mqueue_next:truncate()

            -- In squash_only mode the combiner has not run yet: fold each
            -- receiver's messages down now, before the next superstep reads
            -- them and before __messages is counted.
            self.mqueue:squash()

            self:apply_topology_mutations()

            self.aggregators['__in_progress'](self.in_progress)
            self.aggregators['__messages'](self.mqueue:len())

            log.info('%d message(s) in mqueue, %d vertices in progress',
                     self.mqueue:len(), self.in_progress)

            for _, aggr in pairs(self.aggregators) do
                aggr:inform_master()
            end

            return 'ok'
        end,
        apply_topology_mutations = function(self)
            local tmspace = self.topology_mutation_space
            local tmindex = tmspace.index.type_name
            local processed = {}

            local function done(group)
                for _, req in ipairs(group) do
                    table.insert(processed, req.id)
                end
            end

            log.info('<topology mutation> del_edge %d, del_vertex %d, ' ..
                     'add_vertex %d, add_edge %d tasks',
                     tmindex:count({TOPMT_EDGE_DELETE}),
                     tmindex:count({TOPMT_VERTEX_DELETE}),
                     tmindex:count({TOPMT_VERTEX_STORE}),
                     tmindex:count({TOPMT_EDGE_STORE}))

            -- 1. delete edges
            local groups, order = collect_mutations(tmindex, TOPMT_EDGE_DELETE)
            for _, src in ipairs(order) do
                local group = groups[src]
                local tuple = self.data_space:get{src}
                if tuple == nil then
                    -- The vertex may have been deleted in this same batch, or
                    -- never have existed. Either way there is nothing to
                    -- index into, which is what the old code did here.
                    log.info("<topology mutation, del_edge> vertex '%s' " ..
                             "does not exist", src)
                else
                    local edge_list = tuple:totable()[4]
                    for _, req in ipairs(group) do
                        local removed = false
                        for idx, edge in ipairs(edge_list) do
                            if edge[1] == req.dest then
                                table.remove(edge_list, idx)
                                removed = true
                                break
                            end
                        end
                        log.info("<topology mutation, del_edge> '%s'->'%s': %s",
                                 src, tostring(req.dest),
                                 removed and 'deleted' or 'does not exist')
                    end
                    self.data_space:update(src, {{'=', 4, edge_list}})
                end
                done(group)
            end

            -- 2. delete vertices
            groups, order = collect_mutations(tmindex, TOPMT_VERTEX_DELETE)
            for _, name in ipairs(order) do
                local rv = self.data_space:delete{name}
                if rv ~= nil then
                    log.info("<topology mutation, del_vertex> '%s': deleted",
                             name)
                    -- delete() returns the tuple it removed, so is_halted is
                    -- readable exactly when there was something to delete --
                    -- the old code had this branch inverted and indexed nil.
                    if rv[2] == false then
                        self.in_progress = self.in_progress - 1
                    end
                else
                    log.info("<topology mutation, del_vertex> '%s': " ..
                             "does not exist", name)
                end
                done(groups[name])
            end

            -- 3. add vertices, before any edge that points out of them
            groups, order = collect_mutations(tmindex, TOPMT_VERTEX_STORE)
            for _, name in ipairs(order) do
                local group = groups[name]
                if self.data_space:get{name} ~= nil then
                    log.info("<topology mutation, add_vertex> '%s': exists",
                             name)
                else
                    self.data_space:replace{name, false, group[1].value, {}}
                    log.info("<topology mutation, add_vertex> '%s': added",
                             name)
                    self.in_progress = self.in_progress + 1
                end
                done(group)
            end

            -- 4. add edges
            groups, order = collect_mutations(tmindex, TOPMT_EDGE_STORE)
            for _, src in ipairs(order) do
                local group = groups[src]
                local tuple = self.data_space:get{src}
                if tuple == nil then
                    log.info("<topology mutation, add_edge> vertex '%s' " ..
                             "does not exist", src)
                else
                    local edge_list = tuple:totable()[4]
                    for _, req in ipairs(group) do
                        log.info("<topology mutation, add_edge> '%s'->'%s': " ..
                                 "added", src, tostring(req.dest))
                        table.insert(edge_list, {req.dest, req.value})
                    end
                    self.data_space:update(src, {{'=', 4, edge_list}})
                end
                done(group)
            end

            for _, id in ipairs(processed) do
                tmspace:delete{id}
            end
        end,
        add_aggregator = function(self, name, opts)
            assert(self.aggregators[name] == nil,
                   'aggregator already exists: ' .. tostring(name))
            self.aggregators[name] = aggregator.new(name, self, opts)
            return self
        end,
        preload = function(self)
            if self.preload_func == nil then
                log.info('no worker preload configured')
                return 'ok'
            end
            self.preload_func(self.mpool.self_idx, self.mpool.bucket_cnt)
            self.mpool:flush()
            return 'ok'
        end,
        vertex_store = function(self, value)
            local id = self.obtain_name(value)
            self.data_space:replace{id, false, value, {}}
        end,
        edge_store = function(self, from, edges)
            local tuple = self.data_space:get{from}
            if tuple == nil then
                error("edge.store: vertex '%s' does not exist", tostring(from))
            end
            tuple = tuple:totable()
            tuple[4] = fun.chain(tuple[4], edges):totable()
            self.data_space:replace(tuple)
        end,
        vertex_store_delayed = function(self, value)
            self.topology_mutation_space:insert{
                box.NULL, TOPMT_VERTEX_STORE, self.obtain_name(value),
                box.NULL, value
            }
        end,
        edge_store_delayed = function(self, src, dest, value)
            self.topology_mutation_space:insert{
                box.NULL, TOPMT_EDGE_STORE, src, dest, value
            }
        end,
        vertex_delete_delayed = function(self, vertex_name)
            -- The 1.6 version inserted a stray 2 between the type and the
            -- name, which put the vertex name in the `dest` field and left
            -- the index looking for a vertex called "2".
            self.topology_mutation_space:insert{
                box.NULL, TOPMT_VERTEX_DELETE, vertex_name, box.NULL, box.NULL
            }
        end,
        edge_delete_delayed = function(self, src, dest)
            self.topology_mutation_space:insert{
                box.NULL, TOPMT_EDGE_DELETE, src, dest, box.NULL
            }
        end,
        stop = function(self)
            self.mpool:stop()
            if self.master ~= nil then
                self.master:close()
                self.master = nil
            end
            -- Take the queues out of queue.new's cache without dropping their
            -- spaces. Leaving them there meant a worker created afterwards
            -- under the same name inherited this instance's combiner, engine
            -- and squash_only whatever it asked for -- and queue.new now
            -- refuses a mismatch, so leaving them there would turn a restart
            -- in place with new options into an error instead. The spaces
            -- outlive the instance on purpose: that is what the 'space' engine
            -- is for, and queue.new rebuilds the counters from what is in them.
            for _, q in ipairs({self.mqueue, self.mqueue_next}) do
                if q ~= nil and q.name ~= nil then
                    rawset(queue.list, q.name, nil)
                end
            end
            workers[self.name] = nil
        end,
    }
}

-------------------------------------------------------------------------------
-- Schema
-------------------------------------------------------------------------------

local function create_spaces(name)
    local data = box.schema.space.create('data_' .. name, {
        if_not_exists = true,
        format = {
            {name = 'id',        type = 'string' },
            {name = 'is_halted', type = 'boolean'},
            {name = 'value',     type = 'any'    },
            {name = 'edges',     type = 'array'  },
        }
    })
    data:create_index('primary', {
        type          = 'TREE',
        parts         = {{field = 1, type = 'string'}},
        if_not_exists = true
    })

    local tm = box.schema.space.create('topology_mutation_' .. name, {
        if_not_exists = true,
        format = {
            {name = 'id',    type = 'unsigned'},
            {name = 'type',  type = 'unsigned'},
            {name = 'name',  type = 'string'  },
            {name = 'dest',  type = 'any', is_nullable = true},
            {name = 'value', type = 'any', is_nullable = true},
        }
    })
    -- space:auto_increment() is gone in Tarantool 3; box.NULL in field 1 draws
    -- from the sequence instead.
    tm:create_index('primary', {
        type          = 'TREE',
        parts         = {{field = 1, type = 'unsigned'}},
        sequence      = true,
        if_not_exists = true
    })
    tm:create_index('type_name', {
        type          = 'TREE',
        parts         = {{field = 2, type = 'unsigned'},
                         {field = 3, type = 'string'}},
        unique        = false,
        if_not_exists = true
    })

    return data, tm
end

--- The spaces one worker instance owns.
--
-- The fixed four, plus the delayed_push message buckets -- one space per peer,
-- named 'pregel_mpool_<instance>_<bucket>' by mpool.lua. Those are discovered
-- rather than listed because their number is the size of the cluster, and they
-- have to be here: compute runs inside the pregel.worker.deliver RPC, with the
-- caller's privileges, so a send_message under delayed_push writes to one of
-- them on the caller's behalf. mpool.new() has already created them by the
-- time worker_new() grants.
local function space_names(name)
    local names = {
        'data_' .. name,
        'topology_mutation_' .. name,
        'pregel_tube_mqueue_first_' .. name,
        'pregel_tube_mqueue_second_' .. name,
    }
    local prefix = 'pregel_mpool_' .. name .. '_'
    for _, tuple in box.space._space.index.name:pairs({prefix},
                                                      {iterator = 'GE'}) do
        local space = tuple.name
        if space:sub(1, #prefix) ~= prefix then
            break
        end
        -- Only the bucket index may follow, so an instance whose name is a
        -- prefix of another one's does not collect its neighbour's spaces.
        if space:sub(#prefix + 1):match('^%d+$') ~= nil then
            table.insert(names, space)
        end
    end
    return names
end

--- Let `user` call this module's RPC entry points, and -- given an instance
-- name -- touch that instance's spaces.
--
-- Two halves, because they are needed at different times. The entry-point
-- names do not depend on any instance, so a bootstrap script can grant them
-- before it knows what it will run; the space privileges cannot be granted
-- before the spaces exist, so worker.new() applies them for every user in
-- options.grant_to.
--
-- Both halves are per-object grants. The 1.6 version handed 'execute' on
-- 'universe' to guest instead, which is every function in the process, and
-- needed it because the RPC was conn:eval.
--
-- A lua_call grant alone is not enough to run a worker: the call executes with
-- the caller's privileges, and the entry points write to the instance's
-- spaces.
local function grant(user, instance_name)
    for _, name in ipairs({RPC_DELIVER, RPC_DELIVER_BATCH, RPC_WAIT}) do
        box.schema.user.grant(user, 'execute', 'lua_call', name,
                              {if_not_exists = true})
    end
    if instance_name == nil then
        return
    end
    for _, space in ipairs(space_names(instance_name)) do
        if box.space[space] ~= nil then
            box.schema.user.grant(user, 'read,write', 'space', space,
                                  {if_not_exists = true})
        end
        -- The primary keys are sequence-backed, and drawing from a sequence
        -- is a privileged operation of its own.
        local sequence = space .. '_seq'
        if box.sequence[sequence] ~= nil then
            box.schema.user.grant(user, 'read,write', 'sequence', sequence,
                                  {if_not_exists = true})
        end
    end
end

-------------------------------------------------------------------------------
-- Construction
-------------------------------------------------------------------------------

--- Create the worker called `name`.
--
-- options.workers        -- array of every worker's net.box URI (required)
-- options.master         -- the master's net.box URI (required)
-- options.compute        -- callable(vertex), the compute function (required)
-- options.obtain_name    -- callable(value) -> vertex name (required)
-- options.combiner       -- callable(a, b) -> c, folds two messages
-- options.squash_only    -- run the combiner once per superstep (default false)
-- options.queue_engine   -- 'space' (default) or 'table'
-- options.pool_size      -- messages per batch (default 1000)
-- options.delayed_push   -- back the mpool buckets with spaces (default false)
-- options.worker_context -- passed through to vertex:get_worker_context()
-- options.worker_preload -- callable(self, preload_args) -> loader, or a loader
-- options.preload_args   -- passed to worker_preload
-- options.user           -- net.box user for the outgoing connections
-- options.password       -- net.box password for the outgoing connections
-- options.connect_async  -- build the message pool without waiting for any
--                           peer; the caller then owns the waiting
--                           (worker.mpool:wait_connected(timeout))
-- options.connect_timeout-- seconds to wait for the peers when not async
-- options.grant_to       -- user, or array of users, allowed to reach this
--                           instance: they get the RPC grants and read/write
--                           on this instance's spaces
--
-- A URI -- options.master and every entry of options.workers -- is either a
-- net.box URI string or a {uri = ..., params = ...} table, the form a
-- Tarantool 3 config uses for a listener with transport parameters.
local function worker_new(name, options)
    assert(type(name) == 'string', 'name must be a string')
    assert(type(options) == 'table', 'options must be a table')

    local worker_uris = options.workers or {}
    local compute     = options.compute
    local combiner    = options.combiner
    local master_uri  = options.master
    local pool_size   = options.pool_size or 1000
    local obtain_name = options.obtain_name
    local wrk_context = options.worker_context

    local is_delayed = options.delayed_push
    if is_delayed == nil then is_delayed = false end

    -- These two used to be read as undeclared globals inside worker_new, so
    -- they were always nil and neither option did anything.
    local squash_only = options.squash_only
    if squash_only == nil then squash_only = false end
    local queue_engine = options.queue_engine or 'space'

    assert(is_callable(obtain_name),     'options.obtain_name must be callable')
    assert(is_callable(compute),         'options.compute must be callable')
    assert(type(combiner) == 'nil' or is_callable(combiner),
           'options.combiner must be callable or "nil"')
    assert(type(master_uri) == 'string' or
           (type(master_uri) == 'table' and type(master_uri.uri) == 'string'),
           'options.master must be a URI string or a {uri = ...} table')
    assert(type(squash_only) == 'boolean',
           'options.squash_only must be boolean or "nil"')
    assert(queue_engine == 'space' or queue_engine == 'table',
           'options.queue_engine must be "space", "table" or "nil"')

    local data_space, tm_space = create_spaces(name)

    local self = setmetatable({
        name           = name,
        workers        = worker_uris,
        master_uri     = master_uri,
        preload_func   = nil,
        mpool          = mpool.new(name, worker_uris, {
            msg_count       = pool_size,
            is_delayed      = is_delayed,
            user            = options.user,
            password        = options.password,
            connect_async   = options.connect_async,
            connect_timeout = options.connect_timeout,
        }),
        aggregators    = {},
        in_progress    = 0,
        obtain_name    = obtain_name,
        worker_context = wrk_context,
        data_space              = data_space,
        topology_mutation_space = tm_space,
    }, worker_mt)

    local preload = options.worker_preload
    if type(preload) == 'function' then
        preload = preload(self, options.preload_args)
    elseif type(preload) ~= 'table' and type(preload) ~= 'nil' then
        error('<worker_preload> expected "function"/"table"/"nil", got "%s"',
              type(preload))
    end
    self.preload_func = preload

    self.mqueue = queue.new('mqueue_first_' .. name, {
        combiner    = combiner,
        squash_only = squash_only,
        engine      = queue_engine
    })
    self.mqueue_next = queue.new('mqueue_second_' .. name, {
        combiner    = combiner,
        squash_only = squash_only,
        engine      = queue_engine
    })
    self.vertex_pool = vertex.pool_new{
        compute = compute,
        pregel  = self
    }

    -- The table form is what carries a listener's transport parameters; net.box
    -- takes them as part of the URI argument and has no `params` option.
    self.master = remote.new(type(master_uri) == 'table' and
                             {uri = master_uri.uri, params = master_uri.params}
                             or master_uri, {
        user            = options.user,
        password        = options.password,
        wait_connected  = false,
        reconnect_after = RECONNECT_AFTER
    })

    self:add_aggregator('__in_progress', {
        internal = true,
        default  = 0,
        merge    = function(old, new) return old + new end,
    }):add_aggregator('__messages', {
        internal = true,
        default  = 0,
        merge    = function(old, new) return old + new end,
    })

    -- Now that every space exists, hand out the privileges that name them.
    local grant_to = options.grant_to
    if type(grant_to) == 'string' then
        grant_to = {grant_to}
    end
    for _, user in ipairs(grant_to or {}) do
        grant(user, name)
    end

    workers[name] = self
    return self
end

--- Publish the RPC entry points on _G, where conn:call() can reach them.
--
-- vshard does the same with _G.vshard. rawset, because the table may already
-- exist: master.lua registers into the same one when both run in a process.
rawset(_G, 'pregel', rawget(_G, 'pregel') or {})
_G.pregel.worker = {
    deliver       = deliver_msg,
    deliver_batch = deliver_batch,
    wait          = wait_ready,
}

return {
    new           = worker_new,
    grant         = grant,
    deliver       = deliver_msg,
    deliver_batch = deliver_batch,
    wait          = wait_ready,
    -- for tests and introspection
    workers       = workers,
}
