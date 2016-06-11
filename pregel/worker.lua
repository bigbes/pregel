#!/usr/bin/env tarantool

local fio = require('fio')
local fun = require('fun')
local log = require('log')
local uri = require('uri')
local json = require('json')
local yaml = require('yaml')
local fiber = require('fiber')
local digest = require('digest')
local remote = require('net.box')

local fmtstring   = string.format

local queue      = require('pregel.queue')
local vertex     = require('pregel.vertex')
local aggregator = require('pregel.aggregator')
local mpool      = require('pregel.mpool')

local timeit      = require('pregel.utils').timeit
local xpcall_tb   = require('pregel.utils').xpcall_tb
local is_callable = require('pregel.utils').is_callable
local error       = require('pregel.utils').error

local vertex_compute        = vertex.vertex_private_methods.compute
local vertex_write_solution = vertex.vertex_private_methods.write_solution

local workers = {}

local RECONNECT_AFTER = 0.5

local info_functions = setmetatable({
    ['vertex.store']      = function(instance, args)
        return instance:nd_vertex_store(args)
    end,
    ['edge.store']        = function(instance, args)
        return instance:nd_edge_store(args[1], args[2])
    end,
    ['snapshot']          = function(instance, args)
        return box.snapshot()
    end,
    ['message.deliver']   = function(instance, args)
        -- args[1] - sent to
        -- args[2] - sent value
        -- args[3] - sent from
        return instance.mqueue_next:put(args[1], args[2], args[3])
    end,
    ['aggregator.inform'] = function(instance, args)
        -- args[1] - aggregator name
        -- args[2] - aggregator new value
        instance.aggregators[args[1]].value = args[2]
    end,
    ['superstep.before']  = function(instance)
        return instance:before_superstep()
    end,
    ['superstep']         = function(instance, args)
        return instance:run_superstep(args)
    end,
    ['superstep.after']   = function(instance, args)
        return instance:after_superstep(args)
    end,
    ['test.deliver']      = function(instance, args)
        fiber.sleep(10)
    end,
    ['count']             = function(instance, args)
        instance.in_progress = 0
        instance.data_space:pairs():each(function(tuple)
            if tuple[2] == false then
                instance.in_progress = instance.in_progress + 1
            end
        end)
    end,
    ['preload']          = function(instance)
        instance:preload()
    end
}, {
    __index = function(self, op)
        return function(k)
            error('unknown operation: %s', op)
        end
    end
})

local function deliver_msg(name, msg, args)
    if msg == 'wait' then
        while workers[name] == nil do
            fiber.yield()
        end
        workers[name].master:wait_connected()
    else
        local rv = {xpcall_tb(function()
            local op = info_functions[msg]
            local instance = workers[name]
            assert(instance, 'no instance found')
            return op(instance, args)
        end)}
        local status = table.remove(rv, 1)
        if status == false then
            local errmsg = tostring(rv[1])
            error(errmsg)
        end
        return unpack(rv)
    end
end

local function deliver_batch(name, msgs)
    local stat, err = xpcall_tb(function()
        local instance = workers[name]
        assert(instance)
        for _, msg in ipairs(msgs) do
            local op = info_functions[msg[1]](instance, msg[2])
        end
        return #msgs
    end)
    if stat == false then
        error(tostring(err))
    end
end

local worker_mt = {
    __index = {
        run_superstep = function(self, superstep)
            local function tuple_filter(tuple)
                local id, halt = tuple:unpack(1, 2)
                return not (self.mqueue:len(id) == 0 and halt == true)
            end
            local function tuple_process(tuple)
                local vertex_object = self.vertex_pool:pop(tuple)
                vertex_object.superstep = superstep
                vertex_object:vote_halt(false)
                local rv = vertex_compute(vertex_object)
                self.mqueue:delete(vertex_object.__id)
                self.vertex_pool:push(vertex_object)
                return rv
            end

            log.info('starting superstep %d', superstep)

            self.data_space:pairs()
                           :filter(tuple_filter)
                           :each(tuple_process)

            -- can't reach, for now
            while self.vertex_pool.count > 0 do
                fiber.yield()
            end

            self.mpool:flush()

            log.info('ending superstep %d', superstep)
            return 'ok'
        end,
        after_superstep = function(self)
            -- swap message queues
            local tmp = self.mqueue
            self.mqueue = self.mqueue_next
            self.mqueue_next = tmp
            -- TODO: if mqueue_next is not empty, then execute callback on messages
            -- self.mqueue_next:truncate()

            local len = self.mqueue_next:len()
            if len > 0 then
                log.info('left %d messages', len)
                self.mqueue_next.space:pairs():enumerate():each(function(id, tuple)
                    log.info('msg %d: %s', id, json.encode(tuple))
                end)
            end

            -- update internal aggregator values
            self.aggregators['__in_progress'](self.in_progress)
            self.aggregators['__messages'](self.mqueue:len())

            log.info('%d messages in mqueue', self.mqueue:len())

            for k, v in pairs(self.aggregators) do
                v:inform_master()
            end

            -- TODO: send aggregator's (local) data back to master
            return 'ok'
        end,
        nd_vertex_store = function(self, vertex)
            local id = self.obtain_name(vertex)
            self.data_space:replace{id, false, vertex, {}}
        end,
        nd_edge_store = function(self, from, edges)
            local tuple = self.data_space:get(from)
            assert(tuple, 'absence of vertex')
            tuple = tuple:totable()
            tuple[5] = fun.chain(tuple[5], edges):totable()
            self.data_space:replace(tuple)
        end,
        add_aggregator = function(self, name, opts)
            assert(self.aggregators[name] == nil)
            self.aggregators[name] = aggregator.new(name, self, opts)
            return self
        end,
        preload = function(self)
            self.preload_func(self.mpool.self_idx, self.mpool.bucket_cnt)
            self.mpool:flush()
        end
    }
}

-- obtain_name is a function, that'll convert value to key (string)
local worker_new = function(name, options)
    -- parse workers
    local worker_uris = options.workers or {}
    local compute     = options.compute
    local combiner    = options.combiner
    local master_uri  = options.master
    local pool_size   = options.pool_size or 1000
    local obtain_name = options.obtain_name
    local is_delayed  = options.delayed_push
    if is_delayed == nil then
        is_delayed = false
    end

    assert(is_callable(obtain_name),     'options.obtain_name must be callable')
    assert(is_callable(compute),         'options.compute must be callable')
    assert(type(combiner) == 'nil' or is_callable(combiner),
           'options.combiner must be callable or "nil"')
    assert(type(master_uri) == 'string', 'options.master must be string')

    local self = setmetatable({
        name         = name,
        workers      = worker_uris,
        master_uri   = master_uri,
        preload_func = nil,
        mpool        = mpool.new(name, worker_uris, {
            msg_count  = pool_size,
            is_delayed = is_delayed
        }),
        aggregators  = {},
        in_progress  = 0,
        obtain_name  = obtain_name
    }, worker_mt)

    local preload = options.worker_preload
    if type(preload) == 'function' then
        preload = preload(self, options.preload_args)
    elseif type(preload) ~= 'table' then
        assert(false,
            string.format('<preload> expected "function"/"table", got %s',
                          type(options.master_preload))
        )
    end
    self.preload_func = preload

    box.session.su('guest')
    box.once('pregel_load-' .. name, function()
        local space = box.schema.create_space('data_' .. name)
        space:create_index('primary', {
            type = 'TREE',
            parts = {1, 'STR'}
        })
    end)

    self.mqueue = queue.new('mqueue_first_' .. name, {
        combiner    = combiner,
        squash_only = squash_only,
        engine      = tube_engine
    })
    self.mqueue_next = queue.new('mqueue_second_' .. name, {
        combiner    = combiner,
        squash_only = squash_only,
        engine      = tube_engine
    })
    self.vertex_pool = vertex.pool_new{
        compute = compute,
        pregel = self
    }
    box.session.su('admin')
    self.data_space  = box.space['data_' .. name]
    self.master = remote.new(master_uri, {
        wait_connected  = false,
        reconnect_after = RECONNECT_AFTER
    })
    self:add_aggregator('__in_progress', {
        internal = true,
        default  = 0,
        merge    = function(old, new)
            return old + new
        end,
    }):add_aggregator('__messages', {
        internal = true,
        default  = 0,
        merge    = function(old, new)
            return old + new
        end,
    })

    workers[name] = self
    return self
end

return {
    new           = worker_new,
    deliver       = deliver_msg,
    deliver_batch = deliver_batch,
}
