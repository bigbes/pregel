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

local timeit                = require('pregel.utils').timeit
local xpcall_tb             = require('pregel.utils').xpcall_tb
local is_callable           = require('pregel.utils').is_callable
local error                 = require('pregel.utils').error
local execute_authorized_mr = require('pregel.utils').execute_authorized_mr

local vertex_compute        = vertex.vertex_private_methods.compute
local vertex_write_solution = vertex.vertex_private_methods.write_solution

local workers = {}

local RECONNECT_AFTER = 5

local TOPMT_EDGE_DELETE   = 0
local TOPMT_VERTEX_DELETE = 1
local TOPMT_VERTEX_STORE  = 2
local TOPMT_EDGE_STORE    = 3

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
    ['snapshot'] = function(instance, args)
        return box.snapshot()
    end,
    ['message.deliver'] = function(instance, args)
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
    ['superstep.before'] = function(instance)
        return instance:before_superstep()
    end,
    ['superstep'] = function(instance, args)
        return instance:run_superstep(args)
    end,
    ['superstep.after'] = function(instance, args)
        return instance:after_superstep(args)
    end,
    ['test.deliver'] = function(instance, args)
        fiber.sleep(10)
    end,
    ['count'] = function(instance, args)
        instance.in_progress = instance.data_space:pairs():reduce(count_active, 0)
        log.info('<count> Found %d active vertices', instance.in_progress)
    end,
    ['preload'] = function(instance)
        instance:preload()
    end
}, {
    __index = function(self, op)
        return function(k)
            error('unknown message type: %s', op)
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
            local n_count = 0

            local function tuple_filter(tuple)
                local id, halt = tuple:unpack(1, 2)
                return not (self.mqueue:len(id) == 0 and halt == true)
            end

            local function tuple_process(acc, tuple)
                if acc % 1000 == 0 then
                    fiber.yield()
                end
                if acc % 10000 == 0 then
                    log.info('Processed %d/%d vertices', acc,
                             self.data_space:len())
                end
                local vertex_object = self.vertex_pool:pop(tuple)
                vertex_object.__superstep = superstep
                vertex_object:vote_halt(false)
                local rv = vertex_compute(vertex_object)
                self.mqueue:delete(vertex_object.__id)
                self.vertex_pool:push(vertex_object)
                return acc + 1
            end

            log.info('starting superstep %d', superstep)

            self.data_space:pairs()
                           :filter(tuple_filter)
                           :reduce(tuple_process, 0)

            -- can't reach, for now
            while self.vertex_pool.count > 0 do
                fiber.yield()
            end

            self.mpool:flush()

            log.info('ending superstep %d', superstep)
            return 'ok'
        end,
        after_superstep = function(self)
            -- SWAP MESSAGE QUEUES
            local tmp = self.mqueue
            self.mqueue = self.mqueue_next
            self.mqueue_next = tmp

            -- TODO: if mqueue_next is not empty, then execute callback on messages
            local len = self.mqueue_next:len()
            if len > 0 then
                log.info('left %d messages', len)
                self.mqueue_next.space:pairs():enumerate():each(function(id, tuple)
                    log.info('msg %d: %s', id, json.encode(tuple))
                end)
            end
            self.mqueue_next:truncate()

            -- TOPOLOGY MUTATION
            local tmspace   = self.topology_mutation_space
            local tmspacein = self.topology_mutation_space.index.name
            local last_key  = nil
            log.info('<topology mutation> stats:')
            log.info('<topology mutation, del_edge>   %d tasks', tmspacein:count(TOPMT_EDGE_DELETE))
            log.info('<topology mutation, del_vertex> %d tasks', tmspacein:count(TOPMT_VERTEX_DELETE))
            log.info('<topology mutation, add_vertex> %d tasks', tmspacein:count(TOPMT_VERTEX_STORE))
            log.info('<topology mutation, add_edge>   %d tasks', tmspacein:count(TOPMT_EDGE_STORE))
            -- first part  - delete edges
            -- TODO: conflict resolving if we can't find edge
            last_key = nil
            while true do
                local first = tmspace:select(last_key, {limit=1, iterator='GT'})
                if first[1] == nil or first[1][2] ~= TOPMT_EDGE_DELETE then
                    break
                end
                local src = first[1][3]
                last_key = {TOPMT_EDGE_DELETE, src}
                first = tmspace:select(last_key)
                local edge_list = self.data_space:get{src}[4]
                for idx, tuple in ipairs(first) do
                    local idx, tmtype, src, dest = tuple:unpack()
                    local is_deleted = false
                    for idx, edge in ipairs(edge_list) do
                        if edge[1] == dest then
                            log.info("<topology mutation, del_edge> edge '%s'->'%s': deleted", src, dest)
                            table.remove(edge_list, idx)
                            is_deleted = true
                            break
                        end
                    end
                    if not is_deleted then
                        log.info("<topology mutation, del_edge> edge '%s'->'%s': not exists", src, dest)
                    end
                    tmspace:delete(idx)
                end
                self.data_space:update(src, {{'=', 4, edge_list}})
            end
            -- second part - delete vertices
            -- TODO: conflict resolving if we can't find vertex
            local to_delete = {}
            for _, tuple in tmspacein:pairs{TOPMT_VERTEX_DELETE} do
                local idx, tmtype, vertex_name = tuple:unpack()
                local rv = self.data_space:delete{vertex_name}
                if rv == nil then
                    log.error("<topology mutation, del_vertex> vertex '%s': deleted", vertex_name)
                    if rv[2] == false then
                        self.in_progress = self.in_progress - 1
                    end
                else
                    log.error("<topology mutation, del_vertex> vertex '%s': not exists", vertex_name)
                end
                table.insert(to_delete, idx)
            end
            while true do
                local idx = table.remove(to_delete)
                if idx == nil then break end
                tmspace:delete(idx)
            end
            -- third part - add vertices
            -- optimization - add all edges (that needed to be added to those
            -- vertices) at the same time
            -- TODO: conflict resolving if vertex is already presented
            for _, tuple in tmspacein:pairs{TOPMT_VERTEX_STORE} do
                local idx, tmtype, vertex_name, vertex = tuple:unpack()
                local edges = {}
                local edge_list = tmspacein:select{TOPMT_EDGE_STORE, vertex_name}
                for _, tuple in ipairs(edge_list) do
                    table.insert(edges, {tuple:unpack(4, 5)})
                    table.insert(to_delete, tuple[1])
                end
                if self.data_space:get{vertex_name} ~= nil then
                    log.info("<topology mutation, add_vertex> vertex '%s': exists", vertex_name)
                else
                    self.data_space:replace{vertex_name, false, vertex, edges}
                    log.info("<topology mutation, add_vertex> vertex '%s': added", vertex_name)
                    self.in_progress = self.in_progress + 1
                end
                table.insert(to_delete, idx)
            end
            while true do
                local idx = table.remove(to_delete)
                if idx == nil then break end
                tmspace:delete(idx)
            end
            -- fourth part - add edges
            -- TODO: conflict resolving if edge is already in list of edges
            last_key = nil
            while true do
                local first = tmspace:select(last_key, {limit=1, iterator='GT'})
                if first[1] == nil then
                    break
                end
                assert(first[1][2] == TOPMT_EDGE_STORE)
                local src = first[1][3]
                last_key = {TOPMT_EDGE_STORE, src}
                local edge_list = tmspace:select(last_key)
                local vertex = self.data_space:get{src}
                if vertex == nil then
                    log.info("<topology mutation, add_edge> edge '%s'->'*': vertex '%s' doesn't exists", src, src)
                else
                    local edges = {}
                    for _, tuple in ipairs(edge_list) do
                        local dest, value = tuple:unpack(4, 5)
                        log.info("<topology mutation, add_edge> edge '%s'->'%s': added", src, dest)
                        table.insert(edges, {dest, value})
                    end
                    edge_list = fun.iter(vertex[4]):chain(edges):totable()
                    self.data_space:update(src, {{'=', 4, edge_list}})
                end
            end
            tmspace:truncate()

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
        add_aggregator = function(self, name, opts)
            assert(self.aggregators[name] == nil)
            self.aggregators[name] = aggregator.new(name, self, opts)
            return self
        end,
        preload = function(self)
            self.preload_func(self.mpool.self_idx, self.mpool.bucket_cnt)
            self.mpool:flush()
        end,
        vertex_store = function(self, vertex)
            local id = self.obtain_name(vertex)
            self.data_space:replace{id, false, vertex, {}}
        end,
        edge_store = function(self, from, edges)
            local tuple = self.data_space:get(from)
            assert(tuple, 'absence of vertex')
            tuple = tuple:totable()
            tuple[4] = fun.chain(tuple[4], edges):totable()
            self.data_space:replace(tuple)
        end,
        vertex_store_delayed = function(self, vertex)
            log.info('got vertex store')
            self.topology_mutation_space:auto_increment{
                TOPMT_VERTEX_STORE, self.obtain_name(vertex), vertex
            }
        end,
        edge_store_delayed = function(self, src, dest, value)
            self.topology_mutation_space:auto_increment{
                TOPMT_EDGE_STORE, src, dest, value
            }
        end,
        vertex_delete_delayed = function(self, vertex_name)
            self.topology_mutation_space:auto_increment{
                TOPMT_VERTEX_DELETE, 2, vertex_name
            }
        end,
        edge_delete_delayed = function(self, src, dest)
            self.topology_mutation_space:auto_increment{
                TOPMT_EDGE_DELETE, src, dest
            }
        end,
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
    local wrk_context = options.worker_context
    if is_delayed == nil then
        is_delayed = false
    end

    assert(is_callable(obtain_name),     'options.obtain_name must be callable')
    assert(is_callable(compute),         'options.compute must be callable')
    assert(type(combiner) == 'nil' or is_callable(combiner),
           'options.combiner must be callable or "nil"')
    assert(type(master_uri) == 'string', 'options.master must be string')

    local self = setmetatable({
        name           = name,
        workers        = worker_uris,
        master_uri     = master_uri,
        preload_func   = nil,
        mpool          = mpool.new(name, worker_uris, {
            msg_count    = pool_size,
            is_delayed   = is_delayed
        }),
        aggregators    = {},
        in_progress    = 0,
        obtain_name    = obtain_name,
        worker_context = wrk_context
    }, worker_mt)

    local preload = options.worker_preload
    if     type(preload) == 'function' then
        preload = preload(self, options.preload_args)
    elseif type(preload) ~= 'table' and
           type(preload) ~= 'nil'   then
        assert(false,
            ('<worker_preload> expected "function"/"table"/"nil", got "%s"'):format(
                type(preload)
            )
        )
    end
    self.preload_func = preload

    execute_authorized_mr('guest', function()
        box.once('pregel_load-' .. name, function()
            local space = box.schema.create_space('data_' .. name, {
                format = {
                    [1] = {name = 'id',        type = 'str'  },
                    [2] = {name = 'is_halted', type = 'bool' },
                    [3] = {name = 'value',     type = '*'    },
                    [4] = {name = 'edges',     type = 'array'}
                }
            })
            space:create_index('primary', {
                type = 'TREE',
                parts = {1, 'STR'},
            })
        end)
        box.once('pregel_tm-' .. name, function()
            local space = box.schema.create_space('topology_mutation_' .. name, {
                format = {
                    [1] = {name = 'id',    type = 'num'},
                    [2] = {name = 'type',  type = 'num'},
                    [3] = {name = 'name',  type = 'str'},
                    [4] = {name = 'dest',  type = '*'  },
                    [5] = {name = 'value', type = '*'  },
                }
            })
            space:create_index('primary', {
                type  = 'TREE',
                parts = {1, 'NUM'}
            })
            space:create_index('name', {
                type   = 'TREE',
                parts  = {2, 'NUM', 3, 'STR'},
                unique = false
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
    end)

    self.data_space              = box.space['data_' .. name]
    assert(self.data_space ~= nil)
    self.topology_mutation_space = box.space['topology_mutation_' .. name]
    assert(self.topology_mutation_space ~= nil)

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
