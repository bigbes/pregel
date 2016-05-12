#!/usr/bin/env tarantool

local fio = require('fio')
local fun = require('fun')
local log = require('log')
local uri = require('uri')
local yaml = require('yaml')
local fiber = require('fiber')
local digest = require('digest')

local queue  = require('pregel.local_queue')
local vertex = require('pregel.vertex')

local vertex_compute = vertex.vertex_private_methods.compute
local vertex_write_solution = vertex.vertex_private_methods.write_solution

local fmtstring   = string.format
local timeit      = require('pregel.utils').timeit
local xpcall_tb   = require('pregel.utils').xpcall_tb
local is_callable = require('pregel.utils').is_callable
local fiber_pool  = require('pregel.utils.fiber_pool')
local pool        = require('pregel.utils.connpool')

local workers = {}

local pregel_info_functions = setmetatable({
    ['vertex.add'] = function(instance, args)
        return instance:nd_vertex_add(args)
    end,
    ['vertex.store'] = function(instance, args)
        return instance:nd_vertex_store(args)
    end,
    ['edge.store'] = function(instance, args)
        return instance:nd_edge_store(args)
    end,
    ['snapshot'] = function(instance, args)
        return box.snapshot()
    end,
    ['test.deliver'] = function(instance, args)
        fiber.sleep(10)
    end,
    ['unknown'] = function()
        error('unknown operation')
    end
}, {
    __newindex = function(self, k, v)
        return self.unknown
    end
})

local function pregel_info_worker(name, msg, args)
    local instance = workers[name]
    assert(instance)
    local op = pregel_info_functions[msg]
    op(instance, args)
    return 1
end

local function pregel_info_worker_batch(name, msgs)
    local instance = workers[name]
    assert(instance)
    local cnt = 0
    for _, msg in ipairs(msgs) do
        local message, args = msg[1], msg[2]
        local op = pregel_info_functions[message]
        op(instance, args)
        cnt = cnt + 1
    end
    return cnt
end

local function pregel_pool_on_connected(channel)
    return function()
        channel:put(1)
        while channel:has_readers() do
            channel:put(1)
        end
    end
end

local function pregel_worker_by_key(servers)
    local servers_cnt = #servers
    return function(key)
        key = (type(key) == 'number' and key or digest.crc32(key))
        return servers[1 + digest.guava(key, servers_cnt)]
    end
end

local function pregel_worker_loop(self)
    local ch = fiber.channel(1)
    self.pool.on_connected = pregel_pool_on_connected(ch)
    self.pool:init(self.pool_cfg)
    local rv = ch:get()
    assert(rv == 1, 'bad object returned, expect "1", got "' .. tostring(rv) .. '"')
    -- everyone is connected now

    self.server_list = fun.iter(self.pool:all('default')):map(function(srv)
        return srv.conn
    end):totable()
    self.mapping = pregel_worker_by_key(self.server_list)
end

local pregel_worker_methods = {
    nd_vertex_add = function(self, args)
        local id, name, value = unpack(args)
        local tuple = self.space:get(id)
        if tuple == nil or tuple[3] == nil then
            local edges = tuple and tuple[5] or {}
            tuple = {id, false, name, value, edges}
        end
    end,
    nd_vertex_store = function(self, args)
        local id, name, value = unpack(args)
        self.space:replace{id, false, name, value, {}}
    end,
    nd_edge_store = function(self, args)
        local id = table.remove(args, 1)
        local tuple = self.space:get(id)
        if tuple == nil then
            tuple = {id, false, yaml.NULL, yaml.NULL, {}}
        else
            tuple = tuple:totable()
        end
        tuple[5] = fun.chain(tuple[5], args):totable()
        self.space:replace(tuple)
    end,
    start = function(self)
        self.fiber = fiber.create(pregel_worker_loop, self)
    end,
}

local pregel_worker_new = function(name, options)
    -- parse workers
    worker_list = options.workers or {}
    local workers_pool = fun.iter(worker_list):map(function(wrk)
        local wrk = uri.parse(wrk)
        return {
            uri = wrk.host .. ':' .. wrk.service,
            password = wrk.password,
            login = wrk.login,
            zone = 'default'
        }
    end):totable()

    local pool_cfg = {
        pool_name = name,
        servers = workers_pool,
        monitor = false
    }

    local self = setmetatable({
        name = name,
        pool = pool.new(),
        workers = workers_list,
        pool_cfg = pool_cfg,
    }, {
        __index = pregel_worker_methods
    })

    box.once('pregel_load-' .. name, function()
        local space = box.schema.create_space('data_' .. name)
        space:create_index('primary', {
            type = 'TREE',
            parts = {1, 'NUM'}
        })
        self.space = space
    end)

    workers[name] = self
    return self
end

return {
    new = pregel_worker_new,
    find_worker_by_key = pregel_worker_by_key,
    info_worker = pregel_info_worker,
    deliver = pregel_info_worker,
    deliver_batch = pregel_info_worker_batch,
}
