#!/usr/bin/env tarantool

local fio = require('fio')
local fun = require('fun')
local log = require('log')
local uri = require('uri')
local yaml = require('yaml')
local fiber = require('fiber')

local queue  = require('pregel.queue')
local vertex = require('pregel.vertex')

local vertex_compute = vertex.vertex_private_methods.compute
local vertex_write_solution = vertex.vertex_private_methods.write_solution

local fmtstring   = string.format
local timeit      = require('pregel.utils').timeit
local xpcall_tb   = require('pregel.utils').xpcall_tb
local is_callable = require('pregel.utils').is_callable
local fiber_pool  = require('pregel.utils.fiber_pool')
local pool        = require('pregel.utils.connpool')

local function pregel_info_worker(name, msg, args)
    if msg == 'snapshot' then
        box.snapshot()
    elseif msg:match('vertex%..*') then
        local msg = msg:match('vertex%.(.*)')
    elseif msg == 'edge' then
        local msg = msg:match('edge%.(.*)')
    elseif msg == 'deliver' then
        --
    else
        fiber.sleep(10)
    end
    return box.cfg.listen
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

    self.server_list = fun.iter(self.pool:all('default')):map(function()
        return srv.conn
    end):totable()
    self.mapping = pregel_worker_by_key(self.server_list)
end

local pregel_worker_methods = {
    start = function(self)
        self.fiber = fiber.create(pregel_worker_loop, self)
    end,
}

local pregel_worker_new = function(name, options)
    -- parse workers
    local workers = options.workers or {}
    local workers_pool = fun.iter(workers):map(function(wrk)
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
        workers = workers,
        pool_cfg = pool_cfg,
    }, {
        __index = pregel_worker_methods
    })

    return self
end

return {
    new = pregel_worker_new,
    find_worker_by_key = pregel_worker_by_key,
    info_worker = pregel_info_worker
}
