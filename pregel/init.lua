#!/usr/bin/env tarantool

local fio = require('fio')
local fun = require('fun')
local log = require('log')
local yaml = require('yaml')
local fiber = require('fiber')

local queue  = require('pregel.queue')
local vertex = require('pregel.vertex')

local fmtstring = string.format
local xpcall_tb = require('pregel.utils').xpcall_tb

local pregel_mt = {
    run_superstep = function(self)
        -- -- run through all nodes with messages
        -- fun.wrap(self.msg_in:receiver_closure()):each(function(k)
        --     local tuple = self.space:select{k}[1]
        --     if tuple == nil then
        --         return
        --     end
        --     local vertex_object = self.vertex_pool:pop():apply(tuple)
        --     vertex_object.superstep = superstep
        --     -- fiber.create(vertex_object.compute, vertex_object)
        --     xpcall_tb(vertex_object.compute, vertex_object)
        -- end)
        -- -- run through all nodes that aren't halted
        -- self.space.index.halt:pairs{0}:enumerate():each(function(k, tuple)
        --     local vertex_object = self.vertex_pool:pop():apply(tuple)
        --     vertex_object.superstep = superstep
        --     -- fiber.create(vertex_object.compute, vertex_object)
        --     vertex_object:compute()
        -- end)
        log.info('scanning %d objects', self.space:len())
        self.space:pairs():enumerate():each(function(k, tuple)
            if tuple[3] == 0 and self.msg_in:len{tuple[1]} == 0 then
                return
            end
            local vertex_object = self.vertex_pool:pop():apply(tuple)
            vertex_object.superstep = self.superstep
            -- fiber.create(vertex_object.compute, vertex_object)
            vertex_object:compute()
        end)
        if self.vertex_pool.cnt > 0 then
            fiber.sleep(0)
        end
        -- self.tempspace:pairs{}:enumerate():each(function(k, tuple)
        --     self.space:replace(tuple)
        -- end)
        -- self.tempspace:truncate()
    end,
    run = function(self)
        while true do
            queue.verify(self.msg_out)
            self:run_superstep()
            self.msg_in:truncate()
            log.info('stat: in_progress == %d', self.in_progress)
            log.info('stat:   out_queue == %d', self.msg_out:len())
            log.info('stat:    in_queue == %d', self.msg_in:len())
            if self.in_progress == 0 and self.msg_out:len() == 0 then
                break
            end
            -- swap message queues
            local tmp = self.msg_out
            self.msg_out = self.msg_in
            self.msg_in = tmp
            -- move all from tempspace to space
            -- done
            log.info('superstep %d is finished', self.superstep)
            self.superstep = self.superstep + 1
        end
        log.info('superstep %d is finished', self.superstep)
    end
}

local pregel_new = function(name, compute)
    if type(compute) ~= 'function' then
        error('Bad compute function', 2)
    end
    local self = setmetatable({
        fiber = fiber.self(),
        superstep = 1,
        compute = compute,
    }, {
        __index = pregel_mt
    })
    box.once('pregel_load-' .. name, function()
        local space = box.schema.create_space('data_' .. name)
        space:create_index('primary', {
            type = 'HASH',
            parts = {1, 'NUM'}
        })
        space:create_index('halt', {
            type = 'TREE',
            parts = {2, 'NUM'},
            unique = false
        })

        queue.new('msg_in_'  .. name) --, { temporary = true })
        queue.new('msg_out_' .. name) --, { temporary = true })
        preload_file(space)
        box.snapshot()
    end)
    self.vertex_pool = vertex.pool_new{
        compute = compute,
        pregel = self
    }

    self.msg_in  = queue.list['msg_in_' .. name]
    assert(self.msg_in ~= nil)
    self.msg_out = queue.list['msg_out_' .. name]
    assert(self.msg_out ~= nil)
    self.space   = box.space['data_' .. name]

    self.tempspace = box.schema.create_space('data_temporary_' .. name, {
        temporary = true
    })
    self.tempspace:create_index('primary', {
        type = HASH,
        parts = {1, 'NUM'}
    })

    log.info('%d', self.space:len())
    self.in_progress = self.space:len()

    return self
end

return {
    new = pregel_new
}
