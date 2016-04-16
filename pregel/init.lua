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
        --
        -- -- run through all nodes that aren't halted
        -- self.space.index.halt:pairs{0}:enumerate():each(function(k, tuple)
        --     local vertex_object = self.vertex_pool:pop():apply(tuple)
        --     vertex_object.superstep = superstep
        --     -- fiber.create(vertex_object.compute, vertex_object)
        --     vertex_object:compute()
        -- end
        log.info('scanning %d objects', self.space:len())

        local function tuple_filter(tuple)
            local id, halt = tuple:unpack(1, 2)
            -- if (self.msg_in:len(id) > 0 or halt == false) then
            --     print('tuple_filter: ', self.msg_in:len(id), halt)
            -- end
            return not (self.msg_in:len(id) == 0 and halt == true)
        end

        local function tuple_process(tuple)
            local vertex_object = self.vertex_pool:pop(tuple)
            vertex_object.superstep = self.superstep
            vertex_object:vote_halt(false)
            -- fiber.create(vertex_object.compute, vertex_object)
            local rv = vertex_object:__compute()
            self.msg_in:delete(vertex_object.__id)
            return rv
        end

        local function count_true(acc, val)
            if val ~= nil then
                if val == true then acc[1] = acc[1] + 1 end
                acc[2] = acc[2] + 1
            end
            return acc
        end

        local acc = {0, 0}

        acc = self.space:pairs():filter(tuple_filter):map(tuple_process):reduce(count_true, acc)

        print('accum result', acc[1], acc[2])

        if self.vertex_pool.cnt > 0 then
            fiber.sleep(0)
        end
    end,
    run = function(self)
        while true do
            -- queue.verify(self.msg_out)
            self:run_superstep()
            log.info('stat: in_progress == %d', self.in_progress)
            log.info('stat:   out_queue == %d', self.msg_out:len())
            log.info('stat:    in_queue == %d', self.msg_in:len())
            self.msg_in:truncate()
            if self.in_progress == 0 and self.msg_out:len() == 0 then
                break
            end
            -- swap message queues
            local tmp = self.msg_out
            self.msg_out = self.msg_in
            self.msg_in = tmp
            -- done
            log.info('superstep %d is finished', self.superstep)
            self.superstep = self.superstep + 1
        end
        log.info('superstep %d is finished', self.superstep)
    end
}

local pregel_new = function(name, compute, aggregator)
    if type(compute) ~= 'function' then
        error('Bad compute function', 2)
    end
    if type(aggregator) ~= 'function' and type(aggregator) ~= 'nil' then
        error('Bad aggregator function', 2)
    end
    local self = setmetatable({
        fiber = fiber.self(),
        superstep = 1,
        compute = compute,
        in_progress = 0
    }, {
        __index = pregel_mt
    })
    box.once('pregel_load-' .. name, function()
        local space = box.schema.create_space('data_' .. name)
        space:create_index('primary', {
            type = 'HASH',
            parts = {1, 'NUM'}
        })

        preload_file(space)
        box.snapshot()
    end)
    self.vertex_pool = vertex.pool_new{
        compute = compute,
        pregel = self
    }

    self.msg_in  = queue.new('msg_in_'   .. name, aggregator)
    self.msg_out = queue.new('msg_out_'  .. name, aggregator)
    self.space   = box.space['data_' .. name]

    self.space:pairs():each(function(tuple)
        if tuple[2] == false then
            self.in_progress = self.in_progress + 1
        end
    end)

    return self
end

return {
    new = pregel_new
}
