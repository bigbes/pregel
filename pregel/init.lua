#!/usr/bin/env tarantool

local fio = require('fio')
local fun = require('fun')
local log = require('log')
local uri = require('uri')
local yaml = require('yaml')
local fiber = require('fiber')

local queue  = require('pregel.local_queue')
local vertex = require('pregel.vertex')

local vertex_compute = vertex.vertex_private_methods.compute
local vertex_write_solution = vertex.vertex_private_methods.write_solution

local fmtstring   = string.format
local timeit      = require('pregel.utils').timeit
local xpcall_tb   = require('pregel.utils').xpcall_tb
local is_callable = require('pregel.utils').is_callable
local strict = require('pregel.utils.strict')

local pregel_methods = {
    run_superstep = function(self)
        local function tuple_filter(tuple)
            local id, halt = tuple:unpack(1, 2)
            return not (self.msg_in:len(id) == 0 and halt == true)
        end

        local function tuple_process(tuple)
            local vertex_object = self.vertex_pool:pop(tuple)
            vertex_object.superstep = self.superstep
            vertex_object:vote_halt(false)
            local rv = vertex_compute(vertex_object)
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

        local acc = self.space:pairs():filter(tuple_filter)
                                      :map(tuple_process)
                                      :reduce(count_true, {0, 0})

        log.info('stat:          processed == %d', acc[2])

        if self.vertex_pool.count > 0 then
            fiber.sleep(0)
        end
    end,

    write_solution = function(self, filter)
        if type(filter) == 'nil' then
            filter = function()
                return true
            end
        elseif not is_callable(filter) then
            filter = function(key)
                return (key == filter)
            end
        end

        local function tuple_write_solution(tuple)
            local vertex_object = self.vertex_pool:pop(tuple)
            vertex_object.superstep = self.superstep
            return vertex_write_solution(vertex_object)
        end

        local acc = self.space:pairs():filter(filter):each(tuple_write_solution)
    end,

    run = function(self)
        log.info('stat:      overall count == %d', self.space:len())

        while true do
            log.info('-------------------------------------------------')
            log.info('stat:    begin superstep (%d)',  self.superstep)
            log.info('stat:          step time == %f', timeit(self.run_superstep, self))
            log.info('stat:        in_progress == %d', self.in_progress)
            log.info('stat:          out_queue == %d', self.msg_out:len())
            log.info('stat:           in_queue == %d', self.msg_in:len())
            log.info('stat:      truncate time == %f', timeit(self.msg_in.truncate, self.msg_in))
            log.info('stat:        squash time == %f', timeit(self.msg_out.squash, self.msg_out))
            log.info('stat:   squash_out_queue == %d', self.msg_out:len())
            log.info('stat:  truncate_in_queue == %d', self.msg_in:len())
            log.info('stat: finished superstep (%d)',  self.superstep)
            if self.in_progress == 0 and self.msg_out:len() == 0 then
                break
            end
            -- swap message queues
            local tmp = self.msg_out
            self.msg_out = self.msg_in
            self.msg_in = tmp
            -- done
            self.superstep = self.superstep + 1
        end
        log.info('-------------------------------------------------')
    end
}

local pregel_new = function(name, options)
    assert(type(options) == 'nil' or type(options) == 'table',
           'options must be "table" or "nil"')
    options = options or {}

    local compute        = options.compute
    local preload        = options.preload
    local combiner       = options.combiner
    local write_solution = options.write_solution
    local squash_only    = options.squash_only
    local tube_engine    = options.tube_engine
    assert(is_callable(compute), 'options.compute must be callable')
    assert(is_callable(preload), 'options.preload must be callable')
    assert(type(combiner) == 'nil' or is_callable(combiner),
           'options.combiner must be callable or "nil"')
    assert(type(write_solution) == 'nil' or is_callable(write_solution),
           'options.write_solution must be callable or "nil"')
    assert(type(squash_only) == 'nil' or type(squash_only) == 'boolean',
           'options.squash_only must be "boolean" or "nil"')
    assert(type(tube_engine) == 'nil' or type(tube_engine) == 'string',
           'options.tube_engine must be "string" or "nil"')

    local self = setmetatable({
        fiber       = fiber.self(),
        superstep   = 1,
        compute     = compute,
        in_progress = 0,
        -- just in case
        combiner    = combiner,
        preload     = preload,
        squash_only = squash_only,
        tube_engine = tube_engine
    }, {
        __index = pregel_methods
    })
    box.once('pregel_load-' .. name, function()
        local space = box.schema.create_space('data_' .. name)
        space:create_index('primary', {
            type = 'HASH',
            parts = {1, 'NUM'}
        })

        local rv = timeit(preload, space)
        log.info('stat: preload_file_time == %f', rv)
        box.snapshot()
    end)
    self.vertex_pool = vertex.pool_new{
        compute = compute,
        pregel = self
    }

    self.msg_in  = queue.new('msg_in_'   .. name, {
        combiner    = combiner,
        squash_only = squash_only,
        engine      = tube_engine
    })
    self.msg_out = queue.new('msg_out_'  .. name, {
        combiner    = combiner,
        squash_only = squash_only,
        engine      = tube_engine
    })
    self.space   = box.space['data_' .. name]

    self.space:pairs():each(function(tuple)
        if tuple[2] == false then
            self.in_progress = self.in_progress + 1
        end
    end)

    return self
end

return strict.strictify({
    new = pregel_new
})
