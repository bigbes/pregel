local fun  = require('fun')
local log  = require('log')
local json = require('json')

--[[--
-- Tuple structure:
-- <id>    - string <TODO: maybe it's better to give ability to user decide>
-- <halt>  - <MP_BOOL>
-- <value> - <MP_ANY>
-- <edges> - <MP_ARRAY of <'id', MP_ANY>>
--]]--
local vertex_private_methods = {
    apply = function(self, tuple)
        self.__modified = false
        self.__id, self.__halt, self.__value, self.__edges = tuple:unpack()
        return self
    end,
    compute = function(self)
        self:__compute_func()
        if self.__modified then
            self.__pregel.data_space:replace{
                self.__id, self.__halt, self.__value, self.__edges
            }
        end
        return self.__modified
    end,
    write_solution = function(self)
        return self:__write_solution_func()
    end,
}

local vertex_methods = {
    --[[--
    -- | Base API
    -- * self:vote_halt       ([is_halted = true])
    -- * self:pairs_edges     ()
    -- * self:pairs_messages  ()
    -- * self:send_message    (receiver_id, value)
    -- * self:get_value       ()
    -- * self:set_value       (value)
    -- * self:get_id          ()
    -- * self:get_superstep   ()
    -- * self:get_aggragation (name)
    -- * self:set_aggregation (name, value)
    --]]--
    vote_halt = function(self, is_halted)
        if is_halted == nil then is_halted = true end
        if self.__halt ~= is_halted then
            self.__modified = true
            self.__halt = is_halted
            self.__pregel.in_progress = self.__pregel.in_progress + (is_halted and -1 or 1)
        end
    end,
    pairs_edges = function(self)
        local last = 0
        return function(pos)
            last = last + 1
            local edge = self.__edges[last]
            if edge == nil then
                return nil
            end
            return last, edge[1], edge[2]
        end
    end,
    pairs_messages = function(self)
        return self.__pregel.mqueue:pairs(self.__id)
    end,
    send_message = function(self, receiver, msg)
        self.__pregel.mpool:by_id(receiver):put(
                'message.deliver',
                {receiver, msg}
        )
    end,
    get_value = function(self)
        return self.__value
    end,
    set_value = function(self, new)
        self.__modified = true
        self.__value = new
    end,
    get_id = function(self)
        return self.__id
    end,
    get_superstep = function(self)
        return self.__superstep
    end,
    get_aggregation = function(self, name)
        return self.__pregel(name)
    end,
    set_aggregation = function(self, name, value)
       self.__pregel(name, value)
    end,
    --[[--
    -- | Topology mutation API
    -- -- where src/dest may be id/name.
    -- * self:add_vertex     (name, value)
    -- * self:add_edge       ([src = self:get_id(), ]dest, value)
    -- * self:delete_vertex  (src[, vertices = true])
    -- * self:delete_edge    ([src = self:get_id(), ]dest)
    --]]--
    --[[--
    add_vertex = function(self, value)
    end,
    add_edge = function(self, src, dest, value)
    end,
    delete_vertex = function(self, vertice, edges)
    end,
    delete_edge = function(self, src, dest)
    end
    --]]--
}

local function vertex_new()
    local self = setmetatable({
        -- can't access from inside
        __superstep           = 0,
        __id                  = 0,
        __modified            = false,
        __halt                = false,
        __edges               = nil,
        __value               = 0,
        -- assigned once per vertex
        __pregel              = nil,
        __compute_func        = nil,
        __write_solution_func = nil
    }, {
        __index = vertex_methods
    })
    return self
end

local vertex_pool_methods = {
    pop = function(self, tuple)
        assert(self.count >= 0)
        self.count = self.count + 1
        local vl = table.remove(self.container)
        if vl == nil then
            vl = vertex_new()
            vl.__compute_func = self.compute
            vl.__write_solution_func = self.write_solution
            vl.__pregel = self.pregel
        end
        return vertex_private_methods.apply(vl, tuple)
    end,
    push = function(self, vertex)
        self.count = self.count - 1
        assert(self.count >= 0)
        if #self.container == self.maximum_count then
            return
        end
        table.insert(self.container, vertex)
    end
}

local function vertex_pool_new(cfg)
    cfg = cfg or {}
    return setmetatable({
        count          = 0,
        container      = {},
        maximum_count  = 100,
        pregel         = cfg.pregel,
        compute        = cfg.compute,
        write_solution = cfg.write_solution
    }, {
        __index = vertex_pool_methods
    })
end

return {
    vertex_private_methods = vertex_private_methods,
    new = vertex_new,
    pool_new = vertex_pool_new
}
