local fun  = require('fun')
local log  = require('log')
local json = require('json')

local vertex_private_methods = {
    apply = function(self, tuple)
        self.__modified = false
        self.__name     = nil
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
    -- * self:vote_halt      ([val = true])
    -- * self:pairs_edges    ()
    -- * self:pairs_messages ()
    -- * self:send_message   (neighb, val)
    -- * self:get_value      ()
    -- * self:set_value      (val)
    -- * self:get_id         ()
    --]]--
    vote_halt = function(self, val)
        if val == nil then
            val = true
        end
        if self.__halt == false and val == true then
            self.__modified = true
            self.__halt     = true
            self.__pregel.in_progress = self.__pregel.in_progress - 1
        elseif self.__halt == true and val == false then
            self.__modified = true
            self.__halt     = false
            self.__pregel.in_progress = self.__pregel.in_progress + 1
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
                {receiver, msg, self.__id}
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
        if self.__name == nil then
            self.__name = self.__pregel.obtain_name(
                self.__pregel.name_space:get(self.__id)
            )
        end
        return self.__id, self.__name
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
    add_vertex = function(self, name, value)
        local id = self.__pregel.free_id + 1
        if id == self.__pregel.free_id_max + 1 then
            local min, max = self.__pregel:eval(
                'return require("pregel.master").deliver(...)',
                'aggregator.get_next_range', nil
            )
            self.__pregel.free_id = min
            self.__pregel.free_id_max = max
        end
        self.__pregel.free_id = id
        self.__pregel.mpool:by_id(id):put('vertex.add', {id, name, value})
    end,
    append_edge = function(self, src, dest, value)
        if type(src) == 'string'  then assert(false) end
        if value == nil then
            value = dest
            dest = src
            table.insert(self.__edges, {dest, value})
        end
        if type(dest) == 'string' then assert(false) end
        self.__pregel.mpool:by_id(src):put('edge.append', {src, dest, value})
    end,
    store_edge = function(self, src, dest, value)
        if type(src) == 'string'  then assert(false) end
        if value == nil then
            value = dest
            dest = src
            local changed = false
            for k, edge in ipairs(self.__edges) do
                if edge[1] == dest then
                    edge[2] = value
                    changed = true
                    break
                end
            end
            if not changed then
                table.insert(self.__edges, {dest, value})
            end
        end
        if type(dest) == 'string' then assert(false) end
        self.__pregel.mpool:by_id(src):put('edge.store', {src, dest, value})
    end,
    delete_vertex = function(self, vertice, edges)
        if edges == 'nil' and vertice == 'nil'
        if type(vertice) == 'string' then assert(false) end
        self.__pregel.mpool:by_id(id):put('vertex.delete', {id, name, value})
    end,
    delete_edge = function(self, src, dest)
        if type(src) == 'string'  then assert(false) end
        if dest == nil then
            dest = src
            local pos = {}
            for k, edge in ipairs(self.__edges) do
                if edge[1] == dest then
                    table.insert(pos, k)
                end
            end
            for _, dest in ipairs(pos) do
                table.remove(self.__edges, dest)
            end
        end
        if type(dest) == 'string' then assert(false) end
        self.__pregel.mpool:by_id(src):put('edge.delete', {src, dest, value})
    end
    --]]--
}

local function vertex_new()
    local self = setmetatable({
        superstep      = 0,
        -- can't access from inside
        __id           = 0,
        __name         = nil,
        __modified     = false,
        __halt         = false,
        __edges        = nil,
        __value        = 0,
        -- assigned once per vertex
        __pregel       = nil,
        __compute_func = nil,
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
