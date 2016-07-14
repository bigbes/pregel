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
        self.__id, self.__halt, self.__value, self.__edges = tuple:unpack(1, 4)
        -- self.__id, self.__halt = tuple:unpack(1, 2)
        -- self.__tuple = tuple
        -- self.__value = nil
        -- self.__edges = nil
        return self
    end,
    compute = function(self)
        self:__compute_func()
        if self.__modified or
           #self.__edges_add > 0 or
           #self.__edges_del > 0 then
            while true do
                local edge = table.remove(self.__edges_del)
                if edge == nil then
                    break
                end
                local idx_to_rm = {}
                for idx, val in ipairs(self.__edges) do
                    if val[1] == edge then
                        table.insert(idx_to_rm, idx)
                    end
                end
                for k = #idx_to_rm, 1 do
                    table.remove(self.__edges, idx_to_rm[k])
                end
            end
            while true do
                local edge = table.remove(self.__edges_add)
                if edge == nil then
                    break
                end
                table.insert(self.__edges)
            end
            self.__pregel.data_space:replace{
                self.__id, self.__halt, self.__value, self.__edges
            }
        end
        return self.__modified
    end,
    write_solution = function(self)
        return self:__write_solution_func()
    end,
    add_edge = function(self, dest, value)
        table.insert(self.__edges_add, {dest, value})
    end,
    delete_edge = function(self, dest)
       table.insert(self.__edges_del, dest)
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
    -- * self:get_name        ()
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
        -- if self.__edges == nil then
        --     self.__edges = self.__tuple[4]
        -- end
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
        -- if self.__value == nil then
        --     self.__value = self.__tuple[3]
        -- end
        return self.__value
    end,
    set_value = function(self, new)
        self.__modified = true
        self.__value = new
    end,
    get_name = function(self)
        return self.__id
    end,
    get_superstep = function(self)
        return self.__superstep
    end,
    get_aggregation = function(self, name)
        return self.__pregel.aggregators[name]()
    end,
    set_aggregation = function(self, name, value)
        return self.__pregel.aggregators[name](value)
    end,
    --[[--
    -- | Topology mutation API
    -- * self:add_vertex     (value)
    -- * self:add_edge       ([src = self:get_name(), ]dest, value)
    -- * self:delete_vertex  ([src][, vertices = true])
    -- * self:delete_edge    ([src = self:get_name(), ]dest)
    --]]--
    add_vertex = function(self, value)
        assert(value ~= nil, 'value is nil')
        local name = self.__pregel.obtain_name(value)
        self.__pregel.mpool:by_id(name):put(
                'vertex.store.delayed',
                value
        )
    end,
    add_edge = function(self, src, dest, value)
        local delayed = true
        if value == nil then
            delayed = false
            value = dest
            dest  = src
            src   = self:get_name()
        end
        if value == nil then
            value = json.NULL
        end
        assert(src ~= nil,   'unreachable')
        assert(dest ~= nil,  'destination is nil')
        assert(value ~= nil, 'unreachable')
        if delayed and src == self:get_name() then
            delayed = false
        end
        if not delayed or src == self:get_name() then
            vertex_private_methods.add_edge(self, dest, value)
        else
            self.__pregel.mpool:by_id(src):put(
                    'edge.store.delayed',
                    {dest, value}
            )
        end
    end,
    -- deleting of all input edges is NIY
    -- only complex version can be implemented (full scan)
    delete_vertex = function(self, vertex_name, edges)
        if edges == nil then
            edges       = vertex_name
            vertex_name = self:get_name()
        end
        if edges == nil then
            edges = false
        end
        assert(vertex_name ~= nil, 'unreachable')
        assert(edges ~= nil,  'unreachable')
        -- TODO: !!!!!
        assert(edges == false, 'for now')
        self.__pregel.mpool:by_id(vertex_name):put(
                'vertex.delete.delayed',
                {vertex_name, edges}
        )
    end,
    delete_edge = function(self, src, dest)
        local delayed = true
        if dest == nil then
            delayed = false
            dest = src
            src = self:get_name()
        end
        assert(src ~= nil,  'unreachable')
        assert(dest ~= nil, 'destination is nil')
        if not delayed or src == self:get_name() then
            vertex_private_methods.delete_edge(self, dest)
        else
            self.__pregel.mpool:by_id(src):put(
                    'edge.delete.delayed',
                    {src, dest}
            )
        end
    end,
    get_worker_context = function(self)
        return self.__pregel.worker_context
    end
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
        __write_solution_func = nil,
        __edges_del           = {},
        __edges_add           = {},
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
    pool_new = vertex_pool_new,
    vertex_methods = vertex_methods,
}
