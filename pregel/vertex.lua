local fun  = require('fun')
local log  = require('log')
local json = require('json')

local vertex_mt = {
    --[[-- INTERNAL API --]]--
    __apply = function(self, tuple)
        self.__modified = false
        self.__id, self.__halt, self.__value, self.__edges = tuple:unpack()
        return self
    end,
    __compute = function(self)
        if type(self.__compute_func) ~= 'function' then
            error('No compute function is provided')
        end
        self:__compute_func()
        if self.__modified then
            self.__pregel.space:replace{
                self.__id, self.__halt, self.__value, self.__edges
            }
        end
        return self.__modified
    end,
    __write_solution = function(self)
        -- ??
    end,
    --[[
    -- PUBLIC API:
    -- * self:vote_halt
    -- * self:pairs_edges
    -- * self:pairs_messages
    -- * self:send_message
    -- * self:get_value
    -- * self:set_value
    -- * self:get_id
    --]]
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
        return fun.wrap(pairs(self.__edges))
    end,
    pairs_messages = function(self)
        return self.__pregel.msg_in:pairs(self.__id)
    end,
    send_message = function(self, receiver, msg)
        return self.__pregel.msg_out:put(receiver, msg)
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
}

local function vertex_new()
    local self = setmetatable({
        id = 0,
        superstep = 0,
        -- can't access from inside
        __modified = false,
        __halt = false,
        __edges = nil,
        __value = 0,
        -- assigned once per vertex
        __pregel = nil,
        __compute = nil
    }, {
        __index = vertex_mt
    })
    return self
end

local vertex_pool_mt = {
    pop = function(self, tuple)
        assert(self.cnt >= 0)
        self.cnt = self.cnt + 1
        local vl = table.remove(self.list)
        if vl == nil then
            vl = vertex_new()
            vl.__compute_func = self.compute
            vl.__pregel = self.pregel
        else
            self.len = self.len - 1
        end
        return vl:__apply(tuple)
    end,
    push = function(self, vertex)
        self.cnt = self.cnt - 1
        assert(self.cnt >= 0)
        if self.len == self.max then
            return
        end
        table.insert(self.list, vertex)
        self.len = self.len + 1
    end
}

local function vertex_pool_new(cfg)
    cfg = cfg or {}
    return setmetatable({
        cnt = 0,
        len = 0,
        list = {},
        max = 100,
        pregel = cfg.pregel,
        compute = cfg.compute
    }, {
        __index = vertex_pool_mt
    })
end

return {
    new = vertex_new,
    pool_new = vertex_pool_new
}
