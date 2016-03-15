local fun = require('fun')

vertex_mt = {
    apply = function(self, tuple)
        tuple:pairs():enumerate():each(
            function(k, v)
                if k == 1 then
                    self.id = v
                elseif k == 2 then
                    self.__halt = v
                elseif k == 3 then
                    self.value = v
                elseif k == 4 then
                    self.__edges = v
                else
                    assert('unreacheable')
                end
            end
        )
        return self
    end,
    set_halt = function(self)
        if self.__halt ~= 1 then
            self.__halt = 1
            self.__pregel.in_progress = self.__pregel.in_progress - 1
        end
    end,
    pairs_edges = function(self)
        return fun.iter(self.__edges)
    end,
    pairs_messages = function(self)
        return self.__pregel.msg_in:take_closure(self.id)
    end,
    send_message = function(self, send_to, msg)
        self.__pregel.msg_out:put(send_to, msg)
    end,
    compute = function(self)
        if type(self.__compute) ~= 'function' then
            error('no compute function')
        end
        self:__compute()
        self.__pregel.space:replace{
            self.id, self.__halt, self.value, self.__edges
        }
--         self.__pregel.tempspace:replace{
--             self.id, self.__halt, self.value, self.__edges
--         }
        self.__pregel.vertex_pool:push(self)
    end,
}

local function vertex_new()
    local self = setmetatable({
        superstep = 0,
        id = 0,
        value = 0,
        -- can't access from inside
        __halt = false,
        __edges = 1,
        -- assigned once per vertex
        __pregel = 0,
        __compute = 0
    }, {
        __index = vertex_mt,
        __newindex = function() error("Can't set value to vertex", 2) end
    })
    return self
end

local vertex_pool_mt = {
    pop = function(self)
        assert(self.cnt >= 0)
        self.cnt = self.cnt + 1
        local vl = table.remove(self.list)
        if vl == nil then
            vl = vertex_new()
            vl.__compute = self.compute
            vl.__pregel = self.pregel
        else
            self.len = self.len - 1
        end
        return vl
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
