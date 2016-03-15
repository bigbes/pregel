local log = require('log')
local yaml = require('yaml')

local tube_list = setmetatable({}, {
    __index = function(self, name)
        if box.space['pregel_tube_' .. name] ~= nil then
            return tube_new(name)
        end
        return nil
    end
})

local tube_mt = {
    take = function(self, receiver, offset)
        offset = offset or 0
        local val = self.stats[receiver]
        if val == nil or val == 0 then
            -- assert(self.space.index.receiver:len{receiver} == 0)
            return nil
        end
        local v = self.space.index.receiver:select(receiver, {
            limit = 1,
            offset = offset
        })[1]
        if v == nil then
            return nil
        end
        return v[3]
    end,
    take_closure = function(self, receiver)
        local offset = 0
        return function()
            offset = offset + 1
            return self:take(receiver, offset - 1)
        end
    end,
    receiver_closure = function(self)
        local last = 0
        return function()
            while true do
                local rv = self.space:select({ last }, {
                    limit = 1,
                    iterator = 'GT'
                })
                if rv[1] == nil then break end
                last = rv[1][1]
                return last
            end
            return nil
        end
    end,
    put = function(self, receiver, message)
        local val = self.stats[receiver]
        self.stats[receiver] = val and val + 1 or 1
        self.space:auto_increment{receiver, message}
    end,
    len = function(self, receiver)
        local val = nil
        if receiver == nil then
            val = self.space:len()
        else
            local val = self.space.index.receiver:len{receiver}
            assert(val == self.stats[receiver])
        end
        return val
    end,
    delete = function(self, receiver)
        if receiver == nil then
            return self:truncate()
        end
        fun.iter(self.space.index.receiver:select{receiver}):map(
            function(tuple) return tuple[1] end
        ):each(
            function(key) self.space:delete(tuple[1]) end
        )
        self.stats[receiver] = nil
    end,
    truncate = function(self)
        self.stats = {}
        self.space:truncate()
    end
}

local function verify_queue(queue)
    log.info('verifying queue')
    for k in queue:receiver_closure() do
        local len = self.space.index.receiver:len{k}
        local cnt = queue.stats[k]
        if len ~= cnt then
            print('%d ~= %d for key "%s"', len, cnt, k)
        end
    end
    for k, v in pairs(queue.stats) do
        local len = self.space.index.receiver:len{k}
        if v ~= k then
            print('%d ~= %d for key "%s"', len, v, k)
        end
    end
    log.info('finished')
end

function tube_new(name)
    local self = setmetatable({
        name = name,
        stats = {}
    }, {
        __index = tube_mt
    })
    box.once('pregel_tube-' .. name, function()
        local space = box.schema.create_space('pregel_tube_' .. name)
        space:create_index('primary' , { type = 'TREE', parts = {1, 'NUM'} })
        space:create_index('receiver', {
            type = 'TREE',
            parts = {2, 'NUM'},
            unique = false
        })
    end)
    self.space = box.space['pregel_tube_' .. name]

    tube_list[name] = self
    return self
end

return {
    verify = verify_queue,
    list = tube_list,
    new = tube_new
}
