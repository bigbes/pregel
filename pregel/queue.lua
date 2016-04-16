local log = require('log')
local fun = require('fun')
local yaml = require('yaml')

local collections = require('pregel.utils.collections')
local error = require('pregel.utils').error

local tube_list

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
    pairs = function(self, receiver)
        local fun, param, state = self.space.index.receiver:pairs(receiver)
        return function()
            local key, val = fun(param, state)
            if val == nil then
                return nil
            end
            return key, val[3]
        end
    end,
    receiver_closure = function(self)
        local last = 0
        return function()
            while true do
                local rv = self.space.index.receiver:select({ last }, {
                    limit = 1,
                    iterator = 'GT'
                })
                if rv[1] == nil then break end
                last = rv[1][2]
                return last
            end
            return nil
        end
    end,
    put = function(self, receiver, message)
        assert(receiver ~= nil)
        assert(message  ~= nil)
        if self.aggregator ~= nil and self.squash_only == false then
            local rv = message
            for _, msg in self:pairs(receiver) do
                rv = self.aggregator(rv, msg)
            end
            self:delete(receiver)
            message = rv
        end
        self.stats[receiver] = self.stats[receiver] + 1
        local rv = self.space:auto_increment{receiver, message}
        return rv
    end,
    len = function(self, receiver)
        local val = nil
        if receiver == nil then
            val = self.space:len()
        else
            val = self.space.index.receiver:count{receiver}
            assert(val == self.stats[receiver])
        end
        return val
    end,
    delete = function(self, receiver)
        if receiver == nil then
            self.stats = collections.defaultdict(0)
            return self.space:truncate()
        end
        fun.iter(self.space.index.receiver:select{receiver}):map(
            function(tuple) return tuple[1] end
        ):each(
            function(key) self.space:delete(key) end
        )
        self.stats[receiver] = nil
    end,
    truncate = function(self)
        self:delete()
    end,
    drop = function(self)
        tube_list[ self.name ] = nil
        box.space._schema:delete('once' .. 'pregel_tube_' .. self.name)
        self.space:drop()
        self.name = nil
        self.stats = nil
        setmetatable(self, nil)
    end,
    squash = function(self)
        if self.aggregator ~= nil then
            for receiver in self:receiver_closure() do
                local rv = nil
                for _, v in self:pairs(receiver) do
                    if rv == nil then
                        rv = v
                    else
                        rv = self.aggregator(rv, v)
                    end
                end
                self:delete(receiver)
                self:put(receiver, rv)
            end
        end
    end
}

local function verify_queue(queue)
    log.info('verifying queue')
    for k in queue:receiver_closure() do
        local len = queue.space.index.receiver:len{k}
        local cnt = queue.stats[k]
        if len ~= cnt then
            print('%d ~= %d for key "%s"', len, cnt, k)
        end
    end
    for k, v in pairs(queue.stats) do
        local len = queue.space.index.receiver:len{k}
        if v ~= k then
            print('%d ~= %d for key "%s"', len, v, k)
        end
    end
    log.info('finished')
end

local function tube_new(name, options)
    options = options or {}
    local aggregator = options.aggregator
    local squash_only = options.squash_only
    if squash_only == nil then
        squash_only = false
    end

    local self = rawget(tube_list, name)
    if self == nil then
        if type(aggregator) ~= 'function' and type(aggregator) ~= 'nil' then
            error('"aggregator" expected to be a function, got %s', type(aggregator))
        end
        self = setmetatable({
            name = name,
            aggregator = aggregator,
            squash_only = squash_only,
            stats = collections.defaultdict(0)
        }, {
            __index = tube_mt
        })

        local space = box.space['pregel_tube_' .. name]

        if space == nil then
            space = box.schema.create_space('pregel_tube_' .. name)
            space:create_index('primary' , {
                type = 'TREE',
                parts = {1, 'NUM'}
            })
            space:create_index('receiver', {
                type = 'TREE',
                parts = {2, 'NUM'},
                unique = false
            })
        else
            space:pairs():each(function(tuple)
                local _, rcvr, msg = tuple:unpack()
                self.stats[rcvr] = self.stats[rcvr] + 1
            end)
        end

        self.space = space
        tube_list[name] = self
    end
    return self
end

tube_list = setmetatable({}, {
    __index = function(self, name)
        if box.space['pregel_tube_' .. name] ~= nil then
            return tube_new(name)
        end
        return nil
    end
})

return {
    verify = verify_queue,
    list = tube_list,
    new = tube_new
}
