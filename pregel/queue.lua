local log = require('log')
local fun = require('fun')
local yaml = require('yaml')
local json = require('json')

local collections = require('pregel.utils.collections')
local is_callable = require('pregel.utils').is_callable
local error = require('pregel.utils').error

local tube_list

local tube_space_methods = {
    pairs = function(self, receiver)
        assert(receiver ~= nil)
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
        if self.combiner ~= nil and self.squash_only == false then
            local rv = message
            for _, msg in self:pairs(receiver) do
                rv = self.combiner(rv, msg)
            end
            self:delete(receiver)
            message = rv
        end
        self.stats[receiver] = self.stats[receiver] + 1
        return self.space:auto_increment{receiver, message}
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
        self.space:drop()
        self.name = nil
        self.stats = nil
        setmetatable(self, nil)
    end,
    squash = function(self)
        if self.combiner ~= nil and self.squash_only == true then
            for receiver in self:receiver_closure() do
                local rv = nil
                for _, v in self:pairs(receiver) do
                    if rv == nil then
                        rv = v
                    else
                        rv = self.combiner(rv, v)
                    end
                end
                self:delete(receiver)
                self:put(receiver, rv)
            end
        end
    end
}

local tube_table_methods = {
    pairs = function(self, receiver)
        assert(receiver ~= nil)
        return pairs(self.container[receiver])
    end,
    receiver_closure = function(self)
        local fun, param, state = pairs(self.container)
        return function()
            local key, val = fun(param, state)
            if val == nil then
                return nil
            end
            return key
        end
    end,
    put = function(self, receiver, message)
        assert(receiver ~= nil)
        assert(message  ~= nil)

        if self.combiner ~= nil and self.squash_only == false then
            local rv = message
            for _, msg in self:pairs(receiver) do
                rv = self.combiner(rv, msg)
            end
            self:delete(receiver)
            message = rv
        end
        self.stats[receiver] = self.stats[receiver] + 1
        table.insert(self.container[receiver], message)
        return message
    end,
    len = function(self, receiver)
        local len = 0
        if receiver == nil then
            for k, v in pairs(self.container) do
                len = len + #v
            end
        else
            len = #(self.container[receiver] or {})
        end
        return len
    end,
    delete = function(self, receiver)
        if receiver == nil then
            self.container = collections.defaultdict(function(key)
                return {}
            end)
            self.stats = collections.defaultdict(0)
        else
            self.container[receiver] = nil
            self.stats[receiver] = nil
        end
    end,
    truncate = function(self)
        self:delete()
    end,
    drop = function(self)
        tube_list[ self.name ] = nil
        self.name = nil
        self.stats = nil
        setmetatable(self, nil)
    end,
    squash = function(self)
        if self.combiner ~= nil and self.squash_only == true then
            for receiver in self:receiver_closure() do
                local rv = nil
                for _, v in self:pairs(receiver) do
                    if rv == nil then
                        rv = v
                    else
                        rv = self.combiner(rv, v)
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
    assert(type(options) == 'nil' or type(options) == 'table',
           'options must be "table" or "nil"')
    options = options or {}

    local combiner = options.combiner
    assert(type(combiner) == 'nil' or is_callable(combiner),
           'options.combiner must be callable or "nil"')

    local squash_only = options.squash_only
    assert(type(squash_only) == 'nil' or type(squash_only) == 'boolean',
           'options.squash_only must be boolean or "nil"')
    if squash_only == nil then squash_only = false end

    local engine = options.engine or 'space'
    assert(type(engine) == 'string', 'options.engine must be "string" or "nil"')

    local self = rawget(tube_list, name)
    if self == nil then
        self = {
            name        = name,
            engine      = engine,
            combiner    = combiner,
            squash_only = squash_only,
            stats       = collections.defaultdict(0)
        }

        if engine == 'table' then
            self.container = collections.defaultdict(function(key)
                return {}
            end)
            self = setmetatable(self, { __index = tube_table_methods })
        elseif engine == 'space' then
            local space = box.space['pregel_tube_' .. name]

            if space == nil then
                space = box.schema.create_space('pregel_tube_' .. name)
                space:create_index('primary' , {
                    type = 'TREE',
                    parts = {1, 'NUM'}
                })
                space:create_index('receiver', {
                    type = 'TREE',
                    parts = {2, 'STR'},
                    unique = false
                })
            else
                space:pairs():each(function(tuple)
                    local _, rcvr, msg = tuple:unpack()
                    self.stats[rcvr] = self.stats[rcvr] + 1
                end)
            end
            self.space = space
            self = setmetatable(self, { __index = tube_space_methods })
        else
            assert(false)
        end

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
