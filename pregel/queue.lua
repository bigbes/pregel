--- Per-receiver message queues.
--
-- A queue holds messages addressed to vertex names. Two engines implement the
-- same interface:
--
--   * 'space' -- a Tarantool space, so the queue survives a restart and can
--     hold more messages than fit in a Lua table;
--   * 'table' -- a plain Lua table, for a run that never needs the messages
--     after the process exits.
--
-- Interface: put, pairs, len, delete, truncate, drop, squash, receiver_closure.
--
-- A `combiner` folds several messages for one receiver into one. It runs
-- either on every put (the default) or once per superstep from squash(), when
-- `squash_only` is set -- combining on put costs a read of the receiver's
-- messages per put, which is the wrong trade when a receiver gets many.

local log = require('log')

local collections = require('pregel.utils.collections')
local utils       = require('pregel.utils')
local is_callable = utils.is_callable
local error       = utils.error

local fmtstring = string.format

local SPACE_PREFIX = 'pregel_tube_'

local tube_list

local function space_name_of(name)
    return SPACE_PREFIX .. name
end

--- Number of messages stats claims for `receiver`, without materialising it.
--
-- stats is a defaultdict, so a plain read would create the counter and make
-- the receiver visible to every later pairs() over stats.
local function stat_count(self, receiver)
    return rawget(self.stats, receiver) or 0
end

-------------------------------------------------------------------------------
-- 'space' engine
-------------------------------------------------------------------------------

local tube_space_methods = {
    pairs = function(self, receiver)
        assert(receiver ~= nil, 'receiver is nil')
        local gen, param, state = self.space.index.receiver:pairs({receiver})
        return function()
            local tuple
            state, tuple = gen(param, state)
            if tuple == nil then
                return nil
            end
            return state, tuple[3]
        end
    end,
    receiver_closure = function(self)
        -- GT on the partial key {receiver} skips every message of that
        -- receiver at once, so this walks distinct receivers, not messages.
        local last = nil
        return function()
            local tuple = self.space.index.receiver:select(last, {
                limit = 1,
                iterator = 'GT'
            })[1]
            if tuple == nil then
                return nil
            end
            last = {tuple[2]}
            return tuple[2]
        end
    end,
    put = function(self, receiver, message)
        assert(receiver ~= nil, 'receiver is nil')
        assert(message ~= nil, 'message is nil')
        if self.combiner ~= nil and self.squash_only == false then
            local rv = message
            for _, msg in self:pairs(receiver) do
                rv = self.combiner(rv, msg)
            end
            self:delete(receiver)
            message = rv
        end
        self.stats[receiver] = self.stats[receiver] + 1
        self.space:insert{box.NULL, receiver, message}
        return message
    end,
    len = function(self, receiver)
        if receiver == nil then
            return self.space:len()
        end
        return self.space.index.receiver:count({receiver})
    end,
    delete = function(self, receiver)
        if receiver == nil then
            self.space:truncate()
            self.stats = collections.defaultdict(0)
            return
        end
        -- select() returns a snapshot, so deleting while walking it is safe.
        for _, tuple in ipairs(self.space.index.receiver:select({receiver})) do
            self.space:delete{tuple[1]}
        end
        self.stats[receiver] = nil
    end,
    drop = function(self)
        self.space:drop()
    end,
}

-------------------------------------------------------------------------------
-- 'table' engine
-------------------------------------------------------------------------------

local tube_table_methods = {
    pairs = function(self, receiver)
        assert(receiver ~= nil, 'receiver is nil')
        -- rawget, not container[receiver]: reading must not create a bucket,
        -- or every vertex the worker polls would become a live receiver.
        local messages = rawget(self.container, receiver)
        if messages == nil then
            return function() return nil end
        end
        return pairs(messages)
    end,
    receiver_closure = function(self)
        local gen, param, state = pairs(self.container)
        return function()
            local value
            state, value = gen(param, state)
            if value == nil then
                return nil
            end
            return state
        end
    end,
    put = function(self, receiver, message)
        assert(receiver ~= nil, 'receiver is nil')
        assert(message ~= nil, 'message is nil')
        if self.combiner ~= nil and self.squash_only == false then
            local rv = message
            for _, msg in self:pairs(receiver) do
                rv = self.combiner(rv, msg)
            end
            self:delete(receiver)
            message = rv
        end
        self.stats[receiver] = self.stats[receiver] + 1
        local messages = rawget(self.container, receiver)
        if messages == nil then
            messages = {}
            self.container[receiver] = messages
        end
        table.insert(messages, message)
        return message
    end,
    len = function(self, receiver)
        if receiver ~= nil then
            local messages = rawget(self.container, receiver)
            return messages == nil and 0 or #messages
        end
        local len = 0
        for _, messages in pairs(self.container) do
            len = len + #messages
        end
        return len
    end,
    delete = function(self, receiver)
        if receiver == nil then
            self.container = {}
            self.stats = collections.defaultdict(0)
            return
        end
        self.container[receiver] = nil
        self.stats[receiver] = nil
    end,
    drop = function(self)
        self.container = nil
    end,
}

-------------------------------------------------------------------------------
-- Shared
-------------------------------------------------------------------------------

local tube_common_methods = {
    truncate = function(self)
        return self:delete()
    end,
    --- Fold every receiver's messages down to one with the combiner.
    --
    -- Only does anything in squash_only mode; otherwise put() has already
    -- combined and there is nothing left to fold.
    squash = function(self)
        if self.combiner == nil or self.squash_only == false then
            return
        end
        -- Collect the receivers before touching any of them: squash deletes
        -- and re-puts each one, and mutating a Lua table underneath its own
        -- pairs() iterator is undefined.
        local receivers = {}
        for receiver in self:receiver_closure() do
            table.insert(receivers, receiver)
        end
        for _, receiver in ipairs(receivers) do
            local rv = nil
            for _, message in self:pairs(receiver) do
                if rv == nil then
                    rv = message
                else
                    rv = self.combiner(rv, message)
                end
            end
            self:delete(receiver)
            if rv ~= nil then
                self:put(receiver, rv)
            end
        end
    end,
}

--- Detach the queue: it can no longer be used, and queue.new() will build a
-- fresh one under the same name.
local function tube_drop(self)
    tube_list[self.name] = nil
    self:__drop()
    self.name = nil
    self.stats = nil
    self.space = nil
    setmetatable(self, nil)
end

local function methods_of(engine_methods)
    local result = {}
    for k, v in pairs(tube_common_methods) do result[k] = v end
    for k, v in pairs(engine_methods) do result[k] = v end
    result.__drop = result.drop
    result.drop = tube_drop
    return result
end

local tube_space_mt = { __index = methods_of(tube_space_methods) }
local tube_table_mt = { __index = methods_of(tube_table_methods) }

--- Cross-check the per-receiver counters against what the queue actually holds.
--
-- Returns `true, {}` when they agree and `false, {problem, ...}` when they do
-- not; every problem is also logged.
local function verify_queue(queue)
    local problems = {}
    local seen = {}
    local function check(receiver)
        if seen[receiver] then
            return
        end
        seen[receiver] = true
        local len = queue:len(receiver)
        local cnt = stat_count(queue, receiver)
        if len ~= cnt then
            table.insert(problems, fmtstring(
                'receiver %q: queue holds %d message(s), stats says %d',
                tostring(receiver), len, cnt))
        end
    end

    for receiver in queue:receiver_closure() do
        check(receiver)
    end
    for receiver in pairs(queue.stats) do
        check(receiver)
    end

    for _, problem in ipairs(problems) do
        log.error('<queue verify, %s> %s', tostring(queue.name), problem)
    end
    return #problems == 0, problems
end

--- Open (or create) the queue called `name`.
--
-- options.combiner    -- callable(a, b) -> c, folds two messages into one
-- options.squash_only -- run the combiner from squash() only (default false)
-- options.engine      -- 'space' (default) or 'table'
--
-- A queue that already exists is returned as it is, which is what makes
-- queue.list a cache rather than a factory -- but only when the second call
-- asks for the same options. Asking for different ones used to be a cache hit
-- too, silently: a worker restarted in place with another combiner went on
-- computing with the old one.
local function tube_new(name, options)
    assert(type(name) == 'string', 'queue name must be a string')
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
    assert(engine == 'space' or engine == 'table',
           'options.engine must be "space", "table" or "nil"')

    local self = rawget(tube_list, name)
    if self ~= nil then
        local mismatch
        if self.engine ~= engine then
            mismatch = string.format('engine %q, the existing one has %q',
                                     engine, self.engine)
        elseif self.combiner ~= combiner then
            mismatch = 'a different combiner'
        elseif self.squash_only ~= squash_only then
            mismatch = string.format(
                'squash_only %s, the existing one has %s',
                tostring(squash_only), tostring(self.squash_only))
        end
        if mismatch ~= nil then
            error("queue '%s' is already open with other options: asked for %s",
                  name, mismatch)
        end
        return self
    end

    self = {
        name        = name,
        engine      = engine,
        combiner    = combiner,
        squash_only = squash_only,
        stats       = collections.defaultdict(0)
    }

    if engine == 'table' then
        self.container = {}
        setmetatable(self, tube_table_mt)
    else
        local space_name = space_name_of(name)
        local existed = box.space[space_name] ~= nil
        local space = box.schema.space.create(space_name, {
            if_not_exists = true,
            format = {
                {name = 'id',       type = 'unsigned'},
                {name = 'receiver', type = 'string'  },
                {name = 'message',  type = 'any'     },
            }
        })
        -- space:auto_increment() is gone in Tarantool 3; the primary key is
        -- filled by a sequence instead, and box.NULL in field 1 draws from it.
        space:create_index('primary', {
            type          = 'TREE',
            parts         = {{field = 1, type = 'unsigned'}},
            sequence      = true,
            if_not_exists = true
        })
        space:create_index('receiver', {
            type          = 'TREE',
            parts         = {{field = 2, type = 'string'}},
            unique        = false,
            if_not_exists = true
        })
        self.space = space
        setmetatable(self, tube_space_mt)
        if existed then
            -- Rebuild the counters from what survived the restart.
            for _, tuple in space:pairs() do
                self.stats[tuple[2]] = self.stats[tuple[2]] + 1
            end
        end
    end

    tube_list[name] = self
    return self
end

-- Reading a name whose space already exists adopts it, with default options.
tube_list = setmetatable({}, {
    __index = function(_, name)
        if type(name) == 'string' and box.space[space_name_of(name)] ~= nil then
            return tube_new(name)
        end
        return nil
    end
})

return {
    verify = verify_queue,
    list   = tube_list,
    new    = tube_new
}
