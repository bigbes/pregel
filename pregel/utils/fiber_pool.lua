local log = require('log')
local lfun = require('fun')
local yaml = require('yaml')
local fiber = require('fiber')

local defaultdict = require('pregel.utils.collections').defaultdict
local strict = require('pregel.utils.strict')

-- ========================================================================== --
--                              OLD FIBER POOL                                --
-- ========================================================================== --

local fiber_pool_list = defaultdict(function()
    return {}
end)

-- fiber_pool implementation
local function fiber_pool_handler(self, fun)
    fiber.name('fiber_pool/handler')
    while true do
        local task = self.channel_in:get()
        if not task then
            break
        end
        self.in_progress = self.in_progress + 1
        log.debug('enqueue task')
        local status, reason = pcall(fun, task)
        status = status and 'success' or string.format('failed: %s', reason)
        log.debug('dequeue task %s', status)
        self.in_progress = self.in_progress - 1
        if not status then
            self.error = self.error and self.error .. '\n' .. reason or reason
            self.channel_in:close()
            break
        end
    end
    self.channel_out:put(true)
end

local function fiber_pool_wait(self)
    log.debug("fiber_pool.wait(%s): begin", self)

    fiber.yield()

--    while not (self.channel_in:is_closed() or self.channel_in:is_empty()) do
    while not (self.channel_in:is_closed() or self.channel_in:is_empty()) or
            self.in_progress ~= 0 do
        fiber.sleep(0.01)
    end

    while not self.channel_out:is_empty() do
        self.channel_out:get()
    end

    log.debug("fiber_pool.wait(%s): done", self)
end

local function free_fiber_pool(q)
    table.insert(fiber_pool_list[q.fun], q)
end

local function fiber_pool_join(self)
    self:wait()
    if self.error then
        return error(self.error) -- re-throw error
    end
    free_fiber_pool(self)
end

local function fiber_pool_put(self, arg)
    self.channel_in:put(arg)
end

local function fiber_pool_apply(self, arglist)
    for _, arg in ipairs(arglist) do
        self:put(arg)
    end
    return self
end

local function create_fiber_pool(fun, workers)
    -- Start fiber fiber_pool to processes functions in parallel
    local self = setmetatable({
        fun         = fun,
        workers     = workers,
        channel_in  = fiber.channel(workers),
        channel_out = fiber.channel(workers),
        fibers      = nil,
        in_progress  = 0
    }, {
        __index = {
            put   = fiber_pool_put,
            join  = fiber_pool_join,
            wait  = fiber_pool_wait,
            apply = fiber_pool_apply,
        }
    })
    self.fibers = lfun.range(workers):map(function()
        return fiber.create(fiber_pool_handler, self, fun)
    end):totable()
    return self
end

local function new_fiber_pool(fun, workers)
    local list = fiber_pool_list[fun]
    local rv = table.remove(list)
    return rv or create_fiber_pool(fun, workers)
end

-- ========================================================================== --
--                              NEW FIBER POOL                                --
-- ========================================================================== --

--[[--
-- fiber_pool implementation
local function fiber_pool_handler(self, fid)
    fiber.name('fiber_pool/handler')
    while true do
        local task = self.input:get()
        if not task then
            break
        end
        self.in_progress = self.in_progress + 1
        log.debug('enqueue task')
        local status, reason = pcall(self.fun, task)
        status = status and 'success' or string.format('failed: %s', reason)
        log.debug('dequeue task %s', status)
        self.in_progress = self.in_progress - 1
        self.output:put(reason)
        if not status then
            self.error = self.error and self.error .. '\n' .. reason or reason
            self.input:close()
            break
        end
    end
    self.output:put(true)
end

local function fiber_pool_wait(self)
    while true do
        self.input:put(yaml.NULL)
        while self.in_progress > 0 do
            self.output:get()
        end
    end
end

local function fiber_pool_put(self, arg)
    self.input:put(arg)
end

local function fiber_pool_unordered_map(self, arglist)
    local result = {}
    for _, arg in ipairs(arglist) do
        self:put(arg)
        while not self.output:is_empty() do
            table.insert(self.output:get())
        end
    end
    while self.in_progress > 0 do
        table.insert(self.output:get())
    end
    return result
end

local function fiber_pool_apply(self, arglist, cb)
    for _, arg in ipairs(arglist) do
        self:put(arg)
        while not self.output:is_empty() do
            local rv = self.output:get()
            if cb then cb(rv) end
        end
    end
    while self.in_progress > 0 do
        local rv = self.output:get()
        if cb then cb(rv) end
    end
end

local function fiber_pool_kill(self)
end

local function new_fiber_pool(count)
    local self = setmetatable({
        count  = count,
        input  = fiber.channel(0),
        output = fiber.channel(count),
        fibers = nil
    }, {
        put = fiber_pool_put,
        join = fiber_pool_join,
        kill = fiber_pool_kill,
        apply = fiber_pool_apply,
        set_func = fiber_pool_set_func,
        unordered_map = fiber_pool_unordered_map,
        wait_available = fiber_pool_wait_available,
    })

    self.fibers = lfun.range(workers):map(function(fid)
        return fiber.create(fiber_pool_handler, self, fid)
    end):totable()
end
--]]--

-- ========================================================================== --
--                              NEW FIBER POOL                                --
-- ========================================================================== --

return strict.strictify({
     new = new_fiber_pool,
})
