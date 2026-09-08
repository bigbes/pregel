--[[--
-- A streaming percentile counter over a sorted sample.
--
-- Values go in one at a time and are kept in a sorted array; a percentile is
-- read off it by the nearest-rank method,
--
--     index = ceil(p * n / 100), clamped to [1, n]
--
-- so `percentile(100)` is the largest value seen, `percentile(50)` of 1..100
-- is 50, and `percentile(0)` is the smallest rather than an out-of-range read.
--
-- `p * n` before the division on purpose: p and n are usually whole numbers,
-- so the product is exact and the single division rounds once. `p / 100 * n`
-- rounds twice and lands above the integer often enough to matter -- 290 of
-- the 202101 (p, n) pairs with p in 0..100 and n up to 2000, the first being
-- the 7th percentile of 100 samples, where 7 / 100 * 100 is
-- 7.00000000000000088818 and ceil() takes it to rank 8.
--
-- Keeping every value is O(n) memory and an O(n) insert, which is the honest
-- trade for the sizes this is used at: the 2016 calibration phase fed it ten
-- thousand scores and asked for nineteen percentiles once. A P-square
-- estimator would be constant-space but approximate, and its state is five
-- markers per percentile that only mean anything to itself. This state is the
-- sample.
--
-- The state is a plain table -- `{n, window_size, values}` and numbers -- so a
-- counter can be sent to another vertex as a message or parked in a space and
-- brought back with `wrap`.
--
-- Unbounded by default. `window_size` caps the sample; once it is full each
-- new value evicts one drawn at random, as the 2016 counter did.
--
-- The 2016 insert was `table.insert(self.values, new, idx)`, which passes the
-- value where Lua wants the position: it inserted the *index* at the position
-- named by the value, so a fractional or out-of-range score raised "position
-- out of bounds" and everything else silently stored the wrong number in the
-- wrong place. Only the appending branch ever worked, and only for input that
-- happened to arrive in ascending order.
--
-- @module pregel.math.percentile
--]]--

local strict = require('pregel.utils.strict')
local utils  = require('pregel.utils')

local ceil, floor = math.ceil, math.floor

local counter_mt

-- Which element a full window evicts. Injectable so a test can make eviction
-- deterministic, but kept in a weak-keyed side table rather than in the
-- counter, because a function field would make the state unencodable and the
-- whole point of the state is that it can go into a message or a space.
local evictors = setmetatable({}, {__mode = 'k'})

local function evictor_of(self)
    local chosen = evictors[self]
    if chosen == nil then
        return math.random
    end
    return chosen
end

--- Insert one value, keeping the sample sorted.
--
-- @param x a number
-- @return the counter, so calls can be chained
-- @function add
local function add(self, x)
    if type(x) ~= 'number' then
        utils.error('percentile counter got a %s, not a number', type(x))
    end
    local values = self.values

    if self.window_size ~= nil and self.n >= self.window_size then
        table.remove(values, evictor_of(self)(self.n))
        self.n = self.n - 1
    end

    -- Binary search for the first element greater than x, so equal values keep
    -- arrival order among themselves and the array stays sorted.
    local low, high = 1, self.n + 1
    while low < high do
        local mid = floor((low + high) / 2)
        if values[mid] > x then
            high = mid
        else
            low = mid + 1
        end
    end
    table.insert(values, low, x)
    self.n = self.n + 1
    return self
end

--- The p-th percentile of everything added so far.
--
-- @param p a percentage in [0, 100]
-- @return the value at that rank, or nil if nothing has been added
-- @function percentile
local function percentile(self, p)
    if type(p) ~= 'number' or p < 0 or p > 100 then
        utils.error('percentile wants a percentage in [0, 100], got %s',
                    tostring(p))
    end
    if self.n == 0 then
        return nil
    end
    local index = ceil(p * self.n / 100)
    if index < 1 then
        index = 1
    elseif index > self.n then
        index = self.n
    end
    return self.values[index]
end

--- How many values the counter is holding.
--
-- Not how many were added: once the window is full each new value replaces an
-- old one.
--
-- @return number
-- @function count
local function count(self)
    return self.n
end

--- The sample itself, sorted ascending.
--
-- Returned as-is rather than copied: the calibration phase walks it to build
-- buckets and has no reason to pay for a copy. Do not modify it.
--
-- @return the internal array
-- @function sample
local function sample(self)
    return self.values
end

counter_mt = {
    __index = {
        add        = add,
        percentile = percentile,
        count      = count,
        sample     = sample,
    },
}

--- Build a percentile counter.
--
-- @param opts table of `window_size` (nil for unbounded) and `rng_index`, a
--   function of n returning the index in [1, n] a full window evicts
-- @return a counter
-- @function new
local function new(opts)
    opts = opts or {}
    if opts.window_size ~= nil and opts.window_size < 1 then
        utils.error('window_size must be at least 1, got %s',
                    tostring(opts.window_size))
    end
    local counter = setmetatable({
        n           = 0,
        window_size = opts.window_size,
        values      = {},
    }, counter_mt)
    evictors[counter] = opts.rng_index
    return counter
end

--- Re-attach the counter methods to a state that came back from a space.
--
-- The eviction generator is not part of the state, so a counter restored this
-- way falls back to math.random.
--
-- @param state a table of the shape `new()` produces
-- @return the same table, with methods
-- @function wrap
local function wrap(state)
    state.values = state.values or {}
    state.n = state.n or #state.values
    return setmetatable(state, counter_mt)
end

return strict.strictify({
    new  = new,
    wrap = wrap,
})
