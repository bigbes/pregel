local t = require('luatest')

local percentile = require('pregel.math.percentile')

local g = t.group('math.percentile')

--- Deterministic pseudo-random order, so a shuffled insert is the same shuffle
-- every run.
local function lcg(seed)
    local state = seed
    return function()
        state = (1103515245 * state + 12345) % 2147483648
        return state / 2147483648
    end
end

local function shuffled(n, seed)
    local order = {}
    for i = 1, n do
        order[i] = i
    end
    local rng = lcg(seed)
    for i = n, 2, -1 do
        local j = math.floor(rng() * i) + 1
        order[i], order[j] = order[j], order[i]
    end
    return order
end

local function counter_over(values, opts)
    local counter = percentile.new(opts)
    for _, x in ipairs(values) do
        counter:add(x)
    end
    return counter
end

--
-- The values 1..100
--

g.test_percentiles_of_one_to_hundred = function()
    local counter = percentile.new()
    for i = 1, 100 do
        counter:add(i)
    end
    t.assert_equals(counter:count(), 100)
    -- ceil(p * 100 / 100) = p, so the p-th percentile is the value p.
    t.assert_equals(counter:percentile(50), 50)
    t.assert_equals(counter:percentile(90), 90)
    t.assert_equals(counter:percentile(100), 100)
    t.assert_equals(counter:percentile(1), 1)
    -- Rank 0 is clamped to the first value rather than read as nil, which is
    -- what round(0 * n / 100) gave in the 2016 counter.
    t.assert_equals(counter:percentile(0), 1)
end

g.test_insert_order_does_not_matter = function()
    -- The 2016 insert passed the value where table.insert wants the position,
    -- so only input arriving in ascending order was ever stored correctly.
    local descending = percentile.new()
    for i = 100, 1, -1 do
        descending:add(i)
    end
    t.assert_equals(descending:percentile(50), 50)
    t.assert_equals(descending:percentile(90), 90)
    t.assert_equals(descending:percentile(100), 100)

    local mixed = counter_over(shuffled(100, 20160401))
    t.assert_equals(mixed:count(), 100)
    t.assert_equals(mixed:percentile(50), 50)
    t.assert_equals(mixed:percentile(90), 90)
    t.assert_equals(mixed:percentile(100), 100)
    t.assert_equals(mixed:sample(), descending:sample())
end

g.test_the_sample_stays_sorted = function()
    local counter = counter_over(shuffled(200, 7))
    local values = counter:sample()
    t.assert_equals(#values, 200)
    for i = 2, #values do
        t.assert(values[i - 1] <= values[i],
                 string.format('values[%d] = %s is below values[%d] = %s',
                               i, tostring(values[i]), i - 1,
                               tostring(values[i - 1])))
    end
end

--
-- Nearest rank, on inputs where the arithmetic is not the identity
--

g.test_nearest_rank_on_ten_values = function()
    local counter = counter_over({1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
    t.assert_equals(counter:percentile(50), 5)     -- ceil(5.0)
    t.assert_equals(counter:percentile(55), 6)     -- ceil(5.5)
    t.assert_equals(counter:percentile(95), 10)    -- ceil(9.5)
    t.assert_equals(counter:percentile(25), 3)     -- ceil(2.5)
    t.assert_equals(counter:percentile(10), 1)     -- ceil(1.0)
    t.assert_equals(counter:percentile(100), 10)
end

g.test_the_rank_arithmetic_rounds_once = function()
    -- Measured: 7 / 100 * 100 is 7.00000000000000088818, so `p / 100 * n`
    -- puts the 7th percentile of 100 samples at rank 8. `p * n / 100` is
    -- exact here and rounds once. 7, 14, 28 and 56 are the values of p that
    -- diverge at n = 100.
    local counter = percentile.new()
    for i = 1, 100 do
        counter:add(i)
    end
    for _, p in ipairs({7, 14, 28, 56}) do
        t.assert_equals(counter:percentile(p), p,
                        'percentile ' .. tostring(p))
    end
    -- And the same at the other sample sizes where the two orders disagree.
    local twentyfive = percentile.new()
    for i = 1, 25 do
        twentyfive:add(i)
    end
    t.assert_equals(twentyfive:percentile(28), 7)
    t.assert_equals(twentyfive:percentile(56), 14)
end

g.test_non_integer_and_duplicate_values = function()
    local counter = counter_over({2.5, 2.5, 0.5, -1.25, 2.5, 100})
    t.assert_equals(counter:count(), 6)
    t.assert_equals(counter:sample(), {-1.25, 0.5, 2.5, 2.5, 2.5, 100})
    t.assert_equals(counter:percentile(0), -1.25)
    t.assert_equals(counter:percentile(50), 2.5)
    t.assert_equals(counter:percentile(100), 100)
end

--
-- Degenerate inputs
--

g.test_empty_counter = function()
    local counter = percentile.new()
    t.assert_equals(counter:count(), 0)
    t.assert_equals(counter:percentile(50), nil)
    t.assert_equals(counter:sample(), {})
end

g.test_single_value = function()
    local counter = counter_over({42})
    for _, p in ipairs({0, 1, 50, 99, 100}) do
        t.assert_equals(counter:percentile(p), 42)
    end
end

g.test_bad_arguments = function()
    local counter = percentile.new()
    t.assert_error_msg_contains('not a number', function()
        counter:add('7')
    end)
    t.assert_error_msg_contains('percentage in [0, 100]', function()
        counter:percentile(101)
    end)
    t.assert_error_msg_contains('percentage in [0, 100]', function()
        counter:percentile(-1)
    end)
    t.assert_error_msg_contains('window_size must be at least 1',
                                percentile.new, {window_size = 0})
end

--
-- The bounded window
--

g.test_window_caps_the_sample = function()
    -- Evict the smallest every time, so what survives is deterministic: the
    -- last five values, which here are the five largest.
    local counter = percentile.new({
        window_size = 5,
        rng_index = function() return 1 end,
    })
    for i = 1, 20 do
        counter:add(i)
    end
    t.assert_equals(counter:count(), 5)
    t.assert_equals(counter:sample(), {16, 17, 18, 19, 20})
    t.assert_equals(counter:percentile(100), 20)
    t.assert_equals(counter:percentile(20), 16)
end

g.test_window_with_random_eviction_stays_bounded_and_sorted = function()
    local counter = percentile.new({window_size = 32})
    local rng = lcg(99)
    for _ = 1, 500 do
        counter:add(rng() * 1000)
    end
    t.assert_equals(counter:count(), 32)
    t.assert_equals(#counter:sample(), 32)
    local values = counter:sample()
    for i = 2, #values do
        t.assert(values[i - 1] <= values[i], 'still sorted after eviction')
    end
end

--
-- The state is storable
--

g.test_state_is_a_plain_table = function()
    local counter = counter_over({3, 1, 2}, {window_size = 10})
    -- No functions and no userdata anywhere: this has to survive msgpack.
    for key, value in pairs(counter) do
        t.assert_equals(type(key), 'string')
        t.assert(type(value) == 'number' or type(value) == 'table',
                 string.format('field %s is a %s', key, type(value)))
    end
    t.assert_equals(getmetatable(counter.values), nil)
end

g.test_state_survives_a_round_trip = function()
    local counter = counter_over({5, 3, 9, 1, 7})

    -- What a space would hand back: the same numbers, no metatable.
    local stored = {n = counter.n, window_size = counter.window_size,
                    values = {}}
    for i, x in ipairs(counter.values) do
        stored.values[i] = x
    end

    local restored = percentile.wrap(stored)
    t.assert_equals(restored:count(), 5)
    t.assert_equals(restored:percentile(50), 5)
    t.assert_equals(restored:percentile(100), 9)
    restored:add(4)
    t.assert_equals(restored:sample(), {1, 3, 4, 5, 7, 9})
end

g.test_wrap_accepts_a_bare_value_array = function()
    local restored = percentile.wrap({values = {1, 2, 3, 4}})
    t.assert_equals(restored:count(), 4)
    t.assert_equals(restored:percentile(50), 2)
end

--
-- The shape the 2016 calibration phase used
--

g.test_calibration_buckets = function()
    -- calibrate() asked for floor(100 / 5) - 1 = 19 percentiles at 10, 15,
    -- ... 100 and appended them to the model. On 1..1000 they come out as the
    -- round numbers, which is the check that the whole chain lines up.
    local counter = percentile.new()
    for i = 1, 1000 do
        counter:add(i)
    end
    local bucket_percents = 5.0
    local buckets = {}
    for p = 1, math.floor(100 / bucket_percents) - 1 do
        buckets[p] = counter:percentile((p + 1) * bucket_percents)
    end
    t.assert_equals(#buckets, 19)
    t.assert_equals(buckets[1], 100)     -- 10th percentile
    t.assert_equals(buckets[8], 450)     -- 45th
    t.assert_equals(buckets[19], 1000)   -- 100th
end
