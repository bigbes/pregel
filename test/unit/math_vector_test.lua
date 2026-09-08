local t = require('luatest')

local vector = require('pregel.math.vector')

local g = t.group('math.vector')

g.test_dot = function()
    t.assert_equals(vector.dot({1, 2, 3}, {4, 5, 6}), 32)
    t.assert_equals(vector.dot({}, {}), 0)
    t.assert_equals(vector.dot({1, -1}, {1, 1}), 0)
end

g.test_axpy = function()
    t.assert_equals(vector.axpy(2, {1, 2, 3}, {10, 20, 30}), {12, 24, 36})
    -- Neither argument is touched: a weight vector handed to several samples in
    -- one superstep must read the same every time.
    local x, y = {1, 2}, {3, 4}
    vector.axpy(5, x, y)
    t.assert_equals(x, {1, 2})
    t.assert_equals(y, {3, 4})
end

g.test_scale = function()
    t.assert_equals(vector.scale(3, {1, -2, 0.5}), {3, -6, 1.5})
    local x = {1, 2}
    t.assert_is_not(vector.scale(1, x), x)
end

g.test_add = function()
    t.assert_equals(vector.add({1, 2, 3}, {0.5, -2, 10}), {1.5, 0, 13})
end

g.test_norm = function()
    t.assert_equals(vector.norm({3, 4}), 5)
    t.assert_equals(vector.norm({}), 0)
    t.assert_almost_equals(vector.norm({1, 1, 1, 1}), 2, 1e-12)
end

g.test_zeros = function()
    t.assert_equals(vector.zeros(4), {0, 0, 0, 0})
    t.assert_equals(vector.zeros(0), {})
end

g.test_random_uses_the_generator_it_is_given = function()
    -- A constant generator pins the output exactly, which is the property the
    -- reproducible tests below rely on.
    t.assert_equals(vector.random(3, function() return 0.5 end), {0, 0, 0})
    t.assert_equals(vector.random(2, function() return 0 end), {-1, -1})

    local n = 0
    local v = vector.random(100, function()
        n = n + 1
        return math.random()
    end)
    t.assert_equals(n, 100)
    t.assert_equals(#v, 100)
    for _, x in ipairs(v) do
        t.assert(x >= -1 and x < 1, 'random() stays in [-1, 1)')
    end
end

-- The vectors go into messages and vertex values, so they have to be plain
-- arrays -- msgpack cannot encode anything else.
g.test_results_are_plain_arrays = function()
    for _, v in ipairs({
        vector.axpy(1, {1}, {1}),
        vector.scale(1, {1}),
        vector.add({1}, {1}),
        vector.zeros(1),
        vector.random(1),
    }) do
        t.assert_equals(type(v), 'table')
        t.assert_equals(getmetatable(v), nil)
    end
end
