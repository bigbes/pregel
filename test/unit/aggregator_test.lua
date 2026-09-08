local t = require('luatest')

local aggregator = require('pregel.aggregator')

local g = t.group('aggregator')

-- An aggregator only reaches its instance to report over the network, which
-- none of these tests do, so a bare table is the whole of it here.
local function make(opts)
    return aggregator.new('agg', {}, opts)
end

-------------------------------------------------------------------------------
-- Construction
-------------------------------------------------------------------------------

-- Defect: aggregator_new assigned `value = opts.default` and
-- `global = opts.default` by reference, so until the first make_default() --
-- which only runs when the master's merged value comes back, after superstep 1
-- -- the accumulator *was* the default table. A reduce that folded into its
-- accumulator in place therefore rewrote the job's default, and every later
-- superstep started from whatever the first one had left behind.
g.test_a_table_default_is_not_shared_with_the_accumulator = function()
    local default = {count = 0, seen = {}}
    local a = make({default = default})

    t.assert_is_not(a.value, default, 'value aliases the default')
    t.assert_is_not(a.global, default, 'global aliases the default')
    t.assert_is_not(a.value, a.global, 'value and global are one table')
    t.assert_equals(a.value, {count = 0, seen = {}})

    -- A reduce that mutates its accumulator, which is the shape the defect
    -- punished: after it, the default has to be what it was.
    a.value.count = 7
    table.insert(a.value.seen, 'x')
    t.assert_equals(default, {count = 0, seen = {}})
    t.assert_equals(a.global, {count = 0, seen = {}})
end

-- The copy is deep, so a nested table is not shared either.
g.test_a_nested_table_default_is_copied_deeply = function()
    local default = {inner = {n = 1}}
    local a = make({default = default})

    t.assert_is_not(a.value.inner, default.inner)
    a.value.inner.n = 99
    t.assert_equals(default.inner.n, 1)
end

-- Defect: a function default was stored as the value instead of being called,
-- so the accumulator was the function itself until make_default() replaced it
-- -- and vertex:get_aggregation() answered a function to every vertex of
-- superstep 1.
g.test_a_function_default_is_called_at_construction = function()
    local calls = 0
    local a = make({default = function()
        calls = calls + 1
        return {n = 0}
    end})

    t.assert_equals(type(a.value), 'table', 'value is the function itself')
    t.assert_equals(type(a.global), 'table', 'global is the function itself')
    t.assert_equals(a.value, {n = 0})
    t.assert_equals(a.global, {n = 0})
    -- Once for each: they are two accumulators, not one shared answer.
    t.assert_equals(calls, 2)
    t.assert_is_not(a.value, a.global)
end

-- A scalar default has nothing to alias, and must still be the value.
g.test_a_scalar_default_is_the_value = function()
    local a = make({default = 5})
    t.assert_equals(a.value, 5)
    t.assert_equals(a.global, 5)
    t.assert_equals(a:get_global(), 5)
end

-- No default at all is nil on both sides, as it was.
g.test_no_default = function()
    local a = make()
    t.assert_equals(a.value, nil)
    t.assert_equals(a.global, nil)
end

-------------------------------------------------------------------------------
-- make_default and receive_global
-------------------------------------------------------------------------------

-- The same rule after a reset: the accumulator make_default() installs is a
-- fresh copy, not the default itself.
g.test_make_default_does_not_share_the_default_either = function()
    local default = {n = 0}
    local a = make({default = default})

    a:make_default()
    t.assert_is_not(a.value, default)
    a.value.n = 3
    t.assert_equals(default.n, 0)
end

-- receive_global installs the master's merged value and clears the
-- accumulator, and the two must not become the same table.
g.test_receive_global_leaves_a_fresh_accumulator = function()
    local a = make({default = {n = 0}})
    local merged = {n = 42}

    a:receive_global(merged)

    t.assert_is(a.global, merged)
    t.assert_is_not(a.value, merged)
    t.assert_equals(a.value, {n = 0})
    a.value.n = 1
    t.assert_equals(merged.n, 42)
end

-- Two aggregators declared with the same default table -- an app that reuses
-- one literal -- must not end up contributing to each other.
g.test_two_aggregators_over_one_default_are_independent = function()
    local default = {n = 0}
    local a = aggregator.new('a', {}, {default = default})
    local b = aggregator.new('b', {}, {default = default})

    a.value.n = 1
    t.assert_equals(b.value.n, 0)
    t.assert_equals(default.n, 0)
end

-------------------------------------------------------------------------------
-- The accumulator itself
-------------------------------------------------------------------------------

g.test_call_reads_and_contributes = function()
    local a = make({
        default = 0,
        reduce  = function(old, new) return old + new end,
    })
    t.assert_equals(a(), 0)
    a(3)
    a(4)
    t.assert_equals(a(), 7)
    -- get_global() is the previous superstep's merged value, not this.
    t.assert_equals(a:get_global(), 0)
end

-- A reduce that folds in place is the case the aliasing defect broke; it has
-- to work, and it has to leave the default alone over a whole superstep cycle.
g.test_a_mutating_reduce_over_a_superstep_cycle = function()
    local default = {}
    local a = make({
        default = default,
        reduce  = function(acc, value)
            table.insert(acc, value)
            return acc
        end,
        merge = function(acc, value)
            for _, v in ipairs(value) do table.insert(acc, v) end
            return acc
        end,
    })

    a('one')
    a('two')
    t.assert_equals(a(), {'one', 'two'})
    t.assert_equals(default, {}, 'the default was contributed to')

    -- End of the superstep: the master merges and hands the value back.
    a:receive_global({'one', 'two'})
    t.assert_equals(a:get_global(), {'one', 'two'})
    t.assert_equals(a(), {}, 'the next superstep starts from the default')

    a('three')
    t.assert_equals(a(), {'three'})
    t.assert_equals(a:get_global(), {'one', 'two'},
                    'contributing rewrote the merged value')
    t.assert_equals(default, {})
end

g.test_merge_defaults_to_reduce = function()
    local a = make({
        default = 0,
        reduce  = function(old, new) return old + new end,
    })
    a:merge_master(2)
    a:merge_master(3)
    t.assert_equals(a(), 5)
end

g.test_rejects_a_non_callable_reduce_or_merge = function()
    t.assert_error_msg_contains('options.reduce', function()
        make({reduce = 42})
    end)
    t.assert_error_msg_contains('options.merge', function()
        make({merge = 42})
    end)
end
