local t = require('luatest')

local utils       = require('pregel.utils')
local strict      = require('pregel.utils.strict')
local copy        = require('pregel.utils.copy')
local collections = require('pregel.utils.collections')

local g = t.group('utils')

--
-- utils.error
--

-- Defect: a message that is not a format string used to be fed to
-- string.format() anyway, so a stray '%' replaced the real message with the
-- formatting failure. worker.lua re-raises caught errors through this path.
g.test_error_single_argument_is_not_formatted = function()
    local ok, err = pcall(utils.error, 'literal 100% of the time')
    t.assert_equals(ok, false)
    err = tostring(err)
    t.assert_str_contains(err, 'literal 100% of the time')
    -- The substring on its own is not enough: without the lone-argument guard
    -- the message becomes 'literal 100% of the time [format failed: ...]',
    -- which still contains it, and the test passed over the reintroduced
    -- defect.
    t.assert_not_str_contains(err, 'format failed')

    -- At level 0 there is no position prefix, so the message can be checked
    -- for equality -- nothing added, nothing lost.
    local ok0, err0 = pcall(utils.error, 0, 'literal 100% of the time')
    t.assert_equals(ok0, false)
    t.assert_equals(tostring(err0), 'literal 100% of the time')
end

g.test_error_formats_when_given_arguments = function()
    local ok, err = pcall(utils.error, 'unknown message type: %s', 'frobnicate')
    t.assert_equals(ok, false)
    t.assert_str_contains(tostring(err), 'unknown message type: frobnicate')
end

-- A format that cannot be rendered must still carry the original message.
g.test_error_failed_format_keeps_message = function()
    local ok, err = pcall(utils.error, 'want a number here: %d', {})
    t.assert_equals(ok, false)
    err = tostring(err)
    t.assert_str_contains(err, 'want a number here: %d')
    t.assert_str_contains(err, 'format failed')
end

g.test_error_accepts_leading_level = function()
    local ok, err = pcall(utils.error, 2, 'level %d message', 2)
    t.assert_equals(ok, false)
    t.assert_str_contains(tostring(err), 'level 2 message')
end

-- Level 0 means "no position information at all".
g.test_error_level_zero_has_no_position = function()
    local ok, err = pcall(utils.error, 0, 'bare message')
    t.assert_equals(ok, false)
    t.assert_equals(tostring(err), 'bare message')
end

g.test_syserror_reports_errno = function()
    local ok, err = pcall(utils.syserror, 'cannot open %s', '/nope')
    t.assert_equals(ok, false)
    err = tostring(err)
    t.assert_str_contains(err, 'cannot open /nope')
    t.assert_str_contains(err, '[errno ')
end

--
-- utils.is_callable
--

g.test_is_callable = function()
    t.assert_equals(utils.is_callable(print), true)
    t.assert_equals(utils.is_callable(function() end), true)
    t.assert_equals(utils.is_callable(setmetatable({}, {
        __call = function() end
    })), true)
    t.assert_equals(utils.is_callable({}), false)
    t.assert_equals(utils.is_callable(nil), false)
    t.assert_equals(utils.is_callable(42), false)
    t.assert_equals(utils.is_callable('str'), false)
    -- A metatable whose __call is not a function is not callable either.
    t.assert_equals(utils.is_callable(setmetatable({}, { __call = 42 })), false)
end

--
-- utils.xpcall_tb / timeit / traceback
--

g.test_xpcall_tb_success_returns_values = function()
    local ok, a, b = utils.xpcall_tb(function(x, y)
        return x + y, x * y
    end, 3, 4)
    t.assert_equals(ok, true)
    t.assert_equals(a, 7)
    t.assert_equals(b, 12)
end

g.test_xpcall_tb_failure_returns_error = function()
    local ok, err = utils.xpcall_tb(function()
        error('boom')
    end)
    t.assert_equals(ok, false)
    t.assert_str_contains(tostring(err), 'boom')
end

-- nil arguments must survive the trip through the lazy closure.
g.test_xpcall_tb_passes_trailing_nil = function()
    local seen
    local ok = utils.xpcall_tb(function(...)
        seen = select('#', ...)
    end, 1, nil, nil)
    t.assert_equals(ok, true)
    t.assert_equals(seen, 3)
end

g.test_timeit_runs_function_and_returns_number = function()
    local called = false
    local elapsed = utils.timeit(function(flag)
        called = flag
    end, true)
    t.assert_equals(called, true)
    t.assert_equals(type(elapsed), 'number')
    t.assert_ge(elapsed, 0)
end

g.test_traceback_is_a_list_of_frames = function()
    local tb
    local function inner() tb = utils.traceback() end
    local function outer() inner() end
    outer()
    t.assert_ge(#tb, 1)
    t.assert_equals(type(tb[1].file), 'string')
    t.assert_equals(type(tb[1].line), 'number')
end

--
-- Pruned helpers must be gone, not merely unused: strictify turns a stale
-- reference into an error instead of a nil that fails somewhere else.
--

g.test_pruned_helpers_are_removed = function()
    for _, name in ipairs({'execute_authorized_mr', 'random', 'is_main',
                           'log_traceback', 'lazy_func'}) do
        t.assert_error_msg_contains("'" .. name .. "' is not declared",
                                    function() return utils[name] end)
    end
end

g.test_pruned_modules_are_removed = function()
    for _, name in ipairs({'pregel.utils.fiber_pool', 'pregel.utils.elog',
                           'pregel.utils.checktype'}) do
        t.assert_error(function() require(name) end)
    end
end

--
-- strict
--

g.test_strictify_allows_declared_reads = function()
    local m = strict.strictify({ alpha = 1, beta = function() return 2 end })
    t.assert_equals(m.alpha, 1)
    t.assert_equals(m.beta(), 2)
end

g.test_strictify_refuses_undeclared_reads = function()
    local m = strict.strictify({ alpha = 1 })
    t.assert_error_msg_contains("variable 'gamma' is not declared", function()
        return m.gamma
    end)
end

g.test_strictify_registers_new_keys_on_write = function()
    local m = strict.strictify({ alpha = 1 })
    m.gamma = 3
    t.assert_equals(m.gamma, 3)
end

g.test_unstrictify_restores_plain_table = function()
    local m = strict.strictify({ alpha = 1 })
    strict.unstrictify(m)
    t.assert_equals(m.gamma, nil)
end

-- Two strictified tables must not share a declared-key set.
g.test_strictify_tables_are_independent = function()
    local a = strict.strictify({ alpha = 1 })
    local b = strict.strictify({ beta = 2 })
    t.assert_equals(a.alpha, 1)
    t.assert_error_msg_contains("variable 'beta' is not declared", function()
        return a.beta
    end)
    t.assert_error_msg_contains("variable 'alpha' is not declared", function()
        return b.alpha
    end)
end

--
-- collections.defaultdict
--

g.test_defaultdict_with_constant = function()
    local d = collections.defaultdict(0)
    t.assert_equals(d.missing, 0)
    d.missing = d.missing + 1
    t.assert_equals(d.missing, 1)
end

g.test_defaultdict_with_factory = function()
    local seen = {}
    local d = collections.defaultdict(function(key)
        table.insert(seen, key)
        return {key}
    end)
    table.insert(d.a, 'x')
    t.assert_equals(d.a, {'a', 'x'})
    -- The factory runs once per key: the second read is the materialised table.
    t.assert_equals(seen, {'a'})
end

g.test_defaultdict_materialises_on_read = function()
    local d = collections.defaultdict(function() return {} end)
    local _ = d.k
    local keys = {}
    for key in pairs(d) do table.insert(keys, key) end
    t.assert_equals(keys, {'k'})
end

--
-- copy.deep
--

g.test_deep_copy_is_independent = function()
    local orig = { a = 1, nested = { b = { 2, 3 } } }
    local dup = copy.deep(orig)
    t.assert_equals(dup, orig)
    dup.nested.b[1] = 99
    t.assert_equals(orig.nested.b[1], 2)
end

g.test_deep_copy_of_scalars = function()
    t.assert_equals(copy.deep(7), 7)
    t.assert_equals(copy.deep('str'), 'str')
    t.assert_equals(copy.deep(nil), nil)
end

g.test_copy_shallow_is_removed = function()
    t.assert_error_msg_contains("'shallow' is not declared", function()
        return copy.shallow
    end)
end
