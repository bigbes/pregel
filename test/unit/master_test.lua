local t = require('luatest')

local box_helper = require('test.helpers.box')
local master = require('pregel.master')

local g = t.group('master')

local seq = 0
local function fresh_name()
    seq = seq + 1
    return string.format('mt%03d', seq)
end

local function obtain_name(value)
    return value.name
end

-- mpool.new refuses an empty server list, so even a master whose pool is about
-- to be replaced by a stub needs one real address to be built against. This
-- instance's own socket is the cheapest one there is.
local URI

g.before_all(function()
    URI = box_helper.listen_uri()
end)

-- The stub gives up after this many supersteps. Nothing here should reach it
-- -- the highest a test legitimately runs to is 250 -- and it exists because
-- the subject of half of them is a loop that must *stop*: break the limit
-- check and the loop has nothing to end it, so without this the tests would
-- hang the suite rather than fail it. Which is what they did when the check
-- was mutated out to watch them go red.
local STUB_LIMIT = 1000

--- A master whose message pool is a stub, so the superstep loop runs with no
-- worker behind it.
--
-- The loop is driven entirely by the two internal aggregators, and on a real
-- run those are filled by the workers reporting through
-- `pregel.master.deliver` during 'superstep.after'. The stub does that part
-- itself, from `state_at(superstep)` -- which is what lets one test say "this
-- graph never goes quiet" and another "it goes quiet in the third superstep"
-- without a graph, a worker or a socket.
--
-- @param options extra options for master.new
-- @param state_at superstep -> {in_progress = ..., messages = ...}
-- @return the master, and the stub
local function master_with_stub(options, state_at)
    options = options or {}
    options.workers = {URI}
    options.obtain_name = obtain_name
    local m = master.new(fresh_name(), options)
    local real_mpool = m.mpool

    local stub = {
        -- inform_workers() walks these; with no worker there is nothing to
        -- inform and nothing to record.
        buckets   = {},
        sent      = {},
        flushes   = 0,
        superstep = 0,
    }
    function stub:send_wait(msg, arg)
        table.insert(self.sent, msg)
        if msg == 'superstep' then
            self.superstep = arg
            if arg > STUB_LIMIT then
                error(string.format(
                    'stub mpool: superstep %d, and nothing has stopped this ' ..
                    'run', arg), 0)
            end
            -- One worker's timing report; start() logs v[1] through a numeric
            -- format, so it has to be a number.
            return {{0.0}}
        end
        if msg == 'superstep.after' then
            local state = state_at(self.superstep)
            m.aggregators['__in_progress']:merge_master(state.in_progress)
            m.aggregators['__messages']:merge_master(state.messages)
        end
        return {}
    end
    function stub:flush()
        self.flushes = self.flushes + 1
    end
    m.mpool = stub
    -- Put the real one back for stop(), which is what closes the connection
    -- the pool opened to this instance.
    function stub:stop()
        m.mpool = real_mpool
        real_mpool:stop()
    end

    return m, stub
end

--- A graph that never goes quiet: `active` vertices and `messages` in flight
-- for as long as anything asks.
local function never_quiet(active, messages)
    return function()
        return {in_progress = active, messages = messages}
    end
end

--- A graph that goes quiet after `n` supersteps.
local function quiet_after(n)
    return function(superstep)
        if superstep >= n then
            return {in_progress = 0, messages = 0}
        end
        return {in_progress = 1, messages = 1}
    end
end

--- The log table pregel.master actually writes through.
--
-- Tarantool hands each requiring module its own logger table -- that is what
-- makes per-module levels (log.cfg.modules) possible -- so require('log') in
-- this file answers with a different table from require('log') in master.lua,
-- and patching this one's `warn` is invisible over there. Measured: the two
-- tables have different addresses in a bare tarantool -e as well as under
-- luatest. So reach the real one through the closure that uses it.
local function master_log(m)
    local start = getmetatable(m).__index.start
    for i = 1, debug.getinfo(start, 'u').nups do
        local name, value = debug.getupvalue(start, i)
        if name == 'log' then
            return value
        end
    end
    error('pregel.master.start has no `log` upvalue any more')
end

--- Run `fn` with the master's log.warn recording instead of printing.
local function capture_warnings(m, fn)
    local log = master_log(m)
    local warnings = {}
    local real = log.warn
    log.warn = function(fmt, ...)
        table.insert(warnings, string.format(fmt, ...))
    end
    local ok, err = pcall(fn)
    log.warn = real
    if not ok then
        error(err, 0)
    end
    return warnings
end

-------------------------------------------------------------------------------
-- max_supersteps
-------------------------------------------------------------------------------

-- The loop runs while a vertex is active or a message is in flight, so a job
-- whose vertices keep voting themselves awake has nothing to end it. This is
-- the whole point of the option: a bounded failure instead of a master that
-- has to be killed.
g.test_the_limit_stops_a_job_that_never_goes_quiet = function()
    local m = master_with_stub({max_supersteps = 5}, never_quiet(3, 7))
    t.assert_error_msg_equals(
        'pregel: superstep limit 5 reached with 3 active vertices and ' ..
        '7 messages in flight',
        function() m:start() end)
    m:stop()
end

-- The limit is a number of supersteps actually run, not the number the loop
-- was about to start: five ran, and the sixth is the one that did not.
g.test_the_limit_is_reached_after_that_many_supersteps = function()
    local m, stub = master_with_stub({max_supersteps = 5}, never_quiet(1, 0))
    pcall(function() m:start() end)
    local ran = 0
    for _, msg in ipairs(stub.sent) do
        if msg == 'superstep' then ran = ran + 1 end
    end
    t.assert_equals(ran, 5)
    t.assert_equals(m.superstep_count, 5)
    m:stop()
end

-- The numbers come from the merged aggregators, which is what makes them
-- worth printing: they say whether the job was still spreading messages or
-- just sitting there with vertices that refuse to halt.
g.test_the_error_carries_the_live_counts = function()
    local m = master_with_stub({max_supersteps = 1}, never_quiet(41, 0))
    t.assert_error_msg_contains(
        '41 active vertices and 0 messages in flight',
        function() m:start() end)
    m:stop()
end

-- A limit that is never reached must not change the answer, and in particular
-- must not cut a job short one superstep before it would have finished.
g.test_a_job_that_converges_first_is_untouched = function()
    local m = master_with_stub({max_supersteps = 10}, quiet_after(3))
    t.assert_equals(m:start(), 3)
    m:stop()
end

-- The boundary: converging in exactly the last permitted superstep is not a
-- failure. The check has to come after the convergence check, not before it.
g.test_converging_in_the_last_permitted_superstep_succeeds = function()
    local m = master_with_stub({max_supersteps = 3}, quiet_after(3))
    t.assert_equals(m:start(), 3)
    m:stop()
end

g.test_a_limit_of_one_superstep = function()
    local m = master_with_stub({max_supersteps = 1}, quiet_after(1))
    t.assert_equals(m:start(), 1)
    m:stop()
end

-------------------------------------------------------------------------------
-- Unbounded runs
-------------------------------------------------------------------------------

-- No limit is still the default, so an existing job keeps running to
-- convergence with nothing in its way.
g.test_without_a_limit_a_converging_job_is_unchanged = function()
    local m = master_with_stub({}, quiet_after(4))
    t.assert_equals(m.max_supersteps, nil)
    t.assert_equals(m:start(), 4)
    m:stop()
end

-- An unbounded job that is going nowhere says so in the log rather than
-- silently: the warning names the option, which is the only way a reader of
-- the log learns there is one.
g.test_an_unbounded_run_warns_every_hundred_supersteps = function()
    local m = master_with_stub({}, function(superstep)
        if superstep >= 250 then
            return {in_progress = 0, messages = 0}
        end
        return {in_progress = 2, messages = 0}
    end)
    local warnings = capture_warnings(m, function()
        t.assert_equals(m:start(), 250)
    end)
    t.assert_equals(#warnings, 2)
    t.assert_str_contains(warnings[1], '100 supersteps and still running')
    t.assert_str_contains(warnings[1], 'options.max_supersteps')
    t.assert_str_contains(warnings[2], '200 supersteps and still running')
    m:stop()
end

-- ...and a bounded run does not warn: it has an answer of its own for the
-- question the warning asks.
g.test_a_bounded_run_does_not_warn = function()
    local m = master_with_stub({max_supersteps = 250}, never_quiet(2, 0))
    local warnings = capture_warnings(m, function()
        pcall(function() m:start() end)
    end)
    t.assert_equals(warnings, {})
    m:stop()
end

-------------------------------------------------------------------------------
-- Validation
-------------------------------------------------------------------------------

-- Rejected by new() rather than by start(): a job discovers a bad limit
-- before it loads a graph, and a negative one is never discovered at all if
-- the only check is in the loop.
g.test_max_supersteps_must_be_a_positive_integer = function()
    for _, bad in ipairs({0, -1, 2.5, '5', true}) do
        t.assert_error_msg_contains(
            'options.max_supersteps must be a positive integer or nil',
            function()
                master.new(fresh_name(), {
                    workers = {URI}, obtain_name = obtain_name,
                    max_supersteps = bad,
                })
            end,
            'accepted ' .. tostring(bad))
    end
end

g.test_max_supersteps_accepts_a_positive_integer_and_nil = function()
    local m = master.new(fresh_name(), {
        workers = {URI}, obtain_name = obtain_name, max_supersteps = 7,
    })
    t.assert_equals(m.max_supersteps, 7)
    m:stop()

    local unbounded = master.new(fresh_name(), {
        workers = {URI}, obtain_name = obtain_name,
    })
    t.assert_equals(unbounded.max_supersteps, nil)
    unbounded:stop()
end
