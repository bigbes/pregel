local t = require('luatest')

local auc = require('pregel.math.auc')

local g = t.group('math.auc')

--- A linear congruential generator, so "a random sample" is the same sample on
-- every run and on every machine. math.random is seeded per process in
-- Tarantool, which would make the tolerance below a coin flip.
local function lcg(seed)
    local state = seed
    return function()
        state = (1103515245 * state + 12345) % 2147483648
        return state / 2147483648
    end
end

--- Reference implementation: the definition, as a double loop.
--
-- O(n^2), so it is only ever run on small inputs, but it is the thing the rank
-- statistic is supposed to equal and it shares no code with it.
local function auc_by_definition(samples)
    local wins, pairs_seen = 0.0, 0
    for _, a in ipairs(samples) do
        for _, b in ipairs(samples) do
            if a[2] > 0 and b[2] <= 0 then
                pairs_seen = pairs_seen + 1
                if a[1] > b[1] then
                    wins = wins + 1
                elseif a[1] == b[1] then
                    wins = wins + 0.5
                end
            end
        end
    end
    if pairs_seen == 0 then
        return nil
    end
    return wins / pairs_seen
end

--
-- The extremes
--

g.test_perfect_ranking = function()
    local samples = {}
    for i = 1, 10 do
        samples[i] = {i, i > 5 and 1 or -1}
    end
    t.assert_equals(auc.auc(samples), 1.0)
end

g.test_reversed_ranking = function()
    local samples = {}
    for i = 1, 10 do
        samples[i] = {i, i > 5 and -1 or 1}
    end
    t.assert_equals(auc.auc(samples), 0.0)
end

g.test_input_order_does_not_matter = function()
    -- The same samples shuffled: AUC is a property of the ordering by score.
    t.assert_equals(auc.auc({{5, 1}, {1, -1}, {3, 1}, {2, -1}}), 1.0)
    t.assert_equals(auc.auc({{1, -1}, {2, -1}, {3, 1}, {5, 1}}), 1.0)
end

--
-- Hand-computed values
--

g.test_known_value = function()
    -- Sorted: 0.1(-), 0.35(+), 0.4(-), 0.8(+) -> positive ranks 2 and 4.
    -- (2 + 4 - 2*3/2) / (2*2) = 3/4.
    t.assert_almost_equals(
        auc.auc({{0.1, 0}, {0.4, 0}, {0.35, 1}, {0.8, 1}}), 0.75, 1e-12)
end

g.test_ties_are_averaged = function()
    -- Sorted: 1(-) rank 1, 2(+) and 2(-) sharing rank 2.5, 3(+) rank 4.
    -- (2.5 + 4 - 3) / (2*2) = 0.875. Pairwise: (2>1), (2 ties 2), (3>1),
    -- (3>2) -> (1 + 0.5 + 1 + 1) / 4 = 0.875.
    local samples = {{1, 0}, {2, 1}, {2, 0}, {3, 1}}
    t.assert_almost_equals(auc.auc(samples), 0.875, 1e-12)
    t.assert_almost_equals(auc.auc(samples), auc_by_definition(samples), 1e-12)
end

g.test_all_scores_tied_is_one_half = function()
    -- Without tie averaging this comes out as whatever table.sort happened to
    -- do with equal keys -- 1 or 0, not 0.5.
    local samples = {}
    for i = 1, 8 do
        samples[i] = {7, i % 2 == 0 and 1 or -1}
    end
    t.assert_almost_equals(auc.auc(samples), 0.5, 1e-12)
end

g.test_a_long_run_of_ties = function()
    -- Five samples at the same score, so the run spans ranks 2..6 and every
    -- one of them takes rank 4. The 2016 loop broke out of the run on the
    -- first equal element and could only ever average a pair.
    local samples = {{0, -1}, {1, 1}, {1, 1}, {1, -1}, {1, -1}, {1, 1},
                     {2, 1}}
    t.assert_almost_equals(auc.auc(samples), auc_by_definition(samples), 1e-12)
    -- Positives: three at rank 4, one at rank 7. Sum 19, P = 4, N = 3.
    -- (19 - 10) / 12 = 0.75.
    t.assert_almost_equals(auc.auc(samples), 0.75, 1e-12)
end

--
-- Label conventions
--

g.test_plus_minus_one_and_zero_one_agree = function()
    local pm = {{0.9, 1}, {0.1, -1}, {0.5, 1}, {0.4, -1}}
    local zo = {{0.9, 1}, {0.1, 0}, {0.5, 1}, {0.4, 0}}
    t.assert_equals(auc.auc(pm), auc.auc(zo))
    t.assert_almost_equals(auc.auc(pm), 1.0, 1e-12)
end

--
-- Degenerate inputs
--

g.test_one_class_only_is_undefined = function()
    t.assert_equals(auc.auc({{1, 1}, {2, 1}}), nil)
    t.assert_equals(auc.auc({{1, 0}, {2, -1}}), nil)
    t.assert_equals(auc.auc({}), nil)
end

g.test_malformed_sample_is_an_error = function()
    t.assert_error_msg_contains('sample 2 is not a {score, label} pair',
                                auc.auc, {{1, 1}, 7})
    t.assert_error_msg_contains('sample 1 is not a {score, label} pair',
                                auc.auc, {{'high', 1}})
end

--
-- A large random sample
--

g.test_random_scores_are_uninformative = function()
    -- Scores independent of the labels: the expected AUC is 0.5. With 4000
    -- samples the standard error of the statistic is well under 0.01, so a
    -- tolerance of 0.05 is loose and still catches the 1/N inflation the 2016
    -- code had (which at 2000 negatives is 5e-4 -- see the next test for the
    -- check that does catch it).
    local rng = lcg(20160401)
    local samples = {}
    for i = 1, 4000 do
        samples[i] = {rng(), rng() < 0.5 and 1 or -1}
    end
    t.assert_almost_equals(auc.auc(samples), 0.5, 0.05)
end

g.test_ranks_start_at_one = function()
    -- The 2016 code used rank i + 1, which adds P to the rank sum and so
    -- 1/N to every AUC. This is the smallest input where the difference shows
    -- exactly: one positive, one negative, positive on top -> exactly 1.0,
    -- and rank i + 1 would give 1.5.
    t.assert_equals(auc.auc({{0, -1}, {1, 1}}), 1.0)
    t.assert_equals(auc.auc({{0, 1}, {1, -1}}), 0.0)
end

--
-- Agreement with the definition on many random shapes
--

g.test_matches_the_definition_on_random_inputs = function()
    local rng = lcg(7)
    for case = 1, 60 do
        local samples = {}
        local n = 2 + case % 9
        for i = 1, n do
            -- Scores drawn from a handful of values, so ties are common.
            samples[i] = {math.floor(rng() * 4), rng() < 0.5 and 1 or 0}
        end
        local expected = auc_by_definition(samples)
        local got = auc.auc(samples)
        if expected == nil then
            t.assert_equals(got, nil)
        else
            t.assert_almost_equals(got, expected, 1e-12,
                                   'case ' .. tostring(case))
        end
    end
end

--
-- The streaming collector
--

g.test_collector_matches_the_batch_function = function()
    local samples = {{0.1, 0}, {0.4, 0}, {0.35, 1}, {0.8, 1}}
    local collector = auc.new()
    t.assert_equals(collector:count(), 0)
    t.assert_equals(collector:result(), nil)
    for _, sample in ipairs(samples) do
        collector:add(sample[1], sample[2])
    end
    t.assert_equals(collector:count(), 4)
    t.assert_equals(collector:result(), auc.auc(samples))
    -- Reading the result does not consume anything.
    t.assert_equals(collector:result(), auc.auc(samples))
end

g.test_collector_state_survives_a_round_trip = function()
    local collector = auc.new()
    collector:add(0.1, 0):add(0.8, 1):add(0.35, 1):add(0.4, 0)

    -- What a space would hand back: the same tables, no metatable.
    local stored = {n = collector.n, samples = {}}
    for i, sample in ipairs(collector.samples) do
        stored.samples[i] = {sample[1], sample[2]}
    end
    t.assert_equals(getmetatable(stored.samples), nil)

    local restored = auc.wrap(stored)
    t.assert_equals(restored:count(), 4)
    t.assert_almost_equals(restored:result(), 0.75, 1e-12)
end

g.test_wrap_accepts_a_bare_sample_array = function()
    local restored = auc.wrap({samples = {{1, 1}, {0, -1}}})
    t.assert_equals(restored:count(), 2)
    t.assert_equals(restored:result(), 1.0)
end
