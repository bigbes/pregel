local t = require('luatest')

local mf = require('pregel.math.mf')

local g = t.group('math.mf')

--- Deterministic generator, so the initial factors are the same every run.
local function lcg(seed)
    local state = seed
    return function()
        state = (1103515245 * state + 12345) % 2147483648
        return state / 2147483648
    end
end

--
-- A rank-2 truth on a 4x4 matrix
--

local TRUTH = {
    mu  = 3.5,
    b_u = {0.4, -0.3, 0.1, -0.2},
    b_i = {-0.5, 0.2, 0.3, 0.0},
    p   = {{1.0, 0.2}, {-0.6, 0.9}, {0.3, -1.1}, {0.8, 0.5}},
    q   = {{0.7, -0.4}, {-0.2, 1.0}, {0.5, 0.6}, {-0.9, 0.1}},
}

local function truth_rating(u, i)
    return mf.predict(TRUTH.mu, TRUTH.b_u[u], TRUTH.b_i[i], TRUTH.p[u],
                      TRUTH.q[i])
end

--
-- predict / init_vector / new_model
--

g.test_predict = function()
    -- 1 + 0.5 + (-0.25) + (2*3 + 4*5) = 27.25
    t.assert_almost_equals(mf.predict(1, 0.5, -0.25, {2, 4}, {3, 5}), 27.25,
                           1e-12)
    t.assert_equals(mf.predict(0, 0, 0, {}, {}), 0)
end

g.test_init_vector = function()
    local v = mf.init_vector(4, function() return 0.5 end)
    t.assert_equals(v, {0, 0, 0, 0})
    t.assert_equals(mf.init_vector(2, function() return 1 end), {0.1, 0.1})
    t.assert_equals(mf.init_vector(2, function() return 1 end, 2), {2, 2})
    t.assert_equals(getmetatable(mf.init_vector(3)), nil)
end

g.test_init_vector_is_not_zero = function()
    -- p and q enter each other's gradient, so a pair starting at exactly zero
    -- never moves. Both must be off zero for a step to do anything.
    local rng = lcg(11)
    local v = mf.init_vector(8, rng)
    local nonzero = 0
    for _, x in ipairs(v) do
        if x ~= 0 then
            nonzero = nonzero + 1
        end
        t.assert(math.abs(x) <= 0.1, 'stays inside the default scale')
    end
    t.assert_equals(nonzero, 8)
end

g.test_new_model = function()
    local model = mf.new_model(3, {mu = 3.5, rng = function() return 0.5 end})
    t.assert_equals(model.mu, 3.5)
    t.assert_equals(model.b_u, 0)
    t.assert_equals(model.b_i, 0)
    t.assert_equals(model.p, {0, 0, 0})
    t.assert_equals(model.q, {0, 0, 0})
    t.assert_equals(mf.new_model(2).mu, 0)
    t.assert_error_msg_contains('positive number of factors', mf.new_model, 0)
    t.assert_error_msg_contains('positive number of factors', mf.new_model,
                                'two')
end

--
-- sgd_step
--

g.test_sgd_step_hand_computed = function()
    -- p.q = 2*3 + 4*5 = 26; err = 30 - 1 - 2 - 26 = 1.
    -- p2 = p + 0.1 * (1 * q - 0) = {2.3, 4.5}
    -- q2 = q + 0.1 * (1 * p - 0) = {3.2, 5.4}
    -- bu2 = 1 + 0.1 * 1 = 1.1, bi2 = 2.1
    local p2, q2, bu2, bi2, err = mf.sgd_step({2, 4}, {3, 5}, 1, 2, 30, 0.1, 0)
    t.assert_almost_equals(err, 1, 1e-12)
    t.assert_almost_equals(p2[1], 2.3, 1e-12)
    t.assert_almost_equals(p2[2], 4.5, 1e-12)
    t.assert_almost_equals(q2[1], 3.2, 1e-12)
    t.assert_almost_equals(q2[2], 5.4, 1e-12)
    t.assert_almost_equals(bu2, 1.1, 1e-12)
    t.assert_almost_equals(bi2, 2.1, 1e-12)
end

g.test_sgd_step_does_not_mutate_its_arguments = function()
    -- The vectors travel as messages and sit in vertex values; a step that
    -- wrote through them would corrupt both.
    local p, q = {2, 4}, {3, 5}
    local p2, q2 = mf.sgd_step(p, q, 1, 2, 30, 0.1, 0.05)
    t.assert_equals(p, {2, 4})
    t.assert_equals(q, {3, 5})
    t.assert_is_not(p2, p)
    t.assert_is_not(q2, q)
    t.assert_equals(getmetatable(p2), nil)
end

g.test_sgd_step_updates_both_sides_from_the_old_values = function()
    -- q2 is built from the old p, not from p2. Doing it in sequence would give
    -- q2[1] = 3 + 0.1 * 2.3 = 3.23 rather than 3.2, which is a different
    -- (and no longer gradient) step.
    local _, q2 = mf.sgd_step({2, 4}, {3, 5}, 1, 2, 30, 0.1, 0)
    t.assert_almost_equals(q2[1], 3.2, 1e-12)
    t.assert_not_almost_equals(q2[1], 3.23, 1e-6)
end

g.test_sgd_step_zero_error_only_shrinks = function()
    -- A perfectly predicted rating leaves everything alone when lambda is 0,
    -- and pulls everything toward zero when it is not.
    local p, q, bu, bi = {1, 0}, {0, 1}, 0.5, -0.25
    local rating = bu + bi   -- p.q = 0
    local p2, q2, bu2, bi2, err = mf.sgd_step(p, q, bu, bi, rating, 0.1, 0)
    t.assert_almost_equals(err, 0, 1e-12)
    t.assert_equals(p2, p)
    t.assert_equals(q2, q)
    t.assert_equals(bu2, bu)
    t.assert_equals(bi2, bi)

    local rp, _, rbu = mf.sgd_step(p, q, bu, bi, rating, 0.1, 0.5)
    t.assert_almost_equals(rp[1], 1 - 0.1 * 0.5 * 1, 1e-12)
    t.assert_almost_equals(rbu, 0.5 - 0.1 * 0.5 * 0.5, 1e-12)
end

g.test_sgd_step_rejects_mismatched_vectors = function()
    t.assert_error_msg_contains('different lengths: 2 and 3', mf.sgd_step,
                                {1, 2}, {1, 2, 3}, 0, 0, 1, 0.1, 0)
end

--
-- The step is the gradient of the objective
--

g.test_sgd_step_matches_the_squared_error_gradient = function()
    local lr, lambda = 0.05, 0.3
    local p, q = {0.7, -1.2, 0.4}, {-0.3, 0.9, 1.1}
    local bu, bi, rating = 0.35, -0.6, 1.75

    --- J = 0.5 * e^2 + 0.5 * lambda * (|p|^2 + |q|^2 + bu^2 + bi^2)
    local function objective(pp, qq, bbu, bbi)
        local dot = 0.0
        local penalty = bbu * bbu + bbi * bbi
        for k = 1, #pp do
            dot = dot + pp[k] * qq[k]
            penalty = penalty + pp[k] * pp[k] + qq[k] * qq[k]
        end
        local e = rating - bbu - bbi - dot
        return 0.5 * e * e + 0.5 * lambda * penalty
    end

    local p2, q2, bu2, bi2 = mf.sgd_step(p, q, bu, bi, rating, lr, lambda)

    local h = 1e-6
    local function central(bump_plus, bump_minus)
        return (objective(unpack(bump_plus)) - objective(unpack(bump_minus))) /
               (2 * h)
    end

    -- Every coordinate of p and q.
    for k = 1, #p do
        local pp, pm = {}, {}
        local qp, qm = {}, {}
        for j = 1, #p do
            pp[j], pm[j] = p[j], p[j]
            qp[j], qm[j] = q[j], q[j]
        end
        pp[k], pm[k] = p[k] + h, p[k] - h
        qp[k], qm[k] = q[k] + h, q[k] - h

        local dp = central({pp, q, bu, bi}, {pm, q, bu, bi})
        local dq = central({p, qp, bu, bi}, {p, qm, bu, bi})
        -- A gradient step is w - lr * dJ/dw, so (w - w2) / lr is dJ/dw.
        t.assert_almost_equals((p[k] - p2[k]) / lr, dp, 1e-6,
                               'dJ/dp[' .. k .. ']')
        t.assert_almost_equals((q[k] - q2[k]) / lr, dq, 1e-6,
                               'dJ/dq[' .. k .. ']')
    end

    -- And both biases.
    local dbu = central({p, q, bu + h, bi}, {p, q, bu - h, bi})
    local dbi = central({p, q, bu, bi + h}, {p, q, bu, bi - h})
    t.assert_almost_equals((bu - bu2) / lr, dbu, 1e-6, 'dJ/db_u')
    t.assert_almost_equals((bi - bi2) / lr, dbi, 1e-6, 'dJ/db_i')
end

g.test_the_gradient_check_covers_the_unregularised_step_too = function()
    -- lambda = 0 is the case the convergence test below runs in, and the
    -- penalty terms would otherwise be the only thing keeping the check
    -- honest about the signs.
    local lr = 0.05
    local p, q = {0.7, -1.2}, {-0.3, 0.9}
    local bu, bi, rating = 0.35, -0.6, 1.75
    local function objective(pp, qq, bbu, bbi)
        local dot = 0.0
        for k = 1, #pp do
            dot = dot + pp[k] * qq[k]
        end
        local e = rating - bbu - bbi - dot
        return 0.5 * e * e
    end
    local p2, q2, bu2, bi2 = mf.sgd_step(p, q, bu, bi, rating, lr, 0)
    local h = 1e-6
    for k = 1, #p do
        local pp, pm = {p[1], p[2]}, {p[1], p[2]}
        pp[k], pm[k] = p[k] + h, p[k] - h
        local dp = (objective(pp, q, bu, bi) - objective(pm, q, bu, bi)) /
                   (2 * h)
        t.assert_almost_equals((p[k] - p2[k]) / lr, dp, 1e-6)
        local qp, qm = {q[1], q[2]}, {q[1], q[2]}
        qp[k], qm[k] = q[k] + h, q[k] - h
        local dq = (objective(p, qp, bu, bi) - objective(p, qm, bu, bi)) /
                   (2 * h)
        t.assert_almost_equals((q[k] - q2[k]) / lr, dq, 1e-6)
    end
    local dbu = (objective(p, q, bu + h, bi) - objective(p, q, bu - h, bi)) /
                (2 * h)
    local dbi = (objective(p, q, bu, bi + h) - objective(p, q, bu, bi - h)) /
                (2 * h)
    t.assert_almost_equals((bu - bu2) / lr, dbu, 1e-6)
    t.assert_almost_equals((bi - bi2) / lr, dbi, 1e-6)
end

--
-- rmse
--

g.test_rmse = function()
    t.assert_equals(mf.rmse({{1, 1}, {2, 2}}), 0)
    -- errors 1, -1, 2 -> sqrt((1 + 1 + 4) / 3)
    t.assert_almost_equals(mf.rmse({{2, 1}, {1, 2}, {5, 3}}),
                           math.sqrt(6 / 3), 1e-12)
    t.assert_equals(mf.rmse({}), nil)
    -- Squared, so swapping the columns cannot change the answer.
    t.assert_equals(mf.rmse({{2, 1}}), mf.rmse({{1, 2}}))
    t.assert_error_msg_contains('sample 2 is not a {prediction, target} pair',
                                mf.rmse, {{1, 1}, {1}})
end

--
-- Convergence on the rank-2 truth
--

g.test_sgd_recovers_a_rank_two_four_by_four_matrix = function()
    local ratings = {}
    local total = 0.0
    for u = 1, 4 do
        ratings[u] = {}
        for i = 1, 4 do
            ratings[u][i] = truth_rating(u, i)
            total = total + ratings[u][i]
        end
    end
    local mu = total / 16

    -- A plain Lua loop, exactly the shape the Pregel example will run one
    -- superstep at a time: read (p, b_u) and (q, b_i), take a step, write both
    -- back.
    local rng = lcg(20160401)
    local users, items = {}, {}
    for u = 1, 4 do
        users[u] = {b = 0.0, v = mf.init_vector(2, rng)}
    end
    for i = 1, 4 do
        items[i] = {b = 0.0, v = mf.init_vector(2, rng)}
    end

    -- Measured: RMSE is 0.002 after 100 epochs and indistinguishable from
    -- zero after 500.
    local lr, lambda = 0.05, 0.0
    for _ = 1, 1000 do
        for u = 1, 4 do
            for i = 1, 4 do
                local p2, q2, bu2, bi2 = mf.sgd_step(
                    users[u].v, items[i].v, users[u].b, items[i].b,
                    ratings[u][i] - mu, lr, lambda)
                users[u].v, users[u].b = p2, bu2
                items[i].v, items[i].b = q2, bi2
            end
        end
    end

    local predictions = {}
    for u = 1, 4 do
        for i = 1, 4 do
            table.insert(predictions, {
                mf.predict(mu, users[u].b, items[i].b, users[u].v, items[i].v),
                ratings[u][i],
            })
        end
    end
    local error_ = mf.rmse(predictions)
    t.assert(error_ < 0.05, string.format('RMSE %.6f is not below 0.05',
                                          error_))

    -- What predicting the global mean and nothing else costs on this matrix.
    -- Stated as an absolute number rather than as a ratio against `error_`:
    -- the fit reaches zero, and "zero times ten is still zero" is a
    -- comparison that cannot fail. 0.73 against a bar of 0.05 is what makes
    -- the assertion above worth making.
    local baseline = {}
    for u = 1, 4 do
        for i = 1, 4 do
            table.insert(baseline, {mu, ratings[u][i]})
        end
    end
    t.assert(mf.rmse(baseline) > 0.7,
             'the matrix is not flat enough for the mean to fit it')
end

g.test_regularisation_shrinks_the_fit = function()
    -- The same run with a large lambda cannot reach the truth: the penalty
    -- pulls every parameter toward zero and the residual stays.
    local ratings = {}
    local total = 0.0
    for u = 1, 4 do
        ratings[u] = {}
        for i = 1, 4 do
            ratings[u][i] = truth_rating(u, i)
            total = total + ratings[u][i]
        end
    end
    local mu = total / 16

    local function run(lambda)
        local rng = lcg(20160401)
        local users, items = {}, {}
        for u = 1, 4 do
            users[u] = {b = 0.0, v = mf.init_vector(2, rng)}
        end
        for i = 1, 4 do
            items[i] = {b = 0.0, v = mf.init_vector(2, rng)}
        end
        for _ = 1, 2000 do
            for u = 1, 4 do
                for i = 1, 4 do
                    local p2, q2, bu2, bi2 = mf.sgd_step(
                        users[u].v, items[i].v, users[u].b, items[i].b,
                        ratings[u][i] - mu, 0.05, lambda)
                    users[u].v, users[u].b = p2, bu2
                    items[i].v, items[i].b = q2, bi2
                end
            end
        end
        local predictions = {}
        for u = 1, 4 do
            for i = 1, 4 do
                table.insert(predictions, {
                    mf.predict(mu, users[u].b, items[i].b, users[u].v,
                               items[i].v),
                    ratings[u][i],
                })
            end
        end
        return mf.rmse(predictions)
    end

    t.assert(run(0.5) > run(0.0), 'the penalty costs accuracy on this fit')
end
