local t = require('luatest')

local gd     = require('pregel.math.gd')
local vector = require('pregel.math.vector')

local g = t.group('math.gd')

--
-- Helpers
--

--- Numeric gradient by central differences.
--
-- This is the only check that can tell a gradient which is merely plausible
-- from the gradient of the loss actually being returned. Every analytic
-- gradient below is compared against it.
local function fd_gradient(f, w, h)
    h = h or 1e-6
    local grad = {}
    for i = 1, #w do
        local plus, minus = {}, {}
        for j = 1, #w do
            plus[j], minus[j] = w[j], w[j]
        end
        plus[i]  = plus[i] + h
        minus[i] = minus[i] - h
        grad[i] = (f(plus) - f(minus)) / (2 * h)
    end
    return grad
end

local function assert_vector_almost_equals(got, expected, tolerance, message)
    t.assert_equals(#got, #expected, message)
    for i = 1, #expected do
        t.assert_almost_equals(got[i], expected[i], tolerance,
                               string.format('%s [%d]', message or '', i))
    end
end

--- Compare a loss's own gradient against a finite-difference one at `w`.
local function assert_gradient_matches_fd(loss, label, x, w, message)
    local _, grad = loss.value_and_gradient(label, x, w)
    local numeric = fd_gradient(function(probe)
        local value = loss.value_and_gradient(label, x, probe)
        return value
    end, w)
    assert_vector_almost_equals(grad, numeric, 1e-6, message)
end

--
-- score(): the bias convention
--

g.test_score_uses_w1_as_the_bias = function()
    -- y = w[1] + x[1] * w[2] + x[2] * w[3]
    t.assert_equals(gd.score({2, -1}, {0.5, 1, 2}), 0.5)
    t.assert_equals(gd.score({}, {7}), 7)
end

-- The 2016 model zipped x against w element by element, which silently dropped
-- the last weight and gave w[1] the role of x[1]'s coefficient. A length
-- mismatch is a bug in the caller, not something to absorb.
g.test_score_refuses_a_mismatched_weight_vector = function()
    t.assert_error_msg_contains('one longer than x', gd.score, {1, 2}, {1, 2})
    t.assert_error_msg_contains('one longer than x', gd.score, {1, 2},
                                {1, 2, 3, 4})
end

--
-- Losses: hand-computed values
--

g.test_hinge_value_and_gradient_active = function()
    -- y = 0.5 + 2*1 + (-1)*2 = 0.5, t*y = 0.5 < 1, so the hinge is active.
    local value, grad = gd.loss.hinge.value_and_gradient(1, {2, -1},
                                                         {0.5, 1, 2})
    t.assert_almost_equals(value, 0.5, 1e-12)
    -- grad = -t * (1, x[1], x[2])
    assert_vector_almost_equals(grad, {-1, -2, 1}, 1e-12, 'hinge gradient')
end

g.test_hinge_is_flat_outside_the_margin = function()
    -- y = 0.5 + 2*2 + (-1)*1 = 3.5, t*y >= 1.
    local value, grad = gd.loss.hinge.value_and_gradient(1, {2, -1},
                                                         {0.5, 2, 1})
    t.assert_equals(value, 0)
    t.assert_equals(grad, {0, 0, 0})
end

g.test_hinge_negative_label = function()
    -- y = 1, t = -1, t*y = -1 < 1, loss = 1 - (-1)(1) = 2.
    local value, grad = gd.loss.hinge.value_and_gradient(-1, {1}, {0, 1})
    t.assert_almost_equals(value, 2, 1e-12)
    assert_vector_almost_equals(grad, {1, 1}, 1e-12, 'hinge gradient')
end

g.test_logistic_value_and_gradient = function()
    -- y = 1, t = -1: loss = log(1 + e^1), dL/dy = 1 / (1 + e^-1).
    local value, grad = gd.loss.logistic.value_and_gradient(-1, {1}, {0, 1})
    t.assert_almost_equals(value, math.log(1 + math.exp(1)), 1e-12)
    local dy = 1 / (1 + math.exp(-1))
    assert_vector_almost_equals(grad, {dy, dy}, 1e-12, 'logistic gradient')
end

g.test_logistic_does_not_overflow = function()
    -- exp(-t*y) overflows to inf around 710 and log(1 + inf) is inf, not the
    -- -t*y it should be. A single confidently wrong sample would otherwise
    -- turn the whole batch loss into inf and every weight into a NaN.
    local value, grad = gd.loss.logistic.value_and_gradient(-1, {1000},
                                                            {0, 1})
    t.assert_almost_equals(value, 1000, 1e-9)
    t.assert_almost_equals(grad[1], 1, 1e-12)

    -- The saturated side must produce a zero gradient, not a NaN.
    local far, fgrad = gd.loss.logistic.value_and_gradient(1, {1000}, {0, 1})
    t.assert_almost_equals(far, 0, 1e-12)
    t.assert_equals(fgrad[1], fgrad[1], 'gradient is not NaN')
    t.assert_almost_equals(fgrad[1], 0, 1e-12)
end

g.test_squared_value_and_gradient = function()
    -- y = 1 + 2*0.5 = 2, t = 3, loss = 0.5 * (2 - 3)^2 = 0.5.
    local value, grad = gd.loss.squared.value_and_gradient(3, {2}, {1, 0.5})
    t.assert_almost_equals(value, 0.5, 1e-12)
    assert_vector_almost_equals(grad, {-1, -2}, 1e-12, 'squared gradient')
end

--
-- Losses: finite-difference checks
--

g.test_hinge_gradient_matches_finite_differences = function()
    -- Away from the kink in both directions, and for both labels.
    assert_gradient_matches_fd(gd.loss.hinge, 1, {2, -1, 0.25},
                               {0.5, 1, 2, -0.5}, 'hinge, active')
    assert_gradient_matches_fd(gd.loss.hinge, -1, {2, -1, 0.25},
                               {-1.5, -1, 0.5, 0.25}, 'hinge, active negative')
    assert_gradient_matches_fd(gd.loss.hinge, 1, {2, -1, 0.25},
                               {3, 2, 1, 1}, 'hinge, inactive')
end

g.test_logistic_gradient_matches_finite_differences = function()
    assert_gradient_matches_fd(gd.loss.logistic, 1, {2, -1, 0.25},
                               {0.5, 1, 2, -0.5}, 'logistic')
    assert_gradient_matches_fd(gd.loss.logistic, -1, {0.3, 1.7},
                               {-0.25, 0.75, -1.25}, 'logistic negative')
end

g.test_squared_gradient_matches_finite_differences = function()
    assert_gradient_matches_fd(gd.loss.squared, 3.5, {2, -1, 0.25},
                               {0.5, 1, 2, -0.5}, 'squared')
end

--
-- L2
--

g.test_l2_value_and_gradient_exclude_the_bias = function()
    local value, grad = gd.regulariser.l2.value_and_gradient({5, 2, -3})
    -- 2^2 + (-3)^2 -- w[1] does not contribute.
    t.assert_almost_equals(value, 13, 1e-12)
    t.assert_equals(grad[1], 0)
    assert_vector_almost_equals(grad, {0, 4, -6}, 1e-12, 'l2 gradient')
end

g.test_l2_gradient_matches_finite_differences = function()
    local w = {5, 2, -3, 0.5}
    local _, grad = gd.regulariser.l2.value_and_gradient(w)
    local numeric = fd_gradient(function(probe)
        local value = gd.regulariser.l2.value_and_gradient(probe)
        return value
    end, w)
    -- The bias slot too: the numeric derivative of the penalty with respect to
    -- w[1] is 0 exactly because w[1] is not in the sum.
    assert_vector_almost_equals(grad, numeric, 1e-6, 'l2 gradient')
end

g.test_regularised_gradient_matches_finite_differences = function()
    local model = gd.new({
        loss = 'logistic', regulariser = 'l2', lambda = 0.3,
    })
    local x, w = {2, -1, 0.25}, {0.5, 1, 2, -0.5}
    local _, grad = model:loss_and_gradient(1, x, w)
    local numeric = fd_gradient(function(probe)
        local value = model:loss_and_gradient(1, x, probe)
        return value
    end, w)
    assert_vector_almost_equals(grad, numeric, 1e-6, 'regularised gradient')

    -- And the penalty really is in there: with lambda 0 the gradient is the
    -- bare loss gradient, and the two differ.
    local bare = gd.new({loss = 'logistic', regulariser = 'l2', lambda = 0})
    local _, plain = bare:loss_and_gradient(1, x, w)
    t.assert_not_equals(grad[2], plain[2])
    -- The bias slot is the one the penalty never touches.
    t.assert_almost_equals(grad[1], plain[1], 1e-12)
end

--
-- Learning rates
--

g.test_constant_learning_rate = function()
    local rate = gd.learning_rate.constant(0.25)
    t.assert_equals(rate(1), 0.25)
    t.assert_equals(rate(1000), 0.25)
    t.assert_equals(gd.learning_rate.constant()(1), 0.01)
end

g.test_inverse_decay_learning_rate = function()
    local rate = gd.learning_rate.inverse_decay(1.0, 1.0)
    t.assert_almost_equals(rate(1), 0.5, 1e-12)
    t.assert_almost_equals(rate(3), 0.25, 1e-12)
    local slow = gd.learning_rate.inverse_decay(1.0, 0.1)
    t.assert_almost_equals(slow(10), 0.5, 1e-12)
end

--
-- new() / update() / initialize()
--

g.test_new_reads_its_options = function()
    -- GradientDescent_new(loss, lr) in the 2016 code took both and used
    -- neither; its one call site asked for hinge + l2 and got hinge, lambda 0
    -- and a hard-coded constant rate.
    local model = gd.new({
        loss = 'squared', regulariser = 'l2', lambda = 2,
        learning_rate = 0.5,
    })
    t.assert_is(model.loss, gd.loss.squared)
    t.assert_is(model.regulariser, gd.regulariser.l2)
    t.assert_equals(model.lambda, 2)
    t.assert_equals(model.learning_rate(7), 0.5)
end

g.test_new_rejects_unknown_names = function()
    t.assert_error_msg_contains('unknown loss: perceptron', gd.new,
                                {loss = 'perceptron'})
    t.assert_error_msg_contains('unknown regulariser: l1', gd.new,
                                {regulariser = 'l1'})
    t.assert_error_msg_contains('learning_rate must be', gd.new,
                                {learning_rate = 'fast'})
end

g.test_new_accepts_a_custom_loss = function()
    local mine = {
        value_and_gradient = function(_, _, w)
            return w[1], {1, 0}
        end,
    }
    local model = gd.new({loss = mine})
    local value, grad = model:loss_and_gradient(1, {0}, {3, 4})
    t.assert_equals(value, 3)
    t.assert_equals(grad, {1, 0})
end

g.test_update_is_a_descent_step = function()
    local model = gd.new({learning_rate = 0.5})
    local w = {1, 2, 3}
    local updated = model:update(1, w, {2, 4, -6})
    t.assert_equals(updated, {0, 0, 6})
    -- The old weights are untouched -- a Pregel vertex keeps the previous
    -- model around while the new one is in flight.
    t.assert_equals(w, {1, 2, 3})
end

g.test_update_follows_the_schedule = function()
    local model = gd.new({
        learning_rate = gd.learning_rate.inverse_decay(1.0, 1.0),
    })
    t.assert_equals(model:update(1, {0}, {1}), {-0.5})
    t.assert_equals(model:update(3, {0}, {1}), {-0.25})
end

g.test_initialize = function()
    local model = gd.new({})
    local w = model:initialize(5, function() return 0.5 end)
    t.assert_equals(w, {0, 0, 0, 0, 0})
    t.assert_equals(#model:initialize(3), 3)

    -- The rng given to new() is the default for initialize().
    local seeded = gd.new({rng = function() return 1 end})
    t.assert_equals(seeded:initialize(2), {1, 1})
end

--
-- train()
--

-- Small deterministic problems: a fixed weight vector rather than a random
-- one, so a failure is a failure and not an unlucky draw.

local function grid_batch(predicate, label_of)
    local batch = {}
    for a = -2, 2, 0.5 do
        for b = -2, 2, 0.5 do
            if predicate(a, b) then
                table.insert(batch, {t = label_of(a, b), x = {a, b}})
            end
        end
    end
    return batch
end

g.test_train_separates_a_linearly_separable_problem_with_hinge = function()
    -- Everything at least 0.75 away from the line a + b = 0, labelled by which
    -- side it is on.
    local batch = grid_batch(
        function(a, b) return math.abs(a + b) >= 0.75 end,
        function(a, b) return a + b > 0 and 1 or -1 end
    )
    t.assert(#batch > 20, 'the problem has some points in it')

    local model = gd.new({loss = 'hinge', learning_rate = 0.2})
    local w, history = model:train({batch}, {
        w = {0, 0, 0}, alpha = 1.0, epsilon = 1e-10, max_iter = 5000,
    })

    t.assert(history.converged, 'training converged')
    for _, sample in ipairs(batch) do
        local y = gd.score(sample.x, w)
        t.assert(sample.t * y > 0, string.format(
            'point (%g, %g) with label %d scored %g', sample.x[1],
            sample.x[2], sample.t, y))
    end
    -- The hinge is satisfied everywhere, so the loss is exactly zero.
    t.assert_almost_equals(history.losses[history.iterations], 0, 1e-12)
end

g.test_train_learns_a_boundary_that_needs_the_bias = function()
    -- Labels split on a >= 3, so no line through the origin can separate them:
    -- with b = 0 a bias-free model would need w2 * 1 < 0 and w2 * 4 > 0. This
    -- is the problem the 2016 model could not have solved, because its
    -- "bias" was really the weight of the first feature.
    local batch = {}
    for _, a in ipairs({1, 2, 4, 5}) do
        for _, b in ipairs({-1, 0, 1}) do
            table.insert(batch, {t = a >= 4 and 1 or -1, x = {a, b}})
        end
    end

    local model = gd.new({loss = 'hinge', learning_rate = 0.05})
    local w, history = model:train({batch}, {
        w = {0, 0, 0}, alpha = 0.2, epsilon = 1e-10, max_iter = 5000,
    })

    t.assert(history.converged, 'training converged')
    for _, sample in ipairs(batch) do
        t.assert(sample.t * gd.score(sample.x, w) > 0,
                 string.format('point (%g, %g) misclassified', sample.x[1],
                               sample.x[2]))
    end
    -- The boundary sits at a = 3, so w[1] / w[2] is about -3.
    t.assert(w[1] < 0, 'the bias carries the offset of the boundary')
    t.assert_almost_equals(w[1] / w[2], -3, 0.1, 'boundary at a = 3')
    -- The history holds the *averaged* loss, which only approaches the raw
    -- zero asymptotically, so this cannot be as tight as the alpha = 1 case.
    t.assert_almost_equals(history.losses[history.iterations], 0, 1e-8)
end

-- Measured on the problem above: the raw batch loss falls by lr * |g|^2 per
-- step, except where a step moves a sample across the hinge and two
-- consecutive batch losses come out bit-for-bit equal. With alpha = 1 -- no
-- averaging -- the convergence test reads that plateau as convergence and
-- stops at 0.67 with six of twelve points on the wrong side. The averaged
-- loss lags behind a falling raw loss, so it cannot repeat itself while the
-- run is still making progress, and the same problem trains to zero.
g.test_averaging_keeps_a_flat_step_from_reading_as_convergence = function()
    local batch = {}
    for _, a in ipairs({1, 2, 4, 5}) do
        for _, b in ipairs({-1, 0, 1}) do
            table.insert(batch, {t = a >= 4 and 1 or -1, x = {a, b}})
        end
    end
    local model = gd.new({loss = 'hinge', learning_rate = 0.05})

    local unaveraged, uhistory = model:train({batch}, {
        w = {0, 0, 0}, alpha = 1.0, epsilon = 1e-10, max_iter = 5000,
    })
    t.assert(uhistory.converged, 'the plateau reads as convergence')
    t.assert(uhistory.losses[uhistory.iterations] > 0.5,
             'and it stops nowhere near the optimum')
    local wrong = 0
    for _, sample in ipairs(batch) do
        if sample.t * gd.score(sample.x, unaveraged) <= 0 then
            wrong = wrong + 1
        end
    end
    t.assert(wrong > 0, 'with points still misclassified')

    local _, ahistory = model:train({batch}, {
        w = {0, 0, 0}, alpha = 0.2, epsilon = 1e-10, max_iter = 5000,
    })
    t.assert(ahistory.iterations > uhistory.iterations * 10,
             'averaging keeps it running')
    t.assert_almost_equals(ahistory.losses[ahistory.iterations], 0, 1e-8)
end

g.test_train_recovers_a_linear_regression_with_squared_loss = function()
    -- t = 0.5 + 2*a - 3*b exactly, on a centred grid so the problem is well
    -- conditioned and plain gradient descent gets there.
    local truth = {0.5, 2, -3}
    local batch = {}
    for _, a in ipairs({-1, -0.5, 0, 0.5, 1}) do
        for _, b in ipairs({-1, -0.5, 0, 0.5, 1}) do
            table.insert(batch, {t = truth[1] + truth[2] * a + truth[3] * b,
                                 x = {a, b}})
        end
    end

    local model = gd.new({loss = 'squared', learning_rate = 1.0})
    local w, history = model:train({batch}, {
        w = {0, 0, 0}, alpha = 1.0, epsilon = 1e-16, max_iter = 500,
    })

    assert_vector_almost_equals(w, truth, 1e-3, 'recovered weights')
    t.assert(history.iterations < 500, 'it converged before the iteration cap')
end

g.test_train_derives_the_weight_vector_length_from_the_first_sample = function()
    local batch = {{t = 1, x = {1, 2, 3}}}
    local model = gd.new({loss = 'squared', learning_rate = 0.001})
    local w = model:train({batch}, {
        rng = function() return 0.5 end, max_iter = 1,
    })
    t.assert_equals(#w, 4)
end

g.test_train_accepts_dim_instead_of_weights = function()
    local model = gd.new({loss = 'squared', learning_rate = 0})
    local w = model:train({{{t = 0, x = {1, 2}}}}, {
        dim = 3, rng = function() return 0.5 end, max_iter = 1,
    })
    t.assert_equals(w, {0, 0, 0})
end

g.test_train_rejects_a_dim_that_contradicts_the_weights = function()
    local model = gd.new({})
    t.assert_error_msg_contains('dim 4 and a weight vector of length 3',
                                function()
        model:train({{{t = 1, x = {1, 2}}}}, {w = {0, 0, 0}, dim = 4})
    end)
end

g.test_train_stops_when_the_batch_source_runs_dry = function()
    local left = 3
    local model = gd.new({loss = 'squared', learning_rate = 0.01})
    local _, history = model:train(function()
        left = left - 1
        if left < 0 then
            return nil
        end
        return {{t = 1, x = {1}}}
    end, {w = {0, 0}, alpha = 1.0, epsilon = 0, max_iter = 100})

    t.assert_equals(history.iterations, 3)
    t.assert_equals(history.converged, false)
    t.assert_equals(#history.losses, 3)
end

g.test_train_cycles_an_array_of_batches = function()
    local seen = {}
    local model = gd.new({loss = 'squared', learning_rate = 0})
    model:train({
        {{t = 1, x = {1}}},
        {{t = 2, x = {2}}},
    }, {
        w = {0, 0}, alpha = 1.0, epsilon = -1, max_iter = 5,
        -- epsilon of -1 can never fire, so the cap is what stops it.
    })
    -- Losses differ per batch, which is how the cycling shows: 1, 2, 1, 2, 1.
    local _, history = model:train({
        {{t = 1, x = {1}}},
        {{t = 3, x = {1}}},
    }, {w = {0, 0}, alpha = 1.0, epsilon = -1, max_iter = 4})
    for i, loss in ipairs(history.losses) do
        seen[i] = loss
    end
    t.assert_equals(#seen, 4)
    t.assert_almost_equals(seen[1], 0.5, 1e-12)   -- 0.5 * (0 - 1)^2
    t.assert_almost_equals(seen[2], 4.5, 1e-12)   -- 0.5 * (0 - 3)^2
    t.assert_almost_equals(seen[3], 0.5, 1e-12)
    t.assert_almost_equals(seen[4], 4.5, 1e-12)
end

g.test_train_averages_the_loss_exponentially = function()
    -- alpha 0.25 with a constant batch loss of 4: 4, 4, 4 ... but starting
    -- from the first value, so it stays at 4 and converges immediately.
    -- Alternating batches make the averaging visible.
    local model = gd.new({loss = 'squared', learning_rate = 0})
    local _, history = model:train({
        {{t = 2, x = {0}}},   -- loss 2
        {{t = 0, x = {0}}},   -- loss 0
    }, {w = {0, 0}, alpha = 0.25, epsilon = -1, max_iter = 3})
    t.assert_almost_equals(history.losses[1], 2, 1e-12)
    t.assert_almost_equals(history.losses[2], 1.5, 1e-12)          -- .75*2
    t.assert_almost_equals(history.losses[3], 0.75 * 1.5 + 0.5,
                           1e-12)
end

g.test_train_averages_the_gradient_over_the_batch = function()
    -- The 2016 loop divided the loss by the batch size and summed the
    -- gradient, so doubling a batch doubled the step. Two identical samples
    -- must move the weights exactly as far as one.
    local sample = {t = 4, x = {2}}
    local model = gd.new({loss = 'squared', learning_rate = 0.1})
    local one = model:train({{sample}},
                            {w = {0, 0}, alpha = 1.0, epsilon = -1,
                             max_iter = 1})
    local four = model:train({{sample, sample, sample, sample}},
                             {w = {0, 0}, alpha = 1.0, epsilon = -1,
                              max_iter = 1})
    assert_vector_almost_equals(four, one, 1e-12, 'batch size independence')
end

g.test_train_rejects_bad_batch_sources = function()
    local model = gd.new({})
    t.assert_error_msg_contains('empty array of batches', function()
        model:train({}, {w = {0}})
    end)
    t.assert_error_msg_contains('function or an array', function()
        model:train(42, {w = {0}})
    end)
end

g.test_train_returns_plain_arrays = function()
    local model = gd.new({loss = 'squared', learning_rate = 0.01})
    local w, history = model:train({{{t = 1, x = {1, 2}}}},
                                   {w = {0, 0, 0}, max_iter = 2})
    t.assert_equals(getmetatable(w), nil)
    t.assert_equals(getmetatable(history.losses), nil)
    t.assert_equals(type(history.iterations), 'number')
    t.assert_equals(type(history.converged), 'boolean')
end

--
-- The regularised objective actually shrinks the weights.
--

g.test_regularisation_shrinks_the_weights_but_not_the_bias = function()
    local batch = {{t = 1, x = {1, 1}}, {t = -1, x = {-1, -1}}}
    local plain = gd.new({loss = 'hinge', learning_rate = 0.1})
    local penalised = gd.new({
        loss = 'hinge', regulariser = 'l2', lambda = 0.5,
        learning_rate = 0.1,
    })
    local w0 = plain:train({batch}, {w = {0, 0, 0}, alpha = 1.0,
                                     epsilon = 1e-12, max_iter = 500})
    local w1 = penalised:train({batch}, {w = {0, 0, 0}, alpha = 1.0,
                                         epsilon = 1e-12, max_iter = 500})
    t.assert(vector.norm({w1[2], w1[3]}) < vector.norm({w0[2], w0[3]}),
             'the penalty pulled the feature weights in')
end
