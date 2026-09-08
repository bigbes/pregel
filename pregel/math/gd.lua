--[[--
-- Linear models trained by minibatch gradient descent.
--
-- The model is a linear score with an explicit bias:
--
--     y(x, w) = w[1] + sum_i x[i] * w[i + 1]
--
-- so `w` is one longer than `x`: **w[1] is the bias and w[i + 1] weighs
-- x[i]**. The feature vector never carries a constant 1 term of its own. This
-- is the convention the 2016 code meant to use -- its hinge gradient is laid
-- out for exactly this, writing the derivative for x[i] into slot i + 1 -- but
-- did not implement: `scalar_product(x, parameters)` zipped the two arrays
-- element by element, so w[1] came out as the weight of x[1], the last weight
-- was dropped on the floor, and the model had no constant term at all.
--
-- A loss returns the value and the gradient as two results:
--
--     value_and_gradient(t, x, w) -> loss, grad
--
-- and `grad` is a full-length vector aligned with `w`, gradient of the loss
-- with respect to each weight. The old code returned one table with the loss
-- *value* in slot 1 and the gradient in slots 2..n+1, which is the root of
-- most of what was wrong with it: `update()` as written subtracted the loss
-- value from w[1], and the training loop that called it had to shift every
-- gradient down by one to compensate. Two readings of the same table, and the
-- L2 regulariser was written against the other one -- see `l2` below.
--
-- A regulariser has a shorter signature, `value_and_gradient(w)`: it is a
-- function of the weights alone, and passing it the sample as well only
-- invited the confusion above.
--
-- @module pregel.math.gd
--]]--

local utils  = require('pregel.utils')
local strict = require('pregel.utils.strict')
local vector = require('pregel.math.vector')

local abs, exp, log = math.abs, math.exp, math.log

-- Defaults from the 2016 GDParams table (test-avro/constants.lua):
-- max.gd.iter, gd.loss.averaging.factor, gd.loss.convergence.factor.
local DEFAULT_MAX_ITER = 300
local DEFAULT_ALPHA    = 0.2
local DEFAULT_EPSILON  = 1e-4

--- The linear score, bias included.
--
-- @param x feature vector, length n
-- @param w weight vector, length n + 1, w[1] the bias
-- @return number
-- @function score
local function score(x, w)
    if #w ~= #x + 1 then
        utils.error('weight vector of length %d for %d features: w must be ' ..
                    'one longer than x (w[1] is the bias)', #w, #x)
    end
    local acc = w[1]
    for i = 1, #x do
        acc = acc + x[i] * w[i + 1]
    end
    return acc
end

--- Spread dL/dy over the weights.
--
-- Every loss here is a function of the linear score alone, so its gradient is
-- dL/dy times dy/dw, and dy/dw is 1 for the bias and x[i] for the rest.
local function chain(dy, x)
    local grad = {dy}
    for i = 1, #x do
        grad[i + 1] = dy * x[i]
    end
    return grad
end

--- Hinge loss, `max(0, 1 - t * y)`, for labels t in {-1, +1}.
--
-- Zero gradient at the kink (t * y == 1 exactly) -- the same side the 2016
-- code picked, and the reason a finite-difference check has to stay away from
-- that point.
--
-- @table hinge
local hinge = {
    value_and_gradient = function(t, x, w)
        local y = score(x, w)
        if t * y < 1 then
            return 1 - t * y, chain(-t, x)
        end
        return 0.0, vector.zeros(#w)
    end,
}

--- Logistic loss, `log(1 + exp(-t * y))`, for labels t in {-1, +1}.
--
-- @table logistic
local logistic = {
    value_and_gradient = function(t, x, w)
        local y = score(x, w)
        local z = -t * y
        local value
        -- exp(z) overflows to inf for z around 710, and log(1 + inf) is inf
        -- rather than the z it should be. Fold the large-z case into the
        -- exponent instead, where the argument is negative and cannot blow up.
        if z > 0 then
            value = z + log(1 + exp(-z))
        else
            value = log(1 + exp(z))
        end
        -- dL/dy = -t * sigmoid(-t * y). Written as a reciprocal so a large
        -- t * y saturates to 0 through exp(t * y) = inf rather than to a NaN.
        return value, chain(-t / (1 + exp(t * y)), x)
    end,
}

--- Squared loss, `0.5 * (y - t)^2`, for a real-valued target t.
--
-- The half is there so that dL/dy is exactly (y - t) with no stray factor of
-- two; a caller comparing losses across the three must know it is here.
--
-- @table squared
local squared = {
    value_and_gradient = function(t, x, w)
        local y = score(x, w)
        local d = y - t
        return 0.5 * d * d, chain(d, x)
    end,
}

--- L2 penalty on the weights, `sum_{i >= 2} w[i]^2`, bias excluded.
--
-- The bias is left out because it sets the decision threshold rather than the
-- shape of the boundary: shrinking it toward zero pulls the model toward
-- predicting the positive class at exactly y = 0, which has nothing to do with
-- keeping the weights small.
--
-- The 2016 version got both halves of this wrong. It wrote `2 * w[idx]` into
-- gradient slot `idx`, but the training loop it was written for mapped slot
-- `idx` onto `w[idx - 1]` -- so every weight was pulled toward zero in
-- proportion to its *neighbour's* value, and the last one was never penalised
-- at all. And the slot it zeroed to "skip the bias" landed on the weight of
-- the first feature, because that model had no bias to skip. None of it ever
-- ran: `GradientDescent_new` hard-coded lambda to 0.
--
-- @table l2
local l2 = {
    value_and_gradient = function(w)
        local value = 0.0
        local grad = {0.0}
        for i = 2, #w do
            value = value + w[i] * w[i]
            grad[i] = 2 * w[i]
        end
        return value, grad
    end,
}

--- A learning rate that never changes.
--
-- @param c the rate, default 0.01
-- @return function(iteration) -> number
-- @function constant
local function constant(c)
    c = c or 0.01
    return function()
        return c
    end
end

--- A learning rate of `c / (1 + k * iteration)`.
--
-- Iterations are 1-based, so the first step is already c / (1 + k) rather than
-- c. The point of the decay is that the averaged loss can settle: with a fixed
-- rate a minibatch run bounces around the optimum forever and the convergence
-- test in `train` only fires because the bouncing happens to be small.
--
-- @param c the initial scale, default 0.01
-- @param k the decay rate, default 1
-- @return function(iteration) -> number
-- @function inverse_decay
local function inverse_decay(c, k)
    c = c or 0.01
    k = k or 1
    return function(iteration)
        return c / (1 + k * iteration)
    end
end

local named_losses = {
    hinge    = hinge,
    logistic = logistic,
    squared  = squared,
}

local named_regularisers = {
    l2 = l2,
}

local function resolve(what, value, table_of_names, default)
    if value == nil then
        return default
    end
    if type(value) == 'string' then
        local found = table_of_names[value]
        if found == nil then
            utils.error('unknown %s: %s', what, value)
        end
        return found
    end
    if type(value) ~= 'table' or not utils.is_callable(value.value_and_gradient) then
        utils.error('%s must be a name or a table with value_and_gradient()',
                    what)
    end
    return value
end

local gd_mt

--- Draw a starting weight vector.
--
-- `dim` is the length of the weight vector -- one more than the number of
-- features, because of the bias.
--
-- @param dim length of the weight vector
-- @param rng optional generator in [0, 1), defaults to math.random
-- @return a new array of `dim` numbers in [-1, 1)
-- @function initialize
local function initialize(self, dim, rng)
    return vector.random(dim, rng or self.rng)
end

--- Loss and gradient of the regularised objective at one sample.
--
-- @param t the label (in {-1, +1} for hinge and logistic, real for squared)
-- @param x the feature vector
-- @param w the weight vector, one longer than `x`
-- @return loss, gradient
-- @function loss_and_gradient
local function loss_and_gradient(self, t, x, w)
    local value, grad = self.loss.value_and_gradient(t, x, w)
    if self.regulariser ~= nil and self.lambda ~= 0 then
        local rvalue, rgrad = self.regulariser.value_and_gradient(w)
        value = value + self.lambda * rvalue
        grad = vector.axpy(self.lambda, rgrad, grad)
    end
    return value, grad
end

--- One descent step: `w - rate(iteration) * grad`.
--
-- @param iteration 1-based iteration number, for the learning rate schedule
-- @param w the current weights
-- @param grad a gradient aligned with `w`
-- @return a new weight vector
-- @function update
local function update(self, iteration, w, grad)
    return vector.axpy(-self.learning_rate(iteration), grad, w)
end

--- Run the minibatch loop.
--
-- `batches` is either a function returning the next batch (nil ends the run)
-- or an array of batches, which is cycled until convergence or `max_iter`. A
-- batch is an array of samples `{t = label, x = features}`.
--
-- Both the loss and the gradient are averaged over the batch. The 2016 loop
-- divided the loss by the batch size but *summed* the gradient, so the size of
-- a step scaled with the size of a batch and the learning rate meant something
-- different for the 500-sample train batches than it would have for any other
-- number.
--
-- Convergence is on an exponentially averaged loss, as it was there:
--
--     avg <- (1 - alpha) * avg + alpha * batch_loss
--
-- and the run stops once one iteration moves `avg` by less than `epsilon`.
-- The averaging is what makes the test meaningful at all -- a raw minibatch
-- loss jumps around with whichever samples the batch happened to contain, and
-- two consecutive batches land within epsilon of each other by luck long
-- before the model has settled.
--
-- @param batches a function or an array of batches
-- @param opts table of w, dim, rng, alpha, epsilon, max_iter
-- @return the final weights, and a history
--   `{iterations = number, losses = array, converged = boolean}`
-- @function train
local function train(self, batches, opts)
    opts = opts or {}
    local alpha    = opts.alpha or DEFAULT_ALPHA
    local epsilon  = opts.epsilon or DEFAULT_EPSILON
    local max_iter = opts.max_iter or DEFAULT_MAX_ITER

    local next_batch
    if utils.is_callable(batches) then
        next_batch = batches
    elseif type(batches) == 'table' then
        local n, i = #batches, 0
        if n == 0 then
            utils.error('train() got an empty array of batches')
        end
        next_batch = function()
            i = i % n + 1
            return batches[i]
        end
    else
        utils.error('train() wants a function or an array of batches, got %s',
                    type(batches))
    end

    local w = opts.w
    if w ~= nil and opts.dim ~= nil and #w ~= opts.dim then
        utils.error('train() got dim %d and a weight vector of length %d',
                    opts.dim, #w)
    end
    if w == nil and opts.dim ~= nil then
        w = self:initialize(opts.dim, opts.rng)
    end

    local history = {iterations = 0, losses = {}, converged = false}
    local averaged = nil

    for iteration = 1, max_iter do
        local batch = next_batch()
        if batch == nil or #batch == 0 then
            break
        end
        -- Deferred so a caller that passes neither `w` nor `dim` still gets a
        -- correctly sized vector: the first sample says how long it has to be.
        if w == nil then
            w = self:initialize(#batch[1].x + 1, opts.rng)
        end

        local total = 0.0
        local grad = vector.zeros(#w)
        for _, sample in ipairs(batch) do
            local value, g = self:loss_and_gradient(sample.t, sample.x, w)
            total = total + value
            for i = 1, #w do
                grad[i] = grad[i] + g[i]
            end
        end
        local size = #batch
        total = total / size
        for i = 1, #w do
            grad[i] = grad[i] / size
        end

        history.iterations = iteration
        local previous = averaged
        if averaged == nil then
            averaged = total
        else
            averaged = (1 - alpha) * averaged + alpha * total
        end
        history.losses[iteration] = averaged

        if previous ~= nil and abs(averaged - previous) < epsilon then
            history.converged = true
            break
        end

        w = self:update(iteration, w, grad)
    end

    return w, history
end

gd_mt = {
    __index = {
        initialize        = initialize,
        loss_and_gradient = loss_and_gradient,
        update            = update,
        train             = train,
    },
}

--- Build a gradient descent optimiser.
--
-- `loss` and `regulariser` are either a name ('hinge', 'logistic', 'squared';
-- 'l2') or a table of the shape described at the top of this module.
-- `learning_rate` is either a number, meaning a constant rate, or a function
-- of the iteration number.
--
-- Unlike `GradientDescent_new` in the 2016 code, which took a loss and a
-- learning rate and then ignored both -- its only call site asked for
-- ('hinge', 'l2') and got a hard-coded hinge, lambda 0 and a constant
-- 0.00005 -- every option here is actually read.
--
-- @param opts table of loss, regulariser, lambda, learning_rate, rng
-- @return an optimiser
-- @function new
local function new(opts)
    opts = opts or {}
    local lambda = opts.lambda or 0.0
    local rate = opts.learning_rate
    if rate == nil then
        rate = constant()
    elseif type(rate) == 'number' then
        rate = constant(rate)
    elseif not utils.is_callable(rate) then
        utils.error('learning_rate must be a number or a function, got %s',
                    type(rate))
    end
    return setmetatable({
        loss          = resolve('loss', opts.loss, named_losses, hinge),
        regulariser   = resolve('regulariser', opts.regulariser,
                                named_regularisers, nil),
        lambda        = lambda,
        learning_rate = rate,
        rng           = opts.rng,
    }, gd_mt)
end

return strict.strictify({
    new = new,
    score = score,
    loss = strict.strictify({
        hinge    = hinge,
        logistic = logistic,
        squared  = squared,
    }),
    regulariser = strict.strictify({
        l2 = l2,
    }),
    learning_rate = strict.strictify({
        constant      = constant,
        inverse_decay = inverse_decay,
    }),
})
