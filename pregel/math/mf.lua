--[[--
-- Biased matrix factorisation, one SGD step at a time.
--
-- The model behind a rating is
--
--     r_ui ~ mu + b_u + b_i + <p_u, q_i>
--
-- a global mean, a per-user and a per-item offset, and the dot product of two
-- latent vectors. `mu` is a constant computed once from the data rather than
-- learned; everything else is fitted.
--
-- One observation contributes
--
--     J = 0.5 * e^2 + 0.5 * lambda * (|p|^2 + |q|^2 + b_u^2 + b_i^2)
--     e = r - mu - b_u - b_i - <p, q>
--
-- and `sgd_step` takes one gradient step on it. Both biases are regularised
-- here, unlike the L2 in `pregel.math.gd`: there the bias sets a decision
-- threshold that has no business being shrunk, here b_u and b_i are per-user
-- and per-item parameters fitted from as few as one rating each, and shrinking
-- them toward the global mean is the point.
--
-- **`sgd_step` returns new tables and never touches its arguments.** In a
-- Pregel job a user vertex holds (b_u, p_u) and an item vertex holds
-- (b_i, q_i); a step needs both, so one of the two arrives as a message. A
-- message is shared by reference until it is encoded, and a vertex value is
-- read again by the next superstep, so an in-place update would write through
-- to something that never asked for it. It also matters that both new vectors
-- are computed from the *old* p and q rather than one from the other's update:
-- that is what makes the step the gradient of J at the point it started from.
--
-- **`rating` is the residual `r - mu`,** because `sgd_step` never sees mu.
-- Keeping the global mean out of the fitted parameters is what stops b_u and
-- b_i from having to represent it twice. Pass the raw rating and mu = 0 if
-- there is no reason to centre.
--
-- @module pregel.math.mf
--]]--

local strict = require('pregel.utils.strict')
local utils  = require('pregel.utils')
local vector = require('pregel.math.vector')

local sqrt = math.sqrt

-- Latent vectors start small but not at zero: p and q enter the gradient
-- through each other, so a pair that starts at exactly zero has zero gradient
-- in both and never moves. Only the biases would learn.
local DEFAULT_SCALE = 0.1

--- A starting latent vector: `dim` numbers drawn uniformly from
-- [-scale, scale).
--
-- @param dim number of latent factors
-- @param rng optional generator in [0, 1), defaults to math.random
-- @param scale optional half-width, default 0.1
-- @return a new array
-- @function init_vector
local function init_vector(dim, rng, scale)
    return vector.scale(scale or DEFAULT_SCALE, vector.random(dim, rng))
end

--- A fresh model piece for one vertex.
--
-- Carries both sides so the same constructor serves a user vertex and an item
-- vertex: a user reads `b_u` and `p`, an item reads `b_i` and `q`, and each
-- ignores the other pair.
--
-- @param dim number of latent factors
-- @param opts table of `mu` (default 0), `rng` and `scale`
-- @return `{mu = number, b_u = 0, b_i = 0, p = array, q = array}`
-- @function new_model
local function new_model(dim, opts)
    opts = opts or {}
    if type(dim) ~= 'number' or dim < 1 then
        utils.error('new_model wants a positive number of factors, got %s',
                    tostring(dim))
    end
    return {
        mu  = opts.mu or 0.0,
        b_u = 0.0,
        b_i = 0.0,
        p   = init_vector(dim, opts.rng, opts.scale),
        q   = init_vector(dim, opts.rng, opts.scale),
    }
end

--- The predicted rating.
--
-- @param mu the global mean
-- @param bu the user offset
-- @param bi the item offset
-- @param p the user's latent vector
-- @param q the item's latent vector
-- @return number
-- @function predict
local function predict(mu, bu, bi, p, q)
    return mu + bu + bi + vector.dot(p, q)
end

--- One SGD step on the regularised squared error of a single rating.
--
-- @param p the user's latent vector
-- @param q the item's latent vector
-- @param bu the user offset
-- @param bi the item offset
-- @param rating the observed rating with the global mean already subtracted
-- @param lr the learning rate
-- @param lambda the L2 coefficient, applied to the vectors and to both biases
-- @return new p, new q, new b_u, new b_i, and the residual `rating - <the
--   prediction without mu>`
-- @function sgd_step
local function sgd_step(p, q, bu, bi, rating, lr, lambda)
    if #p ~= #q then
        utils.error('latent vectors of different lengths: %d and %d', #p, #q)
    end
    lambda = lambda or 0.0
    local err = rating - bu - bi - vector.dot(p, q)

    -- Both from the old p and q, so this is the gradient of J at the point the
    -- step started from and not a half-updated version of it.
    local p2, q2 = {}, {}
    for k = 1, #p do
        p2[k] = p[k] + lr * (err * q[k] - lambda * p[k])
        q2[k] = q[k] + lr * (err * p[k] - lambda * q[k])
    end

    return p2, q2,
           bu + lr * (err - lambda * bu),
           bi + lr * (err - lambda * bi),
           err
end

--- Root mean squared error over a set of predictions.
--
-- Each sample is a `{prediction, target}` pair. The two are squared apart, so
-- the order within a pair does not change the answer -- but keep it, because
-- the next reader has to know which column is which.
--
-- @param samples array of {prediction, target}
-- @return the RMSE, or nil if there is nothing to average
-- @function rmse
local function rmse(samples)
    local n = #samples
    if n == 0 then
        return nil
    end
    local acc = 0.0
    for i = 1, n do
        local sample = samples[i]
        if type(sample) ~= 'table' or type(sample[1]) ~= 'number' or
           type(sample[2]) ~= 'number' then
            utils.error('sample %d is not a {prediction, target} pair', i)
        end
        local d = sample[1] - sample[2]
        acc = acc + d * d
    end
    return sqrt(acc / n)
end

return strict.strictify({
    new_model   = new_model,
    init_vector = init_vector,
    predict     = predict,
    sgd_step    = sgd_step,
    rmse        = rmse,
})
