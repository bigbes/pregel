--- Dense vectors as plain Lua arrays.
--
-- Everything here is a 1-based Lua array of numbers and nothing else: no ffi,
-- no cdata, no metatables. That is deliberate. These vectors are model weights
-- and latent factors, and in a Pregel job they travel as message payloads and
-- sit in vertex values, so they have to survive msgpack encoding and a round
-- trip through a space. A cdata array does not.
--
-- Every function returns a fresh table and none of them modify their
-- arguments. A vertex hands the same weight vector to several samples in one
-- superstep, and messages are shared by reference until they are encoded, so
-- an in-place update here would be visible in places that never asked for it.
-- The arrays are short (a few hundred entries at most) and the loops around
-- them are the ones doing the real work, so the allocation is not where the
-- time goes.
--
-- @module pregel.math.vector

local strict = require('pregel.utils.strict')

local sqrt = math.sqrt

--- Scalar product of two vectors.
--
-- Both must have the same length; a shorter `y` reads as nil and errors on the
-- multiplication rather than silently truncating, which is what the 2016 code
-- did by zipping the feature vector against the weight vector and dropping
-- whatever hung off the end.
--
-- @param x array of numbers
-- @param y array of numbers, same length as `x`
-- @return number
-- @function dot
local function dot(x, y)
    local acc = 0.0
    for i = 1, #x do
        acc = acc + x[i] * y[i]
    end
    return acc
end

--- `a * x + y`, the BLAS axpy.
--
-- Unlike BLAS this does not accumulate into `y`; see the module comment.
--
-- @param a number
-- @param x array of numbers
-- @param y array of numbers, same length as `x`
-- @return a new array
-- @function axpy
local function axpy(a, x, y)
    local out = {}
    for i = 1, #x do
        out[i] = a * x[i] + y[i]
    end
    return out
end

--- Multiply every element by a scalar.
--
-- @param a number
-- @param x array of numbers
-- @return a new array
-- @function scale
local function scale(a, x)
    local out = {}
    for i = 1, #x do
        out[i] = a * x[i]
    end
    return out
end

--- Element-wise sum.
--
-- @param x array of numbers
-- @param y array of numbers, same length as `x`
-- @return a new array
-- @function add
local function add(x, y)
    local out = {}
    for i = 1, #x do
        out[i] = x[i] + y[i]
    end
    return out
end

--- Euclidean (L2) norm.
--
-- @param x array of numbers
-- @return number
-- @function norm
local function norm(x)
    local acc = 0.0
    for i = 1, #x do
        acc = acc + x[i] * x[i]
    end
    return sqrt(acc)
end

--- A vector of `n` zeros.
--
-- @param n length
-- @return a new array
-- @function zeros
local function zeros(n)
    local out = {}
    for i = 1, n do
        out[i] = 0.0
    end
    return out
end

--- A vector of `n` numbers drawn uniformly from [-1, 1).
--
-- `rng` is any function returning a number in [0, 1) -- math.random by
-- default. Passing one is what makes a test that depends on the initial
-- weights reproducible without touching the global random state.
--
-- @param n length
-- @param rng optional generator, defaults to math.random
-- @return a new array
-- @function random
local function random(n, rng)
    rng = rng or math.random
    local out = {}
    for i = 1, n do
        out[i] = 2 * rng() - 1
    end
    return out
end

return strict.strictify({
    dot    = dot,
    axpy   = axpy,
    scale  = scale,
    add    = add,
    norm   = norm,
    zeros  = zeros,
    random = random,
})
