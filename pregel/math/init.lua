--- The numerical pieces a learning job on Pregel needs.
--
--     local m = require('pregel.math')
--
--     local model = m.gd.new({loss = 'hinge', regulariser = 'l2',
--                             lambda = 1e-4, learning_rate = 0.05})
--     local w = model:train(batches, {dim = #features + 1})
--
--     local scores = m.auc.new()
--     scores:add(m.gd.score(x, w), label)
--     print(scores:result())
--
--     local calibration = m.percentile.new()
--     calibration:add(m.gd.score(x, w))
--     print(calibration:percentile(95))
--
-- All of it works on plain Lua arrays and plain Lua tables, with no ffi and no
-- cdata anywhere. That is the constraint the whole package is built around: a
-- weight vector is a message payload, a percentile counter is a vertex value,
-- and msgpack has to be able to encode both.
--
-- The pieces are usable on their own: `pregel.math.vector` for the array
-- arithmetic, `pregel.math.gd` for linear models and the descent loop,
-- `pregel.math.auc` for ranking quality, and `pregel.math.percentile` for a
-- streaming quantile counter.
--
-- This module only re-exports; every function it names is documented where it
-- is defined.
--
-- @module pregel.math

local strict = require('pregel.utils.strict')

return strict.strictify({
    vector     = require('pregel.math.vector'),
    gd         = require('pregel.math.gd'),
    auc        = require('pregel.math.auc'),
    percentile = require('pregel.math.percentile'),
})
