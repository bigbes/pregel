local t = require('luatest')

local m = require('pregel.math')

local g = t.group('math.init')

g.test_reexports_are_the_modules_themselves = function()
    t.assert_is(m.vector, require('pregel.math.vector'))
    t.assert_is(m.gd, require('pregel.math.gd'))
    t.assert_is(m.auc, require('pregel.math.auc'))
    t.assert_is(m.percentile, require('pregel.math.percentile'))
    t.assert_is(m.mf, require('pregel.math.mf'))
end

-- strictify() is what turns a typo into an error at the call site instead of
-- an "attempt to call a nil value" three frames later.
g.test_a_misspelt_name_is_an_error = function()
    t.assert_error_msg_contains("variable 'percentiles' is not declared",
                                function()
        return m.percentiles
    end)
end

-- One short end-to-end pass over the package: train a model, score with it,
-- and feed the scores to both readers. It is the wiring, not the maths, that
-- this is here to catch.
g.test_the_pieces_fit_together = function()
    local batch = {}
    for _, a in ipairs({-2, -1, -0.5, 0.5, 1, 2}) do
        table.insert(batch, {t = a > 0 and 1 or -1, x = {a, a * a}})
    end

    local model = m.gd.new({
        loss = 'hinge', regulariser = 'l2', lambda = 1e-4,
        learning_rate = 0.1,
    })
    local w = model:train({batch}, {w = m.vector.zeros(3), max_iter = 500})
    t.assert_equals(#w, 3)

    local scores = m.auc.new()
    local calibration = m.percentile.new()
    for _, sample in ipairs(batch) do
        local y = m.gd.score(sample.x, w)
        scores:add(y, sample.t)
        calibration:add(y)
    end

    t.assert_equals(scores:result(), 1.0)
    t.assert_equals(calibration:count(), #batch)
    t.assert_equals(calibration:percentile(100), m.gd.score({2, 4}, w))
end
