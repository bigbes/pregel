--[[--
-- Area under the ROC curve, by the rank statistic.
--
-- AUC is the probability that a randomly drawn positive scores above a
-- randomly drawn negative, with a tie counting as half. Written out as a
-- double loop that is O(n^2); written as the Mann-Whitney statistic it is one
-- sort:
--
--     AUC = (sum of the ranks of the positives - P * (P + 1) / 2) / (P * N)
--
-- where the ranks run 1..n over the samples sorted by ascending score and tied
-- scores share the average of the ranks they span. The subtracted term is the
-- rank sum the positives would have if they occupied the bottom of the
-- ordering, which is what turns a rank sum into a count of winning pairs.
--
-- Tie handling is the part worth stating: without averaging, two samples with
-- the same score are ordered by whatever the sort happened to do, so a model
-- that gives every sample the same score scores 1 or 0 depending on the
-- sort's internals instead of the 0.5 it deserves.
--
-- Everything here was rewritten rather than ported. The 2016 auc.lua called
-- `qsort2a(truth, probability)`, which sorts its *first* argument -- so it
-- sorted the labels and carried the scores along, then read the sorted labels
-- as the scores. On top of that it assigned rank i + 1 instead of i, inflating
-- every AUC by 1 / N; its tie-averaging loop broke on the first equal element
-- rather than the first unequal one and then wrote one rank past the end of
-- the run; and it tried to skip the run by assigning to the `for` control
-- variable, which Lua ignores.
--
-- @module pregel.math.auc
--]]--

local strict = require('pregel.utils.strict')
local utils  = require('pregel.utils')

--- AUC of a set of scored samples.
--
-- Each sample is a `{score, label}` pair. A label is positive if it is greater
-- than zero, so both the {-1, +1} convention the hinge loss uses and the
-- {0, 1} one work, and they give the same answer.
--
-- @param samples array of {score, label}
-- @return the AUC in [0, 1], or nil if either class is missing (the statistic
--   is a per-pair average and there are no pairs to average)
-- @function auc
local function auc(samples)
    local n = #samples
    if n == 0 then
        return nil
    end

    local order = {}
    for i = 1, n do
        local sample = samples[i]
        if type(sample) ~= 'table' or type(sample[1]) ~= 'number' then
            utils.error('sample %d is not a {score, label} pair', i)
        end
        order[i] = sample
    end
    -- Ascending by score, so rank 1 is the lowest-scoring sample. table.sort is
    -- not stable, which is exactly why ties have to be averaged rather than
    -- left to the order they come out in.
    table.sort(order, function(left, right)
        return left[1] < right[1]
    end)

    local positives, rank_sum = 0, 0.0
    local i = 1
    while i <= n do
        -- [i, last] is the run of samples sharing this score.
        local last = i
        while last < n and order[last + 1][1] == order[i][1] do
            last = last + 1
        end
        local shared = (i + last) / 2.0
        for k = i, last do
            if order[k][2] > 0 then
                positives = positives + 1
                rank_sum = rank_sum + shared
            end
        end
        i = last + 1
    end

    local negatives = n - positives
    if positives == 0 or negatives == 0 then
        return nil
    end
    return (rank_sum - positives * (positives + 1) / 2.0) /
           (positives * negatives)
end

local collector_mt

--- Add one scored sample.
--
-- @param score the model's output
-- @param label positive for the positive class
-- @function add
local function add(self, score, label)
    self.n = self.n + 1
    self.samples[self.n] = {score, label}
    return self
end

--- How many samples have been added.
--
-- @return number
-- @function count
local function count(self)
    return self.n
end

--- The AUC of everything added so far.
--
-- @return the AUC, or nil if either class is missing
-- @function result
local function result(self)
    return auc(self.samples)
end

collector_mt = {
    __index = {
        add    = add,
        count  = count,
        result = result,
    },
}

--- A collector that accumulates samples and computes the AUC on demand.
--
-- The state is `{n = number, samples = array of {score, label}}` and nothing
-- else, so it can be handed to another vertex as a message or parked in a
-- space; `wrap` puts the methods back on it afterwards. This keeps every
-- sample, because the rank statistic needs the whole ordering -- it is not a
-- constant-space estimator.
--
-- @return a collector
-- @function new
local function new()
    return setmetatable({n = 0, samples = {}}, collector_mt)
end

--- Re-attach the collector methods to a state that came back from a space.
--
-- @param state a table of the shape `new()` produces
-- @return the same table, with methods
-- @function wrap
local function wrap(state)
    state.samples = state.samples or {}
    state.n = state.n or #state.samples
    return setmetatable(state, collector_mt)
end

return strict.strictify({
    auc  = auc,
    new  = new,
    wrap = wrap,
})
