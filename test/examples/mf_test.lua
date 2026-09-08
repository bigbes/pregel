--- examples/mf, driven through a Tarantool 3 cluster config.
--
-- Unlike pagerank there is no closed form to check against: SGD is what it is,
-- and a re-implementation of it here would be the same approximation making
-- the same mistakes. What is checked instead is what the algorithm is *for*:
--
--   * the model beats predicting the global mean on ratings it never saw, by
--     enough of a margin that a broken step cannot fake it;
--   * the training error falls, epoch after epoch;
--   * every training rating is used exactly once per epoch;
--   * the schedule is the documented one -- one superstep to seed the vectors
--     and one per epoch after it.
--
-- The held-out score is read the same way the README reads it: by running
-- examples/mf/evaluate.lua on the master, which walks the workers' spaces over
-- the master's own net.box connections. Reading the vertices out of luatest
-- instead would test the arithmetic and skip the half of the example that
-- decides whether an operator can get an answer out of a finished job.

local fio = require('fio')
local t   = require('luatest')

local Cluster = require('luatest.cluster')

local ocf    = require('pregel.avro.ocf')
local mf     = require('pregel.math.mf')
local helper = require('test.examples.cluster')

local g = t.group('examples.mf')

local EXAMPLE = 'mf'
local JOB     = 'mf'

local RANK   = 3
local EPOCHS = 150

-- test/fixtures/ratings: 50 users, 30 items, 378 train and 95 test ratings.
local TRAIN_RATINGS = 378
local TEST_RATINGS  = 95
local USERS         = 50
local ITEMS         = 30

-- Relative, the way the committed config.yaml spells them; examples/common.lua
-- resolves both against examples/mf/.
local TRAIN = '../../test/fixtures/ratings/train.avro'
local TEST  = '../../test/fixtures/ratings/test.avro'

--- What these hyperparameters produce on this fixture, measured.
--
-- Not a fixed number, and this is the one place worth saying why. The starting
-- vectors are drawn from the vertex names, so *they* are the same on every run
-- and on every shard count -- but the order a vertex reads its messages in is
-- the order they arrived, and a vertex chains its own updates as it walks
-- them, so a different interleaving is a slightly different model. Three runs
-- of this cluster gave 0.49109, 0.49077 and 0.49076.
--
-- 0.52 is chosen against what the fixture supports rather than against that
-- spread, which is only 0.0003 wide. The floor is 0.286 -- the hidden factors
-- in truth.json, scored on the held-out half after the generator's clip and
-- rounding -- and the best a rank-3 model gets on this split is about 0.49,
-- from a sequential numpy SGD (0.488) and from a sweep of this app's own
-- schedule outside the cluster (0.4906). So the example runs at its ceiling,
-- 0.52 is a real regression bound rather than a number every run happens to
-- clear, and it is far below the 0.6801 a model that learnt nothing scores --
-- see baseline_rmse. Before the epochs went from 30 to 150 the same example
-- scored 0.587, which this threshold refuses.
local TEST_RMSE_MAX = 0.52

local function app_cfg(overrides)
    local cfg = {
        train  = TRAIN,
        test   = TEST,
        rank   = RANK,
        epochs = EPOCHS,
        lr     = 0.05,
        decay  = 0.99,
        lambda = 0.05,
    }
    for key, value in pairs(overrides or {}) do
        cfg[key] = value
    end
    return cfg
end

local function run(overrides)
    return helper.run(Cluster, {
        name    = EXAMPLE,
        app_cfg = app_cfg(overrides),
    })
end

--- evaluate.lua, run on the master exactly as the README runs it.
local function evaluate(cluster)
    return cluster[helper.MASTER_NAME]:exec(function(path)
        return require('examples.mf.evaluate').evaluate{test = path}
    end, {TEST})
end

--- The per-epoch train RMSE the master's aggregator merge recorded.
local function train_history(cluster)
    return cluster[helper.MASTER_NAME]:exec(function()
        return require('examples.mf.app').train_history()
    end)
end

--- RMSE of answering every held-out rating with the training mean.
--
-- The floor any recommender has to beat: it uses no user, no item and no
-- factor, so a model that has learnt nothing lands on it.
local function baseline_rmse(mu)
    local samples = {}
    for _, record in ipairs(ocf.read_all(fio.pathjoin(
            helper.ROOT, 'test', 'fixtures', 'ratings', 'test.avro'))) do
        table.insert(samples, {mu, record.rating})
    end
    return mf.rmse(samples)
end

g.test_the_model_generalises_to_held_out_ratings = function()
    local cluster, status = run()
    t.assert_equals(status.error, nil)
    -- One superstep to seed the latent vectors, then one per epoch.
    t.assert_equals(status.superstep, EPOCHS + 1)

    local report = evaluate(cluster)

    -- Every user and item of test.avro is in train.avro -- the fixture's split
    -- guarantees it -- so nothing may be skipped. A `missing` above zero would
    -- otherwise make the score look better by scoring less.
    t.assert_equals(report.missing, 0)
    t.assert_equals(report.ratings, TEST_RATINGS)
    t.assert_equals(report.users, USERS)
    t.assert_equals(report.items, ITEMS)

    t.assert_lt(report.rmse, TEST_RMSE_MAX,
                'held-out RMSE: ' .. tostring(report.rmse))

    -- Beating the mean is the part that cannot be faked by a model that sits
    -- still: mu is what every vertex predicts before it has learnt anything.
    local baseline = baseline_rmse(report.mu)
    t.assert_lt(report.rmse, baseline * 0.95,
                string.format('held-out RMSE %.6f against a mean-only ' ..
                              'baseline of %.6f', report.rmse, baseline))
end

g.test_the_training_error_falls_every_epoch = function()
    local cluster = run()

    local history = train_history(cluster)
    t.assert_equals(#history, EPOCHS)

    -- The brief asks for the first ten; measured, all thirty fall, so that is
    -- what is asserted. A non-monotone tail would be a real finding here and
    -- not a tolerance to widen.
    for i = 2, #history do
        t.assert_lt(history[i].rmse, history[i - 1].rmse,
                    string.format('epoch %d (%.6f) is not below epoch %d ' ..
                                  '(%.6f)', history[i].epoch, history[i].rmse,
                                  history[i - 1].epoch, history[i - 1].rmse))
    end

    -- Every training rating goes into the error of every epoch exactly once.
    -- Both directions of each rating are in the graph, so a version that
    -- accumulated on the item side too would report 756 here and a train RMSE
    -- that is quietly the same number.
    for _, epoch in ipairs(history) do
        t.assert_equals(epoch.count, TRAIN_RATINGS,
                        'epoch ' .. epoch.epoch .. ' counted ' ..
                        epoch.count .. ' ratings')
        t.assert_equals(epoch.superstep, epoch.epoch + 1)
    end

    -- The same count the vertices contribute for mu, from the other side of
    -- the graph: the edges rather than the messages.
    local counted = cluster[helper.MASTER_NAME]:exec(function(role)
        local master = require(role).get()
        return {
            count = master.aggregators['rating_count'](),
            sum   = master.aggregators['rating_sum'](),
        }
    end, {helper.MASTER_ROLE})
    t.assert_equals(counted.count, TRAIN_RATINGS)
end

g.test_every_vertex_carries_a_latent_vector_of_the_configured_rank = function()
    local cluster = run{epochs = 2}

    local found = helper.collect_vertices(cluster, {job = JOB})
    helper.assert_spread(found)

    local users, items, edges = 0, 0, 0
    for name, vertex in pairs(found) do
        local value = vertex.value
        t.assert_equals(value.name, name)
        t.assert_equals(#value.p, RANK, name .. ': latent vector')
        t.assert_type(value.b, 'number', name .. ': bias')
        -- The prefix and the kind are the same fact spelled twice, and the
        -- loader is what has to keep them in step.
        if value.kind == 'user' then
            users = users + 1
            t.assert_equals(name:sub(1, 2), 'u:')
        else
            items = items + 1
            t.assert_equals(name:sub(1, 2), 'i:')
        end
        edges = edges + #vertex.edges
    end

    t.assert_equals(users, USERS)
    t.assert_equals(items, ITEMS)
    -- Every rating is stored as an edge in both directions; without the
    -- reverse one the item vertices would never learn.
    t.assert_equals(edges, 2 * TRAIN_RATINGS)
end

--- Feed one run's worth of epochs through the master's own merge hook.
--
-- `train_sse`'s merge is the only code that runs on the master per superstep,
-- and it is where the history is built -- so driving it directly is running a
-- job's worth of master-side bookkeeping without a cluster, and two of them in
-- one process is the case this is about. `default` is what a worker holding no
-- user vertex contributes, superstep 0 and all.
local function drive_epochs(merge, first_superstep, count, workers)
    local default = {sse = 0.0, n = 0, superstep = 0}
    for s = first_superstep, first_superstep + count - 1 do
        for w = 1, workers do
            merge(default, {sse = 1.0 * w, n = 2, superstep = s})
        end
    end
end

g.test_a_second_job_does_not_report_the_first_one_s_epochs = function()
    -- `history` is a module-level upvalue on the master, and nothing used to
    -- clear it: a 30-epoch run followed by a 10-epoch one on the same instance
    -- answered with 30 entries, of which epochs 11..30 belonged to the run
    -- before and were indistinguishable from the current one's.
    local app = require('examples.mf.app')
    local merge = app.aggregators.train_sse.merge
    t.assert_type(merge, 'function', 'train_sse has no merge')

    -- Epoch k is superstep k + 1; the seeding superstep contributes the
    -- default and is not recorded.
    drive_epochs(merge, 2, 30, helper.WORKER_COUNT)
    local first = app.train_history()
    t.assert_equals(#first, 30, 'the first run did not record 30 epochs')
    t.assert_equals(first[#first].epoch, 30)

    drive_epochs(merge, 2, 10, helper.WORKER_COUNT)
    local second = app.train_history()
    t.assert_equals(#second, 10,
                    'the second run reported ' .. #second ..
                    ' epochs, so it is still carrying the first run\'s')
    t.assert_equals(second[#second].epoch, 10)
    for i, entry in ipairs(second) do
        t.assert_equals(entry.epoch, i)
        t.assert_equals(entry.superstep, i + 1)
    end
end

g.test_the_rank_comes_from_app_cfg = function()
    -- Two epochs, because what is under test is the shape of the vectors and
    -- not what is in them -- and a rank the app_cfg did not ask for would be
    -- invisible in every assertion above, all of which use the default 3.
    local cluster, status = run{rank = 5, epochs = 2}
    t.assert_equals(status.superstep, 3)

    for _, vertex in pairs(helper.collect_vertices(cluster, {job = JOB})) do
        t.assert_equals(#vertex.value.p, 5)
    end
end
