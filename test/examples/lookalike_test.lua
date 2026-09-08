--- examples/lookalike, driven through a Tarantool 3 cluster config.
--
-- What is asserted is that the job *learned something*, not that it produced
-- particular numbers: test/fixtures/lookalike was drawn from a hidden linear
-- model whose weights are in truth.json, so there are two independent ways to
-- ask whether the distributed SGD found it.
--
--   * the AUC of each task's model on the split it was not trained on, which
--     the job measures and reports itself
--   * the sign agreement between the learned weight vector and the hidden one,
--     which the job does not know about at all -- so a model that overfitted
--     the held-out split into a good AUC would still fail here
--
-- Both thresholds are far below what the fixture actually produces (see the
-- comments on each), because the point is to catch a broken pipeline rather
-- than to pin a number that a change to the learning rate would move.

local fio     = require('fio')
local json    = require('json')
local t       = require('luatest')

local Cluster = require('luatest.cluster')

local helper = require('test.examples.cluster')

local g = t.group('examples.lookalike')

local EXAMPLE = 'lookalike'
local JOB     = 'lookalike'

local FIXTURE = 'test/fixtures/lookalike'
local AVRO = {
    users  = '../../' .. FIXTURE .. '/users.avro',
    labels = '../../' .. FIXTURE .. '/labels.avro',
}

local TASKS = {'task1', 'task2'}
local USERS = 200

-- The fixture's own Bayes error is 4 flipped labels of 60 for task1 and 7 of
-- 60 for task2, so a perfect model does not reach AUC 1 on either. Measured on
-- this fixture: task1 0.944, task2 0.841. task2 is the harder of the two and
-- the reason the threshold is not tighter: 15 of its 60 labels are positive,
-- so a quarter held out leaves an AUC measured over 4 positives against 11
-- negatives -- 44 pairs, one of which is worth 0.023 of the score.
local MIN_AUC = 0.75
-- Nine weights (a bias and eight features), so 0.8 is "at least eight of the
-- nine". Measured: 9 of 9 for both tasks.
local MIN_AGREEMENT = 0.8

-- Seven phases plus the superstep the master spends starting the tasks; see
-- the timeline in examples/lookalike/app.lua.
local SUPERSTEPS = 8

local function truth()
    local path = fio.pathjoin(helper.ROOT, FIXTURE, 'truth.json')
    local file = assert(io.open(path, 'r'), 'cannot open ' .. path)
    local text = file:read('*a')
    file:close()
    return json.decode(text)
end

local function app_cfg(extra)
    -- No grant_to: this example keeps spaces of its own and grants them to the
    -- user the job runs as, which the roles hand to worker_context as the job
    -- context rather than app_cfg having to repeat it. See ensure_space() in
    -- the app module.
    local cfg = {users = AVRO.users, labels = AVRO.labels}
    for key, value in pairs(extra or {}) do
        cfg[key] = value
    end
    return cfg
end

local function run(extra)
    return helper.run(Cluster, {name = EXAMPLE, app_cfg = app_cfg(extra)})
end

--- The `model` aggregator as the master merged it in the last superstep.
--
-- Which is where the reports live: an aggregator holds one superstep's
-- contributions, and the tasks contribute theirs again on the superstep they
-- halt in precisely so that this is readable afterwards.
local function model_of(cluster)
    return cluster[helper.MASTER_NAME]:exec(function(role)
        return require(role).get().aggregators['model']()
    end, {helper.MASTER_ROLE})
end

--- How many of `a` and `b` agree in sign, as a fraction.
local function sign_agreement(a, b)
    t.assert_equals(#a, #b, 'weight vectors of different length')
    local agree = 0
    for i = 1, #a do
        if (a[i] > 0 and b[i] > 0) or (a[i] < 0 and b[i] < 0) or
           (a[i] == 0 and b[i] == 0) then
            agree = agree + 1
        end
    end
    return agree / #a
end

local function vertices_of(cluster)
    return helper.collect_vertices(cluster, {job = JOB})
end

--- Wait for `pattern` to appear in a worker's log.
--
-- An error raised while the role applies its config at startup exits the
-- process, and all luatest sees of that is a process that went away -- which
-- it can see before the dying instance's log has reached the file. Grepping
-- once is therefore a race, and it is one that loses: with two refusal cases
-- in this file it failed about one run in two, on whichever of them ran.
-- Any worker, not worker1, and with retries: cluster:start() gives up as soon
-- as the first instance dies and luatest then kills the rest, so a worker can
-- be gone before its role has applied and logged anything, and the one that
-- did log may not have flushed yet. Demanding it of worker1 at once failed
-- about one run in three.
local function wait_for_log(cluster, pattern, what)
    t.helpers.retrying({timeout = 10, delay = 0.1}, function()
        local said = false
        for i = 1, helper.WORKER_COUNT do
            if cluster[helper.worker_name(i)]:grep_log(pattern) then
                said = true
            end
        end
        t.assert(said, what)
    end)
end

-------------------------------------------------------------------------------

g.test_every_task_learns_the_hidden_model = function()
    local cluster, status = run()
    t.assert_equals(status.error, nil)
    t.assert_equals(status.state, 'done')
    t.assert_equals(status.superstep, SUPERSTEPS)

    local model = model_of(cluster)
    local hidden = truth().tasks

    for _, task in ipairs(TASKS) do
        local entry = model[task]
        t.assert_not_equals(entry, nil, task .. ' has no model')
        t.assert_equals(entry.state, 'ready', task .. ' did not finish')

        -- The job's own verdict, on the rows it never trained on.
        t.assert_not_equals(entry.report.auc, nil, task .. ' reported no AUC')
        t.assert_ge(entry.report.auc, MIN_AUC, task .. ' AUC')

        -- And an independent one it could not have optimised for.
        local agreement = sign_agreement(entry.weights, hidden[task].weights)
        t.assert_ge(agreement, MIN_AGREEMENT, task .. ' sign agreement')
    end
end

g.test_the_reports_carry_the_sizes = function()
    local cluster = run()
    local model = model_of(cluster)

    for _, task in ipairs(TASKS) do
        local report = model[task].report
        -- The fixture has 60 labels per task and every one of them names a
        -- user that is in users.avro, so nothing is lost between the FETCH and
        -- the answer.
        t.assert_equals(report.labelled, 60, task .. ': labelled')
        t.assert_equals(report.answered, 60, task .. ': answered')
        -- A quarter of each class held out, rounded: the two tasks are
        -- unbalanced in opposite directions, so the split sizes differ.
        t.assert_equals(report.train_size + report.test_size, 60,
                        task .. ': the split covers every answer')
        t.assert_ge(report.test_size, 14, task .. ': test size')
        t.assert_le(report.test_size, 16, task .. ': test size')
        t.assert_equals(report.scored, report.test_size,
                        task .. ': the AUC was measured on the held-out split')

        t.assert_equals(report.features, 8, task .. ': features')
        t.assert_gt(report.iterations, 0, task .. ': iterations')
        t.assert_le(report.iterations, 300, task .. ': iterations')
        t.assert_not_equals(report.loss, nil, task .. ': loss')

        -- The calibration sample is every labelled user plus a top-up drawn
        -- from the users this task's own worker holds, and it stops at
        -- calibration_sample. The top-up runs out first here -- 200 users over
        -- three workers leaves a shard with fewer than 40 that are not already
        -- labelled -- so the number is between the two and not a round 100.
        -- Asserted as a range because moving a vertex between workers must not
        -- fail this; that the top-up happened at all is the `> 60`.
        t.assert_gt(report.calibration_sent, 60,
                    task .. ': the local top-up contributed nothing')
        t.assert_le(report.calibration_sent, 100,
                    task .. ': more than calibration_sample was asked')
        -- Every user asked answered: none of the names came from anywhere but
        -- this job's own vertices.
        t.assert_equals(report.calibration_size, report.calibration_sent,
                        task .. ': calibration answers')
    end
end

g.test_every_user_holds_a_score_and_a_percentile_per_task = function()
    local cluster = run()
    local found = vertices_of(cluster)

    local model = model_of(cluster)
    local users = 0
    for name, vertex in pairs(found) do
        if name:sub(1, 2) == 'u:' then
            users = users + 1
            t.assert_equals(vertex.halted, true, name .. ' did not halt')
            for _, task in ipairs(TASKS) do
                local score = vertex.value.scores[task]
                t.assert_not_equals(score, nil,
                                    name .. ' has no score for ' .. task)
                t.assert_equals(type(score.score), 'number', name .. ' score')
                -- Buckets of 5 percent, so the rank is one of 0, 5, ... 95.
                t.assert_ge(score.percentile, 0, name .. ' percentile')
                t.assert_le(score.percentile, 95, name .. ' percentile')
                t.assert_equals(score.percentile % 5, 0,
                                name .. ' percentile is not on a bucket edge')
                -- The score is the model applied to this user's own features,
                -- which is checkable from outside the job.
                local weights = model[task].weights
                local expected = weights[1]
                for i, x in ipairs(vertex.value.features) do
                    expected = expected + x * weights[i + 1]
                end
                t.assert_almost_equals(score.score, expected, 1e-9,
                                       name .. ' score of ' .. task)
            end
        end
    end
    t.assert_equals(users, USERS, 'not every user came back')

    -- A run that put the whole population on one worker would satisfy most of
    -- the above and prove nothing about the sharding.
    helper.assert_spread(found)
end

g.test_the_percentile_ranks_the_population = function()
    local cluster = run()
    local found = vertices_of(cluster)

    for _, task in ipairs(TASKS) do
        local rows = {}
        for name, vertex in pairs(found) do
            if name:sub(1, 2) == 'u:' then
                table.insert(rows, vertex.value.scores[task])
            end
        end
        table.sort(rows, function(a, b) return a.score < b.score end)
        -- The rank is a monotone function of the score: a user scoring higher
        -- than another can never be placed in a lower bucket. That is the one
        -- property the 2016 predictCalibrated did not have, because it added a
        -- random offset inside the bucket.
        for i = 2, #rows do
            t.assert_ge(rows[i].percentile, rows[i - 1].percentile,
                        task .. ': the percentile is not monotone in the score')
        end
        -- And it discriminates: a calibration sample of 100 over 19 cut points
        -- cannot put the whole population in one bucket.
        t.assert_gt(rows[#rows].percentile, rows[1].percentile,
                    task .. ': every user landed in the same bucket')
    end
end

g.test_the_master_vertex_collects_every_report = function()
    local cluster = run()
    local found = vertices_of(cluster)

    local master = found['master']
    t.assert_not_equals(master, nil, 'the master vertex is missing')
    t.assert_equals(master.halted, true, 'the master vertex did not halt')
    t.assert_equals(master.value.roster, TASKS)
    for _, task in ipairs(TASKS) do
        t.assert_not_equals(master.value.reports[task], nil,
                            'the master vertex has no report for ' .. task)
        t.assert_equals(master.value.reports[task].state, 'ready')
    end
end

-------------------------------------------------------------------------------
-- The negative case
-------------------------------------------------------------------------------

--- A labels file with one task the job cannot train.
--
-- Written rather than committed: test/fixtures/lookalike is the generator's
-- output and adding a hand-made task to it would make it something else. The
-- schema is the generator's own.
local function labels_with_a_starved_task(dir)
    local ocf = require('pregel.avro.ocf')
    local path = fio.pathjoin(dir, 'labels.avro')

    local source = ocf.open(fio.pathjoin(helper.ROOT, FIXTURE, 'labels.avro'),
                            {mode = 'r'})
    local records = {}
    for record in source:records() do
        table.insert(records, record)
    end
    source:close()

    local writer = ocf.open(path, {
        mode   = 'w',
        schema = {
            type = 'record', name = 'Label',
            fields = {
                {name = 'task',   type = 'string'},
                {name = 'vid',    type = 'string'},
                {name = 'target', type = 'int'   },
            },
        },
    })
    for _, record in ipairs(records) do
        writer:append(record)
    end
    -- Five labels, against a min_labels of 20.
    for i = 1, 5 do
        writer:append{task = 'starved', vid = 'u' .. i,
                      target = i % 2 == 0 and 1 or -1}
    end
    writer:close()
    return path
end

--- A labels file with one task whose labels are all the same class.
--
-- Thirty of them, so `min_labels` is cleared and the task reaches training on
-- its own merits: what it cannot do is be *scored*.
local function labels_with_a_one_class_task(dir)
    local ocf = require('pregel.avro.ocf')
    local path = fio.pathjoin(dir, 'labels.avro')

    local source = ocf.open(fio.pathjoin(helper.ROOT, FIXTURE, 'labels.avro'),
                            {mode = 'r'})
    local records = {}
    for record in source:records() do
        table.insert(records, record)
    end
    source:close()

    local writer = ocf.open(path, {
        mode   = 'w',
        schema = {
            type = 'record', name = 'Label',
            fields = {
                {name = 'task',   type = 'string'},
                {name = 'vid',    type = 'string'},
                {name = 'target', type = 'int'   },
            },
        },
    })
    for _, record in ipairs(records) do
        writer:append(record)
    end
    for i = 1, 30 do
        writer:append{task = 'onesided', vid = 'u' .. i, target = 1}
    end
    writer:close()
    return path
end

g.test_a_one_class_task_fails_instead_of_publishing_a_model_with_no_auc =
function()
    local dir = fio.tempdir()
    t.assert_not_equals(dir, nil)
    local labels = labels_with_a_one_class_task(dir)

    local cluster, status = helper.run(Cluster, {
        name    = EXAMPLE,
        app_cfg = app_cfg({labels = labels}),
    })
    t.assert_equals(status.error, nil)
    t.assert_equals(status.state, 'done')

    local model = model_of(cluster)
    local entry = model['onesided']
    t.assert_not_equals(entry, nil, 'the one-class task published nothing')

    -- It used to publish `state = 'ready'` with a report that had no `auc` key
    -- at all: measure_auc returns nil for a one-class test split and msgpack
    -- drops the nil on the way out, so the only trace of "this model was never
    -- scored" was a missing key nobody looked for. Every user was then ranked
    -- against weights that had learnt the bias and nothing else.
    t.assert_equals(entry.state, 'failed', 'onesided state')
    t.assert_equals(entry.report.state, 'failed', 'onesided report state')
    t.assert_str_contains(entry.report.reason, 'one class',
                          'onesided reason')
    t.assert_equals(entry.weights, nil,
                    'a task that cannot be scored published weights')

    -- The guard that would have caught this from the other side: no task may
    -- reach `ready` without an AUC. It holds for the fixture's two as well.
    for task, published in pairs(model) do
        if published.state == 'ready' then
            t.assert_not_equals(published.report.auc, nil,
                                task .. ' is ready with no AUC')
        end
    end

    for _, task in ipairs(TASKS) do
        t.assert_equals(model[task].state, 'ready', task .. ' after a failure')
        t.assert_ge(model[task].report.auc, MIN_AUC, task .. ' AUC')
    end

    for name, vertex in pairs(vertices_of(cluster)) do
        if name:sub(1, 2) == 'u:' then
            t.assert_equals(vertex.halted, true, name .. ' did not halt')
            t.assert_equals(vertex.value.scores['onesided'], nil,
                            name .. ' scored a task that was never validated')
        end
    end

    fio.rmtree(dir)
end

g.test_a_task_with_too_few_labels_fails_without_taking_the_job_down = function()
    local dir = fio.tempdir()
    t.assert_not_equals(dir, nil)
    local labels = labels_with_a_starved_task(dir)

    local cluster, status = helper.run(Cluster, {
        name    = EXAMPLE,
        app_cfg = app_cfg({labels = labels}),
    })

    -- The job finished; it did not crash, and it did not hang waiting for a
    -- task that will never publish a model.
    t.assert_equals(status.error, nil)
    t.assert_equals(status.state, 'done')

    local model = model_of(cluster)
    t.assert_equals(model['starved'].state, 'failed')
    t.assert_equals(model['starved'].report.state, 'failed')
    t.assert_str_contains(model['starved'].report.reason,
                          '5 label(s), fewer than the 20')
    t.assert_equals(model['starved'].weights, nil,
                    'a failed task published weights')

    -- The other two tasks are unaffected: a starved neighbour is not a reason
    -- for them to stop.
    for _, task in ipairs(TASKS) do
        t.assert_equals(model[task].state, 'ready', task .. ' after a failure')
        t.assert_ge(model[task].report.auc, MIN_AUC, task .. ' AUC')
    end

    -- And every user still halted, having scored the two tasks it could and
    -- refused to wait for the one it could not.
    local found = vertices_of(cluster)
    for name, vertex in pairs(found) do
        if name:sub(1, 2) == 'u:' then
            t.assert_equals(vertex.halted, true, name .. ' did not halt')
            t.assert_equals(vertex.value.scores['starved'], nil,
                            name .. ' scored a failed task')
        end
    end

    fio.rmtree(dir)
end

g.test_a_task_left_without_training_rows_fails_with_a_reason = function()
    -- Legal but extreme: 0.99 of each class, rounded, is every row of it, so
    -- the stratified split hands the whole labelled set to test and leaves
    -- training nothing. Before this was caught, `#order` was zero, the batch
    -- draw indexed the staging space with `order[0 % 0]` -- a nil -- and the
    -- job died on `Invalid key part count in an exact match (expected 2,
    -- got 1)` four supersteps in, taking every other task with it.
    local cluster, status = run({test_fraction = 0.99})

    t.assert_equals(status.error, nil)
    t.assert_equals(status.state, 'done')

    local model = model_of(cluster)
    for _, task in ipairs(TASKS) do
        t.assert_equals(model[task].state, 'failed', task .. ' state')
        t.assert_equals(model[task].report.state, 'failed',
                        task .. ' report state')
        t.assert_str_contains(model[task].report.reason, 'no training rows',
                              task .. ' reason')
        t.assert_equals(model[task].weights, nil,
                        task .. ' published weights without training')
        t.assert_equals(model[task].report.auc, nil,
                        task .. ' reported an AUC without training')
    end

    -- And the users stopped waiting for a model that is never coming, exactly
    -- as they do for a task that starved.
    for name, vertex in pairs(vertices_of(cluster)) do
        if name:sub(1, 2) == 'u:' then
            t.assert_equals(vertex.halted, true, name .. ' did not halt')
        end
    end
end

g.test_a_test_fraction_of_one_is_refused_at_startup = function()
    -- The whole job, rather than one task: a `test_fraction` at or above 1
    -- leaves *every* task without training rows, and that is an operator
    -- typo rather than a property of the data. Refusing it while the role
    -- applies its config says so where the operator is looking.
    local cluster = Cluster:new(helper.config({
        name    = EXAMPLE,
        app_cfg = app_cfg({test_fraction = 1.0}),
    }), helper.server_opts)
    local ok, err = pcall(function() cluster:start() end)
    t.assert_equals(ok, false, 'the cluster started on test_fraction 1.0')
    t.assert_str_contains(tostring(err), 'Process is terminated')
    wait_for_log(cluster, 'test_fraction',
                 'the worker did not say why it refused the config')
end

g.test_a_labels_file_with_no_task_is_refused_at_startup = function()
    local dir = fio.tempdir()
    local path = fio.pathjoin(dir, 'labels.avro')
    local writer = require('pregel.avro.ocf').open(path, {
        mode   = 'w',
        schema = {
            type = 'record', name = 'Label',
            fields = {
                {name = 'task',   type = 'string'},
                {name = 'vid',    type = 'string'},
                {name = 'target', type = 'int'   },
            },
        },
    })
    writer:close()

    -- Not a job that runs and produces nothing: a user vertex halts once every
    -- task it can see is terminal, and with no tasks it would wait for the
    -- first one forever. The worker role has to refuse the config instead.
    local cluster = Cluster:new(helper.config({
        name    = EXAMPLE,
        app_cfg = app_cfg({labels = path}),
    }), helper.server_opts)
    local ok, err = pcall(function() cluster:start() end)
    t.assert_equals(ok, false, 'the cluster started on an empty labels file')
    -- All luatest sees is a process that went away; the reason is in that
    -- instance's own log, because an error raised while the config is being
    -- applied at startup exits the process.
    t.assert_str_contains(tostring(err), 'Process is terminated')
    wait_for_log(cluster, 'names no task',
                 'no worker said why it refused the config')

    fio.rmtree(dir)
end

-------------------------------------------------------------------------------
-- The mutation this suite is built to catch
-------------------------------------------------------------------------------

g.test_a_model_that_never_trained_fails_the_auc_assertion = function()
    -- max_iter 0 leaves the weights at the zero vector they start from, so
    -- every user scores exactly 0, every pair is a tie, and the AUC is 0.5.
    -- This is the shape of "the gradient step is broken" -- and it is here so
    -- that the threshold in the tests above is known to be reachable from
    -- below, rather than being a number every run happens to clear.
    local cluster = run({max_iter = 0})
    local model = model_of(cluster)

    for _, task in ipairs(TASKS) do
        local report = model[task].report
        t.assert_equals(report.iterations, 0, task .. ': iterations')
        t.assert_almost_equals(report.auc, 0.5, 1e-9, task .. ': AUC of noise')
        t.assert_lt(report.auc, MIN_AUC,
                    task .. ': an untrained model cleared the AUC threshold')
    end
end
