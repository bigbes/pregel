--- lookalike: a distributed look-alike recommender.
--
-- Several independent binary classifiers are trained at once over one shared
-- population of users, and every user is then scored -- and ranked -- against
-- every model. It is a restoration of the 2016 `test-avro/` job (node_master,
-- node_task, node_data), on the ported core and with the arithmetic moved into
-- pregel.math; see README.md for what changed and why.
--
-- Three kinds of vertex, told apart by `value.type`:
--
--   master        one per job. Starts the tasks and collects their reports.
--   t:<task>      one per task. Owns that task's labels, its model, and the
--                 whole training pipeline for it.
--   u:<vid>       one per user. Owns a feature vector, answers questions
--                 about it, and ends up holding a score per task.
--
-- There are no edges anywhere: nothing here is a graph algorithm. What pregel
-- provides is the partitioning (a vertex name hashes onto a worker), the
-- superstep barrier, and the two ways a vertex reaches something that is not
-- its neighbour -- a message addressed by name, and an aggregator.
--
-- The phases of one task, one superstep each:
--
--   1 SELECTION    the master sends START; the task sends FETCH to every
--                  vertex it holds a label for, the label riding along
--   2 (answers)    each of those users answers with its feature vector
--   3 TRAINING     the task writes the answers into a space of its own,
--                  splits them, trains, measures held-out AUC, and sends
--                  PREDICT_CALIBRATION with the weights to a sample of users
--   4 (answers)    each of those users answers with its raw score
--   5 CALIBRATION  the task turns the sample into percentile cut points and
--                  publishes {weights, cuts} through the `model` aggregator
--   6 PREDICTION   every user reads `model`, scores itself against every task
--                  and halts
--   7 DONE         the tasks and the master see that no user is left waiting,
--                  publish their reports one last time, and halt
--
-- Configured through roles_cfg.app_cfg:
--
--   users, labels       the two Avro files, relative to this directory
--   grant_to            the user roles_cfg.user names, so the job can reach
--                       the per-task staging spaces this module creates
--   test_fraction       held out of training, per class          (0.25)
--   min_labels          fewer than this and the task fails       (20)
--   learning_rate       initial rate of the inverse-decay schedule (0.1)
--   learning_decay      k in rate = c / (1 + k * iteration)      (0.01)
--   lambda              L2 penalty on the weights                (0.001)
--   batch_size          samples per gradient step                (16)
--   max_iter            gradient steps at most                   (300)
--   alpha               loss averaging factor                    (0.2)
--   epsilon             convergence threshold on the averaged loss (1e-5).
--                       Below gd's own 1e-4 default on purpose: at batch_size
--                       16 the batch loss is noisy enough that two iterations
--                       land within 1e-4 of each other by luck, which stopped
--                       one task of the README's run after 11 iterations at
--                       AUC 0.920 against 0.978 trained out.
--   calibration_sample  users asked for a score during calibration (100)
--   calibration_bucket  percent per calibration bucket           (5)

local digest = require('digest')

local gd         = require('pregel.math.gd')
local auc        = require('pregel.math.auc')
local percentile = require('pregel.math.percentile')
local vector     = require('pregel.math.vector')

local loader = require('pregel.loader')
local ocf    = require('pregel.avro.ocf')
local common = require('examples.common')

local HERE = common.here()

local MASTER_NAME = 'master'

--- What a message is asking for. The 2016 code spelled these as ffi enums in
-- constants.lua; they travel through msgpack here, and a string that says what
-- it means costs four bytes more than an integer that does not.
local START             = 'start'
local FETCH             = 'fetch'
local FEATURES          = 'features'
local PREDICT_CALIBRATE = 'predict_calibration'
local SCORE             = 'score'

-- A task's phase, kept in the task vertex's own value rather than in a map on
-- the worker context: one worker may own several task vertices, and a phase
-- belongs to a task and not to the worker that happens to hold it.
local PHASE_NEW         = 'new'
local PHASE_TRAINING    = 'training'
local PHASE_CALIBRATION = 'calibration'
local PHASE_DONE        = 'done'
local PHASE_FAILED      = 'failed'

local SPLIT_TRAIN = 'train'
local SPLIT_TEST  = 'test'

local DEFAULTS = {
    test_fraction      = 0.25,
    min_labels         = 20,
    learning_rate      = 0.1,
    learning_decay     = 0.01,
    lambda             = 0.001,
    batch_size         = 16,
    max_iter           = 300,
    alpha              = 0.2,
    epsilon            = 1e-5,
    calibration_sample = 100,
    calibration_bucket = 5,
}

local app = {}

-------------------------------------------------------------------------------
-- Names, ordering, small arithmetic
-------------------------------------------------------------------------------

local function data_name(vid)
    return 'u:' .. vid
end

local function task_name(task)
    return 't:' .. task
end

function app.obtain_name(vertex)
    return vertex.name
end

--- A stable pseudo-random order over strings.
--
-- Everything this example shuffles -- the train/test split, the batch order,
-- the calibration draw -- goes through this rather than math.random, so a run
-- produces the same model twice and a test can assert a number instead of a
-- range. `salt` keeps the three orders independent of one another.
local function shuffle_key(salt, key)
    return digest.crc32(salt .. ':' .. key)
end

local function sort_by_key(rows, salt, key_of)
    table.sort(rows, function(left, right)
        local a, b = shuffle_key(salt, key_of(left)), shuffle_key(salt, key_of(right))
        if a ~= b then
            return a < b
        end
        -- crc32 collides; the name itself is the tie-break, so the order is
        -- total and does not depend on table.sort's internals.
        return key_of(left) < key_of(right)
    end)
end

--- Where a score falls in the calibration sample, as a percentage.
--
-- `cuts` is ascending, cuts[k] being the (k * bucket)-th percentile of the
-- scores the calibration sample returned. The answer is the largest k * bucket
-- whose cut the score reaches, and 0 when it reaches none -- so a user at 95
-- with bucket 5 scored at least as high as 95% of the calibration sample, and
-- one at 0 is in the bottom 5%.
--
-- The 2016 predictCalibrated did neither half of this. Its lowest bucket was
-- `1 * step` rather than 0 and its highest `nPercentiles * step`, so bucket 0
-- was unreachable and the top one was a duplicate of its neighbour; and it
-- added `math.random(0, step)` to the result, which made the same user's rank
-- differ between two runs over the same data.
local function percentile_of(score, cuts, bucket)
    local rank = 0
    for i = 1, #cuts do
        if score < cuts[i] then
            break
        end
        rank = i
    end
    return rank * bucket
end

-------------------------------------------------------------------------------
-- Worker context
-------------------------------------------------------------------------------

local context_mt

local function space_name_of(task)
    return 'task_' .. task:gsub('%W', '_') .. '_ds'
end

--- Create the space a task stages its training set in, and let the job reach
--- it.
--
-- **This is only possible while the role is applying its config**, which is
-- why worker_context() calls it and nothing else does. Everything else an app
-- module runs -- its loader, its compute function -- happens inside the
-- `pregel.worker.deliver` / `pregel.worker.preload` RPC and therefore with the
-- privileges of `roles_cfg.user`, and that user has:
--
--   * no write access to `_space`, so `box.schema.space.create` raises
--     "Write access to space '_space' is denied for user 'pregel'"
--   * no access to a space it was not granted, and pregel.worker.grant() only
--     covers pregel's own four
--
-- Hence both halves here: the DDL, and a grant of the app's own space to the
-- same user roles_cfg names. The app module cannot read roles_cfg, so that
-- user's name has to arrive through app_cfg -- see `grant_to` in config.yaml.
-- A job whose peers connect as guest with a universe grant needs neither and
-- leaves it unset.
--
-- `temporary`, because the space is scratch: it is written once from the
-- answers to a round of FETCH messages, read a few hundred times by the
-- gradient descent loop and once more for the AUC. A restarted job rebuilds it
-- from the labels rather than from a WAL.
--
-- Every worker gets a space for every task, not only for the tasks whose
-- vertices it owns. Which those are is a hash of the vertex name against the
-- worker list, and the mpool that computes it does not exist yet at the one
-- moment DDL is allowed. An unused one costs an empty space.
local function ensure_space(self, task)
    local name = space_name_of(task)
    local owner = self.space_owner[name]
    if owner ~= nil and owner ~= task then
        error(string.format(
            "lookalike: tasks %q and %q both want the space %q; task names " ..
            'must differ in more than punctuation', owner, task, name))
    end

    local space = box.space[name]
    if space == nil then
        space = box.schema.space.create(name, {
            temporary = true,
            format    = {
                {name = 'split',    type = 'string'},
                {name = 'vid',      type = 'string'},
                {name = 'target',   type = 'number'},
                {name = 'features', type = 'array' },
            },
        })
        space:create_index('primary', {
            type  = 'TREE',
            parts = {{field = 1, type = 'string'},
                     {field = 2, type = 'string'}},
        })
    end
    if self.grant_to ~= nil then
        box.schema.user.grant(self.grant_to, 'read,write', 'space', name,
                              {if_not_exists = true})
    end
    self.space_owner[name] = task
    self.spaces[task] = space
    return space
end

--- The space `ensure_space` made, or a readable error.
--
-- Separate from creating it because the two happen in different sessions with
-- different privileges, and "there is no space for this task" is a
-- configuration problem worth naming rather than a denied DDL three frames
-- down.
local function space_for(self, task)
    local space = self.spaces[task]
    if space == nil then
        error(string.format(
            'lookalike: no staging space for task %q; it was not in %s when ' ..
            'this worker applied its config', task, space_name_of(task)))
    end
    return space
end

--- Delete every row of a task's staging space.
--
-- Not space:truncate(): truncate writes to the `_truncate` system space and is
-- refused for the same reason the DDL above is. Collect the keys first --
-- deleting from under an open iterator is not defined.
local function clear_space(space)
    local keys = {}
    for _, tuple in space:pairs() do
        table.insert(keys, {tuple.split, tuple.vid})
    end
    for _, key in ipairs(keys) do
        space:delete(key)
    end
end

context_mt = {__index = {space_for = space_for, ensure_space = ensure_space}}

--- Read labels.avro into {task -> array of {vid, target}} and a sorted roster.
local function read_labels(path)
    local by_task, order = {}, {}
    local reader = ocf.open(path, {mode = 'r'})
    for record in reader:records() do
        if by_task[record.task] == nil then
            by_task[record.task] = {}
            table.insert(order, record.task)
        end
        table.insert(by_task[record.task],
                     {vid = record.vid, target = record.target})
    end
    reader:close()
    -- The roster the master starts, and the order the tasks are reported in:
    -- the file's own order would make it depend on how the labels happened to
    -- be written.
    table.sort(order)
    return by_task, order
end

--- Built once per worker, while the role applies its config.
--
-- It reads labels.avro rather than leaving that to the loader, because it has
-- to: the task names are what the staging spaces are named after, and this is
-- the only moment at which a space can be created. The loader then works from
-- what is here, so the file is still read once per worker.
function app.worker_context(app_cfg)
    local cfg = common.cfg(app_cfg, {'labels'})
    local resolved = {}
    for key, fallback in pairs(DEFAULTS) do
        resolved[key] = cfg[key] or fallback
    end

    local labels_path = common.resolve(HERE, cfg.labels, 'labels')
    local labels, roster = read_labels(labels_path)
    -- Refused here rather than left to run, because the job would not stop: a
    -- user vertex halts once every task it can see is terminal, and with no
    -- tasks at all it sees none, waits for the first one to appear, and waits
    -- forever. A startup error naming the file is the only useful thing to do
    -- with a label file that names no task.
    if #roster == 0 then
        error(string.format('lookalike: %s names no task, so there is ' ..
                            'nothing for this job to train', labels_path))
    end

    local context = setmetatable({
        cfg      = resolved,
        labels   = labels,
        roster   = roster,
        grant_to = cfg.grant_to,
        -- Every user vertex this worker owns, filled by the loader. A task
        -- draws the part of its calibration sample the labels do not cover
        -- from here; see calibration_targets.
        data_names  = {},
        spaces      = {},
        space_owner = {},
    }, context_mt)

    for _, task in ipairs(roster) do
        context:ensure_space(task)
    end
    return context
end

-------------------------------------------------------------------------------
-- Loading
-------------------------------------------------------------------------------

--- Turn the input into vertices, on every worker at once.
--
-- Run through worker:preload(), so each worker keeps only the vertices whose
-- names hash to it -- the same split loader.avro_files does, spelled out here
-- because that loader wants a vertex file and an edge file, and this job has
-- no edges at all.
--
-- users.avro is read whole by every worker even though most of it is someone
-- else's share; that is the price of a partitioned load with no index over the
-- file. The labels are already in the worker context, which read them at
-- config-apply time (see worker_context) -- a task vertex needs *all* of its
-- own labels and they are scattered through the file, so there was never a
-- partition to make there. That is the training set, so it is small by
-- construction; the population in users.avro is the part that is not.
function app.worker_preload(instance, app_cfg)
    local cfg = common.cfg(app_cfg, {'users', 'labels'})
    local users_path = common.resolve(HERE, cfg.users, 'users')

    return loader.new(instance, function(self, worker_idx)
        local context = instance.worker_context
        local function owns(name)
            return worker_idx == nil or instance.mpool:id(name) == worker_idx
        end

        local stored = 0
        local reader = ocf.open(users_path, {mode = 'r'})
        for record in reader:records() do
            local name = data_name(record.vid)
            if owns(name) then
                self:store_vertex({
                    name     = name,
                    type     = 'data',
                    vid      = record.vid,
                    features = record.features,
                    scores   = {},
                })
                table.insert(context.data_names, name)
                stored = stored + 1
            end
        end
        reader:close()

        for _, task in ipairs(context.roster) do
            local name = task_name(task)
            if owns(name) then
                self:store_vertex({
                    name     = name,
                    type     = 'task',
                    task     = task,
                    labelled = context.labels[task],
                    phase    = PHASE_NEW,
                })
                stored = stored + 1
            end
        end

        if owns(MASTER_NAME) then
            self:store_vertex({
                name    = MASTER_NAME,
                type    = 'master',
                roster  = context.roster,
                reports = {},
            })
            stored = stored + 1
        end

        self:flush()
        return stored
    end)
end

-------------------------------------------------------------------------------
-- Aggregators
-------------------------------------------------------------------------------

--- Union of two maps, the right-hand one winning.
--
-- Deliberately not in place. An aggregator's accumulator *is* its `default`
-- until the first make_default() runs, so a reduce that mutated what it was
-- handed would rewrite the default for the rest of the job.
local function merge_maps(acc, contribution)
    local rv = {}
    for key, value in pairs(acc or {}) do
        rv[key] = value
    end
    for key, value in pairs(contribution or {}) do
        rv[key] = value
    end
    return rv
end

local function add(acc, contribution)
    return (acc or 0) + (contribution or 0)
end

--- Two aggregators, and one of them is the only channel wide enough for what
-- the prediction phase needs.
--
-- `model` is a map from task name to its published state -- {state, weights,
-- cuts, bucket, report}. A task writes its own key and reads nobody's; the
-- users read all of it. This is what replaces a broadcast: a task that had to
-- send its weights to every user by name would need a list of every user in
-- the job, on the one worker that happens to own that task vertex, and would
-- then put one message per user on the wire per task. An aggregator costs one
-- copy of the model per worker per superstep instead, and every user reads it
-- for free -- at the price of the users having to stay awake to look, which is
-- what `pending` is about.
--
-- One aggregator keyed by task rather than the `weights_<task>` and
-- `report_<task>` the 2016 shape suggests: aggregators are declared by the app
-- module as a static table and registered when the role applies its config, so
-- there is no moment at which a name derived from the data could be one. The
-- task names are not known until labels.avro has been read, which happens
-- after that.
--
-- `pending` counts the users that are still waiting for some task's model. It
-- is how the tasks and the master learn that the job is over: they cannot see
-- the users, and the users cannot address them all.
app.aggregators = {
    model   = {default = {}, reduce = merge_maps, merge = merge_maps},
    pending = {default = 0,  reduce = add,        merge = add},
}

-------------------------------------------------------------------------------
-- The master vertex
-------------------------------------------------------------------------------

--- Start the tasks, collect their reports, and go last.
--
-- The 2016 master created the task vertices with add_vertex, because it was
-- the only vertex that had read the file naming the tasks. Here the loader has
-- read labels.avro and can create them directly, so what is left is the two
-- jobs a single vertex is genuinely good for: kicking the tasks off, and
-- holding every report in one place that an operator can read with one
-- `box.space.data_<job>:get('master')` instead of `model` -- which, being an
-- aggregator, only holds what the last superstep contributed.
local function compute_master(self)
    local value = self:get_value()
    local model = self:get_aggregation('model') or {}

    if self:get_superstep() == 1 then
        for _, task in ipairs(value.roster) do
            self:send_message(task_name(task), {
                command = START,
                from    = self:get_name(),
            })
        end
    end

    local reports, waiting = {}, 0
    for _, task in ipairs(value.roster) do
        local entry = model[task]
        if entry == nil or entry.report == nil then
            waiting = waiting + 1
        else
            reports[task] = entry.report
        end
    end
    value.reports = reports
    self:set_value(value)

    -- Nothing left to report and nobody left scoring: the last superstep this
    -- vertex takes part in. `pending` is the previous superstep's total, so a
    -- user that scored itself in it has already stopped counting.
    if waiting == 0 and self:get_aggregation('pending') == 0 then
        self:vote_halt(true)
    else
        self:vote_halt(false)
    end
end

-------------------------------------------------------------------------------
-- The task vertex
-------------------------------------------------------------------------------

--- Split the labelled users into a training and a held-out set.
--
-- Stratified: the fraction is taken out of each class separately, so a task
-- whose labels lean one way -- both of the fixture's do -- still gets both
-- classes on both sides. A single pass of coin flips over all the labels, as
-- the 2016 code did, can hand the held-out set one class and no other, and the
-- AUC of that is not a number at all.
local function split_labels(rows, fraction)
    local positive, negative = {}, {}
    for _, row in ipairs(rows) do
        table.insert(row.target > 0 and positive or negative, row)
    end
    local train, test = {}, {}
    for _, class in ipairs({positive, negative}) do
        sort_by_key(class, 'split', function(row) return row.vid end)
        local n_test = math.floor(#class * fraction + 0.5)
        for i, row in ipairs(class) do
            table.insert(i <= n_test and test or train, row)
        end
    end
    return train, test
end

--- The users a task asks for a raw score during calibration.
--
-- Two sources, and the reason for both is that a task vertex lives on one
-- worker and the population does not:
--
--   * the users it holds labels for. They are spread over every worker,
--     because a vertex's worker is a hash of its name and the labels were
--     drawn without regard to it.
--   * a draw from this worker's own users, when the labels are fewer than
--     `calibration_sample`.
--
-- The second is a sample of one shard rather than of the population, which
-- would be a real bias if the shard were special. It is not: a worker's share
-- is chosen by crc32 of the user's name, which has nothing to do with that
-- user's features, so the shard is itself a uniform sample. What it does bias
-- is *coverage* of a population whose feature distribution varies by name --
-- there is none here, and a job where the names carry meaning should send only
-- to the labelled users and raise `labelled_fraction` instead.
local function calibration_targets(context, labelled)
    local want = context.cfg.calibration_sample
    local chosen, seen = {}, {}

    local ordered = {}
    for _, row in ipairs(labelled) do
        table.insert(ordered, row.vid)
    end
    sort_by_key(ordered, 'calibration', function(vid) return vid end)
    for _, vid in ipairs(ordered) do
        if #chosen >= want then
            break
        end
        local name = data_name(vid)
        if not seen[name] then
            seen[name] = true
            table.insert(chosen, name)
        end
    end

    if #chosen < want then
        local local_names = {}
        for _, name in ipairs(context.data_names) do
            table.insert(local_names, name)
        end
        sort_by_key(local_names, 'calibration-local',
                    function(name) return name end)
        for _, name in ipairs(local_names) do
            if #chosen >= want then
                break
            end
            if not seen[name] then
                seen[name] = true
                table.insert(chosen, name)
            end
        end
    end

    return chosen
end

--- Fill this task's space from the answers to its FETCH messages.
--
-- @return the number of rows written, and the train/test sizes
local function stage_dataset(context, task, messages)
    local space = context:space_for(task)
    clear_space(space)

    local rows = {}
    for _, message in ipairs(messages) do
        table.insert(rows, {
            vid      = message.vid,
            target   = message.target,
            features = message.features,
        })
    end

    local train, test = split_labels(rows, context.cfg.test_fraction)
    for _, row in ipairs(train) do
        space:replace{SPLIT_TRAIN, row.vid, row.target, row.features}
    end
    for _, row in ipairs(test) do
        space:replace{SPLIT_TEST, row.vid, row.target, row.features}
    end
    return #rows, train, test
end

--- Minibatch gradient descent over what the space holds.
--
-- The batches are drawn from the space one sample at a time rather than from a
-- copy of the training set in memory. That is the whole reason the space is
-- there: a task's training set is bounded by the labels rather than by the
-- population, but "bounded by the labels" is still a number the operator
-- chose, and a vertex value has to be msgpack'd into a tuple on every write.
local function train_model(context, space, train, dim)
    local cfg = context.cfg

    local order = {}
    for _, row in ipairs(train) do
        table.insert(order, row.vid)
    end
    sort_by_key(order, 'batch', function(vid) return vid end)

    local position = 0
    local function next_batch()
        local batch = {}
        for _ = 1, cfg.batch_size do
            position = position % #order + 1
            local tuple = space:get{SPLIT_TRAIN, order[position]}
            table.insert(batch, {t = tuple.target, x = tuple.features})
        end
        return batch
    end

    local optimiser = gd.new({
        loss          = 'hinge',
        regulariser   = 'l2',
        lambda        = cfg.lambda,
        learning_rate = gd.learning_rate.inverse_decay(cfg.learning_rate,
                                                       cfg.learning_decay),
    })
    -- Started from zero rather than from gd:initialize()'s random draw: the
    -- example is asserted on, and a model that depends on math.random cannot
    -- be. Zero is also the point the hinge loss has a gradient everywhere
    -- around, so nothing is lost by not breaking symmetry -- there is no
    -- symmetry to break in a linear model.
    return optimiser:train(next_batch, {
        w        = vector.zeros(dim),
        alpha    = cfg.alpha,
        epsilon  = cfg.epsilon,
        max_iter = cfg.max_iter,
    })
end

--- AUC of the model on the split it was not trained on.
--
-- On the raw score, and on the *test* rows. The 2016 code did neither: it read
-- the rows whose split was `train`, under a function called
-- applyModelToTestDataSets, and scored them with predictCalibrated called with
-- one argument too many -- so the calibration percentage arrived where the
-- feature vector was expected and every user was scored against the task's own
-- weights instead of its own features. The number it published was the AUC of
-- the training set, of a model applied to the wrong vector.
local function measure_auc(space, weights)
    local collector = auc.new()
    for _, tuple in space.index.primary:pairs({SPLIT_TEST}) do
        collector:add(gd.score(tuple.features, weights), tuple.target)
    end
    return collector:result(), collector:count()
end

--- The percentile cut points of a calibration sample.
--
-- 100 / bucket - 1 of them, at bucket, 2 * bucket, ... 100 - bucket percent --
-- the interior boundaries of the buckets, which is what a rank is read off.
-- The 2016 calibrate() asked for (p + 1) * bucket for p in 1..n, so its first
-- cut was at 2 * bucket and its last at 100: the bottom two buckets were one
-- bucket and the top one was empty, because no score exceeds the maximum.
local function calibration_cuts(counter, bucket)
    local cuts = {}
    local count = math.floor(100 / bucket) - 1
    for k = 1, count do
        cuts[k] = counter:percentile(k * bucket)
    end
    return cuts
end

local function fail_task(self, value, reason)
    value.phase = PHASE_FAILED
    value.report = {
        task   = value.task,
        state  = PHASE_FAILED,
        reason = reason,
    }
    self:set_value(value)
end

local function compute_task(self)
    local context = self:get_worker_context()
    local value   = self:get_value()
    local cfg     = context.cfg

    local messages = {}
    local started  = false
    for _, message in self:pairs_messages() do
        if message.command == START then
            started = true
        else
            table.insert(messages, message)
        end
    end

    -- A question asked in superstep S is read in S+1 and answered into S+2, so
    -- a phase that sent messages has one superstep of nothing to do before the
    -- answers exist. `await` is the superstep they are due in, and without it
    -- the phase runs immediately on an empty inbox and concludes that nobody
    -- answered -- which is exactly what the 2016 code's "Master didn't receive
    -- any messages, waiting one superstep" branch was papering over, one
    -- superstep at a time and with no way to tell a slow round trip from a
    -- question nobody could answer.
    local due = value.await == nil or self:get_superstep() >= value.await

    if value.phase == PHASE_NEW and started then
        -- SELECTION. The label travels with the request, so the answer can
        -- carry it back and the task needs no second lookup to pair a feature
        -- vector with its target.
        if #value.labelled < cfg.min_labels then
            fail_task(self, value, string.format(
                'task %q has %d label(s), fewer than the %d this job needs',
                value.task, #value.labelled, cfg.min_labels))
        else
            for _, row in ipairs(value.labelled) do
                self:send_message(data_name(row.vid), {
                    command = FETCH,
                    from    = self:get_name(),
                    task    = value.task,
                    target  = row.target,
                })
            end
            value.phase = PHASE_TRAINING
            value.await = self:get_superstep() + 2
            self:set_value(value)
        end

    elseif value.phase == PHASE_TRAINING and due then
        local answered = {}
        for _, message in ipairs(messages) do
            if message.command == FEATURES then
                table.insert(answered, message)
            end
        end
        if #answered < cfg.min_labels then
            fail_task(self, value, string.format(
                'task %q got %d feature vector(s) back for %d label(s): the ' ..
                'users behind the rest are not in this job',
                value.task, #answered, #value.labelled))
        else
            local n, train, test = stage_dataset(context, value.task, answered)
            local space = context:space_for(value.task)
            local dim = #answered[1].features + 1

            local weights, history = train_model(context, space, train, dim)
            local area, tested = measure_auc(space, weights)

            value.weights = weights
            value.report  = {
                task       = value.task,
                state      = 'ready',
                features   = dim - 1,
                labelled   = #value.labelled,
                answered   = n,
                train_size = #train,
                test_size  = #test,
                scored     = tested,
                iterations = history.iterations,
                converged  = history.converged,
                loss       = history.losses[history.iterations],
                auc        = area,
            }

            -- CALIBRATION starts in the same superstep the training ended in:
            -- the weights are what the sample is asked to score against.
            local targets = calibration_targets(context, value.labelled)
            for _, name in ipairs(targets) do
                self:send_message(name, {
                    command = PREDICT_CALIBRATE,
                    from    = self:get_name(),
                    task    = value.task,
                    weights = weights,
                })
            end
            value.report.calibration_sent = #targets
            value.phase = PHASE_CALIBRATION
            value.await = self:get_superstep() + 2
            self:set_value(value)
        end

    elseif value.phase == PHASE_CALIBRATION and due then
        local counter = percentile.new()
        for _, message in ipairs(messages) do
            if message.command == SCORE then
                counter:add(message.score)
            end
        end
        if counter:count() == 0 then
            fail_task(self, value, string.format(
                'task %q got no calibration scores back', value.task))
        else
            value.cuts = calibration_cuts(counter, cfg.calibration_bucket)
            value.report.calibration_size = counter:count()
            value.phase = PHASE_DONE
            self:set_value(value)
        end
    end

    -- Published every superstep, terminal or not. A task that only wrote its
    -- entry once would be invisible in the next one: the master resets every
    -- aggregator before the workers report into it, so an aggregator holds the
    -- previous superstep's contributions and nothing older. Which is also why
    -- this runs to the very last superstep: the report an operator reads out
    -- of the master's copy of `model` is the one contributed here.
    local entry = {
        state  = value.phase == PHASE_FAILED and PHASE_FAILED or
                 (value.phase == PHASE_DONE and 'ready' or 'training'),
        report = value.report,
    }
    if entry.state == 'ready' then
        entry.weights = value.weights
        entry.cuts    = value.cuts
        entry.bucket  = cfg.calibration_bucket
    end
    self:set_aggregation('model', {[value.task] = entry})

    local terminal = value.phase == PHASE_DONE or value.phase == PHASE_FAILED
    if terminal and self:get_aggregation('pending') == 0 then
        self:vote_halt(true)
    else
        self:vote_halt(false)
    end
end

-------------------------------------------------------------------------------
-- The user vertex
-------------------------------------------------------------------------------

local function compute_data(self)
    local value = self:get_value()

    for _, message in self:pairs_messages() do
        if message.command == FETCH then
            self:send_message(message.from, {
                command  = FEATURES,
                from     = self:get_name(),
                task     = message.task,
                vid      = value.vid,
                target   = message.target,
                features = value.features,
            })
        elseif message.command == PREDICT_CALIBRATE then
            self:send_message(message.from, {
                command = SCORE,
                from    = self:get_name(),
                task    = message.task,
                score   = gd.score(value.features, message.weights),
            })
        else
            error(string.format('lookalike: %s got an unknown command %q',
                                self:get_name(), tostring(message.command)))
        end
    end

    -- PREDICTION. No message wakes a user for this: the models arrive through
    -- an aggregator, which only a vertex that is being computed can read, so
    -- every user stays awake until it has scored every task. That is the cost
    -- of not broadcasting -- one compute call per user per superstep, over the
    -- whole population, for as long as the slowest task takes.
    local model   = self:get_aggregation('model') or {}
    local scores  = value.scores or {}
    local changed = false
    local known, waiting = 0, 0

    for task, entry in pairs(model) do
        known = known + 1
        if entry.state == 'ready' then
            if scores[task] == nil then
                local score = gd.score(value.features, entry.weights)
                scores[task] = {
                    score      = score,
                    percentile = percentile_of(score, entry.cuts, entry.bucket),
                }
                changed = true
            end
        elseif entry.state ~= PHASE_FAILED then
            waiting = waiting + 1
        end
    end

    if changed then
        value.scores = scores
        self:set_value(value)
    end

    -- `known == 0` is superstep 1, before any task has contributed: there is
    -- nothing to wait for yet and halting here would end the job before it
    -- started.
    if known > 0 and waiting == 0 then
        self:vote_halt(true)
    else
        self:set_aggregation('pending', 1)
        self:vote_halt(false)
    end
end

-------------------------------------------------------------------------------

function app.compute(self)
    local kind = self:get_value().type
    if kind == 'data' then
        return compute_data(self)
    elseif kind == 'task' then
        return compute_task(self)
    elseif kind == 'master' then
        return compute_master(self)
    end
    error(string.format('lookalike: vertex %s has no known type (%s)',
                        self:get_name(), tostring(kind)))
end

-- Read by the test and by the README's transcript; nothing in the job uses it.
app.MASTER_NAME = MASTER_NAME
app.DEFAULTS    = DEFAULTS

return app
