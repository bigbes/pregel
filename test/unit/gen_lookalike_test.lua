--- tools/gen-lookalike.lua, driven as the command it is.
--
-- Every case runs the real script in a child tarantool and reads the files it
-- left behind, because that is the only way to test the parts a caller
-- actually depends on: the argument names, the schemas, and -- the one the
-- committed fixture rests on -- that the same arguments produce the same
-- bytes twice.

local t     = require('luatest')
local fio   = require('fio')
local json  = require('json')
local popen = require('popen')

local avro = require('pregel.avro')

local g = t.group('gen_lookalike')

local ROOT = fio.dirname(fio.dirname(
    fio.dirname(fio.abspath(debug.getinfo(1, 'S').source:sub(2)))))
local TOOL = fio.pathjoin(ROOT, 'tools', 'gen-lookalike.lua')

-- The interpreter running the suite, so that `make test-ee` exercises the tool
-- under the Enterprise build too. popen runs execve rather than execvp, so a
-- bare 'tarantool' would not be looked up on PATH.
local TARANTOOL = arg[-1]

local run_dir

g.before_all(function()
    t.assert(fio.path.exists(TOOL), 'no such tool: ' .. TOOL)
    t.assert_type(TARANTOOL, 'string',
                  'cannot tell which tarantool is running the suite')
    local base = os.getenv('VARDIR') or '/tmp'
    run_dir = fio.pathjoin(base, 'gen-lookalike-test')
    fio.rmtree(run_dir)
    t.assert(fio.mktree(run_dir), 'cannot create ' .. run_dir)
end)

g.after_all(function()
    if run_dir ~= nil then
        fio.rmtree(run_dir)
        run_dir = nil
    end
end)

--------------------------------------------------------------------------------
-- Running the tool
--------------------------------------------------------------------------------

--- Run the generator into a fresh directory under `run_dir` and return it.
--
-- The child's output is small enough -- four lines, or a traceback -- to fit
-- the pipe buffers, so it is safe to wait for the exit before draining them.
local function generate(name, args)
    local out_dir = fio.pathjoin(run_dir, name)
    fio.rmtree(out_dir)
    local argv = {TARANTOOL, TOOL, out_dir}
    for i = 1, #args do
        argv[#argv + 1] = tostring(args[i])
    end
    local ph = popen.new(argv, {stdout = 'pipe', stderr = 'pipe'})
    t.assert_not_equals(ph, nil, 'cannot run ' .. TARANTOOL)
    local status = ph:wait()
    local stdout = ph:read({timeout = 10}) or ''
    local stderr = ph:read({stderr = true, timeout = 10}) or ''
    ph:close()
    t.assert_equals(status.exit_code, 0,
                    'gen-lookalike failed: ' .. stderr .. stdout)
    return out_dir, stdout
end

local function read_file(path)
    local fh = fio.open(path, {'O_RDONLY'})
    t.assert_not_equals(fh, nil, 'missing file ' .. path)
    local data = fh:read()
    fh:close()
    return data
end

local function read_truth(dir)
    return json.decode(read_file(fio.pathjoin(dir, 'truth.json')))
end

local function field_names(sc)
    local out = {}
    for i = 1, #sc.fields do
        out[i] = sc.fields[i].name
    end
    return out
end

--- score = w[1] + w[2..] . x, from the weights truth.json carries.
local function score_of(weights, features)
    local s = weights[1]
    for j = 1, #features do
        s = s + weights[j + 1] * features[j]
    end
    return s
end

--------------------------------------------------------------------------------
-- Shape
--------------------------------------------------------------------------------

g.test_writes_the_three_files_with_the_documented_schemas = function()
    local dir = generate('shape', {
        '--users', 40, '--features', 5, '--tasks', 3,
        '--labelled-fraction', 0.25, '--seed', 3,
    })

    local users, user_schema = avro.ocf.read_all(
        fio.pathjoin(dir, 'users.avro'))
    t.assert_equals(user_schema.name, 'User')
    t.assert_equals(field_names(user_schema), {'vid', 'features'})
    t.assert_equals(#users, 40)
    for i = 1, #users do
        t.assert_equals(users[i].vid, 'u' .. i)
        t.assert_equals(#users[i].features, 5)
        t.assert_type(users[i].features[1], 'number')
    end

    local labels, label_schema = avro.ocf.read_all(
        fio.pathjoin(dir, 'labels.avro'))
    t.assert_equals(label_schema.name, 'Label')
    t.assert_equals(field_names(label_schema), {'task', 'vid', 'target'})
    -- Three tasks over a quarter of forty users.
    t.assert_equals(#labels, 3 * 10)

    local seen_tasks, seen_vids = {}, {}
    for _, label in ipairs(labels) do
        t.assert(label.target == 1 or label.target == -1,
                 'target is not +1/-1: ' .. tostring(label.target))
        seen_tasks[label.task] = (seen_tasks[label.task] or 0) + 1
        local key = label.task .. '/' .. label.vid
        t.assert_equals(seen_vids[key], nil,
                        'user ' .. label.vid .. ' is labelled twice for ' ..
                        label.task)
        seen_vids[key] = true
    end
    t.assert_equals(seen_tasks, {task1 = 10, task2 = 10, task3 = 10})

    local truth = read_truth(dir)
    t.assert_equals(truth.users, 40)
    t.assert_equals(truth.features, 5)
    t.assert_equals(truth.seed, 3)
    t.assert_equals(truth.labelled_fraction, 0.25)
    for k = 1, 3 do
        local task = truth.tasks['task' .. k]
        t.assert_not_equals(task, nil, 'truth.json has no task' .. k)
        -- One weight per feature, plus the bias.
        t.assert_equals(#task.weights, 6)
    end
end

g.test_labels_only_name_users_that_users_avro_declares = function()
    local dir = generate('vids', {'--users', 60, '--features', 4, '--seed', 5})
    local known = {}
    for _, user in ipairs(avro.ocf.read_all(fio.pathjoin(dir, 'users.avro'))) do
        known[user.vid] = true
    end
    for _, label in ipairs(avro.ocf.read_all(
            fio.pathjoin(dir, 'labels.avro'))) do
        t.assert(known[label.vid],
                 'labels.avro names ' .. label.vid ..
                 ', which users.avro never declared')
    end
end

--------------------------------------------------------------------------------
-- The seed
--------------------------------------------------------------------------------

--- The first weight of task1 is the very first draw of the stream, so it is
--- the one place a seeding mistake is visible in the output: `weights[1]` is
--- `2 * u - 1` for the stream's first uniform `u`, exactly, because the
--- weights are quantised to 12 places and 2u-1 is representable there.
--
-- A linear congruential generator started at `seed + 1` returns
-- `A * (seed + 1) / M` first, which for every seed a human types is a fixed
-- tiny number rising with the seed -- so the first parameter of the hidden
-- model would be a near-extreme draw, the same near-extreme draw, for seed 1
-- and seed 10 alike. Ten seeds are enough to see it: a real uniform stream
-- has a 2/10! chance of coming out sorted and a vanishing chance of staying
-- inside a thousandth of the range.
local function first_weights(seeds)
    local out = {}
    for i, seed in ipairs(seeds) do
        local dir = generate('seed-' .. seed, {
            '--users', 4, '--features', 1, '--tasks', 1, '--seed', seed,
        })
        out[i] = read_truth(dir).tasks.task1.weights[1]
    end
    return out
end

g.test_small_seeds_do_not_all_draw_the_same_extreme_first_weight = function()
    local seeds = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
    local w = first_weights(seeds)

    local up, down = true, true
    for i = 2, #w do
        if w[i] <= w[i - 1] then
            up = false
        end
        if w[i] >= w[i - 1] then
            down = false
        end
    end
    t.assert(not up and not down,
             'task1 weights[1] is monotone in the seed: ' ..
             json.encode(w))

    local lo, hi = w[1], w[1]
    for i = 2, #w do
        lo = math.min(lo, w[i])
        hi = math.max(hi, w[i])
    end
    -- Ten uniforms on [-1, 1] span more than half the range with probability
    -- 1 - 11 * 4^-9 ~ 0.99996.
    t.assert_gt(hi - lo, 1.0,
                ('seeds 1..10 draw their first weight from [%f, %f], a %f ' ..
                 'slice of [-1, 1]'):format(lo, hi, hi - lo))
end

--------------------------------------------------------------------------------
-- Determinism
--------------------------------------------------------------------------------

g.test_the_same_arguments_produce_byte_identical_files = function()
    local args = {
        '--users', 120, '--features', 6, '--tasks', 2,
        '--labelled-fraction', 0.4, '--noise', 0.25, '--seed', 99,
    }
    local first  = generate('det-1', args)
    local second = generate('det-2', args)
    for _, name in ipairs({'users.avro', 'labels.avro', 'truth.json'}) do
        t.assert_equals(read_file(fio.pathjoin(first, name)),
                        read_file(fio.pathjoin(second, name)),
                        name .. ' differs between two runs of the same seed')
    end
end

g.test_a_different_seed_produces_different_files = function()
    local base = {'--users', 120, '--features', 6, '--tasks', 2}
    local function with_seed(name, seed)
        local args = {}
        for i = 1, #base do
            args[i] = base[i]
        end
        args[#args + 1], args[#args + 2] = '--seed', seed
        return generate(name, args)
    end
    local a = with_seed('seed-99', 99)
    local b = with_seed('seed-100', 100)
    t.assert_not_equals(read_file(fio.pathjoin(a, 'users.avro')),
                        read_file(fio.pathjoin(b, 'users.avro')),
                        'two seeds produced the same users.avro')
end

--------------------------------------------------------------------------------
-- The model behind the data
--------------------------------------------------------------------------------

g.test_truth_json_reproduces_every_label_when_there_is_no_noise = function()
    local dir = generate('exact', {
        '--users', 300, '--features', 12, '--tasks', 3,
        '--labelled-fraction', 1, '--noise', 0, '--seed', 17,
    })
    local truth = read_truth(dir)
    local features = {}
    for _, user in ipairs(avro.ocf.read_all(fio.pathjoin(dir, 'users.avro'))) do
        features[user.vid] = user.features
    end
    local labels = avro.ocf.read_all(fio.pathjoin(dir, 'labels.avro'))
    t.assert_equals(#labels, 3 * 300)
    for _, label in ipairs(labels) do
        local weights = truth.tasks[label.task].weights
        local score = score_of(weights, features[label.vid])
        t.assert_equals(label.target, score > 0 and 1 or -1,
                        ('%s/%s: score %s does not explain target %d')
                        :format(label.task, label.vid, tostring(score),
                                label.target))
    end
end

g.test_noise_flips_labels_away_from_the_sign_of_the_score = function()
    local args = {
        '--users', 300, '--features', 12, '--tasks', 2,
        '--labelled-fraction', 1, '--seed', 17,
    }
    local function flips(dir)
        local truth = read_truth(dir)
        local features = {}
        for _, user in ipairs(avro.ocf.read_all(
                fio.pathjoin(dir, 'users.avro'))) do
            features[user.vid] = user.features
        end
        local n = 0
        for _, label in ipairs(avro.ocf.read_all(
                fio.pathjoin(dir, 'labels.avro'))) do
            local score = score_of(truth.tasks[label.task].weights,
                                   features[label.vid])
            if label.target ~= (score > 0 and 1 or -1) then
                n = n + 1
            end
        end
        return n
    end
    local quiet = {unpack(args)}
    quiet[#quiet + 1], quiet[#quiet + 2] = '--noise', 0
    local loud = {unpack(args)}
    loud[#loud + 1], loud[#loud + 2] = '--noise', 2
    t.assert_equals(flips(generate('flip-quiet', quiet)), 0)
    t.assert_gt(flips(generate('flip-loud', loud)), 0,
                'noise 2 flipped no label at all')
end

g.test_labels_are_roughly_balanced = function()
    local dir = generate('balance', {
        '--users', 500, '--features', 32, '--tasks', 8,
        '--labelled-fraction', 1, '--noise', 0, '--seed', 11,
    })
    local per_task, positive, total = {}, 0, 0
    for _, label in ipairs(avro.ocf.read_all(
            fio.pathjoin(dir, 'labels.avro'))) do
        local task = per_task[label.task] or {n = 0, positive = 0}
        task.n = task.n + 1
        total = total + 1
        if label.target > 0 then
            task.positive = task.positive + 1
            positive = positive + 1
        end
        per_task[label.task] = task
    end
    -- Over eight tasks the hidden biases average out; a single task can lean
    -- as far as its own bias takes it, which with 32 features is not far.
    t.assert_almost_equals(positive / total, 0.5, 0.08,
                           'the labels are not close to balanced')
    for name, task in pairs(per_task) do
        t.assert_almost_equals(task.positive / task.n, 0.5, 0.25,
                               name .. ' is badly unbalanced')
    end
end

--------------------------------------------------------------------------------
-- Arguments
--------------------------------------------------------------------------------

g.test_it_refuses_arguments_it_cannot_honour = function()
    local function refuses(args)
        local out_dir = fio.pathjoin(run_dir, 'refused')
        local argv = {TARANTOOL, TOOL, out_dir}
        for i = 1, #args do
            argv[#argv + 1] = tostring(args[i])
        end
        local ph = popen.new(argv, {stdout = 'devnull', stderr = 'pipe'})
        t.assert_not_equals(ph, nil, 'cannot run ' .. TARANTOOL)
        local status = ph:wait()
        local stderr = ph:read({stderr = true, timeout = 10}) or ''
        ph:close()
        t.assert_not_equals(status.exit_code, 0,
                            'accepted ' .. json.encode(args))
        return stderr
    end
    t.assert_str_contains(refuses({'--users', 0}), '--users')
    t.assert_str_contains(refuses({'--labelled-fraction', 1.5}),
                          '--labelled-fraction')
    t.assert_str_contains(refuses({'--noise', -1}), '--noise')
    t.assert_str_contains(refuses({'--codec', 'nope'}), 'codec')
    t.assert_str_contains(refuses({'--nonsense', 1}), 'unknown option')
end

--------------------------------------------------------------------------------
-- The committed fixture
--------------------------------------------------------------------------------

g.test_the_committed_fixture_matches_its_readme = function()
    local dir = fio.pathjoin(ROOT, 'test', 'fixtures', 'lookalike')
    local users  = avro.ocf.read_all(fio.pathjoin(dir, 'users.avro'))
    local labels = avro.ocf.read_all(fio.pathjoin(dir, 'labels.avro'))
    local truth  = read_truth(dir)
    t.assert_equals(#users, 200)
    t.assert_equals(#users[1].features, 8)
    t.assert_equals(#labels, 120)
    t.assert_equals(truth.users, 200)
    t.assert_equals(truth.features, 8)
    t.assert_equals(truth.seed, 7)
    t.assert_equals(truth.noise, 0.5)
    t.assert_equals(truth.labelled_fraction, 0.3)
    t.assert_equals(#truth.tasks.task1.weights, 9)
    t.assert_equals(#truth.tasks.task2.weights, 9)
end
