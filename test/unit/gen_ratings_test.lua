--- tools/gen-ratings.lua, driven as the command it is.
--
-- Every case runs the real script in a child tarantool and reads the files it
-- left behind, because that is the only way to test the parts a caller
-- actually depends on: the argument names, the schema, the train/test split's
-- promise that no test rating names a user or item training never saw, and --
-- the one the committed fixture rests on -- that the same arguments produce
-- the same bytes twice.

local t     = require('luatest')
local fio   = require('fio')
local json  = require('json')
local popen = require('popen')

local avro = require('pregel.avro')

local g = t.group('gen_ratings')

local ROOT = fio.dirname(fio.dirname(
    fio.dirname(fio.abspath(debug.getinfo(1, 'S').source:sub(2)))))
local TOOL = fio.pathjoin(ROOT, 'tools', 'gen-ratings.lua')

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
    run_dir = fio.pathjoin(base, 'gen-ratings-test')
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
                    'gen-ratings failed: ' .. stderr .. stdout)
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

--- mu + b_u + b_i + p_u . q_i, from what truth.json carries -- everything
--- about a rating except its noise draw.
local function predict(truth, user, item)
    local p, q = truth.factors.users[user], truth.factors.items[item]
    t.assert_not_equals(p, nil, 'truth.json has no factors for ' .. user)
    t.assert_not_equals(q, nil, 'truth.json has no factors for ' .. item)
    local dot = 0
    for j = 1, #p do
        dot = dot + p[j] * q[j]
    end
    return truth.mu + truth.biases.users[user] + truth.biases.items[item] + dot
end

local function clip(x)
    if x < 1 then
        return 1
    elseif x > 5 then
        return 5
    end
    return x
end

local function to_star(x)
    return math.floor(clip(x) * 2 + 0.5) / 2
end

--------------------------------------------------------------------------------
-- Shape
--------------------------------------------------------------------------------

g.test_writes_both_halves_with_the_documented_schema = function()
    local dir = generate('shape', {
        '--users', 40, '--items', 20, '--rank', 2, '--density', 0.5,
        '--test-fraction', 0.25, '--seed', 3,
    })

    local train, train_schema = avro.ocf.read_all(
        fio.pathjoin(dir, 'train.avro'))
    local test, test_schema = avro.ocf.read_all(fio.pathjoin(dir, 'test.avro'))
    for _, sc in ipairs({train_schema, test_schema}) do
        t.assert_equals(sc.name, 'Rating')
        t.assert_equals(field_names(sc), {'user', 'item', 'rating'})
    end

    local truth = read_truth(dir)
    t.assert_equals(truth.mu, 3.5)
    t.assert_equals(truth.users, 40)
    t.assert_equals(truth.items, 20)
    t.assert_equals(truth.rank, 2)
    t.assert_equals(truth.seed, 3)
    -- The counts truth.json reports are the counts the files hold.
    t.assert_equals(truth.counts.train, #train)
    t.assert_equals(truth.counts.test, #test)
    t.assert_equals(truth.counts.ratings, #train + #test)
    -- Half of 800 pairs, give or take the draw.
    t.assert_almost_equals(truth.counts.ratings / (40 * 20), 0.5, 0.05)
    -- A quarter of them in test, less whatever the split had to move back.
    t.assert_equals(#test + truth.counts.moved_to_train,
                    math.floor(truth.counts.ratings * 0.25 + 0.5))

    for u = 1, 40 do
        t.assert_equals(#truth.factors.users['u' .. u], 2)
        t.assert_type(truth.biases.users['u' .. u], 'number')
    end
    for i = 1, 20 do
        t.assert_equals(#truth.factors.items['i' .. i], 2)
        t.assert_type(truth.biases.items['i' .. i], 'number')
    end

    for _, rows in ipairs({train, test}) do
        for _, r in ipairs(rows) do
            t.assert_str_matches(r.user, 'u%d+')
            t.assert_str_matches(r.item, 'i%d+')
            t.assert(r.rating >= 1 and r.rating <= 5,
                     'rating out of range: ' .. tostring(r.rating))
            t.assert_equals(r.rating * 2, math.floor(r.rating * 2),
                            'rating is not a multiple of 0.5: ' ..
                            tostring(r.rating))
        end
    end
end

g.test_no_pair_is_rated_twice = function()
    local dir = generate('pairs', {
        '--users', 30, '--items', 15, '--density', 0.5, '--seed', 8,
    })
    local seen = {}
    for _, name in ipairs({'train.avro', 'test.avro'}) do
        for _, r in ipairs(avro.ocf.read_all(fio.pathjoin(dir, name))) do
            local key = r.user .. '/' .. r.item
            t.assert_equals(seen[key], nil, key .. ' is rated twice')
            seen[key] = true
        end
    end
end

--------------------------------------------------------------------------------
-- The split
--------------------------------------------------------------------------------

--- Every user and item test.avro names must also appear in train.avro; a test
--- case factorisation cannot answer at all is not a test case.
local function assert_no_cold_start(dir)
    local known_users, known_items = {}, {}
    for _, r in ipairs(avro.ocf.read_all(fio.pathjoin(dir, 'train.avro'))) do
        known_users[r.user] = true
        known_items[r.item] = true
    end
    local test = avro.ocf.read_all(fio.pathjoin(dir, 'test.avro'))
    for _, r in ipairs(test) do
        t.assert(known_users[r.user],
                 'test.avro rates ' .. r.user .. ', who has no train rating')
        t.assert(known_items[r.item],
                 'test.avro rates ' .. r.item .. ', which has no train rating')
    end
    return #test
end

g.test_the_test_half_never_names_a_cold_user_or_item = function()
    -- Dense enough that the split leaves everyone covered on its own.
    assert_no_cold_start(generate('cold-dense', {
        '--users', 40, '--items', 20, '--density', 0.5, '--seed', 4,
    }))
    -- Sparse enough, and with half the ratings held out, that it does not --
    -- this is the case the move back into train exists for.
    local dir = generate('cold-sparse', {
        '--users', 200, '--items', 100, '--density', 0.02,
        '--test-fraction', 0.5, '--seed', 3,
    })
    assert_no_cold_start(dir)
    t.assert_gt(read_truth(dir).counts.moved_to_train, 0,
                'the sparse case moved nothing back, so it does not exercise ' ..
                'the repair it was chosen for')
end

g.test_moving_a_rating_back_keeps_the_total = function()
    local dir = generate('moved', {
        '--users', 200, '--items', 100, '--density', 0.02,
        '--test-fraction', 0.5, '--seed', 3,
    })
    local truth = read_truth(dir)
    local train = avro.ocf.read_all(fio.pathjoin(dir, 'train.avro'))
    local test  = avro.ocf.read_all(fio.pathjoin(dir, 'test.avro'))
    t.assert_equals(#train + #test, truth.counts.ratings)
    -- The moved ratings left the test half and joined the train half; nothing
    -- was re-drawn, so the total is untouched.
    t.assert_equals(#test + truth.counts.moved_to_train,
                    math.floor(truth.counts.ratings * 0.5 + 0.5))
end

--------------------------------------------------------------------------------
-- The seed
--------------------------------------------------------------------------------

--- u1's bias is the first thing the generator draws, so it is where a seeding
--- mistake shows in the output: it is `0.3 * N(0,1)` built by Box-Muller from
--- the stream's first two uniforms.
--
-- A linear congruential generator started at `seed + 1` returns
-- `A * (seed + 1) / M` first, which for every seed a human types is of the
-- order of 1e-4 -- and Box-Muller turns a first uniform that small into
-- `sqrt(-2 ln u) > 4`, so u1's bias comes out four standard deviations wide
-- for seed 1 and for seed 10 alike. Under the documented N(0, 0.3) a draw
-- beyond two standard deviations happens 4.6% of the time, so at most a
-- couple of ten seeds should manage it.
local function first_user_biases(seeds)
    local out = {}
    for i, seed in ipairs(seeds) do
        local dir = generate('seed-' .. seed, {
            '--users', 4, '--items', 4, '--rank', 1, '--density', 1,
            '--seed', seed,
        })
        out[i] = read_truth(dir).biases.users.u1
    end
    return out
end

g.test_small_seeds_do_not_all_draw_an_extreme_first_bias = function()
    local seeds = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
    local biases = first_user_biases(seeds)

    local extreme, total = 0, 0
    for _, b in ipairs(biases) do
        total = total + math.abs(b)
        if math.abs(b) > 0.6 then
            extreme = extreme + 1
        end
    end
    t.assert_le(extreme, 3,
                ('%d of 10 small seeds put u1 beyond two standard ' ..
                 'deviations: %s'):format(extreme, json.encode(biases)))
    -- E|N(0, 0.3)| is 0.239; a mean of ten such draws above 0.45 is far off.
    t.assert_lt(total / #biases, 0.45,
                'the mean first bias over seeds 1..10 is ' ..
                tostring(total / #biases) .. ', not the 0.24 of N(0, 0.3)')
end

--------------------------------------------------------------------------------
-- Determinism
--------------------------------------------------------------------------------

g.test_the_same_arguments_produce_byte_identical_files = function()
    local args = {
        '--users', 60, '--items', 40, '--rank', 4, '--density', 0.25,
        '--noise', 0.5, '--test-fraction', 0.3, '--seed', 99,
    }
    local first  = generate('det-1', args)
    local second = generate('det-2', args)
    for _, name in ipairs({'train.avro', 'test.avro', 'truth.json'}) do
        t.assert_equals(read_file(fio.pathjoin(first, name)),
                        read_file(fio.pathjoin(second, name)),
                        name .. ' differs between two runs of the same seed')
    end
end

g.test_a_different_seed_produces_different_files = function()
    local function with_seed(name, seed)
        return generate(name, {
            '--users', 60, '--items', 40, '--density', 0.25, '--seed', seed,
        })
    end
    local a = with_seed('seed-99', 99)
    local b = with_seed('seed-100', 100)
    t.assert_not_equals(read_file(fio.pathjoin(a, 'train.avro')),
                        read_file(fio.pathjoin(b, 'train.avro')),
                        'two seeds produced the same train.avro')
end

--------------------------------------------------------------------------------
-- The model behind the data
--------------------------------------------------------------------------------

g.test_truth_json_reproduces_every_rating_when_there_is_no_noise = function()
    local dir = generate('exact', {
        '--users', 60, '--items', 40, '--rank', 3, '--density', 0.3,
        '--noise', 0, '--seed', 5,
    })
    local truth = read_truth(dir)
    local n = 0
    for _, name in ipairs({'train.avro', 'test.avro'}) do
        for _, r in ipairs(avro.ocf.read_all(fio.pathjoin(dir, name))) do
            local p = predict(truth, r.user, r.item)
            t.assert_equals(to_star(p), r.rating,
                            ('%s/%s: %s does not round to the stored %s')
                            :format(r.user, r.item, tostring(p),
                                    tostring(r.rating)))
            -- And the unrounded prediction is within half a step of it, which
            -- is what "rounded to 0.5" means and what a learner is scored on.
            t.assert_le(math.abs(clip(p) - r.rating), 0.25,
                        ('%s/%s: %s is further than a rounding from %s')
                        :format(r.user, r.item, tostring(p),
                                tostring(r.rating)))
            n = n + 1
        end
    end
    t.assert_equals(n, truth.counts.ratings)
    t.assert_gt(n, 500, 'too few ratings to be worth checking')
end

g.test_noise_moves_the_ratings_off_the_hidden_model = function()
    local function mean_deviation(dir)
        local truth = read_truth(dir)
        local total, n = 0, 0
        for _, name in ipairs({'train.avro', 'test.avro'}) do
            for _, r in ipairs(avro.ocf.read_all(fio.pathjoin(dir, name))) do
                total = total +
                    math.abs(clip(predict(truth, r.user, r.item)) - r.rating)
                n = n + 1
            end
        end
        return total / n
    end
    local base = {'--users', 60, '--items', 40, '--density', 0.3, '--seed', 5}
    local quiet = {unpack(base)}
    quiet[#quiet + 1], quiet[#quiet + 2] = '--noise', 0
    local loud = {unpack(base)}
    loud[#loud + 1], loud[#loud + 2] = '--noise', 1
    local without = mean_deviation(generate('dev-quiet', quiet))
    local with    = mean_deviation(generate('dev-loud', loud))
    t.assert_le(without, 0.25, 'noise 0 left the ratings off their model')
    t.assert_gt(with, without,
                'noise 1 did not move the ratings off the hidden model')
end

g.test_the_ratings_are_centred_on_the_global_mean = function()
    local dir = generate('mean', {
        '--users', 200, '--items', 100, '--density', 0.2, '--seed', 21,
    })
    local total, n = 0, 0
    for _, name in ipairs({'train.avro', 'test.avro'}) do
        for _, r in ipairs(avro.ocf.read_all(fio.pathjoin(dir, name))) do
            total = total + r.rating
            n = n + 1
        end
    end
    -- The biases and the factors are zero-mean, so only the clip at 1 and 5
    -- pulls the average away from mu, and it pulls both ways.
    t.assert_almost_equals(total / n, read_truth(dir).mu, 0.15)
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
    t.assert_str_contains(refuses({'--items', 0}), '--items')
    t.assert_str_contains(refuses({'--rank', 0}), '--rank')
    t.assert_str_contains(refuses({'--density', 0}), '--density')
    t.assert_str_contains(refuses({'--density', 1.5}), '--density')
    t.assert_str_contains(refuses({'--test-fraction', -0.1}),
                          '--test-fraction')
    t.assert_str_contains(refuses({'--codec', 'nope'}), 'codec')
    t.assert_str_contains(refuses({'--nonsense', 1}), 'unknown option')
end

--------------------------------------------------------------------------------
-- The committed fixture
--------------------------------------------------------------------------------

g.test_the_committed_fixture_matches_its_readme = function()
    local dir   = fio.pathjoin(ROOT, 'test', 'fixtures', 'ratings')
    local train = avro.ocf.read_all(fio.pathjoin(dir, 'train.avro'))
    local test  = avro.ocf.read_all(fio.pathjoin(dir, 'test.avro'))
    local truth = read_truth(dir)
    t.assert_equals(truth.users, 50)
    t.assert_equals(truth.items, 30)
    t.assert_equals(truth.rank, 3)
    t.assert_equals(truth.seed, 7)
    t.assert_equals(truth.noise, 0.2)
    t.assert_equals(truth.counts, {ratings = 473, train = 378, test = 95,
                                   moved_to_train = 0})
    t.assert_equals(#train, 378)
    t.assert_equals(#test, 95)
    assert_no_cold_start(dir)
    -- Noise 0.2 against a rounding of 0.25, so most ratings still land on the
    -- half point the hidden model predicts.
    local on_model = 0
    for _, r in ipairs(train) do
        if to_star(predict(truth, r.user, r.item)) == r.rating then
            on_model = on_model + 1
        end
    end
    t.assert_gt(on_model / #train, 0.5,
                'the fixture ratings have drifted off truth.json')
end
