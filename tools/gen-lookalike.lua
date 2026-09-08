#!/usr/bin/env tarantool
--- Generate a synthetic look-alike dataset: users with dense features, and
--- per-task binary labels drawn from a hidden linear model.
--
--     tarantool tools/gen-lookalike.lua <out_dir> [--users N] [--features D]
--         [--tasks K] [--labelled-fraction f] [--noise s] [--seed n]
--         [--codec null|deflate|zstandard]
--
-- Defaults: 1000 users, 16 features, 2 tasks, 0.3 labelled, noise 0.5,
-- seed 42, codec null.
--
-- Three files land in <out_dir>:
--
--     users.avro   record User  { string vid; array<double> features; }
--     labels.avro  record Label { string task; string vid; int target; }
--     truth.json   the hidden weight vectors and the parameters that made them
--
-- The model. Every task k gets a hidden weight vector w_k of D+1 entries drawn
-- uniformly from [-1, 1] -- w_k[1] is the bias and w_k[2..D+1] multiply the
-- features. Every user gets x ~ N(0,1)^D. Then
--
--     score_k(u) = w_k[1] + w_k[2..] . x(u)
--     target     = +1 if score_k(u) + noise * N(0,1) > 0 else -1
--
-- so `noise` is the standard deviation of the label flip: at 0 the labels are
-- exactly the sign of the score and a learner can reach them, and above it the
-- Bayes error grows. labels.avro holds only a random `labelled-fraction` of
-- the users, drawn independently per task -- that is the half of the data a
-- look-alike model trains on, and the rest is what it has to score.
--
-- Determinism. The generator uses its own Park-Miller (MINSTD) generator
-- rather than math.random, whose sequence is per-process state that Tarantool
-- may seed or advance on its own; the same --seed therefore gives the same
-- numbers in any process. The Avro sync marker, otherwise 16 random bytes per
-- file, is derived from the parameters too, so two runs with the same
-- arguments produce byte-identical files. What is not guaranteed is
-- reproducibility across machines: the normals come out of libm's log and cos.
--
-- The hidden parameters in truth.json are quantised to 12 decimal places
-- before use, so that reading them back out of JSON -- whose encoder keeps 14
-- significant digits -- returns the very doubles the labels were computed
-- from, and a test can recompute the labels exactly.

local bit    = require('bit')
local fio    = require('fio')
local json   = require('json')
local digest = require('digest')

-- Run from anywhere: resolve this script's repository root and put it ahead of
-- whatever pregel may be installed system-wide.
local script_dir = fio.abspath(fio.dirname(arg[0]))
local root       = fio.dirname(script_dir)
package.path = string.format('%s/?.lua;%s/?/init.lua;%s', root, root,
                             package.path)

local avro = require('pregel.avro')

local USER_SCHEMA = {
    type = 'record', name = 'User',
    fields = {
        {name = 'vid',      type = 'string'},
        {name = 'features', type = {type = 'array', items = 'double'}},
    },
}

local LABEL_SCHEMA = {
    type = 'record', name = 'Label',
    fields = {
        {name = 'task',   type = 'string'},
        {name = 'vid',    type = 'string'},
        {name = 'target', type = 'int'   },
    },
}

local DEFAULTS = {
    users             = 1000,
    features          = 16,
    tasks             = 2,
    labelled_fraction = 0.3,
    noise             = 0.5,
    seed              = 42,
    codec             = 'null',
}

local function die(fmt, ...)
    io.stderr:write('gen-lookalike: ' .. string.format(fmt, ...) .. '\n')
    os.exit(1)
end

local function usage()
    io.stderr:write(
        'usage: tarantool tools/gen-lookalike.lua <out_dir> [--users N] ' ..
        '[--features D] [--tasks K] [--labelled-fraction f] [--noise s] ' ..
        '[--seed n] [--codec null|deflate|zstandard]\n')
    os.exit(2)
end

--------------------------------------------------------------------------------
-- Random numbers
--------------------------------------------------------------------------------

-- Park-Miller / MINSTD. The state stays in 1 .. M-1, so the uniforms are in
-- the open interval (0, 1) and log(u) below is always finite. Both factors of
-- the update fit a double exactly (48271 * 2^31 is well under 2^53), so the
-- modulus is exact arithmetic rather than a rounding.
local RNG_A = 48271
local RNG_M = 2147483647

-- MINSTD's first output is A * state / M, so a state near zero returns a
-- uniform near zero. Taking `seed + 1` as the state -- the obvious thing --
-- therefore makes the first draw 2.2e-5 * (seed + 1) for every seed a human
-- types, and the first parameter drawn from it a fixed extreme value that
-- merely creeps with the seed: the first weight below would be -0.9999 for
-- seed 1 and -0.9995 for seed 10, never a uniform on [-1, 1] at all.
--
-- So the seed is scrambled before it becomes a state (splitmix32's finalizer,
-- primed with the golden-ratio constant so that adjacent and doubled seeds do
-- not stay related through it), and the stream is then run forward
-- RNG_WARMUP times before anything reads it. Both halves are cheap and both
-- are needed: the mix breaks the seed's magnitude, the warm-up costs nothing
-- and covers whatever structure the mix leaves behind.
local RNG_WARMUP = 16
local RNG_GOLDEN = 0x9e3779b9

--- a * b over the 32-bit integers, in two 16-bit halves. A double holds the
--- product of two 32-bit numbers only up to 2^53, which 0xffffffff squared
--- exceeds, so the top half is dropped before it can round.
local function mul32(a, b)
    local ahi, alo = math.floor(a / 65536), a % 65536
    local bhi, blo = math.floor(b / 65536), b % 65536
    return (alo * blo + ((ahi * blo + alo * bhi) % 65536) * 65536) % 4294967296
end

--- x ^ (x >> n), unsigned.
local function xorshift(x, n)
    return bit.bxor(bit.tobit(x), bit.tobit(math.floor(x / 2 ^ n)))
        % 4294967296
end

local function mix32(x)
    x = (x + RNG_GOLDEN) % 4294967296
    x = xorshift(x, 16)
    x = mul32(x, 0x21f0aaad)
    x = xorshift(x, 15)
    x = mul32(x, 0xd35a2d97)
    return xorshift(x, 15)
end

local function rng_new(seed)
    local s = math.floor(seed) % 4294967296
    if s < 0 then
        s = s + 4294967296
    end
    local r = {state = mix32(s) % (RNG_M - 1) + 1}
    for _ = 1, RNG_WARMUP do
        r.state = (RNG_A * r.state) % RNG_M
    end
    return r
end

local function uniform(r)
    r.state = (RNG_A * r.state) % RNG_M
    return r.state / RNG_M
end

--- Uniform on [-1, 1].
local function signed_uniform(r)
    return 2 * uniform(r) - 1
end

--- One standard normal, by Box-Muller. The second of the pair is discarded
--- rather than cached, which costs two uniforms per normal and buys a stream
--- whose position does not depend on how many normals were asked for before.
local function normal(r)
    local u1, u2 = uniform(r), uniform(r)
    return math.sqrt(-2 * math.log(u1)) * math.cos(2 * math.pi * u2)
end

--- Round to 12 decimal places -- see the note about truth.json above.
local function q12(x)
    return math.floor(x * 1e12 + 0.5) / 1e12
end

--- `m` distinct indices out of 1..n, ascending. A partial Fisher-Yates: only
--- the prefix that is taken gets shuffled.
local function sample_indices(r, n, m)
    local idx = {}
    for i = 1, n do
        idx[i] = i
    end
    for i = 1, m do
        local j = i + math.floor(uniform(r) * (n - i + 1))
        if j > n then
            j = n
        end
        idx[i], idx[j] = idx[j], idx[i]
    end
    local out = {}
    for i = 1, m do
        out[i] = idx[i]
    end
    table.sort(out)
    return out
end

--------------------------------------------------------------------------------
-- Arguments
--------------------------------------------------------------------------------

local function to_int(name, value)
    local n = tonumber(value)
    if n == nil or n ~= math.floor(n) then
        die('%s needs an integer, got %q', name, tostring(value))
    end
    return n
end

local function to_number(name, value)
    local n = tonumber(value)
    if n == nil then
        die('%s needs a number, got %q', name, tostring(value))
    end
    return n
end

local OPTIONS = {
    ['--users']             = {'users',             to_int   },
    ['--features']          = {'features',          to_int   },
    ['--tasks']             = {'tasks',             to_int   },
    ['--labelled-fraction'] = {'labelled_fraction', to_number},
    ['--noise']             = {'noise',             to_number},
    ['--seed']              = {'seed',              to_int   },
    ['--codec']             = {'codec',             nil      },
}

local function parse_args(argv)
    local opts = {}
    for k, v in pairs(DEFAULTS) do
        opts[k] = v
    end
    local positional = {}
    local i = 1
    while argv[i] ~= nil do
        local a = argv[i]
        local name, value = a, nil
        local eq = a:find('=', 1, true)
        if eq ~= nil and a:sub(1, 2) == '--' then
            name, value = a:sub(1, eq - 1), a:sub(eq + 1)
        end
        local spec = OPTIONS[name]
        if a == '--help' or a == '-h' then
            usage()
        elseif spec ~= nil then
            if value == nil then
                value = argv[i + 1]
                i = i + 2
            else
                i = i + 1
            end
            if value == nil then
                die('%s needs a value', name)
            end
            opts[spec[1]] = spec[2] ~= nil and spec[2](name, value) or value
        elseif a:sub(1, 1) == '-' then
            die('unknown option %q', a)
        else
            table.insert(positional, a)
            i = i + 1
        end
    end
    if #positional ~= 1 then
        usage()
    end
    if opts.users < 1 then
        die('--users must be at least 1')
    end
    if opts.features < 1 then
        die('--features must be at least 1')
    end
    if opts.tasks < 1 then
        die('--tasks must be at least 1')
    end
    if opts.labelled_fraction < 0 or opts.labelled_fraction > 1 then
        die('--labelled-fraction must be in [0, 1]')
    end
    if opts.noise < 0 then
        die('--noise must not be negative')
    end
    if not avro.ocf.codec_available(opts.codec) then
        die('codec %q is not available in this Tarantool build', opts.codec)
    end
    return positional[1], opts
end

local out_dir, opts = parse_args(arg)

if not fio.mktree(out_dir) then
    die("cannot create '%s'", out_dir)
end

--------------------------------------------------------------------------------
-- Generate
--------------------------------------------------------------------------------

-- Everything about the dataset goes into the sync marker, so that two datasets
-- that differ in any parameter differ in their markers as well.
local signature = string.format(
    'pregel/gen-lookalike:users=%d:features=%d:tasks=%d:' ..
    'labelled=%.17g:noise=%.17g:seed=%d',
    opts.users, opts.features, opts.tasks, opts.labelled_fraction,
    opts.noise, opts.seed)

local function sync_for(name)
    return digest.md5(signature .. ':' .. name)
end

local rng = rng_new(opts.seed)

local task_names = {}
local weights    = {}
for k = 1, opts.tasks do
    task_names[k] = 'task' .. k
    local w = {}
    for j = 1, opts.features + 1 do
        w[j] = q12(signed_uniform(rng))
    end
    weights[k] = w
end

local users_path  = fio.pathjoin(out_dir, 'users.avro')
local labels_path = fio.pathjoin(out_dir, 'labels.avro')
local truth_path  = fio.pathjoin(out_dir, 'truth.json')

local uw = avro.ocf.open(users_path, {
    mode = 'w', schema = USER_SCHEMA, codec = opts.codec,
    sync = sync_for('users'),
})

-- targets[k][i] is the label task k gives user i, computed for every user;
-- which of them reach labels.avro is decided afterwards.
local targets = {}
for k = 1, opts.tasks do
    targets[k] = {}
end

for i = 1, opts.users do
    local x = {}
    for j = 1, opts.features do
        x[j] = normal(rng)
    end
    uw:append{vid = 'u' .. i, features = x}
    for k = 1, opts.tasks do
        local w = weights[k]
        local score = w[1]
        for j = 1, opts.features do
            score = score + w[j + 1] * x[j]
        end
        -- The noise draw happens whatever `noise` is, so that a dataset's
        -- features and scores do not move when only the noise changes.
        score = score + opts.noise * normal(rng)
        targets[k][i] = score > 0 and 1 or -1
    end
end

uw:close()

local per_task = math.floor(opts.users * opts.labelled_fraction + 0.5)

local lw = avro.ocf.open(labels_path, {
    mode = 'w', schema = LABEL_SCHEMA, codec = opts.codec,
    sync = sync_for('labels'),
})

local n_positive = 0
for k = 1, opts.tasks do
    for _, i in ipairs(sample_indices(rng, opts.users, per_task)) do
        local target = targets[k][i]
        if target > 0 then
            n_positive = n_positive + 1
        end
        lw:append{task = task_names[k], vid = 'u' .. i, target = target}
    end
end
lw:close()

local truth = {
    tasks             = {},
    users             = opts.users,
    features          = opts.features,
    labelled_fraction = opts.labelled_fraction,
    seed              = opts.seed,
    noise             = opts.noise,
}
for k = 1, opts.tasks do
    truth.tasks[task_names[k]] = {weights = weights[k]}
end

local th = assert(io.open(truth_path, 'w'), 'cannot write ' .. truth_path)
th:write(json.encode(truth), '\n')
th:close()

local n_labels = opts.tasks * per_task
print(string.format('%s: %d users, %d features', users_path, opts.users,
                    opts.features))
print(string.format('%s: %d labels (%d tasks x %d), %d positive', labels_path,
                    n_labels, opts.tasks, per_task, n_positive))
print(string.format('%s: %d hidden weight vectors of %d', truth_path,
                    opts.tasks, opts.features + 1))
print(string.format('seed: %d, noise: %s, codec: %s', opts.seed,
                    tostring(opts.noise), opts.codec))

os.exit(0)
