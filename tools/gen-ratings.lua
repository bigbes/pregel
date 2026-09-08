#!/usr/bin/env tarantool
--- Generate a synthetic ratings dataset from hidden low-rank factors, split
--- into a train and a test half.
--
--     tarantool tools/gen-ratings.lua <out_dir> [--users U] [--items I]
--         [--rank r] [--density d] [--noise s] [--seed n]
--         [--test-fraction f] [--codec null|deflate|zstandard]
--
-- Defaults: 200 users, 100 items, rank 3, density 0.1, noise 0.2, seed 42,
-- test fraction 0.2, codec null.
--
-- Three files land in <out_dir>:
--
--     train.avro  record Rating { string user; string item; double rating; }
--     test.avro   the same record
--     truth.json  the hidden parameters and the counts
--
-- The model. There is a global mean mu = 3.5, a bias per user and per item
-- drawn from N(0, 0.3), and a factor vector of `rank` entries per user and per
-- item drawn from N(0, 0.5). Each of the U * I pairs is rated with probability
-- `density`, and a rating is
--
--     clip(mu + b_u + b_i + p_u . q_i + noise * N(0,1), 1, 5)
--
-- rounded to the nearest half point, which is the shape of the ratings a real
-- catalogue collects: bounded, coarse, and biased per user and per item on top
-- of whatever taste the factors carry.
--
-- The split. Each rating goes to test.avro with probability `test-fraction`,
-- as an exact count rather than a coin per rating. A user or an item that the
-- split would leave in the test half with nothing in the train half is not a
-- test case but a cold start, which factorisation cannot answer at all -- so
-- those ratings are *moved* back into train.avro rather than re-drawn, which
-- keeps every other rating where the split put it. truth.json counts how many
-- moved.
--
-- Determinism. As tools/gen-lookalike.lua: an own Park-Miller (MINSTD) stream
-- rather than math.random, and an Avro sync marker derived from the parameters
-- rather than 16 random bytes, so the same arguments give byte-identical
-- files. Reproducibility across machines is not promised -- the normals come
-- out of libm's log and cos.
--
-- The hidden parameters in truth.json are quantised to 12 decimal places
-- before use, so that reading them back out of JSON -- whose encoder keeps 14
-- significant digits -- returns the very doubles the ratings were computed
-- from, and a test can recompute a rating exactly rather than approximately.

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

local RATING_SCHEMA = {
    type = 'record', name = 'Rating',
    fields = {
        {name = 'user',   type = 'string'},
        {name = 'item',   type = 'string'},
        {name = 'rating', type = 'double'},
    },
}

local MU        = 3.5
local BIAS_SD   = 0.3
local FACTOR_SD = 0.5
local MIN_RATING, MAX_RATING = 1, 5

local DEFAULTS = {
    users         = 200,
    items         = 100,
    rank          = 3,
    density       = 0.1,
    noise         = 0.2,
    seed          = 42,
    test_fraction = 0.2,
    codec         = 'null',
}

local function die(fmt, ...)
    io.stderr:write('gen-ratings: ' .. string.format(fmt, ...) .. '\n')
    os.exit(1)
end

local function usage()
    io.stderr:write(
        'usage: tarantool tools/gen-ratings.lua <out_dir> [--users U] ' ..
        '[--items I] [--rank r] [--density d] [--noise s] [--seed n] ' ..
        '[--test-fraction f] [--codec null|deflate|zstandard]\n')
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
-- types, and Box-Muller turns a first uniform that small into
-- sqrt(-2 ln u) > 4: u1's bias below would be a four-sigma draw for seed 1
-- and for seed 10 alike, never the N(0, 0.3) the header promises.
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
    ['--users']         = {'users',         to_int   },
    ['--items']         = {'items',         to_int   },
    ['--rank']          = {'rank',          to_int   },
    ['--density']       = {'density',       to_number},
    ['--noise']         = {'noise',         to_number},
    ['--seed']          = {'seed',          to_int   },
    ['--test-fraction'] = {'test_fraction', to_number},
    ['--codec']         = {'codec',         nil      },
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
    if opts.items < 1 then
        die('--items must be at least 1')
    end
    if opts.rank < 1 then
        die('--rank must be at least 1')
    end
    if opts.density <= 0 or opts.density > 1 then
        die('--density must be in (0, 1]')
    end
    if opts.noise < 0 then
        die('--noise must not be negative')
    end
    if opts.test_fraction < 0 or opts.test_fraction > 1 then
        die('--test-fraction must be in [0, 1]')
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
    'pregel/gen-ratings:users=%d:items=%d:rank=%d:density=%.17g:' ..
    'noise=%.17g:seed=%d:test=%.17g',
    opts.users, opts.items, opts.rank, opts.density, opts.noise, opts.seed,
    opts.test_fraction)

local function sync_for(name)
    return digest.md5(signature .. ':' .. name)
end

local rng = rng_new(opts.seed)

local function biases(n, scale)
    local out = {}
    for i = 1, n do
        out[i] = q12(scale * normal(rng))
    end
    return out
end

local function factors(n, rank, scale)
    local out = {}
    for i = 1, n do
        local v = {}
        for j = 1, rank do
            v[j] = q12(scale * normal(rng))
        end
        out[i] = v
    end
    return out
end

local user_bias = biases(opts.users, BIAS_SD)
local item_bias = biases(opts.items, BIAS_SD)
local user_vec  = factors(opts.users, opts.rank, FACTOR_SD)
local item_vec  = factors(opts.items, opts.rank, FACTOR_SD)

--- The rating a (user, item) pair would get, before the noise.
local function predict(u, i)
    local dot = 0
    local p, q = user_vec[u], item_vec[i]
    for j = 1, opts.rank do
        dot = dot + p[j] * q[j]
    end
    return MU + user_bias[u] + item_bias[i] + dot
end

--- Clip to [1, 5] and round to the nearest half point, in that order: the
--- bounds are multiples of a half, so rounding cannot push a value back out.
local function to_star(x)
    if x < MIN_RATING then
        x = MIN_RATING
    elseif x > MAX_RATING then
        x = MAX_RATING
    end
    return math.floor(x * 2 + 0.5) / 2
end

-- Ratings in user-major order, which is the order the split and both files
-- then keep.
local ratings = {}
for u = 1, opts.users do
    for i = 1, opts.items do
        if uniform(rng) < opts.density then
            -- The noise draw happens whatever `noise` is, so that a dataset's
            -- ratings do not move to other pairs when only the noise changes.
            local z = normal(rng)
            ratings[#ratings + 1] = {
                user   = 'u' .. u,
                item   = 'i' .. i,
                rating = to_star(predict(u, i) + opts.noise * z),
            }
        end
    end
end

local n_ratings = #ratings

--------------------------------------------------------------------------------
-- Split
--------------------------------------------------------------------------------

local in_test = {}
for _, idx in ipairs(sample_indices(rng, n_ratings,
                                    math.floor(n_ratings * opts.test_fraction
                                               + 0.5))) do
    in_test[idx] = true
end

-- How much of each user and item the train half holds, so that a test rating
-- whose user or item would otherwise never be seen in training can be spotted
-- and moved back.
local train_users, train_items = {}, {}
for idx = 1, n_ratings do
    if not in_test[idx] then
        local r = ratings[idx]
        train_users[r.user] = (train_users[r.user] or 0) + 1
        train_items[r.item] = (train_items[r.item] or 0) + 1
    end
end

local n_moved = 0
for idx = 1, n_ratings do
    if in_test[idx] then
        local r = ratings[idx]
        if train_users[r.user] == nil or train_items[r.item] == nil then
            in_test[idx] = nil
            train_users[r.user] = (train_users[r.user] or 0) + 1
            train_items[r.item] = (train_items[r.item] or 0) + 1
            n_moved = n_moved + 1
        end
    end
end

--------------------------------------------------------------------------------
-- Write
--------------------------------------------------------------------------------

local train_path = fio.pathjoin(out_dir, 'train.avro')
local test_path  = fio.pathjoin(out_dir, 'test.avro')
local truth_path = fio.pathjoin(out_dir, 'truth.json')

local tw = avro.ocf.open(train_path, {
    mode = 'w', schema = RATING_SCHEMA, codec = opts.codec,
    sync = sync_for('train'),
})
local ew = avro.ocf.open(test_path, {
    mode = 'w', schema = RATING_SCHEMA, codec = opts.codec,
    sync = sync_for('test'),
})

local n_train, n_test = 0, 0
for idx = 1, n_ratings do
    if in_test[idx] then
        ew:append(ratings[idx])
        n_test = n_test + 1
    else
        tw:append(ratings[idx])
        n_train = n_train + 1
    end
end

tw:close()
ew:close()

local truth = {
    mu            = MU,
    users         = opts.users,
    items         = opts.items,
    rank          = opts.rank,
    density       = opts.density,
    noise         = opts.noise,
    seed          = opts.seed,
    test_fraction = opts.test_fraction,
    biases        = {users = {}, items = {}},
    factors       = {users = {}, items = {}},
    counts        = {
        ratings        = n_ratings,
        train          = n_train,
        test           = n_test,
        moved_to_train = n_moved,
    },
}
for u = 1, opts.users do
    truth.biases.users['u' .. u]  = user_bias[u]
    truth.factors.users['u' .. u] = user_vec[u]
end
for i = 1, opts.items do
    truth.biases.items['i' .. i]  = item_bias[i]
    truth.factors.items['i' .. i] = item_vec[i]
end

local th = assert(io.open(truth_path, 'w'), 'cannot write ' .. truth_path)
th:write(json.encode(truth), '\n')
th:close()

print(string.format('%s: %d ratings', train_path, n_train))
print(string.format('%s: %d ratings (%d moved back to train for a user or ' ..
                    'item train.avro would not have held)', test_path, n_test,
                    n_moved))
print(string.format('%s: mu %s, %d user and %d item factors of rank %d',
                    truth_path, tostring(MU), opts.users, opts.items,
                    opts.rank))
print(string.format('density: %d of %d pairs (%.4f), seed: %d, noise: %s, ' ..
                    'codec: %s', n_ratings, opts.users * opts.items,
                    n_ratings / (opts.users * opts.items), opts.seed,
                    tostring(opts.noise), opts.codec))

os.exit(0)
