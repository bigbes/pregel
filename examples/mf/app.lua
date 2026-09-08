--- mf: a matrix-factorisation recommender, trained by SGD over the graph.
--
-- The model is `pregel.math.mf`'s: a rating is
--
--     r_ui ~ mu + b_u + b_i + <p_u, q_i>
--
-- a global mean, a per-user and a per-item offset, and the dot product of two
-- latent vectors. Every one of those parameters except mu is fitted, and every
-- one of them belongs to exactly one vertex.
--
-- ## The graph
--
-- Bipartite, and undirected in the only sense Pregel understands: every
-- training rating is stored as *two* edges, `u:<user> -> i:<item>` and
-- `i:<item> -> u:<user>`, both carrying the rating as the edge value. Pregel
-- only ever walks out-edges, so a rating that existed in one direction only
-- would let the user learn from the item and never the other way round -- the
-- same point examples/wcc makes about symmetry, here forced on us by the
-- algorithm rather than by the question.
--
-- A vertex value is
--
--     {name = 'u:7' | 'i:3', kind = 'user' | 'item', p = {...}, b = <number>}
--
-- `p` is the latent vector -- `p_u` on a user and `q_i` on an item; one name,
-- because the two sides are the same shape and the same arithmetic -- and `b`
-- is that vertex's offset. `rank` entries in `p`, from app_cfg.
--
-- ## The schedule
--
--   superstep 1        every vertex draws its own p, sets b = 0, and sends
--                      {p, b} to every neighbour. mu is not knowable yet.
--   superstep k > 1    epoch k-1: read one {p, b} per neighbour, take one SGD
--                      step per (neighbour, rating) pair, send the updated
--                      {p, b} on. After `epochs` of these, halt.
--
-- So the job runs `epochs + 1` supersteps and stops by counting them, the way
-- examples/pagerank does: SGD converges rather than terminates.
--
-- ## Each vertex updates its own half only
--
-- `mf.sgd_step` computes both new vectors from one rating, and this app throws
-- one of them away: a user vertex keeps the new `p` and lets the item make its
-- own move from its own copy of the same message. That is deliberate, and it
-- is the standard Pregel/parameter-server approximation of SGD:
--
--   * it keeps every parameter owned by exactly one vertex, so nothing is
--     written from two places and no conflict has to be resolved;
--   * both sides step from the *same* pair of vectors -- the ones exchanged at
--     the end of the previous superstep -- so the pair of half-updates is one
--     joint gradient step evaluated at that point, not two chained ones.
--
-- What it is *not* is sequential SGD: within one epoch a vertex sees its
-- neighbours as they were an epoch ago, so this is closer to a mini-batch
-- step per vertex than to Koren's per-rating loop. It converges the same way
-- and a good deal more slowly per epoch, which is the price of running the
-- ratings in parallel rather than one at a time.
--
-- Within one superstep a vertex *does* chain its own updates: the second
-- message it reads steps from the p its first message produced. That is one
-- writer moving through its own ratings, not two writers racing.
--
-- ## What is global
--
-- Only mu, and it reaches the vertices through aggregators, since no vertex
-- can see more than its own neighbourhood:
--
--   rating_sum, rating_count -- summed over *user* vertices only. Both sides
--     of every rating are in the graph, so counting on both would count each
--     rating twice; mu would still come out right, and `rating_count` -- which
--     the tests use as the number of training ratings -- would not.
--
-- Both are contributed on every superstep rather than once, because the master
-- resets an aggregator before the workers report into it: a value contributed
-- in superstep 1 alone is readable in superstep 2 and gone by superstep 3.
-- The same reason examples/pagerank re-votes for `count` every time.
--
-- The third aggregator, `train_sse`, goes the other way: it carries an answer
-- *out* rather than a fact in. See `history` below.
--
-- Configured through roles_cfg.app_cfg:
--
--   train   -- the training ratings, an Avro OCF of
--              `record Rating {string user; string item; double rating;}`,
--              relative to this directory (required)
--   test    -- held-out ratings in the same schema. Not read by the job at
--              all: it is here so that examples/mf/evaluate.lua and the README
--              can name one path, and so the config test notices when it moves
--   rank    -- latent factors per vertex (default 3)
--   epochs  -- SGD passes over the ratings (default 30)
--   lr      -- learning rate of the first epoch (default 0.05)
--   decay   -- the learning rate is multiplied by this per epoch (default 0.98)
--   lambda  -- L2 coefficient on the vectors and the biases (default 0.05)
--
-- @module examples.mf.app

local log = require('log')

local loader = require('pregel.loader')
local mf     = require('pregel.math.mf')
local ocf    = require('pregel.avro.ocf')

local common = require('examples.common')

local HERE = common.here()

local DEFAULT_RANK   = 3
local DEFAULT_EPOCHS = 30
local DEFAULT_LR     = 0.05
local DEFAULT_DECAY  = 0.98
local DEFAULT_LAMBDA = 0.05

local USER_PREFIX = 'u:'
local ITEM_PREFIX = 'i:'

local app = {}

-------------------------------------------------------------------------------
-- Deterministic starting vectors
-------------------------------------------------------------------------------

-- Latent vectors cannot start at zero -- p and q enter each other's gradient,
-- so a pair that starts there never moves -- and they must not start equal
-- either, or every user is the same user. So each vertex draws its own, and
-- draws it from its *name*: the alternative is math.random, whose stream
-- depends on how many vertices happen to share a worker and in what order that
-- worker walked its space. Seeding from the name makes a run reproducible
-- across shard counts, which is what lets a test assert a number at all.

--- djb2 over the vertex name, folded into Park-Miller's range.
local function seed_of(name)
    local h = 5381
    for i = 1, #name do
        h = (h * 33 + name:byte(i)) % 2147483647
    end
    -- Zero is the one state a multiplicative generator cannot leave.
    return h == 0 and 1 or h
end

--- A Park-Miller (MINSTD) stream, as tools/gen-ratings.lua uses.
--
-- 16807 * s stays under 2^53 for every s in range, so the doubles are exact.
local function rng_from(seed)
    local state = seed
    return function()
        state = (state * 16807) % 2147483647
        return state / 2147483647
    end
end

-------------------------------------------------------------------------------
-- Aggregators
-------------------------------------------------------------------------------

--- The train SSE of every epoch, keyed by the superstep that produced it.
--
-- The master keeps one value per aggregator and resets it between supersteps,
-- so once a job is 'done' the twenty-nine epochs before the last one are gone.
-- `merge` is the only hook that runs on the master -- once per worker per
-- superstep -- which makes it the only place a history can be built.
--
-- That is why `train_sse` is a table and not a number: `merge` is handed two
-- accumulators and nothing else, so the superstep an accumulator belongs to
-- has to travel inside it.
--
-- Empty on a worker, which never calls `merge`.
--
-- It belongs to a *run*, and an upvalue does not end when one does: a master
-- instance outlives the job it ran, so a second run -- a restart, or another
-- job through `pregel.roles.master` on that instance -- used to read back the
-- previous run's epochs alongside its own, with nothing to tell them apart.
-- `history_high` is what makes the boundary visible; see merge_sse.
local history = {}
local history_high = 0

local function reset_history()
    history = {}
    history_high = 0
end

--- Sum two {sse, n, superstep} accumulators.
--
-- The superstep is the larger of the two rather than the newer: a worker whose
-- shard holds no user vertex contributes nothing all superstep and reports the
-- default, whose superstep is 0, and merging that must not erase the real one.
local function add_sse(old, new)
    return {
        sse       = old.sse + new.sse,
        n         = old.n + new.n,
        superstep = old.superstep > new.superstep and old.superstep
                                                   or new.superstep,
    }
end

--- add_sse, plus the record on the master.
--
-- Overwritten once per worker within a superstep; the last write is the one
-- with every worker in it, and it is the one a reader sees.
local function merge_sse(old, new)
    local rv = add_sse(old, new)
    if rv.superstep > 0 then
        -- A superstep *below* the highest already recorded is a new run on a
        -- master still holding the last one's epochs, and this is the only
        -- place that can see it: supersteps only rise within a run, and the
        -- several merges of one superstep report it unchanged rather than
        -- higher -- so `<` rather than `<=`, or every worker after the first
        -- would wipe the run so far.
        if rv.superstep < history_high then
            reset_history()
        end
        history[rv.superstep] = {sse = rv.sse, n = rv.n}
        if rv.superstep > history_high then
            history_high = rv.superstep
        end
    end
    return rv
end

local function add(old, new)
    return old + new
end

app.aggregators = {
    rating_sum   = {default = 0, reduce = add, merge = add},
    rating_count = {default = 0, reduce = add, merge = add},
    train_sse    = {
        default = {sse = 0.0, n = 0, superstep = 0},
        reduce  = add_sse,
        merge   = merge_sse,
    },
}

--- Train RMSE per epoch, as the master recorded it.
--
-- An array of `{epoch, superstep, rmse, count}`, oldest first, where `count`
-- is how many ratings went into that epoch's error -- one per training rating
-- when everything is working, which is what makes it worth returning.
--
-- Only meaningful on the master, and only after a superstep has ended there.
--
-- @return array, possibly empty
-- @function train_history
function app.train_history()
    local supersteps = {}
    for superstep in pairs(history) do
        table.insert(supersteps, superstep)
    end
    table.sort(supersteps)

    local rv = {}
    for _, superstep in ipairs(supersteps) do
        local entry = history[superstep]
        table.insert(rv, {
            epoch     = superstep - 1,
            superstep = superstep,
            count     = entry.n,
            rmse      = entry.n > 0 and math.sqrt(entry.sse / entry.n) or nil,
        })
    end
    return rv
end

-------------------------------------------------------------------------------
-- The Pregel job
-------------------------------------------------------------------------------

function app.obtain_name(vertex)
    return vertex.name
end

function app.worker_context(app_cfg)
    local cfg = common.cfg(app_cfg)
    return {
        rank   = cfg.rank   or DEFAULT_RANK,
        epochs = cfg.epochs or DEFAULT_EPOCHS,
        lr     = cfg.lr     or DEFAULT_LR,
        decay  = cfg.decay  or DEFAULT_DECAY,
        lambda = cfg.lambda or DEFAULT_LAMBDA,
    }
end

function app.compute(self)
    local context   = self:get_worker_context()
    local superstep = self:get_superstep()
    local value     = self:get_value()
    local name, kind = value.name, value.kind

    -- The rating of every neighbour, rebuilt each superstep. A message carries
    -- the neighbour's half of the model and not the rating that ties the two
    -- together: the rating is on the edge, which is the only place it needs to
    -- be stored.
    local rating_of = {}
    local rating_sum, rating_count = 0.0, 0
    for _, neighbour, rating in self:pairs_edges() do
        rating_of[neighbour] = rating
        rating_sum   = rating_sum + rating
        rating_count = rating_count + 1
    end

    if kind == 'user' then
        -- User side only: the item side would count every rating a second
        -- time. See the header.
        self:set_aggregation('rating_sum', rating_sum)
        self:set_aggregation('rating_count', rating_count)
    end

    if superstep == 1 then
        -- Nothing global is knowable yet -- mu is being counted right now --
        -- so this superstep exists to draw the starting vectors and put one in
        -- every neighbour's inbox.
        local p = mf.init_vector(context.rank, rng_from(seed_of(name)))
        self:set_value({name = name, kind = kind, p = p, b = 0.0})
        for _, neighbour in self:pairs_edges() do
            self:send_message(neighbour, {name = name, p = p, b = 0.0})
        end
        self:vote_halt(false)
        return
    end

    local epoch = superstep - 1
    local n = self:get_aggregation('rating_count')
    -- A graph with no ratings has no mean; it also has no vertices, so this
    -- guard is for the empty case rather than for a real one.
    local mu = n > 0 and self:get_aggregation('rating_sum') / n or 0.0
    -- Decayed per epoch, so the last passes settle rather than bounce.
    local lr = context.lr * context.decay ^ (epoch - 1)

    local p, b = value.p, value.b
    local sse, seen = 0.0, 0
    for _, message in self:pairs_messages() do
        local rating = rating_of[message.name]
        if rating == nil then
            -- Every message comes down an edge, and every edge of this graph
            -- has a rating on it. Reaching here means the graph was built
            -- wrong -- one direction of an edge missing, or an edge with no
            -- value -- and guessing a rating would hide that in the numbers.
            error(string.format(
                "%s got a message from %s, which is not one of its neighbours",
                name, tostring(message.name)))
        end
        -- `rating - mu` because sgd_step never sees mu: keeping the global
        -- mean out of the fitted parameters is what stops b_u and b_i from
        -- each having to represent it.
        --
        -- The second and fourth returns are the neighbour's half of the step,
        -- and they are dropped: the neighbour makes that move itself, from its
        -- own copy of this same pair of vectors.
        local new_p, _, new_b, _, err =
            mf.sgd_step(p, message.p, b, message.b, rating - mu, lr,
                        context.lambda)
        p, b = new_p, new_b
        if kind == 'user' then
            -- `err` is the residual before this step, so the sum over an epoch
            -- is the error of the model the epoch started with. Accumulated on
            -- the user side alone, for the same reason as rating_count.
            sse  = sse + err * err
            seen = seen + 1
        end
    end
    self:set_value({name = name, kind = kind, p = p, b = b})

    if kind == 'user' then
        self:set_aggregation('train_sse',
                             {sse = sse, n = seen, superstep = superstep})
    end

    if epoch < context.epochs then
        for _, neighbour in self:pairs_edges() do
            self:send_message(neighbour, {name = name, p = p, b = b})
        end
        -- Nothing here converges on its own, so a vertex stays awake until the
        -- epoch count says otherwise.
        self:vote_halt(false)
    else
        self:vote_halt(true)
    end
end

-------------------------------------------------------------------------------
-- Loading
-------------------------------------------------------------------------------

--- Read train.avro and push the bipartite graph out.
--
-- `pregel.loader.avro_files` is no use here: it wants a vertex file and an edge
-- file, and a ratings dataset is one file that is *both* -- every record names
-- two vertices and one edge, and the edge has to exist in both directions.
--
-- Two passes over the file rather than one, so what is held in memory is the
-- set of vertex names (one per user and per item) and not the ratings. The
-- edges go out one message per direction instead of in per-source batches:
-- ratings are grouped by user in the file, so the reverse direction is not
-- contiguous under any ordering, and mpool batches the messages anyway.
function app.master_preload(instance, app_cfg)
    local cfg  = common.cfg(app_cfg, {'train'})
    local path = common.resolve(HERE, cfg.train, 'train')

    return loader.new(instance, function(self)
        -- The other half of scoping the history to a run, and the one that
        -- covers a job loaded and then not run: merge_sse can only notice a
        -- new run once that run has produced an epoch, so without this a
        -- master between `preload()` and the first superstep still answers
        -- with the previous run's history.
        reset_history()
        log.info('mf: loading ratings from "%s"', path)

        local users, items = 0, 0
        local seen = {}
        local function declare(name, kind)
            if seen[name] then
                return
            end
            seen[name] = true
            self:store_vertex({name = name, kind = kind})
            if kind == 'user' then
                users = users + 1
            else
                items = items + 1
            end
        end

        local reader = ocf.open(path, {mode = 'r'})
        local ok, err = pcall(function()
            for record in reader:records() do
                declare(USER_PREFIX .. record.user, 'user')
                declare(ITEM_PREFIX .. record.item, 'item')
            end
        end)
        reader:close()
        if not ok then
            error(err, 0)
        end

        local ratings = 0
        reader = ocf.open(path, {mode = 'r'})
        ok, err = pcall(function()
            for record in reader:records() do
                local user = USER_PREFIX .. record.user
                local item = ITEM_PREFIX .. record.item
                self:store_edge(user, item, record.rating)
                self:store_edge(item, user, record.rating)
                ratings = ratings + 1
            end
        end)
        reader:close()
        if not ok then
            error(err, 0)
        end

        log.info('mf: %d ratings over %d users and %d items', ratings, users,
                 items)
        self:flush()
        return users + items + 2 * ratings
    end)
end

app.USER_PREFIX = USER_PREFIX
app.ITEM_PREFIX = ITEM_PREFIX

return app
