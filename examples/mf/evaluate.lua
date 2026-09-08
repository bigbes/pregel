--- Score the trained model against held-out ratings.
--
-- Not part of the Pregel job: the job fits the model, this reads it back out
-- and answers what it is worth. Run it on the **master**, from the test suite
-- or from a console:
--
--     require('examples.mf.evaluate').evaluate{
--         test = '../../test/fixtures/ratings/test.avro'
--     }
--
-- ## Why the master, when the master owns no graph
--
-- The vertices are on the workers, in each one's `data_<job>` space, and the
-- master already holds a net.box connection to every one of them -- that is
-- what its message pool is. Those connections are what this walks, so nothing
-- new has to be granted and no second address list has to be kept in step with
-- `roles_cfg.workers`.
--
-- The worker role grants the `roles_cfg.user` read and write on the spaces it
-- creates, so `conn.space['data_<job>']:select()` works over exactly the
-- credentials the job already runs on. There is no extra `lua_call` here on
-- purpose: an evaluation that needed one would make the four names in the
-- credentials section five, in every config that ever runs this app.
--
-- ## What it computes
--
--     rmse over the test ratings of  mu + b_u + b_i + <p_u, q_i>
--
-- with `mu` from the master's own `rating_sum` / `rating_count` aggregators --
-- the mean of the *training* ratings, which is the only mean the model was
-- fitted around. Passing `opts.mu` overrides it.
--
-- A test rating naming a user or an item that is not in the graph is a cold
-- start, which factorisation cannot answer at all; it is counted in `missing`
-- and left out of the RMSE rather than scored against mu, so a split that
-- quietly stopped guaranteeing coverage shows up as a number instead of as a
-- slightly better score. tools/gen-ratings.lua guarantees there are none.
--
-- @module examples.mf.evaluate

local fio = require('fio')

local mf  = require('pregel.math.mf')
local ocf = require('pregel.avro.ocf')

local common = require('examples.common')

local HERE = common.here()

-- Tuples per select. The whole model would fit in one call for any graph an
-- example runs on; paging is what keeps that from being an assumption.
local PAGE = 1000

local M = {}

--- Fold one instance's `data_<job>` space into `model`.
--
-- `space` is either a net.box space (a worker reached from the master) or a
-- local `box.space` entry, which have the same select() here.
local function collect_space(space, model)
    local last = nil
    while true do
        local tuples
        if last == nil then
            tuples = space:select({}, {limit = PAGE})
        else
            tuples = space:select({last}, {iterator = 'GT', limit = PAGE})
        end
        if #tuples == 0 then
            return
        end
        for _, tuple in ipairs(tuples) do
            local value = tuple[3]
            -- A vertex the job never computed has no vector; that is a broken
            -- run rather than a vertex to skip, so let it through and let the
            -- prediction complain.
            model[value.name] = {p = value.p, b = value.b, kind = value.kind}
            last = tuple[1]
        end
        if #tuples < PAGE then
            return
        end
    end
end

--- Every vertex of `job`, gathered from the workers of `instance`.
--
-- @param instance the pregel master object (pregel.roles.master.get())
-- @param job the job name, default instance.name
-- @return `{['u:7'] = {p = {...}, b = <number>, kind = 'user'}, ...}`
-- @raise when a worker has no space for this job, which means the job never
--  ran there
-- @function collect
function M.collect(instance, job)
    job = job or instance.name
    local name = 'data_' .. job

    local model = {}
    for _, bucket in ipairs(instance.mpool.buckets) do
        local conn = bucket.connection
        if conn ~= nil then
            conn:wait_connected()
            local space = conn.space[name]
            if space == nil then
                error(string.format(
                    "%s holds no space '%s': the job did not run there",
                    tostring(bucket.uri), name), 0)
            end
            collect_space(space, model)
        elseif box.space[name] ~= nil then
            -- The pool closes the connection to this very instance once it
            -- recognises itself, so a master that also carries the worker role
            -- reaches its own shard through box rather than over a loopback.
            collect_space(box.space[name], model)
        else
            error(string.format(
                "worker '%s' is not reachable and is not this instance",
                tostring(bucket.uri)), 0)
        end
    end
    return model
end

--- RMSE of the model over `records`.
--
-- @param model as collect() returns
-- @param mu the global mean the model was fitted around
-- @param records array of `{user = ..., item = ..., rating = ...}`
-- @return the RMSE, how many ratings it covers, and how many were skipped
--  because the model had never seen the user or the item
-- @function rmse
function M.rmse(model, mu, records)
    local samples, missing = {}, 0
    for _, record in ipairs(records) do
        local user = model['u:' .. record.user]
        local item = model['i:' .. record.item]
        if user == nil or item == nil then
            missing = missing + 1
        else
            table.insert(samples, {
                mf.predict(mu, user.b, item.b, user.p, item.p),
                record.rating,
            })
        end
    end
    return mf.rmse(samples), #samples, missing
end

--- The mean of the training ratings, as the job counted it.
--
-- Both aggregators are re-contributed on every superstep, so the master's copy
-- after the last one is the whole graph's -- see the header of app.lua.
--
-- @param instance the pregel master object
-- @return number
-- @raise when the job has not run, so nothing has been counted yet
-- @function mu_of
function M.mu_of(instance)
    local count = instance.aggregators['rating_count']
    local sum   = instance.aggregators['rating_sum']
    if count == nil or sum == nil then
        error('this master has no mf aggregators: is it running examples/mf?',
              0)
    end
    if count() == 0 then
        error('no ratings have been counted yet: has the job finished?', 0)
    end
    return sum() / count()
end

--- Read the model off the workers and score it against a held-out file.
--
-- opts.test   -- path to the Avro OCF of held-out ratings; a relative path is
--                resolved against examples/mf/, as app_cfg's paths are
--                (required)
-- opts.master -- the pregel master object, default the one the master role on
--                this instance is running
-- opts.job    -- job name, default the master's own
-- opts.mu     -- override the global mean
--
-- @return `{rmse, ratings, missing, mu, users, items}`
-- @raise when there is no master here, when a worker is unreachable, and on an
--  unreadable test file
-- @function evaluate
function M.evaluate(opts)
    opts = opts or {}

    local instance = opts.master
    if instance == nil then
        instance = require('pregel.roles.master').get()
        if instance == nil then
            error('no pregel master on this instance: run this on the master',
                  0)
        end
    end

    local path = common.resolve(HERE, opts.test, 'test')
    if not fio.path.exists(path) then
        error(string.format("evaluate: no such file: '%s'", path), 0)
    end

    local mu    = opts.mu or M.mu_of(instance)
    local model = M.collect(instance, opts.job)

    local users, items = 0, 0
    for _, vertex in pairs(model) do
        if vertex.kind == 'user' then
            users = users + 1
        else
            items = items + 1
        end
    end

    local rmse, ratings, missing = M.rmse(model, mu, ocf.read_all(path))
    return {
        rmse    = rmse,
        ratings = ratings,
        missing = missing,
        mu      = mu,
        users   = users,
        items   = items,
    }
end

return M
