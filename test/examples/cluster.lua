--- A pregel cluster running one of the app modules under examples/.
--
-- The examples are meant to be started with `tt` from their own directory;
-- this helper starts the same app modules from a luatest cluster config, so
-- what a test asserts is the example itself -- its compute function, its
-- aggregators, its loader and the app_cfg contract between them and the
-- cluster config -- and not a copy of it written for the test.
--
-- What the tests deliberately do not share with examples/<name>/config.yaml is
-- the transport: the instances here listen on unix sockets inside luatest's
-- temporary tree, because binding 127.0.0.1:3301 would collide with an example
-- a developer left running. Everything else -- the credentials, the roles, the
-- shape of roles_cfg, the relative paths in app_cfg -- is the same, and
-- test/examples/config_test.lua checks the committed configs against it.

local fio     = require('fio')
local luatest = require('luatest')

local cbuilder = require('luatest.cbuilder')

local helper = {}

-- luatest runs from the repository root, which is where the instances have to
-- find pregel and the example app modules.
helper.ROOT = fio.cwd()

-- The credentials role the pregel user is marked with, and that user. A role
-- and a user cannot share a name -- they live in one namespace -- so the user
-- is not called `pregel`; examples/*/config.yaml spells it the same way.
helper.CREDENTIALS_ROLE = 'pregel'
helper.USER             = 'pregel_peer'
helper.PASSWORD         = 'secret'

helper.MASTER_NAME  = 'master'
helper.MASTER_ROLE  = 'pregel.roles.master'
helper.WORKER_ROLE  = 'pregel.roles.worker'
helper.WORKER_COUNT = 3

--- Every entry point the two roles publish; the `lua_call` half of the
-- privileges, exactly as examples/*/config.yaml spells it.
helper.LUA_CALL = {
    'pregel.worker.deliver',
    'pregel.worker.deliver_batch',
    'pregel.worker.wait',
    'pregel.master.deliver',
}

helper.server_opts = {
    env = {
        LUA_PATH = helper.ROOT .. '/?.lua;' .. helper.ROOT .. '/?/init.lua;;',
    },
}

function helper.worker_name(i)
    return 'worker' .. i
end

--- Where an instance listens, matching cbuilder's default iproto.listen.
function helper.uri(instance_name)
    return 'unix/:./' .. instance_name .. '.iproto'
end

--- An absolute path under test/fixtures/graphs/.
function helper.fixture(...)
    return fio.pathjoin(helper.ROOT, 'test', 'fixtures', 'graphs', ...)
end

--- The directory of one example, absolute.
function helper.example_dir(name)
    return fio.pathjoin(helper.ROOT, 'examples', name)
end

--- The Lua module name of one example's app module.
function helper.app_module(name)
    return 'examples.' .. name .. '.app'
end

-------------------------------------------------------------------------------
-- The config
-------------------------------------------------------------------------------

--- Build a cluster config for one example.
--
-- opts.name         -- the example's directory name (required)
-- opts.job          -- job name, default opts.name with dashes removed
-- opts.app_cfg      -- roles_cfg app_cfg, handed to both roles
-- opts.worker_count -- default helper.WORKER_COUNT
-- opts.autostart    -- default true
-- opts.squash_only  -- roles_cfg.squash_only for the workers
function helper.config(opts)
    assert(type(opts) == 'table' and opts.name ~= nil, 'opts.name is required')
    local count = opts.worker_count or helper.WORKER_COUNT
    local job   = opts.job or opts.name:gsub('%-', '')
    local app   = helper.app_module(opts.name)

    local builder = cbuilder:new()
    builder:set_global_option('credentials.roles.' ..
                              helper.CREDENTIALS_ROLE, {
        privileges = {{
            permissions = {'execute'},
            lua_call    = helper.LUA_CALL,
        }},
    })
    builder:set_global_option('credentials.users.' .. helper.USER, {
        password = helper.PASSWORD,
        roles    = {helper.CREDENTIALS_ROLE},
    })
    builder:set_global_option('wal.mode', 'none')

    local function base_cfg()
        return {
            name    = job,
            app     = app,
            app_cfg = opts.app_cfg,
        }
    end

    local master_cfg = base_cfg()
    master_cfg.autostart = opts.autostart ~= false

    builder:use_group('pregel')
    builder:use_replicaset('r_master')
    builder:add_instance(helper.MASTER_NAME, {
        roles     = {helper.MASTER_ROLE},
        roles_cfg = {[helper.MASTER_ROLE] = master_cfg},
    })

    for i = 1, count do
        local worker_cfg = base_cfg()
        worker_cfg.squash_only = opts.squash_only
        builder:use_replicaset('r_worker' .. i)
        builder:add_instance(helper.worker_name(i), {
            roles     = {helper.WORKER_ROLE},
            roles_cfg = {[helper.WORKER_ROLE] = worker_cfg},
        })
    end

    return builder:config()
end

-------------------------------------------------------------------------------
-- Driving and inspecting
-------------------------------------------------------------------------------

--- The master role's status().
function helper.master_status(cluster)
    return cluster[helper.MASTER_NAME]:exec(function(role)
        return require(role).status()
    end, {helper.MASTER_ROLE})
end

--- Block until the autostarted job reports `state`, or fail the test.
function helper.wait_state(cluster, state, timeout)
    local last
    luatest.helpers.retrying({timeout = timeout or 60, delay = 0.1}, function()
        last = helper.master_status(cluster)
        if last.state ~= state then
            error(string.format("master status is '%s', want '%s'%s",
                                tostring(last.state), state,
                                last.error and (': ' .. last.error) or ''))
        end
    end)
    return last
end

--- Start a cluster for one example and wait for the job to finish.
--
-- Returns the cluster and the master's final status. The caller registers the
-- cluster's teardown itself (luatest.cluster does it per-group).
function helper.run(Cluster, opts)
    local cluster = Cluster:new(helper.config(opts), helper.server_opts)
    cluster:start()
    local status = helper.wait_state(cluster, 'done', opts.timeout)
    return cluster, status
end

--- Every vertex of the job, read out of the workers' own spaces.
--
-- Keyed by vertex name; each entry carries the worker index it was found on,
-- and a vertex found twice fails the test -- the shard split is a partition.
function helper.collect_vertices(cluster, opts)
    opts = opts or {}
    local job   = assert(opts.job, 'opts.job is required')
    local count = opts.worker_count or helper.WORKER_COUNT
    local all   = {}
    for i = 1, count do
        local found = cluster[helper.worker_name(i)]:exec(function(name)
            local rv = {}
            local space = box.space['data_' .. name]
            if space == nil then
                return rv
            end
            for _, tuple in space:pairs() do
                rv[tuple[1]] = {
                    value  = tuple[3],
                    halted = tuple[2],
                    edges  = tuple[4],
                }
            end
            return rv
        end, {job})
        for name, vertex in pairs(found) do
            assert(all[name] == nil,
                   'vertex ' .. name .. ' is on more than one worker')
            vertex.worker = i
            all[name] = vertex
        end
    end
    return all
end

--- How many of the workers hold at least one vertex.
--
-- For a graph of a few dozen vertices this is every worker; for one of six it
-- may not be, and demanding it would be a test of the hash function rather
-- than of the example.
function helper.workers_holding(vertices)
    local seen, rv = {}, 0
    for _, vertex in pairs(vertices) do
        if not seen[vertex.worker] then
            seen[vertex.worker] = true
            rv = rv + 1
        end
    end
    return rv
end

--- Assert that the graph really was spread over every worker.
--
-- A run that landed the whole graph on one instance would satisfy most of what
-- these tests check while proving nothing about the sharding.
function helper.assert_spread(vertices, count)
    count = count or helper.WORKER_COUNT
    local per_worker = {}
    for _, vertex in pairs(vertices) do
        per_worker[vertex.worker] = (per_worker[vertex.worker] or 0) + 1
    end
    for i = 1, count do
        luatest.assert_gt(per_worker[i] or 0, 0,
                          'worker ' .. i .. ' holds no vertices')
    end
end

return helper
