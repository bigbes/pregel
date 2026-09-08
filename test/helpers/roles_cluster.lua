--- A pregel cluster built from a Tarantool 3 cluster config.
--
-- The sibling helper test/helpers/cluster.lua starts bare instances and calls
-- pregel.worker.new / pregel.master.new over net.box. This one starts nothing
-- by hand: it writes a cluster config with `roles` and `roles_cfg`, and every
-- pregel object in the cluster is created by the roles applier. That is the
-- whole point -- the config is the interface under test.
--
-- One group, one instance per replicaset, so every instance is the read-write
-- leader of its own replicaset and nothing replicates: the workers are shards,
-- not copies of each other.

local fio     = require('fio')
local luatest = require('luatest')

local cbuilder = require('luatest.cbuilder')

local helper = {}

-- The checkout the instances load pregel and the test app from.
local ROOT = fio.cwd()

helper.JOB          = 'maxvalue'
helper.APP          = 'test.apps.maxvalue'
helper.VERTEX_COUNT = 12
helper.WORKER_COUNT = 3

helper.MASTER_ROLE = 'pregel.roles.master'
helper.WORKER_ROLE = 'pregel.roles.worker'

helper.USER     = 'pregel'
helper.PASSWORD = 'secret'

--- Every entry point the two roles publish.
--
-- This list is the `lua_call` half of the privilege story: pregel talks to
-- pregel with conn:call() on exactly these names, so a cluster config that
-- grants them (and nothing else) is enough for the graph traffic. Keep it in
-- step with the RPC_* constants in pregel/worker.lua and pregel/master.lua.
helper.LUA_CALL = {
    'pregel.worker.deliver',
    'pregel.worker.deliver_batch',
    'pregel.worker.wait',
    'pregel.master.deliver',
}

--- Options for every luatest.Server the cluster starts.
--
-- The instances are started with their working directory inside luatest's
-- temporary tree, so the checkout has to reach them through LUA_PATH; the
-- trailing ';;' keeps Tarantool's own default path.
helper.server_opts = {
    env = {
        LUA_PATH = ROOT .. '/?.lua;' .. ROOT .. '/?/init.lua;;',
        PREGEL_TEST_VERTICES = tostring(helper.VERTEX_COUNT),
    },
}

--- Where an instance listens, matching cbuilder's default iproto.listen.
--
-- Every instance of the cluster shares one working directory, so this relative
-- socket path means the same thing on all of them.
function helper.uri(instance_name)
    return 'unix/:./' .. instance_name .. '.iproto'
end

function helper.worker_name(i)
    return 'worker' .. i
end

helper.MASTER_NAME = 'master'

-------------------------------------------------------------------------------
-- The config
-------------------------------------------------------------------------------

--- Build the cluster config.
--
-- opts.worker_count -- default helper.WORKER_COUNT
-- opts.job          -- job name, default helper.JOB
-- opts.autostart    -- the master role runs the job by itself (default false)
-- opts.discovery    -- leave `workers`/`master` out of roles_cfg, so the roles
--                      have to find each other in the cluster config
-- opts.pool_size    -- roles_cfg.pool_size for the workers
-- opts.drop_worker  -- index of a worker whose roles list is left empty, as if
--                      the role had been taken off that instance
function helper.config(opts)
    opts = opts or {}
    local job     = opts.job or helper.JOB
    local count   = opts.worker_count or helper.WORKER_COUNT
    local builder = cbuilder:new()

    builder:set_global_option('credentials.users.' .. helper.USER, {
        password   = helper.PASSWORD,
        privileges = {{
            permissions = {'execute'},
            lua_call    = helper.LUA_CALL,
        }},
    })
    -- Nothing in these tests outlives the cluster.
    builder:set_global_option('wal.mode', 'none')

    local worker_uris = {}
    for i = 1, count do
        worker_uris[i] = helper.uri(helper.worker_name(i))
    end

    local function base_cfg()
        return {
            name     = job,
            app      = helper.APP,
            user     = helper.USER,
            password = helper.PASSWORD,
        }
    end

    local master_cfg = base_cfg()
    master_cfg.autostart = opts.autostart or false
    if not opts.discovery then
        master_cfg.workers = worker_uris
    end

    builder:use_group('pregel')
    builder:use_replicaset('r_master')
    builder:add_instance(helper.MASTER_NAME, {
        roles     = {helper.MASTER_ROLE},
        roles_cfg = {[helper.MASTER_ROLE] = master_cfg},
    })

    for i = 1, count do
        local worker_cfg = base_cfg()
        worker_cfg.pool_size = opts.pool_size
        if not opts.discovery then
            worker_cfg.workers = worker_uris
            worker_cfg.master  = helper.uri(helper.MASTER_NAME)
        end
        local roles     = {helper.WORKER_ROLE}
        local roles_cfg = {[helper.WORKER_ROLE] = worker_cfg}
        if opts.drop_worker == i then
            roles, roles_cfg = {}, {}
        end
        builder:use_replicaset('r_worker' .. i)
        builder:add_instance(helper.worker_name(i), {
            roles     = roles,
            roles_cfg = roles_cfg,
        })
    end

    return builder:config()
end

-------------------------------------------------------------------------------
-- Driving and inspecting a running cluster
-------------------------------------------------------------------------------

--- Reload the config on one instance, letting the error through.
function helper.reload(cluster, instance_name)
    return cluster[instance_name]:exec(function()
        require('config'):reload()
    end)
end

--- The master role's status(), as seen on the master instance.
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
            error(string.format("master status is '%s', want '%s'",
                                tostring(last.state), state))
        end
    end)
    return last
end

--- Drive the job by hand through the accessor the master role exposes.
--
-- Returns the number of supersteps.
function helper.run_by_hand(cluster)
    return cluster[helper.MASTER_NAME]:exec(function(role)
        local m = require(role).get()
        assert(m ~= nil, 'the master role has no master object')
        m:wait_up()
        if m.preload_func ~= nil then
            m:preload()
        end
        return m:start()
    end, {helper.MASTER_ROLE})
end

--- Every vertex of the job, read out of the workers' own spaces.
--
-- Keyed by vertex name; each entry carries the worker index it was found on,
-- and a vertex found twice fails the test -- the shard split is supposed to be
-- a partition.
function helper.collect_vertices(cluster, opts)
    opts = opts or {}
    local job   = opts.job or helper.JOB
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
                rv[tuple[1]] = {value = tuple[3], halted = tuple[2]}
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

--- Names of the fibers pregel's message pool runs on one instance.
--
-- The pusher and waitpool fibers are what a stopped role must not leave
-- behind: they hold the net.box connections and keep flushing.
function helper.mpool_fibers(cluster, instance_name)
    return cluster[instance_name]:exec(function()
        local fiber = require('fiber')
        local rv = {}
        for _, info in pairs(fiber.info()) do
            local name = info.name or ''
            if name:find('pusher_handler', 1, true) ~= nil or
               name:find('waitpool_handler', 1, true) ~= nil then
                table.insert(rv, name)
            end
        end
        table.sort(rv)
        return rv
    end)
end

--- Is the job present in pregel.worker's own registry on this instance?
function helper.worker_registered(cluster, instance_name, job)
    return cluster[instance_name]:exec(function(name)
        return require('pregel.worker').workers[name] ~= nil
    end, {job or helper.JOB})
end

return helper
