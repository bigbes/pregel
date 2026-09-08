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

--- The same, over a graph of `count` vertices.
--
-- A ring needs about one superstep per vertex, so this is how a test buys
-- itself a job that is still running when it looks at it.
function helper.server_opts_for(count)
    return {
        env = {
            LUA_PATH = ROOT .. '/?.lua;' .. ROOT .. '/?/init.lua;;',
            PREGEL_TEST_VERTICES = tostring(count),
        },
    }
end

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

--- The read-only replica of worker `i`, when the config has one.
function helper.replica_name(i)
    return helper.worker_name(i) .. 'r'
end

helper.MASTER_NAME = 'master'

--- A worker URI nothing ever listens on.
--
-- Adding it to roles_cfg.workers is how a test says "one participant of this
-- job is down", which is the case that used to kill every other instance.
helper.GHOST_URI = 'unix/:./ghost.iproto'

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
-- opts.drop_master  -- the same for the master instance
-- opts.ghost_worker -- add helper.GHOST_URI to every worker list, so the job
--                      has one participant that is not there
-- opts.connect_timeout -- roles_cfg.connect_timeout for both roles
-- opts.replica_worker -- index of a worker whose replicaset gets a second
--                        instance ('<name>r') carrying the same role with the
--                        same roles_cfg, which is what `roles:` at replicaset
--                        scope produces. Switches the cluster to manual
--                        failover with the first instance as the leader, and
--                        turns the WAL on, since replication needs one.
-- opts.ssl          -- {cert = <path>, key = <path>}: make every instance
--                      listen with `transport: ssl` (Enterprise only)
-- opts.no_user      -- leave user/password out of roles_cfg, so the peers
--                      connect as guest; guest is granted what they need
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
    if opts.no_user then
        -- With no roles_cfg.user the peers connect as guest, and the role
        -- issues no space grants of its own -- giving guest write access to
        -- the graph is the operator's decision, so it has to be in the config.
        builder:set_global_option('credentials.users.guest', {
            privileges = {
                {permissions = {'execute'}, lua_call = helper.LUA_CALL},
                {permissions = {'read', 'write'}, universe = true},
            },
        })
    end
    -- Nothing in these tests outlives the cluster -- but a replica has to read
    -- its leader's WAL, so a replicated cluster pays for one.
    builder:set_global_option('wal.mode',
                              opts.replica_worker and 'write' or 'none')
    if opts.replica_worker then
        builder:set_global_option('replication.failover', 'manual')
    end
    if opts.ssl then
        -- Same address cbuilder listens on by default, with the transport
        -- spelled out. The peers' side of it is not written anywhere: it is
        -- what the roles have to carry over from iproto.listen themselves.
        builder:set_global_option('iproto.listen', {{
            uri    = 'unix/:./{{ instance_name }}.iproto',
            params = {
                transport     = 'ssl',
                ssl_cert_file = opts.ssl.cert,
                ssl_key_file  = opts.ssl.key,
            },
        }})
    end

    local worker_uris = {}
    for i = 1, count do
        worker_uris[i] = helper.uri(helper.worker_name(i))
    end
    if opts.ghost_worker then
        table.insert(worker_uris, helper.GHOST_URI)
    end

    local function base_cfg()
        return {
            name            = job,
            app             = helper.APP,
            user            = not opts.no_user and helper.USER or nil,
            password        = not opts.no_user and helper.PASSWORD or nil,
            connect_timeout = opts.connect_timeout,
        }
    end

    local master_cfg = base_cfg()
    master_cfg.autostart = opts.autostart or false
    if not opts.discovery then
        master_cfg.workers = worker_uris
    end

    builder:use_group('pregel')
    builder:use_replicaset('r_master')
    if opts.drop_master then
        builder:add_instance(helper.MASTER_NAME, {roles = {}, roles_cfg = {}})
    else
        builder:add_instance(helper.MASTER_NAME, {
            roles     = {helper.MASTER_ROLE},
            roles_cfg = {[helper.MASTER_ROLE] = master_cfg},
        })
    end

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
        if opts.replica_worker then
            builder:set_replicaset_option('leader', helper.worker_name(i))
        end
        builder:add_instance(helper.worker_name(i), {
            roles     = roles,
            roles_cfg = roles_cfg,
        })
        if opts.replica_worker == i then
            -- The same roles and the same roles_cfg, because that is what
            -- writing them at replicaset scope produces.
            builder:add_instance(helper.replica_name(i), {
                roles     = roles,
                roles_cfg = roles_cfg,
            })
        end
    end

    if opts.replica_worker then
        builder:use_replicaset('r_master')
        builder:set_replicaset_option('leader', helper.MASTER_NAME)
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

--- The worker role's status(), as seen on one instance.
function helper.worker_status(cluster, instance_name)
    return cluster[instance_name]:exec(function(role)
        return require(role).status()
    end, {helper.WORKER_ROLE})
end

--- What the config framework itself says about this instance.
--
-- A role that reports a problem without dying does it here: config:info()
-- carries the status and the alerts, and an operator sees them in
-- `tt status`/the console rather than in the log.
function helper.config_info(cluster, instance_name)
    return cluster[instance_name]:exec(function()
        local info = require('config'):info()
        local alerts = {}
        for _, alert in ipairs(info.alerts or {}) do
            table.insert(alerts, {type = alert.type, message = alert.message})
        end
        return {status = info.status, alerts = alerts}
    end)
end

--- Block until `fn(cluster)` returns a table whose state is `state`.
function helper.wait_role_state(cluster, fn, state, timeout)
    local last
    luatest.helpers.retrying({timeout = timeout or 60, delay = 0.1}, function()
        last = fn(cluster)
        if last.state ~= state then
            error(string.format("role status is '%s', want '%s'",
                                tostring(last.state), state))
        end
    end)
    return last
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

--- Names of the fibers the master role runs: the autostart one and the fiber
-- that waits for the workers. Both must be gone once the role is stopped.
function helper.master_fibers(cluster)
    return cluster[helper.MASTER_NAME]:exec(function()
        local rv = {}
        for _, info in pairs(require('fiber').info()) do
            local name = info.name or ''
            if name:find('pregel_master_autostart', 1, true) ~= nil or
               name:find('pregel_connect', 1, true) ~= nil then
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
