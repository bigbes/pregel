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

-- The credentials role that marks the user pregel connects as, and the user
-- itself. They cannot share a name: a credentials role and a user live in one
-- namespace, and the applier dies with "User 'pregel' already exists".
helper.CREDENTIALS_ROLE = 'pregel'
helper.USER             = 'pregel_peer'
helper.PASSWORD         = 'secret'

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

--- The spaces one worker of `job` owns, and their sequences.
--
-- The read/write half of the privileges, which a cluster config has to spell
-- out because a lua_call runs with the caller's privileges and the entry
-- points write. Keep in step with worker.space_names(): a name listed here
-- that no instance ever creates is not harmless -- the credentials applier
-- retries it for ever, keeps a `warn` alert about it, and holds
-- config:info().status at 'check_warnings'.
--
-- `data_<job>` has a string primary key and therefore no sequence; the other
-- three are sequence-backed. The delayed_push bucket spaces are not here: no
-- test turns delayed_push on, and listing spaces that are never created is
-- exactly the mistake above.
function helper.job_spaces(job)
    job = job or helper.JOB
    return {
        'data_' .. job,
        'topology_mutation_' .. job,
        'pregel_tube_mqueue_first_' .. job,
        'pregel_tube_mqueue_second_' .. job,
    }
end

function helper.job_sequences(job)
    job = job or helper.JOB
    return {
        'topology_mutation_' .. job .. '_seq',
        'pregel_tube_mqueue_first_' .. job .. '_seq',
        'pregel_tube_mqueue_second_' .. job .. '_seq',
    }
end

--- The `credentials` a worker replicaset carries on top of the global one.
--
-- Written at replicaset scope rather than globally, and it has to be: the
-- master never creates the job's spaces, so a global grant on them would leave
-- that instance warning about objects that will never appear. Replacing the
-- whole `privileges` list is what the config framework does with an option
-- respecified at a narrower scope, so the entry points are repeated here.
function helper.worker_credentials(job)
    return {
        roles = {
            [helper.CREDENTIALS_ROLE] = {
                privileges = {
                    {permissions = {'execute'}, lua_call = helper.LUA_CALL},
                    {
                        permissions = {'read', 'write'},
                        spaces      = helper.job_spaces(job),
                        sequences   = helper.job_sequences(job),
                    },
                },
            },
        },
    }
end

--- What a test cluster adds to the `credentials` section.
--
-- The shape an operator writes: one credentials role carrying the privileges,
-- one user carrying that role. The roles find the user by the role, so the
-- role's name is fixed (helper.CREDENTIALS_ROLE) while the user's is not.
--
-- Keyed by the path *under* `credentials`, one entry per object, because
-- cbuilder's own base config already puts `replicator` and `client` in
-- credentials.users -- and setting the whole `credentials.users` table would
-- take them out, which leaves luatest unable to connect to its own cluster.
-- Those two are also what makes this a realistic test: the config holds three
-- users and only one of them is pregel's.
--
-- opts.user       -- the user carrying the role (default helper.USER)
-- opts.password   -- its password (default helper.PASSWORD); false leaves the
--                    password out, which is a configuration error the roles
--                    report
-- opts.extra_user -- a second user carrying the role, which is also one
--
-- @return a table to hand to helper.config as opts.credentials
function helper.credentials(opts)
    opts = opts or {}
    local rv = {
        ['roles.' .. helper.CREDENTIALS_ROLE] = {
            privileges = {{
                permissions = {'execute'},
                lua_call    = helper.LUA_CALL,
            }},
        },
        ['users.' .. (opts.user or helper.USER)] = {
            password = opts.password ~= false and
                       (opts.password or helper.PASSWORD) or nil,
            roles    = {helper.CREDENTIALS_ROLE},
        },
    }
    if opts.extra_user then
        rv['users.' .. opts.extra_user] = {
            password = helper.PASSWORD,
            roles    = {helper.CREDENTIALS_ROLE},
        }
    end
    return rv
end

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

-------------------------------------------------------------------------------
-- The config
-------------------------------------------------------------------------------

--- Build the cluster config.
--
-- opts.worker_count -- default helper.WORKER_COUNT
-- opts.job          -- job name, default helper.JOB
-- opts.master_job   -- the master's job name, when it is to differ from the
--                      workers': a job's participants are the instances that
--                      name it, so this is how a test says "nobody runs the
--                      other half of this job"
-- opts.worker_job   -- the same for the workers
-- opts.app          -- roles_cfg.app for both roles, default helper.APP
-- opts.app_cfg      -- roles_cfg.app_cfg for both roles
-- opts.autostart    -- the master role runs the job by itself (default false)
-- opts.max_supersteps -- roles_cfg.max_supersteps for the master
-- opts.pool_size    -- roles_cfg.pool_size for the workers
-- opts.drop_worker  -- index of a worker whose roles list is left empty, as if
--                      the role had been taken off that instance
-- opts.drop_master  -- the same for the master instance
-- opts.connect_timeout -- roles_cfg.connect_timeout for both roles
-- opts.replica_worker -- index of a worker whose replicaset gets a second
--                        instance ('<name>r') carrying the same role with the
--                        same roles_cfg, which is what `roles:` at replicaset
--                        scope produces. Switches the cluster to manual
--                        failover with the first instance as the leader, and
--                        turns the WAL on, since replication needs one.
-- opts.ssl          -- {cert = <path>, key = <path>}: make every instance
--                      listen with `transport: ssl` (Enterprise only)
-- opts.credentials  -- replace the global `credentials` entries, for the tests
--                      about resolving the pregel user from it
-- opts.no_space_privileges -- leave the read/write half off the worker
--                      replicasets, so the pregel user may call the entry
--                      points and not write to the spaces they write to
function helper.config(opts)
    opts = opts or {}
    local job     = opts.job or helper.JOB
    local count   = opts.worker_count or helper.WORKER_COUNT
    local builder = cbuilder:new()

    -- Who the instances connect to each other as, and what that login may do.
    -- Both halves hang off one credentials role: the roles look for the user
    -- carrying it, and the privileges of the role are what that user gets.
    for name, value in pairs(opts.credentials or helper.credentials()) do
        builder:set_global_option('credentials.' .. name, value)
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

    local function base_cfg()
        return {
            name            = job,
            app             = opts.app or helper.APP,
            app_cfg         = opts.app_cfg,
            connect_timeout = opts.connect_timeout,
        }
    end

    local master_cfg = base_cfg()
    master_cfg.autostart      = opts.autostart or false
    master_cfg.name           = opts.master_job or job
    master_cfg.max_supersteps = opts.max_supersteps

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
        worker_cfg.name      = opts.worker_job or job
        local roles     = {helper.WORKER_ROLE}
        local roles_cfg = {[helper.WORKER_ROLE] = worker_cfg}
        if opts.drop_worker == i then
            roles, roles_cfg = {}, {}
        end
        builder:use_replicaset('r_worker' .. i)
        -- The job's own spaces, granted where they are created.
        if not opts.no_space_privileges then
            builder:set_replicaset_option('credentials',
                                          helper.worker_credentials(job))
        end
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

--- Start every instance of the cluster except `missing`.
--
-- How a test says "one participant of this job is down". There is no URI to
-- invent for it any more: the participants are exactly the instances the
-- cluster config gives the role to, so a peer that is not there is an instance
-- that was never started -- which is also closer to the case this is about,
-- since tt forks the instances of a cluster in whatever order it likes and one
-- of them may be minutes behind the rest.
--
-- Cluster:start() would wait for every instance including that one, so the
-- servers are started one by one. `each` walks the cluster's own list, so an
-- instance added to the config is started without this having to know about it.
--
-- @param cluster the luatest cluster
-- @param missing the instance name not to start
function helper.start_without(cluster, missing)
    local started = {}
    cluster:each(function(server)
        if server.alias ~= missing then
            server:start({wait_until_ready = false})
            table.insert(started, server)
        end
    end)
    for _, server in ipairs(started) do
        server:wait_until_ready()
    end
end

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

--- What the pregel user and the pregel credentials role hold on one instance.
--
-- `own` is the object types granted to the user itself, sorted -- a role that
-- granted something from Lua would show up here. `granted` maps
-- '<object type> <object name>' to the permission bits the credentials role
-- carries, so an assertion can name the space rather than its id.
function helper.privileges(cluster, instance_name)
    return cluster[instance_name]:exec(function(user, role)
        local own = {}
        for _, tuple in box.space._priv:pairs({
            box.space._user.index.name:get({user})[1]
        }) do
            table.insert(own, tostring(tuple[3]))
        end
        table.sort(own)

        local granted = {}
        for _, tuple in box.space._priv:pairs({
            box.space._user.index.name:get({role})[1]
        }) do
            local name = tostring(tuple[4])
            if tuple[3] == 'space' then
                name = box.space[tuple[4]].name
            elseif tuple[3] == 'sequence' then
                for _, sequence in box.space._sequence:pairs() do
                    if sequence[1] == tuple[4] then
                        name = sequence[3]
                    end
                end
            end
            granted[tuple[3] .. ' ' .. name] = tuple[5]
        end
        return {own = own, granted = granted}
    end, {helper.USER, helper.CREDENTIALS_ROLE})
end

--- Is the job present in pregel.worker's own registry on this instance?
function helper.worker_registered(cluster, instance_name, job)
    return cluster[instance_name]:exec(function(name)
        return require('pregel.worker').workers[name] ~= nil
    end, {job or helper.JOB})
end

return helper
