--- The pregel roles, driven the way an operator drives them: through a
-- Tarantool 3 cluster config.
--
-- Nothing here calls pregel.worker.new or pregel.master.new. Every pregel
-- object in the cluster is built by the roles applier out of `roles` and
-- `roles_cfg`, the privileges come from the `credentials` section, and the
-- assertions read the workers' own spaces -- so what is under test is the
-- config being a working interface to the library, not the library.

local t = require('luatest')

local Cluster = require('luatest.cluster')
local helper  = require('test.helpers.roles_cluster')

local g = t.group('integration.roles')

local MAX_VALUE = helper.VERTEX_COUNT

--- Assert that every vertex of the graph ended up holding the largest value.
local function assert_max_value_everywhere(cluster, opts)
    local vertices = helper.collect_vertices(cluster, opts)
    local per_worker = {}
    for i = 1, helper.VERTEX_COUNT do
        local name = string.format('v%03d', i)
        local vertex = vertices[name]
        t.assert_not_equals(vertex, nil, 'vertex ' .. name .. ' is missing')
        t.assert_equals(vertex.value.value, MAX_VALUE, 'vertex ' .. name)
        t.assert_equals(vertex.halted, true, 'vertex ' .. name)
        per_worker[vertex.worker] = (per_worker[vertex.worker] or 0) + 1
    end
    -- The graph really is spread over the instances; a run that landed
    -- everything on one worker would prove much less.
    local count = opts and opts.worker_count or helper.WORKER_COUNT
    for i = 1, count do
        t.assert_gt(per_worker[i] or 0, 0,
                    'worker ' .. i .. ' holds no vertices')
    end
    return vertices
end

-------------------------------------------------------------------------------
-- (1), (2): autostart
-------------------------------------------------------------------------------

g.test_autostart_runs_the_job_to_completion = function()
    local c = Cluster:new(helper.config({autostart = true}),
                          helper.server_opts)
    c:start()

    local status = helper.wait_state(c, 'done')

    -- A ring of N vertices needs the value to travel all the way round, so
    -- the run is many supersteps and certainly not one.
    t.assert_gt(status.superstep, 1)
    t.assert_le(status.superstep, helper.VERTEX_COUNT + 2)
    t.assert_equals(status.error, nil)
    t.assert_equals(status.name, helper.JOB)

    assert_max_value_everywhere(c)

    -- The aggregator the app module declares reached both sides: the workers
    -- had to be given it by their role, and the master had to merge what they
    -- reported into a value of its own.
    local aggregated = c[helper.MASTER_NAME]:exec(function(role)
        return require(role).get().aggregators['max_seen']()
    end, {helper.MASTER_ROLE})
    t.assert_equals(aggregated, MAX_VALUE)
end

g.test_the_accessor_hands_out_the_running_master = function()
    local c = Cluster:new(helper.config({autostart = true}),
                          helper.server_opts)
    c:start()
    helper.wait_state(c, 'done')

    local seen = c[helper.MASTER_NAME]:exec(function(role)
        local m = require(role).get()
        return {
            is_master  = m ~= nil and m.name or nil,
            supersteps = m.superstep_count,
            status     = require(role).status(),
        }
    end, {helper.MASTER_ROLE})

    t.assert_equals(seen.is_master, helper.JOB)
    t.assert_gt(seen.supersteps, 1)
    t.assert_equals(seen.status.state, 'done')
    t.assert_equals(seen.status.superstep, seen.supersteps)
end

-------------------------------------------------------------------------------
-- (3): a changed config for a running job
-------------------------------------------------------------------------------

g.test_reconfiguring_a_running_job_is_refused = function()
    local c = Cluster:new(helper.config({}), helper.server_opts)
    c:start()

    -- Same config except for one worker option, which is exactly the case the
    -- role cannot honour: pool_size is fixed when the message pool is built.
    c:sync(helper.config({pool_size = 17}))

    t.assert_error_msg_contains(
        'pregel.roles.worker: reconfiguration of a running job is not ' ..
        'supported, stop the role first',
        helper.reload, c, helper.worker_name(1))

    -- Refused, not half-applied: the job that was already there still runs,
    -- and still computes the right answer.
    t.assert_equals(helper.worker_registered(c, helper.worker_name(1)), true)
    local supersteps = helper.run_by_hand(c)
    t.assert_gt(supersteps, 1)
    assert_max_value_everywhere(c)
end

-------------------------------------------------------------------------------
-- (4): taking the role off an instance, and putting it back
-------------------------------------------------------------------------------

g.test_removing_the_role_stops_the_worker = function()
    local c = Cluster:new(helper.config({}), helper.server_opts)
    c:start()

    local worker1 = helper.worker_name(1)
    t.assert_not_equals(helper.mpool_fibers(c, worker1), {},
                        'the worker role left no message pool fibers running')

    -- Take the role off worker1 and reload only that instance.
    c:sync(helper.config({drop_worker = 1}))
    helper.reload(c, worker1)

    t.assert_equals(helper.mpool_fibers(c, worker1), {},
                    'stopping the role left message pool fibers behind')
    t.assert_equals(helper.worker_registered(c, worker1), false,
                    'stopping the role left the job in the registry')
    t.assert_equals(c[worker1]:exec(function(role)
        return require(role).status()
    end, {helper.WORKER_ROLE}), {state = 'idle'})

    -- The other workers are untouched.
    t.assert_equals(helper.worker_registered(c, helper.worker_name(2)), true)

    -- Put it back: the role starts a fresh job on the same instance.
    c:sync(helper.config({}))
    helper.reload(c, worker1)

    t.assert_equals(helper.worker_registered(c, worker1), true)
    t.assert_not_equals(helper.mpool_fibers(c, worker1), {},
                        'restarting the role started no message pool fibers')

    -- And the restarted worker takes part in a real run.
    local supersteps = helper.run_by_hand(c)
    t.assert_gt(supersteps, 1)
    assert_max_value_everywhere(c)
end

-- Taking the master role off an instance while its job is running. Nothing
-- covered this at all, which is where the status below was left stuck: stop()
-- used to set 'idle' and *then* cancel the autostart fiber, so the
-- cancellation landed in that fiber's failure branch and overwrote it.
g.test_removing_the_master_role_mid_job_leaves_it_idle = function()
    -- A ring long enough that the job is certainly still running when the
    -- role is taken away: one superstep carries the value one hop.
    local vertices = 1500
    local c = Cluster:new(helper.config({autostart = true}),
                          helper.server_opts_for(vertices))
    c:start()
    helper.wait_state(c, 'running')

    c:sync(helper.config({autostart = true, drop_master = true}))
    helper.reload(c, helper.MASTER_NAME)

    t.assert_equals(helper.master_status(c), {state = 'idle'})
    t.assert_equals(helper.master_fibers(c), {},
                    'stopping the role left the master fibers running')

    -- And it stays idle: the cancelled fiber must not come back and write
    -- 'failed' over the state stop() set.
    c[helper.MASTER_NAME]:exec(function() require('fiber').sleep(0.5) end)
    t.assert_equals(helper.master_status(c), {state = 'idle'})

    t.assert_equals(c[helper.MASTER_NAME]:grep_log('job .* failed'), nil,
                    'stopping the role logged the job as failed')
    t.assert_equals(c[helper.MASTER_NAME]:grep_log('fiber is cancelled'), nil,
                    'the cancellation reached the log as an error')
end

-------------------------------------------------------------------------------
-- A participant that is not there
-------------------------------------------------------------------------------

-- The role's apply() runs inside the config framework's synchronous
-- post_apply, and an error raised from it at startup is fatal. Connecting
-- there meant one instance that was down killed every other one in the cluster
-- 30 s later, and nothing retried afterwards.
g.test_a_peer_that_is_down_does_not_stop_the_cluster = function()
    local c = Cluster:new(helper.config({ghost_worker = true}),
                          helper.server_opts)
    c:start()

    local worker1 = helper.worker_name(1)
    t.assert_equals(helper.worker_status(c, worker1).state, 'connecting',
                    'the role should still be waiting for the missing peer')
    -- The first attempt is a few seconds long, so the reason appears a moment
    -- after the state does. It has to say which peer and what net.box saw.
    t.helpers.retrying({timeout = 30, delay = 0.5}, function()
        local status = helper.worker_status(c, worker1)
        t.assert_equals(status.state, 'connecting')
        t.assert_str_contains(tostring(status.error), 'ghost.iproto')
        t.assert_str_contains(tostring(status.error), 'No such file')
    end)

    -- The instance is alive and the job is registered: the graph is simply
    -- waiting for a peer, which is what an operator can act on.
    t.assert_equals(helper.worker_registered(c, worker1), true)
    t.assert_equals(helper.master_status(c).state, 'connecting')

    -- The other instances came up too, rather than being taken down with it.
    for i = 2, helper.WORKER_COUNT do
        t.assert_equals(helper.worker_registered(c, helper.worker_name(i)),
                        true)
    end
end

-- Giving up is reported through the role's own alerts namespace, not by
-- raising: measured on 3.9 and on EE 3.7, a role's namespace accepts
-- type='warn' only, and raising from apply() at startup exits the process.
g.test_giving_up_on_a_peer_is_an_alert_and_not_a_dead_instance = function()
    local c = Cluster:new(helper.config({ghost_worker = true,
                                         connect_timeout = 1}),
                          helper.server_opts)
    c:start()

    local worker1 = helper.worker_name(1)
    local status = helper.wait_role_state(c, function(cluster)
        return helper.worker_status(cluster, worker1)
    end, 'failed', 30)
    t.assert_str_contains(tostring(status.error), 'ghost.iproto')

    local info = helper.config_info(c, worker1)
    t.assert_equals(info.status, 'ready')
    local said = false
    for _, alert in ipairs(info.alerts) do
        if alert.message:find('ghost.iproto', 1, true) ~= nil then
            said = true
            t.assert_equals(alert.type, 'warn')
        end
    end
    t.assert_equals(said, true, 'no alert names the peer that never answered')
end

-------------------------------------------------------------------------------
-- (5): discovery
-------------------------------------------------------------------------------

g.test_the_roles_find_each_other_in_the_cluster_config = function()
    local config = helper.config({autostart = true, discovery = true})

    -- Guard against the test passing for the wrong reason: with the URIs
    -- written out this would prove nothing about discovery.
    local instances = config.groups.pregel.replicasets
    for _, replicaset in pairs(instances) do
        for name, instance in pairs(replicaset.instances) do
            for role, cfg in pairs(instance.roles_cfg or {}) do
                t.assert_equals(cfg.workers, nil,
                                name .. ' still lists workers for ' .. role)
                t.assert_equals(cfg.master, nil,
                                name .. ' still lists a master for ' .. role)
            end
        end
    end

    local c = Cluster:new(config, helper.server_opts)
    c:start()

    helper.wait_state(c, 'done')
    assert_max_value_everywhere(c)

    -- Every worker found the same set of peers, itself included.
    local uris = c[helper.worker_name(1)]:exec(function(role)
        local w = require(role).get()
        return w.workers
    end, {helper.WORKER_ROLE})
    t.assert_equals(#uris, helper.WORKER_COUNT)
end

-- config:instance_uri('peer', ...) hands out the login and password from
-- iproto.advertise.peer -- the replication user in a stock config -- and
-- discovery drops them on purpose: replication has no lua_call grant and no
-- business getting one. Nothing said so, because net.box's opts.user wins over
-- a URI's own userinfo and every other test sets roles_cfg.user, so the
-- property was held by mpool's option rather than by discovery
-- (pregel-9vt, M6). Here there is no roles_cfg.user at all: the peers connect
-- as guest, which the config grants, and a URI carrying 'replicator:secret@'
-- would authenticate as a user with neither the lua_call nor the spaces.
g.test_discovery_does_not_borrow_the_replication_login = function()
    local c = Cluster:new(helper.config({autostart = true, discovery = true,
                                         no_user = true}),
                          helper.server_opts)
    c:start()

    -- Checked before the run, so a URI that does carry them says so in one
    -- line instead of as a job that never finishes.
    local uris = c[helper.worker_name(1)]:exec(function(role)
        return require(role).get().workers
    end, {helper.WORKER_ROLE})
    for _, uri in ipairs(uris) do
        local address = type(uri) == 'table' and uri.uri or uri
        t.assert_not_str_contains(address, '@',
                                  'a discovered URI carries credentials')
    end

    helper.wait_state(c, 'done', 30)
    assert_max_value_everywhere(c)
end

-- A replicaset written as `roles: [pregel.roles.worker]` puts the role on the
-- replica too, and discovery used to count the replica as a second worker of
-- the job -- so every instance tried to connect to an address that would never
-- serve pregel, and the whole cluster died 30 s later. One worker per
-- replicaset, addressed through the leader.
g.test_discovery_takes_one_worker_per_replicaset = function()
    local config = helper.config({autostart = true, discovery = true,
                                  replica_worker = 1})
    local c = Cluster:new(config, helper.server_opts)
    c:start()

    helper.wait_state(c, 'done')
    assert_max_value_everywhere(c)

    local uris = c[helper.worker_name(1)]:exec(function(role)
        return require(role).get().workers
    end, {helper.WORKER_ROLE})
    t.assert_equals(#uris, helper.WORKER_COUNT,
                    'the read-only replica was counted as a worker')
    for _, uri in ipairs(uris) do
        local address = type(uri) == 'table' and uri.uri or uri
        t.assert_not_str_contains(address, helper.replica_name(1),
                                  'a replica is among the discovered workers')
    end
end

-- The other half of the same config: the replica carries the role and cannot
-- run it. Refusing to apply is worse than not running -- at startup that is a
-- fatal config error and the process exits -- so the role goes inert and says
-- so. Nothing covered this at all: check_writable could be deleted from both
-- roles and the suite stayed green.
g.test_the_role_is_inert_on_a_read_only_instance = function()
    local c = Cluster:new(helper.config({autostart = true, discovery = true,
                                         replica_worker = 1}),
                          helper.server_opts)
    c:start()

    local replica = helper.replica_name(1)
    t.assert_equals(c[replica]:exec(function() return box.info.ro end), true)
    t.assert_equals(helper.worker_status(c, replica), {state = 'read_only'})
    t.assert_equals(helper.worker_registered(c, replica), false,
                    'the read-only instance created a job anyway')

    -- The instance itself is healthy, which is the whole point.
    t.assert_equals(c[replica]:exec(function()
        return require('config'):info().status
    end), 'ready')

    -- And the job still runs on the rest of the cluster.
    helper.wait_state(c, 'done')
end
