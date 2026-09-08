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
