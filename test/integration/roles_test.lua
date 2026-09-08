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

--- Reload `instance`, expecting it to fail, and return the message.
--
-- luatest wraps what the reload raised in a {class = 'LuatestErrorWrapper'}
-- table, and unwraps it in its own assertions but not for a plain pcall --
-- where tostring() then answers 'table: 0x...', a string that contains no
-- substring anybody meant to assert and fails every check silently for the
-- wrong reason.
local function reload_error(cluster, instance)
    local ok, err = pcall(helper.reload, cluster, instance)
    t.assert_equals(ok, false, 'the reload was expected to fail')
    if type(err) == 'table' and err.class == 'LuatestErrorWrapper' then
        err = err.error
    end
    if type(err) == 'table' and err.message ~= nil then
        return tostring(err.message)
    end
    return tostring(err)
end

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

-- A worker that is listening, holds the `pregel` user and its lua_call grants,
-- and has not applied its role yet. During a cluster start that is the ordinary
-- case rather than an exotic one -- tt forks the instances in whatever order it
-- likes and nothing sequences the role appliers -- and it used to end the job
-- for good: the pool called that peer connected, wait_up() got "Procedure
-- 'pregel.worker.deliver' is not defined", and the master went to 'failed'
-- 0 ms after reporting every peer reached, with no alert and nothing retrying.
--
-- The window is raced for in real life; here it is held open by starting the
-- rest of the cluster from a config that gives worker1 the role -- so the
-- master discovers it as a participant -- and then bringing worker1 itself up
-- from a config that does not, so it listens without ever becoming a worker.
-- Dropping the role from the config the whole cluster starts with would not do
-- it any more: an instance with no role is not a participant of the job at
-- all, and the master would simply run over the other two.
g.test_a_worker_whose_role_applies_late_still_runs_the_job = function()
    local c = Cluster:new(helper.config({autostart = true}),
                          helper.server_opts)

    local worker1 = helper.worker_name(1)
    helper.start_without(c, worker1)

    -- worker1 comes up with the credentials and without the role: the config
    -- on disk no longer gives it one, and the master has already resolved its
    -- participants from the config that did.
    c:sync(helper.config({autostart = true, drop_worker = 1}))
    c:start_instance(worker1)

    -- Waiting, visibly, and saying which peer and why -- the same contract as
    -- a peer that is simply down.
    local said
    t.helpers.retrying({timeout = 60, delay = 0.5}, function()
        t.assert_equals(helper.master_status(c).state, 'connecting')
        said = nil
        for _, alert in ipairs(helper.config_info(c, helper.MASTER_NAME).alerts) do
            if alert.message:find('not serving pregel', 1, true) ~= nil then
                said = alert
            end
        end
        t.assert_not_equals(said, nil, 'no alert says the peer is not serving')
    end)
    t.assert_equals(said.type, 'warn')
    t.assert_str_contains(said.message,
                          "Procedure 'pregel.worker.deliver' is not defined")

    -- The role applies late; the job must pick that up and run to completion.
    c:sync(helper.config({autostart = true}))
    helper.reload(c, worker1)

    helper.wait_state(c, 'done')
    assert_max_value_everywhere(c)
    t.assert_equals(helper.config_info(c, helper.MASTER_NAME).alerts, {},
                    'the alert outlived the peer it was about')
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
    -- A fourth worker the config gives the role to and nothing starts. The
    -- participants come from the config now, so this is the only way a job can
    -- have one that is not there -- and it is the ordinary case during a
    -- cluster start rather than an exotic one.
    local c = Cluster:new(helper.config({worker_count = 4}),
                          helper.server_opts)
    helper.start_without(c, helper.worker_name(4))

    local worker1 = helper.worker_name(1)
    t.assert_equals(helper.worker_status(c, worker1).state, 'connecting',
                    'the role should still be waiting for the missing peer')
    -- The first attempt is a few seconds long, so the reason appears a moment
    -- after the state does. It has to say which peer and what net.box saw.
    t.helpers.retrying({timeout = 30, delay = 0.5}, function()
        local status = helper.worker_status(c, worker1)
        t.assert_equals(status.state, 'connecting')
        t.assert_str_contains(tostring(status.error),
                              helper.worker_name(4) .. '.iproto')
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
    local c = Cluster:new(helper.config({worker_count = 4,
                                         connect_timeout = 1}),
                          helper.server_opts)
    helper.start_without(c, helper.worker_name(4))

    local worker1 = helper.worker_name(1)
    local status = helper.wait_role_state(c, function(cluster)
        return helper.worker_status(cluster, worker1)
    end, 'failed', 30)
    t.assert_str_contains(tostring(status.error),
                          helper.worker_name(4) .. '.iproto')

    local info = helper.config_info(c, worker1)
    t.assert_equals(info.status, 'ready')
    local said = false
    for _, alert in ipairs(info.alerts) do
        if alert.message:find(helper.worker_name(4) .. '.iproto', 1, true)
           ~= nil then
            said = true
            t.assert_equals(alert.type, 'warn')
        end
    end
    t.assert_equals(said, true, 'no alert names the peer that never answered')
end

-------------------------------------------------------------------------------
-- The credentials, read out of the cluster config
-------------------------------------------------------------------------------

-- The stub-config tests in roles_credentials_test.lua check the reading; this
-- checks that what is read is what the roles then connect with, against a real
-- config framework. A wrong login is not a subtle failure -- nothing
-- authenticates -- so the job finishing is the assertion.
g.test_the_peers_connect_as_the_user_the_credentials_mark = function()
    local c = Cluster:new(helper.config({autostart = true}),
                          helper.server_opts)
    c:start()
    helper.wait_state(c, 'done')
    assert_max_value_everywhere(c)

    -- No roles_cfg anywhere carries a login, and the one that is used is the
    -- user the `credentials` section marks -- not `guest`, and not the
    -- replication user the peer URIs would otherwise hand over.
    -- net.box keeps the login on the connection (and clears the password), so
    -- this is the login the graph traffic actually authenticated with.
    local logins = c[helper.worker_name(1)]:exec(function(role)
        local w = require(role).get()
        local rv = {}
        for _, bucket in ipairs(w.mpool.buckets) do
            -- The bucket for this instance itself has no connection: it calls
            -- the registry in this process instead of dialling its own
            -- listener.
            if bucket.connection ~= nil then
                rv[tostring(bucket.connection.opts.user)] = true
            end
        end
        rv[tostring(w.master.opts.user)] = true
        return rv
    end, {helper.WORKER_ROLE})
    t.assert_equals(logins, {[helper.USER] = true})
end

-- The other half of the same property, from the config's side: a cluster whose
-- credentials mark nobody has no login for the graph traffic, and the role has
-- to say so rather than fall back to guest. Driven through a reload, because
-- an error raised from apply() during startup exits the process.
g.test_a_config_that_marks_no_pregel_user_is_refused = function()
    local c = Cluster:new(helper.config({drop_worker = 1}), helper.server_opts)
    c:start()

    local worker1 = helper.worker_name(1)
    -- The role comes back on worker1, but the credentials no longer mark
    -- anyone: the pregel user is there, with its password, and only the role
    -- membership is gone.
    local credentials = helper.credentials()
    credentials['users.' .. helper.USER].roles = nil
    c:sync(helper.config({credentials = credentials}))

    t.assert_error_msg_contains(
        "pregel.roles.worker: no user in the cluster config has the " ..
        "credentials role 'pregel'",
        helper.reload, c, worker1)
    t.assert_equals(helper.worker_registered(c, worker1), false,
                    'the role built a job without a login')
end

-- Two marked users is the case that cannot be resolved by a rule: every
-- instance reads the config for itself, so two of them may pick different
-- logins.
g.test_a_config_that_marks_two_pregel_users_is_refused = function()
    local c = Cluster:new(helper.config({drop_worker = 1}), helper.server_opts)
    c:start()

    c:sync(helper.config({
        credentials = helper.credentials({extra_user = 'pregel_other'}),
    }))

    t.assert_error_msg_contains(
        "2 users in the cluster config have the credentials role 'pregel' " ..
        "('pregel_other', 'pregel_peer')",
        helper.reload, c, helper.worker_name(1))
end

-------------------------------------------------------------------------------
-- The privileges, granted by the credentials applier
-------------------------------------------------------------------------------

-- Nothing in this suite grants a privilege from Lua any more: the roles hand
-- none out, and every job here runs on what `credentials.roles.pregel` says.
-- That is the whole of the deliberate change, so it is worth one test that
-- reads the schema rather than trusting a job that finished -- a leftover
-- role-issued grant would make every other test pass just as well.
g.test_the_job_runs_on_privileges_the_config_granted = function()
    local c = Cluster:new(helper.config({autostart = true}),
                          helper.server_opts)
    c:start()
    helper.wait_state(c, 'done')

    local function assert_granted(label)
        local seen = helper.privileges(c, helper.worker_name(1))
        -- The user itself holds nothing but its role memberships and the
        -- session bits every user has: a grant issued to it directly would be
        -- a role granting behind the config's back.
        t.assert_equals(seen.own, {'role', 'role', 'universe', 'user'},
                        label .. ': the user holds a direct grant')
        -- And the role carries both halves, for the objects the job created
        -- after the credentials applier had already run.
        for _, space in ipairs(helper.job_spaces()) do
            t.assert_equals(seen.granted['space ' .. space], 3,
                            label .. ': read,write on ' .. space)
        end
        for _, sequence in ipairs(helper.job_sequences()) do
            t.assert_equals(seen.granted['sequence ' .. sequence], 3,
                            label .. ': read,write on ' .. sequence)
        end
        for _, name in ipairs(helper.LUA_CALL) do
            t.assert_equals(seen.granted['lua_call ' .. name], 4,
                            label .. ': execute on ' .. name)
        end
    end

    assert_granted('after the job ran')

    -- A reload re-runs every applier, the credentials one included, and the
    -- job's objects are exactly the ones it could not grant when it first ran.
    helper.reload(c, helper.worker_name(1))
    assert_granted('after config:reload()')

    -- And a role that is taken off the instance and put back keeps them: it
    -- leaves the spaces where they are, and nothing revokes what the config
    -- still grants.
    c:sync(helper.config({autostart = true, drop_worker = 1}))
    helper.reload(c, helper.worker_name(1))
    c:sync(helper.config({autostart = true}))
    helper.reload(c, helper.worker_name(1))
    assert_granted('after the role was stopped and started')
    t.assert_equals(helper.worker_registered(c, helper.worker_name(1)), true,
                    'the restarted role built no job')
end

-- The other half: a config that grants the entry points and not the spaces.
-- The job would connect, start, and fail somewhere inside a superstep with an
-- access error from a remote call, so the role says what is missing instead --
-- the privilege, the object, and where to write it.
g.test_a_missing_space_privilege_is_named = function()
    local c = Cluster:new(helper.config({no_space_privileges = true,
                                         connect_timeout = 1}),
                          helper.server_opts)
    c:start()

    local worker1 = helper.worker_name(1)
    local status = helper.wait_role_state(c, function(cluster)
        return helper.worker_status(cluster, worker1)
    end, 'failed', 30)

    t.assert_str_contains(tostring(status.error),
                          "the user 'pregel_peer' is missing")
    t.assert_str_contains(tostring(status.error),
                          "read,write on space 'data_" .. helper.JOB .. "'")
    t.assert_str_contains(tostring(status.error), 'credentials.roles.pregel')

    -- And an operator sees it without asking the role: it is a warn alert,
    -- and the instance stays up -- raising from apply() at startup would take
    -- the process down over a privilege that might still be on its way.
    local info = helper.config_info(c, worker1)
    local said = false
    for _, alert in ipairs(info.alerts) do
        if alert.message:find('credentials.roles.pregel', 1, true) ~= nil then
            said = true
            t.assert_equals(alert.type, 'warn')
        end
    end
    t.assert_equals(said, true, 'no alert names the missing privilege')
    t.assert_equals(c[worker1]:exec(function()
        return box.info.status
    end), 'running')
end

-------------------------------------------------------------------------------
-- (5): discovery
-------------------------------------------------------------------------------

g.test_the_roles_find_each_other_in_the_cluster_config = function()
    local config = helper.config({autostart = true})

    -- Guard against the test passing for the wrong reason: a roles_cfg with
    -- the URIs written out would prove nothing about discovery, and the whole
    -- suite runs against this one helper.
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
-- business getting one.
--
-- The assertion is on the URI itself rather than on the job running, and has
-- to be: net.box's opts.user wins over a URI's own userinfo, so a URI that did
-- carry 'replicator:secret@' would still authenticate as the pregel user and
-- the job would finish either way (pregel-9vt, M6). What is under test is what
-- peer_uri returns, so that is what is read.
g.test_discovery_does_not_borrow_the_replication_login = function()
    local c = Cluster:new(helper.config({autostart = true}),
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

-- Discovery is the only source of the participants now, so "found nobody" is a
-- configuration error rather than a reason to fall back to a list -- and the
-- message is all the operator gets. The usual cause is a typo in one
-- instance's `name`, which is what this builds: the master runs a job of a
-- different name, so from the worker's side no instance runs the master role
-- for its own job.
--
-- Driven through a reload of an instance whose role is not running, because
-- apply() raises and an error raised at startup exits the process.
g.test_no_master_for_the_job_says_what_to_write = function()
    local c = Cluster:new(helper.config({}), helper.server_opts)
    c:start()

    local worker1 = helper.worker_name(1)
    c:sync(helper.config({drop_worker = 1}))
    helper.reload(c, worker1)

    c:sync(helper.config({master_job = 'other'}))
    local err = reload_error(c, worker1)
    t.assert_str_contains(err,
                          "pregel.roles.worker: no instance in the cluster " ..
                          "config runs pregel.roles.master for job 'maxvalue'")
    -- What to fix, not just what is wrong.
    t.assert_str_contains(err, "whose 'roles' names pregel.roles.master")
    t.assert_str_contains(err, 'name: maxvalue')
end

-- The same from the master's side, where the whole worker list is missing.
g.test_no_worker_for_the_job_says_what_to_write = function()
    local c = Cluster:new(helper.config({}), helper.server_opts)
    c:start()

    c:sync(helper.config({drop_master = true}))
    helper.reload(c, helper.MASTER_NAME)

    c:sync(helper.config({worker_job = 'other'}))
    local err = reload_error(c, helper.MASTER_NAME)
    t.assert_str_contains(err,
                          "pregel.roles.master: no instance in the cluster " ..
                          "config runs pregel.roles.worker for job 'maxvalue'")
    t.assert_str_contains(err, "whose 'roles' names pregel.roles.worker")
end

-- A replicaset written as `roles: [pregel.roles.worker]` puts the role on the
-- replica too, and discovery used to count the replica as a second worker of
-- the job -- so every instance tried to connect to an address that would never
-- serve pregel, and the whole cluster died 30 s later. One worker per
-- replicaset, addressed through the leader.
g.test_discovery_takes_one_worker_per_replicaset = function()
    local config = helper.config({autostart = true, replica_worker = 1})
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
    local c = Cluster:new(helper.config({autostart = true,
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
