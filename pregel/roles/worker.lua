--- Tarantool 3 role: run one pregel worker on this instance.
--
--   roles: [pregel.roles.worker]
--   roles_cfg:
--     pregel.roles.worker:
--       name: maxvalue                 # job name, required
--       app: myapp.pregel              # Lua module name, required
--       pool_size: 1000
--       delayed_push: false
--       squash_only: false
--       queue_engine: space            # 'space' or 'table'
--       app_cfg:                       # opaque, handed to the app module
--         graph: '../../data/graph.txt'
--         threshold: 5
--
-- Who the other participants are is not configured here at all: the role reads
-- the cluster config and takes every instance that runs pregel.roles.worker
-- (or pregel.roles.master) for a job of this `name`. The config says who is in
-- the job exactly once -- in `roles` -- and every participant computes the same
-- list from it, which is what makes them agree on the sharding.
--
-- A *replicaset* is one participant, not each of its instances: `roles:` is
-- written at replicaset scope, so every replica carries the role and none of
-- them can run it. Discovery takes the one instance per replicaset that the
-- config says will be read-write -- the only one carrying the role, the `rw`
-- one under `replication.failover: off`, or the replicaset's `leader` under
-- `manual`. Under election or supervised failover the config names nobody and
-- the role says so rather than guessing.
--
-- That is resolved once, by the apply that creates the job: adding a worker to
-- the cluster does not move a running one, and cannot: see the note on
-- reconfiguration below.
--
-- The app module returns a table:
--
--   {compute = fn(vertex), obtain_name = fn(value) -> string,
--    combiner = fn(a, b) -> c or nil, worker_preload = fn(self, app_cfg)/table/nil,
--    worker_context = any or fn(app_cfg) -> any,
--    aggregators = {<name> = {default, reduce, merge}}}
--
-- `app_cfg` is the one option this role does not interpret: it is checked for
-- being a table and then handed to the app module twice over -- as the second
-- argument of `worker_preload`, and (when `worker_context` is callable) as the
-- argument that builds the context every compute function reads through
-- vertex:get_worker_context(). Those two are the whole channel, because a
-- compute function is handed nothing but its vertex, and an app module that
-- read the config itself would be tied to one deployment.
--
-- Credentials. Who pregel connects to its peers as is not written in
-- roles_cfg: it is the user the cluster config marks with the credentials role
-- `pregel`, and its password is that user's own. One login for the whole job,
-- written where every other credential of a Tarantool 3 deployment is written:
--
--   credentials:
--     roles:
--       pregel:
--         privileges:
--           - permissions: [execute]
--             lua_call:
--               - pregel.worker.deliver
--               - pregel.worker.deliver_batch
--               - pregel.worker.wait
--               - pregel.master.deliver
--     users:
--       pregel_peer:
--         password: secret
--         roles: [pregel]
--
-- The user may not be called `pregel` as well: a credentials role and a user
-- share one namespace, and the applier then dies with "User 'pregel' already
-- exists" before any role is applied.
--
-- Exactly one user must carry the role -- every instance resolves this on its
-- own, and two of them picking different logins would authenticate to each
-- other as users with different privileges.
--
-- Everything that reaches a worker is a conn:call() on one of the entry points
-- above, so that lua_call list is the first half of the privileges; there is
-- no guest universe grant anywhere in this library. The other half is
-- read/write on this instance's spaces, because a lua_call runs with the
-- caller's privileges and these entry points write. Those spaces are named
-- after the job and do not exist when the credentials applier first runs, so
-- the role grants them itself right after creating them.
--
-- @module pregel.roles.worker

local log = require('log')

local worker = require('pregel.worker')
local common = require('pregel.roles.common')

-- Level 0: the message is a config alert, not a Lua error, and the position of
-- the raise is noise to whoever wrote the YAML. See pregel/roles/common.lua.
local utils = require('pregel.utils')
local function error(...)
    return utils.error(0, ...)
end

local ROLE = 'pregel.roles.worker'

local SPEC = common.spec({
    delayed_push = {types = {boolean = true}},
    squash_only  = {types = {boolean = true}},
    queue_engine = {
        types = {string = true},
        check = function(v)
            if v ~= 'space' and v ~= 'table' then
                return false, "'space' or 'table'"
            end
            return true
        end,
    },
})

-- The one job this instance runs, if any. A second job in the same process
-- would need a second copy of this module, which the roles applier will not
-- create -- one role name, one instance of it.
local state = {
    cfg     = nil,
    worker  = nil,
    -- The fiber that waits for the peers; see common.connector.
    connect = nil,
    -- This instance is a read-only replica, so the role is inert here.
    read_only = false,
}

--- Role contract: is this roles_cfg one the role could apply?
--
-- The app module is loaded here, not merely named: a syntax error or a missing
-- dependency in it is a broken configuration, and this is the only moment at
-- which saying so reaches whoever wrote the YAML.
--
-- Runs on every instance carrying the role, read-only replicas included --
-- validate() has no view of that, and check_writable belongs to apply().
--
-- @param cfg the roles_cfg table for this role
-- @raise on any problem with the config or the app module
-- @function validate
local function validate(cfg)
    common.check_cfg(ROLE, cfg, SPEC)
    common.load_app(ROLE, cfg.app, {'compute', 'obtain_name'})
end

--- Role contract: create the job, or do nothing if it already exists.
--
-- Called on every config apply and every reload, so the common case is a
-- config that has not changed and this returns at once. A config that *has*
-- changed is refused rather than acted on: a running job's worker list is what
-- its sharding is computed from.
--
-- Returns without waiting for a single peer. It runs inside the config
-- framework's synchronous post_apply, where blocking holds up the instance's
-- whole startup and raising at startup exits the process -- so the waiting is
-- a fiber's job (common.connector) and a peer that is down is an alert.
--
-- @param cfg the roles_cfg table for this role
-- @raise when the config changed under a running job, and on anything
--  discovery or worker.new refuses
-- @function apply
local function apply(cfg)
    if state.worker ~= nil then
        if common.deep_equal(state.cfg, cfg) then
            -- Every config apply and every reload lands here; only a changed
            -- config is interesting.
            return
        end
        error('%s: reconfiguration of a running job is not supported, stop ' ..
              'the role first', ROLE)
    end

    if not common.check_writable(ROLE) then
        state.read_only = true
        return
    end
    state.read_only = false

    local app = common.load_app(ROLE, cfg.app, {'compute', 'obtain_name'})

    -- Resolved once, here, and not re-read on a later apply: a running job
    -- cannot change its worker list, which is what the check above says.
    local workers = common.discover_workers(ROLE, cfg.name)
    local master_uri = common.discover_master(ROLE, cfg.name)
    -- Who this instance connects to its peers as: the cluster config's own
    -- credentials, not a login repeated in every instance's roles_cfg.
    local user, password = common.pregel_user(ROLE)

    local instance = worker.new(cfg.name, {
        workers        = workers,
        master         = master_uri,
        compute        = app.compute,
        combiner       = app.combiner,
        obtain_name    = app.obtain_name,
        worker_context = common.worker_context(ROLE, cfg.app, app,
                                               cfg.app_cfg),
        worker_preload = app.worker_preload,
        preload_args   = cfg.app_cfg,
        squash_only    = cfg.squash_only,
        queue_engine   = cfg.queue_engine,
        pool_size      = cfg.pool_size,
        delayed_push   = cfg.delayed_push,
        user           = user,
        password       = password,
        grant_to       = user,
        -- apply() must not wait for anyone: it runs inside the config
        -- framework's synchronous post_apply. See common.connector.
        connect_async  = true,
    })
    common.add_aggregators(instance, app)

    state.worker = instance
    state.cfg = table.deepcopy(cfg)
    state.connect = common.connector(ROLE, {
        job     = cfg.name,
        pool    = instance.mpool,
        timeout = cfg.connect_timeout,
    }):start()
    -- The URIs are not logged: roles_cfg may spell one as
    -- 'user:password@host:port', and a log line is the wrong place for that.
    log.info("%s: job '%s' is running over %d worker(s)", ROLE, cfg.name,
             #workers)
end

--- Tear the job down: pusher fibers, waitpool fibers, net.box connections and
-- this job's entry in the worker registry.
--
-- What is deliberately left alone: the spaces (the shard survives a restart,
-- which is the point of storing it) and `_G.pregel.worker` (the registry
-- functions are published when pregel.worker is first required, and a require
-- of an already-loaded module does not run it again -- removing them would
-- leave nothing to re-register when the role comes back).
--
-- Role contract, and safe to call when no job was ever created here.
--
-- @function stop
local function stop()
    local instance = state.worker
    local connect = state.connect
    state.worker = nil
    state.cfg = nil
    state.connect = nil
    state.read_only = false
    if connect ~= nil then
        connect:stop()
    end
    if instance == nil then
        return
    end
    instance:stop()
    log.info("%s: job '%s' stopped", ROLE, instance.name)
end

--- The live worker object, for a console session that wants to look at it.
--
-- Not part of the role contract, and not the way to drive a worker: the master
-- does that. It is here to be read.
--
-- @return the worker, or nil when the role is idle or inert here
-- @function get
local function get()
    return state.worker
end

--- What this instance is doing:
--
--   {state = 'idle'|'read_only'|'connecting'|'running'|'failed',
--    name = <job>, in_progress = <n>, messages = <n>,
--    error = <string, when not connected>}
--
-- 'idle' before apply and after stop; 'read_only' when the instance is a
-- replica and the role is therefore inert here; 'connecting' while the job
-- exists but some peer has not answered yet; 'failed' once the role has given
-- up on them (the job object is still there, and a config reload retries).
--
-- The counts are this shard's, not the job's: every worker answers for itself.
--
-- Not part of the role contract; it is what an operator reads to find out
-- whether a job that is not progressing is stuck on a peer or simply working.
--
-- @return a table as above
-- @function status
local function status()
    if state.read_only then
        return {state = 'read_only'}
    end
    if state.worker == nil then
        return {state = 'idle'}
    end
    local rv = {
        state       = 'running',
        name        = state.worker.name,
        in_progress = state.worker.in_progress,
        messages    = state.worker.mqueue:len(),
    }
    local connect = state.connect
    if connect ~= nil and connect.state ~= 'connected' then
        rv.state = connect.state
        rv.error = connect.error
    end
    return rv
end

return {
    validate = validate,
    apply    = apply,
    stop     = stop,
    -- not part of the role contract
    get      = get,
    status   = status,
}
