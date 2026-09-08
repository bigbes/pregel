--- Tarantool 3 role: run one pregel worker on this instance.
--
--   roles: [pregel.roles.worker]
--   roles_cfg:
--     pregel.roles.worker:
--       name: maxvalue                 # job name, required
--       app: myapp.pregel              # Lua module name, required
--       master: '127.0.0.1:3301'       # the master's net.box URI
--       workers:                       # every worker's net.box URI,
--         - '127.0.0.1:3302'           # this instance included
--         - '127.0.0.1:3303'
--       pool_size: 1000
--
-- `master` and `workers` may both be left out: the role then reads the cluster
-- config and takes every instance that runs pregel.roles.worker (or
-- pregel.roles.master) for a job of this `name`. That is resolved once, by the
-- apply that creates the job -- adding a worker to the cluster does not move a
-- running one, and cannot: see the note on reconfiguration below.
--       delayed_push: false
--       squash_only: false
--       queue_engine: space            # 'space' or 'table'
--       user: pregel                   # net.box user for outgoing calls
--       password: secret
--       app_cfg:                       # opaque, handed to the app module
--         graph: '../../data/graph.txt'
--         threshold: 5
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
-- Privileges. Everything that reaches a worker is a conn:call() on one of the
-- entry points below, so the cluster config has to let the pregel user call
-- them -- there is no guest universe grant anywhere in this library:
--
--   credentials:
--     users:
--       pregel:
--         password: secret
--         privileges:
--           - permissions: [execute]
--             lua_call:
--               - pregel.worker.deliver
--               - pregel.worker.deliver_batch
--               - pregel.worker.wait
--               - pregel.master.deliver
--
-- The other half -- read/write on this instance's spaces, because a lua_call
-- runs with the caller's privileges and these entry points write -- cannot be
-- spelled in the config: the spaces are named after the job and do not exist
-- when the credentials applier first runs. So the role grants it itself, to
-- the `user` from roles_cfg, right after creating them. A config that sets no
-- `user` gets no such grant: the peers then connect as guest, and giving guest
-- write access to the graph is a decision for the operator, not for this role.

local log = require('log')

local worker = require('pregel.worker')
local common = require('pregel.roles.common')

local error = require('pregel.utils').error

local ROLE = 'pregel.roles.worker'

local SPEC = common.spec({
    master       = {types = {string = true}},
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
}

local function validate(cfg)
    common.check_cfg(ROLE, cfg, SPEC)
    common.load_app(ROLE, cfg.app, {'compute', 'obtain_name'})
end

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

    common.check_writable(ROLE)

    local app = common.load_app(ROLE, cfg.app, {'compute', 'obtain_name'})

    -- Resolved once, here, and not re-read on a later apply: a running job
    -- cannot change its worker list, which is what the check above says.
    local workers = cfg.workers or common.discover_workers(ROLE, cfg.name)
    local master_uri = cfg.master or common.discover_master(ROLE, cfg.name)

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
        user           = cfg.user,
        password       = cfg.password,
        grant_to       = cfg.user,
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
local function stop()
    local instance = state.worker
    local connect = state.connect
    state.worker = nil
    state.cfg = nil
    state.connect = nil
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
local function get()
    return state.worker
end

--- What this instance is doing:
--
--   {state = 'idle'|'connecting'|'running'|'failed', name = <job>,
--    in_progress = <n>, messages = <n>, error = <string, when not connected>}
--
-- 'idle' before apply and after stop; 'connecting' while the job exists but
-- some peer has not answered yet; 'failed' once the role has given up on them
-- (the job object is still there, and a config reload retries).
local function status()
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
