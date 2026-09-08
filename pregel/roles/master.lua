--- Tarantool 3 role: run the pregel master for one job on this instance.
--
--   roles: [pregel.roles.master]
--   roles_cfg:
--     pregel.roles.master:
--       name: maxvalue                 # job name, required
--       app: myapp.pregel              # Lua module name, required
--       workers:                       # every worker's net.box URI
--         - '127.0.0.1:3302'
--         - '127.0.0.1:3303'
--       pool_size: 1000
--       autostart: false               # run the job as soon as it can
--       user: pregel                   # net.box user for outgoing calls
--       password: secret
--       app_cfg:                       # opaque, handed to the app module
--         graph: '../../data/graph.txt'
--
-- `workers` may be left out: the role then reads the cluster config and looks
-- for instances running pregel.roles.worker for a job of this `name`. A
-- replicaset is one worker, not each of its instances, and which instance that
-- is comes from the config -- see pregel/roles/worker.lua, which resolves the
-- same list the same way, which is what makes the two agree on the sharding.
--
-- `app_cfg` reaches the app module as the second argument of `master_preload`,
-- which is how a loader learns where the graph is without the app module
-- reading the cluster config itself. The worker role hands the same table to
-- `worker_preload` and to `worker_context`; see pregel/roles/worker.lua.
--
-- The master owns no graph. It drives the superstep loop, so what it needs
-- from the app module is `obtain_name` (to shard what a loader pushes), the
-- optional `master_preload` (a loader run here) and the optional `aggregators`
-- -- which must be the same set the workers declare, since a worker reports
-- its copy to the master by name.
--
-- With `autostart`, a background fiber waits for every worker, loads the graph
-- and runs the supersteps; status() reports where it got to. Without it,
-- nothing happens until an operator drives the job by hand:
--
--   local m = require('pregel.roles.master').get()
--   m:wait_up():preload():start()
--
-- Privileges: as for pregel.roles.worker, and the same credentials snippet
-- covers both -- see the comment at the top of pregel/roles/worker.lua. The
-- master's own entry point is `pregel.master.deliver`, which is how a worker
-- reports its aggregators back.
--
-- @module pregel.roles.master

local log   = require('log')
local fiber = require('fiber')

local master = require('pregel.master')
local common = require('pregel.roles.common')

local utils     = require('pregel.utils')
local traceback = utils.traceback

-- Level 0: the message is a config alert, not a Lua error, and the position of
-- the raise is noise to whoever wrote the YAML. See pregel/roles/common.lua.
local function error(...)
    return utils.error(0, ...)
end

local ROLE = 'pregel.roles.master'

local SPEC = common.spec({
    autostart = {types = {boolean = true}},
})

local state = {
    cfg     = nil,
    master  = nil,
    fiber   = nil,
    -- The fiber that waits for the workers; see common.connector.
    connect = nil,
    -- This instance is a read-only replica, so the role is inert here.
    read_only = false,
    status  = {state = 'idle'},
}

local function set_status(name, extra)
    local rv = {state = name}
    for key, value in pairs(extra or {}) do
        rv[key] = value
    end
    state.status = rv
end

--- Role contract: is this roles_cfg one the role could apply?
--
-- The app module is loaded here, not merely named: a syntax error or a missing
-- dependency in it is a broken configuration, and this is the only moment at
-- which saying so reaches whoever wrote the YAML.
--
-- `compute` is not required of a master, which owns no graph and runs no
-- vertices; `obtain_name` is, because the master shards what a loader pushes.
--
-- @param cfg the roles_cfg table for this role
-- @raise on any problem with the config or the app module
-- @function validate
local function validate(cfg)
    common.check_cfg(ROLE, cfg, SPEC)
    common.load_app(ROLE, cfg.app, {'obtain_name'})
end

--- Has this fiber been cancelled?
--
-- There is no fiber.is_cancelled in Lua -- it exists in the C API only, and
-- calling it raises 'attempt to call field is_cancelled (a nil value)', which
-- inside an xpcall message handler becomes 'error in error handling' and takes
-- the real error with it. fiber.testcancel() is the Lua spelling, and it
-- raises rather than answering, so the question is asked through pcall.
--
-- @return true when the running fiber is under cancellation
local function is_cancelled()
    return not pcall(fiber.testcancel)
end

--- Log a failure with the frames it happened in, the way utils.xpcall_tb does.
--
-- Not xpcall_tb itself, because stop() cancels this fiber and the cancellation
-- arrives here as an error like any other: logging it at error level with a
-- traceback is exactly the "spurious job failed" noise that taking the role off
-- an instance used to produce.
--
-- @param instance the master, named in the message
-- @return an xpcall message handler, which returns the error unchanged
local function autostart_traceback(instance)
    return function(err)
        if is_cancelled() then
            return err
        end
        log.error("%s: job '%s' failed: %s", ROLE, instance.name, tostring(err))
        for _, frame in ipairs(traceback()) do
            local name = frame.name and
                         string.format(" function '%s'", frame.name) or ''
            log.error('[%-4s]%s at <%s:%d>', frame.what, name, frame.file,
                      frame.line)
        end
        return err
    end
end

--- Wait for the workers, load the graph, run the supersteps.
--
-- Everything here can block for as long as the job takes, which is why it is a
-- fiber and not part of apply(): a config apply that waited for a graph
-- algorithm to converge would hold up the whole config framework.
--
-- Started from the connector's on_ready, so by the time it runs the workers
-- are already reachable and wait_up() is a formality rather than the place a
-- missing worker would first be noticed.
--
-- It never raises: the outcome is what status() reports. A failure is
-- 'failed' with the error; a cancellation is not a failure and says so in the
-- log, which is what tells "the role was taken off this instance" from "the
-- job broke".
--
-- @param instance the master to drive
-- @param app the app module, read for worker_preload when there is no
--  master-side loader
-- @return a function to hand to fiber.create
local function autostart_body(instance, app)
    return function()
        fiber.self():name('pregel_master_autostart', {truncate = true})
        local ok, err = xpcall(function()
            set_status('loading', {superstep = 0})
            instance:wait_up()
            if instance.preload_func ~= nil then
                instance:preload()
            elseif app.worker_preload ~= nil then
                -- No master-side loader, but the app knows how to load a
                -- worker's own share: ask every worker to do it.
                instance:preload_on_workers()
            else
                log.info('%s: no preload configured, running over whatever ' ..
                         'the workers already hold', ROLE)
            end
            set_status('running', {superstep = instance.superstep_count})
            local supersteps = instance:start()
            set_status('done', {superstep = supersteps})
            log.info("%s: job '%s' finished after %d superstep(s)", ROLE,
                     instance.name, supersteps)
        end, autostart_traceback(instance))
        if not ok then
            -- Two ways this is not a job failure. stop() cancels this fiber,
            -- and it does so *after* taking the job out of the module state,
            -- so both tests below hold for a cancellation -- and neither the
            -- 'idle' stop() has just set nor the 'loading' of a job that
            -- replaced this one is ours to overwrite.
            if is_cancelled() or state.master ~= instance then
                log.info("%s: job '%s' was stopped while running", ROLE,
                         instance.name)
                return
            end
            set_status('failed', {
                superstep = instance.superstep_count,
                error     = tostring(err),
            })
        end
    end
end

--- Role contract: create the job, and with `autostart` arrange for it to run.
--
-- Called on every config apply and every reload, so the common case is a
-- config that has not changed and this returns at once. A config that *has*
-- changed is refused rather than acted on: a running job's worker list is what
-- its sharding is computed from.
--
-- Returns without waiting for a single worker, and without running anything.
-- It runs inside the config framework's synchronous post_apply, where blocking
-- holds up the instance's whole startup and raising at startup exits the
-- process -- so the waiting is a fiber's job (common.connector), and the job
-- itself starts from that fiber's on_ready.
--
-- @param cfg the roles_cfg table for this role
-- @raise when the config changed under a running job, and on anything
--  discovery or master.new refuses
-- @function apply
local function apply(cfg)
    if state.master ~= nil then
        if common.deep_equal(state.cfg, cfg) then
            return
        end
        error('%s: reconfiguration of a running job is not supported, stop ' ..
              'the role first', ROLE)
    end

    if not common.check_writable(ROLE) then
        state.read_only = true
        set_status('read_only')
        return
    end
    state.read_only = false

    local app = common.load_app(ROLE, cfg.app, {'obtain_name'})

    -- Resolved once, here: a running job cannot change its worker list.
    local workers = cfg.workers or common.discover_workers(ROLE, cfg.name)

    local instance = master.new(cfg.name, {
        workers        = workers,
        obtain_name    = app.obtain_name,
        master_preload = app.master_preload,
        preload_args   = cfg.app_cfg,
        pool_size      = cfg.pool_size,
        user           = cfg.user,
        password       = cfg.password,
        -- apply() must not wait for anyone: it runs inside the config
        -- framework's synchronous post_apply. See common.connector.
        connect_async  = true,
    })
    common.add_aggregators(instance, app)
    if cfg.user ~= nil then
        -- The workers call pregel.master.deliver on this instance to report
        -- their aggregators. The credentials section should already say so;
        -- granting it here as well costs nothing and keeps a config that
        -- forgot it working.
        master.grant(cfg.user)
    end

    state.master = instance
    state.cfg = table.deepcopy(cfg)
    set_status('idle', {superstep = 0})

    -- The job starts once the workers are reachable, not before: wait_up()
    -- would otherwise be the first thing to notice, from inside the superstep
    -- loop, where a missing worker is much harder to read.
    state.connect = common.connector(ROLE, {
        job      = cfg.name,
        pool     = instance.mpool,
        timeout  = cfg.connect_timeout,
        on_ready = function()
            -- stop() may have run while this was connecting; the role is then
            -- no longer ours to start a job for.
            if state.master ~= instance or not cfg.autostart then
                return
            end
            state.fiber = fiber.create(autostart_body(instance, app))
        end,
    }):start()
    log.info("%s: job '%s' is configured over %d worker(s), autostart %s",
             ROLE, cfg.name, #workers, tostring(cfg.autostart or false))
end

--- Role contract: stop the job, whatever it was in the middle of.
--
-- Cancelling the autostart fiber is the only way out of a superstep that will
-- not finish once the connections are gone; the fiber's own error path is what
-- keeps that from being reported as a job failure.
--
-- Safe to call when no job was ever created here.
--
-- @function stop
local function stop()
    local instance = state.master
    local worker_fiber = state.fiber
    local connect = state.connect
    -- Cleared before the cancel below, so the autostart fiber's error path can
    -- tell "the job I belong to is gone" from "the job failed" -- setting the
    -- terminal state first was what left status() at failed/'fiber is
    -- cancelled' after the role was taken off the instance.
    state.master = nil
    state.fiber = nil
    state.cfg = nil
    state.connect = nil
    state.read_only = false

    if connect ~= nil then
        connect:stop()
    end
    if worker_fiber ~= nil and worker_fiber:status() ~= 'dead' then
        -- Cancelling is the only way out: the fiber may be blocked on a
        -- superstep that will not finish once the connections below are gone.
        worker_fiber:cancel()
    end
    set_status('idle')
    if instance == nil then
        return
    end
    instance:stop()
    log.info("%s: job '%s' stopped", ROLE, instance.name)
end

--- The live master object, so an operator can drive the job from a console.
--
-- This is how a job runs without `autostart`:
--
--   require('pregel.roles.master').get():wait_up():preload():start()
--
-- Not part of the role contract. A job driven this way still moves
-- status().superstep, because the master keeps that current, but its `state`
-- stays 'idle' -- only the autostart fiber writes the others.
--
-- @return the master, or nil when the role is idle or inert here
-- @function get
local function get()
    return state.master
end

--- Where the autostarted job got to:
--
--   {state = 'idle'|'read_only'|'connecting'|'loading'|'running'|'done'|
--            'failed',
--    superstep = <number>, error = <string, when failed or connecting>}
--
-- It follows the autostart fiber, except while the workers are still being
-- reached: 'connecting' comes first and nothing can have started before it is
-- over. 'read_only' means the instance is a replica and the role is inert
-- here. A job driven by hand through get() moves `superstep` (the master keeps
-- it current) but leaves `state` at 'idle'.
--
-- Not part of the role contract; it is what an operator reads to find out
-- where a job got to, and 'done' is the only state that means it finished.
--
-- @return a table as above, plus `name` once a job exists
-- @function status
local function status()
    local rv = table.deepcopy(state.status)
    local connect = state.connect
    if connect ~= nil and connect.state ~= 'connected' then
        rv.state = connect.state
        rv.error = connect.error
    end
    if state.master ~= nil then
        rv.name = state.master.name
        rv.superstep = state.master.superstep_count
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
