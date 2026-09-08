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
--
-- `workers` may be left out: the role then reads the cluster config and takes
-- every instance running pregel.roles.worker for a job of this `name`.
--       autostart: false               # run the job as soon as it can
--       user: pregel                   # net.box user for outgoing calls
--       password: secret
--       app_cfg:                       # opaque, handed to the app module
--         graph: '../../data/graph.txt'
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

local log   = require('log')
local fiber = require('fiber')

local master = require('pregel.master')
local common = require('pregel.roles.common')

local utils     = require('pregel.utils')
local error     = utils.error
local xpcall_tb = utils.xpcall_tb

local ROLE = 'pregel.roles.master'

local SPEC = common.spec({
    autostart = {types = {boolean = true}},
})

local state = {
    cfg    = nil,
    master = nil,
    fiber  = nil,
    status = {state = 'idle'},
}

local function set_status(name, extra)
    local rv = {state = name}
    for key, value in pairs(extra or {}) do
        rv[key] = value
    end
    state.status = rv
end

local function validate(cfg)
    common.check_cfg(ROLE, cfg, SPEC)
    common.load_app(ROLE, cfg.app, {'obtain_name'})
end

--- Wait for the workers, load the graph, run the supersteps.
--
-- Everything here can block for as long as the job takes, which is why it is a
-- fiber and not part of apply(): a config apply that waited for a graph
-- algorithm to converge would hold up the whole config framework.
local function autostart_body(instance, app)
    return function()
        fiber.self():name('pregel_master_autostart', {truncate = true})
        local ok, err = xpcall_tb(function()
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
        end)
        if not ok then
            set_status('failed', {
                superstep = instance.superstep_count,
                error     = tostring(err),
            })
            log.error("%s: job '%s' failed: %s", ROLE, instance.name,
                      tostring(err))
        end
    end
end

local function apply(cfg)
    if state.master ~= nil then
        if common.deep_equal(state.cfg, cfg) then
            return
        end
        error('%s: reconfiguration of a running job is not supported, stop ' ..
              'the role first', ROLE)
    end

    common.check_writable(ROLE)

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

    if cfg.autostart then
        state.fiber = fiber.create(autostart_body(instance, app))
    end
    log.info("%s: job '%s' is configured over %d worker(s), autostart %s",
             ROLE, cfg.name, #workers, tostring(cfg.autostart or false))
end

local function stop()
    local instance = state.master
    local worker_fiber = state.fiber
    state.master = nil
    state.fiber = nil
    state.cfg = nil
    set_status('idle')

    if worker_fiber ~= nil and worker_fiber:status() ~= 'dead' then
        -- Cancelling is the only way out: the fiber may be blocked on a
        -- superstep that will not finish once the connections below are gone.
        worker_fiber:cancel()
    end
    if instance == nil then
        return
    end
    instance:stop()
    log.info("%s: job '%s' stopped", ROLE, instance.name)
end

--- The live master object, so an operator can drive the job from a console.
local function get()
    return state.master
end

--- Where the autostarted job got to:
--
--   {state = 'idle'|'loading'|'running'|'done'|'failed',
--    superstep = <number>, error = <string, when failed>}
--
-- It follows the autostart fiber. A job driven by hand through get() moves
-- `superstep` (the master keeps it current) but leaves `state` at 'idle'.
local function status()
    local rv = table.deepcopy(state.status)
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
