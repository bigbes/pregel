--- Plumbing shared by the two pregel roles.
--
-- The roles themselves are thin: they turn one roles_cfg table into the
-- options table that pregel.worker.new / pregel.master.new already take. What
-- lives here is everything both of them need to do that -- checking the
-- roles_cfg, loading the app module, and (see discover_*) working out who the
-- other participants of the job are when the config did not spell it out.
--
-- Every error raised from here is reported by the config framework as the
-- reason the role failed to validate or apply, so the messages name the role,
-- the option and what was expected.

local log   = require('log')
local clock = require('clock')
local fiber = require('fiber')

local utils       = require('pregel.utils')
local is_callable = utils.is_callable
local error       = utils.error

local M = {}

--- How long a role keeps trying to reach its peers before giving up, in
-- seconds. Generous on purpose: the cost of waiting is a job that has not
-- started yet, and the cost of giving up early is an operator who has to
-- reload the config after fixing whatever was down.
M.CONNECT_TIMEOUT = 300
-- How long one connect attempt waits before the outcome is logged. Not a
-- deadline: the next attempt follows immediately. It exists so a peer that is
-- down is reported every few seconds instead of once, at the very end.
local CONNECT_ATTEMPT = 5
-- The pause between attempts, which matters only for a failure that comes back
-- at once -- rejected credentials, say.
local CONNECT_RETRY   = 1

-------------------------------------------------------------------------------
-- roles_cfg checking
-------------------------------------------------------------------------------

--- An array of non-empty strings, e.g. a list of net.box URIs.
local function check_uri_array(value)
    if #value == 0 then
        return false, 'a non-empty array of URIs'
    end
    local count = 0
    for _ in pairs(value) do
        count = count + 1
    end
    if count ~= #value then
        return false, 'an array of URIs, not a map'
    end
    for _, uri in ipairs(value) do
        if type(uri) ~= 'string' or uri == '' then
            return false, 'an array of non-empty strings'
        end
    end
    return true
end

--- The options both roles accept, and how they are checked.
--
-- `types` is the set of Lua types the value may have; `check` refines that.
-- Anything not listed here is refused by name, because a typo in roles_cfg is
-- otherwise silent -- the config framework validates the shape of `roles_cfg`
-- itself and knows nothing about the keys inside a role's own table.
M.common_spec = {
    name         = {types = {string = true}, required = true},
    app          = {types = {string = true}, required = true},
    -- Opaque to the roles on purpose: only that it is a table is checked. It
    -- is the app module's own configuration -- data paths, thresholds, a
    -- source vertex -- and a role that knew what belonged in it would have to
    -- be changed for every app.
    app_cfg      = {types = {table = true}},
    workers      = {types = {table = true}, check = check_uri_array},
    pool_size    = {
        types = {number = true},
        check = function(v)
            if v <= 0 or v ~= math.floor(v) then
                return false, 'a positive integer'
            end
            return true
        end,
    },
    user         = {types = {string = true}},
    password     = {types = {string = true}},
    -- Seconds the role keeps trying to reach its peers. See M.connector.
    connect_timeout = {
        types = {number = true},
        check = function(v)
            if v <= 0 then
                return false, 'a positive number of seconds'
            end
            return true
        end,
    },
}

--- Build a spec from M.common_spec plus `extra`.
function M.spec(extra)
    local rv = {}
    for key, rule in pairs(M.common_spec) do
        rv[key] = rule
    end
    for key, rule in pairs(extra or {}) do
        rv[key] = rule
    end
    return rv
end

--- Check `cfg` against `spec`, raising the first problem found.
function M.check_cfg(role, cfg, spec)
    if type(cfg) ~= 'table' then
        error("%s: roles_cfg['%s'] must be a table, got %s", role, role,
              type(cfg))
    end

    for key, value in pairs(cfg) do
        local rule = spec[key]
        if rule == nil then
            error("%s: unknown option '%s'", role, tostring(key))
        end
        if not rule.types[type(value)] then
            local expected = {}
            for name in pairs(rule.types) do
                table.insert(expected, name)
            end
            table.sort(expected)
            error("%s: option '%s' must be %s, got %s", role, key,
                  table.concat(expected, ' or '), type(value))
        end
        if rule.check ~= nil then
            local ok, expected = rule.check(value)
            if not ok then
                error("%s: option '%s' must be %s", role, key, expected)
            end
        end
    end

    -- Sorted, so a config missing two required options always names the same
    -- one: pairs() order would make the message depend on the hash of the key.
    local required = {}
    for key, rule in pairs(spec) do
        if rule.required then
            table.insert(required, key)
        end
    end
    table.sort(required)
    for _, key in ipairs(required) do
        if cfg[key] == nil then
            error("%s: option '%s' is required", role, key)
        end
    end
end

-------------------------------------------------------------------------------
-- The app module
-------------------------------------------------------------------------------

--- Something worker.new / master.new accept as a preload: a loader object, or
-- a function returning one.
local function check_preload(role, app_name, key, value)
    if value == nil or type(value) == 'table' or is_callable(value) then
        return
    end
    error("%s: the app module '%s' exports '%s' of type %s, expected " ..
          "function, table or nil", role, app_name, key, type(value))
end

local function check_aggregators(role, app_name, aggregators)
    if aggregators == nil then
        return
    end
    if type(aggregators) ~= 'table' then
        error("%s: the app module '%s' exports 'aggregators' of type %s, " ..
              "expected table or nil", role, app_name, type(aggregators))
    end
    for name, opts in pairs(aggregators) do
        if type(name) ~= 'string' then
            error("%s: the app module '%s' has an aggregator named by a %s, " ..
                  "expected a string", role, app_name, type(name))
        end
        -- '__in_progress' and '__messages' are pregel's own, and
        -- add_aggregator() asserts on a duplicate.
        if name:sub(1, 2) == '__' then
            error("%s: the app module '%s' declares the aggregator '%s': " ..
                  "names starting with '__' are reserved for pregel",
                  role, app_name, name)
        end
        if type(opts) ~= 'table' then
            error("%s: the app module '%s' declares the aggregator '%s' as " ..
                  "a %s, expected a table", role, app_name, name, type(opts))
        end
        for _, key in ipairs({'reduce', 'merge'}) do
            if opts[key] ~= nil and not is_callable(opts[key]) then
                error("%s: the app module '%s' declares the aggregator " ..
                      "'%s' with a non-callable '%s'", role, app_name, name,
                      key)
            end
        end
    end
end

--- require() the app module and check that it exports what the role needs.
--
-- `required` is the list of callables this role cannot run without: the
-- worker needs a compute function, both need obtain_name.
--
-- The module is required rather than only checked for existence, because a
-- syntax error or a missing dependency in it must be reported while the config
-- is being validated -- not later, from a fiber nobody is watching.
function M.load_app(role, app_name, required)
    local ok, app = pcall(require, app_name)
    if not ok then
        error("%s: cannot load the app module '%s': %s", role, app_name,
              tostring(app))
    end
    if type(app) ~= 'table' then
        error("%s: the app module '%s' must return a table, got %s", role,
              app_name, type(app))
    end
    for _, key in ipairs(required) do
        if not is_callable(app[key]) then
            error("%s: the app module '%s' must export a callable '%s'", role,
                  app_name, key)
        end
    end
    if app.combiner ~= nil and not is_callable(app.combiner) then
        error("%s: the app module '%s' exports a non-callable 'combiner'",
              role, app_name)
    end
    check_preload(role, app_name, 'worker_preload', app.worker_preload)
    check_preload(role, app_name, 'master_preload', app.master_preload)
    check_aggregators(role, app_name, app.aggregators)
    return app
end

--- What the app module wants vertex:get_worker_context() to answer.
--
-- A plain value is used as it is, which is what an app that needs no
-- configuration exports. A callable is called with roles_cfg.app_cfg, which is
-- the only way a compute function -- which is handed nothing but the vertex --
-- can reach a threshold or a source vertex named in the cluster config.
--
-- The two forms are told apart by is_callable rather than by an extra option,
-- so an app whose context genuinely is a function has to wrap it in a table.
function M.worker_context(role, app_name, app, app_cfg)
    local context = app.worker_context
    if not is_callable(context) then
        return context
    end
    local ok, rv = pcall(context, app_cfg)
    if not ok then
        error("%s: the app module '%s' failed to build its worker_context " ..
              'from app_cfg: %s', role, app_name, tostring(rv))
    end
    return rv
end

--- Add the app's aggregators to a worker or a master.
--
-- Both sides need the same set under the same names: a worker reports its copy
-- to the master by name, and the master looks it up by name.
function M.add_aggregators(instance, app)
    for name, opts in pairs(app.aggregators or {}) do
        instance:add_aggregator(name, opts)
    end
end

-------------------------------------------------------------------------------
-- Reporting a problem without dying
-------------------------------------------------------------------------------

-- One namespace per role name, created on first use: config only hands out a
-- namespace once, and both roles may live in the same process.
local alert_namespaces = {}

local function alerts_of(role)
    local ns = alert_namespaces[role]
    if ns ~= nil then
        return ns
    end
    local config = require('config')
    if not is_callable(config.new_alerts_namespace) then
        return nil
    end
    local ok, rv = pcall(config.new_alerts_namespace, config, role)
    if not ok then
        return nil
    end
    alert_namespaces[role] = rv
    return rv
end

--- Publish an alert under `key`, replacing whatever was there before.
--
-- This is the whole reason a connection failure is not raised. The config
-- framework gives a role two ways to report: raising from validate()/apply(),
-- which becomes an error alert and, during startup, exits the process; and the
-- role's own alerts namespace, which -- measured on CE 3.9 and EE 3.7 --
-- accepts type = 'warn' only and leaves config:info().status at 'ready'. A
-- peer that is down is not a broken configuration, so it goes here. A broken
-- configuration still raises; see check_cfg and load_app.
function M.alert(role, key, message)
    local ns = alerts_of(role)
    if ns == nil then
        return
    end
    pcall(ns.set, ns, key, {type = 'warn', message = message})
end

--- Withdraw the alert published under `key`.
function M.alert_clear(role, key)
    local ns = alerts_of(role)
    if ns == nil then
        return
    end
    pcall(ns.unset, ns, key)
end

-------------------------------------------------------------------------------
-- Connecting to the peers
-------------------------------------------------------------------------------

local connector_mt = {
    __index = {
        --- Keep trying until the pool is connected or the timeout runs out.
        run = function(self)
            local deadline = clock.monotonic() + self.timeout
            local attempt = 0
            while not self.stopped do
                attempt = attempt + 1
                local left = deadline - clock.monotonic()
                local ok, err = pcall(self.pool.wait_connected, self.pool,
                                      left < CONNECT_ATTEMPT and left or
                                      CONNECT_ATTEMPT)
                if ok then
                    self.state = 'connected'
                    self.error = nil
                    M.alert_clear(self.role, self.key)
                    log.info("%s: job '%s' reached all %d peer(s) after %d " ..
                             'attempt(s)', self.role, self.job,
                             self.pool.bucket_cnt, attempt)
                    if self.on_ready ~= nil then
                        self.on_ready()
                    end
                    return
                end
                self.error = tostring(err)
                if self.stopped then
                    return
                end
                log.warn("%s: job '%s' is not connected yet (attempt %d): %s",
                         self.role, self.job, attempt, self.error)
                if clock.monotonic() >= deadline then
                    self.state = 'failed'
                    local message = string.format(
                        "%s: job '%s' could not reach its peers within %s " ..
                        'second(s): %s', self.role, self.job,
                        tostring(self.timeout), self.error)
                    log.error('%s', message)
                    M.alert(self.role, self.key, message)
                    return
                end
                M.alert(self.role, self.key, string.format(
                    "%s: job '%s' is waiting for its peers: %s", self.role,
                    self.job, self.error))
                fiber.sleep(CONNECT_RETRY)
            end
        end,
        --- Start the fiber. Returns at once.
        start = function(self)
            self.fiber = fiber.create(function()
                fiber.self():name('pregel_connect', {truncate = true})
                local ok, err = pcall(self.run, self)
                if not ok and not self.stopped then
                    self.state = 'failed'
                    self.error = tostring(err)
                    log.error("%s: the connect fiber of job '%s' failed: %s",
                              self.role, self.job, self.error)
                end
            end)
            return self
        end,
        stop = function(self)
            local f = self.fiber
            self.stopped = true
            self.fiber = nil
            if f ~= nil and f:status() ~= 'dead' then
                f:cancel()
            end
            M.alert_clear(self.role, self.key)
        end,
    },
}

--- Wait for a pregel instance's peers in a fiber of its own.
--
-- apply() is called from the config framework's post_apply, which is
-- synchronous: an apply that waits for every peer holds up the instance's
-- entire startup, and an error raised from it at startup is fatal -- the
-- process exits. Measured on a three-worker cluster with one worker left down:
-- every other instance died 30 seconds later, and bringing the missing one up
-- afterwards recovered nothing. So a job whose peers are not all there is a job
-- that waits, visibly (status().state == 'connecting') and noisily (a warn per
-- attempt, and an alert), and never a cluster that cannot start.
--
-- opts.job      -- job name, for the messages
-- opts.pool     -- the mpool to connect (built with connect_async)
-- opts.timeout  -- seconds before giving up (default M.CONNECT_TIMEOUT)
-- opts.on_ready -- called once, in the fiber, when the pool is connected
function M.connector(role, opts)
    return setmetatable({
        role     = role,
        job      = opts.job,
        pool     = opts.pool,
        timeout  = opts.timeout or M.CONNECT_TIMEOUT,
        on_ready = opts.on_ready,
        key      = 'peers',
        state    = 'connecting',
        error    = nil,
        fiber    = nil,
        stopped  = false,
    }, connector_mt)
end

-------------------------------------------------------------------------------
-- Discovery
-------------------------------------------------------------------------------

M.WORKER_ROLE = 'pregel.roles.worker'
M.MASTER_ROLE = 'pregel.roles.master'

--- The URI another instance can be reached at.
--
-- config:instance_uri() answers with a table, not a string: {uri = ..., login
-- = ..., params = ...}, where the login is whatever iproto.advertise.peer
-- carries -- the replication user, in a stock cbuilder config. Only the
-- address is taken from it; who pregel connects as is roles_cfg's `user`, and
-- borrowing the replication login would connect the graph traffic as a user
-- that has no lua_call grant and every reason not to get one.
local function peer_uri(config, instance)
    local uri = config:instance_uri('peer', {instance = instance})
    if type(uri) == 'table' then
        uri = uri.uri
    end
    if type(uri) ~= 'string' then
        return nil
    end
    return uri
end

local function has_role(roles, role)
    for _, name in ipairs(roles or {}) do
        if name == role then
            return true
        end
    end
    return false
end

--- Each replicaset's configured leader, keyed by replicaset name.
--
-- config:cluster_config() is the only public way to it: `leader` is a
-- replicaset-level option and is not part of the instance config schema at
-- all, so config:get('leader', {instance = ...}) answers "[instance_config]
-- leader: No such field in the schema" (measured on CE 3.9 and EE 3.7).
local function leaders_of(config)
    local rv = {}
    if not is_callable(config.cluster_config) then
        return rv
    end
    local ok, cluster = pcall(config.cluster_config, config)
    if not ok or type(cluster) ~= 'table' then
        return rv
    end
    for _, group in pairs(cluster.groups or {}) do
        for name, replicaset in pairs(group.replicasets or {}) do
            rv[name] = replicaset.leader
        end
    end
    return rv
end

--- The one instance of `members` that will be read-write.
--
-- A replicaset is one participant of a job, not one per instance: its replicas
-- carry the role because `roles:` is written at replicaset scope, and they
-- cannot run it (see check_writable). Counting them made every other instance
-- dial an address that would never serve pregel.
--
-- Which instance that is has to be answered from the config, because discovery
-- runs before anything has connected. Two failover modes say so statically;
-- the other two do not, and there the honest answer is to ask the operator.
local function rw_member(role, role_name, job, replicaset, members, config,
                         leaders)
    if #members == 1 then
        return members[1]
    end
    local failover = config:get('replication.failover',
                                {instance = members[1].instance})
    if failover == nil or failover == box.NULL then
        failover = 'off'
    end

    if failover == 'off' then
        local rw = {}
        for _, member in ipairs(members) do
            if config:get('database.mode', {instance = member.instance}) ==
               'rw' then
                table.insert(rw, member)
            end
        end
        if #rw == 1 then
            return rw[1]
        end
        error("%s: replicaset '%s' runs %s for job '%s' on %d instances and " ..
              "%d of them are 'database.mode: rw'; a job takes one worker " ..
              'per replicaset, so name exactly one or list the URIs in ' ..
              'roles_cfg instead', role, replicaset, role_name, job, #members,
              #rw)
    end

    if failover == 'manual' then
        local leader = leaders[replicaset]
        for _, member in ipairs(members) do
            if member.instance == leader then
                return member
            end
        end
        error("%s: replicaset '%s' runs %s for job '%s' on %d instances and " ..
              "its leader (%s) is not one of them; list the URIs in " ..
              'roles_cfg instead', role, replicaset, role_name, job, #members,
              leader == nil and 'unset' or "'" .. tostring(leader) .. "'")
    end

    error("%s: replicaset '%s' runs %s for job '%s' on %d instances under " ..
          "'%s' failover, which names no leader in the config; list the URIs " ..
          'in roles_cfg instead', role, replicaset, role_name, job, #members,
          tostring(failover))
end

--- Every participant of `job` running `role_name`: one per replicaset.
--
-- Returns an array of {instance, uri} ordered by instance name, so two
-- instances reading the same config produce the same list.
--
-- The job name is part of the test on purpose: one cluster can run several
-- pregel jobs, and an instance belongs to the one whose name its own roles_cfg
-- names.
function M.instances_of(role, role_name, job)
    local config = require('config')
    local instances = config:instances()
    local by_replicaset = {}
    local names = {}
    for instance, info in pairs(instances) do
        local roles = config:get('roles', {instance = instance})
        if has_role(roles, role_name) then
            local roles_cfg = config:get('roles_cfg', {instance = instance})
            local cfg = (roles_cfg or {})[role_name]
            if type(cfg) == 'table' and cfg.name == job then
                local replicaset = info.replicaset_name or instance
                if by_replicaset[replicaset] == nil then
                    by_replicaset[replicaset] = {}
                    table.insert(names, replicaset)
                end
                table.insert(by_replicaset[replicaset], {
                    instance = instance,
                    uri      = peer_uri(config, instance),
                })
            end
        end
    end

    local leaders = leaders_of(config)
    local rv = {}
    table.sort(names)
    for _, replicaset in ipairs(names) do
        local members = by_replicaset[replicaset]
        table.sort(members, function(a, b) return a.instance < b.instance end)
        table.insert(rv, rw_member(role, role_name, job, replicaset, members,
                                   config, leaders))
    end
    table.sort(rv, function(a, b) return a.instance < b.instance end)
    return rv
end

--- URIs only, refusing an instance the config gives no address for.
local function uris_of(role, role_name, job)
    local rv = {}
    for _, found in ipairs(M.instances_of(role, role_name, job)) do
        if found.uri == nil then
            error("%s: cannot discover the URI of instance '%s', which runs " ..
                  "%s for job '%s'; set the URIs in roles_cfg instead",
                  role, found.instance, role_name, job)
        end
        table.insert(rv, found.uri)
    end
    return rv
end

--- Every worker of `job`, from the cluster config.
function M.discover_workers(role, job)
    local uris = uris_of(role, M.WORKER_ROLE, job)
    if #uris == 0 then
        error("%s: no instance in the cluster config runs %s for job '%s'; " ..
              "set roles_cfg.workers instead", role, M.WORKER_ROLE, job)
    end
    return uris
end

--- The single master of `job`, from the cluster config.
function M.discover_master(role, job)
    local uris = uris_of(role, M.MASTER_ROLE, job)
    if #uris == 0 then
        error("%s: no instance in the cluster config runs %s for job '%s'; " ..
              "set roles_cfg.master instead", role, M.MASTER_ROLE, job)
    end
    if #uris > 1 then
        error("%s: %d instances in the cluster config run %s for job '%s'; " ..
              "a job has one master", role, #uris, M.MASTER_ROLE, job)
    end
    return uris[1]
end

-------------------------------------------------------------------------------
-- Misc
-------------------------------------------------------------------------------

--- Structural equality, enough for two roles_cfg tables.
function M.deep_equal(a, b)
    if a == b then
        return true
    end
    if type(a) ~= 'table' or type(b) ~= 'table' then
        return false
    end
    for key, value in pairs(a) do
        if not M.deep_equal(value, b[key]) then
            return false
        end
    end
    for key in pairs(b) do
        if a[key] == nil then
            return false
        end
    end
    return true
end

--- May this instance run the role? False on a read-only one.
--
-- Both roles write to the instance: a worker creates its spaces, and both hand
-- out the privileges the other participants need to reach them. Beyond that,
-- an instance that merely follows another one has no business running half a
-- pregel job -- two masters over one set of workers would be worse.
--
-- Inert rather than refused, and that is the point. `roles:` is normally
-- written at replicaset scope, so the replicas of a worker replicaset carry
-- the role whether or not anyone meant them to; raising here made their config
-- unappliable, and at startup an unappliable config exits the process. So the
-- role does nothing, says so once, and the next reload -- after a promotion,
-- say -- picks the job up.
function M.check_writable(role)
    if box.info.ro then
        log.info('%s: the instance is read-only, so this role does nothing ' ..
                 'here; a config reload once the instance is read-write ' ..
                 'starts the job', role)
        return false
    end
    return true
end

return M
