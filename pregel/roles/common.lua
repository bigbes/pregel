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
--
-- @module pregel.roles.common

local log   = require('log')
local clock = require('clock')
local fiber = require('fiber')

local utils       = require('pregel.utils')
local is_callable = utils.is_callable

--- Raise without a source position.
--
-- What the config framework does with these is show them to whoever wrote the
-- YAML -- as a config alert, and in the log -- so a '/Users/.../roles/
-- common.lua:96:' in front of the message is noise about a file that person
-- did not write. Tarantool's own roles applier raises with level 0 for the
-- same reason.
--
-- Shadows the global for the whole module, so every raise below is this one.
--
-- @param ... a format string and its arguments, as for utils.error
-- @raise always
local function error(...)
    return utils.error(0, ...)
end

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

--- A string that is used as an identifier or an address, so '' is not one.
--
-- Every option below was accepted empty: name gave spaces called 'data_' and a
-- job logged as '', master got as far as a net.box URI error inside apply, and
-- app reached package.searchpath, which answered 'bad argument #1 to
-- searchpath (string expected, got nil)' -- naming neither the role nor the
-- option.
--
-- @param value the option's value, already known to be a string
-- @return false and the expected shape when empty, true otherwise
local function check_nonempty(value)
    if value == '' then
        return false, 'a non-empty string'
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
    name         = {types = {string = true}, required = true,
                    check = check_nonempty},
    app          = {types = {string = true}, required = true,
                    check = check_nonempty},
    -- Opaque to the roles on purpose: only that it is a table is checked. It
    -- is the app module's own configuration -- data paths, thresholds, a
    -- source vertex -- and a role that knew what belonged in it would have to
    -- be changed for every app.
    app_cfg      = {types = {table = true}},
    pool_size    = {
        types = {number = true},
        check = function(v)
            if v <= 0 or v ~= math.floor(v) then
                return false, 'a positive integer'
            end
            return true
        end,
    },
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
--
-- `extra` wins on a name they share, and the result is a fresh table, so a
-- role cannot reach M.common_spec and change it for the other one.
--
-- @param extra the role's own options, or nil
-- @return a spec table for M.check_cfg
-- @function spec
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
--
-- Called from validate(), so the config framework refuses a bad roles_cfg
-- before apply() has built anything from it.
--
-- Unknown keys are refused rather than ignored: the framework validates the
-- shape of `roles_cfg` and knows nothing about what is inside a role's own
-- table, so a typo would otherwise be silent.
--
-- @param role the role name, which every message opens with
-- @param cfg the role's roles_cfg table
-- @param spec as built by M.spec
-- @raise on a non-table cfg, an unknown option, a wrong type, a failed
--  check, or a missing required option
-- @function check_cfg
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
-- Credentials
-------------------------------------------------------------------------------

--- The credentials role that marks the user pregel connects as.
--
-- The same trick vshard is integrated with: a storage's login is whatever
-- `iproto.advertise.sharding` names, and the framework checks that user holds
-- the semi-default credentials role 'sharding' (configdata.lua,
-- _instance_sharding). Pregel has no advertise entry of its own, so the role
-- membership is not a check but the whole identification: the pregel user is
-- the one the config marks with this role.
--
-- It is also where the privileges live -- see the header of
-- pregel/roles/worker.lua -- so the one name ties the login and what it may do
-- together, and a config cannot grant one without the other.
M.CREDENTIALS_ROLE = 'pregel'

--- Does `roles` contain M.CREDENTIALS_ROLE, directly or through another role?
--
-- Credentials roles nest (`credentials.roles.<r>.roles`), and a deployment
-- that wraps pregel's role in one of its own is still marking that user.
-- Mirrors vshard's check_sharding_role, minus its 'super' shortcut: 'super'
-- means "may do anything", which is true of an administrator who is emphatically
-- not the user the graph traffic should authenticate as.
--
-- `seen` guards against a config whose roles reference each other in a cycle:
-- the credentials applier refuses one, but this runs before it has had to.
--
-- @param config the config module
-- @param roles an array of credentials role names, or nil
-- @param seen names already visited, for the recursion
-- @return true when the pregel role is in there somewhere
local function has_credentials_role(config, roles, seen)
    seen = seen or {}
    for _, name in pairs(roles or {}) do
        if name == M.CREDENTIALS_ROLE then
            return true
        end
        if not seen[name] then
            seen[name] = true
            local nested = config:get({'credentials', 'roles', name, 'roles'})
            if has_credentials_role(config, nested, seen) then
                return true
            end
        end
    end
    return false
end

--- The user pregel connects to its peers as, and its password.
--
-- Read from the cluster config rather than from roles_cfg, so the credentials
-- are written once, where every other credential in a Tarantool 3 deployment
-- is written, and a password is not repeated in as many roles_cfg blocks as
-- the cluster has instances.
--
-- Every participant resolves this independently and has to arrive at the same
-- answer, which is why "several such users" is refused rather than resolved by
-- some rule: two instances picking different logins would authenticate to each
-- other as users with different privileges, and the failure would show up as a
-- job that connects and then cannot write.
--
-- `config` is a parameter so the resolution can be exercised without a
-- cluster; every caller in the roles passes nothing and gets the real one.
--
-- @param role the role name, which every message opens with
-- @param config the config module (default: the real one)
-- @return the login, and the password
-- @raise when no user carries the credentials role, when more than one does,
--  or when the one that does has no password
-- @function pregel_user
function M.pregel_user(role, config)
    config = config or require('config')
    local users = config:get({'credentials', 'users'}) or {}

    local found = {}
    for name, user in pairs(users) do
        if type(user) == 'table' and
           has_credentials_role(config, user.roles) then
            table.insert(found, name)
        end
    end
    -- Sorted, so a config with two such users always names them in the same
    -- order: pairs() order would make the message depend on the hash of the
    -- key, and two instances would then disagree about what they read.
    table.sort(found)

    if #found == 0 then
        error("%s: no user in the cluster config has the credentials role " ..
              "'%s', so there is no login for pregel to reach its peers " ..
              "with; add 'roles: [%s]' to the user under credentials.users",
              role, M.CREDENTIALS_ROLE, M.CREDENTIALS_ROLE)
    end
    if #found > 1 then
        local names = {}
        for _, name in ipairs(found) do
            table.insert(names, "'" .. name .. "'")
        end
        error("%s: %d users in the cluster config have the credentials role " ..
              "'%s' (%s); exactly one of them is the user pregel connects " ..
              'as, so leave the role on that one alone', role, #found,
              M.CREDENTIALS_ROLE, table.concat(names, ', '))
    end

    local user = found[1]
    local password = config:get({'credentials', 'users', user, 'password'})
    if password == nil or password == box.NULL then
        error("%s: the user '%s' has the credentials role '%s' but no " ..
              'password; set credentials.users.%s.password, which is what ' ..
              'pregel authenticates to its peers with', role, user,
              M.CREDENTIALS_ROLE, user)
    end
    return user, password
end

-------------------------------------------------------------------------------
-- The app module
-------------------------------------------------------------------------------

--- Something worker.new / master.new accept as a preload: a loader object, or
-- a function returning one.
--
-- nil is fine -- an app may load from only one side, or from neither.
--
-- @param role the role name
-- @param app_name the app module's name
-- @param key which export is being checked, for the message
-- @param value the export
-- @raise when it is none of those
local function check_preload(role, app_name, key, value)
    if value == nil or type(value) == 'table' or is_callable(value) then
        return
    end
    error("%s: the app module '%s' exports '%s' of type %s, expected " ..
          "function, table or nil", role, app_name, key, type(value))
end

--- What an app module's aggregator declaration may say. Keep in step with
-- aggregator.new, minus its `internal` flag.
local AGGREGATOR_OPTIONS = {
    default = true,
    reduce  = true,
    merge   = true,
}

--- Check an app module's `aggregators` declaration.
--
-- Strict about the option names, because an aggregator with a misspelt
-- `default` still works -- it just starts from nil, superstep after superstep,
-- and the wrong answer is the only sign.
--
-- @param role the role name
-- @param app_name the app module's name
-- @param aggregators the declaration, or nil
-- @raise on a non-table declaration, a non-string name, a '__' name, a
--  non-table body, a non-callable reduce or merge, or an unknown option
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
        -- A misspelt option was silently ignored, so an app that wrote
        -- 'defalt' got an aggregator quietly starting from nil. 'internal' is
        -- pregel's own flag for the two aggregators it keeps for itself and is
        -- not an app's to set, so it is unknown here on purpose.
        for key in pairs(opts) do
            if not AGGREGATOR_OPTIONS[key] then
                error("%s: the app module '%s' declares the aggregator '%s' " ..
                      "with an unknown option '%s'", role, app_name, name,
                      tostring(key))
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
--
-- Called twice per apply, once from validate() and once from apply(); the
-- second is package.loaded's cached copy, not a second execution.
--
-- @param role the role name
-- @param app_name the module name from roles_cfg.app
-- @param required array of export names that must be callable
-- @return the app module
-- @raise when the module cannot be loaded, does not return a table, is
--  missing one of `required`, or exports a combiner, preload or aggregators
--  declaration of the wrong shape
-- @function load_app
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
--
-- Built once per apply, not per vertex: whatever comes back is shared by every
-- vertex this worker computes, for as long as the job lives.
--
-- @param role the role name
-- @param app_name the app module's name
-- @param app the app module
-- @param app_cfg roles_cfg.app_cfg, the builder's only argument
-- @return the context, or nil when the app declares none
-- @raise when a callable worker_context failed on app_cfg
-- @function worker_context
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
-- to the master by name, and the master looks it up by name. Both roles read
-- the same app module, which is what makes them agree.
--
-- @param instance a worker or a master
-- @param app the app module
-- @raise when a name is already taken -- '__in_progress' and '__messages' are,
--  which is why load_app refuses a '__' prefix outright
-- @function add_aggregators
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

--- This role's alerts namespace, or nil where there is none to be had.
--
-- Everything is pcall'd and a failure answers nil: alerting is how a problem
-- gets reported, so it must never become a problem of its own. An older
-- Tarantool without new_alerts_namespace simply gets no alerts.
--
-- @param role the role name, which is also the namespace name
-- @return the namespace, or nil
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
--
-- @param role the role name
-- @param key the alert's identity; setting the same one again replaces it
-- @param message what to show
-- @function alert
function M.alert(role, key, message)
    local ns = alerts_of(role)
    if ns == nil then
        return
    end
    pcall(ns.set, ns, key, {type = 'warn', message = message})
end

--- Withdraw the alert published under `key`.
--
-- Clearing one that was never set is not an error, so a caller need not
-- remember whether it alerted.
--
-- @param role the role name
-- @param key the alert's identity
-- @function alert_clear
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
        --
        -- The body of the fiber, and it does not raise: giving up leaves
        -- `state` at 'failed' with `error` saying why, and publishes an alert.
        -- Nothing here is fatal to the instance, which is the whole point --
        -- see M.connector.
        --
        -- @function run
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
        --
        -- What apply() calls, and why apply() does not block.
        --
        -- @return self, so the call chains off M.connector
        -- @function start
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
        --- Stop waiting and withdraw the alert.
        --
        -- `stopped` is set before the cancel, so the fiber's own error path
        -- can tell a cancellation from a genuine failure and not report one.
        --
        -- @function stop
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
--
-- Built but not started: call start() on the result. on_ready runs in the
-- connect fiber, so it may block -- the master role starts a whole job from
-- it -- and it must re-check that the role still owns the job, since stop()
-- may have run while this was connecting.
--
-- @param role the role name, for the messages and the alerts namespace
-- @param opts table as above
-- @return the connector
-- @function connector
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
-- = ..., password = ..., params = ...}.
--
-- The login and the password are dropped. They are whatever
-- iproto.advertise.peer carries -- the replication user, in a stock cbuilder
-- config -- and borrowing them would connect the graph traffic as a user that
-- has no lua_call grant and every reason not to get one. Who pregel connects
-- as is roles_cfg's `user`.
--
-- The params are kept, and have to be: they are the listener's transport
-- settings (`transport: ssl` and the ssl_* files), and dropping them made the
-- peers speak plaintext to an SSL listener -- which shows up as
-- 'SSL_write(128)' on the listening side and, on every instance, a job that
-- never connects. net.box takes them as part of the URI argument, so the
-- return value is the {uri = ..., params = ...} table it accepts; a listener
-- without params still yields a plain string.
--
-- @param config the config module
-- @param instance the instance name to look up
-- @return a URI string, a {uri = ..., params = ...} table, or nil when the
--  config gives that instance no address
local function peer_uri(config, instance)
    local uri = config:instance_uri('peer', {instance = instance})
    if type(uri) ~= 'table' then
        return type(uri) == 'string' and uri or nil
    end
    if type(uri.uri) ~= 'string' then
        return nil
    end
    if uri.params == nil then
        return uri.uri
    end
    return {uri = uri.uri, params = uri.params}
end

--- Is `role` in this instance's `roles` list?
--
-- @param roles the instance's roles array, or nil
-- @param role the role name to look for
-- @return true when it is there
local function has_role(roles, role)
    for _, name in ipairs(roles or {}) do
        if name == role then
            return true
        end
    end
    return false
end

--- The whole cluster config document, or nil.
--
-- Nothing else has it. `leader` is a replicaset-level option and is not part
-- of the instance config schema at all, so config:get('leader', {instance =
-- ...}) answers "[instance_config] leader: No such field in the schema" --
-- measured on CE 3.9 and EE 3.7, and there is no scope argument that changes
-- that.
--
-- The public accessor is config:cluster_config(), which EE 3.7 does not have
-- ("attempt to call method 'cluster_config' (a nil value)"). config:_cconfig()
-- is the same document and exists on both, so it is the fallback rather than
-- the first choice: a private method is a thing that can go away, and when it
-- does, discovery says the leader is unset and asks for an explicit worker
-- list instead of guessing wrong.
--
-- @param config the config module
-- @return the cluster config document, or nil when neither accessor works
local function cluster_config(config)
    for _, method in ipairs({'cluster_config', '_cconfig'}) do
        if is_callable(config[method]) then
            local ok, cluster = pcall(config[method], config)
            if ok and type(cluster) == 'table' then
                return cluster
            end
        end
    end
    return nil
end

--- Each replicaset's configured leader, keyed by replicaset name.
--
-- What the config says, not who is actually read-write: discovery runs before
-- anything has connected, so there is nobody to ask. Only 'manual' failover
-- reads this -- see rw_member.
--
-- @param config the config module
-- @return a map of replicaset name to leader instance name; empty when the
--  cluster config could not be reached, and a replicaset with no leader
--  configured is simply absent
local function leaders_of(config)
    local rv = {}
    local cluster = cluster_config(config)
    if cluster == nil then
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
--
-- 'off' reads database.mode and wants exactly one 'rw'; 'manual' reads the
-- replicaset's configured leader. Under 'election' and 'supervised' the config
-- names nobody, so this raises rather than picking one -- guessing would put a
-- worker's whole shard on an instance that cannot serve it. A replicated
-- pregel deployment therefore has to name its leaders in the config; a
-- replicaset of one instance is unaffected and is what the examples use.
--
-- A single-member replicaset short-circuits all of that, which is why a
-- non-replicated cluster never meets any of these messages.
--
-- @param role the role name, for the messages
-- @param role_name the role being discovered, worker or master
-- @param job the job name
-- @param replicaset the replicaset name
-- @param members array of {instance, uri} carrying the role
-- @param config the config module
-- @param leaders as returned by leaders_of
-- @return the one member that will be read-write
-- @raise when the config does not single one out
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
              'per replicaset, so give exactly one instance of that ' ..
              "replicaset 'database.mode: rw'", role, replicaset, role_name,
              job, #members, #rw)
    end

    if failover == 'manual' then
        local leader = leaders[replicaset]
        for _, member in ipairs(members) do
            if member.instance == leader then
                return member
            end
        end
        error("%s: replicaset '%s' runs %s for job '%s' on %d instances and " ..
              "its leader (%s) is not one of them; set the replicaset's " ..
              "'leader' to an instance that runs %s", role, replicaset,
              role_name, job, #members,
              leader == nil and 'unset' or "'" .. tostring(leader) .. "'",
              role_name)
    end

    error("%s: replicaset '%s' runs %s for job '%s' on %d instances under " ..
          "'%s' failover, which names no leader in the config, and pregel " ..
          'resolves its participants from the config alone; run that role on ' ..
          "a replicaset of one instance, or use 'replication.failover: " ..
          "manual' with a 'leader'", role, replicaset, role_name, job,
          #members, tostring(failover))
end

--- Every participant of `job` running `role_name`: one per replicaset.
--
-- Returns an array of {instance, uri} ordered by instance name, so two
-- instances reading the same config produce the same list.
--
-- The job name is part of the test on purpose: one cluster can run several
-- pregel jobs, and an instance belongs to the one whose name its own roles_cfg
-- names.
--
-- Answered entirely from the config, so it works before anything has
-- connected -- which it has to, since this is what produces the addresses to
-- connect to.
--
-- @param role the role name of the caller, for the messages
-- @param role_name the role to look for, M.WORKER_ROLE or M.MASTER_ROLE
-- @param job the job name to match against each instance's roles_cfg
-- @return array of {instance, uri}, ordered by instance name; `uri` may be
--  nil for an instance the config gives no address
-- @raise when a replicaset carrying the role names no single read-write
--  instance
-- @function instances_of
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
--
-- Refused rather than skipped: a silently shorter list is a different sharding
-- from the one the other instances computed, and the job would then disagree
-- with itself about who owns which vertex.
--
-- @param role the role name of the caller
-- @param role_name the role to look for
-- @param job the job name
-- @return array of URIs, in instance-name order
-- @raise when any discovered instance has no address
local function uris_of(role, role_name, job)
    local rv = {}
    for _, found in ipairs(M.instances_of(role, role_name, job)) do
        if found.uri == nil then
            error("%s: instance '%s' runs %s for job '%s' and the cluster " ..
                  'config gives it no address to reach it at; give it an ' ..
                  "'iproto.listen' entry, or an 'iproto.advertise.peer' one",
                  role, found.instance, role_name, job)
        end
        table.insert(rv, found.uri)
    end
    return rv
end

--- Every worker of `job`, from the cluster config.
--
-- The only source of the worker list, and the reason there is no roles_cfg
-- option for it: both roles call this, and both get the same answer out of the
-- same document, which is what lets them agree on the sharding without being
-- told it. A list written per instance was one more thing to keep in step with
-- `roles`, and a job whose master and workers disagreed about who the workers
-- are is a job that shards the graph two ways.
--
-- @param role the role name of the caller
-- @param job the job name
-- @return array of worker URIs, in instance-name order
-- @raise when the cluster config has no worker for that job
-- @function discover_workers
function M.discover_workers(role, job)
    local uris = uris_of(role, M.WORKER_ROLE, job)
    if #uris == 0 then
        error("%s: no instance in the cluster config runs %s for job '%s'; " ..
              "a worker is an instance whose 'roles' names %s and whose " ..
              "roles_cfg for it says \"name: %s\"", role, M.WORKER_ROLE, job,
              M.WORKER_ROLE, job)
    end
    return uris
end

--- The single master of `job`, from the cluster config.
--
-- Only the worker role needs this. A second master is refused rather than
-- picked between: two of them over one set of workers would each drive their
-- own supersteps against the same graph.
--
-- @param role the role name of the caller
-- @param job the job name
-- @return the master's URI
-- @raise when no instance runs the master role for that job, or more than one
--  does
-- @function discover_master
function M.discover_master(role, job)
    local uris = uris_of(role, M.MASTER_ROLE, job)
    if #uris == 0 then
        error("%s: no instance in the cluster config runs %s for job '%s'; " ..
              "the master is an instance whose 'roles' names %s and whose " ..
              "roles_cfg for it says \"name: %s\"", role, M.MASTER_ROLE, job,
              M.MASTER_ROLE, job)
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
--
-- Used to tell "this apply changes nothing" -- the common case, since every
-- config apply and every reload calls apply() -- from a reconfiguration, which
-- a running job refuses. Enough for a config document: no metatables, no
-- cycles, and keys compared by identity.
--
-- @param a first value
-- @param b second value
-- @return true when they are structurally equal
-- @function deep_equal
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
--
-- Read at apply time and not watched afterwards: an instance that becomes
-- read-write later does not start a job by itself.
--
-- @param role the role name, for the message
-- @return false when the instance is read-only, and the role should stop here
-- @function check_writable
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
