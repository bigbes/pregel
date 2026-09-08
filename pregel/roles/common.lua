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

local utils       = require('pregel.utils')
local is_callable = utils.is_callable
local error       = utils.error

local M = {}

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

--- Refuse to run on a read-only instance.
--
-- Both roles write to the instance: a worker creates its spaces, and both hand
-- out the privileges the other participants need to reach them. Beyond that,
-- an instance that merely follows another one has no business running half a
-- pregel job -- two masters over one set of workers is worse than a config
-- that refuses to apply.
function M.check_writable(role)
    if box.info.ro then
        error("%s: the instance is read-only; this role needs a read-write " ..
              "instance", role)
    end
end

return M
