--- roles_cfg checking, which needs no cluster.
--
-- validate() is the only part of a role that runs before anything has been
-- built, so it is where a configuration mistake can still be reported as a
-- configuration mistake. These tests are about the messages as much as the
-- refusals: the config framework shows them to whoever wrote the YAML.

local t = require('luatest')

local worker_role = require('pregel.roles.worker')
local master_role = require('pregel.roles.master')

local g = t.group('integration.roles_validate')

local APP = 'test.apps.maxvalue'

-- Removing a key from the base config, which a plain nil in `extra` cannot
-- express -- pairs() would never see it.
local REMOVE = setmetatable({}, {__tostring = function() return '<remove>' end})

local function worker_cfg(extra)
    local cfg = {
        name    = 'job',
        app     = APP,
        master  = 'unix/:./master.iproto',
        workers = {'unix/:./worker1.iproto'},
    }
    for key, value in pairs(extra or {}) do
        if value == REMOVE then
            cfg[key] = nil
        else
            cfg[key] = value
        end
    end
    return cfg
end

local function assert_refused(role, cfg, message)
    t.assert_error_msg_contains(message, role.validate, cfg)
end

-------------------------------------------------------------------------------

g.test_a_full_worker_config_is_accepted = function()
    worker_role.validate(worker_cfg({
        pool_size    = 100,
        delayed_push = false,
        squash_only  = true,
        queue_engine = 'table',
        user         = 'pregel',
        password     = 'secret',
    }))
end

g.test_the_uri_options_are_optional = function()
    -- Left out, they come from the cluster config instead; validate() cannot
    -- resolve them (there is no cluster here) and must not pretend otherwise.
    worker_role.validate(worker_cfg({master = REMOVE, workers = REMOVE}))
    master_role.validate({name = 'job', app = APP})
end

g.test_name_and_app_are_required = function()
    assert_refused(worker_role, worker_cfg({name = REMOVE}),
                   "pregel.roles.worker: option 'name' is required")
    assert_refused(worker_role, worker_cfg({app = REMOVE}),
                   "pregel.roles.worker: option 'app' is required")
    assert_refused(master_role, {app = APP},
                   "pregel.roles.master: option 'name' is required")
end

-- A typo in roles_cfg is otherwise silent: the config framework validates the
-- shape of roles_cfg and knows nothing about the keys inside a role's table,
-- so an unrecognised one would simply have no effect.
g.test_an_unknown_option_is_refused = function()
    assert_refused(worker_role, worker_cfg({poolsize = 10}),
                   "pregel.roles.worker: unknown option 'poolsize'")
    -- ... including one that belongs to the other role.
    assert_refused(master_role, {name = 'job', app = APP, master = 'x:1'},
                   "pregel.roles.master: unknown option 'master'")
    assert_refused(worker_role, worker_cfg({autostart = true}),
                   "pregel.roles.worker: unknown option 'autostart'")
end

g.test_option_types_are_checked = function()
    assert_refused(worker_role, worker_cfg({name = 42}),
                   "pregel.roles.worker: option 'name' must be string, " ..
                   'got number')
    assert_refused(worker_role, worker_cfg({pool_size = '100'}),
                   "pregel.roles.worker: option 'pool_size' must be number")
    assert_refused(worker_role, worker_cfg({pool_size = 0}),
                   "pregel.roles.worker: option 'pool_size' must be a " ..
                   'positive integer')
    assert_refused(worker_role, worker_cfg({queue_engine = 'sql'}),
                   "pregel.roles.worker: option 'queue_engine' must be " ..
                   "'space' or 'table'")
    assert_refused(worker_role, worker_cfg({delayed_push = 'yes'}),
                   "pregel.roles.worker: option 'delayed_push' must be " ..
                   'boolean')
    assert_refused(master_role, {name = 'job', app = APP, autostart = 'yes'},
                   "pregel.roles.master: option 'autostart' must be boolean")
end

g.test_the_worker_list_must_be_an_array_of_uris = function()
    assert_refused(worker_role, worker_cfg({workers = {}}),
                   "option 'workers' must be a non-empty array of URIs")
    assert_refused(worker_role, worker_cfg({workers = {a = 'x:1'}}),
                   "option 'workers' must be a non-empty array of URIs")
    assert_refused(worker_role, worker_cfg({workers = {'x:1', 42}}),
                   "option 'workers' must be an array of non-empty strings")
end

g.test_a_missing_app_module_is_reported_at_validation = function()
    assert_refused(worker_role, worker_cfg({app = 'no.such.app'}),
                   "pregel.roles.worker: cannot load the app module " ..
                   "'no.such.app'")
end

g.test_the_app_module_must_export_what_the_role_needs = function()
    package.loaded['test.roles_fake_app'] = {obtain_name = function() end}
    assert_refused(worker_role, worker_cfg({app = 'test.roles_fake_app'}),
                   "must export a callable 'compute'")
    -- The master does not compute, so the same module is fine for it.
    master_role.validate({name = 'job', app = 'test.roles_fake_app'})

    package.loaded['test.roles_fake_app'] = {compute = function() end}
    assert_refused(master_role, {name = 'job', app = 'test.roles_fake_app'},
                   "must export a callable 'obtain_name'")

    package.loaded['test.roles_fake_app'] = 'not a table'
    assert_refused(worker_role, worker_cfg({app = 'test.roles_fake_app'}),
                   'must return a table, got string')

    package.loaded['test.roles_fake_app'] = nil
end

-- Every one of these was accepted. An empty name produced spaces called
-- 'data_' and a job logged as ''; an empty master got as far as a net.box URI
-- error inside apply; an empty app reached package.searchpath and came back
-- with 'bad argument #1 to searchpath (string expected, got nil)', which names
-- neither the role nor the option.
g.test_the_identifier_options_must_not_be_empty = function()
    assert_refused(worker_role, worker_cfg({name = ''}),
                   "pregel.roles.worker: option 'name' must be a non-empty " ..
                   'string')
    assert_refused(worker_role, worker_cfg({app = ''}),
                   "pregel.roles.worker: option 'app' must be a non-empty " ..
                   'string')
    assert_refused(worker_role, worker_cfg({master = ''}),
                   "pregel.roles.worker: option 'master' must be a " ..
                   'non-empty string')
    assert_refused(worker_role, worker_cfg({user = ''}),
                   "pregel.roles.worker: option 'user' must be a non-empty " ..
                   'string')
    assert_refused(master_role, {name = '', app = APP},
                   "pregel.roles.master: option 'name' must be a non-empty " ..
                   'string')
end

-- A password with nobody to use it is not a harmless extra: the peers then
-- connect as guest, and the config says in as many words that they should not.
g.test_a_password_without_a_user_is_refused = function()
    assert_refused(worker_role, worker_cfg({password = 'secret'}),
                   "pregel.roles.worker: option 'password' needs a 'user' " ..
                   'to go with it')
    -- The other way round is fine: a user with an empty password is a real
    -- configuration.
    worker_role.validate(worker_cfg({user = 'pregel'}))
end

-- The role's own messages are what config:info().alerts shows to whoever wrote
-- the YAML, and they used to arrive as
-- '/Users/.../pregel/roles/common.lua:96: pregel.roles.worker: ...'.
-- Tarantool's own applier raises with level 0 for exactly this reason.
g.test_the_messages_carry_no_source_position = function()
    t.assert_error_msg_equals(
        "pregel.roles.worker: option 'name' must be string, got number",
        worker_role.validate, worker_cfg({name = 42}))
    t.assert_error_msg_equals(
        "pregel.roles.worker: unknown option 'poolsize'",
        worker_role.validate, worker_cfg({poolsize = 10}))
    t.assert_error_msg_equals(
        "pregel.roles.master: option 'name' is required",
        master_role.validate, {app = APP})
end

g.test_the_app_aggregators_are_checked = function()
    local function app(aggregators)
        package.loaded['test.roles_fake_app'] = {
            compute     = function() end,
            obtain_name = function() end,
            aggregators = aggregators,
        }
        return worker_cfg({app = 'test.roles_fake_app'})
    end

    worker_role.validate(app({ok = {default = 0}}))
    assert_refused(worker_role, app({bad = 1}),
                   "declares the aggregator 'bad' as a number")
    assert_refused(worker_role, app({bad = {reduce = 1}}),
                   "declares the aggregator 'bad' with a non-callable " ..
                   "'reduce'")
    -- pregel keeps '__in_progress' and '__messages' for itself, and
    -- add_aggregator() asserts on a duplicate rather than reporting it.
    assert_refused(worker_role, app({__messages = {default = 0}}),
                   "names starting with '__' are reserved for pregel")
    -- A misspelt aggregator option was ignored, so an app that meant to give
    -- its aggregator a starting value and wrote 'defalt' got the aggregator
    -- silently starting from nil.
    assert_refused(worker_role, app({bad = {default = 0, bogus = 1}}),
                   "declares the aggregator 'bad' with an unknown option " ..
                   "'bogus'")
    -- 'internal' is pregel's own flag for the two aggregators it keeps; an app
    -- has no business setting it.
    assert_refused(worker_role, app({bad = {internal = true}}),
                   "declares the aggregator 'bad' with an unknown option " ..
                   "'internal'")

    package.loaded['test.roles_fake_app'] = nil
end
