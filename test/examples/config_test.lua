--- The committed cluster configs under examples/.
--
-- The per-example tests run each app module through luatest.cluster, but with
-- a config built by test/examples/cluster.lua -- so nothing they do would
-- notice a typo in examples/<name>/config.yaml, a graph path that no longer
-- exists, or a missing lua_call grant. Those are exactly the failures an
-- operator meets first, and they cost a cluster start to find.
--
-- This checks the files themselves: every example is discovered from the
-- directory rather than listed here, so a new one is covered by existing.

local fio  = require('fio')
local yaml = require('yaml')
local t    = require('luatest')

local helper = require('test.examples.cluster')

local g = t.group('examples.config')

local EXAMPLES_DIR = fio.pathjoin(helper.ROOT, 'examples')

--- Every directory under examples/ that has a config.yaml.
local function examples()
    local rv = {}
    for _, entry in ipairs(fio.listdir(EXAMPLES_DIR)) do
        local dir = fio.pathjoin(EXAMPLES_DIR, entry)
        if fio.path.is_dir(dir) and
           fio.path.exists(fio.pathjoin(dir, 'config.yaml')) then
            table.insert(rv, entry)
        end
    end
    table.sort(rv)
    return rv
end

local function read(path)
    local file = assert(io.open(path, 'r'), 'cannot open ' .. path)
    local text = file:read('*a')
    file:close()
    return text
end

local function config_of(example)
    return yaml.decode(read(fio.pathjoin(EXAMPLES_DIR, example, 'config.yaml')))
end

--- Every instance of a config, as {name, roles, roles_cfg, listen}.
local function instances_of(config)
    local rv = {}
    for _, group in pairs(config.groups or {}) do
        for _, replicaset in pairs(group.replicasets or {}) do
            for name, instance in pairs(replicaset.instances or {}) do
                table.insert(rv, {
                    name      = name,
                    roles     = instance.roles or {},
                    roles_cfg = instance.roles_cfg or {},
                    listen    = instance.iproto and instance.iproto.listen,
                })
            end
        end
    end
    table.sort(rv, function(a, b) return a.name < b.name end)
    return rv
end

g.test_every_example_is_a_tt_application = function()
    local found = examples()
    -- Not a fixed list, but there is no point in a sweep over nothing.
    t.assert_gt(#found, 0, 'no examples found under examples/')

    for _, example in ipairs(found) do
        local dir = fio.pathjoin(EXAMPLES_DIR, example)
        for _, file in ipairs({'app.lua', 'instances.yml', 'tt.yaml',
                              'README.md'}) do
            t.assert(fio.path.exists(fio.pathjoin(dir, file)),
                     example .. ' has no ' .. file)
        end
    end
end

g.test_the_instances_file_names_the_configured_instances = function()
    for _, example in ipairs(examples()) do
        local declared = {}
        for _, instance in ipairs(instances_of(config_of(example))) do
            table.insert(declared, instance.name)
        end

        local listed = {}
        local path = fio.pathjoin(EXAMPLES_DIR, example, 'instances.yml')
        for name in pairs(yaml.decode(read(path))) do
            table.insert(listed, name)
        end
        table.sort(listed)

        -- tt starts what instances.yml names; an instance the cluster config
        -- knows about and instances.yml does not is simply never started, and
        -- the job then waits for a worker that will not arrive.
        t.assert_equals(listed, declared, example .. ': instances.yml')
    end
end

g.test_one_master_and_three_workers_on_distinct_ports = function()
    for _, example in ipairs(examples()) do
        local masters, workers, ports = 0, 0, {}
        for _, instance in ipairs(instances_of(config_of(example))) do
            for _, role in ipairs(instance.roles) do
                if role == helper.MASTER_ROLE then
                    masters = masters + 1
                elseif role == helper.WORKER_ROLE then
                    workers = workers + 1
                end
                -- Every role an example turns on must have a roles_cfg, or it
                -- is not configured at all.
                t.assert_not_equals(instance.roles_cfg[role], nil,
                                    example .. ': ' .. instance.name ..
                                    ' runs ' .. role .. ' with no roles_cfg')
            end
            local uri = instance.listen[1].uri
            t.assert_equals(ports[uri], nil,
                            example .. ': ' .. uri .. ' is listened on twice')
            ports[uri] = instance.name
        end
        t.assert_equals(masters, 1, example .. ': master count')
        t.assert_equals(workers, 3, example .. ': worker count')
    end
end

-- The roles are told no login: they look for the one user the credentials
-- section marks with the `pregel` role, and take its password. A config that
-- marks nobody -- or two people -- has no login for the graph traffic and the
-- roles refuse to apply, so this is not cosmetic.
g.test_exactly_one_user_carries_the_pregel_role = function()
    for _, example in ipairs(examples()) do
        local config = config_of(example)
        local marked = {}
        for name, user in pairs(config.credentials.users or {}) do
            for _, role in ipairs(user.roles or {}) do
                if role == helper.CREDENTIALS_ROLE then
                    table.insert(marked, name)
                end
            end
        end
        t.assert_equals(marked, {helper.USER},
                        example .. ': the users carrying the ' ..
                        helper.CREDENTIALS_ROLE .. ' credentials role')
        t.assert_not_equals(
            config.credentials.users[helper.USER].password, nil,
            example .. ': the pregel user has no password')
        -- A role and a user share one namespace, so a config that called them
        -- both `pregel` would die at startup with "User 'pregel' already
        -- exists" -- before any role of ours is even loaded.
        t.assert_equals(config.credentials.users[helper.CREDENTIALS_ROLE], nil,
                        example .. ': a user is named after the credentials ' ..
                        'role')
    end
end

g.test_the_credentials_grant_what_pregel_calls = function()
    for _, example in ipairs(examples()) do
        local config = config_of(example)
        local role = config.credentials.roles[helper.CREDENTIALS_ROLE]
        t.assert_not_equals(role, nil, example .. ': no pregel credentials role')

        local granted = {}
        for _, privilege in ipairs(role.privileges) do
            for _, name in ipairs(privilege.lua_call or {}) do
                granted[name] = true
            end
        end
        -- Nothing else lets one instance reach another: these four names are
        -- the whole protocol, and a config missing one fails at the first
        -- message rather than at apply time.
        for _, name in ipairs(helper.LUA_CALL) do
            t.assert(granted[name],
                     example .. ': no lua_call grant for ' .. name)
        end
    end
end

-- The options the roles used to take and no longer do: the credentials come
-- from the `credentials` section and the participants from `roles`. An example
-- still spelling one of them does not start at all -- an unknown key in
-- roles_cfg is refused by name, which is the whole point of refusing it.
g.test_no_roles_cfg_spells_a_login_or_a_topology = function()
    for _, example in ipairs(examples()) do
        for _, instance in ipairs(instances_of(config_of(example))) do
            for role, cfg in pairs(instance.roles_cfg) do
                for _, key in ipairs({'user', 'password', 'workers',
                                      'master'}) do
                    t.assert_equals(cfg[key], nil,
                                    example .. ': ' .. instance.name .. ' ' ..
                                    'still spells ' .. key .. ' in the ' ..
                                    'roles_cfg of ' .. role)
                end
            end
        end
    end
end

g.test_every_configured_job_agrees_on_its_name_and_app = function()
    for _, example in ipairs(examples()) do
        local jobs, apps = {}, {}
        for _, instance in ipairs(instances_of(config_of(example))) do
            for role, cfg in pairs(instance.roles_cfg) do
                if role == helper.MASTER_ROLE or
                   role == helper.WORKER_ROLE then
                    jobs[cfg.name] = true
                    apps[cfg.app] = true
                end
            end
        end
        -- A worker whose `name` differs from the master's belongs to a
        -- different job, and the two never find each other.
        local names = {}
        for name in pairs(jobs) do
            table.insert(names, name)
        end
        t.assert_equals(#names, 1,
                        example .. ': the instances disagree on the job name (' ..
                        table.concat(names, ', ') .. ')')
        -- 'app' is the module name, resolved by tt through LUA_PATH from the
        -- example's own directory.
        t.assert_equals(apps, {app = true}, example .. ": roles_cfg.app")
    end
end

g.test_every_path_in_app_cfg_exists = function()
    local checked = 0
    for _, example in ipairs(examples()) do
        local dir = fio.pathjoin(EXAMPLES_DIR, example)
        for _, instance in ipairs(instances_of(config_of(example))) do
            for _, cfg in pairs(instance.roles_cfg) do
                for key, value in pairs(cfg.app_cfg or {}) do
                    -- The convention the app modules share: a string that
                    -- looks like a relative path is one, resolved against the
                    -- example's directory.
                    if type(value) == 'string' and value:find('/') then
                        local path = fio.pathjoin(dir, value)
                        t.assert(fio.path.exists(path),
                                 example .. ': app_cfg.' .. key ..
                                 ' points at ' .. value .. ', which is not '
                                 .. 'there')
                        checked = checked + 1
                    end
                end
            end
        end
    end
    -- A sweep that checked nothing would pass just as quietly.
    t.assert_gt(checked, 0, 'no app_cfg paths were checked')
end
