--- A pregel cluster of real Tarantool processes, for the integration tests.
--
-- One master and N workers, each a luatest.Server running
-- test/instances/pregel.lua. Everything the tests do to an instance goes
-- through Server:exec(), which ships the function to that process -- so a
-- compute function has to be written inside the exec'd body and cannot close
-- over anything here.
--
-- The point of running these as separate processes is that in-process tests
-- cannot see the parts that only exist between processes: the net.box calls,
-- the lua_call grants, the msgpack round trip, and the sharding actually
-- putting different vertices on different instances.

local fio = require('fio')
local luatest = require('luatest')

local Server = luatest.Server

local cluster = {}
local cluster_mt = {__index = cluster}

-- The checkout the instances load pregel from, so they test this tree rather
-- than an installed rock.
local PREGEL_ROOT = fio.cwd()
local INSTANCE = fio.pathjoin(PREGEL_ROOT, 'test', 'instances', 'pregel.lua')

local counter = 0

-- Server:exec() is eval on the far side, so the harness needs a user that may
-- run it. It is deliberately not `guest`: guest is what the pregel instances
-- connect to each other as, and the tests check that it holds nothing beyond
-- the lua_call grants.
local HARNESS_CREDENTIALS = {user = 'luatest', password = 'luatest'}

local function server_new(alias)
    counter = counter + 1
    return Server:new({
        alias = string.format('%s_%d', alias, counter),
        command = INSTANCE,
        net_box_credentials = HARNESS_CREDENTIALS,
        env = {
            PREGEL_ROOT = PREGEL_ROOT,
            TARANTOOL_LOG_LEVEL = '5',
        },
    })
end

--- Start a master and `worker_count` workers.
function cluster.new(worker_count)
    local self = setmetatable({
        master  = server_new('pmaster'),
        workers = {},
    }, cluster_mt)
    for i = 1, worker_count do
        self.workers[i] = server_new('pworker' .. i)
    end

    -- Server:start() only waits for readiness on its own accord when the
    -- command is luatest's built-in instance script; with a custom one it
    -- returns as soon as the process is forked, and the first exec then fails
    -- with "net_box is not connected".
    self.master:start({wait_until_ready = true})
    for _, worker in ipairs(self.workers) do
        worker:start({wait_until_ready = true})
    end

    self.master_uri = self.master.net_box_uri
    self.worker_uris = {}
    for i, worker in ipairs(self.workers) do
        self.worker_uris[i] = worker.net_box_uri
    end
    return self
end

function cluster:stop()
    for _, worker in ipairs(self.workers) do
        worker:drop()
    end
    self.master:drop()
end

--- Run `fn` on every worker; returns the results in worker order.
function cluster:each_worker(fn, args)
    local rv = {}
    for i, worker in ipairs(self.workers) do
        rv[i] = worker:exec(fn, args)
    end
    return rv
end

--- Create the worker instance called `name` on every worker process.
--
-- `compute_body` is the *source* of the compute function, as a string: the
-- function itself cannot travel through exec, and building it on the far side
-- from source is what keeps the test's compute function readable here.
function cluster:create_workers(name, compute_body, options)
    self:each_worker(function(iname, uris, master_uri, body, opts)
        local pregel_worker = require('pregel.worker')
        opts = opts or {}
        opts.workers = uris
        opts.master = master_uri
        opts.compute = assert(loadstring('return ' .. body))()
        opts.obtain_name = function(vertex) return vertex.name end
        -- The other instances connect as guest, so guest needs read/write on
        -- this instance's spaces as well as the lua_call grants the bootstrap
        -- already made.
        opts.grant_to = 'guest'
        _G.worker_instance = pregel_worker.new(iname, opts)
        return true
    end, {name, self.worker_uris, self.master_uri, compute_body, options})
end

--- Create the master instance, loading `graph_path` through the text loader.
function cluster:create_master(name, graph_path, options)
    return self.master:exec(function(iname, uris, path, opts)
        local pregel_master = require('pregel.master')
        local loader = require('pregel.loader')
        opts = opts or {}
        opts.workers = uris
        opts.obtain_name = function(vertex) return vertex.name end
        if path ~= nil then
            opts.master_preload = function(instance)
                return loader.graph_edges_f(instance, path)
            end
        end
        _G.master_instance = pregel_master.new(iname, opts)
        return true
    end, {name, self.worker_uris, graph_path, options})
end

--- Wait for every worker, push the graph out, and run the supersteps.
--
-- Returns the number of supersteps the master ran.
function cluster:run()
    return self.master:exec(function()
        local m = _G.master_instance
        m:wait_up()
        if m.preload_func ~= nil then
            m:preload()
        end
        return m:start()
    end)
end

--- Every vertex on every worker, as {[name] = {value = ..., halted = ...,
-- edges = ...}}.
--
-- Reading the workers' spaces is the only honest way to check the result: the
-- master holds no graph at all.
function cluster:collect_vertices(name)
    local all = {}
    local per_worker = self:each_worker(function(iname)
        local rv = {}
        for _, tuple in box.space['data_' .. iname]:pairs() do
            rv[tuple[1]] = {
                value  = tuple[3],
                halted = tuple[2],
                edges  = tuple[4],
            }
        end
        return rv
    end, {name})
    for i, vertices in ipairs(per_worker) do
        for vname, vertex in pairs(vertices) do
            assert(all[vname] == nil,
                   'vertex ' .. vname .. ' is on more than one worker')
            vertex.worker = i
            all[vname] = vertex
        end
    end
    return all
end

--- Messages still queued on the workers, summed over the cluster.
function cluster:pending_messages(name)
    local total = 0
    for _, n in ipairs(self:each_worker(function(iname)
        local w = _G.worker_instance
        assert(w ~= nil and w.name == iname, 'worker instance is missing')
        return w.mqueue:len() + w.mqueue_next:len()
    end, {name})) do
        total = total + n
    end
    return total
end

return cluster
