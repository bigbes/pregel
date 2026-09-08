local t = require('luatest')
local fio = require('fio')

local cluster = require('test.helpers.cluster')
local graph_fixture = require('test.helpers.graph_fixture')

local g = t.group('integration.max_value')

local VAR = fio.pathjoin(fio.cwd(), 'test', 'var', 'integration')
local VERTEX_COUNT = 50
local WORKER_COUNT = 3

local GRAPH_PATH, VERTICES

-- max-value: every vertex ends up holding the largest value reachable from it.
-- The body travels to the worker processes as source, so it can use nothing
-- from this file.
local MAX_VALUE_COMPUTE = [[
function(self)
    local value = self:get_value().value
    local best = value
    for _, msg in self:pairs_messages() do
        if msg > best then best = msg end
    end
    if self:get_superstep() == 1 or best > value then
        local v = self:get_value()
        self:set_value({id = v.id, name = v.name, value = best})
        for _, dest in self:pairs_edges() do
            self:send_message(dest, best)
        end
    end
    self:vote_halt(true)
end
]]

g.before_all(function()
    fio.rmtree(VAR)
    fio.mktree(VAR)
    -- A ring over the first 50 vertices of the real fixture, so every vertex
    -- can reach every other one and the answer is one number.
    GRAPH_PATH = fio.pathjoin(VAR, 'ring50.txt')
    local _, vertices = graph_fixture.ring(GRAPH_PATH, VERTEX_COUNT)
    VERTICES = vertices
end)

local c

g.after_each(function()
    if c ~= nil then
        c:stop()
        c = nil
    end
end)

local function expected_max()
    local best = VERTICES[1].value
    for _, v in ipairs(VERTICES) do
        if v.value > best then best = v.value end
    end
    return best
end

-------------------------------------------------------------------------------

g.test_max_value_over_a_ring = function()
    c = cluster.new(WORKER_COUNT)
    c:create_workers('maxval', MAX_VALUE_COMPUTE)
    c:create_master('maxval', GRAPH_PATH)

    local supersteps = c:run()

    local vertices = c:collect_vertices('maxval')
    local count = 0
    local best = expected_max()
    for _, v in ipairs(VERTICES) do
        local got = vertices[v.name]
        t.assert_not_equals(got, nil, 'vertex ' .. v.name .. ' is missing')
        t.assert_equals(got.value.value, best, 'vertex ' .. v.name)
        t.assert_equals(got.halted, true, 'vertex ' .. v.name)
        count = count + 1
    end
    t.assert_equals(count, VERTEX_COUNT)

    -- The graph is a 50-ring with one chord, so the maximum needs at most 50
    -- hops and certainly more than one.
    t.assert_ge(supersteps, 2)
    t.assert_le(supersteps, VERTEX_COUNT + 2)

    -- Nothing left undelivered.
    t.assert_equals(c:pending_messages('maxval'), 0)
end

-- The whole point of a cluster: the vertices are actually spread over the
-- worker processes, and every worker holds a share of them.
g.test_graph_is_sharded_across_workers = function()
    c = cluster.new(WORKER_COUNT)
    c:create_workers('sharded', MAX_VALUE_COMPUTE)
    c:create_master('sharded', GRAPH_PATH)
    c:run()

    local vertices = c:collect_vertices('sharded')
    local per_worker = {}
    local total = 0
    for _, v in pairs(vertices) do
        per_worker[v.worker] = (per_worker[v.worker] or 0) + 1
        total = total + 1
    end
    t.assert_equals(total, VERTEX_COUNT)
    for i = 1, WORKER_COUNT do
        t.assert_gt(per_worker[i] or 0, 0,
                    'worker ' .. i .. ' holds no vertices')
    end
end

-- A combiner on the message queues must not change the answer, only the
-- number of messages that travel.
g.test_max_value_with_a_combiner = function()
    c = cluster.new(WORKER_COUNT)
    -- The combiner is a function, so it has to be defined on the far side --
    -- create_workers only carries plain options.
    c:each_worker(function(name, uris, master_uri, body)
        local pregel_worker = require('pregel.worker')
        _G.worker_instance = pregel_worker.new(name, {
            workers = uris,
            master = master_uri,
            compute = assert(loadstring('return ' .. body))(),
            obtain_name = function(vertex) return vertex.name end,
            combiner = function(a, b) return a > b and a or b end,
            squash_only = true,
            grant_to = 'guest',
        })
        return true
    end, {'combined', c.worker_uris, c.master_uri, MAX_VALUE_COMPUTE})
    c:create_master('combined', GRAPH_PATH)

    c:run()

    local vertices = c:collect_vertices('combined')
    local best = expected_max()
    for _, v in ipairs(VERTICES) do
        t.assert_equals(vertices[v.name].value.value, best, 'vertex ' .. v.name)
    end
    t.assert_equals(c:pending_messages('combined'), 0)
end

-------------------------------------------------------------------------------
-- Topology mutation
-------------------------------------------------------------------------------

-- Superstep 1 deletes every edge whose weight is below 2 and adds one vertex.
-- The ring edges all weigh 1 and the single chord weighs 2, so the ring is cut
-- and only the chord survives.
local TOPOLOGY_COMPUTE = [[
function(self)
    if self:get_superstep() == 1 then
        for _, dest, weight in self:pairs_edges() do
            if weight < 2 then
                self:delete_edge(dest)
            end
        end
        if self:get_value().id == 0 then
            self:add_vertex({id = -1, name = 'added-by-compute', value = 0})
        end
    end
    self:vote_halt(true)
end
]]

g.test_topology_mutation_across_the_cluster = function()
    c = cluster.new(WORKER_COUNT)
    c:create_workers('topology', TOPOLOGY_COMPUTE)
    c:create_master('topology', GRAPH_PATH)

    c:run()

    local vertices = c:collect_vertices('topology')

    -- The added vertex exists, on whichever worker owns its name.
    local added = vertices['added-by-compute']
    t.assert_not_equals(added, nil, 'the vertex compute added is missing')
    t.assert_equals(added.value.value, 0)
    t.assert_equals(added.edges, {})

    -- Every weight-1 edge is gone and the one weight-2 chord is still there.
    local remaining = 0
    for name, vertex in pairs(vertices) do
        for _, edge in ipairs(vertex.edges) do
            t.assert_ge(edge[2], 2, 'a cheap edge survived on ' .. name)
            remaining = remaining + 1
        end
    end
    t.assert_equals(remaining, 1, 'only the chord should be left')

    -- 50 original vertices plus the one compute added.
    local count = 0
    for _ in pairs(vertices) do count = count + 1 end
    t.assert_equals(count, VERTEX_COUNT + 1)
end

-------------------------------------------------------------------------------
-- Aggregators
-------------------------------------------------------------------------------

-- Every vertex reports its value; the master's copy is the sum over the whole
-- graph, which is the only value that requires all three workers to have
-- reported.
local AGGREGATOR_COMPUTE = [[
function(self)
    self:set_aggregation('sum', self:get_value().value)
    self:set_aggregation('count', 1)
    self:vote_halt(true)
end
]]

g.test_custom_aggregator_across_the_cluster = function()
    c = cluster.new(WORKER_COUNT)

    c:each_worker(function(name, uris, master_uri, body)
        local pregel_worker = require('pregel.worker')
        local w = pregel_worker.new(name, {
            workers = uris,
            master = master_uri,
            compute = assert(loadstring('return ' .. body))(),
            obtain_name = function(vertex) return vertex.name end,
            grant_to = 'guest',
        })
        local add = function(old, new) return old + new end
        w:add_aggregator('sum', {default = 0, reduce = add, merge = add})
        w:add_aggregator('count', {default = 0, reduce = add, merge = add})
        _G.worker_instance = w
        return true
    end, {'aggr', c.worker_uris, c.master_uri, AGGREGATOR_COMPUTE})

    c:create_master('aggr', GRAPH_PATH)
    c.master:exec(function()
        local add = function(old, new) return old + new end
        local m = _G.master_instance
        m:add_aggregator('sum', {default = 0, reduce = add, merge = add})
        m:add_aggregator('count', {default = 0, reduce = add, merge = add})
    end)

    c:run()

    local totals = c.master:exec(function()
        return {
            sum = _G.master_instance.aggregators['sum'](),
            count = _G.master_instance.aggregators['count'](),
        }
    end)

    local expected_sum = 0
    for _, v in ipairs(VERTICES) do
        expected_sum = expected_sum + v.value
    end
    -- Only the last superstep's values remain: the master resets every
    -- aggregator before the workers report into it, so this is one superstep's
    -- worth, not the running total.
    t.assert_equals(totals.count, VERTEX_COUNT)
    t.assert_equals(totals.sum, expected_sum)
end

-- The test above runs exactly one superstep -- every vertex halts in the first
-- one -- so it cannot see either half of the defect this one is about: a
-- summing aggregator that keeps the master's merged value in the accumulator
-- reports it back, and the master adds it once per worker. Over three
-- supersteps and three workers that turned 50 into thousands.
local COUNTING_COMPUTE = [[
function(self)
    local step = self:get_superstep()
    self:set_aggregation('count', 1)
    local reads = rawget(_G, 'aggr_reads')
    if reads == nil then
        reads = {}
        rawset(_G, 'aggr_reads', reads)
    end
    reads[#reads + 1] = {step = step, read = self:get_aggregation('count')}
    self:vote_halt(step >= 3)
end
]]

g.test_sum_aggregator_over_three_supersteps = function()
    c = cluster.new(WORKER_COUNT)

    c:each_worker(function(name, uris, master_uri, body)
        local pregel_worker = require('pregel.worker')
        local w = pregel_worker.new(name, {
            workers = uris,
            master = master_uri,
            compute = assert(loadstring('return ' .. body))(),
            obtain_name = function(vertex) return vertex.name end,
            grant_to = 'guest',
        })
        local add = function(old, new) return old + new end
        w:add_aggregator('count', {default = 0, reduce = add, merge = add})
        _G.worker_instance = w
        return true
    end, {'counting', c.worker_uris, c.master_uri, COUNTING_COMPUTE})

    c:create_master('counting', GRAPH_PATH)
    c.master:exec(function()
        local add = function(old, new) return old + new end
        _G.master_instance:add_aggregator('count',
                                          {default = 0, reduce = add,
                                           merge = add})
    end)

    local supersteps = c:run()
    t.assert_equals(supersteps, 3)

    local count = c.master:exec(function()
        return _G.master_instance.aggregators['count']()
    end)
    -- One superstep's worth of contributions, merged over the three workers --
    -- not three supersteps' worth, and not three workers' copies of it.
    t.assert_equals(count, VERTEX_COUNT)

    -- What the vertices read: nothing merged yet in superstep 1, and from then
    -- on the whole graph's count -- the same number for every vertex on every
    -- worker, rather than whatever its own shard had reached.
    local per_step = {}
    for _, reads in ipairs(c:each_worker(function()
        return rawget(_G, 'aggr_reads')
    end)) do
        t.assert_not_equals(reads, nil, 'a worker never ran the compute')
        for _, entry in ipairs(reads) do
            per_step[entry.step] = per_step[entry.step] or {}
            per_step[entry.step][entry.read] =
                (per_step[entry.step][entry.read] or 0) + 1
        end
    end
    t.assert_equals(per_step[1], {[0] = VERTEX_COUNT})
    t.assert_equals(per_step[2], {[VERTEX_COUNT] = VERTEX_COUNT})
    t.assert_equals(per_step[3], {[VERTEX_COUNT] = VERTEX_COUNT})
end

-------------------------------------------------------------------------------
-- Privileges
-------------------------------------------------------------------------------

-- guest is the user the instances connect to each other as. It gets execute on
-- lua_call for the four pregel entry points, and read/write on the spaces of
-- the instance running here -- and nothing wider. The 1.6 version gave it
-- execute on universe, which is every function in the process; a cluster that
-- only works because it is over-privileged proves nothing.
g.test_guest_privileges_are_per_object = function()
    c = cluster.new(1)
    c:create_workers('privs', MAX_VALUE_COMPUTE)

    local privs = c.workers[1]:exec(function()
        local rv = {lua_call = {}, space = {}, sequence = {}, universe = {}}
        for _, priv in ipairs(box.schema.user.info('guest')) do
            local kind, object = priv[2], tostring(priv[3])
            if rv[kind] ~= nil then
                rv[kind][object] = priv[1]
            end
        end
        return rv
    end)

    t.assert_equals(privs.lua_call['pregel.worker.deliver'], 'execute')
    t.assert_equals(privs.lua_call['pregel.worker.deliver_batch'], 'execute')
    t.assert_equals(privs.lua_call['pregel.worker.wait'], 'execute')
    t.assert_equals(privs.lua_call['pregel.master.deliver'], 'execute')

    -- A lua_call grant alone is not enough: the call runs with the caller's
    -- privileges and the entry points write to these spaces.
    for _, space in ipairs({'data_privs', 'topology_mutation_privs',
                            'pregel_tube_mqueue_first_privs',
                            'pregel_tube_mqueue_second_privs'}) do
        t.assert_str_contains(privs.space[space] or '', 'write',
                              'guest cannot write ' .. space)
    end

    -- Only this instance's spaces, and nothing on the universe beyond what a
    -- stock Tarantool gives guest.
    for space in pairs(privs.space) do
        t.assert_str_contains(space, 'privs',
                              'guest was granted an unrelated space: ' .. space)
    end
    for object, perms in pairs(privs.universe) do
        t.assert_not_str_contains(perms, 'execute',
            'guest must not hold execute on universe (' .. object .. ')')
    end
end
