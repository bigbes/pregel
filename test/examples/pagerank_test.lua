--- examples/pagerank, driven through a Tarantool 3 cluster config.
--
-- The reference answer is computed here, by a plain-Lua power iteration over
-- the same graph, and the two are compared to 1e-6. That is the point of the
-- test: a distributed PageRank agrees with the textbook one or it is wrong,
-- and an assertion against numbers read back from an earlier run of the same
-- code would say nothing at all.

local t = require('luatest')

local Cluster = require('luatest.cluster')

local helper = require('test.examples.cluster')

local g = t.group('examples.pagerank')

local EXAMPLE    = 'pagerank'
local JOB        = 'pagerank'
local DAMPING    = 0.85
local ITERATIONS = 30
local EPSILON    = 1e-6

local AVRO = {
    vertices = '../../test/fixtures/graphs/small/pagerank6/vertices.avro',
    edges    = '../../test/fixtures/graphs/small/pagerank6/edges.avro',
}

--- test/fixtures/graphs/small/pagerank6.txt, as an adjacency list.
--
-- F has no out-edges: it is the dangling vertex whose rank the `dangling`
-- aggregator has to spread over the graph instead of letting it leak away.
local GRAPH = {
    A = {'B', 'C'},
    B = {'C'},
    C = {'A'},
    D = {'A', 'B', 'C'},
    E = {'D', 'F'},
    F = {},
}

--- PageRank the ordinary way, so the cluster has something to be wrong about.
--
--   r_0[v] = 1/N
--   r_k[v] = (1-d)/N + d * (dangling_{k-1}/N + sum over u->v of r_{k-1}[u]/deg(u))
local function power_iteration(iterations, damping)
    local names = {}
    for name in pairs(GRAPH) do
        table.insert(names, name)
    end
    table.sort(names)
    local n = #names

    local rank = {}
    for _, name in ipairs(names) do
        rank[name] = 1 / n
    end

    for _ = 1, iterations do
        local dangling = 0
        for _, name in ipairs(names) do
            if #GRAPH[name] == 0 then
                dangling = dangling + rank[name]
            end
        end

        local next_rank = {}
        for _, name in ipairs(names) do
            next_rank[name] = (1 - damping) / n + damping * (dangling / n)
        end
        for _, source in ipairs(names) do
            local out = GRAPH[source]
            for _, destination in ipairs(out) do
                next_rank[destination] = next_rank[destination]
                                       + damping * rank[source] / #out
            end
        end
        rank = next_rank
    end
    return rank, n
end

local function run(iterations)
    return helper.run(Cluster, {
        name    = EXAMPLE,
        app_cfg = {
            vertices   = AVRO.vertices,
            edges      = AVRO.edges,
            damping    = DAMPING,
            iterations = iterations,
        },
    })
end

g.test_ranks_match_a_plain_lua_power_iteration = function()
    local expected = power_iteration(ITERATIONS, DAMPING)

    local cluster, status = run(ITERATIONS)
    t.assert_equals(status.error, nil)
    -- One superstep to count the vertices, one to lay down 1/N, then the
    -- iterations themselves.
    t.assert_equals(status.superstep, ITERATIONS + 2)

    local found = helper.collect_vertices(cluster, {job = JOB})
    -- Six vertices over three shards need not touch all three, but a run that
    -- put the whole graph on one instance would prove nothing about the
    -- aggregators, which are merged across workers.
    t.assert_gt(helper.workers_holding(found), 1)

    local total = 0
    for name, want in pairs(expected) do
        local stored = found[name]
        t.assert_not_equals(stored, nil, 'vertex ' .. name .. ' is missing')
        t.assert_almost_equals(stored.value.rank, want, EPSILON,
                               'rank of ' .. name)
        total = total + stored.value.rank
    end

    -- The dangling mass is redistributed rather than dropped, so the ranks
    -- still sum to one. Without that they would leak on every iteration.
    t.assert_almost_equals(total, 1, EPSILON, 'ranks sum to one')
end

g.test_one_iteration_is_one_step_of_the_reference = function()
    -- Fewer supersteps, so a bug that only shows after convergence has
    -- nowhere to hide: after a single iteration the ranks are still visibly
    -- unequal and every term of the formula matters.
    local expected = power_iteration(1, DAMPING)

    local cluster, status = run(1)
    t.assert_equals(status.superstep, 3)

    local found = helper.collect_vertices(cluster, {job = JOB})
    for name, want in pairs(expected) do
        t.assert_almost_equals(found[name].value.rank, want, EPSILON,
                               'rank of ' .. name)
    end
end

g.test_the_count_aggregator_finds_the_whole_graph = function()
    local cluster = run(1)
    -- Read from the master, which merged one number out of each worker: six
    -- vertices over three shards.
    local count = cluster[helper.MASTER_NAME]:exec(function(role)
        return require(role).get().aggregators['count']()
    end, {helper.MASTER_ROLE})
    t.assert_equals(count, 6)
end
