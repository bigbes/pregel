--- examples/topology-mutation, driven through a Tarantool 3 cluster config.
--
-- Eight vertices and ten edges with weights 1..9, so at threshold 5 every
-- case the example is about appears at least once: a vertex that keeps one of
-- its edges, vertices that lose all of theirs, an edge sitting exactly on the
-- threshold, and a vertex that had no out-edges to begin with.

local t = require('luatest')

local Cluster = require('luatest.cluster')

local helper = require('test.examples.cluster')

local g = t.group('examples.topology-mutation')

local EXAMPLE   = 'topology-mutation'
local JOB       = 'topology'
local THRESHOLD = 5

--- test/fixtures/graphs/small/weights8.txt:
--
--   a -9-> b   a -2-> c   b -3-> c   b -1-> d   c -7-> d
--   d -5-> e   e -4-> f   f -8-> g   f -6-> h   g -1-> h
--
-- and h has no out-edges at all.
local SURVIVING_EDGES = {
    a = {{'b', 9}},
    b = {},
    c = {{'d', 7}},
    -- Exactly on the threshold, and kept: the rule is "below", not "below or
    -- equal". A test that only used weights well clear of the line would not
    -- notice which one it was.
    d = {{'e', 5}},
    e = {},
    f = {{'g', 8}, {'h', 6}},
    g = {},
    h = {},
}

--- Every vertex that ends up with no out-edges gets one of these.
local MARKERS = {'b:orphan', 'e:orphan', 'g:orphan', 'h:orphan'}

local function run(threshold)
    return helper.run(Cluster, {
        name    = EXAMPLE,
        job     = JOB,
        app_cfg = {
            graph     = helper.fixture('small', 'weights8.txt'),
            threshold = threshold or THRESHOLD,
        },
    })
end

g.test_weak_edges_are_pruned_and_orphans_marked = function()
    local cluster, status = run()
    t.assert_equals(status.error, nil)
    -- One superstep prunes and asks for the markers; the markers themselves
    -- only exist in the second, because add_vertex is applied between
    -- supersteps and the master counts them as work still to do.
    t.assert_equals(status.superstep, 2)

    local found = helper.collect_vertices(cluster, {job = JOB})

    for name, expected in pairs(SURVIVING_EDGES) do
        local vertex = found[name]
        t.assert_not_equals(vertex, nil, 'vertex ' .. name .. ' is missing')
        t.assert_equals(vertex.edges, expected, 'edges of ' .. name)
        t.assert_equals(vertex.halted, true, 'vertex ' .. name)
    end

    for _, marker in ipairs(MARKERS) do
        local vertex = found[marker]
        t.assert_not_equals(vertex, nil, marker .. ' was not added')
        t.assert_equals(vertex.value.orphan_of, marker:sub(1, 1))
        t.assert_equals(vertex.edges, {}, marker .. ' has edges')
        -- Added unhalted, and halted by the superstep that found it there.
        t.assert_equals(vertex.halted, true, marker .. ' is still running')
    end

    -- Eight vertices plus four markers, and nothing else: a marker for a
    -- vertex that kept an edge would show up here.
    local count = 0
    for _ in pairs(found) do
        count = count + 1
    end
    t.assert_equals(count, 12)
end

g.test_a_marker_can_land_on_another_worker = function()
    -- add_vertex routes by obtain_name of the new value, so a marker goes to
    -- whichever worker owns the string '<name>:orphan' -- which has nothing to
    -- do with the worker that asked for it. That is the point of the delayed
    -- mutation queue: the request travels, and the vertex is created where it
    -- belongs.
    local cluster = run()
    local found = helper.collect_vertices(cluster, {job = JOB})

    local elsewhere = 0
    for _, marker in ipairs(MARKERS) do
        if found[marker].worker ~= found[marker:sub(1, 1)].worker then
            elsewhere = elsewhere + 1
        end
    end
    t.assert_gt(elsewhere, 0,
                'every marker happened to land on its own orphan\'s worker')
end

g.test_the_threshold_comes_from_app_cfg = function()
    -- Below every weight in the graph: nothing is pruned, so the only vertex
    -- with no out-edges is the one that never had any.
    local cluster, status = run(1)
    -- Still two supersteps: h is stranded whatever the threshold is.
    t.assert_equals(status.superstep, 2)

    local found = helper.collect_vertices(cluster, {job = JOB})
    t.assert_equals(found['a'].edges, {{'b', 9}, {'c', 2}})
    t.assert_equals(found['g'].edges, {{'h', 1}})
    t.assert_not_equals(found['h:orphan'], nil)
    t.assert_equals(found['b:orphan'], nil)
end

g.test_a_threshold_above_everything_strands_the_whole_graph = function()
    local cluster = run(10)
    local found = helper.collect_vertices(cluster, {job = JOB})

    for name in pairs(SURVIVING_EDGES) do
        t.assert_equals(found[name].edges, {}, 'edges of ' .. name)
        t.assert_not_equals(found[name .. ':orphan'], nil,
                            name .. ' got no marker')
    end
end
