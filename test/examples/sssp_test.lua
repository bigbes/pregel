--- examples/sssp, driven through a Tarantool 3 cluster config.
--
-- The graph is the one the committed config.yaml points at -- the nine-vertex
-- weighted9 fixture, in the Avro form -- so this exercises the same path as
-- `tt start` does, worker-side loading included. The distances below are the
-- ones worked out by hand from the fixture, not read back from a run.

local t = require('luatest')

local Cluster = require('luatest.cluster')

local helper = require('test.examples.cluster')

local g = t.group('examples.sssp')

local EXAMPLE = 'sssp'
local JOB     = 'sssp'
local INF     = math.huge

-- Both spellings the app module accepts for the same two files, so the
-- relative form in config.yaml is what the test resolves too.
local AVRO = {
    vertices = '../../test/fixtures/graphs/small/weighted9/vertices.avro',
    edges    = '../../test/fixtures/graphs/small/weighted9/edges.avro',
}

--- weighted9, as the fixture spells it:
--
--   a -1-> b      b -2-> c      c -1-> d      d -2-> f      f -1-> g
--   a -4-> c      b -5-> d      c -3-> e      e -1-> d      g -2-> h
--                                             e -7-> f      i -1-> a
--
-- From `a`: b is 1; c is 3 (through b, not the direct 4); d is 4 (through c,
-- not b's 6); e is 6; f is 6 (through d, not e's 13); g is 7; h is 9. Nothing
-- points at `i`, so `i` is unreachable however many edges leave it.
local EXPECTED = {
    a = 0, b = 1, c = 3, d = 4, e = 6, f = 6, g = 7, h = 9, i = INF,
}

local function run(source)
    return helper.run(Cluster, {
        name    = EXAMPLE,
        app_cfg = {
            vertices = AVRO.vertices,
            edges    = AVRO.edges,
            source   = source,
        },
    })
end

g.test_distances_from_the_configured_source = function()
    local cluster, status = run('a')

    t.assert_equals(status.error, nil)
    t.assert_gt(status.superstep, 1)

    local found = helper.collect_vertices(cluster, {job = JOB})
    helper.assert_spread(found)

    local distances = {}
    for name, vertex in pairs(found) do
        distances[name] = vertex.value.dist
    end
    t.assert_equals(distances, EXPECTED)
end

g.test_the_source_comes_from_app_cfg = function()
    -- Same graph, different source: `i` reaches everything, and only `i` does.
    local cluster = run('i')
    local found = helper.collect_vertices(cluster, {job = JOB})

    local distances = {}
    for name, vertex in pairs(found) do
        distances[name] = vertex.value.dist
    end
    t.assert_equals(distances, {
        i = 0, a = 1, b = 2, c = 4, d = 5, e = 7, f = 7, g = 8, h = 10,
    })
end

g.test_an_unreachable_vertex_keeps_infinity = function()
    local cluster = run('h')
    local found = helper.collect_vertices(cluster, {job = JOB})

    -- `h` has no out-edges at all, so it is the whole of its own answer.
    t.assert_equals(found['h'].value.dist, 0)
    for name, vertex in pairs(found) do
        if name ~= 'h' then
            t.assert_equals(vertex.value.dist, INF, 'vertex ' .. name)
        end
    end
end
