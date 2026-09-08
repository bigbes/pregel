--- examples/wcc, driven through a Tarantool 3 cluster config.
--
-- Nine vertices in three components, so the answer is short enough to write
-- out: each component's label is the smallest vertex name in it, and two
-- vertices carry the same label exactly when they are connected.

local t = require('luatest')

local Cluster = require('luatest.cluster')

local helper = require('test.examples.cluster')

local g = t.group('examples.wcc')

local EXAMPLE = 'wcc'
local JOB     = 'wcc'

--- test/fixtures/graphs/small/components3.txt, with every edge both ways:
--
--   1 -- 2 -- 3        4 -- 5        6 -- 7 -- 8 -- 9 -- 6
--
-- The app names vertices by the file's own id, so the labels are the id
-- strings and each component's label is the smallest of them.
local COMPONENTS = {
    ['1'] = {'1', '2', '3'},
    ['4'] = {'4', '5'},
    ['6'] = {'6', '7', '8', '9'},
}

local function run()
    return helper.run(Cluster, {
        name    = EXAMPLE,
        app_cfg = {graph = helper.fixture('small', 'components3.txt')},
    })
end

g.test_every_component_settles_on_its_smallest_name = function()
    local cluster, status = run()
    t.assert_equals(status.error, nil)
    -- The longest component is a four-cycle, so the label has to travel more
    -- than one hop: a single superstep could not have done it.
    t.assert_gt(status.superstep, 1)

    local found = helper.collect_vertices(cluster, {job = JOB})
    t.assert_gt(helper.workers_holding(found), 1)

    local labels = {}
    for name, vertex in pairs(found) do
        labels[name] = vertex.value.label
    end

    local expected = {}
    for label, members in pairs(COMPONENTS) do
        for _, member in ipairs(members) do
            expected[member] = label
        end
    end
    t.assert_equals(labels, expected)
end

g.test_the_components_are_disjoint = function()
    -- The same answer read the other way round: group the vertices by the
    -- label they ended up with and compare the groups. A single label over
    -- everything -- the failure a "smallest name wins" bug produces -- would
    -- satisfy the per-vertex check above only if the expected table were also
    -- wrong, but it would not survive this one.
    local cluster = run()
    local found = helper.collect_vertices(cluster, {job = JOB})

    local groups = {}
    for name, vertex in pairs(found) do
        local label = vertex.value.label
        groups[label] = groups[label] or {}
        table.insert(groups[label], name)
    end
    for _, members in pairs(groups) do
        table.sort(members)
    end

    t.assert_equals(groups, COMPONENTS)
end

g.test_direction_matters_on_a_one_way_graph = function()
    -- Why the committed config.yaml names the '-bi' fixture, and the whole of
    -- the "weakly" in weakly connected components: a message travels along an
    -- out-edge and never back.
    --
    -- components3-oneway.txt is the same nine vertices with one direction of
    -- each edge kept, chosen so the labels run uphill: 2 -> 1 and 3 -> 2, so
    -- nothing ever offers 2 or 3 a smaller label and the first component comes
    -- apart into three. The four-cycle is the exception -- a directed cycle is
    -- still strongly connected -- which is what makes this a test of direction
    -- rather than of the fixture being smaller.
    local cluster = helper.run(Cluster, {
        name    = EXAMPLE,
        app_cfg = {graph = helper.fixture('small', 'components3-oneway.txt')},
    })
    local found = helper.collect_vertices(cluster, {job = JOB})

    local labels = {}
    for name, vertex in pairs(found) do
        labels[name] = vertex.value.label
    end
    t.assert_equals(labels, {
        ['1'] = '1', ['2'] = '2', ['3'] = '3',
        ['4'] = '4', ['5'] = '5',
        ['6'] = '6', ['7'] = '6', ['8'] = '6', ['9'] = '6',
    })
end
