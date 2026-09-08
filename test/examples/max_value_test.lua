--- examples/max-value, driven through a Tarantool 3 cluster config.
--
-- The app module is the one examples/max-value/config.yaml names; only the
-- graph is different -- thirty vertices instead of the fixture's 75879, so the
-- answer can be worked out in the test rather than looked up.

local fio = require('fio')
local t   = require('luatest')

local Cluster = require('luatest.cluster')

local helper  = require('test.examples.cluster')
local fixture = require('test.helpers.graph_fixture')

local g = t.group('examples.max-value')

local EXAMPLE      = 'max-value'
local JOB          = 'maxvalue'
local VERTEX_COUNT = 30

local dir, graph_path, vertices

g.before_all(function()
    -- Outside the checkout and outside luatest's VARDIR: the instances read
    -- this file by the absolute path app_cfg carries, and VARDIR is wiped.
    dir = fio.tempdir()
    graph_path = fio.pathjoin(dir, 'graph.txt')
    local _, generated = fixture.ring(graph_path, VERTEX_COUNT)
    vertices = generated
end)

g.after_all(function()
    if dir ~= nil then
        fio.rmtree(dir)
    end
end)

local function largest_value()
    local rv = 0
    for _, vertex in ipairs(vertices) do
        if vertex.value > rv then
            rv = vertex.value
        end
    end
    return rv
end

g.test_every_vertex_ends_up_holding_the_largest_value = function()
    local expected = largest_value()

    local cluster, status = helper.run(Cluster, {
        name    = EXAMPLE,
        app_cfg = {graph = graph_path},
    })

    -- The graph is a ring, so the largest value has to travel all the way
    -- round: one superstep could not possibly have been enough.
    t.assert_gt(status.superstep, 1)
    t.assert_equals(status.error, nil)
    t.assert_equals(status.name, JOB)

    local found = helper.collect_vertices(cluster, {job = JOB})
    helper.assert_spread(found)

    for _, vertex in ipairs(vertices) do
        -- The app names vertices by the file's own id, not by the person's
        -- name: the real fixture has duplicate names.
        local name = tostring(vertex.id)
        local stored = found[name]
        t.assert_not_equals(stored, nil, 'vertex ' .. name .. ' is missing')
        t.assert_equals(stored.value.value, expected, 'vertex ' .. name)
        t.assert_equals(stored.value.name, vertex.name, 'vertex ' .. name)
        t.assert_equals(stored.halted, true, 'vertex ' .. name)
    end

    -- The app's own aggregator reached both sides: the workers had to be given
    -- it by their role and the master had to merge what they reported.
    local aggregated = cluster[helper.MASTER_NAME]:exec(function(role)
        return require(role).get().aggregators['max_seen']()
    end, {helper.MASTER_ROLE})
    t.assert_equals(aggregated, expected)
end
