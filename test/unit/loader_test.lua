local t = require('luatest')
local fio = require('fio')

local graph_fixture = require('test.helpers.graph_fixture')
local loader = require('pregel.loader')

local g = t.group('loader')

local VAR = fio.pathjoin(fio.cwd(), 'test', 'var', 'loader')

g.before_all(function()
    fio.rmtree(VAR)
    fio.mktree(VAR)
end)

--- An instance stand-in that records every put instead of sending it.
local function recorder()
    local self = {puts = {}, routed = {}, flushed = 0}
    local bucket = {
        put = function(_, msg, args)
            table.insert(self.puts, {msg = msg, args = args})
        end
    }
    self.mpool = {
        by_id = function(_, name)
            table.insert(self.routed, name)
            return bucket
        end,
        flush = function()
            self.flushed = self.flushed + 1
        end,
    }
    -- The loader's vertex records are {id, name, value}; pregel names vertices
    -- by whatever obtain_name returns.
    self.obtain_name = function(vertex) return vertex.name end
    return self
end

local function puts_of(rec, msg)
    local rv = {}
    for _, put in ipairs(rec.puts) do
        if put.msg == msg then
            table.insert(rv, put.args)
        end
    end
    return rv
end

local seq = 0
local function fresh_path()
    seq = seq + 1
    return fio.pathjoin(VAR, string.format('graph_%03d.txt', seq))
end

-------------------------------------------------------------------------------
-- loader_methods
-------------------------------------------------------------------------------

-- Defect: store_edge built '{src {dest, value}}', which Lua parses as a call
-- of the string src, so it raised "attempt to call a string value" and no
-- single edge could ever be stored.
g.test_store_edge_emits_one_edge = function()
    local rec = recorder()
    local l = loader.new(rec, function() end)
    l:store_edge('alice', 'bob', 7)
    t.assert_equals(rec.puts, {
        {msg = 'edge.store', args = {'alice', {{'bob', 7}}}}
    })
    -- Addressed to the bucket owning the source.
    t.assert_equals(rec.routed, {'alice'})
end

g.test_store_vertex_returns_the_name = function()
    local rec = recorder()
    local l = loader.new(rec, function() end)
    local id = l:store_vertex({name = 'alice', value = 1})
    t.assert_equals(id, 'alice')
    t.assert_equals(rec.puts, {
        {msg = 'vertex.store', args = {name = 'alice', value = 1}}
    })
end

g.test_store_edges_batch = function()
    local rec = recorder()
    local l = loader.new(rec, function() end)
    l:store_edges_batch('alice', {{'bob', 1}, {'carol', 2}})
    t.assert_equals(rec.puts, {
        {msg = 'edge.store', args = {'alice', {{'bob', 1}, {'carol', 2}}}}
    })
end

-- Defect: store_vertex_edges handed store_edges_batch the whole vertex value
-- where a source *name* belongs, so the batch was routed by a table and
-- arrived naming a source no worker could find.
g.test_store_vertex_edges_uses_the_name_as_source = function()
    local rec = recorder()
    local l = loader.new(rec, function() end)
    local id = l:store_vertex_edges({name = 'alice', value = 1},
                                    {{'bob', 1}})
    t.assert_equals(id, 'alice')
    t.assert_equals(rec.puts, {
        {msg = 'vertex.store', args = {name = 'alice', value = 1}},
        {msg = 'edge.store', args = {'alice', {{'bob', 1}}}},
    })
    t.assert_equals(rec.routed, {'alice', 'alice'})
end

g.test_flush_reaches_the_mpool = function()
    local rec = recorder()
    local l = loader.new(rec, function() end)
    l:flush()
    t.assert_equals(rec.flushed, 1)
end

g.test_loader_new_rejects_a_non_callable = function()
    t.assert_error_msg_contains('options.loader must be callable', function()
        loader.new(recorder(), 42)
    end)
end

-------------------------------------------------------------------------------
-- graph_edges_f, against slices of the real fixture
-------------------------------------------------------------------------------

g.test_loads_vertices_and_edges = function()
    local path, vertices, edges = graph_fixture.ring(fresh_path(), 20)
    local rec = recorder()
    local l = loader.graph_edges_f(rec, path)
    l()

    local stored = puts_of(rec, 'vertex.store')
    t.assert_equals(#stored, 20)
    for i, vertex in ipairs(stored) do
        t.assert_equals(vertex.id, vertices[i].id)
        t.assert_equals(vertex.name, vertices[i].name)
        t.assert_equals(vertex.value, vertices[i].value)
    end

    -- Every edge arrives exactly once, translated to vertex names.
    local by_id = {}
    for _, v in ipairs(vertices) do by_id[v.id] = v.name end
    local seen = {}
    for _, args in ipairs(puts_of(rec, 'edge.store')) do
        for _, edge in ipairs(args[2]) do
            table.insert(seen, {args[1], edge[1], edge[2]})
        end
    end
    t.assert_equals(#seen, #edges)

    local expected = {}
    for _, e in ipairs(edges) do
        table.insert(expected, {by_id[e[1]], by_id[e[2]], e[3]})
    end
    local function sorter(a, b)
        if a[1] ~= b[1] then return a[1] < b[1] end
        return a[2] < b[2]
    end
    table.sort(seen, sorter)
    table.sort(expected, sorter)
    t.assert_equals(seen, expected)
end

-- Names in the real fixture contain spaces, and the value is separated from
-- the quoted name by one space -- the parser has to keep the whole name.
g.test_vertex_names_with_spaces_survive = function()
    local path, vertices = graph_fixture.ring(fresh_path(), 5)
    local rec = recorder()
    loader.graph_edges_f(rec, path)()
    local stored = puts_of(rec, 'vertex.store')
    t.assert_str_contains(vertices[1].name, ' ')
    t.assert_equals(stored[1].name, vertices[1].name)
end

-- Edges of one source travel as one batch, which is the whole point of
-- grouping them; and the batch names the source it actually belongs to.
g.test_edges_are_batched_per_source = function()
    local path = fresh_path()
    local vertices = graph_fixture.read_vertices(4)
    local id = {}
    for i, v in ipairs(vertices) do id[i] = v.id end
    graph_fixture.write(path, vertices, {
        {id[1], id[2], 1},
        {id[1], id[3], 2},
        {id[1], id[4], 3},
        {id[2], id[3], 4},
    })

    local rec = recorder()
    loader.graph_edges_f(rec, path)()

    local batches = puts_of(rec, 'edge.store')
    t.assert_equals(#batches, 2)
    -- Defect: on rolling over to a new source the flush stored the batch under
    -- the *new* line's source rather than the one it had accumulated, so every
    -- batch but the last was filed against the wrong vertex.
    t.assert_equals(batches[1][1], vertices[1].name)
    t.assert_equals(#batches[1][2], 3)
    t.assert_equals(batches[2][1], vertices[2].name)
    t.assert_equals(#batches[2][2], 1)
end

-- Defect: the last batch was only ever flushed by the arrival of a different
-- source, so the final source's edges were dropped outright.
g.test_the_last_batch_is_flushed = function()
    local path = fresh_path()
    local vertices = graph_fixture.read_vertices(3)
    graph_fixture.write(path, vertices, {
        {vertices[1].id, vertices[2].id, 1},
        {vertices[3].id, vertices[1].id, 9},
    })
    local rec = recorder()
    loader.graph_edges_f(rec, path)()

    local batches = puts_of(rec, 'edge.store')
    t.assert_equals(#batches, 2)
    t.assert_equals(batches[2][1], vertices[3].name)
    t.assert_equals(batches[2][2], {{vertices[1].name, 9}})
end

-- Defect: the reader re-derived its remainder with buf:match("\n([^\n]*)$"),
-- which is nil when a chunk holds no newline, and had no end-of-file test. A
-- file whose last line has no trailing newline is the ordinary case for a hand
-- written or truncated graph.
g.test_file_without_a_trailing_newline = function()
    local path = fresh_path()
    local vertices = graph_fixture.read_vertices(3)
    graph_fixture.write(path, vertices, {
        {vertices[1].id, vertices[2].id, 1},
        {vertices[2].id, vertices[3].id, 2},
    }, {trailing_newline = false})

    local rec = recorder()
    loader.graph_edges_f(rec, path)()

    t.assert_equals(#puts_of(rec, 'vertex.store'), 3)
    local seen = 0
    for _, args in ipairs(puts_of(rec, 'edge.store')) do
        seen = seen + #args[2]
    end
    t.assert_equals(seen, 2, 'the unterminated last line is still a line')
end

g.test_blank_lines_are_skipped = function()
    local path = fresh_path()
    local vertices = graph_fixture.read_vertices(3)
    graph_fixture.write(path, vertices, {
        {vertices[1].id, vertices[2].id, 1},
    }, {blank_lines = true})

    local rec = recorder()
    loader.graph_edges_f(rec, path)()
    t.assert_equals(#puts_of(rec, 'vertex.store'), 3)
    t.assert_equals(#puts_of(rec, 'edge.store'), 1)
end

-- The reader works in 64k chunks, so a graph larger than one chunk exercises
-- the seam between them.
g.test_graph_larger_than_one_read_chunk = function()
    local path, vertices = graph_fixture.ring(fresh_path(), 2500)
    t.assert_gt(fio.stat(path).size, 65536)

    local rec = recorder()
    loader.graph_edges_f(rec, path)()

    t.assert_equals(#puts_of(rec, 'vertex.store'), 2500)
    local seen = 0
    for _, args in ipairs(puts_of(rec, 'edge.store')) do
        seen = seen + #args[2]
    end
    -- 2500 ring edges plus the one chord.
    t.assert_equals(seen, 2501)
    -- No name was cut in half at a chunk boundary.
    local stored = puts_of(rec, 'vertex.store')
    for i, vertex in ipairs(stored) do
        t.assert_equals(vertex.name, vertices[i].name, 'vertex ' .. i)
    end
end

g.test_edge_batch_is_capped = function()
    local path = fresh_path()
    local vertices = graph_fixture.read_vertices(1200)
    local edges = {}
    for i = 2, 1200 do
        table.insert(edges, {vertices[1].id, vertices[i].id, 1})
    end
    graph_fixture.write(path, vertices, edges)

    local rec = recorder()
    loader.graph_edges_f(rec, path)()

    local batches = puts_of(rec, 'edge.store')
    -- 1199 edges of one source, capped at 1000 per batch.
    t.assert_equals(#batches, 2)
    t.assert_equals(#batches[1][2], 1000)
    t.assert_equals(#batches[2][2], 199)
    for _, batch in ipairs(batches) do
        t.assert_equals(batch[1], vertices[1].name)
    end
end

-------------------------------------------------------------------------------
-- Malformed input
-------------------------------------------------------------------------------

g.test_missing_file_is_reported = function()
    local rec = recorder()
    local l = loader.graph_edges_f(rec, fio.pathjoin(VAR, 'no_such_file.txt'))
    t.assert_error_msg_contains('cannot open graph file', function() l() end)
end

g.test_unparsable_vertex_line = function()
    local path = fresh_path()
    local f = assert(io.open(path, 'w'))
    f:write("# List of vertices\nthis is not a vertex\n")
    f:close()
    local rec = recorder()
    local l = loader.graph_edges_f(rec, path)
    t.assert_error_msg_contains('cannot parse vertex line', function() l() end)
end

g.test_unparsable_edge_line = function()
    local path = fresh_path()
    local vertices = graph_fixture.read_vertices(2)
    graph_fixture.write(path, vertices, {})
    local f = assert(io.open(path, 'a'))
    f:write('nonsense\n')
    f:close()
    local rec = recorder()
    local l = loader.graph_edges_f(rec, path)
    t.assert_error_msg_contains('cannot parse edge line', function() l() end)
end

-- An edge to a vertex the file never declared would otherwise be stored with a
-- nil destination and only surface much later, inside a superstep.
g.test_edge_to_an_undeclared_vertex = function()
    local path = fresh_path()
    local vertices = graph_fixture.read_vertices(2)
    graph_fixture.write(path, vertices, {{vertices[1].id, 999999, 1}})
    local rec = recorder()
    local l = loader.graph_edges_f(rec, path)
    t.assert_error_msg_contains('never declared', function() l() end)
end
