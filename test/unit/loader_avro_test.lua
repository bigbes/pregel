local t   = require('luatest')
local fio = require('fio')

local mpool  = require('pregel.mpool')
local loader = require('pregel.loader')

local g = t.group('loader_avro')

local SMALL     = fio.pathjoin('test', 'fixtures', 'graphs', 'small')
local RING      = fio.pathjoin(SMALL, 'ring10')
local VERTICES  = fio.pathjoin(RING, 'vertices.avro')
local EDGES     = fio.pathjoin(RING, 'edges.avro')

-- ring10.txt, in file order. Everything below is asserted against these rather
-- than against a re-reading of the fixture, so a converter that silently
-- dropped or reordered records would still be caught.
local NAMES = {
    'alpha node', 'beta', 'gamma node', 'delta', 'epsilon node',
    'zeta', 'eta node', 'theta', 'iota node', 'kappa',
}

local VERTICES_EXPECTED = {}
for i, name in ipairs(NAMES) do
    VERTICES_EXPECTED[i] = {id = i, name = name, value = i * 10}
end

-- The four chords out of vertex 1 come first, then the ring.
local EDGES_EXPECTED = {
    {'alpha node',    'beta',         1 },
    {'alpha node',    'gamma node',   11},
    {'alpha node',    'delta',        12},
    {'alpha node',    'epsilon node', 13},
    {'alpha node',    'zeta',         14},
    {'beta',          'gamma node',   2 },
    {'gamma node',    'delta',        3 },
    {'delta',         'epsilon node', 4 },
    {'epsilon node',  'zeta',         5 },
    {'zeta',          'eta node',     6 },
    {'eta node',      'theta',        7 },
    {'theta',         'iota node',    8 },
    {'iota node',     'kappa',        9 },
    {'kappa',         'alpha node',   10},
}

--- An instance stand-in that records every put, and the bucket it went to.
--
-- `mpool:id` is the real guava sharding over `workers` buckets, because the
-- partitioned load is defined in terms of exactly that function -- a stub that
-- invented its own answer would test the stub.
local function recorder(workers)
    workers = workers or 1
    local self = {puts = {}, flushed = 0, workers = workers}
    local function bucket_for(id)
        return {
            put = function(_, msg, args)
                table.insert(self.puts, {msg = msg, args = args, bucket = id})
            end
        }
    end
    self.mpool = {
        id = function(_, name)
            return mpool.guava_name(name, workers)
        end,
        by_id = function(this, name)
            return bucket_for(this:id(name))
        end,
        flush = function()
            self.flushed = self.flushed + 1
        end,
    }
    self.obtain_name = function(vertex) return vertex.name end
    return self
end

local function puts_of(rec, msg)
    local rv = {}
    for _, put in ipairs(rec.puts) do
        if put.msg == msg then
            table.insert(rv, put)
        end
    end
    return rv
end

local function vertices_stored(rec)
    local rv = {}
    for _, put in ipairs(puts_of(rec, 'vertex.store')) do
        table.insert(rv, put.args)
    end
    return rv
end

--- Every edge batch as {source, {{destination, value}, ...}}.
local function edge_batches(rec)
    local rv = {}
    for _, put in ipairs(puts_of(rec, 'edge.store')) do
        table.insert(rv, put.args)
    end
    return rv
end

--- Every stored edge flattened out of its batch, as {source, dest, value}.
local function edges_stored(rec)
    local rv = {}
    for _, batch in ipairs(edge_batches(rec)) do
        for _, edge in ipairs(batch[2]) do
            table.insert(rv, {batch[1], edge[1], edge[2]})
        end
    end
    return rv
end

local function ring_loader(rec, extra)
    local opts = {
        vertices   = VERTICES,
        edges      = EDGES,
        vertex_name = 'name',
        edge_src    = 'src',
        edge_dst    = 'dst',
        edge_value  = 'weight',
    }
    for k, v in pairs(extra or {}) do
        opts[k] = v
    end
    return loader.avro_files(rec, opts)
end

-------------------------------------------------------------------------------
-- Whole-graph load
-------------------------------------------------------------------------------

g.test_stores_every_vertex_of_the_file = function()
    local rec = recorder()
    ring_loader(rec)()
    -- The whole record by default, because the worker names a stored vertex by
    -- calling obtain_name on it.
    t.assert_equals(vertices_stored(rec), VERTICES_EXPECTED)
end

g.test_stores_every_edge_of_the_file = function()
    local rec = recorder()
    ring_loader(rec)()
    t.assert_equals(edges_stored(rec), EDGES_EXPECTED)
end

-- Edges of one source travel as one 'edge.store', which is the whole point of
-- batching them, and the batch names the source it was accumulated for.
g.test_edges_are_batched_per_source = function()
    local rec = recorder()
    ring_loader(rec)()

    local batches = edge_batches(rec)
    -- One batch for 'alpha node' holding all five of its edges, then one per
    -- remaining ring vertex.
    t.assert_equals(#batches, 10)
    t.assert_equals(batches[1][1], 'alpha node')
    t.assert_equals(#batches[1][2], 5)
    for i = 2, 10 do
        t.assert_equals(batches[i][1], NAMES[i])
        t.assert_equals(#batches[i][2], 1)
    end
end

-- Mutation target: the loader's final flush_edges(). Without it the last
-- source's batch is only ever ended by a record that never comes.
g.test_the_final_batch_is_flushed = function()
    local rec = recorder()
    ring_loader(rec)()

    local batches = edge_batches(rec)
    t.assert_equals(batches[#batches],
                    {'kappa', {{'alpha node', 10}}},
                    'the last source in the file still reaches the mpool')
end

g.test_every_message_goes_to_the_bucket_owning_its_source = function()
    local rec = recorder(3)
    ring_loader(rec)()
    for _, put in ipairs(rec.puts) do
        local name = put.msg == 'vertex.store' and put.args.name or put.args[1]
        t.assert_equals(put.bucket, mpool.guava_name(name, 3),
                        'message for ' .. name)
    end
end

g.test_flushes_the_pool_once_at_the_end = function()
    local rec = recorder()
    local n = ring_loader(rec)()
    t.assert_equals(rec.flushed, 1)
    -- 10 vertices plus 14 edges.
    t.assert_equals(n, 24)
end

-------------------------------------------------------------------------------
-- Batching
-------------------------------------------------------------------------------

g.test_a_source_longer_than_the_batch_is_split = function()
    local rec = recorder()
    ring_loader(rec, {batch = 3})()

    local batches = edge_batches(rec)
    -- 'alpha node' has five edges, so it needs two batches; every other source
    -- has one edge and needs one.
    t.assert_equals(#batches, 11)
    t.assert_equals(batches[1][1], 'alpha node')
    t.assert_equals(#batches[1][2], 3)
    t.assert_equals(batches[2][1], 'alpha node')
    t.assert_equals(#batches[2][2], 2)
    -- Splitting must not lose or duplicate an edge.
    t.assert_equals(edges_stored(rec), EDGES_EXPECTED)
end

g.test_batch_of_one_sends_every_edge_on_its_own = function()
    local rec = recorder()
    ring_loader(rec, {batch = 1})()
    local batches = edge_batches(rec)
    t.assert_equals(#batches, #EDGES_EXPECTED)
    t.assert_equals(edges_stored(rec), EDGES_EXPECTED)
end

-------------------------------------------------------------------------------
-- Mapping options: field names and functions
-------------------------------------------------------------------------------

g.test_function_form_of_every_mapping_option = function()
    local rec = recorder()
    loader.avro_files(rec, {
        vertices     = VERTICES,
        edges        = EDGES,
        vertex_name  = function(r) return r.name end,
        vertex_value = function(r) return {name = r.name, value = r.value * 2} end,
        edge_src     = function(r) return r.src end,
        edge_dst     = function(r) return r.dst end,
        edge_value   = function(r) return r.weight * 100 end,
    })()

    local stored = vertices_stored(rec)
    t.assert_equals(#stored, 10)
    t.assert_equals(stored[1], {name = 'alpha node', value = 20})
    t.assert_equals(stored[10], {name = 'kappa', value = 200})

    local edges = edges_stored(rec)
    t.assert_equals(#edges, #EDGES_EXPECTED)
    for i, edge in ipairs(edges) do
        t.assert_equals(edge, {EDGES_EXPECTED[i][1], EDGES_EXPECTED[i][2],
                               EDGES_EXPECTED[i][3] * 100})
    end
end

-- The field-name form of vertex_value, on an instance that can still name what
-- comes out of it: whatever is stored is what the worker hands to obtain_name,
-- so a value narrowed to one field only works where that field is nameable --
-- here the value *is* the name.
g.test_vertex_value_as_a_field_name = function()
    local rec = recorder()
    rec.obtain_name = function(value) return value end
    ring_loader(rec, {vertex_value = 'name'})()
    t.assert_equals(vertices_stored(rec), NAMES)
end

-- Without edge_value an edge still needs a value, and json.NULL is the one that
-- survives msgpack round trips as an explicit null.
g.test_edge_value_defaults_to_null = function()
    local rec = recorder()
    loader.avro_files(rec, {
        vertices = VERTICES, edges = EDGES,
        vertex_name = 'name', edge_src = 'src', edge_dst = 'dst',
    })()
    for _, edge in ipairs(edges_stored(rec)) do
        t.assert_equals(edge[3], require('json').NULL)
    end
end

-------------------------------------------------------------------------------
-- Worker-side partitioning
-------------------------------------------------------------------------------

--- Load the ring on each of `count` workers, as worker:preload() would.
local function load_partitioned(count)
    local per_worker = {}
    for idx = 1, count do
        local rec = recorder(count)
        ring_loader(rec)(idx, count)
        per_worker[idx] = rec
    end
    return per_worker
end

-- Mutation target: the `owns()` test in the loader. With it removed every
-- worker stores the whole graph, and each vertex is then seen three times.
g.test_three_workers_cover_every_vertex_exactly_once = function()
    local per_worker = load_partitioned(3)

    local seen = {}
    for idx, rec in ipairs(per_worker) do
        for _, vertex in ipairs(vertices_stored(rec)) do
            t.assert_equals(seen[vertex.name], nil,
                            vertex.name .. ' stored twice, second time on ' ..
                            'worker ' .. idx)
            seen[vertex.name] = idx
        end
    end

    local count = 0
    for _, name in ipairs(NAMES) do
        t.assert_not_equals(seen[name], nil, name .. ' was stored by nobody')
        -- And by the worker the pool would route it to.
        t.assert_equals(seen[name], mpool.guava_name(name, 3))
        count = count + 1
    end
    t.assert_equals(count, #NAMES)

    -- The split has to be a split, not "worker 1 takes everything".
    local shares = {}
    for idx, rec in ipairs(per_worker) do
        shares[idx] = #vertices_stored(rec)
        t.assert_gt(shares[idx], 0, 'worker ' .. idx .. ' got no vertices')
    end
    t.assert_equals(shares[1] + shares[2] + shares[3], #NAMES)
end

g.test_three_workers_cover_every_edge_exactly_once = function()
    local per_worker = load_partitioned(3)

    local seen = {}
    for idx, rec in ipairs(per_worker) do
        for _, edge in ipairs(edges_stored(rec)) do
            local key = edge[1] .. ' -> ' .. edge[2]
            t.assert_equals(seen[key], nil,
                            key .. ' stored twice, second time on worker ' ..
                            idx)
            seen[key] = idx
        end
    end

    local expected = {}
    for _, edge in ipairs(EDGES_EXPECTED) do
        expected[edge[1] .. ' -> ' .. edge[2]] = mpool.guava_name(edge[1], 3)
    end
    -- Same set, and every edge on the worker that owns its source -- which is
    -- the worker that stored that source's vertex, so no edge arrives at an
    -- instance that has never seen the vertex it belongs to.
    t.assert_equals(seen, expected)
end

g.test_a_single_worker_still_gets_the_whole_graph = function()
    local rec = recorder(1)
    ring_loader(rec)(1, 1)
    t.assert_equals(vertices_stored(rec), VERTICES_EXPECTED)
    t.assert_equals(edges_stored(rec), EDGES_EXPECTED)
end

-------------------------------------------------------------------------------
-- Option validation
-------------------------------------------------------------------------------

-- A misspelled field would otherwise read back as nil for every record and
-- store a whole graph of nameless vertices before anything complained.
g.test_unknown_vertex_field_name = function()
    t.assert_error_msg_contains('which Vertex does not have', function()
        ring_loader(recorder(), {vertex_name = 'nmae'})
    end)
end

g.test_unknown_vertex_field_name_lists_the_real_fields = function()
    t.assert_error_msg_contains('fields: id, name, value', function()
        ring_loader(recorder(), {vertex_name = 'nmae'})
    end)
end

g.test_unknown_edge_field_name = function()
    t.assert_error_msg_contains('which Edge does not have', function()
        ring_loader(recorder(), {edge_dst = 'destination'})
    end)
end

g.test_unknown_edge_value_field_name = function()
    t.assert_error_msg_contains('options.edge_value', function()
        ring_loader(recorder(), {edge_value = 'cost'})
    end)
end

g.test_missing_vertex_file = function()
    t.assert_error_msg_contains('no such file', function()
        ring_loader(recorder(), {vertices = fio.pathjoin(SMALL, 'nope.avro')})
    end)
end

g.test_missing_edge_file = function()
    t.assert_error_msg_contains('no such file', function()
        ring_loader(recorder(), {edges = fio.pathjoin(SMALL, 'nope.avro')})
    end)
end

g.test_a_file_that_is_not_an_ocf = function()
    t.assert_error_msg_contains('cannot read', function()
        ring_loader(recorder(), {
            vertices = fio.pathjoin(SMALL, 'ring10.txt'),
        })
    end)
end

g.test_vertex_name_is_required = function()
    t.assert_error_msg_contains('options.vertex_name is required', function()
        loader.avro_files(recorder(), {vertices = VERTICES, edges = EDGES,
                                       edge_src = 'src', edge_dst = 'dst'})
    end)
end

g.test_edge_src_is_required = function()
    t.assert_error_msg_contains('options.edge_src is required', function()
        loader.avro_files(recorder(), {vertices = VERTICES, edges = EDGES,
                                       vertex_name = 'name',
                                       edge_dst = 'dst'})
    end)
end

g.test_edge_dst_is_required = function()
    t.assert_error_msg_contains('options.edge_dst is required', function()
        loader.avro_files(recorder(), {vertices = VERTICES, edges = EDGES,
                                       vertex_name = 'name',
                                       edge_src = 'src'})
    end)
end

g.test_a_mapping_option_of_the_wrong_type = function()
    t.assert_error_msg_contains('must be a field name or a function', function()
        ring_loader(recorder(), {vertex_name = 42})
    end)
end

g.test_batch_must_be_positive = function()
    t.assert_error_msg_contains('options.batch must be a positive number',
                                function()
        ring_loader(recorder(), {batch = 0})
    end)
end

g.test_options_must_be_a_table = function()
    t.assert_error_msg_contains('options must be a table', function()
        loader.avro_files(recorder(), VERTICES)
    end)
end
