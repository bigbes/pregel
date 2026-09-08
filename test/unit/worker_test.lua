local t = require('luatest')

local box_helper = require('test.helpers.box')
local worker = require('pregel.worker')
local master = require('pregel.master')
local queue = require('pregel.queue')

local g = t.group('worker')

local TOPMT_VERTEX_DELETE = 1
local TOPMT_VERTEX_STORE  = 2
local TOPMT_EDGE_STORE    = 3

local URI

local seq = 0
local function fresh_name()
    seq = seq + 1
    return string.format('wt%03d', seq)
end

g.before_all(function()
    URI = box_helper.listen_uri()
    -- The worker reports its aggregators to the master over a real net.box
    -- connection -- here, back to this same instance -- so the guest user it
    -- connects as needs the per-function lua_call grants.
    worker.grant('guest')
    master.grant('guest')
end)

local function obtain_name(value)
    return value.name
end

--- Build a worker whose master is this instance, and register its cleanup.
local function make_worker(options)
    local name = fresh_name()
    options = options or {}
    options.workers     = {URI}
    options.master      = URI
    options.obtain_name = options.obtain_name or obtain_name
    options.compute     = options.compute or function() end

    local w = worker.new(name, options)
    t.assert_equals(w.name, name)
    return w, name
end

--- after_superstep() reports every aggregator to the master over net.box, so a
-- test that calls it needs a master registered in this process.
local function make_master(name)
    return master.new(name, {workers = {URI}, obtain_name = obtain_name})
end

local function drop_worker(w)
    local name = w.name
    w:stop()
    for _, space_name in ipairs({'data_' .. name,
                                 'topology_mutation_' .. name}) do
        if box.space[space_name] ~= nil then
            box.space[space_name]:drop()
        end
    end
    for _, queue_name in ipairs({'mqueue_first_' .. name,
                                 'mqueue_second_' .. name}) do
        local q = rawget(queue.list, queue_name)
        if q ~= nil then
            q:drop()
        end
    end
end

-- Defect: worker:stop() left its two queues in the queue.new cache, so a
-- worker created afterwards under the same name got the stopped instance's
-- queues -- with the old combiner, the old squash_only and the old engine, and
-- no error. A restart in place with a different combiner computed with the old
-- one and returned wrong answers quietly.
g.test_new_after_stop_honours_the_queue_options = function()
    local first = function(a, b) return a + b end
    local second = function(a, b) return a > b and a or b end

    local w1, name = make_worker({combiner = first})
    t.assert_is(w1.mqueue.combiner, first)
    t.assert_equals(w1.mqueue.squash_only, false)
    t.assert_equals(w1.mqueue.engine, 'space')
    w1:stop()

    local w2 = worker.new(name, {
        workers      = {URI},
        master       = URI,
        obtain_name  = obtain_name,
        compute      = function() end,
        combiner     = second,
        squash_only  = true,
        queue_engine = 'table',
    })
    for _, q in ipairs({w2.mqueue, w2.mqueue_next}) do
        t.assert_is(q.combiner, second)
        t.assert_equals(q.squash_only, true)
        t.assert_equals(q.engine, 'table')
    end

    drop_worker(w2)
    -- The first worker's spaces outlive it: that is what the 'space' engine is
    -- for, and w2 asked for 'table'.
    for _, space_name in ipairs({'pregel_tube_mqueue_first_' .. name,
                                 'pregel_tube_mqueue_second_' .. name}) do
        if box.space[space_name] ~= nil then
            box.space[space_name]:drop()
        end
    end
end

local function vertices_of(w)
    local rv = {}
    for _, tuple in w.data_space:pairs() do
        rv[tuple[1]] = {halted = tuple[2], value = tuple[3], edges = tuple[4]}
    end
    return rv
end

-------------------------------------------------------------------------------
-- Registry and privileges
-------------------------------------------------------------------------------

g.test_registry_is_published = function()
    t.assert_equals(type(_G.pregel), 'table')
    t.assert_equals(type(_G.pregel.worker.deliver), 'function')
    t.assert_equals(type(_G.pregel.worker.deliver_batch), 'function')
    t.assert_equals(type(_G.pregel.worker.wait), 'function')
    t.assert_equals(type(_G.pregel.master.deliver), 'function')
end

-- The 1.6 version handed guest 'execute' on 'universe', which is every
-- function in the process. These are per-function lua_call grants instead.
g.test_grant_is_per_function = function()
    local granted = {}
    for _, priv in ipairs(box.schema.user.info('guest')) do
        if priv[2] == 'lua_call' then
            granted[priv[3]] = priv[1]
        end
        -- Nothing here may hand out execute on the universe.
        if priv[2] == 'universe' then
            t.assert_not_str_contains(priv[1], 'execute')
        end
    end
    t.assert_equals(granted['pregel.worker.deliver'], 'execute')
    t.assert_equals(granted['pregel.worker.deliver_batch'], 'execute')
    t.assert_equals(granted['pregel.worker.wait'], 'execute')
    t.assert_equals(granted['pregel.master.deliver'], 'execute')
end

-- The same, for the spaces delayed_push adds. They are created by mpool.new()
-- inside worker.new(), before the grant loop runs, so grant() can find them --
-- it just did not look.
g.test_grant_covers_the_delayed_push_bucket_spaces = function()
    local w, name = make_worker({delayed_push = true, grant_to = 'guest'})

    local space_names = {}
    for _, bucket in ipairs(w.mpool.buckets) do
        t.assert_not_equals(bucket.space_name, nil,
                            'the buckets are not space-backed')
        table.insert(space_names, bucket.space_name)
    end
    t.assert_gt(#space_names, 0)

    local privs = {space = {}, sequence = {}}
    for _, priv in ipairs(box.schema.user.info('guest')) do
        local kind, object = priv[2], tostring(priv[3])
        if privs[kind] ~= nil then
            privs[kind][object] = priv[1]
        end
    end
    for _, space_name in ipairs(space_names) do
        t.assert_str_contains(space_name, 'pregel_mpool_' .. name .. '_')
        t.assert_str_contains(privs.space[space_name] or '', 'write',
                              'guest cannot write ' .. space_name)
        -- The primary key is sequence-backed, and drawing from a sequence is a
        -- privileged operation of its own.
        t.assert_str_contains(privs.sequence[space_name .. '_seq'] or '',
                              'write',
                              'guest cannot use ' .. space_name .. '_seq')
    end

    drop_worker(w)
    for _, space_name in ipairs(space_names) do
        if box.space[space_name] ~= nil then
            box.space[space_name]:drop()
        end
    end
end

g.test_grant_is_idempotent = function()
    worker.grant('guest')
    master.grant('guest')
end

-------------------------------------------------------------------------------
-- Schema
-------------------------------------------------------------------------------

g.test_data_space_schema = function()
    local w, name = make_worker()
    local space = box.space['data_' .. name]
    t.assert_not_equals(space, nil)
    t.assert_equals(space:format(), {
        {name = 'id',        type = 'string' },
        {name = 'is_halted', type = 'boolean'},
        {name = 'value',     type = 'any'    },
        {name = 'edges',     type = 'array'  },
    })
    t.assert_equals(space.index.primary.parts[1].type, 'string')
    t.assert_equals(space.index.primary.unique, true)
    drop_worker(w)
end

g.test_topology_mutation_space_schema = function()
    local w, name = make_worker()
    local space = box.space['topology_mutation_' .. name]
    t.assert_not_equals(space, nil)
    local format = space:format()
    t.assert_equals(format[1], {name = 'id', type = 'unsigned'})
    t.assert_equals(format[2], {name = 'type', type = 'unsigned'})
    t.assert_equals(format[3], {name = 'name', type = 'string'})
    t.assert_equals(format[4].name, 'dest')
    t.assert_equals(format[4].is_nullable, true)
    t.assert_equals(format[5].name, 'value')
    t.assert_equals(format[5].is_nullable, true)

    -- box.once and space:auto_increment() are both gone: a sequence fills the
    -- primary key and the space is created with if_not_exists.
    t.assert_not_equals(space.index.primary.sequence_id, nil)
    t.assert_equals(space.index.type_name.unique, false)
    t.assert_equals(#space.index.type_name.parts, 2)
    drop_worker(w)
end

-- Creating a worker over spaces that already exist must not fail: box.once is
-- gone, so this is the only thing standing between a restart and a crash.
g.test_spaces_are_created_idempotently = function()
    local w, name = make_worker()
    w.data_space:replace{'a', false, {name = 'a'}, {}}
    w:stop()

    local w2 = worker.new(name, {
        workers = {URI}, master = URI,
        obtain_name = obtain_name, compute = function() end,
    })
    t.assert_equals(w2.data_space:len(), 1)
    drop_worker(w2)
end

-------------------------------------------------------------------------------
-- Option wiring
-------------------------------------------------------------------------------

-- Defect: worker_new read `squash_only` and `tube_engine` as undeclared
-- globals, so both were always nil and neither option reached the queues.
g.test_queue_options_reach_the_queues = function()
    local combiner = function(a, b) return a + b end
    local w = make_worker({
        combiner = combiner,
        squash_only = true,
        queue_engine = 'table',
    })
    for _, q in ipairs({w.mqueue, w.mqueue_next}) do
        t.assert_is(q.combiner, combiner)
        t.assert_equals(q.squash_only, true)
        t.assert_equals(q.engine, 'table')
    end
    drop_worker(w)
end

g.test_queue_engine_defaults_to_space = function()
    local w = make_worker()
    t.assert_equals(w.mqueue.engine, 'space')
    t.assert_equals(w.mqueue.squash_only, false)
    drop_worker(w)
end

g.test_rejects_unknown_queue_engine = function()
    t.assert_error_msg_contains('options.queue_engine', function()
        worker.new(fresh_name(), {
            workers = {URI}, master = URI,
            obtain_name = obtain_name, compute = function() end,
            queue_engine = 'mmap',
        })
    end)
end

g.test_worker_context_is_passed_through = function()
    local seen
    local w = make_worker({
        worker_context = {tag = 'ctx'},
        compute = function(self) seen = self:get_worker_context() end,
    })
    w:vertex_store({name = 'a'})
    w:run_superstep(1)
    t.assert_equals(seen, {tag = 'ctx'})
    drop_worker(w)
end

-------------------------------------------------------------------------------
-- Message dispatch
-------------------------------------------------------------------------------

g.test_deliver_dispatches_a_single_message = function()
    local w, name = make_worker()
    worker.deliver(name, 'vertex.store', {name = 'a', value = 1})
    t.assert_equals(w.data_space:get{'a'}:totable(),
                    {'a', false, {name = 'a', value = 1}, {}})
    drop_worker(w)
end

g.test_deliver_batch_dispatches_every_message = function()
    local w, name = make_worker()
    worker.deliver_batch(name, {
        {'vertex.store', {name = 'a', value = 1}},
        {'vertex.store', {name = 'b', value = 2}},
        {'edge.store', {'a', {{'b', 10}}}},
    })
    t.assert_equals(w.data_space:len(), 2)
    t.assert_equals(w.data_space:get{'a'}[4], {{'b', 10}})
    drop_worker(w)
end

g.test_message_deliver_puts_into_the_next_queue = function()
    local w, name = make_worker()
    worker.deliver(name, 'message.deliver', {'a', 42, 'b'})
    t.assert_equals(w.mqueue:len(), 0)
    t.assert_equals(w.mqueue_next:len('a'), 1)
    drop_worker(w)
end

g.test_unknown_message_type_is_named = function()
    local w, name = make_worker()
    t.assert_error_msg_contains('unknown message type: frobnicate', function()
        worker.deliver(name, 'frobnicate', {})
    end)
    drop_worker(w)
end

g.test_deliver_to_an_unknown_instance = function()
    t.assert_error_msg_contains('no pregel instance found', function()
        worker.deliver('no_such_worker', 'count', {})
    end)
end

g.test_count_counts_active_vertices = function()
    local w, name = make_worker()
    w.data_space:replace{'a', false, {name = 'a'}, {}}
    w.data_space:replace{'b', true, {name = 'b'}, {}}
    w.data_space:replace{'c', false, {name = 'c'}, {}}
    worker.deliver(name, 'count', {})
    t.assert_equals(w.in_progress, 2)
    drop_worker(w)
end

g.test_edge_store_on_a_missing_vertex_is_reported = function()
    local w, name = make_worker()
    t.assert_error_msg_contains("vertex 'ghost' does not exist", function()
        worker.deliver(name, 'edge.store', {'ghost', {{'b', 1}}})
    end)
    drop_worker(w)
end

-------------------------------------------------------------------------------
-- Topology mutation
-------------------------------------------------------------------------------

g.test_add_vertex_mutation = function()
    local w = make_worker()
    w:vertex_store_delayed({name = 'a', value = 7})
    local tuple = w.topology_mutation_space:select()[1]
    t.assert_equals(tuple[2], TOPMT_VERTEX_STORE)
    t.assert_equals(tuple[3], 'a')
    t.assert_equals(tuple[5], {name = 'a', value = 7})

    w:apply_topology_mutations()
    t.assert_equals(w.data_space:get{'a'}:totable(),
                    {'a', false, {name = 'a', value = 7}, {}})
    t.assert_equals(w.in_progress, 1)
    t.assert_equals(w.topology_mutation_space:len(), 0)
    drop_worker(w)
end

g.test_add_vertex_that_already_exists_is_left_alone = function()
    local w = make_worker()
    w.data_space:replace{'a', false, {name = 'a', value = 1}, {{'b', 1}}}
    w:vertex_store_delayed({name = 'a', value = 99})
    w:apply_topology_mutations()
    t.assert_equals(w.data_space:get{'a'}[3], {name = 'a', value = 1})
    t.assert_equals(w.in_progress, 0)
    drop_worker(w)
end

-- Defect: vertex_delete_delayed inserted a stray 2 between the type and the
-- name, so the name landed in the `dest` field and the vertex the mutation
-- named was never the one looked up.
g.test_delete_vertex_mutation_stores_the_name = function()
    local w = make_worker()
    w:vertex_delete_delayed('bob')
    local tuple = w.topology_mutation_space:select()[1]
    t.assert_equals(tuple[2], TOPMT_VERTEX_DELETE)
    t.assert_equals(tuple[3], 'bob')
    t.assert_equals(tuple[4], box.NULL)

    -- and the index finds it under (type, name).
    t.assert_equals(
        #w.topology_mutation_space.index.type_name:select(
            {TOPMT_VERTEX_DELETE, 'bob'}), 1)
    drop_worker(w)
end

-- Defect: the delete branch was inverted -- it logged "deleted" when
-- data_space:delete returned nil and then indexed that nil.
g.test_delete_vertex_mutation_applies = function()
    local w = make_worker()
    w.data_space:replace{'a', false, {name = 'a'}, {}}
    w.data_space:replace{'b', false, {name = 'b'}, {}}
    w.in_progress = 2

    w:vertex_delete_delayed('a')
    w:apply_topology_mutations()

    t.assert_equals(w.data_space:get{'a'}, nil)
    t.assert_not_equals(w.data_space:get{'b'}, nil)
    -- The deleted vertex was active, so it stops counting.
    t.assert_equals(w.in_progress, 1)
    t.assert_equals(w.topology_mutation_space:len(), 0)
    drop_worker(w)
end

g.test_delete_of_a_halted_vertex_does_not_change_in_progress = function()
    local w = make_worker()
    w.data_space:replace{'a', true, {name = 'a'}, {}}
    w.in_progress = 0
    w:vertex_delete_delayed('a')
    w:apply_topology_mutations()
    t.assert_equals(w.data_space:get{'a'}, nil)
    t.assert_equals(w.in_progress, 0)
    drop_worker(w)
end

g.test_delete_of_a_missing_vertex_is_harmless = function()
    local w = make_worker()
    w.in_progress = 0
    w:vertex_delete_delayed('ghost')
    w:apply_topology_mutations()
    t.assert_equals(w.in_progress, 0)
    t.assert_equals(w.topology_mutation_space:len(), 0)
    drop_worker(w)
end

g.test_add_edge_mutation = function()
    local w = make_worker()
    w.data_space:replace{'a', false, {name = 'a'}, {{'b', 1}}}
    w:edge_store_delayed('a', 'c', 5)
    w:edge_store_delayed('a', 'd', 6)
    local tuple = w.topology_mutation_space:select()[1]
    t.assert_equals(tuple[2], TOPMT_EDGE_STORE)
    t.assert_equals(tuple[3], 'a')
    t.assert_equals(tuple[4], 'c')
    t.assert_equals(tuple[5], 5)

    w:apply_topology_mutations()
    t.assert_equals(w.data_space:get{'a'}[4], {{'b', 1}, {'c', 5}, {'d', 6}})
    t.assert_equals(w.topology_mutation_space:len(), 0)
    drop_worker(w)
end

-- A vertex added in the same batch as an edge out of it must exist by the time
-- the edge is applied.
g.test_add_vertex_and_its_edges_in_one_batch = function()
    local w = make_worker()
    w:vertex_store_delayed({name = 'new', value = 1})
    w:edge_store_delayed('new', 'a', 3)
    w:apply_topology_mutations()
    t.assert_equals(w.data_space:get{'new'}[4], {{'a', 3}})
    drop_worker(w)
end

g.test_delete_edge_mutation = function()
    local w = make_worker()
    w.data_space:replace{'a', false, {name = 'a'},
                         {{'b', 1}, {'c', 2}, {'d', 3}}}
    w:edge_delete_delayed('a', 'c')
    w:apply_topology_mutations()
    t.assert_equals(w.data_space:get{'a'}[4], {{'b', 1}, {'d', 3}})
    t.assert_equals(w.topology_mutation_space:len(), 0)
    drop_worker(w)
end

-- Defect: the delayed edge-delete pass stopped at the first matching edge,
-- while the local delete_edge path removes every parallel edge to that
-- destination; one queued request left the other copies in place.
g.test_delete_edge_mutation_removes_every_parallel_edge = function()
    local w = make_worker()
    w.data_space:replace{'a', false, {name = 'a'},
                         {{'b', 1}, {'c', 2}, {'b', 3}, {'b', 4}}}
    w:edge_delete_delayed('a', 'b')
    w:apply_topology_mutations()
    t.assert_equals(w.data_space:get{'a'}[4], {{'c', 2}})
    drop_worker(w)
end

-- Defect: the edge-delete pass read data_space:get{src}[4] unguarded, so a
-- mutation naming a vertex that is not here -- deleted in the same batch, or
-- never stored -- crashed the whole superstep.
g.test_delete_edge_of_a_missing_vertex_is_harmless = function()
    local w = make_worker()
    w:edge_delete_delayed('ghost', 'x')
    w:apply_topology_mutations()
    t.assert_equals(w.topology_mutation_space:len(), 0)
    drop_worker(w)
end

g.test_add_edge_of_a_missing_vertex_is_harmless = function()
    local w = make_worker()
    w:edge_store_delayed('ghost', 'x', 1)
    w:apply_topology_mutations()
    t.assert_equals(w.topology_mutation_space:len(), 0)
    drop_worker(w)
end

g.test_delete_edge_that_is_not_there_is_harmless = function()
    local w = make_worker()
    w.data_space:replace{'a', false, {name = 'a'}, {{'b', 1}}}
    w:edge_delete_delayed('a', 'nobody')
    w:apply_topology_mutations()
    t.assert_equals(w.data_space:get{'a'}[4], {{'b', 1}})
    drop_worker(w)
end

g.test_all_four_mutation_types_in_one_batch = function()
    local w = make_worker()
    w.data_space:replace{'a', false, {name = 'a'}, {{'b', 1}, {'c', 2}}}
    w.data_space:replace{'gone', false, {name = 'gone'}, {}}
    w.in_progress = 2

    w:edge_delete_delayed('a', 'b')
    w:vertex_delete_delayed('gone')
    w:vertex_store_delayed({name = 'new', value = 5})
    w:edge_store_delayed('a', 'd', 9)
    w:apply_topology_mutations()

    local vertices = vertices_of(w)
    t.assert_equals(vertices['gone'], nil)
    t.assert_equals(vertices['a'].edges, {{'c', 2}, {'d', 9}})
    t.assert_equals(vertices['new'].value, {name = 'new', value = 5})
    -- one deleted, one added
    t.assert_equals(w.in_progress, 2)
    t.assert_equals(w.topology_mutation_space:len(), 0)
    drop_worker(w)
end

-------------------------------------------------------------------------------
-- Superstep bookkeeping
-------------------------------------------------------------------------------

g.test_after_superstep_swaps_the_queues = function()
    local w, name = make_worker()
    local m = make_master(name)
    local first, second = w.mqueue, w.mqueue_next
    worker.deliver(name, 'message.deliver', {'a', 1, 'b'})
    t.assert_equals(second:len('a'), 1)

    w:after_superstep()

    t.assert_is(w.mqueue, second)
    t.assert_is(w.mqueue_next, first)
    t.assert_equals(w.mqueue:len('a'), 1)
    t.assert_equals(w.mqueue_next:len(), 0)
    t.assert_equals(w.aggregators['__messages'](), 1)
    m:stop()
    drop_worker(w)
end

-- squash_only defers the combiner to once per superstep; after_superstep is
-- where that once happens, and __messages must count the squashed total.
g.test_after_superstep_squashes = function()
    local w, name = make_worker({
        combiner = function(a, b) return a + b end,
        squash_only = true,
    })
    local m = make_master(name)
    for i = 1, 5 do
        worker.deliver(name, 'message.deliver', {'a', i, 'b'})
    end
    t.assert_equals(w.mqueue_next:len('a'), 5)

    w:after_superstep()

    t.assert_equals(w.mqueue:len('a'), 1)
    local got
    for _, msg in w.mqueue:pairs('a') do got = msg end
    t.assert_equals(got, 15)
    t.assert_equals(w.aggregators['__messages'](), 1)
    m:stop()
    drop_worker(w)
end

g.test_run_superstep_skips_halted_vertices_without_messages = function()
    local computed = {}
    local w, name = make_worker({
        compute = function(self)
            table.insert(computed, self:get_name())
            self:vote_halt(true)
        end,
    })
    w.data_space:replace{'a', false, {name = 'a'}, {}}
    w.data_space:replace{'b', true, {name = 'b'}, {}}
    w.data_space:replace{'c', true, {name = 'c'}, {}}
    local m = make_master(name)
    -- c has a message waiting, so it runs despite being halted.
    worker.deliver(name, 'message.deliver', {'c', 1, 'a'})
    w:after_superstep()

    w:run_superstep(1)
    table.sort(computed)
    t.assert_equals(computed, {'a', 'c'})
    m:stop()
    drop_worker(w)
end

-------------------------------------------------------------------------------
-- Master
-------------------------------------------------------------------------------

g.test_master_unknown_operation_is_named = function()
    local m = master.new(fresh_name(), {
        workers = {URI}, obtain_name = obtain_name,
    })
    -- The 1.6 version called the global error() with format arguments, which
    -- raised "bad argument #2 to 'error'" instead of naming the operation.
    t.assert_error_msg_contains('unknown operation: frobnicate', function()
        master.deliver('frobnicate', {})
    end)
    m:stop()
end

g.test_master_merges_aggregator_reports = function()
    local m = master.new(fresh_name(), {
        workers = {URI}, obtain_name = obtain_name,
    })
    m:add_aggregator('sum', {
        default = 0,
        merge = function(old, new) return old + new end,
    })
    master.deliver('aggregator.inform', {'sum', 5})
    master.deliver('aggregator.inform', {'sum', 7})
    t.assert_equals(m.aggregators['sum'](), 12)
    m:stop()
end

g.test_master_rejects_an_unknown_aggregator = function()
    local m = master.new(fresh_name(), {
        workers = {URI}, obtain_name = obtain_name,
    })
    t.assert_error_msg_contains('unknown aggregator: nope', function()
        master.deliver('aggregator.inform', {'nope', 1})
    end)
    m:stop()
end

-- An aggregator with a reduce but no merge used to be rejected outright:
-- `opts.merge or opts.reduce` left merge nil whenever opts.reduce was nil,
-- which is the case for every aggregator that only sets a default.
g.test_aggregator_merge_defaults_to_reduce = function()
    local m = master.new(fresh_name(), {
        workers = {URI}, obtain_name = obtain_name,
    })
    m:add_aggregator('plain', {default = 0})
    m:add_aggregator('reducing', {
        default = 0,
        reduce = function(old, new) return old + new end,
    })
    master.deliver('aggregator.inform', {'reducing', 4})
    master.deliver('aggregator.inform', {'reducing', 6})
    t.assert_equals(m.aggregators['reducing'](), 10)
    m:stop()
end

-------------------------------------------------------------------------------
-- The whole protocol, master and worker in one process
-------------------------------------------------------------------------------

--- max-value: every vertex ends up holding the maximum over its component.
local function max_value_compute(self)
    local value = self:get_value().value
    local best = value
    for _, msg in self:pairs_messages() do
        if msg > best then best = msg end
    end
    if self:get_superstep() == 1 or best > value then
        self:set_value({name = self:get_name(), value = best})
        for _, dest in self:pairs_edges() do
            self:send_message(dest, best)
        end
    end
    self:vote_halt(true)
end

local function load_graph(w, vertices, edges)
    for name, value in pairs(vertices) do
        w.data_space:replace{name, false, {name = name, value = value},
                             edges[name] or {}}
    end
end

g.test_max_value_over_a_small_graph = function()
    local w, name = make_worker({compute = max_value_compute})
    -- a -> b -> c -> d, and a lone e.
    load_graph(w, {a = 3, b = 9, c = 1, d = 4, e = 7}, {
        a = {{'b', 1}},
        b = {{'c', 1}},
        c = {{'d', 1}},
        d = {{'a', 1}},
    })

    local m = master.new(name, {workers = {URI}, obtain_name = obtain_name})
    local supersteps = m:start()

    local vertices = vertices_of(w)
    for _, vname in ipairs({'a', 'b', 'c', 'd'}) do
        t.assert_equals(vertices[vname].value.value, 9, 'vertex ' .. vname)
        t.assert_equals(vertices[vname].halted, true)
    end
    -- e has no edges, so it keeps its own value.
    t.assert_equals(vertices['e'].value.value, 7)

    -- The cycle is four long, so the maximum needs a bounded number of hops.
    t.assert_le(supersteps, 8)
    t.assert_ge(supersteps, 2)
    -- Nothing left over.
    t.assert_equals(w.mqueue:len(), 0)
    t.assert_equals(w.mqueue_next:len(), 0)
    t.assert_equals(w.in_progress, 0)

    m:stop()
    drop_worker(w)
end

g.test_custom_aggregator_over_a_superstep = function()
    local w, name = make_worker({
        compute = function(self)
            self:set_aggregation('sum', self:get_value().value)
            self:vote_halt(true)
        end,
    })
    load_graph(w, {a = 1, b = 2, c = 3}, {})

    local aggr_opts = {
        default = 0,
        reduce = function(old, new) return old + new end,
        merge  = function(old, new) return old + new end,
    }
    w:add_aggregator('sum', aggr_opts)
    local m = master.new(name, {workers = {URI}, obtain_name = obtain_name})
    m:add_aggregator('sum', aggr_opts)

    m:start()

    -- Every vertex contributed its value once, in the one superstep that ran.
    t.assert_equals(m.aggregators['sum'](), 6)
    m:stop()
    drop_worker(w)
end

-- Defect: the worker kept the master's merged value in the very field the next
-- superstep's contributions reduced into, so it reported the global back and
-- the master added it once per worker -- three vertices contributing 1 each
-- ended at 9 after three supersteps rather than 3. The second half of the same
-- defect: get_aggregation() answered that live accumulator, so what a vertex
-- read depended on how much of its own shard had been computed before it.
g.test_aggregator_starts_each_superstep_from_the_default = function()
    local reads = {}
    local w, name = make_worker({
        compute = function(self)
            local step = self:get_superstep()
            -- Contributed in every superstep, by every vertex.
            self:set_aggregation('count', 1)
            reads[step] = reads[step] or {}
            table.insert(reads[step], self:get_aggregation('count'))
            self:vote_halt(step >= 3)
        end,
    })
    load_graph(w, {a = 1, b = 2, c = 3}, {})

    local add = function(old, new) return old + new end
    local aggr_opts = {default = 0, reduce = add, merge = add}
    w:add_aggregator('count', aggr_opts)
    local m = master.new(name, {workers = {URI}, obtain_name = obtain_name})
    m:add_aggregator('count', aggr_opts)

    t.assert_equals(m:start(), 3)

    -- One superstep's worth, not the running total of all three.
    t.assert_equals(m.aggregators['count'](), 3)
    -- And the worker's own accumulator is back at the default, ready for a
    -- superstep that will never come.
    t.assert_equals(w.aggregators['count'](), 0)

    -- Nothing is merged yet in superstep 1; from then on every vertex reads
    -- the same number -- the whole graph's, from the superstep before.
    t.assert_equals(reads[1], {0, 0, 0})
    t.assert_equals(reads[2], {3, 3, 3})
    t.assert_equals(reads[3], {3, 3, 3})

    m:stop()
    drop_worker(w)
end

g.test_topology_mutation_through_a_superstep = function()
    local w, name = make_worker({
        compute = function(self)
            if self:get_superstep() == 1 and self:get_name() == 'a' then
                -- Drop the cheap edge and grow the graph by one vertex.
                self:delete_edge('b')
                self:add_vertex({name = 'z', value = 0})
            end
            self:vote_halt(true)
        end,
    })
    load_graph(w, {a = 1, b = 2}, {a = {{'b', 1}, {'c', 5}}})

    local m = master.new(name, {workers = {URI}, obtain_name = obtain_name})
    m:start()

    local vertices = vertices_of(w)
    t.assert_equals(vertices['a'].edges, {{'c', 5}})
    t.assert_not_equals(vertices['z'], nil)
    t.assert_equals(vertices['z'].value, {name = 'z', value = 0})
    t.assert_equals(w.topology_mutation_space:len(), 0)
    m:stop()
    drop_worker(w)
end
