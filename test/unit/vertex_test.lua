local t = require('luatest')
local json = require('json')

local box_helper = require('test.helpers.box')
local fake_pregel = require('test.helpers.fake_pregel')
local vertex = require('pregel.vertex')
local aggregator = require('pregel.aggregator')

local g = t.group('vertex')

local compute = vertex.vertex_private_methods.compute
local apply = vertex.vertex_private_methods.apply

-- box.tuple.new needs a configured box; nothing else here does.
g.before_all(function()
    box_helper.cfg()
end)

local function tuple(id, halt, value, edges)
    return box.tuple.new{id, halt, value, edges or {}}
end

--- A vertex object wired to a fake instance, with `compute_func` as its
-- compute function.
--
-- A compute function here that ends with vote_halt(false) is keeping the halt
-- flag out of the test: not voting halts the vertex, which would move the
-- second field of every tuple asserted below and turn a test about edges into
-- a test about halting. The halt rule itself is under "Halt by default".
local function make(compute_func, opts)
    local pregel = fake_pregel.new(opts)
    local pool = vertex.pool_new{
        pregel = pregel,
        compute = compute_func or function() end,
    }
    return pool, pregel
end

local function pop(pool, ...)
    return pool:pop(tuple(...))
end

local function puts_of(pregel)
    return pregel.mpool.puts
end

-------------------------------------------------------------------------------
-- apply / accessors
-------------------------------------------------------------------------------

g.test_apply_loads_the_tuple = function()
    local pool = make()
    local v = pop(pool, 'alice', false, 42, {{'bob', 1}, {'carol', 2}})
    t.assert_equals(v:get_name(), 'alice')
    t.assert_equals(v:get_value(), 42)
    t.assert_equals(v.__halt, false)
    t.assert_equals(v.__modified, false)
    t.assert_equals(v.__edges, {{'bob', 1}, {'carol', 2}})
end

g.test_pairs_edges = function()
    local pool = make()
    local v = pop(pool, 'alice', false, 0, {{'bob', 1}, {'carol', 2}})
    local seen = {}
    for idx, dest, value in v:pairs_edges() do
        table.insert(seen, {idx, dest, value})
    end
    t.assert_equals(seen, {{1, 'bob', 1}, {2, 'carol', 2}})
end

g.test_pairs_edges_of_a_vertex_without_edges = function()
    local pool = make()
    local v = pop(pool, 'alice', false, 0, {})
    local count = 0
    for _ in v:pairs_edges() do count = count + 1 end
    t.assert_equals(count, 0)
end

g.test_pairs_messages = function()
    local pool = make(nil, {messages = {alice = {10, 20, 30}}})
    local v = pop(pool, 'alice', false, 0, {})
    local seen = {}
    for _, msg in v:pairs_messages() do table.insert(seen, msg) end
    t.assert_equals(seen, {10, 20, 30})
end

g.test_set_value_marks_modified = function()
    local pool = make()
    local v = pop(pool, 'alice', false, 1, {})
    t.assert_equals(v.__modified, false)
    v:set_value(9)
    t.assert_equals(v:get_value(), 9)
    t.assert_equals(v.__modified, true)
end

g.test_superstep_and_worker_context = function()
    local pool = make(nil, {worker_context = {tag = 'ctx'}})
    local v = pop(pool, 'alice', false, 0, {})
    v.__superstep = 7
    t.assert_equals(v:get_superstep(), 7)
    t.assert_equals(v:get_worker_context(), {tag = 'ctx'})
end

-- The two halves of a worker's aggregator are separate on purpose: what a
-- vertex contributes goes into this superstep's accumulator, what it reads is
-- the value the master merged out of the previous one. A vertex that read the
-- accumulator would see whatever its own shard had contributed before it,
-- which depends on the order the worker walks its space in.
g.test_aggregation_contributes_to_the_accumulator = function()
    local sum = aggregator.new('sum', nil, {
        default = 0,
        reduce  = function(old, new) return old + new end,
    })
    -- As delivered by the master at the end of the previous superstep.
    sum:receive_global(100)

    local pool = make(nil, {aggregators = {sum = sum}})
    local v = pop(pool, 'alice', false, 0, {})
    v:set_aggregation('sum', 5)
    v:set_aggregation('sum', 3)

    t.assert_equals(sum(), 8, 'the accumulator holds this superstep only')
    t.assert_equals(v:get_aggregation('sum'), 100,
                    'a vertex reads the previous superstep')
end

g.test_aggregation_before_any_superstep_reads_the_default = function()
    local sum = aggregator.new('sum', nil, {
        default = 7,
        reduce  = function(old, new) return old + new end,
    })
    local pool = make(nil, {aggregators = {sum = sum}})
    local v = pop(pool, 'alice', false, 0, {})
    t.assert_equals(v:get_aggregation('sum'), 7)
end

-------------------------------------------------------------------------------
-- vote_halt
-------------------------------------------------------------------------------

g.test_vote_halt_tracks_in_progress = function()
    local pool, pregel = make(nil, {in_progress = 10})
    local v = pop(pool, 'alice', false, 0, {})

    v:vote_halt()
    t.assert_equals(v.__halt, true)
    t.assert_equals(v.__modified, true)
    t.assert_equals(pregel.in_progress, 9)

    -- Voting the same way twice must not count twice.
    v:vote_halt(true)
    t.assert_equals(pregel.in_progress, 9)

    v:vote_halt(false)
    t.assert_equals(v.__halt, false)
    t.assert_equals(pregel.in_progress, 10)
end

-------------------------------------------------------------------------------
-- Halt by default
--
-- A compute function that returns without voting leaves its vertex halted; the
-- 1.6 behaviour was the opposite, and a job whose compute forgot to vote ran
-- until something killed it. The three cases below are the whole rule, and
-- each one also pins what it costs: whether the vertex ends active, and
-- whether the tuple is written at all.
-------------------------------------------------------------------------------

g.test_a_silent_compute_halts_the_vertex = function()
    local pool, pregel = make(function() end, {in_progress = 1})
    local v = pop(pool, 'alice', false, 0, {})

    compute(v)

    t.assert_equals(v.__halt, true)
    t.assert_equals(pregel.in_progress, 0)
    -- The halt is a change, so it is persisted -- reading it back from the
    -- space is the only thing later supersteps go by.
    t.assert_equals(pregel.data_space:last(), {'alice', true, 0, {}})
end

g.test_vote_halt_false_keeps_the_vertex_active = function()
    local pool, pregel = make(function(self)
        self:vote_halt(false)
    end, {in_progress = 1})
    local v = pop(pool, 'alice', false, 0, {})

    compute(v)

    t.assert_equals(v.__halt, false)
    t.assert_equals(pregel.in_progress, 1)
    -- Nothing changed, so nothing is written.
    t.assert_equals(#pregel.data_space.replaced, 0)
end

g.test_vote_halt_true_halts_the_vertex = function()
    local pool, pregel = make(function(self)
        self:vote_halt(true)
    end, {in_progress = 1})
    local v = pop(pool, 'alice', false, 0, {})

    compute(v)

    t.assert_equals(v.__halt, true)
    t.assert_equals(pregel.in_progress, 0)
    t.assert_equals(pregel.data_space:last(), {'alice', true, 0, {}})
end

-- The message-woken case: the vertex is already halted when compute runs, so
-- the default halt must be a no-op rather than another decrement.
g.test_a_silent_compute_on_a_halted_vertex_costs_nothing = function()
    local pool, pregel = make(function() end, {in_progress = 0})
    local v = pop(pool, 'alice', true, 0, {})

    compute(v)

    t.assert_equals(v.__halt, true)
    t.assert_equals(pregel.in_progress, 0)
    t.assert_equals(#pregel.data_space.replaced, 0)
end

-- ...and a halted vertex that votes to stay awake is counted once, which is
-- what makes vote_halt(false) worth having at all.
g.test_a_halted_vertex_can_vote_itself_active = function()
    local pool, pregel = make(function(self)
        self:vote_halt(false)
    end, {in_progress = 0})
    local v = pop(pool, 'alice', true, 0, {})

    compute(v)

    t.assert_equals(v.__halt, false)
    t.assert_equals(pregel.in_progress, 1)
    t.assert_equals(pregel.data_space:last(), {'alice', false, 0, {}})
end

-- The flag is per vertex, not per pooled object: the same table serves
-- thousands of vertices, and a vote by one of them must not speak for the
-- next.
g.test_the_vote_does_not_survive_apply = function()
    local pool, pregel = make(function() end, {in_progress = 2})
    local v = pop(pool, 'alice', false, 0, {})
    v:vote_halt(false)
    t.assert_equals(v.__voted, true)

    pool:push(v)
    local v2 = pop(pool, 'bob', false, 0, {})
    t.assert_is(v2, v)
    t.assert_equals(v2.__voted, false)

    compute(v2)
    t.assert_equals(v2.__halt, true)
    t.assert_equals(pregel.in_progress, 1)
end

-------------------------------------------------------------------------------
-- send_message
-------------------------------------------------------------------------------

-- Defect: send_message sent {receiver, message} while message.deliver is
-- documented and handled as {receiver, message, sent_from}, so the receiver
-- could never tell who had written to it.
g.test_send_message_carries_the_sender = function()
    local pool, pregel = make(function(self)
        self:send_message('bob', 'hello')
    end)
    local v = pop(pool, 'alice', false, 0, {})
    compute(v)

    t.assert_equals(puts_of(pregel), {
        {msg = 'message.deliver', args = {'bob', 'hello', 'alice'}}
    })
    -- Routed to the bucket owning the receiver, not the sender.
    t.assert_equals(pregel.mpool.routed, {'bob'})
end

-------------------------------------------------------------------------------
-- add_edge
-------------------------------------------------------------------------------

-- Defect: compute() finished the add loop with table.insert(self.__edges) --
-- one argument -- which raises in LuaJIT, so adding an edge crashed outright.
g.test_add_edge_local_two_arg_form = function()
    local pool, pregel = make(function(self)
        self:add_edge('carol', 5)
        self:vote_halt(false)
    end)
    local v = pop(pool, 'alice', false, 0, {{'bob', 1}})
    compute(v)

    t.assert_equals(v.__edges, {{'bob', 1}, {'carol', 5}})
    t.assert_equals(pregel.data_space:last(),
                    {'alice', false, 0, {{'bob', 1}, {'carol', 5}}})
    -- A local edge is applied here, not queued as a topology mutation.
    t.assert_equals(puts_of(pregel), {})
end

g.test_add_edge_explicit_own_name_is_local = function()
    local pool, pregel = make(function(self)
        self:add_edge('alice', 'carol', 5)
    end)
    local v = pop(pool, 'alice', false, 0, {})
    compute(v)
    t.assert_equals(v.__edges, {{'carol', 5}})
    t.assert_equals(puts_of(pregel), {})
end

g.test_add_edge_without_a_value_stores_null = function()
    local pool = make(function(self)
        self:add_edge('carol')
    end)
    local v = pop(pool, 'alice', false, 0, {})
    compute(v)
    t.assert_equals(#v.__edges, 1)
    t.assert_equals(v.__edges[1][1], 'carol')
    t.assert_equals(v.__edges[1][2], json.NULL)
end

-- Defect: the delayed branch sent {dest, value} while the worker unpacks
-- edge.store.delayed as (src, dest, value), so the edge was stored against the
-- destination as its source and the value went missing.
g.test_add_edge_delayed_carries_the_source = function()
    local pool, pregel = make(function(self)
        self:add_edge('bob', 'carol', 7)
    end)
    local v = pop(pool, 'alice', false, 0, {})
    compute(v)

    t.assert_equals(puts_of(pregel), {
        {msg = 'edge.store.delayed', args = {'bob', 'carol', 7}}
    })
    -- Queued on the bucket owning the source vertex.
    t.assert_equals(pregel.mpool.routed, {'bob'})
    -- and not applied locally.
    t.assert_equals(v.__edges, {})
end

-------------------------------------------------------------------------------
-- delete_edge
-------------------------------------------------------------------------------

g.test_delete_edge_local = function()
    local pool, pregel = make(function(self)
        self:delete_edge('bob')
        self:vote_halt(false)
    end)
    local v = pop(pool, 'alice', false, 0, {{'bob', 1}, {'carol', 2}})
    compute(v)

    t.assert_equals(v.__edges, {{'carol', 2}})
    t.assert_equals(pregel.data_space:last(),
                    {'alice', false, 0, {{'carol', 2}}})
end

-- Defect: the removal loop read `for k = #idx_to_rm, 1 do`, with no -1 step.
--
-- This test and test_delete_edge_that_does_not_exist_is_harmless are the two
-- that pin it, and deleting a single existing edge is not enough to: with one
-- index to remove the broken loop reads `for k = 1, 1`, which runs exactly
-- once and does the right thing by accident. It fails for two or more indexes
-- (`for k = 2, 1` never runs) and for none (`for k = 0, 1` runs at k = 0 and
-- calls table.remove(t, nil)).
g.test_delete_edge_removes_every_parallel_edge = function()
    local pool = make(function(self)
        self:delete_edge('bob')
    end)
    local v = pop(pool, 'alice', false, 0,
                  {{'bob', 1}, {'carol', 2}, {'bob', 3}, {'dave', 4}, {'bob', 5}})
    compute(v)
    t.assert_equals(v.__edges, {{'carol', 2}, {'dave', 4}})
end

g.test_delete_several_edges_in_one_superstep = function()
    local pool = make(function(self)
        self:delete_edge('bob')
        self:delete_edge('dave')
    end)
    local v = pop(pool, 'alice', false, 0,
                  {{'bob', 1}, {'carol', 2}, {'dave', 3}})
    compute(v)
    t.assert_equals(v.__edges, {{'carol', 2}})
end

g.test_delete_edge_that_does_not_exist_is_harmless = function()
    local pool = make(function(self)
        self:delete_edge('nobody')
    end)
    local v = pop(pool, 'alice', false, 0, {{'bob', 1}})
    compute(v)
    t.assert_equals(v.__edges, {{'bob', 1}})
end

g.test_delete_edge_delayed = function()
    local pool, pregel = make(function(self)
        self:delete_edge('bob', 'carol')
    end)
    local v = pop(pool, 'alice', false, 0, {})
    compute(v)
    t.assert_equals(puts_of(pregel), {
        {msg = 'edge.delete.delayed', args = {'bob', 'carol'}}
    })
    t.assert_equals(pregel.mpool.routed, {'bob'})
end

g.test_add_and_delete_edges_in_one_superstep = function()
    local pool = make(function(self)
        self:delete_edge('bob')
        self:add_edge('erin', 9)
    end)
    local v = pop(pool, 'alice', false, 0, {{'bob', 1}, {'carol', 2}})
    compute(v)
    t.assert_equals(v.__edges, {{'carol', 2}, {'erin', 9}})
end

-------------------------------------------------------------------------------
-- add_vertex / delete_vertex
-------------------------------------------------------------------------------

g.test_add_vertex = function()
    local pool, pregel = make(function(self)
        self:add_vertex({name = 'frank', value = 3})
    end, {obtain_name = function(value) return value.name end})
    local v = pop(pool, 'alice', false, 0, {})
    compute(v)

    t.assert_equals(#puts_of(pregel), 1)
    t.assert_equals(puts_of(pregel)[1].msg, 'vertex.store.delayed')
    t.assert_equals(puts_of(pregel)[1].args, {name = 'frank', value = 3})
    -- Routed by the name obtain_name derives from the value.
    t.assert_equals(pregel.mpool.routed, {'frank'})
end

g.test_add_vertex_rejects_nil = function()
    local pool = make(function(self) self:add_vertex(nil) end)
    local v = pop(pool, 'alice', false, 0, {})
    t.assert_error_msg_contains('value is nil', function() compute(v) end)
end

g.test_delete_vertex_defaults_to_self = function()
    local pool, pregel = make(function(self)
        self:delete_vertex()
    end)
    local v = pop(pool, 'alice', false, 0, {})
    compute(v)
    t.assert_equals(puts_of(pregel), {
        {msg = 'vertex.delete.delayed', args = {'alice', false}}
    })
    t.assert_equals(pregel.mpool.routed, {'alice'})
end

-- Defect: delete_vertex('bob') shifted its one argument into the `edges` flag
-- and tried to delete the calling vertex instead, then tripped over its own
-- assert. Only the flag can be a boolean, which is what tells the two apart.
g.test_delete_vertex_by_name = function()
    local pool, pregel = make(function(self)
        self:delete_vertex('bob')
    end)
    local v = pop(pool, 'alice', false, 0, {})
    compute(v)
    t.assert_equals(puts_of(pregel), {
        {msg = 'vertex.delete.delayed', args = {'bob', false}}
    })
    t.assert_equals(pregel.mpool.routed, {'bob'})
end

g.test_delete_vertex_with_edges_is_not_implemented = function()
    local pool = make(function(self)
        self:delete_vertex('bob', true)
    end)
    local v = pop(pool, 'alice', false, 0, {})
    t.assert_error_msg_contains('not implemented', function() compute(v) end)
end

-- The flag-first form, which is the whole reason delete_vertex looks at the
-- type of its first argument. Nothing covered it, so deleting that branch left
-- the suite green: `true` then became the vertex *name*, the assert on the
-- flag passed, and the request was routed by hashing a boolean.
g.test_delete_vertex_with_the_flag_alone = function()
    local pool = make(function(self)
        self:delete_vertex(true)
    end)
    local v = pop(pool, 'alice', false, 0, {})
    t.assert_error_msg_contains('not implemented', function() compute(v) end)
end

-------------------------------------------------------------------------------
-- compute / persistence
-------------------------------------------------------------------------------

-- Halted to begin with, so the compute really does nothing at all: a compute
-- that touches neither the value nor the edges still halts an active vertex,
-- and that is a change like any other.
g.test_compute_does_not_write_an_untouched_vertex = function()
    local pool, pregel = make(function() end)
    local v = pop(pool, 'alice', true, 7, {{'bob', 1}})
    t.assert_equals(compute(v), false)
    t.assert_equals(#pregel.data_space.replaced, 0)
end

g.test_compute_writes_a_changed_value = function()
    local pool, pregel = make(function(self)
        self:set_value(99)
        self:vote_halt(false)
    end)
    local v = pop(pool, 'alice', false, 7, {{'bob', 1}})
    t.assert_equals(compute(v), true)
    t.assert_equals(pregel.data_space:last(),
                    {'alice', false, 99, {{'bob', 1}}})
end

-- An edge change alone must reach the space even when the value did not move,
-- and compute() reports "modified" for the value only.
g.test_compute_writes_edge_changes_without_a_value_change = function()
    local pool, pregel = make(function(self)
        self:add_edge('carol', 5)
        self:vote_halt(false)
    end)
    local v = pop(pool, 'alice', false, 7, {})
    t.assert_equals(compute(v), false)
    t.assert_equals(#pregel.data_space.replaced, 1)
    t.assert_equals(pregel.data_space:last(), {'alice', false, 7, {{'carol', 5}}})
end

-------------------------------------------------------------------------------
-- Pooling
-------------------------------------------------------------------------------

g.test_pool_reuses_objects = function()
    local pool = make()
    local v1 = pop(pool, 'alice', false, 0, {})
    pool:push(v1)
    local v2 = pop(pool, 'bob', false, 0, {})
    t.assert_is(v2, v1)
    t.assert_equals(v2:get_name(), 'bob')
end

-- Defect: __edges_add and __edges_del belong to the pooled object, so anything
-- left in them by one graph vertex was applied to the next one the object
-- served. apply() has to clear them.
g.test_apply_clears_pending_edge_mutations = function()
    local pool, pregel = make(function(self) self:vote_halt(false) end)
    local v = pop(pool, 'alice', false, 0, {})
    -- Queue mutations without running compute(), the way a compute function
    -- that raised part-way through would leave them.
    v:add_edge('carol', 5)
    v:delete_edge('bob')
    t.assert_equals(#v.__edges_add, 1)
    t.assert_equals(#v.__edges_del, 1)

    pool:push(v)
    local v2 = pop(pool, 'zoe', false, 0, {{'bob', 1}})
    t.assert_is(v2, v)
    t.assert_equals(#v2.__edges_add, 0)
    t.assert_equals(#v2.__edges_del, 0)

    -- and running compute() on the new vertex leaves its edges alone.
    t.assert_equals(compute(v2), false)
    t.assert_equals(v2.__edges, {{'bob', 1}})
    t.assert_equals(#pregel.data_space.replaced, 0)
end

g.test_apply_resets_modified = function()
    local pool = make()
    local v = pop(pool, 'alice', false, 0, {})
    v:set_value(1)
    t.assert_equals(v.__modified, true)
    apply(v, tuple('bob', false, 2, {}))
    t.assert_equals(v.__modified, false)
end

g.test_pool_caps_what_it_keeps = function()
    local pool = make()
    local vertices = {}
    for i = 1, pool.maximum_count + 5 do
        vertices[i] = pop(pool, 'v' .. i, false, 0, {})
    end
    for i = 1, #vertices do
        pool:push(vertices[i])
    end
    t.assert_equals(pool.count, 0)
    t.assert_equals(#pool.container, pool.maximum_count)
end
