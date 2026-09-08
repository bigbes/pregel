local t = require('luatest')
local json = require('json')

local box_helper = require('test.helpers.box')
local fake_pregel = require('test.helpers.fake_pregel')
local vertex = require('pregel.vertex')

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

g.test_aggregation_round_trip = function()
    local stored = 0
    local aggregators = {
        sum = function(value)
            if value == nil then return stored end
            stored = stored + value
        end
    }
    local pool = make(nil, {aggregators = aggregators})
    local v = pop(pool, 'alice', false, 0, {})
    v:set_aggregation('sum', 5)
    v:set_aggregation('sum', 3)
    t.assert_equals(v:get_aggregation('sum'), 8)
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

-------------------------------------------------------------------------------
-- compute / persistence
-------------------------------------------------------------------------------

g.test_compute_does_not_write_an_untouched_vertex = function()
    local pool, pregel = make(function() end)
    local v = pop(pool, 'alice', false, 7, {{'bob', 1}})
    t.assert_equals(compute(v), false)
    t.assert_equals(#pregel.data_space.replaced, 0)
end

g.test_compute_writes_a_changed_value = function()
    local pool, pregel = make(function(self)
        self:set_value(99)
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
    local pool, pregel = make()
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
