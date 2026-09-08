--- The vertex object user compute functions are handed.
--
-- Vertices are pooled: run_superstep() pops one object per tuple and pushes it
-- back, so the same table serves thousands of graph vertices and apply() has to
-- leave no trace of the previous one.
--
-- Tuple layout of data_<name>:
--   1 <id>    string   vertex name
--   2 <halt>  boolean  voted to halt
--   3 <value> any      user value
--   4 <edges> array    array of {destination_name, edge_value}
--
-- @module pregel.vertex

local json = require('json')

local table_clear = require('table.clear')

--- Methods the worker drives the object with, kept off the user-facing
--  metatable so a compute function cannot reach them by accident.
local vertex_private_methods = {
    --- Rebind this pooled object to `tuple` and clear the previous vertex's
    --  state.
    --
    -- @param tuple a data_<name> tuple
    -- @return self
    -- @function apply
    apply = function(self, tuple)
        self.__modified = false
        self.__id, self.__halt, self.__value, self.__edges = tuple:unpack(1, 4)
        -- The pool hands this object straight on to the next vertex, so edge
        -- mutations the previous one requested and did not get to flush would
        -- otherwise be applied to this one.
        table_clear(self.__edges_add)
        table_clear(self.__edges_del)
        return self
    end,
    --- Run the user's compute function and persist what it changed.
    --
    -- The tuple is replaced only when something actually changed -- the value,
    -- the halt flag, or a queued edge addition or removal -- so a vertex that
    -- merely reads its messages costs no write. Queued edge mutations are
    -- applied here, deletions before additions.
    --
    -- @return true when the vertex's own value or halt flag changed; edge
    --         mutations alone do not set it
    -- @function compute
    compute = function(self)
        self:__compute_func()
        if self.__modified or
           #self.__edges_add > 0 or
           #self.__edges_del > 0 then
            while true do
                local edge = table.remove(self.__edges_del)
                if edge == nil then
                    break
                end
                local idx_to_rm = {}
                for idx, val in ipairs(self.__edges) do
                    if val[1] == edge then
                        table.insert(idx_to_rm, idx)
                    end
                end
                -- Descending, so removing one does not shift the next.
                for k = #idx_to_rm, 1, -1 do
                    table.remove(self.__edges, idx_to_rm[k])
                end
            end
            while true do
                local edge = table.remove(self.__edges_add)
                if edge == nil then
                    break
                end
                table.insert(self.__edges, edge)
            end
            self.__pregel.data_space:replace{
                self.__id, self.__halt, self.__value, self.__edges
            }
        end
        return self.__modified
    end,
    --- Run the user's write_solution function for this vertex.
    -- @return whatever that function returns
    -- @function write_solution
    write_solution = function(self)
        return self:__write_solution_func()
    end,
    --- Queue an outbound edge on the vertex currently bound to this object.
    --
    -- Applied by compute(), not now. The public vertex_methods.add_edge routes
    -- here only when the source is this very vertex.
    --
    -- @param dest destination vertex name
    -- @param value edge value
    -- @function add_edge
    add_edge = function(self, dest, value)
        table.insert(self.__edges_add, {dest, value})
    end,
    --- Queue the removal of every edge of this vertex pointing at `dest`.
    --
    -- Applied by compute(). Duplicate edges to the same destination all go.
    --
    -- @param dest destination vertex name
    -- @function delete_edge
    delete_edge = function(self, dest)
        table.insert(self.__edges_del, dest)
    end,
}

local vertex_methods = {
    --[[--
    -- | Base API
    -- * self:vote_halt        ([is_halted = true])
    -- * self:pairs_edges      ()
    -- * self:pairs_messages   ()
    -- * self:send_message     (receiver_id, value)
    -- * self:get_value        ()
    -- * self:set_value        (value)
    -- * self:get_name         ()
    -- * self:get_superstep    ()
    -- * self:get_aggregation  (name)
    -- * self:set_aggregation  (name, value)
    -- * self:get_worker_context ()
    --]]--
    --- Declare this vertex done (or undo that).
    --
    -- Maintains the worker's count of active vertices, so it must be the only
    -- way the halt flag is set. A halted vertex is woken again by a message,
    -- which is what makes an unhalt from here rarely necessary.
    --
    -- @param is_halted boolean, default true
    -- @function vote_halt
    vote_halt = function(self, is_halted)
        if is_halted == nil then is_halted = true end
        if self.__halt ~= is_halted then
            self.__modified = true
            self.__halt = is_halted
            self.__pregel.in_progress =
                self.__pregel.in_progress + (is_halted and -1 or 1)
        end
    end,
    --- Iterate this vertex's outbound edges.
    --
    -- Walks the edge array as it stands at the start of the superstep: edges
    -- added or deleted during it are queued and only applied afterwards, so
    -- they are not seen here.
    --
    -- @return iterator yielding (index, destination_name, edge_value)
    -- @function pairs_edges
    pairs_edges = function(self)
        local last = 0
        return function()
            last = last + 1
            local edge = self.__edges[last]
            if edge == nil then
                return nil
            end
            return last, edge[1], edge[2]
        end
    end,
    --- Iterate the messages sent to this vertex in the previous superstep.
    --
    -- @return iterator yielding (key, message); only the message is meaningful,
    --         see pregel.queue's pairs()
    -- @function pairs_messages
    pairs_messages = function(self)
        return self.__pregel.mqueue:pairs(self.__id)
    end,
    --- Send a message to be read in the next superstep.
    --
    -- The message goes to the worker that owns `receiver` and lands in that
    -- worker's *next* message queue, which is swapped in only when the
    -- superstep ends -- so it is unreadable until then even when sender and
    -- receiver live on the same worker.
    --
    -- @param receiver destination vertex name
    -- @param msg any value the message queue can hold
    -- @function send_message
    send_message = function(self, receiver, msg)
        -- The third element is the sender: message.deliver documents
        -- {receiver, message, sent_from} and a combiner or a compute function
        -- that wants to answer has no other way to learn who asked.
        self.__pregel.mpool:by_id(receiver):put(
                'message.deliver',
                {receiver, msg, self.__id}
        )
    end,
    --- This vertex's user value.
    -- @return the value, as it was stored
    -- @function get_value
    get_value = function(self)
        return self.__value
    end,
    --- Replace this vertex's user value.
    --
    -- Marks the vertex modified, which is what makes compute() write the tuple
    -- back; mutating the table returned by get_value() in place does not, and
    -- the change is lost.
    --
    -- @param new the new value
    -- @function set_value
    set_value = function(self, new)
        self.__modified = true
        self.__value = new
    end,
    --- The name pregel knows this vertex by.
    -- @return string
    -- @function get_name
    get_name = function(self)
        return self.__id
    end,
    --- The superstep now running, counted from 1.
    -- @return number
    -- @function get_superstep
    get_superstep = function(self)
        return self.__superstep
    end,
    --- Read an aggregator.
    --
    -- @param name aggregator name
    -- @return the value the whole graph produced in the previous superstep
    -- @raise when no aggregator of that name was declared
    -- @function get_aggregation
    get_aggregation = function(self, name)
        -- The previous superstep's merged value, the same for every vertex of
        -- this one. Reading the live accumulator instead -- which is what
        -- aggregators[name]() answers -- gave each vertex whatever its own
        -- shard had contributed so far, so the answer depended on the order
        -- the worker happened to walk its own space in.
        return self.__pregel.aggregators[name]:get_global()
    end,
    --- Contribute to an aggregator.
    --
    -- Folded into this worker's accumulator through the aggregator's `reduce`;
    -- the result is not visible to get_aggregation() until the next superstep.
    --
    -- @param name aggregator name
    -- @param value the contribution
    -- @raise when no aggregator of that name was declared
    -- @function set_aggregation
    set_aggregation = function(self, name, value)
        return self.__pregel.aggregators[name](value)
    end,
    --[[--
    -- | Topology mutation API
    --
    -- These take effect between supersteps, not immediately: they are queued
    -- as topology mutations on the worker that owns the vertex in question.
    --
    -- * self:add_vertex    (value)
    -- * self:add_edge      ([src = self:get_name(), ]dest, value)
    -- * self:delete_vertex ([src = self:get_name()][, edges = false])
    -- * self:delete_edge   ([src = self:get_name(), ]dest)
    --]]--
    --- Ask for a new vertex to be created.
    --
    -- The name comes from the instance's obtain_name applied to `value`, which
    -- is also what decides the worker it lands on.
    --
    -- @param value the vertex value; obtain_name must be able to name it
    -- @raise when `value` is nil
    -- @function add_vertex
    add_vertex = function(self, value)
        assert(value ~= nil, 'value is nil')
        local name = self.__pregel.obtain_name(value)
        self.__pregel.mpool:by_id(name):put(
                'vertex.store.delayed',
                value
        )
    end,
    --- Add an outbound edge, of this vertex or of another one.
    --
    -- Two shapes: add_edge(dest, value) for an edge of this vertex, and
    -- add_edge(src, dest, value) for one of `src`. The form is told apart by
    -- whether the third argument is nil, so add_edge(src, dest, nil) is read as
    -- the two-argument form -- pass json.NULL rather than nil for a valueless
    -- edge of another vertex. An omitted value becomes json.NULL.
    --
    -- An edge of this vertex is queued locally and applied by compute(); one of
    -- another vertex is sent to the worker that owns `src` and applied between
    -- supersteps.
    --
    -- @param src source vertex name, or the destination in the two-argument form
    -- @param dest destination vertex name, or the value
    -- @param value edge value
    -- @raise when the destination is nil
    -- @function add_edge
    add_edge = function(self, src, dest, value)
        if value == nil then
            -- Two-argument form: add_edge(dest, value).
            value = dest
            dest  = src
            src   = self:get_name()
        end
        if value == nil then
            value = json.NULL
        end
        assert(dest ~= nil, 'destination is nil')
        if src == self:get_name() then
            vertex_private_methods.add_edge(self, dest, value)
        else
            -- The worker on the far side stores this against `src`, so the
            -- source has to travel with it: edge.store.delayed is
            -- {src, dest, value}.
            self.__pregel.mpool:by_id(src):put(
                    'edge.store.delayed',
                    {src, dest, value}
            )
        end
    end,
    --- Delete a vertex: this one by default, or the one named.
    --
    -- Queued on the worker that owns it and applied between supersteps.
    --
    -- deleting all inbound edges is not implemented: only the full-scan
    -- version can be, and nothing needs it yet.
    --
    -- @param vertex_name name to delete (default this vertex); a boolean here
    --        is read as `edges`, so delete_vertex(true) still means self
    -- @param edges must be false or nil
    -- @raise when `edges` is true
    -- @function delete_vertex
    delete_vertex = function(self, vertex_name, edges)
        -- Both arguments are optional and only the flag can be a boolean, so
        -- that is what tells delete_vertex(true) from delete_vertex('name').
        if type(vertex_name) == 'boolean' then
            edges = vertex_name
            vertex_name = nil
        end
        vertex_name = vertex_name or self:get_name()
        if edges == nil then
            edges = false
        end
        assert(edges == false, 'deleting inbound edges is not implemented')
        self.__pregel.mpool:by_id(vertex_name):put(
                'vertex.delete.delayed',
                {vertex_name, edges}
        )
    end,
    --- Delete every edge from a source to a destination.
    --
    -- delete_edge(dest) removes edges of this vertex; delete_edge(src, dest)
    -- removes them from `src`. Parallel edges to the same destination all go,
    -- and a destination with no edge is not an error.
    --
    -- @param src source vertex name, or the destination in the one-argument form
    -- @param dest destination vertex name
    -- @raise when either name resolves to nil
    -- @function delete_edge
    delete_edge = function(self, src, dest)
        if dest == nil then
            -- One-argument form: delete_edge(dest).
            dest = src
            src = self:get_name()
        end
        assert(src ~= nil, 'source is nil')
        assert(dest ~= nil, 'destination is nil')
        if src == self:get_name() then
            vertex_private_methods.delete_edge(self, dest)
        else
            self.__pregel.mpool:by_id(src):put(
                    'edge.delete.delayed',
                    {src, dest}
            )
        end
    end,
    --- The worker-local context this instance was configured with.
    --
    -- Whatever was passed as options.worker_context: one value per worker,
    -- shared by every vertex it computes and outliving all of them, which is
    -- where state a compute function needs across vertices or supersteps goes.
    --
    -- @return the context, or nil when none was configured
    -- @function get_worker_context
    get_worker_context = function(self)
        return self.__pregel.worker_context
    end
}

--- A bare vertex object, with no tuple bound to it yet.
--
-- Only the pool calls this; apply() is what makes the object usable.
--
-- @return vertex object
-- @function new
local function vertex_new()
    return setmetatable({
        -- reset by apply() for every vertex this object serves
        __superstep           = 0,
        __id                  = 0,
        __modified            = false,
        __halt                = false,
        __edges               = nil,
        __value               = 0,
        __edges_del           = {},
        __edges_add           = {},
        -- assigned once, when the pool creates the object
        __pregel              = nil,
        __compute_func        = nil,
        __write_solution_func = nil,
    }, {
        __index = vertex_methods
    })
end

local vertex_pool_methods = {
    --- Take a vertex object bound to `tuple`.
    --
    -- Reuses a parked object when there is one and builds a new one otherwise,
    -- so the pool never blocks -- `maximum_count` caps what is kept, not what
    -- is handed out.
    --
    -- @param tuple a data_<name> tuple
    -- @return vertex object
    -- @function pop
    pop = function(self, tuple)
        assert(self.count >= 0)
        self.count = self.count + 1
        local vl = table.remove(self.container)
        if vl == nil then
            vl = vertex_new()
            vl.__compute_func = self.compute
            vl.__write_solution_func = self.write_solution
            vl.__pregel = self.pregel
        end
        return vertex_private_methods.apply(vl, tuple)
    end,
    --- Give a vertex object back.
    --
    -- Objects beyond `maximum_count` are dropped rather than parked. The
    -- object must not be used afterwards: the next pop() rebinds it.
    --
    -- @param vertex object from pop()
    -- @raise when more objects come back than went out
    -- @function push
    push = function(self, vertex)
        self.count = self.count - 1
        assert(self.count >= 0)
        if #self.container == self.maximum_count then
            return
        end
        table.insert(self.container, vertex)
    end
}

--- A pool of vertex objects for one worker.
--
-- The compute and write_solution functions are bound here, once, and every
-- object the pool creates carries them -- which is why the pool is per
-- instance rather than global.
--
-- @param cfg table with `pregel` (the worker instance), `compute` and
--        `write_solution`
-- @return pool object
-- @function pool_new
local function vertex_pool_new(cfg)
    cfg = cfg or {}
    return setmetatable({
        count          = 0,
        container      = {},
        maximum_count  = 100,
        pregel         = cfg.pregel,
        compute        = cfg.compute,
        write_solution = cfg.write_solution
    }, {
        __index = vertex_pool_methods
    })
end

return {
    vertex_private_methods = vertex_private_methods,
    new = vertex_new,
    pool_new = vertex_pool_new,
    vertex_methods = vertex_methods,
}
