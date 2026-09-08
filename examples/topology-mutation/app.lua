--- topology-mutation: prune weak edges, and mark whatever that strands.
--
-- Not a graph algorithm so much as a demonstration of the one API the other
-- examples do not touch: a compute function may change the graph, not only the
-- values in it. Here every edge whose weight is below a threshold is deleted,
-- and a vertex left with no out-edges at all gets a marker vertex called
-- '<name>:orphan' added next to it.
--
-- The two mutations take effect at different times, which is the thing to take
-- away:
--
--   delete_edge on one's own edges is applied when the vertex is written back
--     at the end of its own compute call. pairs_edges still walks the full
--     list while compute is running, so the survivors have to be counted
--     rather than read off the vertex afterwards.
--
--   add_vertex is queued as a topology mutation on whichever worker will own
--     the new vertex -- which is decided by obtain_name and need not be this
--     one -- and applied between supersteps, after every compute call has
--     finished. So the marker does not exist during the superstep that asks
--     for it; it turns up, active and unhalted, in the next one.
--
-- The second is why this job takes two supersteps rather than one. Everything
-- halts at the end of superstep 1, but the markers arrive after that count is
-- taken, so the master sees work left to do and runs a superstep in which the
-- markers do nothing but stop.
--
-- Configured through roles_cfg.app_cfg:
--
--   graph     -- the two-section text graph to load, relative to this directory
--   threshold -- edges with a smaller weight are deleted (an edge whose weight
--                equals the threshold is kept)

local loader = require('pregel.loader')
local common = require('examples.common')

local HERE = common.here()

local app = {}

function app.obtain_name(vertex)
    return vertex.name
end

function app.worker_context(app_cfg)
    local cfg = common.cfg(app_cfg, {'threshold'})
    return {threshold = cfg.threshold}
end

function app.compute(self)
    local value = self:get_value()

    if value.orphan_of ~= nil then
        -- A marker, added between the previous superstep and this one. It has
        -- no edges and nothing to say; it is here to be counted.
        self:vote_halt(true)
        return
    end

    local threshold = self:get_worker_context().threshold

    -- Deletions are matched by destination name, so two edges to the same
    -- destination stand or fall together however different their weights are.
    -- This graph has no parallel edges; one that had would need the whole
    -- edge list rewritten instead.
    local kept = 0
    for _, destination, weight in self:pairs_edges() do
        if weight < threshold then
            self:delete_edge(destination)
        else
            kept = kept + 1
        end
    end

    -- Counted, not read back: the deletions above are still pending, so
    -- pairs_edges would answer with the list this vertex started the superstep
    -- with. A vertex that never had an out-edge is stranded too, and gets a
    -- marker for the same reason.
    if kept == 0 then
        self:add_vertex({
            name      = self:get_name() .. ':orphan',
            orphan_of = self:get_name(),
        })
    end

    self:vote_halt(true)
end

--- Load the graph on the master and shard it out to the workers.
function app.master_preload(instance, app_cfg)
    local cfg = common.cfg(app_cfg, {'graph'})
    return loader.graph_edges_f(instance,
                                common.resolve(HERE, cfg.graph, 'graph'))
end

return app
