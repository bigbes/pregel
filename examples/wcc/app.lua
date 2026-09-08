--- wcc: weakly connected components, by label propagation.
--
-- Every vertex starts labelled with its own name and keeps the smallest label
-- it is offered, passing on anything that improved. When the graph goes quiet,
-- two vertices carry the same label exactly when they are in the same
-- component, and that label is the smallest name in it -- so the label is both
-- the answer and the component's identity.
--
-- "Weakly" connected means direction is ignored, and pregel only ever walks
-- out-edges: a message travels from a vertex to its out-neighbours and never
-- back. So the *graph* has to be symmetric, not the algorithm -- every edge
-- present in both directions. That is what the '-bi' fixtures are, and it is
-- the one thing to get right before running this on a graph of your own.
--
-- Configured through roles_cfg.app_cfg:
--
--   graph -- the two-section text graph to load, relative to this directory
--
-- Compare examples/max-value: the same shape of propagation, and the same
-- reason it terminates, over a smaller-is-better order on strings instead of a
-- larger-is-better one on numbers.

local loader = require('pregel.loader')
local common = require('examples.common')

local HERE = common.here()

local app = {}

--- Vertices are named by the file's own numbering, not by the person's name.
--
-- soc-Epinions-custom-bi.txt has 4684 duplicate names among its 75879
-- vertices, and merging two vertices into one would merge their components
-- with them -- which for a connectivity algorithm is not a small error but the
-- whole answer.
function app.obtain_name(vertex)
    return tostring(vertex.id)
end

--- The smaller of two labels. Not math.min: these are strings.
--
-- The order is lexicographic over the vertex names, so the label of a
-- component is the name that sorts first in it, whatever that means for the
-- names in use. Any total order would do -- the algorithm needs one, not a
-- particular one.
function app.combiner(a, b)
    if b < a then
        return b
    end
    return a
end

function app.compute(self)
    local value = self:get_value()
    -- On superstep 1 the value is still what the loader stored, and a vertex's
    -- own name is the only label it has.
    local label = value.label or self:get_name()

    local best = label
    for _, message in self:pairs_messages() do
        if message < best then
            best = message
        end
    end

    -- Superstep 1 is the one where nothing has improved yet and every vertex
    -- still has to announce itself.
    if self:get_superstep() == 1 or best < label then
        self:set_value({id = value.id, name = value.name, label = best})
        for _, destination in self:pairs_edges() do
            self:send_message(destination, best)
        end
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
