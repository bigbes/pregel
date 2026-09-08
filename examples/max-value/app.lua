--- max-value: every vertex ends up holding the largest value in its component.
--
-- The smallest interesting Pregel program: a vertex takes the largest value it
-- has been told about, and if that changed anything it tells its out-neighbours.
-- The graph goes quiet once nothing improves, which is what stops the job -- no
-- superstep count is fixed anywhere.
--
-- Configured through roles_cfg.app_cfg:
--
--   graph -- the two-section text graph to load, relative to this directory
--
-- The master loads the whole file and shards it out; see examples/sssp for the
-- other arrangement, where every worker reads its own share of an Avro file.

local loader = require('pregel.loader')
local common = require('examples.common')

local HERE = common.here()

local app = {}

--- Vertices are named by the file's own numbering, not by the person's name.
--
-- soc-Epinions-custom.txt has 4684 duplicate names among its 75879 vertices,
-- so naming by `name` would silently merge them -- two vertices with one name
-- are one vertex to pregel, which routes and stores by exactly this string.
function app.obtain_name(vertex)
    return tostring(vertex.id)
end

--- Fold two messages into one: only the largest can ever matter.
--
-- With a combiner a vertex reads one message per superstep instead of one per
-- in-edge, which on this graph is the difference between 508k messages and
-- 76k.
app.combiner = math.max

function app.compute(self)
    local vertex = self:get_value()
    local best = vertex.value
    for _, message in self:pairs_messages() do
        if message > best then
            best = message
        end
    end

    -- Superstep 1 is the one where nothing has improved yet and every vertex
    -- still has to announce what it holds.
    if self:get_superstep() == 1 or best > vertex.value then
        self:set_value({id = vertex.id, name = vertex.name, value = best})
        for _, destination in self:pairs_edges() do
            self:send_message(destination, best)
        end
    end

    -- Reported by every vertex on every superstep, so the master's copy after
    -- a superstep is the largest value any vertex held during it -- the answer,
    -- readable from the master without touching a worker.
    self:set_aggregation('max_seen', best)

    -- A halted vertex wakes up again when a message arrives for it.
    self:vote_halt(true)
end

app.aggregators = {
    max_seen = {
        default = 0,
        reduce  = function(old, new)
            if new > old then
                return new
            end
            return old
        end,
    },
}

--- Load the graph on the master and shard it out to the workers.
function app.master_preload(instance, app_cfg)
    local cfg = common.cfg(app_cfg, {'graph'})
    return loader.graph_edges_f(instance,
                                common.resolve(HERE, cfg.graph, 'graph'))
end

return app
