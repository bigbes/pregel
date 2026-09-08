--- sssp: single-source shortest paths over a weighted directed graph.
--
-- The source starts at distance 0 and everything else at infinity. A vertex
-- that learns of a shorter route than the one it holds stores it and offers
-- `distance + weight` to each of its out-neighbours; a vertex that learns
-- nothing better says nothing, so the job stops of its own accord once no
-- distance improves. The combiner is math.min: of the several routes that
-- reach a vertex in one superstep, only the shortest can matter.
--
-- Configured through roles_cfg.app_cfg:
--
--   source   -- the name of the vertex to measure from
--   vertices -- the Avro vertex file, relative to this directory
--   edges    -- the Avro edge file
--
-- Unlike max-value this example loads on the *workers*: the loader is
-- `worker_preload`, so all three read the same two Avro files at once and each
-- keeps only the vertices whose names shard to it. Nothing coordinates that --
-- the split uses the same mpool:id() that routes every message, so the three
-- shares cover the graph exactly once between them.

local loader = require('pregel.loader')
local common = require('examples.common')

local HERE = common.here()

-- A vertex nothing reaches keeps this, and it survives a round trip through
-- msgpack: an unreachable vertex is `inf` in the space, not a magic number a
-- reader has to know about.
local INF = math.huge

local app = {}

function app.obtain_name(vertex)
    return vertex.name
end

--- Of the routes offered to one vertex in one superstep, keep the shortest.
app.combiner = math.min

--- What every compute call reads out of the cluster config.
--
-- The role calls this with roles_cfg.app_cfg and hands the result to
-- vertex:get_worker_context(); a compute function is given nothing else.
function app.worker_context(app_cfg)
    local cfg = common.cfg(app_cfg, {'source'})
    return {source = cfg.source}
end

function app.compute(self)
    local context = self:get_worker_context()
    local value   = self:get_value()
    local name, distance

    if self:get_superstep() == 1 then
        -- The loader stored the Avro record; this is where it becomes an SSSP
        -- vertex. Doing it here rather than in the loader keeps the loader
        -- ignorant of which vertex is the source, so a second run from a
        -- different source needs no reload.
        name = value.name
        distance = (name == context.source) and 0 or INF
        self:set_value({name = name, dist = distance})
    else
        name, distance = value.name, value.dist
    end

    local best = distance
    for _, message in self:pairs_messages() do
        if message < best then
            best = message
        end
    end

    -- Superstep 1 is the only one on which a vertex speaks without having
    -- improved: the source has to announce its own zero, and nobody else has
    -- anything finite to announce.
    local announce = (self:get_superstep() == 1 and distance < INF)
    if best < distance then
        distance = best
        self:set_value({name = name, dist = distance})
        announce = true
    end
    if announce then
        for _, destination, weight in self:pairs_edges() do
            self:send_message(destination, distance + weight)
        end
    end

    self:vote_halt(true)
end

-- No aggregators: this example needs nothing that is not in a vertex or in a
-- message. See examples/max-value for one (a max over the graph) and
-- examples/pagerank for the harder kind, where the value of one superstep is
-- an input to the next.

--- Every worker reads its own share of the two Avro files.
function app.worker_preload(instance, app_cfg)
    local cfg = common.cfg(app_cfg, {'vertices', 'edges'})
    return loader.avro_files(instance, {
        vertices    = common.resolve(HERE, cfg.vertices, 'vertices'),
        edges       = common.resolve(HERE, cfg.edges, 'edges'),
        vertex_name = 'name',
        edge_src    = 'src',
        edge_dst    = 'dst',
        edge_value  = 'weight',
    })
end

return app
