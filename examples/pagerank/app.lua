--- pagerank: the rank of every vertex after a fixed number of iterations.
--
-- The one example here that cannot be written with messages alone. Two facts
-- are properties of the whole graph rather than of any vertex, and both reach
-- the vertices through aggregators:
--
--   count    -- how many vertices there are. Nobody is told; every vertex
--               contributes 1 and reads the merged total back, which is what
--               makes 1/N knowable.
--   dangling -- the total rank of the vertices with no out-edges. A dangling
--               vertex has nowhere to send its share, so without this the
--               ranks leak away a little on every iteration and stop summing
--               to one. What it holds is spread uniformly over the graph in
--               the next superstep.
--
-- An aggregator is per-superstep: the master resets its copy before the
-- workers report into it, so what a vertex reads in superstep S is what the
-- whole graph contributed in S-1. That one-superstep lag is why the timeline
-- below starts at 1 and the first arithmetic happens at 2.
--
--   superstep 1  contribute to `count`, nothing else is knowable yet
--   superstep 2  rank := 1/N, send rank/out-degree along each edge
--   superstep 3  the first power iteration, reading superstep 2's messages
--   ...
--   superstep iterations + 2  the last one; nothing left to send, halt
--
-- Configured through roles_cfg.app_cfg:
--
--   vertices, edges -- the Avro files, relative to this directory
--   damping         -- 0.85 unless said otherwise
--   iterations      -- 30 unless said otherwise
--
-- Unlike sssp this job does not stop by itself: PageRank converges rather than
-- terminates, so the vertices count supersteps and halt together.

local loader = require('pregel.loader')
local common = require('examples.common')

local HERE = common.here()

local DEFAULT_DAMPING    = 0.85
local DEFAULT_ITERATIONS = 30

local app = {}

function app.obtain_name(vertex)
    return vertex.name
end

function app.worker_context(app_cfg)
    local cfg = common.cfg(app_cfg)
    return {
        damping    = cfg.damping or DEFAULT_DAMPING,
        iterations = cfg.iterations or DEFAULT_ITERATIONS,
    }
end

function app.compute(self)
    local context   = self:get_worker_context()
    local superstep = self:get_superstep()
    local name      = self:get_value().name

    -- Contributed on every superstep, because the merged value is reset
    -- between them: a vertex that only voted once would leave N at zero from
    -- the second iteration on.
    self:set_aggregation('count', 1)

    if superstep == 1 then
        -- N is not known yet, so there is no arithmetic to do. This superstep
        -- exists to get `count` merged and handed back.
        self:set_value({name = name, rank = 0})
        self:vote_halt(false)
        return
    end

    local n = self:get_aggregation('count')

    local out_degree = 0
    for _ in self:pairs_edges() do
        out_degree = out_degree + 1
    end

    local rank
    if superstep == 2 then
        -- The starting distribution: every vertex equally likely.
        rank = 1 / n
    else
        local incoming = 0
        for _, message in self:pairs_messages() do
            incoming = incoming + message
        end
        -- Last superstep's dangling mass, spread over the whole graph.
        local leaked = self:get_aggregation('dangling') / n
        rank = (1 - context.damping) / n
             + context.damping * (incoming + leaked)
    end
    self:set_value({name = name, rank = rank})

    if out_degree == 0 then
        self:set_aggregation('dangling', rank)
    end

    if superstep < context.iterations + 2 then
        for _, destination in self:pairs_edges() do
            self:send_message(destination, rank / out_degree)
        end
        -- Nothing here converges on its own, so a vertex stays awake until the
        -- iteration count says otherwise.
        self:vote_halt(false)
    else
        self:vote_halt(true)
    end
end

local function add(old, new)
    return old + new
end

app.aggregators = {
    count    = {default = 0, reduce = add, merge = add},
    dangling = {default = 0, reduce = add, merge = add},
}

--- The master reads both Avro files whole and shards the graph out.
--
-- The same loader that examples/sssp runs on every worker: called without a
-- worker index it keeps everything rather than its own share.
function app.master_preload(instance, app_cfg)
    local cfg = common.cfg(app_cfg, {'vertices', 'edges'})
    return loader.avro_files(instance, {
        vertices    = common.resolve(HERE, cfg.vertices, 'vertices'),
        edges       = common.resolve(HERE, cfg.edges, 'edges'),
        vertex_name = 'name',
        edge_src    = 'src',
        edge_dst    = 'dst',
    })
end

return app
