--- An app module whose compute function raises on one named vertex.
--
-- What it is for is the reporting: a compute failure crosses the net.box hop
-- from the worker that raised it to the master that drives the job, and the
-- master role has to hand what it carries -- the vertex, the superstep, the
-- traceback -- to whoever reads status(). A job that fails with a plain string
-- is the other half of that and needs no app of its own: PREGEL_TEST_FAIL_ON
-- names the vertex, and leaving it unset makes this app compute nothing at all.

local loader = require('pregel.loader')

local VERTEX_COUNT = 6
local FAIL_ON = os.getenv('PREGEL_TEST_FAIL_ON') or 'v003'

local app = {}

function app.obtain_name(vertex)
    return vertex.name
end

local function vertex(i)
    return {name = string.format('v%03d', i), value = i}
end

function app.compute(self)
    if self:get_name() == FAIL_ON then
        error('the app module refused to compute')
    end
    self:vote_halt(true)
end

--- A ring, so every vertex is woken at least once.
function app.master_preload(instance)
    return loader.new(instance, function(self)
        for i = 1, VERTEX_COUNT do
            self:store_vertex(vertex(i))
        end
        for i = 1, VERTEX_COUNT do
            self:store_edge(vertex(i).name,
                            vertex((i % VERTEX_COUNT) + 1).name, 1)
        end
        self:flush()
    end)
end

return app
