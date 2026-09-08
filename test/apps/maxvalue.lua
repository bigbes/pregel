--- The app module the roles tests point `roles_cfg.app` at.
--
-- Max-value over a ring: every vertex ends up holding the largest value in the
-- graph, so a single number checks the whole run on every worker.
--
-- The graph is built here rather than read from a fixture file because the
-- point of these tests is the role wiring, and a graph whose answer is known
-- by construction (the values are 1..N, so the answer is N) keeps the
-- assertions to one line. The size comes from the environment so the test can
-- pick it without a second app module.

local loader = require('pregel.loader')

local VERTEX_COUNT = tonumber(os.getenv('PREGEL_TEST_VERTICES')) or 12

local app = {}

app.vertex_count = VERTEX_COUNT

--- Vertex `i` of the ring, 1-based.
--
-- The values are a permutation of 1..N rather than i itself, so a run that
-- somehow kept each vertex's own value would not accidentally look right.
function app.vertex(i)
    return {
        name  = string.format('v%03d', i),
        value = ((i * 7 - 1) % VERTEX_COUNT) + 1,
    }
end

function app.obtain_name(vertex)
    return vertex.name
end

function app.compute(self)
    local value = self:get_value().value
    local best = value
    for _, msg in self:pairs_messages() do
        if msg > best then
            best = msg
        end
    end
    if self:get_superstep() == 1 or best > value then
        local v = self:get_value()
        self:set_value({name = v.name, value = best})
        for _, dest in self:pairs_edges() do
            self:send_message(dest, best)
        end
    end
    -- Contributed by every vertex on every superstep, so the master's copy
    -- after a superstep is the largest value any vertex held during it. It is
    -- here to exercise an app-declared aggregator end to end: the role has to
    -- add it on both the workers and the master, under the same name.
    self:set_aggregation('max_seen', best)
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

--- Push the ring out from the master.
function app.master_preload(instance)
    return loader.new(instance, function(self)
        for i = 1, VERTEX_COUNT do
            self:store_vertex(app.vertex(i))
        end
        for i = 1, VERTEX_COUNT do
            local src  = app.vertex(i).name
            local dest = app.vertex((i % VERTEX_COUNT) + 1).name
            self:store_edge(src, dest, 1)
        end
        self:flush()
    end)
end

return app
