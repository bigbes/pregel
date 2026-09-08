--- The BSP barrier, across real processes.
--
-- Pregel's contract is one line long: a message sent in superstep S is read in
-- S+1, by the vertex it was addressed to, exactly once. Everything else the
-- library does -- the queue swap, `count`, the aggregator round trip -- assumes
-- it, and none of it says anything when it is broken. A message that arrives
-- late is read in S+2 or dropped by the truncate that follows the swap, and
-- the run still ends with a plausible-looking answer.
--
-- So the test tags every message with the superstep that sent it and counts
-- what each superstep read. Two conditions make the window wide enough to see:
--
--   wal_mode = 'write'  -- Tarantool's own default, and the reason it matters:
--                          every space write on the receiving side yields, so
--                          a batch still in flight interleaves with whatever
--                          the master asks next.
--   a compute that yields after its last send_message, with messages large
--                          enough to take more than one write.
--
-- Neither is exotic; between them they are what the stock configuration of a
-- real deployment looks like.

local t = require('luatest')
local fio = require('fio')

local cluster = require('test.helpers.cluster')
local graph_fixture = require('test.helpers.graph_fixture')

local g = t.group('integration.barrier')

local VAR = fio.pathjoin(fio.cwd(), 'test', 'var', 'barrier')
-- Enough that a superstep takes longer than the pusher's 10ms poll interval:
-- with a couple of dozen vertices per worker the whole superstep is over before
-- the background fiber ever wakes, and then there is no in-flight batch to be
-- wrong about.
local VERTEX_COUNT = 500
local WORKER_COUNT = 3
-- The ring plus its one chord: how many messages one superstep sends.
local EDGE_COUNT = VERTEX_COUNT + 1
-- Supersteps 1..4 send; superstep 5 reads the last of them and halts.
local SENDING_SUPERSTEPS = 4
-- Room for the supersteps a broken barrier adds. The counters are dense arrays
-- of this length because a sparse Lua table comes back from Server:exec() with
-- box.NULL in the holes, which is a msgpack fact rather than a pregel one.
local MAX_SUPERSTEPS = 12

local GRAPH_PATH, VERTICES
-- Not VERTEX_COUNT: the fixture is real data and two of its first 500 lines
-- carry the same name ('Craig Jackson'), which is one vertex to pregel because
-- a vertex is its name. The edges are per id and all of them survive, so the
-- message counts below are unaffected -- only the vertex count is.
local DISTINCT_VERTICES

-- Travels to the worker processes as source, so it can use nothing from this
-- file. _G is the worker process's own; rawset/rawget rather than a plain
-- assignment because a strict _G would refuse the first write.
local BSP_COMPUTE = [[
function(self)
    local fiber = require('fiber')
    local stats = rawget(_G, 'bsp_stats')
    if stats == nil then
        -- Dense from the start: a table with holes comes back through
        -- Server:exec() with box.NULL where the holes were.
        stats = {sent = {}, received = {}, bad = 0, late = {}}
        for i = 1, 12 do
            stats.sent[i] = 0
            stats.received[i] = 0
        end
        rawset(_G, 'bsp_stats', stats)
    end

    local s = self:get_superstep()
    for _, message in self:pairs_messages() do
        stats.received[s] = (stats.received[s] or 0) + 1
        if message[1] ~= s - 1 then
            -- Sent in some superstep other than the previous one: this is the
            -- defect, and the tag says how far off it landed.
            stats.bad = stats.bad + 1
            table.insert(stats.late, {read_in = s, sent_in = message[1]})
        end
    end

    if s < 5 then
        -- Big enough that delivering one batch is a slow WAL write on the
        -- receiving side, so a batch still in the pusher's hands when the
        -- sender's flush() returns is still in flight when the master asks the
        -- receiver for the next thing.
        local payload = string.rep('x', 4096)
        for _, dest in self:pairs_edges() do
            self:send_message(dest, {s, payload})
            stats.sent[s] = (stats.sent[s] or 0) + 1
        end
        -- After the last send, so the superstep's final batch is still in the
        -- pusher's hands when run_superstep reaches its flush().
        fiber.yield()
        self:vote_halt(false)
    else
        self:vote_halt(true)
    end
end
]]

g.before_all(function()
    fio.rmtree(VAR)
    fio.mktree(VAR)
    GRAPH_PATH = fio.pathjoin(VAR, 'ring50.txt')
    local _, vertices = graph_fixture.ring(GRAPH_PATH, VERTEX_COUNT)
    VERTICES = vertices
    t.assert_equals(#VERTICES, VERTEX_COUNT)

    local seen = {}
    DISTINCT_VERTICES = 0
    for _, v in ipairs(VERTICES) do
        if not seen[v.name] then
            seen[v.name] = true
            DISTINCT_VERTICES = DISTINCT_VERTICES + 1
        end
    end
end)

local c

g.after_each(function()
    if c ~= nil then
        c:stop()
        c = nil
    end
end)

--- Sum the per-worker counters into one {sent, received, bad, late} picture.
local function collect_stats()
    local total = {sent = {}, received = {}, bad = 0, late = {}}
    for step = 1, MAX_SUPERSTEPS do
        total.sent[step] = 0
        total.received[step] = 0
    end
    for _, stats in ipairs(c:each_worker(function()
        return rawget(_G, 'bsp_stats')
    end)) do
        t.assert_not_equals(stats, nil, 'a worker never ran the compute')
        for step = 1, MAX_SUPERSTEPS do
            total.sent[step] = total.sent[step] + (stats.sent[step] or 0)
            total.received[step] = total.received[step] +
                                   (stats.received[step] or 0)
        end
        total.bad = total.bad + stats.bad
        for _, entry in ipairs(stats.late or {}) do
            table.insert(total.late, entry)
        end
    end
    return total
end

g.test_messages_arrive_in_the_next_superstep_under_wal = function()
    c = cluster.new(WORKER_COUNT, {wal_mode = 'write'})
    -- A small batch, so a superstep's messages leave as a dozen batches rather
    -- than one and the pusher is reliably still carrying the last of them when
    -- run_superstep reaches its flush(). Whether the defect shows at the
    -- default 1000 is down to scheduling luck; this makes it show every time.
    c:create_workers('barrier', BSP_COMPUTE, {pool_size = 4})
    c:create_master('barrier', GRAPH_PATH)

    local supersteps = c:run()
    local stats = collect_stats()

    -- The interesting assertion, and the one the defect breaks: not "roughly
    -- the right number of messages arrived" but "every message was read in the
    -- superstep after the one that sent it".
    t.assert_equals(stats.bad, 0,
                    'messages read in the wrong superstep: ' ..
                    require('json').encode(stats.late))

    for step = 1, SENDING_SUPERSTEPS do
        t.assert_equals(stats.sent[step], EDGE_COUNT,
                        'superstep ' .. step .. ' sent the wrong number')
        t.assert_equals(stats.received[step + 1], EDGE_COUNT,
                        'superstep ' .. (step + 1) .. ' read the wrong number')
    end
    -- Nothing read in superstep 1, and nothing left over. A late message shows
    -- up as an extra superstep too: the master keeps going while any worker
    -- still has one queued.
    t.assert_equals(stats.received[1], 0)
    t.assert_equals(supersteps, SENDING_SUPERSTEPS + 1)
    t.assert_equals(c:pending_messages('barrier'), 0)
end

-- The other half of the same barrier: `count` runs after the preload, and the
-- preload's last batch has to be applied before it does. It was not, so the
-- master counted a graph that was still arriving, in_progress went negative and
-- the superstep loop -- which stops at zero -- never stopped.
g.test_count_sees_the_whole_preload_under_wal = function()
    c = cluster.new(WORKER_COUNT, {wal_mode = 'write'})
    c:create_workers('preload', BSP_COMPUTE)
    c:create_master('preload', GRAPH_PATH)

    local counted = c.master:exec(function()
        local m = _G.master_instance
        m:wait_up()
        m:preload()
        -- What master:start() does first, and the whole graph has to be
        -- visible to it: the preload's flush is a barrier or it is nothing.
        local total = 0
        for _, rv in ipairs(m.mpool:send_wait('count')) do
            total = total + rv[2]
        end
        return total
    end)

    t.assert_equals(counted, DISTINCT_VERTICES)

    -- And every vertex really is stored, not merely counted.
    local vertices = c:collect_vertices('preload')
    local stored = 0
    for _ in pairs(vertices) do stored = stored + 1 end
    t.assert_equals(stored, DISTINCT_VERTICES)
end
