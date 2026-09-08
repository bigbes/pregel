local t = require('luatest')
local clock = require('clock')
local fiber = require('fiber')

local box_helper = require('test.helpers.box')
local mpool = require('pregel.mpool')

local g = t.group('mpool')

local URI

-- Every bucket in these tests points at this very instance, so it is local and
-- reaches the registry directly. That is the interesting half anyway: the
-- remote half is one conn:call() against the same function names, and the
-- integration test drives it across real processes.
local recorded

local function install_registry()
    recorded = {batches = {}, singles = {}}
    rawset(_G, 'pregel', rawget(_G, 'pregel') or {})
    _G.pregel.worker = {
        deliver = function(name, msg, args)
            table.insert(recorded.singles, {name = name, msg = msg, args = args})
            if msg == 'boom' then
                error('deliver refused ' .. tostring(msg))
            end
            return 'ok:' .. tostring(msg)
        end,
        deliver_batch = function(name, msgs)
            local copy = {}
            for i, m in ipairs(msgs) do
                copy[i] = {m[1], m[2]}
            end
            table.insert(recorded.batches, {name = name, msgs = copy})
            return #msgs
        end,
    }
end

-- _G.pregel is process-wide, and pregel.worker registers into it when the
-- suite loads. Stubbing it here would otherwise leave every later group
-- talking to a stub -- or, after the missing-entry test below, to nothing.
local saved_registry

g.before_all(function()
    URI = box_helper.listen_uri()
    saved_registry = rawget(_G, 'pregel') and _G.pregel.worker or nil
end)

g.after_all(function()
    if rawget(_G, 'pregel') ~= nil then
        _G.pregel.worker = saved_registry
    end
end)

g.before_each(function()
    install_registry()
end)

--- A pool of `n` local buckets with the background pushers stopped, so a test
-- decides when a flush happens instead of racing a 10ms timer.
local function quiet_pool(n, options)
    local servers = {}
    for i = 1, n do servers[i] = URI end
    local pool = mpool.new('unit', servers, options)
    for _, bucket in ipairs(pool.buckets) do
        bucket.stopped = true
    end
    -- Let the pushers notice; their longest wait is 10ms.
    fiber.sleep(0.05)
    return pool
end

--- Run `fn` in a fiber and fail the test if it has not returned in `seconds`.
--
-- luatest has no per-test timeout, so a defect whose shape is "nobody ever
-- answers" used to hang the whole suite rather than fail one test -- which in
-- CI is a killed job with no test name in it. Anything here that could wait on
-- another fiber goes through this.
local function within(seconds, fn)
    local rv
    local runner = fiber.new(function()
        local ok, res = pcall(fn)
        rv = {ok, res}
    end)
    runner:set_joinable(true)
    local returned = pcall(t.helpers.retrying,
                           {timeout = seconds, delay = 0.01},
                           function() t.assert_not_equals(rv, nil) end)
    if not returned then
        runner:cancel()
        t.fail(string.format('did not return within %s second(s)', seconds))
    end
    runner:join()
    return rv[1], rv[2]
end

local function drop_delayed(pool)
    for _, bucket in ipairs(pool.buckets) do
        bucket:drop()
    end
end

-------------------------------------------------------------------------------
-- guava_name
-------------------------------------------------------------------------------

g.test_guava_name_is_deterministic = function()
    for _, name in ipairs({'alice', 'bob', 'vertex-00042', ''}) do
        local first = mpool.guava_name(name, 16)
        for _ = 1, 10 do
            t.assert_equals(mpool.guava_name(name, 16), first, name)
        end
    end
end

g.test_guava_name_is_in_range = function()
    for cnt = 1, 8 do
        for i = 1, 500 do
            local id = mpool.guava_name('vertex-' .. tostring(i), cnt)
            t.assert_ge(id, 1)
            t.assert_le(id, cnt)
        end
    end
end

g.test_guava_name_with_one_bucket = function()
    t.assert_equals(mpool.guava_name('anything', 1), 1)
end

g.test_guava_name_spreads_over_buckets = function()
    local counts = {}
    local cnt = 8
    local total = 4000
    for i = 1, total do
        local id = mpool.guava_name('vertex-' .. tostring(i), cnt)
        counts[id] = (counts[id] or 0) + 1
    end
    for id = 1, cnt do
        -- An even split is total/cnt; anything within a factor of two of that
        -- is a working hash and anything outside it is not.
        t.assert_ge(counts[id] or 0, total / cnt / 2,
                    'bucket ' .. id .. ' is starved')
        t.assert_le(counts[id], total / cnt * 2,
                    'bucket ' .. id .. ' is overloaded')
    end
end

-- Jump consistent hashing: growing the ring must move a name to a new bucket
-- or leave it where it was, never shuffle everything.
g.test_guava_name_is_stable_when_growing = function()
    local moved = 0
    local total = 2000
    for i = 1, total do
        local name = 'vertex-' .. tostring(i)
        if mpool.guava_name(name, 8) ~= mpool.guava_name(name, 9) then
            moved = moved + 1
        end
    end
    -- Ideally total/9 move; allow generous slack, but nothing like all of them.
    t.assert_le(moved, total / 3)
    t.assert_gt(moved, 0)
end

g.test_guava_name_accepts_numbers_and_tables = function()
    t.assert_equals(type(mpool.guava_name(42, 4)), 'number')
    local id = mpool.guava_name({'a', 'b'}, 4)
    t.assert_equals(mpool.guava_name({'a', 'b'}, 4), id)
    t.assert_ge(id, 1)
    t.assert_le(id, 4)
end

-------------------------------------------------------------------------------
-- Bucket wiring
-------------------------------------------------------------------------------

g.test_local_bucket_is_detected = function()
    local pool = quiet_pool(1)
    local bucket = pool.buckets[1]
    -- box.info.server.uuid is gone in Tarantool 3; the peer uuid comes from
    -- the connection greeting and needs no eval.
    t.assert_equals(bucket.uuid, box.info.uuid)
    t.assert_equals(bucket.is_local, true)
    -- A local bucket holds no connection at all.
    t.assert_equals(bucket.connection, nil)
    t.assert_equals(pool.self_idx, 1)
    pool:stop()
end

g.test_pool_shape = function()
    local pool = quiet_pool(4)
    t.assert_equals(pool.bucket_cnt, 4)
    t.assert_equals(#pool.buckets, 4)
    for idx, bucket in ipairs(pool.buckets) do
        t.assert_equals(bucket.id, idx)
        t.assert_equals(bucket.name, 'unit')
    end
    pool:stop()
end

g.test_by_id_agrees_with_guava_name = function()
    local pool = quiet_pool(4)
    for i = 1, 200 do
        local name = 'vertex-' .. tostring(i)
        local id = pool:id(name)
        t.assert_equals(id, mpool.guava_name(name, 4))
        t.assert_is(pool:by_id(name), pool.buckets[id])
    end
    pool:stop()
end

g.test_rejects_empty_server_list = function()
    t.assert_error_msg_contains('at least one server', function()
        mpool.new('unit', {})
    end)
end

-------------------------------------------------------------------------------
-- Connecting
-------------------------------------------------------------------------------

local DOWN = 'unix/:./no-such-peer.iproto'

--- A URI that nothing listens on, distinct from DOWN.
local DOWN2 = 'unix/:./no-such-peer-2.iproto'

-- The whole point of connect_async: a Tarantool 3 role builds its pool inside
-- apply(), which holds up the instance's config startup, so the pool must be
-- usable as an object before any peer has answered.
g.test_construction_does_not_wait_for_a_peer = function()
    local started = clock.monotonic()
    local pool = mpool.new('unit', {DOWN}, {connect_async = true})
    local elapsed = clock.monotonic() - started
    t.assert_lt(elapsed, 1, 'mpool.new blocked on an unreachable peer')
    t.assert_equals(pool.connected, false)
    pool:stop()
end

-- The bucket order decides which worker owns which vertex, so it has to be the
-- same on every instance -- and it used to be derived from the peer uuids,
-- which is why the pool could not be built without connecting first.
g.test_bucket_order_does_not_depend_on_the_listed_order = function()
    local one = mpool.new('unit', {DOWN, DOWN2}, {connect_async = true})
    local two = mpool.new('unit', {DOWN2, DOWN}, {connect_async = true})
    local function uris(pool)
        local rv = {}
        for _, bucket in ipairs(pool.buckets) do
            table.insert(rv, bucket.uri)
        end
        return rv
    end
    t.assert_equals(uris(one), uris(two))
    t.assert_equals(uris(one), {DOWN2, DOWN}, 'the order is the sorted one')
    one:stop()
    two:stop()
end

g.test_wait_connected_resolves_the_local_bucket = function()
    local pool = mpool.new('unit', {URI}, {connect_async = true})
    t.assert_equals(pool.self_idx, 0)
    pool:wait_connected(30)
    t.assert_equals(pool.connected, true)
    t.assert_equals(pool.self_idx, 1)
    t.assert_equals(pool.buckets[1].is_local, true)
    t.assert_equals(pool.buckets[1].connection, nil)
    t.assert_equals(pool.buckets[1].uuid, box.info.uuid)
    pool:stop()
end

-- net.box knows exactly why it could not connect; reporting a bare timeout
-- instead is what made a wrong password and a missing instance the same
-- 30-second message.
g.test_wait_connected_reports_why_a_peer_is_unreachable = function()
    local pool = mpool.new('unit', {DOWN}, {connect_async = true})
    local ok, err = pcall(pool.wait_connected, pool, 0.5)
    pool:stop()
    t.assert_equals(ok, false)
    err = tostring(err)
    t.assert_str_contains(err, DOWN)
    t.assert_str_contains(err, 'No such file or directory')
end

-- Retrying a rejected authentication is hopeless, so waiting the whole connect
-- timeout out only delays a message that is already available.
g.test_wait_connected_refuses_bad_credentials_at_once = function()
    local pool = mpool.new('unit', {URI}, {
        connect_async = true,
        user          = 'no-such-user',
        password      = 'wrong',
    })
    local started = clock.monotonic()
    local ok, err = pcall(pool.wait_connected, pool, 30)
    local elapsed = clock.monotonic() - started
    pool:stop()
    t.assert_equals(ok, false)
    t.assert_str_contains(tostring(err),
                          'User not found or supplied credentials are invalid')
    t.assert_lt(elapsed, 10, 'waited out the connect timeout on a hopeless auth')
end

-- A listener with parameters -- `transport: ssl` and the client-side ssl_*
-- files -- reaches net.box only as {uri = ..., params = ...}; there is no
-- separate option for it.
g.test_a_server_may_carry_uri_params = function()
    local pool = mpool.new('unit', {{uri = URI, params = {}}},
                           {connect_async = true})
    t.assert_equals(pool.buckets[1].uri, URI)
    t.assert_equals(pool.buckets[1].params, {})
    pool:wait_connected(30)
    t.assert_equals(pool.buckets[1].is_local, true)
    pool:stop()
end

-- self_idx is what a worker-side loader is handed as its share of the graph,
-- and it is only known once the pool has resolved its connections. Whoever
-- needs it waits here rather than connecting a second time, which would race
-- the close of the local bucket's own connection.
g.test_wait_ready_blocks_until_the_pool_is_connected = function()
    local pool = mpool.new('unit', {URI}, {connect_async = true})
    local ok, err = pcall(pool.wait_ready, pool, 0.2)
    t.assert_equals(ok, false)
    t.assert_str_contains(tostring(err), 'did not reach all 1 peer(s)')

    local reached = false
    fiber.create(function()
        pool:wait_ready(10)
        reached = true
    end)
    fiber.sleep(0.05)
    t.assert_equals(reached, false, 'wait_ready returned before the connect')

    pool:wait_connected(30)
    t.helpers.retrying({timeout = 5}, function()
        t.assert_equals(reached, true)
    end)
    pool:stop()
end

g.test_a_server_must_be_a_uri_or_a_uri_table = function()
    t.assert_error_msg_contains('a URI string or a', function()
        mpool.new('unit', {42}, {connect_async = true})
    end)
    t.assert_error_msg_contains('a URI string or a', function()
        mpool.new('unit', {{params = {}}}, {connect_async = true})
    end)
end

-------------------------------------------------------------------------------
-- Instant buckets: put / flush
-------------------------------------------------------------------------------

g.test_put_and_flush_delivers_a_batch = function()
    local pool = quiet_pool(1)
    local bucket = pool.buckets[1]
    bucket:put('message.deliver', {'alice', 1, 'bob'})
    bucket:put('message.deliver', {'carol', 2, 'bob'})
    t.assert_equals(bucket.count, 2)
    t.assert_equals(#recorded.batches, 0)

    bucket:flush()

    t.assert_equals(bucket.count, 0)
    t.assert_equals(#recorded.batches, 1)
    t.assert_equals(recorded.batches[1].name, 'unit')
    t.assert_equals(recorded.batches[1].msgs, {
        {'message.deliver', {'alice', 1, 'bob'}},
        {'message.deliver', {'carol', 2, 'bob'}},
    })
    pool:stop()
end

g.test_flush_of_empty_bucket_sends_nothing = function()
    local pool = quiet_pool(1)
    pool.buckets[1]:flush()
    t.assert_equals(#recorded.batches, 0)
    pool:stop()
end

-- The accumulation slots are reused by the next put(). A real RPC yields, so a
-- batch that merely referenced those slots gets rewritten underneath the send
-- by whatever produces during the yield -- and the wrong messages arrive under
-- the right count. The stub yields here to open exactly that window.
g.test_flush_copies_out_of_the_reused_buffer = function()
    local pool = quiet_pool(1)
    local bucket = pool.buckets[1]

    local delivered
    _G.pregel.worker.deliver_batch = function(_, msgs)
        -- Read the batch only after a yield, the way a peer on the far side of
        -- a connection would.
        fiber.sleep(0.1)
        delivered = {}
        for i, m in ipairs(msgs) do
            delivered[i] = {m[1], m[2]}
        end
    end

    bucket:put('first', {1})
    local flusher = fiber.create(function() bucket:flush() end)

    -- flush() has reset the counter before its first yield, so this refills
    -- slot 1 while the send is still in flight.
    fiber.sleep(0.02)
    t.assert_equals(bucket.count, 0)
    bucket:put('second', {2})
    t.assert_equals(bucket.count, 1)

    t.helpers.retrying({timeout = 5}, function()
        t.assert_equals(flusher:status(), 'dead')
    end)
    t.assert_equals(delivered, {{'first', {1}}})
    pool:stop()
end

g.test_mpool_flush_touches_only_nonempty_buckets = function()
    local pool = quiet_pool(4)
    pool.buckets[2]:put('m', {'x'})
    pool:flush()
    t.assert_equals(#recorded.batches, 1)
    for _, bucket in ipairs(pool.buckets) do
        t.assert_equals(bucket.count, 0)
    end
    pool:stop()
end

g.test_send_queues_on_every_bucket = function()
    local pool = quiet_pool(3)
    pool:send('aggregator.inform', {'sum', 7})
    for _, bucket in ipairs(pool.buckets) do
        t.assert_equals(bucket.count, 1)
    end
    pool:flush()
    t.assert_equals(#recorded.batches, 3)
    for _, batch in ipairs(recorded.batches) do
        t.assert_equals(batch.msgs, {{'aggregator.inform', {'sum', 7}}})
    end
    pool:stop()
end

-- The background pusher is what makes put() fire-and-forget; with it running,
-- nothing has to call flush() for a message to leave.
g.test_pusher_fiber_flushes_on_its_own = function()
    local pool = mpool.new('unit', {URI})
    pool.buckets[1]:put('auto', {1})
    t.helpers.retrying({timeout = 5}, function()
        t.assert_equals(#recorded.batches, 1)
    end)
    t.assert_equals(recorded.batches[1].msgs, {{'auto', {1}}})
    pool:stop()
end

-- A producer that outruns the batch size blocks until a flush makes room.
g.test_put_blocks_on_a_full_bucket = function()
    local pool = quiet_pool(1, {msg_count = 2})
    local bucket = pool.buckets[1]
    bucket:put('a', {1})
    bucket:put('b', {2})
    t.assert_equals(bucket.count, 2)

    local done = false
    fiber.create(function()
        bucket:put('c', {3})
        done = true
    end)
    fiber.sleep(0.05)
    t.assert_equals(done, false, 'put must wait while the bucket is full')

    -- Bounded: this test is about a producer that waits, so an unbounded call
    -- here is the one thing that could turn a failure into a hung suite.
    local flushed, err = within(10, function() bucket:flush() end)
    t.assert_equals(flushed, true, tostring(err))
    t.helpers.retrying({timeout = 5}, function()
        t.assert_equals(done, true)
    end)
    t.assert_equals(bucket.count, 1)
    pool:stop()
end

-------------------------------------------------------------------------------
-- The BSP barrier
-------------------------------------------------------------------------------

-- Defect: mpool:flush() skipped a bucket whose count was 0, and a bucket the
-- pusher has taken a batch from has a count of 0 -- so flush() returned while
-- the batch was still travelling. Everything downstream of a flush is a BSP
-- phase boundary (run_superstep, preload, the master's inform_workers), so a
-- batch that outlives one lands in the next phase: read a superstep late, or
-- dropped by the queue swap, or counted while half of it is applied.
g.test_flush_waits_for_a_batch_the_pusher_took = function()
    local pool = mpool.new('unit', {URI})
    local bucket = pool.buckets[1]

    local delivered = 0
    _G.pregel.worker.deliver_batch = function(_, msgs)
        -- The far side of a real connection yields -- every space write under
        -- a WAL does -- and that is the whole window this test is about.
        fiber.sleep(0.2)
        delivered = delivered + #msgs
        return #msgs
    end

    bucket:put('message.deliver', {'alice', 1})
    t.helpers.retrying({timeout = 5}, function()
        t.assert_equals(bucket.count, 0, 'the pusher has not taken the batch')
    end)

    pool:flush()
    t.assert_equals(delivered, 1,
                    'flush() returned with a batch still in flight')
    pool:stop()
end

-- And the batch flush() sends itself must not overtake the one already in
-- flight: a preload sends vertex.store before edge.store, and an edge that
-- arrives first is refused outright ("vertex does not exist").
g.test_flush_does_not_overtake_the_batch_in_flight = function()
    local pool = mpool.new('unit', {URI}, {msg_count = 1})
    local bucket = pool.buckets[1]

    local order = {}
    _G.pregel.worker.deliver_batch = function(_, msgs)
        -- The slow one goes first, so a second send started while it is in
        -- flight arrives before it and the reordering is visible rather than
        -- merely possible.
        if msgs[1][1] == 'first' then
            fiber.sleep(0.3)
        end
        for _, m in ipairs(msgs) do
            table.insert(order, m[1])
        end
        return #msgs
    end

    bucket:put('first', {1})
    t.helpers.retrying({timeout = 5}, function()
        t.assert_equals(bucket.count, 0, 'the pusher has not taken the batch')
    end)
    bucket:put('second', {2})

    pool:flush()
    t.assert_equals(order, {'first', 'second'})
    pool:stop()
end

-- Defect: a background flush that raised killed the pusher fiber, and nothing
-- ever noticed -- the batch was gone (flush() had already reset the counter),
-- the superstep reported 'ok', and the next producer to fill the bucket waited
-- on a flush that no longer had anyone to perform it.
g.test_a_failed_background_flush_keeps_the_pusher_alive = function()
    local pool = mpool.new('unit', {URI}, {msg_count = 4})
    local bucket = pool.buckets[1]

    local calls = 0
    _G.pregel.worker.deliver_batch = function()
        calls = calls + 1
        error('simulated worker failure')
    end

    bucket:put('m', {1})
    t.helpers.retrying({timeout = 5}, function()
        t.assert_ge(calls, 1, 'the pusher never tried to deliver')
    end)
    -- The fiber that carries every later batch must survive the failure.
    t.assert_equals(bucket.worker:status() ~= 'dead', true,
                    'the pusher fiber died on the first failure')

    pool:stop()
end

-- ... and the failure has to reach whoever is running the superstep. It used to
-- reach nobody: run_superstep's mpool:flush() saw count == 0 for a bucket whose
-- batch had died inside the pusher and returned 'ok'.
g.test_a_failed_background_flush_is_raised_to_the_producer = function()
    -- Room to spare: the point here is that put() raises, not that it blocks,
    -- so the bucket must not fill up while the retry loop waits for the pusher.
    local pool = mpool.new('unit', {URI}, {msg_count = 64})
    local bucket = pool.buckets[1]

    _G.pregel.worker.deliver_batch = function()
        error('simulated worker failure')
    end

    bucket:put('m', {1})
    t.helpers.retrying({timeout = 5}, function()
        t.assert_error_msg_contains('simulated worker failure', function()
            bucket:put('m', {2})
        end)
    end)
    t.assert_error_msg_contains('simulated worker failure', function()
        pool:flush()
    end)

    pool:stop()
end

-- The hang the two above are really about: fill the bucket after the pusher has
-- failed, and one more put() waits on a flush that will never come.
g.test_put_does_not_hang_after_a_failed_flush = function()
    local pool = mpool.new('unit', {URI}, {msg_count = 4})
    local bucket = pool.buckets[1]

    local original = _G.pregel.worker.deliver_batch
    local failing = true
    _G.pregel.worker.deliver_batch = function(name, msgs)
        if failing then
            error('simulated worker failure')
        end
        return original(name, msgs)
    end

    bucket:put('m', {1})
    t.helpers.retrying({timeout = 5}, function()
        t.assert_equals(bucket.count, 0, 'the pusher never took the batch')
    end)
    failing = false

    -- Whatever the producer does next, it must come back: either every put()
    -- goes through, or one of them raises. Blocking forever is the defect.
    local outcome
    -- fiber.new, so joinable can be set before it runs: it may well be over
    -- before the next yield, and set_joinable() on a dead fiber raises.
    local producer = fiber.new(function()
        local ok, err = pcall(function()
            for i = 1, 8 do
                bucket:put('m', {i})
            end
        end)
        outcome = ok and 'ok' or tostring(err)
    end)
    producer:set_joinable(true)

    local returned = pcall(t.helpers.retrying, {timeout = 5}, function()
        t.assert_not_equals(outcome, nil)
    end)
    if not returned then
        -- Leave nothing blocked behind: the suite runs in one process.
        producer:cancel()
        t.fail('put() is stuck on a bucket whose pusher failed')
    end
    producer:join()

    pool:stop()
end

-------------------------------------------------------------------------------
-- Delayed buckets
-------------------------------------------------------------------------------

g.test_delayed_bucket_uses_a_space = function()
    local pool = quiet_pool(2, {is_delayed = true})
    local bucket = pool.buckets[1]
    local space = box.space[bucket.space_name]
    t.assert_not_equals(space, nil)

    local format = space:format()
    t.assert_equals(format[1], {name = 'id', type = 'unsigned'})
    t.assert_equals(format[2], {name = 'msg', type = 'string'})
    t.assert_equals(format[3], {name = 'args', type = 'any'})
    -- The primary key is filled by a sequence: space:auto_increment() is gone.
    t.assert_not_equals(space.index.primary.sequence_id, nil)

    bucket:put('vertex.store', {id = 1})
    bucket:put('vertex.store', {id = 2})
    t.assert_equals(space:len(), 2)
    local ids = {}
    for _, tuple in space:pairs() do table.insert(ids, tuple[1]) end
    t.assert_not_equals(ids[1], ids[2])

    drop_delayed(pool)
    pool:stop()
end

g.test_delayed_flush_sends_and_clears = function()
    local pool = quiet_pool(1, {is_delayed = true, msg_count = 3})
    local bucket = pool.buckets[1]
    for i = 1, 7 do
        bucket:put('vertex.store', {i})
    end
    t.assert_equals(box.space[bucket.space_name]:len(), 7)

    bucket:flush()

    -- msg_count 3 over 7 messages is three batches.
    t.assert_equals(#recorded.batches, 3)
    local seen = {}
    for _, batch in ipairs(recorded.batches) do
        for _, m in ipairs(batch.msgs) do
            t.assert_equals(m[1], 'vertex.store')
            table.insert(seen, m[2][1])
        end
    end
    table.sort(seen)
    t.assert_equals(seen, {1, 2, 3, 4, 5, 6, 7})
    -- Flushed messages are gone, so a second flush is a no-op.
    t.assert_equals(box.space[bucket.space_name]:len(), 0)
    t.assert_equals(bucket.count, 0)
    bucket:flush()
    t.assert_equals(#recorded.batches, 3)

    drop_delayed(pool)
    pool:stop()
end

-- A send that fails must leave the messages behind to be retried, not drop
-- them: the delayed bucket is the only copy.
g.test_delayed_flush_keeps_messages_when_the_send_fails = function()
    local pool = quiet_pool(1, {is_delayed = true})
    local bucket = pool.buckets[1]
    bucket:put('vertex.store', {1})
    bucket:put('vertex.store', {2})

    _G.pregel.worker.deliver_batch = function()
        error('worker is down')
    end
    t.assert_error(function() bucket:flush() end)
    t.assert_equals(box.space[bucket.space_name]:len(), 2)

    drop_delayed(pool)
    pool:stop()
end

-------------------------------------------------------------------------------
-- waitpool
-------------------------------------------------------------------------------

g.test_send_wait_reaches_every_bucket = function()
    local pool = quiet_pool(4)
    local rv = pool:send_wait('count', {'arg'})

    t.assert_equals(#recorded.singles, 4)
    for _, call in ipairs(recorded.singles) do
        t.assert_equals(call.name, 'unit')
        t.assert_equals(call.msg, 'count')
        t.assert_equals(call.args, {'arg'})
    end
    -- One entry per bucket: {elapsed_seconds, results...}.
    t.assert_equals(#rv, 4)
    for _, entry in ipairs(rv) do
        t.assert_equals(type(entry[1]), 'number')
        t.assert_ge(entry[1], 0)
        t.assert_equals(entry[2], 'ok:count')
    end
    pool:stop()
end

g.test_send_wait_can_be_called_repeatedly = function()
    local pool = quiet_pool(3)
    for i = 1, 5 do
        local rv = pool:send_wait('count', {i})
        t.assert_equals(#rv, 3)
    end
    t.assert_equals(#recorded.singles, 15)
    pool:stop()
end

-- A handler whose bucket raised used to die with the error, so nothing was
-- ever put on the output channel and send_wait waited for it forever.
g.test_send_wait_reports_a_failing_bucket = function()
    local pool = quiet_pool(2)
    -- Bounded, because the defect this covers manifests as "send_wait never
    -- returns": without the bound the test hangs instead of failing.
    local ok, err = within(10, function() return pool:send_wait('boom', {}) end)
    t.assert_equals(ok, false)
    err = tostring(err)
    t.assert_str_contains(err, 'boom')
    t.assert_str_contains(err, 'deliver refused')

    -- And the pool still works afterwards.
    local ok2, rv = within(10, function() return pool:send_wait('count', {}) end)
    t.assert_equals(ok2, true, tostring(rv))
    t.assert_equals(#rv, 2)
    pool:stop()
end

-- The other half of the same story: the pcall in waitpool_handler is what keeps
-- a handler alive through a failing bucket, but a fiber can also be cancelled,
-- and a handler that dies holding its task never answers. send_wait waited for
-- that answer forever -- so a broken pcall did not fail a test, it hung the
-- suite, and in production a dead handler hung the master.
g.test_send_wait_does_not_wait_for_a_dead_handler = function()
    local pool = quiet_pool(1)

    -- A handler that is simply gone. The pcall inside the handler covers a
    -- bucket that raises -- it even survives being cancelled mid-send, which
    -- is why this kills it between tasks instead.
    pool.waitpool.fpool[1]:cancel()
    t.helpers.retrying({timeout = 5}, function()
        t.assert_equals(pool.waitpool.fpool[1]:status(), 'dead')
    end)

    local ok, err = within(15, function() return pool:send_wait('count', {}) end)
    t.assert_equals(ok, false)
    t.assert_str_contains(tostring(err), 'handler')
    pool:stop()
end

g.test_send_wait_runs_buckets_in_parallel = function()
    local pool = quiet_pool(4)
    local inflight, peak = 0, 0
    _G.pregel.worker.deliver = function()
        inflight = inflight + 1
        if inflight > peak then peak = inflight end
        fiber.sleep(0.05)
        inflight = inflight - 1
        return true
    end
    pool:send_wait('slow', {})
    t.assert_equals(peak, 4, 'all buckets must be in flight at once')
    pool:stop()
end

g.test_stop_ends_the_fibers = function()
    local pool = mpool.new('unit', {URI, URI})
    local pushers = {}
    for i, bucket in ipairs(pool.buckets) do
        pushers[i] = bucket.worker
    end
    local handlers = pool.waitpool.fpool
    t.assert_equals(#handlers, 2)

    pool:stop()

    t.helpers.retrying({timeout = 5}, function()
        for _, f in ipairs(pushers) do
            t.assert_equals(f:status(), 'dead')
        end
        for _, f in ipairs(handlers) do
            t.assert_equals(f:status(), 'dead')
        end
    end)
end

-------------------------------------------------------------------------------
-- Registry
-------------------------------------------------------------------------------

-- Everything above goes through the same registry lookup a remote conn:call()
-- would resolve on the other side, so a missing registration is an error with
-- the function name in it rather than a nil call somewhere deeper.
g.test_missing_registry_entry_is_reported = function()
    local pool = quiet_pool(1)
    _G.pregel.worker = nil
    pool.buckets[1]:put('m', {})
    t.assert_error_msg_contains('pregel.worker.deliver_batch', function()
        pool.buckets[1]:flush()
    end)
    pool:stop()
end
