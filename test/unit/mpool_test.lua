local t = require('luatest')
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

    bucket:flush()
    t.helpers.retrying({timeout = 5}, function()
        t.assert_equals(done, true)
    end)
    t.assert_equals(bucket.count, 1)
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
    local ok, err = pcall(function() return pool:send_wait('boom', {}) end)
    t.assert_equals(ok, false)
    err = tostring(err)
    t.assert_str_contains(err, 'boom')
    t.assert_str_contains(err, 'deliver refused')

    -- And the pool still works afterwards.
    local rv = pool:send_wait('count', {})
    t.assert_equals(#rv, 2)
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
