local t = require('luatest')

local box_helper = require('test.helpers.box')
local queue = require('pregel.queue')

-- Both engines implement the same interface, so every behavioural test runs
-- against both. The group name carries the engine so a failure names it.
local groups = {}
for _, engine in ipairs({'space', 'table'}) do
    groups[engine] = t.group('queue.' .. engine, {{engine = engine}})
end
local g_space = groups.space
local g_table = groups.table

local seq = 0
local function fresh_name()
    seq = seq + 1
    return string.format('unit_%03d', seq)
end

for _, g in pairs(groups) do
    g.before_all(function()
        box_helper.cfg()
    end)
end

--- Build a queue for this test and make sure it goes away afterwards.
local function make(cg, options)
    options = options or {}
    options.engine = cg.params.engine
    local q = queue.new(fresh_name(), options)
    t.assert_equals(q.engine, cg.params.engine)
    return q
end

local function drop(q)
    if q.name ~= nil then
        q:drop()
    end
end

local function collect(q, receiver)
    local rv = {}
    for _, message in q:pairs(receiver) do
        table.insert(rv, message)
    end
    return rv
end

local function receivers_of(q)
    local rv = {}
    for receiver in q:receiver_closure() do
        table.insert(rv, receiver)
    end
    table.sort(rv)
    return rv
end

local function stats_total(q)
    local total = 0
    for _, count in pairs(q.stats) do
        total = total + count
    end
    return total
end

local function stats_keys(q)
    local rv = {}
    for receiver in pairs(q.stats) do
        table.insert(rv, receiver)
    end
    table.sort(rv)
    return rv
end

-------------------------------------------------------------------------------
-- Lifecycle
-------------------------------------------------------------------------------

local function test_create_is_cached(cg)
    local name = fresh_name()
    local q1 = queue.new(name, {engine = cg.params.engine})
    local q2 = queue.new(name, {engine = cg.params.engine})
    t.assert_is(q2, q1)
    t.assert_is(queue.list[name], q1)
    q1:drop()
    t.assert_equals(rawget(queue.list, name), nil)
    -- A fresh queue under the same name is a different object.
    local q3 = queue.new(name, {engine = cg.params.engine})
    t.assert_is_not(q3, q1)
    drop(q3)
end
g_space.test_create_is_cached = test_create_is_cached
g_table.test_create_is_cached = test_create_is_cached

-- Defect: the cache answered every second call, whatever it was asked for, so
-- a worker restarted in place with a different combiner went on computing with
-- the old one -- and asking for the 'table' engine after a 'space' run kept the
-- space. Identical options are still a cache hit; different ones are a mistake
-- and now say so.
local function test_reopen_with_other_options_is_refused(cg)
    local name = fresh_name()
    local engine = cg.params.engine
    local other_engine = engine == 'space' and 'table' or 'space'
    local combiner = function(a, b) return a + b end

    local q = queue.new(name, {engine = engine, combiner = combiner})
    -- The same options are the cache doing its job.
    t.assert_is(queue.new(name, {engine = engine, combiner = combiner}), q)

    t.assert_error_msg_contains('combiner', function()
        queue.new(name, {engine = engine,
                         combiner = function(a, b) return a - b end})
    end)
    t.assert_error_msg_contains('squash_only', function()
        queue.new(name, {engine = engine, combiner = combiner,
                         squash_only = true})
    end)
    t.assert_error_msg_contains('engine', function()
        queue.new(name, {engine = other_engine, combiner = combiner})
    end)
    -- The queue that was already there is untouched by any of that.
    t.assert_is(queue.list[name], q)
    t.assert_is(q.combiner, combiner)
    drop(q)
end
g_space.test_reopen_with_other_options_is_refused =
    test_reopen_with_other_options_is_refused
g_table.test_reopen_with_other_options_is_refused =
    test_reopen_with_other_options_is_refused

local function test_rejects_bad_options(cg)
    t.assert_error_msg_contains('options.engine', function()
        queue.new(fresh_name(), {engine = 'mmap'})
    end)
    t.assert_error_msg_contains('options.combiner', function()
        queue.new(fresh_name(), {engine = cg.params.engine, combiner = 42})
    end)
    t.assert_error_msg_contains('options.squash_only', function()
        queue.new(fresh_name(), {engine = cg.params.engine, squash_only = 'yes'})
    end)
end
g_space.test_rejects_bad_options = test_rejects_bad_options
g_table.test_rejects_bad_options = test_rejects_bad_options

-------------------------------------------------------------------------------
-- put / pairs / len
-------------------------------------------------------------------------------

local function test_put_and_read_back(cg)
    local q = make(cg)
    q:put('alice', 1)
    q:put('alice', 2)
    q:put('bob', 3)

    t.assert_equals(collect(q, 'alice'), {1, 2})
    t.assert_equals(collect(q, 'bob'), {3})
    t.assert_equals(q:len(), 3)
    t.assert_equals(q:len('alice'), 2)
    t.assert_equals(q:len('bob'), 1)
    t.assert_equals(q:len('nobody'), 0)
    drop(q)
end
g_space.test_put_and_read_back = test_put_and_read_back
g_table.test_put_and_read_back = test_put_and_read_back

-- Messages are arbitrary values, not just scalars.
local function test_put_structured_message(cg)
    local q = make(cg)
    q:put('alice', {value = 7, tags = {'a', 'b'}})
    local got = collect(q, 'alice')
    t.assert_equals(#got, 1)
    t.assert_equals(got[1].value, 7)
    t.assert_equals(got[1].tags, {'a', 'b'})
    drop(q)
end
g_space.test_put_structured_message = test_put_structured_message
g_table.test_put_structured_message = test_put_structured_message

-- Reading a receiver that has no messages must not turn it into one: the
-- worker polls len()/pairs() for every vertex it walks.
local function test_reading_absent_receiver_creates_nothing(cg)
    local q = make(cg)
    q:put('alice', 1)
    t.assert_equals(collect(q, 'ghost'), {})
    t.assert_equals(q:len('ghost'), 0)
    t.assert_equals(receivers_of(q), {'alice'})
    t.assert_equals(stats_keys(q), {'alice'})
    drop(q)
end
g_space.test_reading_absent_receiver_creates_nothing =
    test_reading_absent_receiver_creates_nothing
g_table.test_reading_absent_receiver_creates_nothing =
    test_reading_absent_receiver_creates_nothing

local function test_put_rejects_nil(cg)
    local q = make(cg)
    t.assert_error(function() q:put(nil, 1) end)
    t.assert_error(function() q:put('alice', nil) end)
    drop(q)
end
g_space.test_put_rejects_nil = test_put_rejects_nil
g_table.test_put_rejects_nil = test_put_rejects_nil

-------------------------------------------------------------------------------
-- the sender
-------------------------------------------------------------------------------

-- Defect: vertex:send_message put {receiver, message, sender} on the wire and
-- the worker's message.deliver handler dropped the third element, because
-- queue:put(receiver, message) had nowhere to keep it. So a compute function
-- could not answer whoever asked, and every request/response protocol had to
-- carry the sender inside the payload by hand. See docs/api-design.md 3.18.

--- Collect (sender, message) pairs in the order the queue yields them.
local function collect_pairs(q, receiver)
    local rv = {}
    for sender, message in q:pairs(receiver) do
        table.insert(rv, {sender = sender, message = message})
    end
    return rv
end

local function test_put_keeps_the_sender(cg)
    local q = make(cg)
    q:put('bob', 'hello', 'alice')

    local got = collect_pairs(q, 'bob')
    t.assert_equals(#got, 1)
    t.assert_equals(got[1].message, 'hello')
    t.assert_equals(got[1].sender, 'alice')
    drop(q)
end
g_space.test_put_keeps_the_sender = test_put_keeps_the_sender
g_table.test_put_keeps_the_sender = test_put_keeps_the_sender

-- Each message keeps its own sender, and the pairing survives several of them.
local function test_each_message_keeps_its_own_sender(cg)
    local q = make(cg)
    q:put('bob', 'from-alice', 'alice')
    q:put('bob', 'from-carol', 'carol')
    q:put('bob', 'from-alice-again', 'alice')

    local by_message = {}
    for _, entry in ipairs(collect_pairs(q, 'bob')) do
        by_message[entry.message] = entry.sender
    end
    t.assert_equals(by_message, {
        ['from-alice']       = 'alice',
        ['from-carol']       = 'carol',
        ['from-alice-again'] = 'alice',
    })
    drop(q)
end
g_space.test_each_message_keeps_its_own_sender =
    test_each_message_keeps_its_own_sender
g_table.test_each_message_keeps_its_own_sender =
    test_each_message_keeps_its_own_sender

-- The sender is optional: nothing in the library requires one, and a message
-- put without one reads back as box.NULL rather than as a missing field.
local function test_put_without_a_sender(cg)
    local q = make(cg)
    q:put('bob', 'anonymous')

    local got = collect_pairs(q, 'bob')
    t.assert_equals(#got, 1)
    t.assert_equals(got[1].message, 'anonymous')
    t.assert_equals(got[1].sender, box.NULL)
    -- The loop shape every example uses keeps working unchanged.
    t.assert_equals(collect(q, 'bob'), {'anonymous'})
    drop(q)
end
g_space.test_put_without_a_sender = test_put_without_a_sender
g_table.test_put_without_a_sender = test_put_without_a_sender

-- A combined message has no one sender, so it says so rather than claiming
-- whichever of its parts happened to be folded in last.
local function test_a_combined_message_has_no_sender(cg)
    local q = make(cg, {combiner = function(a, b) return a + b end})
    q:put('bob', 1, 'alice')
    q:put('bob', 2, 'carol')

    local got = collect_pairs(q, 'bob')
    t.assert_equals(#got, 1)
    t.assert_equals(got[1].message, 3)
    t.assert_equals(got[1].sender, box.NULL)
    drop(q)
end
g_space.test_a_combined_message_has_no_sender =
    test_a_combined_message_has_no_sender
g_table.test_a_combined_message_has_no_sender =
    test_a_combined_message_has_no_sender

local function test_squash_drops_the_sender_too(cg)
    local q = make(cg, {
        combiner = function(a, b) return a + b end,
        squash_only = true,
    })
    q:put('bob', 1, 'alice')
    q:put('bob', 2, 'carol')
    -- Not combined yet, and each message still knows who sent it.
    t.assert_equals(#collect_pairs(q, 'bob'), 2)

    q:squash()

    local got = collect_pairs(q, 'bob')
    t.assert_equals(#got, 1)
    t.assert_equals(got[1].message, 3)
    t.assert_equals(got[1].sender, box.NULL)
    drop(q)
end
g_space.test_squash_drops_the_sender_too = test_squash_drops_the_sender_too
g_table.test_squash_drops_the_sender_too = test_squash_drops_the_sender_too

-- The counters and the delete path do not care about the sender, but they run
-- over the same tuples, so verify() has to still agree after a sender is
-- stored.
local function test_stats_agree_with_senders_stored(cg)
    local q = make(cg)
    q:put('bob', 1, 'alice')
    q:put('bob', 2, 'carol')
    q:put('dave', 3, 'alice')

    t.assert_equals({queue.verify(q)}, {true, {}})
    q:delete('bob')
    t.assert_equals({queue.verify(q)}, {true, {}})
    t.assert_equals(collect_pairs(q, 'dave')[1].sender, 'alice')
    drop(q)
end
g_space.test_stats_agree_with_senders_stored =
    test_stats_agree_with_senders_stored
g_table.test_stats_agree_with_senders_stored =
    test_stats_agree_with_senders_stored

-- A queue whose space survived a restart predates the sender field. Adopting
-- it must widen the format, and the messages already in it read back with no
-- sender.
--
-- The widening is not what makes the next put work: Tarantool accepts a tuple
-- with more fields than the format declares, so the four-field insert lands
-- either way (measured -- with the widening removed every other test here
-- stays green). What it buys is that field 4 has a name, so `tuple.sender`
-- and anything reading the space's format see the same queue the code does.
g_space.test_an_older_space_gains_the_sender_field = function()
    local name = fresh_name()
    local space_name = 'pregel_tube_' .. name
    -- The three-field space an older version of this module created.
    local space = box.schema.space.create(space_name, {
        format = {
            {name = 'id',       type = 'unsigned'},
            {name = 'receiver', type = 'string'  },
            {name = 'message',  type = 'any'     },
        }
    })
    space:create_index('primary', {
        type = 'TREE', parts = {{field = 1, type = 'unsigned'}},
        sequence = true,
    })
    space:create_index('receiver', {
        type = 'TREE', parts = {{field = 2, type = 'string'}}, unique = false,
    })
    space:insert{box.NULL, 'bob', 'from-before'}

    local q = queue.new(name, {engine = 'space'})
    t.assert_equals(q:len('bob'), 1)
    local old = collect_pairs(q, 'bob')
    t.assert_equals(old[1].message, 'from-before')
    t.assert_equals(old[1].sender, box.NULL)

    q:put('bob', 'from-now', 'alice')
    local by_message = {}
    for _, entry in ipairs(collect_pairs(q, 'bob')) do
        by_message[entry.message] = entry.sender
    end
    t.assert_equals(by_message['from-now'], 'alice')

    -- The adopted space now declares the field, so it is reachable by name.
    local format = box.space[space_name]:format()
    t.assert_equals(#format, 4)
    t.assert_equals(format[4].name, 'sender')
    t.assert_equals(format[4].is_nullable, true)
    for _, tuple in box.space[space_name]:pairs() do
        if tuple[3] == 'from-now' then
            t.assert_equals(tuple.sender, 'alice')
        end
    end
    drop(q)
end

-------------------------------------------------------------------------------
-- receiver_closure
-------------------------------------------------------------------------------

-- Defect: the space engine seeded its cursor with the number 0 against a
-- string index and wrapped the step in a `while true ... return`, so it
-- yielded the first receiver forever instead of walking distinct receivers.
local function test_receiver_closure_walks_distinct_receivers(cg)
    local q = make(cg)
    for _, receiver in ipairs({'carol', 'alice', 'bob'}) do
        q:put(receiver, 1)
        q:put(receiver, 2)
        q:put(receiver, 3)
    end
    t.assert_equals(receivers_of(q), {'alice', 'bob', 'carol'})
    drop(q)
end
g_space.test_receiver_closure_walks_distinct_receivers =
    test_receiver_closure_walks_distinct_receivers
g_table.test_receiver_closure_walks_distinct_receivers =
    test_receiver_closure_walks_distinct_receivers

local function test_receiver_closure_on_empty_queue(cg)
    local q = make(cg)
    t.assert_equals(receivers_of(q), {})
    drop(q)
end
g_space.test_receiver_closure_on_empty_queue = test_receiver_closure_on_empty_queue
g_table.test_receiver_closure_on_empty_queue = test_receiver_closure_on_empty_queue

-- Defect: the space engine's pairs() called the iterator without threading its
-- state, so a receiver holding several messages could repeat the first one.
local function test_pairs_returns_every_message_once(cg)
    local q = make(cg)
    for i = 1, 25 do
        q:put('alice', i)
    end
    local got = collect(q, 'alice')
    t.assert_equals(#got, 25)
    table.sort(got)
    for i = 1, 25 do
        t.assert_equals(got[i], i)
    end
    drop(q)
end
g_space.test_pairs_returns_every_message_once = test_pairs_returns_every_message_once
g_table.test_pairs_returns_every_message_once = test_pairs_returns_every_message_once

-------------------------------------------------------------------------------
-- delete / truncate
-------------------------------------------------------------------------------

local function test_delete_one_receiver(cg)
    local q = make(cg)
    q:put('alice', 1)
    q:put('alice', 2)
    q:put('bob', 3)

    q:delete('alice')
    t.assert_equals(q:len('alice'), 0)
    t.assert_equals(q:len('bob'), 1)
    t.assert_equals(q:len(), 1)
    t.assert_equals(stats_keys(q), {'bob'})
    t.assert_equals(receivers_of(q), {'bob'})
    drop(q)
end
g_space.test_delete_one_receiver = test_delete_one_receiver
g_table.test_delete_one_receiver = test_delete_one_receiver

local function test_truncate_clears_everything(cg)
    local q = make(cg)
    q:put('alice', 1)
    q:put('bob', 2)

    q:truncate()
    t.assert_equals(q:len(), 0)
    t.assert_equals(stats_total(q), 0)
    t.assert_equals(stats_keys(q), {})
    t.assert_equals(receivers_of(q), {})

    -- Still usable afterwards.
    q:put('alice', 3)
    t.assert_equals(collect(q, 'alice'), {3})
    drop(q)
end
g_space.test_truncate_clears_everything = test_truncate_clears_everything
g_table.test_truncate_clears_everything = test_truncate_clears_everything

-------------------------------------------------------------------------------
-- stats consistency
-------------------------------------------------------------------------------

local function test_stats_track_the_queue(cg)
    local q = make(cg)
    local expected = {}
    for i = 1, 60 do
        local receiver = 'v' .. tostring(i % 7)
        q:put(receiver, i)
        expected[receiver] = (expected[receiver] or 0) + 1
    end
    t.assert_equals(stats_total(q), 60)
    t.assert_equals(q:len(), 60)
    for receiver, count in pairs(expected) do
        t.assert_equals(q:len(receiver), count)
        t.assert_equals(q.stats[receiver], count)
    end
    t.assert_equals({queue.verify(q)}, {true, {}})
    drop(q)
end
g_space.test_stats_track_the_queue = test_stats_track_the_queue
g_table.test_stats_track_the_queue = test_stats_track_the_queue

-- Defect: verify() compared the stats value against the receiver name
-- (`v ~= k`) and called index:len{k}, which takes no argument -- it could not
-- report a real divergence. Corrupt stats behind the queue's back and check
-- that verify() now says so.
local function test_verify_reports_divergence(cg)
    local q = make(cg)
    q:put('alice', 1)
    q:put('bob', 2)
    t.assert_equals(queue.verify(q), true)

    q.stats['alice'] = 5
    local ok, problems = queue.verify(q)
    t.assert_equals(ok, false)
    t.assert_equals(#problems, 1)
    t.assert_str_contains(problems[1], 'alice')
    t.assert_str_contains(problems[1], '1 message(s)')
    t.assert_str_contains(problems[1], 'stats says 5')

    -- A receiver stats knows about and the queue does not is a divergence too,
    -- and it must be reported once, not once per direction.
    q.stats['alice'] = 1
    q.stats['ghost'] = 2
    ok, problems = queue.verify(q)
    t.assert_equals(ok, false)
    t.assert_equals(#problems, 1)
    t.assert_str_contains(problems[1], 'ghost')
    drop(q)
end
g_space.test_verify_reports_divergence = test_verify_reports_divergence
g_table.test_verify_reports_divergence = test_verify_reports_divergence

-------------------------------------------------------------------------------
-- combiner on put
-------------------------------------------------------------------------------

local function test_combiner_on_put_keeps_one_message(cg)
    local q = make(cg, {combiner = math.max})
    local expected = {}
    for i = 1, 40 do
        local receiver = 'v' .. tostring(i % 5)
        local value = (i * 37) % 101
        q:put(receiver, value)
        expected[receiver] = math.max(expected[receiver] or value, value)
    end
    t.assert_equals(q:len(), 5)
    for receiver, value in pairs(expected) do
        t.assert_equals(q:len(receiver), 1)
        t.assert_equals(q.stats[receiver], 1)
        t.assert_equals(collect(q, receiver), {value})
    end
    t.assert_equals(queue.verify(q), true)
    drop(q)
end
g_space.test_combiner_on_put_keeps_one_message = test_combiner_on_put_keeps_one_message
g_table.test_combiner_on_put_keeps_one_message = test_combiner_on_put_keeps_one_message

local function test_combiner_sum_on_put(cg)
    local q = make(cg, {combiner = function(a, b) return a + b end})
    for i = 1, 10 do
        q:put('alice', i)
    end
    t.assert_equals(collect(q, 'alice'), {55})
    t.assert_equals(q:len('alice'), 1)
    drop(q)
end
g_space.test_combiner_sum_on_put = test_combiner_sum_on_put
g_table.test_combiner_sum_on_put = test_combiner_sum_on_put

-- squash() is a no-op unless squash_only is set: put() has already combined.
local function test_squash_is_noop_without_squash_only(cg)
    local q = make(cg, {combiner = math.max})
    q:put('alice', 1)
    q:put('alice', 9)
    q:squash()
    t.assert_equals(collect(q, 'alice'), {9})
    t.assert_equals(q:len(), 1)
    drop(q)
end
g_space.test_squash_is_noop_without_squash_only = test_squash_is_noop_without_squash_only
g_table.test_squash_is_noop_without_squash_only = test_squash_is_noop_without_squash_only

local function test_squash_without_combiner_is_noop(cg)
    local q = make(cg, {squash_only = true})
    q:put('alice', 1)
    q:put('alice', 2)
    q:squash()
    t.assert_equals(q:len('alice'), 2)
    drop(q)
end
g_space.test_squash_without_combiner_is_noop = test_squash_without_combiner_is_noop
g_table.test_squash_without_combiner_is_noop = test_squash_without_combiner_is_noop

-------------------------------------------------------------------------------
-- squash_only
-------------------------------------------------------------------------------

local function test_squash_only_defers_the_combiner(cg)
    local q = make(cg, {combiner = function(a, b) return a + b end,
                        squash_only = true})
    local expected = {}
    for i = 1, 40 do
        local receiver = 'v' .. tostring(i % 5)
        q:put(receiver, i)
        expected[receiver] = (expected[receiver] or 0) + i
    end
    -- Nothing is combined before squash().
    t.assert_equals(q:len(), 40)
    t.assert_equals(stats_total(q), 40)

    q:squash()

    t.assert_equals(q:len(), 5)
    t.assert_equals(stats_total(q), 5)
    for receiver, value in pairs(expected) do
        t.assert_equals(q:len(receiver), 1)
        t.assert_equals(q.stats[receiver], 1)
        t.assert_equals(collect(q, receiver), {value})
    end
    t.assert_equals(queue.verify(q), true)
    drop(q)
end
g_space.test_squash_only_defers_the_combiner = test_squash_only_defers_the_combiner
g_table.test_squash_only_defers_the_combiner = test_squash_only_defers_the_combiner

-- Defect: squash() deleted and re-put each receiver while iterating the
-- container's own pairs(), which is undefined for the table engine. With many
-- receivers it either skipped some or blew up.
local function test_squash_handles_many_receivers(cg)
    local q = make(cg, {combiner = math.max, squash_only = true})
    local expected = {}
    for i = 1, 200 do
        local receiver = string.format('v%03d', i % 40)
        local value = (i * 17) % 251
        q:put(receiver, value)
        expected[receiver] = math.max(expected[receiver] or value, value)
    end
    q:squash()

    t.assert_equals(q:len(), 40)
    local seen = 0
    for receiver in q:receiver_closure() do
        seen = seen + 1
        t.assert_equals(collect(q, receiver), {expected[receiver]},
                        'receiver ' .. receiver)
    end
    t.assert_equals(seen, 40)
    t.assert_equals(queue.verify(q), true)
    drop(q)
end
g_space.test_squash_handles_many_receivers = test_squash_handles_many_receivers
g_table.test_squash_handles_many_receivers = test_squash_handles_many_receivers

local function test_squash_of_empty_queue(cg)
    local q = make(cg, {combiner = math.max, squash_only = true})
    q:squash()
    t.assert_equals(q:len(), 0)
    drop(q)
end
g_space.test_squash_of_empty_queue = test_squash_of_empty_queue
g_table.test_squash_of_empty_queue = test_squash_of_empty_queue

-------------------------------------------------------------------------------
-- 'space' engine specifics
-------------------------------------------------------------------------------

g_space.test_space_schema = function()
    local name = fresh_name()
    local q = queue.new(name, {engine = 'space'})
    local space = box.space['pregel_tube_' .. name]
    t.assert_not_equals(space, nil)

    local format = space:format()
    t.assert_equals(format[1], {name = 'id', type = 'unsigned'})
    t.assert_equals(format[2], {name = 'receiver', type = 'string'})
    t.assert_equals(format[3], {name = 'message', type = 'any'})
    t.assert_equals(space.index.receiver.unique, false)

    -- The primary key comes from a sequence, not space:auto_increment().
    t.assert_not_equals(space.index.primary.sequence_id, nil)

    q:put('alice', 'x')
    q:put('bob', 'y')
    local ids = {}
    for _, tuple in space:pairs() do
        table.insert(ids, tuple[1])
    end
    table.sort(ids)
    t.assert_equals(#ids, 2)
    t.assert_not_equals(ids[1], ids[2])
    q:drop()
end

g_space.test_drop_removes_space_and_sequence = function()
    local name = fresh_name()
    local q = queue.new(name, {engine = 'space'})
    q:put('alice', 1)
    q:drop()
    t.assert_equals(box.space['pregel_tube_' .. name], nil)
    t.assert_equals(box.sequence['pregel_tube_' .. name .. '_seq'], nil)
end

-- Reopening a name whose space survived must rebuild the counters from it,
-- otherwise stats and the queue disagree from the first put.
g_space.test_reopen_rebuilds_stats = function()
    local name = fresh_name()
    local q = queue.new(name, {engine = 'space'})
    q:put('alice', 1)
    q:put('alice', 2)
    q:put('bob', 3)

    -- Forget the object without touching the space, the way a restart would.
    rawset(queue.list, name, nil)

    local reopened = queue.new(name, {engine = 'space'})
    t.assert_is_not(reopened, q)
    -- Without the rebuild these are 0 and the queue disagrees with its own
    -- counters from the first put.
    t.assert_equals(reopened.stats['alice'], 2)
    t.assert_equals(reopened.stats['bob'], 1)
    t.assert_equals(queue.verify(reopened), true)
    reopened:drop()
end

-- queue.list adopts an existing space by name.
g_space.test_list_adopts_existing_space = function()
    local name = fresh_name()
    local q = queue.new(name, {engine = 'space'})
    q:put('alice', 1)
    rawset(queue.list, name, nil)

    local adopted = queue.list[name]
    t.assert_not_equals(adopted, nil)
    t.assert_equals(adopted:len('alice'), 1)
    adopted:drop()

    t.assert_equals(queue.list['no_such_queue_at_all'], nil)
end

-------------------------------------------------------------------------------
-- 'table' engine specifics
-------------------------------------------------------------------------------

g_table.test_table_engine_creates_no_space = function()
    local name = fresh_name()
    local q = queue.new(name, {engine = 'table'})
    q:put('alice', 1)
    t.assert_equals(box.space['pregel_tube_' .. name], nil)
    q:drop()
end
