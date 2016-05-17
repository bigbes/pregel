box.cfg{
    wal_mode = 'none',
    logger_nonblock = false
}

local fun = require('fun')
local tap = require('tap')
local json = require('json')
local yaml = require('yaml')
local queue = require("pregel.queue")

local xpcall_tb = require('pregel.utils').xpcall_tb

local test = tap.test("queue")

local function oneshdl()
    return 1
end

local function iter_count(...)
    local cnt = 0
    for k, v in ... do
        cnt = cnt + 1
    end
    return cnt
    -- return fun.wrap(...):map(oneshdl):reduce(fun.operator.add, 0)
end

-------------------------------------------------------------------------------
-- Prepare input data
-------------------------------------------------------------------------------
local cnt = 0
local function tablelize(id, k, v)
    if k < 10 or k > 20 or v < 100 or v > 500 then
        cnt = cnt + 1
        return nil
    end
    return {id - cnt, k, v}
end

local input = fun.zip(
    fun.range(100),
    fun.rands(10, 20),
    fun.rands(100, 500)
):map(tablelize):totable()

-------------------------------------------------------------------------------
-- Define tests
-------------------------------------------------------------------------------

local function test_all()
    test:plan(8)

    local tqueue = nil

    test:test("create/drop queue", function(test)
        test:plan(5)
        local q1 = queue.new('queue_1')
        test:isnt(q1, nil, 'object is created')
        test:is(q1, queue.list['queue_1'])
        local q2 = queue.new('queue_1')
        test:is(q2, q1, 'no dups')
        q1:drop()
        test:is(rawget(queue.list, 'queue_1'), nil, 'object is destroyed')
        tqueue = queue.new('queue_1')
        test:isnt(tqueue, nil, 'not nil')
    end)

    test:test('put into queue', function(test)
        test:plan(#input + 2)

        fun.iter(input):each(function(tuple) test:is_deeply(
            {tqueue:put(tuple[2], tuple[3]):unpack()},
            tuple,
            'check what is inserted'
        ) end)

        test:is(
            tqueue:len(),
            #input,
            'check len of space'
        )

        test:is(
            fun.wrap(pairs(tqueue.stats)):reduce(function(acc, v)
                return acc + v
            end, 0),
            #input,
            'check that all is in stats'
        )
    end)

    test:test('check len and stat', function(test)
        local cnt = iter_count(tqueue:receiver_closure())
        cnt = cnt + iter_count(pairs(tqueue.stats))
        test:plan(cnt)
        for receiver in tqueue:receiver_closure() do
            test:is(tqueue:len(receiver), tqueue.stats[receiver], 'len->stats')
        end
        for receiver, count in pairs(tqueue.stats) do
            test:is(tqueue:len(receiver), count, 'stats->len')
        end
    end)

    test:test('delete something', function(test)
        test:plan(2)
        local k, v = next(tqueue.stats)
        tqueue:delete(k)
        test:is(tqueue.stats[k], 0, 'element is deleted')
        test:is(tqueue:len(k), 0, 'element is deleted')
    end)

    test:test('delete everything', function(test)
        test:plan(2)
        tqueue:delete()
        test:is(
            fun.wrap(pairs(tqueue.stats)):reduce(function(acc, v)
                return acc + v
            end, 0),
            0,
            'check that nothing is in stats'
        )
        test:is(tqueue:len(), 0, 'check that nothing is in space')
        tqueue:drop()
    end)

    test:test('aggregator max', function(test)
        local uniq = {}
        for k, v in pairs(input) do
            local val = uniq[v[2]]
            if val == nil then
                uniq[v[2]] = v[3]
            else
                uniq[v[2]] = math.max(v[3], val)
            end
        end
        local len = iter_count(pairs(uniq))
        test:plan(4 * len + 2)
        local q1 = queue.new('queue_1', {aggregator = math.max})
        fun.iter(input):each(function(tuple)
            q1:put(tuple[2], tuple[3])
        end)
        test:is(q1:len(), len, 'space len is right')
        test:is(iter_count(pairs(q1.stats)), len, 'stats len is right')
        for k, v in pairs(q1.stats) do
            test:isnt(uniq[k], nil, 'key is in uniq')
            test:is(v, 1, 'count must be one')
            test:is(q1:len(k), 1, 'only one tuple in space')
            test:is(q1:take(k), uniq[k], 'value is OK')
        end
        q1:drop()
    end)

    test:test('aggregator sum', function(test)
        local uniq = {}
        for k, v in pairs(input) do
            local val = uniq[v[2]]
            if val == nil then
                uniq[v[2]] = v[3]
            else
                uniq[v[2]] = fun.op.add(v[3], val)
            end
        end
        local len = iter_count(pairs(uniq))
        test:plan(4 * len + 2)
        local q1 = queue.new('queue_1', {aggregator = fun.op.add})
        fun.iter(input):each(function(tuple)
            q1:put(tuple[2], tuple[3])
        end)
        test:is(q1:len(), len, 'space len is right')
        test:is(iter_count(pairs(q1.stats)), len, 'stats len is right')
        for k, v in pairs(q1.stats) do
            test:isnt(uniq[k], nil, 'key is in uniq')
            test:is(v, 1, 'count must be one')
            test:is(q1:len(k), 1, 'only one tuple in space')
            test:is(q1:take(k), uniq[k], 'value is OK')
        end
        q1:drop()
    end)

    test:test('aggregator sum + squash', function(test)
        local uniq = {}
        for k, v in pairs(input) do
            local val = uniq[v[2]]
            if val == nil then
                uniq[v[2]] = v[3]
            else
                uniq[v[2]] = fun.op.add(v[3], val)
            end
        end
        local len = iter_count(pairs(uniq))
        test:plan(4 * len + 4 + #input)
        local q1 = queue.new('queue_1', {
            aggregator = fun.op.add,
            squash_only = true
        })
        fun.iter(input):each(function(tuple)
            local rv = q1:put(tuple[2], tuple[3])
            test:is_deeply({rv:unpack()}, tuple, 'check what is inserted')
        end)

        test:is(q1:len(), #input, 'check len of space')

        test:is(
            fun.wrap(pairs(q1.stats)):reduce(function(acc, v)
                return acc + v
            end, 0),
            #input,
            'check that all is in stats'
        )

        q1:squash()

        test:is(q1:len(), len, 'space len is right')
        test:is(iter_count(pairs(q1.stats)), len, 'stats len is right')
        for k, v in pairs(q1.stats) do
            test:isnt(uniq[k],    nil,     'key is in uniq')
            test:is  (v,          1,       'count must be one')
            test:is  (q1:len(k),  1,       'only one tuple in space')
            test:is  (q1:take(k), uniq[k], 'value is OK')
        end
        q1:drop()
    end)

    os.exit(test:check() == true and 0 or 1)
end

xpcall_tb(test_all)

os.exit(-1)
