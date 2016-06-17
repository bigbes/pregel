local fun    = require('fun')
local log    = require('log')
local json   = require('json')
local yaml   = require('yaml')
local clock  = require('clock')
local fiber  = require('fiber')
local digest = require('digest')
local remote = require('net.box')

local table         = require('table')
local table_new     = require('table.new')
local table_clear   = require('table.clear')

local strict        = require('pregel.utils.strict')

local xpcall_tb     = require('pregel.utils').xpcall_tb
local log_traceback = require('pregel.utils').log_traceback

local RECONNECT_AFTER = 5
local INFINITY = 9223372036854775808

local function bench_monotonic(fun, ...)
    local start_time = clock.monotonic()
    local res = {0, fun(...)}
    res[1] = clock.monotonic() - start_time
    return res
end

local crc32 = digest.crc32.new()

local function guava_name(name, server_cnt)
    if type(name) == 'table' then
        for _, el in ipairs(name) do
            crc32:update(el)
        end
        name = crc32:result()
        crc32:clear()
    elseif type(name) ~= 'number' then
        name = digest.crc32(name)
    end
    return 1 + digest.guava(name, server_cnt)
end

local function pusher_handler(id)
    local function handler_tmp(self)
        local overall = 0
        local id = id
        -- construct and set fiber name
        local fiber_name = (self.is_local == true and 'local' or 'remote')
        fiber_name = string.format('%6s_pusher_handler-%02d', fiber_name, id)
        fiber.self():name(fiber_name)

        log.info('start pusher_handler fiber')
        while true do
            if self.count > 0 then
                self:flush()
                if self.is_local == true then
                    fiber.yield()
                end
            else
                fiber.sleep(0.01)
            end
        end
    end

    return function(self)
        xpcall_tb(handler_tmp, self)
    end
end

local bucket_common_methods = {
    send = function(self, msg, args)
        return self.connection:eval(
            'return require("pregel.worker").deliver(...)',
            self.name, msg, args
        )
    end,
}

local bucket_instant_methods = {
    put = function(self, msg, args)
        while self.count == self.max_count do
            table.insert(self.waiting_list, fiber.self())
            fiber.sleep(INFINITY)
        end
        self.count = self.count + 1
        if self.msg_pool[self.count] == nil then
            self.msg_pool[self.count] = table_new(3, 0)
        end
        self.msg_pool[self.count][1] = msg
        self.msg_pool[self.count][2] = args
    end,
    flush = function(self)
        -- Construct tuple with batch (for sending to client)
        -- Dirty hack to avoid some stupid errors (regarding first
        -- yield, when waiting for right state for net.box)
        local msgs = fun.iter(self.msg_pool):take(self.count):totable()
        if self.is_local == false then
            msgs = box.tuple.new(msgs)
        end
        -- Wakeup every fiber, that waits for an empty queue
        while true do
            local f = table.remove(self.waiting_list)
            if f == nil then break end
            f:wakeup()
        end
        self.count = 0
        self.connection:eval(
            'return require("pregel.worker").deliver_batch(...)',
            self.name, msgs
        )
    end,
    start = function(self)
        self.worker = fiber.create(pusher_handler(self.id), self)
    end
}

local bucket_delayed_methods = {
    put = function(self, msg, args)
        local max = self.space.index[0]:max()
        max = max and max[3] + 1 or 1
        self.space:put{msg, args, max}
        self.count = self.count + 1
    end,
    flush = function(self)
        -- log_traceback()
        local last_key = nil
        local iterator = box.index.GT
        while true do
            local msgs = self.space:select(last_key, {
                iterator = iterator,
                limit = self.max_count
            })
            local msgs_no = #msgs
            if msgs_no == 0 then
                break
            end
            local rv = bench_monotonic(self.connection.eval,
                self.connection,
                'return require("pregel.worker").deliver_batch(...)',
                self.name, msgs
            )
            log.info("let's flush %010.6f", rv[1])
            last_key = msgs[msgs_no][3]
        end
        self.count = 0
    end,
    start = function(self)
        self.space_name = string.format('pregel_mpool_%s_%02d',
                                        self.name, self.id)
        local space = box.space[self.space_name]
        if space == nil then
            space = box.schema.create_space(self.space_name)
            space:create_index('primary', {
                type = 'TREE',
                parts = {3, 'NUM'}
            })
        end
        self.space = space
    end
}

local bucket_instant_mt = {
    __index = (function()
        local result = {}
        for k, v in pairs(bucket_common_methods)  do result[k] = v end
        for k, v in pairs(bucket_instant_methods) do result[k] = v end
        return result
    end)(),
    __new   = function(self)
        self.msg_pool  = table_new(self.max_count, 0)
        fun.range(self.max_count):each(function(id)
            self.msg_pool[id] = table_new(3, 0)
        end)
    end,
}

local bucket_delayed_mt = {
    __index = (function()
        local result = {}
        for k, v in pairs(bucket_common_methods)  do result[k] = v end
        for k, v in pairs(bucket_delayed_methods) do result[k] = v end
        return result
    end)(),
    __new   = function(self)
    end,
}

local function bucket_new(id, name, srv, options)
    local conn = remote.new(srv, {
        reconnect_after = RECONNECT_AFTER,
        wait_connected  = true
    })
    local is_local = false
    local uuid = conn:eval("return box.info.server.uuid")
    if uuid == box.info.server.uuid then
        conn:close()
        conn = remote.self
        is_local = true
    end

    options = options or {}
    local msg_count  = options.msg_count or 1000
    local is_delayed = options.is_delayed
    if is_delayed == nil then is_delayed = false end

    local self = {
        id           = id,
        uri          = srv,
        name         = name,
        uuid         = uuid,
        count        = 0,
        connection   = conn,
        waiting_list = {},
        is_local     = is_local,
        is_delayed   = is_delayed,
        max_count    = msg_count,
    }

    local mt = is_delayed and bucket_delayed_mt or bucket_instant_mt
    mt.__new(self)
    setmetatable(self, mt)

    return self
end

local function waitpool_handler(id, bucket)
    local function handler_tmp(self)
        fiber.self():name(string.format('waitpool_handler-%02d', id))
        local id     = id
        local bucket = bucket
        while true do
            local status = self.channel_in:get()
            if status == false then
                error('failed to get message from queue')
            end

            self.rval[id] = bench_monotonic(bucket.send,
                                            bucket,
                                            self.msg,
                                            self.args)
            self.channel_out:put(status)
        end
    end

    return function(self)
        xpcall_tb(handler_tmp, self)
    end
end

local waitpool_mt = {
    __call = function(self, msg, args)
        self.rval = {}
        self.msg  = msg
        self.args = args
        table_clear(self.errors)
        for i = 1, self.fpool_cnt do
            local rv = self.channel_in:put(i, 0)
            if rv == false then
                error('failing to put message, error in worker')
            end
        end
        for i = 1, self.fpool_cnt do
            local rv = self.channel_out:get()
            if rv == false then
                error('error, while sending message')
            end
        end
        return self.rval
    end,
}

local function waitpool_new(pool)
    local self = {
        fpool_cnt   = pool.bucket_cnt,
        channel_in  = fiber.channel(0),
        channel_out = fiber.channel(pool.bucket_cnt),
        errors      = {},
        msg         = nil,
        args        = nil
    }
    self.fpool = fun.iter(pool.buckets):enumerate():map(function(id, bucket)
        local rv = fiber.create(waitpool_handler(id, bucket), self)
        return rv
    end):totable()
    return setmetatable(self, waitpool_mt)
end

local mpool_mt = {
    __index = {
        id = function(self, name)
            return guava_name(name, self.bucket_cnt)
        end,
        by_id = function(self, name)
            return self.buckets[guava_name(name, self.bucket_cnt)]
        end,
        next_id = function(self, id)
            local bucket_id = guava_name(id, self.bucket_cnt)
            while true do
                id = id + 1
                if guava_name(id, self.bucket_cnt) == bucket_id then
                    log.info('next_id is %d', id)
                    return id
                end
            end
        end,
        flush = function(self)
            for _, bucket in ipairs(self.buckets) do
                if bucket.count > 0 then
                    bucket:flush()
                end
            end
        end,
        send_wait = function(self, message, args)
            log.info('mpool:send_wait - instance "%s", message "%s", args <%s>',
                     self.name, message, json.encode(args))
            return self.waitpool(message, args)
        end,
        send = function(self, message, args)
            for _, bucket in ipairs(self.buckets) do
                bucket:put(message, args)
            end
        end,
    }
}

local function mpool_new(name, servers, options)
    options = options or {}
    local msg_count = options.msg_count or 1000

    local is_delayed = options.is_delayed
    if is_delayed == nil then is_delayed = false end

    local self = setmetatable({
        name       = name,
        buckets    = {},
        bucket_cnt = 0,
        is_delayed = is_delayed,
        space      = space,
        self_idx   = 0,
    }, mpool_mt)

    for k, server in ipairs(servers) do
        table.insert(self.buckets, bucket_new(k, name, server, {
            is_delayed = is_delayed,
            msg_count = msg_count
        }))
        self.bucket_cnt = self.bucket_cnt + 1
    end

    -- normalize position of servers and their ID, then finish execution
    table.sort(self.buckets, function(b1, b2)
        return b1.uuid < b2.uuid
    end)
    for idx, bucket in ipairs(self.buckets) do
        if bucket.connection == remote.self then
            self.self_idx = idx
        end
        -- assign new id based on sorting
        bucket.id = idx
        -- start everythin using new id
        bucket:start()
    end

    self.waitpool = waitpool_new(self)

    return self
end

return strict.strictify({
    new = mpool_new,
})
