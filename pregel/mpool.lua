local fun    = require('fun')
local log    = require('log')
local json   = require('json')
local yaml   = require('yaml')
local fiber  = require('fiber')
local digest = require('digest')
local remote = require('net.box')

local table       = require('table')
local table_new   = require('table.new')
local table_clear = require('table.clear')

local strict      = require('pregel.utils.strict')

local xpcall_tb = require('pregel.utils').xpcall_tb

local RECONNECT_AFTER = 5
local INFINITY = 9223372036854775808

local function pusher_handler(id)
    local function handler_tmp(self)
        fiber.self():name(string.format('pusher_handler-%02d', id))
        local id = id
        while true do
            fiber.sleep(INFINITY)
            if self.count > 0 then
                -- Constructs batch from messages
                local messages
                if self.count < self.max_count then
                    messages = table_new(self.count, 0)
                    for i = 1, self.count do
                        messages[i] = self.msg_pool[i]
                    end
                else
                    messages = self.msg_pool
                end
                self.count = 0
                -- Send batch to client
                self.connection:eval(
                    'return require("pregel.worker").deliver_batch(...)',
                    self.name, messages
                )
                -- Wakeup every fiber, that waits for an answer
                while true do
                    local f = table.remove(self.waiting_list)
                    if f == nil then break end
                    f:wakeup()
                end
            end
        end
    end

    return function(self)
        xpcall_tb(handler_tmp, self)
    end
end

local bucket_mt = {
    __index = {
        put = function(self, msg, args)
            while self.count == self.max_count do
                table.insert(self.waiting_list, fiber.self())
                self.worker:wakeup()
                fiber.sleep(INFINITY)
            end
            self.count = self.count + 1
            self.msg_pool[self.count][1] = msg
            self.msg_pool[self.count][2] = args
        end,
        send = function(self, msg, args)
            log.info('bucket:send - instance "%s", message "%s", args <%s>',
                     self.name, msg, json.encode(args))
            self.connection:eval(
                'return require("pregel.worker").deliver(...)',
                self.name, msg, args
            )
        end
    }
}

local function bucket_new(id, name, srv, msg_count)
    local conn = remote.new(srv, {
        reconnect_after = RECONNECT_AFTER,
        wait_connected  = true
    })
    -- if conn:eval("return box.info.server.uuid") == box.info.server.uuid then
    --     conn = remote.self
    -- end

    local self = setmetatable({
        uri          = srv,
        name         = name,
        count        = 0,
        max_count    = msg_count,
        worker       = nil,
        msg_pool     = table_new(msg_count, 0),
        connection   = conn,
        waiting_list = {},
    }, bucket_mt)

    self.worker = fiber.create(pusher_handler(id), self)

    fun.range(msg_count):each(function(id)
        self.msg_pool[id] = table_new(3, 0)
    end)

    return self
end

local function waitpool_handler(id, bucket)
    local function handler_tmp(self)
        fiber.self():name(string.format('waitpool_handler-%02d', id))
        local id     = id
        local bucket = bucket
        while true do
            local rv = self.channel_in:get()
            if rv == false then
                error('error')
            end
            local status = true
            local result = ''
            bucket:send(self.msg, self.args)
            -- local status, result = pcall(bucket.send, bucket, self.msg, self.args)
            if status == false then
                log.error('Error, while executing message: %s', result)
                table.insert(self.errors, result)
            end
            self.channel_out:put(status)
        end
    end

    return function(self)
        xpcall_tb(handler_tmp, self)
    end
end

local waitpool_mt = {
    __call = function(self, msg, args)
        self.msg = msg
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
        by_id = function(self, key)
            key = (type(key) == 'number' and key or digest.crc32(key))
            local bucket_id = 1 + digest.guava(key, self.bucket_cnt)
            return self.buckets[bucket_id]
        end,
        flush = function(self)
            for _, bucket in ipairs(self.buckets) do
                if bucket.count > 0 then
                    bucket.worker:wakeup()
                end
            end
            fiber.yield()
        end,
        send_wait = function(self, message, args)
            self.waitpool(message, args)
        end
    }
}

local function mpool_new(name, servers, options)
    options = options or {}
    local msg_count = options.msg_count or 1000

    local self = setmetatable({
        name       = name,
        may_wakeup = true,
        buckets    = {},
        bucket_cnt = 0,
    }, mpool_mt)

    for k, server in ipairs(servers) do
        table.insert(self.buckets, bucket_new(k, name, server, msg_count))
        self.bucket_cnt = self.bucket_cnt + 1
    end

    self.waitpool = waitpool_new(self)

    return self
end

return strict.strictify({
    new = mpool_new
})
