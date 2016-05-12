local fun    = require('fun')
local log    = require('log')
local fiber  = require('fiber')
local digest = require('digest')
local remote = require('net.box')

local table       = require('table')
local table_new   = require('table.new')

local strict      = require('pregel.utils.strict')

local xpcall_tb = require('pregel.utils').xpcall_tb

local RECONNECT_AFTER = 5
local INFINITY = 9223372036854775808

local function fiber_dispatcher_func(self)
    local fid = fiber.self().id()
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

local function fiber_dispatcher_func_wrapper(self)
    xpcall_tb(fiber_dispatcher_func, self)
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
        end
    }
}

local function bucket_new(name, srv, msg_count)
    local connection = remote.new(srv, {
        reconnect_after = RECONNECT_AFTER,
        wait_connected  = true
    })

    local self = setmetatable({
        uri          = srv,
        name         = name,
        count        = 0,
        max_count    = msg_count,
        worker       = nil,
        msg_pool     = table_new(msg_count, 0),
        connection   = connection,
        waiting_list = {},
    }, bucket_mt)

    self.worker = fiber.create(fiber_dispatcher_func_wrapper, self)

    fun.range(msg_count):each(function(id)
        self.msg_pool[id] = table_new(3, 0)
    end)

    return self
end

local mpool_mt = {
    __index = {
        by_key = function(self, key)
            key = (type(key) == 'number' and key or digest.crc32(key))
            return self.buckets[1 + digest.guava(key, self.bucket_cnt)]
        end,
        flush = function(self)
            for _, bucket in ipairs(self.buckets) do
                if bucket.count > 0 then
                    bucket.worker:wakeup()
                end
            end
            fiber.yield()
        end
    }
}

local function mpool_new(name, servers, options)
    options = options or {}
    local msg_count = options.msg_count or 25000

    local self = setmetatable({
        name       = name,
        may_wakeup = true,
        buckets    = {},
        bucket_cnt = 0,
    }, mpool_mt)

    for _, server in ipairs(servers) do
        table.insert(self.buckets, bucket_new(name, server, msg_count))
        self.bucket_cnt = self.bucket_cnt + 1
    end

    return self
end

return strict.strictify({
    new = mpool_new
})
