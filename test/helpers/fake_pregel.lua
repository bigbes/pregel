--- A stand-in for a worker instance, for testing pregel.vertex in isolation.
--
-- A vertex only ever reaches its instance through four things -- data_space,
-- mqueue, mpool and aggregators -- so the whole of it fits in a recording
-- stub, and a vertex test needs neither a configured box nor a cluster.

local fake = {}

--- An in-memory stand-in for the data_<name> space.
local function data_space_new()
    return {
        replaced = {},
        replace = function(self, tuple)
            table.insert(self.replaced, tuple)
            return tuple
        end,
        last = function(self)
            return self.replaced[#self.replaced]
        end,
    }
end

--- An mpool that records every put instead of sending it.
local function mpool_new()
    local self = {puts = {}}
    local bucket = {
        put = function(_, msg, args)
            table.insert(self.puts, {msg = msg, args = args})
        end
    }
    self.bucket = bucket
    self.by_id = function(_, name)
        table.insert(self.routed, name)
        return bucket
    end
    self.routed = {}
    return self
end

--- A message queue holding a fixed set of messages per receiver.
--
-- Yields (sender, message), the same as the real queue: `senders` is a
-- parallel array per receiver, and a message with no entry in it reads back
-- as box.NULL -- which is what a message put without a sender, or one a
-- combiner produced, looks like.
local function mqueue_new(messages, senders)
    return {
        messages = messages or {},
        senders  = senders or {},
        pairs = function(self, receiver)
            local list = self.messages[receiver] or {}
            local from = self.senders[receiver] or {}
            local idx = 0
            return function()
                idx = idx + 1
                if list[idx] == nil then
                    return nil
                end
                if from[idx] == nil then
                    return box.NULL, list[idx]
                end
                return from[idx], list[idx]
            end
        end,
    }
end

--- Build a fake instance.
--
-- opts.messages       -- {[receiver] = {message, ...}} for pairs_messages
-- opts.senders        -- {[receiver] = {sender, ...}}, parallel to messages
-- opts.aggregators    -- {[name] = callable}
-- opts.obtain_name    -- value -> name, for add_vertex
-- opts.worker_context -- whatever get_worker_context should return
function fake.new(opts)
    opts = opts or {}
    return {
        data_space     = data_space_new(),
        mpool          = mpool_new(),
        mqueue         = mqueue_new(opts.messages, opts.senders),
        aggregators    = opts.aggregators or {},
        in_progress    = opts.in_progress or 0,
        obtain_name    = opts.obtain_name or function(value)
            return tostring(value)
        end,
        worker_context = opts.worker_context,
    }
end

return fake
