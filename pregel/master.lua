--- The pregel master.
--
-- The master owns no graph. It drives the superstep loop -- run the compute
-- functions everywhere, apply the topology mutations everywhere, merge the
-- aggregators, hand the merged values back -- and stops when no worker has a
-- message left to deliver or a vertex left running.

local log = require('log')

local mpool      = require('pregel.mpool')
local aggregator = require('pregel.aggregator')

local utils       = require('pregel.utils')
local is_callable = utils.is_callable
local xpcall_tb   = utils.xpcall_tb
local error       = utils.error

local RPC_DELIVER = 'pregel.master.deliver'

-- One master per process, so the RPC entry point does not need to be told
-- which one it is talking to.
local master = nil

local info_functions = setmetatable({
    ['aggregator.inform'] = function(args)
        -- args[1] - aggregator name, args[2] - the worker's value
        local aggr = master.aggregators[args[1]]
        if aggr == nil then
            error("unknown aggregator: %s", tostring(args[1]))
        end
        return aggr:merge_master(args[2])
    end,
}, {
    __index = function(_, op)
        return function()
            -- The 1.6 version called the global error() here, which takes a
            -- level rather than format arguments: it raised "bad argument #2
            -- to 'error'" and never said which operation was unknown.
            error('unknown operation: %s', op)
        end
    end
})

local function deliver_msg(msg, args)
    assert(master ~= nil, 'no pregel master found')
    local status, err = xpcall_tb(function()
        info_functions[msg](args)
        return 1
    end)
    if status == false then
        error(tostring(err))
    end
    return 'ok'
end

local master_mt = {
    __index = {
        --- Block until every worker is up and has reached this master.
        wait_up = function(self)
            self.mpool:send_wait('wait')
            return self
        end,
        --- Run supersteps until the graph goes quiet.
        start = function(self)
            log.info('master:start(): begin')
            self.mpool:send_wait('count')
            local superstep = 1
            while true do
                -- Published as the loop goes rather than only at the end, so
                -- something watching the master -- pregel.roles.master's
                -- status(), a console -- can see how far a running job is.
                self.superstep_count = superstep
                log.info('master:start(): superstep %d start', superstep)
                local result = self.mpool:send_wait('superstep', superstep)
                for _, v in ipairs(result) do
                    log.info('superstep took %010.6f seconds', v[1])
                end
                -- Reset every aggregator before the workers report into it,
                -- so a superstep aggregates its own values and not the sum of
                -- every superstep so far.
                for _, aggr in pairs(self.aggregators) do
                    aggr:make_default()
                end
                self.mpool:send_wait('superstep.after')
                log.info('master:start(): superstep %d end', superstep)

                -- Now that every worker has reported, hand the merged values
                -- back out.
                for _, aggr in pairs(self.aggregators) do
                    aggr:inform_workers()
                end
                self.mpool:flush()

                local msg_count = self.aggregators['__messages']()
                local inp_count = self.aggregators['__in_progress']()
                log.info('master:start(): %d message(s) and %d in progress',
                         msg_count, inp_count)
                if msg_count == 0 and inp_count == 0 then
                    break
                end
                superstep = superstep + 1
            end
            log.info('master:start(): end after %d superstep(s)', superstep)
            return superstep
        end,
        --- Run the master-side loader, then push what it produced.
        preload = function(self)
            assert(self.preload_func ~= nil,
                   'no master_preload configured for this instance')
            self.preload_func()
            self.mpool:flush()
            return self
        end,
        --- Ask every worker to run its own loader.
        preload_on_workers = function(self)
            self.mpool:send_wait('preload')
            return self
        end,
        add_aggregator = function(self, name, opts)
            assert(self.aggregators[name] == nil,
                   'aggregator already exists: ' .. tostring(name))
            self.aggregators[name] = aggregator.new(name, self, opts)
            return self
        end,
        save_snapshot = function(self)
            self.mpool:send_wait('snapshot')
            return self
        end,
        stop = function(self)
            self.mpool:stop()
            if master == self then
                master = nil
            end
        end,
    }
}

--- Let `user` call this module's RPC entry point.
local function grant(user)
    box.schema.user.grant(user, 'execute', 'lua_call', RPC_DELIVER,
                          {if_not_exists = true})
end

--- Create the master for the instance called `name`.
--
-- options.workers        -- array of every worker's net.box URI, each of which
--                           may carry its own 'user:password@host:port'
-- options.obtain_name    -- callable(value) -> vertex name (required)
-- options.pool_size      -- messages per batch (default 1000)
-- options.master_preload -- callable(self, preload_args) -> loader, or a
--                           loader, or nil for a master that only coordinates
-- options.preload_args   -- passed to master_preload
-- options.user           -- net.box user for the outgoing connections
-- options.password       -- net.box password for the outgoing connections
-- options.connect_async  -- build the message pool without waiting for any
--                           worker; the caller then owns the waiting
--                           (master.mpool:wait_connected(timeout))
-- options.connect_timeout-- seconds to wait for the workers when not async
--
-- A worker URI is either a net.box URI string or a {uri = ..., params = ...}
-- table, the form a Tarantool 3 config uses for a listener with transport
-- parameters.
local function master_new(name, options)
    assert(type(name) == 'string', 'name must be a string')
    assert(type(options) == 'table', 'options must be a table')

    local workers     = options.workers or {}
    local pool_size   = options.pool_size or 1000
    local obtain_name = options.obtain_name

    assert(is_callable(obtain_name), 'options.obtain_name must be callable')

    local self = setmetatable({
        name            = name,
        preload_func    = nil,
        workers         = workers,
        mpool           = mpool.new(name, workers, {
            msg_count       = pool_size,
            user            = options.user,
            password        = options.password,
            connect_async   = options.connect_async,
            connect_timeout = options.connect_timeout,
        }),
        obtain_name     = obtain_name,
        aggregators     = {},
        superstep_count = 0,
    }, master_mt)

    local preload = options.master_preload
    if type(preload) == 'function' then
        preload = preload(self, options.preload_args)
    elseif type(preload) ~= 'table' and type(preload) ~= 'nil' then
        error('<master_preload> expected "function"/"table"/"nil", got "%s"',
              type(preload))
    end
    self.preload_func = preload

    self:add_aggregator('__in_progress', {
        internal = true,
        default  = 0,
        merge    = function(old, new) return old + new end,
    }):add_aggregator('__messages', {
        internal = true,
        default  = 0,
        merge    = function(old, new) return old + new end,
    })

    master = self

    return self
end

-- See the note in worker.lua: the registry table is shared, so it is created
-- only if the other module has not created it already.
rawset(_G, 'pregel', rawget(_G, 'pregel') or {})
_G.pregel.master = {
    deliver = deliver_msg,
}

return {
    new     = master_new,
    grant   = grant,
    deliver = deliver_msg,
}
