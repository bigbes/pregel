local fun   = require('fun')
local log   = require('log')
local uri   = require('uri')
local json  = require('json')
local yaml  = require('yaml')
local clock = require('clock')
local fiber = require('fiber')

local is_callable = require('pregel.utils').is_callable

yaml.cfg{
    encode_load_metatables = true,
    encode_use_tostring = true,
    encode_invalid_as_nil = true,
}

local mpool      = require('pregel.mpool')
local aggregator = require('pregel.aggregator')

local xpcall_tb = require('pregel.utils').xpcall_tb

local master = nil

local info_functions = setmetatable({
    ['aggregator.inform'] = function(args)
        -- args[1] - aggregator name
        -- args[2] - aggregator new value
        return master.aggregators[args[1]]:merge_master(args[2])
    end,
}, {
    __index = function(self, op)
        return function(k)
            error('unknown operation: %s', op)
        end
    end
})

local function deliver_msg(msg, args)
    local stat, err = xpcall_tb(function()
        info_functions[msg](args)
        return 1
    end)

    if stat == false then
        error(tostring(err))
    end
end

local master_mt = {
    __index = {
        wait_up = function (self)
            self.mpool:send_wait('wait')
        end,
        start = function (self)
            log.info('master:start(): begin')
            self.mpool:send_wait('count')
            local superstep = 1
            while true do
                log.info('master:start(): superstep %d start', superstep)
                local result = self.mpool:send_wait('superstep', superstep)
                for _, v in ipairs(result) do
                    log.info('superstep took %010.6f seconds', v[1])
                end
                -- default all aggregators
                for k, v in pairs(self.aggregators) do
                    v:make_default()
                end
                self.mpool:send_wait('superstep.after')
                log.info('master:start(): superstep %d end', superstep)

                -- now, when we gather all values, inform workers
                for k, v in pairs(self.aggregators) do
                    v:inform_workers()
                end
                self.mpool:flush()

                local msg_count = self.aggregators['__messages']()
                local inp_count = self.aggregators['__in_progress']()
                log.info('master:start(): %d messages and %d in progress',
                         msg_count, inp_count)
                if msg_count == 0 and inp_count == 0 then
                    -- we don't have anything to do, so stop iterating
                    break
                end
                superstep = superstep + 1
            end
            log.info('master:start(): end')
        end,
        preload = function(self)
            self.preload_func()
            self.mpool:flush()
        end,
        preload_on_workers = function(self)
            self.mpool:send_wait('preload')
        end,
        add_aggregator = function(self, name, opts)
            assert(self.aggregators[name] == nil)
            self.aggregators[name] = aggregator.new(name, self, opts)
            return self
        end,
        save_snapshot = function(self)
            master.mpool:send_wait('snapshot')
        end,
    }
}

--
-- servers = {
--     'login:password@host1:port1',
--     'login:password@host2:port2',
--     'login:password@host3:port3',
--     'login:password@host4:port4',
-- }
--
local master_new = function(name, options)
    local workers     = options.workers or {}
    local pool_size   = options.pool_size or 1000
    local obtain_name = options.obtain_name

    assert(is_callable(obtain_name),      'options.obtain_name must be callable')

    local self = setmetatable({
        name         = name,
        preload_func = nil,
        workers      = workers,
        mpool        = mpool.new(name, workers, {
            msg_count = pool_size
        }),
        obtain_name  = obtain_name,
        aggregators  = {}
    }, master_mt)

    local preload = options.master_preload
    if type(preload) == 'function' then
        preload = preload(self, options.preload_args)
    elseif type(preload) == 'table' then
        preload = preload
    else
        assert(false,
            string.format('<master_preload> expected "function"/"table", got %s',
                          type(options.master_preload))
        )
    end
    self.preload_func = preload

    self:add_aggregator('__in_progress', {
        internal = true,
        default  = 0,
        merge    = function(old, new)
            return old + new
        end,
    }):add_aggregator('__messages', {
        internal = true,
        default  = 0,
        merge    = function(old, new)
            return old + new
        end,
    })

    master = self

    return self
end

return {
    new     = master_new,
    deliver = deliver_msg,
}
