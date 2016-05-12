local fun   = require('fun')
local log   = require('log')
local uri   = require('uri')
local json  = require('json')
local yaml  = require('yaml')
local fiber = require('fiber')

yaml.cfg{
    encode_load_metatables = true,
    encode_use_tostring = true,
    encode_invalid_as_nil = true,
}

local pool               = require('pregel.utils.connpool')
local fiber_pool         = require('pregel.utils.fiber_pool')
local find_worker_by_key = require('pregel.worker').find_worker_by_key
local mpool              = require('pregel.mpool')

local function pregel_pool_on_connected(channel)
    return function()
        channel:put(1)
        while channel:has_readers() do
            channel:put(1)
        end
    end
end

local function pregel_master_loop(self)
    return self
end

local pregel_master_methods = {
    start = function(self)
    end,
    preload = function(self)
        self.preload_func()
        self.mpool:flush()
    end,
}

--
-- servers = {
--     'login:password@host1:port1',
--     'login:password@host2:port2',
--     'login:password@host3:port3',
--     'login:password@host4:port4',
-- }
--

local pregel_master_new = function(name, options)
    local connections = options.connections

    -- parse workers
    local workers = options.workers or {}

    local self = setmetatable({
        name         = name,
        preload_func = nil,
        workers      = workers,
        mpool        = mpool.new(name, workers)
    }, {
        __index = pregel_master_methods
    })

    local preload = options.preload
    if type(preload) == 'function' then
        preload = preload(self, options.preload_args)
    elseif type(preload) == 'table' then
        preload = preload
    else
        assert(false, 'expected "function"/"table", got ' .. type(options.preload))
    end

    self.preload_func = preload

    return self
end

return {
    new = pregel_master_new
}
