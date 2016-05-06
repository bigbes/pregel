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

local pool = require('pregel.utils.connpool')
local fiber_pool = require('pregel.utils.fiber_pool')

local function pregel_pool_on_connected(channel)
    return function()
        channel:put(1)
        while channel:has_readers() do
            channel:put(1)
        end
    end
end

local function pregel_master_loop(self)
    local ch = fiber.channel(1)
    self.pool.on_connected = pregel_pool_on_connected(ch)
    self.pool:init(self.pool_cfg)
    local rv = ch:get()
    assert(rv == 1, 'bad object returned, expect "1", got "' .. tostring(rv) .. '"')
    -- everyone is connected now

    local srv_list = fun.iter(self.pool:all('default')):map(function()
        return srv.conn
    end):totable()

    local function pregel_alert_start(srv)
        local result = srv:eval('return require("pregel.worker").info_worker(...)', self.name, msg)
        log.info('finished %s', json.encode(result))
    end

    local alert_pool = fiber_pool.new(pregel_alert_start, #srv_list)

    while true do
        -- alert everyone to run superstep
        alert_pool:apply(srv_list):wait()
        log.info('superstep finished')
        os.exit(0)
    end
end

local pregel_master_methods = {
    start = function(self)
        self.fiber = fiber.create(pregel_master_loop, self)
    end
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
    local workers_pool = fun.iter(workers):map(function(wrk)
        local wrk = uri.parse(wrk)
        return {
            uri = wrk.host .. ':' .. wrk.service,
            password = wrk.password,
            login = wrk.login,
            zone = 'default'
        }
    end):totable()

    local pool_cfg = {
        pool_name = name,
        servers = workers_pool,
        monitor = true
    }

    local self = setmetatable({
        name = name,
        pool = pool.new(),
        workers = workers,
        pool_cfg = pool_cfg,
    }, {
        __index = pregel_master_methods
    })

    return self
end

return {
    new = pregel_master_new
}
