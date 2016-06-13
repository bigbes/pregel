local log = require('log')

local ploader = require('pregel.loader')
local pmaster = require('pregel.master')
local pworker = require('pregel.worker')

local xpcall_tb = require('pregel.utils').xpcall_tb

local worker, port_offset = arg[0]:match('(%a+)-(%d+)')

port_offset = port_offset or 0

local function obtain_name(value)
    return value.name
end

local function inform_neighbors(self, val)
    for id, neighbor, weight in self:pairs_edges() do
        self:send_message(neighbor, val)
    end
end

local function graph_max_process(self)
    if self.superstep == 1 then
        inform_neighbors(self, self:get_value())
    elseif self.superstep < 30 then
        local modified = false
        for _, msg in self:pairs_messages() do
            -- if type(self:get_value()) ~= type(msg) then
            --     log.info('============================================')
            --     log.info('bad types "%s" "%s"', tostring(self:get_value()), tostring(msg))
            --     log.info('bad types "%s" "%s"', type(self:get_value()), type(msg))
            --     log.info('============================================')
            -- end
            if self:get_value() < msg then
                self:set_value(msg)
                modified = true
            end
        end
        if modified then
            inform_neighbors(self, self:get_value())
        end
    end
    self:vote_halt(true)
end

local common_cfg = {
    master       = 'localhost:3301',
    workers      = {
        'localhost:3302',
        'localhost:3303',
        'localhost:3304',
        'localhost:3305',
--[[--
        'localhost:3306',
        'localhost:3307',
        'localhost:3308',
        'localhost:3309',
--]]--
    },
    compute      = graph_max_process,
    combiner     = math.max,
    preload      = ploader.graph_edges_f,
    preload_args = '../test/data/soc-Epinions-custom-bi.txt',
    -- preload_args = '/Users/blikh/src/work/pregel-data/actual/soc-pokec-relationshit-custom-bi.txt',
    squash_only  = false,
    pool_size    = 10000,
    delayed_push = false,
    obtain_name  = obtain_name
}

if worker == 'worker' then
    box.cfg{
        wal_mode = 'none',
        listen = 'localhost:' .. tostring(3301 + port_offset),
        background = true,
        logger_nonblock = false
    }
else
    box.cfg{
        wal_mode = 'none',
        listen = 'localhost:' .. tostring(3301 + port_offset),
        logger_nonblock = false
    }
end

box.once('bootstrap', function()
    box.schema.user.grant('guest', 'read,write,execute', 'universe')
end)

if worker == 'worker' then
    worker = pworker.new('test', common_cfg)
else
    xpcall_tb(function()
        local master = pmaster.new('test', common_cfg)
        master:wait_up()
        if arg[1] == 'load' then
            master:preload()
            master.mpool:send_wait('snapshot')
        end
        master:start()
    end)
    os.exit(0)
end
