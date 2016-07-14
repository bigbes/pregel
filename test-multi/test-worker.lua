local log = require('log')

local ploader = require('pregel.loader')
local pmaster = require('pregel.master')
local pworker = require('pregel.worker')

local xpcall_tb = require('pregel.utils').xpcall_tb

local common = require('common')

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
    local val = self:get_value()
    if self:get_superstep() == 1 then
        inform_neighbors(self, val.value)
    elseif self:get_superstep() < 30 then
        local modified = false
        for _, msg in self:pairs_messages() do
            if val.value < msg then
                val.value = msg
                self:set_value(val)
                modified = true
            end
        end
        if modified then
            inform_neighbors(self, val.value)
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
    },
    compute        = graph_max_process,
    combiner       = math.max,
    master_preload = ploader.graph_edges_f,
    worker_preload = nil,
    preload_args   = 'data/soc-Epinions-custom-bi.txt',
    squash_only    = false,
    pool_size      = 10000,
    delayed_push   = false,
    obtain_name    = obtain_name
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
