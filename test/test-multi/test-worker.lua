local common = require('common')
local worker = require('pregel.worker')

local xpcall_tb = require('pregel.utils').xpcall_tb

local port_offset = arg[0]:match('(%d+)')
assert(port_offset ~= nil, 'bad worker name')

box.cfg{
    wal_mode = 'none',
    listen = 'localhost:' .. tostring(3302 + port_offset),
    background = true
}

box.schema.user.grant('guest', 'read,write,execute', 'universe', nil, {
    if_not_exists = true
})

local worker = worker.new('test', {
    master      = 'localhost:3301',
    workers     = {
        'localhost:3303',
        'localhost:3304',
        'localhost:3305',
        'localhost:3306',
    },
    compute     = common.graph_max_process,
    combiner    = math.max,
    preload     = common.preload_from_file('../data/soc-Epinions1.txt'),
    squash_only = false
})

-- worker:add_agregator('information', {
--     ''
-- })
