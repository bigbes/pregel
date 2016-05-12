local common = require('common')
local master = require('pregel.master')

local loader = require('pregel.loader')

local xpcall_tb = require('pregel.utils').xpcall_tb

box.cfg{
    wal_mode = 'none',
    listen = 'localhost:3301',
    logger_nonblock = false
}

local master = master.new('test', {
    workers = {
        'localhost:3303',
        'localhost:3304',
        'localhost:3305',
        'localhost:3306',
    },
    compute = common.graph_max_process,
    preload = loader.graph_edges_f,
    preload_args = '../data/soc-Epinions-custom-bi.txt',
    aggregator = math.max,
    squash_only = false
})

-- xpcall_tb(master.start, master)
master:start()
xpcall_tb(master.preload, master)
os.exit(0)
