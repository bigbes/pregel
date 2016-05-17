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
    master       = 'localhost:3301',
    workers      = {
        'localhost:3303',
        'localhost:3304',
        'localhost:3305',
        'localhost:3306',
    },
    compute      = common.graph_max_process,
    preload      = loader.graph_edges_f,
    preload_args = '../data/soc-Epinions-custom-bi.txt',
    combiner     = math.max,
    squash_only  = false
})

box.schema.user.grant('guest', 'read,write,execute', 'universe')

master:wait_up()
xpcall_tb(master.preload, master)
master.mpool:send_wait('snapshot')
xpcall_tb(master.start, master)
os.exit(0)
