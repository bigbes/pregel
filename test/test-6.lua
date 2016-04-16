local common = require('common')
local is_main = require('pregel.utils').is_main

if is_main() then
    box.cfg{
        wal_mode = 'none',
        logger_nonblock = false,
        logger = 'test-6.log',
    }

    common.main({
        compute = common.graph_max_process,
        preload = common.preload_from_file('data/soc-Epinions1.txt'),
        tube_engine = 'table'
    })

    os.exit(0)
end
