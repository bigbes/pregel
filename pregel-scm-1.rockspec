package = 'pregel'
version = 'scm-1'
source = {
    url    = 'git+https://github.com/bigbes/pregel.git',
    branch = 'tarantool3',
}
description = {
    summary  = 'Pregel graph processing model for Tarantool',
    homepage = 'https://github.com/bigbes/pregel',
    license  = 'BSD',
}
dependencies = {
    'lua ~> 5.1',
}
build = {
    type = 'builtin',
    modules = {
        ['pregel.aggregator']        = 'pregel/aggregator.lua',
        ['pregel.loader']            = 'pregel/loader.lua',
        ['pregel.master']            = 'pregel/master.lua',
        ['pregel.mpool']             = 'pregel/mpool.lua',
        ['pregel.queue']             = 'pregel/queue.lua',
        ['pregel.vertex']            = 'pregel/vertex.lua',
        ['pregel.worker']            = 'pregel/worker.lua',
        ['pregel.roles.common']      = 'pregel/roles/common.lua',
        ['pregel.roles.master']      = 'pregel/roles/master.lua',
        ['pregel.roles.worker']      = 'pregel/roles/worker.lua',
        ['pregel.utils']             = 'pregel/utils/init.lua',
        ['pregel.utils.collections'] = 'pregel/utils/collections.lua',
        ['pregel.utils.copy']        = 'pregel/utils/copy.lua',
        ['pregel.utils.strict']      = 'pregel/utils/strict.lua',
        ['pregel.avro']              = 'pregel/avro/init.lua',
        ['pregel.avro.schema']       = 'pregel/avro/schema.lua',
        ['pregel.avro.codec']        = 'pregel/avro/codec.lua',
        ['pregel.avro.deflate']      = 'pregel/avro/deflate.lua',
        ['pregel.avro.ocf']          = 'pregel/avro/ocf.lua',
        ['pregel.avro.resolve']      = 'pregel/avro/resolve.lua',
        ['pregel.math.vector']       = 'pregel/math/vector.lua',
        ['pregel.math.gd']           = 'pregel/math/gd.lua',
        ['pregel.math.auc']          = 'pregel/math/auc.lua',
    },
}
