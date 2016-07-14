local fio = require('fio')
local log = require('log')
local json = require('json')
local yaml = require('yaml')

yaml.cfg{
    encode_use_tostring = true
}

local random      = require('pregel.utils').random
local timeit      = require('pregel.utils').timeit
local is_main     = require('pregel.utils').is_main
local xpcall_tb   = require('pregel.utils').xpcall_tb
local defaultdict = require('pregel.utils.collections').defaultdict
local strict      = require('pregel.utils.strict')

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

local function main(options)
    xpcall_tb(function()
        local instance = pregel.new('test', options)
        local time = timeit(instance.run, instance)
        log.info('overall time is %f', time)
        instance.space:pairs():take(10):each(function(tuple)
            print(json.encode(tuple))
        end)
    end)
end

return strict.strictify({
    main                      = main,
    graph_max_process         = graph_max_process,
    preload_from_file         = preload_from_file,
    preload_from_file_chunked = preload_from_file_chunked
})
