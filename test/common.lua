local fio = require('fio')
local log = require('log')
local json = require('json')
local yaml = require('yaml')

yaml.cfg{
    encode_use_tostring = true
}

local pregel      = require('pregel')
local random      = require('pregel.utils').random
local timeit      = require('pregel.utils').timeit
local is_main     = require('pregel.utils').is_main
local xpcall_tb   = require('pregel.utils').xpcall_tb
local defaultdict = require('pregel.utils.collections').defaultdict
local strict      = require('pregel.utils.strict')

-- Loader for list of graph edges
local function preload_from_file(filename)
    return function(space)
        log.info('loading table of edges from file "%s"', filename)
        local graph = defaultdict(function(key)
            local tbl = {key, false, random(1000000), {}}
            setmetatable(tbl,    { __serialize = 'seq' })
            setmetatable(tbl[4], { __serialize = 'map' })
            return tbl
        end)

        local f = fio.open(filename, {'O_RDONLY'})
        assert(f ~= nil, 'Bad file path')
        local leftovers = ''

        local processed = 0
        while true do
            local buf = leftovers .. f:read(4096)
            if #buf == 0 then break end
            for line in buf:gmatch("[^\n]+\n") do
                local n1, n2 = line:match("^(%d+)\t*(%d+)")
                n1, n2 = tonumber(n1), tonumber(n2)
                if n1 ~= nil and n2 ~= nil then
                    processed = processed + 1
                    if processed % 100000 == 0 then
                        log.info('loaded %d edges', processed)
                    end
                    graph[n1][4][n2] = true
                    graph[n2][4][n1] = true
                end
            end
            local ed = buf:find("\n[^\n]*$")
            leftovers = buf:sub(ed + 1)
        end

        for _, tuple in pairs(graph) do
            local tbl = {}
            for k, v in pairs(tuple[4]) do
                table.insert(tbl, k)
            end
            table.sort(tbl)
            tuple[4] = tbl
            setmetatable(tbl, { __serialize = 'seq' })
            space:insert(tuple)
        end
    end
end

local function preload_from_file_chunked(filename, chunk_size)
    chunk_size = chunk_size or 1000000
    return function(space)
        log.info('loading table of edges (chunked) from file "%s"', filename)

        local f = fio.open(filename, {'O_RDONLY'})
        assert(f ~= nil, 'Bad file path')
        local leftovers = ''
        local graph = nil

        local processed = 0

        local function reset_graph()
            graph = defaultdict(function(key)
                local tbl = {key, false, random(1000000), {}}
                setmetatable(tbl,    { __serialize = 'seq' })
                setmetatable(tbl[4], { __serialize = 'map' })
                return tbl
            end)
            collectgarbage('collect')
            collectgarbage('collect')
        end

        local function process_chunk()
            while true do
                local buf = leftovers .. f:read(4096)
                if #buf == 0 then
                    break
                end
                for line in buf:gmatch("[^\n]+\n") do
                    local n1, n2 = line:match("^(%d+)\t*(%d+)")
                    n1, n2 = tonumber(n1), tonumber(n2)
                    if n1 ~= nil and n2 ~= nil then
                        processed = processed + 1
                        if processed % 100000 == 0 then
                            log.info('loaded %d edges', processed)
                        end
                        if processed % chunk_size == 0 then
                            return true
                        end
                        graph[n1][4][n2] = true
                        graph[n2][4][n1] = true
                    end
                end
                local ed = buf:find("\n[^\n]*$")
                leftovers = buf:sub(ed + 1)
            end
            log.info('loaded %d edges', processed)
            return false
        end

        reset_graph()
        while process_chunk() do
            for id, edge in pairs(graph) do
                local tbl = {}
                local tuple = space:get{id}
                local value = 0
                if tuple ~= nil then
                    for _, v in pairs(tuple[4]) do
                        edge[4][v] = true
                    end
                    value = edge[3]
                else
                    value = random(1000000)
                end
                for k, v in pairs(edge[4]) do
                    table.insert(tbl, k)
                end
                table.sort(setmetatable(tbl, { __serialize = 'seq' }))
                edge[3] = value
                edge[4] = tbl
                space:replace(edge)
            end
            reset_graph()
        end

    end
end

local function inform_neighbors(self, val)
    for id, neighbor, weight in self:pairs_edges() do
        self:send_message(neighbor, val)
    end
end

local function graph_max_process(self)
    -- log.info('superstep: %s', self.superstep)
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
