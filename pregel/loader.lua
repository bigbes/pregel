local fio = require('fio')
local log = require('log')
local json = require('json')
local yaml = require('yaml')

yaml.cfg{
    encode_use_tostring = true
}

local pregel      = require('pregel')
local strict      = require('pregel.utils.strict')

local fpool       = require('pregel.utils.fiber_pool')

local fmtstring   = string.format
local random      = require('pregel.utils').random
local is_main     = require('pregel.utils').is_main
local defaultdict = require('pregel.utils.collections').defaultdict
local timeit      = require('pregel.utils').timeit
local xpcall_tb   = require('pregel.utils').xpcall_tb
local is_callable = require('pregel.utils').is_callable


local function loader_methods(master)
    return {
        -- add, only if not exists.
        add_vertex   = function(self, id, name, value)
            master.mpool:by_key(id):put('vertex.add', {id, name, value})
        end,
        -- no conflict resolving, reset state to new
        store_vertex = function(self, id, name, value)
            master.mpool:by_key(id):put('vertex.store', {id, name, value})
        end,
        -- no conflict resolving, may be dups in output.
        store_edge   = function(self, src, dest, value)
            master.mpool:by_key(src):put('edge.store', {src, {dest, value}})
        end,
        -- no conflict resolving, may be dups in output.
        store_edges_batch   = function(self, src, list)
            master.mpool:by_key(src):put('edge.store', {src, unpack(list)})
        end,
        add_vertex_edges      = function(self, id, name, value, list)
            self:add_vertex(id, name, value)
            self:store_edges_batch(id, list)
        end,
        store_vertex_edges    = function(self, id, name, value, list)
            self:store_vertex(id, name, value)
            self:store_edges_batch(id, list)
        end,
    }
end

-- local function loader_new(master, options)
--     local loader        = options.loader
local function loader_new(master, loader)
    assert(is_callable(loader), 'options.loader must be callable')
    return setmetatable({
    }, {
        __call  = loader,
        __index = loader_methods(master)
    })
end

local function loader_graph_edges_file(master, file)
    local function loader(self)
        log.info('loading table of edges from file "%s"', file)
        local graph = {}

        local f = fio.open(file, {'O_RDONLY'})
        assert(f ~= nil, 'Bad file path')

        local buf = ''
        local processed = 0
        -- section 0 is initial value
        -- section 1 is for vertex list: <id> '<name>' '<value>'
        -- section 2 is for edge list: <source> <destination> <value>
        -- section ID is updated on strings, that started with '#'
        local section = 0

        while true do
            buf = buf .. f:read(4096)
            if #buf == 0 then
                break
            end
            for line in buf:gmatch("[^\n]+\n") do
                if line:sub(1, 1) == '#' then
                    section = section + 1
                else
                    if section == 1 then
                        local id, name, value = line:match("(%d+) '([^']+)' (%d+)")
                        id = tonumber(id)
                        graph[id] = {id, false, name, value, {}}
                    elseif section == 2 then
                        local v1, v2, val = line:match("(%d+) (%d+) (%d+)")
                        v1, v2 = tonumber(v1), tonumber(v2)
                        table.insert(graph[v1][5], {v2, val})
                    end
                end
                processed = processed + 1
                if processed % 100000 == 0 then
                    log.info('processed %d lines', processed)
                end
            end
            buf = buf:match("\n([^\n]*)$")
        end
        log.info('processed %d lines', processed)

        for _, tuple in pairs(graph) do
            local id, _, name, value, tbl = unpack(tuple)
            table.sort(tbl, function(lp, rp)
                return (lp[1] > rp[1])
            end)
            self:store_vertex_edges(id, name, value, tbl)
        end
    end
    return loader_new(master, loader)
end

local function loader_graph_edges_chunked_file(master, args)
    local file = args.file
    local chunk_size = args.chunk_size
    assert(file and chunk_size)
    local function loader(self)
        log.info('loading table of edges from file "%s"', file)
        local graph = {}

        local f = fio.open(file, {'O_RDONLY'})
        assert(f ~= nil, 'Bad file path')

        local buf = ''
        local processed = 0
        -- section 0 is initial value
        -- section 1 is for vertex list: <id> '<name>' '<value>'
        -- section 2 is for edge list: <source> <destination> <value>
        -- section ID is updated on strings, that started with '#'
        local section = 0

        local function reset_graph()
            graph = {}
            collectgarbage('collect')
            collectgarbage('collect')
        end

        local function process_chunk()
            while true do
                buf = buf .. f:read(4096)
                if #buf == 0 then
                    break
                end
                for line in buf:gmatch("[^\n]+\n") do
                    if line:sub(1, 1) == '#' then
                        section = section + 1
                        buf = buf:match("#[^\n]*\n(.*)")
                        return true
                    else
                        if section == 1 then
                            local id, name, value = line:match("(%d+) '([^']+)' (%d+)")
                            id = tonumber(id)
                            graph[id] = {id, false, name, value, {}}
                        elseif section == 2 then
                            local v1, v2, val = line:match("(%d+) (%d+) (%d+)")
                            v1, v2 = tonumber(v1), tonumber(v2)
                            table.insert(graph[v1][5], {v2, val})
                        end
                    end
                    processed = processed + 1
                    if processed % 100000 == 0 then
                        log.info('processed %d lines', processed)
                    end
                    if processed % chunk_size == 0 then
                        return true
                    end
                end
                buf = buf:match("\n([^\n]*)$")
            end
            return false
        end
        log.info('processed %d lines', processed)

        reset_graph()
        while process_chunk() do
            for _, tuple in pairs(graph) do
                local id, _, name, value, tbl = tuple[5]
                self:store_vertex_edges(id, name, value, tbl)
                if #tbl == 0 then
                    self:store_vertex(id, name, value)
                else
                    self:store_edges_batch(id, tbl)
                end
            end
            reset_graph()
        end

        for _, tuple in pairs(graph) do
            local id, _, name, value, tbl = unpack(tuple)
            table.sort(tbl, function(lp, rp)
                return (lp[1] > rp[1])
            end)
            self:store_vertex_edges(id, name, value, tbl)
        end
    end
    return loader_new(master, loader)
end

return strict.strictify({
    new = loader_new,
    graph_edges_f = loader_graph_edges_file,
    graph_edges_cf = loader_graph_edges_chunked_file,
})
