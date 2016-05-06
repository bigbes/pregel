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

local function loader_methods(master)
    local function send_message(srv, msg, value)
        local result = srv:eval(
                'return require("pregel.worker").info_worker(...)',
                master.name, msg, value
        )
    end

    return {
        -- add, only if not exists.
        add_vertex   = function(id, name, value)
            local srv = master:mapping(id)
            send_message(srv, 'vertex.add' {id, name, value})
        end,
        -- no conflict resolving, reset state to new
        store_vertex = function(id, name, value)
            local srv = master:mapping(id)
            send_message(srv, 'vertex.store' {id, name, value})
        end,
        -- no conflict resolving, may be dups in output.
        store_edge   = function(src, dest, value)
            local srv = master:mapping(src)
            send_message(srv, 'edge.store' {src, {dest, value}})
        end,
        -- no conflict resolving, may be dups in output.
        store_edges_batch   = function(src, list)
            local srv = master:mapping(src)
            send_message(srv, 'edge.store', {src, unpack(list)})
        end,
        add_vertex_edges      = function(id, name, value, list)
            local srv = master:mapping(id)
            self:add_vertex(id, value)
            self:store_edges_batch(id, name, list)
        end,
        store_vertex_edges    = function(id, name, value, list)
            self:store_vertex(id, value)
            self:store_edges_batch(id, name, list)
        end,
    }
end

-- local function loader_new(master, options)
--     local loader        = options.loader
local function loader_new(master, loader)
    assert(is_callable(loader), 'options.loader must be callable')
    return setmetatable({
    }, {
        __call  = loader
        __index = loader_methods(master)
    })
end

local function loader_graph_edges_file(master, file)
    local function loader(self)
        log.info('loading table of edges from file "%s"', file)
        local graph = {}

        local f = fio.open(filename, {'O_RDONLY'})
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
                        local id, name, value = line:match("^(%d+) '([^']+)' (%d+)$")
                        graph[id] = {id, false, name, value, {}}
                    elseif section == 2 then
                        local v1, v2, val = line:match("^(%d+) (%d+) (%d+)$")
                        table.insert(graph[id][5][v1], {v2, val})
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
            local id, _, name, value, tbl = tuple[5]
            table.sort(tbl, function(lp, rp)
                return (lp[1] > rp[1])
            end)
            self:store_vertex_edges(id, name, value, tbl)
        end
    end
    return loader_new(master, loader)
end

local function loader_graph_edges_batch_file(master, file)
    local function loader(self)
        log.info('loading table of edges from file "%s"', file)
        local graph = {}

        local f = fio.open(filename, {'O_RDONLY'})
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
                            local id, name, value = line:match("^(%d+) '([^']+)' (%d+)$")
                            graph[id] = {id, false, name, value, {}}
                        elseif section == 2 then
                            local v1, v2, val = line:match("^(%d+) (%d+) (%d+)$")
                            table.insert(graph[id][5][v1], {v2, val})
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
            for _, edge in pairs(graph) do
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
            local id, _, name, value, tbl = tuple[5]
            table.sort(tbl, function(lp, rp)
                return (lp[1] > rp[1])
            end)
            self:store_vertex_edges(id, name, value, tbl)
        end
    end
    return loader_new(master, loader)
end

return {
    new = loader_new,
    graph_edges_f = loader_graph_edges_file,
    graph_edges_cf = loader_graph_edges_chunked_file,
}
