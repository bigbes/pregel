local fio  = require('fio')
local log  = require('log')
local json = require('json')
local yaml = require('yaml')

yaml.cfg{
    encode_use_tostring = true
}

-- local pregel      = require('pregel')
local strict      = require('pregel.utils.strict')

local fmtstring   = string.format
local random      = require('pregel.utils').random
local is_main     = require('pregel.utils').is_main
local timeit      = require('pregel.utils').timeit
local xpcall_tb   = require('pregel.utils').xpcall_tb
local is_callable = require('pregel.utils').is_callable

local table_clear = require('table.clear')

local function loader_methods(instance)
    return {
        -- no conflict resolving, reset state to new
        store_vertex          = function(self, vertex)
            local id = instance.obtain_name(vertex)
            instance.mpool:by_id(id):put('vertex.store', vertex)
            return id
        end,
        -- no conflict resolving, may be dups in output.
        store_edge            = function(self, src, dest, value)
            instance.mpool:by_id(src):put('edge.store', {src {dest, value}})
        end,
        -- no conflict resolving, may be dups in output.
        store_edges_batch     = function(self, src, list)
            instance.mpool:by_id(src):put('edge.store', {src, list})
        end,
        store_vertex_edges    = function(self, vertex, list)
            local id = self:store_vertex(vertex)
            self:store_edges_batch(vertex, list)
            return id
        end,
        flush                 = function(self)
            instance.mpool:flush()
        end,
    }
end

local function loader_new(instance, loader)
    assert(is_callable(loader), 'options.loader must be callable')
    return setmetatable({
    }, {
        __call  = loader,
        __index = loader_methods(instance)
    })
end

local function loader_graph_edges_file(master, file)
    local function loader(self)
        log.info('loading table of edges from file "%s"', file)

        local f = fio.open(file, {'O_RDONLY'})
        assert(f ~= nil, 'Bad file path')

        local buf = ''
        local processed = 0
        -- section 0 is initial value
        -- section 1 is for vertex list: <id> '<name>' '<value>'
        -- section 2 is for edge list: <source> <destination> <value>
        -- section ID is updated on strings, that started with '#'
        local section = 0

        local count_gc = 0
        -- for batching
        local count = 0
        local current_id = -1
        local current_edges = {}

        local vertices = {}

        while true do
            buf = buf .. f:read(4096)
            if #buf == 0 then
                break
            end
            for line in buf:gmatch("[^\n]+\n") do
                if line:sub(1, 1) == '#' then
                    section = section + 1
                    -- self:flush()
                else
                    if count_gc == 1000000 then
                        count_gc = 0
                        collectgarbage('collect')
                        collectgarbage('collect')
                    end
                    count_gc = count_gc + 1
                    if section == 1 then
                        local id, name, value = line:match("(%d+) '([^']+)' (%d+)")
                        id, value = tonumber(id), tonumber(value)
                        local id_new = self:store_vertex({
                            id = id, value = value, name = name
                        })
                        vertices[id] = id_new
                    elseif section == 2 then
                        local v1, v2, val = line:match("(%d+) (%d+) (%d+)")
                        v1, v2 = tonumber(v1), tonumber(v2)
                        if v1 == current_id and count ~= 1000 then
                            count = count + 1
                            table.insert(current_edges, {vertices[v2], val})
                        else
                            count = 0
                            if #current_edges ~= 0 then
                                self:store_edges_batch(vertices[v1], current_edges)
                            end
                            current_id = v1
                            current_edges = {{vertices[v2], val}}
                        end
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
    end
    return loader_new(master, loader)
end

return strict.strictify({
    new = loader_new,
    graph_edges_f = loader_graph_edges_file,
})
