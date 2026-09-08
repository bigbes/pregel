--- Loaders: how a graph gets into pregel.
--
-- A loader is a callable object. Calling it walks whatever the source is and
-- pushes the graph out through the instance's mpool, one 'vertex.store' or
-- 'edge.store' per batch, each addressed to the worker that owns the vertex.
--
-- Which instance runs the loader is up to the caller: master:preload() runs one
-- on the master, worker:preload() runs one on each worker (and is handed its
-- own index and the worker count, so a loader can split the input).

local fio  = require('fio')
local log  = require('log')

local strict      = require('pregel.utils.strict')
local utils       = require('pregel.utils')
local is_callable = utils.is_callable
local error       = utils.error
local syserror    = utils.syserror

-- How many edges of one source travel in a single 'edge.store'.
local EDGE_BATCH_SIZE = 1000
-- How much of the file to read at a time.
local READ_SIZE = 65536

local SECTION_VERTICES = 1
local SECTION_EDGES    = 2

local function loader_methods(instance)
    return {
        -- no conflict resolving, resets the vertex to this state
        store_vertex = function(_, vertex)
            local id = instance.obtain_name(vertex)
            instance.mpool:by_id(id):put('vertex.store', vertex)
            return id
        end,
        -- no conflict resolving, may produce duplicates
        store_edge = function(_, src, dest, value)
            -- '{src {dest, value}}' parsed as a call of the string src, so
            -- store_edge raised "attempt to call a string value" every time.
            -- edge.store takes (source, list-of-edges).
            instance.mpool:by_id(src):put('edge.store', {src, {{dest, value}}})
        end,
        -- no conflict resolving, may produce duplicates
        store_edges_batch = function(_, src, list)
            instance.mpool:by_id(src):put('edge.store', {src, list})
        end,
        store_vertex_edges = function(self, vertex, list)
            local id = self:store_vertex(vertex)
            -- store_edges_batch wants the source *name*; this used to hand it
            -- the whole vertex value, which then went to the wrong bucket and
            -- arrived as a source no worker could find.
            self:store_edges_batch(id, list)
            return id
        end,
        flush = function()
            instance.mpool:flush()
        end,
    }
end

local function loader_new(instance, loader)
    assert(is_callable(loader), 'options.loader must be callable')
    return setmetatable({}, {
        __call  = loader,
        __index = loader_methods(instance)
    })
end

--- Load a graph from the two-section text format.
--
--   # List of vertices
--   <id> '<name>' <value>
--   # List of edges
--   <source> <destination> <value>
--
-- Ids are the file's own numbering; the vertex names pregel uses come from the
-- instance's obtain_name, and the edge section is translated through that.
-- Edges are batched per source, which is why the file wants them grouped by
-- source -- correctness does not depend on it, only the batch sizes do.
local function loader_graph_edges_file(instance, file)
    local function loader(self)
        log.info('loading graph from file "%s"', file)

        local f = fio.open(file, {'O_RDONLY'})
        if f == nil then
            syserror("cannot open graph file '%s'", file)
        end

        local section = 0
        local processed = 0
        local vertices = {}

        local current_id = nil
        local current_edges = {}

        local function flush_edges()
            if current_id ~= nil and #current_edges > 0 then
                self:store_edges_batch(vertices[current_id], current_edges)
            end
            current_edges = {}
        end

        local function handle_line(line)
            -- Tolerate CRLF, and skip blank lines rather than failing on them.
            line = line:gsub('\r$', '')
            if #line == 0 then
                return
            end
            if line:sub(1, 1) == '#' then
                -- A section header ends whatever batch was accumulating.
                flush_edges()
                current_id = nil
                section = section + 1
                return
            end

            processed = processed + 1
            if processed % 100000 == 0 then
                log.info('processed %d lines', processed)
            end

            if section == SECTION_VERTICES then
                local id, name, value = line:match("^(%d+)%s+'(.*)'%s+(%-?%d+)$")
                if id == nil then
                    error('cannot parse vertex line: %q', line)
                end
                id, value = tonumber(id), tonumber(value)
                vertices[id] = self:store_vertex({
                    id = id, value = value, name = name
                })
            elseif section == SECTION_EDGES then
                local src, dest, value =
                    line:match('^(%d+)%s+(%d+)%s+(%-?%d+)$')
                if src == nil then
                    error('cannot parse edge line: %q', line)
                end
                src, dest, value = tonumber(src), tonumber(dest),
                                   tonumber(value)
                local dest_name = vertices[dest]
                if dest_name == nil then
                    error('edge %d -> %d names a vertex the file never ' ..
                          'declared', src, dest)
                end
                if vertices[src] == nil then
                    error('edge %d -> %d names a vertex the file never ' ..
                          'declared', src, src)
                end
                if src ~= current_id or #current_edges >= EDGE_BATCH_SIZE then
                    flush_edges()
                    current_id = src
                end
                table.insert(current_edges, {dest_name, value})
            else
                error('graph line outside any section: %q', line)
            end
        end

        -- Read in chunks and cut on newlines. The 1.6 version re-derived the
        -- remainder with buf:match("\n([^\n]*)$"), which is nil for a chunk
        -- holding no newline at all, and had no end-of-file test: a file whose
        -- last line had no trailing newline either dropped that line or span.
        local buf = ''
        while true do
            local chunk = f:read(READ_SIZE)
            if chunk == nil then
                f:close()
                syserror("cannot read graph file '%s'", file)
            end
            if chunk == '' then
                break
            end
            buf = buf .. chunk
            local from = 1
            while true do
                local nl = buf:find('\n', from, true)
                if nl == nil then
                    break
                end
                handle_line(buf:sub(from, nl - 1))
                from = nl + 1
            end
            buf = buf:sub(from)
        end
        -- Whatever is left had no trailing newline; it is still a line.
        handle_line(buf)
        f:close()

        flush_edges()
        log.info('processed %d lines', processed)
        return processed
    end
    return loader_new(instance, loader)
end

return strict.strictify({
    new           = loader_new,
    graph_edges_f = loader_graph_edges_file,
})
