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
local json = require('json')

local avro_ocf = require('pregel.avro.ocf')

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

-------------------------------------------------------------------------------
-- Avro object container files
-------------------------------------------------------------------------------

--- Turn one mapping option into a getter over a record.
--
-- A function is used as it is. A string is a field of the file's own schema,
-- and it is checked against that schema here rather than coming back as a nil
-- value halfway through a load -- a misspelled field would otherwise store a
-- whole graph of nameless vertices before anything complained.
local function record_getter(spec, sc, what)
    if is_callable(spec) then
        return spec
    end
    if type(spec) ~= 'string' then
        error('%s must be a field name or a function, got %s', what,
              type(spec))
    end
    if sc.kind ~= 'record' then
        error('%s names the field %q, but the file holds %s records, not a ' ..
              'record type', what, spec, sc.kind)
    end
    if sc.field_map[spec] == nil then
        local known = {}
        for _, field in ipairs(sc.fields) do
            table.insert(known, field.name)
        end
        error('%s names the field %q, which %s does not have (fields: %s)',
              what, spec, sc.fullname, table.concat(known, ', '))
    end
    return function(record)
        return record[spec]
    end
end

--- The schema of an OCF file, with a readable error when the file is not there.
local function schema_of_file(path, what)
    if type(path) ~= 'string' then
        error('options.%s must be a path, got %s', what, type(path))
    end
    if not fio.path.exists(path) then
        error("options.%s: no such file: '%s'", what, path)
    end
    local ok, sc = pcall(avro_ocf.schema_of, path)
    if not ok then
        error("options.%s: cannot read '%s': %s", what, path, tostring(sc))
    end
    return sc
end

--- Load a graph from a pair of Avro object container files.
--
-- options.vertices     -- path to the vertex file (required)
-- options.edges        -- path to the edge file (required)
-- options.vertex_name  -- field name, or function(record) -> string; the name
--                         pregel knows the vertex by (required)
-- options.vertex_value -- field name, or function(record); what gets stored as
--                         the vertex value (default: the whole record)
-- options.edge_src     -- field name, or function(record) -> string; the name
--                         of the source vertex (required)
-- options.edge_dst     -- field name, or function(record) -> string (required)
-- options.edge_value   -- field name, or function(record) (default json.NULL)
-- options.batch        -- edges of one source per 'edge.store' (default 1000)
--
-- Both files are streamed record by record, so a graph larger than memory is
-- only as large as one edge batch here.
--
-- Edges are batched per source and the batch is sent when the source changes
-- or the batch fills, so an edge file grouped by source produces full batches;
-- correctness does not depend on the grouping, only the batch sizes do.
--
-- Called as `loader()` the whole graph is loaded. Called as
-- `loader(worker_idx, workers_count)` -- which is what worker:preload() does,
-- handing over mpool.self_idx and mpool.bucket_cnt -- only the share belonging
-- to that worker is: vertices whose name shards to it, and edges whose *source*
-- shards to it. The split uses the pool's own mpool:id(name), the same function
-- that routes every message, so N workers reading the same two files cover the
-- graph exactly once between them with nothing to coordinate. workers_count is
-- accepted for the calling convention's sake and deliberately not used for the
-- decision: the pool is the one authority on how many buckets there are.
--
-- An edge lands on the worker owning its source, which is the worker that
-- stored that source's vertex, so a partitioned load never sends an edge to an
-- instance that has not seen the vertex it belongs to.
local function loader_avro_files(instance, options)
    if type(options) ~= 'table' then
        error('loader.avro_files: options must be a table, got %s',
              type(options))
    end

    local vertices_path = options.vertices
    local edges_path    = options.edges

    local vertex_schema = schema_of_file(vertices_path, 'vertices')
    local edge_schema   = schema_of_file(edges_path, 'edges')

    if options.vertex_name == nil then
        error('loader.avro_files: options.vertex_name is required')
    end
    local vertex_name = record_getter(options.vertex_name, vertex_schema,
                                      'options.vertex_name')
    -- The default keeps the whole record, because the worker names a stored
    -- vertex by calling the instance's obtain_name on it: a value stripped down
    -- to one field would arrive somewhere it cannot be named.
    local vertex_value = function(record) return record end
    if options.vertex_value ~= nil then
        vertex_value = record_getter(options.vertex_value, vertex_schema,
                                     'options.vertex_value')
    end

    if options.edge_src == nil then
        error('loader.avro_files: options.edge_src is required')
    end
    if options.edge_dst == nil then
        error('loader.avro_files: options.edge_dst is required')
    end
    local edge_src = record_getter(options.edge_src, edge_schema,
                                   'options.edge_src')
    local edge_dst = record_getter(options.edge_dst, edge_schema,
                                   'options.edge_dst')
    local edge_value = function() return json.NULL end
    if options.edge_value ~= nil then
        edge_value = record_getter(options.edge_value, edge_schema,
                                   'options.edge_value')
    end

    local batch_size = options.batch or EDGE_BATCH_SIZE
    if type(batch_size) ~= 'number' or batch_size < 1 then
        error('loader.avro_files: options.batch must be a positive number, ' ..
              'got %s', tostring(options.batch))
    end

    --- Walk an OCF file, closing it whether or not the walk raised.
    local function each_record(path, handler)
        local reader = avro_ocf.open(path, {mode = 'r'})
        local ok, err = pcall(function()
            for record in reader:records() do
                handler(record)
            end
        end)
        reader:close()
        if not ok then
            error(0, tostring(err))
        end
    end

    local function loader(self, worker_idx, workers_count)
        log.info('loading graph from "%s" and "%s"%s', vertices_path,
                 edges_path,
                 worker_idx ~= nil
                     and string.format(' (worker %s of %s)',
                                       tostring(worker_idx),
                                       tostring(workers_count))
                     or '')

        --- True when this instance is the one that should store `name`.
        local function owns(name)
            if worker_idx == nil then
                return true
            end
            return instance.mpool:id(name) == worker_idx
        end

        local stored_vertices = 0
        each_record(vertices_path, function(record)
            local name = vertex_name(record)
            if name == nil then
                error('vertex record has no name: %s', json.encode(record))
            end
            if owns(name) then
                self:store_vertex(vertex_value(record))
                stored_vertices = stored_vertices + 1
            end
        end)
        log.info('stored %d vertices', stored_vertices)

        local stored_edges = 0
        local current_src   = nil
        local current_edges = {}

        local function flush_edges()
            -- Store under the source the batch was accumulated for, not under
            -- whatever record triggered the flush.
            if current_src ~= nil and #current_edges > 0 then
                self:store_edges_batch(current_src, current_edges)
            end
            current_edges = {}
        end

        each_record(edges_path, function(record)
            local src = edge_src(record)
            if src == nil then
                error('edge record has no source: %s', json.encode(record))
            end
            if not owns(src) then
                return
            end
            local dst = edge_dst(record)
            if dst == nil then
                error('edge record has no destination: %s',
                      json.encode(record))
            end
            if src ~= current_src or #current_edges >= batch_size then
                flush_edges()
                current_src = src
            end
            table.insert(current_edges, {dst, edge_value(record)})
            stored_edges = stored_edges + 1
        end)
        -- The last source's batch is only ever ended by the end of the file.
        flush_edges()
        log.info('stored %d edges', stored_edges)

        self:flush()
        return stored_vertices + stored_edges
    end

    return loader_new(instance, loader)
end

return strict.strictify({
    new           = loader_new,
    graph_edges_f = loader_graph_edges_file,
    avro_files    = loader_avro_files,
})
