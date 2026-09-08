#!/usr/bin/env tarantool
--- Convert a pregel text graph into the pair of Avro files loader.avro_files
--- reads.
--
--     tarantool tools/text2avro.lua <input.txt> <out_dir> [--codec CODEC]
--
-- The input is the two-section '-custom' format that pregel.loader's
-- graph_edges_f reads (the '-bi' variant is the same format, only with both
-- directions of every edge spelled out, so it needs nothing special here):
--
--     # List of vertices
--     <id> '<name>' <value>
--     # List of edges
--     <src_id> <dst_id> <weight>
--
-- The output is <out_dir>/vertices.avro and <out_dir>/edges.avro:
--
--     record Vertex { long id; string name; long value; }
--     record Edge   { string src; string dst; long weight; }
--
-- Edges name their endpoints by vertex *name* rather than by the file's own
-- numbering, because a name is what pregel routes and stores by -- the text
-- loader translates the ids through the vertex section for exactly the same
-- reason, and doing it here means the Avro loader never has to hold the
-- id-to-name map in memory.
--
-- CODEC is null (the default), deflate, or zstandard where the build has it.

local fio  = require('fio')
local json = require('json')

-- Run from anywhere: resolve this script's repository root and put it ahead of
-- whatever pregel may be installed system-wide.
local script_dir = fio.abspath(fio.dirname(arg[0]))
local root       = fio.dirname(script_dir)
package.path = string.format('%s/?.lua;%s/?/init.lua;%s', root, root,
                             package.path)

local avro = require('pregel.avro')

local PROGRESS_EVERY = 200000

local VERTEX_SCHEMA = {
    type = 'record', name = 'Vertex',
    fields = {
        {name = 'id',    type = 'long'  },
        {name = 'name',  type = 'string'},
        {name = 'value', type = 'long'  },
    },
}

local EDGE_SCHEMA = {
    type = 'record', name = 'Edge',
    fields = {
        {name = 'src',    type = 'string'},
        {name = 'dst',    type = 'string'},
        {name = 'weight', type = 'long'  },
    },
}

local function die(fmt, ...)
    io.stderr:write('text2avro: ' .. string.format(fmt, ...) .. '\n')
    os.exit(1)
end

local function usage()
    io.stderr:write(
        'usage: tarantool tools/text2avro.lua <input.txt> <out_dir> ' ..
        '[--codec null|deflate|zstandard]\n')
    os.exit(2)
end

local function parse_args(argv)
    local positional, codec = {}, 'null'
    local i = 1
    while argv[i] ~= nil do
        local a = argv[i]
        if a == '--help' or a == '-h' then
            usage()
        elseif a == '--codec' then
            codec = argv[i + 1] or die('--codec needs a value')
            i = i + 2
        elseif a:sub(1, 8) == '--codec=' then
            codec = a:sub(9)
            i = i + 1
        elseif a:sub(1, 1) == '-' then
            die('unknown option %q', a)
        else
            table.insert(positional, a)
            i = i + 1
        end
    end
    if #positional ~= 2 then
        usage()
    end
    if not avro.ocf.codec_available(codec) then
        die('codec %q is not available in this Tarantool build', codec)
    end
    return positional[1], positional[2], codec
end

local input, out_dir, codec = parse_args(arg)

if not fio.path.exists(input) then
    die("no such file: '%s'", input)
end
if not fio.mktree(out_dir) then
    die("cannot create '%s'", out_dir)
end

local vertices_path = fio.pathjoin(out_dir, 'vertices.avro')
local edges_path    = fio.pathjoin(out_dir, 'edges.avro')

local vw = avro.ocf.open(vertices_path, {
    mode = 'w', schema = VERTEX_SCHEMA, codec = codec,
})
local ew = avro.ocf.open(edges_path, {
    mode = 'w', schema = EDGE_SCHEMA, codec = codec,
})

local names     = {}
local section   = 0
local lineno    = 0
local n_vertex  = 0
local n_edge    = 0

local fh = assert(io.open(input, 'r'), 'cannot open ' .. input)
for line in fh:lines() do
    lineno = lineno + 1
    -- Tolerate CRLF and blank lines, exactly as the text loader does.
    line = line:gsub('\r$', '')
    if #line == 0 then -- luacheck: ignore 542
        -- nothing to do
    elseif line:sub(1, 1) == '#' then
        section = section + 1
    elseif section == 1 then
        local id, name, value = line:match("^(%d+)%s+'(.*)'%s+(%-?%d+)$")
        if id == nil then
            die('%s:%d: cannot parse vertex line: %s', input, lineno,
                json.encode(line))
        end
        id, value = tonumber(id), tonumber(value)
        names[id] = name
        vw:append{id = id, name = name, value = value}
        n_vertex = n_vertex + 1
    elseif section == 2 then
        local src, dst, weight = line:match('^(%d+)%s+(%d+)%s+(%-?%d+)$')
        if src == nil then
            die('%s:%d: cannot parse edge line: %s', input, lineno,
                json.encode(line))
        end
        src, dst, weight = tonumber(src), tonumber(dst), tonumber(weight)
        local src_name, dst_name = names[src], names[dst]
        if src_name == nil then
            die('%s:%d: edge %d -> %d names a vertex the file never declared',
                input, lineno, src, src)
        end
        if dst_name == nil then
            die('%s:%d: edge %d -> %d names a vertex the file never declared',
                input, lineno, src, dst)
        end
        ew:append{src = src_name, dst = dst_name, weight = weight}
        n_edge = n_edge + 1
    else
        die('%s:%d: line outside any section: %s', input, lineno,
            json.encode(line))
    end
    if lineno % PROGRESS_EVERY == 0 then
        io.stderr:write(string.format('  %d lines (%d vertices, %d edges)\n',
                                      lineno, n_vertex, n_edge))
    end
end
fh:close()

vw:close()
ew:close()

print(string.format('%s -> %s: %d vertices', input, vertices_path, n_vertex))
print(string.format('%s -> %s: %d edges', input, edges_path, n_edge))
print(string.format('codec: %s', codec))

os.exit(0)
