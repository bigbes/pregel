--- Build small graph files in the two-section text format.
--
-- The integration and loader tests want a graph of a few dozen vertices, not
-- the 75k-vertex fixture. Slicing the real one keeps the vertex lines exactly
-- as they are on disk -- names with spaces, apostrophes and all -- so the
-- parser is still tested against the format it has to read, rather than
-- against a file written by the same hand as the parser.

local fio = require('fio')

local helper = {}

local FIXTURE = fio.pathjoin('test', 'fixtures', 'graphs',
                             'soc-Epinions-custom.txt')

--- Read the first `count` vertex lines of the real fixture.
--
-- Returns an array of {id = number, name = string, value = number, line =
-- string}.
function helper.read_vertices(count, path)
    path = path or FIXTURE
    local f = assert(io.open(path, 'r'), 'cannot open ' .. path)
    local rv = {}
    local in_section = false
    for line in f:lines() do
        if line:sub(1, 1) == '#' then
            if in_section then
                break
            end
            in_section = true
        elseif in_section then
            local id, name, value = line:match("^(%d+)%s+'(.*)'%s+(%-?%d+)$")
            assert(id ~= nil, 'unparsable fixture line: ' .. line)
            table.insert(rv, {
                id = tonumber(id),
                name = name,
                value = tonumber(value),
                line = line,
            })
            if #rv == count then
                break
            end
        end
    end
    f:close()
    assert(#rv == count, 'fixture is shorter than ' .. tostring(count))
    return rv
end

--- Write a graph file from `vertices` (as returned above) and `edges`
-- ({src_id, dest_id, value}).
--
-- opts.trailing_newline -- default true; false leaves the last line unfinished
-- opts.blank_lines      -- insert an empty line between records
function helper.write(path, vertices, edges, opts)
    opts = opts or {}
    local trailing = opts.trailing_newline
    if trailing == nil then trailing = true end

    local out = {'# List of vertices'}
    for _, v in ipairs(vertices) do
        table.insert(out, v.line)
        if opts.blank_lines then table.insert(out, '') end
    end
    table.insert(out, '# List of edges')
    for _, e in ipairs(edges) do
        table.insert(out, string.format('%d %d %d', e[1], e[2], e[3]))
        if opts.blank_lines then table.insert(out, '') end
    end

    fio.mktree(fio.dirname(path))
    local f = assert(io.open(path, 'w'), 'cannot write ' .. path)
    f:write(table.concat(out, '\n'))
    if trailing then f:write('\n') end
    f:close()
    return path
end

--- A ring over `count` vertices, plus one chord, taken from the real fixture.
--
-- Returns the path, the vertex records and the edge list.
function helper.ring(path, count)
    local vertices = helper.read_vertices(count)
    local edges = {}
    for i = 1, count do
        local src = vertices[i].id
        local dest = vertices[(i % count) + 1].id
        table.insert(edges, {src, dest, 1})
    end
    -- One chord, so the graph is not a bare cycle.
    table.insert(edges, {vertices[1].id, vertices[math.floor(count / 2)].id, 2})
    table.sort(edges, function(a, b)
        if a[1] ~= b[1] then return a[1] < b[1] end
        return a[2] < b[2]
    end)
    helper.write(path, vertices, edges)
    return path, vertices, edges
end

return helper
