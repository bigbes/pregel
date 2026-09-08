--- Apache Avro schema parsing, validation, canonical form and fingerprints.
--
-- A schema given as a JSON string or as an already decoded Lua table is turned
-- into a normalised schema object. The object carries everything the binary
-- codec needs (kind, fullname, fields, items, values, types, symbols, size) and
-- knows how to render its Parsing Canonical Form and its CRC-64-AVRO
-- fingerprint.
--
-- Nothing here is Tarantool specific except `json` and `box.NULL`, which stands
-- in for JSON null wherever a Lua nil would vanish from a table.

local bit  = require('bit')
local json = require('json')

local M = {}

local NULL = box.NULL

M.NULL = NULL

local PRIMITIVE = {
    ['null']    = true,
    ['boolean'] = true,
    ['int']     = true,
    ['long']    = true,
    ['float']   = true,
    ['double']  = true,
    ['bytes']   = true,
    ['string']  = true,
}

local NAMED = {
    ['record'] = true,
    ['enum']   = true,
    ['fixed']  = true,
}

-- Attributes the parser interprets itself. Everything else on a type object is
-- kept verbatim in `props` so that logicalType, precision, scale and any other
-- application metadata survive a parse.
local KNOWN_TYPE_ATTR = {
    type = true, name = true, namespace = true, doc = true, aliases = true,
    fields = true, symbols = true, items = true, values = true, size = true,
    default = true,
}

local KNOWN_FIELD_ATTR = {
    name = true, type = true, doc = true, default = true, order = true, aliases = true,
}

local FIELD_ORDER = { ascending = true, descending = true, ignore = true }

M.PRIMITIVE = PRIMITIVE
M.NAMED     = NAMED

local function fail(fmt, ...)
    error('avro.schema: ' .. string.format(fmt, ...), 0)
end

M.error = fail

--------------------------------------------------------------------------------
-- Names
--------------------------------------------------------------------------------

local function is_simple_name(s)
    return type(s) == 'string' and s:match('^[A-Za-z_][A-Za-z0-9_]*$') ~= nil
end

--- A namespace is a dot separated sequence of simple names, or the empty string
--  (the null namespace).
local function is_namespace(s)
    if type(s) ~= 'string' then
        return false
    end
    if s == '' then
        return true
    end
    for part in (s .. '.'):gmatch('([^.]*)%.') do
        if not is_simple_name(part) then
            return false
        end
    end
    return true
end

local function split_fullname(fullname)
    local dot = fullname:match('^.*()%.')
    if dot == nil then
        return nil, fullname
    end
    return fullname:sub(1, dot - 1), fullname:sub(dot + 1)
end

M.split_fullname = split_fullname

--- Resolve the `name`/`namespace` pair of a named type against the namespace of
--  the enclosing definition, per the "Names" section of the specification.
local function resolve_name(name, namespace, enclosing)
    if not (type(name) == 'string' and name ~= '') then
        fail('named type has no "name"')
    end
    if name:find('%.') then
        -- A name containing dots is a fullname; the namespace attribute, if
        -- any, is ignored.
        local ns, short = split_fullname(name)
        if not is_simple_name(short) or not is_namespace(ns) then
            fail('%q is not a valid fullname', name)
        end
        return ns, short, name
    end
    if not is_simple_name(name) then
        fail('%q is not a valid name', name)
    end
    local ns
    if namespace ~= nil and namespace ~= NULL then
        if not is_namespace(namespace) then
            fail('%q is not a valid namespace', tostring(namespace))
        end
        -- An explicit empty namespace means the null namespace.
        ns = namespace ~= '' and namespace or nil
    else
        ns = enclosing
    end
    if ns == nil then
        return nil, name, name
    end
    return ns, name, ns .. '.' .. name
end

--------------------------------------------------------------------------------
-- Schema objects
--------------------------------------------------------------------------------

local schema_mt = {}
schema_mt.__index = schema_mt

function schema_mt:__tostring()
    return 'avro.schema<' .. (self.fullname or self.kind) .. '>'
end

local function new_schema(kind)
    return setmetatable({ kind = kind }, schema_mt)
end

function M.is_schema(v)
    return getmetatable(v) == schema_mt
end

--------------------------------------------------------------------------------
-- Parsing
--------------------------------------------------------------------------------

local parse_any

local function collect_props(spec, known)
    local props
    for k, v in pairs(spec) do
        if type(k) == 'string' and not known[k] then
            props = props or {}
            props[k] = v
        end
    end
    return props
end

--- Aliases of a *named type*, which are fullnames and so are resolved against
--  the enclosing namespace just like the type's own name.
local function parse_aliases(spec, enclosing_ns)
    local raw = spec.aliases
    if raw == nil or raw == NULL then
        return nil
    end
    if type(raw) ~= 'table' then
        fail('"aliases" must be an array of names')
    end
    local out = {}
    for i = 1, #raw do
        local alias = raw[i]
        if type(alias) ~= 'string' then
            fail('"aliases" must be an array of names')
        end
        local _, _, full = resolve_name(alias, nil, enclosing_ns)
        out[i] = full
    end
    return out
end

--- Aliases of a record *field*, which are plain field names: a field has no
--  namespace, so qualifying these the way type aliases are qualified would stop
--  them ever matching a writer's field.
local function parse_field_aliases(spec)
    local raw = spec.aliases
    if raw == nil or raw == NULL then
        return nil
    end
    if type(raw) ~= 'table' then
        fail('field "aliases" must be an array of names')
    end
    local out = {}
    for i = 1, #raw do
        if not is_simple_name(raw[i]) then
            fail('field "aliases" must be an array of names, got %q', tostring(raw[i]))
        end
        out[i] = raw[i]
    end
    return out
end

--- Look a named-type reference up, trying the enclosing namespace first and the
--  null namespace second (the same order the reference implementations use).
local function resolve_reference(ctx, name, enclosing_ns)
    if name:find('%.') == nil and enclosing_ns ~= nil then
        local qualified = enclosing_ns .. '.' .. name
        if ctx.names[qualified] then
            return ctx.names[qualified]
        end
    end
    return ctx.names[name]
end

local function register(ctx, sc)
    if ctx.names[sc.fullname] ~= nil then
        fail('name %q is already defined', sc.fullname)
    end
    if PRIMITIVE[sc.fullname] then
        fail('%q cannot be used as a named type: it is a primitive type name', sc.fullname)
    end
    ctx.names[sc.fullname] = sc
end

local function parse_record(ctx, spec, enclosing_ns)
    local sc = new_schema('record')
    sc.namespace, sc.name, sc.fullname = resolve_name(spec.name, spec.namespace, enclosing_ns)
    sc.is_error = spec.type == 'error'
    register(ctx, sc)

    if spec.doc ~= nil and spec.doc ~= NULL then
        sc.doc = spec.doc
    end
    sc.aliases = parse_aliases(spec, sc.namespace)
    sc.props = collect_props(spec, KNOWN_TYPE_ATTR)
    sc.logical_type = sc.props and sc.props.logicalType or nil

    local raw_fields = spec.fields
    if type(raw_fields) ~= 'table' then
        fail('record %q has no "fields" array', sc.fullname)
    end
    sc.fields = {}
    sc.field_map = {}
    for i = 1, #raw_fields do
        local rf = raw_fields[i]
        if type(rf) ~= 'table' then
            fail('record %q: field #%d is not an object', sc.fullname, i)
        end
        if not is_simple_name(rf.name) then
            fail('record %q: field #%d has an invalid name %q', sc.fullname, i, tostring(rf.name))
        end
        if sc.field_map[rf.name] ~= nil then
            fail('record %q: duplicate field %q', sc.fullname, rf.name)
        end
        if rf.type == nil then
            fail('record %q: field %q has no "type"', sc.fullname, rf.name)
        end
        local field = {
            name     = rf.name,
            index    = i,
            -- A field's type is resolved in the namespace of the record.
            type     = parse_any(ctx, rf.type, sc.namespace),
            aliases  = parse_field_aliases(rf),
            props    = collect_props(rf, KNOWN_FIELD_ATTR),
        }
        if rf.doc ~= nil and rf.doc ~= NULL then
            field.doc = rf.doc
        end
        if rf.order ~= nil and rf.order ~= NULL then
            if not FIELD_ORDER[rf.order] then
                fail('record %q: field %q has an invalid "order" %q',
                     sc.fullname, rf.name, tostring(rf.order))
            end
            field.order = rf.order
        end
        -- `default` is kept in its JSON shape; the Lua value is derived on
        -- demand so that a malformed default only bites the schemas that use it.
        local has_default = false
        for k in pairs(rf) do
            if k == 'default' then
                has_default = true
                break
            end
        end
        field.has_default = has_default
        if has_default then
            field.default_json = rf.default
        end
        sc.fields[i] = field
        sc.field_map[rf.name] = field
    end
    return sc
end

local function parse_enum(ctx, spec, enclosing_ns)
    local sc = new_schema('enum')
    sc.namespace, sc.name, sc.fullname = resolve_name(spec.name, spec.namespace, enclosing_ns)
    register(ctx, sc)

    if spec.doc ~= nil and spec.doc ~= NULL then
        sc.doc = spec.doc
    end
    sc.aliases = parse_aliases(spec, sc.namespace)
    sc.props = collect_props(spec, KNOWN_TYPE_ATTR)
    sc.logical_type = sc.props and sc.props.logicalType or nil

    local symbols = spec.symbols
    if type(symbols) ~= 'table' or #symbols == 0 then
        fail('enum %q has no "symbols" array', sc.fullname)
    end
    sc.symbols = {}
    sc.symbol_index = {}
    for i = 1, #symbols do
        local sym = symbols[i]
        if not is_simple_name(sym) then
            fail('enum %q: %q is not a valid symbol', sc.fullname, tostring(sym))
        end
        if sc.symbol_index[sym] ~= nil then
            fail('enum %q: duplicate symbol %q', sc.fullname, sym)
        end
        sc.symbols[i] = sym
        sc.symbol_index[sym] = i
    end
    if spec.default ~= nil and spec.default ~= NULL then
        if sc.symbol_index[spec.default] == nil then
            fail('enum %q: default %q is not one of its symbols', sc.fullname, tostring(spec.default))
        end
        sc.default = spec.default
    end
    return sc
end

local function parse_fixed(ctx, spec, enclosing_ns)
    local sc = new_schema('fixed')
    sc.namespace, sc.name, sc.fullname = resolve_name(spec.name, spec.namespace, enclosing_ns)
    register(ctx, sc)

    sc.aliases = parse_aliases(spec, sc.namespace)
    sc.props = collect_props(spec, KNOWN_TYPE_ATTR)
    sc.logical_type = sc.props and sc.props.logicalType or nil

    local size = spec.size
    if type(size) ~= 'number' or size < 0 or size % 1 ~= 0 then
        fail('fixed %q has an invalid "size" %s', sc.fullname, tostring(size))
    end
    sc.size = size
    return sc
end

local function parse_union(ctx, spec, enclosing_ns)
    local sc = new_schema('union')
    sc.types = {}
    sc.branch_index = {}
    -- Unions may not immediately contain other unions, and may contain at most
    -- one schema of each unnamed type.
    local seen = {}
    for i = 1, #spec do
        local branch = parse_any(ctx, spec[i], enclosing_ns)
        if branch.kind == 'union' then
            fail('a union may not immediately contain another union')
        end
        local key = branch.fullname or branch.kind
        if seen[key] then
            fail('union contains %q twice', key)
        end
        seen[key] = true
        sc.types[i] = branch
        sc.branch_index[key] = i
    end
    if #sc.types == 0 then
        fail('union must have at least one branch')
    end
    return sc
end

--- Parse one schema node. `spec` is a string (primitive name or reference), a
--  table array (union) or a table object (everything else).
parse_any = function(ctx, spec, enclosing_ns)
    if spec == nil or spec == NULL then
        fail('missing schema')
    end
    if M.is_schema(spec) then
        return spec
    end
    if type(spec) == 'string' then
        if PRIMITIVE[spec] then
            local cached = ctx.primitives[spec]
            if cached == nil then
                cached = new_schema(spec)
                ctx.primitives[spec] = cached
            end
            return cached
        end
        local found = resolve_reference(ctx, spec, enclosing_ns)
        if found == nil then
            fail('unknown type %q', spec)
        end
        return found
    end
    if type(spec) ~= 'table' then
        fail('schema must be a string, an array or an object, got %s', type(spec))
    end
    if spec[1] ~= nil or next(spec) == nil then
        -- A JSON array is a union. An empty table is an empty union, which is
        -- rejected by parse_union with a better message than "no type".
        return parse_union(ctx, spec, enclosing_ns)
    end

    local kind = spec.type
    if kind == nil then
        fail('schema object has no "type"')
    end
    if type(kind) == 'table' then
        -- {"type": {...}} -- a wrapper around a nested schema, as produced by
        -- some tools. The extra attributes are dropped, as they are in Java.
        return parse_any(ctx, kind, enclosing_ns)
    end
    if PRIMITIVE[kind] then
        -- {"type": "int", "logicalType": "date"} and friends: a primitive with
        -- attributes needs its own object so the attributes are not shared.
        local props = collect_props(spec, KNOWN_TYPE_ATTR)
        if props == nil then
            return parse_any(ctx, kind, enclosing_ns)
        end
        local sc = new_schema(kind)
        sc.props = props
        sc.logical_type = props.logicalType
        return sc
    end
    if kind == 'record' or kind == 'error' then
        return parse_record(ctx, spec, enclosing_ns)
    elseif kind == 'enum' then
        return parse_enum(ctx, spec, enclosing_ns)
    elseif kind == 'fixed' then
        return parse_fixed(ctx, spec, enclosing_ns)
    elseif kind == 'array' then
        local sc = new_schema('array')
        if spec.items == nil then
            fail('array has no "items"')
        end
        sc.items = parse_any(ctx, spec.items, enclosing_ns)
        sc.props = collect_props(spec, KNOWN_TYPE_ATTR)
        sc.logical_type = sc.props and sc.props.logicalType or nil
        return sc
    elseif kind == 'map' then
        local sc = new_schema('map')
        if spec.values == nil then
            fail('map has no "values"')
        end
        sc.values = parse_any(ctx, spec.values, enclosing_ns)
        sc.props = collect_props(spec, KNOWN_TYPE_ATTR)
        sc.logical_type = sc.props and sc.props.logicalType or nil
        return sc
    end
    -- A reference spelled as {"type": "some.Name"}.
    local found = resolve_reference(ctx, kind, enclosing_ns)
    if found == nil then
        fail('unknown type %q', tostring(kind))
    end
    return found
end

--- Parse a schema.
--
-- @param spec   JSON text, a decoded Lua table, or an already parsed schema.
-- @param opts   optional table; `names` seeds the named-type scope, which is
--               what a reader schema needs when it refers to types the writer
--               schema defined.
-- @return the schema object.
function M.parse(spec, opts)
    if M.is_schema(spec) then
        return spec
    end
    if type(spec) == 'string' and not PRIMITIVE[spec] then
        local ok, decoded = pcall(json.decode, spec)
        if not ok then
            fail('not valid JSON: %s', tostring(decoded))
        end
        spec = decoded
    end
    local ctx = { names = {}, primitives = {} }
    if opts ~= nil and opts.names ~= nil then
        for k, v in pairs(opts.names) do
            ctx.names[k] = v
        end
    end
    local sc = parse_any(ctx, spec, opts and opts.namespace or nil)
    sc.names = ctx.names
    return sc
end

--------------------------------------------------------------------------------
-- Parsing Canonical Form
--------------------------------------------------------------------------------

local ESCAPES = {
    ['"']    = '\\"',
    ['\\']   = '\\\\',
    ['\b']   = '\\b',
    ['\f']   = '\\f',
    ['\n']   = '\\n',
    ['\r']   = '\\r',
    ['\t']   = '\\t',
}

--- JSON string literal with UTF-8 kept as UTF-8, per the [STRINGS] rule of the
--  canonical form: escapes are resolved, only what JSON requires is escaped.
local function quote(s)
    return '"' .. s:gsub('[%z\1-\31"\\]', function(c)
        return ESCAPES[c] or string.format('\\u%04x', c:byte())
    end) .. '"'
end

M.quote = quote

local canonical_of

canonical_of = function(sc, seen, out)
    local kind = sc.kind
    if PRIMITIVE[kind] then
        out[#out + 1] = '"' .. kind .. '"'
    elseif kind == 'union' then
        out[#out + 1] = '['
        for i = 1, #sc.types do
            if i > 1 then
                out[#out + 1] = ','
            end
            canonical_of(sc.types[i], seen, out)
        end
        out[#out + 1] = ']'
    elseif kind == 'array' then
        out[#out + 1] = '{"type":"array","items":'
        canonical_of(sc.items, seen, out)
        out[#out + 1] = '}'
    elseif kind == 'map' then
        out[#out + 1] = '{"type":"map","values":'
        canonical_of(sc.values, seen, out)
        out[#out + 1] = '}'
    elseif seen[sc.fullname] then
        out[#out + 1] = quote(sc.fullname)
    else
        seen[sc.fullname] = true
        -- [ORDER]: name, type, fields, symbols, items, values, size.
        out[#out + 1] = '{"name":' .. quote(sc.fullname) .. ',"type":"' .. kind .. '"'
        if kind == 'record' then
            out[#out + 1] = ',"fields":['
            for i = 1, #sc.fields do
                if i > 1 then
                    out[#out + 1] = ','
                end
                out[#out + 1] = '{"name":' .. quote(sc.fields[i].name) .. ',"type":'
                canonical_of(sc.fields[i].type, seen, out)
                out[#out + 1] = '}'
            end
            out[#out + 1] = ']'
        elseif kind == 'enum' then
            out[#out + 1] = ',"symbols":['
            for i = 1, #sc.symbols do
                if i > 1 then
                    out[#out + 1] = ','
                end
                out[#out + 1] = quote(sc.symbols[i])
            end
            out[#out + 1] = ']'
        elseif kind == 'fixed' then
            out[#out + 1] = ',"size":' .. string.format('%d', sc.size)
        end
        out[#out + 1] = '}'
    end
    return out
end

--- Parsing Canonical Form of the schema, as a compact JSON string.
function schema_mt:canonical()
    if self._canonical == nil then
        self._canonical = table.concat(canonical_of(self, {}, {}))
    end
    return self._canonical
end

--------------------------------------------------------------------------------
-- Full JSON form
--------------------------------------------------------------------------------

local function put_props(out, props)
    if props == nil then
        return
    end
    -- Sorted so that the same schema always serialises to the same bytes.
    local keys = {}
    for k in pairs(props) do
        keys[#keys + 1] = k
    end
    table.sort(keys)
    for i = 1, #keys do
        out[#out + 1] = ',' .. quote(keys[i]) .. ':' .. json.encode(props[keys[i]])
    end
end

local function put_names(out, names)
    if names == nil then
        return
    end
    local parts = {}
    for i = 1, #names do
        parts[i] = quote(names[i])
    end
    out[#out + 1] = ',"aliases":[' .. table.concat(parts, ',') .. ']'
end

local render_default

--- Render a field default as JSON text, guided by the field's own schema.
--
-- json.encode alone cannot do it: an empty Lua table is both `[]` and `{}`, and
-- it comes out as `[]` whatever the field's type. A schema given as a Lua table
-- with `default = {}` on a map or record field therefore produced
-- `"default":[]`, which fastavro and Java both refuse when they parse the
-- container file's header. (A JSON-text schema escaped this only because
-- Tarantool's json.decode tags `{}` with a map metatable.)
--
-- Only the container kinds need the guidance; json.encode spells everything
-- else unambiguously. Map keys are sorted so that the same schema always
-- renders the same bytes.
render_default = function(out, sc, value)
    local kind = sc.kind
    if kind == 'union' then
        -- The specification says a union default is a value of the first branch.
        return render_default(out, sc.types[1], value)
    end
    if type(value) == 'table' then
        if kind == 'map' then
            local keys = {}
            for k in pairs(value) do
                keys[#keys + 1] = k
            end
            table.sort(keys)
            out[#out + 1] = '{'
            for i = 1, #keys do
                if i > 1 then
                    out[#out + 1] = ','
                end
                out[#out + 1] = quote(keys[i]) .. ':'
                render_default(out, sc.values, value[keys[i]])
            end
            out[#out + 1] = '}'
            return
        end
        if kind == 'record' then
            out[#out + 1] = '{'
            local written = 0
            for i = 1, #sc.fields do
                local f = sc.fields[i]
                local got = value[f.name]
                -- type() rather than ~= nil: box.NULL compares equal to nil.
                if type(got) ~= 'nil' then
                    written = written + 1
                    if written > 1 then
                        out[#out + 1] = ','
                    end
                    out[#out + 1] = quote(f.name) .. ':'
                    render_default(out, f.type, got)
                end
            end
            out[#out + 1] = '}'
            return
        end
        if kind == 'array' then
            out[#out + 1] = '['
            for i = 1, #value do
                if i > 1 then
                    out[#out + 1] = ','
                end
                render_default(out, sc.items, value[i])
            end
            out[#out + 1] = ']'
            return
        end
    end
    out[#out + 1] = json.encode(value)
end

local tojson_of

--- `enclosing` is the namespace a reader would apply to an unqualified name at
--  this point, which is what decides whether a null namespace has to be spelled
--  out below.
tojson_of = function(sc, seen, out, enclosing)
    local kind = sc.kind
    if PRIMITIVE[kind] then
        if sc.props == nil then
            out[#out + 1] = '"' .. kind .. '"'
        else
            out[#out + 1] = '{"type":"' .. kind .. '"'
            put_props(out, sc.props)
            out[#out + 1] = '}'
        end
        return out
    end
    if kind == 'union' then
        out[#out + 1] = '['
        for i = 1, #sc.types do
            if i > 1 then
                out[#out + 1] = ','
            end
            tojson_of(sc.types[i], seen, out, enclosing)
        end
        out[#out + 1] = ']'
        return out
    end
    if kind == 'array' or kind == 'map' then
        out[#out + 1] = '{"type":"' .. kind .. '","' ..
                        (kind == 'array' and 'items' or 'values') .. '":'
        tojson_of(kind == 'array' and sc.items or sc.values, seen, out, enclosing)
        put_props(out, sc.props)
        out[#out + 1] = '}'
        return out
    end
    if seen[sc.fullname] then
        out[#out + 1] = quote(sc.fullname)
        return out
    end
    seen[sc.fullname] = true
    -- The name is emitted as a fullname, which makes the namespace attribute
    -- redundant and the result independent of where the type is nested -- with
    -- one exception. A type in the null namespace has a fullname with no dot in
    -- it, so nested inside a namespaced type a re-parse would hand it the
    -- enclosing namespace and name a different type than the data was written
    -- with. Spell the null namespace out, as Java's Schema.toString does.
    out[#out + 1] = '{"type":"' .. kind .. '","name":' .. quote(sc.fullname)
    if sc.namespace == nil and enclosing ~= nil then
        out[#out + 1] = ',"namespace":""'
    end
    enclosing = sc.namespace
    if sc.doc ~= nil then
        out[#out + 1] = ',"doc":' .. quote(sc.doc)
    end
    put_names(out, sc.aliases)
    if kind == 'record' then
        out[#out + 1] = ',"fields":['
        for i = 1, #sc.fields do
            local f = sc.fields[i]
            if i > 1 then
                out[#out + 1] = ','
            end
            out[#out + 1] = '{"name":' .. quote(f.name) .. ',"type":'
            tojson_of(f.type, seen, out, enclosing)
            if f.doc ~= nil then
                out[#out + 1] = ',"doc":' .. quote(f.doc)
            end
            if f.has_default then
                out[#out + 1] = ',"default":'
                render_default(out, f.type, f.default_json)
            end
            if f.order ~= nil then
                out[#out + 1] = ',"order":' .. quote(f.order)
            end
            put_names(out, f.aliases)
            put_props(out, f.props)
            out[#out + 1] = '}'
        end
        out[#out + 1] = ']'
    elseif kind == 'enum' then
        local parts = {}
        for i = 1, #sc.symbols do
            parts[i] = quote(sc.symbols[i])
        end
        out[#out + 1] = ',"symbols":[' .. table.concat(parts, ',') .. ']'
        if sc.default ~= nil then
            out[#out + 1] = ',"default":' .. quote(sc.default)
        end
    elseif kind == 'fixed' then
        out[#out + 1] = ',"size":' .. string.format('%d', sc.size)
    end
    put_props(out, sc.props)
    out[#out + 1] = '}'
    return out
end

--- The schema as JSON, keeping everything a parse would keep: docs, aliases,
--  defaults, field order and any extra attributes such as logicalType. This is
--  what goes into an object container file's `avro.schema` metadata, where the
--  canonical form would be wrong -- it strips the defaults a reader needs.
function schema_mt:tojson()
    if self._json == nil then
        self._json = table.concat(tojson_of(self, {}, {}))
    end
    return self._json
end

--------------------------------------------------------------------------------
-- CRC-64-AVRO fingerprint
--------------------------------------------------------------------------------

local FP_EMPTY = 0xc15d213aa4d7a795ULL
local fp_table

local function build_fp_table()
    local t = {}
    for i = 0, 255 do
        local fp = i * 1ULL
        for _ = 1, 8 do
            local carry = tonumber(bit.band(fp, 1)) == 1
            fp = bit.bxor(bit.rshift(fp, 1), carry and FP_EMPTY or 0ULL)
        end
        t[i] = fp
    end
    return t
end

--- CRC-64-AVRO of an arbitrary byte string, as an int64 cdata.
function M.crc64(s)
    fp_table = fp_table or build_fp_table()
    local fp = FP_EMPTY
    for i = 1, #s do
        local idx = tonumber(bit.band(bit.bxor(fp, s:byte(i)), 0xff))
        fp = bit.bxor(bit.rshift(fp, 8), fp_table[idx])
    end
    return fp
end

--- CRC-64-AVRO fingerprint of the Parsing Canonical Form, as an int64 cdata.
function schema_mt:fingerprint()
    if self._fingerprint == nil then
        self._fingerprint = M.crc64(self:canonical())
    end
    return self._fingerprint
end

--- The same fingerprint as 16 hex digits in little-endian byte order, which is
--  the order the Java and fastavro implementations print and the order the
--  single-object encoding puts on the wire.
function schema_mt:fingerprint_hex()
    local fp = self:fingerprint()
    local out = {}
    for i = 0, 7 do
        out[i + 1] = string.format('%02x', tonumber(bit.band(bit.rshift(fp, i * 8), 0xff)))
    end
    return table.concat(out)
end

--------------------------------------------------------------------------------
-- Defaults
--------------------------------------------------------------------------------

--- Decode a JSON string in which every character stands for one byte (that is
--  how the specification spells bytes and fixed defaults).
local function json_string_to_bytes(s)
    if not s:find('[\128-\255]') then
        return s
    end
    local out, i, n = {}, 1, #s
    while i <= n do
        local c = s:byte(i)
        local cp, len
        if c < 0x80 then
            cp, len = c, 1
        elseif c >= 0xc0 and c < 0xe0 then
            cp, len = bit.band(c, 0x1f), 2
        elseif c >= 0xe0 and c < 0xf0 then
            cp, len = bit.band(c, 0x0f), 3
        else
            cp, len = bit.band(c, 0x07), 4
        end
        for k = 1, len - 1 do
            cp = bit.bor(bit.lshift(cp, 6), bit.band(s:byte(i + k) or 0, 0x3f))
        end
        if cp > 0xff then
            fail('a bytes/fixed default may only contain code points below 256, got %d', cp)
        end
        out[#out + 1] = string.char(cp)
        i = i + len
    end
    return table.concat(out)
end

M.json_string_to_bytes = json_string_to_bytes

local default_to_lua

--- Turn a JSON default (as it appears in the schema) into the Lua value the
--  codec expects for `sc`.
default_to_lua = function(sc, value)
    local kind = sc.kind
    if kind == 'null' then
        if value ~= nil and value ~= NULL then
            fail('default for null must be null')
        end
        return NULL
    elseif kind == 'boolean' then
        if type(value) ~= 'boolean' then
            fail('default for boolean must be true or false')
        end
        return value
    elseif kind == 'int' or kind == 'long' or kind == 'float' or kind == 'double' then
        if type(value) ~= 'number' and type(value) ~= 'cdata' then
            fail('default for %s must be a number', kind)
        end
        return value
    elseif kind == 'string' then
        if type(value) ~= 'string' then
            fail('default for string must be a string')
        end
        return value
    elseif kind == 'bytes' or kind == 'fixed' then
        if type(value) ~= 'string' then
            fail('default for %s must be a string', kind)
        end
        return json_string_to_bytes(value)
    elseif kind == 'enum' then
        if sc.symbol_index[value] == nil then
            fail('default %q is not a symbol of enum %q', tostring(value), sc.fullname)
        end
        return value
    elseif kind == 'array' then
        if type(value) ~= 'table' then
            fail('default for array must be an array')
        end
        local out = {}
        for i = 1, #value do
            out[i] = default_to_lua(sc.items, value[i])
        end
        return out
    elseif kind == 'map' then
        if type(value) ~= 'table' then
            fail('default for map must be an object')
        end
        local out = {}
        for k, v in pairs(value) do
            out[k] = default_to_lua(sc.values, v)
        end
        return out
    elseif kind == 'union' then
        -- A union default is a value of its first branch.
        return default_to_lua(sc.types[1], value)
    elseif kind == 'record' then
        if type(value) ~= 'table' then
            fail('default for record %q must be an object', sc.fullname)
        end
        local out = {}
        for i = 1, #sc.fields do
            local f = sc.fields[i]
            local got = value[f.name]
            -- type() rather than ~= nil: box.NULL compares equal to nil, so an
            -- explicit null default would otherwise read as an absent one.
            if type(got) ~= 'nil' then
                out[f.name] = default_to_lua(f.type, got)
            elseif f.has_default then
                out[f.name] = default_to_lua(f.type, f.default_json)
            else
                fail('default for record %q has no value for field %q', sc.fullname, f.name)
            end
        end
        return out
    end
    fail('cannot build a default for kind %q', tostring(kind))
end

M.default_to_lua = default_to_lua

--- The Lua value of a record field's default, computed once and memoised.
function M.field_default(field)
    if not field.has_default then
        return nil, false
    end
    if field._default == nil then
        field._default = { default_to_lua(field.type, field.default_json) }
    end
    return field._default[1], true
end

return M
