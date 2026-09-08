--- Avro schema resolution: reading data written with one schema through
--- another.
--
--     local value = avro.decode(writer_schema, data, 1, reader_schema)
--
-- The rules are the "Schema Resolution" chapter of the specification:
--
--   * numeric promotions int -> long -> float -> double, and string <-> bytes;
--   * a record's fields are matched by name, or by an alias the *reader*
--     declares for the writer's name; a writer field the reader does not want
--     is skipped, and a reader field the writer never wrote is filled from the
--     reader's default (an error when it declares none);
--   * an enum symbol the reader does not know falls back to the reader's
--     `default` symbol, and is an error without one;
--   * when the writer wrote a union, its branch index is read and that branch
--     is resolved against the reader's schema; when only the *reader* is a
--     union, the first branch matching the writer's schema is used;
--   * named types match on fullname, or on a reader alias.
--
-- A resolver is compiled once per (writer, reader) pair and cached, so the
-- per-record cost is the decode itself.
--
-- @module pregel.avro.resolve

local avro_schema = require('pregel.avro.schema')
local codec       = require('pregel.avro.codec')

local M = {}

local NULL = box.NULL

local PRIMITIVE = avro_schema.PRIMITIVE
local NAMED     = avro_schema.NAMED

local function fail(fmt, ...)
    error('avro.resolve: ' .. string.format(fmt, ...), 0)
end

-- Widening conversions the specification allows.
local PROMOTIONS = {
    ['int']    = {['long'] = true, ['float'] = true, ['double'] = true},
    ['long']   = {['float'] = true, ['double'] = true},
    ['float']  = {['double'] = true},
    ['string'] = {['bytes'] = true},
    ['bytes']  = {['string'] = true},
}

--------------------------------------------------------------------------------
-- Matching
--------------------------------------------------------------------------------

local function has_alias(sc_or_field, name)
    local aliases = sc_or_field.aliases
    if aliases == nil then
        return false
    end
    for i = 1, #aliases do
        if aliases[i] == name then
            return true
        end
    end
    return false
end

--- Named types match on the *unqualified* name, which is what the
--  specification's Schema Resolution section asks for: "both schemas are
--  records with the same (unqualified) name", and likewise for enum and fixed.
--
-- The fullname is tried first so that a reader alias -- always a fullname --
-- keeps working, and so an exact match never costs the extra comparison. When
-- two branches of a reader union share a short name the first one still wins,
-- which is both the spec's rule ("the first schema in the reader's union that
-- matches") and what fastavro 1.12.2 does.
local function names_match(w, r)
    return w.fullname == r.fullname or has_alias(r, w.fullname) or w.name == r.name
end

--- A shallow compatibility test, used to choose a branch when only the reader
--  is a union. It looks at the type and, for named types, the name -- which is
--  what discriminates the branches of a legal union.
--
-- Shallow on purpose: it does not descend into fields or items, so a `true`
-- here promises a branch worth compiling, not one that will compile. The real
-- check is build().
--
-- @param w the writer schema
-- @param r a candidate reader schema
-- @return boolean
-- @function compatible
local function compatible(w, r)
    if w.kind == r.kind then
        if NAMED[w.kind] then
            return names_match(w, r)
        end
        return true
    end
    local promo = PROMOTIONS[w.kind]
    return promo ~= nil and promo[r.kind] == true
end

M.compatible = compatible

--------------------------------------------------------------------------------
-- Defaults
--------------------------------------------------------------------------------

--- Copy a value recursively, so a schema's default can be handed out as data.
--
-- Metatables are not copied and cycles are not detected; the values this sees
-- are field defaults, which the parser built as plain trees.
--
-- @param v any value
-- @return a copy sharing no table with `v`
-- @function deep_copy
local function deep_copy(v)
    if type(v) ~= 'table' then
        return v
    end
    local out = {}
    for k, item in pairs(v) do
        out[k] = deep_copy(item)
    end
    return out
end

--------------------------------------------------------------------------------
-- Resolver construction
--------------------------------------------------------------------------------

local build

--- Find the reader field that corresponds to a writer field: same name, or an
--  alias the reader declares for the writer's name.
local function reader_field_for(r, writer_field)
    local direct = r.field_map[writer_field.name]
    if direct ~= nil then
        return direct
    end
    for i = 1, #r.fields do
        if has_alias(r.fields[i], writer_field.name) then
            return r.fields[i]
        end
    end
    return nil
end

local function build_record(w, r, cache)
    if not names_match(w, r) then
        fail('record %s cannot be read as %s: the names do not match',
             w.fullname, r.fullname)
    end
    -- One step per writer field, in the order the writer wrote them.
    local steps = {}
    local matched = {}
    for i = 1, #w.fields do
        local wf = w.fields[i]
        local rf = reader_field_for(r, wf)
        if rf == nil then
            steps[i] = {skip = wf.type}
        else
            if matched[rf.name] then
                fail('record %s: reader field %q matches more than one writer field',
                     r.fullname, rf.name)
            end
            matched[rf.name] = true
            steps[i] = {name = rf.name, read = build(wf.type, rf.type, cache)}
        end
    end
    -- Reader fields the writer never wrote come from the reader's defaults.
    local fills = {}
    for i = 1, #r.fields do
        local rf = r.fields[i]
        if not matched[rf.name] then
            local value, present = avro_schema.field_default(rf)
            if not present then
                fail('record %s: the reader wants field %q, the writer does not ' ..
                     'have it and the reader declares no default',
                     r.fullname, rf.name)
            end
            fills[#fills + 1] = {name = rf.name, value = value}
        end
    end

    return function(data, pos)
        local out = {}
        for i = 1, #steps do
            local step = steps[i]
            if step.read ~= nil then
                out[step.name], pos = step.read(data, pos)
            else
                pos = codec.skip_value(step.skip, data, pos)
            end
        end
        for i = 1, #fills do
            local fill = fills[i]
            -- A table default is shared with the schema, so hand out a copy.
            out[fill.name] = deep_copy(fill.value)
        end
        return out, pos
    end
end

local function build_enum(w, r)
    if not names_match(w, r) then
        fail('enum %s cannot be read as %s: the names do not match',
             w.fullname, r.fullname)
    end
    -- A symbol the reader does not know falls back to its declared default,
    -- and is an error without one -- but only when the data actually carries
    -- it, so the check lives in the decoder rather than here.
    local fallback = r.default
    local wname = w.fullname
    return function(data, pos)
        local value, next_pos = codec.decode_value(w, data, pos)
        local sym = r.symbol_index[value] ~= nil and value or fallback
        if sym == nil then
            fail('enum %s: the reader has no symbol %q and declares no default',
                 wname, tostring(value))
        end
        return sym, next_pos
    end
end

local function build_promotion(w, r)
    local from, to = w.kind, r.kind
    if (from == 'string' and to == 'bytes') or (from == 'bytes' and to == 'string') then
        -- Both are Lua strings; nothing to convert.
        return function(data, pos) return codec.decode_value(w, data, pos) end
    end
    if to == 'float' or to == 'double' then
        -- A long past 2^53 arrives as int64 cdata; the reader asked for a
        -- floating point number, so give it one.
        return function(data, pos)
            local value, next_pos = codec.decode_value(w, data, pos)
            return tonumber(value), next_pos
        end
    end
    -- int -> long needs no conversion: both are Lua numbers here.
    return function(data, pos) return codec.decode_value(w, data, pos) end
end

--- Build the resolver for one (writer, reader) pair.
local function compile(w, r, cache)
    -- Only the reader is a union: take the first branch that matches.
    if r.kind == 'union' and w.kind ~= 'union' then
        for i = 1, #r.types do
            if compatible(w, r.types[i]) then
                return build(w, r.types[i], cache)
            end
        end
        fail('no branch of the reader union accepts a writer schema of type %s',
             w.fullname or w.kind)
    end

    -- The writer wrote a union: read the branch index, then resolve that
    -- branch. A branch the reader cannot accept is only an error if it turns
    -- up in the data, so the failure is deferred into the branch itself.
    if w.kind == 'union' then
        local branches = {}
        for i = 1, #w.types do
            local ok, fn = pcall(build, w.types[i], r, cache)
            if ok then
                branches[i] = fn
            else
                local err = fn
                branches[i] = function()
                    error(err, 0)
                end
            end
        end
        local count = #w.types
        local int_schema = avro_schema.parse('int')
        return function(data, pos)
            local idx, next_pos = codec.decode_value(int_schema, data, pos)
            local branch = branches[idx + 1]
            if branch == nil then
                fail('union branch index %d is out of range (%d branches)', idx, count)
            end
            return branch(data, next_pos)
        end
    end

    if w.kind == r.kind then
        if PRIMITIVE[w.kind] then
            return function(data, pos) return codec.decode_value(w, data, pos) end
        elseif w.kind == 'array' then
            local items = build(w.items, r.items, cache)
            return function(data, pos)
                local out, n = {}, 0
                while true do
                    local count
                    count, pos = codec.get_long(data, pos)
                    if count == 0 then
                        return out, pos
                    end
                    if count < 0 then
                        count = -count
                        local _
                        _, pos = codec.get_long(data, pos)
                    end
                    for _ = 1, count do
                        n = n + 1
                        out[n], pos = items(data, pos)
                    end
                end
            end
        elseif w.kind == 'map' then
            local values = build(w.values, r.values, cache)
            local string_schema = avro_schema.parse('string')
            return function(data, pos)
                local out = {}
                while true do
                    local count
                    count, pos = codec.get_long(data, pos)
                    if count == 0 then
                        return out, pos
                    end
                    if count < 0 then
                        count = -count
                        local _
                        _, pos = codec.get_long(data, pos)
                    end
                    for _ = 1, count do
                        local key
                        key, pos = codec.decode_value(string_schema, data, pos)
                        out[key], pos = values(data, pos)
                    end
                end
            end
        elseif w.kind == 'record' then
            return build_record(w, r, cache)
        elseif w.kind == 'enum' then
            return build_enum(w, r)
        elseif w.kind == 'fixed' then
            if not names_match(w, r) then
                fail('fixed %s cannot be read as %s: the names do not match',
                     w.fullname, r.fullname)
            end
            if w.size ~= r.size then
                fail('fixed %s is %d bytes for the writer and %d for the reader',
                     w.fullname, w.size, r.size)
            end
            return function(data, pos) return codec.decode_value(w, data, pos) end
        end
        fail('cannot resolve a schema of kind %q', tostring(w.kind))
    end

    local promo = PROMOTIONS[w.kind]
    if promo ~= nil and promo[r.kind] then
        return build_promotion(w, r)
    end

    fail('a %s cannot be read as a %s', w.fullname or w.kind, r.fullname or r.kind)
end

--- Compile with memoisation, so that recursive schemas terminate: the stub is
--  published before the real function exists, and forwards to it once it does.
build = function(w, r, cache)
    local by_writer = cache[w]
    if by_writer == nil then
        by_writer = {}
        cache[w] = by_writer
    end
    local existing = by_writer[r]
    if existing ~= nil then
        return existing
    end
    local real
    local stub = function(data, pos)
        return real(data, pos)
    end
    by_writer[r] = stub
    local ok, result = pcall(compile, w, r, cache)
    if not ok then
        by_writer[r] = nil
        error(result, 0)
    end
    real = result
    return stub
end

--------------------------------------------------------------------------------
-- Entry points
--------------------------------------------------------------------------------

-- Compiled resolvers, keyed by the writer then the reader schema object. Weak
-- on both, so a schema that goes out of scope takes its resolvers with it.
local CACHE = setmetatable({}, {__mode = 'k'})

--- A reusable decoder for one (writer, reader) pair.
--  Returns function(data, pos) -> value, next_pos.
--
-- The returned decoder does *not* narrow a top-level null to nil -- that is
-- M.decode's job -- so an OCF reader can iterate a file of nulls. `pos`
-- defaults to 1.
--
-- Resolvers are cached, weakly on both schema objects, so calling this per
-- record is cheap as long as the same schema objects are passed each time; two
-- separately parsed copies of the same schema text are two cache entries.
--
-- @param writer the schema the data was written with, parsed or not
-- @param reader the schema to read it as
-- @return function(data, pos) -> value, next_pos
-- @raise when the two schemas cannot be resolved against each other; a writer
--        union branch the reader rejects is deferred, and only raises if the
--        data actually carries it
-- @function resolver
function M.resolver(writer, reader)
    local w = avro_schema.parse(writer)
    local r = avro_schema.parse(reader)
    local by_writer = CACHE[w]
    if by_writer == nil then
        by_writer = setmetatable({}, {__mode = 'k'})
        CACHE[w] = by_writer
    end
    local fn = by_writer[r]
    if fn == nil then
        fn = build(w, r, {})
        by_writer[r] = fn
    end
    return function(data, pos)
        return fn(data, pos or 1)
    end
end

--- Decode one value written with `writer` as if it had been written with
--  `reader`.
--
-- A one-shot wrapper over resolver(); when many records share a pair of
-- schemas, build the resolver once instead.
--
-- @param writer the schema the data was written with
-- @param data the buffer
-- @param pos 1-based offset, default 1
-- @param reader the schema to read it as
-- @return the value, nil for a null at the top level, and the position just
--         past it
-- @raise when the schemas do not resolve, or the input is truncated or
--        malformed
-- @function decode
function M.decode(writer, data, pos, reader)
    local value, next_pos = M.resolver(writer, reader)(data, pos)
    if value == NULL then
        return nil, next_pos
    end
    return value, next_pos
end

M.deep_copy = deep_copy

return M
