--- Apache Avro binary encoding and decoding.
--
-- encode(schema, value) -> string
-- decode(schema, data [, pos]) -> value, next_pos
--
-- Lua value mapping:
--   null            nil, or box.NULL where a nil would vanish from a table
--   boolean         boolean
--   int             number
--   long            number while it fits in a double exactly, int64 cdata beyond
--   float, double   number
--   bytes, string   string (bytes are not validated as UTF-8)
--   fixed           string of exactly `size` bytes
--   enum            the symbol string
--   array           sequential table
--   map             table with string keys
--   record          table keyed by field name
--   union           the branch value itself, or {[branch_name] = value}
--
-- Decoding gives box.NULL for a null nested in a record, array or map, so that
-- the key stays present; a null decoded at the top level comes back as nil.

local bit = require('bit')
local ffi = require('ffi')

local avro_schema = require('pregel.avro.schema')

local M = {}

local NULL = box.NULL

M.NULL = NULL

local ct_i64   = ffi.typeof('int64_t')
local ct_u64   = ffi.typeof('uint64_t')
local i64      = ct_i64
local u64      = ct_u64
local const_cp = ffi.typeof('const char *')

-- Scratch buffers for the float/double conversions. Nothing between the write
-- and the read of one of these yields, so sharing them across calls is safe.
local f32     = ffi.new('float[1]')
local f64     = ffi.new('double[1]')
local f32_ptr = ffi.cast(const_cp, f32)
local f64_ptr = ffi.cast(const_cp, f64)

local INT_MIN, INT_MAX = -2147483648, 2147483647
-- The largest magnitude a double represents exactly; longs within it are handed
-- back as plain Lua numbers, everything else stays int64 cdata.
local EXACT = 9007199254740992LL

local function fail(fmt, ...)
    error('avro.codec: ' .. string.format(fmt, ...), 0)
end

--------------------------------------------------------------------------------
-- Number classification
--------------------------------------------------------------------------------

--- True only for a real Lua nil.
--
-- `box.NULL == nil` is true -- a NULL pointer cdata compares equal to nil in
-- LuaJIT -- so `v == nil` cannot tell an absent table key from a key holding an
-- explicit null. Everywhere that difference matters, ask for the type instead.
local function is_absent(v)
    return type(v) == 'nil'
end

M.is_absent = is_absent

local function is_int64(v)
    return type(v) == 'cdata' and (ffi.istype(ct_i64, v) or ffi.istype(ct_u64, v))
end

M.is_int64 = is_int64

--- True for a Lua number with no fractional part and for 64-bit integer cdata.
local function is_integer(v)
    local tv = type(v)
    if tv == 'number' then
        -- Excludes inf and nan, whose remainder is nan.
        return v % 1 == 0
    end
    return is_int64(v)
end

local function is_number(v)
    return type(v) == 'number' or is_int64(v)
end

local function fits_int(v)
    return is_integer(v) and v >= INT_MIN and v <= INT_MAX
end

--------------------------------------------------------------------------------
-- Primitive writers
--------------------------------------------------------------------------------

--- Zigzag varint for a value already known to be in the int32 range. LuaJIT's
--  bit operations are 32 bit here, which is exactly the width needed.
local function put_int(out, v)
    local zz = bit.bxor(bit.lshift(v, 1), bit.arshift(v, 31))
    while true do
        local b = bit.band(zz, 0x7f)
        zz = bit.rshift(zz, 7)
        if zz == 0 then
            out[#out + 1] = string.char(b)
            return
        end
        out[#out + 1] = string.char(b + 0x80)
    end
end

--- Zigzag varint over the full 64-bit range.
local function put_long(out, v)
    if type(v) == 'number' and v >= INT_MIN and v <= INT_MAX then
        return put_int(out, v)
    end
    local n = i64(v)
    local zz = u64(bit.bxor(bit.lshift(n, 1), bit.arshift(n, 63)))
    while true do
        local b = tonumber(bit.band(zz, 0x7f))
        zz = u64(bit.rshift(zz, 7))
        if zz == 0 then
            out[#out + 1] = string.char(b)
            return
        end
        out[#out + 1] = string.char(b + 0x80)
    end
end

M.put_long = put_long

--------------------------------------------------------------------------------
-- Primitive readers
--------------------------------------------------------------------------------

local function need(data, pos, count)
    if pos + count - 1 > #data then
        fail('unexpected end of input: %d bytes wanted at offset %d, %d available',
             count, pos, #data - pos + 1)
    end
end

--- Read a varint and return it zigzag-decoded as int64 cdata, plus the next
--  position.
local function get_long_raw(data, pos)
    local n = #data
    local shift = 0
    local acc = u64(0)
    while true do
        if pos > n then
            fail('unexpected end of input while reading a varint at offset %d', pos)
        end
        local b = data:byte(pos)
        pos = pos + 1
        acc = u64(bit.bor(acc, bit.lshift(u64(bit.band(b, 0x7f)), shift)))
        if b < 0x80 then
            break
        end
        shift = shift + 7
        if shift > 63 then
            fail('varint is longer than 10 bytes at offset %d', pos)
        end
    end
    -- (acc >>> 1) ^ -(acc & 1)
    local value = i64(bit.bxor(bit.rshift(acc, 1), -i64(bit.band(acc, 1))))
    return value, pos
end

--- A long, narrowed to a Lua number when a double holds it exactly.
local function get_long(data, pos)
    local v, next_pos = get_long_raw(data, pos)
    if v <= EXACT and v >= -EXACT then
        return tonumber(v), next_pos
    end
    return v, next_pos
end

M.get_long = get_long

local function get_int(data, pos)
    local v, next_pos = get_long_raw(data, pos)
    if v < INT_MIN or v > INT_MAX then
        fail('int at offset %d is out of the int range: %s', pos, tostring(v))
    end
    return tonumber(v), next_pos
end

--- A count for an array/map block, or a byte length; must be a Lua-sized
--  integer to be usable as one.
local function get_size(data, pos, what)
    local v, next_pos = get_long_raw(data, pos)
    if v < 0 or v > EXACT then
        fail('%s at offset %d is out of range: %s', what, pos, tostring(v))
    end
    return tonumber(v), next_pos
end

--------------------------------------------------------------------------------
-- Encoders
--------------------------------------------------------------------------------

local encode_value
local validate

local encoders = {}

encoders['null'] = function(_, value, _)
    if value ~= nil and value ~= NULL then
        fail('null expects nil or box.NULL, got %s', type(value))
    end
end

encoders['boolean'] = function(_, value, out)
    if type(value) ~= 'boolean' then
        fail('boolean expects true or false, got %s', type(value))
    end
    out[#out + 1] = value and '\1' or '\0'
end

encoders['int'] = function(_, value, out)
    if not is_number(value) then
        fail('int expects a number, got %s', type(value))
    end
    if not is_integer(value) then
        fail('int must be an integer, got %s', tostring(value))
    end
    if value < INT_MIN or value > INT_MAX then
        fail('%s is out of the int range', tostring(value))
    end
    put_int(out, tonumber(value))
end

encoders['long'] = function(_, value, out)
    if not is_number(value) then
        fail('long expects a number, got %s', type(value))
    end
    if not is_integer(value) then
        fail('long must be an integer, got %s', tostring(value))
    end
    put_long(out, value)
end

encoders['float'] = function(_, value, out)
    if not is_number(value) then
        fail('float expects a number, got %s', type(value))
    end
    -- Avro floats are little-endian IEEE-754, which is the host layout on every
    -- platform Tarantool runs on.
    f32[0] = tonumber(value)
    out[#out + 1] = ffi.string(f32_ptr, 4)
end

encoders['double'] = function(_, value, out)
    if not is_number(value) then
        fail('double expects a number, got %s', type(value))
    end
    f64[0] = tonumber(value)
    out[#out + 1] = ffi.string(f64_ptr, 8)
end

local function encode_binary(kind)
    return function(_, value, out)
        if type(value) ~= 'string' then
            fail('%s expects a string, got %s', kind, type(value))
        end
        put_long(out, #value)
        out[#out + 1] = value
    end
end

encoders['bytes']  = encode_binary('bytes')
encoders['string'] = encode_binary('string')

encoders['fixed'] = function(sc, value, out)
    if type(value) ~= 'string' then
        fail('fixed %s expects a string, got %s', sc.fullname, type(value))
    end
    if #value ~= sc.size then
        fail('fixed %s expects exactly %d bytes, got %d', sc.fullname, sc.size, #value)
    end
    out[#out + 1] = value
end

encoders['enum'] = function(sc, value, out)
    local idx = type(value) == 'string' and sc.symbol_index[value] or nil
    if idx == nil then
        fail('%s is not a symbol of enum %s', tostring(value), sc.fullname)
    end
    put_int(out, idx - 1)
end

encoders['array'] = function(sc, value, out)
    if type(value) ~= 'table' then
        fail('array expects a table, got %s', type(value))
    end
    local n = #value
    if n > 0 then
        put_long(out, n)
        for i = 1, n do
            encode_value(sc.items, value[i], out)
        end
    end
    out[#out + 1] = '\0'
end

encoders['map'] = function(sc, value, out)
    if type(value) ~= 'table' then
        fail('map expects a table, got %s', type(value))
    end
    local n = 0
    for _ in pairs(value) do
        n = n + 1
    end
    if n > 0 then
        put_long(out, n)
        for k, v in pairs(value) do
            if type(k) ~= 'string' then
                fail('map keys must be strings, got %s', type(k))
            end
            put_long(out, #k)
            out[#out + 1] = k
            encode_value(sc.values, v, out)
        end
    end
    out[#out + 1] = '\0'
end

--- Pick the branch of a union a value belongs to.
--
-- A value that exactly one branch accepts picks that branch. Otherwise the
-- tuple notation {[branch_name] = value} decides, and failing that the first
-- accepting branch wins, in schema order.
--
-- @return branch index, the value to encode against it.
local function select_branch(sc, value)
    local types = sc.types
    if value == nil or value == NULL then
        local idx = sc.branch_index['null']
        if idx ~= nil then
            return idx, value
        end
    end
    local first, count = nil, 0
    for i = 1, #types do
        if validate(types[i], value) then
            count = count + 1
            if first == nil then
                first = i
            end
        end
    end
    if count == 1 then
        return first, value
    end
    if type(value) == 'table' then
        local k, inner = next(value)
        if type(k) == 'string' and next(value, k) == nil then
            local idx = sc.branch_index[k]
            if idx ~= nil then
                return idx, inner
            end
        end
    end
    if first ~= nil then
        return first, value
    end
    local names = {}
    for i = 1, #types do
        names[i] = types[i].fullname or types[i].kind
    end
    fail('no branch of the union [%s] accepts a value of type %s',
         table.concat(names, ', '), type(value))
end

M.select_branch = select_branch

encoders['union'] = function(sc, value, out)
    local idx, inner = select_branch(sc, value)
    put_int(out, idx - 1)
    encode_value(sc.types[idx], inner, out)
end

encoders['record'] = function(sc, value, out)
    if type(value) ~= 'table' then
        fail('record %s expects a table, got %s', sc.fullname, type(value))
    end
    local fields = sc.fields
    for i = 1, #fields do
        local f = fields[i]
        local v = value[f.name]
        if is_absent(v) then
            local default, present = avro_schema.field_default(f)
            if present then
                v = default
            elseif f.type.kind ~= 'null' then
                fail('record %s has no value for field %q and the schema declares ' ..
                     'no default', sc.fullname, f.name)
            end
        end
        encode_value(f.type, v, out)
    end
end

encode_value = function(sc, value, out)
    local fn = encoders[sc.kind]
    if fn == nil then
        fail('cannot encode a schema of kind %q', tostring(sc.kind))
    end
    return fn(sc, value, out)
end

M.encode_value = encode_value

--- Encode one value. Returns the bytes.
function M.encode(sc, value)
    sc = avro_schema.parse(sc)
    local out = {}
    encode_value(sc, value, out)
    return table.concat(out)
end

--------------------------------------------------------------------------------
-- Decoders
--------------------------------------------------------------------------------

local decode_value

local decoders = {}

decoders['null'] = function(_, _, pos)
    return NULL, pos
end

decoders['boolean'] = function(_, data, pos)
    need(data, pos, 1)
    local b = data:byte(pos)
    if b > 1 then
        fail('boolean at offset %d has the invalid value %d', pos, b)
    end
    return b == 1, pos + 1
end

decoders['int']  = function(_, data, pos) return get_int(data, pos) end
decoders['long'] = function(_, data, pos) return get_long(data, pos) end

decoders['float'] = function(_, data, pos)
    need(data, pos, 4)
    ffi.copy(f32, ffi.cast(const_cp, data) + (pos - 1), 4)
    return tonumber(f32[0]), pos + 4
end

decoders['double'] = function(_, data, pos)
    need(data, pos, 8)
    ffi.copy(f64, ffi.cast(const_cp, data) + (pos - 1), 8)
    return tonumber(f64[0]), pos + 8
end

local function decode_binary(_, data, pos)
    local len, next_pos = get_size(data, pos, 'length')
    need(data, next_pos, len)
    return data:sub(next_pos, next_pos + len - 1), next_pos + len
end

decoders['bytes']  = decode_binary
decoders['string'] = decode_binary

decoders['fixed'] = function(sc, data, pos)
    need(data, pos, sc.size)
    return data:sub(pos, pos + sc.size - 1), pos + sc.size
end

decoders['enum'] = function(sc, data, pos)
    local idx, next_pos = get_int(data, pos)
    local sym = sc.symbols[idx + 1]
    if sym == nil then
        if sc.default ~= nil then
            return sc.default, next_pos
        end
        fail('enum %s has no symbol at index %d', sc.fullname, idx)
    end
    return sym, next_pos
end

--- Walk the block structure shared by arrays and maps, calling `on_block` with
--  the number of entries in each block.
local function each_block(data, pos, on_block)
    while true do
        local count
        count, pos = get_long_raw(data, pos)
        if count == 0 then
            return pos
        end
        if count < 0 then
            -- A negative count is followed by the block's byte size, which a
            -- reader that wanted to skip the block would use.
            count = -count
            local _
            _, pos = get_size(data, pos, 'block size')
        end
        if count > EXACT then
            fail('block count at offset %d is out of range', pos)
        end
        pos = on_block(tonumber(count), pos)
    end
end

decoders['array'] = function(sc, data, pos)
    local out, n = {}, 0
    local items = sc.items
    local next_pos = each_block(data, pos, function(count, p)
        for _ = 1, count do
            n = n + 1
            out[n], p = decode_value(items, data, p)
        end
        return p
    end)
    return out, next_pos
end

decoders['map'] = function(sc, data, pos)
    local out = {}
    local values = sc.values
    local next_pos = each_block(data, pos, function(count, p)
        for _ = 1, count do
            local key
            key, p = decode_binary(nil, data, p)
            out[key], p = decode_value(values, data, p)
        end
        return p
    end)
    return out, next_pos
end

decoders['union'] = function(sc, data, pos)
    local idx, next_pos = get_int(data, pos)
    local branch = sc.types[idx + 1]
    if branch == nil then
        fail('union branch index %d is out of range (%d branches)', idx, #sc.types)
    end
    return decode_value(branch, data, next_pos)
end

decoders['record'] = function(sc, data, pos)
    local out = {}
    local fields = sc.fields
    for i = 1, #fields do
        local f = fields[i]
        out[f.name], pos = decode_value(f.type, data, pos)
    end
    return out, pos
end

decode_value = function(sc, data, pos)
    local fn = decoders[sc.kind]
    if fn == nil then
        fail('cannot decode a schema of kind %q', tostring(sc.kind))
    end
    return fn(sc, data, pos)
end

M.decode_value = decode_value

--- Decode one value out of `data`, starting at `pos` (1-based, default 1).
--  Returns the value and the position just past it.
function M.decode(sc, data, pos)
    sc = avro_schema.parse(sc)
    if type(data) ~= 'string' then
        fail('decode expects a string, got %s', type(data))
    end
    local value, next_pos = decode_value(sc, data, pos or 1)
    if value == NULL then
        -- A null at the top level has no table slot to keep alive.
        return nil, next_pos
    end
    return value, next_pos
end

--------------------------------------------------------------------------------
-- Validation
--------------------------------------------------------------------------------

local validators = {}

validators['null']    = function(_, v) return v == nil or v == NULL end
validators['boolean'] = function(_, v) return type(v) == 'boolean' end
validators['int']     = function(_, v) return fits_int(v) end
validators['long']    = function(_, v) return is_integer(v) end
validators['float']   = function(_, v) return is_number(v) end
validators['double']  = function(_, v) return is_number(v) end
validators['bytes']   = function(_, v) return type(v) == 'string' end
validators['string']  = function(_, v) return type(v) == 'string' end

validators['fixed'] = function(sc, v)
    return type(v) == 'string' and #v == sc.size
end

validators['enum'] = function(sc, v)
    return type(v) == 'string' and sc.symbol_index[v] ~= nil
end

validators['array'] = function(sc, v)
    if type(v) ~= 'table' then
        return false
    end
    local n = #v
    -- A table with non-integer keys is a map or a record, not an array.
    local seen = 0
    for _ in pairs(v) do
        seen = seen + 1
    end
    if seen ~= n then
        return false
    end
    for i = 1, n do
        if not validate(sc.items, v[i]) then
            return false
        end
    end
    return true
end

validators['map'] = function(sc, v)
    if type(v) ~= 'table' then
        return false
    end
    for k, item in pairs(v) do
        if type(k) ~= 'string' or not validate(sc.values, item) then
            return false
        end
    end
    return true
end

validators['union'] = function(sc, v)
    for i = 1, #sc.types do
        if validate(sc.types[i], v) then
            return true
        end
    end
    return false
end

validators['record'] = function(sc, v)
    if type(v) ~= 'table' then
        return false
    end
    -- Deliberately stricter than encode(), which ignores keys the schema does
    -- not name: union branch selection needs an unexpected key to disqualify a
    -- record, or every table would match every record in a union.
    for k in pairs(v) do
        if type(k) ~= 'string' or sc.field_map[k] == nil then
            return false
        end
    end
    for i = 1, #sc.fields do
        local f = sc.fields[i]
        local item = v[f.name]
        if is_absent(item) then
            if not f.has_default and f.type.kind ~= 'null' then
                return false
            end
        elseif not validate(f.type, item) then
            return false
        end
    end
    return true
end

--- True when `value` can be encoded against `sc`.
validate = function(sc, value)
    local fn = validators[sc.kind]
    if fn == nil then
        return false
    end
    return fn(sc, value)
end

M.validate = function(sc, value)
    return validate(avro_schema.parse(sc), value)
end

return M
