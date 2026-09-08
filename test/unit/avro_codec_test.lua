local t = require('luatest')
local ffi = require('ffi')

local schema = require('pregel.avro.schema')
local codec  = require('pregel.avro.codec')

local g = t.group('avro_codec')

local NULL = box.NULL

local function hex(s)
    local out = {}
    for i = 1, #s do
        out[i] = string.format('%02x', s:byte(i))
    end
    return table.concat(out, ' ')
end

local function unhex(h)
    return (h:gsub('%s*(%x%x)%s*', function(b)
        return string.char(tonumber(b, 16))
    end))
end

--- Encode `value` under `spec` and assert the bytes, then decode them back and
--  assert the value survives the round trip. `decoded`, when given, is a
--  one-element table holding what decode should return -- a table so that an
--  expected nil is expressible.
local function assert_bytes(spec, value, expected_hex, decoded)
    local sc = schema.parse(spec)
    local encoded = codec.encode(sc, value)
    t.assert_equals(hex(encoded), expected_hex, 'encoding under ' .. tostring(spec))
    local back, pos = codec.decode(sc, encoded)
    t.assert_equals(pos, #encoded + 1, 'decode consumed the whole buffer')
    if decoded == nil then
        t.assert_equals(back, value)
    else
        t.assert_equals(back, decoded[1])
        t.assert_equals(type(back), type(decoded[1]))
    end
end

--------------------------------------------------------------------------------
-- Varint / zigzag, the spec's own table
--------------------------------------------------------------------------------

g.test_int_zigzag_varint = function()
    local cases = {
        {0, '00'}, {-1, '01'}, {1, '02'}, {-2, '03'}, {2, '04'},
        {-64, '7f'}, {64, '80 01'}, {8192, '80 80 01'}, {-8193, '81 80 01'},
        {2147483647, 'fe ff ff ff 0f'}, {-2147483648, 'ff ff ff ff 0f'},
    }
    for _, case in ipairs(cases) do
        assert_bytes('"int"', case[1], case[2])
    end
end

g.test_long_zigzag_varint = function()
    for _, case in ipairs({{0, '00'}, {-1, '01'}, {1, '02'}, {-2, '03'}, {2, '04'},
                           {-64, '7f'}, {64, '80 01'}}) do
        assert_bytes('"long"', case[1], case[2])
    end
end

g.test_long_beyond_2_53_round_trips_as_int64 = function()
    local sc = schema.parse('"long"')
    for _, v in ipairs({9223372036854775807LL, -9223372036854775807LL - 1LL,
                        1234567890123456789LL, -1234567890123456789LL}) do
        local back = codec.decode(sc, codec.encode(sc, v))
        t.assert_equals(type(back), 'cdata')
        t.assert_equals(tostring(back), tostring(v))
    end
end

g.test_long_max_bytes = function()
    -- 2^63-1 zigzags to 2^64-2, ten varint bytes with the top one set.
    local sc = schema.parse('"long"')
    t.assert_equals(hex(codec.encode(sc, 9223372036854775807LL)),
                    'fe ff ff ff ff ff ff ff ff 01')
    t.assert_equals(hex(codec.encode(sc, -9223372036854775807LL - 1LL)),
                    'ff ff ff ff ff ff ff ff ff 01')
end

g.test_long_within_2_53_decodes_as_a_lua_number = function()
    local sc = schema.parse('"long"')
    local back = codec.decode(sc, codec.encode(sc, 1234567))
    t.assert_equals(type(back), 'number')
    t.assert_equals(back, 1234567)
end

g.test_int_rejects_out_of_range = function()
    local sc = schema.parse('"int"')
    t.assert_error_msg_contains('out of the int range', codec.encode, sc, 2147483648)
    t.assert_error_msg_contains('must be an integer', codec.encode, sc, 1.5)
end

--------------------------------------------------------------------------------
-- Primitives
--------------------------------------------------------------------------------

g.test_null = function()
    local sc = schema.parse('"null"')
    t.assert_equals(codec.encode(sc, nil), '')
    t.assert_equals(codec.encode(sc, NULL), '')
    local v, pos = codec.decode(sc, '')
    t.assert_equals(v, nil)
    t.assert_equals(pos, 1)
end

g.test_boolean = function()
    assert_bytes('"boolean"', true,  '01')
    assert_bytes('"boolean"', false, '00')
    t.assert_error_msg_contains('boolean', codec.encode, schema.parse('"boolean"'), 1)
end

g.test_float = function()
    assert_bytes('"float"', 1.0,  '00 00 80 3f')
    assert_bytes('"float"', 0.0,  '00 00 00 00')
    assert_bytes('"float"', -2.0, '00 00 00 c0')
end

g.test_double = function()
    assert_bytes('"double"', 1.0,  '00 00 00 00 00 00 f0 3f')
    assert_bytes('"double"', 0.0,  '00 00 00 00 00 00 00 00')
    assert_bytes('"double"', -2.0, '00 00 00 00 00 00 00 c0')
end

g.test_float_keeps_only_single_precision = function()
    local sc = schema.parse('"float"')
    local back = codec.decode(sc, codec.encode(sc, 0.1))
    t.assert_almost_equals(back, 0.1, 1e-7)
    t.assert_not_equals(back, 0.1)
end

g.test_string = function()
    assert_bytes('"string"', 'foo', '06 66 6f 6f')
    assert_bytes('"string"', '',    '00')
    -- UTF-8 is carried through verbatim; the length is in bytes.
    assert_bytes('"string"', 'ы',   '04 d1 8b')
end

g.test_bytes = function()
    assert_bytes('"bytes"', '\0\1', '04 00 01')
    assert_bytes('"bytes"', '',     '00')
end

g.test_fixed = function()
    local spec = '{"type":"fixed","name":"MD5","size":4}'
    assert_bytes(spec, '\1\2\3\4', '01 02 03 04')
    t.assert_error_msg_contains('exactly 4 bytes',
                                codec.encode, schema.parse(spec), 'abc')
end

g.test_enum = function()
    local spec = '{"type":"enum","name":"Suit","symbols":["SPADES","HEARTS","CLUBS"]}'
    assert_bytes(spec, 'SPADES', '00')
    assert_bytes(spec, 'HEARTS', '02')
    assert_bytes(spec, 'CLUBS',  '04')
    t.assert_error_msg_contains('not a symbol', codec.encode, schema.parse(spec), 'NOPE')
end

--------------------------------------------------------------------------------
-- Complex types
--------------------------------------------------------------------------------

g.test_array = function()
    -- count 2 as a zigzag long, the items, then the zero terminator.
    assert_bytes('{"type":"array","items":"long"}', {1, 2}, '04 02 04 00')
    assert_bytes('{"type":"array","items":"long"}', {},     '00')
    assert_bytes('{"type":"array","items":"string"}', {'a'}, '02 02 61 00')
end

g.test_array_reader_accepts_several_blocks = function()
    local sc = schema.parse('{"type":"array","items":"long"}')
    -- Two blocks of one item each, then the terminator.
    t.assert_equals(codec.decode(sc, unhex('02 02 02 04 00')), {1, 2})
end

g.test_array_reader_accepts_the_negative_count_form = function()
    local sc = schema.parse('{"type":"array","items":"long"}')
    -- -2 items followed by the block byte size (2), then the items.
    t.assert_equals(codec.decode(sc, unhex('03 04 02 04 00')), {1, 2})
end

g.test_map = function()
    assert_bytes('{"type":"map","values":"long"}', {a = 1}, '02 02 61 02 00')
    assert_bytes('{"type":"map","values":"long"}', {},      '00')
end

g.test_map_reader_accepts_the_negative_count_form = function()
    local sc = schema.parse('{"type":"map","values":"long"}')
    t.assert_equals(codec.decode(sc, unhex('01 06 02 61 02 00')), {a = 1})
end

g.test_union_null_and_value = function()
    local spec = '["null","string"]'
    -- A top-level null decodes to a plain nil.
    assert_bytes(spec, NULL,  '00', {nil})
    assert_bytes(spec, 'a',   '02 02 61')
    local sc = schema.parse(spec)
    -- nil selects the null branch too.
    t.assert_equals(codec.encode(sc, nil), '\0')
end

g.test_union_branch_order = function()
    assert_bytes('["string","null"]', 'a', '00 02 61')
    assert_bytes('["string","null"]', NULL, '02', {nil})
end

g.test_union_tuple_notation_disambiguates = function()
    local spec = '["float","double"]'
    local sc = schema.parse(spec)
    -- Both branches accept a number, so the first one wins by default.
    t.assert_equals(hex(codec.encode(sc, 1.0)), '00 00 00 80 3f')
    -- The explicit branch name overrides that.
    t.assert_equals(hex(codec.encode(sc, {double = 1.0})),
                    '02 00 00 00 00 00 00 f0 3f')
end

g.test_union_tuple_notation_with_fullnames = function()
    local sc = schema.parse('[{"type":"record","name":"a.A","fields":[]},' ..
                            '{"type":"record","name":"a.B","fields":[]}]')
    t.assert_equals(hex(codec.encode(sc, {['a.B'] = {}})), '02')
    t.assert_equals(hex(codec.encode(sc, {['a.A'] = {}})), '00')
end

g.test_union_rejects_a_value_no_branch_accepts = function()
    local sc = schema.parse('["null","string"]')
    t.assert_error_msg_contains('no branch of the union',
                                codec.encode, sc, {1, 2, 3})
end

g.test_union_decodes_to_the_plain_value = function()
    local sc = schema.parse('["null","string"]')
    t.assert_equals(codec.decode(sc, unhex('02 02 61')), 'a')
    t.assert_equals(codec.decode(sc, unhex('00')), nil)
end

g.test_union_rejects_an_out_of_range_index_on_decode = function()
    local sc = schema.parse('["null","string"]')
    t.assert_error_msg_contains('union branch index', codec.decode, sc, unhex('08'))
end

g.test_record = function()
    local spec = [[{"type":"record","name":"P","fields":[
        {"name":"name","type":"string"},
        {"name":"age","type":"int"}
    ]}]]
    assert_bytes(spec, {name = 'foo', age = 3}, '06 66 6f 6f 06')
end

g.test_record_null_field_is_box_null_after_decode = function()
    local sc = schema.parse('{"type":"record","name":"R","fields":[' ..
                            '{"name":"a","type":["null","int"]}]}')
    local back = codec.decode(sc, unhex('00'))
    t.assert_equals(back.a, NULL)
    -- The key is present, so it is distinguishable from an absent field.
    local seen = false
    for k in pairs(back) do
        seen = seen or k == 'a'
    end
    t.assert_equals(seen, true)
end

g.test_record_uses_a_default_for_a_missing_field = function()
    local sc = schema.parse([[{"type":"record","name":"R","fields":[
        {"name":"a","type":"int"},
        {"name":"b","type":"string","default":"zzz"}
    ]}]])
    t.assert_equals(hex(codec.encode(sc, {a = 1})), '02 06 7a 7a 7a')
end

g.test_explicit_null_is_not_an_absent_field = function()
    -- box.NULL compares equal to nil in LuaJIT, so a `v == nil` test cannot
    -- tell a key holding an explicit null from a key that is not there. An
    -- explicit null must encode as the null branch, not fall back to a default.
    local sc = schema.parse([[{"type":"record","name":"R","fields":[
        {"name":"a","type":["null","int"],"default":null},
        {"name":"b","type":["int","null"],"default":7}
    ]}]])
    -- a = null branch 0, b = explicit null selects branch 1.
    t.assert_equals(hex(codec.encode(sc, {a = NULL, b = NULL})), '00 02')
    -- Absent fields fall back to the declared defaults instead.
    t.assert_equals(hex(codec.encode(sc, {})), '00 00 0e')
end

g.test_validate_separates_absent_from_explicit_null = function()
    local sc = schema.parse('{"type":"record","name":"R","fields":[' ..
                            '{"name":"a","type":["null","int"]}]}')
    t.assert_equals(codec.validate(sc, {a = NULL}), true)
    -- No default and no value at all: not encodable.
    t.assert_equals(codec.validate(sc, {}), false)
end

g.test_record_missing_field_without_a_default_is_an_error = function()
    local sc = schema.parse('{"type":"record","name":"R","fields":[' ..
                            '{"name":"a","type":["null","int"]}]}')
    t.assert_error_msg_contains('has no value for field "a"', codec.encode, sc, {})
end

g.test_nested_record_and_recursion = function()
    local sc = schema.parse([[{"type":"record","name":"Node","fields":[
        {"name":"label","type":"string"},
        {"name":"children","type":{"type":"array","items":"Node"}}
    ]}]])
    local value = {
        label = 'a',
        children = {{label = 'b', children = {}}},
    }
    local encoded = codec.encode(sc, value)
    t.assert_equals(hex(encoded), '02 61 02 02 62 00 00')
    t.assert_equals(codec.decode(sc, encoded), value)
end

--------------------------------------------------------------------------------
-- Framing
--------------------------------------------------------------------------------

g.test_decode_honours_pos_and_returns_the_next_one = function()
    local sc = schema.parse('"string"')
    local data = 'xx' .. codec.encode(sc, 'foo') .. 'yy'
    local v, pos = codec.decode(sc, data, 3)
    t.assert_equals(v, 'foo')
    t.assert_equals(pos, 7)
end

g.test_truncated_input_is_reported = function()
    local sc = schema.parse('"string"')
    t.assert_error_msg_contains('unexpected end of input', codec.decode, sc, '\6fo')
    t.assert_error_msg_contains('unexpected end of input', codec.decode, schema.parse('"double"'), '\1\2')
end

g.test_overlong_varint_is_rejected = function()
    local sc = schema.parse('"long"')
    t.assert_error_msg_contains('varint', codec.decode, sc,
                                string.rep('\255', 10) .. '\1')
end

--------------------------------------------------------------------------------
-- validate()
--------------------------------------------------------------------------------

g.test_validate = function()
    t.assert_equals(codec.validate(schema.parse('"int"'), 1), true)
    t.assert_equals(codec.validate(schema.parse('"int"'), 1.5), false)
    t.assert_equals(codec.validate(schema.parse('"int"'), 2 ^ 40), false)
    t.assert_equals(codec.validate(schema.parse('"long"'), 2 ^ 40), true)
    t.assert_equals(codec.validate(schema.parse('"null"'), nil), true)
    t.assert_equals(codec.validate(schema.parse('"null"'), NULL), true)
    t.assert_equals(codec.validate(schema.parse('"string"'), 'a'), true)
    t.assert_equals(codec.validate(schema.parse('"string"'), 1), false)
    t.assert_equals(codec.validate(
        schema.parse('{"type":"fixed","name":"F","size":2}'), 'ab'), true)
    t.assert_equals(codec.validate(
        schema.parse('{"type":"fixed","name":"F","size":2}'), 'abc'), false)
    t.assert_equals(codec.validate(
        schema.parse('{"type":"enum","name":"E","symbols":["A"]}'), 'A'), true)
    t.assert_equals(codec.validate(
        schema.parse('{"type":"enum","name":"E","symbols":["A"]}'), 'B'), false)

    local rec = schema.parse('{"type":"record","name":"R","fields":[' ..
                             '{"name":"a","type":"int"}]}')
    t.assert_equals(codec.validate(rec, {a = 1}), true)
    t.assert_equals(codec.validate(rec, {a = 1, other = 2}), false)
    t.assert_equals(codec.validate(rec, {}), false)
end

g.test_validate_accepts_int64_cdata_for_long = function()
    t.assert_equals(codec.validate(schema.parse('"long"'), 5LL), true)
    t.assert_equals(codec.validate(schema.parse('"int"'), 5LL), true)
    t.assert_equals(codec.validate(schema.parse('"int"'), 1099511627776LL), false)
    t.assert_equals(codec.validate(schema.parse('"long"'), ffi.new('uint64_t', 5)), true)
end

--------------------------------------------------------------------------------
-- Everything together
--------------------------------------------------------------------------------

g.test_round_trip_of_a_wide_record = function()
    local sc = schema.parse([[{"type":"record","name":"Wide","namespace":"w","fields":[
        {"name":"n","type":"null"},
        {"name":"b","type":"boolean"},
        {"name":"i","type":"int"},
        {"name":"l","type":"long"},
        {"name":"f","type":"float"},
        {"name":"d","type":"double"},
        {"name":"by","type":"bytes"},
        {"name":"s","type":"string"},
        {"name":"e","type":{"type":"enum","name":"E","symbols":["X","Y"]}},
        {"name":"fx","type":{"type":"fixed","name":"F","size":3}},
        {"name":"a","type":{"type":"array","items":"int"}},
        {"name":"m","type":{"type":"map","values":"string"}},
        {"name":"u","type":["null","int"]},
        {"name":"r","type":{"type":"record","name":"Inner","fields":[
            {"name":"v","type":"long"}]}}
    ]}]])
    local value = {
        n = NULL, b = true, i = -7, l = 2 ^ 40, f = 0.5, d = 1.25,
        by = '\0\255', s = 'привет', e = 'Y', fx = 'abc',
        a = {1, 2, 3}, m = {k = 'v'}, u = 42,
        r = {v = -1},
    }
    local encoded = codec.encode(sc, value)
    local back, pos = codec.decode(sc, encoded)
    t.assert_equals(pos, #encoded + 1)
    t.assert_equals(back, value)
end
