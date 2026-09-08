local t = require('luatest')
local json = require('json')

local schema = require('pregel.avro.schema')

local g = t.group('avro_schema')

--------------------------------------------------------------------------------
-- Parsing
--------------------------------------------------------------------------------

g.test_parse_primitive_from_name = function()
    for _, name in ipairs({'null', 'boolean', 'int', 'long', 'float', 'double',
                           'bytes', 'string'}) do
        t.assert_equals(schema.parse(name).kind, name)
        t.assert_equals(schema.parse('"' .. name .. '"').kind, name)
        t.assert_equals(schema.parse({type = name}).kind, name)
    end
end

g.test_parse_accepts_json_text_and_table = function()
    local text = '{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}'
    local from_text  = schema.parse(text)
    local from_table = schema.parse(json.decode(text))
    t.assert_equals(from_text:canonical(), from_table:canonical())
end

g.test_parse_is_idempotent_on_schema_objects = function()
    local sc = schema.parse('"int"')
    t.assert_is(schema.parse(sc), sc)
end

g.test_record_fields_are_indexed = function()
    local sc = schema.parse({
        type = 'record', name = 'User', namespace = 'com.example',
        doc = 'a user',
        fields = {
            {name = 'id',   type = 'long'},
            {name = 'name', type = 'string', default = 'anon'},
        },
    })
    t.assert_equals(sc.kind, 'record')
    t.assert_equals(sc.name, 'User')
    t.assert_equals(sc.namespace, 'com.example')
    t.assert_equals(sc.fullname, 'com.example.User')
    t.assert_equals(sc.doc, 'a user')
    t.assert_equals(#sc.fields, 2)
    t.assert_equals(sc.fields[1].name, 'id')
    t.assert_equals(sc.fields[1].index, 1)
    t.assert_equals(sc.fields[1].type.kind, 'long')
    t.assert_equals(sc.fields[1].has_default, false)
    t.assert_equals(sc.fields[2].has_default, true)
    t.assert_is(sc.field_map.name, sc.fields[2])
end

g.test_namespace_is_inherited_by_nested_definitions = function()
    local sc = schema.parse({
        type = 'record', name = 'Outer', namespace = 'n1',
        fields = {
            {name = 'inner', type = {
                type = 'record', name = 'Inner',
                fields = {{name = 'v', type = 'int'}},
            }},
            {name = 'again', type = 'Inner'},
        },
    })
    t.assert_equals(sc.fields[1].type.fullname, 'n1.Inner')
    -- The unqualified reference resolves to the same object.
    t.assert_is(sc.fields[2].type, sc.fields[1].type)
end

g.test_dotted_name_wins_over_namespace_attribute = function()
    local sc = schema.parse({
        type = 'record', name = 'a.b.C', namespace = 'ignored',
        fields = {},
    })
    t.assert_equals(sc.fullname, 'a.b.C')
    t.assert_equals(sc.namespace, 'a.b')
    t.assert_equals(sc.name, 'C')
end

g.test_empty_namespace_means_null_namespace = function()
    local sc = schema.parse({
        type = 'record', name = 'Outer', namespace = 'n1',
        fields = {
            {name = 'inner', type = {
                type = 'record', name = 'Inner', namespace = '',
                fields = {},
            }},
        },
    })
    t.assert_equals(sc.fields[1].type.fullname, 'Inner')
    t.assert_equals(sc.fields[1].type.namespace, nil)
end

g.test_recursive_reference = function()
    local sc = schema.parse({
        type = 'record', name = 'Node',
        fields = {
            {name = 'label',    type = 'string'},
            {name = 'children', type = {type = 'array', items = 'Node'}},
        },
    })
    t.assert_is(sc.fields[2].type.items, sc)
end

g.test_enum_and_fixed = function()
    local e = schema.parse({type = 'enum', name = 'Suit',
                            symbols = {'SPADES', 'HEARTS'}, default = 'SPADES'})
    t.assert_equals(e.kind, 'enum')
    t.assert_equals(e.symbols, {'SPADES', 'HEARTS'})
    t.assert_equals(e.symbol_index.HEARTS, 2)
    t.assert_equals(e.default, 'SPADES')

    local f = schema.parse({type = 'fixed', name = 'MD5', size = 16})
    t.assert_equals(f.kind, 'fixed')
    t.assert_equals(f.size, 16)
end

g.test_union_branch_index = function()
    local u = schema.parse('["null","string",{"type":"fixed","name":"F","size":2}]')
    t.assert_equals(u.kind, 'union')
    t.assert_equals(#u.types, 3)
    t.assert_equals(u.branch_index['null'], 1)
    t.assert_equals(u.branch_index.string, 2)
    t.assert_equals(u.branch_index.F, 3)
end

g.test_logical_type_is_kept_as_metadata = function()
    local sc = schema.parse({type = 'long', logicalType = 'timestamp-millis'})
    t.assert_equals(sc.kind, 'long')
    t.assert_equals(sc.logical_type, 'timestamp-millis')

    local dec = schema.parse({type = 'bytes', logicalType = 'decimal',
                              precision = 4, scale = 2})
    t.assert_equals(dec.kind, 'bytes')
    t.assert_equals(dec.logical_type, 'decimal')
    t.assert_equals(dec.props.precision, 4)
    t.assert_equals(dec.props.scale, 2)
end

--------------------------------------------------------------------------------
-- Validation
--------------------------------------------------------------------------------

local function assert_rejects(spec, pattern)
    local ok, err = pcall(schema.parse, spec)
    t.assert_equals(ok, false, 'expected the schema to be rejected')
    t.assert_str_contains(tostring(err), pattern)
end

g.test_union_may_not_contain_a_union = function()
    assert_rejects('["null",["int","string"]]', 'may not immediately contain another union')
end

g.test_union_may_not_repeat_an_unnamed_type = function()
    assert_rejects('["int","int"]', 'contains "int" twice')
end

g.test_union_may_repeat_distinct_named_types = function()
    local u = schema.parse('[{"type":"fixed","name":"A","size":1},' ..
                           '{"type":"fixed","name":"B","size":1}]')
    t.assert_equals(#u.types, 2)
end

g.test_duplicate_names_are_rejected = function()
    assert_rejects({
        type = 'record', name = 'R',
        fields = {
            {name = 'a', type = {type = 'record', name = 'R', fields = {}}},
        },
    }, 'already defined')
end

g.test_unresolved_reference_is_rejected = function()
    assert_rejects({type = 'array', items = 'Missing'}, 'unknown type "Missing"')
end

g.test_invalid_names_are_rejected = function()
    assert_rejects({type = 'record', name = '1bad', fields = {}}, 'is not a valid name')
    assert_rejects({type = 'enum', name = 'E', symbols = {'ok', '1bad'}},
                   'is not a valid symbol')
end

g.test_duplicate_field_and_symbol_are_rejected = function()
    assert_rejects({type = 'record', name = 'R',
                    fields = {{name = 'a', type = 'int'}, {name = 'a', type = 'int'}}},
                   'duplicate field "a"')
    assert_rejects({type = 'enum', name = 'E', symbols = {'A', 'A'}},
                   'duplicate symbol "A"')
end

g.test_missing_structural_attributes_are_rejected = function()
    assert_rejects({type = 'record', name = 'R'}, 'has no "fields" array')
    assert_rejects({type = 'array'}, 'array has no "items"')
    assert_rejects({type = 'map'}, 'map has no "values"')
    assert_rejects({type = 'fixed', name = 'F'}, 'invalid "size"')
    assert_rejects({type = 'enum', name = 'E'}, 'has no "symbols" array')
end

g.test_enum_default_must_be_a_symbol = function()
    assert_rejects({type = 'enum', name = 'E', symbols = {'A'}, default = 'Z'},
                   'is not one of its symbols')
end

g.test_bad_field_order_is_rejected = function()
    assert_rejects({type = 'record', name = 'R',
                    fields = {{name = 'a', type = 'int', order = 'sideways'}}},
                   'invalid "order"')
end

--------------------------------------------------------------------------------
-- Parsing Canonical Form and fingerprints
--
-- Every expectation below was produced by fastavro 1.12.2 through
-- fastavro.schema.to_parsing_canonical_form / fingerprint(cf, 'CRC-64-AVRO').
--------------------------------------------------------------------------------

local CANONICAL_CASES = {
    {'"null"',    '"null"',    '8a8f25cce724dd63'},
    {'"boolean"', '"boolean"', '64f7d4a478fc429f'},
    {'"int"',     '"int"',     '8f5c393f1ad57572'},
    {'"long"',    '"long"',    'b71df49344e154d0'},
    {'"float"',   '"float"',   '90d7a83ecb027c4d'},
    {'"double"',  '"double"',  '7e95ab32c035758e'},
    {'"bytes"',   '"bytes"',   '651920c3da16c04f'},
    {'"string"',  '"string"',  'c70345637248018f'},
    {
        '{"type":"fixed","name":"Test","size":1}',
        '{"name":"Test","type":"fixed","size":1}',
        '6869897b4049355b',
    },
    {
        '{"type":"enum","name":"Test","symbols":["A","B"]}',
        '{"name":"Test","type":"enum","symbols":["A","B"]}',
        '03a2f2c2e27f7a16',
    },
    {
        '{"type":"record","name":"Test","fields":[{"name":"f","type":"long"}]}',
        '{"name":"Test","type":"record","fields":[{"name":"f","type":"long"}]}',
        'ed94e5f5e6eb588e',
    },
    {
        -- [FULLNAMES]: the namespace attribute is folded into the name.
        '{"type":"record","namespace":"x.y","name":"Test","fields":[' ..
            '{"name":"f","type":"long"},' ..
            '{"name":"g","type":{"type":"array","items":"string"}}]}',
        '{"name":"x.y.Test","type":"record","fields":[' ..
            '{"name":"f","type":"long"},' ..
            '{"name":"g","type":{"type":"array","items":"string"}}]}',
        '32cff53916efa3f7',
    },
    {
        -- A recursive reference is emitted as the fullname.
        '{"type":"record","name":"a.b.Node","fields":[' ..
            '{"name":"label","type":"string"},' ..
            '{"name":"children","type":{"type":"array","items":"Node"}}]}',
        '{"name":"a.b.Node","type":"record","fields":[' ..
            '{"name":"label","type":"string"},' ..
            '{"name":"children","type":{"type":"array","items":"a.b.Node"}}]}',
        '638fee531d047775',
    },
    {
        -- [STRIP]: doc, default, order and logicalType are all dropped.
        '{"type":"record","name":"Test","doc":"d","fields":[' ..
            '{"name":"f","type":{"type":"int","logicalType":"date"},' ..
            '"doc":"x","default":0,"order":"ignore"}]}',
        '{"name":"Test","type":"record","fields":[{"name":"f","type":"int"}]}',
        '567d052dec219c46',
    },
    {
        '["null",{"type":"map","values":"int"}]',
        '["null",{"type":"map","values":"int"}]',
        '05910561ab878d9b',
    },
    {
        '{"type":"record","name":"Outer","namespace":"n1","fields":[' ..
            '{"name":"inner","type":{"type":"record","name":"Inner","fields":[' ..
                '{"name":"v","type":"int"}]}},' ..
            '{"name":"again","type":"Inner"}]}',
        '{"name":"n1.Outer","type":"record","fields":[' ..
            '{"name":"inner","type":{"name":"n1.Inner","type":"record","fields":[' ..
                '{"name":"v","type":"int"}]}},' ..
            '{"name":"again","type":"n1.Inner"}]}',
        '93384123819d53bc',
    },
}

g.test_canonical_form = function()
    for _, case in ipairs(CANONICAL_CASES) do
        local sc = schema.parse(case[1])
        t.assert_equals(sc:canonical(), case[2], 'canonical form of ' .. case[1])
    end
end

g.test_fingerprint_hex = function()
    for _, case in ipairs(CANONICAL_CASES) do
        local sc = schema.parse(case[1])
        t.assert_equals(sc:fingerprint_hex(), case[3], 'fingerprint of ' .. case[1])
    end
end

g.test_fingerprint_is_a_64_bit_integer = function()
    local bit = require('bit')
    local fp = schema.parse('"int"'):fingerprint()
    t.assert_equals(type(fp), 'cdata')
    -- fingerprint_hex is the little-endian spelling of the same number.
    t.assert_equals(bit.tohex(fp), '7275d51a3f395c8f')
end

g.test_crc64_of_empty_string_is_the_seed = function()
    local bit = require('bit')
    t.assert_equals(bit.tohex(schema.crc64('')), 'c15d213aa4d7a795')
end

g.test_canonical_form_is_memoised = function()
    local sc = schema.parse('"int"')
    t.assert_is(sc:canonical(), sc:canonical())
end

--------------------------------------------------------------------------------
-- Defaults
--------------------------------------------------------------------------------

g.test_field_default_values = function()
    local sc = schema.parse({
        type = 'record', name = 'D',
        fields = {
            {name = 'i', type = 'int',    default = 7},
            {name = 's', type = 'string', default = 'hi'},
            {name = 'n', type = {'null', 'int'}, default = box.NULL},
            {name = 'a', type = {type = 'array', items = 'int'}, default = {1, 2}},
            {name = 'm', type = {type = 'map', values = 'int'}, default = {k = 3}},
        },
    })
    local function default_of(name)
        local value, present = schema.field_default(sc.field_map[name])
        t.assert_equals(present, true)
        return value
    end
    t.assert_equals(default_of('i'), 7)
    t.assert_equals(default_of('s'), 'hi')
    t.assert_equals(default_of('n'), box.NULL)
    t.assert_equals(default_of('a'), {1, 2})
    t.assert_equals(default_of('m'), {k = 3})
end

g.test_bytes_default_is_read_as_latin1 = function()
    -- The specification spells a bytes default as a string whose code points
    -- are the byte values, so ÿ is the single byte 0xff.
    local sc = schema.parse('{"type":"record","name":"D","fields":[' ..
                            '{"name":"b","type":"bytes","default":"\\u0000\\u00ff"}]}')
    local value = schema.field_default(sc.field_map.b)
    t.assert_equals(value, '\0\255')
end

g.test_missing_default_reports_absence = function()
    local sc = schema.parse({type = 'record', name = 'D',
                             fields = {{name = 'i', type = 'int'}}})
    local value, present = schema.field_default(sc.field_map.i)
    t.assert_equals(value, nil)
    t.assert_equals(present, false)
end

g.test_bad_default_is_rejected_on_use = function()
    local sc = schema.parse({type = 'record', name = 'D',
                             fields = {{name = 'i', type = 'int', default = 'nope'}}})
    local ok, err = pcall(schema.field_default, sc.field_map.i)
    t.assert_equals(ok, false)
    t.assert_str_contains(tostring(err), 'default for int must be a number')
end
