local t   = require('luatest')
local fio = require('fio')

local schema  = require('pregel.avro.schema')
local codec   = require('pregel.avro.codec')
local ocf     = require('pregel.avro.ocf')
local resolve = require('pregel.avro.resolve')

local g = t.group('avro_resolve')

local NULL = box.NULL

local FIXTURES = fio.pathjoin(
    fio.dirname(fio.dirname(fio.abspath(debug.getinfo(1, 'S').source:sub(2)))),
    'fixtures', 'avro')

--- Write `value` with `writer`, read it back with `reader`.
local function through(writer, value, reader)
    local w = schema.parse(writer)
    local r = schema.parse(reader)
    local data = codec.encode(w, value)
    local got, pos = codec.decode(w, data, 1, r)
    t.assert_equals(pos, #data + 1, 'resolution consumed the whole buffer')
    return got
end

local function assert_rejects(writer, reader, pattern)
    local ok, err = pcall(resolve.resolver, writer, reader)
    t.assert_equals(ok, false, 'expected the pair to be rejected')
    t.assert_str_contains(tostring(err), pattern)
end

--------------------------------------------------------------------------------
-- Identity
--------------------------------------------------------------------------------

g.test_same_schema_resolves_to_itself = function()
    for _, spec in ipairs({'"null"', '"boolean"', '"int"', '"long"', '"float"',
                           '"double"', '"bytes"', '"string"'}) do
        local sc = schema.parse(spec)
        local value = ({
            ['null'] = NULL, ['boolean'] = true, ['int'] = -7, ['long'] = 1e10,
            ['float'] = 0.5, ['double'] = 1.25, ['bytes'] = '\0\1',
            ['string'] = 'x',
        })[sc.kind]
        local got = through(spec, value, spec)
        if sc.kind == 'null' then
            t.assert_equals(got, nil)
        else
            t.assert_equals(got, value)
        end
    end
end

--------------------------------------------------------------------------------
-- Numeric promotions
--------------------------------------------------------------------------------

g.test_int_promotes_to_long_float_and_double = function()
    t.assert_equals(through('"int"', 42, '"long"'), 42)
    t.assert_equals(through('"int"', 42, '"float"'), 42)
    t.assert_equals(through('"int"', 42, '"double"'), 42)
end

g.test_long_promotes_to_float_and_double = function()
    t.assert_equals(through('"long"', 1234, '"float"'), 1234)
    t.assert_equals(through('"long"', 1234, '"double"'), 1234)
end

g.test_a_long_past_2_53_promotes_to_a_lua_number = function()
    local got = through('"long"', 9007199254740993LL, '"double"')
    t.assert_equals(type(got), 'number')
    t.assert_almost_equals(got, 9007199254740993, 2)
end

g.test_float_promotes_to_double = function()
    t.assert_equals(through('"float"', 0.5, '"double"'), 0.5)
end

g.test_string_and_bytes_promote_to_each_other = function()
    t.assert_equals(through('"string"', 'abc', '"bytes"'), 'abc')
    t.assert_equals(through('"bytes"', '\0\255', '"string"'), '\0\255')
end

g.test_narrowing_is_rejected = function()
    assert_rejects('"long"', '"int"', 'a long cannot be read as a int')
    assert_rejects('"double"', '"float"', 'a double cannot be read as a float')
    assert_rejects('"string"', '"int"', 'a string cannot be read as a int')
end

--------------------------------------------------------------------------------
-- Records
--------------------------------------------------------------------------------

local V1 = [[{"type":"record","name":"User","namespace":"t","fields":[
    {"name":"id","type":"long"},
    {"name":"name","type":"string"},
    {"name":"scratch","type":"string"}
]}]]

g.test_reader_field_order_is_independent_of_the_writer = function()
    local reader = [[{"type":"record","name":"User","namespace":"t","fields":[
        {"name":"name","type":"string"},
        {"name":"scratch","type":"string"},
        {"name":"id","type":"long"}
    ]}]]
    local got = through(V1, {id = 1, name = 'a', scratch = 's'}, reader)
    t.assert_equals(got, {id = 1, name = 'a', scratch = 's'})
end

g.test_a_writer_field_the_reader_dropped_is_skipped = function()
    local reader = [[{"type":"record","name":"User","namespace":"t","fields":[
        {"name":"id","type":"long"},
        {"name":"name","type":"string"}
    ]}]]
    local got = through(V1, {id = 7, name = 'a', scratch = 'ignored'}, reader)
    t.assert_equals(got, {id = 7, name = 'a'})
    -- The dropped field really is gone, not merely nil-valued.
    local keys = {}
    for k in pairs(got) do
        keys[#keys + 1] = k
    end
    table.sort(keys)
    t.assert_equals(keys, {'id', 'name'})
end

g.test_a_new_reader_field_comes_from_its_default = function()
    local reader = [[{"type":"record","name":"User","namespace":"t","fields":[
        {"name":"id","type":"long"},
        {"name":"name","type":"string"},
        {"name":"scratch","type":"string"},
        {"name":"email","type":["null","string"],"default":null},
        {"name":"score","type":"int","default":10}
    ]}]]
    local got = through(V1, {id = 1, name = 'a', scratch = 's'}, reader)
    t.assert_equals(got.email, NULL)
    t.assert_equals(got.score, 10)
end

g.test_a_new_reader_field_without_a_default_is_rejected = function()
    local reader = [[{"type":"record","name":"User","namespace":"t","fields":[
        {"name":"id","type":"long"},
        {"name":"name","type":"string"},
        {"name":"scratch","type":"string"},
        {"name":"email","type":"string"}
    ]}]]
    assert_rejects(V1, reader, 'the reader wants field "email"')
end

g.test_a_table_default_is_copied_not_shared = function()
    local writer = '{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}'
    local reader = [[{"type":"record","name":"R","fields":[
        {"name":"a","type":"int"},
        {"name":"tags","type":{"type":"array","items":"string"},"default":["x"]}
    ]}]]
    local first  = through(writer, {a = 1}, reader)
    table.insert(first.tags, 'mutated')
    local second = through(writer, {a = 2}, reader)
    t.assert_equals(second.tags, {'x'}, 'the second record must not see the mutation')
end

g.test_field_promotion_inside_a_record = function()
    local writer = '{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}'
    local reader = '{"type":"record","name":"R","fields":[{"name":"a","type":"double"}]}'
    t.assert_equals(through(writer, {a = 3}, reader), {a = 3})
end

g.test_a_reader_alias_matches_a_renamed_field = function()
    local reader = [[{"type":"record","name":"User","namespace":"t","fields":[
        {"name":"id","type":"long"},
        {"name":"full_name","type":"string","aliases":["name"]},
        {"name":"scratch","type":"string"}
    ]}]]
    local got = through(V1, {id = 1, name = 'a', scratch = 's'}, reader)
    t.assert_equals(got.full_name, 'a')
    t.assert_equals(got.name, nil)
end

g.test_a_reader_alias_matches_a_renamed_record = function()
    local reader = [[{"type":"record","name":"t.Person","aliases":["t.User"],
        "fields":[
            {"name":"id","type":"long"},
            {"name":"name","type":"string"},
            {"name":"scratch","type":"string"}
        ]}]]
    t.assert_equals(through(V1, {id = 1, name = 'a', scratch = 's'}, reader),
                    {id = 1, name = 'a', scratch = 's'})
end

g.test_mismatched_record_names_are_rejected = function()
    local reader = [[{"type":"record","name":"t.Other","fields":[
        {"name":"id","type":"long"},
        {"name":"name","type":"string"},
        {"name":"scratch","type":"string"}
    ]}]]
    assert_rejects(V1, reader, 'the names do not match')
end

--- The specification matches named types on the *unqualified* name: "both
--  schemas are records with the same (unqualified) name". Matching on the
--  fullname refused a namespaced writer against an unnamespaced reader, which
--  fastavro 1.12.2 accepts.
g.test_named_types_match_on_the_unqualified_name = function()
    local W = '{"type":"record","name":"R","namespace":"n","fields":[' ..
              '{"name":"a","type":"int"}]}'
    local R = '{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}'
    t.assert_equals(through(W, {a = 1}, R), {a = 1})
    t.assert_equals(through(R, {a = 1}, W), {a = 1})
    -- Two different namespaces, same short name.
    local W2 = '{"type":"record","name":"R","namespace":"x.y","fields":[' ..
               '{"name":"a","type":"int"}]}'
    t.assert_equals(through(W, {a = 1}, W2), {a = 1})

    local WE = '{"type":"enum","name":"E","namespace":"n","symbols":["A","B"]}'
    local RE = '{"type":"enum","name":"E","symbols":["A","B"]}'
    t.assert_equals(through(WE, 'B', RE), 'B')

    local WF = '{"type":"fixed","name":"F","namespace":"n","size":2}'
    local RF = '{"type":"fixed","name":"F","size":2}'
    t.assert_equals(through(WF, 'ab', RF), 'ab')
end

--- Relaxing the namespace must not relax the name itself.
g.test_different_unqualified_names_are_still_rejected = function()
    local W = '{"type":"record","name":"R","namespace":"n","fields":[' ..
              '{"name":"a","type":"int"}]}'
    assert_rejects(W, '{"type":"record","name":"n.Q","fields":[' ..
                      '{"name":"a","type":"int"}]}', 'the names do not match')
    assert_rejects('{"type":"enum","name":"n.E","symbols":["A"]}',
                   '{"type":"enum","name":"n.G","symbols":["A"]}',
                   'the names do not match')
    assert_rejects('{"type":"fixed","name":"n.F","size":2}',
                   '{"type":"fixed","name":"n.G","size":2}',
                   'the names do not match')
end

g.test_recursive_schema_resolves = function()
    local writer = [[{"type":"record","name":"Node","fields":[
        {"name":"label","type":"string"},
        {"name":"gone","type":"int"},
        {"name":"children","type":{"type":"array","items":"Node"}}
    ]}]]
    local reader = [[{"type":"record","name":"Node","fields":[
        {"name":"label","type":"string"},
        {"name":"children","type":{"type":"array","items":"Node"}},
        {"name":"depth","type":"int","default":0}
    ]}]]
    local value = {
        label = 'a', gone = 1,
        children = {{label = 'b', gone = 2, children = {}}},
    }
    t.assert_equals(through(writer, value, reader), {
        label = 'a', depth = 0,
        children = {{label = 'b', depth = 0, children = {}}},
    })
end

--------------------------------------------------------------------------------
-- Enums
--------------------------------------------------------------------------------

local ENUM_W = '{"type":"enum","name":"Suit","symbols":["SPADES","HEARTS","CLUBS"]}'

g.test_enum_symbol_known_to_both = function()
    local reader = '{"type":"enum","name":"Suit","symbols":["HEARTS","SPADES"]}'
    t.assert_equals(through(ENUM_W, 'HEARTS', reader), 'HEARTS')
    t.assert_equals(through(ENUM_W, 'SPADES', reader), 'SPADES')
end

g.test_unknown_enum_symbol_falls_back_to_the_reader_default = function()
    local reader = '{"type":"enum","name":"Suit","symbols":["SPADES"],' ..
                   '"default":"SPADES"}'
    t.assert_equals(through(ENUM_W, 'CLUBS', reader), 'SPADES')
end

g.test_unknown_enum_symbol_without_a_default_is_an_error = function()
    local reader = '{"type":"enum","name":"Suit","symbols":["SPADES"]}'
    -- Resolution succeeds; the failure only happens on the offending value.
    t.assert_equals(through(ENUM_W, 'SPADES', reader), 'SPADES')
    t.assert_error_msg_contains('declares no default', through,
                                ENUM_W, 'CLUBS', reader)
end

--------------------------------------------------------------------------------
-- Unions
--------------------------------------------------------------------------------

g.test_writer_union_read_through_the_same_union = function()
    local spec = '["null","int"]'
    t.assert_equals(through(spec, 5, spec), 5)
    t.assert_equals(through(spec, NULL, spec), nil)
end

g.test_writer_union_narrowed_by_the_reader = function()
    -- The reader dropped the string branch; the int branch still resolves.
    t.assert_equals(through('["null","int","string"]', 5, '["null","int"]'), 5)
    t.assert_equals(through('["null","int","string"]', NULL, '["null","int"]'), nil)
end

g.test_a_writer_branch_the_reader_lost_fails_only_when_it_turns_up = function()
    local writer = '["null","int","string"]'
    local reader = '["null","int"]'
    -- Building the resolver must not fail, since the data may never use it.
    local fn = resolve.resolver(writer, reader)
    t.assert_equals(type(fn), 'function')
    local data = codec.encode(schema.parse(writer), {string = 'boom'})
    t.assert_error_msg_contains('no branch of the reader union', fn, data, 1)
end

g.test_non_union_writer_into_a_union_reader = function()
    t.assert_equals(through('"int"', 5, '["null","int"]'), 5)
    t.assert_equals(through('"int"', 5, '["null","long"]'), 5)
end

g.test_union_writer_into_a_non_union_reader = function()
    t.assert_equals(through('["int"]', 5, '"long"'), 5)
end

g.test_no_matching_reader_branch_is_rejected = function()
    assert_rejects('"string"', '["null","int"]',
                   'no branch of the reader union')
end

g.test_union_of_records_picks_by_name = function()
    local writer = '[{"type":"record","name":"A","fields":[{"name":"v","type":"int"}]},' ..
                   '{"type":"record","name":"B","fields":[{"name":"v","type":"int"}]}]'
    local reader = '[{"type":"record","name":"B","fields":[{"name":"v","type":"long"}]},' ..
                   '{"type":"record","name":"A","fields":[{"name":"v","type":"long"}]}]'
    t.assert_equals(through(writer, {A = {v = 1}}, reader), {v = 1})
    t.assert_equals(through(writer, {B = {v = 2}}, reader), {v = 2})
end

--------------------------------------------------------------------------------
-- Arrays, maps, fixed
--------------------------------------------------------------------------------

g.test_array_items_are_resolved = function()
    t.assert_equals(through('{"type":"array","items":"int"}', {1, 2, 3},
                            '{"type":"array","items":"double"}'), {1, 2, 3})
end

g.test_map_values_are_resolved = function()
    t.assert_equals(through('{"type":"map","values":"int"}', {a = 1},
                            '{"type":"map","values":"long"}'), {a = 1})
end

g.test_fixed_must_agree_on_name_and_size = function()
    local spec = '{"type":"fixed","name":"F","size":4}'
    t.assert_equals(through(spec, 'abcd', spec), 'abcd')
    assert_rejects(spec, '{"type":"fixed","name":"F","size":8}',
                   'is 4 bytes for the writer and 8 for the reader')
    assert_rejects(spec, '{"type":"fixed","name":"G","size":4}',
                   'the names do not match')
end

--------------------------------------------------------------------------------
-- Through the container format and the fastavro fixtures
--------------------------------------------------------------------------------

g.test_ocf_reader_schema_evolves_a_fastavro_file = function()
    -- The file was written by fastavro with the full "primitives" schema; read
    -- it through a schema that keeps three fields, promotes one and adds one.
    local reader = [[{"type":"record","name":"Primitives","namespace":"pregel.test",
        "fields":[
            {"name":"f_int","type":"long"},
            {"name":"f_string","type":"string"},
            {"name":"f_double","type":"double"},
            {"name":"note","type":"string","default":"added later"}
        ]}]]
    local r = ocf.open(fio.pathjoin(FIXTURES, 'primitives.null.avro'),
                       {mode = 'r', schema = reader})
    t.assert_equals(r.writer_schema.fullname, 'pregel.test.Primitives')
    local got = {}
    for record in r:records() do
        got[#got + 1] = record
    end
    r:close()

    t.assert_equals(#got, 4)
    for _, record in ipairs(got) do
        t.assert_equals(record.note, 'added later')
        -- Only the four reader fields survive.
        local keys = {}
        for k in pairs(record) do
            keys[#keys + 1] = k
        end
        table.sort(keys)
        t.assert_equals(keys, {'f_double', 'f_int', 'f_string', 'note'})
    end
    t.assert_equals(got[2].f_int, -1)
    t.assert_equals(got[2].f_string, 'foo')
    t.assert_equals(got[3].f_int, 2147483647)
end

g.test_ocf_reader_schema_over_a_deflate_file = function()
    local reader = [[{"type":"record","name":"Outer","namespace":"pregel.test",
        "fields":[
            {"name":"id","type":"double"},
            {"name":"tag","type":"string","default":"none"}
        ]}]]
    local records = ocf.read_all(fio.pathjoin(FIXTURES, 'nested.deflate.avro'),
                                 {mode = 'r', schema = reader})
    t.assert_equals(#records, 2)
    t.assert_equals(records[1], {id = 1, tag = 'none'})
    t.assert_equals(records[2], {id = -1, tag = 'none'})
end

g.test_evolving_a_file_this_implementation_wrote = function()
    local dir = fio.tempdir()
    local ok, err = pcall(function()
        local writer = schema.parse(V1)
        local file = fio.pathjoin(dir, 'v1.avro')
        ocf.write_all(file, writer, {
            {id = 1, name = 'a', scratch = 'x'},
            {id = 2, name = 'b', scratch = 'y'},
        }, {codec = 'deflate'})

        local reader = [[{"type":"record","name":"User","namespace":"t","fields":[
            {"name":"id","type":"double"},
            {"name":"full_name","type":"string","aliases":["name"]},
            {"name":"active","type":"boolean","default":true}
        ]}]]
        local records = ocf.read_all(file, {mode = 'r', schema = reader})
        t.assert_equals(records, {
            {id = 1, full_name = 'a', active = true},
            {id = 2, full_name = 'b', active = true},
        })
    end)
    fio.rmtree(dir)
    if not ok then
        error(err, 0)
    end
end

--------------------------------------------------------------------------------
-- Caching
--------------------------------------------------------------------------------

g.test_resolvers_are_reused_per_schema_pair = function()
    local w = schema.parse(V1)
    local r = schema.parse(V1)
    local data = codec.encode(w, {id = 1, name = 'a', scratch = 's'})
    -- Two separate calls must agree; the second comes from the cache.
    local first  = codec.decode(w, data, 1, r)
    local second = codec.decode(w, data, 1, r)
    t.assert_equals(first, second)
end

g.test_resolver_is_reusable_across_buffers = function()
    local w = schema.parse('"int"')
    local r = schema.parse('"long"')
    local fn = resolve.resolver(w, r)
    for _, value in ipairs({0, 1, -1, 1000, -100000}) do
        local data = codec.encode(w, value)
        local got, pos = fn(data, 1)
        t.assert_equals(got, value)
        t.assert_equals(pos, #data + 1)
    end
end

--------------------------------------------------------------------------------
-- Skipping, which resolution relies on
--------------------------------------------------------------------------------

g.test_skip_walks_past_every_type = function()
    local sc = schema.parse([[{"type":"record","name":"Wide","fields":[
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
        {"name":"u","type":["null","int"]}
    ]}]])
    local value = {
        n = NULL, b = true, i = -7, l = 2 ^ 40, f = 0.5, d = 1.25,
        by = '\0\255', s = 'ok', e = 'Y', fx = 'abc',
        a = {1, 2, 3}, m = {k = 'v'}, u = 42,
    }
    local data = codec.encode(sc, value) .. 'TRAILER'
    t.assert_equals(codec.skip(sc, data, 1), #data - #'TRAILER' + 1)
end

g.test_skip_jumps_a_sized_block_whole = function()
    local sc = schema.parse('{"type":"array","items":"long"}')
    -- The negative-count form: -2 items, block size 2, then the items.
    local data = '\3\4\2\4\0' .. 'TAIL'
    t.assert_equals(codec.skip(sc, data, 1), 6)
end
