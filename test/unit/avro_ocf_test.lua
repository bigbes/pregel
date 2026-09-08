local t   = require('luatest')
local fio = require('fio')

local avro    = require('pregel.avro')
local schema  = require('pregel.avro.schema')
local codec   = require('pregel.avro.codec')
local deflate = require('pregel.avro.deflate')
local ocf     = require('pregel.avro.ocf')

local g = t.group('avro_ocf')

local NULL = box.NULL

local RECORD_JSON = [[{"type":"record","name":"User","namespace":"t","fields":[
    {"name":"id","type":"long"},
    {"name":"name","type":"string"},
    {"name":"tags","type":{"type":"array","items":"string"}},
    {"name":"score","type":["null","double"]}
]}]]

local function sample(n)
    local out = {}
    for i = 1, n do
        out[i] = {
            id = i,
            name = 'user-' .. i,
            tags = {'a', 'b'},
            score = (i % 3 == 0) and NULL or i / 2,
        }
    end
    return out
end

g.before_all(function()
    g.dir = fio.tempdir()
end)

g.after_all(function()
    if g.dir ~= nil then
        fio.rmtree(g.dir)
    end
end)

local function path(name)
    return fio.pathjoin(g.dir, name)
end

local function has_zstd()
    return ocf.codec_available('zstandard')
end

--------------------------------------------------------------------------------
-- Round trips
--------------------------------------------------------------------------------

local function round_trip(codec_name, count, block_size)
    local sc = schema.parse(RECORD_JSON)
    local records = sample(count)
    local file = path('rt-' .. codec_name .. '-' .. count .. '.avro')

    local w = ocf.open(file, {
        mode = 'w', schema = sc, codec = codec_name, block_size = block_size,
    })
    for _, rec in ipairs(records) do
        w:append(rec)
    end
    w:close()

    local r = ocf.open(file, {mode = 'r'})
    t.assert_equals(r.codec, codec_name)
    t.assert_equals(r.schema:canonical(), sc:canonical())
    local back = {}
    for rec in r:records() do
        back[#back + 1] = rec
    end
    r:close()
    t.assert_equals(back, records)
    return w, file
end

g.test_round_trip_null_codec = function()
    round_trip('null', 20)
end

g.test_round_trip_deflate_codec = function()
    -- Reading deflate never needs compress.zlib, so this runs on every build.
    round_trip('deflate', 20)
end

g.test_round_trip_zstandard_codec = function()
    t.skip_if(not has_zstd(), 'compress.zstd is not available in this build')
    round_trip('zstandard', 20)
end

g.test_deflate_actually_compresses_under_enterprise = function()
    t.skip_if(not deflate.has_zlib, 'compress.zlib is not available in this build')
    -- Highly repetitive input, so real compression is unmistakable.
    local body = string.rep('the same line over and over ', 500)
    local compressed = deflate.deflate(body)
    t.assert_lt(#compressed, #body / 10)
    t.assert_equals(deflate.inflate(compressed), body)
end

g.test_deflate_falls_back_to_stored_blocks_without_zlib = function()
    -- Stored blocks are valid RFC 1951 and the inflater reads them either way.
    local body = string.rep('xyz', 1000)
    t.assert_equals(deflate.inflate(deflate.store(body)), body)
end

g.test_multi_block_file = function()
    local sc = schema.parse(RECORD_JSON)
    local records = sample(200)
    local file = path('multi.avro')
    local w = ocf.open(file, {mode = 'w', schema = sc, block_size = 64})
    w:append_all(records)
    w:close()
    -- A 64-byte block budget over 200 records is many blocks, not one.
    t.assert_gt(w._blocks, 10)

    local back = ocf.read_all(file)
    t.assert_equals(back, records)
end

g.test_empty_file_has_a_header_and_no_records = function()
    local sc = schema.parse(RECORD_JSON)
    local file = path('empty.avro')
    local w = ocf.open(file, {mode = 'w', schema = sc})
    w:close()

    local r = ocf.open(file, {mode = 'r'})
    t.assert_equals(r.schema:canonical(), sc:canonical())
    local n = 0
    for _ in r:records() do
        n = n + 1
    end
    r:close()
    t.assert_equals(n, 0)
end

g.test_read_all_and_schema_of = function()
    local sc = schema.parse(RECORD_JSON)
    local file = path('all.avro')
    ocf.write_all(file, sc, sample(5))

    local records, got = ocf.read_all(file)
    t.assert_equals(#records, 5)
    t.assert_equals(got:canonical(), sc:canonical())

    local sc2, meta = ocf.schema_of(file)
    t.assert_equals(sc2:canonical(), sc:canonical())
    t.assert_equals(meta['avro.codec'], 'null')
end

--------------------------------------------------------------------------------
-- File layout
--------------------------------------------------------------------------------

local function slurp(file)
    local fh = fio.open(file, {'O_RDONLY'})
    local out = fh:read()
    fh:close()
    return out
end

g.test_layout_is_magic_metadata_sync = function()
    local sc = schema.parse('"long"')
    local file = path('layout.avro')
    local w = ocf.open(file, {mode = 'w', schema = sc, sync = string.rep('S', 16)})
    w:append(1)
    w:close()

    local raw = slurp(file)
    t.assert_equals(raw:sub(1, 4), 'Obj\1')

    local meta, pos = codec.decode(ocf.META_SCHEMA, raw, 5)
    t.assert_equals(meta['avro.codec'], 'null')
    t.assert_equals(meta['avro.schema'], '"long"')
    t.assert_equals(raw:sub(pos, pos + 15), string.rep('S', 16))

    -- One block: count 1, size 1, the byte 02 (zigzag 1), then the sync marker.
    local body = raw:sub(pos + 16)
    t.assert_equals(body, '\2\2\2' .. string.rep('S', 16))
end

g.test_metadata_is_carried_through = function()
    local sc = schema.parse('"long"')
    local file = path('meta.avro')
    local w = ocf.open(file, {mode = 'w', schema = sc,
                              metadata = {['x.owner'] = 'pregel'}})
    w:close()
    local r = ocf.open(file, {mode = 'r'})
    t.assert_equals(r.metadata['x.owner'], 'pregel')
    r:close()
end

g.test_schema_metadata_keeps_defaults = function()
    -- The canonical form strips defaults, so a writer that used it would lose
    -- information a reader needs.
    local sc = schema.parse([[{"type":"record","name":"R","fields":[
        {"name":"a","type":"int"},
        {"name":"b","type":"string","default":"zzz","doc":"why"}
    ]}]])
    local file = path('defaults.avro')
    local w = ocf.open(file, {mode = 'w', schema = sc})
    w:close()
    local r = ocf.open(file, {mode = 'r'})
    r:close()
    local field = r.schema.field_map.b
    t.assert_equals(field.has_default, true)
    t.assert_equals(schema.field_default(field), 'zzz')
    t.assert_equals(field.doc, 'why')
end

g.test_reading_from_a_string = function()
    local sc = schema.parse('"long"')
    local file = path('instr.avro')
    ocf.write_all(file, sc, {1, 2, 3})
    local bytes = slurp(file)

    local r = ocf.open({mode = 'r', data = bytes})
    local back = {}
    for v in r:records() do
        back[#back + 1] = v
    end
    r:close()
    t.assert_equals(back, {1, 2, 3})
end

--------------------------------------------------------------------------------
-- Errors
--------------------------------------------------------------------------------

g.test_bad_magic_is_rejected = function()
    t.assert_error_msg_contains('bad magic', ocf.open,
                                {mode = 'r', data = 'NOPE' .. string.rep('\0', 64)})
end

g.test_sync_mismatch_is_reported = function()
    local sc = schema.parse('"long"')
    local file = path('corrupt.avro')
    ocf.write_all(file, sc, {1})
    local raw = slurp(file)
    -- Flip the last byte, which is part of the block's sync marker.
    local broken = raw:sub(1, #raw - 1) .. string.char(raw:byte(#raw) == 0 and 1 or 0)
    t.assert_error_msg_contains('sync marker mismatch', function()
        for _ in ocf.open({mode = 'r', data = broken}):records() do end
    end)
end

--- next_block() checked the block's declared byte size for a negative value but
--  not its record count. A negative count made _remaining negative, so the
--  records() loop never reached zero and kept decoding past the block's data
--  until the codec ran out of bytes -- naming an offset deep inside the file
--  rather than the header field that is actually wrong.
g.test_a_negative_block_record_count_is_rejected = function()
    local sc = schema.parse('"long"')
    local file = path('negcount.avro')
    ocf.write_all(file, sc, {1, 2, 3})
    local raw = slurp(file)

    -- The header ends with the sync marker; the block's record count is the
    -- varint right after it. Three records zigzag to 0x06, one byte, so -1
    -- (0x01) replaces it without moving anything.
    local r = ocf.open({mode = 'r', data = raw})
    local sync = r.sync
    r:close()
    local at = raw:find(sync, 1, true)
    t.assert_not_equals(at, nil, 'the header sync marker must be findable')
    local count_at = at + #sync
    t.assert_equals(raw:byte(count_at), 6, 'three records zigzag to 0x06')
    local broken = raw:sub(1, count_at - 1) .. '\1' .. raw:sub(count_at + 1)

    t.assert_error_msg_contains('negative record count', function()
        for _ in ocf.open({mode = 'r', data = broken}):records() do end
    end)
end

g.test_unknown_codec_is_rejected = function()
    t.assert_error_msg_contains('unsupported codec "snappy"', ocf.open,
                                path('never.avro'),
                                {mode = 'w', schema = schema.parse('"long"'),
                                 codec = 'snappy'})
end

g.test_missing_zstd_module_names_it = function()
    t.skip_if(has_zstd(), 'compress.zstd is available, so it cannot be missing')
    t.assert_error_msg_contains('compress.zstd', function()
        ocf.write_all(path('zstd.avro'), schema.parse('"long"'), {1},
                      {codec = 'zstandard'})
    end)
end

g.test_writer_needs_a_schema = function()
    t.assert_error_msg_contains('needs a schema', ocf.open, path('x.avro'),
                                {mode = 'w'})
end

g.test_unknown_mode_is_rejected = function()
    t.assert_error_msg_contains('unknown mode', ocf.open, path('x.avro'),
                                {mode = 'append'})
end

g.test_append_after_close_is_rejected = function()
    local file = path('closed.avro')
    local w = ocf.open(file, {mode = 'w', schema = schema.parse('"long"')})
    w:close()
    t.assert_error_msg_contains('writer is closed', w.append, w, 1)
end

--------------------------------------------------------------------------------
-- Larger shapes
--------------------------------------------------------------------------------

g.test_large_schema_header_spanning_chunks = function()
    -- The header reader grows its buffer on demand; a schema well past the
    -- initial chunk exercises that.
    local fields = {}
    for i = 1, 800 do
        fields[i] = string.format('{"name":"field_number_%04d","type":"long"}', i)
    end
    local sc = schema.parse('{"type":"record","name":"Big","fields":[' ..
                            table.concat(fields, ',') .. ']}')
    t.assert_gt(#sc:tojson(), 8192)

    local record = {}
    for i = 1, 800 do
        record[string.format('field_number_%04d', i)] = i
    end
    local file = path('big-schema.avro')
    ocf.write_all(file, sc, {record})

    local back, got = ocf.read_all(file)
    t.assert_equals(#back, 1)
    t.assert_equals(back[1], record)
    t.assert_equals(got:canonical(), sc:canonical())
end

g.test_many_records_across_default_blocks = function()
    local sc = schema.parse(RECORD_JSON)
    local records = sample(5000)
    local file = path('many.avro')
    ocf.write_all(file, sc, records, {codec = 'deflate'})
    local back = ocf.read_all(file)
    t.assert_equals(#back, 5000)
    t.assert_equals(back[1], records[1])
    t.assert_equals(back[5000], records[5000])
end

g.test_null_records_do_not_end_the_iteration = function()
    -- A record that decodes to a null must not look like the end of the stream.
    local sc = schema.parse('"null"')
    local file = path('nulls.avro')
    ocf.write_all(file, sc, {NULL, NULL, NULL})
    local n = 0
    local r = ocf.open(file)
    for v in r:records() do
        n = n + 1
        t.assert_equals(v, NULL)
    end
    r:close()
    t.assert_equals(n, 3)
end

g.test_avro_package_exposes_ocf = function()
    t.assert_is(avro.ocf, ocf)
    t.assert_is(avro.schema, schema)
    t.assert_equals(avro.NULL, NULL)
end
