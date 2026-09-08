--- Cross-implementation tests against fixtures produced by fastavro.
--
-- test/fixtures/avro/gen.py writes, for each case, the schema (.avsc), the
-- expected values (.json, one document per line), the plain binary encoding
-- (.bin), the canonical form and fingerprint (.meta.json) and two object
-- container files (.null.avro, .deflate.avro). Regenerate them with
--
--     uv run --with fastavro python3 test/fixtures/avro/gen.py
--
-- Nothing here reads a value this repository produced: every expectation comes
-- from the other implementation.

local t    = require('luatest')
local fio  = require('fio')
local json = require('json')

local schema  = require('pregel.avro.schema')
local codec   = require('pregel.avro.codec')
local ocf     = require('pregel.avro.ocf')
local deflate = require('pregel.avro.deflate')

local g = t.group('avro_interop')

local NULL = box.NULL

local FIXTURES = fio.pathjoin(
    fio.dirname(fio.dirname(fio.abspath(debug.getinfo(1, 'S').source:sub(2)))),
    'fixtures', 'avro')

--------------------------------------------------------------------------------
-- Fixture loading
--------------------------------------------------------------------------------

local function slurp(name)
    local path = fio.pathjoin(FIXTURES, name)
    local fh = fio.open(path, {'O_RDONLY'})
    t.assert_not_equals(fh, nil, 'missing fixture ' .. path ..
                        ' -- run test/fixtures/avro/gen.py')
    local out = fh:read()
    fh:close()
    return out
end

--- Turn the {"$bytes": "<hex>"} markers the generator writes back into Lua byte
--  strings. JSON has no byte type, and the fixtures carry real bytes.
local function unmark(value)
    if type(value) ~= 'table' then
        return value
    end
    local hex = value['$bytes']
    if type(hex) == 'string' and next(value, next(value)) == nil then
        return (hex:gsub('%x%x', function(pair)
            return string.char(tonumber(pair, 16))
        end))
    end
    local out = {}
    for k, v in pairs(value) do
        out[k] = unmark(v)
    end
    return out
end

local function load_expectations(name)
    local out = {}
    for line in slurp(name .. '.json'):gmatch('[^\n]+') do
        out[#out + 1] = unmark(json.decode(line))
    end
    return out
end

local CASES = json.decode(slurp('index.json')).cases

--------------------------------------------------------------------------------
-- Comparison
--
-- luatest's deep compare is not usable as is: fastavro's 64-bit integers arrive
-- from json.decode as int64 or uint64 cdata depending on sign, while the codec
-- always produces int64, and box.NULL has to match box.NULL.
--------------------------------------------------------------------------------

local function is_number_cdata(v)
    return type(v) == 'cdata' and v ~= NULL and pcall(function()
        return v + 0
    end)
end

local same

same = function(a, b, path)
    if a == NULL and b == NULL then
        return true, path
    end
    if is_number_cdata(a) or is_number_cdata(b) then
        if type(a) == 'table' or type(b) == 'table' then
            return false, path
        end
        -- Compares by value across int64/uint64/number.
        return a == b, path
    end
    if type(a) ~= type(b) then
        return false, path
    end
    if type(a) ~= 'table' then
        return a == b, path
    end
    for k, v in pairs(a) do
        local ok, where = same(v, b[k], path .. '.' .. tostring(k))
        if not ok then
            return false, where
        end
    end
    for k in pairs(b) do
        if type(a[k]) == 'nil' and not (a[k] == NULL and b[k] == NULL) then
            return false, path .. '.' .. tostring(k)
        end
    end
    return true, path
end

local function assert_same(got, want, label)
    local ok, where = same(got, want, label or 'value')
    if not ok then
        t.fail(string.format('%s differs at %s\n  got:  %s\n  want: %s',
                             label or 'value', where,
                             json.encode(got), json.encode(want)))
    end
end

--------------------------------------------------------------------------------
-- Schema, canonical form and fingerprint
--------------------------------------------------------------------------------

g.test_schemas_parse_and_match_fastavro_canonical_form = function()
    for _, name in ipairs(CASES) do
        local sc   = schema.parse(slurp(name .. '.avsc'))
        local meta = json.decode(slurp(name .. '.meta.json'))
        t.assert_equals(sc:canonical(), meta.canonical,
                        'canonical form of ' .. name)
        t.assert_equals(sc:fingerprint_hex(), meta.fingerprint,
                        'fingerprint of ' .. name)
    end
end

--------------------------------------------------------------------------------
-- The plain binary encoding
--------------------------------------------------------------------------------

g.test_decode_fastavro_binary = function()
    for _, name in ipairs(CASES) do
        local sc   = schema.parse(slurp(name .. '.avsc'))
        local data = slurp(name .. '.bin')
        local want = load_expectations(name)

        local pos = 1
        for i = 1, #want do
            local got
            got, pos = codec.decode(sc, data, pos)
            assert_same(got, want[i], string.format('%s record %d', name, i))
        end
        t.assert_equals(pos, #data + 1,
                        name .. ': decoding consumed the whole .bin')
    end
end

--- True when the value contains a map holding more than one entry.
--
-- Avro leaves the order of map entries undefined and a Lua table has no key
-- order to preserve, so such a record encodes as a permutation of what
-- fastavro wrote. Everything else must match byte for byte.
local function order_sensitive(sc, value)
    local kind = sc.kind
    if kind == 'map' then
        local n = 0
        for _, v in pairs(value) do
            n = n + 1
            if n > 1 or order_sensitive(sc.values, v) then
                return true
            end
        end
        return false
    elseif kind == 'array' then
        for i = 1, #value do
            if order_sensitive(sc.items, value[i]) then
                return true
            end
        end
    elseif kind == 'record' then
        for i = 1, #sc.fields do
            local f = sc.fields[i]
            local item = value[f.name]
            if type(item) ~= 'nil' and order_sensitive(f.type, item) then
                return true
            end
        end
    elseif kind == 'union' then
        for i = 1, #sc.types do
            if codec.validate(sc.types[i], value)
               and order_sensitive(sc.types[i], value) then
                return true
            end
        end
    end
    return false
end

g.test_encode_matches_fastavro_binary_byte_for_byte = function()
    local strict, relaxed = 0, 0
    for _, name in ipairs(CASES) do
        local sc      = schema.parse(slurp(name .. '.avsc'))
        local want    = slurp(name .. '.bin')
        local records = load_expectations(name)

        -- Walk the reference bytes record by record so that each record can be
        -- compared against its own slice.
        local pos = 1
        for i = 1, #records do
            local _, next_pos = codec.decode(sc, want, pos)
            local want_slice = want:sub(pos, next_pos - 1)
            local got_slice  = codec.encode(sc, records[i])
            local label = string.format('%s record %d', name, i)

            if order_sensitive(sc, records[i]) then
                relaxed = relaxed + 1
                -- A permutation has the same length, and must still decode to
                -- the same record.
                t.assert_equals(#got_slice, #want_slice, label .. ': same length')
                local got_back, after = codec.decode(sc, got_slice)
                t.assert_equals(after, #got_slice + 1, label .. ': fully consumed')
                assert_same(got_back, records[i], label)
            else
                strict = strict + 1
                t.assert_equals(got_slice, want_slice, label .. ': byte for byte')
            end
            pos = next_pos
        end
        t.assert_equals(pos, #want + 1, name .. ': consumed the whole .bin')
    end
    -- Guard against the strict path quietly becoming unreachable.
    t.assert_gt(strict, 15, 'most records must be compared byte for byte')
    t.assert_gt(relaxed, 0, 'the multi-entry map records must exist')
end

--------------------------------------------------------------------------------
-- Object container files
--------------------------------------------------------------------------------

g.test_read_fastavro_object_container_files = function()
    for _, name in ipairs(CASES) do
        local want = load_expectations(name)
        for _, codec_name in ipairs({'null', 'deflate'}) do
            local file = fio.pathjoin(FIXTURES, name .. '.' .. codec_name .. '.avro')
            local r = ocf.open(file, {mode = 'r'})
            t.assert_equals(r.codec, codec_name)
            local got = {}
            for record in r:records() do
                got[#got + 1] = record
            end
            r:close()
            t.assert_equals(#got, #want,
                            string.format('%s.%s: record count', name, codec_name))
            for i = 1, #want do
                assert_same(got[i], want[i],
                            string.format('%s.%s record %d', name, codec_name, i))
            end
        end
    end
end

--- Count calls into the pure-Lua inflater for the duration of `body`.
--
-- Without this, "read it through both paths" is unfalsifiable: a `force_pure`
-- that was reported but not obeyed left every such test green while the Lua
-- inflater was never reached. Measured by making the flag a no-op -- 42 tests,
-- 42 green. So the tests below assert the count, not the flag.
local function counting_inflate(body)
    local saved = deflate.inflate
    local calls = 0
    deflate.inflate = function(s)
        calls = calls + 1
        return saved(s)
    end
    local ok, err = pcall(body, function() return calls end,
                          function() calls = 0 end)
    deflate.inflate = saved
    if not ok then
        error(err, 0)
    end
end

g.test_fastavro_deflate_files_read_the_same_through_both_paths = function()
    -- The deflate codec has two readers: zlib through pregel.compress, and the
    -- pure-Lua inflater that is the guarantee on a host with no library. Both
    -- have to produce the same records from bytes fastavro wrote, and on this
    -- machine only the first would ever run -- so the second is forced here
    -- rather than left to a build that happens not to have zlib.
    local saved = deflate.force_pure
    counting_inflate(function(count, reset)
        local ok, err = pcall(function()
            for _, name in ipairs(CASES) do
                local file = fio.pathjoin(FIXTURES, name .. '.deflate.avro')
                local want = load_expectations(name)

                deflate.force_pure = false
                local _, fast_reader = deflate.backend()
                reset()
                local fast = ocf.read_all(file)
                if deflate.has_raw_inflate then
                    t.assert_equals(count(), 0,
                                    name .. ': the zlib reader did not run')
                end

                deflate.force_pure = true
                reset()
                local pure = ocf.read_all(file)
                t.assert_gt(count(), 0,
                            name .. ': the Lua inflater did not run')

                t.assert_equals(#fast, #want, name .. ': record count, zlib path')
                t.assert_equals(#pure, #want, name .. ': record count, pure path')
                for i = 1, #want do
                    assert_same(fast[i], want[i],
                                string.format('%s via %s, record %d', name,
                                              fast_reader, i))
                    assert_same(pure[i], want[i],
                                string.format('%s via pure-lua, record %d',
                                              name, i))
                end
            end
        end)
        deflate.force_pure = saved
        if not ok then
            error(err, 0)
        end
    end)
end

g.test_our_deflate_files_read_the_same_through_both_paths = function()
    -- The other direction: bytes this repository compressed, read back by the
    -- inflater that did not write them. A deflate stream zlib emits uses
    -- dynamic Huffman blocks, which the stored-block fallback never produces,
    -- so without a zlib on the machine this pairing does not happen at all.
    t.skip_if(not deflate.has_zlib, 'no libz can be loaded in this build')
    t.skip_if(not deflate.has_raw_inflate, 'only one reader in this build')
    local dir = fio.tempdir()
    local saved = deflate.force_pure
    counting_inflate(function(count, reset)
        local ok, err = pcall(function()
            for _, name in ipairs(CASES) do
                local sc = schema.parse(slurp(name .. '.avsc'))
                local want = load_expectations(name)
                local file = fio.pathjoin(dir, name .. '.deflate.avro')
                ocf.write_all(file, sc, want,
                              {codec = 'deflate', block_size = 128})
                for _, pure in ipairs({false, true}) do
                    deflate.force_pure = pure
                    reset()
                    local got = ocf.read_all(file)
                    if pure then
                        t.assert_gt(count(), 0, name .. ': the Lua inflater ran')
                    else
                        t.assert_equals(count(), 0, name .. ': zlib ran instead')
                    end
                    t.assert_equals(#got, #want)
                    for i = 1, #want do
                        assert_same(got[i], want[i],
                                    string.format('%s force_pure=%s record %d',
                                                  name, tostring(pure), i))
                    end
                end
            end
        end)
        deflate.force_pure = saved
        if not ok then
            error(err, 0)
        end
    end)
    fio.rmtree(dir)
end

g.test_zstandard_container_files_round_trip = function()
    -- zstandard used to be Enterprise-only here. It is not any more, so the
    -- codec gets the same treatment as the other two: written and read back
    -- against the expectations fastavro produced.
    t.skip_if(not ocf.codec_available('zstandard'),
              'no libzstd can be loaded in this build')
    local dir = fio.tempdir()
    local ok, err = pcall(function()
        for _, name in ipairs(CASES) do
            local sc = schema.parse(slurp(name .. '.avsc'))
            local want = load_expectations(name)
            local file = fio.pathjoin(dir, name .. '.zstandard.avro')
            ocf.write_all(file, sc, want, {codec = 'zstandard', block_size = 128})
            local r = ocf.open(file, {mode = 'r'})
            t.assert_equals(r.codec, 'zstandard')
            r:close()
            local got = ocf.read_all(file)
            t.assert_equals(#got, #want, name .. ': record count')
            for i = 1, #want do
                assert_same(got[i], want[i],
                            string.format('%s zstandard record %d', name, i))
            end
        end
    end)
    fio.rmtree(dir)
    if not ok then
        error(err, 0)
    end
end

g.test_fastavro_file_schema_matches_the_avsc = function()
    for _, name in ipairs(CASES) do
        local sc = schema.parse(slurp(name .. '.avsc'))
        local file_schema = ocf.schema_of(
            fio.pathjoin(FIXTURES, name .. '.null.avro'))
        t.assert_equals(file_schema:canonical(), sc:canonical(),
                        name .. ': the file header carries the same schema')
    end
end

g.test_deflate_fixtures_are_really_deflate = function()
    -- A deflate fixture that happened to be stored uncompressed would make the
    -- inflate path untested, and every deflate assertion above vacuous.
    local smaller = 0
    for _, name in ipairs(CASES) do
        local plain = #slurp(name .. '.null.avro')
        local packed = #slurp(name .. '.deflate.avro')
        if packed < plain then
            smaller = smaller + 1
        end
    end
    t.assert_gt(smaller, 0, 'no deflate fixture is smaller than its null twin')
end

--------------------------------------------------------------------------------
-- The pure-Lua inflater, against zlib's own output
--
-- Every Avro fixture above is a few hundred bytes and a container file this
-- repository writes on a build without compress.zlib uses stored blocks, so
-- between them they never emit a length code above the first few -- the
-- LENGTH_BASE / LENGTH_EXTRA tables of deflate.lua went essentially untested.
-- deflate_corpus.raw is built by gen.py to make the compressor use every one
-- of the 29 length codes; see deflate_corpus() there.
--------------------------------------------------------------------------------

g.test_inflate_matches_zlib_over_a_corpus_using_every_length_code = function()
    local meta  = json.decode(slurp('deflate_corpus.meta.json'))
    local plain = slurp('deflate_corpus.raw')
    t.assert_equals(#plain, meta.plain_bytes, 'the corpus is the size gen.py wrote')
    -- Big enough to reach the long matches; the check is worthless otherwise.
    t.assert_gt(#plain, 65536)

    for _, level in ipairs({'1', '6', '9'}) do
        local entry  = meta.levels[level]
        local packed = slurp(entry.file)
        t.assert_equals(#packed, entry.packed, entry.file .. ' is intact')
        local got = deflate.inflate(packed)
        -- Byte-for-byte, not a length or a checksum: a wrong LENGTH_BASE entry
        -- copies the right number of bytes from the wrong place as often as not.
        t.assert_equals(#got, #plain, 'inflated length at level ' .. level)
        t.assert_equals(got, plain, 'inflated bytes at level ' .. level)
    end
end

g.test_the_zlib_reader_matches_the_lua_one_over_the_same_corpus = function()
    -- Same corpus, the other reader. It is the one that actually runs on this
    -- machine, so leaving it out would mean the fast path was checked only
    -- against streams this repository wrote itself.
    t.skip_if(not deflate.has_raw_inflate, 'no libz can be loaded in this build')
    local meta  = json.decode(slurp('deflate_corpus.meta.json'))
    local plain = slurp('deflate_corpus.raw')
    for _, level in ipairs({'1', '6', '9'}) do
        local packed = slurp(meta.levels[level].file)
        t.assert_equals(deflate.decompress(packed), plain,
                        'zlib inflated bytes at level ' .. level)
    end
end

--------------------------------------------------------------------------------
-- Files this implementation writes are readable by the fixtures' own reader
--------------------------------------------------------------------------------

g.test_our_container_files_match_the_fastavro_ones_record_for_record = function()
    local dir = fio.tempdir()
    local ok, err = pcall(function()
        for _, name in ipairs(CASES) do
            local sc = schema.parse(slurp(name .. '.avsc'))
            local want = load_expectations(name)
            for _, codec_name in ipairs({'null', 'deflate'}) do
                local file = fio.pathjoin(dir, name .. '.' .. codec_name .. '.avro')
                ocf.write_all(file, sc, want, {codec = codec_name, block_size = 128})
                local got = ocf.read_all(file)
                t.assert_equals(#got, #want)
                for i = 1, #want do
                    assert_same(got[i], want[i],
                                string.format('%s.%s round trip %d',
                                              name, codec_name, i))
                end
            end
        end
    end)
    fio.rmtree(dir)
    if not ok then
        error(err, 0)
    end
end

--------------------------------------------------------------------------------
-- Defaults, checked against fastavro's bytes
--------------------------------------------------------------------------------

g.test_defaults_fill_in_to_the_same_bytes_fastavro_wrote = function()
    -- The second record of the "defaults" fixture holds exactly the declared
    -- defaults, so omitting those fields must produce the same bytes.
    local sc = schema.parse(slurp('defaults.avsc'))
    local want = load_expectations('defaults')
    local full = codec.encode(sc, want[2])
    local sparse = codec.encode(sc, {a = want[2].a})
    t.assert_equals(sparse, full)
end

--------------------------------------------------------------------------------
-- Self round trip over randomly generated values
--------------------------------------------------------------------------------

local random_value

random_value = function(sc, depth)
    depth = depth or 0
    local kind = sc.kind
    if kind == 'null' then
        return NULL
    elseif kind == 'boolean' then
        return math.random(0, 1) == 1
    elseif kind == 'int' then
        return math.random(-2147483648, 2147483647)
    elseif kind == 'long' then
        -- Stays inside the range a double holds exactly, so that the decoded
        -- value comes back as the same Lua number.
        return math.random(-9007199254740992, 9007199254740992)
    elseif kind == 'float' then
        -- Halves are exact in single precision, so the round trip is lossless.
        return math.random(-1000, 1000) / 2
    elseif kind == 'double' then
        return math.random(-1000000, 1000000) / 8
    elseif kind == 'bytes' or kind == 'string' then
        local n = math.random(0, 12)
        local out = {}
        for i = 1, n do
            -- Printable ASCII, so the value is valid UTF-8 for the string case.
            out[i] = string.char(math.random(32, 126))
        end
        return table.concat(out)
    elseif kind == 'fixed' then
        local out = {}
        for i = 1, sc.size do
            out[i] = string.char(math.random(0, 255))
        end
        return table.concat(out)
    elseif kind == 'enum' then
        return sc.symbols[math.random(1, #sc.symbols)]
    elseif kind == 'array' then
        local out = {}
        if depth < 3 then
            for i = 1, math.random(0, 5) do
                out[i] = random_value(sc.items, depth + 1)
            end
        end
        return out
    elseif kind == 'map' then
        local out = {}
        if depth < 3 then
            for i = 1, math.random(0, 5) do
                out['k' .. i] = random_value(sc.values, depth + 1)
            end
        end
        return out
    elseif kind == 'union' then
        return random_value(sc.types[math.random(1, #sc.types)], depth)
    elseif kind == 'record' then
        local out = {}
        for i = 1, #sc.fields do
            local f = sc.fields[i]
            out[f.name] = random_value(f.type, depth + 1)
        end
        return out
    end
    error('no generator for kind ' .. tostring(kind))
end

g.test_random_round_trip_per_schema = function()
    math.randomseed(20260908)
    for _, name in ipairs(CASES) do
        local sc = schema.parse(slurp(name .. '.avsc'))
        for round = 1, 25 do
            local value = random_value(sc)
            local encoded = codec.encode(sc, value)
            local back, pos = codec.decode(sc, encoded)
            t.assert_equals(pos, #encoded + 1,
                            string.format('%s round %d: full consumption',
                                          name, round))
            assert_same(back, value,
                        string.format('%s random round %d', name, round))
        end
    end
end

g.test_random_round_trip_through_a_container_file = function()
    math.randomseed(20260908)
    local dir = fio.tempdir()
    local ok, err = pcall(function()
        for _, name in ipairs(CASES) do
            local sc = schema.parse(slurp(name .. '.avsc'))
            local records = {}
            for i = 1, 50 do
                records[i] = random_value(sc)
            end
            local file = fio.pathjoin(dir, name .. '.rand.avro')
            ocf.write_all(file, sc, records, {codec = 'deflate', block_size = 256})
            local back = ocf.read_all(file)
            t.assert_equals(#back, #records)
            for i = 1, #records do
                assert_same(back[i], records[i],
                            string.format('%s random file record %d', name, i))
            end
        end
    end)
    fio.rmtree(dir)
    if not ok then
        error(err, 0)
    end
end
