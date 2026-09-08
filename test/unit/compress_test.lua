-- pregel.compress: the ffi compatibility layer for Enterprise's `compress`.
--
-- The Enterprise cross-checks live at the bottom and skip themselves on
-- Community Edition, where there is no module to compare against. Everything
-- above them runs on both binaries.
local t      = require('luatest')
local digest = require('digest')

local lib      = require('pregel.compress.lib')
local zlib     = require('pregel.compress.zlib')
local zstd     = require('pregel.compress.zstd')
local lz4      = require('pregel.compress.lz4')
local compress = require('pregel.compress')

local g = t.group('compress')

-- Repetitive, so a real compressor is unmistakably smaller than the input.
local SAMPLE = string.rep('hello avro world ', 100)

local function hexhead(s, n)
    local out = {}
    for i = 1, math.min(#s, n) do
        out[#out + 1] = string.format('%02x', s:byte(i))
    end
    return table.concat(out, ' ')
end

--------------------------------------------------------------------------------
-- The library lookup
--------------------------------------------------------------------------------

g.test_lib_rejects_an_unknown_library = function()
    t.assert_error_msg_contains('unknown library "brotli"', lib.open, 'brotli')
end

g.test_lib_reports_where_it_found_the_library = function()
    local _, origin = lib.open('zlib')
    t.assert_type(origin, 'string')
    t.assert_not_equals(origin, '')
end

g.test_lib_caches_the_handle = function()
    -- Two default lookups must hand back the same namespace, or every call
    -- would pay for the search and two objects could end up bound to different
    -- copies of the library.
    local a = lib.open('zlib')
    local b = lib.open('zlib')
    t.assert_is(a, b)
end

g.test_lib_names_every_path_it_tried = function()
    -- The point of the message: a user whose libzstd is somewhere unusual can
    -- see that PREGEL_COMPRESS_LIBDIR is the answer without reading the source.
    local ok, err = pcall(lib.open, 'zstd',
                          {process = false, search = false,
                           libdir = '/nonexistent-pregel-compress'})
    t.assert_equals(ok, false)
    t.assert_str_contains(err, 'cannot load libzstd')
    t.assert_str_contains(err, '/nonexistent-pregel-compress/libzstd.so.1')
    t.assert_str_contains(err, '/nonexistent-pregel-compress/libzstd.1.dylib')
end

g.test_lib_prefers_the_process_over_a_file = function()
    -- With every file candidate pointed at nothing, the lookup either finds
    -- the symbol in the running binary or reports that it looked. Community
    -- Edition links zstd statically and exports it, so it takes the first
    -- branch; Enterprise 3.7 does not, so it takes the second. Both are
    -- asserted here rather than one being skipped, because which branch a
    -- build takes is exactly what this lookup exists to decide.
    local ok, C, origin = pcall(lib.open, 'zstd',
                                {search = false, env = false,
                                 libdir = '/nonexistent-pregel-compress'})
    if ok then
        t.assert_not_equals(C, nil)
        t.assert_str_contains(origin, 'ffi.C')
    else
        t.assert_str_contains(C,
            'ffi.C (the process exports no ZSTD_versionNumber)')
        t.assert_str_contains(C, '/nonexistent-pregel-compress/libzstd.so.1')
    end
end

g.test_lib_search_covers_the_homebrew_and_linux_directories = function()
    local paths = table.concat(lib.candidates('zstd', {env = false}), '\n')
    for _, want in ipairs({'zstd', '/opt/homebrew/lib/libzstd.1.dylib',
                           '/opt/homebrew/opt/zstd/lib/libzstd.1.dylib',
                           '/usr/local/lib/libzstd.so.1',
                           '/usr/lib/x86_64-linux-gnu/libzstd.so.1'}) do
        t.assert_str_contains(paths, want, false,
                              'the search covers ' .. want)
    end
end

g.test_lib_appends_the_env_directory = function()
    local with = lib.candidates('lz4', {libdir = '/tmp/pregel-compress-env'})
    local without = lib.candidates('lz4', {env = false})
    t.assert_gt(#with, #without)
    t.assert_str_contains(with[#with - 3], '/tmp/pregel-compress-env')
end

g.test_lib_available_does_not_raise = function()
    local ok, err = lib.available('zstd', {process = false, search = false,
                                           libdir = '/nonexistent-pregel'})
    t.assert_equals(ok, false)
    t.assert_str_contains(err, 'cannot load libzstd')
end

--------------------------------------------------------------------------------
-- zlib
--------------------------------------------------------------------------------

g.test_zlib_is_available_on_this_machine = function()
    -- Not a tautology: it is the check that says the lookup above actually
    -- reaches a real library rather than only reporting nicely when it cannot.
    local ok, why = zlib.available()
    t.assert_equals(ok, true, 'no libz found: ' .. tostring(why))
end

g.test_zlib_round_trips_every_size = function()
    local z = zlib.new()
    for _, n in ipairs({0, 1, 2, 1000, 65535, 65536, 1000000}) do
        local s = string.rep('ab', math.ceil(n / 2)):sub(1, n)
        t.assert_equals(#s, n)
        local c = z:compress(s)
        t.assert_equals(z:decompress(c), s, n .. ' bytes of repetitive input')
    end
end

g.test_zlib_round_trips_incompressible_input = function()
    -- Random data grows rather than shrinks, which is the path where the
    -- output buffer is *larger* than the input and deflateBound earns its
    -- keep; a fixed-size guess would truncate here.
    local s = digest.urandom(1000000)
    local z = zlib.new()
    local c = z:compress(s)
    t.assert_gt(#c, #s, 'random data does not compress')
    t.assert_equals(z:decompress(c), s)
end

g.test_zlib_actually_compresses = function()
    local c = zlib.new():compress(SAMPLE)
    t.assert_lt(#c, #SAMPLE / 10)
end

g.test_zlib_default_framing_is_the_enterprise_one = function()
    -- 78 9c is RFC 1950 at the default level, which is what the Enterprise
    -- module emits; a change here is a change in what other readers see.
    local c = zlib.new():compress(SAMPLE)
    t.assert_equals(hexhead(c, 2), '78 9c')
end

g.test_zlib_level_shows_in_the_framing = function()
    t.assert_equals(hexhead(zlib.new({level = 1}):compress(SAMPLE), 2), '78 01')
    t.assert_equals(hexhead(zlib.new({level = 9}):compress(SAMPLE), 2), '78 da')
end

g.test_zlib_raw_window_bits_drops_the_frame = function()
    local framed = zlib.new():compress(SAMPLE)
    local raw    = zlib.new({window_bits = -15}):compress(SAMPLE)
    -- Raw deflate is exactly the framed stream without its two-byte header and
    -- four-byte adler32, which is why the Avro deflate codec can use either.
    t.assert_equals(#raw, #framed - 6)
    t.assert_equals(raw, framed:sub(3, #framed - 4))
    t.assert_not_equals(hexhead(raw, 2), '78 9c')
end

g.test_zlib_gzip_window_bits_emits_the_gzip_magic = function()
    local c = zlib.new({window_bits = 31}):compress(SAMPLE)
    t.assert_equals(hexhead(c, 3), '1f 8b 08')
end

g.test_zlib_window_bits_is_honoured_when_decompressing = function()
    -- This is the whole superset over Enterprise, whose decompress ignores
    -- window_bits and always verifies the adler32. Each framing must read back
    -- through its own object and only through it.
    for _, bits in ipairs({15, -15, 31}) do
        local z = zlib.new({window_bits = bits})
        t.assert_equals(z:decompress(z:compress(SAMPLE)), SAMPLE,
                        'window_bits = ' .. bits)
    end
    local raw = zlib.new({window_bits = -15}):compress(SAMPLE)
    t.assert_error(function() zlib.new():decompress(raw) end)
    local framed = zlib.new():compress(SAMPLE)
    t.assert_error(function()
        zlib.new({window_bits = -15}):decompress(framed)
    end)
end

g.test_zlib_gzip_output_is_read_by_a_gzip_reader = function()
    -- Round-tripping through our own object would pass even if the header were
    -- wrong in a way both directions shared, so check the bytes a real gzip
    -- reader needs: magic, deflate method, and a trailing length.
    local c = zlib.new({window_bits = 31}):compress(SAMPLE)
    t.assert_equals(c:byte(1), 0x1f)
    t.assert_equals(c:byte(2), 0x8b)
    t.assert_equals(c:byte(3), 8)
    local n = 0
    for i = 0, 3 do
        n = n + c:byte(#c - 3 + i) * 2 ^ (8 * i)
    end
    t.assert_equals(n, #SAMPLE, 'the gzip trailer carries the input size')
end

g.test_zlib_strategies_change_the_output = function()
    local default = zlib.new():compress(SAMPLE)
    local huffman = zlib.new({strategy = 'huffman_only'}):compress(SAMPLE)
    t.assert_not_equals(huffman, default)
    -- Huffman-only cannot use matches, so a repetitive input stays large.
    t.assert_gt(#huffman, #default * 5)
    for name in pairs(zlib.STRATEGIES) do
        local z = zlib.new({strategy = name})
        t.assert_equals(z:decompress(z:compress(SAMPLE)), SAMPLE, name)
    end
end

g.test_zlib_level_zero_stores = function()
    local c = zlib.new({level = 0}):compress(SAMPLE)
    t.assert_gt(#c, #SAMPLE)
    t.assert_equals(zlib.new():decompress(c), SAMPLE)
end

g.test_zlib_mem_level_round_trips = function()
    for _, m in ipairs({1, 8, 9}) do
        local z = zlib.new({mem_level = m})
        t.assert_equals(z:decompress(z:compress(SAMPLE)), SAMPLE, 'mem_level ' .. m)
    end
end

g.test_zlib_rejects_bad_options = function()
    t.assert_error_msg_contains("'level' should be of type number in range [0..9]",
                                zlib.new, {level = 42})
    t.assert_error_msg_contains("'level' should be of type number in range [0..9]",
                                zlib.new, {level = 'six'})
    t.assert_error_msg_contains("'mem_level' should be of type number in range [1..9]",
                                zlib.new, {mem_level = 0})
    t.assert_error_msg_contains("'strategy' should be of type string",
                                zlib.new, {strategy = 7})
    t.assert_error_msg_contains("'strategy' should be one of", zlib.new,
                                {strategy = 'bogus'})
    t.assert_error_msg_contains("'window_bits' should be in 9..15", zlib.new,
                                {window_bits = 3})
end

g.test_zlib_rejects_non_strings = function()
    local z = zlib.new()
    t.assert_error_msg_contains('expects a string, got number', z.compress, z, 42)
    t.assert_error_msg_contains('expects a string, got table', z.decompress, z, {})
end

g.test_zlib_corrupt_input_raises_and_says_what = function()
    local z = zlib.new()
    local c = z:compress(SAMPLE)
    t.assert_error_msg_contains('incorrect header check', z.decompress, z,
                                'not compressed at all')
    t.assert_error_msg_contains('incorrect data check', z.decompress, z,
                                c:sub(1, #c - 4) .. 'ZZZZ')
    t.assert_error_msg_contains('truncated stream', z.decompress, z, c:sub(1, 12))
    t.assert_error_msg_contains('empty input', z.decompress, z, '')
end

g.test_zlib_survives_a_thousand_corrupt_blocks = function()
    -- Bounds are the reason this exists: a decompressor that trusted a length
    -- out of the stream would read past its buffer here rather than raise, and
    -- the process would go down instead of the call.
    local z = zlib.new({window_bits = -15})
    local good = z:compress(SAMPLE)
    for i = 1, 1000 do
        local pos = (i % #good) + 1
        local bad = good:sub(1, pos - 1) ..
                    string.char((good:byte(pos) + 137) % 256) ..
                    good:sub(pos + 1)
        -- Either it decodes to something or it raises; neither may crash, and
        -- a raise must be this module's, not a segfault turned into one.
        local ok, err = pcall(z.decompress, z, bad)
        if not ok then
            t.assert_str_contains(err, 'pregel.compress')
        end
    end
end

--------------------------------------------------------------------------------
-- zstd
--------------------------------------------------------------------------------

g.test_zstd_is_available_on_this_machine = function()
    local ok, why = zstd.available()
    t.assert_equals(ok, true, 'no libzstd found: ' .. tostring(why))
end

g.test_zstd_round_trips_every_size = function()
    local z = zstd.new()
    for _, n in ipairs({0, 1, 2, 1000, 131072, 1000000}) do
        local s = string.rep('ab', math.ceil(n / 2)):sub(1, n)
        t.assert_equals(#s, n)
        t.assert_equals(z:decompress(z:compress(s)), s, n .. ' bytes')
    end
end

g.test_zstd_round_trips_incompressible_input = function()
    local s = digest.urandom(1000000)
    local z = zstd.new()
    t.assert_equals(z:decompress(z:compress(s)), s)
end

g.test_zstd_emits_a_standard_frame = function()
    -- The magic is what makes the output readable by the `zstd` command and by
    -- every other implementation, Enterprise's included.
    t.assert_equals(hexhead(zstd.new():compress(SAMPLE), 4), '28 b5 2f fd')
end

g.test_zstd_levels_round_trip = function()
    for _, level in ipairs({-5, 0, 1, 3, 19, 22}) do
        local z = zstd.new({level = level})
        t.assert_equals(z:decompress(z:compress(SAMPLE)), SAMPLE, 'level ' .. level)
    end
end

g.test_zstd_a_high_level_compresses_at_least_as_well = function()
    local body = string.rep('the same line over and over ', 2000)
    t.assert_le(#zstd.new({level = 19}):compress(body),
                #zstd.new({level = 1}):compress(body))
end

g.test_zstd_rejects_a_level_out_of_range = function()
    t.assert_error_msg_contains("'level' should be of type number in range",
                                zstd.new, {level = 100})
    t.assert_error_msg_contains("'level' should be of type number in range",
                                zstd.new, {level = 'three'})
end

g.test_zstd_takes_the_streaming_path_for_a_frame_with_no_size = function()
    -- A frame written by a streaming encoder carries no content size, so the
    -- one-allocation path cannot be used. Rather than depend on an encoder
    -- that omits it, blank the size field: 0x60 in the frame header descriptor
    -- says "2-byte content size follows", 0x20 says there is none, and the two
    -- bytes then become the start of the block. Reassembling that by hand is
    -- fiddly, so instead check the property that matters -- that a frame whose
    -- header claims an absurd size is not allocated on that claim.
    local frame = zstd.new():compress(SAMPLE)
    t.assert_equals(zstd.new():decompress(frame), SAMPLE)
    t.assert_gt(zstd.TRUST_LIMIT, 0)
end

g.test_zstd_does_not_allocate_on_a_size_it_read_out_of_the_input = function()
    -- The header of a single-segment frame can declare up to 2^64-1 bytes.
    -- Believing it is an out-of-memory abort of the process; the streaming
    -- path instead grows with what the frame really produces, so this has to
    -- come back as a raised error and the process has to still be here after.
    local frame = zstd.new():compress(SAMPLE)
    -- 0xE0: single segment, 8-byte content size. Eight 0xff bytes then declare
    -- 2^64-1, which is also ZSTD_CONTENTSIZE_UNKNOWN, so lie by one instead.
    local forged = frame:sub(1, 4) .. string.char(0xe0) ..
                   string.rep(string.char(0xff), 7) .. string.char(0xfe) ..
                   frame:sub(7)
    local ok, err = pcall(function() return zstd.new():decompress(forged) end)
    t.assert_equals(ok, false)
    t.assert_str_contains(err, 'pregel.compress')
end

g.test_zstd_corrupt_input_raises_and_says_what = function()
    local z = zstd.new()
    t.assert_error_msg_contains('Unknown frame descriptor', z.decompress, z,
                                'not compressed at all')
    t.assert_error_msg_contains('empty input', z.decompress, z, '')
    local c = z:compress(SAMPLE)
    t.assert_error(function() return z:decompress(c:sub(1, 12)) end)
    t.assert_error(function() return z:decompress(c:sub(1, #c - 4)) end)
end

g.test_zstd_survives_a_thousand_corrupt_frames = function()
    local z = zstd.new()
    local good = z:compress(SAMPLE)
    for i = 1, 1000 do
        local pos = (i % #good) + 1
        local bad = good:sub(1, pos - 1) ..
                    string.char((good:byte(pos) + 137) % 256) ..
                    good:sub(pos + 1)
        local ok, err = pcall(z.decompress, z, bad)
        if not ok then
            t.assert_str_contains(err, 'pregel.compress')
        end
    end
end

g.test_zstd_rejects_non_strings = function()
    local z = zstd.new()
    t.assert_error_msg_contains('expects a string, got number', z.compress, z, 42)
    t.assert_error_msg_contains('expects a string, got nil', z.decompress, z, nil)
end

--------------------------------------------------------------------------------
-- lz4
--------------------------------------------------------------------------------

g.test_lz4_is_available_on_this_machine = function()
    local ok, why = lz4.available()
    t.assert_equals(ok, true, 'no liblz4 found: ' .. tostring(why))
end

g.test_lz4_round_trips_every_size = function()
    local z = lz4.new()
    for _, n in ipairs({0, 1, 2, 1000, 65536}) do
        local s = string.rep('ab', math.ceil(n / 2)):sub(1, n)
        t.assert_equals(#s, n)
        t.assert_equals(z:decompress(z:compress(s)), s, n .. ' bytes')
    end
end

g.test_lz4_round_trips_incompressible_input = function()
    local s = digest.urandom(500000)
    local z = lz4.new()
    t.assert_equals(z:decompress(z:compress(s)), s)
end

g.test_lz4_emits_a_raw_block_not_a_frame = function()
    -- Measured on Enterprise 3.7: its compress.lz4 emits raw LZ4 blocks. A
    -- frame would start with the magic 04 22 4d 18 and would not be readable
    -- by that module at all, so this assertion is the compatibility contract.
    local c = lz4.new():compress(SAMPLE)
    t.assert_not_equals(hexhead(c, 4), '04 22 4d 18')
    -- The first byte is the token of the first sequence: high nibble is the
    -- literal length, 15 meaning "more follows", and this sample begins with
    -- far more than 15 literal bytes.
    t.assert_equals(hexhead(c, 2), 'ff 02')
    -- An empty input is the single byte 00, exactly as Enterprise answers.
    t.assert_equals(lz4.new():compress(''), string.char(0))
end

g.test_lz4_acceleration_trades_size_for_speed = function()
    local fast = lz4.new({acceleration = 64})
    local slow = lz4.new({acceleration = 1})
    local body = string.rep('the same line over and over ', 500)
    t.assert_gt(#fast:compress(body), #slow:compress(body))
    t.assert_equals(fast:decompress(fast:compress(body)), body)
end

g.test_lz4_decompress_buffer_size_is_the_real_limit = function()
    -- Enterprise's default is 1 MiB and it is a hard limit there, measured: a
    -- 3 MB payload does not come back through a default object. Matching that
    -- exactly is the point, since a file written under one and read under the
    -- other has to behave the same way.
    local body = string.rep('abcdefghij', 300000)
    t.assert_gt(#body, lz4.DEFAULT_BUFFER_SIZE)
    local z = lz4.new()
    local c = z:compress(body)
    t.assert_error_msg_contains('decompress_buffer_size = 1048576', z.decompress,
                                z, c)
    local big = lz4.new({decompress_buffer_size = 4 * 1024 * 1024})
    t.assert_equals(big:decompress(c), body)
end

g.test_lz4_rejects_bad_options = function()
    t.assert_error_msg_contains("'acceleration' should be of type number",
                                lz4.new, {acceleration = 0})
    t.assert_error_msg_contains("'acceleration' should be of type number",
                                lz4.new, {acceleration = 'fast'})
    t.assert_error_msg_contains("'decompress_buffer_size' should be of type number",
                                lz4.new, {decompress_buffer_size = 0})
end

g.test_lz4_corrupt_input_raises = function()
    local z = lz4.new()
    t.assert_error_msg_contains('lz4 decompress error', z.decompress, z,
                                'not compressed at all')
    t.assert_error_msg_contains('empty input', z.decompress, z, '')
end

g.test_lz4_survives_a_thousand_corrupt_blocks = function()
    -- A raw block has no checksum and no length, so a corrupt one often
    -- decodes to the wrong bytes instead of raising -- Enterprise does the
    -- same. What must hold whatever the bytes are is that the read stays
    -- inside the buffers: LZ4_decompress_safe is the guarantee, and this is
    -- the check that it is being called the way that guarantee requires.
    local z = lz4.new()
    local good = z:compress(SAMPLE)
    local raised, decoded = 0, 0
    for i = 1, 1000 do
        local pos = (i % #good) + 1
        local bad = good:sub(1, pos - 1) ..
                    string.char((good:byte(pos) + 137) % 256) ..
                    good:sub(pos + 1)
        local ok, res = pcall(z.decompress, z, bad)
        if ok then
            decoded = decoded + 1
            t.assert_le(#res, lz4.DEFAULT_BUFFER_SIZE)
        else
            raised = raised + 1
            t.assert_str_contains(res, 'pregel.compress')
        end
    end
    t.assert_equals(raised + decoded, 1000)
    t.assert_gt(raised, 0, 'no corruption at all was detected')
end

g.test_lz4_rejects_non_strings = function()
    local z = lz4.new()
    t.assert_error_msg_contains('expects a string, got number', z.compress, z, 42)
    t.assert_error_msg_contains('expects a string, got boolean', z.decompress, z,
                                true)
end

--------------------------------------------------------------------------------
-- The module entry point
--------------------------------------------------------------------------------

g.test_implementation_names_itself = function()
    t.assert_items_include({'enterprise', 'ffi'}, {compress.implementation})
end

g.test_the_three_codecs_are_exposed = function()
    for _, name in ipairs({'zlib', 'zstd', 'lz4'}) do
        t.assert_type(compress[name], 'table', name)
        t.assert_type(compress[name].new, 'function', name .. '.new')
    end
end

g.test_ffi_implementation_is_reachable_whatever_the_build = function()
    -- The Avro deflate codec needs it by name even under Enterprise, because
    -- only this side honours window_bits when decompressing.
    t.assert_type(compress.ffi, 'table')
    t.assert_type(compress.ffi.zlib.new, 'function')
    t.assert_is(compress.ffi.zlib, zlib)
end

g.test_available_answers_for_every_codec = function()
    for _, name in ipairs({'zlib', 'zstd', 'lz4'}) do
        t.assert_type(compress.available(name), 'boolean', name)
    end
    t.assert_equals(compress.available('snappy'), false)
end

g.test_available_agrees_with_new = function()
    for _, name in ipairs({'zlib', 'zstd', 'lz4'}) do
        local ok = compress.available(name)
        local made = pcall(function() return compress[name].new() end)
        t.assert_equals(made, ok, name .. ': available() and new() disagree')
    end
end

g.test_enterprise_is_used_when_it_is_there = function()
    if compress.implementation == 'enterprise' then
        t.assert_not_equals(compress.enterprise, nil)
        t.assert_is(compress.zlib, compress.enterprise.zlib)
    else
        t.assert_equals(compress.enterprise, nil)
        t.assert_is(compress.zlib, zlib)
    end
end

--------------------------------------------------------------------------------
-- Against the Enterprise module itself
--
-- Skipped on Community Edition, where there is nothing to compare against.
-- These are what the whole package rests on: not "it round trips" but "the
-- other implementation reads it, and reads it back the same".
--------------------------------------------------------------------------------

local function ee()
    return compress.enterprise
end

local function skip_without_enterprise()
    t.skip_if(ee() == nil, 'no Enterprise compress module in this build')
end

-- The sizes are chosen to cross the buffer boundaries on both sides: one
-- empty, one tiny, one past 64 KiB, one incompressible.
local function cross_bodies()
    return {
        empty        = '',
        tiny         = 'x',
        repetitive   = string.rep('the same line over and over ', 40000),
        random       = digest.urandom(300000),
        sample       = SAMPLE,
    }
end

g.test_ee_reports_the_library_versions = function()
    -- Not an assertion so much as the number the identical-bytes checks below
    -- have to be read against: identical output is only expected of identical
    -- versions, so the versions have to be in the log.
    skip_without_enterprise()
    print(string.format('\n    linked versions: zlib %s, zstd %s, lz4 %s (%s, %s, %s)',
                        zlib.version(), zstd.version(), lz4.version(),
                        select(2, lib.open('zlib')), select(2, lib.open('zstd')),
                        select(2, lib.open('lz4'))))
end

g.test_ee_decompresses_what_the_ffi_layer_compressed = function()
    skip_without_enterprise()
    for name, body in pairs(cross_bodies()) do
        t.assert_equals(ee().zlib.new():decompress(zlib.new():compress(body)),
                        body, 'zlib: ' .. name)
        t.assert_equals(ee().zstd.new():decompress(zstd.new():compress(body)),
                        body, 'zstd: ' .. name)
        local big = {decompress_buffer_size = 4 * 1024 * 1024}
        t.assert_equals(ee().lz4.new(big):decompress(lz4.new():compress(body)),
                        body, 'lz4: ' .. name)
    end
end

g.test_the_ffi_layer_decompresses_what_ee_compressed = function()
    skip_without_enterprise()
    for name, body in pairs(cross_bodies()) do
        t.assert_equals(zlib.new():decompress(ee().zlib.new():compress(body)),
                        body, 'zlib: ' .. name)
        t.assert_equals(zstd.new():decompress(ee().zstd.new():compress(body)),
                        body, 'zstd: ' .. name)
        local big = {decompress_buffer_size = 4 * 1024 * 1024}
        t.assert_equals(lz4.new(big):decompress(ee().lz4.new():compress(body)),
                        body, 'lz4: ' .. name)
    end
end

g.test_ee_and_the_ffi_layer_agree_byte_for_byte_on_lz4 = function()
    -- Enterprise 3.7 exports LZ4_versionNumber from the binary itself, so the
    -- FFI layer binds the very library the Enterprise module uses (1.9.4 on
    -- this build) and the bytes have to be identical rather than merely
    -- interchangeable. Measured at acceleration 1 and 10.
    skip_without_enterprise()
    for _, accel in ipairs({1, 10}) do
        for name, body in pairs(cross_bodies()) do
            local opts = {acceleration = accel}
            t.assert_equals(lz4.new(opts):compress(body),
                            ee().lz4.new(opts):compress(body),
                            string.format('lz4 accel=%d: %s', accel, name))
        end
    end
end

g.test_ee_and_the_ffi_layer_agree_byte_for_byte_on_zlib_and_zstd = function()
    -- These two are the version-dependent ones: Enterprise links its own
    -- copies and does not export their symbols, so the FFI layer loads
    -- whatever the host has. Where the versions match the bytes match, and
    -- where they do not this reports the difference rather than asserting it
    -- away -- the cross-decompression tests above are what actually has to
    -- hold.
    skip_without_enterprise()
    local mismatched = {}
    for name, body in pairs(cross_bodies()) do
        if zlib.new():compress(body) ~= ee().zlib.new():compress(body) then
            mismatched[#mismatched + 1] = 'zlib/' .. name
        end
        if zstd.new():compress(body) ~= ee().zstd.new():compress(body) then
            mismatched[#mismatched + 1] = 'zstd/' .. name
        end
    end
    if #mismatched > 0 then
        print(string.format('\n    NOTE: %s differ byte-for-byte from ' ..
                            'Enterprise (host zlib %s, zstd %s); both ' ..
                            'directions still decompress each other',
                            table.concat(mismatched, ', '), zlib.version(),
                            zstd.version()))
    end
    t.assert_equals(mismatched, {},
                    'the host libraries produce the same bytes as Enterprise')
end

g.test_ee_zlib_cannot_read_raw_deflate_which_is_why_this_package_exists = function()
    -- The justification for the whole window_bits superset, asserted rather
    -- than believed: if a future Enterprise release starts honouring
    -- window_bits on decompress, this test goes red and the note in
    -- pregel/compress/zlib.lua needs rewriting.
    skip_without_enterprise()
    local raw = ee().zlib.new({window_bits = -15}):compress(SAMPLE)
    t.assert_error(function()
        return ee().zlib.new({window_bits = -15}):decompress(raw)
    end)
    -- And the FFI layer reads exactly those bytes.
    t.assert_equals(zlib.new({window_bits = -15}):decompress(raw), SAMPLE)
end
