--- zlib over the FFI, with the same surface as Enterprise's `compress.zlib`.
--
--     local zlib = require('pregel.compress.zlib')
--     local z = zlib.new({level = 6})
--     z:decompress(z:compress(s)) == s
--
-- `new` takes `level` (0..9, default 6), `mem_level` (1..9, default 8),
-- `strategy` ('default', 'filtered', 'huffman_only', 'rle', 'fixed') and
-- `window_bits`, and the object compresses and decompresses strings. With the
-- defaults the bytes are Enterprise's byte for byte: RFC 1950 framing, `78 9c`
-- and a trailing adler32.
--
-- ## window_bits is the one place this is a superset
--
-- Enterprise honours `window_bits` when compressing (15 zlib, -15 raw RFC
-- 1951, 31 gzip) and ignores it when decompressing -- its `decompress` always
-- expects the zlib frame and always verifies the adler32, so a raw deflate
-- stream it produced itself comes back as `incorrect header check`. Here the
-- option reaches `inflateInit2_` as well, which is what lets the Avro deflate
-- codec read the raw blocks the format stores. Measured on the Enterprise 3.7
-- binary; see test/unit/compress_test.lua.
--
-- Every call builds its own `z_stream`, so an object is safe to share between
-- fibers: nothing is carried from one call to the next.
--
-- @module pregel.compress.zlib

local ffi = require('ffi')

local lib = require('pregel.compress.lib')

local M = {}

-- Named apart from `z_stream` so that this declaration cannot collide with
-- another module's, and laid out exactly as zlib.h has it -- the size is
-- handed to deflateInit2_, which refuses a stream whose size is not its own.
ffi.cdef[[
    typedef struct {
        const uint8_t  *next_in;
        unsigned int    avail_in;
        unsigned long   total_in;
        uint8_t        *next_out;
        unsigned int    avail_out;
        unsigned long   total_out;
        const char     *msg;
        void           *state;
        void           *zalloc;
        void           *zfree;
        void           *opaque;
        int             data_type;
        unsigned long   adler;
        unsigned long   reserved;
    } pregel_z_stream;

    int deflateInit2_(pregel_z_stream *strm, int level, int method,
                      int windowBits, int memLevel, int strategy,
                      const char *version, int stream_size);
    int deflate(pregel_z_stream *strm, int flush);
    int deflateEnd(pregel_z_stream *strm);
    unsigned long deflateBound(pregel_z_stream *strm, unsigned long sourceLen);

    int inflateInit2_(pregel_z_stream *strm, int windowBits,
                      const char *version, int stream_size);
    int inflate(pregel_z_stream *strm, int flush);
    int inflateEnd(pregel_z_stream *strm);
]]

local STREAM_SIZE = ffi.sizeof('pregel_z_stream')

-- zlib compares the caller's sizeof against its own and refuses the stream
-- when they differ, so a layout that drifted would show up as a bare
-- Z_VERSION_ERROR from deflateInit2_ with nothing pointing at the cause. Check
-- it here instead, where the message can say what happened.
local EXPECT_SIZE = ffi.abi('64bit') and 112 or 56
if STREAM_SIZE ~= EXPECT_SIZE then
    error(string.format('pregel.compress.zlib: z_stream is %d bytes on this ' ..
                        'platform, expected %d -- the cdef does not match zlib.h',
                        STREAM_SIZE, EXPECT_SIZE), 0)
end

local Z_OK           = 0
local Z_STREAM_END   = 1
local Z_BUF_ERROR    = -5
local Z_NO_FLUSH     = 0
local Z_FINISH       = 4
local Z_DEFLATED     = 8

local ERRNAME = {
    [-1] = 'Z_ERRNO',    [-2] = 'Z_STREAM_ERROR', [-3] = 'Z_DATA_ERROR',
    [-4] = 'Z_MEM_ERROR', [-5] = 'Z_BUF_ERROR',   [-6] = 'Z_VERSION_ERROR',
    [2]  = 'Z_NEED_DICT',
}

local STRATEGY = {
    ['default']      = 0,
    ['filtered']     = 1,
    ['huffman_only'] = 2,
    ['rle']          = 3,
    ['fixed']        = 4,
}

-- One buffer refill; big enough that an ordinary Avro block is one round trip
-- and small enough that a tiny string does not allocate a megabyte.
local CHUNK = 64 * 1024

local function fail(fmt, ...)
    error('pregel.compress: ' .. string.format(fmt, ...), 0)
end

--- Report a zlib return code together with the stream's own message, which is
--  the part that says *what* was wrong ('incorrect header check').
local function fail_rc(what, rc, strm)
    local detail = ERRNAME[rc] or ('code ' .. tostring(rc))
    if strm ~= nil and strm.msg ~= nil then
        detail = detail .. ': ' .. ffi.string(strm.msg)
    end
    fail('zlib %s error: %s', what, detail)
end

local function opt_int(opts, key, default, min, max)
    local v = opts[key]
    if v == nil then
        return default
    end
    if type(v) ~= 'number' or v ~= math.floor(v) or v < min or v > max then
        fail("options parameter '%s' should be of type number in range [%d..%d]",
             key, min, max)
    end
    return v
end

local WINDOW_HELP = '15 for zlib framing, -15 for raw deflate, 31 for gzip'

local function opt_window_bits(opts)
    local v = opts.window_bits
    if v == nil then
        return 15
    end
    if type(v) ~= 'number' or v ~= math.floor(v) then
        fail("options parameter 'window_bits' should be of type number (%s)",
             WINDOW_HELP)
    end
    if not ((v >= 9 and v <= 15) or (v >= -15 and v <= -9) or
            (v >= 25 and v <= 31)) then
        fail("options parameter 'window_bits' should be in 9..15, -15..-9 or " ..
             '25..31 (%s), got %d', WINDOW_HELP, v)
    end
    return v
end

local zlib_mt = {}
zlib_mt.__index = zlib_mt

--- Run a whole stream through zlib, growing the output as it comes.
--
-- Both directions share this: they differ in which pair of entry points they
-- hand over and in which flush mode they drive.
--
-- The flush mode is not cosmetic. `deflate` wants Z_FINISH, which is what says
-- "this is all the input there will be". `inflate` must *not* get it: with
-- Z_FINISH it insists on finishing in the output space it is handed and
-- answers Z_BUF_ERROR when it cannot, so a stream whose output needed a second
-- buffer -- anything past the first 64 KiB -- came back as 'truncated stream'
-- with nothing truncated about it.
local function pump(strm, step, finish, what, flush, chunk_size)
    local buf = ffi.new('uint8_t[?]', chunk_size)
    local out, n = {}, 0
    while true do
        strm.next_out  = buf
        strm.avail_out = chunk_size
        local rc = step(strm, flush)
        -- avail_out is what is *left*, so the produced count is bounded by the
        -- buffer by construction and never trusts anything from the input.
        local produced = chunk_size - strm.avail_out
        if produced > 0 then
            n = n + 1
            out[n] = ffi.string(buf, produced)
        end
        if rc == Z_STREAM_END then
            break
        end
        if rc ~= Z_OK then
            -- Z_BUF_ERROR here means zlib could make no progress with a whole
            -- empty output buffer in hand, which is a truncated stream rather
            -- than a buffer that needs growing.
            if rc == Z_BUF_ERROR then
                finish(strm)
                fail('zlib %s error: truncated stream', what)
            end
            local msg = strm.msg
            local detail = ERRNAME[rc] or ('code ' .. tostring(rc))
            if msg ~= nil then
                detail = detail .. ': ' .. ffi.string(msg)
            end
            finish(strm)
            fail('zlib %s error: %s', what, detail)
        end
        if produced == 0 then
            finish(strm)
            fail('zlib %s error: no progress', what)
        end
    end
    finish(strm)
    if n == 1 then
        return out[1]
    end
    return table.concat(out)
end

--- Compress a string.
--
-- @param data the string to compress
-- @return the compressed bytes, framed as `window_bits` asks
-- @raise on a non-string argument and on any zlib failure, naming the code and
--        zlib's own message
-- @function compress
function zlib_mt:compress(data)
    if type(data) ~= 'string' then
        fail('zlib compress expects a string, got %s', type(data))
    end
    local C = self._C
    local strm = ffi.new('pregel_z_stream')
    local rc = C.deflateInit2_(strm, self.level, Z_DEFLATED, self.window_bits,
                               self.mem_level, self.strategy, C.zlibVersion(),
                               STREAM_SIZE)
    if rc ~= Z_OK then
        fail_rc('compress init', rc, strm)
    end
    strm.next_in  = ffi.cast('const uint8_t *', data)
    strm.avail_in = #data
    -- deflateBound is only meaningful once the stream is initialised, since it
    -- accounts for the wrapper window_bits asked for.
    local bound = tonumber(C.deflateBound(strm, #data))
    local chunk = math.min(math.max(bound, 64), CHUNK * 16)
    -- `data` stays referenced for the whole call, so the pointer handed to
    -- zlib cannot be collected out from under it.
    local ok, res = pcall(pump, strm, C.deflate, C.deflateEnd, 'compress',
                          Z_FINISH, chunk)
    if not ok then
        error(res, 0)
    end
    return res
end

--- Decompress a string.
--
-- Unlike Enterprise's, this honours `window_bits`, so an object made with
-- `window_bits = -15` reads raw RFC 1951 and one made with 31 reads gzip.
--
-- @param data the compressed bytes
-- @return the original string
-- @raise on a non-string argument, on corrupt or truncated input, and on a
--        checksum mismatch -- never a crash, whatever the bytes are
-- @function decompress
function zlib_mt:decompress(data)
    if type(data) ~= 'string' then
        fail('zlib decompress expects a string, got %s', type(data))
    end
    local C = self._C
    local strm = ffi.new('pregel_z_stream')
    local rc = C.inflateInit2_(strm, self.window_bits, C.zlibVersion(),
                               STREAM_SIZE)
    if rc ~= Z_OK then
        fail_rc('decompress init', rc, strm)
    end
    if #data == 0 then
        C.inflateEnd(strm)
        fail('zlib decompress error: empty input')
    end
    strm.next_in  = ffi.cast('const uint8_t *', data)
    strm.avail_in = #data
    local ok, res = pcall(pump, strm, C.inflate, C.inflateEnd, 'decompress',
                          Z_NO_FLUSH, CHUNK)
    if not ok then
        error(res, 0)
    end
    return res
end

--- A compressor/decompressor pair.
--
-- @param opts optional; `level` 0..9 (default 6), `mem_level` 1..9 (default
--        8), `strategy` one of 'default', 'filtered', 'huffman_only', 'rle',
--        'fixed' (default 'default'), `window_bits` 15 / -15 / 31 (default 15)
-- @return an object with `compress` and `decompress`
-- @raise when an option is out of range or of the wrong type, and when no libz
--        can be loaded -- see pregel.compress.lib for where it looks
-- @function new
function M.new(opts)
    opts = opts or {}
    if type(opts) ~= 'table' then
        fail('zlib.new expects a table of options, got %s', type(opts))
    end
    local strategy = opts.strategy
    if strategy == nil then
        strategy = 'default'
    end
    if type(strategy) ~= 'string' then
        fail("options parameter 'strategy' should be of type string")
    end
    if STRATEGY[strategy] == nil then
        fail("options parameter 'strategy' should be one of 'default', " ..
             "'filtered', 'huffman_only', 'rle', 'fixed', got %q", strategy)
    end
    return setmetatable({
        _C          = lib.open('zlib'),
        level       = opt_int(opts, 'level', 6, 0, 9),
        mem_level   = opt_int(opts, 'mem_level', 8, 1, 9),
        strategy    = STRATEGY[strategy],
        window_bits = opt_window_bits(opts),
    }, zlib_mt)
end

--- Whether a libz can be loaded in this process.
-- @return boolean, and the message new() would raise when it cannot
-- @function available
function M.available()
    return lib.available('zlib')
end

--- The libz version new() would bind, e.g. '1.2.12'.
-- @function version
function M.version()
    return lib.version('zlib')
end

M.STRATEGIES = STRATEGY

return M
