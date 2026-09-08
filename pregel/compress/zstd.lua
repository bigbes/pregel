--- Zstandard over the FFI, with the same surface as Enterprise's
--  `compress.zstd`.
--
--     local zstd = require('pregel.compress.zstd')
--     local z = zstd.new({level = 3})
--     z:decompress(z:compress(s)) == s
--
-- The output is a standard Zstandard frame -- magic `28 b5 2f fd`, and the
-- content size in the header, since the one-shot `ZSTD_compress` knows it.
-- That is exactly what the Enterprise module emits, measured on 3.7.
--
-- ## Why there are two decompression paths
--
-- A frame written by this module (or by Enterprise, or by the `zstd` command)
-- carries its uncompressed size, so decompressing is one allocation and one
-- call. A frame written by a streaming encoder need not, and then the size has
-- to be discovered by decoding. Both are handled; which one ran is invisible
-- from the outside.
--
-- The size in the header is a number that came out of the input, so it is
-- never allocated on trust: past a threshold the streaming path takes over and
-- memory then grows with what the frame actually produces rather than with
-- what it claims. A frame declaring 2^60 bytes costs a rejected call, not the
-- process.
--
-- @module pregel.compress.zstd

local ffi = require('ffi')

local lib = require('pregel.compress.lib')

local M = {}

ffi.cdef[[
    size_t ZSTD_compress(void *dst, size_t dstCapacity,
                         const void *src, size_t srcSize, int level);
    size_t ZSTD_decompress(void *dst, size_t dstCapacity,
                           const void *src, size_t compressedSize);
    size_t ZSTD_compressBound(size_t srcSize);
    unsigned ZSTD_isError(size_t code);
    const char *ZSTD_getErrorName(size_t code);
    unsigned long long ZSTD_getFrameContentSize(const void *src, size_t srcSize);
    int ZSTD_minCLevel(void);
    int ZSTD_maxCLevel(void);

    typedef struct pregel_ZSTD_DStream_s pregel_ZSTD_DStream;
    typedef struct { const void *src; size_t size; size_t pos; } pregel_ZSTD_inBuffer;
    typedef struct { void *dst; size_t size; size_t pos; } pregel_ZSTD_outBuffer;

    pregel_ZSTD_DStream *ZSTD_createDStream(void);
    size_t ZSTD_initDStream(pregel_ZSTD_DStream *zds);
    size_t ZSTD_decompressStream(pregel_ZSTD_DStream *zds,
                                 pregel_ZSTD_outBuffer *output,
                                 pregel_ZSTD_inBuffer *input);
    size_t ZSTD_freeDStream(pregel_ZSTD_DStream *zds);
    size_t ZSTD_DStreamOutSize(void);
]]

-- ZSTD_CONTENTSIZE_UNKNOWN and ZSTD_CONTENTSIZE_ERROR, spelled as zstd.h does:
-- the two largest unsigned 64-bit values.
local CONTENTSIZE_UNKNOWN = ffi.cast('unsigned long long', -1)
local CONTENTSIZE_ERROR   = ffi.cast('unsigned long long', -2)

-- Above this a declared content size is not allocated up front; the streaming
-- decoder runs instead and the output grows with what the frame really emits.
-- 64 MiB is far past any Avro block and far short of a size worth trusting
-- from an untrusted file.
local TRUST_LIMIT = 64 * 1024 * 1024

-- Fallback bounds for `level`, used when the library predates ZSTD_minCLevel
-- (1.3.6). The upper one is zstd's ZSTD_MAX_CLEVEL, the lower is what
-- Enterprise reports for the same option.
local FALLBACK_MIN_LEVEL = -131072
local FALLBACK_MAX_LEVEL = 22

local function fail(fmt, ...)
    error('pregel.compress: ' .. string.format(fmt, ...), 0)
end

local function check(C, rc, what)
    if C.ZSTD_isError(rc) ~= 0 then
        fail('zstd %s error: %s', what, ffi.string(C.ZSTD_getErrorName(rc)))
    end
    return rc
end

local function level_range(C)
    local ok, lo = pcall(function() return tonumber(C.ZSTD_minCLevel()) end)
    if not ok then
        return FALLBACK_MIN_LEVEL, FALLBACK_MAX_LEVEL
    end
    local ok2, hi = pcall(function() return tonumber(C.ZSTD_maxCLevel()) end)
    return lo, ok2 and hi or FALLBACK_MAX_LEVEL
end

local zstd_mt = {}
zstd_mt.__index = zstd_mt

--- Compress a string into one Zstandard frame.
--
-- @param data the string to compress
-- @return the frame, magic `28 b5 2f fd`, carrying the uncompressed size
-- @raise on a non-string argument and on any libzstd failure, naming zstd's
--        own message
-- @function compress
function zstd_mt:compress(data)
    if type(data) ~= 'string' then
        fail('zstd compress expects a string, got %s', type(data))
    end
    local C = self._C
    local bound = tonumber(C.ZSTD_compressBound(#data))
    if bound == nil or bound <= 0 then
        fail('zstd compress error: input of %d bytes is too large', #data)
    end
    local buf = ffi.new('uint8_t[?]', bound)
    local n = check(C, C.ZSTD_compress(buf, bound, data, #data, self.level),
                    'compress')
    return ffi.string(buf, tonumber(n))
end

--- Decompress by decoding, without believing the frame header's size.
local function stream_decompress(C, data)
    local zds = C.ZSTD_createDStream()
    if zds == nil then
        fail('zstd decompress error: cannot create a decompression stream')
    end
    -- ffi.gc rather than a plain call at the end: an error raised in the middle
    -- of the loop must not leak the context.
    zds = ffi.gc(zds, C.ZSTD_freeDStream)

    check(C, C.ZSTD_initDStream(zds), 'decompress')
    local chunk = tonumber(C.ZSTD_DStreamOutSize())
    if chunk == nil or chunk <= 0 then
        chunk = 128 * 1024
    end
    local buf = ffi.new('uint8_t[?]', chunk)
    local input = ffi.new('pregel_ZSTD_inBuffer',
                          {src = ffi.cast('const void *', data), size = #data, pos = 0})
    local out, n = {}, 0
    local output = ffi.new('pregel_ZSTD_outBuffer')
    while true do
        output.dst  = buf
        output.size = chunk
        output.pos  = 0
        local rc = check(C, C.ZSTD_decompressStream(zds, output, input),
                         'decompress')
        local produced = tonumber(output.pos)
        if produced > 0 then
            n = n + 1
            out[n] = ffi.string(buf, produced)
        end
        if tonumber(rc) == 0 then
            -- The frame ended. Anything after it is a second frame; the
            -- Enterprise module stops at the first too.
            break
        end
        if tonumber(input.pos) >= #data and produced == 0 then
            fail('zstd decompress error: truncated frame')
        end
    end
    ffi.gc(zds, nil)
    C.ZSTD_freeDStream(zds)
    if n == 1 then
        return out[1]
    end
    return table.concat(out)
end

--- Decompress one Zstandard frame.
--
-- @param data the frame
-- @return the original string
-- @raise on a non-string argument, on a frame that is not Zstandard, and on
--        corrupt or truncated input -- never a crash, whatever the bytes are
-- @function decompress
function zstd_mt:decompress(data)
    if type(data) ~= 'string' then
        fail('zstd decompress expects a string, got %s', type(data))
    end
    if #data == 0 then
        fail('zstd decompress error: empty input')
    end
    local C = self._C
    local declared = C.ZSTD_getFrameContentSize(data, #data)
    if declared == CONTENTSIZE_UNKNOWN or declared == CONTENTSIZE_ERROR or
       declared > TRUST_LIMIT then
        return stream_decompress(C, data)
    end
    local size = tonumber(declared)
    if size == 0 then
        -- ZSTD_decompress refuses a zero-byte destination, and an empty frame
        -- is a legal thing to have written.
        return ''
    end
    local buf = ffi.new('uint8_t[?]', size)
    local n = check(C, C.ZSTD_decompress(buf, size, data, #data), 'decompress')
    n = tonumber(n)
    -- A frame whose header lied low would have been refused by ZSTD_decompress
    -- as dstSize_tooSmall; this is the belt for the other direction.
    if n > size then
        fail('zstd decompress error: produced %d bytes into a %d-byte buffer',
             n, size)
    end
    return ffi.string(buf, n)
end

--- A compressor/decompressor pair.
--
-- @param opts optional; `level`, default 3, in the range the linked libzstd
--        reports (-131072..22 on a current one), as Enterprise documents
-- @return an object with `compress` and `decompress`
-- @raise when `level` is out of range or of the wrong type, and when no
--        libzstd can be loaded -- see pregel.compress.lib for where it looks
-- @function new
function M.new(opts)
    opts = opts or {}
    if type(opts) ~= 'table' then
        fail('zstd.new expects a table of options, got %s', type(opts))
    end
    local C = lib.open('zstd')
    local lo, hi = level_range(C)
    local level = opts.level
    if level == nil then
        level = 3
    end
    if type(level) ~= 'number' or level ~= math.floor(level) or
       level < lo or level > hi then
        fail("options parameter 'level' should be of type number in range " ..
             '[%d..%d]', lo, hi)
    end
    return setmetatable({_C = C, level = level}, zstd_mt)
end

--- Whether a libzstd can be loaded in this process.
-- @return boolean, and the message new() would raise when it cannot
-- @function available
function M.available()
    return lib.available('zstd')
end

--- The libzstd version new() would bind, e.g. '1.5.7'.
-- @function version
function M.version()
    return lib.version('zstd')
end

M.TRUST_LIMIT = TRUST_LIMIT

return M
