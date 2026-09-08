--- LZ4 over the FFI, with the same surface as Enterprise's `compress.lz4`.
--
--     local lz4 = require('pregel.compress.lz4')
--     local z = lz4.new({acceleration = 1, decompress_buffer_size = 1048576})
--     z:decompress(z:compress(s)) == s
--
-- ## What Enterprise actually emits, and why it matters here
--
-- Measured on the Enterprise 3.7 binary: `compress.lz4.new():compress(s)` is a
-- **raw LZ4 block** -- `LZ4_compress_fast` output with nothing around it. Not
-- the LZ4 frame format (no `04 22 4d 18` magic), and not a block with a length
-- prefix either. A 1700-byte sample came back starting `ff 02 68 65 ...`, an
-- empty string came back as the single byte `00`, and the bytes are identical
-- to `LZ4_compress_fast` called directly through the FFI at acceleration 1 and
-- at 10. So that is what this module emits.
--
-- A raw block does not record how large it decompresses to, which is why the
-- Enterprise API has a `decompress_buffer_size` and why its default of 1 MiB
-- is a real limit rather than a hint: measured, a 3 MB payload compressed by
-- the Enterprise module fails to decompress through it with `lz4 decompress
-- error` until the option is raised. The same applies here, with the size said
-- out loud in the message.
--
-- It also means a corrupt block is not always detectable: there is no checksum
-- and no length, so bytes that still parse as literals decode to something
-- wrong rather than raising. Enterprise behaves the same way. Where that
-- matters -- the Avro container format, for one -- the framing above the codec
-- is what catches it.
--
-- @module pregel.compress.lz4

local ffi = require('ffi')

local lib = require('pregel.compress.lib')

local M = {}

ffi.cdef[[
    int LZ4_compress_fast(const char *src, char *dst, int srcSize,
                          int dstCapacity, int acceleration);
    int LZ4_compressBound(int inputSize);
    int LZ4_decompress_safe(const char *src, char *dst, int compressedSize,
                            int dstCapacity);
]]

-- LZ4_MAX_INPUT_SIZE. Past it LZ4_compressBound answers 0, which would
-- otherwise turn into a zero-byte destination buffer.
local MAX_INPUT = 0x7E000000

local DEFAULT_BUFFER_SIZE = 1024 * 1024

local function fail(fmt, ...)
    error('pregel.compress: ' .. string.format(fmt, ...), 0)
end

local lz4_mt = {}
lz4_mt.__index = lz4_mt

--- Compress a string into a raw LZ4 block.
--
-- @param data the string to compress
-- @return the block, byte for byte what Enterprise's `compress.lz4` produces
--         at the same acceleration
-- @raise on a non-string argument, on input past LZ4's 1.9 GiB limit, and when
--        the library refuses the block
-- @function compress
function lz4_mt:compress(data)
    if type(data) ~= 'string' then
        fail('lz4 compress expects a string, got %s', type(data))
    end
    if #data > MAX_INPUT then
        fail('lz4 compress error: input of %d bytes is past the %d-byte limit',
             #data, MAX_INPUT)
    end
    local C = self._C
    local bound = C.LZ4_compressBound(#data)
    if bound <= 0 then
        fail('lz4 compress error: no bound for %d bytes of input', #data)
    end
    local buf = ffi.new('char[?]', bound)
    local n = C.LZ4_compress_fast(data, buf, #data, bound, self.acceleration)
    if n <= 0 then
        -- Only reachable with a destination smaller than the bound, so it
        -- would mean the library and this declaration disagree.
        fail('lz4 compress error: the library returned %d', n)
    end
    return ffi.string(buf, n)
end

--- Decompress a raw LZ4 block.
--
-- The output size is not in the block, so it is bounded by
-- `decompress_buffer_size` -- 1 MiB unless the object was made with another
-- value. `LZ4_decompress_safe` never writes past that buffer and never reads
-- past the input, whatever the block says.
--
-- @param data the block
-- @return the original string
-- @raise on a non-string argument, on empty input, and on a block that is
--        corrupt or larger than `decompress_buffer_size` -- the message names
--        the size, since raising it is the usual fix
-- @function decompress
function lz4_mt:decompress(data)
    if type(data) ~= 'string' then
        fail('lz4 decompress expects a string, got %s', type(data))
    end
    if #data == 0 then
        fail('lz4 decompress error: empty input')
    end
    if #data > 0x7FFFFFFF then
        fail('lz4 decompress error: input of %d bytes is past the 2 GiB limit',
             #data)
    end
    local cap = self.decompress_buffer_size
    local buf = ffi.new('char[?]', cap)
    local n = self._C.LZ4_decompress_safe(data, buf, #data, cap)
    if n < 0 then
        fail('lz4 decompress error: corrupt block, or it decompresses to more ' ..
             'than decompress_buffer_size = %d bytes', cap)
    end
    -- LZ4_decompress_safe is documented never to exceed dstCapacity, so this
    -- cannot fire; it is here because ffi.string with a bad length is a read
    -- past the buffer rather than an error.
    if n > cap then
        fail('lz4 decompress error: the library reported %d bytes in a ' ..
             '%d-byte buffer', n, cap)
    end
    return ffi.string(buf, n)
end

--- A compressor/decompressor pair.
--
-- @param opts optional; `acceleration` >= 1 (default 1; larger is faster and
--        compresses less) and `decompress_buffer_size` (default 1048576, the
--        largest block `decompress` will produce)
-- @return an object with `compress` and `decompress`
-- @raise when an option is out of range or of the wrong type, and when no
--        liblz4 can be loaded -- see pregel.compress.lib for where it looks
-- @function new
function M.new(opts)
    opts = opts or {}
    if type(opts) ~= 'table' then
        fail('lz4.new expects a table of options, got %s', type(opts))
    end
    local accel = opts.acceleration
    if accel == nil then
        accel = 1
    end
    if type(accel) ~= 'number' or accel ~= math.floor(accel) or accel < 1 or
       accel > 0x7FFFFFFF then
        fail("options parameter 'acceleration' should be of type number, at " ..
             'least 1')
    end
    local size = opts.decompress_buffer_size
    if size == nil then
        size = DEFAULT_BUFFER_SIZE
    end
    if type(size) ~= 'number' or size ~= math.floor(size) or size < 1 or
       size > 0x7FFFFFFF then
        fail("options parameter 'decompress_buffer_size' should be of type " ..
             'number in range [1..2147483647]')
    end
    return setmetatable({
        _C                     = lib.open('lz4'),
        acceleration           = accel,
        decompress_buffer_size = size,
    }, lz4_mt)
end

--- Whether a liblz4 can be loaded in this process.
-- @return boolean, and the message new() would raise when it cannot
-- @function available
function M.available()
    return lib.available('lz4')
end

--- The liblz4 version new() would bind, e.g. '1.9.4'.
-- @function version
function M.version()
    return lib.version('lz4')
end

M.DEFAULT_BUFFER_SIZE = DEFAULT_BUFFER_SIZE

return M
