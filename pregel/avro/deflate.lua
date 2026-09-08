--- Raw DEFLATE (RFC 1951) for the Avro "deflate" OCF codec.
--
-- Avro stores deflate blocks without the zlib wrapper. Tarantool's
-- `compress.zlib` almost covers that but not quite:
--
--   * compression honours `window_bits = -15`, so it emits raw RFC 1951 (that
--     is the same byte stream as its framed output minus the two-byte header
--     and the four-byte adler32 trailer);
--   * decompression ignores `window_bits` -- it always expects the zlib frame
--     and verifies the trailing adler32. Re-framing a raw block for it is
--     impossible, because the adler32 is computed over the *decompressed*
--     bytes, which is exactly what is not known yet.
--
-- So inflate is implemented here, in Lua, and it is the only read path. That
-- also means a deflate-compressed Avro file is readable under Community
-- Tarantool, where `compress.zlib` does not exist at all.
--
-- Compression prefers `compress.zlib` when it is present. Without it the
-- encoder falls back to RFC 1951 stored blocks, which are valid, uncompressed
-- deflate that any Avro implementation reads back.
--
-- @module pregel.avro.deflate

local bit = require('bit')
local ffi = require('ffi')

local M = {}

local ok_zlib, zlib = pcall(require, 'compress.zlib')

--- True when a real compressor is available. Reading never needs it.
M.has_zlib = ok_zlib

local function fail(fmt, ...)
    error('avro.deflate: ' .. string.format(fmt, ...), 0)
end

--------------------------------------------------------------------------------
-- Inflate
--------------------------------------------------------------------------------

local MAXBITS = 15

-- RFC 1951 section 3.2.5.
local LENGTH_BASE = {
    3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 15, 17, 19, 23, 27, 31, 35, 43, 51, 59,
    67, 83, 99, 115, 131, 163, 195, 227, 258,
}
local LENGTH_EXTRA = {
    0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3,
    4, 4, 4, 4, 5, 5, 5, 5, 0,
}
local DIST_BASE = {
    1, 2, 3, 4, 5, 7, 9, 13, 17, 25, 33, 49, 65, 97, 129, 193, 257, 385, 513,
    769, 1025, 1537, 2049, 3073, 4097, 6145, 8193, 12289, 16385, 24577,
}
local DIST_EXTRA = {
    0, 0, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8,
    9, 9, 10, 10, 11, 11, 12, 12, 13, 13,
}
-- The order the code-length code lengths appear in a dynamic block header.
local CLEN_ORDER = {
    16, 17, 18, 0, 8, 7, 9, 6, 10, 5, 11, 4, 12, 3, 13, 2, 14, 1, 15,
}

--- Build the canonical Huffman decoding tables of puff.c: `count[len]` is how
--  many codes have that length, `symbol` lists the symbols in code order.
local function construct(lengths, n)
    local count = {}
    for len = 0, MAXBITS do
        count[len] = 0
    end
    for i = 0, n - 1 do
        count[lengths[i]] = count[lengths[i]] + 1
    end
    if count[0] == n then
        return { count = count, symbol = {}, incomplete = true }
    end
    -- Check for an over-subscribed set; a set may be left incomplete, which is
    -- legal for the single-code distance tree.
    local left = 1
    for len = 1, MAXBITS do
        left = left * 2 - count[len]
        if left < 0 then
            fail('over-subscribed Huffman code')
        end
    end
    local offs = { [1] = 0 }
    for len = 1, MAXBITS - 1 do
        offs[len + 1] = offs[len] + count[len]
    end
    local symbol = {}
    for i = 0, n - 1 do
        if lengths[i] ~= 0 then
            symbol[offs[lengths[i]]] = i
            offs[lengths[i]] = offs[lengths[i]] + 1
        end
    end
    return { count = count, symbol = symbol, incomplete = left > 0 }
end

local FIXED_LIT, FIXED_DIST

local function fixed_tables()
    if FIXED_LIT ~= nil then
        return FIXED_LIT, FIXED_DIST
    end
    local lengths = {}
    for i = 0, 143 do lengths[i] = 8 end
    for i = 144, 255 do lengths[i] = 9 end
    for i = 256, 279 do lengths[i] = 7 end
    for i = 280, 287 do lengths[i] = 8 end
    FIXED_LIT = construct(lengths, 288)
    for i = 0, 29 do lengths[i] = 5 end
    FIXED_DIST = construct(lengths, 30)
    return FIXED_LIT, FIXED_DIST
end

--- Decompress a raw RFC 1951 stream.
--
-- Pure Lua, so it works on any Tarantool build, and it is the only read path
-- -- see the note at the top of the module on why compress.zlib cannot be used
-- for this. Stops at the block marked final and ignores whatever follows.
--
-- @param data raw deflate bytes, without a zlib or gzip wrapper
-- @return the decompressed string
-- @raise on a truncated stream and on any malformed block: an over-subscribed
--        Huffman code, an invalid symbol, a back-reference pointing before the
--        start of the output, block type 3
-- @function inflate
function M.inflate(data)
    if type(data) ~= 'string' then
        fail('inflate expects a string, got %s', type(data))
    end
    local n = #data
    local pos = 1
    local bitbuf, bitcnt = 0, 0

    -- Never asked for more than 13 bits at once, so the accumulator stays well
    -- inside the 32-bit range LuaJIT's bit operations work on.
    local function bits(need)
        while bitcnt < need do
            if pos > n then
                fail('unexpected end of the deflate stream')
            end
            bitbuf = bit.bor(bitbuf, bit.lshift(data:byte(pos), bitcnt))
            pos = pos + 1
            bitcnt = bitcnt + 8
        end
        local value = bit.band(bitbuf, bit.lshift(1, need) - 1)
        bitbuf = bit.rshift(bitbuf, need)
        bitcnt = bitcnt - need
        return value
    end

    local function decode(h)
        local code, first, index = 0, 0, 0
        local count = h.count
        for len = 1, MAXBITS do
            code = bit.bor(code, bits(1))
            local cnt = count[len]
            if code - first < cnt then
                return h.symbol[index + (code - first)]
            end
            index = index + cnt
            first = bit.lshift(first + cnt, 1)
            code = bit.lshift(code, 1)
        end
        fail('invalid Huffman code in the deflate stream')
    end

    local cap = 4096
    local out = ffi.new('uint8_t[?]', cap)
    local olen = 0

    local function reserve(extra)
        if olen + extra <= cap then
            return
        end
        local ncap = cap
        repeat
            ncap = ncap * 2
        until olen + extra <= ncap
        local nout = ffi.new('uint8_t[?]', ncap)
        ffi.copy(nout, out, olen)
        out, cap = nout, ncap
    end

    local function block(lit, dist)
        while true do
            local sym = decode(lit)
            if sym < 256 then
                reserve(1)
                out[olen] = sym
                olen = olen + 1
            elseif sym == 256 then
                return
            else
                sym = sym - 256
                if sym > 29 then
                    fail('invalid length symbol %d', sym + 256)
                end
                local len = LENGTH_BASE[sym] + bits(LENGTH_EXTRA[sym])
                local dsym = decode(dist)
                if dsym == nil or dsym > 29 then
                    fail('invalid distance symbol')
                end
                local d = DIST_BASE[dsym + 1] + bits(DIST_EXTRA[dsym + 1])
                if d > olen then
                    fail('deflate back-reference points before the output')
                end
                reserve(len)
                local src = olen - d
                for i = 0, len - 1 do
                    out[olen + i] = out[src + i]
                end
                olen = olen + len
            end
        end
    end

    local function stored()
        -- Stored blocks start on a byte boundary.
        bitbuf, bitcnt = 0, 0
        if pos + 3 > n then
            fail('truncated stored block header')
        end
        local len  = data:byte(pos) + data:byte(pos + 1) * 256
        local nlen = data:byte(pos + 2) + data:byte(pos + 3) * 256
        pos = pos + 4
        if bit.band(len + nlen, 0xffff) ~= 0xffff or len + nlen ~= 0xffff then
            fail('stored block length does not match its complement')
        end
        if pos + len - 1 > n then
            fail('truncated stored block')
        end
        reserve(len)
        if len > 0 then
            ffi.copy(out + olen, ffi.cast('const char *', data) + (pos - 1), len)
            olen = olen + len
        end
        pos = pos + len
    end

    local function dynamic()
        local nlen  = bits(5) + 257
        local ndist = bits(5) + 1
        local ncode = bits(4) + 4
        if nlen > 286 or ndist > 30 then
            fail('too many length or distance codes')
        end
        local lengths = {}
        for i = 0, 18 do
            lengths[i] = 0
        end
        for i = 1, ncode do
            lengths[CLEN_ORDER[i]] = bits(3)
        end
        local clen = construct(lengths, 19)
        local index = 0
        while index < nlen + ndist do
            local sym = decode(clen)
            if sym == nil then
                fail('invalid code-length symbol')
            end
            if sym < 16 then
                lengths[index] = sym
                index = index + 1
            else
                local value, repeats
                if sym == 16 then
                    if index == 0 then
                        fail('no previous code length to repeat')
                    end
                    value = lengths[index - 1]
                    repeats = 3 + bits(2)
                elseif sym == 17 then
                    value, repeats = 0, 3 + bits(3)
                else
                    value, repeats = 0, 11 + bits(7)
                end
                if index + repeats > nlen + ndist then
                    fail('code-length repeat runs past the end of the table')
                end
                for _ = 1, repeats do
                    lengths[index] = value
                    index = index + 1
                end
            end
        end
        if lengths[256] == 0 then
            fail('the dynamic block has no end-of-block code')
        end
        local lit_lengths, dist_lengths = {}, {}
        for i = 0, nlen - 1 do
            lit_lengths[i] = lengths[i]
        end
        for i = 0, ndist - 1 do
            dist_lengths[i] = lengths[nlen + i]
        end
        return construct(lit_lengths, nlen), construct(dist_lengths, ndist)
    end

    repeat
        local final = bits(1)
        local btype = bits(2)
        if btype == 0 then
            stored()
        elseif btype == 1 then
            block(fixed_tables())
        elseif btype == 2 then
            block(dynamic())
        else
            fail('invalid deflate block type 3')
        end
    until final == 1

    return ffi.string(out, olen)
end

--------------------------------------------------------------------------------
-- Deflate
--------------------------------------------------------------------------------

--- RFC 1951 stored blocks: no compression, but a valid deflate stream. Used
--  when `compress.zlib` is not available.
--
-- Grows the data by five bytes per 65535-byte block. An empty input produces
-- one (empty, final) block, because a zero-byte deflate stream is not legal.
--
-- @param data string to wrap
-- @return a raw deflate stream that inflates back to `data`
-- @function store
local function store(data)
    local out = {}
    local n = #data
    local pos = 1
    repeat
        local chunk = math.min(n - pos + 1, 65535)
        local final = (pos + chunk > n) and 1 or 0
        out[#out + 1] = string.char(final)
        out[#out + 1] = string.char(bit.band(chunk, 0xff),
                                    bit.band(bit.rshift(chunk, 8), 0xff))
        local nlen = bit.bxor(chunk, 0xffff)
        out[#out + 1] = string.char(bit.band(nlen, 0xff),
                                    bit.band(bit.rshift(nlen, 8), 0xff))
        if chunk > 0 then
            out[#out + 1] = data:sub(pos, pos + chunk - 1)
        end
        pos = pos + chunk
    until final == 1
    return table.concat(out)
end

M.store = store

--- Compress to a raw RFC 1951 stream.
--
-- Falls back to store() where `compress.zlib` is missing, so the output is
-- always readable but is not always smaller than the input -- check
-- M.has_zlib if that matters.
--
-- @param data string to compress
-- @return raw deflate bytes, with no zlib wrapper
-- @raise when `data` is not a string
-- @function deflate
function M.deflate(data)
    if type(data) ~= 'string' then
        fail('deflate expects a string, got %s', type(data))
    end
    if not ok_zlib then
        return store(data)
    end
    -- window_bits = -15 asks zlib for raw deflate, so nothing has to be
    -- stripped off afterwards.
    return zlib.new({window_bits = -15}):compress(data)
end

return M
