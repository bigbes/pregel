--- Tarantool Enterprise's `compress` module, or an FFI stand-in for it.
--
--     local compress = require('pregel.compress')
--
--     compress.implementation          --> 'enterprise' or 'ffi'
--     compress.available('zstd')       --> true / false
--     local z = compress.zlib.new({level = 6})
--     z:decompress(z:compress(s)) == s
--
-- Under Enterprise the three codec tables are Enterprise's own, so the bytes
-- are whatever that build has always produced and nothing has to be found on
-- disk. Everywhere else they are this package's FFI bindings against the
-- system libz, libzstd and liblz4 -- see `pregel.compress.lib` for the lookup
-- and `PREGEL_COMPRESS_LIBDIR` for the escape hatch.
--
-- ## Two things Enterprise's own module does not have
--
-- `implementation` and `available` are added here; `require('compress')`
-- itself has neither, so a caller that wants to know which side it is talking
-- to has to come through this module.
--
-- `ffi` is the second, and it is not cosmetic. Enterprise's zlib honours
-- `window_bits` when compressing and ignores it when decompressing, so raw
-- RFC 1951 -- what an Avro `deflate` container file stores -- is write-only
-- there. `compress.ffi.zlib` is the FFI binding whatever the build, and it
-- honours the option both ways. Reach for it only when that difference is the
-- point; `compress.zlib` is the drop-in.
--
-- @module pregel.compress

local ffi_zlib = require('pregel.compress.zlib')
local ffi_zstd = require('pregel.compress.zstd')
local ffi_lz4  = require('pregel.compress.lz4')

--- The FFI implementation, reachable under every build.
local FFI = {
    zlib = ffi_zlib,
    zstd = ffi_zstd,
    lz4  = ffi_lz4,
}

--- Whether the FFI implementation can load the library a codec needs.
--
-- @param name 'zlib', 'zstd' or 'lz4'
-- @return boolean; false for a name this module does not know
-- @function ffi.available
function FFI.available(name)
    local mod = FFI[name]
    if type(mod) ~= 'table' or mod.available == nil then
        return false
    end
    return (mod.available())
end

local ok_ee, ee = pcall(require, 'compress')
-- A build could ship a `compress` that is not the Enterprise one; require the
-- three tables to be there before trusting it, rather than failing later at
-- the first `.new`.
if ok_ee then
    ok_ee = type(ee) == 'table' and type(ee.zlib) == 'table' and
            type(ee.zstd) == 'table' and type(ee.lz4) == 'table'
end

local M = {
    --- 'enterprise' when the Enterprise module backs zlib/zstd/lz4 below,
    --  'ffi' when this package's bindings do.
    implementation = ok_ee and 'enterprise' or 'ffi',
    --- The FFI implementation, whichever backs the three below.
    ffi            = FFI,
    --- Enterprise's `compress`, or nil under Community Edition.
    enterprise     = ok_ee and ee or nil,
}

if ok_ee then
    M.zlib, M.zstd, M.lz4 = ee.zlib, ee.zstd, ee.lz4
else
    M.zlib, M.zstd, M.lz4 = ffi_zlib, ffi_zstd, ffi_lz4
end

--- Whether a codec can be used in this build.
--
-- Always true for all three under Enterprise, since the module carries its own
-- libraries. Under the FFI implementation it answers whether the library can
-- actually be loaded, which is the question worth asking before offering a
-- codec to a user.
--
-- @param name 'zlib', 'zstd' or 'lz4'
-- @return boolean; false for a name this module does not know
-- @function available
function M.available(name)
    if type(name) ~= 'string' or type(M[name]) ~= 'table' or
       (name ~= 'zlib' and name ~= 'zstd' and name ~= 'lz4') then
        return false
    end
    if M.implementation == 'enterprise' then
        return true
    end
    return FFI.available(name)
end

return M
