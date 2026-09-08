--- Finding libz / libzstd / liblz4 at run time.
--
-- The codec modules need a handle to a C library. There is no one place to
-- look for it: a Tarantool binary may already carry the symbols (Community
-- Edition links zlib and zstd statically and exports them), macOS keeps the
-- system libraries in the dyld shared cache with no file on disk, Homebrew
-- installs under two different prefixes, and a Linux distribution names the
-- same library `libz.so.1`. So the lookup is a list, and the error names every
-- entry of it rather than saying "not found".
--
--     local lib = require('pregel.compress.lib')
--     local C, origin = lib.open('zstd')
--     print(lib.version('zstd'))     --> '1.5.7'
--
-- The handle is cached per library, so the search runs once per process.
--
-- ## Order
--
-- 1. `ffi.C` -- the running process, when it already exports the library's
--    version symbol. Preferred because it needs no file and cannot skew
--    against whatever the binary itself compresses with.
-- 2. `ffi.load` by plain soname, which is what works on a Linux box with the
--    development package installed and on macOS for anything in the shared
--    cache.
-- 3. Fixed directories: the two Homebrew prefixes, `/usr/local`, and the usual
--    Linux multiarch ones.
-- 4. The directory named by `PREGEL_COMPRESS_LIBDIR`, last, as a fallback for
--    a library installed somewhere none of the above reaches.
--
-- @module pregel.compress.lib

local ffi = require('ffi')

local M = {}

-- Version entry points, used both to probe `ffi.C` and to confirm that a file
-- that loaded is really the library it is named after. Declared here rather
-- than in the codec modules so that the same declaration is not parsed twice.
ffi.cdef[[
    const char *zlibVersion(void);
    unsigned    ZSTD_versionNumber(void);
    const char *ZSTD_versionString(void);
    int         LZ4_versionNumber(void);
]]

local function fail(fmt, ...)
    error('pregel.compress: ' .. string.format(fmt, ...), 0)
end

-- Directories searched for every library, in order. `<opt>` stands for the
-- library's own Homebrew formula prefix, which is where a keg-only formula
-- such as zlib puts its files.
local DIRS = {
    '/opt/homebrew/lib',
    '/opt/homebrew/opt/<opt>/lib',
    '/usr/local/lib',
    '/usr/local/opt/<opt>/lib',
    '/usr/lib',
    '/usr/lib/x86_64-linux-gnu',
    '/usr/lib/aarch64-linux-gnu',
    '/lib/x86_64-linux-gnu',
    '/lib/aarch64-linux-gnu',
}

local SPECS = {
    zlib = {
        pretty  = 'libz',
        opt     = 'zlib',
        probe   = 'zlibVersion',
        sonames = {'z', 'libz.so.1', 'libz.dylib', 'libz.1.dylib'},
        files   = {'libz.so.1', 'libz.1.dylib', 'libz.dylib', 'libz.so'},
    },
    zstd = {
        pretty  = 'libzstd',
        opt     = 'zstd',
        probe   = 'ZSTD_versionNumber',
        sonames = {'zstd', 'libzstd.so.1', 'libzstd.dylib', 'libzstd.1.dylib'},
        files   = {'libzstd.so.1', 'libzstd.1.dylib', 'libzstd.dylib', 'libzstd.so'},
    },
    lz4 = {
        pretty  = 'liblz4',
        opt     = 'lz4',
        probe   = 'LZ4_versionNumber',
        sonames = {'lz4', 'liblz4.so.1', 'liblz4.dylib', 'liblz4.1.dylib'},
        files   = {'liblz4.so.1', 'liblz4.1.dylib', 'liblz4.dylib', 'liblz4.so'},
    },
}

M.SPECS = SPECS

--- Every path this lookup would try, in order, as strings fit for an error
--  message. Split out so that the message and the search cannot drift apart.
local function candidates(spec, opts)
    local out = {}
    if opts.search ~= false then
        for _, name in ipairs(spec.sonames) do
            out[#out + 1] = name
        end
        for _, dir in ipairs(DIRS) do
            dir = dir:gsub('<opt>', spec.opt)
            for _, file in ipairs(spec.files) do
                out[#out + 1] = dir .. '/' .. file
            end
        end
    end
    local libdir = opts.libdir
    if libdir == nil and opts.env ~= false then
        libdir = os.getenv('PREGEL_COMPRESS_LIBDIR')
    end
    if libdir ~= nil and libdir ~= '' then
        for _, file in ipairs(spec.files) do
            out[#out + 1] = libdir .. '/' .. file
        end
    end
    return out
end

--- True when `handle` really exports the library's version symbol.
--
-- Without this a file that merely has the right name -- a stub, the wrong
-- architecture's library reached through a stale symlink -- would be accepted
-- and then blow up on the first real call, far from the lookup that chose it.
local function has_probe(handle, spec)
    local ok = pcall(function() return handle[spec.probe] end)
    return ok
end

local cache = {}

--- Load a compression library.
--
-- @param name 'zlib', 'zstd' or 'lz4'
-- @param opts optional; `search = false` drops the built-in sonames and
--        directories, `env = false` ignores `PREGEL_COMPRESS_LIBDIR`, and
--        `libdir = '<dir>'` names one directly. Passing any of them bypasses
--        the cache, so a test can ask what a differently-configured host would
--        find without poisoning the process.
-- @return the ffi namespace, and a string naming where it came from
-- @raise 'pregel.compress: cannot load libzstd (tried: ...)', listing every
--        candidate
-- @function open
function M.open(name, opts)
    local spec = SPECS[name]
    if spec == nil then
        fail('unknown library %q (known: lz4, zlib, zstd)', tostring(name))
    end
    local cacheable = opts == nil
    if cacheable and cache[name] ~= nil then
        local hit = cache[name]
        return hit[1], hit[2]
    end
    opts = opts or {}

    local tried = {}
    if opts.process ~= false then
        if has_probe(ffi.C, spec) then
            if cacheable then
                cache[name] = {ffi.C, 'the tarantool process itself (ffi.C)'}
            end
            return ffi.C, 'the tarantool process itself (ffi.C)'
        end
        tried[#tried + 1] = string.format('ffi.C (the process exports no %s)',
                                          spec.probe)
    end

    for _, path in ipairs(candidates(spec, opts)) do
        local ok, handle = pcall(ffi.load, path)
        if ok and has_probe(handle, spec) then
            if cacheable then
                cache[name] = {handle, path}
            end
            return handle, path
        end
        tried[#tried + 1] = path
    end

    fail('cannot load %s (tried: %s)', spec.pretty, table.concat(tried, ', '))
end

--- Whether the library can be loaded, without raising when it cannot.
--
-- @param name 'zlib', 'zstd' or 'lz4'
-- @param opts as for open()
-- @return boolean, and on failure the message open() would have raised
-- @function available
function M.available(name, opts)
    local ok, err = pcall(M.open, name, opts)
    if ok then
        return true
    end
    return false, err
end

--- The version of the library that open() resolves to, as a string.
--
-- Reported by the tests that compare this implementation's bytes against the
-- Enterprise module's: identical output is only expected of identical
-- versions, so a mismatch has to be visible rather than inferred.
--
-- @param name 'zlib', 'zstd' or 'lz4'
-- @param opts as for open()
-- @return version string, e.g. '1.2.12' for zlib, '1.5.7' for zstd, '1.10.0'
--         for lz4
-- @raise as open() does
-- @function version
function M.version(name, opts)
    local C = M.open(name, opts)
    if name == 'zlib' then
        return ffi.string(C.zlibVersion())
    elseif name == 'zstd' then
        -- ZSTD_versionString exists from 1.3.0 on; the number always does, so
        -- fall back to it rather than refusing to report anything.
        local ok, s = pcall(function() return ffi.string(C.ZSTD_versionString()) end)
        if ok then
            return s
        end
        local n = tonumber(C.ZSTD_versionNumber())
        return string.format('%d.%d.%d', math.floor(n / 10000),
                             math.floor(n / 100) % 100, n % 100)
    end
    local n = tonumber(C.LZ4_versionNumber())
    return string.format('%d.%d.%d', math.floor(n / 10000),
                         math.floor(n / 100) % 100, n % 100)
end

--- Every candidate open() would try for a library, in order. Exposed so that
--  the error message can be checked against the search rather than retyped.
-- @function candidates
function M.candidates(name, opts)
    local spec = SPECS[name]
    if spec == nil then
        fail('unknown library %q (known: lz4, zlib, zstd)', tostring(name))
    end
    return candidates(spec, opts or {})
end

return M
