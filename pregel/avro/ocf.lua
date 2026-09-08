--- Avro Object Container Files.
--
--     local w = avro.ocf.open('graph.avro', {
--         mode = 'w', schema = sc, codec = 'deflate', block_size = 64 * 1024,
--     })
--     w:append(record)
--     w:close()
--
--     local r = avro.ocf.open('graph.avro', {mode = 'r'})
--     for record in r:records() do ... end
--     r:close()
--
-- A file is the magic 'Obj\1', a map<string, bytes> of metadata carrying at
-- least `avro.schema` and `avro.codec`, a 16-byte sync marker, and then a run
-- of blocks: the record count, the byte length of the (compressed) data, the
-- data, and the sync marker again.
--
-- Codecs: `null` always; `deflate` always, since the inflater is pure Lua and
-- the deflater falls back to stored blocks (see pregel.avro.deflate);
-- `zstandard` wherever `pregel.compress` can reach a libzstd, which is
-- Tarantool Enterprise and any host with the library installed.
-- `codec_available` answers for the running build and names the implementation.
--
-- Reading and writing both stream, so a file larger than memory costs one
-- block. Everything here does blocking file I/O through fio and so must run in
-- a fiber that may yield.
--
-- @module pregel.avro.ocf

local digest = require('digest')
local fio    = require('fio')

local avro_schema = require('pregel.avro.schema')
local codec       = require('pregel.avro.codec')
local compress    = require('pregel.compress')
local deflate     = require('pregel.avro.deflate')

local M = {}

local MAGIC     = 'Obj\1'
local SYNC_SIZE = 16
local DEFAULT_BLOCK_SIZE = 64 * 1024
-- How much of the header to pull before trying to parse it; it grows on demand
-- for schemas that do not fit.
local HEADER_CHUNK = 8192
local READ_CHUNK   = 64 * 1024

local META_SCHEMA = avro_schema.parse({type = 'map', values = 'bytes'})

local function fail(fmt, ...)
    error('avro.ocf: ' .. string.format(fmt, ...), 0)
end

--------------------------------------------------------------------------------
-- Codecs
--------------------------------------------------------------------------------

--- A zstd object, made per block.
--
-- Made rather than kept so that a build with no libzstd can still read and
-- write every other codec, and so that a test can take the library away and
-- see this message. The cost is a table and a cached library lookup against a
-- block of tens of kilobytes, which does not show.
local function zstd_object()
    local ok, res = pcall(function() return compress.zstd.new() end)
    if not ok then
        fail('codec "zstandard" needs a libzstd, which this build cannot ' ..
             'load: %s', tostring(res))
    end
    return res
end

local CODECS = {
    ['null'] = {
        compress   = function(s) return s end,
        decompress = function(s) return s end,
    },
    ['deflate'] = {
        compress   = function(s) return deflate.deflate(s) end,
        decompress = function(s) return deflate.decompress(s) end,
    },
    ['zstandard'] = {
        compress   = function(s) return zstd_object():compress(s) end,
        decompress = function(s) return zstd_object():decompress(s) end,
    },
}

M.codecs = CODECS

--- Whether the codec can be used in this build, and by what.
--
-- 'deflate' is always usable: the inflater is pure Lua and the deflater falls
-- back to stored blocks, so the answer is true even where no library loads --
-- what changes is the second return value. Only 'zstandard' can be
-- unavailable outright.
--
-- @param name codec name
-- @return boolean; false for a codec this package does not know at all
-- @return the implementation, as a string: 'none' for the null codec;
--         '<writer>/<reader>' for deflate, e.g. 'ffi/ffi', 'enterprise/ffi' or
--         'stored/pure-lua'; 'enterprise' or 'ffi' for zstandard, and nil when
--         it is unavailable
-- @function codec_available
function M.codec_available(name)
    if name == 'zstandard' then
        if not compress.available('zstd') then
            return false
        end
        return true, compress.implementation
    end
    if name == 'deflate' then
        local writer, reader = deflate.backend()
        return true, writer .. '/' .. reader
    end
    if CODECS[name] == nil then
        return false
    end
    return true, 'none'
end

local function get_codec(name)
    local c = CODECS[name]
    if c == nil then
        local known = {}
        for k in pairs(CODECS) do
            known[#known + 1] = k
        end
        table.sort(known)
        fail('unsupported codec %q (known: %s)', tostring(name), table.concat(known, ', '))
    end
    return c
end

--------------------------------------------------------------------------------
-- Sources
--------------------------------------------------------------------------------

local function file_source(path)
    local fh, err = fio.open(path, {'O_RDONLY'})
    if fh == nil then
        fail('cannot open %s: %s', path, tostring(err))
    end
    return {
        read  = function(n) return fh:read(n) end,
        close = function() fh:close() end,
    }
end

local function string_source(s)
    local pos = 1
    return {
        read = function(n)
            local chunk = s:sub(pos, pos + n - 1)
            pos = pos + #chunk
            return chunk
        end,
        close = function() end,
    }
end

--------------------------------------------------------------------------------
-- Reader
--------------------------------------------------------------------------------

local reader_mt = {}
reader_mt.__index = reader_mt

--- Bytes still unread in the buffer.
local function available(self)
    return #self._buf - self._pos + 1
end

--- Pull one more chunk from the source. Returns false once it is exhausted.
local function pull(self)
    if self._eof then
        return false
    end
    local more = self._src.read(READ_CHUNK)
    if more == nil or #more == 0 then
        self._eof = true
        return false
    end
    if self._pos > 1 then
        self._buf = self._buf:sub(self._pos)
        self._pos = 1
    end
    self._buf = self._buf .. more
    return true
end

--- Make at least `n` bytes available, if the source has them.
local function ensure(self, n)
    while available(self) < n do
        if not pull(self) then
            return false
        end
    end
    return true
end

local function take(self, n)
    if not ensure(self, n) and available(self) < n then
        fail('unexpected end of file: %d bytes wanted, %d left', n, available(self))
    end
    local s = self._buf:sub(self._pos, self._pos + n - 1)
    self._pos = self._pos + n
    return s
end

local function read_long(self)
    -- A varint is at most ten bytes; a short read at the end of the file is
    -- reported by the codec.
    ensure(self, 10)
    local value, next_pos = codec.get_long(self._buf, self._pos)
    self._pos = next_pos
    return value
end

local function read_header(self)
    if take(self, 4) ~= MAGIC then
        fail('not an Avro object container file: bad magic')
    end
    local want = HEADER_CHUNK
    while true do
        ensure(self, want)
        local ok, meta, next_pos = pcall(codec.decode_value, META_SCHEMA,
                                         self._buf, self._pos)
        if ok then
            self._pos = next_pos
            return meta
        end
        -- The only recoverable failure is a header that has not been read far
        -- enough yet; the message is this package's own, so matching it is safe.
        if self._eof or not tostring(meta):find('unexpected end of input', 1, true) then
            error(meta, 0)
        end
        want = want * 2
    end
end

--- Read the next block into the reader. Returns false at the end of the file.
local function next_block(self)
    if not ensure(self, 1) and available(self) == 0 then
        return false
    end
    local count = read_long(self)
    local size  = read_long(self)
    -- Both are checked here rather than left to the codec: a negative count
    -- made _remaining negative, so the records() loop never reached zero and
    -- decoded past the block's data until the codec ran out of bytes, blaming
    -- an offset deep in the file instead of this header field.
    if count < 0 then
        fail('block at record %d declares a negative record count', self._read)
    end
    if size < 0 then
        fail('block at record %d declares a negative size', self._read)
    end
    local data = take(self, size)
    local sync = take(self, SYNC_SIZE)
    if sync ~= self.sync then
        fail('sync marker mismatch after a block of %d records', count)
    end
    self._block     = self._codec.decompress(data)
    self._bpos      = 1
    self._remaining = count
    return true
end

--- An iterator over the records in the file.
--
-- Reads one block at a time, so the memory cost is a block rather than the
-- file. A record that decodes to null comes back as box.NULL, which keeps the
-- loop going where a nil would end it. Calling this twice does not rewind:
-- both iterators walk on from wherever the reader is.
--
-- @return iterator yielding one record at a time, nil at the end of the file
-- @raise on a truncated file, a sync marker mismatch, or a block header that
--        declares a negative count or size
-- @function records
function reader_mt:records()
    return function()
        while self._remaining == 0 do
            if self._closed then
                return nil
            end
            if not next_block(self) then
                return nil
            end
        end
        local value
        -- decode_value / the resolver rather than decode: a null record must
        -- come back as box.NULL, since a nil would end the iteration.
        if self._resolver ~= nil then
            value, self._bpos = self._resolver(self._block, self._bpos)
        else
            value, self._bpos = codec.decode_value(self.schema, self._block,
                                                   self._bpos)
        end
        self._remaining = self._remaining - 1
        self._read = self._read + 1
        return value
    end
end

--- Alias of records(), so a reader can be walked with the same `for x in
--  r:pairs()` spelling Tarantool's own iterables use.
-- @function pairs
reader_mt.pairs = reader_mt.records

--- Release the underlying file. Idempotent.
--
-- An iterator already running keeps yielding out of the block held in memory
-- and stops when that block is exhausted, rather than at the next call.
--
-- @function close
function reader_mt:close()
    if not self._closed then
        self._closed = true
        self._src.close()
    end
end

local function open_reader(opts)
    local src
    if opts.data ~= nil then
        src = string_source(opts.data)
    else
        src = file_source(opts.path)
    end
    local self = setmetatable({
        _src        = src,
        _buf        = '',
        _pos        = 1,
        _eof        = false,
        _block      = '',
        _bpos       = 1,
        _remaining  = 0,
        _read       = 0,
        _closed     = false,
    }, reader_mt)

    local meta = read_header(self)
    self.sync = take(self, SYNC_SIZE)

    self.metadata = meta
    local schema_json = meta['avro.schema']
    if schema_json == nil then
        fail('the file has no "avro.schema" metadata')
    end
    self.schema_json = schema_json
    -- The schema the file was written with; records are always decoded through
    -- it, resolved against opts.schema when the caller asked for a different
    -- one.
    self.writer_schema = avro_schema.parse(schema_json)
    self.schema = self.writer_schema
    if opts.schema ~= nil then
        self.schema = avro_schema.parse(opts.schema)
        self._resolver = require('pregel.avro.resolve')
            .resolver(self.writer_schema, self.schema)
    end
    -- A file with no codec entry uses the null codec.
    self.codec  = meta['avro.codec'] or 'null'
    self._codec = get_codec(self.codec)
    return self
end

--------------------------------------------------------------------------------
-- Writer
--------------------------------------------------------------------------------

local writer_mt = {}
writer_mt.__index = writer_mt

local function flush_block(self)
    if self._count == 0 then
        return
    end
    local payload = self._codec.compress(table.concat(self._buf))
    local head = {}
    codec.put_long(head, self._count)
    codec.put_long(head, #payload)
    self._fh:write(table.concat(head))
    self._fh:write(payload)
    self._fh:write(self.sync)
    self._blocks = self._blocks + 1
    self._buf    = {}
    self._count  = 0
    self._bytes  = 0
end

--- Append one record. Blocks are flushed once they hold `block_size` bytes of
--  uncompressed data.
--
-- The record is encoded now, so a value that does not fit the schema is caught
-- at this call and not at close().
--
-- @param record the value to write
-- @raise when the writer is closed, and when the record does not fit the
--        schema
-- @function append
function writer_mt:append(record)
    if self._closed then
        fail('the writer is closed')
    end
    local out = {}
    codec.encode_value(self.schema, record, out)
    local encoded = table.concat(out)
    self._buf[#self._buf + 1] = encoded
    self._count = self._count + 1
    self._bytes = self._bytes + #encoded
    self._written = self._written + 1
    if self._bytes >= self.block_size then
        flush_block(self)
    end
end

--- Append every record of a Lua array.
--
-- Not atomic: a record that fails to encode leaves the ones before it written.
--
-- @param records array of values
-- @raise as append() does
-- @function append_all
function writer_mt:append_all(records)
    for i = 1, #records do
        self:append(records[i])
    end
end

--- Flush the current block without closing the file.
--
-- Ends the block early, so calling it per record produces one block per record
-- and compresses far worse. Use it to bound how much is lost if the process
-- dies, not as a matter of course.
--
-- @function flush
function writer_mt:flush()
    flush_block(self)
end

--- Flush the pending block and close the file. Idempotent.
--
-- A file whose writer was never closed is missing its last block, so this is
-- not optional.
--
-- @function close
function writer_mt:close()
    if self._closed then
        return
    end
    flush_block(self)
    self._closed = true
    self._fh:close()
end

local function open_writer(opts)
    if opts.schema == nil then
        fail('a writer needs a schema')
    end
    local sc        = avro_schema.parse(opts.schema)
    local codec_name = opts.codec or 'null'
    local c          = get_codec(codec_name)

    local metadata = {}
    if opts.metadata ~= nil then
        for k, v in pairs(opts.metadata) do
            if type(k) ~= 'string' or type(v) ~= 'string' then
                fail('file metadata must map strings to strings')
            end
            metadata[k] = v
        end
    end
    metadata['avro.schema'] = sc:tojson()
    metadata['avro.codec']  = codec_name

    local sync = opts.sync or digest.urandom(SYNC_SIZE)
    if #sync ~= SYNC_SIZE then
        fail('the sync marker must be exactly %d bytes', SYNC_SIZE)
    end

    local fh, err = fio.open(opts.path, {'O_WRONLY', 'O_CREAT', 'O_TRUNC'},
                             tonumber('644', 8))
    if fh == nil then
        fail('cannot create %s: %s', opts.path, tostring(err))
    end

    local self = setmetatable({
        schema     = sc,
        codec      = codec_name,
        metadata   = metadata,
        sync       = sync,
        block_size = opts.block_size or DEFAULT_BLOCK_SIZE,
        _fh        = fh,
        _codec     = c,
        _buf       = {},
        _count     = 0,
        _bytes     = 0,
        _blocks    = 0,
        _written   = 0,
        _closed    = false,
    }, writer_mt)

    local head = {MAGIC}
    codec.encode_value(META_SCHEMA, metadata, head)
    head[#head + 1] = sync
    fh:write(table.concat(head))
    return self
end

--------------------------------------------------------------------------------
-- Entry points
--------------------------------------------------------------------------------

--- Open an object container file.
--
-- @param path  the file to read or write; may be omitted when `opts.data`
--              carries the bytes of a file to read.
-- @param opts  mode = 'r' (default) or 'w';
--              for 'w': schema (required), codec, block_size, metadata, sync;
--              for 'r': data, to read an in-memory file instead of `path`, and
--              schema, a *reader* schema the records are resolved into.
-- @return a reader (with `records`, `close`, `schema`, `writer_schema`,
--         `metadata`, `codec`) or a writer (with `append`, `append_all`,
--         `flush`, `close`); 'w' truncates an existing file
-- @raise on an unknown mode, a missing path, a file that is not an OCF, an
--        unsupported codec, and a writer with no schema
-- @function open
function M.open(path, opts)
    if type(path) == 'table' and opts == nil then
        opts, path = path, nil
    end
    opts = opts or {}
    local merged = {}
    for k, v in pairs(opts) do
        merged[k] = v
    end
    if path ~= nil then
        merged.path = path
    end
    local mode = merged.mode or 'r'
    if mode == 'r' then
        if merged.path == nil and merged.data == nil then
            fail('reading needs either a path or opts.data')
        end
        return open_reader(merged)
    elseif mode == 'w' then
        if merged.path == nil then
            fail('writing needs a path')
        end
        return open_writer(merged)
    end
    fail('unknown mode %q, expected "r" or "w"', tostring(mode))
end

--- Read every record of a file into an array. Returns the records and the
--  reader's schema.
--
-- Holds the whole file in memory, unlike open():records(). The file is closed
-- whether or not the read raised.
--
-- @param path the file to read
-- @param opts as for open() in mode 'r'
-- @return array of records, and the schema they were decoded into -- the
--         reader schema when opts.schema asked for one, the writer's otherwise
-- @raise as open() and records() do
-- @function read_all
function M.read_all(path, opts)
    local r = M.open(path, opts)
    local out, n = {}, 0
    local ok, err = pcall(function()
        for record in r:records() do
            n = n + 1
            out[n] = record
        end
    end)
    local sc = r.schema
    r:close()
    if not ok then
        error(err, 0)
    end
    return out, sc
end

--- Write an array of records to a file in one call.
--
-- The file is closed whether or not the write raised, so a record that does
-- not fit the schema leaves a valid file holding the records before it.
--
-- @param path the file to create, truncating what is there
-- @param sc the writer schema
-- @param records array of values
-- @param opts as for open() in mode 'w', minus mode and schema
-- @return the writer, already closed
-- @raise as open() and append() do
-- @function write_all
function M.write_all(path, sc, records, opts)
    opts = opts or {}
    local merged = {}
    for k, v in pairs(opts) do
        merged[k] = v
    end
    merged.mode, merged.schema = 'w', sc
    local w = M.open(path, merged)
    local ok, err = pcall(function()
        w:append_all(records)
    end)
    w:close()
    if not ok then
        error(err, 0)
    end
    return w
end

--- The schema of a file, without reading any records.
--
-- Only the header is read, so this is cheap on a large file -- which is what
-- lets pregel.loader check its field-name options before a load starts.
--
-- @param path the file to inspect
-- @param opts as for open() in mode 'r'
-- @return the schema, and the file's metadata map
-- @raise when the file is missing, is not an OCF, or has no avro.schema entry
-- @function schema_of
function M.schema_of(path, opts)
    local r = M.open(path, opts)
    local sc, meta = r.schema, r.metadata
    r:close()
    return sc, meta
end

M.MAGIC       = MAGIC
M.SYNC_SIZE   = SYNC_SIZE
M.META_SCHEMA = META_SCHEMA

return M
