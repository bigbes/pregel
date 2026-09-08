--- Apache Avro for Tarantool, in pure Lua.
--
--     local avro = require('pregel.avro')
--
--     local sc = avro.schema.parse('{"type":"record","name":"P","fields":[...]}')
--     local bytes = avro.encode(sc, {...})
--     local value = avro.decode(sc, bytes)
--
--     local w = avro.ocf.open('graph.avro', {mode = 'w', schema = sc})
--     w:append({...}); w:close()
--     for record in avro.ocf.open('graph.avro'):records() do ... end
--
-- The pieces are usable on their own: `pregel.avro.schema` parses and
-- fingerprints schemas, `pregel.avro.codec` does the binary encoding,
-- `pregel.avro.resolve` reads data written with one schema through another,
-- `pregel.avro.ocf` the container format and `pregel.avro.deflate` the raw
-- DEFLATE its `deflate` codec needs. The compression itself comes from
-- `pregel.compress`, which is Enterprise's module or an FFI stand-in for it.
--
-- This module only re-exports; every function it names is documented where it
-- is defined.
--
-- @module pregel.avro

local schema  = require('pregel.avro.schema')
local codec   = require('pregel.avro.codec')
local ocf     = require('pregel.avro.ocf')
local deflate = require('pregel.avro.deflate')
local resolve = require('pregel.avro.resolve')

local M = {
    schema   = schema,
    codec    = codec,
    ocf      = ocf,
    deflate  = deflate,
    resolve  = resolve,
    -- A reusable decoder for one (writer schema, reader schema) pair.
    resolver = resolve.resolver,
    skip     = codec.skip,
    -- Whether a container-file codec works in this build, and what backs it.
    -- Worth having at the top level because it is the one thing about Avro
    -- here that differs between two Tarantool binaries.
    codec_available = ocf.codec_available,
    -- A stand-in for a JSON null, so that a null value keeps its key in a
    -- record, array or map where a Lua nil would vanish.
    NULL     = schema.NULL,
    parse    = schema.parse,
    encode   = codec.encode,
    decode   = codec.decode,
    validate = codec.validate,
}

return M
