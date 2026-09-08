--- Apache Avro for Tarantool, in pure Lua.
--
--     local avro = require('pregel.avro')
--
--     local sc = avro.schema.parse('{"type":"record","name":"P","fields":[...]}')
--     local bytes = avro.encode(sc, {...})
--     local value = avro.decode(sc, bytes)
--
-- The pieces are usable on their own: `pregel.avro.schema` parses and
-- fingerprints schemas, `pregel.avro.codec` does the binary encoding.

local schema = require('pregel.avro.schema')
local codec  = require('pregel.avro.codec')

local M = {
    schema   = schema,
    codec    = codec,
    -- A stand-in for a JSON null, so that a null value keeps its key in a
    -- record, array or map where a Lua nil would vanish.
    NULL     = schema.NULL,
    parse    = schema.parse,
    encode   = codec.encode,
    decode   = codec.decode,
    validate = codec.validate,
}

return M
