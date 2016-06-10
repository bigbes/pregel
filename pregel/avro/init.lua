-- -*- coding: utf-8 -*-
------------------------------------------------------------------------
-- Copyright © 2011-2015, RedJack, LLC.
-- All rights reserved.
--
-- Please see the COPYING file in this distribution for license details.
------------------------------------------------------------------------

local AC  = require('avro.c')
local ACC = require('avro.constants')
-- local AS  = require('avro.schema')
-- local AW  = require('avro.wrapper')

------------------------------------------------------------------------
-- Copy a bunch of public functions from the submodules.

return {
--[[
    ArraySchema  = AS.ArraySchema,
    EnumSchema   = AS.EnumSchema,
    FixedSchema  = AS.FixedSchema,
    LinkSchema   = AS.LinkSchema,
    MapSchema    = AS.MapSchema,
    RecordSchema = AS.RecordSchema,
    Schema       = AS.Schema,
    UnionSchema  = AS.UnionSchema,

    boolean      = AS.boolean,
    bytes        = AS.bytes,
    double       = AS.double,
    float        = AS.float,
    int          = AS.int,
    long         = AS.long,
    null         = AS.null,
    string       = AS.string,

    array        = AS.array,
    enum         = AS.enum,
    fixed        = AS.fixed,
    link         = AS.link,
    map          = AS.map,
    record       = AS.record,
    union        = AS.union,
]]--
    ResolvedReader   = AC.ResolvedReader,
    ResolvedWriter   = AC.ResolvedWriter,
    open             = AC.open,
    raw_decode_value = AC.raw_decode_value,
    raw_encode_value = AC.raw_encode_value,
    raw_value        = AC.raw_value,
    wrapped_value    = AC.wrapped_value,
--[[
    get_wrapper_class = AW.get_wrapper_class,
    set_wrapper_class = AW.set_wrapper_class,
    Wrapper           = AW.Wrapper,
    ArrayValue        = AW.ArrayValue,
    CompoundValue     = AW.CompoundValue,
    LongValue         = AW.LongValue,
    MapValue          = AW.MapValue,
    RecordValue       = AW.RecordValue,
    ScalarValue       = AW.ScalarValue,
    StringValue       = AW.StringValue,
    UnionValue        = AW.UnionValue,
]]--
    STRING  = ACC.STRING,
    BYTES   = ACC.BYTES,
    INT     = ACC.INT,
    LONG    = ACC.LONG,
    FLOAT   = ACC.FLOAT,
    DOUBLE  = ACC.DOUBLE,
    BOOLEAN = ACC.BOOLEAN,
    NULL    = ACC.NULL,
    RECORD  = ACC.RECORD,
    ENUM    = ACC.ENUM,
    FIXED   = ACC.FIXED,
    MAP     = ACC.MAP,
    ARRAY   = ACC.ARRAY,
    UNION   = ACC.UNION,
    LINK    = ACC.LINK,
}
