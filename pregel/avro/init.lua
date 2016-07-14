-- -*- coding: utf-8 -*-
------------------------------------------------------------------------
-- Copyright © 2011-2015, RedJack, LLC.
-- All rights reserved.
--
-- Please see the COPYING file in this distribution for license details.
------------------------------------------------------------------------

local AC  = require('pregel.avro.c')
local ACC = require('pregel.avro.constants')

------------------------------------------------------------------------
-- Copy a bunch of public functions from the submodules.

return {
    ResolvedReader   = AC.ResolvedReader,
    ResolvedWriter   = AC.ResolvedWriter,
    open             = AC.open,
    raw_decode_value = AC.raw_decode_value,
    raw_encode_value = AC.raw_encode_value,
    raw_value        = AC.raw_value,
    wrapped_value    = AC.wrapped_value,

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
