-- -*- coding: utf-8 -*-
------------------------------------------------------------------------
-- Copyright © 2011-2015, RedJack, LLC.
-- All rights reserved.
--
-- Please see the COPYING file in this distribution for license details.
------------------------------------------------------------------------

local ffi = require('ffi')

local avro = ffi.load("avro")

local L   = require('pregel.avro.legacy')
local ACC = require('pregel.avro.constants')

local assert = assert
local getmetatable = getmetatable
local error = error
local ipairs = ipairs
local next = next
local pairs = pairs
local print = print
local setmetatable = setmetatable
local string = string
local table = table
local tonumber = tonumber
local tostring = tostring
local type = type

pcall(ffi.cdef, [[
void free(void *ptr);
]])

ffi.cdef [[
typedef int avro_type_t;
typedef int avro_class_t;

typedef struct avro_obj_t {
    avro_type_t  type;
    avro_class_t class_type;
    volatile int refcount;
} avro_obj_t;

const char *
avro_strerror(void);
]]

local function get_avro_error()
   return nil, ffi.string(avro.avro_strerror())
end

local function avro_error()
   error(ffi.string(avro.avro_strerror()))
end


------------------------------------------------------------------------
-- Avro value interface

-- Note that the avro_value_t definition below does not exactly match
-- the one from the Avro C library.  We need to store an additional
-- field, indicating whether the value should be decref-ed in its
-- release() method.  Ideally, we'd use a wrapper struct like this:
--
-- typedef struct LuaAvroValue {
--     avro_value_t  value;
--     bool  should_decref;
-- } LuaAvroValue;
--
-- Unfortunately, the LuaJIT compiler doesn't currently support
-- JIT-compiling nested structures, and since Avro values will be used
-- in a lot of tight loops, it's important to get those compiled to
-- machine code.
--
-- So to get around this, we're incorporating the extra field into our
-- own definition of avro_value_t.  The beginning of the struct still
-- matches what the library expects, so we should be okay.

ffi.cdef [[
typedef struct avro_value_iface  avro_value_iface_t;

typedef struct avro_value {
    avro_value_iface_t  *iface;
    void  *self;
    bool  should_decref;
} avro_value_t;

typedef avro_obj_t  *avro_schema_t;
typedef struct avro_wrapped_buffer  avro_wrapped_buffer_t;

struct avro_value_iface {
    avro_value_iface_t *(*incref_iface)(avro_value_iface_t *iface);
    void (*decref_iface)(avro_value_iface_t *iface);
    void (*incref)(avro_value_t *value);
    void (*decref)(avro_value_t *value);
    int (*reset)(const avro_value_iface_t *iface, void *self);
    avro_type_t (*get_type)(const avro_value_iface_t *iface, const void *self);
    avro_schema_t (*get_schema)(const avro_value_iface_t *iface, const void *self);
    int (*get_boolean)(const avro_value_iface_t *iface,
                       const void *self, int *out);
    int (*get_bytes)(const avro_value_iface_t *iface,
                     const void *self, const void **buf, size_t *size);
    int (*grab_bytes)(const avro_value_iface_t *iface,
                      const void *self, avro_wrapped_buffer_t *dest);
    int (*get_double)(const avro_value_iface_t *iface,
                      const void *self, double *out);
    int (*get_float)(const avro_value_iface_t *iface,
                     const void *self, float *out);
    int (*get_int)(const avro_value_iface_t *iface,
                   const void *self, int32_t *out);
    int (*get_long)(const avro_value_iface_t *iface,
                    const void *self, int64_t *out);
    int (*get_null)(const avro_value_iface_t *iface,
                    const void *self);
    int (*get_string)(const avro_value_iface_t *iface,
                      const void *self, const char **str, size_t *size);
    int (*grab_string)(const avro_value_iface_t *iface,
                       const void *self, avro_wrapped_buffer_t *dest);
    int (*get_enum)(const avro_value_iface_t *iface,
                    const void *self, int *out);
    int (*get_fixed)(const avro_value_iface_t *iface,
                     const void *self, const void **buf, size_t *size);
    int (*grab_fixed)(const avro_value_iface_t *iface,
                      const void *self, avro_wrapped_buffer_t *dest);
    int (*set_boolean)(const avro_value_iface_t *iface,
                       void *self, int val);
    int (*set_bytes)(const avro_value_iface_t *iface,
                     void *self, void *buf, size_t size);
    int (*give_bytes)(const avro_value_iface_t *iface,
                      void *self, avro_wrapped_buffer_t *buf);
    int (*set_double)(const avro_value_iface_t *iface,
                      void *self, double val);
    int (*set_float)(const avro_value_iface_t *iface,
                     void *self, float val);
    int (*set_int)(const avro_value_iface_t *iface,
                   void *self, int32_t val);
    int (*set_long)(const avro_value_iface_t *iface,
                    void *self, int64_t val);
    int (*set_null)(const avro_value_iface_t *iface, void *self);
    int (*set_string)(const avro_value_iface_t *iface,
                      void *self, char *str);
    int (*set_string_len)(const avro_value_iface_t *iface,
                          void *self, char *str, size_t size);
    int (*give_string_len)(const avro_value_iface_t *iface,
                           void *self, avro_wrapped_buffer_t *buf);
    int (*set_enum)(const avro_value_iface_t *iface,
                    void *self, int val);
    int (*set_fixed)(const avro_value_iface_t *iface,
                     void *self, void *buf, size_t size);
    int (*give_fixed)(const avro_value_iface_t *iface,
                      void *self, avro_wrapped_buffer_t *buf);
    int (*get_size)(const avro_value_iface_t *iface,
                    const void *self, size_t *size);
    int (*get_by_index)(const avro_value_iface_t *iface,
                        const void *self, size_t index,
                        avro_value_t *child, const char **name);
    int (*get_by_name)(const avro_value_iface_t *iface,
                       const void *self, const char *name,
                       avro_value_t *child, size_t *index);
    int (*get_discriminant)(const avro_value_iface_t *iface,
                            const void *self, int *out);
    int (*get_current_branch)(const avro_value_iface_t *iface,
                              const void *self, avro_value_t *branch);
    int (*append)(const avro_value_iface_t *iface,
                  void *self, avro_value_t *child_out, size_t *new_index);
    int (*add)(const avro_value_iface_t *iface,
               void *self, const char *key,
               avro_value_t *child, size_t *index, int *is_new);
    int (*set_branch)(const avro_value_iface_t *iface,
                      void *self, int discriminant,
                      avro_value_t *branch);
};
]]


------------------------------------------------------------------------
-- Forward declarations

ffi.cdef [[
typedef struct LuaAvroResolvedReader {
    avro_value_iface_t  *resolver;
} LuaAvroResolvedReader;

typedef struct LuaAvroResolvedWriter {
    avro_value_iface_t  *resolver;
    avro_value_t  value;
} LuaAvroResolvedWriter;

typedef struct avro_file_reader_t_  *avro_file_reader_t;
typedef struct avro_file_writer_t_  *avro_file_writer_t;

typedef struct LuaAvroDataInputFile {
    avro_file_reader_t   reader;
    avro_schema_t        wschema;
    avro_value_iface_t  *iface;
} LuaAvroDataInputFile;

typedef struct LuaAvroDataOutputFile {
    avro_file_writer_t  writer;
} LuaAvroDataOutputFile;
]]

local avro_schema_t = ffi.typeof([[avro_schema_t]])

local avro_value_t = ffi.typeof([[avro_value_t]])
local avro_value_t_ptr = ffi.typeof([[avro_value_t *]])

local LuaAvroValue
local LuaAvroDataInputFile
local LuaAvroDataOutputFile

------------------------------------------------------------------------
-- Constants

local STRING  = ACC.STRING
local BYTES   = ACC.BYTES
local INT     = ACC.INT
local LONG    = ACC.LONG
local FLOAT   = ACC.FLOAT
local DOUBLE  = ACC.DOUBLE
local BOOLEAN = ACC.BOOLEAN
local NULL    = ACC.NULL
local RECORD  = ACC.RECORD
local ENUM    = ACC.ENUM
local FIXED   = ACC.FIXED
local MAP     = ACC.MAP
local ARRAY   = ACC.ARRAY
local UNION   = ACC.UNION
local LINK    = ACC.LINK


------------------------------------------------------------------------
-- C type pointers

ffi.cdef [[
typedef struct avro_schema_error_t  *avro_schema_error_t;
typedef struct avro_reader_t        *avro_reader_t;
typedef struct avro_writer_t        *avro_writer_t;
]]

local char_t = ffi.typeof('char')
local char_p = ffi.typeof('char *')
local char_p_ptr = ffi.typeof('char *[1]')
local const_char_p_ptr = ffi.typeof('const char *[1]')
local double_ptr = ffi.typeof('double[1]')
local float_ptr = ffi.typeof('float[1]')
local int_ptr = ffi.typeof('int[1]')
local int8_t_ptr = ffi.typeof('int8_t[1]')
local int32_t_ptr = ffi.typeof('int32_t[1]')
local int64_t_ptr = ffi.typeof('int64_t[1]')
local size_t_ptr = ffi.typeof('size_t[1]')
local void_p = ffi.typeof('void *')
local void_p_ptr = ffi.typeof('void *[1]')
local const_void_p_ptr = ffi.typeof('const void *[1]')

local avro_file_reader_t_ptr = ffi.typeof('avro_file_reader_t[1]')
local avro_file_writer_t_ptr = ffi.typeof('avro_file_writer_t[1]')
local avro_schema_t_ptr = ffi.typeof('avro_schema_t[1]')
local avro_schema_error_t_ptr = ffi.typeof('avro_schema_error_t[1]')


------------------------------------------------------------------------
-- Function declarations

-- avro/generic.h

ffi.cdef [[
avro_value_iface_t *
avro_generic_class_from_schema(avro_schema_t schema);

int
avro_generic_value_new(avro_value_iface_t *iface, avro_value_t *dest);
]]

-- avro/io.h

ffi.cdef [[
int
avro_file_reader(const char *path, avro_file_reader_t *reader);

int
avro_file_reader_close(avro_file_reader_t reader);

avro_schema_t
avro_file_reader_get_writer_schema(avro_file_reader_t reader);

int
avro_file_reader_read_value(avro_file_reader_t reader, avro_value_t *dest);

int
avro_file_writer_append_value(avro_file_writer_t writer, avro_value_t *src);

int
avro_file_writer_close(avro_file_writer_t writer);

int
avro_file_writer_create(const char *path, avro_schema_t schema,
                        avro_file_writer_t *writer);

avro_reader_t
avro_reader_memory(const char *buf, int64_t len);

void
avro_reader_memory_set_source(avro_reader_t r, const char *buf, int64_t len);

void
avro_reader_free(avro_reader_t reader);

int
avro_schema_to_json(const avro_schema_t schema, avro_writer_t out);

int
avro_value_read(avro_reader_t reader, avro_value_t *dest);

int
avro_value_write(avro_writer_t writer, avro_value_t *src);

int
avro_value_sizeof(avro_value_t *src, size_t *size);

avro_writer_t
avro_writer_memory(char *buf, int64_t len);

void
avro_writer_memory_set_dest(avro_writer_t r, const char *buf, int64_t len);

void
avro_writer_free(avro_writer_t writer);

int64_t
avro_writer_tell(avro_writer_t writer);
]]

-- avro/resolver.h

ffi.cdef [[
avro_value_iface_t *
avro_resolved_reader_new(avro_schema_t wschema, avro_schema_t rschema);

int
avro_resolved_reader_new_value(const avro_value_iface_t *iface,
                               avro_value_t *value);

void
avro_resolved_reader_set_source(avro_value_t *self, avro_value_t *src);

avro_value_iface_t *
avro_resolved_writer_new(avro_schema_t wschema, avro_schema_t rschema);

int
avro_resolved_writer_new_value(const avro_value_iface_t *iface,
                               avro_value_t *value);

void
avro_resolved_writer_set_dest(avro_value_t *self, avro_value_t *dest);
]]

-- avro/schema.h

ffi.cdef [[
avro_schema_t
avro_schema_array(const avro_schema_t items);

avro_schema_t
avro_schema_array_items(avro_schema_t schema);

avro_schema_t
avro_schema_boolean(void);

avro_schema_t
avro_schema_bytes(void);

void
avro_schema_decref(avro_schema_t schema);

avro_schema_t
avro_schema_double(void);

avro_schema_t
avro_schema_enum(const char *name);

const char *
avro_schema_enum_get(const avro_schema_t schema, int index);

int
avro_schema_enum_get_by_name(const avro_schema_t schema, const char *name);

size_t
avro_schema_enum_size(const avro_schema_t schema);

int
avro_schema_enum_symbol_append(avro_schema_t schema, const char *symbol);

int
avro_schema_equal(avro_schema_t a, avro_schema_t b);

avro_schema_t
avro_schema_fixed(const char *name, int64_t size);

size_t
avro_schema_fixed_size(const avro_schema_t schema);

avro_schema_t
avro_schema_float(void);

int
avro_schema_from_json(const char *json_str, const int32_t json_len,
                      avro_schema_t *schema, avro_schema_error_t *err);

avro_schema_t
avro_schema_incref(avro_schema_t schema);

avro_schema_t
avro_schema_int(void);

avro_schema_t
avro_schema_link(const avro_schema_t target);

avro_schema_t
avro_schema_link_target(avro_schema_t schema);

avro_schema_t
avro_schema_long(void);

avro_schema_t
avro_schema_map(const avro_schema_t values);

avro_schema_t
avro_schema_map_values(avro_schema_t schema);

avro_schema_t
avro_schema_null(void);

avro_schema_t
avro_schema_record(const char *name, const char *namespace);

int
avro_schema_record_field_append(avro_schema_t rec_schema, const char *name,
                                avro_schema_t field_schema);

const char *
avro_schema_record_field_name(const avro_schema_t rec, int index);

avro_schema_t
avro_schema_record_field_get_by_index(const avro_schema_t rec, int index);

size_t
avro_schema_record_size(const avro_schema_t schema);

avro_schema_t
avro_schema_string(void);

const char *
avro_schema_type_name(const avro_schema_t schema);

avro_schema_t
avro_schema_union(void);

int
avro_schema_union_append(avro_schema_t schema, avro_schema_t branch);

avro_schema_t
avro_schema_union_branch(avro_schema_t schema, int discriminant);

avro_schema_t
avro_schema_union_branch_by_name(avro_schema_t schema, int *branch_index,
                                 const char *branch_name);

size_t
avro_schema_union_size(const avro_schema_t schema);
]]

-- avro/value.h

ffi.cdef [[
void
avro_value_incref(avro_value_t *value);

void
avro_value_decref(avro_value_t *value);

void
avro_value_copy_ref(avro_value_t *dest, const avro_value_t *src);

void
avro_value_move_ref(avro_value_t *dest, avro_value_t *src);

int
avro_value_copy(avro_value_t *dest, avro_value_t *src);

int
avro_value_cmp(avro_value_t *val1, avro_value_t *val2);

int
avro_value_equal(avro_value_t *val1, avro_value_t *val2);

uint32_t
avro_value_hash(avro_value_t *value);

int
avro_value_to_json(const avro_value_t *value, int one_line, char **str);
]]

local static_buf = ffi.new(char_t, 65536)
local static_size = 65536
local memory_writer = avro.avro_writer_memory(nil, 0)

------------------------------------------------------------------------
-- Schemas

local Schema_mt = {
    __index = {
        new_raw_value = function(self, value)
            if self.iface == nil then
                self.iface = avro.avro_generic_class_from_schema(self.self)
                if self.iface == nil then
                    avro_error()
                end
            end
            if value ~= nil then
                value:release()
            else
                value = LuaAvroValue()
            end
            local rc = avro.avro_generic_value_new(self.iface, value)
            if rc ~= 0 then
                avro_error()
            end
            value.should_decref = true
            return value
        end,
        raw = function(self)
            return self.self
        end,
        type = function(self)
            return self.self[0].type
        end
    }
}

local function new_schema(self, legacy)
   local result = {
      self = ffi.cast([[avro_schema_t]], self),
      legacy = legacy,
      iface = nil,
   }
   return setmetatable(result, Schema_mt)
end

local function new_raw_schema(schema)
   local legacy, self = L.new_raw_schema(schema)
   return new_schema(self, legacy)
end

local function Schema(json)
   if getmetatable(json) == Schema_mt then
      return Schema_mt
   else
      local legacy, self = L.Schema(json)
      return new_schema(self, legacy)
   end
end


------------------------------------------------------------------------
-- Values

local v_char_p = ffi.new(char_p_ptr)
local v_const_char_p = ffi.new(const_char_p_ptr)
local v_double = ffi.new(double_ptr)
local v_float = ffi.new(float_ptr)
local v_int = ffi.new(int_ptr)
local v_int32 = ffi.new(int32_t_ptr)
local v_int64 = ffi.new(int64_t_ptr)
local v_size_t = ffi.new(size_t_ptr)
local v_const_void_p = ffi.new(const_void_p_ptr)

local function raw_value(v_ud, should_decref)
   local self = LuaAvroValue()
   self:set_raw_value(v_ud, should_decref)
   return self
end

local function raw_encode_value(self, buf, size)
   local writer = avro.avro_writer_memory(buf, size)
   local rc = avro.avro_value_write(writer, self)
   avro.avro_writer_free(writer)
   if rc == 0 then
      return true
   else
      return get_avro_error()
   end
end

local function iterate_array(state, unused)
   -- NOTE: state.next_index is 0-based
   -- Have we reached the end?
   if state.next_index >= state.length then return nil end
   -- Nope.
   local element = LuaAvroValue()
   local rc = state.value.iface.get_by_index(
      state.value.iface, state.value.self,
      state.next_index, element, nil
   )
   if rc ~= 0 then avro_error() end
   state.next_index = state.next_index + 1
   -- Result should be a 1-based index for Lua
   return state.next_index, element
end

local function iterate_map(state, unused)
   -- NOTE: state.next_index is 0-based
   -- Have we reached the end?
   if state.next_index >= state.length then return nil end
   -- Nope.
   local key = ffi.new(const_char_p_ptr)
   local element = LuaAvroValue()
   local rc = state.value.iface.get_by_index(
      state.value.iface, state.value.self,
      state.next_index, element, key
   )
   if rc ~= 0 then avro_error() end
   state.next_index = state.next_index + 1
   return ffi.string(key[0]), element
end

local Value_mt = {
    __index = {
        is_raw_value = true,
        set_raw_value = function(self, ud, should_decref)
            self:release()
            self.iface = ud.iface
            self.self = ud.self
            self.should_decref = should_decref or false
        end,
        get = function(self, index)
            local value_type = self:type()

            if value_type == BOOLEAN then
                if self.iface.get_boolean == nil then
                    error("No implementation for get_boolean")
                end
                local rc = self.iface.get_boolean(self.iface, self.self, v_int)
                if rc ~= 0 then
                    avro_error()
                end
                return v_int[0] ~= 0
            elseif value_type == BYTES then
                local size = ffi.new(int64_t_ptr)
                if self.iface.get_bytes == nil then
                    error("No implementation for get_bytes")
                end
                local rc = self.iface.get_bytes(self.iface, self.self,
                                                v_const_void_p, v_size_t)
                if rc ~= 0 then
                    avro_error()
                end
                return ffi.string(v_const_void_p[0], v_size_t[0])
            elseif value_type == DOUBLE then
                if self.iface.get_double == nil then
                    error("No implementation for get_double")
                end
                local rc = self.iface.get_double(self.iface, self.self, v_double)
                if rc ~= 0 then
                    avro_error()
                end
                return v_double[0]
            elseif value_type == FLOAT then
                if self.iface.get_float == nil then
                    error("No implementation for get_float")
                end
                local rc = self.iface.get_float(self.iface, self.self, v_float)
                if rc ~= 0 then
                    avro_error()
                end
                return v_float[0]
            elseif value_type == INT then
                if self.iface.get_int == nil then
                    error("No implementation for get_int")
                end
                local rc = self.iface.get_int(self.iface, self.self, v_int32)
                if rc ~= 0 then
                    avro_error()
                end
                return v_int32[0]
            elseif value_type == LONG then
                if self.iface.get_long == nil then
                    error("No implementation for get_long")
                end
                local rc = self.iface.get_long(self.iface, self.self, v_int64)
                if rc ~= 0 then
                    avro_error()
                end
                return v_int64[0]
            elseif value_type == NULL then
                if self.iface.get_null == nil then
                    error("No implementation for get_null")
                end
                local rc = self.iface.get_null(self.iface, self.self)
                if rc ~= 0 then
                    avro_error()
                end
                return nil
            elseif value_type == STRING then
                local size = ffi.new(int64_t_ptr)
                if self.iface.get_string == nil then
                    error("No implementation for get_string")
                end
                local rc = self.iface.get_string(self.iface, self.self,
                                                 v_const_char_p, v_size_t)
                if rc ~= 0 then
                    avro_error()
                end
                -- size contains the NULL terminator
                return ffi.string(v_const_char_p[0], v_size_t[0] - 1)
            elseif value_type == ENUM then
                if self.iface.get_enum == nil then
                    error("No implementation for get_enum")
                end
                local rc = self.iface.get_enum(self.iface, self.self, v_int)
                if rc ~= 0 then
                    avro_error()
                end
                local schema = self.iface.get_schema(self.iface, self.self)
                if schema == nil then
                    avro_error()
                end
                local symbol_name = avro.avro_schema_enum_get(schema, v_int[0])
                if symbol_name == nil then
                    avro_error()
                end
                return ffi.string(symbol_name)
            elseif value_type == FIXED then
                local size = ffi.new(int64_t_ptr)
                if self.iface.get_fixed == nil then
                    error("No implementation for get_fixed")
                end
                local rc = self.iface.get_fixed(self.iface, self.self,
                                                v_const_void_p, v_size_t)
                if rc ~= 0 then
                    avro_error()
                end
                return ffi.string(v_const_void_p[0], v_size_t[0])
            elseif value_type == ARRAY then
                if type(index) == "number" then
                    local rc = self.iface.get_size(self.iface, self.self, v_size_t)
                    if rc ~= 0 then
                        return get_avro_error()
                    end
                    if index < 1 or index > v_size_t[0] then
                        error("Index out of bounds")
                    end
                    local element = LuaAvroValue()
                    element.should_decref = false
                    rc = self.iface.get_by_index(self.iface, self.self,
                                                 index - 1, element, nil)
                    if rc ~= 0 then
                        avro_error()
                    end
                    return element
                end
                error("Can only get integer index from array")
            elseif value_type == MAP then
                if type(index) == "string" then
                    local element = LuaAvroValue()
                    element.should_decref = false
                    local rc = self.iface.get_by_name(self.iface, self.self,
                                                      index, element, v_size_t)
                    if rc ~= 0 then
                        return get_avro_error()
                    end
                    if element.self == nil then
                        error("No element named " .. index)
                    else
                        return element, v_size_t[0]
                    end
                elseif type(index) == "number" then
                    local rc = self.iface.get_size(self.iface, self.self, v_size_t)
                    if rc ~= 0 then return get_avro_error() end
                    if index < 1 or index > v_size_t[0] then
                        error("Index out of bounds")
                    end
                    local element = LuaAvroValue()
                    element.should_decref = false
                    local rc = self.iface.get_by_index(self.iface, self.self,
                                                       index - 1, element,
                                                       v_const_char_p)
                    if rc ~= 0 then
                        return get_avro_error()
                    end
                    return element, ffi.string(v_const_char_p[0])
                end
                error("Can only get string or integer index from map")
            elseif value_type == RECORD then
                if type(index) == "string" then
                    local field = LuaAvroValue()
                    field.should_decref = false
                    local rc = self.iface.get_by_name(self.iface, self.self, index, field, nil)
                    if rc ~= 0 then
                        return get_avro_error()
                    end
                    return field
                elseif type(index) == "number" then
                    local field = LuaAvroValue()
                    field.should_decref = false
                    local rc = self.iface.get_by_index(self.iface, self.self, index-1, field, nil)
                    if rc ~= 0 then
                        return get_avro_error()
                    end
                    return field
                end
                error("Can only get string index from record")
            elseif value_type == UNION then
                if type(index) == "string" then
                    local union_schema = self.iface.get_schema(self.iface,
                                                               self.self)
                    local branch_schema = avro.avro_schema_union_branch_by_name(
                        union_schema, v_int, index
                    )
                    if branch_schema == nil then
                        return get_avro_error()
                    end
                    local branch = LuaAvroValue()
                    local rc = self.iface.set_branch(self.iface, self.self,
                                                     v_int[0], branch)
                    if rc ~= 0 then
                        return get_avro_error()
                    end
                    return branch
                elseif type(index) == "number" then
                    local branch = LuaAvroValue()
                    local rc = self.iface.set_branch(self.iface, self.self,
                                                     index - 1, branch)
                    if rc ~= 0 then
                        return get_avro_error()
                    end
                    return branch
                elseif type(index) == "nil" then
                    local branch = LuaAvroValue()
                    branch.should_decref = false
                    local rc = self.iface.get_current_branch(self.iface,
                                                             self.self, branch)
                    if rc ~= 0 then
                        return get_avro_error()
                    end
                    return branch
                end
            else
                error("Don't know how to get from value type " .. tostring(value_type))
            end
        end,
        set = function(self, val)
            local value_type = self:type()
            if value_type == BOOLEAN then
                if self.iface.set_boolean == nil then
                    error("No implementation for set_boolean")
                end
                local rc = self.iface.set_boolean(self.iface, self.self, val)
                if rc ~= 0 then
                    avro_error()
                end
                return
            elseif value_type == BYTES then
                if self.iface.set_bytes == nil then
                    error("No implementation for set_bytes")
                end
                local void_val = ffi.cast(void_p, val)
                local rc = self.iface.set_bytes(self.iface, self.self,
                                                void_val, #val)
                if rc ~= 0 then
                    avro_error()
                end
                return
            elseif value_type == DOUBLE then
                if self.iface.set_double == nil then
                    error("No implementation for set_double")
                end
                local rc = self.iface.set_double(self.iface, self.self, val)
                if rc ~= 0 then
                    avro_error()
                end
                return
            elseif value_type == FLOAT then
                if self.iface.set_float == nil then
                    error("No implementation for set_float")
                end
                local rc = self.iface.set_float(self.iface, self.self, val)
                if rc ~= 0 then
                    avro_error()
                end
                return
            elseif value_type == INT then
                if self.iface.set_int == nil then
                    error("No implementation for set_int")
                end
                local rc = self.iface.set_int(self.iface, self.self, val)
                if rc ~= 0 then
                    avro_error()
                end
                return
            elseif value_type == LONG then
                if self.iface.set_long == nil then
                    error("No implementation for set_long")
                end
                local rc = self.iface.set_long(self.iface, self.self, val)
                if rc ~= 0 then
                    avro_error()
                end
                return
            elseif value_type == NULL then
                if self.iface.set_null == nil then
                    error("No implementation for set_null")
                end
                local rc = self.iface.set_null(self.iface, self.self)
                if rc ~= 0 then
                    avro_error()
                end
                return
            elseif value_type == STRING then
                if self.iface.set_string_len == nil then
                    error("No implementation for set_string_len")
                end
                -- length must include the NULL terminator
                local char_val = ffi.cast(char_p, val)
                local rc = self.iface.set_string_len(self.iface, self.self,
                                                     char_val, #val + 1)
                if rc ~= 0 then
                    avro_error()
                end
                return
            elseif value_type == ENUM then
                if self.iface.set_enum == nil then
                    error("No implementation for set_enum")
                end
                local symbol_value
                if type(val) == "number" then
                    symbol_value = val - 1
                else
                    local schema = self.iface.get_schema(self.iface, self.self)
                    if schema == nil then avro_error() end
                    symbol_value = avro.avro_schema_enum_get_by_name(schema, val)
                    if symbol_value < 0 then
                        error("No symbol named " .. val)
                    end
                end
                local rc = self.iface.set_enum(self.iface, self.self,
                                               symbol_value)
                if rc ~= 0 then
                    avro_error()
                end
                return
            elseif value_type == FIXED then
                if self.iface.set_fixed == nil then
                    error("No implementation for set_fixed")
                end
                local void_val = ffi.cast(void_p, val)
                local rc = self.iface.set_fixed(self.iface, self.self,
                                                void_val, #val)
                if rc ~= 0 then
                    avro_error()
                end
                return
            elseif value_type == MAP then
                if type(val) == "string" then
                    local element = LuaAvroValue()
                    element.should_decref = false
                    local rc = self.iface.add(self.iface, self.self, val,
                                              element, nil, nil)
                    if rc ~= 0 then
                        return get_avro_error()
                    end
                    return element
                end
                return nil, "Can only set string index in map"
            elseif value_type == UNION then
                if type(val) == "string" then
                    local union_schema = self.iface.get_schema(self.iface,
                                                               self.self)
                    local branch_schema = avro.avro_schema_union_branch_by_name(
                        union_schema, v_int, val
                    )
                    if branch_schema == nil then return get_avro_error() end
                    local branch = LuaAvroValue()
                    local rc = self.iface.set_branch(self.iface, self.self,
                                                     v_int[0], branch)
                    if rc ~= 0 then
                        return get_avro_error()
                    end
                    return branch
                elseif type(val) == "number" then
                    local branch = LuaAvroValue()
                    local rc = self.iface.set_branch(self.iface, self.self,
                                                     val - 1, branch)
                    if rc ~= 0 then
                        return get_avro_error()
                    end
                    return branch
                end
                return nil, "Can only set string or integer index in union"
            end
            error("Don't know how to set in value type "..tostring(value_type))
        end,
        append = function(self)
            if self:type() ~= ARRAY then
                error("Can only append to an array")
            end
            if self.iface.append == nil then
                error("No implementation for append")
            end
            local element = LuaAvroValue()
            local rc = self.iface.append(self.iface, self.self, element, nil)
            if rc ~= 0 then
                avro_error()
            end
            return element
        end,
        add = function(self, key)
            if self:type() ~= MAP then
                error("Can only add to a map")
            end
            if self.iface.add == nil then
                error("No implementation for add")
            end
            local element = LuaAvroValue()
            local rc = self.iface.add(self.iface, self.self, key,
                                      element, nil, nil)
            if rc ~= 0 then
                avro_error()
            end
            return element
        end,
        size = function(self)
            local value_type = self:type()
            if value_type == ARRAY then
                if self.iface.get_size == nil then
                    error("no implementation for get_size")
                end
                local rc = self.iface.get_size(self.iface, self.self, v_size_t)
                if rc ~= 0 then
                    avro_error()
                end
                return tonumber(v_size_t[0])
            elseif value_type == MAP then
                local size = ffi.new(int64_t_ptr)
                if self.iface.get_bytes == nil then
                    error("No implementation for get_bytes")
                end
                local rc = self.iface.get_bytes(self.iface, self.self,
                                                v_const_void_p, v_size_t)
                if rc ~= 0 then
                    avro_error()
                end
                return ffi.string(v_const_void_p[0], v_size_t[0])
            else
                error("Can only get size of array or map")
            end
        end,
        discriminant_index = function(self)
            if self:type() ~= UNION then
                error("Can't get discriminant of a non-union value")
            end
            if self.iface.get_discriminant == nil then
                error("No implementation for get_discriminant")
            end
            local rc = self.iface.get_discriminant(self.iface, self.self, v_int)
            if rc ~= 0 then
                avro_error()
            end
            return v_int[0]+1
        end,
        discriminant = function(self)
            local disc = self:discriminant_index()
            local union_schema = self.iface.get_schema(self.iface, self.self)
            if union_schema == nil then
                avro_error()
            end
            local branch = avro.avro_schema_union_branch(union_schema, disc - 1)
            return ffi.string(avro.avro_schema_type_name(branch))
        end,
        encode = function(self)
            local size = self:encoded_size()

            -- Use the static buffer if we can, to save on some mallocs.
            local buf, free_buf
            if size <= static_size then
                buf = static_buf
                free_buf = false
            else
                buf = ffi.new(char_t, size)
                if buf == nil then
                    return nil, "Out of memory"
                end
                free_buf = true
            end

            avro.avro_writer_memory_set_dest(memory_writer, buf, size)
            local rc = avro.avro_value_write(memory_writer, self)

            if rc ~= 0 then
                return get_avro_error()
            else
                local result = ffi.string(buf, size)
                return result
            end
        end,
        encoded_size = function(self)
            local rc = avro.avro_value_sizeof(self, v_size_t)
            if rc ~= 0 then
                avro_error()
            end
            return v_size_t[0]
        end,
        iterate = function(self)
            local value_type = self:type()
            if value_type == ARRAY then
                local rc = self.iface.get_size(self.iface, self.self, v_size_t)
                if rc ~= 0 then
                    avro_error()
                end
                local state = {
                    no_scalar = no_scalar,
                    value = self,
                    next_index = 0,
                    length = v_size_t[0],
                }
                return iterate_array, state, nil
            elseif value_type == MAP then
                local rc = self.iface.get_size(self.iface, self.self, v_size_t)
                if rc ~= 0 then
                    avro_error()
                end
                local state = {
                    no_scalar = no_scalar,
                    value = self,
                    next_index = 0,
                    length = v_size_t[0],
                }
                return iterate_map, state, nil
            end
            error("Can only iterate arrays and maps")
        end,
        type = function(self)
            return self.iface.get_type(self.iface, self.self)
        end,
        hash = function(self)
            return avro.avro_value_hash(self)
        end,
        schema_name = function(self)
            local schema = self.iface.get_schema(self.iface, self.self)
            return ffi.string(avro.avro_schema_type_name(schema))
        end,
        reset = function(self)
            if self.iface.reset == nil then
                error("No implementation for Value:reset()")
            end
            self.iface.reset(self.iface, self.self)
        end,
        set_source = function(self, src)
            avro.avro_resolved_reader_set_source(self, src)
        end,
        set_dest = function(self, src)
            avro.avro_resolved_writer_set_dest(self, src)
        end,
        copy_from = function(self, src)
            local rc = avro.avro_value_copy(self, src)
            if rc ~= 0 then
                avro_error()
            end
        end,
        to_json = function(self)
            local rc = avro.avro_value_to_json(self, true, v_char_p)
            if rc ~= 0 then
                avro_error()
            end
            local result = ffi.string(v_char_p[0])
            ffi.C.free(v_char_p[0])
            return result
        end,
        cmp = function(self, other)
            return avro.avro_value_cmp(self, other)
        end,
        release = function(self)
            if self.should_decref and self.self ~= nil then
                avro.avro_value_decref(self)
            end
            self.iface = nil
            self.self = nil
            self.should_decref = false
        end,
        -- Fills in the contents of an Avro value from a pure-Lua AST. For
        -- scalars, we expect a compatible Lua scalar value.  For maps and
        -- records, we expect a table. For arrays, we expect an array-like
        -- table. For unions, we expect a scalar nil (if the union contains a
        -- null schema), or a single-element table whose key is the name of one
        -- of the union schemas.
        set_from_ast = function(self, ast)
            local value_type = self:type()

            if false then
                assert(false)
            elseif value_type == BOOLEAN or
                   value_type == NULL    or
                   value_type == ENUM    then
                self:set(ast)
            elseif value_type == BYTES  or
                   value_type == STRING or
                   value_type == FIXED  then
                self:set(tostring(ast))
            elseif value_type == DOUBLE or
                   value_type == FLOAT  or
                   value_type == INT    or
                   value_type == LONG   then
                self:set(tonumber(ast))
            elseif value_type == ARRAY then
                self:reset()
                for i, v in ipairs(ast) do
                    local element = self:append()
                    element:set_from_ast(v)
                end
            elseif value_type == MAP then
                self:reset()
                for k,v in pairs(ast) do
                    local element = self:add(k)
                    element:set_from_ast(v)
                end
            elseif value_type == RECORD then
                for k, v in pairs(ast) do
                    local field = assert(self:get(k))
                    field:set_from_ast(v)
                end
            elseif value_type == UNION then
                if ast == nil then
                    assert(self:set("null"))
                    self:get():set_from_ast(nil)
                else
                    local k, v = next(ast)
                    if not k then
                        error("Union AST must have exactly one element")
                    end
                    assert(self:set(k))
                    self:get():set_from_ast(v)
                end
            end
        end
    },
    __eq = function(self, other)
        if other == nil then
            return false
        end
        local eq = avro.avro_value_equal(self, other)
        return eq ~= 0
    end,
    __lt = function(self, other)
        local cmp = avro.avro_value_cmp(self, other)
        return cmp < 0
    end,
    __le = function(self, other)
        local cmp = avro.avro_value_cmp(self, other)
        return cmp <= 0
    end,
    __gc = function(self)
        if self.self and self.should_decref then
            print("Warning: Freeing non-released value ", self.self)
            self:release()
        end
    end
}

Value_mt.__tostring = Value_mt.__index.to_json

LuaAvroValue = ffi.metatype([[avro_value_t]], Value_mt)

------------------------------------------------------------------------
-- ResolvedReaders

local ResolvedReader_mt = {
    __index = {
        new_raw_value = function(self)
            local value = LuaAvroValue()
            local rc = avro.avro_resolved_reader_new_value(self.resolver, value)
            if rc ~= 0 then
                avro_error()
            end
            value.should_decref = true
            return value
        end,
    },
    __gc = function(self)
        if self.resolver ~= nil then
            self.resolver.decref_iface(self.resolver)
            self.resolver = nil
        end
    end
}

local LuaAvroResolvedReader = ffi.metatype([[LuaAvroResolvedReader]], ResolvedReader_mt)

local function ResolvedReader(wschema, rschema)
   local resolver = LuaAvroResolvedReader()
   wschema = wschema:raw_schema().self
   rschema = rschema:raw_schema().self
   resolver.resolver = avro.avro_resolved_reader_new(wschema, rschema)
   if resolver.resolver == nil then
       return get_avro_error()
   end
   return resolver
end

------------------------------------------------------------------------
-- ResolvedWriters

local memory_reader = avro.avro_reader_memory(nil, 0)

local function raw_decode_value(resolver, buf, size, dest)
   avro.avro_reader_memory_set_source(memory_reader, buf, size)
   avro.avro_resolved_writer_set_dest(resolver.value, dest)
   local rc = avro.avro_value_read(memory_reader, resolver.value)
   if rc == 0 then
      return true
   else
      return get_avro_error()
   end
end

local ResolvedWriter_mt = {
    __index = {
        new_raw_value = function(self)
            local value = LuaAvroValue()
            local rc = avro.avro_resolved_writer_new_value(self.resolver, value)
            if rc ~= 0 then
                avro_error()
            end
            value.should_decref = true
            return value
        end,
        decode = function(self, buf, dest)
            return raw_decode_value(self, buf, #buf, dest)
        end,
    },
    __gc = function(self)
        if self.value.self ~= nil then
            avro.avro_value_decref(self.value)
            self.value.iface = nil
            self.value.self = nil
        end

        if self.resolver ~= nil then
            self.resolver.decref_iface(self.resolver)
            self.resolver = nil
        end
    end
}

local LuaAvroResolvedWriter = ffi.metatype([[LuaAvroResolvedWriter]], ResolvedWriter_mt)

local function ResolvedWriter(wschema, rschema)
   local resolver = LuaAvroResolvedWriter()
   wschema = wschema:raw_schema().self
   rschema = rschema:raw_schema().self
   resolver.resolver = avro.avro_resolved_writer_new(wschema, rschema)
   if resolver.resolver == nil then return get_avro_error() end
   local rc = avro.avro_resolved_writer_new_value(resolver.resolver, resolver.value)
   if rc ~= 0 then return get_avro_error() end
   return resolver
end

------------------------------------------------------------------------
-- Data files

local DataInputFile_mt = {
    __index = {
        schema_json = function(self)
            avro.avro_writer_memory_set_dest(memory_writer, static_buf, static_size)
            local rc = avro.avro_schema_to_json(self.wschema, memory_writer)
            if rc ~= 0 then
                avro_error()
            end
            local length = avro.avro_writer_tell(memory_writer)
            return ffi.string(static_buf, length)
        end,
        read_raw = function(self, value)
            if not value then
                value = LuaAvroValue()
                local rc = avro.avro_generic_value_new(self.iface, value)
                if rc ~= 0 then
                    avro_error()
                end
                value.should_decref = true

                local rc = avro.avro_file_reader_read_value(self.reader, value)
                if rc ~= 0 then
                    value:release()
                    return get_avro_error()
                end
                return value
            end

            local rc = avro.avro_file_reader_read_value(self.reader, value)
            if rc ~= 0 then
                return get_avro_error()
            end
            return value
        end,
        close = function(self)
            if self.reader ~= nil then
                avro.avro_file_reader_close(self.reader)
                self.reader = nil
            end
            self.wschema = nil
            if self.iface ~= nil then
                if self.iface.decref_iface ~= nil then
                    self.iface.decref_iface(self.iface)
                end
                self.iface = nil
            end
        end
    }
}

DataInputFile_mt.__gc = DataInputFile_mt.__index.close

LuaAvroDataInputFile = ffi.metatype([[LuaAvroDataInputFile]], DataInputFile_mt)

local DataOutputFile_mt = {
    __index = {
        write_raw = function(self, value)
            local rc = avro.avro_file_writer_append_value(self.writer, value)
            if rc ~= 0 then
                avro_error()
            end
        end,
        close = function(self)
            if self.writer ~= nil then
                avro.avro_file_writer_close(self.writer)
                self.writer = nil
            end
        end
    }
}

DataOutputFile_mt.__gc = DataOutputFile_mt.__index.close
LuaAvroDataOutputFile = ffi.metatype([[LuaAvroDataOutputFile]], DataOutputFile_mt)

local function new_input_file(reader)
   local l_reader = LuaAvroDataInputFile()
   l_reader.reader = reader
   l_reader.wschema = avro.avro_file_reader_get_writer_schema(reader)
   l_reader.iface = avro.avro_generic_class_from_schema(l_reader.wschema)
   return l_reader
end

local function open(path, mode, schema)
    mode = mode or "r"
    if mode == "r" then
        local reader = ffi.new(avro_file_reader_t_ptr)
        local rc = avro.avro_file_reader(path, reader)
        if rc ~= 0 then
            avro_error()
        end
        return new_input_file(reader[0])
    elseif mode == "w" then
        local writer = ffi.new(avro_file_writer_t_ptr)
        schema = schema:raw_schema().self
        local rc = avro.avro_file_writer_create(path, schema, writer)
        if rc ~= 0 then
            avro_error()
        end
        return LuaAvroDataOutputFile(writer[0])
    end
    error("Invalid mode " .. mode)
end

return {
    new_raw_schema   = new_raw_schema,
    raw_value        = raw_value,
    raw_encode_value = raw_encode_value,
    raw_decode_value = raw_decode_value,
    Schema           = Schema,
    ResolvedReader   = ResolvedReader,
    ResolvedWriter   = ResolvedWriter,
    open             = open,
}
