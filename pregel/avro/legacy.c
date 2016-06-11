/* -*- coding: utf-8 -*-
 * ----------------------------------------------------------------------
 * Copyright © 2010-2015, RedJack, LLC.
 * All rights reserved.
 *
 * Please see the COPYING file in this distribution for license details.
 * ----------------------------------------------------------------------
 */

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include <avro.h>
#include <lauxlib.h>
#include <lua.h>
#include <lualib.h>

#define MT_AVRO_VALUE "avro:AvroValue"

typedef struct _LuaAvroValue
{
	avro_value_t  value;
	bool  should_decref;
} LuaAvroValue;

int
lua_avro_push_value(lua_State *L, avro_value_t *value, bool should_decref)
{
	LuaAvroValue  *l_value;

	l_value = lua_newuserdata(L, sizeof(LuaAvroValue));
	l_value->value = *value;
	l_value->should_decref = should_decref;
	luaL_getmetatable(L, MT_AVRO_VALUE);
	lua_setmetatable(L, -2);
	return 1;
}


/*-----------------------------------------------------------------------
 * Lua access — data
 */

/**
 * The string used to identify the AvroValue class's metatable in the
 * Lua registry.
 */

static int
lua_avro_error(lua_State *L)
{
	lua_pushstring(L, avro_strerror());
	return lua_error(L);
}

#define check(call) \
	do { \
		int __rc; \
		__rc = call; \
		if (__rc != 0) { \
			return lua_avro_error(L); \
		} \
	} while (0)


/*-----------------------------------------------------------------------
 * Lua access — schemas
 */

/**
 * The string used to identify the AvroSchema class's metatable in the
 * Lua registry.
 */

#define MT_AVRO_SCHEMA "avro:AvroSchema"

typedef struct _LuaAvroSchema
{
	avro_schema_t  schema;
	avro_value_iface_t	*iface;
} LuaAvroSchema;

int
lua_avro_push_schema(lua_State *L, avro_schema_t schema)
{
	LuaAvroSchema  *l_schema;
	l_schema = lua_newuserdata(L, sizeof(LuaAvroSchema));
	l_schema->schema = avro_schema_incref(schema);
	l_schema->iface = NULL;
	luaL_getmetatable(L, MT_AVRO_SCHEMA);
	lua_setmetatable(L, -2);
	return 1;
}


avro_schema_t
lua_avro_get_schema(lua_State *L, int index)
{
	lua_pushliteral(L, "raw_schema");
	lua_gettable(L, index);
	lua_pushvalue(L, index);
	lua_call(L, 1, 1);
	LuaAvroSchema  *l_schema = luaL_checkudata(L, -1, MT_AVRO_SCHEMA);
	lua_pop(L, 1);
	return l_schema->schema;
}

avro_schema_t
lua_avro_get_raw_schema(lua_State *L, int index)
{
	LuaAvroSchema  *l_schema = luaL_checkudata(L, index, MT_AVRO_SCHEMA);
	return l_schema->schema;
}


static int
l_new_raw_schema(lua_State *L)
{
	avro_schema_t  schema = lua_touserdata(L, 1);
	if (schema == NULL) {
		lua_pushliteral(L, "Cannot create NULL schema wrapper");
		return lua_error(L);
	}
	lua_avro_push_schema(L, schema);
	lua_pushlightuserdata(L, schema);
	return 2;
}


/**
 * Creates a new AvroValue for the given schema.
 */

static int
l_schema_new_raw_value(lua_State *L)
{
	LuaAvroSchema  *l_schema = luaL_checkudata(L, 1, MT_AVRO_SCHEMA);
	if (l_schema->iface == NULL) {
		l_schema->iface = avro_generic_class_from_schema(l_schema->schema);
		if (l_schema->iface == NULL) {
			lua_pushstring(L, avro_strerror());
			return lua_error(L);
		}
	}

	if (lua_gettop(L) >= 2) {
		LuaAvroValue  *l_value = luaL_checkudata(L, 2, MT_AVRO_VALUE);
		if (l_value->should_decref && l_value->value.self != NULL) {
			avro_value_decref(&l_value->value);
		}
		check(avro_generic_value_new(l_schema->iface, &l_value->value));
		l_value->should_decref = true;
		lua_pushvalue(L, 2);
	} else {
		avro_value_t  value;
		check(avro_generic_value_new(l_schema->iface, &value));
		lua_avro_push_value(L, &value, true);
	}
	return 1;
}


/**
 * Returns the type of an AvroSchema instance.
 */

static int
l_schema_type(lua_State *L)
{
	avro_schema_t  schema = lua_avro_get_raw_schema(L, 1);
	lua_pushnumber(L, avro_typeof(schema));
	return 1;
}


/**
 * Returns the name of an AvroSchema instance.
 */

static int
l_schema_name(lua_State *L)
{
	avro_schema_t  schema = lua_avro_get_raw_schema(L, 1);
	lua_pushstring(L, avro_schema_type_name(schema));
	return 1;
}


/**
 * Finalizes an AvroSchema instance.
 */

static int
l_schema_gc(lua_State *L)
{
	LuaAvroSchema  *l_schema = luaL_checkudata(L, 1, MT_AVRO_SCHEMA);
	if (l_schema->schema != NULL) {
		avro_schema_decref(l_schema->schema);
		l_schema->schema = NULL;
	}
	if (l_schema->iface != NULL) {
		avro_value_iface_decref(l_schema->iface);
		l_schema->iface = NULL;
	}
	return 0;
}


/**
 * Creates a new AvroSchema instance from a JSON schema string.
 */

static int
l_schema_new(lua_State *L)
{
	if (lua_isstring(L, 1)) {
		size_t	json_len;
		const char	*json_str = lua_tolstring(L, 1, &json_len);
		avro_schema_t  schema;

		/* First check for the primitive types */
		if (strcmp(json_str, "boolean") == 0) {
			schema = avro_schema_boolean();
		}

		else if (strcmp(json_str, "bytes") == 0) {
			schema = avro_schema_bytes();
		}

		else if (strcmp(json_str, "double") == 0) {
			schema = avro_schema_double();
		}

		else if (strcmp(json_str, "float") == 0) {
			schema = avro_schema_float();
		}

		else if (strcmp(json_str, "int") == 0) {
			schema = avro_schema_int();
		}

		else if (strcmp(json_str, "long") == 0) {
			schema = avro_schema_long();
		}

		else if (strcmp(json_str, "null") == 0) {
			schema = avro_schema_null();
		}

		else if (strcmp(json_str, "string") == 0) {
			schema = avro_schema_string();
		}

		/* Otherwise assume it's JSON */

		else {
			avro_schema_error_t  schema_error;
			check(avro_schema_from_json(json_str, json_len, &schema, &schema_error));
		}

		lua_avro_push_schema(L, schema);
		avro_schema_decref(schema);
		lua_pushlightuserdata(L, schema);
		return 2;
	}

	if (lua_isuserdata(L, 1)) {
		if (lua_getmetatable(L, 1)) {
			lua_getfield(L, LUA_REGISTRYINDEX, MT_AVRO_SCHEMA);
			if (lua_rawequal(L, -1, -2)) {
				/* This is already a schema object, so just return it. */
				lua_pop(L, 2);	/* remove both metatables */
				LuaAvroSchema  *l_schema = lua_touserdata(L, 1);
				lua_pushlightuserdata(L, l_schema->schema);
				return 2;
			}
		}
	}

	lua_pushliteral(L, "Invalid input to Schema function");
	return lua_error(L);
}


/*-----------------------------------------------------------------------
 * Lua access — module
 */

static const luaL_Reg mod_methods[] = {
	{"Schema", l_schema_new},
	{"new_raw_schema", l_new_raw_schema},
	{NULL, NULL}
};

static const luaL_Reg schema_methods[] = {
	{"name", l_schema_name},
	{"new_raw_value", l_schema_new_raw_value},
	{"type", l_schema_type},
	{NULL, NULL}
};

int
luaopen_pregel_avro_legacy(lua_State *L)
{
	/* AvroSchema metatable */

	luaL_newmetatable(L, MT_AVRO_SCHEMA);
	lua_createtable(L, 0, sizeof(schema_methods) / sizeof(luaL_reg) - 1);
	luaL_register(L, NULL, schema_methods);
	lua_setfield(L, -2, "__index");
	lua_pushcfunction(L, l_schema_gc);
	lua_setfield(L, -2, "__gc");
	lua_pop(L, 1);

	luaL_register(L, "avro.legacy", mod_methods);
	return 1;
}
