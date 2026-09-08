# Tarantool used by `test-under` (and therefore by `test-ee`).
TARANTOOL    ?= tarantool
TARANTOOL_EE ?= /Users/blikh/data/workspace/sdk/3.7.0-r137/tarantool

ROCKS    := $(CURDIR)/.rocks
LUACHECK := $(ROCKS)/bin/luacheck
LUATEST  := $(ROCKS)/bin/luatest

# The tt-generated .rocks/bin/luatest is a shell wrapper that execs a hard-coded
# tarantool binary, so it cannot be pointed at another one -- `$(TARANTOOL)
# .rocks/bin/luatest` feeds the shell script to the Lua parser and dies. To run
# the suite under a specific tarantool, call luatest's Lua entry point directly
# and hand it the rocks tree through LUA_PATH/LUA_CPATH, which is exactly what
# the wrapper does for its own interpreter.
LUATEST_LUA     := $(ROCKS)/share/tarantool/rocks/luatest/scm-1/bin/luatest
ROCKS_LUA_PATH  := $(ROCKS)/share/tarantool/?.lua;$(ROCKS)/share/tarantool/?/init.lua;;
ROCKS_LUA_CPATH := $(ROCKS)/lib/tarantool/?.so;;

# luatest wipes its VARDIR (default /tmp/t, shared by every luatest on the
# host) at startup, so two checkouts running the suite at once delete each
# other's live servers. Keep it private to this checkout. It has to stay short:
# every server gets a unix socket under it and macOS caps socket paths at 103
# bytes, which a path inside a deep checkout exceeds -- so key a /tmp
# directory by a checksum of the checkout path instead of nesting it inside.
export VARDIR ?= /tmp/pregel-t/$(firstword $(shell printf '%s' '$(CURDIR)' | cksum))

.PHONY: deps lint lint-all test test-under test-ee

deps:
	tt rocks install luatest
	tt rocks install luacheck

# The whole tree, core included: pregel/ carried an exemption while it was
# still 2016 code, and the Tarantool 3 port removed the need for it.
lint lint-all:
	$(LUACHECK) .

test:
	$(LUATEST) -v test/

# Runs the suite under $(TARANTOOL) instead of the one baked into the wrapper.
test-under:
	LUA_PATH='$(ROCKS_LUA_PATH)' LUA_CPATH='$(ROCKS_LUA_CPATH)' \
		$(TARANTOOL) $(LUATEST_LUA) -v test/

test-ee:
	$(MAKE) test-under TARANTOOL=$(TARANTOOL_EE)
