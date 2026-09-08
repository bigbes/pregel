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

# Directories that are already luacheck-clean. The 2016 core in pregel/ is
# exempt until the Tarantool 3 port lands -- see lint-all.
LINT_DIRS := $(wildcard test examples)

.PHONY: deps lint lint-all test test-under test-ee

deps:
	tt rocks install luatest
	tt rocks install luacheck

lint:
	$(LUACHECK) $(LINT_DIRS)

# Lints the whole tree, core included. Expected to report warnings until the
# port of pregel/ to Tarantool 3 is finished; kept out of `lint` for that reason.
lint-all:
	$(LUACHECK) .

test:
	$(LUATEST) -v test/

# Runs the suite under $(TARANTOOL) instead of the one baked into the wrapper.
test-under:
	LUA_PATH='$(ROCKS_LUA_PATH)' LUA_CPATH='$(ROCKS_LUA_CPATH)' \
		$(TARANTOOL) $(LUATEST_LUA) -v test/

test-ee:
	$(MAKE) test-under TARANTOOL=$(TARANTOOL_EE)
