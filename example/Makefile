# TNT_PATH = /Users/blikh/src/work/tarantool/src/tarantool
TNT_PATH := tarantool
COUNT    := 4

clean_lr:
	rm -f var/*.log
	LUA_PATH="`pwd`\?.lua;`pwd`\?\init.lua;;" ./workers.sh restart $(COUNT)
	sleep 0.5
	$(TNT_PATH) test-master.lua

clean_all:
	LUA_PATH="`pwd`\?.lua;`pwd`\?\init.lua;;" ./workers.sh stop $(COUNT)
	rm -rf var/
	LUA_PATH="`pwd`\?.lua;`pwd`\?\init.lua;;" ./workers.sh start $(COUNT)
	sleep 0.5
	$(TNT_PATH) test-master.lua load

all: clean_lr
