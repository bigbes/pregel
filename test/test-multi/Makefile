# tnt_path       = /Users/blikh/src/work/tarantool/src/tarantool
tnt_path       = tarantool
instance_count = 8

clean_lr:
	rm -f var/*.log
	LUA_PATH="`pwd`\?.lua;`pwd`\?\init.lua;;" ./workers.sh restart $(instance_count)
	sleep 0.5
	$(tnt_path) common.lua

clean_all:
	LUA_PATH="`pwd`\?.lua;`pwd`\?\init.lua;;" ./workers.sh stop $(instance_count)
	rm -rf var/
	LUA_PATH="`pwd`\?.lua;`pwd`\?\init.lua;;" ./workers.sh start $(instance_count)
	sleep 0.5
	$(tnt_path) common.lua load
