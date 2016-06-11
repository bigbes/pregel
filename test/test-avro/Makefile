tnt_path = /Users/blikh/src/work/tarantool/src/tarantool
tnt_ctl  = `which tarantoolctl`

clean_lr:
	rm var/*.log
	./restart-workers.sh
	sleep 0.5
	$(tnt_path) common.lua

clean_all:
	./stop-workers.sh && rm -rf var/ && ./start-workers.sh
	sleep 0.5
	$(tnt_path) common.lua load
