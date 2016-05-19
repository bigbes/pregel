clean_lr:
	rm var/*.log
	./restart-workers.sh
	tarantool test-master.lua

clean_all:
	./stop-workers.sh && rm -rf var/ && ./start-workers.sh
	tarantool test-master.lua load
