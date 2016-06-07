clean_lr:
	rm var/*.log
	./restart-workers.sh
	sleep 0.5
	tarantool test-master.lua

clean_all:
	./stop-workers.sh && rm -rf var/ && ./start-workers.sh
	sleep 0.5
	tarantool test-master.lua load
