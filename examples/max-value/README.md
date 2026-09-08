# max-value

Every vertex ends up holding the largest value that reaches it.

The smallest Pregel program worth writing, and the one to read first. A vertex
takes the largest value it has been told about; if that is larger than what it
held, it stores it and tells its out-neighbours. Nothing fixes the number of
supersteps — the job stops when no vertex has improved and no message is in
flight, which on the graph below takes twelve supersteps.

Which way that runs matters, and on a directed graph the two readings differ.
Values travel along out-edges, so what a vertex ends up with is the largest
value held by a vertex that can reach it — its ancestors, itself included — and
not the largest it can reach. Measured on the graph below: against a plain
sequential fixpoint over "the largest value that reaches it", 0 of 75879
vertices disagree with what the job stores; against "the largest value
reachable from it", 41981 do. Only 47676 of the 75879 hold the global maximum,
which is also why "the largest value in its component" is not it either — the
giant weak component has 75877 vertices in it.

The example also carries an aggregator, `max_seen`: every vertex reports the
value it holds on every superstep, the workers reduce that locally, and the
master merges the three copies into one number. That is the answer, readable
from the master without touching a worker.

## Files

| file | what it is |
| --- | --- |
| `config.yaml` | the Tarantool 3 cluster config: one master and three workers, the `pregel` user and its `lua_call` privileges, and `roles_cfg` for each instance |
| `instances.yml` | the four instance names, for `tt` |
| `tt.yaml` | makes this directory a `tt` application |
| `app.lua` | the app module: `compute`, `combiner`, `obtain_name`, `master_preload`, `aggregators` |

The graph is `test/fixtures/graphs/soc-Epinions-custom.txt` — 75879 vertices and
508837 edges of the SNAP soc-Epinions1 trust network, with a name and a random
value attached to each vertex. The master reads the whole file and shards it out
over the three workers; loading takes about six tenths of a second. The path is
`app_cfg.graph` in `config.yaml`, relative to this directory; swap in
`../../test/fixtures/graphs/small/ring10.txt` for a ten-vertex graph whose
answer can be read at a glance.

Vertices are named by the file's own numbering rather than by the person's
name, because the fixture has 4684 duplicate names among its 75879 vertices and
pregel routes and stores by exactly that string — two vertices with one name
would be one vertex.

## Run it

`tt` gives every instance a working directory of its own under `var/lib`, so
neither the checkout nor this directory's `app.lua` is reachable by a relative
path once an instance is running. `LUA_PATH` is what the roles' `require()` goes
through, and it has to be set for `tt start`; nothing else needs it. The
trailing `;;` keeps Tarantool's own default path.

    cd examples/max-value
    LUA_PATH="$(cd ../.. && pwd)/?.lua;$(cd ../.. && pwd)/?/init.lua;$PWD/?.lua;;" tt start

    • Starting an instance [max-value:worker2]...
    • Starting an instance [max-value:worker3]...
    • Starting an instance [max-value:master]...
    • Starting an instance [max-value:worker1]...

The master role has `autostart: true`, so it waits for the three workers, loads
the graph and runs the supersteps by itself. Watch it:

    tt status

     INSTANCE           STATUS   PID    MODE  CONFIG  BOX      UPSTREAM
     max-value:master   RUNNING  26141  RW    ready   running  --
     max-value:worker1  RUNNING  26138  RW    ready   running  --
     max-value:worker2  RUNNING  26139  RW    ready   running  --
     max-value:worker3  RUNNING  26140  RW    ready   running  --

`tt connect max-value:master` opens a console on the master. Every console
line below is written as a pipe instead, so it can be pasted as it stands:

    echo "require('pregel.roles.master').status()" | tt connect max-value:master -f -
    ---
    - state: done
      name: maxvalue
      superstep: 12
    ...

`state` moves `idle` → `loading` → `running` → `done`, or `failed` with an
`error` field. `superstep` is live while the job runs, so re-running that one
line is how you follow it.

## Read the results

The answer, from the master's copy of the aggregator:

    echo "require('pregel.roles.master').get().aggregators['max_seen']()" | tt connect max-value:master -f -
    ---
    - 999987
    ...

Each worker keeps its own shard in the space `data_<job name>` — here
`data_maxvalue` — as `{name, halted, value, edges}`:

    echo "box.space.data_maxvalue:len()" | tt connect max-value:worker1 -f -
    ---
    - 25564
    ...

    echo "box.space.data_maxvalue:pairs():take(3):map(function(t) return t.value end):totable()" | tt connect max-value:worker1 -f -
    ---
    - - {'id': 0, 'name': 'James Moore', 'value': 999987}
      - {'value': 999987, 'name': 'Duane Olson', 'id': 10}
      - {'id': 1000, 'name': 'Laura Debar', 'value': 999987}
    ...

A named vertex lives on exactly one worker, so `:get()` for it answers on one
instance and nil on the other two:

    echo "box.space.data_maxvalue:get('2')" | tt connect max-value:worker3 -f -

Selecting a whole tuple prints the vertex's entire edge list, which for this
graph is long; the `:pairs():map(...)` above is the readable way round it.

Each worker also reports for itself:

    echo "require('pregel.roles.worker').status()" | tt connect max-value:worker1 -f -
    ---
    - state: running
      messages: 0
      name: maxvalue
      in_progress: 0
    ...

## Stop it

    tt stop -y

    • The Instance max-value:worker2 (PID = 26139) has been terminated.
    • The Instance max-value:worker3 (PID = 26140) has been terminated.
    • The Instance max-value:master (PID = 26141) has been terminated.
    • The Instance max-value:worker1 (PID = 26138) has been terminated.

`wal.mode` is `none`, so nothing survives; `rm -rf var` clears the working
directories as well.

## The test

`test/examples/max_value_test.lua` runs this same app module through
`luatest.cluster` on a generated thirty-vertex ring, where the answer can be
worked out in the test:

    make test
