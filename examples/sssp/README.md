# sssp — single-source shortest paths

Distances from one vertex to every other, over a weighted directed graph.

The source starts at 0 and everything else at infinity. A vertex that is
offered a route shorter than the one it holds keeps it and offers
`distance + weight` onward; a vertex that learns nothing better says nothing.
That is the whole algorithm, and it is why nothing here counts supersteps: the
job stops when no distance improved anywhere, which on the graph below is after
seven.

The combiner is `math.min` — of the several routes that reach one vertex in one
superstep, only the shortest can matter, so the worker folds them into one
message before the vertex ever sees them.

Two things this example shows that `examples/max-value` does not:

- **The graph is loaded on the workers, from Avro.** The app exports
  `worker_preload` rather than `master_preload`, so the master's autostart asks
  every worker to load instead of loading itself. All three open the same two
  Avro object container files and each keeps only the vertices whose names
  shard to it — nothing coordinates that, because the split uses the same
  `mpool:id()` that routes every message.
- **The algorithm is parameterised from the cluster config.** Which vertex is
  the source is `app_cfg.source`; the app module turns `app_cfg` into a worker
  context, and `vertex:get_worker_context()` is how a compute function — which
  is handed nothing but its vertex — reads it.

## Files

| file | what it is |
| --- | --- |
| `config.yaml` | the cluster config: one master, three workers, the credentials role and the user carrying it, the job's spaces granted on the worker replicasets, `roles_cfg` (the workers carry `app_cfg`, the master needs none) |
| `instances.yml` | the four instance names, for `tt` |
| `tt.yaml` | makes this directory a `tt` application |
| `app.lua` | `compute`, `combiner`, `obtain_name`, `worker_context`, `worker_preload` |

## The graph

`test/fixtures/graphs/small/weighted9.txt`, converted to Avro by

    tarantool tools/text2avro.lua \
        test/fixtures/graphs/small/weighted9.txt \
        test/fixtures/graphs/small/weighted9 --codec null

run from the repository root. Nine vertices, twelve weighted edges:

    a -1-> b      b -2-> c      c -1-> d      d -2-> f      f -1-> g
    a -4-> c      b -5-> d      c -3-> e      e -1-> d      g -2-> h
                                              e -7-> f      i -1-> a

Small enough to check by hand, and shaped so that the obvious answers are
wrong: `c` is 3 through `b` rather than the direct 4, and `d` is 4 through `c`
rather than the 6 it hears from `b` first. `i` has an out-edge but no in-edge,
so it is unreachable from anywhere.

## Run it

`tt` gives every instance a working directory under `var/lib`, so the checkout
and this directory's `app.lua` are only reachable through `LUA_PATH`, which has
to be set for `tt start` and for nothing else. The trailing `;;` keeps
Tarantool's own default path.

    cd examples/sssp
    LUA_PATH="$(cd ../.. && pwd)/?.lua;$(cd ../.. && pwd)/?/init.lua;$PWD/?.lua;;" tt start

    • Starting an instance [sssp:master]...
    • Starting an instance [sssp:worker1]...
    • Starting an instance [sssp:worker2]...
    • Starting an instance [sssp:worker3]...

`tt start` returns before the pid files are written, so a `tt status` run in the
same breath as it prints `NOT RUNNING` for all four. Give it a second.

    tt status

     INSTANCE      STATUS   PID    MODE  CONFIG  BOX      UPSTREAM
     sssp:master   RUNNING  96784  RW    ready   running  --
     sssp:worker1  RUNNING  96785  RW    ready   running  --
     sssp:worker2  RUNNING  96787  RW    ready   running  --
     sssp:worker3  RUNNING  96788  RW    ready   running  --

`tt connect sssp:master` opens a console; every console line below is written
as a pipe instead, so it can be pasted as it stands.

    echo "require('pregel.roles.master').status()" | tt connect sssp:master -f -
    ---
    - state: done
      name: sssp
      superstep: 7
    ...

## Read the results

Each worker holds its own shard in `data_sssp`, and between them they hold the
graph exactly once — which is also the proof that the three parallel loads
partitioned it rather than each loading everything.

The split is not luck of the draw: a vertex name is hashed onto one of the job's
workers — the instances the cluster config gives the worker role to — and every
instance sorts that list by the URI string before hashing, so bucket N is the
same worker on every instance and after every
restart. For the ports in this `config.yaml`, bucket 1 is `worker1`
(`127.0.0.1:3302`), bucket 2 is `worker2` (`:3303`) and bucket 3 is `worker3`
(`:3304`) — which puts six of the nine vertices on `worker1` and makes the
placement below reproducible rather than a snapshot of one run.

    echo "box.space.data_sssp:pairs():map(function(t) return t.value end):totable()" | tt connect sssp:worker1 -f -
    ---
    - - {'name': 'a', 'dist': 0}
      - {'name': 'c', 'dist': 3}
      - {'name': 'd', 'dist': 4}
      - {'name': 'e', 'dist': 6}
      - {'name': 'f', 'dist': 6}
      - {'name': 'g', 'dist': 7}
    ...

    echo "box.space.data_sssp:pairs():map(function(t) return t.value end):totable()" | tt connect sssp:worker2 -f -
    ---
    - - {'name': 'b', 'dist': 1}
      - {'name': 'h', 'dist': 9}
    ...

    echo "box.space.data_sssp:pairs():map(function(t) return t.value end):totable()" | tt connect sssp:worker3 -f -
    ---
    - - {'name': 'i', 'dist': inf}
    ...

`i` is `inf` because nothing points at it. That is a real `math.huge` in the
tuple, not a sentinel a reader has to know about — it survives msgpack intact.

## Measure from somewhere else

Change `app_cfg.source` in `config.yaml` and restart. From `i`, everything is
reachable and every distance is one more than it was from `a`; from `h`, which
has no out-edges at all, nothing is.

## Stop it

    tt stop -y

`wal.mode` is `none`, so nothing survives; `rm -rf var` clears the working
directories as well.

## The test

`test/examples/sssp_test.lua` runs this app module through `luatest.cluster`
against the same two Avro files and asserts the distances above, which were
worked out by hand rather than read back from a run:

    make test
