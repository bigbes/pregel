# topology-mutation

Prune every edge below a weight threshold, and mark whatever that strands.

Not a graph algorithm so much as a demonstration of the one API the other
examples never touch: a compute function may change the **graph**, not only the
values in it. Every edge whose weight is below `app_cfg.threshold` is deleted,
and a vertex that ends the superstep with no out-edges at all gets a marker
vertex called `<name>:orphan` added beside it.

## The two mutations happen at different times

That is the thing to take away, and the reason this job runs two supersteps
instead of one.

`delete_edge` on one's **own** edges is applied when the vertex is written back
at the end of its own compute call. So `pairs_edges` still walks the full list
while compute is running, and the survivors have to be counted as you go rather
than read off the vertex afterwards.

`add_vertex` is queued as a topology mutation on whichever worker will own the
new vertex — decided by `obtain_name` of the value, and not necessarily the
worker that asked — and applied between supersteps, after every compute call has
finished. So the marker does not exist during the superstep that asks for it. It
turns up in the next one, active and unhalted, which is why the master sees work
still to do:

    <topology mutation> del_edge 0, del_vertex 0, add_vertex 3, add_edge 0 tasks
    <topology mutation, add_vertex> 'e:orphan': added
    <topology mutation, add_vertex> 'g:orphan': added
    <topology mutation, add_vertex> 'h:orphan': added

from `var/log/worker3/tt.log`. Each worker logs its own queue this way every
superstep, `does not exist` and `exists` included — an `add_edge` out of a
vertex that was deleted in the same batch says so rather than failing.

One caveat the code cannot hide: deletions are matched by destination name, so
two edges to the same destination stand or fall together however different
their weights are.

## Files

| file | what it is |
| --- | --- |
| `config.yaml` | the cluster config; `app_cfg` carries the graph and the threshold, shared by master and workers through a YAML anchor |
| `instances.yml` | the four instance names, for `tt` |
| `tt.yaml` | makes this directory a `tt` application |
| `app.lua` | `compute`, `obtain_name`, `worker_context`, `master_preload` |

The graph is `test/fixtures/graphs/small/weights8.txt`: eight vertices, ten
edges with weights 1 to 9.

    a -9-> b   a -2-> c   b -3-> c   b -1-> d   c -7-> d
    d -5-> e   e -4-> f   f -8-> g   f -6-> h   g -1-> h

`h` has no out-edges to begin with. At threshold 5 that gives one vertex that
keeps some of its edges and loses the rest (`a`), one edge exactly on the
threshold and therefore kept (`d -5-> e`), three vertices that lose everything
(`b`, `e`, `g`) and one that never had anything (`h`).

## Run it

    cd examples/topology-mutation
    LUA_PATH="$(cd ../.. && pwd)/?.lua;$(cd ../.. && pwd)/?/init.lua;$PWD/?.lua;;" tt start

    • Starting an instance [topology-mutation:master]...
    • Starting an instance [topology-mutation:worker1]...
    • Starting an instance [topology-mutation:worker2]...
    • Starting an instance [topology-mutation:worker3]...

`tt start` returns before the pid files are written, so a `tt status` run in the
same breath as it prints `NOT RUNNING` for all four. Give it a second.

    tt status

     INSTANCE                   STATUS   PID    MODE  CONFIG  BOX      UPSTREAM
     topology-mutation:master   RUNNING  20379  RW    ready   running  --
     topology-mutation:worker1  RUNNING  20380  RW    ready   running  --
     topology-mutation:worker2  RUNNING  20382  RW    ready   running  --
     topology-mutation:worker3  RUNNING  20383  RW    ready   running  --

`tt connect topology-mutation:master` opens a console; every console line below
is written as a pipe instead, so it can be pasted as it stands.

    echo "require('pregel.roles.master').status()" | tt connect topology-mutation:master -f -
    ---
    - state: done
      name: topology
      superstep: 2
    ...

## Read the results

The graph is small enough to print whole. Each tuple is
`{name, halted, value, edges}`. Which worker a vertex lands on is fixed: the
name is hashed onto one of the `workers` entries, and every instance sorts that
list by the URI string first, so bucket 1 is `worker1` (`127.0.0.1:3302`),
bucket 2 is `worker2` (`:3303`) and bucket 3 is `worker3` (`:3304`) — the
placement below is the same on every machine and after every restart.

    echo "box.space.data_topology:select()" | tt connect topology-mutation:worker1 -f -
    ---
    - - ['a', true, {'id': 1, 'name': 'a', 'value': 0}, [['b', 9]]]
      - ['b:orphan', true, {'name': 'b:orphan', 'orphan_of': 'b'}, []]
      - ['c', true, {'id': 3, 'name': 'c', 'value': 0}, [['d', 7]]]
      - ['d', true, {'id': 4, 'name': 'd', 'value': 0}, [['e', 5]]]
      - ['e', true, {'id': 5, 'name': 'e', 'value': 0}, []]
      - ['f', true, {'id': 6, 'name': 'f', 'value': 0}, [['g', 8], ['h', 6]]]
      - ['g', true, {'id': 7, 'name': 'g', 'value': 0}, []]
    ...

    echo "box.space.data_topology:select()" | tt connect topology-mutation:worker2 -f -
    ---
    - - ['b', true, {'id': 2, 'name': 'b', 'value': 0}, []]
      - ['h', true, {'value': 0, 'name': 'h', 'id': 8}, []]
    ...

    echo "box.space.data_topology:select()" | tt connect topology-mutation:worker3 -f -
    ---
    - - ['e:orphan', true, {'name': 'e:orphan', 'orphan_of': 'e'}, []]
      - ['g:orphan', true, {'name': 'g:orphan', 'orphan_of': 'g'}, []]
      - ['h:orphan', true, {'name': 'h:orphan', 'orphan_of': 'h'}, []]
    ...

`a` kept its 9 and lost its 2; `d` kept the edge whose weight is exactly the
threshold; `b`, `e`, `g` and `h` came away with nothing and each has a marker.

And the markers are not where the vertices that asked for them are: `b` is on
`worker2` while `b:orphan` is on `worker1`, and `e`, `g`, `h` are spread over
`worker1` and `worker2` while all three of their markers are on `worker3`. A
vertex is placed by the hash of its name and `<name>:orphan` is a different
name — which is exactly what the delayed mutation queue is for. The request
travels; the vertex is created where it belongs.

## Change the threshold

`app_cfg.threshold` in `config.yaml`, then restart. Below 1 nothing is pruned
and only `h` is stranded; above 9 the whole graph is, and every vertex gets a
marker.

## Stop it

    tt stop -y

`wal.mode` is `none`, so nothing survives; `rm -rf var` clears the working
directories as well.

## The test

`test/examples/topology_mutation_test.lua` runs this app module through
`luatest.cluster` and asserts the surviving edge list of every vertex, the four
markers and what they point back at, that at least one marker landed on a
different worker from its orphan, and the two boundary thresholds:

    make test
