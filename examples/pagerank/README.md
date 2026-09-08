# pagerank

The rank of every vertex after a fixed number of power iterations, damping
0.85.

This is the one example here that cannot be written with messages alone. Two of
the quantities PageRank needs are properties of the whole graph rather than of
any vertex, and both reach the vertices through **aggregators** — values every
vertex contributes to, which the workers reduce locally, the master merges, and
every vertex reads back one superstep later:

- `count` — how many vertices there are. Nobody is told; every vertex
  contributes 1, which is what makes `1/N` knowable.
- `dangling` — the total rank held by vertices with no out-edges. A dangling
  vertex has nowhere to send its share, so without this the ranks leak away a
  little on every iteration and stop summing to one. What it holds is spread
  uniformly over the graph in the next superstep.

An aggregator is per-superstep: the master resets its copy before the workers
report into it, so what a vertex reads in superstep S is what the whole graph
contributed in S−1. That one-superstep lag shapes the timeline:

| superstep | what happens |
| --- | --- |
| 1 | contribute to `count`; nothing else is knowable yet |
| 2 | `rank := 1/N`, send `rank / out-degree` along each edge |
| 3 | the first power iteration, over superstep 2's messages |
| … | |
| `iterations + 2` | the last one: set the rank, send nothing, halt |

Unlike `examples/sssp`, this job does not stop by itself — PageRank converges
rather than terminates — so the vertices count supersteps and halt together.

> **Note.** This example is the only one that depends on a worker's aggregator
> being reset between supersteps. Without that, `count` over-counts (78 rather
> than 6 by the third superstep, because each worker reports the merged value
> back and the master adds it once per worker) and every rank below is wrong.
> The transcripts here were taken with the reset in place.

## Files

| file | what it is |
| --- | --- |
| `config.yaml` | the cluster config; `app_cfg` is a YAML anchor shared by the master and the workers |
| `instances.yml` | the four instance names, for `tt` |
| `tt.yaml` | makes this directory a `tt` application |
| `app.lua` | `compute`, `obtain_name`, `worker_context`, `master_preload`, `aggregators` |

There is no combiner: the incoming shares are summed, and summing them early
would be correct but is left out to keep the compute function the only place
arithmetic happens.

## The graph

`test/fixtures/graphs/small/pagerank6.txt`, converted to Avro by

    tarantool tools/text2avro.lua \
        test/fixtures/graphs/small/pagerank6.txt \
        test/fixtures/graphs/small/pagerank6 --codec null

run from the repository root. Six vertices, nine edges:

    A → B, C      C → A          E → D, F
    B → C         D → A, B, C    F → (nothing)

`F` is the dangling vertex. `E` has no in-edges, so it keeps the minimum rank
`(1-d)/N` throughout and is the check that the damping term is there at all.

The master reads both files whole and shards the graph out — the same
`loader.avro_files` that `examples/sssp` runs on every worker, which keeps
everything rather than its own share when it is called without a worker index.

## Run it

    cd examples/pagerank
    LUA_PATH="$(cd ../.. && pwd)/?.lua;$(cd ../.. && pwd)/?/init.lua;$PWD/?.lua;;" tt start

    • Starting an instance [pagerank:worker3]...
    • Starting an instance [pagerank:master]...
    • Starting an instance [pagerank:worker1]...
    • Starting an instance [pagerank:worker2]...

    tt status

     INSTANCE          STATUS   PID    MODE  CONFIG  BOX      UPSTREAM
     pagerank:master   RUNNING  20593  RW    ready   running  --
     pagerank:worker1  RUNNING  20594  RW    ready   running  --
     pagerank:worker2  RUNNING  20595  RW    ready   running  --
     pagerank:worker3  RUNNING  20592  RW    ready   running  --

`tt connect pagerank:master` opens a console; every console line below is
written as a pipe instead, so it can be pasted as it stands.

    echo "require('pregel.roles.master').status()" | tt connect pagerank:master -f -
    ---
    - state: done
      name: pagerank
      superstep: 32
    ...

Thirty-two: one superstep to count the vertices, one to lay down `1/N`, then
the thirty iterations `app_cfg.iterations` asks for.

## Read the results

The two aggregators, from the master's merged copies:

    echo "require('pregel.roles.master').get().aggregators['count']()" | tt connect pagerank:master -f -
    ---
    - 6
    ...

    echo "require('pregel.roles.master').get().aggregators['dangling']()" | tt connect pagerank:master -f -
    ---
    - 0.044635865309319
    ...

The dangling value is `F`'s rank, which is what it should be: `F` is the only
vertex with no out-edges.

    echo "box.space.data_pagerank:pairs():map(function(t) return t.value end):totable()" | tt connect pagerank:worker1 -f -
    ---
    - - {'name': 'A', 'rank': 0.34102414309093}
      - {'name': 'F', 'rank': 0.044635865309319}
    ...

    echo "box.space.data_pagerank:pairs():map(function(t) return t.value end):totable()" | tt connect pagerank:worker2 -f -
    ---
    - - {'name': 'B', 'rank': 0.18890551299589}
      - {'name': 'C', 'rank': 0.34947519904239}
      - {'name': 'D', 'rank': 0.044635865309319}
      - {'name': 'E', 'rank': 0.031323414252154}
    ...

    echo "box.space.data_pagerank:pairs():map(function(t) return t.value end):totable()" | tt connect pagerank:worker3 -f -
    ---
    - []
    ...

Six vertices over three shards is few enough that a worker can come away with
none, as `worker3` did here; the sharding is by hash of the vertex name and
makes no attempt to balance.

`C` ranks highest, `A` a close second (both are pointed at by three of the six
vertices), and `E` lowest at `(1 - 0.85) / 6 = 0.025` plus its share of the
dangling mass. The six ranks sum to 1 — which they only do because the dangling
mass is redistributed rather than dropped.

## Change the iteration count

`app_cfg.iterations` and `app_cfg.damping` in `config.yaml`, then restart. One
iteration is enough to see the algorithm working and is what the test's second
case pins.

## Stop it

    tt stop -y

`wal.mode` is `none`, so nothing survives; `rm -rf var` clears the working
directories as well.

## The test

`test/examples/pagerank_test.lua` runs this app module through
`luatest.cluster` and compares every rank against a plain-Lua power iteration
over the same graph, to 1e-6 — the reference is computed in the test, not read
back from an earlier run of the same code:

    make test
