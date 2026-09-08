# Examples

Five runnable pregel jobs. Each directory is a `tt` application — a Tarantool 3
cluster config, four instance names, an app module and a README whose every
command was run against it — and each one is picked to show something the
others do not.

| example | algorithm | what it shows |
| --- | --- | --- |
| [max-value](max-value/) | largest incoming value | the smallest complete job: a combiner, an aggregator, and a text graph loaded on the master |
| [sssp](sssp/) | single-source shortest paths | an Avro graph loaded in parallel **on the workers**, and an algorithm parameterised from the cluster config |
| [pagerank](pagerank/) | PageRank, damping 0.85 | aggregators as the way a vertex learns something about the whole graph, and a job that runs a fixed number of supersteps |
| [wcc](wcc/) | weakly connected components | why the graph, not the algorithm, has to be symmetric |
| [topology-mutation](topology-mutation/) | prune weak edges | changing the graph from inside compute, and when each kind of change takes effect |

Read them in that order if you are reading them all: each one assumes the one
above it.

## Running any of them

`tt` gives every instance a working directory of its own under `var/lib`, so
neither the checkout nor the example's own `app.lua` is reachable by a relative
path once an instance is running. `LUA_PATH` is what the roles' `require()`
goes through, and it has to be set for `tt start` and for nothing else — `tt
connect`, `tt status` and `tt stop` need none of it. The trailing `;;` keeps
Tarantool's own default path.

    cd examples/<name>
    LUA_PATH="$(cd ../.. && pwd)/?.lua;$(cd ../.. && pwd)/?/init.lua;$PWD/?.lua;;" tt start
    echo "require('pregel.roles.master').status()" | tt connect <name>:master -f -
    tt stop -y

Each directory carries a `tt.yaml` of its own with `instances_enabled: .`, so
it is a `tt` environment and a `tt` application at once and needs no `tt init`
— unlike the configuration in the top-level README, which has to be dropped
into one.

`tt start` returns before the pid files are written, so a `tt status` run in
the same breath as it prints `NOT RUNNING` for all four instances. Give it a
second; nothing is wrong.

Every example's master has `autostart: true`, so it waits for its three
workers, loads the graph and runs the supersteps by itself; `status()` reports
`idle` → `loading` → `running` → `done`, or `failed` with the error. Without
autostart nothing happens until an operator drives it:

    local m = require('pregel.roles.master').get()
    m:wait_up():preload():start()

All five listen on `127.0.0.1:3301`…`3304`, so run one at a time.

Every per-worker transcript in these READMEs is reproducible, not a snapshot:
a vertex name is hashed onto one of the `workers` entries, and every instance
sorts that list by the URI string before hashing, so bucket 1 is always
`worker1` (`127.0.0.1:3302`), bucket 2 `worker2` (`:3303`) and bucket 3
`worker3` (`:3304`). The same vertex lands on the same worker on every machine
and after every restart.

## Configuring an app module

The roles know nothing about any of these algorithms. What an app module needs
to be told — where its graph is, which vertex is the source, what the threshold
is — travels in `roles_cfg.app_cfg`, an opaque table the roles check only for
being a table and then hand to the app module twice: as the second argument of
`master_preload` / `worker_preload`, and (when the module's `worker_context` is
a function) as the argument that builds the context every compute function
reads through `vertex:get_worker_context()`. Those two are the whole channel,
because a compute function is handed nothing but its vertex.

Paths in `app_cfg` are relative to the example's own directory, resolved by
`examples/common.lua` against the directory the app module was loaded from —
the process's working directory is no use, since it differs between `tt` and
the test suite.

## The tests

`test/examples/` runs every one of these app modules through
`luatest.cluster` on a small graph with a known answer, and
`test/examples/config_test.lua` checks the committed `config.yaml` files
themselves — the instance list, the privileges, and that every path in
`app_cfg` is still there.

    make test
