# Pregel on Tarantool

Large-scale graph processing on Tarantool 3, in pure Lua. A graph is sharded
across a set of worker instances by vertex name; a master instance drives a
loop of supersteps, in each of which every active vertex runs a compute
function, reads the messages sent to it in the previous superstep and sends
messages of its own. The run ends when no vertex is active and no message is in
flight. The model is the one described in the
[Pregel paper](http://kowshik.github.io/JPregel/pregel_paper.pdf); the API it
grew from is Apache Giraph's.

> Many practical computing problems concern large graphs. Standard examples
> include the Web graph and various social networks. The scale of these
> graphs - in some cases billions of vertices, trillions of edges — poses
> challenges to their efficient processing. In this paper we present a
> computational model suitable for this task. Programs are expressed as a
> sequence of iterations, in each of which a vertex can receive messages sent
> in the previous iteration, send mes- sages to other vertices, and modify its
> own state and that of its outgoing edges or mutate graph topology. This
> vertex-centric approach is flexible enough to express a broad set of
> algorithms. The model has been designed for efficient, scalable and
> fault-tolerant implementation on clusters of thousands of commodity
> computers, and its implied synchronicity makes reasoning about programs
> easier. Distribution-related details are hidden behind an abstract API.
> The result is a framework for processing large graphs that is expressive
> and easy to program.

## Requirements and installation

Tarantool 3.x, Community Edition. There is no C code and no external Lua
dependency; luatest and luacheck are needed only to run the test suite, and
`make deps` installs them.

Compression is optional and comes from whatever is at hand. `pregel.compress`
is Tarantool Enterprise's `compress` module where there is one, and otherwise
binds the system libz, libzstd and liblz4 through the FFI — so the `deflate`
and `zstandard` container-file codecs compress for real on Community Edition
too, on any host with the libraries installed. With neither, `deflate` still
writes (uncompressed stored blocks) and still reads (a pure-Lua inflater), and
`zstandard` is unavailable. See [pregel.compress](#pregelcompress).

Install the rock into a `tt` environment's rocks tree:

```
tt rocks make --tree /path/to/env/.rocks pregel-scm-1.rockspec
```

`tt rocks make pregel-scm-1.rockspec` does the same into the current
directory's `.rocks`. Alternatively point `LUA_PATH` at a checkout, which is
what the tests do:

```
LUA_PATH="$PWD/?.lua;$PWD/?/init.lua;;" tarantool your-script.lua
```

## Quick start: the cluster roles

`pregel.roles.master` and `pregel.roles.worker` are Tarantool 3 roles, so a
whole job is a cluster config plus one Lua module. Nothing is created by hand:
the roles applier builds the master and the workers from `roles_cfg`.

The app module is what makes the job this job rather than another one. Both
roles `require()` it by the name in `roles_cfg.app` and read the same fields
out of it — the worker needs `compute`, the master does not; both need
`obtain_name`.

```lua
-- maxvalue.lua: every vertex ends up holding the largest value in the graph.
local loader = require('pregel.loader')

local VERTEX_COUNT = 12
local app = {}

local function vertex(i)
    return {
        name  = string.format('v%03d', i),
        value = ((i * 7 - 1) % VERTEX_COUNT) + 1,
    }
end

--- The name pregel knows a vertex value by. Required by both roles.
function app.obtain_name(value)
    return value.name
end

--- Run once per vertex per superstep. Required by the worker role.
function app.compute(self)
    local value = self:get_value().value
    local best = value
    for _, msg in self:pairs_messages() do
        if msg > best then best = msg end
    end
    if self:get_superstep() == 1 or best > value then
        self:set_value({name = self:get_name(), value = best})
        for _, dest in self:pairs_edges() do
            self:send_message(dest, best)
        end
    end
    self:set_aggregation('max_seen', best)
    self:vote_halt(true)
end

--- Optional: fold several messages for one receiver into one.
function app.combiner(a, b)
    return a > b and a or b
end

--- Optional, and declared on both sides: a worker reports its copy of an
--- aggregator to the master by name, so the master must know the same name.
app.aggregators = {
    max_seen = {
        default = 0,
        reduce  = function(old, new) return new > old and new or old end,
    },
}

--- Optional: push the graph out from the master.
function app.master_preload(instance)
    return loader.new(instance, function(self)
        for i = 1, VERTEX_COUNT do
            self:store_vertex(vertex(i))
        end
        for i = 1, VERTEX_COUNT do
            self:store_edge(vertex(i).name,
                            vertex((i % VERTEX_COUNT) + 1).name, 1)
        end
        self:flush()
    end)
end

return app
```

The cluster config below runs that module over one master and three workers:

```yaml
credentials:
  users:
    replicator:
      password: 'replicator-secret'
      roles: [replication]
    # The user pregel connects to its own peers as. Every message between a
    # master and a worker is a conn:call() on one of these four names, so this
    # grant is the whole privilege story -- the library asks for no universe
    # grant and uses no conn:eval().
    pregel:
      password: 'pregel-secret'
      privileges:
        - permissions: [execute]
          lua_call:
            - pregel.worker.deliver
            - pregel.worker.deliver_batch
            - pregel.worker.wait
            - pregel.master.deliver

iproto:
  advertise:
    peer:
      login: replicator

# One instance per replicaset: every instance is the read-write leader of its
# own, so the workers are shards rather than copies of each other. Both roles
# refuse to apply on a read-only instance.
replication:
  failover: off

groups:
  pregel:
    replicasets:
      r-master:
        instances:
          master:
            iproto:
              listen:
                - uri: '127.0.0.1:3301'
            roles: [pregel.roles.master]
            roles_cfg:
              pregel.roles.master:
                name: maxvalue          # job name
                app: maxvalue           # the Lua module above
                autostart: true         # run the job as soon as it can
                user: pregel
                password: pregel-secret
      r-worker1:
        instances:
          worker1:
            iproto:
              listen:
                - uri: '127.0.0.1:3302'
            roles: [pregel.roles.worker]
            roles_cfg: &worker_cfg
              pregel.roles.worker:
                name: maxvalue
                app: maxvalue
                user: pregel
                password: pregel-secret
      r-worker2:
        instances:
          worker2:
            iproto:
              listen:
                - uri: '127.0.0.1:3303'
            roles: [pregel.roles.worker]
            roles_cfg: *worker_cfg
      r-worker3:
        instances:
          worker3:
            iproto:
              listen:
                - uri: '127.0.0.1:3304'
            roles: [pregel.roles.worker]
            roles_cfg: *worker_cfg
```

Neither `workers` nor `master` appears in any `roles_cfg` here. Both roles fall
back to reading the cluster config and taking every instance that runs the
other role for a job of this `name`, so the config says who the participants
are exactly once. Spell the URIs out instead when the participants are not all
in one cluster config.

### Putting those two files where `tt` will find them

`tt` runs applications out of an *environment* — a directory with a `tt.yaml`
in it — and an application is a directory under that environment's
`instances_enabled` holding a `config.yaml` and an `instances.yml` naming the
instances the config defines. `tt init` creates the environment; the
application directory is yours to make. For the two files above, called
`pregel` because that is the name `tt start pregel` takes:

```
mkdir quickstart && cd quickstart
tt init
mkdir -p instances.enabled/pregel
```

```
quickstart/
├── tt.yaml                              # written by tt init
└── instances.enabled/
    └── pregel/
        ├── config.yaml                  # the cluster config above
        ├── instances.yml                # master: / worker1: / worker2: / worker3:
        └── maxvalue.lua                 # the app module above
```

`instances.yml` is four lines, one per instance name in `config.yaml`, each
with a trailing colon and nothing after it:

```yaml
master:
worker1:
worker2:
worker3:
```

Each instance gets a working directory of its own under `var/lib`, so
`maxvalue.lua` is not reachable by a relative path once one is running: the
roles' `require()` finds it through `LUA_PATH`, which has to be set for `tt
start` and for nothing else. `/path/to/pregel` below is the checkout; with the
rock installed into the environment's own `.rocks` (see above), only the
application directory has to be added. The trailing `;;` keeps Tarantool's own
default path.

Start it, watch it, read the answer, stop it:

```
LUA_PATH="/path/to/pregel/?.lua;/path/to/pregel/?/init.lua;$PWD/instances.enabled/pregel/?.lua;;" tt start pregel
tt status pregel
```

`tt start` returns before the pid files are written, so a `tt status` run in
the same breath as it prints `NOT RUNNING` for all four. Give it a second.

```
 INSTANCE        STATUS   PID    MODE  CONFIG  BOX      UPSTREAM
 pregel:master   RUNNING  33824  RW    ready   running  --
 pregel:worker1  RUNNING  33826  RW    ready   running  --
 pregel:worker2  RUNNING  33827  RW    ready   running  --
 pregel:worker3  RUNNING  33829  RW    ready   running  --
```

The master role's `status()` starts at `connecting` — see [Waiting for the
peers](#waiting-for-the-peers) — and then follows the autostart fiber through
`idle`, `loading`, `running` and `done` (or `failed`, with the error):

```
$ tt connect pregel:master -f - <<< "return require('pregel.roles.master').status()"
---
- state: done
  name: maxvalue
  superstep: 13
...
```

The result is the workers' own spaces. A job called `maxvalue` stores its shard
of the graph in `data_maxvalue`, one tuple per vertex: name, halted flag, the
user value, and the outgoing edges as `{destination, value}` pairs. Which
worker holds which vertices is settled by the vertex name and the sorted list
of worker URIs, so it is the same on every machine and after every restart —
`v004`, `v006` and `v008` are the first three on `worker1` wherever this runs.

```
$ tt connect pregel:worker1 -f - <<< "return box.space.data_maxvalue:select({}, {limit = 3})"
---
- - ['v004', true, {'name': 'v004', 'value': 12}, [['v005', 1]]]
  - ['v006', true, {'name': 'v006', 'value': 12}, [['v007', 1]]]
  - ['v008', true, {'name': 'v008', 'value': 12}, [['v009', 1]]]
...
```

```
tt stop -y pregel
```

Runnable versions of this configuration live in `examples/`. Each of those
directories carries a `tt.yaml` of its own with `instances_enabled: .`, so it
is the environment and the application at once and needs no `tt init` — which
is also why their commands are run from inside the example directory rather
than from an environment above it.

### roles_cfg reference

Both roles take:

* `name` — the job name (required). It names the spaces, and it is what the
  discovery above matches on, so one cluster can run several jobs.
* `app` — the Lua module name both roles `require()` (required).
* `workers` — array of every worker's net.box URI. Left out, it is discovered
  from the cluster config.
* `pool_size` — messages per outgoing batch (default 1000).
* `user`, `password` — the net.box credentials for outgoing calls. A
  `password` without a `user` is refused: the peers would connect as `guest`
  and the password would go unused.
* `connect_timeout` — seconds the role keeps trying to reach its peers before
  giving up (default 300).

`pregel.roles.worker` also takes:

* `master` — the master's net.box URI; discovered when left out.
* `squash_only` (default `false`) — run the app's combiner once per superstep
  instead of on every message put.
* `queue_engine` — `space` (default, so the message queue survives a restart)
  or `table`.
* `delayed_push` (default `false`) — back the outgoing batches with spaces
  rather than memory, for a preload that produces more messages than fit in
  memory.

`pregel.roles.master` also takes `autostart` (default `false`), which starts a
background fiber that waits for every worker, preloads the graph and runs the
supersteps.

One limit is deliberate: a running job cannot be reconfigured. An apply that
changes `roles_cfg` while the job exists fails with a message saying to stop
the role first, because the worker list is resolved once and moving it under a
running job cannot be done consistently.

An unknown key in `roles_cfg` is refused by name rather than ignored, so a
typo stops the config from applying. So is an empty `name`, `app`, `master` or
`user`, and an aggregator option the app module misspells.

### Waiting for the peers

`apply()` never waits for another instance. It is called from the config
framework's synchronous `post_apply`, so an apply that waited would hold up the
instance's whole startup — and an error raised from it during startup is fatal,
which means one instance that is down would take every other one with it. The
role builds its message pool without connecting and hands the waiting to a
fiber of its own:

* `status().state` is `connecting` until every peer has answered, with
  `status().error` carrying net.box's own reason for the last attempt
  (`connect to ...: No such file or directory`, `User not found or supplied
  credentials are invalid`, ...). Each failed attempt is logged at warn level.
* After `connect_timeout` the role gives up: `status().state` becomes `failed`
  and the reason is published as a `warn` alert in `config:info().alerts`. The
  instance stays up and `config:info().status` stays `ready`, because a peer
  that is down is not a broken configuration. A `config:reload()` starts a
  fresh attempt.
* A rejected authentication is not retried to the timeout: net.box would keep
  trying a hopeless login, and the answer is already known.

### Replicas

A role on a read-only instance does nothing. `roles:` is normally written at
replicaset scope, so every replica of a worker replicaset carries the role
whether or not anyone meant it to; refusing to apply there would make the
replica's config unappliable, which at startup exits the process. Instead the
role logs that it is inert, reports `status()` as `{state = 'read_only'}`, and
picks the job up on the first `config:reload()` after the instance becomes
read-write.

Discovery follows the same rule from the other side: a replicaset is one
participant of the job, not one per instance. It is addressed through the
instance that will be read-write — the only one, if only one carries the role;
otherwise the `rw` one under `replication.failover: off`, or the replicaset's
`leader` under `manual`. Under `election` and `supervised` failover the config
names no leader, so the role says so and asks for an explicit `workers` list
rather than guessing.

A discovered peer keeps the transport parameters of its `iproto.listen` entry,
so a cluster listening with `transport: ssl` (Enterprise) is dialled over SSL.
It does not keep the login from `iproto.advertise.peer`: that is the
replication user, and the graph traffic connects as `roles_cfg.user`.

Neither the `lua_call` grant nor the `credentials` section can name the job's
spaces, because they do not exist when the credentials applier first runs. The
worker role grants read/write on them itself, to the `user` from `roles_cfg`,
right after creating them. A config that sets no `user` gets no such grant:
the peers then connect as `guest`, and giving `guest` write access to the graph
is a decision for the operator.

Without `autostart`, nothing happens until someone drives the job. Both roles
expose `get()` for that, returning the live object:

```lua
local m = require('pregel.roles.master').get()
m:wait_up():preload():start()
```

## Programmatic API

Underneath the roles are `pregel.master` and `pregel.worker`, which can be used
directly — this is what the integration tests do.

```lua
local pworker = require('pregel.worker')
local pmaster = require('pregel.master')

-- Requiring the modules publishes the RPC entry points; these two let the user
-- the peers connect as call them.
pworker.grant('guest')
pmaster.grant('guest')

local worker = pworker.new('demo', {
    workers     = {'127.0.0.1:3302', '127.0.0.1:3303'},
    master      = '127.0.0.1:3301',
    obtain_name = function(value) return value.name end,
    compute     = function(self) --[[ ... ]] end,
    grant_to    = 'guest',
})

local master = pmaster.new('demo', {
    workers     = {'127.0.0.1:3302', '127.0.0.1:3303'},
    obtain_name = function(value) return value.name end,
})

local supersteps = master:wait_up():preload():start()
```

`master.new(name, options)`:

* `workers` — array of every worker's net.box URI. Each may carry its own
  `user:password@host:port`.
* `obtain_name` — `callable(value) -> string`, the name pregel routes and
  stores a vertex value by (required).
* `pool_size` — messages per batch (default 1000).
* `master_preload` — a loader object, or `callable(self, preload_args)`
  returning one, or `nil` for a master that only coordinates.
* `preload_args` — passed to `master_preload`.
* `user`, `password` — net.box credentials for the outgoing connections.
* `max_supersteps` — a positive integer, or `nil` (the default) for no limit.
  `master:start()` runs while a vertex is active or a message is in flight, so
  an algorithm that does not converge has nothing to stop it. With a limit set,
  a run that is still going after that many supersteps raises

  ```
  pregel: superstep limit 5 reached with 50 active vertices and 0 messages in flight
  ```

  and the master role reports it as `failed` with that message. A run with no
  limit logs a warning naming this option every hundred supersteps.

`worker.new(name, options)` takes `workers`, `obtain_name`, `pool_size`,
`preload_args`, `user` and `password` with the same meaning, plus:

* `master` — the master's net.box URI (required).
* `compute` — `callable(vertex)`, run once per active vertex per superstep
  (required).
* `combiner` — `callable(a, b) -> c`, folds two messages for one receiver into
  one.
* `squash_only` — run the combiner once per superstep from the queue's
  `squash()` rather than on every put (default `false`). Combining on put costs
  a read of the receiver's messages per put, which is the wrong trade when one
  receiver gets many.
* `queue_engine` — `'space'` (default) or `'table'`.
* `delayed_push` — back the outgoing batches with spaces instead of memory
  (default `false`).
* `worker_context` — any value, handed to every vertex on this instance through
  `vertex:get_worker_context()`.
* `worker_preload` — a loader object, or `callable(self, preload_args)`
  returning one.
* `grant_to` — a user, or an array of users, allowed to reach this instance.
  They get the RPC grants and read/write on this instance's spaces.

Lifecycle, on the master:

* `master:wait_up()` — block until every worker exists and has reached this
  master.
* `master:preload()` — run the master-side loader and push what it produced.
* `master:preload_on_workers()` — ask every worker to run its own loader
  instead. A worker's loader is handed its own bucket index and the bucket
  count, so it can load only its share.
* `master:start()` — run supersteps until no message is in flight and no vertex
  is active. Returns the number of supersteps, or raises when `max_supersteps`
  runs out first.
* `master:add_aggregator(name, options)` — see below.
* `master:save_snapshot()` — tell every worker to `box.snapshot()`.
* `master:stop()` — stop the message pool and drop this master.

Each of `wait_up`, `preload`, `preload_on_workers` and `add_aggregator` returns
the master, so they chain. `worker:stop()` is the worker's half: it stops the
pool, closes the connection to the master and unregisters the instance. Neither
`stop` drops any space — the shard is meant to survive a restart.

Sharding is a pure function of the vertex name: jump consistent hashing over a
CRC-32 of the name (`pregel.mpool.guava_name`), so every instance computes the
same owner without talking to anything, and adding a worker moves only the
names it must.

`worker.grant(user[, instance_name])` and `master.grant(user)` hand out
`execute` on `lua_call` for the entry points those modules publish —
`pregel.worker.deliver`, `pregel.worker.deliver_batch`, `pregel.worker.wait`
and `pregel.master.deliver`. Given an instance name as well, `worker.grant`
also grants read/write on that instance's spaces and their sequences, which a
`lua_call` grant alone does not cover: the call runs with the caller's
privileges and the entry points write. The two halves are separate because the
entry-point names are known before any instance exists and the space names are
not.

## Loaders

A loader is a callable object that walks a source and pushes the graph out
through the instance's message pool, addressing every vertex and edge to the
worker that owns it. `pregel.loader` has three entry points.

`loader.new(instance, fn)` wraps an arbitrary function, which is called with
the loader itself and may use:

* `loader:store_vertex(value)` — store one vertex, returning its name.
* `loader:store_edge(src, dest, value)` — store one edge from the vertex named
  `src`.
* `loader:store_edges_batch(src, list)` — store a list of `{dest, value}` pairs
  from `src` in one message.
* `loader:store_vertex_edges(value, list)` — both at once, returning the vertex
  name.
* `loader:flush()` — send whatever is still batched.

None of these resolve conflicts: a vertex stored twice is reset to the later
value, and edges may be duplicated.

`loader.graph_edges_f(instance, path)` reads the two-section text format:

```
# List of vertices
<id> '<name>' <value>
# List of edges
<source_id> <destination_id> <value>
```

The ids are the file's own numbering. The names pregel uses come from the
instance's `obtain_name`, and the edge section is translated through the vertex
section, so the vertex section has to come first.

`loader.avro_files(instance, options)` streams a graph from a pair of Avro
object container files, so a graph larger than memory costs only one edge
batch here:

* `vertices`, `edges` — paths to the two files (required).
* `vertex_name` — a field name of the vertex file's schema, or
  `function(record) -> string` (required).
* `vertex_value` — field name or function; what gets stored as the vertex
  value. The default is the whole record, because the worker names a stored
  vertex by calling `obtain_name` on it.
* `edge_src`, `edge_dst` — field name or function (both required).
* `edge_value` — field name or function (default `json.NULL`).
* `batch` — edges of one source per message (default 1000).

A field name is checked against the file's own schema when the loader is
built, so a misspelled field fails immediately rather than after storing a
graph of nameless vertices.

Called as `loader()` the whole graph is loaded. Called as
`loader(worker_idx, workers_count)` — which is what `worker:preload()` does —
only the share belonging to that worker is: vertices whose name shards to it,
and edges whose *source* shards to it. The split uses the same hash that routes
every message, so N workers reading the same two files cover the graph exactly
once between them with nothing to coordinate, and an edge always lands on the
worker that stored its source.

`tools/text2avro.lua` converts the text format into that pair of files:

```
$ tarantool tools/text2avro.lua test/fixtures/graphs/small/ring10.txt /tmp/ring10-avro
test/fixtures/graphs/small/ring10.txt -> /tmp/ring10-avro/vertices.avro: 10 vertices
test/fixtures/graphs/small/ring10.txt -> /tmp/ring10-avro/edges.avro: 14 edges
codec: null
```

It takes `--codec null|deflate|zstandard`, and writes edges naming their
endpoints by vertex name rather than by the file's numbering — which is what
lets the Avro loader avoid holding the id-to-name map in memory.

## The vertex API

The compute function is handed a vertex object. Vertex objects are pooled and
reused across the vertices of a superstep, so nothing may be kept between
calls.

**Halting is the default.** A compute function that returns without calling
`vote_halt` leaves its vertex halted, exactly as if it had ended with
`vote_halt(true)`. A vertex that wants another superstep without a message to
wake it asks with `vote_halt(false)`; a message wakes a halted vertex either
way. So a compute function that forgets to vote ends the job rather than
running it forever — which is what it used to do.

Base:

* `vertex:get_name()` — the name pregel routes and stores by.
* `vertex:get_value()` / `vertex:set_value(value)` — the user value.
* `vertex:get_superstep()` — the superstep number, counting from 1.
* `vertex:vote_halt([is_halted = true])` — a halted vertex with no messages is
  skipped in later supersteps. A vertex with messages waiting is computed
  whether it is halted or not, but it stays halted afterwards unless that
  superstep's compute called `vote_halt(false)`. Not calling `vote_halt` at all
  halts the vertex.
* `vertex:get_worker_context()` — the `worker_context` this instance was
  created with, shared by every vertex on it.

Messaging:

* `vertex:pairs_messages()` — iterate the messages sent to this vertex in the
  previous superstep, as `(sender, message)`. There is no guaranteed order.
* `vertex:send_message(receiver_name, value)` — send to any vertex by name, not
  only to a neighbour. It arrives in the next superstep.

The sender is the name of the vertex that sent the message, which is how a
request/response protocol answers whoever asked without putting the sender
inside the payload. It is `box.NULL` for a message that has none: one a
combiner produced, since a combined message came from everyone who contributed
to it, and one delivered straight to a queue without a sender.

```lua
for from, message in vertex:pairs_messages() do
    vertex:send_message(from, answer_to(message))
end
-- A vertex that does not care who asked ignores the first value.
for _, message in vertex:pairs_messages() do
    -- ...
end
for _, neighbour, edge_value in vertex:pairs_edges() do
    vertex:send_message(neighbour, edge_value)
end
```

Aggregation, described under "Aggregators and combiners" below:

* `vertex:get_aggregation(name)` — the value the whole graph produced in the
  previous superstep.
* `vertex:set_aggregation(name, value)` — contribute this vertex's value.

Topology mutation:

* `vertex:add_vertex(value)`
* `vertex:add_edge([src = vertex:get_name(), ]dest, value)`
* `vertex:delete_vertex([name = vertex:get_name()][, edges = false])`
* `vertex:delete_edge([src = vertex:get_name(), ]dest)`

## Aggregators and combiners

An aggregator is a value every vertex can contribute to and read back. Each
worker keeps its own copy; a superstep ends with every worker reporting its
copy to the master, the master merging them, and the merged value going back
out to the workers. So a vertex entering superstep S reads what the whole graph
produced in S-1.

```lua
instance:add_aggregator('max_seen', {
    default = 0,
    reduce  = function(old, new) return new > old and new or old end,
})
```

* `default` — the starting value, or a function returning one. Both sides take
  a fresh copy of it every superstep, so a table default is not shared with the
  superstep before it.
* `reduce` — `callable(accumulator, contribution)`, folds one vertex's
  contribution into its worker's copy. Defaults to taking the contribution,
  which makes an aggregator with only a `merge` a per-superstep count.
* `merge` — `callable(accumulator, worker_value)`, folds one worker's copy into
  the master's. Defaults to `reduce`, which is what it usually is.

Both should be commutative and associative: nothing fixes the order workers
report in.

An aggregator is per-superstep on both sides. The master takes a fresh default
before each superstep's reports arrive; a worker's accumulator goes back to the
default the moment the master hands it the merged value, which is also the
moment the superstep that produced it ended. So each superstep aggregates its
own contributions and nothing else: four vertices each contributing 1 over
three supersteps leave 4, and a summing `reduce` sums that superstep rather
than the whole run.

The merged value and the accumulator are two different things, and
`get_aggregation` reads the merged one. A vertex therefore reads the same
number as every other vertex of its superstep — what the whole graph produced
in S-1 — rather than however much of its own worker's shard happened to be
computed before it.

Names beginning with `__` are reserved — pregel counts messages and active
vertices through `__messages` and `__in_progress`, which is what decides when a
run is over. Add the same aggregator on the master and on every worker, under
the same name; the app module's `aggregators` table does this for both sides at
once.

A combiner is different: it folds two *messages* for one receiver into one, so
a vertex with many incoming messages sees one. It runs on every put by default,
or once per superstep with `squash_only`.

## Topology mutation

`add_vertex`, `add_edge`, `delete_vertex` and `delete_edge` do not take effect
where they are called. Each is queued as a request on the worker that owns the
vertex it acts on, and the whole batch is applied between supersteps, in a
fixed order that makes the result independent of the order the requests
arrived in:

1. edge deletions,
2. vertex deletions,
3. vertex additions,
4. edge additions.

Adding vertices before edges is what lets one superstep add a vertex and an
edge pointing out of it. The one exception to the delay is a change to the
*running* vertex's own edges: `add_edge`/`delete_edge` with no explicit source
are applied to the vertex's own edge list and written back as soon as its
compute function returns.

Conflicts do not raise; they are logged and the run continues. Deleting an edge
or a vertex that is not there, adding an edge whose source does not exist, and
adding a vertex that already exists each produce a log line naming the vertex
and leave the graph as it was.

`delete_vertex` deletes the vertex and its outgoing edges. Deleting the inbound
edges as well is not implemented — only a full scan could find them — so the
`edges` argument must be `false`.

## The Avro module

`pregel.avro` is a self-contained pure-Lua Apache Avro implementation: schema
parsing with the canonical form and CRC-64-AVRO fingerprints, binary encoding
and decoding of every type, object container files, and schema resolution. It
is what `loader.avro_files` and `tools/text2avro.lua` are built on, and it is
usable on its own.

### Schemas

`avro.schema.parse(spec [, opts])` (also `avro.parse`) takes JSON text, a
decoded Lua table or an already parsed schema.

```lua
local avro = require('pregel.avro')
local sc = avro.schema.parse([[{
    "type": "record", "name": "Vertex",
    "fields": [{"name": "name", "type": "string"},
               {"name": "value", "type": "long"}]
}]])

sc.kind              --> 'record'
sc.fullname          --> 'Vertex'
sc:canonical()       --> {"name":"Vertex","type":"record","fields":[...]}
sc:tojson()          --> the full schema, with docs, aliases and defaults
sc:fingerprint()     --> CRC-64-AVRO of the canonical form, as int64 cdata
sc:fingerprint_hex() --> '547b814b11775a54'
```

`canonical()` is the Parsing Canonical Form, which strips everything not
needed to read the data. `tojson()` keeps it all, which is why it — and not the
canonical form — is what goes into a container file's header: a reader needs
the defaults.

### Values

```lua
local bytes = avro.encode(sc, {name = 'v001', value = 7})  --> 6 bytes
local value = avro.decode(sc, bytes)                       --> {name=, value=}
avro.validate(sc, {name = 'v001', value = 7})              --> true
avro.validate(sc, {name = 'v001'})                         --> false
```

`avro.decode(sc, data [, pos [, reader_schema]])` returns the value and the
position after it, so a concatenation of records can be walked; `avro.skip`
walks past one without building it. Lua maps onto Avro the obvious way, with
one wrinkle: a `null` nested in a record, array or map decodes to `box.NULL`
(exported as `avro.NULL`), because a Lua `nil` would take the key with it.
Ranges are enforced: an `int` outside 32 bits or a `long` outside 64 bits is
refused by `encode` and `validate` rather than wrapped, and a union such as
`["long", "double"]` picks the branch that can actually hold the value.

### Object container files

```lua
local w = avro.ocf.open('/tmp/graph.avro', {
    mode = 'w', schema = sc, codec = 'deflate', block_size = 64 * 1024,
})
w:append({name = 'v001', value = 7})
w:append_all({{name = 'v002', value = 9}})
w:close()

local r = avro.ocf.open('/tmp/graph.avro')
for record in r:records() do
    -- ...
end
r:close()
```

`open` takes `mode = 'r'` (the default) or `'w'`. A reader accepts `data`
instead of a path, to read a file already in memory, and `schema` — a *reader*
schema the records are resolved into. A writer takes `schema` (required),
`codec`, `block_size`, `metadata` and `sync`.

Three shorthands cover the common cases: `avro.ocf.read_all(path)` returns
every record plus the file's schema, `avro.ocf.write_all(path, sc, records)`
writes an array in one call, and `avro.ocf.schema_of(path)` returns the schema
and the metadata map without reading any records.

Codecs, and what each build does with them. What varies is not whether a file
can be read — every row of the `deflate` column produces and consumes ordinary
RFC 1951 — but whether it is compressed and by what.

| | CE, system libraries present | CE, no libraries | Enterprise |
| --- | --- | --- | --- |
| `null` | yes | yes | yes |
| `deflate`, writing | zlib through the FFI | stored blocks, uncompressed | Enterprise `compress.zlib` |
| `deflate`, reading | zlib through the FFI | the pure-Lua inflater | zlib through the FFI |
| `zstandard` | libzstd through the FFI | unavailable | Enterprise `compress.zstd` |

Reading `deflate` goes through the FFI binding under Enterprise as well, and
not through Enterprise's own module. That module honours `window_bits` when
compressing and ignores it when decompressing, so the raw deflate an Avro file
stores is write-only there; the FFI binding honours it both ways. Where no
libz can be loaded at all, the pure-Lua inflater takes over — which is what
makes a `deflate` file readable on any build whatsoever.

`avro.codec_available(name)` (also `avro.ocf.codec_available`) answers for the
running build and names the implementation as a second value:

```lua
avro.codec_available('null')       --> true, 'none'
avro.codec_available('deflate')    --> true, 'ffi/ffi'      -- writer/reader
avro.codec_available('zstandard')  --> true, 'ffi'
avro.codec_available('snappy')     --> false
```

The `deflate` value is `'<writer>/<reader>'`: `'enterprise/ffi'` under
Enterprise, `'ffi/ffi'` on a Community build with a system libz,
`'stored/pure-lua'` with neither. `avro.deflate.has_zlib` says whether writing
actually compresses, and `avro.deflate.has_raw_inflate` whether reading uses
zlib rather than Lua.

Setting `PREGEL_AVRO_PURE_LUA=1` in the environment — or
`avro.deflate.force_pure = true` at run time — selects the pure-Lua inflater
whatever else is available. The test suite uses it to exercise both readers in
one process; it is also the way to rule the FFI out when diagnosing something.

### Schema resolution

Data written with one schema can be read through another, following the
specification's Schema Resolution rules: the numeric promotions, string and
bytes either way, record fields matched by name or by a reader alias, a
reader field the writer never wrote filled from its default, an unknown enum
symbol falling back to the reader's `default`.

```lua
local reader = avro.schema.parse([[{
    "type": "record", "name": "Vertex",
    "fields": [{"name": "name", "type": "string"},
               {"name": "value", "type": "double"},
               {"name": "colour", "type": "string", "default": "none"}]
}]])

avro.decode(sc, bytes, 1, reader)  --> {name='v001', value=7, colour='none'}
```

`avro.resolver(writer, reader)` compiles the pair once and returns a decoder to
call per record, which is the cheaper form in a loop.
`avro.resolve.compatible(writer, reader)` is the shallow test the resolver uses
to pick a branch when only the reader is a union: matching kinds, matching
names for the named types, and the promotions. It is not a full answer to
whether the pair resolves — building the resolver is.

## pregel.compress

Tarantool Enterprise ships a `compress` module; Community Edition does not, and
that was the only reason the Avro codecs behaved differently on the two.
`pregel.compress` is that module where it exists and an FFI binding to the
system libraries where it does not, with the same API either way.

```lua
local compress = require('pregel.compress')

compress.implementation           --> 'enterprise' or 'ffi'
compress.available('zstd')        --> true / false

local z = compress.zlib.new({level = 6})
z:decompress(z:compress(s)) == s
```

The three codecs and their options, matching Enterprise's:

```lua
compress.zlib.new({level = 6, mem_level = 8, strategy = 'default',
                   window_bits = 15})
compress.zstd.new({level = 3})
compress.lz4.new({acceleration = 1, decompress_buffer_size = 1048576})
```

`strategy` is one of `default`, `filtered`, `huffman_only`, `rle`, `fixed`.
`level` is 0..9 for zlib and, for zstd, whatever range the linked libzstd
reports (-131072..22 on a current one). `decompress_buffer_size` is a real
limit and not a hint: an LZ4 block records neither its decompressed size nor a
checksum, so this is the largest output `lz4:decompress` will produce, and
Enterprise enforces the same 1 MiB default.

The output is Enterprise's byte for byte where the linked library versions
agree, which the test suite checks in both directions on `make test-ee`. On
this machine (zlib 1.2.12, zstd 1.5.7, lz4 1.9.4) all three match exactly.

### window_bits, the one deliberate difference

`window_bits` — 15 for zlib framing, -15 for raw RFC 1951 deflate, 31 for gzip
— is a superset. Enterprise honours it when compressing and ignores it when
decompressing: its `decompress` always expects the zlib frame and always
verifies the trailing adler32, so raw deflate is write-only there. (Re-framing
a raw block for it is not possible either: the adler32 is computed over the
decompressed bytes, which is what is not known yet.) Here the option reaches
`inflateInit2_` as well, which is what the Avro `deflate` codec needs.

`compress.ffi.zlib` is the FFI binding under every build, Enterprise included,
for exactly that reason. `compress.zlib` is the drop-in; reach for
`compress.ffi` only when the difference is the point.

### Finding the libraries

`ffi.load` is tried against, in order:

1. the running process, when it already exports the library's version symbol —
   Community Edition links zlib and zstd statically and exports them, and
   Enterprise 3.7 does the same for liblz4. Preferred because it needs no file
   and cannot skew against what the binary itself compresses with;
2. the plain soname (`z`, `zstd`, `lz4`) and the versioned ones (`libz.so.1`,
   `libz.1.dylib`, …);
3. `/opt/homebrew/lib`, `/opt/homebrew/opt/<name>/lib`, `/usr/local/lib`,
   `/usr/local/opt/<name>/lib`, `/usr/lib` and the Linux multiarch directories;
4. the directory named by `PREGEL_COMPRESS_LIBDIR`, as a last resort for a
   library none of the above reaches.

A candidate is accepted only once its version symbol resolves, so a file that
merely has the right name fails the lookup rather than the first real call. The
handle is cached per library. When nothing works the error names every path
tried:

```
pregel.compress: cannot load libzstd (tried: ffi.C (the process exports no
ZSTD_versionNumber), zstd, libzstd.so.1, /opt/homebrew/lib/libzstd.1.dylib, …)
```

`require('pregel.compress.lib').version('zstd')` reports what was bound.

## Testing and development

```
make deps     # tt rocks install luatest; tt rocks install luacheck
make lint     # luacheck over the whole tree
make test     # the suite, under the luatest wrapper's own tarantool
make test-ee  # the suite, under $(TARANTOOL_EE)
```

The suite covers both binaries because `pregel.compress` binds a different
implementation under each, and because the checks that the two agree can only
run where both are present. Under Enterprise everything runs. Under Community
Edition the seven tests that compare against the Enterprise module skip
themselves, as does the SSL transport test; the codec tests themselves run on
both, since the FFI bindings make `deflate` and `zstandard` work on either.

`test-ee` is `test-under` with `TARANTOOL` pointed at `TARANTOOL_EE`, which
defaults to a path that will not exist on another machine —
`make test-ee TARANTOOL_EE=/path/to/ee/tarantool`, or set it in the
environment. `make test-under TARANTOOL=...` runs the suite under any binary.

The Makefile exports a `VARDIR` of its own, keyed by a checksum of the checkout
path. luatest wipes its `VARDIR` at startup and the default is `/tmp/t`, shared
by every luatest on the host, so two checkouts running the suite at once would
delete each other's live servers. The path is a checksum rather than a
directory inside the checkout because every server puts a unix socket under it
and macOS caps socket paths at 103 bytes.

The tests are `test/unit` (no servers), `test/integration` (real multi-process
clusters), `test/helpers` (the two cluster harnesses — one starting bare
instances and calling `new()` over net.box, one writing a cluster config and
letting the roles applier do it), `test/apps` (app modules the roles tests
point at) and `test/fixtures` (graphs, and Avro files generated by fastavro
1.12.2 for the cross-implementation tests).

## License and credits

BSD, as declared in `pregel-scm-1.rockspec`.

The computational model is Google's Pregel; the shape of the API follows Apache
Giraph. Both are worth reading for algorithms to port — shortest paths and
PageRank are the canonical starting points:

* [The Pregel paper](http://kowshik.github.io/JPregel/pregel_paper.pdf), and
  the [JPregel site](http://kowshik.github.io/JPregel/) it comes from
* [Shortest paths](https://cwiki.apache.org/confluence/display/GIRAPH/Shortest+Paths+Example)
  and [PageRank](http://giraph.apache.org/pagerank.html) in Giraph
