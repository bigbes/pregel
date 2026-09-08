# API design review and a proposal for v2

## 1. Purpose and how to read this document

The port to Tarantool 3 kept the 2016 API and changed everything underneath it.
That was the right order — a port that also redesigns its surface cannot be
checked against the thing it ported. But it means the public API of this
library has never been designed; it has been *inherited*, and the review bugs
of the last few days are the first evidence about it. Three closed bugs
(`pregel-2qk.4`, `pregel-hr3`, `pregel-iv7`) all come out of the same four
vertex methods that take an optional leading source argument, and the fix in
each case was to make the ambiguity work rather than to remove it.

This document is the design pass that was deferred. Section 2 states what the
API is today; section 3 states what is wrong with it and how each claim was
established; section 4 proposes replacements as signatures rather than as
principles; sections 5 and 6 cover migration and the decisions that are not
mine to make.

Every claim about current behaviour in sections 2 and 3 was either read out of
the code (cited as `file:line`), taken from a test that pins it (cited by test
name), or measured. Measurements were made in this checkout at `8221da0`, on
Tarantool 3.9.0-entrypoint, either in-process against `test/helpers/fake_pregel`
or by running `examples/max-value` under `tt`.

Work that is landing in parallel is described where it touches a problem below,
marked as landing rather than as current state:

- `pregel-60o` — `roles_cfg` loses `workers`, `master`, `user` and `password`;
  topology and credentials come from the cluster config, the way vshard's do.
- `pregel-3v3` — a vertex that does not vote during `compute` is halted
  afterwards, instead of staying active forever.
- `pregel-3wg` — `max_supersteps` on `master.new`, so a job that does not
  converge stops instead of looping.
- `pregel-4l0` — `pregel.compress`, an ffi layer under the Avro codecs.
- `pregel-bkk.3` and `pregel-6p5.3` — the `lookalike` and `mf` examples, the
  first two apps in this repository with more than one kind of vertex.

The v2 proposed here is not a rewrite. It is a change of surface over the same
master, worker, queue and mpool; every problem in section 3 is in the argument
lists and the contracts, not in the machinery.

## 2. The API today

### 2.1 The programmatic master and worker

`pregel.master.new(name, options)` (`pregel/master.lua:255`) and
`pregel.worker.new(name, options)` (`pregel/worker.lua:842`) are the layer
everything else is built on. Both take an instance name — which names the
spaces and is how peers address the job — and a flat options table.

The master's options: `workers`, `obtain_name` (required), `pool_size`,
`master_preload`, `preload_args`, `user`, `password`, `connect_async`,
`connect_timeout`.

The worker's options: `workers`, `master` (required), `compute` (required),
`obtain_name` (required), `combiner`, `squash_only`, `queue_engine`,
`pool_size`, `delayed_push`, `worker_context`, `worker_preload`,
`preload_args`, `user`, `password`, `connect_async`, `connect_timeout`,
`grant_to`.

The master object has exactly seven methods. Measured on the running
`max-value` example by enumerating `getmetatable(m).__index`:

    add_aggregator, preload, preload_on_workers, save_snapshot, start, stop,
    wait_up

`wait_up`, `preload`, `preload_on_workers` and `add_aggregator` return `self`
and chain; `start()` blocks for the whole job and returns the superstep count
(`pregel/master.lua:97`). The worker object is not driven by anyone: its only
public method is `stop()` (`pregel/worker.lua:633`), and everything else that
happens to it arrives as a protocol message through
`pregel.worker.deliver`.

`master.grant(user)` and `worker.grant(user[, instance_name])`
(`pregel/master.lua:220`, `pregel/worker.lua:772`) hand out `execute` on
`lua_call` for the four registry entry points, and — given an instance name —
read/write on that instance's spaces and their sequences. The two halves are
separate because the entry-point names exist before any instance does and the
space names do not.

Lifecycle in full, from the README's own example: `pworker.grant`,
`pmaster.grant`, `worker.new`, `master.new`, then
`master:wait_up():preload():start()`. There is no `master:status()`, no
`master:stop_after_this_superstep()`, and no callback of any kind during
`start()`.

### 2.2 The app-module contract, as the roles consume it

A role is given a Lua module name in `roles_cfg.app` and `require()`s it
(`pregel/roles/common.lua:340`). The module returns a table. What the roles
read out of it:

- `compute(vertex)` — required by the worker role, not by the master
  (`pregel/roles/worker.lua:139` versus `pregel/roles/master.lua:103`).
- `obtain_name(value) -> string` — required by both.
- `combiner(a, b) -> c` — optional, read only by the worker role.
- `worker_preload` / `master_preload` — a loader object, a
  `callable(instance, app_cfg)` returning one, or nil
  (`pregel/roles/common.lua:248`).
- `worker_context` — any value, or a `callable(app_cfg)` returning one
  (`pregel/roles/common.lua:386`).
- `aggregators = {[name] = {default, reduce, merge}}` — checked strictly, and
  a name beginning with `__` is refused (`pregel/roles/common.lua:275`).

Everything else in the module is invisible to the roles. `app_cfg` reaches the
module through exactly two doors: as the second argument of the preload
builder, and as the argument of a callable `worker_context`.

The master role decides how to load by looking at the app module rather than at
the config (`pregel/roles/master.lua:169-178`): a `master_preload` runs
`instance:preload()`, otherwise a `worker_preload` runs
`instance:preload_on_workers()`, otherwise nothing is loaded and the job runs
over whatever the workers already hold. An app that defines both gets the
master-side one.

### 2.3 roles_cfg

Common to both roles (`pregel/roles/common.lua:110`): `name` (required), `app`
(required), `app_cfg`, `workers`, `pool_size`, `user`, `password`,
`connect_timeout`. The worker role adds `master`, `delayed_push`,
`squash_only`, `queue_engine` (`pregel/roles/worker.lua:90`); the master role
adds `autostart` (`pregel/roles/master.lua:66`).

An unknown key is refused by name rather than ignored, because the config
framework validates the shape of `roles_cfg` and knows nothing about the keys
inside a role's own table (`pregel/roles/common.lua:187-191`). `app_cfg` is the
one option the roles do not interpret at all.

`workers` and `master` may be omitted, in which case the role reads the cluster
config and finds the instances running the other role for a job of the same
`name` (`pregel/roles/common.lua:829`). A replicaset is one participant, not
one per instance.

`apply()` never blocks and never connects: it builds the message pool with
`connect_async = true` and hands the waiting to a fiber
(`pregel/roles/common.lua:606`), because it runs inside the config framework's
synchronous `post_apply` and a raise from there at startup exits the process.
A running job cannot be reconfigured (`pregel/roles/worker.lua:159-167`).

Neither `get()` nor `status()` is part of the role contract; both are exported
alongside `validate`/`apply`/`stop` for an operator to reach from a console.

### 2.4 The vertex API

The compute function is handed one vertex object. The objects are pooled and
reused across the vertices of a superstep (`pregel/vertex.lua:406-468`), so the
same table serves thousands of graph vertices.

Base (`pregel/vertex.lua:110-243`):

- `vertex:get_name()`
- `vertex:get_value()` / `vertex:set_value(value)`
- `vertex:get_superstep()` — counting from 1
- `vertex:vote_halt([is_halted = true])`
- `vertex:get_worker_context()`

Messaging:

- `vertex:pairs_messages()` — yields `(key, message)`, where the key is the
  engine's own iteration state and is not meaningful
  (`pregel/vertex.lua:161-168`, `pregel/queue.lua:50-59`).
- `vertex:pairs_edges()` — yields `(index, destination, value)`, walking the
  edge list as it stood at the start of the superstep.
- `vertex:send_message(receiver_name, value)` — to any vertex by name, not only
  to a neighbour; readable in the next superstep.

Aggregation:

- `vertex:get_aggregation(name)` — the merged value from the previous
  superstep, the same for every vertex of this one (`pregel/vertex.lua:230`).
- `vertex:set_aggregation(name, value)` — folds into this worker's accumulator.

Topology mutation, all queued and applied between supersteps except a change to
the running vertex's own edges:

- `vertex:add_vertex(value)`
- `vertex:add_edge([src = self:get_name(), ]dest, value)`
- `vertex:delete_vertex([name = self:get_name()][, edges = false])`
- `vertex:delete_edge([src = self:get_name(), ]dest)`

There is a fifth private method, `write_solution` (`pregel/vertex.lua:85`),
reachable through `vertex.pool_new`'s `write_solution` option. Nothing
constructs a pool with it: `pregel/worker.lua:917` passes `compute` and
`pregel` only. It was already dead in 2016 — the old worker imported
`vertex.vertex_private_methods.write_solution` at its line 27 and never called
it.

### 2.5 Aggregators and combiners

An aggregator is declared identically on the master and on every worker, under
the same name, because a worker reports its copy by name and the master looks it
up by name (`pregel/master.lua:179`, `pregel/worker.lua:506`). The app module's
`aggregators` table is what makes the two sides agree
(`pregel/roles/common.lua:410`).

Each aggregator holds two values (`pregel/aggregator.lua:12-23`): `value`, this
superstep's accumulator, and `global`, the master's merged value from the
previous superstep. `reduce` folds a vertex's contribution into `value`; `merge`
folds a worker's `value` into the master's, and defaults to `reduce`.
`get_aggregation` reads `global`. Keeping them apart is what stopped a summing
aggregator from multiplying itself once per worker per superstep
(`pregel-3e8`; regression `worker.test_aggregator_starts_each_superstep_from_the_default`).

The aggregator object is also callable (`pregel/aggregator.lua:122-127`):
`agg(value)` contributes, `agg()` reads the local accumulator.

A combiner is a different thing: `callable(a, b) -> c` folding two *messages*
for one receiver into one. It runs on every put by default, or once per
superstep under `squash_only` (`pregel/queue.lua:104-118`, `worker.lua:351`).

`__messages` and `__in_progress` are pregel's own aggregators and are what
decide when a run is over (`pregel/master.lua:127-133`).

### 2.6 Loaders, `app_cfg` and path resolution

A loader is a callable object. Calling it walks a source and pushes the graph
out through the instance's mpool, addressing each vertex and edge to the worker
that owns it. The methods a loader function calls on itself
(`pregel/loader.lua:37-98`):

- `store_vertex(value) -> name`
- `store_edge(src, dest, value)`
- `store_edges_batch(src, list)`
- `store_vertex_edges(value, list) -> name`
- `flush()`

Three constructors: `loader.new(instance, fn)`,
`loader.graph_edges_f(instance, path)` for the two-section text format, and
`loader.avro_files(instance, options)`, which streams a pair of object
container files and splits itself by the pool's own hash so N workers reading
the same two files cover the graph exactly once
(`pregel/loader.lua:429-498`).

Path resolution is not in the library. It is in `examples/common.lua`, and it
exists because an app module has no idea where it is:

- `common.here()` (`examples/common.lua:25`) walks one stack frame up with
  `debug.getinfo(level, 'S')` and takes the directory of the caller's source
  file. Every example calls it at module scope and stores the result in a local
  called `HERE` (five call sites, e.g. `examples/max-value/app.lua:25`).
- `common.resolve(dir, path, what)` (`examples/common.lua:37`) joins an
  `app_cfg` path onto that directory unless it is already absolute.
- `common.cfg(app_cfg, required)` (`examples/common.lua:51`) substitutes an
  empty table for nil and names a missing key rather than failing on a nil
  index later.

The reason `HERE` exists is stated in that file's header: an example is started
both by `tt` from the example directory and by the test suite from the
repository root, and `tt` gives every instance a working directory of its own
under `var/lib`. Neither the process's cwd nor the config's is a usable base,
so the only stable anchor is where `require` found the module.

### 2.7 Avro and math, where the graph API touches them

`pregel.avro` is reached by the graph API through exactly one door:
`loader.avro_files` (`pregel/loader.lua:367`). Its option values are either a
field name of the file's own schema or a `function(record)`, and a field name is
checked against the schema when the loader is built rather than per record
(`pregel/loader.lua:274-297`). `vertex_value` defaults to the whole record, and
the comment at `pregel/loader.lua:385-387` says why: the worker names a stored
vertex by calling `obtain_name` on it, so a value stripped to one field would
arrive somewhere it cannot be named.

`pregel.math` does not touch the graph API at all — it is arrays and tables, so
that a weight vector can be a message payload and a percentile counter can be a
vertex value (`pregel/math/init.lua:17-21`). It is worth noting here only
because its objects have the shape section 4 proposes for aggregators:
`auc.new()` gives `:add(score, label)` and `:result()`
(`pregel/math/auc.lua:124-126`), `percentile.new()` gives `:add(v)` and
`:percentile(p)` (`pregel/math/percentile.lua:142-143`).

## 3. Problems

### 3.1 Overloads told apart by argument type or by nil

Three of the closed review bugs are the same defect in three methods, and the
API shape is what made all three possible.

`vertex:add_edge` (`pregel/vertex.lua:288`) tells its two forms apart by
whether the third argument is nil. Measured against the fake instance:

    v = <vertex named 'alice'>
    v:add_edge('bob', 'carol', nil)
    -- alice's own edge list becomes {{'bob', 'carol'}}
    -- nothing is routed to the worker that owns 'bob'

The caller asked for an edge from `bob` to `carol` with no value and got an
edge from `alice` to `bob` whose value is the string `'carol'`. Nothing raises.
The doc comment already tells the caller to pass `json.NULL` instead of nil,
which is an API asking to be worked around.

`vertex:delete_edge` (`pregel/vertex.lua:350`) shifts the same way. Measured:
`v:delete_edge('bob', nil)` on a vertex named `alice` deletes `alice`'s own
edge to `bob`, rather than reporting that a source was named with no
destination.

`vertex:delete_vertex` (`pregel/vertex.lua:323`) tells `delete_vertex(true)`
from `delete_vertex('name')` by testing `type(vertex_name) == 'boolean'`. This
one was already the subject of `pregel-2qk.4` (the argument shift) and then of
`pregel-hr3`, which found that the branch existing to support it had no test at
all: the mutant deleting that branch — now `pregel/vertex.lua:326-329` — left
the whole suite green until
`vertex.test_delete_vertex_with_the_flag_alone` was added.

The overload also gives one method two implementations, and they diverged.
`vertex:delete_edge(dest)` queues locally and `compute()` removes every
parallel edge to the destination in one pass (`pregel/vertex.lua:105-107` and
`58-67`); `delete_edge(src, dest)` goes to the worker's delayed path, which
stopped at the first match until `pregel-iv7` was fixed
(`pregel/worker.lua:419-426` now removes every match). Which form the caller
wrote decided how many edges went, silently, and the divergence was found by an
agent reading the doc comments rather than by any test.

That fix has already left a stale claim behind it: the doc comment at
`pregel/worker.lua:611-616` still says "One request removes one edge:
apply_topology_mutations stops at the first match ... which form was used
decides how many edges go", which the loop it describes no longer does. A
method with one implementation could not have grown that comment.

A fourth instance of the same pattern is in the app contract:
`common.worker_context` (`pregel/roles/common.lua:386`) decides whether the
app's `worker_context` is a value or a builder by `is_callable`. Its own
comment states the consequence — an app whose context genuinely is a function
has to wrap it in a table.

The cost is not that these are hard to use once learned. It is that a wrong
call is silently a different, legal call. Type-dispatched overloads have no
arity to check and no nil to catch.

### 3.2 `obtain_name` is required everywhere, and the name is never given

A vertex has no name of its own. Its name is computed from its value by
`obtain_name`, at four different places:

- the loader, to route (`pregel/loader.lua:47`),
- the worker, again, to store (`pregel/worker.lua:545`),
- `vertex:add_vertex`, to route (`pregel/vertex.lua:265`),
- the worker again, to key the topology mutation
  (`pregel/worker.lua:578`).

Three consequences, all measured or cited:

- It is required even where it means nothing. `master.new{workers = {}}` with
  no loader at all raises `options.obtain_name must be callable`
  (`pregel/master.lua:263`; measured). A master that only coordinates has
  nothing to name.
- A vertex value cannot be reduced. `loader.avro_files`'s `vertex_value`
  defaults to the whole Avro record specifically because a stripped value
  cannot be named afterwards (`pregel/loader.lua:385-387`). So the storage
  layout of every vertex is decided by what `obtain_name` happens to need.
- It becomes a type dispatcher in any app with more than one kind of vertex.
  The 2016 look-alike app's `obtain_name` (`7fba5d4^:test-avro/utils.lua`) is
  twenty lines that branch on `value.vtype`, format a `'<type>:<key>'` string,
  and end in `assert(false)`. The two examples landing now
  (`pregel-bkk.3`, `pregel-6p5.3`) both need the same thing: `mf` prefixes
  names `u:` and `i:` to keep users and items apart.

The name is the one piece of identity pregel actually uses — it routes, stores
and addresses by exactly that string — and it is the one piece the caller is
not allowed to state.

### 3.3 `get_value` / `set_value` and the in-place trap

`set_value` is what sets `__modified`, and `__modified` is what makes
`compute()` write the tuple back (`pregel/vertex.lua:196-205`, `48-81`).
Mutating the table returned by `get_value()` therefore changes nothing that
survives the superstep. Measured: a compute function doing
`local v = self:get_value(); v.n = v.n + 1` over a vertex whose value is
`{n = 1}` leaves `compute()` returning false and zero `replace` calls on the
data space.

The doc comment warns about this, which is the tell. Every example works around
it by rebuilding the whole value: `examples/max-value/app.lua` writes
`self:set_value({id = vertex.id, name = vertex.name, value = best})` — three
fields copied to change one. `examples/wcc/app.lua` and `examples/sssp/app.lua`
do the same. The 2016 code wrote a two-line helper for it
(`node_common.set_status`, which reads, assigns and writes back) — which is
§4.7's proposal, ten years early and in the app rather than the library.

### 3.4 Anything set on the vertex object leaks to the next vertex

`apply()` (`pregel/vertex.lua:28-37`) resets six fields and clears the two
pending-edge arrays. Any other key an app writes onto the vertex object stays
there for whatever vertex the pool hands out next. Measured, with a compute
function that reads `self.scratch` and then sets it:

    compute(alice) saw self.scratch = nil
    compute(bob)   saw self.scratch = alice

This is not hypothetical. The 2016 look-alike app read `self.idType` at
`node_data.lua:89` to build a config key. `idType` is assigned nowhere in that
tree — `git grep idType 7fba5d4^ -- test-avro` returns that one line — so the
key was always `'<task>:nil'`, the lookup always missed, and the
prediction-threshold branch never fired. A vertex object that is an open table
with a metatable makes that a silent nil rather than an error.

The same openness is what the 2016 app used deliberately for typed dispatch:
`computeGradientDescent` (`7fba5d4^:test-avro/common.lua`) saves the vertex's
metatable on first call, `setmetatable`s the pooled object to one of three
per-type tables built by copying `pregel.vertex.vertex_methods`, calls
`compute_new`, and puts the original metatable back. That is what an app must
do today to get typed vertices, and it depends on `vertex_methods` being
exported (`pregel/vertex.lua:474`) and on the pool never noticing.

### 3.5 Two settings channels for one setting

`roles_cfg.app_cfg` reaches the app module twice, in two different shapes:

- as the second argument of `master_preload` / `worker_preload`, passed as
  `options.preload_args` and applied by `worker_new` at
  `pregel/worker.lua:898-905`,
- as the argument of a callable `worker_context`, resolved by the role before
  `worker.new` is called (`pregel/roles/worker.lua:188`).

So an app that needs the same value in both places reads it twice, through two
different mechanisms, with two different error behaviours — the preload builder
raises inside `worker.new`, the context builder raises inside
`common.worker_context` with a message about failing to build a context.

Worse, the master has no `worker_context` option at all: `master.new`'s option
list (`pregel/master.lua:230-239`) does not include it, and the master role
never calls `common.worker_context` (`pregel/roles/master.lua:242-253`). So
`obtain_name` — which the master needs, and which must agree with the workers'
— cannot be configured from `app_cfg` on the master side. Any app whose naming
depends on configuration has to smuggle it through a module-level upvalue read
at `require` time, which is exactly what the 2016 app did with its
`do ... end` worker-context block.

### 3.6 `autostart` re-implements the master lifecycle

`autostart_body` (`pregel/roles/master.lua:163-202`) is sixty lines that call
`wait_up`, choose between `preload` and `preload_on_workers`, call `start`, and
maintain a five-state status table. It also has to distinguish its own
cancellation from a job failure (`is_cancelled`, and the `state.master ~=
instance` test), and it needs `autostart_traceback` because `xpcall_tb` would
log a cancellation as an error — a defect that was filed and fixed
(`pregel-dxl`).

None of that belongs to the role. It is the master's own lifecycle, written
outside the master because the master has no lifecycle API — no `run()` that
can be driven from a fiber, no status, no way to be told to stop between
supersteps. The proof that it is the master's business and not the role's:
`master:start()` already publishes `superstep_count` as it goes
(`pregel/master.lua:105`) precisely so something outside can watch, and the
role's `status()` reads that field directly (`pregel/roles/master.lua:368`).

The same gap shows up as an operator-visible seam. A job driven by hand through
`get()` moves `status().superstep` but leaves `status().state` at `idle`
forever, because only the autostart fiber writes the other states — stated at
`pregel/roles/master.lua:333-334` as a known limitation.

### 3.7 The aggregator is three things behind one name

`agg(value)` contributes, `agg()` reads, and `agg:get_global()` reads a
different value. Measured:

    agg(nil)  -> 0        -- a read, not a contribution; nothing is stored
    agg(5); agg()  -> 5   -- the local accumulator
    agg:get_global()  -> 0  -- what a vertex would read

An aggregator therefore cannot be given nil, and the doc comment says so
(`pregel/aggregator.lua:117-118`). More to the point, `agg()` and
`get_aggregation` answer different questions and the callable form is the one
that looks like the obvious accessor. That confusion is exactly the second half
of `pregel-3e8`: `vertex:get_aggregation` used to call `aggregators[name]()`,
so a vertex read its own worker's partial accumulator — measured then as
`[501, 2501, 10501]` where `[0, 2000, 2000]` was expected.

The trap is still live on the master. Measured on the finished `max-value`
example:

    m.aggregators.max_seen.value        -> 999987   -- the answer
    m.aggregators.max_seen:get_global() -> 0        -- never informed

`get_global()` is the documented accessor and it is wrong on the master,
because `inform_workers` writes to the workers' copies and never to the
master's own (`pregel/aggregator.lua:43-47`).

And there is no public accessor at all. Reading a finished job's answer means
reaching into `m.aggregators.<name>.value` — a field of a field. That is what I
had to do above.

### 3.8 `roles_cfg` duplicates the programmatic options

Nine of the fourteen `roles_cfg` keys are a straight pass-through to
`worker.new` or `master.new`: `pool_size`, `user`, `password`, `workers`,
`master`, `delayed_push`, `squash_only`, `queue_engine`, and — via the
app module — `app_cfg` as `preload_args`. Each one is written three times: in
`common_spec` or the role's `SPEC` with its checker, in the `apply()` call, and
in the README's roles_cfg reference. `pregel-ilf` is what an incomplete third
copy costs: `validate()` accepted an empty `name`, `app`, `master` or `user`
until someone went through them one at a time.

Four of the nine are going away in `pregel-60o` — `workers`, `master`, `user`,
`password` — which is the right direction and does not address the shape. The
five that remain are still tuning knobs of a message pool and a queue,
spelled once in YAML and once in Lua.

### 3.9 Typed vertices exist and the library does not know it

Nothing in the library has a concept of a vertex type, so every app that needs
one invents the same three things: a discriminator field in the value, a
prefix or a branch in `obtain_name`, and a dispatch at the top of `compute`.

- 2016: `value.vtype`, `obtain_name` branching on it
  (`7fba5d4^:test-avro/utils.lua`), and metatable swapping in
  `computeGradientDescent`.
- `pregel-bkk.3`, in progress: MASTER, TASK and DATA vertices, with per-phase
  behaviour on each.
- `pregel-6p5.3`: `u:` and `i:` name prefixes over a bipartite graph.
- `examples/topology-mutation/app.lua` already has a degenerate case — it
  branches on `value.orphan_of ~= nil` at the top of `compute` to tell a marker
  vertex from a real one.

The library's own dispatcher is `info_functions` on the worker
(`pregel/worker.lua:66`), keyed by message name. Apps are doing the same thing
by hand, worse, and each one differently.

### 3.10 No progress and no observability on the programmatic API

`master:start()` blocks for the whole job and returns a number. During it:

- the only progress signal is `master.superstep_count`, a bare field;
- there is no callback per superstep, so an app cannot decide to stop, cannot
  checkpoint, and cannot report;
- there is no way to ask a worker how far it is except by reading its `status()`
  through the role.

Two consequences already visible. The `max_supersteps` feature (`pregel-3wg`)
has to go into `master.new`'s options and into the loop itself, because there is
no other place to put a decision that is taken once per superstep. And the
worker role's `status()` cannot tell a finished job from a running one:
measured on `max-value` after the master reported `state: done, superstep: 12`,
`require('pregel.roles.worker').status()` on worker1 still answered
`state: running, in_progress: 0, messages: 0`. The worker has no notion of the
job being over, because nothing ever tells it.

### 3.11 `HERE`

`examples/common.lua:25` reads `debug.getinfo(level, 'S')` to find out where the
app module lives, so that a relative path in `app_cfg` has a base. It is
correct, it is well documented, and it is in the wrong repository layer: every
app that reads a file needs it, and it is a helper of the examples.

Its own guard shows the fragility — an app module loaded from a string rather
than a file has no directory, and `here()` raises telling the caller to use an
absolute path instead. The `level` argument makes it a positional-stack-frame
API: correct only when called directly from the app module's own chunk, silently
wrong from a helper.

The role knows the answer already. It called `require(cfg.app)`, so
`package.searchpath(cfg.app, package.path)` in the role gives the same
directory without a stack walk.

### 3.12 Smaller things found while reading

- `write_solution` is dead surface (§2.4). `vertex.pool_new` accepts it,
  `vertex_private_methods.write_solution` exists, and no call site constructs
  the pool with it (`pregel/worker.lua:917`). Dead in 2016 too.
- `pairs_messages()` and `pairs_edges()` both yield a leading value that means
  nothing. Every call site in the repository writes `for _, message in` or
  `for _, destination in` — 12 of them across the five examples and
  `test/apps/maxvalue.lua`. An iterator whose first return is always discarded
  is a shape to fix, not a convention to document.
- Out-degree costs a loop. `examples/pagerank/app.lua` counts edges with
  `for _ in self:pairs_edges() do out_degree = out_degree + 1 end` to compute
  `rank / out_degree`; the count is `#self.__edges` and there is no accessor.
- There is no way to store a single vertex from outside a loader. The 2016 app
  had to reach into the pool: `master.mpool:by_id('MASTER:'):put('vertex.store',
  {...})` followed by `master.mpool:flush()`
  (`7fba5d4^:test-avro/common.lua`). That is the internal protocol, written by
  hand, in an app.
- There is no result API. Every example README reads the answer with
  `box.space.data_<job>:pairs()` on each worker in turn
  (`examples/sssp/README.md:107`, `examples/max-value/README.md:116`,
  `examples/topology-mutation/README.md:101`). The space name and the tuple
  layout are therefore public interface, and they are documented as such in the
  worker's header (`pregel/worker.lua:8-13`).
- `master:start()` has no upper bound on supersteps and a compute function that
  never votes leaves its vertex active forever
  (`pregel/worker.lua:297` sets `vote_halt(false)` before every compute). Both
  halves are being fixed — `pregel-3v3` and `pregel-3wg` — and both are
  symptoms of the same thing: termination is a property of the app with no
  guard in the library.

## 4. Proposal for v2

Eight changes and a list of small ones. Each is stated as a signature, with
what it replaces, why, what it costs, and a before/after taken from an example
in this repository.

### 4.0 The two references these signatures are taken from

None of the shapes below is invented. Two bodies of prior art apply, and the
README already names one of them.

**Giraph**, which `README.md:9-10` and `README.md:872-873` cite as the API this
one grew from. From memory rather than from a checkout, so treat it as a
direction and not as a specification:

- `Computation.compute(Vertex vertex, Iterable<M> messages)` — the vertex and
  its messages are two arguments, not one object with an iterator method. That
  is §4.3.
- A vertex has an id given to it at input time by the `VertexInputFormat`; it
  is not derived from the value by a hash function the app supplies. That is
  §4.2, and it is the single largest divergence between this library and the
  API it claims to follow.
- `Vertex.getNumEdges()` exists, which is §3.12's out-degree loop.
- Aggregators are reached through the Computation — `aggregate(name, value)`
  and `getAggregatedValue(name)` — and registered by a `MasterCompute`.
- `MasterCompute.compute()` runs on the master once per superstep and may call
  `haltComputation()`. That is exactly §4.5's `on_superstep`, and it is the
  piece this library has never had.

**vshard**, as the closest Tarantool library in shape and the one Tarantool 3
integrates most deliberately. Read at `/Users/blikh/data/workspace/vshard`:

- `vshard.router.new(name, cfg)` returns an object whose methods live in one
  metatable (`vshard/router/init.lua:1712-1741`), and the module-level
  functions are generated from that same table and bypass to a static instance
  (`:1747-1752`). `pregel.master`'s object is the same shape already; what it
  lacks is `info()`, which vshard has on both the router and the storage
  (`vshard/storage/init.lua:4270`). §4.5's `master:status()` is that method.
- The Tarantool 3 config drives vshard without a role-specific topology
  section: `sharding.roles` says what an instance is,
  `iproto.advertise.sharding` says where it is reached, and a **credentials
  role named `sharding`** carries the privileges — verified against the
  Tarantool 3.9 binary, which contains the strings `iproto.advertise.sharding`,
  `sharding.roles` and the check "Check that the vshard storage user has the
  credential sharding role." That is precisely the arrangement `pregel-60o`
  proposes to copy, and it is why §3.8's remaining five keys are a different
  case from the four that are leaving.

### 4.1 An `app` object, handed to the app module

Replaces: `HERE`, `common.resolve`, `common.cfg`, and the twice-passed
`app_cfg`.

The role builds one object and hands it to every app entry point that needs
configuration. It knows the module's directory because it resolved the module.

    -- what the role builds, once per apply
    app_ctx = {
        cfg  = <roles_cfg.app_cfg, never nil>,
        dir  = <absolute directory the app module was loaded from>,
        path = function(self, rel)  -- rel joined onto dir, absolute passed through
        log  = <a log table prefixed with the job name and the role>,
        name = <job name>,
        role = 'worker' | 'master',
    }

    -- what an app module then exports
    function app.setup(ctx)        -- optional; returns the worker context
    function app.load(ctx)         -- optional; returns a loader
    function app.load_on_worker(ctx, idx, count)   -- optional
    function app.obtain_name(value, ctx)           -- ctx available on both sides

Why: it collapses two channels into one (§3.5), it gives the master a
configuration channel it does not have (§3.5), it removes the stack walk
(§3.11), and `ctx.cfg` being never-nil removes `common.cfg`'s reason to exist.
`ctx:path()` is where `common.resolve` goes.

Cost: every app module changes shape. There are six in the tree today (five
examples plus `test/apps/maxvalue.lua`) and two landing. The roles gain
one small builder; `master.new` and `worker.new` gain nothing — the object is
built above them and captured by the closures they are handed.

Before (`examples/sssp/app.lua`):

    local common = require('examples.common')
    local HERE = common.here()

    function app.worker_context(app_cfg)
        local cfg = common.cfg(app_cfg, {'source'})
        return {source = cfg.source}
    end

    function app.worker_preload(instance, app_cfg)
        local cfg = common.cfg(app_cfg, {'vertices', 'edges'})
        return loader.avro_files(instance, {
            vertices = common.resolve(HERE, cfg.vertices, 'vertices'),
            edges    = common.resolve(HERE, cfg.edges, 'edges'),
            ...
        })
    end

After:

    function app.setup(ctx)
        return {source = assert(ctx.cfg.source, 'app_cfg.source is required')}
    end

    function app.load_on_worker(ctx)
        return loader.avro_files(ctx, {
            vertices = ctx:path(ctx.cfg.vertices),
            edges    = ctx:path(ctx.cfg.edges),
            ...
        })
    end

`examples/common.lua` disappears, and with it the three helpers every example
requires today.

### 4.2 Explicit vertex names

Replaces: `obtain_name` as a required option.

    loader:store_vertex(name, value)
    loader:store_edge(src_name, dst_name, value)
    loader:store_edges_batch(src_name, list)
    loader:store_vertex_edges(name, value, list)

    vertex:add_vertex(name, value)

    -- loader.avro_files keeps `vertex_name`, which becomes the only place a
    -- name is derived from a record, and it already exists.

`obtain_name` becomes optional and is used for exactly one thing: naming a
value that arrives without a name, which after this change is nothing in the
core. Apps that want a derived name write the derivation at the call site, where
it is one expression.

Why: the name is what pregel routes, stores and addresses by (§3.2). Making it
an argument removes the four hidden call sites, removes the coupling between
naming and value layout (`pregel/loader.lua:385-387`), removes the requirement
from a master that only coordinates, and turns the 2016 twenty-line
`obtain_name` into a prefix at the point of storage.

Cost: this is the largest breaking change on paper and a small one in this
tree, because the five examples load through `loader.graph_edges_f` and
`loader.avro_files`, which are library code. Measured app-level scope: one
`store_vertex` call (`test/apps/maxvalue.lua:74`), one `add_vertex` call
(`examples/topology-mutation/app.lua:80`), and six `obtain_name` definitions
that go away — one in each of the five examples and one in the test app.
`store_edge` and `store_edges_batch` already take names and do not change.
Inside the library, the two loaders and the four call sites of §3.2.

Before (`test/apps/maxvalue.lua:31-33, 74`):

    function app.obtain_name(vertex) return vertex.name end
    ...
    self:store_vertex(app.vertex(i))

After:

    -- no obtain_name at all
    local v = app.vertex(i)
    self:store_vertex(v.name, v)

Before (`examples/topology-mutation/app.lua:80`):

    self:add_vertex({name = self:get_name() .. ':orphan',
                     orphan_of = self:get_name()})

After:

    self:add_vertex(self:get_name() .. ':orphan',
                    {orphan_of = self:get_name()})

Note the second gain: the value no longer has to carry its own name, so
`orphan_of` is the whole vertex.

### 4.3 `compute(vertex, messages, ctx)` with typed dispatch

Replaces: `compute(self)`, the value-carried type discriminator, and the
metatable swapping of §3.4/§3.9.

    -- single-type app, unchanged in spirit
    app.compute = function(vertex, messages, ctx) end

    -- typed app: a table of compute functions
    app.compute = {
        data = function(vertex, messages, ctx) end,
        task = function(vertex, messages, ctx) end,
        master = function(vertex, messages, ctx) end,
    }
    app.vertex_type = function(vertex) return vertex:get_type() end  -- optional

A vertex gains a type: `store_vertex(name, value, {type = 'data'})` and
`vertex:get_type()`. The default type is `nil`, and an app with a plain function
`compute` never sees any of this. With a table `compute`, the worker looks the
function up by type and raises naming the vertex and the type when there is no
handler — instead of `assert(false)` in the app
(`7fba5d4^:test-avro/node_common.lua`).

`messages` is a plain array, not an iterator, and `ctx` is the worker context
(§4.1) rather than a method call. Two reasons beyond convenience: an iterator
whose first value is always discarded is not an interface (§3.12), and passing
the context explicitly makes it visible in the signature that it is per-worker
and shared.

Why: every app with more than one kind of vertex has invented this (§3.9), and
the mechanism they invented reaches into `pregel.vertex.vertex_methods` and
mutates a pooled object's metatable. A table of functions keyed by type is what
the worker's own `info_functions` already is.

Once typed dispatch exists, the vertex object stops being a place to keep
things, and §3.4's leak should be closed at the same time: `apply()` clears
every key it did not put there, rather than the fixed seven it clears today
(`pregel/vertex.lua:28-37`). That turns the measured `self.scratch = alice` of
§3.4 into a nil, and it turns the 2016 `self.idType` read into a nil that was
already nil — no behaviour lost, because nothing may legitimately survive an
`apply()`. Per-vertex state that must survive belongs in the value; per-worker
state belongs in `ctx`.

Cost: `compute` signatures change in all six app modules in the tree and in the
two landing ones. The change is mechanical: `function app.compute(self)`
becomes `function app.compute(self, messages, ctx)`, `self:pairs_messages()`
becomes `ipairs(messages)`, and `self:get_worker_context()` becomes `ctx`. The
type field costs one column in the data space or one reserved key in the value;
see open question 6.2.

Before (`examples/topology-mutation/app.lua`):

    function app.compute(self)
        local value = self:get_value()
        if value.orphan_of ~= nil then
            self:vote_halt(true)
            return
        end
        local threshold = self:get_worker_context().threshold
        ...
    end

After:

    app.compute = {
        marker = function(vertex)
            vertex:vote_halt(true)
        end,
        vertex = function(vertex, messages, ctx)
            local threshold = ctx.threshold
            ...
        end,
    }

### 4.4 Unambiguous mutation methods

Replaces: the four overloaded methods of §3.1.

    vertex:add_edge(dst, value)                 -- this vertex's own edge
    vertex:add_edge_from(src, dst, value)       -- another vertex's
    vertex:delete_edge(dst)                     -- this vertex's own
    vertex:delete_edge_from(src, dst)           -- another vertex's
    vertex:delete_self()                        -- this vertex
    vertex:delete_vertex(name)                  -- another one, name required
    vertex:add_vertex(name, value)              -- as in §4.2

Every one has a fixed arity. `nil` in any position is an error rather than a
different call. `add_edge(dst)` with no value stores `json.NULL`, which is what
the current code does anyway — that is arity 1 versus arity 2 on the same
method, which is checkable, not a type test.

The `edges` flag of `delete_vertex` goes away entirely. Today it must be
`false` and asserts otherwise (`pregel/vertex.lua:334`); an argument with one
legal value is not an argument. When inbound-edge deletion is implemented it
comes back as `delete_vertex(name, {inbound = true})`.

Why: three closed bugs (§3.1), and a measured silent misroute in the current
code.

Cost: small. Measured call sites in the repository: `delete_edge(destination)`
once (`examples/topology-mutation/app.lua:69`) and `add_vertex` once
(`:80`) — both already use the short form, so both are unchanged apart from
§4.2's name argument. The `_from` variants have no call site outside the unit
tests, which is itself worth noticing: the overload that caused three bugs is
used by nothing.

Before:

    -- legal today, and silently not what it says
    self:add_edge('bob', 'carol', nil)

After:

    self:add_edge_from('bob', 'carol')   -- arity 2 on _from: value defaults to NULL
    -- self:add_edge_from('bob', 'carol', nil) raises: value is nil

### 4.5 Aggregator objects, and `run` with progress

Replaces: `agg(value)` / `agg()`, `get_aggregation` / `set_aggregation`,
`master:start()`, and the autostart fiber's re-implementation of the lifecycle.

Aggregators:

    local a = vertex:aggregator('count')
    a:add(1)          -- contribute; nil raises
    a:get()           -- the merged value from the previous superstep
                      -- (what get_aggregation answers today)

    master:aggregator('count'):get()   -- the merged value on the master,
                                       -- which today has to be read as
                                       -- m.aggregators.count.value

The callable form goes. `:add` and `:get` are the shape `pregel.math` already
uses for the same kind of object (`auc.new():add(...)`, `:result()`), so an app
that uses both is not learning two conventions. The master gains a public
accessor, which it does not have at all today (§3.7).

The master:

    master:run{
        max_supersteps = 100,          -- pregel-3wg, as an option of run
        on_superstep = function(status)
            -- status = {superstep, messages, in_progress, aggregators = {...}}
            -- return false to stop after this superstep
        end,
    }  -- returns {supersteps, stopped_by = 'quiet'|'limit'|'callback'}

    master:status()  -- the same table, readable while run() blocks in a fiber

`master:start()` stays, as `run{}` with no options. That is not the shim §5
argues against: `start()` is unambiguous and means exactly one thing, so
keeping it costs a two-line method and no lasting confusion, unlike keeping an
overload that has two readings.

Why: `on_superstep` is the missing seam. `max_supersteps` needs it, a
checkpoint needs it, a convergence test needs it, and the autostart fiber needs
it — today that fiber maintains its own five-state table outside the object that
knows the state (§3.6). With `master:status()` the role's `status()` becomes a
projection of the master's, and a hand-driven job stops reporting `idle` while
it runs.

Cost: `master:run` and `master:status` are new methods, roughly forty lines
between them plus the option checking; `autostart_body` loses about half its
body. `master:start()` keeps working. Aggregator objects break every
`get_aggregation` / `set_aggregation` call site — six of them, in
`examples/max-value`, `examples/pagerank` and `test/apps/maxvalue.lua`.

Before (`examples/pagerank/app.lua`):

    self:set_aggregation('count', 1)
    local n = self:get_aggregation('count')
    local leaked = self:get_aggregation('dangling') / n

After:

    local count = vertex:aggregator('count')
    count:add(1)
    local n = count:get()
    local leaked = vertex:aggregator('dangling'):get() / n

Before (reading the answer of a finished job, measured in §3.7):

    m.aggregators.max_seen.value   -- 999987
    m.aggregators.max_seen:get_global()   -- 0, and it is the documented accessor

After:

    m:aggregator('max_seen'):get()   -- 999987

### 4.6 Loaders as plain functions

Replaces: `loader.new(instance, fn)` and the callable-object-with-`__index`
construction (`pregel/loader.lua:111-117`).

    -- an app returns a plain function
    function app.load(ctx)
        return function(sink)
            for i = 1, n do sink:store_vertex(name(i), value(i)) end
            sink:flush()
        end
    end

    -- a partitioned worker-side loader takes the split as an argument
    function app.load_on_worker(ctx)
        return function(sink, part)
            -- part = {index = <1-based>, count = <workers>,
            --         owns = function(name) -> boolean}
        end
    end

`sink` is the object carrying `store_vertex`, `store_edge`,
`store_edges_batch`, `store_vertex_edges` and `flush` — the same five methods,
built by the library rather than glued onto a metatable whose `__call` is the
user's function.

Why: the current loader is a callable object whose `__call` is the loader and
whose `__index` is the sink, so `self` inside a loader function is both the
graph writer and the loader. That is why `loader_new` needs an `instance` it
closes over before the caller has anything to call. A function taking a sink has
no such knot, and the split arguments stop being two trailing positionals whose
meaning is documented three files away (`pregel/loader.lua:429`,
`pregel/worker.lua:530`).

Cost: `loader.graph_edges_f` and `loader.avro_files` change their internal
shape and keep their signatures. One app-level loader in the repository
(`test/apps/maxvalue.lua:72`) changes from `loader.new(instance, function(self)`
to a plain function.

### 4.7 Writing a vertex value

Replaces: nothing, and that is the point — §3.3 has no API answer today, only
a warning in a doc comment.

    vertex:update(function(value)
        value.dist = best
        return value            -- returning nothing keeps the same table
    end)

`update` reads the value, hands it to the function, takes what comes back (or
the same table when the function returns nothing), stores it and marks the
vertex modified. `get_value` and `set_value` stay exactly as they are; this is
the shorthand for the read-modify-write that every example writes by hand.

Why: the trap in §3.3 is that the natural spelling silently does nothing. An
explicit mutator makes the natural spelling correct without making
`get_value()` return a copy — which would be the other fix, and would cost a
deep copy per vertex per superstep on a path that runs millions of times.

Cost: about ten lines on the vertex object, and no call site has to move.
`examples/max-value`, `examples/wcc` and `examples/sssp` each lose one
whole-value rebuild.

Before (`examples/max-value/app.lua:57`):

    self:set_value({id = vertex.id, name = vertex.name, value = best})

After:

    self:update(function(v) v.value = best end)

### 4.8 The settled defaults

Two decisions the landing work should make once and state in the API rather
than leaving to each app:

- A vertex that does not call `vote_halt` during `compute` is halted
  afterwards (`pregel-3v3`). Staying active requires saying so. Every example
  votes explicitly today, so none of them changes; what changes is that an app
  that forgets cannot hang the cluster.
- `max_supersteps` is an option of `master:run` (§4.5), unbounded by default,
  with a warning logged every 100 supersteps when unbounded. A limit that is
  reached stops the loop and reports `stopped_by = 'limit'` rather than raising:
  the vertices' state is intact and worth reading either way.

### 4.9 The small items of §3.12

None of these is worth a subsection of its own, and leaving them undisposed is
how `write_solution` survived ten years.

- Delete `write_solution` — the private method, the `pool_new` option and the
  field on the vertex object. It has never had a call site.
- `vertex:edges()` returns the array of `{destination, value}` pairs, and
  `vertex:out_degree()` returns `#` of it. The edge list already *is* an array
  on the object (`pregel/vertex.lua:30`), so both are accessors over what is
  there; `pairs_edges()` goes with §4.3's `pairs_messages()`. This retires
  `examples/pagerank/app.lua`'s counting loop.
- `master:store_vertex(name, value)`, `master:store_edge(src, dst, value)` and
  `master:flush()` — the loader sink's methods, on the master, for the case the
  2016 app had to write as `master.mpool:by_id(...):put('vertex.store', ...)`.
  They are three lines each and the routing already exists.
- The result API stays out of scope; see open question 6.8.

## 5. Compatibility and migration

**The library has no external users.** It is `pregel-scm-1.rockspec`, unreleased,
on a branch, with every consumer inside this repository: five examples, one
test app, and the test suite. There is no reason to carry a compatibility shim,
and a shim would cost more than it saves — the whole point of §4.1, §4.2 and
§4.4 is that the old shapes are ambiguous, and a shim that accepts both keeps
the ambiguity in the code forever while pretending it is gone.

Recommendation: **no shim, no deprecation cycle.** Change the API, change the
six app modules and the tests in the same commit series, and record the shape
change in the CHANGELOG's `Changed` section as one entry per numbered proposal.

Which changes are breaking:

- Breaking, for every app module: §4.1 (app object), §4.2 (explicit names),
  §4.3 (compute signature), §4.5 (aggregator objects).
- Breaking, but with no call site outside tests: §4.4's `_from` variants.
- Breaking for one caller: §4.6 (`loader.new`).
- Not breaking: §4.5's `master:run` and `master:status`, which are additive;
  `master:start()` stays.
- Additive, breaking nothing: §4.7's `vertex:update`, and §4.9's
  `vertex:edges`, `vertex:out_degree` and the master's store methods.
- Breaking, with no caller at all: §4.9's deletion of `write_solution`.
- Behaviour change with no signature change: §4.8's halt default.

Order of implementation, chosen so that each step is separately testable and
none of them leaves the tree half-migrated:

1. §4.5's `master:run`/`master:status` and the aggregator object — additive,
   nothing breaks, and `max_supersteps` (`pregel-3wg`) lands on top of it
   instead of inside the loop.
2. §4.8's halt default (`pregel-3v3`) — already specified, and independent —
   together with §4.7's `vertex:update`, which is additive and touches nothing
   else.
3. §4.1's app object. The roles build it; `worker_context` becomes `setup`;
   `common.here`/`resolve`/`cfg` are deleted and `examples/common.lua` with
   them.
4. §4.4's mutation methods. Small, self-contained, and it retires three closed
   bugs' worth of shape.
5. §4.2's explicit names, with §4.6's loader shape in the same series — they
   touch the same five methods.
6. §4.3's typed dispatch, last, because it is the one that wants the two
   landing examples as its acceptance test rather than as its migration cost.
   §4.9's accessors go with it, since `pairs_edges` and `pairs_messages` are
   retired together.

How each example migrates:

- `max-value`, `wcc` — steps 3, 5, 6 are mechanical: `HERE`/`common.*` out,
  `store_vertex` gains a name (through `loader.graph_edges_f`, which is library
  code, so the app changes only its `obtain_name` deletion), `compute` gains two
  parameters, `set_aggregation('max_seen', best)` becomes
  `vertex:aggregator('max_seen'):add(best)`, and the whole-value rebuild becomes
  a `vertex:update`.
- `sssp` — the same, plus `worker_context` becomes `setup(ctx)` and
  `worker_preload` becomes `load_on_worker(ctx)`.
- `pagerank` — the same, and it is the example that gains most from §4.5: its
  four `get_aggregation`/`set_aggregation` calls become two aggregator objects
  fetched once per compute.
- `topology-mutation` — the only one touched by §4.4 and by `add_vertex`'s new
  name argument; its `orphan_of` marker also becomes the first natural user of
  §4.3's typed compute, and its README's two-superstep explanation stays true.
- `test/apps/maxvalue.lua` — the only app-level `loader.new` in the tree, so it
  is the acceptance test for §4.6.
- `lookalike` and `mf` (landing) — migrate with the rest, and are the one place
  where that is a genuine question rather than a mechanical step, because
  `lookalike` is the app §4.3 was designed from. See open question 6.1.

## 6. Open questions

**6.1 — Do `lookalike` and `mf` wait for v2, or land on v1 and migrate?**

Recommended: land them on v1 as they are specified now, and migrate them with
everything else. They are the best evidence available about whether §4.3 is the
right shape, and evidence written against a design is not evidence. Their cost
of migration is the same as the other examples'; the cost of blocking them is
that the v2 design has no multi-type app to check itself against.

**6.2 — Where does a vertex type live: a tuple field or a key in the value?**

Recommended: a fifth field in the `data_<name>` tuple, nullable, defaulting to
nil. The value stays entirely the app's, which is the current contract and worth
keeping; and the type becomes indexable, which makes "count the TASK vertices"
a `count()` rather than a full scan through Lua. The cost is a schema change
that a worker restarted over an existing shard must survive —
`create_spaces` uses `if_not_exists` throughout (`pregel/worker.lua:670`), so
adding a field to the format of an existing space needs care.

**6.3 — Does `obtain_name` survive at all after §4.2?**

Recommended: no, and deleted in the same release rather than kept. Keep it
through the migration as an optional `worker.new` option so the series can be
landed one step at a time; then check whether any caller is left. My
expectation is that none is — the six definitions in the tree all reduce to
one expression at the call site — and a hook with no caller is what
`write_solution` has been since 2016 (§3.12). If a real caller does turn up,
that is the answer instead, and it should be written down next to the option.

**6.4 — Should `messages` be an array or stay an iterator?**

Recommended: an array. A superstep's messages for one vertex are already
materialised by the combiner in the common case (one message per receiver,
`pregel/queue.lua:104-118`), the array is what every call site builds anyway,
and `#messages` answers "did anyone talk to me" without a loop. The risk is a
vertex with an unbounded in-degree and no combiner: a hub in a graph like
`soc-Epinions` receives thousands of messages, and today they are streamed. If
that risk is judged real, the answer is `vertex:messages()` returning an array
and `vertex:each_message()` returning an iterator, not an iterator alone.

**6.5 — Does `master:run`'s `on_superstep` run on the master's fiber?**

Recommended: yes, synchronously between supersteps, with the same contract as
a compute function — it may yield, and it must not block forever. Anything else
needs a second fiber and a queue, and the one thing the callback is for is
deciding whether the next superstep happens.

**6.6 — Does §4.5's `master:status()` supersede the roles' `status()`?**

Recommended: no; the role's `status()` keeps its own states (`read_only`,
`connecting`, `failed`) because those are facts about the role and not about
the job, and it composes the master's `status()` into the rest. What goes away
is the role's duplicate `superstep`/`state` bookkeeping (§3.6).

**6.7 — Do the five remaining pass-through `roles_cfg` keys survive
`pregel-60o`?**

Recommended: keep `pool_size`, `delayed_push`, `squash_only`, `queue_engine`
and `connect_timeout` as they are, and revisit only if `pregel-60o` shows that
the cluster config can express them. They are genuine per-deployment tuning,
unlike `workers`/`master`/`user`/`password`, which are topology and identity
and belong to the cluster config. The duplication complained about in §3.8 is
worth accepting for five knobs; it was not worth accepting for nine.

**6.8 — Is the `data_<name>` space layout public interface?**

Recommended: say yes, explicitly, and document the tuple format in the README
rather than only in `pregel/worker.lua:8-13`. Every example README already reads
results that way (§3.12) and there is no other way to read them. The
alternative — a result API on the master that fans out to the workers — is a
larger design than this document covers, and it would still need the space
layout underneath it.
