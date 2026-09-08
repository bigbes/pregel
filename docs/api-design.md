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
principles; section 5 records a second, independent review and what this
document did with it; section 6 is the prior art both reviews drew on; sections
7 and 8 cover migration and the decisions that are not mine to make.

Every claim about current behaviour in sections 2, 3 and 5 was either read out
of the code (cited as `file:line`), taken from a test that pins it (cited by
test name), or measured. Measurements were made in this checkout at `8221da0`,
on Tarantool 3.9.0-entrypoint, either in-process against
`test/helpers/fake_pregel`, against a real in-process `worker.new`, or by
running `examples/max-value` under `tt`.

### 1.1 The second round

A second review of the same API and the same checkout was made independently by
OpenAI Codex (`gpt-6-astra`, read-only). Its verdict on the first draft of this
document was that it "understates how placement, timing, and previous runs can
change an application's behavior", and it is right. Section 5 records every
disagreement, the evidence I checked for it, and the verdict; section 3 has been
re-ranked and extended with five problems it ranked above the ones here, each
re-verified rather than taken on trust.

Three of its findings changed this document's own conclusions, and they are
worth naming up front because two of them were mistakes of mine:

- An in-place change to a vertex value is *sometimes* persisted, not always
  lost (§3.3). The first draft said it was always lost. Measured.
- Making `get_value()` return a copy was rejected in the first draft as
  "a deep copy per vertex per superstep on a path that runs millions of times".
  The value handed to a compute function is **already** a fresh table — a tuple
  field is decoded per unpack — so that objection was against a cost that does
  not exist. Measured: the unpack costs 1514 ns and a deepcopy on top of it
  would add 130 ns. This retires the first draft's `vertex:update(fn)`
  proposal in favour of a returned value (§4.4).
- A compute function that raises does not merely leak a pooled object: it
  wedges the worker permanently (§3.13). Measured. Codex found the cause; the
  consequence is worse than it said.

### 1.2 Work landing in parallel

Described where it touches a problem below, marked as landing rather than as
current state:

- `pregel-60o` — `roles_cfg` loses `workers`, `master`, `user` and `password`;
  topology and credentials come from the cluster config, the way vshard's do.
- `pregel-3v3` — a vertex that does not vote during `compute` is halted
  afterwards, instead of staying active forever.
- `pregel-3wg` — `max_supersteps` on `master.new`, so a job that does not
  converge stops instead of looping.
- `pregel-4l0` — `pregel.compress`, an ffi layer under the Avro codecs.
- `pregel-bkk.3` and `pregel-6p5.3` — the `lookalike` and `mf` examples, the
  first two apps in this repository with more than one kind of vertex.

The v2 proposed here is not a rewrite of the machinery. It is a change of
surface and of contract over the same master, worker, queue and mpool — but
after the second round it is a larger change than the first draft proposed,
because two of the promoted problems (§3.13, §3.14) are not in the argument
lists at all.

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
happens to it arrives as a protocol message through `pregel.worker.deliver`.

`master.grant(user)` and `worker.grant(user[, instance_name])`
(`pregel/master.lua:220`, `pregel/worker.lua:772`) hand out `execute` on
`lua_call` for the four registry entry points, and — given an instance name —
read/write on that instance's spaces and their sequences. The two halves are
separate because the entry-point names exist before any instance does and the
space names do not.

Lifecycle in full, from the README's own example: `pworker.grant`,
`pmaster.grant`, `worker.new`, `master.new`, then
`master:wait_up():preload():start()`. There is no `master:status()`, no
cancellation, and no callback of any kind during `start()`.

### 2.2 The app-module contract, as the roles consume it

A role is given a Lua module name in `roles_cfg.app` and `require()`s it
(`pregel/roles/common.lua:340`). The module returns a table. What the roles
read out of it:

- `compute(vertex)` — required by the worker role, not by the master
  (`pregel/roles/worker.lua:139` versus `pregel/roles/master.lua:103`).
- `obtain_name(value) -> string` — required by both.
- `combiner(a, b) -> c` — optional, read only by the worker role. One combiner
  per instance, for every message the job sends (`pregel/worker.lua:864`,
  and both queues get it at `:907-916`).
- `worker_preload` / `master_preload` — a loader object, a
  `callable(instance, app_cfg)` returning one, or nil
  (`pregel/roles/common.lua:248`).
- `worker_context` — any value, or a `callable(app_cfg)` returning one
  (`pregel/roles/common.lua:386`). Note that this is a *role* behaviour:
  `worker.new` stores whatever it is given, unchanged
  (`pregel/worker.lua:852`, `:893`).
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
  (`pregel/vertex.lua:161-168`, `pregel/queue.lua:50-59`). There is no sender:
  see §3.18, where the sender turns out to be transmitted and then dropped.
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
(`pregel-3e8`; regression
`worker.test_aggregator_starts_each_superstep_from_the_default`).

`reduce` itself defaults to last-write-wins —
`opts.reduce or (function(_, v) return v end)` at `pregel/aggregator.lua:148`.

The aggregator object is also callable (`pregel/aggregator.lua:122-127`):
`agg(value)` contributes, `agg()` reads the local accumulator.

A combiner is a different thing: `callable(a, b) -> c` folding two *messages*
for one receiver into one. It runs on every put by default, or once per
superstep under `squash_only` (`pregel/queue.lua:104-118`, `worker.lua:351`).
There is one per worker instance and it is applied to every message regardless
of what the message is.

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
arrive somewhere it cannot be named. See §3.17 for what that costs.

`pregel.math` does not touch the graph API at all — it is arrays and tables, so
that a weight vector can be a message payload and a percentile counter can be a
vertex value (`pregel/math/init.lua:17-21`). It is worth noting here only
because its objects have the shape section 4 proposes for aggregators:
`auc.new()` gives `:add(score, label)` and `:result()`
(`pregel/math/auc.lua:124-126`), `percentile.new()` gives `:add(v)` and
`:percentile(p)` (`pregel/math/percentile.lua:142-143`).

## 3. Problems

The subsections are numbered in the order they were found, which is not the
order they matter in. The merged ranking after the second round, worst first:

1. **§3.13** — a run has no authoritative owner, and a compute exception wedges
   the worker permanently. Measured.
2. **§3.14** — job identity and partition identity are implicit; renaming one
   worker's URI sends 100% of vertices to a different instance while every
   tuple stays where it was. Measured.
3. **§3.3 with §3.15** — who owns a value table, and when its contents are
   captured. An in-place change is durable or lost depending on an unrelated
   call in the same compute. Measured.
4. **§3.18** — the sender of a message is transmitted and then discarded, so
   `reply` is not expressible and a combiner cannot know who asked. Measured.
5. **§3.16** — conflicting topology mutations are resolved by arrival order.
6. **§3.17** — `loader.avro_files` has two naming authorities that can disagree.
7. **§3.1, §3.2** — the overloads and the derived name.
8. **§3.9, §3.4** — typed vertices by convention, on an object that leaks.
9. **§3.5, §3.6, §3.7, §3.8, §3.10, §3.11, §3.12** — the contract and surface
   problems the first draft led with.

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
has to wrap it in a table. See §3.5 for the half of that which is worse.

The cost is not that these are hard to use once learned. It is that a wrong
call is silently a different, legal call. Type-dispatched overloads have no
arity to check and no nil to catch. What they do **not** cover is the semantics
of the operation itself, which §3.16 says is the larger problem.

### 3.2 `obtain_name` is required everywhere, and the name is never given

A vertex has no name of its own. Its name is computed from its value by
`obtain_name`, at four different places:

- the loader, to route (`pregel/loader.lua:47`),
- the worker, again, to store (`pregel/worker.lua:545`),
- `vertex:add_vertex`, to route (`pregel/vertex.lua:265`),
- the worker again, to key the topology mutation (`pregel/worker.lua:578`).

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
not allowed to state. §3.17 is the sharp end of this: the Avro loader has to
name a record twice, through two different options, and nothing checks that the
two agree.

### 3.3 A value is captured, or not, depending on an unrelated call

`set_value` is what sets `__modified`, and `__modified` is one of three things
that make `compute()` write the tuple back (`pregel/vertex.lua:196-205`,
`48-81`). The other two are a queued edge addition and a queued edge deletion —
and the halt flag, which `vote_halt` also routes through `__modified`.

The first draft of this document said an in-place change to the value is lost.
That is only true when nothing else about the vertex changed. Measured, three
compute functions over a vertex whose value is `{n = 1}`:

    in-place `get_value().n = 99`, then vote_halt(true)  -> 1 write, value {"n":99}
    in-place `get_value().n = 99`, then add_edge('bob',1) -> 1 write, value {"n":99}
    in-place `get_value().n = 99` alone                   -> 0 writes

So the same line of application code is durable or lost depending on whether
the compute function later votes or touches an edge. Every propagation example
in this repository votes at the end of `compute`, which means an in-place change
in any of them would in fact persist — silently, through a code path whose
purpose is something else.

The tell that this was already understood as a hazard is that every example
works around it by rebuilding the whole value: `examples/max-value/app.lua:57`
writes `self:set_value({id = vertex.id, name = vertex.name, value = best})` —
three fields copied to change one. `examples/wcc/app.lua:69` and
`examples/sssp/app.lua:62, 80` do the same. The 2016 code wrote a helper for it
(`node_common.set_status`, which reads, assigns and writes back).

There is no performance argument for the dirty bit. Measured: a tuple's value
field is decoded fresh on every `tuple:unpack`, so the table a compute function
is handed is **already** a detached copy — 1514 ns for the unpack the runtime
already does, against 130 ns for a deepcopy that would not be needed anyway.
The first draft rejected "make `get_value()` return a copy" on a cost that does
not exist. §4.4's returned value is free.

### 3.4 Anything set on the vertex object leaks to the next vertex

`apply()` (`pregel/vertex.lua:28-37`) resets six fields and clears the two
pending-edge arrays. Any other key an app writes onto the vertex object stays
there for whatever vertex the pool hands out next. Measured, with a compute
function that reads `self.scratch` and then sets it:

    compute(alice) saw self.scratch = nil
    compute(bob)   saw self.scratch = alice

This is not hypothetical, and it has bitten in both directions.

Read side: the 2016 look-alike app read `self.idType` at
`test-avro/node_data.lua:89` to build a config key. `idType` is assigned nowhere
in that tree — `git grep idType 7fba5d4^ -- test-avro` returns that one line —
so the key was always `'<task>:nil'`, the lookup always missed, and the
prediction-threshold branch never fired.

Write side: the same app's TASK vertex assigns `self.dataSetSpace =
box.space[space_name]` (`test-avro/node_task.lua:58`). A space handle is a
worker-local *resource*, and it was stashed on a pooled object because the API
offered nowhere else to put it. Under pooling that handle is then visible to the
next vertex the pool hands out, of any kind. §4.2's `worker.open/close` is the
place that should have existed.

The same openness is what the 2016 app used deliberately for typed dispatch:
`computeGradientDescent` (`7fba5d4^:test-avro/common.lua`) saves the vertex's
metatable on first call, `setmetatable`s the pooled object to one of three
per-type tables built by copying `pregel.vertex.vertex_methods`
(`test-avro/node_task.lua:454-469` is one of them), calls `compute_new`, and
puts the original metatable back. That is what an app must do today to get typed
vertices, and it depends on `vertex_methods` being exported
(`pregel/vertex.lua:474`) and on the pool never noticing.

### 3.5 One configuration channel delivered twice, and a resource channel that is not one

`roles_cfg.app_cfg` reaches the app module twice, in two different shapes:

- as the second argument of `master_preload` / `worker_preload`, passed as
  `options.preload_args` and applied by `worker_new` at
  `pregel/worker.lua:898-905`,
- as the argument of a callable `worker_context`, resolved by the role before
  `worker.new` is called (`pregel/roles/worker.lua:188`).

So an app that needs the same value in both places reads it twice, through two
different mechanisms, with two different error behaviours.

Two things are worse than that, and the second round is what brought them out.

**The roles and the constructor disagree about what a callable
`worker_context` means.** The role calls it and stores the result
(`pregel/roles/common.lua:386-397`); `worker.new` stores it unchanged
(`pregel/worker.lua:852`, `:893`), so a programmatic caller passing the same app
module's `worker_context` gets a *function* out of
`vertex:get_worker_context()` where the role's caller gets a table. Two entry
points to one library, two meanings for one field.

**`worker_context` is being asked to be two different things.** Immutable
configuration (`examples/sssp/app.lua`'s `{source = ...}`) and worker-local
resources (the 2016 TASK's dataset spaces, its trained models, its report
tables) are not the same kind of thing and do not have the same lifetime. The
first draft filed both under "settings", which was a wrong diagnosis; see §5.2.

The master, meanwhile, has no `worker_context` option at all: `master.new`'s
option list (`pregel/master.lua:230-239`) does not include it, and the master
role never calls `common.worker_context` (`pregel/roles/master.lua:242-253`). So
`obtain_name` — which the master needs, and which must agree with the workers' —
cannot be configured from `app_cfg` on the master side. Any app whose naming
depends on configuration has to smuggle it through a module-level upvalue read
at `require` time, which is exactly what the 2016 app did with its `do ... end`
worker-context block.

### 3.6 `autostart` re-implements the master lifecycle

`autostart_body` (`pregel/roles/master.lua:163-202`) is sixty lines that call
`wait_up`, choose between `preload` and `preload_on_workers`, call `start`, and
maintain a five-state status table. It also has to distinguish its own
cancellation from a job failure (`is_cancelled`, and the `state.master ~=
instance` test), and it needs `autostart_traceback` because `xpcall_tb` would
log a cancellation as an error — a defect that was filed and fixed
(`pregel-dxl`).

None of that belongs to the role. It is the master's own lifecycle, written
outside the master because the master has no lifecycle API. The proof that it is
the master's business and not the role's: `master:start()` already publishes
`superstep_count` as it goes (`pregel/master.lua:105`) precisely so something
outside can watch, and the role's `status()` reads that field directly
(`pregel/roles/master.lua:368`).

A job driven by hand through `get()` moves `status().superstep` but leaves
`status().state` at `idle` forever, because only the autostart fiber writes the
other states — stated at `pregel/roles/master.lua:333-334` as a known
limitation. §3.13 is why this is more than a cosmetic seam.

### 3.7 The aggregator surface, and the three defects under it

`agg(value)` contributes, `agg()` reads, and `agg:get_global()` reads a
different value. Measured:

    agg(nil)  -> 0        -- a read, not a contribution; nothing is stored
    agg(5); agg()  -> 5   -- the local accumulator
    agg:get_global()  -> 0  -- what a vertex would read

An aggregator therefore cannot be given nil, and the doc comment says so
(`pregel/aggregator.lua:117-118`).

Two clarifications the second round forced, and one thing that stands.

**The S−1 read is correct and stays.** A vertex reading what the whole graph
produced in the *previous* superstep is BSP, not a workaround: every vertex of a
superstep must observe the same completed state. §4.5 keeps it and names it.
`pregel-3e8` was a bug precisely because a vertex was reading its own worker's
partial accumulator instead — measured then as `[501, 2501, 10501]` where
`[0, 2000, 2000]` was expected.

**`agg()` versus `agg(value)` is an exposed internal, not a user-facing
overload.** A compute function already has two separate methods
(`get_aggregation`, `set_aggregation`), so the callable form is only reachable
from library code and from a console. It should still go, but it is a smaller
problem than the first draft implied.

**What stands, and is not cosmetic:** the master has no public accessor at all.
Measured on the finished `max-value` example:

    m.aggregators.max_seen.value        -> 999987   -- the answer
    m.aggregators.max_seen:get_global() -> 0        -- never informed

`get_global()` is the documented accessor and it is wrong on the master, because
`inform_workers` writes to the workers' copies and never to the master's own
(`pregel/aggregator.lua:43-47`). Reading a finished job's answer means reaching
into `m.aggregators.<name>.value` — a field of a field.

And three real defects sit under the surface, all verified:

- **The default reducer is last-write-wins.** `pregel/aggregator.lua:148`:
  `opts.reduce or (function(_, v) return v end)`. An aggregator declared with
  `{default = 0}` and nothing else silently keeps whichever contribution
  happened to arrive last, per worker, and then whichever worker reported last.
  That is not a reduction and there is no reason it should be the default.
- **One combiner for every message.** `worker.new` takes a single `combiner`
  (`pregel/worker.lua:864`) and hands it to both queues (`:907-916`). An app
  with more than one kind of message cannot combine one kind and leave the other
  alone. The 2016 look-alike app declared five message commands in an ffi struct
  (`7fba5d4^:test-avro/constants.lua`: `NONE`, `FETCH`, `PREDICT_CALIBRATION`,
  `PREDICT`, `TERMINATE`) and ran with `combiner = nil`
  (`7fba5d4^:test-avro/common.lua:361`), because no single function could fold a
  feature vector and a scalar prediction alike. The landing `pregel-bkk.3` has
  the same shape.
- **The sender is discarded.** See §3.18.

### 3.8 One contract, two validators

The first draft complained that nine of the fourteen `roles_cfg` keys are a
pass-through to `worker.new` or `master.new`. The second round is right that
this is the wrong complaint: exposing deployment tuning through both YAML and a
constructor is normal, and vshard does exactly that.

What is actually wrong is that the *contract* is written twice and the two
copies are not the same. `common_spec` (`pregel/roles/common.lua:110-145`) has
the types, the ranges and the emptiness checks; `worker_new`
(`pregel/worker.lua:846-873`) has the defaults and a different set of asserts.
Neither is derived from the other. `pregel-ilf` is what that costs: `validate()`
accepted an empty `name`, `app`, `master` or `user`, and a `password` with no
`user`, until someone went through them one at a time — while `worker.new`'s own
asserts had never covered them at all.

Four of the nine keys are going away in `pregel-60o` — `workers`, `master`,
`user`, `password` — which is right for a different reason: they are topology
and identity, and those belong to the cluster config. The five that remain are
genuine tuning and should stay in both places, behind **one** validator.

`pool_size` is also the wrong name for what it is: it is the number of messages
in an outgoing batch (`pregel/mpool.lua`'s `msg_count`), not the size of a pool
of anything.

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

The worker role's `status()` cannot tell a finished job from a running one:
measured on `max-value` after the master reported `state: done, superstep: 12`,
`require('pregel.roles.worker').status()` on worker1 still answered
`state: running, in_progress: 0, messages: 0`. The worker has no notion of the
job being over, because nothing ever tells it.

### 3.11 `HERE`

`examples/common.lua:25` reads `debug.getinfo(level, 'S')` to find out where the
app module lives, so that a relative path in `app_cfg` has a base. It solves a
real packaging problem, and it is in the wrong repository layer: every app that
reads a file needs it, and it is a helper of the examples.

Its own guard shows the fragility — an app module loaded from a string rather
than a file has no directory, and `here()` raises telling the caller to use an
absolute path instead. A module reached through `package.preload` has no
filesystem directory at all, so no amount of stack inspection can answer for it.
The `level` argument makes it a positional-stack-frame API: correct only when
called directly from the app module's own chunk, silently wrong from a helper.

The role knows a better answer already — it called `require(cfg.app)`, so
`package.searchpath` gives the same directory without a stack walk — but the
right answer is neither: a deployment base directory stated in the config
(§4.7), because where the *code* lives and where the *data* lives are not the
same question.

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
  halves are being fixed — `pregel-3v3` and `pregel-3wg`.

### 3.13 A run has no authoritative owner, and a failed compute wedges the worker

Promoted from the second round, which ranked this first. Its diagnosis: "A
`run()` convenience wrapper alone won't establish whether retrying, cancelling,
or reading results is valid."

There is no object that represents one execution. §3.6 and §3.10 are two views
of that: the state lives in the role's autostart wrapper, the superstep counter
lives on the master, the message and vertex counts live on each worker, and
nothing ties them to an execution that can be asked whether it succeeded.

The second round pointed at `pregel/worker.lua:295` — a compute exception skips
the message cleanup and the return of the pooled vertex object. The consequence
is worse than a leak. `tuple_process` (`pregel/worker.lua:295-301`) is:

    local vertex_object = self.vertex_pool:pop(tuple)   -- count = count + 1
    ...
    vertex_compute(vertex_object)                        -- raises here
    self.mqueue:delete(vertex_object.__id)               -- skipped
    self.vertex_pool:push(vertex_object)                 -- skipped, count stays up

and `run_superstep` ends with `while self.vertex_pool.count > 0 do
fiber.yield() end` (`pregel/worker.lua:310-312`), a loop nothing else can
satisfy. Measured against a real in-process worker with two vertices and a
compute that raises:

    superstep 1 with a raising compute: ok=false pool.count=1
    superstep 2 with a benign compute finished within 3s: false (pool.count=1)

The worker is wedged for the life of the process, and nothing upstream converts
that into a failure. A bucket calls with `self.connection:call(path, args)` and
no timeout (`pregel/mpool.lua:274`), and the waitpool's liveness check only
notices a handler fiber that has *died* (`pregel/mpool.lua:846-853`) — a handler
blocked in a call to a wedged worker is alive. So `master:start()` stops at the
next `send_wait('superstep')` and stays there. The job hangs rather than fails,
which is the one outcome an operator cannot act on.

That is a live defect and should be filed regardless of what v2 looks like: the
fix is a `pcall` around the compute with the cleanup on the failure path, so the
superstep raises and the existing error plumbing does the rest.

Two more facts about the same area, both from the code:

- The failed vertex's messages stay in the queue, so they are re-read in the
  next superstep and re-counted into `__messages`.
- `deliver_batch` is explicitly not atomic (`pregel/worker.lua:201-203`): the
  first failing message abandons the rest and leaves what came before it
  applied. So a failure mid-run leaves the graph in a state no contract
  describes.

### 3.14 Job identity and partition identity are implicit

Promoted from the second round, which ranked this second. Three separate facts:

**One master per process, chosen by whoever ran last.**
`pregel/master.lua:24` is `local master = nil` and `:300` is `master = self`,
with no check. A second `master.new` in the same process silently displaces the
first, and the module's doc comment says so as if it were a design note.

**Aggregator RPCs carry no job identity.** The worker's entry point is
`deliver_msg(name, msg, args)` — the instance name first
(`pregel/worker.lua:177`). The master's is `deliver_msg(msg, args)`
(`pregel/master.lua:59`): no name, no job, no run, no superstep. A late report
from a previous run, or from a different job in the same process, is
indistinguishable from a current one and is merged.

**A worker's identity is its position in a sorted list of URI strings.**
`mpool.new` sorts the normalized server list (`pregel/mpool.lua:1129-1140`) and
`mpool:id(name)` is jump consistent hashing over the *count*
(`pregel/mpool.lua:949`). So the bucket number for a name is stable for any
list of the same length, and the mapping from bucket number to instance is
positional.

Measured, three workers, renaming one URI so that it sorts differently
(`127.0.0.1:3304` → `127.0.0.1:3300`):

    before buckets: 127.0.0.1:3302, 127.0.0.1:3303, 127.0.0.1:3304
    after  buckets: 127.0.0.1:3300, 127.0.0.1:3302, 127.0.0.1:3303
      v1: bucket 3 (:3304) -> bucket 3 (:3303)
      v2: bucket 2 (:3303) -> bucket 2 (:3302)
    renaming :3304 -> :3300 sends 10000/10000 names (100.0%) to a different
    instance while every tuple stays where it was

100%, not "some". Every vertex is now looked for on an instance that does not
have it, and every instance is serving a shard that nobody addresses. Nothing
detects it: the spaces are named after the *job*, not after the partition, so a
worker adopts whatever `data_<job>` it finds (`pregel/worker.lua:670`,
`if_not_exists` throughout).

The same mechanism, benignly: adding a fourth worker moves 2469/10000 names
(24.7%), which is what jump hashing promises and is the whole reason it was
chosen — but those 2469 vertices are equally orphaned, because nothing
relocates tuples.

Explicit vertex IDs (§4.3) do not help with any of this. What is missing is a
partition manifest that the shard on disk can be checked against.

### 3.15 Who owns a table, and when its contents are captured

Promoted from the second round. §3.3 is the vertex-value half. The other half
is the message payload.

`bucket:put(msg, args)` stores the arguments **by reference** in a reusable
slot: `slot[1] = msg; slot[2] = args` (`pregel/mpool.lua:518-526`). The batch is
sent later, by a background pusher or by a flush. So a compute function that
does

    local payload = self.scratch or {}
    payload.rank = r
    self:send_message(dest, payload)

sends a table it still holds, and any later mutation of it — including the next
vertex's, if the table came from somewhere shared — changes what is
transmitted. Nothing copies, and nothing documents a capture point.

The contract that is missing is one sentence long: *the runtime captures a
payload before `send` returns, and the caller may reuse the table afterwards.*
Whether that is implemented by copying or by forbidding reuse is an
implementation choice; not stating it is not.

### 3.16 Conflicting topology mutations are resolved by arrival order

Promoted from the second round, which is right that this matters more than the
overload syntax of §3.1.

`apply_topology_mutations` applies the four kinds in a fixed order — delete
edges, delete vertices, add vertices, add edges (`pregel/worker.lua:403-490`) —
and that ordering is documented and tested. What it does *not* fix is a conflict
within one kind. Two vertices adding a vertex of the same name in the same
superstep hit `self.data_space:replace{name, false, group[1].value, {}}`
(`pregel/worker.lua:464`): `group[1]` is whichever request `collect_mutations`
read first, which is primary-key order in the mutation space, which is the
sequence number, which is arrival order. The loser is dropped with a log line at
most.

Edge deletion has the mirror problem, and the topology-mutation example admits
it in a comment (`examples/topology-mutation/app.lua:62-65`): deletion is
matched by destination name, so "two edges to the same destination stand or fall
together however different their weights are". There is no way to delete one of
two parallel edges, because an edge has no identity.

Fixed operation ordering is not deterministic conflict resolution. A rerun of
the same job over the same graph can produce a different result, and nothing
says so.

### 3.17 `loader.avro_files` has two naming authorities

Promoted from the second round. In the Avro loader's per-record path
(`pregel/loader.lua:447-453`):

    local name = vertex_name(record)          -- decides partition ownership
    if name == nil then error(...) end
    if owns(name) then
        self:store_vertex(vertex_value(record))   -- names it AGAIN, via obtain_name
    end

`owns(name)` is `instance.mpool:id(name) == worker_idx`
(`pregel/loader.lua:439-444`), so `options.vertex_name` decides *which worker
keeps the record*. `store_vertex` then calls `instance.obtain_name(value)`
(`pregel/loader.lua:47`) to decide *what the vertex is called*. These are two
different functions supplied by the app, and nothing checks that they agree.

When they disagree — a `vertex_name` reading the `name` field while
`obtain_name` returns `tostring(value.id)`, which is exactly the split
`examples/max-value/app.lua:29-33` documents as necessary because the input has
4684 duplicate names — a partitioned worker-side load stores each vertex under a
name that hashes to a *different* worker than the one that kept it. Every
message for it is then routed away from the shard that holds it.

The examples do not hit this only because the ones using `avro_files` on the
workers (`sssp`) happen to pass the same field to both.

### 3.18 The sender is transmitted and then discarded

Promoted from the second round, and the sharpest single finding of it: this is a
live bug and a half-finished fix.

`pregel-2qk.4` fixed `vertex:send_message` to include the sender, and the
CHANGELOG says so: "`vertex:send_message()` omitted the sender, which
`message.deliver` documents and a combiner has no other way to learn". The
sending side does carry it (`pregel/vertex.lua:183-186` puts
`{receiver, msg, self.__id}`). The receiving side throws it away
(`pregel/worker.lua:88-90`):

    ['message.deliver'] = function(instance, args)
        -- args[1] - receiver, args[2] - message, args[3] - sender
        return instance.mqueue_next:put(args[1], args[2])
    end,

`queue:put(receiver, message)` has nowhere to put a third value. Measured
against a real in-process worker:

    delivered {receiver='bob', msg='hello', sender='alice'}
    -> queue holds: ["hello"]

So the comment names a field that is dropped one line below it, the combiner
still cannot learn who asked, and `pairs_messages` cannot yield a sender. Any
request/response protocol — which is what the 2016 look-alike app is, and what
`pregel-bkk.3` will be — has to put the sender inside the payload by hand, which
is precisely what `node_data.lua` does (`sender = self:get_name()` in every
message it constructs).

This should be filed as a bug against v1 independently of v2.

## 4. Proposal for v2

Ten changes and a list of small ones. Each is stated as a signature, with what
it replaces, why, what it costs, and — where one exists — a before/after taken
from a real file in this repository.

The shape below is close to what the second round proposed, because on most
points it argued better than the first draft did. Where this document differs
from it, §5 says so and why.

### 4.1 One app definition, validated at load

Replaces: the loose bag of exports of §2.2, and the roles' bespoke checking of
it.

    local p = require('pregel')

    return p.define {
        api_version = 2,
        configure   = function(raw) ... end,   -- -> normalized, serializable cfg
        load        = {on = 'master' | 'workers', run = function(sink, ctx) end},
        messages    = {<type> = {combine = fn}, ...},
        aggregators = {<name> = <reducer>, ...},
        worker      = {open = fn(ctx), close = fn(services, outcome)},
        compute     = fn | p.dispatch{<KIND> = fn, ...},
        master      = {after_step = function(control) end},
    }

`p.define` validates the declaration once, where the app is written, and returns
something both `master.new`/`worker.new` and the roles consume unchanged. That
is what makes §3.8's second validator unnecessary: there is one contract object
and one checker for it.

`configure(raw)` is pure and must return something serializable. It is where
defaults and semantic validation live — the thing `examples/*/app.lua` currently
does three different ways with `common.cfg`.

Why: §3.5's two channels, §3.8's two validators, and §2.2's "everything else in
the module is invisible" all come from there being no declaration at all, only a
set of names the roles happen to look up.

Cost: every app module changes shape. Six in the tree, two landing.

### 4.2 Explicit lifetimes for worker-local resources

Replaces: `worker_context`, in both of its current meanings.

    app.worker.open(ctx)                -- -> services; once per run, after readiness
    app.worker.close(services, outcome) -- on completion or failure
    -- in compute:  ctx.cfg      -- the normalized configuration (immutable)
    --              ctx.services -- what open() returned

Why: configuration and resources are different things with different lifetimes
(§3.5), and the current API has one slot for both. The evidence is the 2016 TASK
vertex, which opened a space in `__init` and stashed the handle on a *pooled
vertex object* (`test-avro/node_task.lua:58`) because there was nowhere else —
§3.4's leak, used as a feature. `open`/`close` is also what makes a failed run
releasable: `close(services, outcome)` runs on the failure path too, which
nothing does today.

`open` runs after the pool is ready, so it may talk to peers; `configure` runs
before anything, so it may not. That split is the point.

Cost: one new pair of callbacks; `get_worker_context()` goes.

Before (`examples/sssp/app.lua:45-48`, plus `HERE` at `:25`):

    local HERE = common.here()
    function app.worker_context(app_cfg)
        local cfg = common.cfg(app_cfg, {'source'})
        return {source = cfg.source}
    end

After:

    configure = function(raw)
        checks({source = 'string', vertices = 'string', edges = 'string'})
        return {source = raw.source, vertices = raw.vertices,
                edges = raw.edges}
    end,
    -- no worker.open at all: sssp needs configuration, not resources
    -- compute reads ctx.cfg.source

`examples/common.lua` disappears with `here`, `resolve` and `cfg`.

### 4.3 Explicit vertex identity, and one naming authority

Replaces: `obtain_name`, and the double naming of §3.17.

    sink:vertex{id = '42', kind = 'DATA', value = record}
    sink:edge{src = '42', id = 'e7', dst = '81', value = weight}

    ctx.graph:add_vertex{id = 'TASK:x', kind = 'TASK', value = state}

    v.id      -- immutable
    v.kind    -- immutable
    v.value   -- the detached value

`obtain_name` goes entirely rather than becoming optional. A record's id is
computed once, at the point of storage, by the app — which is where the 2016
twenty-line dispatcher reduces to a prefix. Display names stay in the value,
where `examples/max-value/app.lua:29-33` already explains they belong.

Why: §3.2 (four hidden call sites, a value that cannot be reduced, a
type dispatcher in disguise) and §3.17 (two authorities that can silently
disagree and misroute a whole partitioned load). One `id` makes the second
impossible by construction, because there is nothing to disagree with.

Cost: one `store_vertex` call in the tree (`test/apps/maxvalue.lua:74`), one
`add_vertex` (`examples/topology-mutation/app.lua:80`), six `obtain_name`
definitions deleted, and the two library loaders. `store_edge` already takes
names.

Duplicate ids fail by default rather than silently resetting the vertex's value
and edges, which is what `vertex_store`'s `replace` does today
(`pregel/worker.lua:544-547`).

Before (`test/apps/maxvalue.lua:31-33, 74`):

    function app.obtain_name(vertex) return vertex.name end
    ...
    self:store_vertex(app.vertex(i))

After:

    local v = app.vertex(i)
    sink:vertex{id = v.name, value = v}

### 4.4 `compute(v, inbox, ctx)` returning value and schedule

Replaces: `compute(self)`, `get_value`/`set_value` and the dirty bit,
`vote_halt`, the value-carried type discriminator, and the metatable swapping of
§3.4/§3.9.

    app.compute = function(v, inbox, ctx)
        ...
        return value | p.KEEP, p.ACTIVE | p.HALT
    end

    app.compute = p.dispatch{
        MASTER = master_compute, TASK = task_compute, DATA = data_compute,
    }

Both return values are mandatory. Falling through raises
`INVALID_COMPUTE_RESULT` rather than defaulting to anything. `p.KEEP` says "do
not write the value"; returning the value — including the same table, mutated in
place — persists it.

Why the returned value: §3.3. The dirty bit makes an in-place change durable or
lost depending on whether the compute later votes or touches an edge, which is
not a rule anyone can hold. A returned value has no such coupling and catches
nested table changes, which no dirty bit can. And it is free: measured, the
value handed to compute is already a fresh decode of the tuple field
(1514 ns), so there is nothing extra to copy. This supersedes the first draft's
`vertex:update(fn)`, which was proposed only because copying was believed
expensive.

Why the mandatory schedule: see §5.1. Briefly — halt-by-default (`pregel-3v3`)
is the right v1 fix and the wrong v2 contract. In v1 the runtime must guess,
and halting is the safer guess; in v2 the signature already returns a tuple, so
requiring the second element costs one word and removes the guess. A vertex that
forgets is a bug, and it should say so rather than either hanging the cluster
(today) or quietly reporting a converged answer to an unconverged algorithm
(halt-by-default).

New vertices start `ACTIVE`. A message reactivates a halted vertex, as today.

Why `inbox` and `ctx` as arguments: §3.12's discarded iterator values, and
making it visible in the signature that the context is per-worker and shared
rather than something reached through the vertex.

Why `p.dispatch`: §3.9. It is a table lookup by `v.kind` with a named error for
an unhandled kind, replacing three metatables built by copying
`pregel.vertex.vertex_methods`.

Cost: the compute signature changes in all eight app modules. Mechanical for the
six single-kind ones.

Before (`examples/topology-mutation/app.lua:50-87`):

    function app.compute(self)
        local value = self:get_value()
        if value.orphan_of ~= nil then
            self:vote_halt(true)
            return
        end
        local threshold = self:get_worker_context().threshold
        ...
        self:vote_halt(true)
    end

After:

    compute = p.dispatch{
        MARKER = function() return p.KEEP, p.HALT end,
        VERTEX = function(v, inbox, ctx)
            local kept = 0
            for _, e in ipairs(v:edges()) do
                if e.value < ctx.cfg.threshold then
                    ctx.graph:delete_edge{src = v.id, id = e.id}
                else
                    kept = kept + 1
                end
            end
            if kept == 0 then
                ctx.graph:add_vertex{id = v.id .. ':orphan', kind = 'MARKER',
                                     value = {orphan_of = v.id}}
            end
            return p.KEEP, p.HALT
        end,
    }

### 4.5 Typed messages, real reducers, and a master broadcast

Replaces: one combiner for everything, the last-write-wins reducer, the
aggregator's callable form, and the aggregator-as-broadcast-channel of the 2016
app.

    app.messages = {rank = {combine = function(a, b) return a + b end},
                    fetch = {}, sample = {}, calibrate = {}}

    ctx:send(dst_id, type, payload)
    ctx:reply(message, type, payload)
    ctx:send_edges(v, type, payload)
    inbox:messages(type)             -- iterator of {from = ..., value = ...}
    inbox:fold(type, initial, reduce)

    app.aggregators = {dangling = p.reducers.sum(),
                       models   = p.reducers.unique_map()}
    ctx:aggregate(name, contribution)
    ctx:previous(name)               -- the completed S-1; the identity in S=1
    control:reduced(name)            -- the just-completed S, on the master
    control:broadcast(name, value, {activate = 'DATA'})
    control:finish{reason = 'iterations'}

Six decisions in that, each answering a verified problem:

- **Per-type combiners** (§3.7): combining is scoped to
  `(run, superstep, destination, type)`. A combiner must tolerate arbitrary
  grouping and order, and a combined message has no single sender, so `reply`
  refuses one.
- **`from` on every message** (§3.18): the sender already travels and is
  dropped at `pregel/worker.lua:90`. Carrying it through to the inbox is what
  makes `reply` expressible and stops every request/response app from
  hand-rolling `sender = self:get_name()` in each payload.
- **Reducers are `{init, accumulate, merge}`, with no implicit default**
  (§3.7): `p.reducers.sum()` and friends are the common ones. An aggregator
  declared with no reduction is an error, not last-write-wins.
- **S−1 reads stay, and get a name** (§3.7, §5.4): `ctx:previous(name)` says in
  the call what the current `get_aggregation` says only in a doc comment. The
  master's own view of the step that just finished is a different method on a
  different object (`control:reduced`), because it is a different value.
- **Broadcast is not an aggregator.** The 2016 app used a per-task aggregator
  with a max-by-command merge to push a model out to every DATA vertex
  (`test-avro/common.lua`, `addAggregators`). A master publication that becomes
  visible next step and can activate a whole kind expresses that directly,
  without manufacturing one identical message per vertex.
- **Unknown types and nil payloads fail.** `box.NULL` is the way to send
  nothing, as it already is for edge values.

Cost: six `get_aggregation`/`set_aggregation` call sites, in
`examples/max-value`, `examples/pagerank` and `test/apps/maxvalue.lua`; the
`combiner` option becomes a per-type declaration in three examples.

Before (`examples/pagerank/app.lua:66, 76, 93`):

    self:set_aggregation('count', 1)
    local n = self:get_aggregation('count')
    local leaked = self:get_aggregation('dangling') / n

After:

    -- `count` disappears entirely: the graph knows how many vertices it has
    local n = ctx.graph.vertex_count
    local leaked = ctx:previous('dangling') / n

That deletion is worth stating on its own: PageRank spends its whole first
superstep discovering N by having every vertex contribute 1 and reading the
total back, and re-contributing it forever because the aggregate resets. A
vertex count maintained by the runtime and refreshed at topology barriers
removes a superstep and an aggregator from the example.

Before (reading a finished job's answer, measured in §3.7):

    m.aggregators.max_seen.value          -- 999987
    m.aggregators.max_seen:get_global()   -- 0, and it is the documented accessor

After:

    run:aggregate('max_seen')             -- 999987

### 4.6 Topology with edge identity and an explicit conflict policy

Replaces: the four overloaded mutation methods of §3.1, the arrival-order
resolution of §3.16, and the destination-only edge deletion the
topology-mutation example complains about.

    ctx.graph:add_vertex{id = ..., kind = ..., value = ...}
    ctx.graph:add_edge{src = v.id, id = 'e1', dst = ..., value = ...}
    ctx.graph:delete_edge{src = v.id, id = 'e1'}
    ctx.graph:delete_edges{src = v.id, dst = ...}
    ctx.graph:delete_vertex{id = v.id, if_missing = 'ignore'}

Named fields, so there is no argument to shift and no nil to reinterpret. An
edge is identified by `(src, edge_id)`, which is what makes it possible to
delete one of two parallel edges — the thing
`examples/topology-mutation/app.lua:62-65` says cannot be done.

**Every mutation takes effect at the barrier, including one to the current
vertex's own edges.** Today the local path is applied when the vertex is written
back and the remote path between supersteps (`pregel/vertex.lua:299-309`), which
is two timings for one operation and is exactly the divergence `pregel-iv7` was.
One timing means `pairs_edges` during a superstep always shows the same list,
which is the property the example's "counted, not read back" comment works
around.

The four-phase ordering stays (delete edges, delete vertices, add vertices, add
edges) because it is what lets one superstep add a vertex and an edge out of it.
What changes is that a conflict *within* a phase is refused rather than resolved
by `group[1]` (§3.16). A second creation of the same id fails the run with a
named error; an app that wants last-writer-wins says so.

Vertex deletion removes the vertex's outgoing edges; inbound edges stay, as
today, because only a full scan could find them. That is now stated as a
contract rather than as an unimplemented argument.

Cost: two call sites in the examples, plus the internal `_delayed` handlers.
The first draft answered §3.1 by splitting each overload into two methods
(`add_edge` / `add_edge_from` and so on); named fields make that unnecessary,
which is a better answer than four more names — and it is the local Tarantool
idiom (`box.schema.space.create(name, opts)`, see §6).

Before:

    self:add_edge('bob', 'carol', nil)   -- silently an edge alice -> bob = 'carol'

After:

    ctx.graph:add_edge{src = 'bob', dst = 'carol', id = 'e1'}
    -- no value field means no value; a nil in a named field is not a shift

### 4.7 Loaders as `(sink, ctx)`, with an explicit base directory

Replaces: `loader.new(instance, fn)`, the callable-object-with-`__index`
construction (`pregel/loader.lua:111-117`), the trailing
`worker_idx, workers_count` positionals, and `HERE`.

    load = {
        on  = 'master',                      -- or 'workers'; stated, not inferred
        run = function(sink, ctx)
            p.loaders.avro(sink, ctx, {vertices = ctx:path(ctx.cfg.vertices),
                                       edges    = ctx:path(ctx.cfg.edges), ...})
        end,
    }

    ctx.partition:owns(vertex_id)   -- for a hand-written partitioned loader
    ctx:path(relative)             -- resolved against the deployment base_dir

Three changes:

- **The load location is declared, not inferred.** Today the master role picks
  between `preload()` and `preload_on_workers()` by looking at which export the
  app happens to have (`pregel/roles/master.lua:169-178`), and an app with both
  silently gets the master-side one.
- **`ctx:path` resolves against an explicit `base_dir`** given to
  `master.new`/`worker.new` or the cluster config — never the current directory
  and never a stack walk. §3.11: a module reached through `package.preload` has
  no directory, so no amount of inspection can answer for it. Where the *code*
  lives and where the *data* lives are separate questions, and a separate
  `asset_dir` answers the first when an app really does ship data.
- **The sink is an argument, not `self`.** Today a loader object is both the
  callable and the sink, which is why `loader_new` needs the instance before the
  caller has anything to call.

Cost: one app-level loader in the tree (`test/apps/maxvalue.lua:72`), and the
two library loaders keep their options and change their plumbing.

### 4.8 Run handles, fencing, and structured errors

Replaces: `master:start()`, `master:wait_up()`, the role's autostart wrapper,
and the absence of §3.13.

    local m = p.master.new{name = 'rank', app = app, cfg = cfg,
                           base_dir = '/srv/graphs',
                           cluster = topology, runtime = tuning}

    local run = m:run{max_supersteps = 100, timeout = 300,
                      on_progress = fn}    -- returns immediately
    run:status()                            -- a snapshot, any time
    run:wait{timeout = 10}                  -- -> result, err
    run:cancel{reason = 'operator'}
    run:vertices{batch_size = 1000}         -- result iterator, on success
    run:aggregate(name)
    m:close()                               -- refuses while a run is active

`status()` distinguishes `connecting`, `loading`, `running`, `cancelling`,
`completed`, `failed` and `cancelled`, and carries the current and completed
superstep, per-worker progress, active vertices, queued and in-flight messages,
elapsed time, and a structured error. That is a superset of what the role's
`status()` assembles today (`pregel/roles/master.lua:359-371`), which is why
§4.10 makes the role a projection of it.

A failure is an object, not a string:

    {code = 'COMPUTE_FAILED', run_id = ..., worker = 'worker-b',
     vertex = 'TASK:x', superstep = 7, message = ..., traceback = ...,
     cause = {...}}

**Fencing.** Every RPC carries `(job, run, step)`. §3.14: the master's entry
point carries no identity at all today (`pregel/master.lua:59`), so a late
aggregator report from a previous run is merged into the current one and nothing
can tell. A second master in a process is refused rather than silently replacing
the first.

**Failure containment.** §3.13's wedge is the reason this is not just
ergonomics. A compute exception must unwind the pooled object and the message
cleanup, abort further barriers, and invalidate the run's results. No rollback
and no automatic resume is promised: the queues are spaces, vertex writes have
already landed, and `deliver_batch` is explicitly non-atomic
(`pregel/worker.lua:201-203`). Diagnostics are retained and the next run gets a
fresh namespace.

**Cancellation** is cooperative and idempotent, reported as `cancelled` only
once workers acknowledge. `wait()` timing out does not cancel. A compute that
never yields cannot be interrupted by this API, and that is stated rather than
implied.

Cost: this is the largest new surface in the proposal, and the only one that
adds machinery rather than moving it. It is also the one that pays for §3.13,
§3.14, §3.6 and §3.10 together.

`master:start()` does **not** survive as an alias. The first draft kept it; §5.5
says why that reversed.

### 4.9 The settled defaults

- **The scheduling result is mandatory** (§4.4). `pregel-3v3`'s halt-by-default
  lands in v1 and is right there; v2 has no default to fall through to because
  the signature changed. See §5.1.
- **`max_supersteps` is a safety limit that fails the run**, not an outcome. A
  bounded algorithm says so with `control:finish{reason = ...}` or by returning
  `HALT`; reaching the cap means the app did not terminate, which is a failure
  and should be reported as `SUPERSTEP_LIMIT`. This reverses the first draft,
  which proposed `stopped_by = 'limit'` as an ordinary result. Natural
  completion and explicit `finish()` are evaluated before the cap.
- **A message to a missing recipient fails**, unless the app explicitly asks for
  counted drops. Today it is queued for a vertex that does not exist, is never
  read (a superstep walks the data space, so only existing vertices read), keeps
  the job alive for one extra superstep because `__messages` counts it
  (`pregel/worker.lua:356`), and is then dropped with a warn at the next queue
  swap (`pregel/worker.lua:338-346`). A typo in a receiver name is currently
  worth two log lines and one wasted superstep.

### 4.10 What stays programmatic, and what belongs to the cluster config

The split the second round proposed, which this document adopts:

- **Lua**: algorithms, dispatch, message schemas, reducers, master hooks, and
  everything `p.define` validates. An app is a Lua value, testable without a
  cluster.
- **Cluster config**: participant discovery, stable replicaset identities,
  credential *references*, transport, queue storage engine, batch sizes, the
  path base, and autostart policy.

Both entry points — the constructors and the roles — use the same validator
(§3.8). `pool_size` is renamed `batch_messages`, which is what it is.

Tarantool derives vshard's deployment from `sharding.roles`, the topology,
`iproto.advertise.sharding` and `credentials`, and the separation is worth
copying — including referencing a credentials role rather than repeating a
password, which is what `pregel-60o` proposes. What is *not* available is the
mechanism: pregel cannot invent a built-in config section, so it stays in
`roles_cfg` and does not inherit vshard's migration guarantees.

### 4.11 The small items of §3.12

None of these is worth a subsection, and leaving them undisposed is how
`write_solution` survived ten years.

- Delete `write_solution` — the private method, the `pool_new` option and the
  field. It has never had a call site.
- `v:edges()` returns an array of `{id, dst, value}`, and `v:out_degree()` its
  length. This retires `examples/pagerank/app.lua`'s counting loop and gives
  §4.6's edge ids somewhere to be read.
- The master gains the sink's methods for one-off insertion, so an app never has
  to write `master.mpool:by_id(id):put('vertex.store', ...)` as the 2016 one
  did.
- The result API is `run:vertices{}` (§4.8). The space layout stays documented;
  see open question 8.8.

## 5. Second opinion

The second review's ranked list, in its order:

1. A run has no authoritative owner (its #5 and #9 are one problem).
2. Job identity and partition identity are unsafe.
3. The document's #3 understates the ownership bug.
4. #1 is real, but topology semantics matter more than overload syntax.
5. #2 is an identity contradiction, not merely boilerplate.
6. #6 partly diagnoses correct behavior as a defect.
7. #4 and #7 are wrong diagnoses.
8. #8 is justified; #10 is minor.

Plus a challenge to default halting, three refusals, and a full v2 sketch.

Items 1, 2, 3 (second half), 4 (second half) and 5 were promoted to §3.13–§3.18
after re-verification; they are not repeated here. What follows is the four
disagreements the coordinator asked to be settled explicitly, plus the ones
where this document's verdict is not a straight acceptance.

### 5.1 Halt by default versus an explicit scheduling result

**Its claim, verbatim:** "I also challenge default halting: omission shouldn't
silently make an unfinished algorithm appear complete. Require an explicit
scheduling result. Keep `max_supersteps` as an error-producing safety limit,
separate from intentional bounded algorithms. Neither decision is implemented
here: workers explicitly activate vertices, and the master loop is unbounded
(`worker.lua:297`, `master.lua:97`)."

**Evidence.** Both pointers check out. `pregel/worker.lua:297` is
`vertex_object:vote_halt(false)` immediately before `vertex_compute`, so every
vertex the filter admits is activated by the runtime, and a compute that never
votes leaves it active. `pregel/master.lua:97-138` is `while true do` with the
only exit being `msg_count == 0 and inp_count == 0`.

**Verdict: accepted for v2; v1 is unaffected.**

The two are not in conflict. In v1 the compute function returns nothing, so the
runtime must guess what a silent compute meant, and halting is the better guess:
the failure mode of halt-by-default is a job that ends early with a readable
wrong answer, and the failure mode of the status quo is a cluster that never
stops. `pregel-3v3` is the right v1 fix and lands as decided.

In v2 there is nothing to guess, because §4.4's compute returns a tuple and the
schedule is its second element. Requiring it costs one word per return and
removes the choice entirely: `INVALID_COMPUTE_RESULT` names the bug instead of
either behaviour papering over it. The objection to halt-by-default — that an
unfinished algorithm silently looks converged — is real and is exactly the
class of defect this document is otherwise about.

`max_supersteps` as an error is accepted too, and it reverses the first draft.
The first draft proposed `stopped_by = 'limit'` as an ordinary outcome; that is
wrong for the same reason. A cap is reached only when the app failed to
terminate, and reporting that as a normal completion is how a wrong answer gets
believed. §4.9 states it as `SUPERSTEP_LIMIT`. An intentional bound is a
different thing and has a different spelling (`control:finish`, or `HALT`).

### 5.2 "Two settings channels" is a wrong diagnosis

**Its claim, verbatim:** "#4 and #7 are wrong diagnoses. Configuration and
worker-local resources are different things. The historical TASK owns training
spaces; folding those into immutable settings would damage the API. The actual
inconsistency is that roles *call* a callable context, while programmatic
construction stores it unchanged (`roles/common.lua:386`, `worker.lua:893`)."

**Evidence.** Verified, and it is worse than stated. `pregel/roles/common.lua:386-397`
calls a callable `worker_context` with `app_cfg` and stores the result;
`pregel/worker.lua:852` is `local wrk_context = options.worker_context` and
`:893` is `worker_context = wrk_context`, with no call and no check. So the same
app module produces a table from `vertex:get_worker_context()` under the roles
and a *function* under `worker.new`. The resource half is verified too: the 2016
TASK vertex assigns `self.dataSetSpace = box.space[space_name]`
(`test-avro/node_task.lua:58`) — a space handle, on a pooled vertex object,
because `worker_context` was the only slot on offer and it is built before the
instance exists.

**Verdict: accepted.** §3.5 was rewritten. The corrected diagnosis has three
parts rather than one: there is a single configuration channel delivered in two
shapes; there is a conflation of configuration with worker-local resources; and
there is a roles-versus-constructor disagreement about what a callable context
means. §4.1's `configure` and §4.2's `worker.open/close` split the first two;
one validated declaration (§4.1) removes the third.

One part of the original §3.5 the second round did not address and which stands:
the master has no `worker_context` at all (`pregel/master.lua:230-239`), so
`obtain_name` cannot be configured on the side that must agree with the workers.
§4.3 removes `obtain_name`, which removes the need.

### 5.3 roles_cfg duplication is appropriate

**Its claim, verbatim:** "Likewise, exposing deployment tuning through YAML and
constructors is appropriate; duplicating validation and defaults isn't." And:
"Both constructors and roles use one validator; renaming `pool_size` to
`batch_messages` would finally describe its meaning."

**Evidence.** `pregel/roles/common.lua:110-145` holds types, ranges and
emptiness checks; `pregel/worker.lua:846-873` holds the defaults and a different
set of asserts; neither derives from the other. `pregel-ilf` is the recorded
cost — `validate()` accepted an empty `name`, `app`, `master` or `user` and a
`password` with no `user`, while `worker.new`'s asserts never covered any of
them. `pool_size` is passed to the pool as `msg_count`, an outgoing batch size.

**Verdict: accepted.** §3.8 was rewritten from "the options are duplicated" to
"the contract is written twice and the two copies differ". The first draft's
recommendation (keep five keys, tolerate the duplication) survives with a
different reason: duplication of *exposure* is normal, duplication of
*validation and defaults* is the defect. Open question 8.7 changed accordingly,
and the rename is in §4.10.

### 5.4 S−1 aggregate reads are correct BSP

**Its claim, verbatim:** "#6 partly diagnoses correct behavior as a defect.
Reading S−1 is essential BSP semantics. Keep it. `agg()` versus `agg(value)` is
mainly an exposed implementation problem; vertices already have separate
methods. More concerning are last-write-wins reducer defaults
(`aggregator.lua:145`), one combiner for every payload, and the worker
discarding the transmitted sender (`worker.lua:88`)."

**Evidence.** All three of its "more concerning" items verified; the line
numbers are approximate but land in the right functions.

- Last-write-wins default: `pregel/aggregator.lua:148`,
  `opts.reduce or (function(_, v) return v end)`.
- One combiner: `pregel/worker.lua:864` reads a single `options.combiner` and
  `:907-916` hands the same one to both queues. The landing `lookalike` example
  has six message kinds (`pregel-bkk.3`).
- Dropped sender: `pregel/worker.lua:88-90`. Measured — delivering
  `{'bob', 'hello', 'alice'}` leaves the queue holding `["hello"]`. §3.18.

**Verdict: accepted.** The S−1 read was never actually proposed for change — the
first draft's §4.5 said `:get()` returns "the merged value from the previous
superstep" — but the section title "the aggregator is three things behind one
name" invited the reading, and lumping a correct semantic in with two defects is
how a correct semantic gets removed by someone reading quickly. §3.7 now says
plainly that S−1 stays and why, and §4.5 gives it a name (`ctx:previous`) that
says it in the call. The three real defects are in §3.7 and §3.18 and are
answered by §4.5.

The `agg()`/`agg(value)` demotion is accepted as well: it is reachable only from
library code and a console, so it is an exposed internal. What is not a demotion
is the master having no public accessor and `get_global()` being wrong there —
measured, and §3.7 keeps it.

### 5.5 Where this document changed its own mind

Not disagreements, but reversals the second round caused:

- **`vertex:update(fn)` is withdrawn.** The first draft proposed it and rejected
  a returned/copied value as too expensive. Measured: a tuple's value field is
  decoded fresh per unpack, so the value is already detached and the cost that
  objection rested on does not exist. §4.4's returned value is strictly better
  and free.
- **`master:start()` is not kept as an alias.** The first draft kept it "because
  it is unambiguous". With `run` returning a handle that owns status,
  cancellation and results, a `start()` that returns a number is a second
  lifecycle with none of that, and §3.13 is what a second lifecycle costs. The
  second round's own refusal — "compatibility overloads would preserve precisely
  the ambiguity being removed" — applies.
- **The migration order changed.** See §6.

### 5.6 The three refusals

The second round names three things it would not change. All three are accepted,
and two of them constrain §4 in ways worth stating rather than leaving implicit.

**"BSP visibility and S−1 aggregates, because every vertex must observe the same
completed step."** Accepted; §5.4. §4.5 keeps it and names it `ctx:previous`.

**"Arbitrary-ID messaging and topology mutation, because they make the
classifier expressible without artificial edges."** Accepted, and this is the
constraint that rules out the neighbour-only messaging of GraphX and the
edge-centric decomposition of GAS as a *replacement* (§6). The evidence is in
the 2016 app: a TASK vertex sends `FETCH` to a set of DATA vertices chosen from
a dataset space, not from its edges (`7fba5d4^:test-avro/node_task.lua:60-70`),
and a DATA vertex replies to whoever asked. Those are not graph edges and
inventing edges for them would mean rewriting the graph per task.
`ctx:send(dst_id, ...)` in §4.5 keeps the current freedom.

**"The pure-Lua programmatic core and independently usable Avro/math modules,
because deployment integration shouldn't become an algorithm dependency."**
Accepted, and it is why §4.10 draws the line where it does: an app is a Lua
value that `p.define` validates, and nothing in §4.1–§4.7 requires a cluster
config to exist. It is also the argument against making the roles the source of
the shared validator (open question 8.7).

### 5.7 What the second review got wrong or imprecise

Nothing in it was refuted. Six corrections, all minor, plus one place where it
understated its own case:

- **Understated:** "Changing addresses can therefore reroute names without
  relocating existing tuples." Measured, it is not "can reroute" but *does
  reroute everything*: renaming one of three worker URIs so that it sorts
  differently sends 10000/10000 names to a different instance (§3.14). The
  bucket number is stable — jump hashing is over the count — and it is the
  bucket-to-instance mapping that is positional.
- **Incomplete:** "Compute errors already propagate, but an exception skips
  message cleanup and returning the pooled vertex." True, and the consequence is
  a permanent wedge, not a leak: `pool.count` never returns to zero and the next
  superstep spins forever in `while self.vertex_pool.count > 0`. Measured
  (§3.13).
- Line numbers, all landing in the right function but not on the cited line:
  `aggregator.lua:145` is the constructor head, the default reducer is `:148`;
  `worker.lua:457` is the loop head, `group[1].value` is `:464`;
  `roles/master.lua:332` is inside the doc comment, the sentence is `:333-334`;
  `loader.lua:446` is `local stored_vertices = 0`, the two-authority pair is
  `:448` and `:453`; `pagerank/app.lua:63` is a comment, the call is `:66`.
- Its links point into `.claude/worktrees/tarantool3` rather than this worktree.
  Same commit, so every line number still resolves.

Its `checks` proposal costs nothing: `require('checks')` succeeds on the stock
Tarantool 3.9 binary, and the rockspec's only dependency today is
`lua ~> 5.1`.

## 6. Prior art

The README already claims one lineage (`README.md:9-10`, `:872-873`); the second
round supplied six more. One line each on what to take and what not to.

- **Giraph** — take `compute(vertex, messages)` as two arguments, the separation
  of vertex data from computation services, and `MasterCompute` running between
  worker steps with the ability to broadcast control state. That is §4.4 and
  §4.5's `control`. Do not take Java inheritance or the `Writable` type
  machinery. Its `Vertex` also has an id given at input time and a
  `getNumEdges()`, which are §4.3 and §4.11.
- **Pregel+** — take the separation of partial (worker) and final (master)
  aggregation, which is what this library already does and does not name. Its
  request–respond extension is worth a later specialised read protocol; do not
  fold it into `send`, whose temporal meaning must stay "arrives next
  superstep".
- **PowerGraph / GraphLab** — GAS separates gather, apply and scatter, and can
  distribute work over adjacent edges. Worth offering as a helper. Do not
  replace arbitrary-id messaging with it: the look-alike TASK's requests are not
  graph edges, and that is the whole reason §5.6's refusal list keeps arbitrary-id
  messaging.
- **Flink Gelly** — `ComputeFunction`/`MessageCombiner` is the closest existing
  match to this library's shape, and scatter-gather is worth offering as an
  optional adapter for pure propagation algorithms. Do not make every app
  implement two callbacks; a phased classifier gains nothing and pays ceremony.
- **GraphX** — take the explicitly returned vertex value from
  `vprog(id, value, message)`, which is §4.4 and the direct answer to §3.3. Do
  not take the restriction of messaging to neighbours, RDD lineage, or treating
  an iteration bound as indistinguishable from convergence — that last one is
  §4.9.
- **Ligra** — take frontier-based execution as an implementation option:
  enumerating active vertices and message receivers instead of scanning every
  tuple, which is what `run_superstep`'s full `data_space:pairs()` does today
  (`pregel/worker.lua:306`). Do not promise its shared-memory atomics or dense
  inbound traversal through this API.
- **Tarantool idioms** — take `checks` plus semantic validation, explicit names
  followed by an options table, and stable error objects.
  `box.schema.space.create(name, opts)` and
  `vshard.router.callrw(bucket, fn, args, opts)` are the local precedent, and
  they are the argument against §3.1's shifting. `checks` is in the stock binary.
- **vshard**, read at `/Users/blikh/data/workspace/vshard` — `router.new(name,
  cfg)` returns an object whose methods live in one metatable
  (`vshard/router/init.lua:1712-1741`), with module-level functions generated
  from that table and bypassing to a static instance (`:1747-1752`), and `info()`
  on both router and storage (`vshard/storage/init.lua:4270`). Take the object
  shape and `info()`. Take also the config separation — `sharding.roles`,
  `iproto.advertise.sharding` and a credentials role named `sharding`, all three
  verified present in the Tarantool 3.9 binary, which contains the check
  "Check that the vshard storage user has the credential sharding role." Do not
  expect the mechanism: that section is built into the config schema and pregel
  cannot add one (§4.10).

## 7. Compatibility and migration

**The library has no external users.** It is `pregel-scm-1.rockspec`,
unreleased, on a branch, with every consumer inside this repository: five
examples, one test app, and the test suite. There is no reason to carry a
compatibility shim, and a shim would cost more than it saves — most of §4 exists
because the old shapes are ambiguous, and a shim that accepts both keeps the
ambiguity forever while pretending it is gone. The second round reached the same
conclusion independently.

Recommendation: **no shim, no deprecation cycle**, and no `master:start()` alias
(§5.5).

Which changes are breaking:

- Breaking, for every app module: §4.1 (declaration), §4.2 (lifetimes), §4.3
  (explicit ids), §4.4 (compute signature), §4.5 (messages and reducers), §4.6
  (topology), §4.7 (loaders).
- Breaking for the programmatic caller: §4.8 (`start()` replaced by `run`).
- Breaking, with no caller at all: §4.11's deletion of `write_solution`.
- Additive: §4.11's accessors and the master's sink methods.
- Behaviour changes with no signature change: §4.9's three defaults, and §4.6's
  single mutation timing.

**Order of implementation.** This adopts the second round's order, which differs
from the first draft's and is better. The first draft ordered by blast radius —
additive things first, the compute signature last — which optimises for keeping
the tree green. The second round orders by risk: specify the semantics, then fix
identity and failure containment, then port the hardest consumer, then
consolidate the lifecycle. Two measurements settle it. §3.13's wedge and
§3.14's 100% reroute are not ergonomics, and putting them behind a batch of
renames means shipping a v2 whose worst defects are the ones v1 already had.

1. **Specify the semantics first, as contract tests.** Turn the CHANGELOG's
   delivery, mutation, aggregation and cancellation traps into tests that hold
   for both queue engines and for local as well as remote delivery. The
   CHANGELOG's `Fixed` section is already a catalogue of them; what it is not is
   a specification. Two of them were found by an agent reading doc comments
   rather than by a test going red (`pregel-iv7`, `pregel-3e8`), and two whole
   beads exist because six *other* fixes had no test that went red when the
   defect was reintroduced (`pregel-hr3`, `pregel-9vt`).
2. **Fix identity and failure containment.** Run fencing by `(job, run, step)`,
   a stable partition manifest checked against the shard on disk, refusal of a
   duplicate master, and unwinding on a compute exception (§3.13). File §3.13
   and §3.18 as v1 bugs and fix them there — neither needs to wait for a new
   API, and both are live.
3. **Port the difficult consumer first.** Implement explicit ids, returned
   values, typed messages and dispatch, then port the look-alike TASK
   (`pregel-bkk.3`) *before* simplifying PageRank. It is the only app in the
   history of this repository that uses more than a third of the API, and every
   design decision in §4.4 and §4.5 was made from reading it. Simplifying
   PageRank first would validate the easy half.
4. **Consolidate lifecycle and configuration.** Move state into run handles,
   reduce the roles to adapters over `p.define` and `m:run`, add result
   iteration, then hard-cut the obsolete APIs and the space format.

How each example migrates, once the order above reaches it:

- `max-value`, `wcc` — mechanical. `HERE`/`common.*` out, `obtain_name` deleted,
  ids passed at load, `compute` returns `(value, HALT)`, the aggregation call
  becomes `ctx:aggregate`. Both also lose their whole-value rebuild (§3.3).
- `sssp` — the same, plus `worker_context` becomes `configure` (it is
  configuration, not a resource) and `worker_preload` becomes
  `load = {on = 'workers', run = ...}`.
- `pagerank` — the same, and it loses a superstep and an aggregator to
  `ctx.graph.vertex_count` (§4.5).
- `topology-mutation` — the one touched by §4.6: edge ids, one mutation timing,
  and its `orphan_of` marker becomes the first real user of `p.dispatch`. Its
  README's two-superstep explanation still holds, because a vertex added at the
  barrier still appears in the next superstep.
- `test/apps/maxvalue.lua` — the only app-level `loader.new` in the tree, so it
  is the acceptance test for §4.7.
- `lookalike` and `mf` (landing) — see open question 8.1.

## 8. Open questions

**8.1 — Do `lookalike` and `mf` wait for v2, or land on v1 and migrate?**

Recommended: land them on v1 as specified, and port `lookalike` first in step 3
above. They are the best evidence available about whether §4.4 and §4.5 are the
right shapes, and evidence written against a design is not evidence. This is
unchanged from the first draft and is reinforced by the second round, which
built its entire TASK sketch out of the 2016 code for the same reason.

**8.2 — Where does a vertex kind live: a tuple field or a key in the value?**

Recommended: a fifth field in the `data_<name>` tuple, nullable. The value stays
entirely the app's, and the kind becomes indexable, which makes "count the TASK
vertices" a `count()` and makes §4.5's `broadcast{activate = 'DATA'}`
implementable without a full scan. The cost is a schema change a worker
restarted over an existing shard must survive — `create_spaces` uses
`if_not_exists` throughout (`pregel/worker.lua:670`) — which §3.14's partition
manifest has to handle anyway.

**8.3 — Is `obtain_name` deleted or kept optional?**

Recommended: deleted, in the same release. The first draft said "keep it through
the migration and check". §3.17 changes that: a second naming authority is not a
convenience that goes unused, it is a way for a partitioned load to silently
misroute an entire shard. One authority, enforced by there being only one.

**8.4 — Should `inbox` materialise messages or stream them?**

Recommended: both, and neither as the default shape. `inbox:fold(type, init,
fn)` is what an algorithm actually wants and never materialises;
`inbox:messages(type)` is an iterator of `{from, value}`. A `#`-able array is
not offered, because the case that makes it attractive — "did anyone talk to
me" — is `inbox:empty(type)`. This replaces the first draft's recommendation of
a plain array, which would have materialised a hub's thousands of messages in a
graph like `soc-Epinions` with no combiner.

**8.5 — Does `on_progress` run on the master's fiber?**

Recommended: yes, synchronously between supersteps, and a raise from it fails
the run. It exists to decide whether the next superstep happens, and anything
asynchronous cannot. `run:status()` is the non-blocking read for everyone else.

**8.6 — Does `run:status()` supersede the roles' `status()`?**

Recommended: no. The role keeps `read_only`, `connecting` and `failed`, because
those are facts about the *role*, and composes `run:status()` for everything
about the job. What goes away is the role's duplicate bookkeeping (§3.6) and the
autostart fiber's five-state table.

**8.7 — One validator for `roles_cfg` and the constructors: which direction?**

Recommended: the constructor's option table is the contract, `p.define` and the
role's spec are both derived from it, and the role adds only what is genuinely
YAML-only (`autostart`, `app`, `app_cfg`). The alternative — the role's spec as
the source — puts the cluster config in charge of a programmatic API that must
work without one. Rename `pool_size` to `batch_messages` in the same change
(§4.10).

**8.8 — Is the `data_<name>` space layout public interface?**

Recommended: yes for reading, explicitly, and documented in the README rather
than only in `pregel/worker.lua:8-13`. `run:vertices{}` (§4.8) is the supported
path and does not remove the need: an operator inspecting a *failed* run has no
run handle. What the layout must not be is stable across a partition change —
§3.14's manifest lives beside it, and a shard whose manifest does not match its
job must refuse to serve rather than answer for vertices it no longer owns.

**8.9 — Is `p.KEEP` worth having, or should compute always return a value?**

Recommended: keep it. A vertex that only reads its inbox and forwards is common
(`examples/wcc`, `examples/max-value` on a superstep where nothing improved),
and today the runtime already skips the write for it — `compute()` writes only
when something changed (`pregel/vertex.lua:50-79`). Making every compute return
a value would turn every read-only superstep into a full rewrite of the shard.
`p.KEEP` is how the caller says what the dirty bit used to guess.

**8.10 — Should §3.13 and §3.18 be fixed in v1 first?**

Recommended: yes, both, as their own beads, before any v2 work starts. §3.13
hangs a worker for the life of the process on any compute exception, and every
app under development will hit it. §3.18 makes the CHANGELOG's own claim about
`pregel-2qk.4` untrue. Neither fix depends on anything in §4, and leaving them
until v2 means the landing `lookalike` example — which is a request/response
protocol and will raise from compute during development — meets both on its
first day.
