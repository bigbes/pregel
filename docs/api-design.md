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
principles; section 5 records the two independent reviews and what this
document did with them; section 6 is the prior art they drew on; sections 7 and
8 cover migration and the decisions that are not mine to make.

Every claim about current behaviour in sections 2, 3 and 5 was either read out
of the code (cited as `file:line`), taken from a test that pins it (cited by
test name), or measured. **The base commit is `4549a00`** ("examples: the index
counts seven jobs"), and every pointer in this document was re-checked against
that tree. Measurements were made there, on Tarantool 3.9.0-entrypoint, either
in-process against `test/helpers/fake_pregel`, against a real in-process
`worker.new`, or by running an example under `tt`.

### 1.1 Three rounds

The first draft was written against `8221da0`. Two independent reviews
followed.

**Round two** was OpenAI Codex (`gpt-6-astra`, read-only) over the same
checkout. Its verdict was that the draft "understates how placement, timing,
and previous runs can change an application's behavior", and it was right.
Section 5 records every disagreement, the evidence checked for it, and the
verdict; section 3 was re-ranked and extended with five problems it ranked
above the ones the draft led with.

**Round three** reproduced every measured claim independently — §3.1, §3.3,
§3.4, §3.7, §3.13, §3.14, §3.16, §3.18 and the unpack timing, all within noise
— judged the document fit for a decision, and asked for the changes this
revision makes: re-anchoring to the current tip, closing seven consistency
holes in §4, and the findings §3.19–§3.20 and §4.12 that the two landed
examples produced.

Four findings changed this document's own conclusions, and three of them were
mistakes of mine:

- An in-place change to a vertex value is *sometimes* persisted, not always
  lost (§3.3). The first draft said it was always lost. Measured — and at the
  current tip, with halt-by-default landed, the rule is narrower still.
- Making `get_value()` return a copy was rejected in the first draft as "a deep
  copy per vertex per superstep on a path that runs millions of times". The
  value handed to a compute function is **already** a fresh table — a tuple
  field is decoded per unpack — so that objection was against a cost that does
  not exist. Measured: the unpack costs 1514 ns and a deepcopy on top of it
  would add 130 ns. This retires the first draft's `vertex:update(fn)` in
  favour of a returned value (§4.4).
- A compute function that raises does not merely leak a pooled object: it
  wedges the worker (§3.13). Measured, and still reproducible at `4549a00`.
- The single `combiner` option was twice cited as being read at line 864 of
  `pregel/worker.lua`. It never was — at `8221da0` that line was
  `assert(is_callable(compute), ...)`. The combiner is read at
  `pregel/worker.lua:857` at the current tip (848 at `8221da0`), and the
  citation has been corrected throughout.

### 1.2 What has landed since the first draft

The first draft listed five pieces of parallel work as "landing". Four have
landed and are now current state, which changes what several sections say:

- **`pregel-3v3` — halt by default.** A compute function that returns without
  calling `vote_halt` leaves its vertex halted (`pregel/vertex.lua:74-76`), and
  the worker no longer activates every vertex before computing it — the
  `vote_halt(false)` that used to sit at `pregel/worker.lua:297` is gone and a
  comment stands in its place (`pregel/worker.lua:301-306`). This changes §3.3,
  §3.12, §4.9 and §5.1.
- **`pregel-3wg` — `max_supersteps`.** `master.new` takes it, `start()` raises
  `pregel: superstep limit %d reached with %d active vertices and %d messages
  in flight` when it is hit (`pregel/master.lua:147-150`), and an unbounded job
  warns every hundred supersteps (`pregel/master.lua:151-156`). The first draft
  proposed exactly this and the second round argued it should be an error
  rather than an outcome; that is what shipped.
- **`examples/lookalike`** (`pregel-bkk.3`) — the distributed SGD classifier,
  957 lines, three vertex kinds. It is the app §4.4 and §4.5 were designed
  from, and it produced three new findings: §3.19, §3.20 and §4.12.
- **`examples/mf`** (`pregel-6p5.3`) — matrix factorisation, 430 lines. Source
  of §4.7's single-file loader case and §4.8's aggregate-history case.

There are now seven examples. `pregel.compress` (`pregel-4l0`) also landed.

Still in flight, and described as such where they touch a problem:

- `pregel-2c0` — the §3.13 wedge, being fixed now. Still reproducible at
  `4549a00`.
- `pregel-atx` — the §3.18 dropped sender, being fixed now.
- `pregel-moi` — an aggregator's default is shared by reference until the first
  `make_default`, and a function default is stored rather than called. Found by
  the lookalike work, which works around it with a non-mutating merge. It is
  the v1 half of §4.5's reducer contract and of decision 3(g) in §4.13.
- `pregel-60o` — the vshard-style roles config, in progress.

The v2 proposed here is not a rewrite of the machinery. It is a change of
surface and of contract over the same master, worker, queue and mpool — but
after rounds two and three it is a larger change than the first draft proposed,
because three of the promoted problems (§3.13, §3.14, §3.19) are not in the
argument lists at all.

### 1.3 Effort

Reviewer estimates, in ideal engineer-days, for the proposals in §4:

- §4.1 one app definition — **3–5**
- §4.2 explicit lifetimes — **3–6**, plus the session risk of §3.19, which is
  the one item here that can turn out to be much larger
- §4.3 explicit vertex identity — **4–7**
- §4.4 compute returning value and schedule — **4–6**
- §4.5 typed messages and real reducers — **8–12**
- §4.6 topology with edge identity — **5–8**
- §4.7 loaders as `(sink, ctx)` — **2–4**
- §4.8 run handles, fencing, structured errors — **12–20**; it is a new
  subsystem rather than a change of surface, and it is the single largest item
- §4.9 settled defaults — **2–3**, mostly landed already
- §4.10 and §4.11 config split and small items — **3–5**

**Total 50–80 days.** Against that, the two v1 defects §3.13 and §3.18 are
**1–2 days each** and are worth doing immediately whatever happens to v2 —
which is what `pregel-2c0` and `pregel-atx` are.

## 2. The API today

### 2.1 The programmatic master and worker

`pregel.master.new(name, options)` (`pregel/master.lua:282`) and
`pregel.worker.new(name, options)` (`pregel/worker.lua:851`) are the layer
everything else is built on. Both take an instance name — which names the
spaces and is how peers address the job — and a flat options table.

The master's options (`pregel/master.lua:250-263`): `workers`, `obtain_name`
(required), `pool_size`, `master_preload`, `preload_args`, `user`, `password`,
`connect_async`, `connect_timeout`, `max_supersteps`.

The worker's options (`pregel/worker.lua:810-828`): `workers`, `master`
(required), `compute` (required), `obtain_name` (required), `combiner`,
`squash_only`, `queue_engine`, `pool_size`, `delayed_push`, `worker_context`,
`worker_preload`, `preload_args`, `user`, `password`, `connect_async`,
`connect_timeout`, `grant_to`.

The master object has exactly seven methods (`pregel/master.lua:83-232`):

    add_aggregator, preload, preload_on_workers, save_snapshot, start, stop,
    wait_up

`wait_up`, `preload`, `preload_on_workers` and `add_aggregator` return `self`
and chain; `start()` blocks for the whole job and returns the superstep count
(`pregel/master.lua:105`). The worker object is not driven by anyone: its only
public method is `stop()` (`pregel/worker.lua:642`), and everything else that
happens to it arrives as a protocol message through `pregel.worker.deliver`.

`master.grant(user)` and `worker.grant(user[, instance_name])`
(`pregel/master.lua:243`, `pregel/worker.lua:781`) hand out `execute` on
`lua_call` for the four registry entry points, and — given an instance name —
read/write on that instance's spaces and their sequences. The two halves are
separate because the entry-point names exist before any instance does and the
space names do not.

Lifecycle in full: `pworker.grant`, `pmaster.grant`, `worker.new`,
`master.new`, then `master:wait_up():preload():start()`. There is no
`master:status()`, no cancellation, and no callback of any kind during
`start()`.

### 2.2 The app-module contract, as the roles consume it

A role is given a Lua module name in `roles_cfg.app` and `require()`s it
(`pregel/roles/common.lua:340`). The module returns a table. What the roles
read out of it:

- `compute(vertex)` — required by the worker role, not by the master
  (`pregel/roles/worker.lua:139` versus `pregel/roles/master.lua:103`).
- `obtain_name(value) -> string` — required by both.
- `combiner(a, b) -> c` — optional, read only by the worker role. One combiner
  per instance, for every message the job sends (`pregel/worker.lua:857`, and
  both queues get it at `:916-925`).
- `worker_preload` / `master_preload` — a loader object, a
  `callable(instance, app_cfg)` returning one, or nil
  (`pregel/roles/common.lua:248`).
- `worker_context` — any value, or a `callable(app_cfg)` returning one
  (`pregel/roles/common.lua:386`). Note that this is a *role* behaviour:
  `worker.new` stores whatever it is given, unchanged
  (`pregel/worker.lua:861`, `:902`).
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

Common to both roles (`pregel/roles/common.lua:110-145`): `name` (required),
`app` (required), `app_cfg`, `workers`, `pool_size`, `user`, `password`,
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
one per instance. `pregel-60o` is making that the only way.

`apply()` never blocks and never connects: it builds the message pool with
`connect_async = true` and hands the waiting to a fiber
(`pregel/roles/common.lua:606`), because it runs inside the config framework's
synchronous `post_apply` and a raise from there at startup exits the process.
A running job cannot be reconfigured (`pregel/roles/worker.lua:159-167`).

Neither `get()` nor `status()` is part of the role contract; both are exported
alongside `validate`/`apply`/`stop` for an operator to reach from a console.

### 2.4 The vertex API

The compute function is handed one vertex object. The objects are pooled and
reused across the vertices of a superstep (`pregel/vertex.lua:444-506`), so the
same table serves thousands of graph vertices.

Base (`pregel/vertex.lua:137-415`):

- `vertex:get_name()`
- `vertex:get_value()` / `vertex:set_value(value)`
- `vertex:get_superstep()` — counting from 1
- `vertex:vote_halt([is_halted = true])` — and since `pregel-3v3`, a compute
  function that returns without calling it leaves the vertex halted
  (`pregel/vertex.lua:74-76`).
- `vertex:get_worker_context()`

Messaging:

- `vertex:pairs_messages()` — yields `(key, message)`, where the key is the
  engine's own iteration state and is not meaningful
  (`pregel/vertex.lua:198-205`, `pregel/queue.lua:50-59`). There is no sender:
  see §3.18, where the sender turns out to be transmitted and then dropped.
- `vertex:pairs_edges()` — yields `(index, destination, value)`, walking the
  edge list as it stood at the start of the superstep.
- `vertex:send_message(receiver_name, value)` — to any vertex by name, not only
  to a neighbour; readable in the next superstep.

Aggregation:

- `vertex:get_aggregation(name)` — the merged value from the previous
  superstep, the same for every vertex of this one (`pregel/vertex.lua:267`).
- `vertex:set_aggregation(name, value)` — folds into this worker's accumulator.

Topology mutation, all queued and applied between supersteps except a change to
the running vertex's own edges:

- `vertex:add_vertex(value)`
- `vertex:add_edge([src = self:get_name(), ]dest, value)`
- `vertex:delete_vertex([name = self:get_name()][, edges = false])`
- `vertex:delete_edge([src = self:get_name(), ]dest)`

There is a fifth private method, `write_solution` (`pregel/vertex.lua:112`),
reachable through `vertex.pool_new`'s `write_solution` option. Nothing
constructs a pool with it: `pregel/worker.lua:926` passes `compute` and
`pregel` only. It was already dead in 2016 — the old worker imported
`vertex.vertex_private_methods.write_solution` at its line 27 and never called
it.

### 2.5 Aggregators and combiners

An aggregator is declared identically on the master and on every worker, under
the same name, because a worker reports its copy by name and the master looks it
up by name (`pregel/master.lua:202`, `pregel/worker.lua:515`). The app module's
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
And the declared default is stored by reference until the first
`make_default()`, so a `reduce` that mutates its accumulator rewrites the job's
default; a function default is stored rather than called (`pregel-moi`, found
by the lookalike work).

The aggregator object is also callable (`pregel/aggregator.lua:122-127`):
`agg(value)` contributes, `agg()` reads the local accumulator.

A combiner is a different thing: `callable(a, b) -> c` folding two *messages*
for one receiver into one. It runs on every put by default, or once per
superstep under `squash_only` (`pregel/queue.lua:104-118`,
`pregel/worker.lua:360`). There is one per worker instance and it is applied to
every message regardless of what the message is.

`__messages` and `__in_progress` are pregel's own aggregators and are what
decide when a run is over (`pregel/master.lua:135-140`).

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
  file. All seven examples call it at module scope and store the result in a
  local called `HERE`.
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

`pregel.avro` is reached by the graph API through one door: `loader.avro_files`
(`pregel/loader.lua:367`). Its option values are either a field name of the
file's own schema or a `function(record)`, and a field name is checked against
the schema when the loader is built rather than per record
(`pregel/loader.lua:274-297`). `vertex_value` defaults to the whole record, and
the comment at `pregel/loader.lua:385-387` says why: the worker names a stored
vertex by calling `obtain_name` on it, so a value stripped to one field would
arrive somewhere it cannot be named. See §3.17 for what that costs, and §4.7
for the shape `examples/mf` needed and did not find.

`pregel.math` does not touch the graph API at all — it is arrays and tables, so
that a weight vector can be a message payload and a percentile counter can be a
vertex value (`pregel/math/init.lua:17-21`). It is worth noting here only
because its objects have the shape section 4 proposes for aggregators:
`auc.new()` gives `:add(score, label)` and `:result()`
(`pregel/math/auc.lua:124-126`), `percentile.new()` gives `:add(v)` and
`:percentile(p)` (`pregel/math/percentile.lua:142-143`).

## 3. Problems

The subsections are numbered in the order they were found, which is not the
order they matter in. The merged ranking after three rounds, worst first:

1. **§3.13** — a run has no authoritative owner, and a failed compute leaves
   the worker unable to run another superstep. Measured.
2. **§3.14** — job identity and partition identity are implicit; renaming one
   worker's URI sends 100% of vertices to a different instance while every
   tuple stays where it was. Measured.
3. **§3.19** — an app cannot own storage: compute and loaders run as the job
   user inside an RPC and cannot do DDL, so the one real app has to smuggle a
   user name through `app_cfg` and do its DDL from `worker_context`.
4. **§3.3 with §3.15** — who owns a value table, and when its contents are
   captured. At the tip an in-place change persists exactly when the vertex
   transitions from active to halted. Measured.
5. **§3.18** — the sender of a message is transmitted and then discarded, so
   `reply` is not expressible and every request/response app hand-rolls it.
6. **§3.16** — conflicting topology mutations are resolved by arrival order.
7. **§3.17** — `loader.avro_files` has two naming authorities that can
   disagree.
8. **§3.20 and §3.21** — no round-trip primitive and no retrievable
   per-superstep series, so both landed examples compensate: one by persisting
   a protocol constant into vertex state, the other by making a reducer impure.
9. **§3.1, §3.2** — the overloads and the derived name.
10. **§3.9, §3.4** — typed vertices by convention, on an object that leaks.
11. **§3.5, §3.6, §3.7, §3.8, §3.10, §3.11, §3.12** — the contract and surface
    problems the first draft led with.

Every entry marked *Measured* was reproduced independently in round three,
within noise.

### 3.1 Overloads told apart by argument type or by nil

Three of the closed review bugs are the same defect in three methods, and the
API shape is what made all three possible.

`vertex:add_edge` (`pregel/vertex.lua:325`) tells its two forms apart by
whether the third argument is nil. Measured against the fake instance:

    v = <vertex named 'alice'>
    v:add_edge('bob', 'carol', nil)
    -- alice's own edge list becomes {{'bob', 'carol'}}
    -- nothing is routed to the worker that owns 'bob'

The caller asked for an edge from `bob` to `carol` with no value and got an
edge from `alice` to `bob` whose value is the string `'carol'`. Nothing raises.
The doc comment already tells the caller to pass `json.NULL` instead of nil,
which is an API asking to be worked around.

`vertex:delete_edge` (`pregel/vertex.lua:387`) shifts the same way. Measured:
`v:delete_edge('bob', nil)` on a vertex named `alice` deletes `alice`'s own
edge to `bob`, rather than reporting that a source was named with no
destination.

`vertex:delete_vertex` (`pregel/vertex.lua:360`) tells `delete_vertex(true)`
from `delete_vertex('name')` by testing `type(vertex_name) == 'boolean'`. This
one was already the subject of `pregel-2qk.4` (the argument shift) and then of
`pregel-hr3`, which found that the branch existing to support it had no test at
all: the mutant deleting that branch — now `pregel/vertex.lua:363-366` — left
the whole suite green until
`vertex.test_delete_vertex_with_the_flag_alone` was added.

The overload also gives one method two implementations, and they diverged.
`vertex:delete_edge(dest)` queues locally and `compute()` removes every
parallel edge to the destination in one pass (`pregel/vertex.lua:132-134` and
`85-94`); `delete_edge(src, dest)` goes to the worker's delayed path, which
stopped at the first match until `pregel-iv7` was fixed
(`pregel/worker.lua:428-435` now removes every match). Which form the caller
wrote decided how many edges went, silently, and the divergence was found by an
agent reading the doc comments rather than by any test.

That fix has already left a stale claim behind it: the doc comment at
`pregel/worker.lua:620-625` still says "One request removes one edge:
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
- the worker, again, to store (`pregel/worker.lua:554`),
- `vertex:add_vertex`, to route (`pregel/vertex.lua:302`),
- the worker again, to key the topology mutation (`pregel/worker.lua:587`).

Three consequences, all measured or cited:

- It is required even where it means nothing. `master.new{workers = {}}` with
  no loader at all raises `options.obtain_name must be callable`
  (`pregel/master.lua:290`; measured). A master that only coordinates has
  nothing to name.
- A vertex value cannot be reduced. `loader.avro_files`'s `vertex_value`
  defaults to the whole Avro record specifically because a stripped value
  cannot be named afterwards (`pregel/loader.lua:385-387`). So the storage
  layout of every vertex is decided by what `obtain_name` happens to need.
- It becomes a type dispatcher in any app with more than one kind of vertex.
  The 2016 look-alike app's `obtain_name` (`7fba5d4^:test-avro/utils.lua`) is
  twenty lines that branch on `value.vtype`, format a `'<type>:<key>'` string,
  and end in `assert(false)`. Both examples that landed since do the same
  thing: `examples/mf` prefixes `u:` and `i:` to keep users and items apart,
  and `examples/lookalike` carries a `kind` field its `obtain_name` reads.

The name is the one piece of identity pregel actually uses — it routes, stores
and addresses by exactly that string — and it is the one piece the caller is
not allowed to state. §3.17 is the sharp end of this: the Avro loader has to
name a record twice, through two different options, and nothing checks that the
two agree.

### 3.3 A value is captured, or not, depending on an unrelated call

`set_value` is what sets `__modified` (`pregel/vertex.lua:239-242`), and
`__modified` is one of three things that make `compute()` write the tuple back
(`pregel/vertex.lua:77-106`). The other two are a queued edge addition and a
queued edge deletion — and the halt flag, which `vote_halt` also routes through
`__modified`.

The first draft said an in-place change to the value is lost. That was wrong
then, and since `pregel-3v3` landed the rule has become *narrower and less
holdable*, not safer. Re-measured at `4549a00`, three compute functions that do
nothing but `self:get_value().n = 99`:

    on an ACTIVE vertex, nothing else       -> 1 write, value {"n":99}, now halted
    on an already-HALTED vertex, nothing else -> 0 writes, change lost
    on an ACTIVE vertex, plus vote_halt(false) -> 0 writes, change lost

Halt-by-default is what makes the first row write: the vertex transitions from
active to halted, `vote_halt(true)` sets `__modified`, and the tuple is
rewritten with the mutated table. So the rule at the tip is:

> an in-place change to a vertex value survives exactly when the vertex
> *changes its halt state* in the same compute call.

A vertex that halts for the first time keeps it. A vertex that was already
halted loses it. A vertex that asks to stay awake loses it. Nothing about that
is discoverable from the API, and the three cases differ by a call whose
purpose is scheduling.

The tell that this was already understood as a hazard is that every example
works around it by rebuilding the whole value: `examples/max-value/app.lua:57`
writes `self:set_value({id = vertex.id, name = vertex.name, value = best})` —
three fields copied to change one. `examples/wcc/app.lua:69` and
`examples/sssp/app.lua:62, 80` do the same, and `examples/lookalike/app.lua`
does it at every phase transition. The 2016 code wrote a helper for it
(`node_common.set_status`, which reads, assigns and writes back).

There is no performance argument for the dirty bit. Measured: a tuple's value
field is decoded fresh on every `tuple:unpack`, so the table a compute function
is handed is **already** a detached copy — 1514 ns for the unpack the runtime
already does, against 130 ns for a deepcopy that would not be needed anyway.
The first draft rejected "make `get_value()` return a copy" on a cost that does
not exist. §4.4's returned value is free.

### 3.4 Anything set on the vertex object leaks to the next vertex

`apply()` (`pregel/vertex.lua:40-53`) resets seven fields and clears the two
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
place that should have existed — and §3.19 is why even that is not enough here.

The same openness is what the 2016 app used deliberately for typed dispatch:
`computeGradientDescent` (`7fba5d4^:test-avro/common.lua`) saves the vertex's
metatable on first call, `setmetatable`s the pooled object to one of three
per-type tables built by copying `pregel.vertex.vertex_methods`
(`test-avro/node_task.lua:454-469` is one of them), calls `compute_new`, and
puts the original metatable back. That is what an app must do today to get typed
vertices, and it depends on `vertex_methods` being exported
(`pregel/vertex.lua:512`) and on the pool never noticing. The landed
`examples/lookalike` avoided it only by branching on a `kind` field at the top
of one large `compute`.

### 3.5 One configuration channel delivered twice, and a resource channel that is not one

`roles_cfg.app_cfg` reaches the app module twice, in two different shapes:

- as the second argument of `master_preload` / `worker_preload`, passed as
  `options.preload_args` and applied by `worker_new` at
  `pregel/worker.lua:907-914`,
- as the argument of a callable `worker_context`, resolved by the role before
  `worker.new` is called (`pregel/roles/worker.lua:188`).

So an app that needs the same value in both places reads it twice, through two
different mechanisms, with two different error behaviours.

Three things are worse than that.

**The roles and the constructor disagree about what a callable
`worker_context` means.** The role calls it and stores the result
(`pregel/roles/common.lua:386-397`); `worker.new` stores it unchanged
(`pregel/worker.lua:861`, `:902`), so a programmatic caller passing the same app
module's `worker_context` gets a *function* out of
`vertex:get_worker_context()` where the role's caller gets a table. Two entry
points to one library, two meanings for one field.

**`worker_context` is being asked to be two different things.** Immutable
configuration (`examples/sssp/app.lua`'s `{source = ...}`) and worker-local
resources (`examples/lookalike`'s per-task spaces, its trained models, its
report tables) are not the same kind of thing and do not have the same
lifetime. The first draft filed both under "settings", which was a wrong
diagnosis; see §5.2.

**And it is the only place DDL is possible**, which §3.19 covers and which is
the strongest evidence that the slot is overloaded: `examples/lookalike` uses
`worker_context` to create spaces, not to hold configuration.

The master, meanwhile, has no `worker_context` option at all: `master.new`'s
option list (`pregel/master.lua:250-263`) does not include it, and the master
role never calls `common.worker_context` (`pregel/roles/master.lua:242-253`). So
`obtain_name` — which the master needs, and which must agree with the workers' —
cannot be configured from `app_cfg` on the master side.

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
`superstep_count` as it goes (`pregel/master.lua:113`) precisely so something
outside can watch, and the role's `status()` reads that field directly
(`pregel/roles/master.lua:368`).

A job driven by hand through `get()` moves `status().superstep` but leaves
`status().state` at `idle` forever, because only the autostart fiber writes the
other states — stated at `pregel/roles/master.lua:333-334` as a known
limitation. §3.13 is why this is more than a cosmetic seam.

### 3.7 The aggregator surface, and the four defects under it

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

And four real defects sit under the surface, all verified:

- **The default reducer is last-write-wins.** `pregel/aggregator.lua:148`:
  `opts.reduce or (function(_, v) return v end)`. An aggregator declared with
  `{default = 0}` and nothing else silently keeps whichever contribution
  happened to arrive last, per worker, and then whichever worker reported last.
  That is not a reduction and there is no reason it should be the default.
- **The declared default is aliased.** Until the first `make_default()` the
  accumulator *is* the default table, so a `reduce` that mutates its
  accumulator rewrites the job's default for every later superstep; and a
  function default is stored as the value instead of being called
  (`pregel-moi`, found by the lookalike work, which works around it with a
  non-mutating merge).
- **One combiner for every message.** `worker.new` takes a single `combiner`
  (`pregel/worker.lua:857`) and hands it to both queues (`:916-925`). An app
  with more than one kind of message cannot combine one kind and leave the
  other alone. The 2016 look-alike app declared five message commands in an ffi
  struct (`7fba5d4^:test-avro/constants.lua`: `NONE`, `FETCH`,
  `PREDICT_CALIBRATION`, `PREDICT`, `TERMINATE`) and ran with `combiner = nil`
  (`7fba5d4^:test-avro/common.lua:361`), because no single function could fold
  a feature vector and a scalar prediction alike. The landed
  `examples/lookalike` does the same.
- **The sender is discarded.** See §3.18.

### 3.8 One contract, two validators

The first draft complained that nine of the fourteen `roles_cfg` keys are a
pass-through to `worker.new` or `master.new`. The second round is right that
this is the wrong complaint: exposing deployment tuning through both YAML and a
constructor is normal, and vshard does exactly that.

What is actually wrong is that the *contract* is written twice and the two
copies are not the same. `common_spec` (`pregel/roles/common.lua:110-145`) has
the types, the ranges and the emptiness checks; `worker_new`
(`pregel/worker.lua:855-882`) has the defaults and a different set of asserts.
Neither is derived from the other. `pregel-ilf` is what that costs: `validate()`
accepted an empty `name`, `app`, `master` or `user`, and a `password` with no
`user`, until someone went through them one at a time — while `worker.new`'s own
asserts had never covered them at all.

`pregel-60o` is removing four of the nine — `workers`, `master`, `user`,
`password` — which is right for a different reason: they are topology and
identity, and those belong to the cluster config. The five that remain are
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
- `examples/lookalike`, landed: MASTER, TASK and DATA vertices, told apart by a
  `kind` field and dispatched by a branch at the top of a 957-line module.
- `examples/mf`, landed: `u:` and `i:` name prefixes over a bipartite graph.
- `examples/topology-mutation` has a degenerate case — it branches on
  `value.orphan_of ~= nil` at the top of `compute` to tell a marker vertex from
  a real one.

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

§3.21 is the sharpest consequence: `examples/mf` wants one number per superstep
and has to build it inside an aggregator's `merge` as a side effect.

### 3.11 `HERE`

`examples/common.lua:25` reads `debug.getinfo(level, 'S')` to find out where the
app module lives, so that a relative path in `app_cfg` has a base. It solves a
real packaging problem, and it is in the wrong repository layer: every app that
reads a file needs it, and it is a helper of the examples — all seven of which
now call it.

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
  the pool with it (`pregel/worker.lua:926`). Dead in 2016 too.
- `pairs_messages()` and `pairs_edges()` both yield a leading value that means
  nothing. Every call site in the repository writes `for _, message in` or
  `for _, destination in`. An iterator whose first return is always discarded is
  a shape to fix, not a convention to document.
- Out-degree costs a loop. `examples/pagerank/app.lua` counts edges with
  `for _ in self:pairs_edges() do out_degree = out_degree + 1 end` to compute
  `rank / out_degree`; the count is `#self.__edges` and there is no accessor.
  `examples/mf` needs the same thing and pays the same loop.
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
- The two termination gaps the first draft listed are **closed**: a compute that
  never votes now halts (`pregel/vertex.lua:74-76`) and `master:start()` takes
  `max_supersteps` (`pregel/master.lua:147-150`). What remains is that neither
  is expressible in the compute signature, which §4.4 changes.

### 3.13 A run has no authoritative owner, and a failed compute stops the next one

Promoted from the second round, which ranked this first. Its diagnosis: "A
`run()` convenience wrapper alone won't establish whether retrying, cancelling,
or reading results is valid."

There is no object that represents one execution. §3.6 and §3.10 are two views
of that: the state lives in the role's autostart wrapper, the superstep counter
lives on the master, the message and vertex counts live on each worker, and
nothing ties them to an execution that can be asked whether it succeeded.

The second round pointed at what is now `pregel/worker.lua:299` — a compute
exception skips the message cleanup and the return of the pooled vertex object.
`tuple_process` (`pregel/worker.lua:299-309`) is:

    local vertex_object = self.vertex_pool:pop(tuple)   -- count = count + 1
    ...
    vertex_compute(vertex_object)                        -- raises here
    self.mqueue:delete(vertex_object.__id)               -- skipped
    self.vertex_pool:push(vertex_object)                 -- skipped, count stays up

**The first failure is loud.** The exception propagates out of `run_superstep`,
through `pregel.worker.deliver`'s `xpcall_tb`, back to the master's
`send_wait`, and `master:start()` raises `mpool: 'superstep' failed on 1
bucket(s): ...`. That part works and is not the complaint.

**The wedge is the aftermath.** `pool.count` is left at 1 and nothing ever
decrements it, so the *next* superstep — of this job or of any job created
afterwards on that worker — reaches `while self.vertex_pool.count > 0 do
fiber.yield() end` (`pregel/worker.lua:319-321`) and spins there forever, while
the master blocks in `send_wait` waiting for an answer that cannot come.
Measured at `4549a00`, two vertices, a compute that raises once:

    superstep 1, raising compute: ok=false err=... boom  pool.count=1
    superstep 2, benign compute, finished within 3s: false (pool.count=1)

Nothing upstream converts that second state into a failure. A bucket calls with
`self.connection:call(path, args)` and no timeout (`pregel/mpool.lua:274`), and
the waitpool's liveness check only notices a handler fiber that has *died*
(`pregel/mpool.lua:846-853`) — a handler blocked on a wedged worker is alive.
So the first job fails cleanly and everything after it hangs, which is the one
outcome an operator cannot act on.

`pregel-2c0` is fixing this in v1 now. The fix is a `pcall` around the compute
with the cleanup on the failure path, so the superstep raises and leaves the
pool balanced.

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
`pregel/master.lua:24` is `local master = nil` and `:337` is `master = self`,
with no check. A second `master.new` in the same process silently displaces the
first, and the module's doc comment says so as if it were a design note.

**Aggregator RPCs carry no job identity.** The worker's entry point is
`deliver_msg(name, msg, args)` — the instance name first
(`pregel/worker.lua:177`). The master's is `deliver_msg(msg, args)`
(`pregel/master.lua:59`): no name, no job, no run, no superstep. A late report
from a previous run, or from a different job in the same process, is
indistinguishable from a current one and is merged.

**A worker's identity is its position in a sorted list of URI strings.**
`mpool.new` sorts the normalized server list (`pregel/mpool.lua:1130-1142`) and
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
worker adopts whatever `data_<job>` it finds (`pregel/worker.lua:679`,
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
implementation choice; not stating it is not. §4.13(f) states the receive-side
half of the same rule.

### 3.16 Conflicting topology mutations are resolved by arrival order

Promoted from the second round, which is right that this matters more than the
overload syntax of §3.1.

`apply_topology_mutations` applies the four kinds in a fixed order — delete
edges, delete vertices, add vertices, add edges (`pregel/worker.lua:412-499`) —
and that ordering is documented and tested. What it does *not* fix is a conflict
within one kind. Two vertices adding a vertex of the same name in the same
superstep hit `self.data_space:replace{name, false, group[1].value, {}}`
(`pregel/worker.lua:473`): `group[1]` is whichever request `collect_mutations`
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

The examples do not hit this only because the one using `avro_files` on the
workers (`sssp`) happens to pass the same field to both.

### 3.18 The sender is transmitted and then discarded

Promoted from the second round, and the sharpest single finding of it: this is a
live bug and a half-finished fix. `pregel-atx` is fixing it now.

`pregel-2qk.4` fixed `vertex:send_message` to include the sender, and the
CHANGELOG says so: "`vertex:send_message()` omitted the sender, which
`message.deliver` documents and a combiner has no other way to learn". The
sending side does carry it (`pregel/vertex.lua:220-223` puts
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
still cannot learn who asked, and `pairs_messages` cannot yield a sender. The
landed `examples/lookalike` proves the cost: it is a request/response protocol,
and it writes `from = self:get_name()` into the payload of every message it
sends (three call sites: `examples/lookalike/app.lua:493, 759, 811`), exactly as
the 2016 app did.

### 3.19 An app cannot own storage

New in round three, from the landed `examples/lookalike`, and ranked third.

A compute function and a loader both run inside the `pregel.worker.deliver` or
`preload` RPC, and a `lua_call` executes with the **caller's** privileges —
which are `roles_cfg.user`'s. That user has no write access to `_space`, so
`box.schema.space.create` from inside compute raises `Write access to space
'_space' is denied for user 'pregel'`; no access to any space pregel did not
create, because `worker.grant()` covers pregel's own four and knows nothing
about an app's; and no write access to `_truncate`, so `space:truncate()` is out
and a staging space has to be cleared row by row. All of that is documented in
`examples/lookalike/README.md:155-177` and worked around in
`examples/lookalike/app.lua:193-200, 224-241, 266`.

The workaround the app arrived at, and it is the only one available:

- do the DDL in `worker_context()`, which the worker role calls from `apply()`
  and therefore **as admin**;
- learn the user to grant to through `app_cfg.grant_to`, because an app module
  cannot read `roles_cfg` (`examples/lookalike/app.lua:239-241`, and
  `README.md:323` documents the key);
- create a space for *every* task on *every* worker, not only for the tasks
  whose vertices that worker owns, because the mpool that would say which those
  are does not exist yet at the one moment DDL is allowed
  (`README.md:178-181`).

Three separate design failures in one workaround: the only DDL window is a slot
meant for configuration; the job user's name has to travel through an opaque
app-configuration table; and the one piece of information that would make the
DDL minimal (the partitioning) is not available when the DDL must happen.

This is also why §4.2's `worker.open/close` as first proposed is not enough:
whichever side `open` runs on, it cannot do both. §4.13(h) decides it.

### 3.20 No round-trip primitive, so apps do arithmetic on the superstep

New in round three, from both landed examples.

A question asked in superstep S is read in S+1 and answered into S+2. Nothing in
the API says so or helps, so an app that asks questions must track the delay
itself. `examples/lookalike` does it with an `await` field in the vertex value:

    value.await = self:get_superstep() + 2
    ...
    local due = value.await == nil or self:get_superstep() >= value.await

(`examples/lookalike/app.lua:745, 765, 818`.) Its own comment says what happens
without it — "the phase runs immediately on an empty inbox and concludes that
nobody answered" — and names the 2016 code's version of the same workaround, a
"Master didn't receive any messages, waiting one superstep" branch that could
not tell a slow round trip from a question nobody could answer.

`examples/mf` does a milder version: `local epoch = superstep - 1`
(`examples/mf/app.lua:296`), because superstep 1 is initialisation and every
later one is an epoch.

Both are the app compensating for the runtime not modelling a round trip. The
`await` version is worse than bookkeeping: it encodes a *protocol timing* into
persisted vertex state, so changing when a phase sends its questions means
changing an arithmetic constant in a stored value.

### 3.21 A per-superstep series cannot be retrieved

New in round three, from `examples/mf`.

The master keeps one value per aggregator and resets it between supersteps
(`pregel/master.lua:122-124`), so once a job is done every superstep's
aggregate except the last is gone. `examples/mf` wants exactly that series — the
training error of each of thirty epochs — and the only hook that runs on the
master once per superstep is an aggregator's `merge`. So it builds the history
as a **side effect of merging**:

    local function merge_sse(old, new)
        local rv = add_sse(old, new)
        if rv.superstep > 0 then
            history[rv.superstep] = {sse = rv.sse, n = rv.n}
        end
        return rv
    end

(`examples/mf/app.lua:183-193`.) And because `merge` is handed two accumulators
and nothing else, the superstep number has to travel *inside the accumulator*:
`train_sse` is a table `{sse, n, superstep}` rather than a number, with a
`merge` that takes the larger superstep of the two so that a worker reporting
the default does not erase the real one (`examples/mf/app.lua:171-181`).

An app should not have to make a reducer impure to find out what its own job
did. §4.8's `on_progress` and `run:history` are the answer.

## 4. Proposal for v2

Twelve changes and a list of small ones. Each is stated as a signature, with
what it replaces, why, what it costs, and — where one exists — a before/after
taken from a real file in this repository. §4.13 records the consistency
decisions round three asked for, which are binding on everything above it.

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
        worker      = {prepare = fn(ctx), open = fn(ctx),
                       close = fn(services, outcome)},
        compute     = fn | p.dispatch{<KIND> = fn, ...},
        master      = {after_step = function(control) end},
    }

`p.define` validates the declaration once, where the app is written, and returns
something both `master.new`/`worker.new` and the roles consume unchanged. That
is what makes §3.8's second validator unnecessary: there is one contract object
and one checker for it.

`configure(raw)` is pure and must return something serializable. It is where
defaults and semantic validation live — the thing every example currently does
its own way with `common.cfg`.

Why: §3.5's two channels, §3.8's two validators, and §2.2's "everything else in
the module is invisible" all come from there being no declaration at all, only a
set of names the roles happen to look up.

Cost (3–5 days): every app module changes shape. Seven examples plus the test
app.

### 4.2 Explicit lifetimes for worker-local resources

Replaces: `worker_context`, in all three of its current meanings, and
`examples/lookalike`'s `app_cfg.grant_to`.

    app.worker.prepare(ctx)             -- admin-side, at config apply; DDL allowed
    app.worker.open(ctx)                -- job-side, after readiness; -> services
    app.worker.close(services, outcome) -- on completion or failure
    -- in compute:  ctx.cfg      -- the normalized configuration (immutable)
    --              ctx.services -- what open() returned

Two phases, because one is provably not enough — see §3.19 and the decision at
§4.13(h). `prepare` runs inside the role's `apply()` with admin privileges,
before any peer exists: it may create spaces and grant them, and it may not talk
to another instance. `open` runs after the pool is ready: it may talk to peers
and read the partitioning, and it may not do DDL. `close` runs on both the
success and the failure path, which nothing does today.

`prepare` is given the job user as `ctx.job_user`, supplied by the runtime — the
role knows `roles_cfg.user`, the programmatic constructor takes it as an
option — so a user name never travels through `app_cfg` again. And
`ctx:space(name, format, index)` creates and grants in one call, so an app does
not have to know the grant rule at all.

Why: configuration and resources are different things with different lifetimes
(§3.5), the current API has one slot for both, and that slot is also the only
DDL window (§3.19). The 2016 evidence is a space handle stashed on a pooled
vertex object (`test-avro/node_task.lua:58`); the 2026 evidence is a landed
example creating one space per task on every worker because it cannot find out
which tasks it owns (`examples/lookalike/README.md:178-181`).

Cost (3–6 days, plus risk): one new pair of callbacks and a privileged helper.
The risk is that `prepare` running at config-apply time inherits the role's
constraint that it must not block — an app that reads a large file there holds
up the instance's startup, exactly as §2.3 describes for `apply()`. `open` is
where anything slow belongs, and `prepare` should be limited to DDL.

Before (`examples/lookalike/app.lua:224-241`, plus `README.md:169-173`):

    -- inside worker_context(), which the role calls as admin
    space = box.schema.space.create(name, {...})
    space:create_index('primary', {...})
    if self.grant_to ~= nil then
        box.schema.user.grant(self.grant_to, 'read,write', 'space', name, ...)
    end

After:

    prepare = function(ctx)
        for _, task in ipairs(ctx.cfg.tasks) do
            ctx:space('lookalike_' .. task, FORMAT, INDEX)   -- created and granted
        end
    end,

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
twenty-line dispatcher, `mf`'s `u:`/`i:` prefixes and `lookalike`'s `kind`
branch all reduce to one expression. Display names stay in the value, where
`examples/max-value/app.lua:29-33` already explains they belong.

Why: §3.2 (four hidden call sites, a value that cannot be reduced, a type
dispatcher in disguise) and §3.17 (two authorities that can silently disagree
and misroute a whole partitioned load). One `id` makes the second impossible by
construction.

Cost (4–7 days): one `store_vertex` call in the tree
(`test/apps/maxvalue.lua:74`), one `add_vertex`
(`examples/topology-mutation/app.lua:80`), seven `obtain_name` definitions
deleted, and the library loaders. `store_edge` already takes names.

Duplicate ids fail by default rather than silently resetting the vertex's value
and edges, which is what `vertex_store`'s `replace` does today
(`pregel/worker.lua:553-556`).

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
place — persists it. What `p.KEEP` means in the presence of an edge mutation is
decided at §4.13(a).

Why the returned value: §3.3. At the current tip an in-place change survives
exactly when the vertex changes its halt state in the same call, which is a rule
nobody can hold and which nothing in the API hints at. A returned value has no
such coupling and catches nested table changes, which no dirty bit can. And it
is free: measured, the value handed to compute is already a fresh decode of the
tuple field (1514 ns), so there is nothing extra to copy. This supersedes the
first draft's `vertex:update(fn)`, which was proposed only because copying was
believed expensive.

Why the mandatory schedule: see §5.1. Halt-by-default (`pregel-3v3`) has landed
and is the right v1 answer; v2 has no default to fall through to because the
signature changed, so requiring the second element costs one word and removes
the guess entirely.

New vertices start `ACTIVE`. A message reactivates a halted vertex, as today.

Why `inbox` and `ctx` as arguments: §3.12's discarded iterator values, and
making it visible in the signature that the context is per-worker and shared
rather than something reached through the vertex.

Why `p.dispatch`: §3.9. It is a table lookup by `v.kind` with a named error for
an unhandled kind, replacing three metatables built by copying
`pregel.vertex.vertex_methods` — and replacing the top-of-`compute` branch that
`examples/lookalike` currently opens with.

Cost (4–6 days): the compute signature changes in all eight app modules.
Mechanical for the six single-kind ones.

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

Replaces: one combiner for everything, the last-write-wins reducer, the aliased
default, the aggregator's callable form, and the aggregator-as-broadcast-channel
of the 2016 app.

    app.messages = {rank = {combine = function(a, b) return a + b end},
                    fetch = {}, sample = {}, calibrate = {}}

    ctx:send(dst_id, type, payload)
    ctx:reply(message, type, payload)
    ctx:send_edges(v, type, payload)
    inbox:messages(type)             -- iterator of {from = ..., value = ...}
    inbox:fold(type, initial, reduce)
    inbox:empty(type)

    app.aggregators = {dangling = p.reducers.sum(),
                       models   = p.reducers.unique_map()}
    ctx:aggregate(name, contribution)
    ctx:previous(name)               -- the completed S-1; init() at step 1
    control:reduced(name)            -- the just-completed S, on the master
    control:broadcast(name, value, {activate = 'DATA'})
    control:finish{reason = 'iterations'}

Seven decisions in that, each answering a verified problem:

- **Per-type combiners** (§3.7): combining is scoped to
  `(run, superstep, destination, type)`. A combiner must tolerate arbitrary
  grouping and order.
- **`from` on every message** (§3.18): the sender already travels and is
  dropped at `pregel/worker.lua:90`; `pregel-atx` is fixing that in v1.
  Carrying it through to the inbox is what makes `reply` expressible and stops
  every request/response app from writing `from = self:get_name()` into each
  payload, as `examples/lookalike/app.lua:493, 759, 811` does. What `from` is
  for a *combined* message is decided at §4.13(e).
- **Reducers are `{init, accumulate, merge}`, with no implicit default**
  (§3.7): `p.reducers.sum()` and friends are the common ones. An aggregator
  declared with no reduction is an error, not last-write-wins. `init` is called
  per accumulator rather than shared, which is the contract `pregel-moi` says
  v1 breaks.
- **S−1 reads stay, and get a name** (§3.7, §5.4): `ctx:previous(name)` says in
  the call what `get_aggregation` says only in a doc comment. Its value at step
  1 is decided at §4.13(g).
- **The master's own view is a different method on a different object**
  (`control:reduced`), because it is a different value — the step that just
  finished, not the one before it.
- **Broadcast is not an aggregator.** The 2016 app used a per-task aggregator
  with a max-by-command merge to push a model out to every DATA vertex. A master
  publication that becomes visible next step and can activate a whole kind
  expresses that directly, without manufacturing one identical message per
  vertex.
- **Unknown types and nil payloads fail.** `box.NULL` is the way to send
  nothing, as it already is for edge values.

Cost (8–12 days): every aggregation call site, and the `combiner` option
becomes a per-type declaration.

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
vertex count maintained by the runtime (§4.13(c)) removes a superstep and an
aggregator from the example.

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
`examples/topology-mutation/app.lua:62-65` says cannot be done. Where those
edges live is decided at §4.13(a).

**Every mutation takes effect at the barrier, including one to the current
vertex's own edges.** Today the local path is applied when the vertex is written
back and the remote path between supersteps (`pregel/vertex.lua:336-346`), which
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

Cost (5–8 days): two call sites in the examples, plus the internal `_delayed`
handlers and whatever §4.13(a) decides about storage. The first draft answered
§3.1 by splitting each overload into two methods (`add_edge` / `add_edge_from`
and so on); named fields make that unnecessary, which is a better answer than
four more names — and it is the local Tarantool idiom
(`box.schema.space.create(name, opts)`, see §6).

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

Four changes:

- **The load location is declared, not inferred.** Today the master role picks
  between `preload()` and `preload_on_workers()` by looking at which export the
  app happens to have (`pregel/roles/master.lua:169-178`), and an app with both
  silently gets the master-side one.
- **`ctx:path` resolves against an explicit `base_dir`** given to
  `master.new`/`worker.new` or the cluster config — never the current directory
  and never a stack walk. §3.11: a module reached through `package.preload` has
  no directory, so no amount of inspection can answer for it. A separate
  `asset_dir` covers an app that really does ship data next to its code.
- **The sink is an argument, not `self`.** Today a loader object is both the
  callable and the sink, which is why `loader_new` needs the instance before the
  caller has anything to call.
- **A single file may be both vertices and edges.** New in round three, from
  `examples/mf`, whose comment says `avro_files` "is no use here: it wants a
  vertex file and an edge file, and a ratings dataset is one file that is
  *both* — every record names two vertices and one edge, and the edge has to
  exist in both directions" (`examples/mf/app.lua:360-372`). It writes its own
  two-pass loader instead. The adapter grows a single-file mode:

      p.loaders.avro(sink, ctx, {
          records = ctx:path(ctx.cfg.train),
          vertex  = function(record) return ... end,  -- may return nil, or several
          edges   = function(record) return ... end,  -- may return several
      })

  `vertex` returning nil means "this record declares no vertex", `edges`
  returning several covers the both-directions case, and the adapter still does
  the ownership filtering and the batching that a hand-written loader has to
  re-derive.

Cost (2–4 days): one app-level `loader.new` (`test/apps/maxvalue.lua:72`), plus
`examples/mf`'s hand-written loader, which the single-file mode retires.

### 4.8 Run handles, fencing, and structured errors

Replaces: `master:start()`, `master:wait_up()`, the role's autostart wrapper,
the absence of §3.13, and §3.21's history hack.

    local m = p.master.new{name = 'rank', app = app, cfg = cfg,
                           base_dir = '/srv/graphs', job_user = 'pregel',
                           cluster = topology, runtime = tuning}

    local run = m:run{max_supersteps = 100, timeout = 300,
                      on_progress = fn, history = 200}   -- returns immediately
    run:status()                            -- a snapshot, any time
    run:wait{timeout = 10}                  -- -> result, err
    run:cancel{reason = 'operator'}
    run:vertices{batch_size = 1000}         -- result iterator, on success
    run:aggregate(name)                     -- the final merged value
    run:history(name)                       -- the per-superstep series
    m:close()                               -- refuses while a run is active

`status()` distinguishes `connecting`, `loading`, `running`, `cancelling`,
`completed`, `failed` and `cancelled`, and carries the current and completed
superstep, per-worker progress, active vertices, queued and in-flight messages,
elapsed time, and a structured error. That is a superset of what the role's
`status()` assembles today (`pregel/roles/master.lua:359-371`), which is why
§4.10 makes the role a projection of it.

`on_progress(status)` is called once per completed superstep with that
snapshot, and `status.aggregates` carries the merged value of every aggregator
for the step that just finished. `run:history(name)` returns the retained
series, bounded by the `history` option (default: keep the last 200 steps;
`history = false` keeps none). Together they retire §3.21: `examples/mf` stops
needing an impure `merge`, and `train_sse` goes back to being a number.

A failure is an object, not a string:

    {code = 'COMPUTE_FAILED', run_id = ..., worker = 'worker-b',
     vertex = 'TASK:x', superstep = 7, message = ..., traceback = ...,
     cause = {...}}

**Fencing.** Every RPC carries `(job, run, step)`. §3.14: the master's entry
point carries no identity at all today (`pregel/master.lua:59`), so a late
aggregator report from a previous run is merged into the current one and nothing
can tell. A second master in a process is refused rather than silently replacing
the first.

**Failure containment.** §3.13 is the reason this is not just ergonomics. A
compute exception must unwind the pooled object and the message cleanup, abort
further barriers, and invalidate the run's results — the v1 half of which is
`pregel-2c0`. No rollback and no automatic resume is promised: the queues are
spaces, vertex writes have already landed, and `deliver_batch` is explicitly
non-atomic (`pregel/worker.lua:201-203`). Diagnostics are retained and the next
run gets a fresh namespace.

**Cancellation** is cooperative and idempotent, reported as `cancelled` only
once workers acknowledge. `wait()` timing out does not cancel. A compute that
never yields and never calls `ctx:checkpoint()` (§4.13(d)) cannot be
interrupted, and that is stated rather than implied.

Cost (12–20 days): the largest item in this document, and the only one that
adds a subsystem rather than moving one. It pays for §3.13, §3.14, §3.21, §3.6
and §3.10 together.

`master:start()` does **not** survive as an alias. The first draft kept it;
§5.5 says why that reversed.

### 4.9 The settled defaults

- **The scheduling result is mandatory** (§4.4). `pregel-3v3`'s halt-by-default
  has landed and is right for v1; v2 has no default to fall through to because
  the signature changed. See §5.1.
- **`max_supersteps` is a safety limit that fails the run**, not an outcome —
  which is what `pregel-3wg` shipped (`pregel/master.lua:147-150`). A bounded
  algorithm says so with `control:finish{reason = ...}` or by returning `HALT`;
  reaching the cap means the app did not terminate. This reversed the first
  draft, which proposed `stopped_by = 'limit'` as an ordinary result.
- **A message to a missing recipient fails**, unless the app explicitly asks for
  counted drops. Today it is queued for a vertex that does not exist, is never
  read (a superstep walks the data space, so only existing vertices read), keeps
  the job alive for one extra superstep because `__messages` counts it
  (`pregel/worker.lua:365`), and is then dropped with a warn at the next queue
  swap (`pregel/worker.lua:347-355`). A typo in a receiver name is currently
  worth two log lines and one wasted superstep.

Cost (2–3 days): two thirds of this has landed already.

### 4.10 What stays programmatic, and what belongs to the cluster config

The split the second round proposed, which this document adopts:

- **Lua**: algorithms, dispatch, message schemas, reducers, master hooks, and
  everything `p.define` validates. An app is a Lua value, testable without a
  cluster.
- **Cluster config**: participant discovery, stable replicaset identities,
  credential *references*, transport, queue storage engine, batch sizes, the
  path base, the job user, and autostart policy.

Both entry points — the constructors and the roles — use the same validator
(§3.8). `pool_size` is renamed `batch_messages`, which is what it is.

Tarantool derives vshard's deployment from `sharding.roles`, the topology,
`iproto.advertise.sharding` and `credentials`, and the separation is worth
copying — including referencing a credentials role rather than repeating a
password, which is what `pregel-60o` is doing. What is *not* available is the
mechanism: pregel cannot invent a built-in config section, so it stays in
`roles_cfg` and does not inherit vshard's migration guarantees.

### 4.11 The small items of §3.12

None of these is worth a subsection, and leaving them undisposed is how
`write_solution` survived ten years.

- Delete `write_solution` — the private method, the `pool_new` option and the
  field. It has never had a call site.
- `v:edges()` returns an array of `{id, dst, value}`, and `v:out_degree()` its
  length. This retires the counting loop in `examples/pagerank` and
  `examples/mf`, and gives §4.6's edge ids somewhere to be read.
- The master gains the sink's methods for one-off insertion, so an app never has
  to write `master.mpool:by_id(id):put('vertex.store', ...)` as the 2016 one
  did.
- The result API is `run:vertices{}` (§4.8). The space layout stays documented;
  see open question 8.9.
- Fix the stale comment at `pregel/worker.lua:620-625`, which describes
  behaviour `pregel-iv7` changed (§3.1). It is a one-line v1 change and it is
  currently the only place in the tree that tells a reader the wrong thing about
  edge deletion.

Cost (3–5 days, with §4.10).

### 4.12 A round-trip primitive

New in round three, answering §3.20. This is the one proposal in §4 that this
document recommends **deferring**, and the recommendation is recorded as open
question 8.4 rather than as a decision.

The shape, if it is built:

    ctx:request(dst_id, type, payload)     -- an ordinary send, tracked
    inbox:answers(type)                    -- answers to this vertex's requests
    ctx:pending(type)                      -- how many are still outstanding

with the runtime knowing that a request sent in S is answered into S+2 and a
vertex with outstanding requests staying active until they arrive or the app
gives up.

Why defer it: it needs §3.18's sender fix and §4.5's typed messages underneath,
and both real apps' hand-rolled arithmetic is at least *correct* today. Why not
drop it: `examples/lookalike` persists a protocol constant (`await = step + 2`)
into vertex state, so changing when a phase asks its questions means editing a
number inside stored data.

The cheap half, worth doing in the first v2 either way: `ctx:send(dst, type,
payload, {deliver_in = 2})`, so the delay is stated at the send rather than
recomputed at every read.

### 4.13 Consistency decisions

Round three asked for seven holes in the above to be closed with a decision
rather than left implicit. An eighth (h) covers §3.19.

**(a) `p.KEEP` and edge mutation; where edges live.** *Decision: edges stay in
the vertex tuple, and `p.KEEP` governs the value field only.* The tuple is
written when the returned value is not `p.KEEP`, or the schedule changed the
halt flag, or an edge mutation for this vertex was applied at the barrier —
the same three triggers as today, minus the guessing, because the first is now
explicit. `p.KEEP` never suppresses an edge mutation: mutations are queued
through `ctx.graph` and applied by the runtime, and a compute function that
deletes an edge and returns `p.KEEP` gets exactly that.

Moving edges to their own space was considered and rejected for v2. It has real
benefits — per-edge identity is natural, an edge update stops being a
whole-tuple rewrite, a high-degree vertex stops being one large tuple — but it
costs a second space and index and turns "read a vertex and its edges" into two
lookups on the hot path of every superstep, and §4.6's `(src, edge_id)`
identity is expressible inside the array by making each element
`{id, dst, value}`. So the identity problem does not force the storage change.
Recorded as open question 8.2, because it is a storage decision with a
performance profile I have not measured.

**(b) `ctx.step`.** *Decision: declared, and it is the only place the superstep
number lives.* `ctx.step` is the superstep now running, counted from 1. It
replaces `vertex:get_superstep()`, and it goes on the context rather than the
vertex because it is a property of the run: every vertex of a superstep sees the
same number, and reading it off the vertex suggests otherwise. See open question
8.5.

**(c) `ctx.graph.vertex_count`.** *Decision: declared, refreshed at topology
barriers.* It is the number of vertices in the whole graph as of the last
completed barrier — computed by the runtime during the same barrier that applies
topology mutations, from the workers' own counts, so it is constant for the
whole of a superstep and identical on every worker. At step 1 it is the count
established when loading finished. There is deliberately **no**
`ctx.graph.edge_count`: an edge count would need a scan of every tuple's array
at every barrier, and no app in the tree has asked for one.

**(d) `ctx:checkpoint()`.** *Decision: defined, and kept.* It is a cooperative
yield-and-cancellation point for a long compute: it yields the fiber and raises
`RUN_CANCELLED` if the run is cancelling. A compute that neither yields nor
calls it cannot be interrupted (§4.8 says so). It earns its place because
`examples/lookalike` trains a model inside one compute call — the one place in
the tree where a single vertex can occupy a worker for a long time.

**(e) `from` on a combined message.** *Decision: `nil`, and `ctx:reply` refuses
one.* A combined message is a fold over several messages from several senders,
so there is no honest answer. `ctx:reply(message, ...)` raises `NO_SENDER` when
handed one. The consequence is a rule an app can hold: declaring
`{combine = fn}` for a type is also declaring that the type is not repliable,
so a request type simply does not declare a combiner.

**(f) Ownership of what `inbox:messages` yields.** *Decision: the runtime owns
it; the callback may read it and must not retain it.* The iterator reuses one
table per message, for the same reason the vertex object is pooled. An app that
wants to keep a message copies it. This is the receive-side half of §3.15's
send-side rule (*the runtime captures a payload before `send` returns, and the
caller may reuse its table afterwards*), and stating both is the point:
today neither is stated and both are wrong in a different direction.

**(g) `ctx:previous` at step 1.** *Decision: it returns `init()` — a freshly
built accumulator, never a shared one.* There is no S−0 to read, and the
reducer's identity is the only honest answer; `pregel-3e8`'s fix already does
this with `default`. Building it fresh per call is not pedantry: it is exactly
what `pregel-moi` says v1 gets wrong, where the accumulator *is* the declared
default table until the first `make_default()` and a mutating reduce rewrites
the job's default.

**(h) Where `worker.open` runs.** *Decision: two phases — `prepare` admin-side
at config apply, `open` job-side after readiness.* §3.19 shows that neither
alone works: admin-side has the privileges for DDL but no peers and therefore no
partitioning, job-side has the partitioning but runs as the job user inside an
RPC and cannot create a space. `prepare` learns the job user from
`ctx.job_user`, supplied by the runtime, which retires
`examples/lookalike`'s `app_cfg.grant_to`; `ctx:space(name, format, index)`
creates and grants in one call. See open question 8.1, because the alternative —
granting the job user DDL rights on a namespace — is a security decision I
should not take alone.

## 5. The two reviews

### 5.0 Round three

The third review reproduced every measured claim in this document
independently — §3.1, §3.3, §3.4, §3.7, §3.13, §3.14, §3.16, §3.18 and the
unpack timing — with numbers within noise, and found no factual error. What it
asked for is what §1.2, §3.19–§3.21, §4.12, §4.13 and §8 now contain: the tip
re-anchoring, the seven consistency decisions, the findings the two landed
examples produced, and five maintainer questions.

One correction it prompted that is mine rather than either reviewer's: the
single `combiner` option was twice cited as being read at line 864 of
`pregel/worker.lua`, which at `8221da0` was an assert. It is read at
`pregel/worker.lua:857` (848 at `8221da0`).

### 5.1 Halt by default versus an explicit scheduling result

**Round two's claim, verbatim:** "I also challenge default halting: omission
shouldn't silently make an unfinished algorithm appear complete. Require an
explicit scheduling result. Keep `max_supersteps` as an error-producing safety
limit, separate from intentional bounded algorithms. Neither decision is
implemented here: workers explicitly activate vertices, and the master loop is
unbounded (`worker.lua:297`, `master.lua:97`)."

**Evidence, restated for the current tip.** Both pointers were correct at
`8221da0` and **neither exists now.** `pregel/worker.lua:297` was
`vertex_object:vote_halt(false)` immediately before `vertex_compute`, so every
vertex the filter admitted was activated by the runtime and a compute that never
voted left it active. `pregel-3v3` deleted that line; a comment stands in its
place (`pregel/worker.lua:301-306`) explaining that being computed at all is the
wake-up, and the default halt is applied inside `compute()` instead
(`pregel/vertex.lua:74-76`), routed through `vote_halt(true)` so the worker's
count of active vertices stays exact. `pregel/master.lua:97-138` was the
unbounded `while true`; the loop is now at `pregel/master.lua:105-159` and
raises when `max_supersteps` is reached (`:147-150`), warning every hundred
supersteps when it is unset (`:152-157`).

So round two's challenge was to a state of the code that no longer exists, and
both halves of what it asked for shipped in v1 — one as it asked (the error) and
one against it (the default).

**Verdict: accepted for v2; v1's halt-by-default is right and stays.**

The two are not in conflict. In v1 the compute function returns nothing, so the
runtime must guess what a silent compute meant, and halting is the better guess:
the failure mode of halt-by-default is a job that ends early with a readable
wrong answer, and the failure mode of the status quo was a cluster that never
stopped. In v2 there is nothing to guess, because §4.4's compute returns a tuple
and the schedule is its second element. `INVALID_COMPUTE_RESULT` names the bug
instead of either behaviour papering over it.

`max_supersteps` as an error is accepted too, and it reversed the first draft,
which proposed `stopped_by = 'limit'` as an ordinary outcome. A cap is reached
only when the app failed to terminate, and reporting that as a normal completion
is how a wrong answer gets believed. That is what shipped.

### 5.2 "Two settings channels" is a wrong diagnosis

**Its claim, verbatim:** "#4 and #7 are wrong diagnoses. Configuration and
worker-local resources are different things. The historical TASK owns training
spaces; folding those into immutable settings would damage the API. The actual
inconsistency is that roles *call* a callable context, while programmatic
construction stores it unchanged (`roles/common.lua:386`, `worker.lua:893`)."

**Evidence.** Verified, and it is worse than stated.
`pregel/roles/common.lua:386-397` calls a callable `worker_context` with
`app_cfg` and stores the result; `pregel/worker.lua:861` is `local wrk_context =
options.worker_context` and `:902` is `worker_context = wrk_context`, with no
call and no check. So the same app module produces a table from
`vertex:get_worker_context()` under the roles and a *function* under
`worker.new`. The resource half is verified twice over: the 2016 TASK vertex
assigns `self.dataSetSpace = box.space[space_name]`
(`test-avro/node_task.lua:58`), and the landed `examples/lookalike` uses
`worker_context` to create and grant one space per task
(`examples/lookalike/app.lua:224-241`) — §3.19.

**Verdict: accepted, and round three widened it.** §3.5 was rewritten with three
parts rather than one, and §3.19 is the fourth: the slot is also the only DDL
window. §4.1's `configure` and §4.2's `prepare`/`open`/`close` split them; one
validated declaration removes the roles-versus-constructor disagreement.

### 5.3 roles_cfg duplication is appropriate

**Its claim, verbatim:** "Likewise, exposing deployment tuning through YAML and
constructors is appropriate; duplicating validation and defaults isn't." And:
"Both constructors and roles use one validator; renaming `pool_size` to
`batch_messages` would finally describe its meaning."

**Evidence.** `pregel/roles/common.lua:110-145` holds types, ranges and
emptiness checks; `pregel/worker.lua:855-882` holds the defaults and a different
set of asserts; neither derives from the other. `pregel-ilf` is the recorded
cost. `pool_size` is passed to the pool as `msg_count`, an outgoing batch size.

**Verdict: accepted.** §3.8 was rewritten from "the options are duplicated" to
"the contract is written twice and the two copies differ". Open question 8.8
carries it, and the rename is in §4.10.

### 5.4 S−1 aggregate reads are correct BSP

**Its claim, verbatim:** "#6 partly diagnoses correct behavior as a defect.
Reading S−1 is essential BSP semantics. Keep it. `agg()` versus `agg(value)` is
mainly an exposed implementation problem; vertices already have separate
methods. More concerning are last-write-wins reducer defaults
(`aggregator.lua:145`), one combiner for every payload, and the worker
discarding the transmitted sender (`worker.lua:88`)."

**Evidence.** All three "more concerning" items verified.

- Last-write-wins default: `pregel/aggregator.lua:148`.
- One combiner: `pregel/worker.lua:857` reads a single `options.combiner` and
  `:916-925` hands the same one to both queues.
- Dropped sender: `pregel/worker.lua:88-90`. Measured — delivering
  `{'bob', 'hello', 'alice'}` leaves the queue holding `["hello"]`. §3.18.

A fourth defect in the same area surfaced later and belongs with them: the
aliased default (`pregel-moi`), found by the lookalike work.

**Verdict: accepted.** The S−1 read was never proposed for change, but the
section title invited that reading, and lumping a correct semantic in with three
defects is how a correct semantic gets removed by someone reading quickly. §3.7
now says plainly that S−1 stays, §4.5 names it `ctx:previous`, and §4.13(g)
pins its value at step 1.

### 5.5 Where this document changed its own mind

- **`vertex:update(fn)` is withdrawn.** The first draft proposed it and rejected
  a returned value as too expensive. Measured: the value is already detached, so
  the cost that objection rested on does not exist.
- **`master:start()` is not kept as an alias.** With `run` returning a handle
  that owns status, cancellation and results, a `start()` that returns a number
  is a second lifecycle with none of that, and §3.13 is what a second lifecycle
  costs.
- **`max_supersteps` raises rather than reporting an outcome** (§5.1). Shipped
  that way.
- **The migration order changed.** See §7.
- **§4.12 is deferred rather than proposed.** Round three asked for a
  request/response primitive; the honest answer is that it is right and that it
  cannot be built before §3.18 and §4.5, so it is open question 8.4.

### 5.6 The three refusals

Round two names three things it would not change. All three are accepted, and
two constrain §4 in ways worth stating.

**"BSP visibility and S−1 aggregates, because every vertex must observe the same
completed step."** Accepted; §5.4 and §4.13(g).

**"Arbitrary-ID messaging and topology mutation, because they make the
classifier expressible without artificial edges."** Accepted, and this is what
rules out neighbour-only messaging (GraphX) and edge-centric decomposition (GAS)
as *replacements* (§6). The evidence is in both look-alike implementations: a
TASK vertex sends `FETCH` to a set of DATA vertices chosen from a dataset space,
not from its edges (`7fba5d4^:test-avro/node_task.lua:60-70`, and
`examples/lookalike/app.lua:755-765`), and a DATA vertex replies to whoever
asked. Those are not graph edges.

**"The pure-Lua programmatic core and independently usable Avro/math modules,
because deployment integration shouldn't become an algorithm dependency."**
Accepted, and it is why §4.10 draws the line where it does. It is also the
argument against making the roles the source of the shared validator (open
question 8.8).

### 5.7 What round two got wrong or imprecise

Nothing in it was refuted. One place it understated its own case, one
incomplete, five line numbers off by a few, and one pointer that has since
ceased to exist:

- **Understated:** "Changing addresses can therefore reroute names without
  relocating existing tuples." Measured, it does not merely *can*: renaming one
  of three worker URIs so that it sorts differently sends 10000/10000 names to a
  different instance (§3.14).
- **Incomplete:** "Compute errors already propagate, but an exception skips
  message cleanup and returning the pooled vertex." True, and the first failure
  is indeed loud; the aftermath is a worker that cannot run another superstep
  (§3.13). Measured.
- Line numbers landing in the right function but not on the cited line:
  `aggregator.lua:145`→`:148`; `worker.lua:457`→`:464` (now `:473`);
  `roles/master.lua:332`→`:333-334`; `loader.lua:446`→`:448`/`:453`;
  `pagerank/app.lua:63`→`:66`.
- `worker.lua:297`, its evidence for "workers explicitly activate vertices", was
  correct at `8221da0` and no longer exists (§5.1).
- Its links point into a different worktree; same commit, so the lines resolved.

Its `checks` proposal costs nothing: `require('checks')` succeeds on the stock
Tarantool 3.9 binary, and the rockspec's only dependency is `lua ~> 5.1`.

## 6. Prior art

The README already claims one lineage (`README.md:9-10`, `:872-873`); round two
supplied six more. One line each on what to take and what not to.

- **Giraph** — take `compute(vertex, messages)` as two arguments, the separation
  of vertex data from computation services, and `MasterCompute` running between
  worker steps with the ability to broadcast control state. That is §4.4 and
  §4.5's `control`. Do not take Java inheritance or the `Writable` type
  machinery. Its `Vertex` also has an id given at input time and a
  `getNumEdges()`, which are §4.3 and §4.11.
- **Pregel+** — take the separation of partial (worker) and final (master)
  aggregation, which is what this library already does and does not name. Its
  request–respond extension is the model for §4.12; do not fold it into `send`,
  whose temporal meaning must stay "arrives next superstep".
- **PowerGraph / GraphLab** — GAS separates gather, apply and scatter, and can
  distribute work over adjacent edges. Worth offering as a helper. Do not
  replace arbitrary-id messaging with it: the look-alike TASK's requests are not
  graph edges (§5.6).
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
  (`pregel/worker.lua:315`). Do not promise its shared-memory atomics or dense
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
unreleased, on a branch, with every consumer inside this repository: seven
examples, one test app, and the test suite. There is no reason to carry a
compatibility shim, and a shim would cost more than it saves — most of §4 exists
because the old shapes are ambiguous, and a shim that accepts both keeps the
ambiguity forever while pretending it is gone. Round two reached the same
conclusion independently.

Recommendation: **no shim, no deprecation cycle**, and no `master:start()` alias
(§5.5).

Which changes are breaking:

- Breaking, for every app module: §4.1 (declaration), §4.2 (lifetimes), §4.3
  (explicit ids), §4.4 (compute signature), §4.5 (messages and reducers), §4.6
  (topology), §4.7 (loaders).
- Breaking for the programmatic caller: §4.8 (`start()` replaced by `run`).
- Breaking, with no caller at all: §4.11's deletion of `write_solution`.
- Additive: §4.11's accessors, the master's sink methods, §4.8's `on_progress`
  and `run:history`.
- Behaviour changes with no signature change: §4.9's remaining default, and
  §4.6's single mutation timing.

**Order of implementation.** This adopts round two's order, which differs from
the first draft's and is better. The first draft ordered by blast radius —
additive things first, the compute signature last — which optimises for keeping
the tree green. Round two orders by risk. Three measurements settle it: §3.13's
wedge, §3.14's 100% reroute and §3.19's privilege wall are not ergonomics, and
putting them behind a batch of renames means shipping a v2 whose worst defects
are the ones v1 already had.

1. **Specify the semantics first, as contract tests.** Turn the CHANGELOG's
   delivery, mutation, aggregation and cancellation traps into tests that hold
   for both queue engines and for local as well as remote delivery. The
   CHANGELOG's `Fixed` section is already a catalogue of them; what it is not is
   a specification. Two were found by an agent reading doc comments rather than
   by a test going red (`pregel-iv7`, `pregel-3e8`), and two whole beads exist
   because six *other* fixes had no test that went red when the defect was
   reintroduced (`pregel-hr3`, `pregel-9vt`).
2. **Fix identity and failure containment.** Run fencing by `(job, run, step)`,
   a stable partition manifest checked against the shard on disk, refusal of a
   duplicate master, and unwinding on a compute exception. The v1 halves —
   `pregel-2c0` (§3.13) and `pregel-atx` (§3.18) — are already in progress and
   should land first; `pregel-moi` (§3.7) belongs with them.
3. **Port the difficult consumer first.** Implement explicit ids, returned
   values, typed messages and dispatch, then port `examples/lookalike` *before*
   simplifying PageRank. At 957 lines it is the only app that uses more than a
   third of the API, and every design decision in §4.2, §4.4 and §4.5 came from
   reading it. Simplifying PageRank first would validate the easy half.
4. **Consolidate lifecycle and configuration.** Move state into run handles,
   reduce the roles to adapters over `p.define` and `m:run`, add result
   iteration and history, then hard-cut the obsolete APIs and the space format.

How each example migrates, once the order above reaches it:

- `max-value`, `wcc` — mechanical. `HERE`/`common.*` out, `obtain_name` deleted,
  ids passed at load, `compute` returns `(value, HALT)`, the aggregation call
  becomes `ctx:aggregate`. Both also lose their whole-value rebuild (§3.3).
- `sssp` — the same, plus `worker_context` becomes `configure` (it is
  configuration, not a resource) and `worker_preload` becomes
  `load = {on = 'workers', run = ...}`.
- `pagerank` — the same, and it loses a superstep and an aggregator to
  `ctx.graph.vertex_count` (§4.13(c)).
- `topology-mutation` — the one touched by §4.6: edge ids, one mutation timing,
  and its `orphan_of` marker becomes the first small user of `p.dispatch`.
- `mf` — loses its hand-written two-pass loader to §4.7's single-file mode and
  its impure `merge` to §4.8's `run:history`; `train_sse` goes back to being a
  number.
- `lookalike` — the hard one, and the one that decides whether §4.2's two-phase
  `prepare`/`open` is right. Its `app_cfg.grant_to`, its per-task DDL, its
  `await` arithmetic and its hand-written `from` field are each retired by a
  different part of §4.
- `test/apps/maxvalue.lua` — the only app-level `loader.new` in the tree, so it
  is the acceptance test for §4.7.

## 8. Open questions

Ten, each with a recommendation. The first five are the ones round three asked
be put to the maintainer explicitly.

**8.1 — Where may an app do DDL, and how does it learn the job user?**

Recommended: the two-phase `prepare`/`open` of §4.2 and §4.13(h), with
`ctx.job_user` supplied by the runtime and a `ctx:space()` helper that creates
and grants in one call. The alternative is to grant the job user DDL rights on a
namespace of its own, which would let `open` do everything job-side and would
make the partitioning available at DDL time — retiring
`examples/lookalike`'s "a space for every task on every worker". That is a
better API and a worse security posture, and it is a decision for the
maintainer: it means pregel handing an app the ability to create spaces on every
worker of the cluster at run time.

**8.2 — Do edges stay in the vertex tuple, or move to their own space?**

Recommended: stay, for v2 (§4.13(a)). Edge identity does not require the move,
and the move costs a second lookup on the hot path of every superstep. Revisit
when a real degree distribution demands it — a vertex whose edge array is
megabytes is rewritten in full on every change today, and `examples/lookalike`'s
DATA vertices are the first plausible candidate. The measurement that would
settle it does not exist yet.

**8.3 — Is a per-superstep aggregate history the runtime's job?**

Recommended: yes, as §4.8's `on_progress` plus `run:history(name)` with a
bounded default. `examples/mf` proves the need and shows what the absence costs:
a reducer that writes to a module-local table as a side effect, and a scalar
aggregate turned into a `{sse, n, superstep}` record so the superstep number can
travel inside the accumulator (`examples/mf/app.lua:171-193`). The bound matters
— an unbounded history on a job with a large aggregate value is a memory leak
with a nice name.

**8.4 — Is a request/response primitive worth building?**

Recommended: yes, but not in the first v2 (§4.12). It needs §3.18's sender and
§4.5's typed messages underneath, and both real apps' arithmetic is at least
correct today. Ship `{deliver_in = n}` on `send` in the first v2 so the delay is
stated where the message is sent rather than recomputed where it is read, and
keep `ctx:request`/`inbox:answers` as the follow-up. Pregel+'s request–respond
is the model (§6).

**8.5 — Where does the superstep number live?**

Recommended: `ctx.step`, and nowhere else (§4.13(b)). It is a property of the
run, not of a vertex; every vertex of a superstep sees the same number, and
reading it off the vertex implies otherwise. `vertex:get_superstep()` goes.

**8.6 — Do `lookalike` and `mf` get ported, or rewritten?**

Recommended: ported, and `lookalike` first (§7, step 3). It is the app §4 was
designed from, so it is the acceptance test for the design rather than a
consumer of it — and a rewrite would lose the one thing that makes it valuable,
which is that its shape was arrived at against the *current* API and every
workaround in it is evidence.

**8.7 — Where does a vertex kind live: a tuple field or a key in the value?**

Recommended: a fifth field in the `data_<name>` tuple, nullable. The value stays
entirely the app's, and the kind becomes indexable, which makes "count the TASK
vertices" a `count()` and makes §4.5's `broadcast{activate = 'DATA'}`
implementable without a full scan. The cost is a schema change a worker
restarted over an existing shard must survive — `create_spaces` uses
`if_not_exists` throughout (`pregel/worker.lua:679`) — which §3.14's partition
manifest has to handle anyway.

**8.8 — One validator for `roles_cfg` and the constructors: which direction?**

Recommended: the constructor's option table is the contract, `p.define` and the
role's spec are both derived from it, and the role adds only what is genuinely
YAML-only (`autostart`, `app`, `app_cfg`). The alternative — the role's spec as
the source — puts the cluster config in charge of a programmatic API that must
work without one, which is round two's third refusal (§5.6). Rename `pool_size`
to `batch_messages` in the same change.

**8.9 — Is the `data_<name>` space layout public interface?**

Recommended: yes for reading, explicitly, and documented in the README rather
than only in `pregel/worker.lua:8-13`. `run:vertices{}` (§4.8) is the supported
path and does not remove the need: an operator inspecting a *failed* run has no
run handle. What the layout must not be is stable across a partition change —
§3.14's manifest lives beside it, and a shard whose manifest does not match its
job must refuse to serve rather than answer for vertices it no longer owns.

**8.10 — Should the remaining v1 defects be fixed before any v2 work?**

Recommended: yes, and three of the four already are in progress. `pregel-2c0`
(§3.13) and `pregel-atx` (§3.18) are 1–2 days each and every app under
development will hit both; `pregel-moi` (§3.7) is the v1 half of §4.13(g)'s
contract. The fourth is a one-line comment fix at `pregel/worker.lua:620-625`
(§4.11), which is currently the only place in the tree that tells a reader the
wrong thing about edge deletion.
