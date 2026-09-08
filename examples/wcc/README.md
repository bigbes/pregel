# wcc — weakly connected components

Which vertices are reachable from which, ignoring edge direction.

Every vertex starts labelled with its own name and keeps the smallest label it
is offered, passing on anything that improved. When the graph goes quiet — ten
supersteps on the graph below — two vertices carry the same label exactly when
they are in the same component, and that label is the smallest name in it. So
the label is both the answer and the component's identity; nothing has to be
collected anywhere to count the components.

The same shape as `examples/max-value`, over a smaller-is-better order on
strings instead of a larger-is-better one on numbers. What is different is the
graph it needs.

## Direction is the whole trick

Pregel only ever walks out-edges: a message travels from a vertex to its
out-neighbours and never back. "Weakly connected" means direction is ignored,
so the **graph** has to be symmetric — every edge present in both directions —
and the algorithm need not be. Run this on a one-way graph and it answers with
something else entirely: over `1 → 2 → 3` the label `1` never reaches `2` or
`3`, and three vertices come back as three components.

That is why `config.yaml` names the `-bi` fixture, and it is the one thing to
get right before running this on a graph of your own.
`test/examples/wcc_test.lua` pins it, with the same nine vertices in both forms.

## Files

| file | what it is |
| --- | --- |
| `config.yaml` | the cluster config: one master, three workers, the credentials role and the user carrying it, the job's spaces granted on the worker replicasets, `roles_cfg` |
| `instances.yml` | the four instance names, for `tt` |
| `tt.yaml` | makes this directory a `tt` application |
| `app.lua` | `compute`, `combiner`, `obtain_name`, `master_preload` |

There are no aggregators here: everything this algorithm needs is in a vertex
or in a message.

The graph is `test/fixtures/graphs/soc-Epinions-custom-bi.txt` — the same
75879-vertex trust network as `examples/max-value`, with all 1017674 edges
spelled out in both directions. Swap in
`../../test/fixtures/graphs/small/components3.txt` for nine vertices in three
components. Vertices are named by the file's own id, because the fixture has
4684 duplicate names and merging two vertices would merge their components with
them — for a connectivity algorithm that is not a small error but the whole
answer.

## Run it

    cd examples/wcc
    LUA_PATH="$(cd ../.. && pwd)/?.lua;$(cd ../.. && pwd)/?/init.lua;$PWD/?.lua;;" tt start

    • Starting an instance [wcc:worker1]...
    • Starting an instance [wcc:worker2]...
    • Starting an instance [wcc:worker3]...
    • Starting an instance [wcc:master]...

`tt start` returns before the pid files are written, so a `tt status` run in the
same breath as it prints `NOT RUNNING` for all four. Give it a second.

    tt status

     INSTANCE     STATUS   PID    MODE  CONFIG  BOX      UPSTREAM
     wcc:master   RUNNING  11766  RW    ready   running  --
     wcc:worker1  RUNNING  11763  RW    ready   running  --
     wcc:worker2  RUNNING  11764  RW    ready   running  --
     wcc:worker3  RUNNING  11765  RW    ready   running  --

`tt connect wcc:master` opens a console; every console line below is written as
a pipe instead, so it can be pasted as it stands.

    echo "require('pregel.roles.master').status()" | tt connect wcc:master -f -
    ---
    - state: done
      name: wcc
      superstep: 10
    ...

Loading the million-edge file takes about nine tenths of a second and the ten
supersteps about eight and a half.

## Read the results

    echo "box.space.data_wcc:pairs():take(3):map(function(t) return t.value end):totable()" | tt connect wcc:worker1 -f -
    ---
    - - {'name': 'Rita Swafford', 'label': '0', 'id': 100}
      - {'name': 'Ricky Mayhew', 'label': '0', 'id': 10008}
      - {'name': 'Shirley Flowers', 'label': '0', 'id': 10015}
    ...

Counting the components means counting distinct labels, which each worker can
do over its own shard. Which shard is which is settled before anything runs: a
vertex name is hashed onto one of the job's workers — the instances the cluster
config gives the worker role to — and every instance sorts that list by the URI
string first, so bucket 1 is `worker1`
(`127.0.0.1:3302`), bucket 2 is `worker2` (`:3303`) and bucket 3 is `worker3`
(`:3304`) — the same three numbers below on every machine and after every
restart. This is the same graph `examples/max-value` runs on, and the shard
sizes match its transcript for exactly that reason.

    echo "local n, c = {}, 0 for _, t in box.space.data_wcc:pairs() do if n[t.value.label] == nil then c = c + 1 end n[t.value.label] = (n[t.value.label] or 0) + 1 end return {distinct_labels = c, in_component_0 = n['0'], vertices = box.space.data_wcc:len()}" | tt connect wcc:worker1 -f -
    ---
    - vertices: 25459
      distinct_labels: 2
      in_component_0: 25458
    ...

    ... | tt connect wcc:worker2 -f -
    ---
    - vertices: 25564
      distinct_labels: 1
      in_component_0: 25564
    ...

    ... | tt connect wcc:worker3 -f -
    ---
    - vertices: 24856
      distinct_labels: 2
      in_component_0: 24855
    ...

One component of 75877 vertices — labelled `0`, so vertex 0 is the
lexicographically smallest name in it — plus two isolated vertices, one on
`worker1` and one on `worker3`. The three shards agree on the label without
ever comparing notes, which is the property worth noticing: a vertex only knows
what its neighbours told it.

## Stop it

    tt stop -y

`wal.mode` is `none`, so nothing survives; `rm -rf var` clears the working
directories as well.

## The test

`test/examples/wcc_test.lua` runs this app module through `luatest.cluster` on
nine vertices in three components, reads the answer back both per-vertex and
grouped by label, and then runs the same graph with one direction of each edge
removed to show what symmetry is buying:

    make test
