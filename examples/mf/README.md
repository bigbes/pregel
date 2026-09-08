# mf — a recommender by matrix factorisation

Learn a rating from the ratings around it. Every user and every item gets a
short vector of latent factors and an offset of its own, fitted by stochastic
gradient descent so that

    r_ui  ≈  mu + b_u + b_i + <p_u, q_i>

— a global mean, a bias for the user, a bias for the item, and the dot product
of the two vectors. What the vectors mean is nobody's decision: they are
whatever three (or five, or fifty) numbers per side happen to reconstruct the
ratings best. Held-out ratings are then predicted with the same formula.

This is the one example here that **fits a model** rather than computing a
property of the graph, and it is the reason `pregel.math.mf` exists.
`pregel/math/mf.lua` owns the arithmetic — one SGD step, the prediction, the
RMSE — and this directory owns the schedule: who holds which parameter, when it
moves, and what has to travel for it to move.

## The graph

Bipartite. `u:<user>` and `i:<item>` are vertices; a **training rating is two
edges**, `u:7 → i:3` and `i:3 → u:7`, both carrying the rating as the edge
value. Pregel only ever walks out-edges, so a rating stored one way round would
teach the user about the item and never the item about the user — the point
`examples/wcc` makes about the graph having to be symmetric, here forced by the
algorithm rather than by the question.

A vertex value is

    {name = 'u:7', kind = 'user', p = {0.03, -0.07, 0.01}, b = -0.14}

`p` is that vertex's latent vector — `p_u` on a user, `q_i` on an item, one
field because the two sides are the same shape and the same arithmetic — and
`b` is its offset. Nothing else is stored anywhere: the model *is* the vertices.

## The schedule

| superstep | what happens |
| --- | --- |
| 1 | every vertex draws its own `p`, sets `b = 0`, and sends `{p, b}` to every neighbour. `mu` is being counted and is not readable yet |
| k > 1 | epoch k−1: read one `{p, b}` per neighbour, take one SGD step per (neighbour, rating) pair, send the updated `{p, b}` on |
| epochs + 1 | the last epoch; nothing left to send, halt |

So the job runs `epochs + 1` supersteps and stops by counting them, as
`examples/pagerank` does — SGD converges rather than terminates.

The starting vectors are drawn from a Park–Miller stream seeded with a hash of
the **vertex's own name**, not from `math.random`. Two vectors that start equal
never separate and a pair that starts at zero never moves at all, so they have
to be random; seeding from the name is what makes them the same random on every
run and under any number of workers, which is what lets a test assert a number.

## Each vertex updates its own half only

`mf.sgd_step` computes the new user vector *and* the new item vector from one
rating. This app throws one of them away every time: a user vertex keeps the
new `p` and drops the item's half, and the item makes that move itself when it
reads its own copy of the same message.

That is the standard Pregel/parameter-server approximation, and it buys two
things:

* every parameter has exactly one writer, so there is no conflict to resolve
  and no rule about whose copy wins;
* both sides step from the **same** pair of vectors — the ones exchanged at the
  end of the previous superstep — so the two half-updates together are one
  joint gradient step evaluated at that point, not two chained ones.

What it is not is sequential SGD. Within an epoch a vertex sees its neighbours
as they were an epoch ago, which makes this closer to one mini-batch step per
vertex than to Koren's rating-at-a-time loop: it converges the same way and
more slowly per epoch, and that is the price of running the ratings in
parallel. Within one superstep a vertex *does* chain its own updates — the
second message it reads steps from the `p` the first one produced — because
that is one writer walking its own ratings, not two writers racing.

One consequence worth knowing before reading a transcript: the order a vertex
reads its messages in is the order they arrived, so two runs of the same
configuration end at slightly different models. Three runs of the fixture below
scored 0.4773, 0.4777 and 0.4781.

## What is global, and how it gets there

Only `mu`, and no vertex can see beyond its own neighbourhood, so it arrives
through aggregators:

| aggregator | what it carries |
| --- | --- |
| `rating_sum`, `rating_count` | the training ratings, summed over **user** vertices only — both directions of every rating are in the graph, so counting both sides would count each rating twice |
| `train_sse` | the epoch's squared error, back out to the master |

Both halves of `mu` are contributed on **every** superstep rather than once.
The master resets an aggregator before the workers report into it, so a value
contributed in superstep 1 alone is readable in superstep 2 and gone by
superstep 3 — the same reason `examples/pagerank` re-votes for `count` every
time.

`train_sse` goes the other way, and it is the one thing here that does not fit
the aggregator model cleanly. The master keeps one value per aggregator and
resets it between supersteps, so by the time a job is `done` every epoch but
the last has been overwritten. `merge` — which runs on the master, once per
worker per superstep — is the only hook left to build a history in, and it is
handed two accumulators and nothing else. That is why `train_sse` is a table
rather than a number: the superstep an accumulator belongs to has to travel
inside it. `app.train_history()` reads the result back.

## Files

| file | what it is |
| --- | --- |
| `config.yaml` | the cluster config: one master, three workers, credentials, `roles_cfg` |
| `instances.yml` | the four instance names, for `tt` |
| `tt.yaml` | makes this directory a `tt` application |
| `app.lua` | `compute`, `obtain_name`, `worker_context`, `aggregators`, `master_preload`, `train_history` |
| `evaluate.lua` | not part of the job: reads the fitted model off the workers and scores it |

`app_cfg` carries `train` (the Avro OCF of training ratings), `test` (read by
`evaluate.lua`, not by the job), `rank`, `epochs`, `lr`, `decay` and `lambda`.
Paths are relative to this directory; an absolute one is used as it stands.

### The loader

`pregel.loader.avro_files` is no use here. It wants a vertex file and an edge
file, and a ratings dataset is one file that is both — every record names two
vertices and one edge, and the edge has to exist in both directions. So
`app.master_preload` builds a loader with `loader.new` and reads `train.avro`
through `pregel.avro.ocf` itself, in **two passes**: the first declares each
user and item the first time it appears, the second emits both directions of
every rating. Two passes rather than one because what is then held in memory is
the set of vertex names and not the ratings.

## Run it

    cd examples/mf
    LUA_PATH="$(cd ../.. && pwd)/?.lua;$(cd ../.. && pwd)/?/init.lua;$PWD/?.lua;;" tt start

    • Starting an instance [mf:master]...
    • Starting an instance [mf:worker1]...
    • Starting an instance [mf:worker2]...
    • Starting an instance [mf:worker3]...

`tt start` returns before the pid files are written, so a `tt status` run in the
same breath as it prints `NOT RUNNING` for all four. Give it a second.

    tt status

     INSTANCE    STATUS   PID    MODE  CONFIG  BOX      UPSTREAM
     mf:master   RUNNING  37551  RW    ready   running  --
     mf:worker1  RUNNING  37552  RW    ready   running  --
     mf:worker2  RUNNING  37553  RW    ready   running  --
     mf:worker3  RUNNING  37554  RW    ready   running  --

The committed `config.yaml` trains on `test/fixtures/ratings` — 50 users, 30
items, 363 training and 91 held-out ratings — for 30 epochs at rank 3. It is
over before `tt status` has finished printing: 2 ms to load and 130 ms for the
31 supersteps.

    echo "require('pregel.roles.master').status()" | tt connect mf:master -f -
    ---
    - state: done
      name: mf
      superstep: 31
    ...

## Score it

`evaluate.lua` runs on the **master**, which owns no graph — the vertices are
on the workers, in each one's `data_mf` space, and the master already holds a
net.box connection to every one of them. Those connections are what it walks.
Nothing new has to be granted: the worker role gives `roles_cfg.user` read and
write on the spaces it creates, so a plain `conn.space.data_mf:select()` runs on
exactly the credentials the job already runs on, and the `lua_call` list in
`config.yaml` stays the four names every example has.

    echo "require('examples.mf.evaluate').evaluate{test = '../../test/fixtures/ratings/test.avro'}" | tt connect mf:master -f -
    ---
    - mu: 3.4132231404959
      ratings: 91
      items: 30
      users: 50
      rmse: 0.47777244308092
      missing: 0
    ...

0.478 on ratings the job never saw. The number to compare it against is 0.561,
which is what answering every held-out rating with `mu` scores — that is the
floor a recommender that has learnt nothing lands on, and the margin is the
whole of what the factors bought.

`missing` counts test ratings naming a user or an item the model has never
seen. Factorisation cannot answer those at all, so they are left out of the
RMSE rather than scored against `mu`; `tools/gen-ratings.lua` guarantees there
are none, and a non-zero here means the split stopped guaranteeing it.

The training error per epoch comes off the master too. Under `tt` the app
module's name is what `roles_cfg.app` says — `app`, resolved through
`$PWD/?.lua` — where the test suite loads the same file as `examples.mf.app`:

    echo "local h = require('app').train_history() local rv = {} for _, e in ipairs(h) do if e.epoch % 5 == 0 or e.epoch == 1 then table.insert(rv, {epoch = e.epoch, count = e.count, rmse = e.rmse}) end end return rv" | tt connect mf:master -f -
    ---
    - - {'count': 363, 'rmse': 0.5647533400965, 'epoch': 1}
      - {'count': 363, 'rmse': 0.39096157278837, 'epoch': 5}
      - {'count': 363, 'rmse': 0.37754998302149, 'epoch': 10}
      - {'count': 363, 'rmse': 0.37388727020105, 'epoch': 15}
      - {'count': 363, 'rmse': 0.3702125270194, 'epoch': 20}
      - {'count': 363, 'rmse': 0.36586773244599, 'epoch': 25}
      - {'count': 363, 'rmse': 0.35963134921066, 'epoch': 30}
    ...

`count` is 363 in every row, and that is worth more than it looks: it is the
number of training ratings, counted once each. Both directions of every rating
are in the graph, so a version that accumulated the error on the item side too
would report 726 here — and a train RMSE that is quietly the same number, since
the same errors would be averaged over twice as many of them.

The shards, for the record. A vertex name is hashed onto one of the `workers`
entries and every instance sorts that list by URI first, so bucket 1 is always
`worker1` (`127.0.0.1:3302`):

    echo "local u, i = 0, 0 for _, t in box.space.data_mf:pairs() do if t.value.kind == 'user' then u = u + 1 else i = i + 1 end end return {vertices = box.space.data_mf:len(), users = u, items = i}" | tt connect mf:worker1 -f -
    ---
    - vertices: 38
      items: 10
      users: 28
    ...

    ... | tt connect mf:worker2 -f -    → vertices: 21, items: 10, users: 11
    ... | tt connect mf:worker3 -f -    → vertices: 21, items: 10, users: 11

    tt stop -y

`wal.mode` is `none`, so nothing survives; `rm -rf var` clears the working
directories too.

## On a bigger matrix

500 users, 200 items, rank 5, about a tenth of the pairs rated:

    tarantool tools/gen-ratings.lua /tmp/mf-500x200 \
        --users 500 --items 200 --rank 5 --density 0.1 --seed 11

    /tmp/mf-500x200/train.avro: 8126 ratings
    /tmp/mf-500x200/test.avro: 2032 ratings (0 moved back to train for a user or item train.avro would not have held)
    /tmp/mf-500x200/truth.json: mu 3.5, 500 user and 200 item factors of rank 5
    density: 10158 of 100000 pairs (0.1016), seed: 11, noise: 0.2, codec: null

Point `app_cfg` at it — five lines of `config.yaml`, and the anchor carries them
to the workers:

    train: '/tmp/mf-500x200/train.avro'
    test: '/tmp/mf-500x200/test.avro'
    rank: 5
    epochs: 60
    lr: 0.05
    decay: 0.99

`rank: 5` because the generator hid five factors per side; more epochs because a
bigger matrix has more to fit, and a slower `decay` so the learning rate is
still worth something at epoch 60. Then start it exactly as above.

    echo "require('pregel.roles.master').status()" | tt connect mf:master -f -
    ---
    - state: done
      name: mf
      superstep: 61
    ...

    echo "require('examples.mf.evaluate').evaluate{test = '/tmp/mf-500x200/test.avro'}" | tt connect mf:master -f -
    ---
    - mu: 3.5326729017967
      ratings: 2032
      items: 200
      users: 500
      rmse: 0.49984600762435
      missing: 0
    ...

0.500 against 0.756 for predicting the mean — a wider margin than on the small
fixture, because 8126 ratings support ten factors per vertex far better than
363 support six.

    echo "local h = require('app').train_history() local rv = {} for _, e in ipairs(h) do if e.epoch == 1 or e.epoch % 15 == 0 then table.insert(rv, {epoch = e.epoch, count = e.count, rmse = e.rmse}) end end return rv" | tt connect mf:master -f -
    ---
    - - {'count': 8126, 'rmse': 0.71512897589337, 'epoch': 1}
      - {'count': 8126, 'rmse': 0.5095299471584, 'epoch': 15}
      - {'count': 8126, 'rmse': 0.35025086879895, 'epoch': 30}
      - {'count': 8126, 'rmse': 0.31077394080523, 'epoch': 45}
      - {'count': 8126, 'rmse': 0.28937200618377, 'epoch': 60}
    ...

Loading the 8126 ratings — 700 vertices and 16252 edges — takes 54 ms, and the
61 supersteps 2.5 s. The graph spreads evenly over the three workers:

    ... | tt connect mf:worker1 -f -   → vertices: 250, users: 187, items: 63, edges: 5531
    ... | tt connect mf:worker2 -f -   → vertices: 219, users: 149, items: 70, edges: 5309
    ... | tt connect mf:worker3 -f -   → vertices: 231, users: 164, items: 67, edges: 5412

## The test

`test/examples/mf_test.lua` runs this app module through `luatest.cluster` on
the committed fixture and checks what the algorithm is *for* rather than
re-deriving it: the held-out RMSE beats the mean-only baseline by a wide
margin, the training error falls in all thirty epochs, every training rating is
counted exactly once per epoch, the vectors have the configured rank, and both
directions of every rating are in the graph. The held-out score is read by
running `evaluate.lua` on the master, so the test covers the half of the
example an operator actually uses.

    make test
