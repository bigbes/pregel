# lookalike — distributed SGD over a shared population

Several independent binary classifiers trained at once over one population of
users, and every user then scored and ranked against every model. It is what a
look-alike audience is: you have a few thousand people who did the thing, you
have a few million who might, and you want the second list ordered by how much
they resemble the first.

This is the example that does not fit the shape of the others. There are **no
edges anywhere** — nothing here is a graph algorithm. What it uses pregel for is
the three things pregel has that a `for` loop does not:

* a **partition**: a vertex name hashes onto a worker, and the feature vectors
  live where they land;
* a **barrier**: a superstep ends when every worker has finished, so a task can
  ask a question and know the answers are all in;
* two ways to reach a vertex that is not a neighbour — a **message** addressed
  by name, and an **aggregator** every vertex can read.

It is a restoration of the 2016 `test-avro/` job (`node_master.lua`,
`node_task.lua`, `node_data.lua`, `constants.lua`) on the ported core, with the
arithmetic moved into `pregel.math`. [What changed](#what-changed-from-2016)
is at the bottom; there was a lot.

## The three kinds of vertex

| name | value | what it does |
| --- | --- | --- |
| `master` | `{roster, reports}` | starts every task, collects their reports, goes last |
| `t:<task>` | `{task, labelled, phase, weights, cuts, report}` | owns one task's labels, trains its model, calibrates it |
| `u:<vid>` | `{vid, features, scores}` | owns one user's feature vector; answers questions about it, ends up holding a score and a rank per task |

`compute` dispatches on `value.type`, which is the ordinary way to write a
Pregel job over a heterogeneous graph: there is one compute function, and the
first thing it does is ask what it is looking at.

## The pipeline, one superstep per step

    1  master  -> START to every task in the roster
    2  task    -> FETCH to every user it holds a label for, the label riding along
    3  user    -> answers with its feature vector
    4  task    -- stages the answers, splits, trains, measures held-out AUC
               -> PREDICT_CALIBRATION with the weights, to a sample of users
    5  user    -> answers with its raw score
    6  task    -- turns the sample into percentile cut points
               -> publishes {weights, cuts} through the `model` aggregator
    7  user    -- reads `model`, scores itself against every task, halts
    8  task, master -- see that no user is left waiting; publish, halt

Eight supersteps, whatever the size of the input: the fixture below and a
2000-user run both take exactly eight. What grows is the work inside them.

A question asked in superstep S is read in S+1 and answered into S+2, so each of
the two round trips costs the task a superstep of waiting. `value.await` is the
superstep the answers are due in. Without it the TRAINING branch runs on an
empty inbox the superstep after it sent its FETCHes and concludes that nobody
answered — which is a real failure mode with the same symptom, and the point of
`await` is to be able to tell them apart. The 2016 code could not: it printed
"Master didn't receive any messages, waiting one superstep" and tried again
forever.

## How a model reaches two thousand users

This is the design decision the example exists to show.

A task finishes with a weight vector that every user in the job needs. The
obvious way to deliver it is to send it: `for every user, send_message(user,
weights)`. That does not work at any interesting size, and not because of the
bandwidth. The task vertex is **one vertex on one worker**, and to address every
user by name it would have to hold a list of every user in the job — the very
thing the partition exists to avoid.

So the model is **published, not sent**. The task writes `{weights, cuts}` into
the `model` aggregator; the aggregator is merged by the master at the end of the
superstep and handed back to every worker, so one copy of the model crosses the
wire per worker per superstep instead of one per user. Every user reads it for
free.

The price is on the other side, and it is real: **an aggregator can only be read
by a vertex that is being computed**, and nothing wakes a halted vertex except a
message. So every user vertex stays awake for the whole job, polling
`get_aggregation('model')` once per superstep until it has scored every task.
That is one compute call per user per superstep — 2000 users × 8 supersteps =
16000 calls that mostly do nothing — against 2000 messages per task under the
broadcast. It is the better trade here because it scales with supersteps rather
than with tasks × users, and because the superstep count is fixed. A job with
long-running tasks and a population an order of magnitude larger would want the
opposite: let the users halt, and have the task wake the ones it cares about.

`pending` is the other half of it. The tasks and the master cannot see the users
and the users cannot address them all, so an aggregator counts the users still
waiting for some model; when it reaches zero, everyone halts. That is how the
job terminates.

### One aggregator, keyed by task

The natural spelling would be an aggregator per task — `weights_task1`,
`report_task1`. It is not available: an app module declares its aggregators as a
**static table**, and the roles register them when they apply the cluster
config. The task names come out of `labels.avro`, which is read after that. So
there is one `model` aggregator whose value is a map from task name to
`{state, weights, cuts, bucket, report}`, each task writing its own key and
reading nobody's.

The union used to merge it does not modify what it is handed. An aggregator's
accumulator *is* its `default` until the first `make_default()` runs, so a
reduce that appended in place would rewrite the default for the rest of the job.

## Score and percentile

Every user ends up holding, per task:

    scores[task] = {score = <number>, percentile = <0, 5, ... 95>}

**`score`** is the raw linear score, `w[1] + w[2..] · x` — the bias plus the dot
product, the same `pregel.math.gd.score` the training used. Higher means more
like the positive class. It is unbounded and its scale means nothing across
tasks: a score of 4 under one model and a score of 4 under another are not
comparable, because nothing normalised the weights.

**`percentile`** is what makes them comparable. During calibration the task asks
a sample of users for their raw scores and builds a `pregel.math.percentile`
counter out of the answers; the cut points at 5%, 10%, … 95% of that sample are
published with the weights. A user's percentile is the highest cut its score
reaches, so **95 means "scored at least as high as 95% of the calibration
sample" and 0 means "in the bottom 5%"**. That is a rank, it is monotone in the
score, and it is on the same scale for every task — which is what a
targeting rule downstream ("take the top 5% for this audience") actually needs.

The calibration sample is drawn from two places, because a task vertex lives on
one worker and the population does not:

* **every user it holds a label for.** Those are spread over every worker: a
  vertex's worker is a hash of its name, and the labels were drawn without
  regard to it.
* **a top-up from this worker's own users**, when the labels are fewer than
  `calibration_sample`.

The second is a sample of one shard rather than of the population. That is not a
bias here — a worker's share is chosen by `crc32` of the user's name, which has
nothing to do with that user's features, so the shard is itself a uniform sample
— but it does bound the sample: on the committed fixture, `task1` asks 93 users
rather than the 100 it wanted, because its worker holds only 33 users that are
not already labelled. A population whose names carry meaning should send to the
labelled users alone and raise `labelled_fraction` instead.

## The spaces, and the one thing that surprised this example

Each task stages its training set in a `temporary` space of its own,
`task_<name>_ds`, keyed by `{split, vid}`. The gradient descent loop draws its
minibatches from it one row at a time, which is the reason it is a space rather
than a Lua table: the training set is bounded by the labels rather than by the
population, but "bounded by the labels" is still a number the operator chose,
and a vertex value is msgpack'd into a tuple on every write.

Creating that space is only possible **while the role is applying its config**,
and that is worth knowing before writing an app module that wants storage of its
own. A compute function and a loader both run inside the
`pregel.worker.deliver` / `preload` RPC, and a `lua_call` executes with the
*caller's* privileges — which are `roles_cfg.user`'s. That user has:

* no write access to `_space`, so `box.schema.space.create` from inside compute
  raises `Write access to space '_space' is denied for user 'pregel'`;
* no access to a space pregel did not create, because `pregel.worker.grant()`
  covers pregel's own four and knows nothing about an app's;
* no write access to `_truncate` either, so `space:truncate()` is out and the
  staging space is cleared row by row.

So `worker_context()` — which the worker role calls from `apply()`, as admin —
reads `labels.avro`, creates one space per task and grants it. The user to grant
it to has to arrive through `app_cfg.grant_to`, because an app module cannot
read `roles_cfg`; keep it equal to `roles_cfg.user`. A job whose peers connect
as guest with a universe grant leaves it unset.

Every worker gets a space for every task, not only for the tasks whose vertices
it owns: which those are is a hash against the worker list, and the mpool that
computes it does not exist yet at the one moment DDL is allowed. An unused one
costs an empty space.

## Running it

    cd examples/lookalike
    LUA_PATH="$(cd ../.. && pwd)/?.lua;$(cd ../.. && pwd)/?/init.lua;$PWD/?.lua;;" tt start
    echo "require('pregel.roles.master').status()" | tt connect lookalike:master -f -
    tt stop -y

Out of the box it runs `test/fixtures/lookalike` — 200 users, 8 features, two
tasks of 60 labels. Point `app_cfg.users` and `app_cfg.labels` somewhere else
for a bigger one; `tools/gen-lookalike.lua` writes them:

    tarantool tools/gen-lookalike.lua /tmp/lookalike-big \
        --users 2000 --features 16 --tasks 3 --seed 7

## A transcript

The run below is that 2000-user set, with `app_cfg.users` and `app_cfg.labels`
pointed at `/tmp/lookalike-big` and everything else as committed.

    $ echo "require('pregel.roles.master').status()" | tt connect lookalike:master -f -
    ---
    - state: done
      name: lookalike
      superstep: 8
    ...

One report, out of the master's copy of the `model` aggregator
(`require('pregel.roles.master').get().aggregators['model']()`):

    task1: {'task': 'task1', 'state': 'ready',
            'features': 16,
            'labelled': 600, 'answered': 600,
            'train_size': 450, 'test_size': 150, 'scored': 150,
            'iterations': 300, 'converged': false, 'loss': 0.26513915076392,
            'auc': 0.97763081112624,
            'calibration_sent': 100, 'calibration_size': 100}

`auc` is the area under the ROC curve of the raw score on the 150 rows the model
never saw — `scored` says so, and it is the test half of the split rather than
the train half. The other two tasks came out at 0.97724 and 0.97878, and all
three recovered every one of the 17 weight signs in `truth.json`:

    task1: 17/17 signs, auc 0.97763
    task2: 17/17 signs, auc 0.97724
    task3: 17/17 signs, auc 0.97878

One user vertex, read off the worker that owns it
(`box.space.data_lookalike:get('u:u1')`):

    - worker: worker1
      vertices: 691
      sample:
        name: 'u:u1'
        type: data
        vid: u1
        features: [1.1540589659821, 0.82141370161077, -1.1526840367872, ...]
        scores:
          task1: {'score': 0.38234599731942,  'percentile': 70}
          task2: {'score': 6.9122830019369,   'percentile': 95}
          task3: {'score': -1.2816211899054,  'percentile': 35}

`u1` is in the top 5% of the calibration sample for `task2` and around the
middle for `task3`: three independent models, one feature vector, three ranks on
one scale. The other two workers held 661 and 652 of the 2003 vertices.

### On `converged: false`

`iterations: 300` is `max_iter`, so the convergence test never fired. That is
deliberate, and the reason is worth stating because the default is different.

`pregel.math.gd` stops when one iteration moves the exponentially averaged loss
by less than `epsilon`, and its default `epsilon` is `1e-4` — the 2016
`gd.loss.convergence.factor`. At `batch_size` 16 the batch loss is noisy enough
that two consecutive iterations land within `1e-4` of each other by luck: with
`epsilon: 0.0001` this exact run stopped `task1` after **11 iterations** at AUC
0.920, against 0.978 for the same task trained out. So this example sets
`epsilon: 0.00001` and lets `max_iter` be the real bound. A larger `batch_size`
would be the other way to fix it — the noise is the average of the batch.

## What changed from 2016

The restoration is of the *shape*: the three vertex types, the message
vocabulary, the phase machine, the per-task report. Almost none of the
arithmetic survived contact with a test. `pregel/math/{gd,auc,percentile}.lua`
carry the accounts of what was wrong inside them; these are the ones in the job
logic:

* **The AUC was measured on the training set.** `applyModelToTestDataSets`
  selected the rows whose split was `train`. Here it is the test half, which is
  what the split is for.
* **…and on the wrong vector.** That same loop called
  `predictCalibrated(parameters, features, cbp)` against a method declared
  `predictCalibrated(self, param, calibrationBucketPercents)` — one argument too
  many, so the *features* arrived where the calibration percentage was expected
  and the method scored the task's own weight vector against itself. Every user
  in the loop got the same number.
* **The reported train and test sizes were not sizes.**
  `computeTrainTestRecordCounts` counted how many distinct rounded target values
  had at least one row on each side — 2, for a binary task, whatever the number
  of rows.
* **The percentile buckets were off by one at both ends.** `calibrate` asked for
  the `(p+1) * bucket`-th percentile for `p` in `1..n`, so the lowest cut was at
  2 × bucket and the highest at 100 — the bottom two buckets were one bucket and
  the top one was empty, because no score exceeds the maximum. And
  `predictCalibrated` numbered the buckets from 1 rather than 0, so bucket 0 was
  unreachable.
* **The rank was random inside its bucket.** `predictCalibrated` returned
  `math.random(0, step) + i * step`, so the same user's rank differed between two
  runs over the same data. The rank here is the bucket, and nothing else.
* **The train/test split could hand the held-out set one class.** It was a coin
  flip per row; both of the fixture's tasks lean, and the AUC of a single-class
  sample is not a number. The split here is stratified — the fraction comes out
  of each class separately.
* **The gradient was a sum where the loss was a mean.** Fixed in
  `pregel.math.gd`, which averages both; see its module comment.
* **The batch loops could not terminate on an empty bucket.** `while batchSize >
  0 do ... :take(batchSize):all(fn) end` never decrements when the iterator is
  empty, which is what an unrepresented target value produces.
* **`node_master` created the task vertices** with `add_vertex`, because it was
  the only vertex that had read the file naming the tasks. The loader reads
  `labels.avro` and creates them directly, so the master's remaining jobs are
  starting them and collecting their reports.
* **The phase lived on the worker context**, in a map keyed by task name. It
  lives in the task vertex's own value here: one worker may own several task
  vertices, and a phase belongs to a task rather than to the worker that happens
  to hold it.
* **Nothing was reproducible.** `math.random` chose the split, the batch order
  and the calibration sample. All three go through `crc32` of the vertex name
  here, and training starts from the zero vector rather than
  `gd:initialize()`'s random draw — which is what lets the test assert a number
  instead of a range.

## Configuration

Everything below lives in `roles_cfg.app_cfg` and is read by the app module.
Both roles need it: the workers because they run the loader and every
hyperparameter is read through the worker context, the master because it is the
one place the two paths are written down.

| key | default | what it is |
| --- | --- | --- |
| `users` | — | `users.avro`, relative to this directory |
| `labels` | — | `labels.avro`, relative to this directory |
| `grant_to` | none | the user `roles_cfg.user` names; see the note on spaces |
| `test_fraction` | 0.25 | held out of training, per class |
| `min_labels` | 20 | fewer than this and the task reports `failed` |
| `learning_rate` | 0.1 | `c` in `c / (1 + k * iteration)` |
| `learning_decay` | 0.01 | `k` in the same |
| `lambda` | 0.001 | L2 penalty on the weights, bias excluded |
| `batch_size` | 16 | samples per gradient step |
| `max_iter` | 300 | gradient steps at most |
| `alpha` | 0.2 | loss averaging factor |
| `epsilon` | 1e-5 | convergence threshold on the averaged loss |
| `calibration_sample` | 100 | users asked for a raw score |
| `calibration_bucket` | 5 | percent per bucket of the rank scale |

A task that cannot be trained does not take the job down. Fewer than
`min_labels` labels, or fewer than that many users answering its FETCH, and it
publishes `{state = 'failed', report = {reason = ...}}` instead of a model; the
other tasks finish, and the users score what they can and stop waiting for what
they cannot. `test/examples/lookalike_test.lua` runs exactly that case.

## The test

`test/examples/lookalike_test.lua` runs this app module through
`luatest.cluster` on the committed fixture and asks two independent questions of
each task:

* the **AUC on the held-out split**, which the job measures itself — ≥ 0.9,
  measured 0.955 and 0.945;
* the **sign agreement between the learned weights and the hidden ones** in
  `truth.json`, which the job knows nothing about — ≥ 0.8, measured 9 of 9 for
  both tasks. A model that had overfitted its way to a good AUC would still fail
  here.

Plus: every user holds a score and a percentile per task and the score is the
model applied to that user's own features (recomputed outside the job); the rank
is monotone in the score; the reports carry the sizes; the population is spread
over all three workers; and the starved-task case above.

Two mutations were run against it, and both turn it red rather than merely
changing a number:

    max_iter = 0                 task1 AUC: Assertion failed: 0.5 >= 0.9
    hinge gradient sign flipped  task1 AUC: Assertion failed: 0.13636 >= 0.9
                                 task1 sign agreement: 0.11111 >= 0.8
