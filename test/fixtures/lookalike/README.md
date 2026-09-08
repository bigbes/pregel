# Look-alike fixture

A synthetic look-alike dataset: 200 users with 8 dense features each, and two
tasks whose labels come from a hidden linear model. Produced from the
repository root by

    tarantool tools/gen-lookalike.lua test/fixtures/lookalike \
        --users 200 --features 8 --tasks 2 --seed 7

which leaves the remaining options at their defaults — labelled fraction 0.3,
noise 0.5, codec `null`. The `null` codec keeps the bytes readable in any Avro
implementation and the files small enough to be worth committing.

## What is in it

`users.avro` — `record User { string vid; array<double> features; }`, one
record per user, `u1` … `u200`, features drawn from N(0,1).

`labels.avro` — `record Label { string task; string vid; int target; }`, 120
records: 60 per task, a random 30% of the users drawn independently for each of
`task1` and `task2`. `target` is +1 or -1. The unlabelled 140 users per task
are the ones a look-alike model has to score.

`truth.json` — the hidden weight vectors and the parameters that produced them:

    {"tasks": {"task1": {"weights": [...9 doubles...]}, "task2": {...}},
     "users": 200, "features": 8, "labelled_fraction": 0.3,
     "noise": 0.5, "seed": 7}

`weights[1]` is the bias and `weights[2..9]` multiply the eight features, so a
label is

    target = sign(w[1] + w[2..] . x + 0.5 * N(0,1))

The weights are quantised to 12 decimal places before the labels are computed,
so reading them back out of JSON — whose encoder keeps 14 significant digits —
returns the very doubles that were used, and a test can recompute a label
exactly rather than approximately.

## The numbers to expect

`task1` has 17 of its 60 labels positive and `task2` has 42, so the two tasks
lean opposite ways and a model that learns nothing but the majority class
cannot look good on both. Against the noiseless sign of the score, noise 0.5
flips 2 of task1's labels and 7 of task2's: that is the Bayes error a learner
on this fixture cannot get below, and it is deliberately non-zero so the
example is not scored against a target it could reach exactly.

Pass `--noise 0` to get labels that _are_ the sign of the score, which is what
the generator's own test uses to check that `truth.json` reproduces
`labels.avro`.

## Regenerating

Re-running the command above reproduces these files byte for byte on this
machine: the generator has its own Park–Miller (MINSTD) generator rather than
`math.random`, and it derives the Avro sync marker — otherwise 16 random bytes
per file — from the parameters. Across machines the bytes may differ in the
last place, because the normals are made out of libm's `log` and `cos`.
