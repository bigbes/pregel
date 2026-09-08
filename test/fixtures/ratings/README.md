# Ratings fixture

A synthetic ratings matrix from hidden rank-3 factors: 50 users, 30 items, 454
of the 1500 pairs rated, split 363 train / 91 test. Produced from the
repository root by

    tarantool tools/gen-ratings.lua test/fixtures/ratings \
        --users 50 --items 30 --rank 3 --density 0.3 --seed 7

which leaves the remaining options at their defaults — noise 0.2, test
fraction 0.2, codec `null`. The `null` codec keeps the bytes readable in any
Avro implementation and the files small enough to be worth committing.

## What is in it

`train.avro` and `test.avro` — both `record Rating { string user; string item;
double rating; }`, users `u1` … `u50`, items `i1` … `i30`. `rating` is a
multiple of 0.5 between 1 and 5.

`truth.json` — the hidden parameters, the arguments, and the counts:

    {"mu": 3.5,
     "users": 50, "items": 30, "rank": 3, "density": 0.3,
     "noise": 0.2, "seed": 7, "test_fraction": 0.2,
     "biases":  {"users": {"u1": ...}, "items": {"i1": ...}},
     "factors": {"users": {"u1": [3 doubles]}, "items": {"i1": [...]}},
     "counts":  {"ratings": 454, "train": 363, "test": 91,
                 "moved_to_train": 0}}

A rating is

    round_to_half(clip(mu + b_u + b_i + p_u . q_i + 0.2 * N(0,1), 1, 5))

so `truth.json` gives back everything but the noise draw: recomputing
`mu + b_u + b_i + p_u . q_i`, clipping and rounding lands within one noise
draw of the stored rating, and exactly on it when the generator is run with
`--noise 0`. The parameters are quantised to 12 decimal places before the
ratings are computed, so reading them back out of JSON — whose encoder keeps
14 significant digits — returns the very doubles that were used.

## The numbers to expect

The rating histogram is

    1.5:1  2:10  2.5:41  3:123  3.5:160  4:92  4.5:21  5:6

— centred on the global mean of 3.5 and spread by the biases and the factors,
with the tails thinned by the clip at 1 and 5. All 50 users and all 30 items
appear; every user and item named in `test.avro` also appears in `train.avro`,
which is the property the split guarantees. Here it held on its own
(`moved_to_train` is 0); at a lower density the generator moves the offending
test ratings back into train rather than re-drawing them, and says how many in
that count.

## Regenerating

Re-running the command above reproduces these files byte for byte on this
machine: the generator has its own Park–Miller (MINSTD) generator rather than
`math.random`, and it derives the Avro sync marker — otherwise 16 random bytes
per file — from the parameters. Across machines the bytes may differ in the
last place, because the normals are made out of libm's `log` and `cos`.
