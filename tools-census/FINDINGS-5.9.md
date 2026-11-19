# sbm on sparsemap 5.9.0: re-test of where it could replace Bitmapset

Measured 2026-11-18 on an EC2 c7i.4xlarge (Xeon 8488C, Fedora 44, gcc
16.2.1), release builds, server pinned to one core.  Base is sbm at
sparsemap 5.8.5 (`wip/bms-census-58bench`); new is sbm at 5.9.0
(`wip/bms-census-59`).  The two trees differ only in sbm.c and sbm.h.
Three rounds, alternating base and new; every function reports the minimum
of its own repetitions, and the per-round speedups agree to within a few
percent (see `results-ec2-fedora44-59/cmp.txt`).

## 1. What 5.9.0 bought sbm

On maps of 1e5 members in 1e7 to 1e8 bits, speedup of new over base:

| operation | speedup |
|---|---|
| contains, random | 0.94-2.7x |
| contains, ascending cursor | 1.6-2.9x |
| next_member from a random position | 1.4-3.8x |
| full iteration | 1.8-2.7x |
| full reverse iteration (cursor) | 3.1-51x |
| rank | 6.9-19x |
| select | 4.9-20x |
| locator contains | 3.9-33x |
| locator rank | 25-230x |
| locator select | 62-307x |
| remove_range (4096 bits) | 8.5-74x |
| is_subset(m, m) | 0.95-1.04x |

The large gains are all on paths that used to walk the chunk list or the
bits, which Bitmapset never does.  Two regressions appear on the runs-of-64
map: contains at 0.94x and is_subset at 0.95x.  Locally (werk) the same
shape measured 1.15x and 1.05x faster, so this is within code-layout noise
for one build, not a measured regression.

## 2. Bitmapset shapes: unchanged verdict

The census (`FINDINGS.md`) found that the only Bitmapsets sbm encodes much
smaller are planner relid sets in queries over thousands of partitions.
On those shapes, `test_sbm_bms_shapes(10000, ...)`, sbm 5.9.0 against
Bitmapset:

| shape | union | equal | is_subset | overlap | is_member | iterate | copy | hash |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| tiny {1,2,3} | 40x | 3.6x | 1.8x | 1.6x | 1.1x | 1.6x | 2.3x | 11x |
| child {9997} | 3.3x | 2.1x | 2.1x | 2.1x | 4.8x | 1.2x | 1.7x | 0.14x |
| child join | 6.0x | 6.0x | 5.9x | 103x | 7.1x | 1.5x | 1.0x | 0.22x |
| all 10k parts | 5.2x | 66x | 2.6x | 2.5x | 3.5x | 2.1x | 1.9x | 0.19x |

(sbm time / Bitmapset time; above 1 means sbm is slower.)  Against 5.8.5
the port made is_member 2.4-3.2x faster and equal/is_subset/overlap about
1.1-1.2x faster, but union, copy and hash did not move.  sbm still loses
every planner-hot operation by 2x to 100x, so the memory saving on
partitioned queries still costs more CPU than it saves.  Nothing here
changes the earlier conclusion: a first-word offset in Bitmapset is the
cheaper way to remove the leading zero words.

## 3. IntegerSet (GiST VACUUM): sbm now ties or wins on probes, not on cost

Microbenchmark, 1e5 members, GiST VACUUM's pattern:

| shape | structure | add ns | iterate ns | probe ns | bytes |
|---|---|---:|---:|---:|---:|
| random 1% of 10M | IntegerSet | 7.6 | 3.6 | 101 | 252,984 |
| | sbm + locator | 32.0 | 39.7 | 83 | 1,115,224 |
| random 50% of 200k | IntegerSet | 5.5 | 2.9 | 94 | 105,064 |
| | sbm + locator | 19.7 | 22.4 | 43 | 50,240 |
| runs of 64, 5% of 2M | IntegerSet | 4.1 | 3.2 | 85 | 81,896 |
| | sbm + locator | 19.7 | 23.1 | 61 | 89,480 |

With the 5.9 locator, sbm probes are now faster than IntegerSet's
(5.8.5: 1,286 to 243 ns, slower).  Adds are 2.2x faster than in 5.8.5
but still 4-5x slower than IntegerSet, and iteration is still 7-11x
slower.  sbm is smaller only for dense sets.

End to end, `gist_vacuum_ab.sh`: a 4M-point GiST index, 98% of rows
deleted so 23,936 leaf pages empty, VACUUM run six times per mode,
alternating:

| mode | VACUUM ms (median, range) | page-set ops ms | set bytes |
|---|---|---|---|
| IntegerSet | 433.5 (431-437) | 2.56 | 26,616 |
| sbm | 433.0 (432-434) | 2.44 | 5,120 |

The page sets cost 0.6% of the VACUUM either way, and the difference
between the two (0.1 ms) is inside the run-to-run spread.  sbm holds the
same sets in a fifth of the memory (26 kB to 5 kB), which nobody needs at
these sizes.  Verdict: no measurable benefit, so not worth a patch.

## Where this leaves sbm

5.9.0 made sbm much faster at what it is good at (rank, select, ordered
and reverse iteration, range operations, and directory lookups over large
sparse sets) and a little faster on point lookups.  It did not change the
cost of the small-set constructors and comparators that dominate the
planner, which is why sbm still cannot replace Bitmapset.  The one core
structure where sbm now matches the incumbent per operation, IntegerSet,
spends too little time in its set to make a difference end to end.  sbm's
home remains an index access method that stores and queries large sparse
sets (bark).
