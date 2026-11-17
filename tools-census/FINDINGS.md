# Where sbm could beat Bitmapset: findings

Measured 2026-11-17 on branch `wip/bms-census` (on top of `bits`), first on
werk and then reproduced on an EC2 c7i.4xlarge (Xeon 8488C, Fedora 44,
gcc 16.2.1).  Raw data from the EC2 run is in `results-ec2-fedora44/`.

## Short answer

Nowhere in core PostgreSQL is sbm a better fit than Bitmapset, and it is
not a better fit than IntegerSet either.  The one place where Bitmapsets
waste real memory (queries over thousands of partitions) is fixed better
by giving Bitmapset a first-word offset than by switching to sbm.

## 1. What Bitmapsets look like (census)

`bms_census.c`, linked with `--wrap` around every `bms_*` entry point,
recorded each call made from outside bitmapset.c, by call site.  For each
returned set it counted the bytes allocated by: the real Bitmapset; a
Bitmapset that stores the index of its first nonzero word ("off"); and an
8-byte header plus the sbm encoding ("sbm").

| workload | set-producing calls | avg words | off / bms | sbm / bms |
|---|---:|---:|---:|---:|
| core regression suite | 7.66M | 0.68 | 1.66x | 1.66x |
| pgbench (select-only + tpcb) | 1.40M | 0.77 | 1.67x | 1.67x |
| 12-way join planning | 327k | 0.50 | 1.67x | 1.67x |
| 1500-column table | 530 | 0.62 | 1.07x | 0.84x |
| partitioning, 1k and 10k partitions | 859k | 15.2 | 0.22x | 0.21x |

94% of regression-suite calls come from sites that never produce more
than one word, and pgbench's and the join test's are all one word.  For
those sets either alternative is larger: 40 bytes instead of 24 once
aset.c rounds the chunk.

Single queries on a 10,000-partition table:

| query | planner memory | bms | off | sbm |
|---|---:|---:|---:|---:|
| `count(*)` (no pruning) | 56 MB | 9.9 MB | 0.82 MB | 1.12 MB |
| generic plan, `a = $1` | 104 MB | 48.9 MB | 4.0 MB | 5.6 MB |
| point query (plan-time prune) | 7.8 MB | 0.8 kB | 1.3 kB | 1.3 kB |
| partitionwise join, 1k hash parts | 19 MB | 3.65 MB | 2.96 MB | 1.91 MB |
| partitionwise agg, 1k hash parts | 11 MB | 0.72 MB | 0.64 MB | 0.75 MB |

The waste is leading zero words.  A child partition's relids are a
singleton like `{9001}`, which costs 1.2 kB as a Bitmapset.  Those come
from `build_simple_rel`, `pull_varnos_walker`, `make_plain_restrictinfo`
and `build_index_paths`.  An offset removes the zero words, and does it
slightly better than sbm.  sbm beats the offset form by 2x only on
child-join relid sets, which have one member near each of several
partitions (`adjust_child_relids`, `try_partitionwise_join`,
`create_hashjoin_plan`): about 1-2 MB per 1000-partition join.

## 2. What sbm costs on those shapes

`test_sbm_bms_shapes(10000, 20000, 9)`: each operation is the minimum of
9 alternating blocks, across 3 runs.  The ratio is sbm time / Bitmapset
time, and the range is the ratio across runs.

| shape | union | equal | is_subset | overlap | is_member | iterate | copy | hash |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| tiny `{1,2,3}` | 42x | 3.4x | 2.4x | 1.9x | 1.0x | 1.8x | 2.4x | 11x |
| child `{9997}` | 3.9x | 2.4x | 2.4x | 2.2x | 15x | 2.0x | 1.7x | 0.15x |
| child join | 6.1x | 6.0x | 6.6x | 104x | 17x | 2.3x | 1.0x | 0.23x |
| all 10k parts | 5.2x | 80x | 3.5x | 3.1x | 4.1x | 2.4x | 1.8x | 0.19x |

Spread across runs was under 10% for every cell.  sbm wins only `hash`,
because hashing a Bitmapset walks all its words, zeros included.  Every
other operation is slower, from 2x up to 100x.  `is_member` and
`overlap` are the planner's hottest calls, and they are 15-100x slower
on the partition shapes, which are exactly the shapes where sbm saves
memory.  This agrees with the earlier end-to-end A/B (sbm-backed
Bitmapset: +144% on join search, after optimisation) that led to
reverting that design.

## 3. IntegerSet (GiST VACUUM), the other in-core sparse set

`test_sbm_intset_bench` repeats gistvacuum.c's pattern: an ascending add,
one ordered iteration, then unordered probes.  Minimum of 5 reps, 3 runs.

| members / pattern | add ns (intset / sbm) | iterate ns | probe ns (intset / sbm+locator) | allocated KB |
|---|---|---|---|---|
| 100k, 1% of 10M blocks | 7.8 / 72 | 3.6 / 91 | 100 / 1194 | 247 / 1041 |
| 100k, 50% of 200k | 5.4 / 44 | 3.0 / 64 | 94 / 207 | 103 / 49 |
| 100k, runs of 64 | 4.2 / 39 | 3.2 / 47 | 85 / 235 | 80 / 81 |
| 1M, 1% of 100M | 7.9 / 70 | 3.6 / 91 | 139 / 5195 | 2259 / 8214 |
| 1M, runs of 64 | 4.2 / 39 | 3.1 / 47 | 118 / 1187 | 591 / 529 |

IntegerSet is 9-15x faster to build, 15-25x faster to iterate, and
2.5-40x faster to probe.  It is also smaller, except for dense sets.
Without the locator, an sbm probe walks the chunk list (418 us per probe
at 1M members).  The case where sbm is smaller (50% density) is the one
where a plain bitmap is smaller than both.

## 4. Other candidates, checked and rejected

- Plan-tree and executor Bitmapsets (Append `as_valid_subplans`,
  `es_unpruned_relids`, pruning info): dense ranges of subplan indexes.
  A Bitmapset is already near-minimal for these, and they sit on hot
  `bms_next_member`/`bms_is_member` paths.
- Column sets (`updatedCols`, `extraUpdatedCols`, `attnums`): a wide
  table only reaches 24 words, and its sets are dense.
- `fetch_upper_rel`'s `bms_equal` scan (2M calls x 32 words at 10k
  partitions) is the biggest read cost in the census, but its cause is a
  linear list scan.  A hash table fixes it, and sbm does not help.
- TIDBitmap, the dead-TID store (radix tree), GIN posting lists, the
  shared buffer table, and the KnownAssignedXids array were evaluated
  earlier and rejected (see agent memory).

## What would actually help

If partition-count memory is the target, give Bitmapset a 32-bit
first-word offset (the node has no spare room, so it adds 8 bytes to
every set).  On the census that cuts Bitmapset bytes by 4.5x on
partitioned queries.  It also costs 1.67x on everything else, since
every 24-byte set grows to 40 bytes.  This is the trade Tom Lane
questioned in 2023 ("just about all bitmapsets we deal with are only one
or two words").  Doing it only for sets that start past word 0 would
need a second representation, and every bms function would branch on
it.  I have not prototyped or timed this; it is a direction, not a
result.

sbm's useful home is outside core, as the posting-list and set store for
an index AM like `bark`: large, sparse, mostly build-once sets where
serialized size and set algebra over big sets matter more than
per-operation latency on tiny sets.
