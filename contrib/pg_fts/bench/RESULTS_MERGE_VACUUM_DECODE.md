# pg_fts merge/vacuum/decode benchmarks — 2M Wikipedia (r7i.4xlarge, PG20devel)

Follow-up measurements after the parallel-merge, physical-bloat, and posting-
decode work.  Corpus: wikimedia/wikipedia 20231101.en, 2,188,038 rows.
r7i.4xlarge, shared_buffers=32GB, maintenance_work_mem=8GB, autovacuum off,
median of 9 warm.

## 1. Parallel merge concurrency (the "0 workers on EC2" investigation)

The parallel merge WAS launching workers; the tail was that each participant
held the relation extension lock for its ENTIRE segment write, serializing the
writes.  Fix (committed): hold the extension lock only around the single P_NEW
page extension, so participants write concurrently.  A follow-on attempt to
ITERATE the parallel merge to one segment was measured WORSE and reverted:

| build+merge to 1 segment, 2M | wall time |
|------------------------------|-----------|
| single parallel pass + serial finish (shipped) | ~27 min |
| iterated parallel merge (reverted)             | ~32 min |

Finding: the merge tail is the write of ONE multi-GB output segment by a single
backend.  No group-partition scheme parallelizes a single output write, and
iterating just adds write amplification.  Cutting this needs a streamed/columnar
write path (DEFERRED.md), not more merge parallelism.  Workers do run for the
scan and the intermediate group merges.

## 2. Physical bloat reclaim: fts_vacuum (low-page FSM bias + truncate)

Ordinary merges recycle freed pages to the FSM but never shrink the relation,
so a freshly built index stays physically large.  fts_vacuum() compacts to one
segment reusing the lowest free blocks (packing live pages at the front) then
truncates the free tail back to the OS.

| | size | year count | slovakia count |
|-|-----:|-----------:|---------------:|
| before fts_vacuum | 15 GB   | 735,658 | 10,889 |
| after  fts_vacuum | 3770 MB | 735,658 | 10,889 |

**15 GB -> 3.77 GB (4x) in 1.7 s**, counts identical, ranked queries unaffected
(17 ms post-vacuum).  On-disk confirmation: the trailing segment files (.4..14,
~11 GB) are truncated to 0 bytes.  This puts pg_fts's index (3.77 GB) on par
with pg_search (4.1 GB); it also runs automatically in amvacuumcleanup when the
file is >=25% free.

## 3. Word-oriented FOR decode (5.7x faster posting unpack)

Posting decode is the hot path in every query.  Replacing the per-bit unpack
with a word-oriented extract (one unaligned load + shift + mask per value; 5.7x
in isolation) moved the decode-bound queries:

| query (ms, 2M) | before (4-way run) | after decode+vacuum |
|----------------|-------------------:|--------------------:|
| fts_count rare      |   9.8 |  **6.9** |
| fts_count common    | 305   | **101**  |
| ranked common&mid   |  19.3 | **15.5** |
| ranked rare&mid     |  22.8 |  25.5    |
| ranked top-100 common | 74  |  82      |

The headline: common-term fts_count 305 -> 101 ms (3x), now BELOW pg_search's
123 ms on the same box.  The ranked paths move less because they are gated more
by WAND cursor advance and top-k than by raw decode volume; closing the ranked
gap vs vchord/pg_search remains a codec matter (compact columnar + rank/select
skip), per NOTE_IMPACT_ORDERING.md.
