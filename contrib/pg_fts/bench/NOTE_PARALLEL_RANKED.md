# Parallel ranked CustomScan (Option B stages 2+3): built, measured, reverted

## Goal
A CustomScan for `ORDER BY col <=> q LIMIT k` that fans the WAND top-k across
parallel workers (docid-range partition) to close the common-term ranked gap to
vchord/pg_search.  Built end to end: set_rel_pathlist_hook detection, a
FtsRankedScan CustomScan producing heap tuples, a docid-range-bounded WAND
(bm25_topk_candidates_range + per-cursor [docid_lo, docid_hi)), DSM worker
coordination, and a leader-side k-way merge + visibility.

## Correctness: verified
Parallel and serial top-k are byte-identical (workers partition the docid space
disjointly), confirmed locally for single- and multi-term queries at 30k-200k
rows.

## Why it was reverted: no measured win at 2M
Two independent reasons, both measured on 2M real Wikipedia (r7i.4xlarge):

1. **Amdahl ceiling.** Profiling (NOTE_FORMAT_V3_PROFILE.md) showed the
   common-term ranked query is ~30% decode+WAND (what the workers parallelize)
   and ~70% scoring / top-k heap / MVCC visibility / executor -- and the leader
   still does the merge + visibility serially.  So even with perfect worker
   scaling the ceiling is ~30% off, i.e. best case ~50 ms vs 73 ms.

2. **Workers did not launch inside the executor at scale.** Launching a parallel
   context from within ExecCustomScan fell back to serial on EC2 (0 workers, the
   same context/gate fragility seen with the parallel merge), so the measured
   result was serial both ways: top-100 common 73 vs 66 ms, common&mid 9.9 vs
   9.9, rare&mid 26 vs 26 -- within noise.

The serial ranked CustomScan is also redundant with the existing bm25 AM
ordering scan (amgettuple over the same WAND engine, identical speed), so it
adds ~400 lines and a second code path for no benefit.

## What was kept
- The **COUNT pushdown CustomScan** (Stage 1) -- a real, transparent ~3x win on
  common-term `count(*) WHERE @@@` (240 ms bitmap heap scan -> ~75 ms), shipped.
- The behavior-preserving refactor of bm25_topk_visible into
  bm25_topk_candidates_range(index, q, wantk, [0, MAX)) + a visibility wrapper,
  and the WandCursor docid_lo/docid_hi range fields -- harmless, tested, and a
  ready foundation if a future design overcomes the Amdahl ceiling (e.g. by
  parallelizing visibility too, or via a true executor Gather over a partitioned
  scan rather than internal DSM).

## Direction if revisited
The ranked common-term win requires parallelizing the WHOLE query, not just
candidate generation: a real partial-path / Gather plan where each worker
produces visible ranked rows for its docid slice and the executor merges -- so
visibility (the ~40% serial tail here) also scales.  That is a partial-path AM
(amcanparallel ordering scan) design, not an internal-DSM CustomScan; deferred
until the ranked gap is a priority with a design that beats the ~30% ceiling.

Measured on: EC2 r7i.4xlarge, PG20devel (fts branch), 2,188,038 Wikipedia rows.
