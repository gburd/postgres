# pg_fts — deferred enhancements (discussed, not yet implemented)

Running list of enhancements raised during development that are NOT yet done, so
they are tracked rather than rediscovered. Ordered roughly by value.

## Performance

1. **Parallel merge: verify at scale + fix worker-slot gating on EC2.**
   Parallel merge (`bm25_merge_all_parallel`, commit d23dcd0) is implemented and
   verified CORRECT locally (400k-doc corpus, parallel build -> 9 segments ->
   parallel `fts_merge` -> 1 segment, byte-identical counts, workers launch).
   But it was never timed at 2M scale on EC2: the i4i box fell back to the serial
   path because parallel workers did not launch there (`max_worker_processes=8`
   shared with pg_search's preloaded background workers + the logical-rep
   launcher left too few free slots, so `LaunchParallelWorkers` got 0 and the
   code correctly fell back to serial). TODO: on the next run set
   `max_worker_processes` high enough (e.g. 16) and capture the parallel-merge
   speedup vs the serial ~8-15 min merge.

2. **Level-2 recursive parallel merge (W -> W/2 -> ... -> 1).**
   The current parallel merge does one parallel pass into (workers+1) segments,
   then a serial final combine to one. For very large indexes the final combine
   is still O(index) single-threaded. Recurse the parallel merge so the final
   combine also parallelizes. Noted in a `ponytail:` comment in
   `bm25_merge_all_parallel`. Deferred — one parallel pass already removes the
   dominant per-segment decode cost.

3. **Parallel build: fewer, larger per-worker segments.**
   Each worker currently flushes several segments (budget-triggered), so a
   parallel build leaves ~8-16 segments needing a merge. Giving each worker a
   larger flush budget (its share of `maintenance_work_mem`) would leave ~1
   segment per worker, shrinking the post-build merge input. Complements #1/#2.

4. **Ranked common-term latency (the codec / parallel-scan gap).** THE headline
   gap vs pg_search/vchord: ranked top-k over a common term stays flat (~7-28 ms)
   for them but degrades for pg_fts (docid-ordered block-max WAND; 2M Wikipedia
   `year` LIMIT 100 ~70-88 ms). Two codec-side attempts were made and REVERTED:
   an impact-ordered skip directory (bench/NOTE_IMPACT_ORDERING.md -- per-block
   impact bounds cluster too tightly to prune on real text) and a reusable
   per-cursor block buffer (bench/NOTE_FORMAT_V3_PROFILE.md -- measured slower;
   the per-block palloc is only 1.2%). Profiling (NOTE_FORMAT_V3_PROFILE.md)
   shows the common-term query is ~30% decode+block-load and ~70%
   scoring/heap/executor, and that block-max WAND cannot skip blocks on common
   English terms -- so a columnar-codec rewrite is capped at ~30% and cannot
   enable skipping. The evidence-supported levers instead are: (a) SIMD
   bulk-unpack of the docid column (bounded ~5-8% whole-query, portability-gated
   -- a decode micro-opt, not a format change); (b) a PARALLEL ranked scan
   (split a high-df term's block chain across workers, merge top-k) -- the same
   mechanism behind pg_search/vchord's flat common-term latency and the largest
   remaining lever (an executor/AM change, see item 6). A speculative
   columnar-codec "format v3" is deliberately NOT pursued: the profile shows it
   cannot beat the ceiling nor enable pruning.

5. **COUNT / aggregation Custom Scan pushdown.** pg_search answers common-term
   `count(*)` in ~10 ms via a `ParadeDB Aggregate Scan` (Custom Scan,
   `actual rows=1`); pg_fts's transparent `count(*)` is a bitmap heap scan
   (~300-400 ms on a common term) because a bitmap over a huge match set goes
   lossy and rechecks the heap. `fts_count()` already avoids this (VM-based bulk
   count, 1.8-60 ms) but is an explicit function call, not transparent. A
   `set_rel_pathlist_hook` / `create_upper_paths_hook` CustomScan that pushes
   COUNT into the index would make plain `count(*) WHERE @@@` fast.

6. **Parallel scan (`amcanparallel`).** Query execution is single-threaded;
   pg_search parallelizes ranked scans (Parallel Custom Scan). A parallel bitmap
   / ordering scan for pg_fts would help large scans, though the warm-cache
   selective-query workload benefits little.

7. **Storage AIO / read_stream prefetch for the cold merge full-scan.** Audited
   (CAPABILITIES.md Q6): pg_fts uses no storage AIO; the build heap scan already
   gets core `read_stream` free. Only the cold merge full-scan of posting pages
   could benefit, *if* `BM25SegMeta` recorded a contiguous posting block range
   so a `blk++` read_stream callback can prefetch. Low priority (nextblk pointer
   chains and WAND block-skipping defeat prefetch elsewhere). Deferred until a
   cold-merge I/O bottleneck is measured.

## Sparsemap (vendored)

8. **Exploit v5.3.0 batch/cached APIs under a delete-heavy workload.**
   Integrated `sm_contains_many` (batched tombstone filter) and
   `sm_contains_cached` (stack MRU cache in the WAND cursor + merge), commit
   ~[sparsemap v5.3.0]. These only help the tombstone/merge paths, so they show
   no effect on a delete-free read benchmark. Not yet benchmarked on a
   delete/update-churn workload where they should help — TODO to quantify.

## Benchmark / competitive

9. **Complete the 4-way real-corpus comparison.** The last EC2 run measured
   pg_fts vs pg_search only. Still to build + measure on the same 2M Wikipedia
   corpus: **VectorChord-bm25** (0.3.0 for PG17 downloaded/installed on the box,
   never benchmarked) and the **tsvector/GIN "pg_textsearch"** baseline. RUM was
   built once but dropped per instruction. A clean 4-way table (build time,
   index size, per-query latency across bands, warm/cold) is still owed.

10. **`fts_search` SRF under-fetch safety.** Reducing the top-k over-fetch to
    `k*2` (commit 68ce28f) is safe for the amgettuple ordering scan (which
    retries), but the `fts_search()` SRF does NOT retry — under a heavy-delete
    workload where >50% of the top rows are invisible it could return < k. A
    small internal retry in `bm25_topk_visible` (grow wantk and re-scan if
    nvis < k) would make tight over-fetch fully safe everywhere.

## Correctness / robustness (lower urgency)

11. **Sparsemap error-path leaks.** `sm_create`/blob buffers are libc/palloc
    allocations; on an ereport between create and free they leak for the
    statement (reclaimed at txn/backend end). A PG_TRY/FINALLY around the few
    error-prone spots would tidy this. Low severity (rare error paths).

## Release process

12. **Tag + finalize.** Branch has been rebased onto origin/master and
    force-pushed once; a `pg_fts-v2026.07.04`-style tag was prepared but not cut
    (the ranked-latency goal it was gated on was not met). Decide whether to tag
    the current state (which ships: sparsemap v5.3.0, BM25L parity, parallel
    build + parallel merge, isolation/crash/replication tests, SGML docs +
    CAPABILITIES.md).
