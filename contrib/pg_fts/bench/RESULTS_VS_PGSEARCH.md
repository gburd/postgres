# pg_fts vs ParadeDB pg_search — final head-to-head

Same box, PostgreSQL, corpus; warm cache; table VACUUMed.
EC2 m6i.2xlarge, Fedora, PG 17.5 (both), pg_fts 1.18 vs pg_search 0.24.1,
2,000,000 docs, Zipfian single-token vocab (50k terms). Medians / 15 runs.

## Index size:  pg_fts 204 MB   |   pg_search 213 MB

## Query latency (median ms)
| query                            | pg_fts | pg_search | verdict         |
|----------------------------------|--------|-----------|-----------------|
| Q1 rare count (df 2000)          | **0.85** | 5.2     | **pg_fts 6.1x** |
| Q2 mid count (df 75k)            | **7.1**  | 7.5     | **pg_fts 1.1x** |
| Q3 two-term AND count            | **4.5**  | 6.1     | **pg_fts 1.4x** |
| Q4 ranked top-10 (mid, mid)      | **3.7**  | 6.3     | **pg_fts 1.7x** |
| Q5 ranked top-10 (common, mid)   | 11.1   | **6.2**   | pg_search 1.8x  |
| Q7 ranked top-100 (common, mid)  | 35     | **6.3**   | pg_search 5.6x  |
| Q6 fuzzy count (matches 1.28M)   | 268    | **25**    | pg_search 11x   |

pg_fts wins 4 of 7; the index is smaller.

## The two losses we set out to fix — progress and root cause

### Q5/Q7 ranked over a high-df COMMON term: 13->11 / 40->35 ms
Fix applied: **block-skipping WAND seek** (wand_skip_blocks) so advancing a
common-term cursor to the pivot skips whole 128-blocks by header (first_docid)
instead of stepping/decoding every posting.  Helped, but the residual cost is
the **adaptive-k recompute**: a single WAND pass for this query is 11 ms (vs
pg_search 6 ms -- close), but LIMIT > 64 grows k (64->256) and RE-RUNS the batch
top-k from scratch (measured: LIMIT 64 = 11 ms, LIMIT 65 = 35 ms).  Eliminating
it needs a **resumable ordering scan** (continue instead of recompute) -- a real
cursor-state rewrite, not a tuning knob (raising the initial k just moves the
cost onto the common small-LIMIT case, verified).

### Q6 fuzzy count: 564 -> 268 ms (2.1x)
Two fixes applied:
1. **DFA-guided dictionary skip** -- the Levenshtein automaton reports the dead-
   prefix length and we seek past every term sharing a dead prefix (FST-like),
   instead of scanning all 50k terms.
2. **k-way merge** of the matching terms' already-sorted posting runs, replacing
   a qsort over the whole ~1.28M-doc union (profiled: that qsort alone was
   ~400 ms -- the true bottleneck, not the dictionary scan).
Residual: `zaaaf~1` genuinely matches **1.28M documents**; pg_fts must decode
those postings and produce 1.28M ordered TIDs for the scan (~21k index buffers),
while pg_search counts from per-segment metadata without materializing.  Closing
this needs **count-without-materialization** (a docid bitmap + popcount count
path) -- only valid for count(*), and PG's executor still pulls TIDs one by one,
so it needs a count-pushdown / custom-scan seam.

## Honest verdict
pg_fts wins the selective + ranked-top-k core and boolean counts, is smaller on
disk, and is a true heap-native PG index (WAL/buffer-manager/MVCC via PG, no
private store).  The remaining pg_search wins are both **large-materialization**
cases -- deep ranked pages over a common term (needs a resumable scan) and
fuzzy queries matching a large fraction of the corpus (needs count-without-
materialization).  Both are understood, bounded projects; neither is a
visibility issue (the index-only-scan work already removed that).
