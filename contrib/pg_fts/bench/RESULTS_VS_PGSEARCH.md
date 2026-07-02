# pg_fts vs ParadeDB pg_search — head-to-head

Same box, same PostgreSQL, same corpus, warm cache.

- **Instance**: EC2 m6i.2xlarge (8 vCPU, 32 GB), Fedora 43, gp3 16k IOPS, shared_buffers=8GB.
- **PostgreSQL**: 17.5 (built from source), identical for both.
- **Extensions**: pg_fts 1.18 vs pg_search 0.24.1 (ParadeDB, Tantivy-backed),
  in separate databases (both register an AM named `bm25`, so they cannot
  coexist in one DB).
- **Corpus**: 2,000,000 docs, Zipfian single-token vocab (50k terms), ~68 B/doc.
- Medians / p95 over 15 runs.

## Index size
| pg_fts | pg_search |
|--------|-----------|
| 202 MB | 213 MB    |

Parity (pg_fts marginally smaller).

## Query latency (median ms)
| query                              | pg_fts | pg_search | winner            |
|------------------------------------|--------|-----------|-------------------|
| Q1 rare count (df 2000)            | **1.6**| 6.5       | **pg_fts 4.1x**   |
| Q2 mid count (df 75k)              | 87     | **8.3**   | pg_search 10.5x   |
| Q3 two-term AND count              | **6.4**| 7.1       | pg_fts 1.1x       |
| Q4 ranked top-10 (mid, mid)        | **4.5**| 7.7       | **pg_fts 1.7x**   |
| Q5 ranked top-10 (common, mid)     | 13.3   | **7.8**   | pg_search 1.7x    |
| Q7 ranked top-100 (common, mid)    | 41     | **8.1**   | pg_search 5.0x    |
| Q6 fuzzy count (zaaaf~1)           | 564    | **25**    | pg_search 22x     |

## Who wins, and WHY

**It splits by whether the query MATERIALIZES a large tuple set.**

### pg_fts wins: selective / small-result queries
- **Q1 rare term (4.1x)**, **Q4 small-k ranked (1.7x)**, **Q3 AND (~par)**.
- Why: pg_fts's block-max WAND + lazy paging return the top-k (or a tiny match
  set) without ever reading most postings, and the rare-term path touches a
  handful of pages.  Nothing large is materialized, so PostgreSQL's heap never
  gets hammered.  This is the flagship BM25 use case, and pg_fts is faster than
  the Tantivy engine on it.

### pg_search wins: large-result counts and large-k ranking
- **Q2 mid count (10.5x)**, **Q7 top-100 common (5x)**, **Q6 fuzzy count (22x)**.
- Why — ONE root cause: **the PostgreSQL heap.**  pg_fts is heap-backed and
  delegates MVCC visibility to PG, so any query whose candidate/result set is
  large must run a **Bitmap Heap Scan that fetches every candidate's heap page
  to check visibility** (Q2: 74,932 matches -> 51,953 heap-block fetches ~85ms;
  the bitmap *index* scan itself is ~1ms).  pg_search keeps its own
  segment-resident, MVCC-aware doc store (Tantivy fast-fields + a visibility/
  deleted-docs structure), so it counts and ranks large sets from its own
  structures and never touches the PG heap.  Its latency is nearly constant
  (~8 ms) regardless of match-set size; pg_fts's grows with the match set.
- Fuzzy (Q6) compounds this with a second cost: pg_fts's `term~k` walks the
  whole sorted dictionary applying the Levenshtein automaton (exact, but O(all
  terms)), whereas pg_search's FST does a DFA-over-FST walk that skips
  non-matching ranges.

## The honest one-line verdict
**pg_fts wins the queries a BM25 engine exists for — ranked top-k and selective
lookups — while being a true PostgreSQL-native, buffer-manager/WAL/MVCC index.
pg_search wins large-result counts and large-k ranking because its private
Tantivy store answers them without touching the Postgres heap; pg_fts pays a
heap-visibility fetch there. The trade is architectural: heap-native (pg_fts,
crash-safe/replicated by PG itself, no separate store to keep consistent) vs.
self-contained columnar store (pg_search, faster bulk counts, but its own
copy of the data and its own MVCC bookkeeping).**

## What would close pg_fts's gaps (measured, not guessed)
1. **Index-resident visibility for counts** (the big one): a per-segment
   live-docs bitmap consulted during `count(*)`/large scans so the heap is not
   fetched for visibility.  This is the single change that would flip Q2/Q6/Q7.
   It is real work (must track deletes into the segment, MVCC-correctly) and is
   the essence of what makes Tantivy fast here.
2. **Bounded WAND over-fetch for large k** (Q5/Q7): `wantk` scales the internal
   heap; tune it and the common-term block-max.
3. **DFA range-skip over the dictionary** (Q6): a true FST (or an
   ordered-walk-with-skip over the sorted dict) so fuzzy does not scan every
   term.
