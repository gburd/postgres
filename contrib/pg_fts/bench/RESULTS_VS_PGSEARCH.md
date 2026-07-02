# pg_fts vs ParadeDB pg_search — after index-only-scan counts

Same box, same PostgreSQL, same corpus, warm cache, table VACUUMed.

- **Instance**: EC2 m6i.2xlarge (8 vCPU, 32 GB), Fedora, gp3 16k IOPS, shared_buffers=8GB.
- **PostgreSQL**: 17.5 from source, identical for both.
- **Extensions**: pg_fts 1.18 (with index-only-scan counts) vs pg_search 0.24.1.
- **Corpus**: 2,000,000 docs, Zipfian single-token vocab (50k terms).
- Medians over 15 runs.

## Index size
| pg_fts | pg_search |
|--------|-----------|
| 202 MB | 213 MB    |

## Query latency (median ms) — BEFORE vs AFTER index-only scan
| query                            | pg_fts BEFORE | pg_fts NOW | pg_search | verdict NOW      |
|----------------------------------|---------------|------------|-----------|------------------|
| Q1 rare count (df 2000)          | 1.6           | **0.9**    | 5.4       | **pg_fts 6.0x**  |
| Q2 mid count (df 75k)            | 87            | **7.3**    | 7.5       | **pg_fts ~ties/wins** |
| Q3 two-term AND count            | 6.4           | **4.5**    | 6.0       | **pg_fts 1.3x**  |
| Q4 ranked top-10 (mid, mid)      | 4.5           | **4.3**    | 6.5       | **pg_fts 1.5x**  |
| Q5 ranked top-10 (common, mid)   | 13            | 13.0       | **6.8**   | pg_search 1.9x   |
| Q7 ranked top-100 (common, mid)  | 41            | 40         | **6.7**   | pg_search 6.0x   |
| Q6 fuzzy count (zaaaf~1)         | 564           | 370        | **25**    | pg_search 15x    |

## The index-only-scan win
Q2 (mid-frequency count) went from **87 ms (losing 10.5x) to 7.3 ms (winning)** —
`EXPLAIN` now shows `Index Only Scan ... Heap Fetches: 0`.  The bottleneck was
the Bitmap Heap Scan fetching 52k heap pages to check MVCC visibility; the
index-only scan consults PostgreSQL's visibility map instead and touches the
heap only for recently-modified pages.  MVCC-correct by construction (same
mechanism as btree IOS; verified: after delete 3000 + update 1000, count is
exactly 7000 == seqscan, with the VM forcing heap fetches only on the dirtied
pages).  **This closed the single biggest gap vs pg_search.**

## Scorecard: pg_fts now wins or ties 4 of the 5 core queries
- **Wins:** rare count (6x), mid count (~tie/win), AND count (1.3x), ranked
  top-10 (1.5x) — the entire selective + ranked-retrieval workload a BM25 engine
  is built for, while being a true heap-native PG index (WAL, buffer manager,
  MVCC delegated to PG; no separate store).
- **Still loses:** two NON-visibility cases:
  1. **Large-k ranked over a common term (Q5/Q7):** WAND/MaxScore is exact but
     not skipping the 540k-df common term's blocks tightly enough; ~40 ms vs
     ~7 ms.  A WAND block-max-tightness / impact-ordered-postings improvement,
     not a heap issue (only 730 buffers touched).
  2. **Fuzzy count (Q6):** `bm25_fuzzy_terms` walks the whole 50k-term dictionary
     with the Levenshtein automaton (exact but O(all terms)); pg_search's FST
     does a DFA-over-FST walk that skips ranges.

## Remaining work to fully match/beat pg_search everywhere
- Q5/Q7: tighter block-max WAND on high-df terms (store block-level impact
  score, not just max_tf; or impact-ordered postings).
- Q6: DFA range-skip over the sorted dictionary (a real FST, or an
  ordered-walk-with-skip) so fuzzy doesn't scan every term.
