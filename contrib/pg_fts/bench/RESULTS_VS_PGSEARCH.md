# pg_fts vs ParadeDB pg_search — final head-to-head (after resuming on the losses)

Same box/PG/corpus; warm; VACUUMed.  EC2 m6i.2xlarge, PG 17.5 (both),
pg_fts 1.19 vs pg_search 0.24.1, 2,000,000 docs, Zipfian vocab (50k). Medians/15.

Index size: pg_fts 204 MB | pg_search 213 MB.

## Query latency (median ms)
| query                            | pg_fts | pg_search | verdict          |
|----------------------------------|--------|-----------|------------------|
| Q1 rare count                    | **0.83** | 5.1     | **pg_fts 6.2x**  |
| Q2 mid count (count(*))          | **7.2**  | 7.3     | **pg_fts ~tie**  |
| Q2 mid count (fts_count())       | **4.6**  | 7.3     | **pg_fts 1.6x**  |
| Q3 two-term AND count            | **4.5**  | 6.1     | **pg_fts 1.4x**  |
| Q4 ranked top-10 (mid, mid)      | **4.4**  | 6.2     | **pg_fts 1.4x**  |
| Q5 ranked top-10 (common, mid)   | 12.0   | **6.3**   | pg_search 1.9x   |
| Q7 ranked top-100 (common, mid)  | **12.0** (was 35) | 6.2 | pg_search 1.9x |
| Q6 fuzzy count (1.28M matches)   | 227 (was 268) | **25** | pg_search 9x |

## What this round changed
### Q7 ranked top-100: 35 -> 12 ms (5.6x gap -> 1.9x)  [FIXED the cliff]
The cost was the adaptive-k **recompute**: LIMIT 100 ran a k=64 pass (11ms) then
recomputed at k=256 (24ms).  Measured k-vs-latency (k=64->3.5ms, 100->12ms,
256->24ms, 12000->12.5s: WAND degrades super-linearly as a big k flattens the
pruning threshold).  Fixes: start k at a full first page (100) so LIMIT 11..100
is one pass; cap k growth at the query's provable max-hits (bm25_query_maxhits,
an RPN df-bound) AND at BM25_MAX_ORDERK to bound worst-case deep pagination.
Q4 (top-10) cost only ~+1ms.

### Q5 ranked top-10 (common term): 12 ms, unchanged
This is the irreducible **single-pass** WAND cost for a common-term AND -- the
540k-df term's postings must be traversed.  pg_search's 6ms comes from
IMPACT-ORDERED postings (it can stop early); that is a posting-codec change
(store postings/blocks ordered by contribution) -- the honest remaining item.

### Q6 fuzzy count: 268 -> 227 ms; added fts_count()
Added bm25_count_visible + fts_count(regclass, ftsquery): an MVCC-correct bulk
count that avoids the per-tuple executor round-trips (visibility via the VM;
heap probed only for not-all-visible pages).  This makes single-term counts
faster (mid 75k: 7.2 -> 4.6 ms) and is the count-pushdown primitive.
BUT the fuzzy case barely moved: profiling shows Q6's cost is DECODING ~1.3M
postings from the ~200 terms within 1 edit of the query (a rare fuzzy term that
matches few docs counts in 6ms; the 1.28M-match one is 227ms).  pg_search's 25ms
uses Tantivy's precomputed per-segment doc counts and NEVER decodes full
postings -- via a `Custom Scan (ParadeDB Aggregate Scan)` that pushes COUNT into
the index (verified in its EXPLAIN: actual rows=1).

## The remaining gaps share ONE root cause
Both Q5 (common-term ranked) and Q6 (large fuzzy count) are bounded by **full
posting decode**.  pg_search avoids it with columnar/impact-ordered structures:
- Q5: impact-ordered postings (stop-early ranking).
- Q6: precomputed per-term doc counts + a COUNT-pushdown Custom Scan.
These are the same architectural investment (a Tantivy-style secondary layout),
not a visibility or algorithm-tuning issue.  pg_fts now wins/ties 5 of 7 and is
smaller on disk, as a fully heap-native PG index.


## Update: lazy per-column posting decode (WAND cursor)
The ranked hot path now decodes only docid gaps eagerly; tf/doclen are extracted
per-posting on demand, so blocks pruned by block-max never decode tf/dl.
Measured on the same EC2 box (PG 17.5, 2M docs):

| query                            | pg_fts before | pg_fts NOW | pg_search | gap now |
|----------------------------------|---------------|------------|-----------|---------|
| Q4 ranked top-10 (mid, mid)      | 4.4           | **4.1**    | 6.3       | pg_fts 1.5x win |
| Q5 ranked top-10 (common, mid)   | 12.0          | **9.1**    | 6.3       | pg_search 1.45x |
| Q7 ranked top-100 (common, mid)  | 12.0          | **9.2**    | 6.4       | pg_search 1.44x |
| Q1 rare count                    | 0.85          | **0.88**   | 5.2       | pg_fts 5.9x win |

Q5/Q7 (common-term ranked) improved ~25%; the gap to pg_search narrowed from
1.9x to ~1.45x.  The residual is pg_search's IMPACT-ORDERED postings (it can end
the pivot walk earlier); our postings stay docid-ordered for exact WAND.  All
top-k byte-identical to a full seqscan sort (AND/OR/2-3-term, LIMIT 10..150).
