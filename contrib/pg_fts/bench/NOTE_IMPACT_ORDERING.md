# Impact-ordered posting directory: attempted, measured, reverted

## Goal
Close the ranked-retrieval latency gap to ParadeDB pg_search (Tantivy), whose
single-term / common-term ranked top-k stays flat (~7 ms on 2M Wikipedia) while
pg_fts's block-max WAND degrades with term frequency (single common term
`year`, LIMIT 10: ~32 ms; LIMIT 100: ~66 ms).

## What was built (format v3, since reverted)
A per-term **impact-ordered block skip directory**: for a term with
df >= 2048, the writer recorded every posting block's `(blk, off, max_tf,
min_doclen)` and stored them sorted by an avgdl-independent impact proxy in a
`BM25_SKIPDIR` page chain, referenced from three new `BM25DictEntry` fields.
A new single-term scan (`fts_search_impact_single`) sorted the directory by the
exact recomputed impact bound (at current avgdl -- drift-safe) and visited
blocks best-first, intending to stop once k results beat the next block's bound.

Correctness was verified exact (index top-k == true BM25 order, recomputed with
real N/avgdl/df).  The soundness design was right.

## Why it was reverted: it does not prune on real text
Instrumented block-visit counts on the 2M Wikipedia corpus:

| term | df | blocks | blocks visited before early-stop |
|------|----|--------|----------------------------------|
| `year`    | 677,806 | 5296 | **5282 (99.7%)** |
| `year` (SRF k=10, wantk=32) | 677,806 | 5296 | **3804 (72%)** |
| `hungary` | ~22,000 | 173  | **170 (98%)** |

The early-stop never fires meaningfully because **within one term the per-block
impact bounds are tightly clustered just above the top-k threshold**: the idf is
constant across blocks, and for a common term thousands of blocks each contain
some high-tf document, so `bound[block]` (~2.33 for `year`) barely exceeds the
k-th best score (~2.32).  Ordering the blocks by bound does not help when the
bounds occupy a razor-thin band -- the same fundamental limitation as the
existing block-max WAND, which the directory was meant to beat.  Result: no
measured latency improvement on any band (Q5/Q6/Q7/Q8/Q9 all within noise of the
docid-ordered scan), at the cost of format v3, a skip-page chain, ~3% larger
index, and a per-query directory sort.

## Conclusion
pg_search's flat ranked latency does **not** come from impact-ordered postings
alone -- an impact skip directory cannot early-terminate a common term whose
document scores cluster near the maximum (which real English text produces).
Its advantage is a different, deeper investment: a compact columnar segment
format that decodes far less per candidate, and query parallelism (a Parallel
Custom Scan).  Matching it is a segment-codec rewrite, not a skip structure
bolted onto the existing docid-ordered blocks.

Per the project's rule against shipping optimizations that do not deliver, the
v3 impact directory was reverted (index stays format v2 / extension 1.19).  This
document records the attempt so the negative result is not re-discovered.

Measured on: EC2 r7i.4xlarge, PostgreSQL 17.10, 2,000,000 real Wikipedia
articles, pg_fts vs pg_search 0.24.1.  See RESULTS_VS_PGSEARCH_WIKI.md for the
head-to-head latency table that motivated this attempt.
