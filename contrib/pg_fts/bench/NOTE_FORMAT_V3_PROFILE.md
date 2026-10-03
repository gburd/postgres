# Format-v3 / ranked-latency: profiled, and why the codec rewrite is not the win

## Question
Close the common-term ranked-retrieval gap to vchord/pg_search via a "format v3"
compact columnar codec (the direction suggested at the end of
NOTE_IMPACT_ORDERING.md).

## What was measured (not guessed)
Profiled the common-term ranked top-100 (`common` in all 500k docs, warm,
`perf --no-children` leaf/self time) on the CURRENT code (after the 5.7x
word-oriented FOR-decode landed):

| self time | function | what it is |
|-----------|----------|------------|
| 19.5% | `bm25_for_unpack` | docid (gap) decode, already word-oriented |
| 9.7%  | `wand_load_block` | memcpy block payload + gap->docid loop |
| ~3%   | `hash_bytes`/`hash_search`/`tag_hash` | ReadBuffer hash lookup per block |
| 1.2%  | `palloc` | per-block payload alloc |
| 0.7%  | `UnlockReleaseBuffer` | |
| ~65%  | scoring, top-k heap, visibility, executor | the rest |

So decode + block-load is ~30% of the query; the other ~70% is scoring / heap /
executor plumbing that a codec change does not touch.

## Two codec-side changes were tried and REVERTED (measured worse or flat)
1. **Impact-ordered block skip directory** (the original "format v3"):
   reverted -- per-block impact bounds cluster in a razor-thin band on real
   English, so best-first ordering never early-terminates a common term.  See
   NOTE_IMPACT_ORDERING.md.
2. **Reusable per-cursor block buffer** (remove the per-block palloc that the
   call-graph profile fingered): measured *slower* (top-100 common 15 -> 20 ms).
   The per-block palloc is only 1.2% (PG's bump allocator is ~free), and a
   larger fixed buffer hurt cache locality.  Reverted.

## Why a full columnar-codec v3 is not pursued now
The irreducible cost for a common term is that block-max WAND must LOAD nearly
every block, because the pruning bound cannot separate blocks whose document
scores all cluster near the term maximum (constant idf + many high-tf docs per
block -- the NOTE_IMPACT_ORDERING finding, now confirmed by the flat profile:
`wand_load_block`/`bm25_for_unpack` dominate precisely because no block is
skipped).  A columnar codec would lower the *constant* per-block decode cost,
but:

- the docid decode is already word-oriented (5.7x done), and the next step
  (SIMD bulk-unpack of 128 gaps) is a bounded ~2-3x on the 19.5% slice, i.e. a
  ~5-8% whole-query win -- worth doing, but it is a decode micro-opt, not a
  "format v3", and needs SIMD portability work (guarded x86/ARM paths);
- vchord/pg_search's flat latency also comes from **query parallelism** (a
  parallel custom scan splitting the posting list across workers), which is
  orthogonal to the codec and likely the larger lever for common terms;
- ~70% of the query is scoring/heap/executor, so even a perfect codec caps the
  win at ~30%.

## Direction that the evidence actually supports (ranked common terms)
1. **SIMD FOR-unpack** of the docid column (bounded, in-format, ~5-8% whole-query;
   portability-gated) -- a decode micro-opt, safe to add.
2. **Parallel ranked scan** (split a high-df term's block chain across workers,
   merge top-k) -- the same mechanism that gives pg_search/vchord flat common-
   term latency, and the biggest remaining lever.  This is an executor/AM change,
   not a codec change.

Both are recorded in DEFERRED.md.  A speculative columnar-codec rewrite is NOT
undertaken because the profile shows it cannot beat the ~30% decode+load ceiling
and cannot enable the block-skipping that real English text defeats -- the same
negative result that reverted the impact directory, now confirmed by profiling
rather than re-discovered.

Measured on: local 500k-doc corpus (`common` in every doc), PG20devel,
pg_fts at commit 8ce63c29b1b + the word-oriented decode, perf 7.1.
