# pg_fts strategy re-plan — synthesis of scale testing + algorithm research

Two research passes (production engines: Lucene/Tantivy/PISA/RediSearch/Vespa;
succinct structures: CMU efficient.github.io — SuRF/FST/HOPE/poppy/cuckoo) plus
the scale-test faults converge on a concrete target design.  This supersedes the
ad-hoc choices in the current implementation.

## What the research changed in our plan (the important corrections)

### 1. Posting codec: FOR-128 blocks, NOT delta+varint, NOT sparsemap
Both Lucene and Tantivy independently chose **Frame-Of-Reference bit-packing over
fixed 128-doc blocks** (Δdocid + tf in parallel blocks), with a **PFOR patched-
exception** variant for outlier blocks, and a **per-block max-impact** value that
replaces skip lists and drives block-max WAND.  PISA confirms: Partitioned
Elias-Fano compresses smaller and SIMD-BP128 decodes faster in microbenchmarks,
but FOR-blocks win *wall-clock* WAND latency because WAND is random-skip and EF's
skips touch more cache lines.

- **Current pg_fts:** delta+varint per posting, per-page (not per-128-block)
  block-max.  → **Change to FOR-128 blocks with per-block impact.**
- **Mistake to fix:** we must NOT use sparsemap for the *scored* postings (a
  compressed bitmap has no tf channel and no per-block impact → can't drive WAND
  or index-only BM25).  Sparsemap is correct ONLY for the trigram/fuzzy
  candidate *set-membership* use (which is where we already moved it).

### 2. Term dictionary: FST (Fast Succinct Trie), not a sorted linear-scan list
Lucene and Tantivy both use an **FST** term dictionary (Lucene: FST in-RAM
"which block" index + front-coded on-disk leaves; Tantivy: the `fst` crate).
CMU's **SuRF/FST** is the concrete succinct-trie artifact to port (~10 bits/key,
LOUDS-Dense top + LOUDS-Sparse bottom, rank/select via **poppy**).  Payoffs:
compact, fast prefix (`tok*`), ordered iteration, and range.

- **Current pg_fts:** sorted dictionary, linear scan within a page.  Fine for
  small dicts / exact lookup; poor at large vocabulary and prefix/range.
  → **Adopt an FST term dictionary** (SuRF/FST design, poppy rank/select).

### 3. Fuzzy/regex: Levenshtein-DFA ∩ FST may retire the trigram funnel
Tantivy does fuzzy term expansion by **intersecting a Levenshtein automaton with
the term FST** — cleaner and faster than trigram tiling, and it reuses the FST we
now want anyway.  Our whole term-space trigram index (the thing that just caused
3 bugs at scale) may be **replaceable** by DFA∩FST once we have an FST dictionary.
- → **Re-evaluate:** if we build the FST dictionary, prototype DFA∩FST for
  `tok~k` and `/re/` and compare against the trigram funnel.  Likely simpler and
  removes the sparsemap-set machinery entirely.  (Keep A+C trigram idea only if
  DFA∩FST loses.)

### 4. Norms: 1-byte quantized doclen + 256-entry score cache
Hard consensus (Lucene == Tantivy): quantize field length to **1 byte**
(SmallFloat float→byte), store **columnar** per doc, and precompute the whole
`k1*(1-b+b*|D|/avgdl)` factor into a **256-entry lookup table** at query start →
BM25 length-norm is one array index, no division.
- **Current pg_fts:** stores exact uint32 doclen per posting (works, but 4 bytes
  and a division per posting).  → **Quantize to 1 byte + 256-entry cache.**

### 5. Top-k: BMW default + MaxScore peer (not "flimsier"), VBMW later
BMW is the default everywhere; **MaxScore is a peer that BEATS BMW for long
queries / large k** (PISA); VBMW (variable blocks) beats fixed BMW generally.
- **Current pg_fts:** DAAT block-max WAND.  → Keep BMW default; **add MaxScore**
  for long queries/large k; VBMW as a size/latency tune.  Stop calling MaxScore
  an approximation.

### 6. Architecture: immutable segments + tombstones + tiered merge
The universal consensus, and the direct fix for the scale weaknesses we found
(in-memory monolithic build OOMs; full-rewrite merge is O(index) per merge).
- **Current pg_fts:** single monolithic index + pending list + full-rewrite
  merge.  → **Refactor to immutable segments**, each with its own FST dict,
  FOR-block postings, columnar norms, and a live-docs tombstone bitmap; a
  background **tiered merge** (Lucene TieredMergePolicy) drops tombstoned docs.
  Map tombstones onto **MVCC** — crib the seam from ParadeDB `pg_search`
  (Tantivy-segments-in-Postgres is our closest prior art).

### 7. Optional: HOPE order-preserving key encoding
HOPE (CMU) compresses term/trigram keys 2–4× while preserving order → shallower
FST, fewer cache misses.  Low-risk preprocessing layer; add after the FST lands.

## Revised architecture (target)

```
bm25 index = set of immutable SEGMENTS + a small in-RAM write buffer
  per segment:
    FST term dictionary (SuRF/FST, poppy rank/select; term -> block ptr, df, impact)
    postings: FOR-128 blocks (Δdocid, tf), PFOR-patched, per-block max-impact
    norms:   1-byte quantized doclen per doc, columnar
    live-docs tombstone bitmap (MVCC-mapped)
  writes -> RAM buffer -> flush to a new segment
  deletes/updates -> set tombstone bit
  background tiered merge -> combine small segments, drop tombstones
query top-k: BMW over segments (per-block impacts) + MaxScore for long/large-k
fuzzy/regex: Levenshtein-DFA ∩ FST  (evaluate vs the trigram funnel)
```

## Strategic decision

The scale faults (monolithic build OOM, O(index) merge, docid-set trigram
blowup) are all symptoms of the **monolithic, non-segmented** architecture.
Every production engine solved them with **segments**.  The current pg_fts is a
correct, feature-complete *reference implementation* that validated the SQL
surface, BM25 math, query language, MVCC, and WAL integration — but its storage
engine is not the consensus one and will lose at TB scale.

**Recommendation:** treat the current pg_fts as the proven *front half* (types,
parser, operators, planner integration, scoring, MVCC, WAL) and **rebuild the
storage engine underneath it** to the segmented FOR-block + FST design.  The SQL
surface and tests don't change; the AM internals do.  This is a large but
well-scoped effort, and it's the difference between "works and is correct" and
"beats the competition," which is the stated goal.

Next concrete steps (in order):
  1. **Segment container** [DONE, v2] -- immutable segments + directory +
     O(pending) flush.  Scan/stats/df/WAND all segment-aware.
  2. **Size-tiered merge** [DONE, 1.18] -- bm25_merge_segments compacts when
     the count exceeds a threshold; triggered on flush and VACUUM; old segments
     recycled to the FSM.  fts_index_nsegments() observes it.
  3. FOR-128 block posting codec with per-block max-impact, inside a segment
     (replaces delta+varint per-page blocks; sparsemap stays only for trigram
     sets).  [TODO -- mostly a size + finer-WAND-granularity win; per-page
     block-max already prunes, so this is refinement not correctness.]
  4. 1-byte quantized norms + 256-entry BM25 score cache, per segment.
     [TODO -- query-latency win: eliminates per-posting float division in the
     WAND hot path.  Needs a quantized-norm column + score cache in the cursor.]
  5. FST term dictionary (port SuRF/FST + poppy).  [TODO -- large data-structure
     port; the single biggest remaining piece.  Enables 6.]
  6. Re-evaluate fuzzy/regex as Levenshtein-DFA ∩ FST (needs the FST from 5).
     [TODO -- may retire the trigram funnel.]
  7. Add MaxScore alongside BMW.  [TODO -- peer top-k algorithm for long
     queries / large k.]
  8. THEN re-run the EC2 benchmark against tsvector/GIN, pg_search, Elasticsearch.

Progress: steps 1-3 (segments, tiered merge, FOR-128 blocks + shared posting
pages + lazy block-max WAND) and 5 (dictionary block index) COMPLETE and
qualified; step 4a (norm-constant hoist) and step 7 (BMW pivot skip) done.
Benchmarked on EC2 (2M and 10M docs vs tsvector/GIN): ranked BM25 top-k is
18-38x faster than GIN+ts_rank (advantage widens with scale), index now smaller
than GIN at 10M.  See bench/RESULTS_SEGMENTED.md.

Deferred (measured as lower-value than the wins above):
- Full FST term dict + Levenshtein-DFA fuzzy/regex (step 5/6 remainder): point
  lookups are already O(logP) via the block index; trigram funnel is correct.
- MaxScore (step 7 remainder): BMW covers the short-query common case.
- 1-byte quantized norms (step 4b): introduces score drift; the lossless
  norm-constant hoist already removed the hot-loop divisions.
- Skip-list intersection / docids-only boolean postings: to close the modest
  boolean-COUNT gap (GIN wins Q2/Q3 by 1.2-1.5x); not a BM25 engine's job.

The scale faults that triggered the rebuild (in-memory monolithic build,
O(index) full-rewrite merge, page-per-term posting bloat) are all fixed.

## Deferred work — now DONE (this round)
- **Levenshtein-automaton fuzzy** over the sorted dictionary (pg_fts_lev.c):
  exact term~k, no trigram over-generation, no heap recheck for a single fuzzy
  term.  Verified == core levenshtein.
- **MaxScore** top-k alongside BMW (dispatch at >=4 terms); exact, wins long
  queries.
- **Galloping AND** intersection for skewed selectivity (closed the AND gap vs
  GIN and vs pg_search — Q3 now ~par).
- **Exact-boolean bitmap skips heap recheck** (recheck=false for exact boolean
  and single-term DFA-fuzzy paths); MVCC still correct (bitmap heap scan does
  its own visibility).
- **Quantized norms: correctly DECLINED, not deferred** — the lossless
  norm-constant hoist already removed the hot-loop divisions, and FOR-packing +
  shared pages already made the index smaller than GIN and ~= pg_search, so
  1-byte quantization would trade real score drift for negligible size/speed.
  The benchmark (index 202MB vs pg_search 213MB) supports declining it.

## vs pg_search (ParadeDB/Tantivy) — see RESULTS_VS_PGSEARCH.md
pg_fts wins ranked top-k and selective queries (the BM25 core); pg_search wins
large-result COUNTs and large-k ranking because its Tantivy store answers those
without touching the PG heap, while pg_fts (heap-native) pays a bitmap-heap
visibility fetch.  The remaining gap is one architectural item:
index-resident visibility for counts (a custom scan/aggregate or index-only
count path), which is a larger project than the contained wins above.
