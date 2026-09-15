# pg_fts design review — lessons from scale testing

Context: EC2 testing (Fedora, m6i.2xlarge, up to 50K–100K docs) surfaced
several faults that only appear at scale.  This records what we learned and
what to change, to be merged with the algorithm research (Lucene/Tantivy/PISA
and efficient.github.io succinct structures).

## Faults found at scale (all fixed, but they point at design weaknesses)

1. **Query terms weren't analyzed like documents.** An index on
   `to_ftsdoc('english', body)` stores stemmed lexemes; a raw `'x'::ftsquery`
   kept the unstemmed term → zero matches.  Fixed with
   `to_ftsquery(regconfig, text)`.
   *Lesson:* analysis config is part of the index contract; the query path must
   share it.  A cleaner design binds the analyzer to the index (in reloptions /
   a catalog), so queries can't use the wrong one.  **Consider: store the
   text-search config OID in the bm25 metapage; `@@@` normalizes automatically.**

2. **Trigram index stored docid-sets per trigram → segfault + bloat.**
   A common trigram covered most of the corpus; its sparsemap spanned pages and
   crashed GenericXLogFinish.  Root cause: wrong universe.  Fixed by inverting
   to **term-ordinal sets** (vocabulary is small).
   *Lesson:* always store sets over the *small* universe.  Vocabulary ≪ corpus.

3. **Fixed-buffer sparsemap silently truncated (sm_add ENOSPC).**
   High-cardinality sets lost members → wrong fuzzy results (111 vs 423).
   Fixed with `sm_create`/`sm_add_grow`.
   *Lesson:* never write a growable structure into a fixed buffer without
   checking the grow return; prefer library-owned growth then serialize.

## Structural weaknesses the testing exposed (not yet addressed)

A. **In-memory build.** `bm25_build` collects ALL postings in a hash in
   backend memory before writing.  At TB scale this OOMs.  Lucene/Tantivy/PISA
   all do **external (spill-to-disk) segment builds**.  → Adopt a tuplesort- or
   spill-based build; build fixed-size immutable **segments** and merge them,
   rather than one monolithic structure.

B. **Whole-list-into-memory reads in a few paths.** The WAND cursor is lazy
   now, but `bm25_universe` and the merge still materialize large arrays.
   → Everything hot must stream page-at-a-time.

C. **Merge rewrites the entire index.** `bm25_merge_pending` reads the whole
   dictionary + all postings + pending and rewrites everything.  O(index) per
   merge → quadratic under steady insert load.  → Segment model with tiered
   merge (merge small segments together, touch big ones rarely), like Lucene's
   TieredMergePolicy.

D. **Posting codec is delta+varint only.** Competitive engines use
   SIMD-friendly block codecs (PForDelta / SIMD-BP128) or Elias-Fano.  → Decide
   from the research which codec; keep block-max impacts per block for WAND.

E. **Term dictionary is a linear-scan-within-page sorted list.** Fine for
   exact lookup on small dicts; poor for large vocabularies and prefix/range.
   → An **FST (finite state transducer)** term dictionary (as Lucene & Tantivy
   use) gives compact storage + fast prefix + is the natural home for the
   trigram/fuzzy path.  efficient.github.io's **SuRF** may help range/prefix
   filtering.

F. **Trigram index still per-build monolithic** and rebuilt on every merge.
   Should live per-segment like the postings.

## The strategic question the research must answer

Right now pg_fts is a *single monolithic index* (build-once + pending list +
full-rewrite merge).  Every scale weakness above (A, C, F) traces to that.
The consensus design in Lucene/Tantivy/PISA is **immutable segments +
background tiered merge**, with each segment holding its own FST dictionary,
block-compressed postings with per-block max impacts, and columnar norms.

Decision to make after the research lands:
  - Refactor pg_fts toward a **segmented** architecture (bigger change, but it
    is what makes TB-scale and steady-write workloads viable), OR
  - Keep the monolithic model and only harden the build/merge (spill-to-disk
    build, incremental merge) — cheaper, but likely still loses to Tantivy on
    write-heavy and very large corpora.

The benchmark exists to decide this with numbers, but the scale faults already
argue strongly for segments.  Merge the research recommendations here, then
pick the codec + dictionary + merge policy and re-plan the implementation.
