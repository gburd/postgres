# pg_fts capability / production-readiness matrix

BM25 full-text search index access method (`bm25`) for PostgreSQL.

Every claim below is grounded in the source under `contrib/pg_fts/`. File:line
citations are to the tree this document was generated against. All
`IndexAmRoutine` flags cited are from the `bm25_handler` function in
`pg_fts_am.c`.

## Capability matrix

| Capability | Supported? | Evidence |
|---|---|---|
| `@@@` boolean / phrase / NEAR / prefix / fuzzy / regex match | Yes | opclass strategy 1, `pg_fts--1.2--1.3.sql:19`; `@@@` operators `pg_fts--1.0.sql:108,117`; bitmap scan `bm25_getbitmap` `pg_fts_am_scan.c:1486` |
| `<=>` relevance ordering scan (`ORDER BY d <=> q LIMIT k`, no Sort) | Yes | `amcanorderbyop=true` `pg_fts_am.c:2112`; `<=>` operators + `FOR ORDER BY` `pg_fts--1.15--1.16.sql:18,25,35`; `bm25_gettuple` block-max WAND/MaxScore `pg_fts_am_scan.c:1147` |
| BM25 (Okapi) scoring, index-maintained corpus stats (N, avgdl, df) | Yes | `fts_bm25` `pg_fts--1.1--1.2.sql` (stage 1.2); `fts_index_stats`/`fts_index_df` `pg_fts--1.6--1.7.sql:7,15`; metapage `meta->ndocs/sumdoclen` `pg_fts_am.c:1571-1583` |
| BM25 variants (lucene, robertson, atire, bm25+) | Yes | `fts_bm25_opts` (stage 1.4), README lines 24 |
| BM25F multi-field weighting | Yes | `fts_bm25f(ftsdoc[], ftsquery, weights, ...)` `pg_fts--1.11--1.12.sql:9` |
| Phrase queries (`"a b c"`) via per-term positions | Yes | ftsdoc format v2 (stage 1.9), README line 28; `sql/pg_fts.sql:206-215` |
| Prefix (`term*`), fuzzy (`term~k`), regex (`/re/`) | Yes | README lines 31-34; sequential + index paths, `sql/pg_fts.sql:147-271`; trigram pre-filter `pg_fts_trgm_index.c` |
| Highlight / snippet | Yes | `fts_highlight`, `fts_snippet` `pg_fts--1.4--1.5.sql:6,13` |
| Fast bulk count (`fts_count(regclass, ftsquery)`) | Yes | `pg_fts--1.18--1.19.sql:9`; visibility-map-aware, heap probed only for not-all-visible pages |
| MVCC-correct deletes (tombstones) | Yes | `bm25_bulkdelete` per-segment livedocs tombstone bitmap `pg_fts_am.c:1843-1853`; scans/counts subtract, merge drops |
| Oversized-document handling | Yes | `bm25_insert_oversized_as_segment` one-doc segment (no per-doc size cap) `pg_fts_am.c:1521,1552-1560` |
| WAL-logged / crash-safe / physical-replication safe | Yes | every page write via GenericXLog (14 `GenericXLogStart` cycles: 12 in `pg_fts_am.c`, 2 in `pg_fts_trgm_index.c`); no raw `XLogInsert`/`log_newpage`/`MarkBufferDirty`/`PageSetLSN`/`smgrwrite` anywhere (grep: 0 matches); header `pg_fts_am.c:26-28` |
| tsquery -> ftsquery migration | Yes (partial: helper, not transparent) | `tsquery_to_ftsquery` `pg_fts_migrate.c:130` + ASSIGNMENT cast `pg_fts--1.5--1.6.sql:8-15` |
| CREATE INDEX CONCURRENTLY / REINDEX CONCURRENTLY | Yes (verified empirically) | `aminsert` routes all concurrent writes to the pending list (immediately searchable) `pg_fts_am.c:1515-1520,1521`; see Q1 |
| Index-only / covering scan (IOS) | No | `amcanreturn = bm25_canreturn` returns `false` `pg_fts_am_scan.c:1101-1114`; `amcaninclude=false` `pg_fts_am.c:2131`; non-covering (stores postings, not the ftsdoc) |
| Parallel index build (PARALLEL workers) | Yes | amcanbuildparallel=true; parallel heap scan, per-worker segment flush, leader merge |
| Parallel scan | No | `amcanparallel=false` `pg_fts_am.c:2127`; `amestimateparallelscan/aminitparallelscan/amparallelrescan = NULL` `pg_fts_am.c:2161-2163` |
| Parallel VACUUM | No | `amparallelvacuumoptions = VACUUM_OPTION_NO_PARALLEL` `pg_fts_am.c:2133` |
| Unique / multicolumn / ordered-btree / clusterable | No | `amcanunique=false` `:2119`, `amcanmulticol=false` `:2120`, `amcanorder=false` `:2111`, `amclusterable=false` `:2125` |
| NULL / optional-key indexing | No | `amsearchnulls=false` `:2123`, `amoptionalkey=false` `:2121` (a NULL ftsdoc is not indexed: `bm25_insert` returns early on `isnull[0]` `pg_fts_am.c:1531`) |
| Predicate locks (SSI) | No | `ampredlocks=false` `pg_fts_am.c:2126` |
| Ranked scan over unflushed pending docs | No (partial) | `<=>`/`fts_search` cover merged segments only; pending docs matched by `@@@`/counted by `fts_count` but ranked only after a flush `pg_fts_am_scan.c:2519-2525` |
| Faceting / aggregation Custom Scan pushdown | No | none in tree; only `fts_count` count-pushdown exists |
| Impact-ordered postings | No | postings are docid-ordered (block-max WAND); listed as future work, README lines 62-64 |
| Storage AIO / read_stream prefetch | No (build heap scan gets core AIO free) | 0 `read_stream`/`StartReadBuffers` sites; `nextblk` pointer-chains defeat prefetch; see Q6 |
| pg_repack of the table | N/A (table-level tool) | see Q3 — pg_fts offers VACUUM+`fts_merge()` and REINDEX for in-place compaction |

---

## Answers

### 1. Concurrent index builds (CIC / REINDEX CONCURRENTLY)

**Yes — verified empirically.** `amcanbuildparallel=true` enables parallel
CREATE INDEX (a separate capability from) the two-phase concurrent build. What
makes CIC correct here is that `aminsert`
(`bm25_insert`, `pg_fts_am.c:1521`) always routes a new document to the
**pending write buffer** — "stored verbatim ... and is searched directly at
scan time, so newly inserted rows are immediately visible to `@@@` without a
REINDEX" (`pg_fts_am.c:1515-1520`). Oversized documents that will not fit a
pending page take the equivalent path as a one-document segment
(`bm25_insert_oversized_as_segment`, `pg_fts_am.c:1552-1560`). So writes that
arrive during the build's validate phase are captured by the index and found by
the subsequent scan; the build (`bm25_build`, `pg_fts_am.c:1405`) does a
standard `table_index_build_scan` and never disables inserts. Both CREATE INDEX
CONCURRENTLY and REINDEX CONCURRENTLY have been verified to work.

### 2. Index-only scans and the count tradeoff

**No index-only scan — this is by design and drives the count strategy.**
`amcanreturn = bm25_canreturn` returns `false` (`pg_fts_am_scan.c:1101-1114`):
the index is **non-covering** because it stores *analyzed postings* (terms,
term frequencies, positions, doc lengths), not the original `ftsdoc`, so it
cannot reproduce a column value. `amcaninclude=false` (`pg_fts_am.c:2131`), so
there is no covering `INCLUDE` either.

Consequence for counting: `count(*)`/`EXISTS` need no attribute but the planner
still includes the `@@@` restriction column in the IOS coverage check, so with
`amcanreturn=false` they fall back to a **bitmap (or plain index) scan** — every
matching TID is visited (`bm25_canreturn` comment, `pg_fts_am_scan.c:1106-1113`).
The **fast count** is therefore the explicit
`fts_count(regclass, ftsquery)` (`pg_fts--1.18--1.19.sql:9`), which counts
matches in bulk from the index using the visibility map, probing the heap only
for not-all-visible pages — no per-tuple executor round-trips.

Tradeoff: you trade IOS convenience (transparent `count(*)` over the index) for
a smaller, ranking-ready index (no stored source doc) plus an explicit,
MVCC-correct bulk-count primitive. Callers wanting a fast count must call
`fts_count` rather than relying on the planner picking an index-only `count(*)`.

### 3. REPACK / pg_repack / in-place compaction

**pg_repack does not apply to an index AM** — it rewrites the *table*, which is
orthogonal to `bm25`. What pg_fts offers for compaction:

- **VACUUM + `fts_merge()`** — the size-tiered segment merge. VACUUM's
  `amvacuumcleanup` folds pending docs into a segment and merges;
  `fts_merge(regclass)` (`pg_fts--1.12--1.13.sql:9`) forces it on demand.
  `bm25_merge_segments` (`pg_fts_am.c:1337`) coalesces similarly-sized segments
  and **physically drops tombstoned docs** (`pg_fts_am.c:25`,
  `bm25_bulkdelete` comment `pg_fts_am.c:1848-1851`).
- **REINDEX / REINDEX CONCURRENTLY** — full rebuild.

Honest gap: the merge leaves superseded blocks unreferenced; they are reclaimed
only by REINDEX ("Old blocks are left unreferenced and reclaimed by REINDEX (a
page recycler is future work)", `pg_fts--1.12--1.13.sql:6-8`). So `fts_merge()`
compacts *logical* content (fewer segments, tombstones gone) but does not shrink
the physical file — REINDEX is the only way to reclaim that space. There is **no
online index REPACK beyond VACUUM+`fts_merge()` and REINDEX**.

### 4. Feature parity vs. pg_search/Tantivy, ZomboDB/Elasticsearch, tsvector/GIN

**HAS:**
- BM25 (Okapi) + variants (lucene/robertson/atire/bm25+) and **BM25F**
  multi-field weighting (`fts_bm25f`, `pg_fts--1.11--1.12.sql:9`).
- Rich query language: boolean, phrase `"a b c"`, `NEAR(...)`, prefix `term*`,
  fuzzy `term~k`, regex `/re/` (README 28-34; `sql/pg_fts.sql:147-271,416-426`).
- `<=>` relevance **ordering index scan** (no Sort) via block-max WAND /
  MaxScore (`pg_fts_am_scan.c:1147`, `pg_fts--1.15--1.16.sql:35`).
- Fast MVCC-correct bulk count `fts_count` (`pg_fts--1.18--1.19.sql:9`).
- Highlight / snippet (`fts_highlight`, `fts_snippet`, `pg_fts--1.4--1.5.sql`).
- MVCC-correct tombstone deletes (`bm25_bulkdelete`, `pg_fts_am.c:1843`).
- Oversized-document handling (one-doc segments, `pg_fts_am.c:1552-1560`).
- Full WAL logging via GenericXLog → crash recovery + physical replication
  safety, no custom resource manager (`pg_fts_am.c:26-28`; 0 raw-write sites).

**HONEST GAPS:**
- No parallel scan (`amcanparallel=false`, `pg_fts_am.c:2127`; parallel-scan
  hooks all `NULL`, `:2161-2163`) → **single-threaded query execution**.
- No index-only / covering scan (`amcanreturn`→false, `amcaninclude=false`).
- No faceting / aggregation Custom Scan pushdown (only count-pushdown exists).
- No impact-ordered postings — docid-ordered only (README 62-64).
- `<=>` / `fts_search` ranking does not cover unflushed pending docs until a
  flush (`pg_fts_am_scan.c:2519-2525`).

Versus Elasticsearch/Tantivy this is a single-node, single-threaded-per-query
engine with no distributed aggregation; versus tsvector/GIN it adds real BM25
ranking, index-maintained corpus stats, and a `<=>` ordering scan that GIN
cannot provide, at the cost of GIN's parallel-scan and mature-tooling maturity.

### 5. Drop-in replacement for a tsvector/pg_textsearch system under logical replication?

**No — it is a re-platform, not a drop-in.** Three reasons, all code-backed:

**(a) Different API, no transparent shim.** pg_fts uses `ftsdoc`/`ftsquery` with
`@@@` (`pg_fts--1.0.sql:108`) and `<=>` (`pg_fts--1.15--1.16.sql:18`), not
`tsvector`/`tsquery`/`@@`. Ranking is `fts_bm25`/`<=>`, not `ts_rank`. There is
a **migration helper** `tsquery_to_ftsquery()` (`pg_fts_migrate.c:130`, faithful
`&→AND`, `|→OR`, `!→NOT`, `<N>→FTS_OP_PHRASE` preserving the gap) and an
ASSIGNMENT **cast** (`pg_fts--1.5--1.6.sql:14-15`) so existing tsquery values
flow into `@@@`, but there is **no transparent operator/type shim** — queries,
index DDL (`USING bm25 (to_ftsdoc(...))`) and ranking calls must be rewritten.

**(b) Logical replication does not replicate indexes.** Under logical
replication the subscriber maintains its **own** indexes; a subscriber must have
pg_fts installed and its own `bm25` index provisioned. This is no worse than GIN
(indexes are never logically replicated), but it is a per-subscriber
provisioning step, not automatic.

**(c) Physical replication + crash recovery ARE safe.** Every page write goes
through GenericXLog (14 `GenericXLogStart` cycles; zero raw-WAL/buffer-dirty
sites), so the index is fully WAL-logged and replicated on a physical standby
with no custom resource manager (`pg_fts_am.c:26-28`).

Bottom line: physical replicas and crash recovery are covered transparently;
moving a tsvector/GIN workload to pg_fts is a deliberate migration (rewrite
queries/DDL, provision the index per subscriber for logical replication), not a
transparent swap.

---

*Note on a premise correction:* the actual WAL write-site count is **14
`GenericXLogStart` cycles** (12 in `pg_fts_am.c`, 2 in `pg_fts_trgm_index.c`),
not 27 — but the underlying claim holds and is stronger: **100% of page
mutations go through GenericXLog**, with zero raw `XLogInsert`, `log_newpage`,
`MarkBufferDirty`, `PageSetLSN`, or `smgrwrite`/`smgrextend` sites (grep: 0
matches).

### 6. Asynchronous I/O (AIO / read_stream)

**pg_fts issues no storage AIO of its own; the one place it matters already
gets it from core.** Every index-side read is a plain synchronous
`ReadBuffer` + `LockBuffer` (grep for `read_stream`/`StartReadBuffers`/
`PrefetchBuffer` across `contrib/pg_fts/`: 0 matches; the only `prefetch` hits
are `__builtin_prefetch` CPU cache-line hints in vendored sparsemap, unrelated
to storage AIO).  The build's heap scan runs through
`table_index_build_scan` → `heap_getnextslot` → `read_stream_next_buffer`, so
`CREATE INDEX`'s heap side is **already streamed/prefetched by core** with no
pg_fts code.

*Could it?* Only in one path with real payoff.  Every pg_fts on-disk structure
is a `nextblk` linked list (`BM25PageOpaqueData.nextblk`), so the next block is
known only *after* the current page is read — the classic pointer-chase that
`read_stream` cannot prefetch.  The hot query path (block-max WAND) is
*anti*-prefetch by design: it reads block headers precisely to **skip** blocks
without reading their payload, so prefetching would fetch pages it means to
skip.  The only cheap win is the **cold merge full-scan**
(`bm25_read_segment_into`), which reads every posting page end-to-end: those
pages are written as one contiguous run per segment, so recording a
`[firstblk,lastblk]` range in `BM25SegMeta` would let a trivial `blk++`
`read_stream` callback prefetch the merge.

*Should it?* Not for a warm-cache OLTP search workload (a handful of resident
pages per selective query; AIO adds setup cost with no I/O to hide).  A bounded,
low-effort win exists for cold TB-scale merges *if* a cold-merge I/O bottleneck
is actually measured — deferred until then.


*AIO for the parallel-merge WRITES?* Considered and rejected on two grounds.
(1) No API: pg_fts writes every page through shared buffers + GenericXLog (a
WAL/MVCC/crash-safety requirement), and this tree's buffer manager exposes AIO
for reads only (aio_shared_buffer_readv_cb; there is no buffer-manager AIO
write path -- FlushBuffer is synchronous).  Using the low-level
pgaio_io_start_writev would mean bypassing shared buffers with raw smgr writes,
breaking the GenericXLog invariant the design rests on.  (2) It would not help:
the merge tail measured at 2M is CPU-bound (one backend decoding + re-encoding
postings; workers=0 with the index resident in a 32 GB shared_buffers, so the
writes are absorbed by shared buffers and flushed lazily by the checkpointer --
no write I/O wait to hide).  AIO accelerates I/O wait, not CPU-bound re-encode.
The real lever for the merge tail is the same as the ranked-query gap: a cheaper
posting codec (format v3), not asynchronous writes.