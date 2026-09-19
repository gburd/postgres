# pg_fts vs ParadeDB pg_search — real-corpus head-to-head (2M Wikipedia)

Honest re-run of the pg_search comparison on a **real** corpus (the prior
`RESULTS_VS_PGSEARCH.md` used synthetic Zipfian).  Same cluster, same corpus,
warm, VACUUMed.

## Setup

- EC2 r7i.4xlarge (16 vCPU, 123 GB), PostgreSQL **17.10** built from source.
- pg_fts 1.19 (sparsemap v5.2.0) in `db_fts`; pg_search **0.24.1** (Tantivy) in
  `db_search` (separate DBs — both register a `bm25` AM, cannot coexist).
- Corpus: `wikimedia/wikipedia 20231101.en`, first **2,000,000** articles.
- pg_fts: `USING bm25 (to_ftsdoc('english', body))`.
  pg_search: `USING bm25 (id, title, body) WITH (key_field='id')`.
- Tokenizers differ (PG `english`/Snowball vs Tantivy `en_stem`), so match sets
  differ by ~0.4% (e.g. "slovakia": pg_fts 10256 vs pg_search 10219).  Fine for
  latency; not a correctness comparison.
- Latency = median of 9 warm runs.

## Index size

| | pg_fts | pg_search |
|--|-------:|----------:|
| index size | **3590 MB** | 5574 MB |

On real text pg_fts is **1.55x smaller** (opposite of the synthetic run where
they tied ~204/213 MB — Tantivy stores more per doc for natural-language text).

## Query latency (ms, median of 9, warm)

| # | Query | pg_fts | pg_fts `fts_count` | pg_search | Winner |
|---|-------|-------:|-------------------:|----------:|--------|
| Q1 | rare count (10k)      | 23.5 | **1.9** | 9.1 | fts_count |
| Q2 | mid count (75k)       | 44.9 | **2.4** | 9.8 | fts_count |
| Q3 | common count (678k)   | 303.6 | 41.0 | **13.8** | pg_search |
| Q4 | AND count (rare&mid)  | 11.6 | — | **9.3** | pg_search (close) |
| Q5 | ranked top-10 (rare&mid)   | 26.1 | — | **9.0** | pg_search 2.9x |
| Q6 | ranked top-10 (common&mid) | 17.6 | — | **9.6** | pg_search 1.8x |
| Q7 | ranked top-100 (common)    | 70.4 | — | **8.5** | pg_search 8.3x |

## Honest reading

**pg_search wins ranked retrieval and common-term work; pg_fts wins only
selective counts, and only via the explicit `fts_count()`.**  This is a
tougher result than the synthetic-Zipfian run, because real text has a natural
long-tail vocabulary that plays to Tantivy's design.

Root causes (all previously identified, confirmed on real data):

1. **pg_search ranked latency is ~flat (~9 ms) regardless of term frequency**;
   pg_fts's block-max WAND degrades as terms get common (Q5 26 -> Q7 70 ms).
   The gap is **impact-ordered postings** (pg_search can stop early); pg_fts
   decodes docid-ordered postings.  Confirmed pg_search is 8.3 ms even with
   `max_parallel_workers_per_gather=0`, so this is the codec, not parallelism.
2. **pg_search parallelizes queries** (Parallel Custom Scan, 2 workers); pg_fts
   is single-threaded (`amcanparallel=false`).
3. **pg_search count pushdown** (Custom Scan aggregate, `actual rows=1`) answers
   common-term counts in 14 ms; pg_fts's transparent `count(*)` is a Bitmap Heap
   Scan that visits every matching heap block (677k rows / 192k blocks = 304 ms),
   because a bitmap scan always touches the heap.  pg_fts's `fts_count()` avoids
   this with a visibility-map bulk count and is the fastest of all on selective
   terms (1.9-2.4 ms) but still pays per-block on common terms (41 ms).

## Where pg_fts stands

pg_fts is a fully heap-native, WAL-logged (GenericXLog), MVCC-correct PG index
that is smaller on disk and has the fastest selective count (`fts_count`).  It
trails pg_search on the headline ranked-retrieval path.  Closing that needs the
two documented codec investments — impact-ordered postings and a COUNT/aggregate
Custom Scan pushdown — plus parallel scan/build.  These are architecture work,
not tuning.
