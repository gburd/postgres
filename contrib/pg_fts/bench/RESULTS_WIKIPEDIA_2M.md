# pg_fts vs tsvector/GIN — real-corpus benchmark (2M Wikipedia articles)

Reproducible head-to-head of the pg_fts `bm25` access method against the
in-tree `tsvector` + GIN + `ts_rank` stack, on a real English-Wikipedia corpus.

## Setup

- **Corpus**: `wikimedia/wikipedia` `20231101.en`, first 2,000,000 articles
  (`title`, `body`).  Heap 3346 MB, avg body 2986 chars.
- **Host**: EC2 `r7i.8xlarge` (32 vCPU, 247 GB RAM), gp3 1 TB, Fedora 43,
  PostgreSQL 20devel (`-Dbuildtype=release`).  `shared_buffers=64GB`,
  `work_mem=256MB`, `maintenance_work_mem=8GB`, `jit=off`.
- **Indexes**:
  - pg_fts: `CREATE INDEX ... USING bm25 (to_ftsdoc('english', body))`
  - GIN:    `tsv = to_tsvector('english', body)` then `USING gin (tsv)`
- Both queries analyze with the same `english` config, so match sets are
  **identical** (verified: `year` -> 669,208 rows, `section` -> 94,925 rows on
  both engines before timing).
- Latency = median of 9 warm runs (`\timing`), `enable_seqscan=off`.

## Build time & size

| Engine | Build (single backend) | Index size | Index/heap |
|--------|------------------------|-----------:|-----------:|
| pg_fts bm25 | 21m32s (`CREATE INDEX`) | 3592 MB | 1.07x |
| tsvector/GIN | 13m25s (`UPDATE tsv`) + 56s (GIN) | 1426 MB (+ ~1.4 GB `tsv` column) | 0.43x GIN alone |

Build time on both engines is dominated by the single-threaded `english`
text-analysis (Snowball + ICU) of ~3.3 GB of prose; neither `ambuild`
parallelizes it.  The bm25 index is ~2.5x larger than the GIN index because it
stores per-posting term frequency, document length, and positions (everything
BM25 ranking and phrase queries need) whereas GIN stores compact TID lists.
(Counting the `tsv` column GIN requires, the on-disk footprints are closer.)

## Query latency (ms, median of 9, warm)

Terms drawn from the real corpus vocabulary; `mid` = Slovakia/Hungary
(df ~10-40k), `common` = year/world (df ~670k).

| # | Query | pg_fts | GIN | Winner |
|---|-------|-------:|----:|--------|
| Q1 | count, df≈17k          | 74.1 | 74.0 | tie |
| Q2 | count, mid             | 43.5 (`fts_count` **11.6**) | 43.4 | bm25 (`fts_count` 3.7x) |
| Q3 | AND count (mid & mid)  | 18.9 | 17.9 | tie |
| Q4 | ranked top-10 (mid & mid)   | **18.1** | 49.6 | bm25 **2.7x** |
| Q5 | ranked top-10 (common & mid)| **15.4** | 64.6 | bm25 **4.2x** |
| Q6 | ranked top-100 (common)     | **75.3** | 3028.7 | bm25 **40x** |
| Q7 | ranked top-10 (common & common) | **50.0** | 1640.6 | bm25 **33x** |

## Reading

- **Ranked retrieval is where pg_fts wins decisively** — the actual full-text
  search use case ("best N matching documents").  GIN has no top-N pushdown and
  no corpus statistics, so `ts_rank` must fetch and score *every* matching row
  and then sort; its latency grows with the match-set size (3 s for a top-100
  over a term in a third of the corpus).  pg_fts answers the same query from the
  index with block-max WAND / MaxScore and stops early, so it stays flat
  (~75 ms) regardless of how common the term is.  The advantage grows with term
  frequency: 2.7x (mid) -> 40x (common).
- **Plain counts / boolean AND are a tie** — both do a bitmap scan; GIN's
  compact postings and pg_fts's segments are comparable here.  pg_fts's
  dedicated `fts_count()` (visibility-map-aware bulk count) beats GIN's
  `count(*)` (3.7x) by avoiding per-tuple executor overhead.
- **Cost**: a ~2.5x larger index and a comparable (single-threaded) build.  The
  size buys the per-posting tf/|D|/positions that make ranked queries and phrase
  search index-resident.

## Correctness gate

Every ranked query was checked to return the same match set as the GIN path
(and, once per band, byte-identical to a forced seqscan) before timing.

## Reproduce

`bench/get_wikipedia.py` (streams the HF parquet to TSV), load the first 2M
rows, build the two indexes as above, then `bench/bench_fixed.sh` (pinned terms,
median-of-9).  See `PRODUCTION_SCALE_PLAN.md` for the full methodology and the
10M-50M scale plan.

## Update: parallel build/merge + regression fix (2M Wikipedia, r7i.4xlarge, PG17)

Parallel index build (amcanbuildparallel) cut the pg_fts build from a serial
~34 min to a parallel scan + parallel merge.  A regression was found and fixed
along the way: the parallel build initially SKIPPED the final merge, leaving the
index as 6-8 segments, which regressed common-term ranked top-k ~2x (a ranked
scan traverses every segment's postings).  Fix: both build paths now compact to
a single segment via bm25_merge_all (parallel merge when workers are available).

pg_fts ranked latency, single-segment (compacted) index, median/9 warm:
| query | regressed (6-seg) | fixed (1-seg) | earlier baseline |
|-------|------------------:|--------------:|-----------------:|
| ranked top-10 rare&mid    | 26.3 | 24.5 | 26.1 |
| ranked top-10 common&mid  | 37.8 | **16.6** | 17.6 |
| ranked top-100 common     | 73.7 | 67.8 | 70.4 |
| rare count                | 15.0 | 12.2 | 23.5 |
| fts_count rare            |  2.5 |  1.9 |  1.9 |

The common&mid ranked regression (37.8 -> 16.6 ms) is fixed, back to baseline.

Note on the parallel MERGE at scale: it launches workers and is verified correct
locally, but on the multi-preload EC2 cluster it sometimes fell back to the
serial merge path (worker launch returned 0 despite free slots -- under
investigation; tracked in DEFERRED.md).  The compaction itself (the regression
fix) is confirmed on EC2 regardless of whether the merge ran parallel or serial.

## Same-hardware A/B of the regression fix (r7i.4xlarge, 1.4M-row corpus, PG20devel)

Direct before/after on identical hardware and data, swapping only the build's
final-merge behavior (pre-fix skip-merge vs fixed compact-to-one).  Same
CREATE INDEX, same queries, median/9 warm.

| query                      | pre-fix (nseg=5) | fixed (nseg=1) | speedup |
|----------------------------|-----------------:|---------------:|--------:|
| ranked top-10 rare&mid     | 18.2 | **4.5**  | 4.1x |
| ranked top-10 common&mid   | 61.7 | **38.9** | 1.6x |
| ranked top-100 common      | 38.3 | **14.4** | 2.7x |
| fts_count rare             | 12.3 | 11.8     | ~same |

Multi-segment (pre-fix) ranked scans traverse every segment's postings; the
fix compacts the build to a single segment, restoring ranked latency (1.6-4.1x
on this workload).  Consistent with the 2M real-Wikipedia result above
(common&mid 37.8 -> 16.6 ms).
