# Quasi-Production-Scale FTS Benchmark Plan (pg_fts vs pg_search vs tsvector/GIN)

Purpose: qualify pg_fts against the field at **real, multi-GB scale (10M-50M
docs)** on a single large EC2 box, using a query mix and metrics that match how
the leading engines publish their numbers. This complements the smaller-scale
`BENCHMARK_PLAN.md` / `STRATEGY_REPLAN.md` runs (Zipfian synthetic, 2M/10M)
already committed here; it adds a **real corpus**, a **complete query mix**, and
**concurrency/QPS** the existing runners don't cover.

Read this alongside:
- `BENCHMARK_PLAN.md` — axes, instance sizing, run order, cost.
- `run_latency.sh`, `phase_latency.sh` — existing p50/p99 runners (serial).
- `gen_corpus.sql` — the synthetic Zipfian generator (keep for A/B smoke).
- `ndcg.py` — relevance scorer (BEIR/MS MARCO qrels).
- `RESULTS_SEGMENTED.md`, `RESULTS_VS_PGSEARCH.md` — prior numbers to beat.

---

## Part A — How the leading solutions benchmark (methodology survey)

Grounding for the plan. Each row is "what they measure and on what," so our
numbers are comparable to theirs.

### 1. ParadeDB pg_search (Tantivy-backed)

Their public benchmark (`paradedb/paradedb` repo, `benchmarks/` + the
"Postgres vs Elasticsearch" blog posts):

- **Corpus.** Two families:
  - A **generated "logs"/"mock" dataset** (`benchmark-eslogs` / the ClickBench-
    style `hits` set, and a synthetic log generator) — tens of millions of rows,
    each a JSON-ish log line with a message body + structured fields. They run
    it at **~10M-100M rows / tens of GB**.
  - **Real corpora** for relevance/latency: a Wikipedia-derived set and the
    **StackOverflow / GitHub archive** style text. Their headline "40x faster
    than tsvector" post used a single-node table in the **tens of millions of
    rows**.
- **Query mix.** Single-term match; boolean AND/OR; **phrase**; **fuzzy**;
  **ranked top-N** (`ORDER BY paradedb.score(id) DESC LIMIT N`, N=10 and N=100);
  **faceting/aggregation** (their `Custom Scan (ParadeDB Aggregate Scan)` pushes
  COUNT/GROUP BY into the index — this is the path that beat us on large COUNTs
  in `RESULTS_VS_PGSEARCH.md`); **counts** (`SELECT count(*) ... WHERE @@@`).
- **Metrics.** Latency (they report **mean/median** and sometimes p95),
  **throughput QPS** under a concurrent client driver, **index build time**, and
  **index size on disk**. They emphasize the count/aggregate pushdown and
  large-N ranking, which is exactly where a heap-native engine pays a visibility
  fetch.

### 2. Tantivy's own benchmarks (tantivy / search-benchmark-game)

- **Corpus.** The **English Wikipedia** article dump (the `wikipedia` corpus in
  `tantivy-search/search-benchmark-game`), ~**5M-6M docs, ~10-40 GB** of text
  depending on whether it's abstracts or full articles. Also a smaller
  `enwiki` subset for CI.
- **Query mix** (the benchmark-game "commands"): `COUNT` (matching docs),
  `TOP_10` and `TOP_100` (ranked, no count), `TOP_10_COUNT` (ranked + total
  count), **intersection (AND)**, **union (OR)**, **phrase**. Queries are drawn
  from a real query log of common terms so term frequencies are realistic.
- **Metrics.** Primarily **throughput** (queries/sec) and per-query latency; the
  game reports a table of engines x query-type. Index size and build time are
  reported per engine. This is the canonical "which engine is fastest at each
  query shape" layout — mirror it.

### 3. Elasticsearch / Lucene nightly benchmarks (esrally + rally-tracks)

- **Tracks / corpora** (representative scales they treat as production):
  - **`pmc`** — PubMed Central full-text articles, ~**574k docs, ~5.5 GB** (long
    documents; good for scoring + phrase).
  - **`nyc_taxis`** — ~**165M rows, ~75 GB** (structured + range/agg heavy;
    the "big" track).
  - **`geonames`** — ~**11M docs, ~3 GB** (mixed match + term + agg).
  - **`http_logs`**, **`nested`**, **`so`** (StackOverflow) also used.
- **Query types per track (`operations`/`challenges`).** term, phrase, boolean,
  range, **aggregations (date_histogram, terms agg, cardinality)**, sorted
  results, `scroll`, and **paginated top-N**. Each operation runs at a fixed
  **target throughput** and Rally records **service time p50/p90/p99/p100** and
  **throughput (ops/s)**, plus **index size**, **merge time**, and **indexing
  throughput (docs/s)**.
- **Method to copy:** run each operation at a *fixed target rate* with a set
  number of clients, warm the page cache first, and report the full percentile
  distribution — not just a mean.

### 4. Postgres tsvector/GIN as the baseline (how ParadeDB/pgvector posts frame it)

- The GIN baseline in every "Postgres FTS is slow" post is:
  `... WHERE tsv @@ to_tsquery('english', q) ORDER BY ts_rank(tsv, q) DESC
  LIMIT N` for ranked top-N, and `SELECT count(*) ... WHERE tsv @@ q` for counts.
- The known GIN weakness they exploit: **`ts_rank` must fetch and score every
  match then sort** (no top-N pushdown, no corpus stats), so ranked latency
  grows with the match-set size. Our own `RESULTS_SEGMENTED.md` confirms this
  (GIN ranked top-10 at 10M: 456 ms vs pg_fts 11.9 ms). Counts, by contrast, are
  GIN's strength (compact TID postings + bitmap AND), so it wins or ties there.
- Baseline is always run **warm** and **after `VACUUM ANALYZE`**, same as we do.

### 5. Standard IR corpora practical to obtain on an EC2 box

Ranked by "fastest to get a real, multi-GB corpus running":

| Corpus | How to get | Docs | Size | Best for |
|---|---|---|---|---|
| **Wikipedia (Cohere/wikipedia or wikimedia/wikipedia, HF)** | `datasets`/`huggingface_hub` streaming download; already chunked into `title`+`text` | ~6M (en) up to ~60M (chunked passages) | ~20-90 GB | latency, indexing, scale — **primary** |
| **Wikipedia abstracts (enwiki-latest-abstract.xml)** | one `wget` from dumps.wikimedia.org, ~1 GB gz | ~6.6M | ~6 GB xml | fast smoke, short docs |
| **MS MARCO passages** | `wget` from microsoft.github.io/msmarco or HF `ms_marco` | ~8.8M passages | ~3 GB | relevance at scale + latency |
| **BEIR (nfcorpus/scifact/fiqa)** | `beir` pip / HF; tiny | 3.6k / 5k / 57k | MB | relevance (NDCG@10, Recall@100) only |
| **`pmc` (Rally track)** | `esrally` data or PMC OA bulk | 574k | 5.5 GB | long-doc phrase/scoring cross-check vs ES |
| **Synthetic Zipfian (`gen_corpus.sql`)** | already here, pure SQL, no download | any | any | controllable IDF, CI smoke, term-freq control |

**Recommendation for the production run: Wikipedia (HF `wikimedia/wikipedia`,
`20231101.en`), replicated to the target row count.** It is real English text
with a natural Zipfian vocabulary, downloads in minutes, and is the corpus
Tantivy and the search-benchmark-game already use — so numbers are directly
comparable. For the relevance axis, add **MS MARCO + BEIR** (via the existing
`ndcg.py`). Keep `gen_corpus.sql` for the fast controllable A/B smoke.

---

## Part B — The concrete plan we run on one large EC2 instance

### B.0 Instance & storage

- **`r7i.8xlarge`** (32 vCPU, 256 GB) + **gp3 3 TB, 16000 IOPS / 1000 MB/s**,
  us-east-2 (per `BENCHMARK_PLAN.md` scale row). 256 GB RAM lets a ~30M-doc
  Wikipedia index sit in cache for the *warm* runs and be evicted for the *cold*
  runs. For the 50M scale point the index exceeds RAM — that is the point.
- PG config for all three engines (fair, and matches how ES/ParadeDB tune):
  `shared_buffers=64GB`, `work_mem=256MB`, `maintenance_work_mem=8GB`,
  `max_parallel_workers_per_gather=8`, `effective_cache_size=192GB`,
  `max_wal_size=32GB`, `checkpoint_timeout=30min`, `jit=off` (stable timings).

### B.1 Corpus: real Wikipedia at production row counts

Target three scale points. Wikipedia en has ~6.8M articles; to hit 10M-50M we
replicate with a per-row salt so vocabulary stays natural but rows are distinct.

```bash
# get_wikipedia.sh  (Python, ~30 lines; put in bench/)
#   pip install datasets
#   streams wikimedia/wikipedia 20231101.en -> docs.tsv  (id \t title \t body)
#   writes ~6.8M rows, ~20 GB
python3 get_wikipedia.py --out /data/wiki.tsv
```

```sql
-- load_and_scale.sql  (psql -v target=30000000)
CREATE TABLE docs (id bigint, title text, body text);
\copy docs (id, title, body) FROM '/data/wiki.tsv' WITH (FORMAT csv, DELIMITER E'\t')
-- replicate to :target rows; salt keeps bodies distinct without warping IDF much
INSERT INTO docs
SELECT g,
       title,
       body || ' rep' || (g / (SELECT count(*) FROM docs))::text
FROM docs, generate_series((SELECT count(*)+1 FROM docs), :target) g
-- (drive the copies from a numbers table in practice; illustrative here)
;
```

Scale points to run: **10M**, **30M**, **50M** rows (each `ANALYZE`d).
Record `pg_total_relation_size('docs')` and avg body length per scale point.

> ponytail: the simple salted replication is the cheap way to reach 30M/50M from
> a 6.8M real corpus without downloading the chunked-passage 60M set. Swap in the
> real `wikimedia/wikipedia` passage chunks if reviewers object to replication;
> the harness doesn't change.

### B.2 The three index builds (record build time, size, peak RSS)

Reuse `load.sh`'s three-index pattern; the SQL is exactly:

```sql
-- pg_fts (subject)
CREATE EXTENSION pg_fts;
CREATE INDEX docs_bm25 ON docs USING bm25 (to_ftsdoc('english', body));

-- tsvector + GIN + ts_rank (in-tree baseline)
ALTER TABLE docs ADD COLUMN tsv tsvector;
UPDATE docs SET tsv = to_tsvector('english', body);
CREATE INDEX docs_gin ON docs USING gin (tsv);

-- ParadeDB pg_search (strongest competitor)
CREATE EXTENSION pg_search;
CREATE INDEX docs_search ON docs USING bm25 (id, title, body) WITH (key_field='id');
```

Capture, per engine per scale point:
- **build wall-clock** (`\timing`),
- **index size** (`pg_relation_size` for GIN/bm25; `pg_relation_size` +
  `pg_total_relation_size` for pg_search which stores extra),
- **peak RSS** during build (`/usr/bin/time -v` around a single-backend
  `CREATE INDEX`, or sample `/proc/<pid>/status VmHWM`),
- **incremental insert throughput**: after the initial build, `INSERT` a fresh
  100k-row batch and time it (the pending/segment-flush path) — the write axis
  ParadeDB and bm25s both report.

### B.3 The exact query set

Terms are sampled from the **real corpus vocabulary at three frequency bands** so
IDF is meaningful and results are comparable to the search-benchmark-game (which
also draws from a real query log):

```sql
-- build queries.tsv: 3 bands x N terms, sampled from actual lexemes
-- rare  : df in [10, 1000]      (high IDF, tiny match set)
-- mid   : df in [10k, 200k]     (the interesting middle)
-- common: df > 1M               (stresses posting decode / WAND pivot)
```

Each query type below lists the pg_fts form (the subject), the tsvector/GIN
baseline, and the pg_search competitor. `<=>` drives the bm25 ordering scan;
`@@@`/`to_ftsquery` are the real operators in this extension.

| # | Query type | pg_fts | tsvector/GIN | pg_search |
|---|---|---|---|---|
| Q1 | **single-term match count** (rare) | `SELECT count(*) FROM docs WHERE d @@@ to_ftsquery('english','RARE')` | `... WHERE tsv @@ to_tsquery('english','RARE')` | `... WHERE body @@@ 'RARE'` |
| Q2 | **single-term count** (mid) | `fts_count('docs_bm25', to_ftsquery('english','MID'))` and `count(*)` form | `count(*) ... tsv @@ ...` | `count(*) ... body @@@ ...` (agg pushdown) |
| Q3 | **boolean AND (2 term)** count | `... d @@@ to_ftsquery('english','A & B')` | `... tsv @@ to_tsquery('english','A & B')` | `... body @@@ 'A AND B'` |
| Q4 | **boolean AND (3 term)** count | `'A & B & C'` | `'A & B & C'` | `'A AND B AND C'` |
| Q5 | **boolean OR (2-3 term)** count | `'A \| B \| C'` | `'A \| B \| C'` | `'A OR B OR C'` |
| Q6 | **phrase** | `'"A B"'::ftsquery` (or `to_ftsquery('english','A <-> B')`) | `to_tsquery('english','A <-> B')` | `'"A B"'` |
| Q7 | **ranked top-10 (mid & mid)** | `SELECT id, d <=> q FROM docs WHERE d @@@ q ORDER BY d <=> q LIMIT 10` with `q = to_ftsquery('english','A & B')` | `... ORDER BY ts_rank(tsv, q) DESC LIMIT 10` | `... ORDER BY paradedb.score(id) DESC LIMIT 10` |
| Q8 | **ranked top-10 (common & mid)** | same shape, common+mid terms | same | same |
| Q9 | **ranked top-100** (deep page) | `... LIMIT 100` (exercises the k-growth path fixed in RESULTS_VS_PGSEARCH) | `... LIMIT 100` | `... LIMIT 100` |
| Q10 | **ranked top-10 + total count** | `fts_search('docs_bm25', q, 10)` + `fts_count(...)` | rank query + separate `count(*)` | `... LIMIT 10` + `count(*)` (or agg scan) |
| Q11 | **prefix** | `to_ftsquery('english','postg:*')` (or `'postg*'`) | `to_tsquery('english','postg:*')` | `'postg*'` |
| Q12 | **fuzzy (edit distance)** | `'term~1'::ftsquery` / `'term~2'` | (no native equiv — pg_trgm `%` as a proxy, or N/A) | `'term~1'` |
| Q13 | **faceting / aggregation** | `SELECT count(*) FROM docs WHERE d @@@ q` grouped by a doc field (heap agg) | grouped `count(*)` over `tsv @@ q` | `SELECT ... count(*) ... GROUP BY` (ParadeDB Aggregate Scan) |
| Q14 | **regex over tokens** (pg_fts feature) | `'/postgr.*/'::ftsquery` | N/A (pg_trgm regex proxy) | N/A / limited |

Notes:
- Q7-Q10 are the **headline** numbers (production search = ranked top-N). Q9/Q10
  are where the prior gap to pg_search lived (`RESULTS_VS_PGSEARCH.md`).
- Q2/Q3/Q13 are GIN's and pg_search's strengths (count/agg pushdown); report
  honestly.
- Q11-Q14 are pg_fts feature-parity/differentiators; tsvector has no fuzzy/regex.
- Verify each ranked query returns **byte-identical top-N to a forced seqscan+sort**
  once per band (correctness gate before timing), as prior results did.

### B.4 Metrics to capture

1. **Build time** (s) per engine per scale point.
2. **Index size** (bytes) per engine per scale point; also index/heap ratio.
3. **Peak build RSS** (MB) per engine.
4. **Per-query latency p50/p95/p99** per query type per frequency band,
   **warm** and **cold** cache. (Existing `phase_latency.sh` captures p50/p99
   serial; extend to p95 + cold.)
5. **Throughput QPS under concurrency** per query type (see B.5).
6. **Incremental insert throughput** (docs/s) post-build.

Cache states:
- **Warm**: run the class once to prime, then measure. (Default in existing runners.)
- **Cold**: `sync; echo 3 > /proc/sys/vm/drop_caches; pg_ctl restart` before the
  class — the "index > RAM" reality at 50M. `run_latency.sh` already has
  `drop_caches`; add the restart for a true cold PG buffer cache.

### B.5 Driving load at production concurrency (QPS)

The existing runners are **serial** (one query at a time → latency only, no
QPS). Add a **pgbench custom-script** driver — pgbench is already in the tree
(`src/bin/pgbench`), so no new dependency, and it gives QPS + per-statement
latency + concurrency for free.

One script per (engine, query-type), using pgbench random vars to pick terms:

```
-- bench/pgb_pgfts_rank.sql   (ranked top-10, pg_fts)
\set t1 random(1, :nterms)
\set t2 random(1, :nterms)
SELECT id FROM docs
WHERE d @@@ to_ftsquery('english', :term1 || ' & ' || :term2)
ORDER BY d <=> to_ftsquery('english', :term1 || ' & ' || :term2)
LIMIT 10;
```

In practice, precompute a `bench_terms(k int, band text, term text)` table and
have the script join/pick a term by random `k` (avoids string-building in the
script). Drive it:

```bash
# clients scale from 1 (latency floor) up past vCPU count (saturation)
for C in 1 4 8 16 32 64; do
  pgbench -n -d bench -c $C -j $C -T 60 -f bench/pgb_pgfts_rank.sql \
          -D nterms=$NTERMS --progress=10 -r \
    | tee results/qps_pgfts_rank_c$C.txt
done
```

pgbench reports **tps (QPS)**, **latency average**, and with `-r` the
per-statement latency; `--progress` shows stability. Repeat the identical loop
for `pgb_tsvector_rank.sql` and `pgb_pgsearch_rank.sql`, and for the count/AND/OR
classes. **A/B alternate engines** at each client count (per the drift-control
pattern in `run_latency.sh`) and take medians of 3 x 60s runs.

Report per query type: the **QPS-vs-clients curve** (throughput ceiling and the
client count where it plateaus) and **p95 latency at the plateau** — the two
numbers that describe production behavior. This is the esrally "fixed clients,
full percentile distribution" method, done with in-tree pgbench.

> ponytail: pgbench covers concurrency, QPS, and percentiles with zero new deps.
> Skipped a bespoke async Python load driver — add one only if we need
> closed-loop *fixed-target-rate* (esrally-style rate limiting), which pgbench
> can't cap. YAGNI until a reviewer asks for a fixed-rate SLO test.

### B.6 Relevance axis (accuracy, not speed)

Unchanged from `BENCHMARK_PLAN.md` §3 + `ndcg.py`: run **BEIR (nfcorpus,
scifact, fiqa)** and **MS MARCO dev** through each engine, score **NDCG@10 /
Recall@100 / MRR** against qrels. Target: pg_fts NDCG@10 within noise of
Lucene/ES and clearly above tsvector `ts_rank`; `fts_bm25_opts(variant='lucene')`
matches Lucene scores to ~1e-4 (the score-parity check).

---

## Part C — What we produce (deliverables of a run)

Per scale point (10M / 30M / 50M), one CSV/table each for:
1. `build.csv` — engine, scale, build_s, index_bytes, peak_rss_mb, incr_docs_s.
2. `latency.csv` — engine, qtype, band, cache(warm|cold), p50, p95, p99.
3. `qps.csv` — engine, qtype, clients, tps, lat_avg_ms, lat_p95_ms.
4. `relevance.csv` — engine, dataset, ndcg10, recall100, mrr.

Then one `RESULTS_PRODUCTION.md` in this dir with the head-to-head tables
(mirroring `RESULTS_VS_PGSEARCH.md`), the QPS-vs-clients plots, and the honest
call-outs (where GIN/pg_search win on count/agg pushdown, per the known
architectural gap in `STRATEGY_REPLAN.md`).

## Cost

One `r7i.8xlarge` (~$2.6/hr) for the scale runs; build + all query classes +
QPS sweep + relevance for the three scale points fit in ~1 working day
(~$25-30). Smoke and relevance can stay on the cheaper `m6i.2xlarge` box from
`BENCHMARK_PLAN.md` §9. Terminate after each phase.
