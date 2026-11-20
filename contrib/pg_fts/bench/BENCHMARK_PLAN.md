# pg_fts BM25 Benchmark Plan (EC2, us-east-2)

Adapted from Jim Mlodgenski's clock-sweep benchmark guide
(GREG_BENCHMARK_GUIDE.md).  We reuse Jim's AWS/EC2 mechanics verbatim and swap
the *workload* from pgbench/HammerDB (buffer-manager patch) to **full-text
search / BM25 relevance** (pg_fts).

Goal: measure pg_fts against the field on every axis the competition reports —
relevance accuracy, query latency by class (p50/p99), indexing throughput,
index size, memory — and produce a defensible comparison.

---

## 0. What we are comparing

| System | Role | How queried |
|---|---|---|
| **pg_fts** (this work) | subject | `d @@@ q ORDER BY d <=> q LIMIT k` |
| PostgreSQL **tsvector + GIN + ts_rank** | in-tree baseline | `tsv @@ q ORDER BY ts_rank(tsv,q)` |
| **ParadeDB pg_search** (Tantivy) | strongest PG competitor | `@@@` + `paradedb.score` |
| **Elasticsearch** / OpenSearch | external baseline (optional) | `_search` match + BM25 |

We *lead* with the two PostgreSQL comparisons (in-tree + pg_search) because
those are the ones that decide "best BM25 on Postgres." Elasticsearch is the
external reference for relevance-score parity and absolute latency.

---

## 1. Axes and metrics (the informative subset from the competition)

Borrowed, per axis, from how each competitor benchmarks (see the design doc
FTS_NEXTGEN_PLAN.md §12.5):

| Axis | Corpus | Metric | Source of method |
|---|---|---|---|
| **Relevance / accuracy** | BEIR (nfcorpus, scifact, fiqa) + MS MARCO dev | **NDCG@10, Recall@100, MRR** vs qrels; **score parity vs Lucene** | BEIR, bm25s |
| **Query latency by class** | Wikipedia (5M / 20M docs) | **p50 / p99** for: single-term, AND (2-3), OR (2-3), phrase, top-10 ORDER BY score | Tantivy search-benchmark-game |
| **Indexing** | same Wikipedia | build throughput (docs/s), **on-disk index size**, incremental-insert throughput (pending path) | ParadeDB, bm25s |
| **Scale** | Wikipedia scaled to 50-100M | latency vs corpus-size curve; behavior when index > RAM | ParadeDB |
| **Memory** | all | peak RSS during build and during query | bm25s |

Each latency number: **3 runs, report median**, A/B-alternated across systems at
each query class (Jim's alternation pattern, to control drift).

---

## 2. Instance choice

Jim's guide targets bare-metal NUMA boxes for a buffer-manager patch.  FTS
latency/throughput does **not** need bare metal or 6-node NUMA; it needs enough
RAM to hold (or deliberately not hold) the index, fast storage, and steady
CPU.  Recommended:

| Phase | Instance | Why | Cost/hr |
|---|---|---|---|
| **Dev / smoke** (5M Wikipedia, correctness + first numbers) | `m6i.2xlarge` (8 vCPU, 32 GB) or `r6i.2xlarge` (64 GB) | cheap, enough for 5M docs | ~$0.4-0.5 |
| **Main run** (20M Wikipedia, all axes) | `r7i.4xlarge` (16 vCPU, 128 GB) | index fits in RAM; steady CPU | ~$1.3 |
| **Scale run** (50-100M, index > RAM) | `r7i.8xlarge` (32 vCPU, 256 GB) + gp3 3TB | forces cache-miss behavior | ~$2.6 |

Region **us-east-2** (per your account).  gp3 volume: 16000 IOPS / 1000 MB/s
(same as Jim's data volume).  No bare metal, no hugepages needed for the FTS
workload (we can still set them for the PG-vs-PG runs to be fair).

We do NOT need r8i.metal-96xl — that was for the NUMA clock-sweep effect.
Using it would just burn $14/hr for no FTS-relevant signal.

---

## 3. Corpora and how to get them

1. **Wikipedia** (latency + indexing + scale): the standard IR latency corpus.
   Download an enwiki dump extract (e.g. Cohere/wikipedia or the
   `wikimedia/wikipedia` HuggingFace set), stream to a `docs(id, title, body)`
   table.  Subset to 5M / 20M / 100M rows for the three phases.
2. **BEIR** (relevance): `nfcorpus`, `scifact`, `fiqa` are small (a few thousand
   docs) with published qrels + queries — ideal for NDCG@10 / Recall@100 that
   runs in seconds and directly compares to Lucene/Elasticsearch numbers in the
   BEIR paper.
3. **MS MARCO dev** (relevance at scale): 8.8M passages + qrels; the standard
   "does BM25 rank right at scale" test bm25s and Elasticsearch both report.

Loaders (`load_wikipedia.py`, `load_beir.py`) go in `bench/` — they normalize
into `docs(id int, title text, body text)` and, for BEIR/MARCO, a
`qrels(query_id, doc_id, relevance)` + `queries(query_id, text)` pair.

---

## 4. The three index builds (identical corpus, three systems)

Per corpus table `docs(id, title, body)`:

```sql
-- pg_fts
CREATE EXTENSION pg_fts;
CREATE INDEX docs_bm25 ON docs USING bm25 (to_ftsdoc('english', body));

-- in-tree tsvector + GIN
ALTER TABLE docs ADD COLUMN tsv tsvector;
UPDATE docs SET tsv = to_tsvector('english', body);
CREATE INDEX docs_gin ON docs USING gin (tsv);

-- ParadeDB pg_search (if installed)
CREATE EXTENSION pg_search;
CREATE INDEX docs_search ON docs USING bm25 (id, body) WITH (key_field='id');
```

Record for each: wall-clock build time, `pg_relation_size`, peak RSS
(`/usr/bin/time -v` around the build or sampling the backend's RSS).

---

## 5. Query workload (by class)

A query log of ~200 queries per class, drawn from the corpus vocabulary
(so terms exist).  For each system, an equivalent query; measure with a
percentile-capturing runner (pgbench `-f` custom script per class, or a small
Python driver timing each query and reporting p50/p99).

| Class | pg_fts | tsvector | pg_search |
|---|---|---|---|
| single-term | `d @@@ 'foo' ORDER BY d <=> 'foo' LIMIT 10` | `tsv @@ 'foo' ORDER BY ts_rank … LIMIT 10` | `body @@@ 'foo' ORDER BY score LIMIT 10` |
| AND (2-3) | `'foo & bar'` | `'foo & bar'` | `'foo AND bar'` |
| OR (2-3) | `'foo \| bar'` | `'foo \| bar'` | `'foo OR bar'` |
| phrase | `'"foo bar"'` | `'foo <-> bar'` | `'"foo bar"'` |
| top-10 ranked | the ORDER BY … LIMIT 10 above | same | same |

The ranked top-10 is the headline number (that's what production search does).

---

## 6. Harness (in bench/)

Reusing Jim's structure — A/B alternation, medians, screen, `progress.log`,
`drop_caches` between runs — adapted to FTS:

- `bench/aws_launch.sh` — Jim's §2-3 CLI, parameterized instance type, us-east-2.
- `bench/bootstrap.sh` — Jim's §4 OS setup, minus hugepages/NUMA-pinning
  (not needed for FTS); adds Python + the loaders' deps.
- `bench/build_pg.sh` — build this PG tree + install pg_fts; optionally build
  pg_search and install Elasticsearch.
- `bench/load.sh` — run the corpus loaders, build all three indexes, record
  build time / size / RSS.
- `bench/run_latency.sh` — the per-class A/B latency runner (p50/p99, 3 runs,
  median), CSV out.  Mirrors `run_pgbench_ab.sh`.
- `bench/run_relevance.sh` — run the BEIR/MARCO queries through each system,
  score NDCG@10 / Recall@100 against qrels (a small `ndcg.py`).
- `bench/collect.sh` — Jim's §9 scp results + system_info.

`bench/bench.sql` (already committed) is the smoke-test A/B for a hand-loaded
`bench_corpus`.

---

## 7. Run order

1. **Smoke** (m6i.2xlarge, 5M Wikipedia): correctness + first latency/size
   numbers vs tsvector. Confirms the harness end-to-end for ~$5.
2. **Relevance** (same box, BEIR + MARCO): NDCG/Recall/MRR vs tsvector, and
   score-parity vs a local Lucene/Elasticsearch. This is where `fts_bm25_opts`
   variants prove we match Lucene scores.
3. **Main latency** (r7i.4xlarge, 20M Wikipedia): the full per-class p50/p99
   matrix vs tsvector and pg_search.
4. **Scale** (r7i.8xlarge, 50-100M): latency-vs-size curve, index-larger-than-RAM.

Terminate after each phase (Jim's §10) — don't leave boxes running.

---

## 8. What "winning" requires (honest bar)

- **Relevance:** NDCG@10 within noise of Lucene/Elasticsearch, clearly ahead of
  tsvector cover-density ranking. `fts_bm25_opts(variant='lucene')` should match
  Lucene scores to ~1e-4.
- **Ranked top-10 latency:** meet or beat tsvector+GIN+ts_rank; competitive with
  pg_search. The block-max WAND + index-only scoring is what should carry this.
- **Index size:** within ~2x of pg_search/Tantivy (they have years of encoding
  tuning); clearly better than storing tsvector + GIN.
- **Indexing throughput & memory:** report honestly; the tuplesort-free
  build-in-memory path is the known weak spot at very large scale.

If any axis loses, that's a finding, not a failure — we report it and it drives
the next round of work.

---

## 9. Cost estimate

| Phase | Instance | Hours | Cost |
|---|---|---|---|
| Smoke | m6i.2xlarge | 2 | ~$1 |
| Relevance | m6i.2xlarge | 2 | ~$1 |
| Main latency | r7i.4xlarge | 4 | ~$5 |
| Scale | r7i.8xlarge | 4 | ~$10 |
| **Total** | | | **~$17** |

Far cheaper than Jim's NUMA runs because FTS doesn't need bare metal.
