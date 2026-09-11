/*
 * contrib/pg_fts/bench/bench.sql -- stage 12 parity / performance harness.
 *
 * A minimal, reproducible A/B comparison of pg_fts (bm25) against the existing
 * tsvector + GIN + ts_rank stack on the same corpus.  Load a text corpus into
 * bench_corpus(id, body) first (e.g. a Wikipedia or MS MARCO subset); this
 * script builds both indexes, times a batch of ranked queries, and reports
 * timing.  It is intentionally small -- a full benchmark (latency percentiles,
 * relevance/NDCG vs. a qrels file, concurrent ingest) is the committer-facing
 * gate described in FTS_NEXTGEN_PLAN.md section 12.5.
 *
 * Usage:
 *   psql -f bench/bench.sql
 * with table bench_corpus(id serial primary key, body text) already populated.
 */

\timing on

CREATE EXTENSION IF NOT EXISTS pg_fts;

-- Existing stack: tsvector + GIN + ts_rank
ALTER TABLE bench_corpus ADD COLUMN IF NOT EXISTS tsv tsvector;
UPDATE bench_corpus SET tsv = to_tsvector('english', body) WHERE tsv IS NULL;
CREATE INDEX IF NOT EXISTS bench_gin ON bench_corpus USING gin (tsv);

-- pg_fts stack: bm25 over to_ftsdoc
CREATE INDEX IF NOT EXISTS bench_bm25 ON bench_corpus
  USING bm25 (to_ftsdoc('english', body));

ANALYZE bench_corpus;

\echo === tsvector + ts_rank: top-10 for 'query & planner' ===
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
SELECT id, ts_rank(tsv, q) AS score
FROM bench_corpus, to_tsquery('english', 'query & planner') q
WHERE tsv @@ q
ORDER BY score DESC
LIMIT 10;

\echo === pg_fts + BM25: top-10 for 'query & planner' ===
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
SELECT id,
       fts_bm25(to_ftsdoc('english', body), q, s.ndocs, s.avgdl,
                fts_index_df('bench_bm25', q)) AS score
FROM bench_corpus, fts_index_stats('bench_bm25') s,
     to_ftsquery('query & planner') q
WHERE to_ftsdoc('english', body) @@@ q
ORDER BY score DESC
LIMIT 10;

\echo === index sizes ===
SELECT pg_size_pretty(pg_relation_size('bench_gin'))  AS gin_size,
       pg_size_pretty(pg_relation_size('bench_bm25')) AS bm25_size;
