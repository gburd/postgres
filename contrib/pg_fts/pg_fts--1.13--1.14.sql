/* contrib/pg_fts/pg_fts--1.13--1.14.sql */

\echo Use "ALTER EXTENSION pg_fts UPDATE TO '1.14'" to load this file. \quit

-- Index-only BM25 top-k search.  Scores are computed entirely from the index
-- (postings, dictionary df/max-tf, metapage N/avgdl) with no heap access; the
-- result is the top-k (ctid, score) pairs.  Join back to the table on ctid to
-- fetch rows.  This is the index-only-scoring path; a WAND upper-bound prunes
-- documents that cannot enter the top-k.
CREATE FUNCTION fts_search(index regclass, query ftsquery, k int DEFAULT 10,
                           OUT ctid tid, OUT score float8)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'fts_search'
LANGUAGE C STRICT PARALLEL SAFE;
