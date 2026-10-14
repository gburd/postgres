/* contrib/pg_fts/pg_fts--1.18--1.19.sql */

\echo Use "ALTER EXTENSION pg_fts UPDATE TO '1.19'" to load this file. \quit

-- MVCC-correct count of documents matching a query, computed in bulk from the
-- bm25 index (visibility via the visibility map, heap probed only for
-- not-all-visible pages) without the per-tuple executor round-trips of a scan.
-- This is the count-pushdown primitive; a fast COUNT(*) path.
CREATE FUNCTION fts_count(regclass, ftsquery)
RETURNS bigint
AS 'MODULE_PATHNAME', 'fts_count'
LANGUAGE C STRICT PARALLEL SAFE;
