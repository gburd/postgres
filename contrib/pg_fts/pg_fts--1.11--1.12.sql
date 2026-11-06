/* contrib/pg_fts/pg_fts--1.11--1.12.sql */

\echo Use "ALTER EXTENSION pg_fts UPDATE TO '1.12'" to load this file. \quit

-- BM25F: multi-field BM25.  Pass one ftsdoc per field (e.g. title, body), a
-- weight per field, and an avgdl per field.  Per-field term frequencies are
-- length-normalized per field and combined by weight before tf-saturation
-- (the Robertson/Zaragoza BM25F formulation).
CREATE FUNCTION fts_bm25f(docs ftsdoc[], query ftsquery,
                          weights float8[], n_docs float8, avgdls float8[],
                          dfs float8[] DEFAULT NULL)
RETURNS float8
AS 'MODULE_PATHNAME', 'fts_bm25f'
LANGUAGE C IMMUTABLE PARALLEL SAFE;
