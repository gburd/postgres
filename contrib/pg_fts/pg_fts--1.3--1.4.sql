/* contrib/pg_fts/pg_fts--1.3--1.4.sql */

\echo Use "ALTER EXTENSION pg_fts UPDATE TO '1.4'" to load this file. \quit

-- BM25 with selectable variant and explicit k1/b, for reproducing reference
-- implementations (Lucene, Robertson/classic, ATIRE, BM25+).
CREATE FUNCTION fts_bm25_opts(ftsdoc, ftsquery,
                              n_docs float8, avgdl float8,
                              k1 float8, b float8,
                              variant text,
                              dfs float8[] DEFAULT NULL)
RETURNS float8
AS 'MODULE_PATHNAME', 'fts_bm25_opts'
LANGUAGE C IMMUTABLE PARALLEL SAFE;
