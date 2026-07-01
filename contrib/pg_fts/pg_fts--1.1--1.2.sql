/* contrib/pg_fts/pg_fts--1.1--1.2.sql */

\echo Use "ALTER EXTENSION pg_fts UPDATE TO '1.2'" to load this file. \quit

-- BM25 relevance score of a document against a query.
--   n_docs : total documents in the corpus (N)
--   avgdl  : average document length (in tokens)
--   dfs    : optional float8[] of per-query-term document frequencies, in the
--            order the query's distinct terms appear; NULL treats terms as rare
CREATE FUNCTION fts_bm25(ftsdoc, ftsquery, n_docs float8, avgdl float8,
                         dfs float8[] DEFAULT NULL)
RETURNS float8
AS 'MODULE_PATHNAME', 'fts_bm25'
LANGUAGE C IMMUTABLE PARALLEL SAFE;
