/* contrib/pg_fts/pg_fts--1.6--1.7.sql */

\echo Use "ALTER EXTENSION pg_fts UPDATE TO '1.7'" to load this file. \quit

-- Index-maintained corpus statistics, so BM25 can be scored from the values
-- the bm25 index keeps rather than caller-supplied guesses.
CREATE FUNCTION fts_index_stats(regclass,
                                OUT ndocs float8, OUT avgdl float8,
                                OUT nterms int)
RETURNS record
AS 'MODULE_PATHNAME', 'fts_index_stats'
LANGUAGE C STRICT PARALLEL SAFE;

-- Per-query-term document frequencies from the index (for fts_bm25's dfs arg).
CREATE FUNCTION fts_index_df(regclass, ftsquery)
RETURNS float8[]
AS 'MODULE_PATHNAME', 'fts_index_df'
LANGUAGE C STRICT PARALLEL SAFE;
