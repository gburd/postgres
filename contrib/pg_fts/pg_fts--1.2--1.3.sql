/* contrib/pg_fts/pg_fts--1.2--1.3.sql */

\echo Use "ALTER EXTENSION pg_fts UPDATE TO '1.3'" to load this file. \quit

-- The bm25 index access method: an inverted index over an ftsdoc column that
-- answers the @@@ operator by bitmap scan and maintains corpus statistics.
CREATE FUNCTION bm25handler(internal)
RETURNS index_am_handler
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE ACCESS METHOD bm25 TYPE INDEX HANDLER bm25handler;

COMMENT ON ACCESS METHOD bm25 IS 'bm25 inverted index for full-text search';

-- Operator class: strategy 1 is @@@ (ftsdoc @@@ ftsquery).
CREATE OPERATOR CLASS ftsdoc_bm25_ops
DEFAULT FOR TYPE ftsdoc USING bm25 AS
    OPERATOR 1 @@@ (ftsdoc, ftsquery);
