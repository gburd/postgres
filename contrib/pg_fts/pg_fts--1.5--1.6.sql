/* contrib/pg_fts/pg_fts--1.5--1.6.sql */

\echo Use "ALTER EXTENSION pg_fts UPDATE TO '1.6'" to load this file. \quit

-- Migration helper: mechanically convert a tsquery to an ftsquery.
-- & -> AND, | -> OR, ! -> NOT.  The phrase operator <-> degrades to AND with
-- a NOTICE (phrase search is a later stage).
CREATE FUNCTION tsquery_to_ftsquery(tsquery)
RETURNS ftsquery
AS 'MODULE_PATHNAME', 'tsquery_to_ftsquery'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- Convenience cast so existing tsquery values flow into @@@ queries.
CREATE CAST (tsquery AS ftsquery)
    WITH FUNCTION tsquery_to_ftsquery(tsquery) AS ASSIGNMENT;
