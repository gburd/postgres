/* contrib/pg_fts/pg_fts--1.15--1.16.sql */

\echo Use "ALTER EXTENSION pg_fts UPDATE TO '1.16'" to load this file. \quit

-- BM25 distance operator for ORDER BY.  distance = 1/(1+score), so ascending
-- distance is descending relevance.  When used against a bm25 index it drives
-- an index ordering scan (block-max WAND top-k) with no sort node.
CREATE FUNCTION fts_distance(ftsdoc, ftsquery)
RETURNS float8
AS 'MODULE_PATHNAME', 'fts_distance'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION fts_distance_commutator(ftsquery, ftsdoc)
RETURNS float8
AS 'MODULE_PATHNAME', 'fts_distance_commutator'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE OPERATOR <=> (
    LEFTARG    = ftsdoc,
    RIGHTARG   = ftsquery,
    PROCEDURE  = fts_distance,
    COMMUTATOR = <=>
);

CREATE OPERATOR <=> (
    LEFTARG    = ftsquery,
    RIGHTARG   = ftsdoc,
    PROCEDURE  = fts_distance_commutator,
    COMMUTATOR = <=>
);

-- Add the ORDER BY operator (strategy 2) to the bm25 operator class so
-- "ORDER BY col <=> query LIMIT k" uses an index ordering scan.
ALTER OPERATOR FAMILY ftsdoc_bm25_ops USING bm25 ADD
    OPERATOR 2 <=> (ftsdoc, ftsquery) FOR ORDER BY pg_catalog.float_ops;
