/* contrib/pg_fts/pg_fts--1.17--1.18.sql */

\echo Use "ALTER EXTENSION pg_fts UPDATE TO '1.18'" to load this file. \quit

-- Number of live segments in a bm25 index (the storage engine is now
-- segment-based: inserts flush to new segments, and a size-tiered merge
-- compacts them so query cost stays bounded).  Useful for observing/tuning
-- merge behavior.
CREATE FUNCTION fts_index_nsegments(regclass)
RETURNS integer
AS 'MODULE_PATHNAME', 'fts_index_nsegments'
LANGUAGE C STRICT PARALLEL SAFE;
