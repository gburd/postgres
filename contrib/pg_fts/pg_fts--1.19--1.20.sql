/* contrib/pg_fts/pg_fts--1.19--1.20.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION pg_fts UPDATE TO '1.20'" to load this file. \quit

-- fts_vacuum(regclass): on-demand full compaction + truncation, reclaiming
-- the dead pages left by prior merges (shrinks the physical index file).
CREATE FUNCTION fts_vacuum(regclass)
RETURNS boolean
AS 'MODULE_PATHNAME', 'fts_vacuum'
LANGUAGE C STRICT;
