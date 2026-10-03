/* contrib/pg_fts/pg_fts--1.12--1.13.sql */

\echo Use "ALTER EXTENSION pg_fts UPDATE TO '1.13'" to load this file. \quit

-- Background merge of the pending list.  VACUUM now folds pending (incremental
-- insert) documents into the main dictionary/posting structure via
-- amvacuumcleanup, and fts_merge() does it on demand.  Old blocks are left
-- unreferenced and reclaimed by REINDEX (a page recycler is future work).
CREATE FUNCTION fts_merge(regclass)
RETURNS boolean
AS 'MODULE_PATHNAME', 'fts_merge'
LANGUAGE C STRICT;
