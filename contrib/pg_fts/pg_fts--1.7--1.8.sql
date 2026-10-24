/* contrib/pg_fts/pg_fts--1.7--1.8.sql */

\echo Use "ALTER EXTENSION pg_fts UPDATE TO '1.8'" to load this file. \quit

-- Stage 7 adds incremental maintenance to the bm25 access method: INSERT now
-- appends to an in-index pending list rather than erroring, and pending
-- documents are searched at scan time so new rows are immediately visible.
-- This is a C-level change to the access method; there are no new SQL objects.
-- The bm25 metapage format changed, so bm25 indexes created by earlier
-- versions must be rebuilt: REINDEX any bm25 index after this upgrade.
