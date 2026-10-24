/* contrib/pg_fts/pg_fts--1.9--1.10.sql */

\echo Use "ALTER EXTENSION pg_fts UPDATE TO '1.10'" to load this file. \quit

-- Stage 10 (external content): the bm25 index stores only postings, never the
-- document text, so an expression index on to_ftsdoc(text_column) is the
-- external-content model -- the text lives in the base table and the index is
-- derived from it.  No new SQL objects are required; this upgrade is a
-- documentation marker that the pattern is supported and tested.
