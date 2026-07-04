/* contrib/pg_fts/pg_fts--1.19--1.20.sql */

\echo Use "ALTER EXTENSION pg_fts UPDATE TO '1.20'" to load this file. \quit

-- 1.20: on-disk format v3 adds a per-term impact-ordered block skip directory
-- to the bm25 index, so a single-term ranked top-k (ORDER BY d <=> q LIMIT k)
-- visits high-impact blocks first and stops early instead of scanning a common
-- term's whole posting list.  No SQL surface changes.  The format bump means an
-- index built by an earlier version must be REINDEXed to gain the directory;
-- until then it is read with the exact docid-ordered scan (fully correct, just
-- without the early-stop speedup).
