/* contrib/pg_fts/pg_fts--1.10--1.11.sql */

\echo Use "ALTER EXTENSION pg_fts UPDATE TO '1.11'" to load this file. \quit

-- Stages 13-14: fuzzy (term~k) and regex (/re/) query terms.  ftsquery gains
-- two term forms:
--   term~k   matches any document term within Levenshtein distance k (k
--            defaults to 2), using core's bounded varstr_levenshtein_less_equal
--   /re/     matches any document term matching the POSIX regular expression,
--            using core's cached regex engine
-- Both are C-level extensions to existing functions (no new SQL objects).  The
-- bm25 index returns all indexed tuples as candidates for such queries and the
-- bitmap heap recheck applies the exact test; a trigram pre-filter (the pg_tre
-- approach) to narrow candidates is future work.
