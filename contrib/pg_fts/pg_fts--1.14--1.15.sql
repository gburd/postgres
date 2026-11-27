/* contrib/pg_fts/pg_fts--1.14--1.15.sql */

\echo Use "ALTER EXTENSION pg_fts UPDATE TO '1.15'" to load this file. \quit

-- Trigram pre-filter for fuzzy matching (cribbed in spirit from pg_tre).  A
-- fuzzy term (term~k) is reduced to its trigrams, and only document terms
-- sharing a trigram with it are tested with the (bounded) Levenshtein
-- computation, instead of testing every term.  This is a C-level speedup to
-- fuzzy evaluation; results are unchanged (the filter only prunes candidates
-- that provably cannot match, and falls back to a full scan when the
-- pigeonhole guarantee does not hold).  No new SQL objects.
