/* contrib/pg_fts/pg_fts--1.8--1.9.sql */

\echo Use "ALTER EXTENSION pg_fts UPDATE TO '1.9'" to load this file. \quit

-- Stage 6 (phrase): to_ftsdoc now records per-term token positions (ftsdoc
-- format v2), and ftsquery gains phrase syntax ("a b c") evaluated with those
-- positions.  This is a C-level change to existing functions; there are no new
-- SQL objects.  Stored ftsdoc values from earlier versions are position-free
-- and still valid (phrase queries against them degrade to AND); regenerate
-- them with to_ftsdoc() to enable exact phrase matching.
