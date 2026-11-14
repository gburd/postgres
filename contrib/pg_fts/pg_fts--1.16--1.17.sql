/* contrib/pg_fts/pg_fts--1.16--1.17.sql */

\echo Use "ALTER EXTENSION pg_fts UPDATE TO '1.17'" to load this file. \quit

-- to_ftsquery(regconfig, text): parse query text AND normalize each plain term
-- through the given text search configuration (stemming, case, stopwords), so
-- query terms match the same lexemes an index built with the same config
-- stores.  Prefix (term*), fuzzy (term~k) and regex (/re/) terms stay literal.
-- Use this (not the raw text->ftsquery cast) whenever the indexed ftsdoc was
-- built with to_ftsdoc(regconfig, ...).
CREATE FUNCTION to_ftsquery(regconfig, text)
RETURNS ftsquery
AS 'MODULE_PATHNAME', 'to_ftsquery_byid'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
