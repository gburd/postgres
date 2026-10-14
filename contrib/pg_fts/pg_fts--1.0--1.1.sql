/* contrib/pg_fts/pg_fts--1.0--1.1.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION pg_fts UPDATE TO '1.1'" to load this file. \quit

-- Analyzer that reuses an installed text search configuration (pg_ts_config):
-- the configured parser + dictionary chain (stemming, stopwords, synonyms,
-- thesaurus) is applied, rather than the built-in simple tokenizer.
CREATE FUNCTION to_ftsdoc(regconfig, text)
RETURNS ftsdoc
AS 'MODULE_PATHNAME', 'to_ftsdoc_byid'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
