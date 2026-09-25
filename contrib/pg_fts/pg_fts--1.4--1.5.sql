/* contrib/pg_fts/pg_fts--1.4--1.5.sql */

\echo Use "ALTER EXTENSION pg_fts UPDATE TO '1.5'" to load this file. \quit

-- Highlight query terms in the source text.
CREATE FUNCTION fts_highlight(doc text, query ftsquery,
                              pre text DEFAULT '<b>', post text DEFAULT '</b>')
RETURNS text
AS 'MODULE_PATHNAME', 'fts_highlight'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- Best-matching window (snippet) of the source text.
CREATE FUNCTION fts_snippet(doc text, query ftsquery,
                            pre text DEFAULT '<b>', post text DEFAULT '</b>',
                            ellipsis text DEFAULT '…', max_tokens int DEFAULT 15)
RETURNS text
AS 'MODULE_PATHNAME', 'fts_snippet'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
