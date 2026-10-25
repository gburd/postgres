/* contrib/pg_fts/pg_fts--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_fts" to load this file. \quit

--
-- ftsdoc: an analyzed full-text document (terms + term frequencies).
--
CREATE TYPE ftsdoc;

CREATE FUNCTION ftsdoc_in(cstring)
RETURNS ftsdoc
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION ftsdoc_out(ftsdoc)
RETURNS cstring
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION ftsdoc_recv(internal)
RETURNS ftsdoc
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION ftsdoc_send(ftsdoc)
RETURNS bytea
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE TYPE ftsdoc (
    INPUT          = ftsdoc_in,
    OUTPUT         = ftsdoc_out,
    RECEIVE        = ftsdoc_recv,
    SEND           = ftsdoc_send,
    INTERNALLENGTH = VARIABLE,
    STORAGE        = extended
);

--
-- ftsquery: a parsed boolean query.
--
CREATE TYPE ftsquery;

CREATE FUNCTION ftsquery_in(cstring)
RETURNS ftsquery
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION ftsquery_out(ftsquery)
RETURNS cstring
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION ftsquery_recv(internal)
RETURNS ftsquery
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION ftsquery_send(ftsquery)
RETURNS bytea
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE TYPE ftsquery (
    INPUT          = ftsquery_in,
    OUTPUT         = ftsquery_out,
    RECEIVE        = ftsquery_recv,
    SEND           = ftsquery_send,
    INTERNALLENGTH = VARIABLE,
    STORAGE        = extended
);

--
-- Constructors from text.
--
CREATE FUNCTION to_ftsdoc(text)
RETURNS ftsdoc
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION to_ftsquery(text)
RETURNS ftsquery
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

--
-- Support functions.
--
CREATE FUNCTION ftsdoc_length(ftsdoc)
RETURNS integer
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

--
-- The @@@ match operator.
--
CREATE FUNCTION fts_match(ftsdoc, ftsquery)
RETURNS boolean
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION fts_match_commutator(ftsquery, ftsdoc)
RETURNS boolean
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OPERATOR @@@ (
    LEFTARG    = ftsdoc,
    RIGHTARG   = ftsquery,
    PROCEDURE  = fts_match,
    COMMUTATOR = @@@,
    RESTRICT   = tsmatchsel,
    JOIN       = tsmatchjoinsel
);

CREATE OPERATOR @@@ (
    LEFTARG    = ftsquery,
    RIGHTARG   = ftsdoc,
    PROCEDURE  = fts_match_commutator,
    COMMUTATOR = @@@,
    RESTRICT   = tsmatchsel,
    JOIN       = tsmatchjoinsel
);
