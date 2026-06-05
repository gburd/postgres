/* contrib/quel/quel--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION quel" to load this file. \quit

-- The QUEL extension's primary work happens in shared_preload_libraries
-- via _PG_init() -- it registers grammar tokens and rules with the
-- parser_extension.h API.  The CREATE EXTENSION script provides only
-- the diagnostic / introspection functions.

CREATE FUNCTION quel_extension_status()
RETURNS text
AS 'MODULE_PATHNAME', 'quel_extension_status'
LANGUAGE C STRICT;

COMMENT ON FUNCTION quel_extension_status() IS
'Return a one-line summary of whether QUEL grammar registered '
'successfully at postmaster start, the cache key for the rebuilt '
'parser, and which QUEL features are reachable in the current build '
'(reachability is gated on Track B scanner-table updates which '
'are not yet wired).';

CREATE FUNCTION quel_serialized_lime()
RETURNS text
AS 'MODULE_PATHNAME', 'quel_serialized_lime'
LANGUAGE C STRICT;

COMMENT ON FUNCTION quel_serialized_lime() IS
'Return the .lime grammar fragment QUEL contributed to the '
'rebuilt parser at postmaster start.  Useful for diagnosing '
'integration with other grammar extensions and for understanding '
'the shape of the registered productions.';
