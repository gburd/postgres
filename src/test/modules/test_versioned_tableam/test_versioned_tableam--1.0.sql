/* src/test/modules/test_versioned_tableam/test_versioned_tableam--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_versioned_tableam" to load this file. \quit

CREATE FUNCTION test_versioned_btree_handler(internal)
RETURNS index_am_handler
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE ACCESS METHOD test_versioned_btree TYPE INDEX
  HANDLER test_versioned_btree_handler;
COMMENT ON ACCESS METHOD test_versioned_btree IS 'nbtree storing version tags';

CREATE FUNCTION heap_versioned_handler(internal)
RETURNS table_am_handler
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE ACCESS METHOD heap_versioned TYPE TABLE HANDLER heap_versioned_handler;
COMMENT ON ACCESS METHOD heap_versioned IS 'heap with a versioned locator';

CREATE FUNCTION test_versioned_tag(regclass, tid)
RETURNS bigint
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION test_versioned_counts(OUT counter text, OUT n bigint)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION test_versioned_reset()
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

-- counters that moved since the last reset
CREATE VIEW test_versioned_seen AS
  SELECT counter, n FROM test_versioned_counts() WHERE n > 0 ORDER BY counter;

CREATE FUNCTION test_versioned_fetch(regclass, tid)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;
