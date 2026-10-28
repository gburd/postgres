/* src/test/modules/test_index_impl/test_index_impl--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_index_impl" to load this file. \quit

CREATE FUNCTION test_btree_impl_handler(internal)
RETURNS index_am_handler
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE ACCESS METHOD test_btree_impl TYPE INDEX HANDLER test_btree_impl_handler;
COMMENT ON ACCESS METHOD test_btree_impl IS 'nbtree implementing btree indexes, counting calls';

CREATE FUNCTION test_btree_impl_nounique_handler(internal)
RETURNS index_am_handler
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE ACCESS METHOD test_btree_impl_nounique TYPE INDEX HANDLER test_btree_impl_nounique_handler;
COMMENT ON ACCESS METHOD test_btree_impl_nounique IS 'btree implementation without unique index support';

CREATE FUNCTION test_btree_impl_wide_handler(internal)
RETURNS index_am_handler
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE ACCESS METHOD test_btree_impl_wide TYPE INDEX HANDLER test_btree_impl_wide_handler;
COMMENT ON ACCESS METHOD test_btree_impl_wide IS 'btree implementation with more support procedures';

CREATE FUNCTION test_btree_impl_badstrat_handler(internal)
RETURNS index_am_handler
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE ACCESS METHOD test_btree_impl_badstrat TYPE INDEX HANDLER test_btree_impl_badstrat_handler;
COMMENT ON ACCESS METHOD test_btree_impl_badstrat IS 'btree implementation with another strategy count';

CREATE FUNCTION test_hash_impl_handler(internal)
RETURNS index_am_handler
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE ACCESS METHOD test_hash_impl TYPE INDEX HANDLER test_hash_impl_handler;
COMMENT ON ACCESS METHOD test_hash_impl IS 'hash implementing hash indexes';

CREATE FUNCTION test_impl_heap_handler(internal)
RETURNS table_am_handler
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE ACCESS METHOD test_impl_heap TYPE TABLE HANDLER test_impl_heap_handler;
COMMENT ON ACCESS METHOD test_impl_heap IS 'heap asking for test_btree_impl';

CREATE FUNCTION test_impl_heap_nopref_handler(internal)
RETURNS table_am_handler
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE ACCESS METHOD test_impl_heap_nopref TYPE TABLE HANDLER test_impl_heap_nopref_handler;
COMMENT ON ACCESS METHOD test_impl_heap_nopref IS 'heap with no index implementation preference';

CREATE FUNCTION test_index_impl_inserts()
RETURNS bigint
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION test_index_impl_scans()
RETURNS bigint
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION test_index_impl_reset()
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;
