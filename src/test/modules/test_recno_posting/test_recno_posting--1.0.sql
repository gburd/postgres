/* src/test/modules/test_recno_posting/test_recno_posting--1.0.sql */
\echo Use "CREATE EXTENSION test_recno_posting" to load this file. \quit

CREATE FUNCTION test_recno_posting()
    RETURNS void
    AS 'MODULE_PATHNAME' LANGUAGE C;
