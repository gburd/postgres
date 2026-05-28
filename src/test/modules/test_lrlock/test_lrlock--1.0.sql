/* src/test/modules/test_lrlock/test_lrlock--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_lrlock" to load this file. \quit

CREATE FUNCTION test_lrlock_read() RETURNS BIGINT
    AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_lrlock_write_increment(n BIGINT) RETURNS VOID
    AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_lrlock_write_set(value BIGINT) RETURNS VOID
    AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_lrlock_write_add(value BIGINT) RETURNS VOID
    AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_lrlock_write_no_publish(n BIGINT) RETURNS VOID
    AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_lrlock_publish() RETURNS VOID
    AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_lrlock_stress(nops INT) RETURNS BIGINT
    AS 'MODULE_PATHNAME' LANGUAGE C;
