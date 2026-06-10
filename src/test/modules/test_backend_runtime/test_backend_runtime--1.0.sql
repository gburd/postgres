/* src/test/modules/test_backend_runtime/test_backend_runtime--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_backend_runtime" to load this file. \quit

CREATE FUNCTION test_backend_exit_runtime_continuation()
	RETURNS pg_catalog.int4
	AS 'MODULE_PATHNAME' LANGUAGE C;
