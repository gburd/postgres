/* src/test/modules/test_atomics/test_atomics--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_atomics" to load this file. \quit

-- Test atomic flag operations
CREATE FUNCTION test_atomic_flag_operations(iterations int4)
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

-- Test uint32 atomic operations
CREATE FUNCTION test_atomic_uint32_operations(iterations int4)
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

-- Test uint64 atomic operations
CREATE FUNCTION test_atomic_uint64_operations(iterations int4)
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

-- Benchmark atomic operations
CREATE FUNCTION benchmark_atomic_operations(iterations int4)
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;
