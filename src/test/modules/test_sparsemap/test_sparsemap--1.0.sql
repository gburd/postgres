/* src/test/modules/test_sparsemap/test_sparsemap--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_sparsemap" to load this file. \quit

CREATE FUNCTION test_sparsemap()
RETURNS pg_catalog.void STRICT
AS 'MODULE_PATHNAME' LANGUAGE C;
