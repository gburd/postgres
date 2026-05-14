/* src/test/modules/test_skiplist/test_skiplist--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_skiplist" to load this file. \quit

CREATE FUNCTION test_skiplist()
RETURNS pg_catalog.void STRICT
AS 'MODULE_PATHNAME' LANGUAGE C;
