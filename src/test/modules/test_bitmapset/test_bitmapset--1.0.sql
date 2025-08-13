/* src/test/modules/test_bitmapset/test_bitmapset--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_bitmapset" to load this file. \quit

CREATE FUNCTION test_bitmapset()
RETURNS pg_catalog.void STRICT
AS 'MODULE_PATHNAME' LANGUAGE C;
