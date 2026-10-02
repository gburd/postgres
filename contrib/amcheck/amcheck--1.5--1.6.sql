/* contrib/amcheck/amcheck--1.5--1.6.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION amcheck UPDATE TO '1.6'" to load this file. \quit


-- bark_index_check()
--
CREATE FUNCTION bark_index_check(index regclass)
RETURNS VOID
AS 'MODULE_PATHNAME', 'bark_index_check'
LANGUAGE C STRICT;

REVOKE ALL ON FUNCTION bark_index_check(regclass) FROM PUBLIC;
