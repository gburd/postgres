/* contrib/amcheck/amcheck--1.6--1.7.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION amcheck UPDATE TO '1.7'" to load this file. \quit


-- bark_index_check(), with heapallindexed.  The 1.6 one-argument form is kept
-- beside it, as bt_index_check's older forms are.
--
CREATE FUNCTION bark_index_check(index regclass,
    heapallindexed boolean)
RETURNS VOID
AS 'MODULE_PATHNAME', 'bark_index_check'
LANGUAGE C STRICT PARALLEL RESTRICTED;

--
-- bark_index_parent_check()
--
CREATE FUNCTION bark_index_parent_check(index regclass,
    heapallindexed boolean DEFAULT false)
RETURNS VOID
AS 'MODULE_PATHNAME', 'bark_index_parent_check'
LANGUAGE C STRICT PARALLEL RESTRICTED;

REVOKE ALL ON FUNCTION bark_index_check(regclass, boolean) FROM PUBLIC;
REVOKE ALL ON FUNCTION bark_index_parent_check(regclass, boolean) FROM PUBLIC;
