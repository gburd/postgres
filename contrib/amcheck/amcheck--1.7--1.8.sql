/* contrib/amcheck/amcheck--1.7--1.8.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION amcheck UPDATE TO '1.8'" to load this file. \quit

-- bark_index_check() and bark_index_parent_check() with checkunique.  These
-- are new overloads, as amcheck 1.4 added for bt_index_check, so the older
-- forms keep working.

--
-- bark_index_check()
--
CREATE FUNCTION bark_index_check(index regclass,
    heapallindexed boolean, checkunique boolean)
RETURNS VOID
AS 'MODULE_PATHNAME', 'bark_index_check'
LANGUAGE C STRICT PARALLEL RESTRICTED;

--
-- bark_index_parent_check()
--
CREATE FUNCTION bark_index_parent_check(index regclass,
    heapallindexed boolean, checkunique boolean)
RETURNS VOID
AS 'MODULE_PATHNAME', 'bark_index_parent_check'
LANGUAGE C STRICT PARALLEL RESTRICTED;

-- We don't want this to be available to public
REVOKE ALL ON FUNCTION bark_index_check(regclass, boolean, boolean) FROM PUBLIC;
REVOKE ALL ON FUNCTION bark_index_parent_check(regclass, boolean, boolean) FROM PUBLIC;
