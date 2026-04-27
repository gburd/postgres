/* contrib/pg_bp_lru/pg_bp_lru--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_bp_lru" to load this file. \quit

-- LRU buffer pool replacement handler
CREATE FUNCTION lru_pool_handler(internal) RETURNS internal
AS 'MODULE_PATHNAME' LANGUAGE C STRICT;

-- LRU statistics function
CREATE FUNCTION pg_stat_get_lru_stats()
RETURNS SETOF record
AS 'MODULE_PATHNAME' LANGUAGE C STABLE PARALLEL RESTRICTED
ROWS 10;

-- LRU statistics view
CREATE VIEW pg_stat_lru AS
SELECT
    s.name,
    s.oid,
    s.list_size,
    s.hits,
    s.misses,
    s.evictions
FROM pg_stat_get_lru_stats() AS s(
    name name,
    oid oid,
    list_size int4,
    hits int8,
    misses int8,
    evictions int8
);

GRANT SELECT ON pg_stat_lru TO pg_monitor;
