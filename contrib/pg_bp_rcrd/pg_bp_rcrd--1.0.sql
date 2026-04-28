/* contrib/pg_bp_rcrd/pg_bp_rcrd--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_bp_rcrd" to load this file. \quit

CREATE FUNCTION rcrd_pool_handler(internal) RETURNS internal
    AS 'MODULE_PATHNAME' LANGUAGE C VOLATILE STRICT;

CREATE FUNCTION pg_stat_get_rcrd_stats()
    RETURNS SETOF record
    AS 'MODULE_PATHNAME' LANGUAGE C VOLATILE ROWS 10;

CREATE VIEW pg_stat_rcrd AS
    SELECT s.* FROM pg_stat_get_rcrd_stats() AS s(
        name name, oid oid,
        hot_size int4, cold_size int4, ghost_size int4,
        hot_capacity int4, r_size int4, q_size int4,
        lookups int8, hot_hits int8, cold_hits int8,
        ghost_hits int8, misses int8,
        demotions int8, promotions int8,
        evictions int8, threshold_raises int8);

REVOKE ALL ON pg_stat_rcrd FROM PUBLIC;
GRANT SELECT ON pg_stat_rcrd TO pg_monitor;

CREATE FUNCTION pg_bp_rcrd_size_recommendation(name)
    RETURNS record
    AS 'MODULE_PATHNAME' LANGUAGE C VOLATILE STRICT;
