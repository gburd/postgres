/* contrib/pg_bp_clir/pg_bp_clir--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_bp_clir" to load this file. \quit

CREATE FUNCTION clir_pool_handler(internal) RETURNS internal
    AS 'MODULE_PATHNAME' LANGUAGE C VOLATILE STRICT;

CREATE FUNCTION pg_stat_get_clir_stats()
    RETURNS SETOF record
    AS 'MODULE_PATHNAME' LANGUAGE C VOLATILE ROWS 10;

CREATE VIEW pg_stat_clir AS
    SELECT s.* FROM pg_stat_get_clir_stats() AS s(
        name name, oid oid,
        lir_size int4, hir_size int4,
        lir_resident int4, hir_resident int4,
        lookups int8, lir_hits int8, hir_hits int8,
        ghost_hits int8, misses int8,
        evictions int8, promotions int8, demotions int8,
        clock_walks int8);

REVOKE ALL ON pg_stat_clir FROM PUBLIC;
GRANT SELECT ON pg_stat_clir TO pg_monitor;
