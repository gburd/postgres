/* contrib/pg_bp_lirs/pg_bp_lirs--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_bp_lirs" to load this file. \quit

CREATE FUNCTION lirs_pool_handler(internal) RETURNS internal
    AS 'MODULE_PATHNAME' LANGUAGE C VOLATILE STRICT;

CREATE FUNCTION pg_stat_get_lirs_stats()
    RETURNS SETOF record
    AS 'MODULE_PATHNAME' LANGUAGE C VOLATILE ROWS 10;

CREATE VIEW pg_stat_lirs AS
    SELECT s.* FROM pg_stat_get_lirs_stats() AS s(
        name name, oid oid,
        lir_size int4, hir_size int4, ghost_size int4,
        lir_capacity int4, stack_size int4, q_size int4,
        lookups int8, lir_hits int8, hir_hits int8,
        ghost_hits int8, misses int8,
        lir_demotions int8, hir_promotions int8,
        evictions int8, stack_prunes int8);

REVOKE ALL ON pg_stat_lirs FROM PUBLIC;
GRANT SELECT ON pg_stat_lirs TO pg_monitor;

CREATE FUNCTION pg_bp_lirs_size_recommendation(name)
    RETURNS record
    AS 'MODULE_PATHNAME' LANGUAGE C VOLATILE STRICT;
