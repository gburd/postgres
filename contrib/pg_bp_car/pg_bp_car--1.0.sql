/* contrib/pg_bp_car/pg_bp_car--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_bp_car" to load this file. \quit

CREATE FUNCTION car_pool_handler(internal) RETURNS internal
    AS 'MODULE_PATHNAME' LANGUAGE C VOLATILE STRICT;

CREATE FUNCTION pg_stat_get_car_stats()
    RETURNS SETOF record
    AS 'MODULE_PATHNAME' LANGUAGE C VOLATILE ROWS 10;

CREATE VIEW pg_stat_car AS
    SELECT s.* FROM pg_stat_get_car_stats() AS s(
        name name, oid oid, t1_size int4, t2_size int4,
        b1_size int4, b2_size int4, target_t1_size int4,
        t1_hand int4, t2_hand int4,
        lookups int8, t1_hits int8, t2_hits int8,
        b1_hits int8, b2_hits int8, misses int8,
        t1_evictions int8, t2_evictions int8);

REVOKE ALL ON pg_stat_car FROM PUBLIC;
GRANT SELECT ON pg_stat_car TO pg_monitor;

CREATE FUNCTION pg_bp_car_size_recommendation(name)
    RETURNS record
    AS 'MODULE_PATHNAME' LANGUAGE C VOLATILE STRICT;
