/* contrib/pg_bp_osic/pg_bp_osic--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_bp_osic" to load this file. \quit

-- OSIC pool handler function
CREATE FUNCTION osic_pool_handler(internal)
RETURNS internal
AS 'MODULE_PATHNAME', 'osic_pool_handler'
LANGUAGE C STRICT;

-- OSIC statistics function
CREATE FUNCTION pg_stat_get_osic_stats(OUT pool_name name,
                                       OUT pool_oid oid,
                                       OUT nbuffers int4,
                                       OUT hot_count int4,
                                       OUT cool_count int4,
                                       OUT hits int8,
                                       OUT misses int8,
                                       OUT evictions int8,
                                       OUT cooling_sweeps int8)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'pg_stat_get_osic_stats'
LANGUAGE C STRICT VOLATILE;

-- Convenience view
CREATE VIEW pg_stat_osic AS
    SELECT * FROM pg_stat_get_osic_stats();

GRANT SELECT ON pg_stat_osic TO pg_monitor;
