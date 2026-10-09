/* contrib/pgstattuple/pgstattuple--1.5--1.6.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION pgstattuple UPDATE TO '1.6'" to load this file. \quit

CREATE FUNCTION pgstatbarkindex(IN relname regclass,
    OUT version INT4,
    OUT tree_level INT4,
    OUT index_size BIGINT,
    OUT root_block_no BIGINT,
    OUT internal_pages BIGINT,
    OUT leaf_pages BIGINT,
    OUT empty_pages BIGINT,
    OUT deleted_pages BIGINT,
    OUT overflow_pages BIGINT,
    OUT avg_leaf_density FLOAT8,
    OUT leaf_fragmentation FLOAT8,
    OUT single_entries BIGINT,
    OUT list_entries BIGINT,
    OUT posting_entries BIGINT,
    OUT oversized_entries BIGINT)
AS 'MODULE_PATHNAME', 'pgstatbarkindex'
LANGUAGE C STRICT PARALLEL SAFE;

REVOKE EXECUTE ON FUNCTION pgstatbarkindex(regclass) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pgstatbarkindex(regclass) TO pg_stat_scan_tables;
