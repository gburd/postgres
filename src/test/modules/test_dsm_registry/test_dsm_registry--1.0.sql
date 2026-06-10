/* src/test/modules/test_dsm_registry/test_dsm_registry--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_dsm_registry" to load this file. \quit

CREATE FUNCTION set_val_in_shmem(val INT) RETURNS VOID
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION get_val_in_shmem() RETURNS INT
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION set_val_in_hash(key TEXT, val TEXT) RETURNS VOID
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION get_val_in_hash(key TEXT) RETURNS TEXT
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION reset_dsm_detach_count() RETURNS VOID
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION register_dsm_detach_for_backend_exit() RETURNS VOID
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION get_dsm_detach_count() RETURNS INT
	AS 'MODULE_PATHNAME' LANGUAGE C;
