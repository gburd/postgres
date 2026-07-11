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

CREATE FUNCTION reset_exit_callback_order() RETURNS VOID
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION register_exit_callback_order() RETURNS VOID
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION get_exit_callback_order() RETURNS TEXT
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION reset_backend_exit_temp_file_path() RETURNS VOID
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION create_temp_file_for_backend_exit() RETURNS VOID
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION backend_exit_temp_file_removed() RETURNS BOOL
	AS 'MODULE_PATHNAME' LANGUAGE C;
