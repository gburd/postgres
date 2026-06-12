/* src/test/modules/test_backend_runtime/test_backend_runtime--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_backend_runtime" to load this file. \quit

CREATE FUNCTION test_backend_exit_runtime_continuation()
	RETURNS pg_catalog.int4
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_backend_dsm_shutdown_is_backend_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_backend_interrupt_wakes_target_latch()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_backend_thread_create_join()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_backend_thread_exit_join()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_backend_thread_runtime_state()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_backend_pgproc_has_logical_id()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_backend_thread_ids_are_logical()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_backend_interrupt_holdoffs_are_backend_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_backend_pending_interrupts_are_backend_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_backend_core_state_is_backend_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_execution_debug_query_string_is_execution_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_execution_error_state_is_execution_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_connection_socket_io_is_connection_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_connection_protocol_state_is_connection_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_connection_identity_state_is_connection_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_connection_interrupt_state_is_connection_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_connection_frontend_protocol_is_connection_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_connection_startup_state_is_connection_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_client_connection_info_is_connection_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;
