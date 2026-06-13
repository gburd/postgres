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

CREATE FUNCTION test_session_database_state_is_session_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_session_tablespace_state_is_session_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_session_binary_upgrade_state_is_session_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_session_datetime_state_is_session_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_session_text_search_state_is_session_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_runtime_server_guc_state_is_runtime_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_session_connection_guc_state_is_session_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_session_parser_state_is_session_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_session_vacuum_state_is_session_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_session_buffer_io_state_is_session_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_session_xact_defaults_are_session_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_session_lock_wait_state_is_session_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_session_logging_state_is_session_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_session_pgstat_state_is_session_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_session_query_id_state_is_session_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_session_storage_guc_state_is_session_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_session_user_guc_state_is_session_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_session_command_guc_state_is_session_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_session_replication_guc_state_is_session_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_session_general_guc_state_is_session_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_session_access_wal_guc_state_is_session_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_session_jit_guc_state_is_session_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_session_misc_guc_state_is_session_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_session_sort_guc_state_is_session_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_session_query_memory_state_is_session_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_session_planner_cost_state_is_session_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_session_planner_method_state_is_session_local()
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

CREATE FUNCTION test_execution_memory_contexts_are_execution_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_execution_resource_owners_are_execution_local()
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
