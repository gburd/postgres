/* src/test/modules/test_backend_runtime/test_backend_runtime_threaded--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_backend_runtime_threaded" to load this file. \quit

CREATE FUNCTION test_backend_runtime_model_snapshot()
	RETURNS pg_catalog.text
	AS 'MODULE_PATHNAME',
	   'test_backend_runtime_model_snapshot'
	LANGUAGE C;

CREATE FUNCTION test_backend_runtime_request_autovacuum_worker()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME',
	   'test_backend_runtime_request_autovacuum_worker'
	LANGUAGE C;

CREATE FUNCTION test_backend_runtime_rejects_process_bgworker()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME',
	   'test_backend_runtime_rejects_process_bgworker'
	LANGUAGE C;

CREATE FUNCTION test_backend_runtime_launch_thread_bgworker()
	RETURNS pg_catalog.int4
	AS 'MODULE_PATHNAME',
	   'test_backend_runtime_launch_thread_bgworker'
	LANGUAGE C;

CREATE FUNCTION test_backend_runtime_restart_thread_bgworker()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME',
	   'test_backend_runtime_restart_thread_bgworker'
	LANGUAGE C;

CREATE FUNCTION test_backend_runtime_crash_thread_bgworker()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME',
	   'test_backend_runtime_crash_thread_bgworker'
	LANGUAGE C;

CREATE FUNCTION test_backend_runtime_custom_guc_value()
	RETURNS pg_catalog.text
	AS 'MODULE_PATHNAME',
	   'test_backend_runtime_custom_guc_value'
	LANGUAGE C;

CREATE FUNCTION test_backend_runtime_custom_guc_init_count()
	RETURNS pg_catalog.int4
	AS 'MODULE_PATHNAME',
	   'test_backend_runtime_custom_guc_init_count'
	LANGUAGE C;

CREATE FUNCTION test_backend_runtime_emit_fatal()
	RETURNS pg_catalog.void
	AS 'MODULE_PATHNAME',
	   'test_backend_runtime_emit_fatal'
	LANGUAGE C;

CREATE FUNCTION test_backend_runtime_crash_current_backend()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME',
	   'test_backend_runtime_crash_current_backend'
	LANGUAGE C;

CREATE FUNCTION test_backend_runtime_wait_completion_enabled()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME',
	   'test_backend_runtime_wait_completion_enabled'
	LANGUAGE C;

CREATE FUNCTION test_backend_runtime_wait_completion_snapshot(pg_catalog.int4)
	RETURNS pg_catalog.text
	AS 'MODULE_PATHNAME',
	   'test_backend_runtime_wait_completion_snapshot'
	LANGUAGE C;

CREATE FUNCTION test_backend_runtime_protocol_park_snapshot(pg_catalog.int4)
	RETURNS pg_catalog.text
	AS 'MODULE_PATHNAME',
	   'test_backend_runtime_protocol_park_snapshot'
	LANGUAGE C;

CREATE FUNCTION test_backend_runtime_wait_on_condition_variable(pg_catalog.int4)
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME',
	   'test_backend_runtime_wait_on_condition_variable'
	LANGUAGE C;

CREATE FUNCTION test_backend_runtime_hold_lwlock(pg_catalog.int4)
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME',
	   'test_backend_runtime_hold_lwlock'
	LANGUAGE C;

CREATE FUNCTION test_backend_runtime_wait_on_lwlock()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME',
	   'test_backend_runtime_wait_on_lwlock'
	LANGUAGE C;
