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

CREATE FUNCTION test_backend_thread_split_initializers()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_carrier_threaded_guc_lock_depth_is_carrier_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_carrier_attach_detach_current_work()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_carrier_protocol_park_prepare_commit()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_protocol_scheduler_poll_buffered_read()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_protocol_read_wake_applies_backend_interrupt()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_carrier_misc_state_is_carrier_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_thread_install_adopts_backend_fallback_state()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_thread_install_adopts_session_execution_fallback_state()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_thread_install_adopts_connection_fallback_state()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_backend_pgproc_has_logical_id()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_backend_thread_ids_are_logical()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_session_loop_state_is_session_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_session_tcop_state_is_session_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_session_xact_callback_state_is_session_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_session_backup_state_is_session_local()
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

CREATE FUNCTION test_runtime_extension_module_state_is_runtime_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_session_guc_rebind_table_matches_registry()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_session_prepared_statement_state_is_session_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_session_invalidation_callback_state_is_session_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_session_ri_globals_state_is_session_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_session_relmap_state_is_session_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_session_reset_closed_state()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_session_on_commit_state_is_session_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_session_sequence_state_is_session_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_session_large_object_state_is_session_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_session_regex_portal_state_is_session_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_session_async_state_is_session_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_session_encoding_state_is_session_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_session_temp_file_state_is_session_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_session_random_state_is_session_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_session_optimizer_state_is_session_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_session_plan_cache_state_is_session_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_session_namespace_state_is_session_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_session_locale_state_is_session_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_session_catalog_lookup_state_is_session_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_session_extension_module_state_is_session_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_session_connection_guc_state_is_session_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_backend_extension_module_state_is_backend_local()
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

CREATE FUNCTION test_session_user_identity_state_is_session_local()
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

CREATE FUNCTION test_session_compat_guc_state_is_session_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_session_access_wal_guc_state_is_session_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_session_jit_guc_state_is_session_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_session_jit_provider_state_is_session_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_session_misc_guc_state_is_session_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_session_guc_state_is_session_local()
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

CREATE FUNCTION test_backend_exit_state_is_backend_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_backend_pgstat_pending_state_is_backend_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_backend_activity_state_is_backend_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_backend_memory_manager_state_is_backend_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_backend_utility_state_is_backend_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_backend_reset_closed_state()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_backend_parallel_state_is_backend_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_backend_instrumentation_state_is_backend_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_runtime_hot_bucket_cache_tracks_current_work()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_backend_buffer_state_is_backend_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_backend_storage_state_is_backend_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_backend_lock_state_is_backend_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_backend_wait_state_is_backend_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_backend_wait_completion_publication()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_backend_wait_completion_publication_policy()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_backend_ipc_state_is_backend_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_backend_transaction_state_is_backend_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_backend_timeout_state_is_backend_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_backend_walsender_state_is_backend_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_backend_replication_state_is_backend_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_backend_logical_replication_state_is_backend_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_backend_xlog_state_is_backend_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_backend_recovery_state_is_backend_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_backend_maintenance_worker_state_is_backend_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_backend_autovacuum_state_is_backend_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_backend_repack_state_is_backend_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_backend_aio_state_is_backend_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_pmchild_thread_backend_signal_api()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_pmchild_thread_backend_reset_api()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_pmchild_pooled_logical_backend_signal_api()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_pmchild_thread_backend_publication_race()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_backend_core_state_is_backend_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_backend_command_log_state_is_backend_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_backend_expr_interp_state_is_backend_local()
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

CREATE FUNCTION test_execution_spi_state_is_execution_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_execution_active_portal_is_execution_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_execution_reset_closed_state()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_execution_event_trigger_query_state_reset()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_backend_runtime_noop_event_trigger()
	RETURNS event_trigger
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_execution_vacuum_state_is_execution_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_execution_node_io_state_is_execution_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_execution_basebackup_state_is_execution_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_execution_analyze_state_is_execution_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_execution_extension_state_is_execution_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_execution_matview_state_is_execution_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_execution_snapshot_combo_state_is_execution_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_execution_xloginsert_state_is_execution_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_execution_xact_state_is_execution_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_execution_transaction_cleanup_state_is_execution_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_execution_reporting_replication_state_is_execution_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_execution_guc_error_state_is_execution_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_execution_catalog_state_is_execution_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_execution_catalog_cache_state_is_execution_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_execution_relmap_state_is_execution_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_execution_inval_twophase_state_is_execution_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_execution_async_state_is_execution_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_execution_misc_scratch_state_is_execution_local()
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

CREATE FUNCTION test_connection_protocol_byte_probe()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_connection_reset_closed_state()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_connection_warning_state_is_connection_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION test_connection_output_state_is_connection_local()
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

CREATE FUNCTION test_connection_security_state_is_connection_local()
	RETURNS pg_catalog.bool
	AS 'MODULE_PATHNAME' LANGUAGE C;
