/*--------------------------------------------------------------------------
 *
 * test_backend_runtime.c
 *		Test backend runtime scaffolding
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_backend_runtime/test_backend_runtime.c
 *
 * -------------------------------------------------------------------------
 */
#include "test_backend_runtime.h"

PG_MODULE_MAGIC;

static sigjmp_buf exit_continuation_jmp;
static volatile bool exit_continuation_seen;
static volatile int exit_continuation_code;

static void
test_exit_backend(int code)
{
	exit_continuation_seen = true;
	exit_continuation_code = code;
	siglongjmp(exit_continuation_jmp, 1);
}

PG_FUNCTION_INFO_V1(test_backend_runtime_noop_event_trigger);
Datum
test_backend_runtime_noop_event_trigger(PG_FUNCTION_ARGS)
{
	if (!CALLED_AS_EVENT_TRIGGER(fcinfo))
		elog(ERROR, "not called as an event trigger");

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(test_backend_exit_runtime_continuation);
Datum
test_backend_exit_runtime_continuation(PG_FUNCTION_ARGS)
{
	PgRuntime  *runtime;
	PgBackendExitContinuation saved_exit_backend;
	volatile bool continued;

	if (CurrentPgRuntime == NULL)
		elog(ERROR, "current backend runtime is not initialized");

	runtime = CurrentPgRuntime;
	saved_exit_backend = runtime->exit_backend;
	exit_continuation_seen = false;
	exit_continuation_code = 0;
	continued = false;

	/*
	 * Test the post-cleanup runtime handoff directly.  Calling full
	 * PgBackendExit() here would run backend cleanup and then jump back into a
	 * backend stack that had already been torn down.
	 */
	runtime->exit_backend = test_exit_backend;
	if (sigsetjmp(exit_continuation_jmp, 1) == 0)
		PgBackendExitComplete(17);
	else
		continued = true;
	runtime->exit_backend = saved_exit_backend;

	if (!continued)
		elog(ERROR, "backend exit continuation did not transfer control");
	if (!exit_continuation_seen)
		elog(ERROR, "backend exit continuation was not called");
	if (exit_continuation_code != 17)
		elog(ERROR, "backend exit continuation saw code %d, expected 17",
			 exit_continuation_code);

	PG_RETURN_INT32(exit_continuation_code);
}

PG_FUNCTION_INFO_V1(test_backend_dsm_shutdown_is_backend_local);
Datum
test_backend_dsm_shutdown_is_backend_local(PG_FUNCTION_ARGS)
{
	PgBackend  *saved_backend;
	PgBackend	fake_backend_with_dsm;
	PgBackend	fake_backend_to_exit;
	dsm_segment *seg = NULL;
	dsm_handle	handle = DSM_HANDLE_INVALID;
	bool		found = false;

	saved_backend = CurrentPgBackend;
	MemSet(&fake_backend_with_dsm, 0, sizeof(fake_backend_with_dsm));
	MemSet(&fake_backend_to_exit, 0, sizeof(fake_backend_to_exit));
	dlist_init(&fake_backend_with_dsm.dsm_segment_list);
	dlist_init(&fake_backend_to_exit.dsm_segment_list);

	PG_TRY();
	{
		/*
		 * Simulate two logical backends in one address space.  Only
		 * CurrentPgBackend is switched because this test isolates DSM mapping
		 * ownership; the rest of the current process runtime remains real.
		 */
		CurrentPgBackend = &fake_backend_with_dsm;
		pg_prng_seed(&pg_global_prng_state, 1);
		seg = dsm_create(1024, 0);
		dsm_pin_mapping(seg);
		handle = dsm_segment_handle(seg);

		CurrentPgBackend = &fake_backend_to_exit;
		dsm_backend_shutdown();

		CurrentPgBackend = &fake_backend_with_dsm;
		found = (dsm_find_mapping(handle) == seg);
		dsm_detach(seg);
		seg = NULL;

		CurrentPgBackend = saved_backend;
	}
	PG_CATCH();
	{
		if (seg != NULL)
		{
			CurrentPgBackend = &fake_backend_with_dsm;
			dsm_detach(seg);
		}
		CurrentPgBackend = saved_backend;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!found)
		elog(ERROR, "DSM shutdown for one backend detached another backend's mapping");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_thread_install_adopts_backend_fallback_state);
Datum
test_thread_install_adopts_backend_fallback_state(PG_FUNCTION_ARGS)
{
	PgBackend  *saved_backend;
	PgThreadBackendRuntimeState state;
	Latch		fake_latch;
	bool		ok = true;

	saved_backend = CurrentPgBackend;

	InitLatch(&fake_latch);

	PG_TRY();
	{
		CurrentPgBackend = NULL;
		PgCurrentWalSenderState()->is_walsender = true;
		PgCurrentReplicationState()->sync_rep_wait_mode = 101;
		PgCurrentLogicalReplicationState()->slotsync_sleep_ms = 102;
		PgCurrentXLogState()->local_xlog_insert_allowed = 103;
		PgCurrentRecoveryState()->standby_wait_us = 104;
		PgCurrentMaintenanceWorkerState()->walsummarizer_sleep_quanta = 105;
		PgCurrentAutovacuumState()->av_storage_param_cost_limit = 106;
		PgCurrentRepackState()->current_segment = 107;
		PgCurrentAioState()->my_io_worker_id = 108;
		PgCurrentBackendExtensionModuleState()->pg_stash_advice_state =
			(struct pgsa_shared_state *) &state;
		InterruptPending = true;
		InterruptHoldoffCount = 109;

		InitializePgThreadRuntime(NULL);
		InitializePgThreadBackendRuntimeState(&state, B_BACKEND, NULL,
											  &fake_latch);
		PgBackendAdoptEarlyState(&state.backend);

		ok = ok && state.backend.walsender.is_walsender;
		ok = ok && state.backend.replication.sync_rep_wait_mode == 101;
		ok = ok && state.backend.logical_replication.slotsync_sleep_ms == 102;
		ok = ok && dlist_is_empty(&state.backend.logical_replication.lsn_mapping);
		ok = ok && state.backend.xlog.local_xlog_insert_allowed == 103;
		ok = ok && state.backend.recovery.standby_wait_us == 104;
		ok = ok &&
			state.backend.maintenance_worker.walsummarizer_sleep_quanta == 105;
		ok = ok && state.backend.autovacuum.av_storage_param_cost_limit == 106;
		ok = ok && dlist_is_empty(&state.backend.autovacuum.database_list);
		ok = ok && state.backend.repack.current_segment == 107;
		ok = ok && state.backend.aio.my_io_worker_id == 108;
		ok = ok && state.backend.pending_interrupts.interrupt_pending;
		ok = ok &&
			state.backend.interrupt_holdoffs.interrupt_holdoff_count == 109;

		ok = ok && !PgCurrentWalSenderState()->is_walsender;
		ok = ok && PgCurrentReplicationState()->sync_rep_wait_mode == -1;
		ok = ok && PgCurrentLogicalReplicationState()->slotsync_sleep_ms ==
			PG_BACKEND_SLOTSYNC_INITIAL_SLEEP_MS;
		ok = ok && dlist_is_empty(&PgCurrentLogicalReplicationState()->lsn_mapping);
		ok = ok && PgCurrentXLogState()->local_xlog_insert_allowed == -1;
		ok = ok && PgCurrentRecoveryState()->standby_wait_us ==
			PG_BACKEND_STANDBY_INITIAL_WAIT_US;
		ok = ok &&
			PgCurrentMaintenanceWorkerState()->walsummarizer_sleep_quanta == 1;
		ok = ok && PgCurrentAutovacuumState()->av_storage_param_cost_limit == -1;
		ok = ok && dlist_is_empty(&PgCurrentAutovacuumState()->database_list);
		ok = ok && PgCurrentRepackState()->current_segment == 0;
		ok = ok && PgCurrentAioState()->my_io_worker_id == -1;
		ok = ok &&
			PgCurrentBackendExtensionModuleState()->pg_stash_advice_state == NULL;
		ok = ok && !InterruptPending;
		ok = ok && InterruptHoldoffCount == 0;

		CurrentPgBackend = saved_backend;
	}
	PG_CATCH();
	{
		CurrentPgBackend = saved_backend;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "thread backend install did not adopt backend fallback state");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_thread_install_adopts_session_execution_fallback_state);
Datum
test_thread_install_adopts_session_execution_fallback_state(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgExecution *saved_execution;
	PgSession	session;
	PgExecution execution;
	MemoryContext saved_top_memory_context;
	MemoryContext saved_current_memory_context;
	MemoryContext saved_error_context;
	MemoryContext saved_message_context;
	MemoryContext saved_top_transaction_context;
	MemoryContext saved_cur_transaction_context;
	MemoryContext saved_portal_context;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_execution = CurrentPgExecution;
	saved_top_memory_context = TopMemoryContext;
	saved_current_memory_context = CurrentMemoryContext;
	saved_error_context = ErrorContext;
	saved_message_context = MessageContext;
	saved_top_transaction_context = TopTransactionContext;
	saved_cur_transaction_context = CurTransactionContext;
	saved_portal_context = PortalContext;
	MemSet(&session, 0, sizeof(session));
	MemSet(&execution, 0, sizeof(execution));

	PG_TRY();
	{
		PgSetCurrentSession(NULL);
		CurrentPgExecution = NULL;
		TopMemoryContext = saved_top_memory_context;
		CurrentMemoryContext = saved_current_memory_context;
		ErrorContext = saved_error_context;
		MessageContext = saved_message_context;
		TopTransactionContext = saved_top_transaction_context;
		CurTransactionContext = saved_cur_transaction_context;
		PortalContext = saved_portal_context;
		*PgCurrentDoingCommandReadRef() = true;
		*PgCurrentClientEncodingRef() = &pg_enc2name_tbl[PG_UTF8];
		*PgCurrentPendingClientEncodingRef() = PG_UTF8;
		*PgCurrentPseudoRandomSeedSetRef() = true;
		*PgCurrentDebugQueryStringRef() = "aggregate execution fallback";
		*PgCurrentSPIConnectedRef() = 17;
		*PgCurrentXactIsoLevelRef() = XACT_SERIALIZABLE;
		*PgCurrentGUCCheckErrcodeValueRef() = 503;
		*PgCurrentPendingActionsRef() = (struct ActionList *) &execution;
		*PgCurrentPendingListenActionsRef() = (HTAB *) &execution;
		*PgCurrentPendingNotifiesRef() = (struct NotificationList *) &execution;
		PgCurrentQueueHeadBeforeWriteRef()->page = 11;
		PgCurrentQueueHeadBeforeWriteRef()->offset = 12;
		PgCurrentQueueHeadAfterWriteRef()->page = 13;
		PgCurrentQueueHeadAfterWriteRef()->offset = 14;
		*PgCurrentSignalPidsRef() = (int32 *) &execution;
		*PgCurrentSignalProcnosRef() = (ProcNumber *) &execution;
		*PgCurrentTryAdvanceTailRef() = true;
		*PgCurrentTriggerDepthRef() = 88;
		*PgCurrentAfterTriggersDataRef() = &execution;
		*PgCurrentValgrindOldErrorCountRef() = 77;

		PgSessionAdoptEarlyState(&session);
		PgExecutionAdoptEarlyState(&execution);

		ok = ok && execution.memory_contexts.top_context ==
			saved_top_memory_context;
		ok = ok && execution.memory_contexts.current_context ==
			saved_current_memory_context;
		ok = ok && execution.memory_contexts.error_context ==
			saved_error_context;
		ok = ok && session.loop_state.doing_command_read;
		ok = ok && session.encoding.client_encoding == &pg_enc2name_tbl[PG_UTF8];
		ok = ok && session.encoding.pending_client_encoding == PG_UTF8;
		ok = ok && session.random.prng_seed_set;
		ok = ok && strcmp(*PgExecutionDebugQueryStringRef(&execution),
						  "aggregate execution fallback") == 0;
		ok = ok && execution.spi.connected == 17;
		ok = ok && execution.xact.iso_level == XACT_SERIALIZABLE;
		ok = ok && execution.guc_error.check_errcode_value == 503;
		ok = ok && execution.async.pending_actions ==
			(struct ActionList *) &execution;
		ok = ok && execution.async.pending_listen_actions ==
			(HTAB *) &execution;
		ok = ok && execution.async.pending_notifies ==
			(struct NotificationList *) &execution;
		ok = ok && execution.async.queue_head_before_write.page == 11;
		ok = ok && execution.async.queue_head_before_write.offset == 12;
		ok = ok && execution.async.queue_head_after_write.page == 13;
		ok = ok && execution.async.queue_head_after_write.offset == 14;
		ok = ok && execution.async.signal_pids == (int32 *) &execution;
		ok = ok && execution.async.signal_procnos == (ProcNumber *) &execution;
		ok = ok && execution.async.try_advance_tail;
		ok = ok && execution.trigger.depth == 88;
		ok = ok && execution.trigger.after_triggers_data == &execution;
		ok = ok && execution.valgrind.old_error_count == 77;

		TopMemoryContext = saved_top_memory_context;
		CurrentMemoryContext = saved_current_memory_context;
		ErrorContext = saved_error_context;
		MessageContext = saved_message_context;
		TopTransactionContext = saved_top_transaction_context;
		CurTransactionContext = saved_cur_transaction_context;
		PortalContext = saved_portal_context;

		ok = ok && !*PgCurrentDoingCommandReadRef();
		ok = ok && *PgCurrentClientEncodingRef() ==
			&pg_enc2name_tbl[PG_SQL_ASCII];
		ok = ok && *PgCurrentPendingClientEncodingRef() == PG_SQL_ASCII;
		ok = ok && !*PgCurrentPseudoRandomSeedSetRef();
		ok = ok && *PgCurrentDebugQueryStringRef() == NULL;
		ok = ok && *PgCurrentSPIConnectedRef() == -1;
		ok = ok && *PgCurrentXactIsoLevelRef() == XACT_READ_COMMITTED;
		ok = ok && *PgCurrentGUCCheckErrcodeValueRef() == 0;
		ok = ok && *PgCurrentPendingActionsRef() == NULL;
		ok = ok && *PgCurrentPendingListenActionsRef() == NULL;
		ok = ok && *PgCurrentPendingNotifiesRef() == NULL;
		ok = ok && PgCurrentQueueHeadBeforeWriteRef()->page == 0;
		ok = ok && PgCurrentQueueHeadBeforeWriteRef()->offset == 0;
		ok = ok && PgCurrentQueueHeadAfterWriteRef()->page == 0;
		ok = ok && PgCurrentQueueHeadAfterWriteRef()->offset == 0;
		ok = ok && *PgCurrentSignalPidsRef() == NULL;
		ok = ok && *PgCurrentSignalProcnosRef() == NULL;
		ok = ok && !*PgCurrentTryAdvanceTailRef();
		ok = ok && *PgCurrentTriggerDepthRef() == 0;
		ok = ok && *PgCurrentAfterTriggersDataRef() == NULL;
		ok = ok && *PgCurrentValgrindOldErrorCountRef() == 0;

		PgSetCurrentSession(saved_session);
		CurrentPgExecution = saved_execution;
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		CurrentPgExecution = saved_execution;
		TopMemoryContext = saved_top_memory_context;
		CurrentMemoryContext = saved_current_memory_context;
		ErrorContext = saved_error_context;
		MessageContext = saved_message_context;
		TopTransactionContext = saved_top_transaction_context;
		CurTransactionContext = saved_cur_transaction_context;
		PortalContext = saved_portal_context;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR,
			 "thread backend install did not adopt session/execution fallback state");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_thread_install_adopts_connection_fallback_state);
Datum
test_thread_install_adopts_connection_fallback_state(PG_FUNCTION_ARGS)
{
	PgConnection *saved_connection;
	PgConnection connection;
	Port		fallback_port;
	Port		preserved_port;
	const PQcommMethods methods = {0};
	WaitEventSet *fake_wait_set;
	PgConnectionSocketIOState *socket_io;
	PgConnectionSecurityState *security;
	bool		ok = true;

	saved_connection = CurrentPgConnection;
	MemSet(&connection, 0, sizeof(connection));
	MemSet(&fallback_port, 0, sizeof(fallback_port));
	MemSet(&preserved_port, 0, sizeof(preserved_port));
	fake_wait_set = (WaitEventSet *) &connection;

	PG_TRY();
	{
		CurrentPgConnection = NULL;
		MyProcPort = &fallback_port;
		MyCancelKey[0] = 11;
		MyCancelKeyLength = 1;
		socket_io = PgCurrentConnectionSocketIORef();
		socket_io->send_buffer = (char *) "connection fallback";
		socket_io->send_buffer_size = 32;
		PqCommMethods = &methods;
		FeBeWaitSet = fake_wait_set;
		FrontendProtocol = PG_PROTOCOL(3, 2);
		whereToSendOutput = DestRemote;
		client_connection_check_interval = 13;
		CheckClientConnectionPending = true;
		ClientConnectionLost = true;
		ClientAuthInProgress = true;
		MyClientSocket = (struct ClientSocket *) &fallback_port;
		conn_timing.socket_create = 21;
		conn_timing.ready_for_use = 22;
		MyClientConnectionInfo.authn_id = "fallback-authn";
		MyClientConnectionInfo.auth_method = uaSCRAM;
		security = PgCurrentConnectionSecurityStateRef();
		security->ssl_loaded_verify_locations = true;
		security->gss_send_buffer = (char *) "gss-send";
		security->gss_send_length = 31;
		security->pam_password = "pam-fallback";
		security->pam_port = &fallback_port;
		security->pam_no_password = true;

		PgConnectionAdoptEarlyState(&connection, &preserved_port);

		CurrentPgConnection = &connection;
		ok = ok && MyProcPort == &preserved_port;
		ok = ok && MyCancelKey[0] == 11;
		ok = ok && MyCancelKeyLength == 1;
		ok = ok && strcmp(PgCurrentConnectionSocketIORef()->send_buffer,
						  "connection fallback") == 0;
		ok = ok && PgCurrentConnectionSocketIORef()->send_buffer_size == 32;
		ok = ok && PqCommMethods == &methods;
		ok = ok && FeBeWaitSet == fake_wait_set;
		ok = ok && FrontendProtocol == PG_PROTOCOL(3, 2);
		ok = ok && whereToSendOutput == DestRemote;
		ok = ok && client_connection_check_interval == 13;
		ok = ok && CheckClientConnectionPending;
		ok = ok && ClientConnectionLost;
		ok = ok && ClientAuthInProgress;
		ok = ok && MyClientSocket == (struct ClientSocket *) &fallback_port;
		ok = ok && conn_timing.socket_create == 21;
		ok = ok && conn_timing.ready_for_use == 22;
		ok = ok && strcmp(MyClientConnectionInfo.authn_id,
						  "fallback-authn") == 0;
		ok = ok && MyClientConnectionInfo.auth_method == uaSCRAM;
		ok = ok && PgCurrentConnectionSecurityStateRef()->ssl_loaded_verify_locations;
		ok = ok && strcmp(PgCurrentConnectionSecurityStateRef()->gss_send_buffer,
						  "gss-send") == 0;
		ok = ok && PgCurrentConnectionSecurityStateRef()->gss_send_length == 31;
		ok = ok && strcmp(PgCurrentConnectionSecurityStateRef()->pam_password,
						  "pam-fallback") == 0;
		ok = ok && PgCurrentConnectionSecurityStateRef()->pam_port ==
			&fallback_port;
		ok = ok && PgCurrentConnectionSecurityStateRef()->pam_no_password;

		CurrentPgConnection = NULL;
		ok = ok && MyProcPort == NULL;
		ok = ok && MyCancelKeyLength == 0;
		ok = ok && PgCurrentConnectionSocketIORef()->send_buffer == NULL;
		ok = ok && PgCurrentConnectionSocketIORef()->send_buffer_size == 0;
		ok = ok && PqCommMethods == NULL;
		ok = ok && FeBeWaitSet == NULL;
		ok = ok && FrontendProtocol == 0;
		ok = ok && whereToSendOutput == DestDebug;
		ok = ok && client_connection_check_interval == 0;
		ok = ok && !CheckClientConnectionPending;
		ok = ok && !ClientConnectionLost;
		ok = ok && !ClientAuthInProgress;
		ok = ok && MyClientSocket == NULL;
		ok = ok && conn_timing.socket_create == 0;
		ok = ok && conn_timing.ready_for_use == TIMESTAMP_MINUS_INFINITY;
		ok = ok && MyClientConnectionInfo.authn_id == NULL;
		ok = ok && !PgCurrentConnectionSecurityStateRef()->ssl_loaded_verify_locations;
		ok = ok && PgCurrentConnectionSecurityStateRef()->gss_send_buffer == NULL;
		ok = ok && PgCurrentConnectionSecurityStateRef()->pam_password == NULL;

		CurrentPgConnection = saved_connection;
	}
	PG_CATCH();
	{
		CurrentPgConnection = saved_connection;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR,
			 "thread backend install did not adopt connection fallback state");

	PG_RETURN_BOOL(true);
}
