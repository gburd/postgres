/*--------------------------------------------------------------------------
 *
 * test_backend_runtime_adoption.c
 *		Thread-install fallback adoption tests.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_backend_runtime/test_backend_runtime_adoption.c
 *
 * -------------------------------------------------------------------------
 */
#include "test_backend_runtime.h"

PG_FUNCTION_INFO_V1(test_thread_install_adopts_backend_fallback_state);
Datum
test_thread_install_adopts_backend_fallback_state(PG_FUNCTION_ARGS)
{
	PgBackend  *saved_backend;
	PgThreadBackendRuntimeState state;
	Latch		fake_latch;
	void	  **extension_slot;
	const char *extension_key = "test_backend_runtime.adoption";
	bool		ok = true;

	saved_backend = CurrentPgBackend;

	InitLatch(&fake_latch);

	PG_TRY();
	{
		PgSetCurrentBackend(NULL);
		PgCurrentWalSenderState()->is_walsender = true;
		PgCurrentReplicationState()->sync_rep_wait_mode = 101;
		PgCurrentLogicalReplicationState()->slotsync_sleep_ms = 102;
		PgCurrentXLogState()->local_xlog_insert_allowed = 103;
		PgCurrentRecoveryState()->standby_wait_us = 104;
		PgCurrentMaintenanceWorkerState()->walsummarizer_sleep_quanta = 105;
		PgCurrentAutovacuumState()->av_storage_param_cost_limit = 106;
		PgCurrentRepackState()->current_segment = 107;
		PgCurrentAioState()->my_io_worker_id = 108;
		extension_slot = (void **)
			PgBackendEnsureExtensionPrivateState(extension_key,
												 sizeof(void *),
												 NULL);
		*extension_slot = &state;
		InterruptPending = true;
		InterruptHoldoffCount = 109;

		InitializePgThreadRuntime(NULL);
		InitializePgThreadBackendRuntimeState(&state, B_BACKEND, NULL,
											  &fake_latch);
		PgBackendAdoptEarlyState(&state.logical.backend);

		ok = ok && state.logical.backend.walsender.is_walsender;
		ok = ok && state.logical.backend.replication.sync_rep_wait_mode == 101;
		ok = ok && state.logical.backend.logical_replication.slotsync_sleep_ms == 102;
		ok = ok && dlist_is_empty(&state.logical.backend.logical_replication.lsn_mapping);
		ok = ok && state.logical.backend.xlog.local_xlog_insert_allowed == 103;
		ok = ok && state.logical.backend.recovery.standby_wait_us == 104;
		ok = ok &&
			state.logical.backend.maintenance_worker.walsummarizer_sleep_quanta == 105;
		ok = ok && state.logical.backend.autovacuum.av_storage_param_cost_limit == 106;
		ok = ok && dlist_is_empty(&state.logical.backend.autovacuum.database_list);
		ok = ok && state.logical.backend.repack.current_segment == 107;
		ok = ok && state.logical.backend.aio.my_io_worker_id == 108;
		PgSetCurrentBackend(&state.logical.backend);
		extension_slot = (void **) PgBackendGetExtensionPrivateState(extension_key);
		ok = ok && extension_slot != NULL && *extension_slot == &state;
		PgSetCurrentBackend(NULL);
		ok = ok && state.logical.backend.pending_interrupts.interrupt_pending;
		ok = ok &&
			state.logical.backend.interrupt_holdoffs.interrupt_holdoff_count == 109;

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
		ok = ok && PgBackendGetExtensionPrivateState(extension_key) == NULL;
		ok = ok && !InterruptPending;
		ok = ok && InterruptHoldoffCount == 0;

		PgSetCurrentBackend(saved_backend);
	}
	PG_CATCH();
	{
		PgSetCurrentBackend(saved_backend);
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
		PgSetCurrentExecution(NULL);
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
		*PgCurrentXLogInsertRegisteredBuffersRef() = NULL;
		*PgCurrentXLogInsertMaxRegisteredBuffersRef() = 0;
		*PgCurrentXLogInsertRDatasRef() = NULL;
		*PgCurrentXLogInsertMaxRDatasRef() = 0;
		*PgCurrentXLogInsertMainRDataLastRef() =
			(XLogRecData *) PgCurrentXLogInsertMainRDataHeadRef();
		*PgCurrentXLogInsertContextRef() = NULL;

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
		ok = ok && execution.xloginsert.registered_buffers == NULL;
		ok = ok && execution.xloginsert.max_registered_buffers == 0;
		ok = ok && execution.xloginsert.rdatas == NULL;
		ok = ok && execution.xloginsert.max_rdatas == 0;
		ok = ok && execution.xloginsert.mainrdata_head == NULL;
		ok = ok && execution.xloginsert.mainrdata_last == NULL;
		ok = ok && execution.xloginsert.context == NULL;

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
		PgSetCurrentExecution(saved_execution);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		PgSetCurrentExecution(saved_execution);
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
