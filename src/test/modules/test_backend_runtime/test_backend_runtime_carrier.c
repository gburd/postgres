/*--------------------------------------------------------------------------
 *
 * test_backend_runtime_carrier.c
 *		Carrier-owned runtime state tests.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_backend_runtime/test_backend_runtime_carrier.c
 *
 * -------------------------------------------------------------------------
 */
#include "test_backend_runtime.h"

PG_FUNCTION_INFO_V1(test_carrier_misc_state_is_carrier_local);
Datum
test_carrier_misc_state_is_carrier_local(PG_FUNCTION_ARGS)
{
	PgCarrier  *saved_carrier;
	PgCarrier	fake_carrier1;
	PgCarrier	fake_carrier2;
	char		stack_marker1;
	char		stack_marker2;
	void	   *thread_start1 = &fake_carrier1;
	void	   *thread_start2 = &fake_carrier2;
	bool		saved_is_under_postmaster;
	bool		ok = true;

	saved_carrier = CurrentPgCarrier;
	saved_is_under_postmaster = IsUnderPostmaster;
	MemSet(&fake_carrier1, 0, sizeof(fake_carrier1));
	MemSet(&fake_carrier2, 0, sizeof(fake_carrier2));
	fake_carrier1.kind = PG_CARRIER_THREAD;
	fake_carrier2.kind = PG_CARRIER_THREAD;
	fake_carrier1.wait_event_signal_fd = -1;
	fake_carrier1.wait_event_selfpipe_readfd = -1;
	fake_carrier1.wait_event_selfpipe_writefd = -1;
	fake_carrier2.wait_event_signal_fd = -1;
	fake_carrier2.wait_event_selfpipe_readfd = -1;
	fake_carrier2.wait_event_selfpipe_writefd = -1;

	PG_TRY();
	{
		PgSetCurrentCarrier(&fake_carrier1);
		*PgCurrentWaitEventWaitingRef() = true;
		*PgCurrentWaitEventSignalFdRef() = 11;
		*PgCurrentWaitEventSelfPipeReadFdRef() = 12;
		*PgCurrentWaitEventSelfPipeWriteFdRef() = 13;
		*PgCurrentWaitEventSelfPipeOwnerPidRef() = 14;
		*PgCurrentStackBasePtrRef() = &stack_marker1;
		*PgCurrentBackendThreadStartRef() = thread_start1;
		IsUnderPostmaster = true;

		PgSetCurrentCarrier(&fake_carrier2);
		ok = ok && *PgCurrentWaitEventWaitingRef() == false;
		ok = ok && *PgCurrentWaitEventSignalFdRef() == -1;
		ok = ok && *PgCurrentWaitEventSelfPipeReadFdRef() == -1;
		ok = ok && *PgCurrentWaitEventSelfPipeWriteFdRef() == -1;
		ok = ok && *PgCurrentWaitEventSelfPipeOwnerPidRef() == 0;
		ok = ok && *PgCurrentStackBasePtrRef() == NULL;
		ok = ok && *PgCurrentBackendThreadStartRef() == NULL;
		ok = ok && !IsUnderPostmaster;
		*PgCurrentWaitEventWaitingRef() = false;
		*PgCurrentWaitEventSignalFdRef() = 21;
		*PgCurrentWaitEventSelfPipeReadFdRef() = 22;
		*PgCurrentWaitEventSelfPipeWriteFdRef() = 23;
		*PgCurrentWaitEventSelfPipeOwnerPidRef() = 24;
		*PgCurrentStackBasePtrRef() = &stack_marker2;
		*PgCurrentBackendThreadStartRef() = thread_start2;
		IsUnderPostmaster = false;

		PgSetCurrentCarrier(&fake_carrier1);
		ok = ok && *PgCurrentWaitEventWaitingRef() == true;
		ok = ok && *PgCurrentWaitEventSignalFdRef() == 11;
		ok = ok && *PgCurrentWaitEventSelfPipeReadFdRef() == 12;
		ok = ok && *PgCurrentWaitEventSelfPipeWriteFdRef() == 13;
		ok = ok && *PgCurrentWaitEventSelfPipeOwnerPidRef() == 14;
		ok = ok && *PgCurrentStackBasePtrRef() == &stack_marker1;
		ok = ok && *PgCurrentBackendThreadStartRef() == thread_start1;
		ok = ok && IsUnderPostmaster;

		PgSetCurrentCarrier(&fake_carrier2);
		ok = ok && *PgCurrentWaitEventWaitingRef() == false;
		ok = ok && *PgCurrentWaitEventSignalFdRef() == 21;
		ok = ok && *PgCurrentWaitEventSelfPipeReadFdRef() == 22;
		ok = ok && *PgCurrentWaitEventSelfPipeWriteFdRef() == 23;
		ok = ok && *PgCurrentWaitEventSelfPipeOwnerPidRef() == 24;
		ok = ok && *PgCurrentStackBasePtrRef() == &stack_marker2;
		ok = ok && *PgCurrentBackendThreadStartRef() == thread_start2;
		ok = ok && !IsUnderPostmaster;

		PgSetCurrentCarrier(saved_carrier);
		IsUnderPostmaster = saved_is_under_postmaster;
	}
	PG_CATCH();
	{
		PgSetCurrentCarrier(saved_carrier);
		IsUnderPostmaster = saved_is_under_postmaster;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "carrier miscellaneous state was not carrier-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_carrier_threaded_guc_lock_depth_is_carrier_local);
Datum
test_carrier_threaded_guc_lock_depth_is_carrier_local(PG_FUNCTION_ARGS)
{
	PgCarrier  *saved_carrier;
	PgCarrier	fake_carrier1;
	PgCarrier	fake_carrier2;
	bool		ok = true;

	saved_carrier = CurrentPgCarrier;
	MemSet(&fake_carrier1, 0, sizeof(fake_carrier1));
	MemSet(&fake_carrier2, 0, sizeof(fake_carrier2));
	fake_carrier1.kind = PG_CARRIER_THREAD;
	fake_carrier2.kind = PG_CARRIER_THREAD;

	PG_TRY();
	{
		PgSetCurrentCarrier(&fake_carrier1);
		*PgCurrentThreadedGUCMutexDepthRef() = 1;
		PgSetCurrentCarrier(&fake_carrier2);
		ok = ok && *PgCurrentThreadedGUCMutexDepthRef() == 0;
		*PgCurrentThreadedGUCMutexDepthRef() = 2;
		PgSetCurrentCarrier(&fake_carrier1);
		ok = ok && *PgCurrentThreadedGUCMutexDepthRef() == 1;
		PgSetCurrentCarrier(&fake_carrier2);
		ok = ok && *PgCurrentThreadedGUCMutexDepthRef() == 2;

		PgSetCurrentCarrier(saved_carrier);
	}
	PG_CATCH();
	{
		PgSetCurrentCarrier(saved_carrier);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "threaded GUC mutex depth was not carrier-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_carrier_attach_detach_current_work);
Datum
test_carrier_attach_detach_current_work(PG_FUNCTION_ARGS)
{
	PgRuntime  *saved_runtime;
	PgCarrier  *saved_carrier;
	PgBackend  *saved_backend;
	PgSession  *saved_session;
	PgConnection *saved_connection;
	PgExecution *saved_execution;
	MemoryContext saved_current_memory_context;
	ResourceOwner saved_current_resource_owner;
	PgThreadBackendRuntimeState state;
	PGPROC		fake_proc;
	Latch		fake_latch;
	WaitEventSet *fake_wait_set;
	MemoryContext fake_memory_context;
	ResourceOwner fake_resource_owner;
	bool		ok = true;

	saved_runtime = CurrentPgRuntime;
	saved_carrier = CurrentPgCarrier;
	saved_backend = CurrentPgBackend;
	saved_session = CurrentPgSession;
	saved_connection = CurrentPgConnection;
	saved_execution = CurrentPgExecution;
	saved_current_memory_context = CurrentMemoryContext;
	saved_current_resource_owner = CurrentResourceOwner;

	InitializePgThreadRuntime(NULL);
	InitializePgThreadBackendRuntimeState(&state, B_BACKEND, NULL,
										  &fake_latch);
	PgCarrierDetachBackend(&state.carrier, &state.backend);
	MemSet(&fake_proc, 0, sizeof(fake_proc));
	InitLatch(&fake_latch);
	fake_wait_set = (WaitEventSet *) &state.connection;
	fake_memory_context = (MemoryContext) &state.execution;
	fake_resource_owner = (ResourceOwner) &state.execution;
	state.backend.my_proc = &fake_proc;
	state.backend.my_proc_number = 42;
	state.backend.core.latch = &fake_latch;
	state.backend.timeout.num_active_timeouts = 3;
	state.connection.protocol.fe_be_wait_set = fake_wait_set;
	state.execution.memory_contexts.current_context = fake_memory_context;
	state.execution.resource_owners.current_owner =
		(struct ResourceOwnerData *) fake_resource_owner;

	PG_TRY();
	{
		PgCarrierAttachBackend(&state.carrier, &state.backend,
							   &state.session, &state.connection,
							   &state.execution);

		ok = ok && CurrentPgRuntime == state.backend.runtime;
		ok = ok && CurrentPgRuntime->current_carrier == &state.carrier;
		ok = ok && CurrentPgCarrier == &state.carrier;
		ok = ok && CurrentPgBackend == &state.backend;
		ok = ok && CurrentPgSession == &state.session;
		ok = ok && CurrentPgConnection == &state.connection;
		ok = ok && CurrentPgExecution == &state.execution;
		ok = ok && state.carrier.current_backend == &state.backend;
		ok = ok && state.carrier.current_session == &state.session;
		ok = ok && state.carrier.current_execution == &state.execution;
		ok = ok && state.backend.carrier == &state.carrier;
		ok = ok && state.execution.carrier == &state.carrier;
		ok = ok && CurrentPgBackendTimeoutRuntimeState ==
			&state.backend.timeout;
		ok = ok && CurrentPgConnectionProtocolRuntimeState ==
			&state.connection.protocol;
		ok = ok && CurrentPgExecutionMemoryContextRuntimeState ==
			&state.execution.memory_contexts;
		ok = ok && CurrentPgExecutionResourceOwnerRuntimeState ==
			&state.execution.resource_owners;
		ok = ok && MyProc == &fake_proc;
		ok = ok && MyProcNumber == 42;
		ok = ok && MyLatch == &fake_latch;
		ok = ok && FeBeWaitSet == fake_wait_set;
		ok = ok && CurrentMemoryContext == fake_memory_context;
		ok = ok && CurrentResourceOwner == fake_resource_owner;
		ok = ok && PgCurrentTimeoutState() == &state.backend.timeout;

		PgCarrierDetachBackend(&state.carrier, &state.backend);

		ok = ok && CurrentPgRuntime == state.backend.runtime;
		ok = ok && CurrentPgCarrier == &state.carrier;
		ok = ok && CurrentPgBackend == NULL;
		ok = ok && CurrentPgSession == NULL;
		ok = ok && CurrentPgConnection == NULL;
		ok = ok && CurrentPgExecution == NULL;
		ok = ok && state.carrier.current_backend == NULL;
		ok = ok && state.carrier.current_session == NULL;
		ok = ok && state.carrier.current_execution == NULL;
		ok = ok && state.backend.carrier == NULL;
		ok = ok && state.execution.carrier == NULL;
		ok = ok && CurrentPgBackendTimeoutRuntimeState == NULL;
		ok = ok && CurrentPgConnectionProtocolRuntimeState == NULL;
		ok = ok && CurrentPgExecutionMemoryContextRuntimeState == NULL;
		ok = ok && CurrentPgExecutionResourceOwnerRuntimeState == NULL;
		ok = ok && MyProc != &fake_proc;
		ok = ok && MyLatch != &fake_latch;
		ok = ok && FeBeWaitSet != fake_wait_set;
		ok = ok && CurrentMemoryContext != fake_memory_context;
		ok = ok && CurrentResourceOwner != fake_resource_owner;

		PgRuntimeSetCurrentWork(saved_runtime, saved_carrier, saved_backend,
								saved_session, saved_connection,
								saved_execution, false);
		CurrentMemoryContext = saved_current_memory_context;
		CurrentResourceOwner = saved_current_resource_owner;
	}
	PG_CATCH();
	{
		PgRuntimeSetCurrentWork(saved_runtime, saved_carrier, saved_backend,
								saved_session, saved_connection,
								saved_execution, false);
		CurrentMemoryContext = saved_current_memory_context;
		CurrentResourceOwner = saved_current_resource_owner;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "carrier attach/detach did not refresh current work");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_carrier_protocol_park_prepare_commit);
Datum
test_carrier_protocol_park_prepare_commit(PG_FUNCTION_ARGS)
{
	PgRuntime  *saved_runtime;
	PgCarrier  *saved_carrier;
	PgBackend  *saved_backend;
	PgSession  *saved_session;
	PgConnection *saved_connection;
	PgExecution *saved_execution;
	MemoryContext saved_current_memory_context;
	ResourceOwner saved_current_resource_owner;
	PgThreadBackendRuntimeState state;
	PgProtocolParkSpec park_spec;
	PgProtocolSchedulerState *scheduler;
	PgBackend  *runnable_backend;
	TimestampTz timeout_wake_at;
	uint64		timeout_generation;
	uint64		notify_generation;
	uint32		expected_wake_reasons;
	uint32		expected_wake_events;
	PGPROC		fake_proc;
	Latch		fake_latch;
	WaitEventSet *fake_wait_set;
	MemoryContext fake_memory_context;
	ResourceOwner fake_resource_owner;
	bool		ok = true;

	saved_runtime = CurrentPgRuntime;
	saved_carrier = CurrentPgCarrier;
	saved_backend = CurrentPgBackend;
	saved_session = CurrentPgSession;
	saved_connection = CurrentPgConnection;
	saved_execution = CurrentPgExecution;
	saved_current_memory_context = CurrentMemoryContext;
	saved_current_resource_owner = CurrentResourceOwner;

	InitializePgThreadRuntime(NULL);
	InitializePgThreadBackendRuntimeState(&state, B_BACKEND, NULL,
										  &fake_latch);
	PgCarrierDetachBackend(&state.carrier, &state.backend);
	MemSet(&fake_proc, 0, sizeof(fake_proc));
	InitLatch(&fake_latch);
	fake_wait_set = (WaitEventSet *) &state.connection;
	fake_memory_context = (MemoryContext) &state.execution;
	fake_resource_owner = (ResourceOwner) &state.execution;
	state.backend.my_proc = &fake_proc;
	state.backend.my_proc_number = 42;
	state.backend.core.latch = &fake_latch;
	state.session.loop_state.doing_command_read = true;
	state.connection.protocol.fe_be_wait_set = fake_wait_set;
	state.execution.memory_contexts.current_context = fake_memory_context;
	state.execution.resource_owners.current_owner =
		(struct ResourceOwnerData *) fake_resource_owner;
	state.connection.socket_io.transport_generation = 11;
	state.backend.timeout.all_timeouts_initialized = true;
	state.backend.timeout.signal_delivery = false;
	state.backend.timeout.alarm_enabled = true;
	state.backend.timeout.num_active_timeouts = 1;
	state.backend.timeout.all_timeouts[IDLE_SESSION_TIMEOUT].active = true;
	state.backend.timeout.all_timeouts[IDLE_SESSION_TIMEOUT].fin_time = 424242;
	state.backend.timeout.active_timeouts[0] =
		&state.backend.timeout.all_timeouts[IDLE_SESSION_TIMEOUT];
	state.backend.timeout.generation = 17;

	PG_TRY();
	{
		PgCarrierAttachBackend(&state.carrier, &state.backend,
							   &state.session, &state.connection,
							   &state.execution);
		scheduler = &state.backend.runtime->protocol_scheduler;
		ok = ok && scheduler->parked_protocol_count == 0;
		ok = ok && scheduler->runnable_count == 0;

		MemSet(&park_spec, 0, sizeof(park_spec));
		park_spec.transport_wait_events = WL_SOCKET_READABLE;
		park_spec.transport_generation =
			state.connection.socket_io.transport_generation;
		park_spec.wait_event_info = WAIT_EVENT_CLIENT_READ;

		ok = ok && PgBackendLogicalTimeoutNextWake(&state.backend,
												   &timeout_wake_at,
												   &timeout_generation);
		ok = ok && timeout_wake_at == 424242;
		ok = ok && timeout_generation == 17;

		ok = ok && PgBackendPrepareProtocolReadPark(&state.backend,
													&park_spec);
		ok = ok && state.backend.protocol_park.state ==
			PG_PROTOCOL_PARK_PREPARED;
		ok = ok && state.backend.protocol_park.spec.backend ==
			&state.backend;
		ok = ok && state.backend.protocol_park.spec.session ==
			&state.session;
		ok = ok && state.backend.protocol_park.spec.connection ==
			&state.connection;
		ok = ok && state.backend.protocol_park.spec.transport_wait_events ==
			WL_SOCKET_READABLE;
		ok = ok && state.backend.protocol_park.spec.transport_generation == 11;
		ok = ok && state.backend.protocol_park.spec.timeout_generation == 17;
		ok = ok && state.backend.protocol_park.spec.timeout_wake_at_valid;
		ok = ok && state.backend.protocol_park.spec.timeout_wake_at == 424242;
		ok = ok && state.backend.protocol_park.spec.generation == 1;
		ok = ok && CurrentPgBackend == &state.backend;
		ok = ok && state.carrier.current_backend == &state.backend;
		ok = ok && state.backend.carrier == &state.carrier;
		ok = ok && MyProc == &fake_proc;
		ok = ok && MyProcNumber == 42;
		ok = ok && MyLatch == &fake_latch;
		ok = ok && FeBeWaitSet == fake_wait_set;
		ok = ok && CurrentMemoryContext == fake_memory_context;
		ok = ok && CurrentResourceOwner == fake_resource_owner;

		PgCarrierCommitProtocolReadPark(&state.carrier, &state.backend);

		ok = ok && state.backend.protocol_park.state ==
			PG_PROTOCOL_PARK_COMMITTED;
		ok = ok && state.backend.protocol_park.spec.generation == 1;
		ok = ok && state.backend.protocol_park.wake_reasons ==
			PG_PROTOCOL_PARK_WAKE_NONE;
		ok = ok && state.backend.protocol_park.wake_generation == 0;
		ok = ok && state.backend.protocol_park.scheduler_queue_state ==
			PG_PROTOCOL_SCHEDULER_QUEUE_PARKED_PROTOCOL_READ;
		ok = ok && scheduler->parked_protocol_count == 1;
		ok = ok && scheduler->runnable_count == 0;
		ok = ok && PgBackendProtocolReadParkTimeoutGenerationValid(&state.backend,
																   1);
		state.backend.timeout.generation++;
		ok = ok && !PgBackendProtocolReadParkTimeoutGenerationValid(&state.backend,
																	1);
		state.backend.timeout.generation = 17;
		ok = ok && CurrentPgBackend == NULL;
		ok = ok && CurrentPgSession == NULL;
		ok = ok && CurrentPgConnection == NULL;
		ok = ok && CurrentPgExecution == NULL;
		ok = ok && state.carrier.current_backend == NULL;
		ok = ok && state.backend.carrier == NULL;
		ok = ok && CurrentPgBackendTimeoutRuntimeState == NULL;
		ok = ok && CurrentPgConnectionProtocolRuntimeState == NULL;
		ok = ok && CurrentPgExecutionMemoryContextRuntimeState == NULL;
		ok = ok && CurrentPgExecutionResourceOwnerRuntimeState == NULL;
		ok = ok && MyProc != &fake_proc;
		ok = ok && MyLatch != &fake_latch;
		ok = ok && FeBeWaitSet != fake_wait_set;
		ok = ok && CurrentMemoryContext != fake_memory_context;
		ok = ok && CurrentResourceOwner != fake_resource_owner;

		ok = ok && !PgBackendMarkProtocolReadParkWake(&state.backend, 0,
													  PG_PROTOCOL_PARK_WAKE_LOGICAL,
													  WL_LATCH_SET);
		ok = ok && state.backend.protocol_park.wake_reasons ==
			PG_PROTOCOL_PARK_WAKE_NONE;
		ok = ok && PgBackendMarkProtocolReadParkWake(&state.backend, 1,
													 PG_PROTOCOL_PARK_WAKE_LOGICAL,
													 WL_LATCH_SET);
		ok = ok && state.backend.protocol_park.wake_reasons ==
			PG_PROTOCOL_PARK_WAKE_LOGICAL;
		ok = ok && state.backend.protocol_park.wake_events == WL_LATCH_SET;
		ok = ok && state.backend.protocol_park.wake_generation == 1;
		ok = ok && PgBackendMarkProtocolReadParkWake(&state.backend, 1,
													 PG_PROTOCOL_PARK_WAKE_TIMEOUT,
													 WL_TIMEOUT);
		ok = ok && state.backend.protocol_park.wake_reasons ==
			(PG_PROTOCOL_PARK_WAKE_LOGICAL | PG_PROTOCOL_PARK_WAKE_TIMEOUT);
		ok = ok && state.backend.protocol_park.wake_events ==
			(WL_LATCH_SET | WL_TIMEOUT);
		ok = ok && PgBackendMarkProtocolReadParkWake(&state.backend, 1,
													 PG_PROTOCOL_PARK_WAKE_POSTMASTER,
													 WL_POSTMASTER_DEATH);
		ok = ok && state.backend.protocol_park.wake_reasons ==
			(PG_PROTOCOL_PARK_WAKE_LOGICAL | PG_PROTOCOL_PARK_WAKE_TIMEOUT |
			 PG_PROTOCOL_PARK_WAKE_POSTMASTER);
		ok = ok && state.backend.protocol_park.wake_events ==
			(WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH);
		ok = ok && PgBackendNotifyInterruptGeneration(&state.backend) == 0;
		SendInterrupt(&state.backend, PG_BACKEND_INTERRUPT_NOTIFY);
		ok = ok && PgBackendNotifyInterruptGeneration(&state.backend) == 1;
		SendInterrupt(&state.backend, PG_BACKEND_INTERRUPT_NOTIFY);
		notify_generation =
			PgBackendNotifyInterruptGeneration(&state.backend);
		ok = ok && notify_generation == 2;
		ok = ok && PgBackendMarkProtocolReadParkWake(&state.backend, 1,
													 PG_PROTOCOL_PARK_WAKE_NOTIFY,
													 WL_LATCH_SET);
		expected_wake_reasons =
			PG_PROTOCOL_PARK_WAKE_LOGICAL |
			PG_PROTOCOL_PARK_WAKE_TIMEOUT |
			PG_PROTOCOL_PARK_WAKE_POSTMASTER |
			PG_PROTOCOL_PARK_WAKE_NOTIFY;
		expected_wake_events =
			WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH;
		ok = ok && state.backend.protocol_park.wake_reasons ==
			expected_wake_reasons;
		ok = ok && state.backend.protocol_park.notify_wake_generation ==
			notify_generation;

		ok = ok && PgRuntimeProtocolSchedulerMarkRunnable(state.backend.runtime,
														  &state.backend);
		ok = ok && state.backend.protocol_park.scheduler_queue_state ==
			PG_PROTOCOL_SCHEDULER_QUEUE_RUNNABLE;
		ok = ok && scheduler->parked_protocol_count == 0;
		ok = ok && scheduler->runnable_count == 1;
		runnable_backend =
			PgRuntimeProtocolSchedulerPopRunnable(state.backend.runtime);
		ok = ok && runnable_backend == &state.backend;
		ok = ok && state.backend.protocol_park.scheduler_queue_state ==
			PG_PROTOCOL_SCHEDULER_QUEUE_NONE;
		ok = ok && scheduler->parked_protocol_count == 0;
		ok = ok && scheduler->runnable_count == 0;

		PgCarrierAttachBackend(&state.carrier, &state.backend,
							   &state.session, &state.connection,
							   &state.execution);
		PgBackendResumeProtocolReadPark(&state.backend);

		ok = ok && state.backend.protocol_park.state ==
			PG_PROTOCOL_PARK_NONE;
		ok = ok && state.backend.protocol_park.spec.backend == NULL;
		ok = ok && state.backend.protocol_park.next_generation == 1;
		ok = ok && state.backend.protocol_park.wake_reasons ==
			PG_PROTOCOL_PARK_WAKE_NONE;
		ok = ok && state.backend.protocol_park.wake_events == 0;
		ok = ok && state.backend.protocol_park.wake_generation == 0;
		ok = ok && state.backend.protocol_park.last_wake_reasons ==
			expected_wake_reasons;
		ok = ok && state.backend.protocol_park.last_wake_events ==
			expected_wake_events;
		ok = ok && state.backend.protocol_park.last_wake_generation == 1;
		ok = ok && PgBackendMarkProtocolReadParkDeferredNotify(&state.backend,
															   notify_generation,
															   PG_PROTOCOL_PARK_WAKE_NOTIFY);
		ok = ok && state.backend.protocol_park.deferred_notify_generation ==
			notify_generation;
		ok = ok && state.backend.protocol_park.deferred_notify_park_generation == 1;
		ok = ok && state.backend.protocol_park.deferred_notify_reasons ==
			PG_PROTOCOL_PARK_WAKE_NOTIFY;
		PgBackendClearProtocolReadParkDeferredNotify(&state.backend);
		ok = ok && state.backend.protocol_park.deferred_notify_generation == 0;
		ok = ok && state.backend.protocol_park.deferred_notify_park_generation == 0;
		ok = ok && state.backend.protocol_park.deferred_notify_reasons ==
			PG_PROTOCOL_PARK_WAKE_NONE;
		ok = ok && CurrentPgBackend == &state.backend;
		ok = ok && state.carrier.current_backend == &state.backend;
		ok = ok && state.backend.carrier == &state.carrier;
		ok = ok && MyProc == &fake_proc;
		ok = ok && MyProcNumber == 42;
		ok = ok && MyLatch == &fake_latch;
		ok = ok && FeBeWaitSet == fake_wait_set;
		ok = ok && CurrentMemoryContext == fake_memory_context;
		ok = ok && CurrentResourceOwner == fake_resource_owner;

		PgRuntimeSetCurrentWork(saved_runtime, saved_carrier, saved_backend,
								saved_session, saved_connection,
								saved_execution, false);
		CurrentMemoryContext = saved_current_memory_context;
		CurrentResourceOwner = saved_current_resource_owner;
	}
	PG_CATCH();
	{
		PgRuntimeSetCurrentWork(saved_runtime, saved_carrier, saved_backend,
								saved_session, saved_connection,
								saved_execution, false);
		CurrentMemoryContext = saved_current_memory_context;
		CurrentResourceOwner = saved_current_resource_owner;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "protocol park prepare/commit split failed");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_protocol_read_wake_applies_backend_interrupt);
Datum
test_protocol_read_wake_applies_backend_interrupt(PG_FUNCTION_ARGS)
{
	PgBackend  *backend;
	PgSession  *session;
	PgBackendInterruptMask saved_interrupt_mask;
	uint32		saved_notify_generation;
	uint64		saved_deferred_notify_generation;
	uint64		saved_deferred_notify_park_generation;
	uint32		saved_deferred_notify_reasons;
	bool		saved_doing_command_read;
	bool		saved_interrupt_pending;
	bool		saved_query_cancel_pending;
	bool		saved_notify_interrupt_pending;
	bool		saved_config_reload_pending;
	bool		ok = true;

	backend = CurrentPgBackend;
	session = CurrentPgSession;
	if (backend == NULL || session == NULL)
		elog(ERROR, "test requires a current backend and session");

	saved_interrupt_mask =
		pg_atomic_read_u32(&backend->interrupts.pending_mask);
	saved_notify_generation =
		pg_atomic_read_u32(&backend->interrupts.notify_generation);
	saved_deferred_notify_generation =
		backend->protocol_park.deferred_notify_generation;
	saved_deferred_notify_park_generation =
		backend->protocol_park.deferred_notify_park_generation;
	saved_deferred_notify_reasons =
		backend->protocol_park.deferred_notify_reasons;
	saved_doing_command_read = session->loop_state.doing_command_read;
	saved_interrupt_pending = InterruptPending;
	saved_query_cancel_pending = QueryCancelPending;
	saved_notify_interrupt_pending = notifyInterruptPending;
	saved_config_reload_pending = ConfigReloadPending;

	PG_TRY();
	{
		pg_atomic_write_u32(&backend->interrupts.pending_mask, 0);
		pg_atomic_write_u32(&backend->interrupts.notify_generation, 0);
		PgBackendClearProtocolReadParkDeferredNotify(backend);
		session->loop_state.doing_command_read = true;
		InterruptPending = false;
		QueryCancelPending = false;
		notifyInterruptPending = false;
		ConfigReloadPending = false;

		SendInterrupt(backend, PG_BACKEND_INTERRUPT_QUERY_CANCEL);
		ok = ok && pg_atomic_read_u32(&backend->interrupts.pending_mask) != 0;

		PgSessionServiceProtocolReadWake(session);

		ok = ok && pg_atomic_read_u32(&backend->interrupts.pending_mask) == 0;
		ok = ok && !InterruptPending;
		ok = ok && !QueryCancelPending;

		SendInterrupt(backend, PG_BACKEND_INTERRUPT_NOTIFY);
		ok = ok && PgBackendNotifyInterruptGeneration(backend) == 1;

		PgSessionServiceProtocolReadWake(session);

		ok = ok && pg_atomic_read_u32(&backend->interrupts.pending_mask) == 0;
		ok = ok && notifyInterruptPending;
		ok = ok && backend->protocol_park.deferred_notify_generation == 1;
		ok = ok && backend->protocol_park.deferred_notify_reasons ==
			PG_PROTOCOL_PARK_WAKE_NOTIFY;

		ConfigReloadPending = true;
		PgSessionServiceProtocolReadWake(session);
		ok = ok && !ConfigReloadPending;

		if (MyLatch != NULL)
			ResetLatch(MyLatch);
		pg_atomic_write_u32(&backend->interrupts.pending_mask,
							saved_interrupt_mask);
		pg_atomic_write_u32(&backend->interrupts.notify_generation,
							saved_notify_generation);
		backend->protocol_park.deferred_notify_generation =
			saved_deferred_notify_generation;
		backend->protocol_park.deferred_notify_park_generation =
			saved_deferred_notify_park_generation;
		backend->protocol_park.deferred_notify_reasons =
			saved_deferred_notify_reasons;
		session->loop_state.doing_command_read = saved_doing_command_read;
		InterruptPending = saved_interrupt_pending;
		QueryCancelPending = saved_query_cancel_pending;
		notifyInterruptPending = saved_notify_interrupt_pending;
		ConfigReloadPending = saved_config_reload_pending;
	}
	PG_CATCH();
	{
		if (MyLatch != NULL)
			ResetLatch(MyLatch);
		pg_atomic_write_u32(&backend->interrupts.pending_mask,
							saved_interrupt_mask);
		pg_atomic_write_u32(&backend->interrupts.notify_generation,
							saved_notify_generation);
		backend->protocol_park.deferred_notify_generation =
			saved_deferred_notify_generation;
		backend->protocol_park.deferred_notify_park_generation =
			saved_deferred_notify_park_generation;
		backend->protocol_park.deferred_notify_reasons =
			saved_deferred_notify_reasons;
		session->loop_state.doing_command_read = saved_doing_command_read;
		InterruptPending = saved_interrupt_pending;
		QueryCancelPending = saved_query_cancel_pending;
		notifyInterruptPending = saved_notify_interrupt_pending;
		ConfigReloadPending = saved_config_reload_pending;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "protocol read wake did not service backend interrupt");

	PG_RETURN_BOOL(true);
}
