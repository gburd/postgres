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
	MemoryContext saved_error_context;
	MemoryContext scheduler_memory_context;
	MemoryContext scheduler_error_context;
	ResourceOwner fake_resource_owner;
	ResourceOwner scheduler_resource_owner;
	bool		ok = true;

	saved_runtime = CurrentPgRuntime;
	saved_carrier = CurrentPgCarrier;
	saved_backend = CurrentPgBackend;
	saved_session = CurrentPgSession;
	saved_connection = CurrentPgConnection;
	saved_execution = CurrentPgExecution;
	saved_current_memory_context = CurrentMemoryContext;
	saved_error_context = ErrorContext;
	saved_current_resource_owner = CurrentResourceOwner;
	scheduler_memory_context = TopMemoryContext;
	scheduler_error_context = ErrorContext;
	scheduler_resource_owner = CurrentResourceOwner;

	InitializePgThreadRuntime(NULL);
	InitializePgThreadBackendRuntimeState(&state, B_BACKEND, NULL,
										  &fake_latch);
	PgCarrierDetachBackend(&state.carrier, &state.logical.backend);
	MemSet(&fake_proc, 0, sizeof(fake_proc));
	InitLatch(&fake_latch);
	fake_wait_set = (WaitEventSet *) &state.logical.connection;
	fake_memory_context = (MemoryContext) &state.logical.execution;
	fake_resource_owner = (ResourceOwner) &state.logical.execution;
	state.logical.backend.my_proc = &fake_proc;
	state.logical.backend.my_proc_number = 42;
	state.logical.backend.core.latch = &fake_latch;
	state.logical.backend.timeout.num_active_timeouts = 3;
	state.logical.connection.protocol.fe_be_wait_set = fake_wait_set;
	state.logical.execution.memory_contexts.current_context = fake_memory_context;
	state.logical.execution.resource_owners.current_owner =
		(struct ResourceOwnerData *) fake_resource_owner;
	ok = ok && state.carrier.scheduler_execution != NULL;
	if (state.carrier.scheduler_execution != NULL)
	{
		state.carrier.scheduler_execution->memory_contexts.top_context =
			TopMemoryContext;
		state.carrier.scheduler_execution->memory_contexts.current_context =
			scheduler_memory_context;
		state.carrier.scheduler_execution->memory_contexts.error_context =
			scheduler_error_context;
		state.carrier.scheduler_execution->resource_owners.current_owner =
			(struct ResourceOwnerData *) scheduler_resource_owner;
	}

	PG_TRY();
	{
		PgCarrierAttachBackend(&state.carrier, &state.logical.backend,
							   &state.logical.session, &state.logical.connection,
							   &state.logical.execution);

		ok = ok && CurrentPgRuntime == state.logical.backend.runtime;
		ok = ok && CurrentPgRuntime->current_carrier == &state.carrier;
		ok = ok && CurrentPgCarrier == &state.carrier;
		ok = ok && CurrentPgBackend == &state.logical.backend;
		ok = ok && CurrentPgSession == &state.logical.session;
		ok = ok && CurrentPgConnection == &state.logical.connection;
		ok = ok && CurrentPgExecution == &state.logical.execution;
		ok = ok && state.carrier.current_backend == &state.logical.backend;
		ok = ok && state.carrier.current_session == &state.logical.session;
		ok = ok && state.carrier.current_execution == &state.logical.execution;
		ok = ok && state.logical.backend.carrier == &state.carrier;
		ok = ok && state.logical.execution.carrier == &state.carrier;
		ok = ok && CurrentPgBackendTimeoutRuntimeState ==
			&state.logical.backend.timeout;
		ok = ok && CurrentPgConnectionProtocolRuntimeState ==
			&state.logical.connection.protocol;
		ok = ok && CurrentPgExecutionMemoryContextRuntimeState ==
			&state.logical.execution.memory_contexts;
		ok = ok && CurrentPgExecutionResourceOwnerRuntimeState ==
			&state.logical.execution.resource_owners;
		ok = ok && MyProc == &fake_proc;
		ok = ok && MyProcNumber == 42;
		ok = ok && MyLatch == &fake_latch;
		ok = ok && FeBeWaitSet == fake_wait_set;
		ok = ok && CurrentMemoryContext == fake_memory_context;
		ok = ok && CurrentResourceOwner == fake_resource_owner;
		ok = ok && PgCurrentTimeoutState() == &state.logical.backend.timeout;

		PgCarrierDetachBackend(&state.carrier, &state.logical.backend);

		ok = ok && CurrentPgRuntime == state.logical.backend.runtime;
		ok = ok && CurrentPgCarrier == &state.carrier;
		ok = ok && CurrentPgBackend == NULL;
		ok = ok && CurrentPgSession == NULL;
		ok = ok && CurrentPgConnection == NULL;
		ok = ok && CurrentPgExecution == NULL;
		ok = ok && state.carrier.current_backend == NULL;
		ok = ok && state.carrier.current_session == NULL;
		ok = ok && state.carrier.current_execution == NULL;
		ok = ok && state.logical.backend.carrier == NULL;
		ok = ok && state.logical.execution.carrier == NULL;
		ok = ok && CurrentPgBackendTimeoutRuntimeState == NULL;
		ok = ok && CurrentPgConnectionProtocolRuntimeState == NULL;
		ok = ok && CurrentPgExecutionMemoryContextRuntimeState == NULL;
		ok = ok && CurrentPgExecutionResourceOwnerRuntimeState == NULL;
		ok = ok && MyProc != &fake_proc;
		ok = ok && MyLatch != &fake_latch;
		ok = ok && FeBeWaitSet != fake_wait_set;
		ok = ok && CurrentMemoryContext == scheduler_memory_context;
		ok = ok && ErrorContext == scheduler_error_context;
		ok = ok && CurrentResourceOwner == scheduler_resource_owner;

		PgRuntimeSetCurrentWork(saved_runtime, saved_carrier, saved_backend,
								saved_session, saved_connection,
								saved_execution, false);
		CurrentMemoryContext = saved_current_memory_context;
		ErrorContext = saved_error_context;
		CurrentResourceOwner = saved_current_resource_owner;
	}
	PG_CATCH();
	{
		PgRuntimeSetCurrentWork(saved_runtime, saved_carrier, saved_backend,
								saved_session, saved_connection,
								saved_execution, false);
		CurrentMemoryContext = saved_current_memory_context;
		ErrorContext = saved_error_context;
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
	PgCarrier	resume_carrier;
	PgCarrier	overflow_carrier;
	PgProtocolParkSpec park_spec;
	PgProtocolSchedulerState *scheduler = NULL;
	PgBackend  *runnable_backend;
	PgBackend  *parked_backends[4];
	TimestampTz timeout_wake_at;
	uint64		timeout_generation;
	uint64		notify_generation;
	uint32		saved_carrier_limit = 0;
	uint32		base_registered_carriers = 0;
	uint32		base_idle_carriers = 0;
	uint32		base_active_carriers = 0;
	uint64		base_carrier_rejects = 0;
	uint64		base_carrier_leases = 0;
	uint64		base_carrier_releases = 0;
	uint32		expected_wake_reasons;
	uint32		expected_wake_events;
	PgBackendInterruptMask pending_interrupts;
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
	InitializePgThreadCarrierRuntimeState(&resume_carrier);
	InitializePgThreadCarrierRuntimeState(&overflow_carrier);
	PgCarrierDetachBackend(&state.carrier, &state.logical.backend);
	MemSet(&fake_proc, 0, sizeof(fake_proc));
	InitLatch(&fake_latch);
	fake_wait_set = (WaitEventSet *) &state.logical.connection;
	fake_memory_context = (MemoryContext) &state.logical.execution;
	fake_resource_owner = (ResourceOwner) &state.logical.execution;
	state.logical.backend.my_proc = &fake_proc;
	state.logical.backend.my_proc_number = 42;
	state.logical.backend.core.latch = &fake_latch;
	state.logical.session.loop_state.doing_command_read = true;
	state.logical.connection.protocol.fe_be_wait_set = fake_wait_set;
	state.logical.execution.memory_contexts.current_context = fake_memory_context;
	state.logical.execution.resource_owners.current_owner =
		(struct ResourceOwnerData *) fake_resource_owner;
	state.logical.connection.socket_io.transport_generation = 11;
	state.logical.backend.timeout.all_timeouts_initialized = true;
	state.logical.backend.timeout.signal_delivery = false;
	state.logical.backend.timeout.alarm_enabled = true;
	state.logical.backend.timeout.num_active_timeouts = 1;
	state.logical.backend.timeout.all_timeouts[IDLE_SESSION_TIMEOUT].active = true;
	state.logical.backend.timeout.all_timeouts[IDLE_SESSION_TIMEOUT].fin_time = 424242;
	state.logical.backend.timeout.active_timeouts[0] =
		&state.logical.backend.timeout.all_timeouts[IDLE_SESSION_TIMEOUT];
	state.logical.backend.timeout.generation = 17;

	PG_TRY();
	{
		PgCarrierAttachBackend(&state.carrier, &state.logical.backend,
							   &state.logical.session, &state.logical.connection,
							   &state.logical.execution);
		scheduler = &state.logical.backend.runtime->protocol_scheduler;
		ok = ok && scheduler->parked_protocol_count == 0;
		ok = ok && scheduler->runnable_count == 0;
		saved_carrier_limit = scheduler->carrier_limit;
		base_registered_carriers = scheduler->registered_carrier_count;
		base_idle_carriers = scheduler->idle_carrier_count;
		base_active_carriers = scheduler->active_carrier_count;
		base_carrier_rejects = scheduler->carrier_reject_count;
		base_carrier_leases = scheduler->carrier_lease_count;
		base_carrier_releases = scheduler->carrier_release_count;
		scheduler->carrier_limit = base_registered_carriers + 1;

		ok = ok && PgRuntimeProtocolSchedulerRegisterCarrier(state.logical.backend.runtime,
															 &resume_carrier);
		ok = ok && resume_carrier.protocol_scheduler_registered;
		ok = ok && resume_carrier.protocol_scheduler_idle;
		ok = ok && scheduler->registered_carrier_count ==
			base_registered_carriers + 1;
		ok = ok && scheduler->idle_carrier_count == base_idle_carriers + 1;
		ok = ok && scheduler->active_carrier_count == base_active_carriers;
		ok = ok && !PgRuntimeProtocolSchedulerRegisterCarrier(state.logical.backend.runtime,
															  &overflow_carrier);
		ok = ok && scheduler->carrier_reject_count == base_carrier_rejects + 1;
		scheduler->carrier_limit = saved_carrier_limit;

		MemSet(&park_spec, 0, sizeof(park_spec));
		park_spec.transport_wait_events = WL_SOCKET_READABLE;
		park_spec.transport_generation =
			state.logical.connection.socket_io.transport_generation;
		park_spec.wait_event_info = WAIT_EVENT_CLIENT_READ;

		ok = ok && PgBackendLogicalTimeoutNextWake(&state.logical.backend,
												   &timeout_wake_at,
												   &timeout_generation);
		ok = ok && timeout_wake_at == 424242;
		ok = ok && timeout_generation == 17;

		ok = ok && PgBackendPrepareProtocolReadPark(&state.logical.backend,
													&park_spec);
		ok = ok && state.logical.backend.protocol_park.state ==
			PG_PROTOCOL_PARK_PREPARED;
		ok = ok && state.logical.backend.protocol_park.spec.backend ==
			&state.logical.backend;
		ok = ok && state.logical.backend.protocol_park.spec.session ==
			&state.logical.session;
		ok = ok && state.logical.backend.protocol_park.spec.connection ==
			&state.logical.connection;
		ok = ok && state.logical.backend.protocol_park.spec.transport_wait_events ==
			WL_SOCKET_READABLE;
		ok = ok && state.logical.backend.protocol_park.spec.transport_generation == 11;
		ok = ok && state.logical.backend.protocol_park.spec.timeout_generation == 17;
		ok = ok && state.logical.backend.protocol_park.spec.timeout_wake_at_valid;
		ok = ok && state.logical.backend.protocol_park.spec.timeout_wake_at == 424242;
		ok = ok && state.logical.backend.protocol_park.spec.generation == 1;
		ok = ok && CurrentPgBackend == &state.logical.backend;
		ok = ok && state.carrier.current_backend == &state.logical.backend;
		ok = ok && state.logical.backend.carrier == &state.carrier;
		ok = ok && MyProc == &fake_proc;
		ok = ok && MyProcNumber == 42;
		ok = ok && MyLatch == &fake_latch;
		ok = ok && FeBeWaitSet == fake_wait_set;
		ok = ok && CurrentMemoryContext == fake_memory_context;
		ok = ok && CurrentResourceOwner == fake_resource_owner;

		PgCarrierCommitProtocolReadPark(&state.carrier, &state.logical.backend);

		ok = ok && state.logical.backend.protocol_park.state ==
			PG_PROTOCOL_PARK_COMMITTED;
		ok = ok && state.logical.backend.protocol_park.spec.generation == 1;
		ok = ok && state.logical.backend.protocol_park.wake_reasons ==
			PG_PROTOCOL_PARK_WAKE_NONE;
		ok = ok && state.logical.backend.protocol_park.wake_generation == 0;
		ok = ok && state.logical.backend.protocol_park.scheduler_queue_state ==
			PG_PROTOCOL_SCHEDULER_QUEUE_PARKED_PROTOCOL_READ;
		ok = ok && scheduler->parked_protocol_count == 1;
		ok = ok && scheduler->runnable_count == 0;
		ok = ok && PgRuntimeProtocolSchedulerCollectParked(state.logical.backend.runtime,
														   parked_backends,
														   lengthof(parked_backends)) == 1;
		ok = ok && parked_backends[0] == &state.logical.backend;
		ok = ok && PgBackendProtocolReadParkTimeoutGenerationValid(&state.logical.backend,
																   1);
		state.logical.backend.timeout.generation++;
		ok = ok && !PgBackendProtocolReadParkTimeoutGenerationValid(&state.logical.backend,
																	1);
		state.logical.backend.timeout.generation = 17;
		ok = ok && CurrentPgBackend == NULL;
		ok = ok && CurrentPgSession == NULL;
		ok = ok && CurrentPgConnection == NULL;
		ok = ok && CurrentPgExecution == NULL;
		ok = ok && state.carrier.current_backend == NULL;
		ok = ok && state.logical.backend.carrier == NULL;
		ok = ok && CurrentPgBackendTimeoutRuntimeState == NULL;
		ok = ok && CurrentPgConnectionProtocolRuntimeState == NULL;
		ok = ok && CurrentPgExecutionMemoryContextRuntimeState == NULL;
		ok = ok && CurrentPgExecutionResourceOwnerRuntimeState == NULL;
		ok = ok && MyProc != &fake_proc;
		ok = ok && MyLatch != &fake_latch;
		ok = ok && FeBeWaitSet != fake_wait_set;
		ok = ok && CurrentMemoryContext != fake_memory_context;
		ok = ok && CurrentResourceOwner != fake_resource_owner;

		PgRuntimeSetCurrentWork(state.logical.backend.runtime, &resume_carrier,
								NULL, NULL, NULL, NULL, false);
		ok = ok && PgCarrierLeaseRunnableProtocolBackend(&resume_carrier) == NULL;
		ok = ok && CurrentPgCarrier == &resume_carrier;
		ok = ok && CurrentPgBackend == NULL;
		ok = ok && resume_carrier.current_backend == NULL;
		ok = ok && resume_carrier.protocol_scheduler_idle;
		ok = ok && scheduler->idle_carrier_count == base_idle_carriers + 1;
		ok = ok && scheduler->active_carrier_count == base_active_carriers;
		ok = ok && scheduler->carrier_lease_count == base_carrier_leases;

		ResetLatch(&fake_latch);
		PgBackendRaiseInterrupt(&state.logical.backend,
								PG_BACKEND_INTERRUPT_QUERY_CANCEL);
		ok = ok && fake_latch.is_set;
		ok = ok && CurrentPgBackend == NULL;
		pending_interrupts = PgBackendConsumeInterrupts(&state.logical.backend);
		ok = ok && (pending_interrupts &
					PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_QUERY_CANCEL));
		ResetLatch(&fake_latch);

		ok = ok && !PgBackendMarkProtocolReadParkWake(&state.logical.backend, 0,
													  PG_PROTOCOL_PARK_WAKE_LOGICAL,
													  WL_LATCH_SET);
		ok = ok && state.logical.backend.protocol_park.wake_reasons ==
			PG_PROTOCOL_PARK_WAKE_NONE;
		ok = ok && PgBackendMarkProtocolReadParkWake(&state.logical.backend, 1,
													 PG_PROTOCOL_PARK_WAKE_LOGICAL,
													 WL_LATCH_SET);
		ok = ok && state.logical.backend.protocol_park.wake_reasons ==
			PG_PROTOCOL_PARK_WAKE_LOGICAL;
		ok = ok && state.logical.backend.protocol_park.wake_events == WL_LATCH_SET;
		ok = ok && state.logical.backend.protocol_park.wake_generation == 1;
		ok = ok && PgBackendMarkProtocolReadParkWake(&state.logical.backend, 1,
													 PG_PROTOCOL_PARK_WAKE_TIMEOUT,
													 WL_TIMEOUT);
		ok = ok && state.logical.backend.protocol_park.wake_reasons ==
			(PG_PROTOCOL_PARK_WAKE_LOGICAL | PG_PROTOCOL_PARK_WAKE_TIMEOUT);
		ok = ok && state.logical.backend.protocol_park.wake_events ==
			(WL_LATCH_SET | WL_TIMEOUT);
		ok = ok && PgBackendMarkProtocolReadParkWake(&state.logical.backend, 1,
													 PG_PROTOCOL_PARK_WAKE_POSTMASTER,
													 WL_POSTMASTER_DEATH);
		ok = ok && state.logical.backend.protocol_park.wake_reasons ==
			(PG_PROTOCOL_PARK_WAKE_LOGICAL | PG_PROTOCOL_PARK_WAKE_TIMEOUT |
			 PG_PROTOCOL_PARK_WAKE_POSTMASTER);
		ok = ok && state.logical.backend.protocol_park.wake_events ==
			(WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH);
		ok = ok && PgBackendNotifyInterruptGeneration(&state.logical.backend) == 0;
		SendInterrupt(&state.logical.backend, PG_BACKEND_INTERRUPT_NOTIFY);
		ok = ok && PgBackendNotifyInterruptGeneration(&state.logical.backend) == 1;
		SendInterrupt(&state.logical.backend, PG_BACKEND_INTERRUPT_NOTIFY);
		notify_generation =
			PgBackendNotifyInterruptGeneration(&state.logical.backend);
		ok = ok && notify_generation == 2;
		ok = ok && PgBackendMarkProtocolReadParkWake(&state.logical.backend, 1,
													 PG_PROTOCOL_PARK_WAKE_NOTIFY,
													 WL_LATCH_SET);
		expected_wake_reasons =
			PG_PROTOCOL_PARK_WAKE_LOGICAL |
			PG_PROTOCOL_PARK_WAKE_TIMEOUT |
			PG_PROTOCOL_PARK_WAKE_POSTMASTER |
			PG_PROTOCOL_PARK_WAKE_NOTIFY;
		expected_wake_events =
			WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH;
		ok = ok && state.logical.backend.protocol_park.wake_reasons ==
			expected_wake_reasons;
		ok = ok && state.logical.backend.protocol_park.notify_wake_generation ==
			notify_generation;

		ok = ok && PgRuntimeProtocolSchedulerMarkRunnable(state.logical.backend.runtime,
														  &state.logical.backend);
		ok = ok && state.logical.backend.protocol_park.scheduler_queue_state ==
			PG_PROTOCOL_SCHEDULER_QUEUE_RUNNABLE;
		ok = ok && scheduler->parked_protocol_count == 0;
		ok = ok && scheduler->runnable_count == 1;
		ok = ok && PgRuntimeProtocolSchedulerCollectParked(state.logical.backend.runtime,
														   parked_backends,
														   lengthof(parked_backends)) == 0;
		runnable_backend = PgCarrierLeaseRunnableProtocolBackend(&resume_carrier);
		ok = ok && runnable_backend == &state.logical.backend;
		ok = ok && state.logical.backend.protocol_park.scheduler_queue_state ==
			PG_PROTOCOL_SCHEDULER_QUEUE_LEASED;
		ok = ok && scheduler->parked_protocol_count == 0;
		ok = ok && scheduler->runnable_count == 0;
		ok = ok && CurrentPgCarrier == &resume_carrier;
		ok = ok && CurrentPgBackend == &state.logical.backend;
		ok = ok && resume_carrier.current_backend == &state.logical.backend;
		ok = ok && resume_carrier.current_session == &state.logical.session;
		ok = ok && resume_carrier.current_execution == &state.logical.execution;
		ok = ok && state.carrier.current_backend == NULL;
		ok = ok && state.logical.backend.carrier == &resume_carrier;
		ok = ok && state.logical.execution.carrier == &resume_carrier;
		ok = ok && !resume_carrier.protocol_scheduler_idle;
		ok = ok && scheduler->idle_carrier_count == base_idle_carriers;
		ok = ok && scheduler->active_carrier_count == base_active_carriers + 1;
		ok = ok && scheduler->carrier_lease_count == base_carrier_leases + 1;
		PgBackendResumeProtocolReadPark(&state.logical.backend);
		ok = ok && scheduler->same_carrier_resume_count == 0;
		ok = ok && scheduler->migrated_resume_count == 1;

		ok = ok && state.logical.backend.protocol_park.state ==
			PG_PROTOCOL_PARK_NONE;
		ok = ok && state.logical.backend.protocol_park.spec.backend == NULL;
		ok = ok && state.logical.backend.protocol_park.next_generation == 1;
		ok = ok && state.logical.backend.protocol_park.wake_reasons ==
			PG_PROTOCOL_PARK_WAKE_NONE;
		ok = ok && state.logical.backend.protocol_park.wake_events == 0;
		ok = ok && state.logical.backend.protocol_park.wake_generation == 0;
		ok = ok && state.logical.backend.protocol_park.last_wake_reasons ==
			expected_wake_reasons;
		ok = ok && state.logical.backend.protocol_park.last_wake_events ==
			expected_wake_events;
		ok = ok && state.logical.backend.protocol_park.last_wake_generation == 1;
		ok = ok && PgBackendMarkProtocolReadParkDeferredNotify(&state.logical.backend,
															   notify_generation,
															   PG_PROTOCOL_PARK_WAKE_NOTIFY);
		ok = ok && state.logical.backend.protocol_park.deferred_notify_generation ==
			notify_generation;
		ok = ok && state.logical.backend.protocol_park.deferred_notify_park_generation == 1;
		ok = ok && state.logical.backend.protocol_park.deferred_notify_reasons ==
			PG_PROTOCOL_PARK_WAKE_NOTIFY;
		PgBackendClearProtocolReadParkDeferredNotify(&state.logical.backend);
		ok = ok && state.logical.backend.protocol_park.deferred_notify_generation == 0;
		ok = ok && state.logical.backend.protocol_park.deferred_notify_park_generation == 0;
		ok = ok && state.logical.backend.protocol_park.deferred_notify_reasons ==
			PG_PROTOCOL_PARK_WAKE_NONE;
		ok = ok && CurrentPgBackend == &state.logical.backend;
		ok = ok && resume_carrier.current_backend == &state.logical.backend;
		ok = ok && state.logical.backend.carrier == &resume_carrier;
		ok = ok && MyProc == &fake_proc;
		ok = ok && MyProcNumber == 42;
		ok = ok && MyLatch == &fake_latch;
		ok = ok && FeBeWaitSet == fake_wait_set;
		ok = ok && CurrentMemoryContext == fake_memory_context;
		ok = ok && CurrentResourceOwner == fake_resource_owner;

		PgCarrierDetachBackend(&resume_carrier, &state.logical.backend);
		ok = ok && resume_carrier.protocol_scheduler_idle;
		ok = ok && scheduler->idle_carrier_count == base_idle_carriers + 1;
		ok = ok && scheduler->active_carrier_count == base_active_carriers;
		ok = ok && scheduler->carrier_release_count == base_carrier_releases + 1;
		ok = ok && PgRuntimeProtocolSchedulerUnregisterCarrier(state.logical.backend.runtime,
															   &resume_carrier);
		ok = ok && !resume_carrier.protocol_scheduler_registered;
		ok = ok && scheduler->registered_carrier_count ==
			base_registered_carriers;
		ok = ok && scheduler->idle_carrier_count == base_idle_carriers;
		ok = ok && scheduler->active_carrier_count == base_active_carriers;

		PgRuntimeSetCurrentWork(saved_runtime, saved_carrier, saved_backend,
								saved_session, saved_connection,
								saved_execution, false);
		CurrentMemoryContext = saved_current_memory_context;
		CurrentResourceOwner = saved_current_resource_owner;
	}
	PG_CATCH();
	{
		if (scheduler != NULL)
			scheduler->carrier_limit = saved_carrier_limit;
		if (resume_carrier.current_backend != NULL)
			PgCarrierDetachBackend(&resume_carrier,
								   &state.logical.backend);
		if (resume_carrier.protocol_scheduler_registered)
			(void) PgRuntimeProtocolSchedulerUnregisterCarrier(state.logical.backend.runtime,
															   &resume_carrier);
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

PG_FUNCTION_INFO_V1(test_protocol_scheduler_poll_buffered_read);
Datum
test_protocol_scheduler_poll_buffered_read(PG_FUNCTION_ARGS)
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
	PgRuntime  *runtime;
	PgProtocolSchedulerState *scheduler;
	PgProtocolParkSpec park_spec;
	PgBackend  *scratch[4];
	uint32		base_parked_protocol_count;
	uint32		base_runnable_count;
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
	InitializePgThreadBackendRuntimeState(&state, B_BACKEND, NULL, NULL);
	PgCarrierDetachBackend(&state.carrier, &state.logical.backend);
	state.logical.session.loop_state.doing_command_read = true;
	state.logical.connection.socket_io.transport_generation = 17;
	runtime = state.logical.backend.runtime;
	scheduler = &runtime->protocol_scheduler;
	base_parked_protocol_count = scheduler->parked_protocol_count;
	base_runnable_count = scheduler->runnable_count;

	PG_TRY();
	{
		PgCarrierAttachBackend(&state.carrier, &state.logical.backend,
							   &state.logical.session,
							   &state.logical.connection,
							   &state.logical.execution);

		MemSet(&park_spec, 0, sizeof(park_spec));
		park_spec.transport_buffered_input = true;
		park_spec.transport_generation =
			state.logical.connection.socket_io.transport_generation;
		park_spec.wait_event_info = WAIT_EVENT_CLIENT_READ;

		ok = ok && PgBackendPrepareProtocolReadPark(&state.logical.backend,
													&park_spec);
		PgCarrierCommitProtocolReadPark(&state.carrier,
										&state.logical.backend);
		ok = ok && CurrentPgBackend == NULL;
		ok = ok && state.logical.backend.carrier == NULL;
		ok = ok && scheduler->parked_protocol_count ==
			base_parked_protocol_count + 1;
		ok = ok && scheduler->runnable_count == base_runnable_count;
		ok = ok && PgRuntimeProtocolSchedulerPollParkedReads(runtime,
															 scratch,
															 lengthof(scratch)) == 1;
		ok = ok && state.logical.backend.protocol_park.wake_reasons ==
			PG_PROTOCOL_PARK_WAKE_BUFFERED_INPUT;
		ok = ok && state.logical.backend.protocol_park.wake_events == 0;
		ok = ok && state.logical.backend.protocol_park.scheduler_queue_state ==
			PG_PROTOCOL_SCHEDULER_QUEUE_RUNNABLE;
		ok = ok && scheduler->parked_protocol_count ==
			base_parked_protocol_count;
		ok = ok && scheduler->runnable_count == base_runnable_count + 1;

		ok = ok && PgRuntimeProtocolSchedulerRemoveBackend(runtime,
														   &state.logical.backend);
		ok = ok && scheduler->parked_protocol_count ==
			base_parked_protocol_count;
		ok = ok && scheduler->runnable_count == base_runnable_count;

		PgRuntimeSetCurrentWork(saved_runtime, saved_carrier, saved_backend,
								saved_session, saved_connection,
								saved_execution, false);
		CurrentMemoryContext = saved_current_memory_context;
		CurrentResourceOwner = saved_current_resource_owner;
	}
	PG_CATCH();
	{
		(void) PgRuntimeProtocolSchedulerRemoveBackend(runtime,
													   &state.logical.backend);
		if (state.carrier.current_backend != NULL)
			PgCarrierDetachBackend(&state.carrier, &state.logical.backend);
		PgRuntimeSetCurrentWork(saved_runtime, saved_carrier, saved_backend,
								saved_session, saved_connection,
								saved_execution, false);
		CurrentMemoryContext = saved_current_memory_context;
		CurrentResourceOwner = saved_current_resource_owner;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "protocol scheduler buffered read poll failed");

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


/*
 * Simulate the fiber-context switch handoff that the chained fiber-ctx hook
 * performs on every coroutine switch, using the public current-work snapshot
 * API + the LAZY restore (PgRuntimeRestoreCurrentWorkLazy).  A backend fiber
 * saves its six current-work roots, the scheduler runs a sibling backend on
 * the same OS thread (clobbering the thread-local current-work bridge), and
 * the fiber then lazily restores its own roots on resume.
 *
 * This proves, without a live carrier, the root-handoff claim behind the hook
 * while pinned: the six roots and the compatibility globals derived from them
 * (MyProc/MyProcNumber/MyLatch) round-trip exactly across a sibling switch
 * even though the lazy restore skips RebindSessionGUCVariablePointers.  The
 * companion claim -- that a session-local GUC read still resolves through the
 * restored session root without that rebind -- is covered by
 * test_session_connection_guc_state_is_session_local, which swaps the session
 * root with the same rebind=false refresh (PgSetCurrentSession) and reads GUCs
 * back per session.
 */
PG_FUNCTION_INFO_V1(test_current_work_snapshot_lazy_restore);
Datum
test_current_work_snapshot_lazy_restore(PG_FUNCTION_ARGS)
{
	PgRuntime  *saved_runtime;
	PgCarrier  *saved_carrier;
	PgBackend  *saved_backend;
	PgSession  *saved_session;
	PgConnection *saved_connection;
	PgExecution *saved_execution;
	MemoryContext saved_current_memory_context;
	ResourceOwner saved_current_resource_owner;
	PgThreadBackendRuntimeState state_a;
	PgThreadBackendRuntimeState state_b;
	PGPROC		fake_proc_a;
	PGPROC		fake_proc_b;
	Latch		fake_latch_a;
	Latch		fake_latch_b;
	PgCurrentWorkSnapshot snap;
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
	InitializePgThreadBackendRuntimeState(&state_a, B_BACKEND, NULL,
										  &fake_latch_a);
	InitializePgThreadBackendRuntimeState(&state_b, B_BACKEND, NULL,
										  &fake_latch_b);
	PgCarrierDetachBackend(&state_a.carrier, &state_a.logical.backend);
	PgCarrierDetachBackend(&state_b.carrier, &state_b.logical.backend);
	MemSet(&fake_proc_a, 0, sizeof(fake_proc_a));
	MemSet(&fake_proc_b, 0, sizeof(fake_proc_b));
	InitLatch(&fake_latch_a);
	InitLatch(&fake_latch_b);
	state_a.logical.backend.my_proc = &fake_proc_a;
	state_a.logical.backend.my_proc_number = 101;
	state_a.logical.backend.core.latch = &fake_latch_a;
	state_b.logical.backend.my_proc = &fake_proc_b;
	state_b.logical.backend.my_proc_number = 102;
	state_b.logical.backend.core.latch = &fake_latch_b;

	PG_TRY();
	{
		/* Fiber A becomes current. */
		PgCarrierAttachBackend(&state_a.carrier, &state_a.logical.backend,
							   &state_a.logical.session,
							   &state_a.logical.connection,
							   &state_a.logical.execution);
		ok = ok && CurrentPgBackend == &state_a.logical.backend;
		ok = ok && MyProc == &fake_proc_a;
		ok = ok && MyProcNumber == 101;
		ok = ok && MyLatch == &fake_latch_a;

		/*
		 * Fiber A is about to park: the chained hook's save captures A's six
		 * roots.  (We call the same public entry the hook uses.)
		 */
		PgRuntimeSaveCurrentWork(&snap);
		ok = ok && snap.backend == &state_a.logical.backend;
		ok = ok && snap.session == &state_a.logical.session;
		ok = ok && snap.carrier == &state_a.carrier;

		/*
		 * The scheduler runs sibling fiber B on this OS thread, clobbering the
		 * thread-local current-work bridge with B's roots.
		 */
		PgCarrierAttachBackend(&state_b.carrier, &state_b.logical.backend,
							   &state_b.logical.session,
							   &state_b.logical.connection,
							   &state_b.logical.execution);
		ok = ok && CurrentPgBackend == &state_b.logical.backend;
		ok = ok && CurrentPgSession == &state_b.logical.session;
		ok = ok && MyProc == &fake_proc_b;
		ok = ok && MyProcNumber == 102;
		ok = ok && MyLatch == &fake_latch_b;

		/*
		 * Fiber A resumes: the chained hook's restore reinstalls A's roots
		 * WITHOUT a GUC rebind.  Verify every root is A's again and that the
		 * compatibility globals derived from them track A.
		 */
		PgRuntimeRestoreCurrentWorkLazy(&snap);
		ok = ok && CurrentPgRuntime == snap.runtime;
		ok = ok && CurrentPgCarrier == &state_a.carrier;
		ok = ok && CurrentPgBackend == &state_a.logical.backend;
		ok = ok && CurrentPgSession == &state_a.logical.session;
		ok = ok && CurrentPgConnection == &state_a.logical.connection;
		ok = ok && CurrentPgExecution == &state_a.logical.execution;
		ok = ok && MyProc == &fake_proc_a;
		ok = ok && MyProcNumber == 101;
		ok = ok && MyLatch == &fake_latch_a;

		/* Detach whichever backend is current and restore the real work. */
		if (CurrentPgCarrier == &state_a.carrier &&
			state_a.carrier.current_backend != NULL)
			PgCarrierDetachBackend(&state_a.carrier, &state_a.logical.backend);
		if (state_b.carrier.current_backend != NULL)
			PgCarrierDetachBackend(&state_b.carrier, &state_b.logical.backend);

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
		elog(ERROR, "current-work lazy restore did not preserve fiber-local roots");

	PG_RETURN_BOOL(true);
}
