/*--------------------------------------------------------------------------
 *
 * test_backend_runtime_thread.c
 *		Thread runtime tests.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_backend_runtime/test_backend_runtime_thread.c
 *
 * -------------------------------------------------------------------------
 */
#include "test_backend_runtime.h"

static void test_pg_thread_routine(void *arg);
static void test_pg_thread_exit_routine(void *arg);

typedef struct TestPooledWaitCallbackState
{
	PgRuntime  *runtime;
	PgBackend  *backend;
	Latch	   *scheduler_latch;
	bool		saw_waiting;
	bool		saw_runnable;
	bool		saw_scheduler_wake;
} TestPooledWaitCallbackState;

static int	test_pooled_wait_callback(void *arg);

static void
test_pg_thread_routine(void *arg)
{
	pg_atomic_uint32 *ran = (pg_atomic_uint32 *) arg;

	pg_atomic_write_u32(ran, 1);
}

static void
test_pg_thread_exit_routine(void *arg)
{
	pg_atomic_uint32 *ran = (pg_atomic_uint32 *) arg;

	pg_atomic_write_u32(ran, 1);
	pg_thread_exit();
}

PG_FUNCTION_INFO_V1(test_backend_thread_create_join);
Datum
test_backend_thread_create_join(PG_FUNCTION_ARGS)
{
	PgThread	thread;
	pg_atomic_uint32 ran;
	int			rc;

	pg_atomic_init_u32(&ran, 0);
	rc = pg_thread_create(&thread, "pg test thread",
						  test_pg_thread_routine, &ran);
	if (rc != 0)
	{
		errno = rc;
		elog(ERROR, "pg_thread_create failed: %m");
	}

	rc = pg_thread_join(&thread);
	if (rc != 0)
	{
		errno = rc;
		elog(ERROR, "pg_thread_join failed: %m");
	}

	if (pg_atomic_read_u32(&ran) != 1)
		elog(ERROR, "thread routine did not run");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_backend_thread_exit_join);
Datum
test_backend_thread_exit_join(PG_FUNCTION_ARGS)
{
	PgThread	thread;
	pg_atomic_uint32 ran;
	int			rc;

	pg_atomic_init_u32(&ran, 0);
	rc = pg_thread_create(&thread, "pg test thread exit",
						  test_pg_thread_exit_routine, &ran);
	if (rc != 0)
	{
		errno = rc;
		elog(ERROR, "pg_thread_create failed: %m");
	}

	rc = pg_thread_join(&thread);
	if (rc != 0)
	{
		errno = rc;
		elog(ERROR, "pg_thread_join failed: %m");
	}

	if (pg_atomic_read_u32(&ran) != 1)
		elog(ERROR, "thread exit routine did not run");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_backend_thread_runtime_state);
Datum
test_backend_thread_runtime_state(PG_FUNCTION_ARGS)
{
#define CHECK_THREAD_RUNTIME_STATE(expr) \
	do { \
		if (!(expr)) \
			elog(ERROR, "thread backend runtime state check failed: %s", \
				 #expr); \
	} while (0)

	PgRuntime  *saved_runtime;
	PgCarrier  *saved_carrier;
	PgBackend  *saved_backend;
	PgSession  *saved_session;
	PgConnection *saved_connection;
	PgExecution *saved_execution;
	PgThreadBackendRuntimeState state;
	Latch		fake_latch;

	saved_runtime = CurrentPgRuntime;
	saved_carrier = CurrentPgCarrier;
	saved_backend = CurrentPgBackend;
	saved_session = CurrentPgSession;
	saved_connection = CurrentPgConnection;
	saved_execution = CurrentPgExecution;

	InitLatch(&fake_latch);

	PG_TRY();
	{
		InitializePgThreadRuntime(NULL);
		InitializePgThreadBackendRuntimeState(&state, B_BACKEND, NULL,
											  &fake_latch);

		CHECK_THREAD_RUNTIME_STATE(state.backend.runtime != NULL);
		CHECK_THREAD_RUNTIME_STATE(state.backend.runtime->kind ==
								   PG_RUNTIME_THREAD_PER_SESSION);
		CHECK_THREAD_RUNTIME_STATE(state.backend.runtime->extension_backend_model ==
								   PG_BACKEND_MODEL_THREAD_PER_SESSION);
		CHECK_THREAD_RUNTIME_STATE(state.carrier.kind == PG_CARRIER_THREAD);
		CHECK_THREAD_RUNTIME_STATE(state.carrier.current_backend == &state.backend);
		CHECK_THREAD_RUNTIME_STATE(state.carrier.current_session == &state.session);
		CHECK_THREAD_RUNTIME_STATE(state.carrier.current_execution == &state.execution);
		CHECK_THREAD_RUNTIME_STATE(state.carrier.backend_thread_start == NULL);
		CHECK_THREAD_RUNTIME_STATE(state.carrier.wait_event_waiting == false);
		CHECK_THREAD_RUNTIME_STATE(state.carrier.wait_event_signal_fd == -1);
		CHECK_THREAD_RUNTIME_STATE(state.carrier.wait_event_selfpipe_readfd == -1);
		CHECK_THREAD_RUNTIME_STATE(state.carrier.wait_event_selfpipe_writefd == -1);
		CHECK_THREAD_RUNTIME_STATE(state.carrier.wait_event_selfpipe_owner_pid == 0);
		CHECK_THREAD_RUNTIME_STATE(state.carrier.stack_base_ptr == NULL);
		CHECK_THREAD_RUNTIME_STATE(state.backend.backend_type == B_BACKEND);
		CHECK_THREAD_RUNTIME_STATE(state.backend.interrupt_latch == &fake_latch);
		CHECK_THREAD_RUNTIME_STATE(dlist_is_empty(&state.backend.dsm_segment_list));
		CHECK_THREAD_RUNTIME_STATE(state.backend.session == &state.session);
		CHECK_THREAD_RUNTIME_STATE(state.backend.connection == &state.connection);
		CHECK_THREAD_RUNTIME_STATE(state.backend.execution == &state.execution);
		CHECK_THREAD_RUNTIME_STATE(state.session.backend == &state.backend);
		CHECK_THREAD_RUNTIME_STATE(state.session.connection == &state.connection);
		CHECK_THREAD_RUNTIME_STATE(state.session.execution == &state.execution);
		CHECK_THREAD_RUNTIME_STATE(state.connection.backend == &state.backend);
		CHECK_THREAD_RUNTIME_STATE(state.connection.session == &state.session);
		CHECK_THREAD_RUNTIME_STATE(state.execution.backend == &state.backend);
		CHECK_THREAD_RUNTIME_STATE(state.execution.session == &state.session);
		CHECK_THREAD_RUNTIME_STATE(state.execution.carrier == &state.carrier);
		CHECK_THREAD_RUNTIME_STATE(CurrentPgRuntime == saved_runtime);
		CHECK_THREAD_RUNTIME_STATE(CurrentPgCarrier == saved_carrier);
		CHECK_THREAD_RUNTIME_STATE(CurrentPgBackend == saved_backend);
		CHECK_THREAD_RUNTIME_STATE(CurrentPgSession == saved_session);
		CHECK_THREAD_RUNTIME_STATE(CurrentPgConnection == saved_connection);
		CHECK_THREAD_RUNTIME_STATE(CurrentPgExecution == saved_execution);

		PgSetCurrentRuntime(saved_runtime);
		PgSetCurrentCarrier(saved_carrier);
		PgSetCurrentBackend(saved_backend);
		PgSetCurrentSession(saved_session);
		PgSetCurrentConnection(saved_connection);
		PgSetCurrentExecution(saved_execution);
	}
	PG_CATCH();
	{
		PgSetCurrentRuntime(saved_runtime);
		PgSetCurrentCarrier(saved_carrier);
		PgSetCurrentBackend(saved_backend);
		PgSetCurrentSession(saved_session);
		PgSetCurrentConnection(saved_connection);
		PgSetCurrentExecution(saved_execution);
		PG_RE_THROW();
	}
	PG_END_TRY();

#undef CHECK_THREAD_RUNTIME_STATE

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_backend_pooled_scheduler_queue_state);
Datum
test_backend_pooled_scheduler_queue_state(PG_FUNCTION_ARGS)
{
#define CHECK_POOLED_SCHEDULER(expr) \
	do { \
		if (!(expr)) \
			elog(ERROR, "pooled scheduler check failed: %s", #expr); \
	} while (0)

	PgRuntime	pooled_runtime;
	PgThreadBackendRuntimeState state;
	Latch		fake_latch;
	Latch		scheduler_latch;
	uint32		runnable_count;
	uint32		waiting_count;
	PgBackend  *popped;

	InitLatch(&fake_latch);
	InitLatch(&scheduler_latch);
	MemSet(&pooled_runtime, 0, sizeof(pooled_runtime));
	PgRuntimeSchedulerInitialize(&pooled_runtime);
	pooled_runtime.kind = PG_RUNTIME_POOLED_SCHEDULER;
	pooled_runtime.extension_backend_model =
		PG_BACKEND_MODEL_POOLED_SCHEDULER;
	PgRuntimeSchedulerSetWakeLatch(&pooled_runtime, &scheduler_latch);

	InitializePgThreadRuntime(NULL);
	InitializePgThreadBackendRuntimeState(&state, B_BACKEND, NULL,
										  &fake_latch);
	state.carrier.runtime = &pooled_runtime;
	state.backend.runtime = &pooled_runtime;
	pooled_runtime.current_carrier = &state.carrier;
	PgBackendSchedulerInitialize(&state.backend.scheduler);

	CHECK_POOLED_SCHEDULER(PgRuntimeIsPooledScheduler(&pooled_runtime));
	CHECK_POOLED_SCHEDULER(PgRuntimeUsesLogicalBackends(&pooled_runtime));
	CHECK_POOLED_SCHEDULER(PgRuntimePublishesWaitCompletions(&pooled_runtime));

	PgBackendSchedulerMarkRunning(&state.backend);
	PgRuntimeSchedulerCounts(&pooled_runtime, &runnable_count, &waiting_count);
	CHECK_POOLED_SCHEDULER(runnable_count == 0);
	CHECK_POOLED_SCHEDULER(waiting_count == 0);
	CHECK_POOLED_SCHEDULER(pg_atomic_read_u32(&state.backend.scheduler.state) ==
						   PG_SCHEDULER_BACKEND_RUNNING);

	CHECK_POOLED_SCHEDULER(PgBackendSchedulerMarkWaiting(&state.backend));
	PgRuntimeSchedulerCounts(&pooled_runtime, &runnable_count, &waiting_count);
	CHECK_POOLED_SCHEDULER(runnable_count == 0);
	CHECK_POOLED_SCHEDULER(waiting_count == 1);
	CHECK_POOLED_SCHEDULER(pg_atomic_read_u32(&state.backend.scheduler.state) ==
						   PG_SCHEDULER_BACKEND_WAITING);
	CHECK_POOLED_SCHEDULER(!PgBackendSchedulerMarkWaiting(&state.backend));

	CHECK_POOLED_SCHEDULER(PgBackendSchedulerEnqueueRunnable(&state.backend));
	PgRuntimeSchedulerCounts(&pooled_runtime, &runnable_count, &waiting_count);
	CHECK_POOLED_SCHEDULER(runnable_count == 1);
	CHECK_POOLED_SCHEDULER(waiting_count == 0);
	CHECK_POOLED_SCHEDULER(state.backend.scheduler.enqueue_generation == 1);
	CHECK_POOLED_SCHEDULER(PgRuntimeSchedulerWakeGeneration(&pooled_runtime) == 1);
	CHECK_POOLED_SCHEDULER(scheduler_latch.is_set);
	ResetLatch(&scheduler_latch);
	CHECK_POOLED_SCHEDULER(pg_atomic_read_u32(&state.backend.scheduler.state) ==
						   PG_SCHEDULER_BACKEND_RUNNABLE);
	CHECK_POOLED_SCHEDULER(!PgBackendSchedulerEnqueueRunnable(&state.backend));
	CHECK_POOLED_SCHEDULER(PgRuntimeSchedulerWakeGeneration(&pooled_runtime) == 1);
	CHECK_POOLED_SCHEDULER(!scheduler_latch.is_set);

	popped = PgRuntimeSchedulerPopRunnable(&pooled_runtime);
	CHECK_POOLED_SCHEDULER(popped == &state.backend);
	PgRuntimeSchedulerCounts(&pooled_runtime, &runnable_count, &waiting_count);
	CHECK_POOLED_SCHEDULER(runnable_count == 0);
	CHECK_POOLED_SCHEDULER(waiting_count == 0);
	CHECK_POOLED_SCHEDULER(pg_atomic_read_u32(&state.backend.scheduler.state) ==
						   PG_SCHEDULER_BACKEND_RUNNING);

	PgBackendSchedulerMarkDetached(&state.backend);
	CHECK_POOLED_SCHEDULER(pg_atomic_read_u32(&state.backend.scheduler.state) ==
						   PG_SCHEDULER_BACKEND_DETACHED);

#undef CHECK_POOLED_SCHEDULER

	PG_RETURN_BOOL(true);
}

static int
test_pooled_wait_callback(void *arg)
{
	TestPooledWaitCallbackState *state =
		(TestPooledWaitCallbackState *) arg;
	PgWaitCompletion *completion;
	uint32		runnable_count;
	uint32		waiting_count;

	PgRuntimeSchedulerCounts(state->runtime, &runnable_count, &waiting_count);
	state->saw_waiting =
		runnable_count == 0 &&
		waiting_count == 1 &&
		pg_atomic_read_u32(&state->backend->scheduler.state) ==
		PG_SCHEDULER_BACKEND_WAITING;
	completion = PgBackendCurrentWaitCompletion(state->backend);
	if (completion == NULL || completion->requeue == NULL ||
		completion->requeue_arg != state->runtime)
		elog(ERROR, "pooled wait completion did not install requeue hook");

	if (!PgBackendWakeWaitCompletion(state->backend, WL_LATCH_SET))
		elog(ERROR, "pooled wait completion did not accept wake");

	PgRuntimeSchedulerCounts(state->runtime, &runnable_count, &waiting_count);
	state->saw_runnable =
		runnable_count == 1 &&
		waiting_count == 0 &&
		pg_atomic_read_u32(&state->backend->scheduler.state) ==
		PG_SCHEDULER_BACKEND_RUNNABLE;
	state->saw_scheduler_wake =
		state->scheduler_latch != NULL &&
		state->scheduler_latch->is_set &&
		PgRuntimeSchedulerWakeGeneration(state->runtime) == 1;

	return WL_LATCH_SET;
}

PG_FUNCTION_INFO_V1(test_backend_pooled_wait_requeues_backend);
Datum
test_backend_pooled_wait_requeues_backend(PG_FUNCTION_ARGS)
{
	PgRuntime  *saved_runtime;
	PgCarrier  *saved_carrier;
	PgBackend  *saved_backend;
	PgSession  *saved_session;
	PgConnection *saved_connection;
	PgExecution *saved_execution;
	PgRuntime	pooled_runtime;
	PgThreadBackendRuntimeState state;
	TestPooledWaitCallbackState callback_state;
	PgWaitSpec	wait_spec;
	Latch		fake_latch;
	Latch		scheduler_latch;
	uint32		runnable_count;
	uint32		waiting_count;
	int			result;

	saved_runtime = CurrentPgRuntime;
	saved_carrier = CurrentPgCarrier;
	saved_backend = CurrentPgBackend;
	saved_session = CurrentPgSession;
	saved_connection = CurrentPgConnection;
	saved_execution = CurrentPgExecution;

	InitLatch(&fake_latch);
	InitLatch(&scheduler_latch);
	MemSet(&pooled_runtime, 0, sizeof(pooled_runtime));
	PgRuntimeSchedulerInitialize(&pooled_runtime);
	pooled_runtime.kind = PG_RUNTIME_POOLED_SCHEDULER;
	pooled_runtime.extension_backend_model =
		PG_BACKEND_MODEL_POOLED_SCHEDULER;
	PgRuntimeSchedulerSetWakeLatch(&pooled_runtime, &scheduler_latch);

	PG_TRY();
	{
		InitializePgThreadRuntime(NULL);
		InitializePgThreadBackendRuntimeState(&state, B_BACKEND, NULL,
											  &fake_latch);
		state.carrier.runtime = &pooled_runtime;
		state.backend.runtime = &pooled_runtime;
		pooled_runtime.current_carrier = &state.carrier;
		PgBackendSchedulerInitialize(&state.backend.scheduler);

		PgSetCurrentRuntime(&pooled_runtime);
		PgSetCurrentCarrier(&state.carrier);
		PgSetCurrentBackend(&state.backend);
		PgSetCurrentSession(&state.session);
		PgSetCurrentConnection(&state.connection);
		PgSetCurrentExecution(&state.execution);

		PgBackendSchedulerMarkRunning(&state.backend);

		wait_spec.kind = PG_WAIT_KIND_EVENT_SET;
		wait_spec.wait_event_info = WAIT_EVENT_CLIENT_READ;
		wait_spec.wake_events = WL_LATCH_SET;
		wait_spec.timeout = -1;

		callback_state.runtime = &pooled_runtime;
		callback_state.backend = &state.backend;
		callback_state.scheduler_latch = &scheduler_latch;
		callback_state.saw_waiting = false;
		callback_state.saw_runnable = false;
		callback_state.saw_scheduler_wake = false;

		result = PgSuspend(&wait_spec, test_pooled_wait_callback,
						   &callback_state);

		PgRuntimeSchedulerCounts(&pooled_runtime, &runnable_count,
								 &waiting_count);
		if (result != WL_LATCH_SET ||
			!callback_state.saw_waiting ||
			!callback_state.saw_runnable ||
			!callback_state.saw_scheduler_wake ||
			runnable_count != 0 ||
			waiting_count != 0 ||
			pg_atomic_read_u32(&state.backend.scheduler.state) !=
			PG_SCHEDULER_BACKEND_DETACHED)
			elog(ERROR, "pooled wait did not complete expected requeue cycle");

		PgSetCurrentRuntime(saved_runtime);
		PgSetCurrentCarrier(saved_carrier);
		PgSetCurrentBackend(saved_backend);
		PgSetCurrentSession(saved_session);
		PgSetCurrentConnection(saved_connection);
		PgSetCurrentExecution(saved_execution);
	}
	PG_CATCH();
	{
		PgSetCurrentRuntime(saved_runtime);
		PgSetCurrentCarrier(saved_carrier);
		PgSetCurrentBackend(saved_backend);
		PgSetCurrentSession(saved_session);
		PgSetCurrentConnection(saved_connection);
		PgSetCurrentExecution(saved_execution);
		PG_RE_THROW();
	}
	PG_END_TRY();

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_backend_pgproc_has_logical_id);
Datum
test_backend_pgproc_has_logical_id(PG_FUNCTION_ARGS)
{
	bool		ok;

	ok = MyProc != NULL;
	ok = ok && PgCurrentBackendId() != 0;
	ok = ok && MyProc->backendId != 0;
	ok = ok && MyProc->backendId == PgCurrentBackendId();
	ok = ok && MyProc->pid == MyProcPid;

	PG_RETURN_BOOL(ok);
}

PG_FUNCTION_INFO_V1(test_backend_thread_ids_are_logical);
Datum
test_backend_thread_ids_are_logical(PG_FUNCTION_ARGS)
{
	PgRuntime  *saved_runtime;
	PgCarrier  *saved_carrier;
	PgBackend  *saved_backend;
	PgSession  *saved_session;
	PgConnection *saved_connection;
	PgExecution *saved_execution;
	PgThreadBackendRuntimeState state1;
	PgThreadBackendRuntimeState state2;
	Latch		fake_latch1;
	Latch		fake_latch2;
	PgBackendId current_backend_id;
	PgBackendId thread_backend_id1;
	PgBackendId thread_backend_id2;
	bool		ok = true;

	saved_runtime = CurrentPgRuntime;
	saved_carrier = CurrentPgCarrier;
	saved_backend = CurrentPgBackend;
	saved_session = CurrentPgSession;
	saved_connection = CurrentPgConnection;
	saved_execution = CurrentPgExecution;
	current_backend_id = PgCurrentBackendId();

	InitLatch(&fake_latch1);
	InitLatch(&fake_latch2);

	PG_TRY();
	{
		InitializePgThreadRuntime(NULL);
		InitializePgThreadBackendRuntimeState(&state1, B_BACKEND, NULL,
											  &fake_latch1);
		thread_backend_id1 = PgBackendGetId(&state1.backend);

		InitializePgThreadBackendRuntimeState(&state2, B_BACKEND, NULL,
											  &fake_latch2);
		thread_backend_id2 = PgBackendGetId(&state2.backend);

		ok = ok && current_backend_id != 0;
		ok = ok && thread_backend_id1 != 0;
		ok = ok && thread_backend_id2 != 0;
		ok = ok && thread_backend_id1 != current_backend_id;
		ok = ok && thread_backend_id2 != current_backend_id;
		ok = ok && thread_backend_id1 != thread_backend_id2;
		ok = ok && thread_backend_id1 == PgBackendGetId(&state1.backend);
		ok = ok && thread_backend_id2 == PgBackendGetId(&state2.backend);
		ok = ok && CurrentPgRuntime == saved_runtime;
		ok = ok && CurrentPgCarrier == saved_carrier;
		ok = ok && CurrentPgBackend == saved_backend;
		ok = ok && CurrentPgSession == saved_session;
		ok = ok && CurrentPgConnection == saved_connection;
		ok = ok && CurrentPgExecution == saved_execution;

		PgSetCurrentRuntime(saved_runtime);
		PgSetCurrentCarrier(saved_carrier);
		PgSetCurrentBackend(saved_backend);
		PgSetCurrentSession(saved_session);
		PgSetCurrentConnection(saved_connection);
		PgSetCurrentExecution(saved_execution);
	}
	PG_CATCH();
	{
		PgSetCurrentRuntime(saved_runtime);
		PgSetCurrentCarrier(saved_carrier);
		PgSetCurrentBackend(saved_backend);
		PgSetCurrentSession(saved_session);
		PgSetCurrentConnection(saved_connection);
		PgSetCurrentExecution(saved_execution);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "thread backend ids were not distinct logical ids");

	PG_RETURN_BOOL(true);
}
