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

typedef enum TestPooledSchedulerStepAction
{
	TEST_POOLED_SCHEDULER_STEP_CONTINUE,
	TEST_POOLED_SCHEDULER_STEP_WAIT,
	TEST_POOLED_SCHEDULER_STEP_ERROR_RECOVERED
} TestPooledSchedulerStepAction;

typedef struct TestPooledSchedulerStepState
{
	PgCarrier  *carrier;
	PgBackend  *backend;
	PgWaitSpec	wait_spec;
	TestPooledSchedulerStepAction action;
	int			expected_budget;
	int			call_count;
	bool		expect_cleared_wait;
	bool		published_wait;
	bool		saw_current_work;
	bool		saw_cleared_wait;
} TestPooledSchedulerStepState;

static PgStepResult test_pooled_scheduler_step(PgBackend *backend,
											   PgStepBudget budget,
											   void *arg);
static void test_pooled_scheduler_timeout_handler(void);
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

		PgCarrierAttachBackend(&state.carrier, &state.backend);
		CHECK_THREAD_RUNTIME_STATE(CurrentPgRuntime == state.backend.runtime);
		CHECK_THREAD_RUNTIME_STATE(CurrentPgCarrier == &state.carrier);
		CHECK_THREAD_RUNTIME_STATE(CurrentPgBackend == &state.backend);
		CHECK_THREAD_RUNTIME_STATE(CurrentPgSession == &state.session);
		CHECK_THREAD_RUNTIME_STATE(CurrentPgConnection == &state.connection);
		CHECK_THREAD_RUNTIME_STATE(CurrentPgExecution == &state.execution);
		CHECK_THREAD_RUNTIME_STATE(state.carrier.current_backend == &state.backend);
		CHECK_THREAD_RUNTIME_STATE(state.carrier.current_session == &state.session);
		CHECK_THREAD_RUNTIME_STATE(state.carrier.current_execution == &state.execution);
		CHECK_THREAD_RUNTIME_STATE(state.backend.carrier == &state.carrier);
		CHECK_THREAD_RUNTIME_STATE(state.execution.carrier == &state.carrier);

		PgCarrierDetachBackend(&state.carrier);
		CHECK_THREAD_RUNTIME_STATE(CurrentPgRuntime == state.backend.runtime);
		CHECK_THREAD_RUNTIME_STATE(CurrentPgCarrier == &state.carrier);
		CHECK_THREAD_RUNTIME_STATE(CurrentPgBackend == NULL);
		CHECK_THREAD_RUNTIME_STATE(CurrentPgSession == NULL);
		CHECK_THREAD_RUNTIME_STATE(CurrentPgConnection == NULL);
		CHECK_THREAD_RUNTIME_STATE(CurrentPgExecution == NULL);
		CHECK_THREAD_RUNTIME_STATE(state.carrier.current_backend == NULL);
		CHECK_THREAD_RUNTIME_STATE(state.carrier.current_session == NULL);
		CHECK_THREAD_RUNTIME_STATE(state.carrier.current_execution == NULL);
		CHECK_THREAD_RUNTIME_STATE(state.backend.carrier == NULL);
		CHECK_THREAD_RUNTIME_STATE(state.execution.carrier == NULL);

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

static PgStepResult
test_pooled_scheduler_step(PgBackend *backend, PgStepBudget budget, void *arg)
{
	TestPooledSchedulerStepState *state =
		(TestPooledSchedulerStepState *) arg;

	if (state == NULL ||
		backend != state->backend ||
		CurrentPgCarrier != state->carrier ||
		CurrentPgBackend != state->backend ||
		CurrentPgSession != state->backend->session ||
		CurrentPgConnection != state->backend->connection ||
		CurrentPgExecution != state->backend->execution ||
		budget.max_messages != state->expected_budget)
		elog(ERROR, "pooled scheduler step did not run on expected work");

	state->call_count++;
	state->saw_current_work = true;

	if (state->expect_cleared_wait)
	{
		PgWaitCompletion *completion =
			PgBackendCurrentWaitCompletion(state->backend);

		state->saw_cleared_wait =
			completion != NULL &&
			pg_atomic_read_u32(&state->backend->wait_state.waiting) == 0 &&
			pg_atomic_read_u32(&completion->state) ==
			PG_WAIT_COMPLETION_INACTIVE &&
			completion->backend == NULL;
	}

	if (state->action == TEST_POOLED_SCHEDULER_STEP_WAIT)
	{
		if (!PgBackendPublishWaitCompletion(backend, &state->wait_spec))
			elog(ERROR, "pooled scheduler step did not publish wait");
		state->published_wait = true;
		return PG_STEP_WAITING;
	}
	else if (state->action == TEST_POOLED_SCHEDULER_STEP_ERROR_RECOVERED)
		return PG_STEP_ERROR_RECOVERED;

	return PG_STEP_CONTINUE;
}

static void
test_pooled_scheduler_timeout_handler(void)
{
	PgBackend  *target = get_firing_timeout_target_backend();

	if (target == NULL)
		target = CurrentPgBackend;

	SendInterrupt(target, PG_BACKEND_INTERRUPT_IDLE_SESSION_TIMEOUT);
}

PG_FUNCTION_INFO_V1(test_backend_pooled_scheduler_runs_backend);
Datum
test_backend_pooled_scheduler_runs_backend(PG_FUNCTION_ARGS)
{
#define CHECK_POOLED_RUNNER(expr) \
	do { \
		if (!(expr)) \
			elog(ERROR, "pooled scheduler runner check failed: %s", #expr); \
	} while (0)

	PgRuntime  *saved_runtime;
	PgCarrier  *saved_carrier;
	PgBackend  *saved_backend;
	PgSession  *saved_session;
	PgConnection *saved_connection;
	PgExecution *saved_execution;
	PgRuntime	pooled_runtime;
	PgThreadBackendRuntimeState state;
	TestPooledSchedulerStepState step_state;
	PgStepBudget budget;
	PgStepResult result;
	PgWaitCompletion *completion;
	Latch		fake_latch;
	Latch		scheduler_latch;
	uint32		runnable_count;
	uint32		waiting_count;

	saved_runtime = CurrentPgRuntime;
	saved_carrier = CurrentPgCarrier;
	saved_backend = CurrentPgBackend;
	saved_session = CurrentPgSession;
	saved_connection = CurrentPgConnection;
	saved_execution = CurrentPgExecution;

	MemSet(&state, 0, sizeof(state));
	InitLatch(&fake_latch);
	InitLatch(&scheduler_latch);
	MemSet(&pooled_runtime, 0, sizeof(pooled_runtime));
	PgRuntimeSchedulerInitialize(&pooled_runtime);
	pooled_runtime.kind = PG_RUNTIME_POOLED_SCHEDULER;
	pooled_runtime.extension_backend_model =
		PG_BACKEND_MODEL_POOLED_SCHEDULER;
	PgRuntimeSchedulerSetWakeLatch(&pooled_runtime, &scheduler_latch);

	budget.max_messages = 1;

	PG_TRY();
	{
		InitializePgThreadRuntime(NULL);
		InitializePgThreadBackendRuntimeState(&state, B_BACKEND, NULL,
											  &fake_latch);
		state.carrier.runtime = &pooled_runtime;
		state.backend.runtime = &pooled_runtime;
		PgBackendSchedulerInitialize(&state.backend.scheduler);

		CHECK_POOLED_RUNNER(!PgRuntimeSchedulerRunNext(&pooled_runtime,
													   &state.carrier,
													   budget, &result));

		CHECK_POOLED_RUNNER(PgBackendSchedulerEnqueueRunnable(&state.backend));
		PgRuntimeSchedulerCounts(&pooled_runtime, &runnable_count,
								 &waiting_count);
		CHECK_POOLED_RUNNER(runnable_count == 1);
		CHECK_POOLED_RUNNER(waiting_count == 0);

		MemSet(&step_state, 0, sizeof(step_state));
		step_state.carrier = &state.carrier;
		step_state.backend = &state.backend;
		step_state.action = TEST_POOLED_SCHEDULER_STEP_WAIT;
		step_state.expected_budget = budget.max_messages;
		step_state.wait_spec.kind = PG_WAIT_KIND_EVENT_SET;
		step_state.wait_spec.wait_event_info = WAIT_EVENT_CLIENT_READ;
		step_state.wait_spec.wake_events = WL_SOCKET_READABLE;
		step_state.wait_spec.socket = 42;
		step_state.wait_spec.timeout = -1;
		step_state.wait_spec.timeout_at = 0;

		CHECK_POOLED_RUNNER(PgRuntimeSchedulerRunNextWithCallback(&pooled_runtime,
																  &state.carrier,
																  budget,
																  test_pooled_scheduler_step,
																  &step_state,
																  &result));
		CHECK_POOLED_RUNNER(result == PG_STEP_WAITING);
		CHECK_POOLED_RUNNER(step_state.call_count == 1);
		CHECK_POOLED_RUNNER(step_state.saw_current_work);
		CHECK_POOLED_RUNNER(step_state.published_wait);
		CHECK_POOLED_RUNNER(state.carrier.current_backend == NULL);
		CHECK_POOLED_RUNNER(state.backend.carrier == NULL);
		CHECK_POOLED_RUNNER(CurrentPgBackend == NULL);
		PgRuntimeSchedulerCounts(&pooled_runtime, &runnable_count,
								 &waiting_count);
		CHECK_POOLED_RUNNER(runnable_count == 0);
		CHECK_POOLED_RUNNER(waiting_count == 1);
		CHECK_POOLED_RUNNER(pg_atomic_read_u32(&state.backend.scheduler.state) ==
							PG_SCHEDULER_BACKEND_WAITING);

		completion = PgBackendCurrentWaitCompletion(&state.backend);
		CHECK_POOLED_RUNNER(completion != NULL);
		CHECK_POOLED_RUNNER(completion->spec.socket == 42);
		CHECK_POOLED_RUNNER(pg_atomic_read_u32(&completion->state) ==
							PG_WAIT_COMPLETION_WAITING);
		CHECK_POOLED_RUNNER(PgBackendWakeWaitCompletion(&state.backend,
														WL_SOCKET_READABLE));
		PgRuntimeSchedulerCounts(&pooled_runtime, &runnable_count,
								 &waiting_count);
		CHECK_POOLED_RUNNER(runnable_count == 1);
		CHECK_POOLED_RUNNER(waiting_count == 0);
		CHECK_POOLED_RUNNER(pg_atomic_read_u32(&state.backend.scheduler.state) ==
							PG_SCHEDULER_BACKEND_RUNNABLE);

		MemSet(&step_state, 0, sizeof(step_state));
		step_state.carrier = &state.carrier;
		step_state.backend = &state.backend;
		step_state.action = TEST_POOLED_SCHEDULER_STEP_CONTINUE;
		step_state.expected_budget = budget.max_messages;
		step_state.expect_cleared_wait = true;
		CHECK_POOLED_RUNNER(PgRuntimeSchedulerRunNextWithCallback(&pooled_runtime,
																  &state.carrier,
																  budget,
																  test_pooled_scheduler_step,
																  &step_state,
																  &result));
		CHECK_POOLED_RUNNER(result == PG_STEP_CONTINUE);
		CHECK_POOLED_RUNNER(step_state.call_count == 1);
		CHECK_POOLED_RUNNER(step_state.saw_cleared_wait);
		PgRuntimeSchedulerCounts(&pooled_runtime, &runnable_count,
								 &waiting_count);
		CHECK_POOLED_RUNNER(runnable_count == 1);
		CHECK_POOLED_RUNNER(waiting_count == 0);
		CHECK_POOLED_RUNNER(pg_atomic_read_u32(&state.backend.scheduler.state) ==
							PG_SCHEDULER_BACKEND_RUNNABLE);
		CHECK_POOLED_RUNNER(pg_atomic_read_u32(&completion->state) ==
							PG_WAIT_COMPLETION_INACTIVE);

		MemSet(&step_state, 0, sizeof(step_state));
		step_state.carrier = &state.carrier;
		step_state.backend = &state.backend;
		step_state.action = TEST_POOLED_SCHEDULER_STEP_ERROR_RECOVERED;
		step_state.expected_budget = budget.max_messages;
		CHECK_POOLED_RUNNER(PgRuntimeSchedulerRunNextWithCallback(&pooled_runtime,
																  &state.carrier,
																  budget,
																  test_pooled_scheduler_step,
																  &step_state,
																  &result));
		CHECK_POOLED_RUNNER(result == PG_STEP_ERROR_RECOVERED);
		CHECK_POOLED_RUNNER(step_state.call_count == 1);
		PgRuntimeSchedulerCounts(&pooled_runtime, &runnable_count,
								 &waiting_count);
		CHECK_POOLED_RUNNER(runnable_count == 1);
		CHECK_POOLED_RUNNER(waiting_count == 0);
		CHECK_POOLED_RUNNER(pg_atomic_read_u32(&state.backend.scheduler.state) ==
							PG_SCHEDULER_BACKEND_RUNNABLE);

		PgBackendSchedulerMarkDetached(&state.backend);
		PgCarrierDetachBackend(&state.carrier);
		PgSetCurrentRuntime(saved_runtime);
		PgSetCurrentCarrier(saved_carrier);
		PgSetCurrentBackend(saved_backend);
		PgSetCurrentSession(saved_session);
		PgSetCurrentConnection(saved_connection);
		PgSetCurrentExecution(saved_execution);
	}
	PG_CATCH();
	{
		PgBackendClearPublishedWaitCompletion(&state.backend);
		PgBackendSchedulerMarkDetached(&state.backend);
		PgCarrierDetachBackend(&state.carrier);
		PgSetCurrentRuntime(saved_runtime);
		PgSetCurrentCarrier(saved_carrier);
		PgSetCurrentBackend(saved_backend);
		PgSetCurrentSession(saved_session);
		PgSetCurrentConnection(saved_connection);
		PgSetCurrentExecution(saved_execution);
		PG_RE_THROW();
	}
	PG_END_TRY();

#undef CHECK_POOLED_RUNNER

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

	MemSet(&state, 0, sizeof(state));
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
		PgBackendSchedulerInitialize(&state.backend.scheduler);

		PgCarrierAttachBackend(&state.carrier, &state.backend);

		PgBackendSchedulerMarkRunning(&state.backend);

		wait_spec.kind = PG_WAIT_KIND_EVENT_SET;
		wait_spec.wait_event_info = WAIT_EVENT_CLIENT_READ;
		wait_spec.wake_events = WL_LATCH_SET;
		wait_spec.socket = PGINVALID_SOCKET;
		wait_spec.timeout = -1;
		wait_spec.timeout_at = 0;

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

		PgCarrierDetachBackend(&state.carrier);
		PgSetCurrentRuntime(saved_runtime);
		PgSetCurrentCarrier(saved_carrier);
		PgSetCurrentBackend(saved_backend);
		PgSetCurrentSession(saved_session);
		PgSetCurrentConnection(saved_connection);
		PgSetCurrentExecution(saved_execution);
	}
	PG_CATCH();
	{
		PgCarrierDetachBackend(&state.carrier);
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

PG_FUNCTION_INFO_V1(test_backend_pooled_wait_parks_backend);
Datum
test_backend_pooled_wait_parks_backend(PG_FUNCTION_ARGS)
{
#define CHECK_POOLED_PARK(expr) \
	do { \
		if (!(expr)) \
			elog(ERROR, "pooled wait park check failed: %s", #expr); \
	} while (0)

	PgRuntime  *saved_runtime;
	PgCarrier  *saved_carrier;
	PgBackend  *saved_backend;
	PgSession  *saved_session;
	PgConnection *saved_connection;
	PgExecution *saved_execution;
	PgRuntime	pooled_runtime;
	PgThreadBackendRuntimeState state;
	PgWaitSpec	wait_spec;
	PgWaitCompletion *completion;
	PgRuntimeSchedulerSocketWait socket_waits[1];
	PgRuntimeSchedulerWaitSnapshot snapshot;
	PgBackend  *popped;
	Latch		fake_latch;
	Latch		scheduler_latch;
	uint32		runnable_count;
	uint32		waiting_count;
	bool		wait_published = false;
	TimestampTz now;

	saved_runtime = CurrentPgRuntime;
	saved_carrier = CurrentPgCarrier;
	saved_backend = CurrentPgBackend;
	saved_session = CurrentPgSession;
	saved_connection = CurrentPgConnection;
	saved_execution = CurrentPgExecution;

	MemSet(&state, 0, sizeof(state));
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
		PgBackendSchedulerInitialize(&state.backend.scheduler);

		PgCarrierAttachBackend(&state.carrier, &state.backend);
		PgBackendSchedulerMarkRunning(&state.backend);

		wait_spec.kind = PG_WAIT_KIND_EVENT_SET;
		wait_spec.wait_event_info = WAIT_EVENT_CLIENT_READ;
		wait_spec.wake_events = WL_SOCKET_READABLE;
		wait_spec.socket = 42;
		wait_spec.timeout = -1;
		wait_spec.timeout_at = 0;

		CHECK_POOLED_PARK(PgBackendPublishWaitCompletion(&state.backend,
														 &wait_spec));
		wait_published = true;
		PgRuntimeSchedulerCounts(&pooled_runtime, &runnable_count,
								 &waiting_count);
		completion = PgBackendCurrentWaitCompletion(&state.backend);
		CHECK_POOLED_PARK(completion != NULL);
		CHECK_POOLED_PARK(pg_atomic_read_u32(&completion->state) ==
						  PG_WAIT_COMPLETION_WAITING);
		CHECK_POOLED_PARK(pg_atomic_read_u32(&state.backend.wait_state.waiting) == 1);
		CHECK_POOLED_PARK(completion->backend == &state.backend);
		CHECK_POOLED_PARK(completion->session == &state.session);
		CHECK_POOLED_PARK(completion->execution == &state.execution);
		CHECK_POOLED_PARK(completion->spec.wait_event_info ==
						  WAIT_EVENT_CLIENT_READ);
		CHECK_POOLED_PARK(completion->spec.socket == 42);
		CHECK_POOLED_PARK(completion->requeue_arg == &pooled_runtime);
		CHECK_POOLED_PARK(runnable_count == 0);
		CHECK_POOLED_PARK(waiting_count == 1);
		CHECK_POOLED_PARK(pg_atomic_read_u32(&state.backend.scheduler.state) ==
						  PG_SCHEDULER_BACKEND_WAITING);

		PgCarrierDetachBackend(&state.carrier);
		CHECK_POOLED_PARK(CurrentPgBackend == NULL);
		CHECK_POOLED_PARK(CurrentPgSession == NULL);
		CHECK_POOLED_PARK(CurrentPgConnection == NULL);
		CHECK_POOLED_PARK(CurrentPgExecution == NULL);
		CHECK_POOLED_PARK(PgRuntimeSchedulerSnapshotWaits(&pooled_runtime,
														  socket_waits,
														  lengthof(socket_waits),
														  GetCurrentTimestamp(),
														  &snapshot) == 1);
		CHECK_POOLED_PARK(snapshot.runnable_count == 0);
		CHECK_POOLED_PARK(snapshot.waiting_count == 1);
		CHECK_POOLED_PARK(snapshot.socket_count == 1);
		CHECK_POOLED_PARK(!snapshot.socket_overflow);
		CHECK_POOLED_PARK(!snapshot.has_timeout);
		CHECK_POOLED_PARK(snapshot.timeout == -1);
		CHECK_POOLED_PARK(socket_waits[0].socket == 42);
		CHECK_POOLED_PARK(socket_waits[0].wake_events == WL_SOCKET_READABLE);
		CHECK_POOLED_PARK(socket_waits[0].wait_event_info ==
						  WAIT_EVENT_CLIENT_READ);

		CHECK_POOLED_PARK(PgRuntimeSchedulerWakeSocket(&pooled_runtime, 17,
													   WL_SOCKET_READABLE) == 0);
		CHECK_POOLED_PARK(PgRuntimeSchedulerWakeSocket(&pooled_runtime, 42,
													   WL_SOCKET_WRITEABLE) == 0);
		CHECK_POOLED_PARK(PgRuntimeSchedulerWakeSocket(&pooled_runtime, 42,
													   WL_SOCKET_READABLE) == 1);
		PgRuntimeSchedulerCounts(&pooled_runtime, &runnable_count,
								 &waiting_count);
		CHECK_POOLED_PARK(pg_atomic_read_u32(&completion->state) ==
						  PG_WAIT_COMPLETION_READY);
		CHECK_POOLED_PARK(pg_atomic_read_u32(&completion->ready_events) ==
						  WL_SOCKET_READABLE);
		CHECK_POOLED_PARK(runnable_count == 1);
		CHECK_POOLED_PARK(waiting_count == 0);
		CHECK_POOLED_PARK(scheduler_latch.is_set);
		CHECK_POOLED_PARK(PgRuntimeSchedulerWakeGeneration(&pooled_runtime) == 1);

		popped = PgRuntimeSchedulerPopRunnable(&pooled_runtime);
		CHECK_POOLED_PARK(popped == &state.backend);
		CHECK_POOLED_PARK(pg_atomic_read_u32(&state.backend.scheduler.state) ==
						  PG_SCHEDULER_BACKEND_RUNNING);

		PgCarrierAttachBackend(&state.carrier, &state.backend);
		PgBackendClearPublishedWaitCompletion(&state.backend);
		wait_published = false;
		PgRuntimeSchedulerCounts(&pooled_runtime, &runnable_count,
								 &waiting_count);
		CHECK_POOLED_PARK(runnable_count == 0);
		CHECK_POOLED_PARK(waiting_count == 0);
		CHECK_POOLED_PARK(pg_atomic_read_u32(&state.backend.wait_state.waiting) == 0);
		CHECK_POOLED_PARK(pg_atomic_read_u32(&completion->state) ==
						  PG_WAIT_COMPLETION_INACTIVE);
		CHECK_POOLED_PARK(pg_atomic_read_u32(&state.backend.scheduler.state) ==
						  PG_SCHEDULER_BACKEND_RUNNING);

		ResetLatch(&scheduler_latch);
		CHECK_POOLED_PARK(PgBackendPublishWaitCompletion(&state.backend,
														 &wait_spec));
		wait_published = true;
		PgCarrierDetachBackend(&state.carrier);
		PgRuntimeSchedulerCounts(&pooled_runtime, &runnable_count,
								 &waiting_count);
		CHECK_POOLED_PARK(runnable_count == 0);
		CHECK_POOLED_PARK(waiting_count == 1);
		CHECK_POOLED_PARK(pg_atomic_read_u32(&completion->state) ==
						  PG_WAIT_COMPLETION_WAITING);

		SendInterrupt(&state.backend, PG_BACKEND_INTERRUPT_QUERY_CANCEL);
		PgRuntimeSchedulerCounts(&pooled_runtime, &runnable_count,
								 &waiting_count);
		CHECK_POOLED_PARK(pg_atomic_read_u32(&completion->state) ==
						  PG_WAIT_COMPLETION_READY);
		CHECK_POOLED_PARK(pg_atomic_read_u32(&completion->ready_events) == 0);
		CHECK_POOLED_PARK(pg_atomic_read_u32(&completion->interrupt_events) ==
						  PG_WAIT_COMPLETION_INTERRUPT_CANCEL);
		CHECK_POOLED_PARK(pg_atomic_read_u32(&state.backend.interrupts.pending_mask) ==
						  PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_QUERY_CANCEL));
		CHECK_POOLED_PARK(runnable_count == 1);
		CHECK_POOLED_PARK(waiting_count == 0);
		CHECK_POOLED_PARK(scheduler_latch.is_set);
		CHECK_POOLED_PARK(PgRuntimeSchedulerWakeGeneration(&pooled_runtime) == 2);

		popped = PgRuntimeSchedulerPopRunnable(&pooled_runtime);
		CHECK_POOLED_PARK(popped == &state.backend);
		PgCarrierAttachBackend(&state.carrier, &state.backend);
		PgBackendClearPublishedWaitCompletion(&state.backend);
		wait_published = false;
		(void) PgBackendConsumeInterrupts(&state.backend);
		PgRuntimeSchedulerCounts(&pooled_runtime, &runnable_count,
								 &waiting_count);
		CHECK_POOLED_PARK(runnable_count == 0);
		CHECK_POOLED_PARK(waiting_count == 0);
		CHECK_POOLED_PARK(pg_atomic_read_u32(&completion->state) ==
						  PG_WAIT_COMPLETION_INACTIVE);
		CHECK_POOLED_PARK(pg_atomic_read_u32(&state.backend.scheduler.state) ==
						  PG_SCHEDULER_BACKEND_RUNNING);

		ResetLatch(&scheduler_latch);
		now = GetCurrentTimestamp();
		wait_spec.timeout = 250;
		wait_spec.timeout_at = TimestampTzPlusMilliseconds(now,
														   wait_spec.timeout);
		CHECK_POOLED_PARK(PgBackendPublishWaitCompletion(&state.backend,
														 &wait_spec));
		wait_published = true;
		PgCarrierDetachBackend(&state.carrier);
		PgRuntimeSchedulerCounts(&pooled_runtime, &runnable_count,
								 &waiting_count);
		CHECK_POOLED_PARK(runnable_count == 0);
		CHECK_POOLED_PARK(waiting_count == 1);
		CHECK_POOLED_PARK(pg_atomic_read_u32(&completion->state) ==
						  PG_WAIT_COMPLETION_WAITING);
		CHECK_POOLED_PARK(PgRuntimeSchedulerSnapshotWaits(&pooled_runtime,
														  socket_waits,
														  lengthof(socket_waits),
														  now,
														  &snapshot) == 1);
		CHECK_POOLED_PARK(snapshot.has_timeout);
		CHECK_POOLED_PARK(snapshot.timeout == wait_spec.timeout);
		CHECK_POOLED_PARK(snapshot.timeout_at == wait_spec.timeout_at);
		CHECK_POOLED_PARK(PgRuntimeSchedulerProcessDueTimeouts(&pooled_runtime,
															   &state.carrier,
															   TimestampTzPlusMilliseconds(now, 249)) == 0);
		PgRuntimeSchedulerCounts(&pooled_runtime, &runnable_count,
								 &waiting_count);
		CHECK_POOLED_PARK(runnable_count == 0);
		CHECK_POOLED_PARK(waiting_count == 1);
		CHECK_POOLED_PARK(PgRuntimeSchedulerProcessDueTimeouts(&pooled_runtime,
															   &state.carrier,
															   wait_spec.timeout_at) == 1);
		PgRuntimeSchedulerCounts(&pooled_runtime, &runnable_count,
								 &waiting_count);
		CHECK_POOLED_PARK(pg_atomic_read_u32(&completion->state) ==
						  PG_WAIT_COMPLETION_READY);
		CHECK_POOLED_PARK(pg_atomic_read_u32(&completion->ready_events) ==
						  WL_TIMEOUT);
		CHECK_POOLED_PARK(pg_atomic_read_u32(&state.backend.interrupts.pending_mask) == 0);
		CHECK_POOLED_PARK(runnable_count == 1);
		CHECK_POOLED_PARK(waiting_count == 0);
		CHECK_POOLED_PARK(scheduler_latch.is_set);
		CHECK_POOLED_PARK(PgRuntimeSchedulerWakeGeneration(&pooled_runtime) == 3);
		CHECK_POOLED_PARK(CurrentPgBackend == NULL);

		popped = PgRuntimeSchedulerPopRunnable(&pooled_runtime);
		CHECK_POOLED_PARK(popped == &state.backend);
		PgCarrierAttachBackend(&state.carrier, &state.backend);
		PgBackendClearPublishedWaitCompletion(&state.backend);
		wait_published = false;
		wait_spec.timeout = -1;
		wait_spec.timeout_at = 0;
		PgRuntimeSchedulerCounts(&pooled_runtime, &runnable_count,
								 &waiting_count);
		CHECK_POOLED_PARK(runnable_count == 0);
		CHECK_POOLED_PARK(waiting_count == 0);
		CHECK_POOLED_PARK(pg_atomic_read_u32(&completion->state) ==
						  PG_WAIT_COMPLETION_INACTIVE);
		CHECK_POOLED_PARK(pg_atomic_read_u32(&state.backend.scheduler.state) ==
						  PG_SCHEDULER_BACKEND_RUNNING);

		ResetLatch(&scheduler_latch);
		InitializeLogicalTimeouts();
		RegisterTimeout(USER_TIMEOUT, test_pooled_scheduler_timeout_handler);
		now = GetCurrentTimestamp();
		enable_timeout_at(USER_TIMEOUT, now - 1000);
		CHECK_POOLED_PARK(PgBackendPublishWaitCompletion(&state.backend,
														 &wait_spec));
		wait_published = true;
		PgCarrierDetachBackend(&state.carrier);
		PgRuntimeSchedulerCounts(&pooled_runtime, &runnable_count,
								 &waiting_count);
		CHECK_POOLED_PARK(runnable_count == 0);
		CHECK_POOLED_PARK(waiting_count == 1);
		CHECK_POOLED_PARK(pg_atomic_read_u32(&completion->state) ==
						  PG_WAIT_COMPLETION_WAITING);

		CHECK_POOLED_PARK(PgRuntimeSchedulerProcessDueTimeouts(&pooled_runtime,
															   &state.carrier,
															   GetCurrentTimestamp()) == 1);
		PgRuntimeSchedulerCounts(&pooled_runtime, &runnable_count,
								 &waiting_count);
		CHECK_POOLED_PARK(pg_atomic_read_u32(&completion->state) ==
						  PG_WAIT_COMPLETION_READY);
		CHECK_POOLED_PARK(pg_atomic_read_u32(&completion->ready_events) ==
						  WL_TIMEOUT);
		CHECK_POOLED_PARK(pg_atomic_read_u32(&state.backend.interrupts.pending_mask) ==
						  PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_IDLE_SESSION_TIMEOUT));
		CHECK_POOLED_PARK(runnable_count == 1);
		CHECK_POOLED_PARK(waiting_count == 0);
		CHECK_POOLED_PARK(scheduler_latch.is_set);
		CHECK_POOLED_PARK(PgRuntimeSchedulerWakeGeneration(&pooled_runtime) == 4);
		CHECK_POOLED_PARK(CurrentPgBackend == NULL);

		popped = PgRuntimeSchedulerPopRunnable(&pooled_runtime);
		CHECK_POOLED_PARK(popped == &state.backend);
		PgCarrierAttachBackend(&state.carrier, &state.backend);
		PgBackendClearPublishedWaitCompletion(&state.backend);
		wait_published = false;
		(void) PgBackendConsumeInterrupts(&state.backend);
		PgRuntimeSchedulerCounts(&pooled_runtime, &runnable_count,
								 &waiting_count);
		CHECK_POOLED_PARK(runnable_count == 0);
		CHECK_POOLED_PARK(waiting_count == 0);
		CHECK_POOLED_PARK(pg_atomic_read_u32(&completion->state) ==
						  PG_WAIT_COMPLETION_INACTIVE);
		CHECK_POOLED_PARK(pg_atomic_read_u32(&state.backend.scheduler.state) ==
						  PG_SCHEDULER_BACKEND_RUNNING);

		PgCarrierDetachBackend(&state.carrier);
		PgSetCurrentRuntime(saved_runtime);
		PgSetCurrentCarrier(saved_carrier);
		PgSetCurrentBackend(saved_backend);
		PgSetCurrentSession(saved_session);
		PgSetCurrentConnection(saved_connection);
		PgSetCurrentExecution(saved_execution);
	}
	PG_CATCH();
	{
		if (wait_published)
			PgBackendClearPublishedWaitCompletion(&state.backend);
		PgCarrierDetachBackend(&state.carrier);
		PgSetCurrentRuntime(saved_runtime);
		PgSetCurrentCarrier(saved_carrier);
		PgSetCurrentBackend(saved_backend);
		PgSetCurrentSession(saved_session);
		PgSetCurrentConnection(saved_connection);
		PgSetCurrentExecution(saved_execution);
		PG_RE_THROW();
	}
	PG_END_TRY();

#undef CHECK_POOLED_PARK

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
