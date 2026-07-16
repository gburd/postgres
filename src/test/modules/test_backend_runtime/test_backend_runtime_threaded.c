/*--------------------------------------------------------------------------
 *
 * test_backend_runtime_threaded.c
 *		Thread-safe helper module for backend runtime TAP tests
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_backend_runtime/test_backend_runtime_threaded.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/condition_variable.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/pmsignal.h"
#include "utils/backend_runtime.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/wait_event.h"

PG_MODULE_MAGIC_EXT(
					.name = "test_backend_runtime_threaded",
					.version = PG_VERSION,
					PG_MODULE_MAGIC_BACKEND_MODEL_POOLED_PROTOCOL_AFFINE
);

PG_FUNCTION_INFO_V1(test_backend_runtime_model_snapshot);
PG_FUNCTION_INFO_V1(test_backend_runtime_request_autovacuum_worker);
PG_FUNCTION_INFO_V1(test_backend_runtime_rejects_process_bgworker);
PG_FUNCTION_INFO_V1(test_backend_runtime_launch_thread_bgworker);
PG_FUNCTION_INFO_V1(test_backend_runtime_restart_thread_bgworker);
PG_FUNCTION_INFO_V1(test_backend_runtime_crash_thread_bgworker);
PG_FUNCTION_INFO_V1(test_backend_runtime_custom_guc_value);
PG_FUNCTION_INFO_V1(test_backend_runtime_custom_guc_init_count);
PG_FUNCTION_INFO_V1(test_backend_runtime_emit_fatal);
PG_FUNCTION_INFO_V1(test_backend_runtime_crash_current_backend);
PG_FUNCTION_INFO_V1(test_backend_runtime_wait_completion_enabled);
PG_FUNCTION_INFO_V1(test_backend_runtime_wait_completion_snapshot);
PG_FUNCTION_INFO_V1(test_backend_runtime_protocol_park_snapshot);
PG_FUNCTION_INFO_V1(test_backend_runtime_wait_on_condition_variable);
PG_FUNCTION_INFO_V1(test_backend_runtime_hold_lwlock);
PG_FUNCTION_INFO_V1(test_backend_runtime_wait_on_lwlock);

pg_noreturn PGDLLEXPORT void test_backend_runtime_unreachable_bgworker_main(Datum main_arg);
PGDLLEXPORT void test_backend_runtime_thread_bgworker_main(Datum main_arg);
PGDLLEXPORT void test_backend_runtime_restart_bgworker_main(Datum main_arg);
pg_noreturn PGDLLEXPORT void test_backend_runtime_crash_bgworker_main(Datum main_arg);
PGDLLEXPORT void _PG_init(void);

static uint32 test_backend_runtime_thread_bgworker_wait_event = 0;
static uint32 test_backend_runtime_condition_variable_wait_event = 0;
static uint32 test_backend_runtime_hold_lwlock_wait_event = 0;
static bool test_backend_runtime_lwlock_initialized = false;
static LWLock test_backend_runtime_lwlock;
static pg_atomic_uint32 test_backend_runtime_restart_count;
static pg_atomic_uint32 test_backend_runtime_crash_count;
/*
 * Custom GUC backing storage.  A custom GUC's value address is stored in the
 * (per-backend) GUC table and written through by set_config on whatever carrier
 * thread the session runs on.  A PG_THREAD_LOCAL var is WRONG here: its address
 * is per-OS-thread, but a session's fiber can run on any carrier and _PG_init
 * captures the address once -- so a concurrent set_config on another thread
 * writes/frees through a foreign thread's TLS slot and crashes.  Back it with
 * per-session extension-module state instead (the pattern plperl uses).
 */
#define test_backend_runtime_custom_guc \
	(*PgCurrentTestBackendRuntimeCustomGucRef())
#define test_backend_runtime_custom_guc_init_counter \
	(*PgCurrentTestBackendRuntimeCustomGucInitCounterRef())

static const char *
test_backend_runtime_kind_name(PgRuntimeKind kind)
{
	switch (kind)
	{
		case PG_RUNTIME_PROCESS:
			return "process";
		case PG_RUNTIME_THREAD_PER_SESSION:
			return "thread_per_session";
		case PG_RUNTIME_POOLED_PROTOCOL:
			return "pooled_protocol";
	}

	return "unknown";
}

static const char *
test_backend_runtime_model_name(PgBackendModel model)
{
	switch (model)
	{
		case PG_BACKEND_MODEL_PROCESS:
			return "process";
		case PG_BACKEND_MODEL_THREAD_PER_SESSION:
			return "thread-per-session";
		case PG_BACKEND_MODEL_POOLED_SCHEDULER:
			return "pooled-scheduler";
		case PG_BACKEND_MODEL_POOLED_PROTOCOL_AFFINE:
			return "pooled-protocol-affine";
		case PG_BACKEND_MODEL_POOLED_PROTOCOL_MIGRATABLE:
			return "pooled-protocol-migratable";
		case PG_BACKEND_MODEL_TASK_REENTRANT:
			return "task-reentrant";
	}

	return "unknown";
}

static const char *
test_backend_runtime_wait_kind_name(PgWaitKind kind)
{
	switch (kind)
	{
		case PG_WAIT_KIND_NONE:
			return "none";
		case PG_WAIT_KIND_EVENT_SET:
			return "event_set";
		case PG_WAIT_KIND_SEMAPHORE:
			return "semaphore";
	}

	return "unknown";
}

static const char *
test_backend_runtime_wait_completion_state_name(uint32 state)
{
	switch (state)
	{
		case PG_WAIT_COMPLETION_INACTIVE:
			return "inactive";
		case PG_WAIT_COMPLETION_WAITING:
			return "waiting";
		case PG_WAIT_COMPLETION_READY:
			return "ready";
		case PG_WAIT_COMPLETION_CANCELLED:
			return "cancelled";
	}

	return "unknown";
}

static const char *
test_backend_runtime_protocol_park_state_name(PgProtocolParkState state)
{
	switch (state)
	{
		case PG_PROTOCOL_PARK_NONE:
			return "none";
		case PG_PROTOCOL_PARK_PREPARED:
			return "prepared";
		case PG_PROTOCOL_PARK_COMMITTED:
			return "committed";
	}

	return "unknown";
}

static const char *
test_backend_runtime_protocol_queue_state_name(
	PgProtocolSchedulerQueueState state)
{
	switch (state)
	{
		case PG_PROTOCOL_SCHEDULER_QUEUE_NONE:
			return "none";
		case PG_PROTOCOL_SCHEDULER_QUEUE_PARKED_PROTOCOL_READ:
			return "parked_protocol_read";
		case PG_PROTOCOL_SCHEDULER_QUEUE_POLLING:
			return "polling";
		case PG_PROTOCOL_SCHEDULER_QUEUE_RUNNABLE:
			return "runnable";
		case PG_PROTOCOL_SCHEDULER_QUEUE_LEASED:
			return "leased";
	}

	return "unknown";
}

void
_PG_init(void)
{
	test_backend_runtime_custom_guc_init_counter++;

	DefineCustomStringVariable("test_backend_runtime_threaded.custom_guc",
							   "Test threaded custom GUC.",
							   NULL,
							   &test_backend_runtime_custom_guc,
							   "default",
							   PGC_USERSET,
							   0,
							   NULL,
							   NULL,
							   NULL);

	if (!test_backend_runtime_lwlock_initialized)
	{
		/*
		 * Do NOT allocate the tranche here: _PG_init runs during
		 * process_shared_preload_libraries(), before shared memory exists, and
		 * LWLockNewTrancheId() now takes the shmem-resident LWLockTranches
		 * spinlock (upstream refactor) -> it crashes if called pre-shmem.
		 * Defer tranche allocation + LWLockInitialize to first use (see
		 * test_backend_runtime_ensure_lwlock), when shmem is attached.
		 */
	}
}

/*
 * Lazily allocate the tranche and initialize the (backend-local) test LWLock on
 * first use, so it happens after shared memory is available rather than in
 * _PG_init at preload time.
 */
static void
test_backend_runtime_ensure_lwlock(void)
{
	if (!test_backend_runtime_lwlock_initialized)
	{
		int			tranche_id = LWLockNewTrancheId("TestBackendRuntimeLWLock");

		LWLockInitialize(&test_backend_runtime_lwlock, tranche_id);
		test_backend_runtime_lwlock_initialized = true;
	}
}

Datum
test_backend_runtime_model_snapshot(PG_FUNCTION_ARGS)
{
	PgRuntime  *runtime = CurrentPgRuntime;
	char	   *result;

	if (runtime == NULL)
		PG_RETURN_NULL();

	result = psprintf("%s|%s|%d|%d",
					  test_backend_runtime_kind_name(runtime->kind),
					  test_backend_runtime_model_name(
						  runtime->extension_backend_model),
					  PgRuntimePooledProtocolCarrierLimit(),
					  PgRuntimePooledProtocolRequested());

	PG_RETURN_TEXT_P(cstring_to_text(result));
}

Datum
test_backend_runtime_request_autovacuum_worker(PG_FUNCTION_ARGS)
{
	SendPostmasterSignal(PMSIGNAL_START_AUTOVAC_WORKER);

	PG_RETURN_BOOL(true);
}

Datum
test_backend_runtime_rejects_process_bgworker(PG_FUNCTION_ARGS)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;
	BgwHandleStatus status;
	pid_t		pid;

	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	snprintf(worker.bgw_library_name, MAXPGPATH, "test_backend_runtime_threaded");
	snprintf(worker.bgw_function_name, BGW_MAXLEN,
			 "test_backend_runtime_unreachable_bgworker_main");
	snprintf(worker.bgw_name, BGW_MAXLEN,
			 "test_backend_runtime process bgworker");
	snprintf(worker.bgw_type, BGW_MAXLEN,
			 "test_backend_runtime process bgworker");
	worker.bgw_notify_pid = PgCurrentBackendSignalPid();

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
		elog(ERROR, "could not register process-model background worker");

	status = WaitForBackgroundWorkerStartup(handle, &pid);
	if (status != BGWH_STOPPED)
	{
		if (status == BGWH_STARTED)
			TerminateBackgroundWorker(handle);
		elog(ERROR, "process-model background worker was not rejected: status %d",
			 status);
	}

	PG_RETURN_BOOL(true);
}

Datum
test_backend_runtime_launch_thread_bgworker(PG_FUNCTION_ARGS)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;
	BgwHandleStatus status = BGWH_NOT_YET_STARTED;
	pid_t		pid = 0;

	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
	worker.bgw_backend_model = BgWorkerBackendThreadPerSession;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	snprintf(worker.bgw_library_name, MAXPGPATH, "test_backend_runtime_threaded");
	snprintf(worker.bgw_function_name, BGW_MAXLEN,
			 "test_backend_runtime_thread_bgworker_main");
	snprintf(worker.bgw_name, BGW_MAXLEN,
			 "test_backend_runtime thread bgworker");
	snprintf(worker.bgw_type, BGW_MAXLEN,
			 "test_backend_runtime thread bgworker");
	worker.bgw_notify_pid = PgCurrentBackendSignalPid();

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
		elog(ERROR, "could not register thread-model background worker");

	status = WaitForBackgroundWorkerStartup(handle, &pid);
	if (status != BGWH_STARTED)
		elog(ERROR, "thread-model background worker did not start: status %d",
			 status);

	TerminateBackgroundWorker(handle);
	status = WaitForBackgroundWorkerShutdown(handle);
	if (status != BGWH_STOPPED)
		elog(ERROR, "thread-model background worker did not stop: status %d",
			 status);

	PG_RETURN_INT32(pid);
}

Datum
test_backend_runtime_restart_thread_bgworker(PG_FUNCTION_ARGS)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;
	BgwHandleStatus status = BGWH_NOT_YET_STARTED;
	pid_t		pid = 0;
	bool		restarted = false;

	pg_atomic_init_u32(&test_backend_runtime_restart_count, 0);

	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
	worker.bgw_backend_model = BgWorkerBackendThreadPerSession;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = 1;
	snprintf(worker.bgw_library_name, MAXPGPATH, "test_backend_runtime_threaded");
	snprintf(worker.bgw_function_name, BGW_MAXLEN,
			 "test_backend_runtime_restart_bgworker_main");
	snprintf(worker.bgw_name, BGW_MAXLEN,
			 "test_backend_runtime restart bgworker");
	snprintf(worker.bgw_type, BGW_MAXLEN,
			 "test_backend_runtime restart bgworker");
	worker.bgw_notify_pid = PgCurrentBackendSignalPid();

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
		elog(ERROR, "could not register restartable thread-model background worker");

	status = WaitForBackgroundWorkerStartup(handle, &pid);
	if (status != BGWH_STARTED &&
		!(status == BGWH_STOPPED &&
		  pg_atomic_read_u32(&test_backend_runtime_restart_count) >= 1))
		elog(ERROR, "restartable thread-model background worker did not start: status %d",
			 status);

	for (int i = 0; i < 50; i++)
	{
		PgCurrentBackendApplyInterrupts();
		CHECK_FOR_INTERRUPTS();

		if (pg_atomic_read_u32(&test_backend_runtime_restart_count) >= 2)
		{
			restarted = true;
			break;
		}

		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 100L,
						 WAIT_EVENT_BGWORKER_STARTUP);
		ResetLatch(MyLatch);
	}

	if (!restarted)
	{
		TerminateBackgroundWorker(handle);
		(void) WaitForBackgroundWorkerShutdown(handle);
		elog(ERROR, "restartable thread-model background worker did not restart");
	}

	TerminateBackgroundWorker(handle);
	status = WaitForBackgroundWorkerShutdown(handle);
	if (status != BGWH_STOPPED)
		elog(ERROR, "restartable thread-model background worker did not stop: status %d",
			 status);

	PG_RETURN_BOOL(true);
}

Datum
test_backend_runtime_crash_thread_bgworker(PG_FUNCTION_ARGS)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;
	BgwHandleStatus status = BGWH_NOT_YET_STARTED;
	pid_t		pid = 0;

	pg_atomic_init_u32(&test_backend_runtime_crash_count, 0);

	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
	worker.bgw_backend_model = BgWorkerBackendThreadPerSession;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	snprintf(worker.bgw_library_name, MAXPGPATH, "test_backend_runtime_threaded");
	snprintf(worker.bgw_function_name, BGW_MAXLEN,
			 "test_backend_runtime_crash_bgworker_main");
	snprintf(worker.bgw_name, BGW_MAXLEN,
			 "test_backend_runtime crash bgworker");
	snprintf(worker.bgw_type, BGW_MAXLEN,
			 "test_backend_runtime crash bgworker");
	worker.bgw_notify_pid = PgCurrentBackendSignalPid();

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
		elog(ERROR, "could not register crashing thread-model background worker");

	status = WaitForBackgroundWorkerStartup(handle, &pid);
	if (status != BGWH_STARTED)
		elog(ERROR, "crashing thread-model background worker did not start: status %d",
			 status);

	(void) WaitForBackgroundWorkerShutdown(handle);

	elog(ERROR, "crashing thread-model background worker did not terminate the threaded runtime");
	PG_RETURN_BOOL(false);
}

Datum
test_backend_runtime_custom_guc_value(PG_FUNCTION_ARGS)
{
	if (test_backend_runtime_custom_guc == NULL)
		PG_RETURN_NULL();

	PG_RETURN_TEXT_P(cstring_to_text(test_backend_runtime_custom_guc));
}

Datum
test_backend_runtime_custom_guc_init_count(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT32(test_backend_runtime_custom_guc_init_counter);
}

Datum
test_backend_runtime_emit_fatal(PG_FUNCTION_ARGS)
{
	ereport(FATAL,
			(errmsg("test_backend_runtime requested FATAL")));
	pg_unreachable();
}

/*
 * Crash the CURRENT backend with a SIGSEGV (NULL deref).  In a threaded/pooled
 * runtime this exercises the fiber-crash containment + genuine-crash escalation
 * path: the fault must be contained on the carrier, escalate to a postmaster
 * crash-recovery cycle, and not silently corrupt or hang sibling sessions.
 */
Datum
test_backend_runtime_crash_current_backend(PG_FUNCTION_ARGS)
{
	volatile int *p = NULL;

	*p = 42;					/* SIGSEGV */
	PG_RETURN_BOOL(false);		/* not reached */
}

Datum
test_backend_runtime_wait_completion_enabled(PG_FUNCTION_ARGS)
{
#ifdef PG_RUNTIME_ENABLE_WAIT_COMPLETION_PUBLICATION
	PG_RETURN_BOOL(true);
#else
	PG_RETURN_BOOL(false);
#endif
}

Datum
test_backend_runtime_wait_completion_snapshot(PG_FUNCTION_ARGS)
{
	int32		backend_id_arg = PG_GETARG_INT32(0);
	PgWaitCompletion snapshot;
	uint32		waiting;
	uint32		state;
	uint32		ready_events;
	uint32		interrupt_events;
	const char *wait_event;
	char	   *result;

	if (backend_id_arg <= 0)
		PG_RETURN_NULL();

	if (!PgBackendSnapshotWaitCompletionById((PgBackendId) backend_id_arg,
											 &snapshot, &waiting))
		PG_RETURN_NULL();

	state = pg_atomic_read_u32(&snapshot.state);
	ready_events = pg_atomic_read_u32(&snapshot.ready_events);
	interrupt_events = pg_atomic_read_u32(&snapshot.interrupt_events);
	wait_event = pgstat_get_wait_event(snapshot.spec.wait_event_info);
	if (wait_event == NULL)
		wait_event = "";

	result = psprintf("%s|%s|%s|%u|%u|%u|%u|%d|%d|%d",
					  test_backend_runtime_wait_completion_state_name(state),
					  test_backend_runtime_wait_kind_name(snapshot.spec.kind),
					  wait_event,
					  waiting,
					  snapshot.spec.wake_events,
					  ready_events,
					  interrupt_events,
					  snapshot.backend != NULL,
					  snapshot.session != NULL,
					  snapshot.execution != NULL);

	PG_RETURN_TEXT_P(cstring_to_text(result));
}

Datum
test_backend_runtime_protocol_park_snapshot(PG_FUNCTION_ARGS)
{
	int32		backend_id_arg = PG_GETARG_INT32(0);
	PgProtocolParkSnapshot snapshot;
	char	   *result;

	if (backend_id_arg <= 0)
		PG_RETURN_NULL();

	if (!PgBackendSnapshotProtocolParkById((PgBackendId) backend_id_arg,
										   &snapshot))
		PG_RETURN_NULL();

	result = psprintf("%s|%s|"
					  UINT64_FORMAT "|%u|%u|" UINT64_FORMAT "|%u|%u|"
					  UINT64_FORMAT "|" UINT64_FORMAT "|" UINT64_FORMAT "|"
					  UINT64_FORMAT "|%u|%u|%u|" UINT64_FORMAT "|"
					  UINT64_FORMAT "|%d|%d|%d|%d|%u|" UINT64_FORMAT "|"
					  UINT64_FORMAT "|%u|%u|%u|%d|%ld",
					  test_backend_runtime_protocol_park_state_name(snapshot.state),
					  test_backend_runtime_protocol_queue_state_name(
						  snapshot.scheduler_queue_state),
					  snapshot.generation,
					  snapshot.wake_reasons,
					  snapshot.wake_events,
					  snapshot.wake_generation,
					  snapshot.last_wake_reasons,
					  snapshot.last_wake_events,
					  snapshot.last_wake_generation,
					  snapshot.notify_wake_generation,
					  snapshot.deferred_notify_generation,
					  snapshot.deferred_notify_park_generation,
					  snapshot.deferred_notify_reasons,
					  snapshot.scheduler_runnable_count,
					  snapshot.scheduler_parked_protocol_count,
					  snapshot.scheduler_runnable_enqueue_count,
					  snapshot.scheduler_parked_protocol_enqueue_count,
					  snapshot.carrier_attached,
					  snapshot.session_present,
					  snapshot.connection_present,
					  snapshot.execution_present,
					  snapshot.scheduler_carrier_limit,
					  snapshot.scheduler_same_carrier_resume_count,
					  snapshot.scheduler_migrated_resume_count,
					  snapshot.scheduler_registered_carrier_count,
					  snapshot.scheduler_idle_carrier_count,
					  snapshot.scheduler_active_carrier_count,
					  snapshot.last_park_duration_valid,
					  snapshot.last_park_duration_ms);

	PG_RETURN_TEXT_P(cstring_to_text(result));
}

Datum
test_backend_runtime_wait_on_condition_variable(PG_FUNCTION_ARGS)
{
	int32		timeout_ms = PG_GETARG_INT32(0);
	ConditionVariable cv;
	volatile bool timed_out = false;

	if (timeout_ms < 0)
		ereport(ERROR,
				errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("condition variable wait timeout must not be negative"));

	if (test_backend_runtime_condition_variable_wait_event == 0)
		test_backend_runtime_condition_variable_wait_event =
			WaitEventExtensionNew("TestBackendRuntimeConditionVariable");

	ConditionVariableInit(&cv);
	ConditionVariablePrepareToSleep(&cv);

	PG_TRY();
	{
		timed_out = ConditionVariableTimedSleep(&cv, timeout_ms,
												test_backend_runtime_condition_variable_wait_event);
	}
	PG_FINALLY();
	{
		ConditionVariableCancelSleep();
	}
	PG_END_TRY();

	PG_RETURN_BOOL(timed_out);
}

Datum
test_backend_runtime_hold_lwlock(PG_FUNCTION_ARGS)
{
	int32		timeout_ms = PG_GETARG_INT32(0);
	volatile bool held = false;

	if (timeout_ms < 0)
		ereport(ERROR,
				errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("LWLock hold timeout must not be negative"));

	if (test_backend_runtime_hold_lwlock_wait_event == 0)
		test_backend_runtime_hold_lwlock_wait_event =
			WaitEventExtensionNew("TestBackendRuntimeHoldLWLock");

	test_backend_runtime_ensure_lwlock();
	LWLockAcquire(&test_backend_runtime_lwlock, LW_EXCLUSIVE);
	held = true;

	PG_TRY();
	{
		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 timeout_ms,
						 test_backend_runtime_hold_lwlock_wait_event);
		ResetLatch(MyLatch);
		CHECK_FOR_INTERRUPTS();
	}
	PG_FINALLY();
	{
		if (held)
			LWLockRelease(&test_backend_runtime_lwlock);
	}
	PG_END_TRY();

	PG_RETURN_BOOL(true);
}

Datum
test_backend_runtime_wait_on_lwlock(PG_FUNCTION_ARGS)
{
	test_backend_runtime_ensure_lwlock();
	LWLockAcquire(&test_backend_runtime_lwlock, LW_EXCLUSIVE);
	LWLockRelease(&test_backend_runtime_lwlock);

	PG_RETURN_BOOL(true);
}

void
test_backend_runtime_unreachable_bgworker_main(Datum main_arg)
{
	elog(FATAL, "process-model background worker unexpectedly started");
	pg_unreachable();
}

void
test_backend_runtime_thread_bgworker_main(Datum main_arg)
{
	if (test_backend_runtime_thread_bgworker_wait_event == 0)
		test_backend_runtime_thread_bgworker_wait_event =
			WaitEventExtensionNew("TestBackendRuntimeThreadBgWorker");

	for (;;)
	{
		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 10000L,
						 test_backend_runtime_thread_bgworker_wait_event);
		ResetLatch(MyLatch);

		PgCurrentBackendApplyInterrupts();
		if (ProcDiePending || ShutdownRequestPending)
			break;

		CHECK_FOR_INTERRUPTS();
	}
}

void
test_backend_runtime_restart_bgworker_main(Datum main_arg)
{
	uint32		run_count;

	run_count = pg_atomic_fetch_add_u32(&test_backend_runtime_restart_count, 1) + 1;
	elog(LOG, "test_backend_runtime restart bgworker run %u", run_count);

	if (run_count == 1)
		proc_exit(1);

	test_backend_runtime_thread_bgworker_main(main_arg);
}

void
test_backend_runtime_crash_bgworker_main(Datum main_arg)
{
	uint32		run_count;

	run_count = pg_atomic_fetch_add_u32(&test_backend_runtime_crash_count, 1) + 1;
	elog(LOG, "test_backend_runtime crash bgworker run %u", run_count);

	proc_exit(17);
}
