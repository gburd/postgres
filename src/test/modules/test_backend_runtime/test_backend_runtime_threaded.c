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
#include "portability/instr_time.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "postmaster/pg_xtc_carrier.h"	/* xtc_pg_carrier_total_steals */
#include "replication/worker_internal.h"
#include "storage/dsm.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "utils/backend_runtime.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/injection_point.h"
#include "utils/wait_event.h"

PG_MODULE_MAGIC_EXT(
					.name = "test_backend_runtime_threaded",
					.version = PG_VERSION,
					PG_MODULE_MAGIC_BACKEND_MODEL_THREAD_SAFE
);

PG_FUNCTION_INFO_V1(test_backend_runtime_bgworker_startup_publication);
PG_FUNCTION_INFO_V1(test_backend_runtime_model_snapshot);
PG_FUNCTION_INFO_V1(test_backend_runtime_request_autovacuum_worker);
PG_FUNCTION_INFO_V1(test_backend_runtime_rejects_process_bgworker);
PG_FUNCTION_INFO_V1(test_backend_runtime_launch_thread_bgworker);
PG_FUNCTION_INFO_V1(test_backend_runtime_restart_thread_bgworker);
PG_FUNCTION_INFO_V1(test_backend_runtime_crash_thread_bgworker);
PG_FUNCTION_INFO_V1(test_backend_runtime_custom_guc_value);
PG_FUNCTION_INFO_V1(test_backend_runtime_custom_guc_init_count);
PG_FUNCTION_INFO_V1(test_backend_runtime_carrier_total_steals);
PG_FUNCTION_INFO_V1(test_backend_runtime_emit_fatal);
PG_FUNCTION_INFO_V1(test_backend_runtime_crash_current_backend);
PG_FUNCTION_INFO_V1(test_backend_runtime_send_forged_down);
PG_FUNCTION_INFO_V1(test_backend_runtime_request_walwriter_exit);

pg_noreturn PGDLLEXPORT void test_backend_runtime_unreachable_bgworker_main(Datum main_arg);
PGDLLEXPORT void test_backend_runtime_thread_bgworker_main(Datum main_arg);
PGDLLEXPORT void test_backend_runtime_restart_bgworker_main(Datum main_arg);
pg_noreturn PGDLLEXPORT void test_backend_runtime_crash_bgworker_main(Datum main_arg);
PGDLLEXPORT void _PG_init(void);

static uint32 test_backend_runtime_thread_bgworker_wait_event = 0;
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
		case PG_BACKEND_MODEL_THREAD_SAFE:
			return "thread-safe";
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
}

Datum
test_backend_runtime_model_snapshot(PG_FUNCTION_ARGS)
{
	PgRuntime  *runtime = CurrentPgRuntime;
	char	   *result;

	if (runtime == NULL)
		PG_RETURN_NULL();

	result = psprintf("%s|%s",
					  test_backend_runtime_kind_name(runtime->kind),
					  test_backend_runtime_model_name(
						  runtime->extension_backend_model));

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
	worker.bgw_backend_model = BgWorkerBackendThreadSafe;
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
	worker.bgw_backend_model = BgWorkerBackendThreadSafe;
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
	worker.bgw_backend_model = BgWorkerBackendThreadSafe;
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

/*
 * Total tasks work-stolen across all carrier loops since startup.  Nonzero
 * proves migratable backend fibers actually rebalanced across loops (the
 * forced-migration stress test asserts this after running many CPU-bound
 * sessions on fewer carriers than sessions).  Returns 0 in single-loop mode
 * or a non-carrier build.
 */
Datum
test_backend_runtime_carrier_total_steals(PG_FUNCTION_ARGS)
{
#ifdef USE_XTC_CARRIER
	PG_RETURN_INT64((int64) xtc_pg_carrier_total_steals());
#else
	PG_RETURN_INT64(0);
#endif
}

Datum
test_backend_runtime_emit_fatal(PG_FUNCTION_ARGS)
{
	ereport(FATAL,
			(errmsg("test_backend_runtime requested FATAL")));
	pg_unreachable();
}

/*
 * Ask the WAL writer to exit cleanly, as the postmaster's SIGTERM would.  In
 * threaded mode every fiber shares the postmaster's OS pid, so a test cannot
 * reach the WAL writer with kill(); this raises the same logical interrupt
 * the postmaster routes a SIGTERM to.  Returns whether a WAL writer was found.
 */
Datum
test_backend_runtime_request_walwriter_exit(PG_FUNCTION_ARGS)
{
	for (uint32 i = 0; i < ProcGlobal->allProcCount; i++)
	{
		PGPROC	   *proc = GetPGProcByNumber(i);

		if (proc->pid != 0 && proc->backendType == B_WAL_WRITER &&
			proc->backendId != 0)
			PG_RETURN_BOOL(PgBackendSendInterruptById(proc->backendId,
													  PG_BACKEND_INTERRUPT_SHUTDOWN_REQUEST,
													  MyProcPid, GetUserId()));
	}
	PG_RETURN_BOOL(false);
}

/*
 * Send this loop's carrier supervisor a forged DOWN that claims the calling
 * backend died of SIGSEGV.  Returns whether the message was sent; whether the
 * server survives is the test.
 */
Datum
test_backend_runtime_send_forged_down(PG_FUNCTION_ARGS)
{
#ifdef USE_XTC_CARRIER
	PG_RETURN_BOOL(xtc_pg_carrier_send_forged_down(SIGSEGV));
#else
	PG_RETURN_BOOL(false);
#endif
}

/*
 * Crash the CURRENT backend with a SIGSEGV (NULL deref).  In a threaded
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

#ifdef USE_INJECTION_POINTS
#define STARTUP_POINT "bgworker-before-startup-complete"
#define STARTUP_WORKER "test_backend_runtime startup target"

typedef struct StartupGate
{
	pg_atomic_uint32 entered;
	pg_atomic_uint32 release;
	pg_atomic_uint32 main_entered;
	pg_atomic_uint32 carrier;
	bool		clean_exit;
} StartupGate;

typedef struct StartupTest
{
	BackgroundWorkerHandle *handles[2];
	dsm_segment *seg;
	bool		attached;
	bool		cleaned;
} StartupTest;

PGDLLEXPORT void test_backend_runtime_bgworker_startup_gate(
	const char *name, const void *private_data, void *arg);
PGDLLEXPORT void test_backend_runtime_startup_worker_main(Datum main_arg);

/* All test waits yield the carrier and have a wall-clock deadline. */
static bool
startup_pause(instr_time start, int seconds)
{
	instr_time	now;

	ResetLatch(MyLatch);
	PgCurrentBackendApplyInterrupts();
	CHECK_FOR_INTERRUPTS();
	INSTR_TIME_SET_CURRENT(now);
	INSTR_TIME_SUBTRACT(now, start);
	if (INSTR_TIME_GET_DOUBLE(now) >= seconds)
		return false;
	(void) WaitLatch(MyLatch,
					 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					 10L, WAIT_EVENT_BGWORKER_STARTUP);
	return true;
}

/* Wait for an acknowledgment, terminal startup status, or shutdown. */
static bool
startup_wait(BackgroundWorkerHandle *handle, pg_atomic_uint32 *flag, bool stop)
{
	instr_time	start;

	INSTR_TIME_SET_CURRENT(start);
	for (;;)
	{
		pid_t		pid;
		BgwHandleStatus status = GetBackgroundWorkerPid(handle, &pid);

		if (flag != NULL)
		{
			if (pg_atomic_read_u32(flag))
				return true;
			if (status == BGWH_STOPPED)
				return false;
		}
		else if (stop ? status == BGWH_STOPPED : status != BGWH_NOT_YET_STARTED)
			return true;
		if (!startup_pause(start, 5))
			return false;
	}
}

static const char *
startup_status(BgwHandleStatus status)
{
	switch (status)
	{
		case BGWH_STARTED:
			return "STARTED";
		case BGWH_NOT_YET_STARTED:
			return "NOT_YET_STARTED";
		case BGWH_STOPPED:
			return "STOPPED";
		case BGWH_POSTMASTER_DIED:
			return "POSTMASTER_DIED";
	}
	return "unknown";
}

/* The sentinel has no DSM argument and must never enter the target gate. */
static BackgroundWorkerHandle *
startup_register(dsm_handle handle, bool missing_entry)
{
	BackgroundWorker worker = {0};
	BackgroundWorkerHandle *result;

	worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
	worker.bgw_backend_model = BgWorkerBackendThreadSafe;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	strlcpy(worker.bgw_library_name, "test_backend_runtime_threaded", MAXPGPATH);
	strlcpy(worker.bgw_function_name,
			missing_entry ? "test_backend_runtime_missing_startup_main" :
			"test_backend_runtime_startup_worker_main", BGW_MAXLEN);
	strlcpy(worker.bgw_name, handle == DSM_HANDLE_INVALID ?
			"test_backend_runtime startup sentinel" : STARTUP_WORKER, BGW_MAXLEN);
	strlcpy(worker.bgw_type, worker.bgw_name, BGW_MAXLEN);
	worker.bgw_main_arg = UInt32GetDatum(handle);
	worker.bgw_notify_pid = PgCurrentBackendSignalPid();
	if (!RegisterDynamicBackgroundWorker(&worker, &result))
		elog(ERROR, "could not register startup test worker");
	return result;
}

/* Filter before attaching: unrelated workers share this injection point. */
void
test_backend_runtime_bgworker_startup_gate(const char *name,
										 const void *private_data, void *arg)
{
	BackgroundWorker *worker = arg;
	dsm_handle	handle;
	dsm_segment *seg;
	StartupGate *gate;
	instr_time	start;
	uint32		carrier;
	bool		clean_exit;

	memcpy(&handle, private_data, sizeof(handle));
	if (strcmp(worker->bgw_library_name, "test_backend_runtime_threaded") != 0 ||
		strcmp(worker->bgw_name, STARTUP_WORKER) != 0 ||
		DatumGetUInt32(worker->bgw_main_arg) != handle)
		return;
	seg = dsm_attach(handle);
	if (seg == NULL)
		elog(ERROR, "startup gate DSM segment disappeared");
	gate = dsm_segment_address(seg);
	carrier = PgRuntimeIsThreadBacked(CurrentPgRuntime) ? 2 : 1;
#ifdef USE_XTC_CARRIER
	if (xtc_pg_in_backend_fiber())
		carrier = 3;
#endif
	pg_atomic_write_u32(&gate->carrier, carrier);
	BackgroundWorkerUnblockSignals();
	INSTR_TIME_SET_CURRENT(start);
	pg_atomic_write_u32(&gate->entered, 1);
	while (!pg_atomic_read_u32(&gate->release))
	{
		if (ShutdownRequestPending)
			proc_exit(0);
		if (!startup_pause(start, 30))
			elog(ERROR, "startup gate timed out");
	}
	clean_exit = gate->clean_exit;
	dsm_detach(seg);
	if (clean_exit)
		proc_exit(0);
}

/* Also runs on FATAL, before the creator's DSM mapping can be reclaimed. */
static void
startup_cleanup(int code, Datum arg)
{
	StartupTest *test = (StartupTest *) DatumGetPointer(arg);
	StartupGate *gate = dsm_segment_address(test->seg);
	bool		all_stopped = true;

	HOLD_INTERRUPTS();
	pg_atomic_write_u32(&gate->release, 1);
	if (test->attached)
		InjectionPointDetach(STARTUP_POINT);
	for (int i = 0; i < 2; i++)
		if (test->handles[i] != NULL)
			TerminateBackgroundWorker(test->handles[i]);
	for (int i = 0; i < 2; i++)
		if (test->handles[i] != NULL &&
			!startup_wait(test->handles[i], NULL, true))
			all_stopped = false;
	if (all_stopped)
	{
		dsm_detach(test->seg);
		for (int i = 0; i < 2; i++)
			if (test->handles[i] != NULL)
				pfree(test->handles[i]);
		test->cleaned = true;
	}
	else
	{
		/* Keep late attach and live mappings safe until TAP stops the node. */
		dsm_pin_segment(test->seg);
		dsm_pin_mapping(test->seg);
		elog(WARNING, "startup test cleanup timed out; DSM retained until shutdown");
	}
	RESUME_INTERRUPTS();
}

/* Remain alive until terminated so STARTED cannot race a normal worker exit. */
void
test_backend_runtime_startup_worker_main(Datum main_arg)
{
	dsm_handle	handle = DatumGetUInt32(main_arg);
	instr_time	start;

	BackgroundWorkerUnblockSignals();
	if (handle != DSM_HANDLE_INVALID)
	{
		dsm_segment *seg = dsm_attach(handle);
		StartupGate *gate;

		if (seg == NULL)
			elog(ERROR, "startup worker DSM segment disappeared");
		gate = dsm_segment_address(seg);
		pg_atomic_write_u32(&gate->main_entered, 1);
		dsm_detach(seg);
	}
	INSTR_TIME_SET_CURRENT(start);
	while (!ShutdownRequestPending && startup_pause(start, 30))
		;
}
#endif

/* Return observations only after releasing the gate and reaping our workers. */
Datum
test_backend_runtime_bgworker_startup_publication(PG_FUNCTION_ARGS)
{
#ifdef USE_INJECTION_POINTS
	char	   *test_case = text_to_cstring(PG_GETARG_TEXT_PP(0));
	bool		normal = strcmp(test_case, "normal") == 0;
	bool		missing = strcmp(test_case, "missing") == 0;
	bool		clean = strcmp(test_case, "clean") == 0;
	bool		terminate = strcmp(test_case, "terminate") == 0;
	bool		driver_hold = strcmp(test_case, "driver_hold") == 0;
	StartupTest *test = palloc0_object(StartupTest);
	dsm_segment *seg;
	dsm_handle	handle;
	StartupGate *gate;
	BgwHandleStatus held;
	BgwHandleStatus started;
	BgwHandleStatus stopped;
	pid_t		pid = 0;
	bool		held_pid;
	bool		started_pid;
	uint32		main_entered;
	uint32		carrier;

	if (!normal && !missing && !clean && !terminate && !driver_hold)
		elog(ERROR, "unknown startup publication test case: %s", test_case);
	seg = dsm_create(sizeof(StartupGate), 0);
	test->seg = seg;
	handle = dsm_segment_handle(seg);
	gate = dsm_segment_address(seg);
	pg_atomic_init_u32(&gate->entered, 0);
	pg_atomic_init_u32(&gate->release, 0);
	pg_atomic_init_u32(&gate->main_entered, 0);
	pg_atomic_init_u32(&gate->carrier, 0);
	gate->clean_exit = clean;

	PG_ENSURE_ERROR_CLEANUP(startup_cleanup, PointerGetDatum(test));
	{
		InjectionPointAttach(STARTUP_POINT, "test_backend_runtime_threaded",
							 "test_backend_runtime_bgworker_startup_gate",
							 &handle, sizeof(handle));
		test->attached = true;
		test->handles[0] = startup_register(handle, missing);
		if (!startup_wait(test->handles[0], &gate->entered, false))
			elog(ERROR, "startup target did not enter gate");

		/* Gate entry alone does not prove the launch-time reporter has run. */
		test->handles[1] = startup_register(DSM_HANDLE_INVALID, false);
		if (!startup_wait(test->handles[1], NULL, false) ||
			WaitForBackgroundWorkerStartup(test->handles[1], &pid) != BGWH_STARTED)
			elog(ERROR, "startup sentinel did not start");
		if (driver_hold)
		{
			instr_time	start;

			INSTR_TIME_SET_CURRENT(start);
			elog(NOTICE, "startup driver ready: %d", PgCurrentBackendSignalPid());
			while (startup_pause(start, 10))
				;
			elog(ERROR, "startup driver hold timed out");
		}
		pid = 0;
		held = GetBackgroundWorkerPid(test->handles[0], &pid);
		held_pid = pid > 0;
		carrier = pg_atomic_read_u32(&gate->carrier);

		if (terminate)
			TerminateBackgroundWorker(test->handles[0]);
		else
			pg_atomic_write_u32(&gate->release, 1);

		if (!startup_wait(test->handles[0], NULL, !normal))
			elog(ERROR, "startup target did not finish startup");
		pid = 0;
		started = WaitForBackgroundWorkerStartup(test->handles[0], &pid);
		started_pid = pid > 0;
		if (normal)
		{
			if (!startup_wait(test->handles[0], &gate->main_entered, false))
				elog(ERROR, "startup target did not enter main");
			TerminateBackgroundWorker(test->handles[0]);
		}
		if (!startup_wait(test->handles[0], NULL, true))
			elog(ERROR, "startup target did not stop");
		stopped = WaitForBackgroundWorkerShutdown(test->handles[0]);
		main_entered = pg_atomic_read_u32(&gate->main_entered);

	}
	PG_END_ENSURE_ERROR_CLEANUP(startup_cleanup, PointerGetDatum(test));
	startup_cleanup(0, PointerGetDatum(test));
	if (!test->cleaned)
		elog(ERROR, "startup test workers did not stop during cleanup");
	pfree(test);

	PG_RETURN_TEXT_P(cstring_to_text(psprintf(
		"held=%s;held_pid=%d;startup=%s;startup_pid=%d;main=%u;stopped=%s;carrier=%s",
		startup_status(held), held_pid, startup_status(started), started_pid,
		main_entered, startup_status(stopped),
		carrier == 1 ? "process" : carrier == 3 ? "fiber" : "thread")));
#else
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("startup publication test requires injection points")));
	PG_RETURN_NULL();
#endif
}

/* The keeper owns the mapping; no process-global gate registry is needed. */
#ifdef USE_INJECTION_POINTS
#define SUBSCRIPTION_POINT "init-pre-auth"

typedef struct SubscriptionGateKey
{
	dsm_handle	handle;
	Oid			subid;
} SubscriptionGateKey;

typedef struct SubscriptionGate
{
	pg_atomic_uint32 claimed;
	pg_atomic_uint32 entered;
	pg_atomic_uint32 release;
	pg_atomic_uint32 left;
	pg_atomic_uint32 timed_out;
	pg_atomic_uint32 attached;
	Oid			subid;
	pid_t		pid;
	uint16		generation;
	ProcNumber	procno;
	PgBackendId backend_id;
	uint32		carrier;
} SubscriptionGate;

PGDLLEXPORT void test_backend_runtime_subscription_startup_gate(
	const char *name, const void *private_data, void *arg);

/* FATAL can bypass PG_FINALLY; DSM teardown still records gate departure. */
static void
subscription_gate_left(dsm_segment *seg, Datum arg)
{
	SubscriptionGate *gate = dsm_segment_address(seg);

	pg_atomic_write_u32(&gate->left, 1);
}

/* A failed keeper releases the worker, retaining the segment until shutdown. */
static void
subscription_keeper_detach(dsm_segment *seg, Datum arg)
{
	SubscriptionGate *gate = dsm_segment_address(seg);

	pg_atomic_write_u32(&gate->release, 1);
	if (pg_atomic_read_u32(&gate->attached))
		InjectionPointDetach(SUBSCRIPTION_POINT);
}

void
test_backend_runtime_subscription_startup_gate(const char *name,
											  const void *private_data, void *arg)
{
	SubscriptionGateKey key;
	dsm_segment *seg;
	SubscriptionGate *gate;
	instr_time	start;
	uint32		expected = 0;

	memcpy(&key, private_data, sizeof(key));
	if (MyBackendType != B_BG_WORKER ||
		MyLogicalRepWorker == NULL ||
		MyLogicalRepWorker->type != WORKERTYPE_APPLY ||
		MyLogicalRepWorker->subid != key.subid)
		return;
	seg = dsm_attach(key.handle);
	if (seg == NULL)
		elog(ERROR, "subscription gate DSM segment disappeared");
	gate = dsm_segment_address(seg);
	if (!pg_atomic_compare_exchange_u32(&gate->claimed, &expected, 1))
	{
		dsm_detach(seg);
		return;
	}
	PG_TRY();
	{
		on_dsm_detach(seg, subscription_gate_left, (Datum) 0);
		LWLockAcquire(LogicalRepWorkerLock, LW_SHARED);
		Assert(MyLogicalRepWorker->proc == MyProc);
		Assert(MyLogicalRepWorker->procno == MyProcNumber);
		gate->pid = MyLogicalRepWorker->signal_pid;
		gate->generation = MyLogicalRepWorker->generation;
		gate->procno = MyProcNumber;
		gate->backend_id = MyProc->backendId;
		LWLockRelease(LogicalRepWorkerLock);
		gate->carrier = PgRuntimeIsThreadBacked(CurrentPgRuntime) ? 2 : 1;
#ifdef USE_XTC_CARRIER
		if (xtc_pg_in_backend_fiber())
			gate->carrier = 3;
#endif
		INSTR_TIME_SET_CURRENT(start);
		pg_memory_barrier();
		pg_atomic_write_u32(&gate->entered, 1);
		while (!pg_atomic_read_u32(&gate->release))
		{
			/* PROC_DIE is safe here; no subscription lock is held yet. */
			if (!startup_pause(start, 30))
			{
				pg_atomic_write_u32(&gate->timed_out, 1);
				elog(ERROR, "subscription gate timed out");
			}
		}
	}
	PG_FINALLY();
	{
		dsm_detach(seg);
	}
	PG_END_TRY();
}
#endif

PG_FUNCTION_INFO_V1(test_backend_runtime_subscription_arm);
PG_FUNCTION_INFO_V1(test_backend_runtime_subscription_gate);

Datum
test_backend_runtime_subscription_arm(PG_FUNCTION_ARGS)
{
#ifdef USE_INJECTION_POINTS
	SubscriptionGateKey key = {0};
	dsm_segment *seg;
	SubscriptionGate *gate;

	if (!superuser())
		elog(ERROR, "subscription gate requires superuser");
	key.subid = PG_GETARG_OID(0);
	seg = dsm_create(sizeof(SubscriptionGate), 0);
	key.handle = dsm_segment_handle(seg);
	gate = dsm_segment_address(seg);
	memset(gate, 0, sizeof(*gate));
	pg_atomic_init_u32(&gate->claimed, 0);
	pg_atomic_init_u32(&gate->entered, 0);
	pg_atomic_init_u32(&gate->release, 0);
	pg_atomic_init_u32(&gate->left, 0);
	pg_atomic_init_u32(&gate->timed_out, 0);
	pg_atomic_init_u32(&gate->attached, 0);
	gate->subid = key.subid;
	on_dsm_detach(seg, subscription_keeper_detach, (Datum) 0);
	/* Also protect a callback which copied private_data before keeper exit. */
	dsm_pin_segment(seg);
	PG_TRY();
	{
		InjectionPointAttach(SUBSCRIPTION_POINT, "test_backend_runtime_threaded",
							 "test_backend_runtime_subscription_startup_gate",
							 &key, sizeof(key));
		pg_atomic_write_u32(&gate->attached, 1);
		dsm_pin_mapping(seg);
	}
	PG_CATCH();
	{
		/* Keep the segment even if a callback copied the key before ERROR. */
		dsm_detach(seg);
		PG_RE_THROW();
	}
	PG_END_TRY();
	PG_RETURN_INT64((int64) key.handle);
#else
	elog(ERROR, "subscription gate requires injection points");
	PG_RETURN_NULL();
#endif
}

/* Status only reads worker pointers while holding LogicalRepWorkerLock. */
Datum
test_backend_runtime_subscription_gate(PG_FUNCTION_ARGS)
{
#ifdef USE_INJECTION_POINTS
	dsm_handle	handle = (dsm_handle) PG_GETARG_INT64(0);
	char	   *action = text_to_cstring(PG_GETARG_TEXT_PP(1));
	dsm_segment *seg;
	SubscriptionGate *gate;
	List	   *workers;
	ListCell   *lc;
	bool		matching = false;
	int			live;
	uint32		entered;

	if (!superuser())
		elog(ERROR, "subscription gate requires superuser");
	seg = dsm_find_mapping(handle);
	if (seg == NULL || dsm_segment_map_length(seg) < sizeof(SubscriptionGate))
		elog(ERROR, "subscription gate is not owned by this session");
	gate = dsm_segment_address(seg);
	entered = pg_atomic_read_u32(&gate->entered);
	pg_memory_barrier();
	LWLockAcquire(LogicalRepWorkerLock, LW_SHARED);
	workers = logicalrep_workers_find(gate->subid, false, false);
	live = list_length(workers);
	foreach(lc, workers)
	{
		LogicalRepWorker *worker = lfirst(lc);

		if (entered && worker->type == WORKERTYPE_APPLY &&
			worker->generation == gate->generation &&
			worker->signal_pid == gate->pid && worker->procno == gate->procno &&
			worker->proc != NULL && worker->proc == GetPGProcByNumber(gate->procno) &&
			worker->proc->backendId == gate->backend_id)
			matching = true;
	}
	LWLockRelease(LogicalRepWorkerLock);
	list_free(workers);
	if (strcmp(action, "release") == 0)
		pg_atomic_write_u32(&gate->release, 1);
	else if (strcmp(action, "close") == 0)
	{
		if (live != 0 || (entered && !pg_atomic_read_u32(&gate->left)))
			elog(ERROR, "subscription workers still own the gate");
		InjectionPointDetach(SUBSCRIPTION_POINT);
		cancel_on_dsm_detach(seg, subscription_keeper_detach, (Datum) 0);
		/* Detach is not a join; retain the segment until node shutdown. */
		dsm_detach(seg);
		PG_RETURN_TEXT_P(cstring_to_text("closed"));
	}
	else if (strcmp(action, "status") != 0)
		elog(ERROR, "unknown subscription gate action: %s", action);
	PG_RETURN_TEXT_P(cstring_to_text(psprintf(
		"%u|%u|%u|%u|%d|%u|%d|" UINT64_FORMAT "|%s|%d|%d",
		entered, pg_atomic_read_u32(&gate->release),
		pg_atomic_read_u32(&gate->left), pg_atomic_read_u32(&gate->timed_out),
		entered ? gate->pid : 0, entered ? gate->generation : 0,
		entered ? gate->procno : INVALID_PROC_NUMBER,
		entered ? gate->backend_id : (PgBackendId) 0,
		!entered ? "none" : gate->carrier == 1 ? "process" :
		gate->carrier == 3 ? "fiber" : "thread", matching, live)));
#else
	elog(ERROR, "subscription gate requires injection points");
	PG_RETURN_NULL();
#endif
}
