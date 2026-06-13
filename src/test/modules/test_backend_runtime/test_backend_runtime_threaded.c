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
#include "storage/latch.h"
#include "storage/pmsignal.h"
#include "utils/backend_runtime.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/wait_event.h"

PG_MODULE_MAGIC_EXT(
					.name = "test_backend_runtime_threaded",
					.version = PG_VERSION,
					PG_MODULE_MAGIC_BACKEND_MODEL_THREAD_PER_SESSION
);

PG_FUNCTION_INFO_V1(test_backend_runtime_request_autovacuum_worker);
PG_FUNCTION_INFO_V1(test_backend_runtime_rejects_process_bgworker);
PG_FUNCTION_INFO_V1(test_backend_runtime_launch_thread_bgworker);
PG_FUNCTION_INFO_V1(test_backend_runtime_restart_thread_bgworker);
PG_FUNCTION_INFO_V1(test_backend_runtime_crash_thread_bgworker);
PG_FUNCTION_INFO_V1(test_backend_runtime_custom_guc_value);
PG_FUNCTION_INFO_V1(test_backend_runtime_custom_guc_init_count);

pg_noreturn PGDLLEXPORT void test_backend_runtime_unreachable_bgworker_main(Datum main_arg);
PGDLLEXPORT void test_backend_runtime_thread_bgworker_main(Datum main_arg);
PGDLLEXPORT void test_backend_runtime_restart_bgworker_main(Datum main_arg);
pg_noreturn PGDLLEXPORT void test_backend_runtime_crash_bgworker_main(Datum main_arg);
PGDLLEXPORT void _PG_init(void);

static uint32 test_backend_runtime_thread_bgworker_wait_event = 0;
static pg_atomic_uint32 test_backend_runtime_restart_count;
static pg_atomic_uint32 test_backend_runtime_crash_count;
static PG_THREAD_LOCAL char *test_backend_runtime_custom_guc = NULL;
static PG_THREAD_LOCAL int test_backend_runtime_custom_guc_init_counter = 0;

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
	if (status != BGWH_STARTED)
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
