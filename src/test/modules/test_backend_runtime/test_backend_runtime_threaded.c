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
#include "postmaster/bgworker.h"
#include "storage/pmsignal.h"

PG_MODULE_MAGIC_EXT(
					.name = "test_backend_runtime_threaded",
					.version = PG_VERSION,
					PG_MODULE_MAGIC_BACKEND_MODEL_THREAD_PER_SESSION
);

PG_FUNCTION_INFO_V1(test_backend_runtime_request_autovacuum_worker);
PG_FUNCTION_INFO_V1(test_backend_runtime_rejects_process_bgworker);

pg_noreturn PGDLLEXPORT void test_backend_runtime_unreachable_bgworker_main(Datum main_arg);

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
	BgwHandleStatus status = BGWH_NOT_YET_STARTED;
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
	worker.bgw_notify_pid = 0;

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
		elog(ERROR, "could not register process-model background worker");

	for (int i = 0; i < 50; i++)
	{
		CHECK_FOR_INTERRUPTS();

		status = GetBackgroundWorkerPid(handle, &pid);
		if (status != BGWH_NOT_YET_STARTED)
			break;

		pg_usleep(100000L);
	}

	if (status != BGWH_STOPPED)
	{
		if (status == BGWH_STARTED)
			TerminateBackgroundWorker(handle);
		elog(ERROR, "process-model background worker was not rejected: status %d",
			 status);
	}

	PG_RETURN_BOOL(true);
}

void
test_backend_runtime_unreachable_bgworker_main(Datum main_arg)
{
	elog(FATAL, "process-model background worker unexpectedly started");
	pg_unreachable();
}
