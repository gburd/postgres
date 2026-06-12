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
#include "storage/pmsignal.h"

PG_MODULE_MAGIC_EXT(
					.name = "test_backend_runtime_threaded",
					.version = PG_VERSION,
					PG_MODULE_MAGIC_BACKEND_MODEL_THREAD_PER_SESSION
);

PG_FUNCTION_INFO_V1(test_backend_runtime_request_autovacuum_worker);
Datum
test_backend_runtime_request_autovacuum_worker(PG_FUNCTION_ARGS)
{
	SendPostmasterSignal(PMSIGNAL_START_AUTOVAC_WORKER);

	PG_RETURN_BOOL(true);
}
