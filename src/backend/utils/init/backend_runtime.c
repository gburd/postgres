/*-------------------------------------------------------------------------
 *
 * backend_runtime.c
 *	  Runtime/backend/session scaffolding for backend execution.
 *
 * The process-mode implementation uses static per-process objects. Later
 * phases can replace or extend these objects without changing callers that
 * already refer to the current runtime/backend/session/execution.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/utils/init/backend_runtime.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"
#include "utils/backend_runtime.h"

PG_GLOBAL_RUNTIME PgRuntime *CurrentPgRuntime = NULL;
PG_GLOBAL_CARRIER PgCarrier *CurrentPgCarrier = NULL;
PG_GLOBAL_CARRIER PgBackend *CurrentPgBackend = NULL;
PG_GLOBAL_CARRIER PgSession *CurrentPgSession = NULL;
PG_GLOBAL_CARRIER PgConnection *CurrentPgConnection = NULL;
PG_GLOBAL_CARRIER PgExecution *CurrentPgExecution = NULL;

static PG_GLOBAL_RUNTIME PgRuntime process_runtime;
static PG_GLOBAL_CARRIER PgCarrier process_carrier;
static PG_GLOBAL_BACKEND PgBackend process_backend;
static PG_GLOBAL_SESSION PgSession process_session;
static PG_GLOBAL_CONNECTION PgConnection process_connection;
static PG_GLOBAL_EXECUTION PgExecution process_execution;

void
InitializePgProcessRuntime(void)
{
	MemSet(&process_runtime, 0, sizeof(process_runtime));
	MemSet(&process_carrier, 0, sizeof(process_carrier));
	MemSet(&process_backend, 0, sizeof(process_backend));
	MemSet(&process_session, 0, sizeof(process_session));
	MemSet(&process_connection, 0, sizeof(process_connection));
	MemSet(&process_execution, 0, sizeof(process_execution));

	process_runtime.kind = PG_RUNTIME_PROCESS;
	process_runtime.current_carrier = &process_carrier;

	process_carrier.kind = PG_CARRIER_PROCESS;
	process_carrier.runtime = &process_runtime;
	process_carrier.current_backend = &process_backend;
	process_carrier.current_session = &process_session;
	process_carrier.current_execution = &process_execution;

	process_backend.runtime = &process_runtime;
	process_backend.carrier = &process_carrier;
	process_backend.session = &process_session;
	process_backend.connection = &process_connection;
	process_backend.execution = &process_execution;
	process_backend.backend_type = MyBackendType;

	process_session.backend = &process_backend;
	process_session.connection = &process_connection;
	process_session.execution = &process_execution;

	process_connection.backend = &process_backend;
	process_connection.session = &process_session;
	process_connection.port = MyProcPort;

	process_execution.backend = &process_backend;
	process_execution.session = &process_session;
	process_execution.carrier = &process_carrier;

	CurrentPgRuntime = &process_runtime;
	CurrentPgCarrier = &process_carrier;
	CurrentPgBackend = &process_backend;
	CurrentPgSession = &process_session;
	CurrentPgConnection = &process_connection;
	CurrentPgExecution = &process_execution;
}

void
PgProcessRuntimeAttachSession(Session *session)
{
	Assert(CurrentPgSession != NULL);

	CurrentPgSession->legacy_session = session;
}
