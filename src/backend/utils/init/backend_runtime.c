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

#include <unistd.h>

#include "access/gin.h"
#include "access/parallel.h"
#include "access/session.h"
#include "access/syncscan.h"
#include "access/tableam.h"
#include "access/toast_compression.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xlogreader.h"
#include "archive/archive_module.h"
#include "catalog/binary_upgrade.h"
#include "catalog/storage.h"
#include "commands/async.h"
#include "commands/event_trigger.h"
#include "commands/extension.h"
#include "commands/explain_state.h"
#include "commands/prepare.h"
#include "commands/repack.h"
#include "commands/tablespace.h"
#include "commands/trigger.h"
#include "commands/vacuum.h"
#include "executor/spi.h"
#include "jit/jit.h"
#include "lib/dshash.h"
#include "libpq/crypt.h"
#include "miscadmin.h"
#include "nodes/queryjumble.h"
#include "optimizer/cost.h"
#include "optimizer/geqo.h"
#include "optimizer/optimizer.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "parser/parser.h"
#include "parser/parse_expr.h"
#include "postmaster/pgarch.h"
#include "postmaster/interrupt.h"
#include "regex/regex.h"
#include "replication/logical.h"
#include "replication/reorderbuffer.h"
#include "replication/logicalworker.h"
#include "replication/slotsync.h"
#include "replication/walreceiver.h"
#include "storage/bufmgr.h"
#include "storage/buf_internals.h"
#include "storage/buffile.h"
#include "storage/copydir.h"
#include "storage/dsm.h"
#include "storage/fd.h"
#include "storage/latch.h"
#include "storage/large_object.h"
#include "storage/lock.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "storage/sinval.h"
#include "tsearch/ts_cache.h"
#include "utils/backend_runtime.h"
#include "backend_runtime_internal.h"
#include "utils/builtins.h"
#include "utils/bytea.h"
#include "utils/dsa.h"
#include "utils/elog.h"
#include "utils/float.h"
#include "utils/funccache.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/pgstat_internal.h"
#include "utils/plancache.h"
#include "utils/ps_status.h"
#include "utils/resowner.h"
#include "utils/rls.h"
#include "utils/typcache.h"
#include "utils/xml.h"

PG_THREAD_LOCAL PG_GLOBAL_CARRIER PgRuntime *CurrentPgRuntime = NULL;
PG_THREAD_LOCAL PG_GLOBAL_CARRIER PgCarrier *CurrentPgCarrier = NULL;
PG_THREAD_LOCAL PG_GLOBAL_CARRIER PgBackend *CurrentPgBackend = NULL;
PG_THREAD_LOCAL PG_GLOBAL_CARRIER PgSession *CurrentPgSession = NULL;
PG_THREAD_LOCAL PG_GLOBAL_CARRIER PgConnection *CurrentPgConnection = NULL;
PG_THREAD_LOCAL PG_GLOBAL_CARRIER PgExecution *CurrentPgExecution = NULL;

static PG_GLOBAL_RUNTIME PgRuntime process_runtime;
static PG_GLOBAL_RUNTIME PgRuntime thread_runtime;
static PG_GLOBAL_RUNTIME bool thread_runtime_initialized = false;
static PG_GLOBAL_CARRIER PgCarrier process_carrier;
static PG_GLOBAL_BACKEND PgBackend process_backend;
static PG_GLOBAL_SESSION PgSession process_session;
static PG_GLOBAL_CONNECTION PgConnection process_connection;
static PG_GLOBAL_EXECUTION PgExecution process_execution;

PgBackendPgStatPendingState *PgCurrentBackendPgStatPendingState(void);
PgBackendInstrumentationState *PgCurrentBackendInstrumentationState(void);
PgBackendTransactionState *PgCurrentBackendTransactionState(void);
PgBackendPendingInterruptState *PgCurrentPendingInterrupts(void);
PgBackendInterruptHoldoffState *PgCurrentInterruptHoldoffs(void);



static void
PgRuntimeInitializeRuntimeObject(PgRuntime *runtime)
{
	Assert(runtime != NULL);

#define PG_RUNTIME_BUCKET(field, init, adopt, reset) \
	do { init; } while (0);
#include "backend_runtime_runtime_buckets.def"
#undef PG_RUNTIME_BUCKET
}



static void
PgCarrierInitializeRuntimeObject(PgCarrier *carrier)
{
	bool		is_under_postmaster;

	is_under_postmaster = carrier->is_under_postmaster;
	MemSet(carrier, 0, sizeof(*carrier));
	carrier->is_under_postmaster = is_under_postmaster;

#define PG_CARRIER_BUCKET(field, init, adopt, reset) \
	do { init; } while (0);
#include "backend_runtime_carrier_buckets.def"
#undef PG_CARRIER_BUCKET
}

void
PgRuntimeResetAfterFork(void)
{
	PgBackendResetDsmStateAfterFork();

	CurrentPgRuntime = NULL;
	CurrentPgCarrier = NULL;
	CurrentPgBackend = NULL;
	CurrentPgSession = NULL;
	CurrentPgConnection = NULL;
	CurrentPgExecution = NULL;

	MemSet(&process_runtime, 0, sizeof(process_runtime));
	PgRuntimeInitializeRuntimeObject(&process_runtime);
	PgCarrierInitializeRuntimeObject(&process_carrier);
	PgBackendInitializeRuntimeObject(&process_backend, NULL, NULL, NULL,
									 NULL, NULL, B_INVALID, NULL);
	PgSessionInitializeRuntimeObject(&process_session, NULL, NULL, NULL);
	PgConnectionInitializeRuntimeObject(&process_connection, NULL, NULL,
										NULL);
	PgExecutionInitializeRuntimeObject(&process_execution, NULL, NULL, NULL);

	PgBackendResetEarlyFallbackAfterFork((int) getpid());
}

void
InitializePgProcessRuntime(void)
{
	MemSet(&process_runtime, 0, sizeof(process_runtime));
	PgRuntimeInitializeRuntimeObject(&process_runtime);
	PgCarrierInitializeRuntimeObject(&process_carrier);
	MemSet(&process_backend, 0, sizeof(process_backend));
	MemSet(&process_session, 0, sizeof(process_session));
	MemSet(&process_connection, 0, sizeof(process_connection));
	MemSet(&process_execution, 0, sizeof(process_execution));

	process_runtime.kind = PG_RUNTIME_PROCESS;
	process_runtime.current_carrier = &process_carrier;
	process_runtime.extension_backend_model = PG_BACKEND_MODEL_PROCESS;
	PgRuntimeAdoptEarlyServerGUCState(&process_runtime);
	PgRuntimeAdoptEarlyExtensionModuleState(&process_runtime);

	process_carrier.kind = PG_CARRIER_PROCESS;
	process_carrier.runtime = &process_runtime;
	process_carrier.current_backend = &process_backend;
	process_carrier.current_session = &process_session;
	process_carrier.current_execution = &process_execution;

	PgBackendInitializeRuntimeObject(&process_backend, &process_runtime,
									 &process_carrier, &process_session,
									 &process_connection, &process_execution,
									 MyBackendType, NULL);
	PgBackendAdoptEarlyState(&process_backend);
	PgBackendSetInterruptLatch(&process_backend, process_backend.core.latch);
	PgBackendAdoptEarlyExitState(&process_backend.exit_state);

	PgSessionInitializeRuntimeObject(&process_session, &process_backend,
									 &process_connection, &process_execution);
	PgSessionAdoptEarlyState(&process_session);

	PgConnectionInitializeRuntimeObject(&process_connection, &process_backend,
										&process_session, NULL);
	PgConnectionAdoptEarlyState(&process_connection, NULL);

	PgExecutionInitializeRuntimeObject(&process_execution, &process_backend,
									   &process_session, &process_carrier);
	PgExecutionAdoptEarlyState(&process_execution);

	CurrentPgRuntime = &process_runtime;
	CurrentPgCarrier = &process_carrier;
	CurrentPgBackend = &process_backend;
	CurrentPgConnection = &process_connection;
	CurrentPgExecution = &process_execution;
	PgSetCurrentSession(&process_session);

	if (MyProc != NULL && MyProc->backendId == 0)
		MyProc->backendId = process_backend.id;
}

void
InitializePgThreadRuntime(PgBackendExitContinuation exit_backend)
{
	if (!thread_runtime_initialized)
	{
		PgRuntimeServerGUCState *early_server_guc;

		MemSet(&thread_runtime, 0, sizeof(thread_runtime));
		PgRuntimeInitializeRuntimeObject(&thread_runtime);

		thread_runtime.kind = PG_RUNTIME_THREAD_PER_SESSION;
		thread_runtime.extension_backend_model =
			PG_BACKEND_MODEL_THREAD_PER_SESSION;
		early_server_guc = PgEarlyRuntimeServerGUCState();
		if (PgRuntimeServerGUCStateHasConfigPaths(&process_runtime.server_guc))
			thread_runtime.server_guc = process_runtime.server_guc;
		else if (PgRuntimeServerGUCStateHasConfigPaths(early_server_guc))
			thread_runtime.server_guc = *early_server_guc;
		else if (process_runtime.server_guc.initialized)
			thread_runtime.server_guc = process_runtime.server_guc;
		else
			PgRuntimeInitializeServerGUCState(&thread_runtime.server_guc);
		thread_runtime.extension_modules = process_runtime.extension_modules;
		PgRuntimeEnsureExtensionModuleMemoryContext(&thread_runtime.extension_modules);
		PgBackendInitializeIdCounter();
		thread_runtime_initialized = true;
	}

	thread_runtime.exit_backend = exit_backend;
}

void
InitializePgThreadBackendRuntimeState(PgThreadBackendRuntimeState *state,
									  BackendType backend_type,
									  struct Port *port,
									  struct Latch *interrupt_latch)
{
	Assert(state != NULL);
	Assert(thread_runtime_initialized);

	MemSet(state, 0, sizeof(*state));
	PgCarrierInitializeRuntimeObject(&state->carrier);

	state->carrier.kind = PG_CARRIER_THREAD;
	state->carrier.runtime = &thread_runtime;
	state->carrier.current_backend = &state->backend;
	state->carrier.current_session = &state->session;
	state->carrier.current_execution = &state->execution;

	PgBackendInitializeRuntimeObject(&state->backend, &thread_runtime,
									 &state->carrier, &state->session,
									 &state->connection, &state->execution,
									 backend_type, interrupt_latch);
	PgSessionInitializeRuntimeObject(&state->session, &state->backend,
									 &state->connection, &state->execution);
	PgConnectionInitializeRuntimeObject(&state->connection, &state->backend,
										&state->session, port);
	PgExecutionInitializeRuntimeObject(&state->execution, &state->backend,
									   &state->session, &state->carrier);
}

void
InstallPgThreadBackendRuntimeState(PgThreadBackendRuntimeState *state)
{
	Assert(state != NULL);

	state->carrier.current_backend = &state->backend;
	state->carrier.current_session = &state->session;
	state->carrier.current_execution = &state->execution;
	PgBackendAdoptEarlyState(&state->backend);
	PgSessionAdoptEarlyState(&state->session);
	PgConnectionAdoptEarlyState(&state->connection,
								state->connection.identity.port);
	PgExecutionAdoptEarlyState(&state->execution);
	CurrentPgRuntime = &thread_runtime;
	CurrentPgCarrier = &state->carrier;
	CurrentPgBackend = &state->backend;
	CurrentPgConnection = &state->connection;
	CurrentPgExecution = &state->execution;
	PgSetCurrentSession(&state->session);
	InitializeThreadedSessionRequiredGUCOptions();
}

void
InitializePgThreadBackendRuntime(PgThreadBackendRuntimeState *state,
								 BackendType backend_type,
								 struct Port *port,
								 struct Latch *interrupt_latch)
{
	InitializePgThreadBackendRuntimeState(state, backend_type, port,
										  interrupt_latch);
	InstallPgThreadBackendRuntimeState(state);
}

void
PgSetCurrentSession(PgSession *session)
{
	CurrentPgSession = session;
	if (CurrentPgSession != NULL)
		RebindSessionGUCVariablePointers();
}

PgSession *
PgProcessSessionState(void)
{
	return &process_session;
}


void **
PgCurrentBackendThreadStartRef(void)
{
	if (CurrentPgCarrier != NULL)
		return &CurrentPgCarrier->backend_thread_start;

	return &process_carrier.backend_thread_start;
}

bool *
PgCurrentIsUnderPostmasterRef(void)
{
	return &PgCurrentCarrierState()->is_under_postmaster;
}

PgCarrier *
PgCurrentCarrierState(void)
{
	if (CurrentPgCarrier == NULL)
		return &process_carrier;

	return CurrentPgCarrier;
}


PgBackendLaunchModel
PgRuntimeGetBackendLaunchModel(BackendType backend_type)
{
	if (PgRuntimeShouldThreadBackend(backend_type))
		return PG_BACKEND_LAUNCH_THREAD;

	return PG_BACKEND_LAUNCH_PROCESS;
}

bool
PgRuntimeShouldThreadBackend(BackendType backend_type)
{
	if (!multithreaded)
		return false;

	/*
	 * Phase 10 is scoped to regular client backends.  Phase 11 incrementally
	 * moves in-tree server-owned worker families onto the same carrier
	 * infrastructure as they get dedicated signal and lifecycle conversion.
	 */
	return backend_type == B_BACKEND ||
		backend_type == B_ARCHIVER ||
		backend_type == B_AUTOVAC_LAUNCHER ||
		backend_type == B_AUTOVAC_WORKER ||
		backend_type == B_BG_WRITER ||
		backend_type == B_CHECKPOINTER ||
		backend_type == B_LOGGER ||
		backend_type == B_STARTUP ||
		backend_type == B_WAL_RECEIVER ||
		backend_type == B_SLOTSYNC_WORKER ||
		backend_type == B_WAL_WRITER ||
		backend_type == B_WAL_SUMMARIZER;
}

PgBackendModel
PgRuntimeGetExtensionBackendModel(void)
{
	if (CurrentPgRuntime == NULL)
		return PG_BACKEND_MODEL_PROCESS;

	return CurrentPgRuntime->extension_backend_model;
}

void
PgRuntimeSetExtensionBackendModel(PgBackendModel backend_model)
{
	if (backend_model < PG_BACKEND_MODEL_PROCESS ||
		backend_model > PG_BACKEND_MODEL_POOLED_SCHEDULER)
		elog(ERROR, "invalid backend model: %d", backend_model);

	if (CurrentPgRuntime == NULL)
		return;

	check_loaded_modules_backend_model(backend_model);
	CurrentPgRuntime->extension_backend_model = backend_model;
}
