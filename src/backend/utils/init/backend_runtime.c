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
#define BACKEND_RUNTIME_CURRENT_NO_BUCKET_ALIASES
#define BACKEND_RUNTIME_NO_INLINE_BUCKET_ACCESSORS
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

PG_THREAD_LOCAL PG_GLOBAL_CARRIER PgRuntimeCurrentBridge
			PgRuntimeCurrentBridgeState = {0};
PG_GLOBAL_RUNTIME int PgRuntimeHotCurrentCellModeState =
			PG_RUNTIME_HOT_CURRENT_CELLS_FALLBACK;
PG_THREAD_LOCAL PG_GLOBAL_CARRIER PgRuntimeBridgeFallbackStats
			PgRuntimeBridgeFallbackStatsState = {0};

PG_GLOBAL_RUNTIME PgRuntime **PgCurrentRuntimeHotRefProcessRef = NULL;
PG_THREAD_LOCAL PG_GLOBAL_CARRIER PgRuntime **PgCurrentRuntimeHotRefThreadRef = NULL;
PG_GLOBAL_RUNTIME PgCarrier **PgCurrentCarrierHotRefProcessRef = NULL;
PG_THREAD_LOCAL PG_GLOBAL_CARRIER PgCarrier **PgCurrentCarrierHotRefThreadRef = NULL;
PG_GLOBAL_RUNTIME PgBackend **PgCurrentBackendHotRefProcessRef = NULL;
PG_THREAD_LOCAL PG_GLOBAL_CARRIER PgBackend **PgCurrentBackendHotRefThreadRef = NULL;
PG_GLOBAL_RUNTIME PgSession **PgCurrentSessionHotRefProcessRef = NULL;
PG_THREAD_LOCAL PG_GLOBAL_CARRIER PgSession **PgCurrentSessionHotRefThreadRef = NULL;
PG_GLOBAL_RUNTIME PgConnection **PgCurrentConnectionHotRefProcessRef = NULL;
PG_THREAD_LOCAL PG_GLOBAL_CARRIER PgConnection **PgCurrentConnectionHotRefThreadRef = NULL;
PG_GLOBAL_RUNTIME PgExecution **PgCurrentExecutionHotRefProcessRef = NULL;
PG_THREAD_LOCAL PG_GLOBAL_CARRIER PgExecution **PgCurrentExecutionHotRefThreadRef = NULL;

#define PG_RUNTIME_HOT_CELL(variable, owner, owner_type, type, field) \
PG_GLOBAL_RUNTIME type *variable##ProcessCell = NULL; \
PG_THREAD_LOCAL PG_GLOBAL_CARRIER type *variable##ThreadCell = NULL;
#include "utils/backend_runtime_hot_cells.def"
#undef PG_RUNTIME_HOT_CELL

#define PG_RUNTIME_HOT_FIELD(variable, owner, type, expr) \
PG_GLOBAL_RUNTIME type *variable##ProcessRef = NULL; \
PG_GLOBAL_RUNTIME const void *variable##ProcessOwner = NULL; \
PG_THREAD_LOCAL PG_GLOBAL_CARRIER type *variable##ThreadRef = NULL; \
PG_THREAD_LOCAL PG_GLOBAL_CARRIER const void *variable##ThreadOwner = NULL;
#include "utils/backend_runtime_hot_fields.def"
#undef PG_RUNTIME_HOT_FIELD

static PG_GLOBAL_RUNTIME PgRuntime process_runtime;
static PG_GLOBAL_RUNTIME PgRuntime thread_runtime;
static PG_GLOBAL_RUNTIME bool thread_runtime_initialized = false;
static PG_GLOBAL_CARRIER PgCarrier process_carrier = {
	.wait_event_signal_fd = -1,
	.wait_event_selfpipe_readfd = -1,
	.wait_event_selfpipe_writefd = -1
};
static PG_GLOBAL_BACKEND PgBackend process_backend;
static PG_GLOBAL_SESSION PgSession process_session;
static PG_GLOBAL_CONNECTION PgConnection process_connection;
static PG_GLOBAL_EXECUTION PgExecution process_execution;

PgBackendPgStatPendingState *PgCurrentBackendPgStatPendingState(void);
PgBackendInstrumentationState *PgCurrentBackendInstrumentationState(void);
PgBackendTransactionState *PgCurrentBackendTransactionState(void);
PgBackendPendingInterruptState *PgCurrentPendingInterrupts(void);
PgBackendInterruptHoldoffState *PgCurrentInterruptHoldoffs(void);


static bool
PgRuntimeBridgeFallbackStatsRequested(void)
{
	const char *enabled;

	enabled = getenv("PG_RUNTIME_BRIDGE_FALLBACK_STATS");
	return enabled != NULL && enabled[0] != '\0' && enabled[0] != '0';
}

void
PgRuntimeReportBridgeFallbackStats(void)
{
	PgRuntimeBridgeFallbackStats *stats = &PgRuntimeBridgeFallbackStatsState;
	uint64		total;

	if (!PgRuntimeBridgeFallbackStatsRequested())
		return;

	total = stats->hot_cell +
		stats->hot_mirror +
		stats->hot_field +
		stats->hot_bucket +
		stats->fast_bucket +
		stats->fast_initialized_bucket +
		stats->carrier +
		stats->interrupts +
		stats->memory_contexts +
		stats->session_catalog_lookup +
		stats->after_triggers;

	ereport(LOG,
			(errmsg_internal("runtime bridge fallback stats: pid=%d mode=%d total=" UINT64_FORMAT
							 " hot_cell=" UINT64_FORMAT
							 " hot_mirror=" UINT64_FORMAT
							 " hot_field=" UINT64_FORMAT
							 " hot_bucket=" UINT64_FORMAT
							 " fast_bucket=" UINT64_FORMAT
							 " fast_initialized_bucket=" UINT64_FORMAT
							 " carrier=" UINT64_FORMAT
							 " interrupts=" UINT64_FORMAT
							 " memory_contexts=" UINT64_FORMAT
							 " session_catalog_lookup=" UINT64_FORMAT
							 " after_triggers=" UINT64_FORMAT,
							 MyProcPid,
							 PgRuntimeHotCurrentCellModeState,
							 total,
							 stats->hot_cell,
							 stats->hot_mirror,
							 stats->hot_field,
							 stats->hot_bucket,
							 stats->fast_bucket,
							 stats->fast_initialized_bucket,
							 stats->carrier,
							 stats->interrupts,
							 stats->memory_contexts,
							 stats->session_catalog_lookup,
							 stats->after_triggers)));

	MemSet(stats, 0, sizeof(*stats));
}


static void
PgRuntimeClearHotCurrentRootRefs(void)
{
	PgCurrentRuntimeHotRefProcessRef = NULL;
	PgCurrentRuntimeHotRefThreadRef = NULL;
	PgCurrentCarrierHotRefProcessRef = NULL;
	PgCurrentCarrierHotRefThreadRef = NULL;
	PgCurrentBackendHotRefProcessRef = NULL;
	PgCurrentBackendHotRefThreadRef = NULL;
	PgCurrentSessionHotRefProcessRef = NULL;
	PgCurrentSessionHotRefThreadRef = NULL;
	PgCurrentConnectionHotRefProcessRef = NULL;
	PgCurrentConnectionHotRefThreadRef = NULL;
	PgCurrentExecutionHotRefProcessRef = NULL;
	PgCurrentExecutionHotRefThreadRef = NULL;
}

static void
PgRuntimeLoadHotCurrentRootRefs(void)
{
	switch (PgRuntimeHotCurrentCellModeState)
	{
		case PG_RUNTIME_HOT_CURRENT_CELLS_PROCESS:
			PgCurrentRuntimeHotRefProcessRef =
				&PgRuntimeCurrentBridgeState.runtime;
			PgCurrentCarrierHotRefProcessRef =
				&PgRuntimeCurrentBridgeState.carrier;
			PgCurrentBackendHotRefProcessRef =
				&PgRuntimeCurrentBridgeState.backend;
			PgCurrentSessionHotRefProcessRef =
				&PgRuntimeCurrentBridgeState.session;
			PgCurrentConnectionHotRefProcessRef =
				&PgRuntimeCurrentBridgeState.connection;
			PgCurrentExecutionHotRefProcessRef =
				&PgRuntimeCurrentBridgeState.execution;
			break;

		case PG_RUNTIME_HOT_CURRENT_CELLS_THREAD:
			PgCurrentRuntimeHotRefThreadRef =
				&PgRuntimeCurrentBridgeState.runtime;
			PgCurrentCarrierHotRefThreadRef =
				&PgRuntimeCurrentBridgeState.carrier;
			PgCurrentBackendHotRefThreadRef =
				&PgRuntimeCurrentBridgeState.backend;
			PgCurrentSessionHotRefThreadRef =
				&PgRuntimeCurrentBridgeState.session;
			PgCurrentConnectionHotRefThreadRef =
				&PgRuntimeCurrentBridgeState.connection;
			PgCurrentExecutionHotRefThreadRef =
				&PgRuntimeCurrentBridgeState.execution;
			break;

		case PG_RUNTIME_HOT_CURRENT_CELLS_FALLBACK:
			PgRuntimeClearHotCurrentRootRefs();
			break;
	}
}

static void
PgRuntimeClearHotBucketPointers(void)
{
#define PG_RUNTIME_HOT_BUCKET(variable, type, owner, field) \
	do { \
		PgRuntimeCurrentBridgeState.variable = NULL; \
	} while (0);
#include "utils/backend_runtime_hot_buckets.def"
#undef PG_RUNTIME_HOT_BUCKET
}

void
PgRuntimeFlushCurrentHotCells(void)
{
	/*
	 * Hot current cells cache addresses into the active runtime object.
	 * Direct compatibility-lvalue writes update the runtime owner field.
	 */
}

static void
PgRuntimeLoadHotCurrentCells(void)
{
#define PG_RUNTIME_HOT_CELL(variable, owner, owner_type, type, field) \
	do { \
		PgRuntimeCurrentBridgeState.variable = \
			PgRuntimeCurrentBridgeState.owner != NULL ? \
			&PgRuntimeCurrentBridgeState.owner->field : NULL; \
	} while (0);
#include "utils/backend_runtime_hot_cells.def"
#undef PG_RUNTIME_HOT_CELL

	switch (PgRuntimeHotCurrentCellModeState)
	{
		case PG_RUNTIME_HOT_CURRENT_CELLS_PROCESS:
#define PG_RUNTIME_HOT_CELL(variable, owner, owner_type, type, field) \
			do { \
				variable##ProcessCell = \
					PgRuntimeCurrentBridgeState.owner != NULL ? \
					&PgRuntimeCurrentBridgeState.owner->field : NULL; \
			} while (0);
#include "utils/backend_runtime_hot_cells.def"
#undef PG_RUNTIME_HOT_CELL
			break;

		case PG_RUNTIME_HOT_CURRENT_CELLS_THREAD:
#define PG_RUNTIME_HOT_CELL(variable, owner, owner_type, type, field) \
			do { \
				variable##ThreadCell = \
					PgRuntimeCurrentBridgeState.owner != NULL ? \
					&PgRuntimeCurrentBridgeState.owner->field : NULL; \
			} while (0);
#include "utils/backend_runtime_hot_cells.def"
#undef PG_RUNTIME_HOT_CELL
			break;

		case PG_RUNTIME_HOT_CURRENT_CELLS_FALLBACK:
			break;
	}
}

static void
PgRuntimeClearHotCurrentCells(void)
{
#define PG_RUNTIME_HOT_CELL(variable, owner, owner_type, type, field) \
	do { \
		PgRuntimeCurrentBridgeState.variable = NULL; \
		variable##ProcessCell = NULL; \
		variable##ThreadCell = NULL; \
	} while (0);
#include "utils/backend_runtime_hot_cells.def"
#undef PG_RUNTIME_HOT_CELL
}

static void
PgRuntimeInstallHotCurrentCells(PgRuntimeHotCurrentCellMode mode)
{
	PgRuntimeHotCurrentCellModeState = mode;
	PgRuntimeLoadHotCurrentRootRefs();
	PgRuntimeLoadHotCurrentCells();
}

static void
PgRuntimeInstallHotCurrentCellsForCurrentWork(void)
{
	PgBackend  *backend = PgRuntimeCurrentBridgeState.backend;

#define PG_RUNTIME_HOT_CELL(variable, owner, owner_type, type, field) \
	do { \
		if (PgRuntimeCurrentBridgeState.owner == NULL) \
		{ \
			PgRuntimeInstallHotCurrentCells(PG_RUNTIME_HOT_CURRENT_CELLS_FALLBACK); \
			return; \
		} \
	} while (0);
#include "utils/backend_runtime_hot_cells.def"
#undef PG_RUNTIME_HOT_CELL

	/*
	 * This code is installing the compatibility accessors, so do not call
	 * IsBootstrapProcessingMode() here: Mode itself is a compatibility
	 * accessor and may still resolve through early fallback state.
	 */
	if (backend != NULL && backend->core.mode == BootstrapProcessing)
	{
		PgRuntimeInstallHotCurrentCells(PG_RUNTIME_HOT_CURRENT_CELLS_FALLBACK);
		return;
	}

	if (!IsUnderPostmaster || MyBackendType != B_BACKEND)
	{
		PgRuntimeInstallHotCurrentCells(PG_RUNTIME_HOT_CURRENT_CELLS_FALLBACK);
		return;
	}

	if (CurrentPgCarrier != NULL &&
		CurrentPgCarrier->kind == PG_CARRIER_THREAD)
		PgRuntimeInstallHotCurrentCells(PG_RUNTIME_HOT_CURRENT_CELLS_THREAD);
	else if (CurrentPgRuntime != NULL)
		PgRuntimeInstallHotCurrentCells(PG_RUNTIME_HOT_CURRENT_CELLS_PROCESS);
	else
		PgRuntimeInstallHotCurrentCells(PG_RUNTIME_HOT_CURRENT_CELLS_FALLBACK);
}

void
PgRuntimeReloadCurrentHotCells(void)
{
	PgRuntimeLoadHotCurrentCells();
}

void
PgRuntimeFlushCurrentHotMirrors(void)
{
#define PG_RUNTIME_HOT_MIRROR(variable, owner, owner_type, type, field) \
	do { \
		if (PgRuntimeCurrentBridgeState.variable##Owner != NULL && \
			PgRuntimeCurrentBridgeState.variable##Owner == \
			(const void *) PgRuntimeCurrentBridgeState.owner) \
			((owner_type *) PgRuntimeCurrentBridgeState.variable##Owner)->field = \
				PgRuntimeCurrentBridgeState.variable; \
	} while (0);
#include "utils/backend_runtime_hot_mirrors.def"
#undef PG_RUNTIME_HOT_MIRROR
}

static void
PgRuntimeLoadHotMirrorValues(void)
{
#define PG_RUNTIME_HOT_MIRROR(variable, owner, owner_type, type, field) \
	do { \
		if (PgRuntimeCurrentBridgeState.owner != NULL) \
		{ \
			PgRuntimeCurrentBridgeState.variable = \
				PgRuntimeCurrentBridgeState.owner->field; \
			PgRuntimeCurrentBridgeState.variable##Owner = \
				PgRuntimeCurrentBridgeState.owner; \
		} \
		else \
		{ \
			PgRuntimeCurrentBridgeState.variable = (type) 0; \
			PgRuntimeCurrentBridgeState.variable##Owner = NULL; \
		} \
	} while (0);
#include "utils/backend_runtime_hot_mirrors.def"
#undef PG_RUNTIME_HOT_MIRROR
}

void
PgRuntimeReloadCurrentHotMirrors(void)
{
	PgRuntimeLoadHotMirrorValues();
}

static void
PgRuntimeClearHotMirrorValues(void)
{
#define PG_RUNTIME_HOT_MIRROR(variable, owner, owner_type, type, field) \
	do { \
		PgRuntimeCurrentBridgeState.variable = NULL; \
		PgRuntimeCurrentBridgeState.variable##Owner = NULL; \
	} while (0);
#include "utils/backend_runtime_hot_mirrors.def"
#undef PG_RUNTIME_HOT_MIRROR
}

static void
PgRuntimeClearHotFieldPointers(void)
{
#define PG_RUNTIME_HOT_FIELD(variable, owner, type, expr) \
	do { \
		PgRuntimeCurrentBridgeState.variable = NULL; \
		PgRuntimeCurrentBridgeState.variable##Owner = NULL; \
		variable##ProcessRef = NULL; \
		variable##ProcessOwner = NULL; \
		variable##ThreadRef = NULL; \
		variable##ThreadOwner = NULL; \
	} while (0);
#include "utils/backend_runtime_hot_fields.def"
#undef PG_RUNTIME_HOT_FIELD
}

static void
PgRuntimeInstallHotBucketPointers(PgBackend *backend, PgSession *session,
								  PgConnection *connection,
								  PgExecution *execution)
{
	/*
	 * These pointers are only a cache of the current runtime work.  A future
	 * pooled scheduler must call this whenever it switches carrier work.
	 */
#define PG_RUNTIME_HOT_BUCKET(variable, type, owner, field) \
	do { \
		type	   *bucket = (owner != NULL) ? &owner->field : NULL; \
 \
		PgRuntimeCurrentBridgeState.variable = bucket; \
	} while (0);
#include "utils/backend_runtime_hot_buckets.def"
#undef PG_RUNTIME_HOT_BUCKET
}

static void
PgRuntimeInstallHotMirrorValues(void)
{
	PgRuntimeLoadHotMirrorValues();
}

static void
PgRuntimeInstallHotFieldPointers(void)
{
	/*
	 * Derived slots carry an owner token.  Inline hot paths must compare the
	 * token with the current owner before using the slot.
	 */
#define PG_RUNTIME_HOT_FIELD(variable, owner, type, expr) \
	do { \
		type	   *slot = (expr); \
		const void *slot_owner = PgRuntimeCurrentBridgeState.owner; \
 \
		PgRuntimeCurrentBridgeState.variable = slot; \
		PgRuntimeCurrentBridgeState.variable##Owner = slot_owner; \
		switch (PgRuntimeHotCurrentCellModeState) \
		{ \
			case PG_RUNTIME_HOT_CURRENT_CELLS_PROCESS: \
				variable##ProcessRef = slot; \
				variable##ProcessOwner = slot_owner; \
				break; \
			case PG_RUNTIME_HOT_CURRENT_CELLS_THREAD: \
				variable##ThreadRef = slot; \
				variable##ThreadOwner = slot_owner; \
				break; \
			case PG_RUNTIME_HOT_CURRENT_CELLS_FALLBACK: \
				break; \
		} \
	} while (0);
#include "utils/backend_runtime_hot_fields.def"
#undef PG_RUNTIME_HOT_FIELD
}

static void
PgRuntimeRefreshCurrentWork(bool rebind_session_gucs)
{
	PgRuntimeInstallHotCurrentCellsForCurrentWork();
	PgRuntimeInstallHotBucketPointers(CurrentPgBackend, CurrentPgSession,
									  CurrentPgConnection,
									  CurrentPgExecution);
	PgRuntimeInstallHotMirrorValues();
	PgRuntimeInstallHotFieldPointers();
	if (rebind_session_gucs && CurrentPgSession != NULL)
		RebindSessionGUCVariablePointers();
}

void
PgRuntimeAfterProcessingModeChange(ProcessingMode mode)
{
	/*
	 * Processing mode is part of the current-work contract.  In particular,
	 * bootstrap must not keep process-fast compatibility slots that were
	 * installed before Mode became BootstrapProcessing.
	 */
	PgRuntimeRefreshCurrentWork(false);
}

static void
PgRuntimeSetCurrentWork(PgRuntime *runtime, PgCarrier *carrier,
						PgBackend *backend, PgSession *session,
						PgConnection *connection, PgExecution *execution,
						bool rebind_session_gucs)
{
	PgRuntimeFlushCurrentHotCells();
	PgRuntimeFlushCurrentHotMirrors();
	CurrentPgRuntime = runtime;
	CurrentPgCarrier = carrier;
	CurrentPgBackend = backend;
	CurrentPgSession = session;
	CurrentPgConnection = connection;
	CurrentPgExecution = execution;
	PgRuntimeRefreshCurrentWork(rebind_session_gucs);
}

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

	PgRuntimeFlushCurrentHotCells();
	PgRuntimeFlushCurrentHotMirrors();
	PgRuntimeSetCurrentWork(NULL, NULL, NULL, NULL, NULL, NULL, false);
	PgRuntimeHotCurrentCellModeState = PG_RUNTIME_HOT_CURRENT_CELLS_FALLBACK;
	PgRuntimeClearHotCurrentRootRefs();
	PgRuntimeClearHotBucketPointers();
	PgRuntimeClearHotCurrentCells();
	PgRuntimeClearHotMirrorValues();
	PgRuntimeClearHotFieldPointers();

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
	int			wait_event_signal_fd = process_carrier.wait_event_signal_fd;
	int			wait_event_selfpipe_readfd =
		process_carrier.wait_event_selfpipe_readfd;
	int			wait_event_selfpipe_writefd =
		process_carrier.wait_event_selfpipe_writefd;
	int			wait_event_selfpipe_owner_pid =
		process_carrier.wait_event_selfpipe_owner_pid;

	MemSet(&process_runtime, 0, sizeof(process_runtime));
	PgRuntimeInitializeRuntimeObject(&process_runtime);
	PgCarrierInitializeRuntimeObject(&process_carrier);

	/*
	 * InitPostmasterChild() and InitStandaloneProcess() initialize wait-event
	 * support before BaseInit() installs the process runtime.  Preserve those
	 * child-local descriptors here; PgRuntimeResetAfterFork() still clears any
	 * inherited postmaster descriptors before wait-event support is recreated.
	 */
	process_carrier.wait_event_signal_fd = wait_event_signal_fd;
	process_carrier.wait_event_selfpipe_readfd = wait_event_selfpipe_readfd;
	process_carrier.wait_event_selfpipe_writefd = wait_event_selfpipe_writefd;
	process_carrier.wait_event_selfpipe_owner_pid = wait_event_selfpipe_owner_pid;

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

	PgRuntimeSetCurrentWork(&process_runtime, &process_carrier,
							&process_backend, &process_session,
							&process_connection, &process_execution, true);

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
	PgRuntimeSetCurrentWork(&thread_runtime, &state->carrier, &state->backend,
							&state->session, &state->connection,
							&state->execution, true);
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
PgSetCurrentRuntime(PgRuntime *runtime)
{
	PgRuntimeFlushCurrentHotCells();
	PgRuntimeFlushCurrentHotMirrors();
	CurrentPgRuntime = runtime;
	PgRuntimeRefreshCurrentWork(false);
}

void
PgSetCurrentCarrier(PgCarrier *carrier)
{
	PgRuntimeFlushCurrentHotCells();
	PgRuntimeFlushCurrentHotMirrors();
	CurrentPgCarrier = carrier;
	PgRuntimeRefreshCurrentWork(false);
}

void
PgSetCurrentBackend(PgBackend *backend)
{
	PgRuntimeFlushCurrentHotCells();
	PgRuntimeFlushCurrentHotMirrors();
	CurrentPgBackend = backend;
	PgRuntimeRefreshCurrentWork(false);
}

void
PgSetCurrentSession(PgSession *session)
{
	PgRuntimeFlushCurrentHotCells();
	PgRuntimeFlushCurrentHotMirrors();
	CurrentPgSession = session;
	PgRuntimeRefreshCurrentWork(false);
}

void
PgSetCurrentConnection(PgConnection *connection)
{
	PgRuntimeFlushCurrentHotCells();
	PgRuntimeFlushCurrentHotMirrors();
	CurrentPgConnection = connection;
	PgRuntimeRefreshCurrentWork(false);
}

void
PgSetCurrentExecution(PgExecution *execution)
{
	PgRuntimeFlushCurrentHotCells();
	PgRuntimeFlushCurrentHotMirrors();
	CurrentPgExecution = execution;
	PgRuntimeRefreshCurrentWork(false);
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
