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

#if defined(__GLIBC__)
#include <malloc.h>
#endif
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
#include "libpq/libpq.h"
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
static PG_GLOBAL_RUNTIME MemoryContext thread_runtime_server_guc_context = NULL;
static PG_GLOBAL_CARRIER PgCarrier process_carrier = {
	.wait_event_signal_fd = -1,
	.wait_event_selfpipe_readfd = -1,
	.wait_event_selfpipe_writefd = -1
};
static PG_GLOBAL_BACKEND PgBackend process_backend;
static PG_GLOBAL_SESSION PgSession process_session;
static PG_GLOBAL_CONNECTION PgConnection process_connection;
static PG_GLOBAL_EXECUTION PgExecution process_execution;

static MemoryContext PgRuntimeThreadServerGUCContext(void);
static char *PgRuntimeCopyThreadServerGUCString(char *current,
												const char *source);
static bool PgRuntimeThreadServerGUCStringIsOwned(char *value);
static void PgRuntimeCopyThreadServerGUCState(const PgRuntimeServerGUCState *source);
static void PgRuntimeRefreshThreadServerGUCState(void);
static void PgRuntimeConfigureThreadedAllocator(bool pooled_protocol);

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

void
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

/*
 * xtc-carrier: snapshot / restore the calling thread's current-work pointers.
 *
 * On the xtc carrier, many backend fibers time-share ONE OS thread, but PG's
 * "current work" (CurrentPgRuntime/Carrier/Backend/Session/Connection/
 * Execution) lives in ordinary thread-locals that the coroutine switch does
 * NOT swap.  When a fiber yields at a socket/latch wait and the loop resumes a
 * different fiber, that fiber must see ITS OWN current work, not the yielding
 * fiber's.  The wait seam snapshots current work before parking the fiber and
 * restores it on resume, making the current-work TLS effectively fiber-local
 * across the one cooperative yield point a backend fiber has.
 */
void
PgRuntimeSaveCurrentWork(PgCurrentWorkSnapshot *snap)
{
	Assert(snap != NULL);
	snap->runtime = CurrentPgRuntime;
	snap->carrier = CurrentPgCarrier;
	snap->backend = CurrentPgBackend;
	snap->session = CurrentPgSession;
	snap->connection = CurrentPgConnection;
	snap->execution = CurrentPgExecution;
}

void
PgRuntimeRestoreCurrentWork(const PgCurrentWorkSnapshot *snap)
{
	Assert(snap != NULL);
	PgRuntimeSetCurrentWork(snap->runtime, snap->carrier, snap->backend,
							snap->session, snap->connection, snap->execution,
							true);
}

void
PgCarrierAttachBackend(PgCarrier *carrier, PgBackend *backend,
					   PgSession *session, PgConnection *connection,
					   PgExecution *execution)
{
	PgRuntime  *runtime;

	Assert(carrier != NULL);
	Assert(backend != NULL);
	Assert(session != NULL);
	Assert(connection != NULL);
	Assert(execution != NULL);
	Assert(backend->session == NULL || backend->session == session);
	Assert(backend->connection == NULL || backend->connection == connection);
	Assert(backend->execution == NULL || backend->execution == execution);
	Assert(session->backend == NULL || session->backend == backend);
	Assert(session->connection == NULL || session->connection == connection);
	Assert(session->execution == NULL || session->execution == execution);
	Assert(connection->backend == NULL || connection->backend == backend);
	Assert(connection->session == NULL || connection->session == session);
	Assert(execution->backend == NULL || execution->backend == backend);
	Assert(execution->session == NULL || execution->session == session);

	runtime = carrier->runtime;
	if (runtime == NULL)
		runtime = backend->runtime;
	Assert(runtime != NULL);
	Assert(backend->runtime == NULL || backend->runtime == runtime);

	runtime->current_carrier = carrier;
	carrier->runtime = runtime;
	carrier->current_backend = backend;
	carrier->current_session = session;
	carrier->current_execution = execution;
	backend->runtime = runtime;
	backend->carrier = carrier;
	backend->session = session;
	backend->connection = connection;
	backend->execution = execution;
	session->backend = backend;
	session->connection = connection;
	session->execution = execution;
	connection->backend = backend;
	connection->session = session;
	execution->backend = backend;
	execution->session = session;
	execution->carrier = carrier;
	PgRuntimeProtocolSchedulerCarrierBecameActive(carrier);

	PgRuntimeSetCurrentWork(runtime, carrier, backend, session, connection,
							execution, true);
	if (PgRuntimeIsPooledProtocol(runtime) &&
		backend->my_proc != NULL &&
		backend->core.latch == &backend->my_proc->procLatch)
	{
		ReownLatchCurrentThread(backend->core.latch);
		RefreshLatchWaitSetCurrentCarrier();
		if (FeBeWaitSet != NULL)
			ModifyWaitEvent(FeBeWaitSet, FeBeWaitSetLatchPos, WL_LATCH_SET,
							MyLatch);
	}
	if (PgRuntimeIsPooledProtocol(runtime))
		RestoreBufferManagerIdleMemory();
}

void
PgCarrierDetachBackend(PgCarrier *carrier, PgBackend *backend)
{
	PgRuntime  *runtime;
	PgExecution *execution;

	Assert(carrier != NULL);
	Assert(backend == NULL || carrier->current_backend == backend);

	runtime = carrier->runtime;
	Assert(runtime != NULL);

	if (backend == NULL)
		backend = carrier->current_backend;
	execution = carrier->current_execution;

	carrier->current_backend = NULL;
	carrier->current_session = NULL;
	carrier->current_execution = NULL;
	if (backend != NULL && backend->carrier == carrier)
		backend->carrier = NULL;
	if (execution != NULL && execution->carrier == carrier)
		execution->carrier = NULL;
	PgRuntimeProtocolSchedulerCarrierBecameIdle(carrier);

	if (CurrentPgCarrier == carrier)
		PgRuntimeSetCurrentWork(runtime, carrier, NULL, NULL, NULL, NULL,
								false);
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

/*
 * Reset ONLY this OS thread's runtime thread-locals to the clean state a
 * freshly-spawned carrier pthread starts in, WITHOUT touching the process
 * singletons (process_runtime/backend/session/...) or MyProcPid, which are
 * shared with the postmaster thread and must not be disturbed.
 *
 * The dedicated backend_thread_entry() path assumes a brand-new thread: hot
 * current-cells in FALLBACK mode, no current work bound, and the early/
 * process fallback objects usable for the pre-MemoryContextInit timezone and
 * GUC accesses.  A pthread satisfies that by dying after one backend.  The
 * xtc carrier reuses one thread for many backend fibers, so the previous
 * fiber's thread-locals leak into the next one and its session-timezone /
 * GUC accesses fault (GUCMemoryContext resolved through a torn-down session).
 * Call this between fibers to restore the fresh-thread invariant.
 */
void
PgRuntimeResetThreadForNewBackend(void)
{
	PgRuntimeFlushCurrentHotCells();
	PgRuntimeFlushCurrentHotMirrors();
	PgRuntimeSetCurrentWork(NULL, NULL, NULL, NULL, NULL, NULL, false);
	PgRuntimeHotCurrentCellModeState = PG_RUNTIME_HOT_CURRENT_CELLS_FALLBACK;
	PgRuntimeClearHotCurrentRootRefs();
	PgRuntimeClearHotBucketPointers();
	PgRuntimeClearHotCurrentCells();
	PgRuntimeClearHotMirrorValues();
	PgRuntimeClearHotFieldPointers();
	PgSessionResetEarlyFallbackForNewBackend();
}

void
InitializePgProcessRuntime(void)
{
	BackendType backend_type = MyBackendType;

	/*
	 * Bootstrap and standalone startup can call InitProcess() before BaseInit()
	 * installs the process runtime.  Preserve the PGPROC/latch/wait-event
	 * storage that InitProcess() already made current.
	 */
	PGPROC	   *my_proc = MyProc;
	ProcNumber	my_proc_number = MyProcNumber;
	struct Latch *my_latch = MyLatch;
	uint32	   *my_wait_event_info = *PgCurrentMyWaitEventInfoRef();
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
									 backend_type, NULL);
	PgBackendAdoptEarlyState(&process_backend);
	process_backend.my_proc = my_proc;
	process_backend.my_proc_number = my_proc_number;
	process_backend.core.latch = my_latch;
	process_backend.wait_state.wait_event_info_ptr = my_wait_event_info;
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

static MemoryContext
PgRuntimeThreadServerGUCContext(void)
{
	MemoryContext parent;

	if (thread_runtime_server_guc_context != NULL)
		return thread_runtime_server_guc_context;

	parent = TopMemoryContext;
	Assert(parent != NULL);
	thread_runtime_server_guc_context =
		AllocSetContextCreate(parent,
							  "thread runtime server GUC state",
							  ALLOCSET_DEFAULT_SIZES);
	return thread_runtime_server_guc_context;
}

/*
 * The thread runtime is shared by many logical sessions.  Server GUC strings
 * must therefore live in address-space runtime memory, not in the
 * per-session GUC contexts that pooled carriers repeatedly create and delete.
 */
static bool
PgRuntimeThreadServerGUCStringIsOwned(char *value)
{
	return value != NULL &&
		thread_runtime_server_guc_context != NULL &&
		GetMemoryChunkContext(value) == thread_runtime_server_guc_context;
}

static char *
PgRuntimeCopyThreadServerGUCString(char *current, const char *source)
{
	if (current != NULL && source != NULL && strcmp(current, source) == 0)
		return current;

	if (PgRuntimeThreadServerGUCStringIsOwned(current))
		pfree(current);

	if (source == NULL)
		return NULL;

	return MemoryContextStrdup(PgRuntimeThreadServerGUCContext(), source);
}

static void
PgRuntimeCopyThreadServerGUCState(const PgRuntimeServerGUCState *source)
{
	PgRuntimeServerGUCState *dest = &thread_runtime.server_guc;

	Assert(source != NULL);
	Assert(source->initialized);

	dest->initialized = true;
	dest->cluster_name_value =
		PgRuntimeCopyThreadServerGUCString(dest->cluster_name_value,
										   source->cluster_name_value);
	dest->config_file_name =
		PgRuntimeCopyThreadServerGUCString(dest->config_file_name,
										   source->config_file_name);
	dest->hba_file_name =
		PgRuntimeCopyThreadServerGUCString(dest->hba_file_name,
										   source->hba_file_name);
	dest->ident_file_name =
		PgRuntimeCopyThreadServerGUCString(dest->ident_file_name,
										   source->ident_file_name);
	dest->hosts_file_name =
		PgRuntimeCopyThreadServerGUCString(dest->hosts_file_name,
										   source->hosts_file_name);
	dest->external_pid_file_value =
		PgRuntimeCopyThreadServerGUCString(dest->external_pid_file_value,
										   source->external_pid_file_value);
}

static void
PgRuntimeRefreshThreadServerGUCState(void)
{
	PgRuntimeServerGUCState *early_server_guc;

	/*
	 * File-location server GUCs are postmaster-only.  Once the shared thread
	 * runtime has copied them into durable address-space memory, do not
	 * refresh them from early bootstrap fallback state that auxiliary threads
	 * may temporarily rebind while startup is still unwinding.
	 */
	if (PgRuntimeServerGUCStateHasConfigPaths(&thread_runtime.server_guc))
		return;

	early_server_guc = PgEarlyRuntimeServerGUCState();
	if (PgRuntimeServerGUCStateHasConfigPaths(early_server_guc))
		PgRuntimeCopyThreadServerGUCState(early_server_guc);
	else if (PgRuntimeServerGUCStateHasConfigPaths(&process_runtime.server_guc))
		PgRuntimeCopyThreadServerGUCState(&process_runtime.server_guc);
	else if (early_server_guc->initialized)
		PgRuntimeCopyThreadServerGUCState(early_server_guc);
	else if (process_runtime.server_guc.initialized)
		PgRuntimeCopyThreadServerGUCState(&process_runtime.server_guc);
	else if (!thread_runtime.server_guc.initialized)
		PgRuntimeInitializeServerGUCState(&thread_runtime.server_guc);
}

static void
PgRuntimeConfigureThreadedAllocator(bool pooled_protocol)
{
#if defined(__GLIBC__)
	int			arena_max = pooled_protocol ? 1 : 4;

	/*
	 * Pooled protocol mode targets many mostly-idle logical sessions in one
	 * postmaster child.  Glibc's default arena growth preserves allocator
	 * throughput for pinned hot paths, but retains substantial private memory
	 * in pooled idle-connection profiles and in thread-per-session connection
	 * churn.  Keep pooled mode modest by default, use a less aggressive cap
	 * for pinned-thread mode, and let an operator-provided MALLOC_ARENA_MAX
	 * win.
	 */
	if (getenv("MALLOC_ARENA_MAX") == NULL)
		(void) mallopt(M_ARENA_MAX, arena_max);

	if (pooled_protocol)
	{
		if (getenv("MALLOC_TRIM_THRESHOLD_") == NULL)
			(void) mallopt(M_TRIM_THRESHOLD, 128 * 1024);
		if (getenv("MALLOC_TOP_PAD_") == NULL)
			(void) mallopt(M_TOP_PAD, 0);
	}
#endif
}

void
InitializePgThreadRuntime(PgBackendExitContinuation exit_backend)
{
	if (!thread_runtime_initialized)
	{
		PgRuntimeConfigureThreadedAllocator(PgRuntimePooledProtocolRequested());

		MemSet(&thread_runtime, 0, sizeof(thread_runtime));
		PgRuntimeInitializeRuntimeObject(&thread_runtime);

		if (PgRuntimePooledProtocolRequested())
		{
			thread_runtime.kind = PG_RUNTIME_POOLED_PROTOCOL;
			thread_runtime.extension_backend_model =
				PG_BACKEND_MODEL_POOLED_PROTOCOL_AFFINE;
		}
		else
		{
			thread_runtime.kind = PG_RUNTIME_THREAD_PER_SESSION;
			thread_runtime.extension_backend_model =
				PG_BACKEND_MODEL_THREAD_PER_SESSION;
		}
		thread_runtime.extension_modules = process_runtime.extension_modules;
		PgRuntimeEnsureExtensionModuleMemoryContext(&thread_runtime.extension_modules);
		PgBackendInitializeIdCounter();
		thread_runtime_initialized = true;
	}

	PgRuntimeRefreshThreadServerGUCState();
	thread_runtime.exit_backend = exit_backend;
}

void
InitializePgThreadCarrierRuntimeState(PgCarrier *carrier)
{
	PgExecution *scheduler_execution;

	Assert(carrier != NULL);
	Assert(thread_runtime_initialized);

	PgCarrierInitializeRuntimeObject(carrier);
	scheduler_execution = malloc(sizeof(PgExecution));
	if (scheduler_execution == NULL)
		elog(FATAL, "out of memory allocating carrier scheduler execution state");
	MemSet(scheduler_execution, 0, sizeof(PgExecution));
	PgExecutionInitializeRuntimeObject(scheduler_execution, NULL, NULL,
									   carrier);

	carrier->kind = PG_CARRIER_THREAD;
	carrier->runtime = &thread_runtime;
	carrier->scheduler_execution = scheduler_execution;
}

void
InitializePgThreadBackendLogicalState(PgThreadBackendLogicalState *logical,
									  PgCarrier *carrier,
									  BackendType backend_type,
									  struct Port *port,
									  struct Latch *interrupt_latch)
{
	bool		static_guc_defaults;

	Assert(logical != NULL);
	Assert(thread_runtime_initialized);

	MemSet(logical, 0, sizeof(*logical));

	PgBackendInitializeRuntimeObject(&logical->backend, &thread_runtime,
									 carrier, &logical->session,
									 &logical->connection, &logical->execution,
									 backend_type, interrupt_latch);
	static_guc_defaults =
		PgSessionSetStaticGUCDefaultsForInitialization(true);
	PgSessionInitializeRuntimeObject(&logical->session, &logical->backend,
									 &logical->connection, &logical->execution);
	(void) PgSessionSetStaticGUCDefaultsForInitialization(static_guc_defaults);
	PgConnectionInitializeRuntimeObject(&logical->connection, &logical->backend,
										&logical->session, port);
	PgExecutionInitializeRuntimeObject(&logical->execution, &logical->backend,
									   &logical->session, carrier);
}

void
InitializePgThreadBackendRuntimeState(PgThreadBackendRuntimeState *state,
									  BackendType backend_type,
									  struct Port *port,
									  struct Latch *interrupt_latch)
{
	PgThreadBackendLogicalState *logical;
	PgExecution *scheduler_execution;
	bool		static_guc_defaults;

	Assert(state != NULL);
	Assert(thread_runtime_initialized);

	MemSet(state, 0, sizeof(*state));
	logical = &state->logical;

	PgCarrierInitializeRuntimeObject(&state->carrier);
	scheduler_execution = malloc(sizeof(PgExecution));
	if (scheduler_execution == NULL)
		elog(FATAL, "out of memory allocating carrier scheduler execution state");
	MemSet(scheduler_execution, 0, sizeof(PgExecution));
	PgExecutionInitializeRuntimeObject(scheduler_execution, NULL, NULL,
									   &state->carrier);
	state->carrier.kind = PG_CARRIER_THREAD;
	state->carrier.runtime = &thread_runtime;
	state->carrier.scheduler_execution = scheduler_execution;

	PgBackendInitializeRuntimeObject(&logical->backend, &thread_runtime,
									 &state->carrier, &logical->session,
									 &logical->connection, &logical->execution,
									 backend_type, interrupt_latch);
	static_guc_defaults =
		PgSessionSetStaticGUCDefaultsForInitialization(true);
	PgSessionInitializeRuntimeObject(&logical->session, &logical->backend,
									 &logical->connection, &logical->execution);
	(void) PgSessionSetStaticGUCDefaultsForInitialization(static_guc_defaults);
	PgConnectionInitializeRuntimeObject(&logical->connection, &logical->backend,
										&logical->session, port);
	PgExecutionInitializeRuntimeObject(&logical->execution, &logical->backend,
									   &logical->session, &state->carrier);

	state->carrier.current_backend = &logical->backend;
	state->carrier.current_session = &logical->session;
	state->carrier.current_execution = &logical->execution;
}

void
InstallPgThreadBackendRuntimeState(PgThreadBackendRuntimeState *state)
{
	PgThreadBackendLogicalState *logical;
	PgExecution *scheduler_execution;

	Assert(state != NULL);

	logical = &state->logical;
	state->carrier.current_backend = &logical->backend;
	state->carrier.current_session = &logical->session;
	state->carrier.current_execution = &logical->execution;
	PgBackendAdoptEarlyState(&logical->backend);
	PgSessionAdoptEarlyState(&logical->session);
	PgConnectionAdoptEarlyState(&logical->connection,
								logical->connection.identity.port);
	PgExecutionAdoptEarlyState(&logical->execution);
	PgRuntimeSetCurrentWork(&thread_runtime, &state->carrier,
							&logical->backend, &logical->session,
							&logical->connection, &logical->execution, true);
	scheduler_execution = state->carrier.scheduler_execution;
	if (scheduler_execution != NULL &&
		scheduler_execution->memory_contexts.top_context == NULL)
	{
		scheduler_execution->memory_contexts.top_context =
			logical->execution.memory_contexts.top_context;
		scheduler_execution->memory_contexts.current_context =
			logical->execution.memory_contexts.current_context != NULL ?
			logical->execution.memory_contexts.current_context :
			logical->execution.memory_contexts.top_context;
		scheduler_execution->memory_contexts.error_context =
			logical->execution.memory_contexts.error_context;
		scheduler_execution->resource_owners.current_owner =
			logical->execution.resource_owners.current_owner;
		scheduler_execution->resource_owners.resource_owner_context =
			logical->execution.resource_owners.resource_owner_context;
	}
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

bool
PgRuntimeKindIsThreadBacked(PgRuntimeKind kind)
{
	return kind == PG_RUNTIME_THREAD_PER_SESSION ||
		kind == PG_RUNTIME_POOLED_PROTOCOL;
}

bool
PgRuntimeIsThreadBacked(PgRuntime *runtime)
{
	return runtime != NULL &&
		PgRuntimeKindIsThreadBacked(runtime->kind);
}

bool
PgRuntimeKindIsPooledProtocol(PgRuntimeKind kind)
{
	return kind == PG_RUNTIME_POOLED_PROTOCOL;
}

bool
PgRuntimeIsPooledProtocol(PgRuntime *runtime)
{
	return runtime != NULL &&
		PgRuntimeKindIsPooledProtocol(runtime->kind);
}

bool
PgRuntimePooledProtocolRequested(void)
{
	return multithreaded && pooled_protocol_carriers > 0;
}

int
PgRuntimePooledProtocolCarrierLimit(void)
{
	return pooled_protocol_carriers;
}

uint32
PgRuntimePooledProtocolIdleCarrierCount(void)
{
	PgProtocolSchedulerState *scheduler;
	uint32		idle_carriers;

	if (!thread_runtime_initialized ||
		thread_runtime.kind != PG_RUNTIME_POOLED_PROTOCOL)
		return 0;

	scheduler = &thread_runtime.protocol_scheduler;
	SpinLockAcquire(&scheduler->lock);
	idle_carriers = scheduler->idle_carrier_count;
	SpinLockRelease(&scheduler->lock);

	return idle_carriers;
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
		backend_model > PG_BACKEND_MODEL_TASK_REENTRANT)
		elog(ERROR, "invalid backend model: %d", backend_model);

	if (CurrentPgRuntime == NULL)
		return;

	/*
	 * PG_BACKEND_MODEL_POOLED_SCHEDULER is a transitional generic marker.  The
	 * protocol-boundary scheduler uses stricter protocol-affine and migratable
	 * module promises before claiming Phase 15 carrier-pool compatibility.
	 */
	check_loaded_modules_backend_model(backend_model);
	CurrentPgRuntime->extension_backend_model = backend_model;
}
