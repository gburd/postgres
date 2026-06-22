/*-------------------------------------------------------------------------
 *
 * backend_runtime_execution.c
 *	  Runtime bridge lifecycle and fallback accessors for PgExecution-owned state.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/utils/init/backend_runtime_execution.c
 *
 *-------------------------------------------------------------------------
 */
#define BACKEND_RUNTIME_NO_INLINE_BUCKET_ACCESSORS
#include "postgres.h"

#include "access/xact.h"
#include "access/xlog.h"
#include "access/xlogreader.h"
#include "commands/extension.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "replication/logical.h"
#include "storage/proc.h"
#include "utils/backend_runtime.h"
#include "backend_runtime_internal.h"
#include "utils/elog.h"

static PG_THREAD_LOCAL PG_GLOBAL_EXECUTION PgExecution early_execution_fallback = {
	.error = {
		.errordata_stack_depth = -1
	},
	.spi = {
		.connected = -1
	},
	.snapshot = {
		.transaction_xmin = FirstNormalTransactionId,
		.recent_xmin = FirstNormalTransactionId
	},
	.xact = {
		.iso_level = XACT_READ_COMMITTED,
		.check_xid_alive = InvalidTransactionId
	},
	.replication_scratch = {
		.replorigin_xact = {
			.origin = InvalidReplOriginId,
			.origin_lsn = InvalidXLogRecPtr,
			.origin_timestamp = 0
		}
	},
	.catalog = {
		.currently_reindexed_heap = InvalidOid,
		.currently_reindexed_index = InvalidOid
	}
};

#define early_execution_debug early_execution_fallback.debug
#define early_execution_error early_execution_fallback.error
#define early_execution_memory_contexts early_execution_fallback.memory_contexts
#define early_execution_resource_owners early_execution_fallback.resource_owners
#define early_execution_spi early_execution_fallback.spi
#define early_execution_portal early_execution_fallback.portal
#define early_execution_vacuum early_execution_fallback.vacuum
#define early_execution_node_io early_execution_fallback.node_io
#define early_execution_basebackup early_execution_fallback.basebackup
#define early_execution_analyze early_execution_fallback.analyze
#define early_execution_extension early_execution_fallback.extension
#define early_execution_matview early_execution_fallback.matview
#define early_execution_snapshot early_execution_fallback.snapshot
#define early_execution_combo_cid early_execution_fallback.combo_cid
#define early_execution_xloginsert early_execution_fallback.xloginsert
#define early_execution_xact early_execution_fallback.xact
#define early_execution_transaction_cleanup early_execution_fallback.transaction_cleanup
#define early_execution_replication_scratch early_execution_fallback.replication_scratch
#define early_execution_guc_error early_execution_fallback.guc_error
#define early_execution_async early_execution_fallback.async
#define early_execution_catalog early_execution_fallback.catalog
#define early_execution_catalog_cache early_execution_fallback.catalog_cache
#define early_execution_relmap early_execution_fallback.relmap
#define early_execution_invalidation early_execution_fallback.invalidation
#define early_execution_two_phase_records early_execution_fallback.two_phase_records
#define early_execution_trigger early_execution_fallback.trigger
#define early_execution_regex early_execution_fallback.regex
#define early_execution_valgrind early_execution_fallback.valgrind
#define early_execution_snapbuild early_execution_fallback.snapbuild

StaticAssertDecl(PG_EXECUTION_UNREPORTED_XIDS_CAPACITY == PGPROC_MAX_CACHED_SUBXIDS,
				 "PgExecution xact unreported XID storage must match PGPROC");

static void PgExecutionAdoptEarlyDebugState(PgExecution *execution);
static void PgExecutionAdoptEarlyErrorState(PgExecution *execution);
static void PgExecutionAdoptEarlyMemoryContexts(PgExecution *execution);
static void PgExecutionAdoptEarlyResourceOwners(PgExecution *execution);
static void PgExecutionAdoptEarlySPIState(PgExecution *execution);
static void PgExecutionAdoptEarlyPortalState(PgExecution *execution);
static void PgExecutionAdoptEarlyVacuumState(PgExecution *execution);
static void PgExecutionAdoptEarlyNodeIOState(PgExecution *execution);
static void PgExecutionAdoptEarlyBaseBackupState(PgExecution *execution);
static void PgExecutionAdoptEarlyAnalyzeState(PgExecution *execution);
static void PgExecutionAdoptEarlyExtensionState(PgExecution *execution);
static void PgExecutionAdoptEarlyMatViewState(PgExecution *execution);
static void PgExecutionAdoptEarlySnapshotState(PgExecution *execution);
static void PgExecutionAdoptEarlyComboCidState(PgExecution *execution);
static void PgExecutionAdoptEarlyXLogInsertState(PgExecution *execution);
static bool PgExecutionXLogInsertStateHasWorkingArrays(const PgExecutionXLogInsertState *xloginsert);
static void PgExecutionAdoptEarlyXactState(PgExecution *execution);
static void PgExecutionAdoptEarlyTransactionCleanupState(PgExecution *execution);
static void PgExecutionAdoptEarlyReplicationScratchState(PgExecution *execution);
static void PgExecutionAdoptEarlyGUCErrorState(PgExecution *execution);
static void PgExecutionAdoptEarlyAsyncState(PgExecution *execution);
static void PgExecutionAdoptEarlyCatalogState(PgExecution *execution);
static void PgExecutionAdoptEarlyCatalogCacheState(PgExecution *execution);
static void PgExecutionAdoptEarlyRelMapState(PgExecution *execution);
static void PgExecutionAdoptEarlyInvalidationState(PgExecution *execution);
static void PgExecutionAdoptEarlyTwoPhaseRecordState(PgExecution *execution);
static void PgExecutionAdoptEarlyTriggerState(PgExecution *execution);
static void PgExecutionAdoptEarlyRegexState(PgExecution *execution);
static void PgExecutionAdoptEarlyValgrindState(PgExecution *execution);
static void PgExecutionAdoptEarlySnapBuildState(PgExecution *execution);
PgBackendCoreState *PgCurrentCoreState(void);
PgExecutionErrorState *PgCurrentExecutionErrorState(void);
PgExecutionDebugState *PgCurrentExecutionDebugState(void);
PgExecutionMemoryContextState *PgCurrentExecutionMemoryContexts(void);
PgExecutionResourceOwnerState *PgCurrentExecutionResourceOwners(void);
PgExecutionSPIState *PgCurrentExecutionSPIState(void);
PgExecutionPortalState *PgCurrentExecutionPortalState(void);
PgExecutionVacuumState *PgCurrentExecutionVacuumState(void);
PgExecutionNodeIOState *PgCurrentExecutionNodeIOState(void);
PgExecutionBaseBackupState *PgCurrentExecutionBaseBackupState(void);
PgExecutionAnalyzeState *PgCurrentExecutionAnalyzeState(void);
PgExecutionMatViewState *PgCurrentExecutionMatViewState(void);
PgExecutionSnapshotState *PgCurrentExecutionSnapshotState(void);
PgExecutionComboCidState *PgCurrentExecutionComboCidState(void);
PgExecutionXLogInsertState *PgCurrentExecutionXLogInsertState(void);
PgExecutionXactState *PgCurrentExecutionXactState(void);
PgExecutionTransactionCleanupState *PgCurrentExecutionTransactionCleanupState(void);
PgExecutionReplicationScratchState *PgCurrentExecutionReplicationScratchState(void);
PgExecutionGUCErrorState *PgCurrentExecutionGUCErrorState(void);
PgExecutionAsyncState *PgCurrentExecutionAsyncState(void);
PgExecutionCatalogState *PgCurrentExecutionCatalogState(void);
PgExecutionCatalogCacheState *PgCurrentExecutionCatalogCacheState(void);
PgExecutionRelMapState *PgCurrentExecutionRelMapState(void);
PgExecutionInvalidationState *PgCurrentExecutionInvalidationState(void);
PgExecutionTwoPhaseRecordState *PgCurrentExecutionTwoPhaseRecordState(void);
PgExecutionSnapBuildState *PgCurrentExecutionSnapBuildState(void);

static void
PgExecutionAdoptEarlyDebugState(PgExecution *execution)
{
	Assert(execution != NULL);

	execution->debug.debug_query_string =
		early_execution_debug.debug_query_string;

	early_execution_debug.debug_query_string = NULL;
}

void
PgExecutionInitializeErrorState(PgExecutionErrorState *error)
{
	Assert(error != NULL);

	MemSet(error, 0, sizeof(*error));
	error->errordata_stack_depth = -1;
}

static void
PgExecutionAdoptEarlyErrorState(PgExecution *execution)
{
	Assert(execution != NULL);

	execution->error = early_execution_error;
	PgExecutionInitializeErrorState(&early_execution_error);
}

PG_RUNTIME_DEFINE_ADOPT_EARLY_ZERO(PgExecutionAdoptEarlyMemoryContexts,
								   PgExecution, execution, memory_contexts,
								   early_execution_memory_contexts)
PG_RUNTIME_DEFINE_ADOPT_EARLY_ZERO(PgExecutionAdoptEarlyResourceOwners,
								   PgExecution, execution, resource_owners,
								   early_execution_resource_owners)

void
PgExecutionInitializeSPIState(PgExecutionSPIState *spi)
{
	Assert(spi != NULL);

	MemSet(spi, 0, sizeof(*spi));
	spi->connected = -1;
}

static void
PgExecutionAdoptEarlySPIState(PgExecution *execution)
{
	Assert(execution != NULL);

	execution->spi = early_execution_spi;
	PgExecutionInitializeSPIState(&early_execution_spi);
}

PG_RUNTIME_DEFINE_ADOPT_EARLY_ZERO(PgExecutionAdoptEarlyPortalState,
								   PgExecution, execution, portal,
								   early_execution_portal)

PG_RUNTIME_DEFINE_ZERO_INIT(PgExecutionInitializeVacuumState,
							PgExecutionVacuumState, vacuum)
PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgExecutionAdoptEarlyVacuumState,
										PgExecution, execution, vacuum,
										early_execution_vacuum,
										PgExecutionInitializeVacuumState)
PG_RUNTIME_DEFINE_ZERO_INIT(PgExecutionInitializeNodeIOState,
							PgExecutionNodeIOState, node_io)
PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgExecutionAdoptEarlyNodeIOState,
										PgExecution, execution, node_io,
										early_execution_node_io,
										PgExecutionInitializeNodeIOState)
PG_RUNTIME_DEFINE_ZERO_INIT(PgExecutionInitializeBaseBackupState,
							PgExecutionBaseBackupState, basebackup)
PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgExecutionAdoptEarlyBaseBackupState,
										PgExecution, execution, basebackup,
										early_execution_basebackup,
										PgExecutionInitializeBaseBackupState)
PG_RUNTIME_DEFINE_ZERO_INIT(PgExecutionInitializeAnalyzeState,
							PgExecutionAnalyzeState, analyze)
PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgExecutionAdoptEarlyAnalyzeState,
										PgExecution, execution, analyze,
										early_execution_analyze,
										PgExecutionInitializeAnalyzeState)

void
PgExecutionInitializeExtensionState(PgExecutionExtensionState *extension)
{
	Assert(extension != NULL);

	extension->creating = false;
	extension->current_object = InvalidOid;
	extension->private_states = NIL;
}

static void
PgExecutionAdoptEarlyExtensionState(PgExecution *execution)
{
	Assert(execution != NULL);

	execution->extension = early_execution_extension;
	PgExecutionInitializeExtensionState(&early_execution_extension);
}

void
PgExecutionInitializeMatViewState(PgExecutionMatViewState *matview)
{
	Assert(matview != NULL);

	matview->maintenance_depth = 0;
}

static void
PgExecutionAdoptEarlyMatViewState(PgExecution *execution)
{
	Assert(execution != NULL);

	execution->matview = early_execution_matview;
	PgExecutionInitializeMatViewState(&early_execution_matview);
}

void
PgExecutionInitializeSnapshotState(PgExecutionSnapshotState *snapshot)
{
	Assert(snapshot != NULL);

	MemSet(snapshot, 0, sizeof(*snapshot));
	snapshot->transaction_xmin = FirstNormalTransactionId;
	snapshot->recent_xmin = FirstNormalTransactionId;
}

static void
PgExecutionAdoptEarlySnapshotState(PgExecution *execution)
{
	Assert(execution != NULL);

	execution->snapshot = early_execution_snapshot;
	PgExecutionInitializeSnapshotState(&early_execution_snapshot);
}

PG_RUNTIME_DEFINE_ZERO_INIT(PgExecutionInitializeComboCidState,
							PgExecutionComboCidState, combo_cid)
PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgExecutionAdoptEarlyComboCidState,
										PgExecution, execution, combo_cid,
										early_execution_combo_cid,
										PgExecutionInitializeComboCidState)

void
PgExecutionInitializeXLogInsertState(PgExecutionXLogInsertState *xloginsert)
{
	Assert(xloginsert != NULL);

	MemSet(xloginsert, 0, sizeof(*xloginsert));
}

static bool
PgExecutionXLogInsertStateHasWorkingArrays(const PgExecutionXLogInsertState *xloginsert)
{
	Assert(xloginsert != NULL);

	return xloginsert->registered_buffers != NULL &&
		xloginsert->max_registered_buffers > 0 &&
		xloginsert->rdatas != NULL &&
		xloginsert->max_rdatas > 0;
}

static void
PgExecutionAdoptEarlyXLogInsertState(PgExecution *execution)
{
	Assert(execution != NULL);
	Assert(!early_execution_xloginsert.begininsert_called);

	execution->xloginsert = early_execution_xloginsert;
	if (!PgExecutionXLogInsertStateHasWorkingArrays(&execution->xloginsert))
	{
		PG_RUNTIME_DELETE_MEMORY_CONTEXT(execution->xloginsert.context);
		PgExecutionInitializeXLogInsertState(&execution->xloginsert);
	}
	else if (execution->xloginsert.mainrdata_last ==
			 (XLogRecData *) &early_execution_xloginsert.mainrdata_head)
		execution->xloginsert.mainrdata_last =
			(XLogRecData *) &execution->xloginsert.mainrdata_head;

	PgExecutionInitializeXLogInsertState(&early_execution_xloginsert);
}

void
PgExecutionInitializeXactState(PgExecutionXactState *xact)
{
	Assert(xact != NULL);

	MemSet(xact, 0, sizeof(*xact));
	xact->iso_level = XACT_READ_COMMITTED;
	xact->check_xid_alive = InvalidTransactionId;
	xact->top_full_transaction_id = InvalidFullTransactionId;
}

static void
PgExecutionAdoptEarlyXactState(PgExecution *execution)
{
	Assert(execution != NULL);

	execution->xact = early_execution_xact;
	PgExecutionInitializeXactState(&early_execution_xact);
}

PG_RUNTIME_DEFINE_ZERO_INIT(PgExecutionInitializeTransactionCleanupState,
							PgExecutionTransactionCleanupState,
							transaction_cleanup)
PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgExecutionAdoptEarlyTransactionCleanupState,
										PgExecution, execution,
										transaction_cleanup,
										early_execution_transaction_cleanup,
										PgExecutionInitializeTransactionCleanupState)

void
PgExecutionInitializeReplicationScratchState(PgExecutionReplicationScratchState *replication_scratch)
{
	Assert(replication_scratch != NULL);

	MemSet(replication_scratch, 0, sizeof(*replication_scratch));
	replication_scratch->replorigin_xact.origin = InvalidReplOriginId;
	replication_scratch->replorigin_xact.origin_lsn = InvalidXLogRecPtr;
	replication_scratch->replorigin_xact.origin_timestamp = 0;
}

static void
PgExecutionAdoptEarlyReplicationScratchState(PgExecution *execution)
{
	Assert(execution != NULL);

	execution->replication_scratch = early_execution_replication_scratch;
	PgExecutionInitializeReplicationScratchState(&early_execution_replication_scratch);
}

PG_RUNTIME_DEFINE_ZERO_INIT(PgExecutionInitializeGUCErrorState,
							PgExecutionGUCErrorState, guc_error)
PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgExecutionAdoptEarlyGUCErrorState,
										PgExecution, execution, guc_error,
										early_execution_guc_error,
										PgExecutionInitializeGUCErrorState)
PG_RUNTIME_DEFINE_ZERO_INIT(PgExecutionInitializeAsyncState,
							PgExecutionAsyncState, async)
PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgExecutionAdoptEarlyAsyncState,
										PgExecution, execution, async,
										early_execution_async,
										PgExecutionInitializeAsyncState)

void
PgExecutionInitializeCatalogState(PgExecutionCatalogState *catalog)
{
	Assert(catalog != NULL);

	MemSet(catalog, 0, sizeof(*catalog));
	catalog->currently_reindexed_heap = InvalidOid;
	catalog->currently_reindexed_index = InvalidOid;
}

static void
PgExecutionAdoptEarlyCatalogState(PgExecution *execution)
{
	Assert(execution != NULL);

	execution->catalog = early_execution_catalog;
	PgExecutionInitializeCatalogState(&early_execution_catalog);
}

PG_RUNTIME_DEFINE_ZERO_INIT(PgExecutionInitializeCatalogCacheState,
							PgExecutionCatalogCacheState, catalog_cache)
PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgExecutionAdoptEarlyCatalogCacheState,
										PgExecution, execution, catalog_cache,
										early_execution_catalog_cache,
										PgExecutionInitializeCatalogCacheState)
PG_RUNTIME_DEFINE_ZERO_INIT(PgExecutionInitializeRelMapState,
							PgExecutionRelMapState, relmap)
PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgExecutionAdoptEarlyRelMapState,
										PgExecution, execution, relmap,
										early_execution_relmap,
										PgExecutionInitializeRelMapState)

PG_RUNTIME_DEFINE_ZERO_INIT(PgExecutionInitializeInvalidationState,
							PgExecutionInvalidationState, invalidation)
PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgExecutionAdoptEarlyInvalidationState,
										PgExecution, execution, invalidation,
										early_execution_invalidation,
										PgExecutionInitializeInvalidationState)
PG_RUNTIME_DEFINE_ZERO_INIT(PgExecutionInitializeTwoPhaseRecordState,
							PgExecutionTwoPhaseRecordState,
							two_phase_records)
PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgExecutionAdoptEarlyTwoPhaseRecordState,
										PgExecution, execution,
										two_phase_records,
										early_execution_two_phase_records,
										PgExecutionInitializeTwoPhaseRecordState)

void
PgExecutionInitializeTriggerState(PgExecutionTriggerState *trigger)
{
	Assert(trigger != NULL);

	MemSet(trigger, 0, sizeof(*trigger));
}

PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgExecutionAdoptEarlyTriggerState,
										PgExecution, execution, trigger,
										early_execution_trigger,
										PgExecutionInitializeTriggerState)

PG_RUNTIME_DEFINE_ZERO_INIT(PgExecutionInitializeRegexState,
							PgExecutionRegexState, regex)
PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgExecutionAdoptEarlyRegexState,
										PgExecution, execution, regex,
										early_execution_regex,
										PgExecutionInitializeRegexState)
PG_RUNTIME_DEFINE_ZERO_INIT(PgExecutionInitializeValgrindState,
							PgExecutionValgrindState, valgrind)
PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgExecutionAdoptEarlyValgrindState,
										PgExecution, execution, valgrind,
										early_execution_valgrind,
										PgExecutionInitializeValgrindState)
PG_RUNTIME_DEFINE_ZERO_INIT(PgExecutionInitializeSnapBuildState,
							PgExecutionSnapBuildState, snapbuild)
PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgExecutionAdoptEarlySnapBuildState,
										PgExecution, execution, snapbuild,
										early_execution_snapbuild,
										PgExecutionInitializeSnapBuildState)

void
PgExecutionAdoptEarlyState(PgExecution *execution)
{
	Assert(execution != NULL);

#define PG_EXECUTION_BUCKET(field, init, adopt, reset) \
	do { adopt; } while (0);
#include "backend_runtime_execution_buckets.def"
#undef PG_EXECUTION_BUCKET
}

void
PgExecutionInitializeRuntimeObject(PgExecution *execution,
								   PgBackend *backend,
								   PgSession *session,
								   PgCarrier *carrier)
{
	Assert(execution != NULL);

#define PG_EXECUTION_BUCKET(field, init, adopt, reset) \
	do { init; } while (0);
#include "backend_runtime_execution_buckets.def"
#undef PG_EXECUTION_BUCKET
}

PgExecution *
PgCurrentOrEarlyExecution(void)
{
	PgCarrier  *carrier;

	if (CurrentPgExecution == NULL)
	{
		carrier = CurrentPgCarrier;
		if (carrier != NULL &&
			carrier->kind == PG_CARRIER_THREAD &&
			CurrentPgRuntime != NULL &&
			CurrentPgRuntime == carrier->runtime &&
			carrier->scheduler_execution != NULL)
			return carrier->scheduler_execution;
		return &early_execution_fallback;
	}

	return CurrentPgExecution;
}

const char **
PgExecutionDebugQueryStringRef(PgExecution *execution)
{
	if (execution == NULL)
		return &early_execution_debug.debug_query_string;

	return &execution->debug.debug_query_string;
}
