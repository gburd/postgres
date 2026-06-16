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
static PG_GLOBAL_RUNTIME PgRuntimeServerGUCState early_runtime_server_guc = {
	.initialized = true,
	.cluster_name_value = "",
	.config_file_name = NULL,
	.hba_file_name = NULL,
	.ident_file_name = NULL,
	.hosts_file_name = NULL,
	.external_pid_file_value = NULL
};
static PG_GLOBAL_RUNTIME PgRuntimeExtensionModuleState early_runtime_extension_modules;
static PG_GLOBAL_CARRIER PgCarrier process_carrier;
static PG_GLOBAL_BACKEND PgBackend process_backend;
static PG_GLOBAL_SESSION PgSession process_session;
static PG_GLOBAL_CONNECTION PgConnection process_connection;
static PG_GLOBAL_EXECUTION PgExecution process_execution;
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

static void PgRuntimeInitializeServerGUCState(PgRuntimeServerGUCState *server_guc);
static void PgRuntimeAdoptEarlyServerGUCState(PgRuntime *runtime);
static bool PgRuntimeServerGUCStateHasConfigPaths(PgRuntimeServerGUCState *server_guc);
static void PgRuntimeInitializeExtensionModuleState(PgRuntimeExtensionModuleState *extension_modules);
static void PgRuntimeAdoptEarlyExtensionModuleState(PgRuntime *runtime);
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
PgBackendPgStatPendingState *PgCurrentBackendPgStatPendingState(void);
PgBackendInstrumentationState *PgCurrentBackendInstrumentationState(void);
PgBackendTransactionState *PgCurrentBackendTransactionState(void);
PgBackendPendingInterruptState *PgCurrentPendingInterrupts(void);
PgBackendInterruptHoldoffState *PgCurrentInterruptHoldoffs(void);


static void
PgRuntimeInitializeServerGUCState(PgRuntimeServerGUCState *server_guc)
{
	Assert(server_guc != NULL);

	server_guc->initialized = true;
	server_guc->cluster_name_value = guc_strdup(FATAL, "");
	server_guc->config_file_name = NULL;
	server_guc->hba_file_name = NULL;
	server_guc->ident_file_name = NULL;
	server_guc->hosts_file_name = NULL;
	server_guc->external_pid_file_value = NULL;
}

static void
PgRuntimeAdoptEarlyServerGUCState(PgRuntime *runtime)
{
	Assert(runtime != NULL);

	if (!early_runtime_server_guc.initialized)
		PgRuntimeInitializeServerGUCState(&early_runtime_server_guc);

	/*
	 * Runtime server GUC strings describe address-space state selected during
	 * postmaster startup.  Auxiliary threads can initialize process runtime
	 * state more than once, so keep the early fallback as a persistent mirror
	 * rather than consuming it on first adoption.
	 */
	runtime->server_guc = early_runtime_server_guc;
}

static bool
PgRuntimeServerGUCStateHasConfigPaths(PgRuntimeServerGUCState *server_guc)
{
	return server_guc != NULL &&
		server_guc->initialized &&
		server_guc->config_file_name != NULL &&
		server_guc->config_file_name[0] != '\0';
}

static void
PgRuntimeInitializeExtensionModuleState(PgRuntimeExtensionModuleState *extension_modules)
{
	Assert(extension_modules != NULL);

	extension_modules->memory_context = NULL;
	extension_modules->pg_plan_advice_context = NULL;
	extension_modules->pg_plan_advice_advisor_hook_list = NIL;
	extension_modules->bloom_context = NULL;
	extension_modules->rendezvous_hash = NULL;
}

MemoryContext
PgRuntimeEnsureExtensionModuleMemoryContext(PgRuntimeExtensionModuleState *extension_modules)
{
	Assert(extension_modules != NULL);

	if (extension_modules->memory_context == NULL)
	{
		if (CurrentPgRuntime != NULL &&
			CurrentPgRuntime->kind == PG_RUNTIME_THREAD_PER_SESSION)
			elog(ERROR,
				 "thread runtime extension module memory context is not initialized");

		extension_modules->memory_context =
			AllocSetContextCreate(TopMemoryContext,
								  "RuntimeExtensionModules",
								  ALLOCSET_DEFAULT_SIZES);
	}

	return extension_modules->memory_context;
}

static void
PgRuntimeAdoptEarlyExtensionModuleState(PgRuntime *runtime)
{
	Assert(runtime != NULL);

	runtime->extension_modules = early_runtime_extension_modules;
	PgRuntimeInitializeExtensionModuleState(&early_runtime_extension_modules);
}


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
	extension->auto_explain_nesting_level = 0;
	extension->auto_explain_current_query_sampled = false;
	extension->pgcrypto_debug_handler = NULL;
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

static void
PgExecutionAdoptEarlyXLogInsertState(PgExecution *execution)
{
	Assert(execution != NULL);
	Assert(!early_execution_xloginsert.begininsert_called);

	execution->xloginsert = early_execution_xloginsert;
	if (execution->xloginsert.mainrdata_last ==
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
		MemSet(&thread_runtime, 0, sizeof(thread_runtime));
		PgRuntimeInitializeRuntimeObject(&thread_runtime);

		thread_runtime.kind = PG_RUNTIME_THREAD_PER_SESSION;
		thread_runtime.extension_backend_model =
			PG_BACKEND_MODEL_THREAD_PER_SESSION;
		if (PgRuntimeServerGUCStateHasConfigPaths(&process_runtime.server_guc))
			thread_runtime.server_guc = process_runtime.server_guc;
		else if (PgRuntimeServerGUCStateHasConfigPaths(&early_runtime_server_guc))
			thread_runtime.server_guc = early_runtime_server_guc;
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

PgExecution *
PgCurrentOrEarlyExecution(void)
{
	if (CurrentPgExecution == NULL)
		return &early_execution_fallback;

	return CurrentPgExecution;
}

void
PgRuntimeDeleteOwnedMemoryContext(MemoryContext *context)
{
	Assert(context != NULL);

	if (*context == NULL)
		return;

	if (CurrentMemoryContext == *context)
		MemoryContextSwitchTo(TopMemoryContext);
	MemoryContextDelete(*context);
	*context = NULL;
}

MemoryContext
PgSessionGetDynamicLibraryMemoryContext(PgSession *session)
{
	Assert(session != NULL);

	return PgRuntimeGetOwnedMemoryContext(&session->dynamic_library_context,
										  "dynamic library session state");
}

List **
PgCurrentSessionDynamicLibraryInitsRef(void)
{
	Assert(CurrentPgSession != NULL);

	return &CurrentPgSession->dynamic_library_inits;
}

Session *
PgSessionGetLegacySession(PgSession *session)
{
	if (session == NULL)
		return NULL;

	if (session->legacy_session == NULL)
	{
		Assert(session->legacy_session_context == NULL);
		(void) PgRuntimeGetOwnedMemoryContext(&session->legacy_session_context,
											  "legacy session compatibility state");
		session->legacy_session =
			MemoryContextAllocZero(session->legacy_session_context,
								   sizeof(Session));
	}

	return session->legacy_session;
}

Session *
PgCurrentLegacySession(void)
{
	if (CurrentPgSession == NULL)
	{
		if (TopMemoryContext == NULL)
			return process_session.legacy_session;

		return PgSessionGetLegacySession(&process_session);
	}

	return PgSessionGetLegacySession(CurrentPgSession);
}

Session **
PgCurrentLegacySessionRef(void)
{
	if (CurrentPgSession == NULL)
		return &process_session.legacy_session;

	return &CurrentPgSession->legacy_session;
}

PgRuntimeServerGUCState *
PgCurrentRuntimeServerGUCState(void)
{
	PgRuntimeServerGUCState *server_guc;

	if (CurrentPgRuntime == NULL)
		server_guc = &early_runtime_server_guc;
	else
		server_guc = &CurrentPgRuntime->server_guc;

	if (!server_guc->initialized)
		PgRuntimeInitializeServerGUCState(server_guc);

	return server_guc;
}

PgRuntimeExtensionModuleState *
PgCurrentRuntimeExtensionModuleState(void)
{
	if (CurrentPgRuntime == NULL)
		return &early_runtime_extension_modules;

	return &CurrentPgRuntime->extension_modules;
}

const char **
PgExecutionDebugQueryStringRef(PgExecution *execution)
{
	if (execution == NULL)
		return &early_execution_debug.debug_query_string;

	return &execution->debug.debug_query_string;
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


int
PgSuspend(const PgWaitSpec *wait_spec, PgSuspendCallback callback,
		  void *callback_arg)
{
	PgBackend  *backend = CurrentPgBackend;
	int			result = 0;

	Assert(callback != NULL);

	if (backend != NULL && wait_spec != NULL)
	{
		backend->wait_state.spec = *wait_spec;
		pg_atomic_write_membarrier_u32(&backend->wait_state.waiting, 1);
	}

	PG_TRY();
	{
		result = callback(callback_arg);
	}
	PG_CATCH();
	{
		if (backend != NULL)
		{
			pg_atomic_write_u32(&backend->wait_state.waiting, 0);
			backend->wait_state.spec.kind = PG_WAIT_KIND_NONE;
		}
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (backend != NULL)
	{
		pg_atomic_write_u32(&backend->wait_state.waiting, 0);
		backend->wait_state.spec.kind = PG_WAIT_KIND_NONE;
	}

	return result;
}
