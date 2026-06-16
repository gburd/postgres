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
static PG_GLOBAL_RUNTIME bool backend_id_counter_initialized = false;
static PG_GLOBAL_RUNTIME pg_atomic_uint64 next_backend_id;
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
static PG_THREAD_LOCAL PG_GLOBAL_BACKEND PgBackend early_backend_fallback = {
	.core = {
		.mode = InitProcessing
	},
	.timeout = {0},
	.replication = {
		.sync_rep_wait_mode = -1,
		.walreceiver_recv_file = -1,
		.walreceiver_primary_has_standby_xmin = true
	},
	.logical_replication = {
		.apply_error_callback_arg.remote_attnum = -1,
		.apply_error_callback_arg.remote_xid = InvalidTransactionId,
		.apply_error_callback_arg.finish_lsn = InvalidXLogRecPtr,
		.subxact_data.subxact_last = InvalidTransactionId,
		.remote_final_lsn = InvalidXLogRecPtr,
		.stream_xid = InvalidTransactionId,
		.skip_xact_finish_lsn = InvalidXLogRecPtr,
		.last_flushpos = InvalidXLogRecPtr,
		.slotsync_sleep_ms = PG_BACKEND_SLOTSYNC_INITIAL_SLEEP_MS
	},
	.xlog = {
		.local_recovery_in_progress = true,
		.local_xlog_insert_allowed = -1,
		.proc_last_rec_ptr = InvalidXLogRecPtr,
		.xact_last_rec_end = InvalidXLogRecPtr,
		.xact_last_commit_end = InvalidXLogRecPtr,
		.redo_rec_ptr = InvalidXLogRecPtr,
		.open_log_file = -1,
		.local_min_recovery_point = InvalidXLogRecPtr,
		.update_min_recovery_point = true
	},
	.recovery = {
		.standby_wait_us = PG_BACKEND_STANDBY_INITIAL_WAIT_US
	},
	.maintenance_worker = {
		.bgwriter_last_snapshot_lsn = InvalidXLogRecPtr,
		.walsummarizer_sleep_quanta = 1,
		.walsummarizer_redo_pointer_at_last_summary_removal = InvalidXLogRecPtr
	},
	.autovacuum = {
		.av_storage_param_cost_delay = -1,
		.av_storage_param_cost_limit = -1
	},
	.repack = {
		.repacked_rel_locator.relNumber = InvalidOid,
		.repacked_rel_toast_locator.relNumber = InvalidOid
	},
	.aio = {
		.my_io_worker_id = -1
	},
	.parallel = {
		.worker_number = -1,
		.pq_mq_parallel_leader_proc_number = INVALID_PROC_NUMBER
	},
	.my_proc_number = INVALID_PROC_NUMBER,
	.parallel_leader_proc_number = INVALID_PROC_NUMBER,
	.backend_type = B_INVALID
};

#define early_backend_core early_backend_fallback.core
#define early_backend_command early_backend_fallback.command
#define early_backend_log early_backend_fallback.log_state
#define early_backend_expr_interp early_backend_fallback.expr_interp
#define early_backend_type early_backend_fallback.backend_type
#define early_my_proc early_backend_fallback.my_proc
#define early_my_proc_number early_backend_fallback.my_proc_number
#define early_parallel_leader_proc_number early_backend_fallback.parallel_leader_proc_number
#define early_my_beentry early_backend_fallback.my_beentry
#define early_my_bgworker_entry early_backend_fallback.my_bgworker_entry
#define early_aux_process_resource_owner early_backend_fallback.aux_process_resource_owner
#define early_backend_pgstat_pending early_backend_fallback.pgstat_pending
#define early_backend_instrumentation early_backend_fallback.instrumentation
#define early_backend_buffers early_backend_fallback.buffers
#define early_backend_storage early_backend_fallback.storage
#define early_backend_locks early_backend_fallback.locks
#define early_backend_ipc early_backend_fallback.ipc
#define early_backend_wait_state early_backend_fallback.wait_state
#define early_backend_transaction early_backend_fallback.transaction
#define early_backend_memory_manager early_backend_fallback.memory_manager
#define early_backend_timeout early_backend_fallback.timeout
#define early_backend_walsender early_backend_fallback.walsender
#define early_backend_replication early_backend_fallback.replication
#define early_backend_logical_replication early_backend_fallback.logical_replication
#define early_backend_xlog early_backend_fallback.xlog
#define early_backend_recovery early_backend_fallback.recovery
#define early_backend_maintenance_worker early_backend_fallback.maintenance_worker
#define early_backend_autovacuum early_backend_fallback.autovacuum
#define early_backend_repack early_backend_fallback.repack
#define early_backend_aio early_backend_fallback.aio
#define early_backend_extension_modules early_backend_fallback.extension_modules
#define early_backend_activity early_backend_fallback.activity
#define early_backend_utility early_backend_fallback.utility
#define early_backend_parallel early_backend_fallback.parallel
#define early_pending_interrupts early_backend_fallback.pending_interrupts
#define early_interrupt_holdoffs early_backend_fallback.interrupt_holdoffs
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

StaticAssertDecl(PG_BACKEND_INTERRUPT_COUNT <= 32,
				 "PgBackendInterruptMask must fit all backend interrupts");
StaticAssertDecl(PG_EXECUTION_UNREPORTED_XIDS_CAPACITY == PGPROC_MAX_CACHED_SUBXIDS,
				 "PgExecution xact unreported XID storage must match PGPROC");

static void PgBackendInitializeIdCounter(void);
static PgBackendId PgBackendAssignId(void);
static void PgRuntimeInitializeServerGUCState(PgRuntimeServerGUCState *server_guc);
static void PgRuntimeAdoptEarlyServerGUCState(PgRuntime *runtime);
static bool PgRuntimeServerGUCStateHasConfigPaths(PgRuntimeServerGUCState *server_guc);
static void PgRuntimeInitializeExtensionModuleState(PgRuntimeExtensionModuleState *extension_modules);
static void PgRuntimeAdoptEarlyExtensionModuleState(PgRuntime *runtime);
PgRuntimeServerGUCState *PgCurrentRuntimeServerGUCState(void);
static void PgBackendResetCoreState(PgBackendCoreState *core);
static void PgBackendInitializeCommandState(PgBackendCommandState *command);
static void PgBackendAdoptEarlyCommandState(PgBackend *backend);
static void PgBackendInitializeLogState(PgBackendLogState *log_state);
static void PgBackendAdoptEarlyLogState(PgBackend *backend);
static void PgBackendInitializeExprInterpState(PgBackendExprInterpState *expr_interp);
static void PgBackendAdoptEarlyExprInterpState(PgBackend *backend);
static void PgBackendInitializeProcNumberState(PgBackend *backend);
static void PgBackendAdoptEarlyCoreState(PgBackend *backend);
static void PgBackendAdoptEarlyMyProc(PgBackend *backend);
static void PgBackendAdoptEarlyProcNumberState(PgBackend *backend);
static void PgBackendAdoptEarlyMyBEEntry(PgBackend *backend);
static void PgBackendAdoptEarlyMyBgworkerEntry(PgBackend *backend);
static void PgBackendAdoptEarlyAuxProcessResourceOwner(PgBackend *backend);
void PgBackendInitializePgStatPendingState(PgBackendPgStatPendingState *pgstat_pending);
static void PgBackendAdoptEarlyPgStatPendingState(PgBackend *backend);
static void PgBackendInitializeActivityState(PgBackendActivityState *activity);
static void PgBackendAdoptEarlyActivityState(PgBackend *backend);
static void PgBackendInitializeMemoryManagerState(PgBackendMemoryManagerState *memory_manager);
static void PgBackendAdoptEarlyMemoryManagerState(PgBackend *backend);
static void PgBackendInitializeUtilityState(PgBackendUtilityState *utility);
static void PgBackendAdoptEarlyUtilityState(PgBackend *backend);
void PgBackendInitializeParallelState(PgBackendParallelState *parallel);
static void PgBackendAdoptEarlyParallelState(PgBackend *backend);
static void PgBackendInitializeInstrumentationState(PgBackendInstrumentationState *instrumentation);
static void PgBackendAdoptEarlyInstrumentationState(PgBackend *backend);
void PgBackendInitializeBufferState(PgBackendBufferState *buffers);
static void PgBackendAdoptEarlyBufferState(PgBackend *backend);
static void PgBackendAdoptEarlyStorageState(PgBackend *backend);
static void PgBackendAdoptEarlyLockState(PgBackend *backend);
void PgBackendInitializeIPCState(PgBackendIPCState *ipc);
static void PgBackendAdoptEarlyIPCState(PgBackend *backend);
static void PgBackendEnsureWaitStateInitialized(PgBackendWaitState *wait_state);
void PgBackendInitializeWaitState(PgBackendWaitState *wait_state);
static void PgBackendAdoptEarlyWaitState(PgBackend *backend);
void PgBackendInitializeTransactionState(PgBackendTransactionState *transaction);
static void PgBackendAdoptEarlyTransactionState(PgBackend *backend);
static void PgBackendInitializeTimeoutState(PgBackendTimeoutState *timeout);
static void PgBackendAdoptEarlyTimeoutState(PgBackend *backend);
static void PgBackendInitializeWalSenderState(PgBackendWalSenderState *walsender);
static void PgBackendAdoptEarlyWalSenderState(PgBackend *backend);
static void PgBackendInitializeReplicationState(PgBackendReplicationState *replication);
static void PgBackendAdoptEarlyReplicationState(PgBackend *backend);
static void PgBackendInitializeLogicalReplicationState(PgBackendLogicalReplicationState *logical_replication);
static void PgBackendAdoptEarlyLogicalReplicationState(PgBackend *backend);
static void PgBackendInitializeXLogState(PgBackendXLogState *xlog);
static void PgBackendAdoptEarlyXLogState(PgBackend *backend);
void PgBackendInitializeRecoveryState(PgBackendRecoveryState *recovery);
static void PgBackendAdoptEarlyRecoveryState(PgBackend *backend);
static void PgBackendInitializeMaintenanceWorkerState(PgBackendMaintenanceWorkerState *maintenance_worker);
static void PgBackendAdoptEarlyMaintenanceWorkerState(PgBackend *backend);
static void PgBackendInitializeAutovacuumState(PgBackendAutovacuumState *autovacuum);
static void PgBackendAdoptEarlyAutovacuumState(PgBackend *backend);
void PgBackendInitializeRepackState(PgBackendRepackState *repack);
static void PgBackendAdoptEarlyRepackState(PgBackend *backend);
static void PgBackendInitializeAioState(PgBackendAioState *aio);
static void PgBackendAdoptEarlyAioState(PgBackend *backend);
static void PgBackendAdoptEarlyExtensionModuleState(PgBackend *backend);
static void PgBackendAdoptEarlyPendingInterrupts(PgBackend *backend);
static void PgBackendAdoptEarlyInterruptHoldoffs(PgBackend *backend);
static BackendType *PgCurrentBackendTypeRef(void);
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
PgBackendInitializeIdCounter(void)
{
	if (backend_id_counter_initialized)
		return;

	pg_atomic_init_u64(&next_backend_id, 0);
	backend_id_counter_initialized = true;
}

static PgBackendId
PgBackendAssignId(void)
{
	PgBackendInitializeIdCounter();

	return pg_atomic_add_fetch_u64(&next_backend_id, 1);
}

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
PgBackendResetCoreState(PgBackendCoreState *core)
{
	MemSet(core, 0, sizeof(*core));
	core->mode = InitProcessing;
}

static void
PgBackendInitializeProcNumberState(PgBackend *backend)
{
	Assert(backend != NULL);

	backend->my_proc_number = INVALID_PROC_NUMBER;
	backend->parallel_leader_proc_number = INVALID_PROC_NUMBER;
}

static void
PgBackendAdoptEarlyCoreState(PgBackend *backend)
{
	struct Latch *existing_interrupt_latch;

	Assert(backend != NULL);

	existing_interrupt_latch = backend->interrupt_latch;
	backend->core = early_backend_core;
	PgBackendResetCoreState(&early_backend_core);

	if (backend->core.latch == NULL)
		backend->core.latch = existing_interrupt_latch;
	else
		PgBackendSetInterruptLatch(backend, backend->core.latch);

	if (early_backend_type != B_INVALID)
	{
		backend->backend_type = early_backend_type;
		early_backend_type = B_INVALID;
	}
}

static void
PgBackendAdoptEarlyAuxProcessResourceOwner(PgBackend *backend)
{
	Assert(backend != NULL);

	if (early_aux_process_resource_owner != NULL)
	{
		backend->aux_process_resource_owner = early_aux_process_resource_owner;
		early_aux_process_resource_owner = NULL;
	}
}

static void
PgBackendAdoptEarlyMyProc(PgBackend *backend)
{
	Assert(backend != NULL);

	if (early_my_proc != NULL)
	{
		backend->my_proc = early_my_proc;
		early_my_proc = NULL;
	}
}

static void
PgBackendAdoptEarlyProcNumberState(PgBackend *backend)
{
	Assert(backend != NULL);

	if (early_my_proc_number != INVALID_PROC_NUMBER)
	{
		backend->my_proc_number = early_my_proc_number;
		early_my_proc_number = INVALID_PROC_NUMBER;
	}

	if (early_parallel_leader_proc_number != INVALID_PROC_NUMBER)
	{
		backend->parallel_leader_proc_number =
			early_parallel_leader_proc_number;
		early_parallel_leader_proc_number = INVALID_PROC_NUMBER;
	}
}

static void
PgBackendAdoptEarlyMyBEEntry(PgBackend *backend)
{
	Assert(backend != NULL);

	if (early_my_beentry != NULL)
	{
		backend->my_beentry = early_my_beentry;
		early_my_beentry = NULL;
	}
}

static void
PgBackendAdoptEarlyMyBgworkerEntry(PgBackend *backend)
{
	Assert(backend != NULL);

	if (early_my_bgworker_entry != NULL)
	{
		backend->my_bgworker_entry = early_my_bgworker_entry;
		early_my_bgworker_entry = NULL;
	}
}

static void
PgBackendInitializeCommandState(PgBackendCommandState *command)
{
	Assert(command != NULL);

	MemSet(command, 0, sizeof(*command));
}

PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgBackendAdoptEarlyCommandState,
										PgBackend, backend, command,
										early_backend_command,
										PgBackendInitializeCommandState)

static void
PgBackendInitializeLogState(PgBackendLogState *log_state)
{
	Assert(log_state != NULL);

	MemSet(log_state, 0, sizeof(*log_state));
}

PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgBackendAdoptEarlyLogState,
										PgBackend, backend, log_state,
										early_backend_log,
										PgBackendInitializeLogState)

static void
PgBackendInitializeExprInterpState(PgBackendExprInterpState *expr_interp)
{
	Assert(expr_interp != NULL);

	MemSet(expr_interp, 0, sizeof(*expr_interp));
}

PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgBackendAdoptEarlyExprInterpState,
										PgBackend, backend, expr_interp,
										early_backend_expr_interp,
										PgBackendInitializeExprInterpState)

void
PgBackendInitializePgStatPendingState(PgBackendPgStatPendingState *pgstat_pending)
{
	Assert(pgstat_pending != NULL);

	MemSet(pgstat_pending, 0, sizeof(*pgstat_pending));
	dlist_init(&pgstat_pending->pending);
}

static void
PgBackendAdoptEarlyPgStatPendingState(PgBackend *backend)
{
	Assert(backend != NULL);
	Assert(dlist_is_empty(&early_backend_pgstat_pending.pending));

	backend->pgstat_pending = early_backend_pgstat_pending;
	dlist_init(&backend->pgstat_pending.pending);
	PgBackendInitializePgStatPendingState(&early_backend_pgstat_pending);
}

static void
PgBackendInitializeActivityState(PgBackendActivityState *activity)
{
	Assert(activity != NULL);

	MemSet(activity, 0, sizeof(*activity));
}

PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgBackendAdoptEarlyActivityState,
										PgBackend, backend, activity,
										early_backend_activity,
										PgBackendInitializeActivityState)

static void
PgBackendInitializeMemoryManagerState(PgBackendMemoryManagerState *memory_manager)
{
	Assert(memory_manager != NULL);

	MemSet(memory_manager, 0, sizeof(*memory_manager));
}

static void
PgBackendAdoptEarlyMemoryManagerState(PgBackend *backend)
{
	Assert(backend != NULL);
	Assert(early_backend_memory_manager.context_freelists[0].num_free == 0);
	Assert(early_backend_memory_manager.context_freelists[0].first_free == NULL);
	Assert(early_backend_memory_manager.context_freelists[1].num_free == 0);
	Assert(early_backend_memory_manager.context_freelists[1].first_free == NULL);

	backend->memory_manager = early_backend_memory_manager;
	PgBackendInitializeMemoryManagerState(&early_backend_memory_manager);
}

static void
PgBackendInitializeUtilityState(PgBackendUtilityState *utility)
{
	Assert(utility != NULL);

	MemSet(utility, 0, sizeof(*utility));
	utility->superuser_last_roleid = InvalidOid;
}

static void
PgBackendAdoptEarlyUtilityState(PgBackend *backend)
{
	int			i;

	Assert(backend != NULL);
	Assert(early_backend_utility.async_global_channel_table == NULL);
	Assert(early_backend_utility.async_global_channel_dsa == NULL);
	Assert(early_backend_utility.extension_sibling_list == NULL);
	Assert(early_backend_utility.injection_point_cache == NULL);
	Assert(early_backend_utility.utility_cache_context == NULL);
	Assert(early_backend_utility.resource_release_callbacks == NULL);
	Assert(early_backend_utility.libxml_context == NULL);
	Assert(early_backend_utility.missing_attr_cache == NULL);
	for (i = 0; i < PG_BACKEND_MAX_SEQ_SCANS; i++)
		Assert(early_backend_utility.seq_scan_tables[i] == NULL);
	for (i = 0; i < PG_BACKEND_MAX_DATE_FIELDS; i++)
	{
		Assert(early_backend_utility.date_cache[i] == NULL);
		Assert(early_backend_utility.delta_cache[i] == NULL);
	}
	backend->utility = early_backend_utility;
	PgBackendInitializeUtilityState(&early_backend_utility);
}

void
PgBackendInitializeParallelState(PgBackendParallelState *parallel)
{
	Assert(parallel != NULL);

	MemSet(parallel, 0, sizeof(*parallel));
	parallel->worker_number = -1;
	parallel->pq_mq_parallel_leader_proc_number = INVALID_PROC_NUMBER;
}

static void
PgBackendAdoptEarlyParallelState(PgBackend *backend)
{
	Assert(backend != NULL);
	Assert(early_backend_parallel.message_context == NULL);

	backend->parallel = early_backend_parallel;
	if (early_backend_parallel.context_list_initialized)
	{
		Assert(dlist_is_empty(&early_backend_parallel.context_list));
		dlist_init(&backend->parallel.context_list);
		backend->parallel.context_list_initialized = true;
	}
	PgBackendInitializeParallelState(&early_backend_parallel);
}

static void
PgBackendInitializeInstrumentationState(PgBackendInstrumentationState *instrumentation)
{
	Assert(instrumentation != NULL);

	MemSet(instrumentation, 0, sizeof(*instrumentation));
}

PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgBackendAdoptEarlyInstrumentationState,
										PgBackend, backend,
										instrumentation,
										early_backend_instrumentation,
										PgBackendInitializeInstrumentationState)

void
PgBackendInitializeBufferState(PgBackendBufferState *buffers)
{
	Assert(buffers != NULL);

	MemSet(buffers, 0, sizeof(*buffers));
	buffers->reserved_ref_count_slot = -1;
	buffers->private_ref_count_entry_last = -1;
}

PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgBackendAdoptEarlyBufferState,
										PgBackend, backend, buffers,
										early_backend_buffers,
										PgBackendInitializeBufferState)

void
PgBackendInitializeStorageState(PgBackendStorageState *storage)
{
	Assert(storage != NULL);

	MemSet(storage, 0, sizeof(*storage));
	dlist_init(&storage->smgr_unpinned_relations);
}

static void
PgBackendAdoptEarlyStorageState(PgBackend *backend)
{
	Assert(backend != NULL);
	Assert(early_backend_storage.smgr_relation_hash == NULL);
	Assert(dlist_is_empty(&early_backend_storage.smgr_unpinned_relations));

	backend->storage = early_backend_storage;
	dlist_init(&backend->storage.smgr_unpinned_relations);
	PgBackendInitializeStorageState(&early_backend_storage);
}

void
PgBackendInitializeLockState(PgBackendLockState *locks)
{
	Assert(locks != NULL);

	MemSet(locks, 0, sizeof(*locks));
}

static void
PgBackendAdoptEarlyLockState(PgBackend *backend)
{
	Assert(backend != NULL);
	Assert(early_backend_locks.strong_lock_in_progress == NULL);
	Assert(early_backend_locks.awaited_lock == NULL);
	Assert(early_backend_locks.awaited_owner == NULL);
	Assert(early_backend_locks.blocking_autovacuum_proc == NULL);

	backend->locks = early_backend_locks;
	PgBackendInitializeLockState(&early_backend_locks);
}

void
PgBackendInitializeIPCState(PgBackendIPCState *ipc)
{
	Assert(ipc != NULL);

	MemSet(ipc, 0, sizeof(*ipc));
}

static void
PgBackendAdoptEarlyIPCState(PgBackend *backend)
{
	Assert(backend != NULL);

	backend->ipc = early_backend_ipc;

	if (backend->core.latch == &early_backend_ipc.local_latch_data)
		backend->core.latch = &backend->ipc.local_latch_data;

	if (backend->interrupt_latch == &early_backend_ipc.local_latch_data)
		PgBackendSetInterruptLatch(backend, &backend->ipc.local_latch_data);

	PgBackendInitializeIPCState(&early_backend_ipc);
}

static void
PgBackendEnsureWaitStateInitialized(PgBackendWaitState *wait_state)
{
	Assert(wait_state != NULL);

	if (wait_state->my_wait_event_info == NULL)
		wait_state->my_wait_event_info = &wait_state->local_wait_event_info;
}

void
PgBackendInitializeWaitState(PgBackendWaitState *wait_state)
{
	Assert(wait_state != NULL);

	MemSet(wait_state, 0, sizeof(*wait_state));
	wait_state->my_wait_event_info = &wait_state->local_wait_event_info;
	pg_atomic_init_u32(&wait_state->waiting, 0);
}

static void
PgBackendAdoptEarlyWaitState(PgBackend *backend)
{
	Assert(backend != NULL);

	PgBackendEnsureWaitStateInitialized(&early_backend_wait_state);
	backend->wait_state = early_backend_wait_state;

	if (backend->wait_state.my_wait_event_info ==
		&early_backend_wait_state.local_wait_event_info)
		backend->wait_state.my_wait_event_info =
			&backend->wait_state.local_wait_event_info;

	PgBackendInitializeWaitState(&early_backend_wait_state);
}

void
PgBackendInitializeTransactionState(PgBackendTransactionState *transaction)
{
	Assert(transaction != NULL);

	MemSet(transaction, 0, sizeof(*transaction));
	transaction->cached_fetch_xid = InvalidTransactionId;
	transaction->two_phase_cached_fxid = InvalidFullTransactionId;
	transaction->procarray_cached_xid_not_in_progress = InvalidTransactionId;
	transaction->compute_xid_horizons_result_last_xmin = InvalidTransactionId;
	dclist_init(&transaction->multixact_cache);
}

static void
PgBackendAdoptEarlyTransactionState(PgBackend *backend)
{
	Assert(backend != NULL);
	Assert(!early_backend_transaction.multixact_cache_initialized ||
		   dclist_is_empty(&early_backend_transaction.multixact_cache));

	backend->transaction = early_backend_transaction;
	if (backend->transaction.multixact_cache_initialized)
		dclist_init(&backend->transaction.multixact_cache);
	PgBackendInitializeTransactionState(&early_backend_transaction);
}

static void
PgBackendInitializeTimeoutState(PgBackendTimeoutState *timeout)
{
	Assert(timeout != NULL);

	MemSet(timeout, 0, sizeof(*timeout));
}

PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgBackendAdoptEarlyTimeoutState,
										PgBackend, backend, timeout,
										early_backend_timeout,
										PgBackendInitializeTimeoutState)

static void
PgBackendInitializeWalSenderState(PgBackendWalSenderState *walsender)
{
	Assert(walsender != NULL);

	MemSet(walsender, 0, sizeof(*walsender));
}

static void
PgBackendAdoptEarlyWalSenderState(PgBackend *backend)
{
	Assert(backend != NULL);
	Assert(early_backend_walsender.my_wal_snd == NULL);
	Assert(early_backend_walsender.xlogreader == NULL);
	Assert(early_backend_walsender.uploaded_manifest == NULL);
	Assert(early_backend_walsender.uploaded_manifest_mcxt == NULL);
	Assert(early_backend_walsender.output_message.data == NULL);
	Assert(early_backend_walsender.reply_message.data == NULL);
	Assert(early_backend_walsender.tmpbuf.data == NULL);
	Assert(early_backend_walsender.logical_decoding_ctx == NULL);
	Assert(early_backend_walsender.replication_cmd_context == NULL);
	Assert(early_backend_walsender.lag_tracker == NULL);

	backend->walsender = early_backend_walsender;
	PgBackendInitializeWalSenderState(&early_backend_walsender);
}

static void
PgBackendInitializeReplicationState(PgBackendReplicationState *replication)
{
	Assert(replication != NULL);

	MemSet(replication, 0, sizeof(*replication));
	replication->sync_rep_wait_mode = -1;
	replication->walreceiver_recv_file = -1;
	replication->walreceiver_primary_has_standby_xmin = true;
}

static void
PgBackendAdoptEarlyReplicationState(PgBackend *backend)
{
	Assert(backend != NULL);
	Assert(early_backend_replication.my_replication_slot == NULL);
	Assert(early_backend_replication.walreceiver_conn == NULL);
	Assert(early_backend_replication.walreceiver_recv_file == -1);
	Assert(early_backend_replication.walreceiver_reply_message.data == NULL);

	backend->replication = early_backend_replication;
	PgBackendInitializeReplicationState(&early_backend_replication);
}

static void
PgBackendInitializeLogicalReplicationState(PgBackendLogicalReplicationState *logical_replication)
{
	Assert(logical_replication != NULL);

	MemSet(logical_replication, 0, sizeof(*logical_replication));
	dlist_init(&logical_replication->lsn_mapping);
	logical_replication->apply_error_callback_arg.remote_attnum = -1;
	logical_replication->apply_error_callback_arg.remote_xid = InvalidTransactionId;
	logical_replication->apply_error_callback_arg.finish_lsn = InvalidXLogRecPtr;
	logical_replication->subxact_data.subxact_last = InvalidTransactionId;
	logical_replication->remote_final_lsn = InvalidXLogRecPtr;
	logical_replication->stream_xid = InvalidTransactionId;
	logical_replication->skip_xact_finish_lsn = InvalidXLogRecPtr;
	logical_replication->last_flushpos = InvalidXLogRecPtr;
	logical_replication->slotsync_sleep_ms = PG_BACKEND_SLOTSYNC_INITIAL_SLEEP_MS;
}

static void
PgBackendAdoptEarlyLogicalReplicationState(PgBackend *backend)
{
	Assert(backend != NULL);
	Assert(dlist_is_empty(&early_backend_logical_replication.lsn_mapping));
	Assert(early_backend_logical_replication.apply_error_callback_arg.rel ==
		   NULL);
	Assert(early_backend_logical_replication.apply_error_callback_arg.origin_name
		   == NULL);
	Assert(early_backend_logical_replication.subxact_data.subxacts == NULL);
	Assert(early_backend_logical_replication.apply_context == NULL);
	Assert(early_backend_logical_replication.my_parallel_shared == NULL);
	Assert(early_backend_logical_replication.logrep_worker_walrcv_conn == NULL);
	Assert(early_backend_logical_replication.my_subscription == NULL);
	Assert(early_backend_logical_replication.my_logical_rep_worker == NULL);
	Assert(early_backend_logical_replication.on_commit_wakeup_workers_subids ==
		   NIL);
	Assert(early_backend_logical_replication.stream_fd == NULL);
	Assert(early_backend_logical_replication.table_states_not_ready == NIL);
	Assert(early_backend_logical_replication.copybuf == NULL);
	Assert(early_backend_logical_replication.seqinfos == NIL);
	Assert(early_backend_logical_replication.slotsync_observed_primary_conninfo
		   == NULL);
	Assert(early_backend_logical_replication.slotsync_observed_primary_slotname
		   == NULL);
	Assert(early_backend_logical_replication.launcher_last_start_times_dsa == NULL);
	Assert(early_backend_logical_replication.launcher_last_start_times == NULL);
	Assert(early_backend_logical_replication.parallel_apply_txn_hash == NULL);
	Assert(early_backend_logical_replication.parallel_apply_worker_pool == NIL);
	Assert(early_backend_logical_replication.stream_apply_worker == NULL);
	Assert(early_backend_logical_replication.parallel_apply_subxactlist == NIL);
	Assert(early_backend_logical_replication.parallel_apply_message_context ==
		   NULL);

	backend->logical_replication = early_backend_logical_replication;
	dlist_init(&backend->logical_replication.lsn_mapping);
	PgBackendInitializeLogicalReplicationState(&early_backend_logical_replication);
}

static void
PgBackendInitializeXLogState(PgBackendXLogState *xlog)
{
	Assert(xlog != NULL);

	MemSet(xlog, 0, sizeof(*xlog));
	xlog->local_recovery_in_progress = true;
	xlog->local_xlog_insert_allowed = -1;
	xlog->proc_last_rec_ptr = InvalidXLogRecPtr;
	xlog->xact_last_rec_end = InvalidXLogRecPtr;
	xlog->xact_last_commit_end = InvalidXLogRecPtr;
	xlog->redo_rec_ptr = InvalidXLogRecPtr;
	xlog->open_log_file = -1;
	xlog->local_min_recovery_point = InvalidXLogRecPtr;
	xlog->update_min_recovery_point = true;
}

static void
PgBackendAdoptEarlyXLogState(PgBackend *backend)
{
	Assert(backend != NULL);
	Assert(early_backend_xlog.open_log_file == -1);
	Assert(early_backend_xlog.wal_debug_context == NULL);
	Assert(early_backend_xlog.btree_xlog_op_context == NULL);
	Assert(early_backend_xlog.gin_xlog_op_context == NULL);
	Assert(early_backend_xlog.gist_xlog_op_context == NULL);
	Assert(early_backend_xlog.spgist_xlog_op_context == NULL);

	backend->xlog = early_backend_xlog;
	PgBackendInitializeXLogState(&early_backend_xlog);
}

void
PgBackendInitializeRecoveryState(PgBackendRecoveryState *recovery)
{
	Assert(recovery != NULL);

	MemSet(recovery, 0, sizeof(*recovery));
	recovery->standby_wait_us = PG_BACKEND_STANDBY_INITIAL_WAIT_US;
}

static void
PgBackendAdoptEarlyRecoveryState(PgBackend *backend)
{
	Assert(backend != NULL);
	Assert(early_backend_recovery.recovery_lock_hash == NULL);
	Assert(early_backend_recovery.recovery_lock_xid_hash == NULL);

	backend->recovery = early_backend_recovery;
	PgBackendInitializeRecoveryState(&early_backend_recovery);
}

static void
PgBackendInitializeMaintenanceWorkerState(PgBackendMaintenanceWorkerState *maintenance_worker)
{
	Assert(maintenance_worker != NULL);

	MemSet(maintenance_worker, 0, sizeof(*maintenance_worker));
	maintenance_worker->bgwriter_last_snapshot_lsn = InvalidXLogRecPtr;
	maintenance_worker->walsummarizer_sleep_quanta = 1;
	maintenance_worker->walsummarizer_redo_pointer_at_last_summary_removal =
		InvalidXLogRecPtr;
}

static void
PgBackendAdoptEarlyMaintenanceWorkerState(PgBackend *backend)
{
	Assert(backend != NULL);
	Assert(early_backend_maintenance_worker.arch_module_errdetail_string ==
		   NULL);
	Assert(early_backend_maintenance_worker.archive_callbacks == NULL);
	Assert(early_backend_maintenance_worker.archive_module_state == NULL);
	Assert(early_backend_maintenance_worker.archive_context == NULL);
	Assert(early_backend_maintenance_worker.loaded_archive_library == NULL);
	Assert(early_backend_maintenance_worker.pgarch_files == NULL);
	Assert(early_backend_maintenance_worker.bgwriter_context == NULL);
	Assert(early_backend_maintenance_worker.walwriter_context == NULL);
	Assert(early_backend_maintenance_worker.checkpointer_context == NULL);
	Assert(early_backend_maintenance_worker.walsummarizer_context == NULL);

	backend->maintenance_worker = early_backend_maintenance_worker;
	PgBackendInitializeMaintenanceWorkerState(&early_backend_maintenance_worker);
}

static void
PgBackendInitializeAutovacuumState(PgBackendAutovacuumState *autovacuum)
{
	Assert(autovacuum != NULL);

	MemSet(autovacuum, 0, sizeof(*autovacuum));
	autovacuum->av_storage_param_cost_delay = -1;
	autovacuum->av_storage_param_cost_limit = -1;
	dlist_init(&autovacuum->database_list);
}

static void
PgBackendAdoptEarlyAutovacuumState(PgBackend *backend)
{
	Assert(backend != NULL);
	Assert(early_backend_autovacuum.autovac_mem_cxt == NULL);
	Assert(dlist_is_empty(&early_backend_autovacuum.database_list));
	Assert(early_backend_autovacuum.database_list_cxt == NULL);
	Assert(early_backend_autovacuum.avl_dbase_array == NULL);
	Assert(early_backend_autovacuum.my_worker_info == NULL);

	backend->autovacuum = early_backend_autovacuum;
	dlist_init(&backend->autovacuum.database_list);
	PgBackendInitializeAutovacuumState(&early_backend_autovacuum);
}

void
PgBackendInitializeRepackState(PgBackendRepackState *repack)
{
	Assert(repack != NULL);

	MemSet(repack, 0, sizeof(*repack));
	repack->repacked_rel_locator.relNumber = InvalidOid;
	repack->repacked_rel_toast_locator.relNumber = InvalidOid;
}

static void
PgBackendAdoptEarlyRepackState(PgBackend *backend)
{
	Assert(backend != NULL);
	Assert(early_backend_repack.decoding_worker == NULL);
	Assert(early_backend_repack.worker_dsm_segment == NULL);
	Assert(early_backend_repack.message_context == NULL);

	backend->repack = early_backend_repack;
	PgBackendInitializeRepackState(&early_backend_repack);
}

static void
PgBackendInitializeAioState(PgBackendAioState *aio)
{
	Assert(aio != NULL);

	MemSet(aio, 0, sizeof(*aio));
	aio->my_io_worker_id = -1;
}

static void
PgBackendAdoptEarlyAioState(PgBackend *backend)
{
	Assert(backend != NULL);
	Assert(early_backend_aio.my_backend == NULL);
	Assert(early_backend_aio.my_uring_context == NULL);

	backend->aio = early_backend_aio;
	PgBackendInitializeAioState(&early_backend_aio);
}

void
PgBackendInitializeExtensionModuleState(PgBackendExtensionModuleState *extension_modules)
{
	Assert(extension_modules != NULL);

	MemSet(extension_modules, 0, sizeof(*extension_modules));
	extension_modules->basic_archive_archive_directory = "";
}

static void
PgBackendAdoptEarlyExtensionModuleState(PgBackend *backend)
{
	Assert(backend != NULL);

	backend->extension_modules = early_backend_extension_modules;
	PgBackendInitializeExtensionModuleState(&early_backend_extension_modules);
}

static void
PgBackendAdoptEarlyPendingInterrupts(PgBackend *backend)
{
	Assert(backend != NULL);

	backend->pending_interrupts = early_pending_interrupts;
	MemSet(&early_pending_interrupts, 0, sizeof(early_pending_interrupts));
}

static void
PgBackendAdoptEarlyInterruptHoldoffs(PgBackend *backend)
{
	Assert(backend != NULL);

	backend->interrupt_holdoffs.interrupt_holdoff_count =
		early_interrupt_holdoffs.interrupt_holdoff_count;
	backend->interrupt_holdoffs.query_cancel_holdoff_count =
		early_interrupt_holdoffs.query_cancel_holdoff_count;
	backend->interrupt_holdoffs.crit_section_count =
		early_interrupt_holdoffs.crit_section_count;

	early_interrupt_holdoffs.interrupt_holdoff_count = 0;
	early_interrupt_holdoffs.query_cancel_holdoff_count = 0;
	early_interrupt_holdoffs.crit_section_count = 0;
}

void
PgBackendAdoptEarlyState(PgBackend *backend)
{
	Assert(backend != NULL);

#define PG_BACKEND_BUCKET(field, init, adopt, reset) \
	do { adopt; } while (0);
#include "backend_runtime_backend_buckets.def"
#undef PG_BACKEND_BUCKET
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
PgBackendInitializeRuntimeObject(PgBackend *backend,
								 PgRuntime *runtime,
								 PgCarrier *carrier,
								 PgSession *session,
								 PgConnection *connection,
								 PgExecution *execution,
								 BackendType backend_type,
								 struct Latch *interrupt_latch)
{
	Assert(backend != NULL);

#define PG_BACKEND_BUCKET(field, init, adopt, reset) \
	do { init; } while (0);
#include "backend_runtime_backend_buckets.def"
#undef PG_BACKEND_BUCKET
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

	PgBackendInitializeRuntimeObject(&early_backend_fallback, NULL, NULL,
									 NULL, NULL, NULL, B_INVALID, NULL);
	early_backend_core.proc_pid = (int) getpid();
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

PgBackendActivityState *
PgCurrentBackendActivityState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_activity;

	return &CurrentPgBackend->activity;
}

PgBackendMemoryManagerState *
PgCurrentBackendMemoryManagerState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_memory_manager;

	return &CurrentPgBackend->memory_manager;
}

PgBackendUtilityState *
PgCurrentBackendUtilityState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_utility;

	return &CurrentPgBackend->utility;
}

PgBackendParallelState *
PgCurrentBackendParallelState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_parallel;

	return &CurrentPgBackend->parallel;
}

const char **
PgExecutionDebugQueryStringRef(PgExecution *execution)
{
	if (execution == NULL)
		return &early_execution_debug.debug_query_string;

	return &execution->debug.debug_query_string;
}

PgBackendCoreState *
PgCurrentCoreState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_core;

	return &CurrentPgBackend->core;
}

PGPROC **
PgCurrentMyProcRef(void)
{
	if (CurrentPgBackend == NULL)
		return &early_my_proc;

	return &CurrentPgBackend->my_proc;
}

ProcNumber *
PgCurrentMyProcNumberRef(void)
{
	if (CurrentPgBackend == NULL)
		return &early_my_proc_number;

	return &CurrentPgBackend->my_proc_number;
}

ProcNumber *
PgCurrentParallelLeaderProcNumberRef(void)
{
	if (CurrentPgBackend == NULL)
		return &early_parallel_leader_proc_number;

	return &CurrentPgBackend->parallel_leader_proc_number;
}

PgBackendStatus **
PgCurrentMyBEEntryRef(void)
{
	if (CurrentPgBackend == NULL)
		return &early_my_beentry;

	return &CurrentPgBackend->my_beentry;
}

BackgroundWorker **
PgCurrentMyBgworkerEntryRef(void)
{
	if (CurrentPgBackend == NULL)
		return &early_my_bgworker_entry;

	return &CurrentPgBackend->my_bgworker_entry;
}

ResourceOwner *
PgCurrentAuxProcessResourceOwnerRef(void)
{
	if (CurrentPgBackend == NULL)
		return &early_aux_process_resource_owner;

	return &CurrentPgBackend->aux_process_resource_owner;
}

pg_prng_state *
PgCurrentGlobalPrngStateRef(void)
{
	return &PgCurrentCoreState()->global_prng_state;
}

PgBackendCommandState *
PgCurrentBackendCommandState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_command;

	return &CurrentPgBackend->command;
}

PgBackendLogState *
PgCurrentBackendLogState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_log;

	return &CurrentPgBackend->log_state;
}

PgBackendExprInterpState *
PgCurrentExprInterpState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_expr_interp;

	return &CurrentPgBackend->expr_interp;
}

static BackendType *
PgCurrentBackendTypeRef(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_type;

	return &CurrentPgBackend->backend_type;
}

BackendType *
PgCurrentMyBackendTypeRef(void)
{
	return PgCurrentBackendTypeRef();
}

PgBackendPgStatPendingState *
PgCurrentBackendPgStatPendingState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_pgstat_pending;

	return &CurrentPgBackend->pgstat_pending;
}

PgBackendInstrumentationState *
PgCurrentBackendInstrumentationState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_instrumentation;

	return &CurrentPgBackend->instrumentation;
}

PgBackendBufferState *
PgCurrentBackendBufferState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_buffers;

	return &CurrentPgBackend->buffers;
}

MemoryContext
PgBackendBufferAllocationContext(void)
{
	PgBackendBufferState *buffers = PgCurrentBackendBufferState();

	if (TopMemoryContext != NULL)
		return PgRuntimeGetOwnedMemoryContextWithSizes(&buffers->buffer_context,
													   "BackendBufferContext",
													   ALLOCSET_DEFAULT_SIZES);
	if (CurrentMemoryContext != NULL)
		return CurrentMemoryContext;

	elog(ERROR, "cannot allocate backend buffer state before memory contexts exist");
	return NULL;				/* keep compiler quiet */
}

PgBackendStorageState *
PgCurrentBackendStorageState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_storage;

	return &CurrentPgBackend->storage;
}

PgBackendLockState *
PgCurrentBackendLockState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_locks;

	return &CurrentPgBackend->locks;
}

PgBackendIPCState *
PgCurrentBackendIPCState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_ipc;

	return &CurrentPgBackend->ipc;
}

PgBackendWaitState *
PgCurrentBackendWaitState(void)
{
	if (CurrentPgBackend == NULL)
	{
		PgBackendEnsureWaitStateInitialized(&early_backend_wait_state);
		return &early_backend_wait_state;
	}

	PgBackendEnsureWaitStateInitialized(&CurrentPgBackend->wait_state);
	return &CurrentPgBackend->wait_state;
}

PgBackendTimeoutState *
PgCurrentTimeoutState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_timeout;

	return &CurrentPgBackend->timeout;
}

PgBackendWalSenderState *
PgCurrentWalSenderState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_walsender;

	return &CurrentPgBackend->walsender;
}

PgBackendReplicationState *
PgCurrentReplicationState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_replication;

	return &CurrentPgBackend->replication;
}

PgBackendLogicalReplicationState *
PgCurrentLogicalReplicationState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_logical_replication;

	return &CurrentPgBackend->logical_replication;
}

PgBackendXLogState *
PgCurrentXLogState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_xlog;

	return &CurrentPgBackend->xlog;
}

PgBackendRecoveryState *
PgCurrentRecoveryState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_recovery;

	return &CurrentPgBackend->recovery;
}

PgBackendMaintenanceWorkerState *
PgCurrentMaintenanceWorkerState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_maintenance_worker;

	return &CurrentPgBackend->maintenance_worker;
}

char **
PgCurrentArchModuleCheckErrdetailStringRef(void)
{
	return &PgCurrentMaintenanceWorkerState()->arch_module_errdetail_string;
}

PgBackendAutovacuumState *
PgCurrentAutovacuumState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_autovacuum;

	return &CurrentPgBackend->autovacuum;
}

PgBackendRepackState *
PgCurrentRepackState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_repack;

	return &CurrentPgBackend->repack;
}

volatile sig_atomic_t *
PgCurrentRepackMessagePendingRef(void)
{
	return &PgCurrentRepackState()->message_pending;
}

PgBackendAioState *
PgCurrentAioState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_aio;

	return &CurrentPgBackend->aio;
}

struct PgAioBackend **
PgCurrentAioBackendRef(void)
{
	return &PgCurrentAioState()->my_backend;
}

PgBackendExtensionModuleState *
PgCurrentBackendExtensionModuleState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_extension_modules;

	return &CurrentPgBackend->extension_modules;
}

char **
PgCurrentBasicArchiveDirectoryRef(void)
{
	return &PgCurrentBackendExtensionModuleState()->basic_archive_archive_directory;
}

PgBackendTransactionState *
PgCurrentBackendTransactionState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_transaction;

	return &CurrentPgBackend->transaction;
}

PgBackendPendingInterruptState *
PgCurrentPendingInterrupts(void)
{
	if (CurrentPgBackend == NULL)
		return &early_pending_interrupts;

	return &CurrentPgBackend->pending_interrupts;
}

PgBackendInterruptHoldoffState *
PgCurrentInterruptHoldoffs(void)
{
	if (CurrentPgBackend == NULL)
		return &early_interrupt_holdoffs;

	return &CurrentPgBackend->interrupt_holdoffs;
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

void
PgBackendInitializeInterrupts(PgBackend *backend)
{
	if (backend == NULL)
		return;

	pg_atomic_init_u32(&backend->interrupts.pending_mask, 0);
	backend->interrupts.proc_die_sender_pid = 0;
	backend->interrupts.proc_die_sender_uid = 0;
}

void
PgBackendSetInterruptLatch(PgBackend *backend, struct Latch *interrupt_latch)
{
	if (backend == NULL)
		return;

	backend->interrupt_latch = interrupt_latch;
}

PgBackendId
PgBackendGetId(PgBackend *backend)
{
	if (backend == NULL)
		return 0;

	return backend->id;
}

PgBackendId
PgCurrentBackendId(void)
{
	return PgBackendGetId(CurrentPgBackend);
}

int
PgBackendGetSignalPid(PgBackend *backend)
{
	if (backend == NULL)
		return MyProcPid;

	if (backend->runtime != NULL &&
		backend->runtime->kind == PG_RUNTIME_THREAD_PER_SESSION)
	{
		if (backend->id > PG_INT32_MAX)
			elog(ERROR, "threaded backend identifier exceeds protocol range");

		return (int) backend->id;
	}

	return MyProcPid;
}

int
PgCurrentBackendSignalPid(void)
{
	return PgBackendGetSignalPid(CurrentPgBackend);
}

bool
PgBackendUsesProcessSignals(PgBackend *backend)
{
	if (backend == NULL || backend->runtime == NULL)
		return true;

	return backend->runtime->kind == PG_RUNTIME_PROCESS;
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
