/*-------------------------------------------------------------------------
 *
 * backend_runtime_backend.c
 *	  Runtime bridge lifecycle and accessors for PgBackend-owned state.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/utils/init/backend_runtime_backend.c
 *
 *-------------------------------------------------------------------------
 */
#define BACKEND_RUNTIME_NO_INLINE_BUCKET_ACCESSORS
#include "postgres.h"

#include "access/gin.h"
#include "access/parallel.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xlogreader.h"
#include "archive/archive_module.h"
#include "commands/async.h"
#include "commands/extension.h"
#include "commands/repack.h"
#include "executor/spi.h"
#include "lib/dshash.h"
#include "libpq/libpq.h"
#include "miscadmin.h"
#include "postmaster/pgarch.h"
#include "postmaster/interrupt.h"
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
#include "storage/lock.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "storage/sinval.h"
#include "utils/backend_runtime.h"
#include "backend_runtime_internal.h"
#include "utils/dsa.h"
#include "utils/elog.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/timestamp.h"

static PG_GLOBAL_RUNTIME bool backend_id_counter_initialized = false;
static PG_GLOBAL_RUNTIME pg_atomic_uint64 next_backend_id;

#ifndef WIN32
typedef struct ThreadedBackendRegistryEntry
{
	PgBackendId backend_id;
	PgBackend  *backend;
	struct ThreadedBackendRegistryEntry *next;
} ThreadedBackendRegistryEntry;

static PG_GLOBAL_RUNTIME pthread_mutex_t ThreadedBackendRegistryMutex = PTHREAD_MUTEX_INITIALIZER;
static PG_GLOBAL_RUNTIME ThreadedBackendRegistryEntry *ThreadedBackendRegistry = NULL;
#endif
static PG_THREAD_LOCAL PG_GLOBAL_BACKEND PgBackend early_backend_fallback = {
	.core = {
		.mode = InitProcessing
	},
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

static void PgRuntimeProtocolSchedulerRecordResume(PgBackend *backend);

#define BASIC_ARCHIVE_BACKEND_STATE_KEY "basic_archive.backend"

typedef struct PgBasicArchiveBackendState
{
	char	   *archive_directory;
} PgBasicArchiveBackendState;

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

StaticAssertDecl(PG_BACKEND_INTERRUPT_COUNT <= 32,
				 "PgBackendInterruptMask must fit all backend interrupts");

static PgBackendId PgBackendAssignId(void);
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
static void PgBackendInitializeProtocolParkState(PgBackendProtocolParkState *protocol_park);
static void PgWaitCompletionInitialize(PgWaitCompletion *completion);
#ifdef PG_RUNTIME_ENABLE_WAIT_COMPLETION_PUBLICATION
static void PgWaitCompletionPublish(PgWaitCompletion *completion,
									PgBackend *backend,
									const PgWaitSpec *wait_spec);
static void PgWaitCompletionClear(PgWaitCompletion *completion);
static void PgBackendClearWaitCompletion(PgBackendWaitState *wait_state);
static void PgBackendWakeForWaitCompletion(PgBackend *backend);
#endif
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

void
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

#ifndef WIN32
static void
ThreadedBackendRegistryLock(void)
{
	int			rc;

	rc = pthread_mutex_lock(&ThreadedBackendRegistryMutex);
	if (rc != 0)
	{
		errno = rc;
		elog(FATAL, "could not lock threaded backend registry: %m");
	}
}

static void
ThreadedBackendRegistryUnlock(void)
{
	int			rc;

	rc = pthread_mutex_unlock(&ThreadedBackendRegistryMutex);
	if (rc != 0)
	{
		errno = rc;
		elog(FATAL, "could not unlock threaded backend registry: %m");
	}
}

static PgBackend *
ThreadedBackendRegistryLookupLocked(PgBackendId backend_id)
{
	for (ThreadedBackendRegistryEntry *entry = ThreadedBackendRegistry;
		 entry != NULL;
		 entry = entry->next)
	{
		if (entry->backend_id == backend_id)
			return entry->backend;
	}

	return NULL;
}
#endif

static void
PgBackendRegisterThreadedBackend(PgBackend *backend)
{
#ifndef WIN32
	ThreadedBackendRegistryEntry *entry;

	Assert(backend != NULL);

	if (!PgRuntimeIsThreadBacked(backend->runtime))
		return;

	entry = malloc(sizeof(*entry));
	if (entry == NULL)
		elog(FATAL, "out of memory registering threaded backend");
	entry->backend_id = backend->id;
	entry->backend = backend;

	ThreadedBackendRegistryLock();
	for (ThreadedBackendRegistryEntry *existing = ThreadedBackendRegistry;
		 existing != NULL;
		 existing = existing->next)
	{
		if (existing->backend_id != backend->id)
			continue;

		if (existing->backend != backend)
		{
			ThreadedBackendRegistryUnlock();
			free(entry);
			elog(FATAL, "threaded backend id %llu is already registered",
				 (unsigned long long) backend->id);
		}

		ThreadedBackendRegistryUnlock();
		free(entry);
		return;
	}

	entry->next = ThreadedBackendRegistry;
	ThreadedBackendRegistry = entry;
	ThreadedBackendRegistryUnlock();
#endif
}

void
PgBackendUnregisterThreadedBackend(PgBackend *backend)
{
#ifndef WIN32
	Assert(backend != NULL);

	if (!PgRuntimeIsThreadBacked(backend->runtime))
		return;

	ThreadedBackendRegistryLock();
	for (ThreadedBackendRegistryEntry **link = &ThreadedBackendRegistry;
		 *link != NULL;
		 link = &(*link)->next)
	{
		ThreadedBackendRegistryEntry *entry = *link;

		if (entry->backend_id == backend->id && entry->backend == backend)
		{
			*link = entry->next;
			free(entry);
			break;
		}
	}
	ThreadedBackendRegistryUnlock();
#endif
}

bool
PgBackendSendInterruptById(PgBackendId backend_id,
						   PgBackendInterruptType interrupt_type,
						   int sender_pid, int sender_uid)
{
#ifndef WIN32
	PgBackend  *backend = NULL;

	ThreadedBackendRegistryLock();
	backend = ThreadedBackendRegistryLookupLocked(backend_id);

	if (backend != NULL)
	{
		if (interrupt_type == PG_BACKEND_INTERRUPT_PROC_DIE)
			PgBackendRaiseProcDieInterrupt(backend, sender_pid, sender_uid);
		else
			SendInterrupt(backend, interrupt_type);
	}
	ThreadedBackendRegistryUnlock();

	return backend != NULL;
#else
	(void) backend_id;
	(void) interrupt_type;
	(void) sender_pid;
	(void) sender_uid;
	return false;
#endif
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

	/*
	 * Adopt the early memory-manager state wholesale, INCLUDING any aset.c
	 * context freelists it has accumulated.  Do NOT assert the freelists are
	 * empty here: they legitimately are not.  Bootstrap/standalone startup
	 * runs SelectConfigFiles() -> ProcessConfigFile() and timezone-abbrev
	 * validation (check_timezone_abbreviations -> load_tzoffsets) BEFORE
	 * BaseInit() reaches this adoption point, and those routines create and
	 * delete ALLOCSET_DEFAULT/SMALL contexts ("config file processing",
	 * "TZParserMemory").  AllocSetDelete() of a freelist-eligible context does
	 * not free it -- it caches the reusable header on the current freelist for
	 * the next same-sized context.  That cache is exactly the state we want to
	 * carry into the backend, so the copy below transfers ownership of those
	 * cached contexts to backend->memory_manager and clears the early copy.  A
	 * non-empty freelist at adoption is normal and correct, not a leak.
	 */
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
	locks->held_lwlocks_array = locks->held_lwlocks_inline;
	locks->held_lwlocks_capacity = PG_BACKEND_MAX_INLINE_LWLOCKS;
}

static void
PgBackendAdoptEarlyLockState(PgBackend *backend)
{
	PgBackendLWLockHandle *early_held_lwlocks;

	Assert(backend != NULL);
	Assert(early_backend_locks.strong_lock_in_progress == NULL);
	Assert(early_backend_locks.awaited_lock == NULL);
	Assert(early_backend_locks.awaited_owner == NULL);
	Assert(early_backend_locks.blocking_autovacuum_proc == NULL);

	early_held_lwlocks = early_backend_locks.held_lwlocks_array;
	backend->locks = early_backend_locks;
	if (early_held_lwlocks == NULL ||
		early_held_lwlocks == early_backend_locks.held_lwlocks_inline)
		backend->locks.held_lwlocks_array =
			backend->locks.held_lwlocks_inline;
	else
		backend->locks.held_lwlocks_array = early_held_lwlocks;
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

	if (wait_state->wait_event_info_ptr == NULL)
		wait_state->wait_event_info_ptr = &wait_state->local_wait_event_info;
}

void
PgBackendInitializeWaitState(PgBackendWaitState *wait_state)
{
	Assert(wait_state != NULL);

	MemSet(wait_state, 0, sizeof(*wait_state));
	PgWaitCompletionInitialize(&wait_state->completion);
	wait_state->wait_event_info_ptr = &wait_state->local_wait_event_info;
	pg_atomic_init_u32(&wait_state->waiting, 0);
}

static void
PgBackendInitializeProtocolParkState(PgBackendProtocolParkState *protocol_park)
{
	Assert(protocol_park != NULL);

	MemSet(protocol_park, 0, sizeof(*protocol_park));
	dlist_node_init(&protocol_park->scheduler_node);
	protocol_park->scheduler_queue_state =
		PG_PROTOCOL_SCHEDULER_QUEUE_NONE;
}

static void
PgWaitCompletionInitialize(PgWaitCompletion *completion)
{
	Assert(completion != NULL);

	MemSet(completion, 0, sizeof(*completion));
	pg_atomic_init_u32(&completion->state, PG_WAIT_COMPLETION_INACTIVE);
	pg_atomic_init_u32(&completion->ready_events, 0);
	pg_atomic_init_u32(&completion->interrupt_events, 0);
}

#ifdef PG_RUNTIME_ENABLE_WAIT_COMPLETION_PUBLICATION
static void
PgWaitCompletionPublish(PgWaitCompletion *completion, PgBackend *backend,
						const PgWaitSpec *wait_spec)
{
	PgBackendInterruptMask pending_interrupts;
	uint32		interrupt_events = 0;

	Assert(completion != NULL);
	Assert(backend != NULL);
	Assert(wait_spec != NULL);

	pending_interrupts =
		pg_atomic_read_u32(&backend->interrupts.pending_mask);
	if (pending_interrupts &
		PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_QUERY_CANCEL))
		interrupt_events |= PG_WAIT_COMPLETION_INTERRUPT_CANCEL;
	if (pending_interrupts &
		PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_PROC_DIE))
		interrupt_events |= PG_WAIT_COMPLETION_INTERRUPT_TERMINATE;

	completion->spec = *wait_spec;
	completion->backend = backend;
	completion->session = CurrentPgSession;
	completion->execution = CurrentPgExecution;
	pg_atomic_write_u32(&completion->ready_events, 0);
	pg_atomic_write_u32(&completion->interrupt_events, interrupt_events);
	pg_atomic_write_membarrier_u32(&completion->state,
								   PG_WAIT_COMPLETION_WAITING);
}

static void
PgWaitCompletionClear(PgWaitCompletion *completion)
{
	Assert(completion != NULL);

	MemSet(&completion->spec, 0, sizeof(completion->spec));
	completion->backend = NULL;
	completion->session = NULL;
	completion->execution = NULL;
	pg_atomic_write_u32(&completion->ready_events, 0);
	pg_atomic_write_u32(&completion->interrupt_events, 0);
	pg_atomic_write_u32(&completion->state, PG_WAIT_COMPLETION_INACTIVE);
}

static void
PgBackendClearWaitCompletion(PgBackendWaitState *wait_state)
{
	Assert(wait_state != NULL);

	pg_atomic_write_u32(&wait_state->waiting, 0);
	MemSet(&wait_state->spec, 0, sizeof(wait_state->spec));
	PgWaitCompletionClear(&wait_state->completion);
}
#endif

static void
PgBackendAdoptEarlyWaitState(PgBackend *backend)
{
	Assert(backend != NULL);

	PgBackendEnsureWaitStateInitialized(&early_backend_wait_state);
	backend->wait_state = early_backend_wait_state;

	if (backend->wait_state.wait_event_info_ptr ==
		&early_backend_wait_state.local_wait_event_info)
		backend->wait_state.wait_event_info_ptr =
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

void
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

	PgBackendRegisterThreadedBackend(backend);
}

void
PgBackendResetEarlyFallbackAfterFork(int proc_pid)
{
	int			saved_num_external_fds;

	/*
	 * A forked child INHERITS the postmaster's open external file descriptors
	 * across fork() -- notably the postmaster-death-watch pipe and the
	 * postmaster's WaitEventSet (pm_wait_set) epoll fd -- and immediately
	 * closes+releases them in ClosePostmasterPorts(), each calling
	 * ReleaseExternalFD().  fd.c's numExternalFDs counter lives in this early
	 * fallback backend's storage bucket; in upstream it is a plain process
	 * global that survives fork, so the inherited count matches those releases.
	 * Re-initializing the fallback here would zero it, making those very first
	 * child releases underflow (Assert(numExternalFDs > 0) in fd.c -- a
	 * cassert child-startup abort).  Preserve the inherited count across the
	 * reset so ClosePostmasterPorts() balances, exactly as the process-global
	 * counter did.
	 */
	saved_num_external_fds = early_backend_fallback.storage.num_external_fds;

	PgBackendInitializeRuntimeObject(&early_backend_fallback, NULL, NULL,
								 NULL, NULL, NULL, B_INVALID, NULL);
	early_backend_core.proc_pid = proc_pid;
	early_backend_fallback.storage.num_external_fds = saved_num_external_fds;
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
	PG_RUNTIME_RETURN_CURRENT_BACKEND_BUCKET(CurrentPgBackendMemoryManagerRuntimeState,
											 memory_manager,
											 early_backend_memory_manager);
}

PgBackendUtilityState *
PgCurrentBackendUtilityState(void)
{
	PG_RUNTIME_RETURN_CURRENT_BACKEND_BUCKET(CurrentPgBackendUtilityRuntimeState,
											 utility,
											 early_backend_utility);
}

PgBackendParallelState *
PgCurrentBackendParallelState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_parallel;

	return &CurrentPgBackend->parallel;
}


PgBackendCoreState *
PgCurrentCoreState(void)
{
	PG_RUNTIME_RETURN_CURRENT_BACKEND_BUCKET(CurrentPgBackendCoreRuntimeState,
											 core,
											 early_backend_core);
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
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendCoreRuntimeState, PgCurrentCoreState)->global_prng_state;
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
	PG_RUNTIME_RETURN_CURRENT_BACKEND_BUCKET(CurrentPgBackendPgStatPendingRuntimeState,
											 pgstat_pending,
											 early_backend_pgstat_pending);
}

PgBackendInstrumentationState *
PgCurrentBackendInstrumentationState(void)
{
	PG_RUNTIME_RETURN_CURRENT_BACKEND_BUCKET(CurrentPgBackendInstrumentationRuntimeState,
											 instrumentation,
											 early_backend_instrumentation);
}

PgBackendBufferState *
PgCurrentBackendBufferState(void)
{
	PG_RUNTIME_RETURN_CURRENT_BACKEND_BUCKET(CurrentPgBackendBufferRuntimeState,
											 buffers,
											 early_backend_buffers);
}

MemoryContext
PgBackendBufferAllocationContext(void)
{
	PgBackendBufferState *buffers = PgCurrentBackendBufferState();

	if (TopMemoryContext != NULL)
		return PgRuntimeGetOwnedMemoryContextWithSizes(&buffers->buffer_context,
													   "BackendBufferContext",
													   ALLOCSET_START_SMALL_SIZES);
	if (CurrentMemoryContext != NULL)
		return CurrentMemoryContext;

	elog(ERROR, "cannot allocate backend buffer state before memory contexts exist");
	return NULL;				/* keep compiler quiet */
}

PgBackendStorageState *
PgCurrentBackendStorageState(void)
{
	PG_RUNTIME_RETURN_CURRENT_BACKEND_BUCKET(CurrentPgBackendStorageRuntimeState,
											 storage,
											 early_backend_storage);
}

PgBackendLockState *
PgCurrentBackendLockState(void)
{
	PgBackend  *backend;
	PgBackendLockState *locks;

	locks = CurrentPgBackendLockRuntimeState;
	if (unlikely(locks == NULL))
	{
		backend = CurrentPgBackend;
		locks = backend == NULL ? &early_backend_locks : &backend->locks;
	}
	if (unlikely(locks->held_lwlocks_array == NULL ||
				 locks->held_lwlocks_capacity <= 0))
	{
		locks->held_lwlocks_array = locks->held_lwlocks_inline;
		locks->held_lwlocks_capacity = PG_BACKEND_MAX_INLINE_LWLOCKS;
	}

	return locks;
}

PgBackendIPCState *
PgCurrentBackendIPCState(void)
{
	PG_RUNTIME_RETURN_CURRENT_BACKEND_BUCKET(CurrentPgBackendIPCRuntimeState,
											 ipc,
											 early_backend_ipc);
}

PgBackendWaitState *
PgCurrentBackendWaitState(void)
{
	if (likely(CurrentPgBackendWaitRuntimeState != NULL))
	{
		PgBackendEnsureWaitStateInitialized(CurrentPgBackendWaitRuntimeState);
		return CurrentPgBackendWaitRuntimeState;
	}

	if (CurrentPgBackend == NULL)
	{
		PgBackendEnsureWaitStateInitialized(&early_backend_wait_state);
		return &early_backend_wait_state;
	}

	PgBackendEnsureWaitStateInitialized(&CurrentPgBackend->wait_state);
	return &CurrentPgBackend->wait_state;
}

#ifdef PG_RUNTIME_ENABLE_WAIT_COMPLETION_PUBLICATION
/*
 * Thread-per-session backends publish wait completions automatically.  This
 * override exists for focused tests and diagnostics that need to observe the
 * publication path without constructing a threaded runtime object.
 */
static PG_GLOBAL_RUNTIME bool pg_runtime_publish_wait_specs = false;
#endif

bool
PgSetWaitCompletionPublication(bool enabled)
{
#ifdef PG_RUNTIME_ENABLE_WAIT_COMPLETION_PUBLICATION
	bool		previous = pg_runtime_publish_wait_specs;

	pg_runtime_publish_wait_specs = enabled;
	return previous;
#else
	(void) enabled;
	return false;
#endif
}

PgWaitCompletion *
PgBackendCurrentWaitCompletion(PgBackend *backend)
{
	if (backend == NULL)
		return NULL;

	return &backend->wait_state.completion;
}

bool
PgBackendSnapshotWaitCompletionById(PgBackendId backend_id,
									PgWaitCompletion *snapshot,
									uint32 *waiting)
{
#if defined(PG_RUNTIME_ENABLE_WAIT_COMPLETION_PUBLICATION) && !defined(WIN32)
	PgBackend  *backend = NULL;
	bool		found = false;

	if (snapshot != NULL)
		PgWaitCompletionInitialize(snapshot);
	if (waiting != NULL)
		*waiting = 0;

	ThreadedBackendRegistryLock();
	backend = ThreadedBackendRegistryLookupLocked(backend_id);

	if (backend != NULL)
	{
		PgWaitCompletion *completion = &backend->wait_state.completion;

		if (snapshot != NULL)
		{
			snapshot->spec = completion->spec;
			snapshot->backend = completion->backend;
			snapshot->session = completion->session;
			snapshot->execution = completion->execution;
			pg_atomic_init_u32(&snapshot->state,
							   pg_atomic_read_u32(&completion->state));
			pg_atomic_init_u32(&snapshot->ready_events,
							   pg_atomic_read_u32(&completion->ready_events));
			pg_atomic_init_u32(&snapshot->interrupt_events,
							   pg_atomic_read_u32(&completion->interrupt_events));
		}
		if (waiting != NULL)
			*waiting = pg_atomic_read_u32(&backend->wait_state.waiting);
		found = true;
	}
	ThreadedBackendRegistryUnlock();

	return found;
#else
	(void) backend_id;
	(void) snapshot;
	(void) waiting;
	return false;
#endif
}

void
PgBackendMarkWaitCompletionInterrupt(PgBackend *backend,
									 PgWaitCompletionInterrupt interrupt)
{
#ifdef PG_RUNTIME_ENABLE_WAIT_COMPLETION_PUBLICATION
	PgWaitCompletion *completion;
	uint32		state;

	if (backend == NULL)
		return;

	completion = &backend->wait_state.completion;
	state = pg_atomic_read_u32(&completion->state);
	if (state != PG_WAIT_COMPLETION_WAITING &&
		state != PG_WAIT_COMPLETION_READY)
		return;

	pg_atomic_fetch_or_u32(&completion->interrupt_events, interrupt);
#else
	(void) backend;
	(void) interrupt;
#endif
}

bool
PgBackendWakeWaitCompletion(PgBackend *backend, uint32 ready_events)
{
#ifdef PG_RUNTIME_ENABLE_WAIT_COMPLETION_PUBLICATION
	PgWaitCompletion *completion;
	uint32		state;

	if (backend == NULL)
		return false;

	completion = &backend->wait_state.completion;
	state = pg_atomic_read_u32(&completion->state);
	if (state != PG_WAIT_COMPLETION_WAITING &&
		state != PG_WAIT_COMPLETION_READY)
		return false;

	pg_atomic_fetch_or_u32(&completion->ready_events, ready_events);
	pg_atomic_write_membarrier_u32(&completion->state,
								   PG_WAIT_COMPLETION_READY);
	PgBackendWakeForWaitCompletion(backend);

	return true;
#else
	(void) backend;
	(void) ready_events;
	return false;
#endif
}

bool
PgBackendWakeWaitCompletionById(PgBackendId backend_id, uint32 ready_events)
{
#if defined(PG_RUNTIME_ENABLE_WAIT_COMPLETION_PUBLICATION) && !defined(WIN32)
	PgBackend  *backend = NULL;

	ThreadedBackendRegistryLock();
	backend = ThreadedBackendRegistryLookupLocked(backend_id);

	if (backend != NULL)
		(void) PgBackendWakeWaitCompletion(backend, ready_events);
	ThreadedBackendRegistryUnlock();

	return backend != NULL;
#else
	(void) backend_id;
	(void) ready_events;
	return false;
#endif
}

void
PgRuntimeInitializeProtocolScheduler(PgProtocolSchedulerState *scheduler)
{
	Assert(scheduler != NULL);

	MemSet(scheduler, 0, sizeof(*scheduler));
	SpinLockInit(&scheduler->lock);
	dlist_init(&scheduler->runnable_queue);
	dlist_init(&scheduler->parked_protocol_queue);
	scheduler->carrier_limit = PgRuntimePooledProtocolCarrierLimit();
}

bool
PgRuntimeProtocolSchedulerRegisterCarrier(PgRuntime *runtime, PgCarrier *carrier)
{
	PgProtocolSchedulerState *scheduler;
	bool		carrier_idle;

	if (runtime == NULL || carrier == NULL)
		return false;
	if (carrier->runtime != NULL && carrier->runtime != runtime)
		return false;

	scheduler = &runtime->protocol_scheduler;
	SpinLockAcquire(&scheduler->lock);
	if (carrier->protocol_scheduler_registered)
	{
		bool		ok = carrier->runtime == runtime;

		SpinLockRelease(&scheduler->lock);
		return ok;
	}

	if (scheduler->carrier_limit > 0 &&
		scheduler->registered_carrier_count >= scheduler->carrier_limit)
	{
		scheduler->carrier_reject_count++;
		SpinLockRelease(&scheduler->lock);
		return false;
	}

	carrier_idle = carrier->current_backend == NULL &&
		carrier->current_session == NULL &&
		carrier->current_execution == NULL;
	carrier->runtime = runtime;
	carrier->protocol_scheduler_registered = true;
	carrier->protocol_scheduler_idle = carrier_idle;
	scheduler->registered_carrier_count++;
	if (carrier_idle)
		scheduler->idle_carrier_count++;
	else
		scheduler->active_carrier_count++;
	scheduler->carrier_register_count++;
	SpinLockRelease(&scheduler->lock);

	return true;
}

bool
PgRuntimeProtocolSchedulerUnregisterCarrier(PgRuntime *runtime, PgCarrier *carrier)
{
	PgProtocolSchedulerState *scheduler;

	if (runtime == NULL || carrier == NULL)
		return false;
	if (carrier->runtime != runtime)
		return false;

	scheduler = &runtime->protocol_scheduler;
	SpinLockAcquire(&scheduler->lock);
	if (!carrier->protocol_scheduler_registered)
	{
		SpinLockRelease(&scheduler->lock);
		return false;
	}

	Assert(scheduler->registered_carrier_count > 0);
	scheduler->registered_carrier_count--;
	if (carrier->protocol_scheduler_idle)
	{
		Assert(scheduler->idle_carrier_count > 0);
		scheduler->idle_carrier_count--;
	}
	else
	{
		Assert(scheduler->active_carrier_count > 0);
		scheduler->active_carrier_count--;
	}
	carrier->protocol_scheduler_registered = false;
	carrier->protocol_scheduler_idle = false;
	SpinLockRelease(&scheduler->lock);

	return true;
}

void
PgRuntimeProtocolSchedulerCarrierBecameActive(PgCarrier *carrier)
{
	PgProtocolSchedulerState *scheduler;

	if (carrier == NULL || !carrier->protocol_scheduler_registered)
		return;
	Assert(carrier->runtime != NULL);

	scheduler = &carrier->runtime->protocol_scheduler;
	SpinLockAcquire(&scheduler->lock);
	if (carrier->protocol_scheduler_idle)
	{
		Assert(scheduler->idle_carrier_count > 0);
		scheduler->idle_carrier_count--;
		scheduler->active_carrier_count++;
		carrier->protocol_scheduler_idle = false;
	}
	SpinLockRelease(&scheduler->lock);
}

void
PgRuntimeProtocolSchedulerCarrierBecameIdle(PgCarrier *carrier)
{
	PgProtocolSchedulerState *scheduler;

	if (carrier == NULL || !carrier->protocol_scheduler_registered)
		return;
	Assert(carrier->runtime != NULL);

	scheduler = &carrier->runtime->protocol_scheduler;
	SpinLockAcquire(&scheduler->lock);
	if (!carrier->protocol_scheduler_idle)
	{
		Assert(scheduler->active_carrier_count > 0);
		scheduler->active_carrier_count--;
		scheduler->idle_carrier_count++;
		scheduler->carrier_release_count++;
		carrier->protocol_scheduler_idle = true;
	}
	SpinLockRelease(&scheduler->lock);
}

bool
PgRuntimeProtocolSchedulerParkBackend(PgRuntime *runtime, PgBackend *backend)
{
	PgBackendProtocolParkState *park_state;
	PgProtocolSchedulerState *scheduler;

	if (runtime == NULL || backend == NULL)
		return false;
	if (backend->runtime != runtime)
		return false;
	if (backend->carrier != NULL ||
		backend->execution == NULL ||
		backend->execution->carrier != NULL)
		return false;

	park_state = &backend->protocol_park;
	scheduler = &runtime->protocol_scheduler;
	SpinLockAcquire(&scheduler->lock);
	if (park_state->state != PG_PROTOCOL_PARK_COMMITTED)
	{
		SpinLockRelease(&scheduler->lock);
		return false;
	}
	if (park_state->scheduler_queue_state !=
		PG_PROTOCOL_SCHEDULER_QUEUE_NONE)
	{
		SpinLockRelease(&scheduler->lock);
		return false;
	}

	dlist_push_tail(&scheduler->parked_protocol_queue,
					&park_state->scheduler_node);
	park_state->scheduler_queue_state =
		PG_PROTOCOL_SCHEDULER_QUEUE_PARKED_PROTOCOL_READ;
	scheduler->parked_protocol_count++;
	scheduler->parked_protocol_enqueue_count++;
	SpinLockRelease(&scheduler->lock);

	return true;
}

bool
PgRuntimeProtocolSchedulerMarkRunnable(PgRuntime *runtime, PgBackend *backend)
{
	PgBackendProtocolParkState *park_state;
	PgProtocolSchedulerState *scheduler;

	if (runtime == NULL || backend == NULL)
		return false;
	if (backend->runtime != runtime)
		return false;

	park_state = &backend->protocol_park;
	scheduler = &runtime->protocol_scheduler;
	SpinLockAcquire(&scheduler->lock);

	if (park_state->scheduler_queue_state ==
		PG_PROTOCOL_SCHEDULER_QUEUE_RUNNABLE)
	{
		SpinLockRelease(&scheduler->lock);
		return true;
	}

	if (park_state->scheduler_queue_state ==
		PG_PROTOCOL_SCHEDULER_QUEUE_PARKED_PROTOCOL_READ)
	{
		Assert(scheduler->parked_protocol_count > 0);
		dlist_delete(&park_state->scheduler_node);
		scheduler->parked_protocol_count--;
	}
	else if (park_state->scheduler_queue_state ==
			 PG_PROTOCOL_SCHEDULER_QUEUE_POLLING)
	{
		/* Already removed from the parked list by the polling carrier. */
	}
	else
	{
		SpinLockRelease(&scheduler->lock);
		return false;
	}

	if (park_state->state != PG_PROTOCOL_PARK_COMMITTED)
	{
		SpinLockRelease(&scheduler->lock);
		return false;
	}

	dlist_push_tail(&scheduler->runnable_queue,
					&park_state->scheduler_node);
	park_state->scheduler_queue_state =
		PG_PROTOCOL_SCHEDULER_QUEUE_RUNNABLE;
	scheduler->runnable_count++;
	scheduler->runnable_enqueue_count++;
	SpinLockRelease(&scheduler->lock);

	return true;
}

bool
PgRuntimeProtocolSchedulerLeaseBackend(PgRuntime *runtime, PgBackend *backend)
{
	PgBackendProtocolParkState *park_state;
	PgProtocolSchedulerState *scheduler;

	if (runtime == NULL || backend == NULL)
		return false;
	if (backend->runtime != runtime)
		return false;

	park_state = &backend->protocol_park;
	scheduler = &runtime->protocol_scheduler;
	SpinLockAcquire(&scheduler->lock);

	if (park_state->state != PG_PROTOCOL_PARK_COMMITTED)
	{
		SpinLockRelease(&scheduler->lock);
		return false;
	}

	switch (park_state->scheduler_queue_state)
	{
		case PG_PROTOCOL_SCHEDULER_QUEUE_PARKED_PROTOCOL_READ:
			Assert(scheduler->parked_protocol_count > 0);
			dlist_delete(&park_state->scheduler_node);
			scheduler->parked_protocol_count--;
			break;

		case PG_PROTOCOL_SCHEDULER_QUEUE_RUNNABLE:
			Assert(scheduler->runnable_count > 0);
			dlist_delete(&park_state->scheduler_node);
			scheduler->runnable_count--;
			break;

		case PG_PROTOCOL_SCHEDULER_QUEUE_NONE:
		case PG_PROTOCOL_SCHEDULER_QUEUE_POLLING:
		case PG_PROTOCOL_SCHEDULER_QUEUE_LEASED:
			SpinLockRelease(&scheduler->lock);
			return false;
	}

	park_state->scheduler_queue_state =
		PG_PROTOCOL_SCHEDULER_QUEUE_LEASED;
	SpinLockRelease(&scheduler->lock);

	return true;
}

PgBackend *
PgRuntimeProtocolSchedulerLeaseParkedBackend(PgRuntime *runtime)
{
	PgProtocolSchedulerState *scheduler;
	PgBackend  *backend;
	dlist_node *node;

	if (runtime == NULL)
		return NULL;

	scheduler = &runtime->protocol_scheduler;
	SpinLockAcquire(&scheduler->lock);
	if (dlist_is_empty(&scheduler->parked_protocol_queue))
	{
		SpinLockRelease(&scheduler->lock);
		return NULL;
	}

	Assert(scheduler->parked_protocol_count > 0);
	node = dlist_pop_head_node(&scheduler->parked_protocol_queue);
	backend = dlist_container(PgBackend, protocol_park.scheduler_node, node);
	Assert(backend->protocol_park.state == PG_PROTOCOL_PARK_COMMITTED);
	Assert(backend->protocol_park.scheduler_queue_state ==
		   PG_PROTOCOL_SCHEDULER_QUEUE_PARKED_PROTOCOL_READ);

	backend->protocol_park.scheduler_queue_state =
		PG_PROTOCOL_SCHEDULER_QUEUE_POLLING;
	scheduler->parked_protocol_count--;
	SpinLockRelease(&scheduler->lock);

	return backend;
}

bool
PgRuntimeProtocolSchedulerReparkBackend(PgRuntime *runtime, PgBackend *backend)
{
	PgBackendProtocolParkState *park_state;
	PgProtocolSchedulerState *scheduler;

	if (runtime == NULL || backend == NULL)
		return false;
	if (backend->runtime != runtime)
		return false;

	park_state = &backend->protocol_park;
	scheduler = &runtime->protocol_scheduler;
	SpinLockAcquire(&scheduler->lock);

	if (park_state->state != PG_PROTOCOL_PARK_COMMITTED ||
		park_state->scheduler_queue_state != PG_PROTOCOL_SCHEDULER_QUEUE_POLLING)
	{
		SpinLockRelease(&scheduler->lock);
		return false;
	}

	dlist_push_tail(&scheduler->parked_protocol_queue,
					&park_state->scheduler_node);
	park_state->scheduler_queue_state =
		PG_PROTOCOL_SCHEDULER_QUEUE_PARKED_PROTOCOL_READ;
	scheduler->parked_protocol_count++;
	SpinLockRelease(&scheduler->lock);

	return true;
}

bool
PgRuntimeProtocolSchedulerReparkBackendIfPolling(PgRuntime *runtime,
												 PgBackend *backend)
{
	PgBackendProtocolParkState *park_state;
	PgProtocolSchedulerState *scheduler;

	if (runtime == NULL || backend == NULL)
		return false;
	if (backend->runtime != runtime)
		return false;

	park_state = &backend->protocol_park;
	scheduler = &runtime->protocol_scheduler;
	SpinLockAcquire(&scheduler->lock);

	if (park_state->state == PG_PROTOCOL_PARK_COMMITTED &&
		park_state->scheduler_queue_state ==
		PG_PROTOCOL_SCHEDULER_QUEUE_POLLING)
	{
		dlist_push_tail(&scheduler->parked_protocol_queue,
						&park_state->scheduler_node);
		park_state->scheduler_queue_state =
			PG_PROTOCOL_SCHEDULER_QUEUE_PARKED_PROTOCOL_READ;
		scheduler->parked_protocol_count++;
	}

	SpinLockRelease(&scheduler->lock);

	return true;
}

PgBackend *
PgRuntimeProtocolSchedulerPopRunnable(PgRuntime *runtime)
{
	PgProtocolSchedulerState *scheduler;
	PgBackend  *backend;
	dlist_node *node;

	if (runtime == NULL)
		return NULL;

	scheduler = &runtime->protocol_scheduler;
	SpinLockAcquire(&scheduler->lock);
	if (dlist_is_empty(&scheduler->runnable_queue))
	{
		SpinLockRelease(&scheduler->lock);
		return NULL;
	}

	Assert(scheduler->runnable_count > 0);
	node = dlist_pop_head_node(&scheduler->runnable_queue);
	backend = dlist_container(PgBackend, protocol_park.scheduler_node, node);
	Assert(backend->protocol_park.scheduler_queue_state ==
		   PG_PROTOCOL_SCHEDULER_QUEUE_RUNNABLE);

	backend->protocol_park.scheduler_queue_state =
		PG_PROTOCOL_SCHEDULER_QUEUE_LEASED;
	scheduler->runnable_count--;
	SpinLockRelease(&scheduler->lock);

	return backend;
}

int
PgRuntimeProtocolSchedulerCollectParked(PgRuntime *runtime,
										PgBackend **backends,
										int max_backends)
{
	PgProtocolSchedulerState *scheduler;
	dlist_iter	iter;
	int			nbackends = 0;

	if (runtime == NULL || backends == NULL || max_backends <= 0)
		return 0;

	scheduler = &runtime->protocol_scheduler;
	SpinLockAcquire(&scheduler->lock);
	dlist_foreach(iter, &scheduler->parked_protocol_queue)
	{
		PgBackend  *backend;

		if (nbackends >= max_backends)
			break;

		backend = dlist_container(PgBackend, protocol_park.scheduler_node,
								  iter.cur);
		Assert(backend->protocol_park.state == PG_PROTOCOL_PARK_COMMITTED);
		Assert(backend->protocol_park.scheduler_queue_state ==
			   PG_PROTOCOL_SCHEDULER_QUEUE_PARKED_PROTOCOL_READ);
		backends[nbackends++] = backend;
	}
	SpinLockRelease(&scheduler->lock);

	return nbackends;
}

PgBackend *
PgCarrierLeaseRunnableProtocolBackend(PgCarrier *carrier)
{
	PgRuntime  *runtime;
	PgBackend  *backend;

	Assert(carrier != NULL);
	Assert(carrier == CurrentPgCarrier);
	Assert(carrier->current_backend == NULL);
	Assert(carrier->current_session == NULL);
	Assert(carrier->current_execution == NULL);

	if (!carrier->protocol_scheduler_registered)
		return NULL;

	runtime = carrier->runtime;
	if (runtime == NULL)
		return NULL;

	backend = PgRuntimeProtocolSchedulerPopRunnable(runtime);
	if (backend == NULL)
		return NULL;

	if (backend->runtime != runtime ||
		backend->carrier != NULL ||
		backend->session == NULL ||
		backend->connection == NULL ||
		backend->execution == NULL ||
		backend->execution->carrier != NULL ||
		backend->protocol_park.state != PG_PROTOCOL_PARK_COMMITTED ||
		backend->protocol_park.scheduler_queue_state !=
		PG_PROTOCOL_SCHEDULER_QUEUE_LEASED)
		elog(PANIC, "cannot lease inconsistent protocol scheduler backend: runtime_match=%d carrier=%p session=%p connection=%p execution=%p execution_carrier=%p park_state=%d queue_state=%d",
			 backend->runtime == runtime,
			 backend->carrier,
			 backend->session,
			 backend->connection,
			 backend->execution,
			 backend->execution != NULL ? backend->execution->carrier : NULL,
			 backend->protocol_park.state,
			 backend->protocol_park.scheduler_queue_state);

	PgCarrierAttachBackend(carrier, backend, backend->session,
						   backend->connection, backend->execution);
	SpinLockAcquire(&runtime->protocol_scheduler.lock);
	runtime->protocol_scheduler.carrier_lease_count++;
	SpinLockRelease(&runtime->protocol_scheduler.lock);
	return backend;
}

bool
PgRuntimeProtocolSchedulerRemoveBackend(PgRuntime *runtime, PgBackend *backend)
{
	PgBackendProtocolParkState *park_state;
	PgProtocolSchedulerState *scheduler;

	if (runtime == NULL || backend == NULL)
		return false;
	if (backend->runtime != runtime)
		return false;

	park_state = &backend->protocol_park;
	scheduler = &runtime->protocol_scheduler;
	SpinLockAcquire(&scheduler->lock);

	switch (park_state->scheduler_queue_state)
	{
		case PG_PROTOCOL_SCHEDULER_QUEUE_NONE:
			SpinLockRelease(&scheduler->lock);
			return false;

		case PG_PROTOCOL_SCHEDULER_QUEUE_PARKED_PROTOCOL_READ:
			Assert(scheduler->parked_protocol_count > 0);
			dlist_delete(&park_state->scheduler_node);
			scheduler->parked_protocol_count--;
			break;

		case PG_PROTOCOL_SCHEDULER_QUEUE_POLLING:
			break;

		case PG_PROTOCOL_SCHEDULER_QUEUE_RUNNABLE:
			Assert(scheduler->runnable_count > 0);
			dlist_delete(&park_state->scheduler_node);
			scheduler->runnable_count--;
			break;

		case PG_PROTOCOL_SCHEDULER_QUEUE_LEASED:
			break;
	}

	park_state->scheduler_queue_state =
		PG_PROTOCOL_SCHEDULER_QUEUE_NONE;
	SpinLockRelease(&scheduler->lock);
	return true;
}

bool
PgBackendSnapshotProtocolParkById(PgBackendId backend_id,
								  PgProtocolParkSnapshot *snapshot)
{
#ifndef WIN32
	PgBackend  *backend = NULL;
	bool		found = false;

	if (snapshot != NULL)
		MemSet(snapshot, 0, sizeof(*snapshot));

	ThreadedBackendRegistryLock();
	backend = ThreadedBackendRegistryLookupLocked(backend_id);

	if (backend != NULL)
	{
		PgRuntime  *runtime = backend->runtime;
		PgProtocolSchedulerState *scheduler;
		PgBackendProtocolParkState *park_state;

		if (runtime != NULL && snapshot != NULL)
		{
			scheduler = &runtime->protocol_scheduler;
			park_state = &backend->protocol_park;

			SpinLockAcquire(&scheduler->lock);
			snapshot->state = park_state->state;
			snapshot->scheduler_queue_state =
				park_state->scheduler_queue_state;
			snapshot->generation = park_state->spec.generation;
			snapshot->wake_reasons = park_state->wake_reasons;
			snapshot->wake_events = park_state->wake_events;
			snapshot->wake_generation = park_state->wake_generation;
			snapshot->last_wake_reasons = park_state->last_wake_reasons;
			snapshot->last_wake_events = park_state->last_wake_events;
			snapshot->last_wake_generation = park_state->last_wake_generation;
			snapshot->notify_wake_generation =
				park_state->notify_wake_generation;
			snapshot->deferred_notify_generation =
				park_state->deferred_notify_generation;
			snapshot->deferred_notify_park_generation =
				park_state->deferred_notify_park_generation;
			snapshot->deferred_notify_reasons =
				park_state->deferred_notify_reasons;
			snapshot->scheduler_runnable_count = scheduler->runnable_count;
			snapshot->scheduler_parked_protocol_count =
				scheduler->parked_protocol_count;
			snapshot->scheduler_runnable_enqueue_count =
				scheduler->runnable_enqueue_count;
			snapshot->scheduler_parked_protocol_enqueue_count =
				scheduler->parked_protocol_enqueue_count;
			snapshot->scheduler_carrier_limit =
				scheduler->carrier_limit;
			snapshot->scheduler_same_carrier_resume_count =
				scheduler->same_carrier_resume_count;
			snapshot->scheduler_migrated_resume_count =
				scheduler->migrated_resume_count;
			snapshot->scheduler_registered_carrier_count =
				scheduler->registered_carrier_count;
			snapshot->scheduler_idle_carrier_count =
				scheduler->idle_carrier_count;
			snapshot->scheduler_active_carrier_count =
				scheduler->active_carrier_count;
			snapshot->last_park_duration_valid =
				park_state->last_park_duration_valid;
			snapshot->last_park_duration_ms =
				park_state->last_park_duration_ms;
			SpinLockRelease(&scheduler->lock);

			snapshot->carrier_attached = backend->carrier != NULL;
			snapshot->session_present = backend->session != NULL;
			snapshot->connection_present = backend->connection != NULL;
			snapshot->execution_present = backend->execution != NULL;
		}
		found = true;
	}
	ThreadedBackendRegistryUnlock();

	return found;
#else
	(void) backend_id;
	(void) snapshot;
	return false;
#endif
}

static void
PgRuntimeProtocolSchedulerRecordResume(PgBackend *backend)
{
	PgProtocolSchedulerState *scheduler;
	PgBackendProtocolParkState *park_state;
	PgCarrier  *resume_carrier;
	PgCarrier  *parked_carrier;

	Assert(backend != NULL);

	if (backend->runtime == NULL)
		return;

	park_state = &backend->protocol_park;
	resume_carrier = backend->carrier;
	parked_carrier = park_state->parked_carrier;
	if (resume_carrier == NULL || parked_carrier == NULL)
		return;

	scheduler = &backend->runtime->protocol_scheduler;
	SpinLockAcquire(&scheduler->lock);
	if (resume_carrier == parked_carrier)
		scheduler->same_carrier_resume_count++;
	else
		scheduler->migrated_resume_count++;
	SpinLockRelease(&scheduler->lock);
}

bool
PgBackendLogicalTimeoutNextWake(PgBackend *backend, TimestampTz *wake_at,
								uint64 *generation)
{
	PgBackendTimeoutState *timeout;

	Assert(backend != NULL);

	timeout = &backend->timeout;
	if (generation != NULL)
		*generation = timeout->generation;

	if (!timeout->all_timeouts_initialized ||
		timeout->signal_delivery ||
		timeout->num_active_timeouts <= 0 ||
		!timeout->alarm_enabled)
		return false;

	if (timeout->active_timeouts[0] == NULL)
		return false;

	if (wake_at != NULL)
		*wake_at = timeout->active_timeouts[0]->fin_time;

	return true;
}

bool
PgBackendPrepareProtocolReadPark(PgBackend *backend, PgProtocolParkSpec *spec)
{
	PgSession  *session;
	PgConnection *connection;
	PgBackendProtocolParkState *park_state;

	Assert(backend != NULL);
	Assert(spec != NULL);
	Assert(backend == CurrentPgBackend);

	session = spec->session != NULL ? spec->session : backend->session;
	connection = spec->connection != NULL ? spec->connection : backend->connection;
	Assert(session != NULL);
	Assert(connection != NULL);
	Assert(session == CurrentPgSession);
	Assert(connection == CurrentPgConnection);
	Assert(session->loop_state.doing_command_read);

	if (!PgConnectionCanParkBeforeMessage(connection))
		return false;
	if (spec->transport_wait_events == 0 && !spec->transport_buffered_input)
		return false;

	park_state = &backend->protocol_park;
	Assert(park_state->state == PG_PROTOCOL_PARK_NONE ||
		   park_state->state == PG_PROTOCOL_PARK_COMMITTED);

	spec->backend = backend;
	spec->session = session;
	spec->connection = connection;
	spec->socket = connection->identity.port != NULL ?
		connection->identity.port->sock : PGINVALID_SOCKET;
	spec->generation = ++park_state->next_generation;
	spec->timeout_wake_at_valid =
		PgBackendLogicalTimeoutNextWake(backend, &spec->timeout_wake_at,
										&spec->timeout_generation);
	if (!spec->timeout_wake_at_valid)
		spec->timeout_generation = backend->timeout.generation;

	park_state->spec = *spec;
	park_state->wake_reasons = PG_PROTOCOL_PARK_WAKE_NONE;
	park_state->wake_events = 0;
	park_state->wake_generation = 0;
	park_state->notify_wake_generation = 0;
	park_state->state = PG_PROTOCOL_PARK_PREPARED;

	return true;
}

void
PgCarrierCommitProtocolReadPark(PgCarrier *carrier, PgBackend *backend)
{
	PgBackendProtocolParkState *park_state;
	PgConnection *connection;

	Assert(carrier != NULL);
	Assert(backend != NULL);
	Assert(carrier == CurrentPgCarrier);
	Assert(backend == CurrentPgBackend);
	Assert(carrier->current_backend == backend);

	park_state = &backend->protocol_park;
	Assert(park_state->state == PG_PROTOCOL_PARK_PREPARED);

	connection = park_state->spec.connection;
	Assert(connection != NULL);
	if (!PgConnectionCanParkBeforeMessage(connection))
		elog(PANIC, "cannot commit protocol read park during active message read");

	park_state->state = PG_PROTOCOL_PARK_COMMITTED;
	park_state->parked_carrier = carrier;
	park_state->committed_at = GetCurrentTimestamp();
	PgCarrierDetachBackend(carrier, backend);
	if (!PgRuntimeProtocolSchedulerParkBackend(carrier->runtime, backend))
		elog(PANIC, "could not enqueue committed protocol read park: backend_runtime_match=%d park_state=%d queue_state=%d carrier=%p backend_carrier=%p",
			 backend->runtime == carrier->runtime,
			 park_state->state,
			 park_state->scheduler_queue_state,
			 carrier,
			 backend->carrier);
}

bool
PgBackendMarkProtocolReadParkWake(PgBackend *backend, uint64 generation,
								  uint32 wake_reasons, uint32 wake_events)
{
	PgBackendProtocolParkState *park_state;

	Assert(backend != NULL);

	park_state = &backend->protocol_park;
	if (park_state->state != PG_PROTOCOL_PARK_COMMITTED)
		return false;
	if (park_state->spec.generation != generation)
		return false;

	park_state->wake_reasons |= wake_reasons;
	park_state->wake_events |= wake_events;
	park_state->wake_generation = generation;
	if (wake_reasons & PG_PROTOCOL_PARK_WAKE_NOTIFY)
		park_state->notify_wake_generation =
			PgBackendNotifyInterruptGeneration(backend);

	return true;
}

bool
PgBackendMarkProtocolReadParkDeferredNotify(PgBackend *backend,
											uint64 notify_generation,
											uint32 wake_reasons)
{
	PgBackendProtocolParkState *park_state;

	Assert(backend != NULL);

	if (notify_generation == 0)
		return false;

	park_state = &backend->protocol_park;
	if (park_state->deferred_notify_generation == notify_generation)
	{
		park_state->deferred_notify_reasons |= wake_reasons;
		return true;
	}

	park_state->deferred_notify_generation = notify_generation;
	park_state->deferred_notify_park_generation =
		park_state->last_wake_generation;
	park_state->deferred_notify_reasons = wake_reasons;

	return true;
}

void
PgBackendClearProtocolReadParkDeferredNotify(PgBackend *backend)
{
	PgBackendProtocolParkState *park_state;

	if (backend == NULL)
		return;

	park_state = &backend->protocol_park;
	park_state->deferred_notify_generation = 0;
	park_state->deferred_notify_park_generation = 0;
	park_state->deferred_notify_reasons = PG_PROTOCOL_PARK_WAKE_NONE;
}

bool
PgBackendProtocolReadParkTimeoutGenerationValid(PgBackend *backend,
												uint64 generation)
{
	PgBackendProtocolParkState *park_state;

	if (backend == NULL)
		return false;

	park_state = &backend->protocol_park;
	if (park_state->state != PG_PROTOCOL_PARK_COMMITTED)
		return false;
	if (park_state->spec.generation != generation)
		return false;

	return park_state->spec.timeout_generation == backend->timeout.generation;
}

void
PgBackendResumeProtocolReadPark(PgBackend *backend)
{
	PgBackendProtocolParkState *park_state;

	Assert(backend != NULL);
	Assert(backend == CurrentPgBackend);

	park_state = &backend->protocol_park;
	Assert(park_state->state == PG_PROTOCOL_PARK_COMMITTED);
	Assert(park_state->wake_generation == 0 ||
		   park_state->wake_generation == park_state->spec.generation);
	Assert(park_state->scheduler_queue_state ==
		   PG_PROTOCOL_SCHEDULER_QUEUE_LEASED);

	PgRuntimeProtocolSchedulerRecordResume(backend);
	if (park_state->committed_at != 0)
	{
		park_state->last_park_duration_ms =
			TimestampDifferenceMilliseconds(park_state->committed_at,
											GetCurrentTimestamp());
		if (park_state->last_park_duration_ms < 0)
			park_state->last_park_duration_ms = 0;
		park_state->last_park_duration_valid = true;
	}
	else
	{
		park_state->last_park_duration_ms = 0;
		park_state->last_park_duration_valid = false;
	}
	park_state->last_wake_reasons = park_state->wake_reasons;
	park_state->last_wake_events = park_state->wake_events;
	park_state->last_wake_generation = park_state->wake_generation;
	park_state->state = PG_PROTOCOL_PARK_NONE;
	MemSet(&park_state->spec, 0, sizeof(park_state->spec));
	park_state->committed_at = 0;
	park_state->wake_reasons = PG_PROTOCOL_PARK_WAKE_NONE;
	park_state->wake_events = 0;
	park_state->wake_generation = 0;
	park_state->hibernated = false;
	park_state->parked_carrier = NULL;
	park_state->scheduler_queue_state = PG_PROTOCOL_SCHEDULER_QUEUE_NONE;
}

#ifdef PG_RUNTIME_ENABLE_WAIT_COMPLETION_PUBLICATION
static void
PgBackendWakeForWaitCompletion(PgBackend *backend)
{
	if (backend == NULL)
		return;

	if (backend->interrupt_latch != NULL)
		SetLatch(backend->interrupt_latch);
	else if (backend == CurrentPgBackend && MyLatch != NULL)
		SetLatch(MyLatch);
}
#endif

bool
PgBackendShouldPublishWaitCompletion(PgBackend *backend)
{
#ifdef PG_RUNTIME_ENABLE_WAIT_COMPLETION_PUBLICATION
	if (backend == NULL)
		return false;
	if (pg_runtime_publish_wait_specs)
		return true;
	if (backend->runtime == NULL)
		return false;

	return PgRuntimeIsThreadBacked(backend->runtime);
#else
	(void) backend;
	return false;
#endif
}

int
PgSuspend(const PgWaitSpec *wait_spec, PgSuspendCallback callback,
		  void *callback_arg)
{
#ifdef PG_RUNTIME_ENABLE_WAIT_COMPLETION_PUBLICATION
	PgBackend  *backend;
	PgBackendWaitState *wait_state;
	int			result = 0;

	Assert(callback != NULL);

	backend = CurrentPgBackend;
	if (likely(wait_spec == NULL ||
			   !PgBackendShouldPublishWaitCompletion(backend)))
		return callback(callback_arg);

	wait_state = &backend->wait_state;
	PgBackendEnsureWaitStateInitialized(wait_state);
	wait_state->spec = *wait_spec;
	PgWaitCompletionPublish(&wait_state->completion, backend, wait_spec);
	pg_atomic_write_membarrier_u32(&wait_state->waiting, 1);

	PG_TRY();
	{
		result = callback(callback_arg);
	}
	PG_CATCH();
	{
		if (backend != NULL)
			PgBackendClearWaitCompletion(&backend->wait_state);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (backend != NULL)
		PgBackendClearWaitCompletion(&backend->wait_state);

	return result;
#else
	Assert(callback != NULL);
	(void) wait_spec;
	return callback(callback_arg);
#endif
}

PgBackendTimeoutState *
PgCurrentTimeoutState(void)
{
	PG_RUNTIME_RETURN_CURRENT_BACKEND_BUCKET(CurrentPgBackendTimeoutRuntimeState,
											 timeout,
											 early_backend_timeout);
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

static PgBackendExtensionPrivateState *
PgBackendFindExtensionPrivateState(PgBackendExtensionModuleState *extension_modules,
								   const char *key)
{
	Assert(extension_modules != NULL);
	Assert(key != NULL);

	foreach_ptr(PgBackendExtensionPrivateState, private_state,
				extension_modules->private_states)
	{
		if (strcmp(private_state->key, key) == 0)
			return private_state;
	}

	return NULL;
}

void *
PgBackendGetExtensionPrivateState(const char *key)
{
	PgBackendExtensionPrivateState *private_state;

	private_state = PgBackendFindExtensionPrivateState(
		PgCurrentBackendExtensionModuleState(), key);

	return private_state != NULL ? private_state->state : NULL;
}

void *
PgBackendEnsureExtensionPrivateState(const char *key, Size size,
									 PgExtensionPrivateStateCleanup cleanup)
{
	PgBackendExtensionModuleState *extension_modules;
	PgBackendExtensionPrivateState *private_state;
	MemoryContext old_context;

	Assert(key != NULL);
	Assert(size > 0);

	extension_modules = PgCurrentBackendExtensionModuleState();
	private_state = PgBackendFindExtensionPrivateState(extension_modules, key);
	if (private_state != NULL)
		return private_state->state;

	old_context = MemoryContextSwitchTo(TopMemoryContext);
	private_state = palloc_object(PgBackendExtensionPrivateState);
	private_state->key = key;
	private_state->state = palloc0(size);
	private_state->cleanup = cleanup;
	extension_modules->private_states =
		lappend(extension_modules->private_states, private_state);
	MemoryContextSwitchTo(old_context);

	return private_state->state;
}

char **
PgCurrentBasicArchiveDirectoryRef(void)
{
	PgBasicArchiveBackendState *state;

	state = (PgBasicArchiveBackendState *)
		PgBackendEnsureExtensionPrivateState(BASIC_ARCHIVE_BACKEND_STATE_KEY,
											 sizeof(PgBasicArchiveBackendState),
											 NULL);
	if (state->archive_directory == NULL)
		state->archive_directory = "";

	return &state->archive_directory;
}

PgBackendTransactionState *
PgCurrentBackendTransactionState(void)
{
	PG_RUNTIME_RETURN_CURRENT_BACKEND_BUCKET(CurrentPgBackendTransactionRuntimeState,
											 transaction,
											 early_backend_transaction);
}

PgBackendPendingInterruptState *
PgCurrentPendingInterrupts(void)
{
	PG_RUNTIME_RETURN_CURRENT_BACKEND_BUCKET(CurrentPgBackendPendingInterruptRuntimeState,
											 pending_interrupts,
											 early_pending_interrupts);
}

PgBackendInterruptHoldoffState *
PgCurrentInterruptHoldoffs(void)
{
	PG_RUNTIME_RETURN_CURRENT_BACKEND_BUCKET(CurrentPgBackendInterruptHoldoffRuntimeState,
											 interrupt_holdoffs,
											 early_interrupt_holdoffs);
}

void
PgBackendInitializeInterrupts(PgBackend *backend)
{
	if (backend == NULL)
		return;

	pg_atomic_init_u32(&backend->interrupts.pending_mask, 0);
	pg_atomic_init_u32(&backend->interrupts.notify_generation, 0);
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

	if (PgRuntimeIsThreadBacked(backend->runtime))
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
