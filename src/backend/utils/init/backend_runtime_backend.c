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
#include "utils/pgstat_internal.h"
#include "utils/resowner.h"

static PG_GLOBAL_RUNTIME bool backend_id_counter_initialized = false;
static PG_GLOBAL_RUNTIME pg_atomic_uint64 next_backend_id;

#ifndef WIN32
static PG_GLOBAL_RUNTIME pthread_mutex_t ThreadedBackendRegistryMutex = PTHREAD_MUTEX_INITIALIZER;
static PG_GLOBAL_RUNTIME PgBackend **ThreadedBackendRegistry = NULL;
static PG_GLOBAL_RUNTIME Size ThreadedBackendRegistryCapacity = 0;
#endif
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

StaticAssertDecl(PG_BACKEND_INTERRUPT_COUNT <= 32,
				 "PgBackendInterruptMask must fit all backend interrupts");

static PgBackendId PgBackendAssignId(void);
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
static void PgWaitCompletionInitialize(PgWaitCompletion *completion);
static void PgWaitCompletionPublish(PgWaitCompletion *completion,
									PgBackend *backend,
									const PgWaitSpec *wait_spec);
static void PgWaitCompletionClear(PgWaitCompletion *completion);
static void PgBackendClearWaitCompletion(PgBackendWaitState *wait_state);
static void PgBackendWakeForWaitCompletion(PgBackend *backend);
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

static void
PgBackendEnsureThreadedRegistryCapacity(PgBackendId backend_id)
{
	Size		new_capacity;
	PgBackend **new_registry;

	if (backend_id < ThreadedBackendRegistryCapacity)
		return;

	new_capacity = ThreadedBackendRegistryCapacity == 0 ?
		128 : ThreadedBackendRegistryCapacity;
	while (backend_id >= new_capacity)
	{
		if (new_capacity > (Size) (PG_UINT64_MAX / 2))
			elog(FATAL, "threaded backend registry capacity overflow");
		new_capacity *= 2;
	}

	new_registry = realloc(ThreadedBackendRegistry,
						   new_capacity * sizeof(PgBackend *));
	if (new_registry == NULL)
		elog(FATAL, "out of memory growing threaded backend registry");

	MemSet(new_registry + ThreadedBackendRegistryCapacity, 0,
		   (new_capacity - ThreadedBackendRegistryCapacity) * sizeof(PgBackend *));
	ThreadedBackendRegistry = new_registry;
	ThreadedBackendRegistryCapacity = new_capacity;
}
#endif

static void
PgBackendRegisterThreadedBackend(PgBackend *backend)
{
#ifndef WIN32
	Assert(backend != NULL);

	if (backend->runtime == NULL ||
		backend->runtime->kind != PG_RUNTIME_THREAD_PER_SESSION)
		return;

	ThreadedBackendRegistryLock();
	PgBackendEnsureThreadedRegistryCapacity(backend->id);
	if (ThreadedBackendRegistry[backend->id] != NULL &&
		ThreadedBackendRegistry[backend->id] != backend)
		elog(FATAL, "threaded backend id %llu is already registered",
			 (unsigned long long) backend->id);
	ThreadedBackendRegistry[backend->id] = backend;
	ThreadedBackendRegistryUnlock();
#endif
}

void
PgBackendUnregisterThreadedBackend(PgBackend *backend)
{
#ifndef WIN32
	Assert(backend != NULL);

	if (backend->runtime == NULL ||
		backend->runtime->kind != PG_RUNTIME_THREAD_PER_SESSION)
		return;

	ThreadedBackendRegistryLock();
	if (backend->id < ThreadedBackendRegistryCapacity &&
		ThreadedBackendRegistry[backend->id] == backend)
		ThreadedBackendRegistry[backend->id] = NULL;
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
	if (backend_id < ThreadedBackendRegistryCapacity)
		backend = ThreadedBackendRegistry[backend_id];

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
PgWaitCompletionInitialize(PgWaitCompletion *completion)
{
	Assert(completion != NULL);

	MemSet(completion, 0, sizeof(*completion));
	pg_atomic_init_u32(&completion->state, PG_WAIT_COMPLETION_INACTIVE);
	pg_atomic_init_u32(&completion->ready_events, 0);
	pg_atomic_init_u32(&completion->interrupt_events, 0);
}

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
	completion->requeue = NULL;
	completion->requeue_arg = NULL;
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
	completion->requeue = NULL;
	completion->requeue_arg = NULL;
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
	PgBackendInitializeRuntimeObject(&early_backend_fallback, NULL, NULL,
								 NULL, NULL, NULL, B_INVALID, NULL);
	early_backend_core.proc_pid = proc_pid;
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
													   ALLOCSET_DEFAULT_SIZES);
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
	PG_RUNTIME_RETURN_CURRENT_BACKEND_BUCKET(CurrentPgBackendLockRuntimeState,
											 locks,
											 early_backend_locks);
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

/*
 * Current process and thread-per-session carriers do not consume published
 * wait specs.  A future scheduler that parks and resumes many sessions on a
 * carrier can enable this owner-local switch when it needs to observe waits.
 */
static PG_GLOBAL_RUNTIME bool pg_runtime_publish_wait_specs = false;

bool
PgSetWaitCompletionPublication(bool enabled)
{
	bool		previous = pg_runtime_publish_wait_specs;

	pg_runtime_publish_wait_specs = enabled;
	return previous;
}

PgWaitCompletion *
PgBackendCurrentWaitCompletion(PgBackend *backend)
{
	if (backend == NULL)
		return NULL;

	return &backend->wait_state.completion;
}

void
PgBackendMarkWaitCompletionInterrupt(PgBackend *backend,
									 PgWaitCompletionInterrupt interrupt)
{
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
}

bool
PgBackendWakeWaitCompletion(PgBackend *backend, uint32 ready_events)
{
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

	if (completion->requeue != NULL)
		completion->requeue(completion, completion->requeue_arg);
	else
		PgBackendWakeForWaitCompletion(backend);

	return true;
}

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

int
PgSuspend(const PgWaitSpec *wait_spec, PgSuspendCallback callback,
		  void *callback_arg)
{
	PgBackend  *backend;
	PgBackendWaitState *wait_state;
	int			result = 0;

	Assert(callback != NULL);

	if (likely(!pg_runtime_publish_wait_specs || wait_spec == NULL))
		return callback(callback_arg);

	backend = CurrentPgBackend;
	if (backend != NULL && wait_spec != NULL)
	{
		wait_state = &backend->wait_state;
		PgBackendEnsureWaitStateInitialized(wait_state);
		wait_state->spec = *wait_spec;
		PgWaitCompletionPublish(&wait_state->completion, backend, wait_spec);
		pg_atomic_write_membarrier_u32(&wait_state->waiting, 1);
	}

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

char **
PgCurrentBasicArchiveDirectoryRef(void)
{
	return &PgCurrentBackendExtensionModuleState()->basic_archive_archive_directory;
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
