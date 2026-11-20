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

#ifdef USE_XTC_CARRIER
#include "xtc.h"					/* XTC_OK, XTC_E_EXIST, XTC_E_NOTFOUND, xtc_pid_t */
#include "xtc_proc.h"				/* XTC_PID_NONE */
#include "xtc_reg.h"				/* xtc_reg_put / xtc_reg_visit (libxtc 2.2) */
#endif

static PG_GLOBAL_RUNTIME bool backend_id_counter_initialized = false;
static PG_GLOBAL_RUNTIME pg_atomic_uint64 next_backend_id;

#if defined(USE_XTC_CARRIER) && !defined(WIN32)
/*
 * Threaded backend registry on libxtc's xtc_reg value API (libxtc 2.2).
 *
 * Maps PgBackendId -> PgBackend* with xtc_reg_put (name -> (pid, value)) and
 * xtc_reg_visit (the lookup+use runs under the registry's own lock).  The name
 * is "backend:<PgBackendId>"; the value is the PgBackend*; the pid slot is DELIBERATELY XTC_PID_NONE -- see
 * PgBackendRegisterThreadedBackend for why our dispatch does not use it.
 *
 * The registry is process-global and created once, lazily, on the first
 * register (pthread_once): register runs on the postmaster/spawning thread
 * (potentially concurrently for parallel spawns), which is not an xtc proc, so
 * a bare xtc_reg_create at an xtc-only init hook would be awkward and the once
 * guard makes the create thread-safe without a new init site.  It is
 * process-lifetime and never destroyed (one bringup per process).
 */
static PG_GLOBAL_RUNTIME xtc_reg_t *g_backends = NULL;
static PG_GLOBAL_RUNTIME pthread_once_t g_backends_once = PTHREAD_ONCE_INIT;

static void
ThreadedBackendRegistryCreate(void)
{
	xtc_reg_t  *reg = NULL;

	if (xtc_reg_create(&reg) != XTC_OK || reg == NULL)
		elog(FATAL, "could not create threaded backend registry");
	g_backends = reg;
}

static xtc_reg_t *
ThreadedBackendRegistry(void)
{
	(void) pthread_once(&g_backends_once, ThreadedBackendRegistryCreate);
	return g_backends;
}

/*
 * Build the registry key for a backend id.  "backend:<id>" per the libxtc
 * reply's section 3 example; PgBackendId is 64-bit so format it as such.
 */
static inline void
ThreadedBackendRegistryName(char *buf, size_t buflen, PgBackendId backend_id)
{
	snprintf(buf, buflen, "backend:%llu", (unsigned long long) backend_id);
}
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
		.remote_ctx.remote_attnum = -1,
		.remote_ctx.remote_xid = InvalidTransactionId,
		.remote_ctx.finish_lsn = InvalidXLogRecPtr,
		.subxact_data.subxact_last = InvalidTransactionId,
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

static void
PgBackendRegisterThreadedBackend(PgBackend *backend)
{
#if defined(USE_XTC_CARRIER) && !defined(WIN32)
	char		name[32];
	int			rc;

	Assert(backend != NULL);

	if (!PgRuntimeIsThreadBacked(backend->runtime))
		return;

	ThreadedBackendRegistryName(name, sizeof(name), backend->id);

	/*
	 * Store name -> (pid, value) with the PgBackend* as the value.  The pid
	 * slot is XTC_PID_NONE on purpose: this runs on the postmaster/spawning
	 * thread (launch_backend.c calls InitializePgThreadBackendRuntimeState
	 * -> here BEFORE the backend fiber is spawned), which is not an xtc proc,
	 * so xtc_self() here would be XTC_PID_NONE anyway and the fiber's real pid
	 * does not exist yet.  It does not matter: our interrupt dispatch
	 * (PgBackendSendInterruptById's visit callback) wakes the target through
	 * the PgBackend struct -- SendInterrupt() reads backend->my_proc's
	 * interrupt-wake eventfd and backend->interrupt_latch -- NOT via
	 * xtc_proc_wake(pid).  The registry pid is unused by our dispatch, so an
	 * XTC_PID_NONE pid is correct rather than a stale/wrong one.  (This is the
	 * xtc_self()-context answer from the libxtc 2.2 adoption: we deliberately
	 * do not use the pid slot.)
	 *
	 * flags 0 means a unique name; a taken name returns XTC_E_EXIST.  A unique
	 * PgBackendId should never collide, so treat a duplicate as the
	 * "already registered" FATAL.
	 */
	rc = xtc_reg_put(ThreadedBackendRegistry(), name, XTC_PID_NONE,
					 backend, 0);
	if (rc == XTC_E_EXIST)
		elog(FATAL, "threaded backend id %llu is already registered",
			 (unsigned long long) backend->id);
	else if (rc != XTC_OK)
		elog(FATAL, "could not register threaded backend id %llu (xtc_reg_put rc=%d)",
			 (unsigned long long) backend->id, rc);
#endif
}

void
PgBackendUnregisterThreadedBackend(PgBackend *backend)
{
#if defined(USE_XTC_CARRIER) && !defined(WIN32)
	char		name[32];

	Assert(backend != NULL);

	if (!PgRuntimeIsThreadBacked(backend->runtime))
		return;

	/*
	 * Unregister BEFORE the PgBackend struct can be freed (libxtc's rule:
	 * remove the entry, then free the value, so a concurrent xtc_reg_visit
	 * cannot see a freed value).  PgBackendResetClosedState() calls us first,
	 * before any bucket reset, and it runs inside the fiber's proc_exit
	 * (ipc.c shmem_exit) -- strictly before backend_thread_start_release()
	 * free()s the BackendThreadStart that embeds this PgBackend.  Missing-name
	 * (xtc_reg_del returns XTC_E_NOTFOUND) is fine: a backend that never
	 * registered (not thread-backed, or a double teardown) is a no-op
	 * unregister.
	 */
	ThreadedBackendRegistryName(name, sizeof(name), backend->id);
	(void) xtc_reg_del(ThreadedBackendRegistry(), name, XTC_PID_NONE);
#endif
}

#if defined(USE_XTC_CARRIER) && !defined(WIN32)
/*
 * xtc_reg_visit callback for PgBackendSendInterruptById.  Runs UNDER the
 * registry lock (xtc_reg_visit's contract), so it must be brief and must not
 * park or re-enter the registry.  It does exactly what the old locked body
 * did: raise PROC_DIE (preserving the sender pid/uid distinction) or send the
 * ordinary interrupt.  Both reach SendInterrupt(), which is non-parking (an
 * atomic fetch_or on the pending mask plus a latch/eventfd wake through the
 * PgBackend struct).  `pid` is the registry's stored pid (XTC_PID_NONE here;
 * unused -- we wake through the PgBackend, see the register comment).
 */
typedef struct PgBackendInterruptVisitArg
{
	PgBackendInterruptType interrupt_type;
	int			sender_pid;
	int			sender_uid;
} PgBackendInterruptVisitArg;

static int
PgBackendInterruptVisit(xtc_pid_t pid, void *value, void *user)
{
	PgBackend  *backend = value;
	const PgBackendInterruptVisitArg *a = user;

	(void) pid;
	if (a->interrupt_type == PG_BACKEND_INTERRUPT_PROC_DIE)
		PgBackendRaiseProcDieInterrupt(backend, a->sender_pid, a->sender_uid);
	else
		SendInterrupt(backend, a->interrupt_type);
	return 0;					/* 0 = continue; the name is unique, so there is just this one entry */
}
#endif

bool
PgBackendSendInterruptById(PgBackendId backend_id,
						   PgBackendInterruptType interrupt_type,
						   int sender_pid, int sender_uid)
{
#if defined(USE_XTC_CARRIER) && !defined(WIN32)
	char		name[32];
	PgBackendInterruptVisitArg arg;

	arg.interrupt_type = interrupt_type;
	arg.sender_pid = sender_pid;
	arg.sender_uid = sender_uid;

	ThreadedBackendRegistryName(name, sizeof(name), backend_id);

	/*
	 * Look up and dispatch under one registry lock: xtc_reg_visit calls the
	 * callback with the lock held, so the PgBackend* cannot be freed while
	 * PgBackendInterruptVisit uses it (the teardown unregisters first).
	 * XTC_E_NOTFOUND means no such backend -> false, matching the
	 * "backend != NULL" result of the old lookup.
	 */
	if (xtc_reg_visit(ThreadedBackendRegistry(), name,
					  PgBackendInterruptVisit, &arg, NULL) == XTC_E_NOTFOUND)
		return false;
	return true;
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
	wait_state->wait_event_info_ptr = &wait_state->local_wait_event_info;
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
	logical_replication->remote_ctx.remote_attnum = -1;
	logical_replication->remote_ctx.remote_xid = InvalidTransactionId;
	logical_replication->remote_ctx.finish_lsn = InvalidXLogRecPtr;
	logical_replication->subxact_data.subxact_last = InvalidTransactionId;
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
	Assert(early_backend_logical_replication.remote_ctx.rel ==
		   NULL);
	Assert(early_backend_logical_replication.remote_ctx.origin_name
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
