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
#include "postmaster/pg_xtc_carrier.h"
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
#include "utils/pg_locale.h"
#include "utils/ps_status.h"
#include "utils/resowner.h"
#include "utils/rls.h"
#include "utils/typcache.h"
#include "utils/xml.h"

PG_THREAD_LOCAL PG_GLOBAL_CARRIER PgRuntimeCurrentBridge
			PgRuntimeCurrentBridgeState = {0};

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
static void PgRuntimeConfigureThreadedAllocator(void);

PgBackendPgStatPendingState *PgCurrentBackendPgStatPendingState(void);
PgBackendInstrumentationState *PgCurrentBackendInstrumentationState(void);
PgBackendTransactionState *PgCurrentBackendTransactionState(void);
PgBackendPendingInterruptState *PgCurrentPendingInterrupts(void);
PgBackendInterruptHoldoffState *PgCurrentInterruptHoldoffs(void);

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
}

static void
PgRuntimeClearHotCurrentCells(void)
{
#define PG_RUNTIME_HOT_CELL(variable, owner, owner_type, type, field) \
	do { \
		PgRuntimeCurrentBridgeState.variable = NULL; \
	} while (0);
#include "utils/backend_runtime_hot_cells.def"
#undef PG_RUNTIME_HOT_CELL
}

void
PgRuntimeReloadCurrentHotCells(void)
{
	PgRuntimeLoadHotCurrentCells();
}

static void
PgRuntimeClearHotFieldPointers(void)
{
#define PG_RUNTIME_HOT_FIELD(variable, owner, type, expr) \
	do { \
		PgRuntimeCurrentBridgeState.variable = NULL; \
		PgRuntimeCurrentBridgeState.variable##Owner = NULL; \
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
	 * These pointers are only a cache of the current runtime work.  Anything
	 * that switches carrier work must call this again.
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
	} while (0);
#include "utils/backend_runtime_hot_fields.def"
#undef PG_RUNTIME_HOT_FIELD
}

static void
PgRuntimeRefreshCurrentWork(bool rebind_session_gucs)
{
	PgRuntimeLoadHotCurrentCells();
	PgRuntimeInstallHotBucketPointers(CurrentPgBackend, CurrentPgSession,
									  CurrentPgConnection,
									  CurrentPgExecution);
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

PG_GLOBAL_RUNTIME PgRuntimeResumeHook pg_runtime_resume_hook = NULL;

void
PgRuntimeRestoreCurrentWork(const PgCurrentWorkSnapshot *snap)
{
	Assert(snap != NULL);
	PgRuntimeSetCurrentWork(snap->runtime, snap->carrier, snap->backend,
							snap->session, snap->connection, snap->execution,
							true);
	pg_thread_locale_install();
	if (pg_runtime_resume_hook != NULL)
		pg_runtime_resume_hook();
}

/*
 * Lazy variant of PgRuntimeRestoreCurrentWork.
 *
 * Restore the six root pointers WITHOUT re-running the ~230-entry session
 * GUC pointer rebind (RebindSessionGUCVariablePointers).  That rebind is
 * unnecessary here because every session-local GUC read already resolves
 * through the current PgSession root: the hot read path uses owner-token
 * hot-field caches keyed on CurrentPgSession (miscadmin.h work_mem etc.), and
 * the GUC metadata arrays (guc_variables / guc_variable_states) are
 * themselves session-rooted accessors (PgCurrentGUCVariablesRef /
 * PgCurrentGUCVariableStatesRef).  Swapping the session root therefore lets
 * all ~130 derived field caches and the GUC state re-derive lazily on first
 * touch via the existing owner-token mechanism, instead of eagerly rebinding
 * every GUC on every switch (measured at ~3722ns / 97% of an eager refresh).
 *
 * This is the restore used by the fiber-context switch hook, where the cost
 * of an eager rebind on every coroutine switch would erase the scheduling
 * win.  The one-time-per-session physical rebind still happens where the
 * backing storage location actually changes (InitializeThreadedSessionGUCOptions
 * and PgCarrierAttachBackend), not on every yield.
 */
void
PgRuntimeRestoreCurrentWorkLazy(const PgCurrentWorkSnapshot *snap)
{
	Assert(snap != NULL);
	PgRuntimeSetCurrentWork(snap->runtime, snap->carrier, snap->backend,
							snap->session, snap->connection, snap->execution,
							false);
	pg_thread_locale_install();
	if (pg_runtime_resume_hook != NULL)
		pg_runtime_resume_hook();
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

	PgRuntimeSetCurrentWork(runtime, carrier, backend, session, connection,
							execution, true);
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

	PgRuntimeSetCurrentWork(NULL, NULL, NULL, NULL, NULL, NULL, false);
	PgRuntimeClearHotBucketPointers();
	PgRuntimeClearHotCurrentCells();
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
	PgRuntimeSetCurrentWork(NULL, NULL, NULL, NULL, NULL, NULL, false);
	PgRuntimeClearHotBucketPointers();
	PgRuntimeClearHotCurrentCells();
	PgRuntimeClearHotFieldPointers();
	PgSessionResetEarlyFallbackForNewBackend();
	/* Drop the locale of the fiber that last ran on this thread. */
	pg_thread_locale_install();
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
 * per-session GUC contexts that sessions repeatedly create and delete.
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
PgRuntimeConfigureThreadedAllocator(void)
{
#if defined(__GLIBC__)
	/*
	 * Arena count is a throughput-vs-footprint tradeoff.  Glibc serializes all
	 * allocations that land in the same arena behind that arena's lock.  Unlike
	 * process mode, where each backend is a separate process with private
	 * arenas, the carrier loop threads share one address space, so too few
	 * arenas make them contend on the arena lock under a malloc-heavy load:
	 * measured on 8 loops, a single arena gave 197k tps and 64 gave 265k.
	 * One arena per loop removes the cross-loop serialization without glibc's
	 * default 8*ncpu explosion.  Retained memory is held down by arena
	 * free-list reuse plus the malloc_trim(0) on backend teardown
	 * (backend_thread_maybe_trim_reclaimed_memory), so total retained memory
	 * is a plateau bounded by the arena cap, not a ramp.  An operator
	 * MALLOC_ARENA_MAX wins.
	 */
	if (getenv("MALLOC_ARENA_MAX") == NULL)
	{
		int			arena_max = 4;

#ifdef USE_XTC_CARRIER
		arena_max = Max(arena_max, xtc_pg_carrier_loop_count());
#endif
		(void) mallopt(M_ARENA_MAX, arena_max);
	}

	/*
	 * Large-allocation policy for the shared address space.  In process mode
	 * glibc's mmap-per-large-alloc and munmap-on-free touch only that
	 * process's private address space.  Here every session shares one, so a
	 * query that allocates large blocks (tuplestore, sort, hash, a large
	 * StringInfo) makes glibc mmap and munmap them repeatedly, and the kernel
	 * mmap_lock and cross-core TLB shootdowns serialize every carrier thread.
	 * On a 192-core box with a CPU-bound query this collapsed throughput to
	 * 0.07x of fork; disabling the mmap path (M_MMAP_MAX=0, keep large blocks
	 * in the arena) and raising the trim threshold to 64 MB (retain freed
	 * blocks for reuse) lifted it 18.5x, to 1.34x of fork, with read OLTP
	 * unchanged and a bounded RSS increase (+24%).  M_TOP_PAD stays 0 so the
	 * retained footprint is the arena free-lists, not top-of-heap padding.
	 * Operator env overrides still win.
	 */
	if (getenv("MALLOC_MMAP_MAX_") == NULL)
		(void) mallopt(M_MMAP_MAX, 0);
	if (getenv("MALLOC_TRIM_THRESHOLD_") == NULL)
		(void) mallopt(M_TRIM_THRESHOLD, 64 * 1024 * 1024);
	if (getenv("MALLOC_TOP_PAD_") == NULL)
		(void) mallopt(M_TOP_PAD, 0);
#endif
}

void
InitializePgThreadRuntime(PgBackendExitContinuation exit_backend)
{
	if (!thread_runtime_initialized)
	{
		PgRuntimeConfigureThreadedAllocator();

		MemSet(&thread_runtime, 0, sizeof(thread_runtime));
		PgRuntimeInitializeRuntimeObject(&thread_runtime);

		thread_runtime.kind = PG_RUNTIME_THREAD_PER_SESSION;
		thread_runtime.extension_backend_model =
			PG_BACKEND_MODEL_THREAD_SAFE;
		/*
		 * Carry the early-runtime extension-module state into thread_runtime.
		 * shared_preload_libraries modules registered runtime-scoped private
		 * state (e.g. pg_stat_statements' shmem pointers) under CurrentPgRuntime
		 * == NULL, so it landed in early_runtime_extension_modules; the
		 * postmaster never ran InitializePgProcessRuntime, so
		 * process_runtime.extension_modules is empty here.  Copy from early (not
		 * from the empty process_runtime, and not a move -- a forked
		 * process-fallback backend rebuilds its own early state after fork+exec
		 * (it does not COW-inherit this postmaster's), so copy-vs-move here has
		 * no effect on it; we copy because it is idempotent and nothing else in
		 * the postmaster consumes early).  Without this, such state is stranded in early and invisible to
		 * carrier sessions under mt=on.
		 */
		PgRuntimeCopyEarlyExtensionModuleState(&thread_runtime);
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

/*
 * xtc-carrier: install the fiber-owned logical roots as current work BEFORE
 * the dedicated backend_thread_entry() path runs MemoryContextInit() and the
 * early GUC init.
 *
 * Without this, that window runs with CurrentPgExecution/CurrentPgSession ==
 * NULL, so hot-field accessors fall back to the SHARED per-OS-thread
 * early_execution_fallback / early_session_fallback.  On the xtc carrier many
 * backend fibers time-share one OS thread; the early GUC init takes the
 * process-wide GUC amutex, which PARKS the fiber under concurrent startup.
 * While parked, a sibling backend fiber runs MemoryContextInit() on the SAME
 * carrier thread and writes the SAME shared fallback -- tripping
 * Assert(TopMemoryContext == NULL) (cassert) or corrupting the shared
 * per-thread memory-context / session-GUC state (release).  Installing the
 * per-fiber logical roots first makes the whole startup window resolve to
 * fiber-owned storage that rides the park with the fiber.
 *
 * InstallPgThreadBackendRuntimeState() below detects that the roots are
 * already current and skips the populate-fallback-then-adopt copy (which would
 * otherwise clobber the live per-fiber state with the now-unused fallback),
 * while still finishing the scheduler_execution memory-context bridge and the
 * required-GUC init.  Callers that do NOT pre-install (the roots are not yet
 * current) keep the original adopt-from-fallback behavior unchanged.
 */
void
PreInstallPgThreadBackendRuntimeState(PgThreadBackendRuntimeState *state)
{
	PgThreadBackendLogicalState *logical;

	Assert(state != NULL);

	logical = &state->logical;
	state->carrier.current_backend = &logical->backend;
	state->carrier.current_session = &logical->session;
	state->carrier.current_execution = &logical->execution;
	PgRuntimeSetCurrentWork(&thread_runtime, &state->carrier,
							&logical->backend, &logical->session,
							&logical->connection, &logical->execution, true);
}

void
InstallPgThreadBackendRuntimeState(PgThreadBackendRuntimeState *state)
{
	PgThreadBackendLogicalState *logical;
	PgExecution *scheduler_execution;
	bool		pre_installed;

	Assert(state != NULL);

	logical = &state->logical;

	/*
	 * If PreInstallPgThreadBackendRuntimeState() already made these logical
	 * roots current, the startup window populated them directly (per-fiber),
	 * not the shared thread-local fallback.  Re-running the adopt copy would
	 * overwrite that live state with the unused fallback, so skip it; the
	 * roots are already current.  Otherwise fall through to the original
	 * populate-fallback-then-adopt path.
	 */
	pre_installed = (CurrentPgExecution == &logical->execution &&
					 CurrentPgSession == &logical->session &&
					 CurrentPgBackend == &logical->backend);

	state->carrier.current_backend = &logical->backend;
	state->carrier.current_session = &logical->session;
	state->carrier.current_execution = &logical->execution;
	if (!pre_installed)
	{
		PgBackendAdoptEarlyState(&logical->backend);
		PgSessionAdoptEarlyState(&logical->session);
		PgConnectionAdoptEarlyState(&logical->connection,
									logical->connection.identity.port);
		PgExecutionAdoptEarlyState(&logical->execution);
		PgRuntimeSetCurrentWork(&thread_runtime, &state->carrier,
								&logical->backend, &logical->session,
								&logical->connection, &logical->execution, true);
	}
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
PgSetCurrentRuntime(PgRuntime *runtime)
{
	CurrentPgRuntime = runtime;
	PgRuntimeRefreshCurrentWork(false);
}

void
PgSetCurrentCarrier(PgCarrier *carrier)
{
	CurrentPgCarrier = carrier;
	PgRuntimeRefreshCurrentWork(false);
}

void
PgSetCurrentBackend(PgBackend *backend)
{
	CurrentPgBackend = backend;
	PgRuntimeRefreshCurrentWork(false);
}

void
PgSetCurrentSession(PgSession *session)
{
	CurrentPgSession = session;
	PgRuntimeRefreshCurrentWork(false);
}

void
PgSetCurrentConnection(PgConnection *connection)
{
	CurrentPgConnection = connection;
	PgRuntimeRefreshCurrentWork(false);
}

void
PgSetCurrentExecution(PgExecution *execution)
{
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
	return kind == PG_RUNTIME_THREAD_PER_SESSION;
}

bool
PgRuntimeIsThreadBacked(PgRuntime *runtime)
{
	return runtime != NULL &&
		PgRuntimeKindIsThreadBacked(runtime->kind);
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
	 * Every child type the threaded postmaster starts runs on the carrier,
	 * except the syslogger, which stays a process so it outlives a crash.
	 */
	return backend_type == B_BACKEND ||

	/*
	 * A dead-end backend must be threadable too, or the postmaster cannot
	 * refuse a connection at all once any carrier exists: AllocDeadEndChild()
	 * produces a B_DEAD_END_BACKEND, postmaster_child_launch_carrier() finds no
	 * carrier for it, falls through to the fork-without-exec path, and that path
	 * is (correctly) refused with ENOSYS -- so the client gets "could not fork
	 * new process for connection" instead of "sorry, too many clients already",
	 * and EVERY subsequent connection fails the same way while the server stays
	 * up.  Observed on a 32-core box under pgbench -c 64.
	 *
	 * It is trivially safe as a fiber: a dead-end child only reports its
	 * canAcceptConnections reason and exits.  It ereport(FATAL)s inside
	 * BackendInitialize(), i.e. BEFORE InitProcess(), so it never creates a
	 * PGPROC, never attaches shared memory, and never takes an LWLock.
	 */
		backend_type == B_DEAD_END_BACKEND ||
		backend_type == B_ARCHIVER ||
		backend_type == B_AUTOVAC_LAUNCHER ||
		backend_type == B_AUTOVAC_WORKER ||
		backend_type == B_BG_WRITER ||
		backend_type == B_CHECKPOINTER ||
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
		backend_model > PG_BACKEND_MODEL_THREAD_SAFE)
		elog(ERROR, "invalid backend model: %d", backend_model);

	if (CurrentPgRuntime == NULL)
		return;

	check_loaded_modules_backend_model(backend_model);
	CurrentPgRuntime->extension_backend_model = backend_model;
}
