/*-------------------------------------------------------------------------
 *
 * launch_backend.c
 *	  Functions for launching backends and other postmaster child
 *	  processes.
 *
 * On Unix systems, a new child process is launched with fork().  It inherits
 * all the global variables and data structures that had been initialized in
 * the postmaster.  After forking, the child process closes the file
 * descriptors that are not needed in the child process, and sets up the
 * mechanism to detect death of the parent postmaster process, etc.  After
 * that, it calls the right Main function depending on the kind of child
 * process.
 *
 * In EXEC_BACKEND mode, which is used on Windows but can be enabled on other
 * platforms for testing, the child process is launched by fork() + exec() (or
 * CreateProcess() on Windows).  It does not inherit the state from the
 * postmaster, so it needs to re-attach to the shared memory, re-initialize
 * global variables, reload the config file etc. to get the process to the
 * same state as after fork() on a Unix system.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/launch_backend.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <errno.h>
#if defined(__GLIBC__)
#include <malloc.h>
#endif
#include <poll.h>
#include <fcntl.h>
#ifndef WIN32
#include <sys/eventfd.h>
#endif
#include <sys/time.h>
#include <unistd.h>

#include "access/xact.h"
#include "common/pg_prng.h"
#include "libpq/libpq-be.h"
#include "libpq/libpq.h"		/* ssl_sni (xtc migratability gate) */
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgtime.h"
#include "postmaster/autovacuum.h"
#include "postmaster/bgworker_internals.h"
#include "postmaster/bgwriter.h"
#include "postmaster/fork_process.h"
#include "postmaster/pgarch.h"
#include "postmaster/pg_xtc_carrier.h"	/* fusion F1 runtime counters (no-ops off-carrier) */
#include "postmaster/postmaster.h"
#include "postmaster/startup.h"
#include "postmaster/syslogger.h"
#include "postmaster/walsummarizer.h"
#include "postmaster/walwriter.h"
#ifndef WIN32
#include "port/pg_pthread.h"
#endif
#include "replication/slotsync.h"
#include "replication/walreceiver.h"
#include "storage/dsm.h"
#include "storage/io_worker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/pg_shmem.h"
#include "storage/shmem_internal.h"
#include "storage/waiteventset.h"
#include "tcop/backend_startup.h"
#include "tcop/tcopprot.h"
#include "utils/backend_runtime.h"
#include "utils/guc.h"
#include "utils/global_lifetime.h"
#include "utils/memutils.h"
#include "utils/pgstat_internal.h"
#include "utils/timestamp.h"

#ifdef FORKEXEC_BACKEND
#include "nodes/queryjumble.h"
#include "portability/instr_time.h"
#include "storage/pg_shmem.h"
#include "storage/spin.h"
#endif


#ifdef FORKEXEC_BACKEND

#include "common/file_utils.h"
#include "storage/aio.h"			/* io_method / IOMETHOD_WORKER for the child remap */
#include "storage/fd.h"
#include "storage/lwlock.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "tcop/tcopprot.h"
#include "utils/injection_point.h"

/* Type for a socket that can be inherited to a client process */
#ifdef WIN32
typedef struct
{
	SOCKET		origsocket;		/* Original socket value, or PGINVALID_SOCKET
								 * if not a socket */
	WSAPROTOCOL_INFO wsainfo;
} InheritableSocket;
#else
typedef int InheritableSocket;
#endif

/*
 * Structure contains all variables passed to exec:ed backends
 */
typedef struct
{
	char		DataDir[MAXPGPATH];
#ifndef WIN32
	unsigned long UsedShmemSegID;
#else
	void	   *ShmemProtectiveRegion;
	HANDLE		UsedShmemSegID;
#endif
	void	   *UsedShmemSegAddr;
#ifdef USE_INJECTION_POINTS
	struct InjectionPointsCtl *ActiveInjectionPoints;
#endif
	PROC_HDR   *ProcGlobal;
	PGPROC	   *AuxiliaryProcs;
	PGPROC	   *PreparedXactProcs;
	volatile PMSignalData *PMSignalState;
	ProcSignalHeader *ProcSignal;
	pid_t		PostmasterPid;
	TimestampTz PgStartTime;
	TimestampTz PgReloadTime;
	pg_time_t	first_syslogger_file_time;
	bool		redirection_done;
	bool		IsBinaryUpgrade;
	bool		saved_query_id_enabled;
	int			max_safe_fds;
	int			MaxBackends;
	int			num_pmchild_slots;
#ifdef WIN32
	HANDLE		PostmasterHandle;
	HANDLE		initial_signal_pipe;
	HANDLE		syslogPipe[2];
#else
	int			postmaster_alive_fds[2];
	int			syslogPipe[2];
#endif
	char		my_exec_path[MAXPGPATH];
	char		pkglib_path[MAXPGPATH];

	int			saved_my_pmchild_slot;

	int32		timing_tsc_frequency_khz;

	/*
	 * These are only used by backend processes, but are here because passing
	 * a socket needs some special handling on Windows. 'client_sock' is an
	 * explicit argument to postmaster_child_launch, but is stored in
	 * MyClientSocket in the child process.
	 */
	ClientSocket client_sock;
	InheritableSocket inh_sock;

	/*
	 * Extra startup data, content depends on the child process.
	 */
	size_t		startup_data_len;
	char		startup_data[FLEXIBLE_ARRAY_MEMBER];
} BackendParameters;

#define SizeOfBackendParameters(startup_data_len) (offsetof(BackendParameters, startup_data) + startup_data_len)

static void read_backend_variables(char *id, void **startup_data, size_t *startup_data_len);
static void restore_backend_variables(BackendParameters *param);

static bool save_backend_variables(BackendParameters *param, int child_slot,
								   const ClientSocket *client_sock,
#ifdef WIN32
								   HANDLE childProcess, pid_t childPid,
#endif
								   const void *startup_data, size_t startup_data_len);

static pid_t internal_forkexec(BackendType child_kind, int child_slot,
							   const void *startup_data, size_t startup_data_len,
							   const ClientSocket *client_sock);

#endif							/* FORKEXEC_BACKEND */

/*
 * Information needed to launch different kinds of child processes.
 */
typedef struct
{
	const char *name;
	void		(*main_fn) (const void *startup_data, size_t startup_data_len);
	bool		shmem_attach;
} child_process_kind;

static PG_GLOBAL_IMMUTABLE const child_process_kind child_process_kinds[] = {
#define PG_PROCTYPE(bktype, bkcategory, description, main_func, shmem_attach) \
	[bktype] = {description, main_func, shmem_attach},
#include "postmaster/proctypelist.h"
#undef PG_PROCTYPE
};

typedef enum BackendThreadStartKind
{
	BACKEND_THREAD_START_DEDICATED,
	BACKEND_THREAD_START_POOLED_LOGICAL
} BackendThreadStartKind;

typedef struct BackendThreadPublication
{
	BackendThreadStartKind kind;
	PMChild    *pmchild;
	Latch	   *postmaster_latch;
} BackendThreadPublication;

typedef struct BackendThreadStart
{
	BackendThreadPublication publication;
	BackendType child_type;
	int			child_slot;
	PgThreadBackendRuntimeState runtime_state;
	BackendStartupData startup_data;
	BackgroundWorker bgworker_startup_data;
	ClientSocket client_sock;
	pg_tz	   *startup_session_timezone;
	pg_tz	   *startup_log_timezone;
	pg_atomic_uint32 launch_registered;

	/*
	 * xtc-carrier only: guards the one-time PMChild exit publication of a
	 * fiber-backed worker against a double reap.  A pooled-logical worker
	 * fiber that is spawned onto a carrier loop but never scheduled (a
	 * cross-thread wake to an idle io_uring loop can be missed in the current
	 * libxtc) leaves its PMChild un-reaped, which wedges PM_WAIT_BACKENDS at
	 * fast stop.  The autovacuum launcher's worker-start-timeout cancel is the
	 * authoritative "this worker never started" signal; on that signal the
	 * postmaster reaps the orphaned worker's PMChild.  Both the postmaster
	 * (orphan reap) and the fiber (if it ever does run and reaches proc_exit)
	 * claim this flag; only the winner publishes the pooled-logical exit and
	 * releases the PMChild, so the slot is reaped exactly once.  The struct is
	 * fiber-owned for its whole life and freed only by the fiber, so the
	 * postmaster only ever reads/exchanges this atomic, never frees it (a
	 * fiber that genuinely never runs leaks this one struct -- bounded and
	 * rare, and far cheaper than a wedged shutdown).
	 *
	 * fiber_entered is set by the fiber as its very first action (before any
	 * PMChild access), so the postmaster can tell "the fiber body actually
	 * started running" from "the launch was published but the fiber was never
	 * scheduled".  The orphan reap only targets workers whose fiber never
	 * entered, so it can never release the PMChild slot of a live/starting
	 * worker.
	 *
	 * launch_time is when the postmaster handed this worker to the carrier.
	 * The launcher-cancel reap only reaps an un-entered worker that is OLDER
	 * than the worker-start-timeout, so it can never grab a worker the
	 * launcher just (re)launched in the same signal window -- only the aged,
	 * genuinely-stuck one.  (The shutdown-time reap ignores age: no new
	 * workers launch once pmState >= PM_STOP_BACKENDS, so any un-entered
	 * orphan there is safe to drain.)
	 */
	pg_atomic_uint32 exit_claimed;
	pg_atomic_uint32 fiber_entered;
	/*
	 * start_claimed arbitrates the entry race between a just-scheduled worker
	 * fiber and ReapOrphanedThreadedWorker: whoever wins the exchange (0->1)
	 * owns the PMChild slot.  The fiber claims it right after publishing
	 * fiber_entered (before any slot access); the reaper claims it only after
	 * seeing fiber_entered == 0.  Distinct from exit_claimed (the exit-publish
	 * arbiter) so the normal fiber exit path is unaffected.
	 */
	pg_atomic_uint32 start_claimed;
	TimestampTz launch_time;
} BackendThreadStart;

typedef struct BackendPooledLogicalStart
{
	BackendThreadPublication publication;
	PgThreadBackendLogicalState logical;
	BackendStartupData startup_data;
	ClientSocket client_sock;
	sigjmp_buf	exit_jmp;
	bool		exit_jmp_valid;
	struct BackendPooledLogicalStart *next;
} BackendPooledLogicalStart;

typedef struct BackendPooledCarrierStart
{
	PgCarrier	carrier;
	PgThread	thread;
	int			carrier_index;
	int			selfpipe_readfd;
	int			selfpipe_writefd;
	pg_tz	   *startup_session_timezone;
	pg_tz	   *startup_log_timezone;
} BackendPooledCarrierStart;

static PG_GLOBAL_RUNTIME bool postmaster_thread_carriers_started = false;
#ifndef WIN32
static PG_GLOBAL_RUNTIME pthread_mutex_t pooled_protocol_queue_mutex = PTHREAD_MUTEX_INITIALIZER;
static PG_GLOBAL_RUNTIME pthread_cond_t pooled_protocol_queue_cond = PTHREAD_COND_INITIALIZER;
static PG_GLOBAL_RUNTIME BackendPooledLogicalStart *pooled_protocol_queue_head = NULL;
static PG_GLOBAL_RUNTIME BackendPooledLogicalStart *pooled_protocol_queue_tail = NULL;
static PG_GLOBAL_RUNTIME int pooled_protocol_queue_length = 0;
static PG_GLOBAL_RUNTIME int pooled_protocol_carrier_count = 0;
static PG_GLOBAL_RUNTIME bool pooled_protocol_pool_started = false;
/*
 * Wake eventfd for the pooled carrier loop.  A carrier blocks in one poll() on
 * its parked-session fds; new work (a queued session, or a resumable backend)
 * is not visible to that poll() because it arrives on the shared queue under
 * pooled_protocol_queue_mutex.  Rather than give the poll() a short timeout and
 * busy-recheck the queue ~100x/s per carrier (a wake storm that scales with
 * carrier count -- see plan_docs/MULTITHREADED_PHASE18_PROFILE.md), we add this
 * eventfd to every carrier's poll set and signal it when work is posted.  The
 * carrier then blocks with a long timeout and wakes on EITHER a parked fd
 * becoming readable OR new work, with no periodic self-wakeups.
 */
static PG_GLOBAL_RUNTIME int pooled_protocol_wake_fd = -1;
#endif
#if defined(__GLIBC__)
#define BACKEND_THREAD_MALLOC_TRIM_THRESHOLD ((Size) 64 * 1024 * 1024)
static PG_GLOBAL_RUNTIME pthread_mutex_t backend_thread_malloc_trim_mutex = PTHREAD_MUTEX_INITIALIZER;
static PG_GLOBAL_RUNTIME Size backend_thread_malloc_trim_pending = 0;
#endif

static bool postmaster_backend_thread_launch(PMChild *pmchild,
											 BackendType child_type,
											 int child_slot,
											 void *startup_data,
											 size_t startup_data_len,
											 const ClientSocket *client_sock);
static bool postmaster_pooled_protocol_launch(PMChild *pmchild,
											  int child_slot,
											  void *startup_data,
											  size_t startup_data_len,
											  const ClientSocket *client_sock);
static bool postmaster_pooled_protocol_process_fallback(PMChild *pmchild,
															int child_slot,
															void *startup_data,
															size_t startup_data_len,
															const ClientSocket *client_sock);
static BackendThreadStart *backend_thread_start_alloc(void);
static void backend_thread_start_release(BackendThreadStart *thread_start);
static BackendPooledLogicalStart *backend_pooled_logical_start_alloc(void);
static void backend_pooled_logical_start_release(BackendPooledLogicalStart *logical_start);
static void backend_thread_entry(void *arg);
#ifdef USE_XTC_CARRIER
static bool xtc_carrier_eligible(BackendType child_type);
#endif
static void backend_thread_run_backend(BackendThreadStart *thread_start);
static void backend_thread_run_worker(BackendThreadStart *thread_start);
static BackendThreadPublication *backend_thread_current_publication(void);
static BackendThreadStart *backend_thread_current_start(void);
static void backend_thread_set_current_start(BackendThreadStart *thread_start);
static void backend_thread_wait_until_registered(BackendThreadStart *thread_start);
static void backend_thread_init_random_state(void);
static void backend_thread_clear_deleted_retained_memory_contexts(void);
static void backend_thread_free_deleted_retained_memory_contexts(void);
static void backend_thread_maybe_trim_reclaimed_memory(Size reclaimed);
pg_noreturn static void backend_thread_exit(int code);
pg_noreturn static void backend_thread_finish(int code);
pg_noreturn static void backend_pooled_logical_finish(int code);
static int	backend_thread_exitstatus(int code);
#ifndef WIN32
static bool backend_pooled_protocol_start_pool(void);
static bool backend_pooled_protocol_start_one_carrier(void);
static void backend_pooled_protocol_maybe_start_carrier_for_work(void);
static void backend_pooled_protocol_carrier_entry(void *arg);
static void backend_pooled_protocol_enqueue(BackendPooledLogicalStart *logical_start);
static BackendPooledLogicalStart *backend_pooled_protocol_dequeue(void);
static int	backend_pooled_protocol_queue_count(void);
static uint32 backend_pooled_protocol_idle_carrier_count(void);
static void backend_pooled_protocol_signal_work(void);
static void backend_pooled_protocol_wake_signal(void);
static void backend_pooled_protocol_wake_drain(void);
static void backend_pooled_protocol_signal_ready_work(int count);
static void backend_pooled_protocol_wait_for_work(long timeout_us);
static void backend_pooled_protocol_deadline_after(long timeout_us,
												   struct timespec *deadline);
static BackendPooledLogicalStart *backend_pooled_logical_start_from_backend(PgBackend *backend);
static void backend_pooled_protocol_run_logical_start(BackendPooledCarrierStart *carrier_start,
													  BackendPooledLogicalStart *logical_start);
static void backend_pooled_protocol_resume_logical_start(BackendPooledLogicalStart *logical_start);
static PgStepResult backend_pooled_protocol_run_attached_logical(BackendPooledLogicalStart *logical_start,
																 PgSession *session);
pg_noreturn static void backend_pooled_protocol_exit_logical(int code);
#endif

const char *
PostmasterChildName(BackendType child_type)
{
	return child_process_kinds[child_type].name;
}

bool
PostmasterThreadCarriersStarted(void)
{
	return postmaster_thread_carriers_started;
}

/*
 * Start a new postmaster child using the runtime-selected carrier model.
 */
bool
postmaster_child_launch_carrier(PMChild *pmchild,
								BackendType child_type, int child_slot,
								void *startup_data, size_t startup_data_len,
								const ClientSocket *client_sock)
{
	pid_t		pid;
	PgBackendLaunchModel launch_model;

	if (multithreaded &&
		child_type == B_BACKEND &&
		PgRuntimePooledProtocolRequested())
	{
		return postmaster_pooled_protocol_launch(pmchild, child_slot,
												 startup_data,
												 startup_data_len,
												 client_sock);
	}

	if (multithreaded &&
		child_type == B_BG_WORKER &&
		startup_data != NULL &&
		startup_data_len == sizeof(BackgroundWorker) &&
		BackgroundWorkerCanUseThreadCarrier((BackgroundWorker *) startup_data))
	{
		return postmaster_backend_thread_launch(pmchild, child_type, child_slot,
												startup_data, startup_data_len,
												client_sock);
	}

	/*
	 * B_IO_WORKER is intentionally never routed to a carrier: under
	 * multithreaded=on we remap io_method=worker to the in-fiber "xtc" method
	 * (see PostmasterMain), so pgaio_workers_enabled() is false and no io
	 * workers are ever started.  There is nothing for an io-worker carrier to
	 * do, so we do not launch one.
	 */

	/*
	 * The logger, checkpointer, and background writer are needed before the
	 * startup process is forked, so their initial startup carriers must
	 * remain processes.  After normal running begins and another thread
	 * carrier has made fork-without-exec unsafe, the postmaster hands them off
	 * and relaunches them through the runtime-selected thread carrier path.
	 */
	if (multithreaded &&
		!postmaster_thread_carriers_started &&
		(child_type == B_LOGGER ||
		 child_type == B_CHECKPOINTER || child_type == B_BG_WRITER))
		launch_model = PG_BACKEND_LAUNCH_PROCESS;
	else
		launch_model = PgRuntimeGetBackendLaunchModel(child_type);

	if (launch_model == PG_BACKEND_LAUNCH_THREAD)
	{
		return postmaster_backend_thread_launch(pmchild, child_type, child_slot,
												startup_data, startup_data_len,
												client_sock);
	}

	/*
	 * Once the postmaster has created any thread carrier, later fork-without-
	 * exec process launches are unsafe.  Phase 10 only supports regular client
	 * backend threads; Phase 11 must replace server-owned worker process
	 * launches with worker thread carriers before they can run in normal
	 * threaded mode.
	 */
	if (multithreaded && postmaster_thread_carriers_started)
	{
		errno = ENOSYS;
		return false;
	}

	pid = postmaster_child_launch(child_type, child_slot,
								  startup_data, startup_data_len, client_sock);
	if (pid < 0)
		return false;

	PostmasterChildSetProcess(pmchild, pid);
	return true;
}

static BackendThreadStart *
backend_thread_start_alloc(void)
{
	BackendThreadStart *thread_start;

	thread_start = malloc(sizeof(BackendThreadStart));
	if (thread_start != NULL)
		MemSet(thread_start, 0, sizeof(*thread_start));

	return thread_start;
}

static void
backend_thread_start_release(BackendThreadStart *thread_start)
{
	PgExecution *scheduler_execution;

	if (thread_start == NULL)
		return;

	scheduler_execution = thread_start->runtime_state.carrier.scheduler_execution;
	if (scheduler_execution != NULL)
	{
		thread_start->runtime_state.carrier.scheduler_execution = NULL;
		free(scheduler_execution);
	}

	free(thread_start);
}

static BackendPooledLogicalStart *
backend_pooled_logical_start_alloc(void)
{
	BackendPooledLogicalStart *logical_start;

	logical_start = malloc(sizeof(BackendPooledLogicalStart));
	if (logical_start != NULL)
		MemSet(logical_start, 0, sizeof(*logical_start));

	return logical_start;
}

static void
backend_pooled_logical_start_release(BackendPooledLogicalStart *logical_start)
{
	if (logical_start == NULL)
		return;

	free(logical_start);
}

#ifdef USE_XTC_CARRIER
/*
 * xtc-carrier: which thread-carrier child types may run as xtc fibers on the
 * shared carrier loop pool instead of dedicated pthreads.
 *
 * Start conservative: only regular client backends run as xtc fibers today.
 * They reach postmaster_backend_thread_launch() as thread carriers, run the
 * common backend_thread_entry(), yield through the waiteventset xtc intercept,
 * and reap as pooled-logical PMChildren.
 *
 * Server-owned worker families (bgworker, io worker, autovacuum, WAL, etc.)
 * are deferred: a bare pthread->fiber carrier swap is not enough -- their
 * crash/terminate/restart and shutdown-ordering protocols (and, for io
 * workers, the AIO PM_WAIT_IO_WORKERS handshake, item #6) need fiber-aware
 * handling.  Widen this allowlist one family at a time, each validated under
 * the full threaded-runtime TAP (not just happy-path smoke) on a disk-backed
 * host.  Deferred, not rejected.
 *
 * ponytail: hand-picked allowlist, not a broad opt-in.  Widen one family at a
 * time as each is shown to yield at every blocking wait (never blocks the
 * carrier loop) and to tear down cleanly as a pooled-logical PMChild.
 */
static bool
xtc_carrier_eligible(BackendType child_type)
{
	switch (child_type)
	{
		case B_BACKEND:
			return true;
		case B_BG_WORKER:

			/*
			 * #5 widening (post-#7): background workers are fiber-eligible.
			 * #7 Stage 1b now gives fiber-aware crash containment + escalation
			 * (a faulted worker fiber delivers a DOWN(reason=signal) that the
			 * supervisor escalates), which was the missing piece.  Validate the
			 * full lifecycle -- launch, run, SIGTERM/terminate, crash, and clean
			 * shutdown ordering -- under the threaded-runtime TAP on a
			 * disk-backed host, not just happy-path smoke.
			 */
			return true;
		case B_AUTOVAC_LAUNCHER:
		case B_AUTOVAC_WORKER:
			return true;
		case B_WAL_WRITER:

			/*
			 * #5 widening -- Tier A (2026-07-06 family audit): the WAL writer
			 * is fiber-eligible.  It is a long-lived singleton launched only in
			 * normal running (StartChildProcess(B_WAL_WRITER) at PM_RUN), so it
			 * has no start-before-thread-carriers hazard (unlike
			 * logger/checkpointer/bgwriter, Tier D) and no on-demand
			 * start-timeout cancel race (unlike autovac workers).  Its main
			 * loop parks on WaitLatch(MyLatch, ...) which routes through the
			 * xtc intercept, and SIGTERM/SIGINT map to SHUTDOWN_REQUEST -> a
			 * cross-fiber SetLatch wakes the fiber -> ProcessMainLoopInterrupts
			 * -> proc_exit(0), publishing its pooled-logical exit so
			 * PM_WAIT_* completes.
			 */
			return true;
		case B_WAL_SUMMARIZER:

			/*
			 * #5 widening -- Tier A (2026-07-07): the WAL summarizer is
			 * fiber-eligible.  Like the WAL writer it is a long-lived singleton
			 * (StartChildProcess at PM_RUN) with no start-before-carriers
			 * hazard and no start-timeout cancel race.  It parks on
			 * WaitLatch(MyLatch, ...) in summarizer_wait_for_wal() with a
			 * bounded WL_TIMEOUT and re-polls GetLatestLSN() on each wake, so it
			 * advances as WAL is generated without needing an fd-based wake
			 * (verified: pending_lsn tracks the flush LSN and summarized_lsn
			 * advances across a checkpoint, byte-for-byte identical to process
			 * mode).  Fast stop: SIGTERM -> SHUTDOWN_REQUEST -> cross-fiber
			 * SetLatch wakes the fiber -> ProcessWalSummarizerInterrupts ->
			 * proc_exit(0).  Immediate stop: SIGQUIT -> PROC_DIE, which
			 * ProcessWalSummarizerInterrupts now honors (walsummarizer.c), so
			 * the fiber exits and PM_WAIT_* completes.  (The earlier deferral
			 * blamed a libxtc idle-loop timer-wake gap and a shutdown wedge;
			 * both were stale -- the fast-stop wedge was cured by blocking
			 * process-directed signals across xtc bringup, the immediate-stop
			 * wedge by the PROC_DIE check, and the "stuck summarized_lsn" was a
			 * summary-file boundary artifact that equally affects process mode.)
			 */
			return true;
		case B_ARCHIVER:

			/*
			 * #5 widening -- Tier C: the WAL archiver runs in-process but as a
			 * dedicated THREAD CARRIER, not a fiber (return false here ->
			 * PgRuntimeShouldThreadBackend includes B_ARCHIVER -> PG_BACKEND_LAUNCH_
			 * THREAD).  Rationale (2026-07-09): the archiver runs archive_command
			 * via system() (shell_archive.c), which fork()+exec()+waitpid()s and
			 * BLOCKS the calling thread for the entire command.  As a fiber that
			 * would stall the shared carrier loop -- freezing every sibling client
			 * backend fiber on that loop for the whole (possibly multi-second)
			 * archive command.  A dedicated thread carrier confines the blocking
			 * system()/waitpid to the archiver's own OS thread, so sibling fibers
			 * keep running.  (This matches checkpointer/bgwriter/startup, which are
			 * also thread carriers.)  Everything else about the archiver is
			 * unchanged and already validated as a thread carrier: it is a
			 * PM_RUN/PM_HOT_STANDBY singleton, parks on WaitLatch routed through the
			 * intercept, drains via ProcessPgArchInterrupts(), and its two-step
			 * shutdown (SIGTERM->SHUTDOWN_REQUEST, SIGUSR2->WAKEUP_STOP, SIGQUIT->
			 * PROC_DIE) is wired in thread_child_signal_interrupt.  It is still
			 * in-process (no fork of the archiver itself), so no forked-completer
			 * wake problem.  An archive_library (C API, no system()) would be
			 * fiber-safe, but the default archive_command path needs the thread.
			 */
			return false;
		case B_SLOTSYNC_WORKER:

			/*
			 * #5 widening -- Tier B (2026-07-09): the slot sync worker is
			 * fiber-eligible.  It runs only on a hot standby with
			 * sync_replication_slots=on (StartChildProcess(B_SLOTSYNC_WORKER) at
			 * PM_HOT_STANDBY), so it starts well after thread carriers exist.  Its
			 * main loop and SlotSyncWorkerCheckForStop park on WaitLatch(MyLatch,
			 * ...) with a bounded WL_TIMEOUT routed through the xtc intercept and
			 * re-poll via CHECK_FOR_INTERRUPTS() on each wake.  Fast/immediate stop
			 * map SIGTERM/SIGQUIT to PROC_DIE; the cross-fiber SetLatch wakes the
			 * fiber and ProcessInterrupts() honors ProcDiePending -> proc_exit,
			 * publishing the pooled-logical exit so PM_WAIT_* completes.
			 *
			 * The earlier deferral was a MISCONFIG artifact (sync_replication_slots
			 * without a dbname in primary_conninfo made the worker error out and
			 * relaunch every ~60s, and shutdown could not converge on that churn --
			 * compounded by the standby fiber-backend AIO hang, now fixed by the
			 * io_method=xtc routing).  With a correct slotsync config it starts,
			 * syncs, and tears down cleanly.
			 */
			return true;
		case B_WAL_RECEIVER:

			/*
			 * #5 widening -- Tier B (2026-07-09): the WAL receiver is
			 * fiber-eligible.  It is started on demand by the startup process
			 * during recovery / on a standby (StartChildProcess(B_WAL_RECEIVER)
			 * at PM_STARTUP..PM_HOT_STANDBY), after thread carriers exist.  Its
			 * main loop parks on WaitLatchOrSocket(MyLatch, WL_SOCKET_READABLE |
			 * WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH) -- routed through
			 * the xtc intercept -- and processes interrupts via
			 * CHECK_FOR_INTERRUPTS() plus its own WalRcvShutdownRequested()/
			 * proc_exit(1) handshake.  SIGTERM/SIGQUIT map to PROC_DIE and wake
			 * the fiber via cross-fiber SetLatch, driving proc_exit through the
			 * standard ProcessInterrupts() ProcDiePending path.
			 *
			 * The earlier deferral blamed a "standby shutdown-ordering" wedge, but
			 * that was a SYMPTOM: the standby's fiber CLIENT backends hung during
			 * InitPostgres on their first uncached read (forked io workers cannot
			 * wake in-process fibers under io_method=worker), and a hung backend
			 * keeps PM_WAIT_BACKENDS from converging.  With io_method routed to the
			 * in-fiber xtc method under multithreaded=on (postmaster.c), standby
			 * fiber backends complete queries and shutdown converges, so the
			 * walreceiver's own clean fiber teardown is no longer masked.
			 */
			return true;
		default:

			/*
			 * Remaining server-owned worker families are NOT yet
			 * fiber-eligible.  B_IO_WORKER additionally needs the AIO shutdown
			 * protocol (PM_WAIT_IO_WORKERS) handled on fibers -- that belongs
			 * with the xtc_aio work (item #6).  The auxiliary families
			 * (logger, checkpointer, bgwriter -- which must start as processes
			 * before thread carriers exist -- plus archiver, ...) each have
			 * their own start/shutdown-ordering and restart protocols; widen
			 * one at a time once its full lifecycle is validated on a fiber
			 * under the threaded-runtime TAP.  Deferred, not rejected.
			 * (B_BACKEND, B_BG_WORKER, autovacuum, B_WAL_WRITER,
			 * B_WAL_SUMMARIZER, B_SLOTSYNC_WORKER, and B_WAL_RECEIVER are
			 * handled above.)
			 */
			return false;
	}
}

/*
 * Recover a fiber's own PgCarrier root from the opaque BackendThreadStart *
 * that xtc_carrier_proc receives as its entry arg and hands to
 * xtc_proc_set_userdata().  The carrier lives inside the fiber-owned
 * BackendThreadStart (runtime_state.carrier), so this pointer rides with the
 * fiber across a work-stealing steal and is valid for the fiber's whole life.
 * The carrier layer stays free of the private BackendThreadStart layout via
 * this one accessor.
 */
PgCarrier *
xtc_pg_backend_thread_start_carrier(void *thread_start)
{
	BackendThreadStart *ts = (BackendThreadStart *) thread_start;

	if (ts == NULL)
		return NULL;
	return &ts->runtime_state.carrier;
}

/*
 * Whether a fiber-eligible child of this type may ALSO be work-stolen across
 * carrier loops (xtc_proc_opts_t.migratable) once the gated unpin is live.
 *
 * Default the SAFE way.  Only regular client backends (B_BACKEND) migrate:
 * they are the throughput target, and every cooperative park a backend fiber
 * has is a wait-boundary seam that repoints the six fiber-owned current-work
 * roots on resume (see xtc_pg_verify_current_work_is_self and
 * MULTITHREADED_FIBER_WORKER_DESIGN.md), so a stolen backend resumes correctly
 * on any loop.  Everything else stays PINNED:
 *   - background workers (parallel query, logical replication, extension
 *     workers) run arbitrary/extension code whose thread-affine assumptions
 *     are not audited for migration; Phase 16 / Gate E2-Extensions owns that.
 *   - the WAL writer/summarizer, autovacuum, slotsync, WAL receiver and other
 *     long-lived singletons have no throughput reason to migrate and may hold
 *     loop-affine resources; keep them on their spawn loop.
 *   - the per-loop supervisor is a bare fiber and never routes through here.
 *
 * ssl_sni no-migrate invariant (libxtc SNI #29 deferred): a server-side-SNI
 * connection drives OpenSSL's per-OS-thread state directly and cannot yet be
 * served by the fiber-aware TLS stack from a foreign thread, so when ssl_sni
 * is on NO backend may migrate.  ssl_sni is a process-global GUC
 * (PG_GLOBAL_RUNTIME), authoritative on the postmaster thread where this runs,
 * so reading it here HONORS the invariant (the be_tls_open_server assertion is
 * the tripwire; this is the actual gate).
 */
static bool
xtc_carrier_migratable(BackendType child_type)
{
	/*
	 * Phase D: regular client backends (B_BACKEND) are migratable so a loop
	 * whose run queue drains can steal them under eager rebalance (libxtc
	 * v1.27.0, wired on the threaded multi-loop carrier).  Everything else
	 * stays PINNED (see the child-type rationale above); ssl_sni=on pins ALL
	 * backends (the SNI no-migrate invariant).
	 *
	 * Safe to re-enable now that the three in-tree unseamed-park corruptions
	 * are closed: the GUC-amutex command-path bridge leak is seamed
	 * (guc.c ThreadedGUCLock), the concurrent-startup pre-install window is
	 * per-fiber (PreInstallPgThreadBackendRuntimeState), and every other
	 * backend-fiber park is seamed (xtc_pg_wait_fd -> WaitEventSet/
	 * ProcSemaphoreWaitFiber, AIO r/w + fsync/fdatasync) or detaches the
	 * bridge by design (the protocol-read boundary).  See
	 * plan_docs/MULTITHREADED_UNSEAMED_PARK_AUDIT.md.
	 */
	/*
	 * HELD at migratable=0 (2026-07-22): an independent review of the
	 * migratable=1 re-enable (d04f3bea68c) found TWO real, migration-only,
	 * reproduced-clean-on-pinned regressions under real work-steals:
	 *   (1) guc.c:1396 GUCMemoryContext corruption -- deterministic under
	 *       parallel load, a fiber freeing on the wrong session's GUC context
	 *       (session-root/GUC-metadata unseamed-park hazard, now live under
	 *       real steals); does NOT reproduce on the pinned baseline.
	 *   (2) a deterministic shutdown hang under migratable=1 (release needs
	 *       SIGKILL); pinned shuts down cleanly.
	 * (bug #2 protocol-read-park is confirmed SUBSUMED/safe -- not a blocker.)
	 * RE-ENABLED (2026-07-23): both blockers are resolved and re-validated under
	 * real work-steals.  Blocker (1), the guc.c:1396 GUCMemoryContext corruption,
	 * is fixed by guc_free_if_current_context (foreign-context-safe free).
	 * Blocker (2), the fast-shutdown hang, was root-caused on EC2 (gdb backtrace)
	 * to a SetLatch fiber-park quick-exit race: SetLatch's "quick exit if already
	 * set" dropped the wake of a maybe_sleeping fiber owner across the two-phase
	 * fiber park, permanently stranding a backend parked in a CV/AIO wait.  The
	 * fix (latch.c) falls through to re-deliver the fiber wake when the owner is a
	 * maybe_sleeping fiber in this process; process mode stays byte-for-byte.
	 * Only regular client backends with ssl_sni off migrate (SNI pins all
	 * backends -- the be_tls_open_server assertion is the tripwire).
	 */
	return (child_type == B_BACKEND) && !ssl_sni;
}
#endif

/*
 * Start a regular backend carrier thread.
 *
 * Phase 10 supports one OS thread per regular client backend.  Server-owned
 * worker families are still process-backed or disabled until Phase 11 provides
 * worker thread carriers.
 */
static bool
postmaster_backend_thread_launch(PMChild *pmchild,
								 BackendType child_type, int child_slot,
								 void *startup_data, size_t startup_data_len,
								 const ClientSocket *client_sock)
{
	BackendThreadStart *thread_start;
	PgThread	thread;
	int			rc;

	if (child_type != B_ARCHIVER &&
		child_type != B_BACKEND &&
		child_type != B_AUTOVAC_LAUNCHER &&
		child_type != B_AUTOVAC_WORKER &&
		child_type != B_BG_WRITER &&
		child_type != B_BG_WORKER &&
		child_type != B_CHECKPOINTER &&
		child_type != B_IO_WORKER &&
		child_type != B_LOGGER &&
		child_type != B_SLOTSYNC_WORKER &&
		child_type != B_STARTUP &&
		child_type != B_WAL_RECEIVER &&
		child_type != B_WAL_WRITER &&
		child_type != B_WAL_SUMMARIZER)
	{
		errno = ENOSYS;
		return false;
	}
	if (child_type == B_BACKEND &&
		(client_sock == NULL ||
		 startup_data == NULL ||
		 startup_data_len != sizeof(BackendStartupData)))
	{
		errno = EINVAL;
		return false;
	}
	if ((child_type == B_ARCHIVER ||
		 child_type == B_AUTOVAC_LAUNCHER ||
		 child_type == B_AUTOVAC_WORKER ||
		 child_type == B_BG_WRITER ||
		 child_type == B_BG_WORKER ||
		 child_type == B_CHECKPOINTER ||
		 child_type == B_IO_WORKER ||
		 child_type == B_LOGGER ||
		 child_type == B_SLOTSYNC_WORKER ||
		 child_type == B_STARTUP ||
		 child_type == B_WAL_RECEIVER ||
		 child_type == B_WAL_WRITER ||
		 child_type == B_WAL_SUMMARIZER) &&
		(client_sock != NULL ||
		 (child_type != B_BG_WORKER &&
		  (startup_data != NULL || startup_data_len != 0)) ||
		 (child_type == B_BG_WORKER &&
		  (startup_data == NULL ||
		   startup_data_len != sizeof(BackgroundWorker) ||
		   !BackgroundWorkerCanUseThreadCarrier((BackgroundWorker *) startup_data)))))
	{
		errno = EINVAL;
		return false;
	}

	if (IsExternalConnectionBackend(child_type))
		((BackendStartupData *) startup_data)->fork_started = GetCurrentTimestamp();

#ifdef WIN32
	errno = ENOSYS;
	return false;
#else
	InitializePgThreadRuntime(backend_thread_exit);

	thread_start = backend_thread_start_alloc();
	if (thread_start == NULL)
	{
		errno = ENOMEM;
		return false;
	}

	thread_start->publication.kind = BACKEND_THREAD_START_DEDICATED;
	thread_start->publication.pmchild = pmchild;
	thread_start->child_type = child_type;
	thread_start->child_slot = child_slot;
	if (child_type == B_BACKEND)
	{
		thread_start->startup_data = *((BackendStartupData *) startup_data);
		thread_start->client_sock = *client_sock;
		thread_start->client_sock.sock = dup(client_sock->sock);
	}
	else if (child_type == B_BG_WORKER)
	{
		MemSet(&thread_start->startup_data, 0, sizeof(thread_start->startup_data));
		thread_start->bgworker_startup_data = *((BackgroundWorker *) startup_data);
		MemSet(&thread_start->client_sock, 0, sizeof(thread_start->client_sock));
		thread_start->client_sock.sock = PGINVALID_SOCKET;
	}
	else
	{
		MemSet(&thread_start->startup_data, 0, sizeof(thread_start->startup_data));
		MemSet(&thread_start->bgworker_startup_data, 0,
			   sizeof(thread_start->bgworker_startup_data));
		MemSet(&thread_start->client_sock, 0, sizeof(thread_start->client_sock));
		thread_start->client_sock.sock = PGINVALID_SOCKET;
	}
	thread_start->startup_session_timezone = session_timezone;
	thread_start->startup_log_timezone = log_timezone;
	pg_atomic_init_u32(&thread_start->launch_registered, 0);
	pg_atomic_init_u32(&thread_start->exit_claimed, 0);
	pg_atomic_init_u32(&thread_start->start_claimed, 0);
	pg_atomic_init_u32(&thread_start->fiber_entered, 0);
	thread_start->launch_time = GetCurrentTimestamp();

	if (child_type == B_BACKEND && thread_start->client_sock.sock < 0)
	{
		int			save_errno = errno;

		backend_thread_start_release(thread_start);
		errno = save_errno;
		return false;
	}

	InitializePgThreadBackendRuntimeState(&thread_start->runtime_state,
										  thread_start->child_type, NULL,
										  NULL);
#ifdef USE_XTC_CARRIER

	/*
	 * Phase D: decide, on the postmaster thread (where ssl_sni is
	 * authoritative), whether this fiber may be work-stolen across carrier
	 * loops, and record it on the fiber-owned carrier root.  The carrier layer
	 * reads it back at the spawn site (to set xtc_proc_opts_t.migratable) and
	 * at runtime via xtc_proc_userdata() (xtc_pg_backend_fiber_is_migratable),
	 * so the spawn-time and runtime views cannot disagree.  Only client
	 * backends with ssl_sni off migrate; see xtc_carrier_migratable.
	 */
	thread_start->runtime_state.carrier.migratable =
		xtc_carrier_migratable(thread_start->child_type);
#endif
	thread_start->publication.postmaster_latch = MyLatch;
	if (thread_start->publication.postmaster_latch == NULL)
		thread_start->publication.postmaster_latch = PgCurrentLocalLatchData();
	Assert(thread_start->publication.postmaster_latch != NULL);

#ifdef USE_XTC_CARRIER
	/*
	 * xtc-carrier: run this backend/worker as an xtc fiber on the carrier
	 * loop pool instead of a raw pthread.  The fiber body is the tree's own
	 * backend_thread_entry, so all thread-per-session/worker init is reused
	 * unchanged; only the carrier differs.  Blocking waits route through the
	 * xtc loop (see waiteventset.c).  For B_BACKEND the dup()'d
	 * MyClientSocket->sock is the wait fd; worker families have no client
	 * socket.
	 */
	if (xtc_carrier_eligible(child_type))
	{
		rc = xtc_pg_launch_backend_fiber(backend_thread_entry, thread_start);
		if (rc != 0)
		{
			if (child_type == B_BACKEND)
				closesocket(thread_start->client_sock.sock);
			backend_thread_start_release(thread_start);
			errno = rc;
			return false;
		}
		elog(LOG, "xtc: %s launched as xtc fiber (child_slot=%d)",
			 PostmasterChildName(child_type), child_slot);
		postmaster_thread_carriers_started = true;
		/*
		 * No PgThread handle for the fiber path: the fiber runs on a shared
		 * carrier loop, not a dedicated joinable pthread.  Classify it as a
		 * pooled-logical child so the postmaster reaper releases the slot via
		 * process_pm_pooled_logical_exit() (no pthread_join) when the fiber
		 * exits, letting shutdown's PM_WAIT_BACKENDS complete.  Publish the
		 * logical backend so signal/wake routing still targets it.
		 */
		PostmasterChildSetPooledLogical(pmchild);
		PostmasterChildPublishLogicalBackend(pmchild,
											 &thread_start->runtime_state.logical.backend);

		/*
		 * A fiber-backed autovac worker can be canceled by the launcher's
		 * worker-start-timeout before its fiber is ever scheduled (see
		 * ReapOrphanedThreadedWorker).  Record the BackendThreadStart so the
		 * postmaster can reap the orphaned slot from the launcher-cancel path.
		 */
		if (child_type == B_AUTOVAC_WORKER)
			pmchild->carrier_orphan_start = thread_start;
		pg_atomic_write_u32(&thread_start->launch_registered, 1);
		return true;
	}
#endif

	rc = pg_thread_create(&thread, "postgres backend",
						  backend_thread_entry, thread_start);
	if (rc != 0)
	{
		if (child_type == B_BACKEND)
			closesocket(thread_start->client_sock.sock);
		backend_thread_start_release(thread_start);
		errno = rc;
		return false;
	}

	postmaster_thread_carriers_started = true;
	PostmasterChildSetThread(pmchild, &thread);
	PostmasterChildPublishLogicalBackend(pmchild,
										 &thread_start->runtime_state.logical.backend);
	pg_atomic_write_u32(&thread_start->launch_registered, 1);
	return true;
#endif
}

/*
 * Phase 19 Increment 2: launch a client backend as an isolated, forked+exec'd
 * process backend (the process-fallback route) instead of a carrier fiber.
 *
 * This is used for a session that cannot run on a shared-address-space carrier
 * (currently: forced by xtc_force_process_fallback; later: a session needing a
 * process-only extension).  It MUST fork+exec, not bare fork: carriers run in
 * the postmaster process itself, so once carriers exist the postmaster is
 * multithreaded and a fork-without-exec child would inherit locked
 * sibling-thread mutexes.  internal_forkexec() produces a clean exec'd child
 * (arriving in SubPostmasterMain) that re-attaches shared memory and restores
 * backend variables, then runs as an ordinary supervised process backend --
 * counted against MaxConnections, reaped by the postmaster, and (unlike an
 * in-carrier crash) a crash in it is a normal single-backend crash, not a
 * whole-server fail-stop.
 *
 * Re-attach needs a nameable shared segment: an exec'd child cannot re-attach
 * anonymous mmap.  If shared_memory_type is mmap we cannot use the fallback, so
 * we refuse with a clear, actionable error rather than crash the child.
 */
static bool
postmaster_pooled_protocol_process_fallback(PMChild *pmchild, int child_slot,
											void *startup_data,
											size_t startup_data_len,
											const ClientSocket *client_sock)
{
#if defined(WIN32) || !defined(USE_XTC_PROCESS_FALLBACK)
	errno = ENOSYS;
	return false;
#else
	pid_t		pid;

	if (client_sock == NULL ||
		startup_data == NULL ||
		startup_data_len != sizeof(BackendStartupData))
	{
		errno = EINVAL;
		return false;
	}

	if (shared_memory_type == SHMEM_TYPE_MMAP)
	{
		ereport(LOG,
				(errmsg("cannot start a process-fallback backend with shared_memory_type=mmap"),
				 errdetail("A forked+exec'd process-fallback backend must re-attach shared memory, which anonymous mmap segments do not support."),
				 errhint("Set shared_memory_type=sysv to enable process-fallback backends under multithreaded=on.")));
		errno = ENOTSUP;
		return false;
	}

	pid = internal_forkexec(B_BACKEND, child_slot,
							startup_data, startup_data_len, client_sock);
	if (pid < 0)
		return false;

	PostmasterChildSetProcess(pmchild, pid);
	xtc_pg_runtime_counter_inc(XTC_PG_RC_PROCESS_FALLBACKS);	/* fusion F1 */
	return true;
#endif
}

static bool
postmaster_pooled_protocol_launch(PMChild *pmchild, int child_slot,
								  void *startup_data, size_t startup_data_len,
								  const ClientSocket *client_sock)
{
#ifdef WIN32
	errno = ENOSYS;
	return false;
#else
	BackendPooledLogicalStart *logical_start;

	if (client_sock == NULL ||
		startup_data == NULL ||
		startup_data_len != sizeof(BackendStartupData))
	{
		errno = EINVAL;
		return false;
	}

	InitializePgThreadRuntime(backend_thread_exit);

	/*
	 * Phase 19 Increment 2: process-fallback route.  A session that cannot run
	 * on a shared-address-space carrier (a process-only extension) must run in
	 * an isolated process backend instead.  For now this is driven by the
	 * xtc_force_process_fallback developer knob, which forces every pooled
	 * client backend down the fallback route so the fork+exec path can be
	 * exercised deterministically; real per-session detection is a later
	 * increment.
	 *
	 * The launch MUST be fork+exec, never a bare fork: carriers run in the
	 * postmaster process itself, so once carriers exist the postmaster is
	 * multithreaded and a forked-without-exec child would inherit locked
	 * sibling-thread mutexes.  internal_forkexec() gives a clean exec'd child
	 * that re-attaches shared memory and restores backend variables, arriving
	 * in SubPostmasterMain and then running as an ordinary supervised process
	 * backend.  Re-attach requires a nameable segment, so this needs
	 * shared_memory_type != mmap (an exec'd child cannot re-attach anonymous
	 * mmap); otherwise we keep the fail-closed behaviour.
	 */
	if (xtc_force_process_fallback)
		return postmaster_pooled_protocol_process_fallback(pmchild, child_slot,
														   startup_data,
														   startup_data_len,
														   client_sock);

	if (!backend_pooled_protocol_start_pool())
		return false;

	logical_start = backend_pooled_logical_start_alloc();
	if (logical_start == NULL)
	{
		errno = ENOMEM;
		return false;
	}
	MemSet(logical_start, 0, sizeof(*logical_start));

	logical_start->publication.kind = BACKEND_THREAD_START_POOLED_LOGICAL;
	logical_start->publication.pmchild = pmchild;
	logical_start->publication.postmaster_latch = MyLatch;
	if (logical_start->publication.postmaster_latch == NULL)
		logical_start->publication.postmaster_latch = PgCurrentLocalLatchData();
	Assert(logical_start->publication.postmaster_latch != NULL);
	logical_start->startup_data = *((BackendStartupData *) startup_data);
	logical_start->startup_data.fork_started = GetCurrentTimestamp();
	logical_start->client_sock = *client_sock;
	logical_start->client_sock.sock = dup(client_sock->sock);
	if (logical_start->client_sock.sock < 0)
	{
		int			save_errno = errno;

		backend_pooled_logical_start_release(logical_start);
		errno = save_errno;
		return false;
	}

	InitializePgThreadBackendLogicalState(&logical_start->logical, NULL,
										  B_BACKEND, NULL, NULL);
	PostmasterChildSetPooledLogical(pmchild);
	PostmasterChildPublishLogicalBackend(pmchild,
										 &logical_start->logical.backend);
	backend_pooled_protocol_enqueue(logical_start);
	backend_pooled_protocol_signal_work();
	backend_pooled_protocol_maybe_start_carrier_for_work();
	postmaster_thread_carriers_started = true;
	return true;
#endif
}

#ifndef WIN32
static bool
backend_pooled_protocol_start_pool(void)
{
	if (pooled_protocol_carrier_count > 0)
		return true;

	return backend_pooled_protocol_start_one_carrier();
}

static bool
backend_pooled_protocol_start_one_carrier(void)
{
	BackendPooledCarrierStart *carrier_start;
	int			carrier_limit;
	int			carrier_index;
	int			rc;

	carrier_limit = PgRuntimePooledProtocolCarrierLimit();
	if (carrier_limit <= 0)
	{
		errno = EINVAL;
		return false;
	}
	if (pooled_protocol_carrier_count >= carrier_limit)
		return true;

#ifndef WIN32
	/* Create the shared wake eventfd once, before the first carrier starts. */
	if (pooled_protocol_wake_fd < 0)
	{
		pooled_protocol_wake_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
		if (pooled_protocol_wake_fd < 0)
		{
			elog(LOG, "could not create pooled protocol wake eventfd: %m");
			/* Non-fatal: carriers fall back to the timed cond wait below. */
		}
	}
#endif

	carrier_start = malloc(sizeof(BackendPooledCarrierStart));
	if (carrier_start == NULL)
	{
		errno = ENOMEM;
		return false;
	}
	MemSet(carrier_start, 0, sizeof(*carrier_start));

	carrier_index = pooled_protocol_carrier_count;
	InitializePgThreadCarrierRuntimeState(&carrier_start->carrier);
	carrier_start->carrier_index = carrier_index;
	carrier_start->startup_session_timezone = session_timezone;
	carrier_start->startup_log_timezone = log_timezone;
	carrier_start->selfpipe_readfd = -1;
	carrier_start->selfpipe_writefd = -1;

	/*
	 * Create the carrier's latch self-pipe HERE, on the postmaster thread,
	 * before the carrier thread starts.  A pooled-protocol carrier is a
	 * long-lived, runtime-critical thread; if it created its self-pipe itself
	 * (inside InitializeWaitEventSupport) and pipe() failed under fd
	 * exhaustion, the resulting elog(FATAL) would run the per-backend exit
	 * path on a thread that is not a backend and then pthread_exit() the
	 * carrier -- taking the whole threaded server down.  Doing it here lets an
	 * fd-exhaustion failure fail closed (refuse this carrier, and thus the
	 * triggering connection) with the runtime intact, matching how fork mode
	 * refuses a backend when out of resources.
	 */
#ifndef WIN32
	if (WaitEventSetUsesSelfPipe())
	{
		int			pipefd[2];

		if (pipe(pipefd) < 0)
		{
			int			save_errno = errno;

			elog(LOG, "could not create pooled protocol carrier self-pipe: %m");
			free(carrier_start);
			errno = save_errno;
			return false;
		}
		if (fcntl(pipefd[0], F_SETFL, O_NONBLOCK) == -1 ||
			fcntl(pipefd[1], F_SETFL, O_NONBLOCK) == -1 ||
			fcntl(pipefd[0], F_SETFD, FD_CLOEXEC) == -1 ||
			fcntl(pipefd[1], F_SETFD, FD_CLOEXEC) == -1)
		{
			int			save_errno = errno;

			elog(LOG, "could not configure pooled protocol carrier self-pipe: %m");
			(void) close(pipefd[0]);
			(void) close(pipefd[1]);
			free(carrier_start);
			errno = save_errno;
			return false;
		}
		carrier_start->selfpipe_readfd = pipefd[0];
		carrier_start->selfpipe_writefd = pipefd[1];
	}
#endif

	/*
	 * Fusion F1: register the runtime counters once, BEFORE spawning the
	 * carrier thread, so the counter handles are published (happens-before)
	 * any increment the new carrier performs -- otherwise the carrier could
	 * race the postmaster's register() and undercount a few early events.
	 * register() runs on the postmaster thread here; it is idempotent and a
	 * no-op in a non-carrier build.
	 */
	xtc_pg_runtime_counters_register();

	rc = pg_thread_create(&carrier_start->thread,
						  "postgres pooled protocol carrier",
						  backend_pooled_protocol_carrier_entry,
						  carrier_start);
	if (rc != 0)
	{
#ifndef WIN32
		if (carrier_start->selfpipe_readfd >= 0)
			(void) close(carrier_start->selfpipe_readfd);
		if (carrier_start->selfpipe_writefd >= 0)
			(void) close(carrier_start->selfpipe_writefd);
#endif
		free(carrier_start);
		errno = rc;
		return false;
	}

	pooled_protocol_carrier_count++;
	pooled_protocol_pool_started = true;
	postmaster_thread_carriers_started = true;

	/* Fusion F1: count each carrier that actually started. */
	xtc_pg_runtime_counter_inc(XTC_PG_RC_CARRIERS_STARTED);
	return true;
}

static void
backend_pooled_protocol_maybe_start_carrier_for_work(void)
{
	int			queue_length;
	uint32		idle_carriers;

	if (!pooled_protocol_pool_started)
		return;
	if (pooled_protocol_carrier_count >= PgRuntimePooledProtocolCarrierLimit())
		return;

	queue_length = backend_pooled_protocol_queue_count();
	if (queue_length <= 0)
		return;

	idle_carriers = backend_pooled_protocol_idle_carrier_count();
	if ((uint32) queue_length <= idle_carriers)
		return;

	(void) backend_pooled_protocol_start_one_carrier();
}

static void
backend_pooled_protocol_enqueue(BackendPooledLogicalStart *logical_start)
{
	int			rc;

	Assert(logical_start != NULL);
	Assert(logical_start->next == NULL);

	rc = pthread_mutex_lock(&pooled_protocol_queue_mutex);
	if (rc != 0)
	{
		errno = rc;
		elog(FATAL, "could not lock pooled protocol queue: %m");
	}

	if (pooled_protocol_queue_tail != NULL)
		pooled_protocol_queue_tail->next = logical_start;
	else
		pooled_protocol_queue_head = logical_start;
	pooled_protocol_queue_tail = logical_start;
	pooled_protocol_queue_length++;

	rc = pthread_mutex_unlock(&pooled_protocol_queue_mutex);
	if (rc != 0)
	{
		errno = rc;
		elog(FATAL, "could not unlock pooled protocol queue: %m");
	}
}

static BackendPooledLogicalStart *
backend_pooled_protocol_dequeue(void)
{
	BackendPooledLogicalStart *logical_start;
	int			rc;

	rc = pthread_mutex_lock(&pooled_protocol_queue_mutex);
	if (rc != 0)
	{
		errno = rc;
		elog(FATAL, "could not lock pooled protocol queue: %m");
	}

	logical_start = pooled_protocol_queue_head;
	if (logical_start != NULL)
	{
		pooled_protocol_queue_head = logical_start->next;
		if (pooled_protocol_queue_head == NULL)
			pooled_protocol_queue_tail = NULL;
		logical_start->next = NULL;
		Assert(pooled_protocol_queue_length > 0);
		pooled_protocol_queue_length--;
	}

	rc = pthread_mutex_unlock(&pooled_protocol_queue_mutex);
	if (rc != 0)
	{
		errno = rc;
		elog(FATAL, "could not unlock pooled protocol queue: %m");
	}

	return logical_start;
}

static int
backend_pooled_protocol_queue_count(void)
{
	int			queue_length;
	int			rc;

	rc = pthread_mutex_lock(&pooled_protocol_queue_mutex);
	if (rc != 0)
	{
		errno = rc;
		elog(FATAL, "could not lock pooled protocol queue: %m");
	}

	queue_length = pooled_protocol_queue_length;

	rc = pthread_mutex_unlock(&pooled_protocol_queue_mutex);
	if (rc != 0)
	{
		errno = rc;
		elog(FATAL, "could not unlock pooled protocol queue: %m");
	}

	return queue_length;
}

static uint32
backend_pooled_protocol_idle_carrier_count(void)
{
	return PgRuntimePooledProtocolIdleCarrierCount();
}

static void
backend_pooled_protocol_wake_signal(void)
{
	/*
	 * Wake any carrier blocked in WaitParkedReads' poll().  Writing a u64 to the
	 * eventfd makes it readable; the carrier drains it after poll() returns.  A
	 * write while it is already signalled just adds to the counter (harmless --
	 * we drain the whole counter).  EFD_NONBLOCK so a full counter cannot block
	 * the signaller.
	 */
#ifndef WIN32
	if (pooled_protocol_wake_fd >= 0)
	{
		uint64		one = 1;
		ssize_t		w;

		do
			w = write(pooled_protocol_wake_fd, &one, sizeof(one));
		while (w < 0 && errno == EINTR);
		/* EAGAIN (counter saturated) is fine: it is already readable. */
	}
#endif
}

static void
backend_pooled_protocol_wake_drain(void)
{
#ifndef WIN32
	if (pooled_protocol_wake_fd >= 0)
	{
		uint64		buf;
		ssize_t		r;

		do
			r = read(pooled_protocol_wake_fd, &buf, sizeof(buf));
		while (r < 0 && errno == EINTR);
		/* EAGAIN means another carrier already drained it -- fine. */
	}
#endif
}

static void
backend_pooled_protocol_signal_work(void)
{
	int			rc;

	rc = pthread_mutex_lock(&pooled_protocol_queue_mutex);
	if (rc != 0)
	{
		errno = rc;
		elog(FATAL, "could not lock pooled protocol queue: %m");
	}

	rc = pthread_cond_signal(&pooled_protocol_queue_cond);
	if (rc != 0)
	{
		errno = rc;
		elog(FATAL, "could not signal pooled protocol queue: %m");
	}

	rc = pthread_mutex_unlock(&pooled_protocol_queue_mutex);
	if (rc != 0)
	{
		errno = rc;
		elog(FATAL, "could not unlock pooled protocol queue: %m");
	}

	/* Also wake carriers blocked in poll() on the wake eventfd. */
	backend_pooled_protocol_wake_signal();
}

static void
backend_pooled_protocol_signal_ready_work(int count)
{
	int			rc;

	if (count <= 0)
		return;

	rc = pthread_mutex_lock(&pooled_protocol_queue_mutex);
	if (rc != 0)
	{
		errno = rc;
		elog(FATAL, "could not lock pooled protocol queue: %m");
	}

	for (int i = 0; i < count; i++)
	{
		rc = pthread_cond_signal(&pooled_protocol_queue_cond);
		if (rc != 0)
		{
			errno = rc;
			elog(FATAL, "could not signal pooled protocol queue: %m");
		}
	}

	rc = pthread_mutex_unlock(&pooled_protocol_queue_mutex);
	if (rc != 0)
	{
		errno = rc;
		elog(FATAL, "could not unlock pooled protocol queue: %m");
	}
}

static void
backend_pooled_protocol_wait_for_work(long timeout_us)
{
	struct timespec deadline;
	int			rc;

	rc = pthread_mutex_lock(&pooled_protocol_queue_mutex);
	if (rc != 0)
	{
		errno = rc;
		elog(FATAL, "could not lock pooled protocol queue: %m");
	}

	if (pooled_protocol_queue_length == 0)
	{
		backend_pooled_protocol_deadline_after(timeout_us, &deadline);
		rc = pthread_cond_timedwait(&pooled_protocol_queue_cond,
									&pooled_protocol_queue_mutex,
									&deadline);
		if (rc != 0 && rc != ETIMEDOUT)
		{
			errno = rc;
			elog(FATAL, "could not wait on pooled protocol queue: %m");
		}
	}

	rc = pthread_mutex_unlock(&pooled_protocol_queue_mutex);
	if (rc != 0)
	{
		errno = rc;
		elog(FATAL, "could not unlock pooled protocol queue: %m");
	}
}

static void
backend_pooled_protocol_deadline_after(long timeout_us,
									   struct timespec *deadline)
{
	struct timeval now;
	long		nsec;

	Assert(deadline != NULL);
	Assert(timeout_us >= 0);

	gettimeofday(&now, NULL);
	deadline->tv_sec = now.tv_sec + timeout_us / USECS_PER_SEC;
	nsec = now.tv_usec * 1000L + (timeout_us % USECS_PER_SEC) * 1000L;
	if (nsec >= 1000000000L)
	{
		deadline->tv_sec++;
		nsec -= 1000000000L;
	}
	deadline->tv_nsec = nsec;
}

static BackendPooledLogicalStart *
backend_pooled_logical_start_from_backend(PgBackend *backend)
{
	char	   *logical_base;

	Assert(backend != NULL);

	logical_base = (char *) backend -
		offsetof(PgThreadBackendLogicalState, backend);
	return (BackendPooledLogicalStart *)
		(logical_base - offsetof(BackendPooledLogicalStart, logical));
}

static void
backend_pooled_protocol_carrier_entry(void *arg)
{
	BackendPooledCarrierStart *carrier_start =
		(BackendPooledCarrierStart *) arg;
	PgBackend **scratch;
	struct pollfd *poll_scratch;
	int			max_scratch_backends;

	sigprocmask(SIG_SETMASK, &BlockSig, NULL);

	PgSetCurrentCarrier(&carrier_start->carrier);
	PgRuntimeSetCurrentWork(carrier_start->carrier.runtime,
							&carrier_start->carrier,
							NULL, NULL, NULL, NULL, false);
	MyBackendType = B_BACKEND;
	MyProcPid = (int) getpid();
	IsUnderPostmaster = true;
	session_timezone = carrier_start->startup_session_timezone;
	log_timezone = carrier_start->startup_log_timezone;
	MemoryContextInit();
	WaitEventSetPresupplySelfPipe(carrier_start->selfpipe_readfd,
								 carrier_start->selfpipe_writefd);
	InitializeWaitEventSupport();
	(void) set_stack_base();
	backend_thread_init_random_state();

	/*
	 * The carrier's self-pipe fds were pre-created on the postmaster thread
	 * (see backend_pooled_protocol_start_one_carrier), so InitializeWaitEventSupport()
	 * above cannot elog(FATAL) here on fd exhaustion -- the dominant, reported
	 * carrier-startup crash path.  Two same-class elog(FATAL) hazards still
	 * remain on this runtime-critical carrier thread and are deliberately left
	 * as low-probability follow-ups (Phase 16 / Gate E2-Extensions owns fully
	 * FATAL-safe carrier startup): the scratch allocations below can promote an
	 * OOM ERROR to FATAL, and PgRuntimeProtocolSchedulerRegisterCarrier() below
	 * can FATAL.  The register FATAL is effectively unreachable because the
	 * postmaster gates carrier creation on the same carrier_limit before
	 * starting this thread (see backend_pooled_protocol_start_one_carrier), so
	 * a carrier that got here always fits under the limit.  Neither hazard is
	 * the fd-exhaustion path this fix targets; do not treat carrier startup as
	 * fully FATAL-safe just because the self-pipe pipe() moved out.
	 */
	max_scratch_backends = MaxBackends > 0 ? MaxBackends : 1024;
	scratch = MemoryContextAlloc(TopMemoryContext,
								 sizeof(PgBackend *) * max_scratch_backends);
	poll_scratch = MemoryContextAlloc(TopMemoryContext,
									  sizeof(struct pollfd) *
									  (max_scratch_backends + 2));

	if (!PgRuntimeProtocolSchedulerRegisterCarrier(CurrentPgRuntime,
												   CurrentPgCarrier))
		elog(FATAL, "could not register pooled protocol carrier");

	for (;;)
	{
		BackendPooledLogicalStart *logical_start;
		PgBackend  *backend;
		int			nready;

		Assert(CurrentPgCarrier == &carrier_start->carrier);
		Assert(CurrentPgBackend == NULL);
		Assert(CurrentPgSession == NULL);
		Assert(CurrentPgConnection == NULL);
		Assert(CurrentPgExecution == NULL);

		backend = PgCarrierLeaseRunnableProtocolBackend(CurrentPgCarrier);
		if (backend != NULL)
		{
			xtc_pg_runtime_counter_inc(XTC_PG_RC_SESSIONS_RESUMED);	/* fusion F1 */
			logical_start =
				backend_pooled_logical_start_from_backend(backend);
			backend_pooled_protocol_resume_logical_start(logical_start);
			continue;
		}

		logical_start = backend_pooled_protocol_dequeue();
		if (logical_start != NULL)
		{
			xtc_pg_runtime_counter_inc(XTC_PG_RC_SESSIONS_LEASED);	/* fusion F1 */
			backend_pooled_protocol_run_logical_start(carrier_start,
													  logical_start);
			continue;
		}

		nready = PgRuntimeProtocolSchedulerWaitParkedReads(CurrentPgRuntime,
														   scratch,
														   poll_scratch,
														   max_scratch_backends,
														   pooled_protocol_wake_fd,
														   1000L);
		if (nready > 0)
		{
			xtc_pg_runtime_counter_add(XTC_PG_RC_WAKES_DELIVERED, nready);	/* fusion F1 */
			backend_pooled_protocol_wake_drain();
			backend_pooled_protocol_signal_ready_work(nready);
			continue;
		}

		/*
		 * No parked read ready.  If the wake eventfd fired (new queued work),
		 * drain it and loop to pick it up.  With the wake fd in the poll set new
		 * sessions no longer need a short poll timeout to be noticed, so the
		 * 1000ms above is a safety-net timeout, not a ~100x/s self-wake.
		 * wait_for_work covers a carrier that had NO parked fds to poll
		 * (WaitParkedReads returns 0 immediately): it blocks on the queue cond,
		 * woken by signal_work.
		 */
		backend_pooled_protocol_wake_drain();
		xtc_pg_runtime_counter_inc(XTC_PG_RC_QUEUE_WAITS);	/* fusion F1 */
		backend_pooled_protocol_wait_for_work(10000L);
	}
}

static void
backend_pooled_protocol_run_logical_start(BackendPooledCarrierStart *carrier_start,
										  BackendPooledLogicalStart *logical_start)
{
	PgSession  *session;

	Assert(carrier_start != NULL);
	Assert(logical_start != NULL);
	Assert(CurrentPgCarrier == &carrier_start->carrier);
	Assert(CurrentPgBackend == NULL);

	PgCarrierAttachBackend(CurrentPgCarrier, &logical_start->logical.backend,
						   &logical_start->logical.session,
						   &logical_start->logical.connection,
						   &logical_start->logical.execution);
	*PgCurrentBackendThreadStartRef() = logical_start;

	MyPMChildSlot = logical_start->publication.pmchild->child_slot;
	MyBackendType = B_BACKEND;
	MyProcPid = (int) getpid();
	IsUnderPostmaster = true;
	session_timezone = carrier_start->startup_session_timezone;
	log_timezone = carrier_start->startup_log_timezone;

	InitProcessLocalLatch();
	MemoryContextInit();
	InitializeTransactionState();
	InitializeThreadedSessionGUCOptions();
	read_nondefault_variables();
	InitializeLatchWaitSet();
	InitializeThreadedSessionRequiredGUCOptions();
	PgBackendSetInterruptLatch(CurrentPgBackend, MyLatch);

	MyClientSocket = &logical_start->client_sock;
	conn_timing.socket_create = logical_start->startup_data.socket_created;
	conn_timing.fork_start = logical_start->startup_data.fork_started;
	conn_timing.fork_end = GetCurrentTimestamp();
	MyStartTimestamp = GetCurrentTimestamp();
	MyStartTime = timestamptz_to_time_t(MyStartTimestamp);
	backend_thread_init_random_state();

	if (sigsetjmp(logical_start->exit_jmp, 1) != 0)
	{
		logical_start->exit_jmp_valid = false;
		*PgCurrentBackendThreadStartRef() = NULL;
		PgCarrierDetachBackend(CurrentPgCarrier, NULL);
		backend_pooled_logical_start_release(logical_start);
		return;
	}

	logical_start->exit_jmp_valid = true;
	session = BackendStartSessionWithStartupData(&logical_start->startup_data,
												 &logical_start->client_sock,
												 BACKEND_STARTUP_THREAD);

	/*
	 * InitProcess() (run inside BackendStartSessionWithStartupData above) sets
	 * MyLatch during proc setup so a signal that arrived mid-init is not lost.
	 * An affine pooled session then runs its first command directly here, with
	 * no client startup/auth round-trips to cycle the process latch in between.
	 * Left set, that stale signal makes the command's first, un-looped
	 * WaitLatch return WL_LATCH_SET in 0ms -- unlike pg_sleep, which loops until
	 * its deadline.  TAP 009's pooled LWLock deep-wait cases caught this: the
	 * holder's WaitLatch fell straight through, released its LWLock, and
	 * idle-parked instead of staying carrier-pinned.  Clear it once before the
	 * command loop; any genuinely pending interrupt is re-detected from the
	 * backend interrupt mask by CHECK_FOR_INTERRUPTS, not from the latch.
	 */
	if (MyLatch != NULL)
		ResetLatch(MyLatch);

	(void) backend_pooled_protocol_run_attached_logical(logical_start,
														session);
}

static void
backend_pooled_protocol_resume_logical_start(BackendPooledLogicalStart *logical_start)
{
	PgSession  *session;
	uint32		wake_events;

	Assert(logical_start != NULL);
	Assert(CurrentPgBackend == &logical_start->logical.backend);
	Assert(CurrentPgSession == &logical_start->logical.session);

	*PgCurrentBackendThreadStartRef() = logical_start;
	pgstat_ensure_shmem_attached();
	wake_events = CurrentPgBackend->protocol_park.wake_events;
	PgBackendResumeProtocolReadPark(CurrentPgBackend);
	if (wake_events & WL_LATCH_SET)
		ResetLatch(MyLatch);
	session = CurrentPgSession;

	if (sigsetjmp(logical_start->exit_jmp, 1) != 0)
	{
		logical_start->exit_jmp_valid = false;
		*PgCurrentBackendThreadStartRef() = NULL;
		PgCarrierDetachBackend(CurrentPgCarrier, NULL);
		backend_pooled_logical_start_release(logical_start);
		return;
	}

	logical_start->exit_jmp_valid = true;
	(void) backend_pooled_protocol_run_attached_logical(logical_start,
														session);
}

static PgStepResult
backend_pooled_protocol_run_attached_logical(BackendPooledLogicalStart *logical_start,
											 PgSession *session)
{
	for (;;)
	{
		PgStepResult result;

		result = PgSessionRunProtocolSchedulerUntilBoundary(session);
		switch (result)
		{
			case PG_STEP_PARK_PROTOCOL_READ:
				xtc_pg_runtime_counter_inc(XTC_PG_RC_PROTOCOL_PARKS);	/* fusion F1 */
				logical_start->exit_jmp_valid = false;
				*PgCurrentBackendThreadStartRef() = NULL;
				return result;

			case PG_STEP_DONE:
				backend_pooled_protocol_exit_logical(0);

			case PG_STEP_FATAL_EXIT:
				backend_pooled_protocol_exit_logical(1);

			case PG_STEP_CONTINUE:
			case PG_STEP_ERROR_RECOVERED:
				pg_unreachable();
		}
	}
}

pg_noreturn static void
backend_pooled_protocol_exit_logical(int code)
{
	if (CurrentPgRuntime != NULL && CurrentPgBackend != NULL)
		(void) PgRuntimeProtocolSchedulerRemoveBackend(CurrentPgRuntime,
													   CurrentPgBackend);

	PgBackendExit(code);
}
#endif

static void
backend_thread_entry(void *arg)
{
	BackendThreadStart *thread_start = (BackendThreadStart *) arg;

	/*
	 * A carrier thread inherits the postmaster thread's current signal mask,
	 * but process-directed control signals must be handled by the postmaster
	 * thread.  Keep carriers in the same blocked-signal state that a forked
	 * child sees before its child-specific signal setup.
	 */
	sigprocmask(SIG_SETMASK, &BlockSig, NULL);

#ifdef USE_XTC_CARRIER
	/*
	 * xtc-carrier: this path was designed for a fresh pthread that dies after
	 * one backend.  When run as an xtc fiber, one carrier OS thread hosts many
	 * backend fibers in sequence, so the previous fiber's thread-local runtime
	 * state (hot current-cells, current-work bindings, early-session fallback
	 * flags) is still present.  Restore the fresh-thread invariant before
	 * touching any session/GUC/timezone accessor below.
	 */
	if (xtc_in_backend_fiber)
		PgRuntimeResetThreadForNewBackend();
#endif

	PgSetCurrentCarrier(&thread_start->runtime_state.carrier);
	backend_thread_set_current_start(thread_start);
	backend_thread_wait_until_registered(thread_start);
#ifdef USE_XTC_CARRIER
	/*
	 * Mark that the fiber body actually began running.  The postmaster's
	 * launcher-cancel orphan reap (ReapOrphanedThreadedWorker) only targets a
	 * worker whose fiber never entered, so it can never race a fiber that has
	 * started real work.  Set after wait_until_registered so it is ordered
	 * after the postmaster published the PMChild and stored the orphan-start
	 * pointer.
	 */
	if (xtc_in_backend_fiber)
	{
		/*
		 * Publish that the fiber body began, then close the race with
		 * ReapOrphanedThreadedWorker (postmaster thread).  The reaper reaps a
		 * worker whose fiber_entered is still 0 by claiming it and publishing a
		 * synthetic exit -- which RELEASES this PMChild slot.  If it did so in
		 * the window between the reaper reading fiber_entered as 0 and this
		 * write becoming visible, the slot is already gone and we must NOT run
		 * InitProcess -> RegisterPostmasterChildActive on it (under cassert that
		 * trips Assert(PMChildFlags[slot] == PM_CHILD_ASSIGNED) in pmsignal.c;
		 * in production it corrupts the slot's PMChild accounting and wedges
		 * PM_WAIT_BACKENDS at fast stop).
		 *
		 * Resolve with a dedicated single-winner exchange (start_claimed),
		 * distinct from the exit-publication arbiter (exit_claimed) so the
		 * normal exit path in backend_thread_finish is unaffected.  The reaper
		 * claims start_claimed too (only after seeing fiber_entered == 0);
		 * whoever wins start_claimed owns the slot.  Ordering: write
		 * fiber_entered, full barrier, then exchange start_claimed.  If we lose
		 * (reaper already claimed and published a synthetic exit), bail out of
		 * the fiber immediately without touching the released slot and without
		 * freeing thread_start (the reaper's synthetic-exit path leaves it,
		 * matching backend_thread_finish's lost-claim handling).
		 */
		pg_atomic_write_u32(&thread_start->fiber_entered, 1);
		pg_memory_barrier();
		if (pg_atomic_exchange_u32(&thread_start->start_claimed, 1) != 0)
		{
			backend_thread_set_current_start(NULL);
			xtc_pg_backend_fiber_exit(backend_thread_exitstatus(0));
			pg_unreachable();
		}
	}
#endif

	/*
	 * xtc-carrier: install this fiber's own logical roots as current work
	 * BEFORE the per-backend startup below (the My* globals, MyLatch, the
	 * self-pipe, MemoryContextInit(), and the early GUC init), so that whole
	 * window resolves to per-fiber storage instead of the SHARED per-OS-thread
	 * early_execution_fallback / early_session_fallback.  On the xtc carrier
	 * many backend fibers time-share ONE OS thread and the early GUC init
	 * parks the fiber on the process-wide GUC amutex under concurrent startup;
	 * a sibling backend fiber running on the same carrier thread across that
	 * park must not clobber the shared fallback (which otherwise trips
	 * Assert(TopMemoryContext == NULL) / corrupts session-GUC + memory-context
	 * state).  This mirrors the pooled-protocol path, which attaches its
	 * logical roots via PgCarrierAttachBackend() before its own
	 * MemoryContextInit().  It runs before the My* / InitProcessLocalLatch()
	 * setup so MyLatch and the other backend-rooted My* globals below are
	 * established on this fiber's logical backend, not the fallback.
	 * InstallPgThreadBackendRuntimeState() detects the pre-install and skips
	 * the now-redundant populate-fallback-then-adopt copy.
	 */
	PreInstallPgThreadBackendRuntimeState(&thread_start->runtime_state);

	MyBackendType = thread_start->child_type;
	MyPMChildSlot = thread_start->child_slot;
	MyProcPid = (int) getpid();
	IsUnderPostmaster = true;
	session_timezone = thread_start->startup_session_timezone;
	log_timezone = thread_start->startup_log_timezone;

	InitializeWaitEventSupport();
	InitProcessLocalLatch();
	MemoryContextInit();
	InitializeTransactionState();
	InitializeThreadedSessionGUCOptions();
	read_nondefault_variables();
	InitializeLatchWaitSet();
	InstallPgThreadBackendRuntimeState(&thread_start->runtime_state);
	if (thread_start->child_type == B_BACKEND)
	{
		if (!PgRuntimeProtocolSchedulerRegisterCarrier(CurrentPgRuntime,
													   CurrentPgCarrier))
		{
			if (PgRuntimePooledProtocolRequested())
				ereport(DEBUG1,
						(errmsg_internal("pooled protocol staging carrier exceeded configured carrier limit")));
			else
				elog(FATAL, "could not register threaded protocol scheduler carrier");
		}
	}
	(void) set_stack_base();
	PgBackendSetInterruptLatch(CurrentPgBackend, MyLatch);

	MyStartTimestamp = GetCurrentTimestamp();
	MyStartTime = timestamptz_to_time_t(MyStartTimestamp);
	backend_thread_init_random_state();

	if (thread_start->child_type == B_BACKEND)
		backend_thread_run_backend(thread_start);
	else
		backend_thread_run_worker(thread_start);
}

static void
backend_thread_run_backend(BackendThreadStart *thread_start)
{
	/* Temporary until real backend startup owns the copied ClientSocket. */
	MyClientSocket = &thread_start->client_sock;

	conn_timing.socket_create = thread_start->startup_data.socket_created;
	conn_timing.fork_start = thread_start->startup_data.fork_started;
	conn_timing.fork_end = GetCurrentTimestamp();

	BackendMainWithStartupData(&thread_start->startup_data,
							   &thread_start->client_sock,
							   BACKEND_STARTUP_THREAD);
	pg_unreachable();
}

static void
backend_thread_run_worker(BackendThreadStart *thread_start)
{
	ereport(DEBUG1,
			(errmsg_internal("starting %s thread carrier",
							 PostmasterChildName(thread_start->child_type))));

	/*
	 * Thread-compatible background workers publish their postmaster-visible
	 * startup only after
	 * ThreadedBackendStartupComplete(), so dynamic waiters cannot terminate
	 * them while InitProcess(), BaseInit(), or function lookup are still in
	 * progress.  The autovacuum launcher performs backend initialization
	 * before entering its no-database launcher loop, while autovacuum workers
	 * publish their worker slot before connecting to the selected database and
	 * running table work.  The slot sync worker publishes startup completion
	 * after connecting to the local database and before connecting to the
	 * primary.  The startup process,
	 * archiver, WAL receiver, and WAL summarizer follow the auxiliary-process
	 * common startup path, publish their wakeup/progress state in shared
	 * memory, and keep their per-loop work state backend-local, so they can
	 * start without a serialized startup section.
	 */
	if (thread_start->child_type == B_BG_WORKER)
		child_process_kinds[thread_start->child_type].main_fn(&thread_start->bgworker_startup_data,
															  sizeof(BackgroundWorker));
	else
		child_process_kinds[thread_start->child_type].main_fn(NULL, 0);
	pg_unreachable();
}

static BackendThreadStart *
backend_thread_current_start(void)
{
	BackendThreadPublication *publication;

	publication = backend_thread_current_publication();
	if (publication == NULL)
		return NULL;
	if (publication->kind != BACKEND_THREAD_START_DEDICATED)
		return NULL;

	return (BackendThreadStart *) publication;
}

static BackendThreadPublication *
backend_thread_current_publication(void)
{
	return (BackendThreadPublication *) *PgCurrentBackendThreadStartRef();
}

static void
backend_thread_set_current_start(BackendThreadStart *thread_start)
{
	*PgCurrentBackendThreadStartRef() = thread_start;
}

static void
backend_thread_wait_until_registered(BackendThreadStart *thread_start)
{
	while (pg_atomic_read_u32(&thread_start->launch_registered) == 0)
		pg_usleep(1000L);
}

static void
backend_thread_init_random_state(void)
{
	if (unlikely(!pg_prng_strong_seed(&pg_global_prng_state)))
	{
		uint64		rseed;

		rseed = ((uint64) MyProcPid) ^
			((uint64) MyStartTimestamp << 12) ^
			((uint64) MyStartTimestamp >> 20) ^
			((uint64) PgCurrentBackendId() << 32);

		pg_prng_seed(&pg_global_prng_state, rseed);
	}
}

static void
backend_thread_clear_deleted_retained_memory_contexts(void)
{
	if (CurrentPgExecution == NULL)
		return;

	CurrentPgExecution->memory_contexts.error_context = NULL;
	CurrentPgExecution->memory_contexts.current_context = NULL;
}

static void
backend_thread_free_deleted_retained_memory_contexts(void)
{
	if (CurrentPgBackend != NULL)
		AllocSetFreeContextFreelists(CurrentPgBackend->memory_manager.context_freelists,
									 PG_BACKEND_ALLOCSET_NUM_FREELISTS);
}

static void
backend_thread_maybe_trim_reclaimed_memory(Size reclaimed)
{
#if defined(__GLIBC__)
	bool		trim_now = false;
	int			rc;

	if (reclaimed == 0)
		return;

	rc = pthread_mutex_lock(&backend_thread_malloc_trim_mutex);
	if (rc != 0)
	{
		errno = rc;
		elog(FATAL, "could not lock backend malloc trim state: %m");
	}

	backend_thread_malloc_trim_pending += reclaimed;
	if (backend_thread_malloc_trim_pending < reclaimed)
		backend_thread_malloc_trim_pending =
			BACKEND_THREAD_MALLOC_TRIM_THRESHOLD;

	if (backend_thread_malloc_trim_pending >=
		BACKEND_THREAD_MALLOC_TRIM_THRESHOLD)
	{
		backend_thread_malloc_trim_pending = 0;
		trim_now = true;
	}

	rc = pthread_mutex_unlock(&backend_thread_malloc_trim_mutex);
	if (rc != 0)
	{
		errno = rc;
		elog(FATAL, "could not unlock backend malloc trim state: %m");
	}

	if (trim_now)
		(void) malloc_trim(0);
#else
	(void) reclaimed;
#endif
}

void
ThreadedBackendStartupComplete(void)
{
	BackendThreadPublication *publication = backend_thread_current_publication();

	if (publication == NULL)
		return;

	PostmasterChildPublishLogicalStartupComplete(publication->pmchild,
												 publication->postmaster_latch);
}

#ifdef USE_XTC_CARRIER
/*
 * Reap a fiber-backed worker that was launched but whose fiber was never
 * scheduled to run.  Called from the postmaster when the autovacuum launcher
 * cancels a worker whose start it timed out on (its worker-start-timeout).
 *
 * The postmaster optimistically publishes a pooled-logical PMChild the moment
 * it hands a worker to the carrier, on the assumption -- true for a forked
 * process, which the OS always eventually schedules -- that the child will run
 * and reap itself.  A fiber can break that assumption: a cross-thread wake to
 * an idle io_uring carrier loop can be missed in the current libxtc, so a
 * worker fiber can sit un-scheduled indefinitely.  If the launcher's
 * start-timeout then cancels the worker (reclaiming its shmem WorkerInfo), the
 * orphaned PMChild is never reconciled with a published exit and
 * PM_WAIT_BACKENDS wedges at fast stop.
 *
 * The launcher-cancel is the authoritative "this worker never started" signal:
 * it fires only while the worker still holds av_startingWorker under
 * AutovacuumLock, i.e. the worker fiber has not reached the point where it
 * claims its WorkerInfo (which is well after it would have set fiber_entered).
 * So a worker whose fiber_entered is still 0 genuinely never ran; publishing a
 * synthetic clean exit for it lets process_pm_pooled_logical_exit() reap the
 * slot exactly as a self-exiting fiber would.  The exit_claimed exchange makes
 * the publish exactly-once: if the fiber does eventually run and reach
 * backend_thread_finish, it loses the claim and skips its own publish, so the
 * slot is never reaped twice.
 *
 * min_age_ms guards against reaping a worker the launcher just (re)launched:
 * after canceling a stuck worker the launcher immediately requests a fresh
 * one, so by the time this runs there can be TWO un-entered workers -- the
 * aged, genuinely-stuck one and a brand-new one.  The launcher-cancel caller
 * passes the worker-start-timeout so only the aged orphan is reaped; the
 * shutdown caller passes 0 because no new worker can launch once shutting
 * down.  Reaps at most one orphan per call; the caller loops if it wants to
 * drain several.
 *
 * Returns true if it reaped an orphan (the caller should re-run the postmaster
 * state machine).  Runs in the postmaster main thread only.
 */
bool
ReapOrphanedThreadedWorker(BackendType child_type, int min_age_ms)
{
	dlist_iter	iter;
	TimestampTz now = GetCurrentTimestamp();

	dlist_foreach(iter, &ActiveChildList)
	{
		PMChild    *pmchild = dlist_container(PMChild, elem, iter.cur);
		BackendThreadStart *thread_start;

		if (pmchild->bkend_type != child_type)
			continue;
		if (!PostmasterChildIsPooledLogical(pmchild))
			continue;
		thread_start = (BackendThreadStart *) pmchild->carrier_orphan_start;
		if (thread_start == NULL)
			continue;

		/*
		 * Only an un-entered fiber is a genuine orphan.  A fiber that has
		 * started running owns its own exit publication; never touch its slot.
		 */
		if (pg_atomic_read_u32(&thread_start->fiber_entered) != 0)
			continue;

		/*
		 * Skip a worker that has not yet aged past min_age_ms -- it may be a
		 * healthy worker the launcher just launched whose fiber is about to
		 * run, not the stuck one we were told about.
		 */
		if (min_age_ms > 0 &&
			!TimestampDifferenceExceeds(thread_start->launch_time, now,
										min_age_ms))
			continue;

		/* Claim the one-time slot ownership for this launch. */
		if (pg_atomic_exchange_u32(&thread_start->start_claimed, 1) != 0)
			continue;			/* fiber won the start race; it owns the slot */

		/*
		 * Publish a synthetic clean exit and stop tracking the orphan pointer.
		 * The fiber owns thread_start's memory for its whole life and frees it
		 * only from backend_thread_finish, so we never free it here (an
		 * un-scheduled fiber leaks this one struct -- bounded and rare).
		 */
		ereport(DEBUG1,
				(errmsg_internal("reaping orphaned %s fiber (never scheduled) at child slot %d",
								 PostmasterChildName(child_type),
								 pmchild->child_slot)));
		pmchild->carrier_orphan_start = NULL;
		PostmasterChildUnpublishLogicalBackend(pmchild);
		PostmasterChildPublishPooledLogicalExit(pmchild, 0, 0, 0,
												thread_start->publication.postmaster_latch);
		return true;
	}

	return false;
}
#endif							/* USE_XTC_CARRIER */

static void
backend_thread_exit(int code)
{
	BackendThreadPublication *publication = backend_thread_current_publication();

	if (publication == NULL)
	{
#ifdef USE_XTC_CARRIER
		if (xtc_in_backend_fiber)
			xtc_pg_backend_fiber_exit(backend_thread_exitstatus(code));
#endif
		pg_thread_exit();
	}

	switch (publication->kind)
	{
		case BACKEND_THREAD_START_DEDICATED:
			backend_thread_finish(code);

		case BACKEND_THREAD_START_POOLED_LOGICAL:
			backend_pooled_logical_finish(code);
	}

	pg_unreachable();
}

static void
backend_thread_finish(int code)
{
	BackendThreadStart *thread_start = backend_thread_current_start();
	PgBackendExitState *exit_state;
	MemoryContext retained_top_context;
	int			exitstatus;
	Size		top_memory_allocated = 0;
	Size		top_memory_accounted = 0;
	Size		top_memory_reclaimed = 0;
#ifdef USE_XTC_CARRIER
	bool		is_fiber;
#endif

	Assert(thread_start != NULL);

#ifdef USE_XTC_CARRIER

	/*
	 * Decide fiber vs dedicated-thread exit from the DURABLE PMChild
	 * classification (carrier_kind, set at launch by PostmasterChildSetPooledLogical
	 * and owned by the postmaster main thread), NOT the per-OS-thread
	 * xtc_in_backend_fiber flag.  That flag is __thread state on the carrier
	 * loop thread, set true at fiber entry and cleared when a fiber leaves; a
	 * fiber that parks in xtc_pg_wait_fd can be resumed on the same carrier
	 * thread AFTER a sibling fiber cleared the flag on its own exit, so the
	 * flag can read false here even though this exit belongs to a fiber.  Under
	 * immediate stop that misread routed a fiber bgworker (e.g. the logical-
	 * replication ApplyLauncher) into PostmasterChildPublishThreadExit, which
	 * asserts PostmasterChildIsThread and traps.  The carrier_kind never lies.
	 */
	is_fiber = PostmasterChildIsPooledLogical(thread_start->publication.pmchild);
#endif

	exit_state = PgCurrentBackendExitStateRef();
	retained_top_context = exit_state->retained_top_memory_context;
	top_memory_accounted = PgBackendConsumeRetainedTopMemoryAllocated();
	exitstatus = backend_thread_exitstatus(code);
	MyClientSocket = NULL;
	if (thread_start->client_sock.sock != PGINVALID_SOCKET)
	{
		closesocket(thread_start->client_sock.sock);
		thread_start->client_sock.sock = PGINVALID_SOCKET;
	}

#ifdef USE_XTC_CARRIER
	/*
	 * Claim the exclusive right to publish this fiber's PMChild exit.  The
	 * entry race with the launcher-cancel orphan reap
	 * (ReapOrphanedThreadedWorker) is already resolved earlier via start_claimed
	 * (backend_thread_run_worker): a fiber that reaches here WON that race and
	 * owns the slot, so this exit_claimed exchange always succeeds on the normal
	 * path.  It remains as a hard exactly-once guard.  A lost claim means the
	 * postmaster owns the reap: skip every PMChild access and do NOT free
	 * thread_start (the postmaster may still be mid-reap reading it); just leave
	 * the fiber.
	 */
	if (is_fiber &&
		pg_atomic_exchange_u32(&thread_start->exit_claimed, 1) != 0)
	{
		ShutdownWaitEventSupport();
		backend_thread_set_current_start(NULL);
		xtc_pg_backend_fiber_exit(exitstatus);
		pg_unreachable();
	}
#endif

	/*
	 * Stop publishing the logical backend before the final exit handoff.  This
	 * keeps later signal routing from observing a backend pointer after the
	 * carrier has committed to teardown.  Retained TopMemoryContext accounting
	 * is kept as a postmaster-side regression probe; normal thread teardown
	 * must delete the saved root before publishing PMChild exit.
	 */
	PostmasterChildUnpublishLogicalBackend(thread_start->publication.pmchild);
	if (thread_start->runtime_state.carrier.protocol_scheduler_registered)
		(void) PgRuntimeProtocolSchedulerUnregisterCarrier(thread_start->runtime_state.carrier.runtime,
														   &thread_start->runtime_state.carrier);
	if (retained_top_context != NULL)
	{
		/*
		 * PgBackendExitCleanup() has run the closed connection/session/backend
		 * and execution reset paths, including clearing the live execution
		 * memory-context slots.  At this point the exiting carrier owns the
		 * saved root context exclusively and can release it before publishing
		 * PMChild exit.  If this is wrong, teardown stress should expose a
		 * remaining cross-backend owner as a crash or corruption signature.
		 */
		top_memory_reclaimed = MemoryContextMemAllocated(retained_top_context,
														 true);
		MemoryContextDelete(retained_top_context);
		backend_thread_free_deleted_retained_memory_contexts();
		backend_thread_clear_deleted_retained_memory_contexts();
		if (top_memory_accounted < top_memory_reclaimed)
			top_memory_accounted = top_memory_reclaimed;
		backend_thread_maybe_trim_reclaimed_memory(top_memory_accounted);
		exit_state->retained_top_memory_context = NULL;
		top_memory_allocated = 0;
	}
#ifdef USE_XTC_CARRIER
	if (is_fiber)
		PostmasterChildPublishPooledLogicalExit(thread_start->publication.pmchild,
												exitstatus,
												top_memory_allocated,
												top_memory_reclaimed,
												thread_start->publication.postmaster_latch);
	else
#endif
	PostmasterChildPublishThreadExit(thread_start->publication.pmchild, exitstatus,
									 top_memory_allocated,
									 top_memory_reclaimed,
									 thread_start->publication.postmaster_latch);

	ShutdownWaitEventSupport();
	backend_thread_set_current_start(NULL);
	backend_thread_start_release(thread_start);
#ifdef USE_XTC_CARRIER
	if (is_fiber)
		xtc_pg_backend_fiber_exit(exitstatus);
#endif
	pg_thread_exit();
}

static void
backend_pooled_logical_finish(int code)
{
	BackendPooledLogicalStart *logical_start;
	PgBackendExitState *exit_state;
	MemoryContext retained_top_context;
	int			exitstatus;
	Size		top_memory_allocated = 0;
	Size		top_memory_accounted = 0;
	Size		top_memory_reclaimed = 0;

	logical_start =
		(BackendPooledLogicalStart *) backend_thread_current_publication();
	Assert(logical_start != NULL);
	Assert(logical_start->publication.kind ==
		   BACKEND_THREAD_START_POOLED_LOGICAL);

	exit_state = PgCurrentBackendExitStateRef();
	retained_top_context = exit_state->retained_top_memory_context;
	top_memory_accounted = PgBackendConsumeRetainedTopMemoryAllocated();
	exitstatus = backend_thread_exitstatus(code);
	MyClientSocket = NULL;
	if (logical_start->client_sock.sock != PGINVALID_SOCKET)
	{
		closesocket(logical_start->client_sock.sock);
		logical_start->client_sock.sock = PGINVALID_SOCKET;
	}

	/*
	 * Pooled logical exit retires the session without retiring the carrier.
	 * The postmaster still owns PMChild slot release, while this carrier owns
	 * reclaiming the retained logical TopMemoryContext before jumping back to
	 * the scheduler loop.
	 */
	PostmasterChildUnpublishLogicalBackend(logical_start->publication.pmchild);
	if (retained_top_context != NULL)
	{
		top_memory_reclaimed = MemoryContextMemAllocated(retained_top_context,
														 true);
		MemoryContextDelete(retained_top_context);
		backend_thread_free_deleted_retained_memory_contexts();
		backend_thread_clear_deleted_retained_memory_contexts();
		if (top_memory_accounted < top_memory_reclaimed)
			top_memory_accounted = top_memory_reclaimed;
		backend_thread_maybe_trim_reclaimed_memory(top_memory_accounted);
		exit_state->retained_top_memory_context = NULL;
		top_memory_allocated = 0;
	}
	PostmasterChildPublishPooledLogicalExit(logical_start->publication.pmchild,
											exitstatus,
											top_memory_allocated,
											top_memory_reclaimed,
											logical_start->publication.postmaster_latch);

	if (logical_start->exit_jmp_valid)
		siglongjmp(logical_start->exit_jmp, 1);

	pg_thread_exit();
}

static int
backend_thread_exitstatus(int code)
{
	if (code == 0)
		return 0;

#ifdef WIN32
	return code;
#else
	return code << 8;
#endif
}

/*
 * Start a new postmaster child process.
 *
 * The child process will be restored to roughly the same state whether
 * EXEC_BACKEND is used or not: it will be attached to shared memory if
 * appropriate, and fds and other resources that we've inherited from
 * postmaster that are not needed in a child process have been closed.
 *
 * 'child_slot' is the PMChildFlags array index reserved for the child
 * process.  'startup_data' is an optional contiguous chunk of data that is
 * passed to the child process.
 */
pid_t
postmaster_child_launch(BackendType child_type, int child_slot,
						void *startup_data, size_t startup_data_len,
						const ClientSocket *client_sock)
{
	pid_t		pid;

	Assert(IsPostmasterEnvironment && !IsUnderPostmaster);

	/* Capture time Postmaster initiates process creation for logging */
	if (IsExternalConnectionBackend(child_type))
		((BackendStartupData *) startup_data)->fork_started = GetCurrentTimestamp();

#ifdef EXEC_BACKEND
	pid = internal_forkexec(child_type, child_slot,
							startup_data, startup_data_len, client_sock);
	/* the child process will arrive in SubPostmasterMain */
#else							/* !EXEC_BACKEND */
	pid = fork_process();
	if (pid == 0)				/* child */
	{
		MyBackendType = child_type;

		/* Capture and transfer timings that may be needed for logging */
		if (IsExternalConnectionBackend(child_type))
		{
			conn_timing.socket_create =
				((BackendStartupData *) startup_data)->socket_created;
			conn_timing.fork_start =
				((BackendStartupData *) startup_data)->fork_started;
			conn_timing.fork_end = GetCurrentTimestamp();
		}

		/* Close the postmaster's sockets */
		ClosePostmasterPorts(child_type == B_LOGGER);

		/* Detangle from postmaster */
		InitPostmasterChild();

		/* Detach shared memory if not needed. */
		if (!child_process_kinds[child_type].shmem_attach)
		{
			dsm_detach_all();
			PGSharedMemoryDetach();
		}

		/*
		 * Enter the Main function with TopMemoryContext.  The startup data is
		 * allocated in PostmasterContext, so we cannot release it here yet.
		 * The Main function will do it after it's done handling the startup
		 * data.
		 */
		MemoryContextSwitchTo(TopMemoryContext);

		MyPMChildSlot = child_slot;
		if (client_sock)
		{
			MyClientSocket = palloc_object(ClientSocket);
			memcpy(MyClientSocket, client_sock, sizeof(ClientSocket));
		}

		/*
		 * Run the appropriate Main function
		 */
		child_process_kinds[child_type].main_fn(startup_data, startup_data_len);
		pg_unreachable();		/* main_fn never returns */
	}
#endif							/* EXEC_BACKEND */
	return pid;
}

#ifdef FORKEXEC_BACKEND
#ifndef WIN32

/*
 * internal_forkexec non-win32 implementation
 *
 * - writes out backend variables to the parameter file
 * - fork():s, and then exec():s the child process
 */
static pid_t
internal_forkexec(BackendType child_kind, int child_slot,
				  const void *startup_data, size_t startup_data_len, const ClientSocket *client_sock)
{
	static unsigned long tmpBackendFileNum = 0;
	pid_t		pid;
	char		tmpfilename[MAXPGPATH];
	size_t		paramsz;
	BackendParameters *param;
	FILE	   *fp;
	char	   *argv[4];
	char		forkav[MAXPGPATH];

	/*
	 * Use palloc0 to make sure padding bytes are initialized, to prevent
	 * Valgrind from complaining about writing uninitialized bytes to the
	 * file.  This isn't performance critical, and the win32 implementation
	 * initializes the padding bytes to zeros, so do it even when not using
	 * Valgrind.
	 */
	paramsz = SizeOfBackendParameters(startup_data_len);
	param = palloc0(paramsz);
	if (!save_backend_variables(param, child_slot, client_sock, startup_data, startup_data_len))
	{
		pfree(param);
		return -1;				/* log made by save_backend_variables */
	}

	/* Calculate name for temp file */
	snprintf(tmpfilename, MAXPGPATH, "%s/%s.backend_var.%d.%lu",
			 PG_TEMP_FILES_DIR, PG_TEMP_FILE_PREFIX,
			 MyProcPid, ++tmpBackendFileNum);

	/* Open file */
	fp = AllocateFile(tmpfilename, PG_BINARY_W);
	if (!fp)
	{
		/*
		 * As in OpenTemporaryFileInTablespace, try to make the temp-file
		 * directory, ignoring errors.
		 */
		(void) MakePGDirectory(PG_TEMP_FILES_DIR);

		fp = AllocateFile(tmpfilename, PG_BINARY_W);
		if (!fp)
		{
			ereport(LOG,
					(errcode_for_file_access(),
					 errmsg("could not create file \"%s\": %m",
							tmpfilename)));
			pfree(param);
			return -1;
		}
	}

	if (fwrite(param, paramsz, 1, fp) != 1)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not write to file \"%s\": %m", tmpfilename)));
		FreeFile(fp);
		pfree(param);
		return -1;
	}
	pfree(param);

	/* Release file */
	if (FreeFile(fp))
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not write to file \"%s\": %m", tmpfilename)));
		return -1;
	}

	/* set up argv properly */
	argv[0] = "postgres";
	snprintf(forkav, MAXPGPATH, "--forkchild=%d", (int) child_kind);
	argv[1] = forkav;
	/* Insert temp file name after --forkchild argument */
	argv[2] = tmpfilename;
	argv[3] = NULL;

	/* Fire off execv in child */
	if ((pid = fork_process()) == 0)
	{
		if (execv(postgres_exec_path, argv) < 0)
		{
			ereport(LOG,
					(errmsg("could not execute server process \"%s\": %m",
							postgres_exec_path)));
			/* We're already in the child process here, can't return */
			exit(1);
		}
	}

	return pid;					/* Parent returns pid, or -1 on fork failure */
}
#else							/* WIN32 */

/*
 * internal_forkexec win32 implementation
 *
 * - starts backend using CreateProcess(), in suspended state
 * - writes out backend variables to the parameter file
 *	- during this, duplicates handles and sockets required for
 *	  inheritance into the new process
 * - resumes execution of the new process once the backend parameter
 *	 file is complete.
 */
static pid_t
internal_forkexec(BackendType child_kind, int child_slot,
				  const void *startup_data, size_t startup_data_len, const ClientSocket *client_sock)
{
	int			retry_count = 0;
	STARTUPINFO si;
	PROCESS_INFORMATION pi;
	char		cmdLine[MAXPGPATH * 2];
	HANDLE		paramHandle;
	BackendParameters *param;
	SECURITY_ATTRIBUTES sa;
	size_t		paramsz;
	char		paramHandleStr[32];
	int			l;

	paramsz = SizeOfBackendParameters(startup_data_len);

	/* Resume here if we need to retry */
retry:

	/* Set up shared memory for parameter passing */
	ZeroMemory(&sa, sizeof(sa));
	sa.nLength = sizeof(sa);
	sa.bInheritHandle = TRUE;
	paramHandle = CreateFileMapping(INVALID_HANDLE_VALUE,
									&sa,
									PAGE_READWRITE,
									0,
									paramsz,
									NULL);
	if (paramHandle == INVALID_HANDLE_VALUE)
	{
		ereport(LOG,
				(errmsg("could not create backend parameter file mapping: error code %lu",
						GetLastError())));
		return -1;
	}
	param = MapViewOfFile(paramHandle, FILE_MAP_WRITE, 0, 0, paramsz);
	if (!param)
	{
		ereport(LOG,
				(errmsg("could not map backend parameter memory: error code %lu",
						GetLastError())));
		CloseHandle(paramHandle);
		return -1;
	}

	/* Format the cmd line */
#ifdef _WIN64
	sprintf(paramHandleStr, "%llu", (LONG_PTR) paramHandle);
#else
	sprintf(paramHandleStr, "%lu", (DWORD) paramHandle);
#endif
	l = snprintf(cmdLine, sizeof(cmdLine) - 1, "\"%s\" --forkchild=%d %s",
				 postgres_exec_path, (int) child_kind, paramHandleStr);
	if (l >= sizeof(cmdLine))
	{
		ereport(LOG,
				(errmsg("subprocess command line too long")));
		UnmapViewOfFile(param);
		CloseHandle(paramHandle);
		return -1;
	}

	memset(&pi, 0, sizeof(pi));
	memset(&si, 0, sizeof(si));
	si.cb = sizeof(si);

	/*
	 * Create the subprocess in a suspended state. This will be resumed later,
	 * once we have written out the parameter file.
	 */
	if (!CreateProcess(NULL, cmdLine, NULL, NULL, TRUE, CREATE_SUSPENDED,
					   NULL, NULL, &si, &pi))
	{
		ereport(LOG,
				(errmsg("CreateProcess() call failed: %m (error code %lu)",
						GetLastError())));
		UnmapViewOfFile(param);
		CloseHandle(paramHandle);
		return -1;
	}

	if (!save_backend_variables(param, child_slot, client_sock,
								pi.hProcess, pi.dwProcessId,
								startup_data, startup_data_len))
	{
		/*
		 * log made by save_backend_variables, but we have to clean up the
		 * mess with the half-started process
		 */
		if (!TerminateProcess(pi.hProcess, 255))
			ereport(LOG,
					(errmsg_internal("could not terminate unstarted process: error code %lu",
									 GetLastError())));
		CloseHandle(pi.hProcess);
		CloseHandle(pi.hThread);
		UnmapViewOfFile(param);
		CloseHandle(paramHandle);
		return -1;				/* log made by save_backend_variables */
	}

	/* Drop the parameter shared memory that is now inherited to the backend */
	if (!UnmapViewOfFile(param))
		ereport(LOG,
				(errmsg("could not unmap view of backend parameter file: error code %lu",
						GetLastError())));
	if (!CloseHandle(paramHandle))
		ereport(LOG,
				(errmsg("could not close handle to backend parameter file: error code %lu",
						GetLastError())));

	/*
	 * Reserve the memory region used by our main shared memory segment before
	 * we resume the child process.  Normally this should succeed, but if ASLR
	 * is active then it might sometimes fail due to the stack or heap having
	 * gotten mapped into that range.  In that case, just terminate the
	 * process and retry.
	 */
	if (!pgwin32_ReserveSharedMemoryRegion(pi.hProcess))
	{
		/* pgwin32_ReserveSharedMemoryRegion already made a log entry */
		if (!TerminateProcess(pi.hProcess, 255))
			ereport(LOG,
					(errmsg_internal("could not terminate process that failed to reserve memory: error code %lu",
									 GetLastError())));
		CloseHandle(pi.hProcess);
		CloseHandle(pi.hThread);
		if (++retry_count < 100)
			goto retry;
		ereport(LOG,
				(errmsg("giving up after too many tries to reserve shared memory"),
				 errhint("This might be caused by ASLR or antivirus software.")));
		return -1;
	}

	/*
	 * Now that the backend variables are written out, we start the child
	 * thread so it can start initializing while we set up the rest of the
	 * parent state.
	 */
	if (ResumeThread(pi.hThread) == -1)
	{
		if (!TerminateProcess(pi.hProcess, 255))
		{
			ereport(LOG,
					(errmsg_internal("could not terminate unstartable process: error code %lu",
									 GetLastError())));
			CloseHandle(pi.hProcess);
			CloseHandle(pi.hThread);
			return -1;
		}
		CloseHandle(pi.hProcess);
		CloseHandle(pi.hThread);
		ereport(LOG,
				(errmsg_internal("could not resume thread of unstarted process: error code %lu",
								 GetLastError())));
		return -1;
	}

	/* Set up notification when the child process dies */
	pgwin32_register_deadchild_callback(pi.hProcess, pi.dwProcessId);

	/* Don't close pi.hProcess, it's owned by the deadchild callback now */

	CloseHandle(pi.hThread);

	return pi.dwProcessId;
}
#endif							/* WIN32 */

/*
 * SubPostmasterMain -- Get the fork/exec'd process into a state equivalent
 *			to what it would be if we'd simply forked on Unix, and then
 *			dispatch to the appropriate place.
 *
 * The first two command line arguments are expected to be "--forkchild=<kind>",
 * where <kind> indicates which process type we are to become, and
 * the name of a variables file that we can read to load data that would
 * have been inherited by fork() on Unix.
 */
void
SubPostmasterMain(int argc, char *argv[])
{
	void	   *startup_data;
	size_t		startup_data_len;
	char	   *child_kind;
	BackendType child_type;
	TimestampTz fork_end;

	/* In EXEC_BACKEND case we will not have inherited these settings */
	IsPostmasterEnvironment = true;
	whereToSendOutput = DestNone;

	/*
	 * This backend was started via fork()+exec(): it did not inherit the
	 * postmaster's address space and must re-attach shared memory / re-derive
	 * backend-local state.  Under a plain EXEC_BACKEND build every child is
	 * exec'd so this is redundant with PG_BACKEND_WAS_FORKEXECED being a
	 * constant true, but under USE_XTC_PROCESS_FALLBACK this flag is what
	 * distinguishes the exec'd process-fallback backend from normally-forked
	 * children.
	 */
	pg_backend_was_forkexeced = true;

	/*
	 * Capture the end of process creation for logging. We don't include the
	 * time spent copying data from shared memory and setting up the backend.
	 */
	fork_end = GetCurrentTimestamp();

	/* Setup essential subsystems (to ensure elog() behaves sanely) */
	InitializeGUCOptions();

	/* Check we got appropriate args */
	if (argc != 3)
		elog(FATAL, "invalid subpostmaster invocation");

	/*
	 * Parse the --forkchild argument to find our process type.  We rely with
	 * malice aforethought on atoi returning 0 (B_INVALID) on error.
	 */
	if (strncmp(argv[1], "--forkchild=", 12) != 0)
		elog(FATAL, "invalid subpostmaster invocation (--forkchild argument missing)");
	child_kind = argv[1] + 12;
	child_type = (BackendType) atoi(child_kind);
	if (child_type <= B_INVALID || child_type > BACKEND_NUM_TYPES - 1)
		elog(ERROR, "unknown child kind %s", child_kind);
	MyBackendType = child_type;

	/* Read in the variables file */
	read_backend_variables(argv[2], &startup_data, &startup_data_len);

	/* Close the postmaster's sockets (as soon as we know them) */
	ClosePostmasterPorts(child_type == B_LOGGER);

	/* Setup as postmaster child */
	InitPostmasterChild();

	/*
	 * If appropriate, physically re-attach to shared memory segment. We want
	 * to do this before going any further to ensure that we can attach at the
	 * same address the postmaster used.  On the other hand, if we choose not
	 * to re-attach, we may have other cleanup to do.
	 *
	 * If testing EXEC_BACKEND on Linux, you should run this as root before
	 * starting the postmaster:
	 *
	 * sysctl -w kernel.randomize_va_space=0
	 *
	 * This prevents using randomized stack and code addresses that cause the
	 * child process's memory map to be different from the parent's, making it
	 * sometimes impossible to attach to shared memory at the desired address.
	 * Return the setting to its old value (usually '1' or '2') when finished.
	 */
	if (child_process_kinds[child_type].shmem_attach)
		PGSharedMemoryReAttach();
	else
		PGSharedMemoryNoReAttach();

	/* Read in remaining GUC variables */
	read_nondefault_variables();

	/*
	 * A fork+exec'd child does not run SelectConfigFiles(), which is where the
	 * postmaster derives the absolute hba_file / ident_file paths (defaulting
	 * them to <configdir>/pg_hba.conf and pg_ident.conf) and installs them as
	 * PGC_S_OVERRIDE.  Those derived overrides are not reliably carried across
	 * the GUC-serialization boundary, so an exec'd child can arrive with
	 * hba_file/ident_file NULL and then fail in load_hba()/load_ident() trying
	 * to open "(null)".  Re-derive them here from the config directory the same
	 * way SelectConfigFiles() does.  (Under EXEC_BACKEND proper this is
	 * redundant but harmless: the values are already set, so the NULL checks
	 * below skip.)
	 */
	if (HbaFileName == NULL || IdentFileName == NULL)
	{
		char	   *configdir;

		configdir = make_absolute_path(ConfigFileName);
		if (configdir != NULL)
		{
			char	   *sep = last_dir_separator(configdir);

			if (sep != NULL)
				*sep = '\0';	/* strip the file name, keep the directory */
		}
		if (configdir == NULL || configdir[0] == '\0')
			configdir = DataDir;

		if (configdir != NULL)
		{
			if (HbaFileName == NULL)
			{
				char	   *fname = psprintf("%s/%s", configdir, "pg_hba.conf");

				SetConfigOption("hba_file", fname, PGC_POSTMASTER, PGC_S_OVERRIDE);
				pfree(fname);
			}
			if (IdentFileName == NULL)
			{
				char	   *fname = psprintf("%s/%s", configdir, "pg_ident.conf");

				SetConfigOption("ident_file", fname, PGC_POSTMASTER, PGC_S_OVERRIDE);
				pfree(fname);
			}
		}
	}

	/* Capture and transfer timings that may be needed for log_connections */
	if (IsExternalConnectionBackend(child_type))
	{
		conn_timing.socket_create =
			((BackendStartupData *) startup_data)->socket_created;
		conn_timing.fork_start =
			((BackendStartupData *) startup_data)->fork_started;
		conn_timing.fork_end = fork_end;
	}

	/*
	 * Check that the data directory looks valid, which will also check the
	 * privileges on the data directory and update our umask and file/group
	 * variables for creating files later.  Note: this should really be done
	 * before we create any files or directories.
	 */
	checkDataDir();

	/*
	 * (re-)read control file, as it contains config. The postmaster will
	 * already have read this, but this process doesn't know about that.
	 */
	LocalProcessControlFile(false);

	RegisterBuiltinShmemCallbacks();

	/*
	 * Reload any libraries that were preloaded by the postmaster.  Since we
	 * exec'd this process, those libraries didn't come along with us; but we
	 * should load them into all child processes to be consistent with the
	 * non-EXEC_BACKEND behavior.
	 */
	process_shared_preload_libraries();

	/* Restore basic shared memory pointers */
	if (UsedShmemSegAddr != NULL)
	{
		InitShmemAllocator(UsedShmemSegAddr);

		/*
		 * Re-apply the multithreaded io_method remap in the exec'd child.
		 *
		 * The postmaster routes io_method=worker to the in-fiber "xtc" method
		 * under multithreaded=on (see PostmasterMain), and sizes/attaches shared
		 * memory as xtc (which has no worker submission queue).  That remap is a
		 * PGC_S_OVERRIDE runtime setting that does not survive serialization into
		 * a fork+exec'd child: the child's read_nondefault_variables() restores
		 * io_method=worker, so pgaio_method_ops would be the worker method and
		 * ShmemCallRequestCallbacks() would try to attach the AioWorkerSubmission
		 * Queue the parent never created.  Re-assert the remap here so the child's
		 * AIO method matches the parent's before any shmem attach.
		 */
		if (multithreaded && io_method == IOMETHOD_WORKER)
			SetConfigOption("io_method", "xtc", PGC_POSTMASTER, PGC_S_OVERRIDE);

		ShmemCallRequestCallbacks();
	}

	/*
	 * Run the appropriate Main function
	 */
	child_process_kinds[child_type].main_fn(startup_data, startup_data_len);
	pg_unreachable();			/* main_fn never returns */
}

#ifndef WIN32
#define write_inheritable_socket(dest, src, childpid) ((*(dest) = (src)), true)
#define read_inheritable_socket(dest, src) (*(dest) = *(src))
#else
static bool write_duplicated_handle(HANDLE *dest, HANDLE src, HANDLE child);
static bool write_inheritable_socket(InheritableSocket *dest, SOCKET src,
									 pid_t childPid);
static void read_inheritable_socket(SOCKET *dest, InheritableSocket *src);
#endif


/* Save critical backend variables into the BackendParameters struct */
static bool
save_backend_variables(BackendParameters *param,
					   int child_slot, const ClientSocket *client_sock,
#ifdef WIN32
					   HANDLE childProcess, pid_t childPid,
#endif
					   const void *startup_data, size_t startup_data_len)
{
	if (client_sock)
		memcpy(&param->client_sock, client_sock, sizeof(ClientSocket));
	else
		memset(&param->client_sock, 0, sizeof(ClientSocket));
	if (!write_inheritable_socket(&param->inh_sock,
								  client_sock ? client_sock->sock : PGINVALID_SOCKET,
								  childPid))
		return false;

	strlcpy(param->DataDir, DataDir, MAXPGPATH);

	param->saved_my_pmchild_slot = child_slot;

#ifdef WIN32
	param->ShmemProtectiveRegion = ShmemProtectiveRegion;
#endif
	param->UsedShmemSegID = UsedShmemSegID;
	param->UsedShmemSegAddr = UsedShmemSegAddr;

#ifdef USE_INJECTION_POINTS
	param->ActiveInjectionPoints = ActiveInjectionPoints;
#endif

	param->ProcGlobal = ProcGlobal;
	param->AuxiliaryProcs = AuxiliaryProcs;
	param->PreparedXactProcs = PreparedXactProcs;
	param->PMSignalState = PMSignalState;
	param->ProcSignal = ProcSignal;

	param->PostmasterPid = PostmasterPid;
	param->PgStartTime = PgStartTime;
	param->PgReloadTime = PgReloadTime;
	param->first_syslogger_file_time = first_syslogger_file_time;

	param->redirection_done = redirection_done;
	param->IsBinaryUpgrade = IsBinaryUpgrade;
	param->saved_query_id_enabled = query_id_enabled;
	param->max_safe_fds = max_safe_fds;

	param->MaxBackends = MaxBackends;
	param->num_pmchild_slots = num_pmchild_slots;

	param->timing_tsc_frequency_khz = timing_tsc_frequency_khz;

#ifdef WIN32
	param->PostmasterHandle = PostmasterHandle;
	if (!write_duplicated_handle(&param->initial_signal_pipe,
								 pgwin32_create_signal_listener(childPid),
								 childProcess))
		return false;
#else
	memcpy(&param->postmaster_alive_fds, &postmaster_alive_fds,
		   sizeof(postmaster_alive_fds));
#endif

	memcpy(&param->syslogPipe, &syslogPipe, sizeof(syslogPipe));

	strlcpy(param->my_exec_path, my_exec_path, MAXPGPATH);

	strlcpy(param->pkglib_path, pkglib_path, MAXPGPATH);

	param->startup_data_len = startup_data_len;
	if (startup_data_len > 0)
		memcpy(param->startup_data, startup_data, startup_data_len);

	return true;
}

#ifdef WIN32
/*
 * Duplicate a handle for usage in a child process, and write the child
 * process instance of the handle to the parameter file.
 */
static bool
write_duplicated_handle(HANDLE *dest, HANDLE src, HANDLE childProcess)
{
	HANDLE		hChild = INVALID_HANDLE_VALUE;

	if (!DuplicateHandle(GetCurrentProcess(),
						 src,
						 childProcess,
						 &hChild,
						 0,
						 TRUE,
						 DUPLICATE_CLOSE_SOURCE | DUPLICATE_SAME_ACCESS))
	{
		ereport(LOG,
				(errmsg_internal("could not duplicate handle to be written to backend parameter file: error code %lu",
								 GetLastError())));
		return false;
	}

	*dest = hChild;
	return true;
}

/*
 * Duplicate a socket for usage in a child process, and write the resulting
 * structure to the parameter file.
 * This is required because a number of LSPs (Layered Service Providers) very
 * common on Windows (antivirus, firewalls, download managers etc) break
 * straight socket inheritance.
 */
static bool
write_inheritable_socket(InheritableSocket *dest, SOCKET src, pid_t childpid)
{
	dest->origsocket = src;
	if (src != 0 && src != PGINVALID_SOCKET)
	{
		/* Actual socket */
		if (WSADuplicateSocket(src, childpid, &dest->wsainfo) != 0)
		{
			ereport(LOG,
					(errmsg("could not duplicate socket %d for use in backend: error code %d",
							(int) src, WSAGetLastError())));
			return false;
		}
	}
	return true;
}

/*
 * Read a duplicate socket structure back, and get the socket descriptor.
 */
static void
read_inheritable_socket(SOCKET *dest, InheritableSocket *src)
{
	SOCKET		s;

	if (src->origsocket == PGINVALID_SOCKET || src->origsocket == 0)
	{
		/* Not a real socket! */
		*dest = src->origsocket;
	}
	else
	{
		/* Actual socket, so create from structure */
		s = WSASocket(FROM_PROTOCOL_INFO,
					  FROM_PROTOCOL_INFO,
					  FROM_PROTOCOL_INFO,
					  &src->wsainfo,
					  0,
					  0);
		if (s == INVALID_SOCKET)
		{
			write_stderr("could not create inherited socket: error code %d\n",
						 WSAGetLastError());
			exit(1);
		}
		*dest = s;

		/*
		 * To make sure we don't get two references to the same socket, close
		 * the original one. (This would happen when inheritance actually
		 * works..
		 */
		closesocket(src->origsocket);
	}
}
#endif

static void
read_backend_variables(char *id, void **startup_data, size_t *startup_data_len)
{
	BackendParameters param;

#ifndef WIN32
	/* Non-win32 implementation reads from file */
	FILE	   *fp;

	/* Open file */
	fp = AllocateFile(id, PG_BINARY_R);
	if (!fp)
	{
		write_stderr("could not open backend variables file \"%s\": %m\n", id);
		exit(1);
	}

	if (fread(&param, sizeof(param), 1, fp) != 1)
	{
		write_stderr("could not read from backend variables file \"%s\": %m\n", id);
		exit(1);
	}

	/* read startup data */
	*startup_data_len = param.startup_data_len;
	if (param.startup_data_len > 0)
	{
		*startup_data = palloc(*startup_data_len);
		if (fread(*startup_data, *startup_data_len, 1, fp) != 1)
		{
			write_stderr("could not read startup data from backend variables file \"%s\": %m\n",
						 id);
			exit(1);
		}
	}
	else
		*startup_data = NULL;

	/* Release file */
	FreeFile(fp);
	if (unlink(id) != 0)
	{
		write_stderr("could not remove file \"%s\": %m\n", id);
		exit(1);
	}
#else
	/* Win32 version uses mapped file */
	HANDLE		paramHandle;
	BackendParameters *paramp;

#ifdef _WIN64
	paramHandle = (HANDLE) _atoi64(id);
#else
	paramHandle = (HANDLE) atol(id);
#endif
	paramp = MapViewOfFile(paramHandle, FILE_MAP_READ, 0, 0, 0);
	if (!paramp)
	{
		write_stderr("could not map view of backend variables: error code %lu\n",
					 GetLastError());
		exit(1);
	}

	memcpy(&param, paramp, sizeof(BackendParameters));

	/* read startup data */
	*startup_data_len = param.startup_data_len;
	if (param.startup_data_len > 0)
	{
		*startup_data = palloc(paramp->startup_data_len);
		memcpy(*startup_data, paramp->startup_data, param.startup_data_len);
	}
	else
		*startup_data = NULL;

	if (!UnmapViewOfFile(paramp))
	{
		write_stderr("could not unmap view of backend variables: error code %lu\n",
					 GetLastError());
		exit(1);
	}

	if (!CloseHandle(paramHandle))
	{
		write_stderr("could not close handle to backend parameter variables: error code %lu\n",
					 GetLastError());
		exit(1);
	}
#endif

	restore_backend_variables(&param);
}

/* Restore critical backend variables from the BackendParameters struct */
static void
restore_backend_variables(BackendParameters *param)
{
	if (param->client_sock.sock != PGINVALID_SOCKET)
	{
		MyClientSocket = MemoryContextAlloc(TopMemoryContext, sizeof(ClientSocket));
		memcpy(MyClientSocket, &param->client_sock, sizeof(ClientSocket));
		read_inheritable_socket(&MyClientSocket->sock, &param->inh_sock);
	}

	SetDataDir(param->DataDir);

	MyPMChildSlot = param->saved_my_pmchild_slot;

#ifdef WIN32
	ShmemProtectiveRegion = param->ShmemProtectiveRegion;
#endif
	UsedShmemSegID = param->UsedShmemSegID;
	UsedShmemSegAddr = param->UsedShmemSegAddr;

#ifdef USE_INJECTION_POINTS
	ActiveInjectionPoints = param->ActiveInjectionPoints;
#endif

	ProcGlobal = param->ProcGlobal;
	AuxiliaryProcs = param->AuxiliaryProcs;
	PreparedXactProcs = param->PreparedXactProcs;
	PMSignalState = param->PMSignalState;
	ProcSignal = param->ProcSignal;

	PostmasterPid = param->PostmasterPid;
	PgStartTime = param->PgStartTime;
	PgReloadTime = param->PgReloadTime;
	first_syslogger_file_time = param->first_syslogger_file_time;

	redirection_done = param->redirection_done;
	IsBinaryUpgrade = param->IsBinaryUpgrade;
	query_id_enabled = param->saved_query_id_enabled;
	max_safe_fds = param->max_safe_fds;

	MaxBackends = param->MaxBackends;
	num_pmchild_slots = param->num_pmchild_slots;

	timing_tsc_frequency_khz = param->timing_tsc_frequency_khz;

	/* Re-run logic usually done by assign_timing_clock_source */
	pg_initialize_timing();
	pg_set_timing_clock_source(timing_clock_source);

#ifdef WIN32
	PostmasterHandle = param->PostmasterHandle;
	pgwin32_initial_signal_pipe = param->initial_signal_pipe;
#else
	memcpy(&postmaster_alive_fds, &param->postmaster_alive_fds,
		   sizeof(postmaster_alive_fds));
#endif

	memcpy(&syslogPipe, &param->syslogPipe, sizeof(syslogPipe));

	strlcpy(my_exec_path, param->my_exec_path, MAXPGPATH);

	strlcpy(pkglib_path, param->pkglib_path, MAXPGPATH);

	/*
	 * We need to restore fd.c's counts of externally-opened FDs; to avoid
	 * confusion, be sure to do this after restoring max_safe_fds.  (Note:
	 * BackendInitialize will handle this for (*client_sock)->sock.)
	 */
#ifndef WIN32
	if (postmaster_alive_fds[0] >= 0)
		ReserveExternalFD();
	if (postmaster_alive_fds[1] >= 0)
		ReserveExternalFD();
#endif
}

#endif							/* FORKEXEC_BACKEND */
