/*-------------------------------------------------------------------------
 *
 * postmaster.c
 *	  This program acts as a clearing house for requests to the
 *	  POSTGRES system.  Frontend programs connect to the Postmaster,
 *	  and postmaster forks a new backend process to handle the
 *	  connection.
 *
 *	  The postmaster also manages system-wide operations such as
 *	  startup and shutdown. The postmaster itself doesn't do those
 *	  operations, mind you --- it just forks off a subprocess to do them
 *	  at the right times.  It also takes care of resetting the system
 *	  if a backend crashes.
 *
 *	  The postmaster process creates the shared memory and semaphore
 *	  pools during startup, but as a rule does not touch them itself.
 *	  In particular, it is not a member of the PGPROC array of backends
 *	  and so it cannot participate in lock-manager operations.  Keeping
 *	  the postmaster away from shared memory operations makes it simpler
 *	  and more reliable.  The postmaster is almost always able to recover
 *	  from crashes of individual backends by resetting shared memory;
 *	  if it did much with shared memory then it would be prone to crashing
 *	  along with the backends.
 *
 *	  When a request message is received, we now fork() immediately.
 *	  The child process performs authentication of the request, and
 *	  then becomes a backend if successful.  This allows the auth code
 *	  to be written in a simple single-threaded style (as opposed to the
 *	  crufty "poor man's multitasking" code that used to be needed).
 *	  More importantly, it ensures that blockages in non-multithreaded
 *	  libraries like SSL or PAM cannot cause denial of service to other
 *	  clients.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/postmaster.c
 *
 * NOTES
 *
 * Initialization:
 *		The Postmaster sets up shared memory data structures
 *		for the backends.
 *
 * Synchronization:
 *		The Postmaster shares memory with the backends but should avoid
 *		touching shared memory, so as not to become stuck if a crashing
 *		backend screws up locks or shared memory.  Likewise, the Postmaster
 *		should never block on messages from frontend clients.
 *
 * Garbage Collection:
 *		The Postmaster cleans up after backends if they have an emergency
 *		exit and/or core dump.
 *
 * Error Reporting:
 *		Use write_stderr() only for reporting "interactive" errors
 *		(essentially, bogus arguments on the command line).  Once the
 *		postmaster is launched, use ereport().
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <unistd.h>
#include <signal.h>
#include <time.h>
#include <sys/wait.h>
#include <ctype.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <sys/param.h>
#include <netdb.h>
#include <limits.h>

#ifdef USE_BONJOUR
#include <dns_sd.h>
#endif

#ifdef USE_SYSTEMD
#include <systemd/sd-daemon.h>
#endif

#ifdef HAVE_PTHREAD_IS_THREADED_NP
#include <pthread.h>
#endif

#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xlogrecovery.h"
#include "common/file_perm.h"
#include "common/pg_prng.h"
#include "lib/ilist.h"
#include "libpq/libpq.h"
#include "libpq/pqsignal.h"
#include "pgstat.h"
#include "port/pg_bswap.h"
#include "port/pg_getopt_ctx.h"
#include "postmaster/autovacuum.h"
#include "postmaster/bgworker_internals.h"
#include "postmaster/pgarch.h"
#include "postmaster/pg_xtc_carrier.h"
#include "postmaster/postmaster.h"
#include "postmaster/syslogger.h"
#include "postmaster/walsummarizer.h"
#include "replication/logicallauncher.h"
#include "replication/slotsync.h"
#include "replication/walsender.h"
#include "storage/aio_subsys.h"
#ifdef USE_XTC_CARRIER
#include "storage/aio.h"				/* io_method / IOMETHOD_WORKER (xtc interim) */
#endif
#include "storage/fd.h"
#include "storage/io_worker.h"
#include "storage/ipc.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "storage/shmem_internal.h"
#include "tcop/backend_startup.h"
#include "tcop/tcopprot.h"
#include "utils/datetime.h"
#include "utils/backend_runtime.h"
#include "utils/memutils.h"
#include "utils/pidfile.h"
#include "utils/timestamp.h"
#include "utils/varlena.h"

#ifdef EXEC_BACKEND
#include "common/file_utils.h"
#include "storage/pg_shmem.h"
#endif


/*
 * CountChildren and SignalChildren take a bitmask argument to represent
 * BackendTypes to count or signal.  Define a separate type and functions to
 * work with the bitmasks, to avoid accidentally passing a plain BackendType
 * in place of a bitmask or vice versa.
 */
typedef struct
{
	uint32		mask;
} BackendTypeMask;

StaticAssertDecl(BACKEND_NUM_TYPES < 32, "too many backend types for uint32");

static const BackendTypeMask BTYPE_MASK_ALL = {(1 << BACKEND_NUM_TYPES) - 1};
static const BackendTypeMask BTYPE_MASK_NONE = {0};

static inline BackendTypeMask
btmask(BackendType t)
{
	BackendTypeMask mask = {.mask = 1 << t};

	return mask;
}

static inline BackendTypeMask
btmask_add_n(BackendTypeMask mask, int nargs, BackendType *t)
{
	for (int i = 0; i < nargs; i++)
		mask.mask |= 1 << t[i];
	return mask;
}

#define btmask_add(mask, ...) \
	btmask_add_n(mask, \
		lengthof(((BackendType[]){__VA_ARGS__})), \
		(BackendType[]){__VA_ARGS__} \
	)

static inline BackendTypeMask
btmask_del(BackendTypeMask mask, BackendType t)
{
	mask.mask &= ~(1 << t);
	return mask;
}

static inline BackendTypeMask
btmask_all_except_n(int nargs, BackendType *t)
{
	BackendTypeMask mask = BTYPE_MASK_ALL;

	for (int i = 0; i < nargs; i++)
		mask = btmask_del(mask, t[i]);
	return mask;
}

#define btmask_all_except(...) \
	btmask_all_except_n( \
		lengthof(((BackendType[]){__VA_ARGS__})), \
		(BackendType[]){__VA_ARGS__} \
	)

static inline bool
btmask_contains(BackendTypeMask mask, BackendType t)
{
	return (mask.mask & (1 << t)) != 0;
}


/* The socket number we are listening for connections on */
PG_GLOBAL_RUNTIME int PostPortNumber = DEF_PGPORT;

/* The directory names for Unix socket(s) */
PG_GLOBAL_RUNTIME char *Unix_socket_directories;

/* The TCP listen address(es) */
PG_GLOBAL_RUNTIME char *ListenAddresses;

/*
 * SuperuserReservedConnections is the number of backends reserved for
 * superuser use, and ReservedConnections is the number of backends reserved
 * for use by roles with privileges of the pg_use_reserved_connections
 * predefined role.  These are taken out of the pool of MaxConnections backend
 * slots, so the number of backend slots available for roles that are neither
 * superuser nor have privileges of pg_use_reserved_connections is
 * (MaxConnections - SuperuserReservedConnections - ReservedConnections).
 *
 * If the number of remaining slots is less than or equal to
 * SuperuserReservedConnections, only superusers can make new connections.  If
 * the number of remaining slots is greater than SuperuserReservedConnections
 * but less than or equal to
 * (SuperuserReservedConnections + ReservedConnections), only superusers and
 * roles with privileges of pg_use_reserved_connections can make new
 * connections.  Note that pre-existing superuser and
 * pg_use_reserved_connections connections don't count against the limits.
 */
PG_GLOBAL_RUNTIME int SuperuserReservedConnections;
PG_GLOBAL_RUNTIME int ReservedConnections;

/* The socket(s) we're listening to. */
#define MAXLISTEN	64
static PG_GLOBAL_RUNTIME int NumListenSockets = 0;
static PG_GLOBAL_RUNTIME pgsocket *ListenSockets = NULL;

/* still more option variables */
PG_GLOBAL_RUNTIME bool EnableSSL = false;

PG_GLOBAL_RUNTIME int PreAuthDelay = 0;
PG_GLOBAL_RUNTIME int AuthenticationTimeout = 60;

PG_GLOBAL_RUNTIME bool log_hostname;	/* for ps display and logging */

PG_GLOBAL_RUNTIME bool enable_bonjour = false;
PG_GLOBAL_RUNTIME char *bonjour_name;
PG_GLOBAL_RUNTIME bool restart_after_crash = true;
PG_GLOBAL_RUNTIME bool remove_temp_files_after_crash = true;

/*
 * When terminating child processes after fatal errors, like a crash of a
 * child process, we normally send SIGQUIT -- and most other comments in this
 * file are written on the assumption that we do -- but developers might
 * prefer to use SIGABRT to collect per-child core dumps.
 */
PG_GLOBAL_RUNTIME bool send_abort_for_crash = false;
PG_GLOBAL_RUNTIME bool send_abort_for_kill = false;

/* special child processes; NULL when not running */
static PG_GLOBAL_RUNTIME PMChild *StartupPMChild = NULL,
		   *BgWriterPMChild = NULL,
		   *CheckpointerPMChild = NULL,
		   *WalWriterPMChild = NULL,
		   *WalReceiverPMChild = NULL,
		   *WalSummarizerPMChild = NULL,
		   *AutoVacLauncherPMChild = NULL,
		   *PgArchPMChild = NULL,
		   *SysLoggerPMChild = NULL,
		   *SlotSyncWorkerPMChild = NULL;

/* Startup process's status */
typedef enum
{
	STARTUP_NOT_RUNNING,
	STARTUP_RUNNING,
	STARTUP_SIGNALED,			/* we sent it a SIGQUIT or SIGKILL */
	STARTUP_CRASHED,
} StartupStatusEnum;

static PG_GLOBAL_RUNTIME StartupStatusEnum StartupStatus = STARTUP_NOT_RUNNING;

/* Startup/shutdown state */
#define			NoShutdown		0
#define			SmartShutdown	1
#define			FastShutdown	2
#define			ImmediateShutdown	3

static PG_GLOBAL_RUNTIME int Shutdown = NoShutdown;

static PG_GLOBAL_RUNTIME bool FatalError = false; /* T if recovering from backend crash */

/*
 * We use a simple state machine to control startup, shutdown, and
 * crash recovery (which is rather like shutdown followed by startup).
 *
 * After doing all the postmaster initialization work, we enter PM_STARTUP
 * state and the startup process is launched. The startup process begins by
 * reading the control file and other preliminary initialization steps.
 * In a normal startup, or after crash recovery, the startup process exits
 * with exit code 0 and we switch to PM_RUN state.  However, archive recovery
 * is handled specially since it takes much longer and we would like to support
 * hot standby during archive recovery.
 *
 * When the startup process is ready to start archive recovery, it signals the
 * postmaster, and we switch to PM_RECOVERY state. The background writer and
 * checkpointer are already running (as these are launched during PM_STARTUP),
 * and the startup process continues applying WAL.  If Hot Standby is enabled,
 * then, after reaching a consistent point in WAL redo, startup process
 * signals us again, and we switch to PM_HOT_STANDBY state and begin accepting
 * connections to perform read-only queries.  When archive recovery is
 * finished, the startup process exits with exit code 0 and we switch to
 * PM_RUN state.
 *
 * Normal child backends can only be launched when we are in PM_RUN or
 * PM_HOT_STANDBY state.  (connsAllowed can also restrict launching.)
 * In other states we handle connection requests by launching "dead-end"
 * child processes, which will simply send the client an error message and
 * quit.  (We track these in the ActiveChildList so that we can know when they
 * are all gone; this is important because they're still connected to shared
 * memory, and would interfere with an attempt to destroy the shmem segment,
 * possibly leading to SHMALL failure when we try to make a new one.)
 * In PM_WAIT_DEAD_END state we are waiting for all the dead-end children
 * to drain out of the system, and therefore stop accepting connection
 * requests at all until the last existing child has quit (which hopefully
 * will not be very long).
 *
 * Notice that this state variable does not distinguish *why* we entered
 * states later than PM_RUN --- Shutdown and FatalError must be consulted
 * to find that out.  FatalError is never true in PM_RECOVERY, PM_HOT_STANDBY,
 * or PM_RUN states, nor in PM_WAIT_XLOG_SHUTDOWN states (because we don't
 * enter those states when trying to recover from a crash).  It can be true in
 * PM_STARTUP state, because we don't clear it until we've successfully
 * started WAL redo.
 */
typedef enum
{
	PM_INIT,					/* postmaster starting */
	PM_STARTUP,					/* waiting for startup subprocess */
	PM_RECOVERY,				/* in archive recovery mode */
	PM_HOT_STANDBY,				/* in hot standby mode */
	PM_RUN,						/* normal "database is alive" state */
	PM_STOP_BACKENDS,			/* need to stop remaining backends */
	PM_WAIT_BACKENDS,			/* waiting for live backends to exit */
	PM_WAIT_XLOG_SHUTDOWN,		/* waiting for checkpointer to do shutdown
								 * ckpt */
	PM_WAIT_XLOG_ARCHIVAL,		/* waiting for archiver and walsenders to
								 * finish */
	PM_WAIT_IO_WORKERS,			/* waiting for io workers to exit */
	PM_WAIT_CHECKPOINTER,		/* waiting for checkpointer to shut down */
	PM_WAIT_DEAD_END,			/* waiting for dead-end children to exit */
	PM_NO_CHILDREN,				/* all important children have exited */
} PMState;

static PG_GLOBAL_RUNTIME PMState pmState = PM_INIT;

/*
 * While performing a "smart shutdown", we restrict new connections but stay
 * in PM_RUN or PM_HOT_STANDBY state until all the client backends are gone.
 * connsAllowed is a sub-state indicator showing the active restriction.
 * It is of no interest unless pmState is PM_RUN or PM_HOT_STANDBY.
 */
static PG_GLOBAL_RUNTIME bool connsAllowed = true;

/* Start time of SIGKILL timeout during immediate shutdown or child crash */
/* Zero means timeout is not running */
static PG_GLOBAL_RUNTIME time_t AbortStartTime = 0;

/* Length of said timeout */
#define SIGKILL_CHILDREN_AFTER_SECS		5

static PG_GLOBAL_RUNTIME bool ReachedNormalRunning = false;	/* T if we've reached PM_RUN */

PG_GLOBAL_RUNTIME bool redirection_done = false;	/* stderr redirected for syslogger? */

/* received START_AUTOVAC_LAUNCHER signal */
static PG_GLOBAL_RUNTIME bool start_autovac_launcher = false;

/* the launcher needs to be signaled to communicate some condition */
static PG_GLOBAL_RUNTIME bool avlauncher_needs_signal = false;

/* received START_WALRECEIVER signal */
static PG_GLOBAL_RUNTIME bool WalReceiverRequested = false;

/* set when there's a worker that needs to be started up */
static PG_GLOBAL_RUNTIME bool StartWorkerNeeded = true;
static PG_GLOBAL_RUNTIME bool HaveCrashedWorker = false;

/* set when signals arrive */
static PG_GLOBAL_RUNTIME volatile sig_atomic_t pending_pm_pmsignal;
static PG_GLOBAL_RUNTIME volatile sig_atomic_t pending_pm_child_exit;
static PG_GLOBAL_RUNTIME volatile sig_atomic_t pending_pm_reload_request;
static PG_GLOBAL_RUNTIME volatile sig_atomic_t pending_pm_shutdown_request;
static PG_GLOBAL_RUNTIME volatile sig_atomic_t pending_pm_fast_shutdown_request;
static PG_GLOBAL_RUNTIME volatile sig_atomic_t pending_pm_immediate_shutdown_request;
static PG_GLOBAL_RUNTIME Latch *postmaster_pmsignal_latch;

/* event multiplexing object */
static PG_GLOBAL_RUNTIME WaitEventSet *pm_wait_set;

#ifdef USE_SSL
/* Set when and if SSL has been initialized properly */
PG_GLOBAL_RUNTIME bool LoadedSSL = false;
#endif

#ifdef USE_BONJOUR
static PG_GLOBAL_RUNTIME DNSServiceRef bonjour_sdref = NULL;
#endif

/* State for IO worker management. */
static PG_GLOBAL_RUNTIME TimestampTz io_worker_launch_next_time = 0;
static PG_GLOBAL_RUNTIME int io_worker_count = 0;
static PG_GLOBAL_RUNTIME PMChild *io_worker_children[MAX_IO_WORKERS];
static PG_GLOBAL_RUNTIME bool io_worker_thread_handoff_pending[MAX_IO_WORKERS];
static PG_GLOBAL_RUNTIME bool syslogger_thread_handoff_pending = false;
static PG_GLOBAL_RUNTIME bool bgwriter_thread_handoff_pending = false;
static PG_GLOBAL_RUNTIME bool checkpointer_thread_handoff_pending = false;

/*
 * postmaster.c - function prototypes
 */
static void CloseServerPorts(int status, Datum arg);
static void unlink_external_pid_file(int status, Datum arg);
static void getInstallationPaths(const char *argv0);
static void checkControlFile(void);
static void handle_pm_pmsignal_signal(SIGNAL_ARGS);
static void handle_pm_child_exit_signal(SIGNAL_ARGS);
static void handle_pm_reload_request_signal(SIGNAL_ARGS);
static void handle_pm_shutdown_request_signal(SIGNAL_ARGS);
static void set_postmaster_signal_latch(void);
static void process_pm_pmsignal(void);
static void process_pm_child_exit(void);
static void process_pm_reload_request(void);
static void process_pm_shutdown_request(void);
static void dummy_handler(SIGNAL_ARGS);
static void process_pm_thread_startup_complete(void);
static void process_pm_pooled_logical_exit(void);
static void process_pm_thread_exit(void);
static void CleanupBackend(PMChild *bp, int exitstatus);
static bool cleanup_archiver_child(PMChild *child, int exitstatus);
static bool cleanup_autovac_launcher_child(PMChild *child, int exitstatus);
static bool cleanup_bgwriter_child(PMChild *child, int exitstatus,
								   int crash_pid);
static bool cleanup_checkpointer_child(PMChild *child, int exitstatus,
									   int crash_pid);
static bool cleanup_io_worker_child(PMChild *child);
static bool cleanup_slot_sync_worker_child(PMChild *child, int exitstatus,
										   int crash_pid);
static bool cleanup_startup_child(PMChild *child, int exitstatus,
								  int crash_pid);
static bool cleanup_wal_receiver_child(PMChild *child, int exitstatus,
									   int crash_pid);
static bool cleanup_wal_writer_child(PMChild *child, int exitstatus);
static bool cleanup_wal_summarizer_child(PMChild *child, int exitstatus);
static void HandleChildCrash(int pid, int exitstatus, const char *procname);
static void LogChildExit(int lev, const char *procname,
						 int pid, int exitstatus);
static void PostmasterStateMachine(void);
static void UpdatePMState(PMState newState);

pg_noreturn static void ExitPostmaster(int status);
static int	ServerLoop(void);
static int	BackendStartup(ClientSocket *client_sock);
static void report_fork_failure_to_client(ClientSocket *client_sock, int errnum);
static CAC_state canAcceptConnections(BackendType backend_type);
static void signal_child(PMChild *pmchild, int signal);
static bool thread_child_signal_interrupt(PMChild *pmchild, int signal,
										  PgBackendInterruptType *interrupt);
static bool SignalChildren(int signal, BackendTypeMask targetMask);
static void TerminateChildren(int signal);
static int	CountChildren(BackendTypeMask targetMask);
static void LaunchMissingBackgroundProcesses(void);
static void maybe_start_bgworkers(void);
static bool maybe_reap_io_worker(int pid);
static void maybe_handoff_syslogger(void);
static void maybe_handoff_io_workers(void);
static void maybe_start_io_workers(void);
static TimestampTz maybe_start_io_workers_scheduled_at(void);
static bool CreateOptsFile(int argc, char *argv[], char *fullprogname);
static PMChild *StartChildProcess(BackendType type);
static void StartSysLogger(void);
static void StartAutovacuumWorker(void);
static bool StartBackgroundWorker(RegisteredBgWorker *rw);
static void InitPostmasterDeathWatchHandle(void);

#ifdef WIN32
#define WNOHANG 0				/* ignored, so any integer value will do */

static pid_t waitpid(pid_t pid, int *exitstatus, int options);
static void WINAPI pgwin32_deadchild_callback(PVOID lpParameter, BOOLEAN TimerOrWaitFired);

static PG_GLOBAL_RUNTIME HANDLE win32ChildQueue;

typedef struct
{
	HANDLE		waitHandle;
	HANDLE		procHandle;
	DWORD		procId;
} win32_deadchild_waitinfo;
#endif							/* WIN32 */

/* Macros to check exit status of a child process */
#define EXIT_STATUS_0(st)  ((st) == 0)
#define EXIT_STATUS_1(st)  (WIFEXITED(st) && WEXITSTATUS(st) == 1)
#define EXIT_STATUS_3(st)  (WIFEXITED(st) && WEXITSTATUS(st) == 3)

#ifndef WIN32
/*
 * File descriptors for pipe used to monitor if postmaster is alive.
 * First is POSTMASTER_FD_WATCH, second is POSTMASTER_FD_OWN.
 */
PG_GLOBAL_RUNTIME int postmaster_alive_fds[2] = {-1, -1};
#else
/* Process handle of postmaster used for the same purpose on Windows */
PG_GLOBAL_RUNTIME HANDLE PostmasterHandle;
#endif

/*
 * Postmaster main entry point
 */
void
PostmasterMain(int argc, char *argv[])
{
	pg_getopt_ctx optctx;
	int			opt;
	int			status;
	char	   *userDoption = NULL;
	bool		listen_addr_saved = false;
	char	   *output_config_variable = NULL;

	InitProcessGlobals();

	PostmasterPid = MyProcPid;

	IsPostmasterEnvironment = true;

	/*
	 * Start our win32 signal implementation
	 */
#ifdef WIN32
	pgwin32_signal_initialize();
#endif

	/*
	 * We should not be creating any files or directories before we check the
	 * data directory (see checkDataDir()), but just in case set the umask to
	 * the most restrictive (owner-only) permissions.
	 *
	 * checkDataDir() will reset the umask based on the data directory
	 * permissions.
	 */
	umask(PG_MODE_MASK_OWNER);

	/*
	 * By default, palloc() requests in the postmaster will be allocated in
	 * the PostmasterContext, which is space that can be recycled by backends.
	 * Allocated data that needs to be available to backends should be
	 * allocated in TopMemoryContext.
	 */
	PostmasterContext = AllocSetContextCreate(TopMemoryContext,
											  "Postmaster",
											  ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(PostmasterContext);

	/* Initialize paths to installation files */
	getInstallationPaths(argv[0]);

	/*
	 * Set up signal handlers for the postmaster process.
	 *
	 * CAUTION: when changing this list, check for side-effects on the signal
	 * handling setup of child processes.  See tcop/postgres.c,
	 * bootstrap/bootstrap.c, postmaster/bgwriter.c, postmaster/walwriter.c,
	 * postmaster/autovacuum.c, postmaster/pgarch.c, postmaster/syslogger.c,
	 * postmaster/bgworker.c and postmaster/checkpointer.c.
	 */
	pqinitmask();
	sigprocmask(SIG_SETMASK, &BlockSig, NULL);

	pqsignal(SIGHUP, handle_pm_reload_request_signal);
	pqsignal(SIGINT, handle_pm_shutdown_request_signal);
	pqsignal(SIGQUIT, handle_pm_shutdown_request_signal);
	pqsignal(SIGTERM, handle_pm_shutdown_request_signal);
	pqsignal(SIGALRM, PG_SIG_IGN);	/* ignored */
	pqsignal(SIGPIPE, PG_SIG_IGN);	/* ignored */
	pqsignal(SIGUSR1, handle_pm_pmsignal_signal);
	pqsignal(SIGUSR2, dummy_handler);	/* unused, reserve for children */
	pqsignal(SIGCHLD, handle_pm_child_exit_signal);

	/* This may configure SIGURG, depending on platform. */
	InitializeWaitEventSupport();
	InitProcessLocalLatch();

	/*
	 * No other place in Postgres should touch SIGTTIN/SIGTTOU handling.  We
	 * ignore those signals in a postmaster environment, so that there is no
	 * risk of a child process freezing up due to writing to stderr.  But for
	 * a standalone backend, their default handling is reasonable.  Hence, all
	 * child processes should just allow the inherited settings to stand.
	 */
#ifdef SIGTTIN
	pqsignal(SIGTTIN, PG_SIG_IGN);	/* ignored */
#endif
#ifdef SIGTTOU
	pqsignal(SIGTTOU, PG_SIG_IGN);	/* ignored */
#endif

	/* ignore SIGXFSZ, so that ulimit violations work like disk full */
#ifdef SIGXFSZ
	pqsignal(SIGXFSZ, PG_SIG_IGN);	/* ignored */
#endif

	/* Begin accepting signals. */
	sigprocmask(SIG_SETMASK, &UnBlockSig, NULL);

	/*
	 * Options setup
	 */
	InitializeGUCOptions();

	/*
	 * Parse command-line options.  CAUTION: keep this in sync with
	 * tcop/postgres.c (the option sets should not conflict) and with the
	 * common help() function in main/main.c.
	 */
	pg_getopt_start(&optctx, argc, argv, "B:bC:c:D:d:EeFf:h:ijk:lN:OPp:r:S:sTt:W:-:");
	optctx.opterr = 1;
	while ((opt = pg_getopt_next(&optctx)) != -1)
	{
		switch (opt)
		{
			case 'B':
				SetConfigOption("shared_buffers", optctx.optarg, PGC_POSTMASTER, PGC_S_ARGV);
				break;

			case 'b':
				/* Undocumented flag used for binary upgrades */
				IsBinaryUpgrade = true;
				break;

			case 'C':
				output_config_variable = strdup(optctx.optarg);
				break;

			case '-':

				/*
				 * Error if the user misplaced a special must-be-first option
				 * for dispatching to a subprogram.  parse_dispatch_option()
				 * returns DISPATCH_POSTMASTER if it doesn't find a match, so
				 * error for anything else.
				 */
				if (parse_dispatch_option(optctx.optarg) != DISPATCH_POSTMASTER)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("--%s must be first argument", optctx.optarg)));

				pg_fallthrough;
			case 'c':
				{
					char	   *name,
							   *value;

					ParseLongOption(optctx.optarg, &name, &value);
					if (!value)
					{
						if (opt == '-')
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("--%s requires a value",
											optctx.optarg)));
						else
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("-c %s requires a value",
											optctx.optarg)));
					}

					SetConfigOption(name, value, PGC_POSTMASTER, PGC_S_ARGV);
					pfree(name);
					pfree(value);
					break;
				}

			case 'D':
				userDoption = strdup(optctx.optarg);
				break;

			case 'd':
				set_debug_options(atoi(optctx.optarg), PGC_POSTMASTER, PGC_S_ARGV);
				break;

			case 'E':
				SetConfigOption("log_statement", "all", PGC_POSTMASTER, PGC_S_ARGV);
				break;

			case 'e':
				SetConfigOption("datestyle", "euro", PGC_POSTMASTER, PGC_S_ARGV);
				break;

			case 'F':
				SetConfigOption("fsync", "false", PGC_POSTMASTER, PGC_S_ARGV);
				break;

			case 'f':
				if (!set_plan_disabling_options(optctx.optarg, PGC_POSTMASTER, PGC_S_ARGV))
				{
					write_stderr("%s: invalid argument for option -f: \"%s\"\n",
								 progname, optctx.optarg);
					ExitPostmaster(1);
				}
				break;

			case 'h':
				SetConfigOption("listen_addresses", optctx.optarg, PGC_POSTMASTER, PGC_S_ARGV);
				break;

			case 'i':
				SetConfigOption("listen_addresses", "*", PGC_POSTMASTER, PGC_S_ARGV);
				break;

			case 'j':
				/* only used by interactive backend */
				break;

			case 'k':
				SetConfigOption("unix_socket_directories", optctx.optarg, PGC_POSTMASTER, PGC_S_ARGV);
				break;

			case 'l':
				SetConfigOption("ssl", "true", PGC_POSTMASTER, PGC_S_ARGV);
				break;

			case 'N':
				SetConfigOption("max_connections", optctx.optarg, PGC_POSTMASTER, PGC_S_ARGV);
				break;

			case 'O':
				SetConfigOption("allow_system_table_mods", "true", PGC_POSTMASTER, PGC_S_ARGV);
				break;

			case 'P':
				SetConfigOption("ignore_system_indexes", "true", PGC_POSTMASTER, PGC_S_ARGV);
				break;

			case 'p':
				SetConfigOption("port", optctx.optarg, PGC_POSTMASTER, PGC_S_ARGV);
				break;

			case 'r':
				/* only used by single-user backend */
				break;

			case 'S':
				SetConfigOption("work_mem", optctx.optarg, PGC_POSTMASTER, PGC_S_ARGV);
				break;

			case 's':
				SetConfigOption("log_statement_stats", "true", PGC_POSTMASTER, PGC_S_ARGV);
				break;

			case 'T':

				/*
				 * This option used to be defined as sending SIGSTOP after a
				 * backend crash, but sending SIGABRT seems more useful.
				 */
				SetConfigOption("send_abort_for_crash", "true", PGC_POSTMASTER, PGC_S_ARGV);
				break;

			case 't':
				{
					const char *tmp = get_stats_option_name(optctx.optarg);

					if (tmp)
					{
						SetConfigOption(tmp, "true", PGC_POSTMASTER, PGC_S_ARGV);
					}
					else
					{
						write_stderr("%s: invalid argument for option -t: \"%s\"\n",
									 progname, optctx.optarg);
						ExitPostmaster(1);
					}
					break;
				}

			case 'W':
				SetConfigOption("post_auth_delay", optctx.optarg, PGC_POSTMASTER, PGC_S_ARGV);
				break;

			default:
				write_stderr("Try \"%s --help\" for more information.\n",
							 progname);
				ExitPostmaster(1);
		}
	}

	/*
	 * Postmaster accepts no non-option switch arguments.
	 */
	if (optctx.optind < argc)
	{
		write_stderr("%s: invalid argument: \"%s\"\n",
					 progname, argv[optctx.optind]);
		write_stderr("Try \"%s --help\" for more information.\n",
					 progname);
		ExitPostmaster(1);
	}

	/*
	 * Locate the proper configuration files and data directory, and read
	 * postgresql.conf for the first time.
	 */
	if (!SelectConfigFiles(userDoption, progname))
		ExitPostmaster(2);

	if (output_config_variable != NULL)
	{
		/*
		 * If this is a runtime-computed GUC, it hasn't yet been initialized,
		 * and the present value is not useful.  However, this is a convenient
		 * place to print the value for most GUCs because it is safe to run
		 * postmaster startup to this point even if the server is already
		 * running.  For the handful of runtime-computed GUCs that we cannot
		 * provide meaningful values for yet, we wait until later in
		 * postmaster startup to print the value.  We won't be able to use -C
		 * on running servers for those GUCs, but using this option now would
		 * lead to incorrect results for them.
		 */
		int			flags = GetConfigOptionFlags(output_config_variable, true);

		if ((flags & GUC_RUNTIME_COMPUTED) == 0)
		{
			/*
			 * "-C guc" was specified, so print GUC's value and exit.  No
			 * extra permission check is needed because the user is reading
			 * inside the data dir.
			 */
			const char *config_val = GetConfigOption(output_config_variable,
													 false, false);

			puts(config_val ? config_val : "");
			ExitPostmaster(0);
		}

		/*
		 * A runtime-computed GUC will be printed later on.  As we initialize
		 * a server startup sequence, silence any log messages that may show
		 * up in the output generated.  FATAL and more severe messages are
		 * useful to show, even if one would only expect at least PANIC.  LOG
		 * entries are hidden.
		 */
		SetConfigOption("log_min_messages", "FATAL", PGC_SUSET,
						PGC_S_OVERRIDE);
	}

	/* Verify that DataDir looks reasonable */
	checkDataDir();

	/* Check that pg_control exists */
	checkControlFile();

	/* And switch working directory into it */
	ChangeToDataDir();

#ifdef USE_XTC_CARRIER
	/*
	 * xtc-carrier interim: under multithreaded=on, route the default
	 * io_method=worker to the in-fiber "xtc" method.  A threaded client backend
	 * runs as an xtc fiber inside the postmaster process, but io_method=worker
	 * io workers are FORKED processes; a forked worker that completes a fiber
	 * backend's async IO cannot wake the in-process fiber (SetLatch from another
	 * process takes the cross-process kill() path aimed at the postmaster, not
	 * the fiber's carrier loop), so the backend hangs on its first uncached read
	 * (the hot-standby InitPostgres hang).
	 *
	 * The xtc method does issuer-async READV/WRITEV on the issuing fiber
	 * (xtc_aio_preadv/pwritev park the fiber and complete on it -- no foreign
	 * completer -- while sibling fibers on the loop keep running), falling back
	 * to synchronous for non-fiber issuers.  It is preferred over "sync": sync
	 * blocks the carrier loop for the whole read (serializing every fiber on
	 * that loop behind one backend's disk IO), whereas xtc preserves carrier
	 * concurrency.  (The earlier interim used sync while an xtc concurrent-read
	 * SIGSEGV was outstanding; that was fixed by saving/restoring current-work
	 * across the AIO fiber park -- commit c8077b2c62b -- so xtc is now the
	 * correct default here.)
	 *
	 * We rewrite the GUC value itself (not just pgaio_method_ops) so SHOW
	 * io_method, pgaio_workers_enabled() (no idle io-worker forks), and every
	 * backend's assign_io_method stay consistent -- doing it here, after
	 * SelectConfigFiles() has finalized multithreaded, avoids the boot-value
	 * ordering hazard where the io_method assign hook runs before multithreaded
	 * is known.  An explicit io_method=sync/io_uring/xtc is respected;
	 * multithreaded=off (process mode) is untouched.  Remove once io workers run
	 * in-process as thread carriers (Tier D/F hand-off, including a
	 * PM_HOT_STANDBY hand-off for standbys), after which io_method=worker wakes
	 * fibers via the wired xtc_proc_wake.
	 */
	if (multithreaded && io_method == IOMETHOD_WORKER)
	{
		SetConfigOption("io_method", "xtc", PGC_POSTMASTER, PGC_S_OVERRIDE);
		ereport(LOG,
				(errmsg("multithreaded=on: routing io_method=worker to the in-fiber \"xtc\" method"),
				 errdetail("Forked io workers cannot wake in-process fiber backends; set io_method explicitly to override.")));
	}

	/*
	 * Pooled protocol mode is the default under multithreaded=on.  The GUC's
	 * boot value -1 means "auto": derive a bounded carrier pool so threaded
	 * client backends multiplex over a few carriers instead of one OS thread
	 * per session.  An explicit 0 forces thread-per-session (still supported);
	 * an explicit positive value is respected verbatim.  Under
	 * multithreaded=off the pooled scheduler does not run, so resolve the -1
	 * sentinel down to 0 to keep every `pooled_protocol_carriers > 0` consumer
	 * (and SHOW) honest in process mode.
	 *
	 * Done here, after SelectConfigFiles() has finalized multithreaded, for the
	 * same boot-value ordering reason as the io_method remap above.
	 */
	if (pooled_protocol_carriers < 0)
	{
		int			resolved;

		if (multithreaded)
		{
			long		ncpus = sysconf(_SC_NPROCESSORS_ONLN);

			if (ncpus < 1)
				ncpus = 1;
			/*
			 * Size the pool to overlap disk/lock waits across sessions.  A
			 * carrier-count sweep of a CPU-bound in-RAM pgbench on a 96-core
			 * box (Session 5) showed pooled throughput climbing with carriers
			 * up to ~cpus/4 (8->42k, 16->69k, 24->82k tps) and flattening
			 * beyond, so scale with cores at a quarter of nproc rather than a
			 * flat cap.  Floor of 8 keeps small boxes responsive; the value is
			 * still bounded by nproc (never more carriers than cores) and by
			 * MaxConnections (a carrier per session is pointless past that).
			 * Explicit tuning overrides all of this.
			 */
			resolved = (int) Max(8, ncpus / 4);
			if (resolved > ncpus)
				resolved = (int) ncpus;
			if (MaxConnections > 0 && resolved > MaxConnections)
				resolved = MaxConnections;
			if (resolved < 1)
				resolved = 1;
		}
		else
			resolved = 0;

		{
			char		buf[16];

			snprintf(buf, sizeof(buf), "%d", resolved);
			SetConfigOption("pooled_protocol_carriers", buf,
							PGC_POSTMASTER, PGC_S_OVERRIDE);
		}
		if (multithreaded)
			ereport(LOG,
					(errmsg("multithreaded=on: pooled protocol scheduler enabled with %d carrier%s (default)",
							resolved, resolved == 1 ? "" : "s"),
					 errdetail("Set pooled_protocol_carriers=0 for thread-per-session, or a positive value to fix the pool size.")));
	}
#endif

	/*
	 * Check for invalid combinations of GUC settings.
	 */
	if (SuperuserReservedConnections + ReservedConnections >= MaxConnections)
	{
		write_stderr("%s: \"superuser_reserved_connections\" (%d) plus \"reserved_connections\" (%d) must be less than \"max_connections\" (%d)\n",
					 progname,
					 SuperuserReservedConnections, ReservedConnections,
					 MaxConnections);
		ExitPostmaster(1);
	}
	if (XLogArchiveMode > ARCHIVE_MODE_OFF && wal_level == WAL_LEVEL_MINIMAL)
		ereport(ERROR,
				(errmsg("WAL archival cannot be enabled when \"wal_level\" is \"minimal\"")));
	if (max_wal_senders > 0 && wal_level == WAL_LEVEL_MINIMAL)
		ereport(ERROR,
				(errmsg("WAL streaming (\"max_wal_senders\" > 0) requires \"wal_level\" to be \"replica\" or \"logical\"")));
	if (summarize_wal && wal_level == WAL_LEVEL_MINIMAL)
		ereport(ERROR,
				(errmsg("WAL cannot be summarized when \"wal_level\" is \"minimal\"")));
	if (sync_replication_slots && wal_level == WAL_LEVEL_MINIMAL)
		ereport(ERROR,
				(errmsg("replication slot synchronization (\"sync_replication_slots\" = on) requires \"wal_level\" to be \"replica\" or \"logical\"")));

	/*
	 * Other one-time internal sanity checks can go here, if they are fast.
	 * (Put any slow processing further down, after postmaster.pid creation.)
	 */
	if (!CheckDateTokenTables())
	{
		write_stderr("%s: invalid datetoken tables, please fix\n", progname);
		ExitPostmaster(1);
	}

	/* For debugging: display postmaster environment */
	if (message_level_is_interesting(DEBUG3))
	{
#if !defined(WIN32)
		extern char **environ;
#endif
		char	  **p;
		StringInfoData si;

		initStringInfo(&si);

		appendStringInfoString(&si, "initial environment dump:");
		for (p = environ; *p; ++p)
			appendStringInfo(&si, "\n%s", *p);

		ereport(DEBUG3, errmsg_internal("%s", si.data));
		pfree(si.data);
	}

	/*
	 * Create lockfile for data directory.
	 *
	 * We want to do this before we try to grab the input sockets, because the
	 * data directory interlock is more reliable than the socket-file
	 * interlock (thanks to whoever decided to put socket files in /tmp :-().
	 * For the same reason, it's best to grab the TCP socket(s) before the
	 * Unix socket(s).
	 *
	 * Also note that this internally sets up the on_proc_exit function that
	 * is responsible for removing both data directory and socket lockfiles;
	 * so it must happen before opening sockets so that at exit, the socket
	 * lockfiles go away after CloseServerPorts runs.
	 */
	CreateDataDirLockFile(true);

	/*
	 * Read the control file (for error checking and config info).
	 *
	 * Since we verify the control file's CRC, this has a useful side effect
	 * on machines where we need a run-time test for CRC support instructions.
	 * The postmaster will do the test once at startup, and then its child
	 * processes will inherit the correct function pointer and not need to
	 * repeat the test.
	 */
	LocalProcessControlFile(false);

	/*
	 * Register the apply launcher.  It's probably a good idea to call this
	 * before any modules had a chance to take the background worker slots.
	 */
	ApplyLauncherRegister();

	/*
	 * Register the shared memory needs of all core subsystems.
	 */
	RegisterBuiltinShmemCallbacks();

	/*
	 * process any libraries that should be preloaded at postmaster start
	 */
	process_shared_preload_libraries();

	/*
	 * Under multithreaded=on, shared_preload_libraries were just dlopen'd into
	 * the postmaster's address space -- which the carrier threads will share.
	 * They were loaded while the runtime still advertised the PROCESS model, so
	 * a process-only (unmarked/legacy) preload library would have passed the
	 * loader gate and then silently run inside the multithreaded process.  Re-
	 * check every loaded module against the backend model the carriers will
	 * actually demand, and fail startup with a clear message if any preloaded
	 * library cannot run threaded.  (A process-only extension can still be used
	 * by running with multithreaded=off, or -- for a per-session need rather
	 * than a server-wide preload -- via the Phase 19 process-fallback route.)
	 */
	if (multithreaded)
		check_loaded_modules_backend_model(PgRuntimePooledProtocolRequested() ?
										   PG_BACKEND_MODEL_POOLED_PROTOCOL_AFFINE :
										   PG_BACKEND_MODEL_THREAD_PER_SESSION);

	/*
	 * Initialize SSL library, if specified.
	 */
#ifdef USE_SSL
	if (EnableSSL)
	{
		(void) secure_initialize(true);
		LoadedSSL = true;
	}
#endif

	/*
	 * Now that loadable modules have had their chance to alter any GUCs,
	 * calculate MaxBackends and initialize the machinery to track child
	 * processes.
	 */
	InitializeMaxBackends();
	InitPostmasterChildSlots();

	/*
	 * Calculate the size of the PGPROC fast-path lock arrays.
	 */
	InitializeFastPathLocks();

	/*
	 * Also call any legacy shmem request hooks that might've been installed
	 * by preloaded libraries.
	 *
	 * Note: this must be done before ShmemCallRequestCallbacks(), because the
	 * hooks may request LWLocks with RequestNamedLWLockTranche(), which in
	 * turn affects the size of the LWLock array calculated in lwlock.c.
	 */
	process_shmem_requests();

	/*
	 * Ask all subsystems, including preloaded libraries, to register their
	 * shared memory needs.
	 */
	ShmemCallRequestCallbacks();

	/*
	 * Now that loadable modules have had their chance to request additional
	 * shared memory, determine the value of any runtime-computed GUCs that
	 * depend on the amount of shared memory required.
	 */
	InitializeShmemGUCs();

	/*
	 * Now that modules have been loaded, we can process any custom resource
	 * managers specified in the wal_consistency_checking GUC.
	 */
	InitializeWalConsistencyChecking();

	/*
	 * If -C was specified with a runtime-computed GUC, we held off printing
	 * the value earlier, as the GUC was not yet initialized.  We handle -C
	 * for most GUCs before we lock the data directory so that the option may
	 * be used on a running server.  However, a handful of GUCs are runtime-
	 * computed and do not have meaningful values until after locking the data
	 * directory, and we cannot safely calculate their values earlier on a
	 * running server.  At this point, such GUCs should be properly
	 * initialized, and we haven't yet set up shared memory, so this is a good
	 * time to handle the -C option for these special GUCs.
	 */
	if (output_config_variable != NULL)
	{
		const char *config_val = GetConfigOption(output_config_variable,
												 false, false);

		puts(config_val ? config_val : "");
		ExitPostmaster(0);
	}

	/*
	 * Set up shared memory and semaphores.
	 *
	 * Note: if using SysV shmem and/or semas, each postmaster startup will
	 * normally choose the same IPC keys.  This helps ensure that we will
	 * clean up dead IPC objects if the postmaster crashes and is restarted.
	 */
	CreateSharedMemoryAndSemaphores();

	/*
	 * Estimate number of openable files.  This must happen after setting up
	 * semaphores, because on some platforms semaphores count as open files.
	 */
	set_max_safe_fds();

	/*
	 * Initialize pipe (or process handle on Windows) that allows children to
	 * wake up from sleep on postmaster death.
	 */
	InitPostmasterDeathWatchHandle();

#ifdef WIN32

	/*
	 * Initialize I/O completion port used to deliver list of dead children.
	 */
	win32ChildQueue = CreateIoCompletionPort(INVALID_HANDLE_VALUE, NULL, 0, 1);
	if (win32ChildQueue == NULL)
		ereport(FATAL,
				(errmsg("could not create I/O completion port for child queue")));
#endif

	/*
	 * Write out nondefault GUC settings for child processes and threaded
	 * backend carriers to use.
	 */
#ifdef EXEC_BACKEND
	write_nondefault_variables(PGC_POSTMASTER);
#else
	if (multithreaded)
		write_nondefault_variables(PGC_POSTMASTER);
#endif

#ifdef EXEC_BACKEND
	/*
	 * Clean out the temp directory used to transmit parameters to child
	 * processes (see internal_forkexec).  We must do this before launching
	 * any child processes, else we have a race condition: we could remove a
	 * parameter file before the child can read it.  It should be safe to do
	 * so now, because we verified earlier that there are no conflicting
	 * Postgres processes in this data directory.
	 */
	RemovePgTempFilesInDir(PG_TEMP_FILES_DIR, true, false);
#endif

	/*
	 * Forcibly remove the files signaling a standby promotion request.
	 * Otherwise, the existence of those files triggers a promotion too early,
	 * whether a user wants that or not.
	 *
	 * This removal of files is usually unnecessary because they can exist
	 * only during a few moments during a standby promotion. However there is
	 * a race condition: if pg_ctl promote is executed and creates the files
	 * during a promotion, the files can stay around even after the server is
	 * brought up to be the primary.  Then, if a new standby starts by using
	 * the backup taken from the new primary, the files can exist at server
	 * startup and must be removed in order to avoid an unexpected promotion.
	 *
	 * Note that promotion signal files need to be removed before the startup
	 * process is invoked. Because, after that, they can be used by
	 * postmaster's SIGUSR1 signal handler.
	 */
	RemovePromoteSignalFiles();

	/* Do the same for logrotate signal file */
	RemoveLogrotateSignalFiles();

	/* Remove any outdated file holding the current log filenames. */
	if (unlink(LOG_METAINFO_DATAFILE) < 0 && errno != ENOENT)
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not remove file \"%s\": %m",
						LOG_METAINFO_DATAFILE)));

	/*
	 * If enabled, start up syslogger collection subprocess
	 */
	if (Logging_collector)
		StartSysLogger();

	/*
	 * Reset whereToSendOutput from DestDebug (its starting state) to
	 * DestNone. This stops ereport from sending log messages to stderr unless
	 * Log_destination permits.  We don't do this until the postmaster is
	 * fully launched, since startup failures may as well be reported to
	 * stderr.
	 *
	 * If we are in fact disabling logging to stderr, first emit a log message
	 * saying so, to provide a breadcrumb trail for users who may not remember
	 * that their logging is configured to go somewhere else.
	 */
	if (!(Log_destination & LOG_DESTINATION_STDERR))
		ereport(LOG,
				(errmsg("ending log output to stderr"),
				 errhint("Future log output will go to log destination \"%s\".",
						 Log_destination_string)));

	whereToSendOutput = DestNone;

	/*
	 * Report server startup in log.  While we could emit this much earlier,
	 * it seems best to do so after starting the log collector, if we intend
	 * to use one.
	 */
	ereport(LOG,
			(errmsg("starting %s", PG_VERSION_STR)));

	/*
	 * Establish input sockets.
	 *
	 * First set up an on_proc_exit function that's charged with closing the
	 * sockets again at postmaster shutdown.
	 */
	ListenSockets = palloc(MAXLISTEN * sizeof(pgsocket));
	on_proc_exit(CloseServerPorts, 0);

	if (ListenAddresses)
	{
		char	   *rawstring;
		List	   *elemlist;
		ListCell   *l;
		int			success = 0;

		/* Need a modifiable copy of ListenAddresses */
		rawstring = pstrdup(ListenAddresses);

		/* Parse string into list of hostnames */
		if (!SplitGUCList(rawstring, ',', &elemlist))
		{
			/* syntax error in list */
			ereport(FATAL,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid list syntax in parameter \"%s\"",
							"listen_addresses")));
		}

		foreach(l, elemlist)
		{
			char	   *curhost = (char *) lfirst(l);

			if (strcmp(curhost, "*") == 0)
				status = ListenServerPort(AF_UNSPEC, NULL,
										  (unsigned short) PostPortNumber,
										  NULL,
										  ListenSockets,
										  &NumListenSockets,
										  MAXLISTEN);
			else
				status = ListenServerPort(AF_UNSPEC, curhost,
										  (unsigned short) PostPortNumber,
										  NULL,
										  ListenSockets,
										  &NumListenSockets,
										  MAXLISTEN);

			if (status == STATUS_OK)
			{
				success++;
				/* record the first successful host addr in lockfile */
				if (!listen_addr_saved)
				{
					AddToDataDirLockFile(LOCK_FILE_LINE_LISTEN_ADDR, curhost);
					listen_addr_saved = true;
				}
			}
			else
				ereport(WARNING,
						(errmsg("could not create listen socket for \"%s\"",
								curhost)));
		}

		if (!success && elemlist != NIL)
			ereport(FATAL,
					(errmsg("could not create any TCP/IP sockets")));

		list_free(elemlist);
		pfree(rawstring);
	}

#ifdef USE_BONJOUR
	/* Register for Bonjour only if we opened TCP socket(s) */
	if (enable_bonjour && NumListenSockets > 0)
	{
		DNSServiceErrorType err;

		/*
		 * We pass 0 for interface_index, which will result in registering on
		 * all "applicable" interfaces.  It's not entirely clear from the
		 * DNS-SD docs whether this would be appropriate if we have bound to
		 * just a subset of the available network interfaces.
		 */
		err = DNSServiceRegister(&bonjour_sdref,
								 0,
								 0,
								 bonjour_name,
								 "_postgresql._tcp.",
								 NULL,
								 NULL,
								 pg_hton16(PostPortNumber),
								 0,
								 NULL,
								 NULL,
								 NULL);
		if (err != kDNSServiceErr_NoError)
			ereport(LOG,
					(errmsg("DNSServiceRegister() failed: error code %ld",
							(long) err)));

		/*
		 * We don't bother to read the mDNS daemon's reply, and we expect that
		 * it will automatically terminate our registration when the socket is
		 * closed at postmaster termination.  So there's nothing more to be
		 * done here.  However, the bonjour_sdref is kept around so that
		 * forked children can close their copies of the socket.
		 */
	}
#endif

	if (Unix_socket_directories)
	{
		char	   *rawstring;
		List	   *elemlist;
		ListCell   *l;
		int			success = 0;

		/* Need a modifiable copy of Unix_socket_directories */
		rawstring = pstrdup(Unix_socket_directories);

		/* Parse string into list of directories */
		if (!SplitDirectoriesString(rawstring, ',', &elemlist))
		{
			/* syntax error in list */
			ereport(FATAL,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid list syntax in parameter \"%s\"",
							"unix_socket_directories")));
		}

		foreach(l, elemlist)
		{
			char	   *socketdir = (char *) lfirst(l);

			status = ListenServerPort(AF_UNIX, NULL,
									  (unsigned short) PostPortNumber,
									  socketdir,
									  ListenSockets,
									  &NumListenSockets,
									  MAXLISTEN);

			if (status == STATUS_OK)
			{
				success++;
				/* record the first successful Unix socket in lockfile */
				if (success == 1)
					AddToDataDirLockFile(LOCK_FILE_LINE_SOCKET_DIR, socketdir);
			}
			else
				ereport(WARNING,
						(errmsg("could not create Unix-domain socket in directory \"%s\"",
								socketdir)));
		}

		if (!success && elemlist != NIL)
			ereport(FATAL,
					(errmsg("could not create any Unix-domain sockets")));

		list_free_deep(elemlist);
		pfree(rawstring);
	}

	/*
	 * check that we have some socket to listen on
	 */
	if (NumListenSockets == 0)
		ereport(FATAL,
				(errmsg("no socket created for listening")));

	/*
	 * If no valid TCP ports, write an empty line for listen address,
	 * indicating the Unix socket must be used.  Note that this line is not
	 * added to the lock file until there is a socket backing it.
	 */
	if (!listen_addr_saved)
		AddToDataDirLockFile(LOCK_FILE_LINE_LISTEN_ADDR, "");

	/*
	 * Record postmaster options.  We delay this till now to avoid recording
	 * bogus options (eg, unusable port number).
	 */
	if (!CreateOptsFile(argc, argv, my_exec_path))
		ExitPostmaster(1);

	/*
	 * Write the external PID file if requested
	 */
	if (external_pid_file)
	{
		FILE	   *fpidfile = fopen(external_pid_file, "w");

		if (fpidfile)
		{
			fprintf(fpidfile, "%d\n", MyProcPid);
			fclose(fpidfile);

			/* Make PID file world readable */
			if (chmod(external_pid_file, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH) != 0)
				write_stderr("%s: could not change permissions of external PID file \"%s\": %m\n",
							 progname, external_pid_file);
		}
		else
			write_stderr("%s: could not write external PID file \"%s\": %m\n",
						 progname, external_pid_file);

		on_proc_exit(unlink_external_pid_file, 0);
	}

	/*
	 * Remove old temporary files.  At this point there can be no other
	 * Postgres processes running in this directory, so this should be safe.
	 */
	RemovePgTempFiles();

	/*
	 * Initialize the autovacuum subsystem (again, no process start yet)
	 */
	autovac_init();

	/*
	 * Load configuration files for client authentication.
	 */
	if (!load_hba())
	{
		/*
		 * It makes no sense to continue if we fail to load the HBA file,
		 * since there is no way to connect to the database in this case.
		 */
		ereport(FATAL,
		/* translator: %s is a configuration file */
				(errmsg("could not load %s", HbaFileName)));
	}
	if (!load_ident())
	{
		/*
		 * We can start up without the IDENT file, although it means that you
		 * cannot log in using any of the authentication methods that need a
		 * user name mapping. load_ident() already logged the details of error
		 * to the log.
		 */
	}

#ifdef HAVE_PTHREAD_IS_THREADED_NP

	/*
	 * On macOS, libintl replaces setlocale() with a version that calls
	 * CFLocaleCopyCurrent() when its second argument is "" and every relevant
	 * environment variable is unset or empty.  CFLocaleCopyCurrent() makes
	 * the process multithreaded.  The postmaster calls sigprocmask() and
	 * calls fork() without an immediate exec(), both of which have undefined
	 * behavior in a multithreaded program.  A multithreaded postmaster is the
	 * normal case on Windows, which offers neither fork() nor sigprocmask().
	 * Currently, macOS is the only platform having pthread_is_threaded_np(),
	 * so we need not worry whether this HINT is appropriate elsewhere.
	 */
	if (pthread_is_threaded_np() != 0)
		ereport(FATAL,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("postmaster became multithreaded during startup"),
				 errhint("Set the LC_ALL environment variable to a valid locale.")));
#endif

	/*
	 * Remember postmaster startup time
	 */
	PgStartTime = GetCurrentTimestamp();

	/*
	 * Report postmaster status in the postmaster.pid file, to allow pg_ctl to
	 * see what's happening.
	 */
	AddToDataDirLockFile(LOCK_FILE_LINE_PM_STATUS, PM_STATUS_STARTING);

	UpdatePMState(PM_STARTUP);

	/* Make sure we can perform I/O while starting up. */
	maybe_start_io_workers();

	/* Start bgwriter and checkpointer so they can help with recovery */
	if (CheckpointerPMChild == NULL)
		CheckpointerPMChild = StartChildProcess(B_CHECKPOINTER);
	if (BgWriterPMChild == NULL)
		BgWriterPMChild = StartChildProcess(B_BG_WRITER);

	/*
	 * We're ready to rock and roll...
	 */
	StartupPMChild = StartChildProcess(B_STARTUP);
	Assert(StartupPMChild != NULL);
	StartupStatus = STARTUP_RUNNING;

	/* Some workers may be scheduled to start now */
	maybe_start_bgworkers();

	status = ServerLoop();

	/*
	 * ServerLoop probably shouldn't ever return, but if it does, close down.
	 */
	ExitPostmaster(status != STATUS_OK);

	abort();					/* not reached */
}


/*
 * on_proc_exit callback to close server's listen sockets
 */
static void
CloseServerPorts(int status, Datum arg)
{
	int			i;

	/*
	 * First, explicitly close all the socket FDs.  We used to just let this
	 * happen implicitly at postmaster exit, but it's better to close them
	 * before we remove the postmaster.pid lockfile; otherwise there's a race
	 * condition if a new postmaster wants to re-use the TCP port number.
	 */
	for (i = 0; i < NumListenSockets; i++)
	{
		if (closesocket(ListenSockets[i]) != 0)
			elog(LOG, "could not close listen socket: %m");
	}
	NumListenSockets = 0;

	/*
	 * Next, remove any filesystem entries for Unix sockets.  To avoid race
	 * conditions against incoming postmasters, this must happen after closing
	 * the sockets and before removing lock files.
	 */
	RemoveSocketFiles();

	/*
	 * We don't do anything about socket lock files here; those will be
	 * removed in a later on_proc_exit callback.
	 */
}

/*
 * on_proc_exit callback to delete external_pid_file
 */
static void
unlink_external_pid_file(int status, Datum arg)
{
	if (external_pid_file)
		unlink(external_pid_file);
}


/*
 * Compute and check the directory paths to files that are part of the
 * installation (as deduced from the postgres executable's own location)
 */
static void
getInstallationPaths(const char *argv0)
{
	DIR		   *pdir;

	/* Locate the postgres executable itself */
	if (find_my_exec(argv0, my_exec_path) < 0)
		ereport(FATAL,
				(errmsg("%s: could not locate my own executable path", argv0)));

#ifdef FORKEXEC_BACKEND
	/* Locate executable backend before we change working directory */
	if (find_other_exec(argv0, "postgres", PG_BACKEND_VERSIONSTR,
						postgres_exec_path) < 0)
		ereport(FATAL,
				(errmsg("%s: could not locate matching postgres executable",
						argv0)));
#endif

	/*
	 * Locate the pkglib directory --- this has to be set early in case we try
	 * to load any modules from it in response to postgresql.conf entries.
	 */
	get_pkglib_path(my_exec_path, pkglib_path);

	/*
	 * Verify that there's a readable directory there; otherwise the Postgres
	 * installation is incomplete or corrupt.  (A typical cause of this
	 * failure is that the postgres executable has been moved or hardlinked to
	 * some directory that's not a sibling of the installation lib/
	 * directory.)
	 */
	pdir = AllocateDir(pkglib_path);
	if (pdir == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open directory \"%s\": %m",
						pkglib_path),
				 errhint("This may indicate an incomplete PostgreSQL installation, or that the file \"%s\" has been moved away from its proper location.",
						 my_exec_path)));
	FreeDir(pdir);

	/*
	 * It's not worth checking the share/ directory.  If the lib/ directory is
	 * there, then share/ probably is too.
	 */
}

/*
 * Check that pg_control exists in the correct location in the data directory.
 *
 * No attempt is made to validate the contents of pg_control here.  This is
 * just a sanity check to see if we are looking at a real data directory.
 */
static void
checkControlFile(void)
{
	char		path[MAXPGPATH];
	FILE	   *fp;

	snprintf(path, sizeof(path), "%s/%s", DataDir, XLOG_CONTROL_FILE);

	fp = AllocateFile(path, PG_BINARY_R);
	if (fp == NULL)
	{
		write_stderr("%s: could not find the database system\n"
					 "Expected to find it in the directory \"%s\",\n"
					 "but could not open file \"%s\": %m\n",
					 progname, DataDir, path);
		ExitPostmaster(2);
	}
	FreeFile(fp);
}

/*
 * Determine how long should we let ServerLoop sleep, in milliseconds.
 *
 * In normal conditions we wait at most one minute, to ensure that the other
 * background tasks handled by ServerLoop get done even when no requests are
 * arriving.  However, if there are background workers waiting to be started,
 * we don't actually sleep so that they are quickly serviced.  Other exception
 * cases are as shown in the code.
 */
static int
DetermineSleepTime(void)
{
	TimestampTz next_wakeup;

	/*
	 * If in ImmediateShutdown with a SIGKILL timeout, ignore everything else
	 * and wait for that.
	 *
	 * XXX Shouldn't this also test FatalError?
	 */
	if (Shutdown >= ImmediateShutdown)
	{
		if (AbortStartTime != 0)
		{
			time_t		curtime = time(NULL);
			int			seconds;

			/*
			 * time left to abort; clamp to 0 if it already expired, or if
			 * time goes backwards
			 */
			if (curtime < AbortStartTime ||
				curtime - AbortStartTime >= SIGKILL_CHILDREN_AFTER_SECS)
				seconds = 0;
			else
				seconds = SIGKILL_CHILDREN_AFTER_SECS -
					(curtime - AbortStartTime);

			return seconds * 1000;
		}
	}

	/* Time of next maybe_start_io_workers() call, or 0 for none. */
	next_wakeup = maybe_start_io_workers_scheduled_at();

	/* Ignore bgworkers during shutdown. */
	if (StartWorkerNeeded && Shutdown == NoShutdown)
		return 0;

	if (HaveCrashedWorker && Shutdown == NoShutdown)
	{
		dlist_mutable_iter iter;

		/*
		 * When there are crashed bgworkers, we sleep just long enough that
		 * they are restarted when they request to be.  Scan the list to
		 * determine the minimum of all wakeup times according to most recent
		 * crash time and requested restart interval.
		 */
		dlist_foreach_modify(iter, &BackgroundWorkerList)
		{
			RegisteredBgWorker *rw;
			TimestampTz this_wakeup;

			rw = dlist_container(RegisteredBgWorker, rw_lnode, iter.cur);

			if (rw->rw_crashed_at == 0)
				continue;

			if (rw->rw_worker.bgw_restart_time == BGW_NEVER_RESTART
				|| rw->rw_terminate)
			{
				ForgetBackgroundWorker(rw);
				continue;
			}

			this_wakeup = TimestampTzPlusMilliseconds(rw->rw_crashed_at,
													  1000L * rw->rw_worker.bgw_restart_time);
			if (next_wakeup == 0 || this_wakeup < next_wakeup)
				next_wakeup = this_wakeup;
		}
	}

	if (next_wakeup != 0)
	{
		int			ms;

		/* result of TimestampDifferenceMilliseconds is in [0, INT_MAX] */
		ms = (int) TimestampDifferenceMilliseconds(GetCurrentTimestamp(),
												   next_wakeup);
		return Min(60 * 1000, ms);
	}

	return 60 * 1000;
}

/*
 * Activate or deactivate notifications of server socket events.  Since we
 * don't currently have a way to remove events from an existing WaitEventSet,
 * we'll just destroy and recreate the whole thing.  This is called during
 * shutdown so we can wait for backends to exit without accepting new
 * connections, and during crash reinitialization when we need to start
 * listening for new connections again.  The WaitEventSet will be freed in fork
 * children by ClosePostmasterPorts().
 */
static void
ConfigurePostmasterWaitSet(bool accept_connections)
{
	if (pm_wait_set)
		FreeWaitEventSet(pm_wait_set);
	pm_wait_set = NULL;

	postmaster_pmsignal_latch = MyLatch;
	pm_wait_set = CreateWaitEventSet(NULL,
									 accept_connections ? (1 + NumListenSockets) : 1);
	AddWaitEventToSet(pm_wait_set, WL_LATCH_SET, PGINVALID_SOCKET, MyLatch,
					  NULL);

	if (accept_connections)
	{
		for (int i = 0; i < NumListenSockets; i++)
			AddWaitEventToSet(pm_wait_set, WL_SOCKET_ACCEPT, ListenSockets[i],
							  NULL, NULL);
	}
}

/*
 * Main idle loop of postmaster
 */
static int
ServerLoop(void)
{
	time_t		last_lockfile_recheck_time,
				last_touch_time;
	WaitEvent	events[MAXLISTEN];
	int			nevents;

	ConfigurePostmasterWaitSet(true);
	last_lockfile_recheck_time = last_touch_time = time(NULL);

	for (;;)
	{
		time_t		now;

		/*
		 * A startup-era thread carrier can publish completion before the
		 * postmaster has finished creating the wait set and recorded a latch
		 * pointer for direct wakeups.  Drain thread handoff state before
		 * blocking so such early publications cannot sleep until the next
		 * timeout.
		 */
		process_pm_thread_startup_complete();
		process_pm_pooled_logical_exit();
		process_pm_thread_exit();
#ifdef USE_XTC_CARRIER
		if (xtc_pg_consume_genuine_crash())
		{
			ereport(LOG,
					(errmsg("terminating threaded server runtime after backend fiber crash")));
			ExitPostmaster(1);
		}
#endif

		nevents = WaitEventSetWait(pm_wait_set,
								   DetermineSleepTime(),
								   events,
								   lengthof(events),
								   0 /* postmaster posts no wait_events */ );

		/*
		 * Latch set by signal handler, or new connection pending on any of
		 * our sockets?  Reset the latch before handling control-plane work,
		 * then accept any pending connections after the postmaster-visible
		 * child state has been drained.
		 */
		for (int i = 0; i < nevents; i++)
		{
			if (events[i].events & WL_LATCH_SET)
				ResetLatch(MyLatch);
		}

		/*
		 * The following requests are handled once per loop, even if we didn't
		 * see WL_LATCH_SET.  This gives high priority to shutdown and reload
		 * requests where the latch happens to appear later in events[] or will
		 * be reported by a later call to WaitEventSetWait().
		 *
		 * In threaded mode, process-era auxiliary workers can exit as part of
		 * the startup-to-thread handoff while later server activity is driven
		 * by thread-carrier latch notifications and socket readiness.  Drain
		 * process child exits opportunistically once thread carriers exist so
		 * a missed or coalesced SIGCHLD cannot leave a zombie PMChild that
		 * blocks shutdown-state accounting.
		 */
		if (pending_pm_shutdown_request)
			process_pm_shutdown_request();
		if (pending_pm_reload_request)
			process_pm_reload_request();
		if (pending_pm_child_exit ||
			(multithreaded && PostmasterThreadCarriersStarted()))
			process_pm_child_exit();
		if (pending_pm_pmsignal)
			process_pm_pmsignal();
		process_pm_thread_startup_complete();
		process_pm_pooled_logical_exit();
		process_pm_thread_exit();
#ifdef USE_XTC_CARRIER
		/*
		 * #7 Stage 1b: a per-loop supervisor fiber may have observed a genuine
		 * backend-fiber crash (a fault before the fiber reached its clean,
		 * already-published exit).  Treat it like a crashed thread carrier:
		 * under threaded mode the postmaster address space may be compromised,
		 * so terminate the runtime.  Benign xtc_exit_self teardown faults
		 * (post-clean-exit) never set this, so a normal teardown cannot trip
		 * it.
		 */
		if (xtc_pg_consume_genuine_crash())
		{
			ereport(LOG,
					(errmsg("terminating threaded server runtime after backend fiber crash")));
			ExitPostmaster(1);
		}
#endif

		for (int i = 0; i < nevents; i++)
		{
			if (events[i].events & WL_SOCKET_ACCEPT)
			{
				ClientSocket s;

				if (AcceptConnection(events[i].fd, &s) == STATUS_OK)
					BackendStartup(&s);

				/* We no longer need the open socket in this process */
				if (s.sock != PGINVALID_SOCKET)
				{
					if (closesocket(s.sock) != 0)
						elog(LOG, "could not close client socket: %m");
				}
			}
		}

		/*
		 * If we need to launch any background processes after changing state
		 * or because some exited, do so now.
		 */
		LaunchMissingBackgroundProcesses();

		/* If we need to signal the autovacuum launcher, do so now */
		if (avlauncher_needs_signal)
		{
			avlauncher_needs_signal = false;
			if (AutoVacLauncherPMChild != NULL)
				signal_child(AutoVacLauncherPMChild, SIGUSR2);
		}

#ifdef HAVE_PTHREAD_IS_THREADED_NP

		/*
		 * With assertions enabled, check regularly for appearance of
		 * additional threads.  All builds check at start and exit.
		 */
		Assert(multithreaded || pthread_is_threaded_np() == 0);
#endif

		/*
		 * Lastly, check to see if it's time to do some things that we don't
		 * want to do every single time through the loop, because they're a
		 * bit expensive.  Note that there's up to a minute of slop in when
		 * these tasks will be performed, since DetermineSleepTime() will let
		 * us sleep at most that long; except for SIGKILL timeout which has
		 * special-case logic there.
		 */
		now = time(NULL);

		/*
		 * If we already sent SIGQUIT to children and they are slow to shut
		 * down, it's time to send them SIGKILL (or SIGABRT if requested).
		 * This doesn't happen normally, but under certain conditions backends
		 * can get stuck while shutting down.  This is a last measure to get
		 * them unwedged.
		 *
		 * Note we also do this during recovery from a process crash.
		 */
		if ((Shutdown >= ImmediateShutdown || FatalError) &&
			AbortStartTime != 0 &&
			(now - AbortStartTime) >= SIGKILL_CHILDREN_AFTER_SECS)
		{
			/* We were gentle with them before. Not anymore */
			ereport(LOG,
			/* translator: %s is SIGKILL or SIGABRT */
					(errmsg("issuing %s to recalcitrant children",
							send_abort_for_kill ? "SIGABRT" : "SIGKILL")));
			TerminateChildren(send_abort_for_kill ? SIGABRT : SIGKILL);
			/* reset flag so we don't SIGKILL again */
			AbortStartTime = 0;
		}

		/*
		 * Once a minute, verify that postmaster.pid hasn't been removed or
		 * overwritten.  If it has, we force a shutdown.  This avoids having
		 * postmasters and child processes hanging around after their database
		 * is gone, and maybe causing problems if a new database cluster is
		 * created in the same place.  It also provides some protection
		 * against a DBA foolishly removing postmaster.pid and manually
		 * starting a new postmaster.  Data corruption is likely to ensue from
		 * that anyway, but we can minimize the damage by aborting ASAP.
		 */
		if (now - last_lockfile_recheck_time >= 1 * SECS_PER_MINUTE)
		{
			if (!RecheckDataDirLockFile())
			{
				ereport(LOG,
						(errmsg("performing immediate shutdown because data directory lock file is invalid")));
				kill(MyProcPid, SIGQUIT);
			}
			last_lockfile_recheck_time = now;
		}

		/*
		 * Touch Unix socket and lock files every 58 minutes, to ensure that
		 * they are not removed by overzealous /tmp-cleaning tasks.  We assume
		 * no one runs cleaners with cutoff times of less than an hour ...
		 */
		if (now - last_touch_time >= 58 * SECS_PER_MINUTE)
		{
			TouchSocketFiles();
			TouchSocketLockFiles();
			last_touch_time = now;
		}
	}
}

/*
 * canAcceptConnections --- check to see if database state allows connections
 * of the specified type.  backend_type can be B_BACKEND or B_AUTOVAC_WORKER.
 * (Note that we don't yet know whether a normal B_BACKEND connection might
 * turn into a walsender.)
 */
static CAC_state
canAcceptConnections(BackendType backend_type)
{
	CAC_state	result = CAC_OK;

	Assert(backend_type == B_BACKEND || backend_type == B_AUTOVAC_WORKER);

	/*
	 * Can't start backends when in startup/shutdown/inconsistent recovery
	 * state.  We treat autovac workers the same as user backends for this
	 * purpose.
	 */
	if (pmState != PM_RUN && pmState != PM_HOT_STANDBY)
	{
		if (Shutdown > NoShutdown)
			return CAC_SHUTDOWN;	/* shutdown is pending */
		else if (!FatalError && pmState == PM_STARTUP)
			return CAC_STARTUP; /* normal startup */
		else if (!FatalError && pmState == PM_RECOVERY)
			return CAC_NOTHOTSTANDBY;	/* not yet ready for hot standby */
		else
			return CAC_RECOVERY;	/* else must be crash recovery */
	}

	/*
	 * "Smart shutdown" restrictions are applied only to normal connections,
	 * not to autovac workers.
	 */
	if (!connsAllowed && backend_type == B_BACKEND)
		return CAC_SHUTDOWN;	/* shutdown is pending */

	return result;
}

/*
 * ClosePostmasterPorts -- close all the postmaster's open sockets
 *
 * This is called during child process startup to release file descriptors
 * that are not needed by that child process.  The postmaster still has
 * them open, of course.
 *
 * Note: we pass am_syslogger as a boolean because we don't want to set
 * the global variable yet when this is called.
 */
void
ClosePostmasterPorts(bool am_syslogger)
{
	/* Release resources held by the postmaster's WaitEventSet. */
	if (pm_wait_set)
	{
		FreeWaitEventSetAfterFork(pm_wait_set);
		pm_wait_set = NULL;
	}

#ifndef WIN32

	/*
	 * Close the write end of postmaster death watch pipe. It's important to
	 * do this as early as possible, so that if postmaster dies, others won't
	 * think that it's still running because we're holding the pipe open.
	 */
	if (close(postmaster_alive_fds[POSTMASTER_FD_OWN]) != 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg_internal("could not close postmaster death monitoring pipe in child process: %m")));
	postmaster_alive_fds[POSTMASTER_FD_OWN] = -1;
	/* Notify fd.c that we released one pipe FD. */
	ReleaseExternalFD();
#endif

	/*
	 * Close the postmaster's listen sockets.  These aren't tracked by fd.c,
	 * so we don't call ReleaseExternalFD() here.
	 *
	 * The listen sockets are marked as FD_CLOEXEC, so this isn't needed in
	 * EXEC_BACKEND mode.
	 */
#ifndef EXEC_BACKEND
	if (ListenSockets)
	{
		for (int i = 0; i < NumListenSockets; i++)
		{
			if (closesocket(ListenSockets[i]) != 0)
				elog(LOG, "could not close listen socket: %m");
		}
		pfree(ListenSockets);
	}
	NumListenSockets = 0;
	ListenSockets = NULL;
#endif

	/*
	 * If using syslogger, close the read side of the pipe.  We don't bother
	 * tracking this in fd.c, either.
	 */
	if (!am_syslogger)
	{
#ifndef WIN32
		if (syslogPipe[0] >= 0)
			close(syslogPipe[0]);
		syslogPipe[0] = -1;
#else
		if (syslogPipe[0])
			CloseHandle(syslogPipe[0]);
		syslogPipe[0] = 0;
#endif
	}

#ifdef USE_BONJOUR
	/* If using Bonjour, close the connection to the mDNS daemon */
	if (bonjour_sdref)
		close(DNSServiceRefSockFD(bonjour_sdref));
#endif
}


/*
 * InitProcessGlobals -- set MyStartTime[stamp], random seeds
 *
 * Called early in the postmaster and every backend.
 */
void
InitProcessGlobals(void)
{
	MyStartTimestamp = GetCurrentTimestamp();
	MyStartTime = timestamptz_to_time_t(MyStartTimestamp);

	/* initialize timing infrastructure (required for INSTR_* calls) */
	pg_initialize_timing();

	/*
	 * Set a different global seed in every process.  We want something
	 * unpredictable, so if possible, use high-quality random bits for the
	 * seed.  Otherwise, fall back to a seed based on timestamp and PID.
	 */
	if (unlikely(!pg_prng_strong_seed(&pg_global_prng_state)))
	{
		uint64		rseed;

		/*
		 * Since PIDs and timestamps tend to change more frequently in their
		 * least significant bits, shift the timestamp left to allow a larger
		 * total number of seeds in a given time period.  Since that would
		 * leave only 20 bits of the timestamp that cycle every ~1 second,
		 * also mix in some higher bits.
		 */
		rseed = ((uint64) MyProcPid) ^
			((uint64) MyStartTimestamp << 12) ^
			((uint64) MyStartTimestamp >> 20);

		pg_prng_seed(&pg_global_prng_state, rseed);
	}

	/*
	 * Also make sure that we've set a good seed for random(3).  Use of that
	 * is deprecated in core Postgres, but extensions might use it.
	 */
#ifndef WIN32
	srandom(pg_prng_uint32(&pg_global_prng_state));
#endif
}

/*
 * Child processes use SIGUSR1 to notify us of 'pmsignals'.  pg_ctl uses
 * SIGUSR1 to ask postmaster to check for logrotate and promote files.
 */
static void
handle_pm_pmsignal_signal(SIGNAL_ARGS)
{
	pending_pm_pmsignal = true;
	set_postmaster_signal_latch();
}

/*
 * Same-process thread carriers cannot notify the postmaster by sending
 * SIGUSR1 to PostmasterPid: in a threaded runtime that is the current process,
 * and the signal can be delivered to any carrier.  Set the same pending flag
 * as the signal handler and wake the postmaster's recorded latch directly.
 */
void
PostmasterSignalPMSignal(void)
{
	pending_pm_pmsignal = true;

	set_postmaster_signal_latch();
}

bool
PostmasterSignalAutoVacLauncher(void)
{
	if (AutoVacLauncherPMChild == NULL)
		return false;

	signal_child(AutoVacLauncherPMChild, SIGUSR2);
	return true;
}

/*
 * pg_ctl uses SIGHUP to request a reload of the configuration files.
 */
static void
handle_pm_reload_request_signal(SIGNAL_ARGS)
{
	pending_pm_reload_request = true;
	set_postmaster_signal_latch();
}

/*
 * Re-read config files, and tell children to do same.
 */
static void
process_pm_reload_request(void)
{
	pending_pm_reload_request = false;

	ereport(DEBUG2,
			(errmsg_internal("postmaster received reload request signal")));

	if (Shutdown <= SmartShutdown)
	{
		ereport(LOG,
				(errmsg("received SIGHUP, reloading configuration files")));
		ProcessConfigFile(PGC_SIGHUP);
		SignalChildren(SIGHUP, btmask_all_except(B_DEAD_END_BACKEND));

		/* Reload authentication config files too */
		if (!load_hba())
			ereport(LOG,
			/* translator: %s is a configuration file */
					(errmsg("%s was not reloaded", HbaFileName)));

		if (!load_ident())
			ereport(LOG,
					(errmsg("%s was not reloaded", IdentFileName)));

#ifdef USE_SSL
		/* Reload SSL configuration as well */
		if (EnableSSL)
		{
			if (secure_initialize(false) == 0)
				LoadedSSL = true;
			else
				ereport(LOG,
						(errmsg("SSL configuration was not reloaded")));
		}
		else
		{
			secure_destroy();
			LoadedSSL = false;
		}
#endif

		/* Update the starting-point file for future children */
#ifdef EXEC_BACKEND
		write_nondefault_variables(PGC_SIGHUP);
#else
		if (multithreaded)
			write_nondefault_variables(PGC_SIGHUP);
#endif
	}
}

/*
 * pg_ctl uses SIGTERM, SIGINT and SIGQUIT to request different types of
 * shutdown.
 */
static void
handle_pm_shutdown_request_signal(SIGNAL_ARGS)
{
	switch (postgres_signal_arg)
	{
		case SIGTERM:
			/* smart is implied if the other two flags aren't set */
			pending_pm_shutdown_request = true;
			break;
		case SIGINT:
			pending_pm_fast_shutdown_request = true;
			pending_pm_shutdown_request = true;
			break;
		case SIGQUIT:
			pending_pm_immediate_shutdown_request = true;
			pending_pm_shutdown_request = true;
			break;
	}
	set_postmaster_signal_latch();
}

/*
 * Process-directed signals can be delivered to any unmasked carrier thread in
 * a threaded runtime.  The pending postmaster flags are process-global, but
 * waking a carrier-local MyLatch would leave the postmaster asleep, so route
 * wakeups through the latch recorded by ServerLoop() once available.
 */
static void
set_postmaster_signal_latch(void)
{
	Latch	   *latch = postmaster_pmsignal_latch;

	if (latch == NULL)
		latch = MyLatch;

	if (latch != NULL)
		SetLatch(latch);
}

/*
 * Process shutdown request.
 */
static void
process_pm_shutdown_request(void)
{
	int			mode;

	ereport(DEBUG2,
			(errmsg_internal("postmaster received shutdown request signal")));

	pending_pm_shutdown_request = false;

	/*
	 * If more than one shutdown request signal arrived since the last server
	 * loop, take the one that is the most immediate.  That matches the
	 * priority that would apply if we processed them one by one in any order.
	 */
	if (pending_pm_immediate_shutdown_request)
	{
		pending_pm_immediate_shutdown_request = false;
		pending_pm_fast_shutdown_request = false;
		mode = ImmediateShutdown;
	}
	else if (pending_pm_fast_shutdown_request)
	{
		pending_pm_fast_shutdown_request = false;
		mode = FastShutdown;
	}
	else
		mode = SmartShutdown;

	switch (mode)
	{
		case SmartShutdown:

			/*
			 * Smart Shutdown:
			 *
			 * Wait for children to end their work, then shut down.
			 */
			if (Shutdown >= SmartShutdown)
				break;
			Shutdown = SmartShutdown;
			ereport(LOG,
					(errmsg("received smart shutdown request")));

			/* Report status */
			AddToDataDirLockFile(LOCK_FILE_LINE_PM_STATUS, PM_STATUS_STOPPING);
#ifdef USE_SYSTEMD
			sd_notify(0, "STOPPING=1");
#endif

			/*
			 * If we reached normal running, we go straight to waiting for
			 * client backends to exit.  If already in PM_STOP_BACKENDS or a
			 * later state, do not change it.
			 */
			if (pmState == PM_RUN || pmState == PM_HOT_STANDBY)
				connsAllowed = false;
			else if (pmState == PM_STARTUP || pmState == PM_RECOVERY)
			{
				/* There should be no clients, so proceed to stop children */
				UpdatePMState(PM_STOP_BACKENDS);
			}

			/*
			 * Now wait for online backup mode to end and backends to exit. If
			 * that is already the case, PostmasterStateMachine will take the
			 * next step.
			 */
			PostmasterStateMachine();
			break;

		case FastShutdown:

			/*
			 * Fast Shutdown:
			 *
			 * Abort all children with SIGTERM (rollback active transactions
			 * and exit) and shut down when they are gone.
			 */
			if (Shutdown >= FastShutdown)
				break;
			Shutdown = FastShutdown;
			ereport(LOG,
					(errmsg("received fast shutdown request")));

			/* Report status */
			AddToDataDirLockFile(LOCK_FILE_LINE_PM_STATUS, PM_STATUS_STOPPING);
#ifdef USE_SYSTEMD
			sd_notify(0, "STOPPING=1");
#endif

			if (pmState == PM_STARTUP || pmState == PM_RECOVERY)
			{
				/* Just shut down background processes silently */
				UpdatePMState(PM_STOP_BACKENDS);
			}
			else if (pmState == PM_RUN ||
					 pmState == PM_HOT_STANDBY)
			{
				/* Report that we're about to zap live client sessions */
				ereport(LOG,
						(errmsg("aborting any active transactions")));
				UpdatePMState(PM_STOP_BACKENDS);
			}

			/*
			 * PostmasterStateMachine will issue any necessary signals, or
			 * take the next step if no child processes need to be killed.
			 */
			PostmasterStateMachine();
			break;

		case ImmediateShutdown:

			/*
			 * Immediate Shutdown:
			 *
			 * abort all children with SIGQUIT, wait for them to exit,
			 * terminate remaining ones with SIGKILL, then exit without
			 * attempt to properly shut down the data base system.
			 */
			if (Shutdown >= ImmediateShutdown)
				break;
			Shutdown = ImmediateShutdown;
			ereport(LOG,
					(errmsg("received immediate shutdown request")));

			/* Report status */
			AddToDataDirLockFile(LOCK_FILE_LINE_PM_STATUS, PM_STATUS_STOPPING);
#ifdef USE_SYSTEMD
			sd_notify(0, "STOPPING=1");
#endif

			/* tell children to shut down ASAP */
			/* (note we don't apply send_abort_for_crash here) */
			SetQuitSignalReason(PMQUIT_FOR_STOP);
			TerminateChildren(SIGQUIT);
			UpdatePMState(PM_WAIT_BACKENDS);

			/* set stopwatch for them to die */
			AbortStartTime = time(NULL);

			/*
			 * Now wait for backends to exit.  If there are none,
			 * PostmasterStateMachine will take the next step.
			 */
			PostmasterStateMachine();
			break;
	}
}

static void
handle_pm_child_exit_signal(SIGNAL_ARGS)
{
	pending_pm_child_exit = true;
	set_postmaster_signal_latch();
}

/*
 * Cleanup after a child process dies.
 */
static void
process_pm_child_exit(void)
{
	int			pid;			/* process id of dead child process */
	int			exitstatus;		/* its exit status */

	pending_pm_child_exit = false;

	ereport(DEBUG4,
			(errmsg_internal("reaping dead processes")));

	while ((pid = waitpid(-1, &exitstatus, WNOHANG)) > 0)
	{
		PMChild    *pmchild;

		/*
		 * Check if this child was a startup process.
		 */
		if (StartupPMChild && pid == StartupPMChild->pid)
		{
			(void) cleanup_startup_child(StartupPMChild, exitstatus, pid);
			continue;
		}

		/*
		 * Was it the bgwriter?  Normal exit can be ignored; we'll start a new
		 * one at the next iteration of the postmaster's main loop, if
		 * necessary.  Any other exit condition is treated as a crash.
		 */
		if (BgWriterPMChild && pid == BgWriterPMChild->pid)
		{
			(void) cleanup_bgwriter_child(BgWriterPMChild, exitstatus, pid);
			continue;
		}

		/*
		 * Was it the checkpointer?
		 */
		if (CheckpointerPMChild && pid == CheckpointerPMChild->pid)
		{
			(void) cleanup_checkpointer_child(CheckpointerPMChild, exitstatus,
											  pid);
			continue;
		}

		/*
		 * Was it the wal writer?  Normal exit can be ignored; we'll start a
		 * new one at the next iteration of the postmaster's main loop, if
		 * necessary.  Any other exit condition is treated as a crash.
		 */
		if (WalWriterPMChild && pid == WalWriterPMChild->pid)
		{
			ReleasePostmasterChildSlot(WalWriterPMChild);
			WalWriterPMChild = NULL;
			if (!EXIT_STATUS_0(exitstatus))
				HandleChildCrash(pid, exitstatus,
								 _("WAL writer process"));
			continue;
		}

		/*
		 * Was it the wal receiver?  If exit status is zero (normal) or one
		 * (FATAL exit), we assume everything is all right just like normal
		 * backends.  (If we need a new wal receiver, we'll start one at the
		 * next iteration of the postmaster's main loop.)
		 */
		if (WalReceiverPMChild && pid == WalReceiverPMChild->pid)
		{
			(void) cleanup_wal_receiver_child(WalReceiverPMChild, exitstatus,
											  pid);
			continue;
		}

		/*
		 * Was it the wal summarizer? Normal exit can be ignored; we'll start
		 * a new one at the next iteration of the postmaster's main loop, if
		 * necessary.  Any other exit condition is treated as a crash.
		 */
		if (WalSummarizerPMChild && pid == WalSummarizerPMChild->pid)
		{
			ReleasePostmasterChildSlot(WalSummarizerPMChild);
			WalSummarizerPMChild = NULL;
			if (!EXIT_STATUS_0(exitstatus))
				HandleChildCrash(pid, exitstatus,
								 _("WAL summarizer process"));
			continue;
		}

		/*
		 * Was it the autovacuum launcher?	Normal exit can be ignored; we'll
		 * start a new one at the next iteration of the postmaster's main
		 * loop, if necessary.  Any other exit condition is treated as a
		 * crash.
		 */
		if (AutoVacLauncherPMChild && pid == AutoVacLauncherPMChild->pid)
		{
			ReleasePostmasterChildSlot(AutoVacLauncherPMChild);
			AutoVacLauncherPMChild = NULL;
			if (!EXIT_STATUS_0(exitstatus))
				HandleChildCrash(pid, exitstatus,
								 _("autovacuum launcher process"));
			continue;
		}

		/*
		 * Was it the archiver?  If exit status is zero (normal) or one (FATAL
		 * exit), we assume everything is all right just like normal backends
		 * and just try to start a new one on the next cycle of the
		 * postmaster's main loop, to retry archiving remaining files.
		 */
		if (PgArchPMChild && pid == PgArchPMChild->pid)
		{
			ReleasePostmasterChildSlot(PgArchPMChild);
			PgArchPMChild = NULL;
			if (!EXIT_STATUS_0(exitstatus) && !EXIT_STATUS_1(exitstatus))
				HandleChildCrash(pid, exitstatus,
								 _("archiver process"));
			continue;
		}

		/* Was it the system logger?  If so, try to start a new one */
		if (SysLoggerPMChild && pid == SysLoggerPMChild->pid)
		{
			syslogger_thread_handoff_pending = false;
			ReleasePostmasterChildSlot(SysLoggerPMChild);
			SysLoggerPMChild = NULL;

			/* for safety's sake, launch new logger *first* */
			if (Logging_collector)
				StartSysLogger();

			if (!EXIT_STATUS_0(exitstatus))
				LogChildExit(LOG, _("system logger process"),
							 pid, exitstatus);
			continue;
		}

		/*
		 * Was it the slot sync worker? Normal exit or FATAL exit can be
		 * ignored (FATAL can be caused by libpqwalreceiver on receiving
		 * shutdown request by the startup process during promotion); we'll
		 * start a new one at the next iteration of the postmaster's main
		 * loop, if necessary. Any other exit condition is treated as a crash.
		 */
		if (SlotSyncWorkerPMChild && pid == SlotSyncWorkerPMChild->pid)
		{
			(void) cleanup_slot_sync_worker_child(SlotSyncWorkerPMChild,
												  exitstatus, pid);
			continue;
		}

		/* Was it an IO worker? */
		if (maybe_reap_io_worker(pid))
		{
			if (!EXIT_STATUS_0(exitstatus) && !EXIT_STATUS_1(exitstatus))
				HandleChildCrash(pid, exitstatus, _("io worker"));

			/*
			 * A worker that exited with an error might have brought the pool
			 * size below io_min_workers, or allowed the queue to grow to the
			 * point where another worker called for growth.
			 *
			 * In the common case that a worker timed out due to idleness, no
			 * replacement needs to be started.  maybe_start_io_workers() will
			 * figure that out.
			 */
			maybe_start_io_workers();

			continue;
		}

		/*
		 * Was it a backend or a background worker?
		 */
		pmchild = FindPostmasterChildByPid(pid);
		if (pmchild)
		{
			CleanupBackend(pmchild, exitstatus);
		}

		/*
		 * We don't know anything about this child process.  That's highly
		 * unexpected, as we do track all the child processes that we fork.
		 */
		else
		{
			if (!EXIT_STATUS_0(exitstatus) && !EXIT_STATUS_1(exitstatus))
				HandleChildCrash(pid, exitstatus, _("untracked child process"));
			else
				LogChildExit(LOG, _("untracked child process"), pid, exitstatus);
		}
	}							/* loop over pending child-death reports */

	/*
	 * After cleaning out the SIGCHLD queue, see if we have any state changes
	 * or actions to make.
	 */
	PostmasterStateMachine();
}

/*
 * Reap one child by backend type: dispatch aux/maintenance families to their
 * per-type cleanup_*_child() helper (which clears the family's global PMChild
 * pointer -- WalWriterPMChild, CheckpointerPMChild, etc.), else fall back to
 * CleanupBackend() for regular backends and background workers.
 *
 * Shared by process_pm_thread_exit() (dedicated-thread carriers) and
 * process_pm_pooled_logical_exit() (fiber / pooled-logical carriers): an aux
 * worker that runs as a fiber is reaped through the pooled-logical path, and it
 * MUST still run its per-type cleanup or the postmaster's global aux pointer is
 * left dangling and PostmasterStateMachine trips e.g.
 * Assert(WalWriterPMChild == NULL) at PM_WAIT_DEAD_END.
 */
static void
reap_aux_or_backend_child(PMChild *pmchild, int exitstatus)
{
	switch (pmchild->bkend_type)
	{
		case B_STARTUP:
			(void) cleanup_startup_child(pmchild, exitstatus, 0);
			return;
		case B_ARCHIVER:
			(void) cleanup_archiver_child(pmchild, exitstatus);
			return;
		case B_AUTOVAC_LAUNCHER:
			(void) cleanup_autovac_launcher_child(pmchild, exitstatus);
			return;
		case B_BG_WRITER:
			(void) cleanup_bgwriter_child(pmchild, exitstatus, 0);
			return;
		case B_CHECKPOINTER:
			(void) cleanup_checkpointer_child(pmchild, exitstatus, 0);
			return;
		case B_IO_WORKER:
			(void) cleanup_io_worker_child(pmchild);
			return;
		case B_LOGGER:
			syslogger_thread_handoff_pending = false;
			ReleasePostmasterChildSlot(pmchild);
			SysLoggerPMChild = NULL;

			/* for safety's sake, launch new logger *first* */
			if (Logging_collector)
				StartSysLogger();

			if (!EXIT_STATUS_0(exitstatus))
				LogChildExit(LOG, _("system logger process"),
							 0, exitstatus);
			return;
		case B_SLOTSYNC_WORKER:
			(void) cleanup_slot_sync_worker_child(pmchild, exitstatus, 0);
			return;
		case B_WAL_WRITER:
			(void) cleanup_wal_writer_child(pmchild, exitstatus);
			return;
		case B_WAL_RECEIVER:
			(void) cleanup_wal_receiver_child(pmchild, exitstatus, 0);
			return;
		case B_WAL_SUMMARIZER:
			(void) cleanup_wal_summarizer_child(pmchild, exitstatus);
			return;
		default:
			CleanupBackend(pmchild, exitstatus);
			return;
	}
}

/*
 * Cleanup after a pooled logical backend reports exit.  The carrier thread
 * that ran the session remains alive; the postmaster owns PMChild list
 * mutation and slot release just like process and thread-backed children.
 */
static void
process_pm_pooled_logical_exit(void)
{
	dlist_mutable_iter iter;
	bool		reaped = false;

	dlist_foreach_modify(iter, &ActiveChildList)
	{
		PMChild    *pmchild = dlist_container(PMChild, elem, iter.cur);
		int			exitstatus;
		pid_t		signal_pid;
		Size		top_memory_allocated;
		Size		top_memory_reclaimed;

		if (!PostmasterChildHasExitedPooledLogical(pmchild, &exitstatus,
												  &top_memory_allocated,
												  &top_memory_reclaimed,
												  &signal_pid))
			continue;

		if (top_memory_allocated > 0)
			ereport(WARNING,
					(errmsg_internal("pooled logical backend %d retained %zu bytes in TopMemoryContext at exit",
									 signal_pid, top_memory_allocated)));
		if (top_memory_reclaimed > 0)
			ereport(DEBUG1,
					(errmsg_internal("pooled logical backend %d reclaimed %zu bytes from TopMemoryContext at exit",
									 signal_pid, top_memory_reclaimed)));

		reap_aux_or_backend_child(pmchild, exitstatus);
		reaped = true;
	}

	if (reaped)
		PostmasterStateMachine();
}

/*
 * Cleanup after any thread-backed child reports exit.  The exiting thread only
 * marks its PMChild entry and wakes the postmaster latch; list mutation and
 * PMChild slot release stay in the postmaster main thread.
 */
static void
process_pm_thread_exit(void)
{
	dlist_mutable_iter iter;
	bool		reaped = false;

	dlist_foreach_modify(iter, &ActiveChildList)
	{
		PMChild    *pmchild = dlist_container(PMChild, elem, iter.cur);
		int			exitstatus;
		int			join_rc;
		pid_t		signal_pid;
		Size		top_memory_allocated;
		Size		top_memory_reclaimed;

		if (!PostmasterChildHasExitedThread(pmchild, &exitstatus,
											&top_memory_allocated,
											&top_memory_reclaimed,
											&signal_pid))
			continue;

		if (top_memory_allocated > 0)
			ereport(WARNING,
					(errmsg_internal("thread-backed child %d retained %zu bytes in TopMemoryContext at exit",
									 signal_pid, top_memory_allocated)));
		if (top_memory_reclaimed > 0)
			ereport(DEBUG1,
					(errmsg_internal("thread-backed child %d reclaimed %zu bytes from TopMemoryContext at exit",
									 signal_pid, top_memory_reclaimed)));

		join_rc = PostmasterChildJoinThread(pmchild);
		if (join_rc != 0)
		{
			errno = join_rc;
			elog(LOG, "could not join thread-backed child %d: %m",
				 signal_pid);
			PostmasterChildRetryThreadExit(pmchild);
			continue;
		}
		reap_aux_or_backend_child(pmchild, exitstatus);
		reaped = true;
	}

	if (reaped)
		PostmasterStateMachine();
}

/*
 * Complete postmaster-visible startup for thread-backed children that publish
 * an explicit startup boundary after carrier creation.
 */
static void
process_pm_thread_startup_complete(void)
{
	dlist_iter	iter;

	dlist_foreach(iter, &ActiveChildList)
	{
		PMChild    *pmchild = dlist_container(PMChild, elem, iter.cur);

		if (!PostmasterChildHasStartupComplete(pmchild))
			continue;

		if (pmchild->bkend_type == B_BG_WORKER && pmchild->rw != NULL)
			ReportBackgroundWorkerPID(pmchild->rw);
	}
}

/*
 * CleanupBackend -- cleanup after terminated backend or background worker.
 *
 * Remove all local state associated with the child process and release its
 * PMChild slot.
 */
static void
CleanupBackend(PMChild *bp,
			   int exitstatus)	/* child's exit status. */
{
	char		namebuf[MAXPGPATH];
	const char *procname;
	bool		crashed = false;
	bool		logged = false;
	pid_t		bp_pid;
	bool		bp_bgworker_notify;
	BackendType bp_bkend_type;
	RegisteredBgWorker *rw;

	/* Construct a process name for the log message */
	if (bp->bkend_type == B_BG_WORKER)
	{
		snprintf(namebuf, MAXPGPATH, _("background worker \"%s\""),
				 bp->rw->rw_worker.bgw_type);
		procname = namebuf;
	}
	else
		procname = _(GetBackendTypeDesc(bp->bkend_type));

	/*
	 * If a backend dies in an ugly way then we must signal all other backends
	 * to quickdie.  If exit status is zero (normal) or one (FATAL exit), we
	 * assume everything is all right and proceed to remove the backend from
	 * the active child list.
	 */
	if (!EXIT_STATUS_0(exitstatus) && !EXIT_STATUS_1(exitstatus))
		crashed = true;

#ifdef WIN32

	/*
	 * On win32, also treat ERROR_WAIT_NO_CHILDREN (128) as nonfatal case,
	 * since that sometimes happens under load when the process fails to start
	 * properly (long before it starts using shared memory). Microsoft reports
	 * it is related to mutex failure:
	 * http://archives.postgresql.org/pgsql-hackers/2010-09/msg00790.php
	 */
	if (exitstatus == ERROR_WAIT_NO_CHILDREN)
	{
		LogChildExit(LOG, procname, bp->pid, exitstatus);
		logged = true;
		crashed = false;
	}
#endif

	/*
	 * Release the PMChild entry.
	 *
	 * If the process attached to shared memory, this also checks that it
	 * detached cleanly.
	 */
	bp_pid = PostmasterChildSignalPid(bp);
	bp_bgworker_notify = bp->bgworker_notify;
	bp_bkend_type = bp->bkend_type;
	rw = bp->rw;
	if (!ReleasePostmasterChildSlot(bp))
	{
		/*
		 * Uh-oh, the child failed to clean itself up.  Treat as a crash after
		 * all.
		 */
		crashed = true;
	}
	bp = NULL;

	/*
	 * In a crash case, exit immediately without resetting background worker
	 * state. However, if restart_after_crash is enabled, the background
	 * worker state (e.g., rw_pid) still needs be reset so the worker can
	 * restart after crash recovery. This reset is handled in
	 * ResetBackgroundWorkerCrashTimes(), not here.
	 */
	if (crashed)
	{
		HandleChildCrash(bp_pid, exitstatus, procname);
		return;
	}

	/*
	 * This backend may have been slated to receive SIGUSR1 when some
	 * background worker started or stopped.  Cancel those notifications, as
	 * we don't want to signal PIDs that are not PostgreSQL backends.  This
	 * gets skipped in the (probably very common) case where the backend has
	 * never requested any such notifications.
	 */
	if (bp_bgworker_notify)
		BackgroundWorkerStopNotifications(bp_pid);

	/*
	 * If it was an autovacuum worker, wake up the launcher so that it can
	 * immediately launch a new worker or rebalance to cost limit setting of
	 * the remaining workers.
	 */
	if (bp_bkend_type == B_AUTOVAC_WORKER && AutoVacLauncherPMChild != NULL)
		signal_child(AutoVacLauncherPMChild, SIGUSR2);

	/*
	 * If it was a background worker, also update its RegisteredBgWorker
	 * entry.
	 */
	if (bp_bkend_type == B_BG_WORKER)
	{
		if (!EXIT_STATUS_0(exitstatus))
		{
			/* Record timestamp, so we know when to restart the worker. */
			rw->rw_crashed_at = GetCurrentTimestamp();
		}
		else
		{
			/* Zero exit status means terminate */
			rw->rw_crashed_at = 0;
			rw->rw_terminate = true;
		}

		rw->rw_pid = 0;
		ReportBackgroundWorkerExit(rw); /* report child death */

		if (!logged)
		{
			LogChildExit(EXIT_STATUS_0(exitstatus) ? DEBUG1 : LOG,
						 procname, bp_pid, exitstatus);
			logged = true;
		}

		/* have it be restarted */
		HaveCrashedWorker = true;
	}

	if (!logged)
		LogChildExit(DEBUG2, procname, bp_pid, exitstatus);
}

/*
 * Transition into FatalError state, in response to something bad having
 * happened. Commonly the caller will have logged the reason for entering
 * FatalError state.
 *
 * This should only be called when not already in FatalError or
 * ImmediateShutdown state.
 */
static void
HandleFatalError(QuitSignalReason reason, bool consider_sigabrt)
{
	int			sigtosend;

	Assert(!FatalError);
	Assert(Shutdown != ImmediateShutdown);

	SetQuitSignalReason(reason);

	if (consider_sigabrt && send_abort_for_crash)
		sigtosend = SIGABRT;
	else
		sigtosend = SIGQUIT;

	/*
	 * Signal all other child processes to exit.
	 *
	 * We could exclude dead-end children here, but at least when sending
	 * SIGABRT it seems better to include them.
	 */
	TerminateChildren(sigtosend);

	FatalError = true;

	/*
	 * Choose the appropriate new state to react to the fatal error. Unless we
	 * were already in the process of shutting down, we go through
	 * PM_WAIT_BACKENDS. For errors during the shutdown sequence, we directly
	 * switch to PM_WAIT_DEAD_END.
	 */
	switch (pmState)
	{
		case PM_INIT:
			/* shouldn't have any children */
			Assert(false);
			break;

			/* wait for children to die */
		case PM_STARTUP:
		case PM_RECOVERY:
		case PM_HOT_STANDBY:
		case PM_RUN:
		case PM_STOP_BACKENDS:
			UpdatePMState(PM_WAIT_BACKENDS);
			break;

		case PM_WAIT_BACKENDS:
			/* there might be more backends to wait for */
			break;

		case PM_WAIT_XLOG_SHUTDOWN:
		case PM_WAIT_XLOG_ARCHIVAL:
		case PM_WAIT_CHECKPOINTER:
		case PM_WAIT_IO_WORKERS:

			/*
			 * NB: Similar code exists in PostmasterStateMachine()'s handling
			 * of FatalError in PM_STOP_BACKENDS/PM_WAIT_BACKENDS states.
			 */
			ConfigurePostmasterWaitSet(false);
			UpdatePMState(PM_WAIT_DEAD_END);
			break;

		case PM_WAIT_DEAD_END:
		case PM_NO_CHILDREN:
			break;
	}

	/*
	 * .. and if this doesn't happen quickly enough, now the clock is ticking
	 * for us to kill them without mercy.
	 */
	if (AbortStartTime == 0)
		AbortStartTime = time(NULL);
}

/*
 * HandleChildCrash -- cleanup after failed backend, bgwriter, checkpointer,
 * walwriter, autovacuum, archiver, slot sync worker, or background worker.
 *
 * The objectives here are to clean up our local state about the child
 * process, and to signal all other remaining children to quickdie.
 *
 * The caller has already released its PMChild slot.
 */
static void
HandleChildCrash(int pid, int exitstatus, const char *procname)
{
	/*
	 * We only log messages and send signals if this is the first process
	 * crash and we're not doing an immediate shutdown; otherwise, we're only
	 * here to update postmaster's idea of live processes.  If we have already
	 * signaled children, nonzero exit status is to be expected, so don't
	 * clutter log.
	 */
	if (FatalError || Shutdown == ImmediateShutdown)
		return;

	LogChildExit(LOG, procname, pid, exitstatus);

	/*
	 * Once threaded carriers exist, a child crash means the postmaster's own
	 * address space may be compromised.  The process-mode crash-recovery path
	 * waits for sibling processes to exit and then reinitializes shared state,
	 * but it cannot safely recover from a failed thread inside this process.
	 * Terminate the runtime and let the outside supervisor/user restart a
	 * clean postmaster.
	 */
	if (multithreaded && PostmasterThreadCarriersStarted())
	{
		ereport(LOG,
				(errmsg("terminating threaded server runtime after child crash")));
		ExitPostmaster(1);
	}

	ereport(LOG,
			(errmsg("terminating any other active server processes")));

	/*
	 * Switch into error state. The crashed process has already been removed
	 * from ActiveChildList.
	 */
	HandleFatalError(PMQUIT_FOR_CRASH, true);
}

/*
 * Log the death of a child process.
 */
static void
LogChildExit(int lev, const char *procname, int pid, int exitstatus)
{
	/*
	 * size of activity_buffer is arbitrary, but set equal to default
	 * track_activity_query_size
	 */
	char		activity_buffer[1024];
	const char *activity = NULL;

	if (!EXIT_STATUS_0(exitstatus))
		activity = pgstat_get_crashed_backend_activity(pid,
													   activity_buffer,
													   sizeof(activity_buffer));

	if (WIFEXITED(exitstatus))
		ereport(lev,

		/*------
		  translator: %s is a noun phrase describing a child process, such as
		  "server process" */
				(errmsg("%s (PID %d) exited with exit code %d",
						procname, pid, WEXITSTATUS(exitstatus)),
				 activity ? errdetail("Failed process was running: %s", activity) : 0));
	else if (WIFSIGNALED(exitstatus))
	{
#if defined(WIN32)
		ereport(lev,

		/*------
		  translator: %s is a noun phrase describing a child process, such as
		  "server process" */
				(errmsg("%s (PID %d) was terminated by exception 0x%X",
						procname, pid, WTERMSIG(exitstatus)),
				 errhint("See C include file \"ntstatus.h\" for a description of the hexadecimal value."),
				 activity ? errdetail("Failed process was running: %s", activity) : 0));
#else
		ereport(lev,

		/*------
		  translator: %s is a noun phrase describing a child process, such as
		  "server process" */
				(errmsg("%s (PID %d) was terminated by signal %d: %s",
						procname, pid, WTERMSIG(exitstatus),
						pg_strsignal(WTERMSIG(exitstatus))),
				 activity ? errdetail("Failed process was running: %s", activity) : 0));
#endif
	}
	else
		ereport(lev,

		/*------
		  translator: %s is a noun phrase describing a child process, such as
		  "server process" */
				(errmsg("%s (PID %d) exited with unrecognized status %d",
						procname, pid, exitstatus),
				 activity ? errdetail("Failed process was running: %s", activity) : 0));
}

/*
 * Advance the postmaster's state machine and take actions as appropriate
 *
 * This is common code for process_pm_shutdown_request(),
 * process_pm_child_exit() and process_pm_pmsignal(), which process the signals
 * that might mean we need to change state.
 */
static void
PostmasterStateMachine(void)
{
	/* If we're doing a smart shutdown, try to advance that state. */
	if (pmState == PM_RUN || pmState == PM_HOT_STANDBY)
	{
		if (!connsAllowed)
		{
			/*
			 * This state ends when we have no normal client backends running.
			 * Then we're ready to stop other children.
			 */
			if (CountChildren(btmask(B_BACKEND)) == 0)
				UpdatePMState(PM_STOP_BACKENDS);
		}
	}

	/*
	 * In the PM_WAIT_BACKENDS state, wait for all the regular backends and
	 * processes like autovacuum and background workers that are comparable to
	 * backends to exit.
	 *
	 * PM_STOP_BACKENDS is a transient state that means the same as
	 * PM_WAIT_BACKENDS, but we signal the processes first, before waiting for
	 * them.  Treating it as a distinct pmState allows us to share this code
	 * across multiple shutdown code paths.
	 */
	if (pmState == PM_STOP_BACKENDS || pmState == PM_WAIT_BACKENDS)
	{
		BackendTypeMask targetMask = BTYPE_MASK_NONE;

		/*
		 * PM_WAIT_BACKENDS state ends when we have no regular backends, no
		 * autovac launcher or workers, and no bgworkers (including
		 * unconnected ones).
		 */
		targetMask = btmask_add(targetMask,
								B_BACKEND,
								B_AUTOVAC_LAUNCHER,
								B_AUTOVAC_WORKER,
								B_BG_WORKER);

		/*
		 * No walwriter, bgwriter, slot sync worker, or WAL summarizer either.
		 */
		targetMask = btmask_add(targetMask,
								B_WAL_WRITER,
								B_BG_WRITER,
								B_SLOTSYNC_WORKER,
								B_WAL_SUMMARIZER);

		/* If we're in recovery, also stop startup and walreceiver procs */
		targetMask = btmask_add(targetMask,
								B_STARTUP,
								B_WAL_RECEIVER);

		/*
		 * If we are doing crash recovery or an immediate shutdown then we
		 * expect archiver, checkpointer, io workers and walsender to exit as
		 * well, otherwise not.
		 */
		if (FatalError || Shutdown >= ImmediateShutdown)
			targetMask = btmask_add(targetMask,
									B_CHECKPOINTER,
									B_ARCHIVER,
									B_IO_WORKER,
									B_WAL_SENDER);

		/*
		 * Normally archiver, checkpointer, IO workers and walsenders will
		 * continue running; they will be terminated later after writing the
		 * checkpoint record.  We also let dead-end children to keep running
		 * for now.  The syslogger process exits last.
		 *
		 * This assertion checks that we have covered all backend types,
		 * either by including them in targetMask, or by noting here that they
		 * are allowed to continue running.
		 */
#ifdef USE_ASSERT_CHECKING
		{
			BackendTypeMask remainMask = BTYPE_MASK_NONE;

			remainMask = btmask_add(remainMask,
									B_DEAD_END_BACKEND,
									B_LOGGER);

			/*
			 * Archiver, checkpointer, IO workers, and walsender may or may
			 * not be in targetMask already.
			 */
			remainMask = btmask_add(remainMask,
									B_ARCHIVER,
									B_CHECKPOINTER,
									B_IO_WORKER,
									B_WAL_SENDER);

			/* these are not real postmaster children */
			remainMask = btmask_add(remainMask,
									B_INVALID,
									B_STANDALONE_BACKEND);

			/* also add data checksums processes */
			remainMask = btmask_add(remainMask,
									B_DATACHECKSUMSWORKER_LAUNCHER,
									B_DATACHECKSUMSWORKER_WORKER);

			/* All types should be included in targetMask or remainMask */
			Assert((remainMask.mask | targetMask.mask) == BTYPE_MASK_ALL.mask);
		}
#endif

		/* If we had not yet signaled the processes to exit, do so now */
		if (pmState == PM_STOP_BACKENDS)
		{
			/*
			 * Forget any pending requests for background workers, since we're
			 * no longer willing to launch any new workers.  (If additional
			 * requests arrive, BackgroundWorkerStateChange will reject them.)
			 */
			ForgetUnstartedBackgroundWorkers();

			SignalChildren(SIGTERM, targetMask);

			UpdatePMState(PM_WAIT_BACKENDS);
		}

#ifdef USE_XTC_CARRIER
		/*
		 * Reap any fiber-backed autovac worker that was launched but whose
		 * fiber was never scheduled to run (see ReapOrphanedThreadedWorker).
		 * The launcher's worker-start-timeout cancel normally triggers this,
		 * but the LAST worker launched before shutdown can be orphaned without
		 * a cancel (shutdown pre-empts the launcher's timeout).  Such an
		 * orphan will never publish an exit, so drain it here before checking
		 * whether any target children remain -- otherwise PM_WAIT_BACKENDS
		 * wedges forever waiting on a fiber that will never run.  A no-op in
		 * process mode and when there is no orphan; the fiber_entered guard
		 * means a worker whose fiber has started is never touched.
		 */
		while (ReapOrphanedThreadedWorker(B_AUTOVAC_WORKER, 0))
				/* reap all currently-orphaned worker fibers */ ;
#endif

		/* Are any of the target processes still running? */
		if (CountChildren(targetMask) == 0)
		{
			if (Shutdown >= ImmediateShutdown || FatalError)
			{
				/*
				 * Stop any dead-end children and stop creating new ones.
				 *
				 * NB: Similar code exists in HandleFatalError(), when the
				 * error happens in pmState > PM_WAIT_BACKENDS.
				 */
				UpdatePMState(PM_WAIT_DEAD_END);
				ConfigurePostmasterWaitSet(false);
				SignalChildren(SIGQUIT, btmask(B_DEAD_END_BACKEND));

				/*
				 * We already SIGQUIT'd auxiliary processes (other than
				 * logger), if any, when we started immediate shutdown or
				 * entered FatalError state.
				 */
			}
			else
			{
				/*
				 * If we get here, we are proceeding with normal shutdown. All
				 * the regular children are gone, and it's time to tell the
				 * checkpointer to do a shutdown checkpoint.
				 */
				Assert(Shutdown > NoShutdown);
				/* Start the checkpointer if not running */
				if (CheckpointerPMChild == NULL)
					CheckpointerPMChild = StartChildProcess(B_CHECKPOINTER);
				/* And tell it to write the shutdown checkpoint */
				if (CheckpointerPMChild != NULL)
				{
					signal_child(CheckpointerPMChild, SIGINT);
					UpdatePMState(PM_WAIT_XLOG_SHUTDOWN);
				}
				else
				{
					/*
					 * If we failed to fork a checkpointer, just shut down.
					 * Any required cleanup will happen at next restart. We
					 * set FatalError so that an "abnormal shutdown" message
					 * gets logged when we exit.
					 *
					 * We don't consult send_abort_for_crash here, as it's
					 * unlikely that dumping cores would illuminate the reason
					 * for checkpointer fork failure.
					 *
					 * XXX: It may be worth to introduce a different PMQUIT
					 * value that signals that the cluster is in a bad state,
					 * without a process having crashed. But right now this
					 * path is very unlikely to be reached, so it isn't
					 * obviously worthwhile adding a distinct error message in
					 * quickdie().
					 */
					HandleFatalError(PMQUIT_FOR_CRASH, false);
				}
			}
		}
	}

	/*
	 * The state transition from PM_WAIT_XLOG_SHUTDOWN to
	 * PM_WAIT_XLOG_ARCHIVAL is in process_pm_pmsignal(), in response to
	 * PMSIGNAL_XLOG_IS_SHUTDOWN.
	 */

	if (pmState == PM_WAIT_XLOG_ARCHIVAL)
	{
		/*
		 * PM_WAIT_XLOG_ARCHIVAL state ends when there are no children other
		 * than checkpointer, io workers and dead-end children left. There
		 * shouldn't be any regular backends left by now anyway; what we're
		 * really waiting for is for walsenders and archiver to exit.
		 */
		if (CountChildren(btmask_all_except(B_CHECKPOINTER, B_IO_WORKER,
											B_LOGGER, B_DEAD_END_BACKEND)) == 0)
		{
			UpdatePMState(PM_WAIT_IO_WORKERS);
			SignalChildren(SIGUSR2, btmask(B_IO_WORKER));
		}
	}

	if (pmState == PM_WAIT_IO_WORKERS)
	{
		/*
		 * PM_WAIT_IO_WORKERS state ends when there's only checkpointer and
		 * dead-end children left.
		 */
		if (io_worker_count == 0)
		{
			UpdatePMState(PM_WAIT_CHECKPOINTER);

			/*
			 * Now that the processes mentioned above are gone, tell
			 * checkpointer to shut down too. That allows checkpointer to
			 * perform some last bits of cleanup without other processes
			 * interfering.
			 */
			if (CheckpointerPMChild != NULL)
				signal_child(CheckpointerPMChild, SIGUSR2);
		}
	}

	/*
	 * The state transition from PM_WAIT_CHECKPOINTER to PM_WAIT_DEAD_END is
	 * in process_pm_child_exit().
	 */

	if (pmState == PM_WAIT_DEAD_END)
	{
		/*
		 * PM_WAIT_DEAD_END state ends when all other children are gone except
		 * for the logger.  During normal shutdown, all that remains are
		 * dead-end backends, but in FatalError processing we jump straight
		 * here with more processes remaining.  Note that they have already
		 * been sent appropriate shutdown signals, either during a normal
		 * state transition leading up to PM_WAIT_DEAD_END, or during
		 * FatalError processing.
		 *
		 * The reason we wait is to protect against a new postmaster starting
		 * conflicting subprocesses; this isn't an ironclad protection, but it
		 * at least helps in the shutdown-and-immediately-restart scenario.
		 */
		if (CountChildren(btmask_all_except(B_LOGGER)) == 0)
		{
			/* These other guys should be dead already */
			Assert(StartupPMChild == NULL);
			Assert(WalReceiverPMChild == NULL);
			Assert(WalSummarizerPMChild == NULL);
			Assert(BgWriterPMChild == NULL);
			Assert(CheckpointerPMChild == NULL);
			Assert(WalWriterPMChild == NULL);
			Assert(AutoVacLauncherPMChild == NULL);
			Assert(SlotSyncWorkerPMChild == NULL);
			/* syslogger is not considered here */
			UpdatePMState(PM_NO_CHILDREN);
		}
	}

	/*
	 * If we've been told to shut down, we exit as soon as there are no
	 * remaining children.  If there was a crash, cleanup will occur at the
	 * next startup.  (Before PostgreSQL 8.3, we tried to recover from the
	 * crash before exiting, but that seems unwise if we are quitting because
	 * we got SIGTERM from init --- there may well not be time for recovery
	 * before init decides to SIGKILL us.)
	 *
	 * Note that the syslogger continues to run.  It will exit when it sees
	 * EOF on its input pipe, which happens when there are no more upstream
	 * processes.
	 */
	if (Shutdown > NoShutdown && pmState == PM_NO_CHILDREN)
	{
		if (FatalError)
		{
			ereport(LOG, (errmsg("abnormal database system shutdown")));
			ExitPostmaster(1);
		}
		else
		{
			/*
			 * Normal exit from the postmaster is here.  We don't need to log
			 * anything here, since the UnlinkLockFiles proc_exit callback
			 * will do so, and that should be the last user-visible action.
			 */
			ExitPostmaster(0);
		}
	}

	/*
	 * If the startup process failed, or the user does not want an automatic
	 * restart after backend crashes, wait for all non-syslogger children to
	 * exit, and then exit postmaster.  We don't try to reinitialize when the
	 * startup process fails, because more than likely it will just fail again
	 * and we will keep trying forever.
	 */
	if (pmState == PM_NO_CHILDREN)
	{
		if (StartupStatus == STARTUP_CRASHED)
		{
			ereport(LOG,
					(errmsg("shutting down due to startup process failure")));
			ExitPostmaster(1);
		}
		if (!restart_after_crash)
		{
			ereport(LOG,
					(errmsg("shutting down because \"restart_after_crash\" is off")));
			ExitPostmaster(1);
		}
	}

	/*
	 * If we need to recover from a crash, wait for all non-syslogger children
	 * to exit, then reset shmem and start the startup process.
	 */
	if (FatalError && pmState == PM_NO_CHILDREN)
	{
		ereport(LOG,
				(errmsg("all server processes terminated; reinitializing")));

		/* remove leftover temporary files after a crash */
		if (remove_temp_files_after_crash)
			RemovePgTempFiles();

		/* allow background workers to immediately restart */
		ResetBackgroundWorkerCrashTimes();

		shmem_exit(1);

		/* re-read control file into local memory */
		LocalProcessControlFile(true);

		/*
		 * Re-initialize shared memory and semaphores.  Note: We don't call
		 * RegisterBuiltinShmemCallbacks(), we keep the old registrations.  In
		 * order to re-register structs in extensions, we'd need to reload
		 * shared preload libraries, and we don't want to do that.
		 */
		ResetShmemAllocator();
		ShmemCallRequestCallbacks();
		CreateSharedMemoryAndSemaphores();

		UpdatePMState(PM_STARTUP);

		/* Make sure we can perform I/O while starting up. */
		maybe_start_io_workers();

		StartupPMChild = StartChildProcess(B_STARTUP);
		Assert(StartupPMChild != NULL);
		StartupStatus = STARTUP_RUNNING;
		/* crash recovery started, reset SIGKILL flag */
		AbortStartTime = 0;

		/* start accepting server socket connection events again */
		ConfigurePostmasterWaitSet(true);
	}
}

static const char *
pmstate_name(PMState state)
{
#define PM_TOSTR_CASE(sym) case sym: return #sym
	switch (state)
	{
			PM_TOSTR_CASE(PM_INIT);
			PM_TOSTR_CASE(PM_STARTUP);
			PM_TOSTR_CASE(PM_RECOVERY);
			PM_TOSTR_CASE(PM_HOT_STANDBY);
			PM_TOSTR_CASE(PM_RUN);
			PM_TOSTR_CASE(PM_STOP_BACKENDS);
			PM_TOSTR_CASE(PM_WAIT_BACKENDS);
			PM_TOSTR_CASE(PM_WAIT_XLOG_SHUTDOWN);
			PM_TOSTR_CASE(PM_WAIT_XLOG_ARCHIVAL);
			PM_TOSTR_CASE(PM_WAIT_IO_WORKERS);
			PM_TOSTR_CASE(PM_WAIT_DEAD_END);
			PM_TOSTR_CASE(PM_WAIT_CHECKPOINTER);
			PM_TOSTR_CASE(PM_NO_CHILDREN);
	}
#undef PM_TOSTR_CASE

	pg_unreachable();
	return "";					/* silence compiler */
}

/*
 * Simple wrapper for updating pmState. The main reason to have this wrapper
 * is that it makes it easy to log all state transitions.
 */
static void
UpdatePMState(PMState newState)
{
	elog(DEBUG1, "updating PMState from %s to %s",
		 pmstate_name(pmState), pmstate_name(newState));
	pmState = newState;
}

/*
 * Launch background processes after state change, or relaunch after an
 * existing process has exited.
 *
 * Check the current pmState and the status of any background processes.  If
 * there are any background processes missing that should be running in the
 * current state, but are not, launch them.
 */
static void
LaunchMissingBackgroundProcesses(void)
{
	/* Syslogger is active in all states */
	if (SysLoggerPMChild == NULL && Logging_collector)
		StartSysLogger();
	maybe_handoff_syslogger();

	/*
	 * The number of configured workers might have changed, or a prior start
	 * of a worker might have failed. Check if we need to start/stop any
	 * workers.
	 *
	 * A config file change will always lead to this function being called, so
	 * we always will process the config change in a timely manner.
	 */
	maybe_handoff_io_workers();
	maybe_start_io_workers();

	/*
	 * The checkpointer and the background writer are active from the start,
	 * until shutdown is initiated.
	 *
	 * (If the checkpointer is not running when we enter the
	 * PM_WAIT_XLOG_SHUTDOWN state, it is launched one more time to perform
	 * the shutdown checkpoint.  That's done in PostmasterStateMachine(), not
	 * here.)
	 */
	if (pmState == PM_RUN || pmState == PM_RECOVERY ||
		pmState == PM_HOT_STANDBY || pmState == PM_STARTUP)
	{
		if (multithreaded && pmState == PM_RUN &&
			PostmasterThreadCarriersStarted())
		{
			if (CheckpointerPMChild != NULL &&
				PostmasterChildIsProcess(CheckpointerPMChild) &&
				!checkpointer_thread_handoff_pending)
			{
				checkpointer_thread_handoff_pending = true;
				signal_child(CheckpointerPMChild, SIGUSR2);
			}

			if (BgWriterPMChild != NULL &&
				PostmasterChildIsProcess(BgWriterPMChild) &&
				!bgwriter_thread_handoff_pending)
			{
				bgwriter_thread_handoff_pending = true;
				signal_child(BgWriterPMChild, SIGTERM);
			}
		}

		if (CheckpointerPMChild == NULL)
			CheckpointerPMChild = StartChildProcess(B_CHECKPOINTER);
		if (BgWriterPMChild == NULL)
			BgWriterPMChild = StartChildProcess(B_BG_WRITER);
	}

	/*
	 * WAL writer is needed only in normal operation (else we cannot be
	 * writing any new WAL).
	 */
	if (WalWriterPMChild == NULL && pmState == PM_RUN)
		WalWriterPMChild = StartChildProcess(B_WAL_WRITER);

	/*
	 * We don't want autovacuum to run in binary upgrade mode because
	 * autovacuum might update relfrozenxid for empty tables before the
	 * physical files are put in place.
	 */
	if (!IsBinaryUpgrade && AutoVacLauncherPMChild == NULL &&
		(AutoVacuumingActive() || start_autovac_launcher) &&
		pmState == PM_RUN)
	{
		AutoVacLauncherPMChild = StartChildProcess(B_AUTOVAC_LAUNCHER);
		if (AutoVacLauncherPMChild != NULL)
			start_autovac_launcher = false; /* signal processed */
	}

	/*
	 * If WAL archiving is enabled always, we are allowed to start archiver
	 * even during recovery.
	 */
	if (PgArchPMChild == NULL &&
		((XLogArchivingActive() && pmState == PM_RUN) ||
		 (XLogArchivingAlways() && (pmState == PM_RECOVERY || pmState == PM_HOT_STANDBY))) &&
		PgArchCanRestart())
		PgArchPMChild = StartChildProcess(B_ARCHIVER);

	/*
	 * If we need to start a slot sync worker, try to do that now
	 *
	 * We allow to start the slot sync worker when we are on a hot standby,
	 * fast or immediate shutdown is not in progress, slot sync parameters are
	 * configured correctly, and it is the first time of worker's launch, or
	 * enough time has passed since the worker was launched last.
	 */
	if (SlotSyncWorkerPMChild == NULL && pmState == PM_HOT_STANDBY &&
		Shutdown <= SmartShutdown && sync_replication_slots &&
		ValidateSlotSyncParams(LOG) && SlotSyncWorkerCanRestart())
		SlotSyncWorkerPMChild = StartChildProcess(B_SLOTSYNC_WORKER);

	/*
	 * If we need to start a WAL receiver, try to do that now
	 *
	 * Note: if a walreceiver process is already running, it might seem that
	 * we should clear WalReceiverRequested.  However, there's a race
	 * condition if the walreceiver terminates and the startup process
	 * immediately requests a new one: it's quite possible to get the signal
	 * for the request before reaping the dead walreceiver process.  Better to
	 * risk launching an extra walreceiver than to miss launching one we need.
	 * (The walreceiver code has logic to recognize that it should go away if
	 * not needed.)
	 */
	if (WalReceiverRequested)
	{
		if (WalReceiverPMChild == NULL &&
			(pmState == PM_STARTUP || pmState == PM_RECOVERY ||
			 pmState == PM_HOT_STANDBY) &&
			Shutdown <= SmartShutdown)
		{
			WalReceiverPMChild = StartChildProcess(B_WAL_RECEIVER);
			if (WalReceiverPMChild != NULL)
				WalReceiverRequested = false;
			/* else leave the flag set, so we'll try again later */
		}
	}

	/* If we need to start a WAL summarizer, try to do that now */
	if (summarize_wal && WalSummarizerPMChild == NULL &&
		(pmState == PM_RUN || pmState == PM_HOT_STANDBY) &&
		Shutdown <= SmartShutdown)
		WalSummarizerPMChild = StartChildProcess(B_WAL_SUMMARIZER);

	/* Get other worker processes running, if needed */
	if (StartWorkerNeeded || HaveCrashedWorker)
		maybe_start_bgworkers();
}

/*
 * Return string representation of signal.
 *
 * Because this is only implemented for signals we already rely on in this
 * file we don't need to deal with unimplemented or same-numeric-value signals
 * (as we'd e.g. have to for EWOULDBLOCK / EAGAIN).
 */
static const char *
pm_signame(int signal)
{
#define PM_TOSTR_CASE(sym) case sym: return #sym
	switch (signal)
	{
			PM_TOSTR_CASE(SIGABRT);
			PM_TOSTR_CASE(SIGCHLD);
			PM_TOSTR_CASE(SIGHUP);
			PM_TOSTR_CASE(SIGINT);
			PM_TOSTR_CASE(SIGKILL);
			PM_TOSTR_CASE(SIGQUIT);
			PM_TOSTR_CASE(SIGTERM);
			PM_TOSTR_CASE(SIGUSR1);
			PM_TOSTR_CASE(SIGUSR2);
		default:
			/* all signals sent by postmaster should be listed here */
			Assert(false);
			return "(unknown)";
	}
#undef PM_TOSTR_CASE

	return "";					/* silence compiler */
}

/*
 * Send a signal to a postmaster child process
 *
 * On systems that have setsid(), each child process sets itself up as a
 * process group leader.  For signals that are generally interpreted in the
 * appropriate fashion, we signal the entire process group not just the
 * direct child process.  This allows us to, for example, SIGQUIT a blocked
 * archive_recovery script, or SIGINT a script being run by a backend via
 * system().
 *
 * There is a race condition for recently-forked children: they might not
 * have executed setsid() yet.  So we signal the child directly as well as
 * the group.  We assume such a child will handle the signal before trying
 * to spawn any grandchild processes.  We also assume that signaling the
 * child twice will not cause any problems.
 */
static void
signal_child(PMChild *pmchild, int signal)
{
	pid_t		pid;

	if (PostmasterChildHasLogicalBackendPublication(pmchild))
	{
		PgBackendInterruptType interrupt;

		ereport(DEBUG3,
				(errmsg_internal("sending signal %d/%s to %s logical backend",
								 signal, pm_signame(signal),
								 GetBackendTypeDesc(pmchild->bkend_type))));

		if (thread_child_signal_interrupt(pmchild, signal, &interrupt))
			(void) PostmasterChildRaiseThreadInterrupt(pmchild, interrupt);
		return;
	}

	Assert(PostmasterChildIsProcess(pmchild));
	pid = pmchild->pid;

	ereport(DEBUG3,
			(errmsg_internal("sending signal %d/%s to %s process with pid %d",
							 signal, pm_signame(signal),
							 GetBackendTypeDesc(pmchild->bkend_type),
							 (int) pmchild->pid)));

	if (kill(pid, signal) < 0)
		elog(DEBUG3, "kill(%ld,%d) failed: %m", (long) pid, signal);
#ifdef HAVE_SETSID
	switch (signal)
	{
		case SIGINT:
		case SIGTERM:
		case SIGQUIT:
		case SIGKILL:
		case SIGABRT:
			if (kill(-pid, signal) < 0)
				elog(DEBUG3, "kill(%ld,%d) failed: %m", (long) (-pid), signal);
			break;
		default:
			break;
	}
#endif
}

static bool
thread_child_signal_interrupt(PMChild *pmchild, int signal,
							  PgBackendInterruptType *interrupt)
{
	Assert(PostmasterChildHasLogicalBackendPublication(pmchild));
	Assert(interrupt != NULL);

	switch (signal)
	{
		case SIGUSR1:
			if (pmchild->bkend_type == B_LOGGER)
			{
				*interrupt = PG_BACKEND_INTERRUPT_LOG_ROTATE;
				return true;
			}
			return false;
		case SIGINT:
			if (pmchild->bkend_type == B_ARCHIVER)
				return false;
			if (pmchild->bkend_type == B_LOGGER)
				return false;
			if (pmchild->bkend_type == B_STARTUP)
				return false;
			if (pmchild->bkend_type == B_AUTOVAC_LAUNCHER)
			{
				*interrupt = PG_BACKEND_INTERRUPT_QUERY_CANCEL;
				return true;
			}
			if (pmchild->bkend_type == B_BG_WRITER)
				return false;
			if (pmchild->bkend_type == B_CHECKPOINTER)
			{
				*interrupt = PG_BACKEND_INTERRUPT_CHECKPOINTER_SHUTDOWN_XLOG;
				return true;
			}
			if (pmchild->bkend_type == B_IO_WORKER)
			{
				*interrupt = PG_BACKEND_INTERRUPT_PROC_DIE;
				return true;
			}
			if (pmchild->bkend_type == B_WAL_WRITER)
				return false;
			if (pmchild->bkend_type == B_WAL_RECEIVER)
				return false;
			if (pmchild->bkend_type == B_WAL_SUMMARIZER)
				return false;
			*interrupt = PG_BACKEND_INTERRUPT_QUERY_CANCEL;
			return true;
		case SIGTERM:
			if (pmchild->bkend_type == B_ARCHIVER)
			{
				*interrupt = PG_BACKEND_INTERRUPT_SHUTDOWN_REQUEST;
				return true;
			}
			if (pmchild->bkend_type == B_LOGGER)
				return false;
			if (pmchild->bkend_type == B_AUTOVAC_LAUNCHER)
			{
				*interrupt = PG_BACKEND_INTERRUPT_SHUTDOWN_REQUEST;
				return true;
			}
			if (pmchild->bkend_type == B_BG_WRITER)
			{
				*interrupt = PG_BACKEND_INTERRUPT_SHUTDOWN_REQUEST;
				return true;
			}
			if (pmchild->bkend_type == B_CHECKPOINTER)
				return false;
			if (pmchild->bkend_type == B_IO_WORKER)
				return false;
			if (pmchild->bkend_type == B_WAL_WRITER)
			{
				*interrupt = PG_BACKEND_INTERRUPT_SHUTDOWN_REQUEST;
				return true;
			}
			if (pmchild->bkend_type == B_WAL_RECEIVER)
			{
				*interrupt = PG_BACKEND_INTERRUPT_PROC_DIE;
				return true;
			}
			if (pmchild->bkend_type == B_WAL_SUMMARIZER)
			{
				*interrupt = PG_BACKEND_INTERRUPT_SHUTDOWN_REQUEST;
				return true;
			}
			if (pmchild->bkend_type == B_BG_WORKER)
			{
				*interrupt = PG_BACKEND_INTERRUPT_SHUTDOWN_REQUEST;
				return true;
			}
			if (pmchild->bkend_type == B_STARTUP)
			{
				*interrupt = PG_BACKEND_INTERRUPT_SHUTDOWN_REQUEST;
				return true;
			}
			*interrupt = PG_BACKEND_INTERRUPT_PROC_DIE;
			return true;
		case SIGUSR2:
			if (pmchild->bkend_type == B_STARTUP)
			{
				*interrupt = PG_BACKEND_INTERRUPT_STARTUP_PROMOTE;
				return true;
			}
			if (pmchild->bkend_type == B_ARCHIVER)
			{
				*interrupt = PG_BACKEND_INTERRUPT_WAKEUP_STOP;
				return true;
			}
			if (pmchild->bkend_type == B_AUTOVAC_LAUNCHER)
			{
				*interrupt = PG_BACKEND_INTERRUPT_AUTOVAC_LAUNCHER;
				return true;
			}
			if (pmchild->bkend_type == B_BG_WRITER)
				return false;
			if (pmchild->bkend_type == B_CHECKPOINTER)
			{
				*interrupt = PG_BACKEND_INTERRUPT_SHUTDOWN_REQUEST;
				return true;
			}
			if (pmchild->bkend_type == B_IO_WORKER)
			{
				*interrupt = PG_BACKEND_INTERRUPT_SHUTDOWN_REQUEST;
				return true;
			}
			if (pmchild->bkend_type == B_LOGGER)
				return false;
			return false;
		case SIGQUIT:
		case SIGKILL:
		case SIGABRT:
			*interrupt = PG_BACKEND_INTERRUPT_PROC_DIE;
			return true;
		case SIGHUP:
			*interrupt = PG_BACKEND_INTERRUPT_CONFIG_RELOAD;
			return true;
		default:
			return false;
	}
}

/*
 * Send a signal to the targeted children.
 */
static bool
SignalChildren(int signal, BackendTypeMask targetMask)
{
	dlist_iter	iter;
	bool		signaled = false;

	dlist_foreach(iter, &ActiveChildList)
	{
		PMChild    *bp = dlist_container(PMChild, elem, iter.cur);

		/*
		 * If we need to distinguish between B_BACKEND and B_WAL_SENDER, check
		 * if any B_BACKEND backends have recently announced that they are
		 * actually WAL senders.
		 */
		if (btmask_contains(targetMask, B_WAL_SENDER) != btmask_contains(targetMask, B_BACKEND) &&
			bp->bkend_type == B_BACKEND)
		{
			if (IsPostmasterChildWalSender(bp->child_slot))
				bp->bkend_type = B_WAL_SENDER;
		}

		if (!btmask_contains(targetMask, bp->bkend_type))
			continue;

		signal_child(bp, signal);
		signaled = true;
	}
	return signaled;
}

/*
 * Send a termination signal to children.  This considers all of our children
 * processes, except syslogger.
 */
static void
TerminateChildren(int signal)
{
	SignalChildren(signal, btmask_all_except(B_LOGGER));
	if (StartupPMChild != NULL)
	{
		if (signal == SIGQUIT || signal == SIGKILL || signal == SIGABRT)
			StartupStatus = STARTUP_SIGNALED;
	}
}

/*
 * BackendStartup -- start backend process
 *
 * returns: STATUS_ERROR if the fork failed, STATUS_OK otherwise.
 *
 * Note: if you change this code, also consider StartAutovacuumWorker and
 * StartBackgroundWorker.
 */
static int
BackendStartup(ClientSocket *client_sock)
{
	PMChild    *bn = NULL;
	BackendStartupData startup_data;
	CAC_state	cac;

	/*
	 * Capture time that Postmaster got a socket from accept (for logging
	 * connection establishment and setup total duration).
	 */
	startup_data.socket_created = GetCurrentTimestamp();

	/*
	 * Allocate and assign the child slot.  Note we must do this before
	 * forking, so that we can handle failures (out of memory or child-process
	 * slots) cleanly.
	 */
	cac = canAcceptConnections(B_BACKEND);
	if (cac == CAC_OK)
	{
		/* Can change later to B_WAL_SENDER */
		bn = AssignPostmasterChildSlot(B_BACKEND);
		if (!bn)
		{
			/*
			 * Too many regular child processes; launch a dead-end child
			 * process instead.
			 */
			cac = CAC_TOOMANY;
		}
	}
	if (!bn)
	{
		bn = AllocDeadEndChild();
		if (!bn)
		{
			ereport(LOG,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory")));
			return STATUS_ERROR;
		}
	}

	/* Pass down canAcceptConnections state */
	startup_data.canAcceptConnections = cac;
	bn->rw = NULL;

	/* Hasn't asked to be notified about any bgworkers yet */
	bn->bgworker_notify = false;

	if (!postmaster_child_launch_carrier(bn, bn->bkend_type, bn->child_slot,
										 &startup_data, sizeof(startup_data),
										 client_sock))
	{
		/* in parent, fork failed */
		int			save_errno = errno;

		(void) ReleasePostmasterChildSlot(bn);
		errno = save_errno;
		ereport(LOG,
				(errmsg("could not fork new process for connection: %m")));
		report_fork_failure_to_client(client_sock, save_errno);
		return STATUS_ERROR;
	}

	/* in parent, successful fork */
	ereport(DEBUG2,
			(errmsg_internal("forked new %s, pid=%d socket=%d",
							 GetBackendTypeDesc(bn->bkend_type),
							 (int) bn->pid, (int) client_sock->sock)));

	/*
	 * Everything's been successful, it's safe to add this backend to our list
	 * of backends.
	 */
	return STATUS_OK;
}

/*
 * Try to report backend fork() failure to client before we close the
 * connection.  Since we do not care to risk blocking the postmaster on
 * this connection, we set the connection to non-blocking and try only once.
 *
 * This is grungy special-purpose code; we cannot use backend libpq since
 * it's not up and running.
 */
static void
report_fork_failure_to_client(ClientSocket *client_sock, int errnum)
{
	char		buffer[1000];
	int			rc;

	/* Format the error message packet (always V2 protocol) */
	snprintf(buffer, sizeof(buffer), "E%s%s\n",
			 _("could not fork new process for connection: "),
			 strerror(errnum));

	/* Set port to non-blocking.  Don't do send() if this fails */
	if (!pg_set_noblock(client_sock->sock))
		return;

	/* We'll retry after EINTR, but ignore all other failures */
	do
	{
		rc = send(client_sock->sock, buffer, strlen(buffer) + 1, 0);
	} while (rc < 0 && errno == EINTR);
}

/*
 * ExitPostmaster -- cleanup
 *
 * Do NOT call exit() directly --- always go through here!
 */
static void
ExitPostmaster(int status)
{
#ifdef HAVE_PTHREAD_IS_THREADED_NP

	/*
	 * There is no known cause for a postmaster to become multithreaded after
	 * startup.  However, we might reach here via an error exit before
	 * reaching the test in PostmasterMain, so provide the same hint as there.
	 * This message uses LOG level, because an unclean shutdown at this point
	 * would usually not look much different from a clean shutdown.
	 */
	if (!multithreaded && pthread_is_threaded_np() != 0)
		ereport(LOG,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("postmaster became multithreaded"),
				 errhint("Set the LC_ALL environment variable to a valid locale.")));
#endif

	/* should cleanup shared memory and kill all backends */

	/*
	 * Not sure of the semantics here.  When the Postmaster dies, should the
	 * backends all be killed? probably not.
	 *
	 * MUST		-- vadim 05-10-1999
	 */

	proc_exit(status);
}

/*
 * Handle pmsignal conditions representing requests from backends,
 * and check for promote and logrotate requests from pg_ctl.
 */
static void
process_pm_pmsignal(void)
{
	bool		request_state_update = false;

	pending_pm_pmsignal = false;

	ereport(DEBUG2,
			(errmsg_internal("postmaster received pmsignal signal")));

	/*
	 * RECOVERY_STARTED and BEGIN_HOT_STANDBY signals are ignored in
	 * unexpected states. If the startup process quickly starts up, completes
	 * recovery, exits, we might process the death of the startup process
	 * first. We don't want to go back to recovery in that case.
	 */
	if (CheckPostmasterSignal(PMSIGNAL_RECOVERY_STARTED) &&
		pmState == PM_STARTUP && Shutdown == NoShutdown)
	{
		/* WAL redo has started. We're out of reinitialization. */
		FatalError = false;
		AbortStartTime = 0;
		reachedConsistency = false;

		/*
		 * Start the archiver if we're responsible for (re-)archiving received
		 * files.
		 */
		Assert(PgArchPMChild == NULL);
		if (XLogArchivingAlways())
			PgArchPMChild = StartChildProcess(B_ARCHIVER);

		/*
		 * If we aren't planning to enter hot standby mode later, treat
		 * RECOVERY_STARTED as meaning we're out of startup, and report status
		 * accordingly.
		 */
		if (!EnableHotStandby)
		{
			AddToDataDirLockFile(LOCK_FILE_LINE_PM_STATUS, PM_STATUS_STANDBY);
#ifdef USE_SYSTEMD
			sd_notify(0, "READY=1");
#endif
		}

		UpdatePMState(PM_RECOVERY);
	}

	if (CheckPostmasterSignal(PMSIGNAL_RECOVERY_CONSISTENT) &&
		pmState == PM_RECOVERY && Shutdown == NoShutdown)
	{
		reachedConsistency = true;
	}

	if (CheckPostmasterSignal(PMSIGNAL_BEGIN_HOT_STANDBY) &&
		(pmState == PM_RECOVERY && Shutdown == NoShutdown))
	{
		ereport(LOG,
				(errmsg("database system is ready to accept read-only connections")));

		/* Report status */
		AddToDataDirLockFile(LOCK_FILE_LINE_PM_STATUS, PM_STATUS_READY);
#ifdef USE_SYSTEMD
		sd_notify(0, "READY=1");
#endif

		UpdatePMState(PM_HOT_STANDBY);
		connsAllowed = true;

		/* Some workers may be scheduled to start now */
		StartWorkerNeeded = true;
	}

	/* Process IO worker start requests. */
	if (CheckPostmasterSignal(PMSIGNAL_IO_WORKER_GROW))
	{
		/*
		 * No local flag, as the state is exposed through pgaio_worker_*()
		 * functions.  This signal is received on potentially actionable level
		 * changes, so that maybe_start_io_workers() will run.
		 */
	}

	/* Process background worker state changes. */
	if (CheckPostmasterSignal(PMSIGNAL_BACKGROUND_WORKER_CHANGE))
	{
		/* Accept new worker requests only if not stopping. */
		BackgroundWorkerStateChange(pmState < PM_STOP_BACKENDS);
		StartWorkerNeeded = true;
	}

	/* Tell syslogger to rotate logfile if requested */
	if (SysLoggerPMChild != NULL)
	{
		if (CheckLogrotateSignal())
		{
			signal_child(SysLoggerPMChild, SIGUSR1);
			RemoveLogrotateSignalFiles();
		}
		else if (CheckPostmasterSignal(PMSIGNAL_ROTATE_LOGFILE))
		{
			signal_child(SysLoggerPMChild, SIGUSR1);
		}
	}

	if (CheckPostmasterSignal(PMSIGNAL_START_AUTOVAC_LAUNCHER) &&
		Shutdown <= SmartShutdown && pmState < PM_STOP_BACKENDS)
	{
		/*
		 * Start one iteration of the autovacuum daemon, even if autovacuuming
		 * is nominally not enabled.  This is so we can have an active defense
		 * against transaction ID wraparound.  We set a flag for the main loop
		 * to do it rather than trying to do it here --- this is because the
		 * autovac process itself may send the signal, and we want to handle
		 * that by launching another iteration as soon as the current one
		 * completes.
		 */
		start_autovac_launcher = true;
	}

	if (CheckPostmasterSignal(PMSIGNAL_START_AUTOVAC_WORKER) &&
		Shutdown <= SmartShutdown && pmState < PM_STOP_BACKENDS)
	{
		/* The autovacuum launcher wants us to start a worker process. */
		StartAutovacuumWorker();
	}

#ifdef USE_XTC_CARRIER
	if (CheckPostmasterSignal(PMSIGNAL_AUTOVAC_WORKER_TIMEOUT))
	{
		/*
		 * The autovacuum launcher canceled a worker whose start it timed out
		 * on.  Under the xtc carrier a worker fiber that was never scheduled
		 * leaves an orphaned pooled-logical PMChild; reap it so a later
		 * PM_WAIT_BACKENDS (fast stop) cannot wedge waiting on a fiber that
		 * will never publish an exit.  Gate on the worker-start-timeout so we
		 * only reap the aged, stuck worker and never a fresh one the launcher
		 * just relaunched.  A no-op in process mode / when there is no orphan.
		 * Re-run the state machine if we reaped one, matching
		 * process_pm_pooled_logical_exit().
		 */
		int			start_timeout_ms = Min(autovacuum_naptime, 60) * 1000;

		if (ReapOrphanedThreadedWorker(B_AUTOVAC_WORKER, start_timeout_ms))
			request_state_update = true;
	}
#endif

	if (CheckPostmasterSignal(PMSIGNAL_START_WALRECEIVER))
	{
		/* Startup Process wants us to start the walreceiver process. */
		WalReceiverRequested = true;
	}

	if (CheckPostmasterSignal(PMSIGNAL_XLOG_IS_SHUTDOWN))
	{
		/* Checkpointer completed the shutdown checkpoint */
		if (pmState == PM_WAIT_XLOG_SHUTDOWN)
		{
			/*
			 * If we have an archiver subprocess, tell it to do a last archive
			 * cycle and quit. Likewise, if we have walsender processes, tell
			 * them to send any remaining WAL and quit.
			 */
			Assert(Shutdown > NoShutdown);

			/* Waken archiver for the last time */
			if (PgArchPMChild != NULL)
				signal_child(PgArchPMChild, SIGUSR2);

			/*
			 * Waken walsenders for the last time. No regular backends should
			 * be around anymore.
			 */
			SignalChildren(SIGUSR2, btmask(B_WAL_SENDER));

			UpdatePMState(PM_WAIT_XLOG_ARCHIVAL);
		}
		else if (!FatalError && Shutdown != ImmediateShutdown)
		{
			/*
			 * Checkpointer only ought to perform the shutdown checkpoint
			 * during shutdown.  If somehow checkpointer did so in another
			 * situation, we have no choice but to crash-restart.
			 *
			 * It's possible however that we get PMSIGNAL_XLOG_IS_SHUTDOWN
			 * outside of PM_WAIT_XLOG_SHUTDOWN if an orderly shutdown was
			 * "interrupted" by a crash or an immediate shutdown.
			 */
			ereport(LOG,
					(errmsg("WAL was shut down unexpectedly")));

			/*
			 * Doesn't seem likely to help to take send_abort_for_crash into
			 * account here.
			 */
			HandleFatalError(PMQUIT_FOR_CRASH, false);
		}

		/*
		 * Need to run PostmasterStateMachine() to check if we already can go
		 * to the next state.
		 */
		request_state_update = true;
	}

	/*
	 * Try to advance postmaster's state machine, if a child requests it.
	 */
	if (CheckPostmasterSignal(PMSIGNAL_ADVANCE_STATE_MACHINE))
	{
		request_state_update = true;
	}

	/*
	 * Be careful about the order of this action relative to this function's
	 * other actions.  Generally, this should be after other actions, in case
	 * they have effects PostmasterStateMachine would need to know about.
	 * However, we should do it before the CheckPromoteSignal step, which
	 * cannot have any (immediate) effect on the state machine, but does
	 * depend on what state we're in now.
	 */
	if (request_state_update)
	{
		PostmasterStateMachine();
	}

	if (StartupPMChild != NULL &&
		(pmState == PM_STARTUP || pmState == PM_RECOVERY ||
		 pmState == PM_HOT_STANDBY) &&
		CheckPromoteSignal())
	{
		/*
		 * Tell startup process to finish recovery.
		 *
		 * Leave the promote signal file in place and let the Startup process
		 * do the unlink.
		 */
		signal_child(StartupPMChild, SIGUSR2);
	}
}

/*
 * Dummy signal handler
 *
 * We use this for signals that we don't actually use in the postmaster,
 * but we do use in backends.  If we were to PG_SIG_IGN such signals in the
 * postmaster, then a newly started backend might drop a signal that arrives
 * before it's able to reconfigure its signal processing.  (See notes in
 * tcop/postgres.c.)
 */
static void
dummy_handler(SIGNAL_ARGS)
{
}

/*
 * Count up number of child processes of specified types.
 */
static int
CountChildren(BackendTypeMask targetMask)
{
	dlist_iter	iter;
	int			cnt = 0;

	dlist_foreach(iter, &ActiveChildList)
	{
		PMChild    *bp = dlist_container(PMChild, elem, iter.cur);

		/*
		 * If we need to distinguish between B_BACKEND and B_WAL_SENDER, check
		 * if any B_BACKEND backends have recently announced that they are
		 * actually WAL senders.
		 */
		if (btmask_contains(targetMask, B_WAL_SENDER) != btmask_contains(targetMask, B_BACKEND) &&
			bp->bkend_type == B_BACKEND)
		{
			if (IsPostmasterChildWalSender(bp->child_slot))
				bp->bkend_type = B_WAL_SENDER;
		}

		if (!btmask_contains(targetMask, bp->bkend_type))
			continue;

		ereport(DEBUG4,
				(errmsg_internal("%s process %d is still running",
								 GetBackendTypeDesc(bp->bkend_type), (int) bp->pid)));

		cnt++;
	}
	return cnt;
}


/*
 * StartChildProcess -- start an auxiliary process for the postmaster
 *
 * "type" determines what kind of child will be started.  All child types
 * initially go to AuxiliaryProcessMain, which will handle common setup.
 *
 * Return value of StartChildProcess is subprocess' PMChild entry, or NULL on
 * failure.
 */
static PMChild *
StartChildProcess(BackendType type)
{
	PMChild    *pmchild;

	pmchild = AssignPostmasterChildSlot(type);
	if (!pmchild)
	{
		if (type == B_AUTOVAC_WORKER)
			ereport(LOG,
					(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
					 errmsg("no slot available for new autovacuum worker process")));
		else
		{
			/* shouldn't happen because we allocate enough slots */
			elog(LOG, "no postmaster child slot available for aux process");
		}
		return NULL;
	}

	if (!postmaster_child_launch_carrier(pmchild, type, pmchild->child_slot,
										 NULL, 0, NULL))
	{
		/* in parent, fork failed */
		ReleasePostmasterChildSlot(pmchild);
		ereport(LOG,
				(errmsg("could not fork \"%s\" process: %m", PostmasterChildName(type))));

		/*
		 * fork failure is fatal during startup, but there's no need to choke
		 * immediately if starting other child types fails.
		 */
		if (type == B_STARTUP)
			ExitPostmaster(1);
		return NULL;
	}

	/* in parent, successful fork */
	return pmchild;
}

/*
 * StartSysLogger -- start the syslogger process
 */
void
StartSysLogger(void)
{
	Assert(SysLoggerPMChild == NULL);

	SysLoggerPMChild = AssignPostmasterChildSlot(B_LOGGER);
	if (!SysLoggerPMChild)
		elog(PANIC, "no postmaster child slot available for syslogger");

	if (!SysLogger_Start(SysLoggerPMChild))
	{
		ReleasePostmasterChildSlot(SysLoggerPMChild);
		SysLoggerPMChild = NULL;
	}
}

/*
 * StartAutovacuumWorker
 *		Start an autovac worker process.
 *
 * This function is here because it enters the resulting PID into the
 * postmaster's private backends list.
 *
 * NB -- this code very roughly matches BackendStartup.
 */
static void
StartAutovacuumWorker(void)
{
	PMChild    *bn;

	/*
	 * If not in condition to run a process, don't try, but handle it like a
	 * fork failure.  This does not normally happen, since the signal is only
	 * supposed to be sent by autovacuum launcher when it's OK to do it, but
	 * we have to check to avoid race-condition problems during DB state
	 * changes.
	 */
	if (canAcceptConnections(B_AUTOVAC_WORKER) == CAC_OK)
	{
		bn = StartChildProcess(B_AUTOVAC_WORKER);
		if (bn)
		{
			bn->bgworker_notify = false;
			bn->rw = NULL;
			return;
		}
		else
		{
			/*
			 * fork failed, fall through to report -- actual error message was
			 * logged by StartChildProcess
			 */
		}
	}

	/*
	 * Report the failure to the launcher, if it's running.  (If it's not, we
	 * might not even be connected to shared memory, so don't try to call
	 * AutoVacWorkerFailed.)  Note that we also need to signal it so that it
	 * responds to the condition, but we don't do that here, instead waiting
	 * for ServerLoop to do it.  This way we avoid a ping-pong signaling in
	 * quick succession between the autovac launcher and postmaster in case
	 * things get ugly.
	 */
	if (AutoVacLauncherPMChild != NULL)
	{
		AutoVacWorkerFailed();
		avlauncher_needs_signal = true;
	}
}


/*
 * Create the opts file
 */
static bool
CreateOptsFile(int argc, char *argv[], char *fullprogname)
{
	FILE	   *fp;
	int			i;

#define OPTS_FILE	"postmaster.opts"

	if ((fp = fopen(OPTS_FILE, "w")) == NULL)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not create file \"%s\": %m", OPTS_FILE)));
		return false;
	}

	fprintf(fp, "%s", fullprogname);
	for (i = 1; i < argc; i++)
		fprintf(fp, " \"%s\"", argv[i]);
	fputs("\n", fp);

	if (fclose(fp))
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not write file \"%s\": %m", OPTS_FILE)));
		return false;
	}

	return true;
}


/*
 * Start a new bgworker.
 * Starting time conditions must have been checked already.
 *
 * Returns true on success, false on failure.
 * In either case, update the RegisteredBgWorker's state appropriately.
 *
 * NB -- this code very roughly matches BackendStartup.
 */
static bool
StartBackgroundWorker(RegisteredBgWorker *rw)
{
	PMChild    *bn;
	bool		thread_compatible;

	Assert(rw->rw_pid == 0);
	thread_compatible = BackgroundWorkerCanUseThreadCarrier(&rw->rw_worker);

	/*
	 * Generic background workers still use the process-only bgworker ABI.
	 * Once this postmaster has created thread carriers, another fork is not
	 * safe.  Reject the worker explicitly and notify dynamic waiters rather
	 * than reporting a transient fork failure and retrying indefinitely.
	 */
	if (multithreaded && PostmasterThreadCarriersStarted() && !thread_compatible)
	{
		ereport(LOG,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("background worker \"%s\" is not supported in threaded mode",
						rw->rw_worker.bgw_name),
				 errdetail("Background workers must set a thread-compatible backend model before they can run on thread carriers.")));

		rw->rw_crashed_at = 0;
		rw->rw_terminate = true;
		rw->rw_pid = 0;
		ReportBackgroundWorkerExit(rw);
		return true;
	}

	/*
	 * Allocate and assign the child slot.  Note we must do this before
	 * forking, so that we can handle failures (out of memory or child-process
	 * slots) cleanly.
	 *
	 * Treat failure as though the worker had crashed.  That way, the
	 * postmaster will wait a bit before attempting to start it again; if we
	 * tried again right away, most likely we'd find ourselves hitting the
	 * same resource-exhaustion condition.
	 */
	bn = AssignPostmasterChildSlot(B_BG_WORKER);
	if (bn == NULL)
	{
		ereport(LOG,
				(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("no slot available for new background worker process")));
		rw->rw_crashed_at = GetCurrentTimestamp();
		return false;
	}
	bn->rw = rw;
	bn->bkend_type = B_BG_WORKER;
	bn->bgworker_notify = false;

	if (multithreaded && thread_compatible)
		ereport(DEBUG1,
				(errmsg_internal("starting background worker thread carrier \"%s\"",
								 rw->rw_worker.bgw_name)));
	else
		ereport(DEBUG1,
				(errmsg_internal("starting background worker process \"%s\"",
								 rw->rw_worker.bgw_name)));

	if (!postmaster_child_launch_carrier(bn, B_BG_WORKER, bn->child_slot,
										 &rw->rw_worker,
										 sizeof(BackgroundWorker), NULL))
	{
		/* in postmaster, fork failed ... */
		ereport(LOG,
				(errmsg("could not fork background worker process: %m")));
		/* undo what AssignPostmasterChildSlot did */
		ReleasePostmasterChildSlot(bn);

		/* mark entry as crashed, so we'll try again later */
		rw->rw_crashed_at = GetCurrentTimestamp();
		return false;
	}

	/* in postmaster, carrier launch succeeded ... */
	rw->rw_pid = PostmasterChildSignalPid(bn);

	/*
	 * Process-backed workers are visible to dynamic waiters as soon as fork
	 * succeeds.  Thread-backed workers publish that visibility from the
	 * worker's explicit startup-complete boundary, after the startup path can
	 * safely accept termination.
	 */
	if (!PostmasterChildIsThread(bn))
		ReportBackgroundWorkerPID(rw);
	return true;
}

/*
 * Does the current postmaster state require starting a worker with the
 * specified start_time?
 */
static bool
bgworker_should_start_now(BgWorkerStartTime start_time)
{
	switch (pmState)
	{
		case PM_NO_CHILDREN:
		case PM_WAIT_CHECKPOINTER:
		case PM_WAIT_DEAD_END:
		case PM_WAIT_XLOG_ARCHIVAL:
		case PM_WAIT_XLOG_SHUTDOWN:
		case PM_WAIT_IO_WORKERS:
		case PM_WAIT_BACKENDS:
		case PM_STOP_BACKENDS:
			break;

		case PM_RUN:
			if (start_time == BgWorkerStart_RecoveryFinished)
				return true;
			pg_fallthrough;

		case PM_HOT_STANDBY:
			if (start_time == BgWorkerStart_ConsistentState)
				return true;
			pg_fallthrough;

		case PM_RECOVERY:
		case PM_STARTUP:
		case PM_INIT:
			if (start_time == BgWorkerStart_PostmasterStart)
				return true;
	}

	return false;
}

/*
 * If the time is right, start background worker(s).
 *
 * As a side effect, the bgworker control variables are set or reset
 * depending on whether more workers may need to be started.
 *
 * We limit the number of workers started per call, to avoid consuming the
 * postmaster's attention for too long when many such requests are pending.
 * As long as StartWorkerNeeded is true, ServerLoop will not block and will
 * call this function again after dealing with any other issues.
 */
static void
maybe_start_bgworkers(void)
{
#define MAX_BGWORKERS_TO_LAUNCH 100
	int			num_launched = 0;
	TimestampTz now = 0;
	dlist_mutable_iter iter;

	/*
	 * During crash recovery, we have no need to be called until the state
	 * transition out of recovery.
	 */
	if (FatalError)
	{
		StartWorkerNeeded = false;
		HaveCrashedWorker = false;
		return;
	}

	/* Don't need to be called again unless we find a reason for it below */
	StartWorkerNeeded = false;
	HaveCrashedWorker = false;

	dlist_foreach_modify(iter, &BackgroundWorkerList)
	{
		RegisteredBgWorker *rw;

		rw = dlist_container(RegisteredBgWorker, rw_lnode, iter.cur);

		/* ignore if already running */
		if (rw->rw_pid != 0)
			continue;

		/* if marked for death, clean up and remove from list */
		if (rw->rw_terminate)
		{
			ForgetBackgroundWorker(rw);
			continue;
		}

		/*
		 * If this worker has crashed previously, maybe it needs to be
		 * restarted (unless on registration it specified it doesn't want to
		 * be restarted at all).  Check how long ago did a crash last happen.
		 * If the last crash is too recent, don't start it right away; let it
		 * be restarted once enough time has passed.
		 */
		if (rw->rw_crashed_at != 0)
		{
			if (rw->rw_worker.bgw_restart_time == BGW_NEVER_RESTART)
			{
				int			notify_pid;

				notify_pid = rw->rw_worker.bgw_notify_pid;

				ForgetBackgroundWorker(rw);

				/* Report worker is gone now. */
				if (notify_pid != 0)
					PostmasterNotifyPIDForWorker(notify_pid);

				continue;
			}

			/* read system time only when needed */
			if (now == 0)
				now = GetCurrentTimestamp();

			if (!TimestampDifferenceExceeds(rw->rw_crashed_at, now,
											rw->rw_worker.bgw_restart_time * 1000))
			{
				/* Set flag to remember that we have workers to start later */
				HaveCrashedWorker = true;
				continue;
			}
		}

		if (bgworker_should_start_now(rw->rw_worker.bgw_start_time))
		{
			/* reset crash time before trying to start worker */
			rw->rw_crashed_at = 0;

			/*
			 * Try to start the worker.
			 *
			 * On failure, give up processing workers for now, but set
			 * StartWorkerNeeded so we'll come back here on the next iteration
			 * of ServerLoop to try again.  (We don't want to wait, because
			 * there might be additional ready-to-run workers.)  We could set
			 * HaveCrashedWorker as well, since this worker is now marked
			 * crashed, but there's no need because the next run of this
			 * function will do that.
			 */
			if (!StartBackgroundWorker(rw))
			{
				StartWorkerNeeded = true;
				return;
			}

			/*
			 * If we've launched as many workers as allowed, quit, but have
			 * ServerLoop call us again to look for additional ready-to-run
			 * workers.  There might not be any, but we'll find out the next
			 * time we run.
			 */
			if (++num_launched >= MAX_BGWORKERS_TO_LAUNCH)
			{
				StartWorkerNeeded = true;
				return;
			}
		}
	}
}

static bool
maybe_reap_io_worker(int pid)
{
	for (int i = 0; i < MAX_IO_WORKERS; ++i)
	{
		if (io_worker_children[i] &&
			PostmasterChildIsProcess(io_worker_children[i]) &&
			io_worker_children[i]->pid == pid)
			return cleanup_io_worker_child(io_worker_children[i]);
	}
	return false;
}

static bool
cleanup_archiver_child(PMChild *child, int exitstatus)
{
	Assert(child != NULL);
	Assert(child == PgArchPMChild);
	Assert(child->bkend_type == B_ARCHIVER);

	ReleasePostmasterChildSlot(PgArchPMChild);
	PgArchPMChild = NULL;
	if (!EXIT_STATUS_0(exitstatus) && !EXIT_STATUS_1(exitstatus))
		HandleChildCrash(0, exitstatus, _("archiver process"));

	return true;
}

static bool
cleanup_autovac_launcher_child(PMChild *child, int exitstatus)
{
	Assert(child != NULL);
	Assert(child == AutoVacLauncherPMChild);
	Assert(child->bkend_type == B_AUTOVAC_LAUNCHER);

	ReleasePostmasterChildSlot(AutoVacLauncherPMChild);
	AutoVacLauncherPMChild = NULL;
	if (!EXIT_STATUS_0(exitstatus))
		HandleChildCrash(0, exitstatus, _("autovacuum launcher process"));

	return true;
}

static bool
cleanup_bgwriter_child(PMChild *child, int exitstatus, int crash_pid)
{
	Assert(child != NULL);
	Assert(child == BgWriterPMChild);
	Assert(child->bkend_type == B_BG_WRITER);

	bgwriter_thread_handoff_pending = false;

	ReleasePostmasterChildSlot(BgWriterPMChild);
	BgWriterPMChild = NULL;
	if (!EXIT_STATUS_0(exitstatus))
		HandleChildCrash(crash_pid, exitstatus,
						 _("background writer process"));

	return true;
}

static bool
cleanup_checkpointer_child(PMChild *child, int exitstatus, int crash_pid)
{
	bool		thread_handoff;

	Assert(child != NULL);
	Assert(child == CheckpointerPMChild);
	Assert(child->bkend_type == B_CHECKPOINTER);

	thread_handoff = checkpointer_thread_handoff_pending;
	checkpointer_thread_handoff_pending = false;

	ReleasePostmasterChildSlot(CheckpointerPMChild);
	CheckpointerPMChild = NULL;
	if (EXIT_STATUS_0(exitstatus) && pmState == PM_WAIT_CHECKPOINTER)
	{
		/*
		 * OK, we saw normal exit of the checkpointer after it's been told to
		 * shut down.  We know checkpointer wrote a shutdown checkpoint,
		 * otherwise we'd still be in PM_WAIT_XLOG_SHUTDOWN state.
		 *
		 * At this point only dead-end children and logger should be left.
		 */
		UpdatePMState(PM_WAIT_DEAD_END);
		ConfigurePostmasterWaitSet(false);
		SignalChildren(SIGTERM, btmask_all_except(B_LOGGER));
	}
	else if (EXIT_STATUS_0(exitstatus) && thread_handoff && pmState == PM_RUN)
	{
		/*
		 * The startup-era process checkpointer exited on request so it can be
		 * relaunched as a thread carrier now that normal threaded operation
		 * has begun.
		 */
	}
	else
	{
		/*
		 * Any unexpected exit of the checkpointer (including FATAL exit) is
		 * treated as a crash.
		 */
		HandleChildCrash(crash_pid, exitstatus,
						 _("checkpointer process"));
	}

	return true;
}

static bool
cleanup_io_worker_child(PMChild *child)
{
	for (int i = 0; i < MAX_IO_WORKERS; ++i)
	{
		if (io_worker_children[i] == child)
		{
			ReleasePostmasterChildSlot(child);

			--io_worker_count;
			io_worker_children[i] = NULL;
			io_worker_thread_handoff_pending[i] = false;
			return true;
		}
	}
	return false;
}

static bool
cleanup_slot_sync_worker_child(PMChild *child, int exitstatus, int crash_pid)
{
	Assert(child != NULL);
	Assert(child == SlotSyncWorkerPMChild);
	Assert(child->bkend_type == B_SLOTSYNC_WORKER);

	ReleasePostmasterChildSlot(SlotSyncWorkerPMChild);
	SlotSyncWorkerPMChild = NULL;
	if (!EXIT_STATUS_0(exitstatus) && !EXIT_STATUS_1(exitstatus))
		HandleChildCrash(crash_pid, exitstatus,
						 _("slot sync worker process"));

	return true;
}

static bool
cleanup_startup_child(PMChild *child, int exitstatus, int crash_pid)
{
	Assert(child != NULL);
	Assert(child == StartupPMChild);
	Assert(child->bkend_type == B_STARTUP);

	ReleasePostmasterChildSlot(StartupPMChild);
	StartupPMChild = NULL;

	/*
	 * Startup exited in response to a shutdown request, or completed normally
	 * regardless of the shutdown request.
	 */
	if (Shutdown > NoShutdown &&
		(EXIT_STATUS_0(exitstatus) || EXIT_STATUS_1(exitstatus)))
	{
		StartupStatus = STARTUP_NOT_RUNNING;
		UpdatePMState(PM_WAIT_BACKENDS);
		return true;
	}

	if (EXIT_STATUS_3(exitstatus))
	{
		ereport(LOG,
				(errmsg("shutdown at recovery target")));
		StartupStatus = STARTUP_NOT_RUNNING;
		Shutdown = Max(Shutdown, SmartShutdown);
		TerminateChildren(SIGTERM);
		UpdatePMState(PM_WAIT_BACKENDS);
		return true;
	}

	/*
	 * Any unexpected exit, including FATAL, of startup is catastrophic.  One
	 * exception is a prior SIGQUIT: that means we may need to restart crash
	 * recovery rather than treating startup itself as permanently failed.
	 */
	if (!EXIT_STATUS_0(exitstatus))
	{
		if (StartupStatus == STARTUP_SIGNALED)
		{
			StartupStatus = STARTUP_NOT_RUNNING;
			if (pmState == PM_STARTUP)
				UpdatePMState(PM_WAIT_BACKENDS);
		}
		else
			StartupStatus = STARTUP_CRASHED;
		HandleChildCrash(crash_pid, exitstatus,
						 _("startup process"));
		return true;
	}

	/*
	 * Startup succeeded, commence normal operations.
	 */
	StartupStatus = STARTUP_NOT_RUNNING;
	FatalError = false;
	AbortStartTime = 0;
	ReachedNormalRunning = true;
	UpdatePMState(PM_RUN);
	connsAllowed = true;

	/*
	 * At the next iteration of the postmaster's main loop, we will crank up
	 * the background tasks like the autovacuum launcher and background workers
	 * that were not started earlier already.
	 */
	StartWorkerNeeded = true;

	/* at this point we are really open for business */
	ereport(LOG,
			(errmsg("database system is ready to accept connections")));

	/* Report status */
	AddToDataDirLockFile(LOCK_FILE_LINE_PM_STATUS, PM_STATUS_READY);
#ifdef USE_SYSTEMD
	sd_notify(0, "READY=1");
#endif

	return true;
}

static bool
cleanup_wal_writer_child(PMChild *child, int exitstatus)
{
	Assert(child != NULL);
	Assert(child == WalWriterPMChild);
	Assert(child->bkend_type == B_WAL_WRITER);

	ReleasePostmasterChildSlot(WalWriterPMChild);
	WalWriterPMChild = NULL;
	if (!EXIT_STATUS_0(exitstatus))
		HandleChildCrash(0, exitstatus, _("WAL writer process"));

	return true;
}

static bool
cleanup_wal_receiver_child(PMChild *child, int exitstatus, int crash_pid)
{
	Assert(child != NULL);
	Assert(child == WalReceiverPMChild);
	Assert(child->bkend_type == B_WAL_RECEIVER);

	ReleasePostmasterChildSlot(WalReceiverPMChild);
	WalReceiverPMChild = NULL;
	if (!EXIT_STATUS_0(exitstatus) && !EXIT_STATUS_1(exitstatus))
		HandleChildCrash(crash_pid, exitstatus, _("WAL receiver process"));

	return true;
}

static bool
cleanup_wal_summarizer_child(PMChild *child, int exitstatus)
{
	Assert(child != NULL);
	Assert(child == WalSummarizerPMChild);
	Assert(child->bkend_type == B_WAL_SUMMARIZER);

	ReleasePostmasterChildSlot(WalSummarizerPMChild);
	WalSummarizerPMChild = NULL;
	if (!EXIT_STATUS_0(exitstatus))
		HandleChildCrash(0, exitstatus, _("WAL summarizer process"));

	return true;
}

/*
 * The startup syslogger must be process-backed because it is started before
 * the startup process and other startup-era children are forked.  In threaded
 * mode, once a normal thread carrier exists, ask that process logger to exit.
 * The child-exit path will launch the replacement through the carrier-aware
 * path, which selects a thread carrier after runtime startup.
 */
static void
maybe_handoff_syslogger(void)
{
	if (!multithreaded || pmState != PM_RUN || Shutdown != NoShutdown ||
		!PostmasterThreadCarriersStarted())
		return;

	if (SysLoggerPMChild != NULL &&
		PostmasterChildIsProcess(SysLoggerPMChild) &&
		!syslogger_thread_handoff_pending)
	{
		syslogger_thread_handoff_pending = true;
		signal_child(SysLoggerPMChild, SIGUSR2);
	}
}

/*
 * Initial I/O workers are started before regular backend threads can exist.
 * In threaded mode, ask those startup-era process workers to exit after the
 * postmaster reaches PM_RUN. maybe_start_io_workers() will then fill the
 * minimum worker count through the normal thread-carrier launch path.
 */
static void
maybe_handoff_io_workers(void)
{
	if (!multithreaded || pmState != PM_RUN || Shutdown != NoShutdown ||
		!PostmasterThreadCarriersStarted())
		return;

	for (int i = 0; i < MAX_IO_WORKERS; ++i)
	{
		PMChild    *child = io_worker_children[i];

		if (child != NULL &&
			PostmasterChildIsProcess(child) &&
			!io_worker_thread_handoff_pending[i])
		{
			io_worker_thread_handoff_pending[i] = true;
			signal_child(child, SIGUSR2);
		}
	}
}

/*
 * Returns the next time at which maybe_start_io_workers() would start one or
 * more I/O workers.  Any time in the past means ASAP, and 0 means no worker
 * is currently scheduled.
 *
 * This is called by DetermineSleepTime() and also maybe_start_io_workers()
 * itself, to make sure that they agree.
 */
static TimestampTz
maybe_start_io_workers_scheduled_at(void)
{
	if (!pgaio_workers_enabled())
		return 0;

	/*
	 * If we're in final shutting down state, then we're just waiting for all
	 * processes to exit.
	 */
	if (pmState >= PM_WAIT_IO_WORKERS)
		return 0;

	/* Don't start new workers during an immediate shutdown either. */
	if (Shutdown >= ImmediateShutdown)
		return 0;

	/*
	 * Don't start new workers if we're in the shutdown phase of a crash
	 * restart. But we *do* need to start if we're already starting up again.
	 */
	if (FatalError && pmState >= PM_STOP_BACKENDS)
		return 0;

	/*
	 * Don't start a worker if we're at or above the maximum.  (Excess workers
	 * exit when the GUC is lowered, but the count can be temporarily too high
	 * until they are reaped.)
	 */
	if (io_worker_count >= io_max_workers)
		return 0;

	/* If we're under the minimum, start a worker as soon as possible. */
	if (io_worker_count < io_min_workers)
		return TIMESTAMP_MINUS_INFINITY;	/* start worker ASAP */

	/* Only proceed if a "grow" signal has been received from a worker. */
	if (!pgaio_worker_pm_test_grow_signal_sent())
		return 0;

	/*
	 * maybe_start_io_workers() should start a new I/O worker after this time,
	 * or as soon as possible if is already in the past.
	 */
	return io_worker_launch_next_time;
}

/*
 * Start I/O workers if required.  Used at startup, to respond to change of
 * the io_min_workers GUC, when asked to start a new one due to submission
 * queue backlog, and after workers terminate in response to errors (by
 * starting "replacement" workers).
 */
static void
maybe_start_io_workers(void)
{
	TimestampTz scheduled_at;

	while ((scheduled_at = maybe_start_io_workers_scheduled_at()) != 0)
	{
		TimestampTz now = GetCurrentTimestamp();
		PMChild    *child;
		int			i;

		Assert(pmState < PM_WAIT_IO_WORKERS);

		/* Still waiting for the scheduled time? */
		if (scheduled_at > now)
			break;

		/*
		 * Compute next launch time relative to the previous value, so that
		 * time spent on the postmaster's other duties don't result in an
		 * inaccurate launch interval.
		 */
		io_worker_launch_next_time =
			TimestampTzPlusMilliseconds(io_worker_launch_next_time,
										io_worker_launch_interval);

		/*
		 * If that's already in the past, the interval is either impossibly
		 * short or we received no requests for new workers for a period.
		 * Compute a new future time relative to now instead.
		 */
		if (io_worker_launch_next_time <= now)
			io_worker_launch_next_time =
				TimestampTzPlusMilliseconds(now, io_worker_launch_interval);

		/*
		 * Check if a grow signal has been received, but the grow request has
		 * been canceled since then because work ran out.  We've still
		 * advanced the next launch time, to suppress repeat signals from
		 * workers until then.
		 */
		if (io_worker_count >= io_min_workers && !pgaio_worker_pm_test_grow())
		{
			pgaio_worker_pm_clear_grow_signal_sent();
			break;
		}

		/* find unused entry in io_worker_children array */
		for (i = 0; i < MAX_IO_WORKERS; ++i)
		{
			if (io_worker_children[i] == NULL)
				break;
		}
		if (i == MAX_IO_WORKERS)
			elog(ERROR, "could not find a free IO worker slot");

		/* Try to launch one. */
		child = StartChildProcess(B_IO_WORKER);
		if (child != NULL)
		{
			io_worker_children[i] = child;
			++io_worker_count;
		}
		else
		{
			/*
			 * Fork failure: we'll try again after the launch interval
			 * expires, or be called again without delay if we don't yet have
			 * io_min_workers.  Don't loop here though, the postmaster has
			 * other duties.
			 */
			break;
		}
	}

	/*
	 * Workers decide when to shut down by themselves, according to the
	 * io_max_workers and io_worker_idle_timeout GUCs.
	 */
}


/*
 * When a backend asks to be notified about worker state changes, we
 * set a flag in its backend entry.  The background worker machinery needs
 * to know when such backends exit.
 */
bool
PostmasterMarkPIDForWorkerNotify(int pid)
{
	dlist_iter	iter;
	PMChild    *bp;

	dlist_foreach(iter, &ActiveChildList)
	{
		bp = dlist_container(PMChild, elem, iter.cur);
		if (PostmasterChildSignalPid(bp) == pid)
		{
			bp->bgworker_notify = true;
			return true;
		}
	}
	return false;
}

bool
PostmasterNotifyPIDForWorker(int pid)
{
	dlist_iter	iter;
	PMChild    *bp;

	dlist_foreach(iter, &ActiveChildList)
	{
		bp = dlist_container(PMChild, elem, iter.cur);
		if (PostmasterChildSignalPid(bp) != pid)
			continue;

		if (PostmasterChildHasLogicalBackendPublication(bp))
			return PostmasterChildWakeThreadBackend(bp);
		else if (kill(pid, SIGUSR1) < 0)
			return false;

		return true;
	}
	return false;
}

bool
PostmasterSignalPIDForWorker(int pid, int signal)
{
	dlist_iter	iter;
	PMChild    *bp;

	dlist_foreach(iter, &ActiveChildList)
	{
		bp = dlist_container(PMChild, elem, iter.cur);
		if (PostmasterChildSignalPid(bp) != pid)
			continue;

		signal_child(bp, signal);
		return true;
	}
	return false;
}

#ifdef WIN32

/*
 * Subset implementation of waitpid() for Windows.  We assume pid is -1
 * (that is, check all child processes) and options is WNOHANG (don't wait).
 */
static pid_t
waitpid(pid_t pid, int *exitstatus, int options)
{
	win32_deadchild_waitinfo *childinfo;
	DWORD		exitcode;
	DWORD		dwd;
	ULONG_PTR	key;
	OVERLAPPED *ovl;

	/* Try to consume one win32_deadchild_waitinfo from the queue. */
	if (!GetQueuedCompletionStatus(win32ChildQueue, &dwd, &key, &ovl, 0))
	{
		errno = EAGAIN;
		return -1;
	}

	childinfo = (win32_deadchild_waitinfo *) key;
	pid = childinfo->procId;

	/*
	 * Remove handle from wait - required even though it's set to wait only
	 * once
	 */
	UnregisterWaitEx(childinfo->waitHandle, NULL);

	if (!GetExitCodeProcess(childinfo->procHandle, &exitcode))
	{
		/*
		 * Should never happen. Inform user and set a fixed exitcode.
		 */
		write_stderr("could not read exit code for process\n");
		exitcode = 255;
	}
	*exitstatus = exitcode;

	/*
	 * Close the process handle.  Only after this point can the PID can be
	 * recycled by the kernel.
	 */
	CloseHandle(childinfo->procHandle);

	/*
	 * Free struct that was allocated before the call to
	 * RegisterWaitForSingleObject()
	 */
	pfree(childinfo);

	return pid;
}

/*
 * Note! Code below executes on a thread pool! All operations must
 * be thread safe! Note that elog() and friends must *not* be used.
 */
static void WINAPI
pgwin32_deadchild_callback(PVOID lpParameter, BOOLEAN TimerOrWaitFired)
{
	/* Should never happen, since we use INFINITE as timeout value. */
	if (TimerOrWaitFired)
		return;

	/*
	 * Post the win32_deadchild_waitinfo object for waitpid() to deal with. If
	 * that fails, we leak the object, but we also leak a whole process and
	 * get into an unrecoverable state, so there's not much point in worrying
	 * about that.  We'd like to panic, but we can't use that infrastructure
	 * from this thread.
	 */
	if (!PostQueuedCompletionStatus(win32ChildQueue,
									0,
									(ULONG_PTR) lpParameter,
									NULL))
		write_stderr("could not post child completion status\n");

	/* Queue SIGCHLD signal. */
	pg_queue_signal(SIGCHLD);
}

/*
 * Queue a waiter to signal when this child dies.  The wait will be handled
 * automatically by an operating system thread pool.  The memory and the
 * process handle will be freed by a later call to waitpid().
 */
void
pgwin32_register_deadchild_callback(HANDLE procHandle, DWORD procId)
{
	win32_deadchild_waitinfo *childinfo;

	childinfo = palloc_object(win32_deadchild_waitinfo);
	childinfo->procHandle = procHandle;
	childinfo->procId = procId;

	if (!RegisterWaitForSingleObject(&childinfo->waitHandle,
									 procHandle,
									 pgwin32_deadchild_callback,
									 childinfo,
									 INFINITE,
									 WT_EXECUTEONLYONCE | WT_EXECUTEINWAITTHREAD))
		ereport(FATAL,
				(errmsg_internal("could not register process for wait: error code %lu",
								 GetLastError())));
}

#endif							/* WIN32 */

/*
 * Initialize one and only handle for monitoring postmaster death.
 *
 * Called once in the postmaster, so that child processes can subsequently
 * monitor if their parent is dead.
 */
static void
InitPostmasterDeathWatchHandle(void)
{
#ifndef WIN32

	/*
	 * Create a pipe. Postmaster holds the write end of the pipe open
	 * (POSTMASTER_FD_OWN), and children hold the read end. Children can pass
	 * the read file descriptor to select() to wake up in case postmaster
	 * dies, or check for postmaster death with a (read() == 0). Children must
	 * close the write end as soon as possible after forking, because EOF
	 * won't be signaled in the read end until all processes have closed the
	 * write fd. That is taken care of in ClosePostmasterPorts().
	 */
	Assert(MyProcPid == PostmasterPid);
	if (pipe(postmaster_alive_fds) < 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg_internal("could not create pipe to monitor postmaster death: %m")));

	/* Notify fd.c that we've eaten two FDs for the pipe. */
	ReserveExternalFD();
	ReserveExternalFD();

	/*
	 * Set O_NONBLOCK to allow testing for the fd's presence with a read()
	 * call.
	 */
	if (fcntl(postmaster_alive_fds[POSTMASTER_FD_WATCH], F_SETFL, O_NONBLOCK) == -1)
		ereport(FATAL,
				(errcode_for_socket_access(),
				 errmsg_internal("could not set postmaster death monitoring pipe to nonblocking mode: %m")));
#else

	/*
	 * On Windows, we use a process handle for the same purpose.
	 */
	if (DuplicateHandle(GetCurrentProcess(),
						GetCurrentProcess(),
						GetCurrentProcess(),
						&PostmasterHandle,
						0,
						TRUE,
						DUPLICATE_SAME_ACCESS) == 0)
		ereport(FATAL,
				(errmsg_internal("could not duplicate postmaster handle: error code %lu",
								 GetLastError())));
#endif							/* WIN32 */
}
