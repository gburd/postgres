/*-------------------------------------------------------------------------
 *
 * globals.c
 *	  global variable declarations
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/init/globals.c
 *
 * NOTES
 *	  Globals used all over the place should be declared here and not
 *	  in other modules.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "common/file_perm.h"
#include "libpq/libpq-be.h"
#include "libpq/pqcomm.h"
#include "miscadmin.h"
#include "postmaster/postmaster.h"
#include "storage/procnumber.h"
#include "storage/procsignal.h"

/*
 * DataDir is the absolute path to the top level of the PGDATA directory tree.
 * Except during early startup, this is also the server's working directory;
 * most code therefore can simply use relative paths and not reference DataDir
 * explicitly.
 */
PG_GLOBAL_RUNTIME char *DataDir = NULL;

/*
 * Mode of the data directory.  The default is 0700 but it may be changed in
 * checkDataDir() to 0750 if the data directory actually has that mode.
 */
PG_GLOBAL_RUNTIME int data_directory_mode = PG_DIR_MODE_OWNER;

PG_GLOBAL_RUNTIME char my_exec_path[MAXPGPATH];	/* full path to my executable */
PG_GLOBAL_RUNTIME char pkglib_path[MAXPGPATH]; /* full path to lib directory */

#ifdef FORKEXEC_BACKEND
PG_GLOBAL_RUNTIME char postgres_exec_path[MAXPGPATH];	/* full path to backend */

/* note: currently this is not valid in backend processes */
#endif

PG_GLOBAL_RUNTIME pid_t PostmasterPid = 0;

/*
 * IsPostmasterEnvironment is true in a postmaster process and any postmaster
 * child process; it is false in a standalone process (bootstrap or
 * standalone backend).  IsUnderPostmaster is true in postmaster child
 * processes.  Note that "child process" includes all children, not only
 * regular backends.  These should be set correctly as early as possible
 * in the execution of a process, so that error handling will do the right
 * things if an error should occur during process initialization.
 *
 * These are initialized for the bootstrap/standalone case.
 */
PG_GLOBAL_RUNTIME bool IsPostmasterEnvironment = false;
PG_GLOBAL_RUNTIME bool IsBinaryUpgrade = false;

PG_GLOBAL_RUNTIME bool enableFsync = true;

/*
 * Primary determinants of sizes of shared-memory structures.
 *
 * MaxBackends is computed by PostmasterMain after modules have had a chance to
 * register background workers.
 */
PG_GLOBAL_RUNTIME int NBuffers = 16384;
PG_GLOBAL_RUNTIME int MaxConnections = 100;
PG_GLOBAL_RUNTIME int max_worker_processes = 8;
PG_GLOBAL_RUNTIME int max_parallel_workers = 8;
PG_GLOBAL_RUNTIME int autovacuum_max_parallel_workers = 0;
PG_GLOBAL_RUNTIME int MaxBackends = 0;
PG_GLOBAL_RUNTIME bool multithreaded = false;
PG_GLOBAL_RUNTIME bool xtc_force_process_fallback = false;
/*
 * Fusion F0a: when true (and multithreaded=on), route libxtc's internal
 * diagnostics (the host-tuning advisor via xtc_runtime_tuning_check -- CPU governor,
 * intel_pstate, THP, swappiness, io_uring probes) into the server log instead
 * of libxtc's default stderr.  Developer knob; default off.  Read once at
 * carrier bringup; a plain postmaster-wide global.
 */
bool		xtc_log_to_server = false;

/*
 * F4 supervision-fusion: when true (and multithreaded=on), stand up one libxtc
 * xtc_orc OTP supervisor per carrier loop alongside the hand-rolled per-loop
 * supervisor fiber.  The hand-rolled supervisor stays the source of truth for
 * backend-fiber crash detection and Stage-1b escalation; see the long comment
 * at xtc_carrier_start_orc_supervisors() in pg_xtc_carrier.c for why an
 * orc-owned backend child cannot carry escalation with today's libxtc.
 * Developer knob; default off.  Read once at carrier bringup; a plain
 * postmaster-wide global, like xtc_log_to_server.
 */
bool		xtc_carrier_use_orc = false;

/*
 * True in a backend that was started via fork()+exec() (arriving through
 * SubPostmasterMain) and therefore did NOT inherit the postmaster's address
 * space -- it must re-attach shared memory / re-derive backend-local state.
 * Under a plain EXEC_BACKEND build every child is exec'd, so this is always
 * true there (see the PG_BACKEND_WAS_FORKEXECED macro in miscadmin.h).  Under
 * USE_XTC_PROCESS_FALLBACK only the process-fallback backend is exec'd, so the
 * flag distinguishes it from normally-forked children (aux procs, carriers'
 * host), which DO inherit and must NOT re-attach.
 */
PG_GLOBAL_RUNTIME bool pg_backend_was_forkexeced = false;
PG_GLOBAL_RUNTIME bool threaded_lazy_relcache_init_file = true;

/*
 * Developer-only DEMONSTRATION knob (default off): when on in threaded mode,
 * the carrier runs a self-contained xtc_xproc cross-fork watchdog demo at
 * carrier start (pg_xtc_xproc_demo.c).  It is a demo of the restart-after-
 * crash USAGE pattern only; the real PG backend crash policy stays fail-stop
 * and this knob never touches it.  Off => zero production impact.
 */
PG_GLOBAL_RUNTIME bool xtc_xproc_watchdog_demo = false;

/* configurable SLRU buffer sizes */
PG_GLOBAL_RUNTIME int commit_timestamp_buffers = 0;
PG_GLOBAL_RUNTIME int multixact_member_buffers = 32;
PG_GLOBAL_RUNTIME int multixact_offset_buffers = 16;
PG_GLOBAL_RUNTIME int notify_buffers = 16;
PG_GLOBAL_RUNTIME int serializable_buffers = 32;
PG_GLOBAL_RUNTIME int subtransaction_buffers = 0;
PG_GLOBAL_RUNTIME int transaction_buffers = 0;
