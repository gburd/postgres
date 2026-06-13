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

#ifdef EXEC_BACKEND
PG_GLOBAL_RUNTIME char postgres_exec_path[MAXPGPATH];	/* full path to backend */

/* note: currently this is not valid in backend processes */
#endif

PG_THREAD_LOCAL PG_GLOBAL_BACKEND ProcNumber MyProcNumber = INVALID_PROC_NUMBER;

PG_THREAD_LOCAL PG_GLOBAL_BACKEND ProcNumber ParallelLeaderProcNumber = INVALID_PROC_NUMBER;

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
PG_THREAD_LOCAL PG_GLOBAL_CARRIER bool IsUnderPostmaster = false;
PG_GLOBAL_RUNTIME bool IsBinaryUpgrade = false;

PG_GLOBAL_RUNTIME bool enableFsync = true;
PG_THREAD_LOCAL PG_GLOBAL_SESSION bool allowSystemTableMods = false;

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

/* GUC parameters for vacuum */
PG_THREAD_LOCAL PG_GLOBAL_SESSION int VacuumBufferUsageLimit = 2048;

PG_THREAD_LOCAL PG_GLOBAL_SESSION int VacuumCostPageHit = 1;
PG_THREAD_LOCAL PG_GLOBAL_SESSION int VacuumCostPageMiss = 2;
PG_THREAD_LOCAL PG_GLOBAL_SESSION int VacuumCostPageDirty = 20;
PG_THREAD_LOCAL PG_GLOBAL_SESSION int VacuumCostLimit = 200;
PG_THREAD_LOCAL PG_GLOBAL_SESSION double VacuumCostDelay = 0;

PG_THREAD_LOCAL PG_GLOBAL_EXECUTION int VacuumCostBalance = 0;	/* working state for vacuum */
PG_THREAD_LOCAL PG_GLOBAL_EXECUTION bool VacuumCostActive = false;

/* configurable SLRU buffer sizes */
PG_GLOBAL_RUNTIME int commit_timestamp_buffers = 0;
PG_GLOBAL_RUNTIME int multixact_member_buffers = 32;
PG_GLOBAL_RUNTIME int multixact_offset_buffers = 16;
PG_GLOBAL_RUNTIME int notify_buffers = 16;
PG_GLOBAL_RUNTIME int serializable_buffers = 32;
PG_GLOBAL_RUNTIME int subtransaction_buffers = 0;
PG_GLOBAL_RUNTIME int transaction_buffers = 0;
