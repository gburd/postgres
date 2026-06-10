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


PG_THREAD_LOCAL PG_GLOBAL_CONNECTION ProtocolVersion FrontendProtocol;

PG_THREAD_LOCAL PG_GLOBAL_BACKEND volatile sig_atomic_t InterruptPending = false;
PG_THREAD_LOCAL PG_GLOBAL_BACKEND volatile sig_atomic_t QueryCancelPending = false;
PG_THREAD_LOCAL PG_GLOBAL_BACKEND volatile sig_atomic_t ProcDiePending = false;
PG_THREAD_LOCAL PG_GLOBAL_CONNECTION volatile sig_atomic_t CheckClientConnectionPending = false;
PG_THREAD_LOCAL PG_GLOBAL_CONNECTION volatile sig_atomic_t ClientConnectionLost = false;
PG_THREAD_LOCAL PG_GLOBAL_BACKEND volatile sig_atomic_t IdleInTransactionSessionTimeoutPending = false;
PG_THREAD_LOCAL PG_GLOBAL_BACKEND volatile sig_atomic_t TransactionTimeoutPending = false;
PG_THREAD_LOCAL PG_GLOBAL_BACKEND volatile sig_atomic_t IdleSessionTimeoutPending = false;
PG_THREAD_LOCAL PG_GLOBAL_BACKEND volatile sig_atomic_t ProcSignalBarrierPending = false;
PG_THREAD_LOCAL PG_GLOBAL_BACKEND volatile sig_atomic_t LogMemoryContextPending = false;
PG_THREAD_LOCAL PG_GLOBAL_BACKEND volatile sig_atomic_t IdleStatsUpdateTimeoutPending = false;
PG_THREAD_LOCAL PG_GLOBAL_BACKEND volatile uint32 InterruptHoldoffCount = 0;
PG_THREAD_LOCAL PG_GLOBAL_BACKEND volatile uint32 QueryCancelHoldoffCount = 0;
PG_THREAD_LOCAL PG_GLOBAL_BACKEND volatile uint32 CritSectionCount = 0;
PG_THREAD_LOCAL PG_GLOBAL_BACKEND volatile int ProcDieSenderPid = 0;
PG_THREAD_LOCAL PG_GLOBAL_BACKEND volatile int ProcDieSenderUid = 0;

PG_THREAD_LOCAL PG_GLOBAL_BACKEND int MyProcPid;
PG_THREAD_LOCAL PG_GLOBAL_BACKEND pg_time_t MyStartTime;
PG_THREAD_LOCAL PG_GLOBAL_BACKEND TimestampTz MyStartTimestamp;
PG_THREAD_LOCAL PG_GLOBAL_CONNECTION struct ClientSocket *MyClientSocket;
PG_THREAD_LOCAL PG_GLOBAL_CONNECTION struct Port *MyProcPort;
PG_THREAD_LOCAL PG_GLOBAL_CONNECTION uint8 MyCancelKey[MAX_CANCEL_KEY_LENGTH];
PG_THREAD_LOCAL PG_GLOBAL_CONNECTION int MyCancelKeyLength = 0;
PG_THREAD_LOCAL PG_GLOBAL_BACKEND int MyPMChildSlot;

/*
 * MyLatch points to the latch that should be used for signal handling by the
 * current process. It will either point to a process local latch if the
 * current process does not have a PGPROC entry in that moment, or to
 * PGPROC->procLatch if it has. Thus it can always be used in signal handlers,
 * without checking for its existence.
 */
PG_THREAD_LOCAL PG_GLOBAL_BACKEND struct Latch *MyLatch;

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

PG_THREAD_LOCAL PG_GLOBAL_BACKEND char OutputFileName[MAXPGPATH];	/* debugging output file */

PG_GLOBAL_RUNTIME char my_exec_path[MAXPGPATH];	/* full path to my executable */
PG_GLOBAL_RUNTIME char pkglib_path[MAXPGPATH]; /* full path to lib directory */

#ifdef EXEC_BACKEND
PG_GLOBAL_RUNTIME char postgres_exec_path[MAXPGPATH];	/* full path to backend */

/* note: currently this is not valid in backend processes */
#endif

PG_THREAD_LOCAL PG_GLOBAL_BACKEND ProcNumber MyProcNumber = INVALID_PROC_NUMBER;

PG_THREAD_LOCAL PG_GLOBAL_BACKEND ProcNumber ParallelLeaderProcNumber = INVALID_PROC_NUMBER;

PG_THREAD_LOCAL PG_GLOBAL_SESSION Oid MyDatabaseId = InvalidOid;

PG_THREAD_LOCAL PG_GLOBAL_SESSION Oid MyDatabaseTableSpace = InvalidOid;

PG_THREAD_LOCAL PG_GLOBAL_SESSION bool MyDatabaseHasLoginEventTriggers = false;

/*
 * DatabasePath is the path (relative to DataDir) of my database's
 * primary directory, ie, its directory in the default tablespace.
 */
PG_THREAD_LOCAL PG_GLOBAL_SESSION char *DatabasePath = NULL;

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
PG_GLOBAL_RUNTIME bool IsUnderPostmaster = false;
PG_GLOBAL_RUNTIME bool IsBinaryUpgrade = false;

PG_THREAD_LOCAL PG_GLOBAL_BACKEND bool ExitOnAnyError = false;

PG_THREAD_LOCAL PG_GLOBAL_SESSION int DateStyle = USE_ISO_DATES;
PG_THREAD_LOCAL PG_GLOBAL_SESSION int DateOrder = DATEORDER_MDY;
PG_THREAD_LOCAL PG_GLOBAL_SESSION int IntervalStyle = INTSTYLE_POSTGRES;

PG_GLOBAL_RUNTIME bool enableFsync = true;
PG_THREAD_LOCAL PG_GLOBAL_SESSION bool allowSystemTableMods = false;
PG_THREAD_LOCAL PG_GLOBAL_SESSION int work_mem = 4096;
PG_THREAD_LOCAL PG_GLOBAL_SESSION double hash_mem_multiplier = 2.0;
PG_THREAD_LOCAL PG_GLOBAL_SESSION int maintenance_work_mem = 65536;
PG_THREAD_LOCAL PG_GLOBAL_SESSION int max_parallel_maintenance_workers = 2;

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
