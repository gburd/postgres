/*-------------------------------------------------------------------------
 *
 * logical_revert_worker.c
 *	  Background worker for timer-driven Logical Revert via ATM scan
 *
 * This worker periodically scans the ATM (Aborted Transaction Map) for
 * entries whose WAL-based UNDO chains have not yet been confirmed as applied.
 * For each unreverted entry whose database matches the worker's connected
 * database, the worker:
 *
 *   1. Applies the WAL-based UNDO chain via ApplyUndoChainFromWAL()
 *      (idempotent: CLR records prevent double-application)
 *   2. Marks the ATM entry as reverted via ATMMarkReverted()
 *   3. Emits XLOG_ATM_FORGET and removes the entry via ATMForget()
 *
 * Unlike event-driven UNDO worker variants (which process a shared memory work
 * queue), this worker is timer-driven: it sleeps for logical_revert_naptime
 * milliseconds between scan cycles.
 *
 * Shared memory: a single LogicalRevertState struct holds the LWLock
 * protecting the running flag and a counter for assigning worker IDs.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/undo/logical_revert_worker.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>

#include "access/atm.h"
#include "access/heapam.h"
#include "access/logical_revert_worker.h"
#include "access/table.h"
#include "access/undorecord.h"
#include "access/xact.h"
#include "access/xlogdefs.h"
#include "catalog/pg_database.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "tcop/tcopprot.h"
#include "utils/guc.h"

/* GUC parameter: sleep time between ATM scans in milliseconds */
int			logical_revert_naptime = 1000;

/*
 * Shared memory state for the Logical Revert worker.
 *
 * Minimal: just a lock and a worker-id counter. The ATM itself is the
 * "work queue" -- the worker reads it directly via ATMGetNextUnreverted().
 */
typedef struct LogicalRevertState
{
	LWLock		lock;
	int			next_worker_id;
}			LogicalRevertState;

static LogicalRevertState * RevertState = NULL;

/* Flags set by signal handlers */
static volatile sig_atomic_t got_SIGHUP = false;
static volatile sig_atomic_t got_SIGTERM = false;

/* Signal handlers */
static void logical_revert_sighup(SIGNAL_ARGS);
static void logical_revert_sigterm(SIGNAL_ARGS);
static void process_revert_entry(TransactionId xid, XLogRecPtr last_batch_lsn);

/*
 * LogicalRevertShmemSize
 *		Calculate shared memory space needed.
 */
Size
LogicalRevertShmemSize(void)
{
	return sizeof(LogicalRevertState);
}

/*
 * LogicalRevertShmemInit
 *		Allocate and initialize shared memory.
 */
void
LogicalRevertShmemInit(void)
{
	bool		found;

	RevertState = (LogicalRevertState *)
		ShmemInitStruct("Logical Revert Worker State",
						sizeof(LogicalRevertState),
						&found);

	if (!found)
	{
		LWLockInitialize(&RevertState->lock, LWTRANCHE_UNDO_WORKER);
		RevertState->next_worker_id = 1;
	}
}

/*
 * logical_revert_sighup
 *		SIGHUP signal handler -- reload configuration.
 */
static void
logical_revert_sighup(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_SIGHUP = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

/*
 * logical_revert_sigterm
 *		SIGTERM signal handler -- request shutdown.
 */
static void
logical_revert_sigterm(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_SIGTERM = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

/*
 * process_revert_entry
 *		Apply the WAL-based UNDO chain for a single ATM entry.
 *
 * Walks the UNDO chain from last_batch_lsn backward, applying each record.
 * CLR records are written during application so that crash recovery is
 * idempotent.  Returns silently if last_batch_lsn is invalid (nothing to do).
 */
static void
process_revert_entry(TransactionId xid, XLogRecPtr last_batch_lsn)
{
	if (!XLogRecPtrIsValid(last_batch_lsn))
		return;					/* nothing to apply */

	ereport(DEBUG1,
			(errmsg("logical revert: applying UNDO chain for xid %u "
					"from LSN %X/%X",
					xid, LSN_FORMAT_ARGS(last_batch_lsn))));

	ApplyUndoChainFromWAL(last_batch_lsn);
}

/*
 * LogicalRevertWorkerMain
 *		Main entry point for the Logical Revert background worker.
 *
 * The worker connects to a specific database, then loops: scan the ATM
 * for unreverted entries matching this database, apply them, mark done,
 * forget. Sleep when idle.
 */
void
LogicalRevertWorkerMain(Datum main_arg)
{
	Oid			dboid = DatumGetObjectId(main_arg);
	int			worker_id;

	/* Establish signal handlers */
	pqsignal(SIGHUP, logical_revert_sighup);
	pqsignal(SIGTERM, logical_revert_sigterm);

	BackgroundWorkerUnblockSignals();

	/* Connect to the target database */
	BackgroundWorkerInitializeConnectionByOid(dboid, InvalidOid, 0);

	/* Assign a worker ID */
	LWLockAcquire(&RevertState->lock, LW_EXCLUSIVE);
	worker_id = RevertState->next_worker_id++;
	LWLockRelease(&RevertState->lock);

	elog(LOG, "logical revert worker %d started for database %u",
		 worker_id, dboid);

	while (!got_SIGTERM)
	{
		TransactionId xid;
		Oid			entry_dboid;
		XLogRecPtr	last_batch_lsn;
		int			rc;

		/* Reload configuration on SIGHUP */
		if (got_SIGHUP)
		{
			got_SIGHUP = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		/* Scan ATM for the next unreverted entry */
		if (ATMGetNextUnreverted(&xid, &entry_dboid, &last_batch_lsn))
		{
			/*
			 * ATMGetNextUnreverted returns entries for any database. Skip
			 * entries that belong to a different database.
			 */
			if (entry_dboid != MyDatabaseId)
				goto sleep;

			StartTransactionCommand();

			PG_TRY();
			{
				process_revert_entry(xid, last_batch_lsn);

				/*
				 * Mark the ATM entry as reverted, then emit XLOG_ATM_FORGET
				 * and remove it from the ATM entirely.
				 */
				ATMMarkReverted(xid);
				ATMForget(xid);
			}
			PG_CATCH();
			{
				EmitErrorReport();
				FlushErrorState();

				elog(LOG, "logical revert worker: failed to revert xid %u, "
					 "will retry", xid);
			}
			PG_END_TRY();

			CommitTransactionCommand();

			/* Immediately look for more work instead of sleeping */
			continue;
		}

sleep:
		/* No work available, wait */
		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   logical_revert_naptime,
					   PG_WAIT_EXTENSION);

		ResetLatch(MyLatch);

		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);
	}

	elog(LOG, "logical revert worker %d shutting down", worker_id);
	proc_exit(0);
}

/*
 * StartLogicalRevertWorker
 *		Launch a logical revert worker for the specified database.
 *
 *	The worker uses a modest `bgw_restart_time` so that if it exits due
 *	to a transient error the postmaster auto-restarts it.  That removes
 *	the need for the launcher to re-spawn workers on every scan.
 */
void
StartLogicalRevertWorker(Oid dboid)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;

	memset(&worker, 0, sizeof(BackgroundWorker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = 60;
	sprintf(worker.bgw_library_name, "postgres");
	sprintf(worker.bgw_function_name, "LogicalRevertWorkerMain");
	snprintf(worker.bgw_name, BGW_MAXLEN,
			 "logical revert worker for database %u", dboid);
	snprintf(worker.bgw_type, BGW_MAXLEN, "logical revert worker");
	worker.bgw_main_arg = ObjectIdGetDatum(dboid);
	worker.bgw_notify_pid = MyProcPid;

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
	{
		ereport(WARNING,
				(errmsg("could not register logical revert worker for database %u",
						dboid)));
	}
	else
	{
		elog(DEBUG1, "started logical revert worker for database %u", dboid);
	}
}

/*
 * LogicalRevertLauncherMain
 *		Launcher background worker.
 *
 *	On startup: scan pg_database once and spawn a LogicalRevertWorker
 *	for every connectable database.  Then loop forever doing nothing
 *	meaningful — the per-db workers use `bgw_restart_time = 60` so the
 *	postmaster auto-restarts them if they exit, and the launcher does
 *	NOT re-spawn on every wake-up (the slot pool is small and eager
 *	re-spawning exhausts it).
 *
 *	A future iteration should watch for CREATE DATABASE / DROP DATABASE
 *	events and adjust the worker set accordingly; for now new databases
 *	get a worker only on the next server restart.
 */
void
LogicalRevertLauncherMain(Datum main_arg)
{
	bool		did_initial_scan = false;

	pqsignal(SIGHUP, logical_revert_sighup);
	pqsignal(SIGTERM, logical_revert_sigterm);
	BackgroundWorkerUnblockSignals();

	/*
	 * The launcher does not connect to any database itself.  It spawns
	 * per-db workers which do their own connection (via
	 * BackgroundWorkerInitializeConnectionByOid).  For the one-shot
	 * pg_database scan we connect to postgres (not template1, because
	 * holding a connection to template1 would block CREATE DATABASE).
	 */
	BackgroundWorkerInitializeConnection("postgres", NULL, 0);

	elog(LOG, "logical revert launcher started");

	while (!got_SIGTERM)
	{
		int			rc;

		if (got_SIGHUP)
		{
			got_SIGHUP = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		if (!did_initial_scan)
		{
			StartTransactionCommand();
			{
				Relation	pg_database;
				TableScanDesc scan;
				HeapTuple	tup;

				pg_database = table_open(DatabaseRelationId, AccessShareLock);
				scan = table_beginscan_catalog(pg_database, 0, NULL);
				while ((tup = heap_getnext(scan, ForwardScanDirection)) != NULL)
				{
					Form_pg_database db = (Form_pg_database) GETSTRUCT(tup);

					if (!db->datallowconn)
						continue;
					/*
					 * Skip template databases.  template1 / template0 are
					 * not expected to accumulate ATM entries, and having a
					 * revert-worker connection to template1 would block
					 * subsequent CREATE DATABASE commands that use it as
					 * the source template.
					 */
					if (strncmp(NameStr(db->datname), "template", 8) == 0)
						continue;
					StartLogicalRevertWorker(db->oid);
				}
				table_endscan(scan);
				table_close(pg_database, AccessShareLock);
			}
			CommitTransactionCommand();
			did_initial_scan = true;
			elog(LOG, "logical revert launcher: initial pg_database scan complete");
		}

		/*
		 * Sleep for a long interval.  The launcher's only remaining job is
		 * to stay alive so SIGTERM handling and SIGHUP config reloads can
		 * reach it; the per-db workers do the real work and auto-restart
		 * on their own.
		 */
		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   60000L,		/* 1 minute */
					   PG_WAIT_EXTENSION);

		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		ResetLatch(MyLatch);
	}

	elog(LOG, "logical revert launcher shutting down");
	proc_exit(0);
}

/*
 * LogicalRevertLauncherRegister
 *		Register the logical revert launcher as a static background worker.
 *
 *	Called from postmaster.c at startup, alongside ApplyLauncherRegister().
 *	Enabled only when enable_undo is on at server level; otherwise the
 *	launcher adds no useful work.
 */
void
LogicalRevertLauncherRegister(void)
{
	BackgroundWorker bgw;

	/* Disabled during binary upgrade and when UNDO is off cluster-wide. */
	if (IsBinaryUpgrade)
		return;

	memset(&bgw, 0, sizeof(bgw));
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
	bgw.bgw_restart_time = 5;
	snprintf(bgw.bgw_library_name, MAXPGPATH, "postgres");
	snprintf(bgw.bgw_function_name, BGW_MAXLEN, "LogicalRevertLauncherMain");
	snprintf(bgw.bgw_name, BGW_MAXLEN, "logical revert launcher");
	snprintf(bgw.bgw_type, BGW_MAXLEN, "logical revert launcher");
	bgw.bgw_notify_pid = 0;
	bgw.bgw_main_arg = (Datum) 0;

	RegisterBackgroundWorker(&bgw);
}
