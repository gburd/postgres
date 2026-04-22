/*-------------------------------------------------------------------------
 *
 * logical_revert_worker.c
 *	  Background worker for timer-driven Logical Revert via ATM scan
 *
 * This worker periodically scans the ATM (Aborted Transaction Map) for
 * entries whose per-relation UNDO chains have not yet been applied. For
 * each unreverted entry whose database matches the worker's connected
 * database, the worker:
 *
 *   1. Opens the target relation with AccessExclusiveLock
 *   2. Applies the UNDO chain via RelUndoApplyChain()
 *   3. Marks the ATM entry as reverted via ATMMarkReverted()
 *   4. Emits XLOG_ATM_FORGET and removes the entry via ATMForget()
 *
 * Unlike relundo_worker.c (event-driven, processes a shared memory work
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
#include "access/logical_revert_worker.h"
#include "access/recno_slog.h"
#include "access/relundo.h"
#include "access/table.h"
#include "access/xact.h"
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
 * "work queue" — the worker reads it directly via ATMGetNextUnreverted().
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
static void process_revert_entry(Relation rel, RelUndoRecPtr chain,
								 TransactionId xid);

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
 *		SIGHUP signal handler — reload configuration.
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
 *		SIGTERM signal handler — request shutdown.
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
 *		Apply the UNDO chain for a single ATM entry.
 *
 * On success the relation's UNDO chain has been fully applied and CLR
 * records written. On failure (e.g. relation dropped) we log the error
 * and continue so the ATM entry can still be cleaned up.
 */
static void
process_revert_entry(Relation rel, RelUndoRecPtr chain, TransactionId xid)
{
	elog(LOG, "logical revert: applying UNDO chain for xid %u, relation %u",
		 xid, RelationGetRelid(rel));

	RelUndoApplyChain(rel, chain);

	/*
	 * Clean up ABORTED sLog entries for this transaction now that the tuples
	 * have been physically restored.
	 */
	RecnoSLogRemoveByXidGlobal(xid);

	elog(LOG, "logical revert: completed UNDO chain for xid %u", xid);
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
		Oid			reloid;
		RelUndoRecPtr chain;
		int			rc;

		/* Reload configuration on SIGHUP */
		if (got_SIGHUP)
		{
			got_SIGHUP = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		/* Scan ATM for the next unreverted entry */
		if (ATMGetNextUnreverted(&xid, &entry_dboid, &reloid, &chain))
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
				Relation	rel;

				rel = table_open(reloid, AccessExclusiveLock);

				process_revert_entry(rel, chain, xid);

				table_close(rel, AccessExclusiveLock);

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

				elog(LOG, "logical revert worker: failed to revert xid %u "
					 "relation %u, will retry", xid, reloid);
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
	worker.bgw_restart_time = BGW_NEVER_RESTART;
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
