/*-------------------------------------------------------------------------
 *
 * relundo_worker.c
 *	  Background worker for applying per-relation UNDO records asynchronously
 *
 * This module implements the async per-relation UNDO worker system that
 * applies UNDO records for aborted transactions. Workers run in background
 * processes to avoid blocking ROLLBACK commands with synchronous UNDO
 * application.
 *
 * The system consists of:
 * 1. A launcher process that manages the worker pool
 * 2. Individual worker processes that apply UNDO chains
 * 3. A shared memory work queue for coordinating pending work
 *
 * Architecture matches autovacuum: launcher spawns workers as needed,
 * workers process work items, communicate via shared memory.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/undo/relundo_worker.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>
#include <unistd.h>

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/relundo_worker.h"
#include "access/xact.h"
#include "access/relundo.h"
#include "access/table.h"
#include "access/tableam.h"
#include "catalog/pg_class.h"
#include "common/relpath.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "storage/bufmgr.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/smgr.h"
#include "tcop/tcopprot.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/rel.h"
#include "utils/timestamp.h"

/* GUC parameters */
int			max_relundo_workers = 3;
int			relundo_worker_naptime = 5000;	/* milliseconds */

/* Shared memory state */
static RelUndoWorkQueue *WorkQueue = NULL;

/* Flags set by signal handlers */
static volatile sig_atomic_t got_SIGHUP = false;
static volatile sig_atomic_t got_SIGTERM = false;

/* Forward declarations */
static void relundo_worker_sighup(SIGNAL_ARGS);
static void relundo_worker_sigterm(SIGNAL_ARGS);
static void process_relundo_work_item(RelUndoWorkItem *item);

/*
 * RelUndoWorkerShmemSize
 *		Calculate shared memory space needed for per-relation UNDO workers
 */
Size
RelUndoWorkerShmemSize(void)
{
	Size		size = 0;

	size = add_size(size, sizeof(RelUndoWorkQueue));
	return size;
}

/*
 * RelUndoWorkerShmemInit
 *		Allocate and initialize shared memory for per-relation UNDO workers
 */
void
RelUndoWorkerShmemInit(void)
{
	bool		found;

	WorkQueue = (RelUndoWorkQueue *)
		ShmemInitStruct("Per-Relation UNDO Work Queue",
						sizeof(RelUndoWorkQueue),
						&found);

	if (!found)
	{
		/* First time through, initialize the work queue */
		LWLockInitialize(&WorkQueue->lock, LWTRANCHE_UNDO_WORKER);
		WorkQueue->num_items = 0;
		WorkQueue->next_worker_id = 1;
		memset(WorkQueue->items, 0, sizeof(WorkQueue->items));
	}
}

/*
 * RelUndoQueueAdd
 *		Add a new per-relation UNDO work item to the queue
 *
 * Called during transaction abort to queue UNDO application work for
 * background workers.
 */
void
RelUndoQueueAdd(Oid dboid, Oid reloid, RelUndoRecPtr start_urec_ptr,
				TransactionId xid)
{
	int			i;
	bool		found_slot = false;

	LWLockAcquire(&WorkQueue->lock, LW_EXCLUSIVE);

	/* Check if we already have work for this relation */
	for (i = 0; i < WorkQueue->num_items; i++)
	{
		RelUndoWorkItem *item = &WorkQueue->items[i];

		if (item->dboid == dboid && item->reloid == reloid)
		{
			/* Update existing entry with latest UNDO pointer */
			item->start_urec_ptr = start_urec_ptr;
			item->xid = xid;
			item->queued_at = GetCurrentTimestamp();
			found_slot = true;
			break;
		}
	}

	if (!found_slot)
	{
		RelUndoWorkItem *item;

		/* Add new work item */
		if (WorkQueue->num_items >= MAX_UNDO_WORK_ITEMS)
		{
			LWLockRelease(&WorkQueue->lock);
			ereport(WARNING,
					(errmsg("Per-relation UNDO work queue is full, cannot queue work for relation %u",
							reloid)));
			return;
		}

		item = &WorkQueue->items[WorkQueue->num_items];
		item->dboid = dboid;
		item->reloid = reloid;
		item->start_urec_ptr = start_urec_ptr;
		item->xid = xid;
		item->queued_at = GetCurrentTimestamp();
		item->in_progress = false;
		item->worker_id = 0;
		WorkQueue->num_items++;
	}

	LWLockRelease(&WorkQueue->lock);

	elog(DEBUG1, "Queued per-relation UNDO work for database %u, relation %u (ptr=%lu)",
		 dboid, reloid, (unsigned long) start_urec_ptr);
}

/*
 * RelUndoQueueGetNext
 *		Get the next work item for a worker to process
 *
 * Returns true if work was found, false if queue is empty.
 * Marks the item as in_progress to prevent other workers from taking it.
 */
bool
RelUndoQueueGetNext(RelUndoWorkItem *item_out, int worker_id)
{
	int			i;
	bool		found = false;

	LWLockAcquire(&WorkQueue->lock, LW_EXCLUSIVE);

	for (i = 0; i < WorkQueue->num_items; i++)
	{
		RelUndoWorkItem *item = &WorkQueue->items[i];

		if (!item->in_progress && item->dboid == MyDatabaseId)
		{
			/* Found work for this database */
			memcpy(item_out, item, sizeof(RelUndoWorkItem));
			item->in_progress = true;
			item->worker_id = worker_id;
			found = true;
			break;
		}
	}

	LWLockRelease(&WorkQueue->lock);

	return found;
}

/*
 * RelUndoQueueMarkComplete
 *		Mark a work item as complete and remove it from the queue
 */
void
RelUndoQueueMarkComplete(Oid dboid, Oid reloid, int worker_id)
{
	int			i,
				j;

	LWLockAcquire(&WorkQueue->lock, LW_EXCLUSIVE);

	for (i = 0; i < WorkQueue->num_items; i++)
	{
		RelUndoWorkItem *item = &WorkQueue->items[i];

		if (item->dboid == dboid && item->reloid == reloid &&
			item->worker_id == worker_id)
		{
			/* Found the item, remove it by shifting remaining items */
			for (j = i; j < WorkQueue->num_items - 1; j++)
			{
				memcpy(&WorkQueue->items[j], &WorkQueue->items[j + 1],
					   sizeof(RelUndoWorkItem));
			}
			WorkQueue->num_items--;
			break;
		}
	}

	LWLockRelease(&WorkQueue->lock);

	elog(DEBUG1, "Completed per-relation UNDO work for database %u, relation %u",
		 dboid, reloid);
}

/*
 * relundo_worker_sighup
 *		SIGHUP signal handler for per-relation UNDO worker
 */
static void
relundo_worker_sighup(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_SIGHUP = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

/*
 * relundo_worker_sigterm
 *		SIGTERM signal handler for per-relation UNDO worker
 */
static void
relundo_worker_sigterm(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_SIGTERM = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

/*
 * process_relundo_work_item
 *		Apply per-relation UNDO records for a single work item
 */
static void
process_relundo_work_item(RelUndoWorkItem *item)
{
	Relation	rel;

	elog(LOG, "Per-relation UNDO worker processing: database %u, relation %u, UNDO ptr %lu",
		 item->dboid, item->reloid, (unsigned long) item->start_urec_ptr);

	/*
	 * Open the relation with RowExclusiveLock, the same lock level used by
	 * normal DML.  The UNDO apply modifies individual tuples and does not
	 * need to block concurrent readers or writers at the relation level.
	 *
	 * Previously this used AccessExclusiveLock, which created lock convoys
	 * under high concurrency: the UNDO worker's exclusive lock request would
	 * queue behind active transactions, and all new transactions would queue
	 * behind the UNDO worker, causing a complete stall.
	 */
	PG_TRY();
	{
		rel = table_open(item->reloid, RowExclusiveLock);

		/* Apply the UNDO chain */
		RelUndoApplyChain(rel, item->start_urec_ptr);

		/*
		 * Clean up any ABORTED sLog entries for this transaction.  At abort
		 * time, sLog entries were marked ABORTED (not removed) so visibility
		 * checks could detect aborted-but-not-yet-undone inserts.  Now that
		 * the tuples are physically restored, remove those entries.
		 */
		if (RelUndoAbortCleanup_hook)
			RelUndoAbortCleanup_hook(item->xid);

		table_close(rel, RowExclusiveLock);
	}
	PG_CATCH();
	{
		/*
		 * If relation was dropped or doesn't exist, that's OK - nothing to
		 * do. Just log it and move on.
		 */
		EmitErrorReport();
		FlushErrorState();

		elog(LOG, "Per-relation UNDO worker: failed to process relation %u, skipping",
			 item->reloid);
	}
	PG_END_TRY();
}

/*
 * RelUndoWorkerMain
 *		Main entry point for per-relation UNDO worker process
 */
void
RelUndoWorkerMain(Datum main_arg)
{
	Oid			dboid = DatumGetObjectId(main_arg);
	int			worker_id;

	/* Establish signal handlers */
	pqsignal(SIGHUP, relundo_worker_sighup);
	pqsignal(SIGTERM, relundo_worker_sigterm);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Connect to the specified database */
	BackgroundWorkerInitializeConnectionByOid(dboid, InvalidOid, 0);

	/* Get a worker ID */
	LWLockAcquire(&WorkQueue->lock, LW_EXCLUSIVE);
	worker_id = WorkQueue->next_worker_id++;
	LWLockRelease(&WorkQueue->lock);

	elog(LOG, "Per-relation UNDO worker %d started for database %u", worker_id, dboid);

	/* Main work loop */
	while (!got_SIGTERM)
	{
		RelUndoWorkItem item;

		/* Handle SIGHUP - reload configuration */
		if (got_SIGHUP)
		{
			got_SIGHUP = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		/* Check for UNDO chain application work */
		if (RelUndoQueueGetNext(&item, worker_id))
		{
			/* Start a transaction for applying UNDO */
			StartTransactionCommand();

			/* Process the work item */
			process_relundo_work_item(&item);

			/* Mark as complete */
			RelUndoQueueMarkComplete(item.dboid, item.reloid, worker_id);

			/* Commit the transaction */
			CommitTransactionCommand();
		}
		else
		{
			/*
			 * No UNDO chain work available, so exit.  The worker is
			 * registered with BGW_NEVER_RESTART so once the queue is drained
			 * it should not linger -- the aborting backend may be waiting for
			 * us via WaitForBackgroundWorkerShutdown().
			 */
			break;				/* exit the main loop */
		}
	}

	elog(LOG, "Per-relation UNDO worker %d shutting down", worker_id);
	proc_exit(0);
}

/*
 * Per-database worker tracking in the launcher.
 *
 * The launcher maintains a local array of per-database worker slots.
 * Each slot records the database OID and the background worker handle
 * returned by RegisterDynamicBackgroundWorker().  When a worker exits,
 * the slot is freed for reuse.
 */
#define MAX_LAUNCHER_DB_SLOTS	MAX_UNDO_WORK_ITEMS

typedef struct LauncherDbSlot
{
	Oid			dboid;			/* Database OID, or InvalidOid if free */
	BackgroundWorkerHandle *handle; /* Worker handle (NULL if slot is free) */
	TimestampTz last_spawn_attempt; /* 0 = never attempted */
} LauncherDbSlot;

/*
 * launcher_spawn_worker
 *		Spawn a per-relation UNDO worker for the given database.
 *
 * Returns true on success, false if RegisterDynamicBackgroundWorker fails
 * (e.g., because the max_worker_processes limit was reached).
 */
static bool
launcher_spawn_worker(Oid dboid, BackgroundWorkerHandle **handle_out)
{
	BackgroundWorker worker;

	memset(&worker, 0, sizeof(BackgroundWorker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	sprintf(worker.bgw_library_name, "postgres");
	sprintf(worker.bgw_function_name, "RelUndoWorkerMain");
	snprintf(worker.bgw_name, BGW_MAXLEN,
			 "per-relation undo worker for database %u", dboid);
	snprintf(worker.bgw_type, BGW_MAXLEN, "per-relation undo worker");
	worker.bgw_main_arg = ObjectIdGetDatum(dboid);
	worker.bgw_notify_pid = 0;	/* launcher does not need SIGUSR1 */

	if (!RegisterDynamicBackgroundWorker(&worker, handle_out))
	{
		ereport(DEBUG1,
				(errmsg("per-relation UNDO launcher: could not register worker for database %u",
						dboid)));
		return false;
	}

	elog(DEBUG1, "per-relation UNDO launcher: spawned worker for database %u",
		 dboid);
	return true;
}

/*
 * RelUndoLauncherMain
 *		Main entry point for per-relation UNDO launcher process
 *
 * The launcher periodically scans the shared work queue for databases
 * that have pending UNDO work and spawns per-database worker processes
 * as needed, up to max_relundo_workers total.
 */
void
RelUndoLauncherMain(Datum main_arg)
{
	LauncherDbSlot db_slots[MAX_LAUNCHER_DB_SLOTS];
	int			nslots = 0;

	/* Establish signal handlers */
	pqsignal(SIGHUP, relundo_worker_sighup);
	pqsignal(SIGTERM, relundo_worker_sigterm);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	elog(LOG, "Per-relation UNDO launcher started");

	memset(db_slots, 0, sizeof(db_slots));

	/* Main monitoring loop */
	while (!got_SIGTERM)
	{
		int			rc;
		int			active_count;
		int			i,
					j;
		Oid			pending_dbs[MAX_UNDO_WORK_ITEMS];
		int			npending = 0;

		/* Handle SIGHUP - reload configuration */
		if (got_SIGHUP)
		{
			got_SIGHUP = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		/*
		 * Step 1: Check existing worker handles to see which are still alive.
		 * Workers that have exited (BGWH_STOPPED) are freed.
		 */
		active_count = 0;
		for (i = 0; i < nslots; i++)
		{
			if (db_slots[i].dboid == InvalidOid)
				continue;

			if (db_slots[i].handle != NULL)
			{
				pid_t		pid;
				BgwHandleStatus status;

				status = GetBackgroundWorkerPid(db_slots[i].handle, &pid);
				if (status == BGWH_STOPPED || status == BGWH_POSTMASTER_DIED)
				{
					/*
					 * Worker has exited.  Reset any items that were marked
					 * in_progress for this database so they can be retried.
					 */
					LWLockAcquire(&WorkQueue->lock, LW_EXCLUSIVE);
					for (j = 0; j < WorkQueue->num_items; j++)
					{
						if (WorkQueue->items[j].dboid == db_slots[i].dboid &&
							WorkQueue->items[j].in_progress)
							WorkQueue->items[j].in_progress = false;
					}
					LWLockRelease(&WorkQueue->lock);

					{
						Oid			exited_dboid = db_slots[i].dboid;

						pfree(db_slots[i].handle);
						db_slots[i].handle = NULL;
						db_slots[i].dboid = InvalidOid;

						elog(DEBUG1,
							 "per-relation UNDO launcher: worker for database %u has exited",
							 exited_dboid);
					}
					continue;
				}

				active_count++;
			}
		}

		/* Compact the slots array to remove freed entries */
		nslots = 0;
		for (i = 0; i < MAX_LAUNCHER_DB_SLOTS; i++)
		{
			if (db_slots[i].dboid != InvalidOid)
				nslots = i + 1;
		}

		/*
		 * Step 2: Scan the work queue for databases with pending (not yet
		 * in_progress) work items that do not already have an active worker.
		 */
		LWLockAcquire(&WorkQueue->lock, LW_SHARED);
		for (i = 0; i < WorkQueue->num_items; i++)
		{
			RelUndoWorkItem *item = &WorkQueue->items[i];
			bool		has_worker;
			bool		already_listed;

			if (item->in_progress)
				continue;		/* someone is working on this */

			/* Check if we already noted this database needs a worker */
			already_listed = false;
			for (j = 0; j < npending; j++)
			{
				if (pending_dbs[j] == item->dboid)
				{
					already_listed = true;
					break;
				}
			}
			if (already_listed)
				continue;

			/* Check if there is already an active worker for this database */
			has_worker = false;
			for (j = 0; j < MAX_LAUNCHER_DB_SLOTS; j++)
			{
				if (db_slots[j].dboid == item->dboid &&
					db_slots[j].handle != NULL)
				{
					has_worker = true;
					break;
				}
			}

			if (!has_worker)
			{
				if (npending < MAX_UNDO_WORK_ITEMS)
					pending_dbs[npending++] = item->dboid;
			}
		}
		LWLockRelease(&WorkQueue->lock);

		/*
		 * Step 3: Spawn workers for databases that need them, up to the
		 * max_relundo_workers limit.
		 */
		for (i = 0; i < npending; i++)
		{
			Oid			dboid = pending_dbs[i];
			int			free_slot = -1;
			BackgroundWorkerHandle *handle;

			if (active_count >= max_relundo_workers)
				break;			/* at the worker limit */

			/* Find a free slot in db_slots */
			for (j = 0; j < MAX_LAUNCHER_DB_SLOTS; j++)
			{
				if (db_slots[j].dboid == InvalidOid)
				{
					free_slot = j;
					break;
				}
			}
			if (free_slot < 0)
				break;			/* no free slots (shouldn't happen) */

			/*
			 * Back off if we recently failed to spawn a worker for this slot.
			 * This prevents hammering RegisterDynamicBackgroundWorker() when
			 * worker slots are exhausted.
			 */
			if (db_slots[free_slot].last_spawn_attempt != 0)
			{
				TimestampTz now = GetCurrentTimestamp();

				if (now - db_slots[free_slot].last_spawn_attempt <
					(TimestampTz) relundo_worker_naptime * 4 * 1000)
					continue;	/* too soon, skip this database for now */
			}

			if (launcher_spawn_worker(dboid, &handle))
			{
				db_slots[free_slot].dboid = dboid;
				db_slots[free_slot].handle = handle;
				db_slots[free_slot].last_spawn_attempt = 0;
				if (free_slot >= nslots)
					nslots = free_slot + 1;
				active_count++;
			}
			else
			{
				db_slots[free_slot].last_spawn_attempt = GetCurrentTimestamp();
			}
		}

		/* Sleep until next check or until woken */
		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   relundo_worker_naptime * 2,
					   PG_WAIT_EXTENSION);

		ResetLatch(MyLatch);

		/* Emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);
	}

	elog(LOG, "Per-relation UNDO launcher shutting down");
	proc_exit(0);
}

/*
 * Saved background worker handle for the most recent synchronous UNDO
 * worker.  WaitForPendingRelUndo() uses this to block until the worker
 * exits, making the "sync rollback" path truly synchronous.
 */
static BackgroundWorkerHandle *pending_undo_handle = NULL;

/*
 * StartRelUndoWorker
 *		Request a background worker for applying per-relation UNDO in a database
 */
void
StartRelUndoWorker(Oid dboid)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;

	memset(&worker, 0, sizeof(BackgroundWorker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	sprintf(worker.bgw_library_name, "postgres");
	sprintf(worker.bgw_function_name, "RelUndoWorkerMain");
	snprintf(worker.bgw_name, BGW_MAXLEN, "per-relation undo worker for database %u", dboid);
	snprintf(worker.bgw_type, BGW_MAXLEN, "per-relation undo worker");
	worker.bgw_main_arg = ObjectIdGetDatum(dboid);
	worker.bgw_notify_pid = MyProcPid;

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
	{
		ereport(WARNING,
				(errmsg("could not register per-relation UNDO worker for database %u", dboid)));
	}
	else
	{
		elog(DEBUG1, "Started per-relation UNDO worker for database %u", dboid);
		pending_undo_handle = handle;
	}
}

/*
 * WaitForPendingRelUndo
 *		Block until the most recent synchronous UNDO worker exits.
 *
 * Called from AbortTransaction() AFTER locks have been released so the
 * UNDO worker can acquire AccessExclusiveLock on the target relation.
 * This makes the "sync rollback" path truly synchronous: the aborting
 * backend does not return to the client until the UNDO is applied and
 * the original tuple data is restored.
 */
void
WaitForPendingRelUndo(void)
{
	if (pending_undo_handle == NULL)
		return;

	(void) WaitForBackgroundWorkerShutdown(pending_undo_handle);

	pfree(pending_undo_handle);
	pending_undo_handle = NULL;
}
