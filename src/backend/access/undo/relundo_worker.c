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

#include "access/relundo_worker.h"
#include "access/xact.h"
#include "access/relundo.h"
#include "access/table.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "tcop/tcopprot.h"
#include "utils/guc.h"
#include "utils/timestamp.h"

/* GUC parameters */
int			max_relundo_workers = 3;
int			relundo_worker_naptime = 5000; /* milliseconds */

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
	 * Open the relation. We're in a valid transaction context now, so
	 * catalog access is safe (unlike during transaction abort).
	 */
	PG_TRY();
	{
		rel = table_open(item->reloid, AccessExclusiveLock);

		/* Apply the UNDO chain */
		RelUndoApplyChain(rel, item->start_urec_ptr);

		table_close(rel, AccessExclusiveLock);
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
		int			rc;

		/* Handle SIGHUP - reload configuration */
		if (got_SIGHUP)
		{
			got_SIGHUP = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		/* Check for work */
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
			/* No work available, sleep */
			rc = WaitLatch(MyLatch,
						   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						   relundo_worker_naptime,
						   PG_WAIT_EXTENSION);

			ResetLatch(MyLatch);

			/* Emergency bailout if postmaster has died */
			if (rc & WL_POSTMASTER_DEATH)
				proc_exit(1);
		}
	}

	elog(LOG, "Per-relation UNDO worker %d shutting down", worker_id);
	proc_exit(0);
}

/*
 * RelUndoLauncherMain
 *		Main entry point for per-relation UNDO launcher process
 *
 * The launcher monitors the work queue and spawns workers as needed.
 */
void
RelUndoLauncherMain(Datum main_arg)
{
	/* Establish signal handlers */
	pqsignal(SIGHUP, relundo_worker_sighup);
	pqsignal(SIGTERM, relundo_worker_sigterm);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	elog(LOG, "Per-relation UNDO launcher started");

	/* Main monitoring loop */
	while (!got_SIGTERM)
	{
		int			rc;

		/* Handle SIGHUP - reload configuration */
		if (got_SIGHUP)
		{
			got_SIGHUP = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		/*
		 * TODO: Implement launcher logic:
		 * - Check work queue for databases that need workers
		 * - Track active workers per database
		 * - Spawn new workers if needed (up to max_relundo_workers)
		 * - Monitor worker health and restart if needed
		 */

		/* For now, just sleep */
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
	}
}
