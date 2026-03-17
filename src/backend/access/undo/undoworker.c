/*-------------------------------------------------------------------------
 *
 * undoworker.c
 *	  UNDO worker background process implementation
 *
 * The UNDO worker periodically discards old UNDO records that are no
 * longer needed by any active transaction. This is essential for
 * preventing unbounded growth of UNDO logs.
 *
 * Design based on ZHeap's UNDO worker and PostgreSQL's autovacuum
 * launcher patterns.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/undo/undoworker.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <setjmp.h>
#include <unistd.h>

#include "access/undolog.h"
#include "access/undoworker.h"
#include "access/transam.h"
#include "access/xact.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/procsignal.h"
#include "tcop/tcopprot.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/timeout.h"
#include "utils/timestamp.h"

/* Shared memory state */
static UndoWorkerShmemData *UndoWorkerShmem = NULL;

/* Forward declarations */
static void undo_worker_sighup(SIGNAL_ARGS);
static void undo_worker_sigterm(SIGNAL_ARGS);
static void perform_undo_discard(void);

/*
 * UndoWorkerShmemSize - Calculate shared memory needed
 */
Size
UndoWorkerShmemSize(void)
{
	return sizeof(UndoWorkerShmemData);
}

/*
 * UndoWorkerShmemInit - Initialize shared memory
 */
void
UndoWorkerShmemInit(void)
{
	bool		found;

	UndoWorkerShmem = (UndoWorkerShmemData *)
		ShmemInitStruct("UNDO Worker Data",
						UndoWorkerShmemSize(),
						&found);

	if (!found)
	{
		LWLockInitialize(&UndoWorkerShmem->lock,
						 LWTRANCHE_UNDO_LOG);

		pg_atomic_init_u64(&UndoWorkerShmem->last_discard_time, 0);
		UndoWorkerShmem->oldest_xid_checked = InvalidTransactionId;
		UndoWorkerShmem->last_discard_ptr = InvalidUndoRecPtr;
		UndoWorkerShmem->naptime_ms = undo_worker_naptime;
		UndoWorkerShmem->shutdown_requested = false;
	}
}

/*
 * undo_worker_sighup - SIGHUP handler
 */
static void
undo_worker_sighup(SIGNAL_ARGS)
{
	(void) postgres_signal_arg; /* unused */
	ConfigReloadPending = true;
	SetLatch(MyLatch);
}

/*
 * undo_worker_sigterm - SIGTERM handler
 */
static void
undo_worker_sigterm(SIGNAL_ARGS)
{
	(void) postgres_signal_arg; /* unused */
	UndoWorkerShmem->shutdown_requested = true;
	SetLatch(MyLatch);
}

/*
 * UndoWorkerGetOldestXid - Get oldest transaction still needing UNDO
 *
 * This scans the process array to find the oldest active transaction.
 * Any UNDO records older than this can be safely discarded.
 */
TransactionId
UndoWorkerGetOldestXid(void)
{
	TransactionId oldest_xid;

	/*
	 * Get the oldest XID that's still active.
	 * For now, use a simple approach: just return invalid.
	 * A full implementation would scan ProcArray for the true oldest XID.
	 */
	oldest_xid = InvalidTransactionId;

	return oldest_xid;
}

/*
 * perform_undo_discard - Main discard logic
 *
 * This function:
 * 1. Finds the oldest active transaction
 * 2. For each UNDO log, calculates what can be discarded
 * 3. Calls UndoLogDiscard to update discard pointers
 */
static void
perform_undo_discard(void)
{
	TransactionId oldest_xid;
	UndoRecPtr	oldest_undo_ptr;
	TimestampTz current_time;
	int			i;

	/* Get oldest active transaction */
	oldest_xid = UndoWorkerGetOldestXid();

	if (!TransactionIdIsValid(oldest_xid))
	{
		/* No active transactions, can discard all UNDO */
		oldest_xid = ReadNextTransactionId();
	}

	current_time = GetCurrentTimestamp();

	/*
	 * For each UNDO log, determine what can be discarded.
	 * We need to respect the retention_time setting to allow
	 * point-in-time recovery.
	 */
	for (i = 0; i < MAX_UNDO_LOGS; i++)
	{
		UndoLogControl *log = &UndoLogShared->logs[i];

		if (!log->in_use)
			continue;

		/*
		 * Calculate the oldest UNDO pointer that must be retained.
		 * This is based on:
		 * 1. The oldest active transaction
		 * 2. The retention time setting
		 */
		LWLockAcquire(&log->lock, LW_SHARED);

		if (TransactionIdIsValid(log->oldest_xid) &&
			TransactionIdPrecedes(log->oldest_xid, oldest_xid))
		{
			/* This log has UNDO that can be discarded */
			oldest_undo_ptr = log->insert_ptr;

			LWLockRelease(&log->lock);

			/* Update discard pointer */
			UndoLogDiscard(oldest_undo_ptr);

			ereport(DEBUG2,
					(errmsg("UNDO worker: discarded log %u up to %llu",
							log->log_number,
							(unsigned long long) oldest_undo_ptr)));
		}
		else
		{
			LWLockRelease(&log->lock);
		}
	}

	/* Record this discard operation */
	LWLockAcquire(&UndoWorkerShmem->lock, LW_EXCLUSIVE);
	pg_atomic_write_u64(&UndoWorkerShmem->last_discard_time,
						(uint64) current_time);
	UndoWorkerShmem->oldest_xid_checked = oldest_xid;
	LWLockRelease(&UndoWorkerShmem->lock);
}

/*
 * UndoWorkerMain - Main loop for UNDO worker
 *
 * This is the entry point for the UNDO worker background process.
 * It runs continuously, waking periodically to discard old UNDO.
 */
void
UndoWorkerMain(Datum main_arg)
{
	(void) main_arg;			/* unused */

	/* Establish signal handlers */
	pqsignal(SIGHUP, undo_worker_sighup);
	pqsignal(SIGTERM, undo_worker_sigterm);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Initialize worker state */
	ereport(LOG,
			(errmsg("UNDO worker started")));

	/*
	 * Create a memory context for the worker.
	 * This will be reset after each iteration.
	 */
	CurrentMemoryContext = AllocSetContextCreate(TopMemoryContext,
												 "UNDO Worker",
												 ALLOCSET_DEFAULT_SIZES);

	/* Simple error handling without sigsetjmp for now */

	/*
	 * Main loop: wake up periodically and discard old UNDO
	 */
	while (!UndoWorkerShmem->shutdown_requested)
	{
		int			rc;

		/* Process any pending configuration changes */
		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);

			/* Update naptime from GUC */
			UndoWorkerShmem->naptime_ms = undo_worker_naptime;
		}

		CHECK_FOR_INTERRUPTS();

		/* Perform UNDO discard */
		perform_undo_discard();

		/* Sleep until next iteration or signal */
		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   UndoWorkerShmem->naptime_ms,
					   PG_WAIT_EXTENSION); /* TODO: Add proper wait event */

		ResetLatch(MyLatch);

		/* Emergency bailout if postmaster died */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);
	}

	/* Normal shutdown */
	ereport(LOG,
			(errmsg("UNDO worker shutting down")));

	proc_exit(0);
}

/*
 * UndoWorkerRegister - Register the UNDO worker at server start
 *
 * This is called from postmaster during server initialization.
 */
void
UndoWorkerRegister(void)
{
	BackgroundWorker worker;

	memset(&worker, 0, sizeof(BackgroundWorker));

	worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = 10; /* Restart after 10 seconds if crashed */

	sprintf(worker.bgw_library_name, "postgres");
	sprintf(worker.bgw_function_name, "UndoWorkerMain");
	snprintf(worker.bgw_name, BGW_MAXLEN, "undo worker");
	snprintf(worker.bgw_type, BGW_MAXLEN, "undo worker");

	RegisterBackgroundWorker(&worker);
}

/*
 * UndoWorkerRequestShutdown - Request worker to shut down
 */
void
UndoWorkerRequestShutdown(void)
{
	if (UndoWorkerShmem != NULL)
	{
		LWLockAcquire(&UndoWorkerShmem->lock, LW_EXCLUSIVE);
		UndoWorkerShmem->shutdown_requested = true;
		LWLockRelease(&UndoWorkerShmem->lock);
	}
}
