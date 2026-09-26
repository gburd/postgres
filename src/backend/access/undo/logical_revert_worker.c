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
#include "access/undo_xlog.h"
#include "access/undorecord.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xlogdefs.h"
#include "catalog/pg_database.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "tcop/tcopprot.h"
#include "utils/guc.h"
#include "utils/injection_point.h"
#include "utils/memutils.h"

/* GUC parameter: sleep time between ATM scans in milliseconds */
int			logical_revert_naptime = 1000;

/* GUC parameter: max number of logical revert workers (0 = disabled) */
int			max_logical_revert_workers = 2;

/*
 * Upper bound on distinct databases the launcher tracks per scan when
 * collecting those with unreverted ATM entries.  Far more than the number of
 * databases that realistically have in-flight aborted-transaction UNDO at
 * once; extras (if ever) are picked up on the next scan.
 */
#define MAX_LOGICAL_REVERT_DATABASES	128

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
} LogicalRevertState;

static LogicalRevertState *RevertState = NULL;

/* Flags set by signal handlers */
static volatile sig_atomic_t got_SIGHUP = false;
static volatile sig_atomic_t got_SIGTERM = false;

/* Signal handlers */
static void logical_revert_sighup(SIGNAL_ARGS);
static void logical_revert_sigterm(SIGNAL_ARGS);
static void process_revert_entry(TransactionId xid, XLogRecPtr last_batch_lsn);
static List *get_revertable_database_list(MemoryContext resultcxt);

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

		/*
		 * Service interrupts and ProcSignalBarriers at the top of every
		 * iteration.  Without this, a worker that is busy reverting back-
		 * to-back ATM entries via the `continue` path below never reaches the
		 * WaitLatch sleep, and ALTER DATABASE ... SET TABLESPACE / CREATE
		 * DATABASE ... STRATEGY=FILE_COPY will hang waiting for this backend
		 * to accept PROCSIGNAL_BARRIER_SMGRRELEASE.
		 */
		CHECK_FOR_INTERRUPTS();

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

			/*
			 * Test hook: let a recovery test freeze the worker here, after
			 * the ATM entry has been (re)discovered but before it is reverted
			 * and forgotten, so the test can observe the reconstructed ATM
			 * state.
			 */
			INJECTION_POINT("logical-revert-before-process", NULL);

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

				/*
				 * Reset the cached WAL reader -- it may hold stale segment
				 * state (invalid FD, partial read buffer) after the error.
				 * Without this, the next UndoReadBatchFromWAL call would
				 * reuse the corrupted reader and SIGSEGV.
				 */
				UndoResetBatchReader();

				/*
				 * If the WAL segment holding this abort's UNDO batch is
				 * behind the redo pointer, it looks recycled -- which must
				 * never happen for an un-reverted abort.  The retention chain
				 * pins that WAL against recycling for exactly as long as the
				 * ATM entry survives:
				 *
				 * ATMGetOldestUnrevertedLSN (atm.c) -> UndoGetOldestBatchLSN
				 * (undolog.c) -> KeepLogSeg (xlog.c)
				 *
				 * so KeepLogSeg cannot advance past last_batch_lsn until
				 * ATMForget removes the entry.  Reaching this branch means
				 * that invariant was violated and UNDO WAL for an un-reverted
				 * abort is genuinely gone: the cluster's guaranteed-rollback
				 * claim is already broken.  Silently marking the entry
				 * reverted would fake a rollback that never happened and
				 * leave stale tuple state behind, so fail loudly instead --
				 * the integrity guarantee is cluster-wide, hence PANIC rather
				 * than continuing.
				 *
				 * The other-error branch is a transient failure (the WAL is
				 * still retained); log it and let the next iteration retry.
				 */
				{
					XLogRecPtr	redo = GetRedoRecPtr();

					if (XLogRecPtrIsValid(last_batch_lsn) &&
						last_batch_lsn < redo)
					{
						elog(PANIC, "logical revert worker: UNDO WAL for "
							 "un-reverted xid %u at %X/%X was recycled "
							 "(redo at %X/%X); retention invariant violated "
							 "(ATMGetOldestUnrevertedLSN -> "
							 "UndoGetOldestBatchLSN -> KeepLogSeg must pin "
							 "this WAL until ATMForget) -- guaranteed "
							 "rollback can no longer be honored",
							 xid, LSN_FORMAT_ARGS(last_batch_lsn),
							 LSN_FORMAT_ARGS(redo));
					}
					else
					{
						elog(LOG, "logical revert worker: failed to revert "
							 "xid %u, will retry", xid);
					}
				}
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
		BGWORKER_BACKEND_DATABASE_CONNECTION |
		BGWORKER_INTERRUPTIBLE;
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
 * get_revertable_database_list
 *		Return a palloc'd List of OIDs of databases that should get a worker.
 *
 *	Returns only databases that have at least one unreverted ATM entry: those
 *	are the only databases with aborted-transaction UNDO for a worker to apply.
 *	A connectable, non-template database with no unreverted entries gets no
 *	worker, so an idle cluster (the common case) launches nothing and no
 *	revert worker connects to -- or reports pgstat activity for -- a database
 *	that has no work.  The returned list is allocated in resultcxt (a
 *	long-lived context) so it survives the transaction the scan runs in.
 */
static List *
get_revertable_database_list(MemoryContext resultcxt)
{
	List	   *dblist = NIL;
	Relation	pg_database;
	TableScanDesc scan;
	HeapTuple	tup;
	Oid			work_dboids[MAX_LOGICAL_REVERT_DATABASES];
	int			nwork;
	int			i;

	/*
	 * Fast path: if no database has an unreverted ATM entry, there is nothing
	 * to revert anywhere, so spawn no workers and open no database.
	 */
	nwork = ATMCollectUnrevertedDatabases(work_dboids,
										  MAX_LOGICAL_REVERT_DATABASES);
	if (nwork == 0)
		return NIL;

	StartTransactionCommand();

	pg_database = table_open(DatabaseRelationId, AccessShareLock);
	scan = table_beginscan_catalog(pg_database, 0, NULL);
	while ((tup = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		Form_pg_database db = (Form_pg_database) GETSTRUCT(tup);
		MemoryContext oldcxt;
		bool		has_work = false;

		if (!db->datallowconn)
			continue;

		/*
		 * Skip template databases.  template1 / template0 are not expected to
		 * accumulate ATM entries, and holding a revert-worker connection to
		 * template1 would block subsequent CREATE DATABASE commands that use
		 * it as the source template.
		 */
		if (strncmp(NameStr(db->datname), "template", 8) == 0)
			continue;

		/* Only databases with unreverted ATM entries need a worker. */
		for (i = 0; i < nwork; i++)
		{
			if (work_dboids[i] == db->oid)
			{
				has_work = true;
				break;
			}
		}
		if (!has_work)
			continue;

		/* Append the OID in the caller's long-lived context. */
		oldcxt = MemoryContextSwitchTo(resultcxt);
		dblist = lappend_oid(dblist, db->oid);
		MemoryContextSwitchTo(oldcxt);
	}
	table_endscan(scan);
	table_close(pg_database, AccessShareLock);

	CommitTransactionCommand();

	return dblist;
}

/*
 * LogicalRevertLauncherMain
 *		Launcher background worker.
 *
 *	The launcher re-scans pg_database on every wake-up and spawns a
 *	LogicalRevertWorker for any connectable, non-template database that does
 *	not already have one.  This way databases created after server start get
 *	a worker without requiring a restart.  Databases that disappear are
 *	dropped from the tracking set so their OIDs can be re-tracked if reused.
 *
 *	We track the set of databases we have already spawned a worker for so we
 *	do not re-register on every wake-up: the per-db workers use
 *	`bgw_restart_time = 60` and the postmaster auto-restarts them if they
 *	exit, and the bgworker slot pool is small enough that eager re-spawning
 *	would exhaust it.
 */
void
LogicalRevertLauncherMain(Datum main_arg)
{
	MemoryContext launcher_cxt;
	List	   *known_dbs = NIL;

	pqsignal(SIGHUP, logical_revert_sighup);
	pqsignal(SIGTERM, logical_revert_sigterm);
	BackgroundWorkerUnblockSignals();

	/*
	 * The launcher does not connect to any database itself.  It spawns per-db
	 * workers which do their own connection (via
	 * BackgroundWorkerInitializeConnectionByOid).  For the pg_database scans
	 * we connect to postgres (not template1, because holding a connection to
	 * template1 would block CREATE DATABASE).
	 */
	BackgroundWorkerInitializeConnection("postgres", NULL, 0);

	/*
	 * Long-lived context for the set of databases we have already started a
	 * worker for.  It must outlive the per-scan transactions, so it cannot
	 * live in a transaction-scoped context.
	 */
	launcher_cxt = AllocSetContextCreate(TopMemoryContext,
										 "Logical Revert Launcher",
										 ALLOCSET_SMALL_SIZES);

	elog(LOG, "logical revert launcher started");

	while (!got_SIGTERM)
	{
		int			rc;
		List	   *current_dbs;
		List	   *next_known = NIL;
		MemoryContext oldcxt;
		ListCell   *lc;

		/* Service interrupts and ProcSignalBarriers at every iteration. */
		CHECK_FOR_INTERRUPTS();

		if (got_SIGHUP)
		{
			got_SIGHUP = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		/* Re-scan pg_database to discover databases created or dropped. */
		current_dbs = get_revertable_database_list(launcher_cxt);

		oldcxt = MemoryContextSwitchTo(launcher_cxt);
		foreach(lc, current_dbs)
		{
			Oid			dboid = lfirst_oid(lc);

			/*
			 * Carry forward databases that already have a live worker.
			 */
			if (list_member_oid(known_dbs, dboid))
			{
				next_known = lappend_oid(next_known, dboid);
				continue;
			}

			/*
			 * Start a worker for a newly seen database only while we stay at
			 * or below max_logical_revert_workers concurrently tracked
			 * per-database workers.  The GUC is a hard ceiling on the number
			 * of live revert workers so the launcher never exhausts the
			 * shared background-worker slot pool.  On a cluster with more
			 * connectable databases than workers, the databases past the
			 * ceiling are serviced once a tracked database is dropped (its
			 * slot frees up on the next scan); raise the GUC to cover all
			 * databases concurrently.
			 */
			if (list_length(next_known) >= max_logical_revert_workers)
				continue;

			StartLogicalRevertWorker(dboid);
			next_known = lappend_oid(next_known, dboid);
		}
		MemoryContextSwitchTo(oldcxt);

		/*
		 * Replace the tracked set with the databases that still exist.  Any
		 * dropped database falls out here; its worker (if any) exits on its
		 * own when its database disappears, and forgetting the OID lets a
		 * reused OID be re-tracked on a later scan.
		 */
		list_free(known_dbs);
		known_dbs = next_known;
		list_free(current_dbs);

		/*
		 * Sleep until the next scan.  A shorter interval than the original
		 * one-shot design so newly created databases pick up a revert worker
		 * promptly; the no-op fast path (every db already tracked) is cheap.
		 */
		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   60000L,	/* 1 minute */
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
 *	UNDO is always-on infrastructure; table AMs opt in via am_supports_undo.
 */
void
LogicalRevertLauncherRegister(void)
{
	BackgroundWorker bgw;

	/* Disabled during binary upgrade or when explicitly turned off. */
	if (IsBinaryUpgrade)
		return;
	if (max_logical_revert_workers <= 0)
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
