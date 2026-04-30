/*-------------------------------------------------------------------------
 *
 * undoworker.c
 *	  UNDO worker background process implementation
 *
 * The UNDO worker periodically discards old UNDO records that are no
 * longer needed by any active transaction. This is essential for
 * preventing unbounded growth of UNDO logs.
 *
 * The worker also manages the UNDO log lifecycle: transitioning SEALED
 * logs to DISCARDABLE once all records are discarded, and freeing
 * DISCARDABLE log slots by deleting their segment files.
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

#include "access/index_prune.h"
#include "access/nbtree.h"
#include "access/undolog.h"
#include "access/undorecord.h"
#include "access/undormgr.h"
#include "access/undo_xlog.h"
#include "access/undoworker.h"
#include "access/relation.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xlogreader.h"
#include "access/xlogutils.h"
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
#include "utils/wait_event.h"

/* Shared memory state */
static UndoWorkerShmemData * UndoWorkerShmem = NULL;

/* Adaptive sleep: use shorter interval when sealed logs are pending */
#define UNDO_WORKER_FAST_NAPTIME_MS		200

/* Forward declarations */
static void undo_worker_sighup(SIGNAL_ARGS);
static void undo_worker_sigterm(SIGNAL_ARGS);
static void perform_undo_discard(void);
static void perform_targeted_index_pruning(UndoRecPtr old_discard,
										   UndoRecPtr new_discard,
										   Oid reloid);

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

		/* Rotation coordination fields */
		UndoWorkerShmem->worker_proc = INVALID_PROC_NUMBER;
		pg_atomic_init_u32(&UndoWorkerShmem->sealed_log_count, 0);
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
 * WakeUndoDiscardWorker
 *		Wake the UNDO discard worker via its latch.
 *
 * Follows the WAL writer wakeup pattern: read the worker's ProcNumber
 * and set its latch to interrupt the WaitLatch sleep.  Safe to call
 * from any backend, including during allocation pressure.
 */
void
WakeUndoDiscardWorker(void)
{
	ProcNumber	proc;

	if (UndoWorkerShmem == NULL)
		return;

	proc = UndoWorkerShmem->worker_proc;
	if (proc != INVALID_PROC_NUMBER)
		SetLatch(&GetPGProcByNumber(proc)->procLatch);
}

/*
 * UndoWorkerGetOldestXid - Get oldest transaction still needing UNDO
 *
 * Returns the oldest transaction ID that is still active across all
 * databases.  Any UNDO records created by transactions older than this
 * can be safely discarded, because those transactions have already
 * committed or aborted and their UNDO is no longer needed.
 *
 * We use GetOldestActiveTransactionId() from procarray.c which properly
 * acquires ProcArrayLock and scans all backends.  We pass allDbs=true
 * because UNDO logs are not per-database -- a single UNDO log may
 * contain records for multiple databases.
 *
 * Returns InvalidTransactionId if there are no active transactions,
 * meaning all UNDO records can potentially be discarded (subject to
 * retention policy).
 */
TransactionId
UndoWorkerGetOldestXid(void)
{
	TransactionId oldest_xid;

	/*
	 * Don't attempt the scan during recovery -- the UNDO worker should not be
	 * running in that case, but guard defensively.
	 */
	if (RecoveryInProgress())
		return InvalidTransactionId;

	/*
	 * GetOldestActiveTransactionId scans ProcArray under ProcArrayLock
	 * (LW_SHARED) and returns the smallest XID among all active backends. We
	 * pass inCommitOnly=false (we want all active XIDs, not just those in
	 * commit critical section) and allDbs=true (UNDO spans all databases).
	 */
	oldest_xid = GetOldestActiveTransactionId(false, true);

	return oldest_xid;
}

/*
 * perform_undo_discard - Main discard logic
 *
 * Two-phase approach:
 *   Phase 1: Update discard pointers for all in-use logs based on
 *            the oldest active transaction ID.
 *   Phase 2: Scan SEALED/DISCARDABLE logs and manage lifecycle
 *            transitions: SEALED -> DISCARDABLE -> FREE.
 */
static void
perform_undo_discard(void)
{
	TransactionId oldest_xid;
	UndoRecPtr	oldest_undo_ptr;
	TimestampTz current_time;
	int			i;
	int			freed_count = 0;

	/* Get oldest active transaction */
	oldest_xid = UndoWorkerGetOldestXid();

	if (!TransactionIdIsValid(oldest_xid))
	{
		/* No active transactions, can discard all UNDO */
		oldest_xid = ReadNextTransactionId();
	}

	current_time = GetCurrentTimestamp();

	/*
	 * Scan per-backend UNDO batch LSN slots and clear any that belong to
	 * dead backends.  A backend that was SIGKILLed (or otherwise exited
	 * without calling AtProcExit) will leave its slot occupied, which pins
	 * the WAL discard horizon indefinitely.  We detect dead backends by
	 * checking ProcGlobal->allProcs[i].pid == 0, which indicates the slot
	 * is not in use by a live process (pid 0 also indicates prepared-xact
	 * dummy PGPROCs, but those do not write UNDO data).
	 */
	for (i = 0; i < MaxBackends; i++)
	{
		XLogRecPtr	slot_lsn;

		slot_lsn = (XLogRecPtr)
			pg_atomic_read_u64(&UndoLogShared->backend_undo_lsns[i]);

		if (!XLogRecPtrIsValid(slot_lsn))
			continue;

		if (GetPGProcByNumber(i)->pid == 0)
		{
			pg_atomic_write_u64(&UndoLogShared->backend_undo_lsns[i],
								(uint64) InvalidXLogRecPtr);
			ereport(DEBUG2,
					(errmsg("UNDO worker: cleared stale batch LSN for dead backend slot %d", i)));
		}
	}

	/*
	 * Phase 1: For each UNDO log, determine what can be discarded.  We need
	 * to respect the retention_time setting to allow point-in-time recovery.
	 */
	for (i = 0; i < MAX_UNDO_LOGS; i++)
	{
		UndoLogControl *log = &UndoLogShared->logs[i];

		if (!log->in_use)
			continue;

		/*
		 * Calculate the oldest UNDO pointer that must be retained. This is
		 * based on: 1. The oldest active transaction 2. The retention time
		 * setting
		 */
		LWLockAcquire(&log->lock, LW_SHARED);

		if (TransactionIdIsValid(log->oldest_xid) &&
			TransactionIdPrecedes(log->oldest_xid, oldest_xid))
		{
			/* This log has UNDO that can be discarded */
			UndoRecPtr	old_discard_ptr = log->discard_ptr;

			oldest_undo_ptr = pg_atomic_read_u64(&log->insert_ptr);

			LWLockRelease(&log->lock);

			/*
			 * Before discarding, scan the records being discarded for nbtree
			 * UNDO entries and perform targeted index pruning. This is
			 * O(N_dead) instead of O(N_total_entries).
			 */
			perform_targeted_index_pruning(old_discard_ptr,
										   oldest_undo_ptr,
										   InvalidOid);

			/* Update discard pointer */
			UndoLogDiscard(oldest_undo_ptr);

			/* Update cumulative discard counter */
			pg_atomic_fetch_add_u64(&UndoLogShared->total_discarded,
									UndoRecPtrGetOffset(oldest_undo_ptr));

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

	/*
	 * Phase 2: Manage lifecycle transitions for SEALED and DISCARDABLE logs.
	 *
	 * SEALED logs whose discard_ptr >= seal_ptr have had all their records
	 * discarded and can transition to DISCARDABLE.  DISCARDABLE logs can have
	 * their slot freed and segment file deleted.
	 */
	for (i = 0; i < MAX_UNDO_LOGS; i++)
	{
		UndoLogControl *log = &UndoLogShared->logs[i];

		if (!log->in_use)
			continue;

		LWLockAcquire(&log->lock, LW_EXCLUSIVE);

		if (log->state == UNDO_LOG_SEALED)
		{
			UndoRecPtr	seal = pg_atomic_read_u64(&log->seal_ptr);
			UndoRecPtr	discard = log->discard_ptr;

			if (UndoRecPtrIsValid(seal) &&
				UndoRecPtrGetOffset(discard) >= UndoRecPtrGetOffset(seal))
			{
				/* All records discarded -- transition to DISCARDABLE */
				log->state = UNDO_LOG_DISCARDABLE;
				ereport(DEBUG1,
						(errmsg("UNDO worker: log %u transitioned to DISCARDABLE",
								log->log_number)));
			}
		}

		if (log->state == UNDO_LOG_DISCARDABLE)
		{
			uint32		log_number = log->log_number;

			/* Free the slot */
			log->in_use = false;
			log->state = UNDO_LOG_FREE;
			log->log_number = 0;
			pg_atomic_write_u64(&log->insert_ptr, InvalidUndoRecPtr);
			log->discard_ptr = InvalidUndoRecPtr;
			log->oldest_xid = InvalidTransactionId;
			pg_atomic_write_u64(&log->seal_ptr, InvalidUndoRecPtr);
			log->sealed_time = 0;

			LWLockRelease(&log->lock);

			/* Delete the segment file outside the lock */
			UndoLogDeleteSegmentFile(log_number);

			/* Decrement sealed log count */
			pg_atomic_fetch_sub_u32(&UndoWorkerShmem->sealed_log_count, 1);

			freed_count++;
			continue;
		}

		LWLockRelease(&log->lock);
	}

	if (freed_count > 0)
		ereport(LOG,
				(errmsg("UNDO worker: freed %d discardable log segment(s)",
						freed_count)));

	/*
	 * Advance the WAL discard horizon so KeepLogSeg() can allow recycling
	 * of WAL segments no longer needed for UNDO rollback.
	 *
	 * UndoGetOldestBatchLSN() scans per-backend slots and returns the
	 * minimum first-batch LSN across all active transactions that have
	 * written UNDO data.  WAL before this LSN cannot be recycled.
	 *
	 * If no backend has in-flight UNDO data the function returns
	 * InvalidXLogRecPtr, meaning there is no UNDO-imposed WAL retention
	 * requirement.  We do not call UndoSetDiscardHorizon in that case
	 * because an invalid horizon is already the "no constraint" sentinel.
	 */
	{
		XLogRecPtr	new_horizon = UndoGetOldestBatchLSN();

		if (XLogRecPtrIsValid(new_horizon))
			UndoSetDiscardHorizon(new_horizon);

		/*
		 * If undo_max_wal_retention_size is set, warn when the retained
		 * WAL distance between the current write position and the UNDO
		 * discard horizon exceeds the configured limit.  This helps
		 * operators detect long-running transactions that prevent WAL
		 * recycling.
		 */
		if (undo_max_wal_retention_size > 0 && XLogRecPtrIsValid(new_horizon))
		{
			XLogRecPtr	write_ptr = GetXLogWriteRecPtr();

			if (write_ptr > new_horizon)
			{
				uint64		retained_mb = (write_ptr - new_horizon) >> 20;

				if (retained_mb > (uint64) undo_max_wal_retention_size)
					ereport(WARNING,
							(errmsg("UNDO WAL retention (%lu MB) exceeds undo_max_wal_retention_size (%d MB)",
									(unsigned long) retained_mb, undo_max_wal_retention_size),
							 errhint("Investigate long-running transactions or increase undo_max_wal_retention_size.")));
			}
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
 *
 * Uses adaptive sleep: when sealed logs are pending cleanup, the worker
 * wakes more frequently (200ms) to process them promptly.  Otherwise
 * it uses the configured undo_worker_naptime.
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

	/* Register our ProcNumber for latch-based wakeup by other backends */
	UndoWorkerShmem->worker_proc = MyProcNumber;

	/* Initialize worker state */
	ereport(LOG,
			(errmsg("UNDO worker started")));

	/*
	 * Create a memory context for the worker. This will be reset after each
	 * iteration.
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
		long		naptime;
		uint32		sealed_count;

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

		/*
		 * Adaptive sleep: use a shorter interval when sealed logs are pending
		 * cleanup, similar to the WAL writer's adaptive sleep.
		 */
		sealed_count = pg_atomic_read_u32(&UndoWorkerShmem->sealed_log_count);
		if (sealed_count > 0)
			naptime = UNDO_WORKER_FAST_NAPTIME_MS;
		else
			naptime = UndoWorkerShmem->naptime_ms;

		/* Sleep until next iteration, latch set, or signal */
		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   naptime,
					   WAIT_EVENT_UNDO_WORKER_MAIN);

		ResetLatch(MyLatch);

		/* Emergency bailout if postmaster died */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);
	}

	/* Clear our ProcNumber before exiting */
	UndoWorkerShmem->worker_proc = INVALID_PROC_NUMBER;

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
	worker.bgw_restart_time = 10;	/* Restart after 10 seconds if crashed */

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

/*
 * perform_targeted_index_pruning - Extract nbtree targets from discarded
 * UNDO records and perform targeted index pruning.
 *
 * In UNDO-in-WAL architecture, old_discard and new_discard are XLogRecPtrs
 * (cast as UndoRecPtrs).  This function scans WAL forward from old_discard
 * to new_discard, reading XLOG_UNDO_BATCH records and iterating through
 * the serialized UNDO records within each batch.  For nbtree INSERT_LEAF
 * records, it extracts (index_oid, blkno, offset, heap_tid) targets and
 * dispatches them to IndexPruneNotifyTargeted() for O(N_dead) pruning.
 */
static void
perform_targeted_index_pruning(UndoRecPtr old_discard, UndoRecPtr new_discard,
							   Oid reloid)
{
	XLogRecPtr	start_lsn = (XLogRecPtr) old_discard;
	XLogRecPtr	end_lsn = (XLogRecPtr) new_discard;
	XLogReaderState *reader;
	XLogRecord *record_hdr;
	char	   *errormsg = NULL;
	IndexPruneTarget *targets = NULL;
	int			ntargets = 0;
	int			max_targets = 64;
	Oid			found_reloid = reloid;
	Relation	heaprel;
	MemoryContext oldcontext;
	MemoryContext prune_context;
	int			batches_scanned = 0;
	static XLogReaderRoutine routine = {
		.page_read = read_local_xlog_page,
		.segment_open = wal_segment_open,
		.segment_close = wal_segment_close,
	};

	/* Quick sanity check */
	if (!XLogRecPtrIsValid(start_lsn) || !XLogRecPtrIsValid(end_lsn))
		return;
	if (end_lsn <= start_lsn)
		return;

	/*
	 * Allocate targets in a temporary memory context so we can clean up
	 * easily if an error occurs.
	 */
	prune_context = AllocSetContextCreate(CurrentMemoryContext,
										  "UndoTargetedPrune",
										  ALLOCSET_DEFAULT_SIZES);
	oldcontext = MemoryContextSwitchTo(prune_context);

	targets = palloc(sizeof(IndexPruneTarget) * max_targets);

	/*
	 * Allocate an XLogReader to scan WAL forward from start_lsn to end_lsn.
	 * We read every WAL record in the range and filter for XLOG_UNDO_BATCH
	 * records from RM_UNDO_ID.
	 */
	reader = XLogReaderAllocate(wal_segment_size, NULL, &routine, NULL);
	if (reader == NULL)
	{
		ereport(WARNING,
				(errmsg("UNDO worker: could not allocate XLogReader for targeted pruning")));
		MemoryContextSwitchTo(oldcontext);
		MemoryContextDelete(prune_context);
		return;
	}

	XLogBeginRead(reader, start_lsn);

	/*
	 * Walk WAL records forward.  XLogReadRecord returns records sequentially;
	 * we stop when we reach or pass end_lsn, or hit an error.
	 */
	while (true)
	{
		XLogRecPtr	cur_lsn;

		CHECK_FOR_INTERRUPTS();

		record_hdr = XLogReadRecord(reader, &errormsg);
		if (record_hdr == NULL)
		{
			/* End of available WAL or read error */
			if (errormsg)
				ereport(DEBUG2,
						(errmsg("UNDO worker: WAL read ended at %X/%X: %s",
								LSN_FORMAT_ARGS(end_lsn), errormsg)));
			break;
		}

		cur_lsn = reader->ReadRecPtr;

		/* Stop once we've passed the discard range */
		if (cur_lsn >= end_lsn)
			break;

		/* Only interested in UNDO batch records */
		if (XLogRecGetRmid(reader) != RM_UNDO_ID ||
			(XLogRecGetInfo(reader) & ~XLR_INFO_MASK) != XLOG_UNDO_BATCH)
			continue;

		/*
		 * Found an UNDO batch record.  Parse the batch header and iterate
		 * through the serialized UNDO records within the payload.
		 */
		{
			char	   *record_data = XLogRecGetData(reader);
			Size		record_len = XLogRecGetDataLen(reader);
			char	   *pos;
			char	   *end;

			if (record_len < SizeOfUndoBatch)
				continue;

			pos = record_data + SizeOfUndoBatch;
			end = record_data + record_len;

			batches_scanned++;

			/* Walk serialized UNDO records within this batch */
			while (pos < end)
			{
				UndoRecordHeader rec_hdr;
				const char *payload;

				/* Need at least a header */
				if ((Size) (end - pos) < SizeOfUndoRecordHeader)
					break;

				memcpy(&rec_hdr, pos, SizeOfUndoRecordHeader);

				/* Sanity check record length */
				if (rec_hdr.urec_len < SizeOfUndoRecordHeader ||
					(Size) (end - pos) < rec_hdr.urec_len)
					break;

				/*
				 * For nbtree INSERT_LEAF records, extract a pruning target.
				 */
				if (rec_hdr.urec_rmid == UNDO_RMID_NBTREE &&
					rec_hdr.urec_payload_len > 0)
				{
					payload = pos + SizeOfUndoRecordHeader;

					if (ntargets >= max_targets)
					{
						max_targets *= 2;
						targets = repalloc(targets,
										   sizeof(IndexPruneTarget) * max_targets);
					}

					if (NbtreeUndoExtractPruneTarget(rec_hdr.urec_info,
													 payload,
													 rec_hdr.urec_payload_len,
													 &targets[ntargets]))
					{
						ntargets++;

						/*
						 * Capture the heap relation OID from the UNDO record.
						 * nbtree records for the same transaction typically
						 * reference the same heap relation.
						 */
						if (!OidIsValid(found_reloid) &&
							OidIsValid(rec_hdr.urec_reloid))
							found_reloid = rec_hdr.urec_reloid;
					}
				}

				/* Advance to the next record within batch */
				pos += rec_hdr.urec_len;
			}
		}
	}

	XLogReaderFree(reader);
	MemoryContextSwitchTo(oldcontext);

	ereport(DEBUG2,
			(errmsg("UNDO worker: scanned %d WAL batches in range %X/%X to %X/%X, found %d prune targets",
					batches_scanned,
					LSN_FORMAT_ARGS(start_lsn),
					LSN_FORMAT_ARGS(end_lsn),
					ntargets)));

	/*
	 * If we found targets, dispatch them for targeted pruning.
	 *
	 * IndexPruneNotifyTargeted groups targets by index_oid internally, so we
	 * just need a heap relation for heap TID verification.  Use the reloid
	 * parameter if provided, otherwise try the first record's reloid.
	 */
	if (ntargets > 0)
	{
		Oid			heap_oid = found_reloid;

		if (OidIsValid(heap_oid))
		{
			heaprel = try_relation_open(heap_oid, AccessShareLock);
			if (heaprel != NULL)
			{
				uint64		pruned;

				pruned = IndexPruneNotifyTargeted(heaprel, targets, ntargets);

				if (pruned > 0)
					ereport(DEBUG1,
							(errmsg("UNDO worker: targeted pruning removed %lu index entries from %d targets",
									(unsigned long) pruned, ntargets)));

				relation_close(heaprel, AccessShareLock);
			}
		}
	}

	MemoryContextDelete(prune_context);
}
