/*-------------------------------------------------------------------------
 *
 * undoinsert.c
 *	  UNDO record batch insertion operations
 *
 * This file implements batch insertion of UNDO records into the UNDO log.
 * Records are accumulated in an UndoRecordSet and then written to the
 * UNDO log in a single operation.
 *
 * WAL BATCHING
 * ------------
 * Rather than emitting one XLOG_UNDO_ALLOCATE WAL record per insertion,
 * we coalesce allocations in the same UNDO log into a single deferred
 * WAL record that covers the full range.  The deferred record is flushed
 * at transaction commit/abort, before log rotation, or when a per-backend
 * threshold is reached.  This reduces per-row WAL overhead from ~28 bytes
 * + WAL header per row to a single WAL record per batch.
 *
 * Crash safety: the deferred WAL record must be emitted before the
 * transaction commits (before UndoLogSync) and before UNDO replay on
 * abort.  If we crash before flushing, the allocations are lost, but so
 * is the uncommitted transaction -- no committed data references them.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/undo/undoinsert.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/undolog.h"
#include "access/undorecord.h"
#include "access/undo_xlog.h"
#include "access/xloginsert.h"

/*
 * Maximum number of coalesced allocations before an automatic flush.
 * This bounds the amount of WAL data at risk if we crash before commit.
 */
#define UNDO_WAL_BATCH_FLUSH_COUNT	64

/*
 * Per-backend deferred WAL batch state.
 *
 * We track the first start pointer and the end offset of the last
 * allocation in the current log.  When flushed, a single xl_undo_allocate
 * WAL record is emitted covering the full range [first_start, end_offset).
 *
 * The redo handler only advances the insert pointer forward (never
 * regresses), so overlapping ranges from concurrent backends are safe.
 */
typedef struct UndoWalBatch
{
	bool		active;			/* has pending entries */
	uint32		log_number;		/* UNDO log these allocations are in */
	UndoRecPtr	first_start;	/* start_ptr of first allocation */
	uint64		end_offset;		/* end offset in log (max of all allocs) */
	TransactionId xid;			/* transaction that made these allocations */
	int			count;			/* number of coalesced allocations */
}			UndoWalBatch;

static UndoWalBatch undo_wal_batch = {false, 0, InvalidUndoRecPtr, 0, InvalidTransactionId, 0};

/*
 * UndoWalBatchFlush - Emit deferred UNDO allocation WAL record
 *
 * Flushes any pending coalesced UNDO allocations as a single WAL record.
 * Must be called before transaction commit (before UndoLogSync), before
 * UNDO replay on abort, and before log rotation.
 *
 * Safe to call when no batch is pending (no-op).
 */
void
UndoWalBatchFlush(void)
{
	xl_undo_allocate xlrec;

	if (!undo_wal_batch.active)
		return;

	xlrec.start_ptr = undo_wal_batch.first_start;
	xlrec.length = (uint32) (undo_wal_batch.end_offset -
							 UndoRecPtrGetOffset(undo_wal_batch.first_start));
	xlrec.xid = undo_wal_batch.xid;
	xlrec.log_number = undo_wal_batch.log_number;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, SizeOfUndoAllocate);
	(void) XLogInsert(RM_UNDO_ID, XLOG_UNDO_ALLOCATE);

	/* Reset batch state */
	undo_wal_batch.active = false;
	undo_wal_batch.count = 0;
}

/*
 * UndoWalBatchReset - Discard pending batch without emitting WAL
 *
 * Called during transaction abort cleanup when UNDO replay has already
 * happened or when the transaction had no committed work.
 */
void
UndoWalBatchReset(void)
{
	undo_wal_batch.active = false;
	undo_wal_batch.count = 0;
}

/*
 * UndoWalBatchAdd - Add an allocation to the deferred WAL batch
 *
 * Coalesces this allocation with any pending allocations in the same
 * log.  If the log number changes (rare: rotation during txn), the
 * current batch is flushed first.
 */
static void
UndoWalBatchAdd(UndoRecPtr start_ptr, Size size, TransactionId xid)
{
	uint32		log_number = UndoRecPtrGetLogNo(start_ptr);
	uint64		end_offset = UndoRecPtrGetOffset(start_ptr) + size;

	/*
	 * If switching to a different log, flush the current batch first.
	 * This is rare (only during log rotation within a transaction).
	 */
	if (undo_wal_batch.active && undo_wal_batch.log_number != log_number)
		UndoWalBatchFlush();

	if (!undo_wal_batch.active)
	{
		/* Start a new batch */
		undo_wal_batch.active = true;
		undo_wal_batch.log_number = log_number;
		undo_wal_batch.first_start = start_ptr;
		undo_wal_batch.end_offset = end_offset;
		undo_wal_batch.xid = xid;
		undo_wal_batch.count = 1;
	}
	else
	{
		/* Extend the existing batch */
		if (end_offset > undo_wal_batch.end_offset)
			undo_wal_batch.end_offset = end_offset;
		undo_wal_batch.count++;
	}

	/* Auto-flush when batch reaches threshold */
	if (undo_wal_batch.count >= UNDO_WAL_BATCH_FLUSH_COUNT)
		UndoWalBatchFlush();
}

/*
 * UndoRecordSetInsert - Insert accumulated UNDO records into log
 *
 * This function writes all UNDO records in the set to the UNDO log
 * in a single batch operation. It performs the following steps:
 *
 * 1. Allocate space in the UNDO log
 * 2. Defer the WAL record (coalesce with other allocations in same log)
 * 3. Write the serialized records to the UNDO log
 * 4. Return the starting UndoRecPtr (first record in chain)
 *
 * The records form a backward chain via urec_prev pointers.
 * Returns InvalidUndoRecPtr if the set is empty.
 */
UndoRecPtr
UndoRecordSetInsert(UndoRecordSet *uset)
{
	UndoRecPtr	start_ptr;
	UndoRecPtr	current_ptr;

	if (uset == NULL || uset->nrecords == 0)
		return InvalidUndoRecPtr;

	/* Allocate space in UNDO log */
	start_ptr = UndoLogAllocate(uset->buffer_size);
	if (!UndoRecPtrIsValid(start_ptr))
		elog(ERROR, "failed to allocate UNDO log space");

	/*
	 * Defer the WAL record: coalesce with other allocations in the same
	 * log to reduce per-row WAL overhead.  The batch will be flushed
	 * at commit, abort, log rotation, or when the batch threshold is hit.
	 */
	UndoWalBatchAdd(start_ptr, uset->buffer_size, uset->xid);

	/* Write the records to the UNDO log */
	UndoLogWrite(start_ptr, uset->buffer, uset->buffer_size);

	/*
	 * Update the record set's previous pointer chain. Each subsequent
	 * insertion will chain backward through this pointer.
	 */
	current_ptr = start_ptr;
	if (uset->nrecords > 1)
	{
		/*
		 * The last record in the set becomes the previous pointer for the
		 * next insertion.
		 */
		current_ptr = start_ptr + (uset->buffer_size - 1);
	}

	uset->prev_undo_ptr = current_ptr;

	return start_ptr;
}
