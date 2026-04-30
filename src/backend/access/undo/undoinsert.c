/*-------------------------------------------------------------------------
 *
 * undoinsert.c
 *	  UNDO record batch insertion operations
 *
 * This file implements batch insertion of UNDO records into the WAL
 * stream.  Records are accumulated in an UndoRecordSet and then
 * written as a single XLOG_UNDO_BATCH WAL record.
 *
 * UNDO-IN-WAL ARCHITECTURE
 * ------------------------
 * All UNDO record data flows through the standard WAL pipeline:
 *   UndoRecordSetInsert() -> XLogBeginInsert()
 *                         -> XLogRegisterData(batch_header)
 *                         -> XLogRegisterData(uset->buffer, uset->buffer_size)
 *                         -> XLogInsert(RM_UNDO_ID, XLOG_UNDO_BATCH)
 *
 * This eliminates the separate UNDO segment file I/O path (pwrite +
 * fdatasync) and provides:
 * - Replicas receive and can apply UNDO records
 * - One durability path, one sync at commit
 * - Unified crash recovery with explicit UNDO phase
 *
 * Coalescing: The existing UndoRecordSet mechanism batches records.
 * This batch becomes one WAL record.  A 1000-row INSERT produces ~1
 * WAL record containing 1000 UNDO records.
 *
 * Legacy support: UndoWalBatchFlush/Reset are kept as no-ops for
 * callers that haven't been updated yet.
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
#include "access/xlog.h"

/*
 * UndoWalBatchFlush - Legacy no-op
 *
 * With UNDO-in-WAL, there is no separate deferred WAL batch to flush.
 * UNDO data is written directly to WAL in UndoRecordSetInsert().
 * This function is kept for callers that haven't been updated yet.
 */
void
UndoWalBatchFlush(void)
{
	/* No-op: UNDO data is now written directly to WAL */
}

/*
 * UndoWalBatchReset - Legacy no-op
 *
 * With UNDO-in-WAL, there is no separate deferred WAL batch to reset.
 */
void
UndoWalBatchReset(void)
{
	/* No-op: UNDO data is now written directly to WAL */
}

/*
 * UndoRecordSetInsert - Insert accumulated UNDO records into WAL
 *
 * This function writes all UNDO records in the set as a single
 * XLOG_UNDO_BATCH WAL record.  The batch payload is the serialized
 * content of uset->buffer (concatenated UndoRecordHeader+payload).
 *
 * Returns the (legacy) UndoRecPtr for backward compatibility.
 * The actual record location is the XLogRecPtr stored in
 * uset->last_batch_lsn after this call.
 */
UndoRecPtr
UndoRecordSetInsert(UndoRecordSet *uset)
{
	xl_undo_batch xlrec;
	XLogRecPtr	batch_lsn;
	Oid			primary_reloid = InvalidOid;

	if (uset == NULL || uset->nrecords == 0)
		return InvalidUndoRecPtr;

	/*
	 * Extract the primary relation OID from the first record in the
	 * batch as an optimization hint.  Most batches contain records for
	 * a single relation.
	 */
	if (uset->buffer_size >= SizeOfUndoRecordHeader)
	{
		UndoRecordHeader *first_hdr = (UndoRecordHeader *) uset->buffer;

		primary_reloid = first_hdr->urec_reloid;
	}

	/* Build the batch header */
	xlrec.xid = uset->xid;
	xlrec.chain_prev = uset->last_batch_lsn;
	xlrec.nrecords = (uint32) uset->nrecords;
	xlrec.total_len = (uint32) uset->buffer_size;
	xlrec.primary_reloid = primary_reloid;
	xlrec.persistence = uset->persistence;

	/*
	 * Write the UNDO batch as a single WAL record.
	 *
	 * XLogRegisterData has no size limit on main data (tracked as uint64
	 * in xloginsert.c), so even a 256KB batch is fine.  The WAL
	 * insertion lock will be held for the duration of the record write,
	 * which is acceptable for batch sizes up to a few hundred KB.
	 */
	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, SizeOfUndoBatch);
	XLogRegisterData(uset->buffer, uset->buffer_size);
	batch_lsn = XLogInsert(RM_UNDO_ID, XLOG_UNDO_BATCH);

	/* Update the record set's chain pointer for subsequent batches */
	uset->last_batch_lsn = batch_lsn;

	/*
	 * Register the batch LSN for WAL retention tracking.  Only the first
	 * call per transaction takes effect (UndoRegisterBatchLSN is a no-op
	 * if the slot is already occupied), so this records the oldest batch
	 * for this transaction without additional bookkeeping.
	 */
	UndoRegisterBatchLSN(batch_lsn);

	/*
	 * For legacy compatibility, return a non-zero UndoRecPtr.
	 * The actual location is in uset->last_batch_lsn (XLogRecPtr).
	 */
	uset->prev_undo_ptr = (UndoRecPtr) batch_lsn;

	return (UndoRecPtr) batch_lsn;
}
