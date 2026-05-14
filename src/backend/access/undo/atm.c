/*-------------------------------------------------------------------------
 *
 * atm.c
 *	  Aborted Transaction Map for CTR (Constant-Time Recovery)
 *
 * The ATM is a shared-memory data structure mapping TransactionId to UNDO
 * chain metadata for aborted transactions. It enables:
 *
 *   1. O(1) visibility checks: ATMIsAborted(xid) via SLogXidIsPresent()
 *      for recently-aborted transactions whose effects haven't been
 *      reverted yet.
 *
 *   2. Background Logical Revert: the Logical Revert worker scans the
 *      sLog for entries where revert_complete == false and applies their
 *      UNDO chains asynchronously.
 *
 *   3. Instant abort: at transaction abort time, the backend writes an
 *      ATM entry (sLog + WAL) instead of performing synchronous
 *      rollback, making ROLLBACK O(1).
 *
 * Implementation: All ATM functions are thin wrappers around the sLog
 * (Secondary Log) hash tables defined in access/slog.h.  The sLog
 * provides O(1) lookups via SLogXidHash, replacing the old fixed-size
 * linear array.
 *
 * WAL: ATMAddAborted() emits XLOG_ATM_ABORT; ATMForget() emits
 * XLOG_ATM_FORGET. During recovery, atm_redo() replays these without
 * re-emitting WAL.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/undo/atm.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/atm.h"
#include "access/slog.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlogreader.h"
#include "access/xlogutils.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"

/* Internal helpers that skip WAL emission (used during redo) */
static bool ATMAddAbortedInternal(TransactionId xid, Oid dboid, Oid reloid,
								  XLogRecPtr last_batch_lsn);
static void ATMForgetInternal(TransactionId xid);

/*
 * ATMShmemSize
 *		Calculate shared memory space needed for the ATM.
 *
 * The ATM is now backed by sLog, which manages its own shared memory.
 * ATM itself needs no additional shared memory.
 */
Size
ATMShmemSize(void)
{
	return 0;
}

/*
 * ATMShmemInit
 *		Initialize ATM shared memory (no-op, sLog handles it).
 */
void
ATMShmemInit(void)
{
	/* sLog initialization is done separately via SLogShmemInit() */
}

/*
 * ATMIsAborted
 *		Check whether a transaction is tracked in the ATM.
 *
 * This is the hot-path function called during visibility checks.
 * Delegates to SLogXidIsPresent() for O(1) hash lookup.
 */
bool
ATMIsAborted(TransactionId xid)
{
	return SLogXidIsPresent(xid);
}

/*
 * ATMGetLastBatchLSN
 *		Retrieve the WAL LSN of the last UNDO batch for an aborted transaction.
 *
 * Returns true if found, storing the LSN in *lsn_out.
 */
bool
ATMGetLastBatchLSN(TransactionId xid, XLogRecPtr *lsn_out)
{
	return SLogTxnLookupByXid(xid, lsn_out);
}

/*
 * ATMAddAbortedInternal
 *		Add an entry to the ATM without emitting WAL.
 *
 * Used during both normal operation (after WAL has been written by the
 * caller) and during redo replay.
 *
 * Returns false if the sLog is full.
 */
static bool
ATMAddAbortedInternal(TransactionId xid, Oid dboid, Oid reloid,
					  XLogRecPtr last_batch_lsn)
{
	return SLogTxnInsert(xid, reloid, dboid, last_batch_lsn);
}

/*
 * ATMAddAborted
 *		Record an aborted transaction in the ATM with WAL logging.
 *
 * Called from the abort path. Returns false if the sLog is full,
 * signaling the caller to fall back to synchronous rollback.
 */
bool
ATMAddAborted(TransactionId xid, Oid dboid, XLogRecPtr last_batch_lsn)
{
	xl_atm_abort xlrec;

	/* Write WAL first */
	xlrec.xid = xid;
	xlrec.last_batch_lsn = last_batch_lsn;
	xlrec.dboid = dboid;
	xlrec.reloid = InvalidOid;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, SizeOfXlAtmAbort);
	XLogInsert(RM_ATM_ID, XLOG_ATM_ABORT);

	/* Now update shared memory */
	return ATMAddAbortedInternal(xid, dboid, InvalidOid, last_batch_lsn);
}

/*
 * ATMForgetInternal
 *		Remove ATM entries for a transaction without emitting WAL.
 */
static void
ATMForgetInternal(TransactionId xid)
{
	SLogTxnRemoveByXid(xid);
}

/*
 * ATMForget
 *		Remove ATM entries after Logical Revert has completed.
 *
 * Emits a WAL record so that the removal survives recovery.
 */
void
ATMForget(TransactionId xid)
{
	xl_atm_forget xlrec;

	/* Write WAL first */
	xlrec.xid = xid;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, SizeOfXlAtmForget);
	XLogInsert(RM_ATM_ID, XLOG_ATM_FORGET);

	/* Now update shared memory */
	ATMForgetInternal(xid);
}

/*
 * ATMMarkReverted
 *		Mark an ATM entry's revert as complete.
 *
 * The entry is kept in the ATM (for visibility checks) until ATMForget()
 * is called after the Logical Revert worker confirms all effects are gone.
 */
void
ATMMarkReverted(TransactionId xid)
{
	SLogTxnMarkReverted(xid);
}

/*
 * ATMGetNextUnreverted
 *		Find the next ATM entry that hasn't been reverted yet.
 *
 * Used by the Logical Revert background worker to find work.
 *
 * Returns true if an unreverted entry was found, filling in the output
 * parameters.
 */
bool
ATMGetNextUnreverted(TransactionId *xid_out, Oid *dboid_out,
					 XLogRecPtr *lsn_out)
{
	return SLogTxnGetNextUnreverted(xid_out, dboid_out, lsn_out);
}

/*
 * ATMGetOldestUnrevertedLSN
 *		Return the oldest last_batch_lsn across all unreverted ATM entries.
 *
 * Used by the WAL retention logic to prevent recycling WAL segments that
 * still contain UNDO batches needed by the logical revert worker.
 * Returns InvalidXLogRecPtr if no unreverted entries exist.
 */
XLogRecPtr
ATMGetOldestUnrevertedLSN(void)
{
	return SLogTxnGetOldestUnrevertedLSN();
}

/*
 * ATMRecoveryFinalize
 *		Called at the end of recovery to log the ATM state.
 *
 * After WAL redo has reconstructed the ATM via sLog, this logs the number
 * of unreverted entries so the DBA can see how much Logical Revert work
 * remains.
 */
void
ATMRecoveryFinalize(void)
{
	int			total = 0;
	int			unreverted = 0;

	SLogRecoveryFinalize(&total, &unreverted);

	if (total > 0)
		elog(LOG, "ATM recovery complete: %d entries, %d unreverted",
			 total, unreverted);
}

/*
 * atm_redo
 *		WAL redo handler for ATM resource manager.
 */
void
atm_redo(XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info)
	{
		case XLOG_ATM_ABORT:
			{
				xl_atm_abort *xlrec =
					(xl_atm_abort *) XLogRecGetData(record);

				ATMAddAbortedInternal(xlrec->xid, xlrec->dboid,
									  xlrec->reloid, xlrec->last_batch_lsn);
			}
			break;

		case XLOG_ATM_FORGET:
			{
				xl_atm_forget *xlrec =
					(xl_atm_forget *) XLogRecGetData(record);

				ATMForgetInternal(xlrec->xid);
			}
			break;

		default:
			elog(PANIC, "atm_redo: unknown op code %u", info);
			break;
	}
}
