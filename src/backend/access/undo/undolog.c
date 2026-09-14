/*-------------------------------------------------------------------------
 *
 * undolog.c
 *	  WAL retention for UNDO records
 *
 * See access/undolog.h.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/undo/undolog.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/twophase.h"
#include "access/undolog.h"
#include "miscadmin.h"
#include "storage/procnumber.h"
#include "storage/shmem.h"

pg_atomic_uint64 *UndoBatchLSNs = NULL;

/*
 * UndoLogShmemRequest
 *		Request the per-backend slots from the shared memory framework.
 *
 * Called from the UNDO subsystem's request_fn.  The framework sets
 * UndoBatchLSNs, in the postmaster and, under EXEC_BACKEND, in every child.
 */
void
UndoLogShmemRequest(void)
{
	ShmemRequestStruct(.name = "UNDO Log Control",
					   .size = mul_size(MaxBackends, sizeof(pg_atomic_uint64)),
					   .ptr = (void **) &UndoBatchLSNs);
}

/*
 * UndoLogShmemInit
 *		Initialize the slots requested by UndoLogShmemRequest().
 */
void
UndoLogShmemInit(void)
{
	for (int i = 0; i < MaxBackends; i++)
		pg_atomic_init_u64(&UndoBatchLSNs[i],
						   InvalidXLogRecPtr);
}

/*
 * UndoRegisterBatchLSN
 *		Register the first UNDO batch LSN for the current backend.
 *
 * Called from UndoRecordSetInsert() each time a transaction writes an UNDO
 * batch.  Stores the LSN in the backend's slot, which KeepLogSeg() reads
 * through UndoGetOldestBatchLSN().
 *
 * Only the FIRST call per transaction takes effect (we want the oldest,
 * i.e., smallest, LSN).  Subsequent calls for the same transaction are
 * no-ops because the slot is already occupied.
 */
void
UndoRegisterBatchLSN(XLogRecPtr batch_lsn)
{
	pg_atomic_uint64 *slot;
	uint64		expected;

	if (UndoBatchLSNs == NULL || !XLogRecPtrIsValid(batch_lsn))
		return;
	if (MyProcNumber < 0 || MyProcNumber >= MaxBackends)
		return;

	slot = &UndoBatchLSNs[MyProcNumber];
	expected = InvalidXLogRecPtr;

	/*
	 * Only set if the slot is currently empty.  This records the first
	 * (oldest) batch for this transaction; later batches have larger LSNs and
	 * should not overwrite the stored value.
	 */
	(void) pg_atomic_compare_exchange_u64(slot, &expected, (uint64) batch_lsn);
}

/*
 * UndoClearBatchLSN
 *		Clear the per-backend UNDO batch LSN registration.
 *
 * Called at transaction commit or abort to release the WAL retention
 * hold that was established by UndoRegisterBatchLSN().
 */
void
UndoClearBatchLSN(void)
{
	if (UndoBatchLSNs == NULL)
		return;
	if (MyProcNumber < 0 || MyProcNumber >= MaxBackends)
		return;

	pg_atomic_write_u64(&UndoBatchLSNs[MyProcNumber],
						(uint64) InvalidXLogRecPtr);
}

/*
 * UndoGetOldestBatchLSN
 *		Return the oldest UNDO batch LSN that must be retained in WAL.
 *
 * Considers both:
 *   1. Per-backend slots (in-flight transactions with UNDO data)
 *   2. Prepared transactions, whose chain ROLLBACK PREPARED still reads
 *
 * Returns InvalidXLogRecPtr if no WAL retention is needed for UNDO.
 */
XLogRecPtr
UndoGetOldestBatchLSN(void)
{
	XLogRecPtr	oldest = InvalidXLogRecPtr;
	int			i;

	if (UndoBatchLSNs == NULL)
		return InvalidXLogRecPtr;

	/* Check per-backend slots for in-flight transactions */
	for (i = 0; i < MaxBackends; i++)
	{
		XLogRecPtr	lsn = (XLogRecPtr)
			pg_atomic_read_u64(&UndoBatchLSNs[i]);

		if (XLogRecPtrIsValid(lsn))
		{
			if (!XLogRecPtrIsValid(oldest) || lsn < oldest)
				oldest = lsn;
		}
	}

	/*
	 * Check prepared (2PC) transactions.  A xact can sit PREPARED
	 * indefinitely; its UNDO-batch WAL must survive until ROLLBACK PREPARED
	 * reads it.  The per-backend slot is cleared when the preparing backend
	 * exits, so it does not cover this.
	 */
	{
		XLogRecPtr	prep_oldest = TwoPhaseGetOldestUndoBatchLSN();

		if (XLogRecPtrIsValid(prep_oldest) &&
			(!XLogRecPtrIsValid(oldest) || prep_oldest < oldest))
			oldest = prep_oldest;
	}

	return oldest;
}
