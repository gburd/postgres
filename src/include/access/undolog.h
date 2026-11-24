/*-------------------------------------------------------------------------
 *
 * undolog.h
 *	  WAL retention for UNDO records
 *
 * UNDO records live in the WAL stream as XLOG_UNDO_BATCH records, so the
 * WAL holding a transaction's UNDO must not be recycled while something may
 * still read it: the transaction's own abort, ROLLBACK PREPARED, or crash
 * recovery.  Each backend publishes the LSN of its current transaction's
 * first UNDO batch in a shared slot, and KeepLogSeg() keeps WAL from the
 * oldest such LSN onward.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/undolog.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef UNDOLOG_H
#define UNDOLOG_H

#include "access/xlogdefs.h"
#include "port/atomics.h"

/*
 * The per-backend first-batch LSN slots, MaxBackends of them, indexed by
 * MyProcNumber.  A slot holds the LSN of the first XLOG_UNDO_BATCH record of
 * the backend's current transaction, or InvalidXLogRecPtr.  Only the owning
 * backend writes its slot.
 */
extern PGDLLIMPORT pg_atomic_uint64 *UndoBatchLSNs;

extern void UndoLogShmemRequest(void);
extern void UndoLogShmemInit(void);

extern void UndoRegisterBatchLSN(XLogRecPtr batch_lsn);
extern void UndoClearBatchLSN(void);
extern XLogRecPtr UndoGetOldestBatchLSN(void);

#endif							/* UNDOLOG_H */
