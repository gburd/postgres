/*-------------------------------------------------------------------------
 *
 * slog.h
 *	  Secondary Log (sLog) for shared-memory tracking
 *
 * The sLog provides a shared-memory Aborted Transaction Map (ATM) for the
 * UNDO subsystem's Constant-Time Recovery:
 *
 *   Transaction radix tree - Aborted transaction entries keyed by
 *   (xid, reloid) packed into a uint64, ordered for efficient xid-based
 *   range operations.  Protected by a single LWLock (modifications are
 *   infrequent).
 *
 * An optional per-tuple flat-hash extension (the tuple sLog) adds
 * bounded-recovery uncommitted-writer tracking for in-place-update table
 * AMs.  It shares this subsystem's shared-memory segment and initialization
 * but is not required by the UNDO core; its API is declared where it is
 * defined.
 *
 * WAL: Transaction sLog reuses existing RM_ATM_ID records.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/slog.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SLOG_H
#define SLOG_H

#include "access/transam.h"
#include "access/xlogdefs.h"
#include "datatype/timestamp.h"
#include "storage/lwlock.h"
#include "utils/dsa.h"

/* ----------------------------------------------------------------
 * Transaction sLog structures
 *
 * SLogTxnEntry is used as the public output type for lookups.
 * Internally, the ATM radix tree stores only the data fields; the key
 * (xid, reloid) is implicit in the tree path.
 * ----------------------------------------------------------------
 */

/*
 * SLogTxnEntry - Public output structure for transaction lookups.
 * Callers receive copies of this via SLogTxnLookup().
 */
typedef struct SLogTxnEntry
{
	TransactionId xid;
	Oid			reloid;
	XLogRecPtr	last_batch_lsn; /* LSN of last UNDO batch for this xid */
	Oid			dboid;			/* database OID */
	TimestampTz abort_time;		/* when transaction aborted */
	bool		revert_complete;	/* has Logical Revert finished? */
} SLogTxnEntry;

/* ----------------------------------------------------------------
 * Shared state
 *
 * The transaction radix tree is allocated in shared memory; its internal
 * structures are opaque to callers.  The SLogSharedState is defined in
 * slog.c.
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 * API: Shared memory
 * ----------------------------------------------------------------
 */
extern Size SLogShmemSize(void);
extern void SLogShmemRequest(void);
extern void SLogShmemInit(void);

/* ----------------------------------------------------------------
 * API: Transaction sLog
 * ----------------------------------------------------------------
 */
extern bool SLogTxnInsert(TransactionId xid, Oid reloid, Oid dboid,
						  XLogRecPtr last_batch_lsn);
extern bool SLogTxnLookup(TransactionId xid, Oid reloid,
						  SLogTxnEntry *entry_out);
extern bool SLogTxnLookupByXid(TransactionId xid, XLogRecPtr *lsn_out);
extern void SLogTxnRemove(TransactionId xid, Oid reloid);
extern void SLogTxnRemoveByXid(TransactionId xid);
extern void SLogTxnMarkReverted(TransactionId xid);
extern bool SLogTxnGetNextUnreverted(TransactionId *xid_out, Oid *dboid_out,
									 XLogRecPtr *lsn_out);
extern XLogRecPtr SLogTxnGetOldestUnrevertedLSN(void);
extern int	SLogTxnSnapshotForCheckpoint(SLogTxnEntry **entries_out);
extern void SLogRecoveryFinalize(int *total_out, int *unreverted_out);

/* DSA lifecycle (shared-memory area backing the aborted-txn radix tree) */
extern void SLogEnsureDsaAttached(void);

/* GUC: maximum DSA size (in MB) */
extern int	slog_dsa_max_size_mb;

#endif							/* SLOG_H */
