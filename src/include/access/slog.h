/*-------------------------------------------------------------------------
 *
 * slog.h
 *	  Secondary Log (sLog) for shared-memory tracking
 *
 * The sLog provides shared-memory data structures for O(1) lookups:
 *
 *   1. Transaction skip-list - Aborted transaction entries keyed by
 *      (xid, reloid), ordered for efficient xid-based range operations.
 *      Protected by a single LWLock (modifications are infrequent).
 *
 *   2. XID sparsemap - Compressed bitmap for O(1) SLogXidIsPresent().
 *      Protected by a SpinLock (operations are very fast).
 *
 *   3. SLogTupleHash - Per-tuple operation tracking keyed by (relid, tid).
 *      Designed for RECNO table AM timestamp-based MVCC.
 *      Partitioned by tuple_locks[NUM_SLOG_TUPLE_PARTITIONS].
 *
 * WAL: Transaction sLog reuses existing RM_ATM_ID records.  Tuple sLog
 * is WAL-free (transient entries removed at commit/abort).
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
#include "datatype/timestamp.h"
#include "storage/itemptr.h"
#include "storage/lwlock.h"
#include "storage/spin.h"

/* Forward declaration from relundo.h */
typedef uint64 RelUndoRecPtr;

/*
 * Partition count for tuple LWLock array.
 */
#define NUM_SLOG_TUPLE_PARTITIONS	4

/*
 * Tuple hash capacity (fixed-size, does not grow).
 * Minimal (16) as the RECNO table AM is not yet implemented.
 */
#define SLOG_TUPLE_HASH_SIZE	16
#define SLOG_MAX_TUPLE_OPS		8

/*
 * Skip-list pool capacity: 256 user entries + 2 sentinels + 2 margin.
 */
#define SLOG_TXN_POOL_CAPACITY		260

/*
 * Sparsemap buffer size for XID presence bitmap (64KB).
 */
#define SLOG_XID_MAP_BUFSIZE		65536

/*
 * sLog entry types for tuple operations.
 */
typedef enum SLogOpType
{
	SLOG_ENTRY_ABORTED_TXN = 0, /* Transaction-level abort entry */
	SLOG_OP_INSERT = 1,
	SLOG_OP_DELETE = 2,
	SLOG_OP_UPDATE = 3,
	SLOG_OP_LOCK_SHARE = 4,
	SLOG_OP_LOCK_EXCL = 5,
}			SLogOpType;

/* ----------------------------------------------------------------
 * Transaction sLog structures
 *
 * SLogTxnEntry is used as the public output type for lookups.
 * Internally, the skip-list node (SLogTxnNode) contains these same
 * fields plus skip-list metadata.
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
	RelUndoRecPtr undo_chain;	/* head of per-relation UNDO chain */
	Oid			dboid;			/* database OID */
	TimestampTz abort_time;		/* when transaction aborted */
	bool		revert_complete;	/* has Logical Revert finished? */
}			SLogTxnEntry;

/* ----------------------------------------------------------------
 * Tuple sLog structures
 * ----------------------------------------------------------------
 */

/*
 * SLogTupleKey - Hash key for SLogTupleHash.
 *
 * Note: ItemPointerData is 6 bytes.  Always memset(&key, 0, sizeof(key))
 * before populating to ensure deterministic hashing with HASH_BLOBS.
 */
typedef struct SLogTupleKey
{
	Oid			relid;
	ItemPointerData tid;
}			SLogTupleKey;

/*
 * SLogTupleOp - Single operation on a tuple.
 */
typedef struct SLogTupleOp
{
	TransactionId xid;
	TransactionId subxid;		/* subtransaction ID, or InvalidTransactionId */
	SLogOpType	op_type;
	CommandId	cid;
	TimestampTz commit_ts;		/* 0 if not yet committed */
	uint32		spec_token;		/* speculative insertion token, or 0 */
}			SLogTupleOp;

/*
 * SLogTupleEntry - Value in SLogTupleHash.
 */
typedef struct SLogTupleEntry
{
	SLogTupleKey key;			/* hash key */
	int			nops;			/* number of active operations */
	SLogTupleOp ops[SLOG_MAX_TUPLE_OPS];
}			SLogTupleEntry;

/* ----------------------------------------------------------------
 * Shared state
 *
 * The transaction skip-list and XID sparsemap are allocated in
 * shared memory; their internal structures are opaque to callers.
 * The SLogSharedState is defined in slog.c.
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 * Callback type for tuple iteration
 * ----------------------------------------------------------------
 */
typedef bool (*SLogTupleIterCallback) (const SLogTupleOp * op, void *arg);

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
						  RelUndoRecPtr chain);
extern bool SLogXidIsPresent(TransactionId xid);
extern bool SLogTxnLookup(TransactionId xid, Oid reloid,
						  SLogTxnEntry * entry_out);
extern bool SLogTxnLookupByXid(TransactionId xid, RelUndoRecPtr * chain_out);
extern void SLogTxnRemove(TransactionId xid, Oid reloid);
extern void SLogTxnRemoveByXid(TransactionId xid);
extern void SLogTxnMarkReverted(TransactionId xid);
extern bool SLogTxnGetNextUnreverted(TransactionId *xid_out, Oid *dboid_out,
									 Oid *reloid_out,
									 RelUndoRecPtr * chain_out);
extern void SLogRecoveryFinalize(int *total_out, int *unreverted_out);

/* ----------------------------------------------------------------
 * API: Tuple sLog
 * ----------------------------------------------------------------
 */
extern bool SLogTupleInsert(Oid relid, ItemPointer tid, TransactionId xid,
							SLogOpType op_type, TransactionId subxid,
							CommandId cid, TimestampTz commit_ts,
							uint32 spec_token);
extern bool SLogTupleLookup(Oid relid, ItemPointer tid,
							SLogTupleEntry * entry_out);
extern void SLogTupleRemove(Oid relid, ItemPointer tid, TransactionId xid);
extern void SLogTupleRemoveByXid(TransactionId xid);
extern void SLogTupleRemoveBySubXid(TransactionId xid, TransactionId subxid);
extern void SLogTupleIterateByTid(Oid relid, ItemPointer tid,
								  SLogTupleIterCallback callback, void *arg);

/* Backend-private tracking for cleanup at commit/abort */
extern void SLogTupleTrackKey(SLogTupleKey key);
extern void SLogTupleResetTracking(void);

#endif							/* SLOG_H */
