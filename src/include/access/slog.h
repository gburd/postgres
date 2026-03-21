/*-------------------------------------------------------------------------
 *
 * slog.h
 *	  Secondary Log (sLog) for shared-memory hash-based tracking
 *
 * The sLog provides three shared-memory hash tables for O(1) lookups:
 *
 *   1. SLogTxnHash   - Aborted transaction entries keyed by (xid, reloid).
 *                       Replaces the old fixed-array ATM with a hash table.
 *
 *   2. SLogXidHash   - Per-xid presence refcount for O(1) ATMIsAborted().
 *                       Secondary index into SLogTxnHash.
 *
 *   3. SLogTupleHash - Per-tuple operation tracking keyed by (relid, tid).
 *                       Designed for RECNO table AM timestamp-based MVCC.
 *
 * Locking uses partitioned LWLocks: txn_locks[16] for TxnHash + XidHash,
 * tuple_locks[32] for TupleHash.  All under LWTRANCHE_SLOG.
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

/* Forward declaration from relundo.h */
typedef uint64 RelUndoRecPtr;

/*
 * Partition counts for LWLock arrays.
 *
 * These are kept small to limit shared memory overhead from partitioned
 * hash tables (each partition allocates a segment of 256 bucket pointers).
 * Increase if contention becomes a bottleneck under high abort rates.
 */
#define NUM_SLOG_TXN_PARTITIONS		4
#define NUM_SLOG_TUPLE_PARTITIONS	4

/*
 * Hash table capacity constants (fixed-size, do not grow).
 *
 * TXN and XID sizes are kept small to fit within the shared memory
 * budget of a minimal-config server (shared_buffers = 400kB during initdb).
 * The old ATM flat array held 1024 entries in ~58KB; hash tables have
 * higher per-entry overhead, so we use 256 entries (~20KB each).
 *
 * TUPLE size is minimal (16) as the RECNO table AM is not yet implemented;
 * increase when RECNO lands.
 */
#define SLOG_TXN_HASH_SIZE		256
#define SLOG_XID_HASH_SIZE		256
#define SLOG_TUPLE_HASH_SIZE	16
#define SLOG_MAX_TUPLE_OPS		8

/*
 * sLog entry types for tuple operations.
 */
typedef enum SLogOpType
{
	SLOG_ENTRY_ABORTED_TXN = 0,	/* Transaction-level abort entry */
	SLOG_OP_INSERT = 1,
	SLOG_OP_DELETE = 2,
	SLOG_OP_UPDATE = 3,
	SLOG_OP_LOCK_SHARE = 4,
	SLOG_OP_LOCK_EXCL = 5,
} SLogOpType;

/* ----------------------------------------------------------------
 * Transaction sLog structures
 * ----------------------------------------------------------------
 */

/*
 * SLogTxnKey - Hash key for SLogTxnHash.
 */
typedef struct SLogTxnKey
{
	TransactionId xid;
	Oid			reloid;
} SLogTxnKey;

/*
 * SLogTxnEntry - Value in SLogTxnHash.
 */
typedef struct SLogTxnEntry
{
	SLogTxnKey	key;			/* hash key */
	RelUndoRecPtr undo_chain;	/* head of per-relation UNDO chain */
	Oid			dboid;			/* database OID */
	TimestampTz abort_time;		/* when transaction aborted */
	bool		revert_complete;	/* has Logical Revert finished? */
} SLogTxnEntry;

/*
 * SLogXidPresence - Value in SLogXidHash (secondary index).
 */
typedef struct SLogXidPresence
{
	TransactionId xid;			/* hash key */
	int			refcount;		/* number of SLogTxnHash entries for this xid */
} SLogXidPresence;

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
} SLogTupleKey;

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
} SLogTupleOp;

/*
 * SLogTupleEntry - Value in SLogTupleHash.
 */
typedef struct SLogTupleEntry
{
	SLogTupleKey key;			/* hash key */
	int			nops;			/* number of active operations */
	SLogTupleOp ops[SLOG_MAX_TUPLE_OPS];
} SLogTupleEntry;

/* ----------------------------------------------------------------
 * Shared state
 * ----------------------------------------------------------------
 */

/*
 * SLogSharedState - Top-level shared memory structure.
 */
typedef struct SLogSharedState
{
	LWLockPadded txn_locks[NUM_SLOG_TXN_PARTITIONS];
	LWLockPadded tuple_locks[NUM_SLOG_TUPLE_PARTITIONS];
} SLogSharedState;

/* ----------------------------------------------------------------
 * Callback type for tuple iteration
 * ----------------------------------------------------------------
 */
typedef bool (*SLogTupleIterCallback)(const SLogTupleOp *op, void *arg);

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
						  SLogTxnEntry *entry_out);
extern bool SLogTxnLookupByXid(TransactionId xid, RelUndoRecPtr *chain_out);
extern void SLogTxnRemove(TransactionId xid, Oid reloid);
extern void SLogTxnRemoveByXid(TransactionId xid);
extern void SLogTxnMarkReverted(TransactionId xid);
extern bool SLogTxnGetNextUnreverted(TransactionId *xid_out, Oid *dboid_out,
									 Oid *reloid_out,
									 RelUndoRecPtr *chain_out);
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
							SLogTupleEntry *entry_out);
extern void SLogTupleRemove(Oid relid, ItemPointer tid, TransactionId xid);
extern void SLogTupleRemoveByXid(TransactionId xid);
extern void SLogTupleRemoveBySubXid(TransactionId xid, TransactionId subxid);
extern void SLogTupleIterateByTid(Oid relid, ItemPointer tid,
								  SLogTupleIterCallback callback, void *arg);

/* Backend-private tracking for cleanup at commit/abort */
extern void SLogTupleTrackKey(SLogTupleKey key);
extern void SLogTupleResetTracking(void);

#endif							/* SLOG_H */
