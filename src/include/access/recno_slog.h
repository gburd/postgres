/*-------------------------------------------------------------------------
 *
 * recno_slog.h
 *	  RECNO secondary log (sLog) for transient operation tracking
 *
 * The sLog is a shared-memory hash table that tracks non-versioned,
 * transient coordination state: who is currently operating on which tuple.
 * It replaces t_xmin, t_xmax, and MultiXact for in-progress transaction
 * tracking, allowing the tuple header to contain only the committed
 * timestamp.
 *
 * Based on the Antonopoulos CTR paper (Section 3.4.1): "a secondary log
 * stream designed to only track non-versioned operations" -- lock
 * acquisitions, pending operations, coordination metadata -- that exist
 * only while transactions are in progress and are cleaned up at
 * commit/abort.
 *
 * Three roles:
 *   1. SNAPSHOT_DIRTY: provides xmin/xmax for ON CONFLICT / XactLockTableWait
 *   2. Tuple Locking: replaces t_xmax/MultiXact for SELECT FOR UPDATE/SHARE
 *   3. Self-Visibility: "did I insert this?" check replaces t_xmin lookup
 *
 * Implementation:
 *   The hash table is keyed by (tid, relid).  Each entry stores a small
 *   inline array of operations (one per transaction operating on that TID).
 *   This makes every TID-based lookup a single O(1) hash probe in a single
 *   partition, eliminating the former slow path that scanned all entries.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/recno_slog.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RECNO_SLOG_H
#define RECNO_SLOG_H

#include "postgres.h"

#include "access/xact.h"
#include "storage/itemptr.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/rel.h"

/*
 * sLog operation types.
 *
 * Each entry records what a transaction is doing (or has done but not yet
 * committed) to a particular tuple.
 */
typedef enum RecnoSLogOpType
{
	RECNO_SLOG_INSERT = 1,
	RECNO_SLOG_DELETE = 2,
	RECNO_SLOG_UPDATE = 3,
	RECNO_SLOG_LOCK_SHARE = 4,
	RECNO_SLOG_LOCK_EXCL = 5,
	RECNO_SLOG_ABORTED = 6		/* Marked at abort; UNDO pending */
} RecnoSLogOpType;

/*
 * Maximum concurrent operations on a single TID.
 *
 * Under EPQ retry patterns with high concurrency, each backend that sees
 * TM_Updated calls table_tuple_lock which adds a LOCK_EXCL sLog entry.
 * These entries persist until the owning transaction commits, so with N
 * concurrent backends hitting the same hot row, up to N-1 LOCK entries
 * can coexist.  32 slots handles realistic OLTP concurrency levels.
 * Stale-slot reclamation kicks in before the overflow ERROR.
 */
#define RECNO_SLOG_MAX_OPS		32

/*
 * sLog entry -- returned by RecnoSLogLookup.
 *
 * This is the public API struct.  Multiple entries may be returned for
 * the same TID when multiple transactions hold concurrent locks.
 */
typedef struct RecnoSLogEntry
{
	/* Key fields */
	ItemPointerData tid;			/* Target tuple TID */
	Oid				relid;			/* Relation OID */
	TransactionId	xid;			/* Operating transaction's XID */

	/* Payload */
	uint64			commit_ts;		/* Operation's commit_ts (for self-visibility) */
	uint32			spec_token;		/* Speculative insertion token (0 if none) */
	CommandId		cid;			/* Command ID within transaction */
	uint8			op_type;		/* RecnoSLogOpType */
	SubTransactionId subxid;		/* Subtransaction ID (for rollback granularity) */
} RecnoSLogEntry;

/*
 * Hash key for sLog lookups -- keyed by (tid, relid) only.
 *
 * All operations on the same TID hash to the same entry and partition,
 * making every TID-based lookup a single O(1) hash probe.
 */
typedef struct RecnoSLogKey
{
	ItemPointerData tid;
	Oid				relid;
} RecnoSLogKey;

/*
 * Number of sLog hash partitions for LWLock scalability.
 * Must be a power of 2.  128 partitions reduces partition lock contention
 * by 8x compared to 16 partitions, helping workloads that touch many
 * distinct TIDs concurrently (e.g., pgbench tellers/accounts).
 */
#define RECNO_SLOG_PARTITIONS		128
#define RECNO_SLOG_PARTITION_MASK	(RECNO_SLOG_PARTITIONS - 1)

/* Shared memory sizing and initialization */
extern Size RecnoSLogShmemSize(void);
extern void RecnoSLogShmemInit(void);
extern const ShmemCallbacks RecnoSLogShmemCallbacks;

/* --- Core sLog API --- */

/*
 * RecnoSLogInsert -- register an in-progress operation on a tuple.
 *
 * Called at the start of INSERT, DELETE, UPDATE, or tuple lock.
 * The entry persists until the transaction commits or aborts.
 */
extern void RecnoSLogInsert(Oid relid, ItemPointer tid,
							TransactionId xid, uint64 commit_ts,
							CommandId cid, RecnoSLogOpType op_type,
							SubTransactionId subxid,
							uint32 spec_token);

/*
 * RecnoSLogTrackSubXact -- lightweight subtransaction tracking for inserts.
 *
 * Records (tid, xid, subxid) in the per-backend local list WITHOUT creating
 * a shared sLog entry.  On subtransaction abort, RecnoSLogRemoveBySubXid
 * will create a shared ABORTED entry for visibility checks.
 */
extern void RecnoSLogTrackSubXact(Oid relid, ItemPointer tid,
								  TransactionId xid,
								  SubTransactionId subxid);

/*
 * RecnoSLogLookup -- find sLog entries for a given TID.
 *
 * Returns the number of matching entries found.  Up to max_entries
 * are copied into the caller-supplied entries[] array.
 *
 * If xid_filter is valid, only entries for that specific xid are returned.
 * If xid_filter is InvalidTransactionId, all entries for the TID are returned.
 *
 * Both paths use a single O(1) hash probe in a single partition.
 */
extern int RecnoSLogLookup(Oid relid, ItemPointer tid,
						   TransactionId xid_filter,
						   RecnoSLogEntry *entries, int max_entries);

/*
 * RecnoSLogLookupAll -- find ALL sLog entries for a TID in a single probe.
 *
 * Like RecnoSLogLookup with xid_filter=InvalidTransactionId, but designed
 * for callers that need to inspect all entries for a TID without making
 * multiple separate lookups.  This collapses multiple partition lock
 * acquisitions into one, significantly reducing contention on hot rows.
 *
 * Returns the number of entries found.
 */
extern int RecnoSLogLookupAll(Oid relid, ItemPointer tid,
							  RecnoSLogEntry *entries, int max_entries);

/*
 * RecnoSLogRemove -- remove a specific entry by (tid, relid, xid).
 *
 * Returns true if an entry was found and removed.
 */
extern bool RecnoSLogRemove(Oid relid, ItemPointer tid,
							TransactionId xid);

/*
 * RecnoSLogRemoveByXid -- remove ALL entries for a transaction.
 *
 * Called at commit/abort to clean up all entries for the transaction.
 * This is the primary cleanup path.  Uses the backend-local tracking
 * list for O(1) per-entry removal.
 */
extern void RecnoSLogRemoveByXid(TransactionId xid);

/*
 * RecnoSLogRemoveBySubXid -- mark entries ABORTED for a subtransaction.
 *
 * Called at ROLLBACK TO savepoint.
 */
extern void RecnoSLogRemoveBySubXid(TransactionId xid,
									SubTransactionId subxid);

/*
 * RecnoSLogUpdateSubXid -- update subxid of entries on subtxn commit.
 *
 * When a subtransaction commits, its entries' subxid is updated to the
 * parent's subxid so they survive subtransaction commit but are still
 * cleaned up at top-level commit.
 */
extern void RecnoSLogUpdateSubXid(TransactionId xid,
								  SubTransactionId old_subxid,
								  SubTransactionId new_subxid);

/* --- Convenience wrappers --- */

/*
 * RecnoSLogHasEntry -- quick probe: does ANY active sLog entry exist for
 * this TID?  Uses a single SHARED partition lock + HASH_FIND.
 * Returns true if at least one active entry exists.
 */
extern bool RecnoSLogHasEntry(Oid relid, ItemPointer tid);

/*
 * RecnoSLogIsInsertedByMe -- check if the current transaction inserted
 * this tuple (self-visibility check).
 */
extern bool RecnoSLogIsInsertedByMe(Oid relid, ItemPointer tid);

/*
 * RecnoSLogIsDeletedByMe -- check if the current transaction deleted
 * this tuple.
 */
extern bool RecnoSLogIsDeletedByMe(Oid relid, ItemPointer tid);

/*
 * RecnoSLogGetDirtyXid -- for SNAPSHOT_DIRTY, get the xid of the
 * in-progress transaction operating on this tuple.
 *
 * Returns the xid of the first in-progress INSERT or DELETE/UPDATE
 * entry found, or InvalidTransactionId if none.
 *
 * Sets *is_insert to true if the entry is an INSERT, false if DELETE/UPDATE.
 */
extern TransactionId RecnoSLogGetDirtyXid(Oid relid, ItemPointer tid,
										  bool *is_insert);

/*
 * RecnoSLogHasLockConflict -- check if any active lock entries on this
 * TID conflict with the requested lock mode.
 *
 * Returns true if there's a conflict, false if locks are compatible.
 */
extern bool RecnoSLogHasLockConflict(Oid relid, ItemPointer tid,
									 TransactionId my_xid,
									 RecnoSLogOpType requested_lock);

/*
 * RecnoSLogMarkAborted -- mark all entries for a transaction as ABORTED.
 * Called at transaction abort instead of removing entries, so that
 * visibility checks can distinguish "committed (sLog cleaned up)"
 * from "aborted (UNDO pending)".  The entries are removed later by the
 * UNDO worker after physically restoring the tuples.
 */
extern void RecnoSLogMarkAborted(TransactionId xid);

/*
 * RecnoSLogHasAbortedEntry -- check if any aborted sLog entry exists
 * for this TID.  Used by visibility checks: when UNCOMMITTED or
 * DELETED/UPDATED flag is set and no in-progress operation is found,
 * an aborted entry means the operation was rolled back and UNDO hasn't
 * been applied yet.  Checks both explicit ABORTED markers and CLOG.
 */
extern bool RecnoSLogHasAbortedEntry(Oid relid, ItemPointer tid);

/*
 * RecnoSLogRemoveByXidGlobal -- remove ALL entries for a transaction by
 * scanning the shared hash table directly (without the backend-local
 * tracking list).  Used by the UNDO worker to clean up ABORTED entries
 * after UNDO has been applied.
 */
extern void RecnoSLogRemoveByXidGlobal(TransactionId xid);

/* Transaction callbacks -- registered once per backend */
extern void RecnoSLogXactCallback(XactEvent event, void *arg);
extern void RecnoSLogSubXactCallback(SubXactEvent event,
									 SubTransactionId mySubid,
									 SubTransactionId parentSubid,
									 void *arg);

#endif							/* RECNO_SLOG_H */
