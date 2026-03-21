/*-------------------------------------------------------------------------
 *
 * slog.c
 *	  Secondary Log (sLog) — Hash-based shared-memory tracking
 *
 * The sLog replaces the old fixed-array ATM with three shared-memory hash
 * tables that provide O(1) lookups for aborted transaction tracking and
 * per-tuple operation tracking.
 *
 * Transaction sLog (SLogTxnHash + SLogXidHash):
 *   - SLogTxnHash keyed by (xid, reloid) stores full abort metadata.
 *   - SLogXidHash keyed by xid stores a refcount of TxnHash entries,
 *     enabling O(1) ATMIsAborted() via SLogXidIsPresent().
 *   - Partitioned by txn_locks[xid % NUM_SLOG_TXN_PARTITIONS].
 *
 * Tuple sLog (SLogTupleHash):
 *   - Keyed by (relid, tid), stores up to SLOG_MAX_TUPLE_OPS concurrent
 *     operations per tuple.  Designed for the RECNO table AM.
 *   - Partitioned by tuple_locks[hash(relid,tid) % NUM_SLOG_TUPLE_PARTITIONS].
 *   - WAL-free: entries are transient, removed at commit/abort.
 *
 * Locking: All locks are from LWTRANCHE_SLOG.  Transaction locks cover
 * both TxnHash and XidHash (same partition index = xid % 16).
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/undo/slog.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/slog.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

/* ----------------------------------------------------------------
 * Static variables
 * ----------------------------------------------------------------
 */
static HTAB *SLogTxnHash = NULL;
static HTAB *SLogXidHash = NULL;
static HTAB *SLogTupleHash = NULL;
static SLogSharedState *SLogState = NULL;

/* ----------------------------------------------------------------
 * Backend-private tracking for tuple sLog cleanup
 * ----------------------------------------------------------------
 */
typedef struct SLogTrackedKey
{
	SLogTupleKey key;
	struct SLogTrackedKey *next;
} SLogTrackedKey;

static SLogTrackedKey *slog_tracked_keys = NULL;

/* ----------------------------------------------------------------
 * Internal helpers
 * ----------------------------------------------------------------
 */

/*
 * Compute LWLock partition index for transaction operations.
 */
static inline int
SLogTxnPartition(TransactionId xid)
{
	return xid % NUM_SLOG_TXN_PARTITIONS;
}

/*
 * Compute LWLock partition index for tuple operations.
 */
static inline int
SLogTuplePartition(Oid relid, ItemPointer tid)
{
	uint32		h;

	h = (uint32) relid;
	h ^= (uint32) ItemPointerGetBlockNumber(tid);
	h ^= (uint32) ItemPointerGetOffsetNumber(tid);

	return h % NUM_SLOG_TUPLE_PARTITIONS;
}

/* ----------------------------------------------------------------
 * Shared memory sizing and initialization
 * ----------------------------------------------------------------
 */

/*
 * SLogShmemSize
 *		Calculate shared memory needed for the sLog.
 *
 * This is kept for informational purposes (e.g., UndoShmemSize()).
 * The actual shared memory registration is done by SLogShmemRequest().
 */
Size
SLogShmemSize(void)
{
	Size		size;

	size = MAXALIGN(sizeof(SLogSharedState));
	size = add_size(size, hash_estimate_size(SLOG_TXN_HASH_SIZE,
											 sizeof(SLogTxnEntry)));
	size = add_size(size, hash_estimate_size(SLOG_XID_HASH_SIZE,
											 sizeof(SLogXidPresence)));
	size = add_size(size, hash_estimate_size(SLOG_TUPLE_HASH_SIZE,
											 sizeof(SLogTupleEntry)));

	return size;
}

/*
 * SLogShmemRequest
 *		Register shared memory needs for the sLog.
 *
 * Called from UndoShmemRequest() during the request_fn phase of
 * postmaster startup, before shared memory is allocated.  Uses the
 * modern ShmemRequestStruct/ShmemRequestHash pattern so that
 * CalculateShmemSize() accounts for the sLog's hash tables.
 */
void
SLogShmemRequest(void)
{
	/* Register the shared state structure */
	ShmemRequestStruct(.name = "Secondary Log State",
					   .size = sizeof(SLogSharedState),
					   .ptr = (void **) &SLogState,
		);

	/* SLogTxnHash: keyed by SLogTxnKey */
	ShmemRequestHash(.name = "sLog Transaction Hash",
					 .nelems = SLOG_TXN_HASH_SIZE,
					 .ptr = &SLogTxnHash,
					 .hash_info.keysize = sizeof(SLogTxnKey),
					 .hash_info.entrysize = sizeof(SLogTxnEntry),
					 .hash_info.num_partitions = NUM_SLOG_TXN_PARTITIONS,
					 .hash_flags = HASH_ELEM | HASH_BLOBS |
					 HASH_PARTITION | HASH_FIXED_SIZE,
		);

	/* SLogXidHash: keyed by TransactionId */
	ShmemRequestHash(.name = "sLog XID Presence Hash",
					 .nelems = SLOG_XID_HASH_SIZE,
					 .ptr = &SLogXidHash,
					 .hash_info.keysize = sizeof(TransactionId),
					 .hash_info.entrysize = sizeof(SLogXidPresence),
					 .hash_info.num_partitions = NUM_SLOG_TXN_PARTITIONS,
					 .hash_flags = HASH_ELEM | HASH_BLOBS |
					 HASH_PARTITION | HASH_FIXED_SIZE,
		);

	/* SLogTupleHash: keyed by SLogTupleKey */
	ShmemRequestHash(.name = "sLog Tuple Hash",
					 .nelems = SLOG_TUPLE_HASH_SIZE,
					 .ptr = &SLogTupleHash,
					 .hash_info.keysize = sizeof(SLogTupleKey),
					 .hash_info.entrysize = sizeof(SLogTupleEntry),
					 .hash_info.num_partitions = NUM_SLOG_TUPLE_PARTITIONS,
					 .hash_flags = HASH_ELEM | HASH_BLOBS |
					 HASH_PARTITION | HASH_FIXED_SIZE,
		);
}

/*
 * SLogShmemInit
 *		Initialize sLog shared memory contents.
 *
 * Called from UndoShmemInit() during the init_fn phase.  By this point,
 * the framework has already allocated the shared state struct and hash
 * tables (via SLogShmemRequest), and set SLogState, SLogTxnHash,
 * SLogXidHash, SLogTupleHash.  We only need to initialize the LWLocks.
 */
void
SLogShmemInit(void)
{
	int			i;

	for (i = 0; i < NUM_SLOG_TXN_PARTITIONS; i++)
		LWLockInitialize(&SLogState->txn_locks[i].lock,
						 LWTRANCHE_SLOG);

	for (i = 0; i < NUM_SLOG_TUPLE_PARTITIONS; i++)
		LWLockInitialize(&SLogState->tuple_locks[i].lock,
						 LWTRANCHE_SLOG);
}

/* ================================================================
 * Transaction sLog functions
 * ================================================================
 */

/*
 * SLogTxnInsert
 *		Insert an aborted transaction entry into the sLog.
 *
 * Creates entries in both SLogTxnHash and SLogXidHash (refcount).
 * Returns false if the hash table is full.
 */
bool
SLogTxnInsert(TransactionId xid, Oid reloid, Oid dboid,
			  RelUndoRecPtr chain)
{
	int			partition = SLogTxnPartition(xid);
	SLogTxnKey	key;
	SLogTxnEntry *txn_entry;
	SLogXidPresence *xid_entry;
	bool		found;

	/* Zero the key struct for deterministic hashing */
	memset(&key, 0, sizeof(key));
	key.xid = xid;
	key.reloid = reloid;

	LWLockAcquire(&SLogState->txn_locks[partition].lock, LW_EXCLUSIVE);

	/* Insert into TxnHash */
	txn_entry = (SLogTxnEntry *)
		hash_search(SLogTxnHash, &key, HASH_ENTER_NULL, &found);

	if (txn_entry == NULL)
	{
		/* Table full */
		LWLockRelease(&SLogState->txn_locks[partition].lock);
		return false;
	}

	if (found)
	{
		/* Already present — no-op */
		LWLockRelease(&SLogState->txn_locks[partition].lock);
		return true;
	}

	/* Fill in the new entry */
	txn_entry->undo_chain = chain;
	txn_entry->dboid = dboid;
	txn_entry->abort_time = GetCurrentTimestamp();
	txn_entry->revert_complete = false;

	/* Increment XidHash refcount */
	xid_entry = (SLogXidPresence *)
		hash_search(SLogXidHash, &xid, HASH_ENTER_NULL, &found);

	if (xid_entry == NULL)
	{
		/*
		 * XidHash full — remove the TxnHash entry we just added to keep
		 * the two tables consistent.
		 */
		hash_search(SLogTxnHash, &key, HASH_REMOVE, NULL);
		LWLockRelease(&SLogState->txn_locks[partition].lock);
		return false;
	}

	if (!found)
		xid_entry->refcount = 0;

	xid_entry->refcount++;

	LWLockRelease(&SLogState->txn_locks[partition].lock);
	return true;
}

/*
 * SLogXidIsPresent
 *		O(1) check whether a transaction has any sLog entries.
 *
 * This is the hot-path replacement for the old O(N) ATMIsAborted scan.
 */
bool
SLogXidIsPresent(TransactionId xid)
{
	int			partition = SLogTxnPartition(xid);
	bool		found;

	LWLockAcquire(&SLogState->txn_locks[partition].lock, LW_SHARED);

	hash_search(SLogXidHash, &xid, HASH_FIND, &found);

	LWLockRelease(&SLogState->txn_locks[partition].lock);

	return found;
}

/*
 * SLogTxnLookup
 *		Look up a specific (xid, reloid) entry.
 *
 * Returns true if found, copying the entry into *entry_out.
 */
bool
SLogTxnLookup(TransactionId xid, Oid reloid, SLogTxnEntry *entry_out)
{
	int			partition = SLogTxnPartition(xid);
	SLogTxnKey	key;
	SLogTxnEntry *entry;
	bool		found;

	memset(&key, 0, sizeof(key));
	key.xid = xid;
	key.reloid = reloid;

	LWLockAcquire(&SLogState->txn_locks[partition].lock, LW_SHARED);

	entry = (SLogTxnEntry *)
		hash_search(SLogTxnHash, &key, HASH_FIND, &found);

	if (found && entry_out)
		memcpy(entry_out, entry, sizeof(SLogTxnEntry));

	LWLockRelease(&SLogState->txn_locks[partition].lock);

	return found;
}

/*
 * SLogTxnLookupByXid
 *		Find the UNDO chain for a given xid (any reloid).
 *
 * Uses hash_seq_search over TxnHash.  O(N) but called rarely
 * (only by ATMGetUndoChain).  Returns the first matching entry's chain.
 */
bool
SLogTxnLookupByXid(TransactionId xid, RelUndoRecPtr *chain_out)
{
	HASH_SEQ_STATUS status;
	SLogTxnEntry *entry;
	bool		found = false;

	hash_seq_init(&status, SLogTxnHash);

	while ((entry = (SLogTxnEntry *) hash_seq_search(&status)) != NULL)
	{
		if (entry->key.xid == xid)
		{
			if (chain_out)
				*chain_out = entry->undo_chain;
			found = true;
			hash_seq_term(&status);
			break;
		}
	}

	return found;
}

/*
 * SLogTxnRemove
 *		Remove a specific (xid, reloid) entry.
 *
 * Decrements the XidHash refcount, removing the xid entry when it
 * reaches zero.
 */
void
SLogTxnRemove(TransactionId xid, Oid reloid)
{
	int			partition = SLogTxnPartition(xid);
	SLogTxnKey	key;
	bool		found;
	SLogXidPresence *xid_entry;

	memset(&key, 0, sizeof(key));
	key.xid = xid;
	key.reloid = reloid;

	LWLockAcquire(&SLogState->txn_locks[partition].lock, LW_EXCLUSIVE);

	hash_search(SLogTxnHash, &key, HASH_REMOVE, &found);

	if (found)
	{
		xid_entry = (SLogXidPresence *)
			hash_search(SLogXidHash, &xid, HASH_FIND, NULL);

		if (xid_entry)
		{
			xid_entry->refcount--;
			if (xid_entry->refcount <= 0)
				hash_search(SLogXidHash, &xid, HASH_REMOVE, NULL);
		}
	}

	LWLockRelease(&SLogState->txn_locks[partition].lock);
}

/*
 * SLogTxnRemoveByXid
 *		Remove all sLog entries for a given transaction ID.
 *
 * Two-phase: collect matching keys under SHARED, then remove under
 * EXCLUSIVE.  This avoids modifying the hash table during seq scan.
 */
void
SLogTxnRemoveByXid(TransactionId xid)
{
	HASH_SEQ_STATUS status;
	SLogTxnEntry *entry;
	SLogTxnKey *keys;
	int			nkeys = 0;
	int			max_keys = 64;
	int			partition;
	int			i;

	keys = (SLogTxnKey *) palloc(max_keys * sizeof(SLogTxnKey));

	/* Phase 1: Collect keys */
	hash_seq_init(&status, SLogTxnHash);

	while ((entry = (SLogTxnEntry *) hash_seq_search(&status)) != NULL)
	{
		if (entry->key.xid == xid)
		{
			if (nkeys >= max_keys)
			{
				max_keys *= 2;
				keys = (SLogTxnKey *)
					repalloc(keys, max_keys * sizeof(SLogTxnKey));
			}
			memcpy(&keys[nkeys], &entry->key, sizeof(SLogTxnKey));
			nkeys++;
		}
	}

	/* Phase 2: Remove collected keys */
	for (i = 0; i < nkeys; i++)
	{
		partition = SLogTxnPartition(keys[i].xid);

		LWLockAcquire(&SLogState->txn_locks[partition].lock, LW_EXCLUSIVE);
		hash_search(SLogTxnHash, &keys[i], HASH_REMOVE, NULL);
		LWLockRelease(&SLogState->txn_locks[partition].lock);
	}

	/* Remove XidHash entry */
	if (nkeys > 0)
	{
		partition = SLogTxnPartition(xid);

		LWLockAcquire(&SLogState->txn_locks[partition].lock, LW_EXCLUSIVE);
		hash_search(SLogXidHash, &xid, HASH_REMOVE, NULL);
		LWLockRelease(&SLogState->txn_locks[partition].lock);
	}

	pfree(keys);
}

/*
 * SLogTxnMarkReverted
 *		Mark all entries for a given xid as revert_complete.
 *
 * Two-phase: collect keys under seq scan, then update under lock.
 */
void
SLogTxnMarkReverted(TransactionId xid)
{
	HASH_SEQ_STATUS status;
	SLogTxnEntry *entry;
	SLogTxnKey *keys;
	int			nkeys = 0;
	int			max_keys = 64;
	int			partition;
	int			i;

	keys = (SLogTxnKey *) palloc(max_keys * sizeof(SLogTxnKey));

	/* Phase 1: Collect keys */
	hash_seq_init(&status, SLogTxnHash);

	while ((entry = (SLogTxnEntry *) hash_seq_search(&status)) != NULL)
	{
		if (entry->key.xid == xid)
		{
			if (nkeys >= max_keys)
			{
				max_keys *= 2;
				keys = (SLogTxnKey *)
					repalloc(keys, max_keys * sizeof(SLogTxnKey));
			}
			memcpy(&keys[nkeys], &entry->key, sizeof(SLogTxnKey));
			nkeys++;
		}
	}

	/* Phase 2: Update under lock */
	for (i = 0; i < nkeys; i++)
	{
		bool		found;

		partition = SLogTxnPartition(keys[i].xid);

		LWLockAcquire(&SLogState->txn_locks[partition].lock, LW_EXCLUSIVE);

		entry = (SLogTxnEntry *)
			hash_search(SLogTxnHash, &keys[i], HASH_FIND, &found);

		if (found)
			entry->revert_complete = true;

		LWLockRelease(&SLogState->txn_locks[partition].lock);
	}

	pfree(keys);
}

/*
 * SLogTxnGetNextUnreverted
 *		Find an entry that hasn't been reverted yet.
 *
 * Uses hash_seq_search to scan, then verifies under partition lock.
 * Returns true if an unreverted entry was found.
 */
bool
SLogTxnGetNextUnreverted(TransactionId *xid_out, Oid *dboid_out,
						 Oid *reloid_out, RelUndoRecPtr *chain_out)
{
	HASH_SEQ_STATUS status;
	SLogTxnEntry *entry;

	hash_seq_init(&status, SLogTxnHash);

	while ((entry = (SLogTxnEntry *) hash_seq_search(&status)) != NULL)
	{
		int			partition;
		SLogTxnEntry *verified;
		bool		found;

		if (entry->revert_complete)
			continue;

		/*
		 * Found a candidate.  Verify under partition lock to ensure it's
		 * still valid.
		 */
		partition = SLogTxnPartition(entry->key.xid);

		LWLockAcquire(&SLogState->txn_locks[partition].lock, LW_SHARED);

		verified = (SLogTxnEntry *)
			hash_search(SLogTxnHash, &entry->key, HASH_FIND, &found);

		if (found && !verified->revert_complete)
		{
			*xid_out = verified->key.xid;
			*dboid_out = verified->dboid;
			*reloid_out = verified->key.reloid;
			*chain_out = verified->undo_chain;

			LWLockRelease(&SLogState->txn_locks[partition].lock);
			hash_seq_term(&status);
			return true;
		}

		LWLockRelease(&SLogState->txn_locks[partition].lock);
	}

	return false;
}

/*
 * SLogRecoveryFinalize
 *		Count entries after recovery for logging.
 */
void
SLogRecoveryFinalize(int *total_out, int *unreverted_out)
{
	HASH_SEQ_STATUS status;
	SLogTxnEntry *entry;
	int			total = 0;
	int			unreverted = 0;

	hash_seq_init(&status, SLogTxnHash);

	while ((entry = (SLogTxnEntry *) hash_seq_search(&status)) != NULL)
	{
		total++;
		if (!entry->revert_complete)
			unreverted++;
	}

	if (total_out)
		*total_out = total;
	if (unreverted_out)
		*unreverted_out = unreverted;
}

/* ================================================================
 * Tuple sLog functions
 * ================================================================
 */

/*
 * SLogTupleInsert
 *		Record a tuple operation in the sLog.
 *
 * Finds or creates an entry for (relid, tid) and appends the operation
 * to its ops[] array.  Returns false if the hash table is full or the
 * per-tuple ops array is full.
 *
 * Also adds the key to the backend-private tracking list for cleanup.
 */
bool
SLogTupleInsert(Oid relid, ItemPointer tid, TransactionId xid,
				SLogOpType op_type, TransactionId subxid,
				CommandId cid, TimestampTz commit_ts,
				uint32 spec_token)
{
	int			partition;
	SLogTupleKey key;
	SLogTupleEntry *entry;
	bool		found;

	/* Zero for deterministic hashing (ItemPointerData is 6 bytes) */
	memset(&key, 0, sizeof(key));
	key.relid = relid;
	ItemPointerCopy(tid, &key.tid);

	partition = SLogTuplePartition(relid, tid);

	LWLockAcquire(&SLogState->tuple_locks[partition].lock, LW_EXCLUSIVE);

	entry = (SLogTupleEntry *)
		hash_search(SLogTupleHash, &key, HASH_ENTER_NULL, &found);

	if (entry == NULL)
	{
		LWLockRelease(&SLogState->tuple_locks[partition].lock);
		return false;
	}

	if (!found)
		entry->nops = 0;

	if (entry->nops >= SLOG_MAX_TUPLE_OPS)
	{
		/* ops array full — if entry was just created with nops=0, remove it */
		if (!found)
			hash_search(SLogTupleHash, &key, HASH_REMOVE, NULL);
		LWLockRelease(&SLogState->tuple_locks[partition].lock);
		return false;
	}

	/* Append operation */
	entry->ops[entry->nops].xid = xid;
	entry->ops[entry->nops].subxid = subxid;
	entry->ops[entry->nops].op_type = op_type;
	entry->ops[entry->nops].cid = cid;
	entry->ops[entry->nops].commit_ts = commit_ts;
	entry->ops[entry->nops].spec_token = spec_token;
	entry->nops++;

	LWLockRelease(&SLogState->tuple_locks[partition].lock);

	/* Track for cleanup */
	SLogTupleTrackKey(key);

	return true;
}

/*
 * SLogTupleLookup
 *		Look up a tuple's sLog entry (copy semantics).
 *
 * Returns true if found, copying the full entry into *entry_out.
 */
bool
SLogTupleLookup(Oid relid, ItemPointer tid, SLogTupleEntry *entry_out)
{
	int			partition;
	SLogTupleKey key;
	SLogTupleEntry *entry;
	bool		found;

	memset(&key, 0, sizeof(key));
	key.relid = relid;
	ItemPointerCopy(tid, &key.tid);

	partition = SLogTuplePartition(relid, tid);

	LWLockAcquire(&SLogState->tuple_locks[partition].lock, LW_SHARED);

	entry = (SLogTupleEntry *)
		hash_search(SLogTupleHash, &key, HASH_FIND, &found);

	if (found && entry_out)
		memcpy(entry_out, entry, sizeof(SLogTupleEntry));

	LWLockRelease(&SLogState->tuple_locks[partition].lock);

	return found;
}

/*
 * SLogTupleRemove
 *		Remove operations for a specific xid from a tuple entry.
 *
 * Compacts the ops[] array.  Removes the hash entry if nops reaches 0.
 */
void
SLogTupleRemove(Oid relid, ItemPointer tid, TransactionId xid)
{
	int			partition;
	SLogTupleKey key;
	SLogTupleEntry *entry;
	bool		found;
	int			i,
				j;

	memset(&key, 0, sizeof(key));
	key.relid = relid;
	ItemPointerCopy(tid, &key.tid);

	partition = SLogTuplePartition(relid, tid);

	LWLockAcquire(&SLogState->tuple_locks[partition].lock, LW_EXCLUSIVE);

	entry = (SLogTupleEntry *)
		hash_search(SLogTupleHash, &key, HASH_FIND, &found);

	if (!found)
	{
		LWLockRelease(&SLogState->tuple_locks[partition].lock);
		return;
	}

	/* Compact: remove all ops matching xid */
	j = 0;
	for (i = 0; i < entry->nops; i++)
	{
		if (entry->ops[i].xid != xid)
		{
			if (j != i)
				memcpy(&entry->ops[j], &entry->ops[i], sizeof(SLogTupleOp));
			j++;
		}
	}
	entry->nops = j;

	/* Remove entry if empty */
	if (entry->nops == 0)
		hash_search(SLogTupleHash, &key, HASH_REMOVE, NULL);

	LWLockRelease(&SLogState->tuple_locks[partition].lock);
}

/*
 * SLogTupleRemoveByXid
 *		Remove all tuple sLog entries for a transaction.
 *
 * Uses the backend-private tracking list for O(tracked) cleanup
 * instead of a full hash scan.
 */
void
SLogTupleRemoveByXid(TransactionId xid)
{
	SLogTrackedKey *tk;

	for (tk = slog_tracked_keys; tk != NULL; tk = tk->next)
	{
		ItemPointer tid = &tk->key.tid;

		SLogTupleRemove(tk->key.relid, tid, xid);
	}

	SLogTupleResetTracking();
}

/*
 * SLogTupleRemoveBySubXid
 *		Remove tuple sLog entries matching a specific subtransaction.
 *
 * Like SLogTupleRemove but filters on subxid within each entry.
 */
void
SLogTupleRemoveBySubXid(TransactionId xid, TransactionId subxid)
{
	SLogTrackedKey *tk;

	for (tk = slog_tracked_keys; tk != NULL; tk = tk->next)
	{
		int			partition;
		SLogTupleEntry *entry;
		bool		found;
		int			i,
					j;

		partition = SLogTuplePartition(tk->key.relid, &tk->key.tid);

		LWLockAcquire(&SLogState->tuple_locks[partition].lock, LW_EXCLUSIVE);

		entry = (SLogTupleEntry *)
			hash_search(SLogTupleHash, &tk->key, HASH_FIND, &found);

		if (!found)
		{
			LWLockRelease(&SLogState->tuple_locks[partition].lock);
			continue;
		}

		/* Compact: remove ops matching both xid and subxid */
		j = 0;
		for (i = 0; i < entry->nops; i++)
		{
			if (entry->ops[i].xid != xid || entry->ops[i].subxid != subxid)
			{
				if (j != i)
					memcpy(&entry->ops[j], &entry->ops[i],
						   sizeof(SLogTupleOp));
				j++;
			}
		}
		entry->nops = j;

		if (entry->nops == 0)
			hash_search(SLogTupleHash, &tk->key, HASH_REMOVE, NULL);

		LWLockRelease(&SLogState->tuple_locks[partition].lock);
	}
}

/*
 * SLogTupleIterateByTid
 *		Call a callback for each operation on a tuple.
 *
 * The callback receives a pointer to each SLogTupleOp.  If the callback
 * returns false, iteration stops early.
 */
void
SLogTupleIterateByTid(Oid relid, ItemPointer tid,
					  SLogTupleIterCallback callback, void *arg)
{
	int			partition;
	SLogTupleKey key;
	SLogTupleEntry *entry;
	bool		found;
	int			i;

	memset(&key, 0, sizeof(key));
	key.relid = relid;
	ItemPointerCopy(tid, &key.tid);

	partition = SLogTuplePartition(relid, tid);

	LWLockAcquire(&SLogState->tuple_locks[partition].lock, LW_SHARED);

	entry = (SLogTupleEntry *)
		hash_search(SLogTupleHash, &key, HASH_FIND, &found);

	if (found)
	{
		for (i = 0; i < entry->nops; i++)
		{
			if (!callback(&entry->ops[i], arg))
				break;
		}
	}

	LWLockRelease(&SLogState->tuple_locks[partition].lock);
}

/* ----------------------------------------------------------------
 * Backend-private tracking for tuple sLog cleanup
 * ----------------------------------------------------------------
 */

/*
 * SLogTupleTrackKey
 *		Remember a tuple key for cleanup at commit/abort.
 *
 * Allocated in TopTransactionContext so it's automatically freed
 * when the transaction ends.
 */
void
SLogTupleTrackKey(SLogTupleKey key)
{
	MemoryContext oldcxt;
	SLogTrackedKey *tk;

	/* Check for duplicates */
	for (tk = slog_tracked_keys; tk != NULL; tk = tk->next)
	{
		if (memcmp(&tk->key, &key, sizeof(SLogTupleKey)) == 0)
			return;
	}

	oldcxt = MemoryContextSwitchTo(TopTransactionContext);

	tk = (SLogTrackedKey *) palloc(sizeof(SLogTrackedKey));
	memcpy(&tk->key, &key, sizeof(SLogTupleKey));
	tk->next = slog_tracked_keys;
	slog_tracked_keys = tk;

	MemoryContextSwitchTo(oldcxt);
}

/*
 * SLogTupleResetTracking
 *		Clear the backend-private tracking list.
 *
 * The memory is in TopTransactionContext and will be freed automatically,
 * but we reset the list head to avoid dangling pointers.
 */
void
SLogTupleResetTracking(void)
{
	slog_tracked_keys = NULL;
}
