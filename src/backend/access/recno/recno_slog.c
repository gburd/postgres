/*-------------------------------------------------------------------------
 *
 * recno_slog.c
 *	  RECNO secondary log (sLog) implementation
 *
 * The sLog is a shared-memory hash table that tracks non-versioned,
 * transient coordination state for in-progress transactions.  It
 * replaces t_xmin, t_xmax, and MultiXact in the tuple header.
 *
 * Based on the Antonopoulos CTR paper (Section 3.4.1).
 *
 * Implementation:
 *   - ShmemInitHash with RECNO_SLOG_PARTITIONS LWLock partitions
 *   - Entries keyed by (tid, relid) -- all ops for a TID in one entry
 *   - Each entry holds an inline array of ops (one per concurrent txn)
 *   - Every TID-based lookup is O(1): single hash probe, single partition
 *   - Cleanup via RegisterXactCallback / RegisterSubXactCallback
 *   - Per-backend local tracking list for efficient removeAll
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/recno/recno_slog.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/recno.h"
#include "access/recno_slog.h"
#include "access/transam.h"
#include "access/xact.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/shmem.h"
#include "common/hashfn.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/relcache.h"
#include "utils/rel.h"

/*
 * Maximum number of sLog entries (distinct TIDs with active operations).
 *
 * Every row inserted via recno_tuple_insert gets an sLog entry that
 * persists until the transaction commits or aborts.  A single
 * INSERT ... SELECT FROM generate_series(1, 10000) therefore needs
 * 10,000 entries simultaneously.  The per-backend multiplier must be
 * large enough to handle realistic bulk-insert workloads.
 *
 * During bootstrap MaxBackends is ~4, giving 4 * 2048 = 8192 entries.
 * Normal servers with max_connections = 100 get ~230,000 entries,
 * capped at 262,144.
 *
 * The minimum (128) is a safety floor for edge-case configurations.
 */
#define RECNO_SLOG_ENTRIES_PER_BACKEND	2048
#define RECNO_SLOG_MIN_ENTRIES			128
#define RECNO_SLOG_MAX_ENTRIES			262144

static int
RecnoSLogNumEntries(void)
{
	int		n = MaxBackends * RECNO_SLOG_ENTRIES_PER_BACKEND;

	if (n < RECNO_SLOG_MIN_ENTRIES)
		n = RECNO_SLOG_MIN_ENTRIES;
	if (n > RECNO_SLOG_MAX_ENTRIES)
		n = RECNO_SLOG_MAX_ENTRIES;
	return n;
}

/*
 * Per-operation payload stored inline in each hash entry.
 * Fields ordered for natural alignment (uint64 first).
 */
typedef struct RecnoSLogOp
{
	uint64			commit_ts;		/* Operation's commit_ts */
	TransactionId	xid;			/* Operating transaction's XID */
	uint32			spec_token;		/* Speculative insertion token (0 if none) */
	CommandId		cid;			/* Command ID within transaction */
	SubTransactionId subxid;		/* Subtransaction ID */
	uint8			op_type;		/* RecnoSLogOpType */
	bool			in_use;			/* Slot occupied? */
} RecnoSLogOp;

/*
 * The sLog hash table entry as stored in shared memory.
 * Keyed by (tid, relid).  Multiple concurrent operations on the same
 * TID are stored in the ops[] inline array.
 */
typedef struct RecnoSLogHashEntry
{
	RecnoSLogKey	key;			/* Hash key: tid + relid */
	uint16			nops;			/* Number of active ops */
	RecnoSLogOp		ops[RECNO_SLOG_MAX_OPS];
} RecnoSLogHashEntry;

/* The partitioned hash table */
static HTAB *RecnoSLogHash = NULL;

/* LWLocks for partition-level locking */
static LWLockPadded *RecnoSLogLocks = NULL;

/* Whether transaction callbacks have been registered for this backend */
static bool recno_slog_callbacks_registered = false;

/*
 * Per-backend list of (tid, relid, xid) tuples that have sLog entries,
 * for efficient cleanup.  This avoids scanning the entire hash table
 * on commit/abort.
 */
typedef struct RecnoSLogLocalEntry
{
	RecnoSLogKey	key;			/* (tid, relid) for hash lookup */
	TransactionId	xid;			/* Which xid's op within the entry */
	struct RecnoSLogLocalEntry *next;
} RecnoSLogLocalEntry;

/* Head of the per-backend local tracking list */
static RecnoSLogLocalEntry *slog_local_list = NULL;

/* ----------------------------------------------------------------
 *				Internal helpers
 * ----------------------------------------------------------------
 */

/*
 * Compute which partition a key hashes to.
 * Since key is (tid, relid), all ops for the same TID are in the same
 * partition -- no cross-partition scanning needed.
 */
static inline uint32
RecnoSLogPartition(const RecnoSLogKey *key)
{
	uint32		h;

	h = hash_bytes((const unsigned char *) key, sizeof(RecnoSLogKey));
	return h & RECNO_SLOG_PARTITION_MASK;
}

/*
 * Acquire the LWLock for the partition containing this key.
 */
static inline void
RecnoSLogLockPartition(const RecnoSLogKey *key, LWLockMode mode)
{
	uint32		part = RecnoSLogPartition(key);

	LWLockAcquire(&RecnoSLogLocks[part].lock, mode);
}

static inline void
RecnoSLogUnlockPartition(const RecnoSLogKey *key)
{
	uint32		part = RecnoSLogPartition(key);

	LWLockRelease(&RecnoSLogLocks[part].lock);
}

/*
 * Ensure xact/subxact callbacks are registered for this backend.
 */
static void
RecnoSLogEnsureCallbacks(void)
{
	if (!recno_slog_callbacks_registered)
	{
		RegisterXactCallback(RecnoSLogXactCallback, NULL);
		RegisterSubXactCallback(RecnoSLogSubXactCallback, NULL);
		recno_slog_callbacks_registered = true;
	}
}

/*
 * Add a (key, xid) pair to the backend-local tracking list.
 */
static void
RecnoSLogLocalTrack(const RecnoSLogKey *key, TransactionId xid)
{
	RecnoSLogLocalEntry *entry;
	MemoryContext oldcxt;

	oldcxt = MemoryContextSwitchTo(TopTransactionContext);
	entry = (RecnoSLogLocalEntry *) palloc(sizeof(RecnoSLogLocalEntry));
	entry->key = *key;
	entry->xid = xid;
	entry->next = slog_local_list;
	slog_local_list = entry;
	MemoryContextSwitchTo(oldcxt);
}

/*
 * Build an sLog hash key from (tid, relid).
 */
static inline void
RecnoSLogBuildKey(RecnoSLogKey *key, Oid relid, ItemPointer tid)
{
	memset(key, 0, sizeof(RecnoSLogKey));
	key->tid = *tid;
	key->relid = relid;
}

/* ----------------------------------------------------------------
 *				Shared memory setup
 * ----------------------------------------------------------------
 */

Size
RecnoSLogShmemSize(void)
{
	Size		size;

	/* Hash table */
	size = hash_estimate_size(RecnoSLogNumEntries(),
							  sizeof(RecnoSLogHashEntry));

	/* Partition LWLocks */
	size = add_size(size, mul_size(RECNO_SLOG_PARTITIONS,
								   sizeof(LWLockPadded)));

	return size;
}

void
RecnoSLogShmemInit(void)
{
	int			i;

	/* Initialize partition LWLocks */
	for (i = 0; i < RECNO_SLOG_PARTITIONS; i++)
		LWLockInitialize(&RecnoSLogLocks[i].lock, LWTRANCHE_BUFFER_MAPPING);
}

/*
 * Subsystem callback wrappers for PG_SHMEM_SUBSYSTEM infrastructure
 */
static void
RecnoSLogShmemRequest(void *arg)
{
	/* Register partitioned hash table via ShmemRequestHash */
	ShmemRequestHash(.name = "RECNO sLog Hash",
					 .nelems = RecnoSLogNumEntries(),
					 .ptr = &RecnoSLogHash,
					 .hash_info.keysize = sizeof(RecnoSLogKey),
					 .hash_info.entrysize = sizeof(RecnoSLogHashEntry),
					 .hash_info.num_partitions = RECNO_SLOG_PARTITIONS,
					 .hash_flags = HASH_ELEM | HASH_BLOBS | HASH_PARTITION,
		);

	/* Register space for partition LWLocks */
	ShmemRequestStruct(.name = "RECNO sLog Locks",
					   .size = mul_size(RECNO_SLOG_PARTITIONS,
										sizeof(LWLockPadded)),
					   .ptr = (void **) &RecnoSLogLocks);
}

static void
RecnoSLogShmemInit_cb(void *arg)
{
	RecnoSLogShmemInit();
}

const ShmemCallbacks RecnoSLogShmemCallbacks = {
	.request_fn = RecnoSLogShmemRequest,
	.init_fn = RecnoSLogShmemInit_cb,
};

/* ----------------------------------------------------------------
 *				Core sLog API
 * ----------------------------------------------------------------
 */

/*
 * RecnoSLogInsert -- register an in-progress operation.
 *
 * Looks up the hash entry for (tid, relid), creates it if absent,
 * and adds an op slot for this transaction.
 */
void
RecnoSLogInsert(Oid relid, ItemPointer tid,
				TransactionId xid, uint64 commit_ts,
				CommandId cid, RecnoSLogOpType op_type,
				SubTransactionId subxid,
				uint32 spec_token)
{
	RecnoSLogKey key;
	RecnoSLogHashEntry *entry;
	bool		found;
	int			i;

	Assert(RecnoSLogHash != NULL);
	Assert(TransactionIdIsValid(xid));
	Assert(ItemPointerIsValid(tid));

	RecnoSLogEnsureCallbacks();

	RecnoSLogBuildKey(&key, relid, tid);

	RecnoSLogLockPartition(&key, LW_EXCLUSIVE);

	entry = (RecnoSLogHashEntry *)
		hash_search(RecnoSLogHash, &key, HASH_ENTER, &found);

	if (entry == NULL)
		elog(ERROR, "sLog hash table is full (%d entries)",
			 RecnoSLogNumEntries());

	if (!found)
	{
		/* New entry: initialize */
		entry->nops = 0;
		memset(entry->ops, 0, sizeof(entry->ops));
	}

	/*
	 * Check if this xid already has an op in this entry (e.g., a
	 * transaction that does INSERT then UPDATE on same TID).  If so,
	 * overwrite the existing op.
	 */
	for (i = 0; i < entry->nops; i++)
	{
		if (entry->ops[i].in_use &&
			TransactionIdEquals(entry->ops[i].xid, xid))
		{
			/* Overwrite existing op for this xid */
			entry->ops[i].commit_ts = commit_ts;
			entry->ops[i].spec_token = spec_token;
			entry->ops[i].cid = cid;
			entry->ops[i].op_type = (uint8) op_type;
			entry->ops[i].subxid = subxid;

			RecnoSLogUnlockPartition(&key);

			/* Still track locally (duplicate is fine; cleanup handles it) */
			RecnoSLogLocalTrack(&key, xid);
			return;
		}
	}

	/* Find a free slot */
	for (i = 0; i < RECNO_SLOG_MAX_OPS; i++)
	{
		if (!entry->ops[i].in_use)
		{
			entry->ops[i].commit_ts = commit_ts;
			entry->ops[i].xid = xid;
			entry->ops[i].spec_token = spec_token;
			entry->ops[i].cid = cid;
			entry->ops[i].op_type = (uint8) op_type;
			entry->ops[i].subxid = subxid;
			entry->ops[i].in_use = true;
			entry->nops++;

			RecnoSLogUnlockPartition(&key);

			RecnoSLogLocalTrack(&key, xid);
			return;
		}
	}

	/*
	 * No free slot.  Attempt to reclaim stale slots: entries whose
	 * transaction has already committed (cleanup callback hasn't fired yet)
	 * or aborted and been marked ABORTED.  This handles the race between
	 * EPQ-cycling backends and asynchronous sLog cleanup.
	 */
	{
		int		reclaimed = 0;

		for (i = 0; i < RECNO_SLOG_MAX_OPS; i++)
		{
			if (!entry->ops[i].in_use)
				continue;

			/* Skip our own transaction's entries */
			if (TransactionIdEquals(entry->ops[i].xid, xid))
				continue;

			/*
			 * Check if the transaction has finished.  Committed entries are
			 * safe to reclaim -- their sLog cleanup just hasn't run yet.
			 */
			if (!TransactionIdIsInProgress(entry->ops[i].xid) &&
				TransactionIdDidCommit(entry->ops[i].xid))
			{
				entry->ops[i].in_use = false;
				entry->nops--;
				reclaimed++;
			}
		}

		if (reclaimed > 0)
		{
			elog(DEBUG2, "sLog: reclaimed %d stale slot(s) on TID (%u,%u) in rel %u",
				 reclaimed, ItemPointerGetBlockNumber(tid),
				 ItemPointerGetOffsetNumber(tid), relid);

			/* Retry finding a free slot after reclamation */
			for (i = 0; i < RECNO_SLOG_MAX_OPS; i++)
			{
				if (!entry->ops[i].in_use)
				{
					entry->ops[i].commit_ts = commit_ts;
					entry->ops[i].xid = xid;
					entry->ops[i].spec_token = spec_token;
					entry->ops[i].cid = cid;
					entry->ops[i].op_type = (uint8) op_type;
					entry->ops[i].subxid = subxid;
					entry->ops[i].in_use = true;
					entry->nops++;

					RecnoSLogUnlockPartition(&key);

					RecnoSLogLocalTrack(&key, xid);
					return;
				}
			}
		}
	}

	/*
	 * Still no room.  Release the partition lock and retry up to 3 times
	 * with brief waits, giving concurrent transactions time to commit and
	 * have their cleanup callbacks fire.
	 */
	RecnoSLogUnlockPartition(&key);

	{
		int		retry;

		for (retry = 0; retry < 3; retry++)
		{
			pg_usleep(1000);	/* 1ms */

			RecnoSLogLockPartition(&key, LW_EXCLUSIVE);

			entry = (RecnoSLogHashEntry *)
				hash_search(RecnoSLogHash, &key, HASH_FIND, NULL);

			if (entry == NULL)
			{
				/*
				 * Entry was removed entirely by another backend's cleanup.
				 * Re-create it.
				 */
				entry = (RecnoSLogHashEntry *)
					hash_search(RecnoSLogHash, &key, HASH_ENTER, &found);
				if (entry == NULL)
					elog(ERROR, "sLog hash table is full (%d entries)",
						 RecnoSLogNumEntries());
				entry->nops = 0;
				memset(entry->ops, 0, sizeof(entry->ops));
			}

			for (i = 0; i < RECNO_SLOG_MAX_OPS; i++)
			{
				if (!entry->ops[i].in_use)
				{
					entry->ops[i].commit_ts = commit_ts;
					entry->ops[i].xid = xid;
					entry->ops[i].spec_token = spec_token;
					entry->ops[i].cid = cid;
					entry->ops[i].op_type = (uint8) op_type;
					entry->ops[i].subxid = subxid;
					entry->ops[i].in_use = true;
					entry->nops++;

					RecnoSLogUnlockPartition(&key);

					RecnoSLogLocalTrack(&key, xid);
					return;
				}
			}

			RecnoSLogUnlockPartition(&key);
		}
	}

	/*
	 * Overflow: log diagnostic information about all current ops in the
	 * entry before raising the error.
	 */
	RecnoSLogLockPartition(&key, LW_SHARED);
	entry = (RecnoSLogHashEntry *)
		hash_search(RecnoSLogHash, &key, HASH_FIND, NULL);
	if (entry != NULL)
	{
		for (i = 0; i < RECNO_SLOG_MAX_OPS; i++)
		{
			if (entry->ops[i].in_use)
			{
				bool	in_progress = TransactionIdIsInProgress(entry->ops[i].xid);

				elog(DEBUG1, "sLog overflow slot %d: xid=%u op_type=%u "
					 "in_progress=%s",
					 i, entry->ops[i].xid, entry->ops[i].op_type,
					 in_progress ? "true" : "false");
			}
		}
	}
	RecnoSLogUnlockPartition(&key);

	elog(ERROR, "sLog: too many concurrent operations on TID (%u,%u) in rel %u "
		 "(max %d, all slots occupied after reclamation and retries)",
		 ItemPointerGetBlockNumber(tid),
		 ItemPointerGetOffsetNumber(tid), relid, RECNO_SLOG_MAX_OPS);
}

/*
 * RecnoSLogLookup -- find entries for a TID.
 *
 * Single O(1) hash probe in a single partition.  If xid_filter is valid,
 * returns only the op for that xid.  If InvalidTransactionId, returns
 * all active ops for this TID.
 */
int
RecnoSLogLookup(Oid relid, ItemPointer tid,
				TransactionId xid_filter,
				RecnoSLogEntry *entries, int max_entries)
{
	RecnoSLogKey key;
	RecnoSLogHashEntry *hentry;
	int			nfound = 0;
	int			i;

	Assert(RecnoSLogHash != NULL);

	RecnoSLogBuildKey(&key, relid, tid);

	RecnoSLogLockPartition(&key, LW_SHARED);

	hentry = (RecnoSLogHashEntry *)
		hash_search(RecnoSLogHash, &key, HASH_FIND, NULL);

	if (hentry != NULL)
	{
		for (i = 0; i < RECNO_SLOG_MAX_OPS && nfound < max_entries; i++)
		{
			if (!hentry->ops[i].in_use)
				continue;

			/* If filtering by xid, skip non-matching ops */
			if (TransactionIdIsValid(xid_filter) &&
				!TransactionIdEquals(hentry->ops[i].xid, xid_filter))
				continue;

			entries[nfound].tid = hentry->key.tid;
			entries[nfound].relid = hentry->key.relid;
			entries[nfound].xid = hentry->ops[i].xid;
			entries[nfound].commit_ts = hentry->ops[i].commit_ts;
			entries[nfound].spec_token = hentry->ops[i].spec_token;
			entries[nfound].cid = hentry->ops[i].cid;
			entries[nfound].op_type = hentry->ops[i].op_type;
			entries[nfound].subxid = hentry->ops[i].subxid;
			nfound++;
		}
	}

	RecnoSLogUnlockPartition(&key);

	return nfound;
}

/*
 * RecnoSLogLookupAll -- find ALL entries for a TID in a single probe.
 *
 * This is the preferred function for callers that need to inspect all ops
 * for a TID (e.g., visibility checks).  By returning all entries in one
 * partition lock acquisition, it eliminates the repeated lock/unlock cycles
 * that occur when callers make separate RecnoSLogLookup calls for different
 * xid_filter values.
 */
int
RecnoSLogLookupAll(Oid relid, ItemPointer tid,
				   RecnoSLogEntry *entries, int max_entries)
{
	return RecnoSLogLookup(relid, tid, InvalidTransactionId,
						   entries, max_entries);
}

/*
 * RecnoSLogRemove -- remove a specific op by (tid, relid, xid).
 *
 * Removes the op for the given xid from the entry.  If the entry
 * becomes empty (nops == 0), removes the entire hash entry.
 */
bool
RecnoSLogRemove(Oid relid, ItemPointer tid, TransactionId xid)
{
	RecnoSLogKey key;
	RecnoSLogHashEntry *hentry;
	bool		removed = false;
	int			i;

	Assert(RecnoSLogHash != NULL);

	RecnoSLogBuildKey(&key, relid, tid);

	RecnoSLogLockPartition(&key, LW_EXCLUSIVE);

	hentry = (RecnoSLogHashEntry *)
		hash_search(RecnoSLogHash, &key, HASH_FIND, NULL);

	if (hentry != NULL)
	{
		for (i = 0; i < RECNO_SLOG_MAX_OPS; i++)
		{
			if (hentry->ops[i].in_use &&
				TransactionIdEquals(hentry->ops[i].xid, xid))
			{
				hentry->ops[i].in_use = false;
				hentry->nops--;
				removed = true;
				break;
			}
		}

		/* If no ops remain, remove the entry entirely */
		if (hentry->nops == 0)
			hash_search(RecnoSLogHash, &key, HASH_REMOVE, NULL);
	}

	RecnoSLogUnlockPartition(&key);

	return removed;
}

/*
 * RecnoSLogRemoveByXid -- remove ALL ops for a transaction.
 *
 * Uses the backend-local tracking list: for each tracked (tid, relid),
 * does a single O(1) hash lookup and removes the matching op.
 *
 * Optimization: entries that hash to the same partition are processed
 * together under a single lock acquisition, reducing the number of
 * LWLock round-trips at commit time.
 */
/*
 * Comparison function for sorting local entries by sLog partition number.
 * Sorting ensures each partition is locked at most once during cleanup,
 * reducing the number of LWLock round-trips at commit time.
 */
typedef struct RecnoSLogSortEntry
{
	RecnoSLogKey key;
	uint32		partition;
} RecnoSLogSortEntry;

static int
RecnoSLogSortCmp(const void *a, const void *b)
{
	const RecnoSLogSortEntry *ea = (const RecnoSLogSortEntry *) a;
	const RecnoSLogSortEntry *eb = (const RecnoSLogSortEntry *) b;

	if (ea->partition < eb->partition)
		return -1;
	if (ea->partition > eb->partition)
		return 1;
	return 0;
}

void
RecnoSLogRemoveByXid(TransactionId xid)
{
	RecnoSLogLocalEntry *lentry;
	int			current_partition = -1;
	int			nentries = 0;
	int			idx;
	RecnoSLogSortEntry *sorted;

	if (RecnoSLogHash == NULL)
		return;

	/*
	 * Count matching entries, then collect and sort them by partition.
	 * This guarantees each partition lock is acquired at most once during
	 * cleanup, which is a significant win when DML operations hash to
	 * different partitions (the common case with random TIDs).
	 *
	 * For pgbench TPC-B (4 DML ops per tx), this typically reduces lock
	 * acquisitions from 3-4 to 2-3 (or even 1 if entries happen to share
	 * a partition).
	 */
	for (lentry = slog_local_list; lentry != NULL; lentry = lentry->next)
	{
		if (TransactionIdEquals(lentry->xid, xid))
			nentries++;
	}

	if (nentries == 0)
		return;

	/* Allocate in TopTransactionContext -- freed at xact end */
	sorted = (RecnoSLogSortEntry *)
		palloc(nentries * sizeof(RecnoSLogSortEntry));
	idx = 0;
	for (lentry = slog_local_list; lentry != NULL; lentry = lentry->next)
	{
		if (TransactionIdEquals(lentry->xid, xid))
		{
			sorted[idx].key = lentry->key;
			sorted[idx].partition = RecnoSLogPartition(&lentry->key);
			idx++;
		}
	}

	qsort(sorted, nentries, sizeof(RecnoSLogSortEntry), RecnoSLogSortCmp);

	/* Process sorted entries, locking each partition at most once */
	for (idx = 0; idx < nentries; idx++)
	{
		RecnoSLogHashEntry *hentry;
		int			part = (int) sorted[idx].partition;
		int			i;

		/* Lock the partition if not already holding it */
		if (part != current_partition)
		{
			if (current_partition >= 0)
				LWLockRelease(&RecnoSLogLocks[current_partition].lock);
			LWLockAcquire(&RecnoSLogLocks[part].lock, LW_EXCLUSIVE);
			current_partition = part;
		}

		hentry = (RecnoSLogHashEntry *)
			hash_search(RecnoSLogHash, &sorted[idx].key, HASH_FIND, NULL);

		if (hentry != NULL)
		{
			for (i = 0; i < RECNO_SLOG_MAX_OPS; i++)
			{
				if (hentry->ops[i].in_use &&
					TransactionIdEquals(hentry->ops[i].xid, xid))
				{
					hentry->ops[i].in_use = false;
					hentry->nops--;
					break;
				}
			}

			/* Remove entry if empty */
			if (hentry->nops == 0)
				hash_search(RecnoSLogHash, &sorted[idx].key,
							HASH_REMOVE, NULL);
		}
	}

	/* Release the last held partition lock */
	if (current_partition >= 0)
		LWLockRelease(&RecnoSLogLocks[current_partition].lock);

	pfree(sorted);

	/* Local list is in TopTransactionContext, freed at xact end */
}

/*
 * RecnoSLogMarkAborted -- mark all ops for a transaction as ABORTED.
 *
 * Called at transaction abort.  Entries remain so visibility checks can
 * distinguish "committed (no entry)" from "aborted (UNDO pending)".
 */
void
RecnoSLogMarkAborted(TransactionId xid)
{
	RecnoSLogLocalEntry *lentry;

	if (RecnoSLogHash == NULL)
		return;

	for (lentry = slog_local_list; lentry != NULL; lentry = lentry->next)
	{
		if (TransactionIdEquals(lentry->xid, xid))
		{
			RecnoSLogHashEntry *hentry;
			int			i;

			RecnoSLogLockPartition(&lentry->key, LW_EXCLUSIVE);

			hentry = (RecnoSLogHashEntry *)
				hash_search(RecnoSLogHash, &lentry->key, HASH_FIND, NULL);

			if (hentry != NULL)
			{
				for (i = 0; i < RECNO_SLOG_MAX_OPS; i++)
				{
					if (hentry->ops[i].in_use &&
						TransactionIdEquals(hentry->ops[i].xid, xid))
					{
						hentry->ops[i].op_type = (uint8) RECNO_SLOG_ABORTED;
						break;
					}
				}
			}

			RecnoSLogUnlockPartition(&lentry->key);
		}
	}

	/* Local list is in TopTransactionContext, freed at xact end */
}

/*
 * RecnoSLogHasAbortedEntry -- check if any aborted sLog op exists for a TID.
 *
 * Now a single O(1) hash probe instead of the former O(N) full scan.
 */
bool
RecnoSLogHasAbortedEntry(Oid relid, ItemPointer tid)
{
	RecnoSLogEntry entries[RECNO_SLOG_MAX_OPS];
	int			nfound;
	int			i;

	nfound = RecnoSLogLookup(relid, tid, InvalidTransactionId,
							 entries, RECNO_SLOG_MAX_OPS);

	for (i = 0; i < nfound; i++)
	{
		/*
		 * Explicitly marked as ABORTED by the subtransaction abort callback.
		 * Check this BEFORE skipping our own transaction's entries, because
		 * subtransaction rollback marks our OWN entries as ABORTED.
		 */
		if (entries[i].op_type == RECNO_SLOG_ABORTED)
			return true;

		/* Skip our own transaction's entries for CLOG fallback check */
		if (TransactionIdIsCurrentTransactionId(entries[i].xid))
			continue;

		/*
		 * CLOG fallback: if the entry's XID completed (not in-progress) and
		 * did not commit, it must have aborted.  The sLog entry persists
		 * because the UNDO worker hasn't cleaned it up yet.
		 */
		if (!TransactionIdIsInProgress(entries[i].xid) &&
			TransactionIdDidAbort(entries[i].xid))
			return true;
	}

	return false;
}

/*
 * RecnoSLogRemoveByXidGlobal -- remove ALL ops for a transaction by
 * scanning the shared hash table directly.
 *
 * Unlike RecnoSLogRemoveByXid, this does not use the backend-local tracking
 * list (which doesn't exist in the UNDO worker process).  It scans all
 * partitions of the hash table one at a time.
 *
 * This is a cold path only used by UNDO/revert workers.
 */
void
RecnoSLogRemoveByXidGlobal(TransactionId xid)
{
	HASH_SEQ_STATUS status;
	RecnoSLogHashEntry *hentry;
	int			part;
	RecnoSLogKey *collected_keys;
	int			nkeys = 0;
	int			max_keys;

	if (RecnoSLogHash == NULL)
		return;

	/*
	 * Phase 1: Scan under SHARED locks.  Normal DML backends doing sLog
	 * lookups are not blocked (they also take shared locks for reads).
	 * Collect keys that contain ops for the target xid.
	 */
	for (part = 0; part < RECNO_SLOG_PARTITIONS; part++)
		LWLockAcquire(&RecnoSLogLocks[part].lock, LW_SHARED);

	/* Pre-allocate key array.  Overestimate is fine; this is a cold path. */
	max_keys = RecnoSLogNumEntries();
	collected_keys = (RecnoSLogKey *)
		palloc(sizeof(RecnoSLogKey) * max_keys);

	hash_seq_init(&status, RecnoSLogHash);
	while ((hentry = (RecnoSLogHashEntry *) hash_seq_search(&status)) != NULL)
	{
		int		i;

		for (i = 0; i < RECNO_SLOG_MAX_OPS; i++)
		{
			if (hentry->ops[i].in_use &&
				TransactionIdEquals(hentry->ops[i].xid, xid))
			{
				/* This entry has ops for our xid; collect its key */
				if (nkeys < max_keys)
					collected_keys[nkeys++] = hentry->key;
				break;
			}
		}
	}

	/* Release all shared locks */
	for (part = RECNO_SLOG_PARTITIONS - 1; part >= 0; part--)
		LWLockRelease(&RecnoSLogLocks[part].lock);

	/*
	 * Phase 2: Remove under per-partition EXCLUSIVE locks.  Only the
	 * partition containing each collected key is locked at a time, so
	 * other partitions remain fully accessible to normal DML backends.
	 */
	for (part = 0; part < nkeys; part++)
	{
		RecnoSLogHashEntry *entry;
		bool		modified = false;
		int			i;

		RecnoSLogLockPartition(&collected_keys[part], LW_EXCLUSIVE);

		entry = (RecnoSLogHashEntry *)
			hash_search(RecnoSLogHash, &collected_keys[part], HASH_FIND, NULL);

		if (entry != NULL)
		{
			for (i = 0; i < RECNO_SLOG_MAX_OPS; i++)
			{
				if (entry->ops[i].in_use &&
					TransactionIdEquals(entry->ops[i].xid, xid))
				{
					entry->ops[i].in_use = false;
					entry->nops--;
					modified = true;
				}
			}

			if (modified && entry->nops == 0)
				hash_search(RecnoSLogHash, &collected_keys[part],
							HASH_REMOVE, NULL);
		}

		RecnoSLogUnlockPartition(&collected_keys[part]);
	}

	pfree(collected_keys);
}

/*
 * RecnoSLogRemoveBySubXid -- mark ops ABORTED for a specific subtransaction.
 */
void
RecnoSLogRemoveBySubXid(TransactionId xid, SubTransactionId subxid)
{
	RecnoSLogLocalEntry *lentry;

	if (RecnoSLogHash == NULL)
		return;

	for (lentry = slog_local_list; lentry != NULL; lentry = lentry->next)
	{
		if (TransactionIdEquals(lentry->xid, xid))
		{
			RecnoSLogHashEntry *hentry;
			int			i;

			RecnoSLogLockPartition(&lentry->key, LW_EXCLUSIVE);

			hentry = (RecnoSLogHashEntry *)
				hash_search(RecnoSLogHash, &lentry->key, HASH_FIND, NULL);

			if (hentry != NULL)
			{
				for (i = 0; i < RECNO_SLOG_MAX_OPS; i++)
				{
					if (hentry->ops[i].in_use &&
						TransactionIdEquals(hentry->ops[i].xid, xid) &&
						hentry->ops[i].subxid == subxid)
					{
						hentry->ops[i].op_type = (uint8) RECNO_SLOG_ABORTED;
					}
				}
			}

			RecnoSLogUnlockPartition(&lentry->key);
		}
	}
}

/*
 * RecnoSLogUpdateSubXid -- re-parent ops on subtransaction commit.
 */
void
RecnoSLogUpdateSubXid(TransactionId xid,
					  SubTransactionId old_subxid,
					  SubTransactionId new_subxid)
{
	RecnoSLogLocalEntry *lentry;

	if (RecnoSLogHash == NULL)
		return;

	for (lentry = slog_local_list; lentry != NULL; lentry = lentry->next)
	{
		if (TransactionIdEquals(lentry->xid, xid))
		{
			RecnoSLogHashEntry *hentry;
			int			i;

			RecnoSLogLockPartition(&lentry->key, LW_EXCLUSIVE);

			hentry = (RecnoSLogHashEntry *)
				hash_search(RecnoSLogHash, &lentry->key, HASH_FIND, NULL);

			if (hentry != NULL)
			{
				for (i = 0; i < RECNO_SLOG_MAX_OPS; i++)
				{
					if (hentry->ops[i].in_use &&
						TransactionIdEquals(hentry->ops[i].xid, xid) &&
						hentry->ops[i].subxid == old_subxid)
					{
						hentry->ops[i].subxid = new_subxid;
					}
				}
			}

			RecnoSLogUnlockPartition(&lentry->key);
		}
	}
}

/* ----------------------------------------------------------------
 *				Convenience wrappers
 * ----------------------------------------------------------------
 */

/*
 * RecnoSLogHasEntry -- quick probe: does ANY active sLog entry exist for this TID?
 *
 * This is a fast-path for the common case in UPDATE where a tuple still has
 * RECNO_TUPLE_UNCOMMITTED set from a previous (now committed) transaction.
 * By checking whether the sLog has any entry at all, we can avoid the more
 * expensive RecnoSLogLookupAll + full visibility analysis.
 *
 * Returns true if at least one active (in_use) entry exists for the TID.
 * Uses a single SHARED partition lock + HASH_FIND (no iteration over entries
 * needed if the hash entry doesn't exist at all).
 */
bool
RecnoSLogHasEntry(Oid relid, ItemPointer tid)
{
	RecnoSLogKey key;
	RecnoSLogHashEntry *hentry;
	bool		has_entry = false;

	Assert(RecnoSLogHash != NULL);

	RecnoSLogBuildKey(&key, relid, tid);

	RecnoSLogLockPartition(&key, LW_SHARED);

	hentry = (RecnoSLogHashEntry *)
		hash_search(RecnoSLogHash, &key, HASH_FIND, NULL);

	if (hentry != NULL && hentry->nops > 0)
		has_entry = true;

	RecnoSLogUnlockPartition(&key);

	return has_entry;
}

bool
RecnoSLogIsInsertedByMe(Oid relid, ItemPointer tid)
{
	RecnoSLogEntry entry;
	int			nfound;
	TransactionId myxid = GetCurrentTransactionIdIfAny();

	if (!TransactionIdIsValid(myxid))
		return false;

	nfound = RecnoSLogLookup(relid, tid, myxid, &entry, 1);
	return (nfound > 0 && entry.op_type == RECNO_SLOG_INSERT);
}

bool
RecnoSLogIsDeletedByMe(Oid relid, ItemPointer tid)
{
	RecnoSLogEntry entry;
	int			nfound;
	TransactionId myxid = GetCurrentTransactionIdIfAny();

	if (!TransactionIdIsValid(myxid))
		return false;

	nfound = RecnoSLogLookup(relid, tid, myxid, &entry, 1);
	return (nfound > 0 &&
			(entry.op_type == RECNO_SLOG_DELETE ||
			 entry.op_type == RECNO_SLOG_UPDATE));
}

TransactionId
RecnoSLogGetDirtyXid(Oid relid, ItemPointer tid, bool *is_insert)
{
	RecnoSLogEntry entries[RECNO_SLOG_MAX_OPS];
	int			nfound;
	int			i;

	nfound = RecnoSLogLookup(relid, tid, InvalidTransactionId,
							 entries, RECNO_SLOG_MAX_OPS);

	for (i = 0; i < nfound; i++)
	{
		/*
		 * Skip entries for our own transaction -- SNAPSHOT_DIRTY wants
		 * the xid of OTHER in-progress transactions.
		 */
		if (TransactionIdIsCurrentTransactionId(entries[i].xid))
			continue;

		/* Skip completed transactions */
		if (!TransactionIdIsInProgress(entries[i].xid))
			continue;

		if (is_insert)
			*is_insert = (entries[i].op_type == RECNO_SLOG_INSERT);

		return entries[i].xid;
	}

	return InvalidTransactionId;
}

bool
RecnoSLogHasLockConflict(Oid relid, ItemPointer tid,
						 TransactionId my_xid,
						 RecnoSLogOpType requested_lock)
{
	RecnoSLogEntry entries[RECNO_SLOG_MAX_OPS];
	int			nfound;
	int			i;

	nfound = RecnoSLogLookup(relid, tid, InvalidTransactionId,
							 entries, RECNO_SLOG_MAX_OPS);

	for (i = 0; i < nfound; i++)
	{
		/* Skip our own entries */
		if (TransactionIdEquals(entries[i].xid, my_xid))
			continue;

		/* Skip entries from completed transactions */
		if (!TransactionIdIsInProgress(entries[i].xid))
			continue;

		/* Only lock entries can conflict */
		if (entries[i].op_type != RECNO_SLOG_LOCK_SHARE &&
			entries[i].op_type != RECNO_SLOG_LOCK_EXCL &&
			entries[i].op_type != RECNO_SLOG_DELETE &&
			entries[i].op_type != RECNO_SLOG_UPDATE)
			continue;

		/*
		 * Check lock compatibility matrix:
		 *   SHARE vs SHARE: compatible
		 *   SHARE vs EXCL: conflict
		 *   EXCL vs anything: conflict
		 */
		if (requested_lock == RECNO_SLOG_LOCK_SHARE)
		{
			if (entries[i].op_type == RECNO_SLOG_LOCK_EXCL ||
				entries[i].op_type == RECNO_SLOG_DELETE ||
				entries[i].op_type == RECNO_SLOG_UPDATE)
				return true;
		}
		else if (requested_lock == RECNO_SLOG_LOCK_EXCL)
		{
			/* Exclusive conflicts with everything */
			return true;
		}
	}

	return false;
}

/* ----------------------------------------------------------------
 *				Transaction callbacks
 * ----------------------------------------------------------------
 */

/*
 * RecnoSLogProcessAbortedEntries -- at COMMIT, mark tuples from rolled-back
 * subtransactions as DELETED on their pages.
 *
 * Entries marked ABORTED (from subtransaction rollback) represent INSERT
 * operations whose tuples still exist on disk with RECNO_TUPLE_UNCOMMITTED.
 * Without marking them DELETED, after COMMIT the UNCOMMITTED flag would be
 * lazily cleared and the tuples would incorrectly become visible.
 */
static void
RecnoSLogProcessAbortedEntries(TransactionId xid)
{
	RecnoSLogLocalEntry *lentry;

	if (RecnoSLogHash == NULL)
		return;

	for (lentry = slog_local_list; lentry != NULL; lentry = lentry->next)
	{
		RecnoSLogHashEntry *hentry;
		uint8		found_op_type = 0;
		int			i;

		if (!TransactionIdEquals(lentry->xid, xid))
			continue;

		RecnoSLogLockPartition(&lentry->key, LW_SHARED);

		hentry = (RecnoSLogHashEntry *)
			hash_search(RecnoSLogHash, &lentry->key, HASH_FIND, NULL);

		if (hentry != NULL)
		{
			for (i = 0; i < RECNO_SLOG_MAX_OPS; i++)
			{
				if (hentry->ops[i].in_use &&
					TransactionIdEquals(hentry->ops[i].xid, xid) &&
					hentry->ops[i].op_type == (uint8) RECNO_SLOG_ABORTED)
				{
					found_op_type = (uint8) RECNO_SLOG_ABORTED;
					break;
				}
			}
		}

		RecnoSLogUnlockPartition(&lentry->key);

		if (found_op_type != (uint8) RECNO_SLOG_ABORTED)
			continue;

		/*
		 * This is an ABORTED entry from a rolled-back subtransaction.
		 * Mark the tuple as DELETED on its page so it won't become
		 * visible after the top-level commit clears sLog entries.
		 */
		{
			Buffer		buf;
			Page		page;
			ItemId		itemid;
			RecnoTupleHeader *tuple_hdr;
			OffsetNumber offnum;
			Relation	rel;

			rel = relation_open(lentry->key.relid, AccessShareLock);
			buf = ReadBuffer(rel, ItemPointerGetBlockNumber(&lentry->key.tid));
			LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
			page = BufferGetPage(buf);
			offnum = ItemPointerGetOffsetNumber(&lentry->key.tid);

			if (offnum <= PageGetMaxOffsetNumber(page))
			{
				itemid = PageGetItemId(page, offnum);
				if (ItemIdIsNormal(itemid))
				{
					tuple_hdr = (RecnoTupleHeader *) PageGetItem(page, itemid);

					if (tuple_hdr->t_flags & RECNO_TUPLE_UNCOMMITTED)
					{
						tuple_hdr->t_flags |= RECNO_TUPLE_DELETED;
						tuple_hdr->t_flags &= ~RECNO_TUPLE_UNCOMMITTED;
						MarkBufferDirty(buf);
					}
				}
			}

			UnlockReleaseBuffer(buf);
			relation_close(rel, AccessShareLock);
		}
	}
}

/*
 * RecnoSLogXactCallback -- clean up sLog entries at transaction end.
 *
 * On commit: process ABORTED entries (mark tuples DELETED on pages),
 * then remove all entries.
 *
 * On abort: mark entries as ABORTED rather than removing them.  This
 * allows visibility checks to distinguish "committed (no sLog entry)"
 * from "aborted but UNDO not yet applied (ABORTED entry exists)".
 * The UNDO worker removes these entries after physically restoring tuples.
 */
void
RecnoSLogXactCallback(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_PRE_COMMIT:
		case XACT_EVENT_PRE_PREPARE:
			{
				/*
				 * Process aborted subtransaction entries while still in
				 * transaction state.  This must happen before COMMIT
				 * because it calls relation_open() / ReadBuffer(), which
				 * require IsTransactionState() == true.
				 */
				TransactionId xid = GetCurrentTransactionIdIfAny();

				if (TransactionIdIsValid(xid))
					RecnoSLogProcessAbortedEntries(xid);
			}
			break;

		case XACT_EVENT_COMMIT:
		case XACT_EVENT_PARALLEL_COMMIT:
		case XACT_EVENT_PREPARE:
			{
				TransactionId xid = GetCurrentTransactionIdIfAny();

				if (TransactionIdIsValid(xid))
					RecnoSLogRemoveByXid(xid);

				/* Local list is in TopTransactionContext, auto-freed */
				slog_local_list = NULL;
			}
			break;

		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:
			{
				TransactionId xid = GetCurrentTransactionIdIfAny();

				if (TransactionIdIsValid(xid))
					RecnoSLogMarkAborted(xid);

				/* Local list is in TopTransactionContext, auto-freed */
				slog_local_list = NULL;
			}
			break;

		default:
			break;
	}
}

/*
 * RecnoSLogSubXactCallback -- handle subtransaction events.
 */
void
RecnoSLogSubXactCallback(SubXactEvent event,
						 SubTransactionId mySubid,
						 SubTransactionId parentSubid,
						 void *arg)
{
	TransactionId xid;

	switch (event)
	{
		case SUBXACT_EVENT_ABORT_SUB:
			/*
			 * Subtransaction abort: mark entries ABORTED for this sub-level.
			 * Use top-level XID because all sLog entries are keyed by it.
			 * ABORTED entries prevent rolled-back tuples from being visible.
			 * At top-level COMMIT, RecnoSLogProcessAbortedEntries marks
			 * the corresponding tuples as DELETED on their pages.
			 */
			xid = GetTopTransactionIdIfAny();
			if (TransactionIdIsValid(xid))
				RecnoSLogRemoveBySubXid(xid, mySubid);
			break;

		case SUBXACT_EVENT_COMMIT_SUB:
			/*
			 * Subtransaction commit: re-parent entries to parent subxid
			 * so they survive this subtransaction but are cleaned up at
			 * top-level commit.
			 */
			xid = GetTopTransactionIdIfAny();
			if (TransactionIdIsValid(xid))
				RecnoSLogUpdateSubXid(xid, mySubid, parentSubid);
			break;

		default:
			/* Pre-commit -- nothing to do */
			break;
	}
}
