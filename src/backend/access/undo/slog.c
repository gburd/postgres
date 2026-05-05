/*-------------------------------------------------------------------------
 *
 * slog.c
 *	  Secondary Log (sLog) -- Skip-list + sparsemap shared-memory tracking
 *
 * The sLog tracks aborted transactions and per-tuple operations in shared
 * memory for the UNDO subsystem.
 *
 * Transaction sLog (skip-list + sparsemap):
 *   - A shared-memory skip-list keyed by (xid, reloid) stores full abort
 *     metadata.  Entries are ordered by xid then reloid, enabling efficient
 *     range operations for per-xid lookups.
 *   - A shared-memory sparsemap (compressed bitmap) provides O(1)
 *     SLogXidIsPresent() checks.
 *   - The skip-list is protected by a single LWLock (sLog modifications
 *     only occur on transaction abort, an uncommon path).
 *   - The sparsemap is protected by a SpinLock (operations are O(1)).
 *
 * Tuple sLog (SLogTupleHash):
 *   - Keyed by (relid, tid), stores up to SLOG_MAX_TUPLE_OPS concurrent
 *     operations per tuple.  Designed for the RECNO table AM.
 *   - Partitioned by tuple_locks[hash(relid,tid) % NUM_SLOG_TUPLE_PARTITIONS].
 *   - WAL-free: entries are transient, removed at commit/abort.
 *
 * Locking: All locks are from LWTRANCHE_SLOG.
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
#include "storage/spin.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

#include "lib/sparsemap.h"

/* ----------------------------------------------------------------
 * Skip-list instantiation for transaction sLog
 *
 * The skip-list is designed for lock-free concurrent access, but we
 * use SKIPLIST_SINGLE_THREADED mode here because:
 *  (a) The pool allocator (shared-memory slab) is not itself lock-free.
 *  (b) The sparsemap is not concurrent-safe.
 *  (c) sLog modifications only happen on transaction abort -- an
 *      uncommon path -- so a single LWLock is sufficient.
 *
 * SKIPLIST_SINGLE_THREADED eliminates C11 <stdatomic.h> dependency,
 * replacing atomics with plain loads/stores.  All concurrent access
 * is serialized externally by txn_lock (LWLock).
 *
 * Max height 16 supports 2^16 = 65,536 entries; the pool holds at
 * most 256 user entries, so this provides ample headroom while
 * minimizing per-node overhead (16 level pointers per pool slot).
 * ----------------------------------------------------------------
 */
#define SKIPLIST_MAX_HEIGHT 16
#define SKIPLIST_SINGLE_THREADED
#include "lib/skiplist.h"

/*
 * struct slog_txn_node - Skip-list node for transaction sLog entries.
 *
 * Ordered by (xid ASC, reloid ASC) so all entries for a given xid
 * are contiguous in the skip-list.
 */
struct slog_txn_node
{
	/* Key fields */
	TransactionId xid;
	Oid			reloid;

	/* Data fields */
	XLogRecPtr	last_batch_lsn; /* LSN of last UNDO batch for this xid */
	Oid			dboid;
	TimestampTz abort_time;
	bool		revert_complete;

	/* Skip-list metadata */
	SKIPLIST_ENTRY(slog_txn) entries;
};

/*
 * Suppress warnings from macro-generated skip-list functions:
 * - Missing prototypes: SKIPLIST_DECL generates non-static functions
 * - Mixed declarations: the macro bodies use C99 style
 * - Unused functions: not all generated functions are called
 */
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmissing-prototypes"
#pragma GCC diagnostic ignored "-Wdeclaration-after-statement"
#pragma GCC diagnostic ignored "-Wunused-function"

/* *INDENT-OFF* */
SKIPLIST_DECL(slog_txn, sl_, entries,
	/* compare_entries: order by (xid, reloid) */
	{
		if (a->xid < b->xid)
			return -1;
		if (a->xid > b->xid)
			return 1;
		if (a->reloid < b->reloid)
			return -1;
		if (a->reloid > b->reloid)
			return 1;
		return 0;
	},
	/* free_entry: no-op (no heap resources to free) */
	{
		(void) node;
	},
	/* update_entry: copy data fields from value */
	{
		slog_txn_node_t *src = (slog_txn_node_t *) value;
		node->last_batch_lsn = src->last_batch_lsn;
		node->dboid = src->dboid;
		node->abort_time = src->abort_time;
		node->revert_complete = src->revert_complete;
	},
	/* archive_entry: deep copy */
	{
		dest->xid = src->xid;
		dest->reloid = src->reloid;
		dest->last_batch_lsn = src->last_batch_lsn;
		dest->dboid = src->dboid;
		dest->abort_time = src->abort_time;
		dest->revert_complete = src->revert_complete;
	},
	/* sizeof_entry */
	{
		bytes = sizeof(slog_txn_node_t);
	})

SKIPLIST_DECL_POOL(slog_txn, sl_, entries, SLOG_TXN_POOL_CAPACITY)
/* *INDENT-ON* */

#pragma GCC diagnostic pop

/* ----------------------------------------------------------------
 * Shared state definition
 * ----------------------------------------------------------------
 */
typedef struct SLogSharedState
{
	/* Transaction skip-list */
	slog_txn_t	txn_list;		/* skip-list head struct */
	_skip_pool_slog_txn_t txn_pool; /* pool allocator struct */
	LWLockPadded txn_lock;		/* single LWLock for skip-list */

	/* XID presence bitmap */
	sparsemap_t xid_map;		/* sparsemap struct */
	slock_t		xid_spinlock;	/* SpinLock for sparsemap */

	/* Tuple hash (unchanged from hash-based implementation) */
	LWLockPadded tuple_locks[NUM_SLOG_TUPLE_PARTITIONS];
} SLogSharedState;

/* ----------------------------------------------------------------
 * Static variables
 * ----------------------------------------------------------------
 */
static HTAB *SLogTupleHash = NULL;
static SLogSharedState *SLogState = NULL;

/* Pointers to ShmemAlloc'd regions, set by ShmemRequestStruct framework */
static char *SLogPoolSlots = NULL;
static char *SLogPoolFreeList = NULL;
static char *SLogXidMapBuffer = NULL;

/* Sentinel value for slh_ebr to redirect node deallocation */
static int	slog_ebr_sentinel = 1;

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
 * EBR retire callback for the skip-list.
 *
 * Instead of pfree()'ing the node (which would crash on shared memory),
 * we return it to the pool's free list.
 */
static void
slog_ebr_retire_callback(void *ebr_state, slog_txn_t *slist, slog_txn_node_t *node)
{
	(void) ebr_state;
	(void) slist;
	sl_skip_pool_free_slog_txn(&SLogState->txn_pool, node);
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
 */
Size
SLogShmemSize(void)
{
	Size		size;
	size_t		raw_slot_size;
	size_t		slot_size;

	size = MAXALIGN(sizeof(SLogSharedState));

	/* Pool slots: node + levels array, rounded to 64-byte alignment */
	raw_slot_size = sizeof(slog_txn_node_t) +
		sizeof(struct _skiplist_slog_txn_level) * SKIPLIST_MAX_HEIGHT;
	slot_size = (raw_slot_size + 63u) & ~(size_t) 63u;
	size = add_size(size, MAXALIGN(slot_size * SLOG_TXN_POOL_CAPACITY));

	/* Pool free-list array */
	size = add_size(size, MAXALIGN(SLOG_TXN_POOL_CAPACITY * sizeof(int32_t)));

	/* Sparsemap buffer */
	size = add_size(size, MAXALIGN(SLOG_XID_MAP_BUFSIZE));

	/* Tuple hash table */
	size = add_size(size, hash_estimate_size(SLOG_TUPLE_HASH_SIZE,
											 sizeof(SLogTupleEntry)));

	return size;
}

/*
 * SLogShmemRequest
 *		Register shared memory needs for the sLog.
 *
 * We register the main state struct, pool regions, sparsemap buffer,
 * and tuple hash table via the ShmemRequestStruct/ShmemRequestHash
 * framework.
 */
void
SLogShmemRequest(void)
{
	/* Register the shared state structure */
	ShmemRequestStruct(.name = "Secondary Log State",
					   .size = sizeof(SLogSharedState),
					   .ptr = (void **) &SLogState,
		);

	/* SLogTupleHash: keyed by SLogTupleKey (unchanged) */
	ShmemRequestHash(.name = "sLog Tuple Hash",
					 .nelems = SLOG_TUPLE_HASH_SIZE,
					 .ptr = &SLogTupleHash,
					 .hash_info.keysize = sizeof(SLogTupleKey),
					 .hash_info.entrysize = sizeof(SLogTupleEntry),
					 .hash_info.num_partitions = NUM_SLOG_TUPLE_PARTITIONS,
					 .hash_flags = HASH_ELEM | HASH_BLOBS |
					 HASH_PARTITION | HASH_FIXED_SIZE,
		);

	/*
	 * Additional shared memory for pool slots, free-list, and sparsemap
	 * buffer registered as separate ShmemRequestStruct entries.
	 */
	{
		size_t		raw_slot_size;
		size_t		slot_size;

		raw_slot_size = sizeof(slog_txn_node_t) +
			sizeof(struct _skiplist_slog_txn_level) * SKIPLIST_MAX_HEIGHT;
		slot_size = (raw_slot_size + 63u) & ~(size_t) 63u;

		ShmemRequestStruct(.name = "sLog Pool Slots",
						   .size = slot_size * SLOG_TXN_POOL_CAPACITY,
						   .ptr = (void **) &SLogPoolSlots,
			);

		ShmemRequestStruct(.name = "sLog Pool FreeList",
						   .size = SLOG_TXN_POOL_CAPACITY * sizeof(int32_t),
						   .ptr = (void **) &SLogPoolFreeList,
			);

		ShmemRequestStruct(.name = "sLog XID Map Buffer",
						   .size = SLOG_XID_MAP_BUFSIZE,
						   .ptr = (void **) &SLogXidMapBuffer,
			);
	}
}

/*
 * SLogShmemInit
 *		Initialize sLog shared memory contents.
 *
 * Called from UndoShmemInit() during the init_fn phase.  The framework
 * has already allocated SLogState and SLogTupleHash.  We manually
 * initialize the skip-list pool, skip-list, and sparsemap.
 */
void
SLogShmemInit(void)
{
	slog_txn_t *slist = &SLogState->txn_list;
	_skip_pool_slog_txn_t *pool = &SLogState->txn_pool;
	size_t		raw_slot_size;
	size_t		slot_size;
	slog_txn_node_t *head_node;
	slog_txn_node_t *tail_node;
	int			i;

	/* ---- Initialize the pool manually in shared memory ---- */

	raw_slot_size = sizeof(slog_txn_node_t) +
		sizeof(struct _skiplist_slog_txn_level) * SKIPLIST_MAX_HEIGHT;
	slot_size = (raw_slot_size + 63u) & ~(size_t) 63u;

	pool->capacity = SLOG_TXN_POOL_CAPACITY;
	pool->slot_size = slot_size;

	/* Use the framework-allocated shared memory regions */
	pool->slots = SLogPoolSlots;
	memset(pool->slots, 0, slot_size * SLOG_TXN_POOL_CAPACITY);

	pool->next_free = (int32_t *) SLogPoolFreeList;
	memset(pool->next_free, 0, SLOG_TXN_POOL_CAPACITY * sizeof(int32_t));

	/* Build the free-list chain */
	for (i = 0; i < SLOG_TXN_POOL_CAPACITY - 1; i++)
		pool->next_free[i] = i + 1;
	pool->next_free[SLOG_TXN_POOL_CAPACITY - 1] = -1;
	pool->free_head = 0;

	/* ---- Initialize the skip-list manually ---- */

	/* Allocate head and tail sentinel nodes from the pool */
	head_node = sl_skip_pool_alloc_slog_txn(pool);
	tail_node = sl_skip_pool_alloc_slog_txn(pool);

	if (head_node == NULL || tail_node == NULL)
		elog(PANIC, "sLog: failed to allocate skip-list sentinels from pool");

	/* Set up sentinel heights and forward pointers */
	head_node->entries.sle_height = 1;
	for (i = 0; i < SKIPLIST_MAX_HEIGHT; i++)
		head_node->entries.sle_levels[i].next = tail_node;
	head_node->entries.sle_prev = NULL;

	tail_node->entries.sle_height = 1;
	for (i = 0; i < SKIPLIST_MAX_HEIGHT; i++)
		tail_node->entries.sle_levels[i].next = NULL;
	tail_node->entries.sle_prev = head_node;

	/* Initialize the skip-list struct */
	slist->slh_length = 0;
	slist->slh_aux = NULL;
	slist->slh_head = head_node;
	slist->slh_tail = tail_node;

	/* Set function pointers */
	slist->slh_fns.free_entry = _skip_free_entry_fn_slog_txn;
	slist->slh_fns.update_entry = _skip_update_entry_fn_slog_txn;
	slist->slh_fns.archive_entry = _skip_archive_entry_fn_slog_txn;
	slist->slh_fns.sizeof_entry = _skip_sizeof_entry_fn_slog_txn;
	slist->slh_fns.compare_entries = _skip_compare_entries_fn_slog_txn;
	slist->slh_fns.snapshot_preserve_node = NULL;
	slist->slh_fns.snapshot_release = NULL;

	/* Snapshot fields unused */
	slist->slh_snap.cur_era = 0;
	slist->slh_snap.pres_era = 0;
	slist->slh_snap.pres = NULL;

	/* Seed PRNG */
	slist->slh_prng_state = ((uint32_t) time(NULL) ^
							 ((uint32_t) MyProcPid << 16) ^
							 (uint32_t) (uintptr_t) slist);
	slist->slh_splay_counter = 0;

	/*
	 * Set EBR hooks to redirect node deallocation to pool free-list instead
	 * of pfree().  We use a non-NULL sentinel for slh_ebr so the skip-list's
	 * remove_node code takes the EBR path.
	 */
	slist->slh_ebr = &slog_ebr_sentinel;
	slist->slh_ebr_retire = slog_ebr_retire_callback;

	/* ---- Initialize the sparsemap ---- */

	sparsemap_init(&SLogState->xid_map, (uint8 *) SLogXidMapBuffer,
				 SLOG_XID_MAP_BUFSIZE);

	/* ---- Initialize locks ---- */

	LWLockInitialize(&SLogState->txn_lock.lock, LWTRANCHE_SLOG);
	SpinLockInit(&SLogState->xid_spinlock);

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
 * Creates an entry in the skip-list and sets the corresponding bit
 * in the XID sparsemap.  Returns false if the pool is full.
 */
bool
SLogTxnInsert(TransactionId xid, Oid reloid, Oid dboid,
			  XLogRecPtr last_batch_lsn)
{
	slog_txn_node_t *node;
	slog_txn_node_t query;
	slog_txn_node_t *existing;
	int			rc;

	LWLockAcquire(&SLogState->txn_lock.lock, LW_EXCLUSIVE);

	/* Check for duplicate first */
	memset(&query, 0, sizeof(query));
	query.xid = xid;
	query.reloid = reloid;

	existing = sl_skip_position_eq_slog_txn(&SLogState->txn_list, &query);
	if (existing != NULL)
	{
		/* Already present -- no-op */
		LWLockRelease(&SLogState->txn_lock.lock);
		return true;
	}

	/* Allocate from pool */
	node = sl_skip_pool_alloc_slog_txn(&SLogState->txn_pool);
	if (node == NULL)
	{
		/* Pool full */
		LWLockRelease(&SLogState->txn_lock.lock);
		return false;
	}

	/* Fill key and data fields */
	node->xid = xid;
	node->reloid = reloid;
	node->last_batch_lsn = last_batch_lsn;
	node->dboid = dboid;
	node->abort_time = GetCurrentTimestamp();
	node->revert_complete = false;

	/* Insert into skip-list */
	rc = sl_skip_insert_slog_txn(&SLogState->txn_list, node);
	if (rc != 0)
	{
		/* Duplicate (shouldn't happen after our check, but be safe) */
		sl_skip_pool_free_slog_txn(&SLogState->txn_pool, node);
		LWLockRelease(&SLogState->txn_lock.lock);
		return true;
	}

	/* Update sparsemap */
	SpinLockAcquire(&SLogState->xid_spinlock);
	sparsemap_add(&SLogState->xid_map, (uint64) xid);
	SpinLockRelease(&SLogState->xid_spinlock);

	LWLockRelease(&SLogState->txn_lock.lock);
	return true;
}

/*
 * SLogXidIsPresent
 *		O(1) check whether a transaction has any sLog entries.
 *
 * This is the hot-path replacement for the old O(N) ATMIsAborted scan.
 * Uses only the SpinLock-protected sparsemap, avoiding the heavier LWLock.
 */
bool
SLogXidIsPresent(TransactionId xid)
{
	bool		result;

	SpinLockAcquire(&SLogState->xid_spinlock);
	result = sparsemap_contains(&SLogState->xid_map, (uint64) xid);
	SpinLockRelease(&SLogState->xid_spinlock);

	return result;
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
	slog_txn_node_t query;
	slog_txn_node_t *found;

	memset(&query, 0, sizeof(query));
	query.xid = xid;
	query.reloid = reloid;

	LWLockAcquire(&SLogState->txn_lock.lock, LW_SHARED);

	found = sl_skip_position_eq_slog_txn(&SLogState->txn_list, &query);

	if (found != NULL && entry_out != NULL)
	{
		entry_out->xid = found->xid;
		entry_out->reloid = found->reloid;
		entry_out->last_batch_lsn = found->last_batch_lsn;
		entry_out->dboid = found->dboid;
		entry_out->abort_time = found->abort_time;
		entry_out->revert_complete = found->revert_complete;
	}

	LWLockRelease(&SLogState->txn_lock.lock);

	return (found != NULL);
}

/*
 * SLogTxnLookupByXid
 *		Find the UNDO chain for a given xid (any reloid).
 *
 * Uses skip-list GTE positioning to find the first entry with the given
 * xid.  O(log n) instead of O(n) hash scan.
 */
bool
SLogTxnLookupByXid(TransactionId xid, XLogRecPtr *lsn_out)
{
	slog_txn_node_t query;
	slog_txn_node_t *found;

	memset(&query, 0, sizeof(query));
	query.xid = xid;
	query.reloid = 0;			/* minimum reloid */

	LWLockAcquire(&SLogState->txn_lock.lock, LW_SHARED);

	found = sl_skip_position_gte_slog_txn(&SLogState->txn_list, &query);

	if (found != NULL && found->xid == xid)
	{
		if (lsn_out)
			*lsn_out = found->last_batch_lsn;
		LWLockRelease(&SLogState->txn_lock.lock);
		return true;
	}

	LWLockRelease(&SLogState->txn_lock.lock);
	return false;
}

/*
 * SLogTxnRemove
 *		Remove a specific (xid, reloid) entry.
 *
 * After removal, checks whether any entries remain for this xid and
 * clears the sparsemap bit if not.
 */
void
SLogTxnRemove(TransactionId xid, Oid reloid)
{
	slog_txn_node_t query;
	slog_txn_node_t check;
	slog_txn_node_t *remaining;
	int			rc;

	memset(&query, 0, sizeof(query));
	query.xid = xid;
	query.reloid = reloid;

	LWLockAcquire(&SLogState->txn_lock.lock, LW_EXCLUSIVE);

	rc = sl_skip_remove_node_slog_txn(&SLogState->txn_list, &query);

	if (rc == 0)
	{
		/* Successfully removed.  Check if any entries remain for this xid. */
		memset(&check, 0, sizeof(check));
		check.xid = xid;
		check.reloid = 0;

		remaining = sl_skip_position_gte_slog_txn(&SLogState->txn_list, &check);

		if (remaining == NULL || remaining->xid != xid)
		{
			/* No entries remain -- clear sparsemap bit */
			SpinLockAcquire(&SLogState->xid_spinlock);
			sparsemap_remove(&SLogState->xid_map, (uint64) xid);
			SpinLockRelease(&SLogState->xid_spinlock);
		}
	}

	LWLockRelease(&SLogState->txn_lock.lock);
}

/*
 * SLogTxnRemoveByXid
 *		Remove all sLog entries for a given transaction ID.
 *
 * Collects all nodes for this xid into a local array, then removes
 * them.  The entries are contiguous in the skip-list thanks to the
 * (xid, reloid) ordering.
 */
void
SLogTxnRemoveByXid(TransactionId xid)
{
	slog_txn_node_t query;
	slog_txn_node_t *node;
	slog_txn_node_t *next;
	slog_txn_node_t **to_remove;
	int			nremove = 0;
	int			max_remove = 64;
	int			i;

	to_remove = (slog_txn_node_t **) palloc(max_remove * sizeof(slog_txn_node_t *));

	memset(&query, 0, sizeof(query));
	query.xid = xid;
	query.reloid = 0;

	LWLockAcquire(&SLogState->txn_lock.lock, LW_EXCLUSIVE);

	/* Collect all nodes with matching xid */
	node = sl_skip_position_gte_slog_txn(&SLogState->txn_list, &query);

	while (node != NULL && node->xid == xid)
	{
		if (nremove >= max_remove)
		{
			max_remove *= 2;
			to_remove = (slog_txn_node_t **)
				repalloc(to_remove, max_remove * sizeof(slog_txn_node_t *));
		}
		to_remove[nremove++] = node;
		next = sl_skip_next_node_slog_txn(&SLogState->txn_list, node);
		node = next;
	}

	/* Remove collected nodes */
	for (i = 0; i < nremove; i++)
	{
		sl_skip_remove_node_slog_txn(&SLogState->txn_list, to_remove[i]);
	}

	/* Clear sparsemap bit */
	if (nremove > 0)
	{
		SpinLockAcquire(&SLogState->xid_spinlock);
		sparsemap_remove(&SLogState->xid_map, (uint64) xid);
		SpinLockRelease(&SLogState->xid_spinlock);
	}

	LWLockRelease(&SLogState->txn_lock.lock);

	pfree(to_remove);
}

/*
 * SLogTxnMarkReverted
 *		Mark all entries for a given xid as revert_complete.
 *
 * Walks the contiguous range of entries for this xid in the skip-list.
 */
void
SLogTxnMarkReverted(TransactionId xid)
{
	slog_txn_node_t query;
	slog_txn_node_t *node;

	memset(&query, 0, sizeof(query));
	query.xid = xid;
	query.reloid = 0;

	LWLockAcquire(&SLogState->txn_lock.lock, LW_EXCLUSIVE);

	node = sl_skip_position_gte_slog_txn(&SLogState->txn_list, &query);

	while (node != NULL && node->xid == xid)
	{
		node->revert_complete = true;
		node = sl_skip_next_node_slog_txn(&SLogState->txn_list, node);
	}

	LWLockRelease(&SLogState->txn_lock.lock);
}

/*
 * SLogTxnGetNextUnreverted
 *		Find an entry that hasn't been reverted yet.
 *
 * Iterates the skip-list from head to tail (ordered by xid), returning
 * the first entry with revert_complete == false.
 */
bool
SLogTxnGetNextUnreverted(TransactionId *xid_out, Oid *dboid_out,
						 XLogRecPtr *lsn_out)
{
	slog_txn_node_t *node;
	size_t		iter;

	LWLockAcquire(&SLogState->txn_lock.lock, LW_SHARED);

	SKIPLIST_FOREACH_H2T(slog_txn, sl_, entries, &SLogState->txn_list, node, iter)
	{
		if (!node->revert_complete)
		{
			*xid_out = node->xid;
			*dboid_out = node->dboid;
			*lsn_out = node->last_batch_lsn;

			LWLockRelease(&SLogState->txn_lock.lock);
			return true;
		}
	}

	LWLockRelease(&SLogState->txn_lock.lock);
	return false;
}

/*
 * SLogRecoveryFinalize
 *		Count entries after recovery for logging.
 */
void
SLogRecoveryFinalize(int *total_out, int *unreverted_out)
{
	slog_txn_node_t *node;
	size_t		iter;
	int			total = 0;
	int			unreverted = 0;

	LWLockAcquire(&SLogState->txn_lock.lock, LW_SHARED);

	SKIPLIST_FOREACH_H2T(slog_txn, sl_, entries, &SLogState->txn_list, node, iter)
	{
		total++;
		if (!node->revert_complete)
			unreverted++;
	}

	LWLockRelease(&SLogState->txn_lock.lock);

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
		/* ops array full -- if entry was just created with nops=0, remove it */
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
