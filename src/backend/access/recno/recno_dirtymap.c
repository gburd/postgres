/*-------------------------------------------------------------------------
 *
 * recno_dirtymap.c
 *	  Shared-memory dirty block map for the RECNO table access method.
 *
 * This module tracks which heap pages have uncommitted in-place updates
 * (dirty pages).  The scan path uses this as a fast-path filter: if a
 * page is NOT in the dirty map, all tuples are committed and per-tuple
 * sLog lookups can be skipped entirely.
 *
 * Implementation:
 *   - A partitioned shared hash table (dynahash) keyed on (Oid relid,
 *     BlockNumber blkno) with a uint32 dirty_count value.
 *   - Each backend maintains a local tracking array of increments it has
 *     made, tagged with SubTransactionId for savepoint support.
 *   - At COMMIT, the backend decrements each tracked entry and removes
 *     hash entries whose count reaches zero.
 *   - At ABORT, the backend discards its local tracking without modifying
 *     the shared map (conservative-correct: stale entries just cause extra
 *     sLog lookups until cleaned up by other commits or VACUUM).
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/recno/recno_dirtymap.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/recno_dirtymap.h"
#include "access/xact.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"

/* ----------------------------------------------------------------
 * Shared hash table definitions
 * ----------------------------------------------------------------
 */

/*
 * Hash key: identifies a specific page in a specific relation.
 */
typedef struct DirtyMapKey
{
	Oid			relid;
	BlockNumber blkno;
}			DirtyMapKey;

/*
 * Hash entry: the key plus a reference count of uncommitted modifications.
 */
typedef struct DirtyMapEntry
{
	DirtyMapKey key;			/* hash key -- must be first */
	uint32		dirty_count;	/* number of uncommitted modifications */
}			DirtyMapEntry;

/*
 * Number of lock partitions for the shared hash table.
 * Must be a power of 2.  16 partitions give good concurrency without
 * excessive memory for the LWLock array.
 */
#define NUM_DIRTYMAP_PARTITIONS		16

/*
 * Floor capacity of the shared hash table when shared_buffers is tiny.
 * The actual size is max(NBuffers, this); see RecnoDirtyMapTableSize.
 */
#define DIRTYMAP_MIN_SIZE			4096

/*
 * Capacity of the shared dirty-page hash table.
 *
 * This bounds the number of distinct pages carrying uncommitted in-place
 * modifications across all relations.  A page can only accumulate such
 * modifications while it is resident in a shared buffer (writers pin the
 * buffer to stamp the tuple), so NBuffers is the natural ceiling and makes
 * the table scale with shared_buffers instead of a fixed constant.  The old
 * fixed 4096 saturated under high write concurrency (the "RECNO dirty map
 * hash table full" warning), which forced every page onto the slow per-tuple
 * sLog lookup path.  We keep DIRTYMAP_MIN_SIZE as a floor for tiny configs.
 *
 * The table is HASH_FIXED_SIZE and does not grow at runtime.
 */
static long
RecnoDirtyMapTableSize(void)
{
	long		nelems = NBuffers;

	if (nelems < DIRTYMAP_MIN_SIZE)
		nelems = DIRTYMAP_MIN_SIZE;
	return nelems;
}

/* The shared hash table handle */
static HTAB *DirtyMapHash = NULL;

/* ----------------------------------------------------------------
 * Backend-local tracking of increments
 * ----------------------------------------------------------------
 */

/*
 * Each time the backend calls RecnoDirtyMapTrackIncrement, we append one
 * of these entries to our local array.  At commit we walk the array and
 * decrement each; at abort we discard the array.
 */
typedef struct DirtyMapTrackEntry
{
	Oid			relid;
	BlockNumber blkno;
	SubTransactionId subxid;	/* subtransaction that made this increment */
}			DirtyMapTrackEntry;

/*
 * Backend-local tracking state.  Allocated in TopTransactionContext so
 * it is automatically freed at end-of-transaction.
 */
static DirtyMapTrackEntry * dirtymap_track_entries = NULL;
static int	dirtymap_track_count = 0;
static int	dirtymap_track_capacity = 0;

/* Memory context for the tracking array */
#define DIRTYMAP_TRACK_INIT_SIZE	64

/* ----------------------------------------------------------------
 * Shared memory initialization
 * ----------------------------------------------------------------
 */

/*
 * RecnoDirtyMapShmemSize
 *		Compute shared memory needed for the dirty map hash table.
 *
 * The actual memory is allocated by ShmemRequestHash; this function is
 * provided for informational/estimation purposes.
 */
Size
RecnoDirtyMapShmemSize(void)
{
	return hash_estimate_size(RecnoDirtyMapTableSize(), sizeof(DirtyMapEntry));
}

/*
 * Shmem request callback: register our hash table with the shmem system.
 */
static void
RecnoDirtyMapShmemRequest(void *arg)
{
	ShmemRequestHash(.name = "RECNO DirtyMap Hash",
					 .nelems = RecnoDirtyMapTableSize(),
					 .ptr = &DirtyMapHash,
					 .hash_info.keysize = sizeof(DirtyMapKey),
					 .hash_info.entrysize = sizeof(DirtyMapEntry),
					 .hash_info.num_partitions = NUM_DIRTYMAP_PARTITIONS,
					 .hash_flags = HASH_ELEM | HASH_BLOBS |
					 HASH_PARTITION | HASH_FIXED_SIZE,
		);
}

/*
 * Shmem init callback: nothing additional to do -- the hash table is
 * initialized by the shmem framework after ShmemRequestHash.
 */
static void
RecnoDirtyMapShmemInit_cb(void *arg)
{
	/* Hash table is already initialized by the shmem framework */
	Assert(DirtyMapHash != NULL);
}

/*
 * RecnoDirtyMapShmemInit
 *		Legacy entry point for explicit initialization (called from recno.h
 *		declarations).  With the ShmemCallbacks pattern, this is a no-op
 *		because initialization is driven by the callbacks.
 */
void
RecnoDirtyMapShmemInit(void)
{
	/* Initialization is handled by RecnoDirtyMapShmemCallbacks */
}

/*
 * ShmemCallbacks struct registered in subsystemlist.h.
 */
const ShmemCallbacks RecnoDirtyMapShmemCallbacks = {
	.request_fn = RecnoDirtyMapShmemRequest,
	.init_fn = RecnoDirtyMapShmemInit_cb,
};

/* ----------------------------------------------------------------
 * Per-relation map lifecycle
 * ----------------------------------------------------------------
 */

/*
 * RecnoDirtyMapOpen
 *		Called when a scan begins on a relation.
 *
 * Currently a no-op: the shared hash table does not maintain per-relation
 * metadata.  Retained for API symmetry with RecnoDirtyMapClose and to
 * allow future per-relation tracking (e.g., bloom filter optimization).
 */
void
RecnoDirtyMapOpen(Oid relid, BlockNumber nblocks)
{
	/* No-op: the shared hash is relation-agnostic */
}

/*
 * RecnoDirtyMapClose
 *		Called when a scan ends on a relation.
 *
 * Currently a no-op.  See RecnoDirtyMapOpen.
 */
void
RecnoDirtyMapClose(Oid relid)
{
	/* No-op */
}

/*
 * RecnoDirtyMapExtend
 *		Called when new blocks are added to the relation.
 *
 * Currently a no-op: the hash table accepts any (relid, blkno) pair
 * without pre-registration of block ranges.
 */
void
RecnoDirtyMapExtend(Oid relid, BlockNumber nblocks)
{
	/* No-op: hash table is not bounded by relation size */
}

/* ----------------------------------------------------------------
 * Dirty count manipulation
 * ----------------------------------------------------------------
 */

/*
 * RecnoDirtyMapIncrement
 *		Increment the dirty_count for (relid, blkno) in the shared hash.
 *
 * If no entry exists, one is created with dirty_count = 1.
 */
void
RecnoDirtyMapIncrement(Oid relid, BlockNumber blkno)
{
	DirtyMapKey key;
	DirtyMapEntry *entry;
	bool		found;

	key.relid = relid;
	key.blkno = blkno;

	entry = (DirtyMapEntry *) hash_search(DirtyMapHash, &key,
										  HASH_ENTER_NULL, &found);
	if (entry == NULL)
	{
		/*
		 * Hash table is full.  This is a soft failure -- we cannot track this
		 * page, so the scan path will still work correctly (it will just not
		 * get the fast-path optimization for this page).  Log a warning and
		 * return.
		 */
		ereport(WARNING,
				(errmsg("RECNO dirty map hash table full, cannot track block %u of relation %u",
						blkno, relid)));
		return;
	}

	if (!found)
		entry->dirty_count = 1;
	else
		entry->dirty_count++;
}

/*
 * RecnoDirtyMapDecrement
 *		Decrement the dirty_count for (relid, blkno).
 *		Removes the entry if count reaches zero.
 *
 * This is an internal helper called from RecnoDirtyMapDecrementTracked.
 */
static void
RecnoDirtyMapDecrement(Oid relid, BlockNumber blkno)
{
	DirtyMapKey key;
	DirtyMapEntry *entry;
	bool		found;

	key.relid = relid;
	key.blkno = blkno;

	entry = (DirtyMapEntry *) hash_search(DirtyMapHash, &key,
										  HASH_FIND, &found);
	if (!found)
	{
		/*
		 * Entry not found.  This can happen if the hash table was full when
		 * the increment was attempted, or due to a race in abort paths. Not
		 * an error -- the map is advisory.
		 */
		return;
	}

	Assert(entry->dirty_count > 0);
	entry->dirty_count--;

	if (entry->dirty_count == 0)
	{
		/* Remove the entry entirely */
		hash_search(DirtyMapHash, &key, HASH_REMOVE, NULL);
	}
}

/* ----------------------------------------------------------------
 * Backend-local tracking
 * ----------------------------------------------------------------
 */

/*
 * EnsureTrackingCapacity
 *		Ensure the backend-local tracking array has room for one more entry.
 *		Allocates or grows the array in TopTransactionContext.
 */
static void
EnsureTrackingCapacity(void)
{
	if (dirtymap_track_entries == NULL)
	{
		/* First call in this transaction -- allocate in TopTransactionContext */
		MemoryContext oldctx;

		oldctx = MemoryContextSwitchTo(TopTransactionContext);
		dirtymap_track_capacity = DIRTYMAP_TRACK_INIT_SIZE;
		dirtymap_track_entries = (DirtyMapTrackEntry *)
			palloc(dirtymap_track_capacity * sizeof(DirtyMapTrackEntry));
		MemoryContextSwitchTo(oldctx);
	}
	else if (dirtymap_track_count >= dirtymap_track_capacity)
	{
		/* Need to grow */
		MemoryContext oldctx;

		oldctx = MemoryContextSwitchTo(TopTransactionContext);
		dirtymap_track_capacity *= 2;
		dirtymap_track_entries = (DirtyMapTrackEntry *)
			repalloc(dirtymap_track_entries,
					 dirtymap_track_capacity * sizeof(DirtyMapTrackEntry));
		MemoryContextSwitchTo(oldctx);
	}
}

/*
 * RecnoDirtyMapTrackIncrement
 *		Record an increment in the backend-local tracking list.
 *
 * Called immediately after RecnoDirtyMapIncrement so that we can reverse
 * it at commit time.  The entry is tagged with the current subtransaction
 * ID for savepoint support.
 */
void
RecnoDirtyMapTrackIncrement(Oid relid, BlockNumber blkno)
{
	DirtyMapTrackEntry *te;

	EnsureTrackingCapacity();

	te = &dirtymap_track_entries[dirtymap_track_count++];
	te->relid = relid;
	te->blkno = blkno;
	te->subxid = GetCurrentSubTransactionId();
}

/*
 * RecnoDirtyMapDecrementTracked
 *		Decrement dirty_count for all blocks this transaction dirtied.
 *		Called at COMMIT.
 */
void
RecnoDirtyMapDecrementTracked(void)
{
	int			i;

	for (i = 0; i < dirtymap_track_count; i++)
	{
		DirtyMapTrackEntry *te = &dirtymap_track_entries[i];

		RecnoDirtyMapDecrement(te->relid, te->blkno);
	}

	/* Reset the tracking state; memory freed by TopTransactionContext reset */
	dirtymap_track_entries = NULL;
	dirtymap_track_count = 0;
	dirtymap_track_capacity = 0;
}

/*
 * RecnoDirtyMapDiscardTracked
 *		Discard the backend-local tracking list without decrementing.
 *		Called at ABORT.
 *
 * On abort, we intentionally leave the shared dirty_count elevated.
 * This is conservative-correct: the scan path will still consult the sLog
 * for those pages.  The counts will eventually be cleaned up when other
 * transactions commit their modifications to the same pages, or by a
 * future VACUUM pass.
 */
void
RecnoDirtyMapDiscardTracked(void)
{
	/* Just reset; memory freed by TopTransactionContext reset */
	dirtymap_track_entries = NULL;
	dirtymap_track_count = 0;
	dirtymap_track_capacity = 0;
}

/* ----------------------------------------------------------------
 * Subtransaction support
 * ----------------------------------------------------------------
 */

/*
 * RecnoDirtyMapDiscardTrackedSubXact
 *		Discard tracking entries for a specific subtransaction (on subxact abort).
 *
 * We do NOT decrement the shared counters -- same reasoning as
 * RecnoDirtyMapDiscardTracked.  We remove the entries from the tracking
 * array by compacting it in place.
 */
void
RecnoDirtyMapDiscardTrackedSubXact(SubTransactionId subxid)
{
	int			dst = 0;
	int			i;

	for (i = 0; i < dirtymap_track_count; i++)
	{
		if (dirtymap_track_entries[i].subxid != subxid)
		{
			if (dst != i)
				dirtymap_track_entries[dst] = dirtymap_track_entries[i];
			dst++;
		}
	}

	dirtymap_track_count = dst;
}

/*
 * RecnoDirtyMapReparentTrackedSubXact
 *		Reparent tracking entries from a committed subtransaction to its parent.
 *
 * On subtransaction commit, ownership of the dirty increments transfers to
 * the parent subtransaction.  If the parent later aborts, the discard will
 * correctly match these reparented entries.
 */
void
RecnoDirtyMapReparentTrackedSubXact(SubTransactionId child,
									SubTransactionId parent)
{
	int			i;

	for (i = 0; i < dirtymap_track_count; i++)
	{
		if (dirtymap_track_entries[i].subxid == child)
			dirtymap_track_entries[i].subxid = parent;
	}
}

/* ----------------------------------------------------------------
 * Query interface
 * ----------------------------------------------------------------
 */

/*
 * RecnoDirtyMapCheck
 *		Returns true if the specified block has uncommitted modifications.
 *
 * If the block is not in the hash table, it has no uncommitted changes
 * and the caller can skip per-tuple sLog lookups for the entire page.
 */
bool
RecnoDirtyMapCheck(Oid relid, BlockNumber blkno)
{
	DirtyMapKey key;
	bool		found;

	key.relid = relid;
	key.blkno = blkno;

	hash_search(DirtyMapHash, &key, HASH_FIND, &found);

	return found;
}
