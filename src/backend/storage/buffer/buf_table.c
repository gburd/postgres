/*-------------------------------------------------------------------------
 *
 * buf_table.c
 *	  routines for mapping BufferTags to buffer indexes.
 *
 * Note: the routines in this file do no locking of their own.  The caller
 * must hold a suitable lock on the appropriate BufMappingLock, as specified
 * in the comments.  We can't do the locking inside these functions because
 * in most cases the caller needs to adjust the buffer header contents
 * before the lock is released (see notes in README).
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/buffer/buf_table.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "storage/buf_internals.h"
#include "storage/bufpool_internals.h"
#include "storage/subsystems.h"

/* entry for buffer lookup hashtable */
typedef struct
{
	BufferTag	key;			/* Tag of a disk page */
	int			id;				/* Associated buffer ID */
} BufferLookupEnt;

static HTAB *SharedBufHash;

static void BufTableShmemRequest(void *arg);

const ShmemCallbacks BufTableShmemCallbacks = {
	.request_fn = BufTableShmemRequest,
	/* no special initialization needed, the hash table will start empty */
};

/*
 * Register shmem hash table for mapping buffers.
 *		size is the desired hash table size (possibly more than NBuffers)
 */
void
BufTableShmemRequest(void *arg)
{
	int			size;

	/*
	 * Request the shared buffer lookup hashtable.
	 *
	 * Since we can't tolerate running out of lookup table entries, we must be
	 * sure to specify an adequate table size here.  The maximum steady-state
	 * usage is of course NBuffers entries, but BufferAlloc() tries to insert
	 * a new entry before deleting the old.  In principle this could be
	 * happening in each partition concurrently, so we could need as many as
	 * NBuffers + NUM_BUFFER_PARTITIONS entries.
	 */
	size = NBuffers + NUM_BUFFER_PARTITIONS;

	ShmemRequestHash(.name = "Shared Buffer Lookup Table",
					 .nelems = size,
					 .ptr = &SharedBufHash,
					 .hash_info.keysize = sizeof(BufferTag),
					 .hash_info.entrysize = sizeof(BufferLookupEnt),
					 .hash_info.num_partitions = NUM_BUFFER_PARTITIONS,
					 .hash_flags = HASH_ELEM | HASH_BLOBS | HASH_PARTITION | HASH_FIXED_SIZE,
		);
}

/*
 * BufTableHashCode
 *		Compute the hash code associated with a BufferTag
 *
 * This must be passed to the lookup/insert/delete routines along with the
 * tag.  We do it like this because the callers need to know the hash code
 * in order to determine which buffer partition to lock, and we don't want
 * to do the hash computation twice (hash_any is a bit slow).
 *
 * The hash function used (tag_hash) is the same for both the default pool's
 * SharedBufHash and the dynamic pool open-addressed tables, so the hash
 * code returned here can be used for any pool.
 */
uint32
BufTableHashCode(BufferTag *tagPtr)
{
	return get_hash_value(SharedBufHash, tagPtr);
}

/*
 * BufTableLookup
 *		Lookup the given BufferTag; return buffer ID, or -1 if not found
 *
 * Caller must hold at least share lock on BufMappingLock for tag's partition
 */
int
BufTableLookup(BufferTag *tagPtr, uint32 hashcode)
{
	BufferLookupEnt *result;

	result = (BufferLookupEnt *)
		hash_search_with_hash_value(SharedBufHash,
									tagPtr,
									hashcode,
									HASH_FIND,
									NULL);

	if (!result)
		return -1;

	return result->id;
}

/*
 * BufTableInsert
 *		Insert a hashtable entry for given tag and buffer ID,
 *		unless an entry already exists for that tag
 *
 * Returns -1 on successful insertion.  If a conflicting entry exists
 * already, returns the buffer ID in that entry.
 *
 * Caller must hold exclusive lock on BufMappingLock for tag's partition
 */
int
BufTableInsert(BufferTag *tagPtr, uint32 hashcode, int buf_id)
{
	BufferLookupEnt *result;
	bool		found;

	Assert(buf_id >= 0);		/* -1 is reserved for not-in-table */
	Assert(tagPtr->blockNum != P_NEW);	/* invalid tag */

	result = (BufferLookupEnt *)
		hash_search_with_hash_value(SharedBufHash,
									tagPtr,
									hashcode,
									HASH_ENTER,
									&found);

	if (found)					/* found something already in the table */
		return result->id;

	result->id = buf_id;

	return -1;
}

/*
 * BufTableDelete
 *		Delete the hashtable entry for given tag (which must exist)
 *
 * Caller must hold exclusive lock on BufMappingLock for tag's partition
 */
void
BufTableDelete(BufferTag *tagPtr, uint32 hashcode)
{
	BufferLookupEnt *result;

	result = (BufferLookupEnt *)
		hash_search_with_hash_value(SharedBufHash,
									tagPtr,
									hashcode,
									HASH_REMOVE,
									NULL);

	if (!result)				/* shouldn't happen */
		elog(ERROR, "shared buffer hash table corrupted");
}

/* ----------------------------------------------------------------
 *		Funnel open-addressed hash table for dynamic buffer pools
 *
 * Dynamic pools use a "funnel" open-addressed hash table (Farach-Colton,
 * Krapivin & Kuszmaul, arXiv:2501.02305) stored entirely in DSM memory.
 * The table uses no internal pointers, so it works correctly when the
 * DSM is mapped at different virtual addresses in different backends.
 *
 * The slot array is partitioned into a geometrically decreasing sequence
 * of levels.  Level 0 covers the first half of the array, level 1 the
 * next quarter, and so on, down to a small final level.  A key probes a
 * bounded window (POOL_HASH_LEVEL_PROBES) within each level, hashed to a
 * level-local start position; if the window is full it funnels down to
 * the next, smaller level.  This bounds worst-case probe complexity at
 * O(log^2 1/delta) rather than uniform probing's O(1/delta).
 *
 * The table is created once and never resized.  Because a cache pool
 * holds at most nbuffers live keys and we size capacity above that by
 * the inverse load factor, delta (the empty fraction) is bounded for the
 * life of the pool -- exactly the regime the construction needs.
 *
 * All access is protected by the pool's single mapping LWLock
 * (bp_num_partitions = 1 for dynamic pools).
 * ----------------------------------------------------------------
 */

/*
 * Round up to the next power of two (>= 2).  Funnel levels are carved by
 * repeated halving, which is exact and cheap on a power-of-two capacity.
 */
static inline uint32
pool_hash_next_pow2(uint32 n)
{
	uint32		p = 2;

	while (p < n)
		p <<= 1;
	return p;
}

/*
 * pool_hash_level_bounds
 *		Compute [start, len) of funnel level `level` within an nentries
 *		(power-of-two) array.  Level i occupies the i-th geometric half:
 *		level 0 = [0, n/2), level 1 = [n/2, 3n/4), ...  The final level
 *		holds the last few slots so the levels exactly tile [0, nentries).
 *
 * Returns the number of levels traversed to reach `level`; sets *startp
 * and *lenp.  When level is past the last, *lenp is set to 0.
 */
static inline void
pool_hash_level_bounds(uint32 nentries, int level,
					   uint32 *startp, uint32 *lenp)
{
	uint32		start = 0;
	uint32		remaining = nentries;

	for (int i = 0; i < level; i++)
	{
		uint32		half = remaining / 2;

		if (half < POOL_HASH_LEVEL_PROBES)
		{
			/* collapse the tail into one final level */
			start += 0;
			remaining = 0;
			break;
		}
		start += half;
		remaining -= half;
	}
	*startp = start;
	*lenp = (remaining <= 1) ? remaining : remaining / 2;
	if (*lenp == 0 && remaining > 0)
		*lenp = remaining;
}

/*
 * PoolBufHashNEntries
 *		Compute the number of slots for a pool of given size.
 *
 * Capacity is nbuffers scaled up by the inverse target load factor and
 * rounded to a power of two, so that at most nbuffers live keys occupy
 * at most POOL_HASH_LOAD_NUM/POOL_HASH_LOAD_DEN of the slots.  The table
 * is never resized, so this fixes delta for the life of the pool.
 */
int
PoolBufHashNEntries(int nbuffers)
{
	uint64		need;

	Assert(nbuffers > 0);

	/* slots = nbuffers * (LOAD_DEN / LOAD_NUM), rounded up */
	need = ((uint64) nbuffers * POOL_HASH_LOAD_DEN + POOL_HASH_LOAD_NUM - 1)
		/ POOL_HASH_LOAD_NUM;

	return (int) pool_hash_next_pow2((uint32) need);
}

/*
 * PoolBufHashSize
 *		Compute the total size of the hash table slot array.
 */
Size
PoolBufHashSize(int nbuffers)
{
	return (Size) PoolBufHashNEntries(nbuffers) * sizeof(PoolBufHashEntry);
}

/*
 * PoolBufHashInit
 *		Initialize all slots to unused.
 */
void
PoolBufHashInit(PoolBufHashEntry *entries, int nentries)
{
	for (int i = 0; i < nentries; i++)
	{
		ClearBufferTag(&entries[i].key);
		entries[i].id = POOL_HASH_UNUSED;
	}
}

/*
 * pool_hash_secondary
 *		Derive an independent second hash from the primary hashcode so
 *		level-local start positions are not correlated across levels.
 *		Finalizer mix (splitmix64-style) on the 32-bit primary hash.
 */
static inline uint32
pool_hash_secondary(uint32 hashcode)
{
	uint32		h = hashcode;

	h ^= h >> 16;
	h *= 0x7feb352dU;
	h ^= h >> 15;
	h *= 0x846ca68bU;
	h ^= h >> 16;
	return h;
}

/*
 * PoolBufHashLookup
 *		Look up a buffer tag.  Returns buffer ID, or -1 if not found.
 *
 * Walks the funnel levels: within each level, probes a bounded window
 * starting at a level-local hashed position.  A POOL_HASH_UNUSED slot in
 * the window ends the search (the key was never funneled past here).
 *
 * Caller must hold at least share lock on the pool's mapping lock.
 */
int
PoolBufHashLookup(PoolBufHashEntry *entries, int nentries,
				  BufferTag *tag, uint32 hashcode)
{
	uint32		sec = pool_hash_secondary(hashcode);

	for (int level = 0;; level++)
	{
		uint32		start,
					len;

		pool_hash_level_bounds((uint32) nentries, level, &start, &len);
		if (len == 0)
			return -1;		/* exhausted all levels */

		/* level-local start position from a level-mixed hash */
		{
			uint32		lh = (hashcode + (uint32) level * sec);
			uint32		base = lh % len;

			for (uint32 j = 0; j < POOL_HASH_LEVEL_PROBES && j < len; j++)
			{
				uint32		probe = start + (base + j) % len;
				int32		id = entries[probe].id;

				if (id == POOL_HASH_UNUSED)
					return -1;	/* end of this key's funnel chain */
				if (id == POOL_HASH_DELETED)
					continue;	/* tombstone, keep probing */
				if (BufferTagsEqual(&entries[probe].key, tag))
					return id;	/* found */
			}
			/* window full -> funnel to next level */
		}
	}
}

/*
 * PoolBufHashInsert
 *		Insert a tag/buf_id pair.  Returns -1 on successful insertion, or
 *		the existing buffer ID if a conflicting entry already exists.
 *
 * Caller must hold exclusive lock on the pool's mapping lock.
 */
int
PoolBufHashInsert(PoolBufHashEntry *entries, int nentries,
				  BufferTag *tag, uint32 hashcode, int buf_id)
{
	uint32		sec = pool_hash_secondary(hashcode);
	uint32		first_deleted_probe = 0;
	bool		have_deleted = false;

	Assert(buf_id >= 0);
	Assert(tag->blockNum != P_NEW);

	for (int level = 0;; level++)
	{
		uint32		start,
					len;

		pool_hash_level_bounds((uint32) nentries, level, &start, &len);
		if (len == 0)
			break;			/* exhausted all levels */

		{
			uint32		lh = (hashcode + (uint32) level * sec);
			uint32		base = lh % len;

			for (uint32 j = 0; j < POOL_HASH_LEVEL_PROBES && j < len; j++)
			{
				uint32		probe = start + (base + j) % len;
				int32		id = entries[probe].id;

				if (id == POOL_HASH_UNUSED)
				{
					/* end of chain: insert at first tombstone if any */
					if (have_deleted)
						probe = first_deleted_probe;
					entries[probe].key = *tag;
					entries[probe].id = buf_id;
					return -1;
				}
				if (id == POOL_HASH_DELETED)
				{
					if (!have_deleted)
					{
						first_deleted_probe = probe;
						have_deleted = true;
					}
					continue;
				}
				if (BufferTagsEqual(&entries[probe].key, tag))
					return id;	/* key already exists */
			}
			/* window full -> funnel to next level */
		}
	}

	/* Every level's window was occupied; reuse a tombstone if we saw one. */
	if (have_deleted)
	{
		entries[first_deleted_probe].key = *tag;
		entries[first_deleted_probe].id = buf_id;
		return -1;
	}

	/*
	 * Genuinely full.  With correct from-start sizing (load factor bounded
	 * below 1) and the funnel construction this is unreachable, but keep a
	 * hard guard rather than silently corrupting the cache.
	 */
	elog(ERROR, "pool buffer hash table is full");
	pg_unreachable();
}

/*
 * PoolBufHashDelete
 *		Delete the entry for the given tag (which must exist).  Uses
 *		tombstone deletion (POOL_HASH_DELETED).
 *
 * Caller must hold exclusive lock on the pool's mapping lock.
 */
void
PoolBufHashDelete(PoolBufHashEntry *entries, int nentries,
				  BufferTag *tag, uint32 hashcode)
{
	uint32		sec = pool_hash_secondary(hashcode);

	for (int level = 0;; level++)
	{
		uint32		start,
					len;

		pool_hash_level_bounds((uint32) nentries, level, &start, &len);
		if (len == 0)
			break;

		{
			uint32		lh = (hashcode + (uint32) level * sec);
			uint32		base = lh % len;

			for (uint32 j = 0; j < POOL_HASH_LEVEL_PROBES && j < len; j++)
			{
				uint32		probe = start + (base + j) % len;
				int32		id = entries[probe].id;

				if (id == POOL_HASH_UNUSED)
					elog(ERROR, "pool buffer hash table corrupted: entry not found");
				if (id == POOL_HASH_DELETED)
					continue;
				if (BufferTagsEqual(&entries[probe].key, tag))
				{
					entries[probe].id = POOL_HASH_DELETED;
					return;
				}
			}
		}
	}

	elog(ERROR, "pool buffer hash table corrupted: entry not found");
}
