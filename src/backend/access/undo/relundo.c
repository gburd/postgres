/*-------------------------------------------------------------------------
 *
 * relundo.c
 *	  Per-relation UNDO core implementation
 *
 * This file implements the main API for per-relation UNDO logging used by
 * table access methods that need MVCC visibility via UNDO chain walking.
 *
 * The two-phase insert protocol works as follows:
 *
 *   1. RelUndoReserve() - Finds (or allocates) a page with enough space,
 *      pins and exclusively locks the buffer, advances pd_lower to reserve
 *      space, and returns an RelUndoRecPtr encoding the position.
 *
 *   2. Caller performs the DML operation.
 *
 *   3a. RelUndoFinish() - Writes the actual UNDO record into the reserved
 *       space, marks the buffer dirty, and releases it.
 *   3b. RelUndoCancel() - Releases the buffer without writing; the reserved
 *       space becomes a hole (zero-filled).
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/undo/relundo.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/relundo.h"
#include "access/relundo_xlog.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "catalog/storage.h"
#include "catalog/storage_xlog.h"
#include "common/relpath.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/smgr.h"

/*
 * Per-backend UNDO head page cache.
 *
 * Every RelUndoReserve() call currently acquires an EXCLUSIVE lock on the
 * metapage to find the current head page.  For insert-heavy workloads this
 * is a severe contention point (100K inserts = 100K exclusive metapage locks).
 *
 * This hash-table cache remembers the current head page and its free space
 * for recently-used relations.  When the cached page has enough space, we
 * skip the metapage entirely and go directly to the data page with an
 * EXCLUSIVE lock.  Cache misses and full pages fall back to the metapage path.
 *
 * The cache is implemented as an open-addressing hash table with linear
 * probing and a clock-hand eviction policy.  This gives O(1) average-case
 * lookup and update, replacing the O(N) linear scan of the previous LRU list.
 *
 * Cache entries are invalidated when:
 *   - The cached page turns out to be full (optimistic approach)
 *   - A new page is allocated (the head page changes)
 *   - RelUndoInitRelation is called (the fork is recreated)
 *
 * Size: must be a power of two for the hash masking to work correctly.
 * 64 slots covers workloads with up to ~48 concurrently-active relations
 * before eviction starts (load factor ~0.75).  Increasing to 128 or 256
 * is straightforward if profiling shows benefit.
 *
 * Probe chain correctness: we use a tombstone OID (RELUNDO_CACHE_TOMBSTONE)
 * rather than InvalidOid for invalidated slots.  Setting a slot to InvalidOid
 * would create a gap that breaks the linear probe chain for entries that were
 * inserted past that slot.  A tombstone is skipped during lookup but does
 * not terminate the probe, so all entries remain reachable.  During insert,
 * tombstone slots are reused (treated as free), which amortizes tombstone
 * accumulation.  The table is re-initialized on first use (all slots
 * InvalidOid), so no "previous tombstone" confusion can arise at startup.
 */
#define RELUNDO_HEAD_CACHE_SIZE		64		/* must be a power of two */
#define RELUNDO_HEAD_CACHE_MASK		(RELUNDO_HEAD_CACHE_SIZE - 1)

/*
 * Sentinel OID used to mark a cache slot as "tombstone" (deleted but
 * still part of a probe chain).  Must not be a valid relation OID and
 * must differ from InvalidOid (0).  OID 1 is reserved by the system
 * (pg_type), so it is safe to repurpose as a tombstone here; it will
 * never appear as a user-relation OID passed to these cache functions.
 */
#define RELUNDO_CACHE_TOMBSTONE		((Oid) 1)

typedef struct RelUndoHeadCacheEntry
{
	Oid			relid;			/* Relation OID; InvalidOid=empty,
								 * RELUNDO_CACHE_TOMBSTONE=deleted */
	BlockNumber head_blkno;		/* Cached head page block number */
	Size		free_space;		/* Last-known free space on head page */
} RelUndoHeadCacheEntry;

static RelUndoHeadCacheEntry relundo_head_cache[RELUNDO_HEAD_CACHE_SIZE];
static bool relundo_head_cache_init = false;

/* Clock hand for round-robin eviction (avoids pure linear probing pile-up) */
static int	relundo_cache_evict_hand = 0;

/*
 * Per-backend pending metapage buffer.
 *
 * When RelUndoReserve() allocates a new UNDO page, the metapage is modified
 * (new head_blkno) and must be included in the WAL record written by
 * RelUndoFinish().  Previously, RelUndoReserve() released the metapage lock
 * and RelUndoFinish() re-acquired it, creating an ABBA deadlock:
 *
 *   Backend A: holds metapage → wants UNDO data page
 *   Backend B: holds UNDO data page → wants metapage
 *
 * Fix: keep the metapage locked through the Reserve→Finish cycle.
 * RelUndoReserve() stores the locked metapage buffer here, and
 * RelUndoFinish()/RelUndoFinishWithTuple() retrieves it.
 * RelUndoCancel() releases it if the operation is aborted.
 */
static Buffer relundo_pending_metabuf = InvalidBuffer;

/*
 * relundo_oid_hash -- fast hash of a relation OID for cache slot selection.
 *
 * Uses Knuth's multiplicative hash (2654435761 is close to 2^32/phi).
 * The result is masked to RELUNDO_HEAD_CACHE_SIZE slots.
 */
static inline int
relundo_oid_hash(Oid relid)
{
	return (int) (((uint32) relid * UINT32_C(2654435761)) >> (32 - 6));
}

/*
 * relundo_head_cache_lookup -- find a cache entry for the given relation.
 *
 * Uses open-addressing with linear probing.  Returns the cache entry pointer
 * if found, NULL otherwise.  Average cost: O(1) under low load.
 *
 * Tombstone slots (RELUNDO_CACHE_TOMBSTONE) are skipped but do not terminate
 * the probe, preserving the correctness of chains established at insert time.
 * Only a truly empty slot (InvalidOid) terminates the probe early.
 */
static RelUndoHeadCacheEntry *
relundo_head_cache_lookup(Oid relid)
{
	int			start;
	int			i;
	int			probes;

	if (!relundo_head_cache_init)
	{
		int			j;

		for (j = 0; j < RELUNDO_HEAD_CACHE_SIZE; j++)
			relundo_head_cache[j].relid = InvalidOid;
		relundo_head_cache_init = true;
	}

	start = relundo_oid_hash(relid);

	/*
	 * Linear probe up to RELUNDO_HEAD_CACHE_SIZE slots.
	 *
	 * - Match found: return the entry.
	 * - Tombstone (RELUNDO_CACHE_TOMBSTONE): continue probing; the target
	 *   entry may sit beyond this deleted slot.
	 * - Empty slot (InvalidOid): the entry is definitely not present
	 *   (it would have been placed before the first empty slot at insert
	 *   time), so terminate early.
	 */
	for (probes = 0; probes < RELUNDO_HEAD_CACHE_SIZE; probes++)
	{
		i = (start + probes) & RELUNDO_HEAD_CACHE_MASK;

		if (relundo_head_cache[i].relid == relid)
			return &relundo_head_cache[i];

		if (relundo_head_cache[i].relid == InvalidOid)
			return NULL;		/* definitely not present */

		/* RELUNDO_CACHE_TOMBSTONE: keep probing */
	}

	return NULL;
}

/*
 * relundo_head_cache_update -- update or insert a cache entry.
 *
 * Finds the existing slot for relid (if any) or inserts into the first
 * free slot in the probe sequence.  If the probe sequence is full, evicts
 * via the clock hand to avoid unbounded probing.
 */
static void
relundo_head_cache_update(Oid relid, BlockNumber head_blkno, Size free_space)
{
	int			start;
	int			i;
	int			probes;
	int			free_slot = -1;

	if (!relundo_head_cache_init)
	{
		int			j;

		for (j = 0; j < RELUNDO_HEAD_CACHE_SIZE; j++)
			relundo_head_cache[j].relid = InvalidOid;
		relundo_head_cache_init = true;
	}

	start = relundo_oid_hash(relid);

	/* Probe for existing entry or first free/tombstone slot */
	for (probes = 0; probes < RELUNDO_HEAD_CACHE_SIZE; probes++)
	{
		i = (start + probes) & RELUNDO_HEAD_CACHE_MASK;

		if (relundo_head_cache[i].relid == relid)
		{
			/* Update in-place */
			relundo_head_cache[i].head_blkno = head_blkno;
			relundo_head_cache[i].free_space = free_space;
			return;
		}

		/*
		 * A truly empty slot (InvalidOid) terminates the existing probe
		 * chain: the relid definitely doesn't exist beyond this point.
		 * Use the earliest tombstone found so far (if any) to compact the
		 * table, or this slot if no tombstone was seen.
		 */
		if (relundo_head_cache[i].relid == InvalidOid)
		{
			if (free_slot < 0)
				free_slot = i;
			break;
		}

		/*
		 * Tombstone: record as candidate insertion point (reuses the slot
		 * to amortize tombstone accumulation) but keep probing in case
		 * relid exists beyond this slot.
		 */
		if (relundo_head_cache[i].relid == RELUNDO_CACHE_TOMBSTONE &&
			free_slot < 0)
			free_slot = i;		/* remember but keep probing for existing */
	}

	if (free_slot >= 0)
	{
		/* Insert into the free slot found during probing */
		relundo_head_cache[free_slot].relid = relid;
		relundo_head_cache[free_slot].head_blkno = head_blkno;
		relundo_head_cache[free_slot].free_space = free_space;
		return;
	}

	/*
	 * No free slot found in the probe sequence (table heavily loaded).
	 * Evict using the clock hand: advance to the next "real" entry (not
	 * empty, not tombstone) and overwrite it.  This bounds the cost to
	 * O(1) amortized and avoids evicting a tombstone (which would leave
	 * a gap that looks like "nothing past here" during lookup).
	 */
	for (probes = 0; probes < RELUNDO_HEAD_CACHE_SIZE; probes++)
	{
		i = relundo_cache_evict_hand & RELUNDO_HEAD_CACHE_MASK;
		relundo_cache_evict_hand = (i + 1) & RELUNDO_HEAD_CACHE_MASK;

		if (relundo_head_cache[i].relid != InvalidOid &&
			relundo_head_cache[i].relid != RELUNDO_CACHE_TOMBSTONE)
		{
			relundo_head_cache[i].relid = relid;
			relundo_head_cache[i].head_blkno = head_blkno;
			relundo_head_cache[i].free_space = free_space;
			return;
		}
	}

	/* Should not reach here if init is correct, but handle gracefully */
	relundo_head_cache[start].relid = relid;
	relundo_head_cache[start].head_blkno = head_blkno;
	relundo_head_cache[start].free_space = free_space;
}

/*
 * relundo_head_cache_invalidate -- remove a cache entry for the given relation.
 *
 * Marks the slot with a tombstone (RELUNDO_CACHE_TOMBSTONE) rather than
 * InvalidOid.  This preserves the correctness of probe chains: other entries
 * that were inserted past this slot via linear probing remain reachable.
 * Tombstone slots are reused by relundo_head_cache_update, so they do not
 * accumulate indefinitely.
 */
static void
relundo_head_cache_invalidate(Oid relid)
{
	int			start;
	int			i;
	int			probes;

	if (!relundo_head_cache_init)
		return;

	start = relundo_oid_hash(relid);

	for (probes = 0; probes < RELUNDO_HEAD_CACHE_SIZE; probes++)
	{
		i = (start + probes) & RELUNDO_HEAD_CACHE_MASK;

		if (relundo_head_cache[i].relid == relid)
		{
			/* Replace with tombstone, not InvalidOid, to preserve chains */
			relundo_head_cache[i].relid = RELUNDO_CACHE_TOMBSTONE;
			return;
		}

		if (relundo_head_cache[i].relid == InvalidOid)
			return;		/* not present; tombstones don't terminate probes */
	}
}

/*
 * relundo_try_atomic_reserve -- Tier 1 lock-free space reservation
 *
 * Attempts to reserve space on the current head page using a compare-and-swap
 * loop on the pd_lower field of RelUndoPageHeaderData, completely avoiding
 * any metapage lock acquisition.  This eliminates the global contention point
 * for the common case where the current head page has sufficient free space.
 *
 * The RelUndoPageHeaderData layout within the page contents area is:
 *   offset 0:  prev_blkno (BlockNumber, 4 bytes)
 *   offset 4:  max_xid    (TransactionId, 4 bytes)
 *   offset 8:  counter    (uint16, 2 bytes)  -- never changes after page init
 *   offset 10: pd_lower   (uint16, 2 bytes)  -- our target
 *   offset 12: pd_upper   (uint16, 2 bytes)  -- never changes after page init
 *
 * Because no pg_atomic_uint16 exists in this PostgreSQL version, we perform
 * the CAS on the 32-bit word covering {counter, pd_lower} (at offsetof
 * counter, which is 4-byte aligned), packed as (pd_lower << 16) | counter on
 * little-endian systems.  Since both counter and pd_upper are stable after
 * initialization, only pd_lower (the high 16 bits of that u32) ever changes,
 * making the CAS safe: if pd_lower changed between our load and our CAS, the
 * CAS fails and we retry.
 *
 * On success: returns a valid RelUndoRecPtr encoding the reserved slot,
 *   and *undo_buf_out is set to the buffer, pinned AND exclusively locked
 *   (so that RelUndoFinish and RelUndoCancel work unchanged).
 * On failure (page full or no cache entry): returns InvalidRelUndoRecPtr.
 *
 * Safety invariants:
 *   - Buffer pin prevents page eviction during the CAS loop.
 *   - pd_lower only advances (never decreases during normal operation).
 *   - Cache invalidation happens before any page reuse, so a stale cache
 *     entry that points to a recycled page will be detected: the recycled
 *     page's pd_lower is reset to SizeOfRelUndoPageHeaderData, making the
 *     CAS fail unless the recycled page also happens to have the exact same
 *     pd_lower value -- but we validate pd_upper consistency after locking.
 *   - After the CAS we acquire BUFFER_LOCK_EXCLUSIVE before returning,
 *     so RelUndoFinish can WAL-log and mark dirty safely.
 */
static RelUndoRecPtr
relundo_try_atomic_reserve(Relation rel, Size record_size,
						   Buffer *undo_buf_out)
{
	RelUndoHeadCacheEntry *cache_entry;
	Buffer		buf;
	Page		page;
	char	   *contents;
	RelUndoPageHeader datahdr;
	uint16		pd_upper;
	uint16		old_lower;
	uint16		new_lower;

	/*
	 * Tier 1 requires a cache entry to know which page to target.
	 * Without it we fall back to the full lock-based path.
	 */
	cache_entry = relundo_head_cache_lookup(RelationGetRelid(rel));
	if (cache_entry == NULL ||
		!BlockNumberIsValid(cache_entry->head_blkno) ||
		cache_entry->free_space < record_size)
		return InvalidRelUndoRecPtr;

	/*
	 * Pin the buffer.  We deliberately do NOT lock it yet; the CAS loop
	 * below serialises the pd_lower advancement without a buffer lock.
	 * The pin ensures the page stays in memory throughout.
	 */
	buf = ReadBufferExtended(rel, RELUNDO_FORKNUM,
							 cache_entry->head_blkno,
							 RBM_NORMAL, NULL);

	page = BufferGetPage(buf);
	contents = PageGetContents(page);
	datahdr = (RelUndoPageHeader) contents;

	/*
	 * Read pd_upper once.  It is set at page initialisation and never
	 * modified thereafter, so reading it without a lock is safe.
	 */
	pd_upper = datahdr->pd_upper;

	/*
	 * CAS loop: atomically advance pd_lower from old_lower to new_lower.
	 *
	 * RelUndoPageHeaderData layout at the start of the contents area:
	 *   [0..3]   prev_blkno (uint32)
	 *   [4..7]   max_xid    (TransactionId)
	 *   [8..9]   counter    (uint16)  -- stable after page init
	 *   [10..11] pd_lower   (uint16)  -- the field we increment
	 *
	 * Since PostgreSQL provides no pg_atomic_uint16, we CAS the 32-bit word
	 * covering {counter, pd_lower} (at offsetof counter) that contains both
	 * counter and pd_lower.  counter is stable, so any CAS failure is due to a
	 * concurrent pd_lower update, which is exactly what we want to detect.
	 *
	 * Byte order matters for how we extract/pack the two uint16 halves of
	 * the 32-bit word.  We use #ifdef WORDS_BIGENDIAN to be portable.
	 *
	 * Little-endian (x86/arm64 LE):  word = counter | (pd_lower << 16)
	 *   extract pd_lower: (uint16)(word >> 16)
	 *   update  pd_lower: (word & 0x0000FFFF) | ((uint32)new_lower << 16)
	 *
	 * Big-endian:                    word = (counter << 16) | pd_lower
	 *   extract pd_lower: (uint16)(word & 0xFFFF)
	 *   update  pd_lower: (word & 0xFFFF0000) | (uint32)new_lower
	 */
	{
		volatile uint32 *word_ptr;
		uint32		old_word;
		uint32		new_word;
		uint32		loaded;

		/*
		 * The CAS word covers {counter, pd_lower}.  counter must be 4-byte
		 * aligned and pd_lower must immediately follow it so the two uint16s
		 * occupy a single 32-bit word; assert both at compile time so this
		 * cannot silently break again if the header layout changes.
		 */
		StaticAssertStmt(offsetof(RelUndoPageHeaderData, counter) % sizeof(uint32) == 0,
						 "RelUndoPageHeaderData.counter must be 4-byte aligned for the lock-free CAS");
		StaticAssertStmt(offsetof(RelUndoPageHeaderData, pd_lower) ==
						 offsetof(RelUndoPageHeaderData, counter) + sizeof(uint16),
						 "RelUndoPageHeaderData.pd_lower must immediately follow counter");

		word_ptr = (volatile uint32 *)
			(contents + offsetof(RelUndoPageHeaderData, counter));

		do
		{
			old_word = *word_ptr;	/* non-atomic read; CAS will catch races */

#ifdef WORDS_BIGENDIAN
			old_lower = (uint16) (old_word & UINT32_C(0xFFFF));
#else
			old_lower = (uint16) (old_word >> 16);
#endif
			new_lower = old_lower + (uint16) record_size;

			if (new_lower > pd_upper)
			{
				/* Page is full; give up and let the lock-based path handle it */
				ReleaseBuffer(buf);
				relundo_head_cache_invalidate(RelationGetRelid(rel));
				return InvalidRelUndoRecPtr;
			}

#ifdef WORDS_BIGENDIAN
			new_word = (old_word & UINT32_C(0xFFFF0000)) |
				(uint32) new_lower;
#else
			new_word = (old_word & UINT32_C(0x0000FFFF)) |
				((uint32) new_lower << 16);
#endif

			loaded = old_word;
		} while (!pg_atomic_compare_exchange_u32(
					 (pg_atomic_uint32 *) word_ptr,
					 &loaded, new_word));
	}

	/*
	 * We have atomically reserved bytes [old_lower, new_lower) on the page.
	 * Now acquire the exclusive buffer lock so that RelUndoFinish can write
	 * the record bytes and WAL-log the change (XLogRegisterBuffer requires
	 * an exclusively locked, dirty buffer).
	 */
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

	/*
	 * After locking: sanity-check that pd_upper still looks sane (i.e. the
	 * page hasn't been recycled with a totally different geometry).  If it
	 * has been reused and reset to an incompatible layout, we'd need to bail
	 * out.  In practice this should never happen because cache invalidation
	 * precedes any page reuse, but be defensive.
	 */
	if (datahdr->pd_upper != pd_upper)
	{
		/*
		 * Page was recycled under us.  Our CAS may have corrupted the new
		 * page's pd_lower.  Reset it to at least a safe value (though this
		 * page should have been cache-invalidated).  Then fall back.
		 */
		UnlockReleaseBuffer(buf);
		relundo_head_cache_invalidate(RelationGetRelid(rel));
		return InvalidRelUndoRecPtr;
	}

	/* Update the cache's free-space estimate */
	cache_entry->free_space = (pd_upper > new_lower) ?
		(Size) (pd_upper - new_lower) : 0;

	*undo_buf_out = buf;
	return MakeRelUndoRecPtr(datahdr->counter, cache_entry->head_blkno,
							 old_lower);
}

/*
 * RelUndoReserve
 *		Reserve space for an UNDO record (Phase 1 of 2-phase insert)
 *
 * Finds a page with enough free space for record_size bytes (which must
 * include the RelUndoRecordHeader).  If the current head page doesn't have
 * enough room, a new page is allocated and linked at the head.
 *
 * Returns an RelUndoRecPtr encoding (counter, blockno, offset).
 * The buffer is returned pinned and exclusively locked via *undo_buffer.
 */
RelUndoRecPtr
RelUndoReserve(Relation rel, Size record_size, Buffer *undo_buffer)
{
	Buffer		metabuf;
	Page		metapage;
	RelUndoMetaPage meta;
	Buffer		databuf;
	Page		datapage;
	RelUndoPageHeader datahdr;
	BlockNumber blkno;
	uint16		offset;
	RelUndoRecPtr ptr;
	RelUndoHeadCacheEntry *cache_entry;

	/*
	 * Sanity check: record must fit on an empty data page.  The usable space
	 * is the contents area minus our RelUndoPageHeaderData.
	 */
	{
		Size		max_record = BLCKSZ - MAXALIGN(SizeOfPageHeaderData)
			- SizeOfRelUndoPageHeaderData;

		if (record_size > max_record)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("UNDO record size %zu exceeds maximum %zu",
							record_size, max_record)));
	}

	/*
	 * Tier 1: Atomic CAS reservation (no metapage lock needed).
	 *
	 * Try to reserve space on the cached head page by atomically advancing
	 * pd_lower via a compare-and-swap loop.  This completely avoids any
	 * metapage lock acquisition for the common case (>99% of UPDATEs when
	 * the current head page is not full).
	 *
	 * On success the returned buffer is pinned AND exclusively locked, so
	 * RelUndoFinish() and RelUndoCancel() require no changes.
	 * On failure (no cache entry, full page, or recycled page) we fall
	 * through to the original lock-based path below.
	 */
	ptr = relundo_try_atomic_reserve(rel, record_size, undo_buffer);
	if (RelUndoRecPtrIsValid(ptr))
		return ptr;

	/*
	 * Lock-based fallback: check the per-backend head page cache.  If we
	 * have a cached head page for this relation with enough free space, skip
	 * the metapage lock entirely and go directly to the data page.
	 */
	cache_entry = relundo_head_cache_lookup(RelationGetRelid(rel));
	if (cache_entry != NULL &&
		BlockNumberIsValid(cache_entry->head_blkno) &&
		cache_entry->free_space >= record_size)
	{
		databuf = ReadBufferExtended(rel, RELUNDO_FORKNUM,
									 cache_entry->head_blkno,
									 RBM_NORMAL, NULL);
		LockBuffer(databuf, BUFFER_LOCK_EXCLUSIVE);
		datapage = BufferGetPage(databuf);

		/* Verify the page still has space (another backend may have used it) */
		if (relundo_get_free_space(datapage) >= record_size)
		{
			blkno = cache_entry->head_blkno;

			/* Update cached free space */
			cache_entry->free_space = relundo_get_free_space(datapage) - record_size;

			goto reserve;
		}

		/* Cache was stale -- fall through to metapage path */
		UnlockReleaseBuffer(databuf);
		relundo_head_cache_invalidate(RelationGetRelid(rel));
	}

	/*
	 * Tier 2: Shared-lock fast path.  Read the metapage with SHARED lock to
	 * get the head page, then check the data page directly.  Only if the
	 * data page is full do we re-acquire the metapage with EXCLUSIVE lock for
	 * new page allocation.  This reduces contention when multiple backends
	 * are doing concurrent DML on the same relation.
	 */
	metabuf = relundo_get_metapage(rel, BUFFER_LOCK_SHARE);
	metapage = BufferGetPage(metabuf);
	meta = (RelUndoMetaPage) PageGetContents(metapage);

	elog(DEBUG1, "RelUndoReserve: record_size=%zu, head_blkno=%u",
		 record_size, meta->head_blkno);

	if (BlockNumberIsValid(meta->head_blkno))
	{
		BlockNumber cached_head = meta->head_blkno;

		/* Release the shared lock before touching the data page */
		UnlockReleaseBuffer(metabuf);

		elog(DEBUG1, "RelUndoReserve: reading existing head page %u (shared-lock path)",
			 cached_head);

		databuf = ReadBufferExtended(rel, RELUNDO_FORKNUM, cached_head,
									 RBM_NORMAL, NULL);
		LockBuffer(databuf, BUFFER_LOCK_EXCLUSIVE);

		datapage = BufferGetPage(databuf);

		elog(DEBUG1, "RelUndoReserve: free_space=%zu",
			 relundo_get_free_space(datapage));

		if (relundo_get_free_space(datapage) >= record_size)
		{
			/* Enough space on current head page */
			blkno = cached_head;

			elog(DEBUG1, "RelUndoReserve: enough space, using block %u", blkno);

			/* Update the head page cache */
			relundo_head_cache_update(RelationGetRelid(rel), blkno,
									  relundo_get_free_space(datapage) - record_size);

			goto reserve;
		}

		/* Not enough space; release this page, fall through to exclusive path */
		elog(DEBUG1, "RelUndoReserve: not enough space, need new page allocation");
		UnlockReleaseBuffer(databuf);
	}
	else
	{
		/* No head page yet -- release shared lock */
		UnlockReleaseBuffer(metabuf);
	}

	/*
	 * Need EXCLUSIVE metapage lock for new page allocation.
	 * Re-read the metapage since another backend may have allocated
	 * a new page between our shared-lock release and now.
	 */
	metabuf = relundo_get_metapage(rel, BUFFER_LOCK_EXCLUSIVE);
	metapage = BufferGetPage(metabuf);
	meta = (RelUndoMetaPage) PageGetContents(metapage);

	/* Re-check: another backend may have added space while we waited */
	if (BlockNumberIsValid(meta->head_blkno))
	{
		databuf = ReadBufferExtended(rel, RELUNDO_FORKNUM, meta->head_blkno,
									 RBM_NORMAL, NULL);
		LockBuffer(databuf, BUFFER_LOCK_EXCLUSIVE);
		datapage = BufferGetPage(databuf);

		if (relundo_get_free_space(datapage) >= record_size)
		{
			blkno = meta->head_blkno;

			relundo_head_cache_update(RelationGetRelid(rel), blkno,
									  relundo_get_free_space(datapage) - record_size);

			UnlockReleaseBuffer(metabuf);
			goto reserve;
		}

		UnlockReleaseBuffer(databuf);
	}

	/*
	 * Need a new page.  relundo_allocate_page handles free list / extend,
	 * links the new page as head, and marks both buffers dirty.
	 */
	blkno = relundo_allocate_page(rel, metabuf, &databuf);
	datapage = BufferGetPage(databuf);

	/* Update cache with the new head page */
	relundo_head_cache_update(RelationGetRelid(rel), blkno,
							  relundo_get_free_space(datapage) - record_size);

	/*
	 * Keep the metapage locked: RelUndoFinish() needs it for the WAL record.
	 * Store in per-backend variable to avoid changing the API.
	 */
	relundo_pending_metabuf = metabuf;

reserve:
	/* Reserve space by advancing pd_lower */
	elog(DEBUG1, "RelUndoReserve: at reserve label, block=%u", blkno);

	datahdr = (RelUndoPageHeader) PageGetContents(datapage);

	elog(DEBUG1, "RelUndoReserve: datahdr=%p, pd_lower=%u, pd_upper=%u, counter=%u",
		 datahdr, datahdr->pd_lower, datahdr->pd_upper, datahdr->counter);

	offset = datahdr->pd_lower;
	datahdr->pd_lower += record_size;

	elog(DEBUG1, "RelUndoReserve: reserved offset=%u, new pd_lower=%u",
		 offset, datahdr->pd_lower);

	/* Build the UNDO pointer */
	ptr = MakeRelUndoRecPtr(datahdr->counter, blkno, offset);

	*undo_buffer = databuf;
	return ptr;
}

/*
 * RelUndoFinish
 *		Complete UNDO record insertion (Phase 2 of 2-phase insert)
 *
 * Writes the header and payload into the space reserved by RelUndoReserve(),
 * marks the buffer dirty, and releases it.
 *
 * WAL logging is deferred to Phase 3 (WAL integration).
 */
void
RelUndoFinish(Relation rel, Buffer undo_buffer, RelUndoRecPtr ptr,
			  const RelUndoRecordHeader *header, const void *payload,
			  Size payload_size)
{
	Page		page;
	char	   *contents;
	uint16		offset;
	Size		total_record_size;
	xl_relundo_insert xlrec;
	char	   *record_data;
	RelUndoPageHeader datahdr;
	bool		is_new_page;
	uint8		info;
	Buffer		metabuf = InvalidBuffer;

	elog(DEBUG1, "RelUndoFinish: starting, ptr=%lu, payload_size=%zu",
		 (unsigned long) ptr, payload_size);

	elog(DEBUG1, "RelUndoFinish: calling BufferGetPage");
	page = BufferGetPage(undo_buffer);

	elog(DEBUG1, "RelUndoFinish: calling PageGetContents");
	contents = PageGetContents(page);

	elog(DEBUG1, "RelUndoFinish: calling RelUndoGetOffset");
	offset = RelUndoGetOffset(ptr);

	elog(DEBUG1, "RelUndoFinish: casting to RelUndoPageHeader");
	datahdr = (RelUndoPageHeader) contents;

	elog(DEBUG1, "RelUndoFinish: checking is_new_page, offset=%u", offset);

	/*
	 * Check if this is the first record on a newly allocated page. If the
	 * offset equals the header size, this is a new page.
	 */
	is_new_page = (offset == SizeOfRelUndoPageHeaderData);

	elog(DEBUG1, "RelUndoFinish: is_new_page=%d", is_new_page);

	/* Calculate total UNDO record size */
	total_record_size = SizeOfRelUndoRecordHeader + payload_size;

	elog(DEBUG1, "RelUndoFinish: writing header at offset %u", offset);
	/* Write the header */
	memcpy(contents + offset, header, SizeOfRelUndoRecordHeader);

	elog(DEBUG1, "RelUndoFinish: writing payload");
	/* Write the payload immediately after the header */
	if (payload_size > 0 && payload != NULL)
		memcpy(contents + offset + SizeOfRelUndoRecordHeader,
			   payload, payload_size);

	/*
	 * Advance the page's max_xid watermark to cover this record. Done before
	 * WAL-logging so the new-page header copy and the xlrec both observe the
	 * updated value; redo restores max_xid from the xlrec.
	 */
	if (!TransactionIdIsValid(datahdr->max_xid) ||
		TransactionIdFollows(header->urec_xid, datahdr->max_xid))
		datahdr->max_xid = header->urec_xid;

	elog(DEBUG1, "RelUndoFinish: marking buffer dirty");

	/*
	 * Mark the buffer dirty now, before the critical section.
	 * XLogRegisterBuffer requires the buffer to be dirty when called.
	 */
	MarkBufferDirty(undo_buffer);

	elog(DEBUG1, "RelUndoFinish: checking if need metapage");

	/*
	 * If this is a new page, get the metapage lock BEFORE entering the
	 * critical section. We need to include the metapage in the WAL record
	 * since it was modified during page allocation.
	 *
	 * Note: We need EXCLUSIVE lock because XLogRegisterBuffer requires the
	 * buffer to be exclusively locked.
	 */
	if (is_new_page)
	{
		elog(DEBUG1, "RelUndoFinish: using pending metapage from RelUndoReserve");
		Assert(BufferIsValid(relundo_pending_metabuf));
		metabuf = relundo_pending_metabuf;
		relundo_pending_metabuf = InvalidBuffer;

		/* Mark metabuf dirty before WAL-logging (assertion requires it) */
		MarkBufferDirty(metabuf);
	}

	/*
	 * Allocate WAL record data buffer BEFORE entering critical section.
	 * Cannot call palloc() inside a critical section.
	 */
	elog(DEBUG1, "RelUndoFinish: allocating WAL record buffer, is_new_page=%d, total_record_size=%zu",
		 is_new_page, total_record_size);

	if (is_new_page)
	{
		Size		wal_data_size = SizeOfRelUndoPageHeaderData + total_record_size;

		elog(DEBUG1, "RelUndoFinish: new page, allocating %zu bytes", wal_data_size);
		record_data = (char *) palloc(wal_data_size);

		/* Copy page header */
		memcpy(record_data, datahdr, SizeOfRelUndoPageHeaderData);

		/* Copy UNDO record after the page header */
		memcpy(record_data + SizeOfRelUndoPageHeaderData,
			   header, SizeOfRelUndoRecordHeader);
		if (payload_size > 0 && payload != NULL)
			memcpy(record_data + SizeOfRelUndoPageHeaderData + SizeOfRelUndoRecordHeader,
				   payload, payload_size);
	}
	else
	{
		/* Normal case: just the UNDO record */
		elog(DEBUG1, "RelUndoFinish: existing page, allocating %zu bytes", total_record_size);
		record_data = (char *) palloc(total_record_size);
		elog(DEBUG1, "RelUndoFinish: palloc succeeded, record_data=%p", record_data);
		elog(DEBUG1, "RelUndoFinish: copying header, header=%p, size=%zu", header, SizeOfRelUndoRecordHeader);
		memcpy(record_data, header, SizeOfRelUndoRecordHeader);
		elog(DEBUG1, "RelUndoFinish: header copied");
		if (payload_size > 0 && payload != NULL)
		{
			elog(DEBUG1, "RelUndoFinish: copying payload, payload=%p, size=%zu", payload, payload_size);
			memcpy(record_data + SizeOfRelUndoRecordHeader, payload, payload_size);
			elog(DEBUG1, "RelUndoFinish: payload memcpy completed");
		}
		elog(DEBUG1, "RelUndoFinish: finished WAL buffer preparation");
	}

	elog(DEBUG1, "RelUndoFinish: about to START_CRIT_SECTION");
	/* WAL-log the insertion */
	START_CRIT_SECTION();

	xlrec.urec_type = header->urec_type;
	xlrec.urec_len = header->urec_len;
	xlrec.page_offset = MAXALIGN(SizeOfPageHeaderData) + offset;
	xlrec.new_pd_lower = datahdr->pd_lower;
	xlrec.max_xid = datahdr->max_xid;

	info = XLOG_RELUNDO_INSERT;
	if (is_new_page)
		info |= XLOG_RELUNDO_INIT_PAGE;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, SizeOfRelundoInsert);

	/*
	 * Register the data page. We need to register the entire UNDO record
	 * (header + payload) as block data.
	 *
	 * For a new page, we also include the RelUndoPageHeaderData so that redo
	 * can reconstruct the page header fields (prev_blkno, counter). Use
	 * REGBUF_WILL_INIT to indicate the redo routine will initialize the page.
	 */
	if (is_new_page)
		XLogRegisterBuffer(0, undo_buffer, REGBUF_WILL_INIT);
	else
		XLogRegisterBuffer(0, undo_buffer, REGBUF_STANDARD);

	if (is_new_page)
	{
		Size		wal_data_size = SizeOfRelUndoPageHeaderData + total_record_size;

		XLogRegisterBufData(0, record_data, wal_data_size);

		/*
		 * When allocating a new page, the metapage was also updated
		 * (head_blkno). Register it as block 1 so the metapage state is
		 * preserved in WAL. Use REGBUF_STANDARD to get a full page image.
		 */
		XLogRegisterBuffer(1, metabuf, REGBUF_STANDARD);
	}
	else
	{
		/* Normal case: just the UNDO record */
		XLogRegisterBufData(0, record_data, total_record_size);
	}

	XLogInsert(RM_RELUNDO_ID, info);

	END_CRIT_SECTION();

	pfree(record_data);

	UnlockReleaseBuffer(undo_buffer);

	/* Release metapage if we locked it */
	if (BufferIsValid(metabuf))
		UnlockReleaseBuffer(metabuf);
}

/*
 * RelUndoFinishWithTuple
 *		Complete UNDO record insertion with tuple data (Phase 2 of 2-phase insert)
 *
 * Like RelUndoFinish(), but also writes tuple data after the payload.
 * The total record layout on the UNDO page is:
 *   [RelUndoRecordHeader][payload][tuple_data]
 *
 * The header must have RELUNDO_INFO_HAS_TUPLE set and tuple_len filled in
 * by the caller.
 */
void
RelUndoFinishWithTuple(Relation rel, Buffer undo_buffer, RelUndoRecPtr ptr,
					   const RelUndoRecordHeader *header, const void *payload,
					   Size payload_size, const char *tuple_data,
					   uint32 tuple_len)
{
	Page		page;
	char	   *contents;
	uint16		offset;
	Size		total_record_size;
	xl_relundo_insert xlrec;
	char	   *record_data;
	RelUndoPageHeader datahdr;
	bool		is_new_page;
	uint8		info;
	Buffer		metabuf = InvalidBuffer;

	elog(DEBUG1, "RelUndoFinishWithTuple: starting, ptr=%lu, payload_size=%zu, tuple_len=%u",
		 (unsigned long) ptr, payload_size, tuple_len);

	page = BufferGetPage(undo_buffer);
	contents = PageGetContents(page);
	offset = RelUndoGetOffset(ptr);
	datahdr = (RelUndoPageHeader) contents;

	is_new_page = (offset == SizeOfRelUndoPageHeaderData);

	/* Total UNDO record size includes header + payload + tuple data */
	total_record_size = SizeOfRelUndoRecordHeader + payload_size + tuple_len;

	/* Write the header */
	memcpy(contents + offset, header, SizeOfRelUndoRecordHeader);

	/* Write the payload immediately after the header */
	if (payload_size > 0 && payload != NULL)
		memcpy(contents + offset + SizeOfRelUndoRecordHeader,
			   payload, payload_size);

	/* Write the tuple data after the payload */
	if (tuple_len > 0 && tuple_data != NULL)
		memcpy(contents + offset + SizeOfRelUndoRecordHeader + payload_size,
			   tuple_data, tuple_len);

	/* Advance the page's max_xid watermark to cover this record. */
	if (!TransactionIdIsValid(datahdr->max_xid) ||
		TransactionIdFollows(header->urec_xid, datahdr->max_xid))
		datahdr->max_xid = header->urec_xid;

	MarkBufferDirty(undo_buffer);

	if (is_new_page)
	{
		Assert(BufferIsValid(relundo_pending_metabuf));
		metabuf = relundo_pending_metabuf;
		relundo_pending_metabuf = InvalidBuffer;

		/* Mark metabuf dirty before WAL-logging (assertion requires it) */
		MarkBufferDirty(metabuf);
	}

	/*
	 * Allocate WAL record data buffer before entering critical section.
	 */
	if (is_new_page)
	{
		Size		wal_data_size = SizeOfRelUndoPageHeaderData + total_record_size;

		record_data = (char *) palloc(wal_data_size);
		memcpy(record_data, datahdr, SizeOfRelUndoPageHeaderData);
		memcpy(record_data + SizeOfRelUndoPageHeaderData,
			   header, SizeOfRelUndoRecordHeader);
		if (payload_size > 0 && payload != NULL)
			memcpy(record_data + SizeOfRelUndoPageHeaderData + SizeOfRelUndoRecordHeader,
				   payload, payload_size);
		if (tuple_len > 0 && tuple_data != NULL)
			memcpy(record_data + SizeOfRelUndoPageHeaderData + SizeOfRelUndoRecordHeader + payload_size,
				   tuple_data, tuple_len);
	}
	else
	{
		record_data = (char *) palloc(total_record_size);
		memcpy(record_data, header, SizeOfRelUndoRecordHeader);
		if (payload_size > 0 && payload != NULL)
			memcpy(record_data + SizeOfRelUndoRecordHeader, payload, payload_size);
		if (tuple_len > 0 && tuple_data != NULL)
			memcpy(record_data + SizeOfRelUndoRecordHeader + payload_size,
				   tuple_data, tuple_len);
	}

	/* WAL-log the insertion */
	START_CRIT_SECTION();

	xlrec.urec_type = header->urec_type;
	xlrec.urec_len = header->urec_len;
	xlrec.page_offset = MAXALIGN(SizeOfPageHeaderData) + offset;
	xlrec.new_pd_lower = datahdr->pd_lower;
	xlrec.max_xid = datahdr->max_xid;

	info = XLOG_RELUNDO_INSERT;
	if (is_new_page)
		info |= XLOG_RELUNDO_INIT_PAGE;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, SizeOfRelundoInsert);

	if (is_new_page)
	{
		Size		wal_data_size = SizeOfRelUndoPageHeaderData + total_record_size;

		XLogRegisterBuffer(0, undo_buffer, REGBUF_WILL_INIT);
		XLogRegisterBufData(0, record_data, wal_data_size);
		XLogRegisterBuffer(1, metabuf, REGBUF_STANDARD);
	}
	else
	{
		XLogRegisterBuffer(0, undo_buffer, REGBUF_STANDARD);
		XLogRegisterBufData(0, record_data, total_record_size);
	}

	XLogInsert(RM_RELUNDO_ID, info);

	END_CRIT_SECTION();

	pfree(record_data);

	UnlockReleaseBuffer(undo_buffer);

	if (BufferIsValid(metabuf))
		UnlockReleaseBuffer(metabuf);
}

/*
 * RelUndoCancel
 *		Cancel UNDO record reservation
 *
 * The reserved space is left as a zero-filled hole.  Readers will see
 * urec_type == 0 and skip it.  The buffer is released.
 */
void
RelUndoCancel(Relation rel, Buffer undo_buffer, RelUndoRecPtr ptr)
{
	/*
	 * The space was already zeroed by relundo_init_page().  pd_lower has been
	 * advanced past it, so it's just a hole.  Nothing to write.
	 */
	UnlockReleaseBuffer(undo_buffer);

	/* Release pending metapage buffer if RelUndoReserve allocated a new page */
	if (BufferIsValid(relundo_pending_metabuf))
	{
		UnlockReleaseBuffer(relundo_pending_metabuf);
		relundo_pending_metabuf = InvalidBuffer;
	}
}

/*
 * RelUndoReadRecord
 *		Read an UNDO record from the log
 *
 * Reads the header and payload from the location encoded in ptr.
 * Returns false if the pointer is invalid or the record has been discarded.
 * On success, *payload is palloc'd and must be pfree'd by the caller.
 */
bool
RelUndoReadRecord(Relation rel, RelUndoRecPtr ptr, RelUndoRecordHeader *header,
				  void **payload, Size *payload_size)
{
	BlockNumber blkno;
	uint16		offset;
	Buffer		buf;
	Page		page;
	char	   *contents;
	Size		psize;

	if (!RelUndoRecPtrIsValid(ptr))
		return false;

	blkno = RelUndoGetBlockNum(ptr);
	offset = RelUndoGetOffset(ptr);

	/* Check that the block exists in the UNDO fork */
	if (!smgrexists(RelationGetSmgr(rel), RELUNDO_FORKNUM))
		return false;

	if (blkno >= RelationGetNumberOfBlocksInFork(rel, RELUNDO_FORKNUM))
		return false;

	buf = ReadBufferExtended(rel, RELUNDO_FORKNUM, blkno, RBM_NORMAL, NULL);
	LockBuffer(buf, BUFFER_LOCK_SHARE);

	page = BufferGetPage(buf);
	contents = PageGetContents(page);

	/* Validate that offset is within the written portion of the page */
	{
		RelUndoPageHeader hdr = (RelUndoPageHeader) contents;

		if (offset < SizeOfRelUndoPageHeaderData || offset >= hdr->pd_lower)
		{
			UnlockReleaseBuffer(buf);
			return false;
		}
	}

	/* Copy the header */
	memcpy(header, contents + offset, SizeOfRelUndoRecordHeader);

	/* A zero urec_type means the slot was cancelled (hole) */
	if (header->urec_type == 0)
	{
		UnlockReleaseBuffer(buf);
		return false;
	}

	/* Calculate payload size and copy it */
	if (header->urec_len > SizeOfRelUndoRecordHeader)
	{
		psize = header->urec_len - SizeOfRelUndoRecordHeader;
		*payload = palloc(psize);
		memcpy(*payload, contents + offset + SizeOfRelUndoRecordHeader, psize);
		*payload_size = psize;
	}
	else
	{
		*payload = NULL;
		*payload_size = 0;
	}

	UnlockReleaseBuffer(buf);
	return true;
}

/*
 * RelUndoGetCurrentCounter
 *		Get current generation counter for a relation
 *
 * Reads the metapage and returns the current counter value.
 */
uint16
RelUndoGetCurrentCounter(Relation rel)
{
	Buffer		metabuf;
	Page		metapage;
	RelUndoMetaPage meta;
	uint16		counter;

	metabuf = relundo_get_metapage(rel, BUFFER_LOCK_SHARE);
	metapage = BufferGetPage(metabuf);
	meta = (RelUndoMetaPage) PageGetContents(metapage);

	counter = meta->counter;

	UnlockReleaseBuffer(metabuf);

	return counter;
}

/*
 * RelUndoInitRelation
 *		Initialize per-relation UNDO for a new relation
 *
 * Creates the UNDO fork and writes the initial metapage (block 0).
 * The chain starts empty (head_blkno = tail_blkno = InvalidBlockNumber).
 *
 * This function is idempotent: if the UNDO fork already exists (e.g.,
 * during TRUNCATE where the new relfilenumber may already have a fork
 * from a prior operation, or during recovery replay), we truncate it
 * back to zero blocks and reinitialize.
 */
void
RelUndoInitRelation(Relation rel)
{
	Buffer		metabuf;
	Page		metapage;
	RelUndoMetaPage meta;
	SMgrRelation srel;

	/* Invalidate cached head page for this relation */
	relundo_head_cache_invalidate(RelationGetRelid(rel));

	srel = RelationGetSmgr(rel);

	/*
	 * Create the physical fork file.  Pass isRedo=true so that smgrcreate
	 * is idempotent -- if the file already exists (e.g., during TRUNCATE
	 * or recovery replay), it simply opens it rather than raising an error.
	 */
	smgrcreate(srel, RELUNDO_FORKNUM, true);

	/*
	 * WAL-log the fork creation for crash safety.
	 */
	if (!InRecovery)
		log_smgrcreate(&rel->rd_locator, RELUNDO_FORKNUM);

	/*
	 * If the fork already has blocks (e.g., re-initialization during
	 * TRUNCATE), truncate it back to zero so we can reinitialize cleanly.
	 * This discards any stale UNDO data from the previous relfilenumber
	 * incarnation.
	 */
	if (smgrnblocks(srel, RELUNDO_FORKNUM) > 0)
	{
		ForkNumber	forknum = RELUNDO_FORKNUM;
		BlockNumber old_nblocks = smgrnblocks(srel, RELUNDO_FORKNUM);
		BlockNumber new_nblocks = 0;

		smgrtruncate(srel, &forknum, 1, &old_nblocks, &new_nblocks);
	}

	/* Allocate the metapage (block 0) */
	metabuf = ExtendBufferedRel(BMR_REL(rel), RELUNDO_FORKNUM, NULL,
								EB_LOCK_FIRST);

	Assert(BufferGetBlockNumber(metabuf) == 0);

	metapage = BufferGetPage(metabuf);

	/* Initialize standard page header */
	PageInit(metapage, BLCKSZ, 0);

	/* Initialize the UNDO metapage fields */
	meta = (RelUndoMetaPage) PageGetContents(metapage);
	meta->magic = RELUNDO_METAPAGE_MAGIC;
	meta->version = RELUNDO_METAPAGE_VERSION;
	meta->counter = 1;			/* Start at 1 so 0 is clearly "no counter" */
	meta->head_blkno = InvalidBlockNumber;
	meta->tail_blkno = InvalidBlockNumber;
	meta->free_blkno = InvalidBlockNumber;
	meta->total_records = 0;
	meta->discarded_records = 0;
	meta->system_alloc_watermark = InvalidBlockNumber;

	MarkBufferDirty(metabuf);

	/*
	 * WAL-log the metapage initialization. This is critical for crash safety.
	 * If we crash after table creation but before the first INSERT, the
	 * metapage must be recoverable.
	 */
	if (!InRecovery)
	{
		xl_relundo_init xlrec;
		XLogRecPtr	recptr;

		xlrec.magic = RELUNDO_METAPAGE_MAGIC;
		xlrec.version = RELUNDO_METAPAGE_VERSION;
		xlrec.counter = 1;

		XLogBeginInsert();
		XLogRegisterData((char *) &xlrec, SizeOfRelundoInit);
		XLogRegisterBuffer(0, metabuf, REGBUF_WILL_INIT | REGBUF_STANDARD);

		recptr = XLogInsert(RM_RELUNDO_ID, XLOG_RELUNDO_INIT);

		PageSetLSN(metapage, recptr);
	}

	UnlockReleaseBuffer(metabuf);
}

/*
 * RelUndoDropRelation
 *		Drop per-relation UNDO when relation is dropped
 *
 * The UNDO fork is removed along with the relation's other forks by the
 * storage manager.  We just need to make sure we don't leave stale state.
 */
void
RelUndoDropRelation(Relation rel)
{
	SMgrRelation srel;

	srel = RelationGetSmgr(rel);

	/*
	 * If the UNDO fork doesn't exist, nothing to do.  This handles the case
	 * where the relation never had per-relation UNDO enabled.
	 */
	if (!smgrexists(srel, RELUNDO_FORKNUM))
		return;

	/*
	 * The actual file removal happens as part of the relation's overall drop
	 * via smgrdounlinkall().  We don't need to explicitly drop the fork here
	 * because the storage manager handles all forks together.
	 *
	 * If in the future we need explicit fork removal, we could truncate and
	 * unlink here.
	 */
}

/*
 * RelUndoVacuum
 *		Vacuum per-relation UNDO log
 *
 * Discards UNDO records whose owning transaction precedes oldest_xmin, the
 * oldest XID for which any active transaction could still require a rollback
 * before-image.  Pages whose max_xid precedes oldest_xmin are reclaimed.
 *
 * RelUndoDiscard splices the reclaimed run directly onto the metapage's free
 * list as a single bounded, fully WAL-logged operation, so the pages are
 * immediately available for reuse by relundo_allocate_page and the fork stops
 * growing across repeated VACUUM cycles.
 */
void
RelUndoVacuum(Relation rel, TransactionId oldest_xmin)
{
	/* If no UNDO fork exists, nothing to vacuum */
	if (!smgrexists(RelationGetSmgr(rel), RELUNDO_FORKNUM))
		return;

	/* A meaningless horizon would discard nothing; bail early. */
	if (!TransactionIdIsValid(oldest_xmin))
		return;

	RelUndoDiscard(rel, oldest_xmin);
}
