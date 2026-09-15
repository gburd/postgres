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
#include "storage/procarray.h"
#include "storage/procnumber.h"
#include "storage/smgr.h"
#include "utils/timestamp.h"

/*
 * AM-neutral hook function pointers (declared in access/relundo.h).
 *
 * The owning in-place table AM installs these at subsystem init so the UNDO
 * core can clear AM transient tuple flags and reclaim retained before-images
 * without compile-time knowledge of any AM's tuple format.  They remain NULL
 * when no in-place AM is registered.
 */
void		(*RelUndoClearTransientFlags_hook) (char *tuple_data) = NULL;
void		(*RelUndoAbortCleanup_hook) (TransactionId xid) = NULL;
void		(*RelUndoDiscardRetained_hook) (void) = NULL;

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
#define RELUNDO_HEAD_CACHE_SIZE		64	/* must be a power of two */
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
 * relundo_my_slot -- pick this backend's UNDO head slot.
 *
 * Every committed append hashes to one of RELUNDO_NUM_HEADS independent head
 * pages so concurrent writers spread across distinct tail-page content locks
 * instead of serializing on one.  A backend always resolves to the same slot
 * (its ProcNumber modulo the slot count), which keeps the process-local head
 * cache coherent: a given backend only ever touches its own slot's head page,
 * so the single-entry-per-relation cache never aliases across slots.
 *
 * MyProcNumber is INVALID_PROC_NUMBER (-1) outside a normal backend (e.g. in
 * the startup process during recovery, which never calls RelUndoReserve on the
 * write path).  Fold that to slot 0 defensively so the modulo is well defined.
 */
static inline int
relundo_my_slot(void)
{
	int			pn = (int) MyProcNumber;

	if (pn < 0)
		return 0;
	return pn % RELUNDO_NUM_HEADS;
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
	 * - Match found: return the entry. - Tombstone (RELUNDO_CACHE_TOMBSTONE):
	 * continue probing; the target entry may sit beyond this deleted slot. -
	 * Empty slot (InvalidOid): the entry is definitely not present (it would
	 * have been placed before the first empty slot at insert time), so
	 * terminate early.
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
		 * chain: the relid definitely doesn't exist beyond this point. Use
		 * the earliest tombstone found so far (if any) to compact the table,
		 * or this slot if no tombstone was seen.
		 */
		if (relundo_head_cache[i].relid == InvalidOid)
		{
			if (free_slot < 0)
				free_slot = i;
			break;
		}

		/*
		 * Tombstone: record as candidate insertion point (reuses the slot to
		 * amortize tombstone accumulation) but keep probing in case relid
		 * exists beyond this slot.
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
	 * No free slot found in the probe sequence (table heavily loaded). Evict
	 * using the clock hand: advance to the next "real" entry (not empty, not
	 * tombstone) and overwrite it.  This bounds the cost to O(1) amortized
	 * and avoids evicting a tombstone (which would leave a gap that looks
	 * like "nothing past here" during lookup).
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
			return;				/* not present; tombstones don't terminate
								 * probes */
	}
}

/*
 * RelUndoHeadCacheInvalidate -- public wrapper around the per-backend head
 * page cache invalidation.
 *
 * RelUndoDiscard() (in relundo_discard.c) reclaims pages and may physically
 * truncate the fork back to the metapage.  The discarding backend's head page
 * cache can still name a block that no longer exists on disk; the next reserve
 * in that backend would then ReadBufferExtended() a truncated block and fault
 * with "could not read blocks N..N: read only 0 of 8192 bytes".  Discard must
 * therefore drop the cache entry for the relation so the next reserve re-reads
 * the metapage instead of trusting the stale block number.
 */
void
RelUndoHeadCacheInvalidate(Oid relid)
{
	relundo_head_cache_invalidate(relid);
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
	int			slot = relundo_my_slot();

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
	 * Tier 1: per-backend head page cache.  If we have a cached head page for
	 * this relation with enough free space, skip the metapage lock entirely
	 * and go directly to the data page.
	 *
	 * pd_lower is advanced (at the reserve: label below) with a plain
	 * non-atomic read-add-store, so it MUST be done under the buffer's
	 * EXCLUSIVE content lock.  An earlier "lock-free" tier advanced pd_lower
	 * via an atomic CAS while holding no content lock; that was unsound
	 * because a buffer content lock and a lock-free atomic do not mutually
	 * exclude, so the non-atomic store on this path could straddle and
	 * clobber a concurrent CAS on the same hot head page, handing two
	 * reservers overlapping offsets and corrupting the resulting UNDO
	 * records.  Every remaining reserve path advances pd_lower only under the
	 * buffer lock, so they are mutually exclusive.
	 */
	cache_entry = relundo_head_cache_lookup(RelationGetRelid(rel));
	if (cache_entry != NULL &&
		BlockNumberIsValid(cache_entry->head_blkno) &&
		cache_entry->free_space >= record_size &&
		cache_entry->head_blkno <
		smgrnblocks(RelationGetSmgr(rel), RELUNDO_FORKNUM))
	{
		/*
		 * The head cache is process-local, but a concurrent autovacuum worker
		 * (holding only ShareUpdateExclusiveLock, which does not exclude our
		 * RowExclusiveLock) can whole-chain-discard and physically truncate
		 * this fork.  smgrtruncate broadcasts a smgr invalidation but does
		 * NOT touch any other backend's relundo_head_cache, so our cached
		 * head_blkno may now point past the shrunken EOF.  Reading it would
		 * either fault on a nonexistent block or return a recycled/garbage
		 * page.  The smgrnblocks bound in the guard above rejects that stale
		 * entry, and we fall through to the metapage path, which re-reads the
		 * authoritative head_blkno under lock.
		 */
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
	 * get the head page, then check the data page directly.  Only if the data
	 * page is full do we re-acquire the metapage with EXCLUSIVE lock for new
	 * page allocation.  This reduces contention when multiple backends are
	 * doing concurrent DML on the same relation.
	 */
	metabuf = relundo_get_metapage(rel, BUFFER_LOCK_SHARE);
	metapage = BufferGetPage(metabuf);
	meta = (RelUndoMetaPage) PageGetContents(metapage);

	elog(DEBUG1, "RelUndoReserve: record_size=%zu, slot=%d, head_blkno=%u",
		 record_size, slot, meta->head_blkno[slot]);

	if (BlockNumberIsValid(meta->head_blkno[slot]))
	{
		BlockNumber cached_head = meta->head_blkno[slot];

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
	 * Need EXCLUSIVE metapage lock for new page allocation. Re-read the
	 * metapage since another backend may have allocated a new page between
	 * our shared-lock release and now.
	 */
	metabuf = relundo_get_metapage(rel, BUFFER_LOCK_EXCLUSIVE);
	metapage = BufferGetPage(metabuf);
	meta = (RelUndoMetaPage) PageGetContents(metapage);

	/* Re-check: another backend may have added space while we waited */
	if (BlockNumberIsValid(meta->head_blkno[slot]))
	{
		databuf = ReadBufferExtended(rel, RELUNDO_FORKNUM, meta->head_blkno[slot],
									 RBM_NORMAL, NULL);
		LockBuffer(databuf, BUFFER_LOCK_EXCLUSIVE);
		datapage = BufferGetPage(databuf);

		if (relundo_get_free_space(datapage) >= record_size)
		{
			blkno = meta->head_blkno[slot];

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
	blkno = relundo_allocate_page(rel, metabuf, slot, &databuf);
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
 * RelUndoStage
 *		Write an UNDO record onto its reserved page WITHOUT WAL logging.
 *
 * Performs every page mutation RelUndoFinish() does (header+payload memcpy,
 * max_xid bump, MarkBufferDirty on the data page and, for a new page, the
 * metapage) and builds the block-0 WAL data buffer, but does NOT open a
 * critical section, XLogInsert, PageSetLSN, or release any buffer.  The undo
 * buffer (and metapage, if a new page was allocated) stay locked+pinned.
 *
 * The staged facts are returned in *result so the caller can either emit the
 * standalone RM_RELUNDO_ID record (RelUndoFinish wrapper) or fold the same
 * bytes into a combined record under a different resource manager (the
 * caller's WAL-fold path).
 */
void
RelUndoStage(Relation rel, Buffer undo_buffer, RelUndoRecPtr ptr,
			 const RelUndoRecordHeader *header, const void *payload,
			 Size payload_size, RelUndoStageResult *result)
{
	Page		page;
	char	   *contents;
	uint16		offset;
	Size		total_record_size;
	char	   *record_data;
	RelUndoPageHeader datahdr;
	bool		is_new_page;
	Buffer		metabuf = InvalidBuffer;

	page = BufferGetPage(undo_buffer);
	contents = PageGetContents(page);
	offset = RelUndoGetOffset(ptr);
	datahdr = (RelUndoPageHeader) contents;

	/*
	 * Check if this is the first record on a newly allocated page. If the
	 * offset equals the header size, this is a new page.
	 */
	is_new_page = (offset == SizeOfRelUndoPageHeaderData);

	/* Calculate total UNDO record size */
	total_record_size = SizeOfRelUndoRecordHeader + payload_size;

	/* Write the header */
	memcpy(contents + offset, header, SizeOfRelUndoRecordHeader);

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

	/*
	 * Mark the buffer dirty now, before any critical section.
	 * XLogRegisterBuffer requires the buffer to be dirty when called.
	 */
	MarkBufferDirty(undo_buffer);

	/*
	 * If this is a new page, adopt the metapage lock that RelUndoReserve left
	 * pending.  It was modified during page allocation and must be included
	 * in whichever WAL record the caller emits.
	 */
	if (is_new_page)
	{
		Assert(BufferIsValid(relundo_pending_metabuf));
		metabuf = relundo_pending_metabuf;
		relundo_pending_metabuf = InvalidBuffer;

		/* Mark metabuf dirty before WAL-logging (assertion requires it) */
		MarkBufferDirty(metabuf);
	}

	/*
	 * Build the block-0 WAL data buffer.  For a new page we prepend the
	 * RelUndoPageHeaderData so redo can reconstruct prev_blkno/counter.
	 */
	if (is_new_page)
	{
		Size		wal_data_size = SizeOfRelUndoPageHeaderData + total_record_size;

		record_data = (char *) palloc(wal_data_size);

		/* Copy page header */
		memcpy(record_data, datahdr, SizeOfRelUndoPageHeaderData);

		/* Copy UNDO record after the page header */
		memcpy(record_data + SizeOfRelUndoPageHeaderData,
			   header, SizeOfRelUndoRecordHeader);
		if (payload_size > 0 && payload != NULL)
			memcpy(record_data + SizeOfRelUndoPageHeaderData + SizeOfRelUndoRecordHeader,
				   payload, payload_size);

		result->wal_record_size = wal_data_size;
	}
	else
	{
		/* Normal case: just the UNDO record */
		record_data = (char *) palloc(total_record_size);
		memcpy(record_data, header, SizeOfRelUndoRecordHeader);
		if (payload_size > 0 && payload != NULL)
			memcpy(record_data + SizeOfRelUndoRecordHeader, payload, payload_size);

		result->wal_record_size = total_record_size;
	}

	result->undo_buffer = undo_buffer;
	result->metabuf = metabuf;
	result->is_new_page = is_new_page;
	result->urec_type = header->urec_type;
	result->urec_len = header->urec_len;
	result->page_offset = MAXALIGN(SizeOfPageHeaderData) + offset;
	result->new_pd_lower = datahdr->pd_lower;
	result->max_xid = datahdr->max_xid;
	result->wal_record_data = record_data;
}

/*
 * RelUndoFinish
 *		Complete UNDO record insertion (Phase 2 of 2-phase insert)
 *
 * Writes the header and payload into the space reserved by RelUndoReserve(),
 * WAL-logs the insertion as a standalone RM_RELUNDO_ID record, and releases the
 * buffer(s).  Built on RelUndoStage(): stage the page mutation, then emit the
 * record and stamp the LSN.
 */
void
RelUndoFinish(Relation rel, Buffer undo_buffer, RelUndoRecPtr ptr,
			  const RelUndoRecordHeader *header, const void *payload,
			  Size payload_size)
{
	RelUndoStageResult staged;
	Page		page;
	uint8		info;

	RelUndoStage(rel, undo_buffer, ptr, header, payload, payload_size, &staged);

	page = BufferGetPage(undo_buffer);

	/* WAL-log the insertion */
	START_CRIT_SECTION();

	{
		xl_relundo_insert xlrec;

		xlrec.urec_type = staged.urec_type;
		xlrec.urec_len = staged.urec_len;
		xlrec.page_offset = staged.page_offset;
		xlrec.new_pd_lower = staged.new_pd_lower;
		xlrec.max_xid = staged.max_xid;

		info = XLOG_RELUNDO_INSERT;
		if (staged.is_new_page)
			info |= XLOG_RELUNDO_INIT_PAGE;

		XLogBeginInsert();
		XLogRegisterData((char *) &xlrec, SizeOfRelundoInsert);

		/*
		 * Register the data page.  We register the entire UNDO record (header
		 * + payload) as block data.
		 *
		 * For a new page, the block data also carries the
		 * RelUndoPageHeaderData so redo can reconstruct prev_blkno/counter;
		 * REGBUF_WILL_INIT tells redo it will initialize the page.
		 *
		 * For an existing page, do NOT pass REGBUF_STANDARD.  RelUndo data
		 * pages keep the standard PageHeader.pd_lower pinned at the empty
		 * value and track their real used extent in the shadow
		 * RelUndoPageHeader inside the page contents area.  REGBUF_STANDARD
		 * would treat [pd_lower, pd_upper) as a free "hole" and elide the
		 * entire contents from any full-page image, so a BLK_RESTORED redo
		 * would bring the page back zeroed -- losing the record bytes and the
		 * prev_blkno chain link. Logging the whole page (flag 0) keeps the
		 * FPI faithful.
		 */
		if (staged.is_new_page)
			XLogRegisterBuffer(0, undo_buffer, REGBUF_WILL_INIT);
		else
			XLogRegisterBuffer(0, undo_buffer, 0);

		XLogRegisterBufData(0, staged.wal_record_data, staged.wal_record_size);

		/*
		 * When allocating a new page, the metapage was also updated
		 * (head_blkno). Register it as block 1 so the metapage state is
		 * preserved in WAL.  Use REGBUF_STANDARD to get a full page image.
		 */
		if (staged.is_new_page)
			XLogRegisterBuffer(1, staged.metabuf, REGBUF_STANDARD);

		{
			XLogRecPtr	recptr = XLogInsert(RM_RELUNDO_ID, info);

			/*
			 * Stamp the record LSN onto every page we dirtied and registered.
			 * Without this the buffer manager's WAL-before-data rule is
			 * broken: the checkpointer flushes WAL only up to a dirty
			 * buffer's page LSN before writing it, so a stale/zero LSN lets
			 * an UNDO page (including its prev_blkno chain links) reach disk
			 * ahead of the WAL that describes it, corrupting the chain on
			 * crash recovery.
			 */
			PageSetLSN(page, recptr);
			if (staged.is_new_page)
				PageSetLSN(BufferGetPage(staged.metabuf), recptr);
		}
	}

	END_CRIT_SECTION();

	pfree(staged.wal_record_data);

	UnlockReleaseBuffer(undo_buffer);

	/* Release metapage if we locked it */
	if (BufferIsValid(staged.metabuf))
		UnlockReleaseBuffer(staged.metabuf);
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
		/* Full page image, no hole: see the REGBUF note in RelUndoFinish. */
		XLogRegisterBuffer(0, undo_buffer, 0);
		XLogRegisterBufData(0, record_data, total_record_size);
	}

	{
		XLogRecPtr	recptr = XLogInsert(RM_RELUNDO_ID, info);

		/*
		 * Stamp the record LSN onto every dirtied+registered page; see the
		 * WAL-before-data note in RelUndoFinish.
		 */
		PageSetLSN(page, recptr);
		if (is_new_page)
			PageSetLSN(BufferGetPage(metabuf), recptr);
	}

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
 * relundo_fork_nblocks_fast
 *
 * Return the number of blocks in the UNDO fork with no per-call filesystem
 * syscalls on the hot path.
 *
 * The old guard was smgrexists() + smgrnblocks() per call.  smgrexists() ->
 * mdexists() unconditionally mdclose()s the fork fd (to notice an unlink) and
 * the next access reopens it; on the multiversion read path (per scanned
 * tuple carrying a retained before-image) that FD open/close storm dominated
 * CPU at high core count (dentry lockref contention).  smgrnblocks_cached()
 * does NOT help outside recovery -- it only returns a cached value when
 * InRecovery -- so the previous version still hit smgrexists() every call.
 *
 * smgrnblocks() itself is cheap after the first call: it opens the fork once
 * (mdopenfork) and leaves the fd cached in the SMgrRelation; it never closes
 * it.  So the fix is to drop smgrexists() from the hot path entirely and rely
 * on smgrnblocks().  A valid RelUndoRecPtr can only have been produced by a
 * writer that extended the fork, so the fork provably exists whenever a
 * caller has a valid pointer into it; smgrnblocks() therefore never faults
 * here.  For extra safety against a fake/partial relcache entry whose fork is
 * genuinely absent, we probe smgrexists() exactly ONCE per SMgrRelation and
 * latch the positive result (a stat that then keeps the fd via the
 * subsequent smgrnblocks open); if the fork is absent we return 0 and never
 * cache, so a later-created fork is still picked up.
 */
static inline BlockNumber
relundo_fork_nblocks_fast(Relation rel)
{
	SMgrRelation smgr = RelationGetSmgr(rel);

	/*
	 * If the fork's fd is already open in this SMgrRelation, it exists and is
	 * open -- go straight to smgrnblocks (no stat, no close).
	 * md_num_open_segs is >0 once mdopenfork has run for this fork.
	 */
	if (smgr->md_num_open_segs[RELUNDO_FORKNUM] > 0)
		return smgrnblocks(smgr, RELUNDO_FORKNUM);

	/* Cold: confirm the fork exists once, then open+size it (fd stays open). */
	if (!smgrexists(smgr, RELUNDO_FORKNUM))
		return 0;
	return smgrnblocks(smgr, RELUNDO_FORKNUM);
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

	/*
	 * Bounds-check the block against the UNDO fork size using the cached
	 * nblocks (no per-call filesystem syscalls; see
	 * relundo_fork_nblocks_fast).  A zero result means the fork does not
	 * exist -- treat as out of range.  The UNDO fork is always a standard
	 * BLCKSZ-paged smgr fork.
	 */
	if (blkno >= relundo_fork_nblocks_fast(rel))
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

		/*
		 * ABA defense: reject a verptr whose embedded generation counter does
		 * not match the page's current counter.  A free-list recycle bumps
		 * meta->counter (relundo_page.c) before re-initialising the page, so
		 * a stale verptr to a recycled blkno will see hdr->counter != its own
		 * counter and fail here.  Returning false is the chain-end signal:
		 * the caller's version-reconstruction walk treats it as a best-effort
		 * terminator, which is the correct, safe answer (never silently
		 * reverse-apply a structurally valid but unrelated record).
		 */
		if (RelUndoGetCounter(ptr) != hdr->counter)
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
 * RelUndoReadRecordHeader
 *		Read only the header of an UNDO record.
 *
 * Same discard/ABA/hole semantics as RelUndoReadRecord but skips the
 * payload palloc+memcpy.  Used by hot probes (e.g. the lost-update
 * conflict probe) that need only urec_xid.
 */
bool
RelUndoReadRecordHeader(Relation rel, RelUndoRecPtr ptr,
						RelUndoRecordHeader *header)
{
	BlockNumber blkno;
	uint16		offset;
	Buffer		buf;
	Page		page;
	char	   *contents;

	if (!RelUndoRecPtrIsValid(ptr))
		return false;

	blkno = RelUndoGetBlockNum(ptr);
	offset = RelUndoGetOffset(ptr);

	/* Cached bounds check; see relundo_fork_nblocks_fast. */
	if (blkno >= relundo_fork_nblocks_fast(rel))
		return false;

	buf = ReadBufferExtended(rel, RELUNDO_FORKNUM, blkno, RBM_NORMAL, NULL);
	LockBuffer(buf, BUFFER_LOCK_SHARE);

	page = BufferGetPage(buf);
	contents = PageGetContents(page);

	{
		RelUndoPageHeader hdr = (RelUndoPageHeader) contents;

		if (offset < SizeOfRelUndoPageHeaderData || offset >= hdr->pd_lower)
		{
			UnlockReleaseBuffer(buf);
			return false;
		}

		if (RelUndoGetCounter(ptr) != hdr->counter)
		{
			UnlockReleaseBuffer(buf);
			return false;
		}
	}

	memcpy(header, contents + offset, SizeOfRelUndoRecordHeader);

	if (header->urec_type == 0)
	{
		UnlockReleaseBuffer(buf);
		return false;
	}

	UnlockReleaseBuffer(buf);
	return true;
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
	 * Create the physical fork file.  Pass isRedo=true so that smgrcreate is
	 * idempotent -- if the file already exists (e.g., during TRUNCATE or
	 * recovery replay), it simply opens it rather than raising an error.
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
	for (int s = 0; s < RELUNDO_NUM_HEADS; s++)
	{
		meta->head_blkno[s] = InvalidBlockNumber;
		meta->tail_blkno[s] = InvalidBlockNumber;
	}
	meta->free_blkno = InvalidBlockNumber;
	meta->total_records = 0;
	meta->discarded_records = 0;
	meta->system_alloc_watermark = InvalidBlockNumber;

	/* Include the meta struct in the recorded region of any FPI. */
	RelUndoMetaPageSetPdLower(metapage);

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
RelUndoVacuum(Relation rel, TransactionId oldest_xmin, bool nowait)
{
	/* If no UNDO fork exists, nothing to vacuum */
	if (!smgrexists(RelationGetSmgr(rel), RELUNDO_FORKNUM))
		return;

	/* A meaningless horizon would discard nothing; bail early. */
	if (!TransactionIdIsValid(oldest_xmin))
		return;

	RelUndoDiscard(rel, oldest_xmin, nowait);
}

/*
 * RELUNDO_MAYBE_VACUUM_MIN_BLOCKS
 *		Skip the throttled discard sweep below this fork size (in blocks).
 *		Chosen to make the common case (a quiescent or lightly-written
 *		table) resolve to a single relundo_fork_nblocks_fast() call with no
 *		buffer I/O, while still catching sustained-churn growth well before
 *		it reaches problematic size.
 */
#define RELUNDO_MAYBE_VACUUM_MIN_BLOCKS	64

/*
 * RelUndoMaybeVacuum
 *		Throttled, self-clocking per-relation UNDO fork discard.
 *
 * RelUndoVacuum() (the function above) is normally invoked once per VACUUM,
 * via the owning table AM's relation_vacuum callback.  But an AM whose
 * updates are in-place can correctly report near-zero dead tuples,
 * so autovacuum's dead-tuple/insert-count thresholds may never fire even
 * under sustained write churn -- and RelUndoVacuum is the ONLY code that
 * discards the on-disk UNDO fork, so an AM that never gets vacuumed would
 * grow its UNDO fork without bound.
 *
 * This is a cheap, throttled backstop an AM's DML path can call so fork
 * discard runs on its own schedule, decoupled from VACUUM ever being
 * triggered.  Intended to be called from the AM's DML hot path after
 * releasing all buffer/tuple locks -- RelUndoDiscard() takes the UNDO fork's
 * own metapage lock, which must never be acquired while holding a data page
 * lock (that would convoy every writer to a hot page behind the discard
 * sweep).
 *
 * The cadence uses a per-backend static timestamp: each backend independently
 * throttles to once every 5 seconds, so concurrent backends do redundant but
 * bounded work rather than needing shared coordination.
 */
void
RelUndoMaybeVacuum(Relation rel)
{
	static TimestampTz relundo_last_vacuum = 0;
	TimestampTz now_ts;
	BlockNumber nblocks;

	now_ts = GetCurrentTimestamp();
	if (now_ts - relundo_last_vacuum < 5000000) /* 5 seconds */
		return;

	nblocks = relundo_fork_nblocks_fast(rel);
	if (nblocks < RELUNDO_MAYBE_VACUUM_MIN_BLOCKS)
		return;

	relundo_last_vacuum = now_ts;

	/*
	 * Hot-path discard MUST NOT block: this runs inline from the AM's update
	 * path for every qualifying update.  Blocking on the per-relation UNDO
	 * metapage EXCLUSIVE lock here serializes ALL concurrent updates to the
	 * relation behind one lock (measured: the dominant cause of a TPROC-C
	 * throughput collapse and hours-long lock-wait pileups on hot tables like
	 * TPC-C district).  Pass nowait=true so a contended metapage is simply
	 * skipped -- some other backend (or a later call) reclaims the space;
	 * UNDO discard is space reclamation, never per-update correctness.
	 */
	RelUndoVacuum(rel, GetOldestNonRemovableTransactionId(rel), true);
}
