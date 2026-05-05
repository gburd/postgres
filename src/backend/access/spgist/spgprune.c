/*-------------------------------------------------------------------------
 *
 * spgprune.c
 *	  UNDO-informed pruning for SP-GiST indexes
 *
 * This module implements proactive pruning of SP-GiST index entries when
 * the UNDO discard worker determines that their referenced transactions
 * are no longer visible to any snapshot.
 *
 * SP-GiST INDEX STRUCTURE:
 * -----------------------
 * SP-GiST indexes use space partitioning with inner and leaf tuples.
 * Leaf tuples contain heap TIDs (heapPtr) and can be in one of four
 * states: LIVE, REDIRECT, DEAD, or PLACEHOLDER.
 *
 * ALGORITHM:
 * ----------
 * When notified of an UNDO discard:
 *   1. Scan all pages of the SP-GiST index
 *   2. For leaf pages, iterate through all line pointers
 *   3. For LIVE leaf tuples, check if the referenced heap TID is dead
 *   4. If the heap item is dead, mark the leaf tuple as DEAD
 *
 * We cannot use the hint-bit protocol here because SP-GiST dead tuple
 * marking involves changing the tupstate field, not just line pointer
 * flags.  Instead, we upgrade to an exclusive lock when modifications
 * are needed.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/spgist/spgprune.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/spgist_private.h"
#include "access/index_prune.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "utils/rel.h"

/*
 * _spg_prune_check_heap_tid
 *
 * Check whether a heap TID is dead on the heap page.
 */
static bool
_spg_prune_check_heap_tid(Relation heaprel, ItemPointer heaptid)
{
	Buffer		heapbuf;
	Page		heappage;
	ItemId		heapitemid;
	OffsetNumber offnum;
	bool		is_dead;

	offnum = ItemPointerGetOffsetNumber(heaptid);

	heapbuf = ReadBuffer(heaprel, ItemPointerGetBlockNumber(heaptid));
	LockBuffer(heapbuf, BUFFER_LOCK_SHARE);

	heappage = BufferGetPage(heapbuf);

	if (offnum > PageGetMaxOffsetNumber(heappage) || offnum < FirstOffsetNumber)
	{
		UnlockReleaseBuffer(heapbuf);
		return true;
	}

	heapitemid = PageGetItemId(heappage, offnum);
	is_dead = (ItemIdIsDead(heapitemid) || !ItemIdIsUsed(heapitemid));

	UnlockReleaseBuffer(heapbuf);

	return is_dead;
}

/*
 * _spg_prune_scan_leaf_page
 *
 * Scan a single SP-GiST leaf page and collect offsets of LIVE leaf tuples
 * whose heap TIDs are dead.  We collect them first (while holding a shared
 * lock), then if any are found, upgrade to exclusive and mark them DEAD.
 *
 * Returns the number of tuples marked as dead.
 */
static uint64
_spg_prune_scan_leaf_page(Relation heaprel, Relation indexrel,
						  Buffer buf)
{
	Page		page;
	OffsetNumber maxoff;
	OffsetNumber offnum;
	OffsetNumber dead_offsets[MaxIndexTuplesPerPage];
	int			ndead = 0;
	uint64		entries_pruned = 0;

	page = BufferGetPage(buf);
	maxoff = PageGetMaxOffsetNumber(page);

	/*
	 * First pass (shared lock): identify LIVE leaf tuples with dead heap
	 * TIDs.
	 */
	for (offnum = FirstOffsetNumber;
		 offnum <= maxoff;
		 offnum = OffsetNumberNext(offnum))
	{
		ItemId		itemid;
		SpGistLeafTuple leafTuple;

		itemid = PageGetItemId(page, offnum);

		if (!ItemIdIsUsed(itemid) || ItemIdIsDead(itemid))
			continue;

		if (!ItemIdIsNormal(itemid))
			continue;

		leafTuple = (SpGistLeafTuple) PageGetItem(page, itemid);

		/* Only check LIVE leaf tuples */
		if (leafTuple->tupstate != SPGIST_LIVE)
			continue;

		/* Check if the referenced heap tuple is dead */
		if (_spg_prune_check_heap_tid(heaprel, &leafTuple->heapPtr))
		{
			if (ndead < MaxIndexTuplesPerPage)
				dead_offsets[ndead++] = offnum;
		}
	}

	if (ndead == 0)
		return 0;

	/*
	 * Second pass: upgrade to exclusive lock and mark dead tuples.
	 *
	 * We need to re-verify each tuple after upgrading the lock, since
	 * the page could have been modified between releasing the shared
	 * lock and acquiring the exclusive lock.
	 */
	LockBuffer(buf, BUFFER_LOCK_UNLOCK);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

	/* Re-read the page after lock upgrade */
	page = BufferGetPage(buf);
	maxoff = PageGetMaxOffsetNumber(page);

	for (int i = 0; i < ndead; i++)
	{
		ItemId		itemid;
		SpGistLeafTuple leafTuple;

		offnum = dead_offsets[i];

		/* Re-validate the offset is still in range */
		if (offnum > maxoff)
			continue;

		itemid = PageGetItemId(page, offnum);

		if (!ItemIdIsUsed(itemid) || !ItemIdIsNormal(itemid))
			continue;

		leafTuple = (SpGistLeafTuple) PageGetItem(page, itemid);

		/* Re-verify it's still a LIVE leaf tuple */
		if (leafTuple->tupstate != SPGIST_LIVE)
			continue;

		/*
		 * Re-check the heap TID since the page may have changed.
		 * This is the conservative approach.
		 */
		if (_spg_prune_check_heap_tid(heaprel, &leafTuple->heapPtr))
		{
			leafTuple->tupstate = SPGIST_DEAD;
			entries_pruned++;
		}
	}

	if (entries_pruned > 0)
	{
		MarkBufferDirty(buf);

		/*
		 * Increment the placeholder count to allow future space
		 * reclamation by SP-GiST vacuum.
		 */
		SpGistPageGetOpaque(page)->nPlaceholder += entries_pruned;
	}

	/* Downgrade back to shared lock before returning */
	LockBuffer(buf, BUFFER_LOCK_UNLOCK);
	LockBuffer(buf, BUFFER_LOCK_SHARE);

	return entries_pruned;
}

/*
 * spg_prune_by_undo_counter
 *
 * SP-GiST index pruning callback for UNDO-informed index pruning.
 * Scans all leaf pages and marks dead entries whose heap tuples have
 * been discarded.
 *
 * Returns total number of entries marked as dead.
 */
uint64
spg_prune_by_undo_counter(Relation heaprel, Relation indexrel,
						  uint16 discard_counter)
{
	BlockNumber nblocks;
	BlockNumber blkno;
	uint64		entries_pruned = 0;

	nblocks = RelationGetNumberOfBlocks(indexrel);

	for (blkno = SPGIST_ROOT_BLKNO; blkno < nblocks; blkno++)
	{
		Buffer		buf;
		Page		page;

		CHECK_FOR_INTERRUPTS();

		buf = ReadBuffer(indexrel, blkno);
		LockBuffer(buf, BUFFER_LOCK_SHARE);

		page = BufferGetPage(buf);

		/* Only process leaf pages */
		if (PageIsNew(page) || SpGistPageIsDeleted(page) ||
			!SpGistPageIsLeaf(page))
		{
			UnlockReleaseBuffer(buf);
			continue;
		}

		entries_pruned += _spg_prune_scan_leaf_page(heaprel, indexrel, buf);

		UnlockReleaseBuffer(buf);
	}

	if (entries_pruned > 0)
	{
		elog(DEBUG2, "SP-GiST index %s: marked " UINT64_FORMAT " entries as dead",
			 RelationGetRelationName(indexrel), entries_pruned);
	}

	return entries_pruned;
}
