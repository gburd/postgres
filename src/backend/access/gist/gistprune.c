/*-------------------------------------------------------------------------
 *
 * gistprune.c
 *	  UNDO-informed pruning for GiST indexes
 *
 * This module implements proactive pruning of GiST index entries when the
 * UNDO discard worker determines that their referenced transactions are no
 * longer visible to any snapshot.
 *
 * ALGORITHM:
 * ----------
 * GiST indexes store IndexTuples in leaf pages with heap TIDs.
 * When notified of an UNDO discard:
 *   1. Scan all pages of the GiST index
 *   2. For leaf pages, check each tuple's heap TID
 *   3. If the heap item is LP_DEAD or LP_UNUSED, mark the index entry dead
 *   4. Set F_HAS_GARBAGE flag on modified pages for later cleanup
 *
 * CONCURRENCY:
 * -----------
 * Holds only shared locks on GiST pages and uses the hint-bit protocol
 * for marking entries dead.  This is compatible with concurrent index
 * operations.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/gist/gistprune.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/gist.h"
#include "access/gist_private.h"
#include "access/index_prune.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "utils/rel.h"

/*
 * _gist_prune_check_heap_tid
 *
 * Check whether a heap TID referenced by a GiST leaf entry is dead
 * (LP_DEAD or LP_UNUSED on the heap page).
 */
static bool
_gist_prune_check_heap_tid(Relation heaprel, ItemPointer heaptid)
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
 * gist_prune_by_undo_counter
 *
 * GiST index pruning callback for UNDO-informed index pruning.
 * Scans all leaf pages and marks dead entries whose heap tuples have
 * been discarded.
 *
 * We do a sequential scan of all relation blocks rather than tree
 * traversal, since we need to visit every leaf page anyway.  This
 * avoids the overhead of following internal page pointers.
 *
 * Returns total number of entries marked as dead.
 */
uint64
gist_prune_by_undo_counter(Relation heaprel, Relation indexrel,
						   uint16 discard_counter)
{
	BlockNumber nblocks;
	BlockNumber blkno;
	uint64		entries_pruned = 0;

	nblocks = RelationGetNumberOfBlocks(indexrel);

	/* Start at block 0 (GiST root is at GIST_ROOT_BLKNO == 0) */
	for (blkno = GIST_ROOT_BLKNO; blkno < nblocks; blkno++)
	{
		Buffer		buf;
		Page		page;
		OffsetNumber maxoff;
		OffsetNumber offnum;
		bool		marked_something = false;

		CHECK_FOR_INTERRUPTS();

		buf = ReadBuffer(indexrel, blkno);
		LockBuffer(buf, BUFFER_LOCK_SHARE);

		page = BufferGetPage(buf);

		/* Skip non-leaf pages and deleted pages */
		if (!GistPageIsLeaf(page) || GistPageIsDeleted(page) ||
			PageIsNew(page))
		{
			UnlockReleaseBuffer(buf);
			continue;
		}

		maxoff = PageGetMaxOffsetNumber(page);

		for (offnum = FirstOffsetNumber;
			 offnum <= maxoff;
			 offnum = OffsetNumberNext(offnum))
		{
			ItemId		itemid;
			IndexTuple	itup;

			itemid = PageGetItemId(page, offnum);

			if (ItemIdIsDead(itemid) || !ItemIdIsUsed(itemid))
				continue;

			if (!ItemIdIsNormal(itemid))
				continue;

			itup = (IndexTuple) PageGetItem(page, itemid);

			if (_gist_prune_check_heap_tid(heaprel, &itup->t_tid))
			{
				if (!marked_something)
				{
					if (!BufferBeginSetHintBits(buf))
						goto next_page;
				}

				ItemIdMarkDead(itemid);
				marked_something = true;
				entries_pruned++;
			}
		}

		if (marked_something)
		{
			GistMarkPageHasGarbage(page);
			BufferFinishSetHintBits(buf, true, true);
		}

next_page:
		UnlockReleaseBuffer(buf);
	}

	if (entries_pruned > 0)
	{
		elog(DEBUG2, "GiST index %s: marked " UINT64_FORMAT " entries as dead",
			 RelationGetRelationName(indexrel), entries_pruned);
	}

	return entries_pruned;
}
