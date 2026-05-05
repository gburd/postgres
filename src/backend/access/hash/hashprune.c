/*-------------------------------------------------------------------------
 *
 * hashprune.c
 *	  UNDO-informed pruning for Hash indexes
 *
 * This module implements proactive pruning of hash index entries when the
 * UNDO discard worker determines that their referenced transactions are no
 * longer visible to any snapshot.
 *
 * ALGORITHM:
 * ----------
 * Hash indexes store tuples in bucket pages and their overflow pages.
 * When notified of an UNDO discard:
 *   1. Scan all pages of the hash index sequentially
 *   2. For bucket and overflow pages, scan all tuples
 *   3. Check each tuple's heap TID against the heap page
 *   4. If the heap item is LP_DEAD or LP_UNUSED, mark the index entry dead
 *   5. Use hint-bit protocol for lightweight concurrent marking
 *
 * CONCURRENCY:
 * -----------
 * Holds only shared locks on hash pages and uses the hint-bit protocol
 * for marking entries dead.  This avoids exclusive locks and is compatible
 * with concurrent index operations.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/hash/hashprune.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/hash.h"
#include "access/index_prune.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "utils/rel.h"

/*
 * _hash_prune_check_heap_tid
 *
 * Check whether a heap TID referenced by a hash index entry is dead
 * (LP_DEAD or LP_UNUSED on the heap page).
 */
static bool
_hash_prune_check_heap_tid(Relation heaprel, ItemPointer heaptid)
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
 * hash_prune_by_undo_counter
 *
 * Hash index pruning callback for UNDO-informed index pruning.
 * Scans all bucket and overflow pages, marking dead entries whose heap
 * tuples have been discarded.
 *
 * We scan all pages sequentially rather than traversing bucket chains,
 * since we need to visit every bucket and overflow page anyway and
 * sequential I/O is more efficient.
 *
 * Returns total number of entries marked as dead.
 */
uint64
hash_prune_by_undo_counter(Relation heaprel, Relation indexrel,
						   uint16 discard_counter)
{
	BlockNumber nblocks;
	BlockNumber blkno;
	uint64		entries_pruned = 0;

	nblocks = RelationGetNumberOfBlocks(indexrel);

	/*
	 * Scan all pages.  We skip the metapage (block 0) and bitmap pages,
	 * and only process bucket pages and overflow pages.
	 */
	for (blkno = 1; blkno < nblocks; blkno++)
	{
		Buffer		buf;
		Page		page;
		HashPageOpaque opaque;
		OffsetNumber maxoff;
		OffsetNumber offnum;
		bool		marked_something = false;

		CHECK_FOR_INTERRUPTS();

		buf = ReadBuffer(indexrel, blkno);
		LockBuffer(buf, BUFFER_LOCK_SHARE);

		page = BufferGetPage(buf);

		if (PageIsNew(page) || PageGetSpecialSize(page) != MAXALIGN(sizeof(HashPageOpaqueData)))
		{
			UnlockReleaseBuffer(buf);
			continue;
		}

		opaque = HashPageGetOpaque(page);

		/* Only process bucket pages and overflow pages */
		if ((opaque->hasho_flag & LH_PAGE_TYPE) != LH_BUCKET_PAGE &&
			(opaque->hasho_flag & LH_PAGE_TYPE) != LH_OVERFLOW_PAGE)
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

			if (_hash_prune_check_heap_tid(heaprel, &itup->t_tid))
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
			BufferFinishSetHintBits(buf, true, true);

next_page:
		UnlockReleaseBuffer(buf);
	}

	if (entries_pruned > 0)
	{
		elog(DEBUG2, "hash index %s: marked " UINT64_FORMAT " entries as dead",
			 RelationGetRelationName(indexrel), entries_pruned);
	}

	return entries_pruned;
}
