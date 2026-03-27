/*-------------------------------------------------------------------------
 *
 * ginprune.c
 *	  UNDO-informed pruning for GIN indexes
 *
 * This module implements proactive pruning of GIN index entries when the
 * UNDO discard worker determines that their referenced transactions are no
 * longer visible to any snapshot.
 *
 * GIN INDEX STRUCTURE:
 * -------------------
 * GIN indexes have a two-level structure:
 *   - Entry tree: B-tree of key values, where each entry has a posting
 *     list (inline) or posting tree (separate pages) of heap TIDs
 *   - Posting trees: Separate B-trees of compressed heap TID segments
 *
 * IMPLEMENTATION STATUS:
 * ---------------------
 * GIN pruning is not yet fully implemented due to the complexity of
 * modifying compressed posting lists.  Removing TIDs from a compressed
 * posting list requires:
 *   1. Decoding the compressed segment
 *   2. Removing dead TIDs
 *   3. Re-encoding and potentially resizing the segment
 *   4. Handling the case where a posting list becomes a posting tree
 *      or vice versa
 *
 * The existing GIN vacuum infrastructure (ginvacuum.c) already handles
 * this correctly.  A full UNDO-informed pruning implementation should
 * leverage that infrastructure rather than reimplementing it.
 *
 * For now, this callback performs a lightweight scan of entry tree leaf
 * pages.  If all TIDs in an entry's posting list are dead, the entry
 * itself can potentially be marked for removal.  This provides a
 * partial benefit without the complexity of modifying posting lists.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/gin/ginprune.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/gin_private.h"
#include "access/ginblock.h"
#include "access/index_prune.h"
#include "access/relundo.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "utils/rel.h"

/*
 * _gin_prune_check_heap_tid
 *
 * Check whether a heap TID is dead on the heap page.
 */
static bool
_gin_prune_check_heap_tid(Relation heaprel, ItemPointer heaptid)
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
 * _gin_prune_posting_tree_leaf
 *
 * Scan a single posting tree leaf page and count dead TIDs.
 * Returns the number of dead TIDs found.
 *
 * Note: We do not modify the posting tree pages here.  Removing TIDs from
 * compressed posting lists is complex (decode, filter, re-encode) and is
 * better left to the full VACUUM infrastructure in ginvacuum.c.
 * Instead, we count dead entries to report pruning potential.
 */
static uint64
_gin_prune_scan_posting_tree_leaf(Relation heaprel, Page page)
{
	int			nitems;
	ItemPointer items;
	int			i;
	uint64		dead_count = 0;
	ItemPointerData advancePast;

	ItemPointerSetMin(&advancePast);
	items = GinDataLeafPageGetItems(page, &nitems, advancePast);

	for (i = 0; i < nitems; i++)
	{
		if (_gin_prune_check_heap_tid(heaprel, &items[i]))
			dead_count++;
	}

	if (items != NULL)
		pfree(items);

	return dead_count;
}

/*
 * gin_prune_by_undo_counter
 *
 * GIN index pruning callback for UNDO-informed index pruning.
 *
 * Performs a scan of GIN data leaf pages (posting tree leaves) to identify
 * dead heap TIDs.  Due to the complexity of modifying compressed posting
 * lists, we currently only report the count of dead entries found rather
 * than actually removing them.  The actual removal happens during VACUUM
 * via ginvacuum.c.
 *
 * Future work: integrate with the GIN vacuum machinery to actually remove
 * dead TIDs from posting lists when the dead ratio exceeds a threshold.
 *
 * Returns the count of dead entries identified (not actually removed).
 */
uint64
gin_prune_by_undo_counter(Relation heaprel, Relation indexrel,
						  uint16 discard_counter)
{
	BlockNumber nblocks;
	BlockNumber blkno;
	uint64		dead_entries_found = 0;

	nblocks = RelationGetNumberOfBlocks(indexrel);

	/*
	 * Scan all pages looking for data leaf pages (posting tree leaves).
	 * These contain the actual heap TID posting lists.
	 */
	for (blkno = GIN_ROOT_BLKNO; blkno < nblocks; blkno++)
	{
		Buffer		buf;
		Page		page;

		CHECK_FOR_INTERRUPTS();

		buf = ReadBuffer(indexrel, blkno);
		LockBuffer(buf, BUFFER_LOCK_SHARE);

		page = BufferGetPage(buf);

		/* Skip non-data pages, non-leaf pages, and deleted pages */
		if (PageIsNew(page) || GinPageIsDeleted(page))
		{
			UnlockReleaseBuffer(buf);
			continue;
		}

		/*
		 * Process data leaf pages (posting tree leaves that contain
		 * compressed heap TID arrays).
		 */
		if (GinPageIsData(page) && GinPageIsLeaf(page))
		{
			dead_entries_found += _gin_prune_scan_posting_tree_leaf(heaprel,
																   page);
		}

		UnlockReleaseBuffer(buf);
	}

	if (dead_entries_found > 0)
	{
		elog(DEBUG2, "GIN index %s: found " UINT64_FORMAT " dead entries "
			 "(removal deferred to VACUUM)",
			 RelationGetRelationName(indexrel), dead_entries_found);
	}

	return dead_entries_found;
}
