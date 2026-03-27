/*-------------------------------------------------------------------------
 *
 * nbtprune.c
 *	  UNDO-informed pruning for B-tree indexes
 *
 * This module implements proactive pruning of B-tree index entries when the
 * UNDO discard worker determines that their referenced transactions are no
 * longer visible to any snapshot. By marking entries as LP_DEAD proactively,
 * we reduce the work that VACUUM must perform during index scans.
 *
 * ALGORITHM:
 * ----------
 * When notified of an UNDO discard with a specific counter value:
 *   1. Scan leaf pages of the B-tree from left to right
 *   2. For each index tuple, extract the heap TID
 *   3. Check the heap line pointer: if the heap item is LP_DEAD or LP_UNUSED,
 *      the tuple has been removed and the index entry can be marked dead
 *   4. Mark qualifying index entries as LP_DEAD using hint-bit protocol
 *   5. Set BTP_HAS_GARBAGE on modified pages
 *   6. Return count of pruned entries
 *
 * CONCURRENCY:
 * -----------
 * This function uses the same hint-bit protocol as _bt_killitems():
 * it holds only a shared buffer lock and uses BufferBeginSetHintBits /
 * BufferFinishSetHintBits to mark entries dead.  This avoids taking
 * exclusive locks and is safe for concurrent index scans and inserts.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/nbtree/nbtprune.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/nbtree.h"
#include "access/index_prune.h"
#include "access/relundo.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "utils/rel.h"

/*
 * _bt_prune_check_heap_tid
 *
 * Check whether a heap TID referenced by an index entry points to a
 * dead or unused heap line pointer.  Returns true if the heap item is
 * no longer live (LP_DEAD, LP_UNUSED, or LP_REDIRECT to a dead chain).
 *
 * The caller should hold at least a shared lock on the index page.
 * This function acquires and releases a shared lock on the heap page.
 */
static bool
_bt_prune_check_heap_tid(Relation heaprel, ItemPointer heaptid)
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

	/* Check if the offset is within the valid range */
	if (offnum > PageGetMaxOffsetNumber(heappage) || offnum < FirstOffsetNumber)
	{
		/* Offset out of range - tuple was likely removed */
		UnlockReleaseBuffer(heapbuf);
		return true;
	}

	heapitemid = PageGetItemId(heappage, offnum);

	/*
	 * The heap item is dead if it's LP_DEAD, LP_UNUSED, or a redirect to
	 * a dead chain.  We only mark the index entry dead for LP_DEAD or
	 * LP_UNUSED; LP_REDIRECT is part of HOT chain management and should
	 * not cause index entries to be marked dead.
	 */
	is_dead = (ItemIdIsDead(heapitemid) || !ItemIdIsUsed(heapitemid));

	UnlockReleaseBuffer(heapbuf);

	return is_dead;
}

/*
 * _bt_prune_by_undo_counter
 *
 * Prunes B-tree index entries whose referenced heap tuples have been
 * discarded by the UNDO system.  This is the callback registered with
 * the index pruning infrastructure.
 *
 * The function scans all leaf pages left-to-right and checks each
 * index entry's heap TID.  If the heap item is dead or unused, the
 * index entry is marked LP_DEAD using the hint-bit protocol (same
 * approach as _bt_killitems).
 *
 * Returns the number of index entries marked as LP_DEAD.
 */
uint64
_bt_prune_by_undo_counter(Relation heaprel, Relation indexrel,
						  uint16 discard_counter)
{
	Buffer		metabuf;
	Page		metapage;
	BTMetaPageData *metad;
	BlockNumber blkno;
	uint64		entries_pruned = 0;
	BlockNumber num_pages;

	/* Get the B-tree metapage to find the root */
	metabuf = _bt_getbuf(indexrel, BTREE_METAPAGE, BT_READ);
	metapage = BufferGetPage(metabuf);
	metad = BTPageGetMeta(metapage);

	/* If the tree has no root, nothing to prune */
	if (metad->btm_root == P_NONE)
	{
		_bt_relbuf(indexrel, metabuf);
		return 0;
	}

	_bt_relbuf(indexrel, metabuf);

	/*
	 * Find the leftmost leaf page by descending from the root.
	 */
	{
		Buffer		buf;
		Page		page;
		BTPageOpaque opaque;

		buf = _bt_getroot(indexrel, heaprel, BT_READ);

		if (!BufferIsValid(buf))
			return 0;

		blkno = BufferGetBlockNumber(buf);
		page = BufferGetPage(buf);
		opaque = BTPageGetOpaque(page);

		/* Descend to leftmost leaf */
		while (!P_ISLEAF(opaque))
		{
			ItemId		itemid;
			IndexTuple	itup;
			BlockNumber child;

			itemid = PageGetItemId(page, P_FIRSTDATAKEY(opaque));
			itup = (IndexTuple) PageGetItem(page, itemid);
			child = BTreeTupleGetDownLink(itup);

			_bt_relbuf(indexrel, buf);

			buf = _bt_getbuf(indexrel, child, BT_READ);
			page = BufferGetPage(buf);
			opaque = BTPageGetOpaque(page);
		}

		blkno = BufferGetBlockNumber(buf);
		_bt_relbuf(indexrel, buf);
	}

	/* Scan from leftmost leaf to rightmost leaf */
	num_pages = RelationGetNumberOfBlocks(indexrel);

	while (blkno != P_NONE && blkno < num_pages)
	{
		Buffer		buf;
		Page		page;
		BTPageOpaque opaque;
		OffsetNumber maxoff;
		OffsetNumber offnum;
		BlockNumber nextblkno;
		bool		marked_something = false;

		CHECK_FOR_INTERRUPTS();

		buf = _bt_getbuf(indexrel, blkno, BT_READ);
		page = BufferGetPage(buf);
		opaque = BTPageGetOpaque(page);

		/* Skip if not a leaf page */
		if (!P_ISLEAF(opaque))
		{
			_bt_relbuf(indexrel, buf);
			break;
		}

		/* Remember next page before any modifications */
		nextblkno = opaque->btpo_next;
		maxoff = PageGetMaxOffsetNumber(page);

		/*
		 * Scan items on this leaf page.  For each non-dead item, check if
		 * its heap tuple has been discarded.
		 *
		 * We use the hint-bit protocol (same as _bt_killitems): hold only
		 * a shared lock, and use BufferBeginSetHintBits to check if we're
		 * allowed to modify the page.
		 */
		for (offnum = P_FIRSTDATAKEY(opaque);
			 offnum <= maxoff;
			 offnum = OffsetNumberNext(offnum))
		{
			ItemId		itemid;
			IndexTuple	itup;

			itemid = PageGetItemId(page, offnum);

			/* Skip if already dead or unused */
			if (ItemIdIsDead(itemid) || !ItemIdIsUsed(itemid))
				continue;

			itup = (IndexTuple) PageGetItem(page, itemid);

			/*
			 * Check if the referenced heap tuple is dead.  This reads the
			 * heap page with a shared lock, which is lightweight.
			 */
			if (_bt_prune_check_heap_tid(heaprel, &itup->t_tid))
			{
				/*
				 * Use the hint-bit infrastructure to mark the entry dead
				 * while holding only a shared lock, matching the protocol
				 * used by _bt_killitems().
				 */
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

		/*
		 * If we marked anything, finish the hint-bit update and set
		 * BTP_HAS_GARBAGE so that future operations know to clean up.
		 */
		if (marked_something)
		{
			opaque->btpo_flags |= BTP_HAS_GARBAGE;
			BufferFinishSetHintBits(buf, true, true);
		}

next_page:
		_bt_relbuf(indexrel, buf);
		blkno = nextblkno;
	}

	return entries_pruned;
}
