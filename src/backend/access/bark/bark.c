/*-------------------------------------------------------------------------
 *
 * bark.c
 *	  Implementation of the BARK index access method.
 *
 * This is the skeleton registration of the BARK index access method (see
 * BARK-Design.mediawiki).  It declares BARK's capabilities and passes
 * opclass validation so that CREATE INDEX ... USING bark is accepted and
 * validated, but every operation that would read or write index data errors
 * out: the storage format and the search/build machinery arrive in later
 * commits of the series.  Registering the AM first, on its own, keeps each
 * commit independently buildable and testable.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/bark/bark.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "access/amapi.h"
#include "access/amlocator.h"
#include "access/bark.h"
#include "access/generic_xlog.h"
#include "access/nbtree.h"
#include "access/reloptions.h"
#include "commands/vacuum.h"
#include "storage/bufmgr.h"
#include "storage/indexfsm.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "utils/fmgrprotos.h"
#include "utils/injection_point.h"
#include "utils/selfuncs.h"
#include "utils/spccache.h"

/*
 * Every data-touching entry point routes through this: BARK accepts and
 * validates an index definition but cannot yet build, populate, or scan one.
 */
#define BARK_NOT_IMPLEMENTED() \
	ereport(ERROR, \
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED), \
			 errmsg("BARK index access method is not yet implemented")))

/*
 * Find the parent page that holds the downlink to `childblk` and the offset of
 * that downlink.  Descends from the root toward the child's key range (using
 * the child's high key, which an interior page always carries) and then scans
 * the resulting parent -- continuing into right siblings if a concurrent split
 * moved the downlink -- for the entry whose downlink block equals `childblk`.
 *
 * Returns the parent buffer write-locked with *downoff set, or InvalidBuffer
 * when the downlink cannot be found (the caller then declines to delete the
 * child, leaving it linked -- correct, just not reclaimed).  Modeled on
 * nbtree's _bt_getstackbuf downlink search.
 */
static Buffer
bark_find_parent_downlink(Relation index, BarkKeyInfo *keyinfo,
						  IndexTuple childhikey, BlockNumber childblk,
						  OffsetNumber *downoff)
{
	BarkStack	stack;
	Buffer		pbuf;
	BlockNumber pblk;

	/*
	 * Descend to the leaf for the child's high key, recording the parent path.
	 * nextkey=false lands us at or left of the child so the parent we want is
	 * on the recorded stack (or just right of it).
	 */
	{
		Buffer		lbuf = bark_search(index, keyinfo, childhikey, false, false,
									   &stack);

		if (lbuf != InvalidBuffer)
			UnlockReleaseBuffer(lbuf);
	}
	if (stack == NULL)
		return InvalidBuffer;	/* one-level tree: child is the root, no parent */

	pblk = stack->bark_blkno;
	bark_freestack(stack);

	/* Scan the parent (and right siblings) for the downlink to childblk. */
	pbuf = ReadBuffer(index, pblk);
	LockBuffer(pbuf, BUFFER_LOCK_EXCLUSIVE);
	for (;;)
	{
		Page		ppage = BufferGetPage(pbuf);
		BarkPageOpaque popaque = BarkPageGetOpaque(ppage);
		OffsetNumber maxoff = PageGetMaxOffsetNumber(ppage);
		OffsetNumber firstdata = BarkPageFirstDataKey(popaque);

		for (OffsetNumber off = firstdata; off <= maxoff;
			 off = OffsetNumberNext(off))
		{
			IndexTuple	itup = (IndexTuple)
				PageGetItem(ppage, PageGetItemId(ppage, off));

			if (BarkEntryGetDownLink(itup) == childblk)
			{
				*downoff = off;
				return pbuf;
			}
		}

		/* Not on this page; follow the right link if the parent split. */
		if (BarkPageRightmost(popaque))
			break;
		{
			BlockNumber right = popaque->bark_next;

			UnlockReleaseBuffer(pbuf);
			pbuf = ReadBuffer(index, right);
			LockBuffer(pbuf, BUFFER_LOCK_EXCLUSIVE);
		}
	}

	UnlockReleaseBuffer(pbuf);
	return InvalidBuffer;
}

/*
 * Delete an empty leaf page from the tree and record it in the FSM for reuse.
 *
 * Reclaims the common case a delete-heavy workload produces: an interior leaf
 * (one with both a left and a right sibling) whose every entry VACUUM removed.
 * The page is unlinked from the leaf chain (its left sibling's right link and
 * its right sibling's left link are spliced across it), its parent downlink is
 * removed, and the page is flagged BARK_DELETED and handed to the FSM, so the
 * next split or overflow write reuses it instead of extending the relation.
 * All four touched pages (left sibling, target, right sibling, parent) are
 * updated under one generic-WAL record so the unlink is crash-atomic.
 *
 * Returns true when the leaf was deleted.  Declines (returns false, leaving the
 * leaf linked and correct) when the page is not an eligible interior empty
 * leaf, or when its parent downlink is the leftmost (minus-infinity) entry
 * (whose removal would require promoting the next downlink to minus-infinity --
 * a reshuffle this reclaimer does not perform).
 *
 * This reclaims interior empty leaves only.  A leftmost or rightmost empty
 * leaf, an empty leaf whose parent downlink is the minus-infinity entry, and
 * an emptied internal page are all left linked in place -- correct, and still
 * reusable once their siblings are rewritten, just not directly unlinked here.
 *
 * It locks the parent first and then the left sibling, target and right
 * sibling, which is the reverse of the insert path's order (an inserter that
 * splits a leaf keeps the leaf locked until it has locked the parent).  To
 * avoid a deadlock between the two, the three leaf locks are only tried
 * conditionally while the parent is held, and the leaf is skipped if any of
 * them is busy; a later VACUUM retries it.  nbtree avoids the inversion by
 * locking the leaf level before the parent (_bt_mark_page_halfdead), and the
 * XID-gated recycling nbtree performs is likewise not done here.
 */
static bool
bark_delete_empty_leaf(Relation index, BarkKeyInfo *keyinfo, BlockNumber blkno)
{
	Buffer		buf;
	Buffer		lbuf;
	Buffer		rbuf;
	Buffer		pbuf;
	Page		page;
	BarkPageOpaque opaque;
	BlockNumber leftblk;
	BlockNumber rightblk;
	IndexTuple	hikey;
	OffsetNumber downoff;
	GenericXLogState *gstate;
	Page		p;
	bool		lockedl;
	bool		lockedt;

	buf = ReadBuffer(index, blkno);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	page = BufferGetPage(buf);

	/* Re-check under the lock: must be an interior, empty, live leaf. */
	if (PageIsNew(page))
	{
		UnlockReleaseBuffer(buf);
		return false;
	}
	opaque = BarkPageGetOpaque(page);
	if (!BarkPageIsLeaf(opaque) || BarkPageIsDeleted(opaque) ||
		(opaque->bark_flags & BARK_INCOMPLETE_SPLIT) != 0 ||
		BarkPageIsRoot(opaque) ||
		BarkPageLeftmost(opaque) || BarkPageRightmost(opaque) ||
		PageGetMaxOffsetNumber(page) >= BarkPageFirstDataKey(opaque))
	{
		UnlockReleaseBuffer(buf);
		return false;			/* not an eligible interior empty leaf */
	}

	leftblk = opaque->bark_prev;
	rightblk = opaque->bark_next;

	/*
	 * The high key (first item on this non-rightmost page) names the key range
	 * boundary; use it to locate the parent downlink.  Copy it before dropping
	 * the lock, since finding the parent re-descends the tree.
	 */
	hikey = CopyIndexTuple((IndexTuple)
						   PageGetItem(page, PageGetItemId(page, BARK_P_HIKEY)));
	UnlockReleaseBuffer(buf);

	pbuf = bark_find_parent_downlink(index, keyinfo, hikey, blkno, &downoff);
	pfree(hikey);
	if (pbuf == InvalidBuffer)
		return false;			/* parent downlink not found: leave it linked */

	/*
	 * Decline when the downlink is the parent's leftmost (minus-infinity)
	 * entry: removing it would need the next downlink promoted to
	 * minus-infinity, which this reclaimer does not do.
	 */
	if (downoff <= BarkPageFirstDataKey(BarkPageGetOpaque(BufferGetPage(pbuf))))
	{
		UnlockReleaseBuffer(pbuf);
		return false;
	}

	/*
	 * Lock the siblings and re-acquire the target, then re-validate.  These
	 * are tried without waiting, since we already hold the parent (see
	 * above); if any is busy, give up on this leaf.
	 */
	lbuf = ReadBuffer(index, leftblk);
	buf = ReadBuffer(index, blkno);
	rbuf = ReadBuffer(index, rightblk);
	lockedl = ConditionalLockBuffer(lbuf);
	lockedt = lockedl && ConditionalLockBuffer(buf);
	if (!lockedt || !ConditionalLockBuffer(rbuf))
	{
		if (lockedt)
			LockBuffer(buf, BUFFER_LOCK_UNLOCK);
		if (lockedl)
			LockBuffer(lbuf, BUFFER_LOCK_UNLOCK);
		ReleaseBuffer(rbuf);
		ReleaseBuffer(buf);
		ReleaseBuffer(lbuf);
		UnlockReleaseBuffer(pbuf);
		return false;
	}

	page = BufferGetPage(buf);
	opaque = BarkPageGetOpaque(page);

	/* Re-check the target is still the empty interior leaf we expect. */
	if (PageIsNew(page) || !BarkPageIsLeaf(opaque) || BarkPageIsDeleted(opaque) ||
		(opaque->bark_flags & BARK_INCOMPLETE_SPLIT) != 0 ||
		opaque->bark_prev != leftblk || opaque->bark_next != rightblk ||
		PageGetMaxOffsetNumber(page) >= BarkPageFirstDataKey(opaque) ||
		BarkEntryGetDownLink((IndexTuple)
							 PageGetItem(BufferGetPage(pbuf),
										 PageGetItemId(BufferGetPage(pbuf),
													   downoff))) != blkno)
	{
		UnlockReleaseBuffer(rbuf);
		UnlockReleaseBuffer(buf);
		UnlockReleaseBuffer(lbuf);
		UnlockReleaseBuffer(pbuf);
		return false;
	}

	/* Splice the target out of the chain, drop its downlink, flag it deleted. */
	gstate = GenericXLogStart(index);
	{
		Page		lp = GenericXLogRegisterBuffer(gstate, lbuf, 0);
		Page		rp = GenericXLogRegisterBuffer(gstate, rbuf, 0);
		Page		pp = GenericXLogRegisterBuffer(gstate, pbuf, 0);
		OffsetNumber del = downoff;

		p = GenericXLogRegisterBuffer(gstate, buf, 0);

		BarkPageGetOpaque(lp)->bark_next = rightblk;
		BarkPageGetOpaque(rp)->bark_prev = leftblk;
		PageIndexMultiDelete(pp, &del, 1);

		BarkPageGetOpaque(p)->bark_flags |= BARK_DELETED;
		BarkPageGetOpaque(p)->bark_prev = BARK_P_NONE;
		BarkPageGetOpaque(p)->bark_next = BARK_P_NONE;
	}
	GenericXLogFinish(gstate);

	UnlockReleaseBuffer(rbuf);
	UnlockReleaseBuffer(buf);
	UnlockReleaseBuffer(lbuf);
	UnlockReleaseBuffer(pbuf);

	/* Record for reuse (made durable by IndexFreeSpaceMapVacuum). */
	RecordFreeIndexPage(index, blkno);
	return true;
}

static IndexBuildResult *
barkbuild(Relation heap, Relation index, IndexInfo *indexInfo)
{
	return bark_build(heap, index, indexInfo);
}

static void
barkbuildempty(Relation index)
{
	bark_buildempty(index);
}

static bool
barkinsert(Relation index, Datum *values, bool *isnull,
		   ItemPointer ht_ctid, Relation heapRel,
		   IndexUniqueCheck checkUnique, bool indexUnchanged,
		   IndexInfo *indexInfo)
{
	return bark_insert(index, values, isnull, ht_ctid, heapRel,
					   checkUnique, indexUnchanged, indexInfo);
}

/*
 * Should bark_vacuum_page clean this page?  Only live leaf pages hold heap
 * TIDs: the meta page, internal pages, overflow pages and pages deleted from
 * the tree are skipped.  A deleted page in particular still carries the items
 * it held when it was unlinked (its old high key among them), so reading it as
 * a leaf would misinterpret a pivot as a data entry.  A page reached by
 * backtracking is cleaned only if it was split during this VACUUM (it carries
 * our cycle ID); otherwise it was processed already, in its turn in the scan.
 */
static bool
bark_vacuum_target(Page page, bool backtracking, BTCycleId cycleid)
{
	BarkPageOpaque opaque;

	if (PageIsNew(page))
		return false;
	opaque = BarkPageGetOpaque(page);
	if (!BarkPageIsLeaf(opaque) || BarkPageIsDeleted(opaque))
		return false;
	return !backtracking || opaque->bark_cycleid == cycleid;
}

/*
 * Process one block of barkbulkdelete's physical-order scan: delete the
 * entries of a live leaf whose heap TIDs the callback reports dead.
 *
 * As in nbtree's btvacuumpage, a leaf split that happened after this VACUUM
 * started may have moved entries from a page the scan has not reached yet to
 * a right sibling at a block number the scan has already passed (the right
 * page of a split can be any block the free space map hands out).  Both halves
 * of such a split carry our cycle ID, so after cleaning a page with our cycle
 * ID whose right sibling lies below scanblkno, we follow the right link and
 * clean that page too, and keep going right while the pages we reach carry our
 * cycle ID and lie below scanblkno.  Each cleaned page has its cycle ID
 * cleared, so a later backtrack stops there rather than cleaning it again.
 *
 * A leaf is cleaned under a cleanup lock (an exclusive lock taken when no
 * other backend holds a pin), on every leaf whether or not it has dead
 * entries, as nbtree does.  A scan keeps its pin on the leaf it is returning
 * entries from, so VACUUM cannot remove a TID that a scan has read from the
 * page but not yet returned, and the heap cannot reuse that TID's line pointer
 * under the scan.  The page is first examined under a share lock, since
 * pages that are not live leaves need no cleanup lock.
 */
static void
bark_vacuum_page(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
				 IndexBulkDeleteCallback callback, void *callback_state,
				 BTCycleId cycleid, BlockNumber scanblkno)
{
	Relation	index = info->index;
	BlockNumber blkno = scanblkno;

	for (;;)
	{
		Buffer		buf;
		Page		page;
		BarkPageOpaque opaque;
		OffsetNumber maxoff;
		OffsetNumber todelete[MaxOffsetNumber];
		int			ndelete = 0;
		int			ndelete_single = 0;
		BlockNumber oversized_free[MaxOffsetNumber];
		int			noversized_free = 0;
		GenericXLogState *gstate = NULL;
		Page		p = NULL;
		BlockNumber backtrack_to = BARK_P_NONE;

		buf = ReadBufferExtended(index, MAIN_FORKNUM, blkno, RBM_NORMAL,
								 info->strategy);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		if (!bark_vacuum_target(BufferGetPage(buf), blkno != scanblkno,
								cycleid))
		{
			UnlockReleaseBuffer(buf);
			break;
		}

		/*
		 * Trade the share lock for a cleanup lock, as nbtree's
		 * _bt_upgradelockbufcleanup does, and look at the page again: it was
		 * unlocked in between.
		 */
		LockBuffer(buf, BUFFER_LOCK_UNLOCK);
		LockBufferForCleanup(buf);
		page = BufferGetPage(buf);
		if (!bark_vacuum_target(page, blkno != scanblkno, cycleid))
		{
			UnlockReleaseBuffer(buf);
			break;
		}
		opaque = BarkPageGetOpaque(page);

		/*
		 * Decide whether to backtrack before clearing the cycle ID below.  A
		 * right sibling at or above scanblkno needs nothing from us: the scan
		 * will reach it (or is processing it now).
		 */
		if (cycleid != 0 && opaque->bark_cycleid == cycleid &&
			!BarkPageRightmost(opaque) && opaque->bark_next < scanblkno)
			backtrack_to = opaque->bark_next;

		maxoff = PageGetMaxOffsetNumber(page);

		/*
		 * First pass: shrink LIST entries that lost some (but not all) members
		 * in place with PageIndexTupleOverwrite, which keeps every offset
		 * number stable (it only moves the item data).  Fully-dead entries
		 * (SINGLE whose TID is dead, or LIST whose every member is dead) are
		 * recorded for the MultiDelete pass below.  Doing the shrinks first and
		 * the deletes last means the recorded offsets stay valid until the
		 * single MultiDelete renumbers them.
		 */
		for (OffsetNumber off = BarkPageFirstDataKey(opaque);
			 off <= maxoff; off = OffsetNumberNext(off))
		{
			ItemId		iid = PageGetItemId(page, off);
			IndexTuple	itup = (IndexTuple) PageGetItem(page, iid);

			if (BarkEntryGetShape(itup) == BARK_SHAPE_SINGLE)
			{
				if (callback(&itup->t_tid, callback_state))
				{
					todelete[ndelete++] = off;
					ndelete_single++;
				}
			}
			else if (BarkEntryGetShape(itup) == BARK_SHAPE_OVERSIZED)
			{
				/*
				 * An OVERSIZED entry holds exactly one heap locator (inline in
				 * its ref).  If it is dead, delete the leaf entry and remember
				 * its overflow chain to free after the leaf WAL record finishes
				 * (freeing the chain starts its own WAL records, which cannot
				 * nest inside the leaf's generic-WAL state).
				 */
				ItemPointerData loc = BarkOverflowGetRef(itup)->locator;

				if (callback(&loc, callback_state))
				{
					oversized_free[noversized_free++] =
						BarkOverflowGetFirstBlock(itup);
					todelete[ndelete++] = off;
					ndelete_single++;
				}
			}
			else
			{
				int			ntids = bark_entry_count_tids(itup);
				ItemPointer tids = (ItemPointer)
					palloc(ntids * sizeof(ItemPointerData));
				int			nlive = 0;

				ntids = bark_entry_get_tids(itup, tids, ntids);
				for (int i = 0; i < ntids; i++)
				{
					if (!callback(&tids[i], callback_state))
						tids[nlive++] = tids[i];
				}

				if (nlive == ntids)
				{
					pfree(tids);
					continue;	/* nothing dead in this entry */
				}

				stats->tuples_removed += ntids - nlive;

				if (nlive == 0)
				{
					todelete[ndelete++] = off;	/* whole entry dies */
					pfree(tids);
					continue;
				}

				if (gstate == NULL)
				{
					gstate = GenericXLogStart(index);
					p = GenericXLogRegisterBuffer(gstate, buf, 0);
					page = p;	/* overwrite/delete on the registered copy */
				}

				{
					/*
					 * Rebuild with the surviving members, re-choosing the
					 * shape: a POSTING if its sbm still wins, else a LIST,
					 * else a plain SINGLE when exactly one survives.  The new
					 * entry is never larger than the original, so
					 * PageIndexTupleOverwrite rewrites it in place, in this
					 * page's one WAL record.  A POSTING entry is sized for
					 * the removal bound of its set (see bark_form_posting),
					 * which no subset exceeds; a LIST of fewer locators is
					 * shorter; a LIST is chosen over the POSTING only when it
					 * is the smaller; and a SINGLE is smaller than either.
					 */
					IndexTuple	key = bark_single_from_list(index, itup, NULL);
					IndexTuple	newentry;
					IndexTuple	posting;

					if (nlive == 1)
					{
						newentry = key;
						newentry->t_tid = tids[0];
					}
					else if ((posting = bark_form_posting(RelationGetDescr(index),
															  key, tids, nlive)) != NULL)
					{
						newentry = posting;
						pfree(key);
					}
					else
					{
						newentry = bark_form_list(RelationGetDescr(index), key,
												  tids, nlive);
						pfree(key);
					}
					Assert(MAXALIGN(IndexTupleSize(newentry)) <=
						   MAXALIGN(ItemIdGetLength(iid)));
					if (MAXALIGN(IndexTupleSize(newentry)) >
						MAXALIGN(ItemIdGetLength(iid)) ||
						!PageIndexTupleOverwrite(page, off, (char *) newentry,
												 IndexTupleSize(newentry)))
						elog(ERROR, "failed to shrink BARK leaf entry during vacuum: entry at offset %u of block %u grew from %u to %zu bytes",
							 off, blkno, ItemIdGetLength(iid),
							 IndexTupleSize(newentry));
					pfree(newentry);
				}
				pfree(tids);
			}
		}

		if (ndelete > 0)
		{
			if (gstate == NULL)
			{
				gstate = GenericXLogStart(index);
				p = GenericXLogRegisterBuffer(gstate, buf, 0);
				page = p;
			}
			PageIndexMultiDelete(page, todelete, ndelete);
			stats->tuples_removed += ndelete_single;
		}

		/*
		 * Clear our cycle ID from a page split during this VACUUM, so that a
		 * later backtrack stops here instead of cleaning the page again.
		 * nbtree does this as an unlogged hint; a generic-WAL page has no
		 * redo mask for the field, so it is logged, in the page's record when
		 * there is one.
		 */
		if (cycleid != 0 && opaque->bark_cycleid == cycleid)
		{
			if (gstate == NULL)
			{
				gstate = GenericXLogStart(index);
				p = GenericXLogRegisterBuffer(gstate, buf, 0);
				page = p;
			}
			BarkPageGetOpaque(page)->bark_cycleid = 0;
		}

		if (gstate != NULL)
			GenericXLogFinish(gstate);

		UnlockReleaseBuffer(buf);

		/*
		 * Reclaim the overflow chains of the OVERSIZED entries just deleted,
		 * now that the leaf no longer references them and its WAL record is
		 * durable.  Each chain is freed under its own generic-WAL records.
		 */
		for (int i = 0; i < noversized_free; i++)
		{
			ItemPointerData dummy;
			IndexTuple	stub;

			/*
			 * bark_free_oversized reads only the first-block field of the
			 * entry's t_tid, so a tiny stub carrying that block is enough.
			 */
			ItemPointerSetBlockNumber(&dummy, oversized_free[i]);
			ItemPointerSetOffsetNumber(&dummy, (OffsetNumber) BARK_IS_OVERFLOW);
			stub = (IndexTuple) palloc0(sizeof(IndexTupleData));
			stub->t_info = INDEX_AM_RESERVED_BIT | sizeof(IndexTupleData);
			stub->t_tid = dummy;
			bark_free_oversized(index, stub);
			pfree(stub);
		}

		if (backtrack_to == BARK_P_NONE)
			break;
		blkno = backtrack_to;
		vacuum_delay_point(false);
	}

	/*
	 * Tests stop VACUUM here, between two blocks of its scan, to split a page
	 * it has not reached yet.  The argument is scanblkno in decimal, so a
	 * test can wait for one block.
	 */
#ifdef USE_INJECTION_POINTS
	{
		char		blkstr[12];

		snprintf(blkstr, sizeof(blkstr), "%u", scanblkno);
		INJECTION_POINT("bark-bulkdelete-after-page", blkstr);
	}
#endif
}

static IndexBulkDeleteResult *
barkbulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
			   IndexBulkDeleteCallback callback, void *callback_state)
{
	Relation	index = info->index;

	if (stats == NULL)
		stats = palloc0_object(IndexBulkDeleteResult);

	/*
	 * Scan every page in physical order and delete the entries whose heap TID
	 * the callback reports dead (bark_vacuum_page).  This is a linear scan of
	 * the whole index, as nbtree, bloom and GIN do, rather than a walk of the
	 * leaf chain.  An all-dead leaf is emptied here and unlinked/FSM-recycled
	 * in barkvacuumcleanup (not in this pass, which holds only one page's
	 * lock); a now-empty but still-linked leaf between the two passes is
	 * correct, just briefly not space-optimal.
	 *
	 * Leaf splits during the scan are detected with a vacuum cycle ID, as in
	 * btbulkdelete.  The cycle IDs come from nbtree's registry
	 * (_bt_start_vacuum and friends): it maps a relation's LockRelId to the
	 * cycle ID of the VACUUM now scanning it and has nothing nbtree-specific
	 * in it, so BARK shares it instead of keeping a copy.  It could move out
	 * of nbtree into a common place.  The ENSURE block releases our registry
	 * entry if the scan fails.
	 */
	PG_ENSURE_ERROR_CLEANUP(_bt_end_vacuum_callback, PointerGetDatum(index));
	{
		BTCycleId	cycleid = _bt_start_vacuum(index);
		bool		needLock = !RELATION_IS_LOCAL(index);
		BlockNumber blkno = BARK_METAPAGE + 1;

		/*
		 * Pages added after the scan starts must be visited too, so recheck
		 * the relation length until a pass finds no new pages.  Reading the
		 * length under the extension lock means a page being added is either
		 * not counted yet or already locked by the backend adding it (which
		 * extends with EB_LOCK_FIRST), so we never see it uninitialized and
		 * unlocked.  This is btvacuumscan's loop.
		 */
		for (;;)
		{
			BlockNumber npages;

			if (needLock)
				LockRelationForExtension(index, ExclusiveLock);
			npages = RelationGetNumberOfBlocks(index);
			if (needLock)
				UnlockRelationForExtension(index, ExclusiveLock);

			if (blkno >= npages)
				break;

			for (; blkno < npages; blkno++)
			{
				vacuum_delay_point(false);
				bark_vacuum_page(info, stats, callback, callback_state,
								 cycleid, blkno);
			}
		}
	}
	PG_END_ENSURE_ERROR_CLEANUP(_bt_end_vacuum_callback, PointerGetDatum(index));
	_bt_end_vacuum(index);

	return stats;
}

static IndexBulkDeleteResult *
barkvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
	Relation	index = info->index;
	BlockNumber npages;
	BarkKeyInfo *keyinfo;
	BlockNumber *emptyleaves;
	int			nempty = 0;
	int			emptyalloc;

	/* ANALYZE has nothing to clean up. */
	if (info->analyze_only)
		return stats;

	if (stats == NULL)
		stats = palloc0_object(IndexBulkDeleteResult);

	/*
	 * Report index-wide statistics.  When barkbulkdelete did not run (no dead
	 * tuples this cycle) we count the live leaf entries here so the planner
	 * has an up-to-date tuple count; when it did run, num_index_tuples is
	 * recomputed the same way.  Returning valid stats also lets VACUUM finish
	 * and set the heap visibility map, which is what makes index-only scans
	 * worthwhile.
	 *
	 * In the same walk we collect the empty interior leaves VACUUM produced
	 * (all their entries were removed as dead) so they can be unlinked and
	 * returned to the FSM below -- this is what keeps a delete-heavy index from
	 * growing the relation without bound across delete/vacuum/insert cycles.
	 * We only collect them here (under a share lock); the actual unlink takes
	 * exclusive locks on the siblings and parent in a second pass, so the walk
	 * stays a cheap read.
	 */
	npages = RelationGetNumberOfBlocks(index);
	stats->num_pages = npages;
	stats->num_index_tuples = 0;
	stats->pages_free = 0;

	emptyalloc = 64;
	emptyleaves = (BlockNumber *) palloc(emptyalloc * sizeof(BlockNumber));

	for (BlockNumber blkno = BARK_METAPAGE + 1; blkno < npages; blkno++)
	{
		Buffer		buf;
		Page		page;
		BarkPageOpaque opaque;

		vacuum_delay_point(false);

		buf = ReadBufferExtended(index, MAIN_FORKNUM, blkno, RBM_NORMAL,
								 info->strategy);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);

		if (!PageIsNew(page) && !BarkPageIsOverflow(BarkPageGetOpaque(page)) &&
			BarkPageIsLeaf(BarkPageGetOpaque(page)))
		{
			OffsetNumber maxoff;

			opaque = BarkPageGetOpaque(page);
			maxoff = PageGetMaxOffsetNumber(page);
			stats->num_index_tuples += maxoff - BarkPageFirstDataKey(opaque) + 1;

			/*
			 * An empty interior leaf (no data entries, has both siblings, not
			 * deleted/half-dead) is a reclamation candidate.
			 */
			if (!BarkPageIsDeleted(opaque) &&
				(opaque->bark_flags & BARK_INCOMPLETE_SPLIT) == 0 &&
				!BarkPageIsRoot(opaque) &&
				!BarkPageLeftmost(opaque) && !BarkPageRightmost(opaque) &&
				maxoff < BarkPageFirstDataKey(opaque))
			{
				if (nempty >= emptyalloc)
				{
					emptyalloc *= 2;
					emptyleaves = (BlockNumber *)
						repalloc(emptyleaves, emptyalloc * sizeof(BlockNumber));
				}
				emptyleaves[nempty++] = blkno;
			}
		}
		else if (!PageIsNew(page) &&
				 BarkPageIsDeleted(BarkPageGetOpaque(page)))
			stats->pages_free++;

		UnlockReleaseBuffer(buf);
	}

	/*
	 * Second pass: unlink and FSM-recycle the empty leaves collected above.
	 * Each deletion re-validates the page under exclusive locks, so a leaf that
	 * was concurrently refilled or already reclaimed is simply skipped.
	 */
	keyinfo = bark_build_keyinfo(index);
	for (int i = 0; i < nempty; i++)
	{
		if (bark_delete_empty_leaf(index, keyinfo, emptyleaves[i]))
			stats->pages_free++;
	}
	pfree(keyinfo);
	pfree(emptyleaves);

	/* Make the FSM entries recorded this cycle durable and searchable. */
	IndexFreeSpaceMapVacuum(index);

	return stats;
}

/*
 * barkcostestimate -- the C-STATS cost model for a BARK index scan.
 *
 * Starts from genericcostestimate but tunes two things BARK does differently
 * from a plain one-tuple-per-row index, keeping the estimate honest rather than
 * elaborate:
 *
 *  1. Entry coalescing (LIST / POSTING).  A key with many duplicate rows is one
 *     leaf entry, not N, so a scan touches far fewer leaf tuples -- and pages --
 *     than the number of heap rows it returns.  index->tuples is the number of
 *     leaf entries (barkvacuumcleanup counts entries, not heap rows), so we feed
 *     genericcostestimate the number of *entries* a scan visits
 *     (indexSelectivity * entries) instead of letting it derive leaf tuples from
 *     the heap row count; its pro-rata page formula then reflects the
 *     compression.  (genericcostestimate still caps heap-row fetch cost
 *     elsewhere; here we only correct the index-page side.)
 *
 *  2. Oversized-key / oversized-INCLUDE overflow I/O.  An oversized entry's full
 *     value lives on an overflow chain the scan must read in addition to the
 *     leaf page.  We estimate the average overflow chain length from the index's
 *     own size -- bytes per entry beyond what a leaf slot holds -- and add a
 *     random-page charge per visited entry for those extra reads.  An index with
 *     no oversized entries (average entry well under the item cap) adds nothing,
 *     so a normal index costs exactly as genericcostestimate says.
 *
 * The overflow surcharge is derived from the index's average entry size, not
 * from a count of how many visited entries are actually oversized (which would
 * need a per-index oversized-entry statistic the AM does not keep).  It is a
 * correct expected-value charge for an index whose entries are uniformly large,
 * and zero for an index with none; a mixed index is charged the average.  A
 * dedicated oversized-entry count in the meta/stats would let the planner
 * sharpen this for a skewed mix.
 */
static void
barkcostestimate(PlannerInfo *root, IndexPath *path, double loop_count,
				 Cost *indexStartupCost, Cost *indexTotalCost,
				 Selectivity *indexSelectivity, double *indexCorrelation,
				 double *indexPages)
{
	IndexOptInfo *index = path->indexinfo;
	GenericCosts costs = {0};
	double		entries = index->tuples;
	double		nvisited;

	/*
	 * (1) Model leaf *entries* visited, not heap rows.  genericcostestimate
	 * derives numIndexTuples from indexSelectivity * heap-rows when we leave it
	 * zero; for a coalescing index that overcounts whenever duplicates are
	 * packed into LIST/POSTING entries.  Supply the entry estimate ourselves.
	 * (genericcostestimate clamps it to [1, index->tuples] internally.)
	 */
	if (entries > 0)
	{
		Selectivity sel;

		/*
		 * Reuse genericcostestimate's own selectivity by a cheap pre-pass: run
		 * it once to obtain indexSelectivity, then convert to entries.  (A
		 * second call with numIndexTuples set is cheap; the quals are already
		 * cached.)
		 */
		genericcostestimate(root, path, loop_count, &costs);
		sel = costs.indexSelectivity;
		nvisited = rint(sel * entries);
		if (nvisited < 1.0)
			nvisited = 1.0;

		memset(&costs, 0, sizeof(costs));
		costs.numIndexTuples = nvisited;
		genericcostestimate(root, path, loop_count, &costs);
	}
	else
	{
		genericcostestimate(root, path, loop_count, &costs);
		nvisited = costs.numIndexTuples;
	}

	/*
	 * (2) Overflow-page surcharge.  Estimate the average bytes per entry from
	 * the index's physical size; entries larger than a leaf slot (BarkMaxItemSize)
	 * carry the overage on an overflow chain of ~overage / BarkOverflowChunkSize
	 * pages.  Charge one random page read per such page per visited entry.  For
	 * an index with small entries this is zero.
	 */
	if (entries > 0 && index->pages > 0)
	{
		double		avg_entry_bytes = (double) index->pages * BLCKSZ / entries;

		if (avg_entry_bytes > BarkMaxItemSize)
		{
			double		overflow_bytes = avg_entry_bytes - BarkMaxItemSize;
			double		chain_pages = ceil(overflow_bytes / BarkOverflowChunkSize);
			double		spc_random_page_cost;
			Cost		surcharge;

			get_tablespace_page_costs(index->reltablespace,
									  &spc_random_page_cost, NULL);
			surcharge = nvisited * chain_pages * spc_random_page_cost;
			costs.indexTotalCost += surcharge;
			/* First overflow read is part of fetching the first matching entry. */
			costs.indexStartupCost += chain_pages * spc_random_page_cost;
		}
	}

	*indexStartupCost = costs.indexStartupCost;
	*indexTotalCost = costs.indexTotalCost;
	*indexSelectivity = costs.indexSelectivity;
	*indexCorrelation = costs.indexCorrelation;
	*indexPages = costs.numIndexPages;
}

/*
 * Parse and validate the index's reloptions into a BarkOptions.
 */
static bytea *
barkoptions(Datum reloptions, bool validate)
{
	static const relopt_parse_elt tab[] = {
		{"fillfactor", RELOPT_TYPE_INT, offsetof(BarkOptions, fillfactor)},
	};

	return (bytea *) build_reloptions(reloptions, validate,
									  RELOPT_KIND_BARK,
									  sizeof(BarkOptions),
									  tab, lengthof(tab));
}

/*
 * Tree height for the planner's descent-cost charge: the level of the root,
 * zero for a single-page or empty index.  This reads the meta page each time
 * rather than caching it in rd_amcache as nbtree does; the planner calls it
 * once per index per plan, and BARK keeps no other meta page cache.
 */
static int
barkgettreeheight(Relation rel)
{
	uint32		level;

	(void) bark_get_root(rel, &level);
	return (int) level;
}

/*
 * Opclass validation lives in barkvalidate.c.
 */

static IndexScanDesc
barkbeginscan(Relation r, int nkeys, int norderbys)
{
	return bark_beginscan(r, nkeys, norderbys);
}

static void
barkrescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
		   ScanKey orderbys, int norderbys)
{
	bark_rescan(scan, scankey, nscankeys, orderbys, norderbys);
}

static bool
barkgettuple(IndexScanDesc scan, ScanDirection dir)
{
	return bark_gettuple(scan, dir);
}

static int64
barkgetbitmap(IndexScanDesc scan, TIDBitmap *tbm)
{
	return bark_getbitmap(scan, tbm);
}

static void
barkendscan(IndexScanDesc scan)
{
	bark_endscan(scan);
}

/*
 * BARK index access method handler.
 *
 * Returns an IndexAmRoutine that declares BARK's capabilities.  BARK is an
 * ordered index whose operator families are btree operator families (like
 * the btree AM's), it stores the heap-TID locator, and it answers ordered-
 * operator (KNN) scans -- ORDER BY col <~> const -- over its scalar key
 * (amcanorderbyop; see barkknn.c).
 */
Datum
barkhandler(PG_FUNCTION_ARGS)
{
	static const IndexAmRoutine amroutine = {
		.type = T_IndexAmRoutine,
		.amstrategies = 5,
		.amsupport = BARK_NPROCS,
		.amoptsprocnum = 0,
		.amcanorder = true,
		.ambtreeopfamilies = true,
		.amcanorderbyop = true,	/* KNN: ORDER BY col <~> const (see barkknn.c) */
		.amcanhash = false,
		.amconsistentequality = true,
		.amconsistentordering = true,
		.amcanbackward = true,
		.amcanunique = true,
		.amcanmulticol = true,
		.amoptionalkey = true,
		.amsearcharray = true,	/* ScalarArrayOp (SAOP): col = ANY(array), see barkscan.c */
		.amsearchnulls = true,
		.amstorage = false,
		.amclusterable = true,
		.ampredlocks = true,
		.amcanparallel = true,
		.amcanbuildparallel = true,
		.amcaninclude = true,
		.amusemaintenanceworkmem = false,
		.amsummarizing = false,
		.amcanlocators = LOCATOR_CAP_MASK(LOCATOR_CAP_TID),
		.amparallelvacuumoptions = VACUUM_OPTION_NO_PARALLEL,
		.amkeytype = InvalidOid,

		.ambuild = barkbuild,
		.ambuildempty = barkbuildempty,
		.aminsert = barkinsert,
		.aminsertcleanup = NULL,
		.ambulkdelete = barkbulkdelete,
		.amvacuumcleanup = barkvacuumcleanup,
		.amcanreturn = bark_canreturn,
		.amcostestimate = barkcostestimate,
		.amgettreeheight = barkgettreeheight,
		.amoptions = barkoptions,
		.amproperty = NULL,
		.ambuildphasename = NULL,
		.amvalidate = barkvalidate,
		.amadjustmembers = NULL,
		.ambeginscan = barkbeginscan,
		.amrescan = barkrescan,
		.amgettuple = barkgettuple,
		.amgetbitmap = barkgetbitmap,
		.amendscan = barkendscan,
		.ammarkpos = NULL,
		.amrestrpos = NULL,
		.amestimateparallelscan = bark_estimateparallelscan,
		.aminitparallelscan = bark_initparallelscan,
		.amparallelrescan = bark_parallelrescan,
		.amtranslatestrategy = bark_translate_strategy,
		.amtranslatecmptype = bark_translate_cmptype,
	};

	PG_RETURN_POINTER(&amroutine);
}
