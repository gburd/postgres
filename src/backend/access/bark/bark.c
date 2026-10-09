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
#include "access/barkxlog.h"
#include "access/nbtree.h"
#include "access/reloptions.h"
#include "access/xloginsert.h"
#include "commands/vacuum.h"
#include "miscadmin.h"
#include "optimizer/optimizer.h"
#include "storage/bufmgr.h"
#include "storage/indexfsm.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "storage/procarray.h"
#include "utils/fmgrprotos.h"
#include "utils/injection_point.h"
#include "utils/lsyscache.h"
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
		Buffer		lbuf = bark_search(index, keyinfo, childhikey, NULL, false, false,
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
			BarkItemBuf ibuf;
			IndexTuple	itup = BarkPageGetItem(ppage, off, &ibuf);

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
 * Delete an empty leaf page from the tree.
 *
 * Reclaims the common case a delete-heavy workload produces: an interior leaf
 * (one with both a left and a right sibling) whose every entry VACUUM removed.
 * The page is unlinked from the leaf chain (its left sibling's right link and
 * its right sibling's left link are spliced across it), its key space passes
 * to its right sibling in the parent, and the page becomes a deleted page
 * (BarkPageSetDeleted).  All four touched pages (left sibling, target, right
 * sibling, parent) are updated under one XLOG_BARK_UNLINK_PAGE record so the
 * unlink is crash-atomic.
 *
 * The deleted page keeps its sibling links, as in nbtree, so a scan or
 * descent that read a link to it before the deletion moves right through it.
 * It is not handed to the FSM here: *safexid is set to the transaction ID it
 * must age past before reuse, and the caller records the page in the FSM once
 * that is safe (see bark_vacuum_scan).
 *
 * Returns true when the leaf was deleted.  Declines (returns false, leaving the
 * leaf linked and correct) when the page is not an eligible interior empty
 * leaf, or when it is its parent's last child, so that its right sibling's
 * downlink is not on the same parent page.
 *
 * This reclaims interior empty leaves only.  A leftmost or rightmost empty
 * leaf, an empty leaf that is its parent's last child, and an emptied
 * internal page are all left linked in place -- correct, and still reusable
 * once their siblings are rewritten, just not directly unlinked here.
 *
 * It locks the parent first and then the left sibling, target and right
 * sibling, which is the reverse of the insert path's order (an inserter that
 * splits a leaf keeps the leaf locked until it has locked the parent).  To
 * avoid a deadlock between the two, the three leaf locks are only tried
 * conditionally while the parent is held, and the leaf is skipped if any of
 * them is busy; a later VACUUM retries it.  nbtree avoids the inversion by
 * locking the leaf level before the parent (_bt_mark_page_halfdead).
 */
static bool
bark_delete_empty_leaf(Relation index, BarkKeyInfo *keyinfo, BlockNumber blkno,
					   FullTransactionId *safexid)
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
	Page		lpage;
	Page		rpage;
	Page		ppage;
	IndexTuple	downlink;
	BarkItemBuf ibuf;
	XLogRecPtr	recptr;
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
	 * The right sibling must be the target's next child in the same parent:
	 * its downlink is the one this deletion removes (see below).  Decline
	 * when the target is the parent's last child, as nbtree does for the
	 * rightmost child of a parent.
	 */
	if (downoff >= PageGetMaxOffsetNumber(BufferGetPage(pbuf)))
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
		BarkEntryGetDownLink(BarkPageGetItem(BufferGetPage(pbuf), downoff,
											 &ibuf)) != blkno ||
		BarkEntryGetDownLink(BarkPageGetItem(BufferGetPage(pbuf),
											 OffsetNumberNext(downoff),
											 &ibuf)) != rightblk ||
		(BarkPageGetOpaque(BufferGetPage(rbuf))->bark_flags &
		 (BARK_DELETED | BARK_HALF_DEAD)) != 0)
	{
		UnlockReleaseBuffer(rbuf);
		UnlockReleaseBuffer(buf);
		UnlockReleaseBuffer(lbuf);
		UnlockReleaseBuffer(pbuf);
		return false;
	}

	/*
	 * The target's key space moves right, to its right sibling, as in
	 * nbtree's _bt_mark_page_halfdead: point the target's downlink at the
	 * right sibling and delete the right sibling's own downlink, the next
	 * item.  The parent then routes the target's whole range to the right
	 * sibling, whose keys all sort at or above that range.  Dropping the
	 * target's downlink instead would route the range to the left sibling,
	 * whose high key does not cover it, and an insert there would move right
	 * onto the right sibling below that page's downlink.
	 */
	PredicateLockPageCombine(index, blkno, rightblk);

	/*
	 * Splice the target out of the chain, move its key space, and mark it
	 * deleted.  safexid is read while all four pages are locked, as nbtree's
	 * _bt_unlink_halfdead_page does: a backend that read a link to the target
	 * before we locked these pages took its snapshot before now, so its xmin
	 * is no later than safexid, and a backend that reads a link after we
	 * unlock finds the new links.  Once no snapshot as old as safexid exists,
	 * nobody can hold a link to the page.
	 */
	*safexid = ReadNextFullTransactionId();
	lpage = BufferGetPage(lbuf);
	rpage = BufferGetPage(rbuf);
	ppage = BufferGetPage(pbuf);

	/* No ereport(ERROR) until changes are logged */
	START_CRIT_SECTION();

	BarkPageGetOpaque(lpage)->bark_next = rightblk;
	BarkPageGetOpaque(rpage)->bark_prev = leftblk;
	/* Changed in place: internal items are always stored as they are read. */
	downlink = (IndexTuple) PageGetItem(ppage, PageGetItemId(ppage, downoff));
	BarkEntrySetDownLink(downlink, rightblk);
	PageIndexTupleDelete(ppage, OffsetNumberNext(downoff));
	BarkPageSetDeleted(page, leftblk, rightblk, *safexid);

	MarkBufferDirty(lbuf);
	MarkBufferDirty(buf);
	MarkBufferDirty(rbuf);
	MarkBufferDirty(pbuf);

	if (RelationNeedsWAL(index))
	{
		xl_bark_unlink_page xlrec;

		xlrec.leftsib = leftblk;
		xlrec.rightsib = rightblk;
		xlrec.safexid = *safexid;
		xlrec.poffset = downoff;

		/*
		 * The target is rebuilt from the record in redo, so it needs no
		 * image, as in _bt_unlink_halfdead_page.
		 */
		XLogBeginInsert();
		XLogRegisterBuffer(0, buf, REGBUF_WILL_INIT);
		XLogRegisterBuffer(1, lbuf, REGBUF_STANDARD);
		XLogRegisterBuffer(2, rbuf, REGBUF_STANDARD);
		XLogRegisterBuffer(3, pbuf, REGBUF_STANDARD);
		XLogRegisterData(&xlrec, SizeOfBarkUnlinkPage);

		recptr = XLogInsert(RM_BARK_ID, XLOG_BARK_UNLINK_PAGE);
	}
	else
		recptr = XLogGetFakeLSN(index);

	PageSetLSN(lpage, recptr);
	PageSetLSN(page, recptr);
	PageSetLSN(rpage, recptr);
	PageSetLSN(ppage, recptr);

	END_CRIT_SECTION();

	UnlockReleaseBuffer(rbuf);
	UnlockReleaseBuffer(buf);
	UnlockReleaseBuffer(lbuf);
	UnlockReleaseBuffer(pbuf);
	return true;
}

/*
 * Every build of an index (CREATE INDEX, REINDEX, CONCURRENTLY, and before
 * ambuildempty for an unlogged one) comes through here, so this is where an
 * index BARK cannot hold is refused.  Refusing UNIQUE and exclusion
 * constraints also covers ON CONFLICT, whose arbiters are such indexes.
 */
static IndexBuildResult *
barkbuild(Relation heap, Relation index, IndexInfo *indexInfo)
{
	bark_check_multikey_index(index, indexInfo);
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
 * Apply VACUUM's changes to leaf `buf` and WAL-log them as XLOG_BARK_VACUUM:
 * rewrite the entries at updatedoffsets with `updated` (each no larger than
 * the entry it replaces, and already coded for the page), delete the entries at `deletable`, and clear the
 * vacuum cycle ID.  As nbtree's _bt_delitems_vacuum, the rewrites come first,
 * since PageIndexTupleOverwrite keeps offsets stable and the deletion
 * renumbers them.  `buf` is cleanup-locked.  Redo (bark_xlog_vacuum) makes
 * the same changes in the same order.
 */
static void
bark_delitems_vacuum(Relation index, Buffer buf,
					 OffsetNumber *deletable, int ndeletable,
					 OffsetNumber *updatedoffsets, IndexTuple *updated,
					 int nupdated)
{
	Page		page = BufferGetPage(buf);
	bool		needswal = RelationNeedsWAL(index);
	char	   *updatedbuf = NULL;
	Size		updatedbuflen = 0;
	XLogRecPtr	recptr;

	/*
	 * Gather the new entries into one buffer for the WAL record, as
	 * _bt_delitems_update does, each padded to MAXALIGN so that redo can step
	 * from one to the next.
	 */
	if (needswal && nupdated > 0)
	{
		for (int i = 0; i < nupdated; i++)
			updatedbuflen += MAXALIGN(IndexTupleSize(updated[i]));
		updatedbuf = palloc0(updatedbuflen);
		updatedbuflen = 0;
		for (int i = 0; i < nupdated; i++)
		{
			memcpy(updatedbuf + updatedbuflen, updated[i],
				   IndexTupleSize(updated[i]));
			updatedbuflen += MAXALIGN(IndexTupleSize(updated[i]));
		}
	}

	/* No ereport(ERROR) until changes are logged */
	START_CRIT_SECTION();

	for (int i = 0; i < nupdated; i++)
	{
		if (!PageIndexTupleOverwrite(page, updatedoffsets[i], updated[i],
									 IndexTupleSize(updated[i])))
			elog(PANIC, "failed to rewrite BARK leaf entry at offset %u of block %u of index \"%s\"",
				 updatedoffsets[i], BufferGetBlockNumber(buf),
				 RelationGetRelationName(index));
	}
	if (ndeletable > 0)
		PageIndexMultiDelete(page, deletable, ndeletable);
	BarkPageGetOpaque(page)->bark_cycleid = 0;

	MarkBufferDirty(buf);

	if (needswal)
	{
		xl_bark_vacuum xlrec;

		xlrec.ndeleted = ndeletable;
		xlrec.nupdated = nupdated;

		XLogBeginInsert();
		XLogRegisterBuffer(0, buf, REGBUF_STANDARD);
		XLogRegisterData(&xlrec, SizeOfBarkVacuum);
		if (ndeletable > 0)
			XLogRegisterBufData(0, deletable,
								ndeletable * sizeof(OffsetNumber));
		if (nupdated > 0)
		{
			XLogRegisterBufData(0, updatedoffsets,
								nupdated * sizeof(OffsetNumber));
			XLogRegisterBufData(0, updatedbuf, updatedbuflen);
		}

		recptr = XLogInsert(RM_BARK_ID, XLOG_BARK_VACUUM);
	}
	else
		recptr = XLogGetFakeLSN(index);

	PageSetLSN(page, recptr);

	END_CRIT_SECTION();

	if (updatedbuf != NULL)
		pfree(updatedbuf);
}

/*
 * Should bark_vacuum_page clean this page?  Only live leaf pages hold heap
 * TIDs: the meta page, internal pages, overflow pages and pages deleted from
 * the tree are skipped (bark_vacuum_page counts deleted pages and returns
 * them to the FSM before asking).  A page reached by
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
 * State of one VACUUM scan of a BARK index (bark_vacuum_scan), as nbtree's
 * BTVacState.  callback is NULL for a scan that only counts entries, deletes
 * empty leaves and recycles deleted pages (barkvacuumcleanup when no bulk
 * delete ran).
 */
typedef struct BarkVacState
{
	IndexVacuumInfo *info;
	IndexBulkDeleteResult *stats;
	IndexBulkDeleteCallback callback;
	void	   *callback_state;
	BTCycleId	cycleid;
	BlockNumber *emptyleaves;	/* empty interior leaves to delete */
	int			nempty;
	int			emptyalloc;
} BarkVacState;

/*
 * Process one block of bark_vacuum_scan's physical-order scan.  On a live
 * leaf, delete the entries whose heap TIDs the callback reports dead (when
 * there is a callback), count the heap TIDs left, and remember the leaf if it
 * is now an empty interior leaf, for bark_vacuum_scan to delete.  On a deleted
 * page, count it, and record it in the FSM if it is safe to reuse, as
 * btvacuumpage does.
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
bark_vacuum_page(BarkVacState *vstate, BlockNumber scanblkno)
{
	IndexVacuumInfo *info = vstate->info;
	IndexBulkDeleteResult *stats = vstate->stats;
	IndexBulkDeleteCallback callback = vstate->callback;
	void	   *callback_state = vstate->callback_state;
	BTCycleId	cycleid = vstate->cycleid;
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
		int			nlivetids = 0;
		OffsetNumber updatedoffsets[MaxOffsetNumber];
		IndexTuple	updated[MaxOffsetNumber];
		int			nupdated = 0;
		BlockNumber oversized_free[MaxOffsetNumber];
		int			noversized_free = 0;
		bool		clearcycleid;
		BlockNumber backtrack_to = BARK_P_NONE;

		buf = ReadBufferExtended(index, MAIN_FORKNUM, blkno, RBM_NORMAL,
								 info->strategy);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);

		/* A page deleted from the tree: count it, and recycle it if safe. */
		if (blkno == scanblkno && !PageIsNew(page) &&
			BarkPageIsDeleted(BarkPageGetOpaque(page)))
		{
			stats->pages_deleted++;
			if (BarkPageIsRecyclable(page, info->heaprel))
			{
				RecordFreeIndexPage(index, blkno);
				stats->pages_free++;
			}
			UnlockReleaseBuffer(buf);
			break;
		}

		if (!bark_vacuum_target(page, blkno != scanblkno, cycleid))
		{
			UnlockReleaseBuffer(buf);
			break;
		}

		/*
		 * With entries to delete, trade the share lock for a cleanup lock, as
		 * nbtree's _bt_upgradelockbufcleanup does, and look at the page
		 * again: it was unlocked in between.  A scan that only counts keeps
		 * the share lock.
		 */
		if (callback != NULL)
		{
			LockBuffer(buf, BUFFER_LOCK_UNLOCK);
			LockBufferForCleanup(buf);
			page = BufferGetPage(buf);
			if (!bark_vacuum_target(page, blkno != scanblkno, cycleid))
			{
				UnlockReleaseBuffer(buf);
				break;
			}
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
		 * Decide what to do to the page before changing it, so that the
		 * changes and their WAL record can be made in one critical section,
		 * as _bt_delitems_vacuum does.  Fully-dead entries (SINGLE or
		 * OVERSIZED whose TID is dead, LIST or POSTING whose every member is
		 * dead) are collected for deletion; LIST and POSTING entries that
		 * lost some members are re-formed with the survivors and collected
		 * as updates.
		 */
		for (OffsetNumber off = BarkPageFirstDataKey(opaque);
			 callback != NULL && off <= maxoff; off = OffsetNumberNext(off))
		{
			BarkItemBuf ibuf;
			IndexTuple	itup = BarkPageGetItem(page, off, &ibuf);

			/* A marker names no row; it is never removed. */
			if (BarkEntryIsMarker(itup))
				continue;
			if (BarkEntryGetShape(itup) == BARK_SHAPE_SINGLE)
			{
				if (callback(&itup->t_tid, callback_state))
				{
					todelete[ndelete++] = off;
					ndelete_single++;
				}
				else
					nlivetids++;
			}
			else if (BarkEntryGetShape(itup) == BARK_SHAPE_OVERSIZED)
			{
				/*
				 * An OVERSIZED entry holds exactly one heap locator (inline in
				 * its ref).  If it is dead, delete the leaf entry and remember
				 * its overflow chain, to free once the leaf no longer
				 * references it (bark_free_oversized locks and logs each chain
				 * page in turn, so it does not run under the leaf's lock).
				 */
				ItemPointerData loc = BarkOverflowGetRef(itup)->locator;

				if (callback(&loc, callback_state))
				{
					oversized_free[noversized_free++] =
						BarkOverflowGetFirstBlock(itup);
					todelete[ndelete++] = off;
					ndelete_single++;
				}
				else
					nlivetids++;
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
				nlivetids += nlive;

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

				/* Rebuild with the surviving members (bark_reform_entry). */
				updatedoffsets[nupdated] = off;
				updated[nupdated++] = bark_reform_entry(index, buf, off, itup,
														tids, nlive);
				pfree(tids);
			}
		}

		/*
		 * Clear our cycle ID from a page split during this VACUUM, so that a
		 * later backtrack stops here instead of cleaning the page again.
		 * nbtree does this as an unlogged hint.  BARK logs it, in the page's
		 * XLOG_BARK_VACUUM record, and bark_mask does not mask the field, so
		 * every BARK page stays byte-identical between primary and standby
		 * (see "WAL" in the README).  A page that has nothing to delete gets
		 * a record only when it has a cycle ID to clear, so a VACUUM that
		 * finds nothing to do logs nothing.
		 */
		clearcycleid = (cycleid != 0 && opaque->bark_cycleid == cycleid);

		if (ndelete > 0 || nupdated > 0 || clearcycleid)
			bark_delitems_vacuum(index, buf, todelete, ndelete,
								 updatedoffsets, updated, nupdated);
		stats->tuples_removed += ndelete_single;
		for (int i = 0; i < nupdated; i++)
			pfree(updated[i]);

		/*
		 * Count the heap TIDs that remain, as btvacuumpage does: the planner
		 * reads the count as the index's rows, in the same unit as the
		 * heap's, and barkvacuumcleanup stores it as bark_nkeys.  Markers are
		 * not counted.  A scan that only counts reads each entry's count of
		 * TIDs for itself.
		 */
		if (callback != NULL)
			stats->num_index_tuples += nlivetids;
		else
		{
			for (OffsetNumber off = BarkPageFirstDataKey(opaque);
				 off <= maxoff; off = OffsetNumberNext(off))
			{
				BarkItemBuf ibuf;
				IndexTuple	itup = BarkPageGetItem(page, off, &ibuf);

				if (!BarkEntryIsMarker(itup))
					stats->num_index_tuples += bark_entry_count_tids(itup);
			}
		}

		/*
		 * An empty interior leaf (no data entries, both siblings, not
		 * half-dead or mid-split) is a candidate for deletion, which
		 * bark_vacuum_scan does once the scan is over: it needs the parent
		 * and both siblings locked, so not under this page's lock.
		 */
		if ((opaque->bark_flags & (BARK_INCOMPLETE_SPLIT | BARK_HALF_DEAD)) == 0 &&
			!BarkPageIsRoot(opaque) &&
			!BarkPageLeftmost(opaque) && !BarkPageRightmost(opaque) &&
			PageGetMaxOffsetNumber(page) < BarkPageFirstDataKey(opaque))
		{
			if (vstate->nempty >= vstate->emptyalloc)
			{
				vstate->emptyalloc *= 2;
				vstate->emptyleaves = (BlockNumber *)
					repalloc(vstate->emptyleaves,
							 vstate->emptyalloc * sizeof(BlockNumber));
			}
			vstate->emptyleaves[vstate->nempty++] = blkno;
		}

		UnlockReleaseBuffer(buf);

		/*
		 * Reclaim the overflow chains of the OVERSIZED entries just deleted,
		 * now that the leaf no longer references them and its WAL record is
		 * logged.  Each chain page is freed under its own WAL record.
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
			stats->pages_newly_deleted += bark_free_oversized(index, stub);
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

/*
 * A leaf page deleted by this VACUUM and its safexid, kept so that the pages
 * can be put in the FSM at the end of the VACUUM, once nothing can still
 * hold a link to them.  As nbtree's BTPendingFSM.
 */
typedef struct BarkPendingFSM
{
	BlockNumber target;			/* page deleted by this VACUUM */
	FullTransactionId safexid;	/* its BarkDeletedPageData.safexid */
} BarkPendingFSM;

/*
 * Record in the FSM the pages this VACUUM deleted that are already safe to
 * reuse, as nbtree's _bt_pendingfsm_finalize does.  The rest stay deleted
 * and unrecorded; a later VACUUM finds them in its page walk and records
 * them once BarkPageIsRecyclable says so.
 */
static void
bark_pendingfsm_finalize(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
						 BarkPendingFSM *pending, int npending)
{
	if (npending == 0)
		return;

	/*
	 * Recompute this backend's view of the XID horizon.  We do not need the
	 * result; computing it updates the state GlobalVisCheckRemovableFullXid
	 * consults, which otherwise would not recognize that pages deleted after
	 * this VACUUM took its snapshot may be safe to reuse already.
	 */
	GetOldestNonRemovableTransactionId(info->heaprel);

	for (int i = 0; i < npending; i++)
	{
		/*
		 * The pages were deleted in this order, so their safexids do not
		 * decrease: once one page is not yet recyclable, neither is any later
		 * one.
		 */
		Assert(i == 0 ||
			   FullTransactionIdFollowsOrEquals(pending[i].safexid,
												pending[i - 1].safexid));
		if (!GlobalVisCheckRemovableFullXid(info->heaprel, pending[i].safexid))
			break;

		RecordFreeIndexPage(info->index, pending[i].target);
		stats->pages_free++;
	}
}

/*
 * Scan the whole index once, in physical order, as nbtree's btvacuumscan does:
 * clean every live leaf (bark_vacuum_page), count the leaf entries and the
 * deleted pages, return safe deleted pages to the FSM, and then delete the
 * empty interior leaves the scan left behind.
 *
 * With a callback (barkbulkdelete) this is the whole of the VACUUM's work on
 * the index, and barkvacuumcleanup has nothing left to do; without one
 * (barkvacuumcleanup when no bulk delete ran this cycle) it only counts,
 * deletes and recycles.
 *
 * Leaf splits during the scan are detected with a vacuum cycle ID, as in
 * btbulkdelete.  The cycle IDs come from nbtree's registry (_bt_start_vacuum
 * and friends): it maps a relation's LockRelId to the cycle ID of the VACUUM
 * now scanning it and has nothing nbtree-specific in it, so BARK shares it
 * instead of keeping a copy.  It could move out of nbtree into a common
 * place.  The ENSURE block releases our registry entry if the scan fails.  A
 * scan without a callback deletes no entries, so it needs no cycle ID, as
 * btvacuumscan's cleanup-only scan does not.
 *
 * Parallel VACUUM may run this in a worker; each index is still processed by
 * one backend at a time, so nothing here is shared with another backend
 * beyond what serial VACUUM already shares (the cycle-ID registry, buffer
 * locks, WAL and the FSM).
 */
static void
bark_vacuum_scan(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
				 IndexBulkDeleteCallback callback, void *callback_state)
{
	Relation	index = info->index;
	BarkVacState vstate;
	bool		needLock = !RELATION_IS_LOCAL(index);
	BlockNumber blkno = BARK_METAPAGE + 1;
	BlockNumber npages;
	BarkKeyInfo *keyinfo;
	BarkPendingFSM *pending;
	int			npending = 0;

	vstate.info = info;
	vstate.stats = stats;
	vstate.callback = callback;
	vstate.callback_state = callback_state;
	vstate.cycleid = 0;
	vstate.emptyalloc = 64;
	vstate.nempty = 0;
	vstate.emptyleaves = palloc_array(BlockNumber, vstate.emptyalloc);

	/*
	 * The counts are recomputed by every scan; tuples_removed and
	 * pages_newly_deleted accumulate across the bulk deletes of one VACUUM.
	 */
	stats->num_index_tuples = 0;
	stats->pages_deleted = 0;
	stats->pages_free = 0;

	/*
	 * Recompute this backend's view of the XID horizon, so that pages
	 * deleted after the VACUUM took its snapshot can be recognized as safe to
	 * reuse (see bark_pendingfsm_finalize).
	 */
	GetOldestNonRemovableTransactionId(info->heaprel);

	PG_ENSURE_ERROR_CLEANUP(_bt_end_vacuum_callback, PointerGetDatum(index));
	{
		if (callback != NULL)
			vstate.cycleid = _bt_start_vacuum(index);

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
				bark_vacuum_page(&vstate, blkno);
			}
		}
	}
	PG_END_ENSURE_ERROR_CLEANUP(_bt_end_vacuum_callback, PointerGetDatum(index));
	if (callback != NULL)
		_bt_end_vacuum(index);

	stats->num_pages = npages;

	/*
	 * Delete the empty interior leaves the scan found.  Each deletion
	 * re-validates the page under exclusive locks, so a leaf that was
	 * concurrently refilled or already reclaimed is simply skipped.  The
	 * deleted pages are kept in `pending`, in deletion order, for the FSM.
	 */
	keyinfo = bark_build_keyinfo(index);
	keyinfo->heaprel = info->heaprel;
	pending = palloc_array(BarkPendingFSM, Max(vstate.nempty, 1));
	for (int i = 0; i < vstate.nempty; i++)
	{
		FullTransactionId safexid;

		if (bark_delete_empty_leaf(index, keyinfo, vstate.emptyleaves[i],
								   &safexid))
		{
			pending[npending].target = vstate.emptyleaves[i];
			pending[npending].safexid = safexid;
			npending++;
			stats->pages_newly_deleted++;
			stats->pages_deleted++;
		}
	}
	pfree(keyinfo);
	pfree(vstate.emptyleaves);

	bark_pendingfsm_finalize(info, stats, pending, npending);
	pfree(pending);

	/* Make the FSM entries recorded this cycle durable and searchable. */
	if (stats->pages_free > 0)
		IndexFreeSpaceMapVacuum(index);
}

static IndexBulkDeleteResult *
barkbulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
			   IndexBulkDeleteCallback callback, void *callback_state)
{
	if (stats == NULL)
		stats = palloc0_object(IndexBulkDeleteResult);

	bark_vacuum_scan(info, stats, callback, callback_state);
	return stats;
}

/*
 * After a bulk delete there is little left to do: bark_vacuum_scan already
 * counted the live heap TIDs, deleted the empty leaves and recycled the
 * deleted pages.  Without one (no dead tuples this cycle), scan the index to
 * do those things, so that pages deleted by an earlier VACUUM reach the FSM
 * once they are safe.  That scan takes no cleanup locks, so concurrent
 * splits can make it count an entry twice, and its count is only an
 * estimate: VACUUM keeps the index's reltuples, as with btvacuumcleanup.
 * Either way the count is stored as the meta page's bark_nkeys.  Returning valid stats also lets VACUUM set the heap
 * visibility map, which is what makes index-only scans worthwhile.  This is
 * btvacuumcleanup without its skip-the-scan heuristic.
 */
static IndexBulkDeleteResult *
barkvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
	/* ANALYZE has nothing to clean up. */
	if (info->analyze_only)
		return stats;

	Assert(info->heaprel != NULL);

	if (stats == NULL)
	{
		stats = palloc0_object(IndexBulkDeleteResult);
		bark_vacuum_scan(info, stats, NULL, NULL);
		stats->estimated_count = true;
	}

	/*
	 * Concurrent splits can make the scan count some entries twice, so
	 * disbelieve a total above the heap's, when that one is exact, as
	 * btvacuumcleanup does.  An index with an extracted column has a member
	 * per key of a row, so its total is not bounded by the heap's.
	 */
	if (!info->estimated_count &&
		bark_index_extracted_column(info->index) == 0 &&
		stats->num_index_tuples > info->num_heap_tuples)
		stats->num_index_tuples = info->num_heap_tuples;

	/*
	 * The members left are the K of the cost estimate of an index with an
	 * extracted column ("Statistics" in BARK-Design.mediawiki), stored once
	 * per VACUUM.
	 */
	bark_set_nkeys(info->index, (uint64) stats->num_index_tuples);

	return stats;
}

/*
 * Would a scan with these quals skip over column 1 (bark_skip_eligible)?
 * That needs a forward scan, no qual on column 1 (the clauses are in column
 * order) and a qual on column 2 that bounds it: a plain operator, or an
 * inequality ScalarArrayOp, which rescan reduces to one.
 */
static bool
bark_cost_skips(IndexPath *path)
{
	IndexOptInfo *index = path->indexinfo;
	ListCell   *lc;

	if (index->nkeycolumns < 2 || path->indexclauses == NIL ||
		ScanDirectionIsBackward(path->indexscandir) ||
		linitial_node(IndexClause, path->indexclauses)->indexcol != 1)
		return false;

	foreach(lc, path->indexclauses)
	{
		IndexClause *iclause = lfirst_node(IndexClause, lc);

		if (iclause->indexcol != 1)
			break;
		foreach_node(RestrictInfo, rinfo, iclause->indexquals)
		{
			Expr	   *clause = rinfo->clause;

			if (IsA(clause, OpExpr))
				return true;
			if (IsA(clause, ScalarArrayOpExpr) &&
				get_op_opfamily_strategy(((ScalarArrayOpExpr *) clause)->opno,
										 index->opfamily[1]) != BTEqualStrategyNumber)
				return true;
		}
	}
	return false;
}

/*
 * barkcostestimate -- estimate the cost of a BARK index scan.
 *
 * This is btcostestimate, applied to the quals as a BARK scan uses them.
 * Only the quals that position and stop the scan (the boundary quals)
 * decide how many entries it reads; the rest only filter.  As in nbtree, a
 * column's quals bound the scan only when every earlier column has an
 * equality.  BARK uses fewer quals than nbtree does (bark_make_bound and
 * bark_past_bound):
 *
 *  - IS NULL and IS NOT NULL never bound the scan.
 *  - An equality ScalarArrayOp bounds it like any equality, in either
 *    direction, and the scan descends once per combination of the arrays'
 *    elements, as nbtree's does.  When a column has several equalities, the
 *    scan walks the one with the fewest elements (a plain equality has one),
 *    so that is the column's factor in the number of descents.  An
 *    inequality ScalarArrayOp is reduced to a plain key at rescan.
 *  - A row comparison ends the bound.  Past column 1 it bounds only where
 *    the scan starts, not where it stops, and not a skip scan's groups.
 *  - A skip scan descends once per column-1 value, and only column 2's
 *    plain quals bound it (bark_cost_skips).
 *  - An ordered-operator (KNN) scan walks outward from its constant and
 *    tests every qual as a filter.
 *
 * The number of entries read is counted in heap rows, as nbtree counts index
 * tuples after deduplication: LIST and POSTING entries make the index
 * smaller, and genericcostestimate prorates the scan's pages over the
 * index's pages, so the smaller index costs fewer page reads.  The descent
 * charge, the primitive-scan clamp and the correlation (from the leading
 * column's statistics, or the expression index's own) are btcostestimate's.
 *
 * An oversized entry's full value lives on an overflow chain that the scan
 * reads in addition to the leaf page.  The chain length is estimated from
 * the index's average bytes per row beyond what a leaf slot holds, and each
 * entry read is charged a random page read per chain page.  That is the
 * expected charge for an index whose entries are uniformly large and zero
 * for one with none; a mixed index is charged the average, since the index
 * keeps no count of its oversized entries.
 */
static void
barkcostestimate(PlannerInfo *root, IndexPath *path, double loop_count,
				 Cost *indexStartupCost, Cost *indexTotalCost,
				 Selectivity *indexSelectivity, double *indexCorrelation,
				 double *indexPages)
{
	IndexOptInfo *index = path->indexinfo;
	GenericCosts costs = {0};
	VariableStatData vardata = {0};
	List	   *indexBoundQuals = NIL;
	bool		forward = !ScanDirectionIsBackward(path->indexscandir);
	int			indexcol = 0;
	bool		eqQualHere = false;
	bool		lastcol = false;
	bool		found_array = false;
	bool		skipping = false;
	double		colelems = 1;	/* fewest elements of an equality on indexcol */
	double		num_sa_scans = 1;
	double		numIndexTuples;
	double		correlation = 0.0;
	Cost		descentCost;
	ListCell   *lc;

	examine_indexcol_variable(root, index, 0, &vardata);
	if (HeapTupleIsValid(vardata.statsTuple))
		correlation = btcost_correlation(index, &vardata);

	/*
	 * A skip scan reads column 1's groups one primitive scan at a time.  As
	 * in btcostestimate, count one per distinct column-1 value plus one to
	 * find the first, and assume skipping does not pay when that is more than
	 * the index has pages or the number of values is only a guess.
	 */
	if (path->indexorderbys == NIL && bark_cost_skips(path))
	{
		bool		isdefault;
		double		ndistinct;

		ndistinct = get_variable_numdistinct(&vardata, &isdefault) + 1;
		if (!isdefault && ndistinct <= index->pages)
		{
			num_sa_scans = ndistinct;
			indexcol = 1;
			lastcol = true;
			found_array = true;
			skipping = true;
		}
	}
	ReleaseVariableStats(vardata);

	foreach(lc, path->indexclauses)
	{
		IndexClause *iclause = lfirst_node(IndexClause, lc);

		if (path->indexorderbys != NIL)
			break;
		if (iclause->indexcol != indexcol)
		{
			if (lastcol || !eqQualHere || iclause->indexcol != indexcol + 1)
				break;
			indexcol++;
			eqQualHere = false;
			num_sa_scans *= colelems;
			colelems = 1;
		}

		foreach_node(RestrictInfo, rinfo, iclause->indexquals)
		{
			Expr	   *clause = rinfo->clause;
			Oid			clause_op;
			int			strategy;

			if (IsA(clause, OpExpr))
				clause_op = ((OpExpr *) clause)->opno;
			else if (IsA(clause, ScalarArrayOpExpr))
				clause_op = ((ScalarArrayOpExpr *) clause)->opno;
			else if (IsA(clause, RowCompareExpr))
			{
				RowCompareExpr *rc = (RowCompareExpr *) clause;
				bool		lower = (rc->cmptype == COMPARE_GT ||
									 rc->cmptype == COMPARE_GE) !=
					index->reverse_sort[indexcol];

				/*
				 * Past column 1, it only says where the scan starts, and a
				 * skip scan does not use it.
				 */
				if (indexcol > 0 && (lower != forward || skipping))
					continue;
				clause_op = linitial_oid(rc->opnos);
				lastcol = true;
			}
			else if (IsA(clause, NullTest))
				continue;
			else
				elog(ERROR, "unsupported indexqual type: %d",
					 (int) nodeTag(clause));

			strategy = get_op_opfamily_strategy(clause_op,
												index->opfamily[indexcol]);
			Assert(strategy != 0);
			if (IsA(clause, ScalarArrayOpExpr) &&
				strategy == BTEqualStrategyNumber)
			{
				ScalarArrayOpExpr *saop = (ScalarArrayOpExpr *) clause;
				double		alength;

				/* A skip scan's column-2 arrays only filter. */
				if (skipping)
					continue;
				alength = Max(estimate_array_length(root, lsecond(saop->args)),
							  1);
				colelems = eqQualHere ? Min(colelems, alength) : alength;
				found_array = true;
			}
			else if (strategy == BTEqualStrategyNumber)
				colelems = 1;
			if (strategy == BTEqualStrategyNumber)
				eqQualHere = true;
			indexBoundQuals = lappend(indexBoundQuals, rinfo);
		}
	}
	num_sa_scans *= colelems;

	/*
	 * A unique index with an equality on every column returns at most one
	 * row, as in btcostestimate.
	 */
	if (index->unique && indexcol == index->nkeycolumns - 1 && eqQualHere &&
		!found_array)
		numIndexTuples = 1.0;
	else
	{
		List	   *selectivityQuals;

		selectivityQuals = add_predicate_to_index_quals(index, indexBoundQuals);
		numIndexTuples = clauselist_selectivity(root, selectivityQuals,
												index->rel->relid,
												JOIN_INNER, NULL) *
			index->rel->tuples;

		/*
		 * An array scan reads on along the leaf level when the next
		 * combination starts on the page it is reading, so it cannot descend
		 * more than once per page: clamp as btcostestimate does.
		 */
		num_sa_scans = Min(num_sa_scans, ceil(index->pages * 0.3333333));
		num_sa_scans = Max(num_sa_scans, 1);
		numIndexTuples = rint(numIndexTuples / num_sa_scans);
	}

	/* Count only the meta page as non-leaf, as btcostestimate does. */
	costs.numIndexTuples = numIndexTuples;
	costs.num_sa_scans = num_sa_scans;
	costs.numNonLeafPages = 1;
	genericcostestimate(root, path, loop_count, &costs);

	/*
	 * Charge each descent about log2(N) comparisons and 50 operator costs per
	 * level, the leaf included, as btcostestimate does.  Descents after the
	 * first are not startup cost.
	 */
	if (index->tuples > 1)
	{
		descentCost = ceil(log(index->tuples) / log(2.0)) * cpu_operator_cost;
		costs.indexStartupCost += descentCost;
		costs.indexTotalCost += costs.num_sa_scans * descentCost;
	}
	descentCost = (index->tree_height + 1) * DEFAULT_PAGE_CPU_MULTIPLIER *
		cpu_operator_cost;
	costs.indexStartupCost += descentCost;
	costs.indexTotalCost += costs.num_sa_scans * descentCost;

	/* The overflow-chain surcharge. */
	if (index->tuples > 0 && index->pages > 0)
	{
		double		avg_entry_bytes = (double) index->pages * BLCKSZ / index->tuples;

		if (avg_entry_bytes > BarkMaxItemSize)
		{
			double		chain_pages = ceil((avg_entry_bytes - BarkMaxItemSize) /
										   BarkOverflowChunkSize);
			double		spc_random_page_cost;

			get_tablespace_page_costs(index->reltablespace,
									  &spc_random_page_cost, NULL);
			costs.indexTotalCost += costs.numIndexTuples * costs.num_sa_scans *
				chain_pages * spc_random_page_cost;
			/* The first entry's chain is read before its first row returns. */
			costs.indexStartupCost += chain_pages * spc_random_page_cost;
		}
	}

	*indexStartupCost = costs.indexStartupCost;
	*indexTotalCost = costs.indexTotalCost;
	*indexSelectivity = costs.indexSelectivity;
	*indexCorrelation = correlation;
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
		{"prefix_compression", RELOPT_TYPE_BOOL,
		offsetof(BarkOptions, prefix_compression)},
	};

	return (bytea *) build_reloptions(reloptions, validate,
									  RELOPT_KIND_BARK,
									  sizeof(BarkOptions),
									  tab, lengthof(tab));
}

/*
 * Tree height for the planner's descent-cost charge: the level of the root,
 * zero for a single-page or empty index.  Taken from the root cache, as
 * nbtree's _bt_getrootheight takes it from its meta page cache.
 */
static int
barkgettreeheight(Relation rel)
{
	return (int) bark_get_root_level(rel);
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
		.amstrategies = 0,		/* any int2: a multikey family numbers its own */
		.amsupport = BARK_MULTIKEY_NPROCS,
		.amoptsprocnum = BARK_OPTIONS_PROC,
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
		.amstorage = true,		/* a multikey class stores its keys' type */
		.amclusterable = true,
		.ampredlocks = true,
		.amcanparallel = true,
		.amcanbuildparallel = true,
		.amcaninclude = true,
		.amusemaintenanceworkmem = false,
		.amsummarizing = false,
		.amcanlocators = LOCATOR_CAP_MASK(LOCATOR_CAP_TID),
		.amparallelvacuumoptions =
		VACUUM_OPTION_PARALLEL_BULKDEL | VACUUM_OPTION_PARALLEL_COND_CLEANUP,
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
		.amadjustmembers = barkadjustmembers,
		.ambeginscan = barkbeginscan,
		.amrescan = barkrescan,
		.amgettuple = barkgettuple,
		.amgetbitmap = barkgetbitmap,
		.amendscan = barkendscan,
		.ammarkpos = bark_markpos,
		.amrestrpos = bark_restrpos,
		.amestimateparallelscan = bark_estimateparallelscan,
		.aminitparallelscan = bark_initparallelscan,
		.amparallelrescan = bark_parallelrescan,
		.amtranslatestrategy = bark_translate_strategy,
		.amtranslatecmptype = bark_translate_cmptype,
	};

	PG_RETURN_POINTER(&amroutine);
}
