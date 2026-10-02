/*-------------------------------------------------------------------------
 *
 * barkinsert.c
 *	  Insert into a BARK index: leaf insert and Lehman & Yao page split.
 *
 * bark_insert descends to the target leaf (bark_search), inserts the new
 * SINGLE-shape entry in key order, and -- when the page overflows -- splits
 * it: a new right page takes the upper half, the left page's high key becomes
 * the split key, the right link is published before the parent downlink, and
 * the downlink is inserted into the parent (growing a new root if the split
 * reached the top).  Page changes are made through the buffer pool and
 * WAL-logged with generic WAL (the same facility bloom uses), so no
 * BARK-specific WAL record is needed.
 *
 * This commit keeps every entry SINGLE and does not enforce uniqueness,
 * deduplicate, or recover an interrupted split; those arrive in later commits.
 * Modeled on nbtinsert.c (_bt_doinsert / _bt_insertonpg / _bt_split /
 * _bt_insert_parent / _bt_newroot).
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/bark/barkinsert.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bark.h"
#include "access/genam.h"
#include "access/generic_xlog.h"
#include "access/itup.h"
#include "access/tableam.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "utils/injection_point.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

/* Find the offset at which to insert key on a leaf page (first key > key). */
static OffsetNumber
bark_leaf_insert_off(Relation index, BarkKeyInfo *keyinfo, IndexTuple key,
					 Page page)
{
	BarkPageOpaque opaque = BarkPageGetOpaque(page);
	OffsetNumber low = BarkPageFirstDataKey(opaque);
	OffsetNumber high = PageGetMaxOffsetNumber(page);

	if (high < low)
		return low;

	high = OffsetNumberNext(high);
	while (low < high)
	{
		OffsetNumber mid = low + ((high - low) / 2);
		ItemId		iid = PageGetItemId(page, mid);
		IndexTuple	mitup = (IndexTuple) PageGetItem(page, iid);

		if (bark_compare_itups(keyinfo, index, key, mitup) >= 0)
			low = OffsetNumberNext(mid);
		else
			high = mid;
	}
	return low;
}

/*
 * Produce a clean key-only tuple (SINGLE shape, no appended body) from any
 * leaf entry.  A LIST or POSTING entry's key attributes sit at the front, just
 * like a SINGLE entry, but it carries extra body bytes and alt-TID status in
 * t_tid; a pivot formed from it must drop both.  index_truncate_tuple's
 * "easy case" (leavenatts == natts, i.e. a key-only index) would otherwise
 * copy the body verbatim, so reform the key attributes explicitly here.  For a
 * SINGLE or already-pivot source there is nothing extra to strip and we hand
 * the source back unchanged.
 */
static IndexTuple
bark_strip_to_key(Relation index, IndexTuple src, bool *allocated)
{
	*allocated = false;
	if (BarkEntryGetShape(src) == BARK_SHAPE_LIST ||
		BarkEntryGetShape(src) == BARK_SHAPE_POSTING)
	{
		TupleDesc	tupdesc = RelationGetDescr(index);
		Datum		values[INDEX_MAX_KEYS];
		bool		isnull[INDEX_MAX_KEYS];
		IndexTuple	key;

		index_deform_tuple(src, tupdesc, values, isnull);
		key = index_form_tuple(tupdesc, values, isnull);
		*allocated = true;
		return key;
	}
	return src;
}

/*
 * A pivot (downlink) tuple: a key truncated to its key attributes, carrying
 * natts + a child block.  Non-key INCLUDE attributes (and any lower-key
 * suffix) are physically removed with index_truncate_tuple so they do not
 * bloat internal pages or risk overflowing a pivot; pivots only route by key.
 */
static IndexTuple
bark_make_downlink(Relation index, IndexTuple key, BlockNumber child,
				   int nkeyatts)
{
	bool		allocated;
	IndexTuple	src = bark_strip_to_key(index, key, &allocated);
	IndexTuple	pivot = index_truncate_tuple(RelationGetDescr(index), src,
											 nkeyatts);

	if (allocated)
		pfree(src);
	BarkPivotSetNAtts(pivot, (uint16) nkeyatts);
	BarkPivotSetDownLink(pivot, child);
	return pivot;
}

/* A high-key tuple: a key truncated to its key attributes, no downlink. */
static IndexTuple
bark_make_hikey(Relation index, IndexTuple key, int nkeyatts)
{
	bool		allocated;
	IndexTuple	src = bark_strip_to_key(index, key, &allocated);
	IndexTuple	hikey = index_truncate_tuple(RelationGetDescr(index), src,
											 nkeyatts);

	if (allocated)
		pfree(src);
	BarkPivotSetNAtts(hikey, (uint16) nkeyatts);
	BarkPivotSetDownLink(hikey, BARK_P_NONE);
	return hikey;
}

/*
 * Insert `itup` at offset `off` on `page`, after `off` and later items have
 * been shifted up.  Caller has verified there is room.
 */
static void
bark_page_insert_at(Page page, IndexTuple itup, OffsetNumber off)
{
	if (PageAddItem(page, (char *) itup, IndexTupleSize(itup), off,
					false, false) == InvalidOffsetNumber)
		elog(ERROR, "failed to insert item into BARK page");
}

/*
 * Clear the BARK_INCOMPLETE_SPLIT flag on block `clearblk` as part of the
 * caller's generic-WAL record `gstate`.  Called when the downlink that makes
 * that page's right sibling reachable is being written, so the split becomes
 * complete atomically with the downlink insert.  Returns the locked buffer so
 * the caller can release it after GenericXLogFinish, or InvalidBuffer when
 * clearblk is BARK_P_NONE (an ordinary insert, not split recovery).
 */
static Buffer
bark_clear_incomplete_split(Relation index, GenericXLogState *gstate,
							BlockNumber clearblk)
{
	Buffer		cbuf;
	Page		cpage;

	if (clearblk == BARK_P_NONE)
		return InvalidBuffer;

	cbuf = ReadBuffer(index, clearblk);
	LockBuffer(cbuf, BUFFER_LOCK_EXCLUSIVE);
	cpage = GenericXLogRegisterBuffer(gstate, cbuf, 0);
	Assert((BarkPageGetOpaque(cpage)->bark_flags & BARK_INCOMPLETE_SPLIT) != 0);
	BarkPageGetOpaque(cpage)->bark_flags &= ~BARK_INCOMPLETE_SPLIT;
	return cbuf;
}

static void bark_insert_parent(Relation index, BarkKeyInfo *keyinfo,
							   BarkStack stack, IndexTuple downlink,
							   BlockNumber leftblk, BlockNumber rightblk);
static void bark_split(Relation index, BarkKeyInfo *keyinfo, BarkStack stack,
					   Buffer buf, OffsetNumber newoff, IndexTuple newitup,
					   BlockNumber clearblk);

/* Compare a downlink's key against a parent page's high key. */
static int
bark_compare_off_parent(Relation index, BarkKeyInfo *keyinfo,
						IndexTuple downlink, Page page)
{
	ItemId		iid = PageGetItemId(page, BARK_P_HIKEY);
	IndexTuple	hikey = (IndexTuple) PageGetItem(page, iid);

	return bark_compare_itups(keyinfo, index, downlink, hikey);
}

/* First offset on a parent page whose key is > the downlink's key. */
static OffsetNumber
bark_parent_insert_off(Relation index, BarkKeyInfo *keyinfo,
					   IndexTuple downlink, Page page)
{
	BarkPageOpaque opaque = BarkPageGetOpaque(page);
	OffsetNumber low = BarkPageFirstDataKey(opaque);
	OffsetNumber high = PageGetMaxOffsetNumber(page);

	if (high < low)
		return low;
	high = OffsetNumberNext(high);
	while (low < high)
	{
		OffsetNumber mid = low + ((high - low) / 2);
		ItemId		iid = PageGetItemId(page, mid);
		IndexTuple	mitup = (IndexTuple) PageGetItem(page, iid);

		if (bark_compare_itups(keyinfo, index, downlink, mitup) >= 0)
			low = OffsetNumberNext(mid);
		else
			high = mid;
	}
	return low;
}

/*
 * Create the first leaf of an empty index, holding `itup`, and point the meta
 * page at it as the (leaf) root.  Called when bark_search finds no root.
 */
static void
bark_insert_first_leaf(Relation index, IndexTuple itup)
{
	Buffer		metabuf = ReadBuffer(index, BARK_METAPAGE);
	Buffer		leafbuf;
	GenericXLogState *gstate;
	Page		leafpage;
	Page		metapage;
	BlockNumber leafblk;

	/*
	 * Serialize first-leaf creation on the meta page's exclusive lock so two
	 * backends inserting into a brand-new empty index cannot both create a
	 * root.  A second waiter re-checks bark_root after acquiring the lock.
	 */
	LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);
	if (BarkPageGetMeta(BufferGetPage(metabuf))->bark_root != BARK_P_NONE)
	{
		/* Someone else created the root; fall back to the normal path. */
		UnlockReleaseBuffer(metabuf);
		{
			BarkKeyInfo *keyinfo = bark_build_keyinfo(index);
			Buffer		buf;
			BarkStack	stack;
			Page		page;
			OffsetNumber off;

			buf = bark_search(index, keyinfo, itup, true, true, &stack);
			page = BufferGetPage(buf);
			off = bark_leaf_insert_off(index, keyinfo, itup, page);
			{
				GenericXLogState *g = GenericXLogStart(index);
				Page		p = GenericXLogRegisterBuffer(g, buf, 0);

				bark_page_insert_at(p, itup, off);
				GenericXLogFinish(g);
			}
			UnlockReleaseBuffer(buf);
			if (stack)
				bark_freestack(stack);
			pfree(keyinfo);
		}
		return;
	}

	leafbuf = ReadBuffer(index, P_NEW);
	LockBuffer(leafbuf, BUFFER_LOCK_EXCLUSIVE);
	leafblk = BufferGetBlockNumber(leafbuf);

	gstate = GenericXLogStart(index);
	leafpage = GenericXLogRegisterBuffer(gstate, leafbuf, GENERIC_XLOG_FULL_IMAGE);
	metapage = GenericXLogRegisterBuffer(gstate, metabuf, 0);

	PageInit(leafpage, BLCKSZ, sizeof(BarkPageOpaqueData));
	{
		BarkPageOpaque lo = BarkPageGetOpaque(leafpage);

		lo->bark_prev = BARK_P_NONE;
		lo->bark_next = BARK_P_NONE;
		lo->bark_level = 0;
		lo->bark_flags = BARK_LEAF | BARK_ROOT;
		lo->bark_page_id = BARK_PAGE_ID;
	}
	bark_page_insert_at(leafpage, itup, BARK_P_HIKEY);

	{
		BarkMetaPageData *meta = BarkPageGetMeta(metapage);

		meta->bark_root = leafblk;
		meta->bark_level = 0;
	}

	GenericXLogFinish(gstate);
	UnlockReleaseBuffer(leafbuf);
	UnlockReleaseBuffer(metabuf);
}

/*
 * Split `buf` (a full page) to make room for `newitup` at insert offset
 * `newoff`.  Allocates a right sibling, moves the upper half of the items to
 * it, sets the left page's high key to the right page's first key, chains the
 * right links, and inserts the right page's downlink into the parent via the
 * stack.  All modified pages are logged with one generic WAL record each as
 * they are finished; the right link is published before the parent downlink
 * so a concurrent descender can always move right to find a key.
 */
static void
bark_split(Relation index, BarkKeyInfo *keyinfo, BarkStack stack, Buffer buf,
		   OffsetNumber newoff, IndexTuple newitup, BlockNumber clearblk)
{
	int			nkeyatts = IndexRelationGetNumberOfKeyAttributes(index);
	Page		origpage = BufferGetPage(buf);
	BarkPageOpaque origopaque = BarkPageGetOpaque(origpage);
	bool		isleaf = BarkPageIsLeaf(origopaque);
	BlockNumber origblk = BufferGetBlockNumber(buf);
	BlockNumber origright = origopaque->bark_next;
	bool		origrightmost = BarkPageRightmost(origopaque);
	OffsetNumber firstdata = BarkPageFirstDataKey(origopaque);
	OffsetNumber maxoff = PageGetMaxOffsetNumber(origpage);

	/* Build the full ordered item list (existing items + the new one). */
	int			ntotal = (maxoff - firstdata + 1) + 1;
	IndexTuple *items = palloc(ntotal * sizeof(IndexTuple));
	int			n = 0;
	int			splitidx;
	IndexTuple	orighikey = NULL;
	Buffer		rbuf;
	Page		rightpage;
	BlockNumber rightblk;
	GenericXLogState *gstate;
	Page		leftpage;
	IndexTuple	splitkey;
	IndexTuple	downlink;

	/* Preserve the original high key (if any) for the new right page. */
	if (!origrightmost)
		orighikey = CopyIndexTuple((IndexTuple)
								   PageGetItem(origpage,
											   PageGetItemId(origpage,
															 BARK_P_HIKEY)));

	for (OffsetNumber off = firstdata; off <= maxoff; off = OffsetNumberNext(off))
	{
		if (off == newoff)
			items[n++] = CopyIndexTuple(newitup);
		items[n++] = CopyIndexTuple((IndexTuple)
									PageGetItem(origpage,
												PageGetItemId(origpage, off)));
	}
	if (newoff > maxoff)
		items[n++] = CopyIndexTuple(newitup);
	Assert(n == ntotal);

	/* Split roughly in half; the right page gets items[splitidx..]. */
	splitidx = n / 2;
	if (splitidx < 1)
		splitidx = 1;

	/* Allocate the right sibling. */
	rbuf = ReadBuffer(index, P_NEW);
	LockBuffer(rbuf, BUFFER_LOCK_EXCLUSIVE);
	rightblk = BufferGetBlockNumber(rbuf);

	gstate = GenericXLogStart(index);
	leftpage = GenericXLogRegisterBuffer(gstate, buf, GENERIC_XLOG_FULL_IMAGE);
	rightpage = GenericXLogRegisterBuffer(gstate, rbuf, GENERIC_XLOG_FULL_IMAGE);

	/* --- Rebuild the left page: high key = right's first key, lower half. --- */
	PageInit(leftpage, BLCKSZ, sizeof(BarkPageOpaqueData));
	{
		BarkPageOpaque lo = BarkPageGetOpaque(leftpage);

		lo->bark_prev = origopaque->bark_prev;
		lo->bark_next = rightblk;	/* right link to the new page */
		lo->bark_level = origopaque->bark_level;
		/*
		 * Mark the left page as having an unfinished split: its new right
		 * sibling exists and is right-linked, but the downlink that would make
		 * the sibling reachable from the parent is written in a separate step
		 * below.  A crash in between leaves the flag set; the next writer that
		 * descends here finishes the split (bark_finish_split).  The flag is
		 * cleared atomically with the downlink insert in bark_insert_parent.
		 */
		lo->bark_flags = (origopaque->bark_flags & ~BARK_ROOT) |
			BARK_INCOMPLETE_SPLIT;
		lo->bark_page_id = BARK_PAGE_ID;
	}
	splitkey = items[splitidx];		/* first key on the right page */
	{
		IndexTuple	lhikey = bark_make_hikey(index, splitkey, nkeyatts);
		OffsetNumber o = BARK_P_HIKEY;

		bark_page_insert_at(leftpage, lhikey, o++);
		pfree(lhikey);
		for (int i = 0; i < splitidx; i++)
			bark_page_insert_at(leftpage, items[i], o++);
	}

	/* --- Build the right page: original high key, upper half. --- */
	PageInit(rightpage, BLCKSZ, sizeof(BarkPageOpaqueData));
	{
		BarkPageOpaque ro = BarkPageGetOpaque(rightpage);

		ro->bark_prev = origblk;
		ro->bark_next = origright;
		ro->bark_level = origopaque->bark_level;
		ro->bark_flags = isleaf ? BARK_LEAF : 0;
		ro->bark_page_id = BARK_PAGE_ID;
	}
	{
		OffsetNumber o = BARK_P_HIKEY;

		if (!origrightmost)
			bark_page_insert_at(rightpage, orighikey, o++);	/* keep high key */
		for (int i = splitidx; i < n; i++)
			bark_page_insert_at(rightpage, items[i], o++);
	}

	/*
	 * If the original page had a right sibling, that sibling's bark_prev must
	 * now point at the new right page.  Register and fix it in the same WAL
	 * record.  If this split is finishing a child's incomplete split (clearblk
	 * set), the child's downlink is being written here, so clear the child's
	 * BARK_INCOMPLETE_SPLIT flag atomically in the same record.
	 */
	{
		Buffer		sbuf = InvalidBuffer;
		Buffer		cbuf;

		if (!origrightmost)
		{
			Page		spage;

			sbuf = ReadBuffer(index, origright);
			LockBuffer(sbuf, BUFFER_LOCK_EXCLUSIVE);
			spage = GenericXLogRegisterBuffer(gstate, sbuf, 0);
			BarkPageGetOpaque(spage)->bark_prev = rightblk;
		}
		cbuf = bark_clear_incomplete_split(index, gstate, clearblk);
		GenericXLogFinish(gstate);
		if (cbuf != InvalidBuffer)
			UnlockReleaseBuffer(cbuf);
		if (sbuf != InvalidBuffer)
			UnlockReleaseBuffer(sbuf);
	}

	/* The left (original) and right buffers are now consistent on disk. */
	UnlockReleaseBuffer(rbuf);

	/*
	 * Transfer predicate locks for serializable transactions: a read of the
	 * original page must now also conflict with inserts onto the new right
	 * page, since keys that were covered by one page's read are now split
	 * across both.
	 */
	PredicateLockPageSplit(index, origblk, rightblk);

	/* Form the downlink for the right page and insert it into the parent. */
	downlink = bark_make_downlink(index, splitkey, rightblk, nkeyatts);
	UnlockReleaseBuffer(buf);		/* release leaf before touching parent */

	/*
	 * The split is now durable but its downlink is not yet in the parent --
	 * the window a crash would leave as an incomplete split.  A test may stop
	 * here (via the injection point) to exercise bark_finish_split recovery.
	 */
#ifdef USE_INJECTION_POINTS
	if (isleaf)
		INJECTION_POINT("bark-leave-leaf-split-incomplete", NULL);
	else
		INJECTION_POINT("bark-leave-internal-split-incomplete", NULL);
#endif

	bark_insert_parent(index, keyinfo, stack, downlink, origblk, rightblk);
	pfree(downlink);

	/* Clean up. */
	for (int i = 0; i < n; i++)
		pfree(items[i]);
	pfree(items);
	if (orighikey)
		pfree(orighikey);
}

/*
 * Create a new root one level above `leftblk`/`rightblk` after a split
 * propagated to the top, and point the meta page at it.  The new root has two
 * entries: the left page's low key (as a minus-infinity downlink) and the
 * split key downlink to the right page.
 */
static void
bark_new_root(Relation index, IndexTuple downlink, BlockNumber leftblk,
			  BlockNumber rightblk, uint32 childlevel)
{
	int			nkeyatts = IndexRelationGetNumberOfKeyAttributes(index);
	Buffer		rootbuf = ReadBuffer(index, P_NEW);
	Buffer		metabuf;
	GenericXLogState *gstate;
	Page		rootpage;
	Page		metapage;
	BlockNumber rootblk;
	IndexTuple	leftdown;

	LockBuffer(rootbuf, BUFFER_LOCK_EXCLUSIVE);
	rootblk = BufferGetBlockNumber(rootbuf);
	metabuf = ReadBuffer(index, BARK_METAPAGE);
	LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);

	gstate = GenericXLogStart(index);
	rootpage = GenericXLogRegisterBuffer(gstate, rootbuf, GENERIC_XLOG_FULL_IMAGE);
	metapage = GenericXLogRegisterBuffer(gstate, metabuf, 0);

	PageInit(rootpage, BLCKSZ, sizeof(BarkPageOpaqueData));
	{
		BarkPageOpaque ro = BarkPageGetOpaque(rootpage);

		ro->bark_prev = BARK_P_NONE;
		ro->bark_next = BARK_P_NONE;
		ro->bark_level = childlevel + 1;
		ro->bark_flags = BARK_ROOT;
		ro->bark_page_id = BARK_PAGE_ID;
	}

	/*
	 * First downlink is minus-infinity (zero key attributes): it routes every
	 * key below the split key to the left child.  Second is the split-key
	 * downlink to the right child.
	 */
	leftdown = index_truncate_tuple(RelationGetDescr(index), downlink, 0);
	BarkPivotSetNAtts(leftdown, 0);
	BarkPivotSetDownLink(leftdown, leftblk);
	bark_page_insert_at(rootpage, leftdown, BARK_P_HIKEY);
	pfree(leftdown);
	{
		IndexTuple	rightdown = bark_make_downlink(index, downlink, rightblk, nkeyatts);

		bark_page_insert_at(rootpage, rightdown, BARK_P_FIRSTKEY);
		pfree(rightdown);
	}

	/* Point the meta page at the new root. */
	{
		BarkMetaPageData *meta = BarkPageGetMeta(metapage);

		meta->bark_root = rootblk;
		meta->bark_level = childlevel + 1;
	}

	/*
	 * The left child's downlink now exists (as the minus-infinity entry), so
	 * clear its incomplete-split flag in the same record.  ponytail: locks the
	 * child under the root while BARK is single-writer; revisit for
	 * concurrency (P-series).
	 */
	{
		Buffer		cbuf = bark_clear_incomplete_split(index, gstate, leftblk);

		GenericXLogFinish(gstate);
		if (cbuf != InvalidBuffer)
			UnlockReleaseBuffer(cbuf);
	}
	UnlockReleaseBuffer(metabuf);
	UnlockReleaseBuffer(rootbuf);
}

/*
 * Insert `downlink` (pointing at the just-created right page) into the parent
 * recorded on the stack.  If the stack is empty the split was at the root, so
 * a new root is created.  If the parent itself overflows this recurses as a
 * split one level up.
 */
static void
bark_insert_parent(Relation index, BarkKeyInfo *keyinfo, BarkStack stack,
				   IndexTuple downlink, BlockNumber leftblk,
				   BlockNumber rightblk)
{
	Buffer		pbuf;
	Page		ppage;
	BarkPageOpaque popaque;
	OffsetNumber off;
	Size		itemsz = MAXALIGN(IndexTupleSize(downlink));

	if (stack == NULL)
	{
		/* Split reached the root: grow a new level. */
		uint32		childlevel;
		Buffer		lb = ReadBuffer(index, leftblk);

		LockBuffer(lb, BUFFER_LOCK_SHARE);
		childlevel = BarkPageGetOpaque(BufferGetPage(lb))->bark_level;
		UnlockReleaseBuffer(lb);
		bark_new_root(index, downlink, leftblk, rightblk, childlevel);
		return;
	}

	pbuf = ReadBuffer(index, stack->bark_blkno);
	LockBuffer(pbuf, BUFFER_LOCK_EXCLUSIVE);
	ppage = BufferGetPage(pbuf);
	popaque = BarkPageGetOpaque(ppage);

	/*
	 * Find where the new downlink's key belongs on the parent.  The parent
	 * may itself have split since we descended; move right until the key is
	 * not past the high key.
	 */
	while (!BarkPageRightmost(popaque) &&
		   bark_compare_off_parent(index, keyinfo, downlink, ppage) > 0)
	{
		BlockNumber right = popaque->bark_next;

		UnlockReleaseBuffer(pbuf);
		pbuf = ReadBuffer(index, right);
		LockBuffer(pbuf, BUFFER_LOCK_EXCLUSIVE);
		ppage = BufferGetPage(pbuf);
		popaque = BarkPageGetOpaque(ppage);
	}

	off = bark_parent_insert_off(index, keyinfo, downlink, ppage);

	if (PageGetFreeSpace(ppage) >= itemsz)
	{
		/*
		 * Fits: insert the downlink and log the parent.  The downlink is now
		 * durably reachable, so clear the left child's incomplete-split flag in
		 * the same WAL record.
		 *
		 * ponytail: locks the child (leftblk) while holding the parent, i.e.
		 * down the tree -- safe while BARK has no concurrent inserters; revisit
		 * the lock order when concurrency lands (P-series).
		 */
		GenericXLogState *gstate = GenericXLogStart(index);
		Page		p = GenericXLogRegisterBuffer(gstate, pbuf, 0);
		Buffer		cbuf;

		bark_page_insert_at(p, downlink, off);
		cbuf = bark_clear_incomplete_split(index, gstate, leftblk);
		GenericXLogFinish(gstate);
		if (cbuf != InvalidBuffer)
			UnlockReleaseBuffer(cbuf);
		UnlockReleaseBuffer(pbuf);
	}
	else
	{
		/* Parent is full: split it, carrying the stack one level up.  The
		 * parent split writes this downlink, so it clears leftblk's flag. */
		bark_split(index, keyinfo, stack->bark_parent, pbuf, off, downlink,
				   leftblk);
	}
}

/*
 * Finish a split that was interrupted (by a crash) after the right sibling was
 * published but before its downlink reached the parent: the left page `lbuf`
 * carries BARK_INCOMPLETE_SPLIT.  Reconstruct the missing downlink from the
 * left page's high key -- which equals the right sibling's first key, i.e. the
 * split key -- pointing at the right sibling, and insert it into the parent.
 * bark_insert_parent clears the flag atomically with that insert.
 *
 * `lbuf` is write-locked on entry and released here.  `stack` is the parent
 * path to `lbuf` from the current descent.
 */
static void
bark_finish_split(Relation index, BarkKeyInfo *keyinfo, Buffer lbuf,
				  BarkStack stack)
{
	int			nkeyatts = IndexRelationGetNumberOfKeyAttributes(index);
	Page		lpage = BufferGetPage(lbuf);
	BarkPageOpaque lopaque = BarkPageGetOpaque(lpage);
	BlockNumber lblk = BufferGetBlockNumber(lbuf);
	BlockNumber rblk = lopaque->bark_next;
	ItemId		hiid = PageGetItemId(lpage, BARK_P_HIKEY);
	IndexTuple	hikey = (IndexTuple) PageGetItem(lpage, hiid);
	IndexTuple	downlink;

	Assert((lopaque->bark_flags & BARK_INCOMPLETE_SPLIT) != 0);
	Assert(!BarkPageRightmost(lopaque));	/* has a right sibling */

	INJECTION_POINT("bark-finish-incomplete-split", NULL);

	/* The high key is the split key; make a downlink to the right sibling. */
	downlink = bark_make_downlink(index, hikey, rblk, nkeyatts);
	UnlockReleaseBuffer(lbuf);
	bark_insert_parent(index, keyinfo, stack, downlink, lblk, rblk);
	pfree(downlink);
}

/*
 * Check whether inserting `itup` would violate a unique constraint.
 *
 * Scans forward from the first possibly-equal entry on `buf` (continuing into
 * right siblings while keys stay equal) and, for every index entry whose key
 * equals itup's, fetches the referenced heap tuple under SnapshotDirty.  A
 * visible or in-progress match is a conflict.
 *
 *  - UNIQUE_CHECK_EXISTING skips the entry that is itup itself (the tuple is
 *    already in the heap; we are only verifying that it is unique).
 *  - A conflict with an in-progress transaction returns that xact's id so the
 *    caller can wait for it and retry; *is_unique is left false.
 *  - UNIQUE_CHECK_PARTIAL never errors: on any conflict it sets *is_unique to
 *    false and returns, letting a deferred constraint recheck decide later.
 *  - Otherwise a live conflict raises ERRCODE_UNIQUE_VIOLATION.
 *
 * Returns InvalidTransactionId when no wait is needed (unique, or already
 * errored).  The caller holds the write lock on `buf` throughout and still
 * holds it on return.
 *
 * ponytail: no speculative-insertion (INSERT ... ON CONFLICT) handling and no
 * killing of known-dead index entries yet; both are optimizations layered on
 * the correct check here.
 */
static TransactionId
bark_check_unique(Relation index, BarkKeyInfo *keyinfo, IndexTuple itup,
				  Buffer buf, Relation heapRel, IndexUniqueCheck checkUnique,
				  bool *is_unique)
{
	SnapshotData SnapshotDirty;
	Buffer		curbuf = buf;
	bool		ownbuf = false;		/* do we need to release curbuf? */

	*is_unique = true;
	InitDirtySnapshot(SnapshotDirty);

	/*
	 * The scan below only moves right from the insert leaf (where the insert
	 * descent, nextkey=true, lands -- the leaf holding the position just past
	 * the last key equal to itup's).  This finds every conflicting entry
	 * because of how BARK inserts: a new entry for a key always goes at the
	 * END of that key's run, so any existing live entry for the same key sits
	 * at or after the first equal entry on the insert leaf and is reachable by
	 * scanning right.  Dead, not-yet-vacuumed duplicates may extend the run
	 * left across earlier leaves, but the single live survivor cannot be left
	 * of the insert leaf's first equal entry.
	 *
	 * ponytail: this relies on the insert descent using nextkey=true.  If the
	 * insert positioning ever changes so the live entry could land strictly
	 * left of the descent leaf, this check must first walk left to the first
	 * leaf of the equal-key run (or descend the check with nextkey=false).
	 */
	for (;;)
	{
		Page		page = BufferGetPage(curbuf);
		BarkPageOpaque opaque = BarkPageGetOpaque(page);
		OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
		OffsetNumber lo = BarkPageFirstDataKey(opaque);
		OffsetNumber hi = OffsetNumberNext(maxoff);
		OffsetNumber off;
		bool		go_right = false;

		/*
		 * Binary-search for the first entry whose key is >= itup's.  (The leaf
		 * insert position is one past the last *equal* key, so it would skip
		 * the duplicates we are looking for.)
		 */
		while (lo < hi)
		{
			OffsetNumber mid = lo + ((hi - lo) / 2);
			ItemId		iid = PageGetItemId(page, mid);
			IndexTuple	mitup = (IndexTuple) PageGetItem(page, iid);

			if (bark_compare_itups(keyinfo, index, itup, mitup) > 0)
				lo = OffsetNumberNext(mid);	/* mid < itup: go right */
			else
				hi = mid;					/* mid >= itup: go left */
		}

		for (off = lo; off <= maxoff; off = OffsetNumberNext(off))
		{
			ItemId		iid = PageGetItemId(page, off);
			IndexTuple	curitup = (IndexTuple) PageGetItem(page, iid);
			ItemPointerData htid;
			bool		all_dead = false;

			/* Stop at the first key greater than itup's: no more equal keys. */
			if (bark_compare_itups(keyinfo, index, itup, curitup) != 0)
				goto done;

			htid = curitup->t_tid;

			/* The tuple we are checking is itself, not a conflict. */
			if (checkUnique == UNIQUE_CHECK_EXISTING &&
				ItemPointerCompare(&htid, &itup->t_tid) == 0)
				continue;

			if (table_fetch_tid(heapRel, &htid, &SnapshotDirty, &all_dead))
			{
				TransactionId xwait;

				/* Deferred check: record non-uniqueness, don't error. */
				if (checkUnique == UNIQUE_CHECK_PARTIAL)
				{
					if (ownbuf)
						UnlockReleaseBuffer(curbuf);
					*is_unique = false;
					return InvalidTransactionId;
				}

				/*
				 * If the conflicting tuple is still being inserted or deleted,
				 * return the responsible xact so the caller can wait and retry.
				 */
				xwait = TransactionIdIsValid(SnapshotDirty.xmin) ?
					SnapshotDirty.xmin : SnapshotDirty.xmax;
				if (TransactionIdIsValid(xwait))
				{
					if (ownbuf)
						UnlockReleaseBuffer(curbuf);
					return xwait;
				}

				/* A committed, visible duplicate: raise the constraint error. */
				{
					Datum		values[INDEX_MAX_KEYS];
					bool		isnull[INDEX_MAX_KEYS];
					char	   *key_desc;

					if (ownbuf)
						UnlockReleaseBuffer(curbuf);

					index_deform_tuple(itup, RelationGetDescr(index),
									   values, isnull);
					key_desc = BuildIndexValueDescription(index, values, isnull);
					ereport(ERROR,
							(errcode(ERRCODE_UNIQUE_VIOLATION),
							 errmsg("duplicate key value violates unique constraint \"%s\"",
									RelationGetRelationName(index)),
							 key_desc ? errdetail("Key %s already exists.",
												   key_desc) : 0,
							 errtableconstraint(heapRel,
													RelationGetRelationName(index))));
				}
			}
			/* else: the heap tuple is dead to everyone; not a conflict. */
		}

		/*
		 * Ran off the end of this page while keys were still equal: equal keys
		 * may continue on the right sibling, so follow the right link.
		 */
		if (!BarkPageRightmost(opaque))
			go_right = true;

		if (!go_right)
			break;
		{
			BlockNumber right = opaque->bark_next;
			Buffer		next = ReadBuffer(index, right);

			LockBuffer(next, BUFFER_LOCK_SHARE);
			if (ownbuf)
				UnlockReleaseBuffer(curbuf);
			curbuf = next;
			ownbuf = true;
		}
	}

done:
	if (ownbuf)
		UnlockReleaseBuffer(curbuf);
	return InvalidTransactionId;
}

/*
 * Try to coalesce `newtid` into an existing leaf entry on `buf` that has the
 * same key as `key` (a SINGLE-shape key tuple), forming or extending a LIST,
 * rather than adding another SINGLE entry.  Returns true and performs the
 * replacement (WAL-logged) when it coalesced; returns false (page unchanged)
 * when there is no equal entry, or when the resulting LIST would be too large
 * -- in which case the caller inserts a plain SINGLE and the duplicates stay
 * as separate entries.
 *
 * Only called for non-unique indexes: a unique index never legitimately holds
 * two live tuples with the same key, so it never forms a LIST.  `off` is the
 * leaf insert position (one past the last entry <= key), so the candidate
 * equal entry, if any, is at off-1.
 *
 * ponytail: the size ceiling is BarkMaxItemSize (~1/3 page).  A key with more
 * duplicates than fit in one LIST keeps the overflow as separate entries
 * until A11 promotes the run to a POSTING set; both are correct, LIST is just
 * the compact form up to the ceiling.
 */
static bool
bark_coalesce_list(Relation index, BarkKeyInfo *keyinfo, IndexTuple key,
				   ItemPointer newtid, Buffer buf, OffsetNumber off)
{
	Page		page = BufferGetPage(buf);
	BarkPageOpaque opaque = BarkPageGetOpaque(page);
	OffsetNumber firstdata = BarkPageFirstDataKey(opaque);
	OffsetNumber eqoff;
	ItemId		iid;
	IndexTuple	cur;
	ItemPointerData tids[MaxOffsetNumber];
	int			nold;
	int			nnew;
	int			ins;
	IndexTuple	list;
	Size		listsz;

	/* No entry precedes the insert point: nothing to coalesce with. */
	if (off <= firstdata)
		return false;
	eqoff = OffsetNumberPrev(off);
	iid = PageGetItemId(page, eqoff);
	cur = (IndexTuple) PageGetItem(page, iid);

	/* Only coalesce with a leaf-data entry whose key equals the new key. */
	if (!BarkEntryIsLeafData(cur) ||
		bark_compare_itups(keyinfo, index, key, cur) != 0)
		return false;

	/* Gather the existing locators plus the new one, in ascending order. */
	nold = bark_entry_get_tids(cur, tids, MaxOffsetNumber);
	if (nold >= BARK_LIST_MAX_COUNT)
		return false;			/* count field is full: keep separate */

	/* Insert newtid keeping the array sorted and distinct. */
	for (ins = 0; ins < nold; ins++)
	{
		int			c = ItemPointerCompare(newtid, &tids[ins]);

		if (c == 0)
			return true;		/* already present (should not happen): done */
		if (c < 0)
			break;
	}
	memmove(&tids[ins + 1], &tids[ins],
			(nold - ins) * sizeof(ItemPointerData));
	tids[ins] = *newtid;
	nnew = nold + 1;

	/* Build the candidate LIST and check it against the page item ceiling. */
	list = bark_form_list(RelationGetDescr(index), key, tids, nnew);
	listsz = MAXALIGN(IndexTupleSize(list));
	if (listsz > BarkMaxItemSize)
	{
		pfree(list);
		return false;			/* too big for one entry: keep separate */
	}

	/*
	 * The LIST replaces the old entry.  Removing the old entry and adding the
	 * larger LIST must fit: the net growth is listsz minus the old item's
	 * size.  PageGetFreeSpace plus the reclaimed old slot must cover it.
	 */
	if (PageGetFreeSpace(page) + MAXALIGN(ItemIdGetLength(iid)) < listsz)
	{
		pfree(list);
		return false;			/* no room to grow here: caller splits */
	}

	{
		GenericXLogState *gstate = GenericXLogStart(index);
		Page		p = GenericXLogRegisterBuffer(gstate, buf, 0);
		OffsetNumber deloff = eqoff;

		PageIndexMultiDelete(p, &deloff, 1);
		if (PageAddItem(p, (char *) list, IndexTupleSize(list), eqoff,
						false, false) == InvalidOffsetNumber)
			elog(ERROR, "failed to replace BARK leaf entry with a list");
		GenericXLogFinish(gstate);
	}

	pfree(list);
	return true;
}

bool
bark_insert(Relation index, Datum *values, bool *isnull, ItemPointer ht_ctid,
			Relation heapRel, IndexUniqueCheck checkUnique,
			bool indexUnchanged, IndexInfo *indexInfo)
{
	BarkKeyInfo *keyinfo = bark_build_keyinfo(index);
	IndexTuple	itup = index_form_tuple(RelationGetDescr(index), values, isnull);
	Buffer		buf;
	BarkStack	stack;
	Page		page;
	OffsetNumber off;
	Size		itemsz = MAXALIGN(IndexTupleSize(itup));
	bool		result = false;		/* significant only for UNIQUE_CHECK_PARTIAL */

	itup->t_tid = *ht_ctid;		/* SINGLE shape: locator in t_tid */

retry:
	buf = bark_search(index, keyinfo, itup, true, true, &stack);

	if (buf == InvalidBuffer)
	{
		/* Empty index: create the first leaf and point the meta page at it. */
		bark_insert_first_leaf(index, itup);
		pfree(itup);
		pfree(keyinfo);
		return true;			/* nothing to conflict with: unique */
	}

	/*
	 * If this leaf has an unfinished split (a crash left its right sibling
	 * without a parent downlink), complete it before inserting, then descend
	 * again: the parent now has the missing downlink and the key may belong on
	 * the right sibling.
	 */
	if ((BarkPageGetOpaque(BufferGetPage(buf))->bark_flags &
		 BARK_INCOMPLETE_SPLIT) != 0)
	{
		bark_finish_split(index, keyinfo, buf, stack);	/* releases buf */
		if (stack)
			bark_freestack(stack);
		goto retry;
	}

	page = BufferGetPage(buf);
	off = bark_leaf_insert_off(index, keyinfo, itup, page);

	/*
	 * Serializable conflict check: inserting here conflicts with a concurrent
	 * serializable transaction that read this leaf page.  BARK sets
	 * ampredlocks, so this is our responsibility rather than the generic
	 * index layer's.  Done while holding the write lock on the target leaf,
	 * before the insert or split.
	 */
	CheckForSerializableConflictIn(index, NULL, BufferGetBlockNumber(buf));

	/*
	 * Uniqueness check.  Skipped when the caller doesn't want it, and when the
	 * new key has any NULL attribute (SQL treats NULLs as distinct, so a NULL
	 * key never conflicts).  If a conflicting tuple is still in progress,
	 * bark_check_unique returns its xact id: wait for that transaction to
	 * finish, then re-descend and check again.
	 */
	if (checkUnique != UNIQUE_CHECK_NO)
	{
		bool		nulls_present = false;
		TransactionId xwait;
		bool		is_unique;

		for (int i = 0; i < IndexRelationGetNumberOfKeyAttributes(index); i++)
		{
			if (isnull[i])
			{
				nulls_present = true;
				break;
			}
		}

		if (!nulls_present)
		{
			xwait = bark_check_unique(index, keyinfo, itup, buf, heapRel,
									  checkUnique, &is_unique);
			if (TransactionIdIsValid(xwait))
			{
				/* Conflict with an in-progress xact: wait and retry. */
				UnlockReleaseBuffer(buf);
				if (stack)
					bark_freestack(stack);
				XactLockTableWait(xwait, index, &itup->t_tid,
								  XLTW_InsertIndex);
				goto retry;
			}
			result = is_unique;

			/*
			 * UNIQUE_CHECK_EXISTING only verifies that the already-inserted
			 * tuple is unique; it must not add another index entry.
			 */
			if (checkUnique == UNIQUE_CHECK_EXISTING)
			{
				UnlockReleaseBuffer(buf);
				if (stack)
					bark_freestack(stack);
				pfree(itup);
				pfree(keyinfo);
				return result;
			}
		}
		else
		{
			/* NULL key: unconditionally considered unique. */
			result = true;
		}
	}

	/*
	 * Non-unique index: coalesce the new locator into an existing equal-key
	 * entry, forming or extending a LIST instead of adding another SINGLE.
	 * A unique index never does this -- it would mean two live tuples with the
	 * same key, which the uniqueness check above already rejected.
	 */
	if (!indexInfo->ii_Unique &&
		bark_coalesce_list(index, keyinfo, itup, &itup->t_tid, buf, off))
	{
		UnlockReleaseBuffer(buf);
		if (stack)
			bark_freestack(stack);
		pfree(itup);
		pfree(keyinfo);
		return result;
	}

	if (PageGetFreeSpace(page) >= itemsz)
	{
		GenericXLogState *gstate = GenericXLogStart(index);
		Page		p = GenericXLogRegisterBuffer(gstate, buf, 0);

		bark_page_insert_at(p, itup, off);
		GenericXLogFinish(gstate);
		UnlockReleaseBuffer(buf);
	}
	else
	{
		/* Leaf split: no child below, so no incomplete-split flag to clear. */
		bark_split(index, keyinfo, stack, buf, off, itup, BARK_P_NONE);
		buf = InvalidBuffer;	/* bark_split released it */
	}

	if (stack)
		bark_freestack(stack);
	pfree(itup);
	pfree(keyinfo);
	return result;
}
