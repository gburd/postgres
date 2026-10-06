/*-------------------------------------------------------------------------
 *
 * barkinsert.c
 *	  Insert into a BARK index: leaf insert and Lehman & Yao page split.
 *
 * bark_insert descends to the target leaf (bark_search), inserts the new
 * SINGLE-shape entry in key order, and -- when the page overflows -- splits
 * it: a new right page takes the items above a split point chosen by
 * bark_findsplitloc (barksplitloc.c), the left page gets a new high key (on a
 * leaf, the right page's first key without the attributes not needed to tell
 * it from the left page's last key), the right link is published before the
 * parent downlink, and a copy of the high key is inserted into the parent as
 * the right page's downlink (growing a new root if the split reached the
 * top).  Page changes are made through the buffer pool and WAL-logged with
 * generic WAL (the same facility bloom uses); BARK's own WAL records cover
 * only VACUUM and page deletion so far (see "WAL" in the README).
 *
 * Locks follow nbtree's protocol: a split keeps the left page write-locked
 * until the parent is write-locked and the new downlink written, the parent's
 * downlink to the left page is found by its block number (bark_getstackbuf),
 * and locks are always taken child before parent.  A split interrupted by an
 * error or crash is finished by the next writer whose descent reaches its
 * left page (bark_finish_split, called from bark_search's move-right step).
 * Modeled on nbtinsert.c (_bt_doinsert /
 * _bt_insertonpg / _bt_split / _bt_insert_parent / _bt_getstackbuf /
 * _bt_finish_split / _bt_newlevel).
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
#include "access/nbtree.h"
#include "access/tableam.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "utils/injection_point.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

/*
 * Materialize the page-resident SINGLE/OVERSIZED leaf entry for `full` (a full
 * in-memory key tuple carrying its heap locator).  When `oversized`, writes the
 * full tuple to a fresh overflow chain and returns a small OVERSIZED entry
 * referencing it; otherwise returns a plain copy that is placed inline exactly
 * as before this capability.  Always returns a palloc'd tuple the caller
 * places and then pfrees.
 */
static IndexTuple
bark_leaf_page_entry(Relation index, Relation heaprel, IndexTuple full,
					 bool oversized, Size fulllen)
{
	BlockNumber firstblk;

	if (!oversized)
		return CopyIndexTuple(full);

	firstblk = bark_write_overflow_chain(index, heaprel, full, fulllen);
	{
		IndexTuple	entry = bark_form_oversized_entry(&full->t_tid, fulllen,
														 firstblk, true /* leaf */ , 0);

		bark_set_oversized_prefix(entry, index, full);
		return entry;
	}
}

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
	BarkEntryShape shape = BarkEntryGetShape(src);

	*allocated = false;
	if (shape == BARK_SHAPE_OVERSIZED)
	{
		/* The key attributes are out of line; fetch the full tuple. */
		*allocated = true;
		return bark_fetch_oversized(index, src);
	}
	if (shape == BARK_SHAPE_LIST || shape == BARK_SHAPE_POSTING)
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
 * Build a high key from the leaf entry `key`, keeping its first `keepnatts`
 * key attributes.  Non-key INCLUDE attributes and the key attributes after
 * the first keepnatts are physically removed: pivots only route by key.
 *
 * When the truncated key still exceeds the item ceiling (an oversized key),
 * the pivot is an OVERSIZED pivot: its full key is written to a fresh overflow
 * chain the pivot owns, and the pivot is the small OVERSIZED entry referencing
 * it.  bark_compare_itups fetches that chain, so an oversized pivot routes on
 * the full key exactly as a leaf entry does.  The caller must not hold an open
 * generic-WAL state, since writing the chain starts its own WAL records.
 * heaprel is the index's heap, for bark_get_free_page.
 */
static IndexTuple
bark_make_pivot(Relation index, Relation heaprel, IndexTuple key,
				int keepnatts)
{
	bool		allocated;
	IndexTuple	src = bark_strip_to_key(index, key, &allocated);
	TupleDesc	tupdesc = RelationGetDescr(index);
	Datum		values[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS];
	Size		fulllen;
	IndexTuple	full;
	IndexTuple	pivot;

	/*
	 * Form the truncated key with bark_form_full_tuple (no 8191 cap) so an
	 * oversized key does not error here; a normal key comes out identical to
	 * index_truncate_tuple's result.
	 */
	if (keepnatts < tupdesc->natts)
	{
		TupleDesc	truncdesc = CreateTupleDescTruncatedCopy(tupdesc, keepnatts);

		index_deform_tuple(src, truncdesc, values, isnull);
		full = bark_form_full_tuple(truncdesc, values, isnull, &fulllen);
		FreeTupleDesc(truncdesc);
	}
	else
	{
		index_deform_tuple(src, tupdesc, values, isnull);
		full = bark_form_full_tuple(tupdesc, values, isnull, &fulllen);
	}
	if (allocated)
		pfree(src);

	if (bark_len_is_oversized(fulllen))
	{
		ItemPointerData locator;
		BlockNumber firstblk = bark_write_overflow_chain(index, heaprel, full,
														 fulllen);

		ItemPointerSetBlockNumber(&locator, BARK_P_NONE);
		ItemPointerSetOffsetNumber(&locator, InvalidOffsetNumber);
		pivot = bark_form_oversized_entry(&locator, fulllen, firstblk,
										  false /* pivot */ , (uint16) keepnatts);
		bark_set_oversized_prefix(pivot, index, full);
		pfree(full);
		return pivot;
	}

	pivot = full;
	BarkPivotSetNAtts(pivot, (uint16) keepnatts);
	BarkPivotSetDownLink(pivot, BARK_P_NONE);
	return pivot;
}

/*
 * Form the high key for the left half of a leaf split, whose last item is
 * `lastleft`; `firstright` is the first item of the right half.  As nbtree's
 * _bt_truncate does, keep only as many of firstright's leading key attributes
 * as it takes to tell it from lastleft (bark_keep_natts); the attributes
 * dropped compare as minus infinity, so the high key sorts after lastleft and
 * no later than firstright.  Two items equal on every key attribute keep them
 * all, since BARK has no heap-TID tiebreaker to add.  The README section
 * "Suffix truncation" explains why the result separates the two pages.
 *
 * CREATE INDEX forms its leaf high keys here too (barksort.c).  Its items are
 * never OVERSIZED, and a truncated key is no larger than the item it comes
 * from, so the build never reaches the overflow-chain write in
 * bark_make_pivot, the one place keyinfo->heaprel is used here.
 */
IndexTuple
bark_truncate_pivot(Relation index, BarkKeyInfo *keyinfo, IndexTuple lastleft,
					IndexTuple firstright)
{
	int			nkeyatts = IndexRelationGetNumberOfKeyAttributes(index);
	int			keepnatts = bark_keep_natts(index, keyinfo, lastleft, firstright);

	return bark_make_pivot(index, keyinfo->heaprel, firstright,
						   Min(keepnatts, nkeyatts));
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
 * Clear the BARK_INCOMPLETE_SPLIT flag on the left half of a split, `cbuf`,
 * as part of the caller's generic-WAL record `gstate`, which also writes the
 * downlink to cbuf's right sibling.  The split thus becomes complete in the
 * same atomic step that makes the right sibling reachable from the parent.
 * The caller has held cbuf's exclusive lock since the split itself, so nobody
 * else can have seen the flag, let alone finished the split.
 */
static void
bark_clear_incomplete_split(GenericXLogState *gstate, Buffer cbuf)
{
	Page		cpage = GenericXLogRegisterBuffer(gstate, cbuf, 0);

	Assert((BarkPageGetOpaque(cpage)->bark_flags & BARK_INCOMPLETE_SPLIT) != 0);
	BarkPageGetOpaque(cpage)->bark_flags &= ~BARK_INCOMPLETE_SPLIT;
}

static void bark_insert_parent(Relation index, Relation heaprel,
							   BarkKeyInfo *keyinfo, BarkStack stack,
							   Buffer buf, IndexTuple downlink);
static void bark_split(Relation index, Relation heaprel, BarkKeyInfo *keyinfo,
					   BarkStack stack, Buffer buf, OffsetNumber newoff,
					   IndexTuple newitup, Buffer cbuf);

/*
 * Give an empty index, one whose meta page names no root, its first page: an
 * empty leaf that is also the root.  Called when an insert's descent finds no
 * root.  The caller then descends again and inserts through the normal path,
 * with its uniqueness, serializable-conflict and free-space checks, as nbtree
 * does after _bt_getroot creates the root of an empty index.
 *
 * Two backends can both see the index empty.  Creation is serialized on the
 * meta page's exclusive lock and the root re-checked under it, so only one
 * creates a root; the other finds it and simply returns.  Neither inserts
 * here, so the loser of the race cannot slip a row past the checks.
 *
 * Only the state bark_buildempty writes has no root, and an unlogged index is
 * reset to it after a crash; CREATE INDEX writes a root leaf even for an empty
 * table.
 */
static void
bark_create_root_leaf(Relation index, Relation heaprel)
{
	Buffer		metabuf;
	Buffer		leafbuf;
	GenericXLogState *gstate;
	Page		leafpage;
	Page		metapage;
	BlockNumber leafblk;

	/* A test may stop here, after the descent found no root. */
	INJECTION_POINT("bark-create-root-leaf", NULL);

	metabuf = ReadBuffer(index, BARK_METAPAGE);
	LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);
	if (BarkPageGetMeta(BufferGetPage(metabuf))->bark_root != BARK_P_NONE)
	{
		/* Someone else created the root first. */
		UnlockReleaseBuffer(metabuf);
		return;
	}

	leafbuf = bark_get_free_page(index, heaprel);
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
		lo->bark_cycleid = 0;
		lo->bark_flags = BARK_LEAF | BARK_ROOT;
		lo->bark_page_id = BARK_PAGE_ID;
	}

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
 * `newoff`.  Allocates a right sibling, moves the items from the split point
 * bark_findsplitloc chooses onward to it, gives the left page a new high key,
 * chains the right links, and inserts the right page's downlink, a copy of
 * that high key, into the parent via the stack.  The split itself is one
 * generic WAL record; the right link is published before the parent downlink
 * so a concurrent descender can always move right to find a key.
 *
 * `buf` is write-locked on entry and stays locked until bark_insert_parent
 * has write-locked the parent and written the downlink, so the left page's
 * BARK_INCOMPLETE_SPLIT flag is never visible to another backend while this
 * one is still completing the split (see bark_insert_parent).  `cbuf` is the
 * write-locked left half of a child split when `buf` is an internal page
 * receiving that child's downlink (InvalidBuffer for a leaf); its flag is
 * cleared in the split's WAL record and it is released once that is logged.
 * `heaprel` is the index's heap relation, for bark_get_free_page.
 */
static void
bark_split(Relation index, Relation heaprel, BarkKeyInfo *keyinfo,
		   BarkStack stack, Buffer buf, OffsetNumber newoff,
		   IndexTuple newitup, Buffer cbuf)
{
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
	IndexTuple	lhikey;
	IndexTuple	downlink;
	BTCycleId	cycleid;

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

	/* The right page gets items[splitidx..]; the new item is at newoff. */
	splitidx = bark_findsplitloc(index, keyinfo, items, n,
								 newoff - firstdata, isleaf, orighikey);

	/*
	 * Form the left page's high key BEFORE opening the generic-WAL state: an
	 * oversized key makes an OVERSIZED high key, which writes its own
	 * overflow chain under its own WAL records, and generic WAL states cannot
	 * nest.  A leaf's high key is the right page's first key, truncated
	 * against the left page's last key.  An internal page's items are pivots
	 * already, so its high key is a copy of the right page's first item with
	 * the attributes that item has, as in nbtree.
	 */
	if (isleaf)
		lhikey = bark_truncate_pivot(index, keyinfo, items[splitidx - 1],
									 items[splitidx]);
	else
	{
		lhikey = CopyIndexTuple(items[splitidx]);
		BarkEntrySetDownLink(lhikey, BARK_P_NONE);
	}

	/* Allocate the right sibling (reusing a reclaimed page if the FSM has one). */
	rbuf = bark_get_free_page(index, heaprel);
	rightblk = BufferGetBlockNumber(rbuf);

	/*
	 * The right page's downlink is the same tuple as the left page's high
	 * key, so the parent separates the two pages exactly as the high key
	 * does.  An OVERSIZED high key and its downlink share one overflow chain.
	 */
	downlink = CopyIndexTuple(lhikey);
	BarkEntrySetDownLink(downlink, rightblk);

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
	{
		OffsetNumber o = BARK_P_HIKEY;

		bark_page_insert_at(leftpage, lhikey, o++);
		pfree(lhikey);
		for (int i = 0; i < splitidx; i++)
			bark_page_insert_at(leftpage, items[i], o++);
	}

	/* --- Build the right page: original high key, items[splitidx..]. --- */
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
	 * Stamp both halves of a leaf split with the cycle ID of the VACUUM now
	 * scanning this index (zero if none), as _bt_split does.  The entries just
	 * moved to the right page may land on a block VACUUM has already passed;
	 * the stamp is how barkbulkdelete notices and goes back for them.  It must
	 * be read while both pages are exclusive-locked, so a VACUUM that starts
	 * right after cannot process either page before the split is complete.
	 */
	if (isleaf)
	{
		cycleid = _bt_vacuum_cycleid(index);
		BarkPageGetOpaque(leftpage)->bark_cycleid = cycleid;
		BarkPageGetOpaque(rightpage)->bark_cycleid = cycleid;
	}

	/*
	 * If the original page had a right sibling, that sibling's bark_prev must
	 * now point at the new right page.  Register and fix it in the same WAL
	 * record.  If this split is of an internal page receiving a child's
	 * downlink (cbuf valid), that downlink is being written here, so clear
	 * the child's BARK_INCOMPLETE_SPLIT flag atomically in the same record.
	 */
	{
		Buffer		sbuf = InvalidBuffer;

		if (!origrightmost)
		{
			Page		spage;

			sbuf = ReadBuffer(index, origright);
			LockBuffer(sbuf, BUFFER_LOCK_EXCLUSIVE);
			spage = GenericXLogRegisterBuffer(gstate, sbuf, 0);
			BarkPageGetOpaque(spage)->bark_prev = rightblk;
		}
		if (BufferIsValid(cbuf))
			bark_clear_incomplete_split(gstate, cbuf);
		GenericXLogFinish(gstate);
		if (BufferIsValid(cbuf))
			UnlockReleaseBuffer(cbuf);
		if (sbuf != InvalidBuffer)
			UnlockReleaseBuffer(sbuf);
	}

	/*
	 * The left (original) and right pages are now consistent on disk.  The
	 * right page can be released at once: it is reachable only through the
	 * left page's right link until its downlink exists.  The left page stays
	 * locked across the parent step.
	 */
	UnlockReleaseBuffer(rbuf);

	/*
	 * Transfer predicate locks for serializable transactions: a read of the
	 * original page must now also conflict with inserts onto the new right
	 * page, since keys that were covered by one page's read are now split
	 * across both.
	 */
	PredicateLockPageSplit(index, origblk, rightblk);

	/*
	 * The split is now durable but its downlink is not yet in the parent --
	 * the window a crash would leave as an incomplete split.  A test may stop
	 * here (via the injection point) to exercise bark_finish_split recovery
	 * (an error releases the left page with its flag still set), or to check
	 * that a concurrent inserter waits on the still-locked left page.
	 */
#ifdef USE_INJECTION_POINTS
	if (isleaf)
		INJECTION_POINT("bark-leave-leaf-split-incomplete", NULL);
	else
		INJECTION_POINT("bark-leave-internal-split-incomplete", NULL);
#endif

	bark_insert_parent(index, heaprel, keyinfo, stack, buf, downlink);
	pfree(downlink);

	/* Clean up. */
	for (int i = 0; i < n; i++)
		pfree(items[i]);
	pfree(items);
	if (orighikey)
		pfree(orighikey);
}

/*
 * Create a new root one level above the split of the old root, and point the
 * meta page at it.  The new root has two entries: a minus-infinity downlink
 * to the left half `lbuf` and `downlink`, which already points at the right
 * half.  `metabuf` and `lbuf` are write-locked on entry; the caller checked,
 * under the meta page lock, that `lbuf` is still the root.  All locks are
 * released here.
 */
static void
bark_new_root(Relation index, Relation heaprel, Buffer metabuf, Buffer lbuf,
			  IndexTuple downlink)
{
	BlockNumber leftblk = BufferGetBlockNumber(lbuf);
	uint32		childlevel = BarkPageGetOpaque(BufferGetPage(lbuf))->bark_level;
	Buffer		rootbuf = bark_get_free_page(index, heaprel);
	BlockNumber rootblk = BufferGetBlockNumber(rootbuf);
	GenericXLogState *gstate;
	Page		rootpage;
	Page		metapage;
	IndexTuple	leftdown;

	gstate = GenericXLogStart(index);
	rootpage = GenericXLogRegisterBuffer(gstate, rootbuf, GENERIC_XLOG_FULL_IMAGE);
	metapage = GenericXLogRegisterBuffer(gstate, metabuf, 0);

	PageInit(rootpage, BLCKSZ, sizeof(BarkPageOpaqueData));
	{
		BarkPageOpaque ro = BarkPageGetOpaque(rootpage);

		ro->bark_prev = BARK_P_NONE;
		ro->bark_next = BARK_P_NONE;
		ro->bark_level = childlevel + 1;
		ro->bark_cycleid = 0;
		ro->bark_flags = BARK_ROOT;
		ro->bark_page_id = BARK_PAGE_ID;
	}

	/*
	 * First downlink is minus-infinity (zero key attributes): it routes every
	 * key below the split key to the left child.  Second is the split-key
	 * downlink to the right child -- the caller's `downlink`, which already
	 * points at the right half, reused as-is (re-forming it would re-fetch
	 * and re-write an oversized key's overflow chain, and would do so inside
	 * this open generic-WAL state).
	 */
	leftdown = index_truncate_tuple(RelationGetDescr(index), downlink, 0);
	BarkPivotSetNAtts(leftdown, 0);
	BarkPivotSetDownLink(leftdown, leftblk);
	bark_page_insert_at(rootpage, leftdown, BARK_P_HIKEY);
	pfree(leftdown);
	bark_page_insert_at(rootpage, downlink, BARK_P_FIRSTKEY);

	/* Point the meta page at the new root. */
	{
		BarkMetaPageData *meta = BarkPageGetMeta(metapage);

		meta->bark_root = rootblk;
		meta->bark_level = childlevel + 1;
	}

	/* Both halves are now reachable: the split completes in this record. */
	bark_clear_incomplete_split(gstate, lbuf);
	GenericXLogFinish(gstate);

	UnlockReleaseBuffer(lbuf);
	UnlockReleaseBuffer(metabuf);
	UnlockReleaseBuffer(rootbuf);
}

/*
 * Return the block number of the leftmost page at tree level `level`, walking
 * down from the current root.  Used when a split's stack is empty (the page
 * was the root when we descended) but another backend has since added a level
 * above it.  Modeled on nbtree's _bt_get_endpoint.  Returns BARK_P_NONE when
 * the tree has no such level, which the caller reports as a missing parent.
 */
static BlockNumber
bark_get_leftmost_at_level(Relation index, uint32 level)
{
	Buffer		metabuf = ReadBuffer(index, BARK_METAPAGE);
	BlockNumber blkno;

	LockBuffer(metabuf, BUFFER_LOCK_SHARE);
	blkno = BarkPageGetMeta(BufferGetPage(metabuf))->bark_root;
	UnlockReleaseBuffer(metabuf);

	for (;;)
	{
		Buffer		buf = ReadBuffer(index, blkno);
		Page		page;
		BarkPageOpaque opaque;
		uint32		curlevel;

		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);
		opaque = BarkPageGetOpaque(page);
		curlevel = opaque->bark_level;

		if ((opaque->bark_flags & (BARK_DELETED | BARK_HALF_DEAD)) != 0)
		{
			/* A page being removed from the tree: step right past it. */
			if (BarkPageRightmost(opaque))
				elog(ERROR, "fell off the end of BARK index \"%s\"",
					 RelationGetRelationName(index));
			blkno = opaque->bark_next;
		}
		else if (curlevel == level)
		{
			UnlockReleaseBuffer(buf);
			return blkno;
		}
		else if (curlevel < level)
		{
			UnlockReleaseBuffer(buf);
			return BARK_P_NONE;
		}
		else
		{
			IndexTuple	itup = (IndexTuple)
				PageGetItem(page, PageGetItemId(page, BarkPageFirstDataKey(opaque)));

			blkno = BarkEntryGetDownLink(itup);
		}
		UnlockReleaseBuffer(buf);
	}
}

/*
 * Walk back up the tree one step and return the parent page that holds the
 * downlink to `child`, write-locked.  A port of nbtree's _bt_getstackbuf.
 *
 * The search starts at the page and offset recorded in `stack` during the
 * descent.  Inserts into the parent level can move the downlink right (even
 * onto a right sibling, if the parent split), so scan forward from the
 * recorded offset, then backward, then follow right links.  A parent page that
 * is itself incompletely split is finished first, using the parent's own stack
 * entry, so the parent level is complete before anything is added to it.
 *
 * Matching on the downlink's block number, not on a key, is what makes this
 * exact: duplicate keys can give several downlinks the same key, but only
 * one points at `child`.  On success stack->bark_blkno and bark_offset are
 * updated to where the downlink is now, and the caller inserts the new
 * downlink at bark_offset + 1.  Returns InvalidBuffer if no page on the level
 * holds a downlink to `child`.
 */
static Buffer
bark_getstackbuf(Relation index, BarkKeyInfo *keyinfo, BarkStack stack,
				 BlockNumber child)
{
	BlockNumber blkno = stack->bark_blkno;
	OffsetNumber start = stack->bark_offset;

	for (;;)
	{
		Buffer		buf = ReadBuffer(index, blkno);
		Page		page;
		BarkPageOpaque opaque;

		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
		page = BufferGetPage(buf);
		opaque = BarkPageGetOpaque(page);

		if ((opaque->bark_flags & BARK_INCOMPLETE_SPLIT) != 0)
		{
			bark_finish_split(index, keyinfo, buf, stack->bark_parent);
			continue;
		}

		if ((opaque->bark_flags & (BARK_DELETED | BARK_HALF_DEAD)) == 0)
		{
			OffsetNumber minoff = BarkPageFirstDataKey(opaque);
			OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
			OffsetNumber off;

			/*
			 * InvalidOffsetNumber means search the whole page; also clamp a
			 * start that a concurrent split has left past the end, or that
			 * now points at a high key the page did not have before.
			 */
			if (start < minoff)
				start = minoff;
			if (start > maxoff)
				start = OffsetNumberNext(maxoff);

			for (off = start; off <= maxoff; off = OffsetNumberNext(off))
			{
				IndexTuple	itup = (IndexTuple)
					PageGetItem(page, PageGetItemId(page, off));

				if (BarkEntryGetDownLink(itup) == child)
				{
					stack->bark_blkno = blkno;
					stack->bark_offset = off;
					return buf;
				}
			}
			for (off = OffsetNumberPrev(start); off >= minoff;
				 off = OffsetNumberPrev(off))
			{
				IndexTuple	itup = (IndexTuple)
					PageGetItem(page, PageGetItemId(page, off));

				if (BarkEntryGetDownLink(itup) == child)
				{
					stack->bark_blkno = blkno;
					stack->bark_offset = off;
					return buf;
				}
			}
		}

		/* The downlink moved right at least one page. */
		if (BarkPageRightmost(opaque))
		{
			UnlockReleaseBuffer(buf);
			return InvalidBuffer;
		}
		blkno = opaque->bark_next;
		start = InvalidOffsetNumber;
		UnlockReleaseBuffer(buf);
	}
}

/*
 * Insert `downlink`, which points at the new right half of the split of
 * `buf`, into the parent of `buf`, completing the split.  `stack` is the
 * parent path from the descent; NULL when `buf` was the root then.
 *
 * `buf` is write-locked on entry, with BARK_INCOMPLETE_SPLIT set, and is
 * released here.  It stays locked until the parent is write-locked and the
 * downlink is written, and its flag is cleared in that same WAL record.  So
 * the flag is only ever seen by another backend on a page that is not being
 * completed by anyone -- one whose split was interrupted by an error or crash
 * -- and the backend that sees it (holding the page's lock) can safely finish
 * the split itself.  Locks are always taken child before parent, as in
 * nbtree, so the coupling cannot deadlock with another split.
 *
 * If the parent is full it is split in turn (bark_split with `buf` as the
 * child whose flag that split clears), and the same protocol repeats one
 * level up.
 */
static void
bark_insert_parent(Relation index, Relation heaprel, BarkKeyInfo *keyinfo,
				   BarkStack stack, Buffer buf, IndexTuple downlink)
{
	BlockNumber leftblk = BufferGetBlockNumber(buf);
	BarkStackData fakestack;
	Buffer		pbuf;
	Page		ppage;
	OffsetNumber off;

	if (stack == NULL)
	{
		/*
		 * The page was the root when we descended.  If it still is, grow a
		 * new level.  Only a split of the root, made while holding the root's
		 * lock, changes bark_root, so the answer cannot change while we hold
		 * `buf`.  If another backend already added a level, its root is our
		 * parent level: find it from its leftmost page, and let
		 * bark_getstackbuf move right to the downlink.
		 */
		Buffer		metabuf = ReadBuffer(index, BARK_METAPAGE);
		uint32		level = BarkPageGetOpaque(BufferGetPage(buf))->bark_level;

		LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);
		if (BarkPageGetMeta(BufferGetPage(metabuf))->bark_root == leftblk)
		{
			bark_new_root(index, heaprel, metabuf, buf, downlink);
			return;
		}
		UnlockReleaseBuffer(metabuf);

		elog(DEBUG2, "concurrent ROOT page split in BARK index \"%s\"",
			 RelationGetRelationName(index));
		fakestack.bark_blkno = bark_get_leftmost_at_level(index, level + 1);
		fakestack.bark_offset = InvalidOffsetNumber;
		fakestack.bark_parent = NULL;
		stack = &fakestack;
	}

	if (stack->bark_blkno == BARK_P_NONE)
		pbuf = InvalidBuffer;
	else
		pbuf = bark_getstackbuf(index, keyinfo, stack, leftblk);
	if (pbuf == InvalidBuffer)
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg_internal("failed to re-find parent downlink for block %u in BARK index \"%s\"",
								 leftblk, RelationGetRelationName(index))));

	/* The new downlink goes immediately after the one to its left sibling. */
	off = OffsetNumberNext(stack->bark_offset);
	ppage = BufferGetPage(pbuf);

	if (PageGetFreeSpace(ppage) >= MAXALIGN(IndexTupleSize(downlink)))
	{
		GenericXLogState *gstate = GenericXLogStart(index);
		Page		p = GenericXLogRegisterBuffer(gstate, pbuf, 0);

		bark_page_insert_at(p, downlink, off);
		bark_clear_incomplete_split(gstate, buf);
		GenericXLogFinish(gstate);
		UnlockReleaseBuffer(buf);
		UnlockReleaseBuffer(pbuf);
	}
	else
		bark_split(index, heaprel, keyinfo, stack->bark_parent, pbuf, off,
				   downlink, buf);
}

/*
 * Finish a split that was interrupted (by an error or crash) after the right
 * sibling was published but before its downlink reached the parent: the left
 * page `lbuf` carries BARK_INCOMPLETE_SPLIT.  The missing downlink is a copy
 * of the left page's high key pointing at the right sibling, exactly the
 * tuple the interrupted split would have inserted; insert it into the parent.
 * bark_insert_parent clears the flag atomically with that insert.
 *
 * `lbuf` is write-locked on entry and released by bark_insert_parent, which
 * keeps it locked until the parent is locked, exactly as for a split this
 * backend made itself.  Since the flag was seen under that lock, no other
 * backend is completing this split.  `stack` is the parent path to `lbuf`, or
 * NULL when `lbuf` was reached from the top of the tree; bark_insert_parent
 * then decides from the meta page whether this was a root split.
 *
 * The descents that call this pass no heap relation of their own; a page the
 * parent insert allocates is taken with keyinfo->heaprel, which the insert
 * that started the descent set.
 */
void
bark_finish_split(Relation index, BarkKeyInfo *keyinfo, Buffer lbuf,
				  BarkStack stack)
{
	Page		lpage = BufferGetPage(lbuf);
	BarkPageOpaque lopaque = BarkPageGetOpaque(lpage);
	IndexTuple	hikey;
	IndexTuple	downlink;

	Assert((lopaque->bark_flags & BARK_INCOMPLETE_SPLIT) != 0);
	Assert(!BarkPageRightmost(lopaque));	/* has a right sibling */

	INJECTION_POINT("bark-finish-incomplete-split", NULL);

	/*
	 * Copy the high key, which keeps its own attribute count and, if it is
	 * OVERSIZED, its overflow chain, and point the copy at the right sibling.
	 */
	hikey = (IndexTuple) PageGetItem(lpage, PageGetItemId(lpage, BARK_P_HIKEY));
	downlink = CopyIndexTuple(hikey);
	BarkEntrySetDownLink(downlink, lopaque->bark_next);
	bark_insert_parent(index, keyinfo->heaprel, keyinfo, stack, lbuf, downlink);
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
 *    caller can wait for it and retry; *is_unique is left false.  When that
 *    transaction is itself a speculative inserter (an INSERT ... ON CONFLICT
 *    that has inserted its heap tuple but not yet confirmed or killed it),
 *    *speculativeToken is set to its token so the caller can wait on the
 *    speculative insertion (which resolves the moment the inserter confirms or
 *    kills, not when its whole xact ends) rather than on the xact.
 *  - UNIQUE_CHECK_PARTIAL never errors: on any conflict it sets *is_unique to
 *    false and returns, letting a deferred constraint recheck decide later.
 *  - Otherwise a live conflict raises ERRCODE_UNIQUE_VIOLATION.
 *
 * Returns InvalidTransactionId when no wait is needed (unique, or already
 * errored); *speculativeToken is set to zero unless a speculative conflict was
 * found.  The caller holds the write lock on `buf` throughout and still holds
 * it on return.
 *
 * This does not opportunistically kill known-dead index entries during the
 * check; that is an orthogonal optimization layered on the correct check here.
 */
static TransactionId
bark_check_unique(Relation index, BarkKeyInfo *keyinfo, IndexTuple itup,
				  Buffer buf, Relation heapRel, IndexUniqueCheck checkUnique,
				  bool *is_unique, uint32 *speculativeToken)
{
	SnapshotData SnapshotDirty;
	Buffer		curbuf = buf;
	bool		ownbuf = false;		/* do we need to release curbuf? */

	*is_unique = true;
	*speculativeToken = 0;
	InitDirtySnapshot(SnapshotDirty);

	/*
	 * The scan below only moves right from the insert leaf (where the insert
	 * descent, nextkey=true, lands -- the leaf holding the position just past
	 * the last key equal to itup's).  This finds every conflicting entry
	 * because of how BARK inserts: a new entry for a key always goes at the
	 * END of that key's run, so any existing live entry for the same key sits
	 * at or after the first equal entry on the insert leaf and is reachable by
	 * scanning right.  Dead, not-yet-vacuumed duplicates may extend the run
	 * left across earlier leaves, but the single live survivor cannot be
	 * left of the insert leaf's first equal entry.  Suffix truncation does
	 * not change this: a pivot inside a run of equal keys keeps every key
	 * attribute (see bark_truncate_pivot), so the descent still reaches the
	 * end of the run.
	 *
	 * This correctness argument relies on the insert descent using
	 * nextkey=true.  If the insert positioning ever changes so the live entry
	 * could land strictly left of the descent leaf, this check would have to
	 * first walk left to the first leaf of the equal-key run (or descend the
	 * check with nextkey=false).
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

			/*
			 * Read the entry's heap locator by shape: a unique index only ever
			 * holds SINGLE or OVERSIZED entries (it never coalesces), each with
			 * exactly one locator.  An OVERSIZED entry keeps the locator in its
			 * ref, not in t_tid (which holds the overflow block), so read it the
			 * uniform way rather than from t_tid directly.
			 */
			bark_entry_get_tids(curitup, &htid, 1);

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
				 * A speculative inserter also reports its token here, so the
				 * caller can wait on the speculative insertion rather than on
				 * the whole transaction.
				 */
				xwait = TransactionIdIsValid(SnapshotDirty.xmin) ?
					SnapshotDirty.xmin : SnapshotDirty.xmax;
				if (TransactionIdIsValid(xwait))
				{
					if (ownbuf)
						UnlockReleaseBuffer(curbuf);
					*speculativeToken = SnapshotDirty.speculativeToken;
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
 * same key as `key` (a SINGLE-shape key tuple), forming or extending a LIST or
 * POSTING entry rather than adding another SINGLE.  Returns true and performs
 * the replacement (WAL-logged) when it coalesced; returns false (page
 * unchanged) when there is no equal entry, or when the merged entry would not
 * fit on this page -- in which case the caller inserts a plain SINGLE and the
 * duplicates stay as separate entries until a later insert can merge them.
 *
 * The merged set is encoded as whichever shape is smaller: a LIST (sorted
 * locator array) for a modest number of duplicates, or a POSTING (sbm
 * serialization) once the set is large/clustered enough that the sbm envelope
 * beats the flat array.  bark_form_posting returns NULL when LIST would still
 * win, so the shape is chosen by actual encoded size, not a fixed count.
 *
 * Only called for non-unique indexes: a unique index never legitimately holds
 * two live tuples with the same key, so it never forms a LIST or POSTING.
 * `off` is the leaf insert position (one past the last entry <= key), so the
 * candidate equal entry, if any, is at off-1.
 *
 * The per-entry size ceiling is BarkMaxItemSize (~1/3 page).  A single key
 * with more duplicates than a POSTING entry can hold within that ceiling keeps
 * the overflow as separate entries; splitting one key's posting set across
 * entries is a space optimization, not a correctness matter.
 *
 * The common append case (the new locator sorts after every existing member,
 * as monotonic/append-ish heap TIDs do) is handled by an O(1)-amortized fast
 * path: a LIST entry is extended by appending the one new locator to its body
 * and bumping its count, without re-reading the set, re-sorting, or probing the
 * POSTING encoding.  Only when the appended LIST would exceed the item ceiling,
 * when the new locator lands in the middle of the set, or when the entry is a
 * SINGLE/POSTING does it fall to the general path below, which re-reads the
 * full set, inserts in sorted order, and re-encodes as whichever of LIST /
 * POSTING is smaller.  Correctness is identical either way: members stay sorted
 * and distinct, and the LIST -> POSTING promotion still happens at the ceiling.
 *
 * The POSTING append case is still O(members) per insert (the sbm
 * serialization has no in-place append, so it is deserialized, added to, and
 * re-serialized); an incremental sbm_add into an embedded, growable body would
 * make it O(1) amortized too.  POSTING is only chosen for a large, clustered
 * set whose per-key members are in any case capped by the item ceiling, so
 * this is bounded, not the O(N^2) the LIST phase used to be.
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
	ItemPointer tids;
	int			maxtids;
	int			nold;
	int			nnew;
	int			ins;
	IndexTuple	newentry;
	IndexTuple	posting;
	Size		newsz;

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

	/*
	 * Fast path: appending to a LIST whose every member sorts before the new
	 * locator.  This is the common monotonic/append-ish TID case.  Extend the
	 * LIST body by one locator and bump its count in place -- no re-read, no
	 * re-sort, no POSTING probe -- so building one key's set by repeated single
	 * inserts is O(1) amortized rather than O(N) per insert.  We fall through to
	 * the general path when the appended LIST would exceed the item ceiling (so
	 * the LIST -> POSTING promotion and page-split handling stay in one place).
	 */
	if (BarkEntryGetShape(cur) == BARK_SHAPE_LIST)
	{
		int			ncur = BarkListGetCount(cur);
		ItemPointer last = BarkListGetTID(cur, ncur - 1);

		if (ncur < BARK_LIST_MAX_COUNT &&
			ItemPointerCompare(newtid, last) > 0)
		{
			Size		cursz = IndexTupleSize(cur);
			Size		appended = cursz + sizeof(ItemPointerData);
			IndexTuple	ext;

			if (MAXALIGN(appended) <= BarkMaxItemSize &&
				PageGetFreeSpace(page) + MAXALIGN(ItemIdGetLength(iid)) >=
					MAXALIGN(appended))
			{
				/*
				 * The body is a packed ascending ItemPointerData array ending
				 * at the entry's used size; the new locator goes right after
				 * the last one.  The body offset (t_tid block field) is
				 * unchanged, so copying the old entry verbatim and appending
				 * keeps the layout correct; only the count and size change.
				 */
				ext = (IndexTuple) palloc0(appended);
				memcpy(ext, cur, cursz);
				memcpy((char *) ext + cursz, newtid, sizeof(ItemPointerData));
				ext->t_info = (ext->t_info & ~INDEX_SIZE_MASK) |
					(uint16) appended;
				ItemPointerSetOffsetNumber(&ext->t_tid,
										   (OffsetNumber) ((uint16) (ncur + 1) |
														   BARK_IS_LIST));

				{
					GenericXLogState *gstate = GenericXLogStart(index);
					Page		p = GenericXLogRegisterBuffer(gstate, buf, 0);

					if (!PageIndexTupleOverwrite(p, eqoff, (char *) ext,
												 IndexTupleSize(ext)))
						elog(ERROR, "failed to extend BARK list entry in place");
					GenericXLogFinish(gstate);
				}
				pfree(ext);
				return true;
			}
			/* Too big to grow here: let the general path re-encode / split. */
		}
	}

	/*
	 * Fast path: adding to an existing POSTING entry.  sbm dedups and keeps
	 * order, so the one new locator is added incrementally (deserialize once,
	 * add, re-serialize once) instead of re-reading the whole set and
	 * rebuilding its sbm from scratch -- O(serialized size) rather than
	 * O(members) per insert.  A POSTING never shrinks back to a LIST on insert
	 * (the set only grows), so the shape stays POSTING; we fall through only
	 * when the extended entry would overflow the item ceiling (then the general
	 * path keeps the overflow as a separate entry or splits the page).
	 */
	if (BarkEntryGetShape(cur) == BARK_SHAPE_POSTING)
	{
		Size		room = PageGetFreeSpace(page) + MAXALIGN(ItemIdGetLength(iid));
		Size		cap = Min((Size) BarkMaxItemSize, room);
		IndexTuple	ext = bark_posting_add_tid(RelationGetDescr(index), key,
											   cur, newtid, cap);

		if (ext != NULL)
		{
			GenericXLogState *gstate = GenericXLogStart(index);
			Page		p = GenericXLogRegisterBuffer(gstate, buf, 0);

			if (!PageIndexTupleOverwrite(p, eqoff, (char *) ext,
										 IndexTupleSize(ext)))
				elog(ERROR, "failed to extend BARK posting entry in place");
			GenericXLogFinish(gstate);
			pfree(ext);
			return true;
		}
		/* Too big to grow here: let the general path handle it. */
	}

	/*
	 * Gather the existing locators plus the new one, in ascending order.  A
	 * POSTING entry can already hold many thousands of TIDs, so the buffer is
	 * palloc'd to the current count plus one rather than a fixed stack array.
	 */
	nold = bark_entry_count_tids(cur);
	maxtids = nold + 1;
	tids = (ItemPointer) palloc(maxtids * sizeof(ItemPointerData));
	nold = bark_entry_get_tids(cur, tids, maxtids);

	/* Insert newtid keeping the array sorted and distinct. */
	for (ins = 0; ins < nold; ins++)
	{
		int			c = ItemPointerCompare(newtid, &tids[ins]);

		if (c == 0)
		{
			pfree(tids);
			return true;		/* already present (should not happen): done */
		}
		if (c < 0)
			break;
	}
	memmove(&tids[ins + 1], &tids[ins],
			(nold - ins) * sizeof(ItemPointerData));
	tids[ins] = *newtid;
	nnew = nold + 1;

	/*
	 * Encode the merged set as whichever shape is smaller.  bark_form_posting
	 * returns NULL when the LIST form would be no larger, so a small set stays
	 * a LIST and a large/clustered one is promoted to POSTING -- the LIST ->
	 * POSTING promotion happens automatically at the size crossover.
	 */
	posting = bark_form_posting(RelationGetDescr(index), key, tids, nnew);
	if (posting != NULL)
		newentry = posting;
	else if (nnew <= BARK_LIST_MAX_COUNT)
		newentry = bark_form_list(RelationGetDescr(index), key, tids, nnew);
	else
	{
		pfree(tids);
		return false;			/* too many for a LIST and POSTING did not win */
	}
	pfree(tids);

	newsz = MAXALIGN(IndexTupleSize(newentry));
	if (newsz > BarkMaxItemSize)
	{
		pfree(newentry);
		return false;			/* too big for one entry: keep separate */
	}

	/*
	 * The merged entry replaces the old one.  Removing the old entry and
	 * adding the (possibly larger) new one must fit: PageGetFreeSpace plus the
	 * reclaimed old slot must cover it.
	 */
	if (PageGetFreeSpace(page) + MAXALIGN(ItemIdGetLength(iid)) < newsz)
	{
		pfree(newentry);
		return false;			/* no room to grow here: caller splits */
	}

	{
		GenericXLogState *gstate = GenericXLogStart(index);
		Page		p = GenericXLogRegisterBuffer(gstate, buf, 0);
		OffsetNumber deloff = eqoff;

		PageIndexMultiDelete(p, &deloff, 1);
		if (PageAddItem(p, (char *) newentry, IndexTupleSize(newentry), eqoff,
						false, false) == InvalidOffsetNumber)
			elog(ERROR, "failed to replace BARK leaf entry with a merged entry");
		GenericXLogFinish(gstate);
	}

	pfree(newentry);
	return true;
}

bool
bark_insert(Relation index, Datum *values, bool *isnull, ItemPointer ht_ctid,
			Relation heapRel, IndexUniqueCheck checkUnique,
			bool indexUnchanged, IndexInfo *indexInfo)
{
	BarkKeyInfo *keyinfo = bark_build_keyinfo(index);
	Size		fulllen;
	IndexTuple	itup = bark_form_full_tuple(RelationGetDescr(index), values,
										   isnull, &fulllen);
	bool		oversized = bark_len_is_oversized(fulllen);
	Buffer		buf;
	BarkStack	stack;
	Page		page;
	OffsetNumber off;
	Size		itemsz;
	bool		result = false;		/* significant only for UNIQUE_CHECK_PARTIAL */

	itup->t_tid = *ht_ctid;		/* SINGLE shape: locator in t_tid */
	keyinfo->heaprel = heapRel; /* for pages a split allocates */

	/*
	 * `itup` is the full in-memory key tuple at any size; bark_compare_itups
	 * compares it directly (index_getattr does not care about the 8191-byte
	 * cap).  Only when the entry is actually placed on a page is an oversized
	 * key written to an overflow chain and replaced by a small OVERSIZED entry
	 * (done at the leaf-insert / coalesce / split sites below).  itemsz is the
	 * page footprint: tiny for an oversized key, the tuple size otherwise.
	 */
	itemsz = oversized
		? MAXALIGN(sizeof(IndexTupleData) + MAXALIGN(sizeof(BarkOverflowRef)))
		: MAXALIGN(IndexTupleSize(itup));

retry:
	buf = bark_search(index, keyinfo, itup, true, true, &stack);

	if (buf == InvalidBuffer)
	{
		/* Empty index: give it a root leaf, then insert as usual. */
		bark_create_root_leaf(index, heapRel);
		goto retry;
	}

	/*
	 * bark_search finishes any incomplete split it meets on the way down, so
	 * the leaf it returns should never carry the flag.  The test is cheap and
	 * keeps the insert safe if it ever does: complete the split, then descend
	 * again, since the key may belong on the right sibling.  The flag is
	 * tested under the exclusive lock bark_search returned, and a backend in
	 * the middle of its own split holds that lock until the split is
	 * complete, so a set flag here would mean an abandoned split.
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
	 * finish, then re-descend and check again.  If the conflict is with a
	 * speculative insertion (INSERT ... ON CONFLICT), it also returns the
	 * speculative token, and we wait on the speculative insertion -- which
	 * wakes us the moment the speculative inserter confirms or kills its tuple,
	 * so a losing speculative insert of the same key does not block to end of
	 * xact.  This matches nbtree's _bt_doinsert speculative-wait path exactly.
	 */
	if (checkUnique != UNIQUE_CHECK_NO)
	{
		bool		nulls_present = false;
		TransactionId xwait;
		uint32		speculativeToken;
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
									  checkUnique, &is_unique, &speculativeToken);
			if (TransactionIdIsValid(xwait))
			{
				/* Conflict with an in-progress xact: wait and retry. */
				UnlockReleaseBuffer(buf);
				if (stack)
					bark_freestack(stack);
				if (speculativeToken)
					SpeculativeInsertionWait(xwait, speculativeToken);
				else
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
	 * same key, which the uniqueness check above already rejected.  An
	 * oversized key never coalesces: a LIST/POSTING of oversized keys could not
	 * fit the item ceiling, and each oversized row keeps its own OVERSIZED
	 * entry + overflow chain (duplicate oversized keys are not deduplicated; a
	 * shared overflow chain for identical oversized values would be a space
	 * optimization, not a correctness matter).  Nor do keys whose equal values
	 * can have different stored images, or indexes with INCLUDE columns: a
	 * shared entry would return one row's bytes for all of them (see
	 * bark_allequalimage).
	 */
	if (!indexInfo->ii_Unique && !oversized && bark_allequalimage(index) &&
		bark_coalesce_list(index, keyinfo, itup, &itup->t_tid, buf, off))
	{
		UnlockReleaseBuffer(buf);
		if (stack)
			bark_freestack(stack);
		pfree(itup);
		pfree(keyinfo);
		return result;
	}

	{
		/* The entry actually placed on the page (OVERSIZED when oversized). */
		IndexTuple	entry = bark_leaf_page_entry(index, heapRel, itup,
												 oversized, fulllen);

		if (PageGetFreeSpace(page) >= itemsz)
		{
			GenericXLogState *gstate = GenericXLogStart(index);
			Page		p = GenericXLogRegisterBuffer(gstate, buf, 0);

			bark_page_insert_at(p, entry, off);
			GenericXLogFinish(gstate);
			UnlockReleaseBuffer(buf);
		}
		else
		{
			/* Leaf split: no child below, so no incomplete-split flag to clear. */
			bark_split(index, heapRel, keyinfo, stack, buf, off, entry,
					   InvalidBuffer);
			buf = InvalidBuffer;	/* bark_split released it */
		}
		pfree(entry);
	}

	if (stack)
		bark_freestack(stack);
	pfree(itup);
	pfree(keyinfo);
	return result;
}
