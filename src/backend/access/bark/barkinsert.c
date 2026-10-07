/*-------------------------------------------------------------------------
 *
 * barkinsert.c
 *	  Insert into a BARK index: leaf insert and Lehman & Yao page split.
 *
 * bark_insert descends to the target leaf (bark_search), inserts the new
 * SINGLE-shape entry in (key, heap TID) order, and -- when the page
 * overflows -- splits it: a new right page takes the items above a split point chosen by
 * bark_findsplitloc (barksplitloc.c), the left page gets a new high key (on a
 * leaf, the right page's first key without the attributes not needed to tell
 * it from the left page's last key), the right link is published before the
 * parent downlink, and a copy of the high key is inserted into the parent as
 * the right page's downlink (growing a new root if the split reached the
 * top).  Each step is logged with one of BARK's own WAL records, as nbtree
 * logs it (see "WAL" in the README).
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
#include "access/barkxlog.h"
#include "access/genam.h"
#include "access/itup.h"
#include "access/nbtree.h"
#include "access/tableam.h"
#include "access/xloginsert.h"
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

/*
 * Free space on the leaf `page` that an insert may take.  A split inside a
 * run of equal keys gives the left half a high key with a heap TID, which is
 * MAXALIGN(sizeof(ItemPointerData)) bytes larger than a high key without one,
 * or than the item a rightmost page's last insert left room for.  Keeping
 * that much back means the left half can then still hold every entry it had:
 * coalescing fills a page a few bytes at a time, to the last byte, and
 * without the reserve a split for want of those bytes would move a whole
 * LIST or POSTING entry, up to a third of the page, to the right half.
 */
Size
bark_leaf_free_space(Page page)
{
	Size		free = PageGetFreeSpace(page);

	return free > MAXALIGN(sizeof(ItemPointerData)) ?
		free - MAXALIGN(sizeof(ItemPointerData)) : 0;
}

/*
 * Find the offset at which to insert key, with heap TID scantid, on a leaf
 * page: the first entry that sorts after (key, scantid).  When scantid lies
 * inside an equal-key entry's TID range, that entry is the one just before the
 * offset returned (see bark_coalesce_list).
 */
static OffsetNumber
bark_leaf_insert_off(Relation index, BarkKeyInfo *keyinfo, IndexTuple key,
					 ItemPointer scantid, Page page)
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
		BarkItemBuf ibuf;
		IndexTuple	mitup = BarkPageGetItem(page, mid, &ibuf);

		if (bark_compare_itups_tid(keyinfo, index, key, scantid, mitup) >= 0)
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
 * key attributes and, when `heaptid` is not NULL, that heap TID.  Non-key
 * INCLUDE attributes and the key attributes after the first keepnatts are
 * physically removed: pivots only route by key.  A heap TID is kept only
 * with every key attribute (see bark_truncate_pivot); it goes after the key
 * data, as the last ItemPointerData of the MAXALIGNed tuple, as in nbtree's
 * _bt_truncate.
 *
 * When the truncated key (with its heap TID) still exceeds the item ceiling,
 * the pivot is an OVERSIZED pivot: its full key is written to a fresh
 * overflow chain the pivot owns, the heap TID goes in the pivot's ref, and
 * the pivot is the small OVERSIZED entry referencing the chain.
 * bark_compare_itups fetches that chain, so an oversized pivot routes on the
 * full key exactly as a leaf entry does.  The caller must not be in a
 * critical section, since writing the chain allocates pages and writes its
 * own WAL records.
 * heaprel is the index's heap, for bark_get_free_page.
 */
static IndexTuple
bark_make_pivot(Relation index, Relation heaprel, IndexTuple key,
				int keepnatts, ItemPointer heaptid)
{
	bool		allocated;
	IndexTuple	src = bark_strip_to_key(index, key, &allocated);
	TupleDesc	tupdesc = RelationGetDescr(index);
	Datum		values[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS];
	Size		fulllen;
	Size		tidsz = heaptid ? MAXALIGN(sizeof(ItemPointerData)) : 0;
	IndexTuple	full;
	IndexTuple	pivot;

	Assert(heaptid == NULL ||
		   keepnatts == IndexRelationGetNumberOfKeyAttributes(index));

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

	if (MAXALIGN(fulllen) + tidsz > BarkMaxItemSize)
	{
		ItemPointerData locator;
		BlockNumber firstblk = bark_write_overflow_chain(index, heaprel, full,
														 fulllen);

		ItemPointerSetBlockNumber(&locator, BARK_P_NONE);
		ItemPointerSetOffsetNumber(&locator, InvalidOffsetNumber);
		pivot = bark_form_oversized_entry(&locator, fulllen, firstblk,
										  false /* pivot */ , (uint16) keepnatts);
		bark_set_oversized_prefix(pivot, index, full);
		if (heaptid)
			BarkOverflowGetRef(pivot)->pivottid = *heaptid;
		pfree(full);
		return pivot;
	}

	if (heaptid)
	{
		pivot = (IndexTuple) palloc0(fulllen + tidsz);
		memcpy(pivot, full, fulllen);
		pivot->t_info = (full->t_info & ~INDEX_SIZE_MASK) |
			(uint16) (fulllen + tidsz);
		pfree(full);
	}
	else
		pivot = full;
	BarkPivotSetNAtts(pivot, (uint16) keepnatts);
	BarkPivotSetDownLink(pivot, BARK_P_NONE);
	if (heaptid)
	{
		ItemPointerSetOffsetNumber(&pivot->t_tid,
								   ItemPointerGetOffsetNumberNoCheck(&pivot->t_tid) |
								   BARK_PIVOT_HEAP_TID);
		*BarkPivotGetHeapTID(pivot) = *heaptid;
	}
	return pivot;
}

/*
 * Form the high key for the left half of a leaf split, whose last item is
 * `lastleft`; `firstright` is the first item of the right half.  As nbtree's
 * _bt_truncate does, keep only as many of firstright's leading key attributes
 * as it takes to tell it from lastleft (bark_keep_natts); the attributes
 * dropped compare as minus infinity, so the high key sorts after lastleft and
 * no later than firstright.  Two items equal on every key attribute keep them
 * all and firstright's lowest heap TID, which lies above every heap TID of
 * lastleft, since a run of equal keys is in heap TID order: an insert of the
 * key with a heap TID below it goes left, any other right.  The README
 * section "Suffix truncation" explains why the result separates the two
 * pages.
 *
 * CREATE INDEX forms its leaf high keys here too (barksort.c).  Its items are
 * never OVERSIZED, and the inline limit for a leaf entry leaves room for the
 * heap TID (bark_len_is_oversized), so the build never reaches the
 * overflow-chain write in bark_make_pivot, the one place keyinfo->heaprel is
 * used here.
 */
IndexTuple
bark_truncate_pivot(Relation index, BarkKeyInfo *keyinfo, IndexTuple lastleft,
					IndexTuple firstright)
{
	int			nkeyatts = IndexRelationGetNumberOfKeyAttributes(index);
	int			keepnatts = bark_keep_natts(index, keyinfo, lastleft, firstright);
	ItemPointerData lo;
	ItemPointerData hi;

	if (keepnatts <= nkeyatts)
		return bark_make_pivot(index, keyinfo->heaprel, firstright, keepnatts,
							   NULL);

	bark_entry_tid_range(firstright, &lo, &hi);
#ifdef USE_ASSERT_CHECKING
	{
		ItemPointerData llo;
		ItemPointerData lhi;

		bark_entry_tid_range(lastleft, &llo, &lhi);
		Assert(ItemPointerCompare(&lhi, &lo) < 0);
	}
#endif
	return bark_make_pivot(index, keyinfo->heaprel, firstright, nkeyatts, &lo);
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
 * Lay out items[0..n) on `page`, after its high key `hikey` (NULL for none),
 * coded against a prefix of `prefixlen` bytes when that is not zero.  Returns
 * false when they do not fit.
 */
static bool
bark_split_fill(Page page, IndexTuple hikey, IndexTuple *items, int n,
				const char *prefix, Size prefixlen)
{
	OffsetNumber o = BARK_P_HIKEY;

	if (hikey != NULL &&
		PageAddItem(page, hikey, IndexTupleSize(hikey), o++, false, false) ==
		InvalidOffsetNumber)
		return false;
	if (prefixlen > 0)
	{
		bark_page_set_prefix(page, prefix, prefixlen);
		o++;
	}
	for (int i = 0; i < n; i++)
	{
		IndexTuple	coded = bark_prefix_encode(page, items[i]);
		bool		added;

		added = PageAddItem(page, coded, IndexTupleSize(coded), o++,
							false, false) != InvalidOffsetNumber;
		if (coded != items[i])
			pfree(coded);
		if (!added)
			return false;
	}
	return true;
}

/*
 * Lay out one half of a leaf split on `page`, which the caller has
 * initialized: the high key `hikey` (NULL for none), then items[0..n).
 *
 * The half is formed as a whole, so it may take a prefix of its own: its
 * first item's leading bytes, if the items share enough of them
 * (bark_prefix_choose).  bark_findsplitloc sized the items as they are
 * coded on the original page, `origprefix` of `origprefixlen` bytes (zero
 * when that page has none), which may be less than their plain size, and
 * less than their size against the new prefix.  So when the half does not
 * fit with its own prefix (or does not share enough of one to take it), it
 * is laid out plain, and if that does not fit either, with the original
 * page's prefix, against which it fits by construction: the items keep the
 * shares they had, and the prefix need not lie within the half's key range
 * for coding to be correct.  A half of a plain page always fits plain.
 */
static void
bark_split_leaf_half(Relation index, Page page, IndexTuple hikey,
					 IndexTuple *items, int n, const char *origprefix,
					 Size origprefixlen)
{
	BarkPageOpaqueData opaque = *BarkPageGetOpaque(page);
	const char *prefix[3];
	Size		prefixlen[3];
	int			ntries = 0;

	Assert(!BarkPageHasPrefix(&opaque));
	prefixlen[ntries] = bark_prefix_choose(index, items, n, &prefix[ntries]);
	if (prefixlen[ntries] > 0)
		ntries++;
	prefix[ntries] = NULL;
	prefixlen[ntries++] = 0;
	if (origprefixlen > 0)
	{
		prefix[ntries] = origprefix;
		prefixlen[ntries++] = origprefixlen;
	}

	for (int i = 0; i < ntries; i++)
	{
		PageInit(page, BLCKSZ, sizeof(BarkPageOpaqueData));
		*BarkPageGetOpaque(page) = opaque;
		if (bark_split_fill(page, hikey, items, n, prefix[i], prefixlen[i]))
			return;
	}
	elog(ERROR, "failed to lay out half of a split of a BARK leaf");
}

/*
 * Clear the BARK_INCOMPLETE_SPLIT flag on the left half of a split, `cbuf`,
 * in the caller's critical section, which also writes the downlink to cbuf's
 * right sibling and logs both changes in one record.  The split thus becomes
 * complete in the same atomic step that makes the right sibling reachable
 * from the parent.  The caller has held cbuf's exclusive lock since the split
 * itself, so nobody else can have seen the flag, let alone finished the
 * split.  The caller sets the page's LSN.
 */
static void
bark_clear_incomplete_split(Buffer cbuf)
{
	BarkPageOpaque copaque = BarkPageGetOpaque(BufferGetPage(cbuf));

	Assert((copaque->bark_flags & BARK_INCOMPLETE_SPLIT) != 0);
	copaque->bark_flags &= ~BARK_INCOMPLETE_SPLIT;
	MarkBufferDirty(cbuf);
}

/*
 * Add `itup` at offset `off` on the write-locked page `buf`, and WAL-log it,
 * as _bt_insertonpg does for an insert that needs no split.  When `cbuf` is
 * valid, `itup` is the downlink to the right half of a split of the child
 * `cbuf`, which the caller has kept write-locked since the split; its
 * BARK_INCOMPLETE_SPLIT flag is cleared in the same record (INSERT_UPPER),
 * so the split completes in the step that makes its right half reachable
 * from the parent.  Otherwise this is a leaf insert (INSERT_LEAF).  The
 * caller has checked that the entry fits.
 *
 * The entry is MAXALIGN-sized, as every SINGLE, OVERSIZED and pivot entry
 * is, and so is its coded form on a BARK_PREFIX leaf: PageAddItem copies
 * only the entry's own bytes, and alignment padding taken from the free space
 * could differ between primary and standby.  The entry is coded here and the
 * coded bytes logged, so redo adds them as they are; the caller's fit test
 * used bark_coded_size.
 */
static void
bark_insert_entry(Relation index, Buffer buf, IndexTuple itup,
				  OffsetNumber off, Buffer cbuf)
{
	Page		page = BufferGetPage(buf);
	IndexTuple	coded = bark_prefix_encode(page, itup);
	Size		itemsz = IndexTupleSize(coded);
	XLogRecPtr	recptr;

	Assert(itemsz == MAXALIGN(itemsz));

	/* No ereport(ERROR) until changes are logged */
	START_CRIT_SECTION();

	if (PageAddItem(page, coded, itemsz, off, false, false) == InvalidOffsetNumber)
		elog(PANIC, "failed to add entry to block %u in BARK index \"%s\"",
			 BufferGetBlockNumber(buf), RelationGetRelationName(index));

	MarkBufferDirty(buf);

	if (BufferIsValid(cbuf))
		bark_clear_incomplete_split(cbuf);

	if (RelationNeedsWAL(index))
	{
		xl_bark_insert xlrec;
		uint8		xlinfo = XLOG_BARK_INSERT_LEAF;

		xlrec.offnum = off;

		XLogBeginInsert();
		XLogRegisterData(&xlrec, SizeOfBarkInsert);
		XLogRegisterBuffer(0, buf, REGBUF_STANDARD);
		XLogRegisterBufData(0, coded, itemsz);
		if (BufferIsValid(cbuf))
		{
			xlinfo = XLOG_BARK_INSERT_UPPER;
			XLogRegisterBuffer(1, cbuf, REGBUF_STANDARD);
		}

		recptr = XLogInsert(RM_BARK_ID, xlinfo);
	}
	else
		recptr = XLogGetFakeLSN(index);

	PageSetLSN(page, recptr);
	if (BufferIsValid(cbuf))
		PageSetLSN(BufferGetPage(cbuf), recptr);

	END_CRIT_SECTION();

	if (coded != itup)
		pfree(coded);
}

/*
 * Replace the leaf entry at `off` on the write-locked page `buf` with `itup`,
 * and WAL-log it (OVERWRITE).  PageIndexTupleOverwrite keeps the entry at its
 * offset and moves only the entries stored below it on the page, by the
 * change in size; redo makes the same call.  As in bark_insert_entry, the
 * entry is coded here and logged coded.  The caller has checked that the new
 * entry, at bark_coded_size, fits in the free space plus the old entry's.
 */
static void
bark_overwrite_entry(Relation index, Buffer buf, OffsetNumber off,
					 IndexTuple itup)
{
	Page		page = BufferGetPage(buf);
	IndexTuple	coded = bark_prefix_encode(page, itup);
	Size		itemsz = IndexTupleSize(coded);
	XLogRecPtr	recptr;

	/* No ereport(ERROR) until changes are logged */
	START_CRIT_SECTION();

	if (!PageIndexTupleOverwrite(page, off, coded, itemsz))
		elog(PANIC, "failed to replace entry at offset %u of block %u in BARK index \"%s\"",
			 off, BufferGetBlockNumber(buf), RelationGetRelationName(index));

	MarkBufferDirty(buf);

	if (RelationNeedsWAL(index))
	{
		xl_bark_overwrite xlrec;

		xlrec.offnum = off;

		XLogBeginInsert();
		XLogRegisterData(&xlrec, SizeOfBarkOverwrite);
		XLogRegisterBuffer(0, buf, REGBUF_STANDARD);
		XLogRegisterBufData(0, coded, itemsz);

		recptr = XLogInsert(RM_BARK_ID, XLOG_BARK_OVERWRITE);
	}
	else
		recptr = XLogGetFakeLSN(index);

	PageSetLSN(page, recptr);

	END_CRIT_SECTION();

	if (coded != itup)
		pfree(coded);
}

/*
 * Add heap TID `tid` to the LIST or POSTING entry at `off` on the
 * exclusive-locked leaf `buf`, replacing it with `ext`, the result of
 * bark_entry_add_tid on it, and log just the TID (XLOG_BARK_ADD_TID): replay
 * re-forms `ext` from the entry and the TID, and codes it for the page as
 * this does, which depends on nothing but the page's prefix and `ext`.  The
 * caller has checked that `ext` fits, at bark_coded_size.
 */
static void
bark_add_tid_entry(Relation index, Buffer buf, OffsetNumber off,
				   IndexTuple ext, ItemPointer tid)
{
	Page		page = BufferGetPage(buf);
	IndexTuple	coded = bark_prefix_encode(page, ext);
	XLogRecPtr	recptr;

	/* No ereport(ERROR) until changes are logged */
	START_CRIT_SECTION();

	if (!PageIndexTupleOverwrite(page, off, coded, IndexTupleSize(coded)))
		elog(PANIC, "failed to replace entry at offset %u of block %u in BARK index \"%s\"",
			 off, BufferGetBlockNumber(buf), RelationGetRelationName(index));

	MarkBufferDirty(buf);

	if (RelationNeedsWAL(index))
	{
		xl_bark_add_tid xlrec;

		xlrec.offnum = off;
		xlrec.tid = *tid;

		XLogBeginInsert();
		XLogRegisterData(&xlrec, SizeOfBarkAddTid);
		XLogRegisterBuffer(0, buf, REGBUF_STANDARD);

		recptr = XLogInsert(RM_BARK_ID, XLOG_BARK_ADD_TID);
	}
	else
		recptr = XLogGetFakeLSN(index);

	PageSetLSN(page, recptr);

	END_CRIT_SECTION();

	if (coded != ext)
		pfree(coded);
}

/*
 * Add heap TID `tid`, which falls inside the TID range of the LIST or POSTING
 * entry at `off` on the exclusive-locked leaf `buf` but does not fit in it,
 * by replacing the entry with `left` and adding `right` just after it, the
 * results of bark_entry_swap_tid on the entry, and log just the TID
 * (XLOG_BARK_INSERT_SWAP); replay repeats bark_entry_swap_tid.  The caller
 * has checked that both fit, at bark_coded_size.
 */
static void
bark_swap_tid_entry(Relation index, Buffer buf, OffsetNumber off,
					IndexTuple left, IndexTuple right, ItemPointer tid)
{
	Page		page = BufferGetPage(buf);
	IndexTuple	lcoded = bark_prefix_encode(page, left);
	IndexTuple	rcoded = bark_prefix_encode(page, right);
	XLogRecPtr	recptr;

	/* No ereport(ERROR) until changes are logged */
	START_CRIT_SECTION();

	if (!PageIndexTupleOverwrite(page, off, lcoded, IndexTupleSize(lcoded)) ||
		PageAddItem(page, rcoded, IndexTupleSize(rcoded), OffsetNumberNext(off),
					false, false) == InvalidOffsetNumber)
		elog(PANIC, "failed to divide entry at offset %u of block %u in BARK index \"%s\"",
			 off, BufferGetBlockNumber(buf), RelationGetRelationName(index));

	MarkBufferDirty(buf);

	if (RelationNeedsWAL(index))
	{
		xl_bark_insert_swap xlrec;

		xlrec.offnum = off;
		xlrec.tid = *tid;

		XLogBeginInsert();
		XLogRegisterData(&xlrec, SizeOfBarkInsertSwap);
		XLogRegisterBuffer(0, buf, REGBUF_STANDARD);

		recptr = XLogInsert(RM_BARK_ID, XLOG_BARK_INSERT_SWAP);
	}
	else
		recptr = XLogGetFakeLSN(index);

	PageSetLSN(page, recptr);

	END_CRIT_SECTION();

	if (lcoded != left)
		pfree(lcoded);
	if (rcoded != right)
		pfree(rcoded);
}

static void bark_insert_parent(Relation index, Relation heaprel,
							   BarkKeyInfo *keyinfo, BarkStack stack,
							   Buffer buf, IndexTuple downlink);
static void bark_split(Relation index, Relation heaprel, BarkKeyInfo *keyinfo,
					   BarkStack stack, Buffer buf, OffsetNumber newoff,
					   IndexTuple newitup, Buffer cbuf,
					   OffsetNumber replaceoff, IndexTuple replaceitup);

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
	Page		leafpage;
	Page		metapage;
	BarkMetaPageData *meta;
	BlockNumber leafblk;
	XLogRecPtr	recptr;

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
	leafpage = BufferGetPage(leafbuf);
	metapage = BufferGetPage(metabuf);

	/* No ereport(ERROR) until changes are logged */
	START_CRIT_SECTION();

	BarkPageInit(leafpage, BARK_P_NONE, BARK_P_NONE, 0, BARK_LEAF | BARK_ROOT, 0);
	MarkBufferDirty(leafbuf);

	meta = BarkPageGetMeta(metapage);
	meta->bark_root = leafblk;
	meta->bark_level = 0;
	MarkBufferDirty(metabuf);

	if (RelationNeedsWAL(index))
	{
		xl_bark_metadata md;

		md.root = leafblk;
		md.level = 0;

		XLogBeginInsert();
		XLogRegisterBuffer(0, leafbuf, REGBUF_WILL_INIT);
		XLogRegisterBuffer(1, metabuf, REGBUF_STANDARD);
		XLogRegisterBufData(1, &md, sizeof(xl_bark_metadata));

		recptr = XLogInsert(RM_BARK_ID, XLOG_BARK_CREATE_ROOT);
	}
	else
		recptr = XLogGetFakeLSN(index);

	PageSetLSN(leafpage, recptr);
	PageSetLSN(metapage, recptr);

	END_CRIT_SECTION();

	UnlockReleaseBuffer(leafbuf);
	UnlockReleaseBuffer(metabuf);
}

/*
 * Split `buf` (a full page) to make room for `newitup` at insert offset
 * `newoff`.  Allocates a right sibling, moves the items from the split point
 * bark_findsplitloc chooses onward to it, gives the left page a new high key,
 * chains the right links, and inserts the right page's downlink, a copy of
 * that high key, into the parent via the stack.  The split itself is one
 * XLOG_BARK_SPLIT record; the right link is published before the parent
 * downlink so a concurrent descender can always move right to find a key.
 *
 * `buf` is write-locked on entry and stays locked until bark_insert_parent
 * has write-locked the parent and written the downlink, so the left page's
 * BARK_INCOMPLETE_SPLIT flag is never visible to another backend while this
 * one is still completing the split (see bark_insert_parent).  `cbuf` is the
 * write-locked left half of a child split when `buf` is an internal page
 * receiving that child's downlink (InvalidBuffer for a leaf); its flag is
 * cleared in the split's WAL record and it is released once that is logged.
 * `heaprel` is the index's heap relation, for bark_get_free_page.
 *
 * When `replaceitup` is not NULL, the leaf entry at `replaceoff` is replaced
 * by it in the split, as nbtree's _bt_split takes the rewritten posting list
 * of a posting-list swap: an insert whose heap TID falls inside an entry's
 * range divides the entry (bark_entry_swap_tid), and when the two halves do
 * not fit on the page, they go into the split instead.  The split record
 * logs both halves whole, so redo needs nothing more.
 */
static void
bark_split(Relation index, Relation heaprel, BarkKeyInfo *keyinfo,
		   BarkStack stack, Buffer buf, OffsetNumber newoff,
		   IndexTuple newitup, Buffer cbuf, OffsetNumber replaceoff,
		   IndexTuple replaceitup)
{
	Page		origpage = BufferGetPage(buf);
	BarkPageOpaque origopaque = BarkPageGetOpaque(origpage);
	bool		isleaf = BarkPageIsLeaf(origopaque);
	BlockNumber origblk = BufferGetBlockNumber(buf);
	BlockNumber origleft = origopaque->bark_prev;
	BlockNumber origright = origopaque->bark_next;
	uint32		level = origopaque->bark_level;
	bool		origrightmost = BarkPageRightmost(origopaque);
	OffsetNumber firstdata = BarkPageFirstDataKey(origopaque);
	OffsetNumber maxoff = PageGetMaxOffsetNumber(origpage);

	/* Build the full ordered item list (existing items + the new one). */
	int			ntotal = (maxoff - firstdata + 1) + 1;
	IndexTuple *items = palloc(ntotal * sizeof(IndexTuple));
	Size	   *sizes = palloc(ntotal * sizeof(Size));
	char		origprefix[BARK_PREFIX_MAX];
	Size		origprefixlen = 0;
	Size		reserve = 0;
	int			n = 0;
	int			splitidx;
	IndexTuple	orighikey = NULL;
	Buffer		rbuf;
	Buffer		sbuf = InvalidBuffer;
	Page		rightpage;
	BlockNumber rightblk;
	Page		leftpage;
	IndexTuple	lhikey;
	IndexTuple	downlink;
	BTCycleId	cycleid = 0;
	uint16		leafflag = isleaf ? BARK_LEAF : 0;
	XLogRecPtr	recptr;

	/* Preserve the original high key (if any) for the new right page. */
	if (!origrightmost)
		orighikey = CopyIndexTuple((IndexTuple)
								   PageGetItem(origpage,
											   PageGetItemId(origpage,
															 BARK_P_HIKEY)));

	for (OffsetNumber off = firstdata; off <= maxoff; off = OffsetNumberNext(off))
	{
		BarkItemBuf ibuf;

		if (off == newoff)
			items[n++] = CopyIndexTuple(newitup);
		if (off == replaceoff && replaceitup != NULL)
			items[n++] = CopyIndexTuple(replaceitup);
		else
			items[n++] = CopyIndexTuple(BarkPageGetItem(origpage, off, &ibuf));
	}
	if (newoff > maxoff)
		items[n++] = CopyIndexTuple(newitup);
	Assert(n == ntotal);

	/*
	 * Choose the split point with each item's size as it is stored on the
	 * original page, coded against its prefix if it has one, and room on each
	 * half for that prefix (see bark_split_leaf_half).
	 */
	if (BarkPageHasPrefix(origopaque))
	{
		const char *p = bark_page_get_prefix(origpage, &origprefixlen);

		memcpy(origprefix, p, origprefixlen);
		reserve = MAXALIGN(sizeof(IndexTupleData) + origprefixlen) +
			sizeof(ItemIdData);
	}
	for (int i = 0; i < n; i++)
		sizes[i] = bark_coded_size(origpage, items[i]);

	/* The right page gets items[splitidx..]; the new item is at newoff. */
	splitidx = bark_findsplitloc(index, keyinfo, items, sizes, reserve, n,
								 newoff - firstdata, isleaf, orighikey);

	/*
	 * Form the left page's high key before anything is changed: an oversized
	 * key makes an OVERSIZED high key, which writes its own overflow chain
	 * under its own WAL records, and none of that can happen inside the
	 * split's critical section.  A leaf's high key is the right page's first
	 * key, truncated against the left page's last key.  An internal page's
	 * items are pivots already, so its high key is a copy of the right page's
	 * first item with the attributes that item has, as in nbtree.
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

	/*
	 * Stamp both halves of a leaf split with the cycle ID of the VACUUM now
	 * scanning this index (zero if none), as _bt_split does.  The entries just
	 * moved to the right page may land on a block VACUUM has already passed;
	 * the stamp is how barkbulkdelete notices and goes back for them.  It must
	 * be read while both pages are exclusive-locked, so a VACUUM that starts
	 * right after cannot process either page before the split is complete.
	 */
	if (isleaf)
		cycleid = _bt_vacuum_cycleid(index);

	/*
	 * Build both halves in temporary pages, so that a failure leaves the
	 * original page untouched; they are copied into the buffers in the
	 * critical section below, as _bt_split does with its left page.  Both
	 * get every entry in offset order, so each page's tuple area is exactly
	 * what the WAL record carries and redo's bark_restore_page re-adds.
	 *
	 * The left page is marked as having an unfinished split: its new right
	 * sibling exists and is right-linked, but the downlink that would make
	 * the sibling reachable from the parent is written in a separate step
	 * below.  A crash in between leaves the flag set; the next writer that
	 * descends here finishes the split (bark_finish_split).  The flag is
	 * cleared atomically with the downlink insert in bark_insert_parent.
	 *
	 * Redo sets each half's flags from the record's leaf and prefix flags
	 * alone, so the original page may carry no flag but BARK_LEAF,
	 * BARK_PREFIX and BARK_ROOT (which the left half gives up: a new root is
	 * made above it).
	 */
	Assert((origopaque->bark_flags &
			~(BARK_LEAF | BARK_ROOT | BARK_PREFIX)) == 0);
	leftpage = PageGetTempPage(origpage);
	BarkPageInit(leftpage, origleft, rightblk, level,
				 leafflag | BARK_INCOMPLETE_SPLIT, cycleid);

	/* The right page: the original high key, then items[splitidx..] */
	rightpage = PageGetTempPage(origpage);
	BarkPageInit(rightpage, origblk, origright, level, leafflag, cycleid);

	if (isleaf)
	{
		bark_split_leaf_half(index, leftpage, lhikey, items, splitidx,
							 origprefix, origprefixlen);
		bark_split_leaf_half(index, rightpage,
							 origrightmost ? NULL : orighikey,
							 items + splitidx, n - splitidx,
							 origprefix, origprefixlen);
	}
	else
	{
		OffsetNumber o = BARK_P_HIKEY;

		bark_page_insert_at(leftpage, lhikey, o++);
		for (int i = 0; i < splitidx; i++)
			bark_page_insert_at(leftpage, items[i], o++);

		o = BARK_P_HIKEY;
		if (!origrightmost)
			bark_page_insert_at(rightpage, orighikey, o++);	/* keep high key */
		for (int i = splitidx; i < n; i++)
			bark_page_insert_at(rightpage, items[i], o++);
	}
	pfree(lhikey);

	/*
	 * If the original page had a right sibling, that sibling's bark_prev must
	 * now point at the new right page.  Lock it now, before the critical
	 * section, left to right as every split does.
	 */
	if (!origrightmost)
	{
		sbuf = ReadBuffer(index, origright);
		LockBuffer(sbuf, BUFFER_LOCK_EXCLUSIVE);
	}

	/* No ereport(ERROR) until changes are logged */
	START_CRIT_SECTION();

	memcpy(BufferGetPage(rbuf), rightpage, BLCKSZ);
	memcpy(origpage, leftpage, BLCKSZ);
	MarkBufferDirty(rbuf);
	MarkBufferDirty(buf);

	if (BufferIsValid(sbuf))
	{
		BarkPageGetOpaque(BufferGetPage(sbuf))->bark_prev = rightblk;
		MarkBufferDirty(sbuf);
	}

	/*
	 * If this split is of an internal page receiving a child's downlink (cbuf
	 * valid), that downlink is being written here, so clear the child's
	 * BARK_INCOMPLETE_SPLIT flag atomically in the same record.
	 */
	if (BufferIsValid(cbuf))
		bark_clear_incomplete_split(cbuf);

	if (RelationNeedsWAL(index))
	{
		xl_bark_split xlrec;
		PageHeader	lhdr = (PageHeader) origpage;
		PageHeader	rhdr = (PageHeader) BufferGetPage(rbuf);

		xlrec.level = level;
		xlrec.flags = isleaf ? XLH_BARK_SPLIT_LEAF : 0;
		if (BarkPageHasPrefix(BarkPageGetOpaque(origpage)))
			xlrec.flags |= XLH_BARK_SPLIT_LPREFIX;
		if (BarkPageHasPrefix(BarkPageGetOpaque(BufferGetPage(rbuf))))
			xlrec.flags |= XLH_BARK_SPLIT_RPREFIX;
		xlrec.cycleid = cycleid;
		xlrec.leftprev = origleft;
		xlrec.rightnext = origright;

		XLogBeginInsert();
		XLogRegisterData(&xlrec, SizeOfBarkSplit);

		/*
		 * Both halves are rebuilt in redo from their logged entries alone,
		 * so neither needs a full-page image: the left page is registered as
		 * reinitialized, like the new right page.  This is where BARK departs
		 * from nbtree, which logs only the right half and rebuilds the left
		 * from the original page.
		 */
		XLogRegisterBuffer(0, buf, REGBUF_WILL_INIT);
		XLogRegisterBufData(0, (char *) origpage + lhdr->pd_upper,
							lhdr->pd_special - lhdr->pd_upper);
		XLogRegisterBuffer(1, rbuf, REGBUF_WILL_INIT);
		XLogRegisterBufData(1, (char *) rhdr + rhdr->pd_upper,
							rhdr->pd_special - rhdr->pd_upper);
		if (BufferIsValid(sbuf))
			XLogRegisterBuffer(2, sbuf, REGBUF_STANDARD);
		if (BufferIsValid(cbuf))
			XLogRegisterBuffer(3, cbuf, REGBUF_STANDARD);

		recptr = XLogInsert(RM_BARK_ID, XLOG_BARK_SPLIT);
	}
	else
		recptr = XLogGetFakeLSN(index);

	PageSetLSN(origpage, recptr);
	PageSetLSN(BufferGetPage(rbuf), recptr);
	if (BufferIsValid(sbuf))
		PageSetLSN(BufferGetPage(sbuf), recptr);
	if (BufferIsValid(cbuf))
		PageSetLSN(BufferGetPage(cbuf), recptr);

	END_CRIT_SECTION();

	pfree(leftpage);
	pfree(rightpage);
	if (BufferIsValid(cbuf))
		UnlockReleaseBuffer(cbuf);
	if (BufferIsValid(sbuf))
		UnlockReleaseBuffer(sbuf);

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
	pfree(sizes);
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
	Page		rootpage = BufferGetPage(rootbuf);
	Page		metapage = BufferGetPage(metabuf);
	BarkMetaPageData *meta;
	IndexTuple	leftdown;
	XLogRecPtr	recptr;

	/*
	 * First downlink is minus-infinity (zero key attributes): it routes every
	 * key below the split key to the left child.  Second is the split-key
	 * downlink to the right child -- the caller's `downlink`, which already
	 * points at the right half, reused as-is (re-forming it would re-fetch
	 * and re-write an oversized key's overflow chain).
	 */
	leftdown = index_truncate_tuple(RelationGetDescr(index), downlink, 0);
	BarkPivotSetNAtts(leftdown, 0);
	BarkPivotSetDownLink(leftdown, leftblk);

	/* No ereport(ERROR) until changes are logged */
	START_CRIT_SECTION();

	BarkPageInit(rootpage, BARK_P_NONE, BARK_P_NONE, childlevel + 1,
				 BARK_ROOT, 0);
	if (PageAddItem(rootpage, leftdown, IndexTupleSize(leftdown),
					BARK_P_HIKEY, false, false) == InvalidOffsetNumber ||
		PageAddItem(rootpage, downlink, IndexTupleSize(downlink),
					BARK_P_FIRSTKEY, false, false) == InvalidOffsetNumber)
		elog(PANIC, "failed to add downlinks to new root of BARK index \"%s\"",
			 RelationGetRelationName(index));
	MarkBufferDirty(rootbuf);

	/* Point the meta page at the new root. */
	meta = BarkPageGetMeta(metapage);
	meta->bark_root = rootblk;
	meta->bark_level = childlevel + 1;
	MarkBufferDirty(metabuf);

	/* Both halves are now reachable: the split completes in this record. */
	bark_clear_incomplete_split(lbuf);

	if (RelationNeedsWAL(index))
	{
		xl_bark_newroot xlrec;
		xl_bark_metadata md;
		PageHeader	rhdr = (PageHeader) rootpage;

		xlrec.rootblk = rootblk;
		xlrec.level = childlevel + 1;
		md.root = rootblk;
		md.level = childlevel + 1;

		XLogBeginInsert();
		XLogRegisterData(&xlrec, SizeOfBarkNewroot);
		XLogRegisterBuffer(0, rootbuf, REGBUF_WILL_INIT);
		XLogRegisterBufData(0, (char *) rootpage + rhdr->pd_upper,
							rhdr->pd_special - rhdr->pd_upper);
		XLogRegisterBuffer(1, lbuf, REGBUF_STANDARD);
		XLogRegisterBuffer(2, metabuf, REGBUF_STANDARD);
		XLogRegisterBufData(2, &md, sizeof(xl_bark_metadata));

		recptr = XLogInsert(RM_BARK_ID, XLOG_BARK_NEWROOT);
	}
	else
		recptr = XLogGetFakeLSN(index);

	PageSetLSN(rootpage, recptr);
	PageSetLSN(BufferGetPage(lbuf), recptr);
	PageSetLSN(metapage, recptr);

	END_CRIT_SECTION();

	pfree(leftdown);
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
			BarkItemBuf ibuf;
			IndexTuple	itup = BarkPageGetItem(page,
											   BarkPageFirstDataKey(opaque),
											   &ibuf);

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
				BarkItemBuf ibuf;
				IndexTuple	itup = BarkPageGetItem(page, off, &ibuf);

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
				BarkItemBuf ibuf;
				IndexTuple	itup = BarkPageGetItem(page, off, &ibuf);

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
		bark_insert_entry(index, pbuf, downlink, off, buf);
		UnlockReleaseBuffer(buf);
		UnlockReleaseBuffer(pbuf);
	}
	else
		bark_split(index, heaprel, keyinfo, stack->bark_parent, pbuf, off,
				   downlink, buf, InvalidOffsetNumber, NULL);
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
	 * `buf` is the first leaf that can hold itup's key: bark_insert descends
	 * for a unique check on the key alone (no heap TID) with nextkey=false,
	 * which follows the last downlink strictly less than the key and stops
	 * moving right at a high key equal to it.  Entries of equal keys are in
	 * heap TID order, so the live entry of the key, if there is one, can be
	 * anywhere in its run, which starts on this leaf at or after the first
	 * entry >= the key; scanning right from there while keys stay equal sees
	 * the whole run, across leaves.  As in nbtree's _bt_check_unique.
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
			BarkItemBuf ibuf;
			IndexTuple	mitup = BarkPageGetItem(page, mid, &ibuf);

			if (bark_compare_itups(keyinfo, index, itup, mitup) > 0)
				lo = OffsetNumberNext(mid);	/* mid < itup: go right */
			else
				hi = mid;					/* mid >= itup: go left */
		}

		for (off = lo; off <= maxoff; off = OffsetNumberNext(off))
		{
			BarkItemBuf ibuf;
			IndexTuple	curitup = BarkPageGetItem(page, off, &ibuf);
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
 * Outcome of bark_coalesce_list.  BARK_COALESCE_SPLIT asks the caller to
 * split the page with *replace in place of the entry before the insert
 * offset and *newitem as the item to insert.
 */
typedef enum BarkCoalesceResult
{
	BARK_COALESCE_NONE,			/* nothing done: insert a SINGLE */
	BARK_COALESCE_DONE,			/* the TID is in the index */
	BARK_COALESCE_SPLIT,		/* split with *replace and *newitem */
} BarkCoalesceResult;

/*
 * Try to coalesce `newtid` into an existing leaf entry on `buf` that has the
 * same key as `key` (a SINGLE-shape key tuple), forming or extending a LIST or
 * POSTING entry rather than adding another SINGLE.  Returns
 * BARK_COALESCE_DONE when it did (WAL-logged), and BARK_COALESCE_NONE (page
 * unchanged) when there is no equal entry, or when the merged entry would not
 * fit on this page or under the item ceiling -- in which case the caller
 * inserts a plain SINGLE and the duplicates stay as separate entries until a
 * later insert can merge them.
 *
 * The merged set is encoded as whichever shape is smaller: a LIST (sorted
 * locator array) for a modest number of duplicates, or a POSTING (sbm
 * serialization) once the set is large/clustered enough that the sbm envelope
 * beats the flat array.  bark_form_posting returns NULL when LIST would still
 * win, so the shape is chosen by actual encoded size, not a fixed count.
 *
 * Only called for non-unique indexes: a unique index never legitimately holds
 * two live tuples with the same key, so it never forms a LIST or POSTING.
 * `off` is the leaf insert position, the first entry that sorts after (key,
 * newtid), so the candidate, an entry of the key whose TID range starts at or
 * below newtid, if any, is at off-1.  When newtid lies above the candidate's
 * range, adding it keeps the run in heap TID order, and so does inserting a
 * SINGLE at off.  When it lies inside the range, it must go into the
 * candidate, since anywhere else it would break the order: if the candidate
 * cannot take it, it is divided around newtid (bark_entry_swap_tid, nbtree's
 * posting-list swap), in place when both parts fit on the page (an
 * XLOG_BARK_INSERT_SWAP record), else in a page split, which the caller makes
 * (BARK_COALESCE_SPLIT).  A newtid already in the candidate means the index
 * is corrupt, as nbtree reports for a duplicate heap TID.
 *
 * The common append case (the new locator sorts after every existing member,
 * as monotonic/append-ish heap TIDs do) is handled by an O(1)-amortized fast
 * path: a LIST entry is extended by appending the one new locator to its body
 * and bumping its count, without re-reading the set, re-sorting, or probing the
 * POSTING encoding.  Only when the LIST would exceed the item ceiling, or the
 * entry is a SINGLE, does it fall to the general path below, which re-reads
 * the full set and re-encodes it as whichever of LIST / POSTING is smaller.
 *
 * The POSTING case is O(serialized size) per insert (the sbm serialization
 * has no in-place append, so it is deserialized, added to, and re-serialized).
 * POSTING is only chosen for a large, clustered set whose per-key members are
 * in any case capped by the item ceiling, so this is bounded.
 */
static BarkCoalesceResult
bark_coalesce_list(Relation index, BarkKeyInfo *keyinfo, IndexTuple key,
				   ItemPointer newtid, Buffer buf, OffsetNumber off,
				   IndexTuple *replace, IndexTuple *newitem)
{
	Page		page = BufferGetPage(buf);
	BarkPageOpaque opaque = BarkPageGetOpaque(page);
	OffsetNumber firstdata = BarkPageFirstDataKey(opaque);
	OffsetNumber eqoff;
	ItemId		iid;
	IndexTuple	cur;
	BarkItemBuf ibuf;
	ItemPointerData lo;
	ItemPointerData hi;
	bool		inside;
	Size		room;
	IndexTuple	newentry;
	IndexTuple	left;
	IndexTuple	right;

	/* No entry precedes the insert point: nothing to coalesce with. */
	if (off <= firstdata)
		return BARK_COALESCE_NONE;
	eqoff = OffsetNumberPrev(off);
	iid = PageGetItemId(page, eqoff);
	cur = BarkPageGetItem(page, eqoff, &ibuf);

	/* Only coalesce with a leaf-data entry whose key equals the new key. */
	if (!BarkEntryIsLeafData(cur) ||
		bark_compare_itups(keyinfo, index, key, cur) != 0)
		return BARK_COALESCE_NONE;

	bark_entry_tid_range(cur, &lo, &hi);
	Assert(ItemPointerCompare(&lo, newtid) <= 0);
	inside = ItemPointerCompare(newtid, &hi) <= 0;
	if (inside && bark_entry_has_tid(cur, newtid))
		elog(ERROR, "heap TID (%u,%u) already in BARK entry at offset %u of block %u in index \"%s\"",
			 ItemPointerGetBlockNumber(newtid),
			 ItemPointerGetOffsetNumber(newtid), eqoff,
			 BufferGetBlockNumber(buf), RelationGetRelationName(index));

	room = bark_leaf_free_space(page) + MAXALIGN(ItemIdGetLength(iid));

	/*
	 * Fast path: add the one new locator to the existing LIST or POSTING
	 * entry (bark_entry_add_tid).  For a LIST in the common append case the
	 * body is extended by one locator, no re-read, no POSTING probe, so
	 * building one key's set by repeated single inserts is O(1) amortized. A
	 * POSTING is deserialized once, added to and re-serialized once.  A
	 * POSTING never shrinks back to a LIST on insert.  Either way only the
	 * TID is logged.  On a BARK_PREFIX page the entry is stored coded, which
	 * may take a few bytes more than the plain entry bark_entry_add_tid
	 * measured.
	 */
	if (BarkEntryGetShape(cur) == BARK_SHAPE_LIST ||
		BarkEntryGetShape(cur) == BARK_SHAPE_POSTING)
	{
		IndexTuple	ext = bark_entry_add_tid(cur, newtid,
											 Min((Size) BarkMaxItemSize, room));

		if (ext != NULL && bark_coded_size(page, ext) <= room)
		{
			bark_add_tid_entry(index, buf, eqoff, ext, newtid);
			pfree(ext);
			return BARK_COALESCE_DONE;
		}
		if (ext != NULL)
			pfree(ext);

		/*
		 * A LIST that takes no more members may still become a POSTING (the
		 * general path below).  Otherwise an outside TID gets an entry of its
		 * own, and an inside one divides the entry.
		 */
		if (BarkEntryGetShape(cur) == BARK_SHAPE_POSTING)
		{
			if (!inside)
				return BARK_COALESCE_NONE;
			goto swap;
		}
	}
	else
	{
		/* A SINGLE (or OVERSIZED) entry has no TID range to be inside. */
		Assert(!inside);
		if (BarkEntryGetShape(cur) != BARK_SHAPE_SINGLE)
			return BARK_COALESCE_NONE;
	}

	/*
	 * General path: re-encode the entry's locators plus the new one as
	 * whichever shape is smaller.  bark_form_posting returns NULL when the
	 * LIST form would be no larger, so a small set stays a LIST and a large/
	 * clustered one is promoted to POSTING -- the LIST -> POSTING promotion
	 * happens at the size crossover.
	 */
	{
		int			nold = bark_entry_count_tids(cur);
		ItemPointer all = palloc_array(ItemPointerData, nold + 1);
		int			ins;
		IndexTuple	posting;

		nold = bark_entry_get_tids(cur, all, nold);
		for (ins = nold; ins > 0 && ItemPointerCompare(&all[ins - 1], newtid) > 0;)
			ins--;
		memmove(&all[ins + 1], &all[ins], (nold - ins) * sizeof(ItemPointerData));
		all[ins] = *newtid;

		posting = bark_form_posting(RelationGetDescr(index), key, all, nold + 1);
		if (posting != NULL)
			newentry = posting;
		else if (nold + 1 <= BARK_LIST_MAX_COUNT)
			newentry = bark_form_list(RelationGetDescr(index), key, all,
									  nold + 1);
		else
			newentry = NULL;
		pfree(all);
	}
	if (newentry != NULL &&
		MAXALIGN(IndexTupleSize(newentry)) <= BarkMaxItemSize &&
		bark_coded_size(page, newentry) <= room)
	{
		bark_overwrite_entry(index, buf, eqoff, newentry);
		pfree(newentry);
		return BARK_COALESCE_DONE;
	}
	if (newentry != NULL)
		pfree(newentry);
	if (!inside)
		return BARK_COALESCE_NONE;

swap:

	/*
	 * newtid falls inside the entry, which cannot take it: divide the entry
	 * around it.  In place when both parts fit (the line pointer
	 * PageGetFreeSpace holds back is the new part's), else in a split.
	 */
	bark_entry_swap_tid(cur, newtid, &left, &right);
	if (bark_coded_size(page, left) + bark_coded_size(page, right) <= room)
	{
		bark_swap_tid_entry(index, buf, eqoff, left, right, newtid);
		pfree(left);
		pfree(right);
		return BARK_COALESCE_DONE;
	}
	*replace = left;
	*newitem = right;
	return BARK_COALESCE_SPLIT;
}

/*
 * After a unique check, move right from `buf`, the first leaf that can hold
 * itup's key, to the leaf where itup belongs by heap TID, and return it
 * write-locked.  As nbtree's _bt_findinsertloc and _bt_stepright do, the
 * right page is locked before the left one is released, so another inserter
 * of the key, which must check from the first leaf, cannot get past this one
 * before its entry is in place.  Incomplete splits met on the way are
 * finished (`stack` is the path to the first leaf), and deleted or half-dead
 * pages stepped over.  Rarely moves at all: only when dead entries of the key
 * fill the first leaf.
 */
static Buffer
bark_insert_stepright(Relation index, BarkKeyInfo *keyinfo, IndexTuple itup,
					  Buffer buf, BarkStack stack)
{
	for (;;)
	{
		Page		page = BufferGetPage(buf);
		BarkPageOpaque opaque = BarkPageGetOpaque(page);
		IndexTuple	hikey;
		BlockNumber rblkno;
		Buffer		rbuf;

		/* A heap TID equal to the high key's belongs right. */
		if (BarkPageRightmost(opaque))
			return buf;
		hikey = (IndexTuple) PageGetItem(page, PageGetItemId(page, BARK_P_HIKEY));
		if (bark_compare_itups_tid(keyinfo, index, itup, &itup->t_tid,
								   hikey) < 0)
			return buf;

		rblkno = opaque->bark_next;
		for (;;)
		{
			BarkPageOpaque ropaque;

			rbuf = ReadBuffer(index, rblkno);
			LockBuffer(rbuf, BUFFER_LOCK_EXCLUSIVE);
			ropaque = BarkPageGetOpaque(BufferGetPage(rbuf));
			if ((ropaque->bark_flags & BARK_INCOMPLETE_SPLIT) != 0)
			{
				bark_finish_split(index, keyinfo, rbuf, stack); /* releases */
				continue;
			}
			if (!BarkPageIgnore(ropaque))
				break;
			if (BarkPageRightmost(ropaque))
				elog(ERROR, "fell off the end of BARK index \"%s\"",
					 RelationGetRelationName(index));
			rblkno = ropaque->bark_next;
			UnlockReleaseBuffer(rbuf);
		}
		UnlockReleaseBuffer(buf);
		buf = rbuf;
	}
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
	bool		checkingunique = false;
	bool		result = false;		/* significant only for UNIQUE_CHECK_PARTIAL */

	itup->t_tid = *ht_ctid;		/* SINGLE shape: locator in t_tid */
	keyinfo->heaprel = heapRel; /* for pages a split allocates */

	/*
	 * A uniqueness check is skipped when the caller doesn't want it, and when
	 * the new key has any NULL attribute (SQL treats NULLs as distinct, so a
	 * NULL key never conflicts).
	 */
	if (checkUnique != UNIQUE_CHECK_NO)
	{
		checkingunique = true;
		for (int i = 0; i < IndexRelationGetNumberOfKeyAttributes(index); i++)
		{
			if (isnull[i])
			{
				checkingunique = false;
				result = true;	/* a NULL key is unique */
				break;
			}
		}
	}

	/*
	 * `itup` is the full in-memory key tuple at any size; bark_compare_itups
	 * compares it directly (index_getattr does not care about the 8191-byte
	 * cap).  Only when the entry is actually placed on a page is an oversized
	 * key written to an overflow chain and replaced by a small OVERSIZED entry
	 * (done at the leaf-insert / coalesce / split sites below).
	 *
	 * Entries of equal keys are in heap TID order, so an insert normally
	 * descends with its heap TID as well as its key, straight to the leaf the
	 * entry belongs on.  A unique check instead needs the first leaf that can
	 * hold the key, where any existing entry of it starts: as in nbtree's
	 * _bt_doinsert, it descends on the key alone, checks from there, and only
	 * then moves right to the leaf for the heap TID (bark_insert_stepright).
	 */
retry:
	buf = bark_search(index, keyinfo, itup,
					  checkingunique ? NULL : &itup->t_tid, true,
					  !checkingunique, &stack);

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

	/*
	 * Serializable conflict check: inserting here conflicts with a concurrent
	 * serializable transaction that read this leaf page.  BARK sets
	 * ampredlocks, so this is our responsibility rather than the generic
	 * index layer's.  Done while holding the write lock on the target leaf,
	 * before the insert or split.  For a unique check this is the first leaf
	 * that can hold the key rather than, rarely, a right sibling the entry
	 * goes to; as in nbtree that is enough, since every scan that could see
	 * the entry reads the first leaf too.
	 */
	CheckForSerializableConflictIn(index, NULL, BufferGetBlockNumber(buf));

	/*
	 * Uniqueness check.  If a conflicting tuple is still in progress,
	 * bark_check_unique returns its xact id: wait for that transaction to
	 * finish, then re-descend and check again.  If the conflict is with a
	 * speculative insertion (INSERT ... ON CONFLICT), it also returns the
	 * speculative token, and we wait on the speculative insertion -- which
	 * wakes us the moment the speculative inserter confirms or kills its tuple,
	 * so a losing speculative insert of the same key does not block to end of
	 * xact.  This matches nbtree's _bt_doinsert speculative-wait path exactly.
	 */
	if (checkingunique)
	{
		TransactionId xwait;
		uint32		speculativeToken;
		bool		is_unique;

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
	}

	/*
	 * UNIQUE_CHECK_EXISTING only verifies that the already-inserted tuple is
	 * unique; it must not add another index entry.
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

	if (checkingunique)
		buf = bark_insert_stepright(index, keyinfo, itup, buf, stack);
	page = BufferGetPage(buf);
	off = bark_leaf_insert_off(index, keyinfo, itup, &itup->t_tid, page);

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
	if (!indexInfo->ii_Unique && !oversized && bark_allequalimage(index))
	{
		IndexTuple	replace = NULL;
		IndexTuple	newitem = NULL;

		switch (bark_coalesce_list(index, keyinfo, itup, &itup->t_tid, buf,
								   off, &replace, &newitem))
		{
			case BARK_COALESCE_NONE:
				break;
			case BARK_COALESCE_DONE:
				UnlockReleaseBuffer(buf);
				buf = InvalidBuffer;
				break;
			case BARK_COALESCE_SPLIT:

				/*
				 * The entry at off - 1 is divided; its upper part goes at
				 * off.
				 */
				bark_split(index, heapRel, keyinfo, stack, buf, off, newitem,
						   InvalidBuffer, OffsetNumberPrev(off), replace);
				buf = InvalidBuffer;	/* bark_split released it */
				pfree(replace);
				pfree(newitem);
				break;
		}
	}

	if (BufferIsValid(buf))
	{
		/* The entry actually placed on the page (OVERSIZED when oversized). */
		IndexTuple	entry = bark_leaf_page_entry(index, heapRel, itup,
												 oversized, fulllen);

		/* The page footprint: the entry coded for the page, if it codes. */
		if (bark_leaf_free_space(page) >= bark_coded_size(page, entry))
		{
			bark_insert_entry(index, buf, entry, off, InvalidBuffer);
			UnlockReleaseBuffer(buf);
		}
		else
		{
			bool		roomnow = false;

			/*
			 * The entry is a new version of a row whose key here did not
			 * change: before splitting, delete the entries of dead versions
			 * (bottom-up deletion, barkdelete.c).  Deleting shifts offsets,
			 * so find the entry's place again whether or not that made room.
			 */
			if (indexUnchanged && heapRel != NULL)
			{
				roomnow = bark_bottomup_delete(index, heapRel, keyinfo, buf,
											   itup,
											   bark_coded_size(page, entry));
				off = bark_leaf_insert_off(index, keyinfo, itup, &itup->t_tid,
										   page);
			}
			if (roomnow)
			{
				bark_insert_entry(index, buf, entry, off, InvalidBuffer);
				UnlockReleaseBuffer(buf);
			}
			else
			{
				/* A leaf split: no child's incomplete split to finish. */
				bark_split(index, heapRel, keyinfo, stack, buf, off, entry,
						   InvalidBuffer, InvalidOffsetNumber, NULL);
				buf = InvalidBuffer;	/* bark_split released it */
			}
		}
		pfree(entry);
	}

	if (stack)
		bark_freestack(stack);
	pfree(itup);
	pfree(keyinfo);
	return result;
}
