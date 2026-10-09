/*-------------------------------------------------------------------------
 *
 * verify_bark.c
 *		Verify the structural integrity of a BARK index.
 *
 * bark_index_check(index regclass) walks every page of a BARK index and
 * checks the invariants the access method relies on:
 *
 *	- the meta page's on-disk version is one this server reads;
 *	- within each page, data entries are in non-decreasing key order, and on
 *	  a leaf, entries of equal keys have disjoint, ascending heap TID ranges;
 *	- every data key is less than or equal to the page's high key (the bound
 *	  the page's parent downlink promises), and strictly less when suffix
 *	  truncation dropped key attributes from the high key, which has between
 *	  one and all of the key attributes; a leaf entry equal to the high key
 *	  lies below the high key's heap TID;
 *	- sibling links are consistent (the right sibling's left link points back,
 *	  and levels match across a sibling link), and the right sibling's first
 *	  key is not less than the page's high key, (key, heap TID) on a leaf;
 *	- every downlink points at a page one level down whose first key is not
 *	  less than the downlink, (key, heap TID) for a leaf child;
 *	- no page is still flagged with an unfinished split, which a clean index
 *	  never leaves behind;
 *	- no leaf entry is larger than BarkMaxItemSize, no SINGLE entry is large
 *	  enough to belong on an overflow chain, and every POSTING entry
 *	  reserves room for the largest encoding of any subset of its set;
 *	- a page flagged BARK_PREFIX is a leaf with a well-formed PREFIX item of
 *	  1..BARK_PREFIX_MAX bytes, and every entry on it decodes within its
 *	  bounds, sharing no more of the prefix than there is (the key checks
 *	  above then apply to the decoded entries).
 *
 * Two optional checks follow, as in bt_index_check and bt_index_parent_check
 * (verify_nbtree.c):
 *
 *	- heapallindexed: every heap tuple that a fresh CREATE INDEX would index
 *	  has an entry.  The entries' (key, heap TID) pairs are fingerprinted into
 *	  a Bloom filter, each member of a LIST or POSTING entry as the SINGLE
 *	  entry it would be on its own, and the heap is then scanned, probing the
 *	  filter with each indexable tuple;
 *	- bark_index_parent_check: the tree is walked level by level from the
 *	  root, under a ShareLock that keeps writers out, and each internal page's
 *	  downlinks must name, in order, exactly the pages of the level below,
 *	  each downlink equal to the high key of the child before it (the
 *	  separator a split or CREATE INDEX copies from it), and the parent's
 *	  high key equal to its last child's.  Every live page must be reached.
 *
 * bark_index_check holds only AccessShareLock, so the index can change
 * underneath.  Each cross-page check holds share locks on both pages, taken
 * in an order the write paths also use (left page before right sibling,
 * child before parent), so it cannot deadlock with a split and sees the two
 * pages consistently.
 *
 * Copyright (c) 2017-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/amcheck/verify_bark.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bark.h"
#include "access/detoast.h"
#include "access/genam.h"
#include "access/htup_details.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/index.h"
#include "catalog/pg_am_d.h"
#include "common/pg_prng.h"
#include "fmgr.h"
#include "lib/bloomfilter.h"
#include "lib/sbm.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "verify_common.h"

PG_FUNCTION_INFO_V1(bark_index_check);
PG_FUNCTION_INFO_V1(bark_index_parent_check);

/* State of one verification, passed through amcheck_lock_relation_and_check. */
typedef struct BarkCheckState
{
	bool		heapallindexed; /* check that every heap tuple is indexed */

	/* heapallindexed only: */
	Relation	heaprel;		/* the index's table */
	Snapshot	snapshot;		/* the heap scan's snapshot */
	bloom_filter *filter;		/* fingerprints of the (key, TID) pairs */
	int64		heaptuplespresent;	/* heap tuples found in the filter */
	bool		readonly;		/* ShareLock held (bark_index_parent_check) */
	MemoryContext tmpcxt;		/* reset after every fingerprinted entry */
} BarkCheckState;

static void bark_check_common(FunctionCallInfo fcinfo, bool parentcheck);
static void bark_check_structure(Relation rel, Relation heaprel,
								  void *callback_state, bool readonly);
static bool bark_check_page(Relation rel, BlockNumber blkno, BarkKeyInfo *keyinfo);
static void bark_check_levels(Relation rel, BarkKeyInfo *keyinfo,
							  BlockNumber npages, int64 nlive);
static void bark_fingerprint_leaves(Relation rel, BarkCheckState *state);
static void bark_check_heap(Relation rel, Relation heaprel,
							BarkCheckState *state, bool readonly);
static void bark_check_downlinks(Relation rel, BlockNumber blkno,
								 BarkKeyInfo *keyinfo);
static void bark_check_marker(Relation rel, BarkKeyInfo *keyinfo,
							  BlockNumber blkno, OffsetNumber off,
							  IndexTuple itup);
static void bark_check_list(Relation rel, BlockNumber blkno, OffsetNumber off,
							IndexTuple itup);
static void bark_check_posting(Relation rel, BlockNumber blkno, OffsetNumber off,
							   IndexTuple itup);
static void bark_check_oversized(Relation rel, BlockNumber blkno, OffsetNumber off,
								 IndexTuple itup, BarkKeyInfo *keyinfo);

/*
 * Compare the leaf entry `itup`, taken at its lowest heap TID (or its highest
 * when `high`), with the pivot or leaf entry `other`, in the tree's order of
 * (key, heap TID): bark_compare_itups_tid.
 */
static int
bark_check_compare_leaf(Relation rel, BarkKeyInfo *keyinfo, IndexTuple itup,
						bool high, IndexTuple other)
{
	ItemPointerData lo;
	ItemPointerData hi;

	bark_entry_tid_range(itup, &lo, &hi);
	return bark_compare_itups_tid(keyinfo, rel, itup, high ? &hi : &lo, other);
}

/*
 * bark_index_check(index regclass, heapallindexed boolean)
 *
 * Verify the structural integrity of a BARK index, and with heapallindexed
 * that every heap tuple has an entry.  Takes AccessShareLock on the heap and
 * index, as bt_index_check does.  The one-argument form of amcheck 1.6
 * reaches this function too.
 */
Datum
bark_index_check(PG_FUNCTION_ARGS)
{
	bark_check_common(fcinfo, false);
	PG_RETURN_VOID();
}

/*
 * bark_index_parent_check(index regclass, heapallindexed boolean)
 *
 * As bark_index_check, plus the checks of each level against the level
 * above.  Takes ShareLock on the heap and index, as bt_index_parent_check
 * does, so no page changes while it runs.
 */
Datum
bark_index_parent_check(PG_FUNCTION_ARGS)
{
	bark_check_common(fcinfo, true);
	PG_RETURN_VOID();
}

static void
bark_check_common(FunctionCallInfo fcinfo, bool parentcheck)
{
	Oid			indrelid = PG_GETARG_OID(0);
	BarkCheckState state = {0};

	if (PG_NARGS() >= 2)
		state.heapallindexed = PG_GETARG_BOOL(1);

	amcheck_lock_relation_and_check(indrelid,
									BARK_AM_OID,
									bark_check_structure,
									parentcheck ? ShareLock : AccessShareLock,
									&state);
}

/*
 * Main entry: check every page's own invariants, then, with the stronger
 * lock, each level against its parent, then, if asked, the heap against the
 * index.  readonly is true under ShareLock (bark_index_parent_check).
 */
static void
bark_check_structure(Relation rel, Relation heaprel, void *callback_state,
					  bool readonly)
{
	BarkCheckState *state = (BarkCheckState *) callback_state;
	BarkKeyInfo *keyinfo = bark_build_keyinfo(rel);
	BlockNumber npages = RelationGetNumberOfBlocks(rel);
	Buffer		metabuf = ReadBuffer(rel, BARK_METAPAGE);
	uint32		version;
	int64		nlive = 0;

	LockBuffer(metabuf, BUFFER_LOCK_SHARE);
	version = BarkPageGetMeta(BufferGetPage(metabuf))->bark_version;
	UnlockReleaseBuffer(metabuf);
	if (version < BARK_MIN_VERSION)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("index \"%s\" was built by an older BARK version",
						RelationGetRelationName(rel)),
				 errhint("REINDEX the index.")));

	/*
	 * The heap scan must see only tuples whose entries the index walk will
	 * find, so take its snapshot before the walk starts, as verify_nbtree.c
	 * does.  See bt_check_every_level for why an old transaction snapshot
	 * may not be usable.
	 */
	if (state->heapallindexed)
	{
		/*
		 * An extracted column makes several entries of one row, and the heap
		 * scan would have to form them all, through the operator class's
		 * extraction procedure, to probe for each.
		 */
		if (bark_index_extracted_column(rel) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("heapallindexed is not supported for index \"%s\", which has a multikey column",
							RelationGetRelationName(rel))));

		state->snapshot = RegisterSnapshot(GetTransactionSnapshot());
		if (IsolationUsesXactSnapshot() && rel->rd_index->indcheckxmin &&
			!TransactionIdPrecedes(HeapTupleHeaderGetXmin(rel->rd_indextuple->t_data),
								   state->snapshot->xmin))
			ereport(ERROR,
					(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
					 errmsg("index \"%s\" cannot be verified using transaction snapshot",
							RelationGetRelationName(rel))));
	}

	for (BlockNumber blkno = BARK_METAPAGE + 1; blkno < npages; blkno++)
	{
		CHECK_FOR_INTERRUPTS();
		if (bark_check_page(rel, blkno, keyinfo))
			nlive++;
		bark_check_downlinks(rel, blkno, keyinfo);
	}

	if (readonly)
		bark_check_levels(rel, keyinfo, npages, nlive);

	if (state->heapallindexed)
	{
		bark_check_heap(rel, heaprel, state, readonly);
		UnregisterSnapshot(state->snapshot);
	}

	pfree(keyinfo);
}

/*
 * Check one page's invariants, plus the consistency of its right-sibling link.
 * Returns whether the page is part of the tree: an initialized page that is
 * not an overflow, deleted or half-dead page.
 */
static bool
bark_check_page(Relation rel, BlockNumber blkno, BarkKeyInfo *keyinfo)
{
	Buffer		buf = ReadBuffer(rel, blkno);
	Page		page;
	BarkPageOpaque opaque;
	OffsetNumber maxoff;
	OffsetNumber firstdata;
	IndexTuple	hikey = NULL;
	int			hikeynatts = 0;
	IndexTuple	prev = NULL;
	BarkItemBuf ibuf[2];		/* this item's and the previous one's */
	int			cur = 0;
	bool		live;

	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);

	if (PageIsNew(page))
	{
		UnlockReleaseBuffer(buf);
		return false;
	}

	opaque = BarkPageGetOpaque(page);

	/* Overflow pages hold raw out-of-line bytes, not tree items; skip them. */
	if (BarkPageIsOverflow(opaque))
	{
		UnlockReleaseBuffer(buf);
		return false;
	}

	/*
	 * A deleted page has been unlinked from the tree and holds no items, only
	 * its safexid.  It keeps the sibling links it had when it was deleted,
	 * for readers that still held a link to it, and those siblings need not
	 * link back to it any more, so the link checks below do not apply.  Check
	 * only that it has the deleted-page layout (see BarkDeletedPageData).
	 */
	if (BarkPageIsDeleted(opaque))
	{
		PageHeader	phdr = (PageHeader) page;

		if (phdr->pd_lower != SizeOfPageHeaderData ||
			phdr->pd_upper != phdr->pd_special -
			MAXALIGN(sizeof(BarkDeletedPageData)) ||
			(opaque->bark_flags & (BARK_LEAF | BARK_OVERFLOW)) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg("BARK index \"%s\" has a deleted page %u that does not have the deleted-page layout",
							RelationGetRelationName(rel), blkno)));
		UnlockReleaseBuffer(buf);
		return false;
	}

	/* A clean index never leaves an unfinished split behind. */
	if ((opaque->bark_flags & BARK_INCOMPLETE_SPLIT) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("BARK index \"%s\" has an unfinished split on page %u",
						RelationGetRelationName(rel), blkno)));

	maxoff = PageGetMaxOffsetNumber(page);
	firstdata = BarkPageFirstDataKey(opaque);

	/*
	 * A prefix-compressed page is a leaf whose PREFIX item, just after the
	 * high key, is an IndexTupleData header with nothing but its size set,
	 * followed by the prefix.  Its entries are checked as they decode, below;
	 * bark_prefix_decode reports one that does not decode within its bounds
	 * or claims more of the prefix than the page has.
	 */
	if (BarkPageHasPrefix(opaque))
	{
		OffsetNumber poff = BarkPagePrefixOff(opaque);
		ItemId		iid;
		IndexTuple	pitem;
		Size		plen;

		if (!BarkPageIsLeaf(opaque) || maxoff < poff)
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg("BARK index \"%s\" has a prefix-compressed page %u that is not a leaf with a prefix item",
							RelationGetRelationName(rel), blkno)));
		iid = PageGetItemId(page, poff);
		pitem = (IndexTuple) PageGetItem(page, iid);
		plen = ItemIdGetLength(iid);
		if (plen <= sizeof(IndexTupleData) ||
			plen > sizeof(IndexTupleData) + BARK_PREFIX_MAX ||
			pitem->t_info != plen ||
			ItemPointerGetBlockNumberNoCheck(&pitem->t_tid) != 0 ||
			ItemPointerGetOffsetNumberNoCheck(&pitem->t_tid) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg("BARK index \"%s\" has a malformed prefix item on page %u",
							RelationGetRelationName(rel), blkno)));
	}

	/*
	 * The high key, when present, is the first item on a non-rightmost page.
	 * It is a pivot with at least one key attribute: only a page's downlink
	 * can be minus infinity.
	 */
	if (!BarkPageRightmost(opaque) && maxoff >= BARK_P_HIKEY)
	{
		hikey = (IndexTuple) PageGetItem(page, PageGetItemId(page, BARK_P_HIKEY));
		if (BarkEntryIsLeafData(hikey) &&
			(BarkEntryGetShape(hikey) != BARK_SHAPE_OVERSIZED ||
			 BarkOverflowIsLeaf(hikey)))
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg("BARK index \"%s\" has a high key on page %u that is not a pivot",
							RelationGetRelationName(rel), blkno)));
		hikeynatts = BarkEntryGetPivotNAtts(hikey);
		if (hikeynatts < 1 || hikeynatts > keyinfo->nkeys)
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg("BARK index \"%s\" has a high key with %d key attributes on page %u",
							RelationGetRelationName(rel), hikeynatts, blkno)));
	}

	for (OffsetNumber off = firstdata; off <= maxoff;
		 off = OffsetNumberNext(off))
	{
		IndexTuple	itup = BarkPageGetItem(page, off, &ibuf[cur]);

		/*
		 * Keys must be in non-decreasing order within the page, and on a
		 * leaf, the heap TIDs of a run of equal keys in ascending order: each
		 * entry's lowest TID above the previous entry's highest.
		 */
		if (prev != NULL)
		{
			int			cmp = bark_compare_itups(keyinfo, rel, prev, itup);

			if (cmp > 0)
				ereport(ERROR,
						(errcode(ERRCODE_INDEX_CORRUPTED),
						 errmsg("BARK index \"%s\" has out-of-order keys on page %u at offset %u",
								RelationGetRelationName(rel), blkno, off)));
			if (cmp == 0 && BarkPageIsLeaf(opaque) &&
				bark_check_compare_leaf(rel, keyinfo, prev, true, itup) >= 0)
				ereport(ERROR,
						(errcode(ERRCODE_INDEX_CORRUPTED),
						 errmsg("BARK index \"%s\" has out-of-order heap TIDs among equal keys on page %u at offset %u",
								RelationGetRelationName(rel), blkno, off)));
		}

		/*
		 * Every data key must be within the page's high-key bound.  Equal
		 * keys may sit on both sides of a page boundary, so a key may equal
		 * an untruncated high key.  A truncated high key must be strictly
		 * greater, which is what this test checks for it: a pivot with fewer
		 * key attributes never compares equal to a full key.
		 */
		if (hikey != NULL &&
			bark_compare_itups(keyinfo, rel, itup, hikey) > 0)
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg("BARK index \"%s\" has a key past the high key on page %u at offset %u",
							RelationGetRelationName(rel), blkno, off)));

		/*
		 * A leaf entry equal to the high key on every key attribute must lie
		 * below the high key's heap TID, the lowest of the right sibling's
		 * first entry; a high key without one would be minus infinity there.
		 */
		if (hikey != NULL && BarkPageIsLeaf(opaque) &&
			bark_check_compare_leaf(rel, keyinfo, itup, true, hikey) >= 0)
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg("BARK index \"%s\" has a heap TID past the high key on page %u at offset %u",
							RelationGetRelationName(rel), blkno, off)));

		/*
		 * A LIST entry (sorted duplicates) on a leaf page must carry at least
		 * two locators, stored strictly ascending.  A POSTING entry must hold
		 * a valid sbm serialization with at least two members.  SINGLE
		 * entries and pivots need no extra checks here.  No leaf entry may
		 * exceed the item ceiling the insert and vacuum paths keep to (an
		 * OVERSIZED entry is a small stub, so it always passes).
		 */
		if (BarkPageIsLeaf(opaque))
		{
			if (IndexTupleSize(itup) > BarkMaxItemSize)
				ereport(ERROR,
						(errcode(ERRCODE_INDEX_CORRUPTED),
						 errmsg("BARK index \"%s\" has a %zu-byte leaf entry on page %u at offset %u, larger than the %zu-byte limit",
								RelationGetRelationName(rel), IndexTupleSize(itup),
								blkno, off, (Size) BarkMaxItemSize)));

			/*
			 * Insert and CREATE INDEX both store a row too large to stay
			 * inline as an OVERSIZED entry, so a SINGLE never is one.
			 */
			if (BarkEntryGetShape(itup) == BARK_SHAPE_SINGLE &&
				bark_len_is_oversized(IndexTupleSize(itup)))
				ereport(ERROR,
						(errcode(ERRCODE_INDEX_CORRUPTED),
						 errmsg("BARK index \"%s\" has a %zu-byte SINGLE entry on page %u at offset %u that should be OVERSIZED",
								RelationGetRelationName(rel), IndexTupleSize(itup),
								blkno, off)));

			bark_check_marker(rel, keyinfo, blkno, off, itup);

			if (BarkEntryGetShape(itup) == BARK_SHAPE_LIST)
				bark_check_list(rel, blkno, off, itup);
			else if (BarkEntryGetShape(itup) == BARK_SHAPE_POSTING)
				bark_check_posting(rel, blkno, off, itup);
			else if (BarkEntryGetShape(itup) == BARK_SHAPE_OVERSIZED)
				bark_check_oversized(rel, blkno, off, itup, keyinfo);
		}
		else if (BarkEntryGetShape(itup) == BARK_SHAPE_OVERSIZED)
		{
			/* An oversized downlink / high key also has a chain to validate. */
			bark_check_oversized(rel, blkno, off, itup, keyinfo);
		}

		prev = itup;
		cur = 1 - cur;
	}

	/*
	 * Cross-check the right-sibling link: the sibling's left link must point
	 * back here, the two pages must be at the same level, and the sibling's
	 * first key must not be less than this page's high key.  The sibling is
	 * locked while this page still is, so neither can split in between.
	 */
	if (!BarkPageRightmost(opaque))
	{
		BlockNumber rightblk = opaque->bark_next;
		Buffer		rbuf = ReadBuffer(rel, rightblk);
		Page		rpage;
		BarkPageOpaque ropaque;

		LockBuffer(rbuf, BUFFER_LOCK_SHARE);
		rpage = BufferGetPage(rbuf);
		if (!PageIsNew(rpage))
		{
			ropaque = BarkPageGetOpaque(rpage);
			if (ropaque->bark_prev != blkno)
				ereport(ERROR,
						(errcode(ERRCODE_INDEX_CORRUPTED),
						 errmsg("BARK index \"%s\" has a broken sibling link: page %u's right sibling %u does not link back",
								RelationGetRelationName(rel), blkno, rightblk)));
			if (ropaque->bark_level != opaque->bark_level)
				ereport(ERROR,
						(errcode(ERRCODE_INDEX_CORRUPTED),
						 errmsg("BARK index \"%s\" has a level mismatch across the sibling link from page %u to %u",
								RelationGetRelationName(rel), blkno, rightblk)));
			if (hikey != NULL &&
				(ropaque->bark_flags & (BARK_DELETED | BARK_HALF_DEAD)) == 0 &&
				PageGetMaxOffsetNumber(rpage) >= BarkPageFirstDataKey(ropaque))
			{
				BarkItemBuf ritem;
				IndexTuple	rfirst = BarkPageGetItem(rpage,
													 BarkPageFirstDataKey(ropaque),
													 &ritem);

				if (BarkPageIsLeaf(ropaque) ?
					bark_check_compare_leaf(rel, keyinfo, rfirst, false,
											hikey) < 0 :
					bark_compare_itups(keyinfo, rel, rfirst, hikey) < 0)
					ereport(ERROR,
							(errcode(ERRCODE_INDEX_CORRUPTED),
							 errmsg("BARK index \"%s\" has a first key on page %u that is less than the high key of its left sibling %u",
									RelationGetRelationName(rel), rightblk, blkno)));
			}
		}
		UnlockReleaseBuffer(rbuf);
	}

	live = (opaque->bark_flags & BARK_HALF_DEAD) == 0;
	UnlockReleaseBuffer(buf);
	return live;
}

/*
 * Check every downlink on internal page `blkno`: it must point at a page one
 * level down whose first key is not less than the downlink, since the child
 * holds the keys from its downlink up to its high key.
 *
 * The parent is read once for its list of children.  Each child is then
 * locked before the parent is locked again, the order a split takes them in
 * (the reverse could deadlock with a split waiting for the parent while
 * holding the child), and the downlink is looked up again under both locks.
 * A downlink a concurrent split has moved to the parent's right sibling is
 * not found and goes unchecked here; it is checked when that page is.
 */
static void
bark_check_downlinks(Relation rel, BlockNumber blkno, BarkKeyInfo *keyinfo)
{
	BlockNumber npages = RelationGetNumberOfBlocks(rel);
	Buffer		pbuf = ReadBuffer(rel, blkno);
	Page		ppage;
	BarkPageOpaque popaque;
	uint32		level;
	BlockNumber *children;
	int			nchildren = 0;

	LockBuffer(pbuf, BUFFER_LOCK_SHARE);
	ppage = BufferGetPage(pbuf);
	if (PageIsNew(ppage))
	{
		UnlockReleaseBuffer(pbuf);
		return;
	}
	popaque = BarkPageGetOpaque(ppage);
	if (BarkPageIsOverflow(popaque) || BarkPageIsDeleted(popaque) ||
		BarkPageIsLeaf(popaque) || BarkPageIsMeta(popaque))
	{
		UnlockReleaseBuffer(pbuf);
		return;
	}
	level = popaque->bark_level;
	children = palloc(MaxIndexTuplesPerPage * sizeof(BlockNumber));
	for (OffsetNumber off = BarkPageFirstDataKey(popaque);
		 off <= PageGetMaxOffsetNumber(ppage); off = OffsetNumberNext(off))
	{
		BarkItemBuf ibuf;

		children[nchildren++] =
			BarkEntryGetDownLink(BarkPageGetItem(ppage, off, &ibuf));
	}
	LockBuffer(pbuf, BUFFER_LOCK_UNLOCK);

	for (int i = 0; i < nchildren; i++)
	{
		BlockNumber child = children[i];
		Buffer		cbuf;
		Page		cpage;
		BarkPageOpaque copaque;
		IndexTuple	downlink = NULL;

		CHECK_FOR_INTERRUPTS();

		if (child == BARK_METAPAGE || child >= npages)
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg("BARK index \"%s\" has a downlink on page %u to invalid block %u",
							RelationGetRelationName(rel), blkno, child)));

		cbuf = ReadBuffer(rel, child);
		LockBuffer(cbuf, BUFFER_LOCK_SHARE);
		LockBuffer(pbuf, BUFFER_LOCK_SHARE);

		/*
		 * While unlocked the parent may have been deleted and its block
		 * reused; look for the downlink only on a live internal page.
		 */
		for (OffsetNumber off = BarkPageFirstDataKey(popaque);
			 (popaque->bark_flags & (BARK_LEAF | BARK_DELETED | BARK_OVERFLOW)) == 0 &&
			 popaque->bark_level == level &&
			 off <= PageGetMaxOffsetNumber(ppage); off = OffsetNumberNext(off))
		{
			BarkItemBuf ibuf;
			IndexTuple	itup = BarkPageGetItem(ppage, off, &ibuf);

			if (BarkEntryGetDownLink(itup) == child)
			{
				downlink = itup;
				break;
			}
		}

		cpage = BufferGetPage(cbuf);
		copaque = PageIsNew(cpage) ? NULL : BarkPageGetOpaque(cpage);
		if (downlink != NULL && copaque != NULL &&
			(copaque->bark_flags & (BARK_DELETED | BARK_HALF_DEAD)) == 0)
		{
			if (BarkPageIsOverflow(copaque) || copaque->bark_level + 1 != level)
				ereport(ERROR,
						(errcode(ERRCODE_INDEX_CORRUPTED),
						 errmsg("BARK index \"%s\" has a downlink on page %u at level %u to page %u, which is not at level %u",
								RelationGetRelationName(rel), blkno, level,
								child, level - 1)));
			if (PageGetMaxOffsetNumber(cpage) >= BarkPageFirstDataKey(copaque))
			{
				BarkItemBuf fbuf;
				IndexTuple	first = BarkPageGetItem(cpage,
													BarkPageFirstDataKey(copaque),
													&fbuf);

				if (BarkPageIsLeaf(copaque) ?
					bark_check_compare_leaf(rel, keyinfo, first, false,
											downlink) < 0 :
					bark_compare_itups(keyinfo, rel, first, downlink) < 0)
					ereport(ERROR,
							(errcode(ERRCODE_INDEX_CORRUPTED),
							 errmsg("BARK index \"%s\" has a first key on page %u that is less than its downlink on page %u",
									RelationGetRelationName(rel), child, blkno)));
			}
		}

		LockBuffer(pbuf, BUFFER_LOCK_UNLOCK);
		UnlockReleaseBuffer(cbuf);
	}

	pfree(children);
	ReleaseBuffer(pbuf);
}

/*
 * Validate a LIST entry: it must carry at least two locators, and they must
 * be stored strictly ascending (the invariant the insert and vacuum paths
 * maintain, and the one the scan relies on to return TIDs in order).
 */
static void
bark_check_list(Relation rel, BlockNumber blkno, OffsetNumber off,
				IndexTuple itup)
{
	int			n = BarkListGetCount(itup);

	if (n < 2)
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("BARK index \"%s\" has a list entry with %d locators on page %u at offset %u",
						RelationGetRelationName(rel), n, blkno, off)));

	for (int i = 1; i < n; i++)
	{
		if (ItemPointerCompare(BarkListGetTID(itup, i - 1),
							   BarkListGetTID(itup, i)) >= 0)
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg("BARK index \"%s\" has out-of-order list locators on page %u at offset %u",
							RelationGetRelationName(rel), blkno, off)));
	}
}

/*
 * Check a leaf entry against the rules for markers ("Markers" in
 * BARK-Design.mediawiki).  The reserved heap TID (BarkTidIsMarker) is only
 * ever the TID of a SINGLE entry, in an index with an extracted column
 * whose procedure 7 can return markers, and the entry's other key columns
 * are NULL.  It sorts after every real TID, so an
 * entry of any other shape that held it would hold it as its highest.
 *
 * That each (key, TID) of such an index has one entry needs no check of its
 * own: the TIDs of a run of equal keys are checked to ascend strictly, on a
 * page and across a page boundary, and within a LIST or POSTING entry.
 */
static void
bark_check_marker(Relation rel, BarkKeyInfo *keyinfo, BlockNumber blkno,
				  OffsetNumber off, IndexTuple itup)
{
	int			extracted = bark_index_extracted_column(rel);
	ItemPointerData lo;
	ItemPointerData hi;

	bark_entry_tid_range(itup, &lo, &hi);
	if (!BarkTidIsMarker(&hi))
		return;

	/* Procedure 7 returns markers through its fourth and fifth arguments. */
	if (extracted == 0 ||
		get_func_nargs(index_getprocid(rel, extracted,
									   BARK_EXTRACTVALUE_PROC)) < 5)
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("BARK index \"%s\" has a marker on page %u at offset %u but no column with markers",
						RelationGetRelationName(rel), blkno, off)));
	if (BarkEntryGetShape(itup) != BARK_SHAPE_SINGLE)
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("BARK index \"%s\" has the reserved marker heap TID in an entry that is not SINGLE on page %u at offset %u",
						RelationGetRelationName(rel), blkno, off)));
	for (int i = 0; i < keyinfo->nkeys; i++)
	{
		bool		isnull;

		(void) index_getattr(itup, i + 1, RelationGetDescr(rel), &isnull);
		if ((i + 1 == extracted) == isnull)
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg("BARK index \"%s\" has a marker on page %u at offset %u whose key column %d is %s",
							RelationGetRelationName(rel), blkno, off, i + 1,
							isnull ? "NULL" : "not NULL")));
	}
}

/*
 * Validate a POSTING entry: its body must be a valid sbm serialization that
 * passes a structural self-check and holds at least two members (a smaller set
 * would never have been promoted from a LIST), and the entry must be at least
 * as large as a POSTING entry sized for the set's removal bound.  VACUUM
 * relies on that reserve to rewrite the entry in place after removing members.
 */
static void
bark_check_posting(Relation rel, BlockNumber blkno, OffsetNumber off,
				   IndexTuple itup)
{
	Sbm		   *map = NULL;
	Size		reserved;

	if (BarkPostingDataFits(itup))
		map = sbm_deserialize(BarkPostingGetData(itup),
							  BarkPostingGetDataSize(itup));
	if (map == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("BARK index \"%s\" has a corrupt posting set on page %u at offset %u",
						RelationGetRelationName(rel), blkno, off)));

	if (!sbm_validate(map) || sbm_cardinality(map) < 2)
	{
		size_t		card = sbm_cardinality(map);

		sbm_free(map);
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("BARK index \"%s\" has an invalid posting set (%zu members) on page %u at offset %u",
						RelationGetRelationName(rel), card, blkno, off)));
	}

	reserved = BarkPostingEntrySize(BarkEntryGetBodyOffset(itup),
									sbm_removal_bound(map));
	sbm_free(map);
	if (IndexTupleSize(itup) < reserved)
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("BARK index \"%s\" has a %zu-byte posting entry on page %u at offset %u, smaller than the %zu bytes its set's removal bound requires",
						RelationGetRelationName(rel), IndexTupleSize(itup),
						blkno, off, reserved)));
}

/*
 * Validate an OVERSIZED entry: its overflow chain must be reachable and yield
 * exactly the recorded number of bytes (so the full tuple can be reconstructed
 * and compared), and the reconstructed tuple's leading key must agree with the
 * entry's position in key order (checked implicitly by the per-page order and
 * high-key checks, which fetch the chain through bark_compare_itups).  Here we
 * verify the chain structure directly: every page a BARK_OVERFLOW page, linked
 * through to the recorded length, with no premature terminus.
 */
static void
bark_check_oversized(Relation rel, BlockNumber blkno, OffsetNumber off,
					 IndexTuple itup, BarkKeyInfo *keyinfo)
{
	BarkOverflowRef *ref = BarkOverflowGetRef(itup);
	uint32		fulllen = ref->fulllen;
	BlockNumber chainblk = BarkOverflowGetFirstBlock(itup);
	BlockNumber npages = RelationGetNumberOfBlocks(rel);
	uint32		got = 0;
	IndexTuple	full;

	while (chainblk != BARK_P_NONE && got < fulllen)
	{
		Buffer		cbuf;
		Page		cpage;
		BarkPageOpaque copaque;

		if (chainblk >= npages)
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg("BARK index \"%s\" oversized entry on page %u at offset %u points at out-of-range overflow block %u",
							RelationGetRelationName(rel), blkno, off, chainblk)));

		cbuf = ReadBuffer(rel, chainblk);
		LockBuffer(cbuf, BUFFER_LOCK_SHARE);
		cpage = BufferGetPage(cbuf);
		copaque = BarkPageGetOpaque(cpage);

		if (PageIsNew(cpage) || !BarkPageIsOverflow(copaque))
		{
			UnlockReleaseBuffer(cbuf);
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg("BARK index \"%s\" oversized entry on page %u at offset %u references non-overflow block %u",
							RelationGetRelationName(rel), blkno, off, chainblk)));
		}

		got += Min((uint32) BarkOverflowChunkSize, fulllen - got);
		chainblk = copaque->bark_next;
		UnlockReleaseBuffer(cbuf);
	}

	if (got != fulllen)
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("BARK index \"%s\" oversized entry on page %u at offset %u has a truncated overflow chain (%u of %u bytes)",
						RelationGetRelationName(rel), blkno, off, got, fulllen)));

	/* The reconstructed tuple must deform without error. */
	full = bark_fetch_oversized(rel, itup);

	/*
	 * If the entry carries an inline comparison prefix, it must match the
	 * leading bytes of the reconstructed first key column (that is the
	 * invariant the compare fast path relies on).
	 */
	if (ref->prefixlen > 0)
	{
		TupleDesc	tupdesc = RelationGetDescr(rel);
		Datum		d;
		bool		isnull;

		d = index_getattr(full, 1, tupdesc, &isnull);
		if (isnull)
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg("BARK index \"%s\" oversized entry on page %u at offset %u has a prefix but a NULL first key column",
							RelationGetRelationName(rel), blkno, off)));
		else
		{
			char	   *data = VARDATA_ANY(DatumGetPointer(d));
			Size		fulllen1 = VARSIZE_ANY_EXHDR(DatumGetPointer(d));

			if (ref->prefixcomplete && fulllen1 != ref->prefixlen)
				ereport(ERROR,
						(errcode(ERRCODE_INDEX_CORRUPTED),
						 errmsg("BARK index \"%s\" oversized entry on page %u at offset %u claims a complete %u-byte prefix but the first key column is %zu bytes",
								RelationGetRelationName(rel), blkno, off,
								ref->prefixlen, fulllen1)));
			if (fulllen1 < ref->prefixlen ||
				memcmp(data, ref->prefix, ref->prefixlen) != 0)
				ereport(ERROR,
						(errcode(ERRCODE_INDEX_CORRUPTED),
						 errmsg("BARK index \"%s\" oversized entry on page %u at offset %u has a prefix that does not match its first key column",
								RelationGetRelationName(rel), blkno, off)));
		}
	}
	pfree(full);
}

/*
 * Are two pivots the same separator: equal on the key attributes, with the
 * same number of them and the same heap TID (or none)?  A downlink is a copy
 * of the high key of the child to its left, made by the split that created
 * the child or by CREATE INDEX, so the two never differ.
 */
static bool
bark_check_same_pivot(Relation rel, BarkKeyInfo *keyinfo, IndexTuple a,
					  IndexTuple b)
{
	ItemPointer atid = bark_pivot_heap_tid(a);
	ItemPointer btid = bark_pivot_heap_tid(b);

	if (BarkEntryGetPivotNAtts(a) != BarkEntryGetPivotNAtts(b))
		return false;
	if ((atid == NULL) != (btid == NULL) ||
		(atid != NULL && !ItemPointerEquals(atid, btid)))
		return false;
	return bark_compare_itups(keyinfo, rel, a, b) == 0;
}

/*
 * The parent check, under ShareLock: walk the tree level by level from the
 * root, as bt_check_every_level does, and check each level of internal pages
 * against the level below it.
 *
 * Read left to right, a level's downlinks must name the pages of the level
 * below in their sibling-chain order, starting at that level's leftmost page
 * and ending at its rightmost; the first downlink of the leftmost page has no
 * key attributes (minus infinity), and every other downlink is the high key
 * of the child to its left.  The last child of a page that has a right
 * sibling has that page's high key.  So the keys of each child lie between
 * its downlink and the next one, which the per-page checks (keys between a
 * page's first entry and its high key) extend to every entry.
 *
 * bark_check_page has already checked each page by itself and its sibling
 * links, and counted the pages of the tree in nlive; the walk must reach
 * each of them once, or some page is not in the tree.  A deleted or
 * half-dead page is not, and BARK never leaves one in a sibling chain
 * during a split, so none should be met here.
 */
static void
bark_check_levels(Relation rel, BarkKeyInfo *keyinfo, BlockNumber npages,
				  int64 nlive)
{
	Buffer		metabuf = ReadBuffer(rel, BARK_METAPAGE);
	BarkMetaPageData *meta;
	BlockNumber root;
	uint32		rootlevel;
	BlockNumber leftmost;
	int64		nreached = 0;
	MemoryContext tmpcxt;
	MemoryContext oldcxt;

	LockBuffer(metabuf, BUFFER_LOCK_SHARE);
	meta = BarkPageGetMeta(BufferGetPage(metabuf));
	root = meta->bark_root;
	rootlevel = meta->bark_level;
	UnlockReleaseBuffer(metabuf);

	if (root == BARK_P_NONE)
	{
		if (nlive != 0)
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg("BARK index \"%s\" has no root but has %" PRId64 " tree pages",
							RelationGetRelationName(rel), nlive)));
		return;
	}
	if (root == BARK_METAPAGE || root >= npages)
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("BARK index \"%s\" has a meta page naming invalid root block %u",
						RelationGetRelationName(rel), root)));

	tmpcxt = AllocSetContextCreate(CurrentMemoryContext,
								   "bark_check_levels",
								   ALLOCSET_DEFAULT_SIZES);
	oldcxt = MemoryContextSwitchTo(tmpcxt);

	/*
	 * Walk each level from its leftmost page.  The root is the leftmost page
	 * of the top level; each level's first downlink names the leftmost page
	 * of the level below.
	 */
	leftmost = root;
	for (int64 level = rootlevel; level >= 0; level--)
	{
		BlockNumber blkno = leftmost;
		BlockNumber expect = InvalidBlockNumber;	/* next child, in chain
													 * order */
		IndexTuple	prevhikey = NULL;	/* high key of the last child */
		bool		first = true;

		leftmost = InvalidBlockNumber;
		while (blkno != BARK_P_NONE)
		{
			Buffer		buf;
			Page		page;
			BarkPageOpaque opaque;
			IndexTuple	hikey = NULL;
			OffsetNumber maxoff;

			CHECK_FOR_INTERRUPTS();

			if (blkno == BARK_METAPAGE || blkno >= npages)
				ereport(ERROR,
						(errcode(ERRCODE_INDEX_CORRUPTED),
						 errmsg("BARK index \"%s\" has a link at level %" PRId64 " to invalid block %u",
								RelationGetRelationName(rel), level, blkno)));
			buf = ReadBuffer(rel, blkno);
			LockBuffer(buf, BUFFER_LOCK_SHARE);
			page = BufferGetPage(buf);
			opaque = PageIsNew(page) ? NULL : BarkPageGetOpaque(page);

			if (opaque == NULL ||
				(opaque->bark_flags & (BARK_DELETED | BARK_HALF_DEAD |
									   BARK_OVERFLOW | BARK_META)) != 0 ||
				opaque->bark_level != level ||
				BarkPageIsLeaf(opaque) != (level == 0))
				ereport(ERROR,
						(errcode(ERRCODE_INDEX_CORRUPTED),
						 errmsg("BARK index \"%s\" has a page %u at level %" PRId64 " of the tree that is not a live page of that level",
								RelationGetRelationName(rel), blkno, level)));
			if (BarkPageIsRoot(opaque) != (level == rootlevel) ||
				(level == rootlevel &&
				 (!BarkPageLeftmost(opaque) || !BarkPageRightmost(opaque))))
				ereport(ERROR,
						(errcode(ERRCODE_INDEX_CORRUPTED),
						 errmsg("BARK index \"%s\" has page %u flagged as root inconsistently with the meta page's root %u at level %u",
								RelationGetRelationName(rel), blkno, root,
								rootlevel)));
			if (first != BarkPageLeftmost(opaque))
				ereport(ERROR,
						(errcode(ERRCODE_INDEX_CORRUPTED),
						 errmsg("BARK index \"%s\" has page %u at level %" PRId64 " whose left link does not match its place in the level",
								RelationGetRelationName(rel), blkno, level)));
			nreached++;
			if (nreached > nlive)
				ereport(ERROR,
						(errcode(ERRCODE_INDEX_CORRUPTED),
						 errmsg("BARK index \"%s\" has a cycle in its sibling links at level %" PRId64,
								RelationGetRelationName(rel), level)));

			maxoff = PageGetMaxOffsetNumber(page);
			if (!BarkPageRightmost(opaque) && maxoff >= BARK_P_HIKEY)
				hikey = CopyIndexTuple((IndexTuple)
									   PageGetItem(page, PageGetItemId(page, BARK_P_HIKEY)));

			/* Check this internal page's downlinks against the level below. */
			for (OffsetNumber off = BarkPageFirstDataKey(opaque);
				 level > 0 && off <= maxoff; off = OffsetNumberNext(off))
			{
				IndexTuple	downlink = (IndexTuple)
					PageGetItem(page, PageGetItemId(page, off));
				BlockNumber child = BarkEntryGetDownLink(downlink);
				Buffer		cbuf;
				Page		cpage;
				BarkPageOpaque copaque;

				if (BarkEntryGetShape(downlink) != BARK_SHAPE_PIVOT &&
					(BarkEntryGetShape(downlink) != BARK_SHAPE_OVERSIZED ||
					 BarkOverflowIsLeaf(downlink)))
					ereport(ERROR,
							(errcode(ERRCODE_INDEX_CORRUPTED),
							 errmsg("BARK index \"%s\" has an item on internal page %u at offset %u that is not a pivot",
									RelationGetRelationName(rel), blkno, off)));
				if (leftmost == InvalidBlockNumber)
				{
					/* The level's first downlink: minus infinity. */
					if (BarkEntryGetPivotNAtts(downlink) != 0)
						ereport(ERROR,
								(errcode(ERRCODE_INDEX_CORRUPTED),
								 errmsg("BARK index \"%s\" has a first downlink at level %" PRId64 " on page %u that is not minus infinity",
										RelationGetRelationName(rel), level, blkno)));
					leftmost = child;
				}
				else if (child != expect)
					ereport(ERROR,
							(errcode(ERRCODE_INDEX_CORRUPTED),
							 errmsg("BARK index \"%s\" has a downlink on page %u at offset %u to page %u, where the level below continues at page %u",
									RelationGetRelationName(rel), blkno, off,
									child, expect)));
				else if (prevhikey == NULL ||
						 !bark_check_same_pivot(rel, keyinfo, downlink, prevhikey))
					ereport(ERROR,
							(errcode(ERRCODE_INDEX_CORRUPTED),
							 errmsg("BARK index \"%s\" has a downlink on page %u at offset %u that is not the high key of the page to its child's left",
									RelationGetRelationName(rel), blkno, off)));

				/*
				 * Under ShareLock the child cannot change, so reading it
				 * while the parent is still locked cannot deadlock.
				 */
				if (child == BARK_METAPAGE || child >= npages)
					ereport(ERROR,
							(errcode(ERRCODE_INDEX_CORRUPTED),
							 errmsg("BARK index \"%s\" has a downlink on page %u to invalid block %u",
									RelationGetRelationName(rel), blkno, child)));
				cbuf = ReadBuffer(rel, child);
				LockBuffer(cbuf, BUFFER_LOCK_SHARE);
				cpage = BufferGetPage(cbuf);
				copaque = PageIsNew(cpage) ? NULL : BarkPageGetOpaque(cpage);
				if (copaque == NULL ||
					(copaque->bark_flags & (BARK_DELETED | BARK_HALF_DEAD |
											BARK_OVERFLOW | BARK_META)) != 0)
					ereport(ERROR,
							(errcode(ERRCODE_INDEX_CORRUPTED),
							 errmsg("BARK index \"%s\" has a downlink on page %u to page %u, which is not a live page",
									RelationGetRelationName(rel), blkno, child)));
				if (prevhikey)
					pfree(prevhikey);
				prevhikey = NULL;
				if (!BarkPageRightmost(copaque) &&
					PageGetMaxOffsetNumber(cpage) >= BARK_P_HIKEY)
					prevhikey = CopyIndexTuple((IndexTuple)
											   PageGetItem(cpage, PageGetItemId(cpage, BARK_P_HIKEY)));
				expect = copaque->bark_next;
				UnlockReleaseBuffer(cbuf);
			}

			/*
			 * The last child of a page with a right sibling bounds its keys
			 * by the same separator as the page does.
			 */
			if (level > 0 && hikey != NULL &&
				(prevhikey == NULL ||
				 !bark_check_same_pivot(rel, keyinfo, hikey, prevhikey)))
				ereport(ERROR,
						(errcode(ERRCODE_INDEX_CORRUPTED),
						 errmsg("BARK index \"%s\" has a high key on page %u that is not the high key of its last child",
								RelationGetRelationName(rel), blkno)));
			if (level > 0 && hikey == NULL && !BarkPageRightmost(opaque))
				ereport(ERROR,
						(errcode(ERRCODE_INDEX_CORRUPTED),
						 errmsg("BARK index \"%s\" has a non-rightmost page %u with no high key",
								RelationGetRelationName(rel), blkno)));

			first = false;
			blkno = opaque->bark_next;
			UnlockReleaseBuffer(buf);
		}

		/* Every page of the level below was named, and it ends here. */
		if (level > 0 &&
			(leftmost == InvalidBlockNumber || expect != BARK_P_NONE))
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg("BARK index \"%s\" has pages at level %" PRId64 " that no downlink at level %" PRId64 " names",
							RelationGetRelationName(rel), level - 1, level)));

		MemoryContextReset(tmpcxt);
	}

	MemoryContextSwitchTo(oldcxt);
	MemoryContextDelete(tmpcxt);

	if (nreached != nlive)
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("BARK index \"%s\" has %" PRId64 " live pages, but only %" PRId64 " are reachable from its root",
						RelationGetRelationName(rel), nlive, nreached)));
}

/*
 * The fingerprint of the row (values, isnull) at heap TID `tid`: the row's
 * index tuple as a SINGLE entry would hold it, in a form that does not depend
 * on how the datums reached it.
 *
 * As verify_nbtree.c's bt_normalize_tuple explains, the heap may hold a
 * datum compressed where the index tuple holds it plain, or with a 4-byte
 * header where the index tuple has a 1-byte one, so the same row can come
 * from the heap and from the index with different bytes.  Every varlena is
 * therefore detoasted and packed, and the tuple formed with
 * bark_form_full_tuple, which compresses exactly as the index does and has
 * no size limit (an OVERSIZED entry's full tuple can exceed
 * index_form_tuple's).  Hashing the tuple with its heap TID makes it a
 * (key, TID) fingerprint.  The result is palloc'd in the current context;
 * its length is in *len.
 */
static IndexTuple
bark_check_normalize(Relation rel, Datum *values, bool *isnull,
					 ItemPointer tid, Size *len)
{
	TupleDesc	tupdesc = RelationGetDescr(rel);
	Datum		normalized[INDEX_MAX_KEYS];
	IndexTuple	itup;

	for (int i = 0; i < tupdesc->natts; i++)
	{
		normalized[i] = values[i];
		if (!isnull[i] && TupleDescAttr(tupdesc, i)->attlen == -1)
			normalized[i] = PointerGetDatum(PG_DETOAST_DATUM_PACKED(values[i]));
	}
	itup = bark_form_full_tuple(tupdesc, normalized, isnull, len);
	itup->t_tid = *tid;
	return itup;
}

/*
 * Fingerprint one leaf entry: each of its heap TIDs with the entry's key, so
 * a LIST or POSTING entry adds one fingerprint per member, each the one its
 * row has on its own.
 */
static void
bark_fingerprint_entry(Relation rel, BarkCheckState *state, IndexTuple itup)
{
	TupleDesc	tupdesc = RelationGetDescr(rel);
	Datum		values[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS];
	IndexTuple	keytup = itup;
	ItemPointer tids;
	int			ntids;

	if (BarkEntryGetShape(itup) == BARK_SHAPE_OVERSIZED)
		keytup = bark_fetch_oversized(rel, itup);
	index_deform_tuple(keytup, tupdesc, values, isnull);

	ntids = bark_entry_count_tids(itup);
	tids = palloc_array(ItemPointerData, ntids);
	ntids = bark_entry_get_tids(itup, tids, ntids);
	for (int i = 0; i < ntids; i++)
	{
		Size		len;
		IndexTuple	norm = bark_check_normalize(rel, values, isnull, &tids[i],
												&len);

		bloom_add_element(state->filter, (unsigned char *) norm, len);
		pfree(norm);
	}
}

/*
 * Fingerprint every leaf entry, reading the leaves left to right through
 * their sibling links from the leftmost leaf.
 *
 * Reading the leaves in key order rather than block order is what lets an
 * entry moved by a concurrent split still be found: a split moves entries
 * only to a new right sibling, which this walk reaches after the page it
 * came from.  Every entry the heap scan's snapshot needs was in the index
 * when the snapshot was taken, before this walk started, so it is either on
 * a leaf the walk has yet to reach or on a page split off one.  Under
 * ShareLock nothing moves at all.
 */
static void
bark_fingerprint_leaves(Relation rel, BarkCheckState *state)
{
	Buffer		buf = bark_get_root_buffer(rel, BUFFER_LOCK_SHARE);
	MemoryContext oldcxt;

	if (!BufferIsValid(buf))
		return;					/* an empty index has no entries */

	/* Descend by first downlinks to the leftmost leaf. */
	for (;;)
	{
		Page		page = BufferGetPage(buf);
		BarkPageOpaque opaque = BarkPageGetOpaque(page);
		BlockNumber child;

		/*
		 * A page deleted since its parent's downlink was read cannot be on
		 * the leftmost edge (VACUUM deletes only interior leaves), but move
		 * right through one as every descent does.
		 */
		if (BarkPageIgnore(opaque) && !BarkPageRightmost(opaque))
		{
			BlockNumber next = opaque->bark_next;

			UnlockReleaseBuffer(buf);
			buf = ReadBuffer(rel, next);
			LockBuffer(buf, BUFFER_LOCK_SHARE);
			continue;
		}
		if (BarkPageIsLeaf(opaque))
			break;
		if (PageGetMaxOffsetNumber(page) < BarkPageFirstDataKey(opaque))
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg("BARK index \"%s\" has an internal page %u with no downlinks",
							RelationGetRelationName(rel),
							BufferGetBlockNumber(buf))));
		child = BarkEntryGetDownLink((IndexTuple)
									 PageGetItem(page,
												 PageGetItemId(page,
															   BarkPageFirstDataKey(opaque))));
		UnlockReleaseBuffer(buf);
		buf = ReadBuffer(rel, child);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
	}

	oldcxt = MemoryContextSwitchTo(state->tmpcxt);
	for (;;)
	{
		Page		page = BufferGetPage(buf);
		BarkPageOpaque opaque = BarkPageGetOpaque(page);
		BlockNumber next = opaque->bark_next;

		CHECK_FOR_INTERRUPTS();

		if (BarkPageIsLeaf(opaque) && !BarkPageIgnore(opaque))
		{
			OffsetNumber maxoff = PageGetMaxOffsetNumber(page);

			for (OffsetNumber off = BarkPageFirstDataKey(opaque);
				 off <= maxoff; off = OffsetNumberNext(off))
			{
				BarkItemBuf ibuf;

				bark_fingerprint_entry(rel, state,
									   BarkPageGetItem(page, off, &ibuf));
				MemoryContextReset(state->tmpcxt);
			}
		}
		UnlockReleaseBuffer(buf);
		if (next == BARK_P_NONE)
			break;
		buf = ReadBuffer(rel, next);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
	}
	MemoryContextSwitchTo(oldcxt);
}

/*
 * Per-tuple callback of the heap scan: the tuple's fingerprint must be in
 * the filter.  See bt_tuple_present_callback in verify_nbtree.c for what a
 * failure here can mean.
 */
static void
bark_tuple_present_callback(Relation index, ItemPointer tid, Datum *values,
							bool *isnull, bool tupleIsAlive, void *checkstate)
{
	BarkCheckState *state = (BarkCheckState *) checkstate;
	MemoryContext oldcxt = MemoryContextSwitchTo(state->tmpcxt);
	Size		len;
	IndexTuple	norm = bark_check_normalize(index, values, isnull, tid, &len);

	if (bloom_lacks_element(state->filter, (unsigned char *) norm, len))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("heap tuple (%u,%u) from table \"%s\" lacks matching index tuple within index \"%s\"",
						ItemPointerGetBlockNumber(tid),
						ItemPointerGetOffsetNumber(tid),
						RelationGetRelationName(state->heaprel),
						RelationGetRelationName(index)),
				 !state->readonly
				 ? errhint("Retrying verification using the function bark_index_parent_check() might provide a more specific error.")
				 : 0));
	state->heaptuplespresent++;
	MemoryContextSwitchTo(oldcxt);
	MemoryContextReset(state->tmpcxt);
}

/*
 * heapallindexed: fingerprint the index, then scan the heap as CREATE INDEX
 * CONCURRENTLY's first scan does, with the snapshot taken before the index
 * walk, and probe for every tuple it would index.
 */
static void
bark_check_heap(Relation rel, Relation heaprel, BarkCheckState *state,
				bool readonly)
{
	IndexInfo  *indexinfo;
	TableScanDesc scan;
	int64		total_elems;

	/*
	 * Size the filter for the larger of the index's row count as of its last
	 * build or VACUUM and a full leaf's worth of SINGLE entries on every
	 * page, as verify_nbtree.c sizes its own.  A POSTING entry can hold more
	 * rows than that, which the row count covers.  An undersized filter only
	 * makes a missing entry likelier to go unnoticed.
	 */
	total_elems = Max((int64) RelationGetNumberOfBlocks(rel) *
					  (MaxIndexTuplesPerPage / 3),
					  (int64) rel->rd_rel->reltuples);
	state->filter = bloom_create(total_elems, maintenance_work_mem,
								 pg_prng_uint64(&pg_global_prng_state));
	state->heaprel = heaprel;
	state->readonly = readonly;
	state->tmpcxt = AllocSetContextCreate(CurrentMemoryContext,
										  "bark_check_heap",
										  ALLOCSET_DEFAULT_SIZES);

	bark_fingerprint_leaves(rel, state);

	/*
	 * As in verify_nbtree.c: our own scan, so that it uses our snapshot;
	 * the scan of a concurrent build; and no waits on uncommitted tuples of
	 * a unique or exclusion index.
	 */
	indexinfo = BuildIndexInfo(rel);
	scan = table_beginscan_strat(heaprel, state->snapshot, 0, NULL,
								 true, true);
	indexinfo->ii_Concurrent = true;
	indexinfo->ii_Unique = false;
	indexinfo->ii_ExclusionOps = NULL;
	indexinfo->ii_ExclusionProcs = NULL;
	indexinfo->ii_ExclusionStrats = NULL;

	elog(DEBUG1, "verifying that tuples from index \"%s\" are present in \"%s\"",
		 RelationGetRelationName(rel), RelationGetRelationName(heaprel));

	table_index_build_scan(heaprel, rel, indexinfo, true, false,
						   bark_tuple_present_callback, state, scan);

	ereport(DEBUG1,
			(errmsg_internal("finished verifying presence of %" PRId64 " tuples from table \"%s\" with bitset %.2f%% set",
							 state->heaptuplespresent,
							 RelationGetRelationName(heaprel),
							 100.0 * bloom_prop_bits_set(state->filter))));

	bloom_free(state->filter);
	MemoryContextDelete(state->tmpcxt);
}
