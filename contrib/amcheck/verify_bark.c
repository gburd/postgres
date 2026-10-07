/*-------------------------------------------------------------------------
 *
 * verify_bark.c
 *		Verify the structural integrity of a BARK index.
 *
 * bark_index_check(index regclass) walks every page of a BARK index and
 * checks the invariants the access method relies on:
 *
 *	- within each page, data entries are in non-decreasing key order;
 *	- every data key is less than or equal to the page's high key (the bound
 *	  the page's parent downlink promises), and strictly less when suffix
 *	  truncation dropped key attributes from the high key, which has between
 *	  one and all of the key attributes;
 *	- sibling links are consistent (the right sibling's left link points back,
 *	  and levels match across a sibling link), and the right sibling's first
 *	  key is not less than the page's high key;
 *	- every downlink points at a page one level down whose first key is not
 *	  less than the downlink;
 *	- no page is still flagged with an unfinished split, which a clean index
 *	  never leaves behind;
 *	- no leaf entry is larger than BarkMaxItemSize, and every POSTING entry
 *	  reserves room for the largest encoding of any subset of its set;
 *	- a page flagged BARK_PREFIX is a leaf with a well-formed PREFIX item of
 *	  1..BARK_PREFIX_MAX bytes, and every entry on it decodes within its
 *	  bounds, sharing no more of the prefix than there is (the key checks
 *	  above then apply to the decoded entries).
 *
 * This is a lightweight structural check: it does not cross-check the index
 * against the heap, nor verify that every page is reachable from the root.
 * It is modeled on amcheck's other per-AM verifiers (verify_gin.c) and uses
 * the shared amcheck_lock_relation_and_check harness.
 *
 * Only AccessShareLock is held, so the index can change underneath.  Each
 * cross-page check holds share locks on both pages, taken in an order the
 * write paths also use (left page before right sibling, child before parent),
 * so it cannot deadlock with a split and sees the two pages consistently.
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
#include "catalog/pg_am_d.h"
#include "fmgr.h"
#include "lib/sbm.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "utils/rel.h"
#include "verify_common.h"

PG_FUNCTION_INFO_V1(bark_index_check);

static void bark_check_structure(Relation rel, Relation heaprel,
								  void *callback_state, bool readonly);
static void bark_check_page(Relation rel, BlockNumber blkno, BarkKeyInfo *keyinfo);
static void bark_check_downlinks(Relation rel, BlockNumber blkno,
								 BarkKeyInfo *keyinfo);
static void bark_check_list(Relation rel, BlockNumber blkno, OffsetNumber off,
							IndexTuple itup);
static void bark_check_posting(Relation rel, BlockNumber blkno, OffsetNumber off,
							   IndexTuple itup);
static void bark_check_oversized(Relation rel, BlockNumber blkno, OffsetNumber off,
								 IndexTuple itup, BarkKeyInfo *keyinfo);

/*
 * bark_index_check(index regclass)
 *
 * Verify the structural integrity of a BARK index.  Takes AccessShareLock on
 * the heap and index.
 */
Datum
bark_index_check(PG_FUNCTION_ARGS)
{
	Oid			indrelid = PG_GETARG_OID(0);

	amcheck_lock_relation_and_check(indrelid,
									BARK_AM_OID,
									bark_check_structure,
									AccessShareLock,
									NULL);

	PG_RETURN_VOID();
}

/*
 * Main entry: iterate over every page and check per-page and cross-page
 * invariants.
 */
static void
bark_check_structure(Relation rel, Relation heaprel, void *callback_state,
					  bool readonly)
{
	BarkKeyInfo *keyinfo = bark_build_keyinfo(rel);
	BlockNumber npages = RelationGetNumberOfBlocks(rel);

	for (BlockNumber blkno = BARK_METAPAGE + 1; blkno < npages; blkno++)
	{
		CHECK_FOR_INTERRUPTS();
		bark_check_page(rel, blkno, keyinfo);
		bark_check_downlinks(rel, blkno, keyinfo);
	}

	pfree(keyinfo);
}

/*
 * Check one page's invariants, plus the consistency of its right-sibling link.
 */
static void
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

	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);

	if (PageIsNew(page))
	{
		UnlockReleaseBuffer(buf);
		return;
	}

	opaque = BarkPageGetOpaque(page);

	/* Overflow pages hold raw out-of-line bytes, not tree items; skip them. */
	if (BarkPageIsOverflow(opaque))
	{
		UnlockReleaseBuffer(buf);
		return;
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
		return;
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

		/* Keys must be in non-decreasing order within the page. */
		if (prev != NULL &&
			bark_compare_itups(keyinfo, rel, prev, itup) > 0)
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg("BARK index \"%s\" has out-of-order keys on page %u at offset %u",
							RelationGetRelationName(rel), blkno, off)));

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

				if (bark_compare_itups(keyinfo, rel, rfirst, hikey) < 0)
					ereport(ERROR,
							(errcode(ERRCODE_INDEX_CORRUPTED),
							 errmsg("BARK index \"%s\" has a first key on page %u that is less than the high key of its left sibling %u",
									RelationGetRelationName(rel), rightblk, blkno)));
			}
		}
		UnlockReleaseBuffer(rbuf);
	}

	UnlockReleaseBuffer(buf);
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

				if (bark_compare_itups(keyinfo, rel, first, downlink) < 0)
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
