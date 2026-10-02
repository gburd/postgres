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
 *	  the page's parent downlink promises);
 *	- sibling links are consistent (the right sibling's left link points back,
 *	  and levels match across a sibling link);
 *	- no page is still flagged with an unfinished split, which a clean index
 *	  never leaves behind.
 *
 * This is a lightweight structural check: it does not cross-check the index
 * against the heap, nor verify that every downlink's child is reachable.  It
 * is modeled on amcheck's other per-AM verifiers (verify_gin.c) and uses the
 * shared amcheck_lock_relation_and_check harness.
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
	IndexTuple	prev = NULL;

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

	/* A clean index never leaves an unfinished split behind. */
	if ((opaque->bark_flags & BARK_INCOMPLETE_SPLIT) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("BARK index \"%s\" has an unfinished split on page %u",
						RelationGetRelationName(rel), blkno)));

	maxoff = PageGetMaxOffsetNumber(page);
	firstdata = BarkPageFirstDataKey(opaque);

	/* The high key, when present, is the first item on a non-rightmost page. */
	if (!BarkPageRightmost(opaque) && maxoff >= BARK_P_HIKEY)
		hikey = (IndexTuple) PageGetItem(page, PageGetItemId(page, BARK_P_HIKEY));

	for (OffsetNumber off = firstdata; off <= maxoff;
		 off = OffsetNumberNext(off))
	{
		IndexTuple	itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, off));

		/* Keys must be in non-decreasing order within the page. */
		if (prev != NULL &&
			bark_compare_itups(keyinfo, rel, prev, itup) > 0)
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg("BARK index \"%s\" has out-of-order keys on page %u at offset %u",
							RelationGetRelationName(rel), blkno, off)));

		/* Every data key must be within the page's high-key bound. */
		if (hikey != NULL &&
			bark_compare_itups(keyinfo, rel, itup, hikey) > 0)
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg("BARK index \"%s\" has a key past the high key on page %u at offset %u",
							RelationGetRelationName(rel), blkno, off)));

		/*
		 * A LIST entry (sorted duplicates) on a leaf page must carry at least
		 * two locators, stored strictly ascending.  A POSTING entry must hold a
		 * valid sbm serialization with at least two members.  SINGLE entries
		 * and pivots need no extra checks here.
		 */
		if (BarkPageIsLeaf(opaque))
		{
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
	}

	/*
	 * Cross-check the right-sibling link: the sibling's left link must point
	 * back here and the two pages must be at the same level.
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
		}
		UnlockReleaseBuffer(rbuf);
	}

	UnlockReleaseBuffer(buf);
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
 * would never have been promoted from a LIST).
 */
static void
bark_check_posting(Relation rel, BlockNumber blkno, OffsetNumber off,
				   IndexTuple itup)
{
	Sbm		   *map = sbm_deserialize(BarkPostingGetData(itup),
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
	sbm_free(map);
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
	pfree(full);
}
