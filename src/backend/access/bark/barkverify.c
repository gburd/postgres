/*-------------------------------------------------------------------------
 *
 * barkverify.c
 *	  Checks of one BARK page by itself, shared by amcheck's bark_index_check
 *	  and bark_index_parent_check and by the page validator that cassert
 *	  builds run after every change to a BARK page.
 *
 * bark_page_verify checks the invariants that one page must satisfy on its
 * own: its layout, the shape and size of each entry, the internal order of
 * LIST and POSTING entries, and the order of its keys against each other and
 * against its high key.  Checks that need two pages (sibling links, a
 * downlink against its child) and the walk of the whole tree stay in
 * contrib/amcheck/verify_bark.c.
 *
 * The checks are grouped by what they need, so a caller can run those it can
 * afford (BARK_VERIFY_* in bark.h):
 *
 *	SHAPES		the page alone: the deleted-page and prefix layouts, the high
 *				key's shape, entry sizes and shapes, and the members of LIST
 *				and POSTING entries.  No catalog access, no comparator call,
 *				no other page.
 *	ORDER		the keys, by the comparators: order within the page, heap TID
 *				order among equal keys, entries within the high key.
 *	CONTENTS	entries against the index's definition and other pages:
 *				markers (catalog lookups), OVERSIZED chains read in full and
 *				their inline prefixes.
 *	COMPLETE	no unfinished split, which a quiescent index never has but a
 *				page just split does.
 *
 * Corruption is reported at `elevel` with ERRCODE_INDEX_CORRUPTED and the
 * message amcheck has always used.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/bark/barkverify.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bark.h"
#include "access/genam.h"
#include "access/xlogreader.h"
#include "lib/sbm.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "utils/injection_point.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "varatt.h"

/* One bark_page_verify call. */
typedef struct BarkVerify
{
	Relation	rel;			/* the index; only its name without keyinfo */
	BarkKeyInfo *keyinfo;		/* NULL unless ORDER or CONTENTS */
	const char *relname;
	BlockNumber blkno;
	int			flags;
	int			elevel;			/* ERROR or PANIC */
} BarkVerify;

pg_noreturn static void bark_verify_fail(BarkVerify *v, const char *fmt,...)
			pg_attribute_printf(2, 3);
static void bark_verify_list(BarkVerify *v, OffsetNumber off, IndexTuple itup);
static void bark_verify_posting(BarkVerify *v, OffsetNumber off,
								IndexTuple itup);
static void bark_verify_chain(BarkVerify *v, OffsetNumber off,
							  IndexTuple itup);
static void bark_verify_marker(BarkVerify *v, OffsetNumber off,
							   IndexTuple itup);

/*
 * Report corruption.  One function rather than an ereport at each check, so
 * that the caller's elevel applies to all of them; the message is
 * translated here (see src/backend/nls.mk).
 */
static void
bark_verify_fail(BarkVerify *v, const char *fmt,...)
{
	StringInfoData buf;

	fmt = _(fmt);
	initStringInfo(&buf);
	for (;;)
	{
		va_list		args;
		int			needed;

		va_start(args, fmt);
		needed = appendStringInfoVA(&buf, fmt, args);
		va_end(args);
		if (needed == 0)
			break;
		enlargeStringInfo(&buf, needed);
	}
	ereport(v->elevel,
			errcode(ERRCODE_INDEX_CORRUPTED),
			errmsg_internal("%s", buf.data));
	pg_unreachable();
}

/*
 * Compare the leaf entry `itup`, taken at its lowest heap TID (or its highest
 * when `high`), with the pivot or leaf entry `other`, in the tree's order of
 * (key, heap TID): bark_compare_itups_tid.
 */
static int
bark_verify_compare_leaf(BarkVerify *v, IndexTuple itup, bool high,
						 IndexTuple other)
{
	ItemPointerData lo;
	ItemPointerData hi;

	bark_entry_tid_range(itup, &lo, &hi);
	return bark_compare_itups_tid(v->keyinfo, v->rel, itup, high ? &hi : &lo,
								  other);
}

static void bark_page_verify_range(Relation index, BarkKeyInfo *keyinfo,
								   const char *relname, Page page,
								   BlockNumber blkno, int flags, int elevel,
								   OffsetNumber lo, OffsetNumber hi);

/*
 * Check `page`, block `blkno` of `index`, by the checks `flags` asks for (see
 * the file header), reporting corruption at `elevel` (ERROR or PANIC).
 * `keyinfo` is required for BARK_VERIFY_ORDER and BARK_VERIFY_CONTENTS, and
 * without either `index` is used only for its name, or `relname` names it
 * (WAL redo has no relcache entry).  The caller holds a lock on the page.
 *
 * A new page and an overflow page hold no items and pass.  A deleted page
 * gets the deleted-page layout check only.
 */
void
bark_page_verify(Relation index, BarkKeyInfo *keyinfo, const char *relname,
				 Page page, BlockNumber blkno, int flags, int elevel)
{
	bark_page_verify_range(index, keyinfo, relname, page, blkno, flags, elevel,
						   InvalidOffsetNumber, InvalidOffsetNumber);
}

/*
 * bark_page_verify, with the per-entry checks limited to the data items at
 * offsets lo..hi and the one on each side of them, when lo is valid: the
 * entries a change touched, their order against their neighbours and their
 * bound by the high key.  The page's layout and high key are checked whole.
 */
static void
bark_page_verify_range(Relation index, BarkKeyInfo *keyinfo,
					   const char *relname, Page page, BlockNumber blkno,
					   int flags, int elevel, OffsetNumber lo, OffsetNumber hi)
{
	BarkVerify	v;
	BarkPageOpaque opaque;
	OffsetNumber maxoff;
	OffsetNumber firstdata;
	IndexTuple	hikey = NULL;
	int			hikeynatts = 0;
	int			nkeys;
	IndexTuple	prev = NULL;
	BarkItemBuf ibuf[2];		/* this item's and the previous one's */
	int			cur = 0;

	Assert(elevel >= ERROR);
	Assert(keyinfo != NULL ||
		   (flags & (BARK_VERIFY_ORDER | BARK_VERIFY_CONTENTS)) == 0);

	v.rel = index;
	v.keyinfo = keyinfo;
	v.relname = relname != NULL ? relname : RelationGetRelationName(index);
	v.blkno = blkno;
	v.flags = flags;
	v.elevel = elevel;
	nkeys = keyinfo != NULL ? keyinfo->nkeys : INDEX_MAX_KEYS;

	if (PageIsNew(page))
		return;
	opaque = BarkPageGetOpaque(page);
	if (BarkPageIsOverflow(opaque))
		return;

	/*
	 * A deleted page has been unlinked from the tree and holds no items, only
	 * its safexid.  It keeps the sibling links it had when it was deleted,
	 * for readers that still held a link to it, and those siblings need not
	 * link back to it any more, so the link checks of amcheck do not apply.
	 * Check only that it has the deleted-page layout (see
	 * BarkDeletedPageData).
	 */
	if (BarkPageIsDeleted(opaque))
	{
		PageHeader	phdr = (PageHeader) page;

		if ((flags & BARK_VERIFY_SHAPES) &&
			(phdr->pd_lower != SizeOfPageHeaderData ||
			 phdr->pd_upper != phdr->pd_special -
			 MAXALIGN(sizeof(BarkDeletedPageData)) ||
			 (opaque->bark_flags & (BARK_LEAF | BARK_OVERFLOW)) != 0))
			bark_verify_fail(&v, "BARK index \"%s\" has a deleted page %u that does not have the deleted-page layout",
							 v.relname, blkno);
		return;
	}

	/* A clean index never leaves an unfinished split behind. */
	if ((flags & BARK_VERIFY_COMPLETE) &&
		(opaque->bark_flags & BARK_INCOMPLETE_SPLIT) != 0)
		bark_verify_fail(&v, "BARK index \"%s\" has an unfinished split on page %u",
						 v.relname, blkno);

	maxoff = PageGetMaxOffsetNumber(page);
	firstdata = BarkPageFirstDataKey(opaque);

	/*
	 * A prefix-compressed page is a leaf whose PREFIX item, just after the
	 * high key, is an IndexTupleData header with nothing but its size set,
	 * followed by the prefix.  Its entries are checked as they decode, below;
	 * bark_prefix_decode reports one that does not decode within its bounds
	 * or claims more of the prefix than the page has.
	 */
	if ((flags & BARK_VERIFY_SHAPES) && BarkPageHasPrefix(opaque))
	{
		OffsetNumber poff = BarkPagePrefixOff(opaque);
		ItemId		iid;
		IndexTuple	pitem;
		Size		plen;

		if (!BarkPageIsLeaf(opaque) || maxoff < poff)
			bark_verify_fail(&v, "BARK index \"%s\" has a prefix-compressed page %u that is not a leaf with a prefix item",
							 v.relname, blkno);
		iid = PageGetItemId(page, poff);
		pitem = (IndexTuple) PageGetItem(page, iid);
		plen = ItemIdGetLength(iid);
		if (plen <= sizeof(IndexTupleData) ||
			plen > sizeof(IndexTupleData) + BARK_PREFIX_MAX ||
			pitem->t_info != plen ||
			ItemPointerGetBlockNumberNoCheck(&pitem->t_tid) != 0 ||
			ItemPointerGetOffsetNumberNoCheck(&pitem->t_tid) != 0)
			bark_verify_fail(&v, "BARK index \"%s\" has a malformed prefix item on page %u",
							 v.relname, blkno);
	}

	/*
	 * The high key, when present, is the first item on a non-rightmost page.
	 * It is a pivot with at least one key attribute: only a page's downlink
	 * can be minus infinity.
	 */
	if (!BarkPageRightmost(opaque) && maxoff >= BARK_P_HIKEY)
	{
		hikey = (IndexTuple) PageGetItem(page, PageGetItemId(page, BARK_P_HIKEY));
		if (flags & BARK_VERIFY_SHAPES)
		{
			if (BarkEntryIsLeafData(hikey) &&
				(BarkEntryGetShape(hikey) != BARK_SHAPE_OVERSIZED ||
				 BarkOverflowIsLeaf(hikey)))
				bark_verify_fail(&v, "BARK index \"%s\" has a high key on page %u that is not a pivot",
								 v.relname, blkno);
			hikeynatts = BarkEntryGetPivotNAtts(hikey);
			if (hikeynatts < 1 || hikeynatts > nkeys)
				bark_verify_fail(&v, "BARK index \"%s\" has a high key with %d key attributes on page %u",
								 v.relname, hikeynatts, blkno);
		}
	}

	if (OffsetNumberIsValid(lo))
	{
		Assert(lo <= hi);
		if (lo > firstdata)
			firstdata = lo - 1;
		if (hi < maxoff)
			maxoff = hi + 1;
	}

	for (OffsetNumber off = firstdata; off <= maxoff;
		 off = OffsetNumberNext(off))
	{
		IndexTuple	itup = BarkPageGetItem(page, off, &ibuf[cur]);

		/*
		 * A LIST entry (sorted duplicates) on a leaf page must carry at least
		 * two locators, stored strictly ascending.  A POSTING entry must hold
		 * a valid sbm serialization with at least two members.  SINGLE
		 * entries and pivots need no extra checks here.  No leaf entry may
		 * exceed the item ceiling the insert and vacuum paths keep to (an
		 * OVERSIZED entry is a small stub, so it always passes).  These come
		 * first: the order and marker checks below read the entry's heap TIDs
		 * and key, which a corrupt LIST, POSTING or overflow chain would make
		 * them fail on without saying where.
		 */
		if ((flags & BARK_VERIFY_SHAPES) && BarkPageIsLeaf(opaque))
		{
			if (IndexTupleSize(itup) > BarkMaxItemSize)
				bark_verify_fail(&v, "BARK index \"%s\" has a %zu-byte leaf entry on page %u at offset %u, larger than the %zu-byte limit",
								 v.relname, IndexTupleSize(itup),
								 blkno, off, (Size) BarkMaxItemSize);

			/*
			 * Insert and CREATE INDEX both store a row too large to stay
			 * inline as an OVERSIZED entry, so a SINGLE never is one.
			 */
			if (BarkEntryGetShape(itup) == BARK_SHAPE_SINGLE &&
				bark_len_is_oversized(IndexTupleSize(itup)))
				bark_verify_fail(&v, "BARK index \"%s\" has a %zu-byte SINGLE entry on page %u at offset %u that should be OVERSIZED",
								 v.relname, IndexTupleSize(itup), blkno, off);

			if (BarkEntryGetShape(itup) == BARK_SHAPE_LIST)
				bark_verify_list(&v, off, itup);
			else if (BarkEntryGetShape(itup) == BARK_SHAPE_POSTING)
				bark_verify_posting(&v, off, itup);
		}

		/*
		 * An OVERSIZED entry's chain, leaf entry, downlink or high key, and
		 * a marker against the index's definition.
		 */
		if (flags & BARK_VERIFY_CONTENTS)
		{
			if (BarkEntryGetShape(itup) == BARK_SHAPE_OVERSIZED)
				bark_verify_chain(&v, off, itup);
			if (BarkPageIsLeaf(opaque))
				bark_verify_marker(&v, off, itup);
		}

		/*
		 * Keys must be in non-decreasing order within the page, and on a
		 * leaf, the heap TIDs of a run of equal keys in ascending order: each
		 * entry's lowest TID above the previous entry's highest.
		 */
		if ((flags & BARK_VERIFY_ORDER) && prev != NULL)
		{
			int			cmp = bark_compare_itups(keyinfo, index, prev, itup);

			if (cmp > 0)
				bark_verify_fail(&v, "BARK index \"%s\" has out-of-order keys on page %u at offset %u",
								 v.relname, blkno, off);
			if (cmp == 0 && BarkPageIsLeaf(opaque) &&
				bark_verify_compare_leaf(&v, prev, true, itup) >= 0)
				bark_verify_fail(&v, "BARK index \"%s\" has out-of-order heap TIDs among equal keys on page %u at offset %u",
								 v.relname, blkno, off);
		}

		/*
		 * Every data key must be within the page's high-key bound.  Equal
		 * keys may sit on both sides of a page boundary, so a key may equal
		 * an untruncated high key.  A truncated high key must be strictly
		 * greater, which is what this test checks for it: a pivot with fewer
		 * key attributes never compares equal to a full key.
		 */
		if ((flags & BARK_VERIFY_ORDER) && hikey != NULL &&
			bark_compare_itups(keyinfo, index, itup, hikey) > 0)
			bark_verify_fail(&v, "BARK index \"%s\" has a key past the high key on page %u at offset %u",
							 v.relname, blkno, off);

		/*
		 * A leaf entry equal to the high key on every key attribute must lie
		 * below the high key's heap TID, the lowest of the right sibling's
		 * first entry; a high key without one would be minus infinity there.
		 */
		if ((flags & BARK_VERIFY_ORDER) && hikey != NULL &&
			BarkPageIsLeaf(opaque) &&
			bark_verify_compare_leaf(&v, itup, true, hikey) >= 0)
			bark_verify_fail(&v, "BARK index \"%s\" has a heap TID past the high key on page %u at offset %u",
							 v.relname, blkno, off);

		prev = itup;
		cur = 1 - cur;
	}
}

/*
 * Validate a LIST entry: it must carry at least two locators, and they must
 * be stored strictly ascending (the invariant the insert and vacuum paths
 * maintain, and the one the scan relies on to return TIDs in order).
 */
static void
bark_verify_list(BarkVerify *v, OffsetNumber off, IndexTuple itup)
{
	int			n = BarkListGetCount(itup);
	ItemPointer tids;

	if (n < 2)
		bark_verify_fail(v, "BARK index \"%s\" has a list entry with %d locators on page %u at offset %u",
						 v->relname, n, v->blkno, off);

	/*
	 * Read the locators through the accessor so a delta+varbyte coded LIST
	 * (BarkListIsCoded) is decoded; a plain LIST reads its array in place.
	 */
	tids = palloc_array(ItemPointerData, n);
	bark_entry_get_tids(itup, tids, n);

	for (int i = 1; i < n; i++)
	{
		if (ItemPointerCompare(&tids[i - 1], &tids[i]) >= 0)
			bark_verify_fail(v, "BARK index \"%s\" has out-of-order list locators on page %u at offset %u",
							 v->relname, v->blkno, off);
	}
	pfree(tids);
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
bark_verify_marker(BarkVerify *v, OffsetNumber off, IndexTuple itup)
{
	int			extracted = bark_index_extracted_column(v->rel);
	ItemPointerData lo;
	ItemPointerData hi;

	bark_entry_tid_range(itup, &lo, &hi);
	if (!BarkTidIsMarker(&hi))
		return;

	/* Procedure 7 returns markers through its fourth and fifth arguments. */
	if (extracted == 0 ||
		get_func_nargs(index_getprocid(v->rel, extracted,
									   BARK_EXTRACTVALUE_PROC)) < 5)
		bark_verify_fail(v, "BARK index \"%s\" has a marker on page %u at offset %u but no column with markers",
						 v->relname, v->blkno, off);
	if (BarkEntryGetShape(itup) != BARK_SHAPE_SINGLE)
		bark_verify_fail(v, "BARK index \"%s\" has the reserved marker heap TID in an entry that is not SINGLE on page %u at offset %u",
						 v->relname, v->blkno, off);
	for (int i = 0; i < v->keyinfo->nkeys; i++)
	{
		bool		isnull;

		(void) index_getattr(itup, i + 1, RelationGetDescr(v->rel), &isnull);
		if ((i + 1 == extracted) == isnull)
			bark_verify_fail(v, "BARK index \"%s\" has a marker on page %u at offset %u whose key column %d is %s",
							 v->relname, v->blkno, off, i + 1,
							 isnull ? "NULL" : "not NULL");
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
bark_verify_posting(BarkVerify *v, OffsetNumber off, IndexTuple itup)
{
	Sbm		   *map = NULL;
	Size		reserved;

	if (BarkPostingDataFits(itup))
		map = sbm_deserialize(BarkPostingGetData(itup),
							  BarkPostingGetDataSize(itup));
	if (map == NULL)
		bark_verify_fail(v, "BARK index \"%s\" has a corrupt posting set on page %u at offset %u",
						 v->relname, v->blkno, off);

	if (!sbm_validate(map) || sbm_cardinality(map) < 2)
		bark_verify_fail(v, "BARK index \"%s\" has an invalid posting set (%zu members) on page %u at offset %u",
						 v->relname, sbm_cardinality(map), v->blkno, off);

	reserved = BarkPostingEntrySize(BarkEntryGetBodyOffset(itup),
									sbm_removal_bound(map));
	sbm_free(map);
	if (IndexTupleSize(itup) < reserved)
		bark_verify_fail(v, "BARK index \"%s\" has a %zu-byte posting entry on page %u at offset %u, smaller than the %zu bytes its set's removal bound requires",
						 v->relname, IndexTupleSize(itup),
						 v->blkno, off, reserved);
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
bark_verify_chain(BarkVerify *v, OffsetNumber off, IndexTuple itup)
{
	BarkOverflowRef *ref = BarkOverflowGetRef(itup);
	uint32		fulllen = ref->fulllen;
	BlockNumber chainblk = BarkOverflowGetFirstBlock(itup);
	BlockNumber npages = RelationGetNumberOfBlocks(v->rel);
	uint32		got = 0;
	IndexTuple	full;

	while (chainblk != BARK_P_NONE && got < fulllen)
	{
		Buffer		cbuf;
		Page		cpage;
		BarkPageOpaque copaque;

		if (chainblk >= npages)
			bark_verify_fail(v, "BARK index \"%s\" oversized entry on page %u at offset %u points at out-of-range overflow block %u",
							 v->relname, v->blkno, off, chainblk);

		cbuf = ReadBuffer(v->rel, chainblk);
		LockBuffer(cbuf, BUFFER_LOCK_SHARE);
		cpage = BufferGetPage(cbuf);

		/* A new page has no special space to read the flags from. */
		if (PageIsNew(cpage) ||
			!BarkPageIsOverflow((copaque = BarkPageGetOpaque(cpage))))
		{
			UnlockReleaseBuffer(cbuf);
			bark_verify_fail(v, "BARK index \"%s\" oversized entry on page %u at offset %u references non-overflow block %u",
							 v->relname, v->blkno, off, chainblk);
		}

		got += Min((uint32) BarkOverflowChunkSize, fulllen - got);
		chainblk = copaque->bark_next;
		UnlockReleaseBuffer(cbuf);
	}

	if (got != fulllen)
		bark_verify_fail(v, "BARK index \"%s\" oversized entry on page %u at offset %u has a truncated overflow chain (%u of %u bytes)",
						 v->relname, v->blkno, off, got, fulllen);

	/* The reconstructed tuple must deform without error. */
	full = bark_fetch_oversized(v->rel, itup);

	/*
	 * If the entry carries an inline comparison prefix, it must match the
	 * leading bytes of the reconstructed first key column (that is the
	 * invariant the compare fast path relies on).
	 */
	if (ref->prefixlen > 0)
	{
		TupleDesc	tupdesc = RelationGetDescr(v->rel);
		Datum		d;
		bool		isnull;
		char	   *data;
		Size		fulllen1;

		d = index_getattr(full, 1, tupdesc, &isnull);
		if (isnull)
			bark_verify_fail(v, "BARK index \"%s\" oversized entry on page %u at offset %u has a prefix but a NULL first key column",
							 v->relname, v->blkno, off);
		data = VARDATA_ANY(DatumGetPointer(d));
		fulllen1 = VARSIZE_ANY_EXHDR(DatumGetPointer(d));
		if (ref->prefixcomplete && fulllen1 != ref->prefixlen)
			bark_verify_fail(v, "BARK index \"%s\" oversized entry on page %u at offset %u claims a complete %u-byte prefix but the first key column is %zu bytes",
							 v->relname, v->blkno, off,
							 ref->prefixlen, fulllen1);
		if (fulllen1 < ref->prefixlen ||
			memcmp(data, ref->prefix, ref->prefixlen) != 0)
			bark_verify_fail(v, "BARK index \"%s\" oversized entry on page %u at offset %u has a prefix that does not match its first key column",
							 v->relname, v->blkno, off);
	}
	pfree(full);
}

/* ----------------------------------------------------------------------------
 * The page validator: the checks above, after every change to a BARK page
 * ----------------------------------------------------------------------------
 */

#ifdef USE_ASSERT_CHECKING

/*
 * Check a page bark_page_verify skips: the meta page's magic number and
 * version, and an overflow page's layout (one chunk of 1 to
 * BarkOverflowChunkSize bytes, BARK_OVERFLOW its only flag, no left link).
 */
static void
bark_verify_other_page(Page page, const char *relname, BlockNumber blkno,
					   int elevel)
{
	BarkVerify	v = {.relname = relname, .blkno = blkno, .elevel = elevel};
	BarkPageOpaque opaque = BarkPageGetOpaque(page);

	if (BarkPageIsMeta(opaque))
	{
		BarkMetaPageData *meta = BarkPageGetMeta(page);

		if (meta->bark_magic != BARK_MAGIC ||
			meta->bark_version < BARK_MIN_VERSION ||
			meta->bark_version > BARK_VERSION)
			bark_verify_fail(&v, "BARK index \"%s\" has a meta page %u with magic number %08X and version %u",
							 relname, blkno, meta->bark_magic,
							 meta->bark_version);
	}
	else if (BarkPageIsOverflow(opaque))
	{
		Size		len = ((PageHeader) page)->pd_lower -
			MAXALIGN(SizeOfPageHeaderData);

		if (opaque->bark_flags != BARK_OVERFLOW ||
			opaque->bark_prev != BARK_P_NONE ||
			((PageHeader) page)->pd_lower <= MAXALIGN(SizeOfPageHeaderData) ||
			len > BarkOverflowChunkSize)
			bark_verify_fail(&v, "BARK index \"%s\" has an overflow page %u that does not have the overflow-page layout",
							 relname, blkno);
	}
}

/*
 * bark_verify_pages (a developer option, PGC_SUSET, only in cassert builds,
 * where it is on by default): check every BARK page right after a change to
 * it.  The idea is RethinkDB's: in debug builds, leaf::validate runs the
 * node's full fsck after every mutation (src/btree/leaf_node.cc), so a fault
 * is reported by the call that made it, not by a later reader, or by nobody.
 */
bool		bark_verify_pages = true;

/*
 * One check in this many of a few entries' change takes the whole page.
 *
 * shortcut: a non-local fault is caught only by every 32nd change's full
 * check or by amcheck; check every page in full if a bug hides there
 */
#define BARK_VERIFY_FULL_EVERY	32

/* What the error context names: the function that changed the page. */
typedef struct BarkVerifyContext
{
	const char *caller;
	BlockNumber blkno;
} BarkVerifyContext;

static void
bark_verify_errcontext(void *arg)
{
	BarkVerifyContext *ctx = (BarkVerifyContext *) arg;

	errcontext("BARK page validator, block %u after %s",
			   ctx->blkno, ctx->caller);
}

/*
 * Check the page in `buf`, which `caller` has just changed and still holds
 * exclusive-locked, after its critical section (palloc is not allowed in
 * one, and a comparator may allocate): SHAPES and ORDER.  CONTENTS is not
 * run: an OVERSIZED entry's chain is written and freed apart from the page
 * that references it, under locks of its own, and the marker rules cost a
 * catalog lookup per entry.  COMPLETE is not run: a page just split carries
 * BARK_INCOMPLETE_SPLIT until its downlink is in.
 *
 * A failure is an ERROR, not a PANIC.  The change is already WAL-logged, so
 * the page stays corrupt either way, on the primary and on every standby.
 * The ERROR fails the statement that made the change, at the call site,
 * with the page in the message and the caller in its context, and leaves the
 * page for amcheck to report again.  A PANIC, which is what RethinkDB's
 * rassert amounts to, would only add a restart that replays the same record
 * into the same page, and would abort every other test of the run.  Redo,
 * where an ERROR cannot be contained, checks with bark_verify_redo instead.
 *
 * A change of one leaf entry or a few adjacent ones (an insert, overwrite,
 * added TID or divided entry: most changes) passes their offsets lo..hi,
 * and only those entries and their neighbours are checked, with the page's
 * layout and high key, except that every BARK_VERIFY_FULL_EVERY-th such
 * check in a backend takes the whole page.  Checking the whole page every
 * time made a write-only workload of long equal-key LISTs 5.6x slower under
 * cassert (1.9x bounded), which pushed core regress past its timeout in a
 * coverage build; see "Page validator" in the README for the numbers.  Every other change (splits, new roots, VACUUM, deletion and
 * merging, page deletion, overflow and meta pages) is checked whole.
 */
void
bark_verify_page_change_impl(Relation index, Buffer buf, OffsetNumber lo,
							 OffsetNumber hi, const char *caller)
{
	static uint32 nlocal = 0;
	Page		page;
	BarkVerifyContext ctx;
	ErrorContextCallback errcb;

	if (!bark_verify_pages)
		return;
	Assert(CritSectionCount == 0);
	Assert(BufferIsLockedByMeInMode(buf, BUFFER_LOCK_EXCLUSIVE));

	page = BufferGetPage(buf);
	ctx.caller = caller;
	ctx.blkno = BufferGetBlockNumber(buf);
	errcb.callback = bark_verify_errcontext;
	errcb.arg = &ctx;
	errcb.previous = error_context_stack;
	error_context_stack = &errcb;

	if (PageIsNew(page))
		elog(ERROR, "BARK index \"%s\" left page %u new after changing it",
			 RelationGetRelationName(index), ctx.blkno);
	else if (BarkPageIsMeta(BarkPageGetOpaque(page)) ||
			 BarkPageIsOverflow(BarkPageGetOpaque(page)))
		bark_verify_other_page(page, RelationGetRelationName(index),
							   ctx.blkno, ERROR);
	else if (BarkPageIsDeleted(BarkPageGetOpaque(page)))
		bark_page_verify(index, NULL, NULL, page, ctx.blkno,
						 BARK_VERIFY_SHAPES, ERROR);
	else
	{
		BarkKeyInfo *keyinfo = bark_build_keyinfo(index);

		if (OffsetNumberIsValid(lo) && ++nlocal % BARK_VERIFY_FULL_EVERY == 0)
			lo = hi = InvalidOffsetNumber;
		bark_page_verify_range(index, keyinfo, NULL, page, ctx.blkno,
							   BARK_VERIFY_SHAPES | BARK_VERIFY_ORDER, ERROR,
							   lo, hi);
		pfree(keyinfo);
	}

	error_context_stack = errcb.previous;
}

#endif							/* USE_ASSERT_CHECKING */

#ifdef USE_INJECTION_POINTS

/*
 * Plant a fault in the entry at `off` of the exclusive-locked leaf `page`,
 * whose change is already logged, for the page validator's tests: with
 * bark-verify-fault-count attached, set a LIST entry's count to 1 (a SHAPES
 * check reports it); with bark-verify-fault-order, swap the entry's line
 * pointer with the one before it (an ORDER check reports it).  The page is
 * then corrupt on this server only, as a buggy change would leave it.
 * bark-verify-fault-far swaps the page's first two data entries instead,
 * when both lie more than one entry before `off`, outside what the bounded
 * check of the change looks at (bark_verify_page_change_impl).
 */
void
bark_verify_plant_fault(Page page, OffsetNumber off)
{
	if (IS_INJECTION_POINT_ATTACHED("bark-verify-fault-count"))
	{
		IndexTuple	itup = (IndexTuple) PageGetItem(page,
													PageGetItemId(page, off));

		if (BarkEntryGetShape(itup) == BARK_SHAPE_LIST)
			ItemPointerSetOffsetNumber(&itup->t_tid,
									   (ItemPointerGetOffsetNumberNoCheck(&itup->t_tid) &
										~BARK_OFFSET_MASK) | 1);
	}
	if (IS_INJECTION_POINT_ATTACHED("bark-verify-fault-order") &&
		off > BarkPageFirstDataKey(BarkPageGetOpaque(page)))
	{
		ItemIdData	tmp = *PageGetItemId(page, off);

		*PageGetItemId(page, off) = *PageGetItemId(page, off - 1);
		*PageGetItemId(page, off - 1) = tmp;
	}
	if (IS_INJECTION_POINT_ATTACHED("bark-verify-fault-far"))
	{
		OffsetNumber first = BarkPageFirstDataKey(BarkPageGetOpaque(page));

		if (off > first + 2)
		{
			ItemIdData	tmp = *PageGetItemId(page, first);

			*PageGetItemId(page, first) = *PageGetItemId(page, first + 1);
			*PageGetItemId(page, first + 1) = tmp;
		}
	}
}

#endif							/* USE_INJECTION_POINTS */

/*
 * In WAL redo, check `buf`, block `block_id` of the record just replayed into
 * it, while it is still locked: SHAPES only, since redo has no relcache
 * entry, so no comparator and no catalog access.  A failure is a PANIC:
 * redo cannot leave a page half-trusted, and a standby that went on would
 * serve the corrupt page.  It runs in cassert builds whenever
 * bark_verify_pages is on in the process doing the redo; by then the
 * primary's validator has checked the page in full, so a failure here means
 * redo built a page the primary did not (which wal_consistency_checking
 * would also report, but only on records that carry an image).  As in a
 * backend, a record that changed the entries at lo..hi has only those and
 * their neighbours checked, but every BARK_VERIFY_FULL_EVERY-th one.
 */
void
bark_verify_redo(XLogReaderState *record, uint8 block_id, Buffer buf,
				 OffsetNumber lo, OffsetNumber hi)
{
#ifdef USE_ASSERT_CHECKING
	static uint32 nlocal = 0;
	RelFileLocator rlocator;
	BlockNumber blkno;
	char		relname[NAMEDATALEN];
	Page		page;

	if (!bark_verify_pages || !BufferIsValid(buf))
		return;
	XLogRecGetBlockTag(record, block_id, &rlocator, NULL, &blkno);
	snprintf(relname, sizeof(relname), "%u", rlocator.relNumber);
	page = BufferGetPage(buf);
	if (PageIsNew(page))
		elog(PANIC, "BARK index \"%s\" left page %u new in redo",
			 relname, blkno);
	else if (BarkPageIsMeta(BarkPageGetOpaque(page)) ||
			 BarkPageIsOverflow(BarkPageGetOpaque(page)))
		bark_verify_other_page(page, relname, blkno, PANIC);
	else
	{
		if (OffsetNumberIsValid(lo) && ++nlocal % BARK_VERIFY_FULL_EVERY == 0)
			lo = hi = InvalidOffsetNumber;
		bark_page_verify_range(NULL, NULL, relname, page, blkno,
							   BARK_VERIFY_SHAPES, PANIC, lo, hi);
	}
#endif
}
