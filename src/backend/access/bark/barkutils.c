/*-------------------------------------------------------------------------
 *
 * barkutils.c
 *	  Key comparison helpers for the BARK index access method.
 *
 * BARK orders keys with a btree-family operator class, so a key column is
 * compared with the class's support function 1 -- a three-way comparator
 * returning <0, 0, >0 -- exactly as the btree AM does.  bark_build_keyinfo()
 * resolves that comparator (plus collation and sort direction) once per
 * index; bark_compare_itups() uses it to order two index tuples.
 *
 * This is the BARK-specific scankey machinery the build and (later) search
 * paths need: nbtree's _bt_mkscankey() cannot be reused because it reads the
 * index meta page as a btree meta page, and BARK's meta page has a different
 * layout.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/bark/barkutils.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "access/bark.h"
#include "access/barkxlog.h"
#include "access/detoast.h"
#include "access/heaptoast.h"
#include "access/htup_details.h"
#include "access/itup.h"
#include "access/toast_internals.h"
#include "access/xloginsert.h"
#include "catalog/pg_type.h"
#include "lib/sbm.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/indexfsm.h"
#include "utils/lsyscache.h"
#include "utils/pg_locale.h"
#include "utils/rel.h"

static Sbm *bark_posting_open(IndexTuple itup);

/*
 * Translate between BARK strategy numbers and the generic CompareType.  BARK
 * uses the btree strategy numbers (1=<, 2=<=, 3==, 4=>=, 5=>), so these are
 * the same mappings the btree AM uses; the planner needs them to find an
 * opfamily's equality operator by compare type (e.g. when building pathkeys
 * for an ordered scan).
 */
CompareType
bark_translate_strategy(StrategyNumber strategy, Oid opfamily)
{
	switch (strategy)
	{
		case BTLessStrategyNumber:
			return COMPARE_LT;
		case BTLessEqualStrategyNumber:
			return COMPARE_LE;
		case BTEqualStrategyNumber:
			return COMPARE_EQ;
		case BTGreaterEqualStrategyNumber:
			return COMPARE_GE;
		case BTGreaterStrategyNumber:
			return COMPARE_GT;
		default:
			return COMPARE_INVALID;
	}
}

StrategyNumber
bark_translate_cmptype(CompareType cmptype, Oid opfamily)
{
	switch (cmptype)
	{
		case COMPARE_LT:
			return BTLessStrategyNumber;
		case COMPARE_LE:
			return BTLessEqualStrategyNumber;
		case COMPARE_EQ:
			return BTEqualStrategyNumber;
		case COMPARE_GE:
			return BTGreaterEqualStrategyNumber;
		case COMPARE_GT:
			return BTGreaterStrategyNumber;
		default:
			return InvalidStrategy;
	}
}

/*
 * Build the per-column comparison state for a BARK index.
 *
 * Resolves support function 1 (the ordering comparator) for each key column
 * from the index's operator class, together with the column's collation and
 * its ASC/DESC and NULLS FIRST/LAST options.  The result is palloc'd in the
 * current memory context and reused for every comparison.
 */
BarkKeyInfo *
bark_build_keyinfo(Relation index)
{
	int			nkeys = IndexRelationGetNumberOfKeyAttributes(index);
	BarkKeyInfo *keyinfo;

	keyinfo = (BarkKeyInfo *) palloc0(offsetof(BarkKeyInfo, cols) +
									  nkeys * sizeof(BarkKeyColumn));
	keyinfo->heaprel = NULL;	/* writers set it */
	keyinfo->nkeys = nkeys;

	for (int i = 0; i < nkeys; i++)
	{
		BarkKeyColumn *col = &keyinfo->cols[i];
		FmgrInfo   *procinfo;
		int16		indoption = index->rd_indoption[i];

		/*
		 * Support function 1 is the three-way comparator.  index_getprocinfo
		 * caches the lookup on the relcache entry, so this is cheap to call
		 * and the FmgrInfo stays valid for the life of the relation; copy it
		 * into our own state so the keyinfo is self-contained.
		 */
		procinfo = index_getprocinfo(index, i + 1, BARK_ORDER_PROC);
		fmgr_info_copy(&col->cmp, procinfo, CurrentMemoryContext);
		col->collation = index->rd_indcollation[i];
		col->reverse = (indoption & INDOPTION_DESC) != 0;
		col->nulls_first = (indoption & INDOPTION_NULLS_FIRST) != 0;

		/*
		 * The OVERSIZED inline-prefix fast path (see bark_compare_itups) is
		 * sound only when a leading-byte difference in the first key column's
		 * datum decides its order -- i.e. the column sorts bytewise.  That is
		 * true for a text/bytea-style column under a C/POSIX collation; a
		 * locale-aware collation can reorder across the prefix boundary, so
		 * the fast path is disabled there and comparison fetches the full
		 * value.  Only the first key column matters (the prefix only shortcuts
		 * when the leading column differs).
		 */
		col->bytewise = (i == 0 && OidIsValid(col->collation) &&
						 pg_newlocale_from_collation(col->collation)->collate_is_c);
	}

	return keyinfo;
}

/*
 * Can equal keys in this index share one leaf entry (LIST or POSTING)?
 *
 * A LIST or POSTING entry stores its key attributes once, and an index-only
 * scan returns those bytes for every member.  That is only correct when every
 * row that compares equal also has an identical stored image, so the same
 * rule nbtree applies to deduplication (_bt_allequalimage) applies here: no
 * INCLUDE columns (their values differ between rows with equal keys), and
 * every key column's opclass must have an equalimage support function that
 * says yes for the column's collation.  Without it, text under a
 * nondeterministic collation or numeric (1.0 = 1.00) would hand back the
 * first row's value for all of them.
 */
bool
bark_allequalimage(Relation index)
{
	if (IndexRelationGetNumberOfAttributes(index) !=
		IndexRelationGetNumberOfKeyAttributes(index))
		return false;

	for (int i = 0; i < IndexRelationGetNumberOfKeyAttributes(index); i++)
	{
		Oid			opcintype = index->rd_opcintype[i];
		Oid			proc = get_opfamily_proc(index->rd_opfamily[i], opcintype,
											opcintype, BARK_EQUALIMAGE_PROC);

		if (!OidIsValid(proc) ||
			!DatumGetBool(OidFunctionCall1Coll(proc, index->rd_indcollation[i],
											   ObjectIdGetDatum(opcintype))))
			return false;
	}

	return true;
}

/*
 * Allocate a page for the index, reusing an FSM-recorded free page when one is
 * available and extending the relation only otherwise.  Modeled on nbtree's
 * _bt_allocbuf: a page the FSM names may have been reused by someone else
 * since, so we take the lock conditionally and re-check that the page really
 * is free before handing it back; a page that no longer qualifies is skipped
 * and the FSM is asked again.
 *
 * A page is free when it is new or when it is deleted and safe to recycle
 * (BarkPageIsRecyclable).  VACUUM records a deleted page in the FSM only once
 * it is recyclable, so the second test normally passes; it is repeated here
 * because the FSM is not WAL-logged and a page can be listed there by mistake
 * (after a crash, say).  heaprel is the index's heap, which chooses the
 * visibility horizon for that test and tells a standby whether the page
 * belongs to a catalog.  A deleted page that is not yet recyclable is left
 * alone; a later VACUUM records it again.
 *
 * Before a deleted page is reused, an XLOG_BARK_REUSE_PAGE record is written
 * when hot standby may be running queries, as _bt_allocbuf does: the page's
 * safexid is the conflict horizon that cancels any standby query that may
 * still hold a link to the page.  The page itself is reinitialized by the
 * caller's own record.
 *
 * The returned buffer is pinned and exclusive-locked; its page is left as-is
 * (new or deleted), so the caller's PageInit fully reinitializes it.
 */
Buffer
bark_get_free_page(Relation index, Relation heaprel)
{
	Buffer		buf;

	Assert(heaprel != NULL);

	for (;;)
	{
		BlockNumber blkno = GetFreeIndexPage(index);

		if (blkno == InvalidBlockNumber)
			break;

		/* The meta page is never free; ignore a stale FSM entry for it. */
		if (blkno == BARK_METAPAGE)
			continue;

		buf = ReadBuffer(index, blkno);

		/*
		 * Someone may already have recycled this page (and be holding its
		 * lock); only reuse it if we can take the lock without waiting and the
		 * page still looks free.
		 */
		if (ConditionalLockBuffer(buf))
		{
			Page		page = BufferGetPage(buf);

			if (PageIsNew(page))
				return buf;		/* never initialized: OK */
			if (BarkPageIsRecyclable(page, heaprel))
			{
				/* deleted long enough ago: OK */
				if (RelationNeedsWAL(index) && XLogStandbyInfoActive())
				{
					xl_bark_reuse_page xlrec;

					/*
					 * No buffer is registered: this record changes no page
					 * (see xl_bark_reuse_page).
					 */
					xlrec.locator = index->rd_locator;
					xlrec.block = blkno;
					xlrec.snapshotConflictHorizon = BarkPageGetDeleteXid(page);
					xlrec.isCatalogRel =
						RelationIsAccessibleInLogicalDecoding(heaprel);

					XLogBeginInsert();
					XLogRegisterData(&xlrec, SizeOfBarkReusePage);
					XLogInsert(RM_BARK_ID, XLOG_BARK_REUSE_PAGE);
				}
				return buf;
			}

			LockBuffer(buf, BUFFER_LOCK_UNLOCK);
		}

		ReleaseBuffer(buf);		/* not usable: try the next FSM entry */
	}

	/*
	 * No reusable page: extend the relation.  ExtendBufferedRel takes the
	 * relation extension lock, so concurrent extenders each get a distinct
	 * new block (ReadBuffer(P_NEW) skips that lock and would let two splits
	 * receive the same page).  As in nbtree's _bt_allocbuf.
	 */
	return ExtendBufferedRel(BMR_REL(index), MAIN_FORKNUM, NULL, EB_LOCK_FIRST);
}

/*
 * Compare two BARK index tuples by their key columns.
 *
 * Returns <0, 0, or >0 as a sorts before, equal to, or after b.  NULLs are
 * ordered per each column's NULLS FIRST/LAST option, and a DESC column
 * inverts the comparator's result -- the same total order the btree AM
 * imposes, which is what lets BARK share btree operator families.
 *
 * An OVERSIZED entry carries no key attributes inline (its full tuple lives on
 * an overflow chain).  Two OVERSIZED entries whose first key column sorts
 * bytewise (a C/POSIX collation) are first compared on the short inline prefix
 * each one caches (BarkOverflowRef.prefix); only when those prefixes tie, or
 * when either operand lacks a usable prefix, do we fetch the full tuple(s) from
 * the chain and compare those.  Fetching the whole key is what makes out-of-
 * line storage transparent: two keys that share a long prefix still order on
 * their full value, because the comparison ultimately sees the whole key.  (The
 * caller must pass a non-NULL `index` so the overflow chain can be read; every
 * caller does.)
 */
int
bark_compare_itups(BarkKeyInfo *keyinfo, Relation index,
				   IndexTuple a, IndexTuple b)
{
	TupleDesc	 tupdesc = RelationGetDescr(index);
	IndexTuple	 afull = NULL;
	IndexTuple	 bfull = NULL;
	int			 na;
	int			 nb;
	int			 ncmp;
	int			 result = 0;
	int			 na_override = -1;
	int			 nb_override = -1;

	/*
	 * OVERSIZED compare fast path.  When both operands are OVERSIZED entries
	 * that carry an inline prefix of a bytewise (C/POSIX-collation) first key
	 * column, compare those prefixes before touching the overflow chains: a
	 * leading-byte difference decides the order with no I/O.  The chains are
	 * fetched (below) only when the prefixes tie and at least one is truncated,
	 * so a tie might not be real.  Two complete prefixes that are byte-equal
	 * and the same length are equal on the first column -- but there may be
	 * further key columns, so a decisive answer here requires the prefix
	 * comparison to be non-zero; an equal prefix always falls through to the
	 * full compare.  reverse is applied exactly as the per-column loop does.
	 */
	if (keyinfo->nkeys > 0 && keyinfo->cols[0].bytewise &&
		BarkEntryGetShape(a) == BARK_SHAPE_OVERSIZED &&
		BarkEntryGetShape(b) == BARK_SHAPE_OVERSIZED)
	{
		BarkOverflowRef *ra = BarkOverflowGetRef(a);
		BarkOverflowRef *rb = BarkOverflowGetRef(b);

		if (ra->prefixlen > 0 && rb->prefixlen > 0)
		{
			uint16		cmplen = Min(ra->prefixlen, rb->prefixlen);
			int			c = memcmp(ra->prefix, rb->prefix, cmplen);

			if (c != 0)
				return keyinfo->cols[0].reverse ? -c : c;

			/*
			 * Equal over the shared prefix length.  If the shorter prefix is
			 * COMPLETE (its whole column fit), the columns differ in length:
			 * the shorter column sorts first (bytewise, a prefix is less than
			 * a longer string sharing it).  Only decide here when the longer
			 * side actually has more bytes; equal length + both complete is a
			 * genuine first-column tie that must fall through to later columns.
			 */
			if (ra->prefixlen != rb->prefixlen)
			{
				bool		shorter_a = ra->prefixlen < rb->prefixlen;
				bool		shorter_complete = shorter_a ? ra->prefixcomplete
					: rb->prefixcomplete;

				if (shorter_complete)
				{
					c = shorter_a ? -1 : 1;
					return keyinfo->cols[0].reverse ? -c : c;
				}
			}
			/* else: tie so far with a truncated prefix -- fall through, fetch. */
		}
	}

	/*
	 * Resolve any OVERSIZED operand to its full, inline-comparable tuple.  An
	 * OVERSIZED pivot's full tuple is a plain truncated key tuple carrying only
	 * its natts attributes, so remember that count from the ref before fetching
	 * (the fetched tuple no longer records that it is a pivot).
	 */
	if (BarkEntryGetShape(a) == BARK_SHAPE_OVERSIZED)
	{
		if (!BarkOverflowIsLeaf(a))
			na_override = BarkOverflowGetRef(a)->natts;
		a = afull = bark_fetch_oversized(index, a);
	}
	if (BarkEntryGetShape(b) == BARK_SHAPE_OVERSIZED)
	{
		if (!BarkOverflowIsLeaf(b))
			nb_override = BarkOverflowGetRef(b)->natts;
		b = bfull = bark_fetch_oversized(index, b);
	}

	if (na_override >= 0)
		na = na_override;
	else
		na = (BarkEntryGetShape(a) == BARK_SHAPE_PIVOT) ? BarkPivotGetNAtts(a) : keyinfo->nkeys;
	if (nb_override >= 0)
		nb = nb_override;
	else
		nb = (BarkEntryGetShape(b) == BARK_SHAPE_PIVOT) ? BarkPivotGetNAtts(b) : keyinfo->nkeys;
	ncmp = Min(na, nb);

	/*
	 * Compare the key attributes both tuples carry.  Only a PIVOT tuple may
	 * have been truncated to fewer attributes (BarkPivotGetNAtts); the
	 * leftmost downlink on an internal page is the extreme case, a
	 * minus-infinity pivot with zero key attributes.  Such a tuple compares
	 * less than any tuple that agrees on the attributes they share but has
	 * more of them, which keeps a minus-infinity downlink first in key order.
	 * LIST and POSTING entries also set the alt-TID bit, but their offset-field
	 * low bits hold a locator count, not an attribute count -- they carry the
	 * full set of key attributes, exactly like a SINGLE entry, so they compare
	 * on all nkeys.
	 */
	for (int i = 0; i < ncmp; i++)
	{
		BarkKeyColumn *col = &keyinfo->cols[i];
		bool		anull;
		bool		bnull;
		Datum		adatum = index_getattr(a, i + 1, tupdesc, &anull);
		Datum		bdatum = index_getattr(b, i + 1, tupdesc, &bnull);
		int			cmp;

		if (anull || bnull)
		{
			if (anull && bnull)
				continue;		/* both NULL: equal in this column */
			/* One NULL: order it per the column's NULLS FIRST/LAST option. */
			if (anull)
				result = col->nulls_first ? -1 : 1;
			else
				result = col->nulls_first ? 1 : -1;
			goto out;
		}

		cmp = DatumGetInt32(FunctionCall2Coll(&col->cmp, col->collation,
											  adatum, bdatum));
		if (cmp != 0)
		{
			result = col->reverse ? -cmp : cmp;
			goto out;
		}
	}

	/*
	 * Equal on every shared attribute.  The tuple with fewer key attributes
	 * (a more-truncated pivot) sorts first; equal attribute counts are equal
	 * keys.
	 */
	if (na != nb)
		result = (na < nb) ? -1 : 1;

out:
	if (afull)
		pfree(afull);
	if (bfull)
		pfree(bfull);
	return result;
}

/*
 * The number of leading key attributes a pivot that separates `lastleft` from
 * `firstright`, adjacent leaf items in key order, must keep: one more than the
 * number of leading attributes on which the two are equal.  A return of
 * nkeyatts + 1 means they are equal on every key attribute; nbtree then keeps
 * a heap TID, and BARK, which has none, keeps every key attribute.  A port of
 * nbtree's _bt_keep_natts, also used as the penalty of a split point.
 *
 * Equality is decided by the opclass comparator (NULLs equal only to NULLs),
 * not by image equality: the pivot must compare correctly against both keys,
 * and two values can have different images yet compare equal.  An OVERSIZED
 * item's key is read from its overflow chain.
 */
int
bark_keep_natts(Relation index, BarkKeyInfo *keyinfo, IndexTuple lastleft,
				IndexTuple firstright)
{
	TupleDesc	tupdesc = RelationGetDescr(index);
	IndexTuple	left = lastleft;
	IndexTuple	right = firstright;
	int			keepnatts = 1;

	if (BarkEntryGetShape(left) == BARK_SHAPE_OVERSIZED)
		left = bark_fetch_oversized(index, left);
	if (BarkEntryGetShape(right) == BARK_SHAPE_OVERSIZED)
		right = bark_fetch_oversized(index, right);

	for (int i = 0; i < keyinfo->nkeys; i++)
	{
		BarkKeyColumn *col = &keyinfo->cols[i];
		bool		lnull;
		bool		rnull;
		Datum		ldatum = index_getattr(left, i + 1, tupdesc, &lnull);
		Datum		rdatum = index_getattr(right, i + 1, tupdesc, &rnull);

		if (lnull != rnull)
			break;
		if (!lnull &&
			DatumGetInt32(FunctionCall2Coll(&col->cmp, col->collation,
											ldatum, rdatum)) != 0)
			break;
		keepnatts++;
	}

	if (left != lastleft)
		pfree(left);
	if (right != firstright)
		pfree(right);
	return keepnatts;
}

/*
 * The heap TID of a pivot of either shape (a plain PIVOT or an OVERSIZED
 * pivot), or NULL when it has none: minus infinity on the heap TID.
 */
ItemPointer
bark_pivot_heap_tid(IndexTuple pivot)
{
	if (BarkEntryGetShape(pivot) == BARK_SHAPE_OVERSIZED)
	{
		BarkOverflowRef *ref = BarkOverflowGetRef(pivot);

		Assert(ref->natts != BARK_OVERFLOW_LEAF);
		return ItemPointerIsValid(&ref->pivottid) ? &ref->pivottid : NULL;
	}
	Assert(BarkEntryGetShape(pivot) == BARK_SHAPE_PIVOT);
	return BarkPivotGetHeapTID(pivot);
}

/*
 * The lowest and highest heap TIDs of a leaf entry, the range the entry
 * occupies in the order of a run of equal keys: its t_tid for a SINGLE, its
 * ref's locator for an OVERSIZED entry, the first and last members of a LIST,
 * the sbm minimum and maximum of a POSTING.
 */
void
bark_entry_tid_range(IndexTuple itup, ItemPointer lo, ItemPointer hi)
{
	switch (BarkEntryGetShape(itup))
	{
		case BARK_SHAPE_SINGLE:
			*lo = *hi = itup->t_tid;
			break;
		case BARK_SHAPE_OVERSIZED:
			Assert(BarkOverflowIsLeaf(itup));
			*lo = *hi = BarkOverflowGetRef(itup)->locator;
			break;
		case BARK_SHAPE_LIST:
			*lo = *BarkListGetTID(itup, 0);
			*hi = *BarkListGetTID(itup, BarkListGetCount(itup) - 1);
			break;
		case BARK_SHAPE_POSTING:
			{
				Sbm		   *map = bark_posting_open(itup);

				bark_key_to_tid(sbm_minimum(map), lo);
				bark_key_to_tid(sbm_maximum(map), hi);
				sbm_free(map);
				break;
			}
		default:
			elog(ERROR, "BARK leaf entry has unexpected shape %d",
				 (int) BarkEntryGetShape(itup));
	}
}

/*
 * Compare `key` and, when `scantid` is not NULL, the heap TID `scantid` with
 * `itup`, a pivot or a leaf entry, in the order of the tree: by key first
 * (bark_compare_itups), then, within a key, by heap TID.  This is the
 * comparison of nbtree's insertion scan key with a scantid (_bt_compare).
 *
 * Against a pivot equal on every key attribute, the pivot's heap TID decides,
 * and a pivot without one is minus infinity, so the search key sorts after
 * it.  (A pivot truncated on key attributes never ties: bark_compare_itups
 * sorts the shorter one first.)  Against a leaf entry, 0 means scantid lies
 * inside the entry's TID range, which an insert of that TID must go into.
 */
int
bark_compare_itups_tid(BarkKeyInfo *keyinfo, Relation index, IndexTuple key,
					   ItemPointer scantid, IndexTuple itup)
{
	int			cmp = bark_compare_itups(keyinfo, index, key, itup);
	ItemPointerData lo;
	ItemPointerData hi;

	if (cmp != 0 || scantid == NULL)
		return cmp;

	if (!BarkEntryIsLeafData(itup) ||
		(BarkEntryGetShape(itup) == BARK_SHAPE_OVERSIZED &&
		 !BarkOverflowIsLeaf(itup)))
	{
		ItemPointer ptid = bark_pivot_heap_tid(itup);

		if (ptid == NULL)
			return 1;
		return ItemPointerCompare(scantid, ptid);
	}

	bark_entry_tid_range(itup, &lo, &hi);
	if (ItemPointerCompare(scantid, &lo) < 0)
		return -1;
	if (ItemPointerCompare(scantid, &hi) > 0)
		return 1;
	return 0;
}

/* ----------------------------------------------------------------------------
 * LIST entry construction and reading
 *
 * A LIST entry stores one key with many heap locators.  It is a SINGLE-shape
 * key tuple (header + attribute data) extended with an ascending, duplicate-
 * free array of ItemPointerData locators appended after the key, starting at
 * the MAXALIGNed end of the key prefix.  The alt-TID bit is set with
 * BARK_IS_LIST; the t_tid offset field carries the locator count and the t_tid
 * block field records the body offset (so the key/body split point is O(1)
 * recoverable even though t_info's size covers the whole entry).
 * ----------------------------------------------------------------------------
 */

/*
 * Form a LIST entry from a SINGLE-shape key tuple and `ntids` ascending,
 * distinct locators.  The key's own t_tid is overwritten with the LIST status
 * bits, count, and body offset.  Returns a palloc'd entry.
 */
IndexTuple
bark_form_list(TupleDesc tupdesc, IndexTuple key, ItemPointer tids, int ntids)
{
	Size		keysz = IndexTupleSize(key);
	Size		bodyoff = MAXALIGN(keysz);
	Size		total = bodyoff + ntids * sizeof(ItemPointerData);
	IndexTuple	entry;

	Assert(ntids >= 1 && ntids <= BARK_LIST_MAX_COUNT);
	Assert(bodyoff <= BARK_OFFSET_MASK);		/* fits a uint16 body offset */
	Assert(total <= INDEX_SIZE_MASK);

	entry = (IndexTuple) palloc0(total);
	memcpy(entry, key, keysz);

	/* Record the whole-entry size in t_info, preserving the null bitmap bit. */
	entry->t_info = (key->t_info & ~INDEX_SIZE_MASK) | (uint16) total;

	/* Stamp the alt-TID status, count, and body offset into t_tid. */
	entry->t_info |= INDEX_AM_RESERVED_BIT;
	ItemPointerSetOffsetNumber(&entry->t_tid,
							   (OffsetNumber) ((uint16) ntids | BARK_IS_LIST));
	BarkEntrySetBodyOffset(entry, (uint16) bodyoff);

	memcpy(BarkListGetTIDArray(entry), tids, ntids * sizeof(ItemPointerData));
	return entry;
}

/* Number of heap locators a leaf entry holds (SINGLE = 1, LIST/POSTING = set). */
int
bark_entry_count_tids(IndexTuple itup)
{
	switch (BarkEntryGetShape(itup))
	{
		case BARK_SHAPE_SINGLE:
			return 1;
		case BARK_SHAPE_OVERSIZED:
			return 1;			/* one heap locator, kept inline in the ref */
		case BARK_SHAPE_LIST:
			return BarkListGetCount(itup);
		case BARK_SHAPE_POSTING:
			return bark_posting_count(itup);
		default:
			elog(ERROR, "BARK leaf entry has unexpected shape %d",
				 (int) BarkEntryGetShape(itup));
			return 0;			/* keep the compiler happy */
	}
}

/*
 * Copy a leaf entry's heap locators, ascending, into `out` (capacity maxout).
 * Returns the number written.  SINGLE yields its single t_tid; LIST yields its
 * stored array verbatim (already ascending); POSTING is iterated through its
 * sbm (ascending by construction).
 */
int
bark_entry_get_tids(IndexTuple itup, ItemPointer out, int maxout)
{
	switch (BarkEntryGetShape(itup))
	{
		case BARK_SHAPE_SINGLE:
			Assert(maxout >= 1);
			out[0] = itup->t_tid;
			return 1;
		case BARK_SHAPE_OVERSIZED:
			Assert(maxout >= 1);
			out[0] = BarkOverflowGetRef(itup)->locator;
			return 1;
		case BARK_SHAPE_LIST:
			{
				int			n = BarkListGetCount(itup);

				Assert(maxout >= n);
				memcpy(out, BarkListGetTIDArray(itup),
					   n * sizeof(ItemPointerData));
				return n;
			}
		case BARK_SHAPE_POSTING:
			return bark_posting_get_tids(itup, out, maxout);
		default:
			elog(ERROR, "BARK leaf entry has unexpected shape %d",
				 (int) BarkEntryGetShape(itup));
			return 0;
	}
}

/*
 * Reform a clean SINGLE-shape key tuple from any leaf entry.  The key and
 * INCLUDE attributes sit at the front of every leaf shape, so deforming with
 * the index tuple descriptor and reforming yields a tuple with no appended
 * body and no alt-TID status.  When `tid` is given it becomes the result's
 * locator (SINGLE shape); otherwise the caller sets t_tid itself.
 */
IndexTuple
bark_single_from_list(Relation index, IndexTuple entry, ItemPointer tid)
{
	TupleDesc	tupdesc = RelationGetDescr(index);
	Datum		values[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS];
	IndexTuple	single;

	index_deform_tuple(entry, tupdesc, values, isnull);
	single = index_form_tuple(tupdesc, values, isnull);
	if (tid != NULL)
		single->t_tid = *tid;
	return single;
}

/* ----------------------------------------------------------------------------
 * POSTING entry construction and reading (sbm-backed inverted locator set)
 * ----------------------------------------------------------------------------
 */

/*
 * Reversible, block-clustered TID <-> uint64 mapping.  A run of heap TIDs on
 * one block maps to a contiguous run of sbm indexes, which the sbm encodes
 * densely.  offset is 1-based on a heap page, so it is biased by one.
 */
uint64
bark_tid_to_key(ItemPointer tid)
{
	BlockNumber blk = ItemPointerGetBlockNumberNoCheck(tid);
	OffsetNumber off = ItemPointerGetOffsetNumberNoCheck(tid);

	return (uint64) blk * MaxHeapTuplesPerPage + (off - 1);
}

void
bark_key_to_tid(uint64 key, ItemPointer tid)
{
	BlockNumber blk = (BlockNumber) (key / MaxHeapTuplesPerPage);
	OffsetNumber off = (OffsetNumber) (key % MaxHeapTuplesPerPage) + 1;

	ItemPointerSet(tid, blk, off);
}

/*
 * Lay out a POSTING entry of `total` bytes (see BarkPostingEntrySize) holding
 * key's columns and the serialization of `map`.  The space between the end of
 * the serialization and `total` is the entry's reserve for later removals; it
 * is zeroed.
 */
static IndexTuple
bark_posting_from_map(IndexTuple key, Sbm *map, Size total)
{
	Size		keysz = IndexTupleSize(key);
	Size		bodyoff = MAXALIGN(keysz);
	Size		serialized = sbm_serialized_size(map);
	uint16		datalen = (uint16) serialized;
	IndexTuple	entry;

	Assert(bodyoff <= BARK_OFFSET_MASK);
	Assert(total <= INDEX_SIZE_MASK);
	Assert(bodyoff + sizeof(uint16) + serialized <= total);

	entry = (IndexTuple) palloc0(total);
	memcpy(entry, key, keysz);
	entry->t_info = (key->t_info & ~INDEX_SIZE_MASK) | (uint16) total;
	entry->t_info |= INDEX_AM_RESERVED_BIT;
	ItemPointerSetOffsetNumber(&entry->t_tid, (OffsetNumber) BARK_IS_POSTING);
	BarkEntrySetBodyOffset(entry, (uint16) bodyoff);

	memcpy((char *) entry + bodyoff, &datalen, sizeof(uint16));
	if (sbm_serialize(map, BarkPostingGetData(entry), serialized) != serialized)
		elog(ERROR, "sbm_serialize wrote unexpected length for BARK posting entry");
	return entry;
}

/*
 * Build a POSTING entry from a SINGLE-shape key tuple and `ntids` ascending,
 * distinct locators.  The locator set is serialized with sbm into the entry's
 * body, and the entry is sized for the set's removal bound, so that VACUUM
 * can rewrite it in place whatever members it removes.  Returns NULL when
 * that size is not smaller than the equivalent LIST (the caller then keeps
 * the LIST), so POSTING is used only when it saves space even with the
 * reserve, or when it would not fit t_info's size field.
 */
IndexTuple
bark_form_posting(TupleDesc tupdesc, IndexTuple key, ItemPointer tids, int ntids)
{
	Size		keysz = IndexTupleSize(key);
	Size		listsz;
	Size		total;
	uint64	   *keys;
	Sbm		   *map;
	IndexTuple	entry;

	Assert(ntids >= 1);

	/*
	 * Build the sbm from the block-clustered keys in one pass.  Adding them
	 * one at a time re-walks the chunk list from its head on every add, which
	 * is quadratic for a scattered set, where every member has a chunk.
	 */
	keys = palloc_array(uint64, ntids);
	for (int i = 0; i < ntids; i++)
		keys[i] = bark_tid_to_key(&tids[i]);
	map = sbm_create_from_array(keys, ntids);
	pfree(keys);

	total = BarkPostingEntrySize(keysz, sbm_removal_bound(map));
	listsz = MAXALIGN(MAXALIGN(keysz) + ntids * sizeof(ItemPointerData));
	if (total >= listsz || total > INDEX_SIZE_MASK)
	{
		sbm_free(map);
		return NULL;
	}

	entry = bark_posting_from_map(key, map, total);
	sbm_free(map);
	return entry;
}

/* Open a POSTING entry's serialized sbm (caller must sbm_free the result). */
static Sbm *
bark_posting_open(IndexTuple itup)
{
	Sbm		   *map = NULL;

	if (BarkPostingDataFits(itup))
		map = sbm_deserialize(BarkPostingGetData(itup),
							  BarkPostingGetDataSize(itup));
	if (map == NULL)
		elog(ERROR, "BARK posting entry has a corrupt sbm serialization");
	return map;
}

/* Number of locators in a POSTING entry's sbm set. */
int
bark_posting_count(IndexTuple itup)
{
	Sbm		   *map = bark_posting_open(itup);
	int			n = (int) sbm_cardinality(map);

	sbm_free(map);
	return n;
}

/* Destination for bark_posting_collect: an ItemPointer array and its fill. */
typedef struct BarkTidSink
{
	ItemPointer out;
	int			n;
	int			maxout;
} BarkTidSink;

/*
 * sbm_scan callback: append a batch of members, as heap TIDs, to a
 * BarkTidSink.  Decoding a whole entry through sbm_scan costs one chunk walk;
 * sbm_next_member re-finds the chunk on every call.
 */
static void
bark_posting_collect(uint64 vec[], size_t n, void *aux)
{
	BarkTidSink *sink = (BarkTidSink *) aux;

	Assert(sink->n + n <= (size_t) sink->maxout);
	for (size_t i = 0; i < n; i++)
		bark_key_to_tid(vec[i], &sink->out[sink->n++]);
}

/*
 * Read a POSTING entry's locators, ascending, into `out` (capacity maxout).
 * Returns the number written.  sbm iterates in ascending index order, which
 * the block-clustered mapping turns back into ascending TID order.
 */
int
bark_posting_get_tids(IndexTuple itup, ItemPointer out, int maxout)
{
	Sbm		   *map = bark_posting_open(itup);
	BarkTidSink sink;

	sink.out = out;
	sink.n = 0;
	sink.maxout = maxout;
	sbm_scan(map, bark_posting_collect, 0, &sink);
	sbm_free(map);
	return sink.n;
}

/*
 * Incrementally add one locator to an existing POSTING entry, returning a fresh
 * POSTING entry with `newtid` included.  The existing body is deserialized
 * once, the one new key is added (sbm dedups and keeps order, so `newtid` may
 * be anywhere, not only an append), and the set is re-serialized once -- O(the
 * serialized size), not O(members), so repeated single-row inserts into one
 * key's POSTING set are O(1) amortized for a clustered set rather than the
 * O(members) a full re-read-and-rebuild costs.  Like bark_form_posting, the
 * new entry is sized for the grown set's removal bound.  Returns NULL when the
 * new entry would exceed `maxsz` (the caller then falls back to the general
 * re-encode / split path, keeping the LIST<->POSTING shape decision in one
 * place).
 */
IndexTuple
bark_posting_add_tid(IndexTuple key, IndexTuple posting,
					 ItemPointer newtid, Size maxsz)
{
	Sbm		   *map = bark_posting_open(posting);
	Size		total;
	IndexTuple	entry;

	if (sbm_add_grow(&map, bark_tid_to_key(newtid)) == SBM_IDX_MAX)
		elog(ERROR, "sbm_add_grow failed extending BARK posting entry");

	total = BarkPostingEntrySize(IndexTupleSize(key), sbm_removal_bound(map));
	if (total > maxsz || total > INDEX_SIZE_MASK)
	{
		sbm_free(map);
		return NULL;			/* too big: caller re-encodes / splits */
	}

	entry = bark_posting_from_map(key, map, total);
	sbm_free(map);
	return entry;
}

/*
 * The key part of a LIST or POSTING entry as a key tuple of its own: the
 * bytes before the body, with t_info's size set to them and the alt-TID bit
 * cleared.  The result's t_tid is meaningless; callers overwrite it.
 */
static IndexTuple
bark_entry_key_part(IndexTuple entry)
{
	Size		keysz = BarkEntryGetBodyOffset(entry);
	IndexTuple	key = (IndexTuple) palloc(keysz);

	memcpy(key, entry, keysz);
	key->t_info = (key->t_info & ~(INDEX_SIZE_MASK | INDEX_AM_RESERVED_BIT)) |
		(uint16) keysz;
	return key;
}

/*
 * Return `entry`, a LIST or POSTING entry, with heap TID `tid` added, or NULL
 * when the result would be larger than `maxsz` (or, for a LIST, when the
 * LIST is full or already holds the TID).
 *
 * This is the whole of an insert's change to the entry, so that WAL can log
 * just the TID (XLOG_BARK_ADD_TID) and replay can call this function again
 * on the same entry.  It must therefore depend on nothing but its arguments:
 * the key comes from the entry itself, not from the inserted tuple, whose key
 * is equal but whose image may differ (the TOAST compression method of a
 * large value, say); bark_allequalimage only promises that equal keys mean
 * the same value.
 */
IndexTuple
bark_entry_add_tid(IndexTuple entry, ItemPointer tid, Size maxsz)
{
	if (BarkEntryGetShape(entry) == BARK_SHAPE_LIST)
	{
		int			ncur = BarkListGetCount(entry);
		Size		cursz = IndexTupleSize(entry);
		Size		appended = cursz + sizeof(ItemPointerData);
		Size		insoff;
		int			ins = ncur;
		IndexTuple	ext;

		if (ncur >= BARK_LIST_MAX_COUNT || MAXALIGN(appended) > maxsz)
			return NULL;

		/*
		 * The new locator usually sorts after every member (heap TIDs mostly
		 * ascend); otherwise binary-search for its place.
		 */
		if (ItemPointerCompare(tid, BarkListGetTID(entry, ncur - 1)) <= 0)
		{
			int			lo = 0;

			while (lo < ins)
			{
				int			mid = lo + (ins - lo) / 2;

				if (ItemPointerCompare(BarkListGetTID(entry, mid), tid) < 0)
					lo = mid + 1;
				else
					ins = mid;
			}
			if (ItemPointerEquals(BarkListGetTID(entry, ins), tid))
				return NULL;
		}

		/*
		 * The body is a packed ascending ItemPointerData array ending at the
		 * entry's used size; the new locator goes at its place in it, and the
		 * members after it move up by one.  The body offset (t_tid block
		 * field) is unchanged; only the count and size change.
		 */
		insoff = (char *) BarkListGetTID(entry, ins) - (char *) entry;
		ext = (IndexTuple) palloc0(appended);
		memcpy(ext, entry, insoff);
		memcpy((char *) ext + insoff, tid, sizeof(ItemPointerData));
		memcpy((char *) ext + insoff + sizeof(ItemPointerData),
			   (char *) entry + insoff, cursz - insoff);
		ext->t_info = (ext->t_info & ~INDEX_SIZE_MASK) | (uint16) appended;
		ItemPointerSetOffsetNumber(&ext->t_tid,
								   (OffsetNumber) ((uint16) (ncur + 1) |
												   BARK_IS_LIST));
		return ext;
	}
	else
	{
		IndexTuple	key = bark_entry_key_part(entry);
		IndexTuple	ext;

		Assert(BarkEntryGetShape(entry) == BARK_SHAPE_POSTING);
		ext = bark_posting_add_tid(key, entry, tid, maxsz);
		pfree(key);
		return ext;
	}
}

/*
 * Is `tid` one of the heap TIDs of the leaf entry `itup`?
 */
bool
bark_entry_has_tid(IndexTuple itup, ItemPointer tid)
{
	switch (BarkEntryGetShape(itup))
	{
		case BARK_SHAPE_LIST:
			{
				int			lo = 0;
				int			hi = BarkListGetCount(itup);

				while (lo < hi)
				{
					int			mid = lo + (hi - lo) / 2;
					int			c = ItemPointerCompare(BarkListGetTID(itup, mid),
													   tid);

					if (c == 0)
						return true;
					if (c < 0)
						lo = mid + 1;
					else
						hi = mid;
				}
				return false;
			}
		case BARK_SHAPE_POSTING:
			{
				Sbm		   *map = bark_posting_open(itup);
				bool		found = sbm_contains(map, bark_tid_to_key(tid), NULL);

				sbm_free(map);
				return found;
			}
		default:
			{
				ItemPointerData lo;
				ItemPointerData hi;

				bark_entry_tid_range(itup, &lo, &hi);
				return ItemPointerEquals(&lo, tid);
			}
	}
}

/*
 * The smallest leaf entry of `key` (a key part, see bark_entry_key_part)
 * holding the ascending heap TIDs tids[0..n): a SINGLE for one, else the
 * smaller of POSTING and LIST.  NULL when it would exceed BarkMaxItemSize.
 */
static IndexTuple
bark_form_entry(IndexTuple key, ItemPointer tids, int n)
{
	IndexTuple	entry;

	if (n == 1)
	{
		entry = CopyIndexTuple(key);
		entry->t_tid = tids[0];
		return entry;
	}
	entry = bark_form_posting(NULL, key, tids, n);
	if (entry == NULL && n <= BARK_LIST_MAX_COUNT)
		entry = bark_form_list(NULL, key, tids, n);
	if (entry != NULL && MAXALIGN(IndexTupleSize(entry)) > BarkMaxItemSize)
	{
		pfree(entry);
		entry = NULL;
	}
	return entry;
}

/*
 * Make room for heap TID `tid`, which falls strictly inside the TID range of
 * the LIST or POSTING entry `entry` but cannot be added to it, by dividing
 * entry's members and tid between two entries of its key: *left, which
 * replaces entry, and *right, which goes just after it.  Both are palloc'd.
 * Each side's TIDs are below the other's, so the run stays in heap TID order.
 *
 * The members are divided in the middle.  nbtree's posting-list swap
 * (_bt_swap_posting) instead keeps the list whole with tid in place of its
 * highest member, which becomes a plain tuple; nbtree's deduplication pass
 * later merges such tuples.  BARK has no such pass, and an entry that cannot
 * take tid has usually reached the item ceiling, so every later TID in its
 * range would push out one more member as a SINGLE of its own.  Halves have
 * room for the TIDs that later fall in their ranges.
 *
 * Should a half of a POSTING not fit under BarkMaxItemSize (its sbm encoding
 * is not monotone in the members), the members are cut at tid instead, with
 * tid on either side.  The side without tid is a subset of entry, so it fits
 * (a subset's removal bound is no larger), and the two sides' encodings
 * together are entry's plus a few dozen bytes for tid and the cut, so at
 * least one of those two cuts fits.
 *
 * Like bark_entry_add_tid, this depends only on its arguments, so WAL replay
 * repeats it (XLOG_BARK_INSERT_SWAP).
 */
void
bark_entry_swap_tid(IndexTuple entry, ItemPointer tid, IndexTuple *left,
					IndexTuple *right)
{
	IndexTuple	key = bark_entry_key_part(entry);
	int			n = bark_entry_count_tids(entry);
	ItemPointer tids = palloc_array(ItemPointerData, n + 1);
	int			ins;
	int			cuts[3];

	n = bark_entry_get_tids(entry, tids, n);
	for (ins = 0; ins < n; ins++)
	{
		if (ItemPointerCompare(&tids[ins], tid) >= 0)
			break;
	}
	Assert(ins > 0 && ins < n && !ItemPointerEquals(&tids[ins], tid));
	memmove(&tids[ins + 1], &tids[ins], (n - ins) * sizeof(ItemPointerData));
	tids[ins] = *tid;
	n++;

	/* Members tids[0..cut) go left, the rest right. */
	cuts[0] = n / 2;
	cuts[1] = ins + 1;
	cuts[2] = ins;
	for (int i = 0; i < lengthof(cuts); i++)
	{
		*left = bark_form_entry(key, tids, cuts[i]);
		*right = bark_form_entry(key, tids + cuts[i], n - cuts[i]);
		if (*left != NULL && *right != NULL)
		{
			pfree(tids);
			pfree(key);
			return;
		}
		if (*left)
			pfree(*left);
		if (*right)
			pfree(*right);
	}
	elog(ERROR, "could not divide a BARK entry to add a heap TID");
}

/* ----------------------------------------------------------------------------
 * Leaf prefix compression (see BARK_PREFIX in bark.h for the format)
 * ----------------------------------------------------------------------------
 */

/*
 * Where each part of an entry goes in its coded form.  Coding and decoding
 * both lay the entry out from this, so the two cannot disagree.
 */
typedef struct BarkPrefixPlan
{
	Size		hoff;			/* data offset: header and null bitmap */
	Size		c1;				/* plain first column's size, with header */
	Size		keyend;			/* end of the plain key (the body offset) */
	uint8		code;			/* first payload byte of the coded column */
	const char *suffix;			/* bytes stored after it */
	Size		suffixlen;
	Size		vsize;			/* coded column's size, with header */
	Size		codedkeyend;	/* end of the coded key */
	Size		total;			/* size of the coded entry */
} BarkPrefixPlan;

/*
 * Is `itup` stored coded on a BARK_PREFIX page?  Only a leaf data entry
 * whose first column is not NULL; an OVERSIZED entry keeps its key out of
 * line and is stored as it is.
 */
static bool
bark_prefix_codable(IndexTuple itup)
{
	BarkEntryShape shape = BarkEntryGetShape(itup);

	if (shape != BARK_SHAPE_SINGLE && shape != BARK_SHAPE_LIST &&
		shape != BARK_SHAPE_POSTING)
		return false;
	return !IndexTupleHasNulls(itup) ||
		!att_isnull(0, (uint8 *) itup + sizeof(IndexTupleData));
}

/*
 * Does the varlena `val` have the header decoding would give it back: the
 * 1-byte header if its data fit one, else an uncompressed 4-byte header?
 */
static bool
bark_prefix_canonical(const char *val)
{
	if (VARATT_IS_SHORT(val))
		return !VARATT_IS_EXTERNAL(val);
	return !VARATT_IS_EXTENDED(val) &&
		VARSIZE(val) - VARHDRSZ + VARHDRSZ_SHORT > VARATT_SHORT_MAX;
}

/* End of a leaf entry's key: the body offset of a LIST or POSTING. */
static Size
bark_entry_keyend(IndexTuple itup)
{
	if (BarkEntryGetShape(itup) == BARK_SHAPE_SINGLE)
		return IndexTupleSize(itup);
	return BarkEntryGetBodyOffset(itup);
}

/*
 * The size of a coded first column whose payload is `payload` bytes: a
 * varlena with a 1-byte header when it fits one.
 */
static inline Size
bark_prefix_vsize(Size payload)
{
	if (payload + VARHDRSZ_SHORT <= VARATT_SHORT_MAX)
		return payload + VARHDRSZ_SHORT;
	return payload + VARHDRSZ;
}

/*
 * Offset at which the bytes after the first column go in a coded entry
 * whose coded column ends at `vend`, when in the plain entry they start at
 * `restorig`: the first offset at or after `vend` that is the same distance
 * past a MAXALIGN boundary.
 */
static inline Size
bark_prefix_restoff(Size vend, Size restorig)
{
	return vend + (MAXIMUM_ALIGNOF + restorig % MAXIMUM_ALIGNOF -
				   vend % MAXIMUM_ALIGNOF) % MAXIMUM_ALIGNOF;
}

/* Plan the coding of the codable entry `itup` against `prefix`. */
static void
bark_prefix_plan(const char *prefix, Size prefixlen, IndexTuple itup,
				 BarkPrefixPlan *plan)
{
	const char *val;
	Size		restorig;
	bool		storerest;

	plan->hoff = IndexInfoFindDataOffset(itup->t_info);
	val = (const char *) itup + plan->hoff;
	plan->c1 = VARSIZE_ANY(val);
	plan->keyend = bark_entry_keyend(itup);

	if (bark_prefix_canonical(val))
	{
		const char *data = VARDATA_ANY(val);
		Size		datalen = VARSIZE_ANY_EXHDR(val);
		Size		limit = Min(prefixlen, datalen);
		Size		shared = 0;

		while (shared < limit && prefix[shared] == data[shared])
			shared++;
		plan->code = (uint8) shared;
		plan->suffix = data + shared;
		plan->suffixlen = datalen - shared;
	}
	else
	{
		plan->code = BARK_PREFIX_RAW;
		plan->suffix = val;
		plan->suffixlen = plan->c1;
	}
	plan->vsize = bark_prefix_vsize(1 + plan->suffixlen);

	/*
	 * The bytes after the first column can be left out when they are just
	 * the zero padding that ends the key.
	 */
	restorig = plan->hoff + plan->c1;
	storerest = plan->keyend != MAXALIGN(restorig);
	for (Size i = restorig; !storerest && i < plan->keyend; i++)
		storerest = ((const char *) itup)[i] != 0;

	if (storerest)
	{
		plan->code |= BARK_PREFIX_REST;
		plan->codedkeyend = bark_prefix_restoff(plan->hoff + plan->vsize,
												restorig) +
			(plan->keyend - restorig);
	}
	else
		plan->codedkeyend = MAXALIGN(plan->hoff + plan->vsize);
	plan->total = plan->codedkeyend + (IndexTupleSize(itup) - plan->keyend);
}

/*
 * Copy n bytes, as memcpy.  Decoding moves a few short runs of bytes per
 * entry, and gcc expands a memcpy of a variable length into an aligned
 * buffer into a string move, whose startup cost is most of a scan's
 * decoding time; eight bytes at a time is several times faster for runs
 * this short.
 */
static inline void
bark_prefix_copy(char *dst, const char *src, Size n)
{
	while (n >= sizeof(uint64))
	{
		memcpy(dst, src, sizeof(uint64));
		dst += sizeof(uint64);
		src += sizeof(uint64);
		n -= sizeof(uint64);
	}
	while (n-- > 0)
		*dst++ = *src++;
}

/* The prefix of a BARK_PREFIX page, and its length in *len. */
const char *
bark_page_get_prefix(Page page, Size *len)
{
	BarkPageOpaque opaque = BarkPageGetOpaque(page);
	IndexTuple	item;

	Assert(BarkPageHasPrefix(opaque));
	item = (IndexTuple) PageGetItem(page,
									PageGetItemId(page,
												  BarkPagePrefixOff(opaque)));
	*len = IndexTupleSize(item) - sizeof(IndexTupleData);
	return (const char *) item + sizeof(IndexTupleData);
}

/*
 * Give `page`, a leaf holding nothing yet but its high key (if it has one),
 * the prefix `prefix` of `len` bytes: add the PREFIX item and set
 * BARK_PREFIX.  Its padding comes from the page's free space, which PageInit
 * zeroed, so a page rebuilt in redo is the same.
 */
void
bark_page_set_prefix(Page page, const char *prefix, Size len)
{
	BarkPageOpaque opaque = BarkPageGetOpaque(page);
	union
	{
		IndexTupleData hdr;
		char		data[sizeof(IndexTupleData) + BARK_PREFIX_MAX];
	}			item;

	Assert(BarkPageIsLeaf(opaque) && !BarkPageHasPrefix(opaque));
	Assert(len >= 1 && len <= BARK_PREFIX_MAX);
	Assert(PageGetMaxOffsetNumber(page) == BarkPagePrefixOff(opaque) - 1);

	memset(&item.hdr, 0, sizeof(IndexTupleData));
	item.hdr.t_info = (unsigned short) (sizeof(IndexTupleData) + len);
	memcpy(item.data + sizeof(IndexTupleData), prefix, len);
	if (PageAddItem(page, item.data, sizeof(IndexTupleData) + len,
					BarkPagePrefixOff(opaque), false, false) ==
		InvalidOffsetNumber)
		elog(ERROR, "failed to add prefix item to BARK page");
	opaque->bark_flags |= BARK_PREFIX;
}

/*
 * The coded form of the leaf entry `itup` for `page`, palloc'd; `itup`
 * itself when the page has no prefix or the entry is stored plain.
 */
IndexTuple
bark_prefix_encode(Page page, IndexTuple itup)
{
	const char *prefix;
	Size		prefixlen;
	BarkPrefixPlan plan;
	char	   *coded;
	char	   *v;
	char	   *payload;

	if (!BarkPageHasPrefix(BarkPageGetOpaque(page)) ||
		!bark_prefix_codable(itup))
		return itup;

	prefix = bark_page_get_prefix(page, &prefixlen);
	bark_prefix_plan(prefix, prefixlen, itup, &plan);
	Assert(plan.total <= INDEX_SIZE_MASK);

	coded = palloc0(plan.total);
	memcpy(coded, itup, plan.hoff);
	v = coded + plan.hoff;
	if (plan.vsize <= VARATT_SHORT_MAX)
	{
		SET_VARSIZE_SHORT(v, plan.vsize);
		payload = v + VARHDRSZ_SHORT;
	}
	else
	{
		SET_VARSIZE(v, plan.vsize);
		payload = v + VARHDRSZ;
	}
	payload[0] = (char) plan.code;
	memcpy(payload + 1, plan.suffix, plan.suffixlen);
	if (plan.code & BARK_PREFIX_REST)
	{
		Size		restorig = plan.hoff + plan.c1;

		memcpy(coded + bark_prefix_restoff(plan.hoff + plan.vsize, restorig),
			   (char *) itup + restorig, plan.keyend - restorig);
	}
	memcpy(coded + plan.codedkeyend, (char *) itup + plan.keyend,
		   IndexTupleSize(itup) - plan.keyend);

	((IndexTuple) coded)->t_info =
		(itup->t_info & ~INDEX_SIZE_MASK) | (unsigned short) plan.total;
	if (BarkEntryGetShape(itup) != BARK_SHAPE_SINGLE)
		BarkEntrySetBodyOffset((IndexTuple) coded, (uint16) plan.codedkeyend);

#ifdef USE_ASSERT_CHECKING
	{
		BarkItemBuf check;
		IndexTuple	decoded = bark_prefix_decode(page, (IndexTuple) coded,
												 &check);

		Assert(IndexTupleSize(decoded) == IndexTupleSize(itup) &&
			   memcmp(decoded, itup, IndexTupleSize(itup)) == 0);
	}
#endif

	return (IndexTuple) coded;
}

/* The MAXALIGNed size `itup` takes on `page`, coded if the page codes it. */
Size
bark_coded_size(Page page, IndexTuple itup)
{
	const char *prefix;
	Size		prefixlen;
	BarkPrefixPlan plan;

	if (!BarkPageHasPrefix(BarkPageGetOpaque(page)) ||
		!bark_prefix_codable(itup))
		return MAXALIGN(IndexTupleSize(itup));

	prefix = bark_page_get_prefix(page, &prefixlen);
	bark_prefix_plan(prefix, prefixlen, itup, &plan);
	return MAXALIGN(plan.total);
}

/*
 * Decode the item `itup` of the BARK_PREFIX page `page` into `buf`, and
 * return it; an item stored plain is returned as it is.  The inverse of
 * bark_prefix_encode, byte for byte.  An item that does not decode within
 * its own bounds, or claims more of the prefix than the page has, is
 * reported as corruption.
 */
IndexTuple
bark_prefix_decode(Page page, IndexTuple itup, BarkItemBuf *buf)
{
	const char *prefix;
	Size		prefixlen;
	Size		size = IndexTupleSize(itup);
	Size		hoff;
	Size		codedkeyend;
	const char *v;
	Size		vsize;
	const char *payload;
	Size		paylen;
	uint8		code;
	Size		shared;
	char	   *out = buf->data;
	char	   *val = NULL;
	Size		c1;
	Size		restorig;
	Size		keyend;

	if (!bark_prefix_codable(itup))
		return itup;

	prefix = bark_page_get_prefix(page, &prefixlen);
	hoff = IndexInfoFindDataOffset(itup->t_info);
	codedkeyend = bark_entry_keyend(itup);
	v = (const char *) itup + hoff;
	if (hoff >= codedkeyend || codedkeyend > size ||
		(vsize = VARSIZE_ANY(v)) > codedkeyend - hoff ||
		(paylen = VARSIZE_ANY_EXHDR(v)) < 1)
		goto corrupt;
	payload = VARDATA_ANY(v);
	code = (uint8) payload[0];
	shared = code & BARK_PREFIX_SHARED_MASK;

	bark_prefix_copy(out, (const char *) itup, hoff);
	val = out + hoff;
	if (shared == BARK_PREFIX_RAW)
	{
		c1 = paylen - 1;
		bark_prefix_copy(val, payload + 1, c1);
		if (c1 < VARHDRSZ_SHORT || VARSIZE_ANY(val) != c1)
			goto corrupt;
	}
	else
	{
		Size		datalen = shared + paylen - 1;
		char	   *data;

		if (hoff + VARHDRSZ + datalen > sizeof(BarkItemBuf))
			goto corrupt;
		if (shared > prefixlen)
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg_internal("BARK leaf entry shares %zu bytes of a %zu-byte page prefix",
									 shared, prefixlen)));
		if (datalen + VARHDRSZ_SHORT <= VARATT_SHORT_MAX)
		{
			c1 = datalen + VARHDRSZ_SHORT;
			SET_VARSIZE_SHORT(val, c1);
			data = val + VARHDRSZ_SHORT;
		}
		else
		{
			c1 = datalen + VARHDRSZ;
			SET_VARSIZE(val, c1);
			data = val + VARHDRSZ;
		}
		bark_prefix_copy(data, prefix, shared);
		bark_prefix_copy(data + shared, payload + 1, paylen - 1);
	}

	restorig = hoff + c1;
	if (code & BARK_PREFIX_REST)
	{
		Size		restoff = bark_prefix_restoff(hoff + vsize, restorig);

		if (restoff > codedkeyend)
			goto corrupt;
		keyend = restorig + (codedkeyend - restoff);
		if (keyend + (size - codedkeyend) > sizeof(BarkItemBuf))
			goto corrupt;
		bark_prefix_copy(out + restorig, (const char *) itup + restoff,
						 codedkeyend - restoff);
	}
	else
	{
		keyend = MAXALIGN(restorig);
		if (keyend + (size - codedkeyend) > sizeof(BarkItemBuf))
			goto corrupt;
		memset(out + restorig, 0, keyend - restorig);
	}
	bark_prefix_copy(out + keyend, (const char *) itup + codedkeyend,
					 size - codedkeyend);

	((IndexTuple) out)->t_info = (itup->t_info & ~INDEX_SIZE_MASK) |
		(unsigned short) (keyend + (size - codedkeyend));
	if (BarkEntryGetShape(itup) != BARK_SHAPE_SINGLE)
		BarkEntrySetBodyOffset((IndexTuple) out, (uint16) keyend);
	return (IndexTuple) out;

corrupt:
	ereport(ERROR,
			(errcode(ERRCODE_INDEX_CORRUPTED),
			 errmsg_internal("BARK leaf entry is not a valid prefix-coded entry")));
	return NULL;				/* keep compiler quiet */
}

/*
 * The bytes of `itup`'s first column a page prefix may be taken from, and
 * how many (at most BARK_PREFIX_MAX) in *len; NULL when the entry has none:
 * it is not coded, its value is empty, or it would be stored raw.
 */
const char *
bark_prefix_candidate(IndexTuple itup, Size *len)
{
	const char *val;

	if (!bark_prefix_codable(itup))
		return NULL;
	val = (const char *) itup + IndexInfoFindDataOffset(itup->t_info);
	if (!bark_prefix_canonical(val) || VARSIZE_ANY_EXHDR(val) == 0)
		return NULL;
	*len = Min(VARSIZE_ANY_EXHDR(val), BARK_PREFIX_MAX);
	return VARDATA_ANY(val);
}

/*
 * How many bytes of `prefix` the first column of `itup` would share on a
 * page with that prefix: -1 when the entry would not be coded, 0 when it
 * would be stored raw.
 */
int
bark_prefix_shared(const char *prefix, Size prefixlen, IndexTuple itup)
{
	BarkPrefixPlan plan;

	if (!bark_prefix_codable(itup))
		return -1;
	bark_prefix_plan(prefix, prefixlen, itup, &plan);
	if ((plan.code & BARK_PREFIX_SHARED_MASK) == BARK_PREFIX_RAW)
		return 0;
	return plan.code & BARK_PREFIX_SHARED_MASK;
}

/*
 * May the leaves of `index` have a prefix?  Only when the prefix_compression
 * reloption is on and the first column is of a varlena type.
 */
bool
bark_prefix_enabled(Relation index)
{
	return BarkGetPrefixCompression(index) &&
		TupleDescAttr(RelationGetDescr(index), 0)->attlen == -1;
}

/*
 * Choose the prefix for a leaf about to be built from the `n` entries
 * `items`, in key order: the first entry's first-column bytes, cut to the
 * longest run any later entry shares, provided the entries share at least
 * BARK_PREFIX_MIN_SHARED bytes of it on average.  Returns the prefix's
 * length, with *prefix pointing into items[0], or 0 for no prefix.
 */
Size
bark_prefix_choose(Relation index, IndexTuple *items, int n,
				   const char **prefix)
{
	const char *cand;
	Size		candlen;
	Size		longest = 0;
	Size		total = 0;
	int			ncoded = 0;

	if (!bark_prefix_enabled(index) ||
		(cand = bark_prefix_candidate(items[0], &candlen)) == NULL)
		return 0;
	if (n == 1)
	{
		*prefix = cand;
		return candlen;
	}
	for (int i = 1; i < n; i++)
	{
		int			shared = bark_prefix_shared(cand, candlen, items[i]);

		if (shared < 0)
			continue;
		total += shared;
		ncoded++;
		longest = Max(longest, (Size) shared);
	}
	if (longest == 0 || total < (Size) BARK_PREFIX_MIN_SHARED * ncoded)
		return 0;
	*prefix = cand;
	return longest;
}

/* ----------------------------------------------------------------------------
 * OVERSIZED entry construction and overflow-chain I/O
 *
 * nbtree errors on a key larger than ~1/3 page, and index_form_tuple itself
 * caps a formed tuple at INDEX_SIZE_MASK (8191) bytes.  BARK lifts both caps:
 * an oversized key (or key + INCLUDE payload) is formed in full by
 * bark_form_full_tuple (no cap), stored out-of-line in BarkOverflowChunkSize
 * pieces across a right-linked chain of BARK_OVERFLOW pages, and represented on
 * the leaf (or internal) page by a small fixed-size OVERSIZED entry that keeps
 * only a BarkOverflowRef and the first chunk's block.  The chunks concatenate,
 * in chain order, back to the exact bytes of the full tuple.
 *
 * A full tuple that happens to fit inline (<= BarkMaxItemSize) comes out of
 * bark_form_full_tuple byte-identical to index_form_tuple, so a normal-size key
 * takes exactly the same page-resident path it did before this capability --
 * no overflow indirection unless the entry genuinely exceeds the ceiling.
 * ----------------------------------------------------------------------------
 */

/*
 * The raw-byte data area of an overflow page begins right after the standard
 * page header; the BARK page opaque sits in the special space at the end.
 */
#define BarkOverflowPageData(page) \
	((char *) (page) + MAXALIGN(SizeOfPageHeaderData))

/*
 * Form the complete index tuple for `values`/`isnull`, without the
 * index_form_tuple 8191-byte ceiling, writing the true byte length to
 * *fulllen.  This mirrors index_form_tuple_context (including in-line TOAST
 * compression and external detoasting) but omits the final size-fits-in-t_info
 * check; for an oversized result the t_info size field wraps and must not be
 * read -- the caller uses *fulllen.  A result that fits inline is identical to
 * what index_form_tuple would return.
 */
IndexTuple
bark_form_full_tuple(TupleDesc tupleDescriptor, const Datum *values,
					 const bool *isnull, Size *fulllen)
{
	char	   *tp;
	IndexTuple	tuple;
	Size		size,
				data_size,
				hoff;
	int			i;
	unsigned short infomask = 0;
	bool		hasnull = false;
	uint16		tupmask = 0;
	int			numberOfAttributes = tupleDescriptor->natts;
	Datum		untoasted_values[INDEX_MAX_KEYS] = {0};
	bool		untoasted_free[INDEX_MAX_KEYS] = {0};

	if (numberOfAttributes > INDEX_MAX_KEYS)
		ereport(ERROR,
				(errcode(ERRCODE_TOO_MANY_COLUMNS),
				 errmsg("number of index columns (%d) exceeds limit (%d)",
						numberOfAttributes, INDEX_MAX_KEYS)));

	/* Detoast external, and compress compressible varlenas in-line. */
	for (i = 0; i < numberOfAttributes; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupleDescriptor, i);

		untoasted_values[i] = values[i];
		untoasted_free[i] = false;

		if (isnull[i] || att->attlen != -1)
			continue;

		if (VARATT_IS_EXTERNAL(DatumGetPointer(values[i])))
		{
			untoasted_values[i] =
				PointerGetDatum(detoast_external_attr((varlena *)
													  DatumGetPointer(values[i])));
			untoasted_free[i] = true;
		}

		if (!VARATT_IS_EXTENDED(DatumGetPointer(untoasted_values[i])) &&
			VARSIZE(DatumGetPointer(untoasted_values[i])) > TOAST_INDEX_TARGET &&
			(att->attstorage == TYPSTORAGE_EXTENDED ||
			 att->attstorage == TYPSTORAGE_MAIN))
		{
			Datum		cvalue = toast_compress_datum(untoasted_values[i],
													  att->attcompression);

			if (DatumGetPointer(cvalue) != NULL)
			{
				if (untoasted_free[i])
					pfree(DatumGetPointer(untoasted_values[i]));
				untoasted_values[i] = cvalue;
				untoasted_free[i] = true;
			}
		}
	}

	for (i = 0; i < numberOfAttributes; i++)
	{
		if (isnull[i])
		{
			hasnull = true;
			break;
		}
	}

	if (hasnull)
		infomask |= INDEX_NULL_MASK;

	hoff = IndexInfoFindDataOffset(infomask);
	data_size = heap_compute_data_size(tupleDescriptor, untoasted_values, isnull);
	size = hoff + data_size;
	size = MAXALIGN(size);

	tp = (char *) palloc0(size);
	tuple = (IndexTuple) tp;

	heap_fill_tuple(tupleDescriptor, untoasted_values, isnull, tp + hoff,
					data_size, &tupmask,
					(hasnull ? (uint8 *) tp + sizeof(IndexTupleData) : NULL));

	for (i = 0; i < numberOfAttributes; i++)
	{
		if (untoasted_free[i])
			pfree(DatumGetPointer(untoasted_values[i]));
	}

	if (tupmask & HEAP_HASVARWIDTH)
		infomask |= INDEX_VAR_MASK;
	Assert((tupmask & HEAP_HASEXTERNAL) == 0);

	/*
	 * Record the size in t_info only when it fits; otherwise leave the size
	 * bits wrapped -- the caller never reads IndexTupleSize for an oversized
	 * tuple, it uses *fulllen.
	 */
	infomask |= (unsigned short) (size & INDEX_SIZE_MASK);
	tuple->t_info = infomask;
	*fulllen = size;
	return tuple;
}

/*
 * True when a leaf entry of `fulllen` bytes cannot sit inline on a page.  As
 * nbtree's BTMaxItemSize does, the limit keeps room under the item ceiling
 * for the heap TID a pivot formed from the entry may have to carry (see
 * bark_truncate_pivot), so a pivot is never oversized when its leaf entry is
 * not.
 */
bool
bark_len_is_oversized(Size fulllen)
{
	return MAXALIGN(fulllen) >
		BarkMaxItemSize - MAXALIGN(sizeof(ItemPointerData));
}

/* Overflow pages a full tuple of `fulllen` bytes occupies (at least one). */
BlockNumber
bark_overflow_nchunks(Size fulllen)
{
	return (BlockNumber) ((fulllen + BarkOverflowChunkSize - 1) /
						  BarkOverflowChunkSize);
}

/*
 * Lay out an overflow page holding `len` bytes of a tuple, `chunk`, linked to
 * `nextblk`.
 */
void
bark_init_overflow_page(Page page, const char *chunk, Size len,
						BlockNumber nextblk)
{
	BarkPageOpaque opaque;

	Assert(len <= BarkOverflowChunkSize);
	PageInit(page, BLCKSZ, sizeof(BarkPageOpaqueData));
	opaque = BarkPageGetOpaque(page);
	opaque->bark_prev = BARK_P_NONE;
	opaque->bark_next = nextblk;	/* chain to the next chunk */
	opaque->bark_level = 0;
	opaque->bark_cycleid = 0;
	opaque->bark_flags = BARK_OVERFLOW;		/* not BARK_LEAF: vacuum skips it */
	opaque->bark_page_id = BARK_PAGE_ID;

	memcpy(BarkOverflowPageData(page), chunk, len);
	/* Advance pd_lower to cover the chunk so the page is not seen as empty. */
	((PageHeader) page)->pd_lower = MAXALIGN(SizeOfPageHeaderData) + len;
}

/*
 * Form the small fixed-size OVERSIZED entry referencing an overflow chain that
 * begins at `firstblk`.  `locator` is the heap TID (leaf) or the downlink block
 * as a TID (pivot); for a pivot pass is_leaf=false and the natts count.
 */
IndexTuple
bark_form_oversized_entry(ItemPointer locator, Size fulllen,
						  BlockNumber firstblk, bool is_leaf, uint16 natts)
{
	Size		bodyoff = MAXALIGN(sizeof(IndexTupleData));
	Size		total = bodyoff + MAXALIGN(sizeof(BarkOverflowRef));
	IndexTuple	entry;
	BarkOverflowRef *ref;

	Assert(bodyoff <= BARK_OFFSET_MASK);
	Assert(total <= INDEX_SIZE_MASK);
	Assert(fulllen <= PG_UINT32_MAX);

	/*
	 * The inline entry stores no attribute data (the whole tuple is in the
	 * overflow chain) and so needs no null bitmap: it is just a bare header
	 * plus the ref.  Key comparison always fetches the full tuple, so the
	 * inline entry is never deformed.
	 */
	entry = (IndexTuple) palloc0(total);
	entry->t_info = INDEX_AM_RESERVED_BIT | (uint16) total;

	/* t_tid: OVERFLOW status in the offset field, first block in the block field. */
	ItemPointerSetOffsetNumber(&entry->t_tid, (OffsetNumber) BARK_IS_OVERFLOW);
	ItemPointerSetBlockNumber(&entry->t_tid, firstblk);

	ref = BarkOverflowGetRef(entry);
	ref->fulllen = (uint32) fulllen;
	ref->locator = *locator;
	ref->natts = is_leaf ? BARK_OVERFLOW_LEAF : natts;
	ItemPointerSetInvalid(&ref->pivottid);
	return entry;
}

/*
 * Populate an OVERSIZED entry's inline comparison prefix from the full tuple it
 * references, when the first key column sorts bytewise (keyinfo->cols[0].
 * bytewise).  The prefix is the leading bytes of the first key column's datum;
 * up to BARK_OVERSIZED_PREFIX_LEN bytes are copied, and prefixcomplete records
 * whether the whole column fit.  A NULL or non-bytewise first column leaves
 * prefixlen 0, so bark_compare_itups always fetches the full tuple for that
 * entry -- the safe default.  `full` is the complete (uncapped) index tuple
 * that was written to the overflow chain.
 */
void
bark_set_oversized_prefix(IndexTuple entry, Relation index, IndexTuple full)
{
	BarkOverflowRef *ref = BarkOverflowGetRef(entry);
	TupleDesc	tupdesc = RelationGetDescr(index);
	Oid			collation = index->rd_indcollation[0];
	Datum		d;
	bool		isnull;
	char	   *data;
	Size		len;

	ref->prefixlen = 0;
	ref->prefixcomplete = false;

	/*
	 * The prefix fast path is sound only when a leading-byte difference in the
	 * first key column decides its order -- a text/bytea-style column under a
	 * C/POSIX collation.  A locale-aware collation can reorder across the
	 * prefix boundary, so leave prefixlen 0 and let comparison fetch the full
	 * value.  (Only the first key column drives the prefix.)
	 */
	if (!OidIsValid(collation) ||
		!pg_newlocale_from_collation(collation)->collate_is_c)
		return;

	d = index_getattr(full, 1, tupdesc, &isnull);
	if (isnull)
		return;

	/*
	 * The prefix must be the value's own leading bytes.  bark_form_full_tuple
	 * may have compressed the datum (index_form_tuple compresses large
	 * varlenas), and compressed bytes do not sort like the value, so leave
	 * such an entry without a prefix.  An external datum cannot occur: index
	 * tuples hold their values inline.
	 */
	if (VARATT_IS_COMPRESSED(DatumGetPointer(d)))
		return;
	Assert(!VARATT_IS_EXTERNAL(DatumGetPointer(d)));

	/* A C-collation text/bytea datum is a varlena: use its data area. */
	data = VARDATA_ANY(DatumGetPointer(d));
	len = VARSIZE_ANY_EXHDR(DatumGetPointer(d));

	if (len <= BARK_OVERSIZED_PREFIX_LEN)
		ref->prefixcomplete = true;
	else
		len = BARK_OVERSIZED_PREFIX_LEN;
	memcpy(ref->prefix, data, len);
	ref->prefixlen = (uint16) len;
}

/*
 * Write `full` (fulllen bytes) across a fresh BARK_OVERFLOW chain via the
 * buffer pool, and return the first block.  heaprel is the index's heap, for
 * bark_get_free_page.  The pages are logged in XLOG_BARK_OVERFLOW records,
 * BARK_OVERFLOW_PER_RECORD to a record, each page with its next link and its
 * slice of the tuple, so redo rebuilds it without reading it.  A long chain
 * takes several records; each page is self-contained, so a crash between
 * them leaves only orphaned, BARK_P_NONE-terminated pages that no entry
 * references (the entry is written after the whole chain).
 */
BlockNumber
bark_write_overflow_chain(Relation index, Relation heaprel, IndexTuple full,
						  Size fulllen)
{
	BlockNumber nchunks = bark_overflow_nchunks(fulllen);
	Buffer	   *bufs = palloc(nchunks * sizeof(Buffer));
	BlockNumber *blks = palloc(nchunks * sizeof(BlockNumber));
	BlockNumber firstblk;

	/* Reserve all chunk blocks first so each page's next-link is known. */
	for (BlockNumber i = 0; i < nchunks; i++)
	{
		bufs[i] = bark_get_free_page(index, heaprel);
		blks[i] = BufferGetBlockNumber(bufs[i]);
	}
	firstblk = blks[0];

	for (BlockNumber i = 0; i < nchunks;)
	{
		BlockNumber first = i;
		BlockNumber last = Min(nchunks, i + BARK_OVERFLOW_PER_RECORD);
		xl_bark_overflow_page links[BARK_OVERFLOW_PER_RECORD];
		XLogRecPtr	recptr;

		START_CRIT_SECTION();
		for (; i < last; i++)
		{
			Size		start = (Size) i * BarkOverflowChunkSize;

			links[i - first].next = (i + 1 < nchunks) ? blks[i + 1] : BARK_P_NONE;
			bark_init_overflow_page(BufferGetPage(bufs[i]),
									(const char *) full + start,
									Min((Size) BarkOverflowChunkSize,
										fulllen - start),
									links[i - first].next);
			MarkBufferDirty(bufs[i]);
		}
		if (RelationNeedsWAL(index))
		{
			XLogBeginInsert();
			for (i = first; i < last; i++)
			{
				Size		start = (Size) i * BarkOverflowChunkSize;

				XLogRegisterBuffer(i - first, bufs[i], REGBUF_WILL_INIT);
				XLogRegisterBufData(i - first, &links[i - first],
									sizeof(xl_bark_overflow_page));
				XLogRegisterBufData(i - first, (const char *) full + start,
									Min((Size) BarkOverflowChunkSize,
										fulllen - start));
			}
			recptr = XLogInsert(RM_BARK_ID, XLOG_BARK_OVERFLOW);
		}
		else
			recptr = XLogGetFakeLSN(index);
		for (i = first; i < last; i++)
			PageSetLSN(BufferGetPage(bufs[i]), recptr);
		END_CRIT_SECTION();
	}

	for (BlockNumber i = 0; i < nchunks; i++)
		UnlockReleaseBuffer(bufs[i]);
	pfree(bufs);
	pfree(blks);
	return firstblk;
}

/*
 * Reconstruct the full index tuple an OVERSIZED entry references by walking its
 * overflow chain and concatenating the chunks.  Returns a palloc'd tuple whose
 * byte length is the entry's recorded fulllen.
 */
IndexTuple
bark_fetch_oversized(Relation index, IndexTuple entry)
{
	BarkOverflowRef *ref = BarkOverflowGetRef(entry);
	Size		fulllen = ref->fulllen;
	BlockNumber blkno = BarkOverflowGetFirstBlock(entry);
	char	   *out = palloc(fulllen);
	Size		got = 0;

	while (blkno != BARK_P_NONE && got < fulllen)
	{
		Buffer		buf = ReadBuffer(index, blkno);
		Page		page;
		BarkPageOpaque opaque;
		Size		len;

		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);
		opaque = BarkPageGetOpaque(page);

		/*
		 * Callers read a chain while holding a lock on the page of the entry
		 * that owns it, and VACUUM frees a chain only after removing that
		 * entry under a cleanup lock, so a chain page is never found freed
		 * (deleted, or reused for something else).  Check anyway: a freed
		 * page's chunk bytes are gone, and copying them would return a wrong
		 * value rather than fail.
		 */
		if (PageIsNew(page) || !BarkPageIsOverflow(opaque))
			elog(ERROR, "BARK overflow chain for an oversized entry reaches block %u, which is not an overflow page",
				 blkno);
		len = Min((Size) BarkOverflowChunkSize, fulllen - got);
		memcpy(out + got, BarkOverflowPageData(page), len);
		got += len;
		blkno = opaque->bark_next;
		UnlockReleaseBuffer(buf);
	}

	if (got != fulllen)
		elog(ERROR, "BARK overflow chain for an oversized entry is truncated: got %zu of %zu bytes",
			 got, fulllen);
	return (IndexTuple) out;
}

/*
 * Free the overflow chain an OVERSIZED entry references: make each page a
 * deleted page (BarkPageSetDeleted).  Called by VACUUM when the owning leaf
 * entry is removed.  Each page is freed under its own XLOG_BARK_MARK_DELETED
 * record; the pages are not in the tree, so there is nothing to unlink and a
 * crash part-way leaves only a chain that no entry references.
 *
 * The pages are not put in the FSM here.  A scan that copied the OVERSIZED
 * entry from the leaf before VACUUM removed it may still be walking the
 * chain, so each page keeps its link to the next chunk and is reused only
 * once its safexid shows that no such scan can remain.  barkvacuumcleanup
 * records the pages in the FSM when BarkPageIsRecyclable allows it, in this
 * VACUUM or a later one.  Returns the number of pages freed.
 */
BlockNumber
bark_free_oversized(Relation index, IndexTuple entry)
{
	BlockNumber blkno = BarkOverflowGetFirstBlock(entry);
	BlockNumber nfreed = 0;

	while (blkno != BARK_P_NONE)
	{
		Buffer		buf = ReadBuffer(index, blkno);
		Page		page;
		BlockNumber nextblk;
		FullTransactionId safexid;
		XLogRecPtr	recptr;

		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
		page = BufferGetPage(buf);
		nextblk = BarkPageGetOpaque(page)->bark_next;
		safexid = ReadNextFullTransactionId();

		/* No ereport(ERROR) until changes are logged */
		START_CRIT_SECTION();

		BarkPageSetDeleted(page, BARK_P_NONE, nextblk, safexid);
		MarkBufferDirty(buf);

		if (RelationNeedsWAL(index))
		{
			xl_bark_mark_deleted xlrec;

			xlrec.next = nextblk;
			xlrec.safexid = safexid;

			XLogBeginInsert();
			XLogRegisterBuffer(0, buf, REGBUF_WILL_INIT);
			XLogRegisterData(&xlrec, SizeOfBarkMarkDeleted);
			recptr = XLogInsert(RM_BARK_ID, XLOG_BARK_MARK_DELETED);
		}
		else
			recptr = XLogGetFakeLSN(index);
		PageSetLSN(page, recptr);

		END_CRIT_SECTION();

		UnlockReleaseBuffer(buf);
		nfreed++;

		blkno = nextblk;
	}
	return nfreed;
}

/* ----------------------------------------------------------------------------
 * KNN (ordered-operator) distance functions
 *
 * These back the `<~>` ordering operators registered in btree's integer_ops
 * family for BARK (see pg_amop.dat and BARK_KNN_STRATEGY).  The distance of a
 * scalar key from the ORDER BY constant is simply |key - const|, returned as
 * float8 so the executor can order and (if ever needed) recheck it with the
 * btree float_ops family.  The subtraction is done in float8 to avoid signed
 * overflow at the extremes of the integer range (e.g. INT64_MIN - INT64_MAX).
 * ----------------------------------------------------------------------------
 */
PG_FUNCTION_INFO_V1(bark_int2_distance);
PG_FUNCTION_INFO_V1(bark_int4_distance);
PG_FUNCTION_INFO_V1(bark_int8_distance);

Datum
bark_int2_distance(PG_FUNCTION_ARGS)
{
	double		a = (double) PG_GETARG_INT16(0);
	double		b = (double) PG_GETARG_INT16(1);

	PG_RETURN_FLOAT8(fabs(a - b));
}

Datum
bark_int4_distance(PG_FUNCTION_ARGS)
{
	double		a = (double) PG_GETARG_INT32(0);
	double		b = (double) PG_GETARG_INT32(1);

	PG_RETURN_FLOAT8(fabs(a - b));
}

Datum
bark_int8_distance(PG_FUNCTION_ARGS)
{
	double		a = (double) PG_GETARG_INT64(0);
	double		b = (double) PG_GETARG_INT64(1);

	PG_RETURN_FLOAT8(fabs(a - b));
}
