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
#include "access/detoast.h"
#include "access/generic_xlog.h"
#include "access/heaptoast.h"
#include "access/htup_details.h"
#include "access/itup.h"
#include "access/toast_internals.h"
#include "catalog/pg_type.h"
#include "lib/sbm.h"
#include "storage/bufmgr.h"
#include "storage/indexfsm.h"
#include "utils/lsyscache.h"
#include "utils/pg_locale.h"
#include "utils/rel.h"

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
 * available and extending the relation only otherwise.  Modeled on bloom's
 * BloomNewBuffer: a recycled page may have been grabbed by someone else since
 * the FSM named it, so we take the lock conditionally and re-check that the
 * page really is free (new, or still flagged BARK_DELETED) before handing it
 * back; a page that no longer qualifies is skipped and the FSM is asked again.
 *
 * The returned buffer is pinned and exclusive-locked; its page is left as-is
 * (new or deleted), so the caller's PageInit fully reinitializes it.
 */
Buffer
bark_get_free_page(Relation index)
{
	Buffer		buf;

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
			if (BarkPageIsDeleted(BarkPageGetOpaque(page)))
				return buf;		/* deleted and FSM-recycled: OK */

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
	Sbm		   *map = NULL;
	IndexTuple	entry;

	Assert(ntids >= 1);

	/* Build the sbm from the block-clustered keys. */
	for (int i = 0; i < ntids; i++)
	{
		if (sbm_add_grow(&map, bark_tid_to_key(&tids[i])) == SBM_IDX_MAX)
			elog(ERROR, "sbm_add_grow failed building BARK posting entry");
	}

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
bark_posting_add_tid(TupleDesc tupdesc, IndexTuple key, IndexTuple posting,
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

/* True when `fulllen` bytes cannot sit inline on a page under the item cap. */
bool
bark_len_is_oversized(Size fulllen)
{
	return MAXALIGN(fulllen) > BarkMaxItemSize;
}

/* Overflow pages a full tuple of `fulllen` bytes occupies (at least one). */
BlockNumber
bark_overflow_nchunks(Size fulllen)
{
	return (BlockNumber) ((fulllen + BarkOverflowChunkSize - 1) /
						  BarkOverflowChunkSize);
}

/*
 * Lay out the `which`'th overflow chunk of a tuple of `fulllen` bytes into
 * `page`, copying its slice of `full` and linking it to `nextblk`.
 */
void
bark_init_overflow_page(Page page, const char *full, Size fulllen,
						BlockNumber which, BlockNumber nextblk)
{
	BarkPageOpaque opaque;
	Size		start = (Size) which * BarkOverflowChunkSize;
	Size		len = Min((Size) BarkOverflowChunkSize, fulllen - start);

	PageInit(page, BLCKSZ, sizeof(BarkPageOpaqueData));
	opaque = BarkPageGetOpaque(page);
	opaque->bark_prev = BARK_P_NONE;
	opaque->bark_next = nextblk;	/* chain to the next chunk */
	opaque->bark_level = 0;
	opaque->bark_cycleid = 0;
	opaque->bark_flags = BARK_OVERFLOW;		/* not BARK_LEAF: vacuum skips it */
	opaque->bark_page_id = BARK_PAGE_ID;

	memcpy(BarkOverflowPageData(page), full + start, len);
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
 * buffer pool, each page WAL-logged with generic WAL, and return the first
 * block.  Generic WAL covers at most MAX_GENERIC_XLOG_PAGES buffers per record,
 * so a long chain is written in several records; each page is self-contained
 * (it carries its own next-link and chunk bytes), so a crash between records
 * leaves only orphaned, BARK_P_NONE-terminated pages that no entry references.
 */
BlockNumber
bark_write_overflow_chain(Relation index, IndexTuple full, Size fulllen)
{
	BlockNumber nchunks = bark_overflow_nchunks(fulllen);
	Buffer	   *bufs = palloc(nchunks * sizeof(Buffer));
	BlockNumber *blks = palloc(nchunks * sizeof(BlockNumber));
	BlockNumber firstblk;

	/* Reserve all chunk blocks first so each page's next-link is known. */
	for (BlockNumber i = 0; i < nchunks; i++)
	{
		bufs[i] = bark_get_free_page(index);
		blks[i] = BufferGetBlockNumber(bufs[i]);
	}
	firstblk = blks[0];

	/* Write the chunks, up to MAX_GENERIC_XLOG_PAGES per generic-WAL record. */
	for (BlockNumber i = 0; i < nchunks;)
	{
		GenericXLogState *gstate = GenericXLogStart(index);
		int			nin = 0;

		for (; i < nchunks && nin < MAX_GENERIC_XLOG_PAGES; i++, nin++)
		{
			Page		page = GenericXLogRegisterBuffer(gstate, bufs[i],
														 GENERIC_XLOG_FULL_IMAGE);
			BlockNumber nextblk = (i + 1 < nchunks) ? blks[i + 1] : BARK_P_NONE;

			bark_init_overflow_page(page, (const char *) full, fulllen,
									i, nextblk);
		}
		GenericXLogFinish(gstate);
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
 * Free the overflow chain an OVERSIZED entry references: mark each page deleted,
 * unlink it, and record it in the FSM so a later overflow write or split reuses
 * it instead of extending the relation.  Called by VACUUM when the owning leaf
 * entry is removed.  WAL-logged under its own generic-WAL records; the FSM
 * record is a hint made durable by the subsequent IndexFreeSpaceMapVacuum in
 * barkvacuumcleanup.
 *
 * An overflow page carries no sibling/parent references other than its own
 * forward chain link (which we clear here), so it is safe to recycle the moment
 * the leaf entry that owned the chain is gone -- no half-dead protocol needed.
 */
void
bark_free_oversized(Relation index, IndexTuple entry)
{
	BlockNumber blkno = BarkOverflowGetFirstBlock(entry);

	while (blkno != BARK_P_NONE)
	{
		Buffer		buf = ReadBuffer(index, blkno);
		Page		page;
		BarkPageOpaque opaque;
		BlockNumber nextblk;
		GenericXLogState *gstate;
		Page		p;

		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
		page = BufferGetPage(buf);
		opaque = BarkPageGetOpaque(page);
		nextblk = opaque->bark_next;

		gstate = GenericXLogStart(index);
		p = GenericXLogRegisterBuffer(gstate, buf, 0);
		BarkPageGetOpaque(p)->bark_flags |= BARK_DELETED;
		BarkPageGetOpaque(p)->bark_next = BARK_P_NONE;
		GenericXLogFinish(gstate);

		UnlockReleaseBuffer(buf);

		/* Make the now-deleted page available for reuse. */
		RecordFreeIndexPage(index, blkno);

		blkno = nextblk;
	}
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
