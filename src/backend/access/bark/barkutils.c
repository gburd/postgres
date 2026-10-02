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

#include "access/bark.h"
#include "access/itup.h"
#include "catalog/pg_type.h"
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
	}

	return keyinfo;
}

/*
 * Compare two BARK index tuples by their key columns.
 *
 * Returns <0, 0, or >0 as a sorts before, equal to, or after b.  NULLs are
 * ordered per each column's NULLS FIRST/LAST option, and a DESC column
 * inverts the comparator's result -- the same total order the btree AM
 * imposes, which is what lets BARK share btree operator families.
 */
int
bark_compare_itups(BarkKeyInfo *keyinfo, Relation index,
				   IndexTuple a, IndexTuple b)
{
	TupleDesc	tupdesc = RelationGetDescr(index);
	int			na = (BarkEntryGetShape(a) == BARK_SHAPE_PIVOT) ? BarkPivotGetNAtts(a) : keyinfo->nkeys;
	int			nb = (BarkEntryGetShape(b) == BARK_SHAPE_PIVOT) ? BarkPivotGetNAtts(b) : keyinfo->nkeys;
	int			ncmp = Min(na, nb);

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
				return col->nulls_first ? -1 : 1;
			else
				return col->nulls_first ? 1 : -1;
		}

		cmp = DatumGetInt32(FunctionCall2Coll(&col->cmp, col->collation,
											  adatum, bdatum));
		if (cmp != 0)
			return col->reverse ? -cmp : cmp;
	}

	/*
	 * Equal on every shared attribute.  The tuple with fewer key attributes
	 * (a more-truncated pivot) sorts first; equal attribute counts are equal
	 * keys.
	 */
	if (na != nb)
		return (na < nb) ? -1 : 1;
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

/* Number of heap locators a leaf entry holds (SINGLE = 1, LIST = count). */
int
bark_entry_count_tids(IndexTuple itup)
{
	switch (BarkEntryGetShape(itup))
	{
		case BARK_SHAPE_SINGLE:
			return 1;
		case BARK_SHAPE_LIST:
			return BarkListGetCount(itup);
		default:
			elog(ERROR, "BARK leaf entry has unexpected shape %d",
				 (int) BarkEntryGetShape(itup));
			return 0;			/* keep the compiler happy */
	}
}

/*
 * Copy a leaf entry's heap locators, ascending, into `out` (capacity maxout).
 * Returns the number written.  SINGLE yields its single t_tid; LIST yields its
 * stored array verbatim (already ascending).
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
		case BARK_SHAPE_LIST:
			{
				int			n = BarkListGetCount(itup);

				Assert(maxout >= n);
				memcpy(out, BarkListGetTIDArray(itup),
					   n * sizeof(ItemPointerData));
				return n;
			}
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
