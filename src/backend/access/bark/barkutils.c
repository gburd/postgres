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
#include "access/detoast.h"
#include "access/generic_xlog.h"
#include "access/heaptoast.h"
#include "access/htup_details.h"
#include "access/itup.h"
#include "access/toast_internals.h"
#include "catalog/pg_type.h"
#include "lib/sbm.h"
#include "storage/bufmgr.h"
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
 *
 * An OVERSIZED entry carries no key attributes inline (its full tuple lives on
 * an overflow chain), so when either operand is OVERSIZED we fetch the full
 * tuple(s) and compare those.  This is what makes out-of-line storage
 * transparent: two keys that share a long prefix still order on their full
 * value, because the comparison always sees the whole key.  (The caller must
 * pass a non-NULL `index` so the overflow chain can be read; every caller does.
 * ponytail: no inline prefix shortcut yet -- an OVERSIZED compare always reads
 * the chain.  A length/prefix fast path that resolves most compares without a
 * fetch is a later optimization, not a correctness matter.)
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
 * Build a POSTING entry from a SINGLE-shape key tuple and `ntids` ascending,
 * distinct locators.  The locator set is serialized with sbm into the entry's
 * body.  Returns NULL when the serialized form is not smaller than the
 * equivalent LIST (the caller then keeps the LIST), so POSTING is used only
 * when it actually saves space.
 */
IndexTuple
bark_form_posting(TupleDesc tupdesc, IndexTuple key, ItemPointer tids, int ntids)
{
	Size		keysz = IndexTupleSize(key);
	Size		bodyoff = MAXALIGN(keysz);
	Size		listsz;
	Size		serialized;
	Size		total;
	Sbm		   *map = NULL;
	uint8	   *out;
	IndexTuple	entry;

	Assert(ntids >= 1);

	/* Build the sbm from the block-clustered keys. */
	for (int i = 0; i < ntids; i++)
	{
		if (sbm_add_grow(&map, bark_tid_to_key(&tids[i])) == SBM_IDX_MAX)
			elog(ERROR, "sbm_add_grow failed building BARK posting entry");
	}
	map = sbm_shrink_to_fit(map);

	serialized = sbm_serialized_size(map);
	total = bodyoff + MAXALIGN(serialized);
	listsz = MAXALIGN(bodyoff + ntids * sizeof(ItemPointerData));

	/* Only worth it when the serialized set is smaller than the LIST form. */
	if (MAXALIGN(total) >= listsz)
	{
		sbm_free(map);
		return NULL;
	}

	Assert(bodyoff <= BARK_OFFSET_MASK);
	Assert(total <= INDEX_SIZE_MASK);

	entry = (IndexTuple) palloc0(total);
	memcpy(entry, key, keysz);
	entry->t_info = (key->t_info & ~INDEX_SIZE_MASK) | (uint16) total;
	entry->t_info |= INDEX_AM_RESERVED_BIT;
	ItemPointerSetOffsetNumber(&entry->t_tid, (OffsetNumber) BARK_IS_POSTING);
	BarkEntrySetBodyOffset(entry, (uint16) bodyoff);

	out = BarkPostingGetData(entry);
	if (sbm_serialize(map, out, serialized) != serialized)
		elog(ERROR, "sbm_serialize wrote unexpected length for BARK posting entry");
	sbm_free(map);
	return entry;
}

/* Open a POSTING entry's serialized sbm (caller must sbm_free the result). */
static Sbm *
bark_posting_open(IndexTuple itup)
{
	Sbm		   *map = sbm_deserialize(BarkPostingGetData(itup),
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

/*
 * Read a POSTING entry's locators, ascending, into `out` (capacity maxout).
 * Returns the number written.  sbm iterates in ascending index order, which
 * the block-clustered mapping turns back into ascending TID order.
 */
int
bark_posting_get_tids(IndexTuple itup, ItemPointer out, int maxout)
{
	Sbm		   *map = bark_posting_open(itup);
	SbmCursor	cur = SBM_CURSOR_INIT;
	uint64		idx = SBM_IDX_MAX;
	int			n = 0;

	while ((idx = sbm_next_member(map, idx, &cur)) != SBM_IDX_MAX)
	{
		Assert(n < maxout);
		bark_key_to_tid(idx, &out[n]);
		n++;
	}
	sbm_free(map);
	return n;
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
		bufs[i] = ReadBuffer(index, P_NEW);
		LockBuffer(bufs[i], BUFFER_LOCK_EXCLUSIVE);
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
 * Free the overflow chain an OVERSIZED entry references: mark each page deleted
 * and unlink it.  Called by VACUUM when the owning leaf entry is removed.
 *
 * ponytail: freed overflow pages are flagged BARK_DELETED and left in place
 * (not returned to the FSM or truncated away), exactly as the rest of BARK
 * leaves empty leaves linked today; page recycling is a shared later commit.
 * A reused index therefore does not grow unboundedly across delete/vacuum
 * cycles only once FSM reclamation lands; until then the deleted pages persist
 * but are never read.
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
		blkno = nextblk;
	}
}
