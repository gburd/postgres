/*-------------------------------------------------------------------------
 *
 * barkfuncs.c
 *		Functions to investigate the content of BARK indexes
 *
 * These are the BARK counterparts of btreefuncs.c's bt_metap,
 * bt_page_stats, bt_multi_page_stats and bt_page_items.  BARK's page layout
 * follows nbtree's (sibling links, high keys, pivots), but its leaf entries
 * come in several shapes (SINGLE, LIST, POSTING, OVERSIZED), a leaf may hold
 * a PREFIX item whose prefix the other entries are coded against, and an
 * overflow page holds a slice of an oversized tuple rather than line
 * pointers; see src/backend/access/bark/README.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		contrib/pageinspect/barkfuncs.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bark.h"
#include "access/htup_details.h"
#include "access/relation.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "pageinspect.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/tuplestore.h"
#include "utils/varlena.h"

PG_FUNCTION_INFO_V1(bark_metap);
PG_FUNCTION_INFO_V1(bark_page_stats);
PG_FUNCTION_INFO_V1(bark_multi_page_stats);
PG_FUNCTION_INFO_V1(bark_page_items);
PG_FUNCTION_INFO_V1(bark_page_items_bytea);

#define BARK_PAGE_STATS_COLS	12
#define BARK_PAGE_ITEMS_COLS	10

/*
 * Does the meta page hold `field`?  pd_lower marks the end of the
 * BarkMetaPageData the page was written with, so a field added to the
 * struct after the index was built reads as absent rather than as whatever
 * the page holds there.
 */
#define BarkMetaHasField(page, field) \
	(((PageHeader) (page))->pd_lower >= \
	 MAXALIGN(SizeOfPageHeaderData) + offsetof(BarkMetaPageData, field) + \
	 sizeof(((BarkMetaPageData *) NULL)->field))

static void
check_superuser(void)
{
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to use pageinspect functions")));
}

/*
 * Open the BARK index named `relname` with AccessShareLock, as
 * btreefuncs.c does for btree.
 */
static Relation
bark_open_index(text *relname)
{
	RangeVar   *relrv;
	Relation	rel;

	relrv = makeRangeVarFromNameList(textToQualifiedNameList(relname));
	rel = relation_openrv(relrv, AccessShareLock);

	if (rel->rd_rel->relkind != RELKIND_INDEX ||
		rel->rd_rel->relam != BARK_AM_OID)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a %s index",
						RelationGetRelationName(rel), "bark")));

	/*
	 * Reject attempts to read non-local temporary relations; we would be
	 * likely to get wrong data since we have no visibility into the owning
	 * session's local buffers.
	 */
	if (RELATION_IS_OTHER_TEMP(rel))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot access temporary tables of other sessions")));

	return rel;
}

/* Verify that a block number (given as int64) is valid for the relation. */
static void
bark_check_block_range(Relation rel, int64 blkno)
{
	if (blkno < 0 || blkno > MaxBlockNumber)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid block number %" PRId64, blkno)));

	if ((BlockNumber) blkno >= RelationGetNumberOfBlocks(rel))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("block number %" PRId64 " is out of range", blkno)));
}

/* Copy block `blkno` of `rel` into palloc'd memory, under a share lock. */
static Page
bark_copy_block(Relation rel, BlockNumber blkno)
{
	Buffer		buffer = ReadBuffer(rel, blkno);
	Page		page = palloc(BLCKSZ);

	LockBuffer(buffer, BUFFER_LOCK_SHARE);
	memcpy(page, BufferGetPage(buffer), BLCKSZ);
	UnlockReleaseBuffer(buffer);
	return page;
}

/*
 * The number of line pointers of a page that has items: none on the meta
 * page, whose pd_lower covers BarkMetaPageData; on an overflow page, whose
 * pd_lower covers a slice of an oversized tuple; or on a deleted page.
 */
static OffsetNumber
bark_page_maxoff(Page page)
{
	BarkPageOpaque opaque = BarkPageGetOpaque(page);

	if ((opaque->bark_flags & (BARK_META | BARK_OVERFLOW | BARK_DELETED)) != 0)
		return InvalidOffsetNumber;
	return PageGetMaxOffsetNumber(page);
}

/* ------------------------------------------------
 * bark_metap()
 *
 * Get a BARK index's meta-page information
 *
 * Usage: SELECT * FROM bark_metap('t1_idx')
 * ------------------------------------------------
 */
Datum
bark_metap(PG_FUNCTION_ARGS)
{
	text	   *relname = PG_GETARG_TEXT_PP(0);
	Relation	rel;
	Page		page;
	BarkMetaPageData *meta;
	TupleDesc	tupleDesc;
	Datum		values[7];
	bool		nulls[7] = {0};
	int			j = 0;

	check_superuser();

	rel = bark_open_index(relname);
	page = bark_copy_block(rel, BARK_METAPAGE);
	relation_close(rel, AccessShareLock);

	if (get_call_result_type(fcinfo, NULL, &tupleDesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");
	tupleDesc = BlessTupleDesc(tupleDesc);

	meta = BarkPageGetMeta(page);
	values[j++] = Int32GetDatum((int32) meta->bark_magic);
	values[j++] = Int32GetDatum((int32) meta->bark_version);
	values[j++] = Int64GetDatum(meta->bark_root);
	values[j++] = Int64GetDatum(meta->bark_level);

	/* Fields after the first four are read only when the page has them. */
	if (BarkMetaHasField(page, bark_allequalimage))
		values[j] = BoolGetDatum(meta->bark_allequalimage);
	else
		nulls[j] = true;
	j++;

	/* An index whose meta page predates these has neither: 0. */
	values[j++] = Int64GetDatum(BarkMetaHasField(page, bark_flags) ?
								meta->bark_flags : 0);
	values[j++] = Int64GetDatum(BarkMetaHasField(page, bark_nkeys) ?
								(int64) meta->bark_nkeys : 0);

	Assert(j == tupleDesc->natts);
	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupleDesc, values,
													  nulls)));
}

/*
 * The role of a page, as the "type" column reports it.  As in
 * bt_page_stats, a leaf that is also the root is a leaf; the flags column
 * says it is the root.
 */
static const char *
bark_page_type(BarkPageOpaque opaque)
{
	if (opaque->bark_flags & BARK_DELETED)
		return "deleted";
	if (opaque->bark_flags & BARK_HALF_DEAD)
		return "half-dead";
	if (opaque->bark_flags & BARK_META)
		return "meta";
	if (opaque->bark_flags & BARK_OVERFLOW)
		return "overflow";
	if (opaque->bark_flags & BARK_LEAF)
		return "leaf";
	if (opaque->bark_flags & BARK_ROOT)
		return "root";
	return "internal";
}

/* bark_flags as an array of names; bits without a name are shown in hex. */
static Datum
bark_page_flags(uint16 flagbits)
{
	static const struct
	{
		uint16		bit;
		const char *name;
	}			names[] =
	{
		{BARK_LEAF, "leaf"},
		{BARK_ROOT, "root"},
		{BARK_DELETED, "deleted"},
		{BARK_META, "meta"},
		{BARK_HALF_DEAD, "half_dead"},
		{BARK_INCOMPLETE_SPLIT, "incomplete_split"},
		{BARK_HAS_GARBAGE, "has_garbage"},
		{BARK_OVERFLOW, "overflow"},
		{BARK_PREFIX, "prefix"},
	};
	Datum		flags[lengthof(names) + 1];
	int			nflags = 0;

	for (int i = 0; i < lengthof(names); i++)
	{
		if (flagbits & names[i].bit)
		{
			flags[nflags++] = CStringGetTextDatum(names[i].name);
			flagbits &= ~names[i].bit;
		}
	}
	if (flagbits)
		flags[nflags++] = DirectFunctionCall1(to_hex32, Int32GetDatum(flagbits));

	return PointerGetDatum(construct_array_builtin(flags, nflags, TEXTOID));
}

/*
 * Fill one row of bark_page_stats for block `blkno` of `rel`.  Unlike
 * bt_page_stats, the meta page is accepted: it reports type "meta" and no
 * items.  A page that was allocated but never initialized reports type
 * "new".
 */
static void
bark_page_stats_row(Relation rel, BlockNumber blkno, Datum *values,
					bool *nulls)
{
	Page		page = bark_copy_block(rel, blkno);
	OffsetNumber maxoff;
	uint32		live_items = 0;
	uint32		dead_items = 0;
	uint64		item_size = 0;
	int			j = 0;

	memset(nulls, 0, BARK_PAGE_STATS_COLS * sizeof(bool));

	values[j++] = Int64GetDatum(blkno);
	if (PageIsNew(page))
	{
		values[j++] = CStringGetTextDatum("new");
		for (; j < BARK_PAGE_STATS_COLS; j++)
			nulls[j] = true;
		pfree(page);
		return;
	}

	values[j++] = CStringGetTextDatum(bark_page_type(BarkPageGetOpaque(page)));

	maxoff = bark_page_maxoff(page);
	for (OffsetNumber off = FirstOffsetNumber; off <= maxoff; off++)
	{
		ItemId		id = PageGetItemId(page, off);

		item_size += ItemIdGetLength(id);
		if (ItemIdIsDead(id))
			dead_items++;
		else
			live_items++;
	}

	values[j++] = Int32GetDatum(live_items);
	values[j++] = Int32GetDatum(dead_items);
	values[j++] = Int32GetDatum(live_items + dead_items > 0 ?
								item_size / (live_items + dead_items) : 0);
	values[j++] = Int32GetDatum(PageGetPageSize(page));
	values[j++] = Int32GetDatum(PageGetFreeSpace(page));
	values[j++] = Int64GetDatum(BarkPageGetOpaque(page)->bark_prev);
	values[j++] = Int64GetDatum(BarkPageGetOpaque(page)->bark_next);
	values[j++] = Int64GetDatum(BarkPageGetOpaque(page)->bark_level);
	values[j++] = Int32GetDatum(BarkPageGetOpaque(page)->bark_cycleid);
	values[j++] = bark_page_flags(BarkPageGetOpaque(page)->bark_flags);
	Assert(j == BARK_PAGE_STATS_COLS);

	pfree(page);
}

/* -----------------------------------------------
 * bark_page_stats()
 *
 * Usage: SELECT * FROM bark_page_stats('t1_idx', 1);
 * -----------------------------------------------
 */
Datum
bark_page_stats(PG_FUNCTION_ARGS)
{
	text	   *relname = PG_GETARG_TEXT_PP(0);
	int64		blkno = PG_GETARG_INT64(1);
	Relation	rel;
	TupleDesc	tupleDesc;
	Datum		values[BARK_PAGE_STATS_COLS];
	bool		nulls[BARK_PAGE_STATS_COLS];

	check_superuser();

	rel = bark_open_index(relname);
	bark_check_block_range(rel, blkno);
	bark_page_stats_row(rel, (BlockNumber) blkno, values, nulls);
	relation_close(rel, AccessShareLock);

	if (get_call_result_type(fcinfo, NULL, &tupleDesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");
	tupleDesc = BlessTupleDesc(tupleDesc);

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupleDesc, values,
													  nulls)));
}

/* -----------------------------------------------
 * bark_multi_page_stats()
 *
 * Usage: SELECT * FROM bark_multi_page_stats('t1_idx', 1, 2);
 * Arguments are index relation name, first block number, number of blocks
 * (but number of blocks can be negative to mean "read all the rest")
 * -----------------------------------------------
 */
Datum
bark_multi_page_stats(PG_FUNCTION_ARGS)
{
	text	   *relname = PG_GETARG_TEXT_PP(0);
	int64		blkno = PG_GETARG_INT64(1);
	int64		blk_count = PG_GETARG_INT64(2);
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Relation	rel;

	check_superuser();

	InitMaterializedSRF(fcinfo, 0);

	rel = bark_open_index(relname);
	bark_check_block_range(rel, blkno);

	/*
	 * As in bt_multi_page_stats, a negative count means every block from
	 * blkno on, and a range that runs past the end is an error.
	 */
	if (blk_count < 0)
		blk_count = RelationGetNumberOfBlocks(rel) - blkno;
	else if (blk_count > 1)
		bark_check_block_range(rel, blkno + blk_count - 1);

	for (int64 i = 0; i < blk_count; i++)
	{
		Datum		values[BARK_PAGE_STATS_COLS];
		bool		nulls[BARK_PAGE_STATS_COLS];

		CHECK_FOR_INTERRUPTS();
		bark_page_stats_row(rel, (BlockNumber) (blkno + i), values, nulls);
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	relation_close(rel, AccessShareLock);
	return (Datum) 0;
}

static const char *
bark_shape_name(BarkEntryShape shape)
{
	switch (shape)
	{
		case BARK_SHAPE_SINGLE:
			return "SINGLE";
		case BARK_SHAPE_LIST:
			return "LIST";
		case BARK_SHAPE_POSTING:
			return "POSTING";
		case BARK_SHAPE_OVERSIZED:
			return "OVERSIZED";
		case BARK_SHAPE_PIVOT:
			return "PIVOT";
	}
	return "?";					/* keep the compiler quiet */
}

/* `len` bytes at `ptr` as space-separated hex, as bt_page_items shows data. */
static Datum
bark_hex_bytes(const char *ptr, int len)
{
	char	   *dump = palloc0(len * 3 + 1);
	char	   *p = dump;
	Datum		result;

	for (int off = 0; off < len; off++)
	{
		if (off > 0)
			*p++ = ' ';
		sprintf(p, "%02x", ptr[off] & 0xff);
		p += 2;
	}
	result = CStringGetTextDatum(dump);
	pfree(dump);
	return result;
}

/*
 * A page passed as bytea can hold anything, so check that the parts of an
 * entry the item listing reads lie within the entry before reading them.
 */
static void
bark_check_entry_bounds(IndexTuple itup, BarkEntryShape shape, OffsetNumber off)
{
	Size		size = IndexTupleSize(itup);
	Size		dataoff = IndexInfoFindDataOffset(itup->t_info);
	bool		ok = dataoff <= size;

	switch (shape)
	{
		case BARK_SHAPE_LIST:
			ok = ok && BarkEntryGetBodyOffset(itup) >= dataoff &&
				BarkEntryGetBodyOffset(itup) +
				BarkListGetCount(itup) * sizeof(ItemPointerData) <= size;
			break;
		case BARK_SHAPE_POSTING:
			ok = ok && BarkEntryGetBodyOffset(itup) >= dataoff &&
				BarkPostingDataFits(itup);
			break;
		case BARK_SHAPE_OVERSIZED:
			ok = ok && MAXALIGN(dataoff) + sizeof(BarkOverflowRef) <= size;
			break;
		case BARK_SHAPE_PIVOT:
			ok = ok && (BarkPivotGetHeapTID(itup) == NULL ||
						dataoff + MAXALIGN(sizeof(ItemPointerData)) <= size);
			break;
		case BARK_SHAPE_SINGLE:
			break;
	}
	if (!ok)
		elog(ERROR, "invalid %s entry at offset number %u",
			 bark_shape_name(shape), off);
}

/*
 * Check that `page` is a BARK page whose items can be listed, as
 * bt_page_items_bytea checks a btree page, and return how many items to
 * list.  A deleted or overflow page has none, with a NOTICE.
 */
static OffsetNumber
bark_items_page_check(Page page)
{
	BarkPageOpaque opaque;

	if (PageGetSpecialSize(page) != MAXALIGN(sizeof(BarkPageOpaqueData)))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("input page is not a valid %s page", "bark"),
				 errdetail("Expected special size %d, got %d.",
						   (int) MAXALIGN(sizeof(BarkPageOpaqueData)),
						   (int) PageGetSpecialSize(page))));

	opaque = BarkPageGetOpaque(page);
	if (opaque->bark_page_id != BARK_PAGE_ID)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("input page is not a valid %s page", "bark"),
				 errdetail("Expected %08x, got %08x.",
						   BARK_PAGE_ID, opaque->bark_page_id)));

	if (BarkPageIsMeta(opaque))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("block is a meta page")));

	if (BarkPageIsLeaf(opaque) && opaque->bark_level != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("block is not a valid %s leaf page", "bark")));

	if (BarkPageIsDeleted(opaque))
		elog(NOTICE, "page is deleted");
	else if (BarkPageIsOverflow(opaque))
		elog(NOTICE, "page is an overflow page");

	return bark_page_maxoff(page);
}

/*
 * Add one row per item of `page` to the result of the SRF `fcinfo`.
 *
 * itemlen and ctid are the item as stored.  The data column, and the heap
 * TIDs, come from the entry as BarkPageGetItem decodes it, so on a
 * BARK_PREFIX leaf they show the key with its prefix restored.  data holds
 * the key columns only: the TIDs of a LIST or POSTING entry and a pivot's
 * heap TID are left out of it, as bt_page_items leaves out a posting list,
 * and an OVERSIZED entry, whose key is on its overflow chain, has none.
 */
static void
bark_page_items_internal(FunctionCallInfo fcinfo, Page page)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	BarkPageOpaque opaque = BarkPageGetOpaque(page);
	OffsetNumber maxoff = bark_items_page_check(page);
	bool		leaf = BarkPageIsLeaf(opaque);
	BarkItemBuf buf;

	for (OffsetNumber off = FirstOffsetNumber; off <= maxoff; off++)
	{
		ItemId		id = PageGetItemId(page, off);
		IndexTuple	stored;
		IndexTuple	itup;
		Datum		values[BARK_PAGE_ITEMS_COLS];
		bool		nulls[BARK_PAGE_ITEMS_COLS];
		bool		hikey = !BarkPageRightmost(opaque) && off == BARK_P_HIKEY;
		BarkEntryShape shape;
		ItemPointer htid = NULL;
		int			ntids = 0;
		ItemPointer tids = NULL;
		int			dataoff;
		int			dlen = -1;
		int			j = 0;

		CHECK_FOR_INTERRUPTS();

		if (!ItemIdHasStorage(id) ||
			ItemIdGetOffset(id) + ItemIdGetLength(id) > BLCKSZ)
			elog(ERROR, "invalid ItemId at offset number %u", off);
		stored = (IndexTuple) PageGetItem(page, id);
		if (IndexTupleSize(stored) < sizeof(IndexTupleData) ||
			IndexTupleSize(stored) > ItemIdGetLength(id))
			elog(ERROR, "invalid tuple length %zu for tuple at offset number %u",
				 IndexTupleSize(stored), off);

		memset(nulls, 0, sizeof(nulls));
		values[j++] = Int16GetDatum(off);
		values[j++] = ItemPointerGetDatum(&stored->t_tid);
		values[j++] = Int16GetDatum(IndexTupleSize(stored));

		if (leaf && BarkPageHasPrefix(opaque) && off == BarkPagePrefixOff(opaque))
		{
			/* The PREFIX item: a bare header, then the prefix bytes. */
			values[j++] = CStringGetTextDatum("PREFIX");
			for (; j < BARK_PAGE_ITEMS_COLS - 1; j++)
				nulls[j] = true;
			values[j++] = bark_hex_bytes((char *) stored + sizeof(IndexTupleData),
										 IndexTupleSize(stored) -
										 sizeof(IndexTupleData));
			tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values,
								 nulls);
			continue;
		}

		/* Only a leaf's data items can be prefix-coded. */
		itup = (hikey || !leaf) ? stored : BarkPageGetItem(page, off, &buf);
		shape = BarkEntryGetShape(itup);
		dataoff = IndexInfoFindDataOffset(itup->t_info);
		values[j++] = CStringGetTextDatum(bark_shape_name(shape));
		bark_check_entry_bounds(itup, shape, off);

		switch (shape)
		{
			case BARK_SHAPE_SINGLE:
				ntids = 1;
				htid = &itup->t_tid;
				dlen = IndexTupleSize(itup) - dataoff;
				break;
			case BARK_SHAPE_LIST:
			case BARK_SHAPE_POSTING:
				ntids = bark_entry_count_tids(itup);
				tids = palloc_array(ItemPointerData, Max(ntids, 1));
				ntids = bark_entry_get_tids(itup, tids, ntids);
				htid = ntids > 0 ? &tids[0] : NULL;
				dlen = BarkEntryGetBodyOffset(itup) - dataoff;
				break;
			case BARK_SHAPE_OVERSIZED:
				if (BarkOverflowIsLeaf(itup))
				{
					ntids = 1;
					htid = &BarkOverflowGetRef(itup)->locator;
				}
				else if (ItemPointerIsValid(&BarkOverflowGetRef(itup)->pivottid))
					htid = &BarkOverflowGetRef(itup)->pivottid;
				break;
			case BARK_SHAPE_PIVOT:
				htid = BarkPivotGetHeapTID(itup);
				dlen = IndexTupleSize(itup) - dataoff;
				if (htid)
					dlen -= MAXALIGN(sizeof(ItemPointerData));
				break;
		}

		/* ntids */
		if (ntids > 0)
			values[j] = Int32GetDatum(ntids);
		else
			nulls[j] = true;
		j++;

		/* htid */
		if (htid)
			values[j] = ItemPointerGetDatum(htid);
		else
			nulls[j] = true;
		j++;

		/* tids, for LIST and POSTING only, as bt_page_items for posting lists */
		if (tids)
		{
			Datum	   *tids_datum = palloc_array(Datum, Max(ntids, 1));

			for (int i = 0; i < ntids; i++)
				tids_datum[i] = ItemPointerGetDatum(&tids[i]);
			values[j] = PointerGetDatum(construct_array_builtin(tids_datum,
																ntids, TIDOID));
		}
		else
			nulls[j] = true;
		j++;

		/*
		 * downlink: every pivot on an internal page but its high key.  An
		 * OVERSIZED pivot keeps it in its ref, not in t_tid.
		 */
		if (!leaf && !hikey &&
			(shape == BARK_SHAPE_PIVOT || shape == BARK_SHAPE_OVERSIZED))
			values[j] = Int64GetDatum(BarkEntryGetDownLink(itup));
		else
			nulls[j] = true;
		j++;

		/* overflow_blkno */
		if (shape == BARK_SHAPE_OVERSIZED)
			values[j] = Int64GetDatum(BarkOverflowGetFirstBlock(itup));
		else
			nulls[j] = true;
		j++;

		/* data */
		if (dlen >= 0)
		{
			if (dataoff + dlen > IndexTupleSize(itup))
				elog(ERROR, "invalid tuple length %d for tuple at offset number %u",
					 dlen, off);
			values[j] = bark_hex_bytes((char *) itup + dataoff, dlen);
		}
		else
			nulls[j] = true;
		j++;

		Assert(j == BARK_PAGE_ITEMS_COLS);
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
		if (tids)
			pfree(tids);
	}
}

/*-------------------------------------------------------
 * bark_page_items()
 *
 * Usage: SELECT * FROM bark_page_items('t1_idx', 1);
 *-------------------------------------------------------
 */
Datum
bark_page_items(PG_FUNCTION_ARGS)
{
	text	   *relname = PG_GETARG_TEXT_PP(0);
	int64		blkno = PG_GETARG_INT64(1);
	Relation	rel;
	Page		page;

	check_superuser();

	InitMaterializedSRF(fcinfo, 0);

	rel = bark_open_index(relname);
	if (blkno == BARK_METAPAGE)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("block 0 is a meta page")));
	bark_check_block_range(rel, blkno);

	/*
	 * Work on a copy, as bt_page_items does, so that no buffer pin outlives
	 * the call.
	 */
	page = bark_copy_block(rel, (BlockNumber) blkno);
	relation_close(rel, AccessShareLock);

	if (!PageIsNew(page))
		bark_page_items_internal(fcinfo, page);
	pfree(page);

	return (Datum) 0;
}

/*-------------------------------------------------------
 * bark_page_items_bytea()
 *
 * Usage: SELECT * FROM bark_page_items(get_raw_page('t1_idx', 1));
 *-------------------------------------------------------
 */
Datum
bark_page_items_bytea(PG_FUNCTION_ARGS)
{
	bytea	   *raw_page = PG_GETARG_BYTEA_P(0);
	Page		page;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to use raw page functions")));

	InitMaterializedSRF(fcinfo, 0);

	page = get_page_from_raw(raw_page);
	if (!PageIsNew(page))
		bark_page_items_internal(fcinfo, page);
	pfree(page);

	return (Datum) 0;
}
