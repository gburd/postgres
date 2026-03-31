/*-------------------------------------------------------------------------
 *
 * noxuam_inspect.c
 *	  Debugging functions, for viewing Noxu page contents
 *
 * These should probably be moved to contrib/, but it's handy to have them
 * here during development.
 *
 * Example queries
 * ---------------
 *
 * How many pages of each type a table has?
 *
 * select count(*), pg_nx_page_type('t_noxu', g)
 *   from generate_series(0, pg_table_size('t_noxu') / 8192 - 1) g group by 2;
 *
 *  count | pg_nx_page_type
 * -------+-----------------
 *      1 | META
 *   3701 | BTREE
 *      6 | UNDO
 * (3 rows)
 *
 * Compression ratio of B-tree leaf pages (other pages are not compressed):
 *
 * select sum(uncompressedsz::numeric) / sum(totalsz) as compratio
 *   from pg_nx_btree_pages('t_noxu') ;
 *      compratio
 * --------------------
 *  3.6623829559208134
 * (1 row)
 *
 * Per column compression ratio and number of pages:
 *
 * select attno, count(*), sum(uncompressedsz::numeric) / sum(totalsz) as
 * compratio from pg_nx_btree_pages('t_noxu') group by attno order by
 * attno;
 *
 *  attno | count |       compratio
 * -------+-------+------------------------
 *      0 |   395 | 1.00000000000000000000
 *      1 |    56 |     1.0252948766341260
 *      2 |     3 |    38.7542309420398383
 * (3 rows)
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/noxu/noxuam_inspect.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "miscadmin.h"

#include "access/relscan.h"
#include "access/table.h"
#include "access/noxu_internal.h"
#include "commands/vacuum.h"
#include "funcapi.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/tuplestore.h"

Datum		pg_nx_page_type(PG_FUNCTION_ARGS);
Datum		pg_nx_undo_pages(PG_FUNCTION_ARGS);
Datum		pg_nx_btree_pages(PG_FUNCTION_ARGS);
Datum		pg_nx_overflow_pages(PG_FUNCTION_ARGS);
Datum		pg_nx_meta_page(PG_FUNCTION_ARGS);

Datum
pg_nx_page_type(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	uint64		pageno = PG_GETARG_INT64(1);
	Relation	rel;
	uint16		nx_page_id;
	Buffer		buf;
	Page		page;
	char	   *result;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to use noxu inspection functions"))));

	rel = table_open(relid, AccessShareLock);

	/*
	 * Reject attempts to read non-local temporary relations; we would be
	 * likely to get wrong data since we have no visibility into the owning
	 * session's local buffers.
	 */
	if (RELATION_IS_OTHER_TEMP(rel))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot access temporary tables of other sessions")));

	buf = ReadBuffer(rel, pageno);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);

	nx_page_id = *((uint16 *) ((char *) page + BLCKSZ - sizeof(uint16)));

	UnlockReleaseBuffer(buf);

	table_close(rel, AccessShareLock);

	switch (nx_page_id)
	{
		case NX_META_PAGE_ID:
			result = "META";
			break;
		case NX_BTREE_PAGE_ID:
			result = "BTREE";
			break;
		case NX_UNDO_PAGE_ID:
			result = "UNDO";
			break;
		case NX_OVERFLOW_PAGE_ID:
			result = "OVERFLOW";
			break;
		case NX_FREE_PAGE_ID:
			result = "FREE";
			break;
		case NX_DICT_PAGE_ID:
			result = "DICT";
			break;
		default:
			result = psprintf("UNKNOWN 0x%04x", nx_page_id);
	}

	PG_RETURN_TEXT_P(cstring_to_text(result));
}

/*
 * Deprecated: pg_nx_undo_pages
 *
 * This function previously inspected the bespoke UNDO log pages stored in
 * the main relation fork. UNDO is now managed by the RelUndo subsystem in a
 * separate fork, so this function no longer works.
 *
 * For UNDO inspection, use the RelUndo inspection functions instead.
 *
 *  blkno int8
 *  nrecords int4
 *  freespace int4
 *  firstrecptr int8
 *  lastrecptr int8
 */
Datum
pg_nx_undo_pages(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("pg_nx_undo_pages is deprecated"),
			 errdetail("Noxu now uses the RelUndo subsystem for UNDO management."),
			 errhint("Use RelUndo inspection functions to examine UNDO data.")));

	PG_RETURN_NULL();	/* keep compiler happy */
}

/*
 *  blkno int8
 *  tid int8
 *  total_size int8
 *  prev int8
 *  next int8
 */
Datum
pg_nx_overflow_pages(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Relation	rel;
	BlockNumber blkno;
	BlockNumber nblocks;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to use noxu inspection functions"))));

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));

	/* Switch into long-lived context to construct returned data structures */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	rel = table_open(relid, AccessShareLock);

	/*
	 * Reject attempts to read non-local temporary relations; we would be
	 * likely to get wrong data since we have no visibility into the owning
	 * session's local buffers.
	 */
	if (RELATION_IS_OTHER_TEMP(rel))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot access temporary tables of other sessions")));

	nblocks = RelationGetNumberOfBlocks(rel);

	/* scan all blocks in physical order */
	for (blkno = 1; blkno < nblocks; blkno++)
	{
		Datum		values[6];
		bool		nulls[6];
		Buffer		buf;
		Page		page;
		NXOverflowPageOpaque *opaque;

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		CHECK_FOR_INTERRUPTS();

		/* Read the page */
		buf = ReadBuffer(rel, blkno);
		page = BufferGetPage(buf);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

		/*
		 * We're only interested in overflow pages.
		 */
		if (PageGetSpecialSize(page) != MAXALIGN(sizeof(NXOverflowPageOpaque)))
		{
			UnlockReleaseBuffer(buf);
			continue;
		}
		opaque = (NXOverflowPageOpaque *) PageGetSpecialPointer(page);
		if (opaque->nx_page_id != NX_OVERFLOW_PAGE_ID)
		{
			UnlockReleaseBuffer(buf);
			continue;
		}

		values[0] = Int64GetDatum(blkno);
		if (opaque->nx_tid)
		{
			values[1] = Int64GetDatum(opaque->nx_tid);
			values[2] = Int64GetDatum(opaque->nx_total_size);
		}
		values[3] = Int64GetDatum(opaque->nx_slice_offset);
		values[4] = Int64GetDatum(opaque->nx_prev);
		values[5] = Int64GetDatum(opaque->nx_next);

		UnlockReleaseBuffer(buf);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}
	tuplestore_end(tupstore);

	table_close(rel, AccessShareLock);

	return (Datum) 0;
}


/*
 *  blkno int8
 *  nextblk int8
 *  attno int4
 *  level int4
 *
 *  lokey int8
 *  hikey int8

 *  nitems int4
 *  ncompressed int4
 *  totalsz int4
 *  uncompressedsz int4
 *  freespace int4
 */
Datum
pg_nx_btree_pages(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Relation	rel;
	BlockNumber blkno;
	BlockNumber nblocks;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to use noxu inspection functions"))));

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));

	/* Switch into long-lived context to construct returned data structures */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	rel = table_open(relid, AccessShareLock);

	/*
	 * Reject attempts to read non-local temporary relations; we would be
	 * likely to get wrong data since we have no visibility into the owning
	 * session's local buffers.
	 */
	if (RELATION_IS_OTHER_TEMP(rel))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot access temporary tables of other sessions")));

	nblocks = RelationGetNumberOfBlocks(rel);

	/* scan all blocks in physical order */
	for (blkno = 1; blkno < nblocks; blkno++)
	{
		Datum		values[11];
		bool		nulls[11];
		OffsetNumber off;
		OffsetNumber maxoff;
		Buffer		buf;
		Page		page;
		NXBtreePageOpaque *opaque;
		int			nitems;
		int			ncompressed;
		int			totalsz;
		int			uncompressedsz;

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		CHECK_FOR_INTERRUPTS();

		/* Read the page */
		buf = ReadBuffer(rel, blkno);
		page = BufferGetPage(buf);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

		/*
		 * we're only interested in B-tree pages. (Presumably, most of the
		 * pages in the relation are b-tree pages, so it makes sense to scan
		 * the whole relation in physical order)
		 */
		if (PageGetSpecialSize(page) != MAXALIGN(sizeof(NXBtreePageOpaque)))
		{
			UnlockReleaseBuffer(buf);
			continue;
		}
		opaque = (NXBtreePageOpaque *) PageGetSpecialPointer(page);
		if (opaque->nx_page_id != NX_BTREE_PAGE_ID)
		{
			UnlockReleaseBuffer(buf);
			continue;
		}

		nitems = 0;
		ncompressed = 0;
		totalsz = 0;
		uncompressedsz = 0;
		if (opaque->nx_level == 0)
		{
			/* leaf page */
			maxoff = PageGetMaxOffsetNumber(page);
			for (off = FirstOffsetNumber; off <= maxoff; off++)
			{
				ItemId		iid = PageGetItemId(page, off);

				if (opaque->nx_attno == NX_META_ATTRIBUTE_NUM)
				{
					NXTidArrayItem *item = (NXTidArrayItem *) PageGetItem(page, iid);

					nitems++;
					totalsz += item->t_size;

					uncompressedsz += item->t_size;
				}
				else
				{
					NXAttributeArrayItem *item = (NXAttributeArrayItem *) PageGetItem(page, iid);

					nitems++;
					totalsz += item->t_size;
					if ((item->t_flags & NXBT_ATTR_COMPRESSED) != 0)
					{
						NXAttributeCompressedItem *citem = (NXAttributeCompressedItem *) PageGetItem(page, iid);

						ncompressed++;
						uncompressedsz += offsetof(NXAttributeCompressedItem, t_payload)
							+ citem->t_uncompressed_size;
					}
					else
						uncompressedsz += item->t_size;
				}
			}
		}
		else
		{
			/* internal page */
			nitems = NXBtreeInternalPageGetNumItems(page);
		}
		values[0] = Int64GetDatum(blkno);
		values[1] = Int64GetDatum(opaque->nx_next);
		values[2] = Int32GetDatum(opaque->nx_attno);
		values[3] = Int32GetDatum(opaque->nx_level);
		values[4] = Int64GetDatum(opaque->nx_lokey);
		values[5] = Int64GetDatum(opaque->nx_hikey);
		values[6] = Int32GetDatum(nitems);
		if (opaque->nx_level == 0)
		{
			values[7] = Int32GetDatum(ncompressed);
			values[8] = Int32GetDatum(totalsz);
			values[9] = Int32GetDatum(uncompressedsz);
		}
		else
		{
			nulls[7] = true;
			nulls[8] = true;
			nulls[9] = true;
		}
		values[10] = Int32GetDatum(PageGetExactFreeSpace(page));

		UnlockReleaseBuffer(buf);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}
	tuplestore_end(tupstore);

	table_close(rel, AccessShareLock);

	return (Datum) 0;
}

/*
 *  blkno int8
 *  undo_head int8
 *  undo_tail int8
 *  undo_tail_first_counter int8
 *  undo_oldestpointer_counter int8
 *  undo_oldestpointer_blkno int8
 *  undo_oldestpointer_offset int8
 *  fpm_head int8
 *  flags int4
 */
Datum
pg_nx_meta_page(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Relation	rel;
	TupleDesc	tupdesc;
	Datum		values[9];
	bool		nulls[9];
	Buffer		buf;
	Page		page;
	NXMetaPageOpaque *opaque;
	HeapTuple	tuple;
	Datum		result;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to use noxu inspection functions"))));

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));


	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	CHECK_FOR_INTERRUPTS();

	/* open the metapage */
	rel = table_open(relid, AccessShareLock);

	/*
	 * Reject attempts to read non-local temporary relations; we would be
	 * likely to get wrong data since we have no visibility into the owning
	 * session's local buffers.
	 */
	if (RELATION_IS_OTHER_TEMP(rel))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot access temporary tables of other sessions")));

	/* Read the page */
	buf = ReadBuffer(rel, NX_META_BLK);
	page = BufferGetPage(buf);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

	if (PageGetSpecialSize(page) != MAXALIGN(sizeof(NXMetaPageOpaque)))
	{
		UnlockReleaseBuffer(buf);
		elog(ERROR, "Bad page special size");
	}
	opaque = (NXMetaPageOpaque *) PageGetSpecialPointer(page);
	if (opaque->nx_page_id != NX_META_PAGE_ID)
	{
		UnlockReleaseBuffer(buf);
		elog(ERROR, "The nx_page_id does not match NX_META_PAGE_ID. Got: %d",
			 opaque->nx_page_id);
	}

	memset(values, 0, sizeof(values));
	memset(nulls, 0, sizeof(nulls));

	values[0] = Int64GetDatum(NX_META_BLK);
	values[1] = Int64GetDatum(opaque->nx_undo_head);
	values[2] = Int64GetDatum(opaque->nx_undo_tail);
	values[3] = Int64GetDatum(opaque->nx_undo_tail_first_counter);
	values[4] = Int64GetDatum(RelUndoGetCounter(opaque->nx_undo_oldestptr));
	values[5] = Int64GetDatum(RelUndoGetBlockNum(opaque->nx_undo_oldestptr));
	values[6] = Int32GetDatum(RelUndoGetOffset(opaque->nx_undo_oldestptr));
	values[7] = Int64GetDatum(opaque->nx_fpm_head);
	values[8] = Int32GetDatum(opaque->nx_flags);

	UnlockReleaseBuffer(buf);

	table_close(rel, AccessShareLock);

	tuple = heap_form_tuple(tupdesc, values, nulls);
	result = HeapTupleGetDatum(tuple);

	PG_RETURN_DATUM(result);
}
