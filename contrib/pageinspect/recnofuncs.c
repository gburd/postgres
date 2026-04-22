/*-------------------------------------------------------------------------
 *
 * recnofuncs.c
 *	  Functions to investigate RECNO pages
 *
 * We check the input to these functions for corrupt pointers etc. that
 * might cause crashes, but at the same time we try to print out as much
 * information as possible, even if it's nonsense. That's because if a
 * page is corrupt, we don't know why and how exactly it is corrupt, so we
 * let the user judge it.
 *
 * These functions are restricted to superusers for the fear of introducing
 * security holes if the input checking isn't as water-tight as it should be.
 * You'd need to be superuser to obtain a raw page image anyway, so
 * there's hardly any use case for using these without superuser-rights
 * anyway.
 *
 * Copyright (c) 2007-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/pageinspect/recnofuncs.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/recno.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "pageinspect.h"
#include "port/pg_bitutils.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/pg_lsn.h"

/*
 * recno_page_items
 *
 * Allows inspection of line pointers and tuple headers of a RECNO page.
 */
PG_FUNCTION_INFO_V1(recno_page_items);

typedef struct recno_page_items_state
{
	TupleDesc	tupd;
	Page		page;
	uint16		offset;
} recno_page_items_state;

Datum
recno_page_items(PG_FUNCTION_ARGS)
{
	bytea	   *raw_page = PG_GETARG_BYTEA_P(0);
	recno_page_items_state *inter_call_data = NULL;
	FuncCallContext *fctx;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to use raw page functions")));

	if (SRF_IS_FIRSTCALL())
	{
		TupleDesc	tupdesc;
		MemoryContext mctx;

		fctx = SRF_FIRSTCALL_INIT();
		mctx = MemoryContextSwitchTo(fctx->multi_call_memory_ctx);

		inter_call_data = palloc_object(recno_page_items_state);

		/* Build a tuple descriptor for our result type */
		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
			elog(ERROR, "return type must be a row type");

		inter_call_data->tupd = tupdesc;

		inter_call_data->offset = FirstOffsetNumber;
		inter_call_data->page = get_page_from_raw(raw_page);

		fctx->max_calls = PageGetMaxOffsetNumber(inter_call_data->page);
		fctx->user_fctx = inter_call_data;

		MemoryContextSwitchTo(mctx);
	}

	fctx = SRF_PERCALL_SETUP();
	inter_call_data = fctx->user_fctx;

	if (fctx->call_cntr < fctx->max_calls)
	{
#define RECNO_PAGE_ITEMS_COLS 10
		Page		page = inter_call_data->page;
		HeapTuple	resultTuple;
		Datum		result;
		ItemId		id;
		Datum		values[RECNO_PAGE_ITEMS_COLS];
		bool		nulls[RECNO_PAGE_ITEMS_COLS];
		uint16		lp_offset;
		uint16		lp_flags;
		uint16		lp_len;

		memset(nulls, 0, sizeof(nulls));

		/* Extract information from the line pointer */
		id = PageGetItemId(page, inter_call_data->offset);

		lp_offset = ItemIdGetOffset(id);
		lp_flags = ItemIdGetFlags(id);
		lp_len = ItemIdGetLength(id);

		values[0] = UInt16GetDatum(inter_call_data->offset);	/* lp */
		values[1] = UInt16GetDatum(lp_offset);	/* lp_off */
		values[2] = UInt16GetDatum(lp_flags);	/* lp_flags */
		values[3] = UInt16GetDatum(lp_len);		/* lp_len */

		/*
		 * We do just enough validity checking to make sure we don't reference
		 * data outside the page passed to us. The page could be corrupt in
		 * many other ways, but at least we won't crash.
		 */
		if (ItemIdHasStorage(id) &&
			lp_len >= MAXALIGN(sizeof(RecnoTupleHeader)) &&
			lp_offset == MAXALIGN(lp_offset) &&
			lp_offset + lp_len <= BLCKSZ)
		{
			RecnoTupleHeader *tuphdr;

			/* Extract information from the RECNO tuple header */
			tuphdr = (RecnoTupleHeader *) PageGetItem(page, id);

			values[4] = Int32GetDatum(tuphdr->t_len);		/* t_len */
			values[5] = Int16GetDatum(tuphdr->t_natts);		/* t_natts */
			values[6] = Int16GetDatum(tuphdr->t_flags);		/* t_flags */
			values[7] = Int64GetDatum(tuphdr->t_commit_ts);	/* t_commit_ts */
			values[8] = Int16GetDatum((int16) tuphdr->t_infomask);	/* t_infomask */
			/* t_data: raw tuple data after the header */
			{
				int		hdr_size = MAXALIGN(sizeof(RecnoTupleHeader));
				int		tuple_data_len = lp_len - hdr_size;

				if (tuple_data_len > 0 && hdr_size <= lp_len)
				{
					bytea  *tuple_data_bytea;

					tuple_data_bytea = (bytea *) palloc(tuple_data_len + VARHDRSZ);
					SET_VARSIZE(tuple_data_bytea, tuple_data_len + VARHDRSZ);
					memcpy(VARDATA(tuple_data_bytea),
						   (char *) tuphdr + hdr_size,
						   tuple_data_len);
					values[9] = PointerGetDatum(tuple_data_bytea);
				}
				else
					nulls[9] = true;
			}
		}
		else
		{
			/*
			 * The line pointer is not used, or it's invalid. Set the rest of
			 * the fields to NULL.
			 */
			int			i;

			for (i = 4; i < RECNO_PAGE_ITEMS_COLS; i++)
				nulls[i] = true;
		}

		/* Build and return the result tuple. */
		resultTuple = heap_form_tuple(inter_call_data->tupd, values, nulls);
		result = HeapTupleGetDatum(resultTuple);

		inter_call_data->offset++;

		SRF_RETURN_NEXT(fctx, result);
	}
	else
		SRF_RETURN_DONE(fctx);
}

/*
 * recno_page_stats
 *
 * Returns page-level statistics for a RECNO page.
 */
PG_FUNCTION_INFO_V1(recno_page_stats);

Datum
recno_page_stats(PG_FUNCTION_ARGS)
{
#define RECNO_PAGE_STATS_COLS 13
	bytea	   *raw_page = PG_GETARG_BYTEA_P(0);
	Page		page;
	PageHeader	phdr;
	RecnoPageOpaque opaque;
	TupleDesc	tupdesc;
	HeapTuple	resultTuple;
	Datum		values[RECNO_PAGE_STATS_COLS];
	bool		nulls[RECNO_PAGE_STATS_COLS];

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to use raw page functions")));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	page = get_page_from_raw(raw_page);
	phdr = (PageHeader) page;

	memset(nulls, 0, sizeof(nulls));

	/* Page-level standard fields */
	values[0] = LSNGetDatum(PageGetLSN(page));		/* lsn */
	values[1] = UInt16GetDatum(0);	/* tli - no longer stored in page header */
	values[2] = UInt16GetDatum(phdr->pd_flags);		/* flags */
	values[3] = UInt16GetDatum(phdr->pd_lower);		/* lower */
	values[4] = UInt16GetDatum(phdr->pd_upper);		/* upper */
	values[5] = UInt16GetDatum(phdr->pd_special);	/* special */
	values[6] = UInt16GetDatum(PageGetPageSize(page));	/* pagesize */
	values[7] = UInt16GetDatum(PageGetPageLayoutVersion(page));	/* version */
	values[8] = UInt16GetDatum(PageGetExactFreeSpace(page));	/* free_size */

	/*
	 * Extract RECNO-specific opaque data from the special space. Only attempt
	 * this if the special space is large enough to hold our struct.
	 */
	if (phdr->pd_special <= BLCKSZ &&
		(BLCKSZ - phdr->pd_special) >= sizeof(RecnoPageOpaqueData))
	{
		opaque = RecnoPageGetOpaque(page);

		values[9] = Int64GetDatum(opaque->pd_commit_ts);		/* pd_commit_ts */
		values[10] = UInt16GetDatum(opaque->pd_free_space);		/* pd_free_space */
		values[11] = Int32GetDatum(opaque->pd_flags);			/* pd_flags */
	}
	else
	{
		nulls[9] = true;
		nulls[10] = true;
		nulls[11] = true;
	}

	/* Number of line pointers */
	values[12] = Int32GetDatum(PageGetMaxOffsetNumber(page));	/* max_off */

	resultTuple = heap_form_tuple(tupdesc, values, nulls);
	PG_RETURN_DATUM(HeapTupleGetDatum(resultTuple));
}

/*
 * recno_tuple_infomask_flags
 *
 * Decode t_infomask (uint8) into human-readable flag names.
 */
PG_FUNCTION_INFO_V1(recno_tuple_infomask_flags);

Datum
recno_tuple_infomask_flags(PG_FUNCTION_ARGS)
{
#define RECNO_TUPLE_INFOMASK_COLS 2
	Datum		values[RECNO_TUPLE_INFOMASK_COLS] = {0};
	bool		nulls[RECNO_TUPLE_INFOMASK_COLS] = {0};
	uint8		t_infomask = (uint8) PG_GETARG_INT32(0);
	int			cnt = 0;
	ArrayType  *a;
	int			bitcnt;
	Datum	   *flags;
	TupleDesc	tupdesc;
	HeapTuple	tuple;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to use raw page functions")));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	bitcnt = pg_popcount((const char *) &t_infomask, sizeof(uint8));

	/* If no flags, return empty arrays */
	if (bitcnt <= 0)
	{
		values[0] = PointerGetDatum(construct_empty_array(TEXTOID));
		values[1] = PointerGetDatum(construct_empty_array(TEXTOID));
		tuple = heap_form_tuple(tupdesc, values, nulls);
		PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
	}

	/* Build set of flag names */
	flags = palloc0_array(Datum, bitcnt);

	/* Decode t_infomask (uint8) */
	if ((t_infomask & RECNO_INFOMASK_HASNULL) != 0)
		flags[cnt++] = CStringGetTextDatum("RECNO_HASNULL");
	if ((t_infomask & RECNO_INFOMASK_HASVARWIDTH) != 0)
		flags[cnt++] = CStringGetTextDatum("RECNO_HASVARWIDTH");
	if ((t_infomask & RECNO_INFOMASK_HASEXTERNAL) != 0)
		flags[cnt++] = CStringGetTextDatum("RECNO_HASEXTERNAL");
	if ((t_infomask & RECNO_INFOMASK_COMPRESSED) != 0)
		flags[cnt++] = CStringGetTextDatum("RECNO_COMPRESSED");
	if ((t_infomask & RECNO_INFOMASK_HASOVERFLOW) != 0)
		flags[cnt++] = CStringGetTextDatum("RECNO_HASOVERFLOW");

	/* Build the combined_flags array (human-readable names) */
	Assert(cnt <= bitcnt);
	if (cnt == 0)
		a = construct_empty_array(TEXTOID);
	else
		a = construct_array_builtin(flags, cnt, TEXTOID);
	pfree(flags);
	/* raw_flags: same as combined for RECNO (no separate raw names) */
	values[0] = PointerGetDatum(a);
	values[1] = PointerGetDatum(a);

	/* Returns the record as Datum */
	tuple = heap_form_tuple(tupdesc, values, nulls);
	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}
