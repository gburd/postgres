/*-------------------------------------------------------------------------
 *
 * undostats.c
 *	  UNDO log statistics collection and reporting
 *
 * This module provides monitoring and observability for the UNDO
 * subsystem, including:
 *   - Per-log statistics (insert/discard pointers, size, oldest xid)
 *   - Buffer cache statistics (hits, misses, evictions)
 *   - Aggregate counters (total records, bytes generated)
 *
 * Statistics can be queried via SQL functions pg_stat_get_undo_logs()
 * and pg_stat_get_undo_buffers(), registered in pg_proc.dat.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/undo/undostats.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/undobuffers.h"
#include "access/undolog.h"
#include "access/undostats.h"
#include "funcapi.h"
#include "storage/lwlock.h"
#include "utils/builtins.h"

/*
 * UndoLogStats - Per-log statistics snapshot
 *
 * Used to return a point-in-time snapshot of UNDO log state.
 */

/*
 * GetUndoLogStats - Get statistics for all active UNDO logs
 *
 * Fills the provided array with stats for each active log.
 * Returns the number of active logs found.
 */
int
GetUndoLogStats(UndoLogStat *stats, int max_stats)
{
	int			count = 0;
	int			i;

	if (UndoLogShared == NULL)
		return 0;

	for (i = 0; i < MAX_UNDO_LOGS && count < max_stats; i++)
	{
		UndoLogControl *log = &UndoLogShared->logs[i];

		if (!log->in_use)
			continue;

		LWLockAcquire(&log->lock, LW_SHARED);

		stats[count].log_number = log->log_number;
		stats[count].insert_ptr = log->insert_ptr;
		stats[count].discard_ptr = log->discard_ptr;
		stats[count].oldest_xid = log->oldest_xid;

		/* Calculate size as difference between insert and discard offsets */
		stats[count].size_bytes =
			UndoRecPtrGetOffset(log->insert_ptr) -
			UndoRecPtrGetOffset(log->discard_ptr);

		LWLockRelease(&log->lock);

		count++;
	}

	return count;
}

/*
 * GetUndoBufferStats - Get UNDO buffer cache statistics
 *
 * Returns current hit/miss/eviction/write counts from the
 * UNDO buffer cache (introduced in Commit 17).
 */
void
GetUndoBufferStats(UndoBufferStat *stats)
{
	UndoBufferGetStats(&stats->cache_hits,
					   &stats->cache_misses,
					   &stats->cache_evictions,
					   &stats->cache_writes);

	if (UndoBufCtl != NULL)
		stats->num_buffers = UndoBufCtl->num_buffers;
	else
		stats->num_buffers = 0;
}

/*
 * pg_stat_get_undo_logs - SQL-callable function returning UNDO log stats
 *
 * Returns a set of rows, one per active UNDO log, with columns:
 *   log_number, insert_offset, discard_offset, size_bytes, oldest_xid
 */
Datum
pg_stat_get_undo_logs(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	UndoLogStat *stats;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcxt;
		TupleDesc	tupdesc;
		int			nstats;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcxt = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* Build tuple descriptor */
		tupdesc = CreateTemplateTupleDesc(5);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "log_number",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "insert_offset",
						   INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "discard_offset",
						   INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "size_bytes",
						   INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "oldest_xid",
						   XIDOID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		/* Collect stats snapshot */
		stats = (UndoLogStat *) palloc(sizeof(UndoLogStat) * MAX_UNDO_LOGS);
		nstats = GetUndoLogStats(stats, MAX_UNDO_LOGS);

		funcctx->user_fctx = stats;
		funcctx->max_calls = nstats;

		MemoryContextSwitchTo(oldcxt);
	}

	funcctx = SRF_PERCALL_SETUP();
	stats = (UndoLogStat *) funcctx->user_fctx;

	if (funcctx->call_cntr < funcctx->max_calls)
	{
		UndoLogStat *stat = &stats[funcctx->call_cntr];
		Datum		values[5];
		bool		nulls[5];
		HeapTuple	tuple;

		MemSet(nulls, 0, sizeof(nulls));

		values[0] = Int32GetDatum(stat->log_number);
		values[1] = Int64GetDatum(UndoRecPtrGetOffset(stat->insert_ptr));
		values[2] = Int64GetDatum(UndoRecPtrGetOffset(stat->discard_ptr));
		values[3] = Int64GetDatum(stat->size_bytes);
		values[4] = TransactionIdGetDatum(stat->oldest_xid);

		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);

		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}

	SRF_RETURN_DONE(funcctx);
}

/*
 * pg_stat_get_undo_buffers - SQL-callable function returning buffer stats
 *
 * Returns a single row with UNDO buffer cache statistics:
 *   num_buffers, cache_hits, cache_misses, cache_evictions, cache_writes,
 *   hit_ratio
 */
Datum
pg_stat_get_undo_buffers(PG_FUNCTION_ARGS)
{
	TupleDesc	tupdesc;
	Datum		values[6];
	bool		nulls[6];
	HeapTuple	tuple;
	UndoBufferStat stats;

	/* Build tuple descriptor */
	tupdesc = CreateTemplateTupleDesc(6);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "num_buffers",
					   INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "cache_hits",
					   INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 3, "cache_misses",
					   INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 4, "cache_evictions",
					   INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 5, "cache_writes",
					   INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 6, "hit_ratio",
					   FLOAT4OID, -1, 0);

	tupdesc = BlessTupleDesc(tupdesc);

	/* Get statistics */
	GetUndoBufferStats(&stats);

	MemSet(nulls, 0, sizeof(nulls));

	values[0] = Int32GetDatum(stats.num_buffers);
	values[1] = Int64GetDatum(stats.cache_hits);
	values[2] = Int64GetDatum(stats.cache_misses);
	values[3] = Int64GetDatum(stats.cache_evictions);
	values[4] = Int64GetDatum(stats.cache_writes);

	/* Calculate hit ratio */
	{
		uint64		total = stats.cache_hits + stats.cache_misses;

		if (total > 0)
			values[5] = Float4GetDatum((float4) stats.cache_hits / total);
		else
			values[5] = Float4GetDatum(0.0);
	}

	tuple = heap_form_tuple(tupdesc, values, nulls);

	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}
