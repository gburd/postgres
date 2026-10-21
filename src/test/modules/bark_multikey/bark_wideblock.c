/*-------------------------------------------------------------------------
 *
 * bark_wideblock.c
 *		bark_test_wideblock, a test table AM with locator offsets up to about
 *		2037: heap, with every TID it hands out or takes translated.
 *
 * Heap block b's offset o (1 .. MaxHeapTuplesPerPage) is the logical TID
 * (b / WB_FOLD, (b % WB_FOLD) * MaxHeapTuplesPerPage + o), so WB_FOLD heap
 * blocks make one logical block and a logical offset reaches
 * WB_FOLD * MaxHeapTuplesPerPage (2037 at BLCKSZ 8192).  The mapping keeps
 * TID order.  Every callback that takes a TID turns it back into heap's and
 * runs heap's, and every one that returns a TID, in a slot or through a
 * callback, turns heap's into the logical one.  Bitmap scans are not
 * provided (the planner makes none of a table whose offsets exceed a TID
 * bitmap's); sample scans, CLUSTER and the like are refused.  VACUUM runs
 * heap's, with the indexes' bulk-delete callback translated: their AMs are
 * wrapped (wb_wrap_index) to see logical TIDs both ways.
 *
 * It exists to test BARK on real large offsets (bark_wideblock.sql), and is
 * good for nothing else.
 *
 * src/test/modules/bark_multikey/bark_wideblock.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/amapi.h"
#include "access/amlocator.h"
#include "access/heapam.h"
#include "access/relscan.h"
#include "access/tableam.h"
#include "access/tableam_indexscan.h"
#include "catalog/index.h"
#include "catalog/pg_am_d.h"
#include "catalog/pg_operator_d.h"
#include "catalog/pg_type_d.h"
#include "commands/vacuum.h"
#include "executor/tuptable.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "utils/rel.h"
#include "utils/tuplesort.h"

#define WB_FOLD		7
#define WB_MAXOFF	(WB_FOLD * MaxHeapTuplesPerPage)

StaticAssertDecl(WB_MAXOFF <= MaxOffsetNumber, "WB_FOLD too large");

/* heap TID -> logical TID */
static inline void
wb_to_logical(ItemPointer tid)
{
	BlockNumber b = ItemPointerGetBlockNumberNoCheck(tid);
	OffsetNumber o = ItemPointerGetOffsetNumberNoCheck(tid);

	if (!OffsetNumberIsValid(o) || o > MaxHeapTuplesPerPage)
		return;					/* not a real TID; leave it */
	ItemPointerSet(tid, b / WB_FOLD,
				   (b % WB_FOLD) * MaxHeapTuplesPerPage + o);
}

/* logical TID -> heap TID */
static inline void
wb_to_heap(ItemPointer tid)
{
	BlockNumber b = ItemPointerGetBlockNumberNoCheck(tid);
	OffsetNumber o = ItemPointerGetOffsetNumberNoCheck(tid);

	if (!OffsetNumberIsValid(o) || o > WB_MAXOFF)
		return;
	ItemPointerSet(tid, b * WB_FOLD + (o - 1) / MaxHeapTuplesPerPage,
				   (o - 1) % MaxHeapTuplesPerPage + 1);
}

static inline void
wb_slot_to_logical(TupleTableSlot *slot)
{
	wb_to_logical(&slot->tts_tid);
}

static const TableAmRoutine *heapam;
static TableAmRoutine wb_methods;

static const LocatorDesc wb_locator = {
	.width = sizeof(ItemPointerData),
	.max_offset = WB_MAXOFF,
	.name = "wideblock",
	.stable = false,
	.old_version_retained = true,
};

static const LocatorDesc *
wb_relation_locator(Relation rel)
{
	return &wb_locator;
}

/*
 * Run heap's callback with heap's routine in the relation, for the code
 * that refuses a routine that is not heap's (heap_getnext).
 */
#define WITH_HEAP_ROUTINE(rel, stmt) \
	do { \
		const TableAmRoutine *wb_saved = (rel)->rd_tableam; \
		(rel)->rd_tableam = heapam; \
		PG_TRY(); \
		{ \
			stmt; \
		} \
		PG_FINALLY(); \
		{ \
			(rel)->rd_tableam = wb_saved; \
		} \
		PG_END_TRY(); \
	} while (0)

/* ---- sequential and TID range scans ---- */

static bool
wb_getnextslot(TableScanDesc scan, ScanDirection direction,
			   TupleTableSlot *slot)
{
	if (!heapam->scan_getnextslot(scan, direction, slot))
		return false;
	wb_slot_to_logical(slot);
	return true;
}

static void
wb_set_tidrange(TableScanDesc scan, ItemPointer mintid, ItemPointer maxtid)
{
	ItemPointerData lo = *mintid;
	ItemPointerData hi = *maxtid;

	/* The mapping keeps order; an offset past the range clamps to its end. */
	if (ItemPointerGetOffsetNumberNoCheck(&lo) > WB_MAXOFF)
		ItemPointerSet(&lo, ItemPointerGetBlockNumberNoCheck(&lo) + 1, 1);
	if (ItemPointerGetOffsetNumberNoCheck(&hi) > WB_MAXOFF)
		ItemPointerSetOffsetNumber(&hi, WB_MAXOFF);
	if (ItemPointerGetOffsetNumberNoCheck(&lo) == 0)
		ItemPointerSetOffsetNumber(&lo, 1);
	if (ItemPointerGetOffsetNumberNoCheck(&hi) == 0)
	{
		/* (B,0) is below every TID of block B: the end of block B - 1 */
		if (ItemPointerGetBlockNumberNoCheck(&hi) == 0)
		{
			ItemPointerSet(&lo, 1, 1);	/* empty */
			ItemPointerSet(&hi, 0, 1);
		}
		else
			ItemPointerSet(&hi, ItemPointerGetBlockNumberNoCheck(&hi) - 1,
						   WB_MAXOFF);
	}
	wb_to_heap(&lo);
	wb_to_heap(&hi);
	heapam->scan_set_tidrange(scan, &lo, &hi);
}

static bool
wb_getnextslot_tidrange(TableScanDesc scan, ScanDirection direction,
						TupleTableSlot *slot)
{
	if (!heapam->scan_getnextslot_tidrange(scan, direction, slot))
		return false;
	wb_slot_to_logical(slot);
	return true;
}

/* ---- wrapped index AMs ---- */

/*
 * Heap's code calls the index AM with heap's TIDs in three places: an index
 * scan's fetch reads the TID amgettuple left in xs_heaptid, VACUUM asks
 * ambulkdelete's callback about the index's TIDs, and CREATE INDEX
 * CONCURRENTLY's validation inserts with aminsert.  For the length of such
 * a heap call the index's rd_indam is pointed at a copy of its routine
 * whose callbacks translate (wb_wrap_index).
 */
static IndexBulkDeleteCallback wb_vac_callback;

static bool
wb_bulkdelete_callback(ItemPointer itemptr, void *state)
{
	ItemPointerData htid = *itemptr;

	wb_to_heap(&htid);
	return wb_vac_callback(&htid, state);
}

typedef struct WbIndexAm
{
	const IndexAmRoutine *orig;
	IndexAmRoutine wrapped;
} WbIndexAm;

#define WB_MAX_INDEX_AMS 8
static WbIndexAm wb_index_ams[WB_MAX_INDEX_AMS];
static int	wb_nindex_ams;

static const IndexAmRoutine *
wb_orig_am(Relation index)
{
	for (int i = 0; i < wb_nindex_ams; i++)
		if (&wb_index_ams[i].wrapped == index->rd_indam)
			return wb_index_ams[i].orig;
	return index->rd_indam;
}

static IndexBulkDeleteResult *
wb_ambulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
				IndexBulkDeleteCallback callback, void *callback_state)
{
	IndexBulkDeleteCallback saved = wb_vac_callback;
	IndexBulkDeleteResult *result;

	wb_vac_callback = callback;
	PG_TRY();
	{
		result = wb_orig_am(info->index)->ambulkdelete(info, stats,
													   wb_bulkdelete_callback,
													   callback_state);
	}
	PG_FINALLY();
	{
		wb_vac_callback = saved;
	}
	PG_END_TRY();
	return result;
}

/* An insert from heap's validate scan, with heap's TID. */
static bool
wb_aminsert(Relation index, Datum *values, bool *isnull, ItemPointer heap_tid,
			Relation heapRelation, IndexUniqueCheck checkUnique,
			bool indexUnchanged, IndexInfo *indexInfo)
{
	ItemPointerData ltid = *heap_tid;

	wb_to_logical(&ltid);
	return wb_orig_am(index)->aminsert(index, values, isnull, &ltid,
									   heapRelation, checkUnique,
									   indexUnchanged, indexInfo);
}

/* The index scan's next TID, as heap's. */
static bool
wb_amgettuple(IndexScanDesc scan, ScanDirection direction)
{
	if (!wb_orig_am(scan->indexRelation)->amgettuple(scan, direction))
		return false;
	wb_to_heap(&scan->xs_heaptid);
	return true;
}

/* Point index->rd_indam at its translating copy; returns what to restore. */
static const IndexAmRoutine *
wb_wrap_index(Relation index)
{
	const IndexAmRoutine *prev = index->rd_indam;
	const IndexAmRoutine *orig = wb_orig_am(index);
	WbIndexAm  *w = NULL;

	for (int i = 0; i < wb_nindex_ams; i++)
	{
		if (wb_index_ams[i].orig == orig)
			w = &wb_index_ams[i];
	}
	if (w == NULL)
	{
		if (wb_nindex_ams >= WB_MAX_INDEX_AMS)
			elog(ERROR, "too many index AMs on a bark_test_wideblock table");
		w = &wb_index_ams[wb_nindex_ams++];
		w->orig = orig;
		w->wrapped = *orig;
		w->wrapped.ambulkdelete = wb_ambulkdelete;
		w->wrapped.aminsert = wb_aminsert;
		if (orig->amgettuple != NULL)
			w->wrapped.amgettuple = wb_amgettuple;
	}
	index->rd_indam = &w->wrapped;
	return prev;
}

/* ---- index scans ---- */

/*
 * An index scan: the index AM hands out logical TIDs, and heap's getnext,
 * which this runs, fetches by xs_heaptid, which wb_amgettuple turns into
 * heap's.  The slot heap fills, and xs_heaptid (WHERE CURRENT OF reads it),
 * are then turned back.
 */
typedef struct WbScanState
{
	void	   *heapstate;		/* heap's xs_table_opaque */
	bool		(*heapgetnext) (IndexScanDesc scan, ScanDirection direction,
								TupleTableSlot *slot);
} WbScanState;

static bool
wb_index_getnext_slot(IndexScanDesc scan, ScanDirection direction,
					  TupleTableSlot *slot)
{
	WbScanState *st = (WbScanState *) scan->xs_table_opaque;
	bool		found;

	Relation	index = scan->indexRelation;
	const IndexAmRoutine *orig;

	/* A HOT chain heap is still walking is at xs_heaptid, as heap's. */
	if (scan->xs_heap_continue)
		wb_to_heap(&scan->xs_heaptid);
	scan->xs_table_opaque = st->heapstate;
	orig = wb_wrap_index(index);
	PG_TRY();
	{
		found = st->heapgetnext(scan, direction, slot);
	}
	PG_FINALLY();
	{
		index->rd_indam = orig;
		st->heapstate = scan->xs_table_opaque;
		scan->xs_table_opaque = st;
	}
	PG_END_TRY();
	if (found && !scan->xs_want_itup)
		wb_slot_to_logical(slot);
	if (found)
		wb_to_logical(&scan->xs_heaptid);
	return found;
}

static void
wb_index_scan_begin(IndexScanDesc scan, uint32 flags)
{
	WbScanState *st = palloc0_object(WbScanState);

	heapam->index_scan_begin(scan, flags);
	st->heapstate = scan->xs_table_opaque;
	st->heapgetnext = scan->xs_getnext_slot;
	scan->xs_table_opaque = st;
	scan->xs_getnext_slot = wb_index_getnext_slot;
}

static void
wb_index_scan_reset(IndexScanDesc scan)
{
	WbScanState *st = (WbScanState *) scan->xs_table_opaque;

	scan->xs_table_opaque = st->heapstate;
	heapam->index_scan_reset(scan);
	scan->xs_table_opaque = st;
}

static void
wb_index_scan_end(IndexScanDesc scan)
{
	WbScanState *st = (WbScanState *) scan->xs_table_opaque;

	scan->xs_table_opaque = st->heapstate;
	heapam->index_scan_end(scan);
	pfree(st);
}

/* ---- tuple fetches ---- */

static bool
wb_fetch_tid(Relation rel, ItemPointer tid, Snapshot snapshot, bool *all_dead)
{
	bool		found;

	wb_to_heap(tid);
	found = heapam->fetch_tid(rel, tid, snapshot, all_dead);
	wb_to_logical(tid);
	return found;
}

static bool
wb_fetch_row_version(Relation rel, ItemPointer tid, Snapshot snapshot,
					 TupleTableSlot *slot)
{
	ItemPointerData htid = *tid;
	bool		found;

	wb_to_heap(&htid);
	found = heapam->tuple_fetch_row_version(rel, &htid, snapshot, slot);
	if (found)
		wb_slot_to_logical(slot);
	return found;
}

static bool
wb_tuple_tid_valid(TableScanDesc scan, ItemPointer tid)
{
	ItemPointerData htid = *tid;

	if (ItemPointerGetOffsetNumberNoCheck(tid) > WB_MAXOFF)
		return false;
	wb_to_heap(&htid);
	return heapam->tuple_tid_valid(scan, &htid);
}

static void
wb_tuple_get_latest_tid(TableScanDesc scan, ItemPointer tid)
{
	wb_to_heap(tid);
	heapam->tuple_get_latest_tid(scan, tid);
	wb_to_logical(tid);
}

static TransactionId
wb_index_delete_tuples(Relation rel, TM_IndexDeleteOp *delstate)
{
	TransactionId result;

	for (int i = 0; i < delstate->ndeltids; i++)
		wb_to_heap(&delstate->deltids[i].tid);
	result = heapam->index_delete_tuples(rel, delstate);
	for (int i = 0; i < delstate->ndeltids; i++)
		wb_to_logical(&delstate->deltids[i].tid);
	return result;
}

/* ---- modifications ---- */

static void
wb_tuple_insert(Relation rel, TupleTableSlot *slot, CommandId cid,
				uint32 options, BulkInsertStateData *bistate)
{
	heapam->tuple_insert(rel, slot, cid, options, bistate);
	wb_slot_to_logical(slot);
}

static void
wb_tuple_insert_speculative(Relation rel, TupleTableSlot *slot, CommandId cid,
							uint32 options, BulkInsertStateData *bistate,
							uint32 specToken)
{
	heapam->tuple_insert_speculative(rel, slot, cid, options, bistate,
									 specToken);
	wb_slot_to_logical(slot);
}

static void
wb_tuple_complete_speculative(Relation rel, TupleTableSlot *slot,
							  uint32 specToken, bool succeeded)
{
	wb_to_heap(&slot->tts_tid);
	heapam->tuple_complete_speculative(rel, slot, specToken, succeeded);
	wb_slot_to_logical(slot);
}

static void
wb_multi_insert(Relation rel, TupleTableSlot **slots, int nslots,
				CommandId cid, uint32 options, BulkInsertStateData *bistate)
{
	heapam->multi_insert(rel, slots, nslots, cid, options, bistate);
	for (int i = 0; i < nslots; i++)
		wb_slot_to_logical(slots[i]);
}

static TM_Result
wb_tuple_delete(Relation rel, ItemPointer tid, CommandId cid, uint32 options,
				Snapshot snapshot, Snapshot crosscheck, bool wait,
				TM_FailureData *tmfd)
{
	ItemPointerData htid = *tid;

	wb_to_heap(&htid);
	return heapam->tuple_delete(rel, &htid, cid, options, snapshot,
								crosscheck, wait, tmfd);
}

static TM_Result
wb_tuple_update(Relation rel, ItemPointer otid, TupleTableSlot *slot,
				CommandId cid, uint32 options, Snapshot snapshot,
				Snapshot crosscheck, bool wait, TM_FailureData *tmfd,
				LockTupleMode *lockmode, const Bitmapset *modified_idx_attrs,
				TU_UpdateIndexes *update_indexes)
{
	ItemPointerData htid = *otid;
	TM_Result	result;

	wb_to_heap(&htid);
	result = heapam->tuple_update(rel, &htid, slot, cid, options, snapshot,
								  crosscheck, wait, tmfd, lockmode,
								  modified_idx_attrs, update_indexes);
	wb_slot_to_logical(slot);
	return result;
}

static TM_Result
wb_tuple_lock(Relation rel, ItemPointer tid, Snapshot snapshot,
			  TupleTableSlot *slot, CommandId cid, LockTupleMode mode,
			  LockWaitPolicy wait_policy, uint8 flags, TM_FailureData *tmfd)
{
	TM_Result	result;

	wb_to_heap(tid);
	result = heapam->tuple_lock(rel, tid, snapshot, slot, cid, mode,
								wait_policy, flags, tmfd);
	wb_to_logical(tid);
	if (!TTS_EMPTY(slot))
		wb_slot_to_logical(slot);
	return result;
}

/* ---- index builds ---- */

typedef struct WbBuildState
{
	IndexBuildCallback callback;
	void	   *state;
} WbBuildState;

static void
wb_build_callback(Relation index, ItemPointer tid, Datum *values,
				  bool *isnull, bool tupleIsAlive, void *state)
{
	WbBuildState *bs = (WbBuildState *) state;
	ItemPointerData ltid = *tid;

	wb_to_logical(&ltid);
	bs->callback(index, &ltid, values, isnull, tupleIsAlive, bs->state);
}

static double
wb_index_build_range_scan(Relation table_rel, Relation index_rel,
						  IndexInfo *index_info, bool allow_sync,
						  bool anyvisible, bool progress,
						  BlockNumber start_blockno, BlockNumber numblocks,
						  IndexBuildCallback callback, void *callback_state,
						  TableScanDesc scan)
{
	WbBuildState bs = {callback, callback_state};
	double		result = 0;

	/* A range scan (BRIN) would name logical blocks; none is needed here. */
	if (start_blockno != 0 || numblocks != InvalidBlockNumber)
		elog(ERROR, "bark_test_wideblock does not support block range scans");
	WITH_HEAP_ROUTINE(table_rel,
					  result = heapam->index_build_range_scan(table_rel,
															  index_rel,
															  index_info,
															  allow_sync,
															  anyvisible,
															  progress,
															  start_blockno,
															  numblocks,
															  wb_build_callback,
															  &bs, scan));
	return result;
}

/*
 * CREATE INDEX CONCURRENTLY's validation merges the index's TIDs, sorted, with
 * the heap in heap order.  The mapping keeps order, so it is enough to turn
 * the sorted TIDs into heap's: they were sorted as logical TIDs, an order
 * the mapping preserves.  They are re-sorted through a sort of our own.
 */
static void
wb_index_validate_scan(Relation table_rel, Relation index_rel,
					   IndexInfo *index_info, Snapshot snapshot,
					   ValidateIndexState *state)
{
	Tuplesortstate *logical = state->tuplesort;
	Tuplesortstate *heapsort;
	Datum		val;
	bool		isnull;

	heapsort = tuplesort_begin_datum(INT8OID, Int8LessOperator,
									 InvalidOid, false,
									 maintenance_work_mem, NULL,
									 TUPLESORT_NONE);
	while (tuplesort_getdatum(logical, true, false, &val, &isnull, NULL))
	{
		ItemPointerData tid;

		itemptr_decode(&tid, DatumGetInt64(val));
		wb_to_heap(&tid);
		tuplesort_putdatum(heapsort, Int64GetDatum(itemptr_encode(&tid)),
						   false);
	}
	tuplesort_performsort(heapsort);
	state->tuplesort = heapsort;

	/*
	 * Heap inserts the missing rows with index_insert and heap's TIDs: the
	 * wrapped aminsert (wb_aminsert) turns them into logical ones.
	 */
	{
		const IndexAmRoutine *orig = wb_wrap_index(index_rel);
		const TableAmRoutine *saved = table_rel->rd_tableam;

		table_rel->rd_tableam = heapam;
		PG_TRY();
		{
			heapam->index_validate_scan(table_rel, index_rel, index_info,
										snapshot, state);
		}
		PG_FINALLY();
		{
			table_rel->rd_tableam = saved;
			index_rel->rd_indam = orig;
		}
		PG_END_TRY();
	}
	state->tuplesort = logical;
	tuplesort_end(heapsort);
}

/* ---- VACUUM ---- */

/*
 * heap's VACUUM hands the indexes its dead TIDs through the bulk-delete
 * callback, which the index AM calls with its own, logical, TIDs.
 */
static void
wb_relation_vacuum(Relation rel, const VacuumParams *params,
				   BufferAccessStrategy bstrategy)
{
	VacuumParams p = *params;
	List	   *indexoids = RelationGetIndexList(rel);
	int			n = list_length(indexoids);
	Relation   *indexes = palloc_array(Relation, Max(n, 1));
	const IndexAmRoutine **saved = palloc_array(const IndexAmRoutine *, Max(n, 1));
	int			i = 0;

	/*
	 * Heap's VACUUM opens the indexes itself; they are the same relcache
	 * entries, opened here first, so the wrapped routine is what it sees.
	 * Parallel index vacuum would run the routines in workers, unwrapped.
	 */
	p.nworkers = -1;
	foreach_oid(indexoid, indexoids)
	{
		indexes[i] = index_open(indexoid, RowExclusiveLock);
		saved[i] = wb_wrap_index(indexes[i]);
		i++;
	}
	PG_TRY();
	{
		heapam->relation_vacuum(rel, &p, bstrategy);
	}
	PG_FINALLY();
	{
		for (i = 0; i < n; i++)
		{
			indexes[i]->rd_indam = saved[i];
			index_close(indexes[i], NoLock);
		}
	}
	PG_END_TRY();
}

/* heap_insert writes TOAST rows with heap's TIDs: keep the TOAST table heap. */
static Oid
wb_relation_toast_am(Relation rel)
{
	return HEAP_TABLE_AM_OID;
}

/* ---- refused ---- */

static bool
wb_scan_sample_next_block(TableScanDesc scan, SampleScanState *scanstate)
{
	elog(ERROR, "bark_test_wideblock does not support sample scans");
	return false;
}

static bool
wb_scan_analyze_next_tuple(TableScanDesc scan, double *liverows,
						   double *deadrows, TupleTableSlot *slot)
{
	if (!heapam->scan_analyze_next_tuple(scan, liverows, deadrows, slot))
		return false;
	wb_slot_to_logical(slot);
	return true;
}

static void
wb_relation_copy_for_cluster(Relation OldTable, Relation NewTable,
							 Relation OldIndex, bool use_sort,
							 TransactionId OldestXmin, Snapshot snapshot,
							 TransactionId *xid_cutoff,
							 MultiXactId *multi_cutoff, double *num_tuples,
							 double *tups_vacuumed,
							 double *tups_recently_dead)
{
	elog(ERROR, "bark_test_wideblock does not support CLUSTER or VACUUM FULL");
}

PG_FUNCTION_INFO_V1(bark_test_wideblock_handler);

Datum
bark_test_wideblock_handler(PG_FUNCTION_ARGS)
{
	if (wb_methods.type == 0)
	{
		heapam = GetHeapamTableAmRoutine();
		wb_methods = *heapam;
		wb_methods.relation_locator = wb_relation_locator;
		wb_methods.scan_getnextslot = wb_getnextslot;
		wb_methods.scan_set_tidrange = wb_set_tidrange;
		wb_methods.scan_getnextslot_tidrange = wb_getnextslot_tidrange;
		wb_methods.index_scan_begin = wb_index_scan_begin;
		wb_methods.index_scan_reset = wb_index_scan_reset;
		wb_methods.index_scan_end = wb_index_scan_end;
		wb_methods.fetch_tid = wb_fetch_tid;
		wb_methods.tuple_fetch_row_version = wb_fetch_row_version;
		wb_methods.tuple_tid_valid = wb_tuple_tid_valid;
		wb_methods.tuple_get_latest_tid = wb_tuple_get_latest_tid;
		wb_methods.index_delete_tuples = wb_index_delete_tuples;
		wb_methods.tuple_insert = wb_tuple_insert;
		wb_methods.tuple_insert_speculative = wb_tuple_insert_speculative;
		wb_methods.tuple_complete_speculative = wb_tuple_complete_speculative;
		wb_methods.multi_insert = wb_multi_insert;
		wb_methods.tuple_delete = wb_tuple_delete;
		wb_methods.tuple_update = wb_tuple_update;
		wb_methods.tuple_lock = wb_tuple_lock;
		wb_methods.index_build_range_scan = wb_index_build_range_scan;
		wb_methods.index_validate_scan = wb_index_validate_scan;
		wb_methods.relation_vacuum = wb_relation_vacuum;
		wb_methods.relation_toast_am = wb_relation_toast_am;
		wb_methods.relation_copy_for_cluster = wb_relation_copy_for_cluster;
		wb_methods.scan_analyze_next_tuple = wb_scan_analyze_next_tuple;
		wb_methods.scan_sample_next_block = wb_scan_sample_next_block;
		wb_methods.scan_bitmap_next_tuple = NULL;
	}
	PG_RETURN_POINTER(&wb_methods);
}
