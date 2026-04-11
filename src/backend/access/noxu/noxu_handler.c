/*-------------------------------------------------------------------------
 *
 * noxu_handler.c
 *	  Noxu table access method code
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/noxu/noxu_handler.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "miscadmin.h"

#include "access/detoast.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/multixact.h"
#include "access/relscan.h"
#include "access/tableam.h"
#include "access/tsmapi.h"
#include "access/tupdesc_details.h"
#include "access/heaptoast.h"
#include "access/xact.h"
#include "access/noxu_internal.h"
#include "access/noxu_lsm.h"
#include "access/noxu_nursery.h"
#include "access/noxu_planner.h"
#include "access/noxu_stats.h"
#include "access/relundo.h"
#include "catalog/catalog.h"
#include "catalog/index.h"
#include "catalog/pg_class.h"
#include "catalog/storage.h"
#include "catalog/storage_xlog.h"
#include "commands/progress.h"
#include "commands/vacuum.h"
#include "executor/executor.h"
#include "optimizer/optimizer.h"
#include "optimizer/plancat.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/smgr.h"
#include "storage/predicate.h"
#include "storage/procarray.h"
#include "storage/read_stream.h"
#include "access/htup_details.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/injection_point.h"
#include "utils/rel.h"
#include "utils/hsearch.h"
#include "utils/tuplesort.h"


typedef enum
{
	NXSCAN_STATE_UNSTARTED,
	NXSCAN_STATE_SCANNING,
	NXSCAN_STATE_FINISHED_RANGE,
	NXSCAN_STATE_FINISHED
}			nx_scan_state;

typedef struct NoxuProjectData
{
	int			num_proj_atts;
	Bitmapset  *project_columns;
	int		   *proj_atts;
	NXTidTreeScan tid_scan;
	NXAttrTreeScan *attr_scans;
	MemoryContext context;
}			NoxuProjectData;

typedef struct NoxuDescData
{
	/* scan parameters */
	TableScanDescData rs_scan;	/* */
	NoxuProjectData proj_data;

	bool		started;
	nxtid		cur_range_start;
	nxtid		cur_range_end;

	/*
	 * These fields are used for bitmap scans, to hold a "block's" worth of
	 * data
	 */
#define	MAX_ITEMS_PER_LOGICAL_BLOCK		MaxHeapTuplesPerPage
	int			bmscan_ntuples;
	nxtid	   *bmscan_tids;
	Datum	  **bmscan_datums;
	bool	  **bmscan_isnulls;
	int			bmscan_nexttuple;

	/* These fields are use for TABLESAMPLE scans */
	nxtid		max_tid_to_scan;
	nxtid		next_tid_to_scan;

	/* Nursery read-your-writes state */
	int			nursery_scan_idx;	/* Next nursery entry to check (-1 = done) */
	bool		nursery_flushed;	/* Has nursery been flushed for this scan? */

}			NoxuDescData;

typedef struct NoxuDescData *NoxuDesc;

typedef struct NoxuIndexFetchData
{
	IndexFetchTableData idx_fetch_data;
	NoxuProjectData proj_data;
}			NoxuIndexFetchData;

typedef struct NoxuIndexFetchData *NoxuIndexFetch;

typedef struct ParallelNXScanDescData *ParallelNXScanDesc;

static IndexFetchTableData *noxuam_begin_index_fetch(Relation rel, uint32 flags);
static void noxuam_end_index_fetch(IndexFetchTableData *scan);
static bool noxuam_fetch_row(NoxuIndexFetchData * fetch,
							 ItemPointer tid_p,
							 Snapshot snapshot,
							 TupleTableSlot *slot);
static bool nx_acquire_tuplock(Relation relation, ItemPointer tid, LockTupleMode mode,
							   LockWaitPolicy wait_policy, bool *have_tuple_lock);

static Size nx_parallelscan_estimate(Relation rel);
static Size nx_parallelscan_initialize(Relation rel, ParallelTableScanDesc pscan);
static void nx_parallelscan_reinitialize(Relation rel, ParallelTableScanDesc pscan);
static bool nx_parallelscan_nextrange(Relation rel, ParallelNXScanDesc nxscan,
									  nxtid * start, nxtid * end);
static void nxbt_fill_missing_attribute_value(TupleDesc tupleDesc, int attno, Datum *datum, bool *isnull);
static bool nx_fetch_attr_with_predecessor(Relation rel, TupleDesc tupdesc,
										   AttrNumber attno, nxtid tid,
										   Datum *datum, bool *isnull);

/* ----------------------------------------------------------------
 *				storage AM support routines for noxuam
 * ----------------------------------------------------------------
 */

static bool
noxuam_fetch_row_version(Relation rel,
						 ItemPointer tid_p,
						 Snapshot snapshot,
						 TupleTableSlot *slot)
{
	IndexFetchTableData *fetcher;
	bool		result;

	fetcher = noxuam_begin_index_fetch(rel, 0);

	result = noxuam_fetch_row((NoxuIndexFetchData *) fetcher,
							  tid_p, snapshot, slot);
	ExecMaterializeSlot(slot);
	slot->tts_tableOid = RelationGetRelid(rel);
	slot->tts_tid = *tid_p;

	noxuam_end_index_fetch(fetcher);

	return result;
}

static void
noxuam_get_latest_tid(TableScanDesc sscan,
					  ItemPointer tid)
{
	nxtid		ztid = NXTidFromItemPointer(*tid);

	nxbt_find_latest_tid(sscan->rs_rd, &ztid, sscan->rs_snapshot);
	*tid = ItemPointerFromNXTid(ztid);
}

static inline void
noxuam_insert_internal(Relation relation, TupleTableSlot *slot, CommandId cid,
					   int options, struct BulkInsertStateData *bistate, uint32 speculative_token)
{
	AttrNumber	attno;
	Datum	   *d;
	bool	   *isnulls;
	nxtid		tid;
	TransactionId xid = GetCurrentTransactionId();
	bool		isnull;
	Datum		datum;
	MemoryContext oldcontext;
	MemoryContext insert_mcontext;

	(void) options;
	(void) bistate;

	if (slot->tts_tupleDescriptor->natts != relation->rd_att->natts)
		elog(ERROR, "slot's attribute count doesn't match relcache entry");

	slot_getallattrs(slot);

	/*
	 * Nursery path: buffer attribute data for batch insertion.
	 *
	 * The nursery is used when:
	 * - noxu.nursery_enabled is on
	 * - This is not a speculative insertion (INSERT...ON CONFLICT)
	 * - No datum exceeds MaxNoxuDatumSize (overflow needs the TID immediately)
	 *
	 * The TID tree entry is created immediately (so indexes get a valid TID),
	 * but attribute data is deferred until the nursery flushes, at which point
	 * nxbt_attr_multi_insert() is called with a large batch, enabling
	 * type-specific compression codecs (Chimp, DOD, FOR, Dict, UUID v7, etc.)
	 * that require nitems >= 2.
	 */
	if (noxu_nursery_enabled && speculative_token == INVALID_SPECULATIVE_TOKEN)
	{
		NXNurseryBuffer *nursery;
		bool		has_oversized = false;

		/* Check for oversized datums that need immediate overflow handling */
		d = slot->tts_values;
		isnulls = slot->tts_isnull;
		for (attno = 1; attno <= relation->rd_att->natts; attno++)
		{
			Form_pg_attribute attr = TupleDescAttr(slot->tts_tupleDescriptor, attno - 1);

			if (!isnulls[attno - 1] && attr->attlen < 0 &&
				!VARATT_IS_EXTERNAL((struct varlena *) DatumGetPointer(d[attno - 1])) &&
				VARSIZE_ANY_EXHDR((struct varlena *) DatumGetPointer(d[attno - 1])) > MaxNoxuDatumSize)
			{
				has_oversized = true;
				break;
			}
		}

		if (!has_oversized)
		{
			nursery = nx_nursery_get_or_create(relation);

			/* Flush if at capacity or memory limit */
			if (nursery->nrows >= nursery->capacity ||
				nursery->mem_bytes >= (Size) noxu_nursery_mem_limit_kb * 1024)
			{
				nx_nursery_flush(relation, nursery);
			}

			/* Insert TID immediately (for index correctness) */
			tid = InvalidNXTid;
			nxbt_tid_multi_insert(relation, &tid, 1,
								  xid, cid, speculative_token,
								  InvalidRelUndoRecPtr);

			CheckForSerializableConflictIn(relation, NULL, InvalidBlockNumber);

			/* Buffer attribute data for batch flush later */
			nx_nursery_buffer_row(nursery, slot, tid, cid);

			slot->tts_tableOid = RelationGetRelid(relation);
			slot->tts_tid = ItemPointerFromNXTid(tid);

			pgstat_count_heap_insert(relation, 1);
			nxstats_count_insert(RelationGetRelid(relation), 1);
			return;
		}

		/* Fall through to direct insert for oversized datums */
	}

	/*
	 * Direct insert path: create TID and attribute items immediately.
	 *
	 * Used when the nursery is disabled, for speculative inserts, or when
	 * datums exceed MaxNoxuDatumSize and need overflow pages.
	 *
	 * Insert code performs many small allocations for creating and merging
	 * items, proportional to the number of columns and rows. Using a
	 * dedicated memory context allows efficient bulk cleanup via
	 * MemoryContextDelete rather than tracking individual pfree calls.
	 */
	insert_mcontext = AllocSetContextCreate(CurrentMemoryContext,
											"NoxuAMContext",
											ALLOCSET_DEFAULT_SIZES);
	oldcontext = MemoryContextSwitchTo(insert_mcontext);

	d = slot->tts_values;
	isnulls = slot->tts_isnull;

	tid = InvalidNXTid;

	isnull = true;
	nxbt_tid_multi_insert(relation,
						  &tid, 1,
						  xid, cid, speculative_token, InvalidRelUndoRecPtr);

	/*
	 * We only need to check for table-level SSI locks. Our new tuple can't
	 * possibly conflict with existing tuple locks, and page locks are only
	 * consolidated versions of tuple locks; they do not lock "gaps" as index
	 * page locks do.
	 */
	CheckForSerializableConflictIn(relation, NULL, InvalidBlockNumber);

	for (attno = 1; attno <= relation->rd_att->natts; attno++)
	{
		Form_pg_attribute attr = TupleDescAttr(slot->tts_tupleDescriptor, attno - 1);

		datum = d[attno - 1];
		isnull = isnulls[attno - 1];

		if (!isnull && attr->attlen < 0 && VARATT_IS_EXTERNAL((struct varlena *) DatumGetPointer(datum)))
			datum = PointerGetDatum(detoast_external_attr((struct varlena *) DatumGetPointer(datum)));

		/* If this datum is too large, overflow it */
		if (!isnull && attr->attlen < 0 &&
			VARSIZE_ANY_EXHDR((struct varlena *) DatumGetPointer(datum)) > MaxNoxuDatumSize)
		{
			datum = noxu_overflow_datum(relation, attno, datum, tid);
		}

		nxbt_attr_multi_insert(relation, (AttrNumber) attno,
							   &datum, &isnull, &tid, 1);
	}

	slot->tts_tableOid = RelationGetRelid(relation);
	slot->tts_tid = ItemPointerFromNXTid(tid);

	/*
	 * No need to set visi_info here. For a freshly inserted tuple, the
	 * inserting transaction is always the current transaction, so visibility
	 * is trivially determined. visi_info is populated when tuples are
	 * fetched from the TID tree during scans.
	 */

	MemoryContextSwitchTo(oldcontext);
	MemoryContextDelete(insert_mcontext);

	/* Note: speculative insertions are counted too, even if aborted later */
	pgstat_count_heap_insert(relation, 1);
	nxstats_count_insert(RelationGetRelid(relation), 1);
}

static void
noxuam_insert(Relation relation, TupleTableSlot *slot, CommandId cid,
			  uint32 options, struct BulkInsertStateData *bistate)
{
	noxuam_insert_internal(relation, slot, cid, options, bistate, INVALID_SPECULATIVE_TOKEN);
}

static void
noxuam_insert_speculative(Relation relation, TupleTableSlot *slot, CommandId cid,
						  uint32 options, BulkInsertState bistate, uint32 specToken)
{
	noxuam_insert_internal(relation, slot, cid, options, bistate, specToken);
}

static void
noxuam_complete_speculative(Relation relation, TupleTableSlot *slot, uint32 spekToken,
							bool succeeded)
{
	nxtid		tid;

	tid = NXTidFromItemPointer(slot->tts_tid);

	if (succeeded)
	{
		/*
		 * Mark the speculative insertion as confirmed by clearing the
		 * speculative token.  This mirrors heap_finish_speculative().
		 */
		nxbt_tid_clear_speculative_token(relation, tid, spekToken, true /* for complete */ );
	}
	else
	{
		/*
		 * The speculative insertion failed (conflict detected).  Mark the TID
		 * as dead immediately so that concurrent transactions see it as gone,
		 * matching the behavior of heap_abort_speculative() which sets xmin to
		 * InvalidTransactionId to make the tuple invisible to everyone.
		 *
		 * We do NOT clear the speculative token first: there is no point in
		 * confirming an insertion we are about to kill, and doing so would open
		 * a window where a concurrent SnapshotDirty scan could see the tuple as
		 * a valid, non-speculative insertion before we mark it dead.  Killing
		 * it first (or exclusively) avoids that race.
		 */
		RelUndoRecPtr recent_oldest_undo = nx_get_oldest_visible_undo_ptr(relation);

		nxbt_tid_mark_dead(relation, tid, recent_oldest_undo);
	}
}

static void
noxuam_multi_insert(Relation relation, TupleTableSlot **slots, int ntuples,
					CommandId cid, uint32 options, BulkInsertState bistate)
{
	AttrNumber	attno;
	int			i;
	bool		slotgetandset = true;
	TransactionId xid = GetCurrentTransactionId();
	Datum	   *datums;
	bool	   *isnulls;
	nxtid	   *tids;

	(void) options;
	(void) bistate;

	if (ntuples == 0)
	{
		/* COPY sometimes calls us with 0 tuples. */
		return;
	}

	datums = palloc0(ntuples * sizeof(Datum));
	isnulls = palloc(ntuples * sizeof(bool));
	tids = palloc0(ntuples * sizeof(nxtid));

	for (i = 0; i < ntuples; i++)
		isnulls[i] = true;

	nxbt_tid_multi_insert(relation, tids, ntuples,
						  xid, cid, INVALID_SPECULATIVE_TOKEN, InvalidRelUndoRecPtr);

	/*
	 * We only need to check for table-level SSI locks. Our new tuple can't
	 * possibly conflict with existing tuple locks, and page locks are only
	 * consolidated versions of tuple locks; they do not lock "gaps" as index
	 * page locks do.
	 */
	CheckForSerializableConflictIn(relation, NULL, InvalidBlockNumber);

	for (attno = 1; attno <= relation->rd_att->natts; attno++)
	{
		Form_pg_attribute attr = TupleDescAttr((slots[0])->tts_tupleDescriptor, attno - 1);

		for (i = 0; i < ntuples; i++)
		{
			Datum		datum = slots[i]->tts_values[attno - 1];
			bool		isnull = slots[i]->tts_isnull[attno - 1];

			if (slotgetandset)
			{
				slot_getallattrs(slots[i]);
			}

			/* If this datum is too large, overflow it */
			if (!isnull && attr->attlen < 0 &&
				VARSIZE_ANY_EXHDR((struct varlena *) DatumGetPointer(datum)) > MaxNoxuDatumSize)
			{
				datum = noxu_overflow_datum(relation, attno, datum, tids[i]);
			}
			datums[i] = datum;
			isnulls[i] = isnull;
		}

		nxbt_attr_multi_insert(relation, (AttrNumber) attno,
							   datums, isnulls, tids, ntuples);

		slotgetandset = false;
	}

	for (i = 0; i < ntuples; i++)
	{
		slots[i]->tts_tableOid = RelationGetRelid(relation);
		slots[i]->tts_tid = ItemPointerFromNXTid(tids[i]);
	}

	pgstat_count_heap_insert(relation, ntuples);
	nxstats_count_insert(RelationGetRelid(relation), ntuples);

	pfree(tids);
	pfree(datums);
	pfree(isnulls);
}

static TM_Result
noxuam_delete(Relation relation, ItemPointer tid_p, CommandId cid,
			  uint32 options, Snapshot snapshot, Snapshot crosscheck,
			  bool wait, TM_FailureData *hufd)
{
	nxtid		tid = NXTidFromItemPointer(*tid_p);
	TransactionId xid = GetCurrentTransactionId();
	TM_Result	result = TM_Ok;
	bool		this_xact_has_lock = false;
	bool		have_tuple_lock = false;
	bool		changingPart = (options & TABLE_DELETE_CHANGING_PARTITION) != 0;

retry:
	result = nxbt_tid_delete(relation, tid, xid, cid,
							 snapshot, crosscheck, wait, hufd, changingPart,
							 &this_xact_has_lock);

	if (result != TM_Ok)
	{
		if (result == TM_Invisible)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("attempted to delete invisible tuple")));
		else if (result == TM_BeingModified && wait)
		{
			TransactionId xwait = hufd->xmax;

			if (!TransactionIdIsCurrentTransactionId(xwait))
			{
				/*
				 * Acquire tuple lock to establish our priosity for the tuple
				 * See noxuam_lock_tuple().
				 */
				if (!this_xact_has_lock)
				{
					nx_acquire_tuplock(relation, tid_p, LockTupleExclusive, LockWaitBlock,
									   &have_tuple_lock);
				}

				XactLockTableWait(xwait, relation, tid_p, XLTW_Delete);
				goto retry;
			}
		}
	}

	/*
	 * Check for SSI conflicts.
	 */
	CheckForSerializableConflictIn(relation, tid_p, ItemPointerGetBlockNumber(tid_p));

	if (result == TM_Ok)
	{
		pgstat_count_heap_delete(relation);
		nxstats_count_delete(RelationGetRelid(relation));
	}

	return result;
}


/*
 * Each tuple lock mode has a corresponding heavyweight lock, and one or two
 * corresponding MultiXactStatuses (one to merely lock tuples, another one to
 * update them).  This table (and the macros below) helps us determine the
 * heavyweight lock mode and MultiXactStatus values to use for any particular
 * tuple lock strength.
 *
 * Don't look at lockstatus/updstatus directly!  Use get_mxact_status_for_lock
 * instead.
 */
static const struct
{
	LOCKMODE	hwlock;
	int			lockstatus;
	int			updstatus;
}

			tupleLockExtraInfo[MaxLockTupleMode + 1] =
{
	{							/* LockTupleKeyShare */
		AccessShareLock,
		MultiXactStatusForKeyShare,
		-1						/* KeyShare does not allow updating tuples */
	},
	{							/* LockTupleShare */
		RowShareLock,
		MultiXactStatusForShare,
		-1						/* Share does not allow updating tuples */
	},
	{							/* LockTupleNoKeyExclusive */
		ExclusiveLock,
		MultiXactStatusForNoKeyUpdate,
		MultiXactStatusNoKeyUpdate
	},
	{							/* LockTupleExclusive */
		AccessExclusiveLock,
		MultiXactStatusForUpdate,
		MultiXactStatusUpdate
	}
};


/*
 * Acquire heavyweight locks on tuples, using a LockTupleMode strength value.
 * This is more readable than having every caller translate it to lock.h's
 * LOCKMODE.
 */
#define LockTupleTuplock(rel, tup, mode) \
	LockTuple((rel), (tup), tupleLockExtraInfo[mode].hwlock)
#define UnlockTupleTuplock(rel, tup, mode) \
	UnlockTuple((rel), (tup), tupleLockExtraInfo[mode].hwlock)
#define ConditionalLockTupleTuplock(rel, tup, mode) \
	ConditionalLockTuple((rel), (tup), tupleLockExtraInfo[mode].hwlock, false)

/*
 * Acquire heavyweight lock on the given tuple, in preparation for acquiring
 * its normal, Xmax-based tuple lock.
 *
 * have_tuple_lock is an input and output parameter: on input, it indicates
 * whether the lock has previously been acquired (and this function does
 * nothing in that case).  If this function returns success, have_tuple_lock
 * has been flipped to true.
 *
 * Returns false if it was unable to obtain the lock; this can only happen if
 * wait_policy is Skip.
 *
 * Note: This is intentionally identical to heap_acquire_tuplock. The logic
 * for acquiring heavyweight tuple locks is generic and not storage-specific,
 * but the heapam implementation is not exported as a public API. If it is
 * made available in the future, this duplicate can be removed.
 */

static bool
nx_acquire_tuplock(Relation relation, ItemPointer tid, LockTupleMode mode,
				   LockWaitPolicy wait_policy, bool *have_tuple_lock)
{
	if (*have_tuple_lock)
		return true;

	switch (wait_policy)
	{
		case LockWaitBlock:
			LockTupleTuplock(relation, tid, mode);
			break;

		case LockWaitSkip:
			if (!ConditionalLockTupleTuplock(relation, tid, mode))
				return false;
			break;

		case LockWaitError:
			if (!ConditionalLockTupleTuplock(relation, tid, mode))
				ereport(ERROR,
						(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
						 errmsg("could not obtain lock on row in relation \"%s\"",
								RelationGetRelationName(relation))));
			break;
	}
	*have_tuple_lock = true;

	return true;
}


static TM_Result
noxuam_lock_tuple(Relation relation, ItemPointer tid_p, Snapshot snapshot,
				  TupleTableSlot *slot, CommandId cid, LockTupleMode mode,
				  LockWaitPolicy wait_policy, uint8 flags,
				  TM_FailureData *tmfd)
{
	nxtid		tid = NXTidFromItemPointer(*tid_p);
	TransactionId xid = GetCurrentTransactionId();
	TM_Result	result;
	bool		this_xact_has_lock = false;
	bool		have_tuple_lock = false;
	nxtid		next_tid = tid;
	SnapshotData SnapshotDirty;
	bool		locked_something = false;
	NXUndoSlotVisibility *visi_info = &((NoxuTupleTableSlot *) slot)->visi_info_buf;
	bool		follow_updates = false;
	TransactionId priorXmax = InvalidTransactionId;

	slot->tts_tableOid = RelationGetRelid(relation);
	slot->tts_tid = *tid_p;

	tmfd->traversed = false;

	/*
	 * For now, we lock just the first attribute. As long as everyone does
	 * that, that's enough.
	 */
retry:
	result = nxbt_tid_lock(relation, tid, xid, cid, mode, follow_updates,
						   snapshot, tmfd, &next_tid, &this_xact_has_lock, visi_info);
	((NoxuTupleTableSlot *) slot)->visi_info = visi_info;

	/*
	 * Cross-check the tuple xmin against prior xmax, if any.  If we followed
	 * an update chain link and arrived at a tuple whose xmin doesn't match
	 * the xmax of the previous version, then the update chain is broken and
	 * we've reached an unrelated tuple.  Return the result as-is, matching
	 * heapam's behavior.
	 */
	if (TransactionIdIsValid(priorXmax) &&
		!TransactionIdEquals(priorXmax, visi_info->xmin))
	{
		return result;
	}

	if (result == TM_Invisible)
	{
		/*
		 * This is possible, but only when locking a tuple for ON CONFLICT
		 * UPDATE and some other cases handled below.  We return this value
		 * here rather than throwing an error in order to give that case the
		 * opportunity to throw a more specific error.
		 */
		/*
		 * This can also happen, if we're locking an UPDATE chain for KEY
		 * SHARE mode: A tuple has been inserted, and then updated, by a
		 * different transaction. The updating transaction is still in
		 * progress. We can lock the row in KEY SHARE mode, assuming the key
		 * columns were not updated, and we will try to lock all the row
		 * version, even the still in-progress UPDATEs. It's possible that the
		 * UPDATE aborts while we're chasing the update chain, so that the
		 * updated tuple becomes invisible to us. That's OK.
		 */
		if (mode == LockTupleKeyShare && locked_something)
			return TM_Ok;

		/*
		 * This can also happen, if the caller asked for the latest version of
		 * the tuple and if tuple was inserted by our own transaction, we have
		 * to check cmin against cid: cmin >= current CID means our command
		 * cannot see the tuple, so we should ignore it.
		 */
		Assert(visi_info->cmin != InvalidCommandId);
		if ((flags & TUPLE_LOCK_FLAG_FIND_LAST_VERSION) != 0 &&
			TransactionIdIsCurrentTransactionId(visi_info->xmin) &&
			visi_info->cmin >= cid)
		{
			tmfd->xmax = visi_info->xmin;
			tmfd->cmax = visi_info->cmin;
			return TM_SelfModified;
		}

		return TM_Invisible;
	}
	else if (result == TM_Updated ||
			 (result == TM_SelfModified && tmfd->cmax >= cid))
	{
		/*
		 * The other transaction is an update and it already committed.
		 *
		 * If the caller asked for the latest version, find it.
		 */
		if ((flags & TUPLE_LOCK_FLAG_FIND_LAST_VERSION) != 0 && next_tid != tid)
		{
			if (have_tuple_lock)
			{
				UnlockTupleTuplock(relation, tid_p, mode);
				have_tuple_lock = false;
			}

			if (ItemPointerIndicatesMovedPartitions(&tmfd->ctid))
				ereport(ERROR,
						(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
						 errmsg("tuple to be locked was already moved to another partition due to concurrent update")));

			/* it was updated, so look at the updated version */
			*tid_p = ItemPointerFromNXTid(next_tid);

			/* signal that a tuple later in the chain is getting locked */
			tmfd->traversed = true;

			/* loop back to fetch next in chain */

			/*
			 * Save the old tuple's xmax so we can cross-check it against the
			 * new tuple's xmin after re-fetching.  This matches heapam's
			 * priorXmax pattern for validating update chain consistency.
			 */
			priorXmax = visi_info->xmax;

			InitDirtySnapshot(SnapshotDirty);
			snapshot = &SnapshotDirty;
			tid = next_tid;
			goto retry;
		}

		return result;
	}
	else if (result == TM_Deleted)
	{
		/*
		 * The other transaction is a delete and it already committed.
		 */
		return result;
	}
	else if (result == TM_BeingModified)
	{
		TransactionId xwait = tmfd->xmax;

		/*
		 * Acquire tuple lock to establish our priority for the tuple, or die
		 * trying.  LockTuple will release us when we are next-in-line for the
		 * tuple.  We must do this even if we are share-locking, but not if we
		 * already have a weaker lock on the tuple.
		 *
		 * If we are forced to "start over" below, we keep the tuple lock;
		 * this arranges that we stay at the head of the line while rechecking
		 * tuple state.
		 *
		 * Explanation for why we don't acquire heavy-weight lock when we
		 * already hold a weaker lock:
		 *
		 * Disable acquisition of the heavyweight tuple lock. Otherwise, when
		 * promoting a weaker lock, we might deadlock with another locker that
		 * has acquired the heavyweight tuple lock and is waiting for our
		 * transaction to finish.
		 *
		 * Note that in this case we still need to wait for the xid if
		 * required, to avoid acquiring conflicting locks.
		 *
		 */
		if (!this_xact_has_lock &&
			!nx_acquire_tuplock(relation, tid_p, mode, wait_policy,
								&have_tuple_lock))
		{
			/*
			 * This can only happen if wait_policy is Skip and the lock
			 * couldn't be obtained.
			 */
			return TM_WouldBlock;
		}

		/* wait for regular transaction to end, or die trying */
		switch (wait_policy)
		{
			case LockWaitBlock:
				XactLockTableWait(xwait, relation, tid_p, XLTW_Lock);
				break;
			case LockWaitSkip:
				if (!ConditionalXactLockTableWait(xwait, false))
				{
					/*
					 * Release the heavyweight tuple lock if we hold it.
					 * nx_acquire_tuplock() successfully acquired this lock
					 * before we attempted the conditional xact wait, so we
					 * must release it on this early-return path to avoid
					 * leaking the lock.  This matches heapam's behavior
					 * in heap_lock_tuple().
					 */
					if (have_tuple_lock)
					{
						UnlockTupleTuplock(relation, tid_p, mode);
						have_tuple_lock = false;
					}
					return TM_WouldBlock;
				}
				break;
			case LockWaitError:
				if (!ConditionalXactLockTableWait(xwait, false))
					ereport(ERROR,
							(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
							 errmsg("could not obtain lock on row in relation \"%s\"",
									RelationGetRelationName(relation))));
				break;
		}

		/*
		 * xwait is done. Retry.
		 */
		goto retry;
	}
	if (result == TM_Ok)
		locked_something = true;

	/*
	 * Now that we have successfully marked the tuple as locked, we can
	 * release the lmgr tuple lock, if we had it.
	 */
	if (have_tuple_lock)
	{
		UnlockTupleTuplock(relation, tid_p, mode);
		have_tuple_lock = false;
	}

	if (mode == LockTupleKeyShare)
	{
		/* lock all row versions, if it's a KEY SHARE lock */
		follow_updates = (flags & TUPLE_LOCK_FLAG_LOCK_UPDATE_IN_PROGRESS) != 0;
		if (result == TM_Ok && tid != next_tid && next_tid != InvalidNXTid)
		{
			priorXmax = visi_info->xmax;
			tid = next_tid;
			goto retry;
		}
	}

	/* Fetch the tuple, too. */
	if (!noxuam_fetch_row_version(relation, tid_p, SnapshotAny, slot))
		elog(ERROR, "could not fetch locked tuple");

	return TM_Ok;
}

/* like heap_tuple_attr_equals */
static bool
nx_tuple_attr_equals(int attrnum, TupleTableSlot *slot1, TupleTableSlot *slot2)
{
	TupleDesc	tupdesc = slot1->tts_tupleDescriptor;
	Datum		value1,
				value2;
	bool		isnull1,
				isnull2;
	Form_pg_attribute att;

	/*
	 * If it's a whole-tuple reference, say "not equal".  It's not really
	 * worth supporting this case, since it could only succeed after a no-op
	 * update, which is hardly a case worth optimizing for.
	 */
	if (attrnum == 0)
		return false;

	/*
	 * Likewise, automatically say "not equal" for any system attribute other
	 * than tableOID; we cannot expect these to be consistent in a HOT chain,
	 * or even to be set correctly yet in the new tuple.
	 */
	if (attrnum < 0)
	{
		if (attrnum != TableOidAttributeNumber)
			return false;
	}

	/*
	 * Extract the corresponding values. This is O(n) per column for
	 * many indexed columns, since slot_getattr may deform up to the
	 * requested attribute each time. A bulk deform approach could be
	 * more efficient, but it doesn't work for system columns and this
	 * mirrors the heapam implementation.
	 */
	value1 = slot_getattr(slot1, attrnum, &isnull1);
	value2 = slot_getattr(slot2, attrnum, &isnull2);

	/*
	 * If one value is NULL and other is not, then they are certainly not
	 * equal
	 */
	if (isnull1 != isnull2)
		return false;

	/*
	 * If both are NULL, they can be considered equal.
	 */
	if (isnull1)
		return true;

	/*
	 * We do simple binary comparison of the two datums.  This may be overly
	 * strict because there can be multiple binary representations for the
	 * same logical value.  But we should be OK as long as there are no false
	 * positives.  Using a type-specific equality operator is messy because
	 * there could be multiple notions of equality in different operator
	 * classes; furthermore, we cannot safely invoke user-defined functions
	 * while holding exclusive buffer lock.
	 */
	if (attrnum <= 0)
	{
		/* The only allowed system columns are OIDs, so do this */
		return (DatumGetObjectId(value1) == DatumGetObjectId(value2));
	}
	else
	{
		Assert(attrnum <= tupdesc->natts);
		att = TupleDescAttr(tupdesc, attrnum - 1);
		return datumIsEqual(value1, value2, att->attbyval, att->attlen);
	}
}

static bool
is_key_update(Relation relation, TupleTableSlot *oldslot, TupleTableSlot *newslot)
{
	Bitmapset  *key_attrs;
	Bitmapset  *interesting_attrs;
	Bitmapset  *modified_attrs;
	int			attnum;

	/*
	 * Fetch the list of attributes to be checked for various operations.
	 *
	 * For HOT considerations, this is wasted effort if we fail to update or
	 * have to put the new tuple on a different page.  But we must compute the
	 * list before obtaining buffer lock --- in the worst case, if we are
	 * doing an update on one of the relevant system catalogs, we could
	 * deadlock if we try to fetch the list later.  In any case, the relcache
	 * caches the data so this is usually pretty cheap.
	 *
	 * We also need columns used by the replica identity and columns that are
	 * considered the "key" of rows in the table.
	 *
	 * Note that we get copies of each bitmap, so we need not worry about
	 * relcache flush happening midway through.
	 */
	key_attrs = RelationGetIndexAttrBitmap(relation, INDEX_ATTR_BITMAP_KEY);

	interesting_attrs = NULL;
	interesting_attrs = bms_add_members(interesting_attrs, key_attrs);

	/* Determine columns modified by the update. */
	modified_attrs = NULL;
	attnum = -1;
	while ((attnum = bms_next_member(interesting_attrs, attnum)) >= 0)
	{
		attnum += FirstLowInvalidHeapAttributeNumber;

		if (!nx_tuple_attr_equals(attnum, oldslot, newslot))
			modified_attrs = bms_add_member(modified_attrs,
											attnum - FirstLowInvalidHeapAttributeNumber);
	}

	return bms_overlap(modified_attrs, key_attrs);
}

/*
 * Compute which columns changed between old and new tuple.
 *
 * Returns the number of changed columns. The changed_cols array
 * (caller-allocated, natts elements) is filled with true/false for
 * each attribute.
 */
static int
nx_compute_changed_columns(Relation relation,
						   TupleTableSlot *oldslot,
						   TupleTableSlot *newslot,
						   bool *changed_cols)
{
	int			natts = relation->rd_att->natts;
	int			nchanged = 0;

	for (int attno = 1; attno <= natts; attno++)
	{
		if (!nx_tuple_attr_equals(attno, oldslot, newslot))
		{
			changed_cols[attno - 1] = true;
			nchanged++;
		}
		else
			changed_cols[attno - 1] = false;
	}
	return nchanged;
}

/*
 * Materialize carried-forward column values during VACUUM.
 *
 * When a column-delta UPDATE skips B-tree inserts for unchanged columns,
 * those values still need to be materialized into the new TID's column
 * B-trees before the predecessor TID can be vacuumed away.
 *
 * For chained delta updates, this follows the predecessor chain until
 * it finds the column value or reaches the end of the chain.
 */
#define NX_MAX_PREDECESSOR_DEPTH 10

void
nx_materialize_delta_columns(Relation rel,
							 nxtid newtid,
							 nxtid predecessor_tid,
							 int natts,
							 const uint32 *changed_cols)
{
	TupleDesc	tupdesc = rel->rd_att;
	MemoryContext oldcontext;

	/* Use transaction context to ensure datum copies survive */
	oldcontext = MemoryContextSwitchTo(CurTransactionContext);

	for (int attno = 1; attno <= natts; attno++)
	{
		int			idx = (attno - 1) / 32;
		int			bit = (attno - 1) % 32;
		Datum		datum;
		bool		isnull;
		nxtid		current_tid;
		int			depth;
		bool		found = false;

		/* Skip columns that were changed (already in B-tree) */
		if (changed_cols[idx] & (1U << bit))
			continue;

		/* Initialize to safe defaults before fetch attempt */
		datum = (Datum) 0;
		isnull = true;

		/*
		 * Follow predecessor chain to find the column value. For chained
		 * delta updates, the immediate predecessor might also be a delta
		 * without this column, so we keep following the chain.
		 */
		current_tid = predecessor_tid;
		for (depth = 0; depth < NX_MAX_PREDECESSOR_DEPTH; depth++)
		{
			NXAttrTreeScan scan;

			nxbt_attr_begin_scan(rel, tupdesc, (AttrNumber) attno, &scan);
			if (nxbt_attr_fetch(&scan, &datum, &isnull, current_tid))
			{
				/*
				 * Found the column value. CRITICAL: Copy non-byval datums
				 * before ending the scan, as they point into a pinned buffer
				 * that will be unpinned when we end the scan.
				 */
				if (!isnull && !scan.attdesc->attbyval)
					datum = nx_datumCopy(datum, scan.attdesc->attbyval,
										 scan.attdesc->attlen);
				nxbt_attr_end_scan(&scan);
				found = true;
				break;
			}
			nxbt_attr_end_scan(&scan);

			/*
			 * Column not in this TID. Check if it has a DELTA_INSERT UNDO
			 * record pointing to a predecessor we can follow.
			 */
			{
				NXTidTreeScan tidscan;
				nxtid		found_tid;
				uint8		slotno;
				RelUndoRecPtr undoptr;
				RelUndoRecordHeader header;
				void	   *payload = NULL;
				Size		payload_size;
				bool		follow_predecessor = false;

				nxbt_tid_begin_scan(rel, current_tid, current_tid + 1,
									SnapshotAny, &tidscan);
				found_tid = nxbt_tid_scan_next(&tidscan,
											   ForwardScanDirection);
				if (found_tid != InvalidNXTid)
				{
					slotno = NXTidScanCurUndoSlotNo(&tidscan);
					undoptr = tidscan.array_iter.undoslots[slotno];

					if (RelUndoRecPtrIsValid(undoptr))
					{
						if (RelUndoReadRecord(rel, undoptr, &header, &payload, &payload_size))
						{
							/*
							 * Skip past lock and update records to find the
							 * underlying DELTA_INSERT.  A chained delta
							 * update leaves UPDATE and TUPLE_LOCK records
							 * ahead of the DELTA_INSERT in the UNDO chain.
							 */
							while (header.urec_type == RELUNDO_TUPLE_LOCK ||
								   header.urec_type == RELUNDO_UPDATE)
							{
								RelUndoRecPtr prev = header.urec_prevundorec;

								if (payload)
								{
									pfree(payload);
									payload = NULL;
								}
								if (!RelUndoRecPtrIsValid(prev))
									break;

								if (!RelUndoReadRecord(rel, prev, &header, &payload, &payload_size))
									break;
							}

							if (header.urec_type == RELUNDO_DELTA_INSERT && payload != NULL)
							{
								NXRelUndoDeltaInsertPayload *delta =
									(NXRelUndoDeltaInsertPayload *) payload;

								/*
								 * If this column wasn't changed in the delta,
								 * follow the predecessor chain.
								 */
								if (!nx_relundo_delta_col_is_changed(delta, attno))
								{
									current_tid = delta->predecessor_tid;
									follow_predecessor = true;
								}
							}

							if (payload != NULL)
								pfree(payload);
						}
					}
				}
				nxbt_tid_end_scan(&tidscan);

				if (!follow_predecessor)
					break;
			}
		}

		if (!found)
		{
			/*
			 * Column not found after following predecessor chain. Use missing
			 * attribute default.
			 */
			nxbt_fill_missing_attribute_value(tupdesc, attno,
											  &datum, &isnull);
		}

		/* Insert into new TID's column B-tree */
		nxbt_attr_multi_insert(rel, (AttrNumber) attno,
							   &datum, &isnull, &newtid, 1);
	}

	MemoryContextSwitchTo(oldcontext);
}

/*
 * Column-delta UPDATE threshold.
 *
 * If more than this fraction of columns changed, fall back to full
 * tuple replacement (no delta optimization). The delta path has
 * overhead from UNDO record expansion and potential VACUUM-time
 * materialization, so it's only beneficial when the update is
 * truly partial.
 */
#define NX_DELTA_UPDATE_THRESHOLD	0.5

static TM_Result
noxuam_update(Relation relation, ItemPointer otid_p, TupleTableSlot *slot,
			  CommandId cid, uint32 options, Snapshot snapshot,
			  Snapshot crosscheck, bool wait, TM_FailureData *hufd,
			  LockTupleMode *lockmode, TU_UpdateIndexes *update_indexes)
{
	nxtid		otid = NXTidFromItemPointer(*otid_p);
	TransactionId xid = GetCurrentTransactionId();
	AttrNumber	attno;
	bool		key_update;
	Datum	   *d;
	bool	   *isnulls;
	TM_Result	result;
	nxtid		newtid;
	TupleTableSlot *oldslot;
	IndexFetchTableData *fetcher;
	MemoryContext oldcontext;
	MemoryContext insert_mcontext;
	bool		this_xact_has_lock = false;
	bool		have_tuple_lock = false;
	int			natts;
	bool	   *changed_cols;
	int			nchanged;
	bool		use_delta;

	/*
	 * Allocate changed_cols buffer once in parent context, outside retry
	 * loop, so it persists across retries even when insert_mcontext is reset.
	 */
	natts = relation->rd_att->natts;
	changed_cols = palloc(natts * sizeof(bool));

	/*
	 * Update code performs many small allocations for creating and merging
	 * items, proportional to the number of columns. Using a dedicated memory
	 * context allows efficient bulk cleanup via MemoryContextDelete rather
	 * than tracking individual pfree calls.
	 */
	insert_mcontext = AllocSetContextCreate(CurrentMemoryContext,
											"NoxuAMContext",
											ALLOCSET_DEFAULT_SIZES);
	oldcontext = MemoryContextSwitchTo(insert_mcontext);

	slot_getallattrs(slot);
	d = slot->tts_values;
	isnulls = slot->tts_isnull;

	oldslot = table_slot_create(relation, NULL);
	fetcher = noxuam_begin_index_fetch(relation, 0);

	/*
	 * The meta-attribute holds the visibility information, including the
	 * "t_ctid" pointer to the updated version. All the real attributes are
	 * just inserted, as if for a new row.
	 */
retry:
	/*
	 * Note: We don't reset insert_mcontext here to avoid complications with
	 * partially-constructed state. Memory allocated during retries will
	 * accumulate in insert_mcontext but is freed when the context is deleted
	 * at function end. The changed_cols buffer is allocated in parent context
	 * so it persists across retries without being freed/reallocated.
	 */
	newtid = InvalidNXTid;

	/*
	 * Fetch the old row, so that we can figure out which columns were
	 * modified.
	 *
	 * We always fetch the version at otid_p (which may have been advanced
	 * along the update chain on a previous iteration -- see the TM_Updated
	 * handling below).  We use SnapshotAny because the tuple at otid_p is
	 * either the original target or a successor that we've already validated
	 * through the chain-following logic.
	 */
	INJECTION_POINT("noxu_update-before-pin", NULL);
	if (!noxuam_fetch_row((NoxuIndexFetchData *) fetcher,
						  otid_p, SnapshotAny, oldslot))
	{
		return TM_Invisible;
	}
	key_update = is_key_update(relation, oldslot, slot);

	*lockmode = key_update ? LockTupleExclusive : LockTupleNoKeyExclusive;

	/*
	 * Compute which columns actually changed, for column-delta optimization.
	 * If fewer than half the columns changed, use the delta path to reduce
	 * WAL volume. The changed_cols buffer was pre-allocated before retry.
	 */
	nchanged = nx_compute_changed_columns(relation, oldslot,
										  slot, changed_cols);
	use_delta = (natts > 1 &&
				 nchanged < natts * NX_DELTA_UPDATE_THRESHOLD);

	if (use_delta)
		{
			result = nxbt_tid_delta_update(relation, otid,
										   xid, cid, key_update,
										   snapshot, crosscheck,
										   wait, hufd, &newtid,
										   &this_xact_has_lock,
										   natts, changed_cols);
		}
		else
		{
			result = nxbt_tid_update(relation, otid,
									 xid, cid, key_update,
									 snapshot, crosscheck,
									 wait, hufd, &newtid,
									 &this_xact_has_lock);
		}

		*update_indexes = (result == TM_Ok) ? TU_All : TU_None;
		if (result == TM_Ok)
		{
			CheckForSerializableConflictIn(relation, otid_p,
										   ItemPointerGetBlockNumber(otid_p));

			for (attno = 1; attno <= natts; attno++)
			{
				Form_pg_attribute attr;
				Datum		newdatum;
				bool		newisnull;

				/*
				 * Delta path: skip unchanged columns. Their values will be
				 * fetched from the predecessor TID instead.
				 */
				if (use_delta && !changed_cols[attno - 1])
					continue;

				attr = TupleDescAttr(relation->rd_att, attno - 1);
				newdatum = d[attno - 1];
				newisnull = isnulls[attno - 1];

				if (!newisnull && attr->attlen < 0 &&
					VARATT_IS_EXTERNAL((struct varlena *)
									   DatumGetPointer(newdatum)))
				{
					newdatum = PointerGetDatum(
											   detoast_external_attr(
																	 (struct varlena *)
																	 DatumGetPointer(newdatum)));
				}

				if (!newisnull && attr->attlen < 0 &&
					VARSIZE_ANY_EXHDR((struct varlena *)
									  DatumGetPointer(newdatum)) >
					MaxNoxuDatumSize)
				{
					newdatum = noxu_overflow_datum(relation,
												   attno, newdatum, newtid);
				}

				nxbt_attr_multi_insert(relation, (AttrNumber) attno,
									   &newdatum, &newisnull,
									   &newtid, 1);
			}

			slot->tts_tableOid = RelationGetRelid(relation);
			slot->tts_tid = ItemPointerFromNXTid(newtid);

			pgstat_count_heap_update(relation, false, false);

			nxstats_count_insert(
								 RelationGetRelid(relation), 1);
			nxstats_count_delete(
								 RelationGetRelid(relation));
		}
		else
		{
			if (result == TM_Invisible)
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("attempted to update invisible tuple")));
			else if (result == TM_BeingModified && wait)
			{
				TransactionId xwait = hufd->xmax;

				if (!TransactionIdIsCurrentTransactionId(xwait))
				{
					if (!this_xact_has_lock)
					{
						nx_acquire_tuplock(relation, otid_p,
										   LockTupleExclusive,
										   LockWaitBlock,
										   &have_tuple_lock);
					}

					XactLockTableWait(xwait, relation,
									  otid_p, XLTW_Update);
					goto retry;
				}
			}
			else if (result == TM_Updated)
			{
				/*
				 * The tuple was updated by a concurrent transaction that
				 * has already committed.  Follow the update chain to the
				 * latest committed version so that the executor's EPQ
				 * (EvalPlanQual) re-check operates on current data.
				 *
				 * We advance otid/otid_p to the successor and retry.
				 * On the next iteration noxuam_fetch_row will read the
				 * latest version's column data, giving correct results
				 * for key_update and the delta-column computation.  If
				 * the successor itself has been updated or is being
				 * modified, the normal TM_Updated / TM_BeingModified
				 * handling will kick in again.
				 *
				 * This mirrors the chain-following logic in
				 * noxuam_lock_tuple() (TUPLE_LOCK_FLAG_FIND_LAST_VERSION)
				 * and heapam's heap_lock_updated_tuple_rec().
				 *
				 * We must release any heavyweight tuple lock held on
				 * the old TID before advancing, to avoid holding locks
				 * on versions we no longer intend to update.
				 */
				if (have_tuple_lock)
				{
					UnlockTupleTuplock(relation, otid_p, LockTupleExclusive);
					have_tuple_lock = false;
				}

				*otid_p = hufd->ctid;
				otid = NXTidFromItemPointer(*otid_p);
				goto retry;
			}
		}

		pfree(changed_cols);

	/*
	 * Now that we have successfully updated the tuple, we can release the
	 * lmgr tuple lock, if we had it.
	 */
	if (have_tuple_lock)
	{
		UnlockTupleTuplock(relation, otid_p, LockTupleExclusive);
		have_tuple_lock = false;
	}

	noxuam_end_index_fetch(fetcher);
	ExecDropSingleTupleTableSlot(oldslot);

	MemoryContextSwitchTo(oldcontext);
	MemoryContextDelete(insert_mcontext);

	return result;
}

static const TupleTableSlotOps *
noxuam_slot_callbacks(Relation relation)
{
	(void) relation;
	return &TTSOpsNoxu;
}

static void
nx_initialize_proj_attributes(TupleDesc tupledesc, NoxuProjectData * proj_data)
{
	MemoryContext oldcontext;

	if (proj_data->num_proj_atts != 0)
		return;

	oldcontext = MemoryContextSwitchTo(proj_data->context);
	/* add one for meta-attribute */
	proj_data->proj_atts = palloc((tupledesc->natts + 1) * sizeof(int));
	proj_data->attr_scans = palloc0(tupledesc->natts * sizeof(NXAttrTreeScan));
	proj_data->tid_scan.active = false;

	proj_data->proj_atts[proj_data->num_proj_atts++] = NX_META_ATTRIBUTE_NUM;

	/*
	 * convert booleans array into an array of the attribute numbers of the
	 * required columns.
	 */
	for (int idx = 0; idx < tupledesc->natts; idx++)
	{
		int			att_no = idx + 1;

		/*
		 * never project dropped columns, null will be returned for them in
		 * slot by default.
		 */
		if (TupleDescAttr(tupledesc, idx)->attisdropped)
			continue;

		/* project_columns empty also conveys need all the columns */
		if (proj_data->project_columns == NULL ||
			bms_is_member(att_no, proj_data->project_columns))
			proj_data->proj_atts[proj_data->num_proj_atts++] = att_no;
	}

	MemoryContextSwitchTo(oldcontext);
}

static void
nx_initialize_proj_attributes_extended(NoxuDesc scan, TupleDesc tupledesc)
{
	MemoryContext oldcontext;
	NoxuProjectData *proj_data = &scan->proj_data;

	/* if already initialized return */
	if (proj_data->num_proj_atts != 0)
		return;

	nx_initialize_proj_attributes(tupledesc, proj_data);

	oldcontext = MemoryContextSwitchTo(proj_data->context);
	/* Extra setup for bitmap, sample, and analyze scans */
	if ((scan->rs_scan.rs_flags & SO_TYPE_BITMAPSCAN) ||
		(scan->rs_scan.rs_flags & SO_TYPE_SAMPLESCAN) ||
		(scan->rs_scan.rs_flags & SO_TYPE_ANALYZE))
	{
		int			nattrs;

		scan->bmscan_ntuples = 0;
		scan->bmscan_tids = palloc(MAX_ITEMS_PER_LOGICAL_BLOCK * sizeof(nxtid));

		/*
		 * For ANALYZE scans, num_proj_atts is still 0 at this point. Allocate
		 * arrays for all attributes (+ 1 for meta-attribute).
		 */
		nattrs = (scan->rs_scan.rs_flags & SO_TYPE_ANALYZE) ?
			scan->rs_scan.rs_rd->rd_att->natts + 1 : proj_data->num_proj_atts;

		scan->bmscan_datums = palloc(nattrs * sizeof(Datum *));
		scan->bmscan_isnulls = palloc(nattrs * sizeof(bool *));
		for (int i = 0; i < nattrs; i++)
		{
			scan->bmscan_datums[i] = palloc(MAX_ITEMS_PER_LOGICAL_BLOCK * sizeof(Datum));
			scan->bmscan_isnulls[i] = palloc(MAX_ITEMS_PER_LOGICAL_BLOCK * sizeof(bool));
		}
	}
	MemoryContextSwitchTo(oldcontext);
}

static TableScanDesc
noxuam_beginscan_with_column_projection(Relation relation, Snapshot snapshot,
										int nkeys, ScanKey key,
										ParallelTableScanDesc parallel_scan,
										uint32 flags,
										Bitmapset *project_columns)
{
	NoxuDesc	scan;

	(void) key;

	/* Sample scans have no snapshot, but we need one */
	if (!snapshot)
	{
		Assert(!(flags & SO_TYPE_SAMPLESCAN));
		snapshot = SnapshotAny;
	}

	/*
	 * allocate and initialize scan descriptor
	 */
	scan = (NoxuDesc) palloc0(sizeof(NoxuDescData));

	scan->rs_scan.rs_rd = relation;
	scan->rs_scan.rs_snapshot = snapshot;
	scan->rs_scan.rs_nkeys = nkeys;
	scan->rs_scan.rs_flags = flags;
	scan->rs_scan.rs_parallel = parallel_scan;

	/*
	 * Initialize recent_oldest_undo early to avoid assertion failures if
	 * visibility checks happen before the first getnextslot() call. This will
	 * be updated again when nxbt_tid_begin_scan() is called.
	 */
	scan->proj_data.tid_scan.recent_oldest_undo = nx_get_oldest_visible_undo_ptr(relation);

	/*
	 * Snapshot the maximum TID at scan start to prevent the Halloween
	 * Problem.  Without this, an UPDATE scan can see TIDs allocated by
	 * its own INSERT operations and attempt to update them again.
	 * nxbt_get_last_tid() returns one past the last existing TID.
	 */
	scan->max_tid_to_scan = nxbt_get_last_tid(relation);

	/*
	 * we can use page-at-a-time mode if it's an MVCC-safe snapshot
	 */

	/*
	 * we do this here instead of in initscan() because heap_rescan also calls
	 * initscan() and we don't want to allocate memory again
	 */
	if (nkeys > 0)
		scan->rs_scan.rs_key = (ScanKey) palloc(sizeof(ScanKeyData) * nkeys);
	else
		scan->rs_scan.rs_key = NULL;

	scan->proj_data.context = CurrentMemoryContext;
	scan->proj_data.project_columns = project_columns;

	/*
	 * Flush the nursery before scanning.  Since TID tree entries exist for
	 * nursery-buffered rows but their attribute data is only in memory,
	 * the attribute B-tree scan would find TIDs but miss column values.
	 * Flushing ensures attribute data is in the B-trees before the scan.
	 *
	 * When LSM is enabled, also force-merge any pending Level 1 segments
	 * into the B-tree so the sequential scan can find all data.
	 */
	scan->nursery_scan_idx = -1;
	scan->nursery_flushed = false;
	if (noxu_nursery_enabled)
	{
		NXNurseryBuffer *nursery = nx_nursery_get(relation);

		if (nursery != NULL && nursery->nrows > 0)
		{
			/*
			 * For scan-triggered flushes, always flush to B-tree regardless
			 * of noxu.lsm_enabled.  This ensures the scan sees all data
			 * through the normal attribute B-tree path.  LSM Level 1 is
			 * only used for capacity-triggered flushes during inserts.
			 */
			nx_nursery_flush_to_btree(relation, nursery);
			nx_nursery_reset_internal(nursery);
			scan->nursery_flushed = true;
		}
	}

	/*
	 * Force all LSM Level 1 data into the B-tree.  This first merges
	 * any complete A+B pairs, then flushes any remaining lone segments.
	 * After this, all attribute data is in the B-tree for the scan.
	 */
	if (noxu_lsm_enabled)
		nx_lsm_flush_all_to_btree(relation);

	/*
	 * For a seqscan in a serializable transaction, acquire a predicate lock
	 * on the entire relation. This is required not only to lock all the
	 * matching tuples, but also to conflict with new insertions into the
	 * table. In an indexscan, we take page locks on the index pages covering
	 * the range specified in the scan qual, but in a heap scan there is
	 * nothing more fine-grained to lock. A bitmap scan is a different story,
	 * there we have already scanned the index and locked the index pages
	 * covering the predicate. But in that case we still have to lock any
	 * matching heap tuples.
	 */
	if (!(flags & SO_TYPE_BITMAPSCAN) &&
		!(flags & SO_TYPE_ANALYZE))
		PredicateLockRelation(relation, snapshot);

	/*
	 * Currently, we don't have a stats counter for bitmap heap scans (but the
	 * underlying bitmap index scans will be counted) or sample scans (we only
	 * update stats for tuple fetches there)
	 */
	if (!(flags & SO_TYPE_BITMAPSCAN) && !(flags & SO_TYPE_SAMPLESCAN))
	{
		pgstat_count_heap_scan(relation);
		nxstats_scan_begin(RelationGetRelid(relation));
	}

	return (TableScanDesc) scan;
}

static TableScanDesc
noxuam_beginscan(Relation relation, Snapshot snapshot,
				 int nkeys, ScanKey key,
				 ParallelTableScanDesc parallel_scan,
				 uint32 flags)
{
	return noxuam_beginscan_with_column_projection(relation, snapshot,
												   nkeys, key, parallel_scan, flags, NULL);
}

static void
noxuam_endscan(TableScanDesc sscan)
{
	NoxuDesc	scan = (NoxuDesc) sscan;
	NoxuProjectData *proj_data = &scan->proj_data;

	/* Flush opportunistic scan statistics */
	nxstats_scan_end(RelationGetRelid(scan->rs_scan.rs_rd));

	if (proj_data->proj_atts)
		pfree(proj_data->proj_atts);

	if (proj_data->num_proj_atts > 0)
	{
		nxbt_tid_end_scan(&proj_data->tid_scan);
		for (int i = 1; i < proj_data->num_proj_atts; i++)
			nxbt_attr_end_scan(&proj_data->attr_scans[i - 1]);
	}

	if (scan->rs_scan.rs_flags & SO_TEMP_SNAPSHOT)
		UnregisterSnapshot(scan->rs_scan.rs_snapshot);

	if (proj_data->attr_scans)
		pfree(proj_data->attr_scans);
	pfree(scan);
}

static void
noxuam_rescan(TableScanDesc sscan, struct ScanKeyData *key,
			  bool set_params, bool allow_strat,
			  bool allow_sync, bool allow_pagemode)
{
	NoxuDesc	scan = (NoxuDesc) sscan;

	(void) key;

	/* these params don't do much in noxu yet, but whatever */
	if (set_params)
	{
		if (allow_strat)
			scan->rs_scan.rs_flags |= SO_ALLOW_STRAT;
		else
			scan->rs_scan.rs_flags &= ~SO_ALLOW_STRAT;

		if (allow_sync)
			scan->rs_scan.rs_flags |= SO_ALLOW_SYNC;
		else
			scan->rs_scan.rs_flags &= ~SO_ALLOW_SYNC;

		if (allow_pagemode && scan->rs_scan.rs_snapshot &&
			IsMVCCSnapshot(scan->rs_scan.rs_snapshot))
			scan->rs_scan.rs_flags |= SO_ALLOW_PAGEMODE;
		else
			scan->rs_scan.rs_flags &= ~SO_ALLOW_PAGEMODE;
	}

	if (scan->proj_data.num_proj_atts > 0 && scan->started)
	{
		nxbt_tid_reset_scan(scan->rs_scan.rs_rd, &scan->proj_data.tid_scan,
							scan->cur_range_start, scan->cur_range_end, scan->cur_range_start - 1);
	}
	scan->started = false;
}

static bool
noxuam_getnextslot(TableScanDesc sscan, ScanDirection direction,
				   TupleTableSlot *slot)
{
	NoxuDesc	scan = (NoxuDesc) sscan;
	NoxuProjectData *scan_proj = &scan->proj_data;
	int			slot_natts = slot->tts_tupleDescriptor->natts;
	Datum	   *slot_values = slot->tts_values;
	bool	   *slot_isnull = slot->tts_isnull;
	nxtid		this_tid;
	Datum		datum;
	bool		isnull;
	NXUndoSlotVisibility *visi_info;
	uint8		slotno;
	MemoryContext oldcontext;

	if (direction != ForwardScanDirection && scan->rs_scan.rs_parallel)
		elog(ERROR, "parallel backward scan not implemented");

	if (!scan->started)
	{
		nx_initialize_proj_attributes(slot->tts_tupleDescriptor, scan_proj);

		if (scan->rs_scan.rs_parallel)
		{
			/* Allocate next range of TIDs to scan */
			if (!nx_parallelscan_nextrange(scan->rs_scan.rs_rd,
										   (ParallelNXScanDesc) scan->rs_scan.rs_parallel,
										   &scan->cur_range_start, &scan->cur_range_end))
			{
				ExecClearTuple(slot);
				return false;
			}
		}
		else
		{
			scan->cur_range_start = MinNXTid;

			/*
			 * Use the max TID snapshotted at scan start to prevent the
			 * Halloween Problem: without this bound, the scan would visit
			 * TIDs that were allocated by INSERT operations from the same
			 * UPDATE/DELETE command, causing phantom re-updates.
			 */
			scan->cur_range_end = scan->max_tid_to_scan;
		}

		oldcontext = MemoryContextSwitchTo(scan_proj->context);
		nxbt_tid_begin_scan(scan->rs_scan.rs_rd,
							scan->cur_range_start,
							scan->cur_range_end,
							scan->rs_scan.rs_snapshot,
							&scan_proj->tid_scan);
		scan_proj->tid_scan.serializable = true;
		for (int i = 1; i < scan_proj->num_proj_atts; i++)
		{
			int			attno = scan_proj->proj_atts[i];

			nxbt_attr_begin_scan(scan->rs_scan.rs_rd,
								 slot->tts_tupleDescriptor,
								 (AttrNumber) attno,
								 &scan_proj->attr_scans[i - 1]);
		}
		MemoryContextSwitchTo(oldcontext);
		scan->started = true;
	}
	Assert((scan_proj->num_proj_atts - 1) <= slot_natts);

	/*
	 * Initialize the slot.
	 *
	 * We initialize all columns to NULL. The values for columns that are
	 * projected will be set to the actual values below, but it's important
	 * that non-projected columns are NULL.
	 */
	ExecClearTuple(slot);
	for (int i = 0; i < slot_natts; i++)
		slot_isnull[i] = true;

	/*
	 * Find the next visible TID.
	 */
	for (;;)
	{
		this_tid = nxbt_tid_scan_next(&scan_proj->tid_scan, direction);
		if (this_tid == InvalidNXTid)
		{
			if (scan->rs_scan.rs_parallel)
			{
				/* Allocate next range of TIDs to scan */
				if (!nx_parallelscan_nextrange(scan->rs_scan.rs_rd,
											   (ParallelNXScanDesc) scan->rs_scan.rs_parallel,
											   &scan->cur_range_start, &scan->cur_range_end))
				{
					ExecClearTuple(slot);
					return false;
				}

				nxbt_tid_reset_scan(scan->rs_scan.rs_rd, &scan_proj->tid_scan,
									scan->cur_range_start, scan->cur_range_end, scan->cur_range_start - 1);
				continue;
			}
			else
			{
				ExecClearTuple(slot);
				return false;
			}
		}
		Assert(this_tid < scan->cur_range_end);
		break;
	}

	/*
	 * Note: We don't need to predicate-lock tuples in Serializable mode,
	 * because in a sequential scan, we predicate-locked the whole table.
	 */

	/*
	 * Initialize all slot positions to NULL. The loop below will overwrite
	 * projected columns with actual values.
	 */
	for (int i = 0; i < slot_natts; i++)
	{
		slot_values[i] = (Datum) 0;
		slot_isnull[i] = true;
	}

	/*
	 * CRITICAL: Switch to slot's memory context for datum copies. This
	 * ensures nx_datumCopy() allocates in the correct context.
	 */
	oldcontext = MemoryContextSwitchTo(slot->tts_mcxt);

	/* Fetch the datums of each attribute for this row */
	for (int i = 1; i < scan_proj->num_proj_atts; i++)
	{
		NXAttrTreeScan *btscan = &scan_proj->attr_scans[i - 1];
		Form_pg_attribute attr = btscan->attdesc;
		int			natt;

		/* Initialize to safe defaults before fetch attempt */
		datum = (Datum) 0;
		isnull = true;

		if (!nxbt_attr_fetch(btscan, &datum, &isnull, this_tid))
		{
			/*
			 * Column not found. Try predecessor chain for delta updates, then
			 * fall back to missing attribute value.
			 */
			nx_fetch_attr_with_predecessor(scan->rs_scan.rs_rd,
										   slot->tts_tupleDescriptor,
										   btscan->attno, this_tid,
										   &datum, &isnull);
		}

		/*
		 * Flatten any overflow values, because the rest of the system doesn't
		 * know how to deal with them.
		 */
		natt = scan_proj->proj_atts[i];

		if (!isnull && attr->attlen == -1 &&
			VARATT_IS_EXTERNAL((struct varlena *) DatumGetPointer(datum)) && VARTAG_EXTERNAL((struct varlena *) DatumGetPointer(datum)) == VARTAG_NOXU)
		{
			datum = noxu_overflow_flatten(scan->rs_scan.rs_rd, (AttrNumber) natt, this_tid, datum);
		}

		/* Check that the values coming out of the b-tree are aligned properly */
		if (!isnull && attr->attlen == -1)
		{
			Assert(VARATT_IS_1B(datum) || INTALIGN(datum) == datum);
		}

		/*
		 * CRITICAL: Copy non-byval datums to avoid dangling pointers. When
		 * ExecSort materializes tuples after scan completes, the B-tree scan
		 * buffers will be unpinned. Without copying, slots would hold
		 * pointers to freed memory.
		 */
		if (!isnull && !attr->attbyval)
			datum = nx_datumCopy(datum, attr->attbyval, attr->attlen);

		Assert(natt > 0);
		slot_values[natt - 1] = datum;
		slot_isnull[natt - 1] = isnull;
	}

	/* Restore previous memory context */
	MemoryContextSwitchTo(oldcontext);

	/* Fill in the rest of the fields in the slot, and return the tuple */
	slotno = NXTidScanCurUndoSlotNo(&scan_proj->tid_scan);
	visi_info = &scan_proj->tid_scan.array_iter.undoslot_visibility[slotno];
	((NoxuTupleTableSlot *) slot)->visi_info = visi_info;

	slot->tts_tableOid = RelationGetRelid(scan->rs_scan.rs_rd);
	slot->tts_tid = ItemPointerFromNXTid(this_tid);
	slot->tts_nvalid = (AttrNumber) slot->tts_tupleDescriptor->natts;
	slot->tts_flags &= ~TTS_FLAG_EMPTY;

	pgstat_count_heap_getnext(scan->rs_scan.rs_rd);

	/* Opportunistic stats: observe this live tuple */
	nxstats_scan_observe_tuple(RelationGetRelid(scan->rs_scan.rs_rd),
							   true, slot_isnull, slot_natts);

	return true;
}

static bool
noxuam_tuple_tid_valid(TableScanDesc sscan, ItemPointer tid)
{
	NoxuDesc	scan = (NoxuDesc) sscan;
	nxtid		ztid = NXTidFromItemPointer(*tid);

	if (scan->max_tid_to_scan == InvalidNXTid)
	{
		/*
		 * get the max tid once and store it
		 */
		scan->max_tid_to_scan = nxbt_get_last_tid(sscan->rs_rd);
	}

	/*
	 * We only check the upper bound here. Fetching the lowest TID would
	 * add another B-tree lookup on every bitmap scan start, and TIDs
	 * below the minimum are rare in practice since they only arise from
	 * empty tables or after bulk deletes. The cost is not justified.
	 */
	if (ztid <= scan->max_tid_to_scan)
		return true;
	else
		return false;
}

static bool
noxuam_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot,
								Snapshot snapshot)
{
	/*
	 * Visibility information is not retained in the standard TupleTableSlot,
	 * so we must re-fetch it from the TID tree. NoxuTupleTableSlot carries
	 * a visi_info pointer during scans, but that points into the scan's
	 * working memory and is not valid here. A fully custom slot type that
	 * embeds visibility data could avoid this extra lookup, but the current
	 * approach keeps the slot implementation simpler.
	 */
	nxtid		tid = NXTidFromItemPointer(slot->tts_tid);
	NXTidTreeScan meta_scan;
	bool		found;

	/* Use the meta-data tree for the visibility information. */
	nxbt_tid_begin_scan(rel, tid, tid + 1, snapshot, &meta_scan);

	found = nxbt_tid_scan_next(&meta_scan, ForwardScanDirection) != InvalidNXTid;

	nxbt_tid_end_scan(&meta_scan);

	return found;
}

/*
 * noxuam_scan_set_tidrange - Set the range of TIDs to scan
 *
 * This is used for bitmap heap scans to efficiently scan a specific
 * range of TIDs.
 */
static void
noxuam_scan_set_tidrange(TableScanDesc sscan,
						 ItemPointer mintid,
						 ItemPointer maxtid)
{
	NoxuDesc	scan = (NoxuDesc) sscan;
	nxtid		start_tid;
	nxtid		end_tid;

	/*
	 * Convert ItemPointers to nxtids. Handle cases where TIDs are beyond
	 * table boundaries or mintid > maxtid as required by the API.
	 */
	if (mintid)
		start_tid = NXTidFromItemPointer(*mintid);
	else
		start_tid = MinNXTid;

	if (maxtid)
		end_tid = NXTidFromItemPointer(*maxtid) + 1;	/* inclusive ->
														 * exclusive */
	else
		end_tid = MaxPlusOneNXTid;

	/*
	 * If mintid > maxtid, set an invalid range so getnextslot returns no
	 * tuples
	 */
	if (start_tid > end_tid)
	{
		scan->cur_range_start = MinNXTid;
		scan->cur_range_end = MinNXTid; /* empty range */
	}
	else
	{
		scan->cur_range_start = start_tid;
		scan->cur_range_end = end_tid;
	}

	/* Mark scan as not started so getnextslot_tidrange initializes properly */
	scan->started = false;
}

/*
 * noxuam_scan_getnextslot_tidrange - Get next tuple in TID range
 *
 * Returns the next tuple within the TID range set by scan_set_tidrange.
 * This is similar to noxuam_getnextslot but operates within a fixed TID range.
 */
static bool
noxuam_scan_getnextslot_tidrange(TableScanDesc sscan,
								 ScanDirection direction,
								 TupleTableSlot *slot)
{
	NoxuDesc	scan = (NoxuDesc) sscan;
	NoxuProjectData *scan_proj = &scan->proj_data;
	int			slot_natts = slot->tts_tupleDescriptor->natts;
	Datum	   *slot_values = slot->tts_values;
	bool	   *slot_isnull = slot->tts_isnull;
	nxtid		this_tid;
	Datum		datum;
	bool		isnull;
	MemoryContext oldcontext;

	if (direction != ForwardScanDirection)
		elog(ERROR, "TID range scan does not support backward scan");

	/* Initialize scan on first call */
	if (!scan->started)
	{

		nx_initialize_proj_attributes(slot->tts_tupleDescriptor, scan_proj);

		oldcontext = MemoryContextSwitchTo(scan_proj->context);
		nxbt_tid_begin_scan(scan->rs_scan.rs_rd,
							scan->cur_range_start,
							scan->cur_range_end,
							scan->rs_scan.rs_snapshot,
							&scan_proj->tid_scan);
		for (int i = 1; i < scan_proj->num_proj_atts; i++)
		{
			int			attno = scan_proj->proj_atts[i];

			nxbt_attr_begin_scan(scan->rs_scan.rs_rd,
								 slot->tts_tupleDescriptor,
								 (AttrNumber) attno,
								 &scan_proj->attr_scans[i - 1]);
		}
		MemoryContextSwitchTo(oldcontext);
		scan->started = true;
	}
	Assert((scan_proj->num_proj_atts - 1) <= slot_natts);

	/* Initialize the slot - set all columns to NULL */
	ExecClearTuple(slot);
	for (int i = 0; i < slot_natts; i++)
		slot_isnull[i] = true;

	/* Find the next visible TID in range */
	this_tid = nxbt_tid_scan_next(&scan_proj->tid_scan, direction);
	if (this_tid == InvalidNXTid)
	{
		ExecClearTuple(slot);
		return false;
	}
	Assert(this_tid < scan->cur_range_end);

	/*
	 * CRITICAL: Switch to slot's memory context for datum copies. This
	 * ensures nx_datumCopy() allocates in the correct context.
	 */
	oldcontext = MemoryContextSwitchTo(slot->tts_mcxt);

	/* Fetch the datums of each attribute for this row */
	for (int i = 1; i < scan_proj->num_proj_atts; i++)
	{
		NXAttrTreeScan *btscan = &scan_proj->attr_scans[i - 1];
		Form_pg_attribute attr = btscan->attdesc;
		int			natt = scan_proj->proj_atts[i];

		/* Initialize to safe defaults before fetch attempt */
		datum = (Datum) 0;
		isnull = true;

		if (!nxbt_attr_fetch(btscan, &datum, &isnull, this_tid))
			nx_fetch_attr_with_predecessor(scan->rs_scan.rs_rd,
										   slot->tts_tupleDescriptor,
										   btscan->attno, this_tid,
										   &datum, &isnull);

		/*
		 * Flatten any noxu-overflow values, because the rest of the system
		 * doesn't know how to deal with them.
		 */
		if (!isnull && attr->attlen == -1 &&
			VARATT_IS_EXTERNAL((struct varlena *) DatumGetPointer(datum)) &&
			VARTAG_EXTERNAL((struct varlena *) DatumGetPointer(datum)) == VARTAG_NOXU)
		{
			datum = noxu_overflow_flatten(scan->rs_scan.rs_rd, (AttrNumber) natt, this_tid, datum);
		}

		/*
		 * CRITICAL: Copy non-byval datums to avoid dangling pointers. Same
		 * issue as non-parallel scan - must copy before storing in slot.
		 */
		if (!isnull && !attr->attbyval)
			datum = nx_datumCopy(datum, attr->attbyval, attr->attlen);

		slot_values[natt - 1] = datum;
		slot_isnull[natt - 1] = isnull;
	}

	/* Restore previous memory context */
	MemoryContextSwitchTo(oldcontext);

	/* Fill in the rest of the fields in the slot, and return the tuple */
	{
		uint8		slotno;
		NXUndoSlotVisibility *visi_info;

		slotno = NXTidScanCurUndoSlotNo(&scan_proj->tid_scan);
		visi_info = &scan_proj->tid_scan.array_iter.undoslot_visibility[slotno];
		((NoxuTupleTableSlot *) slot)->visi_info = visi_info;

		slot->tts_tableOid = RelationGetRelid(scan->rs_scan.rs_rd);
		slot->tts_tid = ItemPointerFromNXTid(this_tid);
	}

	ExecStoreVirtualTuple(slot);

	return true;
}


static IndexFetchTableData *
noxuam_begin_index_fetch(Relation rel, uint32 flags)
{
	NoxuIndexFetch idxscan = palloc0(sizeof(NoxuIndexFetchData));

	(void) flags;				/* Unused for now */

	idxscan->idx_fetch_data.rel = rel;
	idxscan->proj_data.context = CurrentMemoryContext;

	return (IndexFetchTableData *) idxscan;
}


static void
noxuam_reset_index_fetch(IndexFetchTableData *scan)
{
	(void) scan;

	/*
	 * Resetting an index fetch is a no-op. The scan state is lightweight
	 * and will be fully cleaned up in noxuam_end_index_fetch. The B-tree
	 * scans are lazily initialized on first use, so there is nothing to
	 * reset here.
	 */
}

static void
noxuam_end_index_fetch(IndexFetchTableData *scan)
{
	NoxuIndexFetch idxscan = (NoxuIndexFetch) scan;
	NoxuProjectData *nxscan_proj = &idxscan->proj_data;

	if (nxscan_proj->num_proj_atts > 0)
	{
		nxbt_tid_end_scan(&nxscan_proj->tid_scan);
		for (int i = 1; i < nxscan_proj->num_proj_atts; i++)
			nxbt_attr_end_scan(&nxscan_proj->attr_scans[i - 1]);
	}

	if (nxscan_proj->proj_atts)
		pfree(nxscan_proj->proj_atts);

	if (nxscan_proj->attr_scans)
		pfree(nxscan_proj->attr_scans);
	pfree(idxscan);
}

static bool
noxuam_index_fetch_tuple(struct IndexFetchTableData *scan,
						 ItemPointer tid_p,
						 Snapshot snapshot,
						 TupleTableSlot *slot,
						 bool *call_again, bool *all_dead)
{
	bool		result;

	/*
	 * we don't do in-place updates, so this is essentially the same as
	 * fetch_row_version.
	 */
	if (call_again)
		*call_again = false;
	if (all_dead)
		*all_dead = false;

	result = noxuam_fetch_row((NoxuIndexFetchData *) scan, tid_p, snapshot, slot);
	return result;
}

/*
 * Shared implementation of fetch_row_version and index_fetch_tuple callbacks.
 */
static bool
noxuam_fetch_row(NoxuIndexFetchData * fetch,
				 ItemPointer tid_p,
				 Snapshot snapshot,
				 TupleTableSlot *slot)
{
	Relation	rel = fetch->idx_fetch_data.rel;
	nxtid		tid = NXTidFromItemPointer(*tid_p);
	bool		found = true;
	NoxuProjectData *fetch_proj = &fetch->proj_data;

	/*
	 * Check if the TID is in the nursery or Level 1 LSM segments.
	 *
	 * When the nursery is enabled, attribute data for recently inserted
	 * rows may be buffered in memory (nursery) or in Level 1 row-oriented
	 * pages (when LSM is enabled).  We check both before falling through
	 * to the B-tree.
	 *
	 * When LSM is disabled, flush the nursery to the B-tree so the
	 * subsequent B-tree fetch can find the data.
	 */
	if (noxu_nursery_enabled)
	{
		NXNurseryBuffer *nursery = nx_nursery_get(rel);

		/* First try a direct nursery lookup (no flush needed) */
		if (nursery != NULL && nursery->nrows > 0)
		{
			if (nx_nursery_lookup_tid(nursery, tid, slot, rel))
				return true;

			/*
			 * Not in nursery.  When LSM is disabled, flush to B-tree so
			 * the B-tree fetch below can find recently-flushed data.
			 * When LSM is enabled, data goes to Level 1 (checked below).
			 */
			if (!noxu_lsm_enabled)
				nx_nursery_flush(rel, nursery);
		}
	}

	/* Check Level 1 LSM segments for the TID */
	if (noxu_lsm_enabled && nx_lsm_lookup_tid(rel, tid, slot))
		return true;

	/* first time here, initialize */
	if (fetch_proj->num_proj_atts == 0)
		nx_initialize_proj_attributes(slot->tts_tupleDescriptor, fetch_proj);
	else
	{
		/* If we had a previous fetches still open, close them first */
		nxbt_tid_end_scan(&fetch_proj->tid_scan);
		for (int i = 1; i < fetch_proj->num_proj_atts; i++)
			nxbt_attr_end_scan(&fetch_proj->attr_scans[i - 1]);
	}

	/*
	 * Initialize the slot.
	 *
	 * If we're not fetching all columns, initialize the unfetched values in
	 * the slot to NULL. (Actually, this initializes all to NULL, and the code
	 * below will overwrite them for the columns that are projected)
	 */
	ExecClearTuple(slot);
	for (int i = 0; i < slot->tts_tupleDescriptor->natts; i++)
		slot->tts_isnull[i] = true;

	/*
	 * Acquire the predicate lock before the visibility check and
	 * CheckForSerializableConflictOut(), matching the ordering used
	 * by heapam in heap_fetch().  This ensures that if the tuple is
	 * visible, the predicate lock is already held before the SSI
	 * conflict check runs, closing a window where a concurrent
	 * transaction could commit a conflicting write undetected.
	 *
	 * Acquiring the lock speculatively (before confirming visibility)
	 * is safe: a false-positive predicate lock on a non-visible tuple
	 * may cause an unnecessary serialization failure but cannot miss
	 * a real conflict.
	 */
	PredicateLockTID(rel, tid_p, snapshot, InvalidTransactionId);

	nxbt_tid_begin_scan(rel, tid, tid + 1, snapshot, &fetch_proj->tid_scan);
	fetch_proj->tid_scan.serializable = true;
	found = nxbt_tid_scan_next(&fetch_proj->tid_scan, ForwardScanDirection) != InvalidNXTid;
	if (found)
	{
		MemoryContext oldcontext = MemoryContextSwitchTo(slot->tts_mcxt);

		for (int i = 1; i < fetch_proj->num_proj_atts; i++)
		{
			int			natt = fetch_proj->proj_atts[i];
			NXAttrTreeScan *btscan = &fetch_proj->attr_scans[i - 1];
			Form_pg_attribute attr;
			Datum		datum = (Datum) 0;
			bool		isnull = true;

			nxbt_attr_begin_scan(rel, slot->tts_tupleDescriptor, (AttrNumber) natt, btscan);
			attr = btscan->attdesc;
			if (nxbt_attr_fetch(btscan, &datum, &isnull, tid))
			{
				/*
				 * flatten any overflow values, because the rest of the system
				 * doesn't know how to deal with them.
				 */
				if (!isnull && attr->attlen == -1 &&
					VARATT_IS_EXTERNAL((struct varlena *) DatumGetPointer(datum)) && VARTAG_EXTERNAL((struct varlena *) DatumGetPointer(datum)) == VARTAG_NOXU)
				{
					datum = noxu_overflow_flatten(rel, (AttrNumber) natt, tid, datum);
				}
			}
			else
				nx_fetch_attr_with_predecessor(rel,
											   slot->tts_tupleDescriptor,
											   btscan->attno, tid,
											   &datum, &isnull);

			/*
			 * CRITICAL: Copy non-byval datums to slot's memory context. The
			 * datum may point into a pinned buffer that will be unpinned when
			 * this scan is closed on the next fetch_row call.
			 */
			if (!isnull && !attr->attbyval)
				datum = nx_datumCopy(datum, attr->attbyval, attr->attlen);

			slot->tts_values[natt - 1] = datum;
			slot->tts_isnull[natt - 1] = isnull;
		}

		MemoryContextSwitchTo(oldcontext);
	}

	if (found)
	{
		NXUndoSlotVisibility *visi_info;
		uint8		slotno = NXTidScanCurUndoSlotNo(&fetch_proj->tid_scan);

		visi_info = &fetch_proj->tid_scan.array_iter.undoslot_visibility[slotno];
		((NoxuTupleTableSlot *) slot)->visi_info = visi_info;

		slot->tts_tableOid = RelationGetRelid(rel);
		slot->tts_tid = ItemPointerFromNXTid(tid);
		slot->tts_nvalid = slot->tts_tupleDescriptor->natts;
		slot->tts_flags &= ~TTS_FLAG_EMPTY;
		return true;
	}

	return false;
}

static void
noxuam_index_validate_scan(Relation baseRelation,
						   Relation indexRelation,
						   IndexInfo *indexInfo,
						   Snapshot snapshot,
						   ValidateIndexState *state)
{
	Datum		values[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS];
	ExprState  *predicate;
	TupleTableSlot *slot;
	EState	   *estate;
	ExprContext *econtext;
	int			attno;
	TableScanDesc scan;
	ItemPointerData idx_ptr;
	bool		tuplesort_empty = false;
	Bitmapset  *proj = NULL;

	/*
	 * sanity checks
	 */
	Assert(OidIsValid(indexRelation->rd_rel->relam));

	/*
	 * Need an EState for evaluation of index expressions and partial-index
	 * predicates.  Also a slot to hold the current tuple.
	 */
	estate = CreateExecutorState();
	econtext = GetPerTupleExprContext(estate);
	slot = table_slot_create(baseRelation, NULL);

	/* Arrange for econtext's scan tuple to be the tuple under test */
	econtext->ecxt_scantuple = slot;

	/* Set up execution state for predicate, if any. */
	predicate = ExecPrepareQual(indexInfo->ii_Predicate, estate);

	/*
	 * Prepare for scan of the base relation.  We need just those tuples
	 * satisfying the passed-in reference snapshot.  We must disable syncscan
	 * here, because it's critical that we read from block zero forward to
	 * match the sorted TIDs.
	 */

	/*
	 * Build a projection bitmap containing only the columns needed for the
	 * index. This allows us to skip fetching unreferenced columns.
	 */
	for (attno = 0; attno < indexInfo->ii_NumIndexKeyAttrs; attno++)
	{
		Assert(indexInfo->ii_IndexAttrNumbers[attno] <= baseRelation->rd_att->natts);
		proj = bms_add_member(proj, indexInfo->ii_IndexAttrNumbers[attno]);
	}

	/* Use column projection to only fetch the columns needed for the index */
	scan = (TableScanDesc) noxuam_beginscan_with_column_projection(
																   baseRelation, snapshot, 0, NULL, NULL,
																   SO_TYPE_SEQSCAN | SO_ALLOW_SYNC, proj);

	/*
	 * Scan all tuples matching the snapshot.
	 */
	ItemPointerSet(&idx_ptr, 0, 0); /* this is less than any real TID */
	while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
	{
		ItemPointerData tup_ptr = slot->tts_tid;
		int			cmp;

		CHECK_FOR_INTERRUPTS();

		/*
		 * Noxu does not currently support in-place updates (HOT-equivalent).
		 * When/if that is added, this will need to handle update chains
		 * similarly to heapam's validate_index logic.
		 */

		MemoryContextReset(econtext->ecxt_per_tuple_memory);

		if (tuplesort_empty)
			cmp = -1;
		else
		{
			while ((cmp = ItemPointerCompare(&tup_ptr, &idx_ptr)) > 0)
			{
				Datum		ts_val;
				bool		ts_isnull;

				tuplesort_empty = !tuplesort_getdatum(state->tuplesort, true, false,
													  &ts_val, &ts_isnull, NULL);
				if (!tuplesort_empty)
				{
					Assert(!ts_isnull);
					itemptr_decode(&idx_ptr, DatumGetInt64(ts_val));

					/* If int8 is pass-by-ref, free (encoded) TID Datum memory */
#ifndef USE_FLOAT8_BYVAL
					pfree(DatumGetPointer(ts_val));
#endif
					break;
				}
				else
				{
					/* Be tidy */
					ItemPointerSetInvalid(&idx_ptr);
					cmp = -1;
				}
			}
		}
		if (cmp < 0)
		{
			/* This item is not in the index */

			/*
			 * In a partial index, discard tuples that don't satisfy the
			 * predicate.
			 */
			if (predicate != NULL)
			{
				if (!ExecQual(predicate, econtext))
					continue;
			}

			/*
			 * For the current heap tuple, extract all the attributes we use
			 * in this index, and note which are null.  This also performs
			 * evaluation of any expressions needed.
			 */
			FormIndexDatum(indexInfo,
						   slot,
						   estate,
						   values,
						   isnull);

			/* Call the AM's callback routine to process the tuple */
			index_insert(indexRelation, values, isnull, &tup_ptr, baseRelation,
						 indexInfo->ii_Unique ?
						 UNIQUE_CHECK_YES : UNIQUE_CHECK_NO,
						 false,
						 indexInfo);

			state->tups_inserted += 1;
		}
	}

	table_endscan(scan);

	ExecDropSingleTupleTableSlot(slot);

	FreeExecutorState(estate);

	/* These may have been pointing to the now-gone estate */
	indexInfo->ii_ExpressionsState = NIL;
	indexInfo->ii_PredicateState = NULL;
}

/*
 * noxuam_index_delete_tuples
 *
 * Bottom-up index deletion optimization callback.
 *
 * Determines which index entries point to vacuumable table tuples. The index
 * AM calls this to check whether TIDs from its index page can be deleted.
 * We mark deletable entries in delstate->status and return a snapshot
 * conflict horizon for WAL logging.
 *
 * Unlike heap, Noxu doesn't have HOT chains, so this is simpler - we just
 * check if each TID is visible to any non-vacuumable snapshot.
 */
static TransactionId
noxuam_index_delete_tuples(Relation rel, TM_IndexDeleteOp *delstate)
{
	TransactionId snapshotConflictHorizon = InvalidTransactionId;
	SnapshotData SnapshotNonVacuumable;
	int			finalndeltids = 0;

	/*
	 * Initialize a snapshot that considers any tuple visible to a running
	 * transaction as non-vacuumable.
	 */
	InitNonVacuumableSnapshot(SnapshotNonVacuumable, GlobalVisTestFor(rel));

	/*
	 * Iterate through all TIDs the index AM wants to delete.
	 */
	for (int i = 0; i < delstate->ndeltids; i++)
	{
		TM_IndexDelete *ideltid = &delstate->deltids[i];
		TM_IndexStatus *istatus = delstate->status + ideltid->id;
		ItemPointer htid = &ideltid->tid;
		nxtid		tid;
		NXTidTreeScan meta_scan;
		bool		tuple_exists;

		/*
		 * If caller already knows this is deletable (e.g., from earlier
		 * pruning), skip the visibility check.
		 */
		if (istatus->knowndeletable)
		{
			Assert(!delstate->bottomup);
			finalndeltids++;
			continue;
		}

		/* Convert ItemPointer to nxtid */
		tid = NXTidFromItemPointer(*htid);

		/*
		 * Check if this tuple is visible to any non-vacuumable snapshot. We
		 * use the TID tree scan to get visibility information.
		 */
		nxbt_tid_begin_scan(rel, tid, tid + 1, &SnapshotNonVacuumable, &meta_scan);
		tuple_exists = (nxbt_tid_scan_next(&meta_scan, ForwardScanDirection) != InvalidNXTid);

		if (tuple_exists)
		{
			/* Tuple is visible to someone, can't delete it */
			nxbt_tid_end_scan(&meta_scan);
			continue;
		}

		nxbt_tid_end_scan(&meta_scan);

		/*
		 * Tuple is not visible to any non-vacuumable snapshot, so it's safe
		 * to delete the index entry.
		 */
		istatus->knowndeletable = true;
		finalndeltids++;

		/*
		 * For bottom-up deletion, track how much free space we've
		 * accumulated. If we've freed enough, we can stop early.
		 */
		if (delstate->bottomup)
		{
			static int	actualfreespace = 0;

			Assert(istatus->freespace > 0);
			actualfreespace += istatus->freespace;
			if (actualfreespace >= delstate->bottomupfreespace)
			{
				/*
				 * We've freed enough space. Mark remaining entries as not
				 * deletable and break.
				 */
				for (int j = i + 1; j < delstate->ndeltids; j++)
				{
					TM_IndexDelete *remaining = &delstate->deltids[j];
					TM_IndexStatus *rstatus = delstate->status + remaining->id;

					rstatus->knowndeletable = false;
				}
				break;
			}
		}

		/*
		 * Update the snapshot conflict horizon for this deletion operation.
		 * For Noxu, we need to check the UNDO records to find the XID that
		 * created/modified this tuple.
		 *
		 * Ideally, we would scan the undo chain for the specific TID to find
		 * the oldest XID that needs to be considered. For now, we use a
		 * conservative approach with GetOldestNonRemovableTransactionId,
		 * which is safe but may retain index entries longer than necessary.
		 */
		if (!TransactionIdIsValid(snapshotConflictHorizon))
		{
			/*
			 * Use GetOldestNonRemovableTransactionId as a conservative
			 * conflict horizon. This ensures we don't break snapshot
			 * isolation.
			 */
			snapshotConflictHorizon = GetOldestNonRemovableTransactionId(rel);
		}
	}

	/*
	 * If no entries were marked deletable, return InvalidTransactionId to
	 * indicate no conflict horizon is needed.
	 */
	if (finalndeltids == 0)
		return InvalidTransactionId;

	return snapshotConflictHorizon;
}

static double
noxuam_index_build_range_scan(Relation baseRelation,
							  Relation indexRelation,
							  IndexInfo *indexInfo,
							  bool allow_sync,
							  bool anyvisible,
							  bool progress,
							  BlockNumber start_blockno,
							  BlockNumber numblocks,
							  IndexBuildCallback callback,
							  void *callback_state,
							  TableScanDesc scan)
{
	Datum		values[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS];
	double		reltuples;
	ExprState  *predicate;
	TupleTableSlot *slot;
	EState	   *estate;
	ExprContext *econtext;
	Snapshot	snapshot;
	SnapshotData NonVacuumableSnapshot;
	bool		need_unregister_snapshot = false;
	TransactionId OldestXmin;
	bool		tupleIsAlive;
	GlobalVisState *vistest = NULL;

#ifdef USE_ASSERT_CHECKING
	bool		checking_uniqueness;
#endif

	(void) progress;

#ifdef USE_ASSERT_CHECKING
	/* See whether we're verifying uniqueness/exclusion properties */
	checking_uniqueness = (indexInfo->ii_Unique ||
						   indexInfo->ii_ExclusionOps != NULL);

	/*
	 * "Any visible" mode is not compatible with uniqueness checks; make sure
	 * only one of those is requested.
	 */
	Assert(!(anyvisible && checking_uniqueness));
#endif

	/*
	 * sanity checks
	 */
	Assert(OidIsValid(indexRelation->rd_rel->relam));

	/*
	 * Need an EState for evaluation of index expressions and partial-index
	 * predicates.  Also a slot to hold the current tuple.
	 */
	estate = CreateExecutorState();
	econtext = GetPerTupleExprContext(estate);
	slot = table_slot_create(baseRelation, NULL);

	/* Arrange for econtext's scan tuple to be the tuple under test */
	econtext->ecxt_scantuple = slot;

	/* Set up execution state for predicate, if any. */
	predicate = ExecPrepareQual(indexInfo->ii_Predicate, estate);

	/*
	 * Prepare for scan of the base relation.  In a normal index build, we use
	 * SnapshotAny because we must retrieve all tuples and do our own time
	 * qual checks (because we have to index RECENTLY_DEAD tuples). In a
	 * concurrent build, or during bootstrap, we take a regular MVCC snapshot
	 * and index whatever's live according to that.
	 */

	/* okay to ignore lazy VACUUMs here */
	if (!IsBootstrapProcessingMode() && !indexInfo->ii_Concurrent)
	{
		vistest = GlobalVisTestFor(baseRelation);
		OldestXmin = GetOldestNonRemovableTransactionId(baseRelation);
	}
	else
	{
		OldestXmin = InvalidTransactionId;
	}

	if (!scan)
	{
		int			attno;
		Bitmapset  *proj = NULL;

		/*
		 * Serial index build.
		 *
		 * Must begin our own noxu scan in this case.  We may also need to
		 * register a snapshot whose lifetime is under our direct control.
		 */
		if (vistest == NULL)
		{
			snapshot = RegisterSnapshot(GetTransactionSnapshot());
			need_unregister_snapshot = true;
		}
		else
		{
			/* leave out completely dead items even with SnapshotAny */
			InitNonVacuumableSnapshot(NonVacuumableSnapshot, vistest);
			snapshot = &NonVacuumableSnapshot;
		}

		/*
		 * Build a projection bitmap containing only the columns needed for
		 * the index. This improves performance for wide tables by skipping
		 * unreferenced columns.
		 */
		for (attno = 0; attno < indexInfo->ii_NumIndexKeyAttrs; attno++)
		{
			Assert(indexInfo->ii_IndexAttrNumbers[attno] <= baseRelation->rd_att->natts);
			proj = bms_add_member(proj, indexInfo->ii_IndexAttrNumbers[attno]);
		}

		/*
		 * Use column projection to only fetch the columns needed for the
		 * index
		 */
		scan = (TableScanDesc) noxuam_beginscan_with_column_projection(
																	   baseRelation, snapshot, 0, NULL, NULL,
																	   SO_TYPE_SEQSCAN | SO_ALLOW_SYNC, proj);

		if (start_blockno != 0 || numblocks != InvalidBlockNumber)
		{
			NoxuDesc	nxscan = (NoxuDesc) scan;
			NoxuProjectData *nxscan_proj = &nxscan->proj_data;

			nxscan->cur_range_start = NXTidFromBlkOff(start_blockno, 1);
			nxscan->cur_range_end = NXTidFromBlkOff(numblocks, 1);

			/*
			 * num_proj_atts can be 0 when the scan was started without a
			 * column projection (e.g., COUNT(*) queries). In that case,
			 * we only need the TID tree scan, not attribute tree scans.
			 */
			if (nxscan_proj->num_proj_atts > 0)
			{
				nxbt_tid_begin_scan(nxscan->rs_scan.rs_rd,
									nxscan->cur_range_start,
									nxscan->cur_range_end,
									nxscan->rs_scan.rs_snapshot,
									&nxscan_proj->tid_scan);
				for (int i = 1; i < nxscan_proj->num_proj_atts; i++)
				{
					int			natt = nxscan_proj->proj_atts[i];

					nxbt_attr_begin_scan(nxscan->rs_scan.rs_rd,
										 RelationGetDescr(nxscan->rs_scan.rs_rd),
										 natt,
										 &nxscan_proj->attr_scans[i - 1]);
				}
			}
		}
	}
	else
	{
		/*
		 * Parallel index build.
		 *
		 * Parallel case never registers/unregisters own snapshot.  Snapshot
		 * is taken from parallel noxu scan, and is SnapshotAny or an MVCC
		 * snapshot, based on same criteria as serial case.
		 */
		Assert(!IsBootstrapProcessingMode());
		Assert(allow_sync);
		Assert(start_blockno == 0);
		Assert(numblocks == InvalidBlockNumber);
		snapshot = scan->rs_snapshot;

		if (snapshot == SnapshotAny)
		{
			/* leave out completely dead items even with SnapshotAny */
			InitNonVacuumableSnapshot(NonVacuumableSnapshot, vistest);
			snapshot = &NonVacuumableSnapshot;
		}
	}

	/*
	 * Must call GetOldestXmin() with SnapshotAny.  Should never call
	 * GetOldestXmin() with MVCC snapshot. (It's especially worth checking
	 * this for parallel builds, since ambuild routines that support parallel
	 * builds must work these details out for themselves.)
	 */
	Assert(snapshot == &NonVacuumableSnapshot || IsMVCCSnapshot(snapshot));
	Assert(snapshot == &NonVacuumableSnapshot ? TransactionIdIsValid(OldestXmin) :
		   vistest == NULL);
	Assert(snapshot == &NonVacuumableSnapshot || !anyvisible);

	reltuples = 0;

	/*
	 * Scan all tuples in the base relation.
	 */
	while (noxuam_getnextslot(scan, ForwardScanDirection, slot))
	{
		HeapTuple	heapTuple;
		NXUndoSlotVisibility *visi_info;

		if (numblocks != InvalidBlockNumber &&
			ItemPointerGetBlockNumber(&slot->tts_tid) >= numblocks)
			break;

		CHECK_FOR_INTERRUPTS();

		/*
		 * Is the tuple deleted, but still visible to old transactions?
		 *
		 * We need to include such tuples in the index, but exclude them from
		 * unique-checking.
		 *
		 * In heapam, tuples with DELETE_IN_PROGRESS are checked separately.
		 * In Noxu, this is handled by the visi_info's nonvacuumable_status
		 * which already accounts for in-progress deletes during the scan.
		 */
		visi_info = ((NoxuTupleTableSlot *) slot)->visi_info;
		tupleIsAlive = (visi_info->nonvacuumable_status != NXNV_RECENTLY_DEAD);

		if (tupleIsAlive)
			reltuples += 1;

		/*
		 * Noxu does not currently support in-place updates (HOT-equivalent).
		 * Each update creates a new TID, so there is no update chain to
		 * follow here. If in-place updates are added, this will need to
		 * determine which tuple version to index.
		 */

		MemoryContextReset(econtext->ecxt_per_tuple_memory);

		/*
		 * In a partial index, discard tuples that don't satisfy the
		 * predicate.
		 */
		if (predicate != NULL)
		{
			if (!ExecQual(predicate, econtext))
				continue;
		}

		/*
		 * For the current heap tuple, extract all the attributes we use in
		 * this index, and note which are null.  This also performs evaluation
		 * of any expressions needed.
		 */
		FormIndexDatum(indexInfo,
					   slot,
					   estate,
					   values,
					   isnull);

		/* Call the AM's callback routine to process the tuple */
		heapTuple = ExecCopySlotHeapTuple(slot);
		heapTuple->t_self = slot->tts_tid;
		callback(indexRelation, &heapTuple->t_self, values, isnull, tupleIsAlive,
				 callback_state);
		pfree(heapTuple);
	}

	table_endscan(scan);

	/* we can now forget our snapshot, if set and registered by us */
	if (need_unregister_snapshot)
		UnregisterSnapshot(snapshot);

	ExecDropSingleTupleTableSlot(slot);

	FreeExecutorState(estate);

	/* These may have been pointing to the now-gone estate */
	indexInfo->ii_ExpressionsState = NIL;
	indexInfo->ii_PredicateState = NULL;

	return reltuples;
}

static void
noxuam_finish_bulk_insert(Relation relation, uint32 options)
{
	(void) options;

	/* Flush any remaining nursery data at end of bulk insert */
	if (noxu_nursery_enabled)
	{
		NXNurseryBuffer *nursery = nx_nursery_get(relation);

		if (nursery != NULL && nursery->nrows > 0)
			nx_nursery_flush(relation, nursery);
	}

	/*
	 * If we skipped writing WAL, then we need to sync the noxu (but not
	 * indexes since those use WAL anyway / don't go through tableam)
	 */
	if (!RelationNeedsWAL(relation))
		smgrimmedsync(RelationGetSmgr(relation), MAIN_FORKNUM);
}

/* ------------------------------------------------------------------------
 * DDL related callbacks for noxu AM.
 * ------------------------------------------------------------------------
 */

static void
noxuam_relation_set_new_filenode(Relation rel,
								 const RelFileLocator *newrnode,
								 char persistence,
								 TransactionId *freezeXid,
								 MultiXactId *minmulti)
{
	SMgrRelation srel;

	/*
	 * Initialize to the minimum XID that could put tuples in the table. We
	 * know that no xacts older than RecentXmin are still running, so that
	 * will do.
	 */
	*freezeXid = RecentXmin;

	/*
	 * Similarly, initialize the minimum Multixact to the first value that
	 * could possibly be stored in tuples in the table.  Running transactions
	 * could reuse values from their local cache, so we are careful to
	 * consider all currently running multis.
	 *
	 * This could be refined by tracking per-table multixact usage, but
	 * GetOldestMultiXactId is safe and the refinement is unlikely to
	 * matter in practice. This mirrors the heapam approach.
	 */
	*minmulti = GetOldestMultiXactId();

	srel = RelationCreateStorage(*newrnode, persistence, true);

	/*
	 * If required, set up an init fork for an unlogged table so that it can
	 * be correctly reinitialized on restart.  An immediate sync is required
	 * even if the page has been logged, because the write did not go through
	 * shared_buffers and therefore a concurrent checkpoint may have moved the
	 * redo pointer past our xlog record.  Recovery may as well remove it
	 * while replaying, for example, XLOG_DBASE_CREATE or XLOG_TBLSPC_CREATE
	 * record. Therefore, logging is necessary even if wal_level=minimal.
	 */
	if (persistence == RELPERSISTENCE_UNLOGGED)
	{
		Assert(rel->rd_rel->relkind == RELKIND_RELATION ||
			   rel->rd_rel->relkind == RELKIND_MATVIEW ||
			   rel->rd_rel->relkind == RELKIND_TOASTVALUE);
		smgrcreate(srel, INIT_FORKNUM, false);
		log_smgrcreate(newrnode, INIT_FORKNUM);
		smgrimmedsync(srel, INIT_FORKNUM);
	}

	/*
	 * Initialize the per-relation UNDO fork.  This creates the UNDO fork file
	 * and writes the initial metapage so that subsequent DML operations can
	 * reserve UNDO space via RelUndoReserve().
	 */
	RelUndoInitRelation(rel);
}

static void
noxuam_relation_nontransactional_truncate(Relation rel)
{
	nxmeta_invalidate_cache(rel);
	RelationTruncate(rel, 0);

	/*
	 * Re-initialize the per-relation UNDO fork after truncation.  The
	 * previous UNDO log is no longer relevant since all data was removed.
	 */
	RelUndoInitRelation(rel);
}

static void
noxuam_relation_copy_data(Relation rel, const RelFileLocator *newrnode)
{
	SMgrRelation dstrel;

	dstrel = smgropen(*newrnode, rel->rd_backend);
	RelationGetSmgr(rel);

	/*
	 * Since we copy the file directly without looking at the shared buffers,
	 * we'd better first flush out any pages of the source relation that are
	 * in shared buffers.  We assume no new changes will be made while we are
	 * holding exclusive lock on the rel.
	 */
	FlushRelationBuffers(rel);

	/*
	 * Create and copy all forks of the relation, and schedule unlinking of
	 * the old physical file.
	 *
	 * NOTE: any conflict in relfilenode value will be caught in
	 * RelationCreateStorage().
	 */
	RelationCreateStorage(*newrnode, rel->rd_rel->relpersistence, true);

	/* copy main fork */
	RelationCopyStorage(rel->rd_smgr, dstrel, MAIN_FORKNUM,
						rel->rd_rel->relpersistence);

	/* copy per-relation UNDO fork, if it exists */
	if (smgrexists(rel->rd_smgr, RELUNDO_FORKNUM))
	{
		smgrcreate(dstrel, RELUNDO_FORKNUM, false);
		RelationCopyStorage(rel->rd_smgr, dstrel, RELUNDO_FORKNUM,
							rel->rd_rel->relpersistence);
	}

	/* drop old relation, and close new one */
	RelationDropStorage(rel);
	smgrclose(dstrel);
}

/*
 * Subroutine of the noxuam_relation_copy_for_cluster() callback.
 *
 * Determines visibility of a tuple in the old table by following UNDO
 * records.  Returns true if the tuple is visible and should be copied,
 * false if it should be skipped.  On success, the output parameters
 * are filled with the visibility information.
 *
 * out_was_update and out_update_newtid are set when the xmax came from
 * an UPDATE record (as opposed to DELETE). out_update_newtid contains
 * the TID of the new row version in the old table, which is used by
 * the caller to reconstruct UPDATE chains in the new table.
 */
static bool
nx_cluster_check_visibility(Relation OldHeap,
							RelUndoRecPtr old_undoptr,
							RelUndoRecPtr recent_oldest_undo,
							TransactionId OldestXmin,
							TransactionId *out_xmin,
							CommandId *out_cmin,
							TransactionId *out_xmax,
							CommandId *out_cmax,
							bool *out_changedPart,
							bool *out_was_update,
							nxtid * out_update_newtid,
							bool *out_key_update,
							uint32 *out_speculative_token)
{
	TransactionId this_xmin;
	CommandId	this_cmin;
	TransactionId this_xmax;
	CommandId	this_cmax;
	bool		this_changedPart;
	bool		this_was_update;
	nxtid		this_update_newtid;
	bool		this_key_update;
	uint32		this_speculative_token;
	RelUndoRecPtr undo_ptr;
	RelUndoRecordHeader header;
	void	   *payload = NULL;
	Size		payload_size;

	/*
	 * Follow the chain of UNDO records for this tuple, to find the
	 * transaction that originally inserted the row  (xmin/cmin), and the
	 * transaction that deleted or updated it away, if any (xmax/cmax)
	 */
	this_xmin = FrozenTransactionId;
	this_cmin = InvalidCommandId;
	this_xmax = InvalidTransactionId;
	this_cmax = InvalidCommandId;
	this_changedPart = false;
	this_was_update = false;
	this_update_newtid = InvalidNXTid;
	this_key_update = false;
	this_speculative_token = INVALID_SPECULATIVE_TOKEN;

	undo_ptr = old_undoptr;
	for (;;)
	{
		if (RelUndoGetCounter(undo_ptr) < RelUndoGetCounter(recent_oldest_undo))
		{
			/* This tuple version is visible to everyone. */
			break;
		}

		/* Fetch the next UNDO record. */
		if (payload != NULL)
		{
			pfree(payload);
			payload = NULL;
		}
		if (!RelUndoReadRecord(OldHeap, undo_ptr, &header, &payload, &payload_size))
			break;

		if (RELUNDO_TYPE_IS_INSERT(header.urec_type))
		{
			if (!TransactionIdIsCurrentTransactionId(header.urec_xid) &&
				!TransactionIdIsInProgress(header.urec_xid) &&
				!TransactionIdDidCommit(header.urec_xid))
			{
				/*
				 * inserter aborted or crashed. This row is not visible to
				 * anyone. Including any later tuple versions we might have
				 * seen.
				 */
				this_xmin = InvalidTransactionId;
				break;
			}
			else
			{
				/* Inserter committed or still in progress. */
				this_xmin = header.urec_xid;
				this_cmin = header.urec_cid;

				/*
				 * Preserve the speculative token so the caller can
				 * propagate it to the new table during rewrites.
				 */
				if (header.urec_type == RELUNDO_INSERT)
				{
					RelUndoInsertPayload *ins_payload =
						(RelUndoInsertPayload *) payload;

					this_speculative_token = ins_payload->speculative_token;
				}
				else if (header.urec_type == RELUNDO_DELTA_INSERT)
				{
					NXRelUndoDeltaInsertPayload *di_payload =
						(NXRelUndoDeltaInsertPayload *) payload;

					this_speculative_token = di_payload->speculative_token;
				}

				/*
				 * we know everything there is to know about this tuple
				 * version.
				 */
				break;
			}
		}
		else if (header.urec_type == RELUNDO_TUPLE_LOCK)
		{
			/*
			 * Tuple locks are not propagated during REPACK/CLUSTER.
			 *
			 * This is acceptable because REPACK holds an AccessExclusiveLock
			 * on the relation, preventing concurrent lock acquisitions. Any
			 * tuple locks held before REPACK started will have been resolved
			 * (committed or aborted) by the time we process the undo chain,
			 * since we only process committed tuples visible to our snapshot.
			 */
			undo_ptr = header.urec_prevundorec;
			continue;
		}
		else if (header.urec_type == RELUNDO_DELETE ||
				 header.urec_type == RELUNDO_UPDATE)
		{
			/* Row was deleted (or updated away). */
			if (!TransactionIdIsCurrentTransactionId(header.urec_xid) &&
				!TransactionIdIsInProgress(header.urec_xid) &&
				!TransactionIdDidCommit(header.urec_xid))
			{
				/*
				 * deleter aborted or crashed. The previous record should be
				 * an insertion (possibly with some tuple-locking in between).
				 * We'll remember the tuple when we see the insertion.
				 */
				undo_ptr = header.urec_prevundorec;
				continue;
			}
			else
			{
				/* deleter committed or is still in progress. */
				if (TransactionIdPrecedes(header.urec_xid, OldestXmin))
				{
					/*
					 * the deletion is visible to everyone. We can skip the
					 * row completely.
					 */
					this_xmin = InvalidTransactionId;
					break;
				}
				else
				{
					/*
					 * deleter/updater committed or is in progress. Remember
					 * that it was deleted/updated by this XID.
					 */
					this_xmax = header.urec_xid;
					this_cmax = header.urec_cid;
					if (header.urec_type == RELUNDO_DELETE)
					{
						RelUndoDeletePayload *del_payload = (RelUndoDeletePayload *) payload;

						this_changedPart = del_payload->changedPart;
						this_was_update = false;
					}
					else
					{
						RelUndoUpdatePayload *upd_payload = (RelUndoUpdatePayload *) payload;

						this_changedPart = false;
						this_was_update = true;
						this_update_newtid = NXTidFromItemPointer(upd_payload->newtid);
						this_key_update = upd_payload->key_update;
					}

					/*
					 * follow the UNDO chain to find information about the
					 * inserting transaction (xmin/cmin)
					 */
					undo_ptr = header.urec_prevundorec;
					continue;
				}
			}
		}
	}

	if (payload != NULL)
		pfree(payload);

	if (this_xmin == InvalidTransactionId)
		return false;

	*out_xmin = this_xmin;
	*out_cmin = this_cmin;
	*out_xmax = this_xmax;
	*out_cmax = this_cmax;
	*out_changedPart = this_changedPart;
	*out_was_update = this_was_update;
	*out_update_newtid = this_update_newtid;
	*out_key_update = this_key_update;
	*out_speculative_token = this_speculative_token;
	return true;
}

/*
 * nx_cluster_write_tuple
 *
 * Write a tuple with the given visibility info into the new table.
 * Returns the new TID, or InvalidNXTid on failure.
 */
static nxtid
nx_cluster_write_tuple(Relation NewHeap,
					   TransactionId this_xmin, CommandId this_cmin,
					   TransactionId this_xmax, CommandId this_cmax,
					   bool this_changedPart,
					   uint32 speculative_token)
{
	nxtid		newtid = InvalidNXTid;

	/* Insert the first version of the row. */
	nxbt_tid_multi_insert(NewHeap,
						  &newtid, 1,
						  this_xmin,
						  this_cmin,
						  speculative_token,
						  InvalidRelUndoRecPtr);

	/*
	 * And if the tuple was deleted/updated away, do the same in the new
	 * table.
	 */
	if (this_xmax != InvalidTransactionId)
	{
		TM_Result	delete_result;
		bool		this_xact_has_lock;

		/* tuple was deleted. */
		delete_result = nxbt_tid_delete(NewHeap, newtid,
										this_xmax, this_cmax,
										NULL, NULL, false, NULL, this_changedPart,
										&this_xact_has_lock);
		if (delete_result != TM_Ok)
			elog(ERROR, "tuple deletion failed during table rewrite");
	}
	return newtid;
}

/*
 * nx_cluster_process_tuple
 *
 * Creates the TID item with correct visibility information for the
 * given tuple in the old table. Returns the tid of the tuple in the
 * new table, or InvalidNXTid if this tuple can be left out completely.
 */
/*
 * Entry in the hash table that maps old TIDs to new TIDs during CLUSTER.
 */
typedef struct NXClusterTidMapEntry
{
	nxtid		old_tid;		/* hash key */
	nxtid		new_tid;
}			NXClusterTidMapEntry;

/*
 * Deferred UPDATE chain fixup entry.
 */
typedef struct NXClusterDeferredUpdate
{
	nxtid		new_old_tid;	/* TID of old version in new table */
	nxtid		old_update_newtid;	/* TID of new version in old table */
	TransactionId xmax;
	CommandId	cmax;
	bool		key_update;
}			NXClusterDeferredUpdate;

static nxtid
nx_cluster_process_tuple(Relation OldHeap, Relation NewHeap,
						 nxtid oldtid, RelUndoRecPtr old_undoptr,
						 RelUndoRecPtr recent_oldest_undo,
						 TransactionId OldestXmin,
						 List **deferred_updates)
{
	TransactionId this_xmin;
	CommandId	this_cmin;
	TransactionId this_xmax;
	CommandId	this_cmax;
	bool		this_changedPart;
	bool		this_was_update;
	nxtid		this_update_newtid;
	bool		this_key_update;
	uint32		this_speculative_token;
	nxtid		newtid;

	(void) oldtid;

	if (!nx_cluster_check_visibility(OldHeap, old_undoptr,
									 recent_oldest_undo, OldestXmin,
									 &this_xmin, &this_cmin,
									 &this_xmax, &this_cmax,
									 &this_changedPart,
									 &this_was_update,
									 &this_update_newtid,
									 &this_key_update,
									 &this_speculative_token))
		return InvalidNXTid;

	if (this_was_update && this_xmax != InvalidTransactionId)
	{
		/*
		 * Tuple was UPDATEd. Insert without xmax; we'll create the UPDATE
		 * UNDO record later once the new version's TID in the new table is
		 * known.
		 */
		newtid = nx_cluster_write_tuple(NewHeap, this_xmin, this_cmin,
										InvalidTransactionId, InvalidCommandId,
										false, this_speculative_token);

		{
			NXClusterDeferredUpdate *fixup = palloc(sizeof(NXClusterDeferredUpdate));

			fixup->new_old_tid = newtid;
			fixup->old_update_newtid = this_update_newtid;
			fixup->xmax = this_xmax;
			fixup->cmax = this_cmax;
			fixup->key_update = this_key_update;
			*deferred_updates = lappend(*deferred_updates, fixup);
		}
	}
	else
	{
		newtid = nx_cluster_write_tuple(NewHeap, this_xmin, this_cmin,
										this_xmax, this_cmax,
										this_changedPart,
										this_speculative_token);
	}

	return newtid;
}

/*
 * nx_cluster_encode_visibility
 *
 * Encode Noxu visibility info into a HeapTuple header so it can survive
 * the tuplesort.  We repurpose HeapTuple header fields as follows:
 *   t_xmin      -> xmin
 *   t_xmax      -> xmax
 *   t_cid       -> cmin (via HeapTupleHeaderSetCmin)
 *   t_ctid      -> cmax encoded as (blockno=cmax, offset=changedPart?1:0)
 *   t_infomask  -> low 16 bits of speculative_token
 *   t_infomask2 -> high 16 bits of speculative_token
 */
static void
nx_cluster_encode_visibility(HeapTuple tuple,
							 TransactionId xmin, CommandId cmin,
							 TransactionId xmax, CommandId cmax,
							 bool changedPart,
							 uint32 speculative_token)
{
	HeapTupleHeaderSetXmin(tuple->t_data, xmin);
	HeapTupleHeaderSetXmax(tuple->t_data, xmax);
	HeapTupleHeaderSetCmin(tuple->t_data, cmin);

	/*
	 * Encode cmax and changedPart into t_ctid.  This field is normally the
	 * self-pointer or chain pointer, but we repurpose it here because the
	 * tuple only lives through the sort and is never stored on disk.
	 */
	ItemPointerSet(&tuple->t_data->t_ctid, (BlockNumber) cmax,
				   changedPart ? 1 : 0);

	/*
	 * Encode the speculative insertion token across t_infomask/t_infomask2.
	 * These fields are unused in this temporary encoding context.
	 */
	tuple->t_data->t_infomask = (uint16) (speculative_token & 0xFFFF);
	tuple->t_data->t_infomask2 = (uint16) ((speculative_token >> 16) & 0xFFFF);
}

/*
 * nx_cluster_decode_visibility
 *
 * Decode visibility info previously encoded in a HeapTuple header by
 * nx_cluster_encode_visibility().
 */
static void
nx_cluster_decode_visibility(HeapTuple tuple,
							 TransactionId *xmin, CommandId *cmin,
							 TransactionId *xmax, CommandId *cmax,
							 bool *changedPart,
							 uint32 *speculative_token)
{
	*xmin = HeapTupleHeaderGetRawXmin(tuple->t_data);
	*xmax = HeapTupleHeaderGetRawXmax(tuple->t_data);
	*cmin = HeapTupleHeaderGetRawCommandId(tuple->t_data);
	*cmax = (CommandId) ItemPointerGetBlockNumberNoCheck(&tuple->t_data->t_ctid);
	*changedPart = (ItemPointerGetOffsetNumberNoCheck(&tuple->t_data->t_ctid) != 0);
	*speculative_token = ((uint32) tuple->t_data->t_infomask2 << 16) |
		(uint32) tuple->t_data->t_infomask;
}

/*
 * nx_cluster_materialize_tuple
 *
 * Materialize a single Noxu row (identified by old_tid) into a HeapTuple,
 * fetching all attribute values from the columnar attribute B-trees.  The
 * caller must have already opened attribute scans for all non-dropped columns.
 * The resulting HeapTuple is allocated in the current memory context.
 */
static HeapTuple
nx_cluster_materialize_tuple(Relation OldHeap, TupleDesc olddesc,
							 NXAttrTreeScan * attr_scans, nxtid old_tid)
{
	Datum	   *values;
	bool	   *isnull;
	HeapTuple	tuple;
	int			natts = olddesc->natts;

	values = palloc(natts * sizeof(Datum));
	isnull = palloc(natts * sizeof(bool));

	for (int attno = 1; attno <= natts; attno++)
	{
		Form_pg_attribute att = TupleDescAttr(olddesc, attno - 1);

		if (att->attisdropped)
		{
			values[attno - 1] = (Datum) 0;
			isnull[attno - 1] = true;
		}
		else
		{
			Datum		datum = (Datum) 0;
			bool		isnullval = true;

			if (!nxbt_attr_fetch(&attr_scans[attno - 1], &datum, &isnullval, old_tid))
				nx_fetch_attr_with_predecessor(OldHeap, olddesc, attno, old_tid, &datum, &isnullval);

			/* Flatten any overflow values for the sort */
			if (!isnullval && att->attlen == -1)
			{
				if (VARATT_IS_EXTERNAL((struct varlena *) DatumGetPointer(datum)) &&
					VARTAG_EXTERNAL((struct varlena *) DatumGetPointer(datum)) == VARTAG_NOXU)
				{
					datum = noxu_overflow_flatten(OldHeap, (AttrNumber) attno, old_tid, datum);
				}
			}

			values[attno - 1] = datum;
			isnull[attno - 1] = isnullval;
		}
	}

	tuple = heap_form_tuple(olddesc, values, isnull);

	pfree(values);
	pfree(isnull);

	return tuple;
}

/*
 * nx_cluster_write_sorted_tuple
 *
 * Write a sorted HeapTuple into the new Noxu table, decomposing it back
 * into columnar form.  The HeapTuple has visibility info encoded in its
 * header by nx_cluster_encode_visibility().
 */
static void
nx_cluster_write_sorted_tuple(Relation NewHeap, HeapTuple tuple,
							  TupleDesc olddesc)
{
	TransactionId xmin,
				xmax;
	CommandId	cmin,
				cmax;
	bool		changedPart;
	uint32		speculative_token;
	nxtid		new_tid;
	int			natts = olddesc->natts;
	Datum	   *values;
	bool	   *isnull;

	/* Decode visibility info from the HeapTuple header */
	nx_cluster_decode_visibility(tuple, &xmin, &cmin, &xmax, &cmax,
								 &changedPart, &speculative_token);

	/* Write the TID with visibility info */
	new_tid = nx_cluster_write_tuple(NewHeap, xmin, cmin, xmax, cmax,
									 changedPart, speculative_token);
	if (new_tid == InvalidNXTid)
		return;

	/* Decompose the HeapTuple into individual attributes */
	values = palloc(natts * sizeof(Datum));
	isnull = palloc(natts * sizeof(bool));
	heap_deform_tuple(tuple, olddesc, values, isnull);

	/* Write each attribute into the new table's column B-trees */
	for (int attno = 1; attno <= natts; attno++)
	{
		Form_pg_attribute att = TupleDescAttr(olddesc, attno - 1);
		Datum		datum = values[attno - 1];

		/* Re-overflow if needed for the new table */
		if (!isnull[attno - 1] && att->attlen == -1)
		{
			if (VARSIZE_ANY_EXHDR((struct varlena *) DatumGetPointer(datum)) > MaxNoxuDatumSize)
			{
				datum = noxu_overflow_datum(NewHeap, attno, datum, new_tid);
			}
		}

		nxbt_attr_multi_insert(NewHeap, (AttrNumber) attno,
							   &datum, &isnull[attno - 1], &new_tid, 1);
	}

	pfree(values);
	pfree(isnull);
}


static void
noxuam_relation_copy_for_cluster(Relation OldHeap, Relation NewHeap,
								 Relation OldIndex, bool use_sort,
								 TransactionId OldestXmin,
								 Snapshot snapshot,
								 TransactionId *xid_cutoff,
								 MultiXactId *multi_cutoff,
								 double *num_tuples,
								 double *tups_vacuumed,
								 double *tups_recently_dead)
{
	TupleDesc	olddesc;
	NXTidTreeScan tid_scan;
	NXAttrTreeScan *attr_scans;
	RelUndoRecPtr recent_oldest_undo = nx_get_oldest_visible_undo_ptr(OldHeap);
	int			attno;
	IndexScanDesc indexScan;
	Tuplesortstate *tuplesort;
	List	   *deferred_updates = NIL;
	HTAB	   *tid_map;
	HASHCTL		hashctl;

	/* Create hash table to map old TIDs to new TIDs for UPDATE chain fixup */
	memset(&hashctl, 0, sizeof(hashctl));
	hashctl.keysize = sizeof(nxtid);
	hashctl.entrysize = sizeof(NXClusterTidMapEntry);
	hashctl.hcxt = CurrentMemoryContext;
	tid_map = hash_create("CLUSTER TID map", 1024, &hashctl,
						  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	(void) xid_cutoff;
	(void) multi_cutoff;
	(void) num_tuples;
	(void) tups_vacuumed;
	(void) tups_recently_dead;

	olddesc = RelationGetDescr(OldHeap);
	attr_scans = palloc(olddesc->natts * sizeof(NXAttrTreeScan));

	/*
	 * Scan the old table. We ignore any old updated-away tuple versions, and
	 * only stop at the latest tuple version of each row. At the latest
	 * version, follow the update chain to get all the old versions of that
	 * row, too. That way, the whole update chain is processed in one go, and
	 * can be reproduced in the new table.
	 */
	nxbt_tid_begin_scan(OldHeap, MinNXTid, MaxPlusOneNXTid,
						SnapshotAny, &tid_scan);

	for (attno = 1; attno <= olddesc->natts; attno++)
	{
		if (TupleDescAttr(olddesc, attno - 1)->attisdropped)
			continue;

		nxbt_attr_begin_scan(OldHeap,
							 olddesc,
							 attno,
							 &attr_scans[attno - 1]);
	}

	/* Set up sorting if requested */
	if (use_sort)
		tuplesort = tuplesort_begin_cluster(olddesc, OldIndex,
											maintenance_work_mem,
											NULL, TUPLESORT_NONE);
	else
		tuplesort = NULL;

	/*
	 * Prepare to scan the OldHeap.  To ensure we see recently-dead tuples
	 * that still need to be copied, we scan with SnapshotAny and use Noxu
	 * UNDO chain visibility for the visibility test.
	 */
	if (OldIndex != NULL && !use_sort)
	{
		const int	ci_index[] = {
			PROGRESS_REPACK_PHASE,
			PROGRESS_REPACK_INDEX_RELID
		};
		int64		ci_val[2];

		/* Set phase and OIDOldIndex to columns */
		ci_val[0] = PROGRESS_REPACK_PHASE_INDEX_SCAN_HEAP;
		ci_val[1] = RelationGetRelid(OldIndex);
		pgstat_progress_update_multi_param(2, ci_index, ci_val);

		indexScan = index_beginscan(OldHeap, OldIndex, SnapshotAny, NULL, 0, 0, 0);
		index_rescan(indexScan, NULL, 0, NULL, 0);
	}
	else
	{
		/* In scan-and-sort mode and also VACUUM FULL, set phase */
		pgstat_progress_update_param(PROGRESS_REPACK_PHASE,
									 PROGRESS_REPACK_PHASE_SEQ_SCAN_HEAP);

		indexScan = NULL;
	}

	/*
	 * Main scan loop: read all tuples from the old table, checking
	 * visibility. In index-scan mode, write directly.  In scan-and-sort mode,
	 * materialize into HeapTuples with encoded visibility and feed to
	 * tuplesort.
	 */
	for (;;)
	{
		nxtid		old_tid;
		RelUndoRecPtr old_undoptr;
		nxtid		fetchtid = InvalidNXTid;

		CHECK_FOR_INTERRUPTS();

		if (indexScan != NULL)
		{
			ItemPointer itemptr;

			itemptr = index_getnext_tid(indexScan, ForwardScanDirection);
			if (!itemptr)
				break;

			/* Since we used no scan keys, should never need to recheck */
			if (indexScan->xs_recheck)
				elog(ERROR, "CLUSTER does not support lossy index conditions");

			fetchtid = NXTidFromItemPointer(*itemptr);
			nxbt_tid_reset_scan(OldHeap, &tid_scan, MinNXTid, MaxPlusOneNXTid, fetchtid - 1);
			old_tid = nxbt_tid_scan_next(&tid_scan, ForwardScanDirection);
			if (old_tid == InvalidNXTid)
				continue;
		}
		else
		{
			old_tid = nxbt_tid_scan_next(&tid_scan, ForwardScanDirection);
			if (old_tid == InvalidNXTid)
				break;
			fetchtid = old_tid;
		}
		if (old_tid != fetchtid)
			continue;

		old_undoptr = tid_scan.array_iter.undoslots[NXTidScanCurUndoSlotNo(&tid_scan)];

		if (tuplesort != NULL)
		{
			/*
			 * Scan-and-sort mode: check visibility, materialize the tuple,
			 * encode visibility into the HeapTuple header, and feed to sort.
			 */
			TransactionId vis_xmin,
						vis_xmax;
			CommandId	vis_cmin,
						vis_cmax;
			bool		vis_changedPart;
			bool		vis_was_update;
			nxtid		vis_update_newtid;
			bool		vis_key_update;
			uint32		vis_speculative_token;
			HeapTuple	htup;

			if (!nx_cluster_check_visibility(OldHeap, old_undoptr,
											 recent_oldest_undo, OldestXmin,
											 &vis_xmin, &vis_cmin,
											 &vis_xmax, &vis_cmax,
											 &vis_changedPart,
											 &vis_was_update,
											 &vis_update_newtid,
											 &vis_key_update,
											 &vis_speculative_token))
				continue;

			htup = nx_cluster_materialize_tuple(OldHeap, olddesc,
												attr_scans, old_tid);
			nx_cluster_encode_visibility(htup, vis_xmin, vis_cmin,
										 vis_xmax, vis_cmax,
										 vis_changedPart,
										 vis_speculative_token);

			tuplesort_putheaptuple(tuplesort, htup);

			pgstat_progress_update_param(PROGRESS_REPACK_HEAP_TUPLES_SCANNED,
										 *num_tuples + 1);
		}
		else
		{
			/*
			 * Index-scan or VACUUM FULL mode: process and write directly.
			 */
			nxtid		new_tid;
			Datum		datum = (Datum) 0;
			bool		isnull = true;

			new_tid = nx_cluster_process_tuple(OldHeap, NewHeap,
											   old_tid, old_undoptr,
											   recent_oldest_undo,
											   OldestXmin,
											   &deferred_updates);
			if (new_tid != InvalidNXTid)
			{
				/* Record old->new TID mapping for UPDATE chain fixup */
				{
					NXClusterTidMapEntry *entry;
					bool		found;

					entry = hash_search(tid_map, &old_tid, HASH_ENTER, &found);
					entry->new_tid = new_tid;
				}

				/* Fetch the attributes and write them out */
				for (attno = 1; attno <= olddesc->natts; attno++)
				{
					Form_pg_attribute att = TupleDescAttr(olddesc, attno - 1);

					if (att->attisdropped)
					{
						datum = (Datum) 0;
						isnull = true;
					}
					else
					{
						if (!nxbt_attr_fetch(&attr_scans[attno - 1], &datum, &isnull, old_tid))
							nx_fetch_attr_with_predecessor(OldHeap, olddesc, attno, old_tid, &datum, &isnull);
					}

					/* flatten and re-overflow any overflow values */
					if (!isnull && att->attlen == -1)
					{
						if (VARATT_IS_EXTERNAL((struct varlena *) DatumGetPointer(datum)) && VARTAG_EXTERNAL((struct varlena *) DatumGetPointer(datum)) == VARTAG_NOXU)
						{
							datum = noxu_overflow_flatten(OldHeap, (AttrNumber) attno, old_tid, datum);
						}

						if (VARSIZE_ANY_EXHDR((struct varlena *) DatumGetPointer(datum)) > MaxNoxuDatumSize)
						{
							datum = noxu_overflow_datum(NewHeap, attno, datum, new_tid);
						}
					}

					nxbt_attr_multi_insert(NewHeap, (AttrNumber) attno, &datum, &isnull, &new_tid, 1);
				}
			}
		}
	}

	if (indexScan != NULL)
		index_endscan(indexScan);

	/*
	 * In scan-and-sort mode, complete the sort, then read out all tuples and
	 * write them to the new relation in sorted order.
	 */
	if (tuplesort != NULL)
	{
		/* Report that we are now sorting tuples */
		pgstat_progress_update_param(PROGRESS_REPACK_PHASE,
									 PROGRESS_REPACK_PHASE_SORT_TUPLES);

		tuplesort_performsort(tuplesort);

		/* Report that we are now writing new heap */
		pgstat_progress_update_param(PROGRESS_REPACK_PHASE,
									 PROGRESS_REPACK_PHASE_WRITE_NEW_HEAP);

		for (;;)
		{
			HeapTuple	tuple;

			CHECK_FOR_INTERRUPTS();

			tuple = tuplesort_getheaptuple(tuplesort, true);
			if (tuple == NULL)
				break;

			nx_cluster_write_sorted_tuple(NewHeap, tuple, olddesc);

			pgstat_progress_update_param(PROGRESS_REPACK_HEAP_TUPLES_INSERTED,
										 *num_tuples + 1);
		}

		tuplesort_end(tuplesort);
	}

	/*
	 * Apply deferred UPDATE chain fixups. For each tuple that was UPDATEd in
	 * the old table, we now know both the old and new TIDs in the new table.
	 * Create UPDATE undo records to preserve the chain pointers.
	 */
	{
		ListCell   *lc;

		foreach(lc, deferred_updates)
		{
			NXClusterDeferredUpdate *fixup = lfirst(lc);
			NXClusterTidMapEntry *entry;
			bool		found;

			/* Look up the new TID of the updated-to version */
			entry = hash_search(tid_map, &fixup->old_update_newtid,
								HASH_FIND, &found);
			if (found)
			{
				/*
				 * Mark the old version as updated, pointing to the new
				 * version. This creates an UPDATE undo record instead of a
				 * DELETE, preserving the chain for READ COMMITTED.
				 */
				nxbt_tid_mark_updated_for_cluster(NewHeap,
												  fixup->new_old_tid,
												  entry->new_tid,
												  fixup->xmax,
												  fixup->cmax,
												  fixup->key_update);
			}
			else
			{
				/*
				 * The updated-to tuple was not copied (e.g. it was dead).
				 * Fall back to marking as deleted.
				 */
				TM_Result	delete_result;
				bool		xact_has_lock;

				delete_result = nxbt_tid_delete(NewHeap, fixup->new_old_tid,
												fixup->xmax, fixup->cmax,
												NULL, NULL, false, NULL, false,
												&xact_has_lock);
				if (delete_result != TM_Ok)
					elog(ERROR, "tuple deletion failed during CLUSTER UPDATE chain fixup");
			}

			pfree(fixup);
		}
		list_free(deferred_updates);
	}

	hash_destroy(tid_map);

	nxbt_tid_end_scan(&tid_scan);
	for (attno = 1; attno <= olddesc->natts; attno++)
	{
		if (TupleDescAttr(olddesc, attno - 1)->attisdropped)
			continue;

		nxbt_attr_end_scan(&attr_scans[attno - 1]);
	}
}

/*
 * noxuam_scan_analyze_next_block
 *
 * Read the next block for ANALYZE sampling using the ReadStream API.
 *
 * Noxu stores data in per-column B-trees, not heap pages. Physical blocks
 * from MAIN_FORKNUM contain B-tree nodes, not tuples. We drain the
 * ReadStream buffer (required by the protocol), then scan a logical NXTid
 * block to collect actual tuple data for ANALYZE statistics.
 */
static bool
noxuam_scan_analyze_next_block(TableScanDesc sscan, ReadStream *stream)
{
	NoxuDesc	scan = (NoxuDesc) sscan;
	Relation	rel = scan->rs_scan.rs_rd;
	Buffer		buf;
	BlockNumber blockno;
	BlockNumber nblocks;
	int			ntuples;
	NXTidTreeScan tid_scan;
	nxtid		tid;
	TupleDesc	reldesc;

	/*
	 * Drain the next buffer from the ReadStream (required by protocol).
	 * The ReadStream may attempt to read blocks beyond the current relation
	 * size due to race conditions or stale size caching. Catch and handle
	 * such errors gracefully by skipping to the next block.
	 */
	PG_TRY();
	{
		buf = read_stream_next_buffer(stream, NULL);
	}
	PG_CATCH();
	{
		/*
		 * If reading the buffer failed (e.g., block beyond EOF), skip this
		 * block and continue. FlushErrorState() clears the error without
		 * propagating it.
		 */
		FlushErrorState();
		return false;
	}
	PG_END_TRY();

	if (!BufferIsValid(buf))
		return false;

	blockno = BufferGetBlockNumber(buf);

	/*
	 * Verify the block exists before releasing the buffer. The ReadStream
	 * may occasionally return block numbers at or beyond the current relation
	 * size due to race conditions or stale size caching.
	 */
	nblocks = smgrnblocks(RelationGetSmgr(rel), MAIN_FORKNUM);
	if (blockno >= nblocks)
	{
		ReleaseBuffer(buf);
		return false;
	}

	ReleaseBuffer(buf);

	/* Initialize projection and bmscan arrays on first call */
	nx_initialize_proj_attributes_extended(scan, RelationGetDescr(rel));

	/*
	 * Scan the logical NXTid block corresponding to this physical block
	 * number. Each logical block holds up to MaxNXTidOffsetNumber - 1 tuples.
	 */
	ntuples = 0;
	nxbt_tid_begin_scan(rel,
						NXTidFromBlkOff(blockno, 1),
						NXTidFromBlkOff(blockno + 1, 1),
						scan->rs_scan.rs_snapshot,
						&tid_scan);

	while ((tid = nxbt_tid_scan_next(&tid_scan,
									 ForwardScanDirection)) != InvalidNXTid)
	{
		if (ntuples >= MAX_ITEMS_PER_LOGICAL_BLOCK)
			break;
		scan->bmscan_tids[ntuples] = tid;
		ntuples++;
	}
	nxbt_tid_end_scan(&tid_scan);

	/* Fetch all projected attributes for the collected TIDs */
	if (ntuples > 0)
	{
		reldesc = RelationGetDescr(rel);

		for (int i = 1; i < scan->proj_data.num_proj_atts; i++)
		{
			int			attno = scan->proj_data.proj_atts[i];
			NXAttrTreeScan attr_scan;
			Datum		datum;
			bool		isnull;
			Datum	   *datums = scan->bmscan_datums[i];
			bool	   *isnulls = scan->bmscan_isnulls[i];

			nxbt_attr_begin_scan(rel, reldesc, attno, &attr_scan);
			for (int n = 0; n < ntuples; n++)
			{
				datum = (Datum) 0;
				isnull = true;

				if (!nxbt_attr_fetch(&attr_scan, &datum, &isnull,
									 scan->bmscan_tids[n]))
					nx_fetch_attr_with_predecessor(rel, reldesc, attno,
												   scan->bmscan_tids[n],
												   &datum, &isnull);

				if (!isnull)
					datum = nx_datumCopy(datum,
										 attr_scan.attdesc->attbyval,
										 attr_scan.attdesc->attlen);

				datums[n] = datum;
				isnulls[n] = isnull;
			}
			nxbt_attr_end_scan(&attr_scan);
		}
	}

	scan->bmscan_nexttuple = 0;
	scan->bmscan_ntuples = ntuples;

	return true;
}

static bool
noxuam_scan_analyze_next_tuple(TableScanDesc sscan,
							   double *liverows, double *deadrows,
							   TupleTableSlot *slot)
{
	NoxuDesc	scan = (NoxuDesc) sscan;
	nxtid		tid;
	MemoryContext oldcontext;

	(void) deadrows;

	if (scan->bmscan_nexttuple >= scan->bmscan_ntuples)
		return false;

	Assert((scan->proj_data.num_proj_atts - 1) <=
		   slot->tts_tupleDescriptor->natts);

	/* Initialize all slot positions to NULL */
	for (int i = 0; i < slot->tts_tupleDescriptor->natts; i++)
	{
		slot->tts_values[i] = (Datum) 0;
		slot->tts_isnull[i] = true;
	}

	oldcontext = MemoryContextSwitchTo(slot->tts_mcxt);

	tid = scan->bmscan_tids[scan->bmscan_nexttuple];
	for (int i = 1; i < scan->proj_data.num_proj_atts; i++)
	{
		int			natt = scan->proj_data.proj_atts[i];
		Form_pg_attribute att =
			TupleDescAttr(slot->tts_tupleDescriptor, natt - 1);
		Datum		datum;
		bool		isnull;

		datum = scan->bmscan_datums[i][scan->bmscan_nexttuple];
		isnull = scan->bmscan_isnulls[i][scan->bmscan_nexttuple];

		/* Flatten overflow values */
		if (!isnull && att->attlen == -1 &&
			VARATT_IS_EXTERNAL(
							   (struct varlena *) DatumGetPointer(datum)) &&
			VARTAG_EXTERNAL(
							(struct varlena *) DatumGetPointer(datum)) == VARTAG_NOXU)
		{
			datum = noxu_overflow_flatten(scan->rs_scan.rs_rd,
										  (AttrNumber) natt, tid, datum);
		}

		/* Copy non-byval datums to slot's memory context */
		if (!isnull && !att->attbyval)
			datum = nx_datumCopy(datum, att->attbyval, att->attlen);

		slot->tts_values[natt - 1] = datum;
		slot->tts_isnull[natt - 1] = isnull;
	}

	MemoryContextSwitchTo(oldcontext);

	slot->tts_tableOid = RelationGetRelid(scan->rs_scan.rs_rd);
	slot->tts_tid = ItemPointerFromNXTid(tid);
	slot->tts_nvalid = slot->tts_tupleDescriptor->natts;
	slot->tts_flags &= ~TTS_FLAG_EMPTY;

	scan->bmscan_nexttuple++;
	(*liverows)++;

	return true;
}

/* ------------------------------------------------------------------------
 * Miscellaneous callbacks for the heap AM
 * ------------------------------------------------------------------------
 */

/*
 * Return the on-disk size of the relation for the given fork.
 *
 * This is used by ANALYZE to determine sampling density and by
 * pg_relation_size() for reporting. For Noxu, all data is stored in
 * MAIN_FORKNUM regardless of the requested fork, so we always report
 * the main fork size. This gives a reasonable approximation of the
 * physical storage consumed by the relation.
 */
static uint64
noxuam_relation_size(Relation rel, ForkNumber forkNumber)
{
	uint64		nblocks = 0;

	(void) forkNumber;

	/* Open it at the smgr level if not already done */
	RelationGetSmgr(rel);
	nblocks = smgrnblocks(rel->rd_smgr, MAIN_FORKNUM);
	return nblocks * BLCKSZ;
}

/*
 * Noxu stores overflow chunks within the table file itself. Hence, doesn't
 * need separate table/index to be created. Return false for this callback
 * avoids creation of toast table.
 */
static bool
noxuam_relation_needs_toast_table(Relation rel)
{
	(void) rel;
	return false;
}

/* ------------------------------------------------------------------------
 * Planner related callbacks for the noxu AM
 * ------------------------------------------------------------------------
 */

/*
 * Noxu-specific relation size estimation for the planner.
 *
 * Estimates tuple density accounting for columnar B-tree storage, TID
 * encoding overhead, per-attribute structure overhead, and compression.
 */
static void
noxuam_relation_estimate_size(Relation rel, int32 *attr_widths,
							  BlockNumber *pages, double *tuples,
							  double *allvisfrac)
{
	BlockNumber curpages;
	BlockNumber relpages;
	double		reltuples;
	BlockNumber relallvisible;
	double		density;

	/* it has storage, ok to call the smgr */
	curpages = RelationGetNumberOfBlocks(rel);

	/* coerce values in pg_class to more desirable types */
	relpages = (BlockNumber) rel->rd_rel->relpages;
	reltuples = (double) rel->rd_rel->reltuples;
	relallvisible = (BlockNumber) rel->rd_rel->relallvisible;

	/*
	 * HACK: if the relation has never yet been vacuumed, use a minimum size
	 * estimate of 10 pages.  The idea here is to avoid assuming a
	 * newly-created table is really small, even if it currently is, because
	 * that may not be true once some data gets loaded into it.  Once a vacuum
	 * or analyze cycle has been done on it, it's more reasonable to believe
	 * the size is somewhat stable.
	 *
	 * (Note that this is only an issue if the plan gets cached and used again
	 * after the table has been filled.  What we're trying to avoid is using a
	 * nestloop-type plan on a table that has grown substantially since the
	 * plan was made.  Normally, autovacuum/autoanalyze will occur once enough
	 * inserts have happened and cause cached-plan invalidation; but that
	 * doesn't happen instantaneously, and it won't happen at all for cases
	 * such as temporary tables.)
	 *
	 * We approximate "never vacuumed" by "has relpages = 0", which means this
	 * will also fire on genuinely empty relations.  Not great, but
	 * fortunately that's a seldom-seen case in the real world, and it
	 * shouldn't degrade the quality of the plan too much anyway to err in
	 * this direction.
	 *
	 * If the table has inheritance children, we don't apply this heuristic.
	 * Totally empty parent tables are quite common, so we should be willing
	 * to believe that they are empty.
	 */
	if (curpages < 10 &&
		relpages == 0 &&
		!rel->rd_rel->relhassubclass)
		curpages = 10;

	/* report estimated # pages */
	*pages = curpages;
	/* quick exit if rel is clearly empty */
	if (curpages == 0)
	{
		*tuples = 0;
		*allvisfrac = 0;
		return;
	}

	/*
	 * Estimate number of tuples from previous tuple density.
	 *
	 * Match the heap convention: require both reltuples >= 0 (meaning
	 * statistics exist, not the "never vacuumed" sentinel of -1) and
	 * relpages > 0.
	 */
	if (reltuples >= 0 && relpages > 0)
		density = reltuples / (double) relpages;
	else
	{
		/*
		 * Noxu-specific tuple width estimation.
		 *
		 * When we have no usable statistics (relation never vacuumed /
		 * analyzed), estimate tuple density from the column data types
		 * and Noxu's on-disk overhead model.
		 *
		 * Noxu stores data in per-column B-trees rather than row-oriented
		 * heap pages.  The per-tuple overhead is quite different from heap:
		 *
		 * - No heap tuple header; instead, each tuple has TID tree overhead
		 *   (amortized ~2 bits for UNDO slot + ~1 bit for Simple-8b TID
		 *   delta encoding per tuple, plus amortized item header).
		 *
		 * - Each column is stored in its own B-tree, so per-attribute page
		 *   overhead (NXBtreePageOpaque + page header) is amortized across
		 *   all tuples on the page.
		 *
		 * - Null bitmaps use 1 bit per element per column (only for columns
		 *   with nulls), rather than heap's per-tuple bitmap.
		 *
		 * - Columnar data is typically compressed, reducing effective width.
		 *
		 * - Non-leaf pages (metapage, internal B-tree nodes, FPM pages)
		 *   consume space but hold no tuples.
		 */
		int32		data_width;
		int			natts;
		double		tuple_width;
		double		compression_ratio;
		double		tid_overhead_per_tuple;
		double		attr_btree_overhead_per_tuple;
		double		non_leaf_factor;
		int			usable_bytes_per_page;

		data_width = get_rel_data_width(rel, attr_widths);
		natts = RelationGetNumberOfAttributes(rel);

		/*
		 * TID tree per-tuple overhead (amortized):
		 *
		 * Each NXTidArrayItem holds up to NXBT_MAX_ITEM_TIDS (128) tuples.
		 * The item header is amortized over ~128 tuples.  Per tuple we
		 * also need:
		 *   - ~1 bit for Simple-8b TID delta (consecutive case)
		 *   - 2 bits for the UNDO slotword entry (32 slots per uint64)
		 *
		 * Approximate: header/128 + 1/8 TID delta + 1/4 slotword
		 */
		tid_overhead_per_tuple =
			(double) offsetof(NXTidArrayItem, t_payload) / NXBT_MAX_ITEM_TIDS
			+ 1.0 / 8.0		/* TID delta: ~1 bit for consecutive case */
			+ 1.0 / 4.0;		/* slotword: 2 bits per tuple */

		/*
		 * Per-attribute B-tree overhead (amortized):
		 *
		 * Each NXAttributeArrayItem header covers a group of datums.
		 * Assume ~64 datums per item as a rough average.  In addition
		 * to the item header, each item has Simple-8b TID codewords
		 * (~1 bit per tuple) and a null bitmap (~1 bit per tuple;
		 * conservatively assume all columns might have nulls).
		 *
		 * Per-tuple per-attribute overhead:
		 *   item_header/64 + TID_codeword(1/8) + null_bit(1/8)
		 */
		attr_btree_overhead_per_tuple =
			natts * ((double) offsetof(NXAttributeArrayItem, t_tid_codewords) / 64.0
					 + 1.0 / 8.0	/* TID codeword in attr item */
					 + 1.0 / 8.0); /* null bitmap bit */

		/*
		 * Apply compression ratio to data width.  Use per-column stats if
		 * available from ANALYZE, otherwise use the default ratio.
		 */
		compression_ratio = NOXU_DEFAULT_COMPRESSION_RATIO;
		{
			double		op_ratio;

			if (nxstats_get_compression_ratio(RelationGetRelid(rel),
											  &op_ratio))
				compression_ratio = op_ratio;
		}

		/*
		 * Total estimated per-tuple width in the noxu file:
		 * compressed data + TID overhead + attribute B-tree overhead.
		 */
		tuple_width = (double) data_width / compression_ratio
			+ tid_overhead_per_tuple
			+ attr_btree_overhead_per_tuple;

		/* Ensure a minimum sensible width */
		if (tuple_width < 1.0)
			tuple_width = 1.0;

		/*
		 * Usable bytes per page, accounting for NXBtreePageOpaque in the
		 * special area and the standard page header.
		 */
		usable_bytes_per_page = BLCKSZ - SizeOfPageHeaderData
			- sizeof(NXBtreePageOpaque);

		density = usable_bytes_per_page / tuple_width;

		/*
		 * Adjust for non-leaf page overhead.  Not all pages in the relation
		 * hold tuple data: the metapage (block 0), internal B-tree pages,
		 * and FPM free-list pages consume space.  For a relation with N+1
		 * B-trees, there is at least one internal page per tree once the
		 * tree exceeds one leaf, plus the metapage.  Estimate ~2% non-leaf
		 * for small relations, ~5% for larger ones where internal trees
		 * are deeper and FPM lists are longer.
		 */
		non_leaf_factor = (curpages < 1000) ? 0.98 : 0.95;
		density *= non_leaf_factor;

		/* Ensure at least one tuple per page */
		density = clamp_row_est(density);
	}
	*tuples = rint(density * (double) curpages);

	/*
	 * Noxu-specific: Use opportunistic statistics if available and fresh.
	 * These are collected during normal DML and scan operations, giving the
	 * planner better estimates between ANALYZE runs.
	 */
	{
		double		op_live = 0;
		double		op_dead = 0;

		if (nxstats_is_fresh(RelationGetRelid(rel),
							 noxu_stats_freshness_threshold) &&
			nxstats_get_tuple_counts(RelationGetRelid(rel),
									 &op_live, &op_dead))
		{
			elog(DEBUG2, "Noxu: using opportunistic stats for %s: "
				 "%.0f live, %.0f dead (was %.0f from density)",
				 RelationGetRelationName(rel),
				 op_live, op_dead, *tuples);
			*tuples = op_live;
		}
	}

	/*
	 * Noxu-specific: Apply columnar cost adjustments.
	 *
	 * For queries that access only a subset of columns, Noxu reads less data
	 * than heap would. Adjust page count estimate to reflect this I/O
	 * reduction.
	 *
	 * Note: We use conservative default estimates here. In the future, this
	 * could use statistics from noxu_get_relation_stats() to get actual
	 * column access patterns from the current query.
	 */
	{
		double		io_factor;
		double		cpu_factor;
		double		column_selectivity;
		double		compression_ratio;

		/*
		 * Conservative defaults when column statistics unavailable: - Assume
		 * 60% of columns accessed (typical for OLTP queries) - Use default
		 * compression ratio
		 */
		column_selectivity = 0.6;
		compression_ratio = NOXU_DEFAULT_COMPRESSION_RATIO;

		/*
		 * Try to use opportunistic compression ratio if available.
		 */
		{
			double		op_ratio;

			if (nxstats_get_compression_ratio(RelationGetRelid(rel),
											  &op_ratio))
				compression_ratio = op_ratio;
		}

		/* Calculate cost adjustment factors */
		noxu_calculate_cost_factors(column_selectivity, compression_ratio,
									&io_factor, &cpu_factor);

		/*
		 * Apply I/O reduction: if we read fewer columns, we read fewer pages.
		 * Multiply page count by io_factor (e.g., 0.6 for 60% of columns).
		 *
		 * However, don't reduce below the actual physical pages - we still
		 * need to scan the TID tree which touches every page.
		 */
		if (io_factor < 1.0)
		{
			BlockNumber adjusted_pages;

			adjusted_pages = (BlockNumber) ceil((double) curpages * io_factor);

			/* Sanity check: never report fewer pages than physically exist */
			if (adjusted_pages < curpages)
			{
				elog(DEBUG2, "Noxu: adjusted page estimate from %u to %u (%.0f%% reduction) "
					 "due to column selectivity %.2f",
					 curpages, adjusted_pages,
					 (1.0 - io_factor) * 100.0, column_selectivity);

				*pages = adjusted_pages;
			}
		}

		/*
		 * Note: cpu_factor represents decompression overhead. We don't
		 * directly apply this here - the planner will implicitly account for
		 * it via actual execution time statistics collected during ANALYZE.
		 */
	}

	/*
	 * We use relallvisible as-is, rather than scaling it up like we do for
	 * the pages and tuples counts, on the theory that any pages added since
	 * the last VACUUM are most likely not marked all-visible.  But costsize.c
	 * wants it converted to a fraction.
	 */
	if (relallvisible == 0 || curpages <= 0)
		*allvisfrac = 0;
	else if ((double) relallvisible >= curpages)
		*allvisfrac = 1;
	else
		*allvisfrac = (double) relallvisible / curpages;
}

/* ------------------------------------------------------------------------
 * Executor related callbacks for the noxu AM
 * ------------------------------------------------------------------------
 */


/*
 * noxuam_bitmap_fetch_next_block
 *
 * Fetch the next block of tuples from the TID bitmap into the scan
 * descriptor's bmscan arrays. Returns true if a block was fetched,
 * false if the bitmap is exhausted.
 *
 * For exact (non-lossy) pages, we extract the specific tuple offsets from the
 * bitmap and convert them to nxtid values. For lossy pages, we scan all TIDs
 * in the logical block range using the TID tree.
 *
 * After fetching TIDs, we batch-fetch all projected column values.
 */
static bool
noxuam_bitmap_fetch_next_block(NoxuDesc scan,
							   bool *recheck,
							   uint64 *lossy_pages,
							   uint64 *exact_pages)
{
	TableScanDesc sscan = &scan->rs_scan;
	Relation	rel = sscan->rs_rd;
	TBMIterateResult tbmres;
	int			ntuples;
	TupleDesc	reldesc;

	for (;;)
	{
		CHECK_FOR_INTERRUPTS();

		/* Get next block from the bitmap iterator */
		if (!tbm_iterate(&sscan->st.rs_tbmiterator, &tbmres))
			return false;

		/* Initialize projection and bmscan arrays on first call */
		nx_initialize_proj_attributes_extended(scan, RelationGetDescr(rel));

		ntuples = 0;

		if (tbmres.lossy)
		{
			/*
			 * Lossy page: we don't know which specific tuples matched, so
			 * scan all TIDs in this logical block range using the TID tree.
			 * The executor will recheck all returned tuples.
			 */
			NXTidTreeScan tid_scan;
			nxtid		tid;

			*recheck = true;

			nxbt_tid_begin_scan(rel,
								NXTidFromBlkOff(tbmres.blockno, 1),
								NXTidFromBlkOff(tbmres.blockno + 1, 1),
								sscan->rs_snapshot,
								&tid_scan);

			while ((tid = nxbt_tid_scan_next(&tid_scan,
											 ForwardScanDirection)) != InvalidNXTid)
			{
				if (ntuples >= MAX_ITEMS_PER_LOGICAL_BLOCK)
					break;
				scan->bmscan_tids[ntuples] = tid;
				ntuples++;
			}
			nxbt_tid_end_scan(&tid_scan);

			(*lossy_pages)++;
		}
		else
		{
			/*
			 * Exact page: extract specific tuple offsets from the bitmap and
			 * convert to nxtid values. We must check visibility for each TID,
			 * because the index may still contain entries for deleted rows.
			 *
			 * We do this by scanning the TID tree for the block range (which
			 * performs visibility checking) and intersecting the results with
			 * the bitmap's TID set.
			 */
			OffsetNumber offsets[TBM_MAX_TUPLES_PER_PAGE];
			int			noffsets;
			NXTidTreeScan tid_scan;
			nxtid		tid;
			nxtid		bitmap_tids[TBM_MAX_TUPLES_PER_PAGE];
			int			bm_idx;

			*recheck = tbmres.recheck;

			noffsets = tbm_extract_page_tuple(&tbmres, offsets,
											  TBM_MAX_TUPLES_PER_PAGE);

			/* Build sorted array of TIDs from bitmap offsets */
			for (int i = 0; i < noffsets; i++)
				bitmap_tids[i] = NXTidFromBlkOff(tbmres.blockno, offsets[i]);

			/* Scan TID tree for the block range with visibility checking */
			nxbt_tid_begin_scan(rel,
								NXTidFromBlkOff(tbmres.blockno, 1),
								NXTidFromBlkOff(tbmres.blockno + 1, 1),
								sscan->rs_snapshot,
								&tid_scan);

			bm_idx = 0;
			while ((tid = nxbt_tid_scan_next(&tid_scan,
											 ForwardScanDirection)) != InvalidNXTid)
			{
				/* Advance bitmap index past TIDs less than current */
				while (bm_idx < noffsets && bitmap_tids[bm_idx] < tid)
					bm_idx++;

				/* If this visible TID is in the bitmap set, include it */
				if (bm_idx < noffsets && bitmap_tids[bm_idx] == tid)
				{
					if (ntuples >= MAX_ITEMS_PER_LOGICAL_BLOCK)
						break;
					scan->bmscan_tids[ntuples] = tid;
					ntuples++;
					bm_idx++;
				}
			}
			nxbt_tid_end_scan(&tid_scan);

			(*exact_pages)++;
		}

		/* Skip empty blocks */
		if (ntuples == 0)
			continue;

		/* Batch-fetch all projected column values for the collected TIDs */
		reldesc = RelationGetDescr(rel);

		for (int i = 1; i < scan->proj_data.num_proj_atts; i++)
		{
			int			attno = scan->proj_data.proj_atts[i];
			NXAttrTreeScan attr_scan;
			Datum		datum;
			bool		isnull;
			Datum	   *datums = scan->bmscan_datums[i];
			bool	   *isnulls = scan->bmscan_isnulls[i];

			nxbt_attr_begin_scan(rel, reldesc, attno, &attr_scan);
			for (int n = 0; n < ntuples; n++)
			{
				datum = (Datum) 0;
				isnull = true;

				if (!nxbt_attr_fetch(&attr_scan, &datum, &isnull,
									 scan->bmscan_tids[n]))
					nx_fetch_attr_with_predecessor(rel, reldesc, attno,
												   scan->bmscan_tids[n],
												   &datum, &isnull);

				if (!isnull)
					datum = nx_datumCopy(datum,
										 attr_scan.attdesc->attbyval,
										 attr_scan.attdesc->attlen);

				datums[n] = datum;
				isnulls[n] = isnull;
			}
			nxbt_attr_end_scan(&attr_scan);
		}

		scan->bmscan_nexttuple = 0;
		scan->bmscan_ntuples = ntuples;
		return true;
	}
}

/*
 * Bitmap scan implementation for Noxu tables.
 *
 * Iterates through the TID bitmap, fetching blocks of matching tuples and
 * returning them one at a time. For exact (non-lossy) bitmap pages, only the
 * specific TIDs from the bitmap are fetched. For lossy pages, all visible
 * TIDs in the logical block are fetched, and recheck is set so the executor
 * re-evaluates the original predicate.
 *
 * Column values are batch-fetched per block for efficiency, using the same
 * bmscan arrays used by ANALYZE and TABLESAMPLE scans.
 */
static bool
noxuam_scan_bitmap_next_tuple(TableScanDesc sscan,
							  TupleTableSlot *slot,
							  bool *recheck,
							  uint64 *lossy_pages,
							  uint64 *exact_pages)
{
	NoxuDesc	scan = (NoxuDesc) sscan;
	nxtid		tid;
	MemoryContext oldcontext;

	/*
	 * If we've exhausted the current block's tuples, fetch the next block
	 * from the bitmap.
	 */
	while (scan->bmscan_nexttuple >= scan->bmscan_ntuples)
	{
		if (!noxuam_bitmap_fetch_next_block(scan, recheck,
											lossy_pages, exact_pages))
			return false;
	}

	Assert((scan->proj_data.num_proj_atts - 1) <=
		   slot->tts_tupleDescriptor->natts);

	/* Initialize all slot positions to NULL */
	for (int i = 0; i < slot->tts_tupleDescriptor->natts; i++)
	{
		slot->tts_values[i] = (Datum) 0;
		slot->tts_isnull[i] = true;
	}

	oldcontext = MemoryContextSwitchTo(slot->tts_mcxt);

	tid = scan->bmscan_tids[scan->bmscan_nexttuple];
	for (int i = 1; i < scan->proj_data.num_proj_atts; i++)
	{
		int			natt = scan->proj_data.proj_atts[i];
		Form_pg_attribute att =
			TupleDescAttr(slot->tts_tupleDescriptor, natt - 1);
		Datum		datum;
		bool		isnull;

		datum = scan->bmscan_datums[i][scan->bmscan_nexttuple];
		isnull = scan->bmscan_isnulls[i][scan->bmscan_nexttuple];

		/* Flatten overflow values */
		if (!isnull && att->attlen == -1 &&
			VARATT_IS_EXTERNAL(
							   (struct varlena *) DatumGetPointer(datum)) &&
			VARTAG_EXTERNAL(
							(struct varlena *) DatumGetPointer(datum)) == VARTAG_NOXU)
		{
			datum = noxu_overflow_flatten(scan->rs_scan.rs_rd,
										  (AttrNumber) natt, tid, datum);
		}

		/* Copy non-byval datums to slot's memory context */
		if (!isnull && !att->attbyval)
			datum = nx_datumCopy(datum, att->attbyval, att->attlen);

		slot->tts_values[natt - 1] = datum;
		slot->tts_isnull[natt - 1] = isnull;
	}

	MemoryContextSwitchTo(oldcontext);

	slot->tts_tableOid = RelationGetRelid(scan->rs_scan.rs_rd);
	slot->tts_tid = ItemPointerFromNXTid(tid);
	slot->tts_nvalid = slot->tts_tupleDescriptor->natts;
	slot->tts_flags &= ~TTS_FLAG_EMPTY;

	scan->bmscan_nexttuple++;

	return true;
}

static bool
noxuam_scan_sample_next_block(TableScanDesc sscan, SampleScanState *scanstate)
{
	NoxuDesc	scan = (NoxuDesc) sscan;
	Relation	rel = scan->rs_scan.rs_rd;
	TsmRoutine *tsm = scanstate->tsmroutine;
	int			ntuples;
	NXTidTreeScan tid_scan;
	nxtid		tid;
	BlockNumber blockno;

	/* Sample scans always project all columns for tuple reconstruction. */
	nx_initialize_proj_attributes_extended(scan, RelationGetDescr(rel));

	if (scan->max_tid_to_scan == InvalidNXTid)
	{
		/*
		 * get the max tid once and store it, used to calculate max blocks to
		 * scan either for SYSTEM or BERNOULLI sampling.
		 */
		scan->max_tid_to_scan = nxbt_get_last_tid(rel);
	}

	if (scan->next_tid_to_scan == InvalidNXTid)
	{
		/*
		 * Initialize the sequential scan position for BERNOULLI sampling.
		 * This is separate from max_tid_to_scan because beginscan() may
		 * have already set max_tid_to_scan for Halloween prevention.
		 *
		 * Starting from block 0 is simpler and correct; the scan will
		 * skip over any empty TID ranges efficiently via the B-tree.
		 */
		scan->next_tid_to_scan = NXTidFromBlkOff(0, 1);
	}

	if (tsm->NextSampleBlock)
	{
		/* Adding one below to convert block number to number of blocks. */
		blockno = tsm->NextSampleBlock(scanstate,
									   NXTidGetBlockNumber(scan->max_tid_to_scan) + 1);

		if (!BlockNumberIsValid(blockno))
			return false;
	}
	else
	{
		/* scanning table sequentially */
		if (scan->next_tid_to_scan > scan->max_tid_to_scan)
			return false;

		blockno = NXTidGetBlockNumber(scan->next_tid_to_scan);
		/* move on to next block of tids for next iteration of scan */
		scan->next_tid_to_scan = NXTidFromBlkOff(blockno + 1, 1);
	}
	Assert(BlockNumberIsValid(blockno));

	ntuples = 0;
	nxbt_tid_begin_scan(scan->rs_scan.rs_rd,
						NXTidFromBlkOff(blockno, 1),
						NXTidFromBlkOff(blockno + 1, 1),
						scan->rs_scan.rs_snapshot,
						&tid_scan);
	while ((tid = nxbt_tid_scan_next(&tid_scan, ForwardScanDirection)) != InvalidNXTid)
	{
		Assert(NXTidGetBlockNumber(tid) == blockno);
		scan->bmscan_tids[ntuples] = tid;
		ntuples++;
	}
	nxbt_tid_end_scan(&tid_scan);

	scan->bmscan_nexttuple = 0;
	scan->bmscan_ntuples = ntuples;

	return true;
}

static bool
noxuam_scan_sample_next_tuple(TableScanDesc sscan, SampleScanState *scanstate,
							  TupleTableSlot *slot)
{
	NoxuDesc	scan = (NoxuDesc) sscan;
	TsmRoutine *tsm = scanstate->tsmroutine;
	nxtid		tid;
	BlockNumber blockno;
	OffsetNumber tupoffset;
	bool		found;

	/* all tuples on this block are invisible */
	if (scan->bmscan_ntuples == 0)
		return false;

	blockno = NXTidGetBlockNumber(scan->bmscan_tids[0]);

	/* find which visible tuple in this block to sample */
	for (;;)
	{
		nxtid		lasttid_for_block = scan->bmscan_tids[scan->bmscan_ntuples - 1];
		OffsetNumber maxoffset = NXTidGetOffsetNumber(lasttid_for_block);

		/* Ask the tablesample method which tuples to check on this page. */
		tupoffset = tsm->NextSampleTuple(scanstate, blockno, maxoffset);

		if (!OffsetNumberIsValid(tupoffset))
			return false;

		tid = NXTidFromBlkOff(blockno, tupoffset);

		found = false;
		for (int n = 0; n < scan->bmscan_ntuples; n++)
		{
			if (scan->bmscan_tids[n] == tid)
			{
				/* visible tuple */
				found = true;
				break;
			}
		}

		if (found)
			break;
		else
			continue;
	}

	/*
	 * projection attributes were created based on Relation tuple descriptor
	 * it better match TupleTableSlot.
	 */
	Assert((scan->proj_data.num_proj_atts - 1) <= slot->tts_tupleDescriptor->natts);

	/*
	 * Initialize all slot positions to NULL. The loop below will overwrite
	 * projected columns with actual values.
	 */
	for (int i = 0; i < slot->tts_tupleDescriptor->natts; i++)
	{
		slot->tts_values[i] = (Datum) 0;
		slot->tts_isnull[i] = true;
	}

	/* fetch values for tuple pointed by tid to sample */
	for (int i = 1; i < scan->proj_data.num_proj_atts; i++)
	{
		int			attno = scan->proj_data.proj_atts[i];
		NXAttrTreeScan attr_scan;
		Form_pg_attribute attr;
		Datum		datum = (Datum) 0;
		bool		isnull = true;

		nxbt_attr_begin_scan(scan->rs_scan.rs_rd,
							 slot->tts_tupleDescriptor,
							 attno,
							 &attr_scan);
		attr = attr_scan.attdesc;

		if (nxbt_attr_fetch(&attr_scan, &datum, &isnull, tid))
		{
			Assert(NXTidGetBlockNumber(tid) == blockno);
		}
		else
		{
			nx_fetch_attr_with_predecessor(scan->rs_scan.rs_rd,
										   slot->tts_tupleDescriptor,
										   attno, tid, &datum, &isnull);
		}

		/*
		 * Make a copy in the slot's tuple context because we close the scan
		 * immediately. This ensures the datum is freed when the slot is
		 * cleared, preventing memory leaks during ANALYZE.
		 */
		if (!isnull)
		{
			MemoryContext oldcontext = MemoryContextSwitchTo(slot->tts_mcxt);

			datum = nx_datumCopy(datum, attr->attbyval, attr->attlen);
			MemoryContextSwitchTo(oldcontext);
		}

		slot->tts_values[attno - 1] = datum;
		slot->tts_isnull[attno - 1] = isnull;

		nxbt_attr_end_scan(&attr_scan);
	}
	slot->tts_tableOid = RelationGetRelid(scan->rs_scan.rs_rd);
	slot->tts_tid = ItemPointerFromNXTid(tid);
	slot->tts_nvalid = slot->tts_tupleDescriptor->natts;
	slot->tts_flags &= ~TTS_FLAG_EMPTY;

	return true;
}

/* ----------------------------------------------------------------
 *			Logical decoding callbacks for REPACK CONCURRENTLY
 *
 * These callbacks enable near-zero-downtime table rewrites by decoding
 * concurrent DML changes and replaying them against a new table copy.
 *
 * The workflow is:
 *   1. REPACK creates an initial copy under SHARE UPDATE EXCLUSIVE lock.
 *   2. A replication slot captures WAL changes from the snapshot point.
 *   3. relation_logical_decode_begin() initializes decode state.
 *   4. For each decoded change, relation_logical_decode_apply() replays
 *      the INSERT/UPDATE/DELETE against the new table.
 *   5. relation_logical_decode_end() cleans up.
 *   6. A brief ACCESS EXCLUSIVE lock swaps the relfilenodes.
 *
 * Noxu's columnar structure means that applying changes requires
 * operating on the per-attribute B-trees, coordinating with the UNDO
 * subsystem, and respecting MVCC visibility through the TID tree.
 * ----------------------------------------------------------------
 */

/*
 * NoxuDecodeState - Per-operation state for logical decoding catch-up.
 *
 * Holds references to source and target relations along with a memory
 * context for transient allocations during the apply phase.
 */
typedef struct NoxuDecodeState
{
	Relation	source_rel;
	Relation	target_rel;
	MemoryContext decode_mcxt;
	int64		applied_inserts;
	int64		applied_updates;
	int64		applied_deletes;
} NoxuDecodeState;

/*
 * noxuam_logical_decode_begin - Initialize logical decoding state.
 *
 * Sets up the NoxuDecodeState and its memory context.  The caller is
 * expected to apply changes via noxuam_logical_decode_apply() and
 * finalize with noxuam_logical_decode_end().
 */
static void *
noxuam_logical_decode_begin(Relation rel, Relation target_rel)
{
	NoxuDecodeState *state;
	MemoryContext decode_mcxt;

	decode_mcxt = AllocSetContextCreate(CurrentMemoryContext,
										"NoxuLogicalDecode",
										ALLOCSET_DEFAULT_SIZES);

	state = (NoxuDecodeState *) MemoryContextAllocZero(decode_mcxt,
													   sizeof(NoxuDecodeState));
	state->source_rel = rel;
	state->target_rel = target_rel;
	state->decode_mcxt = decode_mcxt;
	state->applied_inserts = 0;
	state->applied_updates = 0;
	state->applied_deletes = 0;

	elog(LOG, "noxuam_logical_decode_begin: initialized for relation %s -> %s",
		 RelationGetRelationName(rel),
		 RelationGetRelationName(target_rel));

	return state;
}

/*
 * noxuam_logical_decode_apply - Replay a decoded DML change.
 *
 * Applies an INSERT, UPDATE, or DELETE to the target relation.
 * For Noxu, this routes through the standard tuple_insert / tuple_delete /
 * tuple_update paths on the target relation, which handle the columnar
 * B-tree operations, UNDO coordination, and TID tree updates internally.
 *
 * We use the target relation's own AM callbacks because the target was
 * created as the same AM type.  The slot already contains appropriately
 * deformed tuple data.
 */
static void
noxuam_logical_decode_apply(void *decode_state,
							Relation rel,
							Relation target_rel,
							char change_type,
							ItemPointer old_tid,
							TupleTableSlot *slot)
{
	NoxuDecodeState *state = (NoxuDecodeState *) decode_state;
	MemoryContext oldcontext;

	Assert(state != NULL);
	Assert(target_rel != NULL);

	oldcontext = MemoryContextSwitchTo(state->decode_mcxt);

	switch (change_type)
	{
		case 'I':
			{
				/*
				 * INSERT: Use the target relation's tuple_insert callback.
				 * The slot contains the complete new tuple.
				 */
				Assert(slot != NULL);
				Assert(!TTS_EMPTY(slot));

				target_rel->rd_tableam->tuple_insert(target_rel,
													 slot,
													 GetCurrentCommandId(true),
													 0, /* options */
													 NULL);	/* bistate */
				state->applied_inserts++;

				elog(DEBUG2, "noxuam_logical_decode_apply: INSERT applied to %s",
					 RelationGetRelationName(target_rel));
				break;
			}

		case 'U':
			{
				/*
				 * UPDATE: Delete the old tuple and insert the new one.
				 *
				 * For Noxu's columnar storage, a true in-place update
				 * would require knowing which columns changed to do a
				 * delta update on the attribute B-trees.  For REPACK
				 * CONCURRENTLY we use the simpler delete-then-insert
				 * approach, which is correct because:
				 *   - The target table is not yet visible to other backends
				 *   - No concurrent readers need to see intermediate states
				 *   - The UNDO chain on the target is only for crash safety
				 *
				 * A future optimization could use tuple_update() directly
				 * once TID mapping between source and target is established,
				 * enabling delta updates for partially-changed rows and
				 * reducing B-tree churn.
				 */
				TM_FailureData tmfd;
				TM_Result	result;

				Assert(slot != NULL);
				Assert(!TTS_EMPTY(slot));
				Assert(old_tid != NULL);
				Assert(ItemPointerIsValid(old_tid));

				result = target_rel->rd_tableam->tuple_delete(target_rel,
															  old_tid,
															  GetCurrentCommandId(true),
															  0,	/* options */
															  InvalidSnapshot,
															  InvalidSnapshot,
															  true, /* wait */
															  &tmfd);

				if (result != TM_Ok)
				{
					/*
					 * The old tuple may not exist in the target if it was
					 * inserted and deleted within the catch-up window, or
					 * if this is a replayed update of a tuple that was
					 * already moved.  Log a debug message but don't error.
					 */
					elog(DEBUG1, "noxuam_logical_decode_apply: UPDATE delete phase "
						 "returned %d for tid (%u,%u) in %s",
						 result,
						 ItemPointerGetBlockNumberNoCheck(old_tid),
						 ItemPointerGetOffsetNumberNoCheck(old_tid),
						 RelationGetRelationName(target_rel));
				}

				/* Insert the new version */
				target_rel->rd_tableam->tuple_insert(target_rel,
													 slot,
													 GetCurrentCommandId(true),
													 0,
													 NULL);
				state->applied_updates++;

				elog(DEBUG2, "noxuam_logical_decode_apply: UPDATE applied to %s",
					 RelationGetRelationName(target_rel));
				break;
			}

		case 'D':
			{
				/*
				 * DELETE: Remove the tuple from the target relation.
				 */
				TM_FailureData tmfd;
				TM_Result	result;

				Assert(old_tid != NULL);
				Assert(ItemPointerIsValid(old_tid));

				result = target_rel->rd_tableam->tuple_delete(target_rel,
															  old_tid,
															  GetCurrentCommandId(true),
															  0,	/* options */
															  InvalidSnapshot,
															  InvalidSnapshot,
															  true, /* wait */
															  &tmfd);

				if (result != TM_Ok)
				{
					elog(DEBUG1, "noxuam_logical_decode_apply: DELETE "
						 "returned %d for tid (%u,%u) in %s",
						 result,
						 ItemPointerGetBlockNumberNoCheck(old_tid),
						 ItemPointerGetOffsetNumberNoCheck(old_tid),
						 RelationGetRelationName(target_rel));
				}

				state->applied_deletes++;

				elog(DEBUG2, "noxuam_logical_decode_apply: DELETE applied to %s",
					 RelationGetRelationName(target_rel));
				break;
			}

		default:
			elog(ERROR, "noxuam_logical_decode_apply: unknown change type '%c'",
				 change_type);
			break;
	}

	MemoryContextSwitchTo(oldcontext);

	/*
	 * Periodically reset the decode context to bound memory usage.
	 * Re-allocate state in the freshly-reset context.
	 */
	if ((state->applied_inserts + state->applied_updates +
		 state->applied_deletes) % 10000 == 0)
	{
		int64		inserts = state->applied_inserts;
		int64		updates = state->applied_updates;
		int64		deletes = state->applied_deletes;
		Relation	src = state->source_rel;
		Relation	tgt = state->target_rel;
		MemoryContext mctx = state->decode_mcxt;

		MemoryContextReset(mctx);

		state = (NoxuDecodeState *) MemoryContextAllocZero(mctx,
														   sizeof(NoxuDecodeState));
		state->source_rel = src;
		state->target_rel = tgt;
		state->decode_mcxt = mctx;
		state->applied_inserts = inserts;
		state->applied_updates = updates;
		state->applied_deletes = deletes;
	}
}

/*
 * noxuam_logical_decode_end - Finalize logical decoding.
 *
 * Logs summary statistics and frees the decode memory context.
 */
static void
noxuam_logical_decode_end(void *decode_state,
						  Relation rel,
						  Relation target_rel)
{
	NoxuDecodeState *state = (NoxuDecodeState *) decode_state;

	if (state == NULL)
		return;

	elog(LOG, "noxuam_logical_decode_end: %s -> %s: "
		 INT64_FORMAT " inserts, " INT64_FORMAT " updates, "
		 INT64_FORMAT " deletes applied",
		 RelationGetRelationName(rel),
		 RelationGetRelationName(target_rel),
		 state->applied_inserts,
		 state->applied_updates,
		 state->applied_deletes);

	MemoryContextDelete(state->decode_mcxt);
}

static void
noxuam_vacuum_rel(Relation onerel, const VacuumParams *params,
				  BufferAccessStrategy bstrategy)
{
	VacuumParams mutable_params = *params;

	/* Flush nursery before vacuum to ensure all data is in the B-trees */
	if (noxu_nursery_enabled)
	{
		NXNurseryBuffer *nursery = nx_nursery_get(onerel);

		if (nursery != NULL && nursery->nrows > 0)
			nx_nursery_flush(onerel, nursery);
	}

	/*
	 * Vacuum the per-relation UNDO fork.  nxundo_vacuum determines the oldest
	 * visible XID, calls RelUndoVacuum to discard expired records, and updates
	 * the NX metapage statistics.
	 */
	nxundo_vacuum(onerel, &mutable_params, bstrategy);

	/* Force all LSM Level 1 data into the B-tree during VACUUM */
	nx_lsm_flush_all_to_btree(onerel);
}

const TableAmRoutine noxuam_methods = {
	.type = T_TableAmRoutine,

	.slot_callbacks = noxuam_slot_callbacks,

	.scan_begin = noxuam_beginscan,
	.scan_end = noxuam_endscan,
	.scan_rescan = noxuam_rescan,
	.scan_getnextslot = noxuam_getnextslot,

	.scan_set_tidrange = noxuam_scan_set_tidrange,
	.scan_getnextslot_tidrange = noxuam_scan_getnextslot_tidrange,

	.parallelscan_estimate = nx_parallelscan_estimate,
	.parallelscan_initialize = nx_parallelscan_initialize,
	.parallelscan_reinitialize = nx_parallelscan_reinitialize,

	.index_fetch_begin = noxuam_begin_index_fetch,
	.index_fetch_reset = noxuam_reset_index_fetch,
	.index_fetch_end = noxuam_end_index_fetch,
	.index_fetch_tuple = noxuam_index_fetch_tuple,

	.tuple_insert = noxuam_insert,
	.tuple_insert_speculative = noxuam_insert_speculative,
	.tuple_complete_speculative = noxuam_complete_speculative,
	.multi_insert = noxuam_multi_insert,
	.tuple_delete = noxuam_delete,
	.tuple_update = noxuam_update,
	.tuple_lock = noxuam_lock_tuple,
	.finish_bulk_insert = noxuam_finish_bulk_insert,

	.tuple_fetch_row_version = noxuam_fetch_row_version,
	.tuple_get_latest_tid = noxuam_get_latest_tid,
	.tuple_tid_valid = noxuam_tuple_tid_valid,
	.tuple_satisfies_snapshot = noxuam_tuple_satisfies_snapshot,
	.index_delete_tuples = noxuam_index_delete_tuples,	/* stub implementation */

	.relation_set_new_filelocator = noxuam_relation_set_new_filenode,
	.relation_nontransactional_truncate = noxuam_relation_nontransactional_truncate,
	.relation_copy_data = noxuam_relation_copy_data,
	.relation_copy_for_cluster = noxuam_relation_copy_for_cluster,
	.relation_vacuum = noxuam_vacuum_rel,
	.scan_analyze_next_block = noxuam_scan_analyze_next_block,
	.scan_analyze_next_tuple = noxuam_scan_analyze_next_tuple,

	.index_build_range_scan = noxuam_index_build_range_scan,
	.index_validate_scan = noxuam_index_validate_scan,

	.relation_size = noxuam_relation_size,
	.relation_needs_toast_table = noxuam_relation_needs_toast_table,
	.relation_toast_am = NULL,	/* use default */
	.relation_fetch_toast_slice = NULL, /* use default */

	.relation_estimate_size = noxuam_relation_estimate_size,

	.scan_bitmap_next_tuple = noxuam_scan_bitmap_next_tuple,
	.scan_sample_next_block = noxuam_scan_sample_next_block,
	.scan_sample_next_tuple = noxuam_scan_sample_next_tuple,

	/* REPACK CONCURRENTLY logical decoding callbacks */
	.relation_logical_decode_begin = noxuam_logical_decode_begin,
	.relation_logical_decode_apply = noxuam_logical_decode_apply,
	.relation_logical_decode_end = noxuam_logical_decode_end,
};

/* Table AM handler function */
PG_FUNCTION_INFO_V1(noxu_tableam_handler);

Datum
noxu_tableam_handler(PG_FUNCTION_ARGS)
{
	static bool initialized = false;

	/* Ensure initialization happens once */
	if (!initialized)
	{
		noxu_stats_init();
		noxu_planner_init();
		nx_nursery_init_gucs();
		nx_lsm_init_gucs();
		nx_lsm_merge_init_gucs();

		/* Reserve after all modules have registered their GUCs */
		MarkGUCPrefixReserved("noxu");

		initialized = true;
	}

	PG_RETURN_POINTER(&noxuam_methods);
}

/*
 * Routines for dividing up the TID range for parallel seq scans
 */

typedef struct ParallelNXScanDescData
{
	ParallelTableScanDescData base;

	nxtid		pnx_endtid;		/* last tid + 1 in relation at start of scan */
	pg_atomic_uint64 pnx_allocatedtid_blk;	/* TID space allocated to workers
											 * so far. (in  65536 increments) */
}			ParallelNXScanDescData;
typedef struct ParallelNXScanDescData *ParallelNXScanDesc;

static Size
nx_parallelscan_estimate(Relation rel)
{
	(void) rel;
	return sizeof(ParallelNXScanDescData);
}

static Size
nx_parallelscan_initialize(Relation rel, ParallelTableScanDesc pscan)
{
	ParallelNXScanDesc nxscan = (ParallelNXScanDesc) pscan;

	/* phs_relid field removed from ParallelTableScanDesc */
	nxscan->pnx_endtid = nxbt_get_last_tid(rel);
	pg_atomic_init_u64(&nxscan->pnx_allocatedtid_blk, 0);

	return sizeof(ParallelNXScanDescData);
}

static void
nx_parallelscan_reinitialize(Relation rel, ParallelTableScanDesc pscan)
{
	ParallelNXScanDesc nxscan = (ParallelNXScanDesc) pscan;

	(void) rel;

	pg_atomic_write_u64(&nxscan->pnx_allocatedtid_blk, 0);
}

/*
 * get the next TID range to scan
 *
 * Returns true if there is more to scan, false otherwise.
 *
 * Get the next TID range to scan.  Even if there are no TIDs left to scan,
 * another backend could have grabbed a range to scan and not yet finished
 * looking at it, so it doesn't follow that the scan is done when the first
 * backend gets 'false' return.
 */
static bool
nx_parallelscan_nextrange(Relation rel, ParallelNXScanDesc nxscan,
						  nxtid * start, nxtid * end)
{
	uint64		allocatedtid_blk;

	(void) rel;

	/*
	 * pnx_allocatedtid_blk tracks how much has been allocated to workers
	 * already. When it exceeds rs_lasttid, all TIDs have been allocated.
	 *
	 * Because we use an atomic fetch-and-add to fetch the current value, the
	 * pnx_allocatedtid_blk counter will exceed rs_lasttid, because workers
	 * will still increment the value, when they try to allocate the next
	 * block but all blocks have been allocated already. The counter must be
	 * 64 bits wide because of that, to avoid wrapping around when rs_lasttid
	 * is close to 2^32.  That's also one reason we do this at granularity of
	 * 2^16 TIDs, even though noxu isn't block-oriented.
	 *
	 * We divide the TID space into chunks of 2^16 TIDs each. This is
	 * smaller than optimal for production workloads (the B-tree scan
	 * restart overhead per chunk is non-trivial), but the small chunk
	 * size provides good load balancing across workers and is useful for
	 * exercising the parallel scan code paths during testing. Larger
	 * chunk sizes (e.g., 2^20) could be used for better throughput.
	 */
	allocatedtid_blk = pg_atomic_fetch_add_u64(&nxscan->pnx_allocatedtid_blk, 1);
	*start = NXTidFromBlkOff(allocatedtid_blk, 1);
	*end = NXTidFromBlkOff(allocatedtid_blk + 1, 1);

	return *start < nxscan->pnx_endtid;
}

/*
 * Get the value for a row, when no value has been stored in the attribute tree.
 *
 * This is used after ALTER TABLE ADD COLUMN, when reading rows that were
 * created before column was added. Usually, missing values are implicitly
 * NULLs, but you could specify a different value in the ALTER TABLE command,
 * too, with DEFAULT.
 */
static void
nxbt_fill_missing_attribute_value(TupleDesc tupleDesc, int attno, Datum *datum, bool *isnull)
{
	Form_pg_attribute attr = TupleDescAttr(tupleDesc, attno - 1);

	*isnull = true;
	*datum = (Datum) 0;

	/* This means catalog doesn't have the default value for this attribute */
	if (!attr->atthasmissing)
		return;

	if (tupleDesc->constr &&
		tupleDesc->constr->missing)
	{
		AttrMissing *attrmiss = NULL;

		/*
		 * If there are missing values we want to put them into the tuple.
		 */
		attrmiss = tupleDesc->constr->missing;

		if (attrmiss[attno - 1].am_present)
		{
			*isnull = false;
			if (attr->attbyval)
				*datum = fetch_att(&attrmiss[attno - 1].am_value, attr->attbyval, attr->attlen);
			else
				*datum = nx_datumCopy(attrmiss[attno - 1].am_value, attr->attbyval, attr->attlen);
		}
	}
}

/*
 * Fetch a column value for a TID, with column-delta predecessor fallback.
 *
 * When a TID was created via a delta UPDATE, unchanged columns don't
 * have entries in their B-trees. This function handles that by looking
 * up the TID's UNDO record to find the predecessor TID, then fetching
 * the column value from there.
 *
 * Returns true if a value was found, false if the column is truly missing.
 * In the false case, datum/isnull are set to the missing attribute default.
 *
 * Limits predecessor chain depth to avoid infinite loops from corruption.
 */
#define NX_MAX_PREDECESSOR_DEPTH	10

static bool
nx_fetch_attr_with_predecessor(Relation rel, TupleDesc tupdesc,
							   AttrNumber attno, nxtid tid,
							   Datum *datum, bool *isnull)
{
	NXAttrTreeScan scan;
	nxtid		current_tid = tid;
	int			depth = 0;

	while (depth < NX_MAX_PREDECESSOR_DEPTH)
	{
		nxbt_attr_begin_scan(rel, tupdesc, (AttrNumber) attno, &scan);
		if (nxbt_attr_fetch(&scan, datum, isnull, current_tid))
		{
			/*
			 * CRITICAL: Copy non-byval datums before ending scan. The datum
			 * may point into a pinned buffer. Once we end the scan, that
			 * buffer will be unpinned and the datum pointer becomes dangling.
			 */
			if (!*isnull && !scan.attdesc->attbyval)
				*datum = nx_datumCopy(*datum, scan.attdesc->attbyval, scan.attdesc->attlen);

			nxbt_attr_end_scan(&scan);
			return true;
		}
		nxbt_attr_end_scan(&scan);

		/*
		 * Column not found for this TID. Check if the TID has a DELTA_INSERT
		 * UNDO record with a predecessor.
		 */
		{
			NXTidTreeScan tidscan;
			nxtid		found_tid;
			uint8		slotno;
			RelUndoRecPtr undoptr;
			RelUndoRecordHeader header;
			void	   *payload = NULL;
			Size		payload_size;

			nxbt_tid_begin_scan(rel, current_tid,
								current_tid + 1,
								SnapshotAny, &tidscan);
			found_tid = nxbt_tid_scan_next(&tidscan,
										   ForwardScanDirection);
			if (found_tid == InvalidNXTid)
			{
				nxbt_tid_end_scan(&tidscan);
				break;
			}

			slotno = NXTidScanCurUndoSlotNo(&tidscan);
			undoptr = tidscan.array_iter.undoslots[slotno];
			nxbt_tid_end_scan(&tidscan);

			if (!RelUndoRecPtrIsValid(undoptr))
				break;

			if (!RelUndoReadRecord(rel, undoptr, &header, &payload, &payload_size))
				break;

			/*
			 * Skip past lock and update records to find the underlying
			 * DELTA_INSERT.  When a delta-updated row is subsequently updated
			 * again, the latest UNDO record on the old TID is an UPDATE (from
			 * nxbt_tid_mark_old_updated), followed by a TUPLE_LOCK, then the
			 * original DELTA_INSERT.  We must traverse the prevundorec chain
			 * past these to locate the predecessor information.
			 */
			while (header.urec_type == RELUNDO_TUPLE_LOCK ||
				   header.urec_type == RELUNDO_UPDATE)
			{
				RelUndoRecPtr prev = header.urec_prevundorec;

				if (payload != NULL)
				{
					pfree(payload);
					payload = NULL;
				}
				if (!RelUndoRecPtrIsValid(prev))
					goto not_found;
				if (!RelUndoReadRecord(rel, prev, &header, &payload, &payload_size))
					goto not_found;
			}

			if (header.urec_type == RELUNDO_DELTA_INSERT)
			{
				NXRelUndoDeltaInsertPayload *delta =
					(NXRelUndoDeltaInsertPayload *) payload;

				if (!nx_relundo_delta_col_is_changed(delta, attno))
				{
					current_tid = delta->predecessor_tid;
					pfree(payload);
					depth++;
					continue;
				}
			}

			if (payload != NULL)
				pfree(payload);
			break;
		}
	}

not_found:
	nxbt_fill_missing_attribute_value(tupdesc, attno, datum, isnull);
	return false;
}
