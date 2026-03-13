/*-------------------------------------------------------------------------
 *
 * orvos_handler.c
 *	  Orvos table access method code
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/orvos/orvos_handler.c
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
#include "access/orvos_internal.h"
#include "access/orvos_planner.h"
#include "access/orvos_stats.h"
#include "access/orvos_undorec.h"
#include "catalog/catalog.h"
#include "catalog/index.h"
#include "catalog/pg_class.h"
#include "catalog/storage.h"
#include "catalog/storage_xlog.h"
#include "commands/progress.h"
#include "commands/vacuum.h"
#include "executor/executor.h"
#include "optimizer/plancat.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "storage/procarray.h"
#include "storage/read_stream.h"
#include "utils/builtins.h"
#include "utils/injection_point.h"
#include "utils/rel.h"


typedef enum
{
	OVSCAN_STATE_UNSTARTED,
	OVSCAN_STATE_SCANNING,
	OVSCAN_STATE_FINISHED_RANGE,
	OVSCAN_STATE_FINISHED
}			ov_scan_state;

typedef struct OrvosProjectData
{
	int			num_proj_atts;
	Bitmapset  *project_columns;
	int		   *proj_atts;
	OVTidTreeScan tid_scan;
	OVAttrTreeScan *attr_scans;
	MemoryContext context;
}			OrvosProjectData;

typedef struct OrvosDescData
{
	/* scan parameters */
	TableScanDescData rs_scan;	/* */
	OrvosProjectData proj_data;

	bool		started;
	ovtid		cur_range_start;
	ovtid		cur_range_end;

	/*
	 * These fields are used for bitmap scans, to hold a "block's" worth of
	 * data
	 */
#define	MAX_ITEMS_PER_LOGICAL_BLOCK		MaxHeapTuplesPerPage
	int			bmscan_ntuples;
	ovtid	   *bmscan_tids;
	Datum	  **bmscan_datums;
	bool	  **bmscan_isnulls;
	int			bmscan_nexttuple;

	/* These fields are use for TABLESAMPLE scans */
	ovtid		max_tid_to_scan;
	ovtid		next_tid_to_scan;

}			OrvosDescData;

typedef struct OrvosDescData *OrvosDesc;

typedef struct OrvosIndexFetchData
{
	IndexFetchTableData idx_fetch_data;
	OrvosProjectData proj_data;
}			OrvosIndexFetchData;

typedef struct OrvosIndexFetchData *OrvosIndexFetch;

typedef struct ParallelOVScanDescData *ParallelOVScanDesc;

static IndexFetchTableData *orvosam_begin_index_fetch(Relation rel);
static void orvosam_end_index_fetch(IndexFetchTableData *scan);
static bool orvosam_fetch_row(OrvosIndexFetchData * fetch,
							  ItemPointer tid_p,
							  Snapshot snapshot,
							  TupleTableSlot *slot);
static bool ov_acquire_tuplock(Relation relation, ItemPointer tid, LockTupleMode mode,
							   LockWaitPolicy wait_policy, bool *have_tuple_lock);

static Size ov_parallelscan_estimate(Relation rel);
static Size ov_parallelscan_initialize(Relation rel, ParallelTableScanDesc pscan);
static void ov_parallelscan_reinitialize(Relation rel, ParallelTableScanDesc pscan);
static bool ov_parallelscan_nextrange(Relation rel, ParallelOVScanDesc ovscan,
									  ovtid *start, ovtid *end);
static void ovbt_fill_missing_attribute_value(TupleDesc tupleDesc, int attno, Datum *datum, bool *isnull);
static bool ov_fetch_attr_with_predecessor(Relation rel, TupleDesc tupdesc,
										   AttrNumber attno, ovtid tid,
										   Datum *datum, bool *isnull);

/* ----------------------------------------------------------------
 *				storage AM support routines for orvosam
 * ----------------------------------------------------------------
 */

static bool
orvosam_fetch_row_version(Relation rel,
						  ItemPointer tid_p,
						  Snapshot snapshot,
						  TupleTableSlot *slot)
{
	IndexFetchTableData *fetcher;
	bool		result;

	fetcher = orvosam_begin_index_fetch(rel);

	result = orvosam_fetch_row((OrvosIndexFetchData *) fetcher,
							   tid_p, snapshot, slot);
	if (result)
	{
		/*
		 * FIXME: heapam acquires the predicate lock first, and then calls
		 * CheckForSerializableConflictOut(). We do it in the opposite order,
		 * because CheckForSerializableConflictOut() call as done in
		 * ovbt_get_last_tid() already. Does it matter? I'm not sure.
		 */
		PredicateLockTID(rel, tid_p, snapshot, InvalidTransactionId);
	}
	ExecMaterializeSlot(slot);
	slot->tts_tableOid = RelationGetRelid(rel);
	slot->tts_tid = *tid_p;

	orvosam_end_index_fetch(fetcher);

	return result;
}

static void
orvosam_get_latest_tid(TableScanDesc sscan,
					   ItemPointer tid)
{
	ovtid		ztid = OVTidFromItemPointer(*tid);

	ovbt_find_latest_tid(sscan->rs_rd, &ztid, sscan->rs_snapshot);
	*tid = ItemPointerFromOVTid(ztid);
}

static inline void
orvosam_insert_internal(Relation relation, TupleTableSlot *slot, CommandId cid,
						int options, struct BulkInsertStateData *bistate, uint32 speculative_token)
{
	AttrNumber	attno;
	Datum	   *d;
	bool	   *isnulls;
	ovtid		tid;
	TransactionId xid = GetCurrentTransactionId();
	bool		isnull;
	Datum		datum;
	MemoryContext oldcontext;
	MemoryContext insert_mcontext;

	(void) options;
	(void) bistate;

	/*
	 * insert code performs allocations for creating items and merging items.
	 * These are small allocations but add-up based on number of columns and
	 * rows being inserted. Hence, creating context to track them and
	 * wholesale free instead of retail freeing them. TODO: in long term try
	 * if can avoid creating context here, retail free in normal case and only
	 * create context for page splits maybe.
	 */
	insert_mcontext = AllocSetContextCreate(CurrentMemoryContext,
											"OrvosAMContext",
											ALLOCSET_DEFAULT_SIZES);
	oldcontext = MemoryContextSwitchTo(insert_mcontext);

	if (slot->tts_tupleDescriptor->natts != relation->rd_att->natts)
		elog(ERROR, "slot's attribute count doesn't match relcache entry");

	slot_getallattrs(slot);
	d = slot->tts_values;
	isnulls = slot->tts_isnull;

	tid = InvalidOVTid;

	isnull = true;
	ovbt_tid_multi_insert(relation,
						  &tid, 1,
						  xid, cid, speculative_token, InvalidUndoPtr);

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

		/* If this datum is too large, toast it */
		if (!isnull && attr->attlen < 0 &&
			VARSIZE_ANY_EXHDR((struct varlena *) DatumGetPointer(datum)) > MaxOrvosDatumSize)
		{
			datum = orvos_toast_datum(relation, attno, datum, tid);
		}

		ovbt_attr_multi_insert(relation, (AttrNumber) attno,
							   &datum, &isnull, &tid, 1);
	}

	slot->tts_tableOid = RelationGetRelid(relation);
	slot->tts_tid = ItemPointerFromOVTid(tid);
	/* XXX: should we set visi_info here? */

	MemoryContextSwitchTo(oldcontext);
	MemoryContextDelete(insert_mcontext);

	/* Note: speculative insertions are counted too, even if aborted later */
	pgstat_count_heap_insert(relation, 1);
	ovstats_count_insert(RelationGetRelid(relation), 1);
}

static void
orvosam_insert(Relation relation, TupleTableSlot *slot, CommandId cid,
			   int options, struct BulkInsertStateData *bistate)
{
	orvosam_insert_internal(relation, slot, cid, options, bistate, INVALID_SPECULATIVE_TOKEN);
}

static void
orvosam_insert_speculative(Relation relation, TupleTableSlot *slot, CommandId cid,
						   int options, BulkInsertState bistate, uint32 specToken)
{
	orvosam_insert_internal(relation, slot, cid, options, bistate, specToken);
}

static void
orvosam_complete_speculative(Relation relation, TupleTableSlot *slot, uint32 spekToken,
							 bool succeeded)
{
	ovtid		tid;

	tid = OVTidFromItemPointer(slot->tts_tid);
	ovbt_tid_clear_speculative_token(relation, tid, spekToken, true /* for complete */ );

	/*
	 * there is a conflict
	 *
	 * FIXME: Shouldn't we mark the TID dead first?
	 */
	if (!succeeded)
	{
		OVUndoRecPtr recent_oldest_undo = ovundo_get_oldest_undo_ptr(relation);

		ovbt_tid_mark_dead(relation, tid, recent_oldest_undo);
	}
}

static void
orvosam_multi_insert(Relation relation, TupleTableSlot **slots, int ntuples,
					 CommandId cid, int options, BulkInsertState bistate)
{
	AttrNumber	attno;
	int			i;
	bool		slotgetandset = true;
	TransactionId xid = GetCurrentTransactionId();
	Datum	   *datums;
	bool	   *isnulls;
	ovtid	   *tids;

	(void) options;
	(void) bistate;

	if (ntuples == 0)
	{
		/* COPY sometimes calls us with 0 tuples. */
		return;
	}

	datums = palloc0(ntuples * sizeof(Datum));
	isnulls = palloc(ntuples * sizeof(bool));
	tids = palloc0(ntuples * sizeof(ovtid));

	for (i = 0; i < ntuples; i++)
		isnulls[i] = true;

	ovbt_tid_multi_insert(relation, tids, ntuples,
						  xid, cid, INVALID_SPECULATIVE_TOKEN, InvalidUndoPtr);

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

			/* If this datum is too large, toast it */
			if (!isnull && attr->attlen < 0 &&
				VARSIZE_ANY_EXHDR((struct varlena *) DatumGetPointer(datum)) > MaxOrvosDatumSize)
			{
				datum = orvos_toast_datum(relation, attno, datum, tids[i]);
			}
			datums[i] = datum;
			isnulls[i] = isnull;
		}

		ovbt_attr_multi_insert(relation, (AttrNumber) attno,
							   datums, isnulls, tids, ntuples);

		slotgetandset = false;
	}

	for (i = 0; i < ntuples; i++)
	{
		slots[i]->tts_tableOid = RelationGetRelid(relation);
		slots[i]->tts_tid = ItemPointerFromOVTid(tids[i]);
	}

	pgstat_count_heap_insert(relation, ntuples);
	ovstats_count_insert(RelationGetRelid(relation), ntuples);

	pfree(tids);
	pfree(datums);
	pfree(isnulls);
}

static TM_Result
orvosam_delete(Relation relation, ItemPointer tid_p, CommandId cid,
			   Snapshot snapshot, Snapshot crosscheck, bool wait,
			   TM_FailureData *hufd, bool changingPart)
{
	ovtid		tid = OVTidFromItemPointer(*tid_p);
	TransactionId xid = GetCurrentTransactionId();
	TM_Result	result = TM_Ok;
	bool		this_xact_has_lock = false;
	bool		have_tuple_lock = false;

retry:
	result = ovbt_tid_delete(relation, tid, xid, cid,
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
				 * See orvosam_lock_tuple().
				 */
				if (!this_xact_has_lock)
				{
					ov_acquire_tuplock(relation, tid_p, LockTupleExclusive, LockWaitBlock,
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
		ovstats_count_delete(RelationGetRelid(relation));
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
 * XXX: This is identical to heap_acquire_tuplock
 */

static bool
ov_acquire_tuplock(Relation relation, ItemPointer tid, LockTupleMode mode,
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
orvosam_lock_tuple(Relation relation, ItemPointer tid_p, Snapshot snapshot,
				   TupleTableSlot *slot, CommandId cid, LockTupleMode mode,
				   LockWaitPolicy wait_policy, uint8 flags,
				   TM_FailureData *tmfd)
{
	ovtid		tid = OVTidFromItemPointer(*tid_p);
	TransactionId xid = GetCurrentTransactionId();
	TM_Result	result;
	bool		this_xact_has_lock = false;
	bool		have_tuple_lock = false;
	ovtid		next_tid = tid;
	SnapshotData SnapshotDirty;
	bool		locked_something = false;
	OVUndoSlotVisibility *visi_info = &((OrvosTupleTableSlot *) slot)->visi_info_buf;
	bool		follow_updates = false;

	slot->tts_tableOid = RelationGetRelid(relation);
	slot->tts_tid = *tid_p;

	tmfd->traversed = false;

	/*
	 * For now, we lock just the first attribute. As long as everyone does
	 * that, that's enough.
	 */
retry:
	result = ovbt_tid_lock(relation, tid, xid, cid, mode, follow_updates,
						   snapshot, tmfd, &next_tid, &this_xact_has_lock, visi_info);
	((OrvosTupleTableSlot *) slot)->visi_info = visi_info;

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
			*tid_p = ItemPointerFromOVTid(next_tid);

			/* signal that a tuple later in the chain is getting locked */
			tmfd->traversed = true;

			/* loop back to fetch next in chain */

			/*
			 * FIXME: In the corresponding code in heapam, we cross-check the
			 * xmin/xmax of the old and new tuple. Should we do the same here?
			 */

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
			!ov_acquire_tuplock(relation, tid_p, mode, wait_policy,
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
					/* FIXME: should we release the hwlock here? */
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
		if (result == TM_Ok && tid != next_tid && next_tid != InvalidOVTid)
		{
			tid = next_tid;
			goto retry;
		}
	}

	/* Fetch the tuple, too. */
	if (!orvosam_fetch_row_version(relation, tid_p, SnapshotAny, slot))
		elog(ERROR, "could not fetch locked tuple");

	return TM_Ok;
}

/* like heap_tuple_attr_equals */
static bool
ov_tuple_attr_equals(int attrnum, TupleTableSlot *slot1, TupleTableSlot *slot2)
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
	 * Extract the corresponding values.  XXX this is pretty inefficient if
	 * there are many indexed columns.  Should HeapDetermineModifiedColumns do
	 * a single heap_deform_tuple call on each tuple, instead?	But that
	 * doesn't work for system columns ...
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

		if (!ov_tuple_attr_equals(attnum, oldslot, newslot))
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
ov_compute_changed_columns(Relation relation,
						   TupleTableSlot *oldslot,
						   TupleTableSlot *newslot,
						   bool *changed_cols)
{
	int			natts = relation->rd_att->natts;
	int			nchanged = 0;

	for (int attno = 1; attno <= natts; attno++)
	{
		if (!ov_tuple_attr_equals(attno, oldslot, newslot))
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
#define OV_MAX_PREDECESSOR_DEPTH 10

void
ov_materialize_delta_columns(Relation rel,
							 ovtid newtid,
							 ovtid predecessor_tid,
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
		ovtid		current_tid;
		int			depth;
		bool		found = false;

		/* Skip columns that were changed (already in B-tree) */
		if (changed_cols[idx] & (1U << bit))
			continue;

		/* Initialize to safe defaults before fetch attempt */
		datum = (Datum) 0;
		isnull = true;

		/*
		 * Follow predecessor chain to find the column value.
		 * For chained delta updates, the immediate predecessor might
		 * also be a delta without this column, so we keep following
		 * the chain.
		 */
		current_tid = predecessor_tid;
		for (depth = 0; depth < OV_MAX_PREDECESSOR_DEPTH; depth++)
		{
			OVAttrTreeScan scan;

			ovbt_attr_begin_scan(rel, tupdesc, (AttrNumber) attno, &scan);
			if (ovbt_attr_fetch(&scan, &datum, &isnull, current_tid))
			{
				/*
				 * Found the column value. CRITICAL: Copy non-byval datums
				 * before ending the scan, as they point into a pinned buffer
				 * that will be unpinned when we end the scan.
				 */
				if (!isnull && !scan.attdesc->attbyval)
					datum = ov_datumCopy(datum, scan.attdesc->attbyval,
										 scan.attdesc->attlen);
				ovbt_attr_end_scan(&scan);
				found = true;
				break;
			}
			ovbt_attr_end_scan(&scan);

			/*
			 * Column not in this TID. Check if it has a DELTA_INSERT
			 * UNDO record pointing to a predecessor we can follow.
			 */
			{
				OVTidTreeScan tidscan;
				ovtid		found_tid;
				uint8		slotno;
				OVUndoRecPtr undoptr;
				OVUndoRec  *undorec;
				bool		follow_predecessor = false;

				ovbt_tid_begin_scan(rel, current_tid, current_tid + 1,
									SnapshotAny, &tidscan);
				found_tid = ovbt_tid_scan_next(&tidscan,
											   ForwardScanDirection);
				if (found_tid != InvalidOVTid)
				{
					slotno = OVTidScanCurUndoSlotNo(&tidscan);
					undoptr = tidscan.array_iter.undoslots[slotno];

					if (IsOVUndoRecPtrValid(&undoptr))
					{
						undorec = ovundo_fetch_record(rel, undoptr);
						if (undorec != NULL)
						{
							/* Skip past any lock records */
							while (undorec->type == OVUNDO_TYPE_TUPLE_LOCK)
							{
								OVUndoRecPtr prev = undorec->prevundorec;

								pfree(undorec);
								if (!IsOVUndoRecPtrValid(&prev))
								{
									undorec = NULL;
									break;
								}
								undorec = ovundo_fetch_record(rel, prev);
								if (undorec == NULL)
									break;
							}

							if (undorec != NULL &&
								undorec->type == OVUNDO_TYPE_DELTA_INSERT)
							{
								OVUndoRec_DeltaInsert *delta =
									(OVUndoRec_DeltaInsert *) undorec;

								/*
								 * If this column wasn't changed in the delta,
								 * follow the predecessor chain.
								 */
								if (!ov_delta_col_is_changed(delta, attno))
								{
									current_tid = delta->predecessor_tid;
									follow_predecessor = true;
								}
							}

							if (undorec != NULL)
								pfree(undorec);
						}
					}
				}
				ovbt_tid_end_scan(&tidscan);

				if (!follow_predecessor)
					break;
			}
		}

		if (!found)
		{
			/*
			 * Column not found after following predecessor chain.
			 * Use missing attribute default.
			 */
			ovbt_fill_missing_attribute_value(tupdesc, attno,
											  &datum, &isnull);
		}

		/* Insert into new TID's column B-tree */
		{
			Form_pg_attribute attr = TupleDescAttr(tupdesc, attno - 1);

			elog(LOG, "materialize col %d (attlen=%d, byval=%d) for tid %lu from predecessor %lu, isnull=%d, found=%d",
				 attno, attr->attlen, attr->attbyval, (unsigned long) newtid,
				 (unsigned long) predecessor_tid, isnull, found);
			if (!isnull && attr->attlen == -1)
			{
				struct varlena *vl = (struct varlena *) DatumGetPointer(datum);

				elog(LOG, "  varlena: VARSIZE_ANY=%zu, VARSIZE_ANY_EXHDR=%zu, IS_1B=%d, IS_4B=%d, IS_EXT=%d",
					 VARSIZE_ANY(vl), VARSIZE_ANY_EXHDR(vl),
					 VARATT_IS_1B(vl) ? 1 : 0,
					 (VARATT_IS_4B(vl)) ? 1 : 0,
					 VARATT_IS_EXTERNAL(vl) ? 1 : 0);
			}
		}
		ovbt_attr_multi_insert(rel, (AttrNumber) attno,
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
#define OV_DELTA_UPDATE_THRESHOLD	0.5

static TM_Result
orvosam_update(Relation relation, ItemPointer otid_p, TupleTableSlot *slot,
			   CommandId cid, Snapshot snapshot, Snapshot crosscheck,
			   bool wait, TM_FailureData *hufd,
			   LockTupleMode *lockmode, TU_UpdateIndexes *update_indexes)
{
	ovtid		otid = OVTidFromItemPointer(*otid_p);
	TransactionId xid = GetCurrentTransactionId();
	AttrNumber	attno;
	bool		key_update;
	Datum	   *d;
	bool	   *isnulls;
	TM_Result	result;
	ovtid		newtid;
	TupleTableSlot *oldslot;
	IndexFetchTableData *fetcher;
	MemoryContext oldcontext;
	MemoryContext insert_mcontext;
	bool		this_xact_has_lock = false;
	bool		have_tuple_lock = false;

	/*
	 * insert code performs allocations for creating items and merging items.
	 * These are small allocations but add-up based on number of columns and
	 * rows being inserted. Hence, creating context to track them and
	 * wholesale free instead of retail freeing them. TODO: in long term try
	 * if can avoid creating context here, retail free in normal case and only
	 * create context for page splits maybe.
	 */
	insert_mcontext = AllocSetContextCreate(CurrentMemoryContext,
											"OrvosAMContext",
											ALLOCSET_DEFAULT_SIZES);
	oldcontext = MemoryContextSwitchTo(insert_mcontext);

	slot_getallattrs(slot);
	d = slot->tts_values;
	isnulls = slot->tts_isnull;

	oldslot = table_slot_create(relation, NULL);
	fetcher = orvosam_begin_index_fetch(relation);

	/*
	 * The meta-attribute holds the visibility information, including the
	 * "t_ctid" pointer to the updated version. All the real attributes are
	 * just inserted, as if for a new row.
	 */
retry:
	newtid = InvalidOVTid;

	/*
	 * Fetch the old row, so that we can figure out which columns were
	 * modified.
	 *
	 * FIXME: if we have to follow the update chain, we should look at the
	 * currently latest tuple version, rather than the one visible to our
	 * snapshot.
	 */
	INJECTION_POINT("orvos_update-before-pin", NULL);
	if (!orvosam_fetch_row((OrvosIndexFetchData *) fetcher,
						   otid_p, SnapshotAny, oldslot))
	{
		return TM_Invisible;
	}
	key_update = is_key_update(relation, oldslot, slot);

	*lockmode = key_update ? LockTupleExclusive : LockTupleNoKeyExclusive;

	/*
	 * Compute which columns actually changed, for column-delta optimization.
	 * If fewer than half the columns changed, use the delta path to reduce
	 * WAL volume.
	 */
	{
		int			natts = relation->rd_att->natts;
		bool	   *changed_cols;
		int			nchanged;
		bool		use_delta;

		changed_cols = palloc(natts * sizeof(bool));
		nchanged = ov_compute_changed_columns(relation, oldslot,
											  slot, changed_cols);
		use_delta = (natts > 1 &&
					 nchanged < natts * OV_DELTA_UPDATE_THRESHOLD);

		if (use_delta)
		{
			result = ovbt_tid_delta_update(relation, otid,
										   xid, cid, key_update,
										   snapshot, crosscheck,
										   wait, hufd, &newtid,
										   &this_xact_has_lock,
										   natts, changed_cols);
		}
		else
		{
			result = ovbt_tid_update(relation, otid,
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
					MaxOrvosDatumSize)
				{
					newdatum = orvos_toast_datum(relation,
												 attno, newdatum, newtid);
				}

				ovbt_attr_multi_insert(relation, (AttrNumber) attno,
									   &newdatum, &newisnull,
									   &newtid, 1);
			}

			slot->tts_tableOid = RelationGetRelid(relation);
			slot->tts_tid = ItemPointerFromOVTid(newtid);

			pgstat_count_heap_update(relation, false, false);

			ovstats_count_insert(
								 RelationGetRelid(relation), 1);
			ovstats_count_delete(
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
						ov_acquire_tuplock(relation, otid_p,
										   LockTupleExclusive,
										   LockWaitBlock,
										   &have_tuple_lock);
					}

					XactLockTableWait(xwait, relation,
									  otid_p, XLTW_Update);
					pfree(changed_cols);
					goto retry;
				}
			}
		}

		pfree(changed_cols);
	}

	/*
	 * Now that we have successfully updated the tuple, we can release the
	 * lmgr tuple lock, if we had it.
	 */
	if (have_tuple_lock)
	{
		UnlockTupleTuplock(relation, otid_p, LockTupleExclusive);
		have_tuple_lock = false;
	}

	orvosam_end_index_fetch(fetcher);
	ExecDropSingleTupleTableSlot(oldslot);

	MemoryContextSwitchTo(oldcontext);
	MemoryContextDelete(insert_mcontext);

	return result;
}

static const TupleTableSlotOps *
orvosam_slot_callbacks(Relation relation)
{
	(void) relation;
	return &TTSOpsOrvos;
}

static void
ov_initialize_proj_attributes(TupleDesc tupledesc, OrvosProjectData * proj_data)
{
	MemoryContext oldcontext;

	if (proj_data->num_proj_atts != 0)
		return;

	oldcontext = MemoryContextSwitchTo(proj_data->context);
	/* add one for meta-attribute */
	proj_data->proj_atts = palloc((tupledesc->natts + 1) * sizeof(int));
	proj_data->attr_scans = palloc0(tupledesc->natts * sizeof(OVAttrTreeScan));
	proj_data->tid_scan.active = false;

	proj_data->proj_atts[proj_data->num_proj_atts++] = OV_META_ATTRIBUTE_NUM;

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
ov_initialize_proj_attributes_extended(OrvosDesc scan, TupleDesc tupledesc)
{
	MemoryContext oldcontext;
	OrvosProjectData *proj_data = &scan->proj_data;

	/* if already initialized return */
	if (proj_data->num_proj_atts != 0)
		return;

	ov_initialize_proj_attributes(tupledesc, proj_data);

	oldcontext = MemoryContextSwitchTo(proj_data->context);
	/* Extra setup for bitmap, sample, and analyze scans */
	if ((scan->rs_scan.rs_flags & SO_TYPE_BITMAPSCAN) ||
		(scan->rs_scan.rs_flags & SO_TYPE_SAMPLESCAN) ||
		(scan->rs_scan.rs_flags & SO_TYPE_ANALYZE))
	{
		int			nattrs;

		scan->bmscan_ntuples = 0;
		scan->bmscan_tids = palloc(MAX_ITEMS_PER_LOGICAL_BLOCK * sizeof(ovtid));

		/*
		 * For ANALYZE scans, num_proj_atts is still 0 at this point.
		 * Allocate arrays for all attributes (+ 1 for meta-attribute).
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
orvosam_beginscan_with_column_projection(Relation relation, Snapshot snapshot,
										 int nkeys, ScanKey key,
										 ParallelTableScanDesc parallel_scan,
										 uint32 flags,
										 Bitmapset *project_columns)
{
	OrvosDesc	scan;

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
	scan = (OrvosDesc) palloc0(sizeof(OrvosDescData));

	scan->rs_scan.rs_rd = relation;
	scan->rs_scan.rs_snapshot = snapshot;
	scan->rs_scan.rs_nkeys = nkeys;
	scan->rs_scan.rs_flags = flags;
	scan->rs_scan.rs_parallel = parallel_scan;

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
		ovstats_scan_begin(RelationGetRelid(relation));
	}

	return (TableScanDesc) scan;
}

static TableScanDesc
orvosam_beginscan(Relation relation, Snapshot snapshot,
				  int nkeys, ScanKey key,
				  ParallelTableScanDesc parallel_scan,
				  uint32 flags)
{
	return orvosam_beginscan_with_column_projection(relation, snapshot,
													nkeys, key, parallel_scan, flags, NULL);
}

static void
orvosam_endscan(TableScanDesc sscan)
{
	OrvosDesc	scan = (OrvosDesc) sscan;
	OrvosProjectData *proj_data = &scan->proj_data;

	/* Flush opportunistic scan statistics */
	ovstats_scan_end(RelationGetRelid(scan->rs_scan.rs_rd));

	if (proj_data->proj_atts)
		pfree(proj_data->proj_atts);

	if (proj_data->num_proj_atts > 0)
	{
		ovbt_tid_end_scan(&proj_data->tid_scan);
		for (int i = 1; i < proj_data->num_proj_atts; i++)
			ovbt_attr_end_scan(&proj_data->attr_scans[i - 1]);
	}

	if (scan->rs_scan.rs_flags & SO_TEMP_SNAPSHOT)
		UnregisterSnapshot(scan->rs_scan.rs_snapshot);

	if (proj_data->attr_scans)
		pfree(proj_data->attr_scans);
	pfree(scan);
}

static void
orvosam_rescan(TableScanDesc sscan, struct ScanKeyData *key,
			   bool set_params, bool allow_strat,
			   bool allow_sync, bool allow_pagemode)
{
	OrvosDesc	scan = (OrvosDesc) sscan;

	(void) key;

	/* these params don't do much in orvos yet, but whatever */
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

	if (scan->proj_data.num_proj_atts > 0)
	{
		ovbt_tid_reset_scan(&scan->proj_data.tid_scan,
							scan->cur_range_start, scan->cur_range_end, scan->cur_range_start - 1);
	}
}

static bool
orvosam_getnextslot(TableScanDesc sscan, ScanDirection direction,
					TupleTableSlot *slot)
{
	OrvosDesc	scan = (OrvosDesc) sscan;
	OrvosProjectData *scan_proj = &scan->proj_data;
	int			slot_natts = slot->tts_tupleDescriptor->natts;
	Datum	   *slot_values = slot->tts_values;
	bool	   *slot_isnull = slot->tts_isnull;
	ovtid		this_tid;
	Datum		datum;
	bool		isnull;
	OVUndoSlotVisibility *visi_info;
	uint8		slotno;
	MemoryContext oldcontext;

	if (direction != ForwardScanDirection && scan->rs_scan.rs_parallel)
		elog(ERROR, "parallel backward scan not implemented");

	if (!scan->started)
	{
		ov_initialize_proj_attributes(slot->tts_tupleDescriptor, scan_proj);

		if (scan->rs_scan.rs_parallel)
		{
			/* Allocate next range of TIDs to scan */
			if (!ov_parallelscan_nextrange(scan->rs_scan.rs_rd,
										   (ParallelOVScanDesc) scan->rs_scan.rs_parallel,
										   &scan->cur_range_start, &scan->cur_range_end))
			{
				ExecClearTuple(slot);
				return false;
			}
		}
		else
		{
			scan->cur_range_start = MinOVTid;
			scan->cur_range_end = MaxPlusOneOVTid;
		}

		oldcontext = MemoryContextSwitchTo(scan_proj->context);
		ovbt_tid_begin_scan(scan->rs_scan.rs_rd,
							scan->cur_range_start,
							scan->cur_range_end,
							scan->rs_scan.rs_snapshot,
							&scan_proj->tid_scan);
		scan_proj->tid_scan.serializable = true;
		for (int i = 1; i < scan_proj->num_proj_atts; i++)
		{
			int			attno = scan_proj->proj_atts[i];

			ovbt_attr_begin_scan(scan->rs_scan.rs_rd,
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
		this_tid = ovbt_tid_scan_next(&scan_proj->tid_scan, direction);
		if (this_tid == InvalidOVTid)
		{
			if (scan->rs_scan.rs_parallel)
			{
				/* Allocate next range of TIDs to scan */
				if (!ov_parallelscan_nextrange(scan->rs_scan.rs_rd,
											   (ParallelOVScanDesc) scan->rs_scan.rs_parallel,
											   &scan->cur_range_start, &scan->cur_range_end))
				{
					ExecClearTuple(slot);
					return false;
				}

				ovbt_tid_reset_scan(&scan_proj->tid_scan,
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
	 * ensures ov_datumCopy() allocates in the correct context.
	 */
	oldcontext = MemoryContextSwitchTo(slot->tts_mcxt);

	/* Fetch the datums of each attribute for this row */
	for (int i = 1; i < scan_proj->num_proj_atts; i++)
	{
		OVAttrTreeScan *btscan = &scan_proj->attr_scans[i - 1];
		Form_pg_attribute attr = btscan->attdesc;
		int			natt;

		/* Initialize to safe defaults before fetch attempt */
		datum = (Datum) 0;
		isnull = true;

		if (!ovbt_attr_fetch(btscan, &datum, &isnull, this_tid))
		{
			/*
			 * Column not found. Try predecessor chain for delta updates, then
			 * fall back to missing attribute value.
			 */
			ov_fetch_attr_with_predecessor(scan->rs_scan.rs_rd,
										   slot->tts_tupleDescriptor,
										   btscan->attno, this_tid,
										   &datum, &isnull);
		}

		/*
		 * flatten any ZS-TOASTed values, because the rest of the system
		 * doesn't know how to deal with them.
		 */
		natt = scan_proj->proj_atts[i];

		if (!isnull && attr->attlen == -1 &&
			VARATT_IS_EXTERNAL((struct varlena *) DatumGetPointer(datum)) && VARTAG_EXTERNAL((struct varlena *) DatumGetPointer(datum)) == VARTAG_ORVOS)
		{
			datum = orvos_toast_flatten(scan->rs_scan.rs_rd, (AttrNumber) natt, this_tid, datum);
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
			datum = ov_datumCopy(datum, attr->attbyval, attr->attlen);

		Assert(natt > 0);
		slot_values[natt - 1] = datum;
		slot_isnull[natt - 1] = isnull;
	}

	/* Restore previous memory context */
	MemoryContextSwitchTo(oldcontext);

	/* Fill in the rest of the fields in the slot, and return the tuple */
	slotno = OVTidScanCurUndoSlotNo(&scan_proj->tid_scan);
	visi_info = &scan_proj->tid_scan.array_iter.undoslot_visibility[slotno];
	((OrvosTupleTableSlot *) slot)->visi_info = visi_info;

	slot->tts_tableOid = RelationGetRelid(scan->rs_scan.rs_rd);
	slot->tts_tid = ItemPointerFromOVTid(this_tid);
	slot->tts_nvalid = (AttrNumber) slot->tts_tupleDescriptor->natts;
	slot->tts_flags &= ~TTS_FLAG_EMPTY;

	pgstat_count_heap_getnext(scan->rs_scan.rs_rd);

	/* Opportunistic stats: observe this live tuple */
	ovstats_scan_observe_tuple(RelationGetRelid(scan->rs_scan.rs_rd),
							   true, slot_isnull, slot_natts);

	return true;
}

static bool
orvosam_tuple_tid_valid(TableScanDesc sscan, ItemPointer tid)
{
	OrvosDesc	scan = (OrvosDesc) sscan;
	ovtid		ztid = OVTidFromItemPointer(*tid);

	if (scan->max_tid_to_scan == InvalidOVTid)
	{
		/*
		 * get the max tid once and store it
		 */
		scan->max_tid_to_scan = ovbt_get_last_tid(sscan->rs_rd);
	}

	/*
	 * FIXME: should we get lowest TID as well to further optimize the check.
	 */
	if (ztid <= scan->max_tid_to_scan)
		return true;
	else
		return false;
}

static bool
orvosam_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot,
								 Snapshot snapshot)
{
	/*
	 * TODO: we didn't keep any visibility information about the tuple in the
	 * slot, so we have to fetch it again. A custom slot type might be a good
	 * idea..
	 */
	ovtid		tid = OVTidFromItemPointer(slot->tts_tid);
	OVTidTreeScan meta_scan;
	bool		found;

	/* Use the meta-data tree for the visibility information. */
	ovbt_tid_begin_scan(rel, tid, tid + 1, snapshot, &meta_scan);

	found = ovbt_tid_scan_next(&meta_scan, ForwardScanDirection) != InvalidOVTid;

	ovbt_tid_end_scan(&meta_scan);

	return found;
}

/*
 * orvosam_scan_set_tidrange - Set the range of TIDs to scan
 *
 * This is used for bitmap heap scans to efficiently scan a specific
 * range of TIDs.
 */
static void
orvosam_scan_set_tidrange(TableScanDesc sscan,
						  ItemPointer mintid,
						  ItemPointer maxtid)
{
	OrvosDesc	scan = (OrvosDesc) sscan;
	ovtid		start_tid;
	ovtid		end_tid;

	/*
	 * Convert ItemPointers to ovtids. Handle cases where TIDs are beyond
	 * table boundaries or mintid > maxtid as required by the API.
	 */
	if (mintid)
		start_tid = OVTidFromItemPointer(*mintid);
	else
		start_tid = MinOVTid;

	if (maxtid)
		end_tid = OVTidFromItemPointer(*maxtid) + 1;	/* inclusive ->
														 * exclusive */
	else
		end_tid = MaxPlusOneOVTid;

	/*
	 * If mintid > maxtid, set an invalid range so getnextslot returns no
	 * tuples
	 */
	if (start_tid > end_tid)
	{
		scan->cur_range_start = MinOVTid;
		scan->cur_range_end = MinOVTid; /* empty range */
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
 * orvosam_scan_getnextslot_tidrange - Get next tuple in TID range
 *
 * Returns the next tuple within the TID range set by scan_set_tidrange.
 * This is similar to orvosam_getnextslot but operates within a fixed TID range.
 */
static bool
orvosam_scan_getnextslot_tidrange(TableScanDesc sscan,
								  ScanDirection direction,
								  TupleTableSlot *slot)
{
	OrvosDesc	scan = (OrvosDesc) sscan;
	OrvosProjectData *scan_proj = &scan->proj_data;
	int			slot_natts = slot->tts_tupleDescriptor->natts;
	Datum	   *slot_values = slot->tts_values;
	bool	   *slot_isnull = slot->tts_isnull;
	ovtid		this_tid;
	Datum		datum;
	bool		isnull;
	MemoryContext oldcontext;

	if (direction != ForwardScanDirection)
		elog(ERROR, "TID range scan does not support backward scan");

	/* Initialize scan on first call */
	if (!scan->started)
	{

		ov_initialize_proj_attributes(slot->tts_tupleDescriptor, scan_proj);

		oldcontext = MemoryContextSwitchTo(scan_proj->context);
		ovbt_tid_begin_scan(scan->rs_scan.rs_rd,
							scan->cur_range_start,
							scan->cur_range_end,
							scan->rs_scan.rs_snapshot,
							&scan_proj->tid_scan);
		for (int i = 1; i < scan_proj->num_proj_atts; i++)
		{
			int			attno = scan_proj->proj_atts[i];

			ovbt_attr_begin_scan(scan->rs_scan.rs_rd,
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
	this_tid = ovbt_tid_scan_next(&scan_proj->tid_scan, direction);
	if (this_tid == InvalidOVTid)
	{
		ExecClearTuple(slot);
		return false;
	}
	Assert(this_tid < scan->cur_range_end);

	/*
	 * CRITICAL: Switch to slot's memory context for datum copies. This
	 * ensures ov_datumCopy() allocates in the correct context.
	 */
	oldcontext = MemoryContextSwitchTo(slot->tts_mcxt);

	/* Fetch the datums of each attribute for this row */
	for (int i = 1; i < scan_proj->num_proj_atts; i++)
	{
		OVAttrTreeScan *btscan = &scan_proj->attr_scans[i - 1];
		Form_pg_attribute attr = btscan->attdesc;
		int			natt = scan_proj->proj_atts[i];

		/* Initialize to safe defaults before fetch attempt */
		datum = (Datum) 0;
		isnull = true;

		if (!ovbt_attr_fetch(btscan, &datum, &isnull, this_tid))
			ov_fetch_attr_with_predecessor(scan->rs_scan.rs_rd,
										   slot->tts_tupleDescriptor,
										   btscan->attno, this_tid,
										   &datum, &isnull);

		/*
		 * Flatten any orvos-toasted values, because the rest of the system
		 * doesn't know how to deal with them.
		 */
		if (!isnull && attr->attlen == -1 &&
			VARATT_IS_EXTERNAL((struct varlena *) DatumGetPointer(datum)) &&
			VARTAG_EXTERNAL((struct varlena *) DatumGetPointer(datum)) == VARTAG_ORVOS)
		{
			datum = orvos_toast_flatten(scan->rs_scan.rs_rd, (AttrNumber) natt, this_tid, datum);
		}

		/*
		 * CRITICAL: Copy non-byval datums to avoid dangling pointers. Same
		 * issue as non-parallel scan - must copy before storing in slot.
		 */
		if (!isnull && !attr->attbyval)
			datum = ov_datumCopy(datum, attr->attbyval, attr->attlen);

		slot_values[natt - 1] = datum;
		slot_isnull[natt - 1] = isnull;
	}

	/* Restore previous memory context */
	MemoryContextSwitchTo(oldcontext);

	/* Fill in the rest of the fields in the slot, and return the tuple */
	{
		uint8		slotno;
		OVUndoSlotVisibility *visi_info;

		slotno = OVTidScanCurUndoSlotNo(&scan_proj->tid_scan);
		visi_info = &scan_proj->tid_scan.array_iter.undoslot_visibility[slotno];
		((OrvosTupleTableSlot *) slot)->visi_info = visi_info;

		slot->tts_tableOid = RelationGetRelid(scan->rs_scan.rs_rd);
		slot->tts_tid = ItemPointerFromOVTid(this_tid);
	}

	ExecStoreVirtualTuple(slot);

	return true;
}


static IndexFetchTableData *
orvosam_begin_index_fetch(Relation rel)
{
	OrvosIndexFetch idxscan = palloc0(sizeof(OrvosIndexFetchData));

	idxscan->idx_fetch_data.rel = rel;
	idxscan->proj_data.context = CurrentMemoryContext;

	return (IndexFetchTableData *) idxscan;
}


static void
orvosam_reset_index_fetch(IndexFetchTableData *scan)
{
	(void) scan;
	/* TODO: we could close the scans here, but currently we don't bother */
}

static void
orvosam_end_index_fetch(IndexFetchTableData *scan)
{
	OrvosIndexFetch idxscan = (OrvosIndexFetch) scan;
	OrvosProjectData *ovscan_proj = &idxscan->proj_data;

	if (ovscan_proj->num_proj_atts > 0)
	{
		ovbt_tid_end_scan(&ovscan_proj->tid_scan);
		for (int i = 1; i < ovscan_proj->num_proj_atts; i++)
			ovbt_attr_end_scan(&ovscan_proj->attr_scans[i - 1]);
	}

	if (ovscan_proj->proj_atts)
		pfree(ovscan_proj->proj_atts);

	if (ovscan_proj->attr_scans)
		pfree(ovscan_proj->attr_scans);
	pfree(idxscan);
}

static bool
orvosam_index_fetch_tuple(struct IndexFetchTableData *scan,
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

	result = orvosam_fetch_row((OrvosIndexFetchData *) scan, tid_p, snapshot, slot);
	if (result)
	{
		/*
		 * FIXME: heapam acquires the predicate lock first, and then calls
		 * CheckForSerializableConflictOut(). We do it in the opposite order,
		 * because CheckForSerializableConflictOut() call as done in
		 * ovbt_get_last_tid() already. Does it matter? I'm not sure.
		 */
		PredicateLockTID(scan->rel, tid_p, snapshot, InvalidTransactionId);
	}
	return result;
}

/*
 * Shared implementation of fetch_row_version and index_fetch_tuple callbacks.
 */
static bool
orvosam_fetch_row(OrvosIndexFetchData * fetch,
				  ItemPointer tid_p,
				  Snapshot snapshot,
				  TupleTableSlot *slot)
{
	Relation	rel = fetch->idx_fetch_data.rel;
	ovtid		tid = OVTidFromItemPointer(*tid_p);
	bool		found = true;
	OrvosProjectData *fetch_proj = &fetch->proj_data;

	/* first time here, initialize */
	if (fetch_proj->num_proj_atts == 0)
		ov_initialize_proj_attributes(slot->tts_tupleDescriptor, fetch_proj);
	else
	{
		/* If we had a previous fetches still open, close them first */
		ovbt_tid_end_scan(&fetch_proj->tid_scan);
		for (int i = 1; i < fetch_proj->num_proj_atts; i++)
			ovbt_attr_end_scan(&fetch_proj->attr_scans[i - 1]);
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

	ovbt_tid_begin_scan(rel, tid, tid + 1, snapshot, &fetch_proj->tid_scan);
	fetch_proj->tid_scan.serializable = true;
	found = ovbt_tid_scan_next(&fetch_proj->tid_scan, ForwardScanDirection) != InvalidOVTid;
	if (found)
	{
		MemoryContext oldcontext = MemoryContextSwitchTo(slot->tts_mcxt);

		for (int i = 1; i < fetch_proj->num_proj_atts; i++)
		{
			int			natt = fetch_proj->proj_atts[i];
			OVAttrTreeScan *btscan = &fetch_proj->attr_scans[i - 1];
			Form_pg_attribute attr;
			Datum		datum = (Datum) 0;
			bool		isnull = true;

			ovbt_attr_begin_scan(rel, slot->tts_tupleDescriptor, (AttrNumber) natt, btscan);
			attr = btscan->attdesc;
			if (ovbt_attr_fetch(btscan, &datum, &isnull, tid))
			{
				/*
				 * flatten any ZS-TOASTed values, because the rest of the
				 * system doesn't know how to deal with them.
				 */
				if (!isnull && attr->attlen == -1 &&
					VARATT_IS_EXTERNAL((struct varlena *) DatumGetPointer(datum)) && VARTAG_EXTERNAL((struct varlena *) DatumGetPointer(datum)) == VARTAG_ORVOS)
				{
					datum = orvos_toast_flatten(rel, (AttrNumber) natt, tid, datum);
				}
			}
			else
				ov_fetch_attr_with_predecessor(rel,
											   slot->tts_tupleDescriptor,
											   btscan->attno, tid,
											   &datum, &isnull);

			/*
			 * CRITICAL: Copy non-byval datums to slot's memory context. The
			 * datum may point into a pinned buffer that will be unpinned when
			 * this scan is closed on the next fetch_row call.
			 */
			if (!isnull && !attr->attbyval)
				datum = ov_datumCopy(datum, attr->attbyval, attr->attlen);

			slot->tts_values[natt - 1] = datum;
			slot->tts_isnull[natt - 1] = isnull;
		}

		MemoryContextSwitchTo(oldcontext);
	}

	if (found)
	{
		OVUndoSlotVisibility *visi_info;
		uint8		slotno = OVTidScanCurUndoSlotNo(&fetch_proj->tid_scan);

		visi_info = &fetch_proj->tid_scan.array_iter.undoslot_visibility[slotno];
		((OrvosTupleTableSlot *) slot)->visi_info = visi_info;

		slot->tts_tableOid = RelationGetRelid(rel);
		slot->tts_tid = ItemPointerFromOVTid(tid);
		slot->tts_nvalid = slot->tts_tupleDescriptor->natts;
		slot->tts_flags &= ~TTS_FLAG_EMPTY;
		return true;
	}

	return false;
}

static void
orvosam_index_validate_scan(Relation baseRelation,
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
	scan = (TableScanDesc) orvosam_beginscan_with_column_projection(
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
		 * TODO: Once we have in-place updates, like HOT, this will need to
		 * work harder, like heapam's function.
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
 * orvosam_index_delete_tuples
 *
 * Bottom-up index deletion optimization callback.
 *
 * Determines which index entries point to vacuumable table tuples. The index
 * AM calls this to check whether TIDs from its index page can be deleted.
 * We mark deletable entries in delstate->status and return a snapshot
 * conflict horizon for WAL logging.
 *
 * Unlike heap, Orvos doesn't have HOT chains, so this is simpler - we just
 * check if each TID is visible to any non-vacuumable snapshot.
 */
static TransactionId
orvosam_index_delete_tuples(Relation rel, TM_IndexDeleteOp *delstate)
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
		ovtid		tid;
		OVTidTreeScan meta_scan;
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

		/* Convert ItemPointer to ovtid */
		tid = OVTidFromItemPointer(*htid);

		/*
		 * Check if this tuple is visible to any non-vacuumable snapshot. We
		 * use the TID tree scan to get visibility information.
		 */
		ovbt_tid_begin_scan(rel, tid, tid + 1, &SnapshotNonVacuumable, &meta_scan);
		tuple_exists = (ovbt_tid_scan_next(&meta_scan, ForwardScanDirection) != InvalidOVTid);

		if (tuple_exists)
		{
			/* Tuple is visible to someone, can't delete it */
			ovbt_tid_end_scan(&meta_scan);
			continue;
		}

		ovbt_tid_end_scan(&meta_scan);

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
		 * For Orvos, we need to check the UNDO records to find the XID that
		 * created/modified this tuple.
		 *
		 * TODO: This should scan the undo chain for the TID to find the
		 * oldest XID that needs to be considered. For now, we use a
		 * conservative approach and use the oldest XID from any transaction.
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
orvosam_index_build_range_scan(Relation baseRelation,
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
		 * Must begin our own orvos scan in this case.  We may also need to
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
		scan = (TableScanDesc) orvosam_beginscan_with_column_projection(
																		baseRelation, snapshot, 0, NULL, NULL,
																		SO_TYPE_SEQSCAN | SO_ALLOW_SYNC, proj);

		if (start_blockno != 0 || numblocks != InvalidBlockNumber)
		{
			OrvosDesc	ovscan = (OrvosDesc) scan;
			OrvosProjectData *ovscan_proj = &ovscan->proj_data;

			ovscan->cur_range_start = OVTidFromBlkOff(start_blockno, 1);
			ovscan->cur_range_end = OVTidFromBlkOff(numblocks, 1);

			/* FIXME: when can 'num_proj_atts' be 0? */
			if (ovscan_proj->num_proj_atts > 0)
			{
				ovbt_tid_begin_scan(ovscan->rs_scan.rs_rd,
									ovscan->cur_range_start,
									ovscan->cur_range_end,
									ovscan->rs_scan.rs_snapshot,
									&ovscan_proj->tid_scan);
				for (int i = 1; i < ovscan_proj->num_proj_atts; i++)
				{
					int			natt = ovscan_proj->proj_atts[i];

					ovbt_attr_begin_scan(ovscan->rs_scan.rs_rd,
										 RelationGetDescr(ovscan->rs_scan.rs_rd),
										 natt,
										 &ovscan_proj->attr_scans[i - 1]);
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
		 * is taken from parallel orvos scan, and is SnapshotAny or an MVCC
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
	while (orvosam_getnextslot(scan, ForwardScanDirection, slot))
	{
		HeapTuple	heapTuple;
		OVUndoSlotVisibility *visi_info;

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
		 * TODO: Heap checks for DELETE_IN_PROGRESS do we need as well?
		 */
		visi_info = ((OrvosTupleTableSlot *) slot)->visi_info;
		tupleIsAlive = (visi_info->nonvacuumable_status != OVNV_RECENTLY_DEAD);

		if (tupleIsAlive)
			reltuples += 1;

		/*
		 * TODO: Once we have in-place updates, like HOT, this will need to
		 * work harder, to figure out which tuple version to index.
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
orvosam_finish_bulk_insert(Relation relation, int options)
{
	(void) options;

	/*
	 * If we skipped writing WAL, then we need to sync the orvos (but not
	 * indexes since those use WAL anyway / don't go through tableam)
	 */
	if (!RelationNeedsWAL(relation))
		smgrimmedsync(RelationGetSmgr(relation), MAIN_FORKNUM);
}

/* ------------------------------------------------------------------------
 * DDL related callbacks for orvos AM.
 * ------------------------------------------------------------------------
 */

static void
orvosam_relation_set_new_filenode(Relation rel,
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
	 * XXX this could be refined further, but is it worth the hassle?
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
}

static void
orvosam_relation_nontransactional_truncate(Relation rel)
{
	ovmeta_invalidate_cache(rel);
	RelationTruncate(rel, 0);
}

static void
orvosam_relation_copy_data(Relation rel, const RelFileLocator *newrnode)
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
	 * Create and copy all the relation, and schedule unlinking of the old
	 * physical file.
	 *
	 * NOTE: any conflict in relfilenode value will be caught in
	 * RelationCreateStorage().
	 *
	 * NOTE: There is only the main fork in orvos. Otherwise this would need
	 * to copy other forks, too.
	 */
	RelationCreateStorage(*newrnode, rel->rd_rel->relpersistence, true);

	/* copy main fork */
	RelationCopyStorage(rel->rd_smgr, dstrel, MAIN_FORKNUM,
						rel->rd_rel->relpersistence);

	/* drop old relation, and close new one */
	RelationDropStorage(rel);
	smgrclose(dstrel);
}

/*
 * Subroutine of the orvosam_relation_copy_for_cluster() callback.
 *
 * Creates the TID item with correct visibility information for the
 * given tuple in the old table. Returns the tid of the tuple in the
 * new table, or InvalidOVTid if this tuple can be left out completely.
 *
 * FIXME: This breaks UPDATE chains. I.e. after this is done, an UPDATE
 * looks like DELETE + INSERT, instead of an UPDATE, to any transaction that
 * might try to follow the update chain.
 */
static ovtid
ov_cluster_process_tuple(Relation OldHeap, Relation NewHeap,
						 ovtid oldtid, OVUndoRecPtr old_undoptr,
						 OVUndoRecPtr recent_oldest_undo,
						 TransactionId OldestXmin)
{
	TransactionId this_xmin;
	CommandId	this_cmin;
	TransactionId this_xmax;
	CommandId	this_cmax;
	bool		this_changedPart;
	OVUndoRecPtr undo_ptr;
	OVUndoRec  *undorec;

	(void) oldtid;

	/*
	 * Follow the chain of UNDO records for this tuple, to find the
	 * transaction that originally inserted the row  (xmin/cmin), and the
	 * transaction that deleted or updated it away, if any (xmax/cmax)
	 */
	this_xmin = FrozenTransactionId;
	this_cmin = InvalidCommandId;
	this_xmax = InvalidTransactionId;
	this_cmax = InvalidCommandId;

	undo_ptr = old_undoptr;
	for (;;)
	{
		if (undo_ptr.counter < recent_oldest_undo.counter)
		{
			/* This tuple version is visible to everyone. */
			break;
		}

		/* Fetch the next UNDO record. */
		undorec = ovundo_fetch_record(OldHeap, undo_ptr);

		if (OVUNDO_TYPE_IS_INSERT(undorec->type))
		{
			if (!TransactionIdIsCurrentTransactionId(undorec->xid) &&
				!TransactionIdIsInProgress(undorec->xid) &&
				!TransactionIdDidCommit(undorec->xid))
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
				/* Inserter committed. */
				this_xmin = undorec->xid;
				this_cmin = undorec->cid;

				/*
				 * we know everything there is to know about this tuple
				 * version.
				 */
				break;
			}
		}
		else if (undorec->type == OVUNDO_TYPE_TUPLE_LOCK)
		{
			/*
			 * Ignore tuple locks for now.
			 *
			 * FIXME: we should propagate them to the new copy of the table
			 */
			undo_ptr = undorec->prevundorec;
			continue;
		}
		else if (undorec->type == OVUNDO_TYPE_DELETE ||
				 undorec->type == OVUNDO_TYPE_UPDATE)
		{
			/* Row was deleted (or updated away). */
			if (!TransactionIdIsCurrentTransactionId(undorec->xid) &&
				!TransactionIdIsInProgress(undorec->xid) &&
				!TransactionIdDidCommit(undorec->xid))
			{
				/*
				 * deleter aborted or crashed. The previous record should be
				 * an insertion (possibly with some tuple-locking in between).
				 * We'll remember the tuple when we see the insertion.
				 */
				undo_ptr = undorec->prevundorec;
				continue;
			}
			else
			{
				/* deleter committed or is still in progress. */
				if (TransactionIdPrecedes(undorec->xid, OldestXmin))
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
					 * deleter committed or is in progress. Remember that it
					 * was deleted by this XID.
					 */
					this_xmax = undorec->xid;
					this_cmax = undorec->cid;
					if (undorec->type == OVUNDO_TYPE_DELETE)
						this_changedPart = ((OVUndoRec_Delete *) undorec)->changedPart;
					else
						this_changedPart = false;

					/*
					 * follow the UNDO chain to find information about the
					 * inserting transaction (xmin/cmin)
					 */
					undo_ptr = undorec->prevundorec;
					continue;
				}
			}
		}
	}

	/*
	 * We now know the visibility of this tuple. Re-create it in the new
	 * table.
	 */
	if (this_xmin != InvalidTransactionId)
	{
		/* Insert the first version of the row. */
		ovtid		newtid = InvalidOVTid;

		/* First, insert the tuple. */
		ovbt_tid_multi_insert(NewHeap,
							  &newtid, 1,
							  this_xmin,
							  this_cmin,
							  INVALID_SPECULATIVE_TOKEN,
							  InvalidUndoPtr);

		/*
		 * And if the tuple was deleted/updated away, do the same in the new
		 * table.
		 */
		if (this_xmax != InvalidTransactionId)
		{
			TM_Result	delete_result;
			bool		this_xact_has_lock;

			/* tuple was deleted. */
			delete_result = ovbt_tid_delete(NewHeap, newtid,
											this_xmax, this_cmax,
											NULL, NULL, false, NULL, this_changedPart,
											&this_xact_has_lock);
			if (delete_result != TM_Ok)
				elog(ERROR, "tuple deletion failed during table rewrite");
		}
		return newtid;
	}
	else
		return InvalidOVTid;
}


static void
orvosam_relation_copy_for_cluster(Relation OldHeap, Relation NewHeap,
								  Relation OldIndex, bool use_sort,
								  TransactionId OldestXmin,
								  TransactionId *xid_cutoff,
								  MultiXactId *multi_cutoff,
								  double *num_tuples,
								  double *tups_vacuumed,
								  double *tups_recently_dead)
{
	TupleDesc	olddesc;
	OVTidTreeScan tid_scan;
	OVAttrTreeScan *attr_scans;
	OVUndoRecPtr recent_oldest_undo = ovundo_get_oldest_undo_ptr(OldHeap);
	int			attno;
	IndexScanDesc indexScan;

	(void) xid_cutoff;
	(void) multi_cutoff;
	(void) num_tuples;
	(void) tups_vacuumed;
	(void) tups_recently_dead;

	olddesc = RelationGetDescr(OldHeap),

		attr_scans = palloc(olddesc->natts * sizeof(OVAttrTreeScan));

	/*
	 * Scan the old table. We ignore any old updated-away tuple versions, and
	 * only stop at the latest tuple version of each row. At the latest
	 * version, follow the update chain to get all the old versions of that
	 * row, too. That way, the whole update chain is processed in one go, and
	 * can be reproduced in the new table.
	 */
	ovbt_tid_begin_scan(OldHeap, MinOVTid, MaxPlusOneOVTid,
						SnapshotAny, &tid_scan);

	for (attno = 1; attno <= olddesc->natts; attno++)
	{
		if (TupleDescAttr(olddesc, attno - 1)->attisdropped)
			continue;

		ovbt_attr_begin_scan(OldHeap,
							 olddesc,
							 attno,
							 &attr_scans[attno - 1]);
	}

	/*
	 * TODO: sorting not implemented yet. (it would require materializing each
	 * row into a HeapTuple or something like that, which could carry the
	 * xmin/xmax information through the sorter).
	 */
	use_sort = false;

	/*
	 * Prepare to scan the OldHeap.  To ensure we see recently-dead tuples
	 * that still need to be copied, we scan with SnapshotAny and use
	 * HeapTupleSatisfiesVacuum for the visibility test.
	 */
	if (OldIndex != NULL && !use_sort)
	{
		const int	ci_index[] = {
			PROGRESS_CLUSTER_PHASE,
			PROGRESS_CLUSTER_INDEX_RELID
		};
		int64		ci_val[2];

		/* Set phase and OIDOldIndex to columns */
		ci_val[0] = PROGRESS_CLUSTER_PHASE_INDEX_SCAN_HEAP;
		ci_val[1] = RelationGetRelid(OldIndex);
		pgstat_progress_update_multi_param(2, ci_index, ci_val);

		indexScan = index_beginscan(OldHeap, OldIndex, SnapshotAny, NULL, 0, 0);
		index_rescan(indexScan, NULL, 0, NULL, 0);
	}
	else
	{
		/* In scan-and-sort mode and also VACUUM FULL, set phase */
		pgstat_progress_update_param(PROGRESS_CLUSTER_PHASE,
									 PROGRESS_CLUSTER_PHASE_SEQ_SCAN_HEAP);

		indexScan = NULL;

	/*
	 * TODO: Implement progress tracking for CLUSTER operations.
	 * Need to calculate total row count from Orvos metadata and update
	 * PROGRESS_CLUSTER_TOTAL_HEAP_BLKS or equivalent Orvos-specific param.
	 * Original code referenced heapScan->rs_nblocks which doesn't exist
	 * in columnar storage model.
	 */
	}

	for (;;)
	{
		ovtid		old_tid;
		OVUndoRecPtr old_undoptr;
		ovtid		new_tid;
		Datum		datum = (Datum) 0;
		bool		isnull = true;
		ovtid		fetchtid = InvalidOVTid;

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

			fetchtid = OVTidFromItemPointer(*itemptr);
			ovbt_tid_reset_scan(&tid_scan, MinOVTid, MaxPlusOneOVTid, fetchtid - 1);
			old_tid = ovbt_tid_scan_next(&tid_scan, ForwardScanDirection);
			if (old_tid == InvalidOVTid)
				continue;
		}
		else
		{
			old_tid = ovbt_tid_scan_next(&tid_scan, ForwardScanDirection);
			if (old_tid == InvalidOVTid)
				break;
			fetchtid = old_tid;
		}
		if (old_tid != fetchtid)
			continue;

		old_undoptr = tid_scan.array_iter.undoslots[OVTidScanCurUndoSlotNo(&tid_scan)];

		new_tid = ov_cluster_process_tuple(OldHeap, NewHeap,
										   old_tid, old_undoptr,
										   recent_oldest_undo,
										   OldestXmin);
		if (new_tid != InvalidOVTid)
		{
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
					if (!ovbt_attr_fetch(&attr_scans[attno - 1], &datum, &isnull, old_tid))
						ov_fetch_attr_with_predecessor(OldHeap, olddesc, attno, old_tid, &datum, &isnull);
				}

				/* flatten and re-toast any ZS-TOASTed values */
				if (!isnull && att->attlen == -1)
				{
					if (VARATT_IS_EXTERNAL((struct varlena *) DatumGetPointer(datum)) && VARTAG_EXTERNAL((struct varlena *) DatumGetPointer(datum)) == VARTAG_ORVOS)
					{
						datum = orvos_toast_flatten(OldHeap, (AttrNumber) attno, old_tid, datum);
					}

					if (VARSIZE_ANY_EXHDR((struct varlena *) DatumGetPointer(datum)) > MaxOrvosDatumSize)
					{
						datum = orvos_toast_datum(NewHeap, attno, datum, new_tid);
					}
				}

				ovbt_attr_multi_insert(NewHeap, (AttrNumber) attno, &datum, &isnull, &new_tid, 1);
			}
		}
	}

	if (indexScan != NULL)
		index_endscan(indexScan);

	ovbt_tid_end_scan(&tid_scan);
	for (attno = 1; attno <= olddesc->natts; attno++)
	{
		if (TupleDescAttr(olddesc, attno - 1)->attisdropped)
			continue;

		ovbt_attr_end_scan(&attr_scans[attno - 1]);
	}
}

/*
 * orvosam_scan_analyze_next_block
 *
 * Read the next block for ANALYZE sampling using the ReadStream API.
 *
 * Orvos stores data in per-column B-trees, not heap pages. Physical blocks
 * from MAIN_FORKNUM contain B-tree nodes, not tuples. We drain the
 * ReadStream buffer (required by the protocol), then scan a logical OVTid
 * block to collect actual tuple data for ANALYZE statistics.
 */
static bool
orvosam_scan_analyze_next_block(TableScanDesc sscan, ReadStream *stream)
{
	OrvosDesc	scan = (OrvosDesc) sscan;
	Relation	rel = scan->rs_scan.rs_rd;
	Buffer		buf;
	BlockNumber blockno;
	int			ntuples;
	OVTidTreeScan tid_scan;
	ovtid		tid;
	TupleDesc	reldesc;

	/* Drain the next buffer from the ReadStream (required by protocol) */
	buf = read_stream_next_buffer(stream, NULL);
	if (!BufferIsValid(buf))
		return false;

	blockno = BufferGetBlockNumber(buf);
	ReleaseBuffer(buf);

	/* Initialize projection and bmscan arrays on first call */
	ov_initialize_proj_attributes_extended(scan, RelationGetDescr(rel));

	/*
	 * Scan the logical OVTid block corresponding to this physical block
	 * number. Each logical block holds up to MaxOVTidOffsetNumber - 1
	 * tuples.
	 */
	ntuples = 0;
	ovbt_tid_begin_scan(rel,
						OVTidFromBlkOff(blockno, 1),
						OVTidFromBlkOff(blockno + 1, 1),
						scan->rs_scan.rs_snapshot,
						&tid_scan);

	while ((tid = ovbt_tid_scan_next(&tid_scan,
									 ForwardScanDirection)) != InvalidOVTid)
	{
		if (ntuples >= MAX_ITEMS_PER_LOGICAL_BLOCK)
			break;
		scan->bmscan_tids[ntuples] = tid;
		ntuples++;
	}
	ovbt_tid_end_scan(&tid_scan);

	/* Fetch all projected attributes for the collected TIDs */
	if (ntuples > 0)
	{
		reldesc = RelationGetDescr(rel);

		for (int i = 1; i < scan->proj_data.num_proj_atts; i++)
		{
			int			attno = scan->proj_data.proj_atts[i];
			OVAttrTreeScan attr_scan;
			Datum		datum;
			bool		isnull;
			Datum	   *datums = scan->bmscan_datums[i];
			bool	   *isnulls = scan->bmscan_isnulls[i];

			ovbt_attr_begin_scan(rel, reldesc, attno, &attr_scan);
			for (int n = 0; n < ntuples; n++)
			{
				datum = (Datum) 0;
				isnull = true;

				if (!ovbt_attr_fetch(&attr_scan, &datum, &isnull,
									 scan->bmscan_tids[n]))
					ov_fetch_attr_with_predecessor(rel, reldesc, attno,
												   scan->bmscan_tids[n],
												   &datum, &isnull);

				if (!isnull)
					datum = ov_datumCopy(datum,
										 attr_scan.attdesc->attbyval,
										 attr_scan.attdesc->attlen);

				datums[n] = datum;
				isnulls[n] = isnull;
			}
			ovbt_attr_end_scan(&attr_scan);
		}
	}

	scan->bmscan_nexttuple = 0;
	scan->bmscan_ntuples = ntuples;

	return true;
}

static bool
orvosam_scan_analyze_next_tuple(TableScanDesc sscan,
								double *liverows, double *deadrows,
								TupleTableSlot *slot)
{
	OrvosDesc	scan = (OrvosDesc) sscan;
	ovtid		tid;
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

		/* Flatten Orvos-TOASTed values */
		if (!isnull && att->attlen == -1 &&
			VARATT_IS_EXTERNAL(
				(struct varlena *) DatumGetPointer(datum)) &&
			VARTAG_EXTERNAL(
				(struct varlena *) DatumGetPointer(datum)) == VARTAG_ORVOS)
		{
			datum = orvos_toast_flatten(scan->rs_scan.rs_rd,
										(AttrNumber) natt, tid, datum);
		}

		/* Copy non-byval datums to slot's memory context */
		if (!isnull && !att->attbyval)
			datum = ov_datumCopy(datum, att->attbyval, att->attlen);

		slot->tts_values[natt - 1] = datum;
		slot->tts_isnull[natt - 1] = isnull;
	}

	MemoryContextSwitchTo(oldcontext);

	slot->tts_tableOid = RelationGetRelid(scan->rs_scan.rs_rd);
	slot->tts_tid = ItemPointerFromOVTid(tid);
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
 * FIXME: Implement this function as best for orvos. The return value is
 * for example leveraged by analyze to find which blocks to sample.
 */
static uint64
orvosam_relation_size(Relation rel, ForkNumber forkNumber)
{
	uint64		nblocks = 0;

	(void) forkNumber;

	/* Open it at the smgr level if not already done */
	RelationGetSmgr(rel);
	nblocks = smgrnblocks(rel->rd_smgr, MAIN_FORKNUM);
	return nblocks * BLCKSZ;
}

/*
 * Orvos stores TOAST chunks within the table file itself. Hence, doesn't
 * need separate toast table to be created. Return false for this callback
 * avoids creation of toast table.
 */
static bool
orvosam_relation_needs_toast_table(Relation rel)
{
	(void) rel;
	return false;
}

/* ------------------------------------------------------------------------
 * Planner related callbacks for the orvos AM
 * ------------------------------------------------------------------------
 */

/*
 * currently this is exact duplicate of heapam_estimate_rel_size().
 * TODO fix to tune it based on orvos storage.
 */
static void
orvosam_relation_estimate_size(Relation rel, int32 *attr_widths,
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

	/* estimate number of tuples from previous tuple density */
	if (relpages > 0)
		density = reltuples / (double) relpages;
	else
	{
		/*
		 * When we have no data because the relation was truncated, estimate
		 * tuple width from attribute datatypes.  We assume here that the
		 * pages are completely full, which is OK for tables (since they've
		 * presumably not been VACUUMed yet) but is probably an overestimate
		 * for indexes.  Fortunately get_relation_info() can clamp the
		 * overestimate to the parent table's size.
		 *
		 * Note: this code intentionally disregards alignment considerations,
		 * because (a) that would be gilding the lily considering how crude
		 * the estimate is, and (b) it creates platform dependencies in the
		 * default plans which are kind of a headache for regression testing.
		 */
		int32		tuple_width;

		tuple_width = get_rel_data_width(rel, attr_widths);
		tuple_width += MAXALIGN(SizeofHeapTupleHeader);
		tuple_width += sizeof(ItemIdData);
		/* note: integer division is intentional here */
		density = (BLCKSZ - SizeOfPageHeaderData) / tuple_width;
	}
	*tuples = rint(density * (double) curpages);

	/*
	 * Orvos-specific: Use opportunistic statistics if available and fresh.
	 * These are collected during normal DML and scan operations, giving the
	 * planner better estimates between ANALYZE runs.
	 */
	{
		double		op_live = 0;
		double		op_dead = 0;

		if (ovstats_is_fresh(RelationGetRelid(rel),
							 orvos_stats_freshness_threshold) &&
			ovstats_get_tuple_counts(RelationGetRelid(rel),
									 &op_live, &op_dead))
		{
			elog(DEBUG2, "Orvos: using opportunistic stats for %s: "
				 "%.0f live, %.0f dead (was %.0f from density)",
				 RelationGetRelationName(rel),
				 op_live, op_dead, *tuples);
			*tuples = op_live;
		}
	}

	/*
	 * Orvos-specific: Apply columnar cost adjustments.
	 *
	 * For queries that access only a subset of columns, Orvos reads less data
	 * than heap would. Adjust page count estimate to reflect this I/O
	 * reduction.
	 *
	 * Note: We use conservative default estimates here. In the future, this
	 * could use statistics from orvos_get_relation_stats() to get actual
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
		compression_ratio = ORVOS_DEFAULT_COMPRESSION_RATIO;

		/*
		 * Try to use opportunistic compression ratio if available.
		 */
		{
			double		op_ratio;

			if (ovstats_get_compression_ratio(RelationGetRelid(rel),
											  &op_ratio))
				compression_ratio = op_ratio;
		}

		/* Calculate cost adjustment factors */
		orvos_calculate_cost_factors(column_selectivity, compression_ratio,
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
				elog(DEBUG2, "Orvos: adjusted page estimate from %u to %u (%.0f%% reduction) "
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
 * Executor related callbacks for the orvos AM
 * ------------------------------------------------------------------------
 */


/*
 * Bitmap scan implementation for Orvos tables.
 *
 * For now, this implements a simple sequential scan approach since Orvos
 * doesn't have the traditional block-oriented structure of heap tables.
 * The TID tree structure means we scan TIDs sequentially and check visibility.
 *
 * Future optimization: Could use the bitmap to skip ranges of TIDs more efficiently.
 */
static bool
orvosam_scan_bitmap_next_tuple(TableScanDesc sscan,
							   TupleTableSlot *slot,
							   bool *recheck,
							   uint64 *lossy_pages,
							   uint64 *exact_pages)
{
	/*
	 * For Orvos tables, we always need to recheck visibility since our
	 * columnar structure doesn't directly map to heap's block-based model.
	 */
	*recheck = true;

	/*
	 * Note: lossy_pages and exact_pages are not used in this implementation
	 * since Orvos doesn't have the traditional block-based structure. The
	 * bitmap filtering happens at the executor level above us.
	 */
	(void) lossy_pages;			/* unused */
	(void) exact_pages;			/* unused */

	/*
	 * Use the regular sequential scan to get the next tuple. This is less
	 * efficient than heap's bitmap scan but functionally correct. The bitmap
	 * filtering happens at the executor level above us.
	 */
	return orvosam_getnextslot(sscan, ForwardScanDirection, slot);
}

static bool
orvosam_scan_sample_next_block(TableScanDesc sscan, SampleScanState *scanstate)
{
	OrvosDesc	scan = (OrvosDesc) sscan;
	Relation	rel = scan->rs_scan.rs_rd;
	TsmRoutine *tsm = scanstate->tsmroutine;
	int			ntuples;
	OVTidTreeScan tid_scan;
	ovtid		tid;
	BlockNumber blockno;

	/* TODO: for now, assume that we need all columns */
	ov_initialize_proj_attributes_extended(scan, RelationGetDescr(rel));

	if (scan->max_tid_to_scan == InvalidOVTid)
	{
		/*
		 * get the max tid once and store it, used to calculate max blocks to
		 * scan either for SYSTEM or BERNOULLI sampling.
		 */
		scan->max_tid_to_scan = ovbt_get_last_tid(rel);

		/*
		 * TODO: should get lowest tid instead of starting from 0
		 */
		scan->next_tid_to_scan = OVTidFromBlkOff(0, 1);
	}

	if (tsm->NextSampleBlock)
	{
		/* Adding one below to convert block number to number of blocks. */
		blockno = tsm->NextSampleBlock(scanstate,
									   OVTidGetBlockNumber(scan->max_tid_to_scan) + 1);

		if (!BlockNumberIsValid(blockno))
			return false;
	}
	else
	{
		/* scanning table sequentially */
		if (scan->next_tid_to_scan > scan->max_tid_to_scan)
			return false;

		blockno = OVTidGetBlockNumber(scan->next_tid_to_scan);
		/* move on to next block of tids for next iteration of scan */
		scan->next_tid_to_scan = OVTidFromBlkOff(blockno + 1, 1);
	}

	Assert(BlockNumberIsValid(blockno));

	ntuples = 0;
	ovbt_tid_begin_scan(scan->rs_scan.rs_rd,
						OVTidFromBlkOff(blockno, 1),
						OVTidFromBlkOff(blockno + 1, 1),
						scan->rs_scan.rs_snapshot,
						&tid_scan);
	while ((tid = ovbt_tid_scan_next(&tid_scan, ForwardScanDirection)) != InvalidOVTid)
	{
		Assert(OVTidGetBlockNumber(tid) == blockno);
		scan->bmscan_tids[ntuples] = tid;
		ntuples++;
	}
	ovbt_tid_end_scan(&tid_scan);

	scan->bmscan_nexttuple = 0;
	scan->bmscan_ntuples = ntuples;

	return true;
}

static bool
orvosam_scan_sample_next_tuple(TableScanDesc sscan, SampleScanState *scanstate,
							   TupleTableSlot *slot)
{
	OrvosDesc	scan = (OrvosDesc) sscan;
	TsmRoutine *tsm = scanstate->tsmroutine;
	ovtid		tid;
	BlockNumber blockno;
	OffsetNumber tupoffset;
	bool		found;

	/* all tuples on this block are invisible */
	if (scan->bmscan_ntuples == 0)
		return false;

	blockno = OVTidGetBlockNumber(scan->bmscan_tids[0]);

	/* find which visible tuple in this block to sample */
	for (;;)
	{
		ovtid		lasttid_for_block = scan->bmscan_tids[scan->bmscan_ntuples - 1];
		OffsetNumber maxoffset = OVTidGetOffsetNumber(lasttid_for_block);

		/* Ask the tablesample method which tuples to check on this page. */
		tupoffset = tsm->NextSampleTuple(scanstate, blockno, maxoffset);

		if (!OffsetNumberIsValid(tupoffset))
			return false;

		tid = OVTidFromBlkOff(blockno, tupoffset);

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
		OVAttrTreeScan attr_scan;
		Form_pg_attribute attr;
		Datum		datum = (Datum) 0;
		bool		isnull = true;

		ovbt_attr_begin_scan(scan->rs_scan.rs_rd,
							 slot->tts_tupleDescriptor,
							 attno,
							 &attr_scan);
		attr = attr_scan.attdesc;

		if (ovbt_attr_fetch(&attr_scan, &datum, &isnull, tid))
		{
			Assert(OVTidGetBlockNumber(tid) == blockno);
		}
		else
		{
			ov_fetch_attr_with_predecessor(scan->rs_scan.rs_rd,
										   slot->tts_tupleDescriptor,
										   attno, tid, &datum, &isnull);
		}

		/*
		 * have to make a copy because we close the scan immediately. FIXME: I
		 * think this leaks into a too-long-lived context
		 */
		if (!isnull)
			datum = ov_datumCopy(datum, attr->attbyval, attr->attlen);

		slot->tts_values[attno - 1] = datum;
		slot->tts_isnull[attno - 1] = isnull;

		ovbt_attr_end_scan(&attr_scan);
	}
	slot->tts_tableOid = RelationGetRelid(scan->rs_scan.rs_rd);
	slot->tts_tid = ItemPointerFromOVTid(tid);
	slot->tts_nvalid = slot->tts_tupleDescriptor->natts;
	slot->tts_flags &= ~TTS_FLAG_EMPTY;

	return true;
}

static void
orvosam_vacuum_rel(Relation onerel, const VacuumParams params,
				   BufferAccessStrategy bstrategy)
{
	VacuumParams mutable_params = params;

	/* TODO: Fix ovundo_vacuum to use GlobalVisState instead of TransactionId */
	ovundo_vacuum(onerel, &mutable_params, bstrategy,
				  InvalidTransactionId);
}

const TableAmRoutine orvosam_methods = {
	.type = T_TableAmRoutine,

	.slot_callbacks = orvosam_slot_callbacks,

	.scan_begin = orvosam_beginscan,
	.scan_end = orvosam_endscan,
	.scan_rescan = orvosam_rescan,
	.scan_getnextslot = orvosam_getnextslot,

	.scan_set_tidrange = orvosam_scan_set_tidrange,
	.scan_getnextslot_tidrange = orvosam_scan_getnextslot_tidrange,

	.parallelscan_estimate = ov_parallelscan_estimate,
	.parallelscan_initialize = ov_parallelscan_initialize,
	.parallelscan_reinitialize = ov_parallelscan_reinitialize,

	.index_fetch_begin = orvosam_begin_index_fetch,
	.index_fetch_reset = orvosam_reset_index_fetch,
	.index_fetch_end = orvosam_end_index_fetch,
	.index_fetch_tuple = orvosam_index_fetch_tuple,

	.tuple_insert = orvosam_insert,
	.tuple_insert_speculative = orvosam_insert_speculative,
	.tuple_complete_speculative = orvosam_complete_speculative,
	.multi_insert = orvosam_multi_insert,
	.tuple_delete = orvosam_delete,
	.tuple_update = orvosam_update,
	.tuple_lock = orvosam_lock_tuple,
	.finish_bulk_insert = orvosam_finish_bulk_insert,

	.tuple_fetch_row_version = orvosam_fetch_row_version,
	.tuple_get_latest_tid = orvosam_get_latest_tid,
	.tuple_tid_valid = orvosam_tuple_tid_valid,
	.tuple_satisfies_snapshot = orvosam_tuple_satisfies_snapshot,
	.index_delete_tuples = orvosam_index_delete_tuples, /* stub implementation */

	.relation_set_new_filelocator = orvosam_relation_set_new_filenode,
	.relation_nontransactional_truncate = orvosam_relation_nontransactional_truncate,
	.relation_copy_data = orvosam_relation_copy_data,
	.relation_copy_for_cluster = orvosam_relation_copy_for_cluster,
	.relation_vacuum = orvosam_vacuum_rel,
	.scan_analyze_next_block = orvosam_scan_analyze_next_block,
	.scan_analyze_next_tuple = orvosam_scan_analyze_next_tuple,

	.index_build_range_scan = orvosam_index_build_range_scan,
	.index_validate_scan = orvosam_index_validate_scan,

	.relation_size = orvosam_relation_size,
	.relation_needs_toast_table = orvosam_relation_needs_toast_table,
	.relation_toast_am = NULL,	/* use default */
	.relation_fetch_toast_slice = NULL, /* use default */

	.relation_estimate_size = orvosam_relation_estimate_size,

	.scan_bitmap_next_tuple = orvosam_scan_bitmap_next_tuple,
	.scan_sample_next_block = orvosam_scan_sample_next_block,
	.scan_sample_next_tuple = orvosam_scan_sample_next_tuple
};

/* Table AM handler function */
PG_FUNCTION_INFO_V1(orvos_tableam_handler);

Datum
orvos_tableam_handler(PG_FUNCTION_ARGS)
{
	static bool initialized = false;

	/* Ensure initialization happens once */
	if (!initialized)
	{
		orvos_stats_init();
		orvos_planner_init();
		initialized = true;
	}

	PG_RETURN_POINTER(&orvosam_methods);
}

/*
 * Routines for dividing up the TID range for parallel seq scans
 */

typedef struct ParallelOVScanDescData
{
	ParallelTableScanDescData base;

	ovtid		pov_endtid;		/* last tid + 1 in relation at start of scan */
	pg_atomic_uint64 pov_allocatedtid_blk;	/* TID space allocated to workers
											 * so far. (in  65536 increments) */
}			ParallelOVScanDescData;
typedef struct ParallelOVScanDescData *ParallelOVScanDesc;

static Size
ov_parallelscan_estimate(Relation rel)
{
	(void) rel;
	return sizeof(ParallelOVScanDescData);
}

static Size
ov_parallelscan_initialize(Relation rel, ParallelTableScanDesc pscan)
{
	ParallelOVScanDesc ovscan = (ParallelOVScanDesc) pscan;

	/* phs_relid field removed from ParallelTableScanDesc */
	ovscan->pov_endtid = ovbt_get_last_tid(rel);
	pg_atomic_init_u64(&ovscan->pov_allocatedtid_blk, 0);

	return sizeof(ParallelOVScanDescData);
}

static void
ov_parallelscan_reinitialize(Relation rel, ParallelTableScanDesc pscan)
{
	ParallelOVScanDesc ovscan = (ParallelOVScanDesc) pscan;

	(void) rel;

	pg_atomic_write_u64(&ovscan->pov_allocatedtid_blk, 0);
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
ov_parallelscan_nextrange(Relation rel, ParallelOVScanDesc ovscan,
						  ovtid *start, ovtid *end)
{
	uint64		allocatedtid_blk;

	(void) rel;

	/*
	 * pov_allocatedtid_blk tracks how much has been allocated to workers
	 * already. When it exceeds rs_lasttid, all TIDs have been allocated.
	 *
	 * Because we use an atomic fetch-and-add to fetch the current value, the
	 * pov_allocatedtid_blk counter will exceed rs_lasttid, because workers
	 * will still increment the value, when they try to allocate the next
	 * block but all blocks have been allocated already. The counter must be
	 * 64 bits wide because of that, to avoid wrapping around when
	 * rs_lasttid is close to 2^32.  That's also one reason we do this at
	 * granularity of 2^16 TIDs, even though orvos isn't block-oriented.
	 *
	 * TODO: we divide the TID space into chunks of 2^16 TIDs each. That's
	 * pretty inefficient, there's a fair amount of overhead in re-starting
	 * the B-tree scans between each range. We probably should use much
	 * larger ranges. But this is good for testing.
	 */
	allocatedtid_blk = pg_atomic_fetch_add_u64(&ovscan->pov_allocatedtid_blk, 1);
	*start = OVTidFromBlkOff(allocatedtid_blk, 1);
	*end = OVTidFromBlkOff(allocatedtid_blk + 1, 1);

	return *start < ovscan->pov_endtid;
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
ovbt_fill_missing_attribute_value(TupleDesc tupleDesc, int attno, Datum *datum, bool *isnull)
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
				*datum = ov_datumCopy(attrmiss[attno - 1].am_value, attr->attbyval, attr->attlen);
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
#define OV_MAX_PREDECESSOR_DEPTH	10

static bool
ov_fetch_attr_with_predecessor(Relation rel, TupleDesc tupdesc,
							   AttrNumber attno, ovtid tid,
							   Datum *datum, bool *isnull)
{
	OVAttrTreeScan scan;
	ovtid		current_tid = tid;
	int			depth = 0;

	while (depth < OV_MAX_PREDECESSOR_DEPTH)
	{
		ovbt_attr_begin_scan(rel, tupdesc, (AttrNumber) attno, &scan);
		if (ovbt_attr_fetch(&scan, datum, isnull, current_tid))
		{
			/*
			 * CRITICAL: Copy non-byval datums before ending scan. The datum
			 * may point into a pinned buffer. Once we end the scan, that
			 * buffer will be unpinned and the datum pointer becomes dangling.
			 */
			if (!*isnull && !scan.attdesc->attbyval)
				*datum = ov_datumCopy(*datum, scan.attdesc->attbyval, scan.attdesc->attlen);

			ovbt_attr_end_scan(&scan);
			return true;
		}
		ovbt_attr_end_scan(&scan);

		/*
		 * Column not found for this TID. Check if the TID has a DELTA_INSERT
		 * UNDO record with a predecessor.
		 */
		{
			OVTidTreeScan tidscan;
			ovtid		found_tid;
			uint8		slotno;
			OVUndoRecPtr undoptr;
			OVUndoRec  *undorec;

			ovbt_tid_begin_scan(rel, current_tid,
								current_tid + 1,
								SnapshotAny, &tidscan);
			found_tid = ovbt_tid_scan_next(&tidscan,
										   ForwardScanDirection);
			if (found_tid == InvalidOVTid)
			{
				ovbt_tid_end_scan(&tidscan);
				break;
			}

			slotno = OVTidScanCurUndoSlotNo(&tidscan);
			undoptr = tidscan.array_iter.undoslots[slotno];
			ovbt_tid_end_scan(&tidscan);

			if (!IsOVUndoRecPtrValid(&undoptr))
				break;

			undorec = ovundo_fetch_record(rel, undoptr);
			if (undorec == NULL)
				break;

			/* Skip past any lock records */
			while (undorec->type == OVUNDO_TYPE_TUPLE_LOCK)
			{
				OVUndoRecPtr prev = undorec->prevundorec;

				pfree(undorec);
				if (!IsOVUndoRecPtrValid(&prev))
					goto not_found;
				undorec = ovundo_fetch_record(rel, prev);
				if (undorec == NULL)
					goto not_found;
			}

			if (undorec->type == OVUNDO_TYPE_DELTA_INSERT)
			{
				OVUndoRec_DeltaInsert *delta =
					(OVUndoRec_DeltaInsert *) undorec;

				if (!ov_delta_col_is_changed(delta, attno))
				{
					current_tid = delta->predecessor_tid;
					pfree(undorec);
					depth++;
					continue;
				}
			}

			pfree(undorec);
			break;
		}
	}

not_found:
	ovbt_fill_missing_attribute_value(tupdesc, attno, datum, isnull);
	return false;
}
