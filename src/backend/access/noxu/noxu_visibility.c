/*
 * noxu_visibility.c
 *		Routines for MVCC in Noxu
 *
 * Uses per-relation UNDO (RelUndoReadRecord) for visibility determination.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/noxu/noxu_visibility.c
 */
#include "postgres.h"

#include "access/relundo.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "access/noxu_internal.h"
#include "port/pg_lfind.h"
#include "storage/procarray.h"

/*
 * nx_old_record_is_visible
 *
 * When an UNDO record's counter predates the oldest visible generation,
 * we check the record TYPE and the inserting transaction's commit status
 * to decide visibility:
 *
 *   INSERT: Check if the inserting transaction committed. If it did,
 *           return visible. If it aborted (or is somehow still
 *           in-progress), return not visible.
 *   DELETE/UPDATE: The tuple was deleted/updated long ago. Return false.
 *   TUPLE_LOCK: Follow the chain to the previous record.
 *
 * If the record has been discarded (page recycled), we assume it was a
 * committed INSERT, which is the safe default. This is correct because:
 *   - Discarded DELETE/UPDATE records would only exist for tuples that
 *     were already cleaned up by VACUUM.
 *   - Aborted INSERT records should have had their TIDs marked DEAD by
 *     the rollback mechanism before the UNDO page was eligible for discard.
 *
 * The 'undo_ptr' parameter is updated to reflect the chain position when
 * following TUPLE_LOCK records.
 *
 * Returns:
 *   1  = record indicates tuple is visible (committed INSERT or discarded)
 *   0  = record indicates tuple is not visible (DELETE/UPDATE or aborted INSERT)
 */
static int
nx_old_record_is_visible(Relation rel, RelUndoRecPtr *undo_ptr)
{
	RelUndoRecordHeader hdr;
	void	   *payload = NULL;
	Size		payload_size;

	for (;;)
	{
		/* Try to read the record */
		if (!RelUndoReadRecord(rel, *undo_ptr, &hdr, &payload, &payload_size))
		{
			/* Record discarded - assume committed INSERT (visible) */
			return 1;
		}

		if (RELUNDO_TYPE_IS_INSERT(hdr.urec_type))
		{
			/*
			 * Old INSERT record.  We must verify the inserting transaction
			 * actually committed.  With asynchronous rollback, an aborted
			 * transaction's UNDO records can become "old" (their counter
			 * predates the oldest visible generation) before the background
			 * UNDO worker has had a chance to mark the TIDs as dead.
			 *
			 * If the xid is from the current transaction, it committed
			 * (we wouldn't be here during abort).  Otherwise, check the
			 * commit log.  For very old xids that have been truncated from
			 * the clog, TransactionIdDidCommit returns false, which is the
			 * correct conservative default.
			 */
			bool		visible;

			if (TransactionIdIsCurrentTransactionId(hdr.urec_xid))
				visible = true;
			else if (!TransactionIdIsValid(hdr.urec_xid))
				visible = false;	/* invalid xid, e.g. canceled speculative insert */
			else
				visible = TransactionIdDidCommit(hdr.urec_xid);

			if (payload)
				pfree(payload);
			return visible ? 1 : 0;
		}
		else if (hdr.urec_type == RELUNDO_DELETE ||
				 hdr.urec_type == RELUNDO_UPDATE)
		{
			if (payload)
				pfree(payload);
			return 0;			/* not visible */
		}
		else if (hdr.urec_type == RELUNDO_TUPLE_LOCK)
		{
			/* Follow chain to previous record */
			RelUndoRecPtr prev = hdr.urec_prevundorec;

			if (payload)
				pfree(payload);
			payload = NULL;
			*undo_ptr = prev;
			/* Continue loop to check the previous record */
		}
		else
		{
			if (payload)
				pfree(payload);
			elog(ERROR, "unexpected UNDO record type: %d", hdr.urec_type);
			return 0;			/* keep compiler quiet */
		}
	}
}

static bool
nx_tuplelock_compatible(LockTupleMode mode, LockTupleMode newmode)
{
	switch (newmode)
	{
		case LockTupleKeyShare:
			return mode == LockTupleKeyShare ||
				mode == LockTupleShare ||
				mode == LockTupleNoKeyExclusive;

		case LockTupleShare:
			return mode == LockTupleKeyShare ||
				mode == LockTupleShare;

		case LockTupleNoKeyExclusive:
			return mode == LockTupleKeyShare;
		case LockTupleExclusive:
			return false;

		default:
			elog(ERROR, "unknown tuple lock mode %d", newmode);
	}
}

/*
 * Walk the UNDO chain from the given pointer to find the INSERT record,
 * and check whether the inserting transaction committed.
 *
 * Returns true if the INSERT is "old" (before recent_oldest_undo) or if
 * the inserting transaction committed.  Returns false if the inserting
 * transaction aborted or is still in progress.
 *
 * This is used to avoid waiting on tuple locks when the inserting
 * transaction has already aborted (the tuple never really existed).
 */
static bool
nx_insert_is_committed(Relation rel, RelUndoRecPtr undo_ptr,
					   RelUndoRecPtr recent_oldest_undo)
{
	RelUndoRecordHeader hdr;
	void	   *payload;
	Size		payload_size;

	for (;;)
	{
		if (relundo_counter_precedes(RelUndoGetCounter(undo_ptr), RelUndoGetCounter(recent_oldest_undo)))
			return true;		/* old enough to be visible */

		if (!RelUndoReadRecord(rel, undo_ptr, &hdr, &payload, &payload_size))
		{
			recent_oldest_undo = nx_get_oldest_visible_undo_ptr(rel);
			if (!relundo_counter_precedes(RelUndoGetCounter(undo_ptr), RelUndoGetCounter(recent_oldest_undo)))
				elog(ERROR, "could not find UNDO record " UINT64_FORMAT " at blk %u offset %u",
					 (uint64) RelUndoGetCounter(undo_ptr), RelUndoGetBlockNum(undo_ptr), RelUndoGetOffset(undo_ptr));
			return true;		/* concurrent trim, assume visible */
		}

		if (RELUNDO_TYPE_IS_INSERT(hdr.urec_type))
		{
			bool		result;

			if (TransactionIdIsCurrentTransactionId(hdr.urec_xid))
				result = true;
			else if (TransactionIdIsInProgress(hdr.urec_xid))
				result = false;
			else
				result = TransactionIdDidCommit(hdr.urec_xid);

			pfree(payload);
			return result;
		}

		/* Skip TUPLE_LOCK, DELETE, UPDATE records to reach the INSERT */
		undo_ptr = hdr.urec_prevundorec;
		pfree(payload);
	}
}

static bool
am_i_holding_lock(Relation rel, RelUndoRecPtr undo_ptr,
				  RelUndoRecPtr recent_oldest_undo)
{
	RelUndoRecordHeader hdr;
	void	   *payload;
	Size		payload_size;

	for (;;)
	{
		/* Is it visible? */
		if (relundo_counter_precedes(RelUndoGetCounter(undo_ptr), RelUndoGetCounter(recent_oldest_undo)))
			return false;

		/* have to fetch the UNDO record */
		if (!RelUndoReadRecord(rel, undo_ptr, &hdr, &payload, &payload_size))
		{
			recent_oldest_undo = nx_get_oldest_visible_undo_ptr(rel);
			if (!relundo_counter_precedes(RelUndoGetCounter(undo_ptr), RelUndoGetCounter(recent_oldest_undo)))
				elog(ERROR, "could not find UNDO record " UINT64_FORMAT " at blk %u offset %u",
					 (uint64) RelUndoGetCounter(undo_ptr), RelUndoGetBlockNum(undo_ptr), RelUndoGetOffset(undo_ptr));
			return false;
		}

		if (TransactionIdIsCurrentTransactionId(hdr.urec_xid))
		{
			/*
			 * Any record type (INSERT, TUPLE_LOCK, DELETE, UPDATE) by the
			 * current transaction means we hold a lock.
			 */
			pfree(payload);
			return true;
		}

		undo_ptr = hdr.urec_prevundorec;
		pfree(payload);
	}
}

/*
 * When returns TM_Ok, this also returns a flag in *undo_record_needed, to indicate
 * whether the old UNDO record is still of interest to anyone. If the old record
 * belonged to an aborted deleting transaction, for example, it can be ignored.
 *
 * This does more than HeapTupleSatisfiesUpdate. If HeapTupleSatisfiesUpdate sees
 * an updated or locked tuple, it returns TM_BeingUpdated, and the caller has to
 * check if the tuple lock is compatible with the update. nx_SatisfiesUpdate
 * checks if the new lock mode is compatible with the old one, and returns TM_Ok
 * if so. Waiting for conflicting locks is left to the caller.
 *
 * This is also used for tuple locking (e.g. SELECT FOR UPDATE). 'mode' indicates
 * the lock mode. For a genuine UPDATE, pass LockTupleExclusive or
 * LockTupleNoKeyExclusive depending on whether key columns are being modified.
 *
 * If the tuple was UPDATEd, *next_tid is set to the TID of the new row version.
 *
 * Similar to: HeapTupleSatisfiesUpdate.
 */
TM_Result
nx_SatisfiesUpdate(Relation rel, Snapshot snapshot,
				   RelUndoRecPtr recent_oldest_undo,
				   nxtid item_tid, RelUndoRecPtr item_undoptr,
				   LockTupleMode mode,
				   bool *undo_record_needed, bool *this_xact_has_lock,
				   TM_FailureData *tmfd,
				   nxtid *next_tid, NXUndoSlotVisibility *visi_info)
{
	RelUndoRecPtr undo_ptr;
	RelUndoRecordHeader hdr;
	void	   *payload = NULL;
	Size		payload_size;
	int			chain_depth = 0;

	*this_xact_has_lock = false;
	*undo_record_needed = true;

	undo_ptr = item_undoptr;

fetch_undo_record:
	chain_depth++;

	/* Free payload from previous iteration if any */
	if (payload)
	{
		pfree(payload);
		payload = NULL;
	}

retry_fetch:
	/*
	 * If this record is "old", check its type to determine visibility.
	 * An old INSERT means available for update (TM_Ok); an old
	 * DELETE/UPDATE means the tuple has been removed (TM_Invisible).
	 */
	if (relundo_counter_precedes(RelUndoGetCounter(undo_ptr), RelUndoGetCounter(recent_oldest_undo)))
	{
		int			old_vis = nx_old_record_is_visible(rel, &undo_ptr);

		if (old_vis == 1)
		{
			/*
			 * Old INSERT. The undo record is no longer needed by anyone.
			 * If this is the first record in the chain, we can discard it.
			 */
			if (chain_depth == 1)
				*undo_record_needed = false;

			if (visi_info)
			{
				visi_info->xmin = FrozenTransactionId;
				visi_info->cmin = InvalidCommandId;
			}
			return TM_Ok;
		}
		else
		{
			/* Old DELETE/UPDATE: tuple has been deleted, not available */
			return TM_Invisible;
		}
	}

	/* have to fetch the UNDO record */
	if (!RelUndoReadRecord(rel, undo_ptr, &hdr, &payload, &payload_size))
	{
		recent_oldest_undo = nx_get_oldest_visible_undo_ptr(rel);
		if (!relundo_counter_precedes(RelUndoGetCounter(undo_ptr), RelUndoGetCounter(recent_oldest_undo)))
			elog(ERROR, "could not find UNDO record " UINT64_FORMAT " at blk %u offset %u",
				 (uint64) RelUndoGetCounter(undo_ptr), RelUndoGetBlockNum(undo_ptr), RelUndoGetOffset(undo_ptr));
		goto retry_fetch;
	}

	if (RELUNDO_TYPE_IS_INSERT(hdr.urec_type))
	{
		if (visi_info)
		{
			visi_info->xmin = hdr.urec_xid;
			visi_info->cmin = hdr.urec_cid;
		}

		if (TransactionIdIsCurrentTransactionId(hdr.urec_xid))
		{
			*this_xact_has_lock = true;
			if (hdr.urec_cid >= snapshot->curcid)
			{
				pfree(payload);
				return TM_Invisible;	/* inserted after scan started */
			}
		}
		else if (TransactionIdIsInProgress(hdr.urec_xid))
		{
			pfree(payload);
			return TM_Invisible;	/* inserter has not committed yet */
		}
		else if (!TransactionIdDidCommit(hdr.urec_xid))
		{
			/* it must have aborted or crashed */
			pfree(payload);
			return TM_Invisible;
		}

		/*
		 * The inserting transaction committed (or is ours). The tuple is
		 * visible. Return TM_Ok -- we don't need to check further records
		 * in the chain beyond the INSERT.
		 */
		pfree(payload);
		return TM_Ok;
	}
	else if (hdr.urec_type == RELUNDO_TUPLE_LOCK)
	{
		RelUndoTupleLockPayload *lock_payload = (RelUndoTupleLockPayload *) payload;

		/*
		 * If any subtransaction of the current top transaction already holds
		 * a lock as strong as or stronger than what we're requesting, we
		 * effectively hold the desired lock already.  We *must* succeed
		 * without trying to take the tuple lock, else we will deadlock
		 * against anyone wanting to acquire a stronger lock.
		 */
		if (TransactionIdIsCurrentTransactionId(hdr.urec_xid))
		{
			*this_xact_has_lock = true;
			if (lock_payload->lock_mode >= mode)
			{
				*undo_record_needed = true;
				pfree(payload);
				return TM_Ok;
			}
		}
		else if (!nx_tuplelock_compatible(lock_payload->lock_mode, mode) &&
				 TransactionIdIsInProgress(hdr.urec_xid))
		{
			/*
			 * Before waiting on a conflicting lock, check if the tuple's
			 * inserting transaction actually committed. If it aborted, the
			 * tuple never really existed and we should not wait.
			 */
			RelUndoRecPtr prev = hdr.urec_prevundorec;

			pfree(payload);
			payload = NULL;

			if (!nx_insert_is_committed(rel, prev, recent_oldest_undo))
				return TM_Invisible;

			tmfd->ctid = ItemPointerFromNXTid(item_tid);
			tmfd->xmax = hdr.urec_xid;
			tmfd->cmax = InvalidCommandId;

			/* but am I holding a weaker lock already? */
			if (!*this_xact_has_lock)
				*this_xact_has_lock = am_i_holding_lock(rel, prev, recent_oldest_undo);

			return TM_BeingModified;
		}

		/*
		 * No conflict with this lock. Look at the previous UNDO record,
		 * there might be more locks, or we will reach the INSERT record
		 * to verify visibility.
		 */
		undo_ptr = hdr.urec_prevundorec;
		goto fetch_undo_record;
	}
	else if (hdr.urec_type == RELUNDO_DELETE)
	{
		RelUndoDeletePayload *del_payload = (RelUndoDeletePayload *) payload;

		if (visi_info)
		{
			visi_info->xmin = hdr.urec_xid;
			visi_info->cmin = hdr.urec_cid;
		}

		if (TransactionIdIsCurrentTransactionId(hdr.urec_xid))
		{
			*this_xact_has_lock = true;
			if (hdr.urec_cid >= snapshot->curcid)
			{
				tmfd->ctid = ItemPointerFromNXTid(item_tid);
				tmfd->xmax = hdr.urec_xid;
				tmfd->cmax = hdr.urec_cid;
				pfree(payload);
				return TM_SelfModified; /* deleted/updated after scan started */
			}
			else
			{
				pfree(payload);
				return TM_Invisible;	/* deleted before scan started */
			}
		}

		if (TransactionIdIsInProgress(hdr.urec_xid))
		{
			tmfd->ctid = ItemPointerFromNXTid(item_tid);
			tmfd->xmax = hdr.urec_xid;
			tmfd->cmax = InvalidCommandId;

			/* but am I holding a weaker lock already? */
			if (!*this_xact_has_lock)
				*this_xact_has_lock = am_i_holding_lock(rel, hdr.urec_prevundorec, recent_oldest_undo);

			pfree(payload);
			return TM_BeingModified;
		}

		if (!TransactionIdDidCommit(hdr.urec_xid))
		{
			/*
			 * deleter must have aborted or crashed. We have to keep following
			 * the undo chain, in case there are LOCK records that are still
			 * visible
			 */
			undo_ptr = hdr.urec_prevundorec;
			goto fetch_undo_record;
		}

		tmfd->xmax = hdr.urec_xid;
		tmfd->cmax = InvalidCommandId;
		if (del_payload->changedPart)
		{
			ItemPointerSet(&tmfd->ctid, MovedPartitionsBlockNumber, MovedPartitionsOffsetNumber);
			*next_tid = InvalidNXTid;
			pfree(payload);
			return TM_Updated;
		}
		else
		{
			tmfd->ctid = ItemPointerFromNXTid(item_tid);
			pfree(payload);
			return TM_Deleted;
		}
	}
	else if (hdr.urec_type == RELUNDO_UPDATE)
	{
		/* updated-away tuple */
		RelUndoUpdatePayload *upd_payload = (RelUndoUpdatePayload *) payload;
		LockTupleMode old_lockmode;

		if (visi_info)
		{
			visi_info->xmin = hdr.urec_xid;
			visi_info->cmin = hdr.urec_cid;
		}

		*next_tid = NXTidFromItemPointer(upd_payload->newtid);
		old_lockmode = upd_payload->key_update ? LockTupleExclusive : LockTupleNoKeyExclusive;

		if (TransactionIdIsCurrentTransactionId(hdr.urec_xid))
		{
			*this_xact_has_lock = true;
			if (nx_tuplelock_compatible(old_lockmode, mode))
			{
				pfree(payload);
				return TM_Ok;
			}

			if (hdr.urec_cid >= snapshot->curcid)
			{
				tmfd->ctid = ItemPointerFromNXTid(item_tid);
				tmfd->xmax = hdr.urec_xid;
				tmfd->cmax = hdr.urec_cid;
				pfree(payload);
				return TM_SelfModified; /* deleted/updated after scan started */
			}
			else
			{
				pfree(payload);
				return TM_Invisible;	/* deleted before scan started */
			}
		}

		if (TransactionIdIsInProgress(hdr.urec_xid))
		{
			if (nx_tuplelock_compatible(old_lockmode, mode))
			{
				pfree(payload);
				return TM_Ok;
			}

			tmfd->ctid = ItemPointerFromNXTid(item_tid);
			tmfd->xmax = hdr.urec_xid;
			tmfd->cmax = InvalidCommandId;

			/* but am I holding a weaker lock already? */
			if (!*this_xact_has_lock)
				*this_xact_has_lock = am_i_holding_lock(rel, hdr.urec_prevundorec, recent_oldest_undo);

			pfree(payload);
			return TM_BeingModified;
		}

		if (!TransactionIdDidCommit(hdr.urec_xid))
		{
			/*
			 * deleter must have aborted or crashed. We have to keep following
			 * the undo chain, in case there are LOCK records that are still
			 * visible
			 */
			undo_ptr = hdr.urec_prevundorec;
			goto fetch_undo_record;
		}

		if (nx_tuplelock_compatible(old_lockmode, mode))
		{
			pfree(payload);
			return TM_Ok;
		}

		tmfd->ctid = ItemPointerFromNXTid(NXTidFromItemPointer(upd_payload->newtid));
		tmfd->xmax = hdr.urec_xid;
		tmfd->cmax = InvalidCommandId;
		pfree(payload);
		return TM_Updated;
	}
	else
	{
		pfree(payload);
		elog(ERROR, "unexpected UNDO record type: %d", hdr.urec_type);
	}
}


/*
 * Similar to: HeapTupleSatisfiesAny
 */
static bool
nx_SatisfiesAny(NXTidTreeScan * scan, RelUndoRecPtr item_undoptr, NXUndoSlotVisibility *visi_info)
{
	Relation	rel = scan->rel;
	RelUndoRecPtr undo_ptr;
	RelUndoRecordHeader hdr;
	void	   *payload = NULL;
	Size		payload_size;

	undo_ptr = item_undoptr;

fetch_undo_record:
	/* Free payload from previous iteration if any */
	if (payload)
	{
		pfree(payload);
		payload = NULL;
	}

	/*
	 * If this record is "old", check its type to determine visibility.
	 * An old INSERT means visible; an old DELETE/UPDATE means not visible.
	 */
	if (relundo_counter_precedes(RelUndoGetCounter(undo_ptr), RelUndoGetCounter(scan->recent_oldest_undo)))
	{
		int			old_vis = nx_old_record_is_visible(rel, &undo_ptr);

		if (old_vis == 1)
		{
			visi_info->xmin = FrozenTransactionId;
			visi_info->cmin = InvalidCommandId;
			return true;
		}
		else
		{
			/* old DELETE/UPDATE: tuple is not visible */
			return false;
		}
	}

	/* have to fetch the UNDO record */
	if (!RelUndoReadRecord(rel, undo_ptr, &hdr, &payload, &payload_size))
	{
		scan->recent_oldest_undo = nx_get_oldest_visible_undo_ptr(rel);
		if (!relundo_counter_precedes(RelUndoGetCounter(undo_ptr), RelUndoGetCounter(scan->recent_oldest_undo)))
			elog(ERROR, "could not find UNDO record " UINT64_FORMAT " at blk %u offset %u",
				 (uint64) RelUndoGetCounter(undo_ptr), RelUndoGetBlockNum(undo_ptr), RelUndoGetOffset(undo_ptr));
		goto fetch_undo_record;
	}

	if (RELUNDO_TYPE_IS_INSERT(hdr.urec_type))
	{
		visi_info->xmin = hdr.urec_xid;
		visi_info->cmin = hdr.urec_cid;
		pfree(payload);
		return true;
	}
	else if (hdr.urec_type == RELUNDO_DELETE ||
			 hdr.urec_type == RELUNDO_UPDATE ||
			 hdr.urec_type == RELUNDO_TUPLE_LOCK)
	{
		undo_ptr = hdr.urec_prevundorec;
		goto fetch_undo_record;
	}
	else
	{
		pfree(payload);
		elog(ERROR, "unexpected UNDO record type: %d", hdr.urec_type);
	}

	return true;
}

/*
 * helper function to nx_SatisfiesMVCC(), to check if the given XID
 * is visible to the snapshot.
 */
static bool
xid_is_visible(Snapshot snapshot, TransactionId xid, CommandId cid, bool *aborted)
{
	*aborted = false;
	if (TransactionIdIsCurrentTransactionId(xid))
	{
		if (cid >= snapshot->curcid)
			return false;
		else
			return true;
	}
	else if (XidInMVCCSnapshot(xid, snapshot))
		return false;
	else if (TransactionIdDidCommit(xid))
	{
		return true;
	}
	else
	{
		/* it must have aborted or crashed */
		*aborted = true;
		return false;
	}
}

/*
 * Similar to: HeapTupleSatisfiesMVCC
 */
static bool
nx_SatisfiesMVCC(NXTidTreeScan * scan, RelUndoRecPtr item_undoptr,
				 TransactionId *obsoleting_xid, nxtid *next_tid,
				 NXUndoSlotVisibility *visi_info)
{
	Relation	rel = scan->rel;
	Snapshot	snapshot = scan->snapshot;
	RelUndoRecPtr undo_ptr;
	RelUndoRecordHeader hdr;
	void	   *payload = NULL;
	Size		payload_size;
	bool		aborted;

	undo_ptr = item_undoptr;

fetch_undo_record:
	/* Free payload from previous iteration if any */
	if (payload)
	{
		pfree(payload);
		payload = NULL;
	}

	/*
	 * If this record is "old", check its type to determine visibility.
	 * An old INSERT means visible; an old DELETE/UPDATE means not visible.
	 */
	if (relundo_counter_precedes(RelUndoGetCounter(undo_ptr), RelUndoGetCounter(scan->recent_oldest_undo)))
	{
		int			old_vis = nx_old_record_is_visible(rel, &undo_ptr);

		if (old_vis == 1)
		{
			visi_info->xmin = FrozenTransactionId;
			visi_info->cmin = InvalidCommandId;
			return true;
		}
		else
		{
			/* old DELETE/UPDATE: tuple is not visible */
			return false;
		}
	}

	/* have to fetch the UNDO record */
	if (!RelUndoReadRecord(rel, undo_ptr, &hdr, &payload, &payload_size))
	{
		scan->recent_oldest_undo = nx_get_oldest_visible_undo_ptr(rel);
		if (!relundo_counter_precedes(RelUndoGetCounter(undo_ptr), RelUndoGetCounter(scan->recent_oldest_undo)))
			elog(ERROR, "could not find UNDO record " UINT64_FORMAT " at blk %u offset %u",
				 (uint64) RelUndoGetCounter(undo_ptr), RelUndoGetBlockNum(undo_ptr), RelUndoGetOffset(undo_ptr));
		goto fetch_undo_record;
	}

	if (RELUNDO_TYPE_IS_INSERT(hdr.urec_type))
	{
		/* Inserted tuple */
		bool		result;

		result = xid_is_visible(snapshot, hdr.urec_xid, hdr.urec_cid, &aborted);
		if (!result && !aborted)
			*obsoleting_xid = hdr.urec_xid;

		visi_info->xmin = hdr.urec_xid;
		visi_info->cmin = hdr.urec_cid;

		pfree(payload);
		return result;
	}
	else if (hdr.urec_type == RELUNDO_TUPLE_LOCK)
	{
		/*
		 * we don't care about tuple locks here. Follow the link to the
		 * previous UNDO record for this tuple.
		 */
		undo_ptr = hdr.urec_prevundorec;
		goto fetch_undo_record;
	}
	else if (hdr.urec_type == RELUNDO_DELETE ||
			 hdr.urec_type == RELUNDO_UPDATE)
	{
		if (hdr.urec_type == RELUNDO_UPDATE)
		{
			RelUndoUpdatePayload *upd_payload = (RelUndoUpdatePayload *) payload;

			if (next_tid)
				*next_tid = NXTidFromItemPointer(upd_payload->newtid);
		}

		/*
		 * Deleted or updated-away. They are treated the same in an MVCC
		 * snapshot. They only need different treatment when updating or
		 * locking the row, in SatisfiesUpdate().
		 */
		if (xid_is_visible(snapshot, hdr.urec_xid, hdr.urec_cid, &aborted))
		{
			/* we can see the deletion */
			pfree(payload);
			return false;
		}
		else
		{
			if (!aborted)
				*obsoleting_xid = hdr.urec_xid;
			undo_ptr = hdr.urec_prevundorec;
			goto fetch_undo_record;
		}
	}
	else
	{
		pfree(payload);
		elog(ERROR, "unexpected UNDO record type: %d", hdr.urec_type);
	}
}

/*
 * Similar to: HeapTupleSatisfiesSelf
 */
static bool
nx_SatisfiesSelf(NXTidTreeScan * scan, RelUndoRecPtr item_undoptr,
				 nxtid *next_tid, NXUndoSlotVisibility *visi_info)
{
	Relation	rel = scan->rel;
	RelUndoRecordHeader hdr;
	void	   *payload = NULL;
	Size		payload_size;
	RelUndoRecPtr undo_ptr;

	undo_ptr = item_undoptr;

fetch_undo_record:
	/* Free payload from previous iteration if any */
	if (payload)
	{
		pfree(payload);
		payload = NULL;
	}

	if (relundo_counter_precedes(RelUndoGetCounter(undo_ptr), RelUndoGetCounter(scan->recent_oldest_undo)))
	{
		visi_info->xmin = FrozenTransactionId;
		visi_info->cmin = InvalidCommandId;
		return true;
	}

	/* have to fetch the UNDO record */
	if (!RelUndoReadRecord(rel, undo_ptr, &hdr, &payload, &payload_size))
	{
		scan->recent_oldest_undo = nx_get_oldest_visible_undo_ptr(rel);
		if (!relundo_counter_precedes(RelUndoGetCounter(undo_ptr), RelUndoGetCounter(scan->recent_oldest_undo)))
			elog(ERROR, "could not find UNDO record " UINT64_FORMAT " at blk %u offset %u",
				 (uint64) RelUndoGetCounter(undo_ptr), RelUndoGetBlockNum(undo_ptr), RelUndoGetOffset(undo_ptr));
		goto fetch_undo_record;
	}

	if (RELUNDO_TYPE_IS_INSERT(hdr.urec_type))
	{
		visi_info->xmin = hdr.urec_xid;
		visi_info->cmin = hdr.urec_cid;

		/* Inserted tuple */
		if (TransactionIdIsCurrentTransactionId(hdr.urec_xid))
		{
			pfree(payload);
			return true;		/* inserted by me */
		}
		else if (TransactionIdIsInProgress(hdr.urec_xid))
		{
			pfree(payload);
			return false;
		}
		else if (TransactionIdDidCommit(hdr.urec_xid))
		{
			pfree(payload);
			return true;
		}
		else
		{
			/* it must have aborted or crashed */
			pfree(payload);
			return false;
		}
	}
	else if (hdr.urec_type == RELUNDO_TUPLE_LOCK)
	{
		/*
		 * we don't care about tuple locks here. Follow the link to the
		 * previous UNDO record for this tuple.
		 */
		undo_ptr = hdr.urec_prevundorec;
		goto fetch_undo_record;
	}
	else if (hdr.urec_type == RELUNDO_DELETE ||
			 hdr.urec_type == RELUNDO_UPDATE)
	{
		if (hdr.urec_type == RELUNDO_UPDATE)
		{
			RelUndoUpdatePayload *upd_payload = (RelUndoUpdatePayload *) payload;

			if (next_tid)
				*next_tid = NXTidFromItemPointer(upd_payload->newtid);
		}

		if (TransactionIdIsCurrentTransactionId(hdr.urec_xid))
		{
			/* deleted by me */
			pfree(payload);
			return false;
		}

		if (TransactionIdIsInProgress(hdr.urec_xid))
		{
			pfree(payload);
			return true;
		}

		if (!TransactionIdDidCommit(hdr.urec_xid))
		{
			/*
			 * Deleter must have aborted or crashed. But we have to keep
			 * following the undo chain, to check if the insertion was visible
			 * in the first place.
			 */
			undo_ptr = hdr.urec_prevundorec;
			goto fetch_undo_record;
		}

		pfree(payload);
		return false;
	}
	else
	{
		pfree(payload);
		elog(ERROR, "unexpected UNDO record type: %d", hdr.urec_type);
	}
}

/*
 * Similar to: HeapTupleSatisfiesDirty
 */
static bool
nx_SatisfiesDirty(NXTidTreeScan * scan, RelUndoRecPtr item_undoptr,
				  nxtid *next_tid, NXUndoSlotVisibility *visi_info)
{
	Relation	rel = scan->rel;
	Snapshot	snapshot = scan->snapshot;
	RelUndoRecPtr undo_ptr;
	RelUndoRecordHeader hdr;
	void	   *payload = NULL;
	Size		payload_size;

	snapshot->xmin = snapshot->xmax = InvalidTransactionId;
	snapshot->speculativeToken = INVALID_SPECULATIVE_TOKEN;

	undo_ptr = item_undoptr;

fetch_undo_record:
	/* Free payload from previous iteration if any */
	if (payload)
	{
		pfree(payload);
		payload = NULL;
	}

	if (relundo_counter_precedes(RelUndoGetCounter(undo_ptr), RelUndoGetCounter(scan->recent_oldest_undo)))
	{
		visi_info->xmin = FrozenTransactionId;
		visi_info->cmin = InvalidCommandId;
		return true;
	}

	/* have to fetch the UNDO record */
	if (!RelUndoReadRecord(rel, undo_ptr, &hdr, &payload, &payload_size))
	{
		scan->recent_oldest_undo = nx_get_oldest_visible_undo_ptr(rel);
		if (!relundo_counter_precedes(RelUndoGetCounter(undo_ptr), RelUndoGetCounter(scan->recent_oldest_undo)))
			elog(ERROR, "could not find UNDO record " UINT64_FORMAT " at blk %u offset %u",
				 (uint64) RelUndoGetCounter(undo_ptr), RelUndoGetBlockNum(undo_ptr), RelUndoGetOffset(undo_ptr));
		goto fetch_undo_record;
	}

	if (RELUNDO_TYPE_IS_INSERT(hdr.urec_type))
	{
		RelUndoInsertPayload *ins_payload = (RelUndoInsertPayload *) payload;

		snapshot->speculativeToken = ins_payload->speculative_token;

		/*
		 * HACK: For SnapshotDirty need to set the values of xmin/xmax/... in
		 * snapshot based on tuples. Hence, can't set the visi_info values
		 * here similar to other snapshots. Only setting the value for
		 * TransactionIdIsInProgress().
		 */

		/* Inserted tuple */
		if (TransactionIdIsCurrentTransactionId(hdr.urec_xid))
		{
			pfree(payload);
			return true;		/* inserted by me */
		}
		else if (TransactionIdIsInProgress(hdr.urec_xid))
		{
			snapshot->xmin = hdr.urec_xid;
			visi_info->xmin = hdr.urec_xid;
			visi_info->cmin = hdr.urec_cid;
			pfree(payload);
			return true;
		}
		else if (TransactionIdDidCommit(hdr.urec_xid))
		{
			pfree(payload);
			return true;
		}
		else
		{
			/* it must have aborted or crashed */
			pfree(payload);
			return false;
		}
	}
	else if (hdr.urec_type == RELUNDO_TUPLE_LOCK)
	{
		/* locked tuple. */
		/* look at the previous UNDO record to find the insert record */
		undo_ptr = hdr.urec_prevundorec;
		goto fetch_undo_record;
	}
	else if (hdr.urec_type == RELUNDO_DELETE ||
			 hdr.urec_type == RELUNDO_UPDATE)
	{
		if (hdr.urec_type == RELUNDO_UPDATE)
		{
			RelUndoUpdatePayload *upd_payload = (RelUndoUpdatePayload *) payload;

			if (next_tid)
				*next_tid = NXTidFromItemPointer(upd_payload->newtid);
		}

		/* deleted or updated-away tuple */
		if (TransactionIdIsCurrentTransactionId(hdr.urec_xid))
		{
			/* deleted by me */
			pfree(payload);
			return false;
		}

		if (TransactionIdIsInProgress(hdr.urec_xid))
		{
			/*
			 * Set xmax in both the snapshot and visi_info. The snapshot's
			 * xmax is needed so that the caller can detect the in-progress
			 * delete/update and respond appropriately (e.g., wait or skip).
			 * visi_info->xmax is also populated here because callers may
			 * inspect it before re-checking the snapshot.
			 */
			snapshot->xmax = hdr.urec_xid;
			visi_info->xmax = hdr.urec_xid;
			pfree(payload);
			return true;
		}

		if (!TransactionIdDidCommit(hdr.urec_xid))
		{
			/*
			 * Deleter must have aborted or crashed. But we have to keep
			 * following the undo chain, to check if the insertion was visible
			 * in the first place.
			 */
			undo_ptr = hdr.urec_prevundorec;
			goto fetch_undo_record;
		}

		pfree(payload);
		return false;
	}
	else
	{
		pfree(payload);
		elog(ERROR, "unexpected UNDO record type: %d", hdr.urec_type);
	}
}

/*
 * True if tuple might be visible to some transaction; false if it's
 * surely dead to everyone, ie, vacuumable.
 */
static bool
nx_SatisfiesNonVacuumable(NXTidTreeScan * scan, RelUndoRecPtr item_undoptr,
						  NXUndoSlotVisibility *visi_info)
{
	Relation	rel = scan->rel;
	TransactionId OldestXmin = scan->snapshot->xmin;
	RelUndoRecPtr undo_ptr;
	RelUndoRecordHeader hdr;
	void	   *payload = NULL;
	Size		payload_size;

	Assert(TransactionIdIsValid(OldestXmin));

	undo_ptr = item_undoptr;

fetch_undo_record:
	/* Free payload from previous iteration if any */
	if (payload)
	{
		pfree(payload);
		payload = NULL;
	}

	/* Is it visible? */
	if (relundo_counter_precedes(RelUndoGetCounter(undo_ptr), RelUndoGetCounter(scan->recent_oldest_undo)))
	{
		visi_info->xmin = FrozenTransactionId;
		visi_info->cmin = InvalidCommandId;
		return true;
	}

	/* have to fetch the UNDO record */
	if (!RelUndoReadRecord(rel, undo_ptr, &hdr, &payload, &payload_size))
	{
		scan->recent_oldest_undo = nx_get_oldest_visible_undo_ptr(rel);
		if (!relundo_counter_precedes(RelUndoGetCounter(undo_ptr), RelUndoGetCounter(scan->recent_oldest_undo)))
			elog(ERROR, "could not find UNDO record " UINT64_FORMAT " at blk %u offset %u",
				 (uint64) RelUndoGetCounter(undo_ptr), RelUndoGetBlockNum(undo_ptr), RelUndoGetOffset(undo_ptr));
		goto fetch_undo_record;
	}

	if (RELUNDO_TYPE_IS_INSERT(hdr.urec_type))
	{
		visi_info->xmin = hdr.urec_xid;
		visi_info->cmin = hdr.urec_cid;

		/* Inserted tuple */
		if (TransactionIdIsInProgress(hdr.urec_xid))
		{
			pfree(payload);
			return true;		/* inserter has not committed yet */
		}

		if (TransactionIdDidCommit(hdr.urec_xid))
		{
			pfree(payload);
			return true;
		}

		/* it must have aborted or crashed */
		pfree(payload);
		return false;
	}
	else if (hdr.urec_type == RELUNDO_DELETE ||
			 hdr.urec_type == RELUNDO_UPDATE)
	{
		/* deleted or updated-away tuple */
		RelUndoRecPtr prevptr;

		if (TransactionIdIsInProgress(hdr.urec_xid))
		{
			pfree(payload);
			return true;		/* delete-in-progress */
		}
		else if (TransactionIdDidCommit(hdr.urec_xid))
		{
			/*
			 * Deleter committed. But perhaps it was recent enough that some
			 * open transactions could still see the tuple.
			 */
			if (!TransactionIdPrecedes(hdr.urec_xid, OldestXmin))
			{
				visi_info->nonvacuumable_status = NXNV_RECENTLY_DEAD;
				pfree(payload);
				return true;
			}

			pfree(payload);
			return false;
		}

		/*
		 * The deleting transaction did not commit. But before concluding that
		 * the tuple is live, we have to check if the inserting XID is live.
		 */
		prevptr = hdr.urec_prevundorec;
		pfree(payload);
		payload = NULL;

		do
		{
			if (relundo_counter_precedes(RelUndoGetCounter(prevptr), RelUndoGetCounter(scan->recent_oldest_undo)))
				return true;
			if (!RelUndoReadRecord(rel, prevptr, &hdr, &payload, &payload_size))
			{
				scan->recent_oldest_undo = nx_get_oldest_visible_undo_ptr(rel);
				if (!relundo_counter_precedes(RelUndoGetCounter(prevptr), RelUndoGetCounter(scan->recent_oldest_undo)))
					elog(ERROR, "could not find UNDO record " UINT64_FORMAT " at blk %u offset %u",
						 (uint64) RelUndoGetCounter(prevptr), RelUndoGetBlockNum(prevptr), RelUndoGetOffset(prevptr));
				return true;
			}

			if (hdr.urec_type != RELUNDO_TUPLE_LOCK)
				break;

			prevptr = hdr.urec_prevundorec;
			pfree(payload);
			payload = NULL;
		} while (true);

		Assert(RELUNDO_TYPE_IS_INSERT(hdr.urec_type));

		if (TransactionIdIsInProgress(hdr.urec_xid))
		{
			pfree(payload);
			return true;		/* insert-in-progress */
		}
		else if (TransactionIdDidCommit(hdr.urec_xid))
		{
			pfree(payload);
			return true;		/* inserted committed */
		}

		/* inserter must have aborted or crashed */
		pfree(payload);
		return false;
	}
	else if (hdr.urec_type == RELUNDO_TUPLE_LOCK)
	{
		/* look at the previous UNDO record, to find the Insert record */
		undo_ptr = hdr.urec_prevundorec;
		goto fetch_undo_record;
	}
	else
	{
		pfree(payload);
		elog(ERROR, "unexpected UNDO record type: %d", hdr.urec_type);
	}
}

/*
 * In Noxu, overflow data is stored internally in overflow pages within the same
 * relation, not in a separate toast table as is the case in heap. The semantics
 * of SnapshotOverflow are: if you can see the main table row that references
 * the overflow data, you should be able to see the overflow value. The only
 * exception is tuples from aborted transactions (including speculative
 * insertions).
 *
 * This is essentially the same as SnapshotAny, but we skip tuples whose
 * inserting transaction aborted.
 *
 * Similar to: HeapTupleSatisfiesToast
 */
static bool
nx_SatisfiesOverflow(NXTidTreeScan *scan, RelUndoRecPtr item_undoptr,
				  NXUndoSlotVisibility *visi_info)
{
	Relation	rel = scan->rel;
	RelUndoRecPtr undo_ptr;
	RelUndoRecordHeader hdr;
	void	   *payload = NULL;
	Size		payload_size;

	undo_ptr = item_undoptr;

fetch_undo_record:
	/* Free payload from previous iteration if any */
	if (payload)
	{
		pfree(payload);
		payload = NULL;
	}

	/*
	 * If this record is "old", check its type to determine visibility.
	 * An old INSERT means visible; an old DELETE/UPDATE means not visible.
	 */
	if (relundo_counter_precedes(RelUndoGetCounter(undo_ptr), RelUndoGetCounter(scan->recent_oldest_undo)))
	{
		int			old_vis = nx_old_record_is_visible(rel, &undo_ptr);

		if (old_vis == 1)
		{
			visi_info->xmin = FrozenTransactionId;
			visi_info->cmin = InvalidCommandId;
			return true;
		}
		else
		{
			/* old DELETE/UPDATE: tuple is not visible */
			return false;
		}
	}

	/* have to fetch the UNDO record */
	if (!RelUndoReadRecord(rel, undo_ptr, &hdr, &payload, &payload_size))
	{
		scan->recent_oldest_undo = nx_get_oldest_visible_undo_ptr(rel);
		if (!relundo_counter_precedes(RelUndoGetCounter(undo_ptr), RelUndoGetCounter(scan->recent_oldest_undo)))
			elog(ERROR, "could not find UNDO record " UINT64_FORMAT " at blk %u offset %u",
				 (uint64) RelUndoGetCounter(undo_ptr), RelUndoGetBlockNum(undo_ptr), RelUndoGetOffset(undo_ptr));
		goto fetch_undo_record;
	}

	if (RELUNDO_TYPE_IS_INSERT(hdr.urec_type))
	{
		visi_info->xmin = hdr.urec_xid;
		visi_info->cmin = hdr.urec_cid;

		/*
		 * Reject tuples from aborted transactions. An invalid xid can be left
		 * behind by a speculative insertion that was canceled.
		 */
		if (!TransactionIdIsValid(hdr.urec_xid))
		{
			pfree(payload);
			return false;
		}
		if (!TransactionIdIsCurrentTransactionId(hdr.urec_xid) &&
			!TransactionIdIsInProgress(hdr.urec_xid) &&
			!TransactionIdDidCommit(hdr.urec_xid))
		{
			pfree(payload);
			return false;
		}

		pfree(payload);
		return true;
	}
	else if (hdr.urec_type == RELUNDO_DELETE ||
			 hdr.urec_type == RELUNDO_UPDATE ||
			 hdr.urec_type == RELUNDO_TUPLE_LOCK)
	{
		undo_ptr = hdr.urec_prevundorec;
		goto fetch_undo_record;
	}
	else
	{
		pfree(payload);
		elog(ERROR, "unexpected UNDO record type: %d", hdr.urec_type);
	}

	return true;				/* keep compiler quiet */
}

/*
 * Used for logical decoding. Only usable on catalog tables. In Noxu, this
 * is unlikely to be called since Noxu tables are not catalog tables.
 * However, we provide a correct implementation for completeness.
 *
 * The historic MVCC snapshot uses xid arrays (xip for committed xids,
 * subxip for our own transaction's sub-xids) instead of the normal
 * snapshot mechanism.
 *
 * Similar to: HeapTupleSatisfiesHistoricMVCC
 */
static bool
nx_SatisfiesHistoricMVCC(NXTidTreeScan *scan, RelUndoRecPtr item_undoptr,
						 NXUndoSlotVisibility *visi_info)
{
	Relation	rel = scan->rel;
	Snapshot	snapshot = scan->snapshot;
	RelUndoRecPtr undo_ptr;
	RelUndoRecordHeader hdr;
	void	   *payload = NULL;
	Size		payload_size;
	TransactionId xmin = InvalidTransactionId;
	CommandId	cmin = InvalidCommandId;
	TransactionId xmax = InvalidTransactionId;
	CommandId	cmax = InvalidCommandId;

	undo_ptr = item_undoptr;

fetch_undo_record:
	/* Free payload from previous iteration if any */
	if (payload)
	{
		pfree(payload);
		payload = NULL;
	}

	/*
	 * If this record is "old", check its type to determine visibility.
	 * An old INSERT means visible; an old DELETE/UPDATE means not visible.
	 */
	if (relundo_counter_precedes(RelUndoGetCounter(undo_ptr), RelUndoGetCounter(scan->recent_oldest_undo)))
	{
		int			old_vis = nx_old_record_is_visible(rel, &undo_ptr);

		if (old_vis == 1)
		{
			visi_info->xmin = FrozenTransactionId;
			visi_info->cmin = InvalidCommandId;
			return true;
		}
		else
		{
			/* old DELETE/UPDATE: tuple is not visible */
			return false;
		}
	}

	/* have to fetch the UNDO record */
	if (!RelUndoReadRecord(rel, undo_ptr, &hdr, &payload, &payload_size))
	{
		scan->recent_oldest_undo = nx_get_oldest_visible_undo_ptr(rel);
		if (!relundo_counter_precedes(RelUndoGetCounter(undo_ptr), RelUndoGetCounter(scan->recent_oldest_undo)))
			elog(ERROR, "could not find UNDO record " UINT64_FORMAT " at blk %u offset %u",
				 (uint64) RelUndoGetCounter(undo_ptr), RelUndoGetBlockNum(undo_ptr), RelUndoGetOffset(undo_ptr));
		goto fetch_undo_record;
	}

	if (RELUNDO_TYPE_IS_INSERT(hdr.urec_type))
	{
		xmin = hdr.urec_xid;
		cmin = hdr.urec_cid;
		visi_info->xmin = xmin;
		visi_info->cmin = cmin;

		pfree(payload);
		payload = NULL;

		/* Check xmin visibility using historic snapshot rules */
		if (pg_lfind32(xmin, snapshot->subxip, snapshot->subxcnt))
		{
			/* One of our own sub-transaction's xids */
			if (cmin >= snapshot->curcid)
				return false;	/* inserted after scan started */
			/* fall through to check xmax */
		}
		else if (TransactionIdPrecedes(xmin, snapshot->xmin))
		{
			/* Before our xmin horizon - check if committed */
			if (!TransactionIdDidCommit(xmin))
				return false;
			/* fall through to check xmax */
		}
		else if (TransactionIdFollowsOrEquals(xmin, snapshot->xmax))
		{
			/* Beyond our xmax horizon - invisible */
			return false;
		}
		else if (pg_lfind32(xmin, snapshot->xip, snapshot->xcnt))
		{
			/* Committed transaction in [xmin, xmax) */
			/* fall through to check xmax */
		}
		else
		{
			/* Between [xmin, xmax) but not committed - invisible */
			return false;
		}

		/*
		 * xmin is visible. If the tuple was not deleted/updated, it's visible.
		 */
		if (xmax == InvalidTransactionId)
			return true;

		/* Check xmax visibility */
		if (pg_lfind32(xmax, snapshot->subxip, snapshot->subxcnt))
		{
			if (cmax == InvalidCommandId || cmax >= snapshot->curcid)
				return true;	/* deleted after scan started */
			else
				return false;	/* deleted before scan started */
		}
		else if (TransactionIdPrecedes(xmax, snapshot->xmin))
		{
			if (!TransactionIdDidCommit(xmax))
				return true;	/* deleter aborted */
			return false;		/* deleter committed and old */
		}
		else if (TransactionIdFollowsOrEquals(xmax, snapshot->xmax))
		{
			return true;		/* deleter not yet visible */
		}
		else if (pg_lfind32(xmax, snapshot->xip, snapshot->xcnt))
		{
			return false;		/* deleter committed */
		}
		else
		{
			return true;		/* deleter not committed */
		}
	}
	else if (hdr.urec_type == RELUNDO_DELETE ||
			 hdr.urec_type == RELUNDO_UPDATE)
	{
		/* Remember the xmax info and continue to find the INSERT */
		xmax = hdr.urec_xid;
		cmax = hdr.urec_cid;
		undo_ptr = hdr.urec_prevundorec;
		goto fetch_undo_record;
	}
	else if (hdr.urec_type == RELUNDO_TUPLE_LOCK)
	{
		/* Ignore tuple locks, continue to find INSERT */
		undo_ptr = hdr.urec_prevundorec;
		goto fetch_undo_record;
	}
	else
	{
		pfree(payload);
		elog(ERROR, "unexpected UNDO record type: %d", hdr.urec_type);
	}

	return false;				/* keep compiler quiet */
}

/*
 * If next_tid is not NULL then gets populated for the tuple if tuple was
 * UPDATEd. *next_tid_p is set to the TID of the new row version.
 *
 * Similar to: HeapTupleSatisfiesVisibility
 */
bool
nx_SatisfiesVisibility(NXTidTreeScan * scan, RelUndoRecPtr item_undoptr,
					   TransactionId *obsoleting_xid, nxtid *next_tid,
					   NXUndoSlotVisibility *visi_info)
{
	RelUndoRecPtr undo_ptr;

	/* initialize as invalid, if we find valid one populate the same */
	if (next_tid)
		*next_tid = InvalidNXTid;

	/* The caller should've filled in the recent_oldest_undo pointer */
	Assert(RelUndoRecPtrIsValid(scan->recent_oldest_undo));

	*obsoleting_xid = InvalidTransactionId;

	/*
	 * Items with invalid undo record are considered visible. Mostly META
	 * column stores the valid undo record, all other columns stores invalid
	 * undo pointer. Visibility check is performed based on META column and
	 * only if visible rest of columns are fetched. For in-place updates,
	 * columns other than META column may have valid undo record, in which
	 * case the visibility check needs to be performed for the same. META
	 * column can sometime also have items with invalid undo, see
	 * nxbt_undo_item_deletion().
	 */
	undo_ptr = item_undoptr;
	if (!RelUndoRecPtrIsValid(undo_ptr))
		return true;

	switch (scan->snapshot->snapshot_type)
	{
		case SNAPSHOT_MVCC:
			return nx_SatisfiesMVCC(scan, item_undoptr, obsoleting_xid, next_tid, visi_info);

		case SNAPSHOT_SELF:
			return nx_SatisfiesSelf(scan, item_undoptr, next_tid, visi_info);

		case SNAPSHOT_ANY:
			return nx_SatisfiesAny(scan, item_undoptr, visi_info);

		case SNAPSHOT_TOAST:
			return nx_SatisfiesOverflow(scan, item_undoptr, visi_info);

		case SNAPSHOT_DIRTY:
			return nx_SatisfiesDirty(scan, item_undoptr, next_tid, visi_info);

		case SNAPSHOT_HISTORIC_MVCC:
			return nx_SatisfiesHistoricMVCC(scan, item_undoptr, visi_info);

		case SNAPSHOT_NON_VACUUMABLE:
			return nx_SatisfiesNonVacuumable(scan, item_undoptr, visi_info);
	}

	return false;				/* keep compiler quiet */
}
