/*-------------------------------------------------------------------------
 *
 * noxu_rollback.c
 *	  Transaction rollback for Noxu columnar table access method
 *
 * This module implements async rollback support for Noxu tables using the
 * per-relation UNDO infrastructure. It provides handlers for rolling back
 * INSERT, DELETE, UPDATE, TUPLE_LOCK, and DELTA_INSERT operations.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/noxu/noxu_rollback.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/noxu_internal.h"
#include "access/relundo.h"
#include "access/xactundo.h"
#include "storage/bufmgr.h"
#include "utils/rel.h"

/* Forward declarations */
static void noxu_rollback_insert(Relation rel, RelUndoRecPtr undo_ptr,
								  RelUndoRecordHeader *header, void *payload);
static void noxu_rollback_delete(Relation rel, RelUndoRecPtr undo_ptr,
								  RelUndoRecordHeader *header, void *payload);
static void noxu_rollback_update(Relation rel, RelUndoRecPtr undo_ptr,
								  RelUndoRecordHeader *header, void *payload);
static void noxu_rollback_tuple_lock(Relation rel, RelUndoRecPtr undo_ptr,
									  RelUndoRecordHeader *header, void *payload);
static void noxu_rollback_delta_insert(Relation rel, RelUndoRecPtr undo_ptr,
										RelUndoRecordHeader *header, void *payload);

/*
 * NoxuRelUndoApplyChain - Walk and apply Noxu-specific UNDO chain
 *
 * This is the Noxu-specific implementation of rollback that understands
 * Noxu's columnar B-tree structure. Called by the async rollback worker
 * when processing aborted transactions on Noxu tables.
 */
void
NoxuRelUndoApplyChain(Relation rel, RelUndoRecPtr start_ptr)
{
	RelUndoRecPtr current_ptr = start_ptr;
	int			applied_count = 0;

	if (!RelUndoRecPtrIsValid(current_ptr))
	{
		elog(DEBUG1, "NoxuRelUndoApplyChain: no valid UNDO pointer for relation %s",
			 RelationGetRelationName(rel));
		return;
	}

	elog(LOG, "NoxuRelUndoApplyChain: starting rollback for relation %s at UNDO ptr %lu",
		 RelationGetRelationName(rel), (unsigned long) current_ptr);

	/*
	 * Walk backwards through the UNDO chain, applying each record.
	 * The chain is linked via header.urec_prevundorec.
	 */
	while (RelUndoRecPtrIsValid(current_ptr))
	{
		RelUndoRecordHeader header;
		void	   *payload = NULL;
		Size		payload_size;

		/* Read the UNDO record */
		if (!RelUndoReadRecord(rel, current_ptr, &header, &payload, &payload_size))
		{
			elog(WARNING, "NoxuRelUndoApplyChain: could not read UNDO record at %lu",
				 (unsigned long) current_ptr);
			break;
		}

		elog(DEBUG1, "NoxuRelUndoApplyChain: processing record type %d at %lu",
			 header.urec_type, (unsigned long) current_ptr);

		/* Dispatch to the appropriate handler based on record type */
		switch (header.urec_type)
		{
			case RELUNDO_INSERT:
				noxu_rollback_insert(rel, current_ptr, &header, payload);
				break;

			case RELUNDO_DELETE:
				noxu_rollback_delete(rel, current_ptr, &header, payload);
				break;

			case RELUNDO_UPDATE:
				noxu_rollback_update(rel, current_ptr, &header, payload);
				break;

			case RELUNDO_TUPLE_LOCK:
				noxu_rollback_tuple_lock(rel, current_ptr, &header, payload);
				break;

			case RELUNDO_DELTA_INSERT:
				noxu_rollback_delta_insert(rel, current_ptr, &header, payload);
				break;

			default:
				elog(ERROR, "NoxuRelUndoApplyChain: unknown UNDO record type %d",
					 header.urec_type);
		}

		applied_count++;

		/* Move to the previous record in the chain */
		current_ptr = header.urec_prevundorec;

		/* Clean up payload */
		if (payload)
			pfree(payload);
	}

	elog(LOG, "NoxuRelUndoApplyChain: rollback complete for relation %s (%d operations)",
		 RelationGetRelationName(rel), applied_count);
}

/*
 * noxu_rollback_insert - Undo an INSERT operation
 *
 * To roll back an INSERT, we mark the TID as dead in the TID tree.
 * This makes the tuple invisible to all transactions going forward.
 */
static void
noxu_rollback_insert(Relation rel, RelUndoRecPtr undo_ptr,
					  RelUndoRecordHeader *header, void *payload)
{
	RelUndoInsertPayload *ins_payload = (RelUndoInsertPayload *) payload;
	nxtid		firsttid;
	nxtid		endtid;
	nxtid		tid;
	RelUndoRecPtr recent_oldest_undo;

	(void) undo_ptr;			/* unused */
	(void) header;				/* unused */

	/* Convert ItemPointerData to nxtid range */
	firsttid = NXTidFromItemPointer(ins_payload->firsttid);
	endtid = NXTidFromItemPointer(ins_payload->endtid);

	elog(LOG, "noxu_rollback_insert: marking TIDs %lu..%lu (%lu TIDs) as dead",
		 (unsigned long) firsttid, (unsigned long) endtid,
		 (unsigned long) (endtid - firsttid));

	/* Get the recent oldest UNDO pointer for cleanup */
	recent_oldest_undo = nx_get_oldest_visible_undo_ptr(rel);

	/*
	 * Mark all TIDs in the range as dead.  The INSERT UNDO record covers
	 * a range of consecutive TIDs [firsttid, endtid).  We must mark each
	 * one as dead to fully undo the insert.
	 */
	for (tid = firsttid; tid < endtid; tid++)
	{
		nxbt_tid_mark_dead(rel, tid, recent_oldest_undo);
	}

	elog(DEBUG2, "noxu_rollback_insert: successfully rolled back INSERT of %lu TIDs",
		 (unsigned long) (endtid - firsttid));
}

/*
 * noxu_rollback_delete - Undo a DELETE operation
 *
 * In Noxu's columnar architecture, a DELETE does not remove data from the
 * attribute B-trees.  It only updates the TID's undo pointer in the TID
 * B-tree to point to a DELETE UNDO record.  The actual column data remains
 * intact in the per-attribute B-trees.
 *
 * Therefore, to roll back a DELETE we only need to restore the TID's previous
 * undo pointer (the one it had before the DELETE was applied).  This is
 * extracted from the UNDO record's urec_prevundorec field.  The existing
 * nxbt_tid_undo_deletion() function does exactly this: it finds the TID in
 * the TID B-tree, reads the predecessor pointer from the UNDO record, and
 * restores it using nxbt_tid_item_change_undoptr().
 */
static void
noxu_rollback_delete(Relation rel, RelUndoRecPtr undo_ptr,
					  RelUndoRecordHeader *header, void *payload)
{
	RelUndoDeletePayload *del_payload = (RelUndoDeletePayload *) payload;
	RelUndoRecPtr recent_oldest_undo;
	int			i;

	(void) header;				/* unused */

	recent_oldest_undo = nx_get_oldest_visible_undo_ptr(rel);

	for (i = 0; i < del_payload->ntids; i++)
	{
		nxtid	tid = NXTidFromItemPointer(del_payload->tids[i]);

		elog(DEBUG1, "noxu_rollback_delete: restoring TID %lu visibility (undo ptr %lu)",
			 (unsigned long) tid, (unsigned long) undo_ptr);

		/*
		 * nxbt_tid_undo_deletion restores the TID's undo pointer to whatever
		 * it was before the DELETE.  It reads the UNDO record at undo_ptr,
		 * extracts urec_prevundorec, and uses nxbt_tid_item_change_undoptr()
		 * to set the TID's pointer back to that predecessor value.
		 *
		 * If the TID's current undo pointer no longer matches undo_ptr (e.g.
		 * because a concurrent operation already handled it), the function
		 * safely skips the restoration.
		 */
		nxbt_tid_undo_deletion(rel, tid, undo_ptr, recent_oldest_undo);

		elog(DEBUG2, "noxu_rollback_delete: successfully rolled back DELETE of TID %lu",
			 (unsigned long) tid);
	}
}

/*
 * noxu_rollback_update - Undo an UPDATE operation
 *
 * An UPDATE in Noxu performs two operations on the TID B-tree:
 *   1. Inserts a new TID (the new version) with an INSERT UNDO record
 *   2. Changes the old TID's undo pointer to an UPDATE UNDO record
 *      (via nxbt_tid_mark_old_updated), linking old -> new versions
 *
 * The UPDATE UNDO record's urec_prevundorec contains the old TID's previous
 * undo pointer (what it was before the UPDATE was applied).
 *
 * To roll back:
 *   1. Mark the new TID as dead, removing the updated version
 *   2. Restore the old TID's undo pointer to urec_prevundorec, making the
 *      original tuple visible again with its pre-UPDATE visibility state
 *
 * No tuple data restoration is needed because Noxu is columnar: attribute
 * data for the old TID is still intact in the per-attribute B-trees.
 */
static void
noxu_rollback_update(Relation rel, RelUndoRecPtr undo_ptr,
					  RelUndoRecordHeader *header, void *payload)
{
	RelUndoUpdatePayload *upd_payload = (RelUndoUpdatePayload *) payload;
	nxtid		old_tid;
	nxtid		new_tid;
	RelUndoRecPtr recent_oldest_undo;

	(void) header;				/* unused */

	/* Convert ItemPointerData to nxtid */
	old_tid = NXTidFromItemPointer(upd_payload->oldtid);
	new_tid = NXTidFromItemPointer(upd_payload->newtid);

	elog(DEBUG1, "noxu_rollback_update: rolling back UPDATE from old TID %lu to new TID %lu",
		 (unsigned long) old_tid, (unsigned long) new_tid);

	/* Get the recent oldest UNDO pointer for cleanup */
	recent_oldest_undo = nx_get_oldest_visible_undo_ptr(rel);

	/*
	 * Step 1: Mark the new TID as dead (similar to rolling back an INSERT).
	 * This removes the updated version from visibility.
	 */
	nxbt_tid_mark_dead(rel, new_tid, recent_oldest_undo);

	elog(DEBUG2, "noxu_rollback_update: marked new TID %lu as dead",
		 (unsigned long) new_tid);

	/*
	 * Step 2: Restore the old TID's undo pointer to what it was before the
	 * UPDATE.  The UPDATE UNDO record at undo_ptr was written onto the old
	 * TID's undo chain by nxbt_tid_mark_old_updated().  Its
	 * urec_prevundorec field holds the old TID's previous undo pointer.
	 *
	 * nxbt_tid_undo_deletion() reads the UNDO record, extracts the
	 * predecessor pointer, and uses nxbt_tid_item_change_undoptr() to
	 * restore it.  Despite its name referencing "deletion", it handles
	 * any case where a TID's undo pointer needs to be rolled back to its
	 * predecessor -- exactly the operation we need here.
	 */
	nxbt_tid_undo_deletion(rel, old_tid, undo_ptr, recent_oldest_undo);

	elog(DEBUG2, "noxu_rollback_update: restored old TID %lu visibility",
		 (unsigned long) old_tid);
}

/*
 * noxu_rollback_tuple_lock - Undo a TUPLE_LOCK operation
 *
 * To roll back a tuple lock, we need to remove the lock from the TID's
 * UNDO chain. However, Noxu's locking is integrated with the UNDO system,
 * so rolling back the UNDO record itself effectively removes the lock.
 *
 * No additional action needed beyond removing from the chain.
 */
static void
noxu_rollback_tuple_lock(Relation rel, RelUndoRecPtr undo_ptr,
						  RelUndoRecordHeader *header, void *payload)
{
	RelUndoTupleLockPayload *lock_payload = (RelUndoTupleLockPayload *) payload;
	nxtid		tid;

	(void) rel;					/* unused */
	(void) undo_ptr;			/* unused */
	(void) header;				/* unused */

	/* Convert ItemPointerData to nxtid */
	tid = NXTidFromItemPointer(lock_payload->tid);

	elog(DEBUG1, "noxu_rollback_tuple_lock: rolling back lock on TID %lu (mode %d)",
		 (unsigned long) tid, lock_payload->lock_mode);

	/*
	 * For tuple locks, the lock is represented in the UNDO chain itself.
	 * Removing this record from the effective chain (by processing the
	 * rollback) automatically releases the lock. No additional cleanup
	 * is needed.
	 */

	elog(DEBUG2, "noxu_rollback_tuple_lock: successfully rolled back lock on TID %lu",
		 (unsigned long) tid);
}

/*
 * noxu_rollback_delta_insert - Undo a DELTA_INSERT operation
 *
 * DELTA_INSERT is an Noxu-specific operation for partial-column UPDATEs.
 * To roll it back, we mark the TID as dead, similar to INSERT rollback.
 * Note: The generic RelUndoDeltaInsertPayload only has a single TID.
 */
static void
noxu_rollback_delta_insert(Relation rel, RelUndoRecPtr undo_ptr,
							RelUndoRecordHeader *header, void *payload)
{
	RelUndoDeltaInsertPayload *delta_payload = (RelUndoDeltaInsertPayload *) payload;
	nxtid		tid;
	RelUndoRecPtr recent_oldest_undo;

	(void) undo_ptr;			/* unused */
	(void) header;				/* unused */

	/* Convert ItemPointerData to nxtid */
	tid = NXTidFromItemPointer(delta_payload->tid);

	elog(DEBUG1, "noxu_rollback_delta_insert: rolling back DELTA_INSERT for TID %lu",
		 (unsigned long) tid);

	/* Get the recent oldest UNDO pointer for cleanup */
	recent_oldest_undo = nx_get_oldest_visible_undo_ptr(rel);

	/*
	 * Mark the TID as dead. DELTA_INSERT operations in Noxu represent
	 * partial column updates, and rolling them back is similar to INSERT.
	 */
	nxbt_tid_mark_dead(rel, tid, recent_oldest_undo);

	elog(DEBUG2, "noxu_rollback_delta_insert: successfully rolled back DELTA_INSERT for TID %lu",
		 (unsigned long) tid);
}
