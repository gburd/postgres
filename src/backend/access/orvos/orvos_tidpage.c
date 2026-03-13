/*
 * orvos_tidpage.c
 *		Routines for handling the TID tree.
 *
 * A Orvos table consists of multiple B-trees, one for each attribute. The
 * functions in this file deal with one B-tree at a time, it is the caller's
 * responsibility to tie together the scans of each btree.
 *
 * Operations:
 *
 * - Sequential scan in TID order
 *  - must be efficient with scanning multiple trees in sync
 *
 * - random lookups, by TID (for index scan)
 *
 * - range scans by TID (for bitmap index scan)
 *
 * NOTES:
 * - Locking order: child before parent, left before right
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/orvos/orvos_tidpage.c
 */
#include "postgres.h"

#include "access/orvos_internal.h"
#include "access/orvos_undorec.h"
#include "lib/integerset.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/predicate.h"
#include "storage/procarray.h"
#include "utils/injection_point.h"
#include "utils/memutils.h"
#include "utils/rel.h"


/* prototypes for local functions */
static void ovbt_tid_recompress_replace(Relation rel, Buffer oldbuf, List *items, ov_pending_undo_op * undo_op);
static OffsetNumber ovbt_tid_fetch(Relation rel, ovtid tid,
								   Buffer *buf_p, OVUndoRecPtr *undo_ptr_p, bool *isdead_p);
static void ovbt_tid_add_items(Relation rel, Buffer buf, List *newitems,
							   ov_pending_undo_op * pending_undo_op);
static void ovbt_tid_replace_item(Relation rel, Buffer buf, OffsetNumber off, List *newitems,
								  ov_pending_undo_op * pending_undo_op);

static TM_Result ovbt_tid_update_lock_old(Relation rel, ovtid otid,
										  TransactionId xid, CommandId cid, bool key_update, Snapshot snapshot,
										  Snapshot crosscheck, bool wait, TM_FailureData *hufd,
										  bool *this_xact_has_lock, OVUndoRecPtr *prevundoptr_p);
static void ovbt_tid_update_insert_new(Relation rel, ovtid *newtid,
									   TransactionId xid, CommandId cid, OVUndoRecPtr prevundoptr);
static bool ovbt_tid_mark_old_updated(Relation rel, ovtid otid, ovtid newtid,
									  TransactionId xid, CommandId cid, bool key_update, OVUndoRecPtr prevrecptr);
static OffsetNumber ovbt_binsrch_tidpage(ovtid key, Page page);

/* ----------------------------------------------------------------
 *						 Public interface
 * ----------------------------------------------------------------
 */

/*
 * Begin a scan of the btree.
 */
void
ovbt_tid_begin_scan(Relation rel, ovtid starttid,
					ovtid endtid, Snapshot snapshot, OVTidTreeScan * scan)
{
	scan->rel = rel;
	scan->snapshot = snapshot;
	scan->context = CurrentMemoryContext;
	scan->starttid = starttid;
	scan->endtid = endtid;
	scan->currtid = starttid - 1;
	memset(&scan->recent_oldest_undo, 0, sizeof(scan->recent_oldest_undo));
	memset(&scan->array_iter, 0, sizeof(scan->array_iter));
	scan->array_iter.context = CurrentMemoryContext;
	scan->array_curr_idx = -1;

	scan->active = true;
	scan->lastbuf = InvalidBuffer;
	scan->lastoff = InvalidOffsetNumber;

	scan->recent_oldest_undo = ovundo_get_oldest_undo_ptr(rel);
}

/*
 * Reset the 'next' TID in a scan to the given TID.
 */
void
ovbt_tid_reset_scan(OVTidTreeScan * scan, ovtid starttid, ovtid endtid, ovtid currtid)
{
	scan->starttid = starttid;
	scan->endtid = endtid;
	scan->currtid = currtid;
	scan->array_curr_idx = -1;
}

void
ovbt_tid_end_scan(OVTidTreeScan * scan)
{
	if (!scan->active)
		return;

	if (scan->lastbuf != InvalidBuffer)
		ReleaseBuffer(scan->lastbuf);

	scan->active = false;
	scan->array_iter.num_tids = 0;
	scan->array_curr_idx = -1;

	if (scan->array_iter.tids)
		pfree(scan->array_iter.tids);
	if (scan->array_iter.tid_undoslotnos)
		pfree(scan->array_iter.tid_undoslotnos);
}

/*
 * Helper function of ovbt_tid_scan_next_array(), to extract Datums from the given
 * array item into the scan->array_* fields.
 */
static void
ovbt_tid_scan_extract_array(OVTidTreeScan * scan, OVTidArrayItem *aitem)
{
	bool		slots_visible[4];
	int			first;
	int			last;
	int			num_visible_tids;
	int			continue_at;

	ovbt_tid_item_unpack(aitem, &scan->array_iter);

	slots_visible[OVBT_OLD_UNDO_SLOT] = true;
	slots_visible[OVBT_DEAD_UNDO_SLOT] = false;

	scan->array_iter.undoslot_visibility[OVBT_OLD_UNDO_SLOT] = InvalidUndoSlotVisibility;
	scan->array_iter.undoslot_visibility[OVBT_OLD_UNDO_SLOT].xmin = FrozenTransactionId;

	scan->array_iter.undoslot_visibility[OVBT_DEAD_UNDO_SLOT] = InvalidUndoSlotVisibility;

	for (int i = 2; i < aitem->t_num_undo_slots; i++)
	{
		OVUndoRecPtr undoptr = scan->array_iter.undoslots[i];
		TransactionId obsoleting_xid;

		scan->array_iter.undoslot_visibility[i] = InvalidUndoSlotVisibility;

		slots_visible[i] = ov_SatisfiesVisibility(scan, undoptr, &obsoleting_xid,
												  NULL, &scan->array_iter.undoslot_visibility[i]);
		if (scan->serializable && TransactionIdIsValid(obsoleting_xid))
			CheckForSerializableConflictOut(scan->rel, obsoleting_xid, scan->snapshot);
	}

	/*
	 * Skip over elements at the beginning and end of the array that are not
	 * within the range we're interested in.
	 */
	for (first = 0; first < scan->array_iter.num_tids; first++)
	{
		if (scan->array_iter.tids[first] >= scan->starttid)
			break;
	}
	for (last = scan->array_iter.num_tids - 1; last >= first; last--)
	{
		if (scan->array_iter.tids[last] < scan->endtid)
			break;
	}

	/* squeeze out invisible TIDs */
	if (first == 0)
	{
		int			j;

		for (j = 0; j <= last; j++)
		{
			if (!slots_visible[scan->array_iter.tid_undoslotnos[j]])
				break;
		}
		num_visible_tids = j;
		continue_at = j + 1;
	}
	else
	{
		num_visible_tids = 0;
		continue_at = first;
	}

	for (int i = continue_at; i <= last; i++)
	{
		/* Is this item visible? */
		if (slots_visible[scan->array_iter.tid_undoslotnos[i]])
		{
			scan->array_iter.tids[num_visible_tids] = scan->array_iter.tids[i];
			scan->array_iter.tid_undoslotnos[num_visible_tids] = scan->array_iter.tid_undoslotnos[i];
			num_visible_tids++;
		}
	}
	scan->array_iter.num_tids = num_visible_tids;
	scan->array_curr_idx = -1;
}

/*
 * Advance scan to next batch of TIDs.
 *
 * Finds the next TID array item >= scan->nexttid, and decodes it into
 * scan->array_iter. The values in scan->array_iter are valid until
 * the next call to this function, ovbt_tid_reset_scan() or
 * ovbt_tid_end_scan().
 *
 * Returns true if there was another item, or false if we reached the
 * end of the scan.
 *
 * This is normally not used directly, see ovbt_tid_scan_next() wrapper.
 */
bool
ovbt_tid_scan_next_array(OVTidTreeScan * scan, ovtid nexttid, ScanDirection direction)
{
	if (!scan->active)
		return InvalidOVTid;

	/*
	 * Process items, until we find something that is visible to the snapshot.
	 *
	 * This advances nexttid as it goes.
	 */
	while (nexttid < scan->endtid && nexttid >= scan->starttid)
	{
		Buffer		buf;
		Page		page;
		OVBtreePageOpaque *opaque;
		OffsetNumber maxoff;
		OffsetNumber off;
		BlockNumber next;

		/*
		 * Find and lock the leaf page containing nexttid.
		 */
		buf = ovbt_find_and_lock_leaf_containing_tid(scan->rel, OV_META_ATTRIBUTE_NUM,
													 scan->lastbuf, nexttid,
													 BUFFER_LOCK_SHARE);
		if (buf != scan->lastbuf)
			scan->lastoff = InvalidOffsetNumber;
		scan->lastbuf = buf;
		if (!BufferIsValid(buf))
		{
			/*
			 * Completely empty tree. This should only happen at the beginning
			 * of a scan - a tree cannot go missing after it's been created -
			 * but we don't currently check for that.
			 */
			break;
		}
		page = BufferGetPage(buf);
		opaque = OVBtreePageGetOpaque(page);
		Assert(opaque->ov_page_id == OV_BTREE_PAGE_ID);

		/*
		 * Scan the items on the page, to find the next one that covers
		 * nexttid.
		 *
		 * We check the last offset first, as an optimization
		 */
		maxoff = PageGetMaxOffsetNumber(page);
		if (direction == ForwardScanDirection)
		{
			/* Search for the next item >= nexttid */
			off = FirstOffsetNumber;
			if (scan->lastoff > FirstOffsetNumber && scan->lastoff <= maxoff)
			{
				ItemId		iid = PageGetItemId(page, scan->lastoff);
				OVTidArrayItem *item = (OVTidArrayItem *) PageGetItem(page, iid);

				if (nexttid >= item->t_endtid)
					off = scan->lastoff + 1;
			}

			for (; off <= maxoff; off++)
			{
				ItemId		iid = PageGetItemId(page, off);
				OVTidArrayItem *item = (OVTidArrayItem *) PageGetItem(page, iid);

				if (nexttid >= item->t_endtid)
					continue;

				if (item->t_firsttid >= scan->endtid)
				{
					nexttid = scan->endtid;
					break;
				}

				ovbt_tid_scan_extract_array(scan, item);

				if (scan->array_iter.num_tids > 0)
				{
					if (scan->array_iter.tids[scan->array_iter.num_tids - 1] >= nexttid)
					{
						LockBuffer(scan->lastbuf, BUFFER_LOCK_UNLOCK);
						scan->lastoff = off;
						return true;
					}
					nexttid = scan->array_iter.tids[scan->array_iter.num_tids - 1] + 1;
				}
			}
			/* No more items on this page. Walk right, if possible */
			if (nexttid < opaque->ov_hikey)
				nexttid = opaque->ov_hikey;
			next = opaque->ov_next;
			LockBuffer(buf, BUFFER_LOCK_UNLOCK);

			if (next == InvalidBlockNumber || nexttid >= scan->endtid)
			{
				/* reached end of scan */
				break;
			}

			scan->lastbuf = ReleaseAndReadBuffer(scan->lastbuf, scan->rel, next);
		}
		else
		{
			/* Search for the next item <= nexttid */
			for (off = maxoff; off >= FirstOffsetNumber; off--)
			{
				ItemId		iid = PageGetItemId(page, off);
				OVTidArrayItem *item = (OVTidArrayItem *) PageGetItem(page, iid);

				if (nexttid < item->t_firsttid)
					continue;

				if (item->t_endtid < scan->starttid)
				{
					nexttid = scan->starttid - 1;
					break;
				}

				ovbt_tid_scan_extract_array(scan, item);

				if (scan->array_iter.num_tids > 0)
				{
					if (scan->array_iter.tids[0] <= nexttid)
					{
						LockBuffer(scan->lastbuf, BUFFER_LOCK_UNLOCK);
						scan->lastoff = off;
						return true;
					}
					nexttid = scan->array_iter.tids[0] - 1;
				}
			}
			/* No more items on this page. Loop back to find the left sibling. */
			if (nexttid >= opaque->ov_lokey)
				nexttid = opaque->ov_lokey - 1;
			LockBuffer(buf, BUFFER_LOCK_UNLOCK);
			if (nexttid < scan->starttid)
			{
				/* reached end of scan */
				break;
			}
			scan->lastbuf = InvalidBuffer;
		}
	}

	/* Reached end of scan. */
	scan->array_iter.num_tids = 0;
	if (BufferIsValid(scan->lastbuf))
		ReleaseBuffer(scan->lastbuf);
	scan->lastbuf = InvalidBuffer;
	scan->lastoff = InvalidOffsetNumber;

	return false;
}

/*
 * Get the last tid (plus one) in the tree.
 */
ovtid
ovbt_get_last_tid(Relation rel)
{
	ovtid		rightmostkey;
	ovtid		tid;
	Buffer		buf;
	Page		page;
	OVBtreePageOpaque *opaque;
	OffsetNumber maxoff;

	/* Find the rightmost leaf */
	rightmostkey = MaxOVTid;
	buf = ovbt_descend(rel, OV_META_ATTRIBUTE_NUM, rightmostkey, 0, true, InvalidBuffer, InvalidBuffer);
	if (!BufferIsValid(buf))
	{
		return MinOVTid;
	}
	page = BufferGetPage(buf);
	opaque = OVBtreePageGetOpaque(page);

	/*
	 * Look at the last item, for its tid.
	 */
	maxoff = PageGetMaxOffsetNumber(page);
	if (maxoff >= FirstOffsetNumber)
	{
		ItemId		iid = PageGetItemId(page, maxoff);
		OVTidArrayItem *lastitem = (OVTidArrayItem *) PageGetItem(page, iid);

		tid = lastitem->t_endtid;
	}
	else
	{
		tid = opaque->ov_lokey;
	}
	UnlockReleaseBuffer(buf);

	return tid;
}

/*
 * Insert a multiple TIDs.
 *
 * Populates the TIDs of the new tuples.
 *
 * If 'tid' in list is valid, then that TID is used. It better not be in use already. If
 * it's invalid, then a new TID is allocated, as we see best. (When inserting the
 * first column of the row, pass invalid, and for other columns, pass the TID
 * you got for the first column.)
 */
void
ovbt_tid_multi_insert(Relation rel, ovtid *tids, int ntuples,
					  TransactionId xid, CommandId cid, uint32 speculative_token, OVUndoRecPtr prevundoptr)
{
	Buffer		buf;
	Page		page;
	OVBtreePageOpaque *opaque;
	OffsetNumber maxoff;
	ovtid		insert_target_key;
	List	   *newitems;
	ov_pending_undo_op *undo_op;
	ovtid		endtid;
	ovtid		tid;
	OVTidArrayItem *lastitem;
	bool		modified_orig;

	/*
	 * Insert to the rightmost leaf.
	 *
	 * TODO: use a Free Space Map to find suitable target.
	 */
	insert_target_key = MaxOVTid;
	buf = ovbt_descend(rel, OV_META_ATTRIBUTE_NUM, insert_target_key, 0, false, InvalidBuffer, InvalidBuffer);
	page = BufferGetPage(buf);
	opaque = OVBtreePageGetOpaque(page);
	maxoff = PageGetMaxOffsetNumber(page);

	/*
	 * Look at the last item, for its tid.
	 *
	 * assign TIDS for each item.
	 */
	if (maxoff >= FirstOffsetNumber)
	{
		ItemId		iid = PageGetItemId(page, maxoff);

		lastitem = (OVTidArrayItem *) PageGetItem(page, iid);

		endtid = lastitem->t_endtid;
	}
	else
	{
		endtid = opaque->ov_lokey;
		lastitem = NULL;
	}
	tid = endtid;

	/* Form an undo record */
	if (xid != FrozenTransactionId)
	{
		undo_op = ovundo_create_for_insert(rel, xid, cid, tid, ntuples,
										   speculative_token, prevundoptr);
	}
	else
	{
		undo_op = NULL;
	}

	/*
	 * Create an item to represent all the TIDs, merging with the last
	 * existing item if possible.
	 */
	newitems = ovbt_tid_item_add_tids(lastitem, tid, ntuples, undo_op ? undo_op->reservation.undorecptr : InvalidUndoPtr,
									  &modified_orig);

	/*
	 * Replace the original last item with the new items, or add new items.
	 * This splits the page if necessary.
	 */
	if (modified_orig)
		ovbt_tid_replace_item(rel, buf, maxoff, newitems, undo_op);
	else
		ovbt_tid_add_items(rel, buf, newitems, undo_op);
	/* ovbt_tid_replace/add_item unlocked 'buf' */
	ReleaseBuffer(buf);

	list_free_deep(newitems);

	/* Return the TIDs to the caller */
	for (int i = 0; i < ntuples; i++)
		tids[i] = tid + i;
}

TM_Result
ovbt_tid_delete(Relation rel, ovtid tid,
				TransactionId xid, CommandId cid,
				Snapshot snapshot, Snapshot crosscheck, bool wait,
				TM_FailureData *hufd, bool changingPart, bool *this_xact_has_lock)
{
	OVUndoRecPtr recent_oldest_undo = ovundo_get_oldest_undo_ptr(rel);
	OVUndoRecPtr item_undoptr;
	bool		item_isdead;
	TM_Result	result;
	bool		keep_old_undo_ptr = true;
	ov_pending_undo_op *undo_op;
	OffsetNumber off;
	OVTidArrayItem *origitem;
	Buffer		buf;
	Page		page;
	ovtid		next_tid;
	List	   *newitems = NIL;

	(void) wait;

	/* Find the item to delete. (It could be compressed) */
	off = ovbt_tid_fetch(rel, tid, &buf, &item_undoptr, &item_isdead);
	if (!OffsetNumberIsValid(off))
	{
		/*
		 * or should this be TM_Invisible? The heapam at least just throws an
		 * error, I think..
		 */
		elog(ERROR, "could not find tuple to delete with TID (%u, %u) in TID tree",
			 OVTidGetBlockNumber(tid), OVTidGetOffsetNumber(tid));
	}
	if (item_isdead)
	{
		elog(ERROR, "cannot delete tuple that is already marked DEAD (%u, %u)",
			 OVTidGetBlockNumber(tid), OVTidGetOffsetNumber(tid));
	}

	if (snapshot)
	{
		result = ov_SatisfiesUpdate(rel, snapshot, recent_oldest_undo,
									tid, item_undoptr, LockTupleExclusive,
									&keep_old_undo_ptr, this_xact_has_lock,
									hufd, &next_tid, NULL);
		if (result != TM_Ok)
		{
			UnlockReleaseBuffer(buf);
			/* FIXME: We should fill TM_FailureData *hufd correctly */
			return result;
		}

		if (crosscheck != InvalidSnapshot && result == TM_Ok)
		{
			/*
			 * Perform additional check for transaction-snapshot mode RI
			 * updates
			 */
			/* FIXME: dummmy scan */
			OVTidTreeScan scan;
			TransactionId obsoleting_xid;
			OVUndoSlotVisibility visi_info;

			memset(&scan, 0, sizeof(scan));
			scan.rel = rel;
			scan.snapshot = crosscheck;
			scan.recent_oldest_undo = recent_oldest_undo;

			if (!ov_SatisfiesVisibility(&scan, item_undoptr, &obsoleting_xid, NULL, &visi_info))
			{
				UnlockReleaseBuffer(buf);
				/* FIXME: We should fill TM_FailureData *hufd correctly */
				return TM_Updated;
			}
		}
	}

	/* Create UNDO record. */
	undo_op = ovundo_create_for_delete(rel, xid, cid, tid, changingPart,
									   keep_old_undo_ptr ? item_undoptr : InvalidUndoPtr);

	/* Update the tid with the new UNDO pointer. */
	page = BufferGetPage(buf);
	origitem = (OVTidArrayItem *) PageGetItem(page, PageGetItemId(page, off));
	newitems = ovbt_tid_item_change_undoptr(origitem, tid, undo_op->reservation.undorecptr,
											recent_oldest_undo);
	ovbt_tid_replace_item(rel, buf, off, newitems, undo_op);
	list_free_deep(newitems);
	ReleaseBuffer(buf);			/* ovbt_tid_replace_item unlocked 'buf' */

	return TM_Ok;
}

void
ovbt_find_latest_tid(Relation rel, ovtid *tid, Snapshot snapshot)
{
	OVUndoRecPtr recent_oldest_undo = ovundo_get_oldest_undo_ptr(rel);
	OVUndoRecPtr item_undoptr;
	bool		item_isdead;
	int			idx;
	Buffer		buf;

	/* Just using meta attribute, we can follow the update chain */
	ovtid		curr_tid = *tid;

	for (;;)
	{
		ovtid		next_tid = InvalidOVTid;

		if (curr_tid == InvalidOVTid)
			break;

		/* Find the item */
		idx = ovbt_tid_fetch(rel, curr_tid, &buf, &item_undoptr, &item_isdead);
		if (idx == -1 || item_isdead)
			break;

		if (snapshot)
		{
			/* FIXME: dummmy scan */
			OVTidTreeScan scan;
			TransactionId obsoleting_xid;
			OVUndoSlotVisibility visi_info;

			memset(&scan, 0, sizeof(scan));
			scan.rel = rel;
			scan.snapshot = snapshot;
			scan.recent_oldest_undo = recent_oldest_undo;

			if (ov_SatisfiesVisibility(&scan, item_undoptr,
									   &obsoleting_xid, &next_tid, &visi_info))
			{
				*tid = curr_tid;
			}

			curr_tid = next_tid;
			UnlockReleaseBuffer(buf);
		}
	}
}

/*
 * A new TID is allocated, as we see best and returned to the caller. This
 * function is only called for META attribute btree. Data columns will use the
 * returned tid to insert new items.
 */
TM_Result
ovbt_tid_update(Relation rel, ovtid otid,
				TransactionId xid, CommandId cid, bool key_update, Snapshot snapshot,
				Snapshot crosscheck, bool wait, TM_FailureData *hufd,
				ovtid *newtid_p, bool *this_xact_has_lock)
{
	TM_Result	result;
	OVUndoRecPtr prevundoptr;
	bool		success;

	/*
	 * This is currently only used on the meta-attribute. The other attributes
	 * don't need to carry visibility information, so the caller just inserts
	 * the new values with (multi_)insert() instead. This will change once we
	 * start doing the equivalent of HOT updates, where the TID doesn't
	 * change.
	 */
	Assert(*newtid_p == InvalidOVTid);

	/*
	 * Find and lock the old item.
	 *
	 * TODO: If there's free TID space left on the same page, we should keep
	 * the buffer locked, and use the same page for the new tuple.
	 */
retry:
	result = ovbt_tid_update_lock_old(rel, otid,
									  xid, cid, key_update, snapshot,
									  crosscheck, wait, hufd, this_xact_has_lock, &prevundoptr);

	if (result != TM_Ok)
		return result;

	/* insert new version */
	ovbt_tid_update_insert_new(rel, newtid_p, xid, cid, prevundoptr);

	/* update the old item with the "t_ctid pointer" for the new item */
	success = ovbt_tid_mark_old_updated(rel, otid, *newtid_p, xid, cid, key_update, prevundoptr);
	if (!success)
	{
		OVUndoRecPtr oldest_undoptr = ovundo_get_oldest_undo_ptr(rel);

		ovbt_tid_mark_dead(rel, *newtid_p, oldest_undoptr);
		goto retry;
	}

	return TM_Ok;
}

/*
 * Like ovbt_tid_update, but creates a DELTA_INSERT UNDO record for
 * the new TID. Used for column-delta UPDATEs where only a subset
 * of columns are actually changed.
 */
TM_Result
ovbt_tid_delta_update(Relation rel, ovtid otid,
					  TransactionId xid, CommandId cid,
					  bool key_update, Snapshot snapshot,
					  Snapshot crosscheck, bool wait,
					  TM_FailureData *hufd,
					  ovtid *newtid_p,
					  bool *this_xact_has_lock,
					  int natts, const bool *changed_cols)
{
	TM_Result	result;
	OVUndoRecPtr prevundoptr;
	bool		success;

	Assert(*newtid_p == InvalidOVTid);

retry:
	result = ovbt_tid_update_lock_old(rel, otid,
									  xid, cid, key_update,
									  snapshot, crosscheck, wait,
									  hufd, this_xact_has_lock,
									  &prevundoptr);

	if (result != TM_Ok)
		return result;

	/* Insert new version with delta UNDO record */
	ovbt_tid_delta_insert(rel, newtid_p, xid, cid,
						  otid, natts, changed_cols,
						  prevundoptr);

	success = ovbt_tid_mark_old_updated(rel, otid, *newtid_p,
										xid, cid, key_update,
										prevundoptr);
	if (!success)
	{
		OVUndoRecPtr oldest = ovundo_get_oldest_undo_ptr(rel);

		ovbt_tid_mark_dead(rel, *newtid_p, oldest);
		goto retry;
	}

	return TM_Ok;
}

/*
 * Subroutine of ovbt_update(): locks the old item for update.
 */
static TM_Result
ovbt_tid_update_lock_old(Relation rel, ovtid otid,
						 TransactionId xid, CommandId cid, bool key_update, Snapshot snapshot,
						 Snapshot crosscheck, bool wait, TM_FailureData *hufd, bool *this_xact_has_lock,
						 OVUndoRecPtr *prevundoptr_p)
{
	OVUndoRecPtr recent_oldest_undo;
	Buffer		buf;
	OVUndoRecPtr olditem_undoptr;
	bool		olditem_isdead;
	int			idx;
	TM_Result	result;
	bool		keep_old_undo_ptr = true;
	ovtid		next_tid;

	(void) xid;
	(void) cid;
	(void) wait;

	INJECTION_POINT("orvos_lock_updated_tuple", NULL);

	recent_oldest_undo = ovundo_get_oldest_undo_ptr(rel);

	/*
	 * Find the item to delete.
	 */
	idx = ovbt_tid_fetch(rel, otid, &buf, &olditem_undoptr, &olditem_isdead);
	if (idx == -1 || olditem_isdead)
	{
		/*
		 * or should this be TM_Invisible? The heapam at least just throws an
		 * error, I think..
		 */
		elog(ERROR, "could not find old tuple to update with TID (%u, %u) in TID tree",
			 OVTidGetBlockNumber(otid), OVTidGetOffsetNumber(otid));
	}
	*prevundoptr_p = olditem_undoptr;

	/*
	 * Is it visible to us?
	 */
	result = ov_SatisfiesUpdate(rel, snapshot, recent_oldest_undo,
								otid, olditem_undoptr,
								key_update ? LockTupleExclusive : LockTupleNoKeyExclusive,
								&keep_old_undo_ptr, this_xact_has_lock,
								hufd, &next_tid, NULL);
	if (result != TM_Ok)
	{
		UnlockReleaseBuffer(buf);
		/* FIXME: We should fill TM_FailureData *hufd correctly */
		return result;
	}

	if (crosscheck != InvalidSnapshot && result == TM_Ok)
	{
		/* Perform additional check for transaction-snapshot mode RI updates */
		/* FIXME: dummmy scan */
		OVTidTreeScan scan;
		TransactionId obsoleting_xid;
		OVUndoSlotVisibility visi_info;

		memset(&scan, 0, sizeof(scan));
		scan.rel = rel;
		scan.snapshot = crosscheck;
		scan.recent_oldest_undo = recent_oldest_undo;

		if (!ov_SatisfiesVisibility(&scan, olditem_undoptr, &obsoleting_xid, NULL, &visi_info))
		{
			UnlockReleaseBuffer(buf);
			/* FIXME: We should fill TM_FailureData *hufd correctly */
			result = TM_Updated;
		}
	}

	/*
	 * TODO: tuple-locking not implemented. Pray that there is no competing
	 * concurrent update!
	 */

	UnlockReleaseBuffer(buf);

	return TM_Ok;
}

/*
 * Subroutine of ovbt_update(): inserts the new, updated, item.
 */
static void
ovbt_tid_update_insert_new(Relation rel,
						   ovtid *newtid,
						   TransactionId xid, CommandId cid, OVUndoRecPtr prevundoptr)
{
	ovbt_tid_multi_insert(rel, newtid, 1, xid, cid, INVALID_SPECULATIVE_TOKEN, prevundoptr);
}

/*
 * Like ovbt_tid_multi_insert, but creates a DELTA_INSERT UNDO record
 * that tracks which columns were changed and the predecessor TID.
 * Used for column-delta UPDATEs.
 */
void
ovbt_tid_delta_insert(Relation rel, ovtid *tids,
					  TransactionId xid, CommandId cid,
					  ovtid predecessor_tid,
					  int natts, const bool *changed_cols,
					  OVUndoRecPtr prevundoptr)
{
	Buffer		buf;
	Page		page;
	OVBtreePageOpaque *opaque;
	OffsetNumber maxoff;
	ovtid		insert_target_key;
	List	   *newitems;
	ov_pending_undo_op *undo_op;
	ovtid		endtid;
	ovtid		tid;
	OVTidArrayItem *lastitem;
	bool		modified_orig;

	insert_target_key = MaxOVTid;
	buf = ovbt_descend(rel, OV_META_ATTRIBUTE_NUM,
					   insert_target_key, 0, false,
					   InvalidBuffer, InvalidBuffer);
	page = BufferGetPage(buf);
	opaque = OVBtreePageGetOpaque(page);
	maxoff = PageGetMaxOffsetNumber(page);

	if (maxoff >= FirstOffsetNumber)
	{
		ItemId		iid = PageGetItemId(page, maxoff);

		lastitem = (OVTidArrayItem *)
			PageGetItem(page, iid);
		endtid = lastitem->t_endtid;
	}
	else
	{
		endtid = opaque->ov_lokey;
		lastitem = NULL;
	}
	tid = endtid;

	undo_op = ovundo_create_for_delta_insert(rel, xid, cid,
											 tid, 1,
											 predecessor_tid,
											 natts, changed_cols,
											 prevundoptr);

	newitems = ovbt_tid_item_add_tids(
		lastitem, tid, 1,
		undo_op->reservation.undorecptr,
		&modified_orig);

	if (modified_orig)
		ovbt_tid_replace_item(rel, buf, maxoff,
							  newitems, undo_op);
	else
		ovbt_tid_add_items(rel, buf, newitems, undo_op);
	ReleaseBuffer(buf);

	list_free_deep(newitems);
	tids[0] = tid;
}

/*
 * Subroutine of ovbt_update(): mark old item as updated.
 */
static bool
ovbt_tid_mark_old_updated(Relation rel, ovtid otid, ovtid newtid,
						  TransactionId xid, CommandId cid, bool key_update, OVUndoRecPtr prevrecptr)
{
	OVUndoRecPtr recent_oldest_undo = ovundo_get_oldest_undo_ptr(rel);
	Buffer		buf;
	Page		page;
	OVUndoRecPtr olditem_undoptr;
	bool		olditem_isdead;
	OffsetNumber off;
	bool		keep_old_undo_ptr = true;
	ov_pending_undo_op *undo_op;
	List	   *newitems;
	OVTidArrayItem *origitem;

	/*
	 * Find the item to delete.  It could be part of a compressed item, we let
	 * ovbt_fetch() handle that.
	 */
	off = ovbt_tid_fetch(rel, otid, &buf, &olditem_undoptr, &olditem_isdead);
	if (!OffsetNumberIsValid(off) || olditem_isdead)
	{
		/*
		 * or should this be TM_Invisible? The heapam at least just throws an
		 * error, I think..
		 */
		elog(ERROR, "could not find old tuple to update with TID (%u, %u) in TID tree",
			 OVTidGetBlockNumber(otid), OVTidGetOffsetNumber(otid));
	}

	/*
	 * Did it change while we were inserting new row version?
	 */
	if (!OVUndoRecPtrEquals(olditem_undoptr, prevrecptr))
	{
		UnlockReleaseBuffer(buf);
		return false;
	}

	/* Prepare an UNDO record. */
	undo_op = ovundo_create_for_update(rel, xid, cid, otid, newtid,
									   keep_old_undo_ptr ? olditem_undoptr : InvalidUndoPtr,
									   key_update);

	/* Replace the OVTidArrayItem with one with the updated undo pointer. */
	page = BufferGetPage(buf);
	origitem = (OVTidArrayItem *) PageGetItem(page, PageGetItemId(page, off));
	newitems = ovbt_tid_item_change_undoptr(origitem, otid, undo_op->reservation.undorecptr,
											recent_oldest_undo);
	ovbt_tid_replace_item(rel, buf, off, newitems, undo_op);
	list_free_deep(newitems);
	ReleaseBuffer(buf);			/* ovbt_tid_replace_item unlocked 'buf' */

	return true;
}

TM_Result
ovbt_tid_lock(Relation rel, ovtid tid, TransactionId xid, CommandId cid,
			  LockTupleMode mode, bool follow_updates, Snapshot snapshot,
			  TM_FailureData *hufd, ovtid *next_tid, bool *this_xact_has_lock,
			  OVUndoSlotVisibility *visi_info)
{
	OVUndoRecPtr recent_oldest_undo = ovundo_get_oldest_undo_ptr(rel);
	Buffer		buf;
	Page		page;
	OVUndoRecPtr item_undoptr;
	bool		item_isdead;
	OffsetNumber off;
	TM_Result	result;
	bool		keep_old_undo_ptr = true;
	ov_pending_undo_op *undo_op;
	List	   *newitems;
	OVTidArrayItem *origitem;

	*next_tid = tid;

	off = ovbt_tid_fetch(rel, tid, &buf, &item_undoptr, &item_isdead);
	if (!OffsetNumberIsValid(off) || item_isdead)
	{
		/*
		 * or should this be TM_Invisible? The heapam at least just throws an
		 * error, I think..
		 */
		elog(ERROR, "could not find tuple to lock with TID (%u, %u)",
			 OVTidGetBlockNumber(tid), OVTidGetOffsetNumber(tid));
	}
	result = ov_SatisfiesUpdate(rel, snapshot, recent_oldest_undo,
								tid, item_undoptr, mode,
								&keep_old_undo_ptr, this_xact_has_lock,
								hufd, next_tid, visi_info);

	if (result != TM_Ok)
	{
		if (result == TM_Invisible && follow_updates &&
			TransactionIdIsInProgress(visi_info->xmin))
		{
			/*
			 * need to lock tuple irrespective of its visibility on
			 * follow_updates.
			 */
		}
		else
		{
			UnlockReleaseBuffer(buf);
			return result;
		}
	}

	/* Create UNDO record. */
	undo_op = ovundo_create_for_tuple_lock(rel, xid, cid, tid, mode,
										   keep_old_undo_ptr ? item_undoptr : InvalidUndoPtr);

	/* Replace the item with an identical one, but with updated undo pointer. */
	page = BufferGetPage(buf);
	origitem = (OVTidArrayItem *) PageGetItem(page, PageGetItemId(page, off));
	newitems = ovbt_tid_item_change_undoptr(origitem, tid, undo_op->reservation.undorecptr,
											recent_oldest_undo);
	ovbt_tid_replace_item(rel, buf, off, newitems, undo_op);
	list_free_deep(newitems);
	ReleaseBuffer(buf);			/* ovbt_tid_replace_item unlocked 'buf' */
	return TM_Ok;
}

/*
 * Collect all TIDs marked as dead in the TID tree.
 *
 * This is used during VACUUM.
 */
IntegerSet *
ovbt_collect_dead_tids(Relation rel, ovtid starttid, ovtid *endtid, uint64 *num_live_tuples)
{
	Buffer		buf = InvalidBuffer;
	IntegerSet *result;
	OVBtreePageOpaque *opaque;
	ovtid		nexttid;
	BlockNumber nextblock;
	OVTidItemIterator iter;

	memset(&iter, 0, sizeof(OVTidItemIterator));
	iter.context = CurrentMemoryContext;

	result = intset_create();

	nexttid = starttid;
	nextblock = InvalidBlockNumber;
	for (;;)
	{
		Page		page;
		OffsetNumber maxoff;
		OffsetNumber off;

		if (nextblock != InvalidBlockNumber)
		{
			buf = ReleaseAndReadBuffer(buf, rel, nextblock);
			LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
			page = BufferGetPage(buf);

			if (!ovbt_page_is_expected(rel, OV_META_ATTRIBUTE_NUM, nexttid, 0, buf))
			{
				UnlockReleaseBuffer(buf);
				buf = InvalidBuffer;
			}
		}

		if (!BufferIsValid(buf))
		{
			buf = ovbt_descend(rel, OV_META_ATTRIBUTE_NUM, nexttid, 0, true, InvalidBuffer, InvalidBuffer);
			if (!BufferIsValid(buf))
				return result;
			page = BufferGetPage(buf);
		}

		maxoff = PageGetMaxOffsetNumber(page);
		for (off = FirstOffsetNumber; off <= maxoff; off++)
		{
			ItemId		iid = PageGetItemId(page, off);
			OVTidArrayItem *item = (OVTidArrayItem *) PageGetItem(page, iid);

			ovbt_tid_item_unpack(item, &iter);

			for (int j = 0; j < iter.num_tids; j++)
			{
				(*num_live_tuples)++;
				if (iter.tid_undoslotnos[j] == OVBT_DEAD_UNDO_SLOT)
					intset_add_member(result, iter.tids[j]);
			}
		}

		opaque = OVBtreePageGetOpaque(page);
		nexttid = opaque->ov_hikey;
		nextblock = opaque->ov_next;

		LockBuffer(buf, BUFFER_LOCK_UNLOCK);

		if (nexttid == MaxPlusOneOVTid)
		{
			Assert(nextblock == InvalidBlockNumber);
			break;
		}

		if (intset_memory_usage(result) > (uint64) maintenance_work_mem * 1024)
			break;
	}

	if (BufferIsValid(buf))
		ReleaseBuffer(buf);

	*endtid = nexttid;
	return result;
}

/*
 * Mark item with given TID as dead.
 *
 * This is used when UNDO actions are performed, after a transaction becomes
 * old enough.
 */
void
ovbt_tid_mark_dead(Relation rel, ovtid tid, OVUndoRecPtr recent_oldest_undo)
{
	Buffer		buf;
	Page		page;
	OVUndoRecPtr item_undoptr;
	OffsetNumber off;
	OVTidArrayItem *origitem;
	List	   *newitems;
	bool		isdead;

	/* Find the item to delete. (It could be compressed) */
	off = ovbt_tid_fetch(rel, tid, &buf, &item_undoptr, &isdead);
	if (!OffsetNumberIsValid(off))
	{
		elog(WARNING, "could not find tuple to mark dead with TID (%u, %u)",
			 OVTidGetBlockNumber(tid), OVTidGetOffsetNumber(tid));
		return;
	}

	/* Mark the TID as DEAD. (Unless it's already dead) */
	if (isdead)
	{
		UnlockReleaseBuffer(buf);
		return;
	}

	page = BufferGetPage(buf);
	origitem = (OVTidArrayItem *) PageGetItem(page, PageGetItemId(page, off));
	newitems = ovbt_tid_item_change_undoptr(origitem, tid, DeadUndoPtr,
											recent_oldest_undo);
	ovbt_tid_replace_item(rel, buf, off, newitems, NULL);
	list_free_deep(newitems);
	ReleaseBuffer(buf);			/* ovbt_tid_replace_item unlocked 'buf' */
}


/*
 * Remove items for the given TIDs from the TID tree.
 *
 * This is used during VACUUM.
 */
void
ovbt_tid_remove(Relation rel, IntegerSet *tids)
{
	OVUndoRecPtr recent_oldest_undo = ovundo_get_oldest_undo_ptr(rel);
	ovtid		nexttid;
	MemoryContext oldcontext;
	MemoryContext tmpcontext;

	tmpcontext = AllocSetContextCreate(CurrentMemoryContext,
									   "OrvosAMVacuumContext",
									   ALLOCSET_DEFAULT_SIZES);
	oldcontext = MemoryContextSwitchTo(tmpcontext);

	intset_begin_iterate(tids);
	if (!intset_iterate_next(tids, &nexttid))
		nexttid = MaxPlusOneOVTid;

	while (nexttid < MaxPlusOneOVTid)
	{
		Buffer		buf;
		Page		page;
		OVBtreePageOpaque *opaque;
		List	   *newitems;
		OffsetNumber maxoff;
		OffsetNumber off;

		/*
		 * Find the leaf page containing the next item to remove
		 */
		buf = ovbt_descend(rel, OV_META_ATTRIBUTE_NUM, nexttid, 0, false, InvalidBuffer, InvalidBuffer);
		page = BufferGetPage(buf);
		opaque = OVBtreePageGetOpaque(page);

		/*
		 * Rewrite the items on the page, removing all TIDs that need to be
		 * removed from the page.
		 */
		newitems = NIL;
		maxoff = PageGetMaxOffsetNumber(page);
		for (off = FirstOffsetNumber; off <= maxoff; off++)
		{
			ItemId		iid = PageGetItemId(page, off);
			OVTidArrayItem *item = (OVTidArrayItem *) PageGetItem(page, iid);

			while (nexttid < item->t_firsttid)
			{
				if (!intset_iterate_next(tids, &nexttid))
					nexttid = MaxPlusOneOVTid;
			}

			if (nexttid < item->t_endtid)
			{
				List	   *newitemsx = ovbt_tid_item_remove_tids(item, &nexttid, tids,
																  recent_oldest_undo);

				newitems = list_concat(newitems, newitemsx);
			}
			else
			{
				/* keep this item unmodified */
				newitems = lappend(newitems, item);
			}
		}

		while (nexttid < opaque->ov_hikey)
		{
			if (!intset_iterate_next(tids, &nexttid))
				nexttid = MaxPlusOneOVTid;
		}

		/* Pass the list to the recompressor. */
		IncrBufferRefCount(buf);
		if (newitems)
		{
			ovbt_tid_recompress_replace(rel, buf, newitems, NULL);
		}
		else
		{
			ov_split_stack *stack;

			stack = ovbt_unlink_page(rel, OV_META_ATTRIBUTE_NUM, buf, 0);

			if (!stack)
			{
				/* failed. */
				Page		newpage = PageGetTempPageCopySpecial(BufferGetPage(buf));

				stack = ov_new_split_stack_entry(buf, newpage);
			}

			/* apply the changes */
			ov_apply_split_changes(rel, stack, NULL);
		}

		ReleaseBuffer(buf);

		MemoryContextReset(tmpcontext);
	}
	MemoryContextSwitchTo(oldcontext);
	MemoryContextDelete(tmpcontext);
}

/*
 * Clear an item's UNDO pointer.
 *
 * This is used during VACUUM, to clear out aborted deletions.
 */
void
ovbt_tid_undo_deletion(Relation rel, ovtid tid, OVUndoRecPtr undoptr,
					   OVUndoRecPtr recent_oldest_undo)
{
	Buffer		buf;
	Page		page;
	OVUndoRecPtr item_undoptr;
	bool		item_isdead;
	OffsetNumber off;

	/* Find the item to delete. (It could be compressed) */
	off = ovbt_tid_fetch(rel, tid, &buf, &item_undoptr, &item_isdead);
	if (!OffsetNumberIsValid(off))
	{
		elog(WARNING, "could not find aborted tuple to remove with TID (%u, %u)",
			 OVTidGetBlockNumber(tid), OVTidGetOffsetNumber(tid));
		return;
	}

	if (OVUndoRecPtrEquals(item_undoptr, undoptr))
	{
		OVTidArrayItem *origitem;
		List	   *newitems;

		/*
		 * FIXME: we're overwriting the undo pointer with 'invalid', meaning
		 * the tuple becomes visible to everyone. That doesn't seem right.
		 * Shouldn't we restore the previous undo pointer, if the insertion
		 * was not yet visible to everyone?
		 */
		page = BufferGetPage(buf);
		origitem = (OVTidArrayItem *) PageGetItem(page, PageGetItemId(page, off));
		newitems = ovbt_tid_item_change_undoptr(origitem, tid, InvalidUndoPtr,
												recent_oldest_undo);
		ovbt_tid_replace_item(rel, buf, off, newitems, NULL);
		list_free_deep(newitems);
		ReleaseBuffer(buf);		/* ovbt_tid_replace_item unlocked 'buf' */
	}
	else
	{
		Assert(item_isdead ||
			   item_undoptr.counter > undoptr.counter ||
			   !IsOVUndoRecPtrValid(&item_undoptr));
		UnlockReleaseBuffer(buf);
	}
}

/* ----------------------------------------------------------------
 *						 Internal routines
 * ----------------------------------------------------------------
 */

void
ovbt_tid_clear_speculative_token(Relation rel, ovtid tid, uint32 spectoken, bool forcomplete)
{
	Buffer		buf;
	OVUndoRecPtr item_undoptr;
	bool		item_isdead;
	bool		found;

	(void) spectoken;
	(void) forcomplete;

	found = ovbt_tid_fetch(rel, tid, &buf, &item_undoptr, &item_isdead);
	if (!found || item_isdead)
		elog(ERROR, "couldn't find item for meta column for inserted tuple with TID (%u, %u) in rel %s",
			 OVTidGetBlockNumber(tid), OVTidGetOffsetNumber(tid), rel->rd_rel->relname.data);

	ovundo_clear_speculative_token(rel, item_undoptr);

	UnlockReleaseBuffer(buf);
}

/*
 * Fetch the item with given TID. The page containing the item is kept locked, and
 * returned to the caller in *buf_p. This is used to locate a tuple for updating
 * or deleting it.
 */
static OffsetNumber
ovbt_tid_fetch(Relation rel, ovtid tid, Buffer *buf_p, OVUndoRecPtr *undoptr_p, bool *isdead_p)
{
	Buffer		buf;
	Page		page;
	OffsetNumber maxoff;
	OffsetNumber off;

	buf = ovbt_descend(rel, OV_META_ATTRIBUTE_NUM, tid, 0, false, InvalidBuffer, InvalidBuffer);
	if (buf == InvalidBuffer)
	{
		*buf_p = InvalidBuffer;
		*undoptr_p = InvalidUndoPtr;
		return InvalidOffsetNumber;
	}
	page = BufferGetPage(buf);
	maxoff = PageGetMaxOffsetNumber(page);

	/* Find the item on the page that covers the target TID */
	off = ovbt_binsrch_tidpage(tid, page);
	if (off >= FirstOffsetNumber && off <= maxoff)
	{
		ItemId		iid = PageGetItemId(page, off);
		OVTidArrayItem *item = (OVTidArrayItem *) PageGetItem(page, iid);

		if (tid < item->t_endtid)
		{
			OVTidItemIterator iter;

			memset(&iter, 0, sizeof(OVTidItemIterator));
			iter.context = CurrentMemoryContext;

			ovbt_tid_item_unpack(item, &iter);

			/*
			 * TODO: could do binary search here. Better yet, integrate the
			 * unpack function with the callers
			 */
			for (int i = 0; i < iter.num_tids; i++)
			{
				if (iter.tids[i] == tid)
				{
					int			slotno = iter.tid_undoslotnos[i];
					OVUndoRecPtr undoptr = iter.undoslots[slotno];

					*isdead_p = (slotno == OVBT_DEAD_UNDO_SLOT);
					*undoptr_p = undoptr;
					*buf_p = buf;

					if (iter.tids)
						pfree(iter.tids);
					if (iter.tid_undoslotnos)
						pfree(iter.tid_undoslotnos);

					return off;
				}
			}

			if (iter.tids)
				pfree(iter.tids);
			if (iter.tid_undoslotnos)
				pfree(iter.tid_undoslotnos);
		}
	}
	return InvalidOffsetNumber;
}

/*
 * This helper function is used to implement INSERT.
 *
 * The items in 'newitems' are added to the page, to the correct position.
 * FIXME: Actually, they're always just added to the end of the page, and that
 * better be the correct position.
 *
 * This function handles splitting the page if needed.
 */
static void
ovbt_tid_add_items(Relation rel, Buffer buf, List *newitems, ov_pending_undo_op * undo_op)
{
	Page		page = BufferGetPage(buf);
	OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
	OffsetNumber off;
	Size		newitemsize;
	ListCell   *lc;

	newitemsize = 0;
	foreach(lc, newitems)
	{
		OVTidArrayItem *item = (OVTidArrayItem *) lfirst(lc);

		newitemsize += sizeof(ItemIdData) + item->t_size;
	}

	if (newitemsize <= PageGetExactFreeSpace(page))
	{
		/* The new items fit on the page. Add them. */
		OffsetNumber startoff;

		START_CRIT_SECTION();

		startoff = maxoff + 1;
		off = startoff;
		foreach(lc, newitems)
		{
			OVTidArrayItem *item = (OVTidArrayItem *) lfirst(lc);

			if (!PageAddItem(page, item, item->t_size, off, true, false))
				elog(ERROR, "could not add item to TID tree page");
			off++;
		}

		if (undo_op)
			ovundo_finish_pending_op(undo_op, (char *) &undo_op->payload);

		MarkBufferDirty(buf);

		if (RelationNeedsWAL(rel))
			ovbt_wal_log_leaf_items(rel, OV_META_ATTRIBUTE_NUM, buf,
									startoff, false, newitems,
									undo_op);

		END_CRIT_SECTION();

		LockBuffer(buf, BUFFER_LOCK_UNLOCK);

		if (undo_op)
		{
			UnlockReleaseBuffer(undo_op->reservation.undobuf);
			pfree(undo_op);
		}
	}
	else
	{
		List	   *items = NIL;

		/* Collect all the old items on the page to a list */
		for (off = FirstOffsetNumber; off <= maxoff; off++)
		{
			ItemId		iid = PageGetItemId(page, off);
			OVTidArrayItem *item = (OVTidArrayItem *) PageGetItem(page, iid);

			/*
			 * Get the next item to process from the page.
			 */
			items = lappend(items, item);
		}

		/* Add any new items to the end */
		foreach(lc, newitems)
		{
			items = lappend(items, lfirst(lc));
		}

		/* Now pass the list to the recompressor. */
		IncrBufferRefCount(buf);
		if (items)
		{
			ovbt_tid_recompress_replace(rel, buf, items, undo_op);
		}
		else
		{
			ov_split_stack *stack;

			stack = ovbt_unlink_page(rel, OV_META_ATTRIBUTE_NUM, buf, 0);

			if (!stack)
			{
				/* failed. */
				Page		newpage = PageGetTempPageCopySpecial(BufferGetPage(buf));

				stack = ov_new_split_stack_entry(buf, newpage);
			}

			/* apply the changes */
			ov_apply_split_changes(rel, stack, undo_op);
		}

		list_free(items);
	}
}


/*
 * This helper function is used to implement INSERT, UPDATE and DELETE.
 *
 * If 'newitems' is not empty, the items in the list are added to the page,
 * to the correct position. FIXME: Actually, they're always just added to
 * the end of the page, and that better be the correct position.
 *
 * This function handles decompressing and recompressing items, and splitting
 * the page if needed.
 */
static void
ovbt_tid_replace_item(Relation rel, Buffer buf, OffsetNumber targetoff, List *newitems,
					  ov_pending_undo_op * undo_op)
{
	Page		page = BufferGetPage(buf);
	ItemId		iid;
	OVTidArrayItem *olditem;
	ListCell   *lc;
	ssize_t		sizediff;

	/*
	 * Find the item that covers the given tid.
	 */
	if (targetoff < FirstOffsetNumber || targetoff > PageGetMaxOffsetNumber(page))
		elog(ERROR, "could not find item at off %d to replace", targetoff);
	iid = PageGetItemId(page, targetoff);
	olditem = (OVTidArrayItem *) PageGetItem(page, iid);

	/* Calculate how much free space we'll need */
	sizediff = -(ssize_t) (olditem->t_size + sizeof(ItemIdData));
	foreach(lc, newitems)
	{
		OVTidArrayItem *newitem = (OVTidArrayItem *) lfirst(lc);

		sizediff += (ssize_t) (newitem->t_size + sizeof(ItemIdData));
	}

	/* Can we fit them? */
	if (sizediff <= (ssize_t) PageGetExactFreeSpace(page))
	{
		OVTidArrayItem *newitem;
		OffsetNumber off;

		START_CRIT_SECTION();

		/* Remove existing item, and add new ones */
		if (newitems == 0)
			PageIndexTupleDelete(page, targetoff);
		else
		{
			lc = list_head(newitems);
			newitem = (OVTidArrayItem *) lfirst(lc);
			if (!PageIndexTupleOverwrite(page, targetoff, newitem, newitem->t_size))
				elog(ERROR, "could not replace item in TID tree page at off %d", targetoff);
			lc = lnext(newitems, lc);

			off = targetoff + 1;
			for (; lc != NULL; lc = lnext(newitems, lc))
			{
				newitem = (OVTidArrayItem *) lfirst(lc);
				if (!PageAddItem(page, newitem, newitem->t_size, off, false, false))
					elog(ERROR, "could not add item in TID tree page at off %d", off);
				off++;
			}
		}
		MarkBufferDirty(buf);

		if (undo_op)
			ovundo_finish_pending_op(undo_op, (char *) &undo_op->payload);

		if (RelationNeedsWAL(rel))
			ovbt_wal_log_leaf_items(rel, OV_META_ATTRIBUTE_NUM, buf, targetoff, true, newitems, undo_op);
		END_CRIT_SECTION();

#ifdef USE_ASSERT_CHECKING
		{
			ovtid		lasttid = 0;
			OVTidArrayItem *item;

			for (off = FirstOffsetNumber; off <= PageGetMaxOffsetNumber(page); off++)
			{
				iid = PageGetItemId(page, off);
				item = (OVTidArrayItem *) PageGetItem(page, iid);

				Assert(item->t_firsttid >= lasttid);
				lasttid = item->t_endtid;
			}
		}
#endif

		LockBuffer(buf, BUFFER_LOCK_UNLOCK);

		if (undo_op)
		{
			UnlockReleaseBuffer(undo_op->reservation.undobuf);
			pfree(undo_op);
		}
	}
	else
	{
		/* Have to split the page. */
		List	   *items = NIL;
		OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
		OffsetNumber off;
		OVTidArrayItem *item;

		/*
		 * Construct a List that contains all the items in the right order,
		 * and let ovbt_tid_recompress_page() do the heavy lifting to fit them
		 * on pages.
		 */
		for (off = FirstOffsetNumber; off <= maxoff; off++)
		{
			iid = PageGetItemId(page, off);
			item = (OVTidArrayItem *) PageGetItem(page, iid);

			if (off == targetoff)
			{
				foreach(lc, newitems)
				{
					items = lappend(items, (OVTidArrayItem *) lfirst(lc));
				}
			}
			else
				items = lappend(items, item);
		}

#ifdef USE_ASSERT_CHECKING
		{
			ovtid		endtid = 0;

			foreach(lc, items)
			{
				OVTidArrayItem *i = (OVTidArrayItem *) lfirst(lc);

				Assert(i->t_firsttid >= endtid);
				Assert(i->t_endtid > i->t_firsttid);
				endtid = i->t_endtid;
			}
		}
#endif

		/* Pass the list to the recompressor. */
		IncrBufferRefCount(buf);
		if (items)
		{
			ovbt_tid_recompress_replace(rel, buf, items, undo_op);
		}
		else
		{
			ov_split_stack *stack;

			stack = ovbt_unlink_page(rel, OV_META_ATTRIBUTE_NUM, buf, 0);

			if (!stack)
			{
				/* failed. */
				Page		newpage = PageGetTempPageCopySpecial(BufferGetPage(buf));

				stack = ov_new_split_stack_entry(buf, newpage);
			}

			/* apply the changes */
			ov_apply_split_changes(rel, stack, undo_op);
		}

		list_free(items);
	}
}

/*
 * Recompressor routines
 */
typedef struct
{
	Page		currpage;

	/*
	 * first page writes over the old buffer, subsequent pages get
	 * newly-allocated buffers
	 */
	ov_split_stack *stack_head;
	ov_split_stack *stack_tail;

	int			num_pages;
	int			free_space_per_page;

	ovtid		hikey;
}			ovbt_tid_recompress_context;

static void
ovbt_tid_recompress_newpage(ovbt_tid_recompress_context * cxt, ovtid nexttid, int flags)
{
	Page		newpage;
	OVBtreePageOpaque *newopaque;
	ov_split_stack *stack;

	if (cxt->currpage)
	{
		/* set the last tid on previous page */
		OVBtreePageOpaque *oldopaque = OVBtreePageGetOpaque(cxt->currpage);

		oldopaque->ov_hikey = nexttid;
	}

	newpage = (Page) palloc(BLCKSZ);
	PageInit(newpage, BLCKSZ, sizeof(OVBtreePageOpaque));

	stack = ov_new_split_stack_entry(InvalidBuffer, /* will be assigned later */
									 newpage);
	if (cxt->stack_tail)
		cxt->stack_tail->next = stack;
	else
		cxt->stack_head = stack;
	cxt->stack_tail = stack;

	cxt->currpage = newpage;

	newopaque = OVBtreePageGetOpaque(newpage);
	newopaque->ov_attno = OV_META_ATTRIBUTE_NUM;
	newopaque->ov_next = InvalidBlockNumber;	/* filled in later */
	newopaque->ov_lokey = nexttid;
	newopaque->ov_hikey = cxt->hikey;	/* overwritten later, if this is not
										 * last page */
	newopaque->ov_level = 0;
	newopaque->ov_flags = flags;
	newopaque->ov_page_id = OV_BTREE_PAGE_ID;
}

static void
ovbt_tid_recompress_add_to_page(ovbt_tid_recompress_context * cxt, OVTidArrayItem *item)
{
	OffsetNumber maxoff;
	Size		freespc;

	freespc = PageGetExactFreeSpace(cxt->currpage);
	if (freespc < item->t_size + sizeof(ItemIdData) ||
		freespc < (Size) cxt->free_space_per_page)
	{
		ovbt_tid_recompress_newpage(cxt, item->t_firsttid, 0);
	}

	maxoff = PageGetMaxOffsetNumber(cxt->currpage);
	if (!PageAddItem(cxt->currpage, item, item->t_size, maxoff + 1, true, false))
		elog(ERROR, "could not add item to TID tree page");
}

/*
 * Subroutine of ovbt_tid_recompress_replace.  Compute how much space the
 * items will take, and compute how many pages will be needed for them, and
 * decide how to distribute any free space thats's left over among the
 * pages.
 *
 * Like in B-tree indexes, we aim for 50/50 splits, except for the
 * rightmost page where aim for 90/10, so that most of the free space is
 * left to the end of the index, where it's useful for new inserts. The
 * 90/10 splits ensure that the we don't waste too much space on a table
 * that's loaded at the end, and never updated.
 */
static void
ovbt_tid_recompress_picksplit(ovbt_tid_recompress_context * cxt, List *items)
{
	size_t		total_sz;
	int			num_pages;
	int			space_on_empty_page;
	Size		free_space_per_page;
	ListCell   *lc;

	space_on_empty_page = BLCKSZ - MAXALIGN(SizeOfPageHeaderData) - MAXALIGN(sizeof(OVBtreePageOpaque));

	/* Compute total space needed for all the items. */
	total_sz = 0;
	foreach(lc, items)
	{
		OVTidArrayItem *item = lfirst(lc);

		total_sz += sizeof(ItemIdData) + item->t_size;
	}

	/* How many pages will we need for them? */
	num_pages = (total_sz + space_on_empty_page - 1) / space_on_empty_page;

	/* If everything fits on one page, don't split */
	if (num_pages == 1)
	{
		free_space_per_page = 0;
	}
	/* If this is the rightmost page, do a 90/10 split */
	else if (cxt->hikey == MaxPlusOneOVTid)
	{
		/*
		 * What does 90/10 mean if we have to use more than two pages? It
		 * means that 10% of the items go to the last page, and 90% are
		 * distributed to all the others.
		 */
		double		total_free_space;

		total_free_space = space_on_empty_page * num_pages - total_sz;

		free_space_per_page = total_free_space * 0.1 / (num_pages - 1);
	}
	/* Otherwise, aim for an even 50/50 split */
	else
	{
		free_space_per_page = (space_on_empty_page * num_pages - total_sz) / num_pages;
	}

	cxt->num_pages = num_pages;
	cxt->free_space_per_page = free_space_per_page;
}

/*
 * Rewrite a leaf page, with given 'items' as the new content.
 *
 * If there are any uncompressed items in the list, we try to compress them.
 * Any already-compressed items are added as is.
 *
 * If the items no longer fit on the page, then the page is split. It is
 * entirely possible that they don't fit even on two pages; we split the page
 * into as many pages as needed. Hopefully not more than a few pages, though,
 * because otherwise you might hit limits on the number of buffer pins (with
 * tiny shared_buffers).
 *
 * On entry, 'oldbuf' must be pinned and exclusive-locked. On exit, the lock
 * is released, but it's still pinned.
 *
 * TODO: Try to combine single items, and existing array-items, into new array
 * items.
 */
static void
ovbt_tid_recompress_replace(Relation rel, Buffer oldbuf, List *items, ov_pending_undo_op * undo_op)
{
	ListCell   *lc;
	ovbt_tid_recompress_context cxt;
	OVBtreePageOpaque *oldopaque = OVBtreePageGetOpaque(BufferGetPage(oldbuf));
	BlockNumber orignextblk;
	ov_split_stack *stack;
	List	   *downlinks = NIL;

	orignextblk = oldopaque->ov_next;

	cxt.currpage = NULL;
	cxt.stack_head = cxt.stack_tail = NULL;
	cxt.hikey = oldopaque->ov_hikey;

	ovbt_tid_recompress_picksplit(&cxt, items);
	ovbt_tid_recompress_newpage(&cxt, oldopaque->ov_lokey, (oldopaque->ov_flags & OVBT_ROOT));

	foreach(lc, items)
	{
		OVTidArrayItem *item = (OVTidArrayItem *) lfirst(lc);

		ovbt_tid_recompress_add_to_page(&cxt, item);
	}

	/*
	 * Ok, we now have a list of pages, to replace the original page, as
	 * private in-memory copies. Allocate buffers for them, and write them
	 * out.
	 *
	 * allocate all the pages before entering critical section, so that
	 * out-of-disk-space doesn't lead to PANIC
	 */
	stack = cxt.stack_head;
	Assert(stack->buf == InvalidBuffer);
	stack->buf = oldbuf;
	while (stack->next)
	{
		Page		thispage = stack->page;
		OVBtreePageOpaque *thisopaque = OVBtreePageGetOpaque(thispage);
		OVBtreeInternalPageItem *downlink;
		Buffer		nextbuf;

		Assert(stack->next->buf == InvalidBuffer);

		nextbuf = ovpage_getnewbuf(rel, InvalidBuffer);
		stack->next->buf = nextbuf;

		thisopaque->ov_next = BufferGetBlockNumber(nextbuf);

		downlink = palloc(sizeof(OVBtreeInternalPageItem));
		downlink->tid = thisopaque->ov_hikey;
		downlink->childblk = BufferGetBlockNumber(nextbuf);
		downlinks = lappend(downlinks, downlink);

		stack = stack->next;
	}
	/* last one in the chain */
	OVBtreePageGetOpaque(stack->page)->ov_next = orignextblk;

	/*
	 * ovbt_tid_recompress_picksplit() calculated that we'd need
	 * 'cxt.num_pages' pages. Check that it matches with how many pages we
	 * actually created.
	 */
	Assert(list_length(downlinks) + 1 == cxt.num_pages);

	/* If we had to split, insert downlinks for the new pages. */
	if (cxt.stack_head->next)
	{
		oldopaque = OVBtreePageGetOpaque(cxt.stack_head->page);

		if ((oldopaque->ov_flags & OVBT_ROOT) != 0)
		{
			OVBtreeInternalPageItem *downlink;

			downlink = palloc(sizeof(OVBtreeInternalPageItem));
			downlink->tid = MinOVTid;
			downlink->childblk = BufferGetBlockNumber(cxt.stack_head->buf);
			downlinks = lcons(downlink, downlinks);

			cxt.stack_tail->next = ovbt_newroot(rel, OV_META_ATTRIBUTE_NUM,
												oldopaque->ov_level + 1, downlinks);

			/* clear the OVBT_ROOT flag on the old root page */
			oldopaque->ov_flags &= ~OVBT_ROOT;
		}
		else
		{
			cxt.stack_tail->next = ovbt_insert_downlinks(rel, OV_META_ATTRIBUTE_NUM,
														 oldopaque->ov_lokey, BufferGetBlockNumber(oldbuf), oldopaque->ov_level + 1,
														 downlinks, oldbuf);
		}
		/* note: stack_tail is not the real tail anymore */
	}

	/* Finally, overwrite all the pages we had to modify */
	ov_apply_split_changes(rel, cxt.stack_head, undo_op);
}

static OffsetNumber
ovbt_binsrch_tidpage(ovtid key, Page page)
{
	OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
	OffsetNumber low,
				high,
				mid;

	low = FirstOffsetNumber;
	high = maxoff + 1;
	while (high > low)
	{
		ItemId		iid;
		OVTidArrayItem *item;

		mid = low + (high - low) / 2;

		iid = PageGetItemId(page, mid);
		item = (OVTidArrayItem *) PageGetItem(page, iid);

		if (key >= item->t_firsttid)
			low = mid + 1;
		else
			high = mid;
	}
	return low - 1;
}
