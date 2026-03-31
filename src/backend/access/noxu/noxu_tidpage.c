/*
 * noxu_tidpage.c
 *		Routines for handling the TID tree.
 *
 * A Noxu table consists of multiple B-trees, one for each attribute. The
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
 *	  src/backend/access/noxu/noxu_tidpage.c
 */
#include "postgres.h"

#include "access/noxu_internal.h"
#include "access/relundo.h"
#include "access/xactundo.h"
#include "lib/integerset.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/predicate.h"
#include "storage/procarray.h"
#include "utils/injection_point.h"
#include "utils/memutils.h"
#include "utils/rel.h"


/*
 * nx_relundo_write_record - Write UNDO record data into RelUndo-reserved space.
 *
 * This is used instead of RelUndoFinish() because Noxu bundles B-tree and
 * UNDO changes into a single atomic WAL record.  RelUndoFinish() does its own
 * WAL logging and releases the buffer, which is incompatible with Noxu's
 * approach.
 *
 * This function only writes the record data.  The caller is responsible for
 * WAL logging and buffer release.
 *
 * Must be called inside a critical section (like nxundo_finish_pending_op).
 */
static void
nx_relundo_write_record(nx_pending_undo_op *pendingop)
{
	Assert(CritSectionCount > 0);

	/* Write the payload (RelUndoRecordHeader + type-specific data) into
	 * the reserved space in the UNDO page buffer */
	memcpy(pendingop->reservation.ptr, (char *) pendingop->payload,
		   pendingop->reservation.length);

	MarkBufferDirty(pendingop->reservation.undobuf);
}

/*
 * nx_relundo_create_op - Allocate and initialize an nx_pending_undo_op
 * using RelUndoReserve to get storage from the per-relation UNDO fork.
 *
 * The caller should fill in the type-specific payload after the
 * RelUndoRecordHeader in the returned op's payload area.
 *
 * Returns a palloc'd nx_pending_undo_op with:
 *   - reservation fields populated from RelUndoReserve
 *   - payload area large enough for header + payload_size
 *   - RelUndoRecordHeader at the start of payload, partially filled in
 */
static nx_pending_undo_op *
nx_relundo_create_op(Relation rel, uint16 urec_type, TransactionId xid,
					 CommandId cid, RelUndoRecPtr prev_undo_ptr,
					 Size payload_size)
{
	nx_pending_undo_op *pending_op;
	Size		total_record_size;
	RelUndoRecordHeader *hdr;
	Buffer		undo_buffer;
	RelUndoRecPtr ptr;
	Page		page;
	char	   *contents;
	uint16		offset;

	total_record_size = SizeOfRelUndoRecordHeader + payload_size;

	/* Reserve space in the per-relation UNDO fork */
	ptr = RelUndoReserve(rel, total_record_size, &undo_buffer);

	/* Allocate the pending op with enough room for header + payload */
	pending_op = palloc(offsetof(nx_pending_undo_op, payload) + total_record_size);
	pending_op->is_update = false;

	/* Fill in the reservation fields */
	pending_op->reservation.undobuf = undo_buffer;
	pending_op->reservation.undorecptr = ptr;
	Assert(total_record_size <= PG_UINT16_MAX);
	pending_op->reservation.length = (uint16) total_record_size;

	/* Calculate the direct pointer into the buffer page */
	page = BufferGetPage(undo_buffer);
	contents = PageGetContents(page);
	offset = RelUndoGetOffset(ptr);
	pending_op->reservation.ptr = contents + offset;

	/* Fill in the RelUndoRecordHeader at the start of payload */
	hdr = (RelUndoRecordHeader *) pending_op->payload;
	hdr->urec_type = urec_type;
	hdr->urec_len = (uint16) total_record_size;
	hdr->urec_xid = xid;
	hdr->urec_cid = cid;
	hdr->urec_prevundorec = prev_undo_ptr;
	hdr->info_flags = 0;
	hdr->tuple_len = 0;

	/* Register with transaction UNDO system for rollback support */
	RegisterPerRelUndo(RelationGetRelid(rel), ptr);

	return pending_op;
}

/*
 * Helper to get the type-specific payload area in an nx_pending_undo_op
 * created by nx_relundo_create_op.
 */
static inline void *
nx_relundo_get_payload(nx_pending_undo_op *op)
{
	return (char *) op->payload + SizeOfRelUndoRecordHeader;
}

/* prototypes for local functions */
static void nxbt_tid_recompress_replace(Relation rel, Buffer oldbuf, List *items, nx_pending_undo_op * undo_op);
static OffsetNumber nxbt_tid_fetch(Relation rel, nxtid tid,
								   Buffer *buf_p, RelUndoRecPtr *undo_ptr_p, bool *isdead_p);
static void nxbt_tid_add_items(Relation rel, Buffer buf, List *newitems,
							   nx_pending_undo_op * pending_undo_op);
static void nxbt_tid_replace_item(Relation rel, Buffer buf, OffsetNumber off, List *newitems,
								  nx_pending_undo_op * pending_undo_op);

static TM_Result nxbt_tid_update_lock_old(Relation rel, nxtid otid,
										  TransactionId xid, CommandId cid, bool key_update, Snapshot snapshot,
										  Snapshot crosscheck, bool wait, TM_FailureData *hufd,
										  bool *this_xact_has_lock, RelUndoRecPtr *prevundoptr_p);
static void nxbt_tid_update_insert_new(Relation rel, nxtid *newtid,
									   TransactionId xid, CommandId cid, RelUndoRecPtr prevundoptr);
static bool nxbt_tid_mark_old_updated(Relation rel, nxtid otid, nxtid newtid,
									  TransactionId xid, CommandId cid, bool key_update, RelUndoRecPtr prevrecptr);
static OffsetNumber nxbt_binsrch_tidpage(nxtid key, Page page);

/* ----------------------------------------------------------------
 *						 Public interface
 * ----------------------------------------------------------------
 */

/*
 * Begin a scan of the btree.
 */
void
nxbt_tid_begin_scan(Relation rel, nxtid starttid,
					nxtid endtid, Snapshot snapshot, NXTidTreeScan * scan)
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
	scan->serializable = false;  /* Initialize to prevent uninitialized read */
	scan->lastbuf = InvalidBuffer;
	scan->lastoff = InvalidOffsetNumber;

	scan->recent_oldest_undo = nx_get_oldest_visible_undo_ptr(rel);
}

/*
 * Reset the 'next' TID in a scan to the given TID.
 */
void
nxbt_tid_reset_scan(Relation rel, NXTidTreeScan * scan, nxtid starttid, nxtid endtid, nxtid currtid)
{
	scan->starttid = starttid;
	scan->endtid = endtid;
	scan->currtid = currtid;
	scan->array_curr_idx = -1;
	scan->recent_oldest_undo = nx_get_oldest_visible_undo_ptr(rel);
}

void
nxbt_tid_end_scan(NXTidTreeScan * scan)
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
 * Helper function of nxbt_tid_scan_next_array(), to extract Datums from the given
 * array item into the scan->array_* fields.
 */
static void
nxbt_tid_scan_extract_array(NXTidTreeScan * scan, NXTidArrayItem *aitem)
{
	bool		slots_visible[4];
	int			first;
	int			last;
	int			num_visible_tids;
	int			continue_at;

	nxbt_tid_item_unpack(aitem, &scan->array_iter);

	slots_visible[NXBT_OLD_UNDO_SLOT] = true;
	slots_visible[NXBT_DEAD_UNDO_SLOT] = false;

	scan->array_iter.undoslot_visibility[NXBT_OLD_UNDO_SLOT] = InvalidUndoSlotVisibility;
	scan->array_iter.undoslot_visibility[NXBT_OLD_UNDO_SLOT].xmin = FrozenTransactionId;

	scan->array_iter.undoslot_visibility[NXBT_DEAD_UNDO_SLOT] = InvalidUndoSlotVisibility;

	for (int i = 2; i < aitem->t_num_undo_slots; i++)
	{
		RelUndoRecPtr undoptr = scan->array_iter.undoslots[i];
		TransactionId obsoleting_xid;

		scan->array_iter.undoslot_visibility[i] = InvalidUndoSlotVisibility;

		slots_visible[i] = nx_SatisfiesVisibility(scan, undoptr, &obsoleting_xid,
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
 * the next call to this function, nxbt_tid_reset_scan() or
 * nxbt_tid_end_scan().
 *
 * Returns true if there was another item, or false if we reached the
 * end of the scan.
 *
 * This is normally not used directly, see nxbt_tid_scan_next() wrapper.
 */
bool
nxbt_tid_scan_next_array(NXTidTreeScan * scan, nxtid nexttid, ScanDirection direction)
{
	if (!scan->active)
		return InvalidNXTid;

	/*
	 * Process items, until we find something that is visible to the snapshot.
	 *
	 * This advances nexttid as it goes.
	 */
	while (nexttid < scan->endtid && nexttid >= scan->starttid)
	{
		Buffer		buf;
		Page		page;
		NXBtreePageOpaque *opaque;
		OffsetNumber maxoff;
		OffsetNumber off;
		BlockNumber next;

		/*
		 * Find and lock the leaf page containing nexttid.
		 */
		buf = nxbt_find_and_lock_leaf_containing_tid(scan->rel, NX_META_ATTRIBUTE_NUM,
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
		opaque = NXBtreePageGetOpaque(page);
		Assert(opaque->nx_page_id == NX_BTREE_PAGE_ID);

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
				NXTidArrayItem *item = (NXTidArrayItem *) PageGetItem(page, iid);

				if (nexttid >= item->t_endtid)
					off = scan->lastoff + 1;
			}

			for (; off <= maxoff; off++)
			{
				ItemId		iid = PageGetItemId(page, off);
				NXTidArrayItem *item = (NXTidArrayItem *) PageGetItem(page, iid);

				if (nexttid >= item->t_endtid)
					continue;

				if (item->t_firsttid >= scan->endtid)
				{
					nexttid = scan->endtid;
					break;
				}

				nxbt_tid_scan_extract_array(scan, item);

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
			if (nexttid < opaque->nx_hikey)
				nexttid = opaque->nx_hikey;
			next = opaque->nx_next;
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
				NXTidArrayItem *item = (NXTidArrayItem *) PageGetItem(page, iid);

				if (nexttid < item->t_firsttid)
					continue;

				if (item->t_endtid < scan->starttid)
				{
					nexttid = scan->starttid - 1;
					break;
				}

				nxbt_tid_scan_extract_array(scan, item);

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
			if (nexttid >= opaque->nx_lokey)
				nexttid = opaque->nx_lokey - 1;
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
nxtid
nxbt_get_last_tid(Relation rel)
{
	nxtid		rightmostkey;
	nxtid		tid;
	Buffer		buf;
	Page		page;
	NXBtreePageOpaque *opaque;
	OffsetNumber maxoff;

	/* Find the rightmost leaf */
	rightmostkey = MaxNXTid;
	buf = nxbt_descend(rel, NX_META_ATTRIBUTE_NUM, rightmostkey, 0, true, false, InvalidBuffer, InvalidBuffer);
	if (!BufferIsValid(buf))
	{
		return MinNXTid;
	}
	page = BufferGetPage(buf);
	opaque = NXBtreePageGetOpaque(page);

	/*
	 * Look at the last item, for its tid.
	 */
	maxoff = PageGetMaxOffsetNumber(page);
	if (maxoff >= FirstOffsetNumber)
	{
		ItemId		iid = PageGetItemId(page, maxoff);
		NXTidArrayItem *lastitem = (NXTidArrayItem *) PageGetItem(page, iid);

		tid = lastitem->t_endtid;
	}
	else
	{
		tid = opaque->nx_lokey;
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
nxbt_tid_multi_insert(Relation rel, nxtid *tids, int ntuples,
					  TransactionId xid, CommandId cid, uint32 speculative_token, RelUndoRecPtr prevundoptr)
{
	Buffer		buf;
	Page		page;
	NXBtreePageOpaque *opaque;
	OffsetNumber maxoff;
	nxtid		insert_target_key;
	List	   *newitems;
	nx_pending_undo_op *undo_op;
	nxtid		endtid;
	nxtid		tid;
	NXTidArrayItem *lastitem;
	bool		modified_orig;

	/*
	 * Insert to the rightmost leaf.
	 *
	 * TIDs are always monotonically increasing, so we must insert at the end
	 * of the TID tree.  The metapage cache provides a fast path to the
	 * rightmost leaf (see nxbt_descend), avoiding a full root-to-leaf descent
	 * in the common case.
	 *
	 * If the rightmost page has insufficient free space for even a minimal
	 * new item, the split path in nxbt_tid_add_items will handle page
	 * splitting and FPM allocation automatically.
	 */
	insert_target_key = MaxNXTid;
	buf = nxbt_descend(rel, NX_META_ATTRIBUTE_NUM, insert_target_key, 0, false, true, InvalidBuffer, InvalidBuffer);
	page = BufferGetPage(buf);
	opaque = NXBtreePageGetOpaque(page);
	maxoff = PageGetMaxOffsetNumber(page);

	/*
	 * Look at the last item, for its tid.
	 *
	 * assign TIDS for each item.
	 */
	if (maxoff >= FirstOffsetNumber)
	{
		ItemId		iid = PageGetItemId(page, maxoff);

		lastitem = (NXTidArrayItem *) PageGetItem(page, iid);

		endtid = lastitem->t_endtid;
	}
	else
	{
		endtid = opaque->nx_lokey;
		lastitem = NULL;
	}
	tid = endtid;

	/* Form an undo record using per-relation UNDO */
	if (xid != FrozenTransactionId)
	{
		RelUndoInsertPayload *ins_payload;

		undo_op = nx_relundo_create_op(rel, RELUNDO_INSERT, xid, cid,
									   prevundoptr,
									   sizeof(RelUndoInsertPayload));
		ins_payload = (RelUndoInsertPayload *) nx_relundo_get_payload(undo_op);
		ins_payload->firsttid = ItemPointerFromNXTid(tid);
		ins_payload->endtid = ItemPointerFromNXTid(tid + (nxtid) ntuples);
		ins_payload->speculative_token = speculative_token;
	}
	else
	{
		undo_op = NULL;
	}

	/*
	 * Create an item to represent all the TIDs, merging with the last
	 * existing item if possible.
	 */
	newitems = nxbt_tid_item_add_tids(lastitem, tid, ntuples, undo_op ? undo_op->reservation.undorecptr : InvalidRelUndoRecPtr,
									  &modified_orig);

	/*
	 * Replace the original last item with the new items, or add new items.
	 * This splits the page if necessary.
	 */
	if (modified_orig)
		nxbt_tid_replace_item(rel, buf, maxoff, newitems, undo_op);
	else
		nxbt_tid_add_items(rel, buf, newitems, undo_op);
	/* nxbt_tid_replace/add_item unlocked 'buf' */
	ReleaseBuffer(buf);

	list_free_deep(newitems);

	/* Return the TIDs to the caller */
	for (int i = 0; i < ntuples; i++)
		tids[i] = tid + (nxtid) i;
}

TM_Result
nxbt_tid_delete(Relation rel, nxtid tid,
				TransactionId xid, CommandId cid,
				Snapshot snapshot, Snapshot crosscheck, bool wait,
				TM_FailureData *hufd, bool changingPart, bool *this_xact_has_lock)
{
	RelUndoRecPtr recent_oldest_undo = nx_get_oldest_visible_undo_ptr(rel);
	RelUndoRecPtr item_undoptr;
	bool		item_isdead;
	TM_Result	result;
	bool		keep_old_undo_ptr = true;
	nx_pending_undo_op *undo_op;
	OffsetNumber off;
	NXTidArrayItem *origitem;
	Buffer		buf;
	Page		page;
	nxtid		next_tid;
	List	   *newitems = NIL;

	(void) wait;

	/* Find the item to delete. (It could be compressed) */
	off = nxbt_tid_fetch(rel, tid, &buf, &item_undoptr, &item_isdead);
	if (!OffsetNumberIsValid(off))
	{
		/*
		 * or should this be TM_Invisible? The heapam at least just throws an
		 * error, I think..
		 */
		elog(ERROR, "could not find tuple to delete with TID (%u, %u) in TID tree",
			 NXTidGetBlockNumber(tid), NXTidGetOffsetNumber(tid));
	}
	if (item_isdead)
	{
		elog(ERROR, "cannot delete tuple that is already marked DEAD (%u, %u)",
			 NXTidGetBlockNumber(tid), NXTidGetOffsetNumber(tid));
	}

	if (snapshot)
	{
		result = nx_SatisfiesUpdate(rel, snapshot, recent_oldest_undo,
									tid, item_undoptr, LockTupleExclusive,
									&keep_old_undo_ptr, this_xact_has_lock,
									hufd, &next_tid, NULL);
		if (result != TM_Ok)
		{
			UnlockReleaseBuffer(buf);
			/* nx_SatisfiesUpdate already populates hufd (xmax, cmax, ctid) */
			return result;
		}

		if (crosscheck != InvalidSnapshot && result == TM_Ok)
		{
			/*
			 * Perform additional check for transaction-snapshot mode RI
			 * updates
			 */
			NXTidTreeScan scan;
			TransactionId obsoleting_xid;
			NXUndoSlotVisibility visi_info;

			memset(&scan, 0, sizeof(scan));
			scan.rel = rel;
			scan.snapshot = crosscheck;
			scan.recent_oldest_undo = recent_oldest_undo;

			if (!nx_SatisfiesVisibility(&scan, item_undoptr, &obsoleting_xid, NULL, &visi_info))
			{
				UnlockReleaseBuffer(buf);
				/*
				 * The crosscheck snapshot couldn't see the tuple. Fill in
				 * TM_FailureData so callers can report the conflict.
				 */
				hufd->ctid = ItemPointerFromNXTid(tid);
				hufd->xmax = obsoleting_xid;
				hufd->cmax = InvalidCommandId;
				return TM_Updated;
			}
		}
	}

	/* Create UNDO record using per-relation UNDO. */
	{
		RelUndoDeletePayload *del_payload;

		undo_op = nx_relundo_create_op(rel, RELUNDO_DELETE, xid, cid,
									   keep_old_undo_ptr ? item_undoptr : InvalidRelUndoRecPtr,
									   sizeof(RelUndoDeletePayload));
		del_payload = (RelUndoDeletePayload *) nx_relundo_get_payload(undo_op);
		del_payload->ntids = 1;
		del_payload->changedPart = changingPart;
		del_payload->tids[0] = ItemPointerFromNXTid(tid);
	}

	/* Update the tid with the new UNDO pointer. */
	page = BufferGetPage(buf);
	origitem = (NXTidArrayItem *) PageGetItem(page, PageGetItemId(page, off));
	newitems = nxbt_tid_item_change_undoptr(origitem, tid, undo_op->reservation.undorecptr,
											recent_oldest_undo);
	nxbt_tid_replace_item(rel, buf, off, newitems, undo_op);
	list_free_deep(newitems);
	ReleaseBuffer(buf);			/* nxbt_tid_replace_item unlocked 'buf' */

	return TM_Ok;
}

void
nxbt_find_latest_tid(Relation rel, nxtid *tid, Snapshot snapshot)
{
	RelUndoRecPtr recent_oldest_undo = nx_get_oldest_visible_undo_ptr(rel);
	RelUndoRecPtr item_undoptr;
	bool		item_isdead;
	int			idx;
	Buffer		buf;

	/* Just using meta attribute, we can follow the update chain */
	nxtid		curr_tid = *tid;

	for (;;)
	{
		nxtid		next_tid = InvalidNXTid;

		if (curr_tid == InvalidNXTid)
			break;

		/* Find the item */
		idx = nxbt_tid_fetch(rel, curr_tid, &buf, &item_undoptr, &item_isdead);
		if (idx == -1 || item_isdead)
			break;

		if (snapshot)
		{
			NXTidTreeScan scan;
			TransactionId obsoleting_xid;
			NXUndoSlotVisibility visi_info;

			memset(&scan, 0, sizeof(scan));
			scan.rel = rel;
			scan.snapshot = snapshot;
			scan.recent_oldest_undo = recent_oldest_undo;

			if (nx_SatisfiesVisibility(&scan, item_undoptr,
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
nxbt_tid_update(Relation rel, nxtid otid,
				TransactionId xid, CommandId cid, bool key_update, Snapshot snapshot,
				Snapshot crosscheck, bool wait, TM_FailureData *hufd,
				nxtid *newtid_p, bool *this_xact_has_lock)
{
	TM_Result	result;
	RelUndoRecPtr prevundoptr;
	bool		success;
	int			retry_count = 0;

	/*
	 * This is currently only used on the meta-attribute. The other attributes
	 * don't need to carry visibility information, so the caller just inserts
	 * the new values with (multi_)insert() instead. This will change once we
	 * start doing the equivalent of HOT updates, where the TID doesn't
	 * change.
	 */
	Assert(*newtid_p == InvalidNXTid);

	/*
	 * Find and lock the old item.
	 *
	 * Ideally, if there is free TID space left on the same page we would keep
	 * the buffer locked and insert the new version there, avoiding a second
	 * B-tree descent.  That requires HOT-update-style support where the new
	 * TID can coexist on the old item's page while maintaining B-tree
	 * ordering.  For now, nxbt_tid_update_insert_new() always descends to the
	 * rightmost leaf (fast-pathed via the metapage cache) to allocate a
	 * monotonically increasing TID.
	 */
retry:
	if (++retry_count > 10)
		elog(ERROR, "nxbt_tid_update: infinite retry loop detected! retry_count=%d, otid=%lu", retry_count, otid);
	result = nxbt_tid_update_lock_old(rel, otid,
									  xid, cid, key_update, snapshot,
									  crosscheck, wait, hufd, this_xact_has_lock, &prevundoptr);

	if (result != TM_Ok)
		return result;

	/* insert new version */
	nxbt_tid_update_insert_new(rel, newtid_p, xid, cid, prevundoptr);

	/* update the old item with the "t_ctid pointer" for the new item */
	success = nxbt_tid_mark_old_updated(rel, otid, *newtid_p, xid, cid, key_update, prevundoptr);
	if (!success)
	{
		RelUndoRecPtr oldest_undoptr = nx_get_oldest_visible_undo_ptr(rel);

		nxbt_tid_mark_dead(rel, *newtid_p, oldest_undoptr);
		goto retry;
	}

	return TM_Ok;
}

/*
 * Like nxbt_tid_update, but creates a DELTA_INSERT UNDO record for
 * the new TID. Used for column-delta UPDATEs where only a subset
 * of columns are actually changed.
 */
TM_Result
nxbt_tid_delta_update(Relation rel, nxtid otid,
					  TransactionId xid, CommandId cid,
					  bool key_update, Snapshot snapshot,
					  Snapshot crosscheck, bool wait,
					  TM_FailureData *hufd,
					  nxtid *newtid_p,
					  bool *this_xact_has_lock,
					  int natts, const bool *changed_cols)
{
	TM_Result	result;
	RelUndoRecPtr prevundoptr;
	bool		success;

	Assert(*newtid_p == InvalidNXTid);

retry:
	{
		int delta_retry_count = 0;
		delta_retry_count++;
		if (delta_retry_count > 10)
			elog(ERROR, "nxbt_tid_delta_update: infinite retry loop detected! retry_count=%d, otid=%lu", delta_retry_count, otid);
	}
	result = nxbt_tid_update_lock_old(rel, otid,
									  xid, cid, key_update,
									  snapshot, crosscheck, wait,
									  hufd, this_xact_has_lock,
									  &prevundoptr);

	if (result != TM_Ok)
		return result;

	/* Insert new version with delta UNDO record */
	nxbt_tid_delta_insert(rel, newtid_p, xid, cid,
						  otid, natts, changed_cols,
						  prevundoptr);

	success = nxbt_tid_mark_old_updated(rel, otid, *newtid_p,
										xid, cid, key_update,
										prevundoptr);
	if (!success)
	{
		RelUndoRecPtr oldest = nx_get_oldest_visible_undo_ptr(rel);

		nxbt_tid_mark_dead(rel, *newtid_p, oldest);
		goto retry;
	}

	return TM_Ok;
}

/*
 * Subroutine of nxbt_update(): locks the old item for update.
 */
static TM_Result
nxbt_tid_update_lock_old(Relation rel, nxtid otid,
						 TransactionId xid, CommandId cid, bool key_update, Snapshot snapshot,
						 Snapshot crosscheck, bool wait, TM_FailureData *hufd, bool *this_xact_has_lock,
						 RelUndoRecPtr *prevundoptr_p)
{
	RelUndoRecPtr recent_oldest_undo;
	Buffer		buf;
	RelUndoRecPtr olditem_undoptr;
	bool		olditem_isdead;
	int			idx;
	TM_Result	result;
	bool		keep_old_undo_ptr = true;
	nxtid		next_tid;

	(void) wait;

	INJECTION_POINT("noxu_lock_updated_tuple", NULL);

	recent_oldest_undo = nx_get_oldest_visible_undo_ptr(rel);

	/*
	 * Find the item to delete.
	 */
	idx = nxbt_tid_fetch(rel, otid, &buf, &olditem_undoptr, &olditem_isdead);
	if (idx == -1 || olditem_isdead)
	{
		/*
		 * or should this be TM_Invisible? The heapam at least just throws an
		 * error, I think..
		 */
		elog(ERROR, "could not find old tuple to update with TID (%u, %u) in TID tree",
			 NXTidGetBlockNumber(otid), NXTidGetOffsetNumber(otid));
	}
	*prevundoptr_p = olditem_undoptr;

	/*
	 * Is it visible to us?
	 */
	result = nx_SatisfiesUpdate(rel, snapshot, recent_oldest_undo,
								otid, olditem_undoptr,
								key_update ? LockTupleExclusive : LockTupleNoKeyExclusive,
								&keep_old_undo_ptr, this_xact_has_lock,
								hufd, &next_tid, NULL);
	if (result != TM_Ok)
	{
		UnlockReleaseBuffer(buf);
		/* nx_SatisfiesUpdate already populates hufd (xmax, cmax, ctid) */
		return result;
	}

	if (crosscheck != InvalidSnapshot && result == TM_Ok)
	{
		/* Perform additional check for transaction-snapshot mode RI updates */
		NXTidTreeScan scan;
		TransactionId obsoleting_xid;
		NXUndoSlotVisibility visi_info;

		memset(&scan, 0, sizeof(scan));
		scan.rel = rel;
		scan.snapshot = crosscheck;
		scan.recent_oldest_undo = recent_oldest_undo;

		if (!nx_SatisfiesVisibility(&scan, olditem_undoptr, &obsoleting_xid, NULL, &visi_info))
		{
			UnlockReleaseBuffer(buf);
			/*
			 * The crosscheck snapshot couldn't see the tuple. Fill in
			 * TM_FailureData so callers can report the conflict.
			 */
			hufd->ctid = ItemPointerFromNXTid(otid);
			hufd->xmax = obsoleting_xid;
			hufd->cmax = InvalidCommandId;
			result = TM_Updated;
		}
	}

	/*
	 * Place a tuple lock on the old item to prevent concurrent modifications
	 * between now and when we mark it as updated. This creates a TUPLE_LOCK
	 * UNDO record that other transactions will see via nx_SatisfiesUpdate(),
	 * causing them to wait or return TM_BeingModified.
	 *
	 * If we already have the lock from a previous retry, skip this step to
	 * avoid creating duplicate TUPLE_LOCK records that would cause the
	 * prevundoptr check in nxbt_tid_mark_old_updated to fail infinitely.
	 */
	if (!*this_xact_has_lock)
	{
		nx_pending_undo_op *lock_undo_op;
		RelUndoRecPtr lock_undorecptr;
		Page		lock_page;
		NXTidArrayItem *lock_origitem;
		List	   *lock_newitems;

		{
			RelUndoTupleLockPayload *lock_payload;

			lock_undo_op = nx_relundo_create_op(rel, RELUNDO_TUPLE_LOCK, xid, cid,
												keep_old_undo_ptr ? olditem_undoptr : InvalidRelUndoRecPtr,
												sizeof(RelUndoTupleLockPayload));
			lock_payload = (RelUndoTupleLockPayload *) nx_relundo_get_payload(lock_undo_op);
			lock_payload->tid = ItemPointerFromNXTid(otid);
			lock_payload->lock_mode = (uint16) (key_update ? LockTupleExclusive : LockTupleNoKeyExclusive);
		}

		/*
		 * Save the undorecptr before nxbt_tid_replace_item frees the
		 * undo_op structure.
		 */
		lock_undorecptr = lock_undo_op->reservation.undorecptr;

		/* Replace the item with updated undo pointer reflecting the lock. */
		lock_page = BufferGetPage(buf);
		Assert(idx >= 0 && idx <= MaxOffsetNumber);
		lock_origitem = (NXTidArrayItem *) PageGetItem(lock_page,
													   PageGetItemId(lock_page, (OffsetNumber) idx));
		lock_newitems = nxbt_tid_item_change_undoptr(lock_origitem, otid,
													 lock_undorecptr,
													 recent_oldest_undo);
		nxbt_tid_replace_item(rel, buf, (OffsetNumber) idx, lock_newitems, lock_undo_op);
		list_free_deep(lock_newitems);

		/* Update the prevundoptr to point to our lock record */
		*prevundoptr_p = lock_undorecptr;

		/* Mark that we now have the lock for future retries */
		*this_xact_has_lock = true;
	}
	else
	{
		/*
		 * We already have the lock from a previous retry. The old tuple's
		 * undo pointer should still be pointing to our TUPLE_LOCK record.
		 * Use the current undo pointer as prevundoptr.
		 */
		*prevundoptr_p = olditem_undoptr;

		/* Unlock the buffer since nxbt_tid_replace_item won't be called */
		LockBuffer(buf, BUFFER_LOCK_UNLOCK);
	}

	ReleaseBuffer(buf);			/* buffer was unlocked above */

	return TM_Ok;
}

/*
 * Subroutine of nxbt_update(): inserts the new, updated, item.
 */
static void
nxbt_tid_update_insert_new(Relation rel,
						   nxtid *newtid,
						   TransactionId xid, CommandId cid, RelUndoRecPtr prevundoptr)
{
	nxbt_tid_multi_insert(rel, newtid, 1, xid, cid, INVALID_SPECULATIVE_TOKEN, prevundoptr);
}

/*
 * Like nxbt_tid_multi_insert, but creates a DELTA_INSERT UNDO record
 * that tracks which columns were changed and the predecessor TID.
 * Used for column-delta UPDATEs.
 */
void
nxbt_tid_delta_insert(Relation rel, nxtid *tids,
					  TransactionId xid, CommandId cid,
					  nxtid predecessor_tid,
					  int natts, const bool *changed_cols,
					  RelUndoRecPtr prevundoptr)
{
	Buffer		buf;
	Page		page;
	NXBtreePageOpaque *opaque;
	OffsetNumber maxoff;
	nxtid		insert_target_key;
	List	   *newitems;
	nx_pending_undo_op *undo_op;
	nxtid		endtid;
	nxtid		tid;
	NXTidArrayItem *lastitem;
	bool		modified_orig;

	insert_target_key = MaxNXTid;
	buf = nxbt_descend(rel, NX_META_ATTRIBUTE_NUM,
					   insert_target_key, 0, false, true,
					   InvalidBuffer, InvalidBuffer);
	page = BufferGetPage(buf);
	opaque = NXBtreePageGetOpaque(page);
	maxoff = PageGetMaxOffsetNumber(page);

	if (maxoff >= FirstOffsetNumber)
	{
		ItemId		iid = PageGetItemId(page, maxoff);

		lastitem = (NXTidArrayItem *)
			PageGetItem(page, iid);
		endtid = lastitem->t_endtid;
	}
	else
	{
		endtid = opaque->nx_lokey;
		lastitem = NULL;
	}
	tid = endtid;

	{
		NXRelUndoDeltaInsertPayload *di_payload;
		Size		di_payload_size;
		int			nwords;
		int			nchanged;

		di_payload_size = SizeOfNXRelUndoDeltaInsertPayload(natts);
		undo_op = nx_relundo_create_op(rel, RELUNDO_DELTA_INSERT, xid, cid,
									   prevundoptr, di_payload_size);
		di_payload = (NXRelUndoDeltaInsertPayload *) nx_relundo_get_payload(undo_op);
		di_payload->firsttid = ItemPointerFromNXTid(tid);
		di_payload->endtid = ItemPointerFromNXTid(tid + 1);
		di_payload->speculative_token = INVALID_SPECULATIVE_TOKEN;
		di_payload->predecessor_tid = predecessor_tid;
		Assert(natts >= 0 && natts <= PG_INT16_MAX);
		di_payload->natts = (int16) natts;

		/* Build the changed columns bitmap */
		nwords = NXUNDO_DELTA_BITMAP_WORDS(natts);
		memset(di_payload->changed_cols, 0, (size_t) nwords * sizeof(uint32));
		nchanged = 0;
		for (int attno = 1; attno <= natts; attno++)
		{
			if (changed_cols[attno - 1])
			{
				int idx = (attno - 1) / 32;
				int bit = (attno - 1) % 32;
				di_payload->changed_cols[idx] |= (1U << bit);
				nchanged++;
			}
		}
		di_payload->nchanged = (int16) nchanged;
	}

	newitems = nxbt_tid_item_add_tids(
		lastitem, tid, 1,
		undo_op->reservation.undorecptr,
		&modified_orig);

	if (modified_orig)
		nxbt_tid_replace_item(rel, buf, maxoff,
							  newitems, undo_op);
	else
		nxbt_tid_add_items(rel, buf, newitems, undo_op);
	ReleaseBuffer(buf);

	list_free_deep(newitems);
	tids[0] = tid;
}

/*
 * Subroutine of nxbt_update(): mark old item as updated.
 */
static bool
nxbt_tid_mark_old_updated(Relation rel, nxtid otid, nxtid newtid,
						  TransactionId xid, CommandId cid, bool key_update, RelUndoRecPtr prevrecptr)
{
	RelUndoRecPtr recent_oldest_undo = nx_get_oldest_visible_undo_ptr(rel);
	Buffer		buf;
	Page		page;
	RelUndoRecPtr olditem_undoptr;
	bool		olditem_isdead;
	OffsetNumber off;
	bool		keep_old_undo_ptr = true;
	nx_pending_undo_op *undo_op;
	List	   *newitems;
	NXTidArrayItem *origitem;

	/*
	 * Find the item to delete.  It could be part of a compressed item, we let
	 * nxbt_fetch() handle that.
	 */
	off = nxbt_tid_fetch(rel, otid, &buf, &olditem_undoptr, &olditem_isdead);
	if (!OffsetNumberIsValid(off) || olditem_isdead)
	{
		/*
		 * or should this be TM_Invisible? The heapam at least just throws an
		 * error, I think..
		 */
		elog(ERROR, "could not find old tuple to update with TID (%u, %u) in TID tree",
			 NXTidGetBlockNumber(otid), NXTidGetOffsetNumber(otid));
	}

	/*
	 * Did it change while we were inserting new row version?
	 */
	if (olditem_undoptr != prevrecptr)
	{
		elog(DEBUG1, "nxbt_tid_mark_old_updated: undo pointer mismatch, "
			 "olditem_undoptr=%lu, prevrecptr=%lu, otid=%lu",
			 olditem_undoptr, prevrecptr, otid);
		UnlockReleaseBuffer(buf);
		return false;
	}

	/* Prepare an UNDO record using per-relation UNDO. */
	{
		RelUndoUpdatePayload *upd_payload;

		undo_op = nx_relundo_create_op(rel, RELUNDO_UPDATE, xid, cid,
									   keep_old_undo_ptr ? olditem_undoptr : InvalidRelUndoRecPtr,
									   sizeof(RelUndoUpdatePayload));
		upd_payload = (RelUndoUpdatePayload *) nx_relundo_get_payload(undo_op);
		upd_payload->oldtid = ItemPointerFromNXTid(otid);
		upd_payload->newtid = ItemPointerFromNXTid(newtid);
		upd_payload->key_update = key_update;
	}

	/* Replace the NXTidArrayItem with one with the updated undo pointer. */
	page = BufferGetPage(buf);
	origitem = (NXTidArrayItem *) PageGetItem(page, PageGetItemId(page, off));
	newitems = nxbt_tid_item_change_undoptr(origitem, otid, undo_op->reservation.undorecptr,
											recent_oldest_undo);
	nxbt_tid_replace_item(rel, buf, off, newitems, undo_op);
	list_free_deep(newitems);
	ReleaseBuffer(buf);			/* nxbt_tid_replace_item unlocked 'buf' */

	return true;
}

/*
 * Mark a tuple as updated during CLUSTER/VACUUM FULL.
 *
 * Like nxbt_tid_mark_old_updated, but skips the prevrecptr consistency check
 * since we have exclusive access during CLUSTER. Creates an UPDATE undo
 * record on the old TID pointing to newtid, preserving UPDATE chains.
 */
void
nxbt_tid_mark_updated_for_cluster(Relation rel, nxtid otid, nxtid newtid,
								  TransactionId xid, CommandId cid,
								  bool key_update)
{
	RelUndoRecPtr recent_oldest_undo = nx_get_oldest_visible_undo_ptr(rel);
	Buffer		buf;
	Page		page;
	RelUndoRecPtr olditem_undoptr;
	bool		olditem_isdead;
	OffsetNumber off;
	nx_pending_undo_op *undo_op;
	List	   *newitems;
	NXTidArrayItem *origitem;

	off = nxbt_tid_fetch(rel, otid, &buf, &olditem_undoptr, &olditem_isdead);
	if (!OffsetNumberIsValid(off) || olditem_isdead)
		elog(ERROR, "could not find tuple to mark as updated during CLUSTER");

	{
		RelUndoUpdatePayload *upd_payload;

		undo_op = nx_relundo_create_op(rel, RELUNDO_UPDATE, xid, cid,
									   olditem_undoptr,
									   sizeof(RelUndoUpdatePayload));
		upd_payload = (RelUndoUpdatePayload *) nx_relundo_get_payload(undo_op);
		upd_payload->oldtid = ItemPointerFromNXTid(otid);
		upd_payload->newtid = ItemPointerFromNXTid(newtid);
		upd_payload->key_update = key_update;
	}

	page = BufferGetPage(buf);
	origitem = (NXTidArrayItem *) PageGetItem(page, PageGetItemId(page, off));
	newitems = nxbt_tid_item_change_undoptr(origitem, otid,
											undo_op->reservation.undorecptr,
											recent_oldest_undo);
	nxbt_tid_replace_item(rel, buf, off, newitems, undo_op);
	list_free_deep(newitems);
	ReleaseBuffer(buf);
}

TM_Result
nxbt_tid_lock(Relation rel, nxtid tid, TransactionId xid, CommandId cid,
			  LockTupleMode mode, bool follow_updates, Snapshot snapshot,
			  TM_FailureData *hufd, nxtid *next_tid, bool *this_xact_has_lock,
			  NXUndoSlotVisibility *visi_info)
{
	RelUndoRecPtr recent_oldest_undo = nx_get_oldest_visible_undo_ptr(rel);
	Buffer		buf;
	Page		page;
	RelUndoRecPtr item_undoptr;
	bool		item_isdead;
	OffsetNumber off;
	TM_Result	result;
	bool		keep_old_undo_ptr = true;
	nx_pending_undo_op *undo_op;
	List	   *newitems;
	NXTidArrayItem *origitem;

	*next_tid = tid;

	off = nxbt_tid_fetch(rel, tid, &buf, &item_undoptr, &item_isdead);
	if (!OffsetNumberIsValid(off) || item_isdead)
	{
		/*
		 * or should this be TM_Invisible? The heapam at least just throws an
		 * error, I think..
		 */
		elog(ERROR, "could not find tuple to lock with TID (%u, %u)",
			 NXTidGetBlockNumber(tid), NXTidGetOffsetNumber(tid));
	}
	result = nx_SatisfiesUpdate(rel, snapshot, recent_oldest_undo,
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

	/* Create UNDO record using per-relation UNDO. */
	{
		RelUndoTupleLockPayload *lock_payload;

		undo_op = nx_relundo_create_op(rel, RELUNDO_TUPLE_LOCK, xid, cid,
									   keep_old_undo_ptr ? item_undoptr : InvalidRelUndoRecPtr,
									   sizeof(RelUndoTupleLockPayload));
		lock_payload = (RelUndoTupleLockPayload *) nx_relundo_get_payload(undo_op);
		lock_payload->tid = ItemPointerFromNXTid(tid);
		lock_payload->lock_mode = (uint16) mode;
	}

	/* Replace the item with an identical one, but with updated undo pointer. */
	page = BufferGetPage(buf);
	origitem = (NXTidArrayItem *) PageGetItem(page, PageGetItemId(page, off));
	newitems = nxbt_tid_item_change_undoptr(origitem, tid, undo_op->reservation.undorecptr,
											recent_oldest_undo);
	nxbt_tid_replace_item(rel, buf, off, newitems, undo_op);
	list_free_deep(newitems);
	ReleaseBuffer(buf);			/* nxbt_tid_replace_item unlocked 'buf' */
	return TM_Ok;
}

/*
 * Collect all TIDs marked as dead in the TID tree.
 *
 * This is used during VACUUM.
 */
IntegerSet *
nxbt_collect_dead_tids(Relation rel, nxtid starttid, nxtid *endtid, uint64 *num_live_tuples)
{
	Buffer		buf = InvalidBuffer;
	IntegerSet *result;
	NXBtreePageOpaque *opaque;
	nxtid		nexttid;
	BlockNumber nextblock;
	NXTidItemIterator iter;

	memset(&iter, 0, sizeof(NXTidItemIterator));
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

			if (!nxbt_page_is_expected(rel, NX_META_ATTRIBUTE_NUM, nexttid, 0, buf))
			{
				UnlockReleaseBuffer(buf);
				buf = InvalidBuffer;
			}
		}

		if (!BufferIsValid(buf))
		{
			buf = nxbt_descend(rel, NX_META_ATTRIBUTE_NUM, nexttid, 0, true, false, InvalidBuffer, InvalidBuffer);
			if (!BufferIsValid(buf))
				return result;
			page = BufferGetPage(buf);
		}

		maxoff = PageGetMaxOffsetNumber(page);
		for (off = FirstOffsetNumber; off <= maxoff; off++)
		{
			ItemId		iid = PageGetItemId(page, off);
			NXTidArrayItem *item = (NXTidArrayItem *) PageGetItem(page, iid);

			nxbt_tid_item_unpack(item, &iter);

			for (int j = 0; j < iter.num_tids; j++)
			{
				(*num_live_tuples)++;
				if (iter.tid_undoslotnos[j] == NXBT_DEAD_UNDO_SLOT)
					intset_add_member(result, iter.tids[j]);
			}
		}

		opaque = NXBtreePageGetOpaque(page);
		nexttid = opaque->nx_hikey;
		nextblock = opaque->nx_next;

		LockBuffer(buf, BUFFER_LOCK_UNLOCK);

		if (nexttid == MaxPlusOneNXTid)
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
nxbt_tid_mark_dead(Relation rel, nxtid tid, RelUndoRecPtr recent_oldest_undo)
{
	Buffer		buf;
	Page		page;
	RelUndoRecPtr item_undoptr;
	OffsetNumber off;
	NXTidArrayItem *origitem;
	List	   *newitems;
	bool		isdead;

	/* Find the item to delete. (It could be compressed) */
	off = nxbt_tid_fetch(rel, tid, &buf, &item_undoptr, &isdead);
	if (!OffsetNumberIsValid(off))
	{
		elog(WARNING, "could not find tuple to mark dead with TID (%u, %u)",
			 NXTidGetBlockNumber(tid), NXTidGetOffsetNumber(tid));
		return;
	}

	/* Mark the TID as DEAD. (Unless it's already dead) */
	if (isdead)
	{
		UnlockReleaseBuffer(buf);
		return;
	}

	page = BufferGetPage(buf);
	origitem = (NXTidArrayItem *) PageGetItem(page, PageGetItemId(page, off));
	newitems = nxbt_tid_item_change_undoptr(origitem, tid, DeadRelUndoRecPtr,
											recent_oldest_undo);
	nxbt_tid_replace_item(rel, buf, off, newitems, NULL);
	list_free_deep(newitems);
	ReleaseBuffer(buf);			/* nxbt_tid_replace_item unlocked 'buf' */
}


/*
 * Remove items for the given TIDs from the TID tree.
 *
 * This is used during VACUUM.
 */
void
nxbt_tid_remove(Relation rel, IntegerSet *tids)
{
	RelUndoRecPtr recent_oldest_undo = nx_get_oldest_visible_undo_ptr(rel);
	nxtid		nexttid;
	MemoryContext oldcontext;
	MemoryContext tmpcontext;

	tmpcontext = AllocSetContextCreate(CurrentMemoryContext,
									   "NoxuAMVacuumContext",
									   ALLOCSET_DEFAULT_SIZES);
	oldcontext = MemoryContextSwitchTo(tmpcontext);

	intset_begin_iterate(tids);
	if (!intset_iterate_next(tids, &nexttid))
		nexttid = MaxPlusOneNXTid;

	while (nexttid < MaxPlusOneNXTid)
	{
		Buffer		buf;
		Page		page;
		NXBtreePageOpaque *opaque;
		List	   *newitems;
		OffsetNumber maxoff;
		OffsetNumber off;

		/*
		 * Find the leaf page containing the next item to remove
		 */
		buf = nxbt_descend(rel, NX_META_ATTRIBUTE_NUM, nexttid, 0, false, true, InvalidBuffer, InvalidBuffer);
		page = BufferGetPage(buf);
		opaque = NXBtreePageGetOpaque(page);

		/*
		 * Rewrite the items on the page, removing all TIDs that need to be
		 * removed from the page.
		 */
		newitems = NIL;
		maxoff = PageGetMaxOffsetNumber(page);
		for (off = FirstOffsetNumber; off <= maxoff; off++)
		{
			ItemId		iid = PageGetItemId(page, off);
			NXTidArrayItem *item = (NXTidArrayItem *) PageGetItem(page, iid);

			while (nexttid < item->t_firsttid)
			{
				if (!intset_iterate_next(tids, &nexttid))
					nexttid = MaxPlusOneNXTid;
			}

			if (nexttid < item->t_endtid)
			{
				List	   *newitemsx = nxbt_tid_item_remove_tids(item, &nexttid, tids,
																  recent_oldest_undo);

				newitems = list_concat(newitems, newitemsx);
			}
			else
			{
				/* keep this item unmodified */
				newitems = lappend(newitems, item);
			}
		}

		while (nexttid < opaque->nx_hikey)
		{
			if (!intset_iterate_next(tids, &nexttid))
				nexttid = MaxPlusOneNXTid;
		}

		/* Pass the list to the recompressor. */
		IncrBufferRefCount(buf);
		if (newitems)
		{
			nxbt_tid_recompress_replace(rel, buf, newitems, NULL);
		}
		else
		{
			nx_split_stack *stack;

			stack = nxbt_unlink_page(rel, NX_META_ATTRIBUTE_NUM, buf, 0);

			if (!stack)
			{
				/* failed. */
				Page		newpage = PageGetTempPageCopySpecial(BufferGetPage(buf));

				stack = nx_new_split_stack_entry(buf, newpage);
			}

			/* apply the changes */
			nx_apply_split_changes(rel, stack, NULL);
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
nxbt_tid_undo_deletion(Relation rel, nxtid tid, RelUndoRecPtr undoptr,
					   RelUndoRecPtr recent_oldest_undo)
{
	Buffer		buf;
	Page		page;
	RelUndoRecPtr item_undoptr;
	bool		item_isdead;
	OffsetNumber off;

	/* Find the item to delete. (It could be compressed) */
	off = nxbt_tid_fetch(rel, tid, &buf, &item_undoptr, &item_isdead);
	if (!OffsetNumberIsValid(off))
	{
		elog(WARNING, "could not find aborted tuple to remove with TID (%u, %u)",
			 NXTidGetBlockNumber(tid), NXTidGetOffsetNumber(tid));
		return;
	}

	if (item_undoptr == undoptr)
	{
		NXTidArrayItem *origitem;
		List	   *newitems;
		RelUndoRecPtr restored_undoptr;

		/*
		 * Restore the predecessor undo pointer from the UNDO record being
		 * undone. We must not blindly set InvalidRelUndoRecPtr here, because
		 * the tuple may have older UNDO records (e.g. from a prior committed
		 * insert) that are still needed for MVCC visibility checks. Setting
		 * InvalidRelUndoRecPtr would make the tuple unconditionally visible
		 * to all snapshots, which is incorrect if those older records haven't
		 * yet aged out past the oldest visible undo pointer.
		 *
		 * By reading the UNDO record and extracting urec_prevundorec, we
		 * effectively "pop" the aborted deletion's record off the chain and
		 * restore the tuple's undo pointer to whatever it was before the
		 * deletion was attempted.
		 */
		if (RelUndoRecPtrIsValid(undoptr))
		{
			RelUndoRecordHeader undo_header;
			void	   *payload;
			Size		payload_size;

			if (RelUndoReadRecord(rel, undoptr, &undo_header, &payload, &payload_size))
			{
				restored_undoptr = undo_header.urec_prevundorec;
				if (payload)
					pfree(payload);
			}
			else
			{
				/*
				 * The UNDO record has been discarded (trimmed). This means
				 * it's old enough that all active snapshots can see past it,
				 * so InvalidRelUndoRecPtr is safe here.
				 */
				restored_undoptr = InvalidRelUndoRecPtr;
			}
		}
		else
		{
			restored_undoptr = InvalidRelUndoRecPtr;
		}

		page = BufferGetPage(buf);
		origitem = (NXTidArrayItem *) PageGetItem(page, PageGetItemId(page, off));
		newitems = nxbt_tid_item_change_undoptr(origitem, tid, restored_undoptr,
												recent_oldest_undo);
		nxbt_tid_replace_item(rel, buf, off, newitems, NULL);
		list_free_deep(newitems);
		ReleaseBuffer(buf);		/* nxbt_tid_replace_item unlocked 'buf' */
	}
	else
	{
		Assert(item_isdead ||
			   RelUndoGetCounter(item_undoptr) > RelUndoGetCounter(undoptr) ||
			   !RelUndoRecPtrIsValid(item_undoptr));
		UnlockReleaseBuffer(buf);
	}
}

/* ----------------------------------------------------------------
 *						 Internal routines
 * ----------------------------------------------------------------
 */

void
nxbt_tid_clear_speculative_token(Relation rel, nxtid tid, uint32 spectoken, bool forcomplete)
{
	Buffer		buf;
	RelUndoRecPtr item_undoptr;
	bool		item_isdead;
	bool		found;

	(void) spectoken;
	(void) forcomplete;

	found = nxbt_tid_fetch(rel, tid, &buf, &item_undoptr, &item_isdead);
	if (!found || item_isdead)
		elog(ERROR, "couldn't find item for meta column for inserted tuple with TID (%u, %u) in rel %s",
			 NXTidGetBlockNumber(tid), NXTidGetOffsetNumber(tid), rel->rd_rel->relname.data);

	nxundo_clear_speculative_token(rel, item_undoptr);

	UnlockReleaseBuffer(buf);
}

/*
 * Fetch the item with given TID. The page containing the item is kept locked, and
 * returned to the caller in *buf_p. This is used to locate a tuple for updating
 * or deleting it.
 */
static OffsetNumber
nxbt_tid_fetch(Relation rel, nxtid tid, Buffer *buf_p, RelUndoRecPtr *undoptr_p, bool *isdead_p)
{
	Buffer		buf;
	Page		page;
	OffsetNumber maxoff;
	OffsetNumber off;

	buf = nxbt_descend(rel, NX_META_ATTRIBUTE_NUM, tid, 0, false, true, InvalidBuffer, InvalidBuffer);
	if (buf == InvalidBuffer)
	{
		*buf_p = InvalidBuffer;
		*undoptr_p = InvalidRelUndoRecPtr;
		return InvalidOffsetNumber;
	}
	page = BufferGetPage(buf);
	maxoff = PageGetMaxOffsetNumber(page);

	/* Find the item on the page that covers the target TID */
	off = nxbt_binsrch_tidpage(tid, page);
	if (off >= FirstOffsetNumber && off <= maxoff)
	{
		ItemId		iid = PageGetItemId(page, off);
		NXTidArrayItem *item = (NXTidArrayItem *) PageGetItem(page, iid);

		if (tid < item->t_endtid)
		{
			NXTidItemIterator iter;

			memset(&iter, 0, sizeof(NXTidItemIterator));
			iter.context = CurrentMemoryContext;

			nxbt_tid_item_unpack(item, &iter);

			/*
			 * Binary search for the target TID in the unpacked array.
			 * The TIDs are sorted (decoded from delta-coded codewords).
			 */
			{
				int			lo = 0;
				int			hi = iter.num_tids;

				while (hi > lo)
				{
					int			mid = lo + (hi - lo) / 2;

					if (tid > iter.tids[mid])
						lo = mid + 1;
					else
						hi = mid;
				}

				if (lo < iter.num_tids && iter.tids[lo] == tid)
				{
					int			slotno = iter.tid_undoslotnos[lo];
					RelUndoRecPtr undoptr = iter.undoslots[slotno];

					*isdead_p = (slotno == NXBT_DEAD_UNDO_SLOT);
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
 * The items in 'newitems' are added to the end of the page. This is correct
 * because TIDs are allocated monotonically, so new items always have TIDs
 * greater than all existing items on the page.
 *
 * This function handles splitting the page if needed.
 */
static void
nxbt_tid_add_items(Relation rel, Buffer buf, List *newitems, nx_pending_undo_op * undo_op)
{
	Page		page = BufferGetPage(buf);
	OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
	OffsetNumber off;
	Size		newitemsize;
	ListCell   *lc;

	newitemsize = 0;
	foreach(lc, newitems)
	{
		NXTidArrayItem *item = (NXTidArrayItem *) lfirst(lc);

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
			NXTidArrayItem *item = (NXTidArrayItem *) lfirst(lc);

			if (!PageAddItem(page, item, item->t_size, off, true, false))
				elog(ERROR, "could not add item to TID tree page");
			off++;
		}

		if (undo_op)
			nx_relundo_write_record(undo_op);

		MarkBufferDirty(buf);

		if (RelationNeedsWAL(rel))
			nxbt_wal_log_leaf_items(rel, NX_META_ATTRIBUTE_NUM, buf,
									startoff, false, newitems,
									undo_op);
		else
		{
			/*
			 * For unlogged relations, we still need to update the page LSN
			 * to ensure proper page consistency checks.
			 */
			PageSetLSN(BufferGetPage(buf), GetXLogInsertRecPtr());
			if (undo_op)
				PageSetLSN(BufferGetPage(undo_op->reservation.undobuf), GetXLogInsertRecPtr());
		}

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
			NXTidArrayItem *item = (NXTidArrayItem *) PageGetItem(page, iid);

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
			nxbt_tid_recompress_replace(rel, buf, items, undo_op);
		}
		else
		{
			nx_split_stack *stack;

			stack = nxbt_unlink_page(rel, NX_META_ATTRIBUTE_NUM, buf, 0);

			if (!stack)
			{
				/* failed. */
				Page		newpage = PageGetTempPageCopySpecial(BufferGetPage(buf));

				stack = nx_new_split_stack_entry(buf, newpage);
			}

			/* apply the changes */
			nx_apply_split_changes(rel, stack, undo_op);
		}

		list_free(items);
	}
}


/*
 * This helper function is used to implement INSERT, UPDATE and DELETE.
 *
 * If 'newitems' is not empty, the items in the list are added to the end
 * of the page. This is correct because TIDs are monotonically allocated,
 * so new items always belong after existing items.
 *
 * This function handles decompressing and recompressing items, and splitting
 * the page if needed.
 */
static void
nxbt_tid_replace_item(Relation rel, Buffer buf, OffsetNumber targetoff, List *newitems,
					  nx_pending_undo_op * undo_op)
{
	Page		page = BufferGetPage(buf);
	ItemId		iid;
	NXTidArrayItem *olditem;
	ListCell   *lc;
	ssize_t		sizediff;

	/*
	 * Find the item that covers the given tid.
	 */
	if (targetoff < FirstOffsetNumber || targetoff > PageGetMaxOffsetNumber(page))
		elog(ERROR, "could not find item at off %d to replace", targetoff);
	iid = PageGetItemId(page, targetoff);
	olditem = (NXTidArrayItem *) PageGetItem(page, iid);

	/* Calculate how much free space we'll need */
	sizediff = -(ssize_t) (olditem->t_size + sizeof(ItemIdData));
	foreach(lc, newitems)
	{
		NXTidArrayItem *newitem = (NXTidArrayItem *) lfirst(lc);

		sizediff += (ssize_t) (newitem->t_size + sizeof(ItemIdData));
	}

	/* Can we fit them? */
	if (sizediff <= (ssize_t) PageGetExactFreeSpace(page))
	{
		NXTidArrayItem *newitem;
		OffsetNumber off;

		START_CRIT_SECTION();

		/* Remove existing item, and add new ones */
		if (newitems == 0)
			PageIndexTupleDelete(page, targetoff);
		else
		{
			lc = list_head(newitems);
			newitem = (NXTidArrayItem *) lfirst(lc);
			if (!PageIndexTupleOverwrite(page, targetoff, newitem, newitem->t_size))
				elog(ERROR, "could not replace item in TID tree page at off %d", targetoff);
			lc = lnext(newitems, lc);

			off = targetoff + 1;
			for (; lc != NULL; lc = lnext(newitems, lc))
			{
				newitem = (NXTidArrayItem *) lfirst(lc);
				if (!PageAddItem(page, newitem, newitem->t_size, off, false, false))
					elog(ERROR, "could not add item in TID tree page at off %d", off);
				off++;
			}
		}
		MarkBufferDirty(buf);

		if (undo_op)
			nx_relundo_write_record(undo_op);

		if (RelationNeedsWAL(rel))
			nxbt_wal_log_leaf_items(rel, NX_META_ATTRIBUTE_NUM, buf, targetoff, true, newitems, undo_op);
		else
		{
			/*
			 * For unlogged relations, we still need to update the page LSN
			 * to ensure proper page consistency checks.
			 */
			PageSetLSN(BufferGetPage(buf), GetXLogInsertRecPtr());
			if (undo_op)
				PageSetLSN(BufferGetPage(undo_op->reservation.undobuf), GetXLogInsertRecPtr());
		}
		END_CRIT_SECTION();

#ifdef USE_ASSERT_CHECKING
		{
			nxtid		lasttid = 0;
			NXTidArrayItem *item;

			for (off = FirstOffsetNumber; off <= PageGetMaxOffsetNumber(page); off++)
			{
				iid = PageGetItemId(page, off);
				item = (NXTidArrayItem *) PageGetItem(page, iid);

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
		NXTidArrayItem *item;

		/*
		 * Construct a List that contains all the items in the right order,
		 * and let nxbt_tid_recompress_page() do the heavy lifting to fit them
		 * on pages.
		 */
		for (off = FirstOffsetNumber; off <= maxoff; off++)
		{
			iid = PageGetItemId(page, off);
			item = (NXTidArrayItem *) PageGetItem(page, iid);

			if (off == targetoff)
			{
				foreach(lc, newitems)
				{
					items = lappend(items, (NXTidArrayItem *) lfirst(lc));
				}
			}
			else
				items = lappend(items, item);
		}

#ifdef USE_ASSERT_CHECKING
		{
			nxtid		endtid = 0;

			foreach(lc, items)
			{
				NXTidArrayItem *i = (NXTidArrayItem *) lfirst(lc);

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
			nxbt_tid_recompress_replace(rel, buf, items, undo_op);
		}
		else
		{
			nx_split_stack *stack;

			stack = nxbt_unlink_page(rel, NX_META_ATTRIBUTE_NUM, buf, 0);

			if (!stack)
			{
				/* failed. */
				Page		newpage = PageGetTempPageCopySpecial(BufferGetPage(buf));

				stack = nx_new_split_stack_entry(buf, newpage);
			}

			/* apply the changes */
			nx_apply_split_changes(rel, stack, undo_op);
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
	nx_split_stack *stack_head;
	nx_split_stack *stack_tail;

	int			num_pages;
	int			free_space_per_page;

	nxtid		hikey;
}			nxbt_tid_recompress_context;

static void
nxbt_tid_recompress_newpage(nxbt_tid_recompress_context * cxt, nxtid nexttid, int flags)
{
	Page		newpage;
	NXBtreePageOpaque *newopaque;
	nx_split_stack *stack;

	if (cxt->currpage)
	{
		/* set the last tid on previous page */
		NXBtreePageOpaque *oldopaque = NXBtreePageGetOpaque(cxt->currpage);

		oldopaque->nx_hikey = nexttid;
	}

	newpage = (Page) palloc(BLCKSZ);
	PageInit(newpage, BLCKSZ, sizeof(NXBtreePageOpaque));

	stack = nx_new_split_stack_entry(InvalidBuffer, /* will be assigned later */
									 newpage);
	if (cxt->stack_tail)
		cxt->stack_tail->next = stack;
	else
		cxt->stack_head = stack;
	cxt->stack_tail = stack;

	cxt->currpage = newpage;

	newopaque = NXBtreePageGetOpaque(newpage);
	newopaque->nx_attno = NX_META_ATTRIBUTE_NUM;
	newopaque->nx_next = InvalidBlockNumber;	/* filled in later */
	newopaque->nx_lokey = nexttid;
	newopaque->nx_hikey = cxt->hikey;	/* overwritten later, if this is not
										 * last page */
	newopaque->nx_level = 0;
	newopaque->nx_flags = (uint16) flags;
	newopaque->nx_page_id = NX_BTREE_PAGE_ID;
}

static void
nxbt_tid_recompress_add_to_page(nxbt_tid_recompress_context * cxt, NXTidArrayItem *item)
{
	OffsetNumber maxoff;
	Size		freespc;

	freespc = PageGetExactFreeSpace(cxt->currpage);
	if (freespc < item->t_size + sizeof(ItemIdData) ||
		freespc < (Size) cxt->free_space_per_page)
	{
		nxbt_tid_recompress_newpage(cxt, item->t_firsttid, 0);
	}

	maxoff = PageGetMaxOffsetNumber(cxt->currpage);
	if (!PageAddItem(cxt->currpage, item, item->t_size, maxoff + 1, true, false))
		elog(ERROR, "could not add item to TID tree page");
}

/*
 * Subroutine of nxbt_tid_recompress_replace.  Compute how much space the
 * items will take, and compute how many pages will be needed for them, and
 * decide how to distribute any free space thats's left over among the
 * pages.
 *
 * Like in B-tree indexes, we aim for 50/50 splits, except for the
 * rightmost page where we aim for 90/10 by default, so that most of the
 * free space is left to the end of the index, where it's useful for new
 * inserts.  If the user has set the split_pct reloption, that value is
 * used instead for all pages (rightmost and non-rightmost alike).
 */
static void
nxbt_tid_recompress_picksplit(Relation rel,
							  nxbt_tid_recompress_context * cxt, List *items)
{
	Size		total_sz;
	int			num_pages;
	Size		space_on_empty_page;
	int			free_space_per_page;
	ListCell   *lc;
	int			split_pct;

	space_on_empty_page = BLCKSZ - MAXALIGN(SizeOfPageHeaderData) - MAXALIGN(sizeof(NXBtreePageOpaque));

	/* Compute total space needed for all the items. */
	total_sz = 0;
	foreach(lc, items)
	{
		NXTidArrayItem *item = lfirst(lc);

		total_sz += sizeof(ItemIdData) + item->t_size;
	}

	/* How many pages will we need for them? */
	num_pages = (int) ((total_sz + space_on_empty_page - 1) / space_on_empty_page);

	/*
	 * Determine split ratio.  If the user set split_pct via reloptions,
	 * use that value.  Otherwise default to 90/10 for rightmost pages
	 * (where sequential inserts append) and 50/50 for non-rightmost
	 * pages.  This matches the internal-page strategy in
	 * nxbt_split_internal_page().
	 */
	if (rel->rd_options &&
		((StdRdOptions *) rel->rd_options)->split_pct > 0)
		split_pct = RelationGetSplitPct(rel);
	else if (cxt->hikey == MaxPlusOneNXTid)
		split_pct = 90;
	else
		split_pct = 50;

	/* If everything fits on one page, don't split */
	if (num_pages == 1)
	{
		free_space_per_page = 0;
	}
	/* Rightmost or user-specified: put (100-split_pct)% on the last page */
	else if (cxt->hikey == MaxPlusOneNXTid)
	{
		/*
		 * What does split_pct/rest mean if we have to use more than two
		 * pages?  It means that (100 - split_pct)% of the items go to the
		 * last page, and split_pct% are distributed to all the others.
		 */
		double		total_free_space;

		total_free_space = (double) (space_on_empty_page * (Size) num_pages - total_sz);

		free_space_per_page = (int) (total_free_space * (100 - split_pct) / 100.0 / (num_pages - 1));
	}
	/* Non-rightmost: aim for an even split */
	else
	{
		free_space_per_page = (int) ((space_on_empty_page * (Size) num_pages - total_sz) / (Size) num_pages);
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
 * Adjacent items with contiguous TID ranges and compatible UNDO slots are
 * combined into larger array items to reduce per-item overhead.
 */
static void
nxbt_tid_recompress_replace(Relation rel, Buffer oldbuf, List *items, nx_pending_undo_op * undo_op)
{
	ListCell   *lc;
	nxbt_tid_recompress_context cxt;
	NXBtreePageOpaque *oldopaque = NXBtreePageGetOpaque(BufferGetPage(oldbuf));
	BlockNumber orignextblk;
	nx_split_stack *stack;
	List	   *downlinks = NIL;
	List	   *combined_items;

	orignextblk = oldopaque->nx_next;

	/*
	 * Try to combine adjacent items with contiguous TID ranges into larger
	 * array items.  This reduces per-item overhead (item headers + ItemId
	 * entries) and can avoid unnecessary page splits.
	 */
	combined_items = nxbt_tid_combine_adjacent_items(items);

	cxt.currpage = NULL;
	cxt.stack_head = cxt.stack_tail = NULL;
	cxt.hikey = oldopaque->nx_hikey;

	nxbt_tid_recompress_picksplit(rel, &cxt, combined_items);
	nxbt_tid_recompress_newpage(&cxt, oldopaque->nx_lokey, (oldopaque->nx_flags & NXBT_ROOT));

	foreach(lc, combined_items)
	{
		NXTidArrayItem *item = (NXTidArrayItem *) lfirst(lc);

		nxbt_tid_recompress_add_to_page(&cxt, item);
	}
	list_free(combined_items);

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
		NXBtreePageOpaque *thisopaque = NXBtreePageGetOpaque(thispage);
		NXBtreeInternalPageItem *downlink;
		Buffer		nextbuf;

		Assert(stack->next->buf == InvalidBuffer);

		nextbuf = nxpage_getnewbuf(rel, InvalidBuffer);
		stack->next->buf = nextbuf;

		thisopaque->nx_next = BufferGetBlockNumber(nextbuf);

		downlink = palloc(sizeof(NXBtreeInternalPageItem));
		downlink->tid = thisopaque->nx_hikey;
		downlink->childblk = BufferGetBlockNumber(nextbuf);
		downlinks = lappend(downlinks, downlink);

		stack = stack->next;
	}
	/* last one in the chain */
	NXBtreePageGetOpaque(stack->page)->nx_next = orignextblk;

	/*
	 * nxbt_tid_recompress_picksplit() calculated that we'd need
	 * 'cxt.num_pages' pages. Check that it matches with how many pages we
	 * actually created.
	 */
	Assert(list_length(downlinks) + 1 == cxt.num_pages);

	/* If we had to split, insert downlinks for the new pages. */
	if (cxt.stack_head->next)
	{
		oldopaque = NXBtreePageGetOpaque(cxt.stack_head->page);

		if ((oldopaque->nx_flags & NXBT_ROOT) != 0)
		{
			NXBtreeInternalPageItem *downlink;

			downlink = palloc(sizeof(NXBtreeInternalPageItem));
			downlink->tid = MinNXTid;
			downlink->childblk = BufferGetBlockNumber(cxt.stack_head->buf);
			downlinks = lcons(downlink, downlinks);

			cxt.stack_tail->next = nxbt_newroot(rel, NX_META_ATTRIBUTE_NUM,
												oldopaque->nx_level + 1, downlinks);

			/* clear the NXBT_ROOT flag on the old root page */
			oldopaque->nx_flags &= (uint16) ~NXBT_ROOT;
		}
		else
		{
			cxt.stack_tail->next = nxbt_insert_downlinks(rel, NX_META_ATTRIBUTE_NUM,
														 oldopaque->nx_lokey, BufferGetBlockNumber(oldbuf), oldopaque->nx_level + 1,
														 downlinks, oldbuf,
														 NULL);
		}
		/* note: stack_tail is not the real tail anymore */
	}

	/* Free the downlinks list and items allocated during split */
	list_free_deep(downlinks);

	/* Finally, overwrite all the pages we had to modify */
	nx_apply_split_changes(rel, cxt.stack_head, undo_op);
}

static OffsetNumber
nxbt_binsrch_tidpage(nxtid key, Page page)
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
		NXTidArrayItem *item;

		mid = (OffsetNumber) (low + (high - low) / 2);

		iid = PageGetItemId(page, mid);
		item = (NXTidArrayItem *) PageGetItem(page, iid);

		if (key >= item->t_firsttid)
			low = mid + 1;
		else
			high = mid;
	}
	return low - 1;
}
