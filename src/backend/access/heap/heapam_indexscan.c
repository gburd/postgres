/*-------------------------------------------------------------------------
 *
 * heapam_indexscan.c
 *	  heap table plain index scan and index-only scan code
 *
 * These functions implement the index_fetch callbacks for the heap AM.
 * They are referenced by the TableAmRoutine in heapam_handler.c.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/heap/heapam_indexscan.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/tableam.h"
#include "executor/tuptable.h"
#include "storage/bufmgr.h"
#include "storage/predicate.h"
#include "utils/snapmgr.h"

/* ------------------------------------------------------------------------
 * Index Scan Callbacks for heap AM
 * ------------------------------------------------------------------------
 */

IndexFetchTableData *
heapam_index_fetch_begin(Relation rel, uint32 flags)
{
	IndexFetchHeapData *hscan = palloc0_object(IndexFetchHeapData);

	hscan->xs_base.rel = rel;
	hscan->xs_base.flags = flags;
	hscan->xs_base.indexed_attrs = NULL;
	hscan->xs_cbuf = InvalidBuffer;
	hscan->xs_vmbuffer = InvalidBuffer;

	return &hscan->xs_base;
}

void
heapam_index_fetch_reset(IndexFetchTableData *scan)
{
	/*
	 * Resets are a no-op.
	 *
	 * Deliberately avoid dropping pins now held in xs_cbuf and xs_vmbuffer.
	 * This saves cycles during certain tight nested loop joins (it can avoid
	 * repeated pinning and unpinning of the same buffer across rescans).
	 */
}

void
heapam_index_fetch_end(IndexFetchTableData *scan)
{
	IndexFetchHeapData *hscan = (IndexFetchHeapData *) scan;

	/* drop pin if there's a pinned heap page */
	if (BufferIsValid(hscan->xs_cbuf))
		ReleaseBuffer(hscan->xs_cbuf);

	/* drop pin if there's a pinned visibility map page */
	if (BufferIsValid(hscan->xs_vmbuffer))
		ReleaseBuffer(hscan->xs_vmbuffer);

	/*
	 * Free the indexed_attrs bitmap if it was set.  This was allocated by
	 * indexam.c and stored in the base struct for selective index update
	 * chain following.
	 */
	if (hscan->xs_base.indexed_attrs)
		bms_free(hscan->xs_base.indexed_attrs);

	pfree(hscan);
}

/*
 *	heap_hot_search_buffer	- search HOT chain for tuple satisfying snapshot
 *
 * On entry, *tid is the TID of a tuple (either a simple tuple, or the root
 * of a HOT chain), and buffer is the buffer holding this tuple.  We search
 * for the first chain member satisfying the given snapshot.  If one is
 * found, we update *tid to reference that tuple's offset number, and
 * return true.  If no match, return false without modifying *tid.
 *
 * heapTuple is a caller-supplied buffer.  When a match is found, we return
 * the tuple here, in addition to updating *tid.  If no match is found, the
 * contents of this buffer on return are undefined.
 *
 * If all_dead is not NULL, we check non-visible tuples to see if they are
 * globally dead; *all_dead is set true if all members of the HOT chain
 * are vacuumable, false if not.
 *
 * Unlike heap_fetch, the caller must already have pin and (at least) share
 * lock on the buffer; it is still pinned/locked at exit.
 */
bool
heap_hot_search_buffer(ItemPointer tid, Relation relation, Buffer buffer,
					   Snapshot snapshot, HeapTuple heapTuple,
					   bool *all_dead, bool first_call,
					   const Bitmapset *indexed_attrs)
{
	Page		page = BufferGetPage(buffer);
	TransactionId prev_xmax = InvalidTransactionId;
	BlockNumber blkno;
	OffsetNumber offnum;
	OffsetNumber prev_offnum = InvalidOffsetNumber;
	bool		at_chain_start;
	bool		valid;
	bool		skip;
	GlobalVisState *vistest = NULL;

	/*
	 * Stack-allocated accumulation buffer for SIU bitmaps.  As we traverse
	 * the chain, we OR each INDEXED_UPDATED tuple's raw bitmap into this
	 * buffer.  At the visible tuple, we check if the accumulated bitmap
	 * overlaps with indexed_attrs to determine if this is a stale entry.
	 *
	 * MaxHeapAttributeNumber is 1600, and heap_siu_bitmap_raw_size accounts for
	 * system attributes, so worst case is about 204 bytes.  This is well
	 * within stack limits.
	 */
	uint8		siu_accum[MaxSIUBitmapRawSize];
	int			siu_raw = 0;
	bool		have_siu_accum = false;

	/* If this is not the first call, previous call returned a (live!) tuple */
	if (all_dead)
		*all_dead = first_call;

	blkno = ItemPointerGetBlockNumber(tid);
	offnum = ItemPointerGetOffsetNumber(tid);
	at_chain_start = first_call;
	skip = !first_call;

	/* XXX: we should assert that a snapshot is pushed or registered */
	Assert(TransactionIdIsValid(RecentXmin));
	Assert(BufferGetBlockNumber(buffer) == blkno);

	/* Scan through possible multiple members of HOT-chain */
	for (int chain_iters = 0;; chain_iters++)
	{
		ItemId		lp;

		/*
		 * Safety: bail out if chain exceeds max tuples per page. This guards
		 * against corruption from INDEXED_UPDATED chains.
		 */
		if (chain_iters > MaxHeapTuplesPerPage)
			break;

		/* check for bogus TID */
		if (offnum < FirstOffsetNumber || offnum > PageGetMaxOffsetNumber(page))
			break;

		lp = PageGetItemId(page, offnum);

		/* check for unused, dead, or redirected items */
		if (!ItemIdIsNormal(lp))
		{
			/* We should only see a redirect at start of chain */
			if (ItemIdIsRedirected(lp) && at_chain_start)
			{
				/*
				 * Follow the redirect.  For a selective index update, the old
				 * tuple was pruned to a redirect.  Track this so the
				 * stale-entry check can reject old index entries reaching
				 * tuples via redirect.
				 */
				prev_offnum = offnum;
				offnum = ItemIdGetRedirect(lp);
				at_chain_start = false;

				continue;
			}
			/* else must be end of chain */
			break;
		}

		/*
		 * Update heapTuple to point to the element of the HOT chain we're
		 * currently investigating. Having t_self set correctly is important
		 * because the SSI checks and the *Satisfies routine for historical
		 * MVCC snapshots need the correct tid to decide about the visibility.
		 */
		heapTuple->t_data = (HeapTupleHeader) PageGetItem(page, lp);
		heapTuple->t_len = ItemIdGetLength(lp);
		heapTuple->t_tableOid = RelationGetRelid(relation);
		ItemPointerSet(&heapTuple->t_self, blkno, offnum);

		/*
		 * Shouldn't see a HEAP_ONLY tuple at chain start -- unless this is a
		 * selective index update scan.  HOT selective index updates insert
		 * new index entries that point directly to heap-only tuples (the
		 * updated tuple in the HOT chain).  When indexed_attrs is set, we
		 * allow starting at a HEAP_ONLY tuple and simply check its visibility
		 * without following any chain.
		 */
		if (at_chain_start && HeapTupleIsHeapOnly(heapTuple) &&
			!indexed_attrs &&
			!HeapTupleHeaderIsIndexedUpdatedRaw(heapTuple->t_data))
			break;

		/*
		 * The xmin should match the previous xmax value, else chain is
		 * broken.
		 */
		if (TransactionIdIsValid(prev_xmax) &&
			!TransactionIdEquals(prev_xmax,
								 HeapTupleHeaderGetXmin(heapTuple->t_data)))
			break;

		/*
		 * Accumulate SIU bitmaps from every INDEXED_UPDATED tuple in the
		 * chain (visible or invisible).  We need the full accumulated
		 * bitmap to correctly detect stale entries in multi-step SIU
		 * chains where consecutive updates modify different indexed columns.
		 */
		if (OffsetNumberIsValid(prev_offnum) &&
			indexed_attrs &&
			HeapTupleHeaderIsIndexedUpdatedRaw(heapTuple->t_data))
		{
			if (!have_siu_accum)
			{
				siu_raw = heap_siu_bitmap_raw_size(
					HeapTupleHeaderGetNatts(heapTuple->t_data));
				Assert(siu_raw <= MaxSIUBitmapRawSize);
				memset(siu_accum, 0, siu_raw);
				have_siu_accum = true;
			}
			heap_siu_bitmap_or_raw(heapTuple->t_data, siu_accum, siu_raw);
		}

		/*
		 * When first_call is true (and thus, skip is initially false) we'll
		 * return the first tuple we find.  But on later passes, heapTuple
		 * will initially be pointing to the tuple we returned last time.
		 * Returning it again would be incorrect (and would loop forever), so
		 * we skip it and return the next match we find.
		 */
		if (!skip)
		{
			/* If it's visible per the snapshot, we must return it */
			valid = HeapTupleSatisfiesVisibility(heapTuple, snapshot, buffer);
			HeapCheckForSerializableConflictOut(valid, relation, heapTuple,
												buffer, snapshot);

			if (valid)
			{
				/*
				 * For selective index updates: if we followed a chain to get
				 * here (prev_offnum valid) and the accumulated SIU bitmap
				 * overlaps with this index's attributes, this is a stale
				 * index entry.  Skip it — the fresh index entry (which SIU
				 * always creates for changed columns) will find this tuple
				 * directly at its own TID.
				 *
				 * Note: the accumulation starts from the SECOND tuple in the
				 * chain (prev_offnum must be valid), so it only captures
				 * modifications that occurred AFTER the index entry's target
				 * tuple.  This correctly distinguishes:
				 *  - Stale entries (TID=chain root): accumulates all changes
				 *  - Fresh SIU entries (TID=new tuple): only accumulates
				 *    changes after that tuple, not the change that created it
				 */
				if (OffsetNumberIsValid(prev_offnum) &&
					indexed_attrs &&
					have_siu_accum &&
					heap_siu_accum_overlaps(siu_accum, siu_raw, indexed_attrs))
				{
					/*
					 * Stale entry detected.  Mark the chain as not-all-dead
					 * (the tuple is visible, just reached via a stale path)
					 * and fall through to continue chain traversal, which
					 * will end shortly (visible tuples are typically chain
					 * endpoints for MVCC snapshots).
					 */
					if (all_dead)
						*all_dead = false;
				}
				else
				{
					ItemPointerSetOffsetNumber(tid, offnum);
					PredicateLockTID(relation, &heapTuple->t_self, snapshot,
									 HeapTupleHeaderGetXmin(heapTuple->t_data));
					if (all_dead)
						*all_dead = false;
					return true;
				}
			}
		}
		skip = false;

		/*
		 * If we can't see it, maybe no one else can either.  At caller
		 * request, check whether all chain members are dead to all
		 * transactions.
		 *
		 * Note: if you change the criterion here for what is "dead", fix the
		 * planner's get_actual_variable_range() function to match.
		 */
		if (all_dead && *all_dead)
		{
			if (!vistest)
				vistest = GlobalVisTestFor(relation);

			if (!HeapTupleIsSurelyDead(heapTuple, vistest))
				*all_dead = false;
		}

		/*
		 * Check to see if HOT chain continues past this tuple; if so fetch
		 * the next offnum and loop around.
		 */
		if (HeapTupleIsHotUpdated(heapTuple))
		{
			/*
			 * Follow the HOT chain.  INDEXED_UPDATED tuples are also part of
			 * the HOT chain (they have HOT_UPDATED set on the predecessor),
			 * so chain continuation is driven solely by HOT_UPDATED.  The
			 * INDEXED_UPDATED flag is only used for stale-entry detection
			 * (above), not for chain following.
			 */
			Assert(ItemPointerGetBlockNumber(&heapTuple->t_data->t_ctid) ==
				   blkno);
			prev_offnum = offnum;
			offnum = ItemPointerGetOffsetNumber(&heapTuple->t_data->t_ctid);
			at_chain_start = false;
			prev_xmax = HeapTupleHeaderGetUpdateXid(heapTuple->t_data);
		}
		else
			break;				/* end of chain */
	}

	return false;
}

bool
heapam_index_fetch_tuple(struct IndexFetchTableData *scan,
						 ItemPointer tid,
						 Snapshot snapshot,
						 TupleTableSlot *slot,
						 bool *call_again, bool *all_dead)
{
	IndexFetchHeapData *hscan = (IndexFetchHeapData *) scan;
	BufferHeapTupleTableSlot *bslot = (BufferHeapTupleTableSlot *) slot;
	bool		got_heap_tuple;

	Assert(TTS_IS_BUFFERTUPLE(slot));

	/* We can skip the buffer-switching logic if we're in mid-HOT chain. */
	if (!*call_again)
	{
		/* Switch to correct buffer if we don't have it already */
		Buffer		prev_buf = hscan->xs_cbuf;

		hscan->xs_cbuf = ReleaseAndReadBuffer(hscan->xs_cbuf,
											  hscan->xs_base.rel,
											  ItemPointerGetBlockNumber(tid));

		/*
		 * Prune page, but only if we weren't already on this page
		 */
		if (prev_buf != hscan->xs_cbuf)
			heap_page_prune_opt(hscan->xs_base.rel, hscan->xs_cbuf,
								&hscan->xs_vmbuffer,
								hscan->xs_base.flags & SO_HINT_REL_READ_ONLY);
	}

	/* Obtain share-lock on the buffer so we can examine visibility */
	LockBuffer(hscan->xs_cbuf, BUFFER_LOCK_SHARE);
	got_heap_tuple = heap_hot_search_buffer(tid,
											hscan->xs_base.rel,
											hscan->xs_cbuf,
											snapshot,
											&bslot->base.tupdata,
											all_dead,
											!*call_again,
											hscan->xs_base.indexed_attrs);
	bslot->base.tupdata.t_self = *tid;
	LockBuffer(hscan->xs_cbuf, BUFFER_LOCK_UNLOCK);

	if (got_heap_tuple)
	{
		/*
		 * Only in a non-MVCC snapshot can more than one member of the HOT
		 * chain be visible.
		 */
		*call_again = !IsMVCCLikeSnapshot(snapshot);

		slot->tts_tableOid = RelationGetRelid(scan->rel);
		ExecStoreBufferHeapTuple(&bslot->base.tupdata, slot, hscan->xs_cbuf);
	}
	else
	{
		/* We've reached the end of the HOT chain. */
		*call_again = false;
	}

	return got_heap_tuple;
}
