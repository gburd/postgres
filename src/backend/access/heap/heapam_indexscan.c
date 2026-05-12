/*-------------------------------------------------------------------------
 *
 * heapam_indexscan.c
 *	  heap table plain index scan and index-only scan code
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
#include "access/hot_indexed.h"
#include "access/relscan.h"
#include "storage/predicate.h"


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
	hscan->xs_cbuf = InvalidBuffer;
	hscan->xs_blk = InvalidBlockNumber;
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
 * If hot_indexed_recheck is not NULL, it is set to true iff any tuple
 * visited along the chain (including the returned one) carries
 * HEAP_INDEXED_UPDATED.  Callers use this to know that at least one
 * HOT-indexed update has occurred in the chain, and therefore an
 * index-scan that arrived via this chain must recheck its scan keys
 * against the returned tuple's attribute values -- the index entry's
 * key may no longer agree with the heap tuple for attributes covered by
 * one of the encountered tombstones.  This is a conservative signal:
 * Phase 3.1e will refine it with per-index attr matching.  When there
 * was no hot-indexed in the chain, *hot_indexed_recheck is left set to false.
 *
 * Unlike heap_fetch, the caller must already have pin and (at least) share
 * lock on the buffer; it is still pinned/locked at exit.
 */
bool
heap_hot_search_buffer(ItemPointer tid, Relation relation, Buffer buffer,
					   Snapshot snapshot, HeapTuple heapTuple,
					   bool *all_dead, bool first_call,
					   bool *hot_indexed_recheck)
{
	Page		page = BufferGetPage(buffer);
	TransactionId prev_xmax = InvalidTransactionId;
	BlockNumber blkno;
	OffsetNumber offnum;
	bool		at_chain_start;
	bool		valid;
	bool		skip;
	GlobalVisState *vistest = NULL;

	/* If this is not the first call, previous call returned a (live!) tuple */
	if (all_dead)
		*all_dead = first_call;

	/*
	 * On the first call, clear any stale value left by a previous call. On
	 * subsequent calls (same chain continuing), preserve whatever the earlier
	 * hop observed.
	 */
	if (hot_indexed_recheck && first_call)
		*hot_indexed_recheck = false;

	blkno = ItemPointerGetBlockNumber(tid);
	offnum = ItemPointerGetOffsetNumber(tid);
	at_chain_start = first_call;
	skip = !first_call;

	/* XXX: we should assert that a snapshot is pushed or registered */
	Assert(TransactionIdIsValid(RecentXmin));
	Assert(BufferGetBlockNumber(buffer) == blkno);

	/* Scan through possible multiple members of HOT-chain */
	for (;;)
	{
		ItemId		lp;

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
				/* Follow the redirect */
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
		 * HOT-indexed tombstones (two variants) are never visible tuples.
		 *
		 * - Adjacent-to-live tombstones have t_ctid.blockno =
		 *   InvalidBlockNumber; they sit next to a newly-written HOT-indexed
		 *   tuple and carry its modified-attrs bitmap.  A stale btree entry
		 *   that lands on one has no forward link to follow -- treat as end
		 *   of chain.
		 *
		 * - Bridge tombstones have a valid same-page forward t_ctid, placed
		 *   by pruneheap in the slot a dead mid-chain HOT-indexed heap-only
		 *   tuple used to occupy.  Stale btree entries pointing at the
		 *   bridge's LP still resolve to the live tuple by following the
		 *   forward link.  Skip the bridge transparently: don't apply the
		 *   xmin/xmax chain match (bridges carry neither), raise the recheck
		 *   signal so readers compare the stored leaf key against the live
		 *   tuple's current index form, and continue the walk.
		 */
		if (HeapTupleHeaderIsHotIndexedTombstone(heapTuple->t_data))
		{
			if (HeapTupleHeaderIsHotIndexedBridge(heapTuple->t_data))
			{
				if (hot_indexed_recheck != NULL)
					*hot_indexed_recheck = true;
				offnum = HotIndexedBridgeGetForward(heapTuple->t_data);
				at_chain_start = false;
				/* prev_xmax intentionally not updated: bridges don't advance it */
				continue;
			}
			break;
		}

		/*
		 * Shouldn't see a HEAP_ONLY tuple at chain start, unless that tuple
		 * is the target of a freshly-inserted hot-indexed index entry: then
		 * arriving directly at a heap-only HOT-indexed tuple is legal and the
		 * tuple is the canonical visible version, so we fall through and
		 * apply normal visibility checks to it.  Otherwise, treat it as a
		 * broken chain.
		 */
		if (at_chain_start && HeapTupleIsHeapOnly(heapTuple))
		{
			if ((heapTuple->t_data->t_infomask2 & HEAP_INDEXED_UPDATED) == 0)
				break;

			/*
			 * We were pointed directly at this hot-indexed tuple.  The index
			 * entry we arrived through was inserted *for* this update, so it
			 * agrees with the current tuple's attribute values and the
			 * executor does not strictly have to recheck quals.  We still
			 * raise the recheck flag, though, so higher-level readers (e.g.
			 * systable_getnext) can dedup against other btree entries whose
			 * chain walks end at this same live TID -- the case of an index
			 * key that was cycled back to itself by a HOT-indexed rename.
			 */
			if (hot_indexed_recheck != NULL)
				*hot_indexed_recheck = true;
		}
		else if (hot_indexed_recheck != NULL &&
				 (heapTuple->t_data->t_infomask2 & HEAP_INDEXED_UPDATED) != 0)
		{
			/*
			 * We walked through a HOT-indexed hop reached via an older index
			 * entry.  The scan key that got us here may no longer agree with
			 * the heap tuple's current attribute values -- force the executor
			 * to recheck quals against the returned tuple.
			 */
			*hot_indexed_recheck = true;
		}

		/*
		 * The xmin should match the previous xmax value, else chain is
		 * broken.
		 */
		if (TransactionIdIsValid(prev_xmax) &&
			!TransactionIdEquals(prev_xmax,
								 HeapTupleHeaderGetXmin(heapTuple->t_data)))
			break;

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
				ItemPointerSetOffsetNumber(tid, offnum);
				PredicateLockTID(relation, &heapTuple->t_self, snapshot,
								 HeapTupleHeaderGetXmin(heapTuple->t_data));
				if (all_dead)
					*all_dead = false;
				return true;
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
			Assert(ItemPointerGetBlockNumber(&heapTuple->t_data->t_ctid) ==
				   blkno);
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
						 bool *heap_continue, bool *all_dead,
						 bool *hot_indexed_recheck)
{
	IndexFetchHeapData *hscan = (IndexFetchHeapData *) scan;
	BufferHeapTupleTableSlot *bslot = (BufferHeapTupleTableSlot *) slot;
	bool		got_heap_tuple;

	Assert(TTS_IS_BUFFERTUPLE(slot));

	/* We can skip the buffer-switching logic if we're on the same page. */
	if (hscan->xs_blk != ItemPointerGetBlockNumber(tid))
	{
		Assert(!*heap_continue);

		/* Remember this buffer's block number for next time */
		hscan->xs_blk = ItemPointerGetBlockNumber(tid);

		if (BufferIsValid(hscan->xs_cbuf))
			ReleaseBuffer(hscan->xs_cbuf);

		hscan->xs_cbuf = ReadBuffer(hscan->xs_base.rel, hscan->xs_blk);

		/*
		 * Prune page when it is pinned for the first time
		 */
		heap_page_prune_opt(hscan->xs_base.rel, hscan->xs_cbuf,
							&hscan->xs_vmbuffer,
							hscan->xs_base.flags & SO_HINT_REL_READ_ONLY);
	}

	Assert(BufferGetBlockNumber(hscan->xs_cbuf) == hscan->xs_blk);
	Assert(hscan->xs_blk == ItemPointerGetBlockNumber(tid));

	/* Obtain share-lock on the buffer so we can examine visibility */
	LockBuffer(hscan->xs_cbuf, BUFFER_LOCK_SHARE);
	got_heap_tuple = heap_hot_search_buffer(tid,
											hscan->xs_base.rel,
											hscan->xs_cbuf,
											snapshot,
											&bslot->base.tupdata,
											all_dead,
											!*heap_continue,
											hot_indexed_recheck);
	bslot->base.tupdata.t_self = *tid;
	LockBuffer(hscan->xs_cbuf, BUFFER_LOCK_UNLOCK);

	if (got_heap_tuple)
	{
		/*
		 * Only in a non-MVCC snapshot can more than one member of the HOT
		 * chain be visible.
		 */
		*heap_continue = !IsMVCCLikeSnapshot(snapshot);

		slot->tts_tableOid = RelationGetRelid(scan->rel);
		ExecStoreBufferHeapTuple(&bslot->base.tupdata, slot, hscan->xs_cbuf);
	}
	else
	{
		/* We've reached the end of the HOT chain. */
		*heap_continue = false;
	}

	return got_heap_tuple;
}
