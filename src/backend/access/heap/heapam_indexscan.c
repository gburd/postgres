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
 * hot_indexed_path_overlaps
 *		True iff any chain hop crossed during a walk (whose member offsets are
 *		listed in crossed[0 .. ncrossed-1]) changed an attribute that
 *		index_attrs covers.  Each crossed member's per-hop modified-attrs
 *		bitmap lives in the adjacent tombstone whose target is that member's
 *		offset; we scan the page once for those tombstones.  Bridges carry no
 *		payload of their own -- the bridged tuple's adjacent tombstone is
 *		retained across collapse and still targets the bridge's offset, so the
 *		same lookup serves live tuples and bridges alike.
 *
 * This is a single O(maxoff) pass, not a per-crossed-member lookup.  A
 * tombstone is added at the first free line pointer (PageAddItemExtended with
 * InvalidOffsetNumber), so there is no positional relation between a tombstone
 * and its target: finding one member's tombstone is itself O(maxoff).  Walking
 * the crossed members and looking each up would therefore be O(ncrossed *
 * maxoff) -- strictly worse than this one sweep -- so the single scan is kept
 * deliberately.  Measured cost is a negligible fraction of a hop-crossing
 * fetch at realistic page densities.
 */
static bool
hot_indexed_path_overlaps(Page page, const OffsetNumber *crossed, int ncrossed,
						  const Bitmapset *index_attrs)
{
	OffsetNumber maxoff = PageGetMaxOffsetNumber(page);

	for (OffsetNumber off = FirstOffsetNumber; off <= maxoff;
		 off = OffsetNumberNext(off))
	{
		ItemId		lp = PageGetItemId(page, off);
		HeapTupleHeader htup;
		OffsetNumber target;

		if (!ItemIdIsNormal(lp))
			continue;
		htup = (HeapTupleHeader) PageGetItem(page, lp);
		if (!HeapTupleHeaderIsHotIndexedTombstone(htup) ||
			HeapTupleHeaderIsHotIndexedBridge(htup))
			continue;
		target = HotIndexedTombstoneGetTarget(htup);

		for (int i = 0; i < ncrossed; i++)
		{
			if (crossed[i] != target)
				continue;
			if (heap_hot_indexed_payload_overlaps(HotIndexedTombstoneGetPayloadConst(htup),
												  index_attrs))
				return true;
			break;
		}
	}
	return false;
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
 * If hot_indexed_stale is not NULL, it reports whether the index entry that
 * the caller arrived through is stale for the tuple returned, using the
 * per-hop modified-attrs bitmaps left by HOT-indexed (Selective Index Update)
 * updates:
 *
 *   - When index_attrs is not NULL (a single-index scan or unique check), it
 *     is the set of heap attributes that index covers (RelationGetIndexedAttrs
 *     convention).  *hot_indexed_stale is set true iff some hop strictly after
 *     the entry tuple, up to the returned tuple, changed one of those
 *     attributes -- i.e. the arriving leaf's key no longer agrees with the
 *     live tuple and the row is re-supplied by a fresh entry.  The entry
 *     tuple's own producing hop is excluded, so a fresh entry pointing into
 *     the chain is kept.
 *
 *   - When index_attrs is NULL (e.g. a bitmap heap scan, where the originating
 *     index is no longer identifiable), *hot_indexed_stale is set true iff the
 *     walk crossed any HOT-indexed hop at all; the caller then forces its
 *     recheck qual.
 *
 * When the chain contains no HOT-indexed hop, *hot_indexed_stale is left
 * false.
 *
 * Unlike heap_fetch, the caller must already have pin and (at least) share
 * lock on the buffer; it is still pinned/locked at exit.
 */
bool
heap_hot_search_buffer(ItemPointer tid, Relation relation, Buffer buffer,
					   Snapshot snapshot, HeapTuple heapTuple,
					   bool *all_dead, bool first_call,
					   const Bitmapset *index_attrs,
					   bool *hot_indexed_stale)
{
	Page		page = BufferGetPage(buffer);
	TransactionId prev_xmax = InvalidTransactionId;
	BlockNumber blkno;
	OffsetNumber offnum;
	bool		at_chain_start;
	bool		valid;
	bool		skip;
	GlobalVisState *vistest = NULL;
	OffsetNumber crossed[MaxHeapTuplesPerPage];
	int			ncrossed = 0;
	bool		crossed_hi = false;
	bool		redir_stale = false;

	/* If this is not the first call, previous call returned a (live!) tuple */
	if (all_dead)
		*all_dead = first_call;

	/*
	 * On the first call, clear any stale value left by a previous call. On
	 * subsequent calls (same chain continuing), preserve whatever the earlier
	 * hop observed.
	 */
	if (hot_indexed_stale && first_call)
		*hot_indexed_stale = false;

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
				/*
				 * A HOT-indexed data redirect carries the (single) hop's
				 * modified-attrs bitmap that the reclaimed successor
				 * tombstone used to hold.  An entry arriving through it
				 * crosses that hop, so fold the bitmap into the staleness
				 * decision (and flag the chain hot-indexed for the
				 * bitmap-heap NULL case).
				 */
				if (HotIndexedRedirectIsData(lp))
				{
					crossed_hi = true;
					if (index_attrs != NULL &&
						heap_hot_indexed_redirect_overlaps(HotIndexedRedirectGetData(page, lp),
														   index_attrs))
						redir_stale = true;
				}
				/* Follow the redirect */
				offnum = HotIndexedRedirectGetTarget(page, lp);
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
		 * InvalidBlockNumber; they sit next to a newly-written HOT-indexed
		 * tuple and carry its modified-attrs bitmap.  A stale btree entry
		 * that lands on one has no forward link to follow -- treat as end of
		 * chain.
		 *
		 * - Bridge tombstones have a valid same-page forward t_ctid, placed
		 * by pruneheap in the slot a dead mid-chain HOT-indexed heap-only
		 * tuple used to occupy.  Stale btree entries pointing at the bridge's
		 * LP still resolve to the live tuple by following the forward link.
		 * Skip the bridge transparently: don't apply the xmin/xmax chain
		 * match (bridges carry neither), count it as a crossed hop so the
		 * bitmap-overlap test sees its retained tombstone's modified attrs,
		 * and continue the walk.
		 */
		if (HeapTupleHeaderIsHotIndexedTombstone(heapTuple->t_data))
		{
			if (HeapTupleHeaderIsHotIndexedBridge(heapTuple->t_data))
			{
				crossed_hi = true;

				/*
				 * A bridge advanced to (not the entry item) is a crossed hop;
				 * its modified-attrs bitmap lives in the retained adjacent
				 * tombstone targeting this offset.  When the bridge IS the
				 * entry item (at_chain_start), its own hop is excluded -- the
				 * arriving entry already reflects that value.
				 */
				if (!at_chain_start && ncrossed < (int) lengthof(crossed))
					crossed[ncrossed++] = offnum;
				offnum = HotIndexedBridgeGetForward(heapTuple->t_data);
				at_chain_start = false;

				/*
				 * prev_xmax intentionally not updated: bridges don't advance
				 * it
				 */
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
			 * reflects this tuple's current attribute values: its own
			 * producing hop is excluded from the staleness test.  We still
			 * note that the chain is hot-indexed (crossed_hi) so a bitmap
			 * heap scan, which cannot identify the originating index, falls
			 * back to its recheck qual.
			 */
			crossed_hi = true;
		}
		else if ((heapTuple->t_data->t_infomask2 & HEAP_INDEXED_UPDATED) != 0)
		{
			/*
			 * A hot-indexed hop reached by following the chain from an
			 * earlier entry: this hop is crossed.  Record its offset so the
			 * post-walk overlap test can consult its adjacent tombstone's
			 * modified-attrs bitmap.
			 */
			crossed_hi = true;
			if (ncrossed < (int) lengthof(crossed))
				crossed[ncrossed++] = offnum;
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

				if (hot_indexed_stale != NULL)
				{
					bool		this_stale;

					if (index_attrs == NULL)
						this_stale = crossed_hi;
					else
						this_stale = redir_stale ||
							((ncrossed > 0) &&
							 hot_indexed_path_overlaps(page, crossed, ncrossed,
													   index_attrs));
					*hot_indexed_stale |= this_stale;
				}
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
						 const Bitmapset *index_attrs,
						 bool *hot_indexed_stale)
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
											index_attrs,
											hot_indexed_stale);
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
