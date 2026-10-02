/*-------------------------------------------------------------------------
 *
 * barkknn.c
 *	  Ordered-operator (KNN) index scan for the BARK index access method.
 *
 * A BARK index orders a scalar key, so `ORDER BY col <~> const` -- the keys
 * nearest to a constant, in increasing distance -- is answerable without a
 * sort: the nearest values to const are found by descending to const and
 * expanding OUTWARD in both directions along the leaf chain.  This module
 * runs that two-way merge.  It is the amcanorderbyop capability GiST and
 * SP-GiST expose for multidimensional types, specialized to the one case a
 * total-order B-tree can answer: distance from a point on a scalar axis.
 *
 * Two position cursors walk the leaf chain from the center leaf: a forward
 * cursor over keys >= const (ascending) and a backward cursor over keys <
 * const (descending).  Both distance streams are monotonically increasing in
 * their own direction, so merging them -- popping whichever side's next
 * candidate is closer at each step -- yields keys in exact increasing
 * distance.  The scan stops the moment the caller (a LIMIT, typically) stops
 * pulling, so it never reads past the k nearest.
 *
 * Correctness relies only on the key order the comparator defines and on the
 * ordering operator computing a distance monotone in |key - const| on each
 * side of const; the integer/bigint distance functions in barkutils.c satisfy
 * that.  BARK is exact, so the distances are exact (xs_recheckorderby stays
 * false) and the executor returns tuples straight through without a reorder
 * queue.
 *
 * ponytail: one ordering key only (the first ORDER BY <~> clause).  A scalar
 * B-tree has a single distance axis, so a second ordering key cannot refine
 * the order the way it would for a multidimensional GiST index; supporting
 * several would need GiST's priority queue for no gain here.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/bark/barkknn.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bark.h"
#include "access/relscan.h"
#include "access/skey.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/predicate.h"
#include "utils/float.h"
#include "utils/rel.h"

/*
 * Does index tuple `itup` satisfy every search (WHERE) scan key?  Shared logic
 * with the plain scan's bark_tuple_matches, duplicated here so the KNN path
 * stays self-contained; a KNN scan may still carry ordinary quals
 * (ORDER BY col <~> c combined with WHERE col > lo), which must filter the
 * candidates before they enter the merge.
 */
static bool
bark_knn_match_keys(IndexScanDesc scan, IndexTuple itup)
{
	TupleDesc	tupdesc = RelationGetDescr(scan->indexRelation);

	for (int i = 0; i < scan->numberOfKeys; i++)
	{
		ScanKey		key = &scan->keyData[i];
		Datum		datum;
		bool		isnull;

		datum = index_getattr(itup, key->sk_attno, tupdesc, &isnull);

		if (key->sk_flags & SK_ISNULL)
		{
			if (key->sk_flags & SK_SEARCHNULL)
			{
				if (!isnull)
					return false;
				continue;
			}
			if (key->sk_flags & SK_SEARCHNOTNULL)
			{
				if (isnull)
					return false;
				continue;
			}
			return false;
		}
		if (isnull)
			return false;

		if (!DatumGetBool(FunctionCall2Coll(&key->sk_func, key->sk_collation,
											datum, key->sk_argument)))
			return false;
	}
	return true;
}

/*
 * Compute the ordering distance of index tuple `itup` from the center
 * constant.  A NULL key sorts last (distance +infinity), matching the
 * executor's NULLS LAST default for an ascending ORDER BY.
 */
static double
bark_knn_distance(IndexScanDesc scan, IndexTuple itup)
{
	BarkKnnScanState *knn = ((BarkScanOpaque) scan->opaque)->knn;
	TupleDesc	tupdesc = RelationGetDescr(scan->indexRelation);
	Datum		datum;
	bool		isnull;
	Datum		d;

	datum = index_getattr(itup, knn->attno, tupdesc, &isnull);
	if (isnull)
		return get_float8_infinity();

	d = FunctionCall2Coll(&knn->distfn, knn->distcollation, datum,
						  knn->center);
	return DatumGetFloat8(d);
}

/* Grow a cursor's TID buffer to hold at least n locators. */
static void
bark_knn_ensure_tids(BarkKnnCursor *cur, int n)
{
	if (n > cur->ntidsAlloc)
	{
		if (cur->tids)
			pfree(cur->tids);
		cur->ntidsAlloc = Max(n, 16);
		cur->tids = (ItemPointer) palloc(cur->ntidsAlloc *
										 sizeof(ItemPointerData));
	}
}

/*
 * Build the lower-bound key tuple used to descend to the center leaf: the
 * center constant in the ordered column, every other attribute NULL (exactly
 * as the plain scan's bark_make_lower_bound does).  The caller pfrees it.
 */
static IndexTuple
bark_knn_center_key(IndexScanDesc scan)
{
	Relation	index = scan->indexRelation;
	TupleDesc	tupdesc = RelationGetDescr(index);
	BarkKnnScanState *knn = ((BarkScanOpaque) scan->opaque)->knn;
	int			natts = IndexRelationGetNumberOfAttributes(index);
	Datum	   *values = (Datum *) palloc(natts * sizeof(Datum));
	bool	   *isnull = (bool *) palloc(natts * sizeof(bool));
	IndexTuple	key;

	for (int c = 0; c < natts; c++)
	{
		values[c] = (Datum) 0;
		isnull[c] = true;
	}
	values[knn->attno - 1] = knn->center;
	isnull[knn->attno - 1] = false;

	key = index_form_tuple(tupdesc, values, isnull);
	pfree(values);
	pfree(isnull);
	return key;
}

/*
 * Find the first offset on `page` whose key is >= the center key tuple
 * `center` -- the boundary that splits the center leaf into the backward
 * (< center) and forward (>= center) sides.  Binary search with the opclass
 * comparator, exactly as barksearch.c's bark_binsrch does for descent.
 */
static OffsetNumber
bark_knn_split_offset(IndexScanDesc scan, Page page, IndexTuple center)
{
	Relation	index = scan->indexRelation;
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	BarkPageOpaque opaque = BarkPageGetOpaque(page);
	OffsetNumber low = BarkPageFirstDataKey(opaque);
	OffsetNumber high = PageGetMaxOffsetNumber(page);

	if (high < low)
		return low;				/* empty page */

	high = OffsetNumberNext(high);	/* one past the last item */
	while (low < high)
	{
		OffsetNumber mid = low + ((high - low) / 2);
		ItemId		iid = PageGetItemId(page, mid);
		IndexTuple	itup = (IndexTuple) PageGetItem(page, iid);
		int			cmp = bark_compare_itups(so->keyinfo, index, center, itup);

		if (cmp > 0)
			low = OffsetNumberNext(mid);	/* center > itup: boundary later */
		else
			high = mid;			/* itup >= center: boundary here or earlier */
	}
	return low;
}

/*
 * Position both cursors around the center.  Descend to the first leaf that
 * could hold the center key (nextkey=false lands on the leftmost leaf of a
 * run of equal keys, so neither side skips a duplicate), then split that leaf
 * at the first entry whose key is >= center: the forward cursor starts there
 * and walks toward higher keys, the backward cursor starts one entry earlier
 * and walks toward lower keys.  Both pins are taken here and released as the
 * cursors walk off the chain.
 */
static void
bark_knn_position(IndexScanDesc scan)
{
	Relation	index = scan->indexRelation;
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	BarkKnnScanState *knn = so->knn;
	IndexTuple	center;
	Buffer		buf;
	BlockNumber blkno;
	OffsetNumber splitoff;

	knn->fwd.buf = knn->bwd.buf = InvalidBuffer;
	knn->fwd.primed = knn->bwd.primed = false;
	knn->fwd.have = knn->bwd.have = false;
	knn->fwd.centerleaf = knn->bwd.centerleaf = false;

	if (knn->centernull)
		return;					/* a NULL center matches nothing */

	center = bark_knn_center_key(scan);
	buf = bark_search(index, so->keyinfo, center, false, false, NULL);
	if (buf == InvalidBuffer)
	{
		pfree(center);
		return;					/* empty index */
	}

	blkno = BufferGetBlockNumber(buf);
	/* bark_search left the leaf share-locked: read the split boundary now. */
	splitoff = bark_knn_split_offset(scan, BufferGetPage(buf), center);
	UnlockReleaseBuffer(buf);
	pfree(center);

	knn->fwd.buf = ReadBuffer(index, blkno);
	knn->fwd.off = InvalidOffsetNumber;
	knn->fwd.centerleaf = true;
	knn->fwd.splitoff = splitoff;
	knn->bwd.buf = ReadBuffer(index, blkno);
	knn->bwd.off = InvalidOffsetNumber;
	knn->bwd.centerleaf = true;
	knn->bwd.splitoff = splitoff;
}

/*
 * Advance one cursor to its next matching entry in its direction and buffer
 * the candidate (distance, locators, and the key tuple for an index-only
 * scan) in the cursor.  Sets cur->have on success; releases the cursor's
 * buffer and leaves cur->have false at end of chain.
 *
 * The backward cursor's very first step must land on the entry strictly
 * before the forward cursor's start, so the two sides never return the same
 * entry.  Priming encodes that: a fresh forward cursor (off ==
 * InvalidOffsetNumber) starts at the first data entry; a fresh backward
 * cursor starts one entry earlier.
 */
static void
bark_knn_advance(IndexScanDesc scan, BarkKnnCursor *cur)
{
	Relation	index = scan->indexRelation;
	bool		backward = cur->backward;

	cur->have = false;

	while (BufferIsValid(cur->buf))
	{
		Buffer		buf = cur->buf;
		Page		page;
		BarkPageOpaque opaque;
		OffsetNumber off,
					maxoff,
					firstdata;
		bool		found = false;

		LockBuffer(buf, BUFFER_LOCK_SHARE);
		PredicateLockPage(index, BufferGetBlockNumber(buf), scan->xs_snapshot);

		page = BufferGetPage(buf);
		opaque = BarkPageGetOpaque(page);
		maxoff = PageGetMaxOffsetNumber(page);
		firstdata = BarkPageFirstDataKey(opaque);

		if (!cur->primed)
		{
			/*
			 * First step on this page.  On the shared center leaf, the forward
			 * cursor starts at the split boundary (first key >= center) and
			 * the backward cursor one entry earlier, so the two sides partition
			 * the center leaf without overlap.  On a fresh sibling, each starts
			 * at the page's natural end for its direction: firstdata (forward)
			 * or maxoff (backward).
			 */
			if (cur->centerleaf)
				off = backward ? OffsetNumberPrev(cur->splitoff) : cur->splitoff;
			else
				off = backward ? maxoff : firstdata;
			cur->primed = true;
		}
		else
			off = backward ? OffsetNumberPrev(cur->off)
				: OffsetNumberNext(cur->off);

		for (;
			 backward ? (off >= firstdata && off != InvalidOffsetNumber)
			 : off <= maxoff;
			 off = backward ? OffsetNumberPrev(off) : OffsetNumberNext(off))
		{
			ItemId		iid;
			IndexTuple	itup;

			if (off < firstdata || off > maxoff)
				break;			/* empty page */

			iid = PageGetItemId(page, off);
			itup = (IndexTuple) PageGetItem(page, iid);

			if (!bark_knn_match_keys(scan, itup))
				continue;

			/* Buffer this candidate: distance, locators, and (IOS) its key. */
			cur->dist = bark_knn_distance(scan, itup);
			{
				int			n = bark_entry_count_tids(itup);

				bark_knn_ensure_tids(cur, n);
				cur->ntids = bark_entry_get_tids(itup, cur->tids,
												 cur->ntidsAlloc);
			}
			if (scan->xs_want_itup)
			{
				TupleDesc	tupdesc = RelationGetDescr(index);
				Datum		values[INDEX_MAX_KEYS];
				bool		isnull[INDEX_MAX_KEYS];
				IndexTuple	key;
				Size		sz;

				index_deform_tuple(itup, tupdesc, values, isnull);
				key = index_form_tuple(tupdesc, values, isnull);
				sz = IndexTupleSize(key);
				Assert(sz <= BLCKSZ);
				if (cur->keytup == NULL)
					cur->keytup = palloc(BLCKSZ);
				memcpy(cur->keytup, key, sz);
				pfree(key);
			}
			cur->off = off;
			cur->have = true;
			found = true;
			break;
		}

		LockBuffer(buf, BUFFER_LOCK_UNLOCK);

		if (found)
			return;

		/* Exhausted this page; follow the sibling link in our direction. */
		{
			BlockNumber nextblk = backward ? opaque->bark_prev
				: opaque->bark_next;

			ReleaseBuffer(buf);
			if (nextblk != BARK_P_NONE)
			{
				/*
				 * Fresh sibling: it is no longer the center leaf, and we
				 * re-prime at its natural end (firstdata forward, maxoff
				 * backward) on the next iteration.
				 */
				cur->buf = ReadBuffer(index, nextblk);
				cur->off = InvalidOffsetNumber;
				cur->primed = false;
				cur->centerleaf = false;
			}
			else
				cur->buf = InvalidBuffer;
		}
	}
}

/*
 * Stage the winning cursor's buffered candidate as the entry to emit (its
 * members are then returned one heap TID per gettuple call) and advance that
 * cursor to its next candidate.
 */
static void
bark_knn_take(IndexScanDesc scan, BarkKnnCursor *cur)
{
	BarkKnnScanState *knn = ((BarkScanOpaque) scan->opaque)->knn;

	/* Free the previously-staged entry's buffers before taking a new one. */
	if (knn->emitTids)
		pfree(knn->emitTids);
	if (knn->emitKey)
		pfree(knn->emitKey);

	knn->emitTids = cur->tids;
	knn->nEmit = cur->ntids;
	knn->emitIdx = 0;
	knn->emitDist = cur->dist;
	knn->emitKey = cur->keytup;

	/*
	 * The cursor's tids/keytup buffers are now owned by the emit slot for the
	 * duration of this entry; hand the cursor fresh buffers so advancing it
	 * does not clobber what we are emitting.
	 */
	cur->tids = NULL;
	cur->ntidsAlloc = 0;
	cur->keytup = NULL;
	cur->have = false;

	bark_knn_advance(scan, cur);
}

/* Emit the member at emitIdx: its heap TID, and (IOS) its key tuple. */
static void
bark_knn_emit(IndexScanDesc scan)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	BarkKnnScanState *knn = so->knn;

	Assert(knn->emitIdx >= 0 && knn->emitIdx < knn->nEmit);
	scan->xs_heaptid = knn->emitTids[knn->emitIdx];
	scan->xs_recheck = false;

	if (scan->xs_want_itup)
	{
		IndexTuple	key = (IndexTuple) knn->emitKey;

		key->t_tid = knn->emitTids[knn->emitIdx];
		scan->xs_itup = key;
	}

	/*
	 * Report the exact distance for this tuple.  BARK is exact, so the
	 * distance is never lossy and the executor needs no recheck (it returns
	 * our tuples straight through in the order we hand them back).
	 */
	scan->xs_orderbyvals[0] = Float8GetDatum(knn->emitDist);
	scan->xs_orderbynulls[0] = false;
	scan->xs_recheckorderby = false;
}

void
bark_knn_rescan(IndexScanDesc scan, ScanKey orderbys, int norderbys)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	BarkKnnScanState *knn;
	ScanKey		ob;

	Assert(norderbys > 0);

	if (so->knn == NULL)
		so->knn = (BarkKnnScanState *) palloc0(sizeof(BarkKnnScanState));
	knn = so->knn;

	/* Release any buffers a previous iteration held. */
	if (BufferIsValid(knn->fwd.buf))
		ReleaseBuffer(knn->fwd.buf);
	if (BufferIsValid(knn->bwd.buf))
		ReleaseBuffer(knn->bwd.buf);
	knn->fwd.buf = knn->bwd.buf = InvalidBuffer;
	knn->fwd.have = knn->bwd.have = false;
	knn->fwd.primed = knn->bwd.primed = false;
	knn->fwd.backward = false;
	knn->bwd.backward = true;

	/* Free any entry left staged for emit by a previous iteration. */
	if (knn->emitTids)
		pfree(knn->emitTids);
	if (knn->emitKey)
		pfree(knn->emitKey);
	knn->emitTids = NULL;
	knn->emitKey = NULL;
	knn->nEmit = 0;
	knn->emitIdx = 0;

	/*
	 * Take the single ordering key.  Only the first ORDER BY <~> clause is
	 * used (see the ponytail note in bark.h); any further ordering keys are a
	 * B-tree can't refine with and are ignored, which is correct because a
	 * single scalar distance fully determines the order.
	 */
	ob = &orderbys[0];
	fmgr_info_copy(&knn->distfn, &ob->sk_func, CurrentMemoryContext);
	knn->distcollation = ob->sk_collation;
	knn->center = ob->sk_argument;
	knn->centernull = (ob->sk_flags & SK_ISNULL) != 0;
	knn->attno = ob->sk_attno;

	so->firstCall = true;
}

bool
bark_knn_gettuple(IndexScanDesc scan)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	BarkKnnScanState *knn = so->knn;

	if (so->firstCall)
	{
		so->firstCall = false;
		bark_knn_position(scan);
		bark_knn_advance(scan, &knn->fwd);
		bark_knn_advance(scan, &knn->bwd);
	}

	/* More members of the entry we last staged? Emit the next one. */
	if (knn->emitIdx + 1 < knn->nEmit)
	{
		knn->emitIdx++;
		bark_knn_emit(scan);
		return true;
	}
	knn->nEmit = 0;

	/*
	 * Merge: pick whichever side's buffered candidate is closer to the
	 * center.  A tie goes to the forward (>= center) side, which keeps a
	 * stable, deterministic order when a value equidistant on each side
	 * exists (e.g. center-1 and center+1).
	 */
	for (;;)
	{
		CHECK_FOR_INTERRUPTS();

		if (knn->fwd.have && knn->bwd.have)
		{
			if (knn->fwd.dist <= knn->bwd.dist)
				bark_knn_take(scan, &knn->fwd);
			else
				bark_knn_take(scan, &knn->bwd);
		}
		else if (knn->fwd.have)
			bark_knn_take(scan, &knn->fwd);
		else if (knn->bwd.have)
			bark_knn_take(scan, &knn->bwd);
		else
			return false;		/* both sides exhausted */

		if (knn->nEmit > 0)
		{
			knn->emitIdx = 0;
			bark_knn_emit(scan);
			return true;
		}
		/* Staged an entry with no live members (shouldn't happen); loop. */
	}
}

void
bark_knn_endscan(IndexScanDesc scan)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	BarkKnnScanState *knn;

	if (so == NULL || so->knn == NULL)
		return;
	knn = so->knn;

	if (BufferIsValid(knn->fwd.buf))
		ReleaseBuffer(knn->fwd.buf);
	if (BufferIsValid(knn->bwd.buf))
		ReleaseBuffer(knn->bwd.buf);
	if (knn->fwd.tids)
		pfree(knn->fwd.tids);
	if (knn->bwd.tids)
		pfree(knn->bwd.tids);
	if (knn->fwd.keytup)
		pfree(knn->fwd.keytup);
	if (knn->bwd.keytup)
		pfree(knn->bwd.keytup);
	if (knn->emitTids)
		pfree(knn->emitTids);
	if (knn->emitKey)
		pfree(knn->emitKey);
	pfree(knn);
	so->knn = NULL;
}
