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
 * Two cursors walk the leaf chain from the center leaf: a forward cursor over
 * keys >= const (ascending) and a backward cursor over keys < const
 * (descending).  Both distance streams are monotonically increasing in their
 * own direction, so merging them -- popping whichever side's next candidate
 * is closer at each step -- yields keys in exact increasing distance.
 *
 * Each cursor reads a leaf a page at a time, as the plain scan does
 * (barkscan.c's bark_readpage, after nbtree's _bt_readpage): it copies the
 * page's matching entries under one share lock and returns them from the
 * copy, so concurrent inserts and splits cannot make it repeat or skip
 * entries, and it moves on by the sibling link it saved when it read the
 * page.  A cursor reads its next page only when the merge cannot pick the
 * next candidate without it, that is, when the entries on its unread pages
 * might be closer than everything already copied on both sides.  So a scan
 * stopped by a LIMIT reads, on each side, at most one page whose entries are
 * all farther from const than the last row it returned.
 *
 * Correctness relies only on the key order the comparator defines and on the
 * ordering operator computing a distance monotone in |key - const| on each
 * side of const; the integer/bigint distance functions in barkutils.c satisfy
 * that.  BARK is exact, so the distances are exact (xs_recheckorderby stays
 * false) and the executor returns tuples straight through without a reorder
 * queue.
 *
 * The ordered column need not lead.  Inside a run of entries equal on every
 * column before it, the entries are in its order, so when the scan keys fix
 * those columns to one value each (an equality, or the extracted column's
 * single point: RUM's ORDER BY ts <=> x after a term qual; see
 * barkorderbyopcolumn), the cursors start at (fixed values, const) and stop
 * at the ends of that run.  A row's keys are distinct, so a point holds at
 * most one entry per row and no row repeats.
 *
 * A scalar B-tree orders on a single distance axis, so only one ordering key
 * (the first ORDER BY <~> clause) is meaningful: a second ordering key cannot
 * refine the order the way it would for a multidimensional GiST index, which
 * is why BARK answers one ordering key and ignores any others.  Supporting
 * several would need GiST's priority queue and would buy nothing here.
 *
 * No parallel KNN -- and this is intrinsic to a single-center outward merge,
 * not a deferred optimization.  The scan is driven from one point: at every
 * step it emits the globally nearest remaining key by comparing the two
 * frontier candidates, a decision that is inherently sequential.  The only
 * correct way to split it is to give the forward (>= const) side to one worker
 * and the backward (< const) side to another, but that is 2-way at best, and
 * the leader must still merge the two distance streams in order (it cannot
 * offload the ordering that is the whole point of the scan).  It would buy
 * nothing: a KNN scan is output-bounded -- it stops at the caller's LIMIT and
 * reads only the handful of leaves holding the k nearest keys -- so there is
 * no large page range to divide, and the parallel-scan page cursor (which
 * hands out leaves in index order, see BarkParallelScanDescData) does not even
 * model distance-order traversal.  The planner agrees: an amcanorderbyop
 * ordered scan is never given a parallel index path, so a KNN scan always runs
 * single-copy.  bark_knn_gettuple therefore ignores scan->parallel_scan; were a
 * single-copy gather to set it, the one worker still runs the full merge once
 * and the result is unchanged.
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
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "storage/predicate.h"
#include "utils/float.h"
#include "utils/rel.h"

/*
 * Compute the ordering distance of index tuple `itup` (resolved) from the
 * center constant.  A NULL key sorts last (distance +infinity), matching the
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

	/*
	 * A NULL center makes every distance NULL (the ordering operators are
	 * strict), which ORDER BY sorts last and as equal: every entry is as far
	 * as a NULL key.
	 */
	if (knn->centernull)
		return get_float8_infinity();
	datum = index_getattr(itup, knn->attno, tupdesc, &isnull);
	if (isnull)
		return get_float8_infinity();

	d = FunctionCall2Coll(&knn->distfn, knn->distcollation, datum,
						  knn->center);
	return DatumGetFloat8(d);
}

/*
 * Is entry itup in the run the scan's fixed columns pick, that is, equal to
 * knn->bound on every column before the ordered one?  Always when the
 * ordered column leads.
 */
static bool
bark_knn_in_run(IndexScanDesc scan, IndexTuple itup)
{
	BarkKnnScanState *knn = ((BarkScanOpaque) scan->opaque)->knn;
	TupleDesc	tupdesc = RelationGetDescr(scan->indexRelation);

	for (int i = 0; i < knn->attno - 1; i++)
	{
		bool		isnull;
		Datum		datum = index_getattr(itup, i + 1, tupdesc, &isnull);

		if (knn->bound.procs[i] == NULL)
		{
			if (!isnull)
				return false;
		}
		else if (isnull ||
				 DatumGetInt32(FunctionCall2Coll(knn->bound.procs[i],
												 knn->bound.collations[i],
												 datum,
												 knn->bound.args[i])) != 0)
			return false;
	}
	return true;
}

/*
 * Read the share-locked leaf `page` (block `blkno`) into cursor `cur`: copy
 * every matching entry from offset `offnum` in the cursor's direction to the
 * end of the page into cur->items, nearest to the center first, with its heap
 * TIDs and (index-only scan) its key; record the page's sibling link in the
 * cursor's direction; and advance cur->bound to the distance of the last
 * entry examined.  The caller drops the lock.
 */
static void
bark_knn_readpage(IndexScanDesc scan, BarkKnnCursor *cur, Page page,
				  BlockNumber blkno, OffsetNumber offnum)
{
	Relation	index = scan->indexRelation;
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	BarkPageOpaque opaque = BarkPageGetOpaque(page);
	OffsetNumber minoff = BarkPageFirstDataKey(opaque);
	OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
	bool		backward = cur->backward;

	Assert(BarkPageIsLeaf(opaque) && !BarkPageIgnore(opaque));
	cur->currPage = blkno;
	cur->nextPage = backward ? opaque->bark_prev : opaque->bark_next;
	cur->nitems = 0;
	cur->itemIndex = 0;
	cur->ntids = 0;
	cur->nextTupleOffset = 0;

	/*
	 * Predicate-lock the leaf for serializable transactions: a read here
	 * conflicts with a later insert onto the same page.
	 */
	PredicateLockPage(index, blkno, scan->xs_snapshot);

	for (; backward ? offnum >= minoff : offnum <= maxoff;
		 offnum = backward ? OffsetNumberPrev(offnum) : OffsetNumberNext(offnum))
	{
		BarkItemBuf ibuf;
		IndexTuple	itup = BarkPageGetItem(page, offnum, &ibuf);
		bool		fetched;
		IndexTuple	resolved = bark_scan_resolve(index, itup, &fetched);
		BarkKnnItem *item;
		int			ntids;

		/* Past the end of the fixed columns' run: nothing further matches. */
		if (!bark_knn_in_run(scan, resolved))
		{
			if (fetched)
				pfree(resolved);
			cur->nextPage = BARK_P_NONE;
			break;
		}

		/*
		 * An entry marked LP_DEAD (bark_killitems) still bounds the distance
		 * and the run; only its rows are not returned, as in bark_readpage.
		 */
		cur->bound = bark_knn_distance(scan, resolved);
		ntids = bark_entry_count_tids(itup);
		if (ntids == 0 || BarkEntryIsMarker(itup) ||
			(scan->ignore_killed_tuples &&
			 ItemIdIsDead(PageGetItemId(page, offnum))) ||
			!bark_tuple_matches(scan, resolved, 0))
		{
			if (fetched)
				pfree(resolved);
			continue;
		}

		if (cur->nitems >= cur->maxItems)
		{
			cur->maxItems = Max(cur->maxItems * 2, 64);
			if (cur->items == NULL)
				cur->items = MemoryContextAlloc(so->scanCxt,
												cur->maxItems * sizeof(BarkKnnItem));
			else
				cur->items = repalloc(cur->items,
									  cur->maxItems * sizeof(BarkKnnItem));
		}
		if (cur->ntids + ntids > cur->maxTids)
		{
			cur->maxTids = Max(cur->ntids + ntids, Max(cur->maxTids * 2, 256));
			if (cur->tids == NULL)
				cur->tids = MemoryContextAlloc(so->scanCxt,
											   cur->maxTids * sizeof(ItemPointerData));
			else
				cur->tids = repalloc(cur->tids,
									 cur->maxTids * sizeof(ItemPointerData));
		}

		item = &cur->items[cur->nitems];
		item->dist = cur->bound;
		item->firstTid = cur->ntids;
		item->ntids = bark_entry_get_tids(itup, cur->tids + cur->ntids,
										  cur->maxTids - cur->ntids);
		item->recheck = false;
		if (so->mk != NULL)
		{
			item->ntids = bark_mk_accept(scan, resolved, cur->tids + cur->ntids,
										 item->ntids, &item->recheck);
			if (item->ntids > 0)
				item->ntids = bark_mk_seen(scan, resolved,
										   cur->tids + cur->ntids,
										   item->ntids, false);
			if (item->ntids == 0)
			{
				if (fetched)
					pfree(resolved);
				continue;
			}
		}
		cur->nitems++;
		cur->ntids += item->ntids;
		item->tupleOffset = scan->xs_want_itup ?
			bark_save_tuple(so, &cur->tuples, &cur->tuplesSize,
							&cur->nextTupleOffset, itup, resolved) : 0;
		if (fetched)
			pfree(resolved);
	}
}

/*
 * Drop the lock on the leaf a cursor just read, and the pin too when
 * so->dropPin (the plain scan's rule, set by bark_rescan: only an index-only
 * scan or a non-MVCC scan keeps its pin).
 */
static void
bark_knn_drop_lock_and_maybe_pin(BarkScanOpaque so, BarkKnnCursor *cur,
								 Buffer buf)
{
	if (so->dropPin)
	{
		UnlockReleaseBuffer(buf);
		cur->buf = InvalidBuffer;
	}
	else
	{
		LockBuffer(buf, BUFFER_LOCK_UNLOCK);
		cur->buf = buf;
	}
}

/*
 * Read cursor `cur`'s next live page by the link saved when it read its
 * current one, stepping over deleted and half-dead pages.  A forward step
 * follows the right link; a backward step validates the left page first.  At
 * the end of the chain the cursor is left with no items and no next page.
 */
static void
bark_knn_steppage(IndexScanDesc scan, BarkKnnCursor *cur)
{
	Relation	index = scan->indexRelation;
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	BlockNumber blkno = cur->nextPage;
	BlockNumber lastcurrblkno = cur->currPage;

	Assert(blkno != BARK_P_NONE);
	if (BufferIsValid(cur->buf))
	{
		ReleaseBuffer(cur->buf);
		cur->buf = InvalidBuffer;
	}
	cur->nitems = cur->itemIndex = 0;
	cur->currPage = InvalidBlockNumber;
	cur->nextPage = BARK_P_NONE;

	while (blkno != BARK_P_NONE)
	{
		Buffer		buf;
		Page		page;
		BarkPageOpaque opaque;

		if (cur->backward)
		{
			buf = bark_lock_and_validate_left(index, &blkno, lastcurrblkno);
			if (!BufferIsValid(buf))
				return;
		}
		else
		{
			CHECK_FOR_INTERRUPTS();
			buf = ReadBuffer(index, blkno);
			LockBuffer(buf, BUFFER_LOCK_SHARE);
		}

		page = BufferGetPage(buf);
		opaque = BarkPageGetOpaque(page);
		if (!BarkPageIgnore(opaque))
		{
			bark_knn_readpage(scan, cur, page, blkno,
							  cur->backward ? PageGetMaxOffsetNumber(page) :
							  BarkPageFirstDataKey(opaque));
			bark_knn_drop_lock_and_maybe_pin(so, cur, buf);
			return;
		}
		lastcurrblkno = blkno;
		blkno = cur->backward ? opaque->bark_prev : opaque->bark_next;
		UnlockReleaseBuffer(buf);
	}
}

/*
 * Position both cursors around the center.  Descend to the first leaf that
 * could hold an entry at or after knn->bound, the fixed columns' values and
 * the center (a lower bound lands on the leftmost leaf of a run of equal
 * keys, so neither side skips a duplicate), then, under the share lock the
 * descent leaves, split that leaf at the first such entry: the forward
 * cursor reads the leaf from there toward higher keys, the backward cursor
 * from the entry before it toward lower keys.
 */
static void
bark_knn_position(IndexScanDesc scan)
{
	Relation	index = scan->indexRelation;
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	BarkKnnScanState *knn = so->knn;
	Buffer		buf;
	Page		page;
	BlockNumber blkno;
	OffsetNumber splitoff;

	/* One descent, counted as nbtree counts each _bt_first. */
	pgstat_count_index_scan(index);
	if (scan->instrument)
		scan->instrument->nsearches++;

	/*
	 * A NULL center returns every entry of the run, at distance infinity
	 * (bark_knn_distance), as a Sort returns every row for a NULL distance:
	 * the cursors start at the run's NULL keys and read it to both ends.
	 * Returning nothing would drop rows.
	 */
	if (knn->emptyrun)
		return;					/* a fixed value matches nothing */

	buf = bark_search_bound(index, so->keyinfo, &knn->bound, false);
	if (!BufferIsValid(buf))
	{
		/* Empty index.  A serializable scan must lock the whole relation. */
		PredicateLockRelation(index, scan->xs_snapshot);
		return;
	}

	page = BufferGetPage(buf);
	blkno = BufferGetBlockNumber(buf);
	splitoff = bark_binsrch_bound(index, so->keyinfo, &knn->bound, page);

	bark_knn_readpage(scan, &knn->fwd, page, blkno, splitoff);
	bark_knn_readpage(scan, &knn->bwd, page, blkno, OffsetNumberPrev(splitoff));

	/* Both cursors are on this leaf; each holds its own pin, if any. */
	if (!so->dropPin)
		IncrBufferRefCount(buf);
	bark_knn_drop_lock_and_maybe_pin(so, &knn->fwd, buf);
	if (!so->dropPin)
		knn->bwd.buf = buf;
}

/*
 * The merge: return the cursor whose current item is the next entry in
 * distance order, reading cursors' next pages as needed, or NULL when both
 * sides are exhausted.
 *
 * A cursor's next distance is exact when it has an unused item, and when it
 * has none but has pages left it is at least cur->bound: the distance stream
 * is monotone in the cursor's direction.  Take the side with the smaller of
 * the two, the forward side on a tie as before.  If that side has an item,
 * the other side cannot hold anything closer (nor, on a tie, anything that
 * would win it), so the item is next; otherwise its unread pages might, so
 * read its next page and decide again.
 */
static BarkKnnCursor *
bark_knn_next(IndexScanDesc scan)
{
	BarkKnnScanState *knn = ((BarkScanOpaque) scan->opaque)->knn;
	BarkKnnCursor *fwd = &knn->fwd;
	BarkKnnCursor *bwd = &knn->bwd;

	for (;;)
	{
		bool		fhave = fwd->itemIndex < fwd->nitems;
		bool		bhave = bwd->itemIndex < bwd->nitems;
		bool		fmore = fhave || fwd->nextPage != BARK_P_NONE;
		bool		bmore = bhave || bwd->nextPage != BARK_P_NONE;
		BarkKnnCursor *cur;

		if (!fmore && !bmore)
			return NULL;
		if (fmore && bmore)
		{
			double		fdist = fhave ? fwd->items[fwd->itemIndex].dist : fwd->bound;
			double		bdist = bhave ? bwd->items[bwd->itemIndex].dist : bwd->bound;

			cur = fdist <= bdist ? fwd : bwd;
		}
		else
			cur = fmore ? fwd : bwd;

		if (cur->itemIndex < cur->nitems)
			return cur;
		bark_knn_steppage(scan, cur);
	}
}

/* Return member emitIdx of the emitting cursor's current item. */
static void
bark_knn_emit(IndexScanDesc scan)
{
	BarkKnnScanState *knn = ((BarkScanOpaque) scan->opaque)->knn;
	BarkKnnCursor *cur = knn->emitCur;
	BarkKnnItem *item = &cur->items[cur->itemIndex];

	Assert(knn->emitIdx >= 0 && knn->emitIdx < item->ntids);
	scan->xs_heaptid = cur->tids[item->firstTid + knn->emitIdx];
	scan->xs_recheck = item->recheck;
	if (scan->xs_want_itup)
	{
		scan->xs_itup = (IndexTuple) (cur->tuples + item->tupleOffset);
		if (((BarkScanOpaque) scan->opaque)->mk != NULL)
			scan->xs_hitup = bark_mk_form_hitup(scan, scan->xs_itup);
	}

	/*
	 * Report the exact distance for this tuple.  BARK is exact, so the
	 * distance is never lossy and the executor needs no recheck (it returns
	 * our tuples straight through in the order we hand them back).
	 */
	scan->xs_orderbyvals[0] = Float8GetDatum(item->dist);
	scan->xs_orderbynulls[0] = false;
	scan->xs_recheckorderby = false;
}

/* Release a cursor's pin and forget its page, keeping its allocations. */
static void
bark_knn_reset_cursor(BarkKnnCursor *cur, bool backward)
{
	if (BufferIsValid(cur->buf))
		ReleaseBuffer(cur->buf);
	cur->buf = InvalidBuffer;
	cur->backward = backward;
	cur->currPage = InvalidBlockNumber;
	cur->nextPage = BARK_P_NONE;
	cur->bound = -get_float8_infinity();
	cur->nitems = cur->itemIndex = 0;
	cur->ntids = 0;
	cur->nextTupleOffset = 0;
}

static void
bark_knn_free_cursor(BarkKnnCursor *cur)
{
	bark_knn_reset_cursor(cur, cur->backward);
	if (cur->items)
		pfree(cur->items);
	if (cur->tids)
		pfree(cur->tids);
	if (cur->tuples)
		pfree(cur->tuples);
}

/*
 * Put the value the scan keys fix column col to into knn->bound: the
 * extracted column's one point (bark_mk_point), or a plain equality's
 * argument, compared by its ORDER proc.  A fixed value that matches nothing
 * (an equality with NULL, a point query with no boundary) sets
 * knn->emptyrun.  False when nothing fixes the column.  Further keys on the
 * column only filter, as in any scan.
 */
static bool
bark_knn_fix_column(IndexScanDesc scan, AttrNumber col)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	BarkKnnScanState *knn = so->knn;
	BarkScanBound *bound = &knn->bound;
	int			point = bark_mk_point(scan, col, &bound->args[col - 1],
									  &bound->procs[col - 1],
									  &bound->collations[col - 1]);

	if (point != 0)
	{
		if (point == 2)
			knn->emptyrun = true;
		return point > 0;
	}
	for (int i = 0; i < so->numberOfKeys; i++)
	{
		ScanKey		sk = &scan->keyData[i];

		if (sk->sk_attno != col || sk->sk_strategy != BTEqualStrategyNumber ||
			(sk->sk_flags & (SK_SEARCHARRAY | SK_ROW_HEADER |
							 SK_SEARCHNULL | SK_SEARCHNOTNULL)))
			continue;
		if (sk->sk_flags & SK_ISNULL)
		{
			knn->emptyrun = true;
			return true;
		}
		if (!OidIsValid(so->keyCmp[i].fn_oid))
			continue;
		bound->args[col - 1] = sk->sk_argument;
		bound->procs[col - 1] = &so->keyCmp[i];
		bound->collations[col - 1] = sk->sk_collation;
		return true;
	}
	return false;
}

void
bark_knn_rescan(IndexScanDesc scan, ScanKey orderbys, int norderbys)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	BarkKnnScanState *knn;
	ScanKey		ob;

	Assert(norderbys > 0);

	if (so->knn == NULL)
		so->knn = (BarkKnnScanState *)
			MemoryContextAllocZero(so->scanCxt, sizeof(BarkKnnScanState));
	knn = so->knn;

	bark_knn_reset_cursor(&knn->fwd, false);
	bark_knn_reset_cursor(&knn->bwd, true);
	knn->emitCur = NULL;
	knn->emitIdx = 0;

	/*
	 * Take the single ordering key.  Only the first ORDER BY <~> clause is
	 * used (see the design note in bark.h); any further ordering keys are
	 * ones a scalar B-tree cannot refine with and are ignored, which is
	 * correct because a single scalar distance fully determines the order.
	 */
	ob = &orderbys[0];
	fmgr_info_copy(&knn->distfn, &ob->sk_func, so->scanCxt);
	knn->distcollation = ob->sk_collation;
	knn->center = ob->sk_argument;
	knn->centernull = (ob->sk_flags & SK_ISNULL) != 0;
	knn->attno = ob->sk_attno;

	/*
	 * The bound the scan descends to: the values the scan keys fix on the
	 * columns before the ordered one (barkorderbyopcolumn chose the column
	 * so that they do), then the center.
	 */
	memset(&knn->bound, 0, sizeof(knn->bound));
	knn->emptyrun = false;
	for (AttrNumber col = 1; col < knn->attno; col++)
		if (!bark_knn_fix_column(scan, col))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("ordering operator on column %d of BARK index \"%s\" needs every earlier column fixed by the scan keys",
							knn->attno, RelationGetRelationName(scan->indexRelation))));
	knn->bound.args[knn->attno - 1] = knn->center;
	knn->bound.procs[knn->attno - 1] = knn->centernull ? NULL :
		&so->keyinfo->cols[knn->attno - 1].cmp;
	knn->bound.collations[knn->attno - 1] =
		so->keyinfo->cols[knn->attno - 1].collation;
	knn->bound.nkeys = knn->attno;

	so->firstCall = true;
}

bool
bark_knn_gettuple(IndexScanDesc scan)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	BarkKnnScanState *knn = so->knn;
	BarkKnnCursor *cur;

	if (so->firstCall)
	{
		so->firstCall = false;
		bark_knn_position(scan);
	}

	/* More members of the entry being returned?  Return the next one. */
	cur = knn->emitCur;
	if (cur != NULL)
	{
		if (++knn->emitIdx < cur->items[cur->itemIndex].ntids)
		{
			bark_knn_emit(scan);
			return true;
		}
		cur->itemIndex++;
		knn->emitCur = NULL;
	}

	CHECK_FOR_INTERRUPTS();
	cur = bark_knn_next(scan);
	if (cur == NULL)
		return false;			/* both sides exhausted */
	knn->emitCur = cur;
	knn->emitIdx = 0;
	bark_knn_emit(scan);
	return true;
}

void
bark_knn_endscan(IndexScanDesc scan)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;

	if (so == NULL || so->knn == NULL)
		return;
	bark_knn_free_cursor(&so->knn->fwd);
	bark_knn_free_cursor(&so->knn->bwd);
	pfree(so->knn);
	so->knn = NULL;
}
