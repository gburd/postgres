/*-------------------------------------------------------------------------
 *
 * barkscan.c
 *	  Index scan for the BARK index access method.
 *
 * A BARK scan positions on a leaf and walks the right-link chain, returning
 * the heap TID of each entry that satisfies the scan keys.  When the keys
 * provide a lower bound on the leading index columns (an =, >, or >= qual, or
 * a `col = ANY(array)` SAOP whose smallest element bounds column 1), the scan
 * descends the tree to the first leaf that can contain a match; otherwise it
 * starts at the leftmost leaf.  It stops early once the leading index columns
 * pass an upper bound (an =, <, or <= qual).
 *
 * Modeled on nbtree's scan (nbtsearch.c _bt_first / _bt_next / _bt_readpage),
 * simplified for the SINGLE entry shape: every leaf entry is one heap TID in
 * t_tid.  The index is exact, so no heap recheck is required.
 *
 * ScalarArrayOp (SAOP) quals (`col = ANY(array)` / `col IN (...)`) are pushed
 * into a single scan: bark_rescan sorts and de-duplicates each array into the
 * index's key order (see BarkArrayKeyState in bark.h) and the scan visits the
 * matching keys in index order -- a merged sequence of equality scans.  Every
 * array key filters per tuple by membership; an array on the leading column
 * also drives positioning, so the scan seeks to each element in turn rather
 * than reading the whole index.
 *
 * ponytail: positioning uses only a first-column lower bound; a multi-column
 * or upper-bound-only qual starts at the leftmost leaf and relies on the
 * per-tuple key test.  Sharper positioning is a later optimization, not a
 * correctness matter.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/bark/barkscan.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bark.h"
#include "access/relscan.h"
#include "access/skey.h"
#include "catalog/pg_type.h"
#include "lib/qunique.h"
#include "miscadmin.h"
#include "nodes/tidbitmap.h"
#include "storage/bufmgr.h"
#include "storage/lwlock.h"
#include "storage/predicate.h"
#include "utils/array.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/wait_event.h"

/*
 * Resolve a leaf entry to a tuple whose key and INCLUDE attributes can be read
 * with index_getattr.  For an OVERSIZED entry the attributes live out of line,
 * so fetch the full tuple from the overflow chain; the caller pfrees the result
 * when *fetched is set.  Every other shape carries its attributes inline, so
 * the entry is returned unchanged.
 */
static IndexTuple
bark_scan_resolve(Relation index, IndexTuple itup, bool *fetched)
{
	if (BarkEntryGetShape(itup) == BARK_SHAPE_OVERSIZED)
	{
		*fetched = true;
		return bark_fetch_oversized(index, itup);
	}
	*fetched = false;
	return itup;
}

/*
 * ---------------------------------------------------------------------------
 * ScalarArrayOp (SAOP) support
 *
 * A SAOP scankey (SK_SEARCHARRAY) carries an array Datum in sk_argument.  We
 * sort its elements into the index's key order for that column and remove
 * duplicates once, at rescan time, into a BarkArrayKeyState.  The scan then
 * (a) filters every tuple by array membership and (b) for an array on the
 * leading column, seeks to each element in turn so it visits the matching
 * keys in index order without reading the whole index -- a merged sequence of
 * equality scans.
 * ---------------------------------------------------------------------------
 */

/* Per-column comparator state for sorting/searching array elements. */
typedef struct BarkArraySortCtx
{
	FmgrInfo   *cmp;			/* support-1 three-way comparator */
	Oid			collation;		/* collation to pass it */
	bool		reverse;		/* DESC column: invert the result */
} BarkArraySortCtx;

/* qsort_arg comparator: order two array element Datums as the index does. */
static int
bark_array_cmp(const void *a, const void *b, void *arg)
{
	BarkArraySortCtx *ctx = (BarkArraySortCtx *) arg;
	Datum		da = *((const Datum *) a);
	Datum		db = *((const Datum *) b);
	int32		c = DatumGetInt32(FunctionCall2Coll(ctx->cmp, ctx->collation,
												da, db));

	if (ctx->reverse)
		INVERT_COMPARE_RESULT(c);
	return c;
}

/* Is `datum` one of the array key's elements?  Binary search over the sort. */
static bool
bark_array_contains(BarkScanOpaque so, BarkArrayKeyState *ak, Datum datum)
{
	BarkKeyColumn *col = &so->keyinfo->cols[ak->attno - 1];
	BarkArraySortCtx ctx;
	int			lo = 0;
	int			hi = ak->nelems - 1;

	ctx.cmp = &col->cmp;
	ctx.collation = col->collation;
	ctx.reverse = col->reverse;

	while (lo <= hi)
	{
		int			mid = lo + ((hi - lo) / 2);
		int			c = bark_array_cmp(&datum, &ak->elems[mid], &ctx);

		if (c == 0)
			return true;
		else if (c < 0)
			hi = mid - 1;
		else
			lo = mid + 1;
	}
	return false;
}

/*
 * Preprocess every SK_SEARCHARRAY scankey into a BarkArrayKeyState: deconstruct
 * its array, sort the elements into the index's key order for the column, drop
 * duplicates, and record the array key that constrains the leading column.  A
 * NULL array element is dropped (a NULL never satisfies an equality qual).  An
 * empty array leaves nelems == 0, which makes the scan return nothing.
 *
 * Called from bark_rescan; freed by bark_free_array_keys (endscan/rescan).
 */
static void
bark_setup_array_keys(IndexScanDesc scan)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	int			narrays = 0;

	so->arrayKeys = NULL;
	so->numArrayKeys = 0;
	so->leadArray = NULL;
	so->arrayDone = false;

	for (int i = 0; i < scan->numberOfKeys; i++)
		if (scan->keyData[i].sk_flags & SK_SEARCHARRAY)
			narrays++;
	if (narrays == 0)
		return;

	so->arrayKeys = (BarkArrayKeyState *)
		palloc0(narrays * sizeof(BarkArrayKeyState));

	for (int i = 0; i < scan->numberOfKeys; i++)
	{
		ScanKey		sk = &scan->keyData[i];
		BarkArrayKeyState *ak;
		ArrayType  *arr;
		int16		elmlen;
		bool		elmbyval;
		char		elmalign;
		Datum	   *rawelems;
		bool	   *rawnulls;
		int			nrawelems;
		int			nelems;
		BarkKeyColumn *col;
		BarkArraySortCtx ctx;

		if (!(sk->sk_flags & SK_SEARCHARRAY))
			continue;

		ak = &so->arrayKeys[so->numArrayKeys++];
		ak->scankeyidx = i;
		ak->attno = sk->sk_attno;
		ak->cur = 0;

		/* A NULL array argument matches nothing: leave nelems == 0. */
		if (sk->sk_flags & SK_ISNULL)
		{
			ak->elems = NULL;
			ak->nelems = 0;
			ak->elmbyval = true;
			if (ak->attno == 1)
				so->leadArray = ak;
			continue;
		}

		arr = DatumGetArrayTypeP(sk->sk_argument);
		get_typlenbyvalalign(ARR_ELEMTYPE(arr), &elmlen, &elmbyval, &elmalign);
		ak->elmbyval = elmbyval;
		deconstruct_array(arr, ARR_ELEMTYPE(arr), elmlen, elmbyval, elmalign,
						  &rawelems, &rawnulls, &nrawelems);

		/* Drop NULL elements (a NULL never satisfies an equality qual). */
		nelems = 0;
		for (int e = 0; e < nrawelems; e++)
		{
			if (rawnulls[e])
				continue;
			rawelems[nelems++] = rawelems[e];
		}

		col = &so->keyinfo->cols[ak->attno - 1];
		ctx.cmp = &col->cmp;
		ctx.collation = col->collation;
		ctx.reverse = col->reverse;

		if (nelems > 1)
		{
			qsort_arg(rawelems, nelems, sizeof(Datum), bark_array_cmp, &ctx);
			nelems = qunique_arg(rawelems, nelems, sizeof(Datum),
								 bark_array_cmp, &ctx);
		}

		ak->elems = rawelems;	/* deconstruct_array palloc'd this */
		ak->nelems = nelems;
		pfree(rawnulls);

		if (ak->attno == 1)
			so->leadArray = ak;
	}

	/*
	 * If the leading array is empty (an empty IN-list, or all-NULL), the whole
	 * scan matches nothing; mark it done so positioning returns immediately.
	 */
	if (so->leadArray != NULL && so->leadArray->nelems == 0)
		so->arrayDone = true;
}

/* Release SAOP state (endscan, and before rebuilding it on rescan). */
static void
bark_free_array_keys(BarkScanOpaque so)
{
	if (so->arrayKeys == NULL)
		return;
	for (int i = 0; i < so->numArrayKeys; i++)
		if (so->arrayKeys[i].elems)
			pfree(so->arrayKeys[i].elems);
	pfree(so->arrayKeys);
	so->arrayKeys = NULL;
	so->numArrayKeys = 0;
	so->leadArray = NULL;
	so->arrayDone = false;
}

/*
 * Test one index tuple against all scan keys.  Returns true when every key is
 * satisfied.  A NULL index value never satisfies an ordinary (non-IS NULL)
 * comparison key.  A SK_SEARCHARRAY key is satisfied when the tuple's value is
 * a member of its (preprocessed, sorted) array.
 */
static bool
bark_tuple_matches(IndexScanDesc scan, IndexTuple itup)
{
	Relation	index = scan->indexRelation;
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	TupleDesc	tupdesc = RelationGetDescr(index);
	int			nextarray = 0;

	for (int i = 0; i < scan->numberOfKeys; i++)
	{
		ScanKey		key = &scan->keyData[i];
		Datum		datum;
		bool		isnull;

		datum = index_getattr(itup, key->sk_attno, tupdesc, &isnull);

		/*
		 * SAOP key: satisfied when the value is a member of the array (NULL
		 * never matches an equality).  The array keys were preprocessed in
		 * scankey order, so step the matching BarkArrayKeyState in lockstep.
		 */
		if (key->sk_flags & SK_SEARCHARRAY)
		{
			BarkArrayKeyState *ak = &so->arrayKeys[nextarray++];

			Assert(ak->scankeyidx == i);
			if (isnull)
				return false;
			if (!bark_array_contains(so, ak, datum))
				return false;
			continue;
		}

		if (key->sk_flags & SK_ISNULL)
		{
			/* IS NULL / IS NOT NULL searches are not handled yet. */
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
			/* Ordinary key with NULL argument: never matches. */
			return false;
		}

		if (isnull)
			return false;		/* NULL index value fails a comparison key */

		if (!DatumGetBool(FunctionCall2Coll(&key->sk_func, key->sk_collation,
											datum, key->sk_argument)))
			return false;
	}
	return true;
}

/*
 * Build an index-tuple search key from a lower bound on the first index
 * column, for descending to the first possibly-matching leaf.  Returns NULL
 * (start at the leftmost leaf) when no bound on column 1 is present.
 *
 * The column-1 bound comes from an =, >, or >= qual, or -- when a SAOP
 * (`col = ANY(array)`) constrains column 1 -- from that array's current
 * element (so->leadArray->cur), so a merged array scan descends straight to
 * the element it is about to visit instead of starting leftmost.
 *
 * The bound is formed as a pivot carrying only the leading column (natts = 1).
 * This matters on a multi-column index: a lower bound must leave the trailing
 * columns at minus-infinity so the descent lands at or before the first match,
 * never past it.  A pivot truncated to one attribute is exactly minus-infinity
 * on the dropped columns -- bark_compare_itups orders a tuple with fewer key
 * attributes before one that agrees on the shared attributes but has more.
 * (Padding the trailing columns with NULL instead would, under the default
 * NULLS LAST ordering, sort as plus-infinity and overshoot the whole run of
 * matching rows.)
 */
static IndexTuple
bark_make_lower_bound(IndexScanDesc scan)
{
	Relation	index = scan->indexRelation;
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	TupleDesc	tupdesc = RelationGetDescr(index);
	int			natts;
	Datum	   *values;
	bool	   *isnull;
	Datum		bound = (Datum) 0;
	bool		have_bound = false;
	IndexTuple	full;
	IndexTuple	key;
	Size		fulllen;

	/*
	 * A leading-column SAOP drives positioning: descend to its current
	 * element.  (nelems == 0 was handled as arrayDone before we get here.)
	 */
	if (so->leadArray != NULL && so->leadArray->nelems > 0)
	{
		bound = so->leadArray->elems[so->leadArray->cur];
		have_bound = true;
	}
	else
	{
		for (int i = 0; i < scan->numberOfKeys; i++)
		{
			ScanKey		sk = &scan->keyData[i];

			if (sk->sk_attno != 1 || (sk->sk_flags & SK_ISNULL) ||
				(sk->sk_flags & SK_SEARCHARRAY))
				continue;
			if (sk->sk_strategy == BTEqualStrategyNumber ||
				sk->sk_strategy == BTGreaterStrategyNumber ||
				sk->sk_strategy == BTGreaterEqualStrategyNumber)
			{
				bound = sk->sk_argument;
				have_bound = true;
				break;
			}
		}
	}

	if (!have_bound)
		return NULL;

	/*
	 * Form the leading-column value as a (possibly oversized) tuple, then
	 * truncate it to a one-attribute pivot.  bark_form_full_tuple reads one
	 * entry per descriptor attribute and forms it without the 8191-byte cap, so
	 * an oversized search argument does not error here; the trailing attributes
	 * are left NULL only because the former requires a value for every column,
	 * and are then physically dropped by the truncation.  bark_search descends
	 * with the pivot; bark_compare_itups compares it (fetching an oversized
	 * leaf entry's overflow chain as needed), so the descent lands correctly
	 * even for an oversized bound.
	 */
	natts = IndexRelationGetNumberOfAttributes(index);
	values = (Datum *) palloc(natts * sizeof(Datum));
	isnull = (bool *) palloc(natts * sizeof(bool));
	values[0] = bound;
	isnull[0] = false;
	for (int c = 1; c < natts; c++)
	{
		values[c] = (Datum) 0;
		isnull[c] = true;
	}
	full = bark_form_full_tuple(tupdesc, values, isnull, &fulllen);
	pfree(values);
	pfree(isnull);

	/*
	 * Truncate to a one-attribute pivot (minus-infinity on the trailing
	 * columns).  index_truncate_tuple cannot handle an oversized leading value;
	 * in that rare case leave the bound untruncated -- it is still a safe lower
	 * bound for a single-column index, and an oversized leading key on a
	 * multi-column index is not supported for sharper positioning here.
	 */
	if (natts > 1 && !bark_len_is_oversized(fulllen))
	{
		key = index_truncate_tuple(tupdesc, full, 1);
		BarkPivotSetNAtts(key, 1);
		pfree(full);
	}
	else if (natts > 1)
	{
		/* Oversized leading key: mark the full tuple as a 1-attr pivot. */
		key = full;
		BarkPivotSetNAtts(key, 1);
	}
	else
		key = full;				/* single-column index: no trailing columns */

	return key;
}

IndexScanDesc
bark_beginscan(Relation index, int nkeys, int norderbys)
{
	IndexScanDesc scan = RelationGetIndexScan(index, nkeys, norderbys);
	BarkScanOpaque so = palloc0_object(BarkScanOpaqueData);

	so->keyinfo = bark_build_keyinfo(index);
	so->currentBuffer = InvalidBuffer;
	so->lastOffset = InvalidOffsetNumber;
	so->firstCall = true;
	so->parallelReleased = false;
	so->currTuple = NULL;
	so->currTupleSize = 0;
	so->memberTids = NULL;
	so->nMembersAlloc = 0;
	so->nMembers = 0;
	so->memberIdx = 0;
	so->knn = NULL;

	/*
	 * Set up the index tuple descriptor for index-only scans.  The scratch
	 * buffer that holds a returned tuple is allocated lazily on the first read
	 * (bark_position), because xs_want_itup is set after beginscan returns.
	 */
	scan->xs_itupdesc = RelationGetDescr(index);

	/*
	 * An ordered-operator (KNN) scan reports a per-tuple distance in
	 * xs_orderbyvals/xs_orderbynulls; RelationGetIndexScan only allocates
	 * orderByData, so allocate these here (as GiST does) when the scan has
	 * ORDER BY keys.
	 */
	if (norderbys > 0)
	{
		scan->xs_orderbyvals = palloc0_array(Datum, norderbys);
		scan->xs_orderbynulls = palloc_array(bool, norderbys);
		memset(scan->xs_orderbynulls, true, sizeof(bool) * norderbys);
	}

	scan->opaque = so;
	return scan;
}

void
bark_rescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
			ScanKey orderbys, int norderbys)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;

	if (BufferIsValid(so->currentBuffer))
	{
		ReleaseBuffer(so->currentBuffer);
		so->currentBuffer = InvalidBuffer;
	}
	so->lastOffset = InvalidOffsetNumber;
	so->firstCall = true;
	so->parallelReleased = false;
	so->nMembers = 0;
	so->memberIdx = 0;

	if (scankey && nscankeys > 0)
		memcpy(scan->keyData, scankey, nscankeys * sizeof(ScanKeyData));

	/*
	 * Rebuild ScalarArrayOp state from the (possibly new) scan keys: sort and
	 * de-duplicate each SK_SEARCHARRAY array once, so the scan can visit the
	 * matching keys in index order.  A plain scan builds nothing here.
	 */
	bark_free_array_keys(so);
	bark_setup_array_keys(scan);

	/*
	 * An ordered-operator (KNN) scan carries ORDER BY <~> keys; copy them in
	 * and hand them to the KNN machinery, which runs the outward two-sided
	 * merge in bark_gettuple.  A plain scan (norderbys == 0) is untouched and
	 * takes exactly the same path as before.
	 */
	if (scan->numberOfOrderBys > 0 && orderbys && norderbys > 0)
	{
		memcpy(scan->orderByData, orderbys, norderbys * sizeof(ScanKeyData));
		bark_knn_rescan(scan, scan->orderByData, norderbys);
	}
}

/*
 * Find the block number of the leaf the scan should start on, without keeping
 * the buffer: leftmost possibly-matching leaf for a forward scan (via a
 * first-column lower bound when available), else the leftmost leaf; rightmost
 * leaf for a backward scan.  Returns BARK_P_NONE for an empty index.
 *
 * ponytail: a backward scan always starts rightmost rather than descending to
 * an upper bound first; sharper backward positioning is an optimization, not a
 * correctness matter (symmetric to the forward no-lower-bound case).
 */
static BlockNumber
bark_find_start_block(IndexScanDesc scan, ScanDirection dir)
{
	Relation	index = scan->indexRelation;
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	bool		backward = ScanDirectionIsBackward(dir);
	IndexTuple	lower = backward ? NULL : bark_make_lower_bound(scan);
	BlockNumber startblk;

	if (lower != NULL)
	{
		Buffer		buf = bark_search(index, so->keyinfo, lower, false, false,
									  NULL);

		pfree(lower);
		if (buf == InvalidBuffer)
			return BARK_P_NONE;	/* empty index */
		startblk = BufferGetBlockNumber(buf);
		UnlockReleaseBuffer(buf);	/* search left it share-locked */
		return startblk;
	}

	/*
	 * No usable bound: walk down the spine to the extreme leaf -- leftmost for
	 * a forward scan, rightmost for a backward scan.
	 */
	{
		BlockNumber blkno;
		Buffer		metabuf = ReadBuffer(index, BARK_METAPAGE);

		LockBuffer(metabuf, BUFFER_LOCK_SHARE);
		blkno = BarkPageGetMeta(BufferGetPage(metabuf))->bark_root;
		UnlockReleaseBuffer(metabuf);

		startblk = BARK_P_NONE;
		while (blkno != BARK_P_NONE)
		{
			Buffer		buf = ReadBuffer(index, blkno);
			Page		page;
			BarkPageOpaque opaque;

			LockBuffer(buf, BUFFER_LOCK_SHARE);
			page = BufferGetPage(buf);
			opaque = BarkPageGetOpaque(page);
			if (BarkPageIsLeaf(opaque))
			{
				startblk = blkno;
				UnlockReleaseBuffer(buf);
				break;
			}
			/* Follow the first (forward) or last (backward) downlink. */
			{
				OffsetNumber off = backward ? PageGetMaxOffsetNumber(page)
					: BarkPageFirstDataKey(opaque);
				ItemId		iid = PageGetItemId(page, off);
				IndexTuple	itup = (IndexTuple) PageGetItem(page, iid);
				BlockNumber child = BarkEntryGetDownLink(itup);

				UnlockReleaseBuffer(buf);
				blkno = child;
			}
		}
		return startblk;
	}
}

/* ---------------------------------------------------------------------------
 * Parallel scan coordination (modeled on nbtree's _bt_parallel_seize/release)
 *
 * A parallel BARK scan hands out leaf pages one at a time from a shared
 * cursor.  Only one worker advances the cursor at a time: a worker seizes the
 * scan, reads the page it was handed, then releases the page's sibling (in the
 * scan direction) as the next page for another worker.  Because the direction
 * of a parallel scan never changes, a single next-page cursor is all the
 * coordination needs.
 * ---------------------------------------------------------------------------
 */

static BarkParallelScanDesc
bark_get_parallel_desc(IndexScanDesc scan)
{
	ParallelIndexScanDesc pscan = scan->parallel_scan;

	return (BarkParallelScanDesc) OffsetToPointer(pscan, pscan->ps_offset_am);
}

/*
 * Mark the parallel scan complete so no worker waits forever for a next page.
 */
static void
bark_parallel_done(IndexScanDesc scan)
{
	BarkParallelScanDesc bps;

	if (scan->parallel_scan == NULL)
		return;
	bps = bark_get_parallel_desc(scan);

	LWLockAcquire(&bps->bps_lock, LW_EXCLUSIVE);
	if (bps->bps_state != BARK_PARALLEL_DONE)
		bps->bps_state = BARK_PARALLEL_DONE;
	LWLockRelease(&bps->bps_lock);
	ConditionVariableBroadcast(&bps->bps_cv);
}

/*
 * Seize the parallel scan to obtain the next leaf block to scan.
 *
 * Returns true and sets *next_block when this worker should scan a page:
 *   - *next_block == a valid leaf block: scan it.
 *   - *next_block == BARK_P_NONE: this worker found the scan uninitialized and
 *     has been made the positioner -- it must descend to the start leaf and
 *     call bark_parallel_release with the block it finds.
 * Returns false when the scan is finished (no pages remain).
 *
 * Unlike nbtree's designated-first protocol, any worker that finds the scan
 * NOT_INITIALIZED becomes the positioner (exactly one wins, under the lock);
 * no worker ever sleeps waiting for someone else to initialize, which is what
 * previously deadlocked a parallel scan whose consumer (a merge join, a LIMIT)
 * stopped pulling before the chain was exhausted.
 */
static bool
bark_parallel_seize(IndexScanDesc scan, BlockNumber *next_block)
{
	BarkParallelScanDesc bps = bark_get_parallel_desc(scan);
	bool		exit_loop = false;
	bool		status = true;
	bool		endscan = false;

	*next_block = InvalidBlockNumber;

	for (;;)
	{
		LWLockAcquire(&bps->bps_lock, LW_EXCLUSIVE);

		if (bps->bps_state == BARK_PARALLEL_DONE)
		{
			status = false;		/* scan already finished */
		}
		else if (bps->bps_state == BARK_PARALLEL_NOT_INITIALIZED)
		{
			/*
			 * First worker to reach an uninitialized scan positions it.  We
			 * win the lock, so we are that worker: take ADVANCING and signal
			 * via BARK_P_NONE that the caller must descend and release.
			 */
			bps->bps_state = BARK_PARALLEL_ADVANCING;
			*next_block = BARK_P_NONE;
			exit_loop = true;
		}
		else if (bps->bps_state == BARK_PARALLEL_IDLE &&
				 bps->bps_nextPage == BARK_P_NONE)
		{
			/* Cursor exhausted: end the scan. */
			status = false;
			endscan = true;
		}
		else if (bps->bps_state == BARK_PARALLEL_IDLE)
		{
			/* Seized: claim the next page and mark the scan as advancing. */
			bps->bps_state = BARK_PARALLEL_ADVANCING;
			*next_block = bps->bps_nextPage;
			exit_loop = true;
		}

		LWLockRelease(&bps->bps_lock);
		if (exit_loop || !status)
			break;
		/* Another worker is advancing; wait for it to release a page. */
		ConditionVariableSleep(&bps->bps_cv, WAIT_EVENT_BARK_PAGE);
	}
	ConditionVariableCancelSleep();

	if (endscan)
		bark_parallel_done(scan);

	return status;
}

/*
 * Release the parallel scan: publish next_block as the page another worker
 * should scan, and mark the scan idle.  next_block is BARK_P_NONE at the end
 * of the chain, which bark_parallel_seize treats as end-of-scan.
 */
static void
bark_parallel_release(IndexScanDesc scan, BlockNumber next_block)
{
	BarkParallelScanDesc bps = bark_get_parallel_desc(scan);

	LWLockAcquire(&bps->bps_lock, LW_EXCLUSIVE);
	bps->bps_nextPage = next_block;
	bps->bps_state = BARK_PARALLEL_IDLE;
	LWLockRelease(&bps->bps_lock);
	ConditionVariableSignal(&bps->bps_cv);
}

/*
 * Position the scan on its first leaf.
 *
 * Serial: find the start leaf and pin it.  Parallel: seize the shared cursor;
 * the first worker to seize descends to the start leaf and releases it so the
 * whole pool (itself included) then claims pages from the cursor.  Leaves the
 * leaf pinned but not locked; bark_gettuple locks per page read.
 */
static void
bark_position(IndexScanDesc scan, ScanDirection dir)
{
	Relation	index = scan->indexRelation;
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	BlockNumber startblk;

	/* Allocate the index-only-scan scratch buffer on first use. */
	if (scan->xs_want_itup && so->currTuple == NULL)
	{
		so->currTuple = palloc(BLCKSZ);
		so->currTupleSize = BLCKSZ;
	}

	so->firstCall = false;
	so->parallelReleased = false;	/* the page we claim below has not yet
									 * published its sibling to the cursor */
	so->lastOffset = InvalidOffsetNumber;	/* set on first page read */

	/* A leading SAOP with no elements (empty IN-list) matches nothing. */
	if (so->arrayDone)
	{
		so->currentBuffer = InvalidBuffer;
		return;
	}

	if (scan->parallel_scan != NULL)
	{
		BlockNumber next;

		/* Seize the scan; a NOT_INITIALIZED scan makes this worker position it. */
		if (!bark_parallel_seize(scan, &next))
		{
			so->currentBuffer = InvalidBuffer;	/* scan already finished */
			return;
		}

		if (next == BARK_P_NONE)
		{
			/* We won the right to position: descend and publish the start. */
			startblk = bark_find_start_block(scan, dir);
			bark_parallel_release(scan, startblk);
			/* Now claim a page like any other worker. */
			if (!bark_parallel_seize(scan, &next))
			{
				so->currentBuffer = InvalidBuffer;
				return;
			}
		}
		startblk = next;
	}
	else
		startblk = bark_find_start_block(scan, dir);

	so->currentBuffer = (startblk == BARK_P_NONE) ? InvalidBuffer
		: ReadBuffer(index, startblk);
}

/*
 * Decode the heap locators of a matched leaf entry into so->memberTids (in
 * ascending order) and, for an index-only scan, build the key-only tuple the
 * scan will hand back for each member.  A SINGLE entry yields one locator;
 * LIST and POSTING entries expand into many.  The members are then emitted one
 * per bark_gettuple call by bark_emit_member.
 */
static void
bark_load_members(IndexScanDesc scan, IndexTuple itup, IndexTuple resolved)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	int			n = bark_entry_count_tids(itup);

	if (so->memberTids == NULL || n > so->nMembersAlloc)
	{
		if (so->memberTids)
			pfree(so->memberTids);
		so->nMembersAlloc = Max(n, 16);
		so->memberTids = (ItemPointer)
			palloc(so->nMembersAlloc * sizeof(ItemPointerData));
	}
	so->nMembers = bark_entry_get_tids(itup, so->memberTids, so->nMembersAlloc);

	/*
	 * Index-only scan: every member shares this entry's key, so build a clean
	 * key-only tuple once (dropping any LIST/POSTING body); bark_emit_member
	 * patches its t_tid per member.  `resolved` is the deformable tuple -- the
	 * entry itself for an inline shape, or the full tuple fetched from the
	 * overflow chain for an OVERSIZED entry -- so an oversized key or INCLUDE
	 * payload is returned correctly by an index-only scan.
	 */
	if (scan->xs_want_itup)
	{
		Relation	index = scan->indexRelation;
		TupleDesc	tupdesc = RelationGetDescr(index);
		Datum		values[INDEX_MAX_KEYS];
		bool		isnull[INDEX_MAX_KEYS];
		IndexTuple	key;
		Size		sz;

		index_deform_tuple(resolved, tupdesc, values, isnull);
		/* bark_form_full_tuple: an oversized key/payload may exceed 8191 bytes. */
		key = bark_form_full_tuple(tupdesc, values, isnull, &sz);
		if (sz > so->currTupleSize)
		{
			/* An oversized key/INCLUDE payload needs a larger scratch buffer. */
			pfree(so->currTuple);
			so->currTupleSize = sz;
			so->currTuple = palloc(sz);
		}
		memcpy(so->currTuple, key, sz);
		pfree(key);
	}
}

/*
 * Hand back the member at so->memberIdx: its heap TID (always), and for an
 * index-only scan the key tuple with that TID stamped in.  BARK is exact, so
 * no heap recheck is ever required.
 */
static void
bark_emit_member(IndexScanDesc scan)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;

	Assert(so->memberIdx >= 0 && so->memberIdx < so->nMembers);
	scan->xs_heaptid = so->memberTids[so->memberIdx];
	scan->xs_recheck = false;

	if (scan->xs_want_itup)
	{
		IndexTuple	key = (IndexTuple) so->currTuple;

		key->t_tid = so->memberTids[so->memberIdx];
		scan->xs_itup = key;
	}
}

/*
 * Compare index tuple `itup`'s leading-column value against a leading-array
 * element `elem`, in the index's key order (DESC inverted).  A NULL leading
 * value sorts per the column's NULLS option.  Returns <0, 0, >0 as itup's
 * leading value is before, equal to, or after `elem`.
 */
static int
bark_lead_cmp(IndexScanDesc scan, IndexTuple itup, Datum elem)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	TupleDesc	tupdesc = RelationGetDescr(scan->indexRelation);
	BarkKeyColumn *col = &so->keyinfo->cols[0];
	bool		isnull;
	Datum		datum = index_getattr(itup, 1, tupdesc, &isnull);
	int			c;

	if (isnull)
		return col->nulls_first ? -1 : 1;	/* NULL vs non-NULL elem */
	c = DatumGetInt32(FunctionCall2Coll(&col->cmp, col->collation, datum, elem));
	return col->reverse ? -c : c;
}

/*
 * Leading-array re-seek: advance the leading-array cursor past the element the
 * scan just finished and descend to the next element's start leaf, so the scan
 * skips the gap between array elements instead of filtering every tuple.  Used
 * only on a forward serial scan with a leading array (the pure membership
 * filter stays correct for backward and parallel scans, which take the plain
 * path).  Advances cur to `target` (the first element not yet covered) and
 * re-positions; sets arrayDone and releases the buffer when the array is
 * exhausted.
 */
static void
bark_saop_reseek(IndexScanDesc scan, int target)
{
	Relation	index = scan->indexRelation;
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	BarkArrayKeyState *lead = so->leadArray;
	IndexTuple	lower;
	Buffer		buf;

	Assert(lead != NULL);


	if (BufferIsValid(so->currentBuffer))
	{
		ReleaseBuffer(so->currentBuffer);
		so->currentBuffer = InvalidBuffer;
	}
	so->lastOffset = InvalidOffsetNumber;
	so->nMembers = 0;

	if (target >= lead->nelems)
	{
		so->arrayDone = true;	/* every element visited */
		return;
	}
	lead->cur = target;

	lower = bark_make_lower_bound(scan);	/* uses lead->cur */
	if (lower == NULL)
		return;					/* shouldn't happen with a leading array */
	buf = bark_search(index, so->keyinfo, lower, false, false, NULL);
	pfree(lower);
	if (buf == InvalidBuffer)
		return;					/* empty index */
	so->currentBuffer = ReadBuffer(index, BufferGetBlockNumber(buf));
	UnlockReleaseBuffer(buf);	/* bark_search left it share-locked */
}

bool
bark_gettuple(IndexScanDesc scan, ScanDirection dir)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	Relation	index = scan->indexRelation;
	bool		backward = ScanDirectionIsBackward(dir);

	/*
	 * Ordered-operator (KNN) scan: distances dictate the order, not the key
	 * order, so hand off to the two-sided outward merge.  The executor only
	 * ever drives a KNN scan forward.
	 */
	if (scan->numberOfOrderBys > 0)
		return bark_knn_gettuple(scan);

	if (so->firstCall)
		bark_position(scan, dir);


	/*
	 * If the entry last landed on still has unreturned members (a LIST or
	 * POSTING expands into several heap TIDs, one per call), emit the next one
	 * in the scan direction before reading any further on the page.
	 */
	if (so->nMembers > 0)
	{
		so->memberIdx += backward ? -1 : 1;
		if (so->memberIdx >= 0 && so->memberIdx < so->nMembers)
		{
			bark_emit_member(scan);
			return true;
		}
		so->nMembers = 0;		/* entry exhausted: fall through to advance */
	}

	while (BufferIsValid(so->currentBuffer))
	{
		Buffer		buf = so->currentBuffer;
		Page		page;
		BarkPageOpaque opaque;
		OffsetNumber off,
					maxoff,
					firstdata;

		LockBuffer(buf, BUFFER_LOCK_SHARE);

		/*
		 * Predicate-lock this leaf for serializable transactions: a read here
		 * conflicts with a concurrent insert onto the same page.  BARK sets
		 * ampredlocks, so the generic index layer does not take a coarser
		 * relation-level lock on our behalf -- we must lock each page we read.
		 */
		PredicateLockPage(index, BufferGetBlockNumber(buf), scan->xs_snapshot);

		page = BufferGetPage(buf);
		opaque = BarkPageGetOpaque(page);

		/*
		 * Parallel scan: as soon as we have the page locked, publish its sibling
		 * (the next page in scan direction) to the shared cursor and let the pool
		 * proceed.  We keep our pin on this page and finish consuming it; the
		 * shared cursor is thus held in ADVANCING only for the brief span between
		 * seizing this page and reading its sibling link -- never across tuple
		 * consumption.  That is what keeps a non-exhaustive consumer (a merge
		 * join that stops pulling, a LIMIT) from stranding the cursor in
		 * ADVANCING and deadlocking the other workers.  Each leaf is still
		 * handed out by the cursor exactly once, so exactly one worker scans it.
		 */
		if (scan->parallel_scan != NULL && !so->parallelReleased)
		{
			BlockNumber sib = ScanDirectionIsBackward(dir) ? opaque->bark_prev
				: opaque->bark_next;

			bark_parallel_release(scan, sib);
			so->parallelReleased = true;
		}

		maxoff = PageGetMaxOffsetNumber(page);
		firstdata = BarkPageFirstDataKey(opaque);

		/*
		 * Pick the first offset to examine.  On a fresh page (no item returned
		 * yet) start at the end matching the direction; otherwise step one past
		 * the last-returned item in the current direction.  Deriving the start
		 * from the last-returned offset (rather than a stored "next") keeps a
		 * scroll cursor correct when the fetch direction reverses.
		 */
		if (so->lastOffset == InvalidOffsetNumber)
			off = backward ? maxoff : firstdata;
		else
			off = backward ? OffsetNumberPrev(so->lastOffset)
				: OffsetNumberNext(so->lastOffset);


		for (;
			 backward ? (off >= firstdata && off != InvalidOffsetNumber)
			 : off <= maxoff;
			 off = backward ? OffsetNumberPrev(off) : OffsetNumberNext(off))
		{
			ItemId		iid;
			IndexTuple	itup;

			/* An empty page (maxoff < firstdata) has nothing to return. */
			if (off < firstdata || off > maxoff)
				break;

			iid = PageGetItemId(page, off);
			itup = (IndexTuple) PageGetItem(page, iid);

			{
				bool		fetched;
				IndexTuple	resolved = bark_scan_resolve(index, itup, &fetched);
				bool		matched;

				/*
				 * Leading-array cursor (forward serial scan).  Once the leading
				 * value passes the current element, that element's run of equal
				 * keys is over; advance cur to the first element not before this
				 * value.  We keep walking the page in order -- the membership
				 * filter emits later elements correctly -- and only jump the gap
				 * to a far element at the page boundary (bark_saop_reseek), which
				 * keeps the scan from ever re-reading tuples it already returned.
				 * A parallel scan does not advance a shared cursor this way (each
				 * worker reads a disjoint set of pages); it relies on the
				 * membership filter alone, which is always correct.
				 */
				if (so->leadArray != NULL && !backward &&
					scan->parallel_scan == NULL)
				{
					BarkArrayKeyState *lead = so->leadArray;

					while (lead->cur < lead->nelems &&
						   bark_lead_cmp(scan, resolved,
										 lead->elems[lead->cur]) > 0)
						lead->cur++;
					if (lead->cur >= lead->nelems)
					{
						/* Past the last element: no further matches anywhere. */
						if (fetched)
							pfree(resolved);
						so->arrayDone = true;
						LockBuffer(buf, BUFFER_LOCK_UNLOCK);
						ReleaseBuffer(buf);
						so->currentBuffer = InvalidBuffer;
						return false;
					}
				}

				matched = bark_tuple_matches(scan, resolved);

				if (matched)
				{
					/*
					 * Decode this entry's heap locators into memberTids (SINGLE
					 * and OVERSIZED yield one; LIST/POSTING expand into many)
					 * and remember its key for index-only scans.  The members
					 * are then emitted one per call, starting at the
					 * direction-appropriate end.
					 */
					bark_load_members(scan, itup, resolved);
					so->lastOffset = off;
					so->memberIdx = backward ? so->nMembers - 1 : 0;
					bark_emit_member(scan);
					if (fetched)
						pfree(resolved);
					LockBuffer(buf, BUFFER_LOCK_UNLOCK);
					return true;
				}
				if (fetched)
					pfree(resolved);
			}
		}

		/*
		 * Exhausted this page.  For a forward leading-array scan, decide
		 * whether the next element lives strictly beyond the immediate sibling
		 * and, if so, jump straight to it (bark_saop_reseek) instead of walking
		 * every intervening page.
		 *
		 * A page holds keys strictly less than its high key (the first key of
		 * the right sibling, L&Y layout).  The next element is strictly past
		 * the immediate sibling exactly when it sorts strictly after the high
		 * key -- the comparison must be strict, because a non-strict reseek on
		 * an element equal to the high key would re-descend (nextkey=false) to
		 * the leftmost leaf of that element's run, which can be this very page
		 * when a run of equal keys spans a page boundary, re-reading tuples we
		 * already returned.  When the element is only one sibling away we take
		 * the plain sibling advance below, which never moves backward.
		 */
		if (so->leadArray != NULL && !backward &&
			scan->parallel_scan == NULL &&
			!BarkPageRightmost(opaque) &&
			so->leadArray->cur < so->leadArray->nelems)
		{
			ItemId		hiid = PageGetItemId(page, BARK_P_HIKEY);
			IndexTuple	hikey = (IndexTuple) PageGetItem(page, hiid);
			Datum		elem = so->leadArray->elems[so->leadArray->cur];
			bool		reseek;

			/*
			 * A truncated (zero-attribute) high key compares as -infinity, and
			 * an OVERSIZED high key carries no inline attributes; only reseek
			 * when the high key is an ordinary pivot that actually carries the
			 * leading column, so bark_lead_cmp (which reads attribute 1) is
			 * meaningful.  Otherwise fall through to the plain sibling advance,
			 * which is always correct.
			 */
			reseek = (BarkEntryGetShape(hikey) != BARK_SHAPE_OVERSIZED &&
					  BarkEntryGetPivotNAtts(hikey) >= 1 &&
					  bark_lead_cmp(scan, hikey, elem) < 0);

			if (reseek)
			{
				LockBuffer(buf, BUFFER_LOCK_UNLOCK);
				ReleaseBuffer(buf);
				so->currentBuffer = InvalidBuffer;
				so->lastOffset = InvalidOffsetNumber;
				bark_saop_reseek(scan, so->leadArray->cur);
				continue;
			}
		}

		/* Advance to the sibling in the scan direction. */
		{
			BlockNumber nextblk = backward ? opaque->bark_prev : opaque->bark_next;

			LockBuffer(buf, BUFFER_LOCK_UNLOCK);
			ReleaseBuffer(buf);
			so->currentBuffer = InvalidBuffer;
			so->lastOffset = InvalidOffsetNumber;

			if (scan->parallel_scan != NULL)
			{
				BlockNumber claimed;

				/*
				 * The cursor was already advanced to this page's sibling when we
				 * first locked the page, so do not release again -- just seize
				 * the next page the cursor hands out (which may be the sibling we
				 * published, or a page another worker published).  Mark the newly
				 * claimed page not-yet-released so it, too, publishes its sibling
				 * on first read.
				 */
				so->parallelReleased = false;
				if (bark_parallel_seize(scan, &claimed) &&
					claimed != BARK_P_NONE)
					so->currentBuffer = ReadBuffer(index, claimed);
			}
			else if (nextblk != BARK_P_NONE)
			{
				so->currentBuffer = ReadBuffer(index, nextblk);
			}
		}
	}
	return false;
}

/*
 * amgetbitmap: add every matching heap TID to the TIDBitmap.
 *
 * Walk the leaves forward from the first possibly-matching page (the same
 * positioning as a forward gettuple scan) and, for every entry that satisfies
 * the scan keys, add all of its heap locators to the bitmap in one call.  For
 * a SINGLE entry that is one TID; for a LIST or POSTING entry it is the whole
 * set, decoded in one shot -- the natural fast path, since a posting set is
 * exactly an inverted TID set.  BARK is exact, so every TID is added with
 * recheck=false and the executor's BitmapAnd / BitmapOr combine the resulting
 * bitmaps without any heap recheck.
 */
int64
bark_getbitmap(IndexScanDesc scan, TIDBitmap *tbm)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	Relation	index = scan->indexRelation;
	ItemPointer tids = NULL;
	int			tidsalloc = 0;
	int64		ntids = 0;

	if (so->firstCall)
		bark_position(scan, ForwardScanDirection);

	while (BufferIsValid(so->currentBuffer))
	{
		Buffer		buf = so->currentBuffer;
		Page		page;
		BarkPageOpaque opaque;
		OffsetNumber off,
					maxoff,
					firstdata;
		BlockNumber nextblk;

		LockBuffer(buf, BUFFER_LOCK_SHARE);
		PredicateLockPage(index, BufferGetBlockNumber(buf), scan->xs_snapshot);

		page = BufferGetPage(buf);
		opaque = BarkPageGetOpaque(page);
		maxoff = PageGetMaxOffsetNumber(page);
		firstdata = BarkPageFirstDataKey(opaque);

		for (off = firstdata; off <= maxoff; off = OffsetNumberNext(off))
		{
			ItemId		iid = PageGetItemId(page, off);
			IndexTuple	itup = (IndexTuple) PageGetItem(page, iid);
			bool		fetched;
			IndexTuple	resolved = bark_scan_resolve(index, itup, &fetched);
			bool		matched = bark_tuple_matches(scan, resolved);
			int			n;

			if (fetched)
				pfree(resolved);
			if (!matched)
				continue;

			n = bark_entry_count_tids(itup);
			if (n > tidsalloc)
			{
				if (tids)
					pfree(tids);
				tidsalloc = Max(n, 128);
				tids = (ItemPointer) palloc(tidsalloc * sizeof(ItemPointerData));
			}
			n = bark_entry_get_tids(itup, tids, tidsalloc);
			tbm_add_tuples(tbm, tids, n, false);
			ntids += n;
		}

		nextblk = opaque->bark_next;
		LockBuffer(buf, BUFFER_LOCK_UNLOCK);
		ReleaseBuffer(buf);
		so->currentBuffer = (nextblk != BARK_P_NONE) ?
			ReadBuffer(index, nextblk) : InvalidBuffer;
	}

	if (tids)
		pfree(tids);
	return ntids;
}

void
bark_endscan(IndexScanDesc scan)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;

	if (so == NULL)
		return;
	bark_knn_endscan(scan);
	bark_free_array_keys(so);
	if (BufferIsValid(so->currentBuffer))
		ReleaseBuffer(so->currentBuffer);
	if (so->currTuple)
		pfree(so->currTuple);
	if (so->memberTids)
		pfree(so->memberTids);
	if (so->keyinfo)
		pfree(so->keyinfo);
	pfree(so);
	scan->opaque = NULL;
}

/*
 * bark_canreturn -- can an index-only scan return column `attno`?
 *
 * A BARK leaf entry is the full index tuple (every indexed column plus the
 * heap TID in t_tid), so any column can be returned without a heap fetch.
 */
bool
bark_canreturn(Relation index, int attno)
{
	return true;
}

/*
 * bark_estimateparallelscan -- shared-memory size for a parallel BARK scan.
 *
 * BARK's parallel state is a fixed-size cursor (unlike nbtree, there are no
 * ScalarArrayOp arrays to size for), so this ignores nkeys/norderbys.
 */
Size
bark_estimateparallelscan(Relation index, int nkeys, int norderbys)
{
	return sizeof(BarkParallelScanDescData);
}

/*
 * bark_initparallelscan -- initialize the shared parallel scan descriptor.
 */
void
bark_initparallelscan(void *target)
{
	BarkParallelScanDesc bps = (BarkParallelScanDesc) target;

	LWLockInitialize(&bps->bps_lock, LWTRANCHE_PARALLEL_BARK_SCAN);
	ConditionVariableInit(&bps->bps_cv);
	bps->bps_nextPage = InvalidBlockNumber;
	bps->bps_state = BARK_PARALLEL_NOT_INITIALIZED;
}

/*
 * bark_parallelrescan -- reset the shared parallel scan to its initial state.
 */
void
bark_parallelrescan(IndexScanDesc scan)
{
	BarkParallelScanDesc bps;

	Assert(scan->parallel_scan);
	bps = (BarkParallelScanDesc) OffsetToPointer(scan->parallel_scan,
												 scan->parallel_scan->ps_offset_am);

	/*
	 * No other workers should be running at rescan, but take the lock anyway
	 * for consistency (as nbtree does).
	 */
	LWLockAcquire(&bps->bps_lock, LW_EXCLUSIVE);
	bps->bps_nextPage = InvalidBlockNumber;
	bps->bps_state = BARK_PARALLEL_NOT_INITIALIZED;
	LWLockRelease(&bps->bps_lock);
}
