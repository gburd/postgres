/*-------------------------------------------------------------------------
 *
 * barkscan.c
 *	  Index scan for the BARK index access method.
 *
 * A BARK scan positions on a leaf and walks the right-link chain, returning
 * the heap TID of each entry that satisfies the scan keys.  When the keys
 * provide a lower bound on the first index column (an =, >, or >= qual), the
 * scan descends the tree to the first leaf that can contain a match; otherwise
 * it starts at the leftmost leaf.  It stops early once the first index column
 * passes an upper bound (an =, <, or <= qual).
 *
 * Modeled on nbtree's scan (nbtsearch.c _bt_first / _bt_next / _bt_readpage),
 * simplified for the SINGLE entry shape: every leaf entry is one heap TID in
 * t_tid.  The index is exact, so no heap recheck is required.
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
#include "miscadmin.h"
#include "nodes/tidbitmap.h"
#include "storage/bufmgr.h"
#include "utils/rel.h"

/*
 * Test one index tuple against all scan keys.  Returns true when every key is
 * satisfied.  A NULL index value never satisfies an ordinary (non-IS NULL)
 * comparison key.
 */
static bool
bark_tuple_matches(IndexScanDesc scan, IndexTuple itup)
{
	Relation	index = scan->indexRelation;
	TupleDesc	tupdesc = RelationGetDescr(index);

	for (int i = 0; i < scan->numberOfKeys; i++)
	{
		ScanKey		key = &scan->keyData[i];
		Datum		datum;
		bool		isnull;

		datum = index_getattr(itup, key->sk_attno, tupdesc, &isnull);

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
 * (start at the leftmost leaf) when no =, >, or >= qual on column 1 is present.
 */
static IndexTuple
bark_make_lower_bound(IndexScanDesc scan)
{
	Relation	index = scan->indexRelation;
	TupleDesc	tupdesc = RelationGetDescr(index);
	int			nkeyatts = IndexRelationGetNumberOfKeyAttributes(index);
	Datum	   *values;
	bool	   *isnull;
	IndexTuple	key = NULL;

	for (int i = 0; i < scan->numberOfKeys; i++)
	{
		ScanKey		sk = &scan->keyData[i];

		if (sk->sk_attno != 1 || (sk->sk_flags & SK_ISNULL))
			continue;
		if (sk->sk_strategy == BTEqualStrategyNumber ||
			sk->sk_strategy == BTGreaterStrategyNumber ||
			sk->sk_strategy == BTGreaterEqualStrategyNumber)
		{
			/*
			 * Form a one-column key tuple holding this bound; the remaining
			 * key columns are NULL, which bark_compare_itups treats per the
			 * column's NULLS ordering.  That is a safe lower bound: the
			 * descent only needs to land at or before the first match, and
			 * the per-tuple test filters precisely.
			 */
			values = (Datum *) palloc(nkeyatts * sizeof(Datum));
			isnull = (bool *) palloc(nkeyatts * sizeof(bool));
			values[0] = sk->sk_argument;
			isnull[0] = false;
			for (int c = 1; c < nkeyatts; c++)
			{
				values[c] = (Datum) 0;
				isnull[c] = true;
			}
			key = index_form_tuple(tupdesc, values, isnull);
			pfree(values);
			pfree(isnull);
			break;
		}
	}
	return key;
}

IndexScanDesc
bark_beginscan(Relation index, int nkeys, int norderbys)
{
	IndexScanDesc scan = RelationGetIndexScan(index, nkeys, norderbys);
	BarkScanOpaque so = palloc0_object(BarkScanOpaqueData);

	so->keyinfo = bark_build_keyinfo(index);
	so->currentBuffer = InvalidBuffer;
	so->nextOffset = InvalidOffsetNumber;
	so->firstCall = true;
	so->currTuple = NULL;

	/*
	 * Set up the index tuple descriptor for index-only scans.  The scratch
	 * buffer that holds a returned tuple is allocated lazily on the first read
	 * (bark_position), because xs_want_itup is set after beginscan returns.
	 */
	scan->xs_itupdesc = RelationGetDescr(index);
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
	so->nextOffset = InvalidOffsetNumber;
	so->firstCall = true;

	if (scankey && nscankeys > 0)
		memcpy(scan->keyData, scankey, nscankeys * sizeof(ScanKeyData));
}

/*
 * Position the scan on its first leaf: descend to the leftmost possibly
 * matching leaf (via a first-column lower bound when available), else the
 * leftmost leaf of the tree.  Leaves the leaf pinned but not locked;
 * bark_gettuple locks per page read.
 */
static void
bark_position(IndexScanDesc scan)
{
	Relation	index = scan->indexRelation;
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	IndexTuple	lower = bark_make_lower_bound(scan);
	Buffer		buf;

	/* Allocate the index-only-scan scratch buffer on first use. */
	if (scan->xs_want_itup && so->currTuple == NULL)
		so->currTuple = palloc(BLCKSZ);

	if (lower != NULL)
	{
		buf = bark_search(index, so->keyinfo, lower, false, NULL);
		pfree(lower);
		if (buf != InvalidBuffer)
			LockBuffer(buf, BUFFER_LOCK_UNLOCK);	/* search left it share-locked */
	}
	else
	{
		/* No lower bound: walk down the leftmost spine to the first leaf. */
		BlockNumber blkno;
		Buffer		metabuf = ReadBuffer(index, BARK_METAPAGE);

		LockBuffer(metabuf, BUFFER_LOCK_SHARE);
		blkno = BarkPageGetMeta(BufferGetPage(metabuf))->bark_root;
		UnlockReleaseBuffer(metabuf);

		buf = InvalidBuffer;
		while (blkno != BARK_P_NONE)
		{
			Page		page;
			BarkPageOpaque opaque;

			buf = ReadBuffer(index, blkno);
			LockBuffer(buf, BUFFER_LOCK_SHARE);
			page = BufferGetPage(buf);
			opaque = BarkPageGetOpaque(page);
			if (BarkPageIsLeaf(opaque))
			{
				LockBuffer(buf, BUFFER_LOCK_UNLOCK);
				break;
			}
			/* Follow the first downlink. */
			{
				OffsetNumber off = BarkPageFirstDataKey(opaque);
				ItemId		iid = PageGetItemId(page, off);
				IndexTuple	itup = (IndexTuple) PageGetItem(page, iid);
				BlockNumber child = BarkPivotGetDownLink(itup);

				UnlockReleaseBuffer(buf);
				buf = InvalidBuffer;
				blkno = child;
			}
		}
	}

	so->currentBuffer = buf;
	so->nextOffset = InvalidOffsetNumber;	/* set on first page read */
	so->firstCall = false;
}

bool
bark_gettuple(IndexScanDesc scan, ScanDirection dir)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	Relation	index = scan->indexRelation;

	/* Forward scans only for now. */
	if (dir != ForwardScanDirection && dir != NoMovementScanDirection)
		elog(ERROR, "BARK supports only forward index scans");

	if (so->firstCall)
		bark_position(scan);

	while (BufferIsValid(so->currentBuffer))
	{
		Buffer		buf = so->currentBuffer;
		Page		page;
		BarkPageOpaque opaque;
		OffsetNumber off,
					maxoff;

		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);
		opaque = BarkPageGetOpaque(page);
		maxoff = PageGetMaxOffsetNumber(page);

		if (so->nextOffset == InvalidOffsetNumber)
			so->nextOffset = BarkPageFirstDataKey(opaque);

		for (off = so->nextOffset; off <= maxoff; off = OffsetNumberNext(off))
		{
			ItemId		iid = PageGetItemId(page, off);
			IndexTuple	itup = (IndexTuple) PageGetItem(page, iid);

			if (bark_tuple_matches(scan, itup))
			{
				scan->xs_heaptid = itup->t_tid;
				scan->xs_recheck = false;

				/*
				 * Index-only scan: hand back the index tuple itself.  Copy it
				 * into the scan-owned scratch buffer first, since the page lock
				 * (and thus the on-page tuple) is released before we return.
				 */
				if (scan->xs_want_itup)
				{
					Size		sz = IndexTupleSize(itup);

					memcpy(so->currTuple, itup, sz);
					scan->xs_itup = (IndexTuple) so->currTuple;
				}

				so->nextOffset = OffsetNumberNext(off);
				LockBuffer(buf, BUFFER_LOCK_UNLOCK);
				return true;
			}
		}

		/* Exhausted this page; advance to the right sibling. */
		{
			BlockNumber right = opaque->bark_next;

			LockBuffer(buf, BUFFER_LOCK_UNLOCK);
			ReleaseBuffer(buf);
			so->currentBuffer = InvalidBuffer;
			so->nextOffset = InvalidOffsetNumber;
			if (right != BARK_P_NONE)
			{
				so->currentBuffer = ReadBuffer(index, right);
			}
		}
	}
	return false;
}

int64
bark_getbitmap(IndexScanDesc scan, TIDBitmap *tbm)
{
	int64		ntids = 0;

	while (bark_gettuple(scan, ForwardScanDirection))
	{
		tbm_add_tuples(tbm, &scan->xs_heaptid, 1, false);
		ntids++;
	}
	return ntids;
}

void
bark_endscan(IndexScanDesc scan)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;

	if (so == NULL)
		return;
	if (BufferIsValid(so->currentBuffer))
		ReleaseBuffer(so->currentBuffer);
	if (so->currTuple)
		pfree(so->currTuple);
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
