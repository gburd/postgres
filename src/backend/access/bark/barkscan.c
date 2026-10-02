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
#include "storage/predicate.h"
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
	int			natts;
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
			 * Form a lower-bound key tuple holding this bound in the leading
			 * column; every other attribute is NULL, which bark_compare_itups
			 * treats per the column's NULLS ordering.  That is a safe lower
			 * bound: the descent only needs to land at or before the first
			 * match, and the per-tuple test filters precisely.
			 *
			 * index_form_tuple reads one entry per descriptor attribute, so the
			 * arrays must cover all index attributes (key plus any INCLUDE
			 * columns), not just the key attributes.
			 */
			natts = IndexRelationGetNumberOfAttributes(index);
			values = (Datum *) palloc(natts * sizeof(Datum));
			isnull = (bool *) palloc(natts * sizeof(bool));
			values[0] = sk->sk_argument;
			isnull[0] = false;
			for (int c = 1; c < natts; c++)
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
	so->lastOffset = InvalidOffsetNumber;
	so->firstCall = true;
	so->currTuple = NULL;
	so->memberTids = NULL;
	so->nMembersAlloc = 0;
	so->nMembers = 0;
	so->memberIdx = 0;

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
	so->lastOffset = InvalidOffsetNumber;
	so->firstCall = true;
	so->nMembers = 0;
	so->memberIdx = 0;

	if (scankey && nscankeys > 0)
		memcpy(scan->keyData, scankey, nscankeys * sizeof(ScanKeyData));
}

/*
 * Position the scan on its first leaf.
 *
 * Forward: descend to the leftmost possibly matching leaf (via a first-column
 * lower bound when available), else the leftmost leaf of the tree.  Backward:
 * start at the rightmost leaf and walk left, applying the scan keys per tuple.
 * Leaves the leaf pinned but not locked; bark_gettuple locks per page read.
 *
 * ponytail: a backward scan always starts rightmost rather than descending to
 * an upper bound first; sharper backward positioning is an optimization, not a
 * correctness matter (symmetric to the forward no-lower-bound case).
 */
static void
bark_position(IndexScanDesc scan, ScanDirection dir)
{
	Relation	index = scan->indexRelation;
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	bool		backward = ScanDirectionIsBackward(dir);
	IndexTuple	lower = backward ? NULL : bark_make_lower_bound(scan);
	Buffer		buf;

	/* Allocate the index-only-scan scratch buffer on first use. */
	if (scan->xs_want_itup && so->currTuple == NULL)
		so->currTuple = palloc(BLCKSZ);

	if (lower != NULL)
	{
		buf = bark_search(index, so->keyinfo, lower, false, false, NULL);
		pfree(lower);
		if (buf != InvalidBuffer)
			LockBuffer(buf, BUFFER_LOCK_UNLOCK);	/* search left it share-locked */
	}
	else
	{
		/*
		 * No usable bound: walk down the spine to the extreme leaf -- leftmost
		 * for a forward scan, rightmost for a backward scan.
		 */
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
			/* Follow the first (forward) or last (backward) downlink. */
			{
				OffsetNumber off = backward ? PageGetMaxOffsetNumber(page)
					: BarkPageFirstDataKey(opaque);
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
	so->lastOffset = InvalidOffsetNumber;	/* set on first page read */
	so->firstCall = false;
}

/*
 * Decode the heap locators of a matched leaf entry into so->memberTids (in
 * ascending order) and, for an index-only scan, build the key-only tuple the
 * scan will hand back for each member.  A SINGLE entry yields one locator;
 * LIST and POSTING entries expand into many.  The members are then emitted one
 * per bark_gettuple call by bark_emit_member.
 */
static void
bark_load_members(IndexScanDesc scan, IndexTuple itup)
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
	 * patches its t_tid per member.  index_deform_tuple reads the key and any
	 * INCLUDE attributes, which sit at the front of every leaf shape.
	 */
	if (scan->xs_want_itup)
	{
		Relation	index = scan->indexRelation;
		TupleDesc	tupdesc = RelationGetDescr(index);
		Datum		values[INDEX_MAX_KEYS];
		bool		isnull[INDEX_MAX_KEYS];
		IndexTuple	key;
		Size		sz;

		index_deform_tuple(itup, tupdesc, values, isnull);
		key = index_form_tuple(tupdesc, values, isnull);
		sz = IndexTupleSize(key);
		Assert(sz <= BLCKSZ);
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

bool
bark_gettuple(IndexScanDesc scan, ScanDirection dir)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	Relation	index = scan->indexRelation;
	bool		backward = ScanDirectionIsBackward(dir);

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

			if (bark_tuple_matches(scan, itup))
			{
				/*
				 * Decode this entry's heap locators into memberTids (SINGLE
				 * yields one; LIST/POSTING expand into many) and remember its
				 * key for index-only scans.  The members are then emitted one
				 * per call, starting at the direction-appropriate end.
				 */
				bark_load_members(scan, itup);
				so->lastOffset = off;
				so->memberIdx = backward ? so->nMembers - 1 : 0;
				bark_emit_member(scan);
				LockBuffer(buf, BUFFER_LOCK_UNLOCK);
				return true;
			}
		}

		/* Exhausted this page; advance to the sibling in the scan direction. */
		{
			BlockNumber nextblk = backward ? opaque->bark_prev : opaque->bark_next;

			LockBuffer(buf, BUFFER_LOCK_UNLOCK);
			ReleaseBuffer(buf);
			so->currentBuffer = InvalidBuffer;
			so->lastOffset = InvalidOffsetNumber;
			if (nextblk != BARK_P_NONE)
			{
				so->currentBuffer = ReadBuffer(index, nextblk);
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
