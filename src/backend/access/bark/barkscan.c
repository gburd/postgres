/*-------------------------------------------------------------------------
 *
 * barkscan.c
 *	  Index scan for the BARK index access method.
 *
 * A BARK scan positions on a leaf and walks the right-link chain, returning
 * the heap TID of each entry that satisfies the scan keys.  When the keys
 * provide a lower bound on the leading index columns (an =, >, or >= qual, or
 * a `col = ANY(array)` SAOP whose current element bounds column 1), the scan
 * descends the tree to the first leaf that can contain a match: the descent
 * key uses every usable leading column (bark_make_bound), so a selective
 * second-column bound such as WHERE a = 5 AND b >= 100 lands near the match
 * rather than at the first a = 5 leaf.  Otherwise it starts at the leftmost
 * leaf.  A forward scan also stops early once the leading column passes an
 * upper bound (an =, <, or <= qual: bark_past_upper_bound), so a bounded scan
 * reads only the matching span, not the rest of the index.
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
 * A backward scan starts at the rightmost leaf rather than descending to an
 * upper bound, and does not terminate early at a lower bound; sharper backward
 * positioning would be an optimization (symmetric to the forward case), with
 * no effect on correctness.
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
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "storage/lwlock.h"
#include "storage/predicate.h"
#include "utils/array.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/wait_event.h"

static int	bark_lead_cmp(IndexScanDesc scan, IndexTuple itup, Datum elem);
static bool bark_array_reseek(IndexScanDesc scan, ScanDirection dir);
static int	bark_key_cmp(IndexScanDesc scan, int i, IndexTuple itup);

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
}

/*
 * Test a row comparison such as (a, b) > (100, 5) against an index tuple, as
 * a filter.  This is the comparison loop of nbtree's _bt_check_rowcompare:
 * compare column by column with each member's own comparator until one is
 * unequal (or the last member is reached), then apply the strategy to that
 * three-way result.  A NULL reached before the comparison is decided, in the
 * qual or in the tuple, means no match, as in SQL.
 *
 * nbtree also inverts the result for a DESC column, but only because its
 * preprocessing has already commuted that member's strategy; BARK does no such
 * preprocessing, so the comparison here is in value order with the strategy
 * the executor gave.
 *
 * A row comparison never positions or stops the scan (bark_key_bounds rejects
 * it): the scan reads the whole range the other keys allow and filters.
 */
static bool
bark_rowcompare_matches(ScanKey header, IndexTuple itup, TupleDesc tupdesc)
{
	ScanKey		subkey = (ScanKey) DatumGetPointer(header->sk_argument);
	int32		cmpresult = 0;

	for (;;)
	{
		Datum		datum;
		bool		isnull;

		Assert(subkey->sk_flags & SK_ROW_MEMBER);
		if (subkey->sk_flags & SK_ISNULL)
			return false;
		datum = index_getattr(itup, subkey->sk_attno, tupdesc, &isnull);
		if (isnull)
			return false;
		cmpresult = DatumGetInt32(FunctionCall2Coll(&subkey->sk_func,
													subkey->sk_collation,
													datum,
													subkey->sk_argument));
		if (cmpresult != 0 || (subkey->sk_flags & SK_ROW_END))
			break;
		subkey++;
	}

	switch (subkey->sk_strategy)
	{
		case BTLessStrategyNumber:
			return cmpresult < 0;
		case BTLessEqualStrategyNumber:
			return cmpresult <= 0;
		case BTGreaterEqualStrategyNumber:
			return cmpresult >= 0;
		case BTGreaterStrategyNumber:
			return cmpresult > 0;
		default:
			elog(ERROR, "unexpected strategy number %d in BARK row comparison",
				 subkey->sk_strategy);
			return false;		/* keep compiler quiet */
	}
}

/*
 * Test one index tuple against all scan keys.  Returns true when every key is
 * satisfied.  A NULL index value never satisfies an ordinary (non-IS NULL)
 * comparison key.  A SK_SEARCHARRAY key is satisfied when the tuple's value is
 * a member of its (preprocessed, sorted) array.  Also used by the KNN scan to
 * filter its candidates.
 */
bool
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

		if (key->sk_flags & SK_ROW_HEADER)
		{
			if (!bark_rowcompare_matches(key, itup, tupdesc))
				return false;
			continue;
		}

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
 * Can scan key sk bound the scan in index order?  Sets *lower when the key
 * excludes everything before some point in index order on its column, and
 * *upper when it excludes everything after some point.  On an ASC column =, >
 * and >= are lower bounds and =, < and <= are upper bounds; a DESC column
 * stores its values in reverse, so the roles of < and > swap.  An equality
 * qual is both.
 *
 * Returns false for a key that cannot position or stop the scan; such a key
 * is still applied by bark_tuple_matches as a filter.  That covers a key with
 * a NULL argument (including IS [NOT] NULL), a SAOP array (the leading-array
 * logic positions on those), a row comparison, and a cross-type key whose
 * opfamily has no ORDER proc for its pair of types.  Every other key, cross-
 * type ones included, is compared through so->keyCmp, the ORDER proc for
 * the column type and the argument's type (bark_setup_key_procs), so a qual
 * such as int4col < 3000000000::bigint positions and stops correctly.
 */
static bool
bark_key_bounds(IndexScanDesc scan, ScanKey sk, bool *lower, bool *upper)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	StrategyNumber strat = sk->sk_strategy;
	bool		less;
	bool		greater;

	if (sk->sk_flags & (SK_ISNULL | SK_SEARCHARRAY | SK_ROW_HEADER))
		return false;
	if (!OidIsValid(so->keyCmp[sk - scan->keyData].fn_oid))
		return false;

	less = (strat == BTEqualStrategyNumber ||
			strat == BTLessStrategyNumber ||
			strat == BTLessEqualStrategyNumber);
	greater = (strat == BTEqualStrategyNumber ||
			   strat == BTGreaterStrategyNumber ||
			   strat == BTGreaterEqualStrategyNumber);
	if (so->keyinfo->cols[sk->sk_attno - 1].reverse)
	{
		*lower = less;
		*upper = greater;
	}
	else
	{
		*lower = greater;
		*upper = less;
	}
	return true;
}

/*
 * Can a scan moving in direction dir stop at tuple itup, because itup's
 * leading column is already past a bound in that direction?
 *
 * Entries are visited in index order (forward) or its reverse (backward).
 * Once the leading value sorts strictly after an upper bound on column 1
 * (forward), or strictly before a lower bound (backward), every later entry in
 * that direction does too, so nothing further can match.  A value equal to the
 * bound is left to bark_tuple_matches.  A NULL leading value sorts where the
 * column's NULLS option puts it, and fails every bounding key, so it ends the
 * scan exactly when the NULLs lie beyond the bound.
 *
 * Only column 1 is used: a bound on a later column cannot end the scan, since
 * a later leading value may still have matching trailing values.
 */
static bool
bark_past_bound(IndexScanDesc scan, IndexTuple itup, ScanDirection dir)
{
	bool		forward = ScanDirectionIsForward(dir);

	for (int i = 0; i < scan->numberOfKeys; i++)
	{
		ScanKey		sk = &scan->keyData[i];
		bool		lower;
		bool		upper;
		int			c;

		if (sk->sk_attno != 1 || !bark_key_bounds(scan, sk, &lower, &upper))
			continue;
		if (forward ? !upper : !lower)
			continue;
		c = bark_key_cmp(scan, i, itup);
		if (forward ? c > 0 : c < 0)
			return true;
	}
	return false;
}

/*
 * Compare index tuple itup's value in scan key i's column with the key's
 * argument, through the key's ORDER proc: <0, 0 or >0 as the value sorts
 * before, with or after the argument in index order (DESC inverted).  A NULL
 * value sorts per the column's NULLS option.
 */
static int
bark_key_cmp(IndexScanDesc scan, int i, IndexTuple itup)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	ScanKey		sk = &scan->keyData[i];
	BarkKeyColumn *col = &so->keyinfo->cols[sk->sk_attno - 1];
	bool		isnull;
	Datum		datum = index_getattr(itup, sk->sk_attno,
									  RelationGetDescr(scan->indexRelation),
									  &isnull);
	int32		c;

	if (isnull)
		return col->nulls_first ? -1 : 1;
	c = DatumGetInt32(FunctionCall2Coll(&so->keyCmp[i], sk->sk_collation,
										datum, sk->sk_argument));
	return col->reverse ? -c : c;
}

/*
 * Build the bound a scan in direction dir starts from: the leading index
 * columns the keys bound in index order, for bark_search_bound.  Returns
 * false when column 1 is unbounded in that direction; the scan then starts
 * at the end of the index.
 *
 * Forward, the bound is a lower bound; backward, an upper bound.  It uses the
 * longest run of leading columns 1..k where columns 1..k-1 each have an
 * equality key (or, on column 1, a SAOP whose current element is the equality
 * value) and column k has a key that bounds it in the scan direction.  For
 * WHERE a = 5 AND b >= 100 on (a, b) a forward scan descends to (5, 100), not
 * to the first a = 5 leaf.  Columns after k are minus infinity in a lower
 * bound and plus infinity in an upper one, so the descent lands at or before
 * the first match in scan order, never past it.  A key's argument may be of
 * any type the opfamily has an ORDER proc for, as nbtree's _bt_first allows.
 * When several keys bound one column, the first is used; the others filter.
 */
static bool
bark_make_bound(IndexScanDesc scan, ScanDirection dir, BarkScanBound *bound)
{
	Relation	index = scan->indexRelation;
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	int			nkeyatts = IndexRelationGetNumberOfKeyAttributes(index);
	bool		forward = ScanDirectionIsForward(dir);

	bound->nkeys = 0;
	bound->upper = !forward;

	for (int col = 1; col <= nkeyatts; col++)
	{
		int			found = -1;
		bool		equality = false;

		if (col == 1 && so->leadArray != NULL && so->leadArray->nelems > 0 &&
			forward)
		{
			bound->args[0] = so->leadArray->elems[so->leadArray->cur];
			bound->procs[0] = &so->keyinfo->cols[0].cmp;
			bound->collations[0] = so->keyinfo->cols[0].collation;
			bound->nkeys = 1;
			continue;			/* an equality, one element at a time */
		}

		for (int i = 0; i < scan->numberOfKeys; i++)
		{
			ScanKey		sk = &scan->keyData[i];
			bool		lower;
			bool		upper;

			if (sk->sk_attno != col ||
				!bark_key_bounds(scan, sk, &lower, &upper) ||
				!(forward ? lower : upper))
				continue;
			if (found < 0 || (lower && upper))
			{
				found = i;
				equality = lower && upper;
			}
			if (equality)
				break;			/* prefer an equality on this column */
		}
		if (found < 0)
			break;

		bound->args[col - 1] = scan->keyData[found].sk_argument;
		bound->procs[col - 1] = &so->keyCmp[found];
		bound->collations[col - 1] = scan->keyData[found].sk_collation;
		bound->nkeys = col;
		if (!equality)
			break;				/* a range bound is the last usable column */
	}
	return bound->nkeys > 0;
}

/*
 * Build so->keyCmp: for each scan key that can bound the scan, the three-way
 * ORDER proc that compares the indexed column with the key's argument.  A
 * same-type key uses the column's own comparator; a cross-type key (an int8
 * column with an int4 constant, say) uses the opfamily's ORDER proc for the
 * (column type, argument type) pair, as nbtree's _bt_first does.  A key
 * without one only filters.  Done once per scan, in the scan's context: the
 * executor may change the keys' arguments on rescan, never their types.
 */
static void
bark_setup_key_procs(IndexScanDesc scan)
{
	Relation	index = scan->indexRelation;
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	MemoryContext oldcxt;

	if (so->keyCmpReady)
		return;
	oldcxt = MemoryContextSwitchTo(so->scanCxt);
	if (scan->numberOfKeys > 0)
		so->keyCmp = palloc0_array(FmgrInfo, scan->numberOfKeys);
	for (int i = 0; i < scan->numberOfKeys; i++)
	{
		ScanKey		sk = &scan->keyData[i];
		int			col = sk->sk_attno - 1;
		Oid			subtype;
		Oid			proc;

		so->keyCmp[i].fn_oid = InvalidOid;
		if (sk->sk_flags & (SK_SEARCHARRAY | SK_ROW_HEADER | SK_SEARCHNULL |
							SK_SEARCHNOTNULL))
			continue;
		subtype = OidIsValid(sk->sk_subtype) ? sk->sk_subtype :
			index->rd_opcintype[col];
		if (subtype == index->rd_opcintype[col])
		{
			fmgr_info_copy(&so->keyCmp[i], &so->keyinfo->cols[col].cmp,
						   so->scanCxt);
			continue;
		}
		proc = get_opfamily_proc(index->rd_opfamily[col],
								 index->rd_opcintype[col], subtype,
								 BARK_ORDER_PROC);
		if (OidIsValid(proc))
			fmgr_info_cxt(proc, &so->keyCmp[i], so->scanCxt);
	}
	so->keyCmpReady = true;
	MemoryContextSwitchTo(oldcxt);
}

IndexScanDesc
bark_beginscan(Relation index, int nkeys, int norderbys)
{
	IndexScanDesc scan = RelationGetIndexScan(index, nkeys, norderbys);
	BarkScanOpaque so = palloc0_object(BarkScanOpaqueData);

	so->keyinfo = bark_build_keyinfo(index);
	so->firstCall = true;
	so->scanCxt = CurrentMemoryContext;
	so->knn = NULL;
	BarkScanPosInvalidate(so->currPos);
	so->currPos.items = NULL;
	so->currPos.maxItems = 0;

	/*
	 * Index-only scans read the key columns from xs_itup, described by the
	 * index's own tuple descriptor.  The tuple workspace is allocated when a
	 * page is first read, since xs_want_itup is set after beginscan returns.
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

	BarkScanPosUnpinIfPinned(so->currPos);
	BarkScanPosInvalidate(so->currPos);
	so->firstCall = true;

	/*
	 * Drop the leaf pin as soon as a page has been read when the scan cannot
	 * be hurt by concurrent TID recycling, which is nbtree's dropPin rule
	 * (nbtree README, "Making concurrent TID recycling safe").  A plain index
	 * scan with an MVCC snapshot visits the heap for every TID it returns,
	 * and the snapshot rejects a recycled slot's new occupant, so the pin
	 * that makes VACUUM's cleanup lock wait is not needed; dropping it keeps
	 * an idle cursor from blocking VACUUM.  An index-only scan decides
	 * visibility from the visibility map rather than the heap and so must
	 * keep the pin, as must a scan with a non-MVCC snapshot.
	 */
	so->dropPin = (!scan->xs_want_itup &&
				   IsMVCCLikeSnapshot(scan->xs_snapshot) &&
				   scan->heapRelation != NULL);

	if (scankey && nscankeys > 0)
		memcpy(scan->keyData, scankey, nscankeys * sizeof(ScanKeyData));

	/*
	 * Rebuild ScalarArrayOp state from the (possibly new) scan keys: sort and
	 * de-duplicate each SK_SEARCHARRAY array once, so the scan can visit the
	 * matching keys in index order.  A plain scan builds nothing here.
	 */
	bark_free_array_keys(so);
	bark_setup_array_keys(scan);
	bark_setup_key_procs(scan);

	/*
	 * An ordered-operator (KNN) scan carries ORDER BY <~> keys; copy them in
	 * and hand them to the KNN machinery, which runs the outward two-sided
	 * merge in bark_gettuple.
	 */
	if (scan->numberOfOrderBys > 0 && orderbys && norderbys > 0)
	{
		memcpy(scan->orderByData, orderbys, norderbys * sizeof(ScanKeyData));
		bark_knn_rescan(scan, scan->orderByData, norderbys);
	}
}

/*
 * Descend to the leaf where a scan in direction dir must start, and return it
 * share-locked; InvalidBuffer for an empty index.
 *
 * A scan bounded in its direction on the leading columns descends to the
 * first leaf in that direction that can hold a match (bark_make_bound);
 * otherwise it starts at the leftmost or rightmost leaf.  The returned leaf
 * is never deleted or half-dead.
 */
static Buffer
bark_start_leaf(IndexScanDesc scan, ScanDirection dir)
{
	Relation	index = scan->indexRelation;
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	bool		backward = ScanDirectionIsBackward(dir);
	BarkScanBound bound;
	BlockNumber blkno;
	Buffer		buf;
	Page		page;
	BarkPageOpaque opaque;

	/*
	 * A lower bound descends to the leftmost leaf that can hold a match
	 * (nextkey = false); an upper bound, to the rightmost (nextkey = true),
	 * from which a backward scan reads leftward.
	 */
	if (bark_make_bound(scan, dir, &bound))
		return bark_search_bound(index, so->keyinfo, &bound, backward);

	/*
	 * No usable bound: walk down the edge of the tree, as nbtree's
	 * _bt_get_endpoint does, stepping right past ignorable pages and, for
	 * the rightmost edge, past any page that split since we read its
	 * downlink.
	 */
	blkno = bark_get_root(index, NULL);
	if (blkno == BARK_P_NONE)
		return InvalidBuffer;
	buf = ReadBuffer(index, blkno);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	for (;;)
	{
		OffsetNumber off;
		IndexTuple	itup;

		page = BufferGetPage(buf);
		opaque = BarkPageGetOpaque(page);
		while (BarkPageIgnore(opaque) ||
			   (backward && !BarkPageRightmost(opaque)))
		{
			blkno = opaque->bark_next;
			if (blkno == BARK_P_NONE)
				elog(ERROR, "fell off the end of BARK index \"%s\"",
					 RelationGetRelationName(index));
			LockBuffer(buf, BUFFER_LOCK_UNLOCK);
			buf = ReleaseAndReadBuffer(buf, index, blkno);
			LockBuffer(buf, BUFFER_LOCK_SHARE);
			page = BufferGetPage(buf);
			opaque = BarkPageGetOpaque(page);
		}
		if (BarkPageIsLeaf(opaque))
			return buf;

		off = backward ? PageGetMaxOffsetNumber(page) :
			BarkPageFirstDataKey(opaque);
		itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, off));
		blkno = BarkEntryGetDownLink(itup);
		LockBuffer(buf, BUFFER_LOCK_UNLOCK);
		buf = ReleaseAndReadBuffer(buf, index, blkno);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
	}
}

/* ---------------------------------------------------------------------------
 * Parallel scan coordination (nbtree's _bt_parallel_seize/release/done)
 *
 * Workers claim leaf pages one at a time from a shared cursor.  A worker that
 * seizes the cursor reads the page it was handed and, under the page's lock
 * and before copying any matches, releases the page's sibling in the scan
 * direction for another worker; it then returns tuples from its local copy
 * without holding the cursor.  The first worker to find the scan not yet
 * started positions it.  A parallel scan never changes direction, so one
 * next-page cursor (plus the page it came from, for a backward step's
 * left-link check) is all the state needed.
 * ---------------------------------------------------------------------------
 */

static BarkParallelScanDesc
bark_get_parallel_desc(IndexScanDesc scan)
{
	ParallelIndexScanDesc pscan = scan->parallel_scan;

	return (BarkParallelScanDesc) OffsetToPointer(pscan, pscan->ps_offset_am);
}

/* Mark the parallel scan complete so no worker waits for a next page. */
static void
bark_parallel_done(IndexScanDesc scan)
{
	BarkParallelScanDesc bps;
	bool		changed = false;

	if (scan->parallel_scan == NULL)
		return;
	bps = bark_get_parallel_desc(scan);

	LWLockAcquire(&bps->bps_lock, LW_EXCLUSIVE);
	if (bps->bps_state != BARK_PARALLEL_DONE)
	{
		bps->bps_state = BARK_PARALLEL_DONE;
		changed = true;
	}
	LWLockRelease(&bps->bps_lock);
	if (changed)
		ConditionVariableBroadcast(&bps->bps_cv);
}

/*
 * Seize the parallel scan.  Returns false when the scan is finished.
 * Otherwise *next_page is the page to read and *last_page the page it was
 * linked from, or *next_page is InvalidBlockNumber when this worker found the
 * scan not yet started and must position it (bark_first), releasing the
 * cursor when it reads the first page.
 *
 * As in nbtree, currPos is invalidated and moreLeft/moreRight are set, so
 * the caller steps from *last_page to *next_page as a serial scan would.
 */
static bool
bark_parallel_seize(IndexScanDesc scan, BlockNumber *next_page,
					BlockNumber *last_page)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	BarkParallelScanDesc bps = bark_get_parallel_desc(scan);
	bool		status = true;
	bool		endscan = false;

	*next_page = InvalidBlockNumber;
	*last_page = InvalidBlockNumber;
	BarkScanPosUnpinIfPinned(so->currPos);
	BarkScanPosInvalidate(so->currPos);
	so->currPos.moreLeft = so->currPos.moreRight = true;

	for (;;)
	{
		bool		got = false;

		LWLockAcquire(&bps->bps_lock, LW_EXCLUSIVE);
		if (bps->bps_state == BARK_PARALLEL_DONE)
			status = false;
		else if (bps->bps_state == BARK_PARALLEL_IDLE &&
				 bps->bps_nextPage == BARK_P_NONE)
		{
			status = false;
			endscan = true;
		}
		else if (bps->bps_state != BARK_PARALLEL_ADVANCING)
		{
			/* NOT_INITIALIZED (we position the scan) or IDLE (next page) */
			if (bps->bps_state == BARK_PARALLEL_IDLE)
			{
				*next_page = bps->bps_nextPage;
				*last_page = bps->bps_lastPage;
			}
			bps->bps_state = BARK_PARALLEL_ADVANCING;
			got = true;
		}
		LWLockRelease(&bps->bps_lock);
		if (got || !status)
			break;
		ConditionVariableSleep(&bps->bps_cv, WAIT_EVENT_BARK_PAGE);
	}
	ConditionVariableCancelSleep();

	if (endscan)
		bark_parallel_done(scan);
	return status;
}

/*
 * Release the parallel scan: next_page (linked from curr_page) is the next
 * page another worker should read.  BARK_P_NONE ends the scan.
 */
static void
bark_parallel_release(IndexScanDesc scan, BlockNumber next_page,
					  BlockNumber curr_page)
{
	BarkParallelScanDesc bps = bark_get_parallel_desc(scan);

	LWLockAcquire(&bps->bps_lock, LW_EXCLUSIVE);
	bps->bps_nextPage = next_page;
	bps->bps_lastPage = curr_page;
	bps->bps_state = BARK_PARALLEL_IDLE;
	LWLockRelease(&bps->bps_lock);
	ConditionVariableSignal(&bps->bps_cv);
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

/* Make room for at least n more items in currPos.items. */
static void
bark_pos_reserve(BarkScanOpaque so, int n)
{
	BarkScanPosData *pos = &so->currPos;
	int			need = pos->lastItem + 1 + n;

	if (need <= pos->maxItems)
		return;
	pos->maxItems = Max(need, Max(pos->maxItems * 2, 256));
	if (pos->items == NULL)
		pos->items = MemoryContextAlloc(so->scanCxt,
										pos->maxItems * sizeof(BarkScanPosItem));
	else
		pos->items = repalloc(pos->items,
							  pos->maxItems * sizeof(BarkScanPosItem));
}

/*
 * Copy the key columns of a matching entry into the tuple workspace for an
 * index-only scan, once per entry, and return its offset there.  `resolved`
 * is the entry with its attributes readable (the full tuple of an OVERSIZED
 * entry).  A LIST or POSTING entry is copied without its body; nothing reads
 * xs_itup's t_tid, so its members all share the one copy.
 */
static uint32
bark_save_tuple(BarkScanOpaque so, IndexTuple entry, IndexTuple resolved)
{
	BarkScanPosData *pos = &so->currPos;
	BarkEntryShape shape = BarkEntryGetShape(entry);
	Size		len;
	uint32		off = pos->nextTupleOffset;
	IndexTuple	copy;

	if (shape == BARK_SHAPE_LIST || shape == BARK_SHAPE_POSTING)
		len = BarkEntryGetBodyOffset(entry);
	else if (shape == BARK_SHAPE_OVERSIZED)
		len = BarkOverflowGetRef(entry)->fulllen;
	else
		len = IndexTupleSize(entry);

	if (so->currTuples == NULL || off + MAXALIGN(len) > so->currTuplesSize)
	{
		Size		newsize = Max((Size) BLCKSZ,
								  Max(so->currTuplesSize * 2, off + MAXALIGN(len)));

		if (so->currTuples == NULL)
			so->currTuples = MemoryContextAlloc(so->scanCxt, newsize);
		else
			so->currTuples = repalloc(so->currTuples, newsize);
		so->currTuplesSize = newsize;
	}

	copy = (IndexTuple) (so->currTuples + off);
	memcpy(copy, resolved, len);
	if (shape == BARK_SHAPE_LIST || shape == BARK_SHAPE_POSTING)
	{
		/* Now a plain key tuple: drop the body's size and the alt-TID bit. */
		copy->t_info = (copy->t_info & ~(INDEX_SIZE_MASK | INDEX_AM_RESERVED_BIT)) |
			(uint16) len;
	}
	pos->nextTupleOffset = off + MAXALIGN(len);
	return off;
}

/*
 * Read the share-locked leaf in so->currPos.buf into so->currPos, as nbtree's
 * _bt_readpage does: copy every matching heap TID on the page into items[]
 * (and, for an index-only scan, each matching entry's key into the tuple
 * workspace), so that bark_gettuple returns them without touching the page
 * again.  A concurrent insert or split on the page cannot then shift the
 * scan's place on it.  Reads from offset `offnum` in direction dir (a
 * position bark_first found, or the end of the page).  Returns true when the
 * page holds at least one match.
 *
 * Clears moreRight (forward) or moreLeft (backward) when an entry past the
 * scan's bound in that direction shows no later page can match.  Releases the
 * parallel scan, publishing the next page, before doing any work.
 *
 * items[] is in index order whatever the direction: a backward read fills it
 * from the top down, ending with firstItem at the lowest slot used.
 */
static bool
bark_readpage(IndexScanDesc scan, ScanDirection dir, OffsetNumber offnum)
{
	Relation	index = scan->indexRelation;
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	BarkScanPosData *pos = &so->currPos;
	Page		page = BufferGetPage(pos->buf);
	BarkPageOpaque opaque = BarkPageGetOpaque(page);
	bool		forward = ScanDirectionIsForward(dir);
	OffsetNumber minoff = BarkPageFirstDataKey(opaque);
	OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
	BarkArrayKeyState *lead = so->leadArray;
	int			nmatched = 0;
	int			nitems = 0;

	Assert(!BarkPageIgnore(opaque));
	pos->currPage = BufferGetBlockNumber(pos->buf);
	pos->prevPage = opaque->bark_prev;
	pos->nextPage = opaque->bark_next;
	pos->dir = dir;
	pos->nextTupleOffset = 0;
	pos->arrayReseek = false;
	pos->firstItem = 0;
	pos->lastItem = -1;
	pos->itemIndex = -1;

	if (scan->parallel_scan != NULL)
		bark_parallel_release(scan, forward ? pos->nextPage : pos->prevPage,
							  pos->currPage);

	/*
	 * Predicate-lock the leaf for serializable transactions: a read here
	 * conflicts with a later insert onto the same page.
	 */
	PredicateLockPage(index, pos->currPage, scan->xs_snapshot);

	/* A leading SAOP whose elements are all behind us matches nothing more. */
	if (lead != NULL && lead->cur >= lead->nelems)
	{
		if (forward)
			pos->moreRight = false;
		else
			pos->moreLeft = false;
		return false;
	}

	if (forward)
		offnum = Max(offnum, minoff);
	else
		offnum = Min(offnum, maxoff);

	for (; forward ? offnum <= maxoff : offnum >= minoff;
		 offnum = forward ? OffsetNumberNext(offnum) : OffsetNumberPrev(offnum))
	{
		IndexTuple	itup = (IndexTuple) PageGetItem(page,
													PageGetItemId(page, offnum));
		bool		fetched;
		IndexTuple	resolved = bark_scan_resolve(index, itup, &fetched);
		int			ntids;
		uint32		tupoff = 0;

		/*
		 * Leading-array cursor, forward only: once the leading value is past
		 * the current element, that element's run is over; move the cursor to
		 * the first element not before this value.  The membership filter
		 * returns later elements' entries on this page correctly; the cursor
		 * only decides when to stop and where the next page should re-descend.
		 */
		if (lead != NULL && forward)
		{
			while (lead->cur < lead->nelems &&
				   bark_lead_cmp(scan, resolved, lead->elems[lead->cur]) > 0)
				lead->cur++;
			if (lead->cur >= lead->nelems)
			{
				if (fetched)
					pfree(resolved);
				pos->moreRight = false;
				break;
			}
		}

		if (!bark_tuple_matches(scan, resolved))
		{
			bool		stop = (lead == NULL || !forward) &&
				bark_past_bound(scan, resolved, dir);

			if (fetched)
				pfree(resolved);
			if (stop)
			{
				if (forward)
					pos->moreRight = false;
				else
					pos->moreLeft = false;
				break;
			}
			continue;
		}

		/* Expand the entry into one item per heap TID. */
		ntids = bark_entry_count_tids(itup);
		if (ntids > so->entryTidsAlloc)
		{
			if (so->entryTids)
				pfree(so->entryTids);
			so->entryTidsAlloc = Max(ntids, 64);
			so->entryTids = MemoryContextAlloc(so->scanCxt,
											   so->entryTidsAlloc * sizeof(ItemPointerData));
		}
		ntids = bark_entry_get_tids(itup, so->entryTids, so->entryTidsAlloc);
		if (scan->xs_want_itup)
			tupoff = bark_save_tuple(so, itup, resolved);
		if (fetched)
			pfree(resolved);

		bark_pos_reserve(so, ntids);
		if (forward)
		{
			for (int i = 0; i < ntids; i++)
			{
				BarkScanPosItem *item = &pos->items[pos->lastItem + 1 + i];

				item->heapTid = so->entryTids[i];
				item->indexOffset = offnum;
				item->tupleOffset = tupoff;
			}
			pos->lastItem += ntids;
		}
		else
		{
			/*
			 * Backward: entries arrive in descending order.  Collect them in
			 * reverse (last entry's highest TID first) and flip the whole
			 * array once the page is read.
			 */
			for (int i = 0; i < ntids; i++)
			{
				BarkScanPosItem *item = &pos->items[pos->lastItem + 1 + i];

				item->heapTid = so->entryTids[ntids - 1 - i];
				item->indexOffset = offnum;
				item->tupleOffset = tupoff;
			}
			pos->lastItem += ntids;
		}
		nmatched++;
		nitems += ntids;
	}

	if (!forward && nitems > 1)
	{
		for (int lo = 0, hi = pos->lastItem; lo < hi; lo++, hi--)
		{
			BarkScanPosItem tmp = pos->items[lo];

			pos->items[lo] = pos->items[hi];
			pos->items[hi] = tmp;
		}
	}

	/*
	 * Forward leading-array scan: when the next element sorts strictly after
	 * this page's high key, it lies beyond the right sibling; re-descend to
	 * it instead of reading every page in between.  Strictly: an element
	 * equal to the high key starts on the right sibling (or, in a run of
	 * equal keys that crosses the boundary, already on this page), and the
	 * plain step right reaches it.  A truncated or OVERSIZED high key does not
	 * carry the leading column inline, so take the plain step then.
	 */
	if (lead != NULL && forward && pos->moreRight &&
		!BarkPageRightmost(opaque) && lead->cur < lead->nelems)
	{
		IndexTuple	hikey = (IndexTuple) PageGetItem(page,
													 PageGetItemId(page, BARK_P_HIKEY));

		if (BarkEntryGetShape(hikey) != BARK_SHAPE_OVERSIZED &&
			BarkEntryGetPivotNAtts(hikey) >= 1 &&
			bark_lead_cmp(scan, hikey, lead->elems[lead->cur]) < 0)
			pos->arrayReseek = true;
	}
	pos->arrayCur = lead != NULL ? lead->cur : 0;

	if (nitems == 0)
		return false;

	pos->itemIndex = forward ? -1 : pos->lastItem + 1;
	(void) nmatched;
	return true;
}

/* Return currPos's item at itemIndex to the executor. */
static void
bark_saveitem(IndexScanDesc scan)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	BarkScanPosItem *item = &so->currPos.items[so->currPos.itemIndex];

	scan->xs_heaptid = item->heapTid;
	scan->xs_recheck = false;
	if (scan->xs_want_itup)
		scan->xs_itup = (IndexTuple) (so->currTuples + item->tupleOffset);
}

/*
 * Drop the lock on currPos.buf, and the pin too when so->dropPin
 * (nbtree's _bt_drop_lock_and_maybe_pin).
 */
static void
bark_drop_lock_and_maybe_pin(BarkScanOpaque so)
{
	if (!so->dropPin)
	{
		LockBuffer(so->currPos.buf, BUFFER_LOCK_UNLOCK);
		return;
	}
	UnlockReleaseBuffer(so->currPos.buf);
	so->currPos.buf = InvalidBuffer;
}

/*
 * Lock the left sibling `*blkno` of `lastcurrblkno` for a backward step,
 * recovering from concurrent splits and deletions, as nbtree's
 * _bt_lock_and_validate_left does.  The left page is the right one when its
 * right link still points at lastcurrblkno; if it split since, walk right
 * from it to the page that does; if lastcurrblkno itself was deleted, start
 * again from the page that took over its key space.  Returns the page
 * share-locked, with *blkno set, or InvalidBuffer when there is no page to
 * the left.  The page returned may be half-dead; the caller steps past it.
 */
static Buffer
bark_lock_and_validate_left(Relation index, BlockNumber *blkno,
							BlockNumber lastcurrblkno)
{
	BlockNumber origblkno = *blkno;

	for (;;)
	{
		Buffer		buf;
		Page		page;
		BarkPageOpaque opaque;
		int			tries;

		CHECK_FOR_INTERRUPTS();
		buf = ReadBuffer(index, *blkno);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);
		opaque = BarkPageGetOpaque(page);

		/*
		 * Walk right to the page whose right link is lastcurrblkno, at most
		 * four hops; past that, lastcurrblkno was most likely deleted.  Test
		 * BARK_DELETED, not ignorable: a half-dead page is still linked.
		 */
		tries = 0;
		for (;;)
		{
			if (!BarkPageIsDeleted(opaque) &&
				opaque->bark_next == lastcurrblkno)
				return buf;
			if (BarkPageRightmost(opaque) || ++tries > 4)
				break;
			*blkno = opaque->bark_next;
			LockBuffer(buf, BUFFER_LOCK_UNLOCK);
			buf = ReleaseAndReadBuffer(buf, index, *blkno);
			LockBuffer(buf, BUFFER_LOCK_SHARE);
			page = BufferGetPage(buf);
			opaque = BarkPageGetOpaque(page);
		}

		/* See what became of lastcurrblkno. */
		LockBuffer(buf, BUFFER_LOCK_UNLOCK);
		buf = ReleaseAndReadBuffer(buf, index, lastcurrblkno);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);
		opaque = BarkPageGetOpaque(page);
		if (BarkPageIsDeleted(opaque))
		{
			/*
			 * Deleted: its key space moved to the first live page to its
			 * right, and stepping left from that page goes where we want.
			 */
			for (;;)
			{
				if (BarkPageRightmost(opaque))
					elog(ERROR, "fell off the end of BARK index \"%s\"",
						 RelationGetRelationName(index));
				lastcurrblkno = opaque->bark_next;
				LockBuffer(buf, BUFFER_LOCK_UNLOCK);
				buf = ReleaseAndReadBuffer(buf, index, lastcurrblkno);
				LockBuffer(buf, BUFFER_LOCK_SHARE);
				page = BufferGetPage(buf);
				opaque = BarkPageGetOpaque(page);
				if (!BarkPageIsDeleted(opaque))
					break;
			}
		}
		else if (opaque->bark_prev == origblkno)
		{
			/* Not deleted, and its left link did not move: corrupt. */
			elog(ERROR, "could not find left sibling of block %u in BARK index \"%s\"",
				 lastcurrblkno, RelationGetRelationName(index));
		}

		if (BarkPageLeftmost(opaque))
		{
			UnlockReleaseBuffer(buf);
			return InvalidBuffer;
		}
		*blkno = origblkno = opaque->bark_prev;
		UnlockReleaseBuffer(buf);
	}
}

/*
 * Read pages from blkno in direction dir until one has matches, as nbtree's
 * _bt_readnextpage does.  lastcurrblkno is the page blkno was linked from.
 * Returns true with currPos filled (lock dropped, pin per dropPin), or false
 * with currPos invalidated when nothing more matches in that direction.
 * `seized` says the caller already holds the parallel scan.
 */
static bool
bark_readnextpage(IndexScanDesc scan, BlockNumber blkno,
				  BlockNumber lastcurrblkno, ScanDirection dir, bool seized)
{
	Relation	index = scan->indexRelation;
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	bool		forward = ScanDirectionIsForward(dir);

	Assert(!BarkScanPosIsPinned(so->currPos));

	if (forward)
		so->currPos.moreLeft = true;
	else
		so->currPos.moreRight = true;

	for (;;)
	{
		Page		page;
		BarkPageOpaque opaque;

		if (blkno == BARK_P_NONE ||
			(forward ? !so->currPos.moreRight : !so->currPos.moreLeft))
		{
			BarkScanPosInvalidate(so->currPos);
			bark_parallel_done(scan);
			return false;
		}

		if (!seized && scan->parallel_scan != NULL &&
			!bark_parallel_seize(scan, &blkno, &lastcurrblkno))
		{
			BarkScanPosInvalidate(so->currPos);
			return false;
		}
		Assert(BlockNumberIsValid(blkno));

		if (forward)
		{
			CHECK_FOR_INTERRUPTS();
			so->currPos.buf = ReadBuffer(index, blkno);
			LockBuffer(so->currPos.buf, BUFFER_LOCK_SHARE);
		}
		else
		{
			so->currPos.buf = bark_lock_and_validate_left(index, &blkno,
														  lastcurrblkno);
			if (so->currPos.buf == InvalidBuffer)
			{
				BarkScanPosInvalidate(so->currPos);
				bark_parallel_done(scan);
				return false;
			}
		}

		page = BufferGetPage(so->currPos.buf);
		opaque = BarkPageGetOpaque(page);
		lastcurrblkno = blkno;
		if (!BarkPageIgnore(opaque))
		{
			if (bark_readpage(scan, dir, forward ? BarkPageFirstDataKey(opaque) :
							  PageGetMaxOffsetNumber(page)))
				break;
			blkno = forward ? so->currPos.nextPage : so->currPos.prevPage;
		}
		else
		{
			blkno = forward ? opaque->bark_next : opaque->bark_prev;
			if (scan->parallel_scan != NULL)
				bark_parallel_release(scan, blkno, lastcurrblkno);
		}

		UnlockReleaseBuffer(so->currPos.buf);
		so->currPos.buf = InvalidBuffer;
		seized = false;
	}

	bark_drop_lock_and_maybe_pin(so);
	return true;
}

/*
 * Position the scan on its first page with matches in direction dir and
 * return true, or return false when nothing matches.  nbtree's _bt_first,
 * for BARK's bounds: descend to the start leaf (bark_start_leaf), find the
 * first possibly matching offset on it, and read pages from there.
 */
static bool
bark_first(IndexScanDesc scan, ScanDirection dir)
{
	Relation	index = scan->indexRelation;
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	BlockNumber blkno = InvalidBlockNumber;
	BlockNumber lastcurrblkno = InvalidBlockNumber;
	Buffer		buf;
	Page		page;
	BarkPageOpaque opaque;
	OffsetNumber offnum;

	Assert(!BarkScanPosIsValid(so->currPos));

	pgstat_count_index_scan(index);
	if (scan->instrument)
		scan->instrument->nsearches++;

	/* An empty IN-list (or all-NULL array) on the leading column: no rows. */
	if (so->leadArray != NULL && so->leadArray->nelems == 0)
		return false;

	if (scan->parallel_scan != NULL)
	{
		if (!bark_parallel_seize(scan, &blkno, &lastcurrblkno))
			return false;
		if (BlockNumberIsValid(blkno))
		{
			/* The scan is already under way: read the page we were handed. */
			return bark_readnextpage(scan, blkno, lastcurrblkno, dir, true);
		}
		/* We seized a scan nobody has started: position it ourselves. */
	}

	so->currPos.moreLeft = ScanDirectionIsBackward(dir);
	so->currPos.moreRight = ScanDirectionIsForward(dir);
	if (so->leadArray != NULL && ScanDirectionIsBackward(dir))
		so->leadArray->cur = 0;

	buf = bark_start_leaf(scan, dir);
	if (!BufferIsValid(buf))
	{
		/* Empty index.  A serializable scan must lock the whole relation. */
		PredicateLockRelation(index, scan->xs_snapshot);
		bark_parallel_done(scan);
		return false;
	}

	page = BufferGetPage(buf);
	opaque = BarkPageGetOpaque(page);
	offnum = ScanDirectionIsForward(dir) ? BarkPageFirstDataKey(opaque) :
		PageGetMaxOffsetNumber(page);
	so->currPos.buf = buf;

	if (bark_readpage(scan, dir, offnum))
	{
		bark_drop_lock_and_maybe_pin(so);
		return true;
	}

	/* No match on the first page: step on, as nbtree's _bt_steppage does. */
	blkno = ScanDirectionIsForward(dir) ? so->currPos.nextPage :
		so->currPos.prevPage;
	lastcurrblkno = so->currPos.currPage;
	UnlockReleaseBuffer(so->currPos.buf);
	so->currPos.buf = InvalidBuffer;
	if (so->currPos.arrayReseek && ScanDirectionIsForward(dir))
		return bark_array_reseek(scan, dir);
	return bark_readnextpage(scan, blkno, lastcurrblkno, dir, false);
}

/*
 * Forward leading-array scan whose next element lies beyond the right
 * sibling: re-descend to it.  The cursor was left on that element by
 * bark_readpage.  A parallel scan never takes this path (each worker reads
 * whatever page the shared cursor hands it).
 */
static bool
bark_array_reseek(IndexScanDesc scan, ScanDirection dir)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;

	Assert(scan->parallel_scan == NULL && so->leadArray != NULL);
	so->leadArray->cur = so->currPos.arrayCur;
	BarkScanPosInvalidate(so->currPos);
	return bark_first(scan, dir);
}

/*
 * Step to the next page with matches in direction dir (nbtree's
 * _bt_steppage).  currPos is valid on entry, unlocked, pinned only when
 * !dropPin.
 */
static bool
bark_steppage(IndexScanDesc scan, ScanDirection dir)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	BlockNumber blkno;
	BlockNumber lastcurrblkno;

	Assert(BarkScanPosIsValid(so->currPos));
	BarkScanPosUnpinIfPinned(so->currPos);

	blkno = ScanDirectionIsForward(dir) ? so->currPos.nextPage :
		so->currPos.prevPage;
	lastcurrblkno = so->currPos.currPage;

	if (so->leadArray != NULL)
	{
		if (ScanDirectionIsForward(dir) && so->currPos.dir == dir &&
			so->currPos.arrayReseek && scan->parallel_scan == NULL)
			return bark_array_reseek(scan, dir);

		/*
		 * The cursor drives only forward reads; a forward read after a
		 * backward one, or after a reversal, restarts it from the position's
		 * saved value (0 after a backward read), which is never ahead of the
		 * page we step to.
		 */
		so->leadArray->cur = so->currPos.dir == dir ? so->currPos.arrayCur : 0;
	}

	return bark_readnextpage(scan, blkno, lastcurrblkno, dir, false);
}

bool
bark_gettuple(IndexScanDesc scan, ScanDirection dir)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;

	/*
	 * Ordered-operator (KNN) scan: distances dictate the order, not the key
	 * order, so hand off to the two-sided outward merge.  The executor only
	 * ever drives a KNN scan forward.
	 */
	if (scan->numberOfOrderBys > 0)
		return bark_knn_gettuple(scan);

	if (!BarkScanPosIsValid(so->currPos))
	{
		/*
		 * Not positioned: either the first call, or the scan ran off one end.
		 * nbtree restarts from that end in both cases (a scroll cursor that
		 * fetched past the last row and then fetches backward); so do we.
		 */
		if (!bark_first(scan, dir))
			return false;
	}
	else
	{
		/*
		 * Advance within the page read last.  A reversal of direction
		 * continues from the same item, in the new direction, which is what a
		 * scroll cursor needs; only running off either end steps to a page.
		 */
		if (ScanDirectionIsForward(dir))
		{
			if (++so->currPos.itemIndex > so->currPos.lastItem)
			{
				if (!bark_steppage(scan, dir))
					return false;
			}
		}
		else
		{
			if (--so->currPos.itemIndex < so->currPos.firstItem)
			{
				if (!bark_steppage(scan, dir))
					return false;
			}
		}
	}

	/* A fresh page starts at its first item in the scan direction. */
	if (so->currPos.itemIndex < so->currPos.firstItem)
		so->currPos.itemIndex = so->currPos.firstItem;
	else if (so->currPos.itemIndex > so->currPos.lastItem)
		so->currPos.itemIndex = so->currPos.lastItem;

	bark_saveitem(scan);
	return true;
}

/*
 * amgetbitmap: add every matching heap TID to the TIDBitmap.  Reads pages
 * through the same positioning and page reads as a forward gettuple scan,
 * as nbtree's btgetbitmap does, adding each page's items at once.  BARK is
 * exact, so every TID is added with recheck=false.
 */
int64
bark_getbitmap(IndexScanDesc scan, TIDBitmap *tbm)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	int64		ntids = 0;

	if (!bark_first(scan, ForwardScanDirection))
		return 0;
	do
	{
		for (int i = so->currPos.firstItem; i <= so->currPos.lastItem; i++)
			tbm_add_tuples(tbm, &so->currPos.items[i].heapTid, 1, false);
		ntids += so->currPos.lastItem - so->currPos.firstItem + 1;
	} while (bark_steppage(scan, ForwardScanDirection));
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
	BarkScanPosUnpinIfPinned(so->currPos);
	if (so->currPos.items)
		pfree(so->currPos.items);
	if (so->currTuples)
		pfree(so->currTuples);
	if (so->entryTids)
		pfree(so->entryTids);
	if (so->keyCmp)
		pfree(so->keyCmp);
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
	bps->bps_lastPage = InvalidBlockNumber;
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
	bps->bps_lastPage = InvalidBlockNumber;
	bps->bps_state = BARK_PARALLEL_NOT_INITIALIZED;
	LWLockRelease(&bps->bps_lock);
}
