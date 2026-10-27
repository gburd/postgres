/*-------------------------------------------------------------------------
 *
 * barkscan.c
 *	  Index scan for the BARK index access method.
 *
 * A BARK scan positions on a leaf and walks the right-link chain, returning
 * the heap TID of each entry that satisfies the scan keys.  When the keys
 * provide a lower bound on the leading index columns (an =, >, or >= qual, or
 * the current element of a `col = ANY(array)` SAOP; see below), the scan
 * descends the tree to the first leaf that can contain a match: the descent
 * key uses every usable leading column (bark_make_bound), so a selective
 * second-column bound such as WHERE a = 5 AND b >= 100 lands near the match
 * rather than at the first a = 5 leaf.  Otherwise it starts at the leftmost
 * leaf.  A forward scan also stops early once the leading column passes an
 * upper bound (an =, <, or <= qual: bark_past_bound), so a bounded scan
 * reads only the matching span, not the rest of the index.  A row comparison
 * such as (a, b) > (5, 10) positions and stops the scan the same way on its
 * leading members (bark_row_prefix), so keyset pagination reads only the
 * pages it returns.
 *
 * Modeled on nbtree's scan (nbtsearch.c _bt_first / _bt_next / _bt_readpage),
 * simplified for the SINGLE entry shape: every leaf entry is one heap TID in
 * t_tid.  The index is exact, so no heap recheck is required.
 *
 * ScalarArrayOp (SAOP) quals (`col = ANY(array)` / `col IN (...)`) are pushed
 * into a single scan: bark_rescan sorts and de-duplicates each array into the
 * index's key order (see BarkArrayKeyState in bark.h) and the scan visits the
 * matching keys in index order -- a merged sequence of equality scans.  Every
 * array key filters per tuple by membership.  The equality keys of the
 * leading columns, when one of them is an array, also drive positioning
 * (nbtree's required array keys): the scan visits the combinations of their
 * elements in index order, in either direction, and re-descends to the next
 * one when it starts beyond the page, rather than reading the whole index.
 * An inequality array (`col < ANY(array)`) is reduced to a plain key on its
 * extreme element instead.
 *
 * A backward scan is the mirror image: it descends to the last leaf that can
 * hold a match under an upper bound, and stops once the leading column passes
 * a lower bound.
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
#include "access/detoast.h"
#include "access/gin.h"
#include "access/htup_details.h"
#include "access/relscan.h"
#include "access/skey.h"
#include "catalog/pg_type.h"
#include "common/int.h"
#include "lib/binaryheap.h"
#include "lib/qunique.h"
#include "miscadmin.h"
#include "nodes/tidbitmap.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "storage/lwlock.h"
#include "storage/predicate.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/hsearch.h"
#include "utils/injection_point.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/skipsupport.h"
#include "utils/snapmgr.h"
#include "utils/wait_event.h"

static int	bark_key_cmp(IndexScanDesc scan, int i, IndexTuple itup);
static bool bark_prefix_past_bound(IndexScanDesc scan, IndexTuple itup,
								   ScanDirection dir);
static void bark_killitems(IndexScanDesc scan);
static bool bark_mk_gin_gettuple(IndexScanDesc scan, ScanDirection dir);
static bool bark_key_bounds(IndexScanDesc scan, ScanKey sk, bool *lower,
							bool *upper);

/*
 * Resolve a leaf entry to a tuple whose key and INCLUDE attributes can be read
 * with index_getattr.  For an OVERSIZED entry the attributes live out of line,
 * so fetch the full tuple from the overflow chain; the caller pfrees the result
 * when *fetched is set.  Every other shape carries its attributes inline, so
 * the entry is returned unchanged.
 */
IndexTuple
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
 * (a) filters every tuple by array membership and (b) when the equality keys
 * of the leading columns include an array, walks the combinations of those
 * keys' elements in index order, so it visits the matching keys without
 * reading the whole index -- a merged sequence of equality scans.  An
 * inequality array (col < ANY(array)) is not a set of values but a single
 * bound, so it is reduced to a plain key instead.
 * ---------------------------------------------------------------------------
 */

/* Comparator state for sorting/searching array elements. */
typedef struct BarkArraySortCtx
{
	FmgrInfo   *cmp;			/* ORDER proc (sortproc or cmpproc) */
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

/*
 * Is column value `datum` one of the array key's elements?  Binary search
 * over the sorted elements, comparing through cmpproc.
 */
static bool
bark_array_contains(BarkScanOpaque so, BarkArrayKeyState *ak, Datum datum)
{
	BarkKeyColumn *col = &so->keyinfo->cols[ak->attno - 1];
	BarkArraySortCtx ctx;
	int			lo = 0;
	int			hi = ak->nelems - 1;

	ctx.cmp = &ak->cmpproc;
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
 * Set *finfo, in the array context, to the ORDER proc of index column attno's
 * opfamily that compares lefttype with righttype.  The planner only builds a
 * SAOP index qual from an operator of the column's opfamily, and a btree
 * opfamily has an ORDER proc for each pair of types it has operators for, so
 * a missing one means a broken opfamily.
 */
static void
bark_array_proc(IndexScanDesc scan, AttrNumber attno, Oid lefttype,
				Oid righttype, FmgrInfo *finfo)
{
	Relation	index = scan->indexRelation;
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	Oid			opcintype = index->rd_opcintype[attno - 1];
	Oid			proc;

	if (lefttype == opcintype && righttype == opcintype)
	{
		fmgr_info_copy(finfo, &so->keyinfo->cols[attno - 1].cmp, so->arrayCxt);
		return;
	}
	proc = get_opfamily_proc(index->rd_opfamily[attno - 1], lefttype,
							 righttype, BARK_ORDER_PROC);
	if (!OidIsValid(proc))
		elog(ERROR, "missing support function %d(%u,%u) for attribute %d of index \"%s\"",
			 BARK_ORDER_PROC, lefttype, righttype, attno,
			 RelationGetRelationName(index));
	fmgr_info_cxt(proc, finfo, so->arrayCxt);
}

/* The context of the state rescan rebuilds, created when first needed. */
static MemoryContext
bark_array_cxt(BarkScanOpaque so)
{
	if (so->arrayCxt == NULL)
		so->arrayCxt = AllocSetContextCreate(so->scanCxt, "BARK array keys",
											 ALLOCSET_SMALL_SIZES);
	return so->arrayCxt;
}

/*
 * Preprocess every SK_SEARCHARRAY scankey, as nbtree's
 * _bt_preprocess_array_keys does.  NULL array elements are dropped: btree
 * operators are strict, so a NULL element never matches.
 *
 * An equality array becomes a BarkArrayKeyState: its elements sorted into
 * the index's key order for the column, without duplicates.  An array with no
 * non-NULL element leaves nelems == 0, which makes the scan return nothing.
 *
 * An inequality array is satisfied exactly when the column satisfies the
 * operator against the array's extreme element in value order (whatever the
 * column's DESC option): the greatest for < and <=, the least for > and >=.
 * The key is rewritten in place into that plain key, keeping its strategy
 * and subtype, so bark_setup_key_procs gives it an ORDER proc and it bounds
 * the scan like any other key.  Its argument is a copy of the element in the
 * array context, which lives until bark_rescan next overwrites scan->keyData
 * with the executor's keys.  An inequality array with no non-NULL element
 * stays an array key with no elements.
 *
 * Elements are of the operator's right-hand type, sk_subtype, which can
 * differ from the column's type; see BarkArrayKeyState.  Everything built
 * here lives in so->arrayCxt, reset by bark_free_array_keys.
 */
static void
bark_setup_array_keys(IndexScanDesc scan)
{
	Relation	index = scan->indexRelation;
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	int			narrays = 0;
	MemoryContext oldcxt;

	so->arrayKeys = NULL;
	so->numArrayKeys = 0;

	for (int i = 0; i < so->numberOfKeys; i++)
		if (scan->keyData[i].sk_flags & SK_SEARCHARRAY)
			narrays++;
	if (narrays == 0)
		return;

	oldcxt = MemoryContextSwitchTo(bark_array_cxt(so));
	so->arrayKeys = palloc0_array(BarkArrayKeyState, narrays);

	for (int i = 0; i < so->numberOfKeys; i++)
	{
		ScanKey		sk = &scan->keyData[i];
		BarkArrayKeyState *ak;
		Oid			opcintype;
		Oid			elemtype;
		int16		elmlen = 0;
		bool		elmbyval = true;
		char		elmalign;
		Datum	   *elems = NULL;
		bool	   *nulls;
		int			nelems = 0;
		BarkArraySortCtx ctx;

		if (!(sk->sk_flags & SK_SEARCHARRAY))
			continue;

		opcintype = index->rd_opcintype[sk->sk_attno - 1];
		elemtype = OidIsValid(sk->sk_subtype) ? sk->sk_subtype : opcintype;

		/* A NULL array argument matches nothing: leave nelems == 0. */
		if (!(sk->sk_flags & SK_ISNULL))
		{
			ArrayType  *arr = DatumGetArrayTypeP(sk->sk_argument);
			int			nraw;

			get_typlenbyvalalign(ARR_ELEMTYPE(arr), &elmlen, &elmbyval,
								 &elmalign);
			deconstruct_array(arr, ARR_ELEMTYPE(arr), elmlen, elmbyval,
							  elmalign, &elems, &nulls, &nraw);
			for (int e = 0; e < nraw; e++)
				if (!nulls[e])
					elems[nelems++] = elems[e];
		}

		ctx.collation = so->keyinfo->cols[sk->sk_attno - 1].collation;

		if (sk->sk_strategy != BTEqualStrategyNumber && nelems > 0)
		{
			FmgrInfo	sortproc;
			bool		greatest = (sk->sk_strategy == BTLessStrategyNumber ||
									sk->sk_strategy == BTLessEqualStrategyNumber);
			Datum		extreme = elems[0];

			bark_array_proc(scan, sk->sk_attno, elemtype, elemtype, &sortproc);
			for (int e = 1; e < nelems; e++)
			{
				int32		c = DatumGetInt32(FunctionCall2Coll(&sortproc,
																ctx.collation,
																elems[e],
																extreme));

				if (greatest ? c > 0 : c < 0)
					extreme = elems[e];
			}
			sk->sk_argument = datumCopy(extreme, elmbyval, elmlen);
			sk->sk_flags &= ~SK_SEARCHARRAY;
			continue;
		}

		ak = &so->arrayKeys[so->numArrayKeys++];
		ak->scankeyidx = i;
		ak->attno = sk->sk_attno;
		ak->cur = 0;
		ak->elems = elems;
		ak->nelems = nelems;
		if (nelems == 0)
			continue;

		/*
		 * Sort with the element type's comparator, then walk the index with
		 * the cross-type one: the two agree, by the btree opfamily contract.
		 */
		bark_array_proc(scan, ak->attno, opcintype, elemtype, &ak->cmpproc);
		bark_array_proc(scan, ak->attno, elemtype, elemtype, &ak->sortproc);
		ctx.cmp = &ak->sortproc;
		ctx.reverse = so->keyinfo->cols[ak->attno - 1].reverse;
		if (nelems > 1)
		{
			qsort_arg(elems, nelems, sizeof(Datum), bark_array_cmp, &ctx);
			ak->nelems = qunique_arg(elems, nelems, sizeof(Datum),
									 bark_array_cmp, &ctx);
		}
	}
	MemoryContextSwitchTo(oldcxt);
}

/* Release SAOP state before rebuilding it on rescan. */
static void
bark_free_array_keys(BarkScanOpaque so)
{
	if (so->arrayCxt != NULL)
		MemoryContextReset(so->arrayCxt);
	so->arrayKeys = NULL;
	so->numArrayKeys = 0;
	so->reqKeys = NULL;
	so->numReqKeys = 0;
}

/*
 * ---------------------------------------------------------------------------
 * Multikey scans ("Scans" under "Multikey keys" in BARK-Design.mediawiki)
 *
 * In an index with an extracted column, a row has one entry per key of the
 * column's value, and the class's operators are not btree comparisons, so
 * the keys on that column never reach the btree-style code above: rescan
 * moves them after so->numberOfKeys.  Procedure 8 turns the first of them
 * into boundaries over the column's keys (a ScalarArrayOp, once per
 * element, unioned), which bark_mk_setup sorts into index order.  When every
 * column before the extracted one has an equality, the boundaries are the
 * last required key (BarkArrayKeyState.mk): the scan descends to each in
 * turn, as to an equality array's elements.  Otherwise they only filter.
 * Either way bark_mk_filter decides which entries are inside one, procedure
 * 9 may drop or doubt an entry, and the seen set drops a row's later
 * entries.
 * ---------------------------------------------------------------------------
 */

/*
 * One boundary over the extracted column's keys, in index order: from start
 * to end, each excluded when strict, unbounded where absent.  On a DESC
 * column start is procedure 8's upper search element.  A nulls boundary is
 * instead the NULL entries, wherever the column's NULLS option puts them.
 */
typedef struct BarkMkBoundary
{
	Datum		start;
	Datum		end;
	bool		hasstart;
	bool		hasend;
	bool		startstrict;
	bool		endstrict;
	bool		nulls;
	bool		point;			/* one key, or the NULL entries */
	bool		recheck;		/* return its rows with xs_recheck */
	bool		callproc9;		/* run procedure 9 on its entries */
	bool		partial;		/* a GIN partial-match key: procedure 12
								 * decides */
	Datum		partialkey;		/* ... its query key */
	StrategyNumber strategy;	/* the scan key's, for procedure 9 */
	Pointer		extra;			/* procedure 8's extra, for procedure 9 */
} BarkMkBoundary;

/*
 * The radix tree of the seen set (see bark_mk_seen): key bark_tid_to_key(tid)
 * >> 6, value one bit per TID of that window of 64.
 */
#define RT_PREFIX bark_seen
#define RT_SCOPE static inline
#define RT_DECLARE
#define RT_DEFINE
#define RT_VALUE_TYPE uint64
#include "lib/radixtree.h"

typedef struct BarkMkScanState
{
	AttrNumber	attno;			/* the extracted column */
	BarkKeyColumn *col;			/* its comparator and order */
	FmgrInfo   *extractquery;	/* procedure 8 */
	bool		queryflags;		/* it takes BarkQueryFlags, a 7th argument */
	FmgrInfo   *indexrecheck;	/* procedure 9, or NULL */

	/*
	 * GIN's query interface (procedures 10-13), which the column's keys use
	 * when its class has procedure 10 and not 8 (gin), through
	 * bark_mk_gin_items.
	 */
	bool		gin;
	FmgrInfo   *ginquery;		/* procedure 10, extractQuery */
	FmgrInfo   *ginconsistent;	/* procedure 11, consistent, or NULL */
	FmgrInfo   *gincompare;		/* procedure 12, comparePartial, or NULL */
	FmgrInfo   *gintriconsistent;	/* procedure 13, triConsistent, or NULL */
	bool		partialpast;	/* procedure 12 said the range is over */

	/*
	 * A read of an open-ended partial-match boundary (bark_mk_gin_first) in a
	 * forward scan of an ascending leading column: once one key is inside it,
	 * every later non-NULL key is (instart), so bark_mk_cmp stops comparing
	 * keys with its start (startlatch says the read qualifies).
	 */
	bool		startlatch;
	bool		instart;

	/*
	 * An index scan through GIN's interface (bark_mk_gin_gettuple): the rows
	 * of bark_mk_gin_items, put into a TIDBitmap bounded by work_mem in
	 * ginCxt on the first call after a rescan; an iterator over it, at page
	 * ginpage (in iteration order) whose offsets are in ginoffs; the number
	 * of pages once an iteration has ended (else -1); the place, page ginp
	 * and offset gini (ginp -1 before the first row, ginnpages past the
	 * last); and the marked place.
	 */
	bool		ginready;
	TIDBitmap  *gintbm;
	TBMPrivateIterator *giniter;
	int			ginpage;
	int			ginnpages;
	BlockNumber ginblkno;
	bool		ginrecheck;
	int			ginnoffs;
	OffsetNumber ginoffs[TBM_MAX_TUPLES_PER_PAGE];
	int			ginfold;		/* bitmap pages per heap block, see
								 * bark_mk_gin_tbm_add */
	bool		ginfolding;		/* bark_mk_gin_items is filling gintbm, and
								 * ginfold > 1 */

	/*
	 * The chunks of the GIN-interface scan's exact streams (BarkGinChunk):
	 * allocated one at a time in ginChunkCxt, below AllocSet's chunk limit,
	 * and kept on ginfree when a stream is done, so a query reuses them key
	 * after key, and the scan rescan after rescan.
	 */
	MemoryContext ginChunkCxt;
	struct BarkGinChunk *ginfree;
	uint16		ginstride;		/* the index's TID stride */
	int			ginp;
	int			gini;
	int			ginmarkp;
	int			ginmarki;
	MemoryContext ginCxt;
	ScanKey		keys;			/* the scan keys on the column ... */
	int			nkeys;			/* ... and their number */

	/* the first key's boundaries, sorted and disjoint, in arrayCxt */
	BarkMkBoundary *bounds;
	int			nbounds;
	bool		recheckall;		/* several keys: xs_recheck on every row */
	int			matched;		/* boundary of the entry bark_mk_filter passed */

	/*
	 * An ordering key on the column (bark_mk_order_setup): the scan walks its
	 * boundaries in orderdir and reports each row's first key as its ORDER BY
	 * value, NULL for every row when ordernull.  The npending boundaries in
	 * pending are read after them, by a second positioning, and their rows
	 * come back with a NULL ORDER BY value (bark_mk_order_rest): the NULL
	 * entries when they come first in the walk, or with orderrest the whole
	 * column, for the rows the boundaries do not hold.
	 */
	bool		order;
	bool		ordernull;
	bool		orderrest;		/* procedure 8 set BarkQueryFlags.orderrest */
	ScanDirection orderdir;
	int			npending;
	BarkMkBoundary pending[2];

	/*
	 * The seen set (bark_mk_seen).  seen is NULL until a TID goes in, and
	 * lives in seenCxt, which is reset to empty it.  groupvals are the
	 * columns before the extracted one of the rows in it.
	 */
	bool		useseen;		/* a row may match through two entries */
	bool		oneentry;		/* procedure 8 said it cannot */
	bool		bitmap;			/* an amgetbitmap scan, which needs none */
	bool		started;		/* positioned since rescan, in direction dir */
	ScanDirection dir;
	MemoryContext seenCxt;
	bark_seen_radix_tree *seen;
	uint64		nseen;			/* TIDs in it */
	bool		havegroup;
	MemoryContext groupCxt;		/* groupvals' copies */
	Datum		groupvals[INDEX_MAX_KEYS];
	bool		groupnulls[INDEX_MAX_KEYS];

	/*
	 * Rule D ("Versioned multikey scans" in the README): in an index with
	 * BARK_META_MKTAGS scanned through a table AM, the seen set is R, the
	 * rows the table AM returned, filled from its verdict on each entry the
	 * scan returned (bark_mk_settle), not when a page is read; rejected holds
	 * J, the (TID, tag) pairs it rejected under an MVCC snapshot.  A page
	 * read keeps every accepted entry, and bark_mk_admit skips at return time
	 * the entries of rows in R and the pairs in J.  verdictdue says an entry
	 * was returned whose verdict is not read yet; pendingtid and pendingtag
	 * name it.  A group change (bark_mk_group) is found when the page is read
	 * and recorded on the item (BARK_ITEM_NEWGROUP); R and J empty when the
	 * scan returns from such an item.
	 */
	bool		ruled;
	bool		verdictdue;
	ItemPointerData pendingtid;
	IndexVersionTag pendingtag;
	HTAB	   *rejected;		/* J, in seenCxt, or NULL while empty */

	/*
	 * Mark and restore (bark_markpos): while marked, every TID added to the
	 * set goes on the undo list, which bark_restrpos takes out again, and the
	 * set is never emptied for a new group.
	 */
	bool		marked;
	ItemPointerData *undo;
	int			nundo;
	int			maxundo;

	/*
	 * Index-only scans: the row an entry stands for, in xs_hitup, formed in
	 * hitupCxt with hitupdesc, the index's columns with the extracted one as
	 * the column's type (which the planner never reads; see
	 * bark_mk_form_hitup).
	 */
	TupleDesc	hitupdesc;
	MemoryContext hitupCxt;
} BarkMkScanState;

/* Compare two keys of the extracted column in index order. */
static inline int
bark_mk_keycmp(BarkMkScanState *mk, Datum a, Datum b)
{
	int32		c = DatumGetInt32(FunctionCall2Coll(&mk->col->cmp,
													mk->col->collation, a, b));

	if (mk->col->reverse)
		INVERT_COMPARE_RESULT(c);
	return c;
}

/*
 * Where does a value of the extracted column (isnull) sort against boundary
 * b in index order: <0 before it, 0 inside it, >0 after it.  A point's start
 * and end are the same key, so one comparison with it answers; this is the
 * compare every entry of an = or GIN-interface key pays.
 */
static int
bark_mk_cmp(BarkMkScanState *mk, Datum datum, bool isnull,
			const BarkMkBoundary *b)
{
	int			c;

	if (b->nulls || isnull)
	{
		if (b->nulls && isnull)
			return 0;
		return (isnull == mk->col->nulls_first) ? -1 : 1;
	}
	/* A point is non-strict at both ends, which are the one key. */
	if (b->point)
		return bark_mk_keycmp(mk, datum, b->start);
	/* Inside an open boundary read forward (see BarkMkScanState). */
	if (mk->instart)
		return 0;
	if (b->hasstart)
	{
		c = bark_mk_keycmp(mk, datum, b->start);
		if (c < 0 || (c == 0 && b->startstrict))
			return -1;
	}
	if (mk->startlatch)
	{
		mk->instart = true;
		return 0;
	}
	if (b->hasend)
	{
		c = bark_mk_keycmp(mk, datum, b->end);
		if (c > 0 || (c == 0 && b->endstrict))
			return 1;
	}
	return 0;
}

/* qsort_arg comparator: boundaries by their start in index order. */
static int
bark_mk_boundary_order(const void *a, const void *b, void *arg)
{
	BarkMkScanState *mk = (BarkMkScanState *) arg;
	const BarkMkBoundary *x = (const BarkMkBoundary *) a;
	const BarkMkBoundary *y = (const BarkMkBoundary *) b;
	int			c;

	if (x->nulls || y->nulls)
	{
		if (x->nulls && y->nulls)
			return 0;
		return (x->nulls == mk->col->nulls_first) ? -1 : 1;
	}
	if (!x->hasstart || !y->hasstart)
		return (int) x->hasstart - (int) y->hasstart;
	c = bark_mk_keycmp(mk, x->start, y->start);
	if (c != 0)
		return c;
	return (int) x->startstrict - (int) y->startstrict;
}

/*
 * Do boundaries x and y intersect, x sorting at or before y?  Every NULL
 * entry is in a nulls boundary, and no key is.
 */
static bool
bark_mk_intersect(BarkMkScanState *mk, const BarkMkBoundary *x,
				  const BarkMkBoundary *y)
{
	int			c;

	if (x->nulls || y->nulls)
		return x->nulls && y->nulls;
	if (!x->hasend || !y->hasstart)
		return true;
	c = bark_mk_keycmp(mk, x->end, y->start);
	return c > 0 || (c == 0 && !x->endstrict && !y->startstrict);
}

/*
 * Set one side of boundary b from procedure 8's search element e, the lower
 * one in value order when lower.  Its strategy must say so: =, >= or > for
 * a lower element, =, <= or < for an upper one.
 */
static void
bark_mk_set_side(IndexScanDesc scan, BarkMkScanState *mk, BarkMkBoundary *b,
				 const BarkSearchElement *e, bool lower)
{
	bool		strict;

	if (e->strategy == BTEqualStrategyNumber)
		strict = false;
	else if (e->strategy == (lower ? BTGreaterEqualStrategyNumber :
							 BTLessEqualStrategyNumber))
		strict = false;
	else if (e->strategy == (lower ? BTGreaterStrategyNumber :
							 BTLessStrategyNumber))
		strict = true;
	else
		elog(ERROR, "procedure %d of index \"%s\" returned strategy %d for the %s side of a boundary",
			 BARK_EXTRACTQUERY_PROC, RelationGetRelationName(scan->indexRelation),
			 e->strategy, lower ? "lower" : "upper");

	/* In index order a DESC column's upper element comes first. */
	if (lower != mk->col->reverse)
	{
		b->hasstart = true;
		b->start = e->argument;
		b->startstrict = strict;
	}
	else
	{
		b->hasend = true;
		b->end = e->argument;
		b->endstrict = strict;
	}
}

/*
 * Run procedure 8 on query, the argument of the scan key sk (or one element
 * of its array), and append its boundaries to *bounds (*nbounds of them,
 * *maxbounds allocated), followed by the nulls boundary when it set
 * BarkQueryFlags.searchnulls.  Boundaries that contain no key (a start after
 * the end) are left out.  Two of one call's boundaries must not intersect,
 * as pg_extended_btree requires.  Runs in arrayCxt, so procedure 8's
 * results last until the next rescan.
 *
 * A procedure 8 declared with six arguments, as pg_extended_btree's are, is
 * called with six and never sees BarkQueryFlags, whose fields then stay
 * false.  The flags procedure 8 set are returned in *flagsout unless it is
 * NULL: backward for an ordering key, which picks the direction of its own
 * walk, and oneentry for a search key.
 */
static void
bark_mk_extract(IndexScanDesc scan, BarkMkScanState *mk, ScanKey sk,
				Datum query, BarkMkBoundary **bounds, int *nbounds,
				int *maxbounds, BarkQueryFlags *flagsout)
{
	BarkBoundary *result = NULL;
	int32		nresult = 0;
	Pointer		extra = NULL;
	bool		recheck = false;
	BarkQueryFlags flags;
	int			first = *nbounds;

	memset(&flags, 0, sizeof(flags));
	flags.index = scan->indexRelation;
	if (mk->queryflags)
		FunctionCall7Coll(mk->extractquery, mk->col->collation, query,
						  Int16GetDatum(sk->sk_strategy),
						  PointerGetDatum(&result), PointerGetDatum(&nresult),
						  PointerGetDatum(&extra), PointerGetDatum(&recheck),
						  PointerGetDatum(&flags));
	else
		FunctionCall6Coll(mk->extractquery, mk->col->collation, query,
						  Int16GetDatum(sk->sk_strategy),
						  PointerGetDatum(&result), PointerGetDatum(&nresult),
						  PointerGetDatum(&extra), PointerGetDatum(&recheck));
	if (!mk->queryflags)
		memset(&flags, 0, sizeof(flags));
	if (flagsout != NULL)
		*flagsout = flags;

	if (*nbounds + nresult + 1 > *maxbounds)
	{
		*maxbounds = Max(*maxbounds * 2, *nbounds + nresult + 1);
		*bounds = *bounds == NULL ? palloc_array(BarkMkBoundary, *maxbounds) :
			repalloc_array(*bounds, BarkMkBoundary, *maxbounds);
	}
	for (int i = 0; i < nresult; i++)
	{
		BarkMkBoundary *b = &(*bounds)[*nbounds];

		memset(b, 0, sizeof(*b));
		if (result[i].lower != NULL)
			bark_mk_set_side(scan, mk, b, result[i].lower, true);
		if (result[i].upper != NULL)
			bark_mk_set_side(scan, mk, b, result[i].upper, false);
		if (b->hasstart && b->hasend)
		{
			int			c = bark_mk_keycmp(mk, b->start, b->end);

			if (c > 0 || (c == 0 && (b->startstrict || b->endstrict)))
				continue;		/* empty */
			b->point = (c == 0);
		}
		if (recheck && mk->indexrecheck == NULL)
			elog(ERROR, "missing support function %d for attribute %d of index \"%s\"",
				 BARK_INDEXRECHECK_PROC, mk->attno,
				 RelationGetRelationName(scan->indexRelation));
		b->callproc9 = recheck;
		b->strategy = sk->sk_strategy;
		b->extra = extra;
		(*nbounds)++;
	}
	if (flags.searchnulls)
	{
		BarkMkBoundary *b = &(*bounds)[(*nbounds)++];

		memset(b, 0, sizeof(*b));
		b->nulls = b->point = b->recheck = true;
	}

	if (*nbounds - first > 1)
	{
		qsort_arg(*bounds + first, *nbounds - first, sizeof(BarkMkBoundary),
				  bark_mk_boundary_order, mk);
		for (int i = first + 1; i < *nbounds; i++)
			if (bark_mk_intersect(mk, &(*bounds)[i - 1], &(*bounds)[i]))
				elog(ERROR, "procedure %d of index \"%s\" returned intersecting boundaries",
					 BARK_EXTRACTQUERY_PROC,
					 RelationGetRelationName(scan->indexRelation));
	}
}

/*
 * Build the boundaries of ob, an ordering key on the extracted column, at
 * rescan ("Ordered scans" in BARK-Design.mediawiki).  Procedure 8 returns
 * the boundaries to walk and whether to walk them in descending key order.
 * The scan walks them so with the seen set, which returns a row once, at
 * its first key, and that key is the row's ORDER BY value: MongoDB's sort
 * rule for arrays, an ascending order sees a row at its smallest key and a
 * descending one at its largest.  The operator must therefore return the
 * column's key type.  An ordering operator's NULL result sorts last, so
 * when the column's NULLS option puts the NULL entries first in the walk,
 * they are read after the keys.  When procedure 8 sets orderrest, the
 * boundaries do not hold every row (a wildcard class's path range misses
 * the documents without the path), so the rest of the column, every key
 * and the NULL entries, is read after them, with the seen set even when the
 * index is not multikey, since a row met in the boundaries is met again
 * there.  That reads the boundaries' entries twice, which is the price of
 * walking the rest as one range.  A strict operator with a NULL argument
 * orders every row as NULL, in any order, so the walk reads every entry.
 *
 * The column's scan keys neither position nor filter the walk: a row met
 * first at a key that fails them would otherwise come back later, at a
 * key that passes, out of order.  Its rows come back with xs_recheck, and
 * the executor applies the keys.
 */
static void
bark_mk_order_setup(IndexScanDesc scan, BarkMkScanState *mk, ScanKey ob)
{
	Relation	index = scan->indexRelation;
	BarkMkBoundary *bounds = NULL;
	int			nbounds = 0;
	int			maxbounds = 0;
	BarkQueryFlags flags = {0};
	bool		backward;
	int			first;

	if (get_func_rettype(ob->sk_func.fn_oid) !=
		TupleDescAttr(RelationGetDescr(index), mk->attno - 1)->atttypid)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("the ordering operator on the multikey column of BARK index \"%s\" does not return the column's key type",
						RelationGetRelationName(index))));
	if (mk->extractquery == NULL)
		elog(ERROR, "missing support function %d for attribute %d of index \"%s\"",
			 BARK_EXTRACTQUERY_PROC, mk->attno, RelationGetRelationName(index));

	mk->order = true;
	mk->ordernull = (ob->sk_flags & SK_ISNULL) != 0;
	mk->recheckall = mk->nkeys > 0;
	if (mk->ordernull)
	{
		bounds = palloc0_array(BarkMkBoundary, 2);
		bounds[1].nulls = bounds[1].point = bounds[1].recheck = true;
		nbounds = 2;
		qsort_arg(bounds, nbounds, sizeof(BarkMkBoundary),
				  bark_mk_boundary_order, mk);
	}
	else
		bark_mk_extract(scan, mk, ob, ob->sk_argument, &bounds, &nbounds,
						&maxbounds, &flags);
	backward = flags.backward;

	/* Index order is ascending key order unless the column is DESC. */
	mk->orderdir = backward == mk->col->reverse ? ForwardScanDirection :
		BackwardScanDirection;
	first = ScanDirectionIsForward(mk->orderdir) ? 0 : nbounds - 1;
	if (nbounds > 0 && bounds[first].nulls)
	{
		mk->pending[0] = bounds[first];
		mk->npending = 1;
		nbounds--;
		if (first == 0)
			memmove(bounds, bounds + 1, nbounds * sizeof(BarkMkBoundary));
	}
	if (flags.orderrest)
	{
		mk->orderrest = true;
		memset(mk->pending, 0, sizeof(mk->pending));
		mk->pending[1].nulls = mk->pending[1].point = mk->pending[1].recheck = true;
		mk->npending = 2;
		qsort_arg(mk->pending, 2, sizeof(BarkMkBoundary),
				  bark_mk_boundary_order, mk);
	}
	mk->bounds = bounds;
	mk->nbounds = nbounds;
}

/*
 * Build the extracted column's boundaries from its first scan key, at
 * rescan.  A key that is not an operator of the class is a NULL test: IS
 * NULL reads the NULL entries, IS NOT NULL every key and the NULL entries,
 * whose rows the executor rechecks, since a NULL entry may be an empty
 * value.  A strict operator with a NULL argument (or a NULL array element)
 * matches nothing.  A ScalarArrayOp unions its elements' boundaries:
 * boundaries that intersect are merged, and a merged boundary returns its
 * rows with xs_recheck instead of running procedure 9, since its entries
 * would need the procedure 9 of each part.  Further keys on the column do
 * not narrow the boundaries: two keys on a multikey column may be satisfied
 * by different keys of a row.  The scan positions on the first, and the
 * executor rechecks every row against all of them.
 */
static void
bark_mk_setup(IndexScanDesc scan)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	BarkMkScanState *mk = so->mk;
	ScanKey		sk;
	BarkMkBoundary *bounds = NULL;
	int			nbounds = 0;
	int			maxbounds = 0;
	int			n = 0;

	mk->bounds = NULL;
	mk->nbounds = 0;
	mk->recheckall = mk->nkeys > 1;
	mk->order = false;
	mk->oneentry = false;
	mk->orderrest = false;
	mk->startlatch = mk->instart = false;
	mk->npending = 0;
	if (scan->numberOfOrderBys > 0 &&
		scan->orderByData[0].sk_attno == mk->attno)
	{
		bark_mk_order_setup(scan, mk, &scan->orderByData[0]);
		return;
	}
	if (mk->nkeys == 0)
		return;

	/*
	 * GIN's interface reads the entries of its keys one boundary at a time
	 * (bark_mk_gin_first), each put in this one, so that the boundary is the
	 * last required key when the columns before have equalities.
	 */
	if (mk->gin)
	{
		mk->recheckall = false;
		mk->bounds = palloc0_object(BarkMkBoundary);
		mk->nbounds = 1;
		return;
	}
	sk = &mk->keys[0];

	if (sk->sk_flags & (SK_SEARCHNULL | SK_SEARCHNOTNULL))
	{
		bounds = palloc0_array(BarkMkBoundary, 2);
		if (sk->sk_flags & SK_SEARCHNOTNULL)
			n++;				/* every key */
		bounds[n].nulls = bounds[n].point = bounds[n].recheck = true;
		n++;
		qsort_arg(bounds, n, sizeof(BarkMkBoundary), bark_mk_boundary_order,
				  mk);
		mk->bounds = bounds;
		mk->nbounds = n;
		return;
	}
	if (sk->sk_flags & SK_ISNULL)
		return;

	if (!(sk->sk_flags & SK_SEARCHARRAY))
	{
		BarkQueryFlags flags;

		bark_mk_extract(scan, mk, sk, sk->sk_argument, &bounds, &nbounds,
						&maxbounds, &flags);
		mk->oneentry = flags.oneentry;
	}
	else
	{
		ArrayType  *arr = DatumGetArrayTypeP(sk->sk_argument);
		int16		elmlen;
		bool		elmbyval;
		char		elmalign;
		Datum	   *elems;
		bool	   *nulls;
		int			nelems;

		get_typlenbyvalalign(ARR_ELEMTYPE(arr), &elmlen, &elmbyval, &elmalign);
		deconstruct_array(arr, ARR_ELEMTYPE(arr), elmlen, elmbyval, elmalign,
						  &elems, &nulls, &nelems);
		for (int e = 0; e < nelems; e++)
			if (!nulls[e])
				bark_mk_extract(scan, mk, sk, elems[e], &bounds, &nbounds,
								&maxbounds, NULL);
		if (nbounds > 1)
			qsort_arg(bounds, nbounds, sizeof(BarkMkBoundary),
					  bark_mk_boundary_order, mk);
	}

	/* Merge intersecting boundaries, keeping the start of the first. */
	for (int i = 0; i < nbounds; i++)
	{
		BarkMkBoundary *last = &bounds[n - 1];

		if (n > 0 && bark_mk_intersect(mk, last, &bounds[i]))
		{
			const BarkMkBoundary *b = &bounds[i];

			if (!last->nulls &&
				(!b->hasend ||
				 (last->hasend &&
				  (bark_mk_keycmp(mk, b->end, last->end) > 0 ||
				   (bark_mk_keycmp(mk, b->end, last->end) == 0 &&
					!b->endstrict)))))
			{
				last->hasend = b->hasend;
				last->end = b->end;
				last->endstrict = b->endstrict;
			}
			last->point = last->nulls;
			last->recheck = true;
			last->callproc9 = false;
			continue;
		}
		bounds[n++] = bounds[i];
	}
	mk->bounds = bounds;
	mk->nbounds = n;
}

/*
 * Empty the seen set.
 */
static void
bark_mk_clear_seen(BarkMkScanState *mk)
{
	MemoryContextReset(mk->seenCxt);
	mk->seen = NULL;
	mk->nseen = 0;
	mk->rejected = NULL;
	if (!mk->ruled)
		mk->havegroup = false;
}

/*
 * Decide, at rescan, whether the scan needs the seen set ("Returning a row
 * once" in BARK-Design.mediawiki).  A row can come back through two entries
 * unless the extracted column has exactly one boundary and it is a point (a
 * row's keys are distinct, so one point matches one of its entries), or
 * procedure 8 set BarkQueryFlags.oneentry for the column's first key, or the
 * index has never held a row with two entries.  Procedure 8 ran in
 * bark_mk_setup, at this rescan, so oneentry, like the multikey flag, says
 * what the index held after the snapshot was taken, never what it held when
 * the plan was made.  The multikey flag is read after the
 * snapshot was taken, and every row visible to an MVCC snapshot committed
 * before that, its insert having set the flag first.  A scan with any other
 * snapshot may see a row whose insert is still running, so it always uses
 * the set.
 */
static void
bark_mk_rescan(IndexScanDesc scan)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	BarkMkScanState *mk = so->mk;

	/*
	 * Rule D needs a table AM to judge each entry; a caller that drives
	 * amgettuple itself gets the page-read seen set, dedup by TID.
	 */
	mk->ruled = so->keyinfo->mktags && scan->heapRelation != NULL;
	mk->verdictdue = false;
	mk->havegroup = false;
	MemoryContextReset(mk->groupCxt);
	bark_mk_clear_seen(mk);
	mk->started = false;
	mk->marked = false;
	mk->nundo = 0;
	if (mk->ginCxt != NULL)
		MemoryContextReset(mk->ginCxt);
	mk->ginready = false;
	mk->gintbm = NULL;
	mk->giniter = NULL;
	mk->ginpage = mk->ginnpages = -1;
	mk->ginp = mk->ginmarkp = -1;
	mk->gini = mk->ginmarki = 0;

	/*
	 * GIN's interface reads each boundary into a bitmap of its own and merges
	 * them (bark_mk_gin_items): a seen set there would drop a row's TID from
	 * every boundary after the first that holds it.
	 */
	if (mk->gin && mk->nkeys > 0)
		mk->useseen = false;
	else if (scan->xs_snapshot == NULL ||
			 !IsMVCCSnapshot(scan->xs_snapshot) || mk->orderrest)
		mk->useseen = true;
	else if (mk->nkeys > 0 && !mk->order &&
			 (mk->oneentry || (mk->nbounds == 1 && mk->bounds[0].point)))
		mk->useseen = false;
	else
		mk->useseen = bark_index_is_multikey(scan->indexRelation);

	/*
	 * The workers of a parallel scan read different leaves, and a row's
	 * entries may be on leaves that different workers read, so a scan that
	 * needs the set cannot be parallel.  The planner never makes one: an
	 * index with an extracted column has no amcanparallel (plancat.c).  A
	 * caller of the index AM that asks anyway gets an error, never a row
	 * twice.
	 */
	if (mk->useseen && scan->parallel_scan != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("parallel index scans of multikey BARK index \"%s\" are not supported",
						RelationGetRelationName(scan->indexRelation))));
}

/*
 * Does entry itup belong to another group of the columns before the
 * extracted one than the rows in the seen set?  If so, make it the set's
 * group and return true.  Rows of different groups share no entry group, so
 * the set then empties: it holds at most one group's rows.  Under rule D
 * the set fills when rows are returned, after the page is read, so the
 * caller marks the item and the sets empty when the scan returns from it
 * (bark_mk_admit).
 */
static bool
bark_mk_group(IndexScanDesc scan, BarkMkScanState *mk, IndexTuple itup)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	TupleDesc	tupdesc = RelationGetDescr(scan->indexRelation);
	Datum		vals[INDEX_MAX_KEYS];
	bool		nulls[INDEX_MAX_KEYS];
	bool		same = mk->havegroup;
	MemoryContext oldcxt;

	for (int i = 0; i < mk->attno - 1; i++)
	{
		BarkKeyColumn *col = &so->keyinfo->cols[i];

		vals[i] = index_getattr(itup, i + 1, tupdesc, &nulls[i]);
		if (same &&
			(nulls[i] != mk->groupnulls[i] ||
			 (!nulls[i] &&
			  DatumGetInt32(FunctionCall2Coll(&col->cmp, col->collation,
											  vals[i], mk->groupvals[i])) != 0)))
			same = false;
	}
	if (same)
		return false;

	/* Under rule D the sets empty when the scan returns from this entry. */
	if (!mk->ruled)
		bark_mk_clear_seen(mk);
	MemoryContextReset(mk->groupCxt);
	oldcxt = MemoryContextSwitchTo(mk->groupCxt);
	for (int i = 0; i < mk->attno - 1; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);

		mk->groupnulls[i] = nulls[i];
		mk->groupvals[i] = nulls[i] ? (Datum) 0 :
			datumCopy(vals[i], att->attbyval, att->attlen);
	}
	MemoryContextSwitchTo(oldcxt);
	mk->havegroup = true;
	return true;
}

/*
 * The seen set: of the member TIDs tids[0 .. ntids - 1] of entry itup, which
 * the scan is about to return, keep at the front those of rows not returned
 * yet, add them to the set, and return how many there are.  The set is a
 * radix tree whose key is bark_tid_to_key(tid) >> 6 and whose value has one
 * bit per TID of that window, as TidStore uses lib/radixtree.h.  An entry
 * that a filter or procedure 9 rejected never gets here: a later entry of
 * its row may match.  newgroups lets the set empty for each group of the
 * columns before the extracted one (bark_mk_group); a KNN scan, whose two
 * cursors interleave groups, keeps it whole.
 *
 * The set is filled as a page is read, so the items a page read copies are
 * already free of repeats; bark_gettuple returns them as they are, and a
 * mark on the page restores to them (see bark_markpos for later pages).
 * The set grows past get_hash_memory_limit() rather than return a row
 * twice; the planner keeps plans inside it.
 *
 * Under rule D (mk->ruled) the set is filled from the table AM's verdicts
 * instead (bark_mk_settle), so every accepted member is kept here, and only
 * the group change is reported, in *newgroup.
 */
int
bark_mk_seen(IndexScanDesc scan, IndexTuple itup, ItemPointer tids,
			 int ntids, bool newgroups, bool *newgroup)
{
	BarkMkScanState *mk = ((BarkScanOpaque) scan->opaque)->mk;
	int			nkept = 0;

	if (!mk->useseen || mk->bitmap)
		return ntids;
	if (mk->ruled)
	{
		/* rule D: R and J are read at return time (bark_mk_admit) */
		if (newgroups && mk->attno > 1 && bark_mk_group(scan, mk, itup))
			*newgroup = true;
		return ntids;
	}
	if (newgroups && mk->attno > 1 && !mk->marked)
		(void) bark_mk_group(scan, mk, itup);
	if (mk->seen == NULL)
	{
		MemoryContext oldcxt = MemoryContextSwitchTo(mk->seenCxt);

		mk->seen = bark_seen_create(mk->seenCxt);
		MemoryContextSwitchTo(oldcxt);
	}

	for (int i = 0; i < ntids; i++)
	{
		uint64		key = bark_tid_to_key(&tids[i],
										  ((BarkScanOpaque) scan->opaque)->keyinfo->tidstride);
		uint64		bit = UINT64CONST(1) << (key & 63);
		uint64	   *word = bark_seen_find(mk->seen, key >> 6);

		if (word != NULL && (*word & bit) != 0)
			continue;
		if (word != NULL)
			*word |= bit;
		else
			bark_seen_set(mk->seen, key >> 6, &bit);
		mk->nseen++;
		if (mk->marked)
		{
			if (mk->nundo >= mk->maxundo)
			{
				mk->maxundo = Max(mk->maxundo * 2, 64);
				mk->undo = mk->undo == NULL ?
					MemoryContextAlloc(((BarkScanOpaque) scan->opaque)->scanCxt,
									   mk->maxundo * sizeof(ItemPointerData)) :
					repalloc_array(mk->undo, ItemPointerData, mk->maxundo);
			}
			mk->undo[mk->nundo++] = tids[i];
		}
		tids[nkept++] = tids[i];
	}
	return nkept;
}

/* J's key: an entry's (TID, tag). */
typedef struct BarkMkRejected
{
	ItemPointerData tid;
	IndexVersionTag tag;
}			BarkMkRejected;

/* Add tid to R, the seen set, and to the undo list while marked. */
static void
bark_mk_seen_add(IndexScanDesc scan, BarkMkScanState *mk, ItemPointer tid)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	uint64		key = bark_tid_to_key(tid, so->keyinfo->tidstride);
	uint64		bit = UINT64CONST(1) << (key & 63);
	uint64	   *word;

	if (mk->seen == NULL)
	{
		MemoryContext oldcxt = MemoryContextSwitchTo(mk->seenCxt);

		mk->seen = bark_seen_create(mk->seenCxt);
		MemoryContextSwitchTo(oldcxt);
	}
	word = bark_seen_find(mk->seen, key >> 6);
	if (word != NULL && (*word & bit) != 0)
		return;
	if (word != NULL)
		*word |= bit;
	else
		bark_seen_set(mk->seen, key >> 6, &bit);
	mk->nseen++;
	if (mk->marked)
	{
		if (mk->nundo >= mk->maxundo)
		{
			mk->maxundo = Max(mk->maxundo * 2, 64);
			mk->undo = mk->undo == NULL ?
				MemoryContextAlloc(so->scanCxt,
								   mk->maxundo * sizeof(ItemPointerData)) :
				repalloc_array(mk->undo, ItemPointerData, mk->maxundo);
		}
		mk->undo[mk->nundo++] = *tid;
	}
}

/*
 * Rule D, step 3: settle the entry returned last by the table AM's verdict
 * on it (xs_entry_verdict), which it set before it asked for this row.
 * VALID puts its row in R; INVALID puts (TID, tag) in J under an MVCC
 * snapshot, whose facts hold for the whole scan (a non-MVCC snapshot may
 * see another version later); UNKNOWN means the table AM broke its
 * obligation (amlocator.h), and the scan stops rather than guess.  Nothing
 * is pending after a rescan or a restore (bark_restrpos), and the row
 * returned before a LIMIT stopped the scan is never settled.
 */
void
bark_mk_settle(IndexScanDesc scan)
{
	BarkMkScanState *mk = ((BarkScanOpaque) scan->opaque)->mk;

	if (mk == NULL || !mk->verdictdue)
		return;
	mk->verdictdue = false;
	switch (scan->xs_entry_verdict)
	{
		case INDEX_SCAN_ENTRY_VALID:
			bark_mk_seen_add(scan, mk, &mk->pendingtid);
			break;
		case INDEX_SCAN_ENTRY_INVALID:
			if (scan->xs_snapshot != NULL &&
				IsMVCCSnapshot(scan->xs_snapshot))
			{
				BarkMkRejected key;

				if (mk->rejected == NULL)
				{
					HASHCTL		ctl;

					ctl.keysize = sizeof(BarkMkRejected);
					ctl.entrysize = sizeof(BarkMkRejected);
					ctl.hcxt = mk->seenCxt;
					mk->rejected = hash_create("BARK rejected entries", 64,
											   &ctl,
											   HASH_ELEM | HASH_BLOBS |
											   HASH_CONTEXT);
				}
				memset(&key, 0, sizeof(key));
				key.tid = mk->pendingtid;
				key.tag = mk->pendingtag;
				(void) hash_search(mk->rejected, &key, HASH_ENTER, NULL);
			}
			break;
		case INDEX_SCAN_ENTRY_UNKNOWN:
			elog(ERROR, "table access method gave no verdict on an entry of BARK index \"%s\"",
				 RelationGetRelationName(scan->indexRelation));
	}
}

/*
 * Rule D, step 4: may the scan return the entry (tid, tag)?  Not when its
 * row is in R, nor when the pair is in J.  When it may, it becomes the
 * pending entry, which the next call settles.  An entry of a new group
 * (newgroup) first empties R and J, unless the scan is marked: a restore
 * must find the marked group's R whole.
 */
bool
bark_mk_admit(IndexScanDesc scan, ItemPointer tid, IndexVersionTag tag,
			  bool newgroup)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	BarkMkScanState *mk = so->mk;

	if (newgroup && !mk->marked)
		bark_mk_clear_seen(mk);
	if (mk->seen != NULL)
	{
		uint64		key = bark_tid_to_key(tid, so->keyinfo->tidstride);
		uint64	   *word = bark_seen_find(mk->seen, key >> 6);

		if (word != NULL && (*word & (UINT64CONST(1) << (key & 63))) != 0)
			return false;
	}
	if (mk->rejected != NULL)
	{
		BarkMkRejected rkey;

		memset(&rkey, 0, sizeof(rkey));
		rkey.tid = *tid;
		rkey.tag = tag;
		if (hash_search(mk->rejected, &rkey, HASH_FIND, NULL) != NULL)
			return false;
	}
	mk->verdictdue = true;
	mk->pendingtid = *tid;
	mk->pendingtag = tag;
	return true;
}

/* Does the scan apply rule D? */
bool
bark_mk_ruled(IndexScanDesc scan)
{
	BarkMkScanState *mk = ((BarkScanOpaque) scan->opaque)->mk;

	return mk != NULL && mk->ruled && mk->useseen && !mk->bitmap;
}

/*
 * The row of entry itup for an index-only scan ("Index-only scans" in
 * BARK-Design.mediawiki).  The entry holds a key of the extracted column,
 * not its value, and bark_canreturn says that column cannot be returned, so
 * the planner never reads it: it is NULL here.  (A key cannot be turned back
 * into the value: a row with several keys has one entry per key, and the
 * NULL key stands for a NULL value, an empty value and a NULL element
 * alike.  Procedure 14 is therefore not used.)  The other
 * columns are the entry's own, except that a name column, which name_ops
 * stores as a cstring, becomes a name again, as the executor does for
 * xs_itup (tableam_index_fill_ios_names).
 */
HeapTuple
bark_mk_form_hitup(IndexScanDesc scan, IndexTuple itup)
{
	BarkMkScanState *mk = ((BarkScanOpaque) scan->opaque)->mk;
	int			natts = mk->hitupdesc->natts;
	Datum		values[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS];
	MemoryContext oldcxt;
	HeapTuple	tup;

	MemoryContextReset(mk->hitupCxt);
	oldcxt = MemoryContextSwitchTo(mk->hitupCxt);
	index_deform_tuple(itup, RelationGetDescr(scan->indexRelation), values,
					   isnull);
	values[mk->attno - 1] = (Datum) 0;
	isnull[mk->attno - 1] = true;
	for (int i = 0; i < natts; i++)
	{
		if (!isnull[i] &&
			TupleDescAttr(RelationGetDescr(scan->indexRelation), i)->atttypid ==
			CSTRINGOID)
		{
			Name		name = palloc0_object(NameData);

			namestrcpy(name, DatumGetCString(values[i]));
			values[i] = NameGetDatum(name);
		}
	}
	tup = heap_form_tuple(mk->hitupdesc, values, isnull);
	MemoryContextSwitchTo(oldcxt);
	return tup;
}

/*
 * Set up a scan of an index with an extracted column, at beginscan.
 */
static BarkMkScanState *
bark_mk_begin(IndexScanDesc scan, int attno)
{
	Relation	index = scan->indexRelation;
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	BarkMkScanState *mk = palloc0_object(BarkMkScanState);

	mk->attno = attno;
	mk->col = &so->keyinfo->cols[attno - 1];
	mk->seenCxt = AllocSetContextCreate(so->scanCxt, "BARK seen set",
										ALLOCSET_DEFAULT_SIZES);
	mk->groupCxt = AllocSetContextCreate(so->scanCxt, "BARK seen set group",
										 ALLOCSET_SMALL_SIZES);
	if (OidIsValid(index_getprocid(index, attno, BARK_EXTRACTQUERY_PROC)))
	{
		mk->extractquery = index_getprocinfo(index, attno,
											 BARK_EXTRACTQUERY_PROC);
		mk->queryflags = get_func_nargs(mk->extractquery->fn_oid) >= 7;
	}
	if (OidIsValid(index_getprocid(index, attno, BARK_INDEXRECHECK_PROC)))
		mk->indexrecheck = index_getprocinfo(index, attno,
											 BARK_INDEXRECHECK_PROC);
	mk->gin = bark_index_gin_query(index);
	if (mk->gin)
	{
		mk->ginquery = index_getprocinfo(index, attno,
										 BARK_GIN_EXTRACTQUERY_PROC);
		if (OidIsValid(index_getprocid(index, attno, BARK_GIN_CONSISTENT_PROC)))
			mk->ginconsistent = index_getprocinfo(index, attno,
												  BARK_GIN_CONSISTENT_PROC);
		if (OidIsValid(index_getprocid(index, attno,
									   BARK_GIN_COMPAREPARTIAL_PROC)))
			mk->gincompare = index_getprocinfo(index, attno,
											   BARK_GIN_COMPAREPARTIAL_PROC);
		if (OidIsValid(index_getprocid(index, attno,
									   BARK_GIN_TRICONSISTENT_PROC)))
			mk->gintriconsistent = index_getprocinfo(index, attno,
													 BARK_GIN_TRICONSISTENT_PROC);
		mk->ginCxt = AllocSetContextCreate(CurrentMemoryContext,
										   "BARK GIN result",
										   ALLOCSET_DEFAULT_SIZES);
		mk->ginChunkCxt = AllocSetContextCreate(CurrentMemoryContext,
												"BARK GIN stream chunks",
												ALLOCSET_SMALL_MINSIZE,
												ALLOCSET_SMALL_INITSIZE,
												ALLOCSET_SEPARATE_THRESHOLD);
		mk->ginstride = so->keyinfo->tidstride;
		mk->ginfold = (mk->ginstride + TBM_MAX_TUPLES_PER_PAGE - 1) /
			TBM_MAX_TUPLES_PER_PAGE;
		if (mk->ginconsistent == NULL && mk->gintriconsistent == NULL)
			elog(ERROR, "missing support function %d or %d for attribute %d of index \"%s\"",
				 BARK_GIN_CONSISTENT_PROC, BARK_GIN_TRICONSISTENT_PROC, attno,
				 RelationGetRelationName(index));
	}
	mk->hitupdesc = CreateTupleDescCopy(RelationGetDescr(index));
	TupleDescInitEntry(mk->hitupdesc, attno, NULL,
					   index->rd_opcintype[attno - 1], -1, 0);
	for (int i = 0; i < mk->hitupdesc->natts; i++)
		if (TupleDescAttr(mk->hitupdesc, i)->atttypid == CSTRINGOID)
			TupleDescInitEntry(mk->hitupdesc, i + 1, NULL, NAMEOID, -1, 0);
	TupleDescFinalize(mk->hitupdesc);
	mk->hitupCxt = AllocSetContextCreate(so->scanCxt, "BARK index-only row",
										 ALLOCSET_SMALL_SIZES);
	scan->xs_hitupdesc = mk->hitupdesc;
	return mk;
}

/*
 * At rescan, move the scan keys on the extracted column after every other,
 * keeping their order, and count the others in so->numberOfKeys.  The
 * planner only gives such a key an operator of the column's class, which
 * needs procedure 8.
 */
static void
bark_mk_partition_keys(IndexScanDesc scan)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	BarkMkScanState *mk = so->mk;
	ScanKey		moved;
	int			n = 0;
	int			m = 0;

	so->numberOfKeys = scan->numberOfKeys;
	if (mk == NULL)
		return;
	moved = palloc_array(ScanKeyData, Max(scan->numberOfKeys, 1));
	for (int i = 0; i < scan->numberOfKeys; i++)
	{
		ScanKey		sk = &scan->keyData[i];

		if (sk->sk_attno == mk->attno)
		{
			if (mk->extractquery == NULL && !mk->gin &&
				!(sk->sk_flags & (SK_SEARCHNULL | SK_SEARCHNOTNULL)))
				elog(ERROR, "missing support function %d for attribute %d of index \"%s\"",
					 BARK_EXTRACTQUERY_PROC, mk->attno,
					 RelationGetRelationName(scan->indexRelation));
			moved[m++] = *sk;
		}
		else
			scan->keyData[n++] = *sk;
	}
	/* keyData is NULL in a scan without keys, and memcpy must not see that */
	if (m > 0)
		memcpy(&scan->keyData[n], moved, m * sizeof(ScanKeyData));
	pfree(moved);
	so->numberOfKeys = n;
	mk->keys = &scan->keyData[n];
	mk->nkeys = m;
}

/*
 * Does key, inside a GIN partial-match boundary b, match it?  Procedure 12
 * compares the query key with it: 0 matches, less than 0 does not, more
 * than 0 ends the range.  The range's entries are in key order only when
 * the extracted column leads the index and is ascending, so only then does
 * the end stop the scan (bark_past_bound); otherwise it only filters.
 */
static bool
bark_mk_gin_partial(BarkMkScanState *mk, const BarkMkBoundary *b, Datum key)
{
	int32		c = DatumGetInt32(FunctionCall4Coll(mk->gincompare,
													mk->col->collation,
													b->partialkey, key,
													UInt16GetDatum(b->strategy),
													PointerGetDatum(b->extra)));

	if (c > 0 && mk->attno == 1 && !mk->col->reverse)
		mk->partialpast = true;
	return c == 0;
}

/*
 * Is entry itup inside one of the extracted column's boundaries?  A binary
 * search, since they are sorted and disjoint; the boundary found is kept in
 * mk->matched for bark_mk_accept.  True when the column has no scan key
 * and no ordering key.
 */
static bool
bark_mk_filter(IndexScanDesc scan, IndexTuple itup)
{
	BarkMkScanState *mk = ((BarkScanOpaque) scan->opaque)->mk;
	bool		isnull;
	Datum		datum;
	int			lo = 0;
	int			hi;

	if (mk == NULL || (mk->nkeys == 0 && !mk->order))
		return true;
	datum = index_getattr(itup, mk->attno,
						  RelationGetDescr(scan->indexRelation), &isnull);
	hi = mk->nbounds;
	while (lo < hi)
	{
		int			mid = lo + (hi - lo) / 2;
		int			c = bark_mk_cmp(mk, datum, isnull, &mk->bounds[mid]);

		if (c == 0)
		{
			mk->matched = mid;
			return !mk->bounds[mid].partial ||
				bark_mk_gin_partial(mk, &mk->bounds[mid], datum);
		}
		if (c > 0)
			lo = mid + 1;
		else
			hi = mid;
	}
	return false;
}

/*
 * Put the boundary required key req (on column col) is on into bound, as
 * bark_make_bound puts an element there: its start in index order for a
 * forward scan, its end for a backward one, or a NULL for the NULL entries.
 * Returns true when the bound may go on to the next column, which it may
 * after a point only; an unbounded side leaves the bound on the columns
 * before col.
 */
static bool
bark_mk_bound(BarkScanOpaque so, BarkArrayKeyState *req, int col,
			  bool forward, BarkScanBound * bound)
{
	BarkMkScanState *mk = req->mk;
	const BarkMkBoundary *b = &mk->bounds[req->cur];
	bool		has = forward ? b->hasstart : b->hasend;

	if (b->nulls)
	{
		bound->args[col - 1] = (Datum) 0;
		bound->procs[col - 1] = NULL;
		bound->collations[col - 1] = InvalidOid;
		bound->nkeys = col;
		return true;
	}
	if (!has)
		return false;
	bound->args[col - 1] = forward ? b->start : b->end;
	bound->procs[col - 1] = &mk->col->cmp;
	bound->collations[col - 1] = mk->col->collation;
	bound->nkeys = col;
	if (forward ? b->startstrict : b->endstrict)
	{
		/* Past the entries equal to it, as bark_bound_strict puts it. */
		bound->upper = forward;
		return false;
	}
	return b->point;
}

/*
 * Entry itup has passed the scan keys, bark_mk_filter among them, and its
 * member TIDs are tids[0 .. ntids - 1]: which of them does the scan return?
 * Returns how many are left at the front of tids, and sets *recheck when
 * they come back with xs_recheck.
 *
 * In a boundary whose extract-query asked for a recheck, procedure 9 answers
 * for the entry's key: false drops the entry, maybe returns it with
 * xs_recheck.  A NULL key is never in such a boundary.  A boundary made by
 * merging, the NULL entries, and every entry of a scan with several keys on
 * the column, come back with xs_recheck.
 */
int
bark_mk_accept(IndexScanDesc scan, IndexTuple itup, ItemPointer tids,
			   int ntids, bool *recheck)
{
	BarkMkScanState *mk = ((BarkScanOpaque) scan->opaque)->mk;

	*recheck = false;
	if (mk->nkeys > 0 || mk->order)
	{
		const BarkMkBoundary *b = &mk->bounds[mk->matched];

		*recheck = b->recheck || mk->recheckall;
		if (b->callproc9)
		{
			bool		isnull;
			Datum		key = index_getattr(itup, mk->attno,
											RelationGetDescr(scan->indexRelation),
											&isnull);
			int16		answer;

			answer = DatumGetInt16(FunctionCall3Coll(mk->indexrecheck,
													 mk->col->collation, key,
													 Int16GetDatum(b->strategy),
													 PointerGetDatum(b->extra)));
			if (answer == BARK_RECHECK_FALSE)
				return 0;
			if (answer == BARK_RECHECK_MAYBE)
				*recheck = true;
			else if (answer != BARK_RECHECK_TRUE)
				elog(ERROR, "procedure %d of index \"%s\" returned %d",
					 BARK_INDEXRECHECK_PROC,
					 RelationGetRelationName(scan->indexRelation), answer);
		}
	}
	return ntids;
}

/*
 * What do the scan keys fix column attno to, for a KNN scan ordered on a
 * later column (barkknn.c)?  0: attno is not the extracted column.  -1: its
 * boundaries are not one point, so its entries in the scan are not one run.
 * 1: one point, returned in *key with the column's comparator in *cmp and
 * its collation, or *cmp NULL for the NULL entries.  2: no boundary, so no
 * entry matches.
 */
int
bark_mk_point(IndexScanDesc scan, AttrNumber attno, Datum *key,
			  FmgrInfo **cmp, Oid *collation)
{
	BarkMkScanState *mk = ((BarkScanOpaque) scan->opaque)->mk;
	const BarkMkBoundary *b;

	if (mk == NULL || mk->attno != attno)
		return 0;

	/* An ordering key on the extracted column makes no KNN scan. */
	Assert(!mk->order);
	if (mk->nkeys == 0 || mk->gin)
		return -1;
	if (mk->nbounds == 0)
		return 2;
	b = &mk->bounds[0];
	if (mk->nbounds > 1 || !b->point)
		return -1;
	*key = b->start;
	*cmp = b->nulls ? NULL : &mk->col->cmp;
	*collation = mk->col->collation;
	return 1;
}

/*
 * Choose the scan's required keys (see BarkArrayKeyState), as nbtree marks
 * the keys of the leading columns with equalities required: on index columns
 * 1..m in order, one equality per column, stopping at the first column that
 * has none.  An equality array with elements or a scalar equality that has an
 * ORDER proc qualifies; when a column has several, the one with the fewest
 * elements is used, and the others only filter.  A scalar equality becomes an
 * array of one element, so that one cursor walks every column.  Unless one of
 * the keys is an array, the scan has no required keys: bark_make_bound and
 * bark_past_bound handle an equality prefix of scalars on their own.  A KNN
 * scan reads in distance order and has none either; an ordering key on the
 * extracted column has its boundaries as its one required key.
 *
 * Runs after bark_setup_key_procs, whose ORDER procs the scalar keys use.
 */
static void
bark_setup_req_keys(IndexScanDesc scan)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	int			chosen[INDEX_MAX_KEYS];
	int			nreq = 0;
	bool		anyarray = false;
	MemoryContext oldcxt;

	so->reqKeys = NULL;
	so->numReqKeys = 0;
	if (scan->numberOfOrderBys > 0 && (so->mk == NULL || !so->mk->order))
		return;
	if (so->numArrayKeys == 0 &&
		(so->mk == NULL || so->mk->nbounds == 0))
		return;

	/* chosen[k]: the scan key used on column k + 1 */
	for (int col = 1; col <= so->keyinfo->nkeys; col++)
	{
		int			nextarray = 0;
		int			best = -1;
		int			bestelems = 0;

		for (int i = 0; i < so->numberOfKeys; i++)
		{
			ScanKey		sk = &scan->keyData[i];
			int			nelems;
			bool		lower;
			bool		upper;

			if (sk->sk_flags & SK_SEARCHARRAY)
			{
				BarkArrayKeyState *ak = &so->arrayKeys[nextarray++];

				if (sk->sk_attno != col || ak->nelems == 0)
					continue;
				nelems = ak->nelems;
			}
			else if (sk->sk_attno == col &&
					 sk->sk_strategy == BTEqualStrategyNumber &&
					 bark_key_bounds(scan, sk, &lower, &upper))
				nelems = 1;
			else
				continue;
			if (best < 0 || nelems < bestelems)
			{
				best = i;
				bestelems = nelems;
			}
		}
		if (best < 0)
			break;
		chosen[nreq++] = best;
		if (scan->keyData[best].sk_flags & SK_SEARCHARRAY)
			anyarray = true;
	}

	/*
	 * The extracted column's boundaries are the next required key when every
	 * column before it has an equality, as an equality array would be: the
	 * scan visits them in order.  The column's keys are not in keyData[0 ..
	 * so->numberOfKeys - 1], so the loop above stopped there.
	 */
	if (so->mk != NULL && so->mk->nbounds > 0 && nreq == so->mk->attno - 1)
	{
		chosen[nreq++] = -1;
		anyarray = true;
	}
	if (!anyarray)
		return;

	oldcxt = MemoryContextSwitchTo(bark_array_cxt(so));
	so->reqKeys = palloc_array(BarkArrayKeyState *, nreq);
	for (int k = 0; k < nreq; k++)
	{
		ScanKey		sk = chosen[k] < 0 ? NULL : &scan->keyData[chosen[k]];
		BarkArrayKeyState *req = NULL;

		if (sk == NULL)
		{
			req = palloc0_object(BarkArrayKeyState);
			req->scankeyidx = -1;
			req->attno = so->mk->attno;
			req->nelems = so->mk->nbounds;
			req->mk = so->mk;
		}
		else if (sk->sk_flags & SK_SEARCHARRAY)
		{
			for (int a = 0; a < so->numArrayKeys; a++)
				if (so->arrayKeys[a].scankeyidx == chosen[k])
					req = &so->arrayKeys[a];
		}
		else
		{
			req = palloc0_object(BarkArrayKeyState);
			req->scankeyidx = chosen[k];
			req->attno = sk->sk_attno;
			fmgr_info_copy(&req->cmpproc, &so->keyCmp[chosen[k]],
						   so->arrayCxt);
			req->elems = palloc_object(Datum);
			req->elems[0] = sk->sk_argument;
			req->nelems = 1;
		}
		Assert(req != NULL && req->attno == k + 1);
		so->reqKeys[k] = req;
	}
	so->numReqKeys = nreq;
	MemoryContextSwitchTo(oldcxt);
}

/*
 * Put the required keys' cursors on the first combination in direction dir:
 * every cursor on its first element forward, on its last backward.
 */
static void
bark_req_reset(BarkScanOpaque so, int from, ScanDirection dir)
{
	for (int k = from; k < so->numReqKeys; k++)
		so->reqKeys[k]->cur = ScanDirectionIsForward(dir) ? 0 :
			so->reqKeys[k]->nelems - 1;
}

/*
 * Compare a column value with element e of required key k (on column k + 1)
 * in index order: <0, 0 or >0 as the value sorts before, with or after it.
 * A NULL value sorts per the column's NULLS option.
 */
static int
bark_req_cmp(BarkScanOpaque so, int k, Datum datum, bool isnull, int e)
{
	BarkArrayKeyState *req = so->reqKeys[k];
	BarkKeyColumn *col = &so->keyinfo->cols[k];
	int32		c;

	if (req->mk != NULL)
		return bark_mk_cmp(req->mk, datum, isnull, &req->mk->bounds[e]);
	if (isnull)
		return col->nulls_first ? -1 : 1;
	c = DatumGetInt32(FunctionCall2Coll(&req->cmpproc, col->collation,
										datum, req->elems[e]));
	if (col->reverse)
		INVERT_COMPARE_RESULT(c);
	return c;
}

/*
 * Move required key k's cursor one element on in direction dir, carrying
 * into the keys before it as an odometer does (a key that wraps goes back to
 * its first element in dir and moves the one before it).  Returns false when
 * the carry runs off column 1: no combination is left in dir.
 */
static bool
bark_req_step(BarkScanOpaque so, int k, ScanDirection dir)
{
	for (; k >= 0; k--)
	{
		BarkArrayKeyState *req = so->reqKeys[k];

		if (ScanDirectionIsForward(dir) ? ++req->cur < req->nelems :
			--req->cur >= 0)
			return true;
		bark_req_reset(so, k, dir);
	}
	return false;
}

/*
 * Move the required keys' cursors to the first combination at or after entry
 * itup in direction dir, on columns 1..m: for each column in order, the
 * first element in dir at or after the entry's value.  An equal element
 * goes on to the next column; one after it ends the search, with the later
 * keys on their first element; none carries into the column before.
 * Returns -1 when no combination is left (the carry ran off column 1), 0 when
 * the new combination equals the entry on columns 1..m, and 1 when it sorts
 * after it in dir.
 */
static int
bark_req_seek(IndexScanDesc scan, IndexTuple itup, ScanDirection dir)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	TupleDesc	tupdesc = RelationGetDescr(scan->indexRelation);
	bool		forward = ScanDirectionIsForward(dir);

	for (int k = 0; k < so->numReqKeys; k++)
	{
		BarkArrayKeyState *req = so->reqKeys[k];
		bool		isnull;
		Datum		datum = index_getattr(itup, k + 1, tupdesc, &isnull);
		int			lo = 0;
		int			hi = req->nelems;

		/* lo: the first element that sorts at or after the value */
		while (lo < hi)
		{
			int			mid = lo + (hi - lo) / 2;

			if (bark_req_cmp(so, k, datum, isnull, mid) > 0)
				lo = mid + 1;
			else
				hi = mid;
		}
		if (lo < req->nelems && bark_req_cmp(so, k, datum, isnull, lo) == 0)
		{
			req->cur = lo;
			continue;
		}

		/* Backward, the element in dir after the value is the one before lo. */
		if (!forward)
			lo--;
		if (lo < 0 || lo >= req->nelems)
		{
			bark_req_reset(so, k, dir);
			return bark_req_step(so, k - 1, dir) ? 1 : -1;
		}
		req->cur = lo;
		bark_req_reset(so, k + 1, dir);
		return 1;
	}
	return 0;
}

/*
 * Compare entry itup with the required keys' current combination on columns
 * 1..m, in direction dir: <0, 0 or >0 as the entry comes before, within or
 * after the combination's run.
 */
static int
bark_req_compare(IndexScanDesc scan, IndexTuple itup, ScanDirection dir)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	TupleDesc	tupdesc = RelationGetDescr(scan->indexRelation);

	for (int k = 0; k < so->numReqKeys; k++)
	{
		bool		isnull;
		Datum		datum = index_getattr(itup, k + 1, tupdesc, &isnull);
		int			c = bark_req_cmp(so, k, datum, isnull, so->reqKeys[k]->cur);

		if (c != 0)
			return ScanDirectionIsForward(dir) ? c : -c;
	}
	return 0;
}

/*
 * Does three-way result cmpresult, of a row member's column value against the
 * member's argument, satisfy the member's strategy?  nbtree's
 * _bt_rowcompare_cmpresult.
 */
static bool
bark_rowcompare_cmpresult(ScanKey subkey, int32 cmpresult)
{
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
 * A row comparison only filters here; bark_make_bound and bark_past_bound
 * position and stop the scan on its leading members (bark_row_prefix), and
 * this test still decides every entry they let through.
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

	return bark_rowcompare_cmpresult(subkey, cmpresult);
}

/*
 * Does every data entry of a leaf satisfy row comparison header, as shown by
 * the page's first and last data entries?  keepnatts is bark_keep_natts of
 * the two.  The row case of nbtree's _bt_set_startikey.
 *
 * Members are compared in order on both entries, as bark_rowcompare_matches
 * compares one.  A member is considered only when every index column before
 * its own is equal across the page, so the page's values on its column lie
 * between the first entry's and the last's.  The row holds for every entry
 * when both entries satisfy it as a whole on the same deciding member: the
 * members before it are equal to their arguments on every entry, and every
 * entry is on the same side of the deciding member's argument as the two.
 * When the two are decided by different members, an entry between them can
 * fail the row: under (a, b) > (7, 5) a page running from (7, 9) to (8, 1)
 * holds (7, NULL) when b sorts NULLs last, and an index on (a DESC, b) puts
 * (7, 1) between (8, 1) and (7, 9).  A NULL argument or value never
 * satisfies a member, so it is never marked either.
 */
static bool
bark_row_page_satisfied(ScanKey header, IndexTuple first, IndexTuple last,
						TupleDesc tupdesc, int keepnatts)
{
	ScanKey		subkey = (ScanKey) DatumGetPointer(header->sk_argument);

	for (;;)
	{
		bool		decides = (subkey->sk_flags & SK_ROW_END) != 0;
		Datum		datum;
		bool		isnull;
		int32		firstcmp;
		int32		lastcmp;

		Assert(subkey->sk_flags & SK_ROW_MEMBER);
		if (subkey->sk_attno > keepnatts || (subkey->sk_flags & SK_ISNULL))
			return false;

		datum = index_getattr(first, subkey->sk_attno, tupdesc, &isnull);
		if (isnull)
			return false;
		firstcmp = DatumGetInt32(FunctionCall2Coll(&subkey->sk_func,
												   subkey->sk_collation,
												   datum,
												   subkey->sk_argument));
		datum = index_getattr(last, subkey->sk_attno, tupdesc, &isnull);
		if (isnull)
			return false;
		lastcmp = DatumGetInt32(FunctionCall2Coll(&subkey->sk_func,
												  subkey->sk_collation,
												  datum,
												  subkey->sk_argument));

		if (firstcmp != 0 || lastcmp != 0 || decides)
			return (firstcmp != 0 || decides) && (lastcmp != 0 || decides) &&
				bark_rowcompare_cmpresult(subkey, firstcmp) &&
				bark_rowcompare_cmpresult(subkey, lastcmp);
		subkey++;
	}
}

/*
 * Does index value datum (isnull) satisfy scan key key, which is neither a
 * SAOP array nor a row comparison?  IS NULL and IS NOT NULL test only
 * isnull; any other key with a NULL argument matches nothing, and a NULL
 * value fails every comparison key.
 */
static inline bool
bark_scalar_key_matches(ScanKey key, Datum datum, bool isnull)
{
	if (key->sk_flags & SK_ISNULL)
	{
		if (key->sk_flags & SK_SEARCHNULL)
			return isnull;
		if (key->sk_flags & SK_SEARCHNOTNULL)
			return !isnull;
		return false;
	}
	if (isnull)
		return false;
	return DatumGetBool(FunctionCall2Coll(&key->sk_func, key->sk_collation,
										  datum, key->sk_argument));
}

/*
 * Test one index tuple against all scan keys except those whose bit is set in
 * skipkeys (bit i for scan key i; see bark_page_satisfied_keys).  Returns
 * true when every tested key is satisfied.  A SK_SEARCHARRAY key is satisfied
 * when the tuple's value is a member of its (preprocessed, sorted) array.
 * Also used by the KNN scan to filter its candidates, with no keys skipped.
 */
bool
bark_tuple_matches(IndexScanDesc scan, IndexTuple itup, uint64 skipkeys)
{
	Relation	index = scan->indexRelation;
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	TupleDesc	tupdesc = RelationGetDescr(index);
	int			nextarray = 0;

	for (int i = 0; i < so->numberOfKeys; i++)
	{
		ScanKey		key = &scan->keyData[i];
		Datum		datum;
		bool		isnull;

		if (i < 64 && (skipkeys & (UINT64CONST(1) << i)))
		{
			/* Keep the array keys in lockstep with the scan keys. */
			if (key->sk_flags & SK_SEARCHARRAY)
				nextarray++;
			continue;
		}

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

		if (!bark_scalar_key_matches(key, datum, isnull))
			return false;
	}
	return bark_mk_filter(scan, itup);
}

/*
 * Which scan keys does every data entry of a share-locked leaf satisfy, as
 * shown by its first and last data entries (at minoff and maxoff)?  Returns
 * a mask with bit i set for each such key i, for bark_tuple_matches to skip
 * while the page is read: nbtree's _bt_set_startikey, for BARK's keys.
 *
 * Entries on a leaf are in index order.  When the first and last entries are
 * equal on columns 1..k-1, so is every entry between them, and those
 * entries' column-k values lie between the first's and the last's in the
 * column's order (NULLs sort together at one end).  A key on column k that
 * both entries satisfy then holds for all of them: its operator belongs to
 * the column's opfamily and so agrees with that order, and IS [NOT] NULL
 * holds because the NULLs cannot sit between two values.  Equality of the
 * earlier columns is the opclass comparator's, as in bark_keep_natts.
 *
 * An equality array is marked when its column holds one value across the
 * page and that value is one of its elements; that covers the required
 * arrays on a page inside one combination's run, as nbtree marks a SAOP
 * array.  A row comparison is marked as bark_row_page_satisfied decides.
 * Each key is decided on its own, so a key on a later column is marked only
 * when its own test holds, which needs the columns before it to be equal
 * across the page whatever happened to the keys before it; nbtree's rule
 * for keys after a row comparison asks no less.  Keys after the 64th are
 * never marked; bark_tuple_matches tests them on every entry.  An OVERSIZED
 * entry is read from its overflow chain, as the caller's loop does.
 */
static uint64
bark_page_satisfied_keys(IndexScanDesc scan, Page page, OffsetNumber minoff,
						 OffsetNumber maxoff)
{
	Relation	index = scan->indexRelation;
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	TupleDesc	tupdesc = RelationGetDescr(index);
	BarkItemBuf firstbuf;
	BarkItemBuf lastbuf;
	bool		firstfetched;
	bool		lastfetched;
	IndexTuple	first;
	IndexTuple	last;
	int			keepnatts;
	int			nextarray = 0;
	uint64		satisfied = 0;

	first = bark_scan_resolve(index, BarkPageGetItem(page, minoff, &firstbuf),
							  &firstfetched);
	last = bark_scan_resolve(index, BarkPageGetItem(page, maxoff, &lastbuf),
							 &lastfetched);
	keepnatts = bark_keep_natts(index, so->keyinfo, first, last);

	for (int i = 0; i < Min(so->numberOfKeys, 64); i++)
	{
		ScanKey		key = &scan->keyData[i];
		Datum		datum;
		bool		isnull;

		if (key->sk_flags & SK_SEARCHARRAY)
		{
			BarkArrayKeyState *ak = &so->arrayKeys[nextarray++];

			/*
			 * An array's elements need not be adjacent in the column's order,
			 * so both ends being members says nothing of the entries between
			 * them; the column must instead hold one value across the page.
			 */
			Assert(ak->scankeyidx == i);
			if (key->sk_attno >= keepnatts)
				continue;
			datum = index_getattr(first, key->sk_attno, tupdesc, &isnull);
			if (isnull || !bark_array_contains(so, ak, datum))
				continue;
		}
		else if (key->sk_flags & SK_ROW_HEADER)
		{
			if (!bark_row_page_satisfied(key, first, last, tupdesc, keepnatts))
				continue;
		}
		else
		{
			if (key->sk_attno > keepnatts)
				continue;
			datum = index_getattr(first, key->sk_attno, tupdesc, &isnull);
			if (!bark_scalar_key_matches(key, datum, isnull))
				continue;
			datum = index_getattr(last, key->sk_attno, tupdesc, &isnull);
			if (!bark_scalar_key_matches(key, datum, isnull))
				continue;
		}
		satisfied |= UINT64CONST(1) << i;
	}

	if (firstfetched)
		pfree(first);
	if (lastfetched)
		pfree(last);
	return satisfied;
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
 * a NULL argument (including IS [NOT] NULL), a SAOP array (the required
 * keys position on those), a row comparison (bark_row_prefix handles
 * those), and a cross-type key whose
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
 * The usable prefix of row comparison header, for positioning and stopping
 * the scan: the number of its leading members that each sit on the next index
 * column after the previous one, are not NULL, and bound their column in the
 * same role as the first member, as nbtree's _bt_first uses a row comparison.
 * Every member has the row's strategy, so the role changes only where a
 * column's DESC option differs from the first member's.  Sets *lower and
 * *upper for the first member's column as bark_key_bounds does (a row
 * comparison is never an equality, so exactly one is set).  Returns 0 when
 * the first member is NULL: the row then matches nothing and only filters.
 *
 * Members' sk_func is already the three-way ORDER proc for the column type
 * and the member's type (ExecIndexBuildScanKeys looks it up for row
 * comparisons), so a prefix compares through it directly, cross-type members
 * included.
 */
static int
bark_row_prefix(IndexScanDesc scan, ScanKey header, bool *lower, bool *upper)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	ScanKey		member = (ScanKey) DatumGetPointer(header->sk_argument);
	bool		reverse = so->keyinfo->cols[member->sk_attno - 1].reverse;
	bool		less = (member->sk_strategy == BTLessStrategyNumber ||
						member->sk_strategy == BTLessEqualStrategyNumber);
	int			n = 0;

	*upper = (less != reverse);
	*lower = !*upper;
	for (;;)
	{
		Assert(member->sk_flags & SK_ROW_MEMBER);
		if ((member->sk_flags & SK_ISNULL) ||
			member->sk_attno != header->sk_attno + n ||
			so->keyinfo->cols[member->sk_attno - 1].reverse != reverse)
			break;
		n++;
		if (member->sk_flags & SK_ROW_END)
			break;
		member++;
	}
	return n;
}

/*
 * Put the first n members of row comparison header into bound as its columns
 * from the header's column on, making them the bound's last columns.
 */
static void
bark_row_bound(ScanKey header, int n, BarkScanBound * bound)
{
	ScanKey		member = (ScanKey) DatumGetPointer(header->sk_argument);
	int			first = header->sk_attno - 1;

	for (int m = 0; m < n; m++)
	{
		bound->args[first + m] = member[m].sk_argument;
		bound->procs[first + m] = &member[m].sk_func;
		bound->collations[first + m] = member[m].sk_collation;
	}
	bound->nkeys = first + n;
}

/*
 * Can a scan moving in direction dir stop at tuple itup, because itup is
 * already past a bound in that direction that every later entry is past too?
 *
 * Entries are visited in index order (forward) or its reverse (backward).
 * Once the leading value sorts strictly after an upper bound on column 1
 * (forward), or strictly before a lower bound (backward), every later entry in
 * that direction does too, so nothing further can match.  A value equal to the
 * bound is left to bark_tuple_matches.  A NULL value sorts where the column's
 * NULLS option puts it, and fails every bounding key, so it ends the scan
 * exactly when the NULLs lie beyond the bound.
 *
 * A bound on a later column ends the scan the same way while every column
 * before it has an equality key that itup's value equals, as nbtree's
 * "required" keys do (_bt_check_compare): the scan then is inside one run of
 * those leading values, ordered by the later column.  So a = 1 AND b = 2
 * stops at the first entry after (1, 2), and a = 1 AND b < 5 at the first
 * entry of a = 1 with b >= 5, instead of reading the rest of a = 1.  The walk
 * stops at the first column with no such equality key, or whose value is not
 * equal to it (an entry before the run, which a filter rejects without ending
 * the scan).  Arrays, NULL tests and row comparisons are not equalities here;
 * bark_req_advance ends the runs of required array keys.
 *
 * A row comparison on column 1 stops the scan the same way, on its usable
 * prefix (bark_row_prefix) compared column by column: (a, b) < (5, 3) ends a
 * forward scan at the first entry after (5, 3).  An entry equal to the prefix
 * may still match (<=, or a longer row), so it is left to the filter too.
 */
static bool
bark_past_bound(IndexScanDesc scan, IndexTuple itup, ScanDirection dir)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	bool		forward = ScanDirectionIsForward(dir);

	/* A GIN partial-match range ended (bark_mk_gin_partial). */
	if (so->mk != NULL && so->mk->partialpast)
		return true;

	for (int col = 1; col <= so->keyinfo->nkeys; col++)
	{
		bool		haveeq = false;
		bool		noteq = false;

		for (int i = 0; i < so->numberOfKeys; i++)
		{
			ScanKey		sk = &scan->keyData[i];
			bool		lower;
			bool		upper;
			int			c;

			if (sk->sk_attno != col)
				continue;
			if (col == 1 && (sk->sk_flags & SK_ROW_HEADER))
			{
				int			n = bark_row_prefix(scan, sk, &lower, &upper);
				BarkScanBound rowbound;

				if (n == 0 || (forward ? !upper : !lower))
					continue;

				/*
				 * An upper bound sorts after the entries equal to it and a
				 * lower bound before them, so the entry is past exactly when
				 * the bound sorts before it (forward) or after it (backward).
				 */
				bark_row_bound(sk, n, &rowbound);
				rowbound.upper = forward;
				rowbound.backward = !forward;
				c = bark_compare_bound(scan->indexRelation, so->keyinfo,
									   &rowbound, itup);
				if (forward ? c < 0 : c > 0)
					return true;
				continue;
			}
			if (!bark_key_bounds(scan, sk, &lower, &upper))
				continue;
			if (forward ? !upper : !lower)
				continue;
			c = bark_key_cmp(scan, i, itup);
			if (forward ? c > 0 : c < 0)
				return true;
			if (lower && upper)
			{
				haveeq = true;
				if (c != 0)
					noteq = true;
			}
		}
		if (!haveeq || noteq)
			break;
	}
	return false;
}

/*
 * Can a scan moving in direction dir stop at OVERSIZED entry itup on its
 * inline prefix alone, before its overflow chain is read?  As
 * bark_past_bound does for a key on column 1, when the prefix decides the
 * order (bark_oversized_prefix_cmp).  An entry past such a bound fails that
 * key, so it is not a match either.  False whenever the prefix cannot tell,
 * and for the keys bark_past_bound reads on later columns or row
 * comparisons: the caller then reads the chain and asks bark_past_bound.
 */
static bool
bark_prefix_past_bound(IndexScanDesc scan, IndexTuple itup, ScanDirection dir)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	bool		forward = ScanDirectionIsForward(dir);

	for (int i = 0; i < so->numberOfKeys; i++)
	{
		ScanKey		sk = &scan->keyData[i];
		bool		lower;
		bool		upper;
		bool		known;
		int			c;

		if (sk->sk_attno != 1 || !bark_key_bounds(scan, sk, &lower, &upper) ||
			(forward ? !upper : !lower))
			continue;
		c = bark_oversized_prefix_cmp(so->keyinfo, itup, &so->keyCmp[i],
									  sk->sk_collation, sk->sk_argument,
									  &known);
		if (so->keyinfo->cols[0].reverse)
			c = -c;
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
 * Can this scan skip over column 1's values?  It can when no key constrains
 * column 1 and some key bounds column 2 (nbtree's skip scan, for the case
 * that matters most; nbtree can also skip over several leading columns).  An
 * ordered-operator scan has its own reads.
 */
static bool
bark_skip_eligible(IndexScanDesc scan)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	bool		bounded = false;

	if (IndexRelationGetNumberOfKeyAttributes(scan->indexRelation) < 2 ||
		scan->numberOfOrderBys > 0 ||
		(so->mk != NULL && so->mk->attno == 1 &&
		 (so->mk->nkeys > 0 || so->mk->order)))
		return false;
	for (int i = 0; i < so->numberOfKeys; i++)
	{
		ScanKey		sk = &scan->keyData[i];
		bool		lower;
		bool		upper;

		if (sk->sk_attno == 1)
			return false;
		if (sk->sk_attno == 2 && bark_key_bounds(scan, sk, &lower, &upper))
			bounded = true;
	}
	return bounded;
}

/*
 * Skip scan: where does the next possible match after entry itup start?
 * Returns false when it may be the very next entry; otherwise builds in
 * *bound a lower bound for it.
 *
 * Within one column-1 group the entries are in column-2 order.  When itup's
 * column-2 value is past an upper bound on column 2, so is every later entry
 * of its group: the next match is in a later group, at or after (the next
 * column-1 value, column 2's lower bound) when column 1's opclass has skip
 * support for a discrete type, or after the whole group (a column-1 bound
 * that sorts after every equal entry) otherwise.  When itup's value is before
 * a lower bound on column 2, the next match is at or after (itup's column-1
 * value, that bound).  A NULL column-1 value is not a bound, so a NULL group
 * is read through.  The bound's column-1 value may be itup's own: the caller
 * copies it before letting go of the page.  *palloced says the value is
 * instead a fresh skip-support result of a by-reference type, which the
 * caller frees.
 */
static bool
bark_skip_bound(IndexScanDesc scan, IndexTuple itup, BarkScanBound * bound,
				bool *palloced)
{
	Relation	index = scan->indexRelation;
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	bool		isnull;
	Datum		value = index_getattr(itup, 1, RelationGetDescr(index), &isnull);
	bool		past = false;
	bool		before = false;
	int			lowkey = -1;

	*palloced = false;
	if (isnull)
		return false;
	for (int i = 0; i < so->numberOfKeys; i++)
	{
		ScanKey		sk = &scan->keyData[i];
		bool		lower;
		bool		upper;
		int			c;

		if (sk->sk_attno != 2 || !bark_key_bounds(scan, sk, &lower, &upper))
			continue;
		c = bark_key_cmp(scan, i, itup);
		if (upper && c > 0)
			past = true;
		if (lower && lowkey < 0)
		{
			lowkey = i;
			before = c < 0;
		}
	}
	if (!past && !before)
		return false;

	bound->args[0] = value;
	bound->procs[0] = &so->keyinfo->cols[0].cmp;
	bound->collations[0] = so->keyinfo->cols[0].collation;
	bound->nkeys = 1;
	bound->upper = false;
	bound->backward = false;
	if (past)
	{
		bool		overflow = true;

		if (!so->skipSupportReady)
		{
			MemoryContext oldcxt = MemoryContextSwitchTo(so->scanCxt);

			so->skipSupport =
				PrepareSkipSupportFromOpclass(index->rd_opfamily[0],
											  index->rd_opcintype[0],
											  so->keyinfo->cols[0].reverse);
			so->skipSupportReady = true;
			MemoryContextSwitchTo(oldcxt);
		}
		if (so->skipSupport != NULL)
			bound->args[0] = so->skipSupport->increment(index, value, &overflow);
		if (overflow)
		{
			/* No next value to name: descend past every equal entry. */
			bound->args[0] = value;
			bound->upper = true;
			return true;
		}
		*palloced = !TupleDescAttr(RelationGetDescr(index), 0)->attbyval;
	}
	if (lowkey >= 0)
	{
		bound->nkeys = 2;
		bound->args[1] = scan->keyData[lowkey].sk_argument;
		bound->procs[1] = &so->keyCmp[lowkey];
		bound->collations[1] = scan->keyData[lowkey].sk_collation;
	}
	return true;
}

/*
 * Keep bound as the skip scan's next re-descent, in so->skipBound.  The page
 * is unlocked before the descent, so the column-1 value is copied.  palloced
 * is bark_skip_bound's: the value is a fresh skip-support result, freed here.
 */
static void
bark_skip_save(IndexScanDesc scan, BarkScanBound * bound, bool palloced)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	TupleDesc	tupdesc = RelationGetDescr(scan->indexRelation);
	Form_pg_attribute att = TupleDescAttr(tupdesc, 0);
	MemoryContext oldcxt;

	if (!so->skipValueByVal && so->skipValue != (Datum) 0)
		pfree(DatumGetPointer(so->skipValue));
	oldcxt = MemoryContextSwitchTo(so->scanCxt);

	/*
	 * A value read from an OVERSIZED entry's full tuple may be compressed:
	 * decompress it once here rather than in every comparison of the descent.
	 */
	so->skipValue = att->attlen == -1 ?
		PointerGetDatum(PG_DETOAST_DATUM_COPY(bound->args[0])) :
		datumCopy(bound->args[0], att->attbyval, att->attlen);
	MemoryContextSwitchTo(oldcxt);
	so->skipValueByVal = att->attbyval;
	if (palloced)
		pfree(DatumGetPointer(bound->args[0]));
	bound->args[0] = so->skipValue;
	so->skipBound = *bound;
}

/*
 * Skip scan, reading page forward from entry itup at offnum, which failed the
 * scan keys: return the offset to continue from, past the entries that
 * bark_skip_bound shows cannot match.  maxoff + 1 when none of the rest can
 * match; *reseek then says whether the next read re-descends (with the bound
 * in so->skipBound) rather than step right, as bark_skip_plan decides.
 *
 * The high key is tested first: when the next possible match sorts after it,
 * nothing else on the page can match, and the scan re-descends.  That is the
 * usual case whenever skipping pays (a column-1 group spans pages), and one
 * comparison settles it, as nbtree's skip scan settles it by testing the
 * page's final tuple.  Otherwise the next match may be on this page, and a
 * binary search over the rest of the page finds where.
 */
static OffsetNumber
bark_skip_on_page(IndexScanDesc scan, Page page, IndexTuple itup,
				  OffsetNumber offnum, OffsetNumber maxoff, bool *reseek)
{
	Relation	index = scan->indexRelation;
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	BarkScanBound bound;
	bool		palloced;
	OffsetNumber low = OffsetNumberNext(offnum);
	OffsetNumber high = OffsetNumberNext(maxoff);

	*reseek = false;
	if (!bark_skip_bound(scan, itup, &bound, &palloced))
		return OffsetNumberNext(offnum);

	if (scan->parallel_scan == NULL &&
		!BarkPageRightmost(BarkPageGetOpaque(page)))
	{
		IndexTuple	hikey = (IndexTuple) PageGetItem(page,
													 PageGetItemId(page, BARK_P_HIKEY));

		if (bark_compare_bound(index, so->keyinfo, &bound, hikey) > 0)
		{
			bark_skip_save(scan, &bound, palloced);
			*reseek = true;
			return high;
		}
	}

	/* The first offset in [low, high) whose entry the bound sorts before. */
	while (low < high)
	{
		OffsetNumber mid = low + (high - low) / 2;
		BarkItemBuf ibuf;
		IndexTuple	cur = BarkPageGetItem(page, mid, &ibuf);

		if (bark_compare_bound(index, so->keyinfo, &bound, cur) > 0)
			low = OffsetNumberNext(mid);
		else
			high = mid;
	}
	if (palloced)
		pfree(DatumGetPointer(bound.args[0]));
	return low;
}

/*
 * Skip scan, after a forward read of page that did not end in
 * bark_skip_on_page: should the next read re-descend rather than step right?
 * If so, build that descent's bound in so->skipBound and return true.
 *
 * The page's last entry decides, through bark_skip_bound.  The descent is
 * taken only when its bound sorts after the page's high key, which keeps it
 * from landing on the page just read; otherwise the next match can start on
 * the right sibling and the plain step reaches it.
 */
static bool
bark_skip_plan(IndexScanDesc scan, Page page, OffsetNumber lastoff)
{
	Relation	index = scan->indexRelation;
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	BarkItemBuf ibuf;
	IndexTuple	last = BarkPageGetItem(page, lastoff, &ibuf);
	IndexTuple	hikey = (IndexTuple) PageGetItem(page,
												 PageGetItemId(page, BARK_P_HIKEY));
	bool		fetched;
	IndexTuple	resolved = bark_scan_resolve(index, last, &fetched);
	bool		reseek;
	bool		palloced = false;
	BarkScanBound bound;

	reseek = bark_skip_bound(scan, resolved, &bound, &palloced) &&
		bark_compare_bound(index, so->keyinfo, &bound, hikey) > 0;

	if (reseek)
		bark_skip_save(scan, &bound, palloced);
	else if (palloced)
		pfree(DatumGetPointer(bound.args[0]));
	if (fetched)
		pfree(resolved);
	return reseek;
}

/*
 * A bound whose last column comes from a strict inequality (< or >, or a row
 * comparison whose usable prefix is the whole row) excludes the entries equal
 * to it on the bounded columns, so the scan starts past them, as nbtree's
 * _bt_first descends with nextkey for > (and for < backward): forward, the
 * bound sorts after those entries, backward before them.  Otherwise a
 * forward scan's bound sorts before them and a backward one's after.
 */
static inline void
bark_bound_strict(StrategyNumber strategy, bool forward, BarkScanBound * bound)
{
	if (strategy == BTLessStrategyNumber ||
		strategy == BTGreaterStrategyNumber)
		bound->upper = forward;
}

/*
 * Build the bound a scan in direction dir starts from: the leading index
 * columns the keys bound in index order, for bark_search_bound.  Returns
 * false when column 1 is unbounded in that direction; the scan then starts
 * at the end of the index.
 *
 * The bound sorts before the entries equal to it on its columns when a
 * forward scan may match them, after them when a backward one may (a lower
 * and an upper bound), and the other way around when its last column's key
 * is a strict inequality (bark_bound_strict).  It uses the longest run of
 * leading columns 1..k where columns 1..k-1 each have an equality key and
 * column k has a key that bounds it in the scan direction.
 * Under required keys (see BarkArrayKeyState), columns 1..m are the current
 * combination of their elements, one equality scan of the merged sequence.  For
 * WHERE a = 5 AND b >= 100 on (a, b) a forward scan descends to (5, 100), not
 * to the first a = 5 leaf.  Columns after k are minus infinity in a lower
 * bound and plus infinity in an upper one, so the descent lands at or before
 * the first match in scan order, never past it.  A key's argument may be of
 * any type the opfamily has an ORDER proc for, as nbtree's _bt_first allows.
 * When several keys bound one column, the first is used; the others filter.
 *
 * A row comparison bounds the scan from its first member's column on, with
 * every column of its usable prefix (bark_row_prefix), and ends the bound
 * there as a range key does: (a, b) > (5, 10) descends past (5, 10).  The
 * prefix of a longer row is still a correct start, but not a strict one: the
 * bound only has to sort at or before the first match, so under (a, c) >
 * (5, 10) on (a, b, c) it sorts before the entries equal to (5).  An equality
 * on the column wins over a row, as over a range key; a row wins over a range
 * key when its prefix bounds more than one column.
 */
static bool
bark_make_bound(IndexScanDesc scan, ScanDirection dir, BarkScanBound * bound)
{
	Relation	index = scan->indexRelation;
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	int			nkeyatts = IndexRelationGetNumberOfKeyAttributes(index);
	bool		forward = ScanDirectionIsForward(dir);

	/* A skip scan's re-descent to its next column-1 group (bark_skip_plan). */
	if (so->skipReseeking)
	{
		Assert(forward);
		*bound = so->skipBound;
		so->skipReseeking = false;
		return true;
	}

	bound->nkeys = 0;
	bound->upper = !forward;
	bound->backward = !forward;

	for (int col = 1; col <= nkeyatts; col++)
	{
		int			found = -1;
		bool		equality = false;
		int			row = -1;
		int			rowlen = 0;

		if (col <= so->numReqKeys)
		{
			BarkArrayKeyState *req = so->reqKeys[col - 1];

			if (req->mk != NULL)
			{
				if (!bark_mk_bound(so, req, col, forward, bound))
					break;
				continue;		/* a point: later columns may bound it */
			}
			bound->args[col - 1] = req->elems[req->cur];
			bound->procs[col - 1] = &req->cmpproc;
			bound->collations[col - 1] = so->keyinfo->cols[col - 1].collation;
			bound->nkeys = col;
			continue;			/* the current combination's element */
		}

		for (int i = 0; i < so->numberOfKeys; i++)
		{
			ScanKey		sk = &scan->keyData[i];
			bool		lower;
			bool		upper;

			if (sk->sk_attno == col && (sk->sk_flags & SK_ROW_HEADER))
			{
				int			n = bark_row_prefix(scan, sk, &lower, &upper);

				if (n > rowlen && (forward ? lower : upper))
				{
					row = i;
					rowlen = n;
				}
				continue;
			}
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
		if (row >= 0 && !equality && (found < 0 || rowlen > 1))
		{
			ScanKey		header = &scan->keyData[row];
			ScanKey		member = (ScanKey) DatumGetPointer(header->sk_argument);

			bark_row_bound(header, rowlen, bound);
			if (member[rowlen - 1].sk_flags & SK_ROW_END)
				bark_bound_strict(header->sk_strategy, forward, bound);
			break;				/* the row's prefix ends the bound */
		}
		if (found < 0)
			break;

		bound->args[col - 1] = scan->keyData[found].sk_argument;
		bound->procs[col - 1] = &so->keyCmp[found];
		bound->collations[col - 1] = scan->keyData[found].sk_collation;
		bound->nkeys = col;
		if (!equality)
		{
			bark_bound_strict(scan->keyData[found].sk_strategy, forward, bound);
			break;				/* a range bound is the last usable column */
		}
	}
	return bound->nkeys > 0;
}

/*
 * Is entry itup, equal to the required keys' combination, past a bound on
 * the next column (m + 1) in direction dir?  Within the combination's run the
 * entries are in that column's order, so every later entry of the run is past
 * it too.
 */
static bool
bark_req_past_range(IndexScanDesc scan, IndexTuple itup, ScanDirection dir)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	bool		forward = ScanDirectionIsForward(dir);
	BarkArrayKeyState *last = so->reqKeys[so->numReqKeys - 1];

	/* Inside a range of the extracted column's keys the next is unordered. */
	if (last->mk != NULL && !last->mk->bounds[last->cur].point)
		return false;

	for (int i = 0; i < so->numberOfKeys; i++)
	{
		ScanKey		sk = &scan->keyData[i];
		bool		lower;
		bool		upper;
		int			c;

		if (sk->sk_attno != so->numReqKeys + 1 ||
			!bark_key_bounds(scan, sk, &lower, &upper) ||
			!(forward ? upper : lower))
			continue;
		c = bark_key_cmp(scan, i, itup);
		if (forward ? c > 0 : c < 0)
			return true;
	}
	return false;
}

/*
 * Required keys, at entry itup read in direction dir: move the cursors on
 * when the entry shows their combination's run is over, as nbtree's
 * _bt_advance_array_keys does for required arrays.  An entry before the
 * combination in dir leaves it alone (the filter rejects the entry).  An
 * entry equal to it on columns 1..m that is past a bound on column m + 1
 * exhausts it: the cursors move to the next combination.  An entry after it
 * moves them to the first combination at or after the entry.
 *
 * Returns -1 when no combination is left in dir, so the scan is over; 1 when
 * the cursors moved to a combination that sorts after the entry, so the
 * caller decides where it starts (bark_req_next_page); 0 otherwise.
 */
static int
bark_req_advance(IndexScanDesc scan, IndexTuple itup, ScanDirection dir)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	bool		advanced = false;

	for (;;)
	{
		int			c = bark_req_compare(scan, itup, dir);

		if (c < 0)
			return advanced ? 1 : 0;
		if (c == 0 && !bark_req_past_range(scan, itup, dir))
			return 0;
		advanced = true;
		if (c > 0 ? bark_req_seek(scan, itup, dir) < 0 :
			!bark_req_step(so, so->numReqKeys - 1, dir))
			return -1;
	}
}

/*
 * After bark_req_advance moved the required keys past an entry of page:
 * where does the new combination start?  Returns 0 when it may start on the
 * page, so the scan reads on; 1 when it starts beyond the page in the scan
 * direction, so nothing more on the page can match and the scan re-descends
 * to it (bark_make_bound); -1 when no page in that direction can hold it.
 *
 * Forward, the combination starts beyond the page when its start bound sorts
 * after the page's high key, as for a skip scan's next group.  A page has no
 * low key, so backward the test is against the page's first entry, as
 * nbtree's array advancement uses a backward scan's first tuple.  The
 * combination may start on the very next page; the descent lands there
 * then, at the cost of the inner pages.
 */
static int
bark_req_next_page(IndexScanDesc scan, Page page, ScanDirection dir)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	BarkPageOpaque opaque = BarkPageGetOpaque(page);
	BarkScanBound bound;
	BarkItemBuf ibuf;

	(void) bark_make_bound(scan, dir, &bound);
	if (ScanDirectionIsForward(dir))
	{
		IndexTuple	hikey;

		if (BarkPageRightmost(opaque))
			return 0;
		hikey = (IndexTuple) PageGetItem(page, PageGetItemId(page, BARK_P_HIKEY));
		return bark_compare_bound(scan->indexRelation, so->keyinfo, &bound,
								  hikey) > 0 ? 1 : 0;
	}
	if (bark_compare_bound(scan->indexRelation, so->keyinfo, &bound,
						   BarkPageGetItem(page, BarkPageFirstDataKey(opaque),
										   &ibuf)) > 0)
		return 0;
	return BarkPageLeftmost(opaque) ? -1 : 1;
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
	if (so->numberOfKeys > 0)
		so->keyCmp = palloc0_array(FmgrInfo, so->numberOfKeys);
	for (int i = 0; i < so->numberOfKeys; i++)
	{
		ScanKey		sk = &scan->keyData[i];
		int			col = sk->sk_attno - 1;
		Oid			subtype;
		Oid			proc;

		/*
		 * An inequality SAOP key gets its proc too: bark_setup_array_keys
		 * reduces it to a plain key whenever its array has a non-NULL
		 * element, which can change from one rescan to the next.
		 */
		so->keyCmp[i].fn_oid = InvalidOid;
		if ((sk->sk_flags & (SK_ROW_HEADER | SK_SEARCHNULL | SK_SEARCHNOTNULL)) ||
			((sk->sk_flags & SK_SEARCHARRAY) &&
			 sk->sk_strategy == BTEqualStrategyNumber))
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

/*
 * Detoast, once per rescan, the compressed or external arguments of the keys
 * on a bytewise first column (a Const; the executor detoasts runtime keys
 * itself).  Each comparison with an OVERSIZED entry reads an argument's
 * leading bytes for the inline-prefix fast path, and the column's comparator
 * would decompress it on every call.  The plain copy also serves the final
 * comparison with a full value.  It lives in the array keys' context, which
 * the next rescan resets, along with the keys themselves.
 *
 * shortcut: row comparison members are left as they are (they belong to the
 * executor), so a compressed one is sliced per comparison; detoast those too
 * if a keyset scan over OVERSIZED keys turns up in a profile.
 */
static void
bark_detoast_first_keys(IndexScanDesc scan)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	Oid			opcintype = scan->indexRelation->rd_opcintype[0];

	if (!so->keyinfo->cols[0].bytewise)
		return;
	for (int i = 0; i < so->numberOfKeys; i++)
	{
		ScanKey		sk = &scan->keyData[i];
		varlena    *arg;
		MemoryContext oldcxt;

		/* A cross-type key's argument need not even be a varlena. */
		if (sk->sk_attno != 1 ||
			(sk->sk_flags & (SK_ISNULL | SK_ROW_HEADER | SK_SEARCHARRAY)) ||
			(OidIsValid(sk->sk_subtype) && sk->sk_subtype != opcintype))
			continue;
		arg = (varlena *) DatumGetPointer(sk->sk_argument);
		if (!VARATT_IS_EXTERNAL(arg) && !VARATT_IS_COMPRESSED(arg))
			continue;
		oldcxt = MemoryContextSwitchTo(bark_array_cxt(so));
		sk->sk_argument = PointerGetDatum(detoast_attr(arg));
		MemoryContextSwitchTo(oldcxt);
	}
}

IndexScanDesc
bark_beginscan(Relation index, int nkeys, int norderbys)
{
	IndexScanDesc scan;
	BarkScanOpaque so;

	scan = RelationGetIndexScan(index, nkeys, norderbys);
	so = palloc0_object(BarkScanOpaqueData);

	so->keyinfo = bark_build_keyinfo(index);
	scan->opaque = so;
	if (bark_index_extracted_column(index) > 0)
		so->mk = bark_mk_begin(scan, bark_index_extracted_column(index));
	so->firstCall = true;
	so->scanCxt = CurrentMemoryContext;
	so->knn = NULL;
	BarkScanPosInvalidate(so->currPos);
	so->currPos.items = NULL;
	so->markItemIndex = -1;
	BarkScanPosInvalidate(so->markPos);
	so->markPos.items = NULL;
	so->memberItem = -1;

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

	return scan;
}

void
bark_rescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
			ScanKey orderbys, int norderbys)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;

	bark_killitems(scan);
	BarkScanPosUnpinIfPinned(so->currPos);
	BarkScanPosInvalidate(so->currPos);
	so->markItemIndex = -1;
	BarkScanPosUnpinIfPinned(so->markPos);
	BarkScanPosInvalidate(so->markPos);
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
				   scan->heapRelation != NULL &&
				   IsMVCCLikeSnapshot(scan->xs_snapshot));

	/*
	 * The number of keys handed to amrescan is authoritative, not the count
	 * from beginscan.  index_rescan asserts the two are equal, but an
	 * extension AM that wraps BARK and calls amrescan directly (DocumentDB's
	 * composite-scan fold in pg_documentdb's rum.c folds several planner scan
	 * keys into one key and calls amrescan with nscankeys = 1) may pass
	 * fewer, having asked beginscan for extra key storage.  Believe the
	 * argument, as nbtree and RUM do; every reader below uses
	 * scan->numberOfKeys.
	 */
	scan->numberOfKeys = nscankeys;

	if (scankey && nscankeys > 0)
		memcpy(scan->keyData, scankey, nscankeys * sizeof(ScanKeyData));
	if (orderbys && norderbys > 0)
		memcpy(scan->orderByData, orderbys, norderbys * sizeof(ScanKeyData));
	bark_mk_partition_keys(scan);

	/*
	 * Rebuild ScalarArrayOp state from the (possibly new) scan keys: sort and
	 * de-duplicate each equality array once, so the scan can visit the
	 * matching keys in index order, and reduce each inequality array to a
	 * plain key in scan->keyData.  That must come before
	 * bark_setup_key_procs, which gives the reduced keys their ORDER procs.
	 * A plain scan builds nothing here.
	 */
	bark_free_array_keys(so);
	bark_setup_array_keys(scan);
	bark_detoast_first_keys(scan);
	bark_setup_key_procs(scan);
	if (so->mk != NULL)
	{
		MemoryContext oldcxt = MemoryContextSwitchTo(bark_array_cxt(so));

		bark_mk_setup(scan);
		MemoryContextSwitchTo(oldcxt);
	}
	bark_setup_req_keys(scan);
	so->skip = bark_skip_eligible(scan);
	so->skipReseeking = false;

	/*
	 * An ordered-operator (KNN) scan carries ORDER BY <~> keys; hand them to
	 * the KNN machinery, which runs the outward two-sided merge in
	 * bark_gettuple.  An ordering key on the extracted column walks the
	 * column instead (bark_mk_order_setup).
	 */
	if (scan->numberOfOrderBys > 0 && orderbys && norderbys > 0 &&
		(so->mk == NULL || !so->mk->order))
		bark_knn_rescan(scan, scan->orderByData, norderbys);
	if (so->mk != NULL)
		bark_mk_rescan(scan);
}

/*
 * Descend to the leaf where a scan in direction dir must start, and return it
 * share-locked; InvalidBuffer for an empty index.  *startoff is set to the
 * offset on that leaf where the scan's read starts.
 *
 * A scan bounded in its direction on the leading columns descends to the
 * first leaf in that direction that can hold a match (bark_make_bound), and
 * starts its read at the bound's place on the leaf (bark_binsrch_bound), as
 * nbtree's _bt_first starts at _bt_binsrch's offset rather than at the
 * page's first item; otherwise it starts at the end of the leftmost or
 * rightmost leaf.  The returned leaf is never deleted or half-dead.
 */
static Buffer
bark_start_leaf(IndexScanDesc scan, ScanDirection dir, OffsetNumber *startoff)
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
	 * A lower bound descends to the leftmost leaf that can hold an entry
	 * after it (nextkey = false); an upper bound, to the rightmost (nextkey =
	 * true).  A forward scan reads rightward from there and a backward one
	 * leftward, whichever the bound is (bark_bound_strict).
	 */
	if (bark_make_bound(scan, dir, &bound))
	{
		OffsetNumber off;

		buf = bark_search_bound(index, so->keyinfo, &bound, bound.upper);
		if (!BufferIsValid(buf))
			return InvalidBuffer;

		/*
		 * The first entry after the bound: a forward scan starts there, a
		 * backward one just before it.  bark_readpage clamps an offset past
		 * either end of the page.
		 */
		off = bark_binsrch_bound(index, so->keyinfo, &bound,
								 BufferGetPage(buf));
		*startoff = backward ? OffsetNumberPrev(off) : off;
		return buf;
	}

	/*
	 * No usable bound: walk down the edge of the tree, as nbtree's
	 * _bt_get_endpoint does, stepping right past ignorable pages and, for the
	 * rightmost edge, past any page that split since we read its downlink.
	 */
	buf = bark_get_root_buffer(index, BUFFER_LOCK_SHARE);
	if (!BufferIsValid(buf))
		return InvalidBuffer;
	for (;;)
	{
		OffsetNumber off;
		IndexTuple	itup;
		BarkItemBuf ibuf;

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
		{
			*startoff = backward ? PageGetMaxOffsetNumber(page) :
				BarkPageFirstDataKey(opaque);
			return buf;
		}

		off = backward ? PageGetMaxOffsetNumber(page) :
			BarkPageFirstDataKey(opaque);
		itup = BarkPageGetItem(page, off, &ibuf);
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
 * Allocate pos->items, once per scan: it holds one item per matching entry
 * of a page, so BARK_POS_MAX_ITEMS always suffice, and at 16 bytes an item
 * that is less than a page: at BLCKSZ 8192, 6.5 kB, under AllocSet's chunk
 * limit (ALLOC_CHUNK_LIMIT), so it comes from the scan context's blocks with
 * no malloc of its own.  An allocation that
 * scales with a page's heap TIDs (192 kB for a leaf of coded LISTs) would
 * cost an mmap and munmap per query wherever malloc keeps a fixed mmap
 * threshold.
 */
StaticAssertDecl(sizeof(BarkScanPosItem) == 16,
				 "BarkScanPosItem should be 16 bytes");
StaticAssertDecl(BARK_POS_MAX_ITEMS * sizeof(BarkScanPosItem) <= BLCKSZ,
				 "a scan's items[] should take no more than a page");

static void
bark_pos_alloc(BarkScanOpaque so, BarkScanPosData *pos)
{
	if (pos->items == NULL)
		pos->items = MemoryContextAlloc(so->scanCxt,
										BARK_POS_MAX_ITEMS * sizeof(BarkScanPosItem));
}

/*
 * Make a scan's tuple workspace (currTuples or markTuples) hold at least
 * `need` bytes, keeping its contents.  Like items[], it is kept across
 * rescans.  It holds the copied LIST and POSTING bodies of a page, no more
 * bytes than the page, and for an index-only scan the entries' keys too; it
 * starts at nbtree's BLCKSZ * 2 and grows past that for a page whose keys
 * decode larger (prefix compression, OVERSIZED entries, each copied whole)
 * and for a multikey scan's survivor arrays.
 */
static void
bark_tuples_reserve(BarkScanOpaque so, char **tuples, uint32 *size, Size need)
{
	Size		newsize;

	if (*tuples != NULL && need <= *size)
		return;
	newsize = so->tuplesInit != 0 ? so->tuplesInit : BLCKSZ * 2;
	newsize = Max(newsize, Max((Size) *size * 2, need));
	if (*tuples == NULL)
		*tuples = MemoryContextAlloc(so->scanCxt, newsize);
	else
		*tuples = repalloc(*tuples, newsize);
	*size = newsize;
}

/*
 * Copy position src into dst, as nbtree's mark/restore copies a BTScanPosData:
 * the page details, the items read from the page and the tuple workspace
 * those items refer to (srctuples, into *dsttuples).  dst keeps its own
 * items array.  When src holds a pin, dst gets a pin of its own on the same
 * buffer.
 */
static void
bark_copy_pos(BarkScanOpaque so, BarkScanPosData *dst, char **dsttuples,
			  uint32 *dsttuplessize, BarkScanPosData *src, char *srctuples)
{
	BarkScanPosItem *items;

	Assert(!BarkScanPosIsPinned(*dst));
	bark_pos_alloc(so, dst);
	items = dst->items;
	*dst = *src;
	dst->items = items;
	if (src->lastItem >= 0)
		memcpy(dst->items, src->items,
			   (src->lastItem + 1) * sizeof(BarkScanPosItem));
	if (src->nextTupleOffset > 0)
	{
		bark_tuples_reserve(so, dsttuples, dsttuplessize, src->nextTupleOffset);
		memcpy(*dsttuples, srctuples, src->nextTupleOffset);
	}
	if (BarkScanPosIsPinned(*src))
		IncrBufferRefCount(src->buf);
}

/*
 * Copy the key columns of a matching entry into an index-only scan's tuple
 * workspace (*tuples, *tuplesSize, grown as needed; *nextoff is its first
 * free byte), once per entry, and return its offset there.  `resolved` is
 * the entry with its attributes readable (the full tuple of an OVERSIZED
 * entry).  A LIST or POSTING entry is copied without its body, and a tagged
 * SINGLE without its trailer; nothing reads
 * xs_itup's t_tid, so its members all share the one copy.  The plain scan
 * and each KNN cursor have their own workspace.
 */
uint32
bark_save_tuple(BarkScanOpaque so, char **tuples, uint32 *tuplesSize,
				uint32 *nextoff, IndexTuple entry, IndexTuple resolved)
{
	BarkEntryShape shape = BarkEntryGetShape(entry);
	Size		len;
	uint32		off = *nextoff;
	IndexTuple	copy;

	if (shape == BARK_SHAPE_LIST || shape == BARK_SHAPE_POSTING ||
		(shape == BARK_SHAPE_SINGLE && BarkEntryIsAltTID(entry)))
		len = BarkEntryGetBodyOffset(entry);
	else if (shape == BARK_SHAPE_OVERSIZED)
		len = BarkOverflowGetRef(entry)->fulllen;
	else
		len = IndexTupleSize(entry);

	bark_tuples_reserve(so, tuples, tuplesSize, off + MAXALIGN(len));
	copy = (IndexTuple) (*tuples + off);
	memcpy(copy, resolved, len);
	if (len != IndexTupleSize(entry) && shape != BARK_SHAPE_OVERSIZED)
	{
		/*
		 * Now a plain key tuple: drop the body's (or a tagged SINGLE's
		 * trailer's) size and the alt-TID bit.
		 */
		copy->t_info = (copy->t_info & ~(INDEX_SIZE_MASK | INDEX_AM_RESERVED_BIT)) |
			(uint16) len;
	}
	*nextoff = off + MAXALIGN(len);
	return off;
}

/*
 * Copy the members of LIST or POSTING entry `itup` into currTuples, for
 * bark_gettuple to return one at a time, and return the copy's offset there.
 * `tids`, when not NULL, are the ntids members a multikey scan kept, which
 * are copied as an array instead of the entry's body.  keyoff is the
 * entry's key copy, when the scan saves keys.
 */
static uint32
bark_save_members(BarkScanOpaque so, BarkScanPosData *pos, IndexTuple itup,
				  ItemPointer tids, int ntids, uint32 keyoff)
{
	uint32		off = pos->nextTupleOffset;
	BarkScanMembers *m;
	const char *body;
	Size		nbytes;
	uint16		shape;
	uint16		count;

	if (tids != NULL)
	{
		shape = BARK_MEMBERS_ARRAY;
		count = ntids;
		body = (const char *) tids;
		nbytes = ntids * sizeof(ItemPointerData);
	}
	else if (BarkEntryGetShape(itup) == BARK_SHAPE_POSTING)
	{
		if (!BarkPostingDataFits(itup))
			elog(ERROR, "BARK posting entry has a corrupt sbm serialization");
		shape = BARK_MEMBERS_POSTING;
		count = 0;
		body = (const char *) BarkPostingGetData(itup);
		nbytes = BarkPostingGetDataSize(itup);
	}
	else
	{
		shape = BarkListIsCoded(itup) ? BARK_MEMBERS_CODED : BARK_MEMBERS_ARRAY;
		count = BarkListGetCount(itup);
		body = (const char *) BarkListGetTIDArray(itup);
		nbytes = shape == BARK_MEMBERS_CODED ?
			sizeof(BarkCodedListHdr) + sizeof(ItemPointerData) +
			BarkCodedListGetHdr(itup)->nbytes :
			count * sizeof(ItemPointerData);
	}
	bark_tuples_reserve(so, &so->currTuples, &so->currTuplesSize,
						off + MAXALIGN(sizeof(BarkScanMembers)) + MAXALIGN(nbytes));
	m = (BarkScanMembers *) (so->currTuples + off);
	m->shape = shape;
	m->count = count;
	m->nbytes = nbytes;
	m->keyOffset = keyoff;
	memcpy(BarkScanMembersBody(m), body, nbytes);
	pos->nextTupleOffset = off + MAXALIGN(sizeof(BarkScanMembers)) +
		MAXALIGN(nbytes);
	return off;
}

/*
 * Read the share-locked leaf in so->currPos.buf into so->currPos, as nbtree's
 * _bt_readpage does: copy every matching entry on the page into items[], a
 * LIST or POSTING entry's members (its body) and, for an index-only scan,
 * each entry's key into the tuple workspace, so that bark_gettuple returns
 * them without touching the page again.  A concurrent insert or split on the page cannot then shift the
 * scan's place on it.  Reads from offset `offnum` in direction dir (a
 * position bark_first found, or the end of the page).  Returns true when the
 * page holds at least one match.
 *
 * Clears moreRight (forward) or moreLeft (backward) when an entry past the
 * scan's bound in that direction shows no later page can match.  Releases the
 * parallel scan, publishing the next page, before doing any work.
 *
 * items[] is in index order whatever the direction: a backward read fills it
 * from the bottom up and then reverses it.
 *
 * firstpage says the page is the first one a descent reached.  On any later
 * page the keys every entry satisfies are found once and not tested again
 * per entry (bark_page_satisfied_keys).  As in nbtree, the first page goes
 * without: a selective lookup, which reads only that page, would mostly pay
 * for the check without gaining from it.
 */
static bool
bark_readpage(IndexScanDesc scan, ScanDirection dir, OffsetNumber offnum,
			  bool firstpage)
{
	Relation	index = scan->indexRelation;
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	BarkScanPosData *pos = &so->currPos;
	Page		page = BufferGetPage(pos->buf);
	BarkPageOpaque opaque = BarkPageGetOpaque(page);
	bool		forward = ScanDirectionIsForward(dir);
	OffsetNumber minoff = BarkPageFirstDataKey(opaque);
	OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
	uint64		skipkeys = 0;
	bool		allsatisfied = false;
	bool		skipdone = false;	/* bark_skip_on_page reached the page end */
	bool		skipreseek = false; /* ... and asked for a re-descent */

	Assert(!BarkPageIgnore(opaque));
	so->numKilled = 0;			/* the kills of the page left are moot */
	so->memberItem = -1;
	bark_pos_alloc(so, pos);
	so->tuplesInit = (scan->xs_want_itup || (so->mk != NULL && so->mk->order)) ?
		BLCKSZ * 2 : BLCKSZ;
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
	if (scan->xs_snapshot != NULL)
		PredicateLockPage(index, pos->currPage, scan->xs_snapshot);

	if (forward)
		offnum = Max(offnum, minoff);
	else
		offnum = Min(offnum, maxoff);

	if (!firstpage && minoff < maxoff && so->numberOfKeys > 0)
	{
		skipkeys = bark_page_satisfied_keys(scan, page, minoff, maxoff);
		allsatisfied = so->numberOfKeys < 64 &&
			skipkeys == (UINT64CONST(1) << so->numberOfKeys) - 1 &&
			(so->mk == NULL || (so->mk->nkeys == 0 && !so->mk->order));
	}

	for (; forward ? offnum <= maxoff : offnum >= minoff;
		 offnum = forward ? OffsetNumberNext(offnum) : OffsetNumberPrev(offnum))
	{
		BarkItemBuf ibuf;
		IndexTuple	itup;
		bool		fetched;
		IndexTuple	resolved;
		int			ntids = 0;
		uint32		tupoff = 0;
		bool		recheck = false;
		bool		newgroup = false;
		bool		dead;
		BarkScanPosItem *item;
		BarkEntryShape shape;

		/*
		 * An entry marked LP_DEAD (bark_killitems) is dead to every snapshot,
		 * so its rows are not returned, as in nbtree's _bt_readpage, unless
		 * the scan started in recovery (ignore_killed_tuples is false): a
		 * primary marks by its own horizon, which a standby's snapshots are
		 * not part of.  It is skipped without comparing its keys, except the
		 * page's last entry in the scan's direction, whose keys may still end
		 * the scan here rather than on the next page, as nbtree's backward
		 * scan tests its first tuple.
		 */
		dead = scan->ignore_killed_tuples &&
			ItemIdIsDead(PageGetItemId(page, offnum));
		if (dead && offnum != (forward ? maxoff : minoff))
			continue;

		itup = BarkPageGetItem(page, offnum, &ibuf);

		/* A marker names no row ("Markers" in BARK-Design.mediawiki). */
		if (BarkEntryIsMarker(itup))
			continue;

		/*
		 * An OVERSIZED entry's inline prefix can show that the scan is past
		 * its bound, so that the entry's chain need not be read: a lookup
		 * then reads only the chain of the entry it returns.
		 */
		if (!allsatisfied && BarkEntryGetShape(itup) == BARK_SHAPE_OVERSIZED &&
			bark_prefix_past_bound(scan, itup, dir))
		{
			if (forward)
				pos->moreRight = false;
			else
				pos->moreLeft = false;
			break;
		}

		resolved = bark_scan_resolve(index, itup, &fetched);
		if (!allsatisfied && !bark_tuple_matches(scan, resolved, skipkeys))
		{
			bool		stop = bark_past_bound(scan, resolved, dir);
			bool		leave = false;

			/*
			 * Required keys: an entry that fails the keys may show that the
			 * current combination's run is over.  The cursors then move on,
			 * and when the new combination starts beyond this page the scan
			 * stops reading it and re-descends.  An entry that matches is
			 * equal to some combination, perhaps not the current one: the
			 * cursors may lag behind such entries, never run ahead of them,
			 * which is all bark_req_advance needs.  The membership filter
			 * decides what matches; the cursors only decide when to stop and
			 * where to re-descend.  A parallel scan stops reading the page
			 * the same way, but reads every page the shared cursor hands it
			 * rather than re-descending (bark_readnextpage).
			 */
			if (!stop && so->numReqKeys > 0)
			{
				int			r = bark_req_advance(scan, resolved, dir);
				int			np = r > 0 ? bark_req_next_page(scan, page, dir) : 0;

				stop = r < 0 || np < 0;
				leave = np > 0;
			}

			/*
			 * Skip scan: jump over the entries of this page that cannot
			 * match.  The loop's increment then lands on the entry found.
			 */
			if (so->skip && forward && !stop)
			{
				offnum = bark_skip_on_page(scan, page, resolved, offnum, maxoff,
										   &skipreseek);
				skipdone = offnum > maxoff;
				offnum = OffsetNumberPrev(offnum);
			}
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
			if (leave)
			{
				pos->arrayReseek = true;
				break;
			}
			continue;
		}
		if (dead)
		{
			if (fetched)
				pfree(resolved);
			continue;
		}

		/*
		 * One item per entry.  A multikey scan filters the entry's members
		 * here (it decides which rows it returns as it reads), so it decodes
		 * them; any other scan copies a LIST or POSTING body as it is, and
		 * bark_gettuple decodes it when it gets there.
		 */
		Assert(pos->lastItem + 1 < BARK_POS_MAX_ITEMS);
		item = &pos->items[pos->lastItem + 1];
		shape = BarkEntryGetShape(itup);
		if (so->mk != NULL)
		{
			ntids = bark_entry_count_tids(itup);
			if (ntids > so->entryTidsAlloc)
			{
				if (so->entryTids)
					pfree(so->entryTids);
				so->entryTidsAlloc = Max(ntids, 64);
				so->entryTids = MemoryContextAlloc(so->scanCxt,
												   so->entryTidsAlloc * sizeof(ItemPointerData));
			}
			ntids = bark_entry_get_tids(itup, so->entryTids, so->entryTidsAlloc,
										so->keyinfo->tidstride);
			ntids = bark_mk_accept(scan, resolved, so->entryTids, ntids,
								   &recheck);
			if (ntids > 0)
				ntids = bark_mk_seen(scan, resolved, so->entryTids, ntids,
									 true, &newgroup);
			if (ntids == 0)
			{
				if (fetched)
					pfree(resolved);
				continue;
			}
		}
		if (scan->xs_want_itup || (so->mk != NULL && so->mk->order))
			tupoff = bark_save_tuple(so, &so->currTuples, &so->currTuplesSize,
									 &pos->nextTupleOffset, itup, resolved);
		/* a tagged entry has one TID; a LIST, POSTING or multikey entry 0 */
		item->tag = BarkEntryGetTag(itup);
		item->indexOffset = offnum | (recheck ? BARK_ITEM_RECHECK : 0) |
			(newgroup ? BARK_ITEM_NEWGROUP : 0);
		item->tupleOffset = tupoff;
		if (so->mk != NULL ? ntids > 1 :
			(shape == BARK_SHAPE_LIST || shape == BARK_SHAPE_POSTING))
		{
			item->indexOffset |= BARK_ITEM_MEMBERS;
			item->tupleOffset = bark_save_members(so, pos, itup,
												  so->mk != NULL ? so->entryTids : NULL,
												  ntids, tupoff);
			ItemPointerSetInvalid(&item->heapTid);
		}
		else if (so->mk != NULL)
			item->heapTid = so->entryTids[0];
		else
			item->heapTid = *BarkEntryGetHeapTid(itup);
		if (fetched)
			pfree(resolved);
		pos->lastItem++;
	}

	/*
	 * Backward: the entries arrived in descending order; put them in index
	 * order.  Each entry's members stay ascending in their copy.
	 */
	if (!forward)
	{
		for (int lo = 0, hi = pos->lastItem; lo < hi; lo++, hi--)
		{
			BarkScanPosItem tmp = pos->items[lo];

			pos->items[lo] = pos->items[hi];
			pos->items[hi] = tmp;
		}
	}

	for (int k = 0; k < so->numReqKeys; k++)
		pos->arrayCur[k] = so->reqKeys[k]->cur;

	if (so->skip && forward && pos->moreRight && scan->parallel_scan == NULL &&
		!BarkPageRightmost(opaque) && maxoff >= minoff)
		pos->arrayReseek = skipdone ? skipreseek :
			bark_skip_plan(scan, page, maxoff);

	if (pos->lastItem < 0)
		return false;

	pos->itemIndex = forward ? -1 : pos->lastItem + 1;
	pos->memberIndex = 0;
	pos->nmembers = 0;
	return true;
}

/*
 * The member TIDs of currPos's item i, ascending: *tids is set to an array
 * of the returned count, which stays valid until the next call (it may be
 * so->entryTids, into which a coded LIST or POSTING body is decoded).
 */
static int
bark_item_tids(BarkScanOpaque so, int i, ItemPointer *tids)
{
	BarkScanPosItem *item = &so->currPos.items[i];
	BarkScanMembers *m;
	int			n;

	if ((item->indexOffset & BARK_ITEM_MEMBERS) == 0)
	{
		*tids = &item->heapTid;
		return 1;
	}
	m = (BarkScanMembers *) (so->currTuples + item->tupleOffset);
	if (m->shape == BARK_MEMBERS_ARRAY)
	{
		*tids = (ItemPointer) BarkScanMembersBody(m);
		return m->count;
	}
	so->memberItem = -1;		/* entryTids is about to change */
	if (m->shape == BARK_MEMBERS_CODED)
	{
		n = m->count;
		if (n > so->entryTidsAlloc)
		{
			if (so->entryTids)
				pfree(so->entryTids);
			so->entryTidsAlloc = Max(n, 64);
			so->entryTids = MemoryContextAlloc(so->scanCxt,
											   so->entryTidsAlloc * sizeof(ItemPointerData));
		}
		bark_coded_body_get_tids(BarkScanMembersBody(m), n, so->entryTids,
								 so->keyinfo->tidstride);
	}
	else
		n = bark_posting_body_get_tids((uint8 *) BarkScanMembersBody(m),
									   m->nbytes, &so->entryTids,
									   &so->entryTidsAlloc, so->scanCxt,
									   so->keyinfo->tidstride);
	*tids = so->entryTids;
	return n;
}

/*
 * Make currPos's item i the one whose members so->memberTids holds, and
 * return its number of members.  bark_step_member and bark_saveitem read a
 * SINGLE or OVERSIZED item's heap TID straight from the item instead, so
 * memberItem names the last item with BARK_ITEM_MEMBERS loaded.
 */
static int
bark_load_members(BarkScanOpaque so, int i)
{
	int			n = bark_item_tids(so, i, &so->memberTids);

	so->memberItem = i;
	return n;
}

/*
 * Move currPos to the next member in direction dir, entering the next item
 * when the current one's members are used up.  Returns false when the page's
 * items are, leaving the position as it was.  A fresh page (itemIndex before
 * its first item, or after its last, and no members) enters its first item
 * in dir.  A change of direction continues from the member returned last.
 */
static bool
bark_step_member(BarkScanOpaque so, ScanDirection dir)
{
	BarkScanPosData *pos = &so->currPos;
	int			itemIndex = pos->itemIndex;
	int			n;

	if (ScanDirectionIsForward(dir))
	{
		if (pos->memberIndex + 1 < pos->nmembers)
		{
			pos->memberIndex++;
			return true;
		}
		do
		{
			if (itemIndex >= pos->lastItem)
				return false;
			itemIndex++;
			n = likely((pos->items[itemIndex].indexOffset & BARK_ITEM_MEMBERS) == 0) ?
				1 : bark_load_members(so, itemIndex);
		} while (n == 0);
		pos->memberIndex = 0;
	}
	else
	{
		if (pos->memberIndex > 0 && pos->memberIndex < pos->nmembers)
		{
			pos->memberIndex--;
			return true;
		}
		do
		{
			if (itemIndex <= pos->firstItem)
				return false;
			itemIndex--;
			n = likely((pos->items[itemIndex].indexOffset & BARK_ITEM_MEMBERS) == 0) ?
				1 : bark_load_members(so, itemIndex);
		} while (n == 0);
		pos->memberIndex = n - 1;
	}
	pos->itemIndex = itemIndex;
	pos->nmembers = n;
	return true;
}

/*
 * Remember that the executor found the row returned last, currPos's member
 * memberIndex of item itemIndex, dead to every snapshot, for bark_killitems:
 * extend the last kill run when the member is next to it, as a scan killing
 * an entry's members in turn does; else start a run.  With no room left the
 * kill is dropped, which only loses the hint.
 */
#define BARK_MAX_KILL_RUNS	512

static void
bark_kill_prior(BarkScanOpaque so)
{
	BarkScanPosData *pos = &so->currPos;
	BarkKillRun *r;

	if (so->killRuns == NULL)
		so->killRuns = MemoryContextAlloc(so->scanCxt,
										  BARK_MAX_KILL_RUNS * sizeof(BarkKillRun));
	if (so->numKilled > 0)
	{
		r = &so->killRuns[so->numKilled - 1];
		if (r->item == pos->itemIndex)
		{
			if (pos->memberIndex == r->hi + 1)
			{
				r->hi++;
				return;
			}
			if (pos->memberIndex == r->lo - 1)
			{
				r->lo--;
				return;
			}
		}
	}
	if (so->numKilled >= BARK_MAX_KILL_RUNS)
		return;
	r = &so->killRuns[so->numKilled++];
	r->item = pos->itemIndex;
	r->lo = r->hi = pos->memberIndex;
}

/* Return currPos's member memberIndex of item itemIndex to the executor. */
static void
bark_saveitem(IndexScanDesc scan)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	BarkScanPosData *pos = &so->currPos;
	BarkScanPosItem *item = &pos->items[pos->itemIndex];
	uint32		keyoff = item->tupleOffset;

	if (likely((item->indexOffset & BARK_ITEM_MEMBERS) == 0))
		scan->xs_heaptid = item->heapTid;
	else
	{
		/* A restored mark, or bark_item_tids, may have changed the members. */
		if (so->memberItem != pos->itemIndex)
			(void) bark_load_members(so, pos->itemIndex);
		scan->xs_heaptid = so->memberTids[pos->memberIndex];
		keyoff = ((BarkScanMembers *) (so->currTuples + keyoff))->keyOffset;
	}
	scan->xs_heaptag = item->tag;
	scan->xs_recheck = (item->indexOffset & BARK_ITEM_RECHECK) != 0;
	if (scan->xs_want_itup)
	{
		scan->xs_itup = (IndexTuple) (so->currTuples + keyoff);
		if (so->mk != NULL)
			scan->xs_hitup = bark_mk_form_hitup(scan, scan->xs_itup);
	}

	/* An ordering key on the extracted column: the entry's key, exact. */
	if (so->mk != NULL && so->mk->order)
	{
		IndexTuple	itup = (IndexTuple) (so->currTuples + keyoff);
		bool		isnull;

		scan->xs_orderbyvals[0] = index_getattr(itup, so->mk->attno,
												RelationGetDescr(scan->indexRelation),
												&isnull);
		scan->xs_orderbynulls[0] = isnull || so->mk->ordernull;
		scan->xs_recheckorderby = false;
	}
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

	/*
	 * Without the pin, VACUUM may remove the page's entries and the heap
	 * reuse their TIDs before bark_killitems runs; it checks this LSN to tell
	 * whether the page changed since.
	 */
	so->currPos.lsn = BufferGetLSNAtomic(so->currPos.buf);
	UnlockReleaseBuffer(so->currPos.buf);
	so->currPos.buf = InvalidBuffer;
}

/*
 * Mark LP_DEAD the entries of currPos's page whose every heap TID the
 * executor found dead (kill_prior_tuple), as nbtree's _bt_killitems does,
 * before the scan leaves the page.  Called with currPos unlocked, pinned only
 * when !dropPin; returns with the same pins.
 *
 * The marks are hints: they are set under a share lock through
 * BufferBeginSetHintBits, which can refuse (another backend is writing the
 * page out), and are not WAL-logged, so bark_mask masks them; with
 * checksums or wal_log_hints the first hint after a checkpoint logs a page
 * image (MarkBufferDirtyHint).  An insert that would split the page deletes
 * the marked entries (bark_simple_delete).
 *
 * An entry is found by its offset as read and its heap TIDs, never by offset
 * alone.  A scan that kept its pin since the read (!dropPin) blocked
 * VACUUM's cleanup lock, so no TID on the page can have been removed and
 * reused; entries may have moved right within the page (inserts), or off it
 * (a split, or a deletion before them), and an entry moved off is not
 * found, which only loses the hint.  A scan that dropped its pin gives up
 * unless the page's LSN is the one it read: any logged change at all, a
 * VACUUM's included, makes it stop.  A kill says the heap TID's whole HOT
 * chain is dead to every snapshot, so any entry holding only killed TIDs may
 * be marked, whichever row of a multikey index's it was returned for.
 *
 * In an index that stores version tags a kill is about one entry, the
 * (TID, tag) the table AM was handed: the entry must have the item's tag
 * too, so another entry of the same row, of another version, is not marked.
 *
 * SINGLE and OVERSIZED entries have one TID, so a kill marks them.  Scans
 * skip a marked OVERSIZED entry without reading its overflow chain; simple
 * deletion leaves it, as bottom-up deletion does, for VACUUM, which frees
 * the chain.  A LIST or POSTING entry is marked only when every one of its
 * TIDs was killed by this scan, as nbtree requires of a posting list tuple:
 * one live member keeps the entry, for that member's sake.  A marker names
 * no row and is never killed; pivots are never in items[].  A page with an
 * entry newly marked gets BARK_HAS_GARBAGE, a hint too.
 *
 * A scan of a GIN-interface column (bark_mk_gin_gettuple) has no leaf
 * position when it returns TIDs, so it never collects kills; nor does a
 * bitmap scan (the executor gives it no kill_prior_tuple) or a KNN scan,
 * whose cursors are not currPos.
 */
static void
bark_killitems(IndexScanDesc scan)
{
	Relation	index = scan->indexRelation;
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	BarkScanPosData *pos = &so->currPos;
	Buffer		buf;
	Page		page;
	BarkPageOpaque opaque;
	OffsetNumber minoff;
	OffsetNumber maxoff;
	bool		killedsomething = false;
	ItemPointer tids = NULL;
	int			tidsalloc = 0;

	if (so->numKilled == 0)
		return;
	Assert(BarkScanPosIsValid(*pos));
	Assert(scan->heapRelation != NULL);

	if (!so->dropPin)
	{
		Assert(BarkScanPosIsPinned(*pos));
		buf = pos->buf;
		LockBuffer(buf, BUFFER_LOCK_SHARE);
	}
	else
	{
		Assert(!BarkScanPosIsPinned(*pos));
		buf = ReadBuffer(index, pos->currPage);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		if (BufferGetLSNAtomic(buf) != pos->lsn)
			goto done;			/* changed: give up on the hints */
	}

	page = BufferGetPage(buf);
	opaque = BarkPageGetOpaque(page);
	minoff = BarkPageFirstDataKey(opaque);
	maxoff = PageGetMaxOffsetNumber(page);

	/*
	 * A held pin does not keep the leaf alive: simple and bottom-up deletion
	 * empty a leaf under an exclusive lock, and VACUUM deletes an empty leaf
	 * without a cleanup lock (bark_delete_empty_leaf).  It cannot have been
	 * reused, since this scan's snapshot holds back its safexid.
	 */
	if (BarkPageIgnore(opaque))
		goto done;

	for (int k = 0; k < so->numKilled; k++)
	{
		BarkKillRun *r = &so->killRuns[k];
		BarkScanPosItem *kitem = &pos->items[r->item];
		ItemPointer ktids;
		int			nk;

		/* Every member must be killed, in one run. */
		nk = bark_item_tids(so, r->item, &ktids);
		if (r->lo != 0 || r->hi != nk - 1)
			continue;

		/* The entry may have moved right on the page since it was read. */
		for (OffsetNumber offnum = kitem->indexOffset & BARK_ITEM_OFFSET_MASK;
			 offnum <= maxoff; offnum = OffsetNumberNext(offnum))
		{
			ItemId		iid = PageGetItemId(page, offnum);
			BarkItemBuf ibuf;
			IndexTuple	itup;
			ItemPointerData lo;
			ItemPointerData hi;
			int			n;

			if (offnum < minoff)
				continue;		/* a split gave the page a high key */

			/* A marker's TID is no row's, so its range holds no kill. */
			itup = BarkPageGetItem(page, offnum, &ibuf);
			bark_entry_tid_range(itup, &lo, &hi, so->keyinfo->tidstride);
			if (ItemPointerCompare(&ktids[0], &lo) < 0 ||
				ItemPointerCompare(&ktids[0], &hi) > 0)
				continue;

			/* A kill is of one entry: (TID, tag), of a row's versions. */
			if (BarkEntryGetTag(itup) != kitem->tag)
				continue;

			/* It must still hold exactly the members the scan killed. */
			n = bark_entry_count_tids(itup);
			if (n != nk)
				break;
			if (n > tidsalloc)
			{
				tidsalloc = Max(n, 64);
				tids = tids == NULL ? palloc_array(ItemPointerData, tidsalloc) :
					repalloc_array(tids, ItemPointerData, tidsalloc);
			}
			n = bark_entry_get_tids(itup, tids, n, so->keyinfo->tidstride);
			if (memcmp(tids, ktids, n * sizeof(ItemPointerData)) == 0 &&
				!ItemIdIsDead(iid))
			{
				if (!killedsomething && !BufferBeginSetHintBits(buf))
					goto done;
				ItemIdMarkDead(iid);
				killedsomething = true;
			}
			break;
		}
	}

	if (killedsomething)
	{
		opaque->bark_flags |= BARK_HAS_GARBAGE;
		BufferFinishSetHintBits(buf, true, true);
	}

done:
	so->numKilled = 0;
	if (tids != NULL)
		pfree(tids);
	if (!so->dropPin)
		LockBuffer(buf, BUFFER_LOCK_UNLOCK);
	else
		UnlockReleaseBuffer(buf);
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
Buffer
bark_lock_and_validate_left(Relation index, BlockNumber *blkno,
							BlockNumber lastcurrblkno)
{
	BlockNumber origblkno = *blkno;

	/*
	 * Tests stop a backward step here, between pages, while other sessions
	 * split or delete pages, as nbtree's tests do at nbtree-walk-left.
	 */
	INJECTION_POINT("bark-walk-left", NULL);

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
			INJECTION_POINT("bark-walk-left-step-right", NULL);
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
			INJECTION_POINT("bark-walk-left-deleted", NULL);
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
		INJECTION_POINT("bark-walk-left-restart", NULL);
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
	bool		reseek;
	OffsetNumber startoff;
	OffsetNumber readoff = InvalidOffsetNumber; /* where a re-descent starts */
	bool		firstpage = false;	/* the page is a re-descent's first */

	Assert(!BarkScanPosIsPinned(so->currPos));

	/*
	 * A read in dir of the page we leave may have asked to re-descend rather
	 * than step to the sibling: a scan with required keys whose next
	 * combination starts beyond the sibling (bark_req_next_page), or a
	 * forward skip scan whose next group does.  The descent goes to the
	 * required keys' current combination, which the caller has restored from
	 * currPos.  A parallel scan reads whatever page the shared cursor hands
	 * it instead.  Each re-descent happens in this loop, so a long run of
	 * pages without matches costs no stack.
	 */
	reseek = so->currPos.dir == dir && so->currPos.arrayReseek &&
		scan->parallel_scan == NULL;

	if (forward)
		so->currPos.moreLeft = true;
	else
		so->currPos.moreRight = true;

	for (;;)
	{
		Page		page;
		BarkPageOpaque opaque;

		if (reseek)
		{
			CHECK_FOR_INTERRUPTS();
			reseek = false;
			pgstat_count_index_scan(index);
			if (scan->instrument)
				scan->instrument->nsearches++;
			if (so->skip)
				so->skipReseeking = true;
			so->currPos.buf = bark_start_leaf(scan, dir, &startoff);
			if (!BufferIsValid(so->currPos.buf))
			{
				BarkScanPosInvalidate(so->currPos);
				return false;
			}
			blkno = BufferGetBlockNumber(so->currPos.buf);
			readoff = startoff;
			firstpage = true;
		}
		else if (blkno == BARK_P_NONE ||
				 (forward ? !so->currPos.moreRight : !so->currPos.moreLeft))
		{
			BarkScanPosInvalidate(so->currPos);
			bark_parallel_done(scan);
			return false;
		}
		else if (!seized && scan->parallel_scan != NULL &&
				 !bark_parallel_seize(scan, &blkno, &lastcurrblkno))
		{
			BarkScanPosInvalidate(so->currPos);
			return false;
		}
		else if (forward)
		{
			Assert(BlockNumberIsValid(blkno));
			CHECK_FOR_INTERRUPTS();
			so->currPos.buf = ReadBuffer(index, blkno);
			LockBuffer(so->currPos.buf, BUFFER_LOCK_SHARE);
		}
		else
		{
			Assert(BlockNumberIsValid(blkno));
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
			if (readoff == InvalidOffsetNumber)
				readoff = forward ? BarkPageFirstDataKey(opaque) :
					PageGetMaxOffsetNumber(page);
			if (bark_readpage(scan, dir, readoff, firstpage))
				break;
			blkno = forward ? so->currPos.nextPage : so->currPos.prevPage;
			reseek = so->currPos.arrayReseek && scan->parallel_scan == NULL;
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
		readoff = InvalidOffsetNumber;
		firstpage = false;
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
	OffsetNumber offnum;

	Assert(!BarkScanPosIsValid(so->currPos));

	pgstat_count_index_scan(index);
	if (scan->instrument)
		scan->instrument->nsearches++;

	/* An empty IN-list (or all-NULL array) on any column: no rows. */
	for (int a = 0; a < so->numArrayKeys; a++)
		if (so->arrayKeys[a].nelems == 0)
			return false;
	if (so->mk != NULL)
	{
		if (!so->mk->bitmap)
			elog(DEBUG1, "BARK index \"%s\": scan %s",
				 RelationGetRelationName(index),
				 so->mk->useseen ? "uses a seen set" : "needs no seen set");
		so->mk->started = true;
		so->mk->dir = dir;
		if ((so->mk->nkeys > 0 || so->mk->order) && so->mk->nbounds == 0)
			return false;
	}
	bark_req_reset(so, 0, dir);

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

	buf = bark_start_leaf(scan, dir, &offnum);
	if (!BufferIsValid(buf))
	{
		/* Empty index.  A serializable scan must lock the whole relation. */
		if (scan->xs_snapshot != NULL)
			PredicateLockRelation(index, scan->xs_snapshot);
		bark_parallel_done(scan);
		return false;
	}

	so->currPos.buf = buf;

	if (bark_readpage(scan, dir, offnum, true))
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
	return bark_readnextpage(scan, blkno, lastcurrblkno, dir, false);
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

	/* Before leaving the page, mark the entries the executor killed. */
	bark_killitems(scan);

	/*
	 * A mark on this page is only an itemIndex so far; before leaving the
	 * page, make it a full copy of the position (nbtree's _bt_steppage); the
	 * required keys' cursors and the re-descent flag travel with the copy, so
	 * restoring the mark also restores where the scan goes next.
	 */
	if (so->markItemIndex >= 0)
	{
		Assert(scan->parallel_scan == NULL);
		bark_copy_pos(so, &so->markPos, &so->markTuples, &so->markTuplesSize,
					  &so->currPos, so->currTuples);
		so->markPos.itemIndex = so->markItemIndex;
		so->markPos.memberIndex = so->markMemberIndex;
		so->markPos.nmembers = so->markNmembers;
		so->markItemIndex = -1;
	}

	BarkScanPosUnpinIfPinned(so->currPos);

	blkno = ScanDirectionIsForward(dir) ? so->currPos.nextPage :
		so->currPos.prevPage;
	lastcurrblkno = so->currPos.currPage;

	/*
	 * The required keys' cursors continue from where the page's read left
	 * them, or, when the scan reverses direction, from the first combination
	 * in the new one.  That never skips a match: the first entry read moves
	 * the cursors to where it sorts (bark_req_advance).  The cursors in the
	 * array state may describe another page (a restored mark is read from its
	 * copy).  A re-descent the page asked for (arrayReseek) is taken by
	 * bark_readnextpage.
	 */
	if (so->currPos.dir == dir)
	{
		for (int k = 0; k < so->numReqKeys; k++)
			so->reqKeys[k]->cur = so->currPos.arrayCur[k];
	}
	else
		bark_req_reset(so, 0, dir);

	return bark_readnextpage(scan, blkno, lastcurrblkno, dir, false);
}

/*
 * An ordered scan of the extracted column reads its pending boundaries once
 * its own are done (bark_mk_order_setup): the NULL entries, when they come
 * first in its walk, or the rest of the column.  Position the scan again, on
 * them alone.  A row already returned is in the seen set, so it does not
 * come back; the others have no key in the ordering key's boundaries, and
 * their ORDER BY value is NULL.  Returns false when there is nothing left to
 * read.
 */
static bool
bark_mk_order_rest(IndexScanDesc scan, ScanDirection dir)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	BarkMkScanState *mk = so->mk;

	if (mk == NULL || mk->npending == 0)
		return false;
	mk->bounds = mk->pending;
	mk->nbounds = mk->npending;
	mk->npending = 0;
	mk->ordernull = true;
	for (int k = 0; k < so->numReqKeys; k++)
		if (so->reqKeys[k]->mk != NULL)
			so->reqKeys[k]->nelems = mk->nbounds;
	BarkScanPosUnpinIfPinned(so->currPos);
	BarkScanPosInvalidate(so->currPos);
	return bark_first(scan, dir);
}

bool
bark_gettuple(IndexScanDesc scan, ScanDirection dir)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	bool		ruled;
	ItemPointerData tid;

	if (so->mk != NULL && so->mk->gin && so->mk->nkeys > 0)
		return bark_mk_gin_gettuple(scan, dir);

	/*
	 * Ordered-operator (KNN) scan: distances dictate the order, not the key
	 * order, so hand off to the two-sided outward merge.  The executor only
	 * ever drives a KNN scan forward.
	 */
	if (scan->numberOfOrderBys > 0 && (so->mk == NULL || !so->mk->order))
		return bark_knn_gettuple(scan);

	/*
	 * An ordering key on the extracted column walks the column in the
	 * direction its procedure 8 chose; the executor drives it forward.
	 */
	if (so->mk != NULL && so->mk->order)
		dir = so->mk->orderdir;

	/*
	 * Rule D: settle the entry returned last by the table AM's verdict on it,
	 * before anything else moves ("Versioned multikey scans" in the README).
	 */
	ruled = bark_mk_ruled(scan);
	if (ruled)
		bark_mk_settle(scan);

	/*
	 * A scan with a seen set that reverses direction would meet the entries
	 * of the rows it returned, and drop them.  The executor never asks for
	 * that: bark_property says such an index has no backward scans, so a
	 * scroll cursor over it is materialized.  A caller of the index AM that
	 * does not ask gets an error, not wrong rows.
	 */
	if (so->mk != NULL && so->mk->useseen && so->mk->started &&
		dir != so->mk->dir)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot change the direction of a scan of multikey BARK index \"%s\"",
						RelationGetRelationName(scan->indexRelation))));

	if (!BarkScanPosIsValid(so->currPos))
	{
		/*
		 * Not positioned: either the first call, or the scan ran off one end.
		 * nbtree restarts from that end in both cases (a scroll cursor that
		 * fetched past the last row and then fetches backward); so do we.
		 */
		if (!bark_first(scan, dir) && !bark_mk_order_rest(scan, dir))
			return false;
	}
	else if (scan->kill_prior_tuple)
	{
		/*
		 * The executor found the row returned last dead to every snapshot:
		 * remember it, for bark_killitems to mark when the scan leaves the
		 * page.
		 */
		bark_kill_prior(so);
	}

	/*
	 * Here the row returned last (currPos's member memberIndex of item
	 * itemIndex) is settled: the executor has said whether it was dead, and
	 * under rule D the table AM whether it returned the row through the entry
	 * (bark_mk_settle), before the position moves.
	 *
	 * Advance within the page read last, a member at a time.  A reversal of
	 * direction continues from the row returned last, in the new direction,
	 * which is what a scroll cursor needs; only running off either end of the
	 * page's items steps to a page.  A fresh page enters its first item in
	 * the scan direction.  Under rule D an entry of a row in R, or one the
	 * table AM rejected (J), is passed over.
	 */
	for (;;)
	{
		BarkScanPosItem *item;

		while (!bark_step_member(so, dir))
		{
			if (!bark_steppage(scan, dir) && !bark_mk_order_rest(scan, dir))
				return false;
		}
		if (!ruled)
			break;
		item = &so->currPos.items[so->currPos.itemIndex];
		if (likely((item->indexOffset & BARK_ITEM_MEMBERS) == 0))
			tid = item->heapTid;
		else
		{
			if (so->memberItem != so->currPos.itemIndex)
				(void) bark_load_members(so, so->currPos.itemIndex);
			tid = so->memberTids[so->currPos.memberIndex];
		}
		/* the group changes on the item's first member in the scan's order */
		if (bark_mk_admit(scan, &tid, item->tag,
						  (item->indexOffset & BARK_ITEM_NEWGROUP) != 0 &&
						  so->currPos.memberIndex ==
						  (ScanDirectionIsForward(dir) ? 0 :
						   so->currPos.nmembers - 1)))
			break;
	}

	bark_saveitem(scan);
	return true;
}

/*
 * TIDs go into a TIDBitmap in batches.  tbm_add_tuples looks up a heap
 * page's entry once per run of TIDs on that page within one call, so a call
 * per TID pays a hash lookup per TID.  A bitmap scan collects its single
 * TIDs into a batch per recheck flag (the flag is kept per heap page in the
 * bitmap, so splitting by it is exact), allocated once per scan below
 * AllocSet's chunk limit and flushed when full and when the scan's
 * additions end (bark_tbm_end).  The order in which TIDs reach a bitmap
 * does not matter and nothing reads it until amgetbitmap returns, so a page
 * boundary needs no flush.  A LIST or POSTING entry's members are an array
 * already and go straight in.
 */
#define BARK_TBM_BATCH_SIZE	512

typedef struct BarkTbmBatch
{
	TIDBitmap  *tbm;
	int			ntids[2];		/* exact, recheck */
	ItemPointerData tids[2][BARK_TBM_BATCH_SIZE];
}			BarkTbmBatch;

StaticAssertDecl(sizeof(BarkTbmBatch) <= ALLOCSET_SEPARATE_THRESHOLD,
				 "BarkTbmBatch must stay below AllocSet's chunk limit");

/* Start adding TIDs to tbm, through the scan's batch. */
static BarkTbmBatch *
bark_tbm_begin(BarkScanOpaque so, TIDBitmap *tbm)
{
	if (so->tbmBatch == NULL)
		so->tbmBatch = MemoryContextAlloc(so->scanCxt, sizeof(BarkTbmBatch));
	so->tbmBatch->tbm = tbm;
	so->tbmBatch->ntids[0] = so->tbmBatch->ntids[1] = 0;
	return so->tbmBatch;
}

static void
bark_tbm_flush(BarkTbmBatch * batch, int r)
{
	tbm_add_tuples(batch->tbm, batch->tids[r], batch->ntids[r], r == 1);
	batch->ntids[r] = 0;
}

/* Add the n TIDs at tids to the batch's bitmap, with recheck. */
static inline void
bark_tbm_add(BarkTbmBatch * batch, ItemPointer tids, int n, bool recheck)
{
	int			r = recheck ? 1 : 0;

	if (n > 1)
		tbm_add_tuples(batch->tbm, tids, n, recheck);
	else if (n == 1)
	{
		if (batch->ntids[r] == BARK_TBM_BATCH_SIZE)
			bark_tbm_flush(batch, r);
		batch->tids[r][batch->ntids[r]++] = *tids;
	}
}

/*
 * Add every TID of currPos's items to the batch's bitmap, each with a
 * recheck when recheck or its item asks for one, and return how many.  The
 * counts are kept in locals while copying: PostgreSQL is built with
 * -fno-strict-aliasing, so a count in the batch would be reloaded after
 * every TID stored.
 */
static int64
bark_tbm_add_pos(BarkScanOpaque so, BarkTbmBatch * batch, bool recheck)
{
	int			nexact = batch->ntids[0];
	int			nrecheck = batch->ntids[1];
	int64		ntids = 0;

	for (int i = so->currPos.firstItem; i <= so->currPos.lastItem; i++)
	{
		BarkScanPosItem *item = &so->currPos.items[i];
		bool		r = recheck || (item->indexOffset & BARK_ITEM_RECHECK) != 0;

		if (item->indexOffset & BARK_ITEM_MEMBERS)
		{
			ItemPointer tids;
			int			n = bark_item_tids(so, i, &tids);

			tbm_add_tuples(batch->tbm, tids, n, r);
			ntids += n;
			continue;
		}
		if (!r)
		{
			if (nexact == BARK_TBM_BATCH_SIZE)
			{
				batch->ntids[0] = nexact;
				bark_tbm_flush(batch, 0);
				nexact = 0;
			}
			batch->tids[0][nexact++] = item->heapTid;
		}
		else
		{
			if (nrecheck == BARK_TBM_BATCH_SIZE)
			{
				batch->ntids[1] = nrecheck;
				bark_tbm_flush(batch, 1);
				nrecheck = 0;
			}
			batch->tids[1][nrecheck++] = item->heapTid;
		}
		ntids++;
	}
	batch->ntids[0] = nexact;
	batch->ntids[1] = nrecheck;
	return ntids;
}

/* Flush what the batch holds into its bitmap. */
static void
bark_tbm_end(BarkTbmBatch * batch)
{
	for (int r = 0; r < 2; r++)
	{
		if (batch->ntids[r] > 0)
			bark_tbm_flush(batch, r);
	}
}

/*
 * GIN's query interface (procedures 10-13; "Which interface first." in
 * BARK-Design.mediawiki).  A scan key on a column whose class has
 * procedure 10 and not 8 is answered row by row, as ginget.c answers it:
 * extractQuery's keys become boundaries, each read on its own (a point, a
 * partial-match range from the key on, the NULL entries for a NULL key or
 * for a search mode that includes empty values, every key for ALL); each
 * row met is given its per-key presence vector, and triConsistent (13), or
 * consistent (11), decides it.  That needs every entry of a row before the
 * answer, so a scan computes its whole result before its first row: a
 * bitmap scan adds it to the bitmap, an index scan keeps it in a bitmap of
 * its own and returns it a row at a time (bark_mk_gin_gettuple).
 *
 * Each boundary is read into a stream: a TIDBitmap of its own, bounded by
 * work_mem and lossy beyond it, as ginget.c's collectMatchBitmap reads a
 * partial match (BarkGinStream).  A bitmap needs no sort and absorbs a
 * row's repeats within a boundary (a row with several keys in a partial
 * match), and its memory does not grow with the result: about 64 bytes per
 * heap page exact, a few bytes per page lossy.  The streams are iterated
 * in heap block order and merged a heap page at a time (bark_mk_gin_key),
 * as ginget.c's keyGetItem merges its entries' streams: on each page held
 * by a required stream, a page any stream holds lossy is first tried
 * whole, with the lossy streams MAYBE and the others FALSE; when
 * triConsistent says that is enough, the whole page goes in lossy, else
 * each exact offset on the page is decided on its presence vector, with
 * the lossy streams MAYBE.  When triConsistent says that no row lacking all
 * of some streams can match (a pure AND such as @>), only those required
 * streams drive the merge, rarest first, and the others are only advanced
 * to the pages they drive (GIN's required and additional entries), so a
 * rare key with a frequent one costs about as many consistent calls as the
 * rare key has rows.  The frequent key's entries are still read: each
 * boundary is read to its end into its bitmap, so the skip saves the merge
 * and the consistent calls, not the leaf pages.
 *
 * A bitmap scan of a key that every row of any of its boundaries matches (a
 * partial match over a path, an OR of prefixes) skips the streams: each
 * boundary's TIDs go straight into the destination (bark_mk_gin_union).
 *
 * Several keys on the column are intersected, each key's result in a
 * bitmap of its own (tbm_intersect), and a ScalarArrayOp's elements
 * unioned in one bitmap.  A row met through a NULL entry comes back with a
 * recheck, since that entry stands for a NULL value, an empty value and a
 * NULL element alike.  No allocation of the scan grows with its result:
 * the bitmaps are TIDBitmaps, and everything else is per query key or per
 * heap page.
 */

/*
 * An exact stream's TIDs, ascending, in a list of chunks below AllocSet's
 * chunk limit (one heap TID and its recheck flag per slot).
 */
#define BARK_GIN_CHUNK_TIDS \
	((ALLOCSET_SEPARATE_THRESHOLD - 256 - MAXALIGN(sizeof(void *) + sizeof(int))) / \
	 (sizeof(ItemPointerData) + sizeof(bool)))

typedef struct BarkGinChunk
{
	struct BarkGinChunk *next;
	int			n;
	ItemPointerData tids[BARK_GIN_CHUNK_TIDS];
	bool		recheck[BARK_GIN_CHUNK_TIDS];
}			BarkGinChunk;

StaticAssertDecl(sizeof(BarkGinChunk) <= ALLOCSET_SEPARATE_THRESHOLD - 64,
				 "BarkGinChunk must stay below AllocSet's chunk limit");

/*
 * One boundary's rows, iterated a heap page at a time: positioned on heap
 * page blkno (InvalidBlockNumber once the stream is done), which is lossy,
 * or whose exact offsets are offs[0 .. noffs - 1], with recheck flags
 * offrecheck.  A boundary whose TIDs come in TID order (a point on the
 * leading column) is exact: its TIDs are kept in chunks (first, the
 * read's place cur and curi).  Any other is read into a TIDBitmap of its
 * own and iterated (tbm, iter, res), its offsets extracted when the merge
 * first needs them (noffs -1 until then: an additional stream passes most
 * of its pages unread), with the page's recheck flag for every offset.
 */
typedef struct BarkGinStream
{
	int			entry;			/* the query key, or nentries for a hidden one */
	int64		ntids;			/* the TIDs read, to order the streams */
	BlockNumber blkno;
	bool		lossy;
	int			noffs;
	int			pos;			/* the merge's place in offs */
	TIDBitmap  *tbm;			/* NULL for an exact stream */
	TBMPrivateIterator *iter;
	TBMIterateResult res;
	BarkGinChunk *first;
	BarkGinChunk *cur;
	int			curi;
	OffsetNumber offs[TBM_MAX_TUPLES_PER_PAGE];
	bool		offrecheck[TBM_MAX_TUPLES_PER_PAGE];
}			BarkGinStream;

/* Move stream s to its next heap page; false when it has none. */
static bool
bark_gin_stream_next(BarkGinStream * s)
{
	s->pos = 0;
	if (s->tbm == NULL)
	{
		/* An exact stream: the run of its TIDs on the next heap page. */
		s->noffs = 0;
		s->lossy = false;
		/* the loop below leaves cur on an unread TID, or NULL */
		if (s->cur == NULL)
		{
			s->blkno = InvalidBlockNumber;
			return false;
		}
		s->blkno = ItemPointerGetBlockNumber(&s->cur->tids[s->curi]);
		while (s->cur != NULL &&
			   ItemPointerGetBlockNumber(&s->cur->tids[s->curi]) == s->blkno)
		{
			/* distinct offsets of one heap page, at most a page's worth */
			Assert(s->noffs < TBM_MAX_TUPLES_PER_PAGE);
			s->offs[s->noffs] = ItemPointerGetOffsetNumber(&s->cur->tids[s->curi]);
			s->offrecheck[s->noffs++] = s->cur->recheck[s->curi];
			if (++s->curi == s->cur->n)
			{
				s->cur = s->cur->next;
				s->curi = 0;
			}
		}
		return true;
	}
	if (s->iter == NULL || !tbm_private_iterate(s->iter, &s->res))
	{
		s->blkno = InvalidBlockNumber;
		return false;
	}
	s->blkno = s->res.blockno;
	s->lossy = s->res.lossy;
	s->noffs = s->lossy ? 0 : -1;
	return true;
}

/* Move stream s on to its first page at or after blkno; is that blkno? */
static bool
bark_gin_stream_seek(BarkGinStream * s, BlockNumber blkno)
{
	while (BlockNumberIsValid(s->blkno) && s->blkno < blkno)
		bark_gin_stream_next(s);
	return s->blkno == blkno;
}

/* binaryheap comparator: the stream with the smallest heap page on top. */
static int
bark_gin_stream_cmp(Datum a, Datum b, void *arg)
{
	BarkGinStream **streams = (BarkGinStream * *) arg;

	return pg_cmp_u32(streams[DatumGetInt32(b)]->blkno,
					  streams[DatumGetInt32(a)]->blkno);
}

/* qsort_arg comparator: stream indexes by the streams' sizes. */
static int
bark_gin_stream_size_cmp(const void *a, const void *b, void *arg)
{
	BarkGinStream **streams = (BarkGinStream * *) arg;

	return pg_cmp_s64(streams[*(const int *) a]->ntids,
					  streams[*(const int *) b]->ntids);
}

/* Position the scan on boundary b, for a read of its entries. */
static bool
bark_mk_gin_first(IndexScanDesc scan, const BarkMkBoundary *b)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;

	so->mk->bounds[0] = *b;
	so->mk->partialpast = false;

	/*
	 * The read goes forward from the boundary's start.  On the leading
	 * column, ascending, entries come in key order with the NULLs together at
	 * one end, so the non-NULL keys met after one inside an open-ended
	 * boundary are inside it too (bark_mk_cmp skips their compares).  A later
	 * column restarts its keys at each new value of the columns before, so it
	 * never latches.  A partial boundary has one side, set by
	 * bark_mk_set_side from its lower element: the start on an ascending
	 * column, the end on a DESC one, which therefore never latches either.
	 * The GIN reads use bounds[0] alone (nbounds is 1), so no other boundary
	 * is ever compared while the latch is set.
	 */
	so->mk->startlatch = b->partial && b->hasstart && so->mk->attno == 1;
	so->mk->instart = false;
	BarkScanPosUnpinIfPinned(so->currPos);
	BarkScanPosInvalidate(so->currPos);
	return bark_first(scan, ForwardScanDirection);
}

/*
 * Add the n TIDs at tids to the batch's bitmap with recheck.  A TIDBitmap
 * holds offsets only up to TBM_MAX_TUPLES_PER_PAGE, so the bitmaps of an
 * index scan (gintbm, and its streams and per-key results, ginfolding) of
 * an index whose TID stride is larger keep heap block B's offset O at page
 * B * ginfold + (O - 1) / TBM_MAX_TUPLES_PER_PAGE, offset
 * (O - 1) % TBM_MAX_TUPLES_PER_PAGE + 1: one-to-one, and in TID order, so
 * a merge of such bitmaps and the iteration are still in TID order; the
 * merge works on those pages as if they were heap pages, which is exact
 * (a lossy one stands for a run of offsets of one heap block), and
 * bark_mk_gin_load turns each page back.  Any other bitmap is the
 * executor's, which gets the TID as it is (the planner makes no bitmap
 * scan of such a table; a caller of the AM that asks gets tbm_add_tuples's
 * error).
 *
 * A heap block past MaxBlockNumber / ginfold (4 TB at BLCKSZ 8192, with
 * the largest stride) has no such page, and the scan reports it.
 */
static void
bark_mk_gin_tbm_add(BarkMkScanState *mk, BarkTbmBatch * batch,
					ItemPointer tids, int n, bool recheck)
{
	if (!mk->ginfolding)
	{
		bark_tbm_add(batch, tids, n, recheck);
		return;
	}
	for (int i = 0; i < n; i++)
	{
		BlockNumber blk = ItemPointerGetBlockNumber(&tids[i]);
		OffsetNumber off = ItemPointerGetOffsetNumber(&tids[i]) - 1;
		ItemPointerData folded;

		if (blk > (MaxBlockNumber - (mk->ginfold - 1)) / mk->ginfold)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("heap block %u is too large for an index scan through GIN's query interface",
							blk)));
		ItemPointerSet(&folded,
					   blk * mk->ginfold + off / TBM_MAX_TUPLES_PER_PAGE,
					   off % TBM_MAX_TUPLES_PER_PAGE + 1);
		bark_tbm_add(batch, &folded, 1, recheck);
	}
}

/*
 * Read boundary b's entries straight into the batch's bitmap, each TID with
 * recheck when recheck or its entry asks for one, a page at a time
 * (bark_mk_gin_read_bitmap_pos, the page the scan is on).  Returns how many
 * TIDs were added.
 */
static int64
bark_mk_gin_read_bitmap_pos(IndexScanDesc scan, bool recheck,
							BarkTbmBatch * batch)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	int64		ntids = 0;

	if (!so->mk->ginfolding)
		return bark_tbm_add_pos(so, batch, recheck);
	for (int i = so->currPos.firstItem; i <= so->currPos.lastItem; i++)
	{
		ItemPointer tids;
		int			n = bark_item_tids(so, i, &tids);
		bool		r = recheck || (so->currPos.items[i].indexOffset &
									BARK_ITEM_RECHECK) != 0;

		bark_mk_gin_tbm_add(so->mk, batch, tids, n, r);
		ntids += n;
	}
	return ntids;
}

static int64
bark_mk_gin_read_bitmap(IndexScanDesc scan, const BarkMkBoundary *b,
						bool recheck, BarkTbmBatch * batch)
{
	int64		ntids = 0;

	if (!bark_mk_gin_first(scan, b))
		return 0;
	do
	{
		ntids += bark_mk_gin_read_bitmap_pos(scan, recheck, batch);
	} while (bark_steppage(scan, ForwardScanDirection));
	return ntids;
}

/*
 * A new TIDBitmap for an intermediate result of the GIN-interface scan, in
 * the current context (the query's, bark_mk_gin_items), bounded by work_mem
 * as the executor's own bitmaps are.
 */
static TIDBitmap *
bark_mk_gin_tbm_create(void)
{
	return tbm_create(work_mem * (Size) 1024, NULL);
}

/* Give exact stream s's chunks back to the scan's free list. */
static void
bark_gin_stream_release(BarkMkScanState *mk, BarkGinStream * s)
{
	while (s->first != NULL)
	{
		BarkGinChunk *c = s->first;

		s->first = c->next;
		c->next = mk->ginfree;
		mk->ginfree = c;
	}
	s->cur = NULL;
}

/*
 * Read the rest of the boundary that the scan is positioned on (its current
 * page from item i on) into stream s's TIDBitmap, after the TIDs already in
 * its chunks, which go in first and are released.
 */
static void
bark_gin_stream_to_bitmap(IndexScanDesc scan, BarkGinStream * s, int i,
						  BarkTbmBatch * batch)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;

	s->tbm = bark_mk_gin_tbm_create();
	batch->tbm = s->tbm;
	for (BarkGinChunk * c = s->first; c != NULL; c = c->next)
	{
		for (int j = 0; j < c->n; j++)
			bark_tbm_add(batch, &c->tids[j], 1, c->recheck[j]);
	}
	bark_gin_stream_release(so->mk, s);
	for (; i <= so->currPos.lastItem; i++)
	{
		ItemPointer tids;
		int			n = bark_item_tids(so, i, &tids);

		bark_mk_gin_tbm_add(so->mk, batch, tids, n,
							(so->currPos.items[i].indexOffset &
							 BARK_ITEM_RECHECK) != 0);
		s->ntids += n;
	}
	while (bark_steppage(scan, ForwardScanDirection))
		s->ntids += bark_mk_gin_read_bitmap_pos(scan, false, batch);
	bark_tbm_end(batch);
}

/*
 * Read boundary b's entries, with the scan's other keys as filters, into a
 * new stream for query key entry, positioned on its first page.  A point
 * on the leading column comes in TID order and is kept exact, in
 * chunks taken from the scan's free list; a TID that does not ascend (the
 * index has key columns after this one, whose order breaks a point's runs)
 * turns the stream into a TIDBitmap from there on, as is any other
 * boundary.  The scan's batch fills a stream's bitmap, then goes back to
 * its own.
 */
static BarkGinStream *
bark_mk_gin_read(IndexScanDesc scan, const BarkMkBoundary *b, int entry,
				 BarkTbmBatch * batch)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	BarkMkScanState *mk = so->mk;
	TIDBitmap  *dest = batch->tbm;
	BarkGinStream *s = palloc0_object(BarkGinStream);
	BarkGinChunk *last = NULL;
	ItemPointerData prev;

	s->entry = entry;
	bark_tbm_end(batch);		/* what is pending is dest's */
	/* offs[] holds a heap page's offsets only up to TBM_MAX_TUPLES_PER_PAGE */
	if (!(b->point && !b->nulls && mk->attno == 1 && mk->ginfold == 1))
	{
		s->tbm = bark_mk_gin_tbm_create();
		batch->tbm = s->tbm;
		s->ntids = bark_mk_gin_read_bitmap(scan, b, false, batch);
		bark_tbm_end(batch);
		batch->tbm = dest;
		s->iter = tbm_is_empty(s->tbm) ? NULL :
			tbm_begin_private_iterate(s->tbm);
		bark_gin_stream_next(s);
		return s;
	}

	ItemPointerSetInvalid(&prev);
	if (bark_mk_gin_first(scan, b))
	{
		do
		{
			for (int i = so->currPos.firstItem; i <= so->currPos.lastItem; i++)
			{
				ItemPointer tids;
				int			n = bark_item_tids(so, i, &tids);
				bool		r = (so->currPos.items[i].indexOffset &
								 BARK_ITEM_RECHECK) != 0;

				if (ItemPointerIsValid(&prev) &&
					ItemPointerCompare(&tids[0], &prev) <= 0)
				{
					bark_gin_stream_to_bitmap(scan, s, i, batch);
					batch->tbm = dest;
					s->iter = tbm_begin_private_iterate(s->tbm);
					bark_gin_stream_next(s);
					return s;
				}
				for (int j = 0; j < n; j++)
				{
					if (last == NULL || last->n == BARK_GIN_CHUNK_TIDS)
					{
						BarkGinChunk *c = mk->ginfree;

						if (c != NULL)
							mk->ginfree = c->next;
						else
							c = MemoryContextAlloc(mk->ginChunkCxt,
												   sizeof(BarkGinChunk));
						c->next = NULL;
						c->n = 0;
						if (last == NULL)
							s->first = c;
						else
							last->next = c;
						last = c;
					}
					last->tids[last->n] = tids[j];
					last->recheck[last->n++] = r;
				}
				prev = tids[n - 1];
				s->ntids += n;
			}
		} while (bark_steppage(scan, ForwardScanDirection));
	}
	s->cur = s->first;
	s->curi = 0;
	bark_gin_stream_next(s);
	return s;
}

/*
 * One scan key's query as extractQuery gave it, and scratch space for its
 * consistent calls: check and tcheck, nentries + 1 each (the last slot is
 * the hidden entries', which consistent never reads).
 */
typedef struct BarkGinKey
{
	ScanKey		sk;
	Datum		query;
	bool		nulltest;		/* IS NULL or IS NOT NULL: no consistent */
	int32		nentries;
	Datum	   *qkeys;
	Pointer    *extra;
	bool	   *nullFlags;
	bool	   *check;
	GinTernaryValue *tcheck;
}			BarkGinKey;

/* triConsistent (13) on key->tcheck, as it answers. */
static GinTernaryValue
bark_mk_gin_tricall(BarkMkScanState *mk, BarkGinKey * key)
{
	return DatumGetGinTernaryValue(FunctionCall7Coll(mk->gintriconsistent,
													 mk->col->collation,
													 PointerGetDatum(key->tcheck),
													 UInt16GetDatum(key->sk->sk_strategy),
													 key->query,
													 Int32GetDatum(key->nentries),
													 PointerGetDatum(key->extra),
													 PointerGetDatum(key->qkeys),
													 PointerGetDatum(key->nullFlags)));
}

/*
 * Does a row whose presence vector is key->check[0 .. nentries - 1] match
 * key?  triConsistent (13) answers when the class has it, else consistent
 * (11); *recheck is set when the answer needs one.  A NULL test has no
 * consistent call: bark_mk_gin_triconsistent answers it.
 */
static bool
bark_mk_gin_consistent(BarkMkScanState *mk, BarkGinKey * key, bool *recheck)
{
	Assert(!key->nulltest);
	*recheck = false;
	if (mk->gintriconsistent != NULL)
	{
		GinTernaryValue r;

		for (int k = 0; k < key->nentries; k++)
			key->tcheck[k] = key->check[k] ? GIN_TRUE : GIN_FALSE;
		r = bark_mk_gin_tricall(mk, key);
		*recheck = r == GIN_MAYBE;
		return r != GIN_FALSE;
	}
	else
	{
		/* As GIN's directBoolConsistentFn: recheck unless told not to. */
		bool		crecheck = true;
		bool		match;

		match = DatumGetBool(FunctionCall8Coll(mk->ginconsistent,
											   mk->col->collation,
											   PointerGetDatum(key->check),
											   UInt16GetDatum(key->sk->sk_strategy),
											   key->query,
											   Int32GetDatum(key->nentries),
											   PointerGetDatum(key->extra),
											   PointerGetDatum(&crecheck),
											   PointerGetDatum(key->qkeys),
											   PointerGetDatum(key->nullFlags)));
		*recheck = crecheck;
		return match;
	}
}

/*
 * Does a row whose presence vector is key->tcheck[0 .. nentries - 1], which
 * may hold GIN_MAYBE (an entry whose stream holds the row's heap page
 * lossy), match key?  GIN_TRUE, GIN_FALSE, or GIN_MAYBE for a match that
 * needs a recheck.  Without triConsistent, consistent is asked about every
 * combination of up to four MAYBE entries, as ginlogic.c's shim does, and
 * with more the answer is MAYBE.
 */
static GinTernaryValue
bark_mk_gin_triconsistent(BarkMkScanState *mk, BarkGinKey * key)
{
	int			maybe[4];
	int			nmaybe = 0;
	bool		first = false;
	bool		anyrecheck = false;

	if (key->nulltest)
		return GIN_TRUE;
	if (mk->gintriconsistent != NULL)
	{
		GinTernaryValue r = bark_mk_gin_tricall(mk, key);

		/* As keyGetItem: anything else is a match that needs a recheck. */
		return r == GIN_TRUE || r == GIN_FALSE ? r : GIN_MAYBE;
	}
	for (int k = 0; k < key->nentries; k++)
	{
		if (key->tcheck[k] == GIN_MAYBE)
		{
			if (nmaybe == lengthof(maybe))
				return GIN_MAYBE;
			maybe[nmaybe++] = k;
		}
		key->check[k] = key->tcheck[k] == GIN_TRUE;
	}
	for (uint32 v = 0; v < ((uint32) 1 << nmaybe); v++)
	{
		bool		recheck;
		bool		match;

		for (int i = 0; i < nmaybe; i++)
			key->check[maybe[i]] = (v & ((uint32) 1 << i)) != 0;
		match = bark_mk_gin_consistent(mk, key, &recheck);
		if (v == 0)
			first = match;
		else if (match != first)
			return GIN_MAYBE;
		anyrecheck |= recheck;
	}
	if (!first)
		return GIN_FALSE;
	return anyrecheck ? GIN_MAYBE : GIN_TRUE;
}

/*
 * Is every row that any of key's nentries query keys holds a match,
 * whichever of the others hold it too?  Then the key's rows are the union
 * of its boundaries', and each boundary can be read straight into the
 * key's bitmap (bark_mk_gin_key).  Consistent is asked about every
 * non-empty presence vector, as ginlogic.c's shim enumerates MAYBE inputs,
 * so only a few keys are tried.  The bitmap keeps a recheck per key, not
 * per vector, so *rmask gets the keys whose rows go in with a recheck: each
 * key whose rows alone need one, and every key of a vector that needs one
 * when none of its keys is in the mask yet.  The vectors come in numeric
 * order, so each single key is tried before the vectors that contain it.
 */
static bool
bark_mk_gin_union(BarkMkScanState *mk, BarkGinKey * key, uint32 *rmask)
{
	Assert(key->nentries >= 1);
	*rmask = 0;
	if (key->nentries > 4)
		return false;
	for (uint32 v = 1; v < ((uint32) 1 << key->nentries); v++)
	{
		bool		recheck;

		for (int i = 0; i < key->nentries; i++)
			key->check[i] = (v & ((uint32) 1 << i)) != 0;
		if (!bark_mk_gin_consistent(mk, key, &recheck))
			return false;
		if (recheck && (v & *rmask) == 0)
			*rmask |= v;
	}
	return true;
}

/*
 * Decide the rows of heap page blkno for key, from the streams on that
 * page, streams[on[0 .. non - 1]], of which the first nreq are required,
 * and add the matches to the batch's bitmap.  Returns how many were added,
 * a lossy page counting one, as gingetbitmap counts.
 *
 * As ginget.c's keyGetItem: when a stream holds the page lossy, the page is
 * first tried whole, with the lossy entries MAYBE and the others FALSE; if
 * that can match, any row on the page may, and the page goes in lossy.
 * Otherwise a match has at least one exact entry, so the candidates are
 * the exact streams' offsets: the required ones', or every exact stream's
 * when a required stream is lossy here (a row of it may be in an
 * additional stream alone).  Each candidate gets its presence vector, the
 * lossy entries MAYBE, and comes back with a recheck when a stream that
 * holds it asked for one, when an entry was MAYBE, or when consistent says.
 */
static int64
bark_mk_gin_page(BarkMkScanState *mk, BarkGinKey * key,
				 BarkGinStream * *streams, int *on, int non, int nreq,
				 BlockNumber blkno, BarkTbmBatch * batch)
{
	bool		anylossy = false;
	bool		reqlossy = false;
	int64		nadded = 0;

	for (int k = 0; k < key->nentries; k++)
		key->tcheck[k] = GIN_FALSE;
	for (int i = 0; i < non; i++)
	{
		BarkGinStream *s = streams[on[i]];

		if (s->lossy)
		{
			anylossy = true;
			reqlossy |= i < nreq;
			if (s->entry < key->nentries)
				key->tcheck[s->entry] = GIN_MAYBE;
		}
		else if (s->noffs < 0)
		{
			s->noffs = tbm_extract_page_tuple(&s->res, s->offs,
											  TBM_MAX_TUPLES_PER_PAGE);
			memset(s->offrecheck, s->res.recheck, s->noffs);
		}
	}
	if (anylossy && bark_mk_gin_triconsistent(mk, key) != GIN_FALSE)
	{
		tbm_add_page(batch->tbm, blkno);
		return 1;
	}

	for (;;)
	{
		int			o = INT_MAX;
		bool		recheck = anylossy;
		GinTernaryValue r;

		for (int i = 0; i < non; i++)
		{
			BarkGinStream *s = streams[on[i]];

			if (!s->lossy && (i < nreq || reqlossy) && s->pos < s->noffs)
				o = Min(o, s->offs[s->pos]);
		}
		if (o == INT_MAX)
			break;
		for (int i = 0; i < non; i++)
		{
			BarkGinStream *s = streams[on[i]];
			bool		here;

			if (s->lossy)
				continue;
			while (s->pos < s->noffs && s->offs[s->pos] < o)
				s->pos++;
			here = s->pos < s->noffs && s->offs[s->pos] == o;
			if (here)
			{
				recheck |= s->offrecheck[s->pos];
				s->pos++;
			}
			if (s->entry < key->nentries)
				key->tcheck[s->entry] = here ? GIN_TRUE : GIN_FALSE;
		}
		r = bark_mk_gin_triconsistent(mk, key);
		if (r != GIN_FALSE)
		{
			ItemPointerData tid;

			ItemPointerSet(&tid, blkno, (OffsetNumber) o);
			bark_tbm_add(batch, &tid, 1, recheck || r == GIN_MAYBE);
			nadded++;
		}
	}
	return nadded;
}

/*
 * Add the rows that scan key sk, with query as its argument, accepts to the
 * batch's bitmap, with their recheck flags, and return how many were added.
 * When the key's rows are the union of its boundaries' (bark_mk_gin_union)
 * each boundary is read straight into the bitmap, as ginget.c's
 * collectMatchBitmap reads a partial match: no stream and no merge.  That
 * is the common shape of a partial match over a whole path.  Any other key
 * reads each boundary into a stream and merges them a heap page at a time
 * (bark_mk_gin_page).
 */
static int64
bark_mk_gin_key(IndexScanDesc scan, ScanKey sk, Datum query,
				BarkTbmBatch * batch)
{
	BarkMkScanState *mk = ((BarkScanOpaque) scan->opaque)->mk;
	BarkGinKey	key = {0};
	bool	   *pmatch = NULL;
	int32		mode = GIN_SEARCH_MODE_DEFAULT;
	BarkMkBoundary b;
	BarkGinStream **streams;
	int			nstreams = 0;
	int		   *order;
	int			nrequired;
	int		   *on;
	binaryheap *heap;
	bool		direct = false;
	uint32		rmask = 0;
	int64		nadded = 0;

	key.sk = sk;
	key.query = query;
	key.nulltest = (sk->sk_flags & (SK_SEARCHNULL | SK_SEARCHNOTNULL)) != 0;
	if (!key.nulltest)
	{
		key.qkeys = (Datum *)
			DatumGetPointer(FunctionCall7Coll(mk->ginquery, mk->col->collation,
											  query,
											  PointerGetDatum(&key.nentries),
											  UInt16GetDatum(sk->sk_strategy),
											  PointerGetDatum(&pmatch),
											  PointerGetDatum(&key.extra),
											  PointerGetDatum(&key.nullFlags),
											  PointerGetDatum(&mode)));
		/* As ginNewScanKey: a bogus mode is ALL, and EVERYTHING is GIN's own. */
		if (mode < GIN_SEARCH_MODE_DEFAULT || mode > GIN_SEARCH_MODE_ALL)
			mode = GIN_SEARCH_MODE_ALL;
		if (key.qkeys == NULL || key.nentries <= 0)
		{
			if (mode == GIN_SEARCH_MODE_DEFAULT)
				return 0;		/* no keys: nothing matches */
			key.nentries = 0;
		}
		if (key.nullFlags == NULL)
			key.nullFlags = palloc0_array(bool, key.nentries);
	}
	key.check = palloc0_array(bool, key.nentries + 1);
	key.tcheck = palloc_array(GinTernaryValue, key.nentries + 1);
	direct = !key.nulltest && mode == GIN_SEARCH_MODE_DEFAULT &&
		bark_mk_gin_union(mk, &key, &rmask);

	/* Streams 0 .. nentries - 1 are the query keys', then the hidden ones. */
	streams = palloc_array(BarkGinStream *, key.nentries + 2);
	for (int i = 0; i < key.nentries; i++)
	{
		memset(&b, 0, sizeof(b));
		if (key.nullFlags[i])
			b.nulls = b.point = b.recheck = true;
		else if (pmatch != NULL && pmatch[i] && mk->gincompare != NULL)
		{
			BarkSearchElement e = {key.qkeys[i], BTGreaterEqualStrategyNumber};

			bark_mk_set_side(scan, mk, &b, &e, true);
			b.partial = true;
			b.partialkey = key.qkeys[i];
			b.strategy = sk->sk_strategy;
			b.extra = key.extra != NULL ? key.extra[i] : NULL;
		}
		else
		{
			b.hasstart = b.hasend = b.point = true;
			b.start = b.end = key.qkeys[i];
		}
		if (direct)
			nadded += bark_mk_gin_read_bitmap(scan, &b,
											  (rmask & (((uint32) 1) << i)) != 0,
											  batch);
		else
			streams[nstreams++] = bark_mk_gin_read(scan, &b, i, batch);
	}
	if (direct)
		return nadded;

	/* The hidden entries: the NULL entries, and for ALL every key. */
	if (key.nulltest || mode != GIN_SEARCH_MODE_DEFAULT)
	{
		memset(&b, 0, sizeof(b));
		b.nulls = b.point = b.recheck = true;
		streams[nstreams++] = bark_mk_gin_read(scan, &b, key.nentries, batch);
		if (mode == GIN_SEARCH_MODE_ALL || (sk->sk_flags & SK_SEARCHNOTNULL))
		{
			memset(&b, 0, sizeof(b));
			b.recheck = key.nulltest;
			streams[nstreams++] = bark_mk_gin_read(scan, &b, key.nentries,
												   batch);
		}
	}

	/*
	 * Which streams are required: as startScanKey, take them rarest first
	 * until triConsistent says that a row in none of them cannot match, with
	 * every other key unknown.  A stream's size is the count of TIDs read
	 * into it, where GIN has to predict it.  Every stream is required without
	 * triConsistent, and with hidden streams, whose rows consistent may
	 * accept with no key present at all.
	 */
	order = palloc_array(int, nstreams);
	for (int i = 0; i < nstreams; i++)
		order[i] = i;
	nrequired = nstreams;
	if (!key.nulltest && mk->gintriconsistent != NULL &&
		nstreams == key.nentries && key.nentries > 1)
	{
		qsort_arg(order, nstreams, sizeof(int), bark_gin_stream_size_cmp,
				  streams);
		for (int i = 0; i < key.nentries; i++)
			key.tcheck[i] = GIN_MAYBE;
		for (nrequired = 1; nrequired < nstreams; nrequired++)
		{
			key.tcheck[order[nrequired - 1]] = GIN_FALSE;
			if (bark_mk_gin_tricall(mk, &key) == GIN_FALSE)
				break;
			CHECK_FOR_INTERRUPTS();
		}
	}

	/*
	 * The merge: the required streams in a binary heap on their heap pages,
	 * the others moved forward to each page the heap gives.
	 */
	heap = binaryheap_allocate(nrequired, bark_gin_stream_cmp, streams);
	for (int i = 0; i < nrequired; i++)
	{
		if (BlockNumberIsValid(streams[order[i]]->blkno))
			binaryheap_add_unordered(heap, Int32GetDatum(order[i]));
	}
	binaryheap_build(heap);
	on = palloc_array(int, nstreams);
	while (!binaryheap_empty(heap))
	{
		BlockNumber blkno = streams[DatumGetInt32(binaryheap_first(heap))]->blkno;
		int			non = 0;
		int			nreq;

		while (!binaryheap_empty(heap) &&
			   streams[DatumGetInt32(binaryheap_first(heap))]->blkno == blkno)
			on[non++] = DatumGetInt32(binaryheap_remove_first(heap));
		nreq = non;
		for (int i = nrequired; i < nstreams; i++)
		{
			if (bark_gin_stream_seek(streams[order[i]], blkno))
				on[non++] = order[i];
		}
		nadded += bark_mk_gin_page(mk, &key, streams, on, non, nreq, blkno,
								   batch);
		for (int i = 0; i < nreq; i++)
		{
			if (bark_gin_stream_next(streams[on[i]]))
				binaryheap_add(heap, Int32GetDatum(on[i]));
		}
		CHECK_FOR_INTERRUPTS();
	}
	for (int i = 0; i < nstreams; i++)
		bark_gin_stream_release(mk, streams[i]);
	return nadded;
}

/*
 * bark_mk_gin_key in keycxt, which is reset after it: the streams of one
 * key (or ScalarArrayOp element) are freed before the next is read.
 */
static int64
bark_mk_gin_key_in(IndexScanDesc scan, ScanKey sk, Datum query,
				   BarkTbmBatch * batch, MemoryContext keycxt)
{
	MemoryContext oldcxt = MemoryContextSwitchTo(keycxt);
	int64		n = bark_mk_gin_key(scan, sk, query, batch);

	MemoryContextSwitchTo(oldcxt);
	MemoryContextReset(keycxt);
	return n;
}

/*
 * Add every row of bitmap src to the batch's bitmap, a lossy page as a
 * lossy page, and return how many were added, a lossy page counting one.
 */
static int64
bark_mk_gin_copy(TIDBitmap *src, BarkTbmBatch * batch)
{
	TBMPrivateIterator *it = tbm_begin_private_iterate(src);
	TBMIterateResult res = {0};
	OffsetNumber offs[TBM_MAX_TUPLES_PER_PAGE];
	int64		n = 0;

	while (tbm_private_iterate(it, &res))
	{
		int			noffs;

		if (res.lossy)
		{
			tbm_add_page(batch->tbm, res.blockno);
			n++;
			continue;
		}
		noffs = tbm_extract_page_tuple(&res, offs, TBM_MAX_TUPLES_PER_PAGE);
		for (int i = 0; i < noffs; i++)
		{
			ItemPointerData tid;

			ItemPointerSet(&tid, res.blockno, offs[i]);
			bark_tbm_add(batch, &tid, 1, res.recheck);
		}
		n += noffs;
	}
	tbm_end_private_iterate(it);
	return n;
}

/*
 * Add the rows that a scan with keys on a column that uses GIN's query
 * interface accepts, every key accepting each, as in GIN, to tbm, with their
 * recheck flags, and return how many TIDs were added.  A single key on the
 * column fills tbm directly; with several, each key's rows go into a bitmap
 * of their own, intersected with the keys' before (tbm_intersect), and the
 * intersection into tbm.  The work happens in a context of its own,
 * deleted before returning, and no buffer stays pinned.  Both a bitmap
 * scan and an index scan (bark_mk_gin_gettuple) come here, so both are
 * bounded alike: every bitmap by work_mem, one per boundary of the key
 * being read plus one per key, and nothing else grows with the result.
 */
static int64
bark_mk_gin_items(IndexScanDesc scan, TIDBitmap *tbm)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	BarkMkScanState *mk = so->mk;
	MemoryContext cxt = AllocSetContextCreate(CurrentMemoryContext,
											  "BARK GIN query",
											  ALLOCSET_DEFAULT_SIZES);
	MemoryContext oldcxt = MemoryContextSwitchTo(cxt);
	MemoryContext keycxt = AllocSetContextCreate(cxt, "BARK GIN key",
												 ALLOCSET_DEFAULT_SIZES);
	BarkTbmBatch *batch = bark_tbm_begin(so, tbm);
	TIDBitmap  *acc = NULL;
	int64		nadded = 0;

	Assert(scan->parallel_scan == NULL);
	mk->ginfolding = tbm == mk->gintbm && mk->ginfold > 1;
	for (int k = 0; k < mk->nkeys; k++)
	{
		ScanKey		sk = &mk->keys[k];

		bark_tbm_end(batch);
		batch->tbm = mk->nkeys == 1 ? tbm : bark_mk_gin_tbm_create();
		if ((sk->sk_flags & SK_ISNULL) &&
			!(sk->sk_flags & (SK_SEARCHNULL | SK_SEARCHNOTNULL)))
			;					/* a strict operator with a NULL argument */
		else if (sk->sk_flags & SK_SEARCHARRAY)
		{
			ArrayType  *arr = DatumGetArrayTypeP(sk->sk_argument);
			int16		elmlen;
			bool		elmbyval;
			char		elmalign;
			Datum	   *elems;
			bool	   *nulls;
			int			nelems;

			/* The key's bitmap is the union of its elements' rows. */
			get_typlenbyvalalign(ARR_ELEMTYPE(arr), &elmlen, &elmbyval,
								 &elmalign);
			deconstruct_array(arr, ARR_ELEMTYPE(arr), elmlen, elmbyval,
							  elmalign, &elems, &nulls, &nelems);
			for (int e = 0; e < nelems; e++)
			{
				if (!nulls[e])
					nadded += bark_mk_gin_key_in(scan, sk, elems[e], batch,
												 keycxt);
			}
		}
		else
			nadded += bark_mk_gin_key_in(scan, sk, sk->sk_argument, batch,
										 keycxt);
		bark_tbm_end(batch);

		if (mk->nkeys == 1)
			break;
		if (acc == NULL)
			acc = batch->tbm;
		else
		{
			tbm_intersect(acc, batch->tbm);
			tbm_free(batch->tbm);
		}
		if (tbm_is_empty(acc))
			break;
	}
	if (acc != NULL)
	{
		batch->tbm = tbm;
		nadded = bark_mk_gin_copy(acc, batch);
	}
	bark_tbm_end(batch);
	BarkScanPosUnpinIfPinned(so->currPos);
	BarkScanPosInvalidate(so->currPos);
	mk->startlatch = mk->instart = false;
	mk->ginfolding = false;

	MemoryContextSwitchTo(oldcxt);
	MemoryContextDelete(cxt);
	return nadded;
}

/*
 * Load page k (in iteration order) of an index scan's bitmap into ginblkno
 * and ginoffs: its offsets, or every offset a TIDBitmap can hold for a lossy
 * page, whose rows then all need a recheck, as the heap block and offsets
 * they stand for (bark_mk_gin_tbm_add).  Returns false when there is no page
 * k, and then knows the number of pages.
 *
 * ponytail: a TIDBitmap iterates forward only, so moving back (a backward
 * step across a page, a restore) starts a new iteration and walks to the
 * page, O(pages) a step; the planner never asks a backward scan of such an
 * index (bark_property), so only a direct caller of the AM or a restore
 * pays it.  A doubly linked page list would cost memory past work_mem.
 */
static bool
bark_mk_gin_load(BarkMkScanState *mk, int k)
{
	TBMIterateResult res = {0};

	if (k == mk->ginpage)
		return true;
	if (mk->ginnpages >= 0 && k >= mk->ginnpages)
		return false;
	if (mk->giniter == NULL || k < mk->ginpage)
	{
		MemoryContext oldcxt = MemoryContextSwitchTo(mk->ginCxt);

		if (mk->giniter != NULL)
			tbm_end_private_iterate(mk->giniter);
		mk->giniter = tbm_begin_private_iterate(mk->gintbm);
		mk->ginpage = -1;
		MemoryContextSwitchTo(oldcxt);
	}
	while (mk->ginpage < k)
	{
		if (!tbm_private_iterate(mk->giniter, &res))
		{
			mk->ginnpages = mk->ginpage + 1;
			tbm_end_private_iterate(mk->giniter);
			mk->giniter = NULL;
			mk->ginpage = -1;
			return false;
		}
		mk->ginpage++;
	}
	mk->ginblkno = res.blockno;
	mk->ginrecheck = res.recheck;
	if (res.lossy)
	{
		mk->ginnoffs = TBM_MAX_TUPLES_PER_PAGE;
		for (int i = 0; i < mk->ginnoffs; i++)
			mk->ginoffs[i] = FirstOffsetNumber + i;
	}
	else
		mk->ginnoffs = tbm_extract_page_tuple(&res, mk->ginoffs,
											  TBM_MAX_TUPLES_PER_PAGE);
	if (mk->ginfold > 1)
	{
		int			base = (res.blockno % mk->ginfold) * TBM_MAX_TUPLES_PER_PAGE;
		int			n = 0;

		/* A lossy page's last offsets may be past the stride. */
		mk->ginblkno = res.blockno / mk->ginfold;
		for (int i = 0; i < mk->ginnoffs; i++)
		{
			if (base + mk->ginoffs[i] <= mk->ginstride)
				mk->ginoffs[n++] = base + mk->ginoffs[i];
		}
		mk->ginnoffs = n;
		Assert(n > 0);
	}
	return true;
}

/*
 * amgettuple for a scan with keys on a GIN-interface column.  The first call
 * after a rescan computes the scan's whole result into a TIDBitmap, as a
 * bitmap scan does and bounded by work_mem as it is, and each call returns
 * the next of its rows, in TID order, or in reverse TID order for a backward
 * scan (one that reverses continues from the same place, and one that ran
 * off an end restarts from it, as in bark_gettuple).  Past work_mem the
 * bitmap goes lossy, and a lossy heap page returns every offset a TIDBitmap
 * can hold, each with a recheck: a fetch finds nothing at an offset with no
 * tuple, and nothing at a heap-only tuple's (it is found from its chain's
 * root), so each row comes back once, and the recheck decides it.
 *
 * The TIDs are collected with no pin held on any leaf, so VACUUM may remove
 * an entry, and the heap reuse its slot for a new row, before the scan
 * returns its TID.  Only an MVCC snapshot makes that harmless: the new row
 * is not visible to it, nbtree's argument for dropping its pin
 * (so->dropPin).  A scan with any other snapshot gets an error, so such a
 * column cannot be in an exclusion constraint, whose check scans with a
 * dirty snapshot (bark_check_multikey_index refuses one).  The scan has no
 * leaf position to mark LP_DEAD hints on (bark_killitems), so
 * kill_prior_tuple is ignored; its reads skip the entries other scans
 * marked.
 * The planner makes no index-only, ordered or parallel scan of such an index
 * (bark_canreturn; the column has no sort family, and bark_property keeps an
 * index with columns before it to bitmap scans; plancat.c's amcanparallel);
 * a caller of the index AM that asks for one gets an error.  Mark and
 * restore keep a place in the result (bark_markpos).
 */
static bool
bark_mk_gin_gettuple(IndexScanDesc scan, ScanDirection dir)
{
	BarkMkScanState *mk = ((BarkScanOpaque) scan->opaque)->mk;

	if (!mk->ginready)
	{
		MemoryContext oldcxt;

		/*
		 * A caller of the AM with no heap relation (and so no snapshot) reads
		 * TIDs, not rows: DocumentDB asks whether a metadata key is present
		 * this way (ambeginscan, amrescan, amgettuple).  No heap slot is
		 * fetched, so recycling cannot hurt it.
		 */
		if (scan->heapRelation != NULL &&
			!IsMVCCLikeSnapshot(scan->xs_snapshot))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("index scans of BARK index \"%s\" through GIN's query interface require an MVCC snapshot",
							RelationGetRelationName(scan->indexRelation))));

		/*
		 * An ordering key on the column was refused at rescan
		 * (bark_mk_order_setup: a GIN-interface class has no procedure 8),
		 * and the planner makes no parallel scan of such an index.
		 */
		Assert(scan->numberOfOrderBys == 0);
		if (scan->parallel_scan != NULL)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("parallel index scans of BARK index \"%s\" through GIN's query interface are not supported",
							RelationGetRelationName(scan->indexRelation))));
		oldcxt = MemoryContextSwitchTo(mk->ginCxt);
		mk->gintbm = tbm_create(work_mem * (Size) 1024, NULL);
		(void) bark_mk_gin_items(scan, mk->gintbm);
		MemoryContextSwitchTo(oldcxt);
		mk->ginready = true;
		mk->ginp = -1;
		if (!ScanDirectionIsForward(dir))
		{
			/* Start past the last row: count the pages. */
			(void) bark_mk_gin_load(mk, INT_MAX);
			mk->ginp = mk->ginnpages;
		}
	}

	if (ScanDirectionIsForward(dir))
	{
		if (mk->ginnpages >= 0 && mk->ginp >= mk->ginnpages)
			return false;		/* past the last row: stay there */
		if (mk->ginp >= 0 && mk->gini + 1 < mk->ginnoffs)
			mk->gini++;
		else if (bark_mk_gin_load(mk, mk->ginp + 1))
		{
			mk->ginp++;
			mk->gini = 0;
		}
		else
		{
			mk->ginp = mk->ginnpages;	/* the failed load counted them */
			return false;
		}
	}
	else
	{
		bool		onpage = mk->ginp >= 0 &&
			(mk->ginnpages < 0 || mk->ginp < mk->ginnpages);

		if (mk->ginp < 0)
			return false;		/* before the first row: stay there */
		if (onpage && mk->gini > 0)
			mk->gini--;
		else if (mk->ginp == 0)
		{
			mk->ginp = -1;
			return false;
		}
		else
		{
			/* A page before one the scan has been on exists. */
			mk->ginp--;
			(void) bark_mk_gin_load(mk, mk->ginp);
			mk->gini = mk->ginnoffs - 1;
		}
	}
	ItemPointerSet(&scan->xs_heaptid, mk->ginblkno, mk->ginoffs[mk->gini]);
	scan->xs_heaptag = InvalidIndexVersionTag;	/* multikey: never tagged */
	scan->xs_recheck = mk->ginrecheck;
	return true;
}

/*
 * amgetbitmap: add every matching heap TID to the TIDBitmap.  Reads pages
 * through the same positioning and page reads as a forward gettuple scan,
 * as nbtree's btgetbitmap does, adding each page's items at once.  A TID
 * needs a recheck only when a multikey scan says so (bark_mk_accept); a
 * row's repeats under several keys are absorbed by the bitmap.  A bitmap
 * holds TIDs only, so entries' version tags are dropped here; the table AM
 * rechecks rows whose history may make an entry stale (see "Versioned
 * locators" in amlocator.h).
 */
int64
bark_getbitmap(IndexScanDesc scan, TIDBitmap *tbm)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;
	BarkTbmBatch *batch;
	int64		ntids = 0;

	if (so->mk != NULL)
		so->mk->bitmap = true;
	if (so->mk != NULL && so->mk->gin && so->mk->nkeys > 0)
		return bark_mk_gin_items(scan, tbm);
	if (!bark_first(scan, ForwardScanDirection))
		return 0;
	batch = bark_tbm_begin(so, tbm);
	do
	{
		ntids += bark_tbm_add_pos(so, batch, false);
	} while (bark_steppage(scan, ForwardScanDirection));
	bark_tbm_end(batch);
	return ntids;
}

void
bark_endscan(IndexScanDesc scan)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;

	if (so == NULL)
		return;
	bark_killitems(scan);
	bark_knn_endscan(scan);
	if (so->arrayCxt != NULL)
		MemoryContextDelete(so->arrayCxt);
	BarkScanPosUnpinIfPinned(so->currPos);
	BarkScanPosUnpinIfPinned(so->markPos);
	if (so->currPos.items)
		pfree(so->currPos.items);
	if (so->markPos.items)
		pfree(so->markPos.items);
	if (so->currTuples)
		pfree(so->currTuples);
	if (so->markTuples)
		pfree(so->markTuples);
	if (so->entryTids)
		pfree(so->entryTids);
	if (so->tbmBatch)
		pfree(so->tbmBatch);
	if (so->killRuns)
		pfree(so->killRuns);
	if (so->keyCmp)
		pfree(so->keyCmp);
	if (!so->skipValueByVal && so->skipValue != (Datum) 0)
		pfree(DatumGetPointer(so->skipValue));
	if (so->skipSupport)
		pfree(so->skipSupport);
	if (so->keyinfo)
		pfree(so->keyinfo);
	if (so->mk)
	{
		MemoryContextDelete(so->mk->seenCxt);
		MemoryContextDelete(so->mk->hitupCxt);
		if (so->mk->ginCxt != NULL)
			MemoryContextDelete(so->mk->ginCxt);
		if (so->mk->ginChunkCxt != NULL)
			MemoryContextDelete(so->mk->ginChunkCxt);
		FreeTupleDesc(so->mk->hitupdesc);
		if (so->mk->undo)
			pfree(so->mk->undo);
		pfree(so->mk);
	}
	pfree(so);
	scan->opaque = NULL;
}

/*
 * bark_markpos -- remember the current scan position (nbtree's btmarkpos).
 *
 * Only the itemIndex is recorded.  If the scan leaves the page before the
 * mark is moved, bark_steppage makes the full copy in markPos; a merge join
 * usually moves its mark first, so that copy is rarely needed.
 */
void
bark_markpos(IndexScanDesc scan)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;

	/* The planner never asks a parallel or an ordered-operator scan. */
	Assert(scan->parallel_scan == NULL);
	Assert(scan->numberOfOrderBys == 0);

	/* A GIN-interface scan marks its place in its result. */
	if (so->mk != NULL && so->mk->gin && so->mk->nkeys > 0)
	{
		so->mk->ginmarkp = so->mk->ginready ? so->mk->ginp : -1;
		so->mk->ginmarki = so->mk->gini;
		return;
	}

	/* An older mark may hold a pin (never a lock). */
	BarkScanPosUnpinIfPinned(so->markPos);

	/*
	 * The rows the scan adds to the seen set from here on are the ones a
	 * restore must take out again; the ones before stay, since the scan
	 * returns them, or returns them again from the marked page's copy. Under
	 * rule D the marked row is settled now: a restore resumes after it, so
	 * its verdict belongs to the time before the mark.
	 */
	if (so->mk != NULL)
	{
		if (bark_mk_ruled(scan))
			bark_mk_settle(scan);
		so->mk->marked = true;
		so->mk->nundo = 0;
	}

	if (BarkScanPosIsValid(so->currPos))
	{
		so->markItemIndex = so->currPos.itemIndex;
		so->markMemberIndex = so->currPos.memberIndex;
		so->markNmembers = so->currPos.nmembers;
	}
	else
	{
		BarkScanPosInvalidate(so->markPos);
		so->markItemIndex = -1;
	}
}

/*
 * bark_restrpos -- return the scan to the marked position (nbtree's
 * btrestrpos).  The next bark_gettuple advances from the marked row in the
 * scan direction: a row of a LIST or POSTING entry is restored to the same
 * member of the entry's item.
 */
void
bark_restrpos(IndexScanDesc scan)
{
	BarkScanOpaque so = (BarkScanOpaque) scan->opaque;

	/* The planner never asks a parallel or an ordered-operator scan. */
	Assert(scan->parallel_scan == NULL);
	Assert(scan->numberOfOrderBys == 0);

	if (so->mk != NULL && so->mk->gin && so->mk->nkeys > 0)
	{
		BarkMkScanState *mk = so->mk;

		if (!mk->ginready)
			return;
		mk->ginp = mk->ginmarkp;
		mk->gini = mk->ginmarki;

		/*
		 * A place on a page needs the page loaded (it exists: the scan was on
		 * it); one off an end does not, and loading past the last page only
		 * fails.
		 */
		if (mk->ginp >= 0)
			(void) bark_mk_gin_load(mk, mk->ginp);
		return;
	}

	if (so->mk != NULL)
	{
		BarkMkScanState *mk = so->mk;

		/*
		 * The entry whose verdict is due was returned after the mark, so the
		 * restored scan returns it again; core has reset its verdict
		 * (index_restrpos).  The marked row was settled by bark_markpos.
		 */
		mk->verdictdue = false;
		for (int i = 0; i < mk->nundo; i++)
		{
			uint64		key = bark_tid_to_key(&mk->undo[i],
											  so->keyinfo->tidstride);
			uint64	   *word = bark_seen_find(mk->seen, key >> 6);

			*word &= ~(UINT64CONST(1) << (key & 63));
			mk->nseen--;
		}
		mk->nundo = 0;
	}

	if (so->markItemIndex >= 0)
	{
		/*
		 * The scan has not left the marked page, so currPos still holds it;
		 * markPos may be stale and is not used.
		 */
		so->currPos.itemIndex = so->markItemIndex;
		so->currPos.memberIndex = so->markMemberIndex;
		so->currPos.nmembers = so->markNmembers;
	}
	else
	{
		bark_killitems(scan);
		BarkScanPosUnpinIfPinned(so->currPos);
		if (BarkScanPosIsValid(so->markPos))
		{
			bark_copy_pos(so, &so->currPos, &so->currTuples,
						  &so->currTuplesSize, &so->markPos, so->markTuples);
			so->memberItem = -1;	/* the loaded members are another page's */

			/*
			 * skipBound describes the page read last, not the marked one; the
			 * restored scan steps right from the marked page instead.
			 */
			if (so->skip)
				so->currPos.arrayReseek = false;
		}
		else
		{
			/*
			 * No mark was taken on a page, so the next gettuple starts the
			 * scan again (bark_first), as nbtree's does, with the required
			 * keys on their first combination, and an empty seen set.
			 */
			BarkScanPosInvalidate(so->currPos);
			if (so->mk != NULL)
				bark_mk_clear_seen(so->mk);
		}
	}
}

/*
 * bark_canreturn -- can an index-only scan return column `attno`?
 *
 * A BARK leaf entry is the full index tuple (every indexed column plus the
 * heap TID in t_tid), so any column can be returned without a heap fetch,
 * except an extracted column: its entries hold keys, not the column's
 * value.  A key does not determine the value (a row with several keys has
 * an entry per key, and the NULL key stands for a NULL value, an empty value
 * and a NULL element alike), so an index-only scan never returns it.  A
 * query that needs the value index-only names the column again as an
 * INCLUDE column, which holds the value whole and is returned; the qual is
 * then rechecked on it.  An
 * index whose extracted column uses GIN's query interface returns no column
 * at all: its scans compute TIDs (bark_mk_gin_gettuple) and never read an
 * entry for a row.
 */
bool
bark_canreturn(Relation index, int attno)
{
	if (bark_index_gin_query(index))
		return false;
	return !bark_column_is_extracted(index, attno);
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
