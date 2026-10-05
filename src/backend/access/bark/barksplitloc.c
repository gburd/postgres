/*-------------------------------------------------------------------------
 *
 * barksplitloc.c
 *	  Choose the split point for a BARK page split.
 *
 * bark_split gathers a full page's data items, in key order and with the
 * incoming item already in place, and asks bark_findsplitloc which of them
 * should be the first item on the new right page.  This is a simplified port
 * of nbtree's nbtsplitloc.c (_bt_findsplitloc and its helpers):
 *
 * - Only split points that fit are considered.  Sizes are counted in bytes,
 *   not items, so pages of mixed-size entries (long keys, LIST and POSTING
 *   entries) split where the bytes balance.
 * - The rightmost page on a level is split so the left page is left at the
 *   fillfactor (the leaf fillfactor reloption, BARK_NONLEAF_FILLFACTOR for
 *   internal pages).  Ascending inserts then fill pages as full as CREATE
 *   INDEX packs them, instead of leaving every left page half empty.  Any
 *   other page is split evenly by bytes.
 * - On a leaf, among the split points close to that target, the one whose
 *   neighboring items agree on the fewest leading key attributes is taken:
 *   a split between two very different keys leaves a pivot that separates
 *   them on fewer attributes, and suffix truncation (bark_truncate_pivot)
 *   keeps only those.  When every split point near the target falls inside
 *   one run of equal keys, the split moves to the edge of the run if the
 *   page has one (nbtree's "many duplicates" strategy).
 * - A leaf that holds a single key value is split so the left page is left
 *   BARK_SINGLEVAL_FILLFACTOR full (nbtree's "single value" strategy).
 *
 * nbtree's "split after new item" heuristic for composite keys is not
 * ported, and an internal page is always split at the point closest to the
 * target rather than at the one with the smallest first-right item.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/bark/barksplitloc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bark.h"
#include "common/int.h"
#include "utils/rel.h"

/* A legal split point: items[firstright..] go to the new right page. */
typedef struct BarkSplitPoint
{
	int			firstright;		/* index of the first item on the right page */
	int			leftfree;		/* bytes left free on the left page */
	int			rightfree;		/* bytes left free on the right page */
	int			delta;			/* distance from the target split */
} BarkSplitPoint;

/*
 * How far from the best-balanced leaf split point, as a fraction of the
 * page's data bytes, a split point's free space on either side may be and
 * still be considered.  With items of uniform size this admits about the best
 * 10% of split points.  nbtree's LEAF_SPLIT_DISTANCE.
 */
#define BARK_LEAF_SPLIT_DISTANCE	0.050

/* Bytes an item takes on a page, line pointer included. */
static inline int
bark_split_itemsz(IndexTuple itup)
{
	return MAXALIGN(IndexTupleSize(itup)) + sizeof(ItemIdData);
}

/*
 * Bytes, line pointer included, of the high key bark_split forms for the left
 * page when `firstright` becomes the right page's first item.
 *
 * The high key holds at most the key attributes, so a LIST or POSTING
 * entry's locator body is left behind; nbtree makes the same adjustment for
 * posting lists.  Suffix truncation (bark_truncate_pivot) can drop further
 * attributes, but as in nbtree the estimate does not try to predict it: the
 * high key is sized as if nothing were truncated, which can only overstate
 * it.  The one case where the high key can be larger than the item is an
 * oversized leaf key in an index of more than one column: without the
 * INCLUDE columns, or truncated to its leading key attributes, the key may
 * fit inline, at up to BarkMaxItemSize.
 */
static int
bark_split_hikeysz(Relation index, IndexTuple firstright)
{
	BarkEntryShape shape = BarkEntryGetShape(firstright);
	Size		sz = IndexTupleSize(firstright);

	if (shape == BARK_SHAPE_LIST || shape == BARK_SHAPE_POSTING)
		sz = BarkEntryGetBodyOffset(firstright);
	else if (shape == BARK_SHAPE_OVERSIZED && BarkOverflowIsLeaf(firstright) &&
			 IndexRelationGetNumberOfAttributes(index) > 1)
		sz = BarkMaxItemSize;
	return MAXALIGN(sz) + sizeof(ItemIdData);
}

/* qsort comparator: closest to the target first, then in key order. */
static int
bark_split_cmp(const void *arg1, const void *arg2)
{
	const BarkSplitPoint *split1 = arg1;
	const BarkSplitPoint *split2 = arg2;

	if (split1->delta != split2->delta)
		return pg_cmp_s32(split1->delta, split2->delta);
	return pg_cmp_s32(split1->firstright, split2->firstright);
}

/*
 * Sort split points by their distance from a split that leaves the left page
 * `fillfactor` percent full.  Weighting each side's free space by the other
 * side's share makes the distance zero when leftfree : rightfree is
 * (100 - fillfactor) : fillfactor, which for the split of a full page leaves
 * about (100 - fillfactor)% of the left page free.  A fillfactor of 50 is an
 * even division of free space.  As nbtree's _bt_deltasortsplits.
 */
static void
bark_split_sort(BarkSplitPoint *splits, int nsplits, int fillfactor)
{
	for (int i = 0; i < nsplits; i++)
		splits[i].delta = abs(fillfactor * splits[i].leftfree -
							  (100 - fillfactor) * splits[i].rightfree);
	qsort(splits, nsplits, sizeof(BarkSplitPoint), bark_split_cmp);
}

/*
 * Number of split points, from the start of the delta-sorted array, whose
 * free space on each side is within tolerance of the first (best-balanced)
 * one.  A port of nbtree's _bt_defaultinterval.
 */
static int
bark_split_interval(BarkSplitPoint *splits, int nsplits, int databytes)
{
	int			tolerance = databytes * BARK_LEAF_SPLIT_DISTANCE;
	BarkSplitPoint *best = &splits[0];

	for (int i = 1; i < nsplits; i++)
	{
		BarkSplitPoint *split = &splits[i];

		if (split->leftfree < best->leftfree - tolerance ||
			split->leftfree > best->leftfree + tolerance ||
			split->rightfree < best->rightfree - tolerance ||
			split->rightfree > best->rightfree + tolerance)
			return i;
	}
	return nsplits;
}

/*
 * Choose where to split a full page.
 *
 * `items` holds the page's n data items in key order, the incoming item
 * among them at `newitemidx`.  `orighikey` is the page's high key, which the
 * new right page keeps; NULL when the page is the rightmost on its level.
 * The left page gets a new high key formed from the right page's first item.
 *
 * Returns the index of the first item that goes to the right page, in
 * [1, n-1].
 */
int
bark_findsplitloc(Relation index, BarkKeyInfo *keyinfo, IndexTuple *items,
				  int n, int newitemidx, bool isleaf, IndexTuple orighikey)
{
	bool		rightmost = (orighikey == NULL);
	int			nkeyatts = IndexRelationGetNumberOfKeyAttributes(index);
	int			leftspace;
	int			rightspace;
	int			totalbytes = 0;
	int			leftbytes = 0;
	int			fillfactor;
	BarkSplitPoint *splits;
	int			nsplits = 0;
	int			pagelow;
	int			pagehigh;
	int			interval;
	int			low;
	int			high;
	int			perfectpenalty;
	bool		manyduplicates = false;
	int			bestpenalty = INT_MAX;
	int			best = 0;
	int			firstright;

	Assert(n >= 2);
	Assert(newitemidx >= 0 && newitemidx < n);

	/* Room for items on an empty page; the right page keeps the high key. */
	leftspace = rightspace = BLCKSZ - SizeOfPageHeaderData -
		MAXALIGN(sizeof(BarkPageOpaqueData));
	if (!rightmost)
		rightspace -= bark_split_itemsz(orighikey);

	for (int i = 0; i < n; i++)
		totalbytes += bark_split_itemsz(items[i]);

	/*
	 * Record every split point at which both halves fit, in key order.  The
	 * left page also holds its new high key, sized pessimistically from the
	 * right page's first item.
	 */
	splits = palloc_array(BarkSplitPoint, n - 1);
	for (int i = 1; i < n; i++)
	{
		int			leftfree;
		int			rightfree;

		leftbytes += bark_split_itemsz(items[i - 1]);
		leftfree = leftspace - leftbytes - bark_split_hikeysz(index, items[i]);
		rightfree = rightspace - (totalbytes - leftbytes);
		if (leftfree < 0 || rightfree < 0)
			continue;

		splits[nsplits].firstright = i;
		splits[nsplits].leftfree = leftfree;
		splits[nsplits].rightfree = rightfree;
		nsplits++;
	}

	/* Items are at most a third of a page, so this should not happen. */
	if (nsplits == 0)
		elog(ERROR, "could not find a feasible split point for BARK index \"%s\"",
			 RelationGetRelationName(index));

	/* The page's extreme split points, before sorting loses key order. */
	pagelow = splits[0].firstright;
	pagehigh = splits[nsplits - 1].firstright;

	/*
	 * The target: the left page fillfactor% full on the rightmost page of a
	 * level, an even division of free space elsewhere.
	 */
	if (!rightmost)
		fillfactor = 50;
	else if (isleaf)
		fillfactor = BarkGetFillFactor(index);
	else
		fillfactor = BARK_NONLEAF_FILLFACTOR;
	bark_split_sort(splits, nsplits, fillfactor);

	/* An internal page has no penalty to weigh: take the closest point. */
	if (!isleaf)
	{
		firstright = splits[0].firstright;
		pfree(splits);
		return firstright;
	}

	/*
	 * Every split point in the interval lies between its lowest and highest,
	 * so the penalty of splitting between those two is the least any of them
	 * can have.  The penalty is the number of attributes the high key keeps
	 * (bark_keep_natts); nkeyatts + 1 means the two items are equal on all
	 * of them, so the interval is inside one run of equal keys, and the
	 * strategy changes, as in nbtree's _bt_strategy.
	 */
	interval = bark_split_interval(splits, nsplits, totalbytes);
	low = high = splits[0].firstright;
	for (int i = 1; i < interval; i++)
	{
		low = Min(low, splits[i].firstright);
		high = Max(high, splits[i].firstright);
	}
	perfectpenalty = bark_keep_natts(index, keyinfo,
									 items[low - 1], items[high]);
	if (perfectpenalty > nkeyatts)
	{
		if (bark_keep_natts(index, keyinfo, items[pagelow - 1],
							items[pagehigh]) <= nkeyatts)
		{
			/*
			 * The page is not one run: consider every split point and take
			 * the one closest to the target that is not inside a run.
			 */
			manyduplicates = true;
			interval = nsplits;
			perfectpenalty = nkeyatts;
		}
		else if (rightmost ||
				 bark_compare_itups(keyinfo, index, orighikey,
									items[newitemidx]) != 0)
		{
			/*
			 * The whole page is one key, and the page is the last one that
			 * can hold it: the high key is a different key, or there is none.
			 * Leave the left page nearly full, since it will receive no more
			 * inserts (see BARK_SINGLEVAL_FILLFACTOR).  When the high key
			 * equals the key, the run continues on the right sibling, inserts
			 * of the key go there, and the default split stands.
			 */
			bark_split_sort(splits, nsplits, BARK_SINGLEVAL_FILLFACTOR);
			interval = 1;
		}
	}

	/* The lowest penalty wins; ties go to the point closest to the target. */
	for (int i = 0; i < interval; i++)
	{
		int			penalty = bark_keep_natts(index, keyinfo,
											  items[splits[i].firstright - 1],
											  items[splits[i].firstright]);

		if (penalty < bestpenalty)
		{
			bestpenalty = penalty;
			best = i;
		}
		if (penalty <= perfectpenalty)
			break;
	}

	/*
	 * Moving the split to the edge of a run can go wrong with descending
	 * inserts just right of a large run of duplicates: each split would put
	 * the new item first on a new right page that later inserts, all smaller,
	 * never reach, leaving a trail of nearly empty pages.  As nbtree does,
	 * split at the target instead when the new item would start the right
	 * page.
	 */
	if (manyduplicates && !rightmost &&
		splits[best].firstright == newitemidx)
		best = 0;

	firstright = splits[best].firstright;
	pfree(splits);
	return firstright;
}
