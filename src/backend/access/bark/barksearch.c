/*-------------------------------------------------------------------------
 *
 * barksearch.c
 *	  Tree descent for the BARK index access method.
 *
 * bark_search descends from the root (named by the meta page) to the leaf
 * that should hold a given key, following right links when a page has split
 * since its parent downlink was written -- the Lehman & Yao move-right rule,
 * detected by comparing the key against each page's high key.  It records the
 * path in a BarkStack so an insert that splits the leaf can insert the new
 * downlinks on the way back up.
 *
 * This is modeled on nbtsearch.c's _bt_search / _bt_moveright / _bt_binsrch.
 * BARK compares with bark_compare_itups (the opclass's support-1 comparator)
 * rather than a btree scankey, because the "search key" during an insert is
 * itself an index tuple.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/bark/barksearch.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bark.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "utils/rel.h"

/* Compare search key against the index tuple at offset `off` on `page`. */
static int
bark_compare_off(Relation index, BarkKeyInfo *keyinfo, IndexTuple key,
				 Page page, OffsetNumber off)
{
	ItemId		iid = PageGetItemId(page, off);
	IndexTuple	itup = (IndexTuple) PageGetItem(page, iid);

	return bark_compare_itups(keyinfo, index, key, itup);
}

/*
 * Should the search move right off this page?  True when the page is not
 * rightmost and the key is greater than the page's high key, meaning the page
 * split after our parent pointed here and the key now lives further right.
 */
static bool
bark_should_move_right(Relation index, BarkKeyInfo *keyinfo, IndexTuple key,
					   Page page)
{
	BarkPageOpaque opaque = BarkPageGetOpaque(page);

	if (BarkPageRightmost(opaque))
		return false;
	/* High key is at BARK_P_HIKEY; move right if key > high key. */
	return bark_compare_off(index, keyinfo, key, page, BARK_P_HIKEY) > 0;
}

/*
 * On an internal page, find the offset of the downlink to follow for `key`.
 * On a leaf page, find the offset at which `key` should be inserted (the first
 * entry whose key is > the search key).
 *
 * `nextkey` selects which child a run of equal keys routes to on an internal
 * page.  With nextkey=true (the insert / true-key descent) we follow the last
 * downlink whose key is <= the search key -- the rightmost child that can hold
 * the key.  With nextkey=false (a lower-bound scan descent) we follow the last
 * downlink whose key is strictly < the search key -- the leftmost child that
 * can hold the key.  The distinction matters only when equal downlink keys
 * exist, which happens when a leaf splits in the middle of a run of equal
 * keys: a forward equality or lower-bound scan must then start at the FIRST
 * such leaf, or it silently skips the earlier duplicates.  (Each leaf's high
 * key equals the next leaf's first key, so an earlier leaf of the run still
 * holds matching keys; landing on the last leaf of the run loses them.)
 */
static OffsetNumber
bark_binsrch(Relation index, BarkKeyInfo *keyinfo, IndexTuple key, Page page,
			 bool nextkey, bool *leaf_out)
{
	BarkPageOpaque opaque = BarkPageGetOpaque(page);
	OffsetNumber low = BarkPageFirstDataKey(opaque);
	OffsetNumber high = PageGetMaxOffsetNumber(page);
	bool		isleaf = BarkPageIsLeaf(opaque);

	if (leaf_out)
		*leaf_out = isleaf;

	if (high < low)
		return low;				/* empty page */

	/*
	 * Binary search for the first offset whose key is > the search key
	 * (nextkey=true) or >= the search key (nextkey=false).  Invariant:
	 * everything below `low` is on the near side of that boundary.
	 */
	high = OffsetNumberNext(high);	/* high is now one past the last item */
	while (low < high)
	{
		OffsetNumber mid = low + ((high - low) / 2);
		int			cmp = bark_compare_off(index, keyinfo, key, page, mid);

		if (nextkey ? (cmp >= 0) : (cmp > 0))
			low = OffsetNumberNext(mid);	/* mid is before the boundary */
		else
			high = mid;			/* mid is at or past the boundary */
	}

	/*
	 * `low` is the boundary offset.  For a leaf that is the insert position.
	 * For an internal page the downlink to descend is the one just before it;
	 * clamp to the first data key when the search key precedes every entry.
	 */
	if (isleaf)
		return low;
	if (low <= BarkPageFirstDataKey(opaque))
		return BarkPageFirstDataKey(opaque);
	return OffsetNumberPrev(low);
}

/*
 * Read the root block number from the meta page.  Returns BARK_P_NONE when
 * the index is empty (no root yet).
 */
static BlockNumber
bark_get_root(Relation index, uint32 *level_out)
{
	Buffer		metabuf = ReadBuffer(index, BARK_METAPAGE);
	BarkMetaPageData *meta;
	BlockNumber root;

	LockBuffer(metabuf, BUFFER_LOCK_SHARE);
	meta = BarkPageGetMeta(BufferGetPage(metabuf));
	root = meta->bark_root;
	if (level_out)
		*level_out = meta->bark_level;
	UnlockReleaseBuffer(metabuf);
	return root;
}

/*
 * Descend to the leaf that should contain `key`.
 *
 * Returns the leaf buffer, locked for write when forwrite (else share), after
 * following any right links needed to reach the correct page.  When stack is
 * non-NULL it is set to the parent path (caller frees with bark_freestack);
 * it is NULL for a one-level tree (root is the leaf) or an empty index.
 *
 * Returns InvalidBuffer when the index has no root yet (empty index); the
 * caller creates the first leaf.
 */
Buffer
bark_search(Relation index, BarkKeyInfo *keyinfo, IndexTuple key,
			bool forwrite, bool nextkey, BarkStack *stack)
{
	BlockNumber blkno;
	Buffer		buf;
	BarkStack	path = NULL;

	if (stack)
		*stack = NULL;

	blkno = bark_get_root(index, NULL);
	if (blkno == BARK_P_NONE)
		return InvalidBuffer;	/* empty index */

	/* Descend internal levels with share locks, recording the stack. */
	for (;;)
	{
		Page		page;
		BarkPageOpaque opaque;
		OffsetNumber off;
		bool		isleaf;

		buf = ReadBuffer(index, blkno);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);
		opaque = BarkPageGetOpaque(page);

		/* Move right if a split put the key beyond this page. */
		if (bark_should_move_right(index, keyinfo, key, page))
		{
			BlockNumber right = opaque->bark_next;

			UnlockReleaseBuffer(buf);
			blkno = right;
			continue;
		}

		if (BarkPageIsLeaf(opaque))
		{
			/*
			 * Reached the target leaf.  Re-lock for write if asked: drop the
			 * share lock and take an exclusive one, then re-check for a split
			 * that may have happened in between by moving right as needed.
			 */
			if (forwrite)
			{
				LockBuffer(buf, BUFFER_LOCK_UNLOCK);
				LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
				page = BufferGetPage(buf);
				while (bark_should_move_right(index, keyinfo, key, page))
				{
					BlockNumber right = BarkPageGetOpaque(page)->bark_next;

					LockBuffer(buf, BUFFER_LOCK_UNLOCK);
					buf = ReleaseAndReadBuffer(buf, index, right);
					LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
					page = BufferGetPage(buf);
				}
			}
			if (stack)
				*stack = path;
			else
				bark_freestack(path);
			return buf;
		}

		/* Internal page: find the downlink to follow and push the stack. */
		off = bark_binsrch(index, keyinfo, key, page, nextkey, &isleaf);
		{
			ItemId		iid = PageGetItemId(page, off);
			IndexTuple	itup = (IndexTuple) PageGetItem(page, iid);
			BlockNumber child = BarkEntryGetDownLink(itup);
			BarkStack	item = palloc(sizeof(BarkStackData));

			item->bark_blkno = blkno;
			item->bark_offset = off;
			item->bark_parent = path;
			path = item;

			blkno = child;
			UnlockReleaseBuffer(buf);
		}
	}
}

void
bark_freestack(BarkStack stack)
{
	while (stack != NULL)
	{
		BarkStack	parent = stack->bark_parent;

		pfree(stack);
		stack = parent;
	}
}
