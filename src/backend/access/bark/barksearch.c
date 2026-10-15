/*-------------------------------------------------------------------------
 *
 * barksearch.c
 *	  Tree descent for the BARK index access method.
 *
 * bark_search descends from the root (named by the meta page) to the leaf
 * that should hold a given key, following right links when a page has split
 * since its parent downlink was written -- the Lehman & Yao move-right rule,
 * detected by comparing the key against each page's high key -- and past
 * pages that are being removed from the tree.  A descent for an insert also
 * finishes any incomplete split it meets on the way.  It records the path in
 * a BarkStack so an insert that splits the leaf can insert the new downlinks
 * on the way back up.
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
#include "utils/injection_point.h"
#include "utils/rel.h"

/*
 * What a descent searches for: an index tuple (insert, and the internal
 * lookups of VACUUM and split completion), or a scan's bound, whose
 * arguments may be of another type in the column's opfamily.  An index tuple
 * may come with a heap TID, scantid, which orders it within a run of equal
 * keys (bark_compare_itups_tid); NULL compares the key only, as every scan
 * does.
 */
typedef struct BarkSearchKey
{
	IndexTuple	key;
	ItemPointer scantid;
	const BarkScanBound *bound;
} BarkSearchKey;

/*
 * Compare a scan bound with an index tuple: <0 or >0 as the bound sorts
 * before or after the tuple in index order (never 0, see below).  Truncated
 * pivot attributes are minus infinity, as in bark_compare_itups.
 */
int
bark_compare_bound(Relation index, BarkKeyInfo *keyinfo,
				   const BarkScanBound *bound, IndexTuple itup)
{
	TupleDesc	tupdesc = RelationGetDescr(index);
	IndexTuple	full = NULL;
	int			natts;
	int			ncmp;
	bool		notid = false;
	int			result = 0;

	/* natts, and whether the tuple is a pivot whose heap TID was truncated. */
	if (BarkEntryGetShape(itup) == BARK_SHAPE_OVERSIZED)
	{
		if (BarkOverflowIsLeaf(itup))
			natts = keyinfo->nkeys;
		else
		{
			natts = BarkOverflowGetRef(itup)->natts;
			notid = bark_pivot_heap_tid(itup) == NULL;
		}
		itup = full = bark_fetch_oversized(index, itup);
	}
	else if (BarkEntryGetShape(itup) == BARK_SHAPE_PIVOT)
	{
		natts = BarkPivotGetNAtts(itup);
		notid = BarkPivotGetHeapTID(itup) == NULL;
	}
	else
		natts = keyinfo->nkeys;

	ncmp = Min(natts, bound->nkeys);
	for (int i = 0; i < ncmp; i++)
	{
		BarkKeyColumn *col = &keyinfo->cols[i];
		bool		isnull;
		Datum		datum = index_getattr(itup, i + 1, tupdesc, &isnull);
		int32		cmp;

		if (bound->procs[i] == NULL)
		{
			/* A NULL bound, for a multikey scan of the NULL entries. */
			if (isnull)
				continue;
			result = col->nulls_first ? -1 : 1;
			break;
		}
		if (isnull)
		{
			/* A NULL entry sorts per NULLS FIRST/LAST. */
			result = col->nulls_first ? 1 : -1;
			break;
		}

		/* The ORDER proc compares the entry with the argument, in value order. */
		cmp = DatumGetInt32(FunctionCall2Coll(bound->procs[i],
											  bound->collations[i],
											  datum, bound->args[i]));
		if (cmp != 0)
		{
			result = col->reverse ? cmp : -cmp;
			break;
		}
	}

	/*
	 * Equal on every column both have.  A pivot truncated below the bound's
	 * columns is minus infinity past its own, so the bound sorts after it.
	 * Otherwise a lower bound sorts just before every tuple equal to it, and
	 * an upper bound just after: a descent for a lower bound then stops at
	 * the first page that can hold an equal key, and one for an upper bound
	 * moves on to the last such page, past a high key equal to the bound.
	 * A bound is never equal to a tuple, so nextkey does not matter.
	 *
	 * One exception, as in nbtree's _bt_compare: a forward scan's lower
	 * bound sorts after a pivot that has exactly the bound's columns and no
	 * heap TID.  Suffix truncation kept one column more than lastleft and
	 * firstright share, so lastleft is strictly less than the pivot on those
	 * columns, and so is every entry left of the pivot (an insert equal to
	 * the pivot on them goes right of it).  No entry there can be at or after
	 * the bound, and a descent that went left would only read a page with no
	 * match and step right.  A pivot with more columns than the bound, or
	 * with a heap TID, may have entries equal to the bound on its left.  A
	 * backward scan's lower bound (a strict <) wants the entries left of the
	 * pivot, so it keeps sorting before it, as nbtree's does.  Leaf entries
	 * are never truncated, and an upper bound sorts after the pivot anyway.
	 */
	if (result == 0)
	{
		if (natts < bound->nkeys || bound->upper ||
			(notid && natts == bound->nkeys && !bound->backward))
			result = 1;
		else
			result = -1;
	}

	if (full)
		pfree(full);
	return result;
}

/*
 * Compare the search key with index tuple itup.  The caller reads the tuple
 * (a high key with PageGetItem, a data item with BarkPageGetItem into a
 * workspace it keeps for the whole page), so that this is cheap enough to
 * inline into the binary search, which calls it once per probe.
 */
static inline int
bark_compare_key(Relation index, BarkKeyInfo *keyinfo,
				 const BarkSearchKey *key, IndexTuple itup)
{
	if (key->bound != NULL)
		return bark_compare_bound(index, keyinfo, key->bound, itup);
	return bark_compare_itups_tid(keyinfo, index, key->key, key->scantid,
								  itup);
}

/*
 * Move right from `buf`, as far as needed, to the page at this level that can
 * hold `key`, and return it locked in mode `access`.  A port of nbtree's
 * _bt_moveright.
 *
 * Move right past a page that split after our parent pointed at it (the key
 * is greater than its high key, or, with a heap TID, not less than it: a high
 * key's heap TID is the lowest of the right page's first entry, so an equal
 * TID belongs right) and past a page that is being removed from
 * the tree (BARK_DELETED or BARK_HALF_DEAD; such a page keeps its right link
 * so that descents and scans already on their way to it can step past it).
 *
 * A write descent (forwrite) also finishes any incomplete split it finds on
 * the way, holding the page's exclusive lock while it does, and `stack` is
 * the path to this level's parent that bark_finish_split needs.  This is what
 * guarantees that an abandoned split gets its downlink even when every later
 * insert lands to the right of its left half: if those inserts merely moved
 * right, the right half would have no downlink of its own when it split in
 * turn.  The flag is only ever seen on a split nobody is completing, since a
 * backend that is in the middle of a split holds the left page's lock until
 * the downlink is written.
 */
static Buffer
bark_moveright(Relation index, BarkKeyInfo *keyinfo, const BarkSearchKey *key,
			   Buffer buf, bool forwrite, BarkStack stack,
			   BufferLockMode access)
{
	Page		page;
	BarkPageOpaque opaque;

	for (;;)
	{
		page = BufferGetPage(buf);
		opaque = BarkPageGetOpaque(page);

		if (BarkPageRightmost(opaque))
			break;

		if (forwrite && (opaque->bark_flags & BARK_INCOMPLETE_SPLIT) != 0)
		{
			BlockNumber blkno = BufferGetBlockNumber(buf);

			if (access == BUFFER_LOCK_SHARE)
			{
				LockBuffer(buf, BUFFER_LOCK_UNLOCK);
				LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
			}
			if ((opaque->bark_flags & BARK_INCOMPLETE_SPLIT) != 0)
				bark_finish_split(index, keyinfo, buf, stack);	/* releases buf */
			else
				UnlockReleaseBuffer(buf);

			/* Re-read the page in the caller's lock mode and look again. */
			buf = ReadBuffer(index, blkno);
			LockBuffer(buf, access);
			continue;
		}

		/* A page being removed may have no high key: test that first. */
		if ((opaque->bark_flags & (BARK_DELETED | BARK_HALF_DEAD)) != 0 ||
			bark_compare_key(index, keyinfo, key,
							 (IndexTuple) PageGetItem(page,
													  PageGetItemId(page, BARK_P_HIKEY))) >=
			(key->scantid != NULL ? 0 : 1))
		{
			BlockNumber right = opaque->bark_next;

			LockBuffer(buf, BUFFER_LOCK_UNLOCK);
			buf = ReleaseAndReadBuffer(buf, index, right);
			LockBuffer(buf, access);
			continue;
		}
		break;
	}

	/*
	 * A page being removed is never the rightmost page of its level while it
	 * is still linked, so reaching one here means the level ended under us.
	 */
	if ((opaque->bark_flags & (BARK_DELETED | BARK_HALF_DEAD)) != 0)
		elog(ERROR, "fell off the end of BARK index \"%s\"",
			 RelationGetRelationName(index));

	return buf;
}

/*
 * On an internal page, find the offset of the downlink to follow for `key`.
 * On a leaf page, find the offset at which `key` should be inserted (the first
 * entry whose key is > the search key).
 *
 * `nextkey` selects which child a run of equal keys routes to on an internal
 * page.  With nextkey=true we follow the last downlink whose key is <= the
 * search key -- the rightmost child that can hold the key.  With
 * nextkey=false (a lower-bound scan descent, and a unique check's) we follow
 * the last downlink whose key is strictly < the search key -- the leftmost
 * child that can hold the key.  The distinction matters only when equal
 * downlink keys exist, which happens when a leaf splits in the middle of a
 * run of equal keys: a forward equality or lower-bound scan must then start
 * at the FIRST such leaf, or it silently skips the earlier duplicates.  An
 * insert's search key has a heap TID (scantid), which a downlink with every
 * key attribute is never equal to unless their TIDs are equal, and then the
 * entry belongs right of it: so an insert uses nextkey=true.
 */
static OffsetNumber
bark_binsrch(Relation index, BarkKeyInfo *keyinfo, const BarkSearchKey *key,
			 Page page, bool nextkey)
{
	BarkPageOpaque opaque = BarkPageGetOpaque(page);
	OffsetNumber low = BarkPageFirstDataKey(opaque);
	OffsetNumber high = PageGetMaxOffsetNumber(page);
	bool		isleaf = BarkPageIsLeaf(opaque);
	BarkItemBuf ibuf;

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
		int			cmp = bark_compare_key(index, keyinfo, key,
										   BarkPageGetItem(page, mid, &ibuf));

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
 * Return the index's BarkAmCache, creating it empty on first use.
 */
BarkAmCache *
bark_get_amcache(Relation index)
{
	BarkAmCache *cache = (BarkAmCache *) index->rd_amcache;

	if (cache == NULL)
	{
		cache = MemoryContextAlloc(index->rd_indexcxt,
								   offsetof(BarkAmCache, markerdata));
		cache->root = BARK_P_NONE;
		cache->level = 0;
		cache->extracted = -1;
		cache->multikey = false;
		cache->nmarkers = 0;
		cache->markerlen = 0;
		cache->nl1 = cache->maxl1 = 0;
		cache->l1keylen = cache->maxl1keylen = 0;
		index->rd_amcache = cache;
	}
	return cache;
}

/*
 * The level-1 entry table (see BarkAmCache and "Level-1 entry cache" in the
 * README).  Its slots and keys sit after the markers in the cache's chunk:
 * slots first, at a MAXALIGNed offset, then the keys.
 */
#define BARK_L1_MAX_SLOTS	2048
#define BARK_L1_MAX_KEYLEN	(256 * 1024)

static inline BarkL1Slot *
bark_l1_slots(BarkAmCache *cache)
{
	return (BarkL1Slot *) (cache->markerdata + MAXALIGN(cache->markerlen));
}

static inline char *
bark_l1_keys(BarkAmCache *cache)
{
	return (char *) bark_l1_slots(cache) +
		MAXALIGN(sizeof(BarkL1Slot) * cache->maxl1);
}

static inline IndexTuple
bark_l1_key(BarkAmCache *cache, int i)
{
	return (IndexTuple) (bark_l1_keys(cache) + bark_l1_slots(cache)[i].keyoff);
}

/* Forget the level-1 table (the chunk keeps its size until it next grows). */
void
bark_l1_reset(BarkAmCache *cache)
{
	cache->nl1 = cache->maxl1 = 0;
	cache->l1keylen = cache->maxl1keylen = 0;
}

/*
 * The bytes the level-1 table takes after the markers (its slots and keys,
 * as allocated), for bark_cache_marker, which moves the table when a marker
 * is added before it.
 */
Size
bark_l1_region_size(BarkAmCache *cache)
{
	if (cache->maxl1 == 0)
		return 0;
	return MAXALIGN(sizeof(BarkL1Slot) * cache->maxl1) + cache->maxl1keylen;
}

/*
 * Make room in a full level-1 table by forgetting half its slots: every
 * other one, so the table still covers the whole key range at half the
 * density, and the descents that miss refill the gaps.  The surviving
 * keys are packed down to the start of the key space.
 */
static void
bark_l1_thin(BarkAmCache *cache)
{
	BarkL1Slot *slots = bark_l1_slots(cache);
	char	   *keys = bark_l1_keys(cache);
	char	   *packed = palloc(cache->l1keylen);
	int			n = 0;
	Size		keylen = 0;

	/*
	 * Slots are in key order but keys sit in the order they were added, so
	 * copy the survivors out and back rather than pack them in place.
	 */
	for (int i = 0; i < cache->nl1; i += 2)
	{
		IndexTuple	key = (IndexTuple) (keys + slots[i].keyoff);

		memcpy(packed + keylen, key, IndexTupleSize(key));
		slots[n].blkno = slots[i].blkno;
		slots[n].keyoff = keylen;
		keylen += MAXALIGN(IndexTupleSize(key));
		n++;
	}
	memcpy(keys, packed, keylen);
	pfree(packed);
	cache->nl1 = n;
	cache->l1keylen = keylen;
}

/*
 * The slot a read descent for key may start at: the one with the greatest
 * low key that the key does not sort before (nextkey) or after (!nextkey),
 * by the same rule bark_binsrch uses to pick a downlink, so that starting
 * there and moving right reaches the page the root would have led to.
 * Returns -1 when no slot qualifies.
 */
static int
bark_l1_find(Relation index, BarkKeyInfo *keyinfo, BarkAmCache *cache,
			 const BarkSearchKey *key, bool nextkey)
{
	int			low = 0;
	int			high = cache->nl1;

	/* The first slot whose low key the search key is not past. */
	while (low < high)
	{
		int			mid = low + (high - low) / 2;
		int			cmp = bark_compare_key(index, keyinfo, key,
										   bark_l1_key(cache, mid));

		if (nextkey ? (cmp >= 0) : (cmp > 0))
			low = mid + 1;
		else
			high = mid;
	}
	return low - 1;
}

/*
 * Remember that level-1 page blkno has low key lowkey, a pivot read from the
 * downlink that named it, under the parent's lock.  The table is kept
 * sorted; an entry for a block already present is left alone (its low key
 * cannot have changed), and when the table is full it is thinned to half
 * (bark_l1_thin), so that a long-lived backend follows a tree that grows.
 */
static void
bark_l1_remember(Relation index, BarkKeyInfo *keyinfo, BlockNumber blkno,
				 IndexTuple lowkey)
{
	BarkAmCache *cache = bark_get_amcache(index);
	BarkSearchKey skey = {lowkey, NULL, NULL};
	Size		keysz = MAXALIGN(IndexTupleSize(lowkey));
	int			pos;
	BarkL1Slot *slots;

	/* Where it goes: after every slot whose low key is <= lowkey. */
	pos = bark_l1_find(index, keyinfo, cache, &skey, true) + 1;
	if (pos > 0 && bark_l1_slots(cache)[pos - 1].blkno == blkno)
		return;

	if (cache->nl1 >= BARK_L1_MAX_SLOTS ||
		cache->l1keylen + keysz > BARK_L1_MAX_KEYLEN)
	{
		bark_l1_thin(cache);
		/* The new key's place in the thinned table. */
		pos = bark_l1_find(index, keyinfo, cache, &skey, true) + 1;
		if (pos > 0 && bark_l1_slots(cache)[pos - 1].blkno == blkno)
			return;
		/* A single key larger than the whole budget is not cached. */
		if (cache->l1keylen + keysz > BARK_L1_MAX_KEYLEN)
			return;
	}

	/* Grow the chunk: double the slots or the key space as needed. */
	if (cache->nl1 >= cache->maxl1 || cache->l1keylen + keysz > cache->maxl1keylen)
	{
		int			maxl1 = Max(cache->maxl1, 16);
		Size		maxkey = Max(cache->maxl1keylen, 1024);
		char	   *oldkeys = cache->nl1 > 0 ? palloc(cache->l1keylen) : NULL;
		BarkL1Slot *oldslots = cache->nl1 > 0 ?
			palloc(sizeof(BarkL1Slot) * cache->nl1) : NULL;
		int			n = cache->nl1;
		Size		klen = cache->l1keylen;

		while (maxl1 <= cache->nl1)
			maxl1 *= 2;
		while (maxkey < cache->l1keylen + keysz)
			maxkey *= 2;
		if (n > 0)
		{
			memcpy(oldkeys, bark_l1_keys(cache), klen);
			memcpy(oldslots, bark_l1_slots(cache), sizeof(BarkL1Slot) * n);
		}
		cache = repalloc(cache, offsetof(BarkAmCache, markerdata) +
						 MAXALIGN(cache->markerlen) +
						 MAXALIGN(sizeof(BarkL1Slot) * maxl1) + maxkey);
		index->rd_amcache = cache;
		cache->maxl1 = maxl1;
		cache->maxl1keylen = maxkey;
		if (n > 0)
		{
			memcpy(bark_l1_slots(cache), oldslots, sizeof(BarkL1Slot) * n);
			memcpy(bark_l1_keys(cache), oldkeys, klen);
			pfree(oldslots);
			pfree(oldkeys);
		}
	}

	slots = bark_l1_slots(cache);
	memmove(&slots[pos + 1], &slots[pos], sizeof(BarkL1Slot) * (cache->nl1 - pos));
	slots[pos].blkno = blkno;
	slots[pos].keyoff = cache->l1keylen;
	memcpy(bark_l1_keys(cache) + cache->l1keylen, lowkey, IndexTupleSize(lowkey));
	cache->l1keylen += keysz;
	cache->nl1++;
}

/*
 * Read the root block number, and the root's level when level_out is not
 * NULL, from the meta page, and refresh the root cache.  Returns BARK_P_NONE
 * when the index is empty (no root yet).
 *
 * Every descent reaches the meta page here when the root cache is empty, so
 * this is where an index in an on-disk format older than BARK_MIN_VERSION is
 * refused.  The meta page is read under its lock before the error, which
 * leaves the cache unset, so every later access is refused too.
 */
BlockNumber
bark_get_root(Relation index, uint32 *level_out)
{
	Buffer		metabuf = ReadBuffer(index, BARK_METAPAGE);
	BarkMetaPageData *meta;
	BlockNumber root;
	uint32		level;

	LockBuffer(metabuf, BUFFER_LOCK_SHARE);
	meta = BarkPageGetMeta(BufferGetPage(metabuf));
	if (meta->bark_version < BARK_MIN_VERSION)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("index \"%s\" was built by an older BARK version",
						RelationGetRelationName(index)),
				 errhint("REINDEX the index.")));
	root = meta->bark_root;
	level = meta->bark_level;
	UnlockReleaseBuffer(metabuf);

	if (root != BARK_P_NONE)
	{
		BarkAmCache *cache = bark_get_amcache(index);

		cache->root = root;
		cache->level = level;
	}
	if (level_out)
		*level_out = level;
	return root;
}

/*
 * The root's level, for the planner: from the root cache when there is one,
 * else from the meta page.  Zero for a single-page or empty index.
 */
uint32
bark_get_root_level(Relation index)
{
	uint32		level;

	BarkAmCache *cache = (BarkAmCache *) index->rd_amcache;

	if (cache != NULL && cache->root != BARK_P_NONE)
		return cache->level;
	(void) bark_get_root(index, &level);
	return level;
}

/*
 * Return the root page locked in mode access, or InvalidBuffer when the
 * index is empty.
 *
 * The cached root is used when, under the lock, it is still the root: the
 * BARK_ROOT flag is set, the page is not being deleted, and it is at the
 * cached level.  A root split clears the flag on the old root before the new
 * root is made, so a page that passes is the root the meta page names, or
 * the one it is about to name.  Otherwise the meta page is read, as it
 * always was before the cache; the root it names may itself have split
 * since, which the caller's move right handles.
 */
Buffer
bark_get_root_buffer(Relation index, BufferLockMode access)
{
	BarkAmCache *cache = (BarkAmCache *) index->rd_amcache;
	BlockNumber root;
	Buffer		buf;

	if (cache != NULL && cache->root != BARK_P_NONE)
	{
		Page		page;

		buf = ReadBuffer(index, cache->root);
		LockBuffer(buf, access);
		page = BufferGetPage(buf);
		if (!PageIsNew(page))
		{
			BarkPageOpaque opaque = BarkPageGetOpaque(page);

			if (BarkPageIsRoot(opaque) && !BarkPageIgnore(opaque) &&
				opaque->bark_level == cache->level)
				return buf;
		}
		UnlockReleaseBuffer(buf);
	}

	root = bark_get_root(index, NULL);
	if (root == BARK_P_NONE)
		return InvalidBuffer;
	buf = ReadBuffer(index, root);
	LockBuffer(buf, access);
	return buf;
}

/*
 * The cached level-1 page a read descent for key may start at, share-locked,
 * or InvalidBuffer when there is none (the table is empty, or the key sorts
 * before every cached low key).  The page is checked under its lock to be a
 * live level-1 internal page; the caller then moves right from it as from
 * any page it reached through a downlink, so a split since it was cached is
 * handled the same way.
 */
static Buffer
bark_l1_start(Relation index, BarkKeyInfo *keyinfo, const BarkSearchKey *key,
			  bool nextkey)
{
	BarkAmCache *cache = (BarkAmCache *) index->rd_amcache;
	int			slot;
	Buffer		buf;
	Page		page;

	if (cache == NULL || cache->nl1 == 0)
		return InvalidBuffer;
	slot = bark_l1_find(index, keyinfo, cache, key, nextkey);
	if (slot < 0)
		return InvalidBuffer;

	buf = ReadBuffer(index, bark_l1_slots(cache)[slot].blkno);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);
	if (!PageIsNew(page))
	{
		BarkPageOpaque opaque = BarkPageGetOpaque(page);

		/*
		 * Tests make the check fail (bark-l1-stale) to exercise the fallback
		 * below, which the invariants otherwise keep unreachable.
		 */
		if (!BarkPageIsLeaf(opaque) && !BarkPageIgnore(opaque) &&
			opaque->bark_level == 1 &&
			!IS_INJECTION_POINT_ATTACHED("bark-l1-stale"))
		{
			/*
			 * The slot is the nearest cached page at or left of the key's,
			 * not necessarily the key's: the pages between may not be in
			 * the table yet.  Use it only when the key is within it (not
			 * past its high key, by bark_moveright's rule); otherwise moving
			 * right would lock page after page, each a buffer header every
			 * descent writes, which is what starting below the root avoids.
			 * The root's descent then records the key's page.
			 */
			if (BarkPageRightmost(opaque) ||
				bark_compare_key(index, keyinfo, key,
								 (IndexTuple) PageGetItem(page,
														  PageGetItemId(page, BARK_P_HIKEY))) <
				(key->scantid != NULL ? 0 : 1))
			{
				/* Tests watch which descents start below the root. */
				INJECTION_POINT("bark-l1-start", NULL);
				return buf;
			}
			UnlockReleaseBuffer(buf);
			return InvalidBuffer;
		}
	}

	/*
	 * Not a level-1 page any more (an internal page is never freed, so this
	 * means the index was rebuilt under a cache that survived, which a
	 * relcache rebuild should prevent): forget the table and use the root.
	 */
	UnlockReleaseBuffer(buf);
	bark_l1_reset(cache);
	return InvalidBuffer;
}

/*
 * Descend to the leaf that should contain `key`.
 *
 * Returns the leaf buffer, locked for write when forwrite (else share), after
 * following any right links needed to reach the correct page.  When stack is
 * non-NULL it is set to the parent path (caller frees with bark_freestack);
 * it is NULL for a one-level tree (root is the leaf) or an empty index.
 *
 * A write descent finishes every incomplete split it meets, at every level
 * (see bark_moveright), so the leaf it returns never carries
 * BARK_INCOMPLETE_SPLIT.  As in nbtree's _bt_search, internal pages are
 * share-locked and the leaf is locked exclusively straight from its parent,
 * except when the root is itself the leaf; then the share lock is traded for
 * an exclusive one and the page re-checked for a split made in between.
 *
 * Returns InvalidBuffer when the index has no root yet (empty index); the
 * caller creates the first leaf.
 */
static Buffer
bark_descend(Relation index, BarkKeyInfo *keyinfo, const BarkSearchKey *key,
			 bool forwrite, bool nextkey, BarkStack *stack)
{
	BlockNumber blkno;
	Buffer		buf;
	BarkStack	path = NULL;
	BufferLockMode access = BUFFER_LOCK_SHARE;

	if (stack)
		*stack = NULL;

	/*
	 * A read descent that needs no parent path starts at a cached level-1
	 * page when one may hold the key, and never touches the root, whose
	 * buffer header every descent would otherwise write (see "Level-1 entry
	 * cache" in the README).  A write descent, or one recording its path,
	 * starts at the root as before.
	 */
	buf = InvalidBuffer;
	if (!forwrite && stack == NULL)
		buf = bark_l1_start(index, keyinfo, key, nextkey);
	if (!BufferIsValid(buf))
		buf = bark_get_root_buffer(index, access);
	if (!BufferIsValid(buf))
		return InvalidBuffer;	/* empty index */

	for (;;)
	{
		Page		page;
		BarkPageOpaque opaque;
		OffsetNumber off;
		IndexTuple	itup;
		BarkStack	item;
		BarkItemBuf ibuf;

		/*
		 * The page may have split since we read its downlink (or the meta
		 * page), and a writer may have to finish an incomplete split here.
		 * `path` is this level's parent path, which is what finishing a split
		 * at this level needs.
		 */
		buf = bark_moveright(index, keyinfo, key, buf, forwrite, path, access);

		page = BufferGetPage(buf);
		opaque = BarkPageGetOpaque(page);
		if (BarkPageIsLeaf(opaque))
			break;

		/* Internal page: find the downlink to follow and push the stack. */
		off = bark_binsrch(index, keyinfo, key, page, nextkey);
		itup = BarkPageGetItem(page, off, &ibuf);

		item = palloc(sizeof(BarkStackData));
		item->bark_blkno = BufferGetBlockNumber(buf);
		item->bark_offset = off;
		item->bark_parent = path;
		path = item;

		/* The children of a level-1 page are leaves: lock them for write. */
		if (forwrite && opaque->bark_level == 1)
			access = BUFFER_LOCK_EXCLUSIVE;

		blkno = BarkEntryGetDownLink(itup);

		/*
		 * A level-2 page's downlink names a level-1 page and is its low key,
		 * fixed for the page's life: remember the pair for later read
		 * descents.  A page's first downlink is minus infinity on that page
		 * only (its true low key is the page's own), so it is skipped.
		 */
		if (opaque->bark_level == 2 && off > BarkPageFirstDataKey(opaque))
			bark_l1_remember(index, keyinfo, blkno, itup);
		LockBuffer(buf, BUFFER_LOCK_UNLOCK);

		/*
		 * Tests stop a descent here, holding a downlink it has read but not
		 * followed, so that VACUUM can delete the child first.
		 */
		INJECTION_POINT("bark-search-descend", NULL);

		buf = ReleaseAndReadBuffer(buf, index, blkno);
		LockBuffer(buf, access);
	}

	if (forwrite && access == BUFFER_LOCK_SHARE)
	{
		/*
		 * The root is the leaf, so it was share-locked.  Trade up, and move
		 * right again in case it split while it was unlocked.
		 */
		LockBuffer(buf, BUFFER_LOCK_UNLOCK);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
		buf = bark_moveright(index, keyinfo, key, buf, true, path,
							 BUFFER_LOCK_EXCLUSIVE);
	}

	if (stack)
		*stack = path;
	else
		bark_freestack(path);
	return buf;
}

Buffer
bark_search(Relation index, BarkKeyInfo *keyinfo, IndexTuple key,
			ItemPointer scantid, bool forwrite, bool nextkey, BarkStack *stack)
{
	BarkSearchKey skey = {key, scantid, NULL};

	/* A heap TID equal to a pivot's belongs right of it (bark_moveright). */
	Assert(scantid == NULL || nextkey);

	return bark_descend(index, keyinfo, &skey, forwrite, nextkey, stack);
}

/*
 * Descend to the leaf where a scan bounded by `bound` starts.  A read-only
 * descent: like nbtree's readers, it moves right past splits and removed
 * pages but never finishes an incomplete split.
 */
Buffer
bark_search_bound(Relation index, BarkKeyInfo *keyinfo,
				  const BarkScanBound *bound, bool nextkey)
{
	BarkSearchKey skey = {NULL, NULL, bound};

	return bark_descend(index, keyinfo, &skey, false, nextkey, NULL);
}

/*
 * On leaf `page`, the first offset whose entry sorts after `bound`, as
 * nbtree's _bt_binsrch finds where _bt_first starts on the leaf.  A bound
 * never equals an entry (see bark_compare_bound), so every entry before the
 * offset sorts before the bound: a forward scan from a lower bound starts at
 * the offset, and a backward scan from an upper bound starts just before it.
 */
OffsetNumber
bark_binsrch_bound(Relation index, BarkKeyInfo *keyinfo,
				   const BarkScanBound *bound, Page page)
{
	BarkSearchKey skey = {NULL, NULL, bound};

	Assert(BarkPageIsLeaf(BarkPageGetOpaque(page)));
	return bark_binsrch(index, keyinfo, &skey, page, false);
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
