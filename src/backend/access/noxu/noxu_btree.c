/*
 * noxu_btree.c
 *		Common routines for handling TID and attibute B-tree structures
 *
 * A Noxu table consists of multiple B-trees, one to store TIDs and
 * visibility information of the rows, and one tree for each attribute,
 * to hold the data. The TID and attribute trees differ at the leaf
 * level, but the internal pages have the same layout. This file contains
 * routines to deal with internal pages, and some other common
 * functionality.
 *
 * When dealing with the TID tree, pass NX_META_ATTRIBUTE_NUM as the
 * attribute number.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/noxu/noxu_btree.c
 */
#include "postgres.h"

#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "access/noxu_internal.h"
#include "access/noxu_wal.h"
#include "access/relundo.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/procarray.h"
#include "utils/rel.h"

/* prototypes for local functions */
static nx_split_stack * nxbt_split_internal_page(Relation rel, AttrNumber attno,
												 Buffer leftbuf, OffsetNumber newoff, List *downlinks,
												 NXBtreePathEntry *path);
static nx_split_stack * nxbt_merge_pages(Relation rel, AttrNumber attno, Buffer leftbuf, Buffer rightbuf, bool target_is_left);

static int	nxbt_binsrch_internal(nxtid key, NXBtreeInternalPageItem *arr, int arr_elems);
static void nxbt_invalidate_cache_if_needed(Relation rel, AttrNumber attno,
											 BlockNumber held_block);

/*
 * Defensive cache invalidation before descending the tree.
 *
 * If we're holding a buffer lock and the cache might point to that
 * buffer anywhere in the tree structure, invalidate the cache to force
 * a fresh read from the metapage.
 *
 * This prevents self-deadlock where we try to lock a buffer we already hold.
 */
static void
nxbt_invalidate_cache_if_needed(Relation rel, AttrNumber attno,
								 BlockNumber held_block)
{
	NXMetaCacheData *metacache;

	if (held_block == InvalidBlockNumber)
		return;  /* No buffer held, no risk */

	metacache = nxmeta_get_cache(rel);
	if (attno >= metacache->cache_nattributes)
		return;

	/*
	 * Invalidate if ANY cached value matches the block we're holding:
	 * - Root block
	 * - Rightmost block
	 *
	 * We don't track parent/internal nodes in cache, so those should be safe.
	 * But to be absolutely safe, we invalidate the entire attribute cache.
	 */
	if (metacache->cache_attrs[attno].root == held_block ||
		metacache->cache_attrs[attno].rightmost == held_block)
	{
		/* Invalidate this attribute's cache */
		metacache->cache_attrs[attno].root = InvalidBlockNumber;
		metacache->cache_attrs[attno].rightmost = InvalidBlockNumber;
		metacache->cache_attrs[attno].rightmost_lokey = InvalidNXTid;
	}
}

/*
 * Find the page containing the given key TID at the given level.
 *
 * Level 0 means leaf. The returned buffer is locked in the mode determined
 * by 'for_update': exclusive for writes, shared for reads.
 *
 * If tree doesn't exist at all (probably because the table was just created
 * or truncated), the behavior depends on the 'readonly' argument. If
 * readonly == true, then returns InvalidBuffer. If readonly == false, then
 * the tree is created.
 *
 * If 'held_buf' or 'held_buf2' are not InvalidBuffer, we are holding locks
 * on those buffers and must not try to lock them again (would cause
 * self-deadlock).  Two held buffers are supported because nxbt_merge_pages
 * holds locks on both left and right pages while descending to find the
 * parent.
 */
Buffer
nxbt_descend(Relation rel, AttrNumber attno, nxtid key, int level,
			 bool readonly, bool for_update, Buffer held_buf, Buffer held_buf2)
{
	BlockNumber next;
	Buffer		buf;
	Page		page;
	NXBtreePageOpaque *opaque;
	NXBtreeInternalPageItem *items;
	int			nitems;
	int			itemno;
	int			nextlevel;
	BlockNumber failblk = InvalidBlockNumber;
	int			faillevel = -1;
	NXMetaCacheData *metacache;
	BlockNumber held_block = InvalidBlockNumber;
	BlockNumber held_block2 = InvalidBlockNumber;
	int			self_deadlock_retries = 0;

	if (BufferIsValid(held_buf))
		held_block = BufferGetBlockNumber(held_buf);
	if (BufferIsValid(held_buf2))
		held_block2 = BufferGetBlockNumber(held_buf2);

	Assert(key != InvalidNXTid);

	/*
	 * Fast path for the very common case that we're looking for the rightmost
	 * page.  Skip the fast path when we hold buffers, because the cached
	 * rightmost block could be one of them (stale cache after a split).
	 */
	metacache = nxmeta_get_cache(rel);
	if (level == 0 &&
		held_block == InvalidBlockNumber &&
		held_block2 == InvalidBlockNumber &&
		attno < metacache->cache_nattributes &&
		metacache->cache_attrs[attno].rightmost != InvalidBlockNumber &&
		key >= metacache->cache_attrs[attno].rightmost_lokey)
	{
		next = metacache->cache_attrs[attno].rightmost;
		nextlevel = 0;
	}
	else
	{
		/* start from root */
		next = nxmeta_get_root_for_attribute(rel, attno, readonly);
		if (next == InvalidBlockNumber)
		{
			/* completely empty tree */
			return InvalidBuffer;
		}
		nextlevel = -1;
	}
	for (;;)
	{
		/*
		 * If we arrive again to a block that was a dead-end earlier, it seems
		 * that the tree is corrupt.
		 *
		 * It is theoretically possible for a block to be recycled at the same
		 * location between retries (freed, re-allocated, then freed again).
		 * In practice this is extremely unlikely: it would require a
		 * concurrent page split or deletion to reuse the exact same block
		 * number during our descent. The elog(ERROR) here is a safety net;
		 * if it ever fires, the operation can be retried at a higher level.
		 */
		if (next == failblk || next == NX_META_BLK)
			elog(ERROR, "arrived at incorrect block %u while descending noxu btree", next);

		buf = ReadBuffer(rel, next);

		/*
		 * CRITICAL: Check for self-deadlock before locking.
		 *
		 * If we're about to lock a buffer we already hold, it means
		 * the metacache was stale. Invalidate cache and retry from root.
		 */
		if ((held_block != InvalidBlockNumber && next == held_block) ||
			(held_block2 != InvalidBlockNumber && next == held_block2))
		{
			ReleaseBuffer(buf);

			if (++self_deadlock_retries > 3)
				elog(ERROR, "persistent self-deadlock in B-tree descent: "
							"block %u is always reached after cache "
							"invalidation (held blocks: %u, %u)",
							next, held_block, held_block2);

			elog(WARNING, "avoided self-deadlock in B-tree descent: "
						 "tried to lock block %u which is already held",
						 next);
			nxmeta_invalidate_cache(rel);
			next = nxmeta_get_root_for_attribute(rel, attno, readonly);
			if (next == InvalidBlockNumber)
				elog(ERROR, "could not find root for attribute %d", attno);
			nextlevel = -1;
			continue;
		}

		LockBuffer(buf, for_update ? BUFFER_LOCK_EXCLUSIVE : BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);
		if (!nxbt_page_is_expected(rel, attno, key, nextlevel, buf))
		{
			/*
			 * We arrived at an unexpected page. This can happen with
			 * concurrent splits, or page deletions. We could try following
			 * the right-link, but there's no guarantee that's the correct
			 * page either, so let's restart from the root. If we landed here
			 * because of concurrent modifications, the next attempt should
			 * land on the correct page. Remember that we incorrectly ended up
			 * on this page, so that if this happens because the tree is
			 * corrupt, rather than concurrent splits, and we land here again,
			 * we won't loop forever.
			 */
			UnlockReleaseBuffer(buf);

			failblk = next;
			faillevel = nextlevel;
			nextlevel = -1;
			nxmeta_invalidate_cache(rel);
			next = nxmeta_get_root_for_attribute(rel, attno, readonly);
			if (next == InvalidBlockNumber)
				elog(ERROR, "could not find root for attribute %d", attno);

			/*
			 * If the root was split after we cached the metadata, it's
			 * possible that the page we thought was the root page no longer
			 * is, but as we descend from the new root page, we'll end up on
			 * the same page again anyway. Don't treat thatas an error. To
			 * avoid it, check for the root case here, and if reset 'failblk'.
			 */
			if (faillevel == -1)
			{
				if (next == failblk)
					elog(ERROR, "arrived at incorrect block %u while descending noxu btree", next);
				failblk = InvalidBlockNumber;
			}
			continue;
		}
		opaque = NXBtreePageGetOpaque(page);

		if (nextlevel == -1)
			nextlevel = opaque->nx_level;

		else if (opaque->nx_level != nextlevel)
			elog(ERROR, "unexpected level encountered when descending tree");

		if (opaque->nx_level == level)
			break;

		/* Find the downlink and follow it */
		items = NXBtreeInternalPageGetItems(page);
		nitems = NXBtreeInternalPageGetNumItems(page);

		itemno = nxbt_binsrch_internal(key, items, nitems);
		if (itemno < 0)
			elog(ERROR, "could not descend tree for tid (%u, %u)",
				 NXTidGetBlockNumber(key), NXTidGetOffsetNumber(key));

		next = items[itemno].childblk;
		nextlevel--;

		UnlockReleaseBuffer(buf);
	}

	if (opaque->nx_level == 0 && opaque->nx_next == InvalidBlockNumber)
	{
		metacache = nxmeta_get_cache(rel);
		if (attno < metacache->cache_nattributes)
		{
			metacache->cache_attrs[attno].rightmost = next;
			metacache->cache_attrs[attno].rightmost_lokey = opaque->nx_lokey;
		}
	}

	return buf;
}

/*
 * Variant of nxbt_descend() that records the descent path.
 *
 * If path_out is non-NULL, the path from root to the target level is
 * returned as a singly-linked list of NXBtreePathEntry nodes (leaf at
 * front, root at tail).  The caller must free the path with
 * nxbt_free_path() when done.
 *
 * This is used by the insert path so that after a leaf-level split,
 * nxbt_insert_downlinks() can walk back up the remembered path instead
 * of re-descending from the root.
 */
Buffer
nxbt_descend_with_path(Relation rel, AttrNumber attno, nxtid key, int level,
					   bool readonly, bool for_update,
					   Buffer held_buf, Buffer held_buf2,
					   NXBtreePathEntry **path_out)
{
	BlockNumber next;
	Buffer		buf;
	Page		page;
	NXBtreePageOpaque *opaque;
	NXBtreeInternalPageItem *items;
	int			nitems;
	int			itemno;
	int			nextlevel;
	BlockNumber failblk = InvalidBlockNumber;
	int			faillevel = -1;
	NXMetaCacheData *metacache;
	BlockNumber held_block = InvalidBlockNumber;
	BlockNumber held_block2 = InvalidBlockNumber;
	int			self_deadlock_retries = 0;
	NXBtreePathEntry *path = NULL;

	if (path_out)
		*path_out = NULL;

	if (BufferIsValid(held_buf))
		held_block = BufferGetBlockNumber(held_buf);
	if (BufferIsValid(held_buf2))
		held_block2 = BufferGetBlockNumber(held_buf2);

	Assert(key != InvalidNXTid);

	/*
	 * Fast path for the rightmost page.  When recording path, skip the fast
	 * path because we need the full descent for the path stack.
	 */
	metacache = nxmeta_get_cache(rel);
	if (path_out == NULL &&
		level == 0 &&
		held_block == InvalidBlockNumber &&
		held_block2 == InvalidBlockNumber &&
		attno < metacache->cache_nattributes &&
		metacache->cache_attrs[attno].rightmost != InvalidBlockNumber &&
		key >= metacache->cache_attrs[attno].rightmost_lokey)
	{
		next = metacache->cache_attrs[attno].rightmost;
		nextlevel = 0;
	}
	else
	{
		/* start from root */
		next = nxmeta_get_root_for_attribute(rel, attno, readonly);
		if (next == InvalidBlockNumber)
			return InvalidBuffer;
		nextlevel = -1;
	}

	for (;;)
	{
		if (next == failblk || next == NX_META_BLK)
			elog(ERROR, "arrived at incorrect block %u while descending noxu btree", next);

		buf = ReadBuffer(rel, next);

		/* Self-deadlock check */
		if ((held_block != InvalidBlockNumber && next == held_block) ||
			(held_block2 != InvalidBlockNumber && next == held_block2))
		{
			ReleaseBuffer(buf);

			if (++self_deadlock_retries > 3)
				elog(ERROR, "persistent self-deadlock in B-tree descent: "
					 "block %u is always reached after cache "
					 "invalidation (held blocks: %u, %u)",
					 next, held_block, held_block2);

			elog(WARNING, "avoided self-deadlock in B-tree descent: "
				 "tried to lock block %u which is already held",
				 next);
			nxmeta_invalidate_cache(rel);
			next = nxmeta_get_root_for_attribute(rel, attno, readonly);
			if (next == InvalidBlockNumber)
				elog(ERROR, "could not find root for attribute %d", attno);
			nextlevel = -1;

			/* Invalidate path on restart */
			if (path_out)
			{
				nxbt_free_path(path);
				path = NULL;
			}
			continue;
		}

		LockBuffer(buf, for_update ? BUFFER_LOCK_EXCLUSIVE : BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);
		if (!nxbt_page_is_expected(rel, attno, key, nextlevel, buf))
		{
			UnlockReleaseBuffer(buf);

			failblk = next;
			faillevel = nextlevel;
			nextlevel = -1;
			nxmeta_invalidate_cache(rel);
			next = nxmeta_get_root_for_attribute(rel, attno, readonly);
			if (next == InvalidBlockNumber)
				elog(ERROR, "could not find root for attribute %d", attno);

			if (faillevel == -1)
			{
				if (next == failblk)
					elog(ERROR, "arrived at incorrect block %u while descending noxu btree", next);
				failblk = InvalidBlockNumber;
			}

			/* Invalidate path on restart */
			if (path_out)
			{
				nxbt_free_path(path);
				path = NULL;
			}
			continue;
		}
		opaque = NXBtreePageGetOpaque(page);

		if (nextlevel == -1)
			nextlevel = opaque->nx_level;
		else if (opaque->nx_level != nextlevel)
			elog(ERROR, "unexpected level encountered when descending tree");

		if (opaque->nx_level == level)
			break;

		/* Record this internal page in the path */
		if (path_out)
		{
			NXBtreePathEntry *entry = palloc(sizeof(NXBtreePathEntry));

			entry->blkno = next;
			entry->level = opaque->nx_level;
			entry->parent = path;
			path = entry;
		}

		/* Find the downlink and follow it */
		items = NXBtreeInternalPageGetItems(page);
		nitems = NXBtreeInternalPageGetNumItems(page);

		itemno = nxbt_binsrch_internal(key, items, nitems);
		if (itemno < 0)
			elog(ERROR, "could not descend tree for tid (%u, %u)",
				 NXTidGetBlockNumber(key), NXTidGetOffsetNumber(key));

		next = items[itemno].childblk;
		nextlevel--;

		UnlockReleaseBuffer(buf);
	}

	if (opaque->nx_level == 0 && opaque->nx_next == InvalidBlockNumber)
	{
		metacache = nxmeta_get_cache(rel);
		if (attno < metacache->cache_nattributes)
		{
			metacache->cache_attrs[attno].rightmost = next;
			metacache->cache_attrs[attno].rightmost_lokey = opaque->nx_lokey;
		}
	}

	if (path_out)
		*path_out = path;

	return buf;
}


/*
 * Find and lock the leaf page that contains data for scan->nexttid.
 *
 * If 'buf' is valid, it is a previously pinned page. We will check that
 * page first. If it's not the correct page, it will be released.
 *
 * Returns InvalidBuffer, if the attribute tree doesn't exist at all.
 * That should only happen after ALTER TABLE ADD COLUMN. Or on a newly
 * created table, but none of the current callers would even try to
 * fetch attribute data, without scanning the TID tree first.)
 */
Buffer
nxbt_find_and_lock_leaf_containing_tid(Relation rel, AttrNumber attno,
									   Buffer buf, nxtid nexttid, int lockmode)
{
	if (BufferIsValid(buf))
	{
retry:
		LockBuffer(buf, lockmode);

		/*
		 * It's possible that the page was concurrently split or recycled by
		 * another backend (or ourselves). Have to re-check that the page is
		 * still valid.
		 */
		if (nxbt_page_is_expected(rel, attno, nexttid, 0, buf))
			return buf;
		else
		{
			/*
			 * It's not valid for the TID we're looking for, but maybe it was
			 * the right page for the previous TID. In that case, we don't
			 * need to restart from the root, we can follow the right-link
			 * instead.
			 */
			if (nexttid > MinNXTid &&
				nxbt_page_is_expected(rel, attno, nexttid - 1, 0, buf))
			{
				Page		page = BufferGetPage(buf);
				NXBtreePageOpaque *opaque = NXBtreePageGetOpaque(page);
				BlockNumber next = opaque->nx_next;

				if (next != InvalidBlockNumber)
				{
					LockBuffer(buf, BUFFER_LOCK_UNLOCK);
					buf = ReleaseAndReadBuffer(buf, rel, next);
					goto retry;
				}
			}

			UnlockReleaseBuffer(buf);
			buf = InvalidBuffer;
		}
	}

	/* Descend the B-tree to find the correct leaf page. */
	if (!BufferIsValid(buf))
		buf = nxbt_descend(rel, attno, nexttid, 0, true,
						   lockmode == BUFFER_LOCK_EXCLUSIVE,
						   InvalidBuffer, InvalidBuffer);

	return buf;
}


/*
 * Check that a page is a valid B-tree page, and covers the given key.
 *
 * This is used when traversing the tree, to check that e.g. a concurrent page
 * split didn't move pages around, so that the page we were walking to isn't
 * the correct one anymore.
 */
bool
nxbt_page_is_expected(Relation rel, AttrNumber attno, nxtid key, int level, Buffer buf)
{
	Page		page = BufferGetPage(buf);
	NXBtreePageOpaque *opaque;

	(void) rel;

	/*
	 * The page might have been deleted and even reused as a completely
	 * different kind of a page, so we must be prepared for anything.
	 */
	if (PageIsNew(page))
		return false;

	if (PageGetSpecialSize(page) != MAXALIGN(sizeof(NXBtreePageOpaque)))
		return false;

	opaque = NXBtreePageGetOpaque(page);

	if (opaque->nx_page_id != NX_BTREE_PAGE_ID)
		return false;

	if (opaque->nx_attno != attno)
		return false;

	if (level == -1)
	{
		if ((opaque->nx_flags & NXBT_ROOT) == 0)
			return false;
	}
	else
	{
		if (opaque->nx_level != level)
			return false;
	}

	if (opaque->nx_lokey > key || opaque->nx_hikey <= key)
		return false;

	/* extra checks for corrupted pages */
	if (opaque->nx_next == BufferGetBlockNumber(buf))
		elog(ERROR, "btree page %u next-pointer points to itself", opaque->nx_next);

	return true;
}

/*
 * Create a new btree root page, containing two downlinks.
 *
 * NOTE: the very first root page of a btree, which is also the leaf, is created
 * in nxmeta_get_root_for_attribute(), not here.
 *
 * We check that all downlinks fit on the page before writing them; if they
 * don't, we error out (this shouldn't happen in practice).
 */
nx_split_stack *
nxbt_newroot(Relation rel, AttrNumber attno, int level, List *downlinks)
{
	Page		metapage;
	NXMetaPage *metapg;
	Buffer		newrootbuf;
	Page		newrootpage;
	NXBtreePageOpaque *newrootopaque;
	NXBtreeInternalPageItem *items;
	Buffer		metabuf;
	nx_split_stack *stack1;
	nx_split_stack *stack2;
	ListCell   *lc;
	int			i;

	metabuf = ReadBuffer(rel, NX_META_BLK);
	LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);

	/* allocate a new root page */
	newrootbuf = nxpage_getnewbuf(rel, metabuf);
	newrootpage = palloc(BLCKSZ);
	PageInit(newrootpage, BLCKSZ, sizeof(NXBtreePageOpaque));
	newrootopaque = NXBtreePageGetOpaque(newrootpage);
	newrootopaque->nx_attno = attno;
	newrootopaque->nx_next = InvalidBlockNumber;
	newrootopaque->nx_lokey = MinNXTid;
	newrootopaque->nx_hikey = MaxPlusOneNXTid;
	newrootopaque->nx_level = level;
	newrootopaque->nx_flags = NXBT_ROOT;
	newrootopaque->nx_page_id = NX_BTREE_PAGE_ID;

	items = NXBtreeInternalPageGetItems(newrootpage);

	/* Check that all the downlinks fit on the page. */
	{
		int		ndownlinks = list_length(downlinks);
		Size	required = ndownlinks * sizeof(NXBtreeInternalPageItem);
		Size	available = PageGetExactFreeSpace(newrootpage);

		if (required > available)
			elog(ERROR, "too many downlinks (%d) to fit on new root page for attribute %d of \"%s\" (need %zu bytes, have %zu bytes)",
				 ndownlinks, attno, RelationGetRelationName(rel),
				 required, available);
	}

	/* add all the downlinks */
	i = 0;
	foreach(lc, downlinks)
	{
		NXBtreeInternalPageItem *downlink = (NXBtreeInternalPageItem *) lfirst(lc);

		items[i++] = *downlink;
	}
	((PageHeader) newrootpage)->pd_lower += i * sizeof(NXBtreeInternalPageItem);

	/* update the metapage */
	metapage = PageGetTempPageCopy(BufferGetPage(metabuf));

	metapg = (NXMetaPage *) PageGetContents(metapage);
	if ((attno != NX_META_ATTRIBUTE_NUM) && (attno <= 0 || attno > metapg->nattributes))
		elog(ERROR, "invalid attribute number %d (table \"%s\" has only %d attributes)",
			 attno, RelationGetRelationName(rel), metapg->nattributes);

	metapg->tree_root_dir[attno].root = BufferGetBlockNumber(newrootbuf);

	stack1 = nx_new_split_stack_entry(metabuf, metapage);
	stack2 = nx_new_split_stack_entry(newrootbuf, newrootpage);
	stack2->next = stack1;

	return stack2;
}

/*
 * After page split, insert the downlink of 'rightblkno' to the parent.
 *
 * On entry, 'leftbuf' must be pinned exclusive-locked.
 *
 * If 'path' is non-NULL, it is a remembered descent path from a previous
 * nxbt_descend_with_path() call.  We try to use it to find the parent
 * directly instead of re-descending from the root.  If the path entry is
 * stale (page was split concurrently), we fall back to a full descent.
 * Consumed path entries are freed.
 */
nx_split_stack *
nxbt_insert_downlinks(Relation rel, AttrNumber attno,
					  nxtid leftlokey, BlockNumber leftblkno, int level,
					  List *downlinks, Buffer held_buf,
					  NXBtreePathEntry *path)
{
	int			numdownlinks = list_length(downlinks);
	NXBtreeInternalPageItem *items;
	int			nitems;
	int			itemno;
	Buffer		parentbuf;
	Page		parentpage;
	nx_split_stack *split_stack;
	NXBtreeInternalPageItem *firstdownlink;

	/*
	 * Try to use the remembered path to find the parent directly.
	 *
	 * Walk up the path stack to find an entry at the target level.  If
	 * found, read that block and verify it's still the right page.  This
	 * avoids the cost of a full root-to-target descent after every split.
	 */
	parentbuf = InvalidBuffer;
	if (path != NULL)
	{
		NXBtreePathEntry *entry = path;

		/* Walk up to find the entry at the target level */
		while (entry != NULL && entry->level < level)
			entry = entry->parent;

		if (entry != NULL && entry->level == level)
		{
			BlockNumber parent_blkno = entry->blkno;

			/*
			 * Skip if the cached block is one we're holding a lock on;
			 * attempting to lock it again would self-deadlock.
			 */
			if (parent_blkno != leftblkno &&
				(!BufferIsValid(held_buf) ||
				 parent_blkno != BufferGetBlockNumber(held_buf)))
			{
				Buffer		buf = ReadBuffer(rel, parent_blkno);

				LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

				if (nxbt_page_is_expected(rel, attno, leftlokey, level, buf))
				{
					parentbuf = buf;
				}
				else
				{
					/* Path is stale; fall back to full descent */
					UnlockReleaseBuffer(buf);
				}
			}
		}
	}

	/*
	 * Fall back to re-descending from root if path didn't work.
	 */
	if (!BufferIsValid(parentbuf))
	{
		/*
		 * Defensive cache invalidation before descending to find parent.
		 *
		 * We're holding a lock on leftblkno. If the cache incorrectly thinks
		 * leftblkno is the root (or rightmost), we would deadlock with
		 * ourselves.  Invalidate the cache if it points to the block we're
		 * holding.
		 */
		nxbt_invalidate_cache_if_needed(rel, attno, leftblkno);

		parentbuf = nxbt_descend(rel, attno, leftlokey, level, false, true, held_buf, InvalidBuffer);
	}
	parentpage = BufferGetPage(parentbuf);

	firstdownlink = (NXBtreeInternalPageItem *) linitial(downlinks);

	/* Find the position in the parent for the downlink */
	items = NXBtreeInternalPageGetItems(parentpage);
	nitems = NXBtreeInternalPageGetNumItems(parentpage);
	itemno = nxbt_binsrch_internal(firstdownlink->tid, items, nitems);

	/* sanity checks */
	if (itemno < 0 || items[itemno].tid != leftlokey ||
		items[itemno].childblk != leftblkno)
	{
		elog(ERROR, "could not find downlink for block %u TID (%u, %u)",
			 leftblkno, NXTidGetBlockNumber(leftlokey),
			 NXTidGetOffsetNumber(leftlokey));
	}
	itemno++;

	if (PageGetExactFreeSpace(parentpage) < numdownlinks * sizeof(NXBtreeInternalPageItem))
	{
		/* split internal page */
		split_stack = nxbt_split_internal_page(rel, attno, parentbuf, itemno, downlinks, path);
	}
	else
	{
		NXBtreeInternalPageItem *newitems;
		Page		newpage;
		int			i;
		ListCell   *lc;

		newpage = PageGetTempPageCopySpecial(parentpage);

		split_stack = nx_new_split_stack_entry(parentbuf, newpage);

		/* insert the new downlink for the right page. */
		newitems = NXBtreeInternalPageGetItems(newpage);
		memcpy(newitems, items, itemno * sizeof(NXBtreeInternalPageItem));

		i = itemno;
		foreach(lc, downlinks)
		{
			NXBtreeInternalPageItem *downlink = (NXBtreeInternalPageItem *) lfirst(lc);

			Assert(downlink->childblk != 0);
			newitems[i++] = *downlink;
		}

		memcpy(&newitems[i], &items[itemno], (nitems - itemno) * sizeof(NXBtreeInternalPageItem));
		((PageHeader) newpage)->pd_lower += (nitems + numdownlinks) * sizeof(NXBtreeInternalPageItem);
	}
	return split_stack;
}

/*
 * Split an internal page.
 *
 * The new downlink specified by 'newkey' is inserted to position 'newoff', on 'leftbuf'.
 * The page is split.
 */
static nx_split_stack *
nxbt_split_internal_page(Relation rel, AttrNumber attno, Buffer origbuf,
						 OffsetNumber newoff, List *newitems,
						 NXBtreePathEntry *path)
{
	Page		origpage = BufferGetPage(origbuf);
	NXBtreePageOpaque *origopaque = NXBtreePageGetOpaque(origpage);
	Buffer		buf;
	Page		page;
	NXBtreeInternalPageItem *origitems;
	int			orignitems;
	nx_split_stack *stack_first;
	nx_split_stack *stack;
	Size		splitthreshold;
	ListCell   *lc;
	int			origitemno;
	List	   *downlinks = NIL;

	origitems = NXBtreeInternalPageGetItems(origpage);
	orignitems = NXBtreeInternalPageGetNumItems(origpage);

	page = PageGetTempPageCopySpecial(origpage);
	buf = origbuf;

	stack = nx_new_split_stack_entry(buf, page);
	stack_first = stack;

	/*
	 * Adaptive split ratio.  If the user set split_pct via reloptions, use
	 * that value.  Otherwise default to 90/10 for rightmost internal pages
	 * (where sequential inserts append) and 50/50 for non-rightmost pages.
	 * This matches the leaf-level strategy in nxbt_tid_recompress_picksplit().
	 */
	{
		int			split_pct;

		if (rel->rd_options &&
			((StdRdOptions *) rel->rd_options)->split_pct > 0)
			split_pct = RelationGetSplitPct(rel);
		else if (origopaque->nx_hikey == MaxPlusOneNXTid)
			split_pct = 90;
		else
			split_pct = 50;

		splitthreshold = PageGetExactFreeSpace(page) * (100 - split_pct) / 100;
	}

	lc = list_head(newitems);
	origitemno = 0;
	for (;;)
	{
		NXBtreeInternalPageItem *item;
		NXBtreeInternalPageItem *p;

		if (origitemno == newoff && lc)
		{
			item = lfirst(lc);
			lc = lnext(newitems, lc);
		}
		else
		{
			if (origitemno == orignitems)
				break;
			item = &origitems[origitemno];
			origitemno++;
		}

		if (PageGetExactFreeSpace(page) < splitthreshold)
		{
			/* have to split to another page */
			NXBtreePageOpaque *prevopaque = NXBtreePageGetOpaque(page);
			NXBtreePageOpaque *opaque = NXBtreePageGetOpaque(page);
			BlockNumber blkno;
			NXBtreeInternalPageItem *downlink;

			buf = nxpage_getnewbuf(rel, InvalidBuffer);
			blkno = BufferGetBlockNumber(buf);
			page = palloc(BLCKSZ);
			PageInit(page, BLCKSZ, sizeof(NXBtreePageOpaque));

			opaque = NXBtreePageGetOpaque(page);
			opaque->nx_attno = attno;
			opaque->nx_next = prevopaque->nx_next;
			opaque->nx_lokey = item->tid;
			opaque->nx_hikey = prevopaque->nx_hikey;
			opaque->nx_level = prevopaque->nx_level;
			opaque->nx_flags = 0;
			opaque->nx_page_id = NX_BTREE_PAGE_ID;

			prevopaque->nx_next = blkno;
			prevopaque->nx_hikey = item->tid;

			stack->next = nx_new_split_stack_entry(buf, page);
			stack = stack->next;

			downlink = palloc(sizeof(NXBtreeInternalPageItem));
			downlink->tid = item->tid;
			downlink->childblk = blkno;
			downlinks = lappend(downlinks, downlink);
		}

		p = (NXBtreeInternalPageItem *) ((char *) page + ((PageHeader) page)->pd_lower);
		*p = *item;
		((PageHeader) page)->pd_lower += sizeof(NXBtreeInternalPageItem);
	}

	/* recurse to insert downlinks, if we had to split. */
	if (downlinks)
	{
		if ((origopaque->nx_flags & NXBT_ROOT) != 0)
		{
			NXBtreeInternalPageItem *downlink;

			downlink = palloc(sizeof(NXBtreeInternalPageItem));
			downlink->tid = MinNXTid;
			downlink->childblk = BufferGetBlockNumber(origbuf);
			downlinks = lcons(downlink, downlinks);

			stack->next = nxbt_newroot(rel, attno, origopaque->nx_level + 1, downlinks);

			/* clear the NXBT_ROOT flag on the old root page */
			NXBtreePageGetOpaque(stack_first->page)->nx_flags &= ~NXBT_ROOT;
		}
		else
		{
			stack->next = nxbt_insert_downlinks(rel, attno,
												origopaque->nx_lokey,
												BufferGetBlockNumber(origbuf),
												origopaque->nx_level + 1,
												downlinks, origbuf,
												path);
		}
	}

	/* Free the downlinks list and items allocated during split */
	list_free_deep(downlinks);

	return stack_first;
}


/*
 * Removes the last item from page, and unlinks the page from the tree.
 *
 * NOTE: you cannot remove the only leaf. Returns NULL if the page could not
 * be deleted.
 */
nx_split_stack *
nxbt_unlink_page(Relation rel, AttrNumber attno, Buffer buf, int level)
{
	Page		page = BufferGetPage(buf);
	NXBtreePageOpaque *opaque = NXBtreePageGetOpaque(page);
	Buffer		leftbuf;
	Buffer		rightbuf;
	nx_split_stack *stack;

	/* cannot currently remove the only page at its level. */
	if (opaque->nx_lokey == MinNXTid && opaque->nx_hikey == MaxPlusOneNXTid)
	{
		return NULL;
	}

	/*
	 * Find left sibling. or if this is leftmost page, find right sibling.
	 */
	if (opaque->nx_lokey != MinNXTid)
	{
		rightbuf = buf;
		leftbuf = nxbt_descend(rel, attno, opaque->nx_lokey - 1, level, false, true, buf, InvalidBuffer);

		stack = nxbt_merge_pages(rel, attno, leftbuf, rightbuf, false);
		if (!stack)
		{
			UnlockReleaseBuffer(leftbuf);
			return NULL;
		}
	}
	else
	{
		rightbuf = nxbt_descend(rel, attno, opaque->nx_hikey, level, false, true, buf, InvalidBuffer);
		leftbuf = buf;
		stack = nxbt_merge_pages(rel, attno, leftbuf, rightbuf, true);
		if (!stack)
		{
			UnlockReleaseBuffer(rightbuf);
			return NULL;
		}
	}

	return stack;
}

/*
 * Page deletion:
 *
 * Mark page empty, remove downlink. If parent becomes empty, recursively delete it.
 *
 * Unlike in the nbtree index, we don't need to worry about concurrent scans. They
 * will simply retry if they land on an unexpected page.
 */
static nx_split_stack *
nxbt_merge_pages(Relation rel, AttrNumber attno, Buffer leftbuf, Buffer rightbuf, bool target_is_left)
{
	Buffer		parentbuf;
	Page		origleftpage;
	Page		leftpage;
	Page		rightpage;
	NXBtreePageOpaque *leftopaque;
	NXBtreePageOpaque *origleftopaque;
	NXBtreePageOpaque *rightopaque;
	NXBtreeInternalPageItem *parentitems;
	int			parentnitems;
	Page		parentpage;
	int			itemno;
	nx_split_stack *stack;
	nx_split_stack *stack_head;
	nx_split_stack *stack_tail;

	origleftpage = BufferGetPage(leftbuf);
	origleftopaque = NXBtreePageGetOpaque(origleftpage);
	rightpage = BufferGetPage(rightbuf);
	rightopaque = NXBtreePageGetOpaque(rightpage);

	/*
	 * Invalidate cache if it points to buffers we're holding,
	 * to prevent self-deadlock.
	 */
	nxbt_invalidate_cache_if_needed(rel, attno, BufferGetBlockNumber(leftbuf));
	nxbt_invalidate_cache_if_needed(rel, attno, BufferGetBlockNumber(rightbuf));

	/* find downlink for 'rightbuf' in the parent */
	parentbuf = nxbt_descend(rel, attno, rightopaque->nx_lokey, origleftopaque->nx_level + 1, false, true, leftbuf, rightbuf);
	parentpage = BufferGetPage(parentbuf);

	parentitems = NXBtreeInternalPageGetItems(parentpage);
	parentnitems = NXBtreeInternalPageGetNumItems(parentpage);
	itemno = nxbt_binsrch_internal(rightopaque->nx_lokey, parentitems, parentnitems);
	if (itemno < 0 || parentitems[itemno].childblk != BufferGetBlockNumber(rightbuf))
		elog(ERROR, "could not find downlink to FPM page %u", BufferGetBlockNumber(rightbuf));

	if (parentnitems > 1 && itemno == 0)
	{
		/*
		 * Deleting the leftmost child requires updating the parent's lokey.
		 * We handle this by updating the parent's lokey to match the second
		 * child's lokey after removal.
		 */
		NXBtreePageOpaque *parentopaque = NXBtreePageGetOpaque(parentpage);

		/*
		 * The new lokey for the parent will be the lokey of the second child
		 * (which becomes the first child after deletion).
		 */
		if (parentnitems > 1)
		{
			/*
			 * We'll update the parent's lokey after removing the downlink.
			 * The parent's new lokey will be taken from parentitems[1].lokey
			 * after we remove parentitems[0].
			 */
			elog(DEBUG2, "deleting leftmost child of parent at level %d, updating parent lokey",
				 parentopaque->nx_level);
		}
		/* Continue with normal deletion - we'll update parent lokey below */
	}

	if (target_is_left)
	{
		/* move all items from right to left before unlinking the right page */
		leftpage = PageGetTempPageCopy(rightpage);
		leftopaque = NXBtreePageGetOpaque(leftpage);

		memcpy(leftopaque, origleftopaque, sizeof(NXBtreePageOpaque));
	}
	else
	{
		/* right page is empty. */
		leftpage = PageGetTempPageCopy(origleftpage);
		leftopaque = NXBtreePageGetOpaque(leftpage);
	}

	/* update left hikey */
	leftopaque->nx_hikey = NXBtreePageGetOpaque(rightpage)->nx_hikey;
	leftopaque->nx_next = NXBtreePageGetOpaque(rightpage)->nx_next;

	Assert(NXBtreePageGetOpaque(leftpage)->nx_level == NXBtreePageGetOpaque(rightpage)->nx_level);

	stack = nx_new_split_stack_entry(leftbuf, leftpage);
	stack_head = stack_tail = stack;

	/* Mark right page as empty/unused */
	rightpage = palloc0(BLCKSZ);

	stack = nx_new_split_stack_entry(rightbuf, rightpage);
	stack->recycle = true;
	stack_tail->next = stack;
	stack_tail = stack;

	/* remove downlink from parent */
	if (parentnitems > 1)
	{
		Page		newpage = PageGetTempPageCopySpecial(parentpage);
		NXBtreeInternalPageItem *newitems = NXBtreeInternalPageGetItems(newpage);
		NXBtreePageOpaque *newparentopaque = NXBtreePageGetOpaque(newpage);

		memcpy(newitems, parentitems, itemno * sizeof(NXBtreeInternalPageItem));
		memcpy(&newitems[itemno], &parentitems[itemno + 1], (parentnitems - itemno - 1) * sizeof(NXBtreeInternalPageItem));

		((PageHeader) newpage)->pd_lower += (parentnitems - 1) * sizeof(NXBtreeInternalPageItem);

		/*
		 * If we deleted the leftmost child (itemno == 0), update the parent's
		 * lokey to match the new leftmost child's tid.
		 */
		if (itemno == 0 && parentnitems > 1)
		{
			newparentopaque->nx_lokey = newitems[0].tid;
			elog(DEBUG2, "updated parent lokey to %lu after deleting leftmost child",
				 (unsigned long) newitems[0].tid);
		}

		stack = nx_new_split_stack_entry(parentbuf, newpage);
		stack_tail->next = stack;
		stack_tail = stack;
	}
	else
	{
		/* the parent becomes empty as well. Recursively remove it. */
		stack_tail->next = nxbt_unlink_page(rel, attno, parentbuf, leftopaque->nx_level + 1);
		if (stack_tail->next == NULL)
		{
			/* oops, couldn't remove the parent. Back out */
			stack = stack_head;
			while (stack)
			{
				nx_split_stack *next = stack->next;

				pfree(stack->page);
				pfree(stack);
				stack = next;
			}
		}
	}

	return stack_head;
}

/*
 * Allocate a new nx_split_stack struct.
 */
nx_split_stack *
nx_new_split_stack_entry(Buffer buf, Page page)
{
	nx_split_stack *stack;

	stack = palloc(sizeof(nx_split_stack));
	stack->next = NULL;
	stack->buf = buf;
	stack->page = page;
	stack->recycle = false;		/* caller can change this */

	return stack;
}

/*
 * Apply all the changes represented by a list of nx_split_stack
 * entries.
 *
 * Pages marked with recycle=true are added to the Free Page Map within
 * the same critical section and WAL record, so that crash recovery will
 * also recycle them (avoiding page leaks).
 */
void
nx_apply_split_changes(Relation rel, nx_split_stack * stack, nx_pending_undo_op * undo_op)
{
	nx_split_stack *head = stack;
	bool		wal_needed = RelationNeedsWAL(rel);
	List	   *buffers = NIL;
	uint32		recycle_bitmap = 0;
	bool		has_recycle = false;
	Buffer		metabuf = InvalidBuffer;
	int			idx;

	/* Build the buffer list and recycle bitmap */
	idx = 0;
	stack = head;
	while (stack)
	{
		if (wal_needed)
			buffers = lappend_int(buffers, stack->buf);
		if (stack->recycle)
		{
			Assert(idx < 32);
			recycle_bitmap |= (1U << idx);
			has_recycle = true;
		}
		idx++;
		stack = stack->next;
	}

	/*
	 * If any pages need recycling, lock the metapage now so we can update
	 * nx_fpm_head inside the critical section.
	 */
	if (has_recycle)
	{
		metabuf = ReadBuffer(rel, NX_META_BLK);
		LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);
	}

	if (wal_needed)
	{
		int		nbufs = list_length(buffers);

		/* +1 for undo, +1 for metapage if recycling */
		XLogEnsureRecordSpace(nbufs + (has_recycle ? 1 : 0), 0);
	}

	START_CRIT_SECTION();

	stack = head;
	while (stack)
	{
		PageRestoreTempPage(stack->page, BufferGetPage(stack->buf));
		MarkBufferDirty(stack->buf);
		stack = stack->next;
	}

	if (undo_op)
	{
		/*
		 * Write the UNDO record into the RelUndo-reserved space.
		 * This replaces nxundo_finish_pending_op() as part of the
		 * migration to per-relation UNDO.
		 */
		Assert(CritSectionCount > 0);
		memcpy(undo_op->reservation.ptr, (char *) undo_op->payload,
			   undo_op->reservation.length);
		MarkBufferDirty(undo_op->reservation.undobuf);
	}

	/*
	 * Recycle pages inside the critical section so that the WAL record
	 * captures the FPM state change atomically.  Save old_fpm_head before
	 * modifying so we can include it in the WAL record for redo.
	 */
	{
		BlockNumber saved_old_fpm_head = InvalidBlockNumber;

		if (has_recycle)
		{
			Page		metapage = BufferGetPage(metabuf);
			NXMetaPageOpaque *metaopaque = (NXMetaPageOpaque *) PageGetSpecialPointer(metapage);
			BlockNumber fpm_head = metaopaque->nx_fpm_head;

			saved_old_fpm_head = fpm_head;

			stack = head;
			while (stack)
			{
				if (stack->recycle)
				{
					BlockNumber blk = BufferGetBlockNumber(stack->buf);
					Page		page = BufferGetPage(stack->buf);

					nxpage_mark_page_deleted(page, fpm_head);
					fpm_head = blk;
					MarkBufferDirty(stack->buf);
				}
				stack = stack->next;
			}

			metaopaque->nx_fpm_head = fpm_head;
			MarkBufferDirty(metabuf);
		}

		if (wal_needed)
		{
			nxbt_wal_log_rewrite_pages(rel, 0, buffers, undo_op,
									   recycle_bitmap, saved_old_fpm_head,
									   has_recycle ? metabuf : InvalidBuffer);
			list_free(buffers);
		}
	}

	END_CRIT_SECTION();

	if (BufferIsValid(metabuf))
		UnlockReleaseBuffer(metabuf);

	stack = head;
	while (stack)
	{
		nx_split_stack *next;

		UnlockReleaseBuffer(stack->buf);

		next = stack->next;
		pfree(stack);
		stack = next;
	}

	if (undo_op)
	{
		UnlockReleaseBuffer(undo_op->reservation.undobuf);
		pfree(undo_op);
	}
}

static int
nxbt_binsrch_internal(nxtid key, NXBtreeInternalPageItem *arr, int arr_elems)
{
	int			low,
				high,
				mid;

	low = 0;
	high = arr_elems;
	while (high > low)
	{
		mid = low + (high - low) / 2;

		if (key >= arr[mid].tid)
			low = mid + 1;
		else
			high = mid;
	}
	return low - 1;
}


void
nxbt_wal_log_leaf_items(Relation rel, AttrNumber attno, Buffer buf,
						OffsetNumber off, bool replace, List *items,
						nx_pending_undo_op * undo_op)
{
	ListCell   *lc;
	XLogRecPtr	recptr;
	wal_noxu_btree_leaf_items xlrec;

	(void) rel;

	xlrec.attno = attno;
	xlrec.nitems = list_length(items);
	xlrec.off = off;

	XLogBeginInsert();

	/* Register ALL buffers first, before any data */
	XLogRegisterBuffer(0, buf, REGBUF_STANDARD);
	if (undo_op)
		XLogRegisterUndoOp(1, undo_op);

	/* Now register all data after buffers are registered */
	XLogRegisterData((char *) &xlrec, SizeOfNXWalBtreeLeafItems);

	foreach(lc, items)
	{
		void	   *item = (void *) lfirst(lc);
		size_t		itemsz;

		if (attno == NX_META_ATTRIBUTE_NUM)
			itemsz = ((NXTidArrayItem *) item)->t_size;
		else
			itemsz = ((NXAttributeArrayItem *) item)->t_size;

		XLogRegisterBufData(0, item, itemsz);
	}

	recptr = XLogInsert(RM_NOXU_ID,
						replace ? WAL_NOXU_BTREE_REPLACE_LEAF_ITEM : WAL_NOXU_BTREE_ADD_LEAF_ITEMS);

	PageSetLSN(BufferGetPage(buf), recptr);
	if (undo_op)
		PageSetLSN(BufferGetPage(undo_op->reservation.undobuf), recptr);
}

void
nxbt_leaf_items_redo(XLogReaderState *record, bool replace)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	wal_noxu_btree_leaf_items *xlrec =
		(wal_noxu_btree_leaf_items *) XLogRecGetData(record);
	Buffer		buffer;
	Buffer		undobuf;

	if (XLogRecHasBlockRef(record, 1))
		undobuf = XLogRedoUndoOp(record, 1);
	else
		undobuf = InvalidBuffer;

	if (XLogReadBufferForRedo(record, 0, &buffer) == BLK_NEEDS_REDO)
	{
		Page		page = (Page) BufferGetPage(buffer);
		OffsetNumber off = xlrec->off;

		if (xlrec->nitems == 0)
		{
			Assert(replace);
			PageIndexTupleDelete(page, off);
			PageSetLSN(page, lsn);
			MarkBufferDirty(buffer);
		}
		else
		{
			char		itembuf[BLCKSZ + MAXIMUM_ALIGNOF];
			char	   *itembufp;
			Size		datasz;
			char	   *data;
			char	   *p;
			int			i;

			itembufp = (char *) MAXALIGN(itembuf);

			data = XLogRecGetBlockData(record, 0, &datasz);
			p = data;
			for (i = 0; i < xlrec->nitems; i++)
			{
				uint16		itemsz;

				/*
				 * Both NXTidArrayItem and NXAttributeArrayItem are laid out
				 * with t_size (uint16) as the first field. This is enforced
				 * by their struct definitions and relied upon here to read
				 * item sizes generically during WAL replay.
				 */
				memcpy(&itemsz, p, sizeof(uint16));
				Assert(itemsz > 0);
				Assert(itemsz < BLCKSZ);
				memcpy(itembufp, p, itemsz);
				p += itemsz;

				if (replace && i == 0)
				{
					if (!PageIndexTupleOverwrite(page, off, itembuf, itemsz))
						elog(ERROR, "could not replace item on noxu btree page at off %d", off);
				}
				else if (PageAddItem(page, itembufp, itemsz, off, false, false)
						 == InvalidOffsetNumber)
				{
					elog(ERROR, "could not add item to noxu btree page");
				}
				off++;
			}
			Assert((Size) (p - data) == datasz);

			PageSetLSN(page, lsn);
			MarkBufferDirty(buffer);
		}
	}
	if (BufferIsValid(buffer))
		UnlockReleaseBuffer(buffer);
	if (BufferIsValid(undobuf))
		UnlockReleaseBuffer(undobuf);
}

#define MAX_BLOCKS_IN_REWRITE		100

void
nxbt_wal_log_rewrite_pages(Relation rel, AttrNumber attno, List *buffers,
						   nx_pending_undo_op * undo_op,
						   uint32 recycle_bitmap, BlockNumber old_fpm_head,
						   Buffer metabuf)
{
	ListCell   *lc;
	XLogRecPtr	recptr;
	wal_noxu_btree_rewrite_pages xlrec;
	uint8		block_id;

	(void) rel;

	if (1 /* for undo */ + list_length(buffers) + (BufferIsValid(metabuf) ? 1 : 0) > MAX_BLOCKS_IN_REWRITE)
		elog(ERROR, "too many blocks for noxu rewrite_pages record: %d", list_length(buffers));

	xlrec.attno = attno;
	xlrec.numpages = list_length(buffers);
	xlrec.recycle_bitmap = recycle_bitmap;
	xlrec.old_fpm_head = old_fpm_head;

	XLogBeginInsert();

	/* Register ALL buffers first, before any data */
	if (undo_op)
		XLogRegisterUndoOp(0, undo_op);

	block_id = 1;
	foreach(lc, buffers)
	{
		Buffer		buf = (Buffer) lfirst_int(lc);
		uint8		flags = REGBUF_STANDARD | REGBUF_FORCE_IMAGE | REGBUF_KEEP_DATA;

		/*
		 * Pages being recycled are re-initialized as free pages, so use
		 * REGBUF_WILL_INIT for them during redo.
		 */
		if (recycle_bitmap & (1U << (block_id - 1)))
			flags = REGBUF_WILL_INIT | REGBUF_STANDARD;

		XLogRegisterBuffer(block_id, buf, flags);
		block_id++;
	}

	/* Register the metapage if we have recycle pages */
	if (BufferIsValid(metabuf))
	{
		XLogRegisterBuffer(block_id, metabuf, REGBUF_STANDARD);
		block_id++;
	}

	/* Now register data after all buffers are registered */
	XLogRegisterData((char *) &xlrec, SizeOfNXWalBtreeRewritePages);

	recptr = XLogInsert(RM_NOXU_ID, WAL_NOXU_BTREE_REWRITE_PAGES);

	if (undo_op)
		PageSetLSN(BufferGetPage(undo_op->reservation.undobuf), recptr);
	foreach(lc, buffers)
	{
		Buffer		buf = (Buffer) lfirst_int(lc);

		PageSetLSN(BufferGetPage(buf), recptr);
	}

	if (BufferIsValid(metabuf))
		PageSetLSN(BufferGetPage(metabuf), recptr);
}

void
nxbt_rewrite_pages_redo(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	wal_noxu_btree_rewrite_pages *xlrec = (wal_noxu_btree_rewrite_pages *) XLogRecGetData(record);
	Buffer		buffers[MAX_BLOCKS_IN_REWRITE];
	uint8		block_id;
	uint32		recycle_bitmap = xlrec->recycle_bitmap;
	int			numpages = xlrec->numpages;
	int			meta_block_id = -1;

	/* Initialize buffer array to prevent reading uninitialized memory */
	memset(buffers, 0, sizeof(buffers));

	if (XLogRecMaxBlockId(record) >= MAX_BLOCKS_IN_REWRITE)
		elog(ERROR, "too many blocks in noxu rewrite_pages record: %d", XLogRecMaxBlockId(record) + 1);

	/* Block 0: UNDO buffer */
	if (XLogRecHasBlockRef(record, 0))
		buffers[0] = XLogRedoUndoOp(record, 0);
	else
		buffers[0] = InvalidBuffer;

	/*
	 * Determine metapage block_id: the metapage is registered as the block
	 * after all b-tree pages (block numpages + 1) whenever the metabuf was
	 * valid during logging. Check if the block is actually present in the
	 * WAL record to determine if we need to process it.
	 */
	meta_block_id = numpages + 1;

	/* Restore b-tree page images */
	for (block_id = 1; block_id <= (uint8) numpages; block_id++)
	{
		if (recycle_bitmap & (1U << (block_id - 1)))
		{
			/*
			 * This page is being recycled. Initialize it as a free page.
			 * The page content was already set by nxpage_mark_page_deleted
			 * during normal operation; during redo we re-initialize it.
			 */
			buffers[block_id] = XLogInitBufferForRedo(record, block_id);
			{
				BlockNumber blk;
				BlockNumber next_free;
				Page		page = BufferGetPage(buffers[block_id]);
				int			bit_idx = block_id - 1;

				XLogRecGetBlockTag(record, block_id, NULL, NULL, &blk);

				/*
				 * Determine the nx_next for this free page. The first
				 * recycled page (lowest block_id) points to old_fpm_head.
				 * Subsequent recycled pages point to the previous recycled
				 * page's block number.  We chain them in the same order as
				 * the normal-path code does.
				 */
				next_free = xlrec->old_fpm_head;
				{
					int			j;

					for (j = 0; j < bit_idx; j++)
					{
						if (recycle_bitmap & (1U << j))
						{
							BlockNumber prev_blk;

							XLogRecGetBlockTag(record, j + 1, NULL, NULL, &prev_blk);
							next_free = prev_blk;
						}
					}
				}

				nxpage_mark_page_deleted(page, next_free);

				PageSetLSN(page, lsn);
				MarkBufferDirty(buffers[block_id]);
			}
		}
		else
		{
			if (XLogReadBufferForRedo(record, block_id, &buffers[block_id]) != BLK_RESTORED)
				elog(ERROR, "noxu rewrite_pages WAL record did not contain a full-page image");
		}
	}

	/* Redo metapage FPM head update if there were recycles */
	if (meta_block_id > 0 && XLogRecHasBlockRef(record, meta_block_id))
	{
		Buffer		metabuf;

		buffers[meta_block_id] = InvalidBuffer;
		if (XLogReadBufferForRedo(record, meta_block_id, &metabuf) == BLK_NEEDS_REDO)
		{
			Page		metapage = BufferGetPage(metabuf);
			NXMetaPageOpaque *metaopaque = (NXMetaPageOpaque *) PageGetSpecialPointer(metapage);
			BlockNumber new_fpm_head;

			/*
			 * The new FPM head is the last recycled page (highest block_id)
			 * since we chain them forward.
			 */
			{
				int			last_recycle_bit = -1;
				int			j;

				for (j = 0; j < numpages; j++)
				{
					if (recycle_bitmap & (1U << j))
						last_recycle_bit = j;
				}
				Assert(last_recycle_bit >= 0);
				XLogRecGetBlockTag(record, last_recycle_bit + 1, NULL, NULL, &new_fpm_head);
			}

			metaopaque->nx_fpm_head = new_fpm_head;

			PageSetLSN(metapage, lsn);
			MarkBufferDirty(metabuf);
		}
		buffers[meta_block_id] = metabuf;
	}

	/* Unlock and release all buffers */
	for (block_id = 0; block_id <= XLogRecMaxBlockId(record); block_id++)
	{
		if (BufferIsValid(buffers[block_id]))
			UnlockReleaseBuffer(buffers[block_id]);
	}
}
