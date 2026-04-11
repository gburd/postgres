/*-------------------------------------------------------------------------
 *
 * noxu_freepagemap.c
 *	  Noxu free space management
 *
 * The Free Page Map keeps track of unused pages in the relation.
 *
 * The FPM is a linked list of pages. Each page contains a pointer to the
 * next free page.

 * Design principles:
 *
 * - it's ok to have a block incorrectly stored in the FPM. Before actually
 *   reusing a page, we must check that it's safe.
 *
 * - a deletable page must be simple to detect just by looking at the page,
 *   and perhaps a few other pages. It should *not* require scanning the
 *   whole table, or even a whole b-tree. For example, if a column is dropped,
 *   we can detect if a b-tree page belongs to the dropped column just by
 *   looking at the information (the attribute number) stored in the page
 *   header.
 *
 * - if a page is deletable, it should become immediately reusable. No
 *   "wait out all possible readers that might be about to follow a link
 *   to it" business. All code that reads pages need to keep pages locked
 *   while following a link, or be prepared to retry if they land on an
 *   unexpected page.
 *
 *
 * Scalability and fragmentation mitigations:
 *
 * - Batch extension: nxpage_extendrel_newbuf() extends the relation by
 *   8-512 pages at once (adaptive based on relation size), adding the
 *   extras to the FPM.  This amortizes extension lock overhead and
 *   improves spatial locality for new B-tree pages.
 *
 * - Deferred deallocation: nxpage_delete_page() enqueues freed pages
 *   in a per-backend queue (NXDeallocQueueEntry) rather than immediately
 *   acquiring the metapage EXCLUSIVE lock.  The queue is flushed at
 *   commit time by nxfpm_flush_dealloc_queue(), reducing metapage lock
 *   contention from O(N) to O(R) where R is the number of distinct
 *   relations that had pages freed.
 *
 * - Batch allocation cache: nxpage_getnewbuf() pre-pops up to
 *   NX_ALLOC_CACHE_SIZE (8) extra block numbers from the FPM linked list
 *   into a backend-local cache (NXAllocCache) while already holding the
 *   metapage lock.  Subsequent allocation requests for the same relation
 *   are served from this cache without re-acquiring the metapage lock.
 *   Unused cached blocks are returned to the FPM at transaction end.
 *   This reduces allocation-side metapage lock acquisitions from O(N) to
 *   O(N/8) for tight allocation loops such as B-tree page splits.
 *
 * Known limitation: the FPM is LIFO, so pages are not handed out in
 * physical order.  A future improvement could sort free pages by block
 * number, or maintain per-attribute free lists, to improve I/O
 * readahead effectiveness when B-tree pages for the same attribute are
 * physically contiguous.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/noxu/noxu_freepagemap.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "access/xact.h"
#include "access/xloginsert.h"
#include "access/xlogreader.h"
#include "access/xlogutils.h"
#include "access/noxu_internal.h"
#include "access/noxu_wal.h"
#include "miscadmin.h"
#include "storage/bufpage.h"
#include "storage/relfilelocator.h"
#include "storage/smgr.h"
#include "utils/memutils.h"
#include "utils/rel.h"

typedef struct NXFreePageOpaque
{
	BlockNumber nx_next;
	uint16		padding;
	uint16		nx_page_id;		/* NX_FREE_PAGE_ID */
}			NXFreePageOpaque;

/*
 * Deallocation queue for batching page frees.
 *
 * Instead of acquiring the metapage EXCLUSIVE lock for every individual page
 * deletion, we accumulate (RelFileLocator, BlockNumber) pairs in a
 * transaction-local queue and flush them all at commit time.  This reduces
 * metapage lock contention from O(N) acquisitions to O(R) where R is the
 * number of distinct relations that had pages freed.
 *
 * The queue is allocated in TopTransactionContext so it survives until
 * commit/abort.  On commit we batch-prepend all queued pages to the FPM
 * linked list.  On abort we simply discard the queue -- the pages were
 * never linked into the FPM, so they remain allocated (which is correct
 * since the deleting transaction rolled back).
 */
typedef struct NXDeallocQueueEntry
{
	RelFileLocator locator;		/* identifies the relation */
	BlockNumber blkno;			/* page to add to FPM */
	bool		needs_wal;		/* does this relation need WAL? */
	struct NXDeallocQueueEntry *next;
}			NXDeallocQueueEntry;

/* Head of the per-backend deallocation queue (transaction-local) */
static NXDeallocQueueEntry *nxfpm_dealloc_queue = NULL;

/* Have we registered the transaction callback yet? */
static bool nxfpm_xact_callback_registered = false;

static void nxfpm_flush_dealloc_queue(void);
static void nxfpm_xact_callback(XactEvent event, void *arg);

/*
 * Allocation cache for batching page allocations.
 *
 * When nxpage_getnewbuf() pops a page from the FPM linked list, it also
 * walks ahead and caches the next few block numbers.  Subsequent allocation
 * requests for the same relation are served from this cache without
 * re-acquiring the metapage EXCLUSIVE lock, reducing contention from O(N)
 * to O(N/BATCH) for tight allocation loops (e.g. B-tree page splits).
 *
 * The cache holds at most NX_ALLOC_CACHE_SIZE block numbers per relation,
 * and we only keep one relation's worth of blocks cached at a time (to
 * limit pin/leak exposure).  When a request comes in for a different
 * relation, any remaining cached blocks are returned to the FPM.
 *
 * If the backend exits without using all cached blocks, they become
 * "leaked" -- they are no longer in the FPM list but aren't used by any
 * B-tree.  Such blocks are still marked NX_FREE_PAGE_ID on disk, so a
 * future VACUUM or relation-wide scan can reclaim them.
 */
#define NX_ALLOC_CACHE_SIZE		8

typedef struct NXAllocCache
{
	RelFileLocator locator;		/* which relation these blocks belong to */
	bool		valid;			/* is this cache populated? */
	int			count;			/* number of cached blocks */
	BlockNumber blocks[NX_ALLOC_CACHE_SIZE];
}			NXAllocCache;

static NXAllocCache nxfpm_alloc_cache = {.valid = false, .count = 0};

static void nxfpm_return_cached_blocks(RelFileLocator locator);
static BlockNumber nxfpm_cache_pop(RelFileLocator locator);

/*
 * nxfpm_xact_callback - Transaction callback for the deallocation queue.
 *
 * On commit: flush all queued pages into their respective FPM lists.
 * On abort: discard the queue (memory is freed when TopTransactionContext
 * is reset, but we NULL out the head pointer for safety).
 */
static void
nxfpm_xact_callback(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_PRE_COMMIT:
		case XACT_EVENT_PARALLEL_PRE_COMMIT:
		case XACT_EVENT_PRE_PREPARE:
			/*
			 * Return any cached allocation blocks to their FPM before
			 * flushing the deallocation queue.  This ensures that pages
			 * cached but unused by this transaction aren't leaked.
			 */
			if (nxfpm_alloc_cache.valid && nxfpm_alloc_cache.count > 0)
				nxfpm_return_cached_blocks(nxfpm_alloc_cache.locator);
			nxfpm_alloc_cache.valid = false;
			nxfpm_alloc_cache.count = 0;

			nxfpm_flush_dealloc_queue();
			break;

		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:
			/*
			 * On abort, return cached allocation blocks to FPM and discard
			 * the deallocation queue.
			 */
			if (nxfpm_alloc_cache.valid && nxfpm_alloc_cache.count > 0)
				nxfpm_return_cached_blocks(nxfpm_alloc_cache.locator);
			nxfpm_alloc_cache.valid = false;
			nxfpm_alloc_cache.count = 0;

			nxfpm_dealloc_queue = NULL;
			break;

		case XACT_EVENT_COMMIT:
		case XACT_EVENT_PARALLEL_COMMIT:
		case XACT_EVENT_PREPARE:
			/*
			 * Post-commit cleanup.  The queue should have been flushed
			 * during PRE_COMMIT.  Explicitly NULL the pointer to prevent
			 * dangling references into the now-reset TopTransactionContext.
			 */
			nxfpm_dealloc_queue = NULL;
			nxfpm_alloc_cache.valid = false;
			nxfpm_alloc_cache.count = 0;
			break;

		default:
			break;
	}
}

/*
 * nxfpm_return_cached_blocks - Return unused cached blocks to the FPM.
 *
 * Re-prepends cached block numbers to the FPM linked list.  Called at
 * transaction end or when switching to a different relation's cache.
 */
static void
nxfpm_return_cached_blocks(RelFileLocator locator)
{
	Buffer		metabuf;
	Page		metapage;
	NXMetaPageOpaque *metaopaque;
	BlockNumber fpm_head;
	BlockNumber nblocks;
	SMgrRelation srel;
	int			i;

	if (nxfpm_alloc_cache.count == 0)
		return;

	/*
	 * Get the current relation size to skip blocks that no longer exist.
	 * This can happen when a concurrent UNDO worker frees pages and the
	 * relation is implicitly truncated.
	 */
	srel = smgropen(locator, INVALID_PROC_NUMBER);
	if (!smgrexists(srel, MAIN_FORKNUM))
	{
		nxfpm_alloc_cache.count = 0;
		return;
	}
	nblocks = smgrnblocks(srel, MAIN_FORKNUM);

	metabuf = ReadBufferWithoutRelcache(locator,
										MAIN_FORKNUM,
										NX_META_BLK,
										RBM_NORMAL,
										NULL,
										true);
	LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);
	metapage = BufferGetPage(metabuf);
	metaopaque = (NXMetaPageOpaque *) PageGetSpecialPointer(metapage);
	fpm_head = metaopaque->nx_fpm_head;

	for (i = 0; i < nxfpm_alloc_cache.count; i++)
	{
		Buffer		pagebuf;
		Page		page;
		BlockNumber old_fpm_head = fpm_head;
		BlockNumber blk = nxfpm_alloc_cache.blocks[i];

		/* Skip blocks beyond the current relation size */
		if (blk >= nblocks)
			continue;

		pagebuf = ReadBufferWithoutRelcache(locator,
											MAIN_FORKNUM,
											blk,
											RBM_NORMAL,
											NULL,
											true);
		LockBuffer(pagebuf, BUFFER_LOCK_EXCLUSIVE);

		START_CRIT_SECTION();

		page = BufferGetPage(pagebuf);
		nxpage_mark_page_deleted(page, old_fpm_head);
		fpm_head = blk;

		MarkBufferDirty(pagebuf);

		END_CRIT_SECTION();

		UnlockReleaseBuffer(pagebuf);
	}

	metaopaque->nx_fpm_head = fpm_head;
	MarkBufferDirty(metabuf);
	UnlockReleaseBuffer(metabuf);

	nxfpm_alloc_cache.count = 0;
}

/*
 * nxfpm_cache_pop - Pop a block number from the allocation cache.
 *
 * Returns a cached BlockNumber for the given relation, or
 * InvalidBlockNumber if the cache is empty or belongs to a different
 * relation.
 */
static BlockNumber
nxfpm_cache_pop(RelFileLocator locator)
{
	if (!nxfpm_alloc_cache.valid ||
		nxfpm_alloc_cache.count == 0 ||
		!RelFileLocatorEquals(nxfpm_alloc_cache.locator, locator))
		return InvalidBlockNumber;

	nxfpm_alloc_cache.count--;
	return nxfpm_alloc_cache.blocks[nxfpm_alloc_cache.count];
}

/*
 * nxpage_is_unused()
 *
 * Is the current page recyclable?
 *
 * Conceptually a page is recyclable if it is any of:
 *   - an empty, all-zeros page
 *   - explicitly marked as deleted (NX_FREE_PAGE_ID)
 *   - an UNDO page older than oldest_undo_ptr
 *   - a B-tree page belonging to a deleted attribute
 *   - an overflow page belonging to a dead item
 *
 * Currently only the explicit NX_FREE_PAGE_ID mark is checked.  Callers
 * (nxbt_unlink_page, nxpage_delete_page) must mark pages with
 * nxpage_mark_page_deleted() before adding them to the FPM.  Detecting
 * the other recyclable cases would require cross-referencing with the
 * UNDO log or attribute metadata, which is more expensive than the
 * simple page-header check used here.
 */
static bool
nxpage_is_unused(Buffer buf)
{
	Page		page;
	NXFreePageOpaque *opaque;

	page = BufferGetPage(buf);

	if (PageIsNew(page))
		return false;

	if (PageGetSpecialSize(page) != sizeof(NXFreePageOpaque))
		return false;
	opaque = (NXFreePageOpaque *) PageGetSpecialPointer(page);
	if (opaque->nx_page_id != NX_FREE_PAGE_ID)
		return false;

	return true;
}

/*
 * Allocate a new page.
 *
 * The page is exclusive-locked, but not initialized.
 *
 * When the caller does not provide a metabuf (metabuf == InvalidBuffer),
 * this function first checks a backend-local allocation cache.  If the
 * cache has a block for this relation, we use it directly without touching
 * the metapage.  When the cache is empty (or belongs to a different
 * relation), we acquire the metapage lock, pop the first block for
 * immediate return, and walk the FPM linked list to pre-pop up to
 * NX_ALLOC_CACHE_SIZE additional blocks into the cache.  This reduces
 * metapage lock acquisitions during tight allocation loops (e.g. B-tree
 * page splits that allocate multiple pages in quick succession).
 *
 * When the caller provides a locked metabuf, we skip the cache (the
 * caller needs the metapage lock held across the allocation anyway)
 * but still cache extra blocks for future calls.
 */
Buffer
nxpage_getnewbuf(Relation rel, Buffer metabuf)
{
	bool		release_metabuf;
	Buffer		buf;
	BlockNumber blk;
	Page		metapage;
	NXMetaPageOpaque *metaopaque;

	/* Register the transaction callback if not already done */
	if (!nxfpm_xact_callback_registered)
	{
		RegisterXactCallback(nxfpm_xact_callback, NULL);
		nxfpm_xact_callback_registered = true;
	}

	/*
	 * Fast path: try the allocation cache first.  Only when the caller did
	 * not provide a metabuf -- callers that pass metabuf need the metapage
	 * locked across the allocation for their own purposes.
	 */
	if (metabuf == InvalidBuffer)
	{
		/*
		 * If the cache belongs to a different relation, return those blocks
		 * first.
		 */
		if (nxfpm_alloc_cache.valid && nxfpm_alloc_cache.count > 0 &&
			!RelFileLocatorEquals(nxfpm_alloc_cache.locator, rel->rd_locator))
		{
			nxfpm_return_cached_blocks(nxfpm_alloc_cache.locator);
			nxfpm_alloc_cache.valid = false;
		}

		blk = nxfpm_cache_pop(rel->rd_locator);
		if (blk != InvalidBlockNumber)
		{
			buf = ReadBuffer(rel, blk);
			LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

			/* Verify the page is still unused (safety check). */
			if (nxpage_is_unused(buf))
				return buf;

			/*
			 * Page was unexpectedly reused (shouldn't happen in normal
			 * operation).  Fall through to the normal allocation path.
			 */
			UnlockReleaseBuffer(buf);
		}
	}

	if (metabuf == InvalidBuffer)
	{
		metabuf = ReadBuffer(rel, NX_META_BLK);
		LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);
		release_metabuf = true;
	}
	else
		release_metabuf = false;

	metapage = BufferGetPage(metabuf);
	metaopaque = (NXMetaPageOpaque *) PageGetSpecialPointer(metapage);

	/* Get a block from the FPM. */
	blk = metaopaque->nx_fpm_head;
	if (blk == 0)
	{
		/* metapage, not expected */
		elog(ERROR, "could not find valid page in FPM");
	}
	if (blk == InvalidBlockNumber)
	{
		/* No free pages. Have to extend the relation. */
		buf = nxpage_extendrel_newbuf(rel, metabuf);
		blk = BufferGetBlockNumber(buf);
	}
	else
	{
		NXFreePageOpaque *opaque;
		Page		page;
		BlockNumber walk_blk;

		buf = ReadBuffer(rel, blk);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

		/* Check that the page really is unused. */
		if (!nxpage_is_unused(buf))
		{
			UnlockReleaseBuffer(buf);
			elog(ERROR, "unexpected page found in free page list");
		}
		page = BufferGetPage(buf);
		opaque = (NXFreePageOpaque *) PageGetSpecialPointer(page);
		walk_blk = opaque->nx_next;

		/*
		 * Pre-populate the allocation cache by walking ahead in the FPM
		 * linked list.  We read each subsequent free page, verify it, and
		 * record its block number.  We update nx_fpm_head to skip past all
		 * cached blocks, so we only need to read/lock the metapage once.
		 *
		 * We only cache blocks we can verify as NX_FREE_PAGE_ID.  If a
		 * page fails verification, we stop caching but still update
		 * nx_fpm_head to the last good position.
		 */
		nxfpm_alloc_cache.count = 0;
		nxfpm_alloc_cache.locator = rel->rd_locator;
		nxfpm_alloc_cache.valid = true;

		while (nxfpm_alloc_cache.count < NX_ALLOC_CACHE_SIZE &&
			   walk_blk != InvalidBlockNumber &&
			   walk_blk != 0)
		{
			Buffer		cachebuf;
			Page		cachepage;
			NXFreePageOpaque *cacheopaque;

			cachebuf = ReadBuffer(rel, walk_blk);
			LockBuffer(cachebuf, BUFFER_LOCK_EXCLUSIVE);

			if (!nxpage_is_unused(cachebuf))
			{
				UnlockReleaseBuffer(cachebuf);
				break;
			}

			cachepage = BufferGetPage(cachebuf);
			cacheopaque = (NXFreePageOpaque *) PageGetSpecialPointer(cachepage);

			nxfpm_alloc_cache.blocks[nxfpm_alloc_cache.count++] = walk_blk;
			walk_blk = cacheopaque->nx_next;

			UnlockReleaseBuffer(cachebuf);
		}

		/* Update FPM head to skip past both the returned block and cached blocks */
		metaopaque->nx_fpm_head = walk_blk;
		MarkBufferDirty(metabuf);
	}

	if (release_metabuf)
		UnlockReleaseBuffer(metabuf);
	return buf;
}

/*
 * Extend the relation.
 *
 * Returns the new page, exclusive-locked. Also extends by additional pages
 * to reduce extension lock contention and improve spatial locality.
 */
Buffer
nxpage_extendrel_newbuf(Relation rel, Buffer metabuf)
{
	Buffer		buf;
	Buffer		local_metabuf = InvalidBuffer;
	bool		release_metabuf = false;
	Page		metapage;
	NXMetaPageOpaque *metaopaque;
	int			num_extra_pages;
	uint32		i;

	/*
	 * Determine how many extra pages to allocate. For smaller relations,
	 * allocate fewer pages. For larger relations (>1GB), allocate more
	 * pages at once to reduce lock contention.
	 */
	{
		BlockNumber nblocks = RelationGetNumberOfBlocks(rel);

		if (nblocks < 1280)		/* < 10MB */
			num_extra_pages = 8;
		else if (nblocks < 12800)	/* < 100MB */
			num_extra_pages = 32;
		else if (nblocks < 128000)	/* < 1GB */
			num_extra_pages = 128;
		else
			num_extra_pages = 512;	/* Large tables benefit most from
									 * batching */
	}

	/*
	 * Use ExtendBufferedRelBy to extend the relation by multiple pages at once.
	 * This is the modern API that properly handles buffer locking and extension.
	 * We extend by (1 + num_extra_pages) pages total: the first page is what
	 * we'll return to the caller, and the extra pages are added to the FPM.
	 */
	{
		Buffer		buffers[513];	/* 1 main + up to 512 extra */
		uint32		extend_by = 1 + num_extra_pages;
		uint32		extended_by = extend_by;
		uint32		flags = EB_LOCK_FIRST;

		/* Skip extension lock for local relations */
		if (RELATION_IS_LOCAL(rel))
			flags |= EB_SKIP_EXTENSION_LOCK;

		/* Extend the relation */
		ExtendBufferedRelBy(BMR_REL(rel),
							MAIN_FORKNUM,
							NULL,		/* strategy */
							flags,
							extend_by,
							buffers,
							&extended_by);

		/* First buffer is returned locked */
		buf = buffers[0];

		/*
		 * Add the extra pages to the free page map.
		 * This amortizes the cost of extension locks and improves spatial
		 * locality.
		 */
		if (extended_by > 1)
		{
			/* Get the metapage to update the FPM */
			if (metabuf == InvalidBuffer)
			{
				local_metabuf = ReadBuffer(rel, NX_META_BLK);
				LockBuffer(local_metabuf, BUFFER_LOCK_EXCLUSIVE);
				release_metabuf = true;
			}
			else
			{
				/* Caller already has metabuf locked */
				local_metabuf = metabuf;
				release_metabuf = false;
			}
			metapage = BufferGetPage(local_metabuf);
			metaopaque = (NXMetaPageOpaque *) PageGetSpecialPointer(metapage);

			for (i = 1; i < extended_by; i++)
			{
				Buffer		extrabuf = buffers[i];
				Page		page;
				BlockNumber extrablk;
				BlockNumber old_fpm_head;

				/*
				 * The extra buffers are pinned but not locked by
				 * ExtendBufferedRelBy. We need to lock them to initialize.
				 */
				extrablk = BufferGetBlockNumber(extrabuf);
				LockBuffer(extrabuf, BUFFER_LOCK_EXCLUSIVE);

				old_fpm_head = metaopaque->nx_fpm_head;

				START_CRIT_SECTION();

				/* Mark it as free and add to the FPM linked list */
				page = BufferGetPage(extrabuf);
				nxpage_mark_page_deleted(page, old_fpm_head);
				MarkBufferDirty(extrabuf);

				/* Update FPM head to point to this new free page */
				metaopaque->nx_fpm_head = extrablk;
				MarkBufferDirty(local_metabuf);

				if (RelationNeedsWAL(rel))
				{
					wal_noxu_fpm_delete xlrec;
					XLogRecPtr	recptr;

					xlrec.old_fpm_head = old_fpm_head;

					XLogBeginInsert();

					/* Register ALL buffers first, before any data */
					XLogRegisterBuffer(0, local_metabuf, REGBUF_STANDARD);
					XLogRegisterBuffer(1, extrabuf, REGBUF_WILL_INIT | REGBUF_STANDARD);

					/* Now register data after buffers are registered */
					XLogRegisterData((char *) &xlrec, SizeOfNXWalFpmDelete);

					recptr = XLogInsert(RM_NOXU_ID, WAL_NOXU_FPM_DELETE);

					PageSetLSN(metapage, recptr);
					PageSetLSN(page, recptr);
				}

				END_CRIT_SECTION();

				UnlockReleaseBuffer(extrabuf);
			}

			if (release_metabuf)
				UnlockReleaseBuffer(local_metabuf);
		}
	}

	return buf;
}

void
nxpage_mark_page_deleted(Page page, BlockNumber next_free_blk)
{
	NXFreePageOpaque *opaque;

	PageInit(page, BLCKSZ, sizeof(NXFreePageOpaque));
	opaque = (NXFreePageOpaque *) PageGetSpecialPointer(page);
	opaque->nx_page_id = NX_FREE_PAGE_ID;
	opaque->nx_next = next_free_blk;

}

/*
 * Explicitly mark a page as deleted and recyclable, and add it to the FPM.
 *
 * The caller must hold an exclusive-lock on the page.
 *
 * Rather than immediately acquiring the metapage lock and prepending the page
 * to the FPM linked list, we mark the page as deleted (with a temporary
 * nx_next of InvalidBlockNumber) and enqueue it for batch processing at
 * commit time.  This reduces metapage lock contention when many pages are
 * freed in a single transaction.
 *
 * The page is marked dirty here so that its deleted state is durable even
 * before the FPM linkage is established.  The FPM head update and proper
 * nx_next chaining happen in nxfpm_flush_dealloc_queue() at commit time.
 */
void
nxpage_delete_page(Relation rel, Buffer buf)
{
	BlockNumber blk = BufferGetBlockNumber(buf);
	Page		page;
	NXDeallocQueueEntry *entry;
	MemoryContext oldcxt;

	/* Register the transaction callback on first use */
	if (!nxfpm_xact_callback_registered)
	{
		RegisterXactCallback(nxfpm_xact_callback, NULL);
		nxfpm_xact_callback_registered = true;
	}

	/*
	 * Mark the page as deleted immediately.  We use InvalidBlockNumber as
	 * the nx_next placeholder; the real chaining is done at commit time.
	 */
	page = BufferGetPage(buf);
	nxpage_mark_page_deleted(page, InvalidBlockNumber);
	MarkBufferDirty(buf);

	/* Enqueue for batch FPM insertion at commit */
	oldcxt = MemoryContextSwitchTo(TopTransactionContext);
	entry = palloc(sizeof(NXDeallocQueueEntry));
	entry->locator = rel->rd_locator;
	entry->blkno = blk;
	entry->needs_wal = RelationNeedsWAL(rel);
	entry->next = nxfpm_dealloc_queue;
	nxfpm_dealloc_queue = entry;
	MemoryContextSwitchTo(oldcxt);
}

/*
 * nxfpm_flush_dealloc_queue - Batch-flush all queued page deletions.
 *
 * Called from the pre-commit transaction callback.  For each distinct
 * relation in the queue, we acquire the metapage lock once and chain all
 * queued pages into the FPM linked list.  Each page still gets its own
 * WAL record (WAL_NOXU_FPM_DELETE) for correct redo behavior, but the
 * metapage lock is held across all pages for that relation, turning N
 * lock acquire/release cycles into 1.
 */
static void
nxfpm_flush_dealloc_queue(void)
{
	NXDeallocQueueEntry *entry;
	NXDeallocQueueEntry *next;

	while (nxfpm_dealloc_queue != NULL)
	{
		RelFileLocator cur_locator;
		Buffer		metabuf;
		Page		metapage;
		NXMetaPageOpaque *metaopaque;

		/*
		 * Pick the first entry's relation and process all entries for that
		 * same relation in one batch.
		 */
		cur_locator = nxfpm_dealloc_queue->locator;

		{
			Buffer		pagebuf;
			BlockNumber fpm_head;
			BlockNumber nblocks;
			SMgrRelation srel;
			bool		needs_wal;

			/*
			 * Get the current relation size to skip blocks that no longer
			 * exist (e.g., freed by a concurrent UNDO worker).
			 */
			srel = smgropen(cur_locator, INVALID_PROC_NUMBER);
			if (!smgrexists(srel, MAIN_FORKNUM))
			{
				/* Relation gone; discard all entries for it */
				entry = nxfpm_dealloc_queue;
				nxfpm_dealloc_queue = NULL;
				while (entry != NULL)
				{
					next = entry->next;
					if (!RelFileLocatorEquals(entry->locator, cur_locator))
					{
						entry->next = nxfpm_dealloc_queue;
						nxfpm_dealloc_queue = entry;
					}
					entry = next;
				}
				continue;
			}
			nblocks = smgrnblocks(srel, MAIN_FORKNUM);

			/* Read and lock the metapage */
			metabuf = ReadBufferWithoutRelcache(cur_locator,
												MAIN_FORKNUM,
												NX_META_BLK,
												RBM_NORMAL,
												NULL,
												true);
			LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);
			metapage = BufferGetPage(metabuf);
			metaopaque = (NXMetaPageOpaque *) PageGetSpecialPointer(metapage);
			fpm_head = metaopaque->nx_fpm_head;

			/*
			 * Walk the queue and process all entries matching cur_locator.
			 * Unmatched entries are left in the queue for later processing.
			 */
			{
				NXDeallocQueueEntry *remaining = NULL;
				NXDeallocQueueEntry **remaining_tail = &remaining;

				entry = nxfpm_dealloc_queue;
				nxfpm_dealloc_queue = NULL;

				while (entry != NULL)
				{
					next = entry->next;

					if (RelFileLocatorEquals(entry->locator, cur_locator))
					{
						BlockNumber old_fpm_head = fpm_head;
						Page		page;

						needs_wal = entry->needs_wal;

						/* Skip blocks beyond the current relation size */
						if (entry->blkno >= nblocks)
						{
							entry = next;
							continue;
						}

						/* Re-read the page to update its nx_next pointer */
						pagebuf = ReadBufferWithoutRelcache(cur_locator,
														   MAIN_FORKNUM,
														   entry->blkno,
														   RBM_NORMAL,
														   NULL,
														   true);
						LockBuffer(pagebuf, BUFFER_LOCK_EXCLUSIVE);

						START_CRIT_SECTION();

						page = BufferGetPage(pagebuf);
						nxpage_mark_page_deleted(page, old_fpm_head);
						fpm_head = entry->blkno;

						metaopaque->nx_fpm_head = fpm_head;
						MarkBufferDirty(metabuf);
						MarkBufferDirty(pagebuf);

						if (needs_wal)
						{
							wal_noxu_fpm_delete xlrec;
							XLogRecPtr	recptr;

							xlrec.old_fpm_head = old_fpm_head;

							XLogBeginInsert();
							XLogRegisterBuffer(0, metabuf, REGBUF_STANDARD);
							XLogRegisterBuffer(1, pagebuf, REGBUF_WILL_INIT | REGBUF_STANDARD);
							XLogRegisterData((char *) &xlrec, SizeOfNXWalFpmDelete);

							recptr = XLogInsert(RM_NOXU_ID, WAL_NOXU_FPM_DELETE);

							PageSetLSN(metapage, recptr);
							PageSetLSN(page, recptr);
						}

						END_CRIT_SECTION();

						UnlockReleaseBuffer(pagebuf);
						/* entry memory freed when TopTransactionContext resets */
					}
					else
					{
						/* Keep entries for other relations */
						entry->next = NULL;
						*remaining_tail = entry;
						remaining_tail = &entry->next;
					}

					entry = next;
				}

				nxfpm_dealloc_queue = remaining;
			}

			UnlockReleaseBuffer(metabuf);
		}
	}
}

/*
 * nxfpm_flush_pending_deletes - Public interface to flush the deallocation queue.
 *
 * This can be called explicitly within a transaction to ensure that all
 * queued page deletions are flushed to the FPM before allocating new pages
 * from the same relation.  Normally the queue is flushed automatically at
 * commit time via the transaction callback.
 */
void
nxfpm_flush_pending_deletes(void)
{
	nxfpm_flush_dealloc_queue();
}

/*
 * WAL redo for WAL_NOXU_FPM_DELETE.
 *
 * blkref #0: the metapage (update nx_fpm_head)
 * blkref #1: the freed page (re-initialize as free page)
 */
void
nxfpm_delete_redo(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	wal_noxu_fpm_delete *xlrec = (wal_noxu_fpm_delete *) XLogRecGetData(record);
	BlockNumber old_fpm_head = xlrec->old_fpm_head;
	Buffer		metabuf;
	Buffer		freebuf;
	BlockNumber freeblk;

	XLogRecGetBlockTag(record, 1, NULL, NULL, &freeblk);

	if (XLogReadBufferForRedo(record, 0, &metabuf) == BLK_NEEDS_REDO)
	{
		Page		metapage = BufferGetPage(metabuf);
		NXMetaPageOpaque *metaopaque;

		metaopaque = (NXMetaPageOpaque *) PageGetSpecialPointer(metapage);
		metaopaque->nx_fpm_head = freeblk;

		PageSetLSN(metapage, lsn);
		MarkBufferDirty(metabuf);
	}

	/* The freed page is always re-initialized */
	freebuf = XLogInitBufferForRedo(record, 1);
	{
		Page		freepage = BufferGetPage(freebuf);

		nxpage_mark_page_deleted(freepage, old_fpm_head);

		PageSetLSN(freepage, lsn);
		MarkBufferDirty(freebuf);
	}

	if (BufferIsValid(metabuf))
		UnlockReleaseBuffer(metabuf);
	UnlockReleaseBuffer(freebuf);
}
