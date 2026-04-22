/*-------------------------------------------------------------------------
 *
 * recno_fsm.c
 *	  RECNO free space management and page defragmentation
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/recno/recno_fsm.c
 *
 * NOTES
 *	  This implements advanced free space management for RECNO,
 *	  including efficient page allocation, defragmentation scheduling,
 *	  and space reclamation to minimize the need for VACUUM.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/recno.h"
#include "access/recno_xlog.h"
#include "storage/bufmgr.h"
#include "storage/freespace.h"
#include "storage/lmgr.h"
#include "storage/smgr.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "miscadmin.h"

/*
 * Free space management constants
 */
#define RECNO_FSM_CATEGORIES		5	/* Number of free space categories */
#define RECNO_DEFRAG_THRESHOLD		0.6 /* Defrag when page is 60% fragmented */
#define RECNO_DEFRAG_BATCH_SIZE		32	/* Max pages to defrag in one batch */
#define RECNO_MIN_FREE_PERCENT		0.1 /* Keep 10% free space on page */

/*
 * Free space category thresholds (as fraction of page size)
 */
static const double fsm_category_thresholds[RECNO_FSM_CATEGORIES] = {
	0.0,						/* FULL: 0% free */
	0.25,						/* 25% free */
	0.5,						/* 50% free */
	0.75,						/* 75% free */
	1.0							/* EMPTY: 100% free */
};

/*
 * Per-relation FSM state
 */
typedef struct RecnoFSMState
{
	Relation	rel;
	BlockNumber total_pages;
	BlockNumber *defrag_queue;
	int			defrag_queue_size;
	int			defrag_queue_head;
	int			defrag_queue_tail;
	MemoryContext fsm_context;
}			RecnoFSMState;

/* Forward declarations */
static RecnoFSMState * RecnoGetFSMState(Relation rel);
static int	RecnoClassifyFreeSpace(Size free_space, Size page_size);
static bool RecnoShouldDefragPage(Page page);
static void RecnoDefragmentPage(Relation rel, BlockNumber blockno);
static void RecnoCompactPage(Page page, RecnoOffsetMapping *mappings, int *nmappings);
static void RecnoScheduleDefrag(RecnoFSMState * fsm_state, BlockNumber blockno);
static BlockNumber RecnoGetNextDefragPage(RecnoFSMState * fsm_state);

/*
 * RecnoInitFSM
 *
 * Initialize the free space map for a RECNO relation by scanning every
 * existing page and recording its current free space category.  This is
 * called during relation creation or first access to ensure the FSM
 * accurately reflects the on-disk state.
 *
 * Parameters:
 *   rel - open relation to initialize FSM for
 */
void
RecnoInitFSM(Relation rel)
{
	BlockNumber nblocks;
	BlockNumber blkno;
	Buffer		buffer;
	Page		page;

	nblocks = RelationGetNumberOfBlocks(rel);

	/* Initialize free space map with actual page free space */
	for (blkno = 0; blkno < nblocks; blkno++)
	{
		buffer = ReadBufferExtended(rel, MAIN_FORKNUM, blkno,
									RBM_NORMAL, NULL);
		LockBuffer(buffer, BUFFER_LOCK_SHARE);

		page = BufferGetPage(buffer);
		RecordPageWithFreeSpace(rel, blkno, PageGetFreeSpace(page));

		LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
		ReleaseBuffer(buffer);
	}
}

/*
 * RecnoGetPageWithFreeSpace
 *
 * Find or create a page with at least 'needed' bytes of free space.
 *
 * First queries PostgreSQL's standard FSM (GetPageWithFreeSpace).  If the
 * returned page actually has enough space, returns it.  If the FSM is stale,
 * updates the FSM with the actual free space and falls through.
 *
 * If no suitable existing page is found, extends the relation by allocating
 * a new page with ReadBufferExtended(P_NEW), initializes it with
 * RecnoInitPage(), WAL-logs the initialization, and returns the new block.
 *
 * Parameters:
 *   rel    - open relation
 *   needed - minimum number of free bytes required
 *
 * Returns the block number of a page with sufficient free space.
 */
BlockNumber
RecnoGetPageWithFreeSpace(Relation rel, Size needed)
{
	BlockNumber target_block;
	Buffer		buffer;
	Page		page;
	Size		free_space;

	/*
	 * Ask the FSM for a page with enough free space.  Note: we do NOT verify
	 * the page by locking it here, because callers may already hold buffer
	 * locks (e.g., the update path holds the old tuple's page lock, and
	 * vacuum cross-page defrag holds the source page lock).  Locking a page
	 * here would risk self-deadlock if the FSM returns a block that the
	 * caller already has locked.  Callers are responsible for rechecking free
	 * space after they acquire their own lock on the returned page.
	 */
	target_block = GetPageWithFreeSpace(rel, needed);

	if (target_block != InvalidBlockNumber)
		return target_block;

	/*
	 * No suitable page found -- extend the relation.
	 *
	 * Use the modern ExtendBufferedRel() API which properly handles
	 * concurrent extension by multiple backends.  The old
	 * ReadBufferExtended(P_NEW) path had a race condition that caused
	 * BM_IO_IN_PROGRESS assertion failures under concurrency.
	 */
	buffer = ExtendBufferedRel(BMR_REL(rel), MAIN_FORKNUM, NULL,
							   EB_LOCK_FIRST);
	target_block = BufferGetBlockNumber(buffer);

	page = BufferGetPage(buffer);
	RecnoInitPage(page, BufferGetPageSize(buffer));

	START_CRIT_SECTION();

	MarkBufferDirty(buffer);

	/* Log page initialization */
	if (RelationNeedsWAL(rel))
	{
		uint64		init_commit_ts = RecnoGetCommitTimestamp();
		RecnoPageOpaque phdr;
		XLogRecPtr	recptr;

		/*
		 * Set the page's opaque data to match what the REDO handler will
		 * produce.  This is essential for WAL consistency checking: the page
		 * image stored with the WAL record must match what REDO generates
		 * when replaying.
		 */
		phdr = RecnoPageGetOpaque(page);
		phdr->pd_commit_ts = init_commit_ts;
		/* pd_flags stays 0 (matching what REDO sets from xlrec->flags) */

		recptr = RecnoXLogInitPage(rel, buffer, 0, init_commit_ts);

		PageSetLSN(page, recptr);
	}

	END_CRIT_SECTION();

	/* Capture free space before releasing the buffer */
	free_space = PageGetFreeSpace(page);

	UnlockReleaseBuffer(buffer);

	/* Record the new page in FSM */
	RecnoRecordFreeSpace(rel, target_block, free_space);

	/*
	 * Propagate the new FSM leaf value up through the FSM tree so that
	 * subsequent GetPageWithFreeSpace() calls can find it.  Without this, the
	 * root of the FSM tree remains at zero and all searches fail.
	 */
	FreeSpaceMapVacuumRange(rel, target_block, target_block + 1);

	return target_block;
}

/*
 * RecnoRecordFreeSpace
 *
 * Update the FSM with the actual free space for a page.  Also classifies
 * the page into one of 5 free space categories and checks whether the page
 * should be scheduled for defragmentation.
 *
 * A page is marked for defragmentation when it has some free space
 * (category > 0) but the total free space is less than 60% of the page
 * size (RECNO_DEFRAG_THRESHOLD), indicating internal fragmentation.
 *
 * Parameters:
 *   rel       - open relation
 *   page      - block number of the page
 *   freespace - actual free space in bytes on the page
 */
void
RecnoRecordFreeSpace(Relation rel, BlockNumber page, Size freespace)
{
	Size		page_size = BLCKSZ;
	int			category = RecnoClassifyFreeSpace(freespace, page_size);

	/*
	 * Record the actual free space in the FSM.  Do NOT call
	 * RecnoUpdateFSMCategory afterwards, since that overwrites the precise
	 * value with a coarser categorized approximation, which can cause
	 * GetPageWithFreeSpace to miss pages that actually have enough room.
	 */
	RecordPageWithFreeSpace(rel, page, freespace);

	/* Check if page needs defragmentation */
	if (category > 0 && freespace < page_size * RECNO_DEFRAG_THRESHOLD)
	{
		RecnoMarkPageForDefrag(rel, page);
	}
}

/*
 * RecnoMarkPageForDefrag
 *
 * Add a page to the defragmentation queue.  The page will be defragmented
 * during the next call to RecnoOpportunisticDefrag() or RecnoBatchDefrag().
 *
 * Parameters:
 *   rel  - open relation
 *   page - block number to schedule for defragmentation
 */
void
RecnoMarkPageForDefrag(Relation rel, BlockNumber page)
{
	RecnoFSMState *fsm_state = RecnoGetFSMState(rel);

	RecnoScheduleDefrag(fsm_state, page);
}

/*
 * Classify free space into categories
 */
static int
RecnoClassifyFreeSpace(Size free_space, Size page_size)
{
	double		free_ratio = (double) free_space / page_size;
	int			category;

	for (category = 0; category < RECNO_FSM_CATEGORIES - 1; category++)
	{
		if (free_ratio <= fsm_category_thresholds[category + 1])
			break;
	}

	return category;
}

/*
 * Get or create FSM state for relation
 */
static RecnoFSMState *
RecnoGetFSMState(Relation rel)
{
	RecnoFSMState *fsm_state;
	MemoryContext old_context;

	/* For simplicity, create a new state each time */
	/* In practice, this would be cached per relation */

	fsm_state = (RecnoFSMState *) palloc0(sizeof(RecnoFSMState));
	fsm_state->rel = rel;
	fsm_state->total_pages = RelationGetNumberOfBlocks(rel);

	fsm_state->fsm_context = AllocSetContextCreate(CurrentMemoryContext,
												   "RECNO FSM Context",
												   ALLOCSET_DEFAULT_SIZES);

	old_context = MemoryContextSwitchTo(fsm_state->fsm_context);

	fsm_state->defrag_queue = (BlockNumber *)
		palloc0(sizeof(BlockNumber) * RECNO_DEFRAG_BATCH_SIZE);
	fsm_state->defrag_queue_size = RECNO_DEFRAG_BATCH_SIZE;
	fsm_state->defrag_queue_head = 0;
	fsm_state->defrag_queue_tail = 0;

	MemoryContextSwitchTo(old_context);

	return fsm_state;
}

/*
 * Schedule a page for defragmentation
 */
static void
RecnoScheduleDefrag(RecnoFSMState * fsm_state, BlockNumber blockno)
{
	int			next_tail;

	/* Check if queue is full */
	next_tail = (fsm_state->defrag_queue_tail + 1) % fsm_state->defrag_queue_size;
	if (next_tail == fsm_state->defrag_queue_head)
	{
		/* Queue is full, process one page first */
		BlockNumber defrag_page = RecnoGetNextDefragPage(fsm_state);

		if (defrag_page != InvalidBlockNumber)
		{
			RecnoDefragmentPage(fsm_state->rel, defrag_page);
		}
	}

	/* Add page to defrag queue */
	fsm_state->defrag_queue[fsm_state->defrag_queue_tail] = blockno;
	fsm_state->defrag_queue_tail = next_tail;
}

/*
 * Get next page to defragment
 */
static BlockNumber
RecnoGetNextDefragPage(RecnoFSMState * fsm_state)
{
	BlockNumber blockno;

	if (fsm_state->defrag_queue_head == fsm_state->defrag_queue_tail)
		return InvalidBlockNumber;

	blockno = fsm_state->defrag_queue[fsm_state->defrag_queue_head];
	fsm_state->defrag_queue_head =
		(fsm_state->defrag_queue_head + 1) % fsm_state->defrag_queue_size;

	return blockno;
}

/*
 * Check if a page should be defragmented
 */
static bool
RecnoShouldDefragPage(Page page)
{
	Size		free_space = PageGetFreeSpace(page);
	Size		page_size = PageGetPageSize(page);
	OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
	int			live_tuples = 0;
	int			deleted_tuples = 0;
	OffsetNumber offnum;

	/* Count live and deleted tuples */
	for (offnum = FirstOffsetNumber; offnum <= maxoff; offnum++)
	{
		ItemId		itemid = PageGetItemId(page, offnum);

		if (ItemIdIsNormal(itemid))
		{
			RecnoTupleHeader *tuple = (RecnoTupleHeader *) PageGetItem(page, itemid);

			if (tuple->t_flags & RECNO_TUPLE_DELETED)
				deleted_tuples++;
			else
				live_tuples++;
		}
	}

	/* Defrag if we have deleted tuples and low free space */
	if (deleted_tuples > 0 && free_space < page_size * RECNO_DEFRAG_THRESHOLD)
		return true;

	/* Defrag if page is highly fragmented */
	if (live_tuples > 0 && (deleted_tuples * 2) > live_tuples)
		return true;

	return false;
}

/*
 * Defragment a page
 */
static void
RecnoDefragmentPage(Relation rel, BlockNumber blockno)
{
	Buffer		buffer;
	Page		page;
	RecnoOffsetMapping mappings[MaxOffsetNumber];
	int			nmappings = 0;

	buffer = ReadBufferExtended(rel, MAIN_FORKNUM, blockno,
								RBM_NORMAL, NULL);
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

	page = BufferGetPage(buffer);

	/* Check if defragmentation is still needed */
	if (!RecnoShouldDefragPage(page))
	{
		UnlockReleaseBuffer(buffer);
		return;
	}

	{
		uint64		defrag_ts;
		Size		defrag_free_space;

		/*
		 * Get timestamp BEFORE entering critical section, as this may
		 * allocate memory.
		 */
		defrag_ts = RecnoGetCommitTimestamp();

		START_CRIT_SECTION();

		/* Compact the page and track offset mappings */
		RecnoCompactPage(page, mappings, &nmappings);

		MarkBufferDirty(buffer);

		/* Log the defragmentation */
		if (RelationNeedsWAL(rel))
		{
			XLogRecPtr	recptr = RecnoXLogDefrag(rel, buffer, mappings,
												 nmappings, defrag_ts);

			PageSetLSN(page, recptr);
		}

		END_CRIT_SECTION();

		/* Capture free space before releasing buffer */
		defrag_free_space = PageGetFreeSpace(page);

		UnlockReleaseBuffer(buffer);

		/* Update FSM with new free space */
		RecnoRecordFreeSpace(rel, blockno, defrag_free_space);
	}
}

/*
 * Compact a page by removing deleted tuples and consolidating free space
 */
static void
RecnoCompactPage(Page page, RecnoOffsetMapping *mappings, int *nmappings)
{
	OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
	OffsetNumber offnum;
	OffsetNumber new_offnum = FirstOffsetNumber;
	RecnoPageOpaque phdr = RecnoPageGetOpaque(page);

	*nmappings = 0;

	/* First pass: identify live tuples and create mappings */
	for (offnum = FirstOffsetNumber; offnum <= maxoff; offnum++)
	{
		ItemId		itemid = PageGetItemId(page, offnum);

		if (ItemIdIsNormal(itemid))
		{
			RecnoTupleHeader *tuple = (RecnoTupleHeader *) PageGetItem(page, itemid);

			if (!(tuple->t_flags & RECNO_TUPLE_DELETED))
			{
				/* Live tuple - will be kept */
				if (offnum != new_offnum)
				{
					mappings[*nmappings].old_offnum = offnum;
					mappings[*nmappings].new_offnum = new_offnum;
					(*nmappings)++;
				}
				new_offnum++;
			}
		}
	}

	/* Use PageRepairFragmentation to do the actual compaction */
	PageRepairFragmentation(page);

	/* Update page header */
	phdr->pd_free_space = PageGetFreeSpace(page);
	phdr->pd_flags &= ~RECNO_PAGE_DEFRAG_NEEDED;
}

/*
 * RecnoOpportunisticDefrag
 *
 * Process up to 3 pages from the defragmentation queue during normal
 * operations.  This is called opportunistically (e.g., after DML
 * operations) to incrementally reduce fragmentation without requiring
 * a dedicated maintenance window.
 *
 * The FSM state is created fresh each call and cleaned up afterward.
 * In a production implementation, this state would be cached per-relation.
 *
 * Parameters:
 *   rel - open relation to defragment
 */
void
RecnoOpportunisticDefrag(Relation rel)
{
	RecnoFSMState *fsm_state = RecnoGetFSMState(rel);
	BlockNumber defrag_page;
	int			pages_defragged = 0;

	/* Process a few pages from defrag queue */
	while (pages_defragged < 3 &&
		   (defrag_page = RecnoGetNextDefragPage(fsm_state)) != InvalidBlockNumber)
	{
		RecnoDefragmentPage(rel, defrag_page);
		pages_defragged++;
	}

	/* Clean up FSM state */
	if (fsm_state->fsm_context)
		MemoryContextDelete(fsm_state->fsm_context);
	pfree(fsm_state);
}

/*
 * RecnoVacuumFSM
 *
 * Update the FSM after a relation truncation.  If the new block count is
 * smaller than the old count, calls FreeSpaceMapPrepareTruncateRel() to
 * remove FSM entries for the truncated pages.
 *
 * Parameters:
 *   rel         - open relation
 *   new_nblocks - new number of blocks after truncation
 */
void
RecnoVacuumFSM(Relation rel, BlockNumber new_nblocks)
{
	BlockNumber old_nblocks = RelationGetNumberOfBlocks(rel);

	if (new_nblocks < old_nblocks)
	{
		/* Truncated - update FSM */
		FreeSpaceMapPrepareTruncateRel(rel, new_nblocks);
	}
}

/*
 * RecnoGetFSMStats
 *
 * Scan the entire relation to compute free space statistics.  This is a
 * full sequential scan under shared buffer locks, intended for diagnostic
 * or monitoring purposes (not for hot paths).
 *
 * Parameters (all are output):
 *   rel            - open relation to examine
 *   total_pages    - total number of blocks in the relation
 *   free_pages     - number of blocks with any free space
 *   avg_free_space - average free space per block in bytes
 *   defrag_needed  - number of blocks that RecnoShouldDefragPage() returns
 *                    true for (pages with internal fragmentation)
 */
void
RecnoGetFSMStats(Relation rel, int64 *total_pages, int64 *free_pages,
				 double *avg_free_space, int64 *defrag_needed)
{
	BlockNumber nblocks;
	BlockNumber blkno;
	Buffer		buffer;
	Page		page;
	Size		free_space;
	int64		total_free_space = 0;
	int64		pages_needing_defrag = 0;

	*total_pages = 0;
	*free_pages = 0;
	*avg_free_space = 0.0;
	*defrag_needed = 0;

	nblocks = RelationGetNumberOfBlocks(rel);
	*total_pages = nblocks;

	for (blkno = 0; blkno < nblocks; blkno++)
	{
		buffer = ReadBufferExtended(rel, MAIN_FORKNUM, blkno,
									RBM_NORMAL, NULL);
		LockBuffer(buffer, BUFFER_LOCK_SHARE);

		page = BufferGetPage(buffer);
		free_space = PageGetFreeSpace(page);

		total_free_space += free_space;

		if (free_space > 0)
			(*free_pages)++;

		if (RecnoShouldDefragPage(page))
			pages_needing_defrag++;

		LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
		ReleaseBuffer(buffer);
	}

	if (nblocks > 0)
		*avg_free_space = (double) total_free_space / nblocks;

	*defrag_needed = pages_needing_defrag;
}

/*
 * RecnoBatchDefrag
 *
 * Scan the relation and defragment up to max_pages pages that need
 * compaction.  This is intended for maintenance operations (e.g., during
 * VACUUM) where processing multiple pages in one pass is acceptable.
 *
 * Pages are examined sequentially; each candidate is checked with
 * RecnoShouldDefragPage() and compacted with RecnoDefragmentPage().
 *
 * Parameters:
 *   rel       - open relation to defragment
 *   max_pages - maximum number of pages to defragment in this call
 */
void
RecnoBatchDefrag(Relation rel, int max_pages)
{
	BlockNumber nblocks = RelationGetNumberOfBlocks(rel);
	BlockNumber blkno;
	Buffer		buffer;
	Page		page;
	int			pages_defragged = 0;

	for (blkno = 0; blkno < nblocks && pages_defragged < max_pages; blkno++)
	{
		buffer = ReadBufferExtended(rel, MAIN_FORKNUM, blkno,
									RBM_NORMAL, NULL);
		LockBuffer(buffer, BUFFER_LOCK_SHARE);

		page = BufferGetPage(buffer);

		if (RecnoShouldDefragPage(page))
		{
			LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
			ReleaseBuffer(buffer);

			/* Defragment this page */
			RecnoDefragmentPage(rel, blkno);
			pages_defragged++;
		}
		else
		{
			LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
			ReleaseBuffer(buffer);
		}
	}
}
