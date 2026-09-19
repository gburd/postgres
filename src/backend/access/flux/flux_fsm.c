/*-------------------------------------------------------------------------
 *
 * flux_fsm.c
 *	  FLUX free space management
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/flux/flux_fsm.c
 *
 * NOTES
 *	  This is a thin wrapper over PostgreSQL's standard free space map
 *	  (src/backend/storage/freespace).  It adds FLUX-specific relation
 *	  extension (page initialization and WAL-logging of the new page) on
 *	  top of the generic GetPageWithFreeSpace/RecordPageWithFreeSpace API.
 *
 *	  Page defragmentation is handled entirely by VACUUM via
 *	  FluxPageDefragment() in flux_tuple.c; the FSM only tracks free
 *	  space, it does not schedule or perform compaction.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/flux.h"
#include "access/flux_xlog.h"
#include "storage/bufmgr.h"
#include "storage/freespace.h"
#include "utils/rel.h"
#include "miscadmin.h"

/*
 * FluxGetPageWithFreeSpace
 *
 * Find or create a page with at least 'needed' bytes of free space.
 *
 * First queries PostgreSQL's standard FSM (GetPageWithFreeSpace).  If a
 * page is returned, that block is used directly.
 *
 * If no suitable existing page is found, extends the relation by allocating
 * a new page with ExtendBufferedRel(), initializes it with FluxInitPage(),
 * WAL-logs the initialization, and returns the new block.
 *
 * Parameters:
 *   rel    - open relation
 *   needed - minimum number of free bytes required
 *
 * Returns the block number of a page with sufficient free space.
 */
BlockNumber
FluxGetPageWithFreeSpace(Relation rel, Size needed)
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
	FluxInitPage(page, BufferGetPageSize(buffer));

	START_CRIT_SECTION();

	MarkBufferDirty(buffer);

	/* Log page initialization */
	if (RelationNeedsWAL(rel))
	{
		uint64		init_commit_ts = FluxGetCommitTimestamp();
		FluxPageOpaque phdr;
		XLogRecPtr	recptr;

		/*
		 * Set the page's opaque data to match what the REDO handler will
		 * produce.  This is essential for WAL consistency checking: the page
		 * image stored with the WAL record must match what REDO generates
		 * when replaying.
		 */
		phdr = FluxPageGetOpaque(page);
		FluxPageSetCommitTs(phdr, init_commit_ts);

		recptr = FluxXLogInitPage(rel, buffer, 0, init_commit_ts);

		PageSetLSN(page, recptr);
	}

	END_CRIT_SECTION();

	/* Capture free space before releasing the buffer */
	free_space = PageGetFreeSpace(page);

	UnlockReleaseBuffer(buffer);

	/* Record the new page in FSM */
	FluxRecordFreeSpace(rel, target_block, free_space);

	/*
	 * Propagate the new FSM leaf value up through the FSM tree so that
	 * subsequent GetPageWithFreeSpace() calls can find it.  Without this, the
	 * root of the FSM tree remains at zero and all searches fail.
	 */
	FreeSpaceMapVacuumRange(rel, target_block, target_block + 1);

	return target_block;
}

/*
 * FluxRecordFreeSpace
 *
 * Update the FSM with the actual free space for a page.
 *
 * Parameters:
 *   rel       - open relation
 *   page      - block number of the page
 *   freespace - actual free space in bytes on the page
 */
void
FluxRecordFreeSpace(Relation rel, BlockNumber page, Size freespace)
{
	RecordPageWithFreeSpace(rel, page, freespace);
}

/*
 * FluxVacuumFSM
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
FluxVacuumFSM(Relation rel, BlockNumber new_nblocks)
{
	BlockNumber old_nblocks = RelationGetNumberOfBlocks(rel);

	if (new_nblocks < old_nblocks)
	{
		/* Truncated - update FSM */
		FreeSpaceMapPrepareTruncateRel(rel, new_nblocks);
	}
}
