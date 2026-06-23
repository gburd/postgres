/*-------------------------------------------------------------------------
 *
 * relundo_xlog.c
 *	  Per-relation UNDO resource manager WAL redo routines
 *
 * This module implements the WAL redo callback for the RM_RELUNDO_ID
 * resource manager.  It handles replay of:
 *
 *   XLOG_RELUNDO_INIT    - Replay metapage initialization
 *   XLOG_RELUNDO_INSERT  - Replay UNDO record insertion into a data page
 *   XLOG_RELUNDO_DISCARD - Replay discard of old UNDO pages
 *
 * Redo Strategy
 * -------------
 * INIT and DISCARD use full page images (FPI) via XLogInitBufferForRedo()
 * or REGBUF_FORCE_IMAGE, so redo simply restores the page image.
 *
 * INSERT records may include FPIs on the first modification after a
 * checkpoint.  When no FPI is present (BLK_NEEDS_REDO), the redo
 * function reconstructs the insertion by copying the UNDO record data
 * into the page at the recorded offset and updating pd_lower.
 *
 * Async I/O Strategy
 * ------------------
 * INSERT records may reference two blocks: block 0 (data page) and
 * block 1 (metapage, when the head pointer was updated).  To overlap
 * the I/O for both blocks, we issue a PrefetchSharedBuffer() for
 * block 1 before processing block 0.  This allows the kernel or the
 * AIO worker to start reading the metapage in parallel with the data
 * page read, reducing overall latency during crash recovery.
 *
 * When io_method is WORKER or IO_URING, we also enter batch mode
 * (pgaio_enter_batchmode) so that multiple I/O submissions can be
 * coalesced into fewer system calls.  The batch is exited after all
 * blocks in the record have been processed.
 *
 * Parallel Redo Support
 * ---------------------
 * This resource manager supports parallel WAL replay for multi-core crash
 * recovery via the startup, cleanup, and mask callbacks registered in
 * rmgrlist.h.
 *
 * Page dependency rules for parallel redo:
 *
 *   - Records that touch different pages can be replayed in parallel with
 *     no ordering constraints.
 *
 *   - Within the same page, XLOG_RELUNDO_INIT (or INSERT with the
 *     XLOG_RELUNDO_INIT_PAGE flag) must be replayed before any subsequent
 *     XLOG_RELUNDO_INSERT on that page.  The recovery manager enforces
 *     this automatically via the page LSN check in XLogReadBufferForRedo.
 *
 *   - XLOG_RELUNDO_DISCARD only modifies the metapage (block 0).  It is
 *     ordered relative to other metapage modifications by the page LSN.
 *
 *   - The metapage (block 0) is a serialization point: INSERT records that
 *     update the head pointer and DISCARD records both touch the metapage,
 *     so they are serialized on that page by the buffer lock.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/undo/relundo_xlog.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bufmask.h"
#include "access/relundo.h"
#include "access/relundo_xlog.h"
#include "access/xlogutils.h"
#include "miscadmin.h"
#include "storage/aio.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/smgr.h"

/*
 * relundo_redo_init - Replay metapage initialization
 *
 * The metapage is always logged with a full page image via
 * XLogInitBufferForRedo, so we just need to initialize and restore it.
 */
static void
relundo_redo_init(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_relundo_init *xlrec = (xl_relundo_init *) XLogRecGetData(record);
	Buffer		buf;
	Page		page;
	RelUndoMetaPageData *meta;

	/* Consistency checks on WAL record data */
	if (xlrec->magic != RELUNDO_METAPAGE_MAGIC)
		elog(PANIC, "relundo_redo_init: invalid magic 0x%X (expected 0x%X)",
			 xlrec->magic, RELUNDO_METAPAGE_MAGIC);

	if (xlrec->version != RELUNDO_METAPAGE_VERSION)
		elog(PANIC, "relundo_redo_init: invalid version %u (expected %u)",
			 xlrec->version, RELUNDO_METAPAGE_VERSION);

	/*
	 * Initial counter should be 1 for a freshly initialized metapage.
	 * RelUndoInitRelation sets counter to 1 so that 0 is clearly "no counter
	 * / uninitialized".  Accept any small value as valid since the counter
	 * only increments from 1 and a freshly initialized metapage will have
	 * counter == 1.
	 */
	if (xlrec->counter > 1)
		elog(PANIC, "relundo_redo_init: initial counter %u too large for init record",
			 xlrec->counter);

	buf = XLogInitBufferForRedo(record, 0);
	page = BufferGetPage(buf);

	/* Initialize the metapage from scratch */
	PageInit(page, BLCKSZ, 0);

	meta = (RelUndoMetaPageData *) PageGetContents(page);
	meta->magic = xlrec->magic;
	meta->version = xlrec->version;
	meta->counter = xlrec->counter;
	for (int slot = 0; slot < RELUNDO_NUM_HEADS; slot++)
	{
		meta->head_blkno[slot] = InvalidBlockNumber;
		meta->tail_blkno[slot] = InvalidBlockNumber;
	}
	meta->free_blkno = InvalidBlockNumber;
	meta->total_records = 0;
	meta->discarded_records = 0;

	/* Match the do-time metapage: cover the meta struct with pd_lower. */
	RelUndoMetaPageSetPdLower(page);

	PageSetLSN(page, lsn);
	MarkBufferDirty(buf);
	UnlockReleaseBuffer(buf);
}

/*
 * relundo_prefetch_block - Issue async prefetch for a WAL-referenced block
 *
 * If the WAL record references the given block_id and it has not already
 * been prefetched by the XLogPrefetcher, initiate an async read via
 * PrefetchSharedBuffer().  This is a no-op when USE_PREFETCH is not
 * available or when the block is already in the buffer pool.
 *
 * Returns true if I/O was initiated, false otherwise (cache hit or no-op).
 */
static bool
relundo_prefetch_block(XLogReaderState *record, uint8 block_id)
{
#ifdef USE_PREFETCH
	RelFileLocator rlocator;
	ForkNumber	forknum;
	BlockNumber blkno;
	Buffer		prefetch_buffer;
	SMgrRelation smgr;

	if (!XLogRecGetBlockTagExtended(record, block_id,
									&rlocator, &forknum, &blkno,
									&prefetch_buffer))
		return false;

	/* If the XLogPrefetcher already cached a buffer hint, skip prefetch. */
	if (BufferIsValid(prefetch_buffer))
		return false;

	smgr = smgropen(rlocator, INVALID_PROC_NUMBER);

	/*
	 * Only prefetch if the relation fork exists and the block is within the
	 * current size.  During recovery, relations may not yet have been
	 * extended to the referenced block.
	 */
	if (smgrexists(smgr, forknum))
	{
		BlockNumber nblocks = smgrnblocks(smgr, forknum);

		if (blkno < nblocks)
		{
			PrefetchSharedBuffer(smgr, forknum, blkno);
			return true;
		}
	}
#endif							/* USE_PREFETCH */

	return false;
}

/*
 * relundo_redo_insert - Replay UNDO record insertion
 *
 * When a full page image is present, it is restored automatically by
 * XLogReadBufferForRedo (BLK_RESTORED).  Otherwise (BLK_NEEDS_REDO),
 * we copy the UNDO record data into the page at the recorded offset
 * and update pd_lower.
 *
 * If the XLOG_RELUNDO_INIT_PAGE flag is set, the page is a newly
 * allocated data page and must be initialized from scratch before
 * inserting the record.
 *
 * Async I/O: When this record references both block 0 (data page) and
 * block 1 (metapage), we prefetch block 1 before reading block 0.
 * This allows the I/O for the metapage to proceed in parallel with
 * the data page read and redo processing, reducing stall time.
 */
static void
relundo_redo_insert(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_relundo_insert *xlrec = (xl_relundo_insert *) XLogRecGetData(record);
	Buffer		buf;
	XLogRedoAction action;
	bool		has_metapage = XLogRecHasBlockRef(record, 1);
	bool		use_batchmode;

	/* Consistency checks on WAL record data */
	if (xlrec->urec_len < SizeOfRelUndoRecordHeader)
		elog(PANIC, "relundo_redo_insert: invalid record length %u (min %zu)",
			 xlrec->urec_len, SizeOfRelUndoRecordHeader);

	if (xlrec->page_offset > BLCKSZ - sizeof(RelUndoPageHeaderData))
		elog(PANIC, "relundo_redo_insert: invalid page offset %u",
			 xlrec->page_offset);

	if (xlrec->new_pd_lower > BLCKSZ)
		elog(PANIC, "relundo_redo_insert: pd_lower %u exceeds page size",
			 xlrec->new_pd_lower);

	/* Cross-field check: record must fit within page */
	if ((uint32) xlrec->page_offset + (uint32) xlrec->urec_len > BLCKSZ)
		elog(PANIC, "relundo_redo_insert: record extends past page end (offset %u + len %u > %u)",
			 xlrec->page_offset, xlrec->urec_len, (uint32) BLCKSZ);

	/*
	 * new_pd_lower must be at least as far as the start of the record we are
	 * inserting.  page_offset is page-absolute
	 * (MAXALIGN(SizeOfPageHeaderData) + contents offset) while new_pd_lower
	 * is relative to the page contents area, so compare them in the same
	 * coordinate system by adding the standard page-header size to the
	 * contents-relative pd_lower.
	 */
	if (xlrec->new_pd_lower + MAXALIGN(SizeOfPageHeaderData) < xlrec->page_offset)
		elog(PANIC, "relundo_redo_insert: new_pd_lower %u precedes page_offset %u",
			 xlrec->new_pd_lower, xlrec->page_offset);

	/* Validate record type is in valid range */
	if (xlrec->urec_type < RELUNDO_INSERT || xlrec->urec_type > RELUNDO_TUPLE_LOCK)
		elog(PANIC, "relundo_redo_insert: invalid record type %u", xlrec->urec_type);

	/*
	 * Async I/O optimization: when the record touches both the data page
	 * (block 0) and the metapage (block 1), issue a prefetch for the metapage
	 * before we read block 0.  This allows both I/Os to be in flight
	 * simultaneously.
	 *
	 * Enter batch mode so that the buffer manager can coalesce the I/O
	 * submissions when using io_method = worker or io_uring.  Batch mode is
	 * only useful when we have multiple blocks to process; for single- block
	 * records the overhead is not worthwhile.
	 */
	use_batchmode = has_metapage && (io_method != IOMETHOD_SYNC);

	if (use_batchmode)
		pgaio_enter_batchmode();

	if (has_metapage)
		relundo_prefetch_block(record, 1);

	if (XLogRecGetInfo(record) & XLOG_RELUNDO_INIT_PAGE)
	{
		/* New page: initialize from scratch, then apply insert */
		buf = XLogInitBufferForRedo(record, 0);
		action = BLK_NEEDS_REDO;
	}
	else
	{
		action = XLogReadBufferForRedo(record, 0, &buf);
	}

	if (action == BLK_NEEDS_REDO)
	{
		Page		page = BufferGetPage(buf);
		char	   *record_data;
		Size		record_len;

		record_data = XLogRecGetBlockData(record, 0, &record_len);

		if (record_data == NULL || record_len == 0)
			elog(PANIC, "relundo_redo_insert: no block data for UNDO record");

		/* Consistency check: verify data length is reasonable */
		if (record_len > BLCKSZ)
			elog(PANIC, "relundo_redo_insert: block data too large (%zu bytes)", record_len);

		/*
		 * If the page was just initialized (INIT_PAGE flag), the block data
		 * contains both the RelUndoPageHeaderData and the UNDO record.
		 * Initialize the page structure first, then copy both.
		 */
		if (XLogRecGetInfo(record) & XLOG_RELUNDO_INIT_PAGE)
		{
			char	   *contents;

			/* INIT_PAGE data must include at least the page header */
			if (record_len < SizeOfRelUndoPageHeaderData)
				elog(PANIC, "relundo_redo_insert: INIT_PAGE block data too small (%zu < %zu)",
					 record_len, SizeOfRelUndoPageHeaderData);

			/* Block data plus page header must fit in a page */
			if (record_len > BLCKSZ - MAXALIGN(SizeOfPageHeaderData))
				elog(PANIC, "relundo_redo_insert: INIT_PAGE block data too large (%zu bytes)",
					 record_len);

			PageInit(page, BLCKSZ, 0);

			/*
			 * The record_data contains: 1. RelUndoPageHeaderData
			 * (SizeOfRelUndoPageHeaderData bytes) 2. UNDO record (remaining
			 * bytes)
			 *
			 * Copy both to the page contents area.
			 */
			contents = PageGetContents(page);
			memcpy(contents, record_data, record_len);
		}
		else
		{
			RelUndoPageHeader undohdr = (RelUndoPageHeader) PageGetContents(page);

			/* Consistency check: verify pd_lower is reasonable before update */
			if (undohdr->pd_lower > BLCKSZ)
				elog(PANIC, "relundo_redo_insert: existing pd_lower %u exceeds page size",
					 undohdr->pd_lower);

			/*
			 * Normal case: page already exists, just copy the UNDO record to
			 * the specified offset.
			 */
			memcpy((char *) page + xlrec->page_offset, record_data, record_len);

			/* Update the page's free space pointer */
			undohdr->pd_lower = xlrec->new_pd_lower;

			/* Restore the page's max_xid discard watermark. */
			undohdr->max_xid = xlrec->max_xid;

			/*
			 * Post-condition check: verify pd_lower is reasonable after
			 * update.  pd_lower is contents-relative while page_offset is
			 * page-absolute, so add the standard page-header size to pd_lower
			 * before comparing against the page-absolute end of the record.
			 */
			if (undohdr->pd_lower + MAXALIGN(SizeOfPageHeaderData) < xlrec->page_offset + record_len)
				elog(PANIC, "relundo_redo_insert: pd_lower %u too small for offset %u + len %zu",
					 undohdr->pd_lower, xlrec->page_offset, record_len);
		}

		PageSetLSN(page, lsn);
		MarkBufferDirty(buf);
	}

	if (BufferIsValid(buf))
		UnlockReleaseBuffer(buf);

	/*
	 * Block 1 (metapage) may also be present if the head pointer was updated.
	 * If so, restore its FPI.  The prefetch issued above should have brought
	 * the page into cache (or at least started the I/O), so this read should
	 * complete quickly.
	 */
	if (has_metapage)
	{
		action = XLogReadBufferForRedo(record, 1, &buf);
		/* Metapage is always logged with FPI, so BLK_RESTORED or BLK_DONE */
		if (BufferIsValid(buf))
			UnlockReleaseBuffer(buf);
	}

	if (use_batchmode)
		pgaio_exit_batchmode();
}

/*
 * relundo_redo_discard - Replay UNDO page discard
 *
 * Discard splices a contiguous run of discardable pages off the tail of the
 * data chain directly onto the metapage's free list.  The run is already
 * internally linked by durable prev_blkno fields, so only the run boundaries
 * change.  The record carries a bounded set of buffers:
 *
 *   Block 0: the metapage (tail + free-list head)
 *   Block 1: the run's old-tail page (prev_blkno -> old free head)
 *   Block 2: the new live tail page (prev_blkno -> Invalid), if any
 *
 * The metapage is registered REGBUF_STANDARD (not a forced FPI), so on a
 * BLK_NEEDS_REDO replay we re-apply the metapage mutations explicitly rather
 * than relying on a restored image.
 */
static void
relundo_redo_discard(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	Buffer		buf;
	XLogRedoAction action;
	xl_relundo_discard *xlrec = (xl_relundo_discard *) XLogRecGetData(record);
	bool		whole_chain = !BlockNumberIsValid(xlrec->new_tail_blkno);

	/*
	 * Consistency check on WAL record data.  A run is always at least one
	 * page; there is no upper bound, since a whole-chain discard of a large
	 * fork legitimately frees arbitrarily many pages.  Do not impose an
	 * arbitrary ceiling here: do-time has no matching guard, so any cap we
	 * reject below an achievable run length would turn a logged record into
	 * one its own crash recovery refuses to replay (a PANIC loop).
	 */
	if (xlrec->npages_freed == 0)
		elog(PANIC, "relundo_redo_discard: npages_freed is zero");

	/*
	 * Block 0 is the metapage; the run's old tail is a real data page, so its
	 * block number must never be the metapage (block 0).
	 */
	if (xlrec->old_tail_blkno == 0)
		elog(PANIC, "relundo_redo_discard: old_tail_blkno is metapage block 0");

	/*
	 * new_tail_blkno is either a real data page (>= 1) or InvalidBlockNumber
	 * when the whole chain was discarded; it must never be the metapage.
	 */
	if (xlrec->new_tail_blkno == 0)
		elog(PANIC, "relundo_redo_discard: new_tail_blkno is metapage block 0");

	/* Block 0: metapage (tail + free-list head). */
	action = XLogReadBufferForRedo(record, 0, &buf);

	if (action == BLK_NEEDS_REDO)
	{
		Page		page = BufferGetPage(buf);
		RelUndoMetaPageData *meta;

		meta = (RelUndoMetaPageData *) PageGetContents(page);

		/* Post-condition checks on metapage */
		if (meta->magic != RELUNDO_METAPAGE_MAGIC)
			elog(PANIC, "relundo_redo_discard: metapage has invalid magic 0x%X",
				 meta->magic);

		if (meta->counter > 65535)
			elog(PANIC, "relundo_redo_discard: counter %u exceeds maximum",
				 meta->counter);

		/* Advance the live data chain tail (and head if it is now empty). */
		meta->tail_blkno[xlrec->slot] = xlrec->new_tail_blkno;
		if (whole_chain)
			meta->head_blkno[xlrec->slot] = InvalidBlockNumber;

		/* Splice the run directly onto the free list. */
		meta->free_blkno = xlrec->free_head_blkno;
		meta->discarded_records += xlrec->npages_freed;

		PageSetLSN(page, lsn);
		MarkBufferDirty(buf);
	}

	if (BufferIsValid(buf))
		UnlockReleaseBuffer(buf);

	/*
	 * Block 1: the run's old-tail page, whose prev_blkno now links to the old
	 * free-list head (appending the prior free list after the run).
	 */
	action = XLogReadBufferForRedo(record, 1, &buf);
	if (action == BLK_NEEDS_REDO)
	{
		Page		page = BufferGetPage(buf);
		RelUndoPageHeader hdr = (RelUndoPageHeader) PageGetContents(page);

		hdr->prev_blkno = xlrec->old_free_head;
		PageSetLSN(page, lsn);
		MarkBufferDirty(buf);
	}
	if (BufferIsValid(buf))
		UnlockReleaseBuffer(buf);

	/*
	 * Block 2: the new live tail page (present only when the chain is not
	 * fully discarded), whose prev_blkno is cleared to detach it from the
	 * run.
	 */
	if (!whole_chain && XLogRecHasBlockRef(record, 2))
	{
		action = XLogReadBufferForRedo(record, 2, &buf);
		if (action == BLK_NEEDS_REDO)
		{
			Page		page = BufferGetPage(buf);
			RelUndoPageHeader hdr = (RelUndoPageHeader) PageGetContents(page);

			hdr->prev_blkno = InvalidBlockNumber;
			PageSetLSN(page, lsn);
			MarkBufferDirty(buf);
		}
		if (BufferIsValid(buf))
			UnlockReleaseBuffer(buf);
	}
}

/*
 * relundo_redo_truncate - Replay physical truncation of an emptied UNDO fork
 *
 * The do-time path (RelUndoTruncateEmptyChain) logs this only when a discard
 * empties the whole data chain, at which point the free list -- and thus every
 * allocated data block -- is the contiguous physical suffix [1 .. watermark].
 * Redo restores the metapage (block 0) with its free-list head and allocation
 * watermark reset, then drops the truncated buffers and shrinks the fork file.
 *
 * The metapage is registered REGBUF_STANDARD, so on BLK_NEEDS_REDO we re-apply
 * the field resets explicitly rather than relying on a restored image.
 */
static void
relundo_redo_truncate(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_relundo_truncate *xlrec = (xl_relundo_truncate *) XLogRecGetData(record);
	Buffer		buf;
	XLogRedoAction action;
	RelFileLocator rlocator;
	ForkNumber	forknum;
	BlockNumber blkno;
	SMgrRelation reln;
	BlockNumber old_nblocks;
	BlockNumber new_nblocks = xlrec->new_nblocks;

	/* The fork is always truncated back to just the metapage (block 0). */
	if (new_nblocks != 1)
		elog(PANIC, "relundo_redo_truncate: unexpected new_nblocks %u",
			 new_nblocks);

	/* Block 0: metapage (free-list head + watermark reset). */
	action = XLogReadBufferForRedo(record, 0, &buf);

	if (action == BLK_NEEDS_REDO)
	{
		Page		page = BufferGetPage(buf);
		RelUndoMetaPageData *meta;

		meta = (RelUndoMetaPageData *) PageGetContents(page);

		if (meta->magic != RELUNDO_METAPAGE_MAGIC)
			elog(PANIC, "relundo_redo_truncate: metapage has invalid magic 0x%X",
				 meta->magic);

		/*
		 * The discard that triggered this truncation already emptied the data
		 * chain; the freed pages no longer exist, so clear the free-list head
		 * and the allocation watermark too.
		 */
		for (int slot = 0; slot < RELUNDO_NUM_HEADS; slot++)
		{
			meta->head_blkno[slot] = InvalidBlockNumber;
			meta->tail_blkno[slot] = InvalidBlockNumber;
		}
		meta->free_blkno = InvalidBlockNumber;
		meta->system_alloc_watermark = InvalidBlockNumber;

		PageSetLSN(page, lsn);
		MarkBufferDirty(buf);
	}

	if (BufferIsValid(buf))
		UnlockReleaseBuffer(buf);

	/*
	 * Now physically truncate the fork.  Resolve the relation from block 0's
	 * tag, force-create the fork if it is missing (it may have been dropped
	 * later in the WAL stream), and mirror smgr_redo's XLOG_SMGR_TRUNCATE
	 * arm: advance the minimum recovery point, then smgrtruncate (which drops
	 * the to-be-removed buffers via DropRelationBuffers).
	 */
	XLogRecGetBlockTag(record, 0, &rlocator, &forknum, &blkno);
	Assert(forknum == RELUNDO_FORKNUM);
	reln = smgropen(rlocator, INVALID_PROC_NUMBER);
	smgrcreate(reln, forknum, true);

	XLogFlush(lsn);

	old_nblocks = smgrnblocks(reln, forknum);
	if (old_nblocks > new_nblocks)
	{
		/* Tell xlogutils.c so cached relation sizes stay consistent. */
		XLogTruncateRelation(rlocator, forknum, new_nblocks);

		START_CRIT_SECTION();
		smgrtruncate(reln, &forknum, 1, &old_nblocks, &new_nblocks);
		END_CRIT_SECTION();
	}
}

/*
 * relundo_redo_apply - Replay a rollback compensation log record (CLR)
 *
 * The CLR carries full-page images of every data page (main fork) restored
 * during online abort plus the UNDO-fork page (carrying RELUNDO_INFO_CLR_APPLIED).
 * Each referenced block was registered with REGBUF_FORCE_IMAGE, so redo simply
 * restores the recorded images, reinstating the before-image rollback that
 * would otherwise be lost when dirty buffers vanish on an immediate crash.
 *
 * Because every block is logged with a forced FPI, XLogReadBufferForRedo
 * returns BLK_RESTORED for blocks needing redo and BLK_DONE for those already
 * at or past the record LSN; in neither case is there extra work to do.
 */
static void
relundo_redo_apply(XLogReaderState *record)
{
	int			block_id;
	int			max_block_id = XLogRecMaxBlockId(record);

	for (block_id = 0; block_id <= max_block_id; block_id++)
	{
		Buffer		buf;

		if (!XLogRecHasBlockRef(record, block_id))
			continue;

		(void) XLogReadBufferForRedo(record, (uint8) block_id, &buf);
		if (BufferIsValid(buf))
			UnlockReleaseBuffer(buf);
	}
}

/*
 * relundo_redo - Main redo dispatch for RM_RELUNDO_ID
 */
void
relundo_redo(XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	/*
	 * Strip XLOG_RELUNDO_INIT_PAGE flag for the switch; it only affects
	 * INSERT processing.
	 */
	switch (info & ~XLOG_RELUNDO_INIT_PAGE)
	{
		case XLOG_RELUNDO_INIT:
			relundo_redo_init(record);
			break;

		case XLOG_RELUNDO_INSERT:
			relundo_redo_insert(record);
			break;

		case XLOG_RELUNDO_DISCARD:
			relundo_redo_discard(record);
			break;

		case XLOG_RELUNDO_TRUNCATE:
			relundo_redo_truncate(record);
			break;

		case XLOG_RELUNDO_APPLY:
			relundo_redo_apply(record);
			break;

		default:
			elog(PANIC, "relundo_redo: unknown op code %u", info);
	}
}

/*
 * relundo_startup - Initialize per-backend state for parallel redo
 *
 * Called once per backend at the start of parallel WAL replay.
 * We don't currently need any special per-backend state for per-relation UNDO,
 * but this hook is required for parallel redo support.
 */
void
relundo_startup(void)
{
	/*
	 * No per-backend initialization needed currently. If we add backend-local
	 * caches or state in the future, initialize them here.
	 */
}

/*
 * relundo_cleanup - Clean up per-backend state after parallel redo
 *
 * Called once per backend at the end of parallel WAL replay.
 * Counterpart to relundo_startup().
 */
void
relundo_cleanup(void)
{
	/*
	 * No per-backend cleanup needed currently. If relundo_startup()
	 * initializes any resources, release them here.
	 */
}

/*
 * relundo_mask - Mask non-critical page fields for consistency checking
 *
 * During parallel redo, pages may be replayed in different order across
 * backends.  This function masks out fields that may differ but do not
 * indicate corruption, so that page comparisons (e.g. by pg_waldump
 * --check) avoid false positives.
 *
 * We use the standard mask_page_lsn_and_checksum() helper from bufmask.h,
 * matching the convention used by heap, btree, and other resource managers.
 *
 * RelUndo pages do not use the standard line-pointer layout, so we cannot
 * call mask_unused_space() (which operates on the standard PageHeader's
 * pd_lower/pd_upper).  Instead, for data pages we mask the free space
 * tracked by the RelUndoPageHeader's own pd_lower and pd_upper fields
 * within the contents area.
 */
void
relundo_mask(char *pagedata, BlockNumber blkno)
{
	Page		page = (Page) pagedata;

	/*
	 * Mask LSN and checksum -- these may differ across parallel redo workers
	 * due to replay ordering.
	 */
	mask_page_lsn_and_checksum(page);

	if (blkno == 0)
	{
		/*
		 * Metapage: do not mask magic, version, counter, or block pointers.
		 * Those must match exactly for consistency.  LSN and checksum are
		 * already masked above.
		 */
	}
	else
	{
		/*
		 * Data page: mask unused space between the UNDO page header's
		 * pd_lower (next insertion point) and pd_upper (end of usable space).
		 * This region may contain stale data from prior page reuse and is not
		 * meaningful for consistency.
		 *
		 * The RelUndoPageHeader sits at the start of the page contents area
		 * (after the standard PageHeaderData).  Its pd_lower and pd_upper are
		 * offsets relative to the contents area.
		 */
		RelUndoPageHeader undohdr = (RelUndoPageHeader) PageGetContents(page);
		char	   *contents = (char *) PageGetContents(page);
		int			lower = undohdr->pd_lower;
		int			upper = undohdr->pd_upper;

		if (lower < upper)
			memset(contents + lower, MASK_MARKER, upper - lower);
	}
}
