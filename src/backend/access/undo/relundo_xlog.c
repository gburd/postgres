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
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/undo/relundo_xlog.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/relundo.h"
#include "access/relundo_xlog.h"
#include "access/xlogutils.h"
#include "storage/bufmgr.h"

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

	buf = XLogInitBufferForRedo(record, 0);
	page = BufferGetPage(buf);

	/* Initialize the metapage from scratch */
	PageInit(page, BLCKSZ, 0);

	meta = (RelUndoMetaPageData *) PageGetContents(page);
	meta->magic = xlrec->magic;
	meta->version = xlrec->version;
	meta->counter = xlrec->counter;
	meta->head_blkno = InvalidBlockNumber;
	meta->tail_blkno = InvalidBlockNumber;
	meta->free_blkno = InvalidBlockNumber;
	meta->total_records = 0;
	meta->discarded_records = 0;

	PageSetLSN(page, lsn);
	MarkBufferDirty(buf);
	UnlockReleaseBuffer(buf);
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
 */
static void
relundo_redo_insert(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_relundo_insert *xlrec = (xl_relundo_insert *) XLogRecGetData(record);
	Buffer		buf;
	XLogRedoAction action;

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

		/*
		 * If the page was just initialized (INIT_PAGE flag), the block data
		 * contains both the RelUndoPageHeaderData and the UNDO record.
		 * Initialize the page structure first, then copy both.
		 */
		if (XLogRecGetInfo(record) & XLOG_RELUNDO_INIT_PAGE)
		{
			char	   *contents;

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
			/*
			 * Normal case: page already exists, just copy the UNDO record to
			 * the specified offset.
			 */
			memcpy((char *) page + xlrec->page_offset, record_data, record_len);

			/* Update the page's free space pointer */
			((RelUndoPageHeader) PageGetContents(page))->pd_lower = xlrec->new_pd_lower;
		}

		PageSetLSN(page, lsn);
		MarkBufferDirty(buf);
	}

	if (BufferIsValid(buf))
		UnlockReleaseBuffer(buf);

	/*
	 * Block 1 (metapage) may also be present if the head pointer was updated.
	 * If so, restore its FPI.
	 */
	if (XLogRecHasBlockRef(record, 1))
	{
		action = XLogReadBufferForRedo(record, 1, &buf);
		/* Metapage is always logged with FPI, so BLK_RESTORED or BLK_DONE */
		if (BufferIsValid(buf))
			UnlockReleaseBuffer(buf);
	}
}

/*
 * relundo_redo_discard - Replay UNDO page discard
 *
 * The metapage is logged with a full page image, so we just restore it.
 * The actual page unlinking was already reflected in the metapage state.
 */
static void
relundo_redo_discard(XLogReaderState *record)
{
	Buffer		buf;
	XLogRedoAction action;

	/* Block 0 is the metapage with updated tail/free pointers */
	action = XLogReadBufferForRedo(record, 0, &buf);

	if (action == BLK_NEEDS_REDO)
	{
		XLogRecPtr	lsn = record->EndRecPtr;
		xl_relundo_discard *xlrec = (xl_relundo_discard *) XLogRecGetData(record);
		Page		page = BufferGetPage(buf);
		RelUndoMetaPageData *meta;

		meta = (RelUndoMetaPageData *) PageGetContents(page);

		/* Update the metapage to reflect the discard */
		meta->tail_blkno = xlrec->new_tail_blkno;
		meta->discarded_records += xlrec->npages_freed;

		PageSetLSN(page, lsn);
		MarkBufferDirty(buf);
	}

	if (BufferIsValid(buf))
		UnlockReleaseBuffer(buf);
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

		case XLOG_RELUNDO_APPLY:
			/* CLR - already replayed, nothing to do */
			break;

		default:
			elog(PANIC, "relundo_redo: unknown op code %u", info);
	}
}
