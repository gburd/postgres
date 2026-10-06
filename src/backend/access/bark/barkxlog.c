/*-------------------------------------------------------------------------
 *
 * barkxlog.c
 *	  WAL replay logic for BARK indexes.
 *
 * See "WAL" in src/backend/access/bark/README for the records and the locks
 * their redo takes.  Changes that have no record here (splits, new roots,
 * overflow chains) are logged with generic WAL and replayed by generic_redo.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/bark/barkxlog.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bark.h"
#include "access/barkxlog.h"
#include "access/bufmask.h"
#include "access/xlogutils.h"
#include "storage/standby.h"

/*
 * Clear BARK_INCOMPLETE_SPLIT on the left half of a split whose downlink an
 * INSERT_UPPER record added, as nbtree's _bt_clear_incomplete_split.
 */
static void
bark_xlog_clear_incomplete_split(XLogReaderState *record, uint8 block_id)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	Buffer		buffer;

	if (XLogReadBufferForRedo(record, block_id, &buffer) == BLK_NEEDS_REDO)
	{
		Page		page = BufferGetPage(buffer);
		BarkPageOpaque opaque = BarkPageGetOpaque(page);

		Assert((opaque->bark_flags & BARK_INCOMPLETE_SPLIT) != 0);
		opaque->bark_flags &= ~BARK_INCOMPLETE_SPLIT;

		PageSetLSN(page, lsn);
		MarkBufferDirty(buffer);
	}
	if (BufferIsValid(buffer))
		UnlockReleaseBuffer(buffer);
}

/*
 * Replay the insertion of one entry (bark_insert_entry): a leaf entry, or a
 * downlink that finishes a child's split.
 *
 * The primary keeps the child locked until the downlink is in the parent.
 * Replay clears the child's flag first and does not couple the two locks, as
 * btree_xlog_insert does not: the flag matters only to writers, and there
 * are none during recovery, while readers reach the right half through the
 * child's right link either way.
 */
static void
bark_xlog_insert(bool isleaf, XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_bark_insert *xlrec = (xl_bark_insert *) XLogRecGetData(record);
	Buffer		buffer;

	if (!isleaf)
		bark_xlog_clear_incomplete_split(record, 1);

	if (XLogReadBufferForRedo(record, 0, &buffer) == BLK_NEEDS_REDO)
	{
		Page		page = BufferGetPage(buffer);
		Size		datalen;
		char	   *datapos = XLogRecGetBlockData(record, 0, &datalen);

		if (PageAddItem(page, datapos, datalen, xlrec->offnum,
						false, false) == InvalidOffsetNumber)
			elog(PANIC, "failed to add BARK entry during replay");

		PageSetLSN(page, lsn);
		MarkBufferDirty(buffer);
	}
	if (BufferIsValid(buffer))
		UnlockReleaseBuffer(buffer);
}

/*
 * Replay the replacement of one leaf entry (bark_overwrite_entry).
 */
static void
bark_xlog_overwrite(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_bark_overwrite *xlrec = (xl_bark_overwrite *) XLogRecGetData(record);
	Buffer		buffer;

	if (XLogReadBufferForRedo(record, 0, &buffer) == BLK_NEEDS_REDO)
	{
		Page		page = BufferGetPage(buffer);
		Size		datalen;
		char	   *datapos = XLogRecGetBlockData(record, 0, &datalen);

		if (!PageIndexTupleOverwrite(page, xlrec->offnum, datapos, datalen))
			elog(PANIC, "failed to replace BARK leaf entry during replay");

		PageSetLSN(page, lsn);
		MarkBufferDirty(buffer);
	}
	if (BufferIsValid(buffer))
		UnlockReleaseBuffer(buffer);
}

/*
 * Replay VACUUM's changes to one leaf, in the order bark_delitems_vacuum made
 * them: rewrites, then deletions, then the cycle-ID clear.
 */
static void
bark_xlog_vacuum(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_bark_vacuum *xlrec = (xl_bark_vacuum *) XLogRecGetData(record);
	Buffer		buffer;

	/*
	 * Take a cleanup lock, as btree_xlog_vacuum does, so that replay waits
	 * for (or, after max_standby_streaming_delay, cancels) a standby scan
	 * that holds a pin on the page, as VACUUM did on the primary.
	 */
	if (XLogReadBufferForRedoExtended(record, 0, RBM_NORMAL, true, &buffer)
		== BLK_NEEDS_REDO)
	{
		Page		page = BufferGetPage(buffer);

		/* A record that only clears the cycle ID carries no block data */
		if (xlrec->ndeleted > 0 || xlrec->nupdated > 0)
		{
			char	   *ptr = XLogRecGetBlockData(record, 0, NULL);
			OffsetNumber *deleted = (OffsetNumber *) ptr;
			OffsetNumber *updatedoffsets = deleted + xlrec->ndeleted;
			char	   *itup = (char *) (updatedoffsets + xlrec->nupdated);

			/*
			 * The entries follow the offsets, each padded to MAXALIGN.  They
			 * are only copied, so they need no alignment beyond the two bytes
			 * that reading t_info takes.
			 */
			for (int i = 0; i < xlrec->nupdated; i++)
			{
				Size		itemsz = IndexTupleSize((IndexTuple) itup);

				if (!PageIndexTupleOverwrite(page, updatedoffsets[i], itup,
											 itemsz))
					elog(PANIC, "failed to rewrite BARK leaf entry during vacuum replay");
				itup += MAXALIGN(itemsz);
			}

			if (xlrec->ndeleted > 0)
				PageIndexMultiDelete(page, deleted, xlrec->ndeleted);
		}

		BarkPageGetOpaque(page)->bark_cycleid = 0;

		PageSetLSN(page, lsn);
		MarkBufferDirty(buffer);
	}
	if (BufferIsValid(buffer))
		UnlockReleaseBuffer(buffer);
}

/*
 * Replay the deletion of an empty leaf (bark_delete_empty_leaf).
 *
 * The primary locks the parent first and the three leaves conditionally (see
 * bark_delete_empty_leaf).  Replay cannot skip a page, so it locks
 * unconditionally, in the order a split and amcheck use: the leaves left to
 * right, then the parent.  Standby readers hold one page lock at a time, or
 * a child's lock while they lock its parent, so this order cannot deadlock
 * with them.  All four locks are held until the record is applied, so a
 * reader sees the deletion whole or not at all, as on the primary.
 */
static void
bark_xlog_unlink_page(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_bark_unlink_page *xlrec = (xl_bark_unlink_page *) XLogRecGetData(record);
	Buffer		leftbuf;
	Buffer		target;
	Buffer		rightbuf;
	Buffer		parentbuf;
	Page		page;

	/* Fix the right link of the left sibling */
	if (XLogReadBufferForRedo(record, 1, &leftbuf) == BLK_NEEDS_REDO)
	{
		page = BufferGetPage(leftbuf);
		BarkPageGetOpaque(page)->bark_next = xlrec->rightsib;

		PageSetLSN(page, lsn);
		MarkBufferDirty(leftbuf);
	}

	/* Rewrite the target as an empty deleted page that keeps its links */
	target = XLogInitBufferForRedo(record, 0);
	page = BufferGetPage(target);
	BarkPageSetDeleted(page, xlrec->leftsib, xlrec->rightsib, xlrec->safexid);
	PageSetLSN(page, lsn);
	MarkBufferDirty(target);

	/* Fix the left link of the right sibling */
	if (XLogReadBufferForRedo(record, 2, &rightbuf) == BLK_NEEDS_REDO)
	{
		page = BufferGetPage(rightbuf);
		BarkPageGetOpaque(page)->bark_prev = xlrec->leftsib;

		PageSetLSN(page, lsn);
		MarkBufferDirty(rightbuf);
	}

	/*
	 * Move the target's key space to the right sibling: point the target's
	 * downlink at the right sibling and remove the right sibling's own
	 * downlink, the next item.
	 */
	if (XLogReadBufferForRedo(record, 3, &parentbuf) == BLK_NEEDS_REDO)
	{
		IndexTuple	downlink;

		page = BufferGetPage(parentbuf);
		downlink = (IndexTuple) PageGetItem(page,
											PageGetItemId(page, xlrec->poffset));
		BarkEntrySetDownLink(downlink, xlrec->rightsib);
		PageIndexTupleDelete(page, OffsetNumberNext(xlrec->poffset));

		PageSetLSN(page, lsn);
		MarkBufferDirty(parentbuf);
	}

	if (BufferIsValid(leftbuf))
		UnlockReleaseBuffer(leftbuf);
	UnlockReleaseBuffer(target);
	if (BufferIsValid(rightbuf))
		UnlockReleaseBuffer(rightbuf);
	if (BufferIsValid(parentbuf))
		UnlockReleaseBuffer(parentbuf);
}

/*
 * Replay the freeing of an overflow page (bark_free_oversized).
 */
static void
bark_xlog_mark_deleted(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_bark_mark_deleted *xlrec = (xl_bark_mark_deleted *) XLogRecGetData(record);
	Buffer		buffer;
	Page		page;

	buffer = XLogInitBufferForRedo(record, 0);
	page = BufferGetPage(buffer);
	BarkPageSetDeleted(page, BARK_P_NONE, xlrec->next, xlrec->safexid);
	PageSetLSN(page, lsn);
	MarkBufferDirty(buffer);
	UnlockReleaseBuffer(buffer);
}

/*
 * A deleted page is about to be reused.  Cancel the standby queries that may
 * still hold a link to it, as btree_xlog_reuse_page does.  safexid was read
 * while the page and its neighbours were locked for the deletion, so any
 * snapshot that could have read a link to the page has an xmin no later than
 * safexid; the primary reuses the page only once GlobalVisCheckRemovableFullXid
 * says no such snapshot remains there, and this makes the same true here.
 */
static void
bark_xlog_reuse_page(XLogReaderState *record)
{
	xl_bark_reuse_page *xlrec = (xl_bark_reuse_page *) XLogRecGetData(record);

	if (InHotStandby)
		ResolveRecoveryConflictWithSnapshotFullXid(xlrec->snapshotConflictHorizon,
												   xlrec->isCatalogRel,
												   xlrec->locator);
}

void
bark_redo(XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info)
	{
		case XLOG_BARK_VACUUM:
			bark_xlog_vacuum(record);
			break;
		case XLOG_BARK_UNLINK_PAGE:
			bark_xlog_unlink_page(record);
			break;
		case XLOG_BARK_REUSE_PAGE:
			bark_xlog_reuse_page(record);
			break;
		case XLOG_BARK_MARK_DELETED:
			bark_xlog_mark_deleted(record);
			break;
		case XLOG_BARK_INSERT_LEAF:
			bark_xlog_insert(true, record);
			break;
		case XLOG_BARK_INSERT_UPPER:
			bark_xlog_insert(false, record);
			break;
		case XLOG_BARK_OVERWRITE:
			bark_xlog_overwrite(record);
			break;
		default:
			elog(PANIC, "bark_redo: unknown op code %u", info);
	}
}

/*
 * Mask a BARK page before performing consistency checks on it.
 *
 * BARK makes no unlogged changes to its pages: it sets no LP_DEAD bits, and
 * it logs the clearing of the vacuum cycle ID, which nbtree treats as a hint
 * and masks (see "WAL" in the README).  So only the standard fields need
 * masking.
 */
void
bark_mask(char *pagedata, BlockNumber blkno)
{
	Page		page = (Page) pagedata;

	mask_page_lsn_and_checksum(page);
	mask_page_hint_bits(page);
	mask_unused_space(page);
}
