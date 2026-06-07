/*-------------------------------------------------------------------------
 *
 * relundo_discard.c
 *	  Per-relation UNDO discard and space reclamation
 *
 * This file implements the counter-based discard logic for per-relation UNDO.
 * During VACUUM, old UNDO records are discarded and their pages reclaimed
 * to the free list for reuse.
 *
 * Discard walks the page chain from the tail (oldest) toward the head
 * (newest).  Each page's generation counter is compared against the
 * oldest-visible cutoff using modular 16-bit arithmetic.  If a page's
 * counter precedes the cutoff, all records on that page are safe to
 * discard and the page is moved to the free list.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/undo/relundo_discard.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/relundo.h"
#include "access/relundo_xlog.h"
#include "access/transam.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "common/relpath.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/smgr.h"

/*
 * relundo_page_is_discardable
 *		Check if every record on a page is older than the discard horizon.
 *
 * A page is discardable iff its max_xid (the largest urec_xid of any record
 * on the page) precedes oldest_xmin.  In that case no active transaction can
 * still need any record on the page for rollback, so the page can be freed.
 *
 * An empty page (max_xid == InvalidTransactionId) carries no live records and
 * is trivially discardable.
 */
static bool
relundo_page_is_discardable(Page page, TransactionId oldest_xmin)
{
	RelUndoPageHeader hdr;

	hdr = (RelUndoPageHeader) PageGetContents(page);

	if (!TransactionIdIsValid(hdr->max_xid))
		return true;

	return TransactionIdPrecedes(hdr->max_xid, oldest_xmin);
}

/*
 * RelUndoDiscard
 *		Discard old UNDO records and reclaim space.
 *
 * Walks the page chain from the head toward the tail.  Any page whose
 * max_xid precedes oldest_xmin holds only records that no active
 * transaction can still need for rollback; such pages are unlinked from
 * the data chain and deferred for deallocation to the free list.
 *
 * The chain is chronologically ordered (head newest, tail oldest), so the
 * discardable pages form a contiguous run at the tail end.
 */
void
RelUndoDiscard(Relation rel, TransactionId oldest_xmin)
{
	Buffer		metabuf;
	Page		metapage;
	RelUndoMetaPage meta;
	BlockNumber old_tail_blkno;
	BlockNumber new_tail_blkno = InvalidBlockNumber;
	BlockNumber run_head_blkno = InvalidBlockNumber;
	BlockNumber current_blkno;
	BlockNumber old_free_head;
	uint32		npages_freed = 0;
	Buffer		runtail_buf;
	Buffer		newtail_buf = InvalidBuffer;

	/* Lock the metapage exclusively for the duration of discard */
	metabuf = relundo_get_metapage(rel, BUFFER_LOCK_EXCLUSIVE);
	metapage = BufferGetPage(metabuf);
	meta = (RelUndoMetaPage) PageGetContents(metapage);

	old_tail_blkno = meta->tail_blkno;
	old_free_head = meta->free_blkno;

	if (!BlockNumberIsValid(old_tail_blkno))
	{
		/* Empty chain, nothing to discard */
		UnlockReleaseBuffer(metabuf);
		return;
	}

	/*
	 * Pass 1 (read-only): walk from head toward tail following prev_blkno.
	 * A page is discardable iff its max_xid precedes oldest_xmin.  The last
	 * (closest-to-tail) page that is NOT discardable becomes the new tail;
	 * every page below it forms a contiguous discardable run.  run_head is
	 * the newest page in that run (the page just below the new tail, or the
	 * chain head if the whole chain is discardable).
	 *
	 * This relies on a precondition of the append-only fork: discardability is
	 * monotonic from tail (oldest) to head (newest).  Records are appended in
	 * commit order, so a page's max_xid never decreases as the chain advances
	 * head-ward; once a page is non-discardable (its max_xid reaches
	 * oldest_xmin) every newer page above it is non-discardable too.  Thus the
	 * discardable pages always form a single contiguous run at the tail, and
	 * keeping the closest-to-tail non-discardable page as the new tail never
	 * splices a still-live page onto the free list.
	 */
	current_blkno = meta->head_blkno;
	while (BlockNumberIsValid(current_blkno))
	{
		Buffer		buf;
		Page		page;
		RelUndoPageHeader hdr;
		BlockNumber prev;

		buf = ReadBufferExtended(rel, RELUNDO_FORKNUM, current_blkno,
								 RBM_NORMAL, NULL);
		LockBuffer(buf, BUFFER_LOCK_SHARE);

		page = BufferGetPage(buf);
		hdr = (RelUndoPageHeader) PageGetContents(page);
		prev = hdr->prev_blkno;

		if (!relundo_page_is_discardable(page, oldest_xmin))
		{
			new_tail_blkno = current_blkno;
			run_head_blkno = prev;
		}

		UnlockReleaseBuffer(buf);
		current_blkno = prev;
	}

	if (!BlockNumberIsValid(new_tail_blkno))
	{
		/* Whole chain is discardable: the run starts at the chain head. */
		run_head_blkno = meta->head_blkno;
	}

	if (!BlockNumberIsValid(run_head_blkno))
	{
		/* Nothing below the new tail is discardable. */
		UnlockReleaseBuffer(metabuf);
		return;
	}

	/*
	 * Pass 2 (read-only): count the pages in the discardable run, walking
	 * from run_head down to (and including) the old tail.
	 */
	current_blkno = run_head_blkno;
	while (BlockNumberIsValid(current_blkno))
	{
		Buffer		buf;
		Page		page;
		RelUndoPageHeader hdr;

		buf = ReadBufferExtended(rel, RELUNDO_FORKNUM, current_blkno,
								 RBM_NORMAL, NULL);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);
		hdr = (RelUndoPageHeader) PageGetContents(page);
		current_blkno = hdr->prev_blkno;
		UnlockReleaseBuffer(buf);
		npages_freed++;
	}

	Assert(npages_freed > 0);

	/*
	 * Splice the whole run directly onto the free list with a bounded, fully
	 * WAL-logged set of mutations.  Both the free list and the discardable run
	 * are threaded through the same durable prev_blkno fields (logged at insert
	 * time), so the run's internal links are left untouched and only its
	 * boundaries change:
	 *
	 *   - the run's tail (old chain tail) gets prev_blkno = old_free_head,
	 *     appending the prior free list after the run;
	 *   - the new live tail (if any) gets prev_blkno = InvalidBlockNumber,
	 *     detaching the live chain from the run;
	 *   - the metapage's tail and free-list head are updated.
	 *
	 * Folding reclamation into this single WAL-logged operation makes discard
	 * crash-safe end to end: there is no separate, unlogged deallocation step
	 * whose replay could leave the free list inconsistent with the metapage.
	 *
	 * Pin and exclusively lock the boundary data pages BEFORE the critical
	 * section so XLogRegisterBuffer sees them dirty and locked.
	 */
	runtail_buf = ReadBufferExtended(rel, RELUNDO_FORKNUM, old_tail_blkno,
									 RBM_NORMAL, NULL);
	LockBuffer(runtail_buf, BUFFER_LOCK_EXCLUSIVE);

	if (BlockNumberIsValid(new_tail_blkno))
	{
		newtail_buf = ReadBufferExtended(rel, RELUNDO_FORKNUM, new_tail_blkno,
										 RBM_NORMAL, NULL);
		LockBuffer(newtail_buf, BUFFER_LOCK_EXCLUSIVE);
	}

	/* Apply the in-memory mutations. */
	{
		RelUndoPageHeader runtail_hdr;

		runtail_hdr = (RelUndoPageHeader) PageGetContents(BufferGetPage(runtail_buf));
		runtail_hdr->prev_blkno = old_free_head;
		MarkBufferDirty(runtail_buf);

		if (BufferIsValid(newtail_buf))
		{
			RelUndoPageHeader newtail_hdr;

			newtail_hdr = (RelUndoPageHeader) PageGetContents(BufferGetPage(newtail_buf));
			newtail_hdr->prev_blkno = InvalidBlockNumber;
			MarkBufferDirty(newtail_buf);

			meta->tail_blkno = new_tail_blkno;
		}
		else
		{
			/* Whole chain discarded: the data chain is now empty. */
			meta->head_blkno = InvalidBlockNumber;
			meta->tail_blkno = InvalidBlockNumber;
		}

		meta->free_blkno = run_head_blkno;
		meta->discarded_records += npages_freed;	/* approximate */
		MarkBufferDirty(metabuf);
	}

	/* WAL-log the discard operation. */
	START_CRIT_SECTION();
	{
		xl_relundo_discard xlrec;

		xlrec.old_tail_blkno = old_tail_blkno;
		xlrec.new_tail_blkno = meta->tail_blkno;
		xlrec.free_head_blkno = run_head_blkno;
		xlrec.old_free_head = old_free_head;
		xlrec.discard_xid = oldest_xmin;
		xlrec.npages_freed = npages_freed;

		XLogBeginInsert();
		XLogRegisterData((char *) &xlrec, SizeOfRelundoDiscard);

		/* Block 0: metapage (tail + free-list head). */
		XLogRegisterBuffer(0, metabuf, REGBUF_STANDARD);

		/* Block 1: run tail, whose prev_blkno now links to old_free_head. */
		XLogRegisterBuffer(1, runtail_buf, REGBUF_STANDARD);

		/* Block 2: new live tail, whose prev_blkno is cleared (if present). */
		if (BufferIsValid(newtail_buf))
			XLogRegisterBuffer(2, newtail_buf, REGBUF_STANDARD);

		XLogInsert(RM_RELUNDO_ID, XLOG_RELUNDO_DISCARD);
	}
	END_CRIT_SECTION();

	if (BufferIsValid(newtail_buf))
		UnlockReleaseBuffer(newtail_buf);
	UnlockReleaseBuffer(runtail_buf);
	UnlockReleaseBuffer(metabuf);
}
