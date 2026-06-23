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
#include "storage/lmgr.h"
#include "storage/proc.h"
#include "storage/smgr.h"
#include "utils/rel.h"

static void RelUndoTruncateEmptyChain(Relation rel, Buffer metabuf);
static void RelUndoDiscardSlot(Relation rel, Buffer metabuf, int slot,
							   TransactionId oldest_xmin);

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
 *		Discard old UNDO records and reclaim space across all head slots.
 *
 * Each of the RELUNDO_NUM_HEADS append chains is independently append-only,
 * so discardability is monotonic tail->head within each slot.  We walk and
 * splice each slot's chain separately under the single metapage exclusive
 * lock, then physically truncate the fork only if every slot ended empty.
 */
void
RelUndoDiscard(Relation rel, TransactionId oldest_xmin, bool nowait)
{
	Buffer		metabuf;
	Page		metapage;
	RelUndoMetaPage meta;
	bool		all_empty;

	/*
	 * Lock the metapage exclusively for the duration of discard.  When nowait
	 * is set (the inline hot-path caller, RelUndoMaybeVacuum), acquire the lock
	 * CONDITIONALLY: if another backend holds it, skip this discard entirely
	 * rather than block the update.  This keeps concurrent updates to the same
	 * relation from serializing behind the single per-relation UNDO metapage
	 * lock.  Space reclamation is best-effort; a skipped discard is retried on
	 * a later update.
	 */
	if (nowait)
	{
		/*
		 * Pin the metapage (block 0) and take its content lock CONDITIONALLY.
		 * The UNDO fork is known to exist (RelUndoVacuum checked smgrexists),
		 * so block 0 is present and initialized.  If another backend holds the
		 * lock, skip this discard rather than block the update.
		 */
		metabuf = ReadBufferExtended(rel, RELUNDO_FORKNUM,
									 RELUNDO_METAPAGE_BLKNO, RBM_NORMAL, NULL);
		if (!ConditionalLockBuffer(metabuf))
		{
			ReleaseBuffer(metabuf);
			return;
		}
	}
	else
		metabuf = relundo_get_metapage(rel, BUFFER_LOCK_EXCLUSIVE);
	metapage = BufferGetPage(metabuf);
	meta = (RelUndoMetaPage) PageGetContents(metapage);

	/*
	 * A conditionally-locked metapage might (in a rare crash-recovery window)
	 * be uninitialized; the blocking relundo_get_metapage path reinitializes
	 * it, but the nowait path must not do WAL work, so just skip if invalid.
	 */
	if (nowait && meta->magic != RELUNDO_METAPAGE_MAGIC)
	{
		UnlockReleaseBuffer(metabuf);
		return;
	}

	for (int slot = 0; slot < RELUNDO_NUM_HEADS; slot++)
		RelUndoDiscardSlot(rel, metabuf, slot, oldest_xmin);

	/*
	 * If every slot's chain is now empty, the free list holds every allocated
	 * data block -- the contiguous physical suffix [1 .. system_alloc_watermark]
	 * -- so the fork can be physically truncated back to the metapage.  That is
	 * the only provably-safe truncate case, and we still hold the metapage
	 * exclusive lock (blocking any concurrent allocator).
	 */
	all_empty = true;
	for (int slot = 0; slot < RELUNDO_NUM_HEADS; slot++)
	{
		if (BlockNumberIsValid(meta->tail_blkno[slot]))
		{
			all_empty = false;
			break;
		}
	}

	if (all_empty)
		RelUndoTruncateEmptyChain(rel, metabuf);

	/*
	 * Discard moved (and possibly truncated away) the pages this backend's
	 * head page cache may still name.  Drop the cache entry so the next
	 * reserve re-reads the metapage instead of faulting on a stale block
	 * number that no longer exists on disk.
	 */
	RelUndoHeadCacheInvalidate(RelationGetRelid(rel));

	UnlockReleaseBuffer(metabuf);
}

/*
 * RelUndoDiscardSlot
 *		Discard the tail run of one head slot's chain.
 *
 * Walks the slot's page chain from the head toward the tail.  Any page whose
 * max_xid precedes oldest_xmin holds only records that no active transaction
 * can still need for rollback; such pages are unlinked from the data chain and
 * spliced onto the shared free list in one WAL-logged operation.
 *
 * The chain is chronologically ordered (head newest, tail oldest), so the
 * discardable pages form a contiguous run at the tail end.  The caller holds
 * metabuf pinned and exclusively locked.
 */
static void
RelUndoDiscardSlot(Relation rel, Buffer metabuf, int slot,
				   TransactionId oldest_xmin)
{
	Page		metapage = BufferGetPage(metabuf);
	RelUndoMetaPage meta = (RelUndoMetaPage) PageGetContents(metapage);
	BlockNumber old_tail_blkno;
	BlockNumber new_tail_blkno = InvalidBlockNumber;
	BlockNumber run_head_blkno = InvalidBlockNumber;
	BlockNumber current_blkno;
	BlockNumber old_free_head;
	uint32		npages_freed = 0;
	Buffer		runtail_buf;
	Buffer		newtail_buf = InvalidBuffer;

	old_tail_blkno = meta->tail_blkno[slot];
	old_free_head = meta->free_blkno;

	if (!BlockNumberIsValid(old_tail_blkno))
	{
		/* Empty chain, nothing to discard */
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
	current_blkno = meta->head_blkno[slot];
	while (BlockNumberIsValid(current_blkno) && current_blkno != RELUNDO_METAPAGE_BLKNO)
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

		/*
		 * Data pages are block >= 1; the metapage is block 0 and the caller
		 * already holds it EXCLUSIVE.  A well-formed chain terminates with
		 * InvalidBlockNumber, never 0.  Guard against a 0 (or meta-block) link
		 * so a malformed/uninitialized chain head cannot self-deadlock by
		 * re-locking the metapage.
		 */
		if (prev == RELUNDO_METAPAGE_BLKNO)
			break;
		current_blkno = prev;
	}

	if (!BlockNumberIsValid(new_tail_blkno))
	{
		/* Whole chain is discardable: the run starts at the chain head. */
		run_head_blkno = meta->head_blkno[slot];
	}

	if (!BlockNumberIsValid(run_head_blkno))
	{
		/* Nothing below the new tail is discardable. */
		return;
	}

	/*
	 * Pass 2 (read-only): count the pages in the discardable run, walking
	 * from run_head down to (and including) the old tail.
	 */
	current_blkno = run_head_blkno;
	while (BlockNumberIsValid(current_blkno) && current_blkno != RELUNDO_METAPAGE_BLKNO)
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
		UnlockReleaseBuffer(buf);
		npages_freed++;
		if (prev == RELUNDO_METAPAGE_BLKNO)
			break;
		current_blkno = prev;
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

			meta->tail_blkno[slot] = new_tail_blkno;
		}
		else
		{
			/* Whole chain discarded: this slot's data chain is now empty. */
			meta->head_blkno[slot] = InvalidBlockNumber;
			meta->tail_blkno[slot] = InvalidBlockNumber;
		}

		meta->free_blkno = run_head_blkno;
		meta->discarded_records += npages_freed;	/* approximate */
		MarkBufferDirty(metabuf);
	}

	/* WAL-log the discard operation. */
	START_CRIT_SECTION();
	{
		xl_relundo_discard xlrec;
		XLogRecPtr	lsn;

		xlrec.old_tail_blkno = old_tail_blkno;
		xlrec.new_tail_blkno = meta->tail_blkno[slot];
		xlrec.free_head_blkno = run_head_blkno;
		xlrec.old_free_head = old_free_head;
		xlrec.discard_xid = oldest_xmin;
		xlrec.npages_freed = npages_freed;
		xlrec.slot = (uint16) slot;

		XLogBeginInsert();
		XLogRegisterData((char *) &xlrec, SizeOfRelundoDiscard);

		/* Block 0: metapage (tail + free-list head). */
		XLogRegisterBuffer(0, metabuf, REGBUF_STANDARD);

		/*
		 * Blocks 1 and 2 are data pages, which pin the standard PageHeader
		 * pd_lower at the empty value and track their real extent in the shadow
		 * RelUndoPageHeader.  REGBUF_STANDARD would elide the whole contents as
		 * a free "hole", so a restored FPI would return a zeroed page and lose
		 * the prev_blkno chain links.  Log the full page image (flag 0).
		 */

		/* Block 1: run tail, whose prev_blkno now links to old_free_head. */
		XLogRegisterBuffer(1, runtail_buf, 0);

		/* Block 2: new live tail, whose prev_blkno is cleared (if present). */
		if (BufferIsValid(newtail_buf))
			XLogRegisterBuffer(2, newtail_buf, 0);

		lsn = XLogInsert(RM_RELUNDO_ID, XLOG_RELUNDO_DISCARD);

		/*
		 * Stamp the record LSN onto every page we dirtied and registered so
		 * the buffer manager cannot flush any of them to disk ahead of this
		 * WAL record (WAL-before-data).  Otherwise a crash could leave a page's
		 * spliced prev_blkno link on disk without the matching WAL, corrupting
		 * the free-list / data-chain threading.
		 */
		PageSetLSN(metapage, lsn);
		PageSetLSN(BufferGetPage(runtail_buf), lsn);
		if (BufferIsValid(newtail_buf))
			PageSetLSN(BufferGetPage(newtail_buf), lsn);
	}
	END_CRIT_SECTION();

	if (BufferIsValid(newtail_buf))
		UnlockReleaseBuffer(newtail_buf);
	UnlockReleaseBuffer(runtail_buf);
}

/*
 * RelUndoTruncateEmptyChain
 *		Physically truncate an emptied UNDO fork back to the metapage.
 *
 * Precondition: the caller holds metabuf pinned and exclusively locked, and
 * the data chain is empty (head_blkno == tail_blkno == InvalidBlockNumber)
 * after a whole-chain discard.  In that state the free list contains every
 * data block ever allocated, i.e. the contiguous suffix [1 .. watermark], so
 * the fork can be truncated to a single block (the metapage).
 *
 * Physical truncation drops buffers beyond the new EOF, so it must not run
 * while a concurrent lock-free reserver holds a pin on (and is about to write
 * to) one of those data pages.  VACUUM holds only ShareUpdateExclusiveLock,
 * which does not exclude the RowExclusiveLock that DML reservers hold, so we
 * gate the truncate behind a *conditional* AccessExclusiveLock (mirroring
 * lazy_truncate_heap()).  If the lock is not immediately available, we skip
 * the physical reclaim entirely: the pages remain on the free list and are
 * safely recyclable on the next allocation, so nothing is lost but disk
 * space until a later discard succeeds in acquiring the lock.
 *
 * When the lock is held, this mirrors the crash-safe WAL-logged truncate
 * pattern in RelationTruncate(): set delayChkptFlags, enter the critical
 * section, mutate the metapage and WAL-log it, XLogFlush, then smgrtruncate
 * (which drops the now-defunct buffers), leave the critical section, and
 * finally clear delayChkptFlags.  Dirtying the metapage and setting the
 * checkpoint-delay flags both happen inside the critical section, so a
 * concurrent checkpoint can never observe a half-applied truncate.
 */
static void
RelUndoTruncateEmptyChain(Relation rel, Buffer metabuf)
{
	Page		metapage = BufferGetPage(metabuf);
	RelUndoMetaPage meta = (RelUndoMetaPage) PageGetContents(metapage);
	SMgrRelation srel = RelationGetSmgr(rel);
	ForkNumber	forknum = RELUNDO_FORKNUM;
	BlockNumber old_nblocks;
	BlockNumber new_nblocks = 1;	/* keep only the metapage (block 0) */

	/* Caller guarantees every slot's chain was discarded. */
#ifdef USE_ASSERT_CHECKING
	for (int slot = 0; slot < RELUNDO_NUM_HEADS; slot++)
	{
		Assert(!BlockNumberIsValid(meta->head_blkno[slot]));
		Assert(!BlockNumberIsValid(meta->tail_blkno[slot]));
	}
#endif

	old_nblocks = smgrnblocks(srel, RELUNDO_FORKNUM);

	/* Nothing to reclaim if the fork is already just the metapage. */
	if (old_nblocks <= new_nblocks)
		return;

	/*
	 * With the whole chain discarded, the free list holds every data block the
	 * metapage durably knows about -- the contiguous suffix [1 .. watermark].
	 * The watermark is advanced in the same WAL-logged metapage mutation that
	 * threads a freshly extended block onto the chain, but ExtendBufferedRel
	 * grows the file before that record is flushed.  A crash in that window
	 * leaves orphaned tail blocks (watermark < nblocks - 1) that belong to no
	 * chain and no free list.  Truncating to the metapage drops them too, which
	 * is safe under the AccessExclusiveLock gate below since no reserver can
	 * hold a pin.  The watermark can never exceed the physical EOF, so assert
	 * only that bound.  It may also be invalid if the only blocks on disk are
	 * orphans from a torn extend whose metapage update never reached WAL.
	 */
	Assert(!BlockNumberIsValid(meta->system_alloc_watermark) ||
		   meta->system_alloc_watermark <= old_nblocks - 1);

	/*
	 * Gate the physical truncate behind a conditional AccessExclusiveLock so
	 * we never drop a buffer that a concurrent lock-free reserver has pinned.
	 * If the lock is unavailable, leave the pages on the free list (still
	 * recyclable) and reclaim the disk on a future discard.
	 */
	if (!ConditionalLockRelation(rel, AccessExclusiveLock))
		return;

	Assert((MyProc->delayChkptFlags &
			(DELAY_CHKPT_START | DELAY_CHKPT_COMPLETE)) == 0);

	START_CRIT_SECTION();
	{
		xl_relundo_truncate xlrec;
		XLogRecPtr	lsn;

		MyProc->delayChkptFlags |= DELAY_CHKPT_START | DELAY_CHKPT_COMPLETE;

		/*
		 * Reset metapage to the empty-fork state: the data chain is already
		 * empty (head/tail cleared by the discard above) and the freed pages
		 * cease to exist.  Reset all four pointers here too so the truncate
		 * WAL record is self-describing and its redo does not depend on the
		 * preceding discard record having been flushed.
		 */
		for (int slot = 0; slot < RELUNDO_NUM_HEADS; slot++)
		{
			meta->head_blkno[slot] = InvalidBlockNumber;
			meta->tail_blkno[slot] = InvalidBlockNumber;
		}
		meta->free_blkno = InvalidBlockNumber;
		meta->system_alloc_watermark = InvalidBlockNumber;
		MarkBufferDirty(metabuf);

		xlrec.new_nblocks = new_nblocks;

		XLogBeginInsert();
		XLogRegisterData((char *) &xlrec, SizeOfRelundoTruncate);

		/* Block 0: metapage (free-list head + watermark reset). */
		XLogRegisterBuffer(0, metabuf, REGBUF_STANDARD);

		lsn = XLogInsert(RM_RELUNDO_ID, XLOG_RELUNDO_TRUNCATE);
		PageSetLSN(metapage, lsn);

		/*
		 * Flush so the truncation cannot reach disk before its WAL record;
		 * smgrtruncate drops the to-be-removed buffers and shrinks the file.
		 */
		XLogFlush(lsn);

		smgrtruncate(srel, &forknum, 1, &old_nblocks, &new_nblocks);
	}
	END_CRIT_SECTION();

	MyProc->delayChkptFlags &= ~(DELAY_CHKPT_START | DELAY_CHKPT_COMPLETE);

	UnlockRelation(rel, AccessExclusiveLock);
}
