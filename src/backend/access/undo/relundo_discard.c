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

#include "access/index_prune.h"
#include "access/relundo.h"
#include "access/relundo_xlog.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "common/relpath.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/smgr.h"

/*
 * relundo_counter_precedes
 *		Compare two counter values handling 16-bit wraparound.
 *
 * Uses modular arithmetic: counter1 "precedes" counter2 if the signed
 * difference (counter1 - counter2) is negative but not more negative
 * than half the counter space (32768).
 *
 * This correctly handles wraparound and mirrors the logic used by
 * TransactionIdPrecedes() for 32-bit XIDs.
 */
bool
relundo_counter_precedes(uint16 counter1, uint16 counter2)
{
	int32		diff = (int32) counter1 - (int32) counter2;

	return (diff < 0) && (diff > -32768);
}

/*
 * relundo_page_is_discardable
 *		Check if all records on a page are older than the cutoff counter.
 *
 * Returns true if the page's generation counter precedes
 * oldest_visible_counter, meaning all records on this page are
 * invisible to all active transactions and can be discarded.
 */
static bool
relundo_page_is_discardable(Page page, uint16 oldest_visible_counter)
{
	RelUndoPageHeader hdr;

	hdr = (RelUndoPageHeader) PageGetContents(page);

	return relundo_counter_precedes(hdr->counter, oldest_visible_counter);
}

/*
 * relundo_free_page
 *		Free an UNDO page and add it to the free list.
 *
 * The page's prev_blkno is overwritten with the current free list head,
 * and the metapage's free_blkno is updated to point to this page.
 * Both the page buffer and metapage buffer are marked dirty.
 *
 * The page buffer is released after updating.
 */
static void
relundo_free_page(Relation rel, Buffer pagebuf, Buffer metabuf)
{
	Page		metapage;
	RelUndoMetaPage meta;
	Page		page;
	RelUndoPageHeader hdr;

	metapage = BufferGetPage(metabuf);
	meta = (RelUndoMetaPage) PageGetContents(metapage);

	page = BufferGetPage(pagebuf);
	hdr = (RelUndoPageHeader) PageGetContents(page);

	/* Thread onto free list: this page's prev points to old free head */
	hdr->prev_blkno = meta->free_blkno;

	/* Update metapage free list head */
	meta->free_blkno = BufferGetBlockNumber(pagebuf);

	MarkBufferDirty(pagebuf);
	MarkBufferDirty(metabuf);

	UnlockReleaseBuffer(pagebuf);
}

/*
 * relundo_block_exists
 *		Check if a block number is within the UNDO fork's physical size.
 *
 * Returns true if the block exists on disk, false otherwise.
 * This guards against reading blocks that were allocated but never
 * flushed, or blocks referenced by a stale metapage after a crash.
 */
static bool
relundo_block_exists(Relation rel, BlockNumber blkno)
{
	SMgrRelation srel = RelationGetSmgr(rel);
	BlockNumber nblocks;

	if (!smgrexists(srel, RELUNDO_FORKNUM))
		return false;

	nblocks = smgrnblocks(srel, RELUNDO_FORKNUM);
	return (blkno < nblocks);
}

/*
 * RelUndoDiscard
 *		Discard old UNDO records and reclaim space.
 *
 * Walks the page chain from the tail toward the head.  For each page
 * whose counter precedes oldest_visible_counter, the page is unlinked
 * from the data chain and added to the free list.
 *
 * The walk stops as soon as we find a page that is NOT discardable,
 * since all newer pages (toward head) will have equal or later counters.
 *
 * WAL logging is deferred to Phase 3.
 */
void
RelUndoDiscard(Relation rel, uint16 oldest_visible_counter)
{
	Buffer		metabuf;
	Page		metapage;
	RelUndoMetaPage meta;
	BlockNumber tail_blkno;
	uint32		npages_freed = 0;

	/* Lock the metapage exclusively for the duration of discard */
	metabuf = relundo_get_metapage(rel, BUFFER_LOCK_EXCLUSIVE);
	metapage = BufferGetPage(metabuf);
	meta = (RelUndoMetaPage) PageGetContents(metapage);

	tail_blkno = meta->tail_blkno;

	/*
	 * Validate metapage pointers before attempting to walk the chain.
	 * If the pointers reference blocks beyond the fork size, reset the chain.
	 */
	if (BlockNumberIsValid(meta->head_blkno) &&
		!relundo_block_exists(rel, meta->head_blkno))
	{
		elog(WARNING, "UNDO head block %u does not exist, resetting chain", meta->head_blkno);
		meta->head_blkno = InvalidBlockNumber;
		meta->tail_blkno = InvalidBlockNumber;
		MarkBufferDirty(metabuf);
		UnlockReleaseBuffer(metabuf);
		return;
	}

	if (BlockNumberIsValid(tail_blkno) &&
		!relundo_block_exists(rel, tail_blkno))
	{
		elog(WARNING, "UNDO tail block %u does not exist, resetting chain", tail_blkno);
		meta->head_blkno = InvalidBlockNumber;
		meta->tail_blkno = InvalidBlockNumber;
		MarkBufferDirty(metabuf);
		UnlockReleaseBuffer(metabuf);
		return;
	}

	/*
	 * Walk from tail toward head, freeing discardable pages.
	 *
	 * The chain is: head -> ... -> prev -> ... -> tail But we can't walk
	 * forward from the tail since pages only have prev_blkno pointers (toward
	 * tail).  Instead we need to find the page that *points to* the tail (the
	 * "next" page toward head).
	 *
	 * However, for discard we can use a simpler approach: since we're
	 * removing from the tail, we need to find the new tail.  We walk from the
	 * head toward the tail, collecting pages.  But that's expensive.
	 *
	 * Actually, we can use an iterative approach: read the tail, check if
	 * discardable.  If so, we need the page whose prev_blkno == tail_blkno.
	 * But we don't have a next pointer.
	 *
	 * The simplest approach: walk from the head and build a stack of pages to
	 * discard.  Since pages are chronologically ordered (head is newest, tail
	 * is oldest), we walk from head following prev_blkno links until we find
	 * non-discardable pages, then free everything beyond.
	 *
	 * For large chains this could be expensive, but VACUUM runs periodically
	 * so the number of pages to walk is bounded in practice.
	 */

	if (!BlockNumberIsValid(tail_blkno))
	{
		/* Empty chain, nothing to discard */
		UnlockReleaseBuffer(metabuf);
		return;
	}

	/*
	 * Walk from head toward tail to find the new tail boundary. We want to
	 * keep pages whose counter >= oldest_visible_counter.
	 */
	{
		BlockNumber current_blkno;
		BlockNumber new_tail_blkno = InvalidBlockNumber;
		BlockNumber prev_of_new_tail = InvalidBlockNumber;

		/*
		 * Walk from head following prev_blkno links.  The last page we see
		 * that is NOT discardable becomes the new tail.
		 */
		current_blkno = meta->head_blkno;

		while (BlockNumberIsValid(current_blkno))
		{
			Buffer		buf;
			Page		page;
			RelUndoPageHeader hdr;
			BlockNumber prev;

			/*
			 * Check that this block physically exists before reading it.
			 * After a crash, the metapage may reference blocks that were
			 * allocated but never flushed to disk, or the fork may be
			 * shorter than expected.  In that case, stop walking the chain.
			 */
			if (!relundo_block_exists(rel, current_blkno))
			{
				elog(WARNING, "UNDO discard: block %u does not exist in fork (relation \"%s\"), truncating chain walk",
					 current_blkno, RelationGetRelationName(rel));
				break;
			}

			buf = ReadBufferExtended(rel, RELUNDO_FORKNUM, current_blkno,
									 RBM_NORMAL, NULL);
			LockBuffer(buf, BUFFER_LOCK_SHARE);

			page = BufferGetPage(buf);
			hdr = (RelUndoPageHeader) PageGetContents(page);
			prev = hdr->prev_blkno;

			if (!relundo_page_is_discardable(page, oldest_visible_counter))
			{
				/* This page is still live; it might be the new tail */
				new_tail_blkno = current_blkno;
				prev_of_new_tail = prev;
			}

			UnlockReleaseBuffer(buf);
			current_blkno = prev;
		}

		/*
		 * If all pages are discardable (new_tail_blkno is invalid), free
		 * everything and leave the chain empty.
		 */
		if (!BlockNumberIsValid(new_tail_blkno))
		{
			/* Free all pages from head to tail */
			current_blkno = meta->head_blkno;
			while (BlockNumberIsValid(current_blkno))
			{
				Buffer		buf;
				Page		page;
				RelUndoPageHeader hdr;
				BlockNumber prev;

				if (!relundo_block_exists(rel, current_blkno))
				{
					elog(WARNING, "UNDO discard: block %u does not exist in fork (relation \"%s\"), stopping free walk",
						 current_blkno, RelationGetRelationName(rel));
					break;
				}

				buf = ReadBufferExtended(rel, RELUNDO_FORKNUM, current_blkno,
										 RBM_NORMAL, NULL);
				LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

				page = BufferGetPage(buf);
				hdr = (RelUndoPageHeader) PageGetContents(page);
				prev = hdr->prev_blkno;

				relundo_free_page(rel, buf, metabuf);
				npages_freed++;

				current_blkno = prev;
			}

			meta->head_blkno = InvalidBlockNumber;
			meta->tail_blkno = InvalidBlockNumber;
		}
		else if (BlockNumberIsValid(prev_of_new_tail))
		{
			/*
			 * Free pages from prev_of_new_tail backward to the old tail. Then
			 * update the new tail's prev_blkno to InvalidBlockNumber.
			 */
			current_blkno = prev_of_new_tail;
			while (BlockNumberIsValid(current_blkno))
			{
				Buffer		buf;
				Page		page;
				RelUndoPageHeader hdr;
				BlockNumber prev;

				if (!relundo_block_exists(rel, current_blkno))
				{
					elog(WARNING, "UNDO discard: block %u does not exist in fork (relation \"%s\"), stopping free walk",
						 current_blkno, RelationGetRelationName(rel));
					break;
				}

				buf = ReadBufferExtended(rel, RELUNDO_FORKNUM, current_blkno,
										 RBM_NORMAL, NULL);
				LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

				page = BufferGetPage(buf);
				hdr = (RelUndoPageHeader) PageGetContents(page);
				prev = hdr->prev_blkno;

				relundo_free_page(rel, buf, metabuf);
				npages_freed++;

				current_blkno = prev;
			}

			/* Update the new tail: clear its prev link */
			if (relundo_block_exists(rel, new_tail_blkno))
			{
				Buffer		tailbuf;
				Page		tailpage;
				RelUndoPageHeader tailhdr;

				tailbuf = ReadBufferExtended(rel, RELUNDO_FORKNUM,
											 new_tail_blkno,
											 RBM_NORMAL, NULL);
				LockBuffer(tailbuf, BUFFER_LOCK_EXCLUSIVE);

				tailpage = BufferGetPage(tailbuf);
				tailhdr = (RelUndoPageHeader) PageGetContents(tailpage);
				tailhdr->prev_blkno = InvalidBlockNumber;

				MarkBufferDirty(tailbuf);
				UnlockReleaseBuffer(tailbuf);
			}
			else
			{
				elog(WARNING, "UNDO tail block %u does not exist, truncating discard", new_tail_blkno);
			}

			meta->tail_blkno = new_tail_blkno;
		}
		/* else: tail hasn't changed, nothing to discard */
	}

	if (npages_freed > 0)
	{
		meta->discarded_records += npages_freed;	/* approximate */

		/*
		 * Notify all indexes on this relation that UNDO records have been
		 * discarded. This allows indexes to proactively mark dead entries,
		 * reducing VACUUM work.
		 */
		IndexPruneNotifyDiscard(rel, oldest_visible_counter);

		/* WAL-log the discard operation */
		START_CRIT_SECTION();

		{
			xl_relundo_discard xlrec;

			xlrec.old_tail_blkno = tail_blkno;
			xlrec.new_tail_blkno = meta->tail_blkno;
			xlrec.oldest_counter = oldest_visible_counter;
			xlrec.npages_freed = npages_freed;

			XLogBeginInsert();
			XLogRegisterData((char *) &xlrec, SizeOfRelundoDiscard);

			/*
			 * Register the metapage buffer. Use REGBUF_STANDARD to allow
			 * incremental updates if the page was recently modified.
			 */
			XLogRegisterBuffer(0, metabuf, REGBUF_STANDARD);

			XLogInsert(RM_RELUNDO_ID, XLOG_RELUNDO_DISCARD);
		}

		END_CRIT_SECTION();

		MarkBufferDirty(metabuf);
	}

	UnlockReleaseBuffer(metabuf);
}
