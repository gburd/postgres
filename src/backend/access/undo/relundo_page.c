/*-------------------------------------------------------------------------
 *
 * relundo_page.c
 *	  Per-relation UNDO page management
 *
 * This file handles UNDO page allocation, metapage management, and chain
 * traversal for per-relation UNDO logs.
 *
 * The UNDO fork layout is:
 *   Block 0:  Metapage (standard PageHeaderData + RelUndoMetaPageData)
 *   Block 1+: Data pages (standard PageHeaderData + RelUndoPageHeaderData + records)
 *
 * Data pages grow from the bottom up: pd_lower advances as records are
 * appended.  All offsets in RelUndoPageHeaderData are relative to the
 * start of the page contents area (after standard PageHeaderData).
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/undo/relundo_page.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/relundo.h"
#include "common/relpath.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/smgr.h"

/*
 * relundo_get_metapage
 *		Read and pin the metapage for a relation's UNDO fork.
 *
 * The caller specifies the lock mode (BUFFER_LOCK_SHARE or
 * BUFFER_LOCK_EXCLUSIVE).  Returns a pinned and locked buffer.
 * The caller must release the buffer when done.
 */
Buffer
relundo_get_metapage(Relation rel, int mode)
{
	Buffer		buf;
	Page		page;
	RelUndoMetaPage meta;

	/*
	 * If the RELUNDO fork has no blocks (e.g., after crash recovery where the
	 * fork was created but the metapage wasn't written), create and
	 * initialize the metapage now.
	 */
	if (smgrnblocks(RelationGetSmgr(rel), RELUNDO_FORKNUM) == 0)
	{
		if (mode == BUFFER_LOCK_EXCLUSIVE)
		{
			elog(LOG, "UNDO fork for relation \"%s\" has no blocks, initializing metapage",
				 RelationGetRelationName(rel));

			buf = ExtendBufferedRel(BMR_REL(rel), RELUNDO_FORKNUM, NULL,
									EB_LOCK_FIRST);
			Assert(BufferGetBlockNumber(buf) == 0);

			page = BufferGetPage(buf);
			PageInit(page, BLCKSZ, 0);
			meta = (RelUndoMetaPage) PageGetContents(page);
			meta->magic = RELUNDO_METAPAGE_MAGIC;
			meta->version = RELUNDO_METAPAGE_VERSION;
			meta->counter = 1;
			for (int s = 0; s < RELUNDO_NUM_HEADS; s++)
			{
				meta->head_blkno[s] = InvalidBlockNumber;
				meta->tail_blkno[s] = InvalidBlockNumber;
			}
			meta->free_blkno = InvalidBlockNumber;
			meta->total_records = 0;
			meta->discarded_records = 0;
			meta->system_alloc_watermark = InvalidBlockNumber;

			/* Include the meta struct in the recorded region of any FPI. */
			RelUndoMetaPageSetPdLower(page);

			MarkBufferDirty(buf);

			/* Downgrade lock if caller only wants SHARE */
			if (mode == BUFFER_LOCK_SHARE)
			{
				LockBuffer(buf, BUFFER_LOCK_UNLOCK);
				LockBuffer(buf, BUFFER_LOCK_SHARE);
			}

			return buf;
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg("UNDO fork for relation \"%s\" has no blocks",
							RelationGetRelationName(rel))));
		}
	}

	buf = ReadBufferExtended(rel, RELUNDO_FORKNUM, 0, RBM_NORMAL, NULL);
	LockBuffer(buf, mode);

	page = BufferGetPage(buf);
	meta = (RelUndoMetaPage) PageGetContents(page);

	if (meta->magic != RELUNDO_METAPAGE_MAGIC)
	{
		/*
		 * The metapage magic is invalid.  This can happen after crash
		 * recovery if the RELUNDO fork was created but the metapage
		 * initialization WAL record wasn't replayed (e.g., the crash occurred
		 * between smgrcreate and the metapage write).
		 *
		 * Reinitialize the metapage so subsequent UNDO operations can
		 * proceed.  This is safe because an uninitialized metapage means no
		 * UNDO records were ever written, so there's nothing to lose.
		 */
		/*
		 * Upgrade to exclusive lock if needed for reinitialization.
		 */
		if (mode != BUFFER_LOCK_EXCLUSIVE)
		{
			LockBuffer(buf, BUFFER_LOCK_UNLOCK);
			LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
			page = BufferGetPage(buf);
			meta = (RelUndoMetaPage) PageGetContents(page);

			/* Re-check after acquiring exclusive lock */
			if (meta->magic == RELUNDO_METAPAGE_MAGIC)
			{
				/* Another backend fixed it while we re-locked */
				LockBuffer(buf, BUFFER_LOCK_UNLOCK);
				LockBuffer(buf, mode);
				return buf;
			}
		}

		elog(LOG, "reinitializing corrupted UNDO metapage for relation \"%s\" "
			 "(found magic 0x%08X, expected 0x%08X)",
			 RelationGetRelationName(rel), meta->magic,
			 RELUNDO_METAPAGE_MAGIC);

		PageInit(page, BLCKSZ, 0);
		meta = (RelUndoMetaPage) PageGetContents(page);
		meta->magic = RELUNDO_METAPAGE_MAGIC;
		meta->version = RELUNDO_METAPAGE_VERSION;
		meta->counter = 1;
		for (int s = 0; s < RELUNDO_NUM_HEADS; s++)
		{
			meta->head_blkno[s] = InvalidBlockNumber;
			meta->tail_blkno[s] = InvalidBlockNumber;
		}
		meta->free_blkno = InvalidBlockNumber;
		meta->total_records = 0;
		meta->discarded_records = 0;
		meta->system_alloc_watermark = InvalidBlockNumber;

		/* Include the meta struct in the recorded region of any FPI. */
		RelUndoMetaPageSetPdLower(page);

		MarkBufferDirty(buf);

		/* Downgrade back to requested lock mode */
		if (mode != BUFFER_LOCK_EXCLUSIVE)
		{
			LockBuffer(buf, BUFFER_LOCK_UNLOCK);
			LockBuffer(buf, mode);
		}
	}

	if (meta->version != RELUNDO_METAPAGE_VERSION)
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("unsupported UNDO metapage version %u in relation \"%s\" (expected %u)",
						meta->version, RelationGetRelationName(rel),
						RELUNDO_METAPAGE_VERSION)));

	return buf;
}

/*
 * relundo_allocate_page
 *		Allocate a new UNDO page and add it to the head of the slot's chain.
 *
 * The metapage buffer must be pinned and exclusively locked by the caller.
 * Returns the new block number and the pinned/exclusively-locked buffer
 * via *newbuf.  The metapage is updated (head_blkno[slot]) and marked dirty.
 *
 * slot selects which of the RELUNDO_NUM_HEADS independent append chains the
 * new page joins; the free list and the generation counter are shared across
 * all slots (both are protected by the same metapage exclusive lock held here).
 */
BlockNumber
relundo_allocate_page(Relation rel, Buffer metabuf, int slot, Buffer *newbuf)
{
	Page		metapage;
	RelUndoMetaPage meta;
	BlockNumber newblkno;
	BlockNumber old_head;
	Buffer		buf;
	Page		page;

	Assert(slot >= 0 && slot < RELUNDO_NUM_HEADS);

	metapage = BufferGetPage(metabuf);
	meta = (RelUndoMetaPage) PageGetContents(metapage);

	old_head = meta->head_blkno[slot];

	/* Try the free list first */
	if (BlockNumberIsValid(meta->free_blkno))
	{
		Buffer		freebuf;
		Page		freepage;
		RelUndoPageHeader freehdr;

		newblkno = meta->free_blkno;

		freebuf = ReadBufferExtended(rel, RELUNDO_FORKNUM, newblkno,
									 RBM_NORMAL, NULL);
		LockBuffer(freebuf, BUFFER_LOCK_EXCLUSIVE);

		freepage = BufferGetPage(freebuf);
		freehdr = (RelUndoPageHeader) PageGetContents(freepage);

		/*
		 * The free list is threaded through prev_blkno.  Pop the head of the
		 * free list.
		 */
		meta->free_blkno = freehdr->prev_blkno;

		/*
		 * ABA defense for the WS-PVS2 reader: a recycled block reused at the
		 * same (blkno, offset) as a discarded prior record would otherwise be
		 * indistinguishable from that prior record to a stale verptr.  Bump
		 * the generation counter so the recycled page's hdr->counter differs
		 * from any previous one at this blkno; RelUndoReadRecord validates
		 * (RelUndoGetCounter(ptr) == hdr->counter) to reject stale verptrs.
		 *
		 * Modular 16-bit increment, skipping 0 (reserved for "uninitialized"
		 * so a zeroed page never aliases a live counter).  16-bit wraparound
		 * (every 65535 recycles of the same fork) can produce a counter that
		 * matches a long-ago page header — RelUndoReadRecord then returns
		 * false (chain-end / best-effort fallback), never returning wrong
		 * data.  No further handling needed.
		 *
		 * Retention invariant: the oldest_xmin discard gate already protects
		 * every record the reader REVERSE-APPLIES (urec_xid >= oldest_xmin,
		 * thus non-discardable).  This counter+validation only protects the
		 * single visibility-PROBE record the reader STOPS on, whose urec_xid
		 * may legitimately be < oldest_xmin and thus reside on a discardable
		 * (and reusable) page.
		 *
		 * Only the recycle branch needs the bump: a freshly extended block
		 * has never been the target of any verptr, so it has no ABA hazard.
		 * The counter need not be globally unique per page; uniqueness for
		 * the reader comes from a GIVEN blkno getting a different counter
		 * each time IT is recycled.
		 */
		meta->counter++;
		if (meta->counter == 0)
			meta->counter = 1;

		/* Re-initialize the page for use as a data page */
		relundo_init_page(freepage, old_head, meta->counter);

		MarkBufferDirty(freebuf);
		buf = freebuf;
	}
	else
	{
		/* Extend the relation to get a new block */
		buf = ExtendBufferedRel(BMR_REL(rel), RELUNDO_FORKNUM, NULL,
								EB_LOCK_FIRST);
		newblkno = BufferGetBlockNumber(buf);

		page = BufferGetPage(buf);
		relundo_init_page(page, old_head, meta->counter);

		MarkBufferDirty(buf);
	}

	/* Update metapage: new head of this slot's chain */
	meta->head_blkno[slot] = newblkno;

	/* If this is the first data page in the slot, it's also the tail */
	if (!BlockNumberIsValid(old_head))
		meta->tail_blkno[slot] = newblkno;

	/*
	 * Track system allocation watermark.  This records the highest block
	 * number allocated, enabling efficient reclamation of pages that were
	 * allocated by a system transaction but never used (because the user
	 * transaction aborted).
	 *
	 * Verified: The metapage is WAL-logged via REGBUF_STANDARD as block 1 in
	 * the caller's XLOG_RELUNDO_INSERT record (see RelUndoFinish). On crash
	 * recovery the FPI restores all metapage fields including
	 * system_alloc_watermark.
	 */
	if (!BlockNumberIsValid(meta->system_alloc_watermark) ||
		newblkno > meta->system_alloc_watermark)
		meta->system_alloc_watermark = newblkno;

	MarkBufferDirty(metabuf);

	*newbuf = buf;
	return newblkno;
}

/*
 * relundo_init_page
 *		Initialize a new UNDO data page.
 *
 * Uses standard PageInit for compatibility with the buffer manager's
 * page verification, then sets up the RelUndoPageHeaderData in the
 * contents area.
 *
 * pd_lower starts just after the UNDO page header; pd_upper is set to
 * the full extent of the contents area.
 */
void
relundo_init_page(Page page, BlockNumber prev_blkno, uint16 counter)
{
	RelUndoPageHeader hdr;

	/* Initialize with standard page header (no special area) */
	PageInit(page, BLCKSZ, 0);

	/* Set up our UNDO-specific header in the page contents area */
	hdr = (RelUndoPageHeader) PageGetContents(page);
	hdr->prev_blkno = prev_blkno;
	hdr->max_xid = InvalidTransactionId;
	hdr->counter = counter;
	hdr->pd_lower = SizeOfRelUndoPageHeaderData;
	hdr->pd_upper = BLCKSZ - MAXALIGN(SizeOfPageHeaderData);
}

/*
 * relundo_get_free_space
 *		Get amount of free space on an UNDO page.
 *
 * Returns the number of bytes available for new UNDO records.
 * The offsets in the page header are relative to the contents area.
 */
Size
relundo_get_free_space(Page page)
{
	RelUndoPageHeader hdr;

	hdr = (RelUndoPageHeader) PageGetContents(page);

	if (hdr->pd_upper <= hdr->pd_lower)
		return 0;

	return (Size) (hdr->pd_upper - hdr->pd_lower);
}
