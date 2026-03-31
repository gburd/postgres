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

	buf = ReadBufferExtended(rel, RELUNDO_FORKNUM, 0, RBM_NORMAL, NULL);
	LockBuffer(buf, mode);

	page = BufferGetPage(buf);
	meta = (RelUndoMetaPage) PageGetContents(page);

	if (meta->magic != RELUNDO_METAPAGE_MAGIC)
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("invalid magic number in UNDO metapage of relation \"%s\"",
						RelationGetRelationName(rel)),
				 errdetail("Expected 0x%08X, found 0x%08X.",
						   RELUNDO_METAPAGE_MAGIC, meta->magic)));

	if (meta->version != RELUNDO_METAPAGE_VERSION)
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("unsupported UNDO metapage version %u in relation \"%s\"",
						meta->version, RelationGetRelationName(rel))));

	return buf;
}

/*
 * relundo_allocate_page
 *		Allocate a new UNDO page and add it to the head of the chain.
 *
 * The metapage buffer must be pinned and exclusively locked by the caller.
 * Returns the new block number and the pinned/exclusively-locked buffer
 * via *newbuf.  The metapage is updated (head_blkno) and marked dirty.
 */
BlockNumber
relundo_allocate_page(Relation rel, Buffer metabuf, Buffer *newbuf)
{
	Page		metapage;
	RelUndoMetaPage meta;
	BlockNumber newblkno;
	BlockNumber old_head;
	Buffer		buf;
	Page		page;

	metapage = BufferGetPage(metabuf);
	meta = (RelUndoMetaPage) PageGetContents(metapage);

	old_head = meta->head_blkno;

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

		/* Re-initialize the page for use as a data page */
		relundo_init_page(freepage, old_head, meta->counter);

		/*
		 * Increment the generation counter so the next page gets a different
		 * counter value. This is critical: UNDO pointers embed the counter to
		 * distinguish different generations of the same physical page. Without
		 * incrementing, reused pages would have the same counter as their
		 * previous incarnation, breaking UNDO chain traversal.
		 *
		 * Counter 0 is reserved for InvalidRelUndoRecPtr, so wrap to 1.
		 */
		meta->counter++;
		if (meta->counter == 0)
			meta->counter = 1;

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

		/*
		 * Increment the generation counter so the next page gets a different
		 * counter value. This is critical: UNDO pointers embed the counter to
		 * distinguish different generations of the same physical page. Without
		 * incrementing, reused pages would have the same counter as their
		 * previous incarnation, breaking UNDO chain traversal.
		 *
		 * Counter 0 is reserved for InvalidRelUndoRecPtr, so wrap to 1.
		 */
		meta->counter++;
		if (meta->counter == 0)
			meta->counter = 1;

		MarkBufferDirty(buf);
	}

	/* Update metapage: new head */
	meta->head_blkno = newblkno;

	/* If this is the first data page, it's also the tail */
	if (!BlockNumberIsValid(old_head))
		meta->tail_blkno = newblkno;

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
