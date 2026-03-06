/*-------------------------------------------------------------------------
 *
 * orvos_freepagemap.c
 *	  Orvos free space management
 *
 * The Free Page Map keeps track of unused pages in the relation.
 *
 * The FPM is a linked list of pages. Each page contains a pointer to the
 * next free page.

 * Design principles:
 *
 * - it's ok to have a block incorrectly stored in the FPM. Before actually
 *   reusing a page, we must check that it's safe.
 *
 * - a deletable page must be simple to detect just by looking at the page,
 *   and perhaps a few other pages. It should *not* require scanning the
 *   whole table, or even a whole b-tree. For example, if a column is dropped,
 *   we can detect if a b-tree page belongs to the dropped column just by
 *   looking at the information (the attribute number) stored in the page
 *   header.
 *
 * - if a page is deletable, it should become immediately reusable. No
 *   "wait out all possible readers that might be about to follow a link
 *   to it" business. All code that reads pages need to keep pages locked
 *   while following a link, or be prepared to retry if they land on an
 *   unexpected page.
 *
 *
 * TODO:
 *
 * - Avoid fragmentation. If B-tree page is split, try to hand out a page
 *   that's close to the old page. When the relation is extended, allocate
 *   a larger chunk at once.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/orvos/orvos_freepagemap.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "access/orvos_internal.h"
#include "miscadmin.h"
#include "storage/bufpage.h"
#include "utils/rel.h"

typedef struct ZSFreePageOpaque
{
	BlockNumber ov_next;
	uint16		padding;
	uint16		ov_page_id;		/* OV_FREE_PAGE_ID */
}			ZSFreePageOpaque;

/*
 * zspage_is_recyclable()
 *
 * Is the current page recyclable?
 *
 * It can be:
 *
 * - an empty, all-zeros page,
 * - explicitly marked as deleted,
 * - an UNDO page older than oldest_undo_ptr
 * - a b-tree page belonging to a deleted attribute
 * - a TOAST page belonging to a dead item
 *
 * TODO: currently though, we require that it's always  explicitly marked as empty.
 *
 */
static bool
zspage_is_unused(Buffer buf)
{
	Page		page;
	ZSFreePageOpaque *opaque;

	page = BufferGetPage(buf);

	if (PageIsNew(page))
		return false;

	if (PageGetSpecialSize(page) != sizeof(ZSFreePageOpaque))
		return false;
	opaque = (ZSFreePageOpaque *) PageGetSpecialPointer(page);
	if (opaque->ov_page_id != OV_FREE_PAGE_ID)
		return false;

	return true;
}

/*
 * Allocate a new page.
 *
 * The page is exclusive-locked, but not initialized.
 */
Buffer
ovpage_getnewbuf(Relation rel, Buffer metabuf)
{
	bool		release_metabuf;
	Buffer		buf;
	BlockNumber blk;
	Page		metapage;
	OVMetaPageOpaque *metaopaque;

	if (metabuf == InvalidBuffer)
	{
		metabuf = ReadBuffer(rel, OV_META_BLK);
		LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);
		release_metabuf = true;
	}
	else
		release_metabuf = false;

	metapage = BufferGetPage(metabuf);
	metaopaque = (OVMetaPageOpaque *) PageGetSpecialPointer(metapage);

	/* Get a block from the FPM. */
	blk = metaopaque->ov_fpm_head;
	if (blk == 0)
	{
		/* metapage, not expected */
		elog(ERROR, "could not find valid page in FPM");
	}
	if (blk == InvalidBlockNumber)
	{
		/* No free pages. Have to extend the relation. */
		buf = ovpage_extendrel_newbuf(rel, metabuf);
		blk = BufferGetBlockNumber(buf);
	}
	else
	{
		ZSFreePageOpaque *opaque;
		Page		page;

		buf = ReadBuffer(rel, blk);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

		/* Check that the page really is unused. */
		if (!zspage_is_unused(buf))
		{
			UnlockReleaseBuffer(buf);
			elog(ERROR, "unexpected page found in free page list");
		}
		page = BufferGetPage(buf);
		opaque = (ZSFreePageOpaque *) PageGetSpecialPointer(page);
		metaopaque->ov_fpm_head = opaque->ov_next;
	}

	if (release_metabuf)
		UnlockReleaseBuffer(metabuf);
	return buf;
}

/*
 * Extend the relation.
 *
 * Returns the new page, exclusive-locked. Also extends by additional pages
 * to reduce extension lock contention and improve spatial locality.
 */
Buffer
ovpage_extendrel_newbuf(Relation rel, Buffer metabuf)
{
	Buffer		buf;
	Buffer		local_metabuf = InvalidBuffer;
	bool		release_metabuf = false;
	Page		metapage;
	OVMetaPageOpaque *metaopaque;
	int			num_extra_pages;
	uint32		i;

	/*
	 * Determine how many extra pages to allocate. For smaller relations,
	 * allocate fewer pages. For larger relations (>1GB), allocate more
	 * pages at once to reduce lock contention.
	 */
	{
		BlockNumber nblocks = RelationGetNumberOfBlocks(rel);

		if (nblocks < 1280)		/* < 10MB */
			num_extra_pages = 8;
		else if (nblocks < 12800)	/* < 100MB */
			num_extra_pages = 32;
		else if (nblocks < 128000)	/* < 1GB */
			num_extra_pages = 128;
		else
			num_extra_pages = 512;	/* Large tables benefit most from
									 * batching */
	}

	/*
	 * Use ExtendBufferedRelBy to extend the relation by multiple pages at once.
	 * This is the modern API that properly handles buffer locking and extension.
	 * We extend by (1 + num_extra_pages) pages total: the first page is what
	 * we'll return to the caller, and the extra pages are added to the FPM.
	 */
	{
		Buffer		buffers[513];	/* 1 main + up to 512 extra */
		uint32		extend_by = 1 + num_extra_pages;
		uint32		extended_by = extend_by;
		uint32		flags = EB_LOCK_FIRST;

		/* Skip extension lock for local relations */
		if (RELATION_IS_LOCAL(rel))
			flags |= EB_SKIP_EXTENSION_LOCK;

		/* Extend the relation */
		ExtendBufferedRelBy(BMR_REL(rel),
							MAIN_FORKNUM,
							NULL,		/* strategy */
							flags,
							extend_by,
							buffers,
							&extended_by);

		/* First buffer is returned locked */
		buf = buffers[0];

		/*
		 * Add the extra pages to the free page map.
		 * This amortizes the cost of extension locks and improves spatial
		 * locality.
		 */
		if (extended_by > 1)
		{
			/* Get the metapage to update the FPM */
			if (metabuf == InvalidBuffer)
			{
				local_metabuf = ReadBuffer(rel, OV_META_BLK);
				LockBuffer(local_metabuf, BUFFER_LOCK_EXCLUSIVE);
				release_metabuf = true;
			}
			else
			{
				/* Caller already has metabuf locked */
				local_metabuf = metabuf;
				release_metabuf = false;
			}
			metapage = BufferGetPage(local_metabuf);
			metaopaque = (OVMetaPageOpaque *) PageGetSpecialPointer(metapage);

			for (i = 1; i < extended_by; i++)
			{
				Buffer		extrabuf = buffers[i];
				Page		page;
				BlockNumber extrablk;

				/*
				 * The extra buffers are pinned but not locked by
				 * ExtendBufferedRelBy. We need to lock them to initialize.
				 */
				extrablk = BufferGetBlockNumber(extrabuf);
				LockBuffer(extrabuf, BUFFER_LOCK_EXCLUSIVE);

				/* Mark it as free and add to the FPM linked list */
				page = BufferGetPage(extrabuf);
				ovpage_mark_page_deleted(page, metaopaque->ov_fpm_head);
				MarkBufferDirty(extrabuf);

				/* Update FPM head to point to this new free page */
				metaopaque->ov_fpm_head = extrablk;

				UnlockReleaseBuffer(extrabuf);
			}

			MarkBufferDirty(local_metabuf);
			if (release_metabuf)
				UnlockReleaseBuffer(local_metabuf);
		}
	}

	return buf;
}

void
ovpage_mark_page_deleted(Page page, BlockNumber next_free_blk)
{
	ZSFreePageOpaque *opaque;

	PageInit(page, BLCKSZ, sizeof(ZSFreePageOpaque));
	opaque = (ZSFreePageOpaque *) PageGetSpecialPointer(page);
	opaque->ov_page_id = OV_FREE_PAGE_ID;
	opaque->ov_next = next_free_blk;

}

/*
 * Explictly mark a page as deleted and recyclable, and add it to the FPM.
 *
 * The caller must hold an exclusive-lock on the page.
 */
void
ovpage_delete_page(Relation rel, Buffer buf)
{
	BlockNumber blk = BufferGetBlockNumber(buf);
	Buffer		metabuf;
	Page		metapage;
	OVMetaPageOpaque *metaopaque;
	Page		page;

	metabuf = ReadBuffer(rel, OV_META_BLK);
	LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);
	metapage = BufferGetPage(metabuf);
	metaopaque = (OVMetaPageOpaque *) PageGetSpecialPointer(metapage);

	page = BufferGetPage(buf);
	ovpage_mark_page_deleted(page, metaopaque->ov_fpm_head);
	metaopaque->ov_fpm_head = blk;

	MarkBufferDirty(metabuf);
	MarkBufferDirty(buf);

	/* FIXME: WAL-logging */

	UnlockReleaseBuffer(metabuf);
}
