/**
 * @file noxu_wal.h
 * @brief WAL (Write-Ahead Log) record definitions for Noxu.
 *
 * Defines the WAL record type codes and payload structures for all
 * Noxu WAL operations: metapage initialization, UNDO log management,
 * B-tree leaf modifications, page splits/rewrites, overflow pages, and
 * Free Page Map updates.
 *
 * @par WAL Record Types
 * | Code | Constant                           | Description                    |
 * |------|------------------------------------|--------------------------------|
 * | 0x00 | WAL_NOXU_INIT_METAPAGE             | Initialize metapage            |
 * | 0x10 | WAL_NOXU_UNDO_NEWPAGE              | Extend UNDO log with new page  |
 * | 0x20 | WAL_NOXU_UNDO_DISCARD              | Discard old UNDO records       |
 * | 0x30 | WAL_NOXU_BTREE_NEW_ROOT            | Create new B-tree root         |
 * | 0x40 | WAL_NOXU_BTREE_ADD_LEAF_ITEMS      | Add items to B-tree leaf       |
 * | 0x50 | WAL_NOXU_BTREE_REPLACE_LEAF_ITEM   | Replace item on B-tree leaf    |
 * | 0x60 | WAL_NOXU_BTREE_REWRITE_PAGES       | Page split/rewrite             |
 * | 0x70 | WAL_NOXU_OVERFLOW_NEWPAGE          | Add overflow page              |
 * | 0x80 | WAL_NOXU_FPM_DELETE                | Add page to Free Page Map      |
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/include/access/noxu_wal.h
 */
#ifndef NOXU_WAL_H
#define NOXU_WAL_H

#include "c.h"
#include "access/attnum.h"
#include "access/xlogreader.h"
#include "access/noxu_tid.h"
#include "access/relundo.h"
#include "lib/stringinfo.h"
#include "storage/off.h"

#define WAL_NOXU_INIT_METAPAGE			0x00
#define WAL_NOXU_UNDO_NEWPAGE			0x10
#define WAL_NOXU_UNDO_DISCARD			0x20
#define WAL_NOXU_BTREE_NEW_ROOT			0x30
#define WAL_NOXU_BTREE_ADD_LEAF_ITEMS	        0x40
#define WAL_NOXU_BTREE_REPLACE_LEAF_ITEM	0x50
#define WAL_NOXU_BTREE_REWRITE_PAGES	        0x60
#define WAL_NOXU_OVERFLOW_NEWPAGE	        0x70
#define WAL_NOXU_FPM_DELETE			0x80

/* in noxu_wal.c */
extern void noxu_redo(XLogReaderState *record);
extern void noxu_mask(char *pagedata, BlockNumber blkno);

/* in noxudesc.c */
extern void noxu_desc(StringInfo buf, XLogReaderState *record);
extern const char *noxu_identify(uint8 info);

/*
 * WAL record for initializing noxu metapage (WAL_NOXU_INIT_METAPAGE)
 *
 * These records always use a full-page image, so this data is really just
 * for debugging purposes.
 */
typedef struct wal_noxu_init_metapage
{
	int32		natts;			/* number of attributes. */
}			wal_noxu_init_metapage;

#define SizeOfNXWalInitMetapage (offsetof(wal_noxu_init_metapage, natts) + sizeof(int32))

/*
 * WAL record for extending the UNDO log with one page.
 */
typedef struct wal_noxu_undo_newpage
{
	uint64		first_counter;
}			wal_noxu_undo_newpage;

#define SizeOfNXWalUndoNewPage (offsetof(wal_noxu_undo_newpage, first_counter) + sizeof(uint64))

/*
 * WAL record for updating the oldest undo pointer on the metapage, after
 * discarding an old portion the UNDO log.
 *
 * blkref #0 is the metapage.
 *
 * If an old UNDO page was discarded away, advancing nx_undo_head, that page
 * is stored as blkref #1. The new block number to store in nx_undo_head is
 * stored as the data of blkref #0.
 */
typedef struct wal_noxu_undo_discard
{
	RelUndoRecPtr oldest_undorecptr;

	/*
	 * Next oldest remaining block in the UNDO chain. This is not the same as
	 * RelUndoGetBlockNum(oldest_undorecptr), if we are discarding multiple UNDO blocks. We
	 * will update oldest_undorecptr in the first iteration already, so that
	 * visibility checks can use the latest value immediately. But we can't
	 * hold a potentially unlimited number of pages locked while we mark them
	 * as deleted, so they are deleted one by one, and each deletion is
	 * WAL-logged separately.
	 */
	BlockNumber oldest_undopage;
}			wal_noxu_undo_discard;

#define SizeOfNXWalUndoDiscard (offsetof(wal_noxu_undo_discard, oldest_undopage) + sizeof(BlockNumber))

/*
 * WAL record for creating a new, empty, root page for an attribute.
 */
typedef struct wal_noxu_btree_new_root
{
	AttrNumber	attno;			/* 0 means TID tree */
}			wal_noxu_btree_new_root;

#define SizeOfNXWalBtreeNewRoot	(offsetof(wal_noxu_btree_new_root, attno) + sizeof(AttrNumber))

/*
 * WAL record for replacing/adding items to the TID tree, or to an attribute tree.
 */
typedef struct wal_noxu_btree_leaf_items
{
	AttrNumber	attno;			/* 0 means TID tree */
	int16		nitems;
	OffsetNumber off;

	/* the items follow */
}			wal_noxu_btree_leaf_items;

#define SizeOfNXWalBtreeLeafItems (offsetof(wal_noxu_btree_leaf_items, off) + sizeof(OffsetNumber))

/*
 * WAL record for page splits, and other more complicated operations where
 * we just rewrite whole pages.
 *
 * block #0 is UNDO buffer, if any.
 * Blocks 1..numpages are the b-tree pages.
 * If recycle_bitmap is non-zero, the block after the last b-tree page is
 * the metapage (for updating nx_fpm_head).  Each bit i in recycle_bitmap
 * indicates that b-tree page at block_id (i + 1) should be recycled into
 * the Free Page Map.
 */
typedef struct wal_noxu_btree_rewrite_pages
{
	AttrNumber	attno;			/* 0 means TID tree */
	int			numpages;
	uint32		recycle_bitmap; /* bits for pages to recycle (max 32 pages) */
	BlockNumber old_fpm_head;	/* FPM head before recycling */
}			wal_noxu_btree_rewrite_pages;

#define SizeOfNXWalBtreeRewritePages (offsetof(wal_noxu_btree_rewrite_pages, old_fpm_head) + sizeof(BlockNumber))

/*
 * WAL record for noxu overflow. When a large datum spans multiple pages,
 * we write one of these for every page. The chain will appear valid between
 * every operation, except that the total size won't match the total size of
 * all the pages until the last page is written.
 *
 * blkref 0: the new page being added
 * blkref 1: the previous page in the chain
 */
typedef struct wal_noxu_overflow_newpage
{
	nxtid		tid;
	AttrNumber	attno;
	int32		total_size;
	int32		offset;
}			wal_noxu_overflow_newpage;

#define SizeOfNXWalOverflowNewPage (offsetof(wal_noxu_overflow_newpage, offset) + sizeof(int32))

/*
 * WAL record for adding a page to the Free Page Map.
 * (WAL_NOXU_FPM_DELETE)
 *
 * This is used when a page is marked as deleted and added to the FPM
 * linked list. The metapage's nx_fpm_head is updated to point to the
 * newly freed page.
 *
 * blkref #0: the metapage
 * blkref #1: the page being added to the FPM (WILL_INIT)
 *
 * old_fpm_head is the previous FPM head value that becomes the
 * nx_next pointer on the freed page.
 */
typedef struct wal_noxu_fpm_delete
{
	BlockNumber old_fpm_head;
}			wal_noxu_fpm_delete;

#define SizeOfNXWalFpmDelete (offsetof(wal_noxu_fpm_delete, old_fpm_head) + sizeof(BlockNumber))

extern void nxbt_leaf_items_redo(XLogReaderState *record, bool replace);
extern void nxmeta_new_btree_root_redo(XLogReaderState *record);
extern void nxbt_rewrite_pages_redo(XLogReaderState *record);
extern void nxoverflow_newpage_redo(XLogReaderState *record);
extern void nxfpm_delete_redo(XLogReaderState *record);

#endif							/* NOXU_WAL_H */
