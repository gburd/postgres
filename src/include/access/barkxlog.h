/*-------------------------------------------------------------------------
 *
 * barkxlog.h
 *	  header file for BARK WAL records and redo routines
 *
 * BARK changes that have no record of their own here (splits, new roots,
 * overflow chains) are still WAL-logged with generic WAL (generic_xlog.c).
 * Each change to a page is in exactly one record of either kind, so replay
 * applies them in LSN order per page.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/barkxlog.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef BARKXLOG_H
#define BARKXLOG_H

#include "access/transam.h"
#include "access/xlogreader.h"
#include "lib/stringinfo.h"
#include "storage/itemptr.h"
#include "storage/off.h"

/*
 * XLOG records for BARK operations
 *
 * XLOG allows to store some information in high 4 bits of log record xl_info
 * field.
 */
#define XLOG_BARK_VACUUM		0x00	/* delete entries on a leaf during
										 * VACUUM */
#define XLOG_BARK_UNLINK_PAGE	0x10	/* delete an empty leaf from the tree */
#define XLOG_BARK_REUSE_PAGE	0x20	/* deleted page is about to be reused
										 * from the FSM */
#define XLOG_BARK_MARK_DELETED	0x30	/* free one overflow page */
#define XLOG_BARK_INSERT_LEAF	0x40	/* add an entry to a leaf */
#define XLOG_BARK_INSERT_UPPER	0x50	/* add a downlink to an internal page,
										 * finishing a child's split */
#define XLOG_BARK_OVERWRITE		0x60	/* replace an entry on a leaf */
#define XLOG_BARK_ADD_TID		0x70	/* add one heap TID to a LIST or
										 * POSTING entry */

/*
 * VACUUM's changes to one leaf page, as nbtree's xl_btree_vacuum: entries
 * deleted whole, and LIST or POSTING entries rewritten with the members that
 * survive.  Unlike nbtree, which logs which TIDs to remove from each posting
 * list and re-forms the tuple in redo, the rewritten entries are logged
 * whole: re-forming a POSTING entry needs the index's tuple descriptor, which
 * redo does not have.  Rewrites never grow an entry.
 *
 * Redo also clears the page's vacuum cycle ID.  A record with nothing deleted
 * or rewritten is written only to clear a page's cycle ID.
 *
 * Backup Blk 0: leaf page
 *
 * In payload of blk 0:
 * - DELETED TARGET OFFSET NUMBERS (ndeleted)
 * - UPDATED TARGET OFFSET NUMBERS (nupdated)
 * - UPDATED ENTRIES (nupdated), each padded to MAXALIGN(IndexTupleSize())
 */
typedef struct xl_bark_vacuum
{
	uint16		ndeleted;
	uint16		nupdated;
} xl_bark_vacuum;

#define SizeOfBarkVacuum	(offsetof(xl_bark_vacuum, nupdated) + sizeof(uint16))

/*
 * Deletion of an empty leaf (bark_delete_empty_leaf): the left sibling's
 * right link and the right sibling's left link skip the target, the target's
 * downlink in the parent (at poffset) is pointed at the right sibling and the
 * right sibling's own downlink, the next item, is removed, and the target is
 * reinitialized as a deleted leaf that keeps its sibling links.  This is
 * nbtree's mark-half-dead and unlink steps in one record.
 *
 * Backup Blk 0: target leaf (reinitialized)
 * Backup Blk 1: left sibling
 * Backup Blk 2: right sibling
 * Backup Blk 3: parent
 */
typedef struct xl_bark_unlink_page
{
	BlockNumber leftsib;		/* target's left sibling */
	BlockNumber rightsib;		/* target's right sibling */
	FullTransactionId safexid;	/* target's BarkPageSetDeleted() XID */
	OffsetNumber poffset;		/* target's downlink in the parent */
} xl_bark_unlink_page;

#define SizeOfBarkUnlinkPage	(offsetof(xl_bark_unlink_page, poffset) + sizeof(OffsetNumber))

/*
 * A deleted page is about to be reused (bark_get_free_page), as nbtree's
 * xl_btree_reuse_page.  No buffer is registered: the record changes no
 * page, and exists only so that hot standby can cancel queries whose
 * snapshots may still hold a link to the page.
 */
typedef struct xl_bark_reuse_page
{
	RelFileLocator locator;
	BlockNumber block;
	FullTransactionId snapshotConflictHorizon;
	bool		isCatalogRel;	/* to handle recovery conflict during logical
								 * decoding on standby */
} xl_bark_reuse_page;

#define SizeOfBarkReusePage	(offsetof(xl_bark_reuse_page, isCatalogRel) + sizeof(bool))

/*
 * An overflow page freed by VACUUM (bark_free_oversized) is reinitialized as
 * a deleted page that keeps its link to the next chunk of its chain.  It is
 * not in the tree, so there is nothing to unlink.
 *
 * Backup Blk 0: overflow page (reinitialized)
 */
typedef struct xl_bark_mark_deleted
{
	BlockNumber next;			/* the page's link to the next chunk */
	FullTransactionId safexid;	/* the page's BarkPageSetDeleted() XID */
} xl_bark_mark_deleted;

#define SizeOfBarkMarkDeleted	(offsetof(xl_bark_mark_deleted, safexid) + sizeof(FullTransactionId))

/*
 * Insertion of one entry, as nbtree's xl_btree_insert: a leaf entry
 * (INSERT_LEAF), or a downlink to the right half of a split child
 * (INSERT_UPPER), which also clears BARK_INCOMPLETE_SPLIT on the child, the
 * left half.  The entry is MAXALIGN-sized, so the page that redo produces
 * does not depend on the free space it is placed in.
 *
 * Backup Blk 0: page the entry is added to
 * Backup Blk 1: child whose split the downlink finishes (INSERT_UPPER only)
 *
 * In payload of blk 0: the entry
 */
typedef struct xl_bark_insert
{
	OffsetNumber offnum;		/* where the entry goes */
} xl_bark_insert;

#define SizeOfBarkInsert	(offsetof(xl_bark_insert, offnum) + sizeof(OffsetNumber))

/*
 * Replacement of one leaf entry by a new one, usually larger: a LIST or
 * POSTING entry that gains a locator, or a SINGLE or LIST that becomes a
 * LIST or POSTING (bark_coalesce_list).  The primary and redo both use
 * PageIndexTupleOverwrite, which keeps the entry's offset and moves only the
 * entries stored below it.
 *
 * Backup Blk 0: leaf page
 *
 * In payload of blk 0: the new entry
 */
typedef struct xl_bark_overwrite
{
	OffsetNumber offnum;		/* entry being replaced */
} xl_bark_overwrite;

#define SizeOfBarkOverwrite	(offsetof(xl_bark_overwrite, offnum) + sizeof(OffsetNumber))

/*
 * One heap TID added to a LIST or POSTING entry on a leaf, by either of
 * bark_coalesce_list's fast paths.  Only the TID is logged: redo calls
 * bark_entry_add_tid on the entry at offnum, as the primary did, and
 * overwrites the entry with the result, as nbtree's XLOG_BTREE_INSERT_POST
 * logs the new item and has redo re-form the posting list.  The result
 * depends only on the entry and the TID, so both sides build the same bytes.
 *
 * Backup Blk 0: leaf page
 */
typedef struct xl_bark_add_tid
{
	OffsetNumber offnum;		/* entry gaining the TID */
	ItemPointerData tid;		/* the TID */
} xl_bark_add_tid;

#define SizeOfBarkAddTid	(offsetof(xl_bark_add_tid, tid) + sizeof(ItemPointerData))

/*
 * prototypes for functions in barkxlog.c
 */
extern void bark_redo(XLogReaderState *record);
extern void bark_mask(char *pagedata, BlockNumber blkno);

/*
 * prototypes for functions in barkdesc.c
 */
extern void bark_desc(StringInfo buf, XLogReaderState *record);
extern const char *bark_identify(uint8 info);

#endif							/* BARKXLOG_H */
