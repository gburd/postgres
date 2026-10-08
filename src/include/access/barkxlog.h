/*-------------------------------------------------------------------------
 *
 * barkxlog.h
 *	  header file for BARK WAL records and redo routines
 *
 * Every change BARK makes to a page after CREATE INDEX is in exactly one of
 * these records.
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
#define XLOG_BARK_SPLIT			0x80	/* split a leaf or internal page */
#define XLOG_BARK_OVERFLOW		0x90	/* write overflow pages of an
										 * oversized entry */
#define XLOG_BARK_NEWROOT		0xA0	/* add a level above a split root */
#define XLOG_BARK_CREATE_ROOT	0xB0	/* give an empty index its root leaf */
#define XLOG_BARK_INSERT_SWAP	0xC0	/* divide a LIST or POSTING entry
										 * around a new heap TID */
#define XLOG_BARK_DELETE		0xD0	/* delete entries on a leaf whose heap
										 * tuples are dead (bottom-up
										 * deletion) */
#define XLOG_BARK_MERGE			0xE0	/* merge a leaf's equal-key entries
										 * before a split */

/*
 * The meta page's root and level, set by NEWROOT and CREATE_ROOT, as
 * nbtree's xl_btree_metadata.
 */
typedef struct xl_bark_metadata
{
	BlockNumber root;
	uint32		level;
} xl_bark_metadata;

/*
 * A page split (bark_split), as nbtree's xl_btree_split.  Both halves get
 * the cycle ID that the split stamped on them (zero for an internal page).
 *
 * Backup Blk 0: left page (the original block, rebuilt)
 * Backup Blk 1: new right page
 * Backup Blk 2: original right sibling (only if the original had one)
 * Backup Blk 3: child whose split the new downlink finishes (internal only)
 *
 * The right page is logged whole: its block data is the page's tuple area,
 * pd_upper to pd_special, of the page the primary built by adding the
 * entries in offset order to an empty page, the format bark_restore_page
 * reads (as nbtree's _bt_restore_page).  It holds the original high key
 * unless the original page was rightmost, then the entries from the split
 * point on.
 *
 * The left page is usually rebuilt from the original page, which redo has
 * (it is the same block, before the split), as btree_xlog_split does: its
 * entries are the original page's entries before firstrightoff, stored as
 * they are, with the new entry added before newitemoff if it went left
 * (XLH_BARK_SPLIT_NEWLEFT) and the entry at replaceoff replaced if that
 * entry stayed left (XLH_BARK_SPLIT_REPLACE), after the new high key and,
 * on a BARK_PREFIX leaf, the original page's PREFIX item; both sides build
 * it with bark_split_build_left.  The block data is then the new high key,
 * the new entry if it went left and the replacement if it stayed left, in
 * that order, each coded for the page and padded to MAXALIGN.  That works
 * only while the left half keeps the original page's prefix, or neither has
 * one: a leaf split that gives the left half a different prefix (or takes
 * its prefix away) re-codes every entry, so the left page is then logged
 * whole as the right page is (XLH_BARK_SPLIT_LWHOLE), and the offsets are
 * unused.
 */
typedef struct xl_bark_split
{
	uint32		level;			/* tree level of the split page */
	uint16		flags;			/* XLH_BARK_SPLIT_* */
	uint16		cycleid;		/* BTCycleId stamped on both halves */
	BlockNumber leftprev;		/* original page's left sibling */
	BlockNumber rightnext;		/* original page's right sibling */
	OffsetNumber firstrightoff; /* first original entry that moved right */
	OffsetNumber newitemoff;	/* new entry's offset, if it went left */
	OffsetNumber replaceoff;	/* replaced entry's offset, if it stayed
								 * left */
} xl_bark_split;

#define SizeOfBarkSplit	(offsetof(xl_bark_split, replaceoff) + sizeof(OffsetNumber))

#define XLH_BARK_SPLIT_LEAF		0x0001	/* the split page is a leaf */
#define XLH_BARK_SPLIT_LPREFIX	0x0002	/* left half has BARK_PREFIX */
#define XLH_BARK_SPLIT_RPREFIX	0x0004	/* right half has BARK_PREFIX */
#define XLH_BARK_SPLIT_NEWLEFT	0x0008	/* new entry went to the left half */
#define XLH_BARK_SPLIT_REPLACE	0x0010	/* replaced entry stayed left */
#define XLH_BARK_SPLIT_LWHOLE	0x0020	/* left half is logged whole */

/*
 * A new root above the split of the old one (bark_new_root), as nbtree's
 * xl_btree_newroot.
 *
 * Backup Blk 0: new root
 * Backup Blk 1: left child, whose split the new root finishes
 * Backup Blk 2: meta page
 *
 * In payload of blk 0: the root's two downlinks, as the tuple area of a split
 * In payload of blk 2: xl_bark_metadata
 */
typedef struct xl_bark_newroot
{
	BlockNumber rootblk;		/* location of new root */
	uint32		level;			/* its tree level */
} xl_bark_newroot;

#define SizeOfBarkNewroot	(offsetof(xl_bark_newroot, level) + sizeof(uint32))

/*
 * Overflow pages of an OVERSIZED entry's chain (bark_write_overflow_chain),
 * up to BARK_OVERFLOW_PER_RECORD per record.  Each page is rebuilt from its
 * block data: an xl_bark_overflow_page, then the page's slice of the tuple.
 * No main data.
 *
 * Backup Blk 0..n-1: the overflow pages, in chain order
 */
#define BARK_OVERFLOW_PER_RECORD	XLR_NORMAL_MAX_BLOCK_ID

typedef struct xl_bark_overflow_page
{
	BlockNumber next;			/* next page of the chain, or BARK_P_NONE */
} xl_bark_overflow_page;

/*
 * The first root of an empty index (bark_create_root_leaf): an empty leaf
 * that is also the root.  No main data.
 *
 * Backup Blk 0: the new leaf
 * Backup Blk 1: meta page
 *
 * In payload of blk 1: xl_bark_metadata
 */

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
 * Bottom-up deletion's changes to one leaf (bark_delitems_delete), as
 * nbtree's xl_btree_delete: the same payload as xl_bark_vacuum, entries
 * deleted whole and LIST or POSTING entries rewritten with the members that
 * survive, plus the conflict horizon of the heap tuples whose entries go.
 * Unlike VACUUM, the record leaves the page's vacuum cycle ID alone, and redo
 * takes an exclusive lock rather than a cleanup lock (see "Bottom-up
 * deletion" in the README).
 *
 * Backup Blk 0: leaf page
 *
 * In payload of blk 0: as xl_bark_vacuum
 */
typedef struct xl_bark_delete
{
	TransactionId snapshotConflictHorizon;
	uint16		ndeleted;
	uint16		nupdated;
	bool		isCatalogRel;	/* to handle recovery conflict during logical
								 * decoding on standby */
} xl_bark_delete;

#define SizeOfBarkDelete	(offsetof(xl_bark_delete, isCatalogRel) + sizeof(bool))

/*
 * The merge of adjacent equal-key entries on one leaf (bark_merge_page), as
 * nbtree's xl_btree_dedup is a record of its own: the entries absorbed into
 * the one before them are deleted, and that one is rewritten as the merged
 * entry.  The payload is xl_bark_vacuum's, but the changes are applied in the
 * other order, deletions first: a merged entry is larger than the entry it
 * replaces, and on a full page it fits only once the absorbed entries' space
 * is free.  The rewritten entries' offsets are therefore numbered as they are
 * after the deletions.  No heap TID leaves the page, so there is no conflict
 * horizon, and redo takes an exclusive lock, not a cleanup lock.
 *
 * Backup Blk 0: leaf page
 *
 * In payload of blk 0: as xl_bark_vacuum
 */
typedef struct xl_bark_merge
{
	uint16		ndeleted;
	uint16		nupdated;
} xl_bark_merge;

#define SizeOfBarkMerge	(offsetof(xl_bark_merge, nupdated) + sizeof(uint16))

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
 * A heap TID inserted inside the TID range of a LIST or POSTING entry that
 * cannot take it (bark_swap_tid_entry): the entry at offnum is divided around
 * the TID into two entries of its key, which replace it at offnum and
 * offnum + 1, as nbtree's posting-list split (XLOG_BTREE_INSERT_POST with a
 * postingoff) rewrites the posting list and adds the new item.  Only the TID
 * is logged: redo calls bark_entry_swap_tid on the entry, as the primary did.
 *
 * Backup Blk 0: leaf page
 */
typedef struct xl_bark_insert_swap
{
	OffsetNumber offnum;		/* entry being divided */
	ItemPointerData tid;		/* the TID */
} xl_bark_insert_swap;

#define SizeOfBarkInsertSwap	(offsetof(xl_bark_insert_swap, tid) + sizeof(ItemPointerData))

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
