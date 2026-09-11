/*-------------------------------------------------------------------------
 *
 * relundo_xlog.h
 *	  Per-relation UNDO WAL record definitions
 *
 * This file contains the WAL record format definitions for per-relation
 * UNDO operations.  These records are logged by the RM_RELUNDO_ID resource
 * manager.
 *
 * Record types:
 *   XLOG_RELUNDO_INIT    - Metapage initialization
 *   XLOG_RELUNDO_INSERT  - UNDO record insertion into a data page
 *   XLOG_RELUNDO_DISCARD - Discard old UNDO pages during VACUUM
 *
 * Per-relation UNDO stores operation metadata for MVCC visibility in
 * each relation's UNDO fork.  This is distinct from the cluster-wide
 * UNDO system (RM_UNDO_ID) which handles transaction rollback.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/relundo_xlog.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RELUNDO_XLOG_H
#define RELUNDO_XLOG_H

#include "postgres.h"

#include "access/xlogreader.h"
#include "lib/stringinfo.h"
#include "storage/block.h"
#include "storage/relfilelocator.h"

/* Forward declaration - full definition in relundo.h */
typedef uint64 RelUndoRecPtr;

/*
 * WAL record types for per-relation UNDO operations
 *
 * The high 4 bits of the info byte encode the operation type,
 * following PostgreSQL convention.
 */
#define XLOG_RELUNDO_INIT			0x00	/* Metapage initialization */
#define XLOG_RELUNDO_INSERT			0x10	/* UNDO record insertion */
#define XLOG_RELUNDO_DISCARD		0x20	/* Discard old UNDO pages */
#define XLOG_RELUNDO_TRUNCATE		0x30	/* Physically truncate the fork */
#define XLOG_RELUNDO_APPLY			0x40	/* Apply UNDO for rollback (CLR) */

/*
 * Flag: set when the data page being inserted into is newly initialized
 * (first tuple on the page).  When set, redo will re-initialize the
 * page from scratch before applying the insert.
 */
#define XLOG_RELUNDO_INIT_PAGE		0x80

/*
 * xl_relundo_init - WAL record for metapage initialization
 *
 * Logged when RelUndoInitRelation() creates the UNDO fork and writes
 * the initial metapage (block 0).
 *
 * Backup block 0: the metapage
 */
typedef struct xl_relundo_init
{
	uint32		magic;			/* RELUNDO_METAPAGE_MAGIC */
	uint16		version;		/* Format version */
	uint16		counter;		/* Initial generation counter */
} xl_relundo_init;

#define SizeOfRelundoInit	(offsetof(xl_relundo_init, counter) + sizeof(uint16))

/*
 * xl_relundo_insert - WAL record for UNDO record insertion
 *
 * Logged when RelUndoFinish() writes an UNDO record to a data page.
 *
 * Backup block 0: the data page receiving the UNDO record
 * Backup block 1: the metapage (if head_blkno was updated)
 *
 * The actual UNDO record data is stored as block data associated with
 * backup block 0 (via XLogRegisterBufData).
 */
typedef struct xl_relundo_insert
{
	uint16		urec_type;		/* RelUndoRecordType of the UNDO record */
	uint16		urec_len;		/* Total length of UNDO record */
	uint16		page_offset;	/* Byte offset within page where record starts */
	uint16		new_pd_lower;	/* Updated pd_lower after insertion */
	TransactionId max_xid;		/* Updated page max_xid watermark after insert */
} xl_relundo_insert;

#define SizeOfRelundoInsert	(offsetof(xl_relundo_insert, max_xid) + sizeof(TransactionId))

/*
 * xl_relundo_discard - WAL record for UNDO page discard
 *
 * Logged when RelUndoDiscard() reclaims a contiguous run of discardable
 * pages from the tail of the data chain by splicing the whole run directly
 * onto the metapage's free list.  Only the run boundaries change, so the
 * record covers a fixed set of buffers regardless of run length:
 *
 * Backup block 0: the metapage (tail + free-list head)
 * Backup block 1: the run's old-tail page (prev_blkno -> old free head)
 * Backup block 2: the new live tail page (prev_blkno -> Invalid)
 */
typedef struct xl_relundo_discard
{
	BlockNumber old_tail_blkno; /* Old chain tail (run's tail), block 1 */
	BlockNumber new_tail_blkno; /* New chain tail after discard */
	BlockNumber free_head_blkno;	/* New free-list head (run's head) */
	BlockNumber old_free_head;	/* Prior free-list head, written to block 1 */
	TransactionId discard_xid;	/* oldest_xmin cutoff used for discard */
	uint32		npages_freed;	/* Number of pages spliced onto free list */
	uint16		slot;			/* Head slot whose chain was discarded */
} xl_relundo_discard;

#define SizeOfRelundoDiscard	(offsetof(xl_relundo_discard, slot) + sizeof(uint16))

/*
 * xl_relundo_truncate - WAL record for physical fork truncation
 *
 * Logged when RelUndoDiscard() empties the entire data chain.  At that
 * point the free list holds every allocated data block, i.e. the
 * contiguous physical suffix [1 .. system_alloc_watermark], so the fork
 * can be physically truncated back to just the metapage (block 0).  Redo
 * resets the metapage free-list/watermark fields and truncates the fork.
 *
 * Backup block 0: the metapage (free_blkno + watermark reset)
 */
typedef struct xl_relundo_truncate
{
	BlockNumber new_nblocks;	/* New fork length in blocks (always 1) */
} xl_relundo_truncate;

#define SizeOfRelundoTruncate	(offsetof(xl_relundo_truncate, new_nblocks) + sizeof(BlockNumber))

/* Resource manager functions */
extern void relundo_redo(XLogReaderState *record);
extern void relundo_desc(StringInfo buf, XLogReaderState *record);
extern const char *relundo_identify(uint8 info);

/* Parallel redo support */
extern void relundo_startup(void);
extern void relundo_cleanup(void);
extern void relundo_mask(char *pagedata, BlockNumber blkno);

/*
 * XLOG_RELUNDO_APPLY - Compensation Log Record for UNDO application
 *
 * Records that we've applied an UNDO operation during transaction rollback.
 * Prevents double-application if we crash during rollback.
 */
typedef struct xl_relundo_apply
{
	RelUndoRecPtr urec_ptr;		/* UNDO record that was applied */
	RelFileLocator target_reloc;	/* Target relation */
} xl_relundo_apply;

#define SizeOfRelUndoApply	(offsetof(xl_relundo_apply, target_reloc) + sizeof(RelFileLocator))

#endif							/* RELUNDO_XLOG_H */
