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
}			xl_relundo_init;

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
}			xl_relundo_insert;

#define SizeOfRelundoInsert	(offsetof(xl_relundo_insert, new_pd_lower) + sizeof(uint16))

/*
 * xl_relundo_discard - WAL record for UNDO page discard
 *
 * Logged when RelUndoDiscard() reclaims space by removing old pages
 * from the tail of the page chain.
 *
 * Backup block 0: the metapage (updated tail/free pointers)
 */
typedef struct xl_relundo_discard
{
	BlockNumber old_tail_blkno; /* Previous tail block number */
	BlockNumber new_tail_blkno; /* New tail after discard */
	uint16		oldest_counter; /* Counter cutoff used for discard */
	uint32		npages_freed;	/* Number of pages freed */
}			xl_relundo_discard;

#define SizeOfRelundoDiscard	(offsetof(xl_relundo_discard, npages_freed) + sizeof(uint32))

/* Resource manager functions */
extern void relundo_redo(XLogReaderState *record);
extern void relundo_desc(StringInfo buf, XLogReaderState *record);
extern const char *relundo_identify(uint8 info);

/*
 * XLOG_RELUNDO_APPLY - Compensation Log Record for UNDO application
 *
 * Records that we've applied an UNDO operation during transaction rollback.
 * Prevents double-application if we crash during rollback.
 */
typedef struct xl_relundo_apply
{
	RelUndoRecPtr urec_ptr;		/* UNDO record that was applied */
	RelFileLocator target_reloc; /* Target relation */
} xl_relundo_apply;

#define SizeOfRelUndoApply	(offsetof(xl_relundo_apply, target_reloc) + sizeof(RelFileLocator))

#endif							/* RELUNDO_XLOG_H */
