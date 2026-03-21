/*-------------------------------------------------------------------------
 *
 * undo_xlog.h
 *	  UNDO resource manager WAL record definitions
 *
 * This file contains the WAL record format definitions for UNDO log
 * operations. These records are logged by the RM_UNDO_ID resource manager.
 *
 * Record types:
 *   XLOG_UNDO_ALLOCATE       - Log UNDO space allocation
 *   XLOG_UNDO_DISCARD        - Log UNDO record discard
 *   XLOG_UNDO_EXTEND         - Log UNDO log file extension
 *   XLOG_UNDO_APPLY_RECORD   - CLR: Log physical UNDO application to a page
 *
 * The XLOG_UNDO_APPLY_RECORD type is a Compensation Log Record (CLR).
 * CLRs record the fact that an UNDO operation was applied to a page
 * during transaction rollback.  This ensures crash safety: if we crash
 * during rollback, the already-applied UNDO operations are preserved
 * via WAL replay of the CLR's full page image.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/undo_xlog.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef UNDO_XLOG_H
#define UNDO_XLOG_H

#include "access/transam.h"
#include "access/xlogdefs.h"
#include "access/xlogreader.h"
#include "lib/stringinfo.h"
#include "storage/block.h"
#include "storage/off.h"
#include "storage/relfilelocator.h"

/*
 * UndoRecPtr type definition.  We use undodefs.h which is lightweight
 * and can be included in both frontend and backend code.  If undodefs.h
 * has already been included (via undolog.h or directly), this is a no-op.
 */
#include "access/undodefs.h"

/*
 * WAL record types for UNDO operations
 *
 * These are the info codes for UNDO WAL records. The low 4 bits are used
 * for operation type, leaving the upper 4 bits for flags.
 */
#define XLOG_UNDO_ALLOCATE			0x00	/* Allocate UNDO log space */
#define XLOG_UNDO_DISCARD			0x10	/* Discard old UNDO records */
#define XLOG_UNDO_EXTEND			0x20	/* Extend UNDO log file */
#define XLOG_UNDO_APPLY_RECORD		0x30	/* CLR: UNDO applied to page */

/*
 * xl_undo_allocate - WAL record for UNDO space allocation
 *
 * Logged when a backend allocates space in an UNDO log for writing
 * UNDO records. This ensures crash recovery can reconstruct the
 * insert pointer state.
 */
typedef struct xl_undo_allocate
{
	UndoRecPtr	start_ptr;		/* Starting position of allocation */
	uint32		length;			/* Length of allocation in bytes */
	TransactionId xid;			/* Transaction that allocated this space */
	uint32		log_number;		/* Log number (extracted from start_ptr) */
}			xl_undo_allocate;

#define SizeOfUndoAllocate	(offsetof(xl_undo_allocate, log_number) + sizeof(uint32))

/*
 * xl_undo_discard - WAL record for UNDO discard operation
 *
 * Logged when the UNDO worker discards old UNDO records that are no
 * longer needed by any active transaction. This allows space to be
 * reclaimed.
 */
typedef struct xl_undo_discard
{
	UndoRecPtr	discard_ptr;	/* New discard pointer (oldest still needed) */
	uint32		log_number;		/* Which log is being discarded */
	TransactionId oldest_xid;	/* Oldest XID still needing UNDO */
}			xl_undo_discard;

#define SizeOfUndoDiscard	(offsetof(xl_undo_discard, oldest_xid) + sizeof(TransactionId))

/*
 * xl_undo_extend - WAL record for UNDO log file extension
 *
 * Logged when an UNDO log file is extended to accommodate more UNDO
 * records. This ensures the file size is correctly restored during
 * crash recovery.
 */
typedef struct xl_undo_extend
{
	uint32		log_number;		/* Which log is being extended */
	uint64		new_size;		/* New size of log file in bytes */
}			xl_undo_extend;

#define SizeOfUndoExtend	(offsetof(xl_undo_extend, new_size) + sizeof(uint64))

/*
 * xl_undo_apply - CLR for physical UNDO application
 *
 * This is a Compensation Log Record (CLR) generated when an UNDO record
 * is physically applied to a heap page during transaction rollback.
 *
 * The actual page modification is captured via REGBUF_FORCE_IMAGE, which
 * stores a full page image in the WAL record.  The xl_undo_apply metadata
 * provides additional context for debugging, pg_waldump output, and
 * potential future optimization of the redo path.
 *
 * During redo, if a full page image is present (BLK_RESTORED), no
 * additional action is needed.  If BLK_NEEDS_REDO, the page must be
 * re-read and the UNDO operation re-applied (but this case should not
 * occur with REGBUF_FORCE_IMAGE).
 */
typedef struct xl_undo_apply
{
	UndoRecPtr	urec_ptr;		/* UNDO record pointer that was applied */
	TransactionId xid;			/* Transaction being rolled back */
	RelFileLocator target_locator;	/* Target relation file locator */
	BlockNumber target_block;	/* Target block number */
	OffsetNumber target_offset; /* Target item offset within page */
	uint16		operation_type; /* UNDO record type (UNDO_INSERT, etc.) */
}			xl_undo_apply;

#define SizeOfUndoApply	(offsetof(xl_undo_apply, operation_type) + sizeof(uint16))

/*
 * xl_undo_chain_state - UNDO chain state for prepared transactions
 *
 * Saved in the two-phase state file during PREPARE TRANSACTION, so the
 * UNDO chain can be restored during COMMIT/ROLLBACK PREPARED.
 */
typedef struct xl_undo_chain_state
{
	UndoRecPtr	firstUndoPtr;	/* First UNDO record in transaction chain */
	UndoRecPtr	currentUndoPtr; /* Most recent UNDO record in chain */
}			xl_undo_chain_state;

/* Function declarations for WAL operations */
extern void undo_redo(XLogReaderState *record);
extern void undo_desc(StringInfo buf, XLogReaderState *record);
extern const char *undo_identify(uint8 info);

/* Two-phase commit support */
extern void undo_twophase_recover(FullTransactionId fxid, uint16 info,
								  void *recdata, uint32 len);
extern void undo_twophase_postcommit(FullTransactionId fxid, uint16 info,
									 void *recdata, uint32 len);
extern void undo_twophase_postabort(FullTransactionId fxid, uint16 info,
									void *recdata, uint32 len);

#endif							/* UNDO_XLOG_H */
