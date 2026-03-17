/*-------------------------------------------------------------------------
 *
 * undo_xlog.h
 *	  UNDO resource manager WAL record definitions
 *
 * This file contains the WAL record format definitions for UNDO log
 * operations. These records are logged by the RM_UNDO_ID resource manager.
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

#include "access/xlogdefs.h"
#include "access/xlogreader.h"
#include "lib/stringinfo.h"

/* Forward declaration - full definition in undolog.h (backend only) */
typedef uint64 UndoRecPtr;
typedef uint32 TransactionId;

/*
 * WAL record types for UNDO operations
 *
 * These are the info codes for UNDO WAL records. The low 4 bits are used
 * for operation type, leaving the upper 4 bits for flags.
 */
#define XLOG_UNDO_ALLOCATE		0x00	/* Allocate UNDO log space */
#define XLOG_UNDO_DISCARD		0x10	/* Discard old UNDO records */
#define XLOG_UNDO_EXTEND		0x20	/* Extend UNDO log file */

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
} xl_undo_allocate;

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
} xl_undo_discard;

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
} xl_undo_extend;

#define SizeOfUndoExtend	(offsetof(xl_undo_extend, new_size) + sizeof(uint64))

/* Function declarations for WAL operations */
extern void undo_redo(XLogReaderState *record);
extern void undo_desc(StringInfo buf, XLogReaderState *record);
extern const char *undo_identify(uint8 info);

#endif							/* UNDO_XLOG_H */
