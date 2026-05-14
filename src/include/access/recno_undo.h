/*-------------------------------------------------------------------------
 *
 * recno_undo.h
 *	  Public interface for the RECNO UNDO resource manager
 *
 * RECNO participates in UNDO-in-WAL via its own UNDO resource manager
 * (UNDO_RMID_RECNO).  Records are written through the shared
 * UndoBuffer* (access/undobuffer.h) / Xact-level UNDO APIs (access/xactundo.h); rollback is
 * dispatched via undoapply.c to recno_undo_apply() based on the rmid
 * stamped into each UNDO record.
 *
 * Visibility correctness for aborted transactions is handled by
 * RECNO's sLog + RECNO_TUPLE_UNCOMMITTED flag, independently of
 * physical UNDO application.  The UNDO records written here drive
 * the logical-revert worker's physical cleanup of aborted rows so
 * VACUUM does not have to.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/include/access/recno_undo.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RECNO_UNDO_H
#define RECNO_UNDO_H

#include "access/undodefs.h"
#include "access/undormgr.h"
#include "storage/itemptr.h"

/*
 * RECNO UNDO subtypes.  Values occupy the 16-bit urec_info field of
 * the UNDO record header and are orthogonal to the RECNO WAL opcodes
 * in recno_xlog.h.
 */
#define RECNO_UNDO_INSERT			0x0001
#define RECNO_UNDO_UPDATE			0x0002	/* full-tuple before-image */
#define RECNO_UNDO_DELETE			0x0003	/* restore deleted tuple */
#define RECNO_UNDO_DELTA_UPDATE		0x0004	/* byte-diff before-image */

/*
 * Common fixed-length header for every RECNO UNDO payload.  The
 * variable-length tuple / diff image (if any) follows immediately
 * after the header.
 *
 * The header is deliberately small and self-describing so the same
 * struct can be passed as part1 in UndoBufferAddRecordParts()
 * avoiding an intermediate palloc.
 */
typedef struct RecnoUndoPayloadHeader
{
	ItemPointerData tid;		/* target tuple id */
	uint32		tuple_len;		/* length of trailing tuple/diff image */
	uint16		flags;			/* future use: partial-tuple, index-flags */
	uint16		pad;
}			RecnoUndoPayloadHeader;

#define SizeOfRecnoUndoPayloadHeader	(sizeof(RecnoUndoPayloadHeader))

/* flags bits */
#define RECNO_UNDO_FLAG_HAS_TUPLE		0x0001
#define RECNO_UNDO_FLAG_PARTIAL_TUPLE	0x0002

/*
 * Registration entry point, called once at postmaster startup from
 * InitializeUndoSubsystem() alongside HeapUndoRmgrInit and friends.
 */
extern void RecnoUndoRmgrInit(void);

#endif							/* RECNO_UNDO_H */
