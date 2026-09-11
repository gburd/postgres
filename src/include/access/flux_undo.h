/*-------------------------------------------------------------------------
 *
 * flux_undo.h
 *	  Public interface for the FLUX UNDO resource manager
 *
 * FLUX participates in UNDO-in-WAL via its own UNDO resource manager
 * (UNDO_RMID_FLUX).  Records are written through the shared
 * UndoBuffer* (access/undobuffer.h) / Xact-level UNDO APIs (access/xactundo.h); rollback is
 * dispatched via undoapply.c to flux_undo_apply() based on the rmid
 * stamped into each UNDO record.
 *
 * Visibility correctness for aborted transactions is handled by
 * FLUX's sLog + FLUX_TUPLE_UNCOMMITTED flag, independently of
 * physical UNDO application.  The UNDO records written here drive
 * the logical-revert worker's physical cleanup of aborted rows so
 * VACUUM does not have to.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/include/access/flux_undo.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef FLUX_UNDO_H
#define FLUX_UNDO_H

#include "access/undodefs.h"
#include "access/undormgr.h"
#include "storage/itemptr.h"

/*
 * FLUX's UNDO resource-manager ID.  Defined in FLUX's own header, not in the
 * generic access/undormgr.h, so the UNDO core names no specific consumer.
 * See access/undormgr.h for the shared, WAL-durable ID number space.
 */
#define UNDO_RMID_FLUX		5

/*
 * FLUX UNDO subtypes.  Values occupy the 16-bit urec_info field of
 * the UNDO record header and are orthogonal to the FLUX WAL opcodes
 * in flux_xlog.h.
 */
#define FLUX_UNDO_INSERT			0x0001
#define FLUX_UNDO_UPDATE			0x0002	/* full-tuple before-image */
#define FLUX_UNDO_DELETE			0x0003	/* restore deleted tuple */

/*
 * Common fixed-length header for every FLUX UNDO payload.  The
 * variable-length tuple / diff image (if any) follows immediately
 * after the header.
 *
 * The header is deliberately small and self-describing so the same
 * struct can be passed as part1 in UndoBufferAddRecordParts()
 * avoiding an intermediate palloc.
 */
typedef struct FluxUndoPayloadHeader
{
	ItemPointerData tid;		/* target tuple id */
	uint32		tuple_len;		/* length of trailing tuple/diff image */
	uint16		flags;			/* future use: partial-tuple, index-flags */
	uint16		pad;
} FluxUndoPayloadHeader;

#define SizeOfFluxUndoPayloadHeader	(sizeof(FluxUndoPayloadHeader))

/* flags bits */
#define FLUX_UNDO_FLAG_HAS_TUPLE		0x0001
#define FLUX_UNDO_FLAG_PARTIAL_TUPLE	0x0002

/*
 * Registration entry point, called once at postmaster startup from
 * InitializeUndoSubsystem() alongside HeapUndoRmgrInit and friends.
 */
extern void FluxUndoRmgrInit(void);

/*
 * Install the FLUX implementations of the AM-neutral per-relation UNDO
 * hooks (see access/relundo.h).  Called from FluxUndoRmgrInit() so the
 * pointers are live before crash recovery replays any RELUNDO CLR.
 */
extern void FluxRelUndoInstallHooks(void);

#endif							/* FLUX_UNDO_H */
