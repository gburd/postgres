/*-------------------------------------------------------------------------
 *
 * recno_undo.h
 *	  Public interface for the RECNO UNDO resource manager.
 *
 * RECNO records one UNDO record per tuple INSERT/UPDATE/DELETE.  Records
 * carry rmid UNDO_RMID_RECNO and a subtype in urec_info; rollback is
 * dispatched to recno_undo_apply() (recno_undo.c).
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * Author: Greg Burd <greg@burd.me>
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
 * RECNO's UNDO resource-manager ID.  IDs 1 (NBTREE) and 3 (HASH) are core;
 * 2 and 4 are reserved out-of-core; 5 = FLUX, 6 = ZHEAP.  RECNO takes 7.
 */
#define UNDO_RMID_RECNO		7

/* RECNO UNDO subtypes (occupy urec_info). */
#define RECNO_UNDO_INSERT			0x0001
#define RECNO_UNDO_UPDATE			0x0002
#define RECNO_UNDO_DELETE			0x0003
#define RECNO_UNDO_DELTA_UPDATE		0x0004

/* flags bits */
#define RECNO_UNDO_FLAG_HAS_TUPLE		0x0001
#define RECNO_UNDO_FLAG_PARTIAL_TUPLE	0x0002

/*
 * Fixed-length header prefixing every RECNO UNDO payload.  The
 * variable-length before-image (tuple bytes or a RecnoDiffRecord) follows.
 */
typedef struct RecnoUndoPayloadHeader
{
	ItemPointerData tid;		/* target tuple id */
	uint32		tuple_len;		/* length of trailing image */
	uint16		flags;			/* RECNO_UNDO_FLAG_* */
	uint16		pad;
} RecnoUndoPayloadHeader;

#define SizeOfRecnoUndoPayloadHeader	(sizeof(RecnoUndoPayloadHeader))

/* Registration entry point, called at postmaster startup. */
extern void RecnoUndoRmgrInit(void);

#endif							/* RECNO_UNDO_H */
