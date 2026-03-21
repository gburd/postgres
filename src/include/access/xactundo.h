/*-------------------------------------------------------------------------
 *
 * xactundo.h
 *	  Transaction-level undo management
 *
 * This module manages per-transaction undo record sets. It maintains
 * up to NUndoPersistenceLevels (3) record sets per transaction -- one
 * for each persistence level (permanent, unlogged, temporary). This
 * design follows the EDB undo-record-set branch architecture where
 * undo records for different persistence levels are kept separate.
 *
 * Code that wants to write transactional undo should interface with
 * these functions rather than manipulating UndoRecordSet directly.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/xactundo.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef XACTUNDO_H
#define XACTUNDO_H

#include "access/undodefs.h"
#include "access/undorecord.h"
#include "access/xlogdefs.h"

/*
 * XactUndoContext - Context for a single undo insertion within a transaction.
 *
 * Created by PrepareXactUndoData(), consumed by InsertXactUndoData()
 * and cleaned up by CleanupXactUndoInsertion(). The plevel tracks which
 * persistence-level record set this insertion belongs to.
 */
typedef struct XactUndoContext
{
	UndoPersistenceLevel plevel;
	UndoRecordSet *uset;		/* borrowed reference, do not free */
}			XactUndoContext;

/* Shared memory initialization */
extern Size XactUndoShmemSize(void);
extern void XactUndoShmemInit(void);

/* Per-backend initialization */
extern void InitializeXactUndo(void);

/*
 * Undo insertion API for table AMs.
 *
 * PrepareXactUndoData: Find or create the appropriate per-persistence-level
 *   UndoRecordSet for the current transaction and prepare it for a new
 *   record. Returns the UndoRecPtr where the record will be written.
 *
 * InsertXactUndoData: Actually write the record data into the undo log.
 *
 * CleanupXactUndoInsertion: Release any resources held by the context.
 */
extern UndoRecPtr PrepareXactUndoData(XactUndoContext * ctx,
									  char persistence,
									  uint16 record_type,
									  Relation rel,
									  BlockNumber blkno,
									  OffsetNumber offset,
									  HeapTuple oldtuple);
extern void InsertXactUndoData(XactUndoContext * ctx);
extern void CleanupXactUndoInsertion(XactUndoContext * ctx);

/* Transaction lifecycle hooks */
extern void AtCommit_XactUndo(void);
extern void AtAbort_XactUndo(void);
extern void AtSubCommit_XactUndo(int level);
extern void AtSubAbort_XactUndo(int level);
extern void AtProcExit_XactUndo(void);

/* Undo chain traversal for rollback */
extern UndoRecPtr GetCurrentXactUndoRecPtr(UndoPersistenceLevel plevel);

#endif							/* XACTUNDO_H */
