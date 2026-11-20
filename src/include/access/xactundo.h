/*-------------------------------------------------------------------------
 *
 * xactundo.h
 *	  Transaction-level undo management
 *
 * This module manages per-transaction undo record sets. It maintains
 * up to NUndoPersistenceLevels (3) record sets per transaction -- one
 * for each persistence level (permanent, unlogged, temporary), so that
 * records for different persistence levels are kept separate.
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
} XactUndoContext;

/* Shared memory initialization */
extern Size XactUndoShmemSize(void);
extern void XactUndoShmemRequest(void);
extern void XactUndoShmemInit(void);

/* Per-backend initialization */
extern void InitializeXactUndo(void);

/*
 * Undo insertion API for any AM or subsystem.
 *
 * PrepareXactUndoData: Find or create the appropriate per-persistence-level
 *   UndoRecordSet for the current transaction and prepare it for a new
 *   record. Returns the UndoRecPtr where the record will be written.
 *
 *   Parameters are AM-agnostic: the caller provides an RM ID, RM-specific
 *   info flags, a relation OID, and an opaque payload.
 *
 * InsertXactUndoData: Actually write the record data into the undo log.
 *
 * CleanupXactUndoInsertion: Release any resources held by the context.
 */
extern UndoRecPtr PrepareXactUndoData(XactUndoContext *ctx,
									  char persistence,
									  uint8 rmid,
									  uint16 info,
									  Oid reloid,
									  const char *payload,
									  Size payload_len);
extern void InsertXactUndoData(XactUndoContext *ctx);
extern void CleanupXactUndoInsertion(XactUndoContext *ctx);

/* Transaction lifecycle hooks */
extern void AtCommit_XactUndo(void);
extern void AtAbort_XactUndo(void);
extern void AtPrepare_XactUndo(void);
extern void AtSubCommit_XactUndo(int level);
extern void AtSubAbort_XactUndo(int level);
extern void AtProcExit_XactUndo(void);


/* Checkpoint: WAL-log the UNDO chains of in-flight transactions */
extern void LogXactUndoInflight(void);

/* Undo chain traversal for rollback */
extern XLogRecPtr GetCurrentXactLastBatchLSN(UndoPersistenceLevel plevel);

#endif							/* XACTUNDO_H */
