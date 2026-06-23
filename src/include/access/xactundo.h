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

#include "access/relundo.h"
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
extern UndoRecPtr PrepareXactUndoDataParts(XactUndoContext *ctx,
										   char persistence,
										   uint8 rmid,
										   uint16 info,
										   Oid reloid,
										   const char *part1,
										   Size part1_len,
										   const char *part2,
										   Size part2_len);
extern void InsertXactUndoData(XactUndoContext *ctx);
extern void CleanupXactUndoInsertion(XactUndoContext *ctx);

/* Transaction lifecycle hooks */
extern void AtCommit_XactUndo(void);
extern void AtAbort_XactUndo(void);
extern void AtSubCommit_XactUndo(int level);
extern void AtSubAbort_XactUndo(int level);
extern void AtProcExit_XactUndo(void);

/* Per-relation UNDO chain registration (used by AMs on the per-relation fork) */
extern void RegisterPerRelUndo(Oid relid, RelUndoRecPtr start_urec_ptr);
extern RelUndoRecPtr GetPerRelUndoPtr(Oid relid);

/* Callback for IteratePerRelUndo: one call per registered chain head. */
typedef void (*PerRelUndoIterCB) (Oid relid, RelUndoRecPtr start_urec_ptr,
								  void *arg);
extern void IteratePerRelUndo(PerRelUndoIterCB callback, void *arg);
extern bool XactUndoHasUnrecoverableUndo(void);

/* Undo chain traversal for rollback */
extern UndoRecPtr GetCurrentXactUndoRecPtr(UndoPersistenceLevel plevel);
extern XLogRecPtr GetCurrentXactLastBatchLSN(UndoPersistenceLevel plevel);
extern void XActUndoUpdateLastBatchLSN(XLogRecPtr lsn,
									   UndoPersistenceLevel plevel);

/*
 * GUC: UNDO bytes threshold for instant abort via ATM.
 *
 * Transactions with estimated UNDO bytes >= this threshold use ATM instant
 * abort (deferred rollback via Logical Revert worker).  Transactions below
 * the threshold use synchronous rollback inline during transaction abort.
 *
 * A value of 0 means always use ATM instant abort regardless of size.
 */
extern int	undo_instant_abort_threshold;

#endif							/* XACTUNDO_H */
