/*-------------------------------------------------------------------------
 *
 * undo.h
 *	  Common undo layer interface
 *
 * See access/undo/README for the pieces of the UNDO engine.  This header
 * provides the entry points for shared memory set-up and per-backend
 * initialization across them.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/undo.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef UNDO_H
#define UNDO_H

#include "access/undodefs.h"
#include "utils/palloc.h"

/*
 * Shared memory initialization, called through UndoShmemCallbacks (see
 * storage/subsystemlist.h).
 */
extern void UndoShmemInit(void);

/* Per-backend initialization */
extern void InitializeUndo(void);

/* Memory context for undo-related allocations */
extern MemoryContext UndoContext;

#endif							/* UNDO_H */
