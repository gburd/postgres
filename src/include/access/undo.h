/*-------------------------------------------------------------------------
 *
 * undo.h
 *	  Common undo layer interface
 *
 * The undo subsystem consists of several logically separate subsystems
 * that work together:
 *
 *   undolog.c       - Undo log file management and space allocation
 *   undorecord.c    - Record format, serialization, and UndoRecordSet
 *   xactundo.c      - Per-transaction record set management
 *   undoapply.c     - Physical undo application during rollback
 *   undoworker.c    - Background discard worker
 *   undo_bufmgr.c   - Buffer management via shared_buffers
 *   undo_xlog.c     - WAL redo routines
 *
 * This header provides the unified entry points for shared memory
 * initialization and startup/shutdown coordination across all undo
 * subsystems.  Uses the v19devel subsystem callback API where
 * UndoShmemCallbacks coordinate shared memory requests and initialization
 * across all UNDO subsystems.
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
 * Unified shared memory initialization via subsystem callbacks.
 *
 * UndoShmemCallbacks is registered in subsystemlist.h and coordinates
 * shared memory requests and initialization for all UNDO subsystems.
 */
typedef struct ShmemCallbacks ShmemCallbacks;

extern const ShmemCallbacks UndoShmemCallbacks;
extern void UndoShmemInit(void);

/* Per-backend initialization */
extern void InitializeUndo(void);

/* Memory context for undo-related allocations */
extern MemoryContext UndoContext;

#endif							/* UNDO_H */
