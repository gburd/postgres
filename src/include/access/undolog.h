/*-------------------------------------------------------------------------
 *
 * undolog.h
 *	  PostgreSQL UNDO log manager
 *
 * This module provides transactional UNDO logging capability to support:
 * 1. Heap tuple version recovery (pruned tuple versions)
 * 2. Transaction rollback using UNDO records
 * 3. Point-in-time recovery of deleted data
 *
 * UNDO records are organized in sequential logs stored in $PGDATA/base/undo/.
 * Each UNDO pointer (UndoRecPtr) encodes both log number and offset within log.
 *
 * Design inspired by ZHeap, BerkeleyDB, and Aether DB.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/undolog.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef UNDOLOG_H
#define UNDOLOG_H

#include "access/transam.h"
#include "access/undodefs.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "port/pg_crc32c.h"

/*
 * UndoRecPtr: 64-bit pointer to UNDO record
 *
 * Format (inspired by ZHeap):
 *   Bits 0-39:  Offset within log (40 bits = 1TB per log)
 *   Bits 40-63: Log number (24 bits = 16M logs)
 *
 * The actual UndoRecPtr typedef and InvalidUndoRecPtr are in undodefs.h
 * to avoid circular include dependencies.
 */

/* Extract log number and offset from UndoRecPtr */
#define UndoRecPtrGetLogNo(ptr) ((uint32) (((uint64) (ptr)) >> 40))
#define UndoRecPtrGetOffset(ptr) (((uint64) (ptr)) & 0xFFFFFFFFFFULL)

/* Construct UndoRecPtr from log number and offset */
#define MakeUndoRecPtr(logno, offset) \
	((((uint64) (logno)) << 40) | ((uint64) (offset)))

/*
 * UNDO log segment size: 1GB default
 * Can be overridden by undo_log_segment_size GUC
 */
#define UNDO_LOG_SEGMENT_SIZE (1024 * 1024 * 1024)

/* Maximum number of concurrent UNDO logs */
#define MAX_UNDO_LOGS 100

/*
 * UndoLogControl: Shared memory control structure for one UNDO log
 *
 * Each active UNDO log has one of these in shared memory.
 */
typedef struct UndoLogControl
{
	uint32		log_number;		/* Log number (matches file name) */
	UndoRecPtr	insert_ptr;		/* Next insertion point (end of log) */
	UndoRecPtr	discard_ptr;	/* Can discard older than this */
	TransactionId oldest_xid;	/* Oldest transaction needing this log */
	LWLock		lock;			/* Protects allocation and metadata */
	bool		in_use;			/* Is this log slot active? */
}			UndoLogControl;

/*
 * UndoLogSharedData: Shared memory for all UNDO logs
 */
typedef struct UndoLogSharedData
{
	UndoLogControl logs[MAX_UNDO_LOGS];
	uint32		next_log_number;	/* Next log number to allocate */
	LWLock		allocation_lock;	/* Protects log allocation */
}			UndoLogSharedData;

/* Global shared memory pointer (set during startup) */
extern UndoLogSharedData * UndoLogShared;

/* GUC parameters */
extern bool enable_undo;
extern int	undo_log_segment_size;
extern int	max_undo_logs;
extern int	undo_retention_time;
extern int	undo_worker_naptime;
extern int	undo_buffer_size;

/*
 * Public API for UNDO log management
 */

/* Shared memory initialization */
extern void UndoLogShmemRequest(void);
extern void UndoLogShmemInit(void);

/* UNDO log operations */
extern UndoRecPtr UndoLogAllocate(Size size);
extern void UndoLogWrite(UndoRecPtr ptr, const char *data, Size size);
extern void UndoLogRead(UndoRecPtr ptr, char *buffer, Size size);
extern void UndoLogDiscard(UndoRecPtr oldest_needed);

/* Utility functions */
extern char *UndoLogPath(uint32 log_number, char *path);
extern UndoRecPtr UndoLogGetInsertPtr(uint32 log_number);
extern UndoRecPtr UndoLogGetDiscardPtr(uint32 log_number);
extern UndoRecPtr UndoLogGetOldestDiscardPtr(void);

/* File management (also called from undo_xlog.c during redo) */
extern void ExtendUndoLogFile(uint32 log_number, uint64 new_size);

#endif							/* UNDOLOG_H */
