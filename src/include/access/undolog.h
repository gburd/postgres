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
#include "datatype/timestamp.h"
#include "port/atomics.h"
#include "port/pg_crc32c.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"

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

/* Rotation and pressure thresholds (percentage of segment size) */
#define UNDO_ROTATE_THRESHOLD_PCT		85	/* Seal & rotate at 85% capacity */
#define UNDO_PRESSURE_THRESHOLD_PCT		95	/* Emergency sync discard at 95% */
#define UNDO_CHECKPOINT_ROTATE_PCT		50	/* Checkpoint rotates if >50% full */
#define UNDO_BACKPRESSURE_MIN_US	   100	/* Min sleep under pressure */
#define UNDO_BACKPRESSURE_MAX_US	100000	/* Max sleep (100ms) under pressure */

/*
 * UndoLogState: Lifecycle state of an UNDO log segment
 *
 * Each UNDO log slot progresses through these states:
 *   FREE -> ACTIVE -> SEALED -> DISCARDABLE -> FREE
 *
 * At most one log is ACTIVE at any time (accepting new writes).
 */
typedef enum UndoLogState
{
	UNDO_LOG_FREE = 0,			/* Slot available, no file */
	UNDO_LOG_ACTIVE,			/* Accepting writes */
	UNDO_LOG_SEALED,			/* Full or rotated; no more writes */
	UNDO_LOG_DISCARDABLE		/* All records discarded; file can be deleted */
}			UndoLogState;

/*
 * UndoLogControl: Shared memory control structure for one UNDO log
 *
 * Each active UNDO log has one of these in shared memory.
 */
typedef struct UndoLogControl
{
	uint32		log_number;		/* Log number (matches file name) */
	pg_atomic_uint64 insert_ptr;	/* Next insertion point (atomic) */
	UndoRecPtr	discard_ptr;	/* Can discard older than this */
	TransactionId oldest_xid;	/* Oldest transaction needing this log */
	LWLock		lock;			/* Protects metadata (NOT insert_ptr) */
	bool		in_use;			/* Is this log slot active? */
	/* Lifecycle management fields */
	UndoLogState state;			/* Current lifecycle state */
	pg_atomic_uint64 seal_ptr;	/* insert_ptr frozen at seal time */
	TimestampTz sealed_time;	/* When this log was sealed (monitoring) */
}			UndoLogControl;

/*
 * UndoLogSharedData: Shared memory for all UNDO logs
 */
typedef struct UndoLogSharedData
{
	UndoLogControl logs[MAX_UNDO_LOGS];
	uint32		next_log_number;	/* Next log number to allocate */
	LWLock		allocation_lock;	/* Protects log allocation */
	/* Active log tracking and cumulative counters */
	pg_atomic_uint32 active_log_idx;	/* Index of ACTIVE log (MAX_UNDO_LOGS =
										 * none) */
	pg_atomic_uint64 total_allocated;	/* Cumulative bytes allocated */
	pg_atomic_uint64 total_discarded;	/* Cumulative bytes discarded */
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
extern Size UndoLogShmemSize(void);
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

/* Cached fd management: sync dirty logs and close cached fds */
extern void UndoLogSync(void);
extern void UndoLogCloseFiles(void);

/* Per-backend max write pointer for UNDO flush daemon */
extern UndoRecPtr UndoFlushGetMaxWritePtr(void);
extern void UndoFlushResetMaxWritePtr(void);

/* Checkpoint support */
extern void CheckPointUndoLog(void);

/* Segment rotation and pressure management */
extern void UndoLogSealAndRotate(uint8 trigger);
extern void UndoLogDeleteSegmentFile(uint32 log_number);
extern bool UndoLogTryPressureDiscard(void);

#endif							/* UNDOLOG_H */
