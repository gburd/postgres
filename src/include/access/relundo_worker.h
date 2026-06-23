/*-------------------------------------------------------------------------
 *
 * relundo_worker.h
 *	  Background worker for applying per-relation UNDO records asynchronously
 *
 * This module implements background workers that apply per-relation UNDO
 * records for aborted transactions. The workers run asynchronously, similar
 * to autovacuum, to avoid blocking ROLLBACK commands.
 *
 * Architecture:
 * - Main launcher process manages worker pool
 * - Individual workers process UNDO chains for specific databases
 * - Shared memory queue tracks pending UNDO work
 * - Workers coordinate to avoid duplicate work
 *
 * This follows the ZHeap architecture where UNDO application is deferred
 * to background processes rather than being synchronous during ROLLBACK.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/relundo_worker.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RELUNDO_WORKER_H
#define RELUNDO_WORKER_H

#include "postgres.h"
#include "access/relundo.h"
#include "datatype/timestamp.h"
#include "storage/lwlock.h"

/*
 * Shared memory structure for UNDO work queue
 */
/*
 * MAX_UNDO_WORK_ITEMS limits the in-flight UNDO work queue.
 * Keep moderate (64) so shmem stays small during bootstrap.
 * Production workloads rarely exceed this with synchronous abort.
 */
#define MAX_UNDO_WORK_ITEMS 64

typedef struct RelUndoWorkItem
{
	Oid			dboid;			/* Database OID */
	Oid			reloid;			/* Relation OID */
	RelUndoRecPtr start_urec_ptr; /* First UNDO record to apply */
	TransactionId xid;			/* Transaction that created the UNDO */
	TimestampTz	queued_at;		/* When this was queued */
	bool		in_progress;	/* Worker currently processing this */
	int			worker_id;		/* ID of worker processing (if in_progress) */
} RelUndoWorkItem;

typedef struct RelUndoWorkQueue
{
	LWLock		lock;			/* Protects the queue */
	int			num_items;		/* Number of pending items */
	int			next_worker_id; /* For assigning worker IDs */
	RelUndoWorkItem items[MAX_UNDO_WORK_ITEMS];
} RelUndoWorkQueue;

/*
 * Worker registration and lifecycle
 */
extern Size RelUndoWorkerShmemSize(void);
extern void RelUndoWorkerShmemInit(void);
extern void RelUndoLauncherMain(Datum main_arg);
extern void RelUndoWorkerMain(Datum main_arg);

/*
 * Work queue operations
 */
extern void RelUndoQueueAdd(Oid dboid, Oid reloid, RelUndoRecPtr start_urec_ptr,
							TransactionId xid);
extern bool RelUndoQueueGetNext(RelUndoWorkItem *item_out, int worker_id);
extern void RelUndoQueueMarkComplete(Oid dboid, Oid reloid, int worker_id);

/*
 * Worker management
 */
extern void StartRelUndoWorker(Oid dboid);
extern void WaitForPendingRelUndo(void);

/* GUC parameters */
extern int	max_relundo_workers;
extern int	relundo_worker_naptime;

#endif							/* RELUNDO_WORKER_H */
