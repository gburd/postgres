/*-------------------------------------------------------------------------
 *
 * undo_flush.h
 *	  UNDO flush daemon for group commit
 *
 * The UNDO flush daemon batches fdatasync calls across all backends,
 * reducing N independent disk flushes to 1 when N backends commit
 * concurrently.  Backends register flush requests via a shared
 * UndoRecPtr and wait on a ConditionVariable until the daemon has
 * synced past their request point.
 *
 * If the daemon is not running (startup, crash restart), backends
 * fall back to direct per-backend UndoLogSync() with fdatasync.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/undo_flush.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef UNDO_FLUSH_H
#define UNDO_FLUSH_H

#include "access/undodefs.h"
#include "port/atomics.h"
#include "storage/condition_variable.h"
#include "storage/lwlock.h"
#include "storage/procnumber.h"

/*
 * UndoFlushSharedData: shared memory for the UNDO flush daemon.
 *
 * flush_request is the highest UndoRecPtr that any backend needs synced.
 * flush_complete is the highest UndoRecPtr that has been synced to disk.
 * Backends compare their own write pointer against flush_complete to
 * decide when it is safe to return from commit.
 */
typedef struct UndoFlushSharedData
{
	ProcNumber	flush_writer_proc;	/* INVALID_PROC_NUMBER if not running */
	LWLock		lock;				/* protects non-atomic fields */
	ConditionVariable flush_cv;		/* backends wait here */
	pg_atomic_uint64 flush_request; /* highest UndoRecPtr needing flush */
	pg_atomic_uint64 flush_complete;	/* highest UndoRecPtr flushed */
	bool		sleeping;			/* hint: daemon is in WaitLatch */
	bool		shutdown_requested; /* daemon should exit */
} UndoFlushSharedData;

/* Shared memory sizing and initialization */
extern Size UndoFlushShmemSize(void);
extern void UndoFlushShmemInit(void);

/* Background worker registration (called from postmaster context) */
extern void UndoFlushWriterRegister(void);

/* Daemon entry point */
extern void UndoFlushWriterMain(Datum main_arg);

/* Backend interface: wait for flush daemon to sync up to my_ptr */
extern void UndoFlushWaitForSync(UndoRecPtr my_ptr);

/* Is the flush daemon currently running? */
extern bool UndoFlushWriterIsRunning(void);

#endif							/* UNDO_FLUSH_H */
