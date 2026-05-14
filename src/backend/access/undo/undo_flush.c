/*-------------------------------------------------------------------------
 *
 * undo_flush.c
 *	  UNDO flush daemon -- stub (UNDO-in-WAL version)
 *
 * With UNDO-in-WAL, UNDO data is stored in the standard WAL stream and
 * made durable via the normal WAL fsync at commit time.  The separate
 * UNDO flush daemon is no longer needed.  These stub functions are
 * retained because they are called from undo.c (shmem init) and
 * registered in bgworker.c (worker entry point table).
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/undo/undo_flush.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/undo_flush.h"
#include "access/undolog.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "utils/wait_event.h"

/* Shared memory state (still allocated for ABI compatibility) */
static UndoFlushSharedData *UndoFlushShared = NULL;

/*
 * UndoFlushShmemSize
 *		Still needed because undo.c calls this during shmem sizing.
 */
Size
UndoFlushShmemSize(void)
{
	return sizeof(UndoFlushSharedData);
}

/*
 * UndoFlushShmemInit
 *		Still needed because undo.c calls this during shmem init.
 */
void
UndoFlushShmemInit(void)
{
	bool		found;

	UndoFlushShared = (UndoFlushSharedData *)
		ShmemInitStruct("UNDO Flush Writer Data",
						UndoFlushShmemSize(),
						&found);

	if (!found)
	{
		UndoFlushShared->flush_writer_proc = INVALID_PROC_NUMBER;
		LWLockInitialize(&UndoFlushShared->lock, LWTRANCHE_UNDO_LOG);
		ConditionVariableInit(&UndoFlushShared->flush_cv);
		pg_atomic_init_u64(&UndoFlushShared->flush_request, 0);
		pg_atomic_init_u64(&UndoFlushShared->flush_complete, 0);
		UndoFlushShared->sleeping = false;
		UndoFlushShared->shutdown_requested = false;
	}
}

/*
 * UndoFlushWriterRegister
 *		Register the background worker entry.
 *
 * Still called from undo.c but the daemon does nothing.
 */
void
UndoFlushWriterRegister(void)
{
	BackgroundWorker worker;

	memset(&worker, 0, sizeof(BackgroundWorker));

	worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;	/* Don't restart */

	sprintf(worker.bgw_library_name, "postgres");
	sprintf(worker.bgw_function_name, "UndoFlushWriterMain");
	snprintf(worker.bgw_name, BGW_MAXLEN, "undo flush writer");
	snprintf(worker.bgw_type, BGW_MAXLEN, "undo flush writer");

	RegisterBackgroundWorker(&worker);
}

/*
 * UndoFlushWriterMain
 *		Daemon entry point -- immediately exits since flush is handled by WAL.
 */
void
UndoFlushWriterMain(Datum main_arg pg_attribute_unused())
{
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	BackgroundWorkerUnblockSignals();

	ereport(LOG,
			(errmsg("UNDO flush writer exiting (not needed with UNDO-in-WAL)")));

	proc_exit(0);
}

/*
 * UndoFlushWriterIsRunning
 */
bool
UndoFlushWriterIsRunning(void)
{
	return false;
}

/*
 * UndoFlushWaitForSync
 *		No-op: WAL sync at commit handles durability.
 */
void
UndoFlushWaitForSync(UndoRecPtr my_ptr pg_attribute_unused())
{
	/* No-op: UNDO data is in WAL, synced by XLogFlush at commit */
}
