/*-------------------------------------------------------------------------
 *
 * undo.c
 *	  Common undo layer coordination
 *
 * The undo subsystem consists of several logically separate subsystems
 * that work together to achieve a common goal. The code in this file
 * provides a limited amount of common infrastructure that can be used
 * by all of those various subsystems, and helps coordinate activities
 * such as shared memory initialization and startup/shutdown.
 *
 * Shared memory initialization uses the PG_SHMEM_SUBSYSTEM pattern:
 * UndoShmemCallbacks is registered in subsystemlist.h, and the framework
 * calls UndoShmemRequest() and UndoShmemInit() at the appropriate times
 * during postmaster startup.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/undo/undo.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/atm.h"
#include "access/heapam.h"
#include "access/nbtree.h"
#include "access/recno_undo.h"
#include "access/slog.h"
#include "access/logical_revert_worker.h"
#include "access/undo.h"
#include "access/undo_flush.h"
#include "access/undolog.h"
#include "access/undormgr.h"
#include "access/undoworker.h"
#include "access/xactundo.h"
#include "storage/fileops.h"
#include "storage/ipc.h"
#include "storage/shmem.h"
#include "storage/subsystems.h"
#include "utils/memutils.h"

/*
 * UndoContext is a child of TopMemoryContext which is never reset. The only
 * reason for having a separate context is to make it easier to spot leaks or
 * excessive memory utilization related to undo operations.
 */
MemoryContext UndoContext = NULL;

static void AtProcExit_Undo(int code, Datum arg);
static void UndoShmemRequest_internal(void *arg);
static void UndoShmemInit_internal(void *arg);

/*
 * ShmemCallbacks for the UNDO subsystem.
 *
 * Registered via PG_SHMEM_SUBSYSTEM(UndoShmemCallbacks) in subsystemlist.h.
 *
 * request_fn registers the sLog's shared-memory hash tables via the modern
 * ShmemRequestHash() pattern so that CalculateShmemSize() accounts for them.
 * Other UNDO sub-modules still use the legacy ShmemInitStruct() pattern in
 * init_fn, fitting within the 100KB padding.
 *
 * init_fn initializes the contents of all UNDO shared memory structures.
 */
const ShmemCallbacks UndoShmemCallbacks = {
	.request_fn = UndoShmemRequest_internal,
	.init_fn = UndoShmemInit_internal,
};

/*
 * UndoShmemSize
 *		Figure out how much shared memory will be needed for undo.
 *
 * Each subsystem separately computes the space it requires, and we
 * carefully add up those values here.  Note: sLog shared memory is
 * registered via SLogShmemRequest() in the request_fn callback, so
 * CalculateShmemSize() accounts for it directly.  SLogShmemSize()
 * is included here for informational completeness only.
 */
Size
UndoShmemSize(void)
{
	Size		size;

	size = UndoLogShmemSize();
	size = add_size(size, XactUndoShmemSize());
	size = add_size(size, UndoWorkerShmemSize());
	size = add_size(size, LogicalRevertShmemSize());
	size = add_size(size, ATMShmemSize());
	size = add_size(size, SLogShmemSize());
	size = add_size(size, UndoFlushShmemSize());

	return size;
}

/*
 * UndoShmemRequest_internal
 *		Register shared memory needs for UNDO subsystems.
 *
 * Called during the request_fn phase of postmaster startup, before shared
 * memory is allocated.  Currently only the sLog uses the modern
 * ShmemRequestHash/ShmemRequestStruct pattern; other UNDO sub-modules
 * use the legacy ShmemInitStruct pattern in init_fn.
 */
static void
UndoShmemRequest_internal(void *arg)
{
	SLogShmemRequest();

	/*
	 * Register the UNDO flush writer background worker.  This must happen
	 * during the request_fn phase (before BackgroundWorkerShmemInit runs
	 * in the init_fn phase), because RegisterBackgroundWorker() cannot be
	 * called after BackgroundWorkerShmemInit().
	 *
	 * Use a static flag to ensure we only register once.  The request_fn
	 * callback is called again during postmaster reinitialize (after a
	 * child crash), and RegisterBackgroundWorker() would fail if called
	 * after the first shmem init.
	 */
	{
		static bool flush_writer_registered = false;

		if (enable_undo && !flush_writer_registered)
		{
			UndoFlushWriterRegister();
			flush_writer_registered = true;
		}
	}
}

/*
 * UndoShmemInit / UndoShmemInit_internal
 *		Initialize undo-related shared memory.
 *
 * Also, perform other initialization steps that need to be done very early.
 * This is called once during postmaster startup via the ShmemCallbacks
 * framework.
 */
static void
UndoShmemInit_internal(void *arg)
{
	UndoShmemInit();
}

void
UndoShmemInit(void)
{
	/*
	 * Initialize the undo memory context. If it already exists (crash restart
	 * via reset_shared()), reset it instead.
	 */
	if (UndoContext)
		MemoryContextReset(UndoContext);
	else
		UndoContext = AllocSetContextCreate(TopMemoryContext, "Undo",
											ALLOCSET_DEFAULT_SIZES);

	/* Now give various undo subsystems a chance to initialize. */
	UndoLogShmemInit();
	XactUndoShmemInit();
	UndoWorkerShmemInit();
	LogicalRevertShmemInit();
	ATMShmemInit();
	SLogShmemInit();
	UndoFlushShmemInit();

	/*
	 * Initialize the UNDO resource manager dispatch table.
	 */
	InitUndoRmgrs();

	/*
	 * Register built-in UNDO resource managers.
	 */
	HeapUndoRmgrInit();
	NbtreeUndoRmgrInit();
	FileopsUndoRmgrInit();
	RecnoUndoRmgrInit();
}

/*
 * InitializeUndo
 *		Per-backend initialization for the undo subsystem.
 *
 * Called once per backend from InitPostgres().
 */
void
InitializeUndo(void)
{
	InitializeXactUndo();
	on_shmem_exit(AtProcExit_Undo, 0);
}

/*
 * AtProcExit_Undo
 *		Shut down undo subsystems in the correct order.
 *
 * Higher-level stuff should be shut down first.
 */
static void
AtProcExit_Undo(int code, Datum arg)
{
	AtProcExit_XactUndo();
}
