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
 * This design follows the EDB undo-record-set branch architecture
 * where UndoShmemSize()/UndoShmemInit() aggregate all subsystem
 * requirements into a single entry point called from ipci.c.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/undo/undo.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/relundo_worker.h"
#include "access/undo.h"
#include "access/undolog.h"
#include "access/undoworker.h"
#include "access/xactundo.h"
#include "storage/ipc.h"
#include "storage/shmem.h"
#include "utils/memutils.h"

/*
 * UndoContext is a child of TopMemoryContext which is never reset. The only
 * reason for having a separate context is to make it easier to spot leaks or
 * excessive memory utilization related to undo operations.
 */
MemoryContext UndoContext = NULL;

static void UndoShmemRequest(void *arg);
static void UndoShmemInitWrapper(void *arg);
static void AtProcExit_Undo(int code, Datum arg);

const ShmemCallbacks UndoShmemCallbacks = {
	.request_fn = UndoShmemRequest,
	.init_fn = UndoShmemInitWrapper,
};

/*
 * UndoShmemRequest
 *		Request shared memory for undo subsystem.
 */
static void
UndoShmemRequest(void *arg)
{
	/*
	 * Request shared memory for UNDO subsystem structures using the new
	 * shmem allocation API. Each subsystem registers its structures here.
	 */
	UndoLogShmemRequest();
	UndoWorkerShmemRequest();
	RelUndoWorkerShmemRequest();
}

/*
 * UndoShmemInit
 *		Initialize undo-related shared memory.
 *
 * Also, perform other initialization steps that need to be done very early.
 * This is called once from ipci.c during postmaster startup.
 */
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
	RelUndoWorkerShmemInit();
}

/*
 * UndoShmemInitWrapper
 *		Wrapper for new subsystem callback API.
 */
static void
UndoShmemInitWrapper(void *arg)
{
	UndoShmemInit();
}

/*
 * InitializeUndo
 *		Per-backend initialization for the undo subsystem.
 *
 * Called once per backend from InitPostgres() or similar initialization
 * path.
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
