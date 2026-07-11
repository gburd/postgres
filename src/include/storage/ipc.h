/*-------------------------------------------------------------------------
 *
 * ipc.h
 *	  POSTGRES inter-process communication definitions.
 *
 * This file is misnamed, as it no longer has much of anything directly
 * to do with IPC.  The functionality here is concerned with managing
 * exit-time cleanup for either a postmaster or a backend.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/ipc.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef IPC_H
#define IPC_H

#include "utils/global_lifetime.h"

typedef void (*pg_on_exit_callback) (int code, Datum arg);
typedef void (*shmem_startup_hook_type) (void);

#define PG_BACKEND_MAX_ON_EXITS 20

typedef struct PgBackendExitCallback
{
	pg_on_exit_callback function;
	Datum		arg;
} PgBackendExitCallback;

typedef struct PgBackendExitState
{
	PgBackendExitCallback on_proc_exit_list[PG_BACKEND_MAX_ON_EXITS];
	PgBackendExitCallback on_shmem_exit_list[PG_BACKEND_MAX_ON_EXITS];
	PgBackendExitCallback before_shmem_exit_list[PG_BACKEND_MAX_ON_EXITS];
	MemoryContext retained_top_memory_context;
	int			on_proc_exit_index;
	int			on_shmem_exit_index;
	int			before_shmem_exit_index;
	bool		proc_exit_active;
	bool		shmem_exit_active;
	bool		proc_exit_done;
} PgBackendExitState;

/*----------
 * API for handling cleanup that must occur during either ereport(ERROR)
 * or ereport(FATAL) exits from a block of code.  (Typical examples are
 * undoing transient changes to shared-memory state.)
 *
 *		PG_ENSURE_ERROR_CLEANUP(cleanup_function, arg);
 *		{
 *			... code that might throw ereport(ERROR) or ereport(FATAL) ...
 *		}
 *		PG_END_ENSURE_ERROR_CLEANUP(cleanup_function, arg);
 *
 * where the cleanup code is in a function declared per pg_on_exit_callback.
 * The Datum value "arg" can carry any information the cleanup function
 * needs.
 *
 * This construct ensures that cleanup_function() will be called during
 * either ERROR or FATAL exits.  It will not be called on successful
 * exit from the controlled code.  (If you want it to happen then too,
 * call the function yourself from just after the construct.)
 *
 * Note: the macro arguments are multiply evaluated, so avoid side-effects.
 *----------
 */
#define PG_ENSURE_ERROR_CLEANUP(cleanup_function, arg)	\
	do { \
		before_shmem_exit(cleanup_function, arg); \
		PG_TRY()

#define PG_END_ENSURE_ERROR_CLEANUP(cleanup_function, arg)	\
		cancel_before_shmem_exit(cleanup_function, arg); \
		PG_CATCH(); \
		{ \
			cancel_before_shmem_exit(cleanup_function, arg); \
			cleanup_function (0, arg); \
			PG_RE_THROW(); \
		} \
		PG_END_TRY(); \
	} while (0)


/* ipc.c */
extern PgBackendExitState *PgCurrentBackendExitStateRef(void);
extern Size PgBackendConsumeRetainedTopMemoryAllocated(void);
extern void PgBackendInitializeExitState(PgBackendExitState *exit_state);
extern void PgBackendAdoptEarlyExitState(PgBackendExitState *exit_state);
extern bool PgBackendExitInProgress(void);
extern bool PgBackendShmemExitInProgress(void);
extern void PgBackendExitCleanup(int code);
pg_noreturn extern void PgBackendExitComplete(int code);
pg_noreturn extern void PgBackendExit(int code);
pg_noreturn extern void proc_exit(int code);
extern void shmem_exit(int code);
extern void on_proc_exit(pg_on_exit_callback function, Datum arg);
extern void on_shmem_exit(pg_on_exit_callback function, Datum arg);
extern void before_shmem_exit(pg_on_exit_callback function, Datum arg);
extern void cancel_before_shmem_exit(pg_on_exit_callback function, Datum arg);
extern void on_exit_reset(void);
extern void check_on_shmem_exit_lists_are_empty(void);

#define proc_exit_inprogress \
	(PgCurrentBackendExitStateRef()->proc_exit_active)
#define shmem_exit_inprogress \
	(PgCurrentBackendExitStateRef()->shmem_exit_active)

/* ipci.c */
extern PGDLLIMPORT PG_GLOBAL_RUNTIME shmem_startup_hook_type shmem_startup_hook;

extern void RegisterBuiltinShmemCallbacks(void);
extern Size CalculateShmemSize(void);
extern void CreateSharedMemoryAndSemaphores(void);
#ifdef EXEC_BACKEND
extern void AttachSharedMemoryStructs(void);
#endif
extern void InitializeShmemGUCs(void);

#endif							/* IPC_H */
