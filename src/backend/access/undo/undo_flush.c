/*-------------------------------------------------------------------------
 *
 * undo_flush.c
 *	  UNDO flush daemon for group commit
 *
 * The UNDO flush daemon batches fdatasync() calls across all backends.
 * When multiple backends commit concurrently, each registers its highest
 * written UndoRecPtr in a shared atomic variable and waits on a
 * ConditionVariable.  The daemon wakes, performs one fdatasync() per
 * dirty UNDO log file, and broadcasts the CV to wake all waiters.
 *
 * On Linux, fdatasync() operates on the file's inode/page cache, not
 * per-fd, so one fdatasync from the daemon syncs all writes from all
 * backends to that file.  Under N concurrent committing backends this
 * reduces disk flushes from N to 1.
 *
 * We use fdatasync instead of fsync because UNDO files are pre-allocated
 * via ftruncate(), so metadata sync is unnecessary.
 *
 * If the daemon is not running (startup, crash restart), backends fall
 * back to direct per-backend UndoLogSync().
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

#include <unistd.h>

#include "access/undo_flush.h"
#include "access/undolog.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "utils/guc.h"
#include "utils/wait_event.h"

/* Shared memory state */
static UndoFlushSharedData *UndoFlushShared = NULL;

/*
 * Per-daemon private fd cache for syncing UNDO log files.
 *
 * The daemon only reads and syncs files, never writes.  This cache is
 * independent of the per-backend write caches.
 */
typedef struct FlushDaemonFdEntry
{
	int			fd;				/* kernel fd, -1 if not open */
	uint32		log_number;		/* which UNDO log */
} FlushDaemonFdEntry;

static FlushDaemonFdEntry flush_fd_cache[MAX_UNDO_LOGS];
static bool flush_fd_cache_initialized = false;

/*
 * InitFlushFdCache
 *		Lazily initialize the daemon's private fd cache.
 */
static void
InitFlushFdCache(void)
{
	if (flush_fd_cache_initialized)
		return;

	for (int i = 0; i < MAX_UNDO_LOGS; i++)
	{
		flush_fd_cache[i].fd = -1;
		flush_fd_cache[i].log_number = 0;
	}
	flush_fd_cache_initialized = true;
}

/*
 * GetFlushDaemonFd
 *		Return a cached fd for the given log_number, opening if needed.
 */
static int
GetFlushDaemonFd(uint32 log_number)
{
	char		path[MAXPGPATH];
	int			free_slot = -1;

	InitFlushFdCache();

	/* Search for existing entry */
	for (int i = 0; i < MAX_UNDO_LOGS; i++)
	{
		if (flush_fd_cache[i].fd >= 0 &&
			flush_fd_cache[i].log_number == log_number)
			return flush_fd_cache[i].fd;
		if (flush_fd_cache[i].fd < 0 && free_slot < 0)
			free_slot = i;
	}

	/* Need to open the file */
	if (free_slot < 0)
	{
		/* Evict slot 0 */
		free_slot = 0;
		close(flush_fd_cache[free_slot].fd);
		flush_fd_cache[free_slot].fd = -1;
	}

	UndoLogPath(log_number, path);
	flush_fd_cache[free_slot].fd = BasicOpenFile(path, O_RDWR | PG_BINARY);
	if (flush_fd_cache[free_slot].fd < 0)
	{
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("UNDO flush daemon could not open \"%s\": %m", path)));
		return -1;
	}

	flush_fd_cache[free_slot].log_number = log_number;
	return flush_fd_cache[free_slot].fd;
}

/*
 * CloseFlushDaemonFds
 *		Close all cached fds in the daemon's private cache.
 */
static void
CloseFlushDaemonFds(void)
{
	if (!flush_fd_cache_initialized)
		return;

	for (int i = 0; i < MAX_UNDO_LOGS; i++)
	{
		if (flush_fd_cache[i].fd >= 0)
		{
			close(flush_fd_cache[i].fd);
			flush_fd_cache[i].fd = -1;
		}
	}
}

/*
 * PerformUndoFlush
 *		Sync all active UNDO log files to disk using fdatasync.
 *
 * Returns true if any work was done.
 */
static bool
PerformUndoFlush(void)
{
	UndoRecPtr	request;
	UndoRecPtr	complete;

	request = pg_atomic_read_u64(&UndoFlushShared->flush_request);
	complete = pg_atomic_read_u64(&UndoFlushShared->flush_complete);

	if (request <= complete)
		return false;

	/* Sync each active UNDO log file */
	for (int i = 0; i < MAX_UNDO_LOGS; i++)
	{
		UndoLogControl *log = &UndoLogShared->logs[i];
		int			fd;

		if (!log->in_use)
			continue;

		fd = GetFlushDaemonFd(log->log_number);
		if (fd < 0)
			continue;

		if (pg_fdatasync(fd) < 0)
			ereport(WARNING,
					(errcode_for_file_access(),
					 errmsg("UNDO flush daemon could not fdatasync log %u: %m",
							log->log_number)));
	}

	/*
	 * Advance flush_complete to the request level.  Use CAS to ensure we
	 * only move it forward.
	 */
	while (true)
	{
		uint64		old_complete = pg_atomic_read_u64(&UndoFlushShared->flush_complete);

		if (request <= old_complete)
			break;
		if (pg_atomic_compare_exchange_u64(&UndoFlushShared->flush_complete,
										   &old_complete, request))
			break;
	}

	/* Wake all waiting backends */
	ConditionVariableBroadcast(&UndoFlushShared->flush_cv);

	return true;
}

/*
 * UndoFlushShmemSize
 *		Calculate shared memory needed for the UNDO flush daemon.
 */
Size
UndoFlushShmemSize(void)
{
	return sizeof(UndoFlushSharedData);
}

/*
 * UndoFlushShmemInit
 *		Initialize shared memory for the UNDO flush daemon.
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
 *		Register the UNDO flush writer background worker.
 *
 * Must be called from postmaster context during startup (before fork).
 * Follows the UndoWorkerRegister() pattern.
 */
void
UndoFlushWriterRegister(void)
{
	BackgroundWorker worker;

	memset(&worker, 0, sizeof(BackgroundWorker));

	worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = 10;	/* restart after 10s if crashed */

	sprintf(worker.bgw_library_name, "postgres");
	sprintf(worker.bgw_function_name, "UndoFlushWriterMain");
	snprintf(worker.bgw_name, BGW_MAXLEN, "undo flush writer");
	snprintf(worker.bgw_type, BGW_MAXLEN, "undo flush writer");

	RegisterBackgroundWorker(&worker);
}

/*
 * UndoFlushWriterMain
 *		Main entry point for the UNDO flush daemon.
 *
 * Follows the walwriter.c sleeping/wakeup/SetLatch pattern.
 */
void
UndoFlushWriterMain(Datum main_arg)
{
	(void) main_arg;

	/* Establish signal handlers */
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);

	BackgroundWorkerUnblockSignals();

	/* Register ourselves in shared memory */
	LWLockAcquire(&UndoFlushShared->lock, LW_EXCLUSIVE);
	UndoFlushShared->flush_writer_proc = MyProcNumber;
	UndoFlushShared->shutdown_requested = false;
	LWLockRelease(&UndoFlushShared->lock);

	ereport(LOG,
			(errmsg("UNDO flush writer started")));

	/* Main loop */
	while (!ShutdownRequestPending)
	{
		ResetLatch(MyLatch);

		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		CHECK_FOR_INTERRUPTS();

		PerformUndoFlush();

		/*
		 * Sleep until woken by a backend or timeout.  The 200ms timeout
		 * matches the WAL writer and ensures we don't miss requests if a
		 * SetLatch is lost.
		 */
		LWLockAcquire(&UndoFlushShared->lock, LW_EXCLUSIVE);
		UndoFlushShared->sleeping = true;
		LWLockRelease(&UndoFlushShared->lock);

		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 200L,
						 WAIT_EVENT_UNDO_FLUSH_MAIN);

		LWLockAcquire(&UndoFlushShared->lock, LW_EXCLUSIVE);
		UndoFlushShared->sleeping = false;
		LWLockRelease(&UndoFlushShared->lock);
	}

	/* Perform one final flush before exiting */
	PerformUndoFlush();

	/* Clean up */
	CloseFlushDaemonFds();

	LWLockAcquire(&UndoFlushShared->lock, LW_EXCLUSIVE);
	UndoFlushShared->flush_writer_proc = INVALID_PROC_NUMBER;
	LWLockRelease(&UndoFlushShared->lock);

	ereport(LOG,
			(errmsg("UNDO flush writer shutting down")));

	proc_exit(0);
}

/*
 * UndoFlushWriterIsRunning
 *		Check whether the flush daemon is currently alive.
 */
bool
UndoFlushWriterIsRunning(void)
{
	if (UndoFlushShared == NULL)
		return false;
	return UndoFlushShared->flush_writer_proc != INVALID_PROC_NUMBER;
}

/*
 * UndoFlushWaitForSync
 *		Wait for the flush daemon to sync UNDO data up to my_ptr.
 *
 * If the daemon is not running, falls back to direct UndoLogSync().
 */
void
UndoFlushWaitForSync(UndoRecPtr my_ptr)
{
	if (!UndoRecPtrIsValid(my_ptr))
		return;

	/*
	 * Advance flush_request to at least my_ptr using CAS.
	 */
	while (true)
	{
		uint64		old_request = pg_atomic_read_u64(&UndoFlushShared->flush_request);

		if (my_ptr <= old_request)
			break;				/* someone else already requested >= ours */
		if (pg_atomic_compare_exchange_u64(&UndoFlushShared->flush_request,
										   &old_request, my_ptr))
			break;
	}

	/* Wake the daemon if it is sleeping */
	{
		bool		is_sleeping;

		LWLockAcquire(&UndoFlushShared->lock, LW_SHARED);
		is_sleeping = UndoFlushShared->sleeping;
		LWLockRelease(&UndoFlushShared->lock);

		if (is_sleeping)
		{
			ProcNumber	writer_proc = UndoFlushShared->flush_writer_proc;

			if (writer_proc != INVALID_PROC_NUMBER)
			{
				PGPROC	   *proc = GetPGProcByNumber(writer_proc);

				SetLatch(&proc->procLatch);
			}
		}
	}

	/* Wait until flush_complete >= my_ptr */
	ConditionVariablePrepareToSleep(&UndoFlushShared->flush_cv);
	while (true)
	{
		UndoRecPtr	complete;

		complete = pg_atomic_read_u64(&UndoFlushShared->flush_complete);
		if (complete >= my_ptr)
			break;

		/*
		 * Use a 5-second timeout so we can detect if the daemon has died and
		 * fall back to direct sync.
		 */
		if (ConditionVariableTimedSleep(&UndoFlushShared->flush_cv,
										5000,
										WAIT_EVENT_UNDO_FLUSH_SYNC))
		{
			/* Timeout -- check if daemon is still alive */
			if (!UndoFlushWriterIsRunning())
			{
				ConditionVariableCancelSleep();
				UndoLogSync();
				return;
			}
		}
	}
	ConditionVariableCancelSleep();
}
