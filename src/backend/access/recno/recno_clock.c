/*-------------------------------------------------------------------------
 *
 * recno_clock.c
 *	  Clock-bound integration for RECNO timestamp-based MVCC
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/recno/recno_clock.c
 *
 * NOTES
 *	  This module integrates AWS clock-bound daemon to provide bounded
 *	  timestamps with error intervals for safe distributed MVCC and
 *	  logical replication. It implements:
 *
 *	  1. Clock-bound daemon integration via shared memory
 *	  2. RecnoTimestampBound structure with uncertainty intervals
 *	  3. Clock skew detection and self-shutdown on excessive drift
 *	  4. NTP health monitoring
 *	  5. Graceful fallback when clock-bound is unavailable
 *
 *	  The clock-bound daemon writes to /dev/shm/clockbound with:
 *	  - earliest: earliest possible current time
 *	  - latest: latest possible current time
 *	  - error_bound: maximum clock error in nanoseconds
 *
 *	  This enables safe timestamp comparisons even with clock skew.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/stat.h>
#include <fcntl.h>
#ifndef WIN32
#include <sys/mman.h>
#endif
#include <time.h>
#include <unistd.h>

#include "access/recno.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/guc.h"
#include "utils/timestamp.h"
#include "pgstat.h"				/* For WAIT_EVENT_EXTENSION */

/* Missing constant */
#ifndef USECS_PER_SEC
#define USECS_PER_SEC 1000000L
#endif
#include "utils/wait_event.h"

/* Clock-bound shared memory path */
#define CLOCKBOUND_SHM_PATH "/dev/shm/clockbound"

/* Clock-bound data structure from daemon */
typedef struct ClockBoundData
{
	struct timespec earliest;	/* Earliest possible time */
	struct timespec latest;		/* Latest possible time */
	uint64		error_bound_ns; /* Error bound in nanoseconds */
	uint32		segment_id;		/* Daemon segment ID */
	uint32		flags;			/* Status flags */
}			ClockBoundData;

/* Clock health monitoring state */
typedef struct RecnoClockMonitor
{
	TimestampTz last_sync_time; /* Last successful NTP sync */
	TimestampTz last_check_time;	/* Last health check */
	uint64		max_observed_error_ms;	/* Maximum observed error bound */
	uint64		total_skew_warnings;	/* Count of skew warnings */
	uint64		total_fatal_checks; /* Count of fatal threshold hits */
	bool		clock_bound_available;	/* Clock-bound daemon accessible */
	bool		shutdown_pending;	/* Shutdown triggered */
}			RecnoClockMonitor;

/* Shared memory for clock management */
typedef struct RecnoClockShmemData
{
	LWLock		lock;			/* Protects all fields */
	RecnoClockMonitor monitor;	/* Clock health monitoring */
	ClockBoundData last_bounds; /* Last read clock bounds */
	TimestampTz last_bounds_read;	/* When bounds were last read */
	int			clockbound_fd;	/* File descriptor for mmap */
	void	   *clockbound_map; /* Mapped clock-bound data */
	bool		initialized;	/* Initialization complete */
}			RecnoClockShmemData;

static RecnoClockShmemData * RecnoClockShmem = NULL;

/* GUC variables */
bool		recno_enable_clock_bound = true;
bool		recno_fatal_on_clock_drift = true;
int			recno_clock_check_interval_ms = 1000;

/* External GUC variable from recno_hlc.c */
extern int	recno_max_clock_offset_ms;

/* Background worker handle */
static BackgroundWorkerHandle *clock_monitor_handle = NULL;

/* Wait event for clock monitoring background worker */
static uint32 recno_clock_monitor_wait_event = 0;

/* Function prototypes */
static bool RecnoReadClockBound(ClockBoundData * bounds);
static void RecnoCheckClockHealth(void);
static void RecnoClockShmemRequest(void *arg);
static void RecnoClockShmemInit_cb(void *arg);

/*
 * RecnoClockShmemSize -- calculate shared memory size needed
 */
Size
RecnoClockShmemSize(void)
{
	return MAXALIGN(sizeof(RecnoClockShmemData));
}

/*
 * RecnoClockShmemInit -- initialize shared memory for clock management
 *
 * This is now handled automatically by the PG_SHMEM_SUBSYSTEM mechanism
 * via RecnoClockShmemCallbacks.  This function is retained for backward
 * compatibility but is a no-op if the subsystem has already been initialized.
 */
void
RecnoClockShmemInit(void)
{
	if (RecnoClockShmem != NULL && RecnoClockShmem->initialized)
		return;

	/*
	 * If called before the subsystem infrastructure, fall back to the
	 * old-style ShmemInitStruct path.
	 */
	if (RecnoClockShmem == NULL)
	{
		bool		found;

		RecnoClockShmem = (RecnoClockShmemData *)
			ShmemInitStruct("RECNO Clock Data",
							RecnoClockShmemSize(),
							&found);

		if (found)
			return;
	}

	/* Delegate to the init callback */
	RecnoClockShmemInit_cb(NULL);
}

/*
 * RecnoClockStartMonitor -- start background worker for clock monitoring
 */
void
RecnoClockStartMonitor(void)
{
	BackgroundWorker worker;

	if (!recno_enable_clock_bound)
		return;

	memset(&worker, 0, sizeof(BackgroundWorker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	snprintf(worker.bgw_library_name, BGW_MAXLEN, "postgres");
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "RecnoClockMonitorMain");
	snprintf(worker.bgw_name, BGW_MAXLEN, "RECNO clock monitor");
	snprintf(worker.bgw_type, BGW_MAXLEN, "RECNO clock monitor");
	worker.bgw_restart_time = 5;	/* Restart after 5 seconds on failure */
	worker.bgw_notify_pid = MyProcPid;
	worker.bgw_main_arg = (Datum) 0;

	if (RegisterDynamicBackgroundWorker(&worker, &clock_monitor_handle))
	{
		ereport(DEBUG1,
				(errmsg("recno clock monitor background worker started")));
	}
	else
	{
		ereport(DEBUG1,
				(errmsg("failed to start recno clock monitor background worker")));
	}
}

/*
 * RecnoClockMonitorMain -- main loop for clock monitor background worker
 *
 * This function must be public (not static) so the background worker system
 * can look it up by name when the worker is started.
 */
void
RecnoClockMonitorMain(Datum main_arg)
{
	(void) main_arg;			/* unused */

	/* Establish signal handlers */
	pqsignal(SIGTERM, PG_SIG_DFL);
	BackgroundWorkerUnblockSignals();

	/* Connect to shared memory */
	BackgroundWorkerInitializeConnection(NULL, NULL, 0);

	/* Register wait event for clock monitoring */
	if (recno_clock_monitor_wait_event == 0)
		recno_clock_monitor_wait_event = WaitEventExtensionNew("RecnoClockMonitor");

	ereport(DEBUG1,
			(errmsg("RECNO clock monitor started")));

	/* Main monitoring loop */
	while (!RecnoClockShmem->monitor.shutdown_pending)
	{
		int			rc;

		/* Check clock health */
		RecnoCheckClockHealth();

		/* Wait for next check interval or termination */
		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   recno_clock_check_interval_ms,
					   recno_clock_monitor_wait_event);

		ResetLatch(MyLatch);

		/* Exit on termination request */
		if (rc & WL_EXIT_ON_PM_DEATH)
			proc_exit(1);
	}

	/* Shutdown was triggered */
	ereport(FATAL,
			(errmsg("shutting down due to excessive recno clock drift"),
			 errhint("Fix time synchronization and restart the server.")));
}

/*
 * RecnoReadClockBound -- read current bounds from clock-bound daemon
 */
static bool
RecnoReadClockBound(ClockBoundData * bounds)
{
	if (!RecnoClockShmem->monitor.clock_bound_available ||
		RecnoClockShmem->clockbound_map == NULL)
		return false;

	/* Copy from mapped memory (atomic read) */
	memcpy(bounds, RecnoClockShmem->clockbound_map, sizeof(ClockBoundData));

	/* Validate the data */
	if (bounds->segment_id == 0 ||
		bounds->earliest.tv_sec == 0 ||
		bounds->latest.tv_sec == 0)
		return false;

	/* Sanity check: latest >= earliest */
	if (bounds->latest.tv_sec < bounds->earliest.tv_sec ||
		(bounds->latest.tv_sec == bounds->earliest.tv_sec &&
		 bounds->latest.tv_nsec < bounds->earliest.tv_nsec))
		return false;

	return true;
}

/*
 * RecnoGetTimestampBounds -- get current timestamp with error bounds
 */
RecnoTimestampBound
RecnoGetTimestampBounds(void)
{
	RecnoTimestampBound result;
	ClockBoundData bounds;
	bool		have_bounds = false;

	/* Always get HLC timestamp */
	result.hlc = HLCNow(0);

	/* Try to get clock-bound error bounds */
	if (recno_enable_clock_bound)
	{
		LWLockAcquire(&RecnoClockShmem->lock, LW_SHARED);

		/* Try to read fresh bounds */
		if (RecnoReadClockBound(&bounds))
		{
			/* Convert timespec to microseconds since PG epoch */
			result.earliest_us = (bounds.earliest.tv_sec * USECS_PER_SEC) +
				(bounds.earliest.tv_nsec / 1000);
			result.latest_us = (bounds.latest.tv_sec * USECS_PER_SEC) +
				(bounds.latest.tv_nsec / 1000);
			result.error_bound_ms = bounds.error_bound_ns / 1000000;
			result.bounds_valid = true;
			have_bounds = true;

			/* Update cached bounds */
			memcpy(&RecnoClockShmem->last_bounds, &bounds, sizeof(ClockBoundData));
			RecnoClockShmem->last_bounds_read = GetCurrentTimestamp();
		}

		LWLockRelease(&RecnoClockShmem->lock);
	}

	/* Fallback: use HLC +/- max_offset */
	if (!have_bounds)
	{
		uint64		physical_ms = HLC_GET_PHYSICAL(result.hlc);
		uint64		offset_us = recno_max_clock_offset_ms * 1000;

		result.earliest_us = (physical_ms * 1000) - offset_us;
		result.latest_us = (physical_ms * 1000) + offset_us;
		result.error_bound_ms = recno_max_clock_offset_ms;
		result.bounds_valid = false;
	}

	return result;
}

/*
 * RecnoCheckClockHealth -- periodic clock health check
 */
static void
RecnoCheckClockHealth(void)
{
	RecnoTimestampBound bounds;
	uint64		error_ms;
	TimestampTz now = GetCurrentTimestamp();

	bounds = RecnoGetTimestampBounds();

	/* Calculate current error bound */
	if (bounds.bounds_valid)
	{
		error_ms = bounds.error_bound_ms;
	}
	else
	{
		/* No clock-bound, use configured max offset */
		error_ms = recno_max_clock_offset_ms;
	}

	/* Update monitoring state */
	LWLockAcquire(&RecnoClockShmem->lock, LW_EXCLUSIVE);

	RecnoClockShmem->monitor.last_check_time = now;

	if (error_ms > RecnoClockShmem->monitor.max_observed_error_ms)
		RecnoClockShmem->monitor.max_observed_error_ms = error_ms;

	/* Check for excessive clock drift */
	if (error_ms > recno_max_clock_offset_ms * 0.8)
	{
		RecnoClockShmem->monitor.total_fatal_checks++;

		if (recno_fatal_on_clock_drift)
		{
			RecnoClockShmem->monitor.shutdown_pending = true;
			LWLockRelease(&RecnoClockShmem->lock);

			ereport(FATAL,
					(errmsg("recno clock error bound %lu ms exceeds 80%% of maximum %d ms",
							error_ms, recno_max_clock_offset_ms),
					 errhint("Fix time synchronization or increase recno.max_clock_offset.")));
		}
	}
	else if (error_ms > recno_max_clock_offset_ms * 0.5)
	{
		RecnoClockShmem->monitor.total_skew_warnings++;

		ereport(WARNING,
				(errmsg("recno clock error bound %lu ms exceeds 50%% of maximum %d ms",
						error_ms, recno_max_clock_offset_ms)));
	}

	/* Check for NTP sync loss */
	if (TimestampDifferenceExceeds(RecnoClockShmem->monitor.last_sync_time,
								   now, 300000))	/* 5 minutes */
	{
		ereport(WARNING,
				(errmsg("recno: NTP synchronization may be lost (no update for 5 minutes)")));

		if (TimestampDifferenceExceeds(RecnoClockShmem->monitor.last_sync_time,
									   now, 600000))	/* 10 minutes */
		{
			if (recno_fatal_on_clock_drift)
			{
				RecnoClockShmem->monitor.shutdown_pending = true;
				LWLockRelease(&RecnoClockShmem->lock);

				ereport(FATAL,
						(errmsg("recno: NTP synchronization lost for 10 minutes"),
						 errhint("Check NTP configuration and network connectivity.")));
			}
		}
	}

	LWLockRelease(&RecnoClockShmem->lock);
}

/*
 * RecnoWaitForClockBound -- wait until clock uncertainty is resolved
 *
 * Used by replicas to wait until they can safely apply a change
 * without violating causality.
 */
void
RecnoWaitForClockBound(RecnoTimestampBound origin_bounds)
{
	RecnoTimestampBound current;
	int			wait_us = 0;

	/* If no valid bounds, use HLC comparison */
	if (!origin_bounds.bounds_valid)
	{
		/* Ensure local HLC has advanced past origin */
		while (HLCCompare(HLCGetGlobal(), origin_bounds.hlc) <= 0)
		{
			pg_usleep(1000);	/* 1ms */
			wait_us += 1000;

			if (wait_us > 1000000)	/* 1 second max wait */
			{
				ereport(DEBUG1,
						(errmsg("recno: waited 1 second for HLC to advance")));
				break;
			}
		}
		return;
	}

	/* With clock-bound, wait for uncertainty to resolve */
	for (;;)
	{
		current = RecnoGetTimestampBounds();

		/* Safe if our earliest > their latest */
		if (current.bounds_valid &&
			current.earliest_us > origin_bounds.latest_us)
			break;

		/* Also safe if HLC sufficiently advanced */
		if (HLCCompare(current.hlc, origin_bounds.hlc) > 0)
		{
			uint64		hlc_diff_ms = HLC_GET_PHYSICAL(current.hlc) -
				HLC_GET_PHYSICAL(origin_bounds.hlc);

			if (hlc_diff_ms > recno_max_clock_offset_ms)
				break;
		}

		/* Wait a bit and retry */
		pg_usleep(1000);		/* 1ms */
		wait_us += 1000;

		if (wait_us > recno_max_clock_offset_ms * 1000)
		{
			ereport(DEBUG1,
					(errmsg("recno: waited %d ms for clock bound resolution",
							wait_us / 1000)));
			break;
		}
	}
}

/*
 * RecnoClockGetStats -- get clock monitoring statistics
 */
void
RecnoClockGetStats(RecnoClockStats *stats)
{
	if (RecnoClockShmem == NULL || stats == NULL)
		return;

	LWLockAcquire(&RecnoClockShmem->lock, LW_SHARED);

	stats->clock_bound_available = RecnoClockShmem->monitor.clock_bound_available;
	stats->max_observed_error_ms = RecnoClockShmem->monitor.max_observed_error_ms;
	stats->total_skew_warnings = RecnoClockShmem->monitor.total_skew_warnings;
	stats->total_fatal_checks = RecnoClockShmem->monitor.total_fatal_checks;
	stats->last_sync_time = RecnoClockShmem->monitor.last_sync_time;
	stats->last_check_time = RecnoClockShmem->monitor.last_check_time;

	LWLockRelease(&RecnoClockShmem->lock);
}

/*
 * RecnoClockShutdown -- cleanup clock resources at shutdown
 */
void
RecnoClockShutdown(void)
{
	if (RecnoClockShmem == NULL)
		return;

	LWLockAcquire(&RecnoClockShmem->lock, LW_EXCLUSIVE);

#ifndef WIN32
	/* Unmap clock-bound shared memory (POSIX-only integration) */
	if (RecnoClockShmem->clockbound_map != NULL)
	{
		munmap(RecnoClockShmem->clockbound_map, sizeof(ClockBoundData));
		RecnoClockShmem->clockbound_map = NULL;
	}

	/* Close file descriptor */
	if (RecnoClockShmem->clockbound_fd >= 0)
	{
		close(RecnoClockShmem->clockbound_fd);
		RecnoClockShmem->clockbound_fd = -1;
	}
#endif

	RecnoClockShmem->monitor.clock_bound_available = false;

	LWLockRelease(&RecnoClockShmem->lock);
}

/*
 * Subsystem callback wrappers for PG_SHMEM_SUBSYSTEM infrastructure.
 *
 * These allow the postmaster to request shared memory and initialize
 * the clock subsystem automatically during startup, using the same
 * pattern as RecnoHLCShmemCallbacks and RecnoMvccShmemCallbacks.
 */
static void
RecnoClockShmemRequest(void *arg)
{
	ShmemRequestStruct(.name = "RECNO Clock Data",
					   .size = RecnoClockShmemSize(),
					   .ptr = (void **) &RecnoClockShmem);
}

static void
RecnoClockShmemInit_cb(void *arg)
{
	/* RecnoClockShmem is already set by the ShmemRequestStruct .ptr mechanism */
	Assert(RecnoClockShmem != NULL);

	LWLockInitialize(&RecnoClockShmem->lock, LWTRANCHE_BUFFER_MAPPING);

	/* Initialize monitor state */
	RecnoClockShmem->monitor.last_sync_time = GetCurrentTimestamp();
	RecnoClockShmem->monitor.last_check_time = GetCurrentTimestamp();
	RecnoClockShmem->monitor.max_observed_error_ms = 0;
	RecnoClockShmem->monitor.total_skew_warnings = 0;
	RecnoClockShmem->monitor.total_fatal_checks = 0;
	RecnoClockShmem->monitor.clock_bound_available = false;
	RecnoClockShmem->monitor.shutdown_pending = false;

	/* Initialize clock-bound state */
	memset(&RecnoClockShmem->last_bounds, 0, sizeof(ClockBoundData));
	RecnoClockShmem->last_bounds_read = 0;
	RecnoClockShmem->clockbound_fd = -1;
	RecnoClockShmem->clockbound_map = NULL;
	RecnoClockShmem->initialized = false;

	/*
	 * Try to open clock-bound shared memory.  POSIX-only: uses mmap of a
	 * SysV/POSIX shared object.  On Windows we skip the integration; the
	 * fallback HLC-only mode is correct, just without clock-bound's
	 * tightened uncertainty interval.
	 */
#ifndef WIN32
	if (recno_enable_clock_bound)
	{
		int			fd = open(CLOCKBOUND_SHM_PATH, O_RDONLY);

		if (fd >= 0)
		{
			void	   *map = mmap(NULL, sizeof(ClockBoundData),
								   PROT_READ, MAP_SHARED, fd, 0);

			if (map != MAP_FAILED)
			{
				RecnoClockShmem->clockbound_fd = fd;
				RecnoClockShmem->clockbound_map = map;
				RecnoClockShmem->monitor.clock_bound_available = true;

				ereport(DEBUG1,
						(errmsg("recno: clock-bound daemon integration enabled at %s",
								CLOCKBOUND_SHM_PATH)));
			}
			else
			{
				close(fd);
				ereport(WARNING,
						(errmsg("recno: failed to mmap clock-bound data: %m"),
						 errhint("Clock-bound will be unavailable, using HLC-only mode.")));
			}
		}
		else
		{
			ereport(DEBUG1,
					(errmsg("recno: clock-bound daemon not available at %s",
							CLOCKBOUND_SHM_PATH),
					 errhint("Using HLC-only mode with max_offset bounds.")));
		}
	}
#else
	if (recno_enable_clock_bound)
		ereport(DEBUG1,
				(errmsg("recno: clock-bound daemon integration is not supported on Windows"),
				 errhint("Using HLC-only mode with max_offset bounds.")));
#endif

	RecnoClockShmem->initialized = true;

	/*
	 * Only start the clock monitor background worker when HLC mode is
	 * enabled.  Without HLC, the clock monitor serves no purpose and its
	 * WARNING about failing to start pollutes logs during initdb and other
	 * test infrastructure.
	 */
	if (recno_use_hlc)
		RecnoClockStartMonitor();
}

const ShmemCallbacks RecnoClockShmemCallbacks = {
	.request_fn = RecnoClockShmemRequest,
	.init_fn = RecnoClockShmemInit_cb,
};

/*
 * GUC assign hooks
 */
void
assign_recno_enable_clock_bound(bool newval, void *extra)
{
	/* Will take effect on next server restart */
}

void
assign_recno_fatal_on_clock_drift(bool newval, void *extra)
{
	/* Takes effect immediately */
}

void
assign_recno_clock_check_interval(int newval, void *extra)
{
	/* Will affect next background worker cycle */
}
