/*-------------------------------------------------------------------------
 *
 * recno_clock_simple.c
 *	  Simplified clock-bound integration for RECNO (Phase 1)
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/recno/recno_clock_simple.c
 *
 * NOTES
 *	  This is a simplified implementation that focuses on core clock-bound
 *	  reading functionality. The background monitoring worker will be
 *	  added in Phase 2.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <time.h>
#include <unistd.h>

#include "access/recno.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/guc.h"
#include "utils/timestamp.h"

/* Clock-bound shared memory path */
#define CLOCKBOUND_SHM_PATH "/dev/shm/clockbound"

/* Missing constant */
#ifndef USECS_PER_SEC
#define USECS_PER_SEC 1000000L
#endif

/* Clock-bound data structure from daemon */
typedef struct ClockBoundData
{
	struct timespec earliest;	/* Earliest possible time */
	struct timespec latest;		/* Latest possible time */
	uint64		error_bound_ns; /* Error bound in nanoseconds */
	uint32		segment_id;		/* Daemon segment ID */
	uint32		flags;			/* Status flags */
}			ClockBoundData;

/* Simplified shared memory for clock management */
typedef struct RecnoClockShmemData
{
	LWLock		lock;			/* Protects all fields */
	bool		clock_bound_available;	/* Clock-bound daemon accessible */
	int			clockbound_fd;	/* File descriptor for mmap */
	void	   *clockbound_map; /* Mapped clock-bound data */
	uint64		max_observed_error_ms;	/* Maximum observed error bound */
	TimestampTz last_check_time;	/* Last health check */
}			RecnoClockShmemData;

static RecnoClockShmemData * RecnoClockShmem = NULL;

/* GUC variables */
bool		recno_enable_clock_bound = true;

/* External GUC variable from recno_hlc.c */
extern int	recno_max_clock_offset_ms;

/* External HLC functions */
extern HLCTimestamp HLCNow(HLCTimestamp msg_hlc);
extern HLCTimestamp HLCGetGlobal(void);
extern int	HLCCompare(HLCTimestamp a, HLCTimestamp b);

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
 */
void
RecnoClockShmemInit(void)
{
	bool		found;

	RecnoClockShmem = (RecnoClockShmemData *)
		ShmemInitStruct("RECNO Clock Data",
						RecnoClockShmemSize(),
						&found);

	if (!found)
	{
		LWLockInitialize(&RecnoClockShmem->lock, LWTRANCHE_BUFFER_MAPPING);
		RecnoClockShmem->clock_bound_available = false;
		RecnoClockShmem->clockbound_fd = -1;
		RecnoClockShmem->clockbound_map = NULL;
		RecnoClockShmem->max_observed_error_ms = 0;
		RecnoClockShmem->last_check_time = GetCurrentTimestamp();

		/* Try to open clock-bound shared memory */
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
					RecnoClockShmem->clock_bound_available = true;

					ereport(DEBUG1,
							(errmsg("RECNO: clock-bound daemon integration enabled at %s",
									CLOCKBOUND_SHM_PATH)));
				}
				else
				{
					close(fd);
					ereport(DEBUG1,
							(errmsg("RECNO: failed to mmap clock-bound data: %m"),
							 errhint("Using HLC-only mode with max_offset bounds.")));
				}
			}
			else
			{
				ereport(DEBUG1,
						(errmsg("RECNO: clock-bound daemon not available at %s",
								CLOCKBOUND_SHM_PATH),
						 errhint("Using HLC-only mode with max_offset bounds.")));
			}
		}
	}
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
	if (recno_enable_clock_bound && RecnoClockShmem != NULL)
	{
		LWLockAcquire(&RecnoClockShmem->lock, LW_SHARED);

		if (RecnoClockShmem->clock_bound_available &&
			RecnoClockShmem->clockbound_map != NULL)
		{
			/* Copy from mapped memory (atomic read) */
			memcpy(&bounds, RecnoClockShmem->clockbound_map, sizeof(ClockBoundData));

			/* Validate the data */
			if (bounds.segment_id != 0 &&
				bounds.earliest.tv_sec != 0 &&
				bounds.latest.tv_sec != 0 &&
				(bounds.latest.tv_sec > bounds.earliest.tv_sec ||
				 (bounds.latest.tv_sec == bounds.earliest.tv_sec &&
				  bounds.latest.tv_nsec >= bounds.earliest.tv_nsec)))
			{
				/* Convert timespec to microseconds since PG epoch */
				result.earliest_us = (bounds.earliest.tv_sec * USECS_PER_SEC) +
					(bounds.earliest.tv_nsec / 1000);
				result.latest_us = (bounds.latest.tv_sec * USECS_PER_SEC) +
					(bounds.latest.tv_nsec / 1000);
				result.error_bound_ms = bounds.error_bound_ns / 1000000;
				result.bounds_valid = true;
				have_bounds = true;

				/* Track maximum observed error */
				if (result.error_bound_ms > RecnoClockShmem->max_observed_error_ms)
					RecnoClockShmem->max_observed_error_ms = result.error_bound_ms;
			}
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
		HLCTimestamp global_hlc = HLCGetGlobal();

		while (HLCCompare(global_hlc, origin_bounds.hlc) <= 0)
		{
			pg_usleep(1000);	/* 1ms */
			wait_us += 1000;

			if (wait_us > 1000000)	/* 1 second max wait */
			{
				ereport(DEBUG1,
						(errmsg("RECNO: waited 1 second for HLC to advance")));
				break;
			}
			global_hlc = HLCGetGlobal();
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
					(errmsg("RECNO: waited %d ms for clock bound resolution",
							wait_us / 1000)));
			break;
		}
	}
}

/*
 * RecnoClockGetStats -- get clock statistics
 */
void
RecnoClockGetStats(RecnoClockStats * stats)
{
	if (RecnoClockShmem == NULL || stats == NULL)
		return;

	LWLockAcquire(&RecnoClockShmem->lock, LW_SHARED);

	stats->clock_bound_available = RecnoClockShmem->clock_bound_available;
	stats->max_observed_error_ms = RecnoClockShmem->max_observed_error_ms;
	stats->total_skew_warnings = 0; /* Not tracked in simple version */
	stats->total_fatal_checks = 0;	/* Not tracked in simple version */
	stats->last_sync_time = GetCurrentTimestamp();	/* Approximation */
	stats->last_check_time = RecnoClockShmem->last_check_time;

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

	/* Unmap clock-bound shared memory */
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

	RecnoClockShmem->clock_bound_available = false;

	LWLockRelease(&RecnoClockShmem->lock);
}

/*
 * RecnoClockStartMonitor -- stub for now (will add monitoring later)
 */
void
RecnoClockStartMonitor(void)
{
	/* Phase 2: Add background monitoring worker */
}
