/*-------------------------------------------------------------------------
 *
 * bgwriter.c
 *
 * The background writer (bgwriter) is new as of Postgres 8.0.  It attempts
 * to keep regular backends from having to write out dirty shared buffers
 * (which they would only do when needing to free a shared buffer to read in
 * another page).  In the best scenario all writes from shared buffers will
 * be issued by the background writer process.  However, regular backends are
 * still empowered to issue writes if the bgwriter fails to maintain enough
 * clean shared buffers.
 *
 * As of Postgres 9.2 the bgwriter no longer handles checkpoints.
 *
 * Normal termination is by SIGTERM, which instructs the bgwriter to exit(0).
 * Emergency termination is by SIGQUIT; like any backend, the bgwriter will
 * simply abort and exit on SIGQUIT.
 *
 * If the bgwriter exits unexpectedly, the postmaster treats that the same
 * as a backend crash: shared memory may be corrupted, so remaining backends
 * should be killed by SIGQUIT and then a recovery cycle started.
 *
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/bgwriter.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/auxprocess.h"
#include "postmaster/bgwriter.h"
#include "postmaster/interrupt.h"
#include "storage/aio_subsys.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "storage/condition_variable.h"
#include "storage/fd.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "storage/smgr.h"
#include "storage/standby.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/timestamp.h"

/*
 * GUC parameters
 */
int			BgWriterDelay = 200;

/*
 * Multiplier to apply to BgWriterDelay when we decide to hibernate.
 * (Perhaps this needs to be configurable?)
 */
#define HIBERNATE_FACTOR			50

/*
 * Interval in which standby snapshots are logged into the WAL stream, in
 * milliseconds.
 */
#define LOG_SNAPSHOT_INTERVAL_MS 15000

/*
 * LSN and timestamp at which we last issued a LogStandbySnapshot(), to avoid
 * doing so too often or repeatedly if there has been no other write activity
 * in the system.
 */
static TimestampTz last_snapshot_ts;
static XLogRecPtr last_snapshot_lsn = InvalidXLogRecPtr;

/*
 * Collected buffer usage information.
 */
typedef struct BackendBufferStats
{
	int			backend_id;
	uint64		usage_sum;
	double		usage_ratio;
} BackendBufferStats;

static int
compare_backend_usage(const void *a, const void *b)
{
	const BackendBufferStats *stat_a = (const BackendBufferStats *) a;
	const BackendBufferStats *stat_b = (const BackendBufferStats *) b;

	if (stat_a->usage_ratio < stat_b->usage_ratio)
		return -1;
	if (stat_a->usage_ratio > stat_b->usage_ratio)
		return 1;
	return 0;
}

static uint64
CalculateSystemBufferPressure(BackendBufferStats *backend_stats[], int *num_backends)
{
	uint64		total_usage = 0;
	int			active_backends = 0;
	BackendBufferStats *stats;

	/* Count active backends first */
	for (int i = 0; i < ProcGlobal->allProcCount; i++)
	{
		PGPROC	   *proc = &ProcGlobal->allProcs[i];

		if (proc->pid != 0 && proc->databaseId != InvalidOid)
			active_backends++;
	}

	if (active_backends == 0)
	{
		*backend_stats = NULL;
		*num_backends = 0;
		return 0;
	}

	/* Allocate stats array */
	stats = palloc(sizeof(BackendBufferStats) * active_backends);
	*backend_stats = stats;
	*num_backends = active_backends;

	/* Collect stats from all active backends */
	for (int i = 0, j = 0; i < ProcGlobal->allProcCount; i++)
	{
		PGPROC	   *proc = &ProcGlobal->allProcs[i];

		if (proc->pid != 0 && proc->databaseId != InvalidOid)
		{
			uint64		usage_sum = pg_atomic_read_u32(&proc->bufferUsageSum);

			stats[j].backend_id = i;
			stats[j].usage_sum = usage_sum;
			stats[j].usage_ratio = (double) usage_sum / NBuffers;
			total_usage += usage_sum;
			j++;
		}
	}

	/* Sort by usage ratio for percentile calculation */
	qsort(stats, active_backends, sizeof(BackendBufferStats),
		  compare_backend_usage);

	return total_usage;
}

static void
GetHighUsageBackends(BackendBufferStats *stats, int num_backends,
					 int **high_usage_backends, int *num_high_usage)
{
	int			percentile_90_idx = (int) (num_backends * 0.9);

	*num_high_usage = num_backends - percentile_90_idx;

	if (*num_high_usage > 0)
	{
		*high_usage_backends = palloc(sizeof(int) * (*num_high_usage));
		for (int i = 0; i < *num_high_usage; i++)
		{
			(*high_usage_backends)[i] = stats[percentile_90_idx + i].backend_id;
		}
	}
	else
	{
		*high_usage_backends = NULL;
		*num_high_usage = 0;
	}
}

/*
 * Shared buffer sync function used by both main loop and aggressive writing
 */
static int
SyncTargetedBuffers(WritebackContext *wb_context, int *target_backends,
					int num_targets, int max_buffers)
{
	int			buffers_written = 0;
	int			buffer_id;
	BufferDesc *bufHdr;
	uint32		buf_state;

	/* If no specific targets, sync any dirty buffers */
	if (target_backends == NULL || num_targets == 0)
	{
		return BgBufferSync(wb_context);
	}

	/* Scan through all buffers looking for dirty ones from target backends */
	for (buffer_id = 0; buffer_id < NBuffers && buffers_written < max_buffers; buffer_id++)
	{
		uint32		dirty_backend;
		bool		is_target;

		bufHdr = GetBufferDescriptor(buffer_id);

		/* Quick check if buffer is dirty */
		buf_state = pg_atomic_read_u32(&bufHdr->state);
		if (!(buf_state & BM_DIRTY))
			continue;

		/* Check if this buffer is from one of our target backends */
		dirty_backend = pg_atomic_read_u32(&bufHdr->dirty_backend_id);
		is_target = false;

		for (int i = 0; i < num_targets; i++)
			if (dirty_backend == target_backends[i])
			{
				is_target = true;
				break;
			}

		if (!is_target)
			continue;

		/* Skip if buffer is pinned */
		if (BUF_STATE_GET_REFCOUNT(buf_state) > 0)
			continue;

		/* Try to write this buffer using the writeback context */
		ScheduleBufferTagForWriteback(wb_context,
									  IOContextForStrategy(NULL),
									  &bufHdr->tag);
		buffers_written++;
	}

	/* Issue the actual writes */
	if (buffers_written > 0)
	{
		IssuePendingWritebacks(wb_context, IOContextForStrategy(NULL));
	}

	return buffers_written;
}

static void
AggressiveBufferWrite(WritebackContext *wb_context, int *high_usage_backends,
					  int num_high_usage, bool critical)
{
	int			write_target = critical ? bgwriter_lru_maxpages * 3 : bgwriter_lru_maxpages * 2;
	int			buffers_written = 0;

	/* Focus on buffers from high-usage backends first */
	buffers_written = SyncTargetedBuffers(wb_context, high_usage_backends,
										  num_high_usage, write_target);

	/* If still under target, write additional dirty buffers */
	if (buffers_written < write_target)
		BgBufferSync(wb_context);
}

/* In src/backend/postmaster/bgwriter.c - Enhanced UpdateBackendDecayRates */
static void
UpdateBackendDecayRates(BackendBufferStats *backend_stats, int num_backends,
						double pressure_ratio, int *high_usage_backends, int num_high_usage)
{
	uint32		base_decay_rate;
	uint64		total_usage = 0;
	uint64		avg_usage;
	int			i,
				j;

	/* Calculate base decay rate from system pressure */
	if (pressure_ratio > 0.90)
		base_decay_rate = 3;	/* Critical pressure - aggressive decay */
	else if (pressure_ratio > 0.75)
		base_decay_rate = 2;	/* High pressure */
	else
		base_decay_rate = 1;	/* Normal decay rate */

	/* Calculate total usage for relative comparisons */
	for (i = 0; i < num_backends; i++)
		total_usage += backend_stats[i].usage_sum;
	avg_usage = num_backends > 0 ? total_usage / num_backends : 0;

	if (base_decay_rate > 1)
		elog(LOG, "Buffer pressure: %.2f%%, base decay rate: %u, avg usage: %lu",
			 pressure_ratio * 100, base_decay_rate, avg_usage);

	/* Update each backend's personalized decay rate */
	for (i = 0; i < ProcGlobal->allProcCount; i++)
	{
		PGPROC	   *proc = &ProcGlobal->allProcs[i];

		/* Only update active user backends */
		if (proc->pid != 0 && proc->databaseId != InvalidOid)
		{
			uint32		backend_usage = pg_atomic_read_u32(&proc->bufferUsageSum);
			uint32		personalized_rate = base_decay_rate;

			/* Find this backend in the stats array */
			BackendBufferStats *backend_stat = NULL;

			for (j = 0; j < num_backends; j++)
			{
				if (backend_stats[j].backend_id == i)
				{
					backend_stat = &backend_stats[j];
					break;
				}
			}

			/*
			 * Calculate personalized decay rate based on usage and
			 * clock-sweep performance
			 */
			if (backend_stat != NULL && avg_usage > 0)
			{
				double		usage_ratio = (double) backend_usage / avg_usage;

				/* Get clock-sweep performance metrics */
				uint32		search_count = pg_atomic_read_u32(&proc->bufferSearchCount);
				uint64		total_distance = pg_atomic_read_u64(&proc->clockSweepDistance);
				uint32		total_passes = pg_atomic_read_u32(&proc->clockSweepPasses);
				uint64		total_time = pg_atomic_read_u64(&proc->clockSweepTimeMicros);

				/* Calculate average search metrics */
				double		avg_distance = search_count > 0 ? (double) total_distance / search_count : 0;
				double		avg_passes = search_count > 0 ? (double) total_passes / search_count : 0;
				double		avg_time = search_count > 0 ? (double) total_time / search_count : 0;

				/* Adjust decay rate based on usage relative to average */
				if (usage_ratio > 2.0)
				{
					/* High usage backends get more aggressive decay */
					personalized_rate = Min(4, base_decay_rate + 2);
				}
				else if (usage_ratio > 1.5)
				{
					personalized_rate = Min(4, base_decay_rate + 1);
				}
				else if (usage_ratio < 0.5)
				{
					/* Low usage backends get less aggressive decay */
					personalized_rate = Max(1, base_decay_rate > 1 ? base_decay_rate - 1 : 1);
				}

				/* Further adjust based on clock-sweep performance */
				if (avg_distance > NBuffers * 0.5)	/* Searching more than
													 * half the buffer pool */
				{
					personalized_rate = Min(4, personalized_rate + 1);
				}
				if (avg_passes > 1.0)	/* Making multiple complete passes */
				{
					personalized_rate = Min(4, personalized_rate + 1);
				}
				if (avg_time > 1000.0)	/* Taking more than 1ms per search */
				{
					personalized_rate = Min(4, personalized_rate + 1);
				}

				elog(LOG, "Backend %d: usage_ratio=%.2f, avg_distance=%.1f, avg_passes=%.2f, "
					 "avg_time=%.1fμs, decay_rate=%u",
					 i, usage_ratio, avg_distance, avg_passes, avg_time, personalized_rate);
			}

			/* Update the backend's decay rate */
			pg_atomic_write_u32(&proc->bufferDecayRate, personalized_rate);
		}
	}
}

/*
 * Main entry point for bgwriter process
 *
 * This is invoked from AuxiliaryProcessMain, which has already created the
 * basic execution environment, but not enabled signals yet.
 */
void
BackgroundWriterMain(const void *startup_data, size_t startup_data_len)
{
	sigjmp_buf	local_sigjmp_buf;
	MemoryContext bgwriter_context;
	bool		prev_hibernate;
	WritebackContext wb_context;

	Assert(startup_data_len == 0);

	MyBackendType = B_BG_WRITER;
	AuxiliaryProcessMainCommon();

	/*
	 * Properly accept or ignore signals that might be sent to us.
	 */
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGINT, SIG_IGN);
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	/* SIGQUIT handler was already set up by InitPostmasterChild */
	pqsignal(SIGALRM, SIG_IGN);
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	pqsignal(SIGUSR2, SIG_IGN);

	/*
	 * Reset some signals that are accepted by postmaster but not here
	 */
	pqsignal(SIGCHLD, SIG_DFL);

	/*
	 * We just started, assume there has been either a shutdown or
	 * end-of-recovery snapshot.
	 */
	last_snapshot_ts = GetCurrentTimestamp();

	/*
	 * Create a memory context that we will do all our work in.  We do this so
	 * that we can reset the context during error recovery and thereby avoid
	 * possible memory leaks.  Formerly this code just ran in
	 * TopMemoryContext, but resetting that would be a really bad idea.
	 */
	bgwriter_context = AllocSetContextCreate(TopMemoryContext,
											 "Background Writer",
											 ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(bgwriter_context);

	WritebackContextInit(&wb_context, &bgwriter_flush_after);

	/*
	 * If an exception is encountered, processing resumes here.
	 *
	 * You might wonder why this isn't coded as an infinite loop around a
	 * PG_TRY construct.  The reason is that this is the bottom of the
	 * exception stack, and so with PG_TRY there would be no exception handler
	 * in force at all during the CATCH part.  By leaving the outermost setjmp
	 * always active, we have at least some chance of recovering from an error
	 * during error recovery.  (If we get into an infinite loop thereby, it
	 * will soon be stopped by overflow of elog.c's internal state stack.)
	 *
	 * Note that we use sigsetjmp(..., 1), so that the prevailing signal mask
	 * (to wit, BlockSig) will be restored when longjmp'ing to here.  Thus,
	 * signals other than SIGQUIT will be blocked until we complete error
	 * recovery.  It might seem that this policy makes the HOLD_INTERRUPTS()
	 * call redundant, but it is not since InterruptPending might be set
	 * already.
	 */
	if (sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		/* Since not using PG_TRY, must reset error stack by hand */
		error_context_stack = NULL;

		/* Prevent interrupts while cleaning up */
		HOLD_INTERRUPTS();

		/* Report the error to the server log */
		EmitErrorReport();

		/*
		 * These operations are really just a minimal subset of
		 * AbortTransaction().  We don't have very many resources to worry
		 * about in bgwriter, but we do have LWLocks, buffers, and temp files.
		 */
		LWLockReleaseAll();
		ConditionVariableCancelSleep();
		pgaio_error_cleanup();
		UnlockBuffers();
		ReleaseAuxProcessResources(false);
		AtEOXact_Buffers(false);
		AtEOXact_SMgr();
		AtEOXact_Files(false);
		AtEOXact_HashTables(false);

		/*
		 * Now return to normal top-level context and clear ErrorContext for
		 * next time.
		 */
		MemoryContextSwitchTo(bgwriter_context);
		FlushErrorState();

		/* Flush any leaked data in the top-level context */
		MemoryContextReset(bgwriter_context);

		/* re-initialize to avoid repeated errors causing problems */
		WritebackContextInit(&wb_context, &bgwriter_flush_after);

		/* Now we can allow interrupts again */
		RESUME_INTERRUPTS();

		/*
		 * Sleep at least 1 second after any error.  A write error is likely
		 * to be repeated, and we don't want to be filling the error logs as
		 * fast as we can.
		 */
		pg_usleep(1000000L);

		/* Report wait end here, when there is no further possibility of wait */
		pgstat_report_wait_end();
	}

	/* We can now handle ereport(ERROR) */
	PG_exception_stack = &local_sigjmp_buf;

	/*
	 * Unblock signals (they were blocked when the postmaster forked us)
	 */
	sigprocmask(SIG_SETMASK, &UnBlockSig, NULL);

	/*
	 * Reset hibernation state after any error.
	 */
	prev_hibernate = false;

	/*
	 * Loop forever
	 */
	for (;;)
	{
		BackendBufferStats *backend_stats = NULL;
		int			num_backends;
		int		   *high_usage_backends = NULL;
		int			num_high_usage;
		uint64		max_possible;
		uint64		total_usage;
		double		pressure_ratio;
		bool		high_pressure;
		bool		critical_pressure;
		bool		can_hibernate;
		int			rc;

		/* Clear any already-pending wakeups */
		ResetLatch(MyLatch);

		ProcessMainLoopInterrupts();

		/* Calculate current buffer pressure */
		total_usage = CalculateSystemBufferPressure(&backend_stats, &num_backends);
		max_possible = (uint64) NBuffers * BM_MAX_USAGE_COUNT;
		total_usage = total_usage > max_possible ? max_possible : total_usage;
		pressure_ratio = (double) total_usage / max_possible;

		/* Get high-usage backends (90th percentile) */
		if (backend_stats != NULL)
			GetHighUsageBackends(backend_stats, num_backends,
								 &high_usage_backends, &num_high_usage);

		/* Update global decay rate based on current pressure */
		UpdateBackendDecayRates(backend_stats, num_backends, pressure_ratio,
								high_usage_backends, num_high_usage);

		/* Determine if proactive action is needed */
		high_pressure = pressure_ratio > 0.75;	/* 75% threshold */
		critical_pressure = pressure_ratio > 0.90;	/* 90% threshold */

		if (high_pressure)
		{
			elog(LOG, "%s buffer pressure detected: %.2f%% (%d high-usage backends)",
				 critical_pressure ? "Critical" : "High",
				 pressure_ratio * 100, num_high_usage);

			/* Aggressive writing of dirty buffers */
			AggressiveBufferWrite(&wb_context, high_usage_backends, num_high_usage, critical_pressure);
		}

		/*
		 * Do one cycle of dirty-buffer writing.
		 */
		can_hibernate = BgBufferSync(&wb_context);

		/* Report pending statistics to the cumulative stats system */
		pgstat_report_bgwriter();
		pgstat_report_wal(true);

		if (FirstCallSinceLastCheckpoint())
		{
			/*
			 * After any checkpoint, free all smgr objects.  Otherwise we
			 * would never do so for dropped relations, as the bgwriter does
			 * not process shared invalidation messages or call
			 * AtEOXact_SMgr().
			 */
			smgrdestroyall();
		}

		/*
		 * Log a new xl_running_xacts every now and then so replication can
		 * get into a consistent state faster (think of suboverflowed
		 * snapshots) and clean up resources (locks, KnownXids*) more
		 * frequently. The costs of this are relatively low, so doing it 4
		 * times (LOG_SNAPSHOT_INTERVAL_MS) a minute seems fine.
		 *
		 * We assume the interval for writing xl_running_xacts is
		 * significantly bigger than BgWriterDelay, so we don't complicate the
		 * overall timeout handling but just assume we're going to get called
		 * often enough even if hibernation mode is active. It's not that
		 * important that LOG_SNAPSHOT_INTERVAL_MS is met strictly. To make
		 * sure we're not waking the disk up unnecessarily on an idle system
		 * we check whether there has been any WAL inserted since the last
		 * time we've logged a running xacts.
		 *
		 * We do this logging in the bgwriter as it is the only process that
		 * is run regularly and returns to its mainloop all the time. E.g.
		 * Checkpointer, when active, is barely ever in its mainloop and thus
		 * makes it hard to log regularly.
		 */
		if (XLogStandbyInfoActive() && !RecoveryInProgress())
		{
			TimestampTz timeout = 0;
			TimestampTz now = GetCurrentTimestamp();

			timeout = TimestampTzPlusMilliseconds(last_snapshot_ts,
												  LOG_SNAPSHOT_INTERVAL_MS);

			/*
			 * Only log if enough time has passed and interesting records have
			 * been inserted since the last snapshot.  Have to compare with <=
			 * instead of < because GetLastImportantRecPtr() points at the
			 * start of a record, whereas last_snapshot_lsn points just past
			 * the end of the record.
			 */
			if (now >= timeout &&
				last_snapshot_lsn <= GetLastImportantRecPtr())
			{
				last_snapshot_lsn = LogStandbySnapshot();
				last_snapshot_ts = now;
			}
		}

		if (backend_stats != NULL)
			pfree(backend_stats);
		if (high_usage_backends != NULL)
			pfree(high_usage_backends);

		/*
		 * Sleep until we are signaled or BgWriterDelay has elapsed.
		 *
		 * Note: the feedback control loop in BgBufferSync() expects that we
		 * will call it every BgWriterDelay msec.  While it's not critical for
		 * correctness that that be exact, the feedback loop might misbehave
		 * if we stray too far from that.  Hence, avoid loading this process
		 * down with latch events that are likely to happen frequently during
		 * normal operation.
		 */
		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   BgWriterDelay /* ms */ , WAIT_EVENT_BGWRITER_MAIN);

		/*
		 * If no latch event and BgBufferSync says nothing's happening, extend
		 * the sleep in "hibernation" mode, where we sleep for much longer
		 * than bgwriter_delay says.  Fewer wakeups save electricity.  When a
		 * backend starts using buffers again, it will wake us up by setting
		 * our latch.  Because the extra sleep will persist only as long as no
		 * buffer allocations happen, this should not distort the behavior of
		 * BgBufferSync's control loop too badly; essentially, it will think
		 * that the system-wide idle interval didn't exist.
		 *
		 * There is a race condition here, in that a backend might allocate a
		 * buffer between the time BgBufferSync saw the alloc count as zero
		 * and the time we call StrategyNotifyBgWriter.  While it's not
		 * critical that we not hibernate anyway, we try to reduce the odds of
		 * that by only hibernating when BgBufferSync says nothing's happening
		 * for two consecutive cycles.  Also, we mitigate any possible
		 * consequences of a missed wakeup by not hibernating forever.
		 */
		if (rc == WL_TIMEOUT && can_hibernate && prev_hibernate)
		{
			/* Ask for notification at next buffer allocation */
			StrategyNotifyBgWriter(MyProcNumber);
			/* Sleep ... */
			(void) WaitLatch(MyLatch,
							 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
							 BgWriterDelay * HIBERNATE_FACTOR,
							 WAIT_EVENT_BGWRITER_HIBERNATE);
			/* Reset the notification request in case we timed out */
			StrategyNotifyBgWriter(-1);
		}

		prev_hibernate = can_hibernate;
	}
}
