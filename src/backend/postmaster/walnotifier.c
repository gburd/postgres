/*-------------------------------------------------------------------------
 *
 * walnotifier.c
 *
 * The WAL notifier is a dedicated auxiliary process that wakes deferred-durable
 * (synchronous_commit=on) committers once their commit records have been made
 * durable by the WAL writer.  Historically the WAL writer performed both jobs:
 * it flushed WAL greedily and then called XLogSyncFlushDrain() to wake the
 * parked committers.  Splitting the wake work into this separate process lets
 * the flush and the wake run on separate cores in parallel, so the WAL writer
 * can stay busy flushing while the notifier drains and wakes waiters.
 *
 * Like the WAL writer, the notifier is a critical auxiliary process: it is
 * started in PM_RUN, an unexpected exit triggers a full crash-recovery cycle,
 * and it is terminated by SIGTERM together with the other auxiliary processes
 * at shutdown.  It performs no WAL writes and no I/O of its own.
 *
 * The notifier is woken via its latch by committers parking in
 * XLogWaitForFlush(); the committers themselves are woken with a futex
 * (PGSemaphoreUnlock), not a latch/signal.
 *
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/walnotifier.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>
#include <unistd.h>

#include "access/xact.h"
#include "access/xlog.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/auxprocess.h"
#include "postmaster/interrupt.h"
#include "postmaster/walnotifier.h"
#include "postmaster/walwriter.h"
#include "storage/aio_subsys.h"
#include "storage/bufmgr.h"
#include "storage/condition_variable.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "storage/smgr.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/wait_event.h"


/*
 * GUC: number of WAL notifier shards (and hence notifier processes).  Fixed at
 * postmaster start (PGC_POSTMASTER).  0 means a single shard; a positive value
 * shards the wake work across that many notifier processes.  Do not read this
 * directly for the effective count -- call GetNotifierShards().
 */
int			notifier_shards = 0;

/* Which shard this notifier owns; claimed once at startup. */
static int	MyNotifierShard = 0;

/*
 * shmem-exit callback: release our shard so a relaunched notifier reclaims it.
 */
static void
WalNotifierReleaseShard(int code, Datum arg)
{
	XLogSyncFlushReleaseShard(MyNotifierShard);
}


/*
 * Main entry point for walnotifier process
 *
 * This is invoked from AuxiliaryProcessMain, which has already created the
 * basic execution environment, but not enabled signals yet.
 */
void
WalNotifierMain(const void *startup_data, size_t startup_data_len)
{
	sigjmp_buf	local_sigjmp_buf;
	MemoryContext walnotifier_context;

	Assert(startup_data_len == 0);

	MyBackendType = B_WAL_NOTIFIER;
	AuxiliaryProcessMainCommon();

	/*
	 * Properly accept or ignore signals the postmaster might send us
	 *
	 * We have no particular use for SIGINT at the moment, but seems
	 * reasonable to treat like SIGTERM.
	 */
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGINT, SignalHandlerForShutdownRequest);
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	/* SIGQUIT handler was already set up by InitPostmasterChild */
	pqsignal(SIGALRM, PG_SIG_IGN);
	pqsignal(SIGPIPE, PG_SIG_IGN);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	pqsignal(SIGUSR2, PG_SIG_IGN); /* not used */

	/*
	 * Reset some signals that are accepted by postmaster but not here
	 */
	pqsignal(SIGCHLD, PG_SIG_DFL);

	/*
	 * Create a memory context that we will do all our work in.  We do this so
	 * that we can reset the context during error recovery and thereby avoid
	 * possible memory leaks.  Formerly this code just ran in
	 * TopMemoryContext, but resetting that would be a really bad idea.
	 */
	walnotifier_context = AllocSetContextCreate(TopMemoryContext,
												"Wal Notifier",
												ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(walnotifier_context);

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
		 * about in walnotifier, but we do have LWLocks, and perhaps buffers?
		 */
		LWLockReleaseAll();
		ConditionVariableCancelSleep();
		pgstat_report_wait_end();
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
		MemoryContextSwitchTo(walnotifier_context);
		FlushErrorState();

		/* Flush any leaked data in the top-level context */
		MemoryContextReset(walnotifier_context);

		/* Now we can allow interrupts again */
		RESUME_INTERRUPTS();

		/*
		 * Sleep at least 1 second after any error.  A write error is likely
		 * to be repeated, and we don't want to be filling the error logs as
		 * fast as we can.
		 */
		pg_usleep(1000000L);
	}

	/* We can now handle ereport(ERROR) */
	PG_exception_stack = &local_sigjmp_buf;

	/*
	 * Unblock signals (they were blocked when the postmaster forked us)
	 */
	sigprocmask(SIG_SETMASK, &UnBlockSig, NULL);

	/*
	 * Claim a distinct shard and advertise our proc number on it, so committers
	 * on that shard (and the WAL writer) can wake us while we're sleeping.  We
	 * claim the lowest free shard and release it on exit (WalNotifierReleaseShard),
	 * so a clean exit + postmaster relaunch reclaims the same low slot rather than
	 * an ever-increasing index.  A crash instead forces a full cluster restart,
	 * which resets all shard state.  Stable shard indices matter for the NUMA
	 * pinning below: the shard must keep equalling its node across restarts.
	 */
	MyNotifierShard = XLogSyncFlushClaimShard(MyProcNumber);
	before_shmem_exit(WalNotifierReleaseShard, 0);

	/*
	 * Loop forever
	 */
	for (;;)
	{
		/* Clear any already-pending wakeups */
		ResetLatch(MyLatch);

		/* Process any signals received recently (SIGHUP reload, shutdown) */
		ProcessMainLoopInterrupts();

		/*
		 * Wake every deferred-durable committer whose commit record the WAL
		 * writer has now made durable.  One drain pass detaches the whole wait
		 * stack, wakes all currently-durable committers, and re-pushes the rest.
		 *
		 * Do NOT loop here on XLogSyncFlushWorkPending(): the re-pushed (not-yet-
		 * durable) waiters keep the stack non-empty, so a loop would busy-spin
		 * until the WAL writer flushes further.  Instead we drain once and sleep;
		 * the WAL writer nudges our latch after it advances the flush LSN, and
		 * committers nudge it when they enqueue, so we re-drain exactly when
		 * there is new progress to act on.
		 */
		XLogSyncFlushDrain(MyNotifierShard);

		/*
		 * Choose the wait timeout from two signals: whether this shard has
		 * parked committers right now, and the server's synchronous_commit
		 * level (for the notifier this is the cluster default, refreshed on
		 * SIGHUP by ProcessMainLoopInterrupts() above).
		 *
		 *  - synchronous_commit=off AND shard empty: nobody parks here, so wait
		 *    with NO timeout.  Nobody sets our latch, so we stay fully dormant --
		 *    zero wakeups, zero CPU -- until a committer parks and latches us.
		 *    The deferred-durable machinery is a true no-op in that state.
		 *
		 *  - otherwise (synchronous_commit>off, or items are parked on the
		 *    shard): keep the WalWriterDelay periodic re-check as a defensive
		 *    backstop, exactly as the original single-notifier design did.  This
		 *    preserves synchronous_commit=on behavior unchanged, and also keeps a
		 *    backstop whenever there are not-yet-durable waiters, even if the
		 *    cluster default is off but a session set synchronous_commit=on.
		 *
		 * Re-decided every iteration, so a runtime synchronous_commit change (or
		 * items arriving/draining on the shard) flips us between the two modes on
		 * the next cycle.  Correctness never depends on the timeout: a parking
		 * committer always latches us (so already-durable arrivals are handled
		 * immediately, without sleeping), and the WAL writer nudges us after it
		 * advances the flush LSN.
		 */
		if (XLogSyncFlushShardPending(MyNotifierShard) ||
			synchronous_commit > SYNCHRONOUS_COMMIT_OFF)
			(void) WaitLatch(MyLatch,
							 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
							 WalWriterDelay,	/* in ms */
							 WAIT_EVENT_WAL_WRITER_MAIN);
		else
			(void) WaitLatch(MyLatch,
							 WL_LATCH_SET | WL_EXIT_ON_PM_DEATH,
							 -1L,	/* no timeout: dormant until latched */
							 WAIT_EVENT_WAL_WRITER_MAIN);
	}
}
