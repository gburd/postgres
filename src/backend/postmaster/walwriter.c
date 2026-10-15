/*-------------------------------------------------------------------------
 *
 * walwriter.c
 *
 * The WAL writer background process is new as of Postgres 8.3.  It attempts
 * to keep regular backends from having to write out (and fsync) WAL pages.
 * Also, it guarantees that transaction commit records that weren't synced
 * to disk immediately upon commit (ie, were "asynchronously committed")
 * will reach disk within a knowable time --- which, as it happens, is at
 * most three times the wal_writer_delay cycle time.
 *
 * Note that as with the bgwriter for shared buffers, regular backends are
 * still empowered to issue WAL writes and fsyncs when the walwriter doesn't
 * keep up. This means that the WALWriter is not an essential process and
 * can shutdown quickly when requested.
 *
 * Because the walwriter's cycle is directly linked to the maximum delay
 * before async-commit transactions are guaranteed committed, it's probably
 * unwise to load additional functionality onto it.  For instance, if you've
 * got a yen to create xlog segments further in advance, that'd be better done
 * in bgwriter than in walwriter.
 *
 * The walwriter is started by the postmaster as soon as the startup subprocess
 * finishes.  It remains alive until the postmaster commands it to terminate.
 * Normal termination is by SIGTERM, which instructs the walwriter to exit(0).
 * Emergency termination is by SIGQUIT; like any backend, the walwriter will
 * simply abort and exit on SIGQUIT.
 *
 * If the walwriter exits unexpectedly, the postmaster treats that the same
 * as a backend crash: shared memory may be corrupted, so remaining backends
 * should be killed by SIGQUIT and then a recovery cycle started.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/walwriter.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>
#include <unistd.h>

#include "access/xlog.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/auxprocess.h"
#include "postmaster/interrupt.h"
#include "postmaster/walwriter.h"
#include "storage/aio_subsys.h"
#include "storage/bufmgr.h"
#include "storage/condition_variable.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/procsignal.h"
#include "storage/smgr.h"
#include "utils/hsearch.h"
#include "utils/injection_point.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/wait_event.h"


/*
 * GUC parameters
 */
int			WalWriterDelay = 200;
int			WalWriterFlushAfter = DEFAULT_WAL_WRITER_FLUSH_AFTER;

/*
 * Number of do-nothing loops before lengthening the delay time, and the
 * multiplier to apply to WalWriterDelay when we do decide to hibernate.
 * (Perhaps these need to be configurable?)
 */
#define LOOPS_UNTIL_HIBERNATE		50
#define HIBERNATE_FACTOR			25

static void WalWriterFinalDrain(int code, Datum arg);

/*
 * Shutdown callback: flush everything committed to disk and then wake any
 * deferred-durable committers that are now durable.
 *
 * The wake work is normally done by the WAL notifier, but the notifier just
 * exits at shutdown.  A committer parked in XLogWaitForFlush() waits on a
 * no-timeout semaphore, so if we exited without waking it, it would stay
 * parked forever, keeping its backend alive and blocking PM_WAIT_BACKENDS.
 * This callback closes that shutdown-wake dependency by draining here.
 */
static void
WalWriterFinalDrain(int code, Datum arg)
{
	/*
	 * Flush everything committed to disk, then wake the committers it made
	 * durable.  We flush again inside the loop so that a committer which
	 * enqueues a beyond-flushed LSN while we are draining still gets flushed
	 * and woken -- otherwise the drain (which re-pushes not-yet-durable
	 * waiters) could spin.  Backends are terminating, so no new WAL is
	 * generated indefinitely and this converges.
	 */
	do
	{
		while (XLogBackgroundFlush())
			;
		XLogSyncFlushDrain();
	} while (XLogSyncFlushWorkPending());
}

/*
 * Main entry point for walwriter process
 *
 * This is invoked from AuxiliaryProcessMain, which has already created the
 * basic execution environment, but not enabled signals yet.
 */
void
WalWriterMain(const void *startup_data, size_t startup_data_len)
{
	sigjmp_buf	local_sigjmp_buf;
	MemoryContext walwriter_context;
	int			left_till_hibernate;
	bool		hibernating;

	Assert(startup_data_len == 0);

	AuxiliaryProcessMainCommon();

	/*
	 * Properly accept or ignore signals the postmaster might send us
	 */
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGINT, PG_SIG_IGN);	/* no query to cancel */
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	/* SIGQUIT handler was already set up by InitPostmasterChild */
	pqsignal(SIGALRM, PG_SIG_IGN);
	pqsignal(SIGPIPE, PG_SIG_IGN);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	pqsignal(SIGUSR2, PG_SIG_IGN);	/* not used */

	/*
	 * Reset some signals that are accepted by postmaster but not here
	 */
	pqsignal(SIGCHLD, PG_SIG_DFL);

	/*
	 * On exit, flush everything committed and wake any parked committers, so a
	 * committer waiting on a no-timeout semaphore cannot block shutdown.
	 */
	before_shmem_exit(WalWriterFinalDrain, 0);

	/*
	 * Create a memory context that we will do all our work in.  We do this so
	 * that we can reset the context during error recovery and thereby avoid
	 * possible memory leaks.  Formerly this code just ran in
	 * TopMemoryContext, but resetting that would be a really bad idea.
	 */
	walwriter_context = AllocSetContextCreate(TopMemoryContext,
											  "Wal Writer",
											  ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(walwriter_context);

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
		 * about in walwriter, but we do have LWLocks, and perhaps buffers?
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
		MemoryContextSwitchTo(walwriter_context);
		FlushErrorState();

		/* Flush any leaked data in the top-level context */
		MemoryContextReset(walwriter_context);

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
	 * Reset hibernation state after any error.
	 */
	left_till_hibernate = LOOPS_UNTIL_HIBERNATE;
	hibernating = false;
	SetWalWriterSleeping(false);

	/*
	 * Loop forever
	 */
	for (;;)
	{
		long		cur_timeout;

		/*
		 * Advertise whether we might hibernate in this cycle.  We do this
		 * before resetting the latch to ensure that any async commits will
		 * see the flag set if they might possibly need to wake us up, and
		 * that we won't miss any signal they send us.  (If we discover work
		 * to do in the last cycle before we would hibernate, the global flag
		 * will be set unnecessarily, but little harm is done.)  But avoid
		 * touching the global flag if it doesn't need to change.
		 */
		if (hibernating != (left_till_hibernate <= 1))
		{
			hibernating = (left_till_hibernate <= 1);
			SetWalWriterSleeping(hibernating);
		}

		/* Clear any already-pending wakeups */
		ResetLatch(MyLatch);

		/* Process any signals received recently */
		ProcessMainLoopInterrupts();

		INJECTION_POINT("walwriter-before-background-flush", NULL);

		/*
		 * Optional batching for deferred-durable (synchronous_commit=on)
		 * committers.  By default (commit_delay = 0) the WAL writer is fully
		 * greedy: it flushes parked committers' records as fast as the storage
		 * allows.  When commit_delay > 0, pause briefly before flushing so that
		 * more commits coalesce into a single fsync -- the same throughput /
		 * latency trade-off that commit_delay provides on the legacy XLogFlush
		 * group-commit path, now driven from the single flusher.
		 *
		 * We only delay when committers are actually parked
		 * (XLogSyncFlushWorkPending) and, like XLogFlush, when fsync is enabled
		 * and there are at least commit_siblings other active backends -- so we
		 * never add latency when there is little concurrency to batch.
		 */
		if (CommitDelay > 0 && enableFsync &&
			XLogSyncFlushWorkPending() &&
			MinimumActiveBackends(CommitSiblings))
			pg_usleep(CommitDelay);

		/*
		 * Do what we're here for; then, if XLogBackgroundFlush() found useful
		 * work to do, reset hibernation counter.
		 */
		if (XLogBackgroundFlush())
			left_till_hibernate = LOOPS_UNTIL_HIBERNATE;
		else if (left_till_hibernate > 0)
			left_till_hibernate--;

		/* report pending statistics to the cumulative stats system */
		pgstat_report_wal(false);

		/*
		 * Our flush above may have made parked deferred-durable committers
		 * durable.  Waking them is the WAL notifier's job, not ours -- nudge it
		 * so it re-drains against the advanced flush LSN.  This is a single
		 * latch set on one fixed process per flush cycle (not per waiter), so it
		 * does not reintroduce the per-committer signal cost we split out.
		 */
		if (XLogSyncFlushWorkPending())
		{
			ProcNumber	walnotifierProc = pg_atomic_read_u32(&ProcGlobal->walnotifierProc);

			if (walnotifierProc != INVALID_PROC_NUMBER)
				SetLatch(&GetPGProcByNumber(walnotifierProc)->procLatch);
		}

		/*
		 * While a deferred-durable committer is still parked with unflushed WAL,
		 * don't sleep: loop straight back and flush again so its commit reaches
		 * disk as fast as the storage allows (the fsync itself paces the loop).
		 * When no such committer is waiting (e.g. synchronous_commit=off), this
		 * is false and we fall through to the normal lazy schedule below.
		 */
		if (XLogSyncFlushWorkPending())
		{
			/*
			 * This path skips WaitLatch(), so it must make the postmaster-death
			 * check that WL_EXIT_ON_PM_DEATH would, and exit the same way, so
			 * WalWriterFinalDrain() flushes and wakes the parked committers.
			 * Without it, a crash of the postmaster leaves us spinning here
			 * forever: the notifiers have exited, so the parked committers are
			 * never woken.
			 */
			if (!PostmasterIsAlive())
				proc_exit(1);
			continue;
		}

		/*
		 * Sleep until we are signaled or WalWriterDelay has elapsed.  If we
		 * haven't done anything useful for quite some time, lengthen the
		 * sleep time so as to reduce the server's idle power consumption.
		 */
		if (left_till_hibernate > 0)
			cur_timeout = WalWriterDelay;	/* in ms */
		else
			cur_timeout = WalWriterDelay * HIBERNATE_FACTOR;

		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 cur_timeout,
						 WAIT_EVENT_WAL_WRITER_MAIN);
	}
}
