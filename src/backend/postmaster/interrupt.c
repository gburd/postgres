/*-------------------------------------------------------------------------
 *
 * interrupt.c
 *	  Interrupt handling routines.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/interrupt.c
 *
 *-------------------------------------------------------------------------
 */

#define BACKEND_RUNTIME_NO_INLINE_BUCKET_ACCESSORS
#include "postgres.h"

#include <unistd.h>

#include "access/parallel.h"
#include "commands/async.h"
#include "commands/repack.h"
#include "miscadmin.h"
#include "postmaster/interrupt.h"
#include "replication/logicalworker.h"
#include "replication/slotsync.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "storage/sinval.h"
#include "utils/backend_runtime.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "../utils/init/backend_runtime_internal.h"

static void PgBackendWakeForInterrupt(PgBackend *backend);

PgBackendPendingInterruptState *
PgCurrentPendingInterruptStateRef(void)
{
	return PgCurrentPendingInterrupts();
}

volatile uint32 *
PgCurrentInterruptHoldoffCountRef(void)
{
	return &PgCurrentInterruptHoldoffs()->interrupt_holdoff_count;
}

volatile uint32 *
PgCurrentQueryCancelHoldoffCountRef(void)
{
	return &PgCurrentInterruptHoldoffs()->query_cancel_holdoff_count;
}

volatile uint32 *
PgCurrentCritSectionCountRef(void)
{
	return &PgCurrentInterruptHoldoffs()->crit_section_count;
}

void *
PgCurrentBackendInterruptMaskRef(void)
{
	PgBackend  *backend = CurrentPgBackend;

	if (backend == NULL)
		return NULL;

	return &backend->interrupts.pending_mask;
}

void
PgBackendWakeup(PgBackend *backend)
{
	if (backend == NULL)
		return;

	PgBackendWakeForInterrupt(backend);
}

static uint32
PgBackendAdvanceNotifyGeneration(PgBackend *backend)
{
	uint32		generation;

	generation =
		pg_atomic_add_fetch_u32(&backend->interrupts.notify_generation, 1);
	if (unlikely(generation == 0))
		generation =
			pg_atomic_add_fetch_u32(&backend->interrupts.notify_generation, 1);

	return generation;
}

uint64
PgBackendNotifyInterruptGeneration(PgBackend *backend)
{
	if (backend == NULL)
		return 0;

	return pg_atomic_read_u32(&backend->interrupts.notify_generation);
}

void
SendInterrupt(PgBackend *backend, PgBackendInterruptType interrupt_type)
{
	PgBackendInterruptMask interrupt_mask;
	PgBackendInterruptMask old_mask;
	bool		notify_interrupt;

	if (backend == NULL)
		return;
	if (interrupt_type < 0 || interrupt_type >= PG_BACKEND_INTERRUPT_COUNT)
		return;

	notify_interrupt = interrupt_type == PG_BACKEND_INTERRUPT_NOTIFY;
	if (notify_interrupt)
		(void) PgBackendAdvanceNotifyGeneration(backend);

	interrupt_mask = PG_BACKEND_INTERRUPT_MASK(interrupt_type);
	old_mask = pg_atomic_fetch_or_u32(&backend->interrupts.pending_mask,
									  interrupt_mask);
	if ((old_mask & interrupt_mask) == 0 || notify_interrupt)
	{
#ifdef PG_RUNTIME_ENABLE_WAIT_COMPLETION_PUBLICATION
		if (interrupt_type == PG_BACKEND_INTERRUPT_QUERY_CANCEL)
			PgBackendMarkWaitCompletionInterrupt(backend,
												 PG_WAIT_COMPLETION_INTERRUPT_CANCEL);
		else if (interrupt_type == PG_BACKEND_INTERRUPT_PROC_DIE)
			PgBackendMarkWaitCompletionInterrupt(backend,
												 PG_WAIT_COMPLETION_INTERRUPT_TERMINATE);
#endif

		PgBackendWakeForInterrupt(backend);
	}
}

void
PgBackendRaiseInterrupt(PgBackend *backend,
						PgBackendInterruptType interrupt_type)
{
	SendInterrupt(backend, interrupt_type);
}

static void
PgBackendWakeForInterrupt(PgBackend *backend)
{
	/*
	 * Process mode has one logical backend per address space, so waking the
	 * current backend must still arm the historical fast-path flag used by
	 * signal-era code. Non-current logical backends rely on the mailbox test in
	 * CHECK_FOR_INTERRUPTS() after their carrier wakes.
	 */
	if (backend == CurrentPgBackend)
		InterruptPending = true;

#ifdef USE_XTC_CARRIER
	/*
	 * Level-triggered secondary wake (test-75 fix): if the target backend runs
	 * as an xtc fiber and has a live interrupt-wake eventfd, write it.  Its
	 * read end is added to the target's FeBe wait set on the first read-command
	 * park (PgSessionStagingWaitProtocolRead; it cannot be added at pq_init,
	 * which runs before InitProcess creates the fd), so a fiber parked in the
	 * read-command park wakes deterministically, immune to the
	 * owner_fiber generation staleness that can drop the SetLatch xtc_proc_wake
	 * below.  Level-triggered: readiness persists until the parked fiber drains
	 * it, so there is no lost-wake window between this write and the park.
	 * Safe cross-fiber (an eventfd add is atomic).  A NULL/dead fd (process
	 * fallback, or before InitProcGlobal) is skipped.
	 */
	{
		PGPROC	   *tproc = backend->my_proc;

		if (tproc != NULL && tproc->sem_fiber_backed &&
			tproc->interrupt_wake_fd >= 0)
		{
			uint64		one = 1;
			ssize_t		wrc;

			wrc = write(tproc->interrupt_wake_fd, &one, sizeof(one));
			(void) wrc;			/* eventfd add; only fails at UINT64_MAX-1 */
		}
	}
#endif

	if (backend->interrupt_latch != NULL)
		SetLatch(backend->interrupt_latch);
	else if (backend == CurrentPgBackend && MyLatch != NULL)
		SetLatch(MyLatch);
}

void
PgCurrentBackendRaiseInterrupt(PgBackendInterruptType interrupt_type)
{
	RaiseInterrupt(interrupt_type);
}

void
RaiseInterrupt(PgBackendInterruptType interrupt_type)
{
	SendInterrupt(CurrentPgBackend, interrupt_type);
}

void
PgBackendRaiseProcDieInterrupt(PgBackend *backend, int sender_pid,
							   int sender_uid)
{
	if (backend == NULL)
		return;

	if (backend->interrupts.proc_die_sender_pid == 0)
	{
		backend->interrupts.proc_die_sender_pid = sender_pid;
		backend->interrupts.proc_die_sender_uid = sender_uid;
	}

	SendInterrupt(backend, PG_BACKEND_INTERRUPT_PROC_DIE);
}

void
PgCurrentBackendRaiseProcDieInterrupt(int sender_pid, int sender_uid)
{
	PgBackendRaiseProcDieInterrupt(CurrentPgBackend, sender_pid, sender_uid);
}

PgBackendInterruptMask
PgBackendConsumeInterrupts(PgBackend *backend)
{
	if (backend == NULL)
		return 0;

	return pg_atomic_exchange_u32(&backend->interrupts.pending_mask, 0);
}

bool
PgCurrentBackendHasPendingInterrupts(void)
{
	PgBackend  *backend = CurrentPgBackend;

	if (backend == NULL)
		return ProcSignalBackendInterruptsPending();

	return pg_atomic_read_u32(&backend->interrupts.pending_mask) != 0 ||
		ProcSignalBackendInterruptsPending();
}

void
PgBackendConsumeProcDieSender(PgBackend *backend, int *sender_pid,
							  int *sender_uid)
{
	if (sender_pid != NULL)
		*sender_pid = 0;
	if (sender_uid != NULL)
		*sender_uid = 0;

	if (backend == NULL)
		return;

	if (sender_pid != NULL)
		*sender_pid = backend->interrupts.proc_die_sender_pid;
	if (sender_uid != NULL)
		*sender_uid = backend->interrupts.proc_die_sender_uid;

	backend->interrupts.proc_die_sender_pid = 0;
	backend->interrupts.proc_die_sender_uid = 0;
}

void
PgCurrentBackendApplyInterrupts(void)
{
	PgBackendInterruptMask pending;
	int			proc_signal_sender_pid = 0;
	int			proc_signal_sender_uid = 0;

	pending = PgBackendConsumeInterrupts(CurrentPgBackend);
	pending |= ConsumeBackendInterruptsFromProcSignal(&proc_signal_sender_pid,
													  &proc_signal_sender_uid);
	if (pending == 0)
		return;

	/*
	 * The logical mailbox feeds the legacy per-backend pending flags below.
	 * Arm the legacy dispatcher as well, so callers that consume the mailbox
	 * immediately before CHECK_FOR_INTERRUPTS() still run ProcessInterrupts().
	 */
	InterruptPending = true;

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_QUERY_CANCEL))
		QueryCancelPending = true;

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_PROC_DIE))
	{
		int			sender_pid;
		int			sender_uid;

		ProcDiePending = true;
		PgBackendConsumeProcDieSender(CurrentPgBackend, &sender_pid,
									  &sender_uid);
		if (sender_pid == 0 && proc_signal_sender_pid != 0)
		{
			sender_pid = proc_signal_sender_pid;
			sender_uid = proc_signal_sender_uid;
		}
		if (ProcDieSenderPid == 0)
		{
			ProcDieSenderPid = sender_pid;
			ProcDieSenderUid = sender_uid;
		}
	}

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_CLIENT_CONNECTION_CHECK))
		CheckClientConnectionPending = true;

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_IDLE_IN_TRANSACTION_SESSION_TIMEOUT))
		IdleInTransactionSessionTimeoutPending = true;

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_TRANSACTION_TIMEOUT))
		TransactionTimeoutPending = true;

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_IDLE_SESSION_TIMEOUT))
		IdleSessionTimeoutPending = true;

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_IDLE_STATS_UPDATE_TIMEOUT))
		IdleStatsUpdateTimeoutPending = true;

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_PROC_SIGNAL_BARRIER))
		ProcSignalBarrierPending = true;

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_LOG_MEMORY_CONTEXT))
		LogMemoryContextPending = true;

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_CONFIG_RELOAD))
		ConfigReloadPending = true;

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_SHUTDOWN_REQUEST))
		ShutdownRequestPending = true;

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_CATCHUP))
		catchupInterruptPending = true;

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_NOTIFY))
		notifyInterruptPending = true;

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_PARALLEL_MESSAGE))
		ParallelMessagePending = true;

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_PARALLEL_APPLY_MESSAGE))
		ParallelApplyMessagePending = true;

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_SLOT_SYNC_MESSAGE))
		SlotSyncShutdownPending = true;

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_REPACK_MESSAGE))
		RepackMessagePending = true;

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_WAKEUP_STOP))
		WakeupStopPending = true;

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_AUTOVAC_LAUNCHER))
		AutoVacLauncherPending = true;

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_CHECKPOINTER_SHUTDOWN_XLOG))
		CheckpointerShutdownXLOGPending = true;
}

/*
 * Simple interrupt handler for main loops of background processes.
 */
void
ProcessMainLoopInterrupts(void)
{
	PgCurrentBackendApplyInterrupts();

	if (ProcSignalBarrierPending)
		ProcessProcSignalBarrier();

	if (ProcDiePending)
		proc_exit(1);

	if (ConfigReloadPending)
	{
		ConfigReloadPending = false;

		/*
		 * Thread-backed workers share GUC storage with the postmaster, which
		 * owns parsing and applying config files for the shared address space.
		 * They only need to observe the updated shared values.
		 */
		if (CurrentPgRuntime == NULL ||
			CurrentPgRuntime->kind == PG_RUNTIME_PROCESS)
			ProcessConfigFile(PGC_SIGHUP);
	}

	if (ShutdownRequestPending)
		proc_exit(0);

	/* Perform logging of memory contexts of this process */
	if (LogMemoryContextPending)
		ProcessLogMemoryContextInterrupt();
}

/*
 * Simple signal handler for triggering a configuration reload.
 *
 * Normally, this handler would be used for SIGHUP. The idea is that code
 * which uses it would arrange to check the ConfigReloadPending flag at
 * convenient places inside main loops, or else call ProcessMainLoopInterrupts.
 */
void
SignalHandlerForConfigReload(SIGNAL_ARGS)
{
	RaiseInterrupt(PG_BACKEND_INTERRUPT_CONFIG_RELOAD);
	ConfigReloadPending = true;
	SetLatch(MyLatch);
}

/*
 * Simple signal handler for exiting quickly as if due to a crash.
 *
 * Normally, this would be used for handling SIGQUIT.
 */
void
SignalHandlerForCrashExit(SIGNAL_ARGS)
{
	/*
	 * We DO NOT want to run proc_exit() or atexit() callbacks -- we're here
	 * because shared memory may be corrupted, so we don't want to try to
	 * clean up our transaction.  Just nail the windows shut and get out of
	 * town.  The callbacks wouldn't be safe to run from a signal handler,
	 * anyway.
	 *
	 * Note we do _exit(2) not _exit(0).  This is to force the postmaster into
	 * a system reset cycle if someone sends a manual SIGQUIT to a random
	 * backend.  This is necessary precisely because we don't clean up our
	 * shared memory state.  (The "dead man switch" mechanism in pmsignal.c
	 * should ensure the postmaster sees this as a crash, too, but no harm in
	 * being doubly sure.)
	 */
	_exit(2);
}

/*
 * Simple signal handler for triggering a long-running background process to
 * shut down and exit.
 *
 * Typically, this handler would be used for SIGTERM, but some processes use
 * other signals. In particular, the checkpointer and parallel apply worker
 * exit on SIGUSR2, and the WAL writer exits on either SIGINT or SIGTERM.
 *
 * ShutdownRequestPending should be checked at a convenient place within the
 * main loop, or else the main loop should call ProcessMainLoopInterrupts.
 */
void
SignalHandlerForShutdownRequest(SIGNAL_ARGS)
{
	RaiseInterrupt(PG_BACKEND_INTERRUPT_SHUTDOWN_REQUEST);
	ShutdownRequestPending = true;
	SetLatch(MyLatch);
}
