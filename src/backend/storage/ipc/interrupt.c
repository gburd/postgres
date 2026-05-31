/*-------------------------------------------------------------------------
 *
 * interrupt.c
 *	  Inter-process interrupts
 *
 * This is the PG-on-xtc re-derivation of Heikki Linnakangas's "Replace
 * Latches with Interrupts" work, brought forward onto current master.  This
 * file currently provides the interrupt bitmask core: the per-process
 * pending-interrupt word reached through the session_local MyPendingInterrupts
 * pointer, the local/shared switch, and the raise/send entry points.
 *
 * The wait entry points (WaitInterrupt / WaitInterruptOrSocket) and the
 * shared WaitEventSet initializer (InitializeInterruptWaitSet) depend on
 * WaitEventSet learning about the interrupt word as an event source
 * (WL_INTERRUPT); they land in the follow-up commit that modifies
 * waiteventset.c.  Until then nothing in the tree calls them, latch.c is
 * untouched, and behaviour is unchanged.  See
 * docs/threading/INTERRUPTS_REDERIVATION.md.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/storage/ipc/interrupt.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/interrupt.h"
#include "storage/proc.h"
#include "storage/waiteventset.h"

static session_local pg_atomic_uint32 LocalPendingInterrupts;

session_local pg_atomic_uint32 *MyPendingInterrupts;

/*
 * Initialize the interrupt subsystem for this process.  Point
 * MyPendingInterrupts at the process-local word so that RaiseInterrupt() is
 * usable before (and independently of) having a PGPROC.  Must run early in
 * every process's initialization, before any RaiseInterrupt().
 */
void
InitializeInterruptSupport(void)
{
	pg_atomic_init_u32(&LocalPendingInterrupts, 0);
	MyPendingInterrupts = &LocalPendingInterrupts;
}

/*
 * Switch to local interrupts.  Other backends can't send interrupts to this
 * one.  Only RaiseInterrupt() can set them, from inside this process.
 */
void
SwitchToLocalInterrupts(void)
{
	if (MyPendingInterrupts == &LocalPendingInterrupts)
		return;

	MyPendingInterrupts = &LocalPendingInterrupts;

	/*
	 * Make sure that SIGALRM handlers that call RaiseInterrupt() are now
	 * seeing the new MyPendingInterrupts destination.
	 */
	pg_memory_barrier();

	/*
	 * Mix in the interrupts that we have received already in our shared
	 * interrupt vector, while atomically clearing it.  Other backends may
	 * continue to set bits in it after this point, but we've atomically
	 * transferred the existing bits to our local vector so we won't get
	 * duplicated interrupts later if we switch back.
	 */
	pg_atomic_fetch_or_u32(MyPendingInterrupts,
						   pg_atomic_exchange_u32(&MyProc->pendingInterrupts, 0));
}

/*
 * Switch to shared memory interrupts.  Other backends can send interrupts to
 * this one if they know its ProcNumber, and we'll now see any that we missed.
 */
void
SwitchToSharedInterrupts(void)
{
	if (MyPendingInterrupts == &MyProc->pendingInterrupts)
		return;

	MyPendingInterrupts = &MyProc->pendingInterrupts;

	/*
	 * Make sure that SIGALRM handlers that call RaiseInterrupt() are now
	 * seeing the new MyPendingInterrupts destination.
	 */
	pg_memory_barrier();

	/* Mix in any unhandled bits from LocalPendingInterrupts. */
	pg_atomic_fetch_or_u32(MyPendingInterrupts,
						   pg_atomic_exchange_u32(&LocalPendingInterrupts, 0));
}

/*
 * Set an interrupt flag in this backend.
 */
void
RaiseInterrupt(uint32 interruptMask)
{
	uint32		old_pending;

	old_pending = pg_atomic_fetch_or_u32(MyPendingInterrupts, interruptMask);

	/*
	 * If the process is currently blocked waiting for an interrupt to arrive,
	 * and the interrupt wasn't already pending, wake it up.
	 */
	if ((old_pending & (interruptMask | SLEEPING_ON_INTERRUPTS)) == SLEEPING_ON_INTERRUPTS)
		WakeupMyProc();
}

/*
 * Set an interrupt flag in another backend.
 *
 * Note: This can also be called from the postmaster, so be careful to not
 * trust the contents of shared memory.
 */
void
SendInterrupt(uint32 interruptMask, ProcNumber pgprocno)
{
	PGPROC	   *proc;
	uint32		old_pending;

	Assert(pgprocno != INVALID_PROC_NUMBER);
	Assert(pgprocno >= 0);
	Assert(pgprocno < ProcGlobal->allProcCount);

	proc = &ProcGlobal->allProcs[pgprocno];
	old_pending = pg_atomic_fetch_or_u32(&proc->pendingInterrupts, interruptMask);

	/*
	 * If the process is currently blocked waiting for an interrupt to arrive,
	 * and the interrupt wasn't already pending, wake it up.
	 *
	 * Master's WakeupOtherProc() takes a pid (kill(pid, SIGURG)), so unlike
	 * Heikki's PGPROC*-based call we pass proc->pid.
	 */
	if ((old_pending & (interruptMask | SLEEPING_ON_INTERRUPTS)) == SLEEPING_ON_INTERRUPTS)
		WakeupOtherProc(proc->pid);
}
