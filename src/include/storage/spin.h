/*-------------------------------------------------------------------------
 *
 * spin.h
 *	   API for spinlocks.
 *
 *
 *	The interface to spinlocks is defined by the typedef "slock_t" and
 *	these functions:
 *
 *	void SpinLockInit(volatile slock_t *lock)
 *		Initialize a spinlock (to the unlocked state).
 *
 *	void SpinLockAcquire(volatile slock_t *lock)
 *		Acquire a spinlock, waiting if necessary.
 *		Time out and abort() if unable to acquire the lock in a
 *		"reasonable" amount of time --- typically ~ 1 minute.
 *
 *	void SpinLockRelease(volatile slock_t *lock)
 *		Unlock a previously acquired lock.
 *
 *	bool SpinLockFree(slock_t *lock)
 *		Tests if the lock is free. Returns true if free, false if locked.
 *		This is not a synchronization primitive; use with caution.
 *
 *	Load and store operations in calling code are guaranteed not to be
 *	reordered with respect to these operations, because they include a
 *	compiler barrier.  (Before PostgreSQL 9.5, callers needed to use a
 *	volatile qualifier to access data protected by spinlocks.)
 *
 *	Keep in mind the coding rule that spinlocks must not be held for more
 *	than a few instructions.  In particular, we assume it is not possible
 *	for a CHECK_FOR_INTERRUPTS() to occur while holding a spinlock, and so
 *	it is not necessary to do HOLD/RESUME_INTERRUPTS() in these functions.
 *
 *	These functions are implemented in terms of hardware-dependent macros
 *	supplied by s_lock.h.  There is not currently any extra functionality
 *	added by this header, but there has been in the past and may someday
 *	be again.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/spin.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SPIN_H
#define SPIN_H

#ifdef USE_STDATOMIC_H

/*
 * Atomics-based spinlocks.  The traditional path is deprecated as of PG19.
 *
 * When stdatomic.h is available, spinlocks are implemented directly on top
 * of the pg_atomic_flag API rather than platform-specific TAS assembly.
 */
#include "port/atomics.h"

typedef pg_atomic_flag slock_t;

/* SpinDelayStatus and helpers shared with the traditional s_lock.h path. */
#include "port/spin_delay_status.h"

extern int s_lock(volatile slock_t *lock, const char *file, int line, const char *func);

static inline void
SpinLockInit(volatile slock_t *lock)
{
	pg_atomic_init_flag(lock);
}

/*
 * SpinLockAcquire - acquire a spinlock, waiting if necessary.
 *
 * This is a macro (not an inline function) so that __FILE__, __LINE__, and
 * __func__ resolve at the call site for "stuck spinlock" diagnostics.
 */
#define SpinLockAcquire(lock) \
	(pg_atomic_test_set_flag(lock) ? (void) 0 : \
	 (void) s_lock((lock), __FILE__, __LINE__, __func__))

static inline void
SpinLockRelease(volatile slock_t *lock)
{
	pg_atomic_clear_flag(lock);
}

static inline bool
SpinLockFree(volatile slock_t *lock)
{
	return pg_atomic_unlocked_test_flag(lock);
}

#else							/* !USE_STDATOMIC_H */

/*
 * Traditional spinlock implementation using platform-specific TAS assembly.
 */
#include "storage/s_lock.h"

static inline void
SpinLockInit(volatile slock_t *lock)
{
	S_INIT_LOCK(lock);
}

#define SpinLockAcquire(lock) S_LOCK(lock)

static inline void
SpinLockRelease(volatile slock_t *lock)
{
	S_UNLOCK(lock);
}

#ifdef S_LOCK_FREE
static inline bool
SpinLockFree(volatile slock_t *lock)
{
	return S_LOCK_FREE(lock);
}
#else
/* Fallback when platform doesn't provide S_LOCK_FREE: always report busy */
static inline bool
SpinLockFree(volatile slock_t *lock)
{
	return false;
}
#endif

#endif							/* USE_STDATOMIC_H */

#endif							/* SPIN_H */
