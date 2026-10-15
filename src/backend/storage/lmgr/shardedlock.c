/*-------------------------------------------------------------------------
 *
 * shardedlock.c
 *	  Sharded reader/writer lock built on top of LWLock.
 *
 * See storage/shardedlock.h for the design.  In short: a reader increments
 * the count of its own shard and reads the writer flag of the same shard, so
 * it uses only one cache line.  If a writer is present, the reader sleeps on
 * that flag with a futex.  A writer takes the underlying LWLock in exclusive
 * mode, sets the writer flag in each shard, and waits until all reader counts
 * are zero.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/storage/lmgr/shardedlock.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <limits.h>

#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/proc.h"
#include "storage/procnumber.h"
#include "storage/shardedlock.h"
#include "storage/spin.h"

#if defined(__linux__)
#define SHARDEDLOCK_HAS_FUTEX
#include <linux/futex.h>
#include <sys/syscall.h>
#include <unistd.h>
#endif

/* Bits in the wflag word of a shard. */
#define SHARDED_WRITER		((uint32) 1 << 0)	/* a writer holds the lock */
#define SHARDED_WAITERS		((uint32) 1 << 1)	/* a reader sleeps on wflag */

/*
 * Backend-local record of the sharded locks that this backend holds.  A
 * reader hold records its shard.  A writer hold records WRITER_HOLD.
 * lwlock.c does not track these holds, so LWLockReleaseAll() calls
 * ShardedLWLockReleaseAll() to release them after an error.  Only a few
 * ShardedLWLocks exist, and they are not acquired recursively, so a small
 * array is sufficient.
 */
#define MAX_SIMUL_SHARDED_LWLOCKS	8
#define WRITER_HOLD					(-1)

typedef struct ShardedHeld
{
	ShardedLWLock *sll;
	int			shard;			/* reader shard, or WRITER_HOLD */
} ShardedHeld;

static int	num_sharded_held = 0;
static ShardedHeld sharded_held[MAX_SIMUL_SHARDED_LWLOCKS];

#ifdef SHARDEDLOCK_HAS_FUTEX
/*
 * The futex word is in shared memory, and many processes use it.  Thus we do
 * not use FUTEX_PRIVATE_FLAG.
 */
static inline long
sharded_futex_wait(volatile uint32 *uaddr, uint32 val)
{
	return syscall(SYS_futex, uaddr, FUTEX_WAIT, val, NULL, NULL, 0);
}

static inline long
sharded_futex_wake(volatile uint32 *uaddr)
{
	return syscall(SYS_futex, uaddr, FUTEX_WAKE, INT_MAX, NULL, NULL, 0);
}
#endif

/*
 * The shard that this backend uses for a lock.  MyProcNumber does not change
 * during the life of the backend, so acquire and release use the same shard.
 */
static inline int
my_shard(const ShardedLWLock *sll)
{
	Assert(sll->nshards > 0);
	Assert(MyProcNumber >= 0);
	return (int) (((uint32) MyProcNumber) % (uint32) sll->nshards);
}

/*
 * Make sure that there is space to record one more hold.  Call this before
 * the lock changes shared state.  An error after that point would leave a
 * reader count or a writer flag set with no record to release it.
 */
static inline void
check_held_space(void)
{
	if (num_sharded_held >= MAX_SIMUL_SHARDED_LWLOCKS)
		elog(ERROR, "too many sharded LWLocks held");
}

static inline void
record_held(ShardedLWLock *sll, int shard)
{
	Assert(num_sharded_held < MAX_SIMUL_SHARDED_LWLOCKS);
	sharded_held[num_sharded_held].sll = sll;
	sharded_held[num_sharded_held].shard = shard;
	num_sharded_held++;
}

/*
 * If this backend has a record for 'sll', remove it, set *shard, and return
 * true.  Otherwise return false.
 */
static inline bool
forget_held(ShardedLWLock *sll, int *shard)
{
	for (int i = num_sharded_held - 1; i >= 0; i--)
	{
		if (sharded_held[i].sll == sll)
		{
			*shard = sharded_held[i].shard;
			num_sharded_held--;
			for (; i < num_sharded_held; i++)
				sharded_held[i] = sharded_held[i + 1];
			return true;
		}
	}
	return false;
}

static inline bool
holds_as_reader(const ShardedLWLock *sll)
{
	for (int i = 0; i < num_sharded_held; i++)
		if (sharded_held[i].sll == sll && sharded_held[i].shard != WRITER_HOLD)
			return true;
	return false;
}

/*
 * Try to get shared access through our shard.  Returns true if we now hold
 * the lock.  Returns false if a writer is present, and then holds nothing.
 *
 * The reader increments its count and then reads wflag.  The writer sets
 * wflag and then reads each count.  Both use full barriers (the atomic
 * read-modify-write operations).  Thus at least one sees the other: the
 * writer waits for our count, or we see the writer flag and back out.
 */
static bool
shared_try(ShardedLWLock *sll, int shard)
{
	ShardedLWLockCounter *c = &sll->shards[shard];

	pg_atomic_fetch_add_u32(&c->s.count, 1);
	if ((pg_atomic_read_u32(&c->s.wflag) & SHARDED_WRITER) == 0)
		return true;

	pg_atomic_fetch_sub_u32(&c->s.count, 1);
	return false;
}

#ifdef SHARDEDLOCK_HAS_FUTEX
/*
 * Sleep until no writer holds the lock in this shard.  This does not spin.
 *
 * Before it sleeps, the reader sets SHARDED_WAITERS, so that the writer knows
 * to wake this shard.  futex_wait() gets the value that we expect.  If the
 * writer changes wflag before we sleep, futex_wait() returns at once, so no
 * wakeup is lost.  The loop repeats only when wflag changes or a signal
 * arrives.
 */
static void
wait_for_writer(ShardedLWLockCounter *c)
{
	for (;;)
	{
		uint32		w = pg_atomic_read_u32(&c->s.wflag);

		if ((w & SHARDED_WRITER) == 0)
			return;

		if ((w & SHARDED_WAITERS) == 0)
		{
			if (!pg_atomic_compare_exchange_u32(&c->s.wflag, &w,
												w | SHARDED_WAITERS))
				continue;		/* wflag changed; examine it again */
			w |= SHARDED_WAITERS;
		}

		(void) sharded_futex_wait(&c->s.wflag.value, w);
	}
}
#endif

/*
 * Set the writer flag in each shard, then wait until each reader count is
 * zero.  The caller holds the underlying LWLock in exclusive mode.
 *
 * All flags are set before the wait.  Thus a new reader of any shard backs
 * out while we wait for the readers that came before.
 */
static void
writer_enter(ShardedLWLock *sll)
{
	for (int i = 0; i < sll->nshards; i++)
		pg_atomic_fetch_or_u32(&sll->shards[i].s.wflag, SHARDED_WRITER);

	for (int i = 0; i < sll->nshards; i++)
	{
		SpinDelayStatus delay;

		if (pg_atomic_read_u32(&sll->shards[i].s.count) == 0)
			continue;

		init_local_spin_delay(&delay);
		while (pg_atomic_read_u32(&sll->shards[i].s.count) != 0)
			perform_spin_delay(&delay);
		finish_spin_delay(&delay);
	}
}

/*
 * Clear the writer flag in each shard.  Wake the readers of a shard only if
 * one of them sleeps on it.
 */
static void
writer_exit(ShardedLWLock *sll)
{
	for (int i = 0; i < sll->nshards; i++)
	{
		uint32		old = pg_atomic_exchange_u32(&sll->shards[i].s.wflag, 0);

#ifdef SHARDEDLOCK_HAS_FUTEX
		if (old & SHARDED_WAITERS)
			(void) sharded_futex_wake(&sll->shards[i].s.wflag.value);
#else
		(void) old;
#endif
	}
}


/*
 * ShardedLWLockShardsSize - shared memory needed for a lock's reader shards
 *
 * Returns the number of bytes the caller must reserve (and later pass to
 * ShardedLWLockInit) for a lock with the given number of reader shards, or 0
 * when sharding is disabled (nshards <= 0).  Includes one extra shard's worth
 * of slack so the array base can be cache-line aligned.
 */
Size
ShardedLWLockShardsSize(int nshards)
{
	if (nshards <= 0)
		return 0;
	/* Over-allocate by one line so we can cache-line-align the base. */
	return mul_size(sizeof(ShardedLWLockCounter), nshards + 1);
}

/*
 * ShardedLWLockInit - initialize a sharded lock
 *
 * 'lock' is the underlying LWLock (already LWLockInitialize()d).  It
 * serializes writers.  'shards' points at a region of
 * ShardedLWLockShardsSize(nshards) bytes in shared memory, or NULL when
 * nshards <= 0.  The shard array is cache-line aligned and its words are
 * zeroed here.  With nshards == 0 the lock is disabled: every operation is a
 * straight pass-through to the underlying LWLock, so behavior matches a plain
 * LWLock exactly.
 */
void
ShardedLWLockInit(ShardedLWLock *sll, LWLock *lock,
				  ShardedLWLockCounter *shards, int nshards)
{
	sll->lock = lock;

	if (nshards <= 0)
	{
		sll->nshards = 0;
		sll->shards = NULL;
		return;
	}

	/* Cache-line-align the shard array base. */
	shards = (ShardedLWLockCounter *)
		TYPEALIGN(PG_CACHE_LINE_SIZE, (uintptr_t) shards);

	sll->nshards = nshards;
	sll->shards = shards;
	for (int i = 0; i < nshards; i++)
	{
		pg_atomic_init_u32(&shards[i].s.count, 0);
		pg_atomic_init_u32(&shards[i].s.wflag, 0);
	}
}

/*
 * ShardedLWLockAcquire - acquire a sharded lock in the specified mode
 *
 * If the lock is not available, sleep until it is.  Returns true if the lock
 * was available immediately, false if we had to sleep.  This is the same
 * convention as LWLockAcquire().
 *
 * Side effect: cancel/die interrupts are held off until lock release, as for
 * an LWLock.
 */
bool
ShardedLWLockAcquire(ShardedLWLock *sll, LWLockMode mode)
{
	/* Disabled, or no proc number yet: behave exactly like a plain LWLock. */
	if (sll->nshards == 0 || MyProcNumber < 0)
		return LWLockAcquire(sll->lock, mode);

	check_held_space();

	if (mode == LW_SHARED)
	{
		int			shard = my_shard(sll);
		bool		immediate = true;

		for (;;)
		{
			if (shared_try(sll, shard))
			{
				HOLD_INTERRUPTS();
				record_held(sll, shard);
				return immediate;
			}
			immediate = false;

#ifdef SHARDEDLOCK_HAS_FUTEX
			wait_for_writer(&sll->shards[shard]);
#else
			/* No futex: wait on the underlying LWLock. */
			(void) LWLockAcquire(sll->lock, LW_SHARED);
			return false;
#endif
		}
	}

	/*
	 * Exclusive: the underlying LWLock serializes writers.  Then set the
	 * writer flags and wait for the readers.
	 */
	Assert(mode == LW_EXCLUSIVE);
	{
		bool		immediate = LWLockAcquire(sll->lock, LW_EXCLUSIVE);

		record_held(sll, WRITER_HOLD);
		writer_enter(sll);
		return immediate;
	}
}

/*
 * ShardedLWLockConditionalAcquire - acquire a sharded lock, but don't block
 *
 * If the lock is not available, return false with no side-effects.
 *
 * In LW_EXCLUSIVE mode "not available" means another writer or a fallback
 * reader holds the underlying lock.  After we take it, we still wait for the
 * shard readers.  That is a short wait, not a sleep.
 *
 * If successful, cancel/die interrupts are held off until lock release.
 */
bool
ShardedLWLockConditionalAcquire(ShardedLWLock *sll, LWLockMode mode)
{
	if (sll->nshards == 0 || MyProcNumber < 0)
		return LWLockConditionalAcquire(sll->lock, mode);

	check_held_space();

	if (mode == LW_SHARED)
	{
		int			shard = my_shard(sll);

		if (!shared_try(sll, shard))
			return false;
		HOLD_INTERRUPTS();
		record_held(sll, shard);
		return true;
	}

	Assert(mode == LW_EXCLUSIVE);
	if (!LWLockConditionalAcquire(sll->lock, LW_EXCLUSIVE))
		return false;

	record_held(sll, WRITER_HOLD);
	writer_enter(sll);
	return true;
}

/*
 * ShardedLWLockRelease - release a previously acquired sharded lock
 *
 * A reader hold decrements the count of its shard.  A writer hold clears the
 * writer flags, wakes sleeping readers, and releases the underlying LWLock.
 * A fallback reader hold (no futex support) releases the underlying LWLock.
 */
void
ShardedLWLockRelease(ShardedLWLock *sll)
{
	int			shard;

	if (sll->nshards == 0)
	{
		LWLockRelease(sll->lock);
		return;
	}

	if (forget_held(sll, &shard))
	{
		if (shard != WRITER_HOLD)
		{
			pg_atomic_fetch_sub_u32(&sll->shards[shard].s.count, 1);
			RESUME_INTERRUPTS();
			return;
		}
		writer_exit(sll);
	}

	/* A writer hold, or a fallback reader hold, on the underlying LWLock. */
	LWLockRelease(sll->lock);
}

/*
 * ShardedLWLockHeldByMe - test whether my process holds the lock in any mode
 *
 * This is meant as debug support only.
 */
bool
ShardedLWLockHeldByMe(ShardedLWLock *sll)
{
	if (sll->nshards != 0 && holds_as_reader(sll))
		return true;
	return LWLockHeldByMe(sll->lock);
}

/*
 * ShardedLWLockHeldByMeInMode - test whether my process holds the lock in the
 * given mode
 *
 * This is meant as debug support only.
 */
bool
ShardedLWLockHeldByMeInMode(ShardedLWLock *sll, LWLockMode mode)
{
	if (mode == LW_SHARED && sll->nshards != 0 && holds_as_reader(sll))
		return true;
	return LWLockHeldByMeInMode(sll->lock, mode);
}

/*
 * ShardedLWLockReleaseAll - release all sharded lock holds after an error
 *
 * LWLockReleaseAll() calls this first.  A reader hold decrements its shard
 * count.  A writer hold clears the writer flags and wakes sleeping readers.
 * If it did not, those readers would sleep forever.  LWLockReleaseAll() then
 * releases the underlying LWLock of a writer hold.
 *
 * This does not resume interrupts, because LWLockReleaseAll() does not
 * either.  Error recovery resets the interrupt holdoff count.
 *
 * This must be safe to call before the lock subsystem is initialized.  Then
 * num_sharded_held is 0, and this does nothing.
 */
void
ShardedLWLockReleaseAll(void)
{
	while (num_sharded_held > 0)
	{
		ShardedHeld *h = &sharded_held[--num_sharded_held];

		if (h->shard == WRITER_HOLD)
			writer_exit(h->sll);
		else
			pg_atomic_fetch_sub_u32(&h->sll->shards[h->shard].s.count, 1);
	}
}
