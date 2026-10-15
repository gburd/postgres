/*-------------------------------------------------------------------------
 *
 * shardedlock.c
 *	  Sharded reader/writer lock built on top of LWLock.
 *
 * See storage/shardedlock.h for the design.  In short: readers bump their own
 * cache-line-padded shard counter and re-check that no writer holds the
 * underlying LWLock exclusively; writers take the underlying LWLock exclusively
 * (which serializes writers, publishes the exclusive bit, and waits out any
 * fallback readers) and then drain the shard counters.
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

#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/proc.h"
#include "storage/procnumber.h"
#include "storage/shardedlock.h"
#include "storage/spin.h"

/*
 * Backend-local record of fast-path (shard) reader holds.  These are not
 * tracked in lwlock.c's held_lwlocks[] (no LWLock is actually held on the fast
 * path), so we track them here and release them from LWLockReleaseAll() via
 * ShardedLWLockReleaseAll().  Only a handful of ShardedLWLocks exist and they
 * are not acquired recursively, so a small array suffices.
 */
#define MAX_SIMUL_SHARDED_LWLOCKS	8

typedef struct ShardedHeld
{
	ShardedLWLock *sll;
	int			shard;
} ShardedHeld;

static int	num_sharded_held = 0;
static ShardedHeld sharded_held[MAX_SIMUL_SHARDED_LWLOCKS];

/*
 * The shard this backend uses for a given lock.  Stable for the life of the
 * backend (MyProcNumber does not change), so acquire and release agree.
 */
static inline int
my_shard(const ShardedLWLock *sll)
{
	Assert(sll->nshards > 0);
	Assert(MyProcNumber >= 0);
	return (int) (((uint32) MyProcNumber) % (uint32) sll->nshards);
}

static inline void
record_fast_held(ShardedLWLock *sll, int shard)
{
	if (num_sharded_held >= MAX_SIMUL_SHARDED_LWLOCKS)
		elog(ERROR, "too many sharded LWLocks held");
	sharded_held[num_sharded_held].sll = sll;
	sharded_held[num_sharded_held].shard = shard;
	num_sharded_held++;
}

/*
 * If this backend holds 'sll' via the fast path, remove the record and return
 * its shard index; otherwise return -1.
 */
static inline int
forget_fast_held(ShardedLWLock *sll)
{
	for (int i = num_sharded_held - 1; i >= 0; i--)
	{
		if (sharded_held[i].sll == sll)
		{
			int			shard = sharded_held[i].shard;

			num_sharded_held--;
			for (; i < num_sharded_held; i++)
				sharded_held[i] = sharded_held[i + 1];
			return shard;
		}
	}
	return -1;
}

static inline bool
holds_fast(const ShardedLWLock *sll)
{
	for (int i = 0; i < num_sharded_held; i++)
		if (sharded_held[i].sll == sll)
			return true;
	return false;
}

/*
 * Wait until every shard reader count is zero.  The caller already holds the
 * underlying LWLock exclusively, so new fast readers observe the exclusive bit
 * and back off; this only drains readers that incremented their shard before
 * the exclusive bit became visible, which is a short wait.
 */
static void
drain_shards(ShardedLWLock *sll)
{
	for (int i = 0; i < sll->nshards; i++)
	{
		SpinDelayStatus delay;

		if (pg_atomic_read_u32(&sll->shards[i].count) == 0)
			continue;

		init_local_spin_delay(&delay);
		while (pg_atomic_read_u32(&sll->shards[i].count) != 0)
			perform_spin_delay(&delay);
		finish_spin_delay(&delay);
	}
}

/*
 * Try to acquire shared access via the fast path.  Returns true on success
 * (and records the hold); false if a writer is present (nothing is held).
 */
static bool
shared_acquire_fast(ShardedLWLock *sll)
{
	int			shard = my_shard(sll);

	/*
	 * Announce ourselves in our shard, then check for a writer.  The fetch_add
	 * is a full barrier, and the writer publishes the exclusive bit under a
	 * full barrier (the LWLock CAS) before scanning shards, so at least one of
	 * us sees the other: either the writer waits for our shard, or we see the
	 * exclusive bit and back off.
	 */
	pg_atomic_fetch_add_u32(&sll->shards[shard].count, 1);

	if (!LWLockExclusiveHeld(sll->lock))
	{
		record_fast_held(sll, shard);
		return true;
	}

	/* A writer is present; back out and let the caller take the slow path. */
	pg_atomic_fetch_sub_u32(&sll->shards[shard].count, 1);
	return false;
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
 * 'lock' is the underlying LWLock (already LWLockInitialize()d) that serves as
 * the writer mutex and the reader slow path.  'shards' points at a region of
 * ShardedLWLockShardsSize(nshards) bytes in shared memory, or NULL when
 * nshards <= 0.  The shard array is cache-line aligned and its counters are
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
		pg_atomic_init_u32(&shards[i].count, 0);
}

/*
 * ShardedLWLockAcquire - acquire a sharded lock in the specified mode
 *
 * If the lock is not available, sleep until it is.  Returns true if the lock
 * was available immediately, false if we had to sleep.
 *
 * In LW_SHARED mode the common path only touches this backend's own shard
 * counter (its own cache line) and falls back to LWLockAcquire(LW_SHARED) on
 * the underlying lock when a writer is present.  In LW_EXCLUSIVE mode we take
 * the underlying lock and then wait for the shard readers to drain.
 *
 * Side effect: cancel/die interrupts are held off until lock release.
 */
bool
ShardedLWLockAcquire(ShardedLWLock *sll, LWLockMode mode)
{
	/* Disabled, or no proc number yet: behave exactly like a plain LWLock. */
	if (sll->nshards == 0 || MyProcNumber < 0)
		return LWLockAcquire(sll->lock, mode);

	if (mode == LW_SHARED)
	{
		if (shared_acquire_fast(sll))
			return false;		/* fast path never blocked */

		/* Writer present: block on the underlying LWLock (fallback reader). */
		return LWLockAcquire(sll->lock, LW_SHARED);
	}

	/*
	 * Exclusive: take the underlying LWLock (serializes writers, publishes the
	 * exclusive bit, waits out fallback readers), then drain fast readers.
	 */
	Assert(mode == LW_EXCLUSIVE);
	{
		bool		waited = LWLockAcquire(sll->lock, LW_EXCLUSIVE);

		drain_shards(sll);
		return waited;
	}
}

/*
 * ShardedLWLockConditionalAcquire - acquire a sharded lock, but don't block
 *
 * If the lock is not available, return false with no side-effects.
 *
 * In LW_EXCLUSIVE mode "not available" means another writer or a fallback
 * reader holds the underlying lock; once we take it we still wait out any
 * fast-path shard readers, which is a short, bounded drain rather than a sleep.
 *
 * If successful, cancel/die interrupts are held off until lock release.
 */
bool
ShardedLWLockConditionalAcquire(ShardedLWLock *sll, LWLockMode mode)
{
	if (sll->nshards == 0 || MyProcNumber < 0)
		return LWLockConditionalAcquire(sll->lock, mode);

	if (mode == LW_SHARED)
	{
		/* No blocking: succeed via fast path or fail if a writer is present. */
		return shared_acquire_fast(sll);
	}

	Assert(mode == LW_EXCLUSIVE);
	if (!LWLockConditionalAcquire(sll->lock, LW_EXCLUSIVE))
		return false;

	/* Got the lock; drain fast readers (a short, bounded wait). */
	drain_shards(sll);
	return true;
}

/*
 * ShardedLWLockRelease - release a previously acquired sharded lock
 *
 * Releases whichever way this backend acquired the lock: a fast-path shard
 * hold decrements this backend's shard counter; a writer or fallback-reader
 * hold is released on the underlying LWLock.
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

	/* Fast-path reader hold? */
	shard = forget_fast_held(sll);
	if (shard >= 0)
	{
		pg_atomic_fetch_sub_u32(&sll->shards[shard].count, 1);
		return;
	}

	/* Otherwise a writer or fallback reader hold on the underlying LWLock. */
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
	if (sll->nshards != 0 && holds_fast(sll))
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
	if (mode == LW_SHARED && sll->nshards != 0 && holds_fast(sll))
		return true;
	return LWLockHeldByMeInMode(sll->lock, mode);
}

/*
 * ShardedLWLockReleaseAll - release all fast-path shard holds
 *
 * Used to clean up after ereport(ERROR).  Called from LWLockReleaseAll() so
 * sharded reader holds are released alongside ordinary LWLock holds; the
 * underlying-LWLock holds taken by writers and fallback readers are released
 * by LWLockReleaseAll() itself.  Like that function, this must be safe to call
 * before the lock subsystem is initialized, in which case num_sharded_held is
 * 0 and we do nothing.
 */
void
ShardedLWLockReleaseAll(void)
{
	while (num_sharded_held > 0)
	{
		ShardedHeld *h = &sharded_held[--num_sharded_held];

		pg_atomic_fetch_sub_u32(&h->sll->shards[h->shard].count, 1);
	}
}
