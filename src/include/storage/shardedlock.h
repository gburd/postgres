/*-------------------------------------------------------------------------
 *
 * shardedlock.h
 *	  Sharded reader/writer lock built on top of LWLock.
 *
 * A ShardedLWLock is a reader/writer lock with the same acquire/release
 * semantics as an LWLock, but whose *shared* (reader) side is sharded across
 * an array of per-shard atomic counters, each on its own cache line.  This
 * removes the single-cache-line contention that a plain LWLock's shared count
 * suffers under many concurrent readers on a large (multi-socket) machine.
 *
 * It is intended only for a few hot, read-mostly locks (notably ProcArrayLock);
 * ordinary locks should keep using LWLock, since each ShardedLWLock costs
 * nshards extra cache lines of shared memory.
 *
 * Design:
 *	- The lock wraps an ordinary LWLock, which still serves as the writer mutex,
 *	  the waiter/wakeup queue, and the "slow path" for readers.
 *	- A reader normally just bumps its own shard counter (its own cache line)
 *	  and checks that no writer holds the underlying LWLock exclusively.  If a
 *	  writer is present it backs out and falls back to LWLockAcquire(LW_SHARED),
 *	  blocking via the existing LWLock machinery.
 *	- A writer takes the underlying LWLock exclusively (which serializes writers,
 *	  makes the exclusive bit visible so new readers back off, and waits out any
 *	  fallback readers) and then drains the per-shard reader counters.
 *
 * When nshards == 0 the lock is "disabled": every operation is a straight
 * pass-through to the underlying LWLock, so behavior is identical to stock.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/shardedlock.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SHARDEDLOCK_H
#define SHARDEDLOCK_H

#include "port/atomics.h"
#include "storage/lwlock.h"

/*
 * A single reader shard, padded to a full cache line so that bumping one
 * backend's shard never invalidates another backend's shard.
 */
typedef union ShardedLWLockCounter
{
	pg_atomic_uint32 count;
	char		pad[PG_CACHE_LINE_SIZE];
} ShardedLWLockCounter;

/*
 * A sharded lock.  'lock' and 'shards' both point into shared memory; the
 * struct itself may live anywhere (it only holds pointers + a count).
 */
typedef struct ShardedLWLock
{
	LWLock	   *lock;			/* underlying LWLock (writer mutex + waiters) */
	int			nshards;		/* number of reader shards; 0 => disabled */
	ShardedLWLockCounter *shards;	/* [nshards], cache-line aligned, or NULL */
} ShardedLWLock;

/*
 * Shared-memory sizing for the per-lock reader-shard array (0 if disabled).
 * Callers add this to their own ShmemSize and pass the carved-out region to
 * ShardedLWLockInit().
 */
extern Size ShardedLWLockShardsSize(int nshards);

/*
 * Initialize a ShardedLWLock.  'lock' is the underlying LWLock (already
 * LWLockInitialize()d, e.g. a predefined MainLWLockArray lock).  'shards' is a
 * cache-line-aligned region of ShardedLWLockShardsSize(nshards) bytes, or NULL
 * when nshards == 0.  The counters are zeroed here.
 */
extern void ShardedLWLockInit(ShardedLWLock *sll, LWLock *lock,
							  ShardedLWLockCounter *shards, int nshards);

extern bool ShardedLWLockAcquire(ShardedLWLock *sll, LWLockMode mode);
extern bool ShardedLWLockConditionalAcquire(ShardedLWLock *sll, LWLockMode mode);
extern void ShardedLWLockRelease(ShardedLWLock *sll);

extern bool ShardedLWLockHeldByMe(ShardedLWLock *sll);
extern bool ShardedLWLockHeldByMeInMode(ShardedLWLock *sll, LWLockMode mode);

/*
 * Release any fast-path (shard) reader holds still recorded for this backend.
 * Called from LWLockReleaseAll() so error/abort cleanup covers sharded holds
 * exactly as it does ordinary LWLock holds.
 */
extern void ShardedLWLockReleaseAll(void);

#endif							/* SHARDEDLOCK_H */
