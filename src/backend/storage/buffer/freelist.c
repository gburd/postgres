/*-------------------------------------------------------------------------
 *
 * freelist.c
 *	  routines for managing the buffer pool's replacement strategy.
 *
 * The buffer replacement algorithm is abstracted behind the
 * BufferPoolRoutine vtable (see storage/bufpool.h).  The default
 * implementation is clock-sweep, defined in this file.  Alternative
 * algorithms (ARC, CAR, etc.) can be loaded as extensions.
 *
 * The public entry points (StrategyGetBuffer, StrategyReportAllocs, etc.)
 * dispatch through ActivePoolRoutine, which is set to clock_pool_routine
 * at startup.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/buffer/freelist.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "pgstat.h"
#include "port/atomics.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "storage/bufpool.h"
#include "storage/bufpool_internals.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "storage/subsystems.h"
#include "utils/fmgrprotos.h"
#include "utils/guc.h"

#include <unistd.h>				/* sysconf(_SC_NPROCESSORS_ONLN) */


/*
 * ClockPoolState -- per-pool clock-sweep state.
 *
 * Used by every clock-swept pool, including the DEFAULT pool: the default
 * pool is "just a pool" over [0, NBuffers) whose ClockPoolState lives in the
 * "Buffer Strategy Status" shmem region (StrategyControl), while dynamic pools
 * store theirs in their DSM segment as the pool's strategy_data.  The clock
 * hand sweeps from first_buf_id through first_buf_id + nbuffers - 1.
 *
 * batchSize is the number of consecutive clock-hand values a backend claims
 * per pg_atomic_fetch_add on nextVictimBuffer.  batchSize == 1 is byte-
 * identical to the classic one-at-a-time clock sweep; a larger batch (set on
 * NUMA hardware) cuts cross-socket contention on the shared hand atomic.
 *
 * completePasses is intentionally absent: the pass count is derived from the
 * monotonic hand (hand / nbuffers), and the global background writer that once
 * consumed a stored pass count and an allocation-driven wakeup (bgwprocno) has
 * been retired.  numBufferAllocs survives only to feed
 * pg_stat_bgwriter.buffers_alloc, drained by the checkpointer.
 */
typedef struct ClockPoolState
{
	slock_t		lock;
	pg_atomic_uint32 nextVictimBuffer;	/* monotonically increasing, mod
										 * nbuffers */
	pg_atomic_uint32 numBufferAllocs;
	int			nbuffers;		/* buffer count in this pool */
	int			first_buf_id;	/* starting buffer ID */
	uint32		batchSize;		/* hand values claimed per fetch_add */
} ClockPoolState;

/* Pointers to shared state -- the DEFAULT pool's ClockPoolState */
static ClockPoolState *StrategyControl = NULL;

/*
 * Active buffer pool routine and its strategy data.
 *
 * For the default pool, ActivePoolRoutine points to clock_pool_routine
 * and ActivePoolData points to StrategyControl.  These are set during
 * shared memory initialization.
 */
const BufferPoolRoutine *ActivePoolRoutine = NULL;
void	   *ActivePoolData = NULL;
bool		ActivePoolHasAccessHooks = false;

/*
 * True when the active DEFAULT pool is the built-in clock sweep
 * (clock_pool_routine).  Lets StrategyGetBuffer() devirtualize the
 * overwhelmingly common victim path with a single predicted-true branch,
 * bypassing the vtable indirection.  False for dynamic pools and any
 * extension-provided algorithm -- those keep the indirect vtable path.
 */
static bool ActivePoolIsClock = false;

/*
 * True when the active DEFAULT pool's algorithm declares itself scan-resistant
 * (BufferPoolRoutine.scan_resistant).  Enables LeanStore-style probationary
 * admission: every demand-loaded page is admitted at usage_count 0
 * (COOL/probationary) instead of 1, and earns "hot" status only on a second
 * access (PinBuffer bumps usage_count).  A sequential scan touches each page
 * once, so its pages stay cool and are evicted first -- the algorithm itself
 * provides scan resistance, independent of the BufferAccessStrategy ring.
 * Read by BufferAlloc; false for plain clock and other non-scan-resistant
 * pools, so it is a no-op there.
 */
bool		ActivePoolProbationaryScan = false;

/*
 * GUC variable: name of the replacement algorithm for the DEFAULT pool.
 *
 * Looked up in the algorithm registry during shared-memory initialization.
 * Only "clock" is built in; extensions register additional names from
 * their shared_preload_libraries _PG_init().
 */
char	   *buffer_pool_algorithm = NULL;

/*
 * DEFAULT pool algorithm registry.
 *
 * A small linear-probed array of (name, routine) pairs.  Registrations
 * are append-only and happen from extension _PG_init() hooks before
 * shared memory initialization.  We cap at MAX_DEFAULT_POOL_ALGORITHMS
 * because the number of plausible replacement algorithms is tiny; if
 * that becomes inaccurate in the future, switching to a hash table is
 * straightforward.
 */
#define MAX_DEFAULT_POOL_ALGORITHMS 16

typedef struct DefaultPoolAlgorithmEntry
{
	const char *name;
	const BufferPoolRoutine *routine;
} DefaultPoolAlgorithmEntry;

static DefaultPoolAlgorithmEntry algo_registry[MAX_DEFAULT_POOL_ALGORITHMS] =
{
	{
		BP_ALGO_CLOCK_NAME, &clock_pool_routine
	},
};
static int	algo_registry_len = 1;

static void StrategyCtlShmemRequest(void *arg);
static void StrategyCtlShmemInit(void *arg);

const ShmemCallbacks StrategyCtlShmemCallbacks = {
	.request_fn = StrategyCtlShmemRequest,
	.init_fn = StrategyCtlShmemInit,
};

/*
 * Private (non-shared) state for managing a ring of shared buffers to re-use.
 * This is currently the only kind of BufferAccessStrategy object, but someday
 * we might have more kinds.
 */
typedef struct BufferAccessStrategyData
{
	/* Overall strategy type */
	BufferAccessStrategyType btype;
	/* Number of elements in buffers[] array */
	int			nbuffers;

	/*
	 * Index of the "current" slot in the ring, ie, the one most recently
	 * returned by GetBufferFromRing.
	 */
	int			current;

	/*
	 * When the RECYCLE pool is enabled (recycle_pool_buffers > 0), this
	 * points to the RECYCLE pool descriptor.  Victim selection bypasses the
	 * private ring and routes through the shared RECYCLE pool instead. NULL
	 * when using the traditional per-backend ring buffer approach.
	 */
	struct BufferPoolDesc *recycle_pool;

	/*
	 * Array of buffer numbers.  InvalidBuffer (that is, zero) indicates we
	 * have not yet selected a buffer for this ring slot.  For allocation
	 * simplicity this is palloc'd together with the fixed fields of the
	 * struct.  Unused when recycle_pool is non-NULL.
	 */
	Buffer		buffers[FLEXIBLE_ARRAY_MEMBER];
}			BufferAccessStrategyData;


/* Prototypes for internal functions */
static BufferDesc *GetBufferFromRing(BufferAccessStrategy strategy,
									 uint64 *buf_state);
static void AddBufferToRing(BufferAccessStrategy strategy,
							BufferDesc *buf);

/* Prototypes for clock-sweep vtable implementation */
static BufferDesc *ClockGetVictim(void *strategy_data,
								  BufferAccessStrategy strategy,
								  uint64 *buf_state,
								  bool *from_ring);
static int	ClockSyncStart(void *strategy_data,
						   uint32 *complete_passes,
						   uint32 *num_buf_alloc);
static void ClockNotifyTrickle(void *strategy_data, int bgwprocno);
static bool ClockRejectBuffer(void *strategy_data,
							  BufferAccessStrategy strategy,
							  BufferDesc *buf, bool from_ring);
static Size ClockPoolShmemSize(int nbuffers);
static void ClockPoolShmemInit(void *strategy_data, int nbuffers,
							   int first_buf_id, bool init);


/* ----------------------------------------------------------------
 *			Clock-sweep buffer pool routine (vtable)
 * ----------------------------------------------------------------
 */

/*
 * Shared-memory control region for an extension-provided algorithm chosen as
 * the DEFAULT pool (buffer_pool_algorithm != "clock").  Reserved separately
 * in StrategyCtlShmemRequest() and sized by the algorithm's own shmem_size().
 * NULL when the default pool uses the built-in clock sweep, which stores its
 * state directly in StrategyControl.
 */
static void *DefaultAlgoCtl = NULL;

const BufferPoolRoutine clock_pool_routine = {
	.type = T_Invalid,			/* NodeTag not needed for built-in routine */
	.on_hit = NULL,				/* clock-sweep uses usage_count, not explicit
								 * tracking */
	.on_miss = NULL,			/* clock-sweep doesn't track misses */
	.on_evict = NULL,			/* clock-sweep doesn't track evictions */
	.on_new_tag = NULL,			/* clock-sweep doesn't track insertions */
	.get_victim = ClockGetVictim,
	.sync_start = ClockSyncStart,
	.notify_trickle = ClockNotifyTrickle,
	.trickle_iter_begin = NULL, /* clock-sweep uses linear scan */
	.trickle_iter_next = NULL,
	.trickle_iter_end = NULL,
	.hint_vacuum = NULL,		/* clock-sweep doesn't adjust for VACUUM */
	.reject_buffer = ClockRejectBuffer,
	.prefetch_hint = NULL,		/* clock-sweep doesn't use prefetch hints */
	.shmem_size = ClockPoolShmemSize,
	.shmem_init = ClockPoolShmemInit,
	.shutdown = NULL,
	.scan_resistant = true,		/* probationary cool admission handles scans */
};

/*
 * Clock2BitGetVictim -- examine one candidate buffer for the 2-bit clock sweep.
 *
 * The buffer's cooling state (BufferDesc->state cooling bits) is a 2-bit
 * hot/cooling/cold value.  On a tick:
 *   - refcount != 0            -> in use; caller advances the hand and retries
 *                                 (decrement trycounter; error at 0).
 *   - BM_LOCKED                -> wait for the header, then re-read.
 *   - cooling state > 0 (warm) -> COOL it one level via a CAS decrement and
 *                                 return NULL (caller advances the hand);
 *                                 trycounter is reset (a decrement is progress).
 *   - cooling state == 0 (cold)-> claim it: CAS the refcount to pin, add to the
 *                                 strategy ring if any, and return the victim.
 *
 * The cooling decrement is a CAS (not a blind atomic sub): the clock hand is
 * pool-scoped and may be shared by several backends, so two sweepers can race
 * on the same buffer; CAS makes that race-correct with no risk of underflowing
 * the 2-bit field into the flag bits.
 *
 * Marked pg_attribute_always_inline: called once per tick from the sweep loop,
 * so inlining collapses the per-tick call and keeps the hot state in
 * registers.  Contains no PG/setjmp, so force-inlining is safe.
 */
static pg_attribute_always_inline BufferDesc *
Clock2BitGetVictim(int victim_id, BufferAccessStrategy strategy,
				   uint64 *buf_state, int *trycounter,
				   int reset_budget)
{
	BufferDesc *buf = GetBufferDescriptor(victim_id);
	uint64		old_buf_state = pg_atomic_read_u64(&buf->state);

	/* Unpinnable right now: let the caller advance the hand and retry. */
	if (BUF_STATE_GET_REFCOUNT(old_buf_state) != 0)
	{
		if (--(*trycounter) == 0)
			elog(ERROR, "no unpinned buffers available");
		return NULL;
	}

	/* Header momentarily locked: wait, then fall through to re-read below. */
	if (unlikely(old_buf_state & BM_LOCKED))
		old_buf_state = WaitBufHdrUnlocked(buf);

	/*
	 * CAS loop: cool a warm buffer (cooling state > 0) one level, or claim a
	 * cold one (state == 0).  A pool-scoped hand may be shared, so both the
	 * decrement and the pin use compare-exchange to stay race-correct.
	 */
	for (;;)
	{
		uint64		local_buf_state = old_buf_state;

		if (BUF_STATE_GET_REFCOUNT(local_buf_state) != 0)
		{
			if (--(*trycounter) == 0)
				elog(ERROR, "no unpinned buffers available");
			return NULL;
		}
		if (unlikely(local_buf_state & BM_LOCKED))
		{
			old_buf_state = WaitBufHdrUnlocked(buf);
			continue;
		}
		if (BUF_STATE_GET_HEAT(local_buf_state) != 0)
		{
			/* Warm: cool one level and let the caller advance the hand. */
			local_buf_state -= BUF_HEAT_ONE;
			if (pg_atomic_compare_exchange_u64(&buf->state, &old_buf_state,
											   local_buf_state))
			{
				*trycounter = reset_budget;
				return NULL;
			}
			continue;
		}
		local_buf_state += BUF_REFCOUNT_ONE;
		if (pg_atomic_compare_exchange_u64(&buf->state, &old_buf_state,
										   local_buf_state))
		{
			if (strategy != NULL && strategy->recycle_pool == NULL)
				AddBufferToRing(strategy, buf);
			*buf_state = local_buf_state;
			TrackNewBufferPin(BufferDescriptorGetBuffer(buf));
			return buf;
		}
	}
}

/* ----------------------------------------------------------------
 *			Clock-sweep implementation
 * ----------------------------------------------------------------
 */

/*
 * Clock2BitSweep -- the ONE pool-scoped, batched 2-bit clock sweep.
 *
 * Serves both the DEFAULT pool (a ClockPoolState over [0, NBuffers) stored in
 * StrategyControl) and every dynamic pool (its own ClockPoolState in DSM).
 * There is a single clock hand per pool (pool_state->nextVictimBuffer).  A
 * backend claims pool_state->batchSize consecutive hand values per
 * pg_atomic_fetch_add and then iterates them privately, so the contended hand
 * atomic fires ~1/batch as often -- the multi-socket win.  batchSize == 1
 * (the non-NUMA default) is byte-identical to the classic one-at-a-time sweep.
 *
 * Cooling uses the CAS decrement inside Clock2BitGetVictim (not a blind sub):
 * the hand is pool-scoped and may be shared by several backends, and a batch
 * only bounds -- does not eliminate -- overlap in the wrap window, so CAS is
 * required to keep the 2-bit field from underflowing into the flag bits.
 *
 * The pass count is derived in ClockSyncStart from the monotonic hand
 * (hand / nbuffers), so the hot loop maintains no pass counter and takes no
 * spinlock.  (The global background writer that once used a stored pass count
 * has been retired.)
 *
 * Marked pg_attribute_always_inline so StrategyGetBuffer's devirtualized
 * default-pool call collapses to a tight inlined sweep.  Contains no
 * PG_TRY/setjmp, so force-inlining is safe.
 */
static pg_attribute_always_inline BufferDesc *
Clock2BitSweep(ClockPoolState *pool_state,
			   BufferAccessStrategy strategy,
			   uint64 *buf_state,
			   bool *from_ring)
{
	int			pool_nbuffers = pool_state->nbuffers;
	int			pool_first_buf = pool_state->first_buf_id;
	uint32		batchSize = pool_state->batchSize;
	pg_atomic_uint32 *nextVictimPtr = &pool_state->nextVictimBuffer;
	int			trycounter;

	/*
	 * Per-backend batch of hand values claimed from the pool's clock hand.
	 * MyBatchPos is the next raw hand value to consider; MyBatchEnd is one
	 * past the end of the claimed batch.  These are per-backend (static) and
	 * therefore only ever describe ONE pool at a time in a given backend;
	 * that is fine because a backend sweeps one pool per victim request and
	 * spent batches are simply re-claimed against whichever pool it next
	 * sweeps.  Both are absolute (monotonically increasing) hand values; the
	 * actual buffer id is pool_first_buf + (value mod pool_nbuffers).
	 */
	static uint32 MyBatchPos = 0;
	static uint32 MyBatchEnd = 0;

	*from_ring = false;

	/*
	 * If given a strategy object, see whether it can select a buffer.
	 *
	 * When the RECYCLE pool is enabled, dispatch through it instead of the
	 * per-backend ring buffer.  Otherwise use the per-backend ring, exactly
	 * as the classic clock sweep does -- this preserves the BufferAccessStrategy
	 * ring accounting (pg_stat_io reuses/evictions in the vacuum/bulkread
	 * contexts) that VACUUM and bulk reads depend on.  Scan resistance comes
	 * from probationary admission (InitialUsageCountBits stamps demand-loaded
	 * pages COOL), which is orthogonal to the ring.
	 */
	if (strategy != NULL)
	{
		if (strategy->recycle_pool != NULL &&
			strategy->recycle_pool->bp_active)
		{
			PoolLocalState *local = EnsurePoolAttached(strategy->recycle_pool);
			BufferDesc *buf = strategy->recycle_pool->bp_routine->get_victim(
																			 local->strategy_data, NULL, buf_state, from_ring);

			if (buf != NULL)
			{
				*from_ring = true;
				return buf;
			}
			/* Fall through to main pool if RECYCLE pool failed */
		}
		else
		{
			BufferDesc *buf = GetBufferFromRing(strategy, buf_state);

			if (buf != NULL)
			{
				*from_ring = true;
				return buf;
			}
		}
	}

	/*
	 * Count this allocation for pg_stat_bgwriter.buffers_alloc.  The
	 * checkpointer periodically drains this counter (see
	 * StrategyReportAllocs()).
	 */
	pg_atomic_fetch_add_u32(&pool_state->numBufferAllocs, 1);

	/* Use the "clock sweep" algorithm to find a free buffer */
	trycounter = pool_nbuffers;
	for (;;)
	{
		uint32		handval;
		uint32		victim_off;
		int			victim_id;
		BufferDesc *buf;

		/*
		 * Claim a fresh batch from the pool's hand when the local one is
		 * spent.  For batchSize == 1 this is exactly one fetch_add per tick,
		 * matching the classic sweep.
		 */
		if (MyBatchPos >= MyBatchEnd)
		{
			uint32		start = pg_atomic_fetch_add_u32(nextVictimPtr, batchSize);

			MyBatchPos = start;
			MyBatchEnd = start + batchSize;
		}

		handval = MyBatchPos++;
		victim_off = handval % (uint32) pool_nbuffers;
		victim_id = pool_first_buf + (int) victim_off;

		buf = Clock2BitGetVictim(victim_id, strategy, buf_state,
								 &trycounter, pool_nbuffers);
		if (buf != NULL)
			return buf;
	}

	pg_unreachable();
}

/*
 * ClockGetVictim -- clock-sweep implementation of get_victim
 *
 * Called by StrategyGetBuffer() via the vtable.  Selects the next candidate
 * buffer to use in GetVictimBuffer().  The only hard requirement is that the
 * selected buffer must not currently be pinned by anyone.
 *
 * strategy is a BufferAccessStrategy object, or NULL for default strategy.
 *
 * For the default pool, strategy_data points to StrategyControl (a
 * ClockPoolState over [0, NBuffers)).  For dynamic pools, strategy_data points
 * to a ClockPoolState stored in the pool's DSM segment.  Either way the sweep
 * is the same pool-scoped Clock2BitSweep.
 *
 * The buffer is pinned and marked as owned, using TrackNewBufferPin(),
 * before returning.
 */
static BufferDesc *
ClockGetVictim(void *strategy_data,
			   BufferAccessStrategy strategy,
			   uint64 *buf_state,
			   bool *from_ring)
{
	/*
	 * All clock-swept pools -- the default pool and dynamic pools alike --
	 * store a ClockPoolState as their strategy_data, so a single call handles
	 * both.  Clock2BitSweep is force-inlined; the default pool's hot path is
	 * devirtualized in StrategyGetBuffer and does not reach this vtable slot.
	 */
	return Clock2BitSweep((ClockPoolState *) strategy_data,
						  strategy, buf_state, from_ring);
}

/*
 * ClockSyncStart -- clock-sweep implementation of sync_start
 *
 * Reports the current clock-hand position, a derived pass count, and drains
 * the pool's allocation counter.  The core no longer consumes the position or
 * passes (the global background writer that used them is retired); only the
 * drained allocation count is still used, by the checkpointer, to advance
 * pg_stat_bgwriter.buffers_alloc.  The full signature is kept because it is
 * the sync_start plugin API shared with the contrib algorithms.
 *
 * passes is derived from the monotonic hand (hand / nbuffers), so the sweep
 * hot loop maintains no pass counter.
 */
static int
ClockSyncStart(void *strategy_data, uint32 *complete_passes, uint32 *num_buf_alloc)
{
	ClockPoolState *pool_state = (ClockPoolState *) strategy_data;
	uint32		nbuf = (pool_state->nbuffers > 0) ? (uint32) pool_state->nbuffers : 1;
	uint32		hand;

	SpinLockAcquire(&pool_state->lock);
	hand = pg_atomic_read_u32(&pool_state->nextVictimBuffer);

	if (complete_passes)
		*complete_passes = hand / nbuf;

	if (num_buf_alloc)
		*num_buf_alloc = pg_atomic_exchange_u32(&pool_state->numBufferAllocs, 0);

	SpinLockRelease(&pool_state->lock);

	return pool_state->first_buf_id + (int) (hand % nbuf);
}

/*
 * ClockNotifyTrickle -- clock-sweep implementation of notify_trickle
 *
 * The default pool's trickle writer polls on a timeout, so it needs no
 * allocation-driven wakeup; this is a no-op.  The slot is kept because
 * notify_trickle is part of the plugin API.
 */
static void
ClockNotifyTrickle(void *strategy_data, int bgwprocno)
{
}

/*
 * ClockRejectBuffer -- clock-sweep implementation of reject_buffer
 *
 * Consider rejecting a dirty buffer.  When a nondefault strategy is used,
 * the buffer manager calls this function when the buffer selected by
 * ClockGetVictim needs to be written out and doing so would require flushing
 * WAL too.  This gives us a chance to choose a different victim.
 *
 * Returns true if buffer manager should ask for a new victim, and false
 * if this buffer should be written and re-used.
 */
static bool
ClockRejectBuffer(void *strategy_data, BufferAccessStrategy strategy,
				  BufferDesc *buf, bool from_ring)
{
	/* We only do this in bulkread mode */
	if (strategy->btype != BAS_BULKREAD)
		return false;

	/* Don't muck with behavior of normal buffer-replacement strategy */
	if (!from_ring ||
		strategy->buffers[strategy->current] != BufferDescriptorGetBuffer(buf))
		return false;

	/*
	 * Remove the dirty buffer from the ring; necessary to prevent infinite
	 * loop if all ring members are dirty.
	 */
	strategy->buffers[strategy->current] = InvalidBuffer;

	return true;
}


/*
 * Clock2BitBatchSize -- pick the clock-hand batch size for a pool.
 *
 * Off NUMA: 1 -- byte-identical to the classic one-at-a-time clock sweep.
 *
 * On NUMA: the number of consecutive hand values a backend claims per
 * pg_atomic_fetch_add, sized to cut cross-socket contention on the shared
 * hand atomic (Jim Mlodgenski's design).  We target roughly this pool's
 * buffers divided by the online core count, clamped to [16, pool_nbuffers /
 * ncores] and rounded DOWN to a power of two (so modulo/AND stay cheap and
 * the batch tiles the pool cleanly).  pool_nbuffers is THIS pool's buffer
 * count, not the global total, so a small pool gets a small batch.
 */
static uint32
Clock2BitBatchSize(int pool_nbuffers)
{
	long		ncores;
	uint32		target;
	uint32		batch;

	if (!BufPoolNumaActive() || pool_nbuffers <= 0)
		return 1;

	ncores = sysconf(_SC_NPROCESSORS_ONLN);
	if (ncores < 1)
		ncores = 1;

	/* target ~ pool_nbuffers / ncores, which is also the upper clamp */
	target = (uint32) (pool_nbuffers / ncores);
	if (target < 16)
		target = 16;

	/* do not exceed pool_nbuffers / ncores (unless that is below the floor) */
	{
		uint32		upper = (uint32) (pool_nbuffers / ncores);

		if (upper < 16)
			upper = 16;
		if (target > upper)
			target = upper;
	}

	/* never batch more than the whole pool */
	if (target > (uint32) pool_nbuffers)
		target = (uint32) pool_nbuffers;

	/* round DOWN to a power of two, floor 16 (or pool size if smaller) */
	batch = 1;
	while ((batch << 1) <= target)
		batch <<= 1;
	if (batch < 16)
		batch = (pool_nbuffers >= 16) ? 16 : (uint32) pool_nbuffers;

	return batch;
}

/*
 * ClockPoolShmemSize -- return the shared memory needed for a dynamic
 *		clock-sweep pool with the given number of buffers.
 */
static Size
ClockPoolShmemSize(int nbuffers)
{
	return sizeof(ClockPoolState);
}

/*
 * ClockPoolShmemInit -- initialize per-pool clock-sweep state in DSM.
 *
 * This is called from CreateDynamicBufferPool() after the DSM segment
 * is created and carved up.  first_buf_id is the starting buffer ID
 * for this pool's buffers (i.e. pool->bp_first_buf).
 */
static void
ClockPoolShmemInit(void *strategy_data, int nbuffers,
				   int first_buf_id, bool init)
{
	ClockPoolState *state = (ClockPoolState *) strategy_data;

	if (!init)
		return;					/* re-attach: nothing to do */

	SpinLockInit(&state->lock);
	pg_atomic_init_u32(&state->nextVictimBuffer, 0);
	pg_atomic_init_u32(&state->numBufferAllocs, 0);
	state->nbuffers = nbuffers;
	state->first_buf_id = first_buf_id;
	state->batchSize = Clock2BitBatchSize(nbuffers);
}


/* ----------------------------------------------------------------
 *			Public dispatch functions
 *
 * These maintain the same signatures as before the vtable refactor.
 * They dispatch through ActivePoolRoutine for the default pool.
 * ----------------------------------------------------------------
 */

/*
 * StrategyGetBuffer
 *
 *	Called by the bufmgr to get the next candidate buffer to use in
 *	GetVictimBuffer(). Dispatches to the active pool's get_victim callback.
 */
BufferDesc *
StrategyGetBuffer(BufferAccessStrategy strategy, uint64 *buf_state, bool *from_ring)
{
	/*
	 * Devirtualize the built-in clock sweep (the overwhelmingly common case):
	 * call the inlinable pool-scoped sweep directly on the default pool's
	 * ClockPoolState, bypassing the vtable indirection.  Extensions and
	 * dynamic pools keep the indirect ActivePoolRoutine->get_victim path.
	 */
	if (likely(ActivePoolIsClock))
		return Clock2BitSweep(StrategyControl, strategy, buf_state, from_ring);

	return ActivePoolRoutine->get_victim(ActivePoolData, strategy,
										 buf_state, from_ring);
}

/*
 * StrategyReportAllocs -- return and reset the default pool's buffer
 * allocation count since the last call.
 *
 * Surfaced as pg_stat_bgwriter.buffers_alloc.  The dedicated background writer
 * that once drained this counter has been retired; the checkpointer calls this
 * from its periodic stats reporting instead.  Dispatches to the active pool's
 * sync_start callback (the clock position it also returns is ignored).
 */
uint32
StrategyReportAllocs(void)
{
	uint32		num_buf_alloc = 0;

	(void) ActivePoolRoutine->sync_start(ActivePoolData, NULL, &num_buf_alloc);
	return num_buf_alloc;
}

/*
 * BufPoolClockBatchSize -- clock-sweep batch size for a pool, for statistics.
 *
 * The batch size is the number of consecutive victim-hand values a backend
 * claims per atomic fetch_add (1 = classic one-at-a-time; >1 on NUMA to
 * de-contend the shared counter).  Only the default pool's value is exposed
 * here (it is the one that varies with NUMA); other pools report 1.
 */
int
BufPoolClockBatchSize(int pool_idx)
{
	if (pool_idx == 0 && StrategyControl != NULL)
		return (int) StrategyControl->batchSize;
	return 1;
}

/*
 * StrategyRejectBuffer -- consider rejecting a dirty buffer
 *
 * Dispatches to the active pool's reject_buffer callback.
 */
bool
StrategyRejectBuffer(BufferAccessStrategy strategy, BufferDesc *buf, bool from_ring)
{
	return ActivePoolRoutine->reject_buffer(ActivePoolData, strategy,
											buf, from_ring);
}

/*
 * StrategyHintVacuum -- hint that VACUUM is starting or ending
 *
 * Dispatches to the active pool's hint_vacuum callback, if provided.
 * This allows algorithms like ARC to insert vacuum-loaded pages at the
 * LRU end, preventing cache pollution.
 */
void
StrategyHintVacuum(bool vacuum_active)
{
	if (ActivePoolRoutine->hint_vacuum)
		ActivePoolRoutine->hint_vacuum(ActivePoolData, vacuum_active);
}


/* ----------------------------------------------------------------
 *			Shared memory initialization
 * ----------------------------------------------------------------
 */

/*
 * ResolveDefaultPoolRoutine -- map the buffer_pool_algorithm GUC to a routine.
 *
 * Returns the registered routine for the configured name, or
 * &clock_pool_routine if the name is unknown/unset.  Shared by the shmem
 * request and init callbacks so both agree on which algorithm owns the
 * DEFAULT pool (and therefore how much shared memory to reserve).  Callers
 * that need to warn about an unknown name pass warn_if_unknown = true.
 */
static const BufferPoolRoutine *
ResolveDefaultPoolRoutine(bool warn_if_unknown)
{
	const char *name = buffer_pool_algorithm ? buffer_pool_algorithm
		: BP_ALGO_CLOCK_NAME;
	const BufferPoolRoutine *routine = LookupDefaultPoolAlgorithm(name);

	if (routine == NULL)
	{
		if (warn_if_unknown)
			ereport(WARNING,
					(errmsg("buffer pool algorithm \"%s\" is not registered, using \"%s\"",
							name, BP_ALGO_CLOCK_NAME),
					 errhint("Load the providing extension via shared_preload_libraries.")));
		routine = &clock_pool_routine;
	}
	return routine;
}

/*
 * StrategyCtlShmemRequest -- request shared memory for the buffer
 *		cache replacement strategy.
 */
static void
StrategyCtlShmemRequest(void *arg)
{
	const BufferPoolRoutine *routine = ResolveDefaultPoolRoutine(false);

	ShmemRequestStruct(.name = "Buffer Strategy Status",
					   .size = sizeof(ClockPoolState),
					   .alignment = PG_CACHE_LINE_SIZE,
					   .ptr = (void **) &StrategyControl
		);

	/*
	 * If an extension algorithm is the DEFAULT pool, reserve its own control
	 * region sized by the algorithm.  The built-in clock sweep stores its
	 * state directly in StrategyControl and needs nothing extra here.
	 */
	if (routine != &clock_pool_routine && routine->shmem_size != NULL)
		ShmemRequestStruct(.name = "Default Pool Algorithm Control",
						   .size = routine->shmem_size(NBuffers),
						   .alignment = PG_CACHE_LINE_SIZE,
						   .ptr = (void **) &DefaultAlgoCtl
			);
}

/*
 * StrategyCtlShmemInit -- initialize the buffer cache replacement strategy.
 */
static void
StrategyCtlShmemInit(void *arg)
{
	/*
	 * The DEFAULT pool is "just a pool": a ClockPoolState over [0, NBuffers)
	 * living in the "Buffer Strategy Status" region.  Initialize it exactly
	 * like a dynamic clock pool (this also sets the NUMA auto-batch size).
	 */
	ClockPoolShmemInit(StrategyControl, NBuffers, 0, true);

	/*
	 * Set the active pool routine based on the buffer_pool_algorithm GUC.
	 * Only clock-sweep is built in; extension-provided algorithms register
	 * themselves by name via RegisterDefaultPoolAlgorithm() from a
	 * shared_preload_libraries _PG_init() hook.
	 *
	 * If the configured name is unknown (typo or missing extension), fall
	 * back to clock-sweep and WARNING.  We intentionally do not ERROR out of
	 * shmem init because refusing to start is a worse UX than continuing with
	 * a documented fallback.
	 */
	{
		const BufferPoolRoutine *routine = ResolveDefaultPoolRoutine(true);

		ActivePoolRoutine = routine;
		ActivePoolData = StrategyControl;

		/*
		 * An extension algorithm chosen as the DEFAULT pool keeps its state
		 * in the region reserved for it in StrategyCtlShmemRequest()
		 * (DefaultAlgoCtl), not in StrategyControl.  Point ActivePoolData
		 * there and let the algorithm initialize its own spinlocks/lists via
		 * shmem_init(), exactly like a dynamic pool does.  first_buf_id is 0
		 * for the default pool.
		 */
		if (routine != &clock_pool_routine && routine->shmem_size != NULL)
		{
			Assert(DefaultAlgoCtl != NULL);
			ActivePoolData = DefaultAlgoCtl;
			if (routine->shmem_init)
				routine->shmem_init(ActivePoolData, NBuffers, 0, true);
		}

		/*
		 * If NUMA distribution is active (buffer_pool_numa on + multi-node
		 * hardware) AND the configured algorithm is the built-in clock sweep,
		 * bind the default pool's buffer blocks and descriptors to nodes in
		 * matching contiguous chunks (a buffer and its descriptor on the same
		 * node).  The single Clock2BitSweep is itself NUMA-aware: its per-pool
		 * batchSize was set to a NUMA-sized power of two by ClockPoolShmemInit
		 * above, cutting cross-socket contention on the shared clock hand.
		 * Placement is best-effort; correctness is unaffected if the kernel
		 * ignores the binding.  Extension-provided algorithms are left as-is.
		 */
		if (routine == &clock_pool_routine && BufPoolNumaActive())
		{
			int			nnodes = BufPoolNumaNodes();

			BufPoolNumaDistribute((char *) BufferBlocks,
								  (char *) BufferDescriptors,
								  sizeof(BufferDescPadded),
								  NBuffers);

			elog(LOG, "default buffer pool using batched clock-sweep with NUMA interleaved placement across %d nodes, batch=%u",
				 Min(nnodes, BufPoolNumaNodes()), StrategyControl->batchSize);
		}
		else
		{
			/*
			 * Report the resolved default-pool algorithm.  With no config
			 * this is always the built-in clock-sweep -- matching
			 * upstream/master behavior (clock for every relation, including
			 * TOAST).  Emitting it makes the default observable (and
			 * regression-testable): a zero-config cluster must log
			 * "clock-sweep".
			 */
			elog(LOG, "default buffer pool using %s replacement algorithm",
				 (ActivePoolRoutine == &clock_pool_routine)
				 ? "clock-sweep"
				 : (buffer_pool_algorithm ? buffer_pool_algorithm : "clock-sweep"));
		}

		/*
		 * Cache whether the active algorithm uses any per-access tracking
		 * hooks.  The built-in clock-sweep leaves on_hit/on_miss/on_new_tag
		 * NULL, so the hot BufferAlloc path can skip three vtable pointer
		 * loads per access with a single predicted-false branch on this flag.
		 */
		ActivePoolHasAccessHooks = (routine->on_hit != NULL ||
									routine->on_miss != NULL ||
									routine->on_new_tag != NULL);

		/*
		 * Cache whether the active algorithm is the built-in clock sweep, so
		 * StrategyGetBuffer() can call the pool-scoped sweep directly instead
		 * of through the vtable.  Only the built-in clock_pool_routine with
		 * StrategyControl as its data qualifies: dynamic pools keep the
		 * indirect path.
		 */
		ActivePoolIsClock = (ActivePoolRoutine == &clock_pool_routine &&
							 ActivePoolData == (void *) StrategyControl);

		/*
		 * A scan-resistant algorithm owns scan resistance through its own
		 * admission policy, so enable probationary (cool) admission for it.
		 * Clock2BitSweep declares scan_resistant = true, so this is on for the
		 * default pool.  See ActivePoolProbationaryScan / InitialUsageCountBits.
		 */
		ActivePoolProbationaryScan =
			(ActivePoolRoutine != NULL && ActivePoolRoutine->scan_resistant);
	}
}


/* ----------------------------------------------------------------
 *			SQL-callable handler function
 * ----------------------------------------------------------------
 */

/*
 * clock_pool_handler -- returns the clock-sweep BufferPoolRoutine
 *
 * This follows the access-method handler pattern so that the default
 * pool has a handler like any other pool.
 */
Datum
clock_pool_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&clock_pool_routine);
}

/* ----------------------------------------------------------------
 *			KEEP buffer pool routine (vtable)
 *
 * The KEEP algorithm never evicts buffers.  Pages stay resident
 * until the pool is dropped or explicitly cleared.  This is useful
 * for pinning important data sets in memory.
 *
 * KEEP is a built-in algorithm, not a well-known pool.  Users
 * create pools with the KEEP handler like any other pool:
 *   CREATE BUFFER POOL mykeep SIZE '64MB' HANDLER keep_pool_handler;
 * ----------------------------------------------------------------
 */

/*
 * KeepGetVictim -- KEEP pools cannot evict; scan for free buffers only.
 *
 * Returns a free (uninitialized) buffer if one exists.  If all
 * buffers are in use, raises an error.
 */
static BufferDesc *
KeepGetVictim(void *strategy_data, BufferAccessStrategy strategy,
			  uint64 *buf_state, bool *from_ring)
{
	ClockPoolState *state = (ClockPoolState *) strategy_data;
	int			nbuffers = state->nbuffers;
	int			first_buf_id = state->first_buf_id;

	*from_ring = false;

	/* Scan for an unused buffer (tag not valid, not pinned) */
	for (int i = 0; i < nbuffers; i++)
	{
		BufferDesc *buf = GetBufferDescriptor(first_buf_id + i);
		uint64		old_buf_state;
		uint64		local_buf_state;

		old_buf_state = pg_atomic_read_u64(&buf->state);
		for (;;)
		{
			local_buf_state = old_buf_state;

			/* Only consider buffers that are not pinned and have no valid tag */
			if (BUF_STATE_GET_REFCOUNT(local_buf_state) != 0)
				break;

			if (local_buf_state & BM_TAG_VALID)
				break;			/* occupied -- KEEP never evicts */

			if (unlikely(local_buf_state & BM_LOCKED))
			{
				old_buf_state = WaitBufHdrUnlocked(buf);
				continue;
			}

			/* Pin the buffer */
			local_buf_state += BUF_REFCOUNT_ONE;
			if (pg_atomic_compare_exchange_u64(&buf->state, &old_buf_state,
											   local_buf_state))
			{
				*buf_state = local_buf_state;
				TrackNewBufferPin(BufferDescriptorGetBuffer(buf));
				return buf;
			}
		}
	}

	ereport(ERROR,
			(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
			 errmsg("KEEP buffer pool is full; cannot allocate new buffer"),
			 errhint("Increase the KEEP pool size or drop the pool and recreate it larger.")));
	pg_unreachable();
}

/*
 * KeepShmemSize -- KEEP reuses ClockPoolState for minimal bookkeeping.
 */
static Size
KeepPoolShmemSize(int nbuffers)
{
	return sizeof(ClockPoolState);
}

/*
 * KeepPoolShmemInit -- initialize per-pool KEEP state (reuses ClockPoolState).
 */
static void
KeepPoolShmemInit(void *strategy_data, int nbuffers,
				  int first_buf_id, bool init)
{
	ClockPoolState *state = (ClockPoolState *) strategy_data;

	if (!init)
		return;

	SpinLockInit(&state->lock);
	pg_atomic_init_u32(&state->nextVictimBuffer, 0);
	pg_atomic_init_u32(&state->numBufferAllocs, 0);
	state->nbuffers = nbuffers;
	state->first_buf_id = first_buf_id;
}

/*
 * KeepSyncStart -- return 0 (KEEP pools don't have meaningful scan state).
 */
static int
KeepSyncStart(void *strategy_data, uint32 *complete_passes,
			  uint32 *num_buf_alloc)
{
	if (complete_passes)
		*complete_passes = 0;
	if (num_buf_alloc)
		*num_buf_alloc = 0;
	return 0;
}

/*
 * KeepNotifyTrickle -- no-op for KEEP (never evicts, trickle writer
 * still flushes dirty pages but doesn't invalidate them).
 */
static void
KeepNotifyTrickle(void *strategy_data, int bgwprocno)
{
	/* KEEP pools don't need trickle writer wakeup */
}

/*
 * KeepRejectBuffer -- KEEP doesn't use ring strategies.
 */
static bool
KeepRejectBuffer(void *strategy_data, BufferAccessStrategy strategy,
				 BufferDesc *buf, bool from_ring)
{
	return false;
}

const BufferPoolRoutine keep_pool_routine = {
	.type = T_Invalid,
	.on_hit = NULL,				/* no tracking overhead */
	.on_miss = NULL,
	.on_evict = NULL,			/* should never happen */
	.on_new_tag = NULL,
	.get_victim = KeepGetVictim,
	.sync_start = KeepSyncStart,
	.notify_trickle = KeepNotifyTrickle,
	.trickle_iter_begin = NULL, /* KEEP never evicts */
	.trickle_iter_next = NULL,
	.trickle_iter_end = NULL,
	.hint_vacuum = NULL,
	.reject_buffer = KeepRejectBuffer,
	.prefetch_hint = NULL,
	.shmem_size = KeepPoolShmemSize,
	.shmem_init = KeepPoolShmemInit,
	.shutdown = NULL,
};

/*
 * keep_pool_handler -- SQL-callable handler returning the KEEP BufferPoolRoutine.
 */
Datum
keep_pool_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&keep_pool_routine);
}


/* ----------------------------------------------------------------
 *				RECYCLE pool replacement strategy
 *
 * The RECYCLE pool replaces per-backend ring buffers with a shared pool
 * for bulk reads, bulk writes, and VACUUM operations.  It uses a one-chance
 * clock sweep where usage_count is capped at 1: pages get one chance to
 * survive a sweep cycle, then they're evicted.  This provides aggressive
 * replacement suitable for scan-heavy and VACUUM workloads.
 *
 * Three modes share the same physical buffer pool:
 *   RECYCLE_BULKREAD  -- large sequential scans
 *   RECYCLE_BULKWRITE -- COPY IN, bulk inserts
 *   RECYCLE_VACUUM    -- VACUUM operations
 * ----------------------------------------------------------------
 */

/*
 * RecycleMode -- distinguishes the three usage patterns sharing the
 * RECYCLE pool's physical buffers.
 */
typedef enum RecycleMode
{
	RECYCLE_BULKREAD,			/* large sequential scans */
	RECYCLE_BULKWRITE,			/* COPY IN, bulk inserts */
	RECYCLE_VACUUM,				/* VACUUM operations */
} RecycleMode;

/*
 * RecyclePoolState -- shared state for the RECYCLE pool.
 *
 * Extends ClockPoolState with per-mode statistics.  The eviction algorithm
 * is the same for all modes (one-chance clock), but tracking per-mode
 * activity helps monitor workload distribution.
 */
typedef struct RecyclePoolState
{
	/* Core clock-sweep state (same layout as ClockPoolState) */
	slock_t		lock;
	pg_atomic_uint32 nextVictimBuffer;
	pg_atomic_uint32 numBufferAllocs;
	int			nbuffers;
	int			first_buf_id;

	/* Per-mode allocation counts for monitoring */
	pg_atomic_uint64 bulkread_allocs;
	pg_atomic_uint64 bulkwrite_allocs;
	pg_atomic_uint64 vacuum_allocs;
} RecyclePoolState;

/* Forward declarations */
static BufferDesc *RecycleGetVictim(void *strategy_data,
									BufferAccessStrategy strategy,
									uint64 *buf_state,
									bool *from_ring);
static Size RecycleShmemSize(int nbuffers);
static void RecycleShmemInit(void *strategy_data, int nbuffers,
							 int first_buf_id, bool init);
static int	RecycleSyncStart(void *strategy_data, uint32 *complete_passes,
							 uint32 *num_buf_alloc);
static void RecycleNotifyTrickle(void *strategy_data, int bgwprocno);

/* Trickle iterator for RECYCLE pool */
typedef struct RecycleTrickleIter
{
	int			pos;			/* current offset in pool */
	int			remaining;		/* max candidates left */
} RecycleTrickleIter;

static void *RecycleTrickleIterBegin(void *strategy_data, int max_candidates);
static int	RecycleTrickleIterNext(void *strategy_data, void *iter);
static void RecycleTrickleIterEnd(void *strategy_data, void *iter);

/*
 * RecycleGetVictim -- one-chance clock sweep for the RECYCLE pool.
 *
 * Like ClockGetVictim but with usage_count capped at 1: if a buffer has
 * usage_count >= 1, we set it to 0 in one step (rather than decrementing).
 * This ensures pages loaded by scans don't persist longer than one sweep
 * cycle, preventing cache pollution from bulk operations.
 */
static BufferDesc *
RecycleGetVictim(void *strategy_data,
				 BufferAccessStrategy strategy,
				 uint64 *buf_state,
				 bool *from_ring)
{
	RecyclePoolState *state = (RecyclePoolState *) strategy_data;
	BufferDesc *buf;
	int			trycounter;

	*from_ring = false;

	pg_atomic_fetch_add_u32(&state->numBufferAllocs, 1);

	trycounter = state->nbuffers;
	for (;;)
	{
		uint64		old_buf_state;
		uint64		local_buf_state;
		uint32		victim_raw;
		uint32		victim_id;

		victim_raw = pg_atomic_fetch_add_u32(&state->nextVictimBuffer, 1);
		victim_id = state->first_buf_id + (victim_raw % state->nbuffers);

		buf = GetBufferDescriptor(victim_id);

		old_buf_state = pg_atomic_read_u64(&buf->state);
		for (;;)
		{
			local_buf_state = old_buf_state;

			/* Skip pinned buffers */
			if (BUF_STATE_GET_REFCOUNT(local_buf_state) != 0)
			{
				if (--trycounter == 0)
					elog(ERROR, "no unpinned buffers available in RECYCLE pool");
				break;
			}

			if (unlikely(local_buf_state & BM_LOCKED))
			{
				old_buf_state = WaitBufHdrUnlocked(buf);
				continue;
			}

			if (BUF_STATE_GET_HEAT(local_buf_state) != 0)
			{
				/*
				 * One-chance: clear usage_count entirely instead of
				 * decrementing.  This gives each page exactly one sweep cycle
				 * to be re-accessed before eviction.
				 */
				local_buf_state &= ~BUF_HEAT_MASK;

				if (pg_atomic_compare_exchange_u64(&buf->state,
												   &old_buf_state,
												   local_buf_state))
				{
					trycounter = state->nbuffers;
					break;
				}
			}
			else
			{
				/* Pin the buffer */
				local_buf_state += BUF_REFCOUNT_ONE;

				if (pg_atomic_compare_exchange_u64(&buf->state,
												   &old_buf_state,
												   local_buf_state))
				{
					*buf_state = local_buf_state;
					TrackNewBufferPin(BufferDescriptorGetBuffer(buf));
					return buf;
				}
			}
		}
	}
}

/*
 * RecycleShmemSize -- shared memory size for RECYCLE pool state.
 */
static Size
RecycleShmemSize(int nbuffers)
{
	return sizeof(RecyclePoolState);
}

/*
 * RecycleShmemInit -- initialize RECYCLE pool shared state.
 */
static void
RecycleShmemInit(void *strategy_data, int nbuffers,
				 int first_buf_id, bool init)
{
	RecyclePoolState *state = (RecyclePoolState *) strategy_data;

	if (!init)
		return;

	SpinLockInit(&state->lock);
	pg_atomic_init_u32(&state->nextVictimBuffer, 0);
	pg_atomic_init_u32(&state->numBufferAllocs, 0);
	state->nbuffers = nbuffers;
	state->first_buf_id = first_buf_id;

	pg_atomic_init_u64(&state->bulkread_allocs, 0);
	pg_atomic_init_u64(&state->bulkwrite_allocs, 0);
	pg_atomic_init_u64(&state->vacuum_allocs, 0);
}

/*
 * RecycleSyncStart -- return current clock position for checkpoint sync.
 */
static int
RecycleSyncStart(void *strategy_data, uint32 *complete_passes,
				 uint32 *num_buf_alloc)
{
	RecyclePoolState *state = (RecyclePoolState *) strategy_data;
	uint32		result;

	SpinLockAcquire(&state->lock);
	result = pg_atomic_read_u32(&state->nextVictimBuffer) % state->nbuffers;
	if (complete_passes)
		*complete_passes = 0;
	SpinLockRelease(&state->lock);

	if (num_buf_alloc)
		*num_buf_alloc = pg_atomic_exchange_u32(&state->numBufferAllocs, 0);

	return result;
}

/*
 * RecycleNotifyTrickle -- no-op; the trickle writer polls.
 */
static void
RecycleNotifyTrickle(void *strategy_data, int bgwprocno)
{
}

/*
 * Trickle iterator: walk from clock hand, yield dirty pages with
 * usage_count == 0 (already swept, ready for write-back).
 */
static void *
RecycleTrickleIterBegin(void *strategy_data, int max_candidates)
{
	RecycleTrickleIter *iter = palloc(sizeof(RecycleTrickleIter));
	RecyclePoolState *state = (RecyclePoolState *) strategy_data;

	iter->pos = pg_atomic_read_u32(&state->nextVictimBuffer) % state->nbuffers;
	iter->remaining = Min(max_candidates, state->nbuffers);
	return iter;
}

static int
RecycleTrickleIterNext(void *strategy_data, void *opaque)
{
	RecycleTrickleIter *iter = (RecycleTrickleIter *) opaque;
	RecyclePoolState *state = (RecyclePoolState *) strategy_data;

	while (iter->remaining > 0)
	{
		int			buf_id = state->first_buf_id + iter->pos;
		BufferDesc *buf = GetBufferDescriptor(buf_id);
		uint64		buf_state = pg_atomic_read_u64(&buf->state);

		iter->pos = (iter->pos + 1) % state->nbuffers;
		iter->remaining--;

		if ((buf_state & BM_VALID) && (buf_state & BM_DIRTY) &&
			BUF_STATE_GET_REFCOUNT(buf_state) == 0 &&
			BUF_STATE_GET_HEAT(buf_state) == 0)
			return buf_id;
	}
	return -1;
}

static void
RecycleTrickleIterEnd(void *strategy_data, void *opaque)
{
	pfree(opaque);
}

/*
 * recycle_pool_routine -- vtable for the RECYCLE buffer pool strategy.
 */
const BufferPoolRoutine recycle_pool_routine = {
	.type = T_Invalid,
	.on_hit = NULL,				/* one-chance: no usage tracking on hit */
	.on_miss = NULL,
	.on_evict = NULL,
	.on_new_tag = NULL,
	.get_victim = RecycleGetVictim,
	.sync_start = RecycleSyncStart,
	.notify_trickle = RecycleNotifyTrickle,
	.trickle_iter_begin = RecycleTrickleIterBegin,
	.trickle_iter_next = RecycleTrickleIterNext,
	.trickle_iter_end = RecycleTrickleIterEnd,
	.hint_vacuum = NULL,
	.reject_buffer = NULL,
	.prefetch_hint = NULL,
	.shmem_size = RecycleShmemSize,
	.shmem_init = RecycleShmemInit,
	.shutdown = NULL,
};


/* ----------------------------------------------------------------
 *				Backend-private buffer ring management
 * ----------------------------------------------------------------
 */


/*
 * GetAccessStrategy -- create a BufferAccessStrategy object
 *
 * The object is allocated in the current memory context.
 */
BufferAccessStrategy
GetAccessStrategy(BufferAccessStrategyType btype)
{
	int			ring_size_kb;

	/*
	 * Select ring size to use.  See buffer/README for rationales.
	 *
	 * Note: if you change the ring size for BAS_BULKREAD, see also
	 * SYNC_SCAN_REPORT_INTERVAL in access/heap/syncscan.c.
	 */
	switch (btype)
	{
		case BAS_NORMAL:
			/* if someone asks for NORMAL, just give 'em a "default" object */
			return NULL;

		case BAS_BULKREAD:
			{
				int			ring_max_kb;

				/*
				 * The ring always needs to be large enough to allow some
				 * separation in time between providing a buffer to the user
				 * of the strategy and that buffer being reused. Otherwise the
				 * user's pin will prevent reuse of the buffer, even without
				 * concurrent activity.
				 *
				 * We also need to ensure the ring always is large enough for
				 * SYNC_SCAN_REPORT_INTERVAL, as noted above.
				 *
				 * Thus we start out a minimal size and increase the size
				 * further if appropriate.
				 */
				ring_size_kb = 256;

				/*
				 * There's no point in a larger ring if we won't be allowed to
				 * pin sufficiently many buffers.  But we never limit to less
				 * than the minimal size above.
				 */
				ring_max_kb = GetPinLimit() * (BLCKSZ / 1024);
				ring_max_kb = Max(ring_size_kb, ring_max_kb);

				/*
				 * We would like the ring to additionally have space for the
				 * configured degree of IO concurrency. While being read in,
				 * buffers can obviously not yet be reused.
				 *
				 * Each IO can be up to io_combine_limit blocks large, and we
				 * want to start up to effective_io_concurrency IOs.
				 *
				 * Note that effective_io_concurrency may be 0, which disables
				 * AIO.
				 */
				ring_size_kb += (BLCKSZ / 1024) *
					io_combine_limit * effective_io_concurrency;

				if (ring_size_kb > ring_max_kb)
					ring_size_kb = ring_max_kb;
				break;
			}
		case BAS_BULKWRITE:
			ring_size_kb = 16 * 1024;
			break;
		case BAS_VACUUM:
			ring_size_kb = 2048;
			break;

		default:
			elog(ERROR, "unrecognized buffer access strategy: %d",
				 (int) btype);
			return NULL;		/* keep compiler quiet */
	}

	return GetAccessStrategyWithSize(btype, ring_size_kb);
}

/*
 * GetAccessStrategyWithSize -- create a BufferAccessStrategy object with a
 *		number of buffers equivalent to the passed in size.
 *
 * If the given ring size is 0, no BufferAccessStrategy will be created and
 * the function will return NULL.  ring_size_kb must not be negative.
 */
BufferAccessStrategy
GetAccessStrategyWithSize(BufferAccessStrategyType btype, int ring_size_kb)
{
	int			ring_buffers;
	BufferAccessStrategy strategy;
	BufferPoolDesc *recycle;

	Assert(ring_size_kb >= 0);

	/* Figure out how many buffers ring_size_kb is */
	ring_buffers = ring_size_kb / (BLCKSZ / 1024);

	/* 0 means unlimited, so no BufferAccessStrategy required */
	if (ring_buffers == 0)
		return NULL;

	/*
	 * If the RECYCLE pool is available, route through it instead of
	 * allocating a private ring buffer.  We still allocate a minimal strategy
	 * struct to carry the btype and recycle_pool pointer.
	 */
	recycle = GetBufferPoolByKind(BUFPOOL_RECYCLE);
	if (recycle != NULL && recycle->bp_active)
	{
		strategy = (BufferAccessStrategy)
			palloc0(offsetof(BufferAccessStrategyData, buffers));

		strategy->btype = btype;
		strategy->nbuffers = 0;
		strategy->recycle_pool = recycle;

		return strategy;
	}

	/* Traditional per-backend ring buffer path */

	/* Cap to 1/8th of shared_buffers */
	ring_buffers = Min(NBuffers / 8, ring_buffers);

	/* NBuffers should never be less than 16, so this shouldn't happen */
	Assert(ring_buffers > 0);

	/* Allocate the object and initialize all elements to zeroes */
	strategy = (BufferAccessStrategy)
		palloc0(offsetof(BufferAccessStrategyData, buffers) +
				ring_buffers * sizeof(Buffer));

	/* Set fields that don't start out zero */
	strategy->btype = btype;
	strategy->nbuffers = ring_buffers;
	strategy->recycle_pool = NULL;

	return strategy;
}

/*
 * GetAccessStrategyBufferCount -- an accessor for the number of buffers in
 *		the ring
 *
 * Returns 0 on NULL input to match behavior of GetAccessStrategyWithSize()
 * returning NULL with 0 size.
 */
int
GetAccessStrategyBufferCount(BufferAccessStrategy strategy)
{
	if (strategy == NULL)
		return 0;

	/* When using the RECYCLE pool, report its buffer count */
	if (strategy->recycle_pool != NULL)
		return strategy->recycle_pool->bp_nbuffers;

	return strategy->nbuffers;
}

/*
 * GetAccessStrategyPinLimit -- get cap of number of buffers that should be pinned
 *
 * When pinning extra buffers to look ahead, users of a ring-based strategy are
 * in danger of pinning too much of the ring at once while performing look-ahead.
 * For some strategies, that means "escaping" from the ring, and in others it
 * means forcing dirty data to disk very frequently with associated WAL
 * flushing.  Since external code has no insight into any of that, allow
 * individual strategy types to expose a clamp that should be applied when
 * deciding on a maximum number of buffers to pin at once.
 *
 * Callers should combine this number with other relevant limits and take the
 * minimum.
 */
int
GetAccessStrategyPinLimit(BufferAccessStrategy strategy)
{
	if (strategy == NULL)
		return NBuffers;

	/*
	 * When using the RECYCLE pool, the pin limit is based on the RECYCLE
	 * pool's buffer count rather than the ring size.
	 */
	if (strategy->recycle_pool != NULL)
	{
		int			pool_nbuffers = strategy->recycle_pool->bp_nbuffers;

		switch (strategy->btype)
		{
			case BAS_BULKREAD:
				return pool_nbuffers;
			default:
				return pool_nbuffers / 2;
		}
	}

	switch (strategy->btype)
	{
		case BAS_BULKREAD:

			/*
			 * Since BAS_BULKREAD uses StrategyRejectBuffer(), dirty buffers
			 * shouldn't be a problem and the caller is free to pin up to the
			 * entire ring at once.
			 */
			return strategy->nbuffers;

		default:

			/*
			 * Tell caller not to pin more than half the buffers in the ring.
			 * This is a trade-off between look ahead distance and deferring
			 * writeback and associated WAL traffic.
			 */
			return strategy->nbuffers / 2;
	}
}

/*
 * FreeAccessStrategy -- release a BufferAccessStrategy object
 *
 * A simple pfree would do at the moment, but we would prefer that callers
 * don't assume that much about the representation of BufferAccessStrategy.
 */
void
FreeAccessStrategy(BufferAccessStrategy strategy)
{
	/* don't crash if called on a "default" strategy */
	if (strategy != NULL)
		pfree(strategy);
}

/*
 * GetBufferFromRing -- returns a buffer from the ring, or NULL if the
 *		ring is empty / not usable.
 *
 * The buffer is pinned and marked as owned, using TrackNewBufferPin(), before
 * returning.
 */
static BufferDesc *
GetBufferFromRing(BufferAccessStrategy strategy, uint64 *buf_state)
{
	BufferDesc *buf;
	Buffer		bufnum;
	uint64		old_buf_state;
	uint64		local_buf_state;	/* to avoid repeated (de-)referencing */


	/* Advance to next ring slot */
	if (++strategy->current >= strategy->nbuffers)
		strategy->current = 0;

	/*
	 * If the slot hasn't been filled yet, tell the caller to allocate a new
	 * buffer with the normal allocation strategy.  He will then fill this
	 * slot by calling AddBufferToRing with the new buffer.
	 */
	bufnum = strategy->buffers[strategy->current];
	if (bufnum == InvalidBuffer)
		return NULL;

	buf = GetBufferDescriptor(bufnum - 1);

	/*
	 * Check whether the buffer can be used and pin it if so. Do this using a
	 * CAS loop, to avoid having to lock the buffer header.
	 */
	old_buf_state = pg_atomic_read_u64(&buf->state);
	for (;;)
	{
		local_buf_state = old_buf_state;

		/*
		 * If the buffer is pinned we cannot use it under any circumstances.
		 *
		 * If usage_count is 0 or 1 then the buffer is fair game (we expect 1,
		 * since our own previous usage of the ring element would have left it
		 * there, but it might've been decremented by clock-sweep since then).
		 * A higher usage_count indicates someone else has touched the buffer,
		 * so we shouldn't re-use it.
		 */
		if (BUF_STATE_GET_REFCOUNT(local_buf_state) != 0
			|| BUF_STATE_GET_HEAT(local_buf_state) > 1)
			break;

		/* See equivalent code in PinBuffer() */
		if (unlikely(local_buf_state & BM_LOCKED))
		{
			old_buf_state = WaitBufHdrUnlocked(buf);
			continue;
		}

		/* pin the buffer if the CAS succeeds */
		local_buf_state += BUF_REFCOUNT_ONE;

		if (pg_atomic_compare_exchange_u64(&buf->state, &old_buf_state,
										   local_buf_state))
		{
			*buf_state = local_buf_state;

			TrackNewBufferPin(BufferDescriptorGetBuffer(buf));
			return buf;
		}
	}

	/*
	 * Tell caller to allocate a new buffer with the normal allocation
	 * strategy.  He'll then replace this ring element via AddBufferToRing.
	 */
	return NULL;
}

/*
 * AddBufferToRing -- add a buffer to the buffer ring
 *
 * Caller must hold the buffer header spinlock on the buffer.  Since this
 * is called with the spinlock held, it had better be quite cheap.
 */
static void
AddBufferToRing(BufferAccessStrategy strategy, BufferDesc *buf)
{
	strategy->buffers[strategy->current] = BufferDescriptorGetBuffer(buf);
}

/*
 * Utility function returning the IOContext of a given BufferAccessStrategy's
 * strategy ring.
 */
IOContext
IOContextForStrategy(BufferAccessStrategy strategy)
{
	if (!strategy)
		return IOCONTEXT_NORMAL;

	switch (strategy->btype)
	{
		case BAS_NORMAL:

			/*
			 * Currently, GetAccessStrategy() returns NULL for
			 * BufferAccessStrategyType BAS_NORMAL, so this case is
			 * unreachable.
			 */
			pg_unreachable();
			return IOCONTEXT_NORMAL;
		case BAS_BULKREAD:
			return IOCONTEXT_BULKREAD;
		case BAS_BULKWRITE:
			return IOCONTEXT_BULKWRITE;
		case BAS_VACUUM:
			return IOCONTEXT_VACUUM;
	}

	elog(ERROR, "unrecognized BufferAccessStrategyType: %d", strategy->btype);
	pg_unreachable();
}


/*
 * RegisterDefaultPoolAlgorithm -- register an algorithm for DEFAULT pool use.
 *
 * Called from _PG_init() of buffer pool extensions.  Must happen before
 * shared memory initialization so the algorithm is visible to the DEFAULT
 * pool's routine lookup.  'name' must be pointer-stable for the lifetime
 * of the server; the registry keeps the pointer by reference.
 */
void
RegisterDefaultPoolAlgorithm(const char *name,
							 const BufferPoolRoutine *routine)
{
	if (name == NULL || name[0] == '\0')
		elog(ERROR, "buffer pool algorithm name must be non-empty");
	if (routine == NULL)
		elog(ERROR, "buffer pool algorithm routine for \"%s\" is NULL", name);

	for (int i = 0; i < algo_registry_len; i++)
	{
		if (strcmp(algo_registry[i].name, name) == 0)
		{
			elog(WARNING,
				 "buffer pool algorithm \"%s\" already registered, replacing",
				 name);
			algo_registry[i].routine = routine;
			return;
		}
	}

	if (algo_registry_len >= MAX_DEFAULT_POOL_ALGORITHMS)
		elog(ERROR,
			 "too many buffer pool algorithms registered (max %d)",
			 MAX_DEFAULT_POOL_ALGORITHMS);

	algo_registry[algo_registry_len].name = name;
	algo_registry[algo_registry_len].routine = routine;
	algo_registry_len++;
}

/*
 * LookupDefaultPoolAlgorithm -- return the routine registered under name,
 * or NULL if no such name is registered.
 */
const BufferPoolRoutine *
LookupDefaultPoolAlgorithm(const char *name)
{
	if (name == NULL)
		return NULL;

	for (int i = 0; i < algo_registry_len; i++)
	{
		if (strcmp(algo_registry[i].name, name) == 0)
			return algo_registry[i].routine;
	}
	return NULL;
}
