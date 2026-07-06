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
 * The public entry points (StrategyGetBuffer, StrategySyncStart, etc.)
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

#define INT_ACCESS_ONCE(var)	((int)(*((volatile int *)&(var))))


/*
 * The shared freelist control information.
 */
typedef struct
{
	/* Spinlock: protects the values below */
	slock_t		buffer_strategy_lock;

	/*
	 * clock-sweep hand: index of next buffer to consider grabbing. Note that
	 * this isn't a concrete buffer - we only ever increase the value. So, to
	 * get an actual buffer, it needs to be used modulo NBuffers.
	 */
	pg_atomic_uint32 nextVictimBuffer;

	/*
	 * Statistics.  These counters should be wide enough that they can't
	 * overflow during a single bgwriter cycle.
	 */
	uint32		completePasses; /* Complete cycles of the clock-sweep */
	pg_atomic_uint32 numBufferAllocs;	/* Buffers allocated since last reset */

	/*
	 * Bgworker process to be notified upon activity or -1 if none. See
	 * StrategyNotifyBgWriter.
	 */
	int			bgwprocno;
} BufferStrategyControl;

/*
 * ClockPoolState -- per-pool clock-sweep state for dynamic buffer pools.
 *
 * This is the equivalent of BufferStrategyControl for dynamic pools.
 * It is stored in the DSM segment as the pool's strategy_data and includes
 * the pool's buffer range so that ClockGetVictim knows which buffers to sweep.
 *
 * Unlike the default pool which uses the global BufferDescriptors array
 * and NBuffers, dynamic pools have their own buffer descriptor arrays with
 * buffer IDs starting at first_buf_id.  The clock hand sweeps from
 * first_buf_id through first_buf_id + nbuffers - 1.
 */
typedef struct ClockPoolState
{
	slock_t		lock;
	pg_atomic_uint32 nextVictimBuffer;	/* monotonically increasing, mod
										 * nbuffers */
	uint32		completePasses;
	pg_atomic_uint32 numBufferAllocs;
	int			bgwprocno;		/* trickle writer procno (-1 = none) */
	int			nbuffers;		/* buffer count in this pool */
	int			first_buf_id;	/* starting buffer ID */
}			ClockPoolState;

/* Pointers to shared state */
static BufferStrategyControl *StrategyControl = NULL;

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
 * True when the active DEFAULT pool is the plain built-in clock sweep
 * (clock_pool_routine).  Lets StrategyGetBuffer() devirtualize the
 * overwhelmingly common victim path with a single predicted-true branch,
 * bypassing the vtable indirection.  False for dynamic pools, numa_clock, and
 * any extension-provided algorithm -- those keep the indirect vtable path.
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
}			DefaultPoolAlgorithmEntry;

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
 * ClockTryAcquireVictim -- try to claim buffer victim_id as a victim.
 *
 * Shared by the plain clock sweep and the NUMA-partitioned clock sweep so the
 * delicate lock-free CAS acquire logic exists in exactly one place.  Returns
 * the pinned BufferDesc on success (with *buf_state set and the pin tracked),
 * or NULL if the buffer was pinned/in-use (caller advances the hand and
 * retries).  On a usage_count>0 buffer it decrements the count (a "tick") and
 * returns NULL; trycounter is reset to reset_budget.  When refcount!=0 reduces
 * trycounter to zero the pool is genuinely full and we elog(ERROR).
 */
static BufferDesc *
ClockTryAcquireVictim(int victim_id, BufferAccessStrategy strategy,
					  uint64 *buf_state, int *trycounter, int reset_budget)
{
	BufferDesc *buf = GetBufferDescriptor(victim_id);
	uint64		old_buf_state = pg_atomic_read_u64(&buf->state);

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

		if (BUF_STATE_GET_USAGECOUNT(local_buf_state) != 0)
		{
			local_buf_state -= BUF_USAGECOUNT_ONE;
			if (pg_atomic_compare_exchange_u64(&buf->state, &old_buf_state,
											   local_buf_state))
			{
				*trycounter = reset_budget;
				return NULL;
			}
		}
		else
		{
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
}

/*
 * NumaClockControl -- NUMA-partitioned clock-sweep state for the default pool.
 *
 * One independent clock hand per NUMA node, each confined to that node's
 * contiguous buffer range (see BufPoolNumaBufferRange).  A backend sweeps its
 * own node's partition first (node-local victims, the whole point), and only
 * falls back to other nodes' partitions when its own cannot yield a buffer --
 * so a single backend can still use all of shared_buffers, satisfying the
 * "steal from other partitions" requirement Vondra's series calls out.
 *
 * This targets the scalability cliff Andres/Vondra measured on multi-socket
 * hardware: the single global nextVictimBuffer counter and cross-node victim
 * traffic become per-node, so backends contend on their own node's hand and
 * reuse node-local memory.
 */
#define BUFPOOL_MAX_NUMA_NODES	64

/*
 * Per-core stripes within a node's buffer range (buffer_pool_numa_cooling).
 * Each stripe has its own clock hand, so a buffer is swept by exactly one
 * owner per pass -- which makes the blind-atomic usage_count cooling sub
 * (fetch_sub instead of a CAS loop) borrow-safe.  Capped small: more stripes
 * than cores-per-node gives no benefit and bloats shmem.
 */
#define BUFPOOL_MAX_STRIPES		16

/*
 * Number of consecutive buffer IDs a backend claims from the global clock hand
 * per atomic fetch_add in the batched cooling sweep (Jim Mlodgenski's design).
 * Larger reduces cross-socket atomic contention proportionally.
 */
#define CLOCK_SWEEP_BATCH_SIZE	64

/*
 * A clock-sweep hand on its own cache line.
 *
 * The stripe hands are the atomic each core bumps on every tick.  Packed
 * naively, BUFPOOL_MAX_STRIPES 4-byte atomics share a single 64-byte line, so
 * cores on the same node would false-share and ping-pong that line -- exactly
 * the cross-core atomic traffic this design exists to remove.  Padding each
 * hand to a full cache line makes each core's hand private.  (The per-buffer
 * usage_count is already isolated: BufferDesc is padded to a full line, so the
 * blind cooling sub touches one line per buffer regardless of stripe size.)
 */
typedef union CacheAlignedHand
{
	pg_atomic_uint32 hand;
	char		pad[PG_CACHE_LINE_SIZE];
}			CacheAlignedHand;

typedef struct NumaClockControl
{
	slock_t		lock;			/* protects completePasses[] */
	int			nnodes;
	int			nbuffers;
	int			nstripes;		/* per-node stripe count (>=1); 1 == off */
	uint32		batchSize;		/* clock-sweep batch claimed per fetch_add */
	pg_atomic_uint32 nextVictim[BUFPOOL_MAX_NUMA_NODES];	/* per-node hand */
	uint32		completePasses[BUFPOOL_MAX_NUMA_NODES];
	/*
	 * Per-(node,stripe) hands for the cooling sweep.  Each is confined to its
	 * stripe sub-range, giving single-owner-per-buffer-per-pass, and each sits
	 * on its own cache line so same-node cores do not false-share.  Unused when
	 * nstripes == 1 (cooling disabled).
	 */
	CacheAlignedHand stripeHand[BUFPOOL_MAX_NUMA_NODES][BUFPOOL_MAX_STRIPES];
	pg_atomic_uint32 numBufferAllocs;
	int			bgwprocno;
} NumaClockControl;

static NumaClockControl *NumaClockCtl = NULL;

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
};

/*
 * NumaClockGetVictim -- NUMA-partitioned clock-sweep victim selection.
 *
 * Used only for the DEFAULT pool on multi-node hardware (strategy_data ==
 * NumaClockCtl).  Sweeps the caller's local node partition first, then other
 * nodes in turn, so victims are node-local when possible but a backend can
 * still consume the whole pool when its node is saturated.
 */
static BufferDesc *
NumaClockGetVictim(void *strategy_data,
				   BufferAccessStrategy strategy,
				   uint64 *buf_state,
				   bool *from_ring)
{
	NumaClockControl *ctl = (NumaClockControl *) strategy_data;
	int			nnodes = ctl->nnodes;
	int			home = BufPoolNumaNodeForProc();

	*from_ring = false;

	/* Ring-buffer / RECYCLE handling is identical to the plain sweep. */
	if (strategy != NULL)
	{
		if (strategy->recycle_pool != NULL && strategy->recycle_pool->bp_active)
		{
			PoolLocalState *local = EnsurePoolAttached(strategy->recycle_pool);
			BufferDesc *buf = strategy->recycle_pool->bp_routine->get_victim(
																		local->strategy_data, NULL, buf_state, from_ring);

			if (buf != NULL)
			{
				*from_ring = true;
				return buf;
			}
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

	{
		int			bgwprocno = INT_ACCESS_ONCE(ctl->bgwprocno);

		if (bgwprocno != -1)
		{
			ctl->bgwprocno = -1;
			SetLatch(&GetPGProcByNumber(bgwprocno)->procLatch);
		}
	}
	pg_atomic_fetch_add_u32(&ctl->numBufferAllocs, 1);

	/* Try the home node first, then each other node in order. */
	for (int attempt = 0; attempt < nnodes; attempt++)
	{
		int			node = (home + attempt) % nnodes;
		int			start,
					end,
					range;
		int			trycounter;

		BufPoolNumaBufferRange(node, ctl->nbuffers, &start, &end);
		range = end - start;
		if (range <= 0)
			continue;

		/*
		 * Bound the work on this node's partition before falling back to the
		 * next: one full pass of this partition.  This prevents a saturated
		 * node from spinning forever while another node has free buffers.
		 */
		trycounter = range;
		while (trycounter > 0)
		{
			uint32		victim_raw = pg_atomic_fetch_add_u32(&ctl->nextVictim[node], 1);
			int			victim_id = start + (int) (victim_raw % (uint32) range);
			int			local_try = range;
			BufferDesc *buf;

			/* Maintain per-node completePasses on wraparound. */
			if (victim_raw >= (uint32) range &&
				(victim_raw % (uint32) range) == 0)
			{
				SpinLockAcquire(&ctl->lock);
				ctl->completePasses[node]++;
				SpinLockRelease(&ctl->lock);
			}

			buf = ClockTryAcquireVictim(victim_id, strategy, buf_state,
										&local_try, range);
			if (buf != NULL)
				return buf;

			trycounter--;
		}
		/* This node's partition yielded nothing; try the next node. */
	}

	elog(ERROR, "no unpinned buffers available");
	pg_unreachable();
}

/*
 * ClockTryAcquireVictimCooling -- like ClockTryAcquireVictim, but the
 * usage_count cooling decrement uses a single blind pg_atomic_fetch_sub_u64
 * instead of a CAS retry loop.
 *
 * SAFETY: this is only correct when the caller guarantees a single sweeping
 * owner for victim_id in the current pass (per-core stripe ownership).  With a
 * single owner, at most one sub touches this buffer's usage_count per pass, so
 * a blind sub from a value we just read as > 0 always lands >= 0 and never
 * borrows into the flag/lock bits above usage_count (see NUMA_PARTITIONED_
 * SWEEP_DESIGN.md and /tmp/borrow_test.c).  A racing PinBuffer can only RAISE
 * usage_count/refcount between our read and sub, so the worst case is cooling
 * an already-hot buffer one extra tick -- benign, within usage_count's
 * resolution.
 *
 * The pin transition (usage_count==0, refcount==0 -> pinned) STILL uses CAS:
 * that one must be race-correct against concurrent pins from other backends.
 * Only the cooling tick goes blind.
 */
static BufferDesc *
ClockTryAcquireVictimCooling(int victim_id, BufferAccessStrategy strategy,
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

	if (BUF_STATE_GET_USAGECOUNT(old_buf_state) != 0)
	{
		/*
		 * Cooling tick: blind atomic sub, no CAS.  Safe because this stripe
		 * has a single owner this pass (see function header): only one sub
		 * touches this buffer's usage_count per pass, and we just read it as
		 * > 0, so subtracting BUF_USAGECOUNT_ONE (bit 18) lands within bits
		 * 0..21 and never borrows into the flag/lock bits.  Use the full
		 * 64-bit atomic (endian-safe; a 32-bit sub on the low half would hit
		 * the wrong bits on big-endian).  Assert we did not underflow -- the
		 * runnable check that the single-owner invariant holds.
		 */
		uint64		prev PG_USED_FOR_ASSERTS_ONLY;

		prev = pg_atomic_fetch_sub_u64(&buf->state, BUF_USAGECOUNT_ONE);
		Assert(BUF_STATE_GET_USAGECOUNT(prev) != 0);
		*trycounter = reset_budget;
		return NULL;
	}

	/* usage_count == 0, refcount == 0: claim it.  This MUST stay a CAS. */
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
		if (BUF_STATE_GET_USAGECOUNT(local_buf_state) != 0)
		{
			/* Someone re-warmed it; treat as a cooling tick and move on. */
			local_buf_state -= BUF_USAGECOUNT_ONE;
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

/*
 * NumaCoolingStripeRange -- the [start, end) sub-range of node's partition
 * owned by stripe (0..nstripes-1).  Even split of the node range; the last
 * stripe absorbs any remainder.
 */
static inline void
NumaCoolingStripeRange(int node_start, int node_end, int nstripes, int stripe,
					   int *start, int *end)
{
	int			node_range = node_end - node_start;
	int			w = node_range / nstripes;

	if (w <= 0)
	{
		/* Fewer buffers than stripes: stripe 0 owns all, others empty. */
		*start = (stripe == 0) ? node_start : node_end;
		*end = node_end;
		return;
	}
	*start = node_start + stripe * w;
	*end = (stripe == nstripes - 1) ? node_end : (*start + w);
}

/*
 * NumaCoolingGetVictim -- per-core GLOBALLY-striped clock sweep with
 * blind-atomic cooling (buffer_pool_numa_cooling).
 *
 * Each core owns a disjoint stripe that tiles the ENTIRE pool [0, NBuffers),
 * swept with its own hand.  A buffer thus has exactly one sweeping owner per
 * pass, so the cooling decrement is a blind fetch_sub
 * (ClockTryAcquireVictimCooling) rather than a CAS loop -- the payoff that
 * makes this cheaper than the global CAS-per-tick sweep on multi-socket
 * hardware.
 *
 * Stripes tile the whole pool, NOT each node's slice (unlike numa_clock):
 * confining a backend's loads/evictions to its node's 1/nnodes of
 * shared_buffers shrinks the effective cache to that slice and craters the hit
 * ratio for a uniform working set (measured ~8x collapse).  Physical NUMA
 * locality comes from interleaved placement (BufPoolNumaDistribute); the
 * logical eviction is NOT node-confined.
 *
 * When the core's stripe cannot yield a victim, we fall back to a shared
 * whole-pool CAS sweep (ClockTryAcquireVictim), which guarantees reachability.
 */
static BufferDesc *
NumaCoolingGetVictim(void *strategy_data,
					 BufferAccessStrategy strategy,
					 uint64 *buf_state,
					 bool *from_ring)
{
	NumaClockControl *ctl = (NumaClockControl *) strategy_data;
	int			nbuffers = ctl->nbuffers;
	int			trycounter;

	/*
	 * Per-backend batch of buffer IDs claimed from the single global clock
	 * hand.  MyBatchPos is the next id to consider; MyBatchEnd is one past the
	 * end of the claimed batch.  Both are absolute (monotonically increasing)
	 * hand values; the actual buffer id is the value modulo nbuffers.
	 */
	static uint32 MyBatchPos = 0;
	static uint32 MyBatchEnd = 0;

	*from_ring = false;

	/* Ring-buffer / RECYCLE handling is identical to the plain sweep. */
	if (strategy != NULL)
	{
		if (strategy->recycle_pool != NULL && strategy->recycle_pool->bp_active)
		{
			PoolLocalState *local = EnsurePoolAttached(strategy->recycle_pool);
			BufferDesc *buf = strategy->recycle_pool->bp_routine->get_victim(
																local->strategy_data, NULL, buf_state, from_ring);

			if (buf != NULL)
			{
				*from_ring = true;
				return buf;
			}
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

	{
		int			bgwprocno = INT_ACCESS_ONCE(ctl->bgwprocno);

		if (bgwprocno != -1)
		{
			ctl->bgwprocno = -1;
			SetLatch(&GetPGProcByNumber(bgwprocno)->procLatch);
		}
	}
	pg_atomic_fetch_add_u32(&ctl->numBufferAllocs, 1);

	/*
	 * Batched global clock sweep (Jim Mlodgenski's design) + blind-atomic
	 * cooling.
	 *
	 * There is ONE global clock hand (ctl->nextVictim[0]) that traverses the
	 * WHOLE pool, exactly like the plain clock -- so every buffer is reachable
	 * and fillable by every backend, and the effective cache is all of
	 * shared_buffers (NOT 1/N of it -- the fatal flaw of any per-backend
	 * sub-range partition).  The only change from the plain clock is that a
	 * backend advances the hand a BATCH at a time (one fetch_add of
	 * ClockSweepBatchSize) and then iterates that batch privately, so the
	 * contended atomic fires ~1/batch as often -- the multi-socket win.
	 *
	 * Blind cooling is safe here because BATCH ownership is exclusive:
	 * consecutive fetch_add(batch) calls hand out DISJOINT [start, start+batch)
	 * ranges, so within one pass each buffer is examined by exactly one
	 * backend -- the single-owner-per-pass property the blind usage_count
	 * fetch_sub needs (see ClockTryAcquireVictimCooling).
	 */
	trycounter = nbuffers;
	for (;;)
	{
		uint32		handval;
		int			victim_id;
		BufferDesc *buf;

		/* Claim a fresh batch from the global hand when the local one is spent. */
		if (MyBatchPos >= MyBatchEnd)
		{
			uint32		batch = ctl->batchSize;
			uint32		start;

			start = pg_atomic_fetch_add_u32(&ctl->nextVictim[0], batch);
			MyBatchPos = start;
			MyBatchEnd = start + batch;
		}

		handval = MyBatchPos++;
		victim_id = (int) (handval % (uint32) nbuffers);

		/*
		 * Pass the OUTER trycounter: ClockTryAcquireVictimCooling resets it to
		 * nbuffers on a cooling tick (a decrement is progress toward an
		 * evictable buffer) and only decrements it when a buffer is pinned (no
		 * progress).  So we only error after nbuffers consecutive PINNED
		 * buffers, matching the plain clock -- not after nbuffers cooling ticks.
		 */
		buf = ClockTryAcquireVictimCooling(victim_id, strategy, buf_state,
										   &trycounter, nbuffers);
		if (buf != NULL)
			return buf;
	}

	pg_unreachable();
}

/*
 * NumaClockSyncStart / NumaClockNotifyTrickle -- sync/notify for the
 * NUMA-partitioned default-pool routine.  The NUMA control is neither
 * StrategyControl nor a ClockPoolState, so it needs its own (trivial)
 * implementations rather than the dynamic-pool-detecting plain ones.
 */
static int
NumaClockSyncStart(void *strategy_data, uint32 *complete_passes,
				   uint32 *num_buf_alloc)
{
	NumaClockControl *ctl = (NumaClockControl *) strategy_data;
	uint32		next0 = pg_atomic_read_u32(&ctl->nextVictim[0]);
	int			start,
				end,
				range;

	BufPoolNumaBufferRange(0, ctl->nbuffers, &start, &end);
	range = (end > start) ? (end - start) : 1;

	if (complete_passes)
	{
		/* Sum per-node passes for a whole-pool view. */
		uint32		sum = 0;

		SpinLockAcquire(&ctl->lock);
		for (int n = 0; n < ctl->nnodes; n++)
			sum += ctl->completePasses[n];
		SpinLockRelease(&ctl->lock);
		*complete_passes = sum;
	}
	if (num_buf_alloc)
		*num_buf_alloc = pg_atomic_exchange_u32(&ctl->numBufferAllocs, 0);

	return start + (int) (next0 % (uint32) range);
}

static void
NumaClockNotifyTrickle(void *strategy_data, int bgwprocno)
{
	NumaClockControl *ctl = (NumaClockControl *) strategy_data;
	ctl->bgwprocno = bgwprocno;
}

/*
 * NumaCoolingSyncStart -- sync_start for the batched global cooling sweep.
 *
 * The batched sweep advances the single global hand ctl->nextVictim[0] (a
 * batch at a time) and maintains ctl->completePasses[0] on wraparound, exactly
 * like the plain clock's StrategyControl.  So report that hand and pass count
 * directly; it advances under load, so BgBufferSync's strategy_delta stays >= 0
 * and its pacing works.
 */
static int
NumaCoolingSyncStart(void *strategy_data, uint32 *complete_passes,
					 uint32 *num_buf_alloc)
{
	NumaClockControl *ctl = (NumaClockControl *) strategy_data;
	uint32		nbuf = (ctl->nbuffers > 0) ? (uint32) ctl->nbuffers : 1;
	uint32		hand = pg_atomic_read_u32(&ctl->nextVictim[0]);

	/*
	 * Derive BOTH position and passes from the SAME monotonic hand value so
	 * the (position, passes) pair BgBufferSync sees is always consistent and
	 * non-decreasing (its strategy_delta Assert requires delta >= 0).  The
	 * global hand grows unboundedly (never wrapped in place), so passes is
	 * simply hand / nbuffers and position is hand %% nbuffers.
	 */
	if (complete_passes)
		*complete_passes = hand / nbuf;
	if (num_buf_alloc)
		*num_buf_alloc = pg_atomic_exchange_u32(&ctl->numBufferAllocs, 0);

	return (int) (hand % nbuf);
}

/*
 * BufPoolNumaClockStatsMax -- upper bound on rows BufPoolNumaClockStats emits.
 *
 * One row per (node, stripe).  When the NUMA/cooling sweep is inactive the
 * function still emits exactly one synthetic row, so this is at least 1.
 */
int
BufPoolNumaClockStatsMax(void)
{
	bool		numa = (ActivePoolData == NumaClockCtl && NumaClockCtl != NULL &&
						NumaClockCtl->nnodes > 0);

	if (!numa)
		return 1;
	return NumaClockCtl->nnodes * Max(1, NumaClockCtl->nstripes);
}

/*
 * BufPoolNumaClockStats -- fill out[] with per-(node,stripe) sweep state.
 *
 * Read-only and cheap: a handful of atomic reads per row plus one spinlock
 * acquire to snapshot completePasses[].  Returns the number of rows written
 * (<= maxrows).
 *
 * Three shapes, all reported through the same row layout:
 *   - NUMA off (default): one row, node 0 / stripe 0, describing the single
 *     global clock hand in StrategyControl.
 *   - NUMA on, cooling off: one row per node, each with that node's hand and
 *     completePasses; stripe is always 0.
 *   - NUMA on, cooling on: one row per (node, stripe), each stripe reporting
 *     its own hand; completePasses is the node's (stripe hands share it).
 */
int
BufPoolNumaClockStats(BufPoolNumaStat *out, int maxrows)
{
	bool		numa = (ActivePoolData == NumaClockCtl && NumaClockCtl != NULL &&
						NumaClockCtl->nnodes > 0);

	if (maxrows <= 0)
		return 0;

	if (!numa)
	{
		/* Plain global clock sweep: one synthetic node-0 row. */
		uint32		hand;

		SpinLockAcquire(&StrategyControl->buffer_strategy_lock);
		hand = pg_atomic_read_u32(&StrategyControl->nextVictimBuffer);
		out[0].complete_passes = StrategyControl->completePasses +
			(NBuffers > 0 ? hand / (uint32) NBuffers : 0);
		SpinLockRelease(&StrategyControl->buffer_strategy_lock);

		out[0].node = 0;
		out[0].stripe = 0;
		out[0].nbuffers = NBuffers;
		out[0].clock_hand = (NBuffers > 0) ? (hand % (uint32) NBuffers) : 0;
		return 1;
	}
	else
	{
		NumaClockControl *ctl = NumaClockCtl;
		bool		cooling = (ActivePoolRoutine == &numa_cooling_pool_routine);
		uint32		nbuf = (ctl->nbuffers > 0) ? (uint32) ctl->nbuffers : 1;

		if (cooling)
		{
			/*
			 * Batched global cooling sweep: ONE global hand over the whole
			 * pool (see NumaCoolingGetVictim).  Report a single row describing
			 * it; the per-node/per-stripe hands are unused in this mode.
			 * stripe carries the batch size for visibility.
			 */
			uint32		hand = pg_atomic_read_u32(&ctl->nextVictim[0]);

			out[0].node = 0;
			out[0].stripe = (int) ctl->batchSize;
			out[0].nbuffers = ctl->nbuffers;
			out[0].clock_hand = hand % nbuf;
			out[0].complete_passes = hand / nbuf;
			return 1;
		}
		else
		{
			/* NUMA-partitioned (non-cooling): one hand per node. */
			uint32		passes[BUFPOOL_MAX_NUMA_NODES];
			int			n = 0;

			SpinLockAcquire(&ctl->lock);
			for (int node = 0; node < ctl->nnodes; node++)
				passes[node] = ctl->completePasses[node];
			SpinLockRelease(&ctl->lock);

			for (int node = 0; node < ctl->nnodes && n < maxrows; node++)
			{
				int			start,
							end,
							range;
				uint32		hand;

				BufPoolNumaBufferRange(node, ctl->nbuffers, &start, &end);
				hand = pg_atomic_read_u32(&ctl->nextVictim[node]);
				range = (end > start) ? (end - start) : 0;

				out[n].node = node;
				out[n].stripe = 0;
				out[n].nbuffers = range;
				out[n].clock_hand = (range > 0)
					? (uint32) (start + (int) (hand % (uint32) range))
					: (uint32) start;
				out[n].complete_passes = passes[node];
				n++;
			}
			return n;
		}
	}
}

/*
 * The NUMA-partitioned clock-sweep routine for the DEFAULT pool.
 *
 * Identical to clock_pool_routine except for get_victim; reuses the plain
 * sync_start / notify_trickle / reject_buffer (sync_start reads node 0's hand,
 * which is sufficient to seed BgBufferSync's circular scan).  Selected
 * automatically at startup when buffer_pool_numa is on and the hardware has
 * more than one NUMA node.
 */
const BufferPoolRoutine numa_clock_pool_routine = {
	.type = T_Invalid,
	.on_hit = NULL,
	.on_miss = NULL,
	.on_evict = NULL,
	.on_new_tag = NULL,
	.get_victim = NumaClockGetVictim,
	.sync_start = NumaClockSyncStart,
	.notify_trickle = NumaClockNotifyTrickle,
	.trickle_iter_begin = NULL,
	.trickle_iter_next = NULL,
	.trickle_iter_end = NULL,
	.hint_vacuum = NULL,
	.reject_buffer = ClockRejectBuffer,
	.prefetch_hint = NULL,
	.shmem_size = ClockPoolShmemSize,
	.shmem_init = ClockPoolShmemInit,
	.shutdown = NULL,
};

/*
 * The per-core striped NUMA clock sweep with blind-atomic cooling for the
 * DEFAULT pool.  Same as numa_clock_pool_routine but with the striped
 * get_victim.  Selected at startup when buffer_pool_numa AND
 * buffer_pool_numa_cooling are on and the hardware has more than one NUMA node.
 */
const BufferPoolRoutine numa_cooling_pool_routine = {
	.type = T_Invalid,
	.on_hit = NULL,
	.on_miss = NULL,
	.on_evict = NULL,
	.on_new_tag = NULL,
	.get_victim = NumaCoolingGetVictim,
	.sync_start = NumaCoolingSyncStart,
	.notify_trickle = NumaClockNotifyTrickle,
	.trickle_iter_begin = NULL,
	.trickle_iter_next = NULL,
	.trickle_iter_end = NULL,
	.hint_vacuum = NULL,
	.reject_buffer = ClockRejectBuffer,
	.prefetch_hint = NULL,
	.shmem_size = ClockPoolShmemSize,
	.shmem_init = ClockPoolShmemInit,
	.shutdown = NULL,
	.scan_resistant = true,		/* probationary cool admission handles scans */
};


/* ----------------------------------------------------------------
 *			Clock-sweep implementation
 * ----------------------------------------------------------------
 */

/*
 * ClockSweepDefault -- default-pool clock sweep (setjmp-free, inlinable core)
 *
 * This is the byte-for-byte equivalent of upstream/master's StrategyGetBuffer
 * clock sweep: it uses the global NBuffers and &StrategyControl->nextVictimBuffer
 * directly (no struct-loaded locals, no per-tick is_dynamic_pool branch), so the
 * compiler optimizes the hot tick loop identically to upstream.
 *
 * The caller has already handled the strategy-ring / RECYCLE-pool / bgwriter /
 * numBufferAllocs preamble.  This helper only performs the victim sweep.
 *
 * Marked pg_attribute_always_inline so that when StrategyGetBuffer devirtualizes
 * the default clock (the overwhelmingly common case) it collapses to the same
 * tight inlined sweep upstream has.  Safe to force-inline: contains no
 * PG_TRY/setjmp.
 */
static pg_attribute_always_inline BufferDesc *
ClockSweepDefault(BufferAccessStrategy strategy, uint64 *buf_state)
{
	BufferDesc *buf;
	int			trycounter;

	/* Use the "clock sweep" algorithm to find a free buffer */
	trycounter = NBuffers;
	for (;;)
	{
		uint64		old_buf_state;
		uint64		local_buf_state;
		uint32		victim;

		/*
		 * Atomically advance the clock hand.  Byte-for-byte upstream
		 * ClockSweepTick(): global NBuffers, direct nextVictimBuffer, and
		 * completePasses maintenance on wraparound.
		 */
		victim = pg_atomic_fetch_add_u32(&StrategyControl->nextVictimBuffer, 1);

		if (victim >= NBuffers)
		{
			uint32		originalVictim = victim;

			victim = victim % NBuffers;

			if (victim == 0)
			{
				uint32		expected;
				uint32		wrapped;
				bool		success = false;

				expected = originalVictim + 1;

				while (!success)
				{
					SpinLockAcquire(&StrategyControl->buffer_strategy_lock);
					wrapped = expected % NBuffers;
					success = pg_atomic_compare_exchange_u32(&StrategyControl->nextVictimBuffer,
															 &expected, wrapped);
					if (success)
						StrategyControl->completePasses++;
					SpinLockRelease(&StrategyControl->buffer_strategy_lock);
				}
			}
		}

		buf = GetBufferDescriptor(victim);

		/*
		 * Check whether the buffer can be used and pin it if so. Do this
		 * using a CAS loop, to avoid having to lock the buffer header.
		 */
		old_buf_state = pg_atomic_read_u64(&buf->state);
		for (;;)
		{
			local_buf_state = old_buf_state;

			if (BUF_STATE_GET_REFCOUNT(local_buf_state) != 0)
			{
				if (--trycounter == 0)
					elog(ERROR, "no unpinned buffers available");
				break;
			}

			/* See equivalent code in PinBuffer() */
			if (unlikely(local_buf_state & BM_LOCKED))
			{
				old_buf_state = WaitBufHdrUnlocked(buf);
				continue;
			}

			if (BUF_STATE_GET_USAGECOUNT(local_buf_state) != 0)
			{
				local_buf_state -= BUF_USAGECOUNT_ONE;

				if (pg_atomic_compare_exchange_u64(&buf->state, &old_buf_state,
												   local_buf_state))
				{
					trycounter = NBuffers;
					break;
				}
			}
			else
			{
				/* pin the buffer if the CAS succeeds */
				local_buf_state += BUF_REFCOUNT_ONE;

				if (pg_atomic_compare_exchange_u64(&buf->state, &old_buf_state,
												   local_buf_state))
				{
					/* Found a usable buffer */
					if (strategy != NULL && strategy->recycle_pool == NULL)
						AddBufferToRing(strategy, buf);
					*buf_state = local_buf_state;

					TrackNewBufferPin(BufferDescriptorGetBuffer(buf));

					return buf;
				}
			}
		}
	}
}

/*
 * ClockGetVictimDefault -- default-pool preamble + sweep (setjmp-free)
 *
 * Handles the strategy-ring / RECYCLE-pool dispatch, bgwriter wakeup and
 * numBufferAllocs accounting for the default pool, then runs the inlined
 * ClockSweepDefault sweep.  Callable directly by StrategyGetBuffer to bypass
 * the vtable indirection for the built-in clock (see there), or via the
 * clock_pool_routine.get_victim vtable slot (ClockGetVictim below).
 */
static pg_attribute_always_inline BufferDesc *
ClockGetVictimDefault(BufferAccessStrategy strategy,
					  uint64 *buf_state,
					  bool *from_ring)
{
	BufferDesc *buf;

	*from_ring = false;

	/*
	 * If given a strategy object, see whether it can select a buffer. We
	 * assume strategy objects don't need buffer_strategy_lock.
	 *
	 * When the RECYCLE pool is enabled, dispatch through it instead of using
	 * the per-backend ring buffer.  The RECYCLE pool's get_victim returns a
	 * buffer from the shared RECYCLE pool, preventing scan and VACUUM
	 * operations from polluting the main buffer pool.
	 */
	if (strategy != NULL)
	{
		if (strategy->recycle_pool != NULL &&
			strategy->recycle_pool->bp_active)
		{
			PoolLocalState *local;

			local = EnsurePoolAttached(strategy->recycle_pool);
			buf = strategy->recycle_pool->bp_routine->get_victim(
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
			buf = GetBufferFromRing(strategy, buf_state);
			if (buf != NULL)
			{
				*from_ring = true;
				return buf;
			}
		}
	}

	/*
	 * Wake the bgwriter if asked.  See StrategyNotifyBgWriter().
	 */
	{
		int			bgwprocno = INT_ACCESS_ONCE(StrategyControl->bgwprocno);

		if (bgwprocno != -1)
		{
			StrategyControl->bgwprocno = -1;
			SetLatch(&GetPGProcByNumber(bgwprocno)->procLatch);
		}
	}

	pg_atomic_fetch_add_u32(&StrategyControl->numBufferAllocs, 1);

	return ClockSweepDefault(strategy, buf_state);
}

/*
 * ClockGetVictimDynamic -- dynamic-pool clock sweep
 *
 * Sweeps a dynamic pool's buffer range using its per-pool ClockPoolState.
 * The per-tick loop here has no is_dynamic_pool branch: it always does the
 * simple modulo into the pool's [first_buf_id, first_buf_id+nbuffers) range.
 */
static BufferDesc *
ClockGetVictimDynamic(ClockPoolState *pool_state,
					  BufferAccessStrategy strategy,
					  uint64 *buf_state,
					  bool *from_ring)
{
	BufferDesc *buf;
	int			trycounter;
	int			pool_nbuffers = pool_state->nbuffers;
	int			pool_first_buf = pool_state->first_buf_id;
	pg_atomic_uint32 *nextVictimPtr = &pool_state->nextVictimBuffer;

	*from_ring = false;

	/*
	 * Wake trickle writer if registered.  For dynamic pools the trickle
	 * writer is a per-pool background worker, not the global bgwriter.
	 */
	{
		int			bgwprocno = INT_ACCESS_ONCE(pool_state->bgwprocno);

		if (bgwprocno != -1)
		{
			pool_state->bgwprocno = -1;
			SetLatch(&GetPGProcByNumber(bgwprocno)->procLatch);
		}
	}

	pg_atomic_fetch_add_u32(&pool_state->numBufferAllocs, 1);

	/* Use the "clock sweep" algorithm to find a free buffer */
	trycounter = pool_nbuffers;
	for (;;)
	{
		uint64		old_buf_state;
		uint64		local_buf_state;
		uint32		victim_raw;
		uint32		victim_id;

		/* Dynamic pool: simple modulo, no completePasses tracking */
		victim_raw = pg_atomic_fetch_add_u32(nextVictimPtr, 1);
		victim_id = pool_first_buf + (victim_raw % pool_nbuffers);

		buf = GetBufferDescriptor(victim_id);

		/*
		 * Check whether the buffer can be used and pin it if so. Do this
		 * using a CAS loop, to avoid having to lock the buffer header.
		 */
		old_buf_state = pg_atomic_read_u64(&buf->state);
		for (;;)
		{
			local_buf_state = old_buf_state;

			if (BUF_STATE_GET_REFCOUNT(local_buf_state) != 0)
			{
				if (--trycounter == 0)
					elog(ERROR, "no unpinned buffers available");
				break;
			}

			/* See equivalent code in PinBuffer() */
			if (unlikely(local_buf_state & BM_LOCKED))
			{
				old_buf_state = WaitBufHdrUnlocked(buf);
				continue;
			}

			if (BUF_STATE_GET_USAGECOUNT(local_buf_state) != 0)
			{
				local_buf_state -= BUF_USAGECOUNT_ONE;

				if (pg_atomic_compare_exchange_u64(&buf->state, &old_buf_state,
												   local_buf_state))
				{
					trycounter = pool_nbuffers;
					break;
				}
			}
			else
			{
				/* pin the buffer if the CAS succeeds */
				local_buf_state += BUF_REFCOUNT_ONE;

				if (pg_atomic_compare_exchange_u64(&buf->state, &old_buf_state,
												   local_buf_state))
				{
					/* Found a usable buffer */
					if (strategy != NULL && strategy->recycle_pool == NULL)
						AddBufferToRing(strategy, buf);
					*buf_state = local_buf_state;

					TrackNewBufferPin(BufferDescriptorGetBuffer(buf));

					return buf;
				}
			}
		}
	}
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
 * BufferStrategyControl*) and we sweep through BufferDescriptors[0..NBuffers-1].
 * For dynamic pools, strategy_data points to a ClockPoolState stored in the
 * pool's DSM segment, and we sweep through that pool's buffer range.
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
	 * Determine whether we're sweeping the default pool or a dynamic pool.
	 * For the default pool, strategy_data == StrategyControl.  This branch is
	 * hoisted out of the per-tick loop: each path has a tick body with no
	 * is_dynamic_pool test.
	 */
	if (strategy_data == (void *) StrategyControl)
		return ClockGetVictimDefault(strategy, buf_state, from_ring);
	else
		return ClockGetVictimDynamic((ClockPoolState *) strategy_data,
									 strategy, buf_state, from_ring);
}

/*
 * ClockSyncStart -- clock-sweep implementation of sync_start
 *
 * Tell BgBufferSync where to start syncing.  Returns the buffer index of the
 * best buffer to sync first.  BgBufferSync() will proceed circularly around
 * the buffer array from there.
 *
 * In addition, we return the completed-pass count (which is effectively the
 * higher-order bits of nextVictimBuffer) and the count of recent buffer
 * allocs if non-NULL pointers are passed.  The alloc count is reset after
 * being read.
 */
static int
ClockSyncStart(void *strategy_data, uint32 *complete_passes, uint32 *num_buf_alloc)
{
	uint32		nextVictimBuffer;
	int			result;
	bool		is_dynamic_pool = (strategy_data != (void *) StrategyControl);

	if (is_dynamic_pool)
	{
		ClockPoolState *pool_state = (ClockPoolState *) strategy_data;

		SpinLockAcquire(&pool_state->lock);
		nextVictimBuffer = pg_atomic_read_u32(&pool_state->nextVictimBuffer);
		result = pool_state->first_buf_id +
			(nextVictimBuffer % pool_state->nbuffers);

		if (complete_passes)
			*complete_passes = pool_state->completePasses;

		if (num_buf_alloc)
			*num_buf_alloc = pg_atomic_exchange_u32(&pool_state->numBufferAllocs, 0);

		SpinLockRelease(&pool_state->lock);
	}
	else
	{
		SpinLockAcquire(&StrategyControl->buffer_strategy_lock);
		nextVictimBuffer = pg_atomic_read_u32(&StrategyControl->nextVictimBuffer);
		result = nextVictimBuffer % NBuffers;

		if (complete_passes)
		{
			*complete_passes = StrategyControl->completePasses;
			*complete_passes += nextVictimBuffer / NBuffers;
		}

		if (num_buf_alloc)
			*num_buf_alloc = pg_atomic_exchange_u32(&StrategyControl->numBufferAllocs, 0);

		SpinLockRelease(&StrategyControl->buffer_strategy_lock);
	}

	return result;
}

/*
 * ClockNotifyTrickle -- clock-sweep implementation of notify_trickle
 *
 * Set or clear allocation notification latch for the background writer.
 * If bgwprocno isn't -1, the next invocation of ClockGetVictim will set
 * that latch.  Pass -1 to clear the pending notification before it happens.
 */
static void
ClockNotifyTrickle(void *strategy_data, int bgwprocno)
{
	bool		is_dynamic_pool = (strategy_data != (void *) StrategyControl);

	if (is_dynamic_pool)
	{
		ClockPoolState *pool_state = (ClockPoolState *) strategy_data;

		SpinLockAcquire(&pool_state->lock);
		pool_state->bgwprocno = bgwprocno;
		SpinLockRelease(&pool_state->lock);
	}
	else
	{
		SpinLockAcquire(&StrategyControl->buffer_strategy_lock);
		StrategyControl->bgwprocno = bgwprocno;
		SpinLockRelease(&StrategyControl->buffer_strategy_lock);
	}
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
	state->completePasses = 0;
	pg_atomic_init_u32(&state->numBufferAllocs, 0);
	state->bgwprocno = -1;
	state->nbuffers = nbuffers;
	state->first_buf_id = first_buf_id;
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
	 * call the inlinable default-pool victim path directly, bypassing the
	 * vtable indirection.  Extensions, dynamic pools and numa_clock keep the
	 * indirect ActivePoolRoutine->get_victim path below.
	 */
	if (likely(ActivePoolIsClock))
		return ClockGetVictimDefault(strategy, buf_state, from_ring);

	return ActivePoolRoutine->get_victim(ActivePoolData, strategy,
										 buf_state, from_ring);
}

/*
 * StrategySyncStart -- tell BgBufferSync where to start syncing
 *
 * Dispatches to the active pool's sync_start callback.
 */
int
StrategySyncStart(uint32 *complete_passes, uint32 *num_buf_alloc)
{
	return ActivePoolRoutine->sync_start(ActivePoolData,
										 complete_passes, num_buf_alloc);
}

/*
 * StrategyNotifyBgWriter -- set or clear allocation notification latch
 *
 * Dispatches to the active pool's notify_trickle callback.
 */
void
StrategyNotifyBgWriter(int bgwprocno)
{
	ActivePoolRoutine->notify_trickle(ActivePoolData, bgwprocno);
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
					   .size = sizeof(BufferStrategyControl),
					   .ptr = (void **) &StrategyControl
		);
	ShmemRequestStruct(.name = "NUMA Clock Control",
					   .size = sizeof(NumaClockControl),
					   .alignment = PG_CACHE_LINE_SIZE,
					   .ptr = (void **) &NumaClockCtl
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
	SpinLockInit(&StrategyControl->buffer_strategy_lock);

	/* Initialize the clock-sweep pointer */
	pg_atomic_init_u32(&StrategyControl->nextVictimBuffer, 0);

	/* Clear statistics */
	StrategyControl->completePasses = 0;
	pg_atomic_init_u32(&StrategyControl->numBufferAllocs, 0);

	/* No pending notification */
	StrategyControl->bgwprocno = -1;

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
		 * An extension algorithm chosen as the DEFAULT pool keeps its state in
		 * the region reserved for it in StrategyCtlShmemRequest() (DefaultAlgoCtl),
		 * not in StrategyControl.  Point ActivePoolData there and let the
		 * algorithm initialize its own spinlocks/lists via shmem_init(), exactly
		 * like a dynamic pool does.  first_buf_id is 0 for the default pool.
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
		 * upgrade the DEFAULT pool to the NUMA-partitioned clock sweep: one
		 * clock hand per node, each confined to that node's buffer range.
		 * This directly targets the multi-socket scalability cliff (single
		 * global clock hand + cross-node victim traffic).  Extension-provided
		 * algorithms are left as-is -- they opt into NUMA via their own
		 * routine if they want it.
		 */
		if (routine == &clock_pool_routine && BufPoolNumaActive())
		{
			int			nnodes = BufPoolNumaNodes();
			int			nstripes = 1;

			/*
			 * When buffer_pool_numa_cooling is on, sub-divide each node range
			 * into per-core stripes so a buffer has a single sweeping owner per
			 * pass (see NumaCoolingGetVictim).  Cap at BUFPOOL_MAX_STRIPES.
			 */
			if (buffer_pool_numa_cooling)
				nstripes = BufPoolNumaCoresPerNode(BUFPOOL_MAX_STRIPES);

			SpinLockInit(&NumaClockCtl->lock);
			NumaClockCtl->nnodes = Min(nnodes, BUFPOOL_MAX_NUMA_NODES);
			NumaClockCtl->nbuffers = NBuffers;
			NumaClockCtl->nstripes = Max(1, Min(nstripes, BUFPOOL_MAX_STRIPES));
			/*
			 * Clock-sweep batch size (Jim Mlodgenski's design): a backend claims
			 * this many consecutive buffer IDs from the global hand per fetch_add,
			 * cutting the contended atomic traffic by ~this factor.  64 on
			 * multi-node hardware where the atomic crosses the interconnect;
			 * capped so a single claim cannot wrap the whole pool.
			 */
			NumaClockCtl->batchSize =
				(NumaClockCtl->nnodes > 1)
				? Min((uint32) CLOCK_SWEEP_BATCH_SIZE, (uint32) Max(1, NBuffers))
				: 1;
			NumaClockCtl->bgwprocno = -1;
			pg_atomic_init_u32(&NumaClockCtl->numBufferAllocs, 0);
			for (int n = 0; n < BUFPOOL_MAX_NUMA_NODES; n++)
			{
				pg_atomic_init_u32(&NumaClockCtl->nextVictim[n], 0);
				NumaClockCtl->completePasses[n] = 0;
				for (int s = 0; s < BUFPOOL_MAX_STRIPES; s++)
					pg_atomic_init_u32(&NumaClockCtl->stripeHand[n][s].hand, 0);
			}

			ActivePoolRoutine = buffer_pool_numa_cooling
				? &numa_cooling_pool_routine
				: &numa_clock_pool_routine;
			ActivePoolData = NumaClockCtl;

			/*
			 * Bind the default pool's buffer blocks and descriptors to nodes
			 * in matching contiguous chunks (buffer + its descriptor on the
			 * same node).  Best-effort placement; correctness is unaffected if
			 * the kernel ignores it.
			 */
			BufPoolNumaDistribute((char *) BufferBlocks,
								  (char *) BufferDescriptors,
								  sizeof(BufferDescPadded),
								  NBuffers);

			if (buffer_pool_numa_cooling)
				elog(LOG, "default buffer pool using batched global clock sweep with blind-atomic cooling: %d nodes, batch=%u",
					 NumaClockCtl->nnodes, NumaClockCtl->batchSize);
			else
				elog(LOG, "default buffer pool using NUMA-partitioned clock sweep across %d nodes",
					 NumaClockCtl->nnodes);
		}
		else
		{
			/*
			 * Report the resolved default-pool algorithm.  With no config this
			 * is always the built-in clock-sweep -- matching upstream/master
			 * behavior (clock for every relation, including TOAST).  Emitting
			 * it makes the default observable (and regression-testable): a
			 * zero-config cluster must log "clock-sweep".
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
		 * Cache whether the active algorithm is the plain built-in clock
		 * sweep, so StrategyGetBuffer() can call the default sweep directly
		 * instead of through the vtable.  Only the plain clock_pool_routine
		 * with StrategyControl as its data qualifies: numa_clock and dynamic
		 * pools keep the indirect path.
		 */
		ActivePoolIsClock = (ActivePoolRoutine == &clock_pool_routine &&
							 ActivePoolData == (void *) StrategyControl);

		/*
		 * A scan-resistant algorithm owns scan resistance through its own
		 * admission policy, so enable probationary (cool) admission for it.
		 * See ActivePoolProbationaryScan / InitialUsageCountBits.
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
	state->completePasses = 0;
	pg_atomic_init_u32(&state->numBufferAllocs, 0);
	state->bgwprocno = -1;
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
}			RecycleMode;

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
	uint32		completePasses;
	pg_atomic_uint32 numBufferAllocs;
	int			bgwprocno;		/* trickle writer procno (-1 = none) */
	int			nbuffers;
	int			first_buf_id;

	/* Per-mode allocation counts for monitoring */
	pg_atomic_uint64 bulkread_allocs;
	pg_atomic_uint64 bulkwrite_allocs;
	pg_atomic_uint64 vacuum_allocs;
}			RecyclePoolState;

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
}			RecycleTrickleIter;

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

	/* Wake trickle writer if registered */
	{
		int			bgwprocno = INT_ACCESS_ONCE(state->bgwprocno);

		if (bgwprocno != -1)
		{
			state->bgwprocno = -1;
			SetLatch(&GetPGProcByNumber(bgwprocno)->procLatch);
		}
	}

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

			if (BUF_STATE_GET_USAGECOUNT(local_buf_state) != 0)
			{
				/*
				 * One-chance: clear usage_count entirely instead of
				 * decrementing.  This gives each page exactly one sweep cycle
				 * to be re-accessed before eviction.
				 */
				local_buf_state &= ~BUF_USAGECOUNT_MASK;

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
	state->completePasses = 0;
	pg_atomic_init_u32(&state->numBufferAllocs, 0);
	state->bgwprocno = -1;
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
		*complete_passes = state->completePasses;
	SpinLockRelease(&state->lock);

	if (num_buf_alloc)
		*num_buf_alloc = pg_atomic_exchange_u32(&state->numBufferAllocs, 0);

	return result;
}

/*
 * RecycleNotifyTrickle -- register trickle writer for wakeup.
 */
static void
RecycleNotifyTrickle(void *strategy_data, int bgwprocno)
{
	RecyclePoolState *state = (RecyclePoolState *) strategy_data;

	state->bgwprocno = bgwprocno;
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
			BUF_STATE_GET_USAGECOUNT(buf_state) == 0)
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
			|| BUF_STATE_GET_USAGECOUNT(local_buf_state) > 1)
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
