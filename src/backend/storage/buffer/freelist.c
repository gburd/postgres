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

#include "commands/vacuum.h"
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
#include "utils/memutils.h"
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
} ClockPoolState;

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
 * Per-backend ring buffer state, indexed by intent.
 *
 * For BUF_INTENT_BULKREAD, BUF_INTENT_BULKWRITE, and BUF_INTENT_VACUUM the
 * buffer manager confines the work to a small set of reused buffers to
 * keep these one-shot or write-once accesses from polluting the cache.
 * When a RECYCLE pool is configured (recycle_pool_buffers > 0) that pool
 * provides the mechanism; otherwise this per-backend ring is the
 * fallback.  Allocated lazily on first use; freed at proc exit.
 */
typedef struct IntentRingBuffer
{
	int			nbuffers;		/* slots in this ring */
	int			current;		/* most recently returned index */
	Buffer		buffers[FLEXIBLE_ARRAY_MEMBER];
}			IntentRingBuffer;

#define BUF_INTENT_COUNT (BUF_INTENT_VACUUM + 1)

static IntentRingBuffer * intent_rings[BUF_INTENT_COUNT] =
{
	NULL
};

/*
 * Per-VACUUM-command override of the BUF_INTENT_VACUUM ring size.
 *
 * The VACUUM grammar's BUFFER_USAGE_LIMIT option overrides the
 * vacuum_buffer_usage_limit GUC for one command.  ExecVacuum sets this
 * before calling vacuum() and resets it on completion / error; parallel
 * vacuum workers receive the same value through pvs.bstrategy_ring_kb
 * and call SetVacuumIntentRingOverride() at start.  -1 means "use the
 * GUC".
 */
static int	vacuum_intent_ring_kb_override = -1;


/* Prototypes for internal functions */
static IntentRingBuffer * GetIntentRing(BufferAccessIntent intent);
static BufferDesc *GetBufferFromRing(IntentRingBuffer * ring,
									 uint64 *buf_state);
static void AddBufferToRing(IntentRingBuffer * ring, BufferDesc *buf);

/* Prototypes for clock-sweep vtable implementation */
static BufferDesc *ClockGetVictim(void *strategy_data, uint64 *buf_state);
static int	ClockSyncStart(void *strategy_data,
						   uint32 *complete_passes,
						   uint32 *num_buf_alloc);
static void ClockNotifyTrickle(void *strategy_data, int bgwprocno);
static Size ClockPoolShmemSize(int nbuffers);
static void ClockPoolShmemInit(void *strategy_data, int nbuffers,
							   int first_buf_id, bool init);


/* ----------------------------------------------------------------
 *			Clock-sweep buffer pool routine (vtable)
 * ----------------------------------------------------------------
 */

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
	.prefetch_hint = NULL,		/* clock-sweep doesn't use prefetch hints */
	.shmem_size = ClockPoolShmemSize,
	.shmem_init = ClockPoolShmemInit,
	.shutdown = NULL,
	.scan_resistant = false,	/* clock-sweep is scan-vulnerable */
};


/* ----------------------------------------------------------------
 *			Clock-sweep implementation
 * ----------------------------------------------------------------
 */

/*
 * ClockGetVictim -- clock-sweep implementation of get_victim
 *
 * Called by StrategyGetBuffer() via the vtable.  Selects the next candidate
 * buffer to use in GetVictimBuffer().  The only hard requirement is that the
 * selected buffer must not currently be pinned by anyone.
 *
 * strategy is a BufferAccessIntent object, or NULL for default strategy.
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
ClockGetVictim(void *strategy_data, uint64 *buf_state)
{
	BufferDesc *buf;
	int			trycounter;
	bool		is_dynamic_pool;
	int			pool_nbuffers;
	int			pool_first_buf;
	pg_atomic_uint32 *nextVictimPtr;

	/*
	 * Determine whether we're sweeping the default pool or a dynamic pool.
	 * For the default pool, strategy_data == StrategyControl.
	 */
	is_dynamic_pool = (strategy_data != (void *) StrategyControl);

	if (is_dynamic_pool)
	{
		ClockPoolState *pool_state = (ClockPoolState *) strategy_data;

		pool_nbuffers = pool_state->nbuffers;
		pool_first_buf = pool_state->first_buf_id;
		nextVictimPtr = &pool_state->nextVictimBuffer;

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
	}
	else
	{
		pool_nbuffers = NBuffers;
		pool_first_buf = 0;
		nextVictimPtr = &StrategyControl->nextVictimBuffer;

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
	}

	/* Use the "clock sweep" algorithm to find a free buffer */
	trycounter = pool_nbuffers;
	for (;;)
	{
		uint64		old_buf_state;
		uint64		local_buf_state;
		uint32		victim_raw;
		uint32		victim_id;

		/*
		 * Atomically advance the clock hand.  For the default pool this
		 * mirrors the old ClockSweepTick() logic including completePasses
		 * maintenance.  For dynamic pools we use a simpler modulo.
		 */
		victim_raw = pg_atomic_fetch_add_u32(nextVictimPtr, 1);

		if (!is_dynamic_pool)
		{
			/* Default pool: maintain completePasses on wraparound */
			victim_id = victim_raw % pool_nbuffers;

			if (victim_raw >= (uint32) pool_nbuffers && victim_id == 0)
			{
				uint32		expected;
				uint32		wrapped;
				bool		success = false;

				expected = victim_raw + 1;

				while (!success)
				{
					SpinLockAcquire(&StrategyControl->buffer_strategy_lock);
					wrapped = expected % pool_nbuffers;
					success = pg_atomic_compare_exchange_u32(
															 &StrategyControl->nextVictimBuffer,
															 &expected, wrapped);
					if (success)
						StrategyControl->completePasses++;
					SpinLockRelease(&StrategyControl->buffer_strategy_lock);
				}
			}
		}
		else
		{
			/* Dynamic pool: simple modulo, no completePasses tracking */
			victim_id = pool_first_buf + (victim_raw % pool_nbuffers);
		}

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
					/*
					 * Found a usable buffer.  Ring-buffer accounting (and the
					 * AddBufferToRing call) is now done by
					 * GetVictimWithStrategy() for every algorithm, so we
					 * don't need to do it here.
					 */
					*buf_state = local_buf_state;

					TrackNewBufferPin(BufferDescriptorGetBuffer(buf));

					return buf;
				}
			}
		}
	}
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
 * GetVictimWithStrategy -- common pre/post-dispatch for any pool.
 *
 * Handles the BufferAccessIntent decision in one place so every
 * replacement algorithm gets the same ring-buffer / RECYCLE-pool
 * semantics for free.  Algorithms' get_victim() callbacks are now free
 * to ignore the strategy parameter.
 *
 * Pre-dispatch:
 *   - strategy with recycle_pool: hand the request to the RECYCLE pool,
 *     return *from_ring = true on success.
 *   - strategy with a per-backend ring (nbuffers > 0): try
 *     GetBufferFromRing first; on hit, return *from_ring = true.
 *
 * If neither pre-dispatch returned a buffer, dispatch to the chosen
 * algorithm's get_victim().  Algorithm callbacks set *from_ring = false.
 *
 * Post-dispatch:
 *   - strategy with a per-backend ring and no recycle_pool: AddBufferToRing
 *     so subsequent calls in this same strategy reuse the same N buffers.
 *
 * routine / strategy_data identify which pool's algorithm to call: the
 * default pool passes &clock_pool_routine + StrategyControl, dynamic
 * pools pass pool->bp_routine + the pool's strategy_data.
 */
static BufferDesc *
GetVictimWithStrategy(BufferAccessIntent intent,
					  const BufferPoolRoutine *routine,
					  void *strategy_data,
					  uint64 *buf_state,
					  bool *from_ring)
{
	BufferDesc *buf;
	BufferPoolDesc *recycle;
	IntentRingBuffer *ring;

	*from_ring = false;

	/*
	 * For non-NORMAL intents, try the RECYCLE pool first if configured.
	 * Scan-resistant algorithms decline to route BAS_BULKREAD through RECYCLE
	 * -- they protect themselves -- so the
	 * GetAccessStrategyShouldUseRecycle() check considers the active
	 * algorithm's scan_resistant flag.
	 */
	if (intent != BUF_INTENT_NORMAL)
	{
		recycle = GetBufferPoolByKind(BUFPOOL_RECYCLE);
		if (recycle != NULL && recycle->bp_active &&
			!(intent == BUF_INTENT_BULKREAD &&
			  ActivePoolRoutine != NULL &&
			  ActivePoolRoutine->scan_resistant))
		{
			PoolLocalState *local = EnsurePoolAttached(recycle);

			buf = recycle->bp_routine->get_victim(local->strategy_data,
												  buf_state);
			if (buf != NULL)
			{
				*from_ring = true;
				return buf;
			}
			/* Fall through if RECYCLE pool failed. */
		}

		/*
		 * No RECYCLE pool (or scan-resistant skip): use a per-backend ring
		 * buffer for this intent.
		 */
		ring = GetIntentRing(intent);
		if (ring != NULL)
		{
			buf = GetBufferFromRing(ring, buf_state);
			if (buf != NULL)
			{
				*from_ring = true;
				return buf;
			}
		}
	}
	else
		ring = NULL;

	buf = routine->get_victim(strategy_data, buf_state);

	if (buf != NULL && ring != NULL)
		AddBufferToRing(ring, buf);

	return buf;
}

/*
 * StrategyGetBuffer
 *
 *	Called by the bufmgr to get the next candidate buffer to use in
 *	GetVictimBuffer().  This is the default-pool entry point; dynamic
 *	pools call GetPoolVictim() instead.
 */
BufferDesc *
StrategyGetBuffer(BufferAccessIntent intent, uint64 *buf_state, bool *from_ring)
{
	return GetVictimWithStrategy(intent, ActivePoolRoutine, ActivePoolData,
								 buf_state, from_ring);
}

/*
 * GetPoolVictim -- victim selection for a non-default (dynamic) pool.
 *
 * Wraps the pool's algorithm with the same strategy / ring / RECYCLE
 * pre and post-dispatch logic StrategyGetBuffer applies to the default
 * pool, so dynamic pools get correct ring-buffer accounting too.
 */
BufferDesc *
GetPoolVictim(BufferPoolDesc *pool, void *strategy_data,
			  BufferAccessIntent intent,
			  uint64 *buf_state, bool *from_ring)
{
	return GetVictimWithStrategy(intent, pool->bp_routine, strategy_data,
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
 * StrategyRejectBuffer -- decide whether to skip a dirty victim buffer
 *
 * Called from GetVictimBuffer when a victim came from the per-backend
 * ring buffer (from_ring = true) and writing it out would require a
 * WAL flush.  For BUF_INTENT_BULKREAD we'd rather grow the ring than
 * pay the WAL flush, so we drop the dirty buffer from the ring slot
 * and have the caller pick a fresh victim from the algorithm.  For
 * other intents the framework just writes the buffer.
 *
 * The reject decision is now framework-internal -- algorithms no
 * longer implement reject_buffer; the per-backend ring is owned by
 * the framework, not the algorithm.
 */
bool
StrategyRejectBuffer(BufferAccessIntent intent, BufferDesc *buf, bool from_ring)
{
	IntentRingBuffer *ring;

	if (intent != BUF_INTENT_BULKREAD || !from_ring)
		return false;

	ring = intent_rings[intent];
	if (ring == NULL)
		return false;

	if (ring->buffers[ring->current] != BufferDescriptorGetBuffer(buf))
		return false;

	/*
	 * Remove the dirty buffer from the ring; necessary to prevent infinite
	 * loop if all ring members are dirty.
	 */
	ring->buffers[ring->current] = InvalidBuffer;
	return true;
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
 * StrategyCtlShmemRequest -- request shared memory for the buffer
 *		cache replacement strategy.
 *
 * The size of the strategy region depends on which algorithm the
 * buffer_pool_algorithm GUC names: clock-sweep uses BufferStrategyControl;
 * an extension-provided algorithm uses whatever its shmem_size() callback
 * returns.  Extensions register themselves from _PG_init() before the
 * shmem callbacks fire, so by the time we are called the registry is
 * populated and we can ask the chosen algorithm how much it needs.
 */
static void
StrategyCtlShmemRequest(void *arg)
{
	const char *name = buffer_pool_algorithm ? buffer_pool_algorithm
		: BP_ALGO_CLOCK_NAME;
	const BufferPoolRoutine *routine = LookupDefaultPoolAlgorithm(name);
	Size		strategy_size;

	if (routine == NULL || routine == &clock_pool_routine)
		strategy_size = sizeof(BufferStrategyControl);
	else
		strategy_size = routine->shmem_size(NBuffers);

	ShmemRequestStruct(.name = "Buffer Strategy Status",
					   .size = strategy_size,
					   .ptr = (void **) &StrategyControl
		);
}

/*
 * StrategyCtlShmemInit -- initialize the buffer cache replacement strategy.
 *
 * For clock-sweep we initialize BufferStrategyControl in place.  For an
 * extension-provided algorithm we hand the region to its shmem_init()
 * and let the algorithm own the layout.  In both cases ActivePoolData is
 * set to point at the region, so dispatch through the vtable lands on
 * the right state.
 */
static void
StrategyCtlShmemInit(void *arg)
{
	const char *name = buffer_pool_algorithm ? buffer_pool_algorithm
		: BP_ALGO_CLOCK_NAME;
	const BufferPoolRoutine *routine = LookupDefaultPoolAlgorithm(name);

	if (routine == NULL)
	{
		ereport(WARNING,
				(errmsg("buffer pool algorithm \"%s\" is not registered, using \"%s\"",
						name, BP_ALGO_CLOCK_NAME),
				 errhint("Load the providing extension via shared_preload_libraries.")));
		routine = &clock_pool_routine;
	}

	if (routine == &clock_pool_routine)
	{
		/*
		 * Built-in clock-sweep operates on BufferStrategyControl directly
		 * (the historical layout); initialize its fields here rather than
		 * routing through ClockPoolShmemInit, which is for ClockPoolState in
		 * dynamic pools.
		 */
		SpinLockInit(&StrategyControl->buffer_strategy_lock);
		pg_atomic_init_u32(&StrategyControl->nextVictimBuffer, 0);
		StrategyControl->completePasses = 0;
		pg_atomic_init_u32(&StrategyControl->numBufferAllocs, 0);
		StrategyControl->bgwprocno = -1;
	}
	else
	{
		/*
		 * Extension algorithm: hand the shmem region to its shmem_init(). The
		 * DEFAULT pool covers the whole BufferDescriptors[] array, so pass
		 * nbuffers = NBuffers and first_buf_id = 0.
		 */
		if (routine->shmem_init != NULL)
			routine->shmem_init((void *) StrategyControl, NBuffers, 0, true);
	}

	ActivePoolRoutine = routine;
	ActivePoolData = StrategyControl;
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
PG_FUNCTION_INFO_V1(clock_pool_handler);

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
KeepGetVictim(void *strategy_data, uint64 *buf_state)
{
	ClockPoolState *state = (ClockPoolState *) strategy_data;
	int			nbuffers = state->nbuffers;
	int			first_buf_id = state->first_buf_id;


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
	.prefetch_hint = NULL,
	.shmem_size = KeepPoolShmemSize,
	.shmem_init = KeepPoolShmemInit,
	.shutdown = NULL,
	.scan_resistant = false,	/* KEEP never evicts; scan-resistance N/A */
};

/*
 * keep_pool_handler -- SQL-callable handler returning the KEEP BufferPoolRoutine.
 */
PG_FUNCTION_INFO_V1(keep_pool_handler);

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
	uint32		completePasses;
	pg_atomic_uint32 numBufferAllocs;
	int			bgwprocno;		/* trickle writer procno (-1 = none) */
	int			nbuffers;
	int			first_buf_id;

	/* Per-mode allocation counts for monitoring */
	pg_atomic_uint64 bulkread_allocs;
	pg_atomic_uint64 bulkwrite_allocs;
	pg_atomic_uint64 vacuum_allocs;
} RecyclePoolState;

/* Forward declarations */
static BufferDesc *RecycleGetVictim(void *strategy_data, uint64 *buf_state);
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
RecycleGetVictim(void *strategy_data, uint64 *buf_state)
{
	RecyclePoolState *state = (RecyclePoolState *) strategy_data;
	BufferDesc *buf;
	int			trycounter;


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
	.prefetch_hint = NULL,
	.shmem_size = RecycleShmemSize,
	.shmem_init = RecycleShmemInit,
	.shutdown = NULL,
	.scan_resistant = false,	/* RECYCLE *is* the scan-isolation mechanism;
								 * flag is N/A but report false so it's never
								 * queried as a candidate to skip itself */
};


/* ----------------------------------------------------------------
 *			Backend-private buffer ring management
 * ----------------------------------------------------------------
 *
 * For BUF_INTENT_BULKREAD / BUF_INTENT_BULKWRITE / BUF_INTENT_VACUUM the
 * buffer manager confines the work to a small set of reused buffers so
 * one-shot or write-once accesses do not pollute the cache.  When a
 * RECYCLE pool is configured the framework dispatches there; otherwise
 * a per-backend ring buffer (allocated lazily in TopMemoryContext) is
 * used.  See GetVictimWithStrategy() above for the dispatch.
 *
 * Sizing per intent:
 *   BUF_INTENT_BULKREAD   256 kB (fixed; small enough to fit in L2)
 *   BUF_INTENT_BULKWRITE  16 MB (or shared_buffers/8 if smaller)
 *   BUF_INTENT_VACUUM     vacuum_buffer_usage_limit GUC; overridable
 *                         per-command via SetVacuumIntentRingOverride().
 *
 * The overall ring size is capped at NBuffers/8 so a single backend
 * can never starve the rest of the cache.
 */

/*
 * Default ring sizes in kilobytes.  Match the values that were used by
 * the previous BufferAccessStrategy implementation in upstream master.
 */
#define BULKREAD_RING_SIZE_KB	256
#define BULKWRITE_RING_SIZE_KB	(16 * 1024)

/*
 * IntentRingBufferKB
 *		Returns the configured ring size in KB for the given intent.
 *		Reads vacuum_buffer_usage_limit (or its per-VACUUM override)
 *		for BUF_INTENT_VACUUM, returns the fixed default for the
 *		others, returns 0 for BUF_INTENT_NORMAL (no ring).
 */
static int
IntentRingBufferKB(BufferAccessIntent intent)
{
	switch (intent)
	{
		case BUF_INTENT_NORMAL:
			return 0;
		case BUF_INTENT_BULKREAD:
			return BULKREAD_RING_SIZE_KB;
		case BUF_INTENT_BULKWRITE:
			return BULKWRITE_RING_SIZE_KB;
		case BUF_INTENT_VACUUM:
			if (vacuum_intent_ring_kb_override >= 0)
				return vacuum_intent_ring_kb_override;
			return VacuumBufferUsageLimit;
	}
	return 0;
}

/*
 * IntentRingBufferCount
 *		Public: returns the configured ring size in pages for the given
 *		intent.  Used by parallel VACUUM (vacuumparallel.c) to size the
 *		shared ring count workers will use.  Returns 0 if no ring is
 *		configured for this intent.
 */
int
IntentRingBufferCount(BufferAccessIntent intent)
{
	int			kb = IntentRingBufferKB(intent);
	int			ring_buffers;

	if (kb <= 0)
		return 0;
	ring_buffers = kb / (BLCKSZ / 1024);
	if (ring_buffers <= 0)
		return 0;
	if (ring_buffers > NBuffers / 8)
		ring_buffers = NBuffers / 8;
	return ring_buffers;
}

/*
 * IntentPinLimit
 *		Public: how many buffers a backend may have pinned at once for
 *		this intent.  Bulk operations are bounded by their ring size
 *		minus a small reserve so the next victim selection isn't
 *		instantly OOM.  Read by read_stream.c when sizing async pin
 *		windows.
 */
int
IntentPinLimit(BufferAccessIntent intent)
{
	int			ring_pages;

	if (intent == BUF_INTENT_NORMAL)
		return NBuffers;

	ring_pages = IntentRingBufferCount(intent);
	if (ring_pages <= 0)
		return NBuffers;
	if (ring_pages <= 1)
		return 1;
	return ring_pages - 1;
}

/*
 * SetVacuumIntentRingOverride
 *		VACUUM's BUFFER_USAGE_LIMIT command option overrides
 *		vacuum_buffer_usage_limit for one command.  ExecVacuum sets the
 *		override before calling vacuum() and clears it after; parallel
 *		workers receive the value through ParallelVacuumState->shared
 *		->ring_nbuffers and call SetVacuumIntentRingOverride at start.
 *
 *		Pass kb < 0 to clear the override and fall back to the GUC.
 */
void
SetVacuumIntentRingOverride(int kb)
{
	vacuum_intent_ring_kb_override = kb;

	/*
	 * If a previously-allocated ring no longer matches, drop it so the next
	 * allocation rebuilds at the new size.  This is rare (only on VACUUM end
	 * / parallel-worker startup) so the cost is irrelevant.
	 */
	if (intent_rings[BUF_INTENT_VACUUM] != NULL)
	{
		int			want = IntentRingBufferCount(BUF_INTENT_VACUUM);

		if (intent_rings[BUF_INTENT_VACUUM]->nbuffers != want)
		{
			pfree(intent_rings[BUF_INTENT_VACUUM]);
			intent_rings[BUF_INTENT_VACUUM] = NULL;
		}
	}
}

/*
 * GetIntentRing
 *		Internal: returns the per-backend ring for this intent, allocating
 *		it lazily on first use.  Returns NULL when the intent has no ring
 *		(BUF_INTENT_NORMAL, or any intent whose configured size is 0).
 */
static IntentRingBuffer *
GetIntentRing(BufferAccessIntent intent)
{
	IntentRingBuffer *ring;
	int			ring_buffers;

	Assert(intent >= 0 && intent < BUF_INTENT_COUNT);

	if (intent == BUF_INTENT_NORMAL)
		return NULL;

	ring = intent_rings[intent];
	if (ring != NULL)
		return ring;

	ring_buffers = IntentRingBufferCount(intent);
	if (ring_buffers <= 0)
		return NULL;

	ring = MemoryContextAllocZero(TopMemoryContext,
								  offsetof(IntentRingBuffer, buffers) +
								  ring_buffers * sizeof(Buffer));
	ring->nbuffers = ring_buffers;
	ring->current = 0;
	intent_rings[intent] = ring;
	return ring;
}

/*
 * AtProcExit_IntentRings
 *		Free per-backend ring state at proc exit.  Called from buf_init.
 */
void
AtProcExit_IntentRings(void)
{
	for (int i = 0; i < BUF_INTENT_COUNT; i++)
	{
		if (intent_rings[i] != NULL)
		{
			pfree(intent_rings[i]);
			intent_rings[i] = NULL;
		}
	}
}

/*
 * GetBufferFromRing -- returns a buffer from the ring, or NULL if the ring
 *		is empty / the slot's buffer is no longer suitable.
 *
 * The bufmgr interaction with this function is much like StrategyGetBuffer
 * was on master: examine the buffer at "current" position; if it's a
 * candidate (not pinned, usage_count == 0), return it pinned; otherwise
 * leave the slot empty so the caller falls through to the algorithm,
 * which will populate the slot via AddBufferToRing.
 */
static BufferDesc *
GetBufferFromRing(IntentRingBuffer * ring, uint64 *buf_state)
{
	BufferDesc *buf;
	Buffer		bufnum;
	uint64		old_buf_state;
	uint64		local_buf_state;

	/* Advance to next slot, wrapping at the end of the ring. */
	if (++ring->current >= ring->nbuffers)
		ring->current = 0;

	/*
	 * If the slot hasn't been filled yet, tell the caller to allocate a new
	 * buffer with the normal allocation algorithm.  It'll then fill this slot
	 * by calling AddBufferToRing with the new buffer.
	 */
	bufnum = ring->buffers[ring->current];
	if (bufnum == InvalidBuffer)
		return NULL;

	/*
	 * If the buffer is pinned we cannot use it under any circumstances.
	 *
	 * If usage_count is 0 or 1 then the buffer is fair game (we expect 1,
	 * since our own previous usage count increment).  The buffer might be
	 * entirely free now, in which case it's been reused already.  In either
	 * case, drop the slot so the caller picks a fresh buffer.
	 */
	buf = GetBufferDescriptor(bufnum - 1);
	old_buf_state = pg_atomic_read_u64(&buf->state);
	for (;;)
	{
		local_buf_state = old_buf_state;
		if (BUF_STATE_GET_REFCOUNT(local_buf_state) != 0)
			return NULL;
		if (unlikely(local_buf_state & BM_LOCKED))
		{
			old_buf_state = WaitBufHdrUnlocked(buf);
			continue;
		}
		if (BUF_STATE_GET_USAGECOUNT(local_buf_state) > 1)
			return NULL;

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

/*
 * AddBufferToRing -- replace the current ring slot with the given buffer.
 *
 * The ring's "current" position was advanced by GetBufferFromRing before
 * the algorithm picked a fresh victim.  Once the framework gets a victim
 * back from the algorithm it calls this to record which buffer occupies
 * the slot for the next ring revolution.
 */
static void
AddBufferToRing(IntentRingBuffer * ring, BufferDesc *buf)
{
	ring->buffers[ring->current] = BufferDescriptorGetBuffer(buf);
}

/*
 * IOContextForStrategy
 *		Map a BufferAccessIntent to its IOContext for pgstat purposes.
 */
IOContext
IOContextForStrategy(BufferAccessIntent intent)
{
	switch (intent)
	{
		case BUF_INTENT_NORMAL:
			return IOCONTEXT_NORMAL;
		case BUF_INTENT_BULKREAD:
			return IOCONTEXT_BULKREAD;
		case BUF_INTENT_BULKWRITE:
			return IOCONTEXT_BULKWRITE;
		case BUF_INTENT_VACUUM:
			return IOCONTEXT_VACUUM;
	}
	elog(ERROR, "unrecognized BufferAccessIntent: %d", (int) intent);
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
