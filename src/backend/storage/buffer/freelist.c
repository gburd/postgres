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
#include "storage/proc.h"
#include "storage/shmem.h"
#include "storage/subsystems.h"
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
	 * Array of buffer numbers.  InvalidBuffer (that is, zero) indicates we
	 * have not yet selected a buffer for this ring slot.  For allocation
	 * simplicity this is palloc'd together with the fixed fields of the
	 * struct.
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
	BufferDesc *buf;
	int			trycounter;
	bool		is_dynamic_pool;
	int			pool_nbuffers;
	int			pool_first_buf;
	pg_atomic_uint32 *nextVictimPtr;

	*from_ring = false;

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
		 * If given a strategy object, see whether it can select a buffer. We
		 * assume strategy objects don't need buffer_strategy_lock. Ring
		 * buffers are only supported for the default pool.
		 */
		if (strategy != NULL)
		{
			buf = GetBufferFromRing(strategy, buf_state);
			if (buf != NULL)
			{
				*from_ring = true;
				return buf;
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
					/* Found a usable buffer */
					if (strategy != NULL)
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
 * StrategyCtlShmemRequest -- request shared memory for the buffer
 *		cache replacement strategy.
 */
static void
StrategyCtlShmemRequest(void *arg)
{
	ShmemRequestStruct(.name = "Buffer Strategy Status",
					   .size = sizeof(BufferStrategyControl),
					   .ptr = (void **) &StrategyControl
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
		ActivePoolRoutine = routine;
		ActivePoolData = StrategyControl;
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
PG_FUNCTION_INFO_V1(clock_pool_handler);

Datum
clock_pool_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&clock_pool_routine);
}


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

	Assert(ring_size_kb >= 0);

	/* Figure out how many buffers ring_size_kb is */
	ring_buffers = ring_size_kb / (BLCKSZ / 1024);

	/* 0 means unlimited, so no BufferAccessStrategy required */
	if (ring_buffers == 0)
		return NULL;

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
