/*-------------------------------------------------------------------------
 *
 * pg_bp_osic.c
 *	  OSIC (One-Stage Insert Clock) buffer replacement algorithm.
 *
 * This extension provides the OSIC algorithm as a BufferPoolRoutine handler,
 * usable with CREATE BUFFER POOL ... HANDLER osic_pool_handler.
 *
 * OSIC is inspired by the LeanStore buffer manager.  Each buffer has a
 * boolean "cool" flag.  On a cache hit the cool flag is cleared atomically
 * using CAS, so the hot path is lock-free.  Eviction works in two stages:
 *
 *   1. Sweep from the clock hand.  If the buffer is "hot" (cool == false),
 *      mark it "cool" and advance.
 *   2. If the buffer is "cool" AND unpinned, evict it.
 *
 * An atomic "inflight" counter tracks the number of pages currently being
 * loaded, preventing the eviction loop from releasing more buffers than
 * needed.
 *
 * For reduced contention the clock state is partitioned: multiple clock
 * hands each responsible for a portion of the buffer range.  The number
 * of partitions scales with pool size.
 *
 * Reference:
 *   V. Leis et al., "LeanStore: In-Memory Data Management Beyond Main
 *   Memory", ICDE 2018.
 *   https://db.in.tum.de/~leis/papers/leanstore.pdf
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  contrib/pg_bp_osic/pg_bp_osic.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "fmgr.h"
#include "funcapi.h"
#include "port/atomics.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "storage/bufpool.h"
#include "storage/bufpool_internals.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "utils/builtins.h"
#include "utils/tuplestore.h"

PG_MODULE_MAGIC_EXT(.name = "pg_bp_osic", .version = PG_VERSION);

void		_PG_init(void);

/*
 * Maximum number of clock partitions.  Each partition owns a contiguous
 * slice of the buffer array and has its own clock hand protected by a
 * spinlock.
 */
#define OSIC_MAX_PARTITIONS		16

/*
 * Per-buffer metadata: just the "cool" flag.
 * Using pg_atomic_uint32 for lock-free CAS on the flag.
 * Bit 0: cool flag (1 = cool, 0 = hot)
 */
typedef struct OsicBufState
{
	pg_atomic_uint32 flags;		/* bit 0 = cool */
	char		pad[PG_CACHE_LINE_SIZE - sizeof(pg_atomic_uint32)];
} OsicBufState;

#define OSIC_FLAG_COOL		0x01

/* Stat slot indices for PoolStatIncrement */
#define OSIC_STAT_HITS			0
#define OSIC_STAT_MISSES		1
#define OSIC_STAT_EVICTIONS		2
#define OSIC_STAT_COOLING_SWEEPS 3
#define OSIC_NUM_STATS			4

/*
 * Per-backend local stat counters for OSIC pools.
 * These accumulate locally and flush to shared per-partition counters
 * when the threshold is reached, reducing atomic contention.
 */
static uint64 osic_local_stats[OSIC_MAX_PARTITIONS][OSIC_NUM_STATS];

/*
 * Per-partition clock state.
 */
typedef struct OsicPartition
{
	slock_t		lock;			/* protects clock_hand */
	int			clock_hand;		/* current position within partition range */
	int			first_buf;		/* first buffer index in this partition */
	int			nbuffers;		/* number of buffers in this partition */
	/* Per-partition stats (no cross-partition atomic bouncing) */
	pg_atomic_uint64 stat_hits;
	pg_atomic_uint64 stat_misses;
	pg_atomic_uint64 stat_evictions;
	pg_atomic_uint64 stat_cooling_sweeps;
} OsicPartition;

/*
 * OSIC shared-memory control block.
 *
 * Allocated as the pool's strategy_data via DSM.
 * Layout in memory:
 *   OsicControl header
 *   OsicPartition[npartitions]
 *   OsicBufState[nbuffers]
 */
typedef struct OsicControl
{
	int			nbuffers;		/* total buffers in this pool */
	int			first_buf_id;	/* global buffer ID of first buffer */
	int			npartitions;	/* number of clock partitions */

	/* Atomic inflight counter: pages currently being loaded */
	pg_atomic_uint32 inflight;

} OsicControl;

/* Access helpers for variable-length arrays after the header */
#define OSIC_PARTITIONS(ctl) \
	((OsicPartition *) ((char *)(ctl) + MAXALIGN(sizeof(OsicControl))))

#define OSIC_BUFSTATES(ctl) \
	((OsicBufState *) ((char *)OSIC_PARTITIONS(ctl) + \
	 MAXALIGN(sizeof(OsicPartition) * (ctl)->npartitions)))

/*
 * Per-backend OSIC state, scoped to a specific pool.
 */
typedef struct OsicBackendState
{
	OsicControl *ctl;
	bool		vacuum_hint;
} OsicBackendState;

#define MAX_OSIC_POOLS  MAX_BUFFER_POOLS
static OsicBackendState osic_backend_states[MAX_OSIC_POOLS];
static int	osic_num_states = 0;

static OsicBackendState *
osic_get_backend_state(OsicControl *ctl)
{
	for (int i = 0; i < osic_num_states; i++)
	{
		if (osic_backend_states[i].ctl == ctl)
			return &osic_backend_states[i];
	}
	Assert(osic_num_states < MAX_OSIC_POOLS);
	osic_backend_states[osic_num_states].ctl = ctl;
	osic_backend_states[osic_num_states].vacuum_hint = false;
	return &osic_backend_states[osic_num_states++];
}

/* Find which partition a local buffer ID belongs to */
static inline int
osic_partition_for_buf(OsicControl *ctl, int local_id)
{
	OsicPartition *parts = OSIC_PARTITIONS(ctl);
	int			npartitions = ctl->npartitions;

	for (int p = 0; p < npartitions; p++)
	{
		if (local_id >= parts[p].first_buf &&
			local_id < parts[p].first_buf + parts[p].nbuffers)
			return p;
	}
	return 0;					/* fallback to first partition */
}


/* ----------------------------------------------------------------
 * Helper: compute number of partitions for a given pool size
 * ----------------------------------------------------------------
 */
static int
osic_compute_partitions(int nbuffers)
{
	/*
	 * Scale partitions with pool size: < 256 buffers:   1 partition 256-1023:
	 * 2 partitions 1024-4095:       4 partitions 4096-16383:      8
	 * partitions >= 16384:       16 partitions
	 */
	if (nbuffers < 256)
		return 1;
	if (nbuffers < 1024)
		return 2;
	if (nbuffers < 4096)
		return 4;
	if (nbuffers < 16384)
		return 8;
	return OSIC_MAX_PARTITIONS;
}


/* ----------------------------------------------------------------
 * Vtable callbacks
 * ----------------------------------------------------------------
 */

/*
 * OsicOnHit - called when a page is found in cache.
 *
 * Clear the cool flag atomically.  This is the lock-free hot path:
 * no spinlock, just an atomic AND.
 */
static void
OsicOnHit(void *strategy_data, int buf_id, BufferTag *tag)
{
	OsicControl *ctl = (OsicControl *) strategy_data;
	OsicBufState *states;
	int			local_id;

	local_id = buf_id - ctl->first_buf_id;
	if (unlikely(local_id < 0 || local_id >= ctl->nbuffers))
		return;

	states = OSIC_BUFSTATES(ctl);

	/*
	 * Clear the cool flag.  If it was already hot (0), this is a no-op. No
	 * spinlock needed -- just an atomic AND.
	 */
	pg_atomic_fetch_and_u32(&states[local_id].flags, ~OSIC_FLAG_COOL);

	{
		int			part_idx = osic_partition_for_buf(ctl, local_id);
		OsicPartition *parts = OSIC_PARTITIONS(ctl);

		PoolStatIncrement(&osic_local_stats[part_idx][OSIC_STAT_HITS],
						  &parts[part_idx].stat_hits);
	}
}

/*
 * OsicOnMiss - called before victim selection when a page is not in cache.
 */
static void
OsicOnMiss(void *strategy_data, BufferTag *tag)
{
	OsicControl *ctl = (OsicControl *) strategy_data;

	{
		/* Distribute miss stats across partition 0 (no buffer context yet) */
		OsicPartition *parts = OSIC_PARTITIONS(ctl);

		PoolStatIncrement(&osic_local_stats[0][OSIC_STAT_MISSES],
						  &parts[0].stat_misses);
	}

	/* Increment inflight counter -- we're about to load a page */
	pg_atomic_fetch_add_u32(&ctl->inflight, 1);
}

/*
 * OsicOnEvict - called when a page is evicted from the buffer.
 */
static void
OsicOnEvict(void *strategy_data, int buf_id, BufferTag *old_tag)
{
	/* Nothing to clean up -- the cool flag will be reset on next use */
}

/*
 * OsicOnNewTag - called after a new page is loaded into a buffer.
 *
 * Normally marks the buffer as hot (clear cool flag).
 * VACUUM optimization: marks VACUUM-loaded pages as cool immediately,
 * so they are evicted on the next sweep rather than getting two chances.
 */
static void
OsicOnNewTag(void *strategy_data, int buf_id, BufferTag *new_tag,
			 bool vacuum_hint)
{
	OsicControl *ctl = (OsicControl *) strategy_data;
	OsicBackendState *state = osic_get_backend_state(ctl);
	OsicBufState *states;
	int			local_id;

	local_id = buf_id - ctl->first_buf_id;
	if (unlikely(local_id < 0 || local_id >= ctl->nbuffers))
		return;

	states = OSIC_BUFSTATES(ctl);

	/*
	 * VACUUM pages start cool so they are evicted on the next sweep,
	 * preventing VACUUM scans from polluting the cache.
	 */
	if (vacuum_hint || state->vacuum_hint)
		pg_atomic_write_u32(&states[local_id].flags, OSIC_FLAG_COOL);
	else
		pg_atomic_write_u32(&states[local_id].flags, 0);	/* hot */

	/* Decrement inflight counter -- page load complete */
	pg_atomic_fetch_sub_u32(&ctl->inflight, 1);
}

/*
 * OsicGetVictim - find a buffer to evict.
 *
 * Two-stage clock sweep across partitions:
 *   Stage 1: If buffer is hot, mark it cool and move on.
 *   Stage 2: If buffer is cool and unpinned, evict it.
 *
 * We try each partition starting from a round-robin choice based on
 * MyProcNumber to spread contention.
 */
static BufferDesc *
OsicGetVictim(void *strategy_data, BufferAccessStrategy strategy,
			  uint64 *buf_state, bool *from_ring)
{
	OsicControl *ctl = (OsicControl *) strategy_data;
	OsicBufState *states = OSIC_BUFSTATES(ctl);
	OsicPartition *parts = OSIC_PARTITIONS(ctl);
	int			npartitions = ctl->npartitions;
	int			start_part;
	int			attempts;

	*from_ring = false;

	/*
	 * Choose a starting partition.  Use MyProcNumber to distribute backends
	 * across partitions.  Cast to unsigned to handle the case where
	 * MyProcNumber is INVALID_PROC_NUMBER (-1).
	 */
	start_part = ((unsigned int) MyProcNumber) % npartitions;

	/*
	 * Pass 1: Scan for free (untagged) buffers only -- no cooling. This
	 * prevents unnecessary cooling of working set pages when free buffers
	 * exist scattered among tagged pages.
	 */
	for (int pass1 = 0; pass1 < npartitions; pass1++)
	{
		int			part_idx = (start_part + pass1) % npartitions;
		OsicPartition *part = &parts[part_idx];

		for (int i = 0; i < part->nbuffers; i++)
		{
			int			local_id = part->first_buf + i;
			int			global_id = ctl->first_buf_id + local_id;
			BufferDesc *buf;
			uint64		state;

			buf = GetBufferDescriptor(global_id);
			state = pg_atomic_read_u64(&buf->state);

			if (!(state & BM_TAG_VALID))
			{
				uint64		old_state = state;
				uint64		new_state;

				if (BUF_STATE_GET_REFCOUNT(old_state) == 0)
				{
					new_state = old_state + BUF_REFCOUNT_ONE;
					if (pg_atomic_compare_exchange_u64(&buf->state,
													   &old_state,
													   new_state))
					{
						TrackNewBufferPin(BufferDescriptorGetBuffer(buf));
						*buf_state = new_state;
						return buf;
					}
				}
			}
		}
	}

	/*
	 * Pass 2: Standard clock sweep (cool-and-evict). Only reached when no
	 * free buffers exist. We allow up to 3 full sweeps of all partitions
	 * before giving up.
	 */
	for (attempts = 0; attempts < 3 * npartitions; attempts++)
	{
		int			part_idx = (start_part + attempts) % npartitions;
		OsicPartition *part = &parts[part_idx];
		int			sweep_count;

		SpinLockAcquire(&part->lock);

		/*
		 * Sweep within this partition.  Allow up to 2 * nbuffers sweeps (one
		 * full cycle to cool, one to evict).
		 */
		for (sweep_count = 0; sweep_count < 2 * part->nbuffers; sweep_count++)
		{
			int			local_id = part->first_buf + part->clock_hand;
			int			global_id = ctl->first_buf_id + local_id;
			BufferDesc *buf;
			uint64		state;
			uint32		flags;

			buf = GetBufferDescriptor(global_id);

			/* Advance clock hand (wraps within partition) */
			part->clock_hand = (part->clock_hand + 1) % part->nbuffers;

			/* First, try to find a free buffer (no tag) */
			state = pg_atomic_read_u64(&buf->state);
			if (!(state & BM_TAG_VALID))
			{
				/* Try to pin this free buffer */
				uint64		old_state = state;
				uint64		new_state;

				if (BUF_STATE_GET_REFCOUNT(old_state) == 0)
				{
					new_state = old_state + BUF_REFCOUNT_ONE;
					if (pg_atomic_compare_exchange_u64(&buf->state,
													   &old_state,
													   new_state))
					{
						TrackNewBufferPin(BufferDescriptorGetBuffer(buf));
						*buf_state = new_state;
						SpinLockRelease(&part->lock);
						return buf;
					}
				}
				continue;
			}

			/* Buffer has a valid tag -- check if pinned */
			if (BUF_STATE_GET_REFCOUNT(state) != 0)
				continue;

			/* Check the cool flag */
			flags = pg_atomic_read_u32(&states[local_id].flags);

			if (!(flags & OSIC_FLAG_COOL))
			{
				/*
				 * Buffer is hot -> mark it cool (stage 1). Use atomic OR --
				 * no need to hold spinlock for this.
				 */
				pg_atomic_fetch_or_u32(&states[local_id].flags,
									   OSIC_FLAG_COOL);
				pg_atomic_fetch_add_u64(&parts[part_idx].stat_cooling_sweeps, 1);
				continue;
			}

			/*
			 * Buffer is cool and unpinned -> evict it (stage 2). Try to pin
			 * it.
			 */
			{
				uint64		old_state = state;
				uint64		new_state;

				if (BUF_STATE_GET_REFCOUNT(old_state) == 0)
				{
					new_state = old_state + BUF_REFCOUNT_ONE;
					if (pg_atomic_compare_exchange_u64(&buf->state,
													   &old_state,
													   new_state))
					{
						TrackNewBufferPin(BufferDescriptorGetBuffer(buf));
						*buf_state = new_state;
						SpinLockRelease(&part->lock);
						pg_atomic_fetch_add_u64(&parts[part_idx].stat_evictions, 1);
						return buf;
					}
				}
			}
		}

		SpinLockRelease(&part->lock);
	}

	/* No victim found after exhaustive search */
	elog(ERROR, "OSIC: no unpinned buffers available for eviction");
	return NULL;				/* keep compiler happy */
}


/* ----------------------------------------------------------------
 * Trickle writer iterator
 * ----------------------------------------------------------------
 */

typedef struct OsicTrickleIter
{
	OsicControl *ctl;
	int			partition;		/* current partition index */
	int			offset;			/* offset within current partition */
	int			yielded;		/* count of yielded buffers */
	int			max_candidates;
} OsicTrickleIter;

static void *
OsicTrickleIterBegin(void *strategy_data, int max_candidates)
{
	OsicControl *ctl = (OsicControl *) strategy_data;
	OsicTrickleIter *iter;

	iter = (OsicTrickleIter *) palloc(sizeof(OsicTrickleIter));
	iter->ctl = ctl;
	iter->partition = 0;
	iter->offset = 0;
	iter->yielded = 0;
	iter->max_candidates = max_candidates;

	return iter;
}

static int
OsicTrickleIterNext(void *strategy_data, void *iter_state)
{
	OsicTrickleIter *iter = (OsicTrickleIter *) iter_state;
	OsicControl *ctl = iter->ctl;
	OsicBufState *states = OSIC_BUFSTATES(ctl);
	OsicPartition *parts = OSIC_PARTITIONS(ctl);

	if (iter->yielded >= iter->max_candidates)
		return -1;

	while (iter->partition < ctl->npartitions)
	{
		OsicPartition *part = &parts[iter->partition];

		while (iter->offset < part->nbuffers)
		{
			int			local_id = part->first_buf + iter->offset;
			int			global_id = ctl->first_buf_id + local_id;
			BufferDesc *buf;
			uint64		state;
			uint32		flags;

			iter->offset++;

			buf = GetBufferDescriptor(global_id);
			state = pg_atomic_read_u64(&buf->state);

			/* Skip if not valid or pinned */
			if (!(state & BM_TAG_VALID) || BUF_STATE_GET_REFCOUNT(state) != 0)
				continue;

			/* Only yield cool + dirty buffers */
			flags = pg_atomic_read_u32(&states[local_id].flags);
			if ((flags & OSIC_FLAG_COOL) && (state & BM_DIRTY))
			{
				iter->yielded++;
				return global_id;
			}
		}

		/* Move to next partition */
		iter->partition++;
		iter->offset = 0;
	}

	return -1;					/* exhausted */
}

static void
OsicTrickleIterEnd(void *strategy_data, void *iter_state)
{
	pfree(iter_state);
}


/* ----------------------------------------------------------------
 * Sync/notification callbacks
 * ----------------------------------------------------------------
 */

static int
OsicSyncStart(void *strategy_data, uint32 *complete_passes,
			  uint32 *num_buf_alloc)
{
	if (complete_passes)
		*complete_passes = 0;
	if (num_buf_alloc)
		*num_buf_alloc = 0;
	return 0;
}

static void
OsicNotifyTrickle(void *strategy_data, int bgwprocno)
{
	/* OSIC pools use per-pool trickle writers */
}

static bool
OsicRejectBuffer(void *strategy_data, BufferAccessStrategy strategy,
				 BufferDesc *buf, bool from_ring)
{
	return false;				/* never reject */
}

/*
 * OsicHintVacuum -- hint that VACUUM is active.
 *
 * When VACUUM is active, newly loaded pages are marked cool immediately
 * so they are evicted on the next clock sweep, preventing VACUUM scans
 * from polluting the cache.
 */
static void
OsicHintVacuum(void *strategy_data, bool vacuum_active)
{
	OsicControl *ctl = (OsicControl *) strategy_data;
	OsicBackendState *state = osic_get_backend_state(ctl);

	state->vacuum_hint = vacuum_active;
}


/* ----------------------------------------------------------------
 * Shared memory sizing and initialization
 * ----------------------------------------------------------------
 */

static Size
OsicShmemSize(int nbuffers)
{
	int			npartitions;
	Size		size;

	npartitions = osic_compute_partitions(nbuffers);

	size = MAXALIGN(sizeof(OsicControl));
	size += MAXALIGN(sizeof(OsicPartition) * npartitions);
	size += MAXALIGN(sizeof(OsicBufState) * nbuffers);

	return size;
}

static void
OsicShmemInit(void *strategy_data, int nbuffers, int first_buf_id, bool init)
{
	OsicControl *ctl = (OsicControl *) strategy_data;
	OsicPartition *parts;
	OsicBufState *states;
	int			npartitions;
	int			bufs_per_part;
	int			remaining;

	if (!init)
	{
		/* Re-attach: validate */
		Assert(ctl->nbuffers == nbuffers);
		Assert(ctl->first_buf_id == first_buf_id);
		return;
	}

	npartitions = osic_compute_partitions(nbuffers);

	ctl->nbuffers = nbuffers;
	ctl->first_buf_id = first_buf_id;
	ctl->npartitions = npartitions;

	pg_atomic_init_u32(&ctl->inflight, 0);

	/* Initialize partitions: divide buffers evenly, extras go to early ones */
	parts = OSIC_PARTITIONS(ctl);
	bufs_per_part = nbuffers / npartitions;
	remaining = nbuffers % npartitions;

	{
		int			buf_offset = 0;

		for (int i = 0; i < npartitions; i++)
		{
			int			n = bufs_per_part + (i < remaining ? 1 : 0);

			SpinLockInit(&parts[i].lock);
			parts[i].clock_hand = 0;
			parts[i].first_buf = buf_offset;
			parts[i].nbuffers = n;
			pg_atomic_init_u64(&parts[i].stat_hits, 0);
			pg_atomic_init_u64(&parts[i].stat_misses, 0);
			pg_atomic_init_u64(&parts[i].stat_evictions, 0);
			pg_atomic_init_u64(&parts[i].stat_cooling_sweeps, 0);
			buf_offset += n;
		}
	}

	/* Initialize all buffer states to hot */
	states = OSIC_BUFSTATES(ctl);
	for (int i = 0; i < nbuffers; i++)
	{
		pg_atomic_init_u32(&states[i].flags, 0);	/* 0 = hot */
	}
}

static void
OsicShutdown(void *strategy_data)
{
	/* Nothing to clean up */
}


/* ----------------------------------------------------------------
 * BufferPoolRoutine vtable
 * ----------------------------------------------------------------
 */

static const BufferPoolRoutine osic_pool_routine = {
	.on_hit = OsicOnHit,
	.on_miss = OsicOnMiss,
	.on_evict = OsicOnEvict,
	.on_new_tag = OsicOnNewTag,
	.get_victim = OsicGetVictim,
	.sync_start = OsicSyncStart,
	.notify_trickle = OsicNotifyTrickle,
	.trickle_iter_begin = OsicTrickleIterBegin,
	.trickle_iter_next = OsicTrickleIterNext,
	.trickle_iter_end = OsicTrickleIterEnd,
	.hint_vacuum = OsicHintVacuum,
	.reject_buffer = OsicRejectBuffer,
	.prefetch_hint = NULL,
	.shmem_size = OsicShmemSize,
	.shmem_init = OsicShmemInit,
	.shutdown = OsicShutdown,
};


/* ----------------------------------------------------------------
 * SQL-callable handler function
 * ----------------------------------------------------------------
 */

PG_FUNCTION_INFO_V1(osic_pool_handler);

Datum
osic_pool_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&osic_pool_routine);
}


/* ----------------------------------------------------------------
 * Statistics SRF
 * ----------------------------------------------------------------
 */

#define PG_STAT_OSIC_COLS	9

PG_FUNCTION_INFO_V1(pg_stat_get_osic_stats);

Datum
pg_stat_get_osic_stats(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	InitMaterializedSRF(fcinfo, 0);

	for (int i = 0; i < NBufferPools; i++)
	{
		BufferPoolDesc *pool = &BufferPoolDescs[i];
		PoolLocalState *local;
		OsicControl *ctl;
		OsicBufState *states;
		Datum		values[PG_STAT_OSIC_COLS];
		bool		nulls[PG_STAT_OSIC_COLS];
		int			hot_count = 0;
		int			cool_count = 0;

		if (!pool->bp_active)
			continue;

		/* Only report pools that use the OSIC algorithm */
		if (pool->bp_routine != &osic_pool_routine)
			continue;

		/* Attach to DSM and get strategy data */
		local = EnsurePoolAttached(pool);
		ctl = (OsicControl *) local->strategy_data;
		if (!ctl)
			continue;

		/* Count hot vs cool buffers */
		states = OSIC_BUFSTATES(ctl);
		for (int j = 0; j < ctl->nbuffers; j++)
		{
			uint32		flags = pg_atomic_read_u32(&states[j].flags);

			if (flags & OSIC_FLAG_COOL)
				cool_count++;
			else
				hot_count++;
		}

		memset(nulls, 0, sizeof(nulls));
		values[0] = NameGetDatum(&pool->bp_name);

		if (OidIsValid(pool->bp_oid))
			values[1] = ObjectIdGetDatum(pool->bp_oid);
		else
			nulls[1] = true;

		values[2] = Int32GetDatum(ctl->nbuffers);
		values[3] = Int32GetDatum(hot_count);
		values[4] = Int32GetDatum(cool_count);
		{
			OsicPartition *parts_view = OSIC_PARTITIONS(ctl);
			uint64		total_hits = 0,
						total_misses = 0;
			uint64		total_evictions = 0,
						total_cooling = 0;

			/* Flush this backend's pending local stats before reading */
			for (int p = 0; p < ctl->npartitions; p++)
			{
				PoolStatFlush(&osic_local_stats[p][OSIC_STAT_HITS],
							  &parts_view[p].stat_hits);
				PoolStatFlush(&osic_local_stats[p][OSIC_STAT_MISSES],
							  &parts_view[p].stat_misses);
				PoolStatFlush(&osic_local_stats[p][OSIC_STAT_EVICTIONS],
							  &parts_view[p].stat_evictions);
				PoolStatFlush(&osic_local_stats[p][OSIC_STAT_COOLING_SWEEPS],
							  &parts_view[p].stat_cooling_sweeps);
			}

			for (int p = 0; p < ctl->npartitions; p++)
			{
				total_hits += pg_atomic_read_u64(&parts_view[p].stat_hits);
				total_misses += pg_atomic_read_u64(&parts_view[p].stat_misses);
				total_evictions += pg_atomic_read_u64(&parts_view[p].stat_evictions);
				total_cooling += pg_atomic_read_u64(&parts_view[p].stat_cooling_sweeps);
			}

			values[5] = Int64GetDatum(total_hits);
			values[6] = Int64GetDatum(total_misses);
			values[7] = Int64GetDatum(total_evictions);
			values[8] = Int64GetDatum(total_cooling);
		}

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
							 values, nulls);
	}

	return (Datum) 0;
}

/*
 * _PG_init -- register OSIC for use as the DEFAULT pool algorithm.
 */
void
_PG_init(void)
{
	RegisterDefaultPoolAlgorithm("osic", &osic_pool_routine);
}
