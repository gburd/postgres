/*-------------------------------------------------------------------------
 *
 * jam_pool.c
 *	  JAM buffer replacement algorithm for TOAST/overflow data.
 *
 * JAM is optimized for the write-heavy, read-few, temporal-locality
 * pattern characteristic of TOAST access:
 *
 *   - New buffers are inserted with usage_count = 1 (expect few re-reads)
 *   - On hit, usage_count is boosted modestly (capped at 3)
 *   - Clock sweep with accelerated decay: halve usage_count instead of
 *     decrementing by 1, so pages age out faster
 *   - Trickle iterator walks from clock hand, yielding dirty+cold pages
 *
 * This is built into core (not a contrib extension) because it is the
 * default replacement algorithm for TOAST relations and lives next to
 * the other built-in routines (clock_pool_routine, keep_pool_routine,
 * recycle_pool_routine) in src/backend/storage/buffer.  It is reachable
 * from SQL via jam_pool_handler so users can also assign it to their
 * own pools, but it is not auto-assigned to TOAST tables yet (the
 * heap AM's relation_overflow_pool callback is still a stub; see the
 * separate TOAST-routing commit).
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/storage/buffer/jam_pool.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "port/atomics.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "storage/bufpool.h"
#include "storage/bufpool_internals.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "utils/fmgrprotos.h"

/*
 * JAM limits: usage count cap is low since TOAST pages are rarely re-read.
 */
#define JAM_MAX_USAGE		3

/*
 * JamControl -- shared-memory control block for JAM pool.
 *
 * Uses a clock-sweep approach with accelerated decay.  Per-buffer
 * usage counters are stored in a variable-length array that follows
 * this struct in shared memory.
 */
typedef struct JamControl
{
	slock_t		jam_lock;		/* spinlock protecting sweep state */

	int			nbuffers;		/* number of physical buffers */
	int			first_buf_id;	/* global buffer ID of first buffer */
	int			clock_hand;		/* current position for sweep */

	/* Statistics (atomics, no lock needed) */
	pg_atomic_uint64 stat_hits;
	pg_atomic_uint64 stat_misses;
	pg_atomic_uint64 stat_evictions;
	pg_atomic_uint64 stat_sweeps;	/* number of complete sweeps */

	/* Trickle writer wakeup */
	int			trickle_procno;

	/* Per-buffer usage counts follow: uint8 usage[nbuffers] */
} JamControl;

#define JAM_USAGE(ctl) ((uint8 *) ((char *)(ctl) + MAXALIGN(sizeof(JamControl))))


/* ----------------------------------------------------------------
 *			JAM vtable implementation
 * ----------------------------------------------------------------
 */

/*
 * on_hit -- boost usage modestly on cache hit.
 */
static void
jam_on_hit(void *strategy_data, int buf_id, struct buftag *tag)
{
	JamControl *ctl = (JamControl *) strategy_data;
	int			local_id = buf_id - ctl->first_buf_id;
	uint8	   *usage = JAM_USAGE(ctl);

	Assert(local_id >= 0 && local_id < ctl->nbuffers);

	pg_atomic_fetch_add_u64(&ctl->stat_hits, 1);

	/* Boost usage, capped at JAM_MAX_USAGE.  No lock needed for single byte. */
	if (usage[local_id] < JAM_MAX_USAGE)
		usage[local_id]++;
}

/*
 * on_miss -- record a miss for statistics.
 */
static void
jam_on_miss(void *strategy_data, struct buftag *tag)
{
	JamControl *ctl = (JamControl *) strategy_data;

	pg_atomic_fetch_add_u64(&ctl->stat_misses, 1);
}

/*
 * on_evict -- clear usage on eviction.
 */
static void
jam_on_evict(void *strategy_data, int buf_id, struct buftag *old_tag)
{
	JamControl *ctl = (JamControl *) strategy_data;
	int			local_id = buf_id - ctl->first_buf_id;
	uint8	   *usage = JAM_USAGE(ctl);

	Assert(local_id >= 0 && local_id < ctl->nbuffers);

	usage[local_id] = 0;
	pg_atomic_fetch_add_u64(&ctl->stat_evictions, 1);
}

/*
 * on_new_tag -- set initial usage for a newly loaded page.
 *
 * TOAST pages get usage_count = 1 since they're typically read few times.
 * VACUUM-loaded pages get 0 (immediately evictable).
 */
static void
jam_on_new_tag(void *strategy_data, int buf_id, struct buftag *new_tag,
			   bool vacuum_hint)
{
	JamControl *ctl = (JamControl *) strategy_data;
	int			local_id = buf_id - ctl->first_buf_id;
	uint8	   *usage = JAM_USAGE(ctl);

	Assert(local_id >= 0 && local_id < ctl->nbuffers);

	usage[local_id] = vacuum_hint ? 0 : 1;
}

/*
 * get_victim -- clock sweep with accelerated decay.
 *
 * Instead of decrementing usage_count by 1 (standard clock-sweep),
 * JAM halves the usage_count.  This means:
 *   usage 3 -> 1 (one sweep to become evictable)
 *   usage 2 -> 1 (one sweep to become evictable)
 *   usage 1 -> 0 (evictable)
 *
 * Pages that aren't re-read within one or two sweep cycles are
 * quickly evicted, which suits the write-once-read-few TOAST pattern.
 *
 * Uses lock-free CAS for pinning, following the same pattern as OSIC
 * and LRU pool implementations.
 */
static BufferDesc *
jam_get_victim(void *strategy_data, BufferAccessStrategy access_strategy,
			   uint64 *buf_state, bool *from_ring)
{
	JamControl *ctl = (JamControl *) strategy_data;
	uint8	   *usage = JAM_USAGE(ctl);
	int			nbuffers = ctl->nbuffers;
	int			first_buf = ctl->first_buf_id;
	int			tries;

	*from_ring = false;

	/*
	 * First pass: look for free (no tag) buffers during warmup.
	 */
	for (int i = 0; i < nbuffers; i++)
	{
		int			buf_id = first_buf + i;
		BufferDesc *buf;
		uint64		old_state;
		uint64		new_state;

		buf = GetBufferDescriptor(buf_id);
		old_state = pg_atomic_read_u64(&buf->state);

		if (old_state & BM_TAG_VALID)
			continue;			/* already in use */

		if (BUF_STATE_GET_REFCOUNT(old_state) != 0)
			continue;			/* pinned */

		if (unlikely(old_state & BM_LOCKED))
			continue;			/* locked, skip */

		new_state = old_state + BUF_REFCOUNT_ONE;
		if (pg_atomic_compare_exchange_u64(&buf->state, &old_state,
										   new_state))
		{
			TrackNewBufferPin(BufferDescriptorGetBuffer(buf));
			*buf_state = new_state;
			return buf;
		}
	}

	/*
	 * Clock sweep with accelerated decay.
	 */
	for (tries = nbuffers * 3; tries > 0; tries--)
	{
		int			local_id;
		int			buf_id;
		BufferDesc *buf;
		uint64		old_state;
		uint64		new_state;

		SpinLockAcquire(&ctl->jam_lock);
		local_id = ctl->clock_hand;
		ctl->clock_hand = (local_id + 1) % nbuffers;
		if (local_id == 0)
			pg_atomic_fetch_add_u64(&ctl->stat_sweeps, 1);
		SpinLockRelease(&ctl->jam_lock);

		buf_id = first_buf + local_id;
		buf = GetBufferDescriptor(buf_id);

		old_state = pg_atomic_read_u64(&buf->state);

		/* Skip pinned or locked buffers */
		if (BUF_STATE_GET_REFCOUNT(old_state) != 0)
			continue;
		if (unlikely(old_state & BM_LOCKED))
			continue;

		if (usage[local_id] > 0)
		{
			/* Accelerated decay: halve instead of decrement */
			usage[local_id] = usage[local_id] / 2;
			continue;
		}

		/* usage == 0 and not pinned: try to pin via CAS */
		new_state = old_state + BUF_REFCOUNT_ONE;
		if (pg_atomic_compare_exchange_u64(&buf->state, &old_state,
										   new_state))
		{
			TrackNewBufferPin(BufferDescriptorGetBuffer(buf));
			*buf_state = new_state;
			return buf;
		}
		/* CAS failed, someone else pinned/modified it; try next */
	}

	/* Shouldn't happen unless all buffers are pinned */
	elog(ERROR, "JAM pool: no unpinned buffers available");
	return NULL;				/* keep compiler quiet */
}

/*
 * sync_start -- return current clock hand position.
 */
static int
jam_sync_start(void *strategy_data, uint32 *complete_passes,
			   uint32 *num_buf_alloc)
{
	JamControl *ctl = (JamControl *) strategy_data;

	if (complete_passes)
		*complete_passes = (uint32) pg_atomic_read_u64(&ctl->stat_sweeps);
	if (num_buf_alloc)
		*num_buf_alloc = 0;

	return ctl->clock_hand;
}

/*
 * notify_trickle -- register/deregister trickle writer for wakeup.
 */
static void
jam_notify_trickle(void *strategy_data, int bgwprocno)
{
	JamControl *ctl = (JamControl *) strategy_data;

	ctl->trickle_procno = bgwprocno;
}

/*
 * Trickle iterator: walk from clock hand yielding dirty + cold pages.
 */
typedef struct JamTrickleIter
{
	int			pos;
	int			remaining;
} JamTrickleIter;

static void *
jam_trickle_iter_begin(void *strategy_data, int max_candidates)
{
	JamControl *ctl = (JamControl *) strategy_data;
	JamTrickleIter *iter = palloc(sizeof(JamTrickleIter));

	iter->pos = ctl->clock_hand;
	iter->remaining = max_candidates > 0 ? max_candidates : ctl->nbuffers;

	return iter;
}

static int
jam_trickle_iter_next(void *strategy_data, void *iter_state)
{
	JamControl *ctl = (JamControl *) strategy_data;
	JamTrickleIter *iter = (JamTrickleIter *) iter_state;
	uint8	   *usage = JAM_USAGE(ctl);

	while (iter->remaining > 0)
	{
		int			local_id = iter->pos;
		int			buf_id = ctl->first_buf_id + local_id;
		BufferDesc *buf_hdr;
		uint64		state;

		iter->pos = (iter->pos + 1) % ctl->nbuffers;
		iter->remaining--;

		/* Prefer cold pages (low usage) that are dirty */
		if (usage[local_id] > 1)
			continue;

		buf_hdr = GetBufferDescriptor(buf_id);
		state = pg_atomic_read_u64(&buf_hdr->state);

		if ((state & BM_DIRTY) &&
			BUF_STATE_GET_REFCOUNT(state) == 0)
			return buf_id;
	}

	return -1;					/* no more candidates */
}

static void
jam_trickle_iter_end(void *strategy_data, void *iter_state)
{
	pfree(iter_state);
}

/*
 * shmem_size -- compute shared memory needed.
 */
static Size
jam_shmem_size(int nbuffers)
{
	Size		size;

	size = MAXALIGN(sizeof(JamControl));
	size += MAXALIGN(sizeof(uint8) * nbuffers); /* usage array */

	return size;
}

/*
 * shmem_init -- initialize JAM state in shared memory.
 */
static void
jam_shmem_init(void *strategy_data, int nbuffers, int first_buf_id, bool init)
{
	JamControl *ctl = (JamControl *) strategy_data;

	if (init)
	{
		SpinLockInit(&ctl->jam_lock);
		ctl->nbuffers = nbuffers;
		ctl->first_buf_id = first_buf_id;
		ctl->clock_hand = 0;
		ctl->trickle_procno = -1;

		pg_atomic_init_u64(&ctl->stat_hits, 0);
		pg_atomic_init_u64(&ctl->stat_misses, 0);
		pg_atomic_init_u64(&ctl->stat_evictions, 0);
		pg_atomic_init_u64(&ctl->stat_sweeps, 0);

		memset(JAM_USAGE(ctl), 0, sizeof(uint8) * nbuffers);
	}
	else
	{
		/* Re-attach: verify consistency */
		Assert(ctl->nbuffers == nbuffers);
		Assert(ctl->first_buf_id == first_buf_id);
	}
}

/*
 * The JAM BufferPoolRoutine vtable.
 */
static const BufferPoolRoutine jam_pool_routine = {
	.type = T_Invalid,

	.on_hit = jam_on_hit,
	.on_miss = jam_on_miss,
	.on_evict = jam_on_evict,
	.on_new_tag = jam_on_new_tag,

	.get_victim = jam_get_victim,

	.sync_start = jam_sync_start,
	.notify_trickle = jam_notify_trickle,

	.trickle_iter_begin = jam_trickle_iter_begin,
	.trickle_iter_next = jam_trickle_iter_next,
	.trickle_iter_end = jam_trickle_iter_end,

	.hint_vacuum = NULL,
	.reject_buffer = NULL,
	.prefetch_hint = NULL,

	.shmem_size = jam_shmem_size,
	.shmem_init = jam_shmem_init,
	.shutdown = NULL,
};


/*
 * jam_pool_handler -- SQL-callable handler returning the JAM routine.
 */
Datum
jam_pool_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&jam_pool_routine);
}
