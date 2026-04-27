/*-------------------------------------------------------------------------
 *
 * pg_bp_car.c
 *	  Clock with Adaptive Replacement (CAR) buffer replacement algorithm.
 *
 * This extension provides the CAR algorithm as a BufferPoolRoutine handler,
 * usable with CREATE BUFFER POOL ... HANDLER car_pool_handler.
 *
 * CAR uses clock hands instead of linked lists for the T1/T2 resident
 * sets, while keeping the same adaptive ghost list mechanism as ARC.
 * Key advantages over ARC:
 *   - Lower per-hit overhead (atomic reference bit vs spinlock+list-move)
 *   - Prefetch hint callback (ghost-list-aware prefetch)
 *   - T1-priority bgwriter (trickle writer starts at T1 clock hand)
 *
 * Data structures:
 *   T1 - circular array with clock hand, reference bits (recency)
 *   T2 - circular array with clock hand, reference bits (frequency)
 *   B1 - ghost list (linked list) for pages evicted from T1
 *   B2 - ghost list (linked list) for pages evicted from T2
 *
 * The adaptive parameter target_T1_size controls the balance between
 * T1 and T2.  Ghost list hits (B1 or B2) cause this target to shift.
 *
 * Patent-free: CAR is not covered by IBM's ARC patent (US 6,996,676).
 *
 * Reference:
 *   S. Bansal and D. Modha, "CAR: Clock with Adaptive Replacement",
 *   FAST 2004.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  contrib/pg_bp_car/pg_bp_car.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "common/hashfn.h"
#include "fmgr.h"
#include "funcapi.h"
#include "port/atomics.h"
#include "port/pg_bitutils.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "storage/bufpool.h"
#include "storage/bufpool_internals.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "utils/builtins.h"
#include "utils/tuplestore.h"

PG_MODULE_MAGIC_EXT(.name = "pg_bp_car", .version = PG_VERSION);

void		_PG_init(void);

/* CAR list identifiers */
#define CAR_LIST_T1			0	/* recently accessed (clock ring) */
#define CAR_LIST_T2			1	/* frequently accessed (clock ring) */
#define CAR_LIST_B1			2	/* ghost entries evicted from T1 */
#define CAR_LIST_B2			3	/* ghost entries evicted from T2 */
#define CAR_NUM_LISTS		4
#define CAR_LIST_UNUSED		(-1)

/* Stat slot indices for PoolStatIncrement */
#define CAR_STAT_LOOKUPS		0
#define CAR_STAT_T1_HITS		1
#define CAR_STAT_T2_HITS		2
#define CAR_STAT_B1_HITS		3
#define CAR_STAT_B2_HITS		4
#define CAR_STAT_MISSES			5
#define CAR_STAT_T1_EVICTIONS	6
#define CAR_STAT_T2_EVICTIONS	7
#define CAR_NUM_STATS			8

static uint64 car_local_stats[CAR_NUM_STATS];

/*
 * CAR Cache Directory Block (CDB).
 *
 * Each CDB tracks a page's position in the CAR structures.
 * For T1/T2 entries, buf_id is the pool-local buffer index.
 * For B1/B2 ghost entries, buf_id is -1.
 */
typedef struct CarCDB
{
	BufferTag	buf_tag;		/* page identity */
	int			buf_id;			/* pool-local buffer index (-1 for ghost) */
	int			list;			/* which CAR list, or CAR_LIST_UNUSED */
	bool		reference_bit;	/* set on access, cleared by clock hand */
	int			ghost_next;		/* next in ghost hash chain (-1 = end) */

	/* For T1/T2 clock rings: circular doubly-linked list */
	int			clock_prev;
	int			clock_next;

	/* For B1/B2 ghost lists: doubly-linked list */
	int			ghost_prev;		/* previous in ghost list (-1 = head) */
	int			ghost_list_next; /* next in ghost list (-1 = tail) */
} CarCDB;

/*
 * CAR shared-memory control block.
 */
typedef struct CarControl
{
	slock_t		car_lock;		/* spinlock protecting all mutable fields */

	int			nbuffers;		/* number of physical buffers in this pool */
	int			first_buf_id;	/* global buffer ID of first buffer in pool */
	int			ncdb;			/* total CDB entries (2 * nbuffers) */
	int			target_T1_size; /* adaptive T1 target */

	/* T1 and T2 clock rings */
	int			t1_size;		/* current T1 resident count */
	int			t1_hand;		/* CDB index of T1 clock hand (-1 if empty) */
	int			t2_size;		/* current T2 resident count */
	int			t2_hand;		/* CDB index of T2 clock hand (-1 if empty) */

	/* Ghost lists B1/B2 (doubly-linked, same as ARC) */
	int			b1_head;
	int			b1_tail;
	int			b1_size;
	int			b2_head;
	int			b2_tail;
	int			b2_size;

	/* Ghost hash table */
	int			ghost_hash_size;

	/* Free CDB list */
	int			free_cdb_list;

	/* Statistics (atomics) */
	pg_atomic_uint64 stat_lookups;
	pg_atomic_uint64 stat_t1_hits;
	pg_atomic_uint64 stat_t2_hits;
	pg_atomic_uint64 stat_b1_hits;
	pg_atomic_uint64 stat_b2_hits;
	pg_atomic_uint64 stat_misses;
	pg_atomic_uint64 stat_t1_evictions;
	pg_atomic_uint64 stat_t2_evictions;

	int			bgwprocno;		/* trickle writer procno (-1 = none) */

	pg_atomic_uint32 free_scan_start;	/* round-robin for free buffer scan */

	/*
	 * Variable-length arrays follow:
	 *   CarCDB cdb[ncdb]
	 *   int ghost_hash[ghost_hash_size]
	 *   int buf_to_cdb[nbuffers]
	 */
} CarControl;

/*
 * Per-backend CAR state for cross-call communication.
 */
typedef struct CarBackendState
{
	CarControl *ctl;
	int			ghost_cdb;		/* CDB index of ghost hit (-1 = miss) */
	bool		vacuum_hint;
} CarBackendState;

#define MAX_CAR_POOLS  MAX_BUFFER_POOLS
static CarBackendState car_backend_states[MAX_CAR_POOLS];
static int	car_num_states = 0;

static CarBackendState *
car_get_backend_state(CarControl *ctl)
{
	for (int i = 0; i < car_num_states; i++)
	{
		if (car_backend_states[i].ctl == ctl)
			return &car_backend_states[i];
	}
	Assert(car_num_states < MAX_CAR_POOLS);
	car_backend_states[car_num_states].ctl = ctl;
	car_backend_states[car_num_states].ghost_cdb = -1;
	car_backend_states[car_num_states].vacuum_hint = false;
	return &car_backend_states[car_num_states++];
}

/* Accessor macros for variable-length arrays */
#define CAR_CDB(ctl)		((CarCDB *) ((char *)(ctl) + sizeof(CarControl)))
#define CAR_GHOST_HASH(ctl)	((int *) ((char *)(ctl) + sizeof(CarControl) + \
							 sizeof(CarCDB) * (ctl)->ncdb))
#define CAR_BUF_TO_CDB(ctl) ((int *) ((char *)(ctl) + sizeof(CarControl) + \
							 sizeof(CarCDB) * (ctl)->ncdb + \
							 sizeof(int) * (ctl)->ghost_hash_size))

/* ----------------------------------------------------------------
 *			Ghost hash table operations
 * ----------------------------------------------------------------
 */

static inline uint32
car_ghost_hash_bucket(CarControl *ctl, const BufferTag *tag)
{
	uint32		h = tag_hash(tag, sizeof(BufferTag));

	return h & (ctl->ghost_hash_size - 1);
}

static int
car_ghost_lookup(CarControl *ctl, const BufferTag *tag)
{
	int		   *ghost_hash = CAR_GHOST_HASH(ctl);
	CarCDB	   *cdb = CAR_CDB(ctl);
	uint32		bucket = car_ghost_hash_bucket(ctl, tag);
	int			idx = ghost_hash[bucket];

	while (idx >= 0)
	{
		if (BufferTagsEqual(&cdb[idx].buf_tag, tag))
			return idx;
		idx = cdb[idx].ghost_next;
	}
	return -1;
}

static void
car_ghost_insert(CarControl *ctl, int cdb_idx)
{
	int		   *ghost_hash = CAR_GHOST_HASH(ctl);
	CarCDB	   *cdb = CAR_CDB(ctl);
	uint32		bucket = car_ghost_hash_bucket(ctl, &cdb[cdb_idx].buf_tag);

	cdb[cdb_idx].ghost_next = ghost_hash[bucket];
	ghost_hash[bucket] = cdb_idx;
}

static void
car_ghost_remove(CarControl *ctl, int cdb_idx)
{
	int		   *ghost_hash = CAR_GHOST_HASH(ctl);
	CarCDB	   *cdb = CAR_CDB(ctl);
	uint32		bucket = car_ghost_hash_bucket(ctl, &cdb[cdb_idx].buf_tag);
	int			prev = -1;
	int			cur = ghost_hash[bucket];

	while (cur >= 0)
	{
		if (cur == cdb_idx)
		{
			if (prev < 0)
				ghost_hash[bucket] = cdb[cur].ghost_next;
			else
				cdb[prev].ghost_next = cdb[cur].ghost_next;
			cdb[cur].ghost_next = -1;
			return;
		}
		prev = cur;
		cur = cdb[cur].ghost_next;
	}
}

/* ----------------------------------------------------------------
 *			Clock ring operations (T1/T2)
 * ----------------------------------------------------------------
 */

/*
 * Insert a CDB into the T1 or T2 clock ring, just before the hand.
 * This is the "MRU" position -- the hand won't reach it for a full cycle.
 */
static void
car_clock_insert(CarControl *ctl, CarCDB *cdb_arr, int cdb_idx, bool is_t2)
{
	CarCDB	   *cdb = &cdb_arr[cdb_idx];
	int		   *hand = is_t2 ? &ctl->t2_hand : &ctl->t1_hand;
	int		   *size = is_t2 ? &ctl->t2_size : &ctl->t1_size;

	cdb->list = is_t2 ? CAR_LIST_T2 : CAR_LIST_T1;

	if (*hand < 0)
	{
		/* Empty ring: this is the only element */
		cdb->clock_next = cdb_idx;
		cdb->clock_prev = cdb_idx;
		*hand = cdb_idx;
	}
	else
	{
		/* Insert just before the hand */
		int			before_hand = cdb_arr[*hand].clock_prev;

		cdb->clock_next = *hand;
		cdb->clock_prev = before_hand;
		cdb_arr[before_hand].clock_next = cdb_idx;
		cdb_arr[*hand].clock_prev = cdb_idx;
	}
	(*size)++;
}

/*
 * Remove a CDB from its clock ring.
 */
static void
car_clock_remove(CarControl *ctl, CarCDB *cdb_arr, int cdb_idx)
{
	CarCDB	   *cdb = &cdb_arr[cdb_idx];
	bool		is_t2 = (cdb->list == CAR_LIST_T2);
	int		   *hand = is_t2 ? &ctl->t2_hand : &ctl->t1_hand;
	int		   *size = is_t2 ? &ctl->t2_size : &ctl->t1_size;

	Assert(cdb->list == CAR_LIST_T1 || cdb->list == CAR_LIST_T2);

	if (cdb->clock_next == cdb_idx)
	{
		/* Last element in ring */
		*hand = -1;
	}
	else
	{
		cdb_arr[cdb->clock_prev].clock_next = cdb->clock_next;
		cdb_arr[cdb->clock_next].clock_prev = cdb->clock_prev;
		if (*hand == cdb_idx)
			*hand = cdb->clock_next;
	}

	cdb->clock_prev = -1;
	cdb->clock_next = -1;
	cdb->list = CAR_LIST_UNUSED;
	(*size)--;
}

/* ----------------------------------------------------------------
 *			Ghost list operations (B1/B2)
 * ----------------------------------------------------------------
 */

/*
 * Append a CDB to the tail of a ghost list (MRU position).
 */
static void
car_ghost_list_append(CarControl *ctl, CarCDB *cdb_arr, int cdb_idx, bool is_b2)
{
	CarCDB	   *cdb = &cdb_arr[cdb_idx];
	int		   *head = is_b2 ? &ctl->b2_head : &ctl->b1_head;
	int		   *tail = is_b2 ? &ctl->b2_tail : &ctl->b1_tail;
	int		   *size = is_b2 ? &ctl->b2_size : &ctl->b1_size;

	cdb->list = is_b2 ? CAR_LIST_B2 : CAR_LIST_B1;

	if (*tail < 0)
	{
		cdb->ghost_prev = -1;
		cdb->ghost_list_next = -1;
		*head = cdb_idx;
		*tail = cdb_idx;
	}
	else
	{
		cdb->ghost_list_next = -1;
		cdb->ghost_prev = *tail;
		cdb_arr[*tail].ghost_list_next = cdb_idx;
		*tail = cdb_idx;
	}
	(*size)++;
}

/*
 * Remove a CDB from its ghost list.
 */
static void
car_ghost_list_remove(CarControl *ctl, CarCDB *cdb_arr, int cdb_idx)
{
	CarCDB	   *cdb = &cdb_arr[cdb_idx];
	bool		is_b2 = (cdb->list == CAR_LIST_B2);
	int		   *head = is_b2 ? &ctl->b2_head : &ctl->b1_head;
	int		   *tail = is_b2 ? &ctl->b2_tail : &ctl->b1_tail;
	int		   *size = is_b2 ? &ctl->b2_size : &ctl->b1_size;

	Assert(cdb->list == CAR_LIST_B1 || cdb->list == CAR_LIST_B2);

	if (cdb->ghost_prev < 0)
		*head = cdb->ghost_list_next;
	else
		cdb_arr[cdb->ghost_prev].ghost_list_next = cdb->ghost_list_next;

	if (cdb->ghost_list_next < 0)
		*tail = cdb->ghost_prev;
	else
		cdb_arr[cdb->ghost_list_next].ghost_prev = cdb->ghost_prev;

	cdb->ghost_prev = -1;
	cdb->ghost_list_next = -1;
	cdb->list = CAR_LIST_UNUSED;
	(*size)--;
}

/* ----------------------------------------------------------------
 *			CDB allocation
 * ----------------------------------------------------------------
 */

static int
car_alloc_cdb(CarControl *ctl, CarCDB *cdb_arr)
{
	int			idx;

	if (ctl->free_cdb_list >= 0)
	{
		idx = ctl->free_cdb_list;
		ctl->free_cdb_list = cdb_arr[idx].clock_next;
		cdb_arr[idx].clock_next = -1;
		return idx;
	}

	/* Recycle from ghost lists */
	if (ctl->b1_size > 0 &&
		(ctl->t1_size + ctl->b1_size) >= ctl->nbuffers)
		idx = ctl->b1_head;
	else if (ctl->b2_size > 0)
		idx = ctl->b2_head;
	else if (ctl->b1_size > 0)
		idx = ctl->b1_head;
	else
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("CAR: no CDB entries available for recycling")));

	car_ghost_remove(ctl, idx);
	car_ghost_list_remove(ctl, cdb_arr, idx);
	cdb_arr[idx].buf_id = -1;
	ClearBufferTag(&cdb_arr[idx].buf_tag);

	return idx;
}


/* ----------------------------------------------------------------
 *			CAR vtable callback implementations
 * ----------------------------------------------------------------
 */

/*
 * CarOnHit -- called when a page is found in the buffer cache.
 *
 * Just sets the reference bit. No spinlock needed for the common case.
 * T1->T2 promotion happens lazily during clock sweep in CarGetVictim.
 */
static void
CarOnHit(void *strategy_data, int buf_id, BufferTag *tag)
{
	CarControl *ctl = (CarControl *) strategy_data;
	CarCDB	   *cdb_arr = CAR_CDB(ctl);
	int		   *buf_to_cdb = CAR_BUF_TO_CDB(ctl);
	int			pool_local_id = buf_id - ctl->first_buf_id;
	int			cdb_idx;

	PoolStatIncrement(&car_local_stats[CAR_STAT_LOOKUPS], &ctl->stat_lookups);

	if (pool_local_id < 0 || pool_local_id >= ctl->nbuffers)
		return;

	cdb_idx = buf_to_cdb[pool_local_id];
	if (cdb_idx < 0)
		return;

	if (cdb_arr[cdb_idx].list == CAR_LIST_T1)
		PoolStatIncrement(&car_local_stats[CAR_STAT_T1_HITS], &ctl->stat_t1_hits);
	else if (cdb_arr[cdb_idx].list == CAR_LIST_T2)
		PoolStatIncrement(&car_local_stats[CAR_STAT_T2_HITS], &ctl->stat_t2_hits);

	/*
	 * Set reference bit atomically.  No spinlock needed -- this is the
	 * key performance advantage of CAR over ARC.
	 */
	cdb_arr[cdb_idx].reference_bit = true;
	pg_write_barrier();
}

/*
 * CarOnMiss -- called when a page is NOT found in the buffer cache.
 *
 * Checks ghost lists (B1/B2) and adapts target_T1_size.
 */
static void
CarOnMiss(void *strategy_data, BufferTag *tag)
{
	CarControl *ctl = (CarControl *) strategy_data;
	CarBackendState *state = car_get_backend_state(ctl);
	int			ghost_idx;

	PoolStatIncrement(&car_local_stats[CAR_STAT_LOOKUPS], &ctl->stat_lookups);

	SpinLockAcquire(&ctl->car_lock);

	ghost_idx = car_ghost_lookup(ctl, tag);

	if (ghost_idx >= 0)
	{
		CarCDB	   *cdb_arr = CAR_CDB(ctl);
		int			ghost_list = cdb_arr[ghost_idx].list;

		if (ghost_list == CAR_LIST_B1)
		{
			int			delta;

			PoolStatIncrement(&car_local_stats[CAR_STAT_B1_HITS], &ctl->stat_b1_hits);
			delta = Max(ctl->b2_size / Max(ctl->b1_size, 1), 1);
			ctl->target_T1_size = Min(ctl->target_T1_size + delta,
									  ctl->nbuffers);
			state->ghost_cdb = ghost_idx;
		}
		else if (ghost_list == CAR_LIST_B2)
		{
			int			delta;

			PoolStatIncrement(&car_local_stats[CAR_STAT_B2_HITS], &ctl->stat_b2_hits);
			delta = Max(ctl->b1_size / Max(ctl->b2_size, 1), 1);
			ctl->target_T1_size = Max(ctl->target_T1_size - delta, 0);
			state->ghost_cdb = ghost_idx;
		}
		else
		{
			state->ghost_cdb = -1;
		}
	}
	else
	{
		PoolStatIncrement(&car_local_stats[CAR_STAT_MISSES], &ctl->stat_misses);
		state->ghost_cdb = -1;
	}

	SpinLockRelease(&ctl->car_lock);
}

/*
 * CarOnEvict -- called when a buffer's old content is being evicted.
 *
 * Moves CDB from clock ring to ghost list.
 */
static void
CarOnEvict(void *strategy_data, int buf_id, BufferTag *old_tag)
{
	CarControl *ctl = (CarControl *) strategy_data;
	CarCDB	   *cdb_arr = CAR_CDB(ctl);
	int		   *buf_to_cdb = CAR_BUF_TO_CDB(ctl);
	int			pool_local_id = buf_id - ctl->first_buf_id;
	int			cdb_idx;
	int			old_list;

	SpinLockAcquire(&ctl->car_lock);

	if (pool_local_id < 0 || pool_local_id >= ctl->nbuffers)
	{
		SpinLockRelease(&ctl->car_lock);
		return;
	}

	cdb_idx = buf_to_cdb[pool_local_id];
	if (cdb_idx < 0)
	{
		SpinLockRelease(&ctl->car_lock);
		return;
	}

	old_list = cdb_arr[cdb_idx].list;

	if (old_list == CAR_LIST_T1)
	{
		car_clock_remove(ctl, cdb_arr, cdb_idx);
		car_ghost_list_append(ctl, cdb_arr, cdb_idx, false);	/* B1 */
		PoolStatIncrement(&car_local_stats[CAR_STAT_T1_EVICTIONS], &ctl->stat_t1_evictions);
	}
	else if (old_list == CAR_LIST_T2)
	{
		car_clock_remove(ctl, cdb_arr, cdb_idx);
		car_ghost_list_append(ctl, cdb_arr, cdb_idx, true);	/* B2 */
		PoolStatIncrement(&car_local_stats[CAR_STAT_T2_EVICTIONS], &ctl->stat_t2_evictions);
	}
	else
	{
		SpinLockRelease(&ctl->car_lock);
		return;
	}

	/* Convert to ghost entry */
	cdb_arr[cdb_idx].buf_id = -1;
	cdb_arr[cdb_idx].reference_bit = false;
	buf_to_cdb[pool_local_id] = -1;

	/* Add to ghost hash table */
	car_ghost_insert(ctl, cdb_idx);

	SpinLockRelease(&ctl->car_lock);
}

/*
 * CarOnNewTag -- called when a buffer is assigned a new page.
 */
static void
CarOnNewTag(void *strategy_data, int buf_id, BufferTag *new_tag,
			bool vacuum_hint)
{
	CarControl *ctl = (CarControl *) strategy_data;
	CarBackendState *state = car_get_backend_state(ctl);
	CarCDB	   *cdb_arr = CAR_CDB(ctl);
	int		   *buf_to_cdb = CAR_BUF_TO_CDB(ctl);
	int			pool_local_id = buf_id - ctl->first_buf_id;
	int			cdb_idx;

	Assert(pool_local_id >= 0 && pool_local_id < ctl->nbuffers);

	SpinLockAcquire(&ctl->car_lock);

	Assert(buf_to_cdb[pool_local_id] == -1);

	if (state->ghost_cdb >= 0)
	{
		/* Ghost hit: promote from B1/B2 to T2 */
		cdb_idx = state->ghost_cdb;
		state->ghost_cdb = -1;

		if (cdb_idx >= ctl->ncdb ||
			(cdb_arr[cdb_idx].list != CAR_LIST_B1 &&
			 cdb_arr[cdb_idx].list != CAR_LIST_B2))
			goto complete_miss;

		car_ghost_remove(ctl, cdb_idx);
		car_ghost_list_remove(ctl, cdb_arr, cdb_idx);

		cdb_arr[cdb_idx].buf_id = pool_local_id;
		cdb_arr[cdb_idx].reference_bit = false;
		car_clock_insert(ctl, cdb_arr, cdb_idx, true);	/* T2 */
	}
	else
	{
complete_miss:
		/* Complete miss: allocate CDB, place on T1 */
		cdb_idx = car_alloc_cdb(ctl, cdb_arr);

		cdb_arr[cdb_idx].buf_tag = *new_tag;
		cdb_arr[cdb_idx].buf_id = pool_local_id;
		cdb_arr[cdb_idx].ghost_next = -1;
		cdb_arr[cdb_idx].reference_bit = false;

		car_clock_insert(ctl, cdb_arr, cdb_idx, false);	/* T1 */
	}

	buf_to_cdb[pool_local_id] = cdb_idx;

	SpinLockRelease(&ctl->car_lock);
}


/* ----------------------------------------------------------------
 *			CAR get_victim -- clock-sweep eviction
 * ----------------------------------------------------------------
 */

/*
 * CarGetVictim -- select a victim buffer using clock sweep.
 *
 * Scans T1 clock hand.  If reference_bit set, clear it and promote
 * from T1 to T2.  If reference_bit clear and buffer unpinned, evict.
 * Falls back to T2 scan if T1 exhausted.
 */
static BufferDesc *
CarGetVictim(void *strategy_data, BufferAccessStrategy strategy,
			 uint64 *buf_state, bool *from_ring)
{
	CarControl *ctl = (CarControl *) strategy_data;
	CarBackendState *state = car_get_backend_state(ctl);
	CarCDB	   *cdb_arr = CAR_CDB(ctl);
	BufferDesc *buf;
	int			attempts = 0;
	int			max_attempts;

	*from_ring = false;
	state->ghost_cdb = -1;

	max_attempts = ctl->nbuffers * 3;	/* generous sweep limit */

	/*
	 * Phase 0: Check for free/untracked buffers first.
	 * When the pool is larger than the working set, there will be many
	 * free buffers.  Scanning for them first avoids unnecessary T1/T2
	 * clock sweeps that would evict working set pages.
	 */
	{
		int		   *buf_to_cdb = CAR_BUF_TO_CDB(ctl);
		int			scan_start = pg_atomic_read_u32(&ctl->free_scan_start) % ctl->nbuffers;

		for (int j = 0; j < ctl->nbuffers; j++)
		{
			int			i = (scan_start + j) % ctl->nbuffers;
			uint64		old_buf_state;
			uint64		local_buf_state;

			if (buf_to_cdb[i] >= 0)
				continue;		/* tracked, skip */

			buf = GetBufferDescriptor(ctl->first_buf_id + i);
			old_buf_state = pg_atomic_read_u64(&buf->state);

			for (;;)
			{
				local_buf_state = old_buf_state;

				if (BUF_STATE_GET_REFCOUNT(local_buf_state) != 0)
					break;

				if (unlikely(local_buf_state & BM_LOCKED))
				{
					old_buf_state = WaitBufHdrUnlocked(buf);
					continue;
				}

				local_buf_state += BUF_REFCOUNT_ONE;
				if (pg_atomic_compare_exchange_u64(&buf->state,
												   &old_buf_state,
												   local_buf_state))
				{
					pg_atomic_write_u32(&ctl->free_scan_start,
										(i + 1) % ctl->nbuffers);
					*buf_state = local_buf_state;
					TrackNewBufferPin(BufferDescriptorGetBuffer(buf));
					return buf;
				}
			}
		}
	}

	SpinLockAcquire(&ctl->car_lock);

	/*
	 * Phase 1: Scan T1 clock.
	 * Pages with reference_bit set are promoted to T2 (lazy promotion).
	 * Pages with reference_bit clear and unpinned are eviction candidates.
	 */
	while (ctl->t1_hand >= 0 && attempts < max_attempts)
	{
		int			cdb_idx = ctl->t1_hand;
		CarCDB	   *cdb = &cdb_arr[cdb_idx];

		attempts++;

		if (cdb->buf_id < 0 || cdb->list != CAR_LIST_T1)
		{
			/* Advance hand */
			ctl->t1_hand = cdb->clock_next;
			if (ctl->t1_hand == cdb_idx)
				ctl->t1_hand = -1;	/* wrapped around empty */
			continue;
		}

		if (cdb->reference_bit)
		{
			/*
			 * Referenced: promote from T1 to T2 (lazy promotion).
			 * This is the CAR equivalent of ARC's on_hit T1->T2 move.
			 */
			int			next = cdb->clock_next;

			cdb->reference_bit = false;
			car_clock_remove(ctl, cdb_arr, cdb_idx);
			car_clock_insert(ctl, cdb_arr, cdb_idx, true);	/* T2 */
			ctl->t1_hand = (next == cdb_idx) ? -1 : next;
			continue;
		}

		/*
		 * Unreferenced: check if T1 is at or above target.
		 * If T1 is too large, evict from T1.  Otherwise, skip and try T2.
		 */
		if (ctl->t1_size > Max(1, ctl->target_T1_size))
		{
			/* Try to pin the buffer */
			Assert(cdb->buf_id >= 0 && cdb->buf_id < ctl->nbuffers);
			buf = GetBufferDescriptor(cdb->buf_id + ctl->first_buf_id);

			{
				uint64		old_buf_state = pg_atomic_read_u64(&buf->state);

				for (;;)
				{
					uint64		local_buf_state = old_buf_state;

					if (BUF_STATE_GET_REFCOUNT(local_buf_state) != 0)
						break;	/* pinned, skip */

					if (unlikely(local_buf_state & BM_LOCKED))
					{
						SpinLockRelease(&ctl->car_lock);
						old_buf_state = WaitBufHdrUnlocked(buf);
						SpinLockAcquire(&ctl->car_lock);
						continue;
					}

					local_buf_state += BUF_REFCOUNT_ONE;
					if (pg_atomic_compare_exchange_u64(&buf->state,
													   &old_buf_state,
													   local_buf_state))
					{
						/* Advance T1 hand past this entry */
						ctl->t1_hand = cdb->clock_next;
						if (ctl->t1_hand == cdb_idx)
							ctl->t1_hand = -1;

						*buf_state = local_buf_state;
						SpinLockRelease(&ctl->car_lock);
						TrackNewBufferPin(BufferDescriptorGetBuffer(buf));
						return buf;
					}
				}
			}
		}

		/* Can't evict this one; advance hand */
		ctl->t1_hand = cdb->clock_next;
		if (ctl->t1_hand == cdb_idx)
			break;				/* full cycle */
	}

	/*
	 * Phase 2: Scan T2 clock.
	 * Same logic: reference_bit clear + unpinned -> evict.
	 */
	attempts = 0;
	while (ctl->t2_hand >= 0 && attempts < max_attempts)
	{
		int			cdb_idx = ctl->t2_hand;
		CarCDB	   *cdb = &cdb_arr[cdb_idx];

		attempts++;

		if (cdb->buf_id < 0 || cdb->list != CAR_LIST_T2)
		{
			ctl->t2_hand = cdb->clock_next;
			if (ctl->t2_hand == cdb_idx)
				ctl->t2_hand = -1;
			continue;
		}

		if (cdb->reference_bit)
		{
			/* Referenced: clear and advance */
			cdb->reference_bit = false;
			ctl->t2_hand = cdb->clock_next;
			if (ctl->t2_hand == cdb_idx)
				break;
			continue;
		}

		/* Unreferenced: try to evict */
		Assert(cdb->buf_id >= 0 && cdb->buf_id < ctl->nbuffers);
		buf = GetBufferDescriptor(cdb->buf_id + ctl->first_buf_id);

		{
			uint64		old_buf_state = pg_atomic_read_u64(&buf->state);

			for (;;)
			{
				uint64		local_buf_state = old_buf_state;

				if (BUF_STATE_GET_REFCOUNT(local_buf_state) != 0)
					break;

				if (unlikely(local_buf_state & BM_LOCKED))
				{
					SpinLockRelease(&ctl->car_lock);
					old_buf_state = WaitBufHdrUnlocked(buf);
					SpinLockAcquire(&ctl->car_lock);
					continue;
				}

				local_buf_state += BUF_REFCOUNT_ONE;
				if (pg_atomic_compare_exchange_u64(&buf->state,
												   &old_buf_state,
												   local_buf_state))
				{
					ctl->t2_hand = cdb->clock_next;
					if (ctl->t2_hand == cdb_idx)
						ctl->t2_hand = -1;

					*buf_state = local_buf_state;
					SpinLockRelease(&ctl->car_lock);
					TrackNewBufferPin(BufferDescriptorGetBuffer(buf));
					return buf;
				}
			}
		}

		ctl->t2_hand = cdb->clock_next;
		if (ctl->t2_hand == cdb_idx)
			break;
	}

	SpinLockRelease(&ctl->car_lock);

	/*
	 * Phase 3 (last resort): scan buffer descriptors for any usable buffer.
	 * This should rarely be reached now that Phase 0 handles free buffers.
	 */
	{
		int		   *buf_to_cdb = CAR_BUF_TO_CDB(ctl);

		for (int i = 0; i < ctl->nbuffers; i++)
		{
			uint64		old_buf_state;
			uint64		local_buf_state;

			if (buf_to_cdb[i] >= 0)
				continue;

			buf = GetBufferDescriptor(ctl->first_buf_id + i);
			old_buf_state = pg_atomic_read_u64(&buf->state);

			for (;;)
			{
				local_buf_state = old_buf_state;

				if (BUF_STATE_GET_REFCOUNT(local_buf_state) != 0)
					break;

				if (unlikely(local_buf_state & BM_LOCKED))
				{
					old_buf_state = WaitBufHdrUnlocked(buf);
					continue;
				}

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

	ereport(ERROR,
			(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
			 errmsg("no unpinned buffers available in CAR pool")));
	pg_unreachable();
}


/* ----------------------------------------------------------------
 *			CAR sync/trickle/hint support
 * ----------------------------------------------------------------
 */

/*
 * CarSyncStart -- return T1 clock hand position for trickle writer.
 *
 * The bgwriter should prefer writing dirty T1 pages (evicted sooner).
 */
static int
CarSyncStart(void *strategy_data, uint32 *complete_passes,
			 uint32 *num_buf_alloc)
{
	CarControl *ctl = (CarControl *) strategy_data;
	int			result = 0;

	if (complete_passes)
		*complete_passes = 0;
	if (num_buf_alloc)
		*num_buf_alloc = 0;

	SpinLockAcquire(&ctl->car_lock);
	if (ctl->t1_hand >= 0)
	{
		CarCDB	   *cdb_arr = CAR_CDB(ctl);

		if (cdb_arr[ctl->t1_hand].buf_id >= 0)
			result = cdb_arr[ctl->t1_hand].buf_id + ctl->first_buf_id;
	}
	SpinLockRelease(&ctl->car_lock);

	return result;
}

static void
CarNotifyTrickle(void *strategy_data, int bgwprocno)
{
	CarControl *ctl = (CarControl *) strategy_data;

	SpinLockAcquire(&ctl->car_lock);
	ctl->bgwprocno = bgwprocno;
	SpinLockRelease(&ctl->car_lock);
}

static void
CarHintVacuum(void *strategy_data, bool vacuum_active)
{
	CarControl *ctl = (CarControl *) strategy_data;
	CarBackendState *state = car_get_backend_state(ctl);

	state->vacuum_hint = vacuum_active;
}

static bool
CarRejectBuffer(void *strategy_data, BufferAccessStrategy strategy,
				BufferDesc *buf, bool from_ring)
{
	return false;
}

/*
 * CarPrefetchHint -- ghost-list-aware prefetch integration.
 *
 * When called with upcoming page tags, checks B1/B2 ghost lists.
 * Pages found in ghost lists signal that a ghost hit is imminent,
 * allowing CAR to pre-adapt its target_T1_size.
 */
static void
CarPrefetchHint(void *strategy_data, BufferTag *tags, int ntags)
{
	CarControl *ctl = (CarControl *) strategy_data;
	CarCDB	   *cdb_arr;

	if (ntags <= 0)
		return;

	SpinLockAcquire(&ctl->car_lock);
	cdb_arr = CAR_CDB(ctl);

	for (int i = 0; i < ntags; i++)
	{
		int			ghost_idx = car_ghost_lookup(ctl, &tags[i]);

		if (ghost_idx >= 0)
		{
			int			ghost_list = cdb_arr[ghost_idx].list;

			/*
			 * Move to MRU end of its ghost list to prevent recycling
			 * before the actual access arrives.
			 */
			if (ghost_list == CAR_LIST_B1)
			{
				car_ghost_list_remove(ctl, cdb_arr, ghost_idx);
				car_ghost_list_append(ctl, cdb_arr, ghost_idx, false);
			}
			else if (ghost_list == CAR_LIST_B2)
			{
				car_ghost_list_remove(ctl, cdb_arr, ghost_idx);
				car_ghost_list_append(ctl, cdb_arr, ghost_idx, true);
			}
		}
	}

	SpinLockRelease(&ctl->car_lock);
}


/* ----------------------------------------------------------------
 *			CAR lifecycle
 * ----------------------------------------------------------------
 */

static Size
CarShmemSize(int nbuffers)
{
	int			ncdb = nbuffers * 2;
	int			ghost_hash_size = pg_nextpower2_32(Max(ncdb, 64));
	Size		size;

	size = sizeof(CarControl);
	size = MAXALIGN(size);
	size += sizeof(CarCDB) * ncdb;
	size = MAXALIGN(size);
	size += sizeof(int) * ghost_hash_size;
	size = MAXALIGN(size);
	size += sizeof(int) * nbuffers;
	size = MAXALIGN(size);

	return size;
}

static void
CarShmemInit(void *strategy_data, int nbuffers, int first_buf_id, bool init)
{
	CarControl *ctl = (CarControl *) strategy_data;
	CarCDB	   *cdb_arr;
	int		   *ghost_hash;
	int		   *buf_to_cdb;
	int			ncdb;
	int			ghost_hash_size;

	if (!init)
	{
		Assert(ctl->nbuffers == nbuffers);
		return;
	}

	ncdb = nbuffers * 2;
	ghost_hash_size = pg_nextpower2_32(Max(ncdb, 64));

	SpinLockInit(&ctl->car_lock);
	ctl->nbuffers = nbuffers;
	ctl->first_buf_id = first_buf_id;
	ctl->ncdb = ncdb;
	ctl->target_T1_size = nbuffers / 2;
	ctl->ghost_hash_size = ghost_hash_size;

	/* Empty clock rings */
	ctl->t1_size = 0;
	ctl->t1_hand = -1;
	ctl->t2_size = 0;
	ctl->t2_hand = -1;

	/* Empty ghost lists */
	ctl->b1_head = -1;
	ctl->b1_tail = -1;
	ctl->b1_size = 0;
	ctl->b2_head = -1;
	ctl->b2_tail = -1;
	ctl->b2_size = 0;

	ctl->bgwprocno = -1;

	/* Initialize statistics */
	pg_atomic_init_u64(&ctl->stat_lookups, 0);
	pg_atomic_init_u64(&ctl->stat_t1_hits, 0);
	pg_atomic_init_u64(&ctl->stat_t2_hits, 0);
	pg_atomic_init_u64(&ctl->stat_b1_hits, 0);
	pg_atomic_init_u64(&ctl->stat_b2_hits, 0);
	pg_atomic_init_u64(&ctl->stat_misses, 0);
	pg_atomic_init_u64(&ctl->stat_t1_evictions, 0);
	pg_atomic_init_u64(&ctl->stat_t2_evictions, 0);
	pg_atomic_init_u32(&ctl->free_scan_start, 0);

	/* Initialize CDB array: all on free list */
	cdb_arr = CAR_CDB(ctl);
	for (int i = 0; i < ncdb; i++)
	{
		ClearBufferTag(&cdb_arr[i].buf_tag);
		cdb_arr[i].buf_id = -1;
		cdb_arr[i].list = CAR_LIST_UNUSED;
		cdb_arr[i].reference_bit = false;
		cdb_arr[i].ghost_next = -1;
		cdb_arr[i].clock_prev = -1;
		cdb_arr[i].clock_next = (i < ncdb - 1) ? i + 1 : -1;
		cdb_arr[i].ghost_prev = -1;
		cdb_arr[i].ghost_list_next = -1;
	}
	ctl->free_cdb_list = 0;

	/* Initialize ghost hash table */
	ghost_hash = CAR_GHOST_HASH(ctl);
	for (int i = 0; i < ghost_hash_size; i++)
		ghost_hash[i] = -1;

	/* Initialize buffer-to-CDB mapping */
	buf_to_cdb = CAR_BUF_TO_CDB(ctl);
	for (int i = 0; i < nbuffers; i++)
		buf_to_cdb[i] = -1;
}

static void
CarShutdown(void *strategy_data)
{
	/* Nothing to clean up */
}


/* ----------------------------------------------------------------
 *			CAR trickle writer iterator
 *
 * Sweep from T1 clock hand, then T2 clock hand.
 * Yield dirty+unpinned buffers with reference_bit == false
 * (cold pages that are best candidates for proactive flush).
 * ----------------------------------------------------------------
 */

typedef struct CarTrickleIter
{
	CarControl *ctl;
	int			phase;			/* 0 = scanning T1, 1 = scanning T2, 2 = done */
	int			cdb_idx;		/* current CDB index in the ring */
	int			start_idx;		/* starting position (to detect full loop) */
	bool		started;		/* have we yielded from this phase yet */
	int			yielded;
	int			max_candidates;
} CarTrickleIter;

static void *
CarTrickleIterBegin(void *strategy_data, int max_candidates)
{
	CarControl *ctl = (CarControl *) strategy_data;
	CarTrickleIter *iter;

	iter = (CarTrickleIter *) palloc(sizeof(CarTrickleIter));
	iter->ctl = ctl;
	iter->phase = 0;
	iter->yielded = 0;
	iter->max_candidates = max_candidates;

	/* Snapshot the T1 hand position under the lock */
	SpinLockAcquire(&ctl->car_lock);
	iter->cdb_idx = ctl->t1_hand;
	SpinLockRelease(&ctl->car_lock);

	iter->start_idx = iter->cdb_idx;
	iter->started = false;

	return iter;
}

static int
CarTrickleIterNext(void *strategy_data, void *iter_state)
{
	CarTrickleIter *iter = (CarTrickleIter *) iter_state;
	CarControl *ctl = iter->ctl;
	CarCDB	   *cdb_arr = CAR_CDB(ctl);

	if (iter->yielded >= iter->max_candidates)
		return -1;

	while (iter->phase < 2)
	{
		/* Walk the clock ring */
		while (iter->cdb_idx >= 0)
		{
			CarCDB	   *cdb = &cdb_arr[iter->cdb_idx];
			int			next_idx = cdb->clock_next;

			/* Detect full loop around the ring */
			if (iter->started && iter->cdb_idx == iter->start_idx)
			{
				iter->cdb_idx = -1;
				break;
			}
			iter->started = true;

			/* Advance for next iteration */
			iter->cdb_idx = next_idx;

			/* Only consider resident entries with cold reference bit */
			if (cdb->buf_id >= 0 && !cdb->reference_bit)
			{
				int			global_id = ctl->first_buf_id + cdb->buf_id;
				BufferDesc *buf = GetBufferDescriptor(global_id);
				uint64		state = pg_atomic_read_u64(&buf->state);

				/* Yield dirty + unpinned buffers */
				if ((state & BM_DIRTY) &&
					(state & BM_VALID) &&
					BUF_STATE_GET_REFCOUNT(state) == 0)
				{
					iter->yielded++;
					return global_id;
				}
			}
		}

		/* Move to next phase */
		iter->phase++;
		if (iter->phase == 1)
		{
			/* Switch to T2 */
			SpinLockAcquire(&ctl->car_lock);
			iter->cdb_idx = ctl->t2_hand;
			SpinLockRelease(&ctl->car_lock);
			iter->start_idx = iter->cdb_idx;
			iter->started = false;
		}
	}

	return -1;		/* exhausted */
}

static void
CarTrickleIterEnd(void *strategy_data, void *iter_state)
{
	pfree(iter_state);
}


/* ----------------------------------------------------------------
 *			CAR vtable and handler
 * ----------------------------------------------------------------
 */

static const BufferPoolRoutine car_pool_routine = {
	.type = T_Invalid,
	.on_hit = CarOnHit,
	.on_miss = CarOnMiss,
	.on_evict = CarOnEvict,
	.on_new_tag = CarOnNewTag,
	.get_victim = CarGetVictim,
	.sync_start = CarSyncStart,
	.notify_trickle = CarNotifyTrickle,
	.trickle_iter_begin = CarTrickleIterBegin,
	.trickle_iter_next = CarTrickleIterNext,
	.trickle_iter_end = CarTrickleIterEnd,
	.hint_vacuum = CarHintVacuum,
	.reject_buffer = CarRejectBuffer,
	.prefetch_hint = CarPrefetchHint,
	.shmem_size = CarShmemSize,
	.shmem_init = CarShmemInit,
	.shutdown = CarShutdown,
};

PG_FUNCTION_INFO_V1(car_pool_handler);

Datum
car_pool_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&car_pool_routine);
}


/* ----------------------------------------------------------------
 *			CAR statistics SRF
 * ----------------------------------------------------------------
 */

#define PG_STAT_GET_CAR_STATS_COLS 17

PG_FUNCTION_INFO_V1(pg_stat_get_car_stats);

Datum
pg_stat_get_car_stats(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	InitMaterializedSRF(fcinfo, 0);

	for (int i = 0; i < NBufferPools; i++)
	{
		Datum		values[PG_STAT_GET_CAR_STATS_COLS] = {0};
		bool		nulls[PG_STAT_GET_CAR_STATS_COLS] = {0};
		BufferPoolDesc *pool = &BufferPoolDescs[i];
		CarControl *ctl;

		if (!pool->bp_active)
			continue;
		if (pool->bp_routine != &car_pool_routine)
			continue;

		{
			PoolLocalState *local = EnsurePoolAttached(pool);

			ctl = (CarControl *) local->strategy_data;
		}
		if (ctl == NULL)
			continue;

		values[0] = NameGetDatum(&pool->bp_name);

		if (OidIsValid(pool->bp_oid))
			values[1] = ObjectIdGetDatum(pool->bp_oid);
		else
			nulls[1] = true;

		SpinLockAcquire(&ctl->car_lock);
		values[2] = Int32GetDatum(ctl->t1_size);
		values[3] = Int32GetDatum(ctl->t2_size);
		values[4] = Int32GetDatum(ctl->b1_size);
		values[5] = Int32GetDatum(ctl->b2_size);
		values[6] = Int32GetDatum(ctl->target_T1_size);
		values[7] = Int32GetDatum(ctl->t1_hand);
		values[8] = Int32GetDatum(ctl->t2_hand);
		SpinLockRelease(&ctl->car_lock);

		/* Flush this backend's pending local stats before reading */
		PoolStatFlush(&car_local_stats[CAR_STAT_LOOKUPS], &ctl->stat_lookups);
		PoolStatFlush(&car_local_stats[CAR_STAT_T1_HITS], &ctl->stat_t1_hits);
		PoolStatFlush(&car_local_stats[CAR_STAT_T2_HITS], &ctl->stat_t2_hits);
		PoolStatFlush(&car_local_stats[CAR_STAT_B1_HITS], &ctl->stat_b1_hits);
		PoolStatFlush(&car_local_stats[CAR_STAT_B2_HITS], &ctl->stat_b2_hits);
		PoolStatFlush(&car_local_stats[CAR_STAT_MISSES], &ctl->stat_misses);
		PoolStatFlush(&car_local_stats[CAR_STAT_T1_EVICTIONS], &ctl->stat_t1_evictions);
		PoolStatFlush(&car_local_stats[CAR_STAT_T2_EVICTIONS], &ctl->stat_t2_evictions);

		values[9] = Int64GetDatum(pg_atomic_read_u64(&ctl->stat_lookups));
		values[10] = Int64GetDatum(pg_atomic_read_u64(&ctl->stat_t1_hits));
		values[11] = Int64GetDatum(pg_atomic_read_u64(&ctl->stat_t2_hits));
		values[12] = Int64GetDatum(pg_atomic_read_u64(&ctl->stat_b1_hits));
		values[13] = Int64GetDatum(pg_atomic_read_u64(&ctl->stat_b2_hits));
		values[14] = Int64GetDatum(pg_atomic_read_u64(&ctl->stat_misses));
		values[15] = Int64GetDatum(pg_atomic_read_u64(&ctl->stat_t1_evictions));
		values[16] = Int64GetDatum(pg_atomic_read_u64(&ctl->stat_t2_evictions));

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
							values, nulls);
	}

	return (Datum) 0;
}


/* ----------------------------------------------------------------
 *			CAR size recommendation advisory
 * ----------------------------------------------------------------
 */

PG_FUNCTION_INFO_V1(pg_bp_car_size_recommendation);

Datum
pg_bp_car_size_recommendation(PG_FUNCTION_ARGS)
{
	Name		pool_name = PG_GETARG_NAME(0);
	TupleDesc	tupdesc;
	Datum		values[4];
	bool		nulls[4] = {false, false, false, false};
	HeapTuple	tuple;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context that cannot accept type record")));

	BlessTupleDesc(tupdesc);

	for (int i = 0; i < NBufferPools; i++)
	{
		BufferPoolDesc *pool = &BufferPoolDescs[i];
		CarControl *ctl;
		int64		b1_hits,
					b2_hits,
					misses,
					lookups,
					t1_hits,
					t2_hits;
		float8		ghost_pressure,
					hit_ratio;
		int			current_size,
					recommended_size;

		if (!pool->bp_active)
			continue;
		if (pool->bp_routine != &car_pool_routine)
			continue;
		if (namestrcmp(&pool->bp_name, NameStr(*pool_name)) != 0)
			continue;

		{
			PoolLocalState *local = EnsurePoolAttached(pool);

			ctl = (CarControl *) local->strategy_data;
		}
		if (ctl == NULL)
			continue;

		current_size = ctl->nbuffers;
		b1_hits = pg_atomic_read_u64(&ctl->stat_b1_hits);
		b2_hits = pg_atomic_read_u64(&ctl->stat_b2_hits);
		misses = pg_atomic_read_u64(&ctl->stat_misses);
		lookups = pg_atomic_read_u64(&ctl->stat_lookups);
		t1_hits = pg_atomic_read_u64(&ctl->stat_t1_hits);
		t2_hits = pg_atomic_read_u64(&ctl->stat_t2_hits);

		ghost_pressure = (float8) (b1_hits + b2_hits) / Max(misses, 1);
		hit_ratio = (lookups > 0) ?
			(float8) (t1_hits + t2_hits) / lookups : 0.0;

		if (ghost_pressure > 0.1)
			recommended_size = Min(current_size * (1.0 + ghost_pressure),
								   current_size * 2);
		else
			recommended_size = current_size;

		values[0] = Int32GetDatum(current_size);
		values[1] = Int32GetDatum(recommended_size);
		values[2] = Float8GetDatum(ghost_pressure);
		values[3] = Float8GetDatum(hit_ratio);

		tuple = heap_form_tuple(tupdesc, values, nulls);
		PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
	}

	ereport(ERROR,
			(errcode(ERRCODE_UNDEFINED_OBJECT),
			 errmsg("CAR pool \"%s\" not found", NameStr(*pool_name))));
	PG_RETURN_NULL();
}

/*
 * _PG_init -- register CAR for use as the DEFAULT pool algorithm.
 */
void
_PG_init(void)
{
	RegisterDefaultPoolAlgorithm(BP_ALGO_CAR, &car_pool_routine);
}
