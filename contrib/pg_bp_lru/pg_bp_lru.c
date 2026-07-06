/*-------------------------------------------------------------------------
 *
 * pg_bp_lru.c
 *	  Least Recently Used (LRU) buffer replacement algorithm.
 *
 * This extension provides a classic LRU algorithm as a BufferPoolRoutine
 * handler, usable with CREATE BUFFER POOL ... HANDLER lru_pool_handler.
 *
 * LRU maintains a single doubly-linked list of all cached pages, ordered
 * by access recency.  On every cache hit, the accessed page moves to the
 * MRU (most recently used) end.  On eviction, the page at the LRU (least
 * recently used) end is chosen as victim.
 *
 * Properties:
 *   - O(1) hit handling (move to MRU)
 *   - O(n) worst-case victim search (skip pinned buffers from LRU end)
 *   - No ghost lists, no adaptation
 *   - Vulnerable to sequential scan pollution
 *   - Useful as a baseline for benchmarking more sophisticated algorithms
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  contrib/pg_bp_lru/pg_bp_lru.c
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

PG_MODULE_MAGIC_EXT(.name = "pg_bp_lru", .version = PG_VERSION);

void		_PG_init(void);

/* Stat slot indices for PoolStatIncrement */
#define LRU_STAT_HITS		0
#define LRU_STAT_MISSES		1
#define LRU_STAT_EVICTIONS	2
#define LRU_NUM_STATS		3

static uint64 lru_local_stats[LRU_NUM_STATS];


/*
 * LRU list node -- one per buffer in the pool.
 */
typedef struct LruNode
{
	int			prev;			/* index of previous node (-1 = none) */
	int			next;			/* index of next node (-1 = none) */
	bool		on_list;		/* true if this node is on the LRU list */
} LruNode;

/*
 * LruControl -- shared-memory control block for LRU pool.
 *
 * The LRU list is ordered from LRU (head) to MRU (tail).
 * Buffers not yet used are not on the list (on_list == false).
 *
 * Variable-length array follows:
 *   LruNode nodes[nbuffers]
 */
typedef struct LruControl
{
	slock_t		lru_lock;		/* spinlock protecting the list */

	int			nbuffers;		/* number of physical buffers */
	int			first_buf_id;	/* global buffer ID of first buffer */
	int			list_head;		/* LRU end (-1 = empty) */
	int			list_tail;		/* MRU end (-1 = empty) */
	int			list_size;		/* number of buffers on list */

	/* Statistics */
	pg_atomic_uint64 stat_hits;
	pg_atomic_uint64 stat_misses;
	pg_atomic_uint64 stat_evictions;
} LruControl;

#define LRU_NODES(ctl)	((LruNode *) ((char *)(ctl) + MAXALIGN(sizeof(LruControl))))

/*
 * Per-backend LRU state, scoped to a specific pool.
 */
typedef struct LruBackendState
{
	LruControl *ctl;
	bool		vacuum_hint;
} LruBackendState;

#define MAX_LRU_POOLS  MAX_BUFFER_POOLS
static LruBackendState lru_backend_states[MAX_LRU_POOLS];
static int	lru_num_states = 0;

static LruBackendState *
lru_get_backend_state(LruControl *ctl)
{
	for (int i = 0; i < lru_num_states; i++)
	{
		if (lru_backend_states[i].ctl == ctl)
			return &lru_backend_states[i];
	}
	Assert(lru_num_states < MAX_LRU_POOLS);
	lru_backend_states[lru_num_states].ctl = ctl;
	lru_backend_states[lru_num_states].vacuum_hint = false;
	return &lru_backend_states[lru_num_states++];
}


/* ----------------------------------------------------------------
 *			LRU list operations (caller must hold lru_lock)
 * ----------------------------------------------------------------
 */

/*
 * Remove a node from the LRU list.
 */
static inline void
lru_list_remove(LruControl *ctl, LruNode *nodes, int idx)
{
	LruNode    *node = &nodes[idx];

	Assert(node->on_list);

	if (node->prev >= 0)
		nodes[node->prev].next = node->next;
	else
		ctl->list_head = node->next;

	if (node->next >= 0)
		nodes[node->next].prev = node->prev;
	else
		ctl->list_tail = node->prev;

	node->prev = -1;
	node->next = -1;
	node->on_list = false;
	ctl->list_size--;
}

/*
 * Insert a node at the MRU (tail) end of the list.
 */
static inline void
lru_mru_insert(LruControl *ctl, LruNode *nodes, int idx)
{
	LruNode    *node = &nodes[idx];

	Assert(!node->on_list);

	node->next = -1;
	node->prev = ctl->list_tail;

	if (ctl->list_tail >= 0)
		nodes[ctl->list_tail].next = idx;
	else
		ctl->list_head = idx;	/* list was empty */

	ctl->list_tail = idx;
	node->on_list = true;
	ctl->list_size++;
}

/*
 * Insert a node at the LRU (head) end of the list.
 * Used for VACUUM pages so they are evicted first.
 */
static inline void
lru_lru_insert(LruControl *ctl, LruNode *nodes, int idx)
{
	LruNode    *node = &nodes[idx];

	Assert(!node->on_list);

	node->prev = -1;
	node->next = ctl->list_head;

	if (ctl->list_head >= 0)
		nodes[ctl->list_head].prev = idx;
	else
		ctl->list_tail = idx;	/* list was empty */

	ctl->list_head = idx;
	node->on_list = true;
	ctl->list_size++;
}


/* ----------------------------------------------------------------
 *			LRU vtable callback implementations
 * ----------------------------------------------------------------
 */

/*
 * LruOnHit -- move the accessed buffer to the MRU position.
 */
static void
LruOnHit(void *strategy_data, int buf_id, BufferTag *tag)
{
	LruControl *ctl = (LruControl *) strategy_data;
	LruNode    *nodes = LRU_NODES(ctl);
	int			local_id = buf_id - ctl->first_buf_id;

	PoolStatIncrement(&lru_local_stats[LRU_STAT_HITS], &ctl->stat_hits);

	if (local_id < 0 || local_id >= ctl->nbuffers)
		return;

	SpinLockAcquire(&ctl->lru_lock);

	if (nodes[local_id].on_list)
	{
		lru_list_remove(ctl, nodes, local_id);
		lru_mru_insert(ctl, nodes, local_id);
	}

	SpinLockRelease(&ctl->lru_lock);
}

/*
 * LruOnMiss -- record a cache miss.
 */
static void
LruOnMiss(void *strategy_data, BufferTag *tag)
{
	LruControl *ctl = (LruControl *) strategy_data;

	PoolStatIncrement(&lru_local_stats[LRU_STAT_MISSES], &ctl->stat_misses);
}

/*
 * LruOnEvict -- remove the evicted buffer from the LRU list.
 */
static void
LruOnEvict(void *strategy_data, int buf_id, BufferTag *old_tag)
{
	LruControl *ctl = (LruControl *) strategy_data;
	LruNode    *nodes = LRU_NODES(ctl);
	int			local_id = buf_id - ctl->first_buf_id;

	if (local_id < 0 || local_id >= ctl->nbuffers)
		return;

	SpinLockAcquire(&ctl->lru_lock);

	if (nodes[local_id].on_list)
		lru_list_remove(ctl, nodes, local_id);

	SpinLockRelease(&ctl->lru_lock);

	PoolStatIncrement(&lru_local_stats[LRU_STAT_EVICTIONS], &ctl->stat_evictions);
}

/*
 * LruOnNewTag -- add the newly loaded buffer to the list.
 *
 * VACUUM optimization: insert at LRU end (head) so VACUUM-loaded pages
 * are evicted first, preventing cache pollution.
 */
static void
LruOnNewTag(void *strategy_data, int buf_id, BufferTag *new_tag,
			bool vacuum_hint)
{
	LruControl *ctl = (LruControl *) strategy_data;
	LruBackendState *state = lru_get_backend_state(ctl);
	LruNode    *nodes = LRU_NODES(ctl);
	int			local_id = buf_id - ctl->first_buf_id;

	if (local_id < 0 || local_id >= ctl->nbuffers)
		return;

	SpinLockAcquire(&ctl->lru_lock);

	/* Should not already be on the list after eviction */
	Assert(!nodes[local_id].on_list);

	if (vacuum_hint || state->vacuum_hint)
		lru_lru_insert(ctl, nodes, local_id);
	else
		lru_mru_insert(ctl, nodes, local_id);

	SpinLockRelease(&ctl->lru_lock);
}


/* ----------------------------------------------------------------
 *			LRU get_victim implementation
 * ----------------------------------------------------------------
 */

/*
 * LruGetVictim -- select a victim buffer from the LRU end.
 *
 * First tries to find a free (unused) buffer.  If all buffers are
 * on the LRU list, walks from the LRU end looking for an unpinned
 * buffer.
 */
static BufferDesc *
LruGetVictim(void *strategy_data, BufferAccessStrategy strategy,
			 uint64 *buf_state, bool *from_ring)
{
	LruControl *ctl = (LruControl *) strategy_data;
	LruNode    *nodes = LRU_NODES(ctl);
	BufferDesc *buf;
	int			idx;

	*from_ring = false;

	/*
	 * Try free (untracked) buffers first.  During warmup, not all buffers are
	 * on the list yet.
	 */
	if (ctl->list_size < ctl->nbuffers)
	{
		for (int i = 0; i < ctl->nbuffers; i++)
		{
			uint64		old_buf_state;
			uint64		local_buf_state;

			if (nodes[i].on_list)
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
				if (pg_atomic_compare_exchange_u64(&buf->state, &old_buf_state,
												   local_buf_state))
				{
					*buf_state = local_buf_state;
					TrackNewBufferPin(BufferDescriptorGetBuffer(buf));
					return buf;
				}
			}
		}
	}

	/*
	 * No free buffers.  Walk from LRU end (head) looking for an unpinned
	 * buffer to evict.
	 */
	SpinLockAcquire(&ctl->lru_lock);

	idx = ctl->list_head;
	while (idx >= 0)
	{
		uint64		old_buf_state;
		uint64		local_buf_state;

		buf = GetBufferDescriptor(ctl->first_buf_id + idx);

		old_buf_state = pg_atomic_read_u64(&buf->state);
		for (;;)
		{
			local_buf_state = old_buf_state;

			if (BUF_STATE_GET_REFCOUNT(local_buf_state) != 0)
				break;			/* pinned, try next */

			if (unlikely(local_buf_state & BM_LOCKED))
			{
				SpinLockRelease(&ctl->lru_lock);
				old_buf_state = WaitBufHdrUnlocked(buf);
				SpinLockAcquire(&ctl->lru_lock);
				continue;
			}

			local_buf_state += BUF_REFCOUNT_ONE;
			if (pg_atomic_compare_exchange_u64(&buf->state, &old_buf_state,
											   local_buf_state))
			{
				*buf_state = local_buf_state;
				SpinLockRelease(&ctl->lru_lock);
				TrackNewBufferPin(BufferDescriptorGetBuffer(buf));
				return buf;
			}
		}

		idx = nodes[idx].next;
	}

	SpinLockRelease(&ctl->lru_lock);

	ereport(ERROR,
			(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
			 errmsg("no unpinned buffers available in LRU pool"),
			 errhint("Increase the buffer pool size or reduce concurrent queries.")));
	pg_unreachable();
}


/* ----------------------------------------------------------------
 *			LRU trickle iterator
 * ----------------------------------------------------------------
 */

typedef struct LruTrickleIter
{
	int			current;		/* current position in LRU list */
	int			remaining;		/* candidates left to return */
} LruTrickleIter;

/*
 * LruTrickleIterBegin -- start iterating dirty buffers from LRU end.
 */
static void *
LruTrickleIterBegin(void *strategy_data, int max_candidates)
{
	LruControl *ctl = (LruControl *) strategy_data;
	LruTrickleIter *iter;

	iter = (LruTrickleIter *) MemoryContextAlloc(TopMemoryContext,
												 sizeof(LruTrickleIter));

	SpinLockAcquire(&ctl->lru_lock);
	iter->current = ctl->list_head;
	SpinLockRelease(&ctl->lru_lock);

	iter->remaining = max_candidates;
	return iter;
}

/*
 * LruTrickleIterNext -- return next dirty+unpinned buffer ID from LRU end.
 */
static int
LruTrickleIterNext(void *strategy_data, void *iter_data)
{
	LruControl *ctl = (LruControl *) strategy_data;
	LruTrickleIter *iter = (LruTrickleIter *) iter_data;
	LruNode    *nodes = LRU_NODES(ctl);

	while (iter->current >= 0 && iter->remaining > 0)
	{
		int			local_id = iter->current;
		BufferDesc *buf = GetBufferDescriptor(ctl->first_buf_id + local_id);
		uint64		state = pg_atomic_read_u64(&buf->state);

		/* Advance iterator (snapshot next under lock) */
		SpinLockAcquire(&ctl->lru_lock);
		iter->current = nodes[local_id].next;
		SpinLockRelease(&ctl->lru_lock);

		/* Return dirty+unpinned buffers */
		if ((state & BM_VALID) && (state & BM_DIRTY) &&
			BUF_STATE_GET_REFCOUNT(state) == 0)
		{
			iter->remaining--;
			return ctl->first_buf_id + local_id;
		}
	}

	return -1;
}

/*
 * LruTrickleIterEnd -- release iterator resources.
 */
static void
LruTrickleIterEnd(void *strategy_data, void *iter_data)
{
	pfree(iter_data);
}


/* ----------------------------------------------------------------
 *			LRU sync/trickle support
 * ----------------------------------------------------------------
 */

static int
LruSyncStart(void *strategy_data, uint32 *complete_passes,
			 uint32 *num_buf_alloc)
{
	if (complete_passes)
		*complete_passes = 0;
	if (num_buf_alloc)
		*num_buf_alloc = 0;
	return 0;
}

static void
LruNotifyTrickle(void *strategy_data, int bgwprocno)
{
	/* Per-pool trickle writers handle this */
}

static bool
LruRejectBuffer(void *strategy_data, BufferAccessStrategy strategy,
				BufferDesc *buf, bool from_ring)
{
	return false;
}

/*
 * LruHintVacuum -- hint that VACUUM is active.
 *
 * When VACUUM is active, newly loaded pages are placed at the LRU end
 * (head) to be evicted first, preventing VACUUM scans from polluting
 * the cache.
 */
static void
LruHintVacuum(void *strategy_data, bool vacuum_active)
{
	LruControl *ctl = (LruControl *) strategy_data;
	LruBackendState *state = lru_get_backend_state(ctl);

	state->vacuum_hint = vacuum_active;
}


/* ----------------------------------------------------------------
 *			LRU lifecycle (shmem_size / shmem_init / shutdown)
 * ----------------------------------------------------------------
 */

static Size
LruShmemSize(int nbuffers)
{
	Size		size;

	size = MAXALIGN(sizeof(LruControl));
	size += MAXALIGN(sizeof(LruNode) * nbuffers);

	return size;
}

static void
LruShmemInit(void *strategy_data, int nbuffers, int first_buf_id, bool init)
{
	LruControl *ctl = (LruControl *) strategy_data;
	LruNode    *nodes;

	if (!init)
	{
		Assert(ctl->nbuffers == nbuffers);
		return;
	}

	SpinLockInit(&ctl->lru_lock);
	ctl->nbuffers = nbuffers;
	ctl->first_buf_id = first_buf_id;
	ctl->list_head = -1;
	ctl->list_tail = -1;
	ctl->list_size = 0;

	pg_atomic_init_u64(&ctl->stat_hits, 0);
	pg_atomic_init_u64(&ctl->stat_misses, 0);
	pg_atomic_init_u64(&ctl->stat_evictions, 0);

	/* Initialize all nodes as not on list */
	nodes = LRU_NODES(ctl);
	for (int i = 0; i < nbuffers; i++)
	{
		nodes[i].prev = -1;
		nodes[i].next = -1;
		nodes[i].on_list = false;
	}
}

static void
LruShutdown(void *strategy_data)
{
	/* Nothing to clean up; all state is in DSM */
}


/* ----------------------------------------------------------------
 *			LRU vtable and handler function
 * ----------------------------------------------------------------
 */

static const BufferPoolRoutine lru_pool_routine = {
	.type = T_Invalid,
	.on_hit = LruOnHit,
	.on_miss = LruOnMiss,
	.on_evict = LruOnEvict,
	.on_new_tag = LruOnNewTag,
	.get_victim = LruGetVictim,
	.sync_start = LruSyncStart,
	.notify_trickle = LruNotifyTrickle,
	.trickle_iter_begin = LruTrickleIterBegin,
	.trickle_iter_next = LruTrickleIterNext,
	.trickle_iter_end = LruTrickleIterEnd,
	.hint_vacuum = LruHintVacuum,
	.reject_buffer = LruRejectBuffer,
	.prefetch_hint = NULL,
	.shmem_size = LruShmemSize,
	.shmem_init = LruShmemInit,
	.shutdown = LruShutdown,
};

PG_FUNCTION_INFO_V1(lru_pool_handler);

Datum
lru_pool_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&lru_pool_routine);
}


/* ----------------------------------------------------------------
 *			LRU statistics SRF
 * ----------------------------------------------------------------
 */

#define PG_STAT_GET_LRU_STATS_COLS 6

PG_FUNCTION_INFO_V1(pg_stat_get_lru_stats);

Datum
pg_stat_get_lru_stats(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	InitMaterializedSRF(fcinfo, 0);

	for (int i = 0; i < NBufferPools; i++)
	{
		Datum		values[PG_STAT_GET_LRU_STATS_COLS] = {0};
		bool		nulls[PG_STAT_GET_LRU_STATS_COLS] = {0};
		BufferPoolDesc *pool = &BufferPoolDescs[i];
		LruControl *ctl;

		if (!pool->bp_active)
			continue;

		if (pool->bp_routine != &lru_pool_routine)
			continue;

		{
			PoolLocalState *local = EnsurePoolAttached(pool);

			ctl = (LruControl *) local->strategy_data;
		}

		if (ctl == NULL)
			continue;

		values[0] = NameGetDatum(&pool->bp_name);

		if (OidIsValid(pool->bp_oid))
			values[1] = ObjectIdGetDatum(pool->bp_oid);
		else
			nulls[1] = true;

		SpinLockAcquire(&ctl->lru_lock);
		values[2] = Int32GetDatum(ctl->list_size);
		SpinLockRelease(&ctl->lru_lock);

		/* Flush this backend's pending local stats before reading */
		PoolStatFlush(&lru_local_stats[LRU_STAT_HITS], &ctl->stat_hits);
		PoolStatFlush(&lru_local_stats[LRU_STAT_MISSES], &ctl->stat_misses);
		PoolStatFlush(&lru_local_stats[LRU_STAT_EVICTIONS], &ctl->stat_evictions);

		values[3] = Int64GetDatum(pg_atomic_read_u64(&ctl->stat_hits));
		values[4] = Int64GetDatum(pg_atomic_read_u64(&ctl->stat_misses));
		values[5] = Int64GetDatum(pg_atomic_read_u64(&ctl->stat_evictions));

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
							 values, nulls);
	}

	return (Datum) 0;
}

/*
 * _PG_init -- register LRU for use as the DEFAULT pool algorithm.
 */
void
_PG_init(void)
{
	RegisterDefaultPoolAlgorithm("lru", &lru_pool_routine);
}
