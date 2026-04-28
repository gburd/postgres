/*-------------------------------------------------------------------------
 *
 * pg_bp_rcrd.c
 *	  RCRD (Recent Consecutive Reuse Distance) buffer replacement algorithm.
 *
 * This extension provides the RCRD algorithm as a BufferPoolRoutine handler,
 * usable with CREATE BUFFER POOL ... HANDLER rcrd_pool_handler.
 *
 * RCRD is based on LIRS2 (Chen Zhong, Xingsheng Zhao, Song Jiang, 2013)
 * and tracks Consecutive Reuse Distance (CRD) -- the number of distinct
 * pages accessed between two consecutive accesses to the same page.
 *
 * Key differences from LIRS:
 *   - Uses CRD (consecutive reuse distance) instead of IRR
 *   - Simpler stack management: no bottom-up stack pruning needed
 *   - Lower memory overhead: uses a compact recency list with distance
 *     counters instead of a full stack with ghost entries
 *   - Better performance on scan-heavy workloads due to distance tracking
 *
 * Data structures:
 *   R (recency list)  - doubly-linked list ordered by recency, tracks
 *                        all pages (both resident and non-resident) with
 *                        their CRD values.
 *   Q (cold list)     - doubly-linked list of resident cold pages
 *                        (eviction candidates).
 *   Ghost hash        - hash table for quick lookup of non-resident entries.
 *
 * Pages are classified:
 *   HOT  - CRD <= hot_threshold; kept in cache (like LIR in LIRS)
 *   COLD - CRD > hot_threshold; eviction candidates in Q (like HIR in LIRS)
 *
 * The hot_threshold adapts over time: ghost hits indicate the threshold
 * should increase (more pages deserve hot status), while excess hot pages
 * suggest it should decrease.
 *
 * References:
 *   C. Zhong, X. Zhao, and S. Jiang, "LIRS2: An Improved LIRS
 *   Replacement Algorithm", Technical Report, 2013.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  contrib/pg_bp_rcrd/pg_bp_rcrd.c
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

PG_MODULE_MAGIC_EXT(.name = "pg_bp_rcrd", .version = PG_VERSION);

void		_PG_init(void);

/* RCRD page status values */
#define RCRD_STATUS_HOT		0	/* Low CRD -- hot page, kept in cache */
#define RCRD_STATUS_COLD	1	/* High CRD -- cold page, eviction candidate */
#define RCRD_STATUS_GHOST	2	/* Non-resident (ghost entry) */
#define RCRD_STATUS_UNUSED	(-1)

/* Stat slot indices for PoolStatIncrement */
#define RCRD_STAT_LOOKUPS			0
#define RCRD_STAT_HOT_HITS			1
#define RCRD_STAT_COLD_HITS			2
#define RCRD_STAT_GHOST_HITS		3
#define RCRD_STAT_MISSES			4
#define RCRD_STAT_DEMOTIONS			5
#define RCRD_STAT_PROMOTIONS		6
#define RCRD_STAT_EVICTIONS			7
#define RCRD_STAT_THRESHOLD_RAISES	8
#define RCRD_NUM_STATS				9

static uint64 rcrd_local_stats[RCRD_NUM_STATS];

/*
 * RCRD Page Entry (RPE).
 *
 * Each RPE tracks a page's position in the RCRD structures.
 * For HOT/COLD entries, buf_id is the pool-local buffer index.
 * For GHOST entries, buf_id is -1.
 */
typedef struct RcrdEntry
{
	BufferTag	buf_tag;		/* page identity */
	int			buf_id;			/* pool-local buffer index (-1 for ghost) */
	int			status;			/* RCRD_STATUS_HOT/COLD/GHOST/UNUSED */
	int			crd;			/* consecutive reuse distance */
	int			ghost_next;		/* next in ghost hash chain (-1 = end) */

	/* Recency list R links (doubly-linked: bottom=LRU, top=MRU) */
	int			r_prev;			/* toward LRU (bottom) */
	int			r_next;			/* toward MRU (top) */

	/* Cold list Q links (doubly-linked: head=eviction, tail=insertion) */
	int			q_prev;			/* toward head (eviction end) */
	int			q_next;			/* toward tail (insertion end) */
} RcrdEntry;

/*
 * RCRD shared-memory control block.
 */
typedef struct RcrdControl
{
	slock_t		rcrd_lock;		/* spinlock protecting all mutable fields */

	int			nbuffers;		/* number of physical buffers in this pool */
	int			first_buf_id;	/* global buffer ID of first buffer in pool */
	int			nentries;		/* total entry slots (3 * nbuffers) */
	int			hot_capacity;	/* max HOT pages (adaptive threshold) */
	int			max_r_size;		/* 2 * nbuffers (bounded recency list) */

	/* Recency list R */
	int			r_top;			/* MRU end (-1 if empty) */
	int			r_bottom;		/* LRU end (-1 if empty) */
	int			r_size;

	/* Cold list Q (resident cold pages) */
	int			q_head;			/* eviction end (-1 if empty) */
	int			q_tail;			/* insertion end (-1 if empty) */
	int			q_size;

	/* Current counts */
	int			hot_count;		/* resident HOT pages */
	int			cold_count;		/* resident COLD pages */
	int			ghost_count;	/* non-resident ghost entries */

	/* CRD tracking */
	int			distinct_counter;	/* running count of distinct pages seen */

	/* Ghost hash table */
	int			ghost_hash_size;

	/* Free entry list (singly-linked via r_next) */
	int			free_list;

	/* Statistics (atomics -- no lock needed to increment) */
	pg_atomic_uint64 stat_lookups;
	pg_atomic_uint64 stat_hot_hits;
	pg_atomic_uint64 stat_cold_hits;
	pg_atomic_uint64 stat_ghost_hits;
	pg_atomic_uint64 stat_misses;
	pg_atomic_uint64 stat_demotions;	/* HOT -> COLD */
	pg_atomic_uint64 stat_promotions;	/* COLD/ghost -> HOT */
	pg_atomic_uint64 stat_evictions;
	pg_atomic_uint64 stat_threshold_raises;

	int			bgwprocno;		/* trickle writer procno (-1 = none) */

	/*
	 * Variable-length arrays follow: RcrdEntry  entries[nentries] int
	 * ghost_hash[ghost_hash_size] int        buf_to_entry[nbuffers]
	 */
} RcrdControl;

/*
 * Per-backend RCRD state for cross-call communication.
 */
typedef struct RcrdBackendState
{
	RcrdControl *ctl;
	int			ghost_entry;	/* entry index of ghost hit (-1 = miss) */
	bool		vacuum_hint;
} RcrdBackendState;

#define MAX_RCRD_POOLS	MAX_BUFFER_POOLS
static RcrdBackendState rcrd_backend_states[MAX_RCRD_POOLS];
static int	rcrd_num_states = 0;

static RcrdBackendState *
rcrd_get_backend_state(RcrdControl *ctl)
{
	for (int i = 0; i < rcrd_num_states; i++)
	{
		if (rcrd_backend_states[i].ctl == ctl)
			return &rcrd_backend_states[i];
	}
	Assert(rcrd_num_states < MAX_RCRD_POOLS);
	rcrd_backend_states[rcrd_num_states].ctl = ctl;
	rcrd_backend_states[rcrd_num_states].ghost_entry = -1;
	rcrd_backend_states[rcrd_num_states].vacuum_hint = false;
	return &rcrd_backend_states[rcrd_num_states++];
}

/* Accessor macros for variable-length arrays */
#define RCRD_ENTRIES(ctl)	((RcrdEntry *) ((char *)(ctl) + sizeof(RcrdControl)))
#define RCRD_GHOST_HASH(ctl) ((int *) ((char *)(ctl) + sizeof(RcrdControl) + \
							  sizeof(RcrdEntry) * (ctl)->nentries))
#define RCRD_BUF_TO_ENTRY(ctl) ((int *) ((char *)(ctl) + sizeof(RcrdControl) + \
							   sizeof(RcrdEntry) * (ctl)->nentries + \
							   sizeof(int) * (ctl)->ghost_hash_size))


/* ----------------------------------------------------------------
 *			Ghost hash table operations
 * ----------------------------------------------------------------
 */

static inline uint32
rcrd_ghost_hash_bucket(RcrdControl *ctl, const BufferTag *tag)
{
	uint32		h = tag_hash(tag, sizeof(BufferTag));

	return h & (ctl->ghost_hash_size - 1);
}

static int
rcrd_ghost_lookup(RcrdControl *ctl, const BufferTag *tag)
{
	int		   *ghost_hash = RCRD_GHOST_HASH(ctl);
	RcrdEntry  *entries = RCRD_ENTRIES(ctl);
	uint32		bucket = rcrd_ghost_hash_bucket(ctl, tag);
	int			idx = ghost_hash[bucket];

	while (idx >= 0)
	{
		if (BufferTagsEqual(&entries[idx].buf_tag, tag))
			return idx;
		idx = entries[idx].ghost_next;
	}
	return -1;
}

static void
rcrd_ghost_insert(RcrdControl *ctl, int entry_idx)
{
	int		   *ghost_hash = RCRD_GHOST_HASH(ctl);
	RcrdEntry  *entries = RCRD_ENTRIES(ctl);
	uint32		bucket = rcrd_ghost_hash_bucket(ctl, &entries[entry_idx].buf_tag);

	entries[entry_idx].ghost_next = ghost_hash[bucket];
	ghost_hash[bucket] = entry_idx;
}

static void
rcrd_ghost_remove(RcrdControl *ctl, int entry_idx)
{
	int		   *ghost_hash = RCRD_GHOST_HASH(ctl);
	RcrdEntry  *entries = RCRD_ENTRIES(ctl);
	uint32		bucket = rcrd_ghost_hash_bucket(ctl, &entries[entry_idx].buf_tag);
	int			prev = -1;
	int			cur = ghost_hash[bucket];

	while (cur >= 0)
	{
		if (cur == entry_idx)
		{
			if (prev < 0)
				ghost_hash[bucket] = entries[cur].ghost_next;
			else
				entries[prev].ghost_next = entries[cur].ghost_next;
			entries[cur].ghost_next = -1;
			return;
		}
		prev = cur;
		cur = entries[cur].ghost_next;
	}
}


/* ----------------------------------------------------------------
 *			Entry allocation and free list
 * ----------------------------------------------------------------
 */

static void
rcrd_free_entry(RcrdControl *ctl, RcrdEntry *entries, int entry_idx)
{
	ClearBufferTag(&entries[entry_idx].buf_tag);
	entries[entry_idx].buf_id = -1;
	entries[entry_idx].status = RCRD_STATUS_UNUSED;
	entries[entry_idx].crd = 0;
	entries[entry_idx].ghost_next = -1;
	entries[entry_idx].r_prev = -1;
	entries[entry_idx].q_prev = -1;
	entries[entry_idx].q_next = -1;
	/* Use r_next for free list chaining */
	entries[entry_idx].r_next = ctl->free_list;
	ctl->free_list = entry_idx;
}


/* ----------------------------------------------------------------
 *			Recency list R operations (doubly-linked list)
 *
 * r_bottom = LRU end (oldest access)
 * r_top    = MRU end (most recently accessed)
 * r_prev   = toward LRU (bottom)
 * r_next   = toward MRU (top)
 * ----------------------------------------------------------------
 */

/*
 * Push an entry onto the recency list top (MRU position).
 */
static void
rcrd_r_push(RcrdControl *ctl, RcrdEntry *entries, int entry_idx)
{
	RcrdEntry  *e = &entries[entry_idx];

	e->r_next = -1;
	e->r_prev = ctl->r_top;

	if (ctl->r_top >= 0)
		entries[ctl->r_top].r_next = entry_idx;
	ctl->r_top = entry_idx;

	if (ctl->r_bottom < 0)
		ctl->r_bottom = entry_idx;

	ctl->r_size++;
}

/*
 * Remove an entry from the recency list.
 */
static void
rcrd_r_remove(RcrdControl *ctl, RcrdEntry *entries, int entry_idx)
{
	RcrdEntry  *e = &entries[entry_idx];

	if (e->r_prev >= 0)
		entries[e->r_prev].r_next = e->r_next;
	else
		ctl->r_bottom = e->r_next;

	if (e->r_next >= 0)
		entries[e->r_next].r_prev = e->r_prev;
	else
		ctl->r_top = e->r_prev;

	e->r_prev = -1;
	e->r_next = -1;
	ctl->r_size--;
}

/*
 * Move an entry to the recency list top (MRU position).
 *
 * Safe to call on entries not currently in the recency list (e.g. COLD
 * entries pruned by rcrd_r_prune, or vacuum-hinted inserts that were
 * never added to R).  In that case the entry is simply pushed onto the
 * top without a preceding remove -- avoiding the corruption that would
 * result from rcrd_r_remove blindly updating r_bottom / r_top.
 */
static void
rcrd_r_move_to_top(RcrdControl *ctl, RcrdEntry *entries, int entry_idx)
{
	if (entries[entry_idx].r_prev >= 0 ||
		entries[entry_idx].r_next >= 0 ||
		ctl->r_bottom == entry_idx)
	{
		rcrd_r_remove(ctl, entries, entry_idx);
	}
	rcrd_r_push(ctl, entries, entry_idx);
}

/*
 * RCRD recency list pruning.
 *
 * When the recency list exceeds 2*nbuffers, remove non-HOT entries
 * from the bottom.  Ghost entries are freed entirely; resident COLD
 * entries lose their recency list position but remain in Q.
 */
static void
rcrd_r_prune(RcrdControl *ctl, RcrdEntry *entries)
{
	while (ctl->r_size > ctl->max_r_size && ctl->r_bottom >= 0)
	{
		int			idx = ctl->r_bottom;

		if (entries[idx].status == RCRD_STATUS_HOT)
			break;				/* never prune HOT entries */

		rcrd_r_remove(ctl, entries, idx);

		if (entries[idx].status == RCRD_STATUS_GHOST)
		{
			rcrd_ghost_remove(ctl, idx);
			ctl->ghost_count--;
			rcrd_free_entry(ctl, entries, idx);
		}
		/* Resident COLD: stays in Q, just loses recency list position */
	}
}


/* ----------------------------------------------------------------
 *			Cold list Q operations (resident COLD doubly-linked list)
 *
 * q_head = eviction end (oldest COLD)
 * q_tail = insertion end (newest COLD)
 * ----------------------------------------------------------------
 */

static void
rcrd_q_append(RcrdControl *ctl, RcrdEntry *entries, int entry_idx)
{
	RcrdEntry  *e = &entries[entry_idx];

	e->q_next = -1;
	e->q_prev = ctl->q_tail;

	if (ctl->q_tail >= 0)
		entries[ctl->q_tail].q_next = entry_idx;
	ctl->q_tail = entry_idx;

	if (ctl->q_head < 0)
		ctl->q_head = entry_idx;

	ctl->q_size++;
}

static void
rcrd_q_prepend(RcrdControl *ctl, RcrdEntry *entries, int entry_idx)
{
	RcrdEntry  *e = &entries[entry_idx];

	e->q_prev = -1;
	e->q_next = ctl->q_head;

	if (ctl->q_head >= 0)
		entries[ctl->q_head].q_prev = entry_idx;
	ctl->q_head = entry_idx;

	if (ctl->q_tail < 0)
		ctl->q_tail = entry_idx;

	ctl->q_size++;
}

static void
rcrd_q_remove(RcrdControl *ctl, RcrdEntry *entries, int entry_idx)
{
	RcrdEntry  *e = &entries[entry_idx];

	if (e->q_prev >= 0)
		entries[e->q_prev].q_next = e->q_next;
	else
		ctl->q_head = e->q_next;

	if (e->q_next >= 0)
		entries[e->q_next].q_prev = e->q_prev;
	else
		ctl->q_tail = e->q_prev;

	e->q_prev = -1;
	e->q_next = -1;
	ctl->q_size--;
}


/* ----------------------------------------------------------------
 *			Entry allocation (cont.)
 * ----------------------------------------------------------------
 */

static int
rcrd_alloc_entry(RcrdControl *ctl, RcrdEntry *entries)
{
	int			idx;

	/* Try free list first */
	if (ctl->free_list >= 0)
	{
		idx = ctl->free_list;
		ctl->free_list = entries[idx].r_next;
		entries[idx].r_next = -1;
		entries[idx].r_prev = -1;
		entries[idx].q_prev = -1;
		entries[idx].q_next = -1;
		entries[idx].ghost_next = -1;
		entries[idx].crd = 0;
		entries[idx].status = RCRD_STATUS_UNUSED;
		return idx;
	}

	/* Recycle oldest ghost from recency list bottom */
	{
		int			scan = ctl->r_bottom;

		while (scan >= 0)
		{
			int			next_scan = entries[scan].r_next;

			if (entries[scan].status == RCRD_STATUS_GHOST)
			{
				idx = scan;
				rcrd_ghost_remove(ctl, idx);
				rcrd_r_remove(ctl, entries, idx);
				ctl->ghost_count--;

				ClearBufferTag(&entries[idx].buf_tag);
				entries[idx].buf_id = -1;
				entries[idx].status = RCRD_STATUS_UNUSED;
				entries[idx].crd = 0;
				entries[idx].ghost_next = -1;
				entries[idx].r_prev = -1;
				entries[idx].r_next = -1;
				entries[idx].q_prev = -1;
				entries[idx].q_next = -1;
				return idx;
			}
			scan = next_scan;
		}
	}

	/*
	 * Should not reach here: the on_evict fix ensures pruned COLD entries are
	 * freed rather than orphaned as ghosts.  Release the spinlock before
	 * ereport to avoid leaving it permanently locked (which would cause a
	 * stuck-spinlock PANIC in other processes such as the trickle writer).
	 */
	SpinLockRelease(&ctl->rcrd_lock);
	ereport(ERROR,
			(errcode(ERRCODE_OUT_OF_MEMORY),
			 errmsg("RCRD: no entries available for recycling")));
	pg_unreachable();
}


/* ----------------------------------------------------------------
 *			RCRD demote bottom HOT to COLD
 *
 * Called when hot_count exceeds hot_capacity after a promotion.
 * Searches from the recency list bottom for the first HOT entry,
 * demotes it to COLD, and appends to Q tail.
 * ----------------------------------------------------------------
 */

static void
rcrd_demote_bottom_hot(RcrdControl *ctl, RcrdEntry *entries)
{
	int			idx = ctl->r_bottom;

	while (idx >= 0 && entries[idx].status != RCRD_STATUS_HOT)
		idx = entries[idx].r_next;

	if (idx < 0)
		return;

	entries[idx].status = RCRD_STATUS_COLD;
	ctl->hot_count--;
	ctl->cold_count++;

	rcrd_r_remove(ctl, entries, idx);
	rcrd_q_append(ctl, entries, idx);

	PoolStatIncrement(&rcrd_local_stats[RCRD_STAT_DEMOTIONS], &ctl->stat_demotions);
}


/* ----------------------------------------------------------------
 *			RCRD CRD computation
 *
 * Compute the consecutive reuse distance for a page by counting
 * distinct entries between the page's old recency list position
 * and the current top.  For simplicity, we use the recency list
 * position: CRD = number of entries above us in R before re-access.
 * ----------------------------------------------------------------
 */

static int
rcrd_compute_crd(RcrdControl *ctl, RcrdEntry *entries, int entry_idx)
{
	int			count = 0;
	int			walk = entries[entry_idx].r_next;

	/*
	 * Count distinct entries above this entry in the recency list. Cap at
	 * 2*nbuffers to avoid excessive traversal.
	 */
	while (walk >= 0 && count < ctl->max_r_size)
	{
		count++;
		walk = entries[walk].r_next;
	}

	return count;
}


/* ----------------------------------------------------------------
 *			RCRD vtable callback implementations
 * ----------------------------------------------------------------
 */

/*
 * RcrdOnHit -- called when a page is found in the buffer cache.
 *
 * Compute CRD, update entry, and potentially promote or demote.
 */
static void
RcrdOnHit(void *strategy_data, int buf_id, BufferTag *tag)
{
	RcrdControl *ctl = (RcrdControl *) strategy_data;
	RcrdEntry  *entries = RCRD_ENTRIES(ctl);
	int		   *buf_to_entry = RCRD_BUF_TO_ENTRY(ctl);
	int			pool_local_id = buf_id - ctl->first_buf_id;
	int			entry_idx;
	int			new_crd;

	PoolStatIncrement(&rcrd_local_stats[RCRD_STAT_LOOKUPS], &ctl->stat_lookups);

	if (pool_local_id < 0 || pool_local_id >= ctl->nbuffers)
		return;

	entry_idx = buf_to_entry[pool_local_id];
	if (entry_idx < 0)
		return;

	SpinLockAcquire(&ctl->rcrd_lock);

	/* Compute CRD before moving to top */
	new_crd = rcrd_compute_crd(ctl, entries, entry_idx);
	entries[entry_idx].crd = new_crd;

	if (entries[entry_idx].status == RCRD_STATUS_HOT)
	{
		PoolStatIncrement(&rcrd_local_stats[RCRD_STAT_HOT_HITS], &ctl->stat_hot_hits);

		/* Move to recency list top */
		rcrd_r_move_to_top(ctl, entries, entry_idx);
		rcrd_r_prune(ctl, entries);
	}
	else if (entries[entry_idx].status == RCRD_STATUS_COLD)
	{
		PoolStatIncrement(&rcrd_local_stats[RCRD_STAT_COLD_HITS], &ctl->stat_cold_hits);

		if (new_crd <= ctl->hot_capacity)
		{
			/*
			 * COLD page with small CRD: promote to HOT. Remove from Q, move
			 * to recency list top, set HOT.
			 */
			rcrd_q_remove(ctl, entries, entry_idx);
			rcrd_r_move_to_top(ctl, entries, entry_idx);
			entries[entry_idx].status = RCRD_STATUS_HOT;
			ctl->cold_count--;
			ctl->hot_count++;

			PoolStatIncrement(&rcrd_local_stats[RCRD_STAT_PROMOTIONS], &ctl->stat_promotions);

			rcrd_r_prune(ctl, entries);

			if (ctl->hot_count > ctl->hot_capacity)
				rcrd_demote_bottom_hot(ctl, entries);

			rcrd_r_prune(ctl, entries);
		}
		else
		{
			/*
			 * COLD page with large CRD: stay COLD, move to Q tail.
			 */
			rcrd_q_remove(ctl, entries, entry_idx);
			rcrd_q_append(ctl, entries, entry_idx);
			rcrd_r_move_to_top(ctl, entries, entry_idx);
			rcrd_r_prune(ctl, entries);
		}
	}

	SpinLockRelease(&ctl->rcrd_lock);
}

/*
 * RcrdOnMiss -- called when a page is NOT found in the buffer cache.
 *
 * Checks ghost hash for non-resident entries.
 */
static void
RcrdOnMiss(void *strategy_data, BufferTag *tag)
{
	RcrdControl *ctl = (RcrdControl *) strategy_data;
	RcrdBackendState *state = rcrd_get_backend_state(ctl);
	RcrdEntry  *entries pg_attribute_unused() = RCRD_ENTRIES(ctl);
	int			ghost_idx;

	PoolStatIncrement(&rcrd_local_stats[RCRD_STAT_LOOKUPS], &ctl->stat_lookups);

	SpinLockAcquire(&ctl->rcrd_lock);

	ghost_idx = rcrd_ghost_lookup(ctl, tag);

	if (ghost_idx >= 0 && entries[ghost_idx].status == RCRD_STATUS_GHOST)
	{
		PoolStatIncrement(&rcrd_local_stats[RCRD_STAT_GHOST_HITS], &ctl->stat_ghost_hits);
		state->ghost_entry = ghost_idx;
	}
	else
	{
		PoolStatIncrement(&rcrd_local_stats[RCRD_STAT_MISSES], &ctl->stat_misses);
		state->ghost_entry = -1;
	}

	SpinLockRelease(&ctl->rcrd_lock);
}

/*
 * RcrdOnEvict -- called when a buffer's old content is being evicted.
 */
static void
RcrdOnEvict(void *strategy_data, int buf_id, BufferTag *old_tag pg_attribute_unused())
{
	RcrdControl *ctl = (RcrdControl *) strategy_data;
	RcrdEntry  *entries = RCRD_ENTRIES(ctl);
	int		   *buf_to_entry = RCRD_BUF_TO_ENTRY(ctl);
	int			pool_local_id = buf_id - ctl->first_buf_id;
	int			entry_idx;
	int			old_status;

	SpinLockAcquire(&ctl->rcrd_lock);

	if (pool_local_id < 0 || pool_local_id >= ctl->nbuffers)
	{
		SpinLockRelease(&ctl->rcrd_lock);
		return;
	}

	entry_idx = buf_to_entry[pool_local_id];
	if (entry_idx < 0)
	{
		SpinLockRelease(&ctl->rcrd_lock);
		return;
	}

	old_status = entries[entry_idx].status;

	if (old_status == RCRD_STATUS_COLD)
	{
		rcrd_q_remove(ctl, entries, entry_idx);
		ctl->cold_count--;

		/*
		 * If the entry is still in the recency list, convert to ghost so its
		 * CRD can inform future admission decisions.  If it was pruned from
		 * the recency list (r_prev == -1 && r_next == -1 && not the sole
		 * element), the entry has no useful recency information -- free it
		 * directly to avoid orphaned ghosts that can never be recycled.
		 */
		if (entries[entry_idx].r_prev >= 0 ||
			entries[entry_idx].r_next >= 0 ||
			ctl->r_bottom == entry_idx)
		{
			entries[entry_idx].status = RCRD_STATUS_GHOST;
			entries[entry_idx].buf_id = -1;
			rcrd_ghost_insert(ctl, entry_idx);
			ctl->ghost_count++;
		}
		else
		{
			rcrd_free_entry(ctl, entries, entry_idx);
		}
	}
	else if (old_status == RCRD_STATUS_HOT)
	{
		ctl->hot_count--;

		/* Convert to ghost */
		entries[entry_idx].status = RCRD_STATUS_GHOST;
		entries[entry_idx].buf_id = -1;
		rcrd_ghost_insert(ctl, entry_idx);
		ctl->ghost_count++;
	}
	else
	{
		SpinLockRelease(&ctl->rcrd_lock);
		return;
	}

	buf_to_entry[pool_local_id] = -1;
	PoolStatIncrement(&rcrd_local_stats[RCRD_STAT_EVICTIONS], &ctl->stat_evictions);

	SpinLockRelease(&ctl->rcrd_lock);
}

/*
 * RcrdOnNewTag -- called when a buffer is assigned a new page.
 *
 * Ghost hit: use stored CRD to decide HOT/COLD placement, raise threshold.
 * Complete miss: insert as HOT (warm-up) or COLD.
 */
static void
RcrdOnNewTag(void *strategy_data, int buf_id, BufferTag *new_tag,
			 bool vacuum_hint)
{
	RcrdControl *ctl = (RcrdControl *) strategy_data;
	RcrdBackendState *state = rcrd_get_backend_state(ctl);
	RcrdEntry  *entries = RCRD_ENTRIES(ctl);
	int		   *buf_to_entry = RCRD_BUF_TO_ENTRY(ctl);
	int			pool_local_id = buf_id - ctl->first_buf_id;
	int			entry_idx;

	Assert(pool_local_id >= 0 && pool_local_id < ctl->nbuffers);

	SpinLockAcquire(&ctl->rcrd_lock);

	Assert(buf_to_entry[pool_local_id] == -1);

	if (state->ghost_entry >= 0)
	{
		/* Ghost hit: re-admit with knowledge of previous CRD */
		entry_idx = state->ghost_entry;
		state->ghost_entry = -1;

		if (entry_idx >= ctl->nentries ||
			entries[entry_idx].status != RCRD_STATUS_GHOST)
			goto complete_miss;

		/* Remove from ghost hash */
		rcrd_ghost_remove(ctl, entry_idx);
		ctl->ghost_count--;

		/*
		 * Ghost hit indicates the page was evicted too early. Promote to HOT
		 * and raise the hot_capacity threshold slightly, capped at 99% of
		 * nbuffers.
		 */
		entries[entry_idx].buf_id = pool_local_id;
		entries[entry_idx].status = RCRD_STATUS_HOT;

		rcrd_r_move_to_top(ctl, entries, entry_idx);

		ctl->hot_count++;
		PoolStatIncrement(&rcrd_local_stats[RCRD_STAT_PROMOTIONS], &ctl->stat_promotions);

		/* Adaptively raise hot_capacity */
		{
			int			max_hot = Max(ctl->nbuffers * 99 / 100, 1);

			if (ctl->hot_capacity < max_hot)
			{
				ctl->hot_capacity++;
				PoolStatIncrement(&rcrd_local_stats[RCRD_STAT_THRESHOLD_RAISES],
								  &ctl->stat_threshold_raises);
			}
		}

		rcrd_r_prune(ctl, entries);

		if (ctl->hot_count > ctl->hot_capacity)
			rcrd_demote_bottom_hot(ctl, entries);

		rcrd_r_prune(ctl, entries);
	}
	else
	{
complete_miss:
		/* Complete miss: allocate new entry */
		entry_idx = rcrd_alloc_entry(ctl, entries);

		entries[entry_idx].buf_tag = *new_tag;
		entries[entry_idx].buf_id = pool_local_id;
		entries[entry_idx].ghost_next = -1;
		entries[entry_idx].crd = ctl->nbuffers; /* max CRD for new pages */

		if (ctl->hot_count < ctl->hot_capacity &&
			ctl->hot_count + ctl->cold_count < ctl->nbuffers)
		{
			/* Cache warm-up: insert as HOT directly */
			entries[entry_idx].status = RCRD_STATUS_HOT;
			ctl->hot_count++;
			rcrd_r_push(ctl, entries, entry_idx);
		}
		else
		{
			/* Cache full: insert as COLD */
			entries[entry_idx].status = RCRD_STATUS_COLD;
			ctl->cold_count++;

			if (vacuum_hint || state->vacuum_hint)
			{
				/* VACUUM: insert at Q head for fast eviction */
				rcrd_q_prepend(ctl, entries, entry_idx);
			}
			else
			{
				rcrd_q_append(ctl, entries, entry_idx);
				rcrd_r_push(ctl, entries, entry_idx);
			}
		}

		rcrd_r_prune(ctl, entries);
	}

	buf_to_entry[pool_local_id] = entry_idx;

	SpinLockRelease(&ctl->rcrd_lock);
}


/* ----------------------------------------------------------------
 *			RCRD get_victim -- Q-head eviction
 * ----------------------------------------------------------------
 */

static BufferDesc *
RcrdGetVictim(void *strategy_data, uint64 *buf_state)
{
	RcrdControl *ctl = (RcrdControl *) strategy_data;
	RcrdBackendState *state = rcrd_get_backend_state(ctl);
	RcrdEntry  *entries = RCRD_ENTRIES(ctl);
	int		   *buf_to_entry = RCRD_BUF_TO_ENTRY(ctl);
	BufferDesc *buf;
	int			retry;

	state->ghost_entry = -1;

	for (retry = 0; retry < 50; retry++)
	{
		/* Phase 1: Try free (untracked) buffers first */
		if (ctl->hot_count + ctl->cold_count < ctl->nbuffers)
		{
			for (int i = 0; i < ctl->nbuffers; i++)
			{
				uint64		old_buf_state;
				uint64		local_buf_state;

				if (buf_to_entry[i] >= 0)
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

		/* Phase 2: Evict from Q head (resident COLD pages) */
		SpinLockAcquire(&ctl->rcrd_lock);

		{
			int			attempts = 0;
			int			max_attempts = ctl->nbuffers * 3;
			int			idx = ctl->q_head;

			while (idx >= 0 && idx < ctl->nentries && attempts < max_attempts)
			{
				RcrdEntry  *e = &entries[idx];

				attempts++;

				if (e->buf_id < 0 || e->status != RCRD_STATUS_COLD)
				{
					idx = e->q_next;
					continue;
				}

				Assert(e->buf_id >= 0 && e->buf_id < ctl->nbuffers);
				buf = GetBufferDescriptor(e->buf_id + ctl->first_buf_id);

				{
					uint64		old_buf_state = pg_atomic_read_u64(&buf->state);

					for (;;)
					{
						uint64		local_buf_state = old_buf_state;

						if (BUF_STATE_GET_REFCOUNT(local_buf_state) != 0)
							break;

						if (unlikely(local_buf_state & BM_LOCKED))
						{
							SpinLockRelease(&ctl->rcrd_lock);
							old_buf_state = WaitBufHdrUnlocked(buf);
							SpinLockAcquire(&ctl->rcrd_lock);

							if (e->status != RCRD_STATUS_COLD ||
								e->buf_id < 0)
								break;
							continue;
						}

						local_buf_state += BUF_REFCOUNT_ONE;
						if (pg_atomic_compare_exchange_u64(&buf->state,
														   &old_buf_state,
														   local_buf_state))
						{
							*buf_state = local_buf_state;
							SpinLockRelease(&ctl->rcrd_lock);
							TrackNewBufferPin(BufferDescriptorGetBuffer(buf));
							return buf;
						}
					}
				}

				idx = e->q_next;
			}
		}

		SpinLockRelease(&ctl->rcrd_lock);

		/* Phase 3: Fallback -- scan all buffers */
		for (int i = 0; i < ctl->nbuffers; i++)
		{
			uint64		old_buf_state;
			uint64		local_buf_state;

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

		pg_usleep(1000);		/* 1ms */
	}

	ereport(ERROR,
			(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
			 errmsg("no unpinned buffers available in RCRD pool")));
	pg_unreachable();
}


/* ----------------------------------------------------------------
 *			RCRD sync/trickle/hint support
 * ----------------------------------------------------------------
 */

static int
RcrdSyncStart(void *strategy_data, uint32 *complete_passes,
			  uint32 *num_buf_alloc)
{
	RcrdControl *ctl = (RcrdControl *) strategy_data;
	int			result = 0;

	if (complete_passes)
		*complete_passes = 0;
	if (num_buf_alloc)
		*num_buf_alloc = 0;

	SpinLockAcquire(&ctl->rcrd_lock);
	if (ctl->q_head >= 0)
	{
		RcrdEntry  *entries = RCRD_ENTRIES(ctl);

		if (entries[ctl->q_head].buf_id >= 0)
			result = entries[ctl->q_head].buf_id + ctl->first_buf_id;
	}
	SpinLockRelease(&ctl->rcrd_lock);

	return result;
}

static void
RcrdNotifyTrickle(void *strategy_data, int bgwprocno)
{
	RcrdControl *ctl = (RcrdControl *) strategy_data;

	SpinLockAcquire(&ctl->rcrd_lock);
	ctl->bgwprocno = bgwprocno;
	SpinLockRelease(&ctl->rcrd_lock);
}

static void
RcrdHintVacuum(void *strategy_data, bool vacuum_active)
{
	RcrdControl *ctl = (RcrdControl *) strategy_data;
	RcrdBackendState *state = rcrd_get_backend_state(ctl);

	state->vacuum_hint = vacuum_active;
}

static void
RcrdPrefetchHint(void *strategy_data, BufferTag *tags, int ntags)
{
	RcrdControl *ctl = (RcrdControl *) strategy_data;
	RcrdEntry  *entries;

	if (ntags <= 0)
		return;

	SpinLockAcquire(&ctl->rcrd_lock);
	entries = RCRD_ENTRIES(ctl);

	for (int i = 0; i < ntags; i++)
	{
		int			ghost_idx = rcrd_ghost_lookup(ctl, &tags[i]);

		if (ghost_idx >= 0 && entries[ghost_idx].status == RCRD_STATUS_GHOST)
		{
			/* Move ghost to recency list top to prevent premature pruning */
			rcrd_r_move_to_top(ctl, entries, ghost_idx);
		}
	}

	SpinLockRelease(&ctl->rcrd_lock);
}


/* ----------------------------------------------------------------
 *			RCRD lifecycle
 * ----------------------------------------------------------------
 */

static Size
RcrdShmemSize(int nbuffers)
{
	int			nentries = nbuffers * 3;
	int			ghost_hash_size = pg_nextpower2_32(Max(nentries, 64));
	Size		size;

	size = sizeof(RcrdControl);
	size = MAXALIGN(size);
	size += sizeof(RcrdEntry) * nentries;
	size = MAXALIGN(size);
	size += sizeof(int) * ghost_hash_size;
	size = MAXALIGN(size);
	size += sizeof(int) * nbuffers;
	size = MAXALIGN(size);

	return size;
}

static void
RcrdShmemInit(void *strategy_data, int nbuffers, int first_buf_id, bool init)
{
	RcrdControl *ctl = (RcrdControl *) strategy_data;
	RcrdEntry  *entries;
	int		   *ghost_hash;
	int		   *buf_to_entry;
	int			nentries;
	int			ghost_hash_size;

	if (!init)
	{
		Assert(ctl->nbuffers == nbuffers);
		return;
	}

	nentries = nbuffers * 3;
	ghost_hash_size = pg_nextpower2_32(Max(nentries, 64));

	SpinLockInit(&ctl->rcrd_lock);
	ctl->nbuffers = nbuffers;
	ctl->first_buf_id = first_buf_id;
	ctl->nentries = nentries;

	/*
	 * HOT capacity determines how many buffers can be "hot". Scale the ratio
	 * with pool size (same as LIRS): < 128 buffers:  75% HOT 128-511: 85% HOT
	 * 512-2047:       90% HOT 2048-8191:      95% HOT >= 8192: 99% HOT
	 */
	if (nbuffers < 128)
		ctl->hot_capacity = Max(nbuffers * 75 / 100, 1);
	else if (nbuffers < 512)
		ctl->hot_capacity = Max(nbuffers * 85 / 100, 1);
	else if (nbuffers < 2048)
		ctl->hot_capacity = Max(nbuffers * 90 / 100, 1);
	else if (nbuffers < 8192)
		ctl->hot_capacity = Max(nbuffers * 95 / 100, 1);
	else
		ctl->hot_capacity = Max(nbuffers * 99 / 100, 1);
	ctl->max_r_size = nbuffers * 2;
	ctl->ghost_hash_size = ghost_hash_size;

	/* Empty recency list */
	ctl->r_top = -1;
	ctl->r_bottom = -1;
	ctl->r_size = 0;

	/* Empty Q list */
	ctl->q_head = -1;
	ctl->q_tail = -1;
	ctl->q_size = 0;

	/* Counts */
	ctl->hot_count = 0;
	ctl->cold_count = 0;
	ctl->ghost_count = 0;
	ctl->distinct_counter = 0;

	ctl->bgwprocno = -1;

	/* Initialize statistics */
	pg_atomic_init_u64(&ctl->stat_lookups, 0);
	pg_atomic_init_u64(&ctl->stat_hot_hits, 0);
	pg_atomic_init_u64(&ctl->stat_cold_hits, 0);
	pg_atomic_init_u64(&ctl->stat_ghost_hits, 0);
	pg_atomic_init_u64(&ctl->stat_misses, 0);
	pg_atomic_init_u64(&ctl->stat_demotions, 0);
	pg_atomic_init_u64(&ctl->stat_promotions, 0);
	pg_atomic_init_u64(&ctl->stat_evictions, 0);
	pg_atomic_init_u64(&ctl->stat_threshold_raises, 0);

	/* Initialize entry array: all on free list */
	entries = RCRD_ENTRIES(ctl);
	for (int i = 0; i < nentries; i++)
	{
		ClearBufferTag(&entries[i].buf_tag);
		entries[i].buf_id = -1;
		entries[i].status = RCRD_STATUS_UNUSED;
		entries[i].crd = 0;
		entries[i].ghost_next = -1;
		entries[i].r_prev = -1;
		entries[i].r_next = (i < nentries - 1) ? i + 1 : -1;
		entries[i].q_prev = -1;
		entries[i].q_next = -1;
	}
	ctl->free_list = 0;

	/* Initialize ghost hash table */
	ghost_hash = RCRD_GHOST_HASH(ctl);
	for (int i = 0; i < ghost_hash_size; i++)
		ghost_hash[i] = -1;

	/* Initialize buffer-to-entry mapping */
	buf_to_entry = RCRD_BUF_TO_ENTRY(ctl);
	for (int i = 0; i < nbuffers; i++)
		buf_to_entry[i] = -1;
}

static void
RcrdShutdown(void *strategy_data pg_attribute_unused())
{
	/* Nothing to clean up */
}


/* ----------------------------------------------------------------
 *			RCRD trickle writer iterator
 * ----------------------------------------------------------------
 */

typedef struct RcrdTrickleIter
{
	RcrdControl *ctl;
	int			entry_idx;
	int			yielded;
	int			max_candidates;
} RcrdTrickleIter;

static void *
RcrdTrickleIterBegin(void *strategy_data, int max_candidates)
{
	RcrdControl *ctl = (RcrdControl *) strategy_data;
	RcrdTrickleIter *iter;

	iter = (RcrdTrickleIter *) palloc(sizeof(RcrdTrickleIter));
	iter->ctl = ctl;
	iter->yielded = 0;
	iter->max_candidates = max_candidates;

	SpinLockAcquire(&ctl->rcrd_lock);
	iter->entry_idx = ctl->q_head;
	SpinLockRelease(&ctl->rcrd_lock);

	return iter;
}

static int
RcrdTrickleIterNext(void *strategy_data, void *iter_state)
{
	RcrdTrickleIter *iter = (RcrdTrickleIter *) iter_state;
	RcrdControl *ctl = iter->ctl;
	RcrdEntry  *entries = RCRD_ENTRIES(ctl);

	if (iter->yielded >= iter->max_candidates)
		return -1;

	while (iter->entry_idx >= 0 && iter->entry_idx < ctl->nentries)
	{
		RcrdEntry  *e = &entries[iter->entry_idx];
		int			next_idx = e->q_next;

		iter->entry_idx = next_idx;

		if (e->status == RCRD_STATUS_COLD && e->buf_id >= 0)
		{
			int			global_id = ctl->first_buf_id + e->buf_id;
			BufferDesc *buf = GetBufferDescriptor(global_id);
			uint64		bstate = pg_atomic_read_u64(&buf->state);

			if ((bstate & BM_DIRTY) &&
				(bstate & BM_VALID) &&
				BUF_STATE_GET_REFCOUNT(bstate) == 0)
			{
				iter->yielded++;
				return global_id;
			}
		}
	}

	return -1;
}

static void
RcrdTrickleIterEnd(void *strategy_data, void *iter_state)
{
	pfree(iter_state);
}


/* ----------------------------------------------------------------
 *			RCRD vtable and handler
 * ----------------------------------------------------------------
 */

static const BufferPoolRoutine rcrd_pool_routine = {
	.type = T_Invalid,
	.on_hit = RcrdOnHit,
	.on_miss = RcrdOnMiss,
	.on_evict = RcrdOnEvict,
	.on_new_tag = RcrdOnNewTag,
	.get_victim = RcrdGetVictim,
	.sync_start = RcrdSyncStart,
	.notify_trickle = RcrdNotifyTrickle,
	.trickle_iter_begin = RcrdTrickleIterBegin,
	.trickle_iter_next = RcrdTrickleIterNext,
	.trickle_iter_end = RcrdTrickleIterEnd,
	.hint_vacuum = RcrdHintVacuum,
	.prefetch_hint = RcrdPrefetchHint,
	.shmem_size = RcrdShmemSize,
	.shmem_init = RcrdShmemInit,
	.shutdown = RcrdShutdown,
	.scan_resistant = true,		/* LIRS2/RCRD inherits LIRS's scan resistance */
};

PG_FUNCTION_INFO_V1(rcrd_pool_handler);

Datum
rcrd_pool_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&rcrd_pool_routine);
}


/* ----------------------------------------------------------------
 *			RCRD statistics SRF
 * ----------------------------------------------------------------
 */

#define PG_STAT_GET_RCRD_STATS_COLS 17

PG_FUNCTION_INFO_V1(pg_stat_get_rcrd_stats);

Datum
pg_stat_get_rcrd_stats(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	InitMaterializedSRF(fcinfo, 0);

	for (int i = 0; i < NBufferPools; i++)
	{
		Datum		values[PG_STAT_GET_RCRD_STATS_COLS] = {0};
		bool		nulls[PG_STAT_GET_RCRD_STATS_COLS] = {0};
		BufferPoolDesc *pool = &BufferPoolDescs[i];
		RcrdControl *ctl;

		if (!pool->bp_active)
			continue;
		if (pool->bp_routine != &rcrd_pool_routine)
			continue;

		{
			PoolLocalState *local = EnsurePoolAttached(pool);

			ctl = (RcrdControl *) local->strategy_data;
		}
		if (ctl == NULL)
			continue;

		values[0] = NameGetDatum(&pool->bp_name);

		if (OidIsValid(pool->bp_oid))
			values[1] = ObjectIdGetDatum(pool->bp_oid);
		else
			nulls[1] = true;

		SpinLockAcquire(&ctl->rcrd_lock);
		values[2] = Int32GetDatum(ctl->hot_count);
		values[3] = Int32GetDatum(ctl->cold_count);
		values[4] = Int32GetDatum(ctl->ghost_count);
		values[5] = Int32GetDatum(ctl->hot_capacity);
		values[6] = Int32GetDatum(ctl->r_size);
		values[7] = Int32GetDatum(ctl->q_size);
		SpinLockRelease(&ctl->rcrd_lock);

		/* Flush this backend's pending local stats before reading */
		PoolStatFlush(&rcrd_local_stats[RCRD_STAT_LOOKUPS], &ctl->stat_lookups);
		PoolStatFlush(&rcrd_local_stats[RCRD_STAT_HOT_HITS], &ctl->stat_hot_hits);
		PoolStatFlush(&rcrd_local_stats[RCRD_STAT_COLD_HITS], &ctl->stat_cold_hits);
		PoolStatFlush(&rcrd_local_stats[RCRD_STAT_GHOST_HITS], &ctl->stat_ghost_hits);
		PoolStatFlush(&rcrd_local_stats[RCRD_STAT_MISSES], &ctl->stat_misses);
		PoolStatFlush(&rcrd_local_stats[RCRD_STAT_DEMOTIONS], &ctl->stat_demotions);
		PoolStatFlush(&rcrd_local_stats[RCRD_STAT_PROMOTIONS], &ctl->stat_promotions);
		PoolStatFlush(&rcrd_local_stats[RCRD_STAT_EVICTIONS], &ctl->stat_evictions);
		PoolStatFlush(&rcrd_local_stats[RCRD_STAT_THRESHOLD_RAISES], &ctl->stat_threshold_raises);

		values[8] = Int64GetDatum(pg_atomic_read_u64(&ctl->stat_lookups));
		values[9] = Int64GetDatum(pg_atomic_read_u64(&ctl->stat_hot_hits));
		values[10] = Int64GetDatum(pg_atomic_read_u64(&ctl->stat_cold_hits));
		values[11] = Int64GetDatum(pg_atomic_read_u64(&ctl->stat_ghost_hits));
		values[12] = Int64GetDatum(pg_atomic_read_u64(&ctl->stat_misses));
		values[13] = Int64GetDatum(pg_atomic_read_u64(&ctl->stat_demotions));
		values[14] = Int64GetDatum(pg_atomic_read_u64(&ctl->stat_promotions));
		values[15] = Int64GetDatum(pg_atomic_read_u64(&ctl->stat_evictions));
		values[16] = Int64GetDatum(pg_atomic_read_u64(&ctl->stat_threshold_raises));

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
							 values, nulls);
	}

	return (Datum) 0;
}


/* ----------------------------------------------------------------
 *			RCRD size recommendation advisory
 * ----------------------------------------------------------------
 */

PG_FUNCTION_INFO_V1(pg_bp_rcrd_size_recommendation);

Datum
pg_bp_rcrd_size_recommendation(PG_FUNCTION_ARGS)
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
		RcrdControl *ctl;
		int64		ghost_hits,
					misses,
					lookups,
					hot_hits,
					cold_hits;
		float8		ghost_pressure,
					hit_ratio;
		int			current_size,
					recommended_size;

		if (!pool->bp_active)
			continue;
		if (pool->bp_routine != &rcrd_pool_routine)
			continue;
		if (namestrcmp(&pool->bp_name, NameStr(*pool_name)) != 0)
			continue;

		{
			PoolLocalState *local = EnsurePoolAttached(pool);

			ctl = (RcrdControl *) local->strategy_data;
		}
		if (ctl == NULL)
			continue;

		current_size = ctl->nbuffers;
		ghost_hits = pg_atomic_read_u64(&ctl->stat_ghost_hits);
		misses = pg_atomic_read_u64(&ctl->stat_misses);
		lookups = pg_atomic_read_u64(&ctl->stat_lookups);
		hot_hits = pg_atomic_read_u64(&ctl->stat_hot_hits);
		cold_hits = pg_atomic_read_u64(&ctl->stat_cold_hits);

		ghost_pressure = (float8) ghost_hits / (float8) Max(misses, 1);
		hit_ratio = (lookups > 0) ?
			(float8) (hot_hits + cold_hits) / (float8) lookups : 0.0;

		if (ghost_pressure > 0.1)
			recommended_size = (int) Min((double) current_size * (1.0 + ghost_pressure),
										 (double) current_size * 2.0);
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
			 errmsg("RCRD pool \"%s\" not found", NameStr(*pool_name))));
	PG_RETURN_NULL();
}

/*
 * _PG_init -- register RCRD for use as the DEFAULT pool algorithm.
 */
void
_PG_init(void)
{
	RegisterDefaultPoolAlgorithm("rcrd", &rcrd_pool_routine);
}
