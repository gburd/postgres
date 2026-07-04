/*-------------------------------------------------------------------------
 *
 * pg_bp_lirs.c
 *	  LIRS-2 (Low Inter-reference Recency Set) buffer replacement algorithm.
 *
 * This extension provides the LIRS-2 algorithm as a BufferPoolRoutine handler,
 * usable with CREATE BUFFER POOL ... HANDLER lirs_pool_handler.
 *
 * LIRS tracks Inter-Reference Recency (IRR) -- the number of distinct pages
 * accessed between two consecutive accesses to the same page.  Pages with
 * low IRR are "hot" (LIR status), pages with high IRR are "cold" (HIR status).
 *
 * Data structures:
 *   S (stack) - recency-ordered doubly-linked list, contains all LIR entries
 *               plus some HIR and ghost (non-resident HIR) entries.
 *   Q         - doubly-linked list of resident HIR pages (eviction candidates).
 *   Ghost set - non-resident HIR entries tracked via ghost hash table for
 *               fast lookup on cache miss.
 *
 * The LIRS-2 improvement bounds the stack S to 2*cache_size entries,
 * preventing the unbounded memory growth of the original LIRS algorithm.
 *
 * Key advantages of LIRS over ARC/CAR:
 *   - Handles variable reuse distances (cyclic/looping access patterns)
 *   - Tracks actual inter-reference gap rather than frequency alone
 *   - Ghost-aware prefetch and Q-head-priority trickle writer
 *
 * References:
 *   X. Jiang et al., "LIRS2: An Improved LIRS Replacement Algorithm",
 *   HPL-2017-28, 2017.
 *   S. Jiang and X. Zhang, "LIRS: An Efficient Low Inter-reference Recency
 *   Set Replacement Policy to Improve Buffer Cache Performance",
 *   SIGMETRICS 2002.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  contrib/pg_bp_lirs/pg_bp_lirs.c
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

PG_MODULE_MAGIC_EXT(.name = "pg_bp_lirs",.version = PG_VERSION);

void		_PG_init(void);

/* LIRS page status values */
#define LIRS_STATUS_LIR		0	/* Low IRR -- hot page, kept in cache */
#define LIRS_STATUS_HIR		1	/* High IRR -- cold page, eviction candidate */
#define LIRS_STATUS_GHOST	2	/* Non-resident HIR (ghost entry) */
#define LIRS_STATUS_UNUSED	(-1)

/* Stat slot indices for PoolStatIncrement */
#define LIRS_STAT_LOOKUPS			0
#define LIRS_STAT_LIR_HITS			1
#define LIRS_STAT_HIR_HITS			2
#define LIRS_STAT_GHOST_HITS		3
#define LIRS_STAT_MISSES			4
#define LIRS_STAT_LIR_DEMOTIONS		5
#define LIRS_STAT_HIR_PROMOTIONS	6
#define LIRS_STAT_EVICTIONS			7
#define LIRS_STAT_STACK_PRUNES		8
#define LIRS_NUM_STATS				9

static uint64 lirs_local_stats[LIRS_NUM_STATS];

/*
 * LIRS Cache Directory Block (CDB).
 *
 * Each CDB tracks a page's position in the LIRS structures.
 * For LIR/HIR entries, buf_id is the pool-local buffer index.
 * For GHOST entries, buf_id is -1.
 */
typedef struct LirsCDB
{
	BufferTag	buf_tag;		/* page identity */
	int			buf_id;			/* pool-local buffer index (-1 for ghost) */
	int			status;			/* LIRS_STATUS_LIR/HIR/GHOST/UNUSED */
	bool		in_stack;		/* true if entry is in LIRS stack S */
	int			ghost_next;		/* next in ghost hash chain (-1 = end) */

	/* Stack S links (doubly-linked: bottom=LRU, top=MRU) */
	int			stack_prev;		/* toward LRU (bottom) */
	int			stack_next;		/* toward MRU (top) */

	/* Q list links (doubly-linked: head=eviction, tail=insertion) */
	int			q_prev;			/* toward head (eviction end) */
	int			q_next;			/* toward tail (insertion end) */
} LirsCDB;

/*
 * LIRS shared-memory control block.
 */
typedef struct LirsControl
{
	slock_t		lirs_lock;		/* spinlock protecting all mutable fields */

	int			nbuffers;		/* number of physical buffers in this pool */
	int			first_buf_id;	/* global buffer ID of first buffer in pool */
	int			ncdb;			/* total CDB entries (3 * nbuffers) */
	int			lir_capacity;	/* max LIR pages (adaptive threshold) */
	int			max_stack_size; /* 2 * nbuffers (LIRS-2 bound) */

	/* Stack S (recency-ordered) */
	int			stack_top;		/* MRU end (-1 if empty) */
	int			stack_bottom;	/* LRU end (-1 if empty) */
	int			stack_size;

	/* Q list (resident HIR pages) */
	int			q_head;			/* eviction end (-1 if empty) */
	int			q_tail;			/* insertion end (-1 if empty) */
	int			q_size;

	/* Current counts */
	int			lir_count;		/* resident LIR pages */
	int			hir_count;		/* resident HIR pages */
	int			ghost_count;	/* non-resident HIR ghost entries */

	/* Ghost hash table */
	int			ghost_hash_size;

	/* Free CDB list (singly-linked via stack_next) */
	int			free_cdb_list;

	/* Statistics (atomics -- no lock needed to increment) */
	pg_atomic_uint64 stat_lookups;
	pg_atomic_uint64 stat_lir_hits;
	pg_atomic_uint64 stat_hir_hits;
	pg_atomic_uint64 stat_ghost_hits;
	pg_atomic_uint64 stat_misses;
	pg_atomic_uint64 stat_lir_demotions;	/* LIR -> HIR */
	pg_atomic_uint64 stat_hir_promotions;	/* HIR/ghost -> LIR */
	pg_atomic_uint64 stat_evictions;
	pg_atomic_uint64 stat_stack_prunes; /* LIRS-2 pruning events */

	int			bgwprocno;		/* trickle writer procno (-1 = none) */

	/*
	 * Variable-length arrays follow: LirsCDB cdb[ncdb] int
	 * ghost_hash[ghost_hash_size] int buf_to_cdb[nbuffers]
	 */
} LirsControl;

/*
 * Per-backend LIRS state for cross-call communication.
 */
typedef struct LirsBackendState
{
	LirsControl *ctl;
	int			ghost_cdb;		/* CDB index of ghost hit (-1 = miss) */
	bool		vacuum_hint;
} LirsBackendState;

#define MAX_LIRS_POOLS	MAX_BUFFER_POOLS
static LirsBackendState lirs_backend_states[MAX_LIRS_POOLS];
static int	lirs_num_states = 0;

static LirsBackendState *
lirs_get_backend_state(LirsControl *ctl)
{
	for (int i = 0; i < lirs_num_states; i++)
	{
		if (lirs_backend_states[i].ctl == ctl)
			return &lirs_backend_states[i];
	}
	Assert(lirs_num_states < MAX_LIRS_POOLS);
	lirs_backend_states[lirs_num_states].ctl = ctl;
	lirs_backend_states[lirs_num_states].ghost_cdb = -1;
	lirs_backend_states[lirs_num_states].vacuum_hint = false;
	return &lirs_backend_states[lirs_num_states++];
}

/* Accessor macros for variable-length arrays */
#define LIRS_CDB(ctl)		((LirsCDB *) ((char *)(ctl) + sizeof(LirsControl)))
#define LIRS_GHOST_HASH(ctl) ((int *) ((char *)(ctl) + sizeof(LirsControl) + \
							  sizeof(LirsCDB) * (ctl)->ncdb))
#define LIRS_BUF_TO_CDB(ctl) ((int *) ((char *)(ctl) + sizeof(LirsControl) + \
							   sizeof(LirsCDB) * (ctl)->ncdb + \
							   sizeof(int) * (ctl)->ghost_hash_size))


/* ----------------------------------------------------------------
 *			Ghost hash table operations
 * ----------------------------------------------------------------
 */

static inline uint32
lirs_ghost_hash_bucket(LirsControl *ctl, const BufferTag *tag)
{
	uint32		h = tag_hash(tag, sizeof(BufferTag));

	return h & (ctl->ghost_hash_size - 1);
}

static int
lirs_ghost_lookup(LirsControl *ctl, const BufferTag *tag)
{
	int		   *ghost_hash = LIRS_GHOST_HASH(ctl);
	LirsCDB    *cdb = LIRS_CDB(ctl);
	uint32		bucket = lirs_ghost_hash_bucket(ctl, tag);
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
lirs_ghost_insert(LirsControl *ctl, int cdb_idx)
{
	int		   *ghost_hash = LIRS_GHOST_HASH(ctl);
	LirsCDB    *cdb = LIRS_CDB(ctl);
	uint32		bucket = lirs_ghost_hash_bucket(ctl, &cdb[cdb_idx].buf_tag);

	cdb[cdb_idx].ghost_next = ghost_hash[bucket];
	ghost_hash[bucket] = cdb_idx;
}

static void
lirs_ghost_remove(LirsControl *ctl, int cdb_idx)
{
	int		   *ghost_hash = LIRS_GHOST_HASH(ctl);
	LirsCDB    *cdb = LIRS_CDB(ctl);
	uint32		bucket = lirs_ghost_hash_bucket(ctl, &cdb[cdb_idx].buf_tag);
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
 *			CDB allocation and free list
 * ----------------------------------------------------------------
 */

static void
lirs_free_cdb(LirsControl *ctl, LirsCDB *cdb_arr, int cdb_idx)
{
	ClearBufferTag(&cdb_arr[cdb_idx].buf_tag);
	cdb_arr[cdb_idx].buf_id = -1;
	cdb_arr[cdb_idx].status = LIRS_STATUS_UNUSED;
	cdb_arr[cdb_idx].in_stack = false;
	cdb_arr[cdb_idx].ghost_next = -1;
	cdb_arr[cdb_idx].stack_prev = -1;
	cdb_arr[cdb_idx].q_prev = -1;
	cdb_arr[cdb_idx].q_next = -1;
	/* Use stack_next for free list chaining */
	cdb_arr[cdb_idx].stack_next = ctl->free_cdb_list;
	ctl->free_cdb_list = cdb_idx;
}


/* ----------------------------------------------------------------
 *			Stack S operations (recency-ordered doubly-linked list)
 *
 * stack_bottom = LRU end (prune/demote candidates)
 * stack_top    = MRU end (most recently accessed)
 * stack_prev   = toward LRU (bottom)
 * stack_next   = toward MRU (top)
 * ----------------------------------------------------------------
 */

/*
 * Push an entry onto the stack top (MRU position).
 * The entry must not already be in the stack.
 */
static void
lirs_stack_push(LirsControl *ctl, LirsCDB *cdb_arr, int cdb_idx)
{
	LirsCDB    *cdb = &cdb_arr[cdb_idx];

	Assert(!cdb->in_stack);

	cdb->in_stack = true;
	cdb->stack_next = -1;		/* nothing above top */
	cdb->stack_prev = ctl->stack_top;

	if (ctl->stack_top >= 0)
		cdb_arr[ctl->stack_top].stack_next = cdb_idx;
	ctl->stack_top = cdb_idx;

	if (ctl->stack_bottom < 0)
		ctl->stack_bottom = cdb_idx;

	ctl->stack_size++;
}

/*
 * Remove an entry from the stack.
 */
static void
lirs_stack_remove(LirsControl *ctl, LirsCDB *cdb_arr, int cdb_idx)
{
	LirsCDB    *cdb = &cdb_arr[cdb_idx];

	Assert(cdb->in_stack);

	if (cdb->stack_prev >= 0)
		cdb_arr[cdb->stack_prev].stack_next = cdb->stack_next;
	else
		ctl->stack_bottom = cdb->stack_next;	/* was bottom */

	if (cdb->stack_next >= 0)
		cdb_arr[cdb->stack_next].stack_prev = cdb->stack_prev;
	else
		ctl->stack_top = cdb->stack_prev;	/* was top */

	cdb->stack_prev = -1;
	cdb->stack_next = -1;
	cdb->in_stack = false;
	ctl->stack_size--;
}

/*
 * Move an entry to the stack top (remove if present, then push).
 */
static void
lirs_stack_move_to_top(LirsControl *ctl, LirsCDB *cdb_arr, int cdb_idx)
{
	if (cdb_arr[cdb_idx].in_stack)
		lirs_stack_remove(ctl, cdb_arr, cdb_idx);
	lirs_stack_push(ctl, cdb_arr, cdb_idx);
}

/*
 * LIRS-2 stack pruning.
 *
 * Step 1: Remove non-LIR entries from the stack bottom until the bottom
 * is an LIR entry (standard LIRS invariant).
 *
 * Step 2: If the stack exceeds the LIRS-2 bound (2*nbuffers), force-prune
 * non-LIR entries from the bottom.
 *
 * Ghost entries removed during pruning are freed entirely (removed from
 * ghost hash and returned to the free list).  Resident HIR entries removed
 * from the stack remain in Q.
 */
static void
lirs_stack_prune(LirsControl *ctl, LirsCDB *cdb_arr)
{
	/* Step 1: standard bottom prune -- ensure bottom is LIR */
	while (ctl->stack_bottom >= 0 &&
		   cdb_arr[ctl->stack_bottom].status != LIRS_STATUS_LIR)
	{
		int			idx = ctl->stack_bottom;

		lirs_stack_remove(ctl, cdb_arr, idx);

		if (cdb_arr[idx].status == LIRS_STATUS_GHOST)
		{
			lirs_ghost_remove(ctl, idx);
			ctl->ghost_count--;
			lirs_free_cdb(ctl, cdb_arr, idx);
		}
		/* Resident HIR: stays in Q, just no longer in stack */

		PoolStatIncrement(&lirs_local_stats[LIRS_STAT_STACK_PRUNES], &ctl->stat_stack_prunes);
	}

	/* Step 2: LIRS-2 hard bound enforcement */
	while (ctl->stack_size > ctl->max_stack_size && ctl->stack_bottom >= 0)
	{
		int			idx = ctl->stack_bottom;

		if (cdb_arr[idx].status == LIRS_STATUS_LIR)
			break;				/* never prune LIR entries */

		lirs_stack_remove(ctl, cdb_arr, idx);

		if (cdb_arr[idx].status == LIRS_STATUS_GHOST)
		{
			lirs_ghost_remove(ctl, idx);
			ctl->ghost_count--;
			lirs_free_cdb(ctl, cdb_arr, idx);
		}

		PoolStatIncrement(&lirs_local_stats[LIRS_STAT_STACK_PRUNES], &ctl->stat_stack_prunes);
	}
}


/* ----------------------------------------------------------------
 *			Q list operations (resident HIR doubly-linked list)
 *
 * q_head = eviction end (oldest HIR)
 * q_tail = insertion end (newest HIR)
 * ----------------------------------------------------------------
 */

/*
 * Append an entry to Q tail (insertion end).
 */
static void
lirs_q_append(LirsControl *ctl, LirsCDB *cdb_arr, int cdb_idx)
{
	LirsCDB    *cdb = &cdb_arr[cdb_idx];

	cdb->q_next = -1;
	cdb->q_prev = ctl->q_tail;

	if (ctl->q_tail >= 0)
		cdb_arr[ctl->q_tail].q_next = cdb_idx;
	ctl->q_tail = cdb_idx;

	if (ctl->q_head < 0)
		ctl->q_head = cdb_idx;

	ctl->q_size++;
}

/*
 * Prepend an entry to Q head (eviction end) -- used for VACUUM pages.
 */
static void
lirs_q_prepend(LirsControl *ctl, LirsCDB *cdb_arr, int cdb_idx)
{
	LirsCDB    *cdb = &cdb_arr[cdb_idx];

	cdb->q_prev = -1;
	cdb->q_next = ctl->q_head;

	if (ctl->q_head >= 0)
		cdb_arr[ctl->q_head].q_prev = cdb_idx;
	ctl->q_head = cdb_idx;

	if (ctl->q_tail < 0)
		ctl->q_tail = cdb_idx;

	ctl->q_size++;
}

/*
 * Remove an entry from Q.
 */
static void
lirs_q_remove(LirsControl *ctl, LirsCDB *cdb_arr, int cdb_idx)
{
	LirsCDB    *cdb = &cdb_arr[cdb_idx];

	if (cdb->q_prev >= 0)
		cdb_arr[cdb->q_prev].q_next = cdb->q_next;
	else
		ctl->q_head = cdb->q_next;

	if (cdb->q_next >= 0)
		cdb_arr[cdb->q_next].q_prev = cdb->q_prev;
	else
		ctl->q_tail = cdb->q_prev;

	cdb->q_prev = -1;
	cdb->q_next = -1;
	ctl->q_size--;
}


/* ----------------------------------------------------------------
 *			CDB allocation (cont.)
 * ----------------------------------------------------------------
 */

static int
lirs_alloc_cdb(LirsControl *ctl, LirsCDB *cdb_arr)
{
	int			idx;

	/* Try free list first */
	if (ctl->free_cdb_list >= 0)
	{
		idx = ctl->free_cdb_list;
		ctl->free_cdb_list = cdb_arr[idx].stack_next;
		cdb_arr[idx].stack_next = -1;
		cdb_arr[idx].stack_prev = -1;
		cdb_arr[idx].q_prev = -1;
		cdb_arr[idx].q_next = -1;
		cdb_arr[idx].ghost_next = -1;
		cdb_arr[idx].in_stack = false;
		cdb_arr[idx].status = LIRS_STATUS_UNUSED;
		return idx;
	}

	/* Recycle oldest ghost from stack bottom */
	{
		int			scan = ctl->stack_bottom;

		while (scan >= 0)
		{
			int			next_scan = cdb_arr[scan].stack_next;

			if (cdb_arr[scan].status == LIRS_STATUS_GHOST)
			{
				idx = scan;
				lirs_ghost_remove(ctl, idx);
				lirs_stack_remove(ctl, cdb_arr, idx);
				ctl->ghost_count--;

				ClearBufferTag(&cdb_arr[idx].buf_tag);
				cdb_arr[idx].buf_id = -1;
				cdb_arr[idx].status = LIRS_STATUS_UNUSED;
				cdb_arr[idx].in_stack = false;
				cdb_arr[idx].ghost_next = -1;
				cdb_arr[idx].stack_prev = -1;
				cdb_arr[idx].stack_next = -1;
				cdb_arr[idx].q_prev = -1;
				cdb_arr[idx].q_next = -1;
				return idx;
			}
			scan = next_scan;
		}
	}

	/*
	 * Release the spinlock before ereport to avoid leaving it permanently
	 * locked, which would cause a stuck-spinlock PANIC in other processes.
	 */
	SpinLockRelease(&ctl->lirs_lock);
	ereport(ERROR,
			(errcode(ERRCODE_OUT_OF_MEMORY),
			 errmsg("LIRS: no CDB entries available for recycling")));
	pg_unreachable();
}


/* ----------------------------------------------------------------
 *			LIRS demote bottom LIR to HIR
 *
 * Called when lir_count exceeds lir_capacity after a promotion.
 * Searches from the stack bottom for the first LIR entry, demotes
 * it to HIR, removes it from the stack, and appends to Q tail.
 * ----------------------------------------------------------------
 */

static void
lirs_demote_bottom_lir(LirsControl *ctl, LirsCDB *cdb_arr)
{
	int			idx = ctl->stack_bottom;

	/*
	 * Find the bottom-most LIR entry.  After a prune, the stack bottom should
	 * be LIR, but be defensive and skip non-LIR entries.
	 */
	while (idx >= 0 && cdb_arr[idx].status != LIRS_STATUS_LIR)
		idx = cdb_arr[idx].stack_next;

	if (idx < 0)
		return;					/* no LIR entry to demote */

	/* Demote to HIR */
	cdb_arr[idx].status = LIRS_STATUS_HIR;
	ctl->lir_count--;
	ctl->hir_count++;

	/* Remove from stack */
	lirs_stack_remove(ctl, cdb_arr, idx);

	/* Add to Q tail (resident HIR) */
	lirs_q_append(ctl, cdb_arr, idx);

	PoolStatIncrement(&lirs_local_stats[LIRS_STAT_LIR_DEMOTIONS], &ctl->stat_lir_demotions);
}


/* ----------------------------------------------------------------
 *			LIRS vtable callback implementations
 * ----------------------------------------------------------------
 */

/*
 * LirsOnHit -- called when a page is found in the buffer cache.
 *
 * LIR page: move to stack top, prune.
 * Resident HIR in stack: promote to LIR, demote bottom LIR.
 * Resident HIR not in stack: move to Q tail, push onto stack top.
 */
static void
LirsOnHit(void *strategy_data, int buf_id, BufferTag *tag)
{
	LirsControl *ctl = (LirsControl *) strategy_data;
	LirsCDB    *cdb_arr = LIRS_CDB(ctl);
	int		   *buf_to_cdb = LIRS_BUF_TO_CDB(ctl);
	int			pool_local_id = buf_id - ctl->first_buf_id;
	int			cdb_idx;

	PoolStatIncrement(&lirs_local_stats[LIRS_STAT_LOOKUPS], &ctl->stat_lookups);

	if (pool_local_id < 0 || pool_local_id >= ctl->nbuffers)
		return;

	cdb_idx = buf_to_cdb[pool_local_id];
	if (cdb_idx < 0)
		return;

	SpinLockAcquire(&ctl->lirs_lock);

	if (cdb_arr[cdb_idx].status == LIRS_STATUS_LIR)
	{
		PoolStatIncrement(&lirs_local_stats[LIRS_STAT_LIR_HITS], &ctl->stat_lir_hits);

		/* Move to stack top, prune bottom */
		lirs_stack_move_to_top(ctl, cdb_arr, cdb_idx);
		lirs_stack_prune(ctl, cdb_arr);
	}
	else if (cdb_arr[cdb_idx].status == LIRS_STATUS_HIR)
	{
		PoolStatIncrement(&lirs_local_stats[LIRS_STAT_HIR_HITS], &ctl->stat_hir_hits);

		if (cdb_arr[cdb_idx].in_stack)
		{
			/*
			 * HIR in stack: promote to LIR.
			 *
			 * Per LIRS algorithm: remove from Q, move to stack top, set LIR,
			 * then demote bottom-of-stack LIR to HIR.
			 */
			lirs_q_remove(ctl, cdb_arr, cdb_idx);
			lirs_stack_move_to_top(ctl, cdb_arr, cdb_idx);
			cdb_arr[cdb_idx].status = LIRS_STATUS_LIR;
			ctl->hir_count--;
			ctl->lir_count++;

			PoolStatIncrement(&lirs_local_stats[LIRS_STAT_HIR_PROMOTIONS], &ctl->stat_hir_promotions);

			/* Prune first to establish LIR at bottom */
			lirs_stack_prune(ctl, cdb_arr);

			/* Demote bottom LIR if over capacity */
			if (ctl->lir_count > ctl->lir_capacity)
				lirs_demote_bottom_lir(ctl, cdb_arr);

			lirs_stack_prune(ctl, cdb_arr);
		}
		else
		{
			/*
			 * HIR not in stack: move to Q tail, push onto stack top. Stays as
			 * HIR (no promotion without stack membership).
			 */
			lirs_q_remove(ctl, cdb_arr, cdb_idx);
			lirs_q_append(ctl, cdb_arr, cdb_idx);
			lirs_stack_push(ctl, cdb_arr, cdb_idx);
			lirs_stack_prune(ctl, cdb_arr);
		}
	}

	SpinLockRelease(&ctl->lirs_lock);
}

/*
 * LirsOnMiss -- called when a page is NOT found in the buffer cache.
 *
 * Checks ghost hash for non-resident HIR entries.  Ghost hits indicate
 * pages that were recently evicted and should be promoted to LIR.
 */
static void
LirsOnMiss(void *strategy_data, BufferTag *tag)
{
	LirsControl *ctl = (LirsControl *) strategy_data;
	LirsBackendState *state = lirs_get_backend_state(ctl);
	LirsCDB    *cdb_arr pg_attribute_unused() = LIRS_CDB(ctl);
	int			ghost_idx;

	PoolStatIncrement(&lirs_local_stats[LIRS_STAT_LOOKUPS], &ctl->stat_lookups);

	SpinLockAcquire(&ctl->lirs_lock);

	ghost_idx = lirs_ghost_lookup(ctl, tag);

	if (ghost_idx >= 0 && cdb_arr[ghost_idx].status == LIRS_STATUS_GHOST)
	{
		PoolStatIncrement(&lirs_local_stats[LIRS_STAT_GHOST_HITS], &ctl->stat_ghost_hits);
		state->ghost_cdb = ghost_idx;
	}
	else
	{
		PoolStatIncrement(&lirs_local_stats[LIRS_STAT_MISSES], &ctl->stat_misses);
		state->ghost_cdb = -1;
	}

	SpinLockRelease(&ctl->lirs_lock);
}

/*
 * LirsOnEvict -- called when a buffer's old content is being evicted.
 *
 * Converts resident entries to ghost entries (non-resident HIR).
 * HIR pages in the stack become ghost entries; those not in the stack
 * are freed entirely.  LIR eviction (unusual) converts to ghost.
 */
static void
LirsOnEvict(void *strategy_data, int buf_id, BufferTag *old_tag pg_attribute_unused())
{
	LirsControl *ctl = (LirsControl *) strategy_data;
	LirsCDB    *cdb_arr = LIRS_CDB(ctl);
	int		   *buf_to_cdb = LIRS_BUF_TO_CDB(ctl);
	int			pool_local_id = buf_id - ctl->first_buf_id;
	int			cdb_idx;
	int			old_status;

	SpinLockAcquire(&ctl->lirs_lock);

	if (pool_local_id < 0 || pool_local_id >= ctl->nbuffers)
	{
		SpinLockRelease(&ctl->lirs_lock);
		return;
	}

	cdb_idx = buf_to_cdb[pool_local_id];
	if (cdb_idx < 0)
	{
		SpinLockRelease(&ctl->lirs_lock);
		return;
	}

	old_status = cdb_arr[cdb_idx].status;

	if (old_status == LIRS_STATUS_HIR)
	{
		/* Remove from Q */
		lirs_q_remove(ctl, cdb_arr, cdb_idx);
		ctl->hir_count--;

		if (cdb_arr[cdb_idx].in_stack)
		{
			/* Stays in stack as ghost entry */
			cdb_arr[cdb_idx].status = LIRS_STATUS_GHOST;
			cdb_arr[cdb_idx].buf_id = -1;
			lirs_ghost_insert(ctl, cdb_idx);
			ctl->ghost_count++;
		}
		else
		{
			/* Not in stack: free entirely */
			lirs_free_cdb(ctl, cdb_arr, cdb_idx);
		}
	}
	else if (old_status == LIRS_STATUS_LIR)
	{
		/*
		 * LIR eviction (unusual -- normally only HIR evicted). Convert to
		 * ghost and keep in stack for future tracking.
		 */
		ctl->lir_count--;
		cdb_arr[cdb_idx].status = LIRS_STATUS_GHOST;
		cdb_arr[cdb_idx].buf_id = -1;
		lirs_ghost_insert(ctl, cdb_idx);
		ctl->ghost_count++;

		/* If not in stack (shouldn't happen for LIR), just leave as ghost */
	}
	else
	{
		/* Already ghost or unused -- nothing to do */
		SpinLockRelease(&ctl->lirs_lock);
		return;
	}

	buf_to_cdb[pool_local_id] = -1;
	PoolStatIncrement(&lirs_local_stats[LIRS_STAT_EVICTIONS], &ctl->stat_evictions);

	SpinLockRelease(&ctl->lirs_lock);
}

/*
 * LirsOnNewTag -- called when a buffer is assigned a new page.
 *
 * Ghost hit: promote to LIR, demote bottom LIR to HIR.
 * Complete miss: insert as LIR (during warm-up) or HIR.
 * VACUUM pages are inserted at Q head for fast eviction.
 */
static void
LirsOnNewTag(void *strategy_data, int buf_id, BufferTag *new_tag,
			 bool vacuum_hint)
{
	LirsControl *ctl = (LirsControl *) strategy_data;
	LirsBackendState *state = lirs_get_backend_state(ctl);
	LirsCDB    *cdb_arr = LIRS_CDB(ctl);
	int		   *buf_to_cdb = LIRS_BUF_TO_CDB(ctl);
	int			pool_local_id = buf_id - ctl->first_buf_id;
	int			cdb_idx;

	Assert(pool_local_id >= 0 && pool_local_id < ctl->nbuffers);

	SpinLockAcquire(&ctl->lirs_lock);

	Assert(buf_to_cdb[pool_local_id] == -1);

	if (state->ghost_cdb >= 0)
	{
		/* Ghost hit: promote to LIR */
		cdb_idx = state->ghost_cdb;
		state->ghost_cdb = -1;

		if (cdb_idx >= ctl->ncdb ||
			cdb_arr[cdb_idx].status != LIRS_STATUS_GHOST)
			goto complete_miss;

		/* Remove from ghost hash */
		lirs_ghost_remove(ctl, cdb_idx);
		ctl->ghost_count--;

		/* Set as LIR resident */
		cdb_arr[cdb_idx].buf_id = pool_local_id;
		cdb_arr[cdb_idx].status = LIRS_STATUS_LIR;

		/* Move to stack top (may already be in stack as ghost) */
		lirs_stack_move_to_top(ctl, cdb_arr, cdb_idx);

		ctl->lir_count++;
		PoolStatIncrement(&lirs_local_stats[LIRS_STAT_HIR_PROMOTIONS], &ctl->stat_hir_promotions);

		/* Prune to establish LIR at bottom, then demote if needed */
		lirs_stack_prune(ctl, cdb_arr);

		if (ctl->lir_count > ctl->lir_capacity)
			lirs_demote_bottom_lir(ctl, cdb_arr);

		lirs_stack_prune(ctl, cdb_arr);
	}
	else
	{
complete_miss:
		/* Complete miss: allocate new CDB */
		cdb_idx = lirs_alloc_cdb(ctl, cdb_arr);

		cdb_arr[cdb_idx].buf_tag = *new_tag;
		cdb_arr[cdb_idx].buf_id = pool_local_id;
		cdb_arr[cdb_idx].ghost_next = -1;

		if (ctl->lir_count < ctl->lir_capacity &&
			ctl->lir_count + ctl->hir_count < ctl->nbuffers)
		{
			/*
			 * Cache warm-up: insert as LIR directly. During initial fill, all
			 * pages become LIR until we reach lir_capacity.
			 */
			cdb_arr[cdb_idx].status = LIRS_STATUS_LIR;
			ctl->lir_count++;
			lirs_stack_push(ctl, cdb_arr, cdb_idx);
		}
		else
		{
			/* Cache full: insert as HIR */
			cdb_arr[cdb_idx].status = LIRS_STATUS_HIR;
			ctl->hir_count++;

			if (vacuum_hint || state->vacuum_hint)
			{
				/* VACUUM: insert at Q head for fast eviction */
				lirs_q_prepend(ctl, cdb_arr, cdb_idx);
				/* Don't push onto stack (transient VACUUM pages) */
			}
			else
			{
				/* Normal: add to Q tail and push onto stack */
				lirs_q_append(ctl, cdb_arr, cdb_idx);
				lirs_stack_push(ctl, cdb_arr, cdb_idx);
			}
		}

		lirs_stack_prune(ctl, cdb_arr);
	}

	buf_to_cdb[pool_local_id] = cdb_idx;

	SpinLockRelease(&ctl->lirs_lock);
}


/* ----------------------------------------------------------------
 *			LIRS get_victim -- Q-head eviction
 * ----------------------------------------------------------------
 */

/*
 * LirsGetVictim -- select a victim buffer for eviction.
 *
 * Phase 1: Scan for free (untracked) buffers -- no lock needed.
 * Phase 2: Evict from Q head (oldest resident HIR pages).
 * Phase 3: Fallback scan for any untracked buffer.
 */
static BufferDesc *
LirsGetVictim(void *strategy_data, BufferAccessStrategy strategy pg_attribute_unused(),
			  uint64 *buf_state, bool *from_ring)
{
	LirsControl *ctl = (LirsControl *) strategy_data;
	LirsBackendState *state = lirs_get_backend_state(ctl);
	LirsCDB    *cdb_arr = LIRS_CDB(ctl);
	int		   *buf_to_cdb = LIRS_BUF_TO_CDB(ctl);
	BufferDesc *buf;
	int			retry;

	*from_ring = false;
	state->ghost_cdb = -1;

	/*
	 * Retry loop: if all phases fail to find a victim (all buffers pinned),
	 * wait briefly and retry.  This can happen under heavy concurrent load on
	 * small pools where VACUUM, queries, and the trickle writer all compete
	 * for a limited number of buffers.
	 */
	for (retry = 0; retry < 50; retry++)
	{
		/*
		 * Phase 1: Try free (untracked) buffers first. During cache warm-up,
		 * many buffers have no CDB entry yet.
		 */
		if (ctl->lir_count + ctl->hir_count < ctl->nbuffers)
		{
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

		/*
		 * Phase 2: Evict from Q head (resident HIR pages). Walk Q from head
		 * looking for an unpinned buffer.
		 *
		 * Note: we must re-read q_next from the CDB at the end of each
		 * iteration rather than caching it up front, because the spinlock may
		 * be released and reacquired while waiting for a locked buffer
		 * header, and Q list pointers can change in that window.
		 */
		SpinLockAcquire(&ctl->lirs_lock);

		{
			int			attempts = 0;
			int			max_attempts = ctl->nbuffers * 3;
			int			idx = ctl->q_head;

			while (idx >= 0 && idx < ctl->ncdb && attempts < max_attempts)
			{
				LirsCDB    *cdb = &cdb_arr[idx];

				attempts++;

				if (cdb->buf_id < 0 || cdb->status != LIRS_STATUS_HIR)
				{
					idx = cdb->q_next;
					continue;
				}

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
							SpinLockRelease(&ctl->lirs_lock);
							old_buf_state = WaitBufHdrUnlocked(buf);
							SpinLockAcquire(&ctl->lirs_lock);

							/*
							 * Q list may have changed while the lock was
							 * released.  Re-validate this CDB entry: if it
							 * was evicted, freed, or changed status, abandon
							 * this candidate and advance to the next Q entry.
							 */
							if (cdb->status != LIRS_STATUS_HIR ||
								cdb->buf_id < 0)
								break;
							continue;
						}

						local_buf_state += BUF_REFCOUNT_ONE;
						if (pg_atomic_compare_exchange_u64(&buf->state,
														   &old_buf_state,
														   local_buf_state))
						{
							*buf_state = local_buf_state;
							SpinLockRelease(&ctl->lirs_lock);
							TrackNewBufferPin(BufferDescriptorGetBuffer(buf));
							return buf;
						}
					}
				}

				/*
				 * Re-read q_next under the lock.  A stale cached value could
				 * point to a freed or reallocated CDB if the lock was
				 * released during the BM_LOCKED wait above.
				 */
				idx = cdb->q_next;
			}
		}

		SpinLockRelease(&ctl->lirs_lock);

		/*
		 * Phase 3: Fallback -- scan ALL buffer descriptors for any unpinned
		 * buffer, including those with CDB entries (LIR pages that could be
		 * demoted).
		 */
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

		/*
		 * All buffers are pinned.  Wait briefly for a concurrent backend to
		 * release a pin, then retry from the top.
		 */
		pg_usleep(1000);		/* 1ms */
	}

	ereport(ERROR,
			(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
			 errmsg("no unpinned buffers available in LIRS pool")));
	pg_unreachable();
}


/* ----------------------------------------------------------------
 *			LIRS sync/trickle/hint support
 * ----------------------------------------------------------------
 */

/*
 * LirsSyncStart -- return a monotonic sync position for the bgwriter.
 *
 * BgBufferSync requires sync_start to report a position that advances
 * monotonically with a matching complete_passes count (Assert(strategy_delta
 * >= 0), bufmgr.c).  LIRS's Q head wanders (buf_ids reorder as HIR pages are
 * promoted/evicted), so returning its buf_id violated that contract.  We
 * return a fixed 0/0 like the other list-based algorithms (lru/arc/osic);
 * real dirty-page writeback is driven by the trickle_iter callbacks, not this
 * position.
 */
static int
LirsSyncStart(void *strategy_data pg_attribute_unused(), uint32 *complete_passes,
			  uint32 *num_buf_alloc)
{
	if (complete_passes)
		*complete_passes = 0;
	if (num_buf_alloc)
		*num_buf_alloc = 0;

	return 0;
}

static void
LirsNotifyTrickle(void *strategy_data, int bgwprocno)
{
	LirsControl *ctl = (LirsControl *) strategy_data;

	SpinLockAcquire(&ctl->lirs_lock);
	ctl->bgwprocno = bgwprocno;
	SpinLockRelease(&ctl->lirs_lock);
}

static void
LirsHintVacuum(void *strategy_data, bool vacuum_active)
{
	LirsControl *ctl = (LirsControl *) strategy_data;
	LirsBackendState *state = lirs_get_backend_state(ctl);

	state->vacuum_hint = vacuum_active;
}

static bool
LirsRejectBuffer(void *strategy_data pg_attribute_unused(),
				 BufferAccessStrategy strategy pg_attribute_unused(),
				 BufferDesc *buf pg_attribute_unused(),
				 bool from_ring pg_attribute_unused())
{
	return false;
}

/*
 * LirsPrefetchHint -- ghost-aware prefetch integration.
 *
 * When the buffer manager signals upcoming page accesses, LIRS checks
 * the ghost hash.  Ghost entries found are moved to the stack top to
 * prevent premature recycling.  This pre-warms the IRR tracking so
 * that when the actual access arrives, the ghost is correctly positioned
 * for promotion to LIR.
 */
static void
LirsPrefetchHint(void *strategy_data, BufferTag *tags, int ntags)
{
	LirsControl *ctl = (LirsControl *) strategy_data;
	LirsCDB    *cdb_arr;

	if (ntags <= 0)
		return;

	SpinLockAcquire(&ctl->lirs_lock);
	cdb_arr = LIRS_CDB(ctl);

	for (int i = 0; i < ntags; i++)
	{
		int			ghost_idx = lirs_ghost_lookup(ctl, &tags[i]);

		if (ghost_idx >= 0 && cdb_arr[ghost_idx].status == LIRS_STATUS_GHOST)
		{
			/*
			 * Move ghost entry to stack top to prevent premature recycling
			 * during stack pruning.
			 */
			if (cdb_arr[ghost_idx].in_stack)
				lirs_stack_move_to_top(ctl, cdb_arr, ghost_idx);
		}
	}

	SpinLockRelease(&ctl->lirs_lock);
}


/* ----------------------------------------------------------------
 *			LIRS lifecycle
 * ----------------------------------------------------------------
 */

static Size
LirsShmemSize(int nbuffers)
{
	int			ncdb = nbuffers * 3;
	int			ghost_hash_size = pg_nextpower2_32(Max(ncdb, 64));
	Size		size;

	size = sizeof(LirsControl);
	size = MAXALIGN(size);
	size += sizeof(LirsCDB) * ncdb;
	size = MAXALIGN(size);
	size += sizeof(int) * ghost_hash_size;
	size = MAXALIGN(size);
	size += sizeof(int) * nbuffers;
	size = MAXALIGN(size);

	return size;
}

static void
LirsShmemInit(void *strategy_data, int nbuffers, int first_buf_id, bool init)
{
	LirsControl *ctl = (LirsControl *) strategy_data;
	LirsCDB    *cdb_arr;
	int		   *ghost_hash;
	int		   *buf_to_cdb;
	int			ncdb;
	int			ghost_hash_size;

	if (!init)
	{
		Assert(ctl->nbuffers == nbuffers);
		return;
	}

	ncdb = nbuffers * 3;
	ghost_hash_size = pg_nextpower2_32(Max(ncdb, 64));

	SpinLockInit(&ctl->lirs_lock);
	ctl->nbuffers = nbuffers;
	ctl->first_buf_id = first_buf_id;
	ctl->ncdb = ncdb;

	/*
	 * LIR capacity determines how many buffers can be "hot" (Low IRR). The
	 * remaining slots are HIR (eviction candidates in Q).
	 *
	 * For small pools we need proportionally more HIR slots to avoid running
	 * out of eviction candidates under concurrent load (e.g., autovacuum +
	 * benchmark queries).  Scale the ratio with pool size:
	 *
	 * < 128 buffers:   75% LIR  (32 HIR slots per 128 buffers) 128-511: 85%
	 * LIR 512-2047:        90% LIR 2048-8191:       95% LIR >= 8192: 99% LIR
	 */
	if (nbuffers < 128)
		ctl->lir_capacity = Max(nbuffers * 75 / 100, 1);
	else if (nbuffers < 512)
		ctl->lir_capacity = Max(nbuffers * 85 / 100, 1);
	else if (nbuffers < 2048)
		ctl->lir_capacity = Max(nbuffers * 90 / 100, 1);
	else if (nbuffers < 8192)
		ctl->lir_capacity = Max(nbuffers * 95 / 100, 1);
	else
		ctl->lir_capacity = Max(nbuffers * 99 / 100, 1);
	ctl->max_stack_size = nbuffers * 2;
	ctl->ghost_hash_size = ghost_hash_size;

	/* Empty stack */
	ctl->stack_top = -1;
	ctl->stack_bottom = -1;
	ctl->stack_size = 0;

	/* Empty Q list */
	ctl->q_head = -1;
	ctl->q_tail = -1;
	ctl->q_size = 0;

	/* Counts */
	ctl->lir_count = 0;
	ctl->hir_count = 0;
	ctl->ghost_count = 0;

	ctl->bgwprocno = -1;

	/* Initialize statistics */
	pg_atomic_init_u64(&ctl->stat_lookups, 0);
	pg_atomic_init_u64(&ctl->stat_lir_hits, 0);
	pg_atomic_init_u64(&ctl->stat_hir_hits, 0);
	pg_atomic_init_u64(&ctl->stat_ghost_hits, 0);
	pg_atomic_init_u64(&ctl->stat_misses, 0);
	pg_atomic_init_u64(&ctl->stat_lir_demotions, 0);
	pg_atomic_init_u64(&ctl->stat_hir_promotions, 0);
	pg_atomic_init_u64(&ctl->stat_evictions, 0);
	pg_atomic_init_u64(&ctl->stat_stack_prunes, 0);

	/* Initialize CDB array: all on free list */
	cdb_arr = LIRS_CDB(ctl);
	for (int i = 0; i < ncdb; i++)
	{
		ClearBufferTag(&cdb_arr[i].buf_tag);
		cdb_arr[i].buf_id = -1;
		cdb_arr[i].status = LIRS_STATUS_UNUSED;
		cdb_arr[i].in_stack = false;
		cdb_arr[i].ghost_next = -1;
		cdb_arr[i].stack_prev = -1;
		cdb_arr[i].stack_next = (i < ncdb - 1) ? i + 1 : -1;
		cdb_arr[i].q_prev = -1;
		cdb_arr[i].q_next = -1;
	}
	ctl->free_cdb_list = 0;

	/* Initialize ghost hash table */
	ghost_hash = LIRS_GHOST_HASH(ctl);
	for (int i = 0; i < ghost_hash_size; i++)
		ghost_hash[i] = -1;

	/* Initialize buffer-to-CDB mapping */
	buf_to_cdb = LIRS_BUF_TO_CDB(ctl);
	for (int i = 0; i < nbuffers; i++)
		buf_to_cdb[i] = -1;
}

static void
LirsShutdown(void *strategy_data pg_attribute_unused())
{
	/* Nothing to clean up */
}


/* ----------------------------------------------------------------
 *			LIRS trickle writer iterator
 *
 * Walk Q from head (HIR eviction candidates).
 * Yield dirty+unpinned HIR buffers as proactive flush candidates.
 * ----------------------------------------------------------------
 */

typedef struct LirsTrickleIter
{
	LirsControl *ctl;
	int			cdb_idx;		/* current CDB index in Q list */
	int			yielded;
	int			max_candidates;
} LirsTrickleIter;

static void *
LirsTrickleIterBegin(void *strategy_data, int max_candidates)
{
	LirsControl *ctl = (LirsControl *) strategy_data;
	LirsTrickleIter *iter;

	iter = (LirsTrickleIter *) palloc(sizeof(LirsTrickleIter));
	iter->ctl = ctl;
	iter->yielded = 0;
	iter->max_candidates = max_candidates;

	/* Snapshot the Q head position under the lock */
	SpinLockAcquire(&ctl->lirs_lock);
	iter->cdb_idx = ctl->q_head;
	SpinLockRelease(&ctl->lirs_lock);

	return iter;
}

static int
LirsTrickleIterNext(void *strategy_data, void *iter_state)
{
	LirsTrickleIter *iter = (LirsTrickleIter *) iter_state;
	LirsControl *ctl = iter->ctl;
	LirsCDB    *cdb_arr = LIRS_CDB(ctl);

	if (iter->yielded >= iter->max_candidates)
		return -1;

	while (iter->cdb_idx >= 0 && iter->cdb_idx < ctl->ncdb)
	{
		LirsCDB    *cdb = &cdb_arr[iter->cdb_idx];
		int			next_idx = cdb->q_next;

		/* Advance for next call */
		iter->cdb_idx = next_idx;

		/* Only consider resident HIR entries */
		if (cdb->status == LIRS_STATUS_HIR && cdb->buf_id >= 0)
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

	return -1;					/* exhausted */
}

static void
LirsTrickleIterEnd(void *strategy_data, void *iter_state)
{
	pfree(iter_state);
}


/* ----------------------------------------------------------------
 *			LIRS vtable and handler
 * ----------------------------------------------------------------
 */

static const BufferPoolRoutine lirs_pool_routine = {
	.type = T_Invalid,
	.on_hit = LirsOnHit,
	.on_miss = LirsOnMiss,
	.on_evict = LirsOnEvict,
	.on_new_tag = LirsOnNewTag,
	.get_victim = LirsGetVictim,
	.sync_start = LirsSyncStart,
	.notify_trickle = LirsNotifyTrickle,
	.trickle_iter_begin = LirsTrickleIterBegin,
	.trickle_iter_next = LirsTrickleIterNext,
	.trickle_iter_end = LirsTrickleIterEnd,
	.hint_vacuum = LirsHintVacuum,
	.reject_buffer = LirsRejectBuffer,
	.prefetch_hint = LirsPrefetchHint,
	.shmem_size = LirsShmemSize,
	.shmem_init = LirsShmemInit,
	.shutdown = LirsShutdown,
};

PG_FUNCTION_INFO_V1(lirs_pool_handler);

Datum
lirs_pool_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&lirs_pool_routine);
}


/* ----------------------------------------------------------------
 *			LIRS statistics SRF
 * ----------------------------------------------------------------
 */

#define PG_STAT_GET_LIRS_STATS_COLS 17

PG_FUNCTION_INFO_V1(pg_stat_get_lirs_stats);

Datum
pg_stat_get_lirs_stats(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	InitMaterializedSRF(fcinfo, 0);

	for (int i = 0; i < NBufferPools; i++)
	{
		Datum		values[PG_STAT_GET_LIRS_STATS_COLS] = {0};
		bool		nulls[PG_STAT_GET_LIRS_STATS_COLS] = {0};
		BufferPoolDesc *pool = &BufferPoolDescs[i];
		LirsControl *ctl;

		if (!pool->bp_active)
			continue;
		if (pool->bp_routine != &lirs_pool_routine)
			continue;

		{
			PoolLocalState *local = EnsurePoolAttached(pool);

			ctl = (LirsControl *) local->strategy_data;
		}
		if (ctl == NULL)
			continue;

		values[0] = NameGetDatum(&pool->bp_name);

		if (OidIsValid(pool->bp_oid))
			values[1] = ObjectIdGetDatum(pool->bp_oid);
		else
			nulls[1] = true;

		SpinLockAcquire(&ctl->lirs_lock);
		values[2] = Int32GetDatum(ctl->lir_count);
		values[3] = Int32GetDatum(ctl->hir_count);
		values[4] = Int32GetDatum(ctl->ghost_count);
		values[5] = Int32GetDatum(ctl->lir_capacity);
		values[6] = Int32GetDatum(ctl->stack_size);
		values[7] = Int32GetDatum(ctl->q_size);
		SpinLockRelease(&ctl->lirs_lock);

		/* Flush this backend's pending local stats before reading */
		PoolStatFlush(&lirs_local_stats[LIRS_STAT_LOOKUPS], &ctl->stat_lookups);
		PoolStatFlush(&lirs_local_stats[LIRS_STAT_LIR_HITS], &ctl->stat_lir_hits);
		PoolStatFlush(&lirs_local_stats[LIRS_STAT_HIR_HITS], &ctl->stat_hir_hits);
		PoolStatFlush(&lirs_local_stats[LIRS_STAT_GHOST_HITS], &ctl->stat_ghost_hits);
		PoolStatFlush(&lirs_local_stats[LIRS_STAT_MISSES], &ctl->stat_misses);
		PoolStatFlush(&lirs_local_stats[LIRS_STAT_LIR_DEMOTIONS], &ctl->stat_lir_demotions);
		PoolStatFlush(&lirs_local_stats[LIRS_STAT_HIR_PROMOTIONS], &ctl->stat_hir_promotions);
		PoolStatFlush(&lirs_local_stats[LIRS_STAT_EVICTIONS], &ctl->stat_evictions);
		PoolStatFlush(&lirs_local_stats[LIRS_STAT_STACK_PRUNES], &ctl->stat_stack_prunes);

		values[8] = Int64GetDatum(pg_atomic_read_u64(&ctl->stat_lookups));
		values[9] = Int64GetDatum(pg_atomic_read_u64(&ctl->stat_lir_hits));
		values[10] = Int64GetDatum(pg_atomic_read_u64(&ctl->stat_hir_hits));
		values[11] = Int64GetDatum(pg_atomic_read_u64(&ctl->stat_ghost_hits));
		values[12] = Int64GetDatum(pg_atomic_read_u64(&ctl->stat_misses));
		values[13] = Int64GetDatum(pg_atomic_read_u64(&ctl->stat_lir_demotions));
		values[14] = Int64GetDatum(pg_atomic_read_u64(&ctl->stat_hir_promotions));
		values[15] = Int64GetDatum(pg_atomic_read_u64(&ctl->stat_evictions));
		values[16] = Int64GetDatum(pg_atomic_read_u64(&ctl->stat_stack_prunes));

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
							 values, nulls);
	}

	return (Datum) 0;
}


/* ----------------------------------------------------------------
 *			LIRS size recommendation advisory
 * ----------------------------------------------------------------
 */

PG_FUNCTION_INFO_V1(pg_bp_lirs_size_recommendation);

Datum
pg_bp_lirs_size_recommendation(PG_FUNCTION_ARGS)
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
		LirsControl *ctl;
		int64		ghost_hits,
					misses,
					lookups,
					lir_hits,
					hir_hits;
		float8		ghost_pressure,
					hit_ratio;
		int			current_size,
					recommended_size;

		if (!pool->bp_active)
			continue;
		if (pool->bp_routine != &lirs_pool_routine)
			continue;
		if (namestrcmp(&pool->bp_name, NameStr(*pool_name)) != 0)
			continue;

		{
			PoolLocalState *local = EnsurePoolAttached(pool);

			ctl = (LirsControl *) local->strategy_data;
		}
		if (ctl == NULL)
			continue;

		current_size = ctl->nbuffers;
		ghost_hits = pg_atomic_read_u64(&ctl->stat_ghost_hits);
		misses = pg_atomic_read_u64(&ctl->stat_misses);
		lookups = pg_atomic_read_u64(&ctl->stat_lookups);
		lir_hits = pg_atomic_read_u64(&ctl->stat_lir_hits);
		hir_hits = pg_atomic_read_u64(&ctl->stat_hir_hits);

		ghost_pressure = (float8) ghost_hits / (float8) Max(misses, 1);
		hit_ratio = (lookups > 0) ?
			(float8) (lir_hits + hir_hits) / (float8) lookups : 0.0;

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
			 errmsg("LIRS pool \"%s\" not found", NameStr(*pool_name))));
	PG_RETURN_NULL();
}

/*
 * _PG_init -- register LIRS for use as the DEFAULT pool algorithm.
 */
void
_PG_init(void)
{
	RegisterDefaultPoolAlgorithm("lirs", &lirs_pool_routine);
}
