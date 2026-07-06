/*-------------------------------------------------------------------------
 *
 * pg_bp_arc.c
 *	  Adaptive Replacement Cache (ARC) buffer replacement algorithm.
 *
 * This extension provides the ARC algorithm as a BufferPoolRoutine handler,
 * usable with CREATE BUFFER POOL ... HANDLER arc_pool_handler.
 *
 * ARC maintains four lists:
 *   T1 - recently accessed pages (accessed once since entering cache)
 *   T2 - frequently accessed pages (accessed 2+ times)
 *   B1 - ghost entries for pages recently evicted from T1
 *   B2 - ghost entries for pages recently evicted from T2
 *
 * The adaptive parameter target_T1_size controls the balance between
 * T1 and T2.  Ghost list hits (B1 or B2) cause this target to shift,
 * adapting the cache to the workload.
 *
 * References:
 *   N. Megiddo and D. Modha, "ARC: A Self-Tuning, Low Overhead
 *   Replacement Cache", FAST 2003.
 *     https://www.usenix.org/event/fast03/tech/full_papers/megiddo/megiddo.pdf
 *   N. Megiddo and D. Modha, "Outperforming LRU with an Adaptive
 *   Replacement Cache Algorithm", IEEE Computer, 2004.
 *     https://ieeexplore.ieee.org/document/1297303
 *
 * Patent disposition
 * ------------------
 * ARC was historically encumbered by patents assigned to IBM
 * (US 6,996,676; US 7,096,321; US 7,058,766; US 8,612,689) and Sun
 * Microsystems (US 7,469,320).  As of 2024 the IBM family has reached
 * its 20-year statutory term and expired; the Sun patent reaches term
 * shortly thereafter.  Sun's ZFS shipped an ARC implementation under
 * the CDDL for years, which is the existing public-domain reference
 * point most people cite.  This file is included here on the
 * understanding that the ARC patents have expired; reviewers and
 * downstreams should still confirm with their own counsel before
 * relying on that conclusion in their jurisdiction.
 *
 * Implementation note
 * -------------------
 * The list-management and adapt-on-ghost-hit logic in this file
 * follows the pseudocode from the FAST 2003 paper and was cross-
 * checked against the publicly available reference implementation in
 * Caffeine's policy simulator (ArcPolicy.java), which is licensed
 * under the Apache License, Version 2.0:
 *
 *	https://github.com/ben-manes/caffeine/blob/master/simulator/
 *	  src/main/java/com/github/benmanes/caffeine/cache/simulator/
 *	  policy/adaptive/ArcPolicy.java
 *
 * Per the Caffeine project's Apache 2.0 grant:
 *
 *	Copyright 2015 Ben Manes. All Rights Reserved.
 *
 *	Licensed under the Apache License, Version 2.0 (the "License");
 *	you may not use this file except in compliance with the License.
 *	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 *	Unless required by applicable law or agreed to in writing, software
 *	distributed under the License is distributed on an "AS IS" BASIS,
 *	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *	implied.  See the License for the specific language governing
 *	permissions and limitations under the License.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  contrib/pg_bp_arc/pg_bp_arc.c
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
#include "storage/lwlock.h"
#include "storage/s_lock.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/tuplestore.h"

PG_MODULE_MAGIC_EXT(.name = "pg_bp_arc", .version = PG_VERSION);

void		_PG_init(void);

/* ARC list identifiers */
#define ARC_LIST_B1			0	/* ghost entries evicted from T1 */
#define ARC_LIST_T1			1	/* recently accessed (recency) */
#define ARC_LIST_T2			2	/* frequently accessed (frequency) */
#define ARC_LIST_B2			3	/* ghost entries evicted from T2 */
#define ARC_NUM_LISTS		4
#define ARC_LIST_UNUSED		(-1)

/* Stat slot indices for PoolStatIncrement */
#define ARC_STAT_LOOKUPS		0
#define ARC_STAT_T1_HITS		1
#define ARC_STAT_T2_HITS		2
#define ARC_STAT_B1_HITS		3
#define ARC_STAT_B2_HITS		4
#define ARC_STAT_MISSES			5
#define ARC_STAT_T1_EVICTIONS	6
#define ARC_STAT_T2_EVICTIONS	7
#define ARC_NUM_STATS			8

static uint64 arc_local_stats[ARC_NUM_STATS];

/*
 * ARC Cache Directory Block (CDB).
 *
 * Each CDB tracks a page's position in the ARC lists.
 * For T1/T2 entries, buf_id is the pool-local buffer index.
 * For B1/B2 ghost entries, buf_id is -1.
 */
typedef struct ArcCDB
{
	int			prev;			/* previous in doubly-linked list (-1 = head) */
	int			next;			/* next in doubly-linked list (-1 = tail) */
	int			list;			/* which ARC list, or ARC_LIST_UNUSED */
	BufferTag	buf_tag;		/* page identity */
	int			buf_id;			/* pool-local buffer index (-1 for ghost) */
	int			ghost_next;		/* next in ghost hash chain (-1 = end) */

	/*
	 * Deferred hot-path touch flag.  ArcOnHit (the hottest callback) sets
	 * this atomically without taking arc_lock; the actual T1->T2 / T2-MRU
	 * list move is applied lazily during arc_drain_touches(), which every
	 * lock holder runs before it reads list order/size.  This bit -- not the
	 * touch ring -- is the source of truth: drain applies a move iff the bit
	 * is set, so a stale or duplicate ring entry is a harmless skip and a
	 * concurrent hit is never lost or double-applied.
	 */
	pg_atomic_uint32 touched;
} ArcCDB;

/*
 * ARC shared-memory control block.
 *
 * This is allocated as the pool's bp_strategy_data via DSM.
 * The CDB array, ghost hash buckets, and buf-to-CDB map follow
 * the fixed header as variable-length arrays.
 */
typedef struct ArcControl
{
	/*
	 * LWLock protecting all mutable list state.  An LWLock (not a spinlock)
	 * is used because ArcGetVictim can hold it across an O(list) walk looking
	 * for an unpinned victim; a spinlock held that long busy-spins other
	 * backends and risks a "stuck spinlock" PANIC, whereas an LWLock sleeps.
	 * The hot path (ArcOnHit) does NOT take this lock -- see the touch ring.
	 */
	LWLock		arc_lock;

	int			nbuffers;		/* number of physical buffers in this pool */
	int			first_buf_id;	/* global buffer ID of first buffer in pool */
	int			ncdb;			/* total CDB entries (2 * nbuffers) */
	int			target_T1_size; /* adaptive T1 target */

	/*
	 * Deferred-touch ring.  ArcOnHit enqueues the touched pool-local buf_id
	 * here (lock-free) after setting the CDB's touched bit; arc_drain_touches
	 * consumes it under arc_lock.  Producers reserve a slot with an atomic
	 * fetch-add on touch_write; the consumer index touch_read is only ever
	 * advanced under arc_lock.  If the ring is full a producer falls back to
	 * taking arc_lock and applying its own move, so correctness never depends
	 * on the ring having room.  Sized to touch_ring_mask+1 (a power of two).
	 */
	uint32		touch_ring_mask;	/* ring capacity - 1 (power of two - 1) */
	pg_atomic_uint32 touch_write;	/* producer reservation counter */
	uint32		touch_read;		/* consumer index (arc_lock-protected) */

	/* Doubly-linked list heads and tails */
	int			list_head[ARC_NUM_LISTS];
	int			list_tail[ARC_NUM_LISTS];
	int			list_size[ARC_NUM_LISTS];

	/* Singly-linked free CDB list (uses .next field) */
	int			free_cdb_list;

	/* Ghost hash table parameters */
	int			ghost_hash_size;	/* power of 2, >= ncdb */

	/*
	 * Statistics (atomics so they can be read without lock). These count
	 * operations since last reset.
	 */
	pg_atomic_uint64 stat_lookups;
	pg_atomic_uint64 stat_t1_hits;
	pg_atomic_uint64 stat_t2_hits;
	pg_atomic_uint64 stat_b1_hits;
	pg_atomic_uint64 stat_b2_hits;
	pg_atomic_uint64 stat_misses;

	/* Per-list eviction counters */
	pg_atomic_uint64 stat_t1_evictions;
	pg_atomic_uint64 stat_t2_evictions;

	/*
	 * Variable-length arrays follow in this order: ArcCDB cdb[ncdb] int
	 * ghost_hash[ghost_hash_size] int buf_to_cdb[nbuffers] uint32
	 * touch_ring[touch_ring_mask + 1]
	 */
} ArcControl;

/*
 * Per-backend ARC state, scoped to a specific pool.
 *
 * These variables communicate between on_miss and get_victim within
 * a single BufferAlloc call.  Each backend keeps one per pool to
 * prevent cross-pool state leakage when multiple ARC pools exist.
 */
typedef struct ArcBackendState
{
	ArcControl *ctl;			/* owning pool, for consistency checks */
	int			ghost_cdb;		/* CDB index of ghost hit (-1 = miss) */
	int			evict_from;		/* list to evict from */
	bool		vacuum_hint;	/* true if VACUUM is active */
} ArcBackendState;

#define MAX_ARC_POOLS  MAX_BUFFER_POOLS
static ArcBackendState arc_backend_states[MAX_ARC_POOLS];
static int	arc_num_states = 0;

static ArcBackendState *
arc_get_backend_state(ArcControl *ctl)
{
	for (int i = 0; i < arc_num_states; i++)
	{
		if (arc_backend_states[i].ctl == ctl)
			return &arc_backend_states[i];
	}
	/* First time this backend encounters this pool */
	Assert(arc_num_states < MAX_ARC_POOLS);
	arc_backend_states[arc_num_states].ctl = ctl;
	arc_backend_states[arc_num_states].ghost_cdb = -1;
	arc_backend_states[arc_num_states].evict_from = ARC_LIST_T1;
	arc_backend_states[arc_num_states].vacuum_hint = false;
	return &arc_backend_states[arc_num_states++];
}

/*
 * Accessor macros for the variable-length arrays stored after ArcControl.
 */
#define ARC_CDB(ctl)		((ArcCDB *) ((char *)(ctl) + sizeof(ArcControl)))
#define ARC_GHOST_HASH(ctl)	((int *) ((char *)(ctl) + sizeof(ArcControl) + \
							 sizeof(ArcCDB) * (ctl)->ncdb))
#define ARC_BUF_TO_CDB(ctl) ((int *) ((char *)(ctl) + sizeof(ArcControl) + \
							 sizeof(ArcCDB) * (ctl)->ncdb + \
							 sizeof(int) * (ctl)->ghost_hash_size))
#define ARC_TOUCH_RING(ctl) ((pg_atomic_uint32 *) ((char *)(ctl) + sizeof(ArcControl) + \
							 sizeof(ArcCDB) * (ctl)->ncdb + \
							 sizeof(int) * (ctl)->ghost_hash_size + \
							 sizeof(int) * (ctl)->nbuffers))

/*
 * Ghost hash helper: compute bucket index for a tag.
 */
static inline uint32
arc_ghost_hash_bucket(ArcControl *ctl, const BufferTag *tag)
{
	uint32		h = tag_hash(tag, sizeof(BufferTag));

	return h & (ctl->ghost_hash_size - 1);
}

/*
 * Ghost hash: look up a tag in the ghost hash table.
 * Returns the CDB index if found, -1 otherwise.
 */
static int
arc_ghost_lookup(ArcControl *ctl, const BufferTag *tag)
{
	int		   *ghost_hash = ARC_GHOST_HASH(ctl);
	ArcCDB	   *cdb = ARC_CDB(ctl);
	uint32		bucket = arc_ghost_hash_bucket(ctl, tag);
	int			idx = ghost_hash[bucket];

	while (idx >= 0)
	{
		if (BufferTagsEqual(&cdb[idx].buf_tag, tag))
			return idx;
		idx = cdb[idx].ghost_next;
	}
	return -1;
}

/*
 * Ghost hash: insert a CDB index into the ghost hash table.
 */
static void
arc_ghost_insert(ArcControl *ctl, int cdb_idx)
{
	int		   *ghost_hash = ARC_GHOST_HASH(ctl);
	ArcCDB	   *cdb = ARC_CDB(ctl);
	uint32		bucket = arc_ghost_hash_bucket(ctl, &cdb[cdb_idx].buf_tag);

	cdb[cdb_idx].ghost_next = ghost_hash[bucket];
	ghost_hash[bucket] = cdb_idx;
}

/*
 * Ghost hash: remove a CDB index from the ghost hash table.
 */
static void
arc_ghost_remove(ArcControl *ctl, int cdb_idx)
{
	int		   *ghost_hash = ARC_GHOST_HASH(ctl);
	ArcCDB	   *cdb = ARC_CDB(ctl);
	uint32		bucket = arc_ghost_hash_bucket(ctl, &cdb[cdb_idx].buf_tag);
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

/*
 * Remove a CDB from its doubly-linked ARC list.
 */
static inline void
arc_list_remove(ArcControl *ctl, ArcCDB *cdb_arr, int cdb_idx)
{
	ArcCDB	   *cdb = &cdb_arr[cdb_idx];
	int			list = cdb->list;

	Assert(list >= 0 && list < ARC_NUM_LISTS);

	if (cdb->prev < 0)
		ctl->list_head[list] = cdb->next;
	else
		cdb_arr[cdb->prev].next = cdb->next;

	if (cdb->next < 0)
		ctl->list_tail[list] = cdb->prev;
	else
		cdb_arr[cdb->next].prev = cdb->prev;

	ctl->list_size[list]--;
	cdb->list = ARC_LIST_UNUSED;
	cdb->prev = -1;
	cdb->next = -1;
}

/*
 * Insert a CDB at the MRU (tail) position of an ARC list.
 */
static inline void
arc_mru_insert(ArcControl *ctl, ArcCDB *cdb_arr, int cdb_idx, int list)
{
	ArcCDB	   *cdb = &cdb_arr[cdb_idx];

	Assert(cdb->list == ARC_LIST_UNUSED);

	if (ctl->list_tail[list] < 0)
	{
		/* empty list */
		cdb->prev = -1;
		cdb->next = -1;
		ctl->list_head[list] = cdb_idx;
		ctl->list_tail[list] = cdb_idx;
	}
	else
	{
		cdb->next = -1;
		cdb->prev = ctl->list_tail[list];
		cdb_arr[ctl->list_tail[list]].next = cdb_idx;
		ctl->list_tail[list] = cdb_idx;
	}
	ctl->list_size[list]++;
	cdb->list = list;
}

/*
 * Insert a CDB at the LRU (head) position of an ARC list.
 */
static inline void
arc_lru_insert(ArcControl *ctl, ArcCDB *cdb_arr, int cdb_idx, int list)
{
	ArcCDB	   *cdb = &cdb_arr[cdb_idx];

	Assert(cdb->list == ARC_LIST_UNUSED);

	if (ctl->list_head[list] < 0)
	{
		/* empty list */
		cdb->prev = -1;
		cdb->next = -1;
		ctl->list_head[list] = cdb_idx;
		ctl->list_tail[list] = cdb_idx;
	}
	else
	{
		cdb->prev = -1;
		cdb->next = ctl->list_head[list];
		cdb_arr[ctl->list_head[list]].prev = cdb_idx;
		ctl->list_head[list] = cdb_idx;
	}
	ctl->list_size[list]++;
	cdb->list = list;
}

/*
 * Allocate a free CDB.  If none are free, recycle the LRU ghost entry
 * from B1 or B2.
 */
static int
arc_alloc_cdb(ArcControl *ctl, ArcCDB *cdb_arr)
{
	int			idx;

	if (ctl->free_cdb_list >= 0)
	{
		idx = ctl->free_cdb_list;
		ctl->free_cdb_list = cdb_arr[idx].next;
		cdb_arr[idx].next = -1;
		return idx;
	}

	/*
	 * No free CDBs.  Recycle from ghost lists. Prefer B1 if T1+B1 >= c (the
	 * total cache size), else B2.
	 */
	if (ctl->list_size[ARC_LIST_B1] > 0 &&
		(ctl->list_size[ARC_LIST_T1] + ctl->list_size[ARC_LIST_B1]) >= ctl->nbuffers)
		idx = ctl->list_head[ARC_LIST_B1];
	else if (ctl->list_size[ARC_LIST_B2] > 0)
		idx = ctl->list_head[ARC_LIST_B2];
	else if (ctl->list_size[ARC_LIST_B1] > 0)
		idx = ctl->list_head[ARC_LIST_B1];
	else
	{
		/*
		 * Release the LWLock before ereport to keep the hold time explicit
		 * (LWLockReleaseAll would also release it at abort).
		 */
		LWLockRelease(&ctl->arc_lock);
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("ARC: no CDB entries available for recycling")));
	}

	/* Remove from ghost hash and list */
	arc_ghost_remove(ctl, idx);
	arc_list_remove(ctl, cdb_arr, idx);
	cdb_arr[idx].buf_id = -1;
	ClearBufferTag(&cdb_arr[idx].buf_tag);

	return idx;
}


/* ----------------------------------------------------------------
 *			Deferred hot-path touch (scalability)
 * ----------------------------------------------------------------
 *
 * ArcOnHit runs on every buffer cache hit and is by far the hottest
 * callback.  Doing the T1->T2 / T2-MRU list move under arc_lock there
 * serializes all hits on one lock -- pure contention, not algorithm.
 *
 * Instead ArcOnHit records the hit lock-free: it sets the CDB's atomic
 * "touched" bit and enqueues the pool-local buf_id into a bounded ring.
 * The move itself is applied later by arc_drain_touches(), which every
 * arc_lock holder calls FIRST, before it reads any list order or size.
 *
 * Semantic equivalence to the original synchronous move:
 *
 *   - The touched bit, not the ring, is the source of truth.  Drain
 *     applies a move iff the bit is set (cleared atomically as it is
 *     consumed), so a stale/duplicate ring slot is a harmless skip and
 *     no hit is ever lost or applied twice.
 *   - Every code path that inspects list order/size for an eviction
 *     decision (ArcOnMiss's evict_from + ghost adapt, arc_alloc_cdb's
 *     B1/B2 choice, ArcGetVictim's LRU walk, ArcOnNewTag, the stats
 *     reader) drains first while holding arc_lock.  So at the instant a
 *     decision is computed, every hit that happened-before it (i.e. whose
 *     ArcOnHit returned before the drainer acquired arc_lock) is already
 *     reflected in the lists -- identical to synchronous application.
 *   - Concurrent hits are only ever ordered arbitrarily relative to each
 *     other in BOTH the original (spinlock acquisition order) and this
 *     code (ring enqueue order); ARC's decisions do not depend on the
 *     interleaving of hits that are not ordered by happens-before.
 *
 * The move applied at drain is exactly what ArcOnHit did synchronously:
 * a page on T1 is promoted to T2-MRU, a page already on T2 is moved to
 * T2-MRU; a page no longer on T1/T2 (evicted meanwhile) is skipped.
 */

/* Sentinel: ring stores buf_id + 1 so 0 means "empty slot". */
#define ARC_TOUCH_EMPTY		0

/*
 * arc_apply_touch -- apply one deferred hit to the ARC lists.
 *
 * Caller must hold arc_lock.  local_id is a pool-local buffer index.
 * Mirrors the synchronous body of the old ArcOnHit exactly.
 */
static inline void
arc_apply_touch(ArcControl *ctl, ArcCDB *cdb_arr, int local_id)
{
	int		   *buf_to_cdb = ARC_BUF_TO_CDB(ctl);
	int			cdb_idx;

	if (local_id < 0 || local_id >= ctl->nbuffers)
		return;

	cdb_idx = buf_to_cdb[local_id];
	if (cdb_idx < 0)
		return;					/* buffer no longer tracked */

	/*
	 * Consume the touched bit.  If it was not set, this ring entry is stale
	 * (already drained, or the buffer was recycled): skip.
	 */
	if (pg_atomic_exchange_u32(&cdb_arr[cdb_idx].touched, 0) == 0)
		return;

	if (cdb_arr[cdb_idx].list == ARC_LIST_T1)
	{
		PoolStatIncrement(&arc_local_stats[ARC_STAT_T1_HITS], &ctl->stat_t1_hits);
		arc_list_remove(ctl, cdb_arr, cdb_idx);
		arc_mru_insert(ctl, cdb_arr, cdb_idx, ARC_LIST_T2);
	}
	else if (cdb_arr[cdb_idx].list == ARC_LIST_T2)
	{
		PoolStatIncrement(&arc_local_stats[ARC_STAT_T2_HITS], &ctl->stat_t2_hits);
		arc_list_remove(ctl, cdb_arr, cdb_idx);
		arc_mru_insert(ctl, cdb_arr, cdb_idx, ARC_LIST_T2);
	}
	/* else: on B1/B2/UNUSED now -- nothing to promote */
}

/*
 * arc_drain_touches -- apply all pending deferred hits.
 *
 * Caller must hold arc_lock.  Consumes ring slots [touch_read, touch_write)
 * in FIFO order and applies each, preserving the exact order in which the
 * producers reserved their slots.  touch_read is arc_lock-protected;
 * producers only append (fetch-add on touch_write, then a single store of
 * buf_id+1 into the reserved slot).
 *
 * A producer can reserve slot i (bumping touch_write) a few instructions
 * before it stores buf_id+1 there.  To keep FIFO order and never let a
 * later, already-published hit be applied before an earlier reserved one
 * (which could otherwise let a just-hit page be chosen as a victim), the
 * drainer waits for that in-flight store rather than skipping the slot.
 * The wait is bounded and safe: the producer holds no lock between the
 * fetch-add and the store (no syscall, no allocation, no CHECK_FOR_
 * INTERRUPTS), so it publishes in a few instructions.  spin_delay() yields
 * appropriately if the producer was descheduled.  Because every published
 * hit is thus applied before any eviction decision ordered after it, the
 * lists at each decision point are identical to the original synchronous
 * ArcOnHit -- ARC's eviction choices are unchanged.
 */
static void
arc_drain_touches(ArcControl *ctl)
{
	ArcCDB	   *cdb_arr = ARC_CDB(ctl);
	pg_atomic_uint32 *ring = ARC_TOUCH_RING(ctl);
	uint32		mask = ctl->touch_ring_mask;
	uint32		write_idx = pg_atomic_read_u32(&ctl->touch_write);

	while (ctl->touch_read != write_idx)
	{
		uint32		slot = ctl->touch_read & mask;
		uint32		val;
		SpinDelayStatus delay;

		/* Wait for the reserving producer to publish this slot. */
		init_local_spin_delay(&delay);
		while ((val = pg_atomic_read_u32(&ring[slot])) == ARC_TOUCH_EMPTY)
			perform_spin_delay(&delay);
		finish_spin_delay(&delay);

		pg_atomic_write_u32(&ring[slot], ARC_TOUCH_EMPTY);
		ctl->touch_read++;
		arc_apply_touch(ctl, cdb_arr, (int) (val - 1));
	}
}


/* ----------------------------------------------------------------
 *			ARC list consistency validation (debug builds)
 * ----------------------------------------------------------------
 */

#ifdef USE_ASSERT_CHECKING
/*
 * ArcValidateLists -- validate ARC list consistency.
 *
 * Must be called while holding arc_lock.  Checks:
 *   - All CDBs on T1/T2 have buf_id >= 0
 *   - All CDBs on B1/B2 have buf_id == -1
 *   - list_size[] matches actual linked-list count
 *   - buf_to_cdb[] is consistent with CDB buf_id fields
 */
static void
ArcValidateLists(ArcControl *ctl)
{
	ArcCDB	   *cdb_arr = ARC_CDB(ctl);
	int		   *buf_to_cdb = ARC_BUF_TO_CDB(ctl);

	for (int list = 0; list < ARC_NUM_LISTS; list++)
	{
		int			count = 0;
		int			idx = ctl->list_head[list];

		while (idx >= 0)
		{
			Assert(idx < ctl->ncdb);
			Assert(cdb_arr[idx].list == list);

			if (list == ARC_LIST_T1 || list == ARC_LIST_T2)
			{
				Assert(cdb_arr[idx].buf_id >= 0);
				Assert(cdb_arr[idx].buf_id < ctl->nbuffers);
			}
			else
			{
				/* B1 or B2: ghost entries must not have a buffer */
				Assert(cdb_arr[idx].buf_id == -1);
			}

			count++;
			Assert(count <= ctl->ncdb); /* guard against infinite loop */
			idx = cdb_arr[idx].next;
		}

		Assert(ctl->list_size[list] == count);
	}

	/* Validate buf_to_cdb consistency */
	for (int i = 0; i < ctl->nbuffers; i++)
	{
		int			cdb_idx = buf_to_cdb[i];

		if (cdb_idx >= 0)
		{
			Assert(cdb_idx < ctl->ncdb);
			Assert(cdb_arr[cdb_idx].buf_id == i);
			Assert(cdb_arr[cdb_idx].list == ARC_LIST_T1 ||
				   cdb_arr[cdb_idx].list == ARC_LIST_T2);
		}
	}
}
#endif							/* USE_ASSERT_CHECKING */


/* ----------------------------------------------------------------
 *			ARC vtable callback implementations
 * ----------------------------------------------------------------
 */

/*
 * ArcOnHit -- called when a page is found in the buffer cache.
 *
 * HOT PATH.  Takes NO lock.  Records the hit for later application: sets
 * the CDB's atomic touched bit and, if this producer was the one that
 * flipped it 0->1, enqueues the pool-local buf_id into the touch ring.
 * The actual T1->T2 / T2-MRU move (and the t1/t2 hit stat) is applied by
 * arc_drain_touches() under arc_lock before any eviction decision -- see
 * the deferred-touch commentary above for why this preserves ARC's exact
 * eviction semantics.
 *
 * The touched bit dedupes: a page hammered many times between drains only
 * occupies one ring slot.  If the ring is momentarily full we fall back to
 * taking arc_lock and applying the move synchronously, so correctness never
 * depends on the ring having room.
 */
static void
ArcOnHit(void *strategy_data, int buf_id, BufferTag *tag)
{
	ArcControl *ctl = (ArcControl *) strategy_data;
	ArcCDB	   *cdb_arr = ARC_CDB(ctl);
	int		   *buf_to_cdb = ARC_BUF_TO_CDB(ctl);
	int			pool_local_id;
	int			cdb_idx;
	uint32		mask = ctl->touch_ring_mask;
	uint32		idx;

	PoolStatIncrement(&arc_local_stats[ARC_STAT_LOOKUPS], &ctl->stat_lookups);

	/*
	 * Convert global buf_id to pool-local index.  For the default pool,
	 * first_buf_id == 0 so this is a no-op.
	 */
	pool_local_id = buf_id - ctl->first_buf_id;

	if (pool_local_id < 0 || pool_local_id >= ctl->nbuffers)
		return;

	/*
	 * Read buf_to_cdb without the lock.  This is a plain int slot updated
	 * only under arc_lock; a stale read here is harmless -- we either mark a
	 * since-recycled CDB (drain skips it: the touched bit sits on a CDB no
	 * longer on T1/T2, or the mapping is gone) or miss an in-flight remap
	 * (that buffer's next hit re-marks it).  The eviction path always
	 * re-reads buf_to_cdb under the lock at drain time.
	 */
	cdb_idx = buf_to_cdb[pool_local_id];
	if (cdb_idx < 0 || cdb_idx >= ctl->ncdb)
		return;					/* not tracked by ARC */

	/*
	 * Dedupe: only enqueue if we are the producer that set the bit.  If it
	 * was already set, an earlier hit already has a ring slot pending drain.
	 */
	if (pg_atomic_fetch_or_u32(&cdb_arr[cdb_idx].touched, 1) != 0)
		return;

	/*
	 * Reserve a ring slot and publish buf_id+1 into it (stored as buf_id+1 so
	 * 0 stays the empty sentinel).  Reserve with a CAS loop so the capacity
	 * check and the touch_write bump are atomic: a producer never laps a slot
	 * that has not yet been drained.  The winning CAS and the following store
	 * are adjacent with nothing that can longjmp between them, so the slot is
	 * published within a few instructions -- the drainer relies on that.
	 *
	 * If the ring is momentarily full we must NOT bump touch_write without
	 * publishing (that would leave a permanent gap and deadlock the
	 * spin-waiting drainer).  Instead take arc_lock and apply the move
	 * synchronously; the touched bit we just set is consumed by
	 * arc_apply_touch, so no promotion is lost or doubled.
	 */
	for (;;)
	{
		idx = pg_atomic_read_u32(&ctl->touch_write);

		if (idx - ctl->touch_read >= mask + 1)
		{
			/* Ring full: apply synchronously under the lock. */
			LWLockAcquire(&ctl->arc_lock, LW_EXCLUSIVE);
			arc_apply_touch(ctl, cdb_arr, pool_local_id);
			LWLockRelease(&ctl->arc_lock);
			return;
		}

		if (pg_atomic_compare_exchange_u32(&ctl->touch_write, &idx, idx + 1))
			break;
	}

	pg_atomic_write_u32(&ARC_TOUCH_RING(ctl)[idx & mask],
						(uint32) (pool_local_id + 1));
}

/*
 * ArcOnMiss -- called when a page is NOT found in the buffer cache.
 *
 * Checks ghost lists (B1/B2) for the tag and adapts target_T1_size.
 * Sets backend-local state for the subsequent get_victim call.
 */
static void
ArcOnMiss(void *strategy_data, BufferTag *tag)
{
	ArcControl *ctl = (ArcControl *) strategy_data;
	ArcBackendState *state = arc_get_backend_state(ctl);
	int			ghost_idx;

	PoolStatIncrement(&arc_local_stats[ARC_STAT_LOOKUPS], &ctl->stat_lookups);

	LWLockAcquire(&ctl->arc_lock, LW_EXCLUSIVE);
	arc_drain_touches(ctl);

	ghost_idx = arc_ghost_lookup(ctl, tag);

	if (ghost_idx >= 0)
	{
		ArcCDB	   *cdb_arr = ARC_CDB(ctl);
		int			ghost_list = cdb_arr[ghost_idx].list;

		if (ghost_list == ARC_LIST_B1)
		{
			/*
			 * B1 hit: T1 cache is probably too small. Increase target T1
			 * size.
			 */
			int			delta;

			PoolStatIncrement(&arc_local_stats[ARC_STAT_B1_HITS], &ctl->stat_b1_hits);

			delta = Max(ctl->list_size[ARC_LIST_B2] /
						Max(ctl->list_size[ARC_LIST_B1], 1), 1);
			ctl->target_T1_size = Min(ctl->target_T1_size + delta,
									  ctl->nbuffers);

			state->ghost_cdb = ghost_idx;
		}
		else if (ghost_list == ARC_LIST_B2)
		{
			/*
			 * B2 hit: T2 cache is probably too small. Decrease target T1
			 * size.
			 */
			int			delta;

			PoolStatIncrement(&arc_local_stats[ARC_STAT_B2_HITS], &ctl->stat_b2_hits);

			delta = Max(ctl->list_size[ARC_LIST_B1] /
						Max(ctl->list_size[ARC_LIST_B2], 1), 1);
			ctl->target_T1_size = Max(ctl->target_T1_size - delta, 0);

			state->ghost_cdb = ghost_idx;
		}
		else
		{
			/* Ghost entry on unexpected list, treat as complete miss */
			state->ghost_cdb = -1;
		}
	}
	else
	{
		/* Complete cache miss */
		PoolStatIncrement(&arc_local_stats[ARC_STAT_MISSES], &ctl->stat_misses);
		state->ghost_cdb = -1;
	}

	/*
	 * Decide which list to evict from if we need a victim.
	 */
	if (ctl->list_size[ARC_LIST_T1] >= Max(1, ctl->target_T1_size))
		state->evict_from = ARC_LIST_T1;
	else
		state->evict_from = ARC_LIST_T2;

	LWLockRelease(&ctl->arc_lock);
}

/*
 * ArcOnEvict -- called when a buffer's old content is being evicted.
 *
 * Moves the CDB from T1/T2 to B1/B2 respectively, converting it
 * into a ghost entry.
 */
static void
ArcOnEvict(void *strategy_data, int buf_id, BufferTag *old_tag)
{
	ArcControl *ctl = (ArcControl *) strategy_data;
	ArcCDB	   *cdb_arr = ARC_CDB(ctl);
	int		   *buf_to_cdb = ARC_BUF_TO_CDB(ctl);
	int			pool_local_id = buf_id - ctl->first_buf_id;
	int			cdb_idx;
	int			old_list;

	LWLockAcquire(&ctl->arc_lock, LW_EXCLUSIVE);
	arc_drain_touches(ctl);

	if (pool_local_id < 0 || pool_local_id >= ctl->nbuffers)
	{
		LWLockRelease(&ctl->arc_lock);
		return;
	}

	cdb_idx = buf_to_cdb[pool_local_id];
	if (cdb_idx < 0)
	{
		/* Buffer not tracked (e.g., was a free buffer) */
		LWLockRelease(&ctl->arc_lock);
		elog(DEBUG1,
			 "ARC on_evict: buffer %d (pool-local %d) not tracked by ARC",
			 buf_id, pool_local_id);
		return;
	}

	old_list = cdb_arr[cdb_idx].list;

	/*
	 * Move the CDB to the corresponding ghost list and track eviction source.
	 */
	if (old_list == ARC_LIST_T1)
	{
		arc_list_remove(ctl, cdb_arr, cdb_idx);
		arc_mru_insert(ctl, cdb_arr, cdb_idx, ARC_LIST_B1);
		PoolStatIncrement(&arc_local_stats[ARC_STAT_T1_EVICTIONS], &ctl->stat_t1_evictions);
	}
	else if (old_list == ARC_LIST_T2)
	{
		arc_list_remove(ctl, cdb_arr, cdb_idx);
		arc_mru_insert(ctl, cdb_arr, cdb_idx, ARC_LIST_B2);
		PoolStatIncrement(&arc_local_stats[ARC_STAT_T2_EVICTIONS], &ctl->stat_t2_evictions);
	}
	else
	{
		/* Not on T1 or T2 -- shouldn't normally happen */
		LWLockRelease(&ctl->arc_lock);
		return;
	}

	/* Convert to ghost entry */
	cdb_arr[cdb_idx].buf_id = -1;
	buf_to_cdb[pool_local_id] = -1;

	/* Add to ghost hash table */
	arc_ghost_insert(ctl, cdb_idx);

#ifdef USE_ASSERT_CHECKING
	ArcValidateLists(ctl);
#endif

	LWLockRelease(&ctl->arc_lock);
}

/*
 * ArcOnNewTag -- called when a buffer is assigned a new page.
 *
 * Creates a new CDB for the page, or promotes a ghost CDB from B1/B2
 * to T2 if the on_miss found a ghost hit.
 */
static void
ArcOnNewTag(void *strategy_data, int buf_id, BufferTag *new_tag,
			bool vacuum_hint)
{
	ArcControl *ctl = (ArcControl *) strategy_data;
	ArcBackendState *state = arc_get_backend_state(ctl);
	ArcCDB	   *cdb_arr = ARC_CDB(ctl);
	int		   *buf_to_cdb = ARC_BUF_TO_CDB(ctl);
	int			pool_local_id = buf_id - ctl->first_buf_id;
	int			cdb_idx;

	Assert(pool_local_id >= 0 && pool_local_id < ctl->nbuffers);

	LWLockAcquire(&ctl->arc_lock, LW_EXCLUSIVE);
	arc_drain_touches(ctl);

	Assert(buf_to_cdb[pool_local_id] == -1);

	if (state->ghost_cdb >= 0)
	{
		/*
		 * This was a ghost hit (B1 or B2).  The ghost CDB already has the
		 * right tag.  Remove it from the ghost list and hash, then promote to
		 * T2 MRU (since it's been accessed before).
		 */
		cdb_idx = state->ghost_cdb;
		state->ghost_cdb = -1;

		/*
		 * Validate: the ghost CDB must still be on B1 or B2.  It can become
		 * stale if ExtendBufferedRelShared bypasses on_miss (e.g. during
		 * VACUUM FSM/VM extension) and a prior ghost_cdb leaks through.
		 */
		if (cdb_idx >= ctl->ncdb ||
			(cdb_arr[cdb_idx].list != ARC_LIST_B1 &&
			 cdb_arr[cdb_idx].list != ARC_LIST_B2))
		{
			/* Stale ghost_cdb -- treat as a complete miss */
			goto complete_miss;
		}

		arc_ghost_remove(ctl, cdb_idx);
		arc_list_remove(ctl, cdb_arr, cdb_idx);

		cdb_arr[cdb_idx].buf_id = pool_local_id;
		pg_atomic_write_u32(&cdb_arr[cdb_idx].touched, 0);
		arc_mru_insert(ctl, cdb_arr, cdb_idx, ARC_LIST_T2);
	}
	else
	{
		/*
		 * Complete cache miss.  Allocate a new CDB and place on T1.
		 */
complete_miss:
		cdb_idx = arc_alloc_cdb(ctl, cdb_arr);

		cdb_arr[cdb_idx].buf_tag = *new_tag;
		cdb_arr[cdb_idx].buf_id = pool_local_id;
		cdb_arr[cdb_idx].ghost_next = -1;
		pg_atomic_write_u32(&cdb_arr[cdb_idx].touched, 0);

		/*
		 * VACUUM optimization: insert at LRU of T1 so vacuum-loaded pages are
		 * evicted first, preventing cache pollution.
		 */
		if (vacuum_hint || state->vacuum_hint)
			arc_lru_insert(ctl, cdb_arr, cdb_idx, ARC_LIST_T1);
		else
			arc_mru_insert(ctl, cdb_arr, cdb_idx, ARC_LIST_T1);
	}

	/* Update buffer-to-CDB mapping */
	buf_to_cdb[pool_local_id] = cdb_idx;

#ifdef USE_ASSERT_CHECKING
	ArcValidateLists(ctl);
#endif

	LWLockRelease(&ctl->arc_lock);
}


/* ----------------------------------------------------------------
 *			ARC get_victim implementation
 * ----------------------------------------------------------------
 */

/*
 * ArcGetVictim -- select a victim buffer from T1 or T2.
 *
 * The eviction decision was prepared by ArcOnMiss (setting arc_evict_from).
 * We walk the chosen list from LRU looking for an unpinned buffer,
 * falling back to the other list if necessary.
 */
static BufferDesc *
ArcGetVictim(void *strategy_data, BufferAccessStrategy strategy,
			 uint64 *buf_state, bool *from_ring)
{
	ArcControl *ctl = (ArcControl *) strategy_data;
	ArcBackendState *state = arc_get_backend_state(ctl);
	ArcCDB	   *cdb_arr = ARC_CDB(ctl);
	BufferDesc *buf;
	int			cdb_idx;
	int			primary_list = state->evict_from;
	int			secondary_list = (primary_list == ARC_LIST_T1) ?
		ARC_LIST_T2 : ARC_LIST_T1;

	*from_ring = false;

	/*
	 * Try free/untracked buffers first.  During cold start or warm-up, T1 +
	 * T2 < nbuffers means there are free buffers available. Using them avoids
	 * unnecessary evictions from the ARC lists.
	 */
	if (ctl->list_size[ARC_LIST_T1] + ctl->list_size[ARC_LIST_T2] < ctl->nbuffers)
	{
		int		   *buf_to_cdb = ARC_BUF_TO_CDB(ctl);

		for (int i = 0; i < ctl->nbuffers; i++)
		{
			uint64		old_buf_state;
			uint64		local_buf_state;

			/* Skip buffers already tracked by ARC */
			if (buf_to_cdb[i] >= 0)
				continue;

			buf = GetBufferDescriptor(ctl->first_buf_id + i);

			old_buf_state = pg_atomic_read_u64(&buf->state);
			for (;;)
			{
				local_buf_state = old_buf_state;

				if (BUF_STATE_GET_REFCOUNT(local_buf_state) != 0)
					break;		/* pinned, skip */

				if (unlikely(local_buf_state & BM_LOCKED))
				{
					old_buf_state = WaitBufHdrUnlocked(buf);
					continue;
				}

				/* Try to pin */
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
	 * No free buffers available.  Walk the primary list from LRU (head)
	 * looking for an unpinned buffer to evict.
	 */
	LWLockAcquire(&ctl->arc_lock, LW_EXCLUSIVE);
	arc_drain_touches(ctl);

	cdb_idx = ctl->list_head[primary_list];
	while (cdb_idx >= 0)
	{
		uint64		old_buf_state;
		uint64		local_buf_state;

		if (cdb_arr[cdb_idx].buf_id < 0)
		{
			cdb_idx = cdb_arr[cdb_idx].next;
			continue;
		}

		Assert(cdb_arr[cdb_idx].buf_id >= 0 &&
			   cdb_arr[cdb_idx].buf_id < ctl->nbuffers);
		buf = GetBufferDescriptor(cdb_arr[cdb_idx].buf_id + ctl->first_buf_id);

		/*
		 * Try to pin this buffer using CAS, same pattern as ClockGetVictim.
		 */
		old_buf_state = pg_atomic_read_u64(&buf->state);
		for (;;)
		{
			local_buf_state = old_buf_state;

			if (BUF_STATE_GET_REFCOUNT(local_buf_state) != 0)
				break;			/* pinned, skip */

			if (unlikely(local_buf_state & BM_LOCKED))
			{
				LWLockRelease(&ctl->arc_lock);
				old_buf_state = WaitBufHdrUnlocked(buf);
				LWLockAcquire(&ctl->arc_lock, LW_EXCLUSIVE);
				continue;
			}

			/* Try to pin */
			local_buf_state += BUF_REFCOUNT_ONE;
			if (pg_atomic_compare_exchange_u64(&buf->state, &old_buf_state,
											   local_buf_state))
			{
				/* Success - found our victim */
				*buf_state = local_buf_state;
				LWLockRelease(&ctl->arc_lock);

				TrackNewBufferPin(BufferDescriptorGetBuffer(buf));
				return buf;
			}
		}

		cdb_idx = cdb_arr[cdb_idx].next;
	}

	/*
	 * No unpinned buffer found in primary list.  Try secondary list.
	 */
	cdb_idx = ctl->list_head[secondary_list];
	while (cdb_idx >= 0)
	{
		uint64		old_buf_state;
		uint64		local_buf_state;

		if (cdb_arr[cdb_idx].buf_id < 0)
		{
			cdb_idx = cdb_arr[cdb_idx].next;
			continue;
		}

		Assert(cdb_arr[cdb_idx].buf_id >= 0 &&
			   cdb_arr[cdb_idx].buf_id < ctl->nbuffers);
		buf = GetBufferDescriptor(cdb_arr[cdb_idx].buf_id + ctl->first_buf_id);

		old_buf_state = pg_atomic_read_u64(&buf->state);
		for (;;)
		{
			local_buf_state = old_buf_state;

			if (BUF_STATE_GET_REFCOUNT(local_buf_state) != 0)
				break;

			if (unlikely(local_buf_state & BM_LOCKED))
			{
				LWLockRelease(&ctl->arc_lock);
				old_buf_state = WaitBufHdrUnlocked(buf);
				LWLockAcquire(&ctl->arc_lock, LW_EXCLUSIVE);
				continue;
			}

			local_buf_state += BUF_REFCOUNT_ONE;
			if (pg_atomic_compare_exchange_u64(&buf->state, &old_buf_state,
											   local_buf_state))
			{
				*buf_state = local_buf_state;
				LWLockRelease(&ctl->arc_lock);

				TrackNewBufferPin(BufferDescriptorGetBuffer(buf));
				return buf;
			}
		}

		cdb_idx = cdb_arr[cdb_idx].next;
	}

	LWLockRelease(&ctl->arc_lock);

	ereport(ERROR,
			(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
			 errmsg("no unpinned buffers available in ARC pool"),
			 errhint("Increase the buffer pool size or reduce the number of concurrent queries.")));
	pg_unreachable();
}


/* ----------------------------------------------------------------
 *			ARC sync/trickle support
 * ----------------------------------------------------------------
 */

/*
 * ArcSyncStart -- return the current scan position for trickle writing.
 *
 * For ARC, we simply return 0 and provide allocation stats.
 */
static int
ArcSyncStart(void *strategy_data, uint32 *complete_passes,
			 uint32 *num_buf_alloc)
{
	if (complete_passes)
		*complete_passes = 0;
	if (num_buf_alloc)
		*num_buf_alloc = 0;
	return 0;
}

/*
 * ArcNotifyTrickle -- no-op for ARC; per-pool trickle writer handles this.
 */
static void
ArcNotifyTrickle(void *strategy_data, int bgwprocno)
{
	/* ARC pools use per-pool trickle writers instead */
}

/*
 * ArcRejectBuffer -- consider rejecting a dirty buffer from a ring strategy.
 *
 * ARC doesn't have special ring-buffer rejection logic.  Dirty buffers
 * from ring strategies are written out normally.
 */
static bool
ArcRejectBuffer(void *strategy_data, BufferAccessStrategy strategy,
				BufferDesc *buf, bool from_ring)
{
	/* ARC doesn't reject ring buffers; let the buffer manager write them */
	return false;
}

/*
 * ArcHintVacuum -- hint that VACUUM is active.
 */
static void
ArcHintVacuum(void *strategy_data, bool vacuum_active)
{
	ArcControl *ctl = (ArcControl *) strategy_data;
	ArcBackendState *state = arc_get_backend_state(ctl);

	state->vacuum_hint = vacuum_active;
}

/*
 * ArcPrefetchHint -- ghost-list-aware prefetch integration.
 *
 * When called with upcoming page tags, checks B1/B2 ghost lists.
 * Pages found in ghost lists are moved to the MRU end to prevent
 * premature recycling before the actual access arrives.
 */
static void
ArcPrefetchHint(void *strategy_data, BufferTag *tags, int ntags)
{
	ArcControl *ctl = (ArcControl *) strategy_data;
	ArcCDB	   *cdb_arr;

	if (ntags <= 0)
		return;

	LWLockAcquire(&ctl->arc_lock, LW_EXCLUSIVE);
	cdb_arr = ARC_CDB(ctl);

	for (int i = 0; i < ntags; i++)
	{
		int			ghost_idx = arc_ghost_lookup(ctl, &tags[i]);

		if (ghost_idx >= 0)
		{
			int			ghost_list = cdb_arr[ghost_idx].list;

			/*
			 * Move to MRU end of its ghost list to prevent recycling before
			 * the actual access arrives.
			 */
			if (ghost_list == ARC_LIST_B1 || ghost_list == ARC_LIST_B2)
			{
				arc_list_remove(ctl, cdb_arr, ghost_idx);
				arc_mru_insert(ctl, cdb_arr, ghost_idx, ghost_list);
			}
		}
	}

	LWLockRelease(&ctl->arc_lock);
}


/* ----------------------------------------------------------------
 *			ARC lifecycle (shmem_size / shmem_init / shutdown)
 * ----------------------------------------------------------------
 */

/*
 * ArcShmemSize -- compute shared memory needed for ARC with nbuffers.
 */
static Size
ArcShmemSize(int nbuffers)
{
	int			ncdb = nbuffers * 2;
	int			ghost_hash_size = pg_nextpower2_32(Max(ncdb, 64));
	Size		size;

	size = sizeof(ArcControl);
	size = MAXALIGN(size);
	size += sizeof(ArcCDB) * ncdb;
	size = MAXALIGN(size);
	size += sizeof(int) * ghost_hash_size;
	size = MAXALIGN(size);
	size += sizeof(int) * nbuffers;
	size = MAXALIGN(size);
	/* deferred-touch ring: one power-of-two slot per buffer is plenty */
	size += sizeof(pg_atomic_uint32) * pg_nextpower2_32(Max(nbuffers, 64));
	size = MAXALIGN(size);

	return size;
}

/*
 * ArcShmemInit -- initialize ARC data structures in shared memory.
 */
static void
ArcShmemInit(void *strategy_data, int nbuffers, int first_buf_id, bool init)
{
	ArcControl *ctl = (ArcControl *) strategy_data;
	ArcCDB	   *cdb_arr;
	int		   *ghost_hash;
	int		   *buf_to_cdb;
	pg_atomic_uint32 *touch_ring;
	int			ncdb;
	int			ghost_hash_size;
	uint32		touch_ring_size;

	if (!init)
	{
		/* Re-attach: just validate */
		Assert(ctl->nbuffers == nbuffers);
		return;
	}

	ncdb = nbuffers * 2;
	ghost_hash_size = pg_nextpower2_32(Max(ncdb, 64));
	touch_ring_size = pg_nextpower2_32(Max(nbuffers, 64));

	LWLockInitialize(&ctl->arc_lock, LWLockNewTrancheId("pg_bp_arc"));
	ctl->nbuffers = nbuffers;
	ctl->first_buf_id = first_buf_id;
	ctl->ncdb = ncdb;
	ctl->target_T1_size = nbuffers / 2;
	ctl->ghost_hash_size = ghost_hash_size;

	/* Deferred-touch ring (power of two; mask = size - 1) */
	ctl->touch_ring_mask = touch_ring_size - 1;
	ctl->touch_read = 0;
	pg_atomic_init_u32(&ctl->touch_write, 0);

	/* Initialize lists to empty */
	for (int i = 0; i < ARC_NUM_LISTS; i++)
	{
		ctl->list_head[i] = -1;
		ctl->list_tail[i] = -1;
		ctl->list_size[i] = 0;
	}

	/* Initialize statistics */
	pg_atomic_init_u64(&ctl->stat_lookups, 0);
	pg_atomic_init_u64(&ctl->stat_t1_hits, 0);
	pg_atomic_init_u64(&ctl->stat_t2_hits, 0);
	pg_atomic_init_u64(&ctl->stat_b1_hits, 0);
	pg_atomic_init_u64(&ctl->stat_b2_hits, 0);
	pg_atomic_init_u64(&ctl->stat_misses, 0);
	pg_atomic_init_u64(&ctl->stat_t1_evictions, 0);
	pg_atomic_init_u64(&ctl->stat_t2_evictions, 0);

	/* Initialize CDB array: all entries on the free list */
	cdb_arr = ARC_CDB(ctl);
	for (int i = 0; i < ncdb; i++)
	{
		cdb_arr[i].prev = -1;
		cdb_arr[i].next = (i < ncdb - 1) ? i + 1 : -1;
		cdb_arr[i].list = ARC_LIST_UNUSED;
		ClearBufferTag(&cdb_arr[i].buf_tag);
		cdb_arr[i].buf_id = -1;
		cdb_arr[i].ghost_next = -1;
		pg_atomic_init_u32(&cdb_arr[i].touched, 0);
	}
	ctl->free_cdb_list = 0;

	/* Initialize ghost hash table to empty */
	ghost_hash = ARC_GHOST_HASH(ctl);
	for (int i = 0; i < ghost_hash_size; i++)
		ghost_hash[i] = -1;

	/* Initialize buffer-to-CDB mapping to unmapped */
	buf_to_cdb = ARC_BUF_TO_CDB(ctl);
	for (int i = 0; i < nbuffers; i++)
		buf_to_cdb[i] = -1;

	/* Initialize deferred-touch ring to empty */
	touch_ring = ARC_TOUCH_RING(ctl);
	for (uint32 i = 0; i < touch_ring_size; i++)
		pg_atomic_init_u32(&touch_ring[i], ARC_TOUCH_EMPTY);
}

/*
 * ArcShutdown -- cleanup on pool destruction.
 */
static void
ArcShutdown(void *strategy_data)
{
	/* Nothing to clean up; all state is in DSM which will be freed */
}


/* ----------------------------------------------------------------
 *			ARC trickle writer iterator
 *
 * Walk T1 from LRU end (cheapest to evict), then T2 LRU end.
 * Yield dirty+unpinned buffers as proactive flush candidates.
 * ----------------------------------------------------------------
 */

typedef struct ArcTrickleIter
{
	ArcControl *ctl;
	int			phase;			/* 0 = scanning T1, 1 = scanning T2, 2 = done */
	int			cdb_idx;		/* current CDB index in the list */
	int			yielded;
	int			max_candidates;
} ArcTrickleIter;

static void *
ArcTrickleIterBegin(void *strategy_data, int max_candidates)
{
	ArcControl *ctl = (ArcControl *) strategy_data;
	ArcTrickleIter *iter;

	iter = (ArcTrickleIter *) palloc(sizeof(ArcTrickleIter));
	iter->ctl = ctl;
	iter->phase = 0;
	iter->yielded = 0;
	iter->max_candidates = max_candidates;

	/* Snapshot the T1 head position under the lock */
	LWLockAcquire(&ctl->arc_lock, LW_EXCLUSIVE);
	iter->cdb_idx = ctl->list_head[ARC_LIST_T1];
	LWLockRelease(&ctl->arc_lock);

	return iter;
}

static int
ArcTrickleIterNext(void *strategy_data, void *iter_state)
{
	ArcTrickleIter *iter = (ArcTrickleIter *) iter_state;
	ArcControl *ctl = iter->ctl;
	ArcCDB	   *cdb_arr = ARC_CDB(ctl);

	if (iter->yielded >= iter->max_candidates)
		return -1;

	while (iter->phase < 2)
	{
		while (iter->cdb_idx >= 0)
		{
			ArcCDB	   *cdb = &cdb_arr[iter->cdb_idx];
			int			next_idx = cdb->next;

			/* Advance iterator for next call */
			iter->cdb_idx = next_idx;

			/* Only consider resident entries (buf_id >= 0) */
			if (cdb->buf_id >= 0)
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
			LWLockAcquire(&ctl->arc_lock, LW_EXCLUSIVE);
			iter->cdb_idx = ctl->list_head[ARC_LIST_T2];
			LWLockRelease(&ctl->arc_lock);
		}
	}

	return -1;					/* exhausted */
}

static void
ArcTrickleIterEnd(void *strategy_data, void *iter_state)
{
	pfree(iter_state);
}


/* ----------------------------------------------------------------
 *			ARC vtable and handler function
 * ----------------------------------------------------------------
 */

static const BufferPoolRoutine arc_pool_routine = {
	.type = T_Invalid,			/* vtable is never node-walked; T_Invalid is
								 * fine */
	.on_hit = ArcOnHit,
	.on_miss = ArcOnMiss,
	.on_evict = ArcOnEvict,
	.on_new_tag = ArcOnNewTag,
	.get_victim = ArcGetVictim,
	.sync_start = ArcSyncStart,
	.notify_trickle = ArcNotifyTrickle,
	.trickle_iter_begin = ArcTrickleIterBegin,
	.trickle_iter_next = ArcTrickleIterNext,
	.trickle_iter_end = ArcTrickleIterEnd,
	.hint_vacuum = ArcHintVacuum,
	.reject_buffer = ArcRejectBuffer,
	.prefetch_hint = ArcPrefetchHint,
	.shmem_size = ArcShmemSize,
	.shmem_init = ArcShmemInit,
	.shutdown = ArcShutdown,
};

/*
 * arc_pool_handler -- SQL-callable handler returning the ARC BufferPoolRoutine.
 */
PG_FUNCTION_INFO_V1(arc_pool_handler);

Datum
arc_pool_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&arc_pool_routine);
}


/* ----------------------------------------------------------------
 *			ARC statistics SRF
 * ----------------------------------------------------------------
 */

/*
 * pg_stat_get_arc_stats -- return ARC algorithm statistics as a SRF.
 *
 * Returns one row per ARC-managed buffer pool with columns:
 *   name, oid, t1_size, t2_size, b1_size, b2_size, target_t1_size,
 *   lookups, t1_hits, t2_hits, b1_hits, b2_hits, misses,
 *   t1_evictions, t2_evictions
 */
#define PG_STAT_GET_ARC_STATS_COLS 15

PG_FUNCTION_INFO_V1(pg_stat_get_arc_stats);

Datum
pg_stat_get_arc_stats(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	InitMaterializedSRF(fcinfo, 0);

	for (int i = 0; i < NBufferPools; i++)
	{
		Datum		values[PG_STAT_GET_ARC_STATS_COLS] = {0};
		bool		nulls[PG_STAT_GET_ARC_STATS_COLS] = {0};
		BufferPoolDesc *pool = &BufferPoolDescs[i];
		ArcControl *ctl;

		if (!pool->bp_active)
			continue;

		/* Only report pools that use the ARC algorithm */
		if (pool->bp_routine != &arc_pool_routine)
			continue;

		/* Dynamic pool: attach if needed and get strategy data */
		{
			PoolLocalState *local = EnsurePoolAttached(pool);

			ctl = (ArcControl *) local->strategy_data;
		}

		if (ctl == NULL)
			continue;

		/* Pool name */
		values[0] = NameGetDatum(&pool->bp_name);

		/* Pool OID */
		if (OidIsValid(pool->bp_oid))
			values[1] = ObjectIdGetDatum(pool->bp_oid);
		else
			nulls[1] = true;

		/* List sizes (read under the lock for consistency) */
		LWLockAcquire(&ctl->arc_lock, LW_EXCLUSIVE);
		arc_drain_touches(ctl);
		values[2] = Int32GetDatum(ctl->list_size[ARC_LIST_T1]);
		values[3] = Int32GetDatum(ctl->list_size[ARC_LIST_T2]);
		values[4] = Int32GetDatum(ctl->list_size[ARC_LIST_B1]);
		values[5] = Int32GetDatum(ctl->list_size[ARC_LIST_B2]);
		values[6] = Int32GetDatum(ctl->target_T1_size);
		LWLockRelease(&ctl->arc_lock);

		/* Flush this backend's pending local stats before reading */
		PoolStatFlush(&arc_local_stats[ARC_STAT_LOOKUPS], &ctl->stat_lookups);
		PoolStatFlush(&arc_local_stats[ARC_STAT_T1_HITS], &ctl->stat_t1_hits);
		PoolStatFlush(&arc_local_stats[ARC_STAT_T2_HITS], &ctl->stat_t2_hits);
		PoolStatFlush(&arc_local_stats[ARC_STAT_B1_HITS], &ctl->stat_b1_hits);
		PoolStatFlush(&arc_local_stats[ARC_STAT_B2_HITS], &ctl->stat_b2_hits);
		PoolStatFlush(&arc_local_stats[ARC_STAT_MISSES], &ctl->stat_misses);
		PoolStatFlush(&arc_local_stats[ARC_STAT_T1_EVICTIONS], &ctl->stat_t1_evictions);
		PoolStatFlush(&arc_local_stats[ARC_STAT_T2_EVICTIONS], &ctl->stat_t2_evictions);

		/* Statistics (atomics, no lock needed) */
		values[7] = Int64GetDatum(pg_atomic_read_u64(&ctl->stat_lookups));
		values[8] = Int64GetDatum(pg_atomic_read_u64(&ctl->stat_t1_hits));
		values[9] = Int64GetDatum(pg_atomic_read_u64(&ctl->stat_t2_hits));
		values[10] = Int64GetDatum(pg_atomic_read_u64(&ctl->stat_b1_hits));
		values[11] = Int64GetDatum(pg_atomic_read_u64(&ctl->stat_b2_hits));
		values[12] = Int64GetDatum(pg_atomic_read_u64(&ctl->stat_misses));
		values[13] = Int64GetDatum(pg_atomic_read_u64(&ctl->stat_t1_evictions));
		values[14] = Int64GetDatum(pg_atomic_read_u64(&ctl->stat_t2_evictions));

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
							 values, nulls);
	}

	return (Datum) 0;
}


/* ----------------------------------------------------------------
 *			ARC size recommendation advisory function
 * ----------------------------------------------------------------
 */

/*
 * pg_bp_arc_size_recommendation -- advisory function for pool sizing.
 *
 * Returns (current_size, recommended_size, ghost_pressure, hit_ratio)
 * for an ARC-managed pool.
 */
PG_FUNCTION_INFO_V1(pg_bp_arc_size_recommendation);

Datum
pg_bp_arc_size_recommendation(PG_FUNCTION_ARGS)
{
	Name		pool_name = PG_GETARG_NAME(0);
	TupleDesc	tupdesc;
	Datum		values[4];
	bool		nulls[4] = {false, false, false, false};
	HeapTuple	tuple;

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context that cannot accept type record")));

	BlessTupleDesc(tupdesc);

	for (int i = 0; i < NBufferPools; i++)
	{
		BufferPoolDesc *pool = &BufferPoolDescs[i];
		ArcControl *ctl;
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
		if (pool->bp_routine != &arc_pool_routine)
			continue;
		if (namestrcmp(&pool->bp_name, NameStr(*pool_name)) != 0)
			continue;

		{
			PoolLocalState *local = EnsurePoolAttached(pool);

			ctl = (ArcControl *) local->strategy_data;
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

		/*
		 * Simple heuristic: if ghost_pressure > 0.1, the pool is probably too
		 * small.  Recommend increasing by the ghost pressure ratio, capped at
		 * 2x current size.
		 */
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

	/* Pool not found */
	ereport(ERROR,
			(errcode(ERRCODE_UNDEFINED_OBJECT),
			 errmsg("ARC pool \"%s\" not found", NameStr(*pool_name))));
	PG_RETURN_NULL();
}

/*
 * _PG_init -- register ARC for use as the DEFAULT pool algorithm.
 *
 * Called when the extension is loaded via shared_preload_libraries.
 */
void
_PG_init(void)
{
	RegisterDefaultPoolAlgorithm("arc", &arc_pool_routine);
}
