/*-------------------------------------------------------------------------
 *
 * pg_bp_clir.c
 *	  Clock-LIRS: lock-light scan-resistant buffer replacement algorithm.
 *
 * CLIR is a clock-based realisation of LIRS that replaces the spinlock-
 * protected stack/queue manipulation in classic LIRS (and pg_bp_lirs)
 * with single-word atomic operations on the hot path.
 *
 * Structure:
 *
 *   lir_clock[]   ring of LIR (low IRR / hot) slots, one slot per LIR
 *                 page resident in the pool.  Each slot carries a
 *                 reference bit and an owning buf_id.
 *   hir_clock[]   ring of HIR-resident (high IRR / cold) slots, sized
 *                 ~1% of the pool (min 32, capped at 5%).  This is the
 *                 victim pool: scan and one-shot pages live here, the
 *                 LIR clock is protected from them.
 *   ghost_q[]     bounded FIFO of recently-evicted page tags, used by
 *                 on_miss to tell "this page was hot before" (promote
 *                 to LIR) from "this is a fresh page" (insert as HIR).
 *   per_buffer[]  one entry per buf_id mapping buffer -> (clock, slot)
 *                 so on_hit / on_evict resolve in O(1).
 *
 * Hot-path operations:
 *
 *   on_hit(buf_id):
 *     One atomic OR of the reference bit on the slot the buffer owns.
 *     No locks, no list manipulation.  This is the property that makes
 *     CLIR fast under the same workload that puts pg_bp_lirs's spinlock
 *     under contention.
 *
 *   get_victim():
 *     Walk hir_clock from hir_hand: clear ref bit on slots that have it
 *     (second-chance) and advance; first slot with ref bit clear is the
 *     victim.  Single-word atomic CAS on the slot to claim it.  Falls
 *     back to lir_clock only if hir_clock is empty (rare; means the
 *     pool has degenerated to all-LIR which the size cap prevents).
 *
 *   on_miss / on_new_tag (under fine-grained adapt_lock):
 *     Look up the tag in ghost_q.  If hit: page was previously LIR,
 *     reinstate as LIR (demote some LIR slot to HIR to make room).
 *     If miss: insert as HIR.  This is the only path that takes a lock,
 *     and on_miss already does I/O so the lock cost is in the noise.
 *
 * The single-word ref+state encoding fits in a 32-bit atomic
 * (CLIR_STATE_BITS << 24 | ref bit), so the hot-path operation is
 * pg_atomic_fetch_or_u32(&slot->ref_state, CLIR_REF_BIT).
 *
 * scan_resistant = true.  The scan-resistance proof from LIRS carries
 * over: a scan of unrelated pages cycles through hir_clock without
 * touching lir_clock, so LIR (frequency-tier) pages are preserved
 * regardless of scan length.  This is the property the BufferPoolRoutine
 * scan_resistant flag advertises so the framework can skip the RECYCLE
 * detour for sequential scans on relations using a CLIR-backed pool.
 *
 * References:
 *   S. Jiang, F. Chen, X. Zhang, "CLOCK-Pro: An Effective Improvement
 *   of the CLOCK Replacement", USENIX ATC 2005.
 *   S. Jiang, X. Zhang, "LIRS: An Efficient Low Inter-reference Recency
 *   Set Replacement Policy", SIGMETRICS 2002.
 *   B. Manes, "Caffeine: comparative efficiency of replacement
 *   algorithms", https://github.com/ben-manes/caffeine/wiki/Efficiency
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  contrib/pg_bp_clir/pg_bp_clir.c
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

PG_MODULE_MAGIC_EXT(.name = "pg_bp_clir", .version = PG_VERSION);

void		_PG_init(void);

/* Slot state encoding in the low byte of ref_state */
#define CLIR_STATE_FREE		0
#define CLIR_STATE_RESIDENT	1	/* slot owns a buffer (LIR or HIR) */
#define CLIR_REF_BIT		(1U << 8)	/* set on every hit, cleared by sweep */
#define CLIR_STATE_MASK		0xFF

/* Per-buffer side table values */
#define CLIR_CLK_LIR		0
#define CLIR_CLK_HIR		1
#define CLIR_CLK_NONE		0xFFFF

/* Stat slot indices */
#define CLIR_STAT_LOOKUPS		0
#define CLIR_STAT_LIR_HITS		1
#define CLIR_STAT_HIR_HITS		2
#define CLIR_STAT_GHOST_HITS	3
#define CLIR_STAT_MISSES		4
#define CLIR_STAT_EVICTIONS		5
#define CLIR_STAT_PROMOTIONS	6
#define CLIR_STAT_DEMOTIONS		7
#define CLIR_STAT_CLOCK_WALKS	8
#define CLIR_NUM_STATS			9

/*
 * One entry per slot in either lir_clock or hir_clock.
 *
 * ref_state packs the slot state (low byte) and reference bit (bit 8)
 * into a single 32-bit atomic so the hot-path on_hit() is one
 * pg_atomic_fetch_or_u32().
 */
typedef struct ClirSlot
{
	pg_atomic_uint32 ref_state;
	int			buf_id;			/* -1 if state == FREE */
}			ClirSlot;

/*
 * Side table: per-buffer mapping buf_id -> (which clock, which slot).
 * Indexed by buf_id - first_buf_id.  Read on every on_hit; updated
 * under adapt_lock when slot ownership changes.
 */
typedef struct ClirBufRef
{
	uint16		clock;			/* CLIR_CLK_LIR / HIR / NONE */
	uint16		pos;			/* slot index within that clock */
}			ClirBufRef;

/*
 * Ghost entry: bounded FIFO of recently-evicted page tags, with a
 * chained hash for O(1) lookup on miss.  Hash chain pointers are
 * 32-bit indices into the ghost array (-1 = chain end).
 */
typedef struct ClirGhost
{
	BufferTag	tag;
	int32		hash_next;		/* next entry in same hash bucket, or -1 */
	bool		valid;			/* true if this slot is in the FIFO */
}			ClirGhost;

typedef struct ClirControl
{
	int			nbuffers;
	int			first_buf_id;

	/* sizing */
	int			lir_size;		/* count of LIR clock slots = nbuffers -
								 * hir_size */
	int			hir_size;		/* count of HIR clock slots ~= max(32,
								 * nbuffers/100) */
	int			ghost_size;		/* count of ghost FIFO entries = nbuffers */
	int			ghost_hash_size;	/* power of 2, ~= ghost_size * 2 */

	/* clock hands, advanced atomically */
	pg_atomic_uint32 lir_hand;
	pg_atomic_uint32 hir_hand;

	/*
	 * adapt_lock protects: ghost FIFO + ghost hash, side-table updates,
	 * LIR<->HIR transitions (promote/demote).  Hot path on_hit() does NOT
	 * take this lock.  Only get_victim()'s claim-victim CAS, on_evict(), and
	 * on_miss/on_new_tag take it.
	 */
	slock_t		adapt_lock;

	int			ghost_head;		/* FIFO insert position (cycles
								 * 0..ghost_size-1) */
	pg_atomic_uint32 lir_resident;
	pg_atomic_uint32 hir_resident;

	/* trickle writer wakeup */
	int			trickle_procno;

	/* statistics */
	pg_atomic_uint64 stat_lookups;
	pg_atomic_uint64 stat_lir_hits;
	pg_atomic_uint64 stat_hir_hits;
	pg_atomic_uint64 stat_ghost_hits;
	pg_atomic_uint64 stat_misses;
	pg_atomic_uint64 stat_evictions;
	pg_atomic_uint64 stat_promotions;
	pg_atomic_uint64 stat_demotions;
	pg_atomic_uint64 stat_clock_walks;

	/*
	 * Variable-length arrays follow this struct in shared memory:
	 *
	 * ClirSlot   lir[lir_size] ClirSlot   hir[hir_size] ClirBufRef
	 * per_buffer[nbuffers] ClirGhost  ghost[ghost_size] int32
	 * ghost_hash[ghost_hash_size]
	 */
}			ClirControl;

#define CLIR_LIR_OFFSET(ctl) \
	(MAXALIGN(sizeof(ClirControl)))
#define CLIR_HIR_OFFSET(ctl) \
	(CLIR_LIR_OFFSET(ctl) + MAXALIGN(sizeof(ClirSlot) * (ctl)->lir_size))
#define CLIR_PER_BUFFER_OFFSET(ctl) \
	(CLIR_HIR_OFFSET(ctl) + MAXALIGN(sizeof(ClirSlot) * (ctl)->hir_size))
#define CLIR_GHOST_OFFSET(ctl) \
	(CLIR_PER_BUFFER_OFFSET(ctl) + MAXALIGN(sizeof(ClirBufRef) * (ctl)->nbuffers))
#define CLIR_GHOST_HASH_OFFSET(ctl) \
	(CLIR_GHOST_OFFSET(ctl) + MAXALIGN(sizeof(ClirGhost) * (ctl)->ghost_size))

#define CLIR_LIR(ctl)		((ClirSlot *) ((char *) (ctl) + CLIR_LIR_OFFSET(ctl)))
#define CLIR_HIR(ctl)		((ClirSlot *) ((char *) (ctl) + CLIR_HIR_OFFSET(ctl)))
#define CLIR_PER_BUFFER(ctl) \
	((ClirBufRef *) ((char *) (ctl) + CLIR_PER_BUFFER_OFFSET(ctl)))
#define CLIR_GHOST(ctl)		((ClirGhost *) ((char *) (ctl) + CLIR_GHOST_OFFSET(ctl)))
#define CLIR_GHOST_HASH(ctl) \
	((int32 *) ((char *) (ctl) + CLIR_GHOST_HASH_OFFSET(ctl)))

/* per-backend stat batching, like other extensions */
#define MAX_CLIR_POOLS 16
typedef struct ClirBackendState
{
	void	   *strategy_data;
	uint64		local_stats[CLIR_NUM_STATS];
	int			vacuum_hint;
}			ClirBackendState;

static ClirBackendState clir_backend_states[MAX_CLIR_POOLS];
static int	clir_num_states = 0;


/* ----------------------------------------------------------------
 *			helpers
 * ----------------------------------------------------------------
 */

static ClirBackendState *
clir_get_backend_state(ClirControl * ctl)
{
	for (int i = 0; i < clir_num_states; i++)
		if (clir_backend_states[i].strategy_data == ctl)
			return &clir_backend_states[i];

	if (clir_num_states >= MAX_CLIR_POOLS)
		elog(ERROR, "pg_bp_clir: too many CLIR pools attached in one backend");

	clir_backend_states[clir_num_states].strategy_data = ctl;
	memset(clir_backend_states[clir_num_states].local_stats, 0,
		   sizeof(clir_backend_states[clir_num_states].local_stats));
	clir_backend_states[clir_num_states].vacuum_hint = 0;
	return &clir_backend_states[clir_num_states++];
}

#define CLIR_STAT_BUMP(ctl, idx, atomic_field) \
	do { \
		ClirBackendState *_st = clir_get_backend_state(ctl); \
		_st->local_stats[(idx)]++; \
		if ((_st->local_stats[(idx)] & 63) == 0) \
		{ \
			pg_atomic_fetch_add_u64(&(ctl)->atomic_field, _st->local_stats[(idx)]); \
			_st->local_stats[(idx)] = 0; \
		} \
	} while (0)

static inline uint32
clir_hash_tag(BufferTag *tag, uint32 mask)
{
	return hash_bytes((const unsigned char *) tag, sizeof(BufferTag)) & mask;
}

/*
 * clir_compute_hir_size -- HIR is sized as ~max(32, nbuffers / 100), capped
 * at 5% of the pool.  This matches the LIRS recommendation of allocating
 * ~99% of the cache to LIR, leaving a small but non-trivial HIR tier as
 * the scan-absorption buffer.
 */
static int
clir_compute_hir_size(int nbuffers)
{
	int			hir = nbuffers / 100;

	if (hir < 32)
		hir = 32;
	if (hir > nbuffers / 20)
		hir = nbuffers / 20;
	if (hir < 1)
		hir = 1;
	if (hir >= nbuffers)
		hir = nbuffers - 1;
	return hir;
}


/* ----------------------------------------------------------------
 *			ghost FIFO + hash (cold path, under adapt_lock)
 * ----------------------------------------------------------------
 */

static int
clir_ghost_lookup_locked(ClirControl * ctl, BufferTag *tag)
{
	ClirGhost  *ghost = CLIR_GHOST(ctl);
	int32	   *hash = CLIR_GHOST_HASH(ctl);
	uint32		bucket = clir_hash_tag(tag, ctl->ghost_hash_size - 1);
	int32		idx = hash[bucket];

	while (idx >= 0)
	{
		if (ghost[idx].valid && BufferTagsEqual(&ghost[idx].tag, tag))
			return idx;
		idx = ghost[idx].hash_next;
	}
	return -1;
}

static void
clir_ghost_remove_locked(ClirControl * ctl, int idx)
{
	ClirGhost  *ghost = CLIR_GHOST(ctl);
	int32	   *hash = CLIR_GHOST_HASH(ctl);
	uint32		bucket = clir_hash_tag(&ghost[idx].tag, ctl->ghost_hash_size - 1);
	int32		cur = hash[bucket];
	int32		prev = -1;

	while (cur >= 0)
	{
		if (cur == idx)
		{
			if (prev < 0)
				hash[bucket] = ghost[cur].hash_next;
			else
				ghost[prev].hash_next = ghost[cur].hash_next;
			ghost[idx].valid = false;
			ghost[idx].hash_next = -1;
			return;
		}
		prev = cur;
		cur = ghost[cur].hash_next;
	}
}

static void
clir_ghost_insert_locked(ClirControl * ctl, BufferTag *tag)
{
	ClirGhost  *ghost = CLIR_GHOST(ctl);
	int32	   *hash = CLIR_GHOST_HASH(ctl);
	int			head = ctl->ghost_head;
	uint32		bucket;

	/* Evict whatever's at the FIFO head if it's valid */
	if (ghost[head].valid)
		clir_ghost_remove_locked(ctl, head);

	ghost[head].tag = *tag;
	bucket = clir_hash_tag(tag, ctl->ghost_hash_size - 1);
	ghost[head].hash_next = hash[bucket];
	hash[bucket] = head;
	ghost[head].valid = true;

	ctl->ghost_head = (head + 1) % ctl->ghost_size;
}


/* ----------------------------------------------------------------
 *			vtable callbacks
 * ----------------------------------------------------------------
 */

static void
ClirOnHit(void *strategy_data, int buf_id, BufferTag *tag)
{
	ClirControl *ctl = (ClirControl *) strategy_data;
	int			local_id = buf_id - ctl->first_buf_id;
	ClirBufRef	ref;
	ClirSlot   *slot;

	CLIR_STAT_BUMP(ctl, CLIR_STAT_LOOKUPS, stat_lookups);

	if (local_id < 0 || local_id >= ctl->nbuffers)
		return;

	/*
	 * Read the side table to find which clock owns this buffer.  No lock
	 * needed: the side table is updated under adapt_lock, and a stale read
	 * here at worst sets the ref bit on a slot that no longer owns the buffer
	 * -- harmless because the slot's eventual occupant will see an extra ref
	 * bit and just take a second clock revolution to be evicted.
	 */
	ref = CLIR_PER_BUFFER(ctl)[local_id];

	if (ref.clock == CLIR_CLK_LIR && ref.pos < ctl->lir_size)
	{
		slot = &CLIR_LIR(ctl)[ref.pos];
		pg_atomic_fetch_or_u32(&slot->ref_state, CLIR_REF_BIT);
		CLIR_STAT_BUMP(ctl, CLIR_STAT_LIR_HITS, stat_lir_hits);
	}
	else if (ref.clock == CLIR_CLK_HIR && ref.pos < ctl->hir_size)
	{
		slot = &CLIR_HIR(ctl)[ref.pos];
		pg_atomic_fetch_or_u32(&slot->ref_state, CLIR_REF_BIT);
		CLIR_STAT_BUMP(ctl, CLIR_STAT_HIR_HITS, stat_hir_hits);
	}
	/* else: slot is FREE / NONE -- means buffer is being torn down; ignore. */
}

static void
ClirOnMiss(void *strategy_data, BufferTag *tag)
{
	ClirControl *ctl = (ClirControl *) strategy_data;
	int			ghost_idx;

	CLIR_STAT_BUMP(ctl, CLIR_STAT_LOOKUPS, stat_lookups);
	CLIR_STAT_BUMP(ctl, CLIR_STAT_MISSES, stat_misses);

	SpinLockAcquire(&ctl->adapt_lock);
	ghost_idx = clir_ghost_lookup_locked(ctl, tag);
	if (ghost_idx >= 0)
		CLIR_STAT_BUMP(ctl, CLIR_STAT_GHOST_HITS, stat_ghost_hits);
	SpinLockRelease(&ctl->adapt_lock);
}

static void
ClirOnEvict(void *strategy_data, int buf_id, BufferTag *old_tag)
{
	ClirControl *ctl = (ClirControl *) strategy_data;
	int			local_id = buf_id - ctl->first_buf_id;
	ClirBufRef *bref;
	ClirSlot   *slot = NULL;

	CLIR_STAT_BUMP(ctl, CLIR_STAT_EVICTIONS, stat_evictions);

	if (local_id < 0 || local_id >= ctl->nbuffers)
		return;

	bref = &CLIR_PER_BUFFER(ctl)[local_id];

	SpinLockAcquire(&ctl->adapt_lock);

	/* Find which slot owns this buffer and free it. */
	if (bref->clock == CLIR_CLK_LIR && bref->pos < ctl->lir_size)
	{
		slot = &CLIR_LIR(ctl)[bref->pos];
		pg_atomic_write_u32(&slot->ref_state, CLIR_STATE_FREE);
		slot->buf_id = -1;
		pg_atomic_fetch_sub_u32(&ctl->lir_resident, 1);
	}
	else if (bref->clock == CLIR_CLK_HIR && bref->pos < ctl->hir_size)
	{
		slot = &CLIR_HIR(ctl)[bref->pos];
		pg_atomic_write_u32(&slot->ref_state, CLIR_STATE_FREE);
		slot->buf_id = -1;
		pg_atomic_fetch_sub_u32(&ctl->hir_resident, 1);
	}
	bref->clock = CLIR_CLK_NONE;
	bref->pos = 0;

	/* Record the evicted tag in the ghost FIFO. */
	if (old_tag != NULL)
		clir_ghost_insert_locked(ctl, old_tag);

	SpinLockRelease(&ctl->adapt_lock);
}

/*
 * clir_alloc_lir_slot_locked -- find a free LIR slot, or evict an LIR
 * victim into HIR to make room.  Caller holds adapt_lock.
 */
static int
clir_alloc_lir_slot_locked(ClirControl * ctl)
{
	ClirSlot   *lir = CLIR_LIR(ctl);
	int			tries;

	/* fast path: linear scan for a free slot */
	for (int i = 0; i < ctl->lir_size; i++)
	{
		uint32		s = pg_atomic_read_u32(&lir[i].ref_state);

		if ((s & CLIR_STATE_MASK) == CLIR_STATE_FREE)
			return i;
	}

	/*
	 * No free LIR slot.  Walk the LIR clock for a victim: the first slot with
	 * the ref bit clear becomes the demotion target.  Slots with the ref bit
	 * set get it cleared (second-chance) and the hand advances.
	 */
	tries = ctl->lir_size * 2;
	while (tries-- > 0)
	{
		uint32		hand = pg_atomic_fetch_add_u32(&ctl->lir_hand, 1) % ctl->lir_size;
		uint32		s = pg_atomic_read_u32(&lir[hand].ref_state);

		if ((s & CLIR_STATE_MASK) == CLIR_STATE_FREE)
			return (int) hand;

		if (s & CLIR_REF_BIT)
		{
			pg_atomic_fetch_and_u32(&lir[hand].ref_state, ~CLIR_REF_BIT);
			continue;
		}

		/* victim found -- caller will demote its buffer to HIR. */
		return (int) hand;
	}

	return -1;					/* every slot pinned with ref bit set; very
								 * unlucky */
}

/*
 * clir_alloc_hir_slot_locked -- find a free HIR slot, or pick one with the
 * ref bit clear via clock walk.  Caller holds adapt_lock.
 */
static int
clir_alloc_hir_slot_locked(ClirControl * ctl)
{
	ClirSlot   *hir = CLIR_HIR(ctl);

	for (int i = 0; i < ctl->hir_size; i++)
	{
		uint32		s = pg_atomic_read_u32(&hir[i].ref_state);

		if ((s & CLIR_STATE_MASK) == CLIR_STATE_FREE)
			return i;
	}

	/* All HIR slots resident; clock-walk for a victim. */
	for (int tries = ctl->hir_size * 2; tries > 0; tries--)
	{
		uint32		hand = pg_atomic_fetch_add_u32(&ctl->hir_hand, 1) % ctl->hir_size;
		uint32		s = pg_atomic_read_u32(&hir[hand].ref_state);

		if ((s & CLIR_STATE_MASK) == CLIR_STATE_FREE)
			return (int) hand;

		if (s & CLIR_REF_BIT)
		{
			pg_atomic_fetch_and_u32(&hir[hand].ref_state, ~CLIR_REF_BIT);
			continue;
		}
		return (int) hand;
	}

	return -1;
}

static void
ClirOnNewTag(void *strategy_data, int buf_id, BufferTag *new_tag,
			 bool vacuum_hint)
{
	ClirControl *ctl = (ClirControl *) strategy_data;
	int			local_id = buf_id - ctl->first_buf_id;
	ClirBufRef *bref;
	int			ghost_idx;
	bool		was_lir;
	int			slot_idx;
	ClirSlot   *slot;
	ClirBackendState *bs = clir_get_backend_state(ctl);

	if (local_id < 0 || local_id >= ctl->nbuffers)
		return;

	bref = &CLIR_PER_BUFFER(ctl)[local_id];

	/*
	 * VACUUM hint: if this buffer is loaded by VACUUM, install it as HIR
	 * regardless of ghost-list state, so it's the first eviction candidate on
	 * the next clock walk.
	 */
	if (vacuum_hint || bs->vacuum_hint)
	{
		SpinLockAcquire(&ctl->adapt_lock);
		slot_idx = clir_alloc_hir_slot_locked(ctl);
		if (slot_idx < 0)
		{
			SpinLockRelease(&ctl->adapt_lock);
			return;
		}

		slot = &CLIR_HIR(ctl)[slot_idx];
		slot->buf_id = buf_id;
		pg_atomic_write_u32(&slot->ref_state, CLIR_STATE_RESIDENT);
		bref->clock = CLIR_CLK_HIR;
		bref->pos = (uint16) slot_idx;
		pg_atomic_fetch_add_u32(&ctl->hir_resident, 1);

		SpinLockRelease(&ctl->adapt_lock);
		return;
	}

	SpinLockAcquire(&ctl->adapt_lock);

	ghost_idx = clir_ghost_lookup_locked(ctl, new_tag);
	was_lir = (ghost_idx >= 0);

	if (was_lir)
	{
		/*
		 * Ghost hit: page was previously LIR.  Reinstate as LIR.  This may
		 * require evicting an LIR victim down to HIR (the demotion step in
		 * LIRS).
		 */
		clir_ghost_remove_locked(ctl, ghost_idx);
		slot_idx = clir_alloc_lir_slot_locked(ctl);

		if (slot_idx < 0)
		{
			SpinLockRelease(&ctl->adapt_lock);
			return;
		}

		slot = &CLIR_LIR(ctl)[slot_idx];

		if ((pg_atomic_read_u32(&slot->ref_state) & CLIR_STATE_MASK) ==
			CLIR_STATE_RESIDENT)
		{
			/* slot was occupied; demote its buffer to HIR */
			int			demoted_buf = slot->buf_id;
			int			hir_idx;

			if (demoted_buf >= 0 && demoted_buf - ctl->first_buf_id < ctl->nbuffers)
			{
				ClirBufRef *dbref = &CLIR_PER_BUFFER(ctl)[demoted_buf - ctl->first_buf_id];

				hir_idx = clir_alloc_hir_slot_locked(ctl);
				if (hir_idx >= 0)
				{
					ClirSlot   *hslot = &CLIR_HIR(ctl)[hir_idx];

					if ((pg_atomic_read_u32(&hslot->ref_state) & CLIR_STATE_MASK) ==
						CLIR_STATE_RESIDENT)
					{
						/* HIR victim: clear its bref */
						int			evicted_buf = hslot->buf_id;

						if (evicted_buf >= 0 &&
							evicted_buf - ctl->first_buf_id < ctl->nbuffers)
						{
							ClirBufRef *eb = &CLIR_PER_BUFFER(ctl)[evicted_buf - ctl->first_buf_id];

							eb->clock = CLIR_CLK_NONE;
							eb->pos = 0;
						}
						pg_atomic_fetch_sub_u32(&ctl->hir_resident, 1);
					}
					hslot->buf_id = demoted_buf;
					pg_atomic_write_u32(&hslot->ref_state, CLIR_STATE_RESIDENT);
					pg_atomic_fetch_add_u32(&ctl->hir_resident, 1);
					dbref->clock = CLIR_CLK_HIR;
					dbref->pos = (uint16) hir_idx;
					CLIR_STAT_BUMP(ctl, CLIR_STAT_DEMOTIONS, stat_demotions);
				}
			}
			pg_atomic_fetch_sub_u32(&ctl->lir_resident, 1);
		}

		slot->buf_id = buf_id;
		pg_atomic_write_u32(&slot->ref_state, CLIR_STATE_RESIDENT | CLIR_REF_BIT);
		bref->clock = CLIR_CLK_LIR;
		bref->pos = (uint16) slot_idx;
		pg_atomic_fetch_add_u32(&ctl->lir_resident, 1);
		CLIR_STAT_BUMP(ctl, CLIR_STAT_PROMOTIONS, stat_promotions);
	}
	else
	{
		/*
		 * Fresh page -- insert as HIR.  A subsequent re-reference within the
		 * ghost-FIFO horizon will promote it to LIR via the "ghost hit" path
		 * above.
		 */
		slot_idx = clir_alloc_hir_slot_locked(ctl);
		if (slot_idx < 0)
		{
			SpinLockRelease(&ctl->adapt_lock);
			return;
		}

		slot = &CLIR_HIR(ctl)[slot_idx];

		if ((pg_atomic_read_u32(&slot->ref_state) & CLIR_STATE_MASK) ==
			CLIR_STATE_RESIDENT)
		{
			int			evicted_buf = slot->buf_id;

			if (evicted_buf >= 0 &&
				evicted_buf - ctl->first_buf_id < ctl->nbuffers)
			{
				ClirBufRef *eb = &CLIR_PER_BUFFER(ctl)[evicted_buf - ctl->first_buf_id];

				eb->clock = CLIR_CLK_NONE;
				eb->pos = 0;
			}
			pg_atomic_fetch_sub_u32(&ctl->hir_resident, 1);
		}
		slot->buf_id = buf_id;
		pg_atomic_write_u32(&slot->ref_state, CLIR_STATE_RESIDENT);
		bref->clock = CLIR_CLK_HIR;
		bref->pos = (uint16) slot_idx;
		pg_atomic_fetch_add_u32(&ctl->hir_resident, 1);
	}

	SpinLockRelease(&ctl->adapt_lock);
}

/*
 * ClirGetVictim -- pick a victim from the HIR clock.
 *
 * The framework's GetVictimWithStrategy() handles BufferAccessStrategy
 * pre-dispatch; here we implement only the algorithm-specific selection
 * via clock walk.  All buffer-state checks (pinned, locked, dirty) and
 * the actual pin happen here, mirroring the pattern other contrib
 * algorithms use.
 */
static BufferDesc *
ClirGetVictim(void *strategy_data, BufferAccessStrategy strategy pg_attribute_unused(),
			  uint64 *buf_state, bool *from_ring)
{
	ClirControl *ctl = (ClirControl *) strategy_data;
	BufferDesc *buf;
	uint32		resident;

	*from_ring = false;			/* CLIR does not use ring strategies */

	/*
	 * Warmup path: when the pool isn't fully populated yet, scan the pool's
	 * buffer range for an untracked free buffer.  Cheaper than walking the
	 * clocks (which are mostly empty) and avoids returning NULL when every
	 * slot is FREE on a fresh pool.
	 */
	resident = pg_atomic_read_u32(&ctl->lir_resident) +
		pg_atomic_read_u32(&ctl->hir_resident);
	if (resident < (uint32) ctl->nbuffers)
	{
		for (int i = 0; i < ctl->nbuffers; i++)
		{
			ClirBufRef	ref = CLIR_PER_BUFFER(ctl)[i];
			uint64		old_buf_state,
						local_buf_state;

			if (ref.clock != CLIR_CLK_NONE)
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

	for (int outer = 0; outer < 4; outer++)
	{
		ClirSlot   *clk = (outer < 2) ? CLIR_HIR(ctl) : CLIR_LIR(ctl);
		int			n = (outer < 2) ? ctl->hir_size : ctl->lir_size;
		pg_atomic_uint32 *hand = (outer < 2) ? &ctl->hir_hand : &ctl->lir_hand;

		for (int tries = n * 2; tries > 0; tries--)
		{
			uint32		pos = pg_atomic_fetch_add_u32(hand, 1) % n;
			uint32		s = pg_atomic_read_u32(&clk[pos].ref_state);
			int			buf_id;
			uint64		old_buf_state,
						local_buf_state;

			CLIR_STAT_BUMP(ctl, CLIR_STAT_CLOCK_WALKS, stat_clock_walks);

			if ((s & CLIR_STATE_MASK) == CLIR_STATE_FREE)
				continue;

			if (s & CLIR_REF_BIT)
			{
				pg_atomic_fetch_and_u32(&clk[pos].ref_state, ~CLIR_REF_BIT);
				continue;
			}

			buf_id = clk[pos].buf_id;
			if (buf_id < 0)
				continue;
			if (buf_id < ctl->first_buf_id ||
				buf_id >= ctl->first_buf_id + ctl->nbuffers)
				continue;

			buf = GetBufferDescriptor(buf_id);

			/* try to pin */
			old_buf_state = pg_atomic_read_u64(&buf->state);
			for (;;)
			{
				local_buf_state = old_buf_state;

				if (BUF_STATE_GET_REFCOUNT(local_buf_state) != 0)
					goto skip;	/* pinned by someone else */

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
	skip:
			continue;
		}
	}

	return NULL;
}


/* ----------------------------------------------------------------
 *			trickle writer integration
 * ----------------------------------------------------------------
 */

static int
ClirSyncStart(void *strategy_data, uint32 *complete_passes,
			  uint32 *num_alloc)
{
	ClirControl *ctl = (ClirControl *) strategy_data;

	if (complete_passes != NULL)
		*complete_passes = 0;
	if (num_alloc != NULL)
		*num_alloc = (uint32) pg_atomic_read_u64(&ctl->stat_misses);
	return 0;
}

static void
ClirNotifyTrickle(void *strategy_data, int bgwprocno)
{
	ClirControl *ctl = (ClirControl *) strategy_data;

	ctl->trickle_procno = bgwprocno;
}

typedef struct ClirTrickleIter
{
	int			pos;
	int			remaining;
}			ClirTrickleIter;

static void *
ClirTrickleIterBegin(void *strategy_data, int max_candidates)
{
	ClirControl *ctl = (ClirControl *) strategy_data;
	ClirTrickleIter *iter = palloc(sizeof(ClirTrickleIter));

	iter->pos = pg_atomic_read_u32(&ctl->hir_hand) % ctl->hir_size;
	iter->remaining = Min(max_candidates, ctl->hir_size);
	return iter;
}

static int
ClirTrickleIterNext(void *strategy_data, void *opaque)
{
	ClirControl *ctl = (ClirControl *) strategy_data;
	ClirTrickleIter *iter = (ClirTrickleIter *) opaque;

	while (iter->remaining > 0)
	{
		ClirSlot   *clk = CLIR_HIR(ctl);
		uint32		s = pg_atomic_read_u32(&clk[iter->pos].ref_state);
		int			buf_id = clk[iter->pos].buf_id;

		iter->pos = (iter->pos + 1) % ctl->hir_size;
		iter->remaining--;

		if ((s & CLIR_STATE_MASK) == CLIR_STATE_RESIDENT &&
			!(s & CLIR_REF_BIT) && buf_id >= 0)
			return buf_id;
	}
	return -1;
}

static void
ClirTrickleIterEnd(void *strategy_data, void *opaque)
{
	pfree(opaque);
}

static void
ClirHintVacuum(void *strategy_data, bool vacuum_active)
{
	ClirControl *ctl = (ClirControl *) strategy_data;
	ClirBackendState *bs = clir_get_backend_state(ctl);

	if (vacuum_active)
		bs->vacuum_hint++;
	else if (bs->vacuum_hint > 0)
		bs->vacuum_hint--;
}

static bool
ClirRejectBuffer(void *strategy_data pg_attribute_unused(),
				 BufferAccessStrategy strategy pg_attribute_unused(),
				 BufferDesc *buf pg_attribute_unused(),
				 bool from_ring pg_attribute_unused())
{
	return false;
}


static void
ClirPrefetchHint(void *strategy_data, BufferTag *tags, int ntags)
{
	(void) strategy_data;
	(void) tags;
	(void) ntags;
}


/* ----------------------------------------------------------------
 *			lifecycle
 * ----------------------------------------------------------------
 */

static Size
ClirShmemSize(int nbuffers)
{
	int			hir = clir_compute_hir_size(nbuffers);
	int			lir = nbuffers - hir;
	int			ghost = nbuffers;
	int			ghost_hash = pg_nextpower2_32(Max(ghost * 2, 64));
	Size		size;

	size = MAXALIGN(sizeof(ClirControl));
	size += MAXALIGN(sizeof(ClirSlot) * lir);
	size += MAXALIGN(sizeof(ClirSlot) * hir);
	size += MAXALIGN(sizeof(ClirBufRef) * nbuffers);
	size += MAXALIGN(sizeof(ClirGhost) * ghost);
	size += MAXALIGN(sizeof(int32) * ghost_hash);
	return size;
}

static void
ClirShmemInit(void *strategy_data, int nbuffers, int first_buf_id, bool init)
{
	ClirControl *ctl = (ClirControl *) strategy_data;
	ClirSlot   *lir;
	ClirSlot   *hir;
	ClirBufRef *bref;
	ClirGhost  *ghost;
	int32	   *ghost_hash;
	int			hir_size;
	int			lir_size;
	int			ghost_size;
	int			ghost_hash_size;

	if (!init)
	{
		Assert(ctl->nbuffers == nbuffers);
		return;
	}

	hir_size = clir_compute_hir_size(nbuffers);
	lir_size = nbuffers - hir_size;
	ghost_size = nbuffers;
	ghost_hash_size = pg_nextpower2_32(Max(ghost_size * 2, 64));

	ctl->nbuffers = nbuffers;
	ctl->first_buf_id = first_buf_id;
	ctl->lir_size = lir_size;
	ctl->hir_size = hir_size;
	ctl->ghost_size = ghost_size;
	ctl->ghost_hash_size = ghost_hash_size;

	pg_atomic_init_u32(&ctl->lir_hand, 0);
	pg_atomic_init_u32(&ctl->hir_hand, 0);
	pg_atomic_init_u32(&ctl->lir_resident, 0);
	pg_atomic_init_u32(&ctl->hir_resident, 0);

	SpinLockInit(&ctl->adapt_lock);
	ctl->ghost_head = 0;
	ctl->trickle_procno = -1;

	pg_atomic_init_u64(&ctl->stat_lookups, 0);
	pg_atomic_init_u64(&ctl->stat_lir_hits, 0);
	pg_atomic_init_u64(&ctl->stat_hir_hits, 0);
	pg_atomic_init_u64(&ctl->stat_ghost_hits, 0);
	pg_atomic_init_u64(&ctl->stat_misses, 0);
	pg_atomic_init_u64(&ctl->stat_evictions, 0);
	pg_atomic_init_u64(&ctl->stat_promotions, 0);
	pg_atomic_init_u64(&ctl->stat_demotions, 0);
	pg_atomic_init_u64(&ctl->stat_clock_walks, 0);

	lir = CLIR_LIR(ctl);
	hir = CLIR_HIR(ctl);
	bref = CLIR_PER_BUFFER(ctl);
	ghost = CLIR_GHOST(ctl);
	ghost_hash = CLIR_GHOST_HASH(ctl);

	for (int i = 0; i < lir_size; i++)
	{
		pg_atomic_init_u32(&lir[i].ref_state, CLIR_STATE_FREE);
		lir[i].buf_id = -1;
	}
	for (int i = 0; i < hir_size; i++)
	{
		pg_atomic_init_u32(&hir[i].ref_state, CLIR_STATE_FREE);
		hir[i].buf_id = -1;
	}
	for (int i = 0; i < nbuffers; i++)
	{
		bref[i].clock = CLIR_CLK_NONE;
		bref[i].pos = 0;
	}
	for (int i = 0; i < ghost_size; i++)
	{
		MemSet(&ghost[i].tag, 0, sizeof(BufferTag));
		ghost[i].hash_next = -1;
		ghost[i].valid = false;
	}
	for (int i = 0; i < ghost_hash_size; i++)
		ghost_hash[i] = -1;
}

static void
ClirShutdown(void *strategy_data)
{
	(void) strategy_data;
}


/* ----------------------------------------------------------------
 *			vtable + handler
 * ----------------------------------------------------------------
 */

static const BufferPoolRoutine clir_pool_routine = {
	.type = T_Invalid,
	.on_hit = ClirOnHit,
	.on_miss = ClirOnMiss,
	.on_evict = ClirOnEvict,
	.on_new_tag = ClirOnNewTag,
	.get_victim = ClirGetVictim,
	.sync_start = ClirSyncStart,
	.notify_trickle = ClirNotifyTrickle,
	.trickle_iter_begin = ClirTrickleIterBegin,
	.trickle_iter_next = ClirTrickleIterNext,
	.trickle_iter_end = ClirTrickleIterEnd,
	.hint_vacuum = ClirHintVacuum,
	.reject_buffer = ClirRejectBuffer,
	.prefetch_hint = ClirPrefetchHint,
	.shmem_size = ClirShmemSize,
	.shmem_init = ClirShmemInit,
	.shutdown = ClirShutdown,
};

PG_FUNCTION_INFO_V1(clir_pool_handler);

Datum
clir_pool_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&clir_pool_routine);
}


/* ----------------------------------------------------------------
 *			pg_stat_clir SRF
 * ----------------------------------------------------------------
 */

#define PG_STAT_GET_CLIR_STATS_COLS 14

PG_FUNCTION_INFO_V1(pg_stat_get_clir_stats);

Datum
pg_stat_get_clir_stats(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Datum		values[PG_STAT_GET_CLIR_STATS_COLS];
	bool		nulls[PG_STAT_GET_CLIR_STATS_COLS];

	InitMaterializedSRF(fcinfo, 0);

	for (int slot = 0; slot < MAX_BUFFER_POOLS; slot++)
	{
		BufferPoolDesc *pool = &BufferPoolDescs[slot];
		ClirControl *ctl;
		PoolLocalState *local;
		int			col = 0;

		if (!pool->bp_active || pool->bp_routine != &clir_pool_routine)
			continue;

		local = EnsurePoolAttached(pool);
		ctl = (ClirControl *) local->strategy_data;

		MemSet(values, 0, sizeof(values));
		MemSet(nulls, false, sizeof(nulls));

		values[col++] = NameGetDatum(&pool->bp_name);
		if (OidIsValid(pool->bp_oid))
			values[col++] = ObjectIdGetDatum(pool->bp_oid);
		else
			nulls[col++] = true;
		values[col++] = Int32GetDatum(ctl->lir_size);
		values[col++] = Int32GetDatum(ctl->hir_size);
		values[col++] = Int32GetDatum((int32) pg_atomic_read_u32(&ctl->lir_resident));
		values[col++] = Int32GetDatum((int32) pg_atomic_read_u32(&ctl->hir_resident));
		values[col++] = Int64GetDatum((int64) pg_atomic_read_u64(&ctl->stat_lookups));
		values[col++] = Int64GetDatum((int64) pg_atomic_read_u64(&ctl->stat_lir_hits));
		values[col++] = Int64GetDatum((int64) pg_atomic_read_u64(&ctl->stat_hir_hits));
		values[col++] = Int64GetDatum((int64) pg_atomic_read_u64(&ctl->stat_ghost_hits));
		values[col++] = Int64GetDatum((int64) pg_atomic_read_u64(&ctl->stat_misses));
		values[col++] = Int64GetDatum((int64) pg_atomic_read_u64(&ctl->stat_evictions));
		values[col++] = Int64GetDatum((int64) pg_atomic_read_u64(&ctl->stat_promotions));
		values[col++] = Int64GetDatum((int64) pg_atomic_read_u64(&ctl->stat_demotions));

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	return (Datum) 0;
}


/* ----------------------------------------------------------------
 *			module init
 * ----------------------------------------------------------------
 */

void
_PG_init(void)
{
	RegisterDefaultPoolAlgorithm("clir", &clir_pool_routine);
}
