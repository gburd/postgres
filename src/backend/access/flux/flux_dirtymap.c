/*-------------------------------------------------------------------------
 *
 * flux_dirtymap.c
 *	  Shared-memory dirty block map for the FLUX table access method.
 *
 * This module tracks which heap pages have ever carried an in-place
 * modification whose before-image a scanner might still need from the sLog.
 * The scan path uses it as a fast-path filter: if a page's bit is CLEAR,
 * every tuple on it is plain-committed with no retained before-image, so the
 * per-tuple sLog before-image probe can be skipped for the whole page.
 *
 * Implementation:
 *   - A partitioned, open-addressed hash set of 64-bit page keys
 *     (((uint64) relid << 32) | blkno) in a fixed shared-memory buffer.
 *   - Partitioned by hash into FLUX_DIRTYMAP_PARTITIONS independent tables,
 *     each with its own writer spinlock, so concurrent Mark calls on
 *     different pages do not contend a single global lock.
 *   - FluxDirtyMapCheck (the per-scanned-tuple hot path) is LOCK-FREE: the
 *     set is grow-only (a published key is never removed or moved), so a
 *     reader linear-probing the chain sees either the key or an empty
 *     terminator, never a torn or reverted slot.  Slots are published with a
 *     single atomic store and read with a single atomic load.
 *   - FluxDirtyMapMark takes only the target partition's spinlock and does
 *     an O(1)-amortized open-addressed insert.
 *   - The map is GROW-ONLY: a key, once set, is never cleared during normal
 *     operation.  See flux_dirtymap.h for the correctness rationale.
 *   - If a partition ever fills past its load-factor ceiling, a sticky "full"
 *     flag is latched for that partition and every check on it returns dirty
 *     (safe degradation to always-probe).
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/flux/flux_dirtymap.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/flux_dirtymap.h"
#include "port/atomics.h"
#include "storage/shmem.h"
#include "storage/spin.h"

/*
 * Partition count and per-partition capacity.
 *
 * Partitioning spreads Mark-path writer-lock contention across many locks.
 * The Check path is lock-free regardless.  Capacity is a power of two so the
 * probe index is a cheap mask.  128 partitions x 8192 slots = ~1M slots x 8
 * bytes = 8 MB of shared memory; with the 0.75 load-factor ceiling that holds
 * ~786K distinct in-place-modified pages before a partition latches full.
 */
#define FLUX_DIRTYMAP_PARTITIONS	128
#define FLUX_DIRTYMAP_PART_SLOTS	8192	/* power of two */
#define FLUX_DIRTYMAP_PART_MASK	(FLUX_DIRTYMAP_PART_SLOTS - 1)
#define FLUX_DIRTYMAP_MAX_LOAD		((FLUX_DIRTYMAP_PART_SLOTS * 3) / 4)

/*
 * Empty-slot sentinel.  A key of 0 would require relid 0, which is never a
 * user relation, so 0 is safe to reserve as "empty".
 */
#define FLUX_DIRTYMAP_EMPTY_KEY	UINT64CONST(0)

/*
 * Compose the collision-free 64-bit page key.  relid is a 32-bit Oid and
 * blkno a 32-bit BlockNumber, so the key is unique per (relid, blkno).
 */
#define FLUX_DIRTYMAP_KEY(relid, blkno) \
	(((uint64) (relid) << 32) | (uint64) (blkno))

/*
 * One partition: a fixed open-addressed slot array of atomic 64-bit keys,
 * a writer spinlock, a live-entry count, and a sticky full flag.
 */
typedef struct FluxDirtyMapPartition
{
	slock_t		mutex;			/* serializes Mark inserts into this partition */
	int			nentries;		/* live keys (writer-lock protected) */
	bool		full;			/* sticky: load ceiling hit -> check == dirty */
	pg_atomic_uint64 slots[FLUX_DIRTYMAP_PART_SLOTS];
}			FluxDirtyMapPartition;

typedef struct FluxDirtyMapShared
{
	FluxDirtyMapPartition parts[FLUX_DIRTYMAP_PARTITIONS];
}			FluxDirtyMapShared;

static FluxDirtyMapShared *DirtyMap = NULL;

/*
 * Hash a 64-bit key to a partition index and an initial probe slot.  A
 * multiplicative (Fibonacci) hash mixes the high and low words so that the
 * sequential (relid, blkno) keys of a scan spread across partitions and
 * slots rather than clustering.
 */
static inline uint64
flux_dirtymap_mix(uint64 key)
{
	uint64		h = key * UINT64CONST(0x9E3779B97F4A7C15);

	return h ^ (h >> 32);
}

static inline uint32
flux_dirtymap_part(uint64 mixed)
{
	return (uint32) (mixed & (FLUX_DIRTYMAP_PARTITIONS - 1));
}

static inline uint32
flux_dirtymap_slot0(uint64 mixed)
{
	return (uint32) ((mixed >> 7) & FLUX_DIRTYMAP_PART_MASK);
}

/* ----------------------------------------------------------------
 * Shared memory initialization
 * ----------------------------------------------------------------
 */

Size
FluxDirtyMapShmemSize(void)
{
	return MAXALIGN(sizeof(FluxDirtyMapShared));
}

static void
FluxDirtyMapShmemRequest(void *arg)
{
	ShmemRequestStruct(.name = "FLUX DirtyMap",
					   .size = FluxDirtyMapShmemSize(),
					   .ptr = (void **) &DirtyMap,
		);
}

static void
FluxDirtyMapShmemInit_cb(void *arg)
{
	int			p;
	int			s;

	Assert(DirtyMap != NULL);

	for (p = 0; p < FLUX_DIRTYMAP_PARTITIONS; p++)
	{
		FluxDirtyMapPartition *part = &DirtyMap->parts[p];

		SpinLockInit(&part->mutex);
		part->nentries = 0;
		part->full = false;
		for (s = 0; s < FLUX_DIRTYMAP_PART_SLOTS; s++)
			pg_atomic_init_u64(&part->slots[s], FLUX_DIRTYMAP_EMPTY_KEY);
	}
}

void
FluxDirtyMapShmemInit(void)
{
	/* Initialization is handled by FluxDirtyMapShmemCallbacks */
}

const ShmemCallbacks FluxDirtyMapShmemCallbacks = {
	.request_fn = FluxDirtyMapShmemRequest,
	.init_fn = FluxDirtyMapShmemInit_cb,
};

/* ----------------------------------------------------------------
 * Mark and query
 * ----------------------------------------------------------------
 */

/*
 * FluxDirtyMapMark
 *		Record that (relid, blkno) carries a retained in-place modification.
 *
 * Must be called while the page's buffer is exclusively locked, before that
 * lock is released, so a concurrent scanner always observes the published
 * key.  Idempotent.  If the target partition is full, its sticky flag is
 * latched so every subsequent check on it conservatively returns dirty.
 */
void
FluxDirtyMapMark(Oid relid, BlockNumber blkno)
{
	uint64		key = FLUX_DIRTYMAP_KEY(relid, blkno);
	uint64		mixed = flux_dirtymap_mix(key);
	FluxDirtyMapPartition *part = &DirtyMap->parts[flux_dirtymap_part(mixed)];
	uint32		slot = flux_dirtymap_slot0(mixed);
	int			probes;

	SpinLockAcquire(&part->mutex);

	if (part->full)
	{
		SpinLockRelease(&part->mutex);
		return;
	}

	for (probes = 0; probes < FLUX_DIRTYMAP_PART_SLOTS; probes++)
	{
		uint64		cur = pg_atomic_read_u64(&part->slots[slot]);

		if (cur == key)
		{
			/* already present -- idempotent */
			SpinLockRelease(&part->mutex);
			return;
		}
		if (cur == FLUX_DIRTYMAP_EMPTY_KEY)
		{
			/*
			 * Publish the key with a single atomic store.  A concurrent
			 * lock-free reader on the same chain sees either the old empty
			 * (and keeps probing / terminates) or the full key -- never a
			 * torn value.  Grow-only means this slot never reverts.
			 */
			if (part->nentries >= FLUX_DIRTYMAP_MAX_LOAD)
			{
				part->full = true;
				SpinLockRelease(&part->mutex);
				return;
			}
			pg_atomic_write_u64(&part->slots[slot], key);
			part->nentries++;
			SpinLockRelease(&part->mutex);
			return;
		}
		slot = (slot + 1) & FLUX_DIRTYMAP_PART_MASK;
	}

	/* probe chain exhausted without an empty slot -> latch full */
	part->full = true;
	SpinLockRelease(&part->mutex);
}

/*
 * FluxDirtyMapCheck
 *		Return true if the page's key is present (or its partition overflowed).
 *
 * LOCK-FREE.  The set is grow-only, so a reader linear-probing the chain sees
 * either the key (dirty), an empty terminator (clean), or the partition's
 * sticky full flag (dirty).  A concurrent Mark only publishes new keys with
 * an atomic store; it never moves or clears a slot, so a reader can miss a
 * key that is being inserted *concurrently* only if the insert has not yet
 * completed -- and the mark-before-buffer-unlock ordering (see
 * FluxDirtyMapMark's contract) guarantees any modification a scanner could
 * observe was published before the scanner could reach the page.
 *
 * A false result means the page is provably clean and the scan path may skip
 * the per-tuple sLog before-image probe for the whole page.
 */
bool
FluxDirtyMapCheck(Oid relid, BlockNumber blkno)
{
	uint64		key = FLUX_DIRTYMAP_KEY(relid, blkno);
	uint64		mixed = flux_dirtymap_mix(key);
	FluxDirtyMapPartition *part = &DirtyMap->parts[flux_dirtymap_part(mixed)];
	uint32		slot = flux_dirtymap_slot0(mixed);
	int			probes;

	/*
	 * The full flag is read without the spinlock.  It is set-once (sticky)
	 * and only ever transitions false->true, so a stale-false read at worst
	 * costs one more lock-free probe pass, and a true read is permanent.
	 */
	if (part->full)
		return true;

	for (probes = 0; probes < FLUX_DIRTYMAP_PART_SLOTS; probes++)
	{
		uint64		cur = pg_atomic_read_u64(&part->slots[slot]);

		if (cur == key)
			return true;
		if (cur == FLUX_DIRTYMAP_EMPTY_KEY)
			return false;		/* provably clean */
		slot = (slot + 1) & FLUX_DIRTYMAP_PART_MASK;
	}

	/* full chain of non-matching keys: conservatively dirty */
	return true;
}
