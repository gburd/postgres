/*-------------------------------------------------------------------------
 *
 * lrlock.c
 *	  Left-right lock implementation.
 *
 * This implements the left-right concurrency primitive for PostgreSQL.
 * The algorithm maintains two copies of a data structure so that readers
 * can proceed wait-free (only an atomic epoch counter increment) while a
 * single writer mutates the other copy and periodically publishes via a
 * pointer swap.
 *
 * Algorithm overview:
 *
 * Each reader has a per-backend epoch counter (cache-line padded).
 *   - On read-begin: increment epoch (even -> odd), full fence, load pointer.
 *   - On read-end: increment epoch (odd -> even).
 *
 * The writer:
 *   1. Applies operations to the current write copy.
 *   2. On publish: waits until all reader epochs have advanced past their
 *      last-seen values (meaning all readers who had the old pointer have
 *      departed), then swaps the read/write pointers atomically.
 *   3. Replays queued operations on the now-stale copy to bring it up
 *      to date.
 *
 * Key properties:
 *   - Reader path is wait-free: no CAS, no spinlock, just increment + load.
 *   - Writer path may spin waiting for departing readers.
 *   - Only one writer at a time (enforced by spinlock).
 *   - Operations must be deterministic.
 *
 * References:
 *   - Ramalhete & Correia, "Left-Right: A Concurrency Control Technique
 *     with Wait-Free Population Oblivious Reads"
 *   - Jon Gjengset, left-right Rust crate
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/storage/lmgr/lrlock.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"
#include "port/atomics.h"
#include "port/pg_bitutils.h"
#include "storage/lrlock.h"
#include "storage/proc.h"
#include "storage/procnumber.h"
#include "storage/shmem.h"
#include "storage/spin.h"

/*
 * Default initial capacity for the operation log, in bytes.
 */
#define LRLOCK_OPLOG_INITIAL_CAPACITY	4096

/*
 * Maximum number of spin iterations before yielding in the publish loop.
 */
#define LRLOCK_SPIN_LIMIT	20

/*
 * Per-backend epoch counter and nesting depth, padded to a full cache
 * line to avoid false sharing between backends.
 *
 * 'epoch' is the atomic epoch counter read by the writer.
 * 'enters' is the nesting depth, only accessed by the owning backend
 * (like Rust's Cell<usize>) -- no atomics needed.
 */
typedef union LRLockEpoch
{
	struct
	{
		pg_atomic_uint32 epoch;
		uint32		enters;		/* nesting depth, backend-local */
	};
	char		pad[PG_CACHE_LINE_SIZE];
}			LRLockEpoch;

/*
 * A single entry in the operation log.
 *
 * Operations are stored contiguously in the oplog buffer.  Each entry
 * is preceded by an LRLockOpHeader that records its size, allowing
 * iteration during replay.
 */
typedef struct LRLockOpHeader
{
	Size		op_size;		/* size of the operation data following */
}			LRLockOpHeader;

/*
 * The left-right lock structure.
 *
 * Allocated in shared memory.  The two data copies, epoch array, and
 * operation log are allocated as part of the same shared memory region
 * (or separately, depending on the creation method).
 */
struct LRLock
{
	/* Two copies of the protected data structure */
	void	   *data[2];
	Size		data_size;

	/*
	 * Index of the current read copy (0 or 1).  Readers load this atomically
	 * after incrementing their epoch counter.  The writer toggles it during
	 * publish.
	 */
	pg_atomic_uint32 read_idx;

	/*
	 * Per-backend epoch counters.  Each backend increments its own counter on
	 * read-begin (even->odd) and read-end (odd->even). The writer reads all
	 * counters during publish to determine when all pre-swap readers have
	 * departed.
	 */
	LRLockEpoch *epochs;
	int			max_backends;

	/*
	 * Active-reader bitmask: one bit per backend slot. Bit i is set while
	 * backend i is inside a read-side critical section (between
	 * LRLockReadBegin and LRLockReadEnd at the outermost level). This allows
	 * lrlock_snapshot_epochs to skip idle backends instead of scanning all
	 * max_backends cache-line-padded epoch entries.
	 *
	 * nbitmask_words = ceil(max_backends / 64).
	 */
	pg_atomic_uint64 *active_readers_mask;
	int			nbitmask_words;

	/*
	 * Writer's snapshot of epoch values, taken at the end of each publish.
	 * Used on the next publish to detect whether each reader has advanced.
	 * Entries for inactive backends are set to 0 (even) so the wait loop can
	 * skip them quickly.
	 */
	uint32	   *last_seen_epochs;

	/*
	 * Operation log: a growable buffer of serialized operations. Each entry
	 * is an LRLockOpHeader followed by op_size bytes of operation data.
	 */
	char	   *oplog;
	Size		oplog_used;		/* bytes used in oplog */
	Size		oplog_capacity; /* total allocated bytes */
	int			oplog_count;	/* number of entries */

	/* Callbacks */
	LRLockApplyFn apply_fn;
	LRLockSyncFn sync_fn;

	/* Writer mutex: only one writer at a time */
	slock_t		writer_mutex;

	/* Has the first publish happened yet? */
	bool		first_publish_done;

	/* Diagnostic name for this lock */
	char		name[NAMEDATALEN];
};


/* ----------------------------------------------------------------
 *		Internal helpers
 * ----------------------------------------------------------------
 */

/*
 * Grow the operation log to accommodate at least 'needed' more bytes.
 */
static void
lrlock_oplog_grow(LRLock * lock, Size needed)
{
	Size		new_capacity;
	char	   *new_oplog;

	new_capacity = lock->oplog_capacity * 2;
	while (new_capacity < lock->oplog_used + needed)
		new_capacity *= 2;

	new_oplog = (char *) ShmemAlloc(new_capacity);
	if (new_oplog == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of shared memory for LRLock \"%s\" operation log",
						lock->name)));

	if (lock->oplog_used > 0)
		memcpy(new_oplog, lock->oplog, lock->oplog_used);

	/* We can't free the old oplog (shared memory), but it won't be used */
	lock->oplog = new_oplog;
	lock->oplog_capacity = new_capacity;
}

/*
 * Replay all operations in the oplog onto the given data copy.
 *
 * This iterates through the serialized operation entries and calls
 * the apply callback for each one.
 */
static void
lrlock_replay_ops(LRLock * lock, void *data, Size from_offset, Size to_offset)
{
	Size		pos = from_offset;

	while (pos < to_offset)
	{
		LRLockOpHeader *hdr = (LRLockOpHeader *) (lock->oplog + pos);
		void	   *op_data = (void *) (lock->oplog + pos + sizeof(LRLockOpHeader));

		lock->apply_fn(data, op_data, hdr->op_size);
		pos += sizeof(LRLockOpHeader) + MAXALIGN(hdr->op_size);
	}

	Assert(pos == to_offset);
}

/*
 * Wait until all readers that were active before the pointer swap have
 * departed.  A reader has "departed" if its epoch counter has changed
 * from the value we recorded in last_seen_epochs.
 *
 * The key insight: if a reader's epoch was odd (active) when we last
 * checked, and now it's different, the reader must have exited and
 * possibly re-entered -- either way, they've re-loaded the pointer and
 * are no longer using the old copy.
 *
 * lrlock_snapshot_epochs() sets last_seen_epochs[i] = 0 (even) for idle
 * backends, so even entries are skipped by the "(last & 1) == 0" check
 * without fetching the 64-byte epoch cache line.  Only active-at-snapshot
 * backends (odd last_seen_epochs) trigger the heavyweight epoch re-read.
 * This reduces cache pressure from O(max_backends x 64B) to
 * O(active_readers x 64B) per wait iteration.
 */
static void
lrlock_wait_for_readers(LRLock * lock)
{
	int			spin_count;
	bool		all_departed;
	int			i;

	spin_count = 0;
retry:
	all_departed = true;

	for (i = 0; i < lock->max_backends; i++)
	{
		uint32		last = lock->last_seen_epochs[i];
		uint32		current;

		/*
		 * If the last-seen epoch was even (or zero for idle backends set by
		 * lrlock_snapshot_epochs), skip without accessing the epoch array.
		 */
		if ((last & 1) == 0)
			continue;

		/* Reader was active (odd epoch).  Check if it has advanced. */
		current = pg_atomic_read_u32(&lock->epochs[i].epoch);

		if (current == last)
		{
			/* Reader hasn't moved yet -- must wait */
			all_departed = false;
			break;
		}

		/*
		 * Reader has advanced -- it has departed or re-entered with new
		 * pointer
		 */
	}

	if (!all_departed)
	{
		if (spin_count < LRLOCK_SPIN_LIMIT)
		{
			spin_count++;
			pg_spin_delay();
		}
		else
		{
			pg_usleep(1);		/* yield to OS scheduler */
		}
		goto retry;
	}
}

/*
 * Snapshot epoch counters into last_seen_epochs, using the active-reader
 * bitmask to skip idle backends.
 *
 * For each backend with its bitmask bit set: read and store its current
 * epoch.  For backends with bit clear (idle): store 0 (even) so the
 * wait loop skips them without fetching the 64-byte epoch cache line.
 *
 * This reduces cache pressure from O(max_backends x 64 bytes) to
 * O(active_readers x 64 bytes + max_backends/64 x 8 bytes).
 */
static void
lrlock_snapshot_epochs(LRLock * lock)
{
	int			w;
	int			base;
	int			nbits;
	uint64		word;

	/*
	 * Process each 64-backend word of the bitmask.  We zero the epoch
	 * snapshot for the entire 64-backend range first (one memset per word),
	 * then fill in only the active entries.
	 */
	for (w = 0; w < lock->nbitmask_words; w++)
	{
		base = w * 64;
		nbits = Min(64, lock->max_backends - base);

		/* Zero all entries in this word's range */
		MemSet(&lock->last_seen_epochs[base], 0, nbits * sizeof(uint32));

		/* Read bitmask word and snapshot epochs for active backends */
		word = pg_atomic_read_u64(&lock->active_readers_mask[w]);
		while (word != 0)
		{
			int			bit = pg_rightmost_one_pos64(word);
			int			i = base + bit;

			lock->last_seen_epochs[i] =
				pg_atomic_read_u32(&lock->epochs[i].epoch);
			word &= word - 1;	/* clear lowest set bit */
		}
	}
}


/* ----------------------------------------------------------------
 *		Shared memory size calculation
 * ----------------------------------------------------------------
 */

/*
 * lrlock_nbitmask_words
 *
 * Number of 64-bit words needed for the active-reader bitmask
 * covering max_backends slots.
 */
static inline int
lrlock_nbitmask_words(int max_backends)
{
	return (max_backends + 63) / 64;
}

/*
 * LRLockShmemSize
 *
 * Compute the total shared memory needed for an LRLock with the given
 * parameters.
 */
Size
LRLockShmemSize(Size data_size, int max_backends, Size oplog_capacity)
{
	Size		size;

	/* The LRLock structure itself */
	size = MAXALIGN(sizeof(LRLock));

	/* Two copies of the protected data */
	size = add_size(size, mul_size(2, MAXALIGN(data_size)));

	/* Per-backend epoch counters (cache-line padded) */
	size = add_size(size, mul_size(max_backends, sizeof(LRLockEpoch)));

	/* Active-reader bitmask: ceil(max_backends/64) uint64 words */
	size = add_size(size,
					mul_size(lrlock_nbitmask_words(max_backends),
							 sizeof(pg_atomic_uint64)));

	/* Writer's snapshot of epochs */
	size = add_size(size, mul_size(max_backends, sizeof(uint32)));

	/* Operation log */
	size = add_size(size, MAXALIGN(oplog_capacity));

	return size;
}


/* ----------------------------------------------------------------
 *		Creation and initialization
 * ----------------------------------------------------------------
 */

/*
 * LRLockCreate
 *
 * Allocate and initialize a new left-right lock in shared memory.
 * This is the simple creation path for callers that don't need to
 * embed the lock in a larger structure.
 */
LRLock *
LRLockCreate(Size data_size, LRLockApplyFn apply_fn,
			 LRLockSyncFn sync_fn, const char *name)
{
	LRLock	   *lock;

	lock = (LRLock *) ShmemAlloc(MAXALIGN(sizeof(LRLock)));
	if (lock == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of shared memory for LRLock \"%s\"", name)));

	LRLockInit(lock, data_size, apply_fn, sync_fn,
			   MaxBackends + NUM_AUXILIARY_PROCS, name);

	return lock;
}

/*
 * LRLockInit
 *
 * Initialize an already-allocated LRLock structure.  Allocates the
 * data copies, epoch array, and operation log from shared memory.
 */
void
LRLockInit(LRLock * lock, Size data_size, LRLockApplyFn apply_fn,
		   LRLockSyncFn sync_fn, int max_backends, const char *name)
{
	int			i;

	MemSet(lock, 0, sizeof(LRLock));

	/* Store parameters */
	lock->data_size = data_size;
	lock->apply_fn = apply_fn;
	lock->sync_fn = sync_fn;
	lock->max_backends = max_backends;
	lock->first_publish_done = false;
	strlcpy(lock->name, name, NAMEDATALEN);

	/* Allocate the two data copies */
	lock->data[0] = ShmemAlloc(MAXALIGN(data_size));
	lock->data[1] = ShmemAlloc(MAXALIGN(data_size));
	if (lock->data[0] == NULL || lock->data[1] == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of shared memory for LRLock \"%s\" data copies",
						name)));
	MemSet(lock->data[0], 0, data_size);
	MemSet(lock->data[1], 0, data_size);

	/* Initialize read index to 0 (readers start on data[0]) */
	pg_atomic_init_u32(&lock->read_idx, 0);

	/* Allocate and initialize per-backend epoch counters */
	lock->epochs = (LRLockEpoch *) ShmemAlloc(
											  mul_size(max_backends, sizeof(LRLockEpoch)));
	if (lock->epochs == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of shared memory for LRLock \"%s\" epoch array",
						name)));

	for (i = 0; i < max_backends; i++)
	{
		pg_atomic_init_u32(&lock->epochs[i].epoch, 0);
		lock->epochs[i].enters = 0;
	}

	/* Allocate and zero the active-reader bitmask */
	lock->nbitmask_words = lrlock_nbitmask_words(max_backends);
	lock->active_readers_mask = (pg_atomic_uint64 *) ShmemAlloc(
																mul_size(lock->nbitmask_words, sizeof(pg_atomic_uint64)));
	if (lock->active_readers_mask == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of shared memory for LRLock \"%s\" reader bitmask",
						name)));
	for (i = 0; i < lock->nbitmask_words; i++)
		pg_atomic_init_u64(&lock->active_readers_mask[i], UINT64CONST(0));

	/* Allocate writer's epoch snapshot */
	lock->last_seen_epochs = (uint32 *) ShmemAlloc(
												   mul_size(max_backends, sizeof(uint32)));
	if (lock->last_seen_epochs == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of shared memory for LRLock \"%s\" epoch snapshot",
						name)));
	MemSet(lock->last_seen_epochs, 0, max_backends * sizeof(uint32));

	/* Allocate operation log */
	lock->oplog_capacity = LRLOCK_OPLOG_INITIAL_CAPACITY;
	lock->oplog = (char *) ShmemAlloc(MAXALIGN(lock->oplog_capacity));
	if (lock->oplog == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of shared memory for LRLock \"%s\" operation log",
						name)));
	lock->oplog_used = 0;
	lock->oplog_count = 0;

	/* Initialize writer mutex */
	SpinLockInit(&lock->writer_mutex);
}

/*
 * LRLockInitInPlace
 *
 * Initialize an LRLock from a contiguous pre-allocated memory block.
 * All sub-structures (data copies, epoch array, operation log) are
 * carved out of 'block' instead of calling ShmemAlloc.  The block
 * must be at least LRLockShmemSize(data_size, max_backends, oplog_capacity)
 * bytes.  The LRLock header itself occupies the start of the block.
 *
 * This is used by core subsystems that register their memory needs via
 * ShmemRequestStruct rather than relying on ShmemAlloc.
 */
LRLock *
LRLockInitInPlace(void *block, Size data_size, LRLockApplyFn apply_fn,
				  LRLockSyncFn sync_fn, int max_backends,
				  Size oplog_capacity, const char *name)
{
	LRLock	   *lock;
	char	   *ptr;
	int			i;

	ptr = (char *) block;

	/* The LRLock structure itself */
	lock = (LRLock *) ptr;
	ptr += MAXALIGN(sizeof(LRLock));

	MemSet(lock, 0, sizeof(LRLock));

	/* Store parameters */
	lock->data_size = data_size;
	lock->apply_fn = apply_fn;
	lock->sync_fn = sync_fn;
	lock->max_backends = max_backends;
	lock->first_publish_done = false;
	strlcpy(lock->name, name, NAMEDATALEN);

	/* Two data copies */
	lock->data[0] = ptr;
	ptr += MAXALIGN(data_size);
	lock->data[1] = ptr;
	ptr += MAXALIGN(data_size);
	MemSet(lock->data[0], 0, data_size);
	MemSet(lock->data[1], 0, data_size);

	/* Initialize read index */
	pg_atomic_init_u32(&lock->read_idx, 0);

	/* Per-backend epoch counters */
	lock->epochs = (LRLockEpoch *) ptr;
	ptr += mul_size(max_backends, sizeof(LRLockEpoch));
	for (i = 0; i < max_backends; i++)
	{
		pg_atomic_init_u32(&lock->epochs[i].epoch, 0);
		lock->epochs[i].enters = 0;
	}

	/* Active-reader bitmask */
	lock->nbitmask_words = lrlock_nbitmask_words(max_backends);
	lock->active_readers_mask = (pg_atomic_uint64 *) ptr;
	ptr += mul_size(lock->nbitmask_words, sizeof(pg_atomic_uint64));
	for (i = 0; i < lock->nbitmask_words; i++)
		pg_atomic_init_u64(&lock->active_readers_mask[i], UINT64CONST(0));

	/* Writer's epoch snapshot */
	lock->last_seen_epochs = (uint32 *) ptr;
	ptr += mul_size(max_backends, sizeof(uint32));
	MemSet(lock->last_seen_epochs, 0, max_backends * sizeof(uint32));

	/* Operation log */
	lock->oplog_capacity = oplog_capacity;
	lock->oplog = ptr;
	/* ptr += MAXALIGN(oplog_capacity); -- not needed, end of block */
	lock->oplog_used = 0;
	lock->oplog_count = 0;

	/* Initialize writer mutex */
	SpinLockInit(&lock->writer_mutex);

	return lock;
}


/* ----------------------------------------------------------------
 *		Reader API
 * ----------------------------------------------------------------
 */

/*
 * LRLockReadBegin
 *
 * Begin a read-side critical section.  Returns a pointer to the current
 * read copy of the data.  This pointer remains valid until LRLockReadEnd().
 *
 * Supports nested reads: if this backend already holds a read guard,
 * we skip the expensive epoch bump + SeqCst fence and just do an
 * Acquire pointer load (matching Rust left-right nested-read behavior).
 */
const void *
LRLockReadBegin(LRLock * lock)
{
	uint32		idx;
	uint32		enters;

	Assert(MyProcNumber >= 0 && MyProcNumber < lock->max_backends);

	enters = lock->epochs[MyProcNumber].enters;

	if (enters != 0)
	{
		/*
		 * Nested read -- epoch is already odd.  Just load the pointer with
		 * Acquire semantics.  No epoch bump or fence needed since the writer
		 * already knows we're active.
		 */
		idx = pg_atomic_read_acquire_u32(&lock->read_idx);
		lock->epochs[MyProcNumber].enters = enters + 1;
		return lock->data[idx];
	}

	/*
	 * First entry: full protocol.
	 *
	 * Step 1: Mark ourselves as active in the bitmask BEFORE incrementing the
	 * epoch.  This ensures that if the writer's bitmask snapshot sees bit[i]
	 * set, the epoch increment (step 2) has not yet happened, so it will
	 * observe an even epoch and wait.  Importantly, if the writer sees bit[i]
	 * clear, our epoch is also still even -- we haven't entered the critical
	 * section yet.  The release ordering ensures the bit store is visible
	 * before the epoch write in step 2.
	 */
	pg_atomic_fetch_or_u64(
						   &lock->active_readers_mask[MyProcNumber / 64],
						   (uint64) 1 << (MyProcNumber % 64));

	/*
	 * Step 2: Announce our presence by incrementing epoch to odd. AcqRel
	 * suffices here (matches Rust read.rs:169).
	 */
	pg_atomic_fetch_add_acqrel_u32(&lock->epochs[MyProcNumber].epoch, 1);

	/*
	 * Step 3: SeqCst fence.  This establishes a total ordering: the epoch
	 * increment above MUST be visible to the writer BEFORE we load the
	 * pointer below.  Without this fence, the writer could see a stale (even)
	 * epoch and swap the pointer while we're about to load it (matches Rust
	 * read.rs:173).
	 */
	pg_atomic_seq_cst_fence();

	/*
	 * Step 4: Acquire load of the current read index (matches Rust
	 * read.rs:175).  Ensures subsequent data reads see writes that preceded
	 * the pointer store.
	 */
	idx = pg_atomic_read_acquire_u32(&lock->read_idx);

	lock->epochs[MyProcNumber].enters = 1;
	return lock->data[idx];
}

/*
 * LRLockReadEnd
 *
 * End a read-side critical section.  After this call, the pointer
 * returned by LRLockReadBegin() is no longer guaranteed to be valid.
 *
 * For nested reads, only the outermost end actually releases the epoch
 * back to even.
 */
void
LRLockReadEnd(LRLock * lock)
{
	uint32		enters;

	Assert(MyProcNumber >= 0 && MyProcNumber < lock->max_backends);

	enters = lock->epochs[MyProcNumber].enters;
	Assert(enters > 0);

	enters--;
	lock->epochs[MyProcNumber].enters = enters;

	if (enters == 0)
	{
		/*
		 * Last guard dropped -- release epoch back to even.  AcqRel ensures
		 * all our reads of the data complete before the epoch becomes visible
		 * to the writer (matches Rust guard.rs:123).
		 */
		pg_atomic_fetch_add_acqrel_u32(&lock->epochs[MyProcNumber].epoch, 1);

		/*
		 * Clear the active-reader bitmask bit AFTER the epoch becomes even.
		 * This ordering is important: if the writer's bitmask snapshot sees
		 * bit[i] set, the epoch may still be odd (reader active) or may just
		 * have become even (reader exiting).  In either case the epoch check
		 * in lrlock_wait_for_readers handles it correctly.  Clearing the bit
		 * only after the epoch is even ensures that a clear bit always
		 * implies an even (inactive) epoch -- never an odd one.
		 */
		pg_atomic_fetch_and_u64(
								&lock->active_readers_mask[MyProcNumber / 64],
								~((uint64) 1 << (MyProcNumber % 64)));
	}
}


/* ----------------------------------------------------------------
 *		Writer API
 * ----------------------------------------------------------------
 */

/*
 * LRLockWriteBegin
 *
 * Acquire exclusive writer access.  Only one writer can operate at a
 * time; concurrent writers are serialized via a spinlock.
 *
 * Returns a pointer to the current write copy of the data.  The writer
 * can mutate this directly, or use LRLockApplyOp() to record
 * replayable operations.
 */
void *
LRLockWriteBegin(LRLock * lock)
{
	uint32		read_idx;

	SpinLockAcquire(&lock->writer_mutex);

	/*
	 * The write copy is whichever one readers are NOT currently using.
	 */
	read_idx = pg_atomic_read_u32(&lock->read_idx);

	return lock->data[1 - read_idx];
}

/*
 * LRLockApplyOp
 *
 * Record an operation in the operation log and apply it to the current
 * write copy immediately.
 *
 * The operation data is copied into the log.  It will be replayed on
 * the other copy during the next LRLockPublish().
 */
void
LRLockApplyOp(LRLock * lock, const void *operation, Size op_size)
{
	Size		entry_size;
	LRLockOpHeader *hdr;
	uint32		read_idx;
	void	   *write_data;

	Assert(op_size > 0);

	entry_size = sizeof(LRLockOpHeader) + MAXALIGN(op_size);

	/*
	 * Ensure the oplog has space for this entry.
	 *
	 * A single fixed-size op always fits in the initial oplog capacity, so an
	 * overflow only happens when a caller applies many ops in one write cycle
	 * (e.g. a large rollback or retained-entry cleanup).  In that case we
	 * drain the oplog by publishing in place rather than growing it: growth
	 * would call ShmemAlloc() while we hold the writer spinlock, and a failed
	 * allocation there raises ERROR with the spinlock still held, leaking it
	 * and tripping the stuck-spinlock PANIC in every other writer on this
	 * partition.  LRLockPublish() resets oplog_used to 0 with no allocation
	 * and leaves both copies in sync, so the partial batch becomes durable
	 * and the new op fits.
	 *
	 * If a single op cannot fit even an empty oplog, the oplog was sized too
	 * small for this lock's op type -- that is a configuration error, so fall
	 * back to growing (which is safe here only because the empty oplog means
	 * no prior ops are at risk if the grow fails before any
	 * spinlock-sensitive state changed).
	 */
	if (lock->oplog_used + entry_size > lock->oplog_capacity)
	{
		if (lock->oplog_used > 0)
		{
			LRLockPublish(lock);
			Assert(lock->oplog_used == 0);
		}

		if (entry_size > lock->oplog_capacity)
			lrlock_oplog_grow(lock, entry_size);
	}

	/* Write the entry */
	hdr = (LRLockOpHeader *) (lock->oplog + lock->oplog_used);
	hdr->op_size = op_size;
	memcpy(lock->oplog + lock->oplog_used + sizeof(LRLockOpHeader),
		   operation, op_size);
	lock->oplog_used += entry_size;
	lock->oplog_count++;

	/* Apply to the current write copy immediately */
	read_idx = pg_atomic_read_u32(&lock->read_idx);
	write_data = lock->data[1 - read_idx];
	lock->apply_fn(write_data, operation, op_size);
}

/*
 * LRLockPublish
 *
 * Make all operations applied since the last publish visible to readers.
 *
 * Algorithm (swap-then-wait):
 *
 *   1. For the first publish only, sync the write copy from the read copy
 *      and re-apply the oplog (the sync overwrites the writer's ops).
 *   2. Memory barrier to ensure write-copy mutations are visible.
 *   3. Atomic exchange of read_idx -- readers now see the updated copy.
 *   4. SeqCst fence -- ensures the swap is globally visible.
 *   5. Snapshot epoch counters, then wait for all snapshotted readers
 *      to depart.  After the swap, new readers go to the new read copy;
 *      only pre-swap readers may still be on the old copy.
 *   6. Bring the stale copy (old read, now new write) up to date.
 *      Use oplog replay when few ops, full sync otherwise.
 *   7. Clear the operation log.
 *
 * Invariant: after LRLockPublish returns, BOTH copies are in sync.
 * The writer can safely apply new ops to the write copy.
 *
 * The swap-before-wait approach avoids blocking new readers during the
 * wait (they immediately use the new read copy), so the wait only
 * covers stragglers from just before the swap.
 */
void
LRLockPublish(LRLock * lock)
{
	uint32		old_read_idx;
	uint32		new_read_idx;

	if (!lock->first_publish_done)
	{
		/*
		 * First publish: synchronize the write copy from the read copy, then
		 * re-apply any operations that were applied to the write copy before
		 * this publish (the sync overwrote them).
		 */
		old_read_idx = pg_atomic_read_u32(&lock->read_idx);

		lock->sync_fn(lock->data[1 - old_read_idx],
					  lock->data[old_read_idx],
					  lock->data_size);

		if (lock->oplog_used > 0)
			lrlock_replay_ops(lock, lock->data[1 - old_read_idx],
							  0, lock->oplog_used);

		lock->first_publish_done = true;
	}

	/*
	 * Step 2: Ensure write-copy mutations are visible before the swap.
	 */
	pg_memory_barrier();

	old_read_idx = pg_atomic_read_u32(&lock->read_idx);
	new_read_idx = 1 - old_read_idx;

	/*
	 * Step 3: Atomic exchange swaps the pointer.  On x86, xchg has an
	 * implicit lock prefix providing full barrier semantics.
	 */
	pg_atomic_exchange_u32(&lock->read_idx, new_read_idx);

	/*
	 * Step 4: SeqCst fence ensures the new read_idx is globally visible
	 * before we snapshot epochs.
	 */
	pg_atomic_seq_cst_fence();

	/*
	 * Step 5: Snapshot epoch counters and wait for all snapshotted readers to
	 * depart.  After the swap, new readers use the new read copy
	 * (new_read_idx).  Only pre-swap readers may still be on the old read
	 * copy (old_read_idx).  The wait also covers readers on the new copy, but
	 * reads are short-lived so the overhead is minimal.
	 */
	lrlock_snapshot_epochs(lock);
	lrlock_wait_for_readers(lock);

	/*
	 * Step 6: The old read copy (data[old_read_idx]) is now reader-free and
	 * becomes the new write copy.  Bring it up to date so the writer can
	 * safely apply new ops on top.
	 *
	 * Use oplog replay when the number of ops is small relative to the data
	 * size (O(ops) vs O(data_size)).  Fall back to full sync for large oplogs
	 * where sequential copy is faster than random-access replay.
	 *
	 * Threshold: replay if oplog_count * 256 <= data_size.
	 */
	if (lock->oplog_used > 0)
	{
		if ((Size) lock->oplog_count * 256 <= lock->data_size)
		{
			/* Few ops -- replay onto the stale copy */
			lrlock_replay_ops(lock, lock->data[old_read_idx],
							  0, lock->oplog_used);
		}
		else
		{
			/* Many ops -- full sync from the up-to-date read copy */
			lock->sync_fn(lock->data[old_read_idx],
						  lock->data[new_read_idx],
						  lock->data_size);
		}
	}

	/*
	 * Step 7: Clear the operation log.  Both copies are now in sync.
	 */
	lock->oplog_used = 0;
	lock->oplog_count = 0;
}

/*
 * LRLockPublishFullSync
 *
 * Like LRLockPublish(), but unconditionally synchronizes the stale copy
 * (the old read copy, now the new write copy) via sync_fn after the swap.
 *
 * Use this when the write copy was directly modified without LRLockApplyOp()
 * -- i.e., when the caller wrote the full current state into the write copy
 * and did not record incremental operations in the oplog.  After this call,
 * both copies hold identical up-to-date state, which is required before any
 * subsequent LRLockApplyOp() calls can safely apply incremental ops.
 *
 * Writers that always use LRLockApplyOp() should call the regular
 * LRLockPublish() instead, which uses oplog replay for efficiency.
 */
void
LRLockPublishFullSync(LRLock * lock)
{
	uint32		old_read_idx;
	uint32		new_read_idx;

	if (!lock->first_publish_done)
	{
		old_read_idx = pg_atomic_read_u32(&lock->read_idx);

		lock->sync_fn(lock->data[1 - old_read_idx],
					  lock->data[old_read_idx],
					  lock->data_size);

		if (lock->oplog_used > 0)
			lrlock_replay_ops(lock, lock->data[1 - old_read_idx],
							  0, lock->oplog_used);

		lock->first_publish_done = true;
	}

	pg_memory_barrier();

	old_read_idx = pg_atomic_read_u32(&lock->read_idx);
	new_read_idx = 1 - old_read_idx;

	pg_atomic_exchange_u32(&lock->read_idx, new_read_idx);

	pg_atomic_seq_cst_fence();

	lrlock_snapshot_epochs(lock);
	lrlock_wait_for_readers(lock);

	/*
	 * Unconditionally sync the stale copy from the current read copy. This
	 * ensures both copies are identical after the call, regardless of whether
	 * the oplog was used.
	 */
	lock->sync_fn(lock->data[old_read_idx],
				  lock->data[new_read_idx],
				  lock->data_size);

	/* Clear the operation log -- it's no longer needed. */
	lock->oplog_used = 0;
	lock->oplog_count = 0;
}

/*
 * LRLockWriteEnd
 *
 * Release writer access.  Note: any operations applied since the last
 * LRLockPublish() are NOT yet visible to readers.
 */
void
LRLockWriteEnd(LRLock * lock)
{
	SpinLockRelease(&lock->writer_mutex);
}


/* ----------------------------------------------------------------
 *		Convenience accessors
 * ----------------------------------------------------------------
 */

/*
 * LRLockGetReadData
 *
 * Return the current read-side data pointer without epoch coordination.
 * Only safe during writer access or during initialization.
 */
const void *
LRLockGetReadData(LRLock * lock)
{
	uint32		idx;

	idx = pg_atomic_read_u32(&lock->read_idx);
	return lock->data[idx];
}

/*
 * LRLockGetWriteData
 *
 * Return a mutable pointer to the write-side data.
 * Only safe during writer access (between WriteBegin/WriteEnd).
 */
void *
LRLockGetWriteData(LRLock * lock)
{
	uint32		read_idx;

	read_idx = pg_atomic_read_u32(&lock->read_idx);
	return lock->data[1 - read_idx];
}

/*
 * LRLockMarkReady
 *
 * Mark the lock as ready after the caller has directly initialized both
 * data copies.  This sets first_publish_done so the first real publish
 * won't attempt to synchronize the write copy from the read copy
 * (which would overwrite valid data with stale data).
 *
 * Only call this during initialization before any concurrent access.
 */
void
LRLockMarkReady(LRLock * lock)
{
	lock->first_publish_done = true;
}
