/*-------------------------------------------------------------------------
 *
 * slog.c
 *	  Secondary Log (sLog) -- radix-tree + flat-hash shared-memory tracking
 *
 * The sLog tracks aborted transactions and per-tuple operations in shared
 * memory for the UNDO subsystem.
 *
 * Transaction sLog (adaptive radix tree):
 *   - A shared-memory adaptive radix tree (radixtree.h, RT_SHMEM) keyed by
 *     (xid, reloid) packed into a uint64 stores full abort metadata.  With
 *     xid in the high bits, ordered iteration groups all entries for a
 *     given xid contiguously, enabling efficient per-xid range operations.
 *   - The tree lives in the sLog's DSA area and grows on demand.
 *   - The radix tree is protected by a single LWLock (sLog modifications
 *     only occur on transaction abort, an uncommon path).
 *
 * Tuple sLog (seqlock-protected flat hash):
 *   - Keyed by (relid, tid), stores up to SLOG_MAX_TUPLE_OPS concurrent
 *     operations per tuple.  Designed for the RECNO table AM.
 *   - Uses a flat open-addressing hash table protected by a seqlock for
 *     wait-free reads.  Writes are serialized via the per-partition
 *     writer_lock.
 *   - WAL-free: entries are transient, removed at commit/abort.
 *
 * Locking: Transaction sLog uses LWTRANCHE_SLOG.  Tuple sLog uses a
 * per-partition seqlock (wait-free reads) + per-partition writer_lock
 * (writer serialization).
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/undo/slog.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>
#ifdef WIN32
#include <windows.h>
#endif

/*
 * slog_num_cpus - portable replacement for sysconf(_SC_NPROCESSORS_ONLN).
 *
 * Returns the number of online CPUs (or a sane positive fallback if the
 * platform refuses to answer).  Used to auto-size the sLog flat-hash
 * partition count when the GUC is left at zero.
 */
static int
slog_num_cpus(void)
{
#ifdef WIN32
	SYSTEM_INFO si;

	GetSystemInfo(&si);
	if (si.dwNumberOfProcessors > 0)
		return (int) si.dwNumberOfProcessors;
	return 4;
#else
	long	n = sysconf(_SC_NPROCESSORS_ONLN);

	if (n <= 0)
		return 4;
	return (int) n;
#endif
}

#include "access/relundo.h"
#include "access/slog.h"
#include "access/slog_flathash.h"
#include "access/transam.h"
#include "access/xact.h"
#include "common/hashfn.h"
#include "nodes/lockoptions.h"
#include "miscadmin.h"
#include "storage/lock.h"
#include "storage/lwlock.h"
#include "storage/off.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "storage/subsystems.h"
#include "utils/dsa.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"

/* ----------------------------------------------------------------
 * Adaptive radix tree instantiation for the transaction sLog (ATM)
 *
 * The Aborted Transaction Map is keyed by a single uint64 formed from
 * (xid, reloid):  key = (xid << 32) | reloid.  Both are uint32 and the
 * two halves pack exactly.  Because xid occupies the high 32 bits, the
 * radix tree's ordered iteration groups all entries for one xid
 * contiguously (xid-major, reloid-minor ascending) -- the same ordering
 * property the old skip-list provided, which the per-xid range
 * operations below rely on.
 *
 * The tree lives in the sLog's existing DSA area (the one used for
 * before-images) so it grows on demand rather than reserving a fixed
 * shared-memory pool.  All access is serialized externally by the
 * existing txn_lock (LWLock): SET/DELETE under LW_EXCLUSIVE, FIND and
 * iteration under LW_SHARED.  We do not use the radix tree's own
 * internal lock (RT_LOCK_*), since txn_lock already provides the needed
 * serialization.
 * ----------------------------------------------------------------
 */

/*
 * slog_atm_value_t - value stored in the ATM radix tree.
 *
 * The key (xid, reloid) is implicit in the tree path, so only the data
 * fields are stored here.
 */
typedef struct slog_atm_value_t
{
	XLogRecPtr	last_batch_lsn; /* LSN of last UNDO batch for this xid */
	Oid			dboid;
	TimestampTz abort_time;
	bool		revert_complete;
} slog_atm_value_t;

#define RT_PREFIX slog_atm
#define RT_SCOPE static pg_attribute_unused()
#define RT_DECLARE
#define RT_DEFINE
#define RT_VALUE_TYPE slog_atm_value_t
#define RT_SHMEM
#define RT_USE_DELETE
#include "lib/radixtree.h"

/* ATM key encoding: xid in the high 32 bits, reloid in the low 32 bits. */
static inline uint64
slog_atm_key(TransactionId xid, Oid reloid)
{
	return ((uint64) xid << 32) | (uint64) reloid;
}

static inline TransactionId
slog_atm_key_xid(uint64 key)
{
	return (TransactionId) (key >> 32);
}

static inline Oid
slog_atm_key_reloid(uint64 key)
{
	return (Oid) (key & 0xFFFFFFFFu);
}

/*
 * Initial size for the DSA area used by before-image storage.
 * Grows dynamically as needed up to slog_before_image_max_mb.
 */
#define SLOG_DSA_INIT_SIZE		(512 * 1024)	/* 512 KB */
#define SLOG_DSA_MAX_SIZE_MB	256				/* default max: 256 MB */

/*
 * Maximum number of abort ops applied to a partition between seqlock
 * begin/end cycles in SLogTupleMarkAborted / SLogTupleCleanupRetained.
 *
 * The seqlock keeps its counter odd for the whole write cycle, so any
 * concurrent wait-free reader spins until the counter turns even.  A very
 * large rollback (hundreds of thousands of tuples applied in one cycle)
 * would pin readers spinning for the entire batch.  Ending and re-beginning
 * the seqlock every SLOG_ABORT_PUBLISH_BATCH ops bounds that odd-hold
 * window; the partition writer_lock is held across the whole function, so
 * dropping to even at a batch boundary leaves the partition consistent for
 * any reader that observes it.
 */
#define SLOG_ABORT_PUBLISH_BATCH	24

/*
 * Wall-clock sampling period for the self-clocking throttles in
 * SLogTupleInsert() and SLogTupleMaybeCleanupRetained().  GetCurrentTimestamp()
 * is a clock_gettime() syscall that dominates the per-tuple hot path, so we
 * read it once every SLOG_INSERT_CLOCK_PERIOD calls and reuse the cached
 * horizon in between.  Must be a power of two (callers mask with PERIOD - 1).
 */
#define SLOG_INSERT_CLOCK_PERIOD	256

/* ----------------------------------------------------------------
 * Shared state definition
 * ----------------------------------------------------------------
 */
typedef struct SLogSharedState
{
	/* Transaction ATM (adaptive radix tree in the DSA area below) */
	dsa_pointer atm_handle;		/* RT handle; InvalidDsaPointer until init */
	LWLockPadded txn_lock;		/* single LWLock serializing ATM access */

	/* Tuple flat hash: N-way partitioned for reduced writer contention.
	 * Partition count determined at startup by slog_num_partitions GUC. */
	SLogFlatPartition *tuple_partitions;	/* palloc'd array in shmem */
	int			num_partitions;				/* actual partition count */

	/* DSA area for shared before-images */
	dsa_area   *dsa_area;		/* set during SLogShmemInit, NULL until then */
	char		dsa_space[SLOG_DSA_INIT_SIZE];
} SLogSharedState;

/*
 * SLogTupleNumEntries
 *		Calculate the number of hash entries for the tuple sLog.
 *
 * Auto-sizing formula: MaxBackends * 1024, clamped to [4096, 4194304].
 */
int
SLogTupleNumEntries(void)
{
	int			n = MaxBackends * SLOG_TUPLE_PER_BACKEND_SLOTS;

	n = Max(n, SLOG_TUPLE_MIN_ENTRIES);
	n = Min(n, SLOG_TUPLE_MAX_ENTRIES);
	return n;
}

/* GUC: maximum DSA area size for before-images (in MB) */
int			slog_dsa_max_size_mb = SLOG_DSA_MAX_SIZE_MB;

/*
 * GUC: slog_num_partitions — number of flat hash partitions.
 * 0 = auto (heuristic: 4 × NumCPUs, clamped [16..256], power of 2).
 * Set at postmaster startup, immutable thereafter.
 */
int			slog_num_partitions = 0;

/* Runtime partition count (set once during SLogShmemInit, read everywhere) */
int			SLogNumPartitions = SLOG_FLAT_DEFAULT_PARTITIONS;

/*
 * Compute the effective partition count from the GUC value.
 * Called once during SLogShmemInit.
 */
static int
SLogComputeNumPartitions(void)
{
	int			n;

	if (slog_num_partitions > 0)
	{
		/* Explicit GUC value — clamp and round to power of 2 */
		n = slog_num_partitions;
	}
	else
	{
		/* Auto-size: 4 x number of CPUs */
		int		ncpus = slog_num_cpus();

		n = ncpus * 4;
	}

	/* Clamp */
	n = Max(n, SLOG_FLAT_MIN_PARTITIONS);
	n = Min(n, SLOG_FLAT_MAX_PARTITIONS);

	/* Round up to next power of 2 (for fast modulo via bitmask) */
	{
		int		p = 1;

		while (p < n)
			p <<= 1;
		n = p;
	}

	return n;
}

/*
 * Flat hash capacity: round up SLogTupleNumEntries to next power of 2,
 * then divide by 0.7 (max load factor) to ensure probe chains stay short.
 */
static inline int
SLogFlatHashCapacity(void)
{
	int			n = SLogTupleNumEntries();
	int			cap;

	/* Target: num_entries / capacity <= 0.7, so capacity >= n / 0.7 */
	cap = (int) ((double) n / 0.7) + 1;

	/* Round up to next power of 2 */
	cap--;
	cap |= cap >> 1;
	cap |= cap >> 2;
	cap |= cap >> 4;
	cap |= cap >> 8;
	cap |= cap >> 16;
	cap++;

	/* Clamp to reasonable bounds */
	if (cap < 2048)
		cap = 2048;
	if (cap > 2 * 1048576)
		cap = 2 * 1048576;

	return cap;
}

/* ----------------------------------------------------------------
 * Static variables
 * ----------------------------------------------------------------
 */
static SLogSharedState *SLogState = NULL;

/* Per-backend DSA attachment (lazy, via SLogEnsureDsaAttached) */
static dsa_area *slog_dsa_handle = NULL;

/* Per-backend attached ATM radix tree (lazy, via slog_atm_tree) */
static slog_atm_radix_tree *slog_atm_tree_local = NULL;

/* Pointers to ShmemAlloc'd regions, set by ShmemRequestStruct framework */
static char *SLogFlatHashBlock = NULL;	/* single allocation for all partition flat-hash copies */

/* Rate-limiting for sLog overflow warnings (per-backend) */
static int	slog_overflow_warning_count = 0;
static TimestampTz slog_overflow_last_warning = 0;


/* ----------------------------------------------------------------
 * Backend-private tracking for tuple sLog cleanup
 * ----------------------------------------------------------------
 */
typedef struct SLogTrackedKey
{
	SLogTupleKey key;
	TransactionId xid;
	TransactionId subxid;
	bool		local_only;		/* no shared hash entry (INSERT-only) */
	SLogOpType	op_type;		/* DML type (for commit retention decisions) */

	/* Before-image for savepoint rollback (NULL if not applicable) */
	char	   *before_image;	/* palloc'd copy of tuple data, or NULL */
	int			before_image_len;	/* length of before_image data */
	uint16		before_flags;	/* original t_flags before DML */
	uint64		before_commit_ts;	/* original t_commit_ts before DML */

	/*
	 * Physical relation locator captured at store time.  Savepoint-abort
	 * restore runs in TRANS_ABORT state where relation_open's relcache lookup
	 * is unsafe, so we read the buffer via ReadBufferWithoutRelcache instead.
	 */
	RelFileLocator before_rlocator;
	char		before_relpersistence;

	/* DSA pointer to shared before-image (for abort/rollback cleanup) */
	dsa_pointer before_image_dp;	/* InvalidDsaPointer if none */

	struct SLogTrackedKey *next;
} SLogTrackedKey;

static SLogTrackedKey *slog_tracked_keys = NULL;
static bool slog_has_shared_entries = false;	/* any non-local_only entries? */

/* ----------------------------------------------------------------
 * TID encoding for backend-local INSERT tracking
 *
 * Each (blkno, offnum) pair maps to a dense 64-bit key used by the
 * backend-local INSERT-tracking hash (see below):
 *
 *   blkno * MaxOffsetNumber + (offnum - 1)
 *
 * Losslessness invariant: this key is in-memory and per-backend only --
 * never written to WAL or disk -- so the divisor need only be strictly
 * greater than any real offnum a tracked page can hold.  MaxOffsetNumber
 * is the maximum number of line pointers any page can hold and is an
 * AM-agnostic upper bound: offnum <= MaxOffsetNumber always holds, so the
 * divisor never aliases two pages' TIDs onto the same key.  A check at each
 * encode site enforces this; an out-of-range offset is a corruption bug,
 * not a recoverable condition, so it raises ERROR even in production builds.
 * ----------------------------------------------------------------
 */
#define SLOG_ENCODE_TID(blkno, offnum) \
	((uint64) (blkno) * MaxOffsetNumber + (uint64) ((offnum) - 1))
#define SLOG_DECODE_BLKNO(encoded) \
	((BlockNumber) ((encoded) / MaxOffsetNumber))
#define SLOG_DECODE_OFFNUM(encoded) \
	((OffsetNumber) ((encoded) % MaxOffsetNumber + 1))

/* ----------------------------------------------------------------
 * Backend-local INSERT tracking (simplehash)
 *
 * Top-level local-only INSERTs are recorded in a single backend-local
 * open-addressing hash keyed by (relid, encoded_tid).  This replaces the
 * former per-relid linked-list-of-sparsemaps, which forced an O(n) chunk
 * scan on every per-tuple visibility probe.  The hash lives in
 * TopTransactionContext and is destroyed per transaction.  No locks: it is
 * strictly backend-private.
 * ----------------------------------------------------------------
 */
typedef struct SLogInsertTidKey
{
	Oid			relid;
	uint64		encoded_tid;	/* SLOG_ENCODE_TID(blkno, offnum) */
} SLogInsertTidKey;

typedef struct SLogInsertTidEntry
{
	SLogInsertTidKey key;		/* (relid, encoded_tid) */
	char		status;			/* required by simplehash */
} SLogInsertTidEntry;

static inline uint32
slog_insert_tid_hash(Oid relid, uint64 encoded_tid)
{
	uint64		h = hash_combine64((uint64) hash_uint32((uint32) relid),
								   murmurhash64(encoded_tid));

	return (uint32) (h ^ (h >> 32));
}

#define SH_PREFIX sloginsert
#define SH_ELEMENT_TYPE SLogInsertTidEntry
#define SH_KEY_TYPE SLogInsertTidKey
#define SH_KEY key
#define SH_HASH_KEY(tb, key) slog_insert_tid_hash((key).relid, (key).encoded_tid)
#define SH_EQUAL(tb, a, b) ((a).relid == (b).relid && (a).encoded_tid == (b).encoded_tid)
#define SH_SCOPE static inline
#define SH_DEFINE
#define SH_DECLARE
#include "lib/simplehash.h"

static sloginsert_hash *slog_insert_tids = NULL;

/*
 * Lazily create the backend-local INSERT hash in TopTransactionContext.
 * Returns the (now non-NULL) table.
 */
static inline sloginsert_hash *
slog_insert_tids_ensure(void)
{
	if (slog_insert_tids == NULL)
		slog_insert_tids = sloginsert_create(TopTransactionContext, 256, NULL);
	return slog_insert_tids;
}

/* ----------------------------------------------------------------
 * Internal helpers
 * ----------------------------------------------------------------
 */

/*
 * Partition accessor helpers for the tuple sLog.
 *
 * These inline functions encapsulate the key→partition routing so that
 * callers don't need to repeat the pattern.
 */
static inline SLogFlatPartition *
SLogGetPartition(const SLogTupleKey *key)
{
	int			part = SLogFlatHashPartitionIndex(key);

	return &SLogState->tuple_partitions[part];
}

static inline SLogFlatPartition *
SLogGetPartitionByIndex(int part)
{
	Assert(part >= 0 && part < SLogNumPartitions);
	return &SLogState->tuple_partitions[part];
}

/*
 * Convenience macros for partition-routed locking.
 *
 * Most functions have a local `key` variable of type SLogTupleKey and need
 * to route to the correct partition.  These macros minimize the diff from
 * the old single-lock code.  The `fp__` variable is defined in-scope by
 * SLOG_PART_READ_BEGIN / SLOG_PART_WRITE_BEGIN.
 */
#define SLOG_PART_WRITER_LOCK(key_ptr) \
	(&SLogGetPartition(key_ptr)->writer_lock.lock)

/*
 * SLogPartApplyOne
 *		Single-op write helper: acquire writer_lock, one seqlock cycle, one
 *		apply, release.  Covers the many write sites that apply exactly one
 *		SLogFlatOp to fp->hash.
 */
static inline void
SLogPartApplyOne(SLogFlatPartition * fp, const SLogFlatOp *op)
{
	LWLockAcquire(&fp->writer_lock.lock, LW_EXCLUSIVE);
	SLogSeqWriteBegin(fp);
	SLogFlatHashApply(fp->hash, op, sizeof(*op));
	SLogSeqWriteEnd(fp);
	LWLockRelease(&fp->writer_lock.lock);
}

/*
 * slog_atm_tree
 *		Return this backend's attached ATM radix tree, attaching lazily.
 *
 * Ensures the sLog DSA area is attached, then attaches to the shared
 * radix tree via its handle.  The attached wrapper is cached for the
 * life of the backend (the control block lives in the pinned DSA area).
 * The caller must hold txn_lock across any use of the returned tree.
 */
static slog_atm_radix_tree *
slog_atm_tree(void)
{
	MemoryContext oldcxt;

	if (slog_atm_tree_local != NULL)
		return slog_atm_tree_local;

	SLogEnsureDsaAttached();
	if (slog_dsa_handle == NULL)
		elog(PANIC, "sLog: DSA not attached for ATM radix tree");

	/*
	 * Attach in TopMemoryContext so the wrapper persists across
	 * transactions, mirroring the DSA attach above.
	 */
	oldcxt = MemoryContextSwitchTo(TopMemoryContext);
	slog_atm_tree_local = slog_atm_attach(slog_dsa_handle,
										  SLogState->atm_handle);
	MemoryContextSwitchTo(oldcxt);

	return slog_atm_tree_local;
}



/* ----------------------------------------------------------------
 * Shared memory sizing and initialization
 * ----------------------------------------------------------------
 */

/*
 * SLogShmemSize
 *		Calculate shared memory needed for the sLog.
 *
 * Note: The DSA initial region is embedded in SLogSharedState (dsa_space[]),
 * so sizeof(SLogSharedState) already includes SLOG_DSA_INIT_SIZE.  The ATM
 * radix tree is allocated from that DSA area on demand, so it needs no fixed
 * reservation here.
 */
Size
SLogShmemSize(void)
{
	Size		size;

	size = MAXALIGN(sizeof(SLogSharedState));

	/* Partitioned seqlock flat hashes */
	size = add_size(size, SLogFlatHashPartitionedShmemSize(SLogFlatHashCapacity(),
														  MaxBackends));

	return size;
}

/*
 * SLogShmemRequest
 *		Register shared memory needs for the sLog.
 *
 * We register the main state struct, pool regions, and tuple hash table
 * via the ShmemRequestStruct/ShmemRequestHash framework.
 */
void
SLogShmemRequest(void)
{
	/* Compute partition count early so shmem sizing is correct */
	SLogNumPartitions = SLogComputeNumPartitions();

	/* Register the shared state structure */
	ShmemRequestStruct(.name = "Secondary Log State",
					   .size = sizeof(SLogSharedState),
					   .ptr = (void **) &SLogState,
		);

	/*
	 * Additional shared memory for the tuple hash partitions.  The ATM radix
	 * tree allocates from the DSA area, so it needs no ShmemRequestStruct
	 * entry.
	 */
	{
		/* Single block for all partitioned seqlock flat hashes */
		ShmemRequestStruct(.name = "sLog Flat Hash Partitions",
						   .size = SLogFlatHashPartitionedShmemSize(
							   SLogFlatHashCapacity(), MaxBackends),
						   .ptr = (void **) &SLogFlatHashBlock,
			);
	}
}

/*
 * SLogShmemInit
 *		Initialize sLog shared memory contents.
 *
 * Called from UndoShmemInit() during the init_fn phase.  The framework
 * has already allocated SLogState.  We manually initialize the ATM radix
 * tree and seqlock flat hash.
 */
void
SLogShmemInit(void)
{
	/* ATM radix tree is created below, after the DSA area exists. */
	SLogState->atm_handle = InvalidDsaPointer;

	/* ---- Initialize locks ---- */

	LWLockInitialize(&SLogState->txn_lock.lock, LWTRANCHE_SLOG);

	/* ---- Compute and set partition count (once, globally) ---- */
	SLogNumPartitions = SLogComputeNumPartitions();
	SLogState->num_partitions = SLogNumPartitions;

	/* Allocate partition array in shared memory (after SLogState) */
	SLogState->tuple_partitions = (SLogFlatPartition *)
		ShmemAlloc(sizeof(SLogFlatPartition) * SLogNumPartitions);
	memset(SLogState->tuple_partitions, 0,
		   sizeof(SLogFlatPartition) * SLogNumPartitions);

	ereport(LOG,
			(errmsg("sLog: %d flat hash partitions (slog_num_partitions=%d, CPUs=%d)",
					SLogNumPartitions, slog_num_partitions,
					slog_num_cpus())));

	/* ---- Initialize partitioned seqlock flat hashes ---- */
	{
		int			total_capacity = SLogFlatHashCapacity();
		int			per_part_cap = total_capacity / SLogNumPartitions;
		Size		per_part_shmem_size;
		char	   *block_ptr;
		int			part;

		if (per_part_cap < 64)
			per_part_cap = 64;

		per_part_shmem_size = MAXALIGN(SLogFlatHashShmemSize(per_part_cap,
															 MaxBackends));
		block_ptr = SLogFlatHashBlock;

		for (part = 0; part < SLogNumPartitions; part++)
		{
			SLogFlatPartition *fp = &SLogState->tuple_partitions[part];

			/* Initialize per-partition writer lock */
			LWLockInitialize(&fp->writer_lock.lock, LWTRANCHE_SLOG);

			/* Seqlock counter starts even (stable, no writer in progress) */
			SeqLockInit(&fp->seqlock);

			/* Single flat-hash copy carved from the shared block */
			fp->hash = (SLogFlatHash *) block_ptr;
			SLogFlatHashInit(fp->hash, per_part_cap);

			block_ptr += per_part_shmem_size;
		}
	}

	/* ---- Initialize DSA area for before-images ---- */
	SLogState->dsa_area = dsa_create_in_place(SLogState->dsa_space,
											  SLOG_DSA_INIT_SIZE,
											  LWTRANCHE_SLOG,
											  0);
	dsa_pin(SLogState->dsa_area);
	dsa_set_size_limit(SLogState->dsa_area,
					   (Size) slog_dsa_max_size_mb * 1024 * 1024);

	/*
	 * Create the ATM radix tree in the DSA area and record its handle.  The
	 * transient tree wrapper returned here is discarded when init's memory
	 * context is reset; the control block lives in the pinned DSA area, and
	 * each backend re-attaches lazily via slog_atm_tree().  We reuse
	 * LWTRANCHE_SLOG for the tree's internal lock (unused -- txn_lock
	 * serializes all access -- but a valid tranche is required).
	 */
	{
		slog_atm_radix_tree *tree;

		tree = slog_atm_create(SLogState->dsa_area, LWTRANCHE_SLOG);
		SLogState->atm_handle = slog_atm_get_handle(tree);
	}

	dsa_detach(SLogState->dsa_area);
	SLogState->dsa_area = NULL;	/* backends re-attach lazily */
}

/*
 * SLogShmemRequest_cb / SLogShmemInit_cb
 *		ShmemCallbacks adapters for SLogShmemRequest()/SLogShmemInit().
 *
 * sLog is RECNO's shared-memory tracking structure; it is registered as its
 * own PG_SHMEM_SUBSYSTEM entry (see storage/subsystemlist.h) rather than
 * being sized/initialized from within the generic UNDO subsystem's callback,
 * so the UNDO core has no compile-time or link-time dependency on RECNO.
 *
 * No .attach_fn is provided: SLogShmemInit() has no found-guard and
 * unconditionally reinitializes the flat-hash pool, which would corrupt live
 * shared sLog state if re-run in an EXEC_BACKEND child.  The framework only
 * calls attach_fn when non-NULL, so omitting it means EXEC_BACKEND children
 * simply re-attach to the already-initialized shared structs via the
 * ShmemRequestStruct(.ptr=&SLogState) registration in SLogShmemRequest(),
 * exactly as UndoShmemAttach_internal() used to do by hand.
 */
static void
SLogShmemRequest_cb(void *arg)
{
	SLogShmemRequest();
}

static void
SLogShmemInit_cb(void *arg)
{
	SLogShmemInit();
}

const ShmemCallbacks SLogShmemCallbacks = {
	.request_fn = SLogShmemRequest_cb,
	.init_fn = SLogShmemInit_cb,
};

/* ----------------------------------------------------------------
 * DSA lifecycle for before-image shared memory
 * ----------------------------------------------------------------
 */

/*
 * SLogEnsureDsaAttached
 *		Lazy per-backend DSA attachment.
 *
 * Must be called before any DSA alloc/free/get_address operations.
 * Safe to call multiple times (no-op after first attach).
 */
void
SLogEnsureDsaAttached(void)
{
	MemoryContext oldcxt;

	if (slog_dsa_handle != NULL)
		return;					/* already attached */

	if (SLogState == NULL)
		return;					/* sLog not yet initialized */

	/*
	 * Allocate in TopMemoryContext so the dsa_area struct persists across
	 * transactions.  dsa_attach_in_place internally palloc's, so if we're
	 * in a transaction context, the handle would become a dangling pointer
	 * after transaction end.
	 */
	oldcxt = MemoryContextSwitchTo(TopMemoryContext);
	slog_dsa_handle = dsa_attach_in_place(SLogState->dsa_space, NULL);
	dsa_pin_mapping(slog_dsa_handle);
	MemoryContextSwitchTo(oldcxt);
}

/*
 * SLogDsaFreeBeforeImage
 *		Free a before-image from the shared DSA area.
 */
void
SLogDsaFreeBeforeImage(dsa_pointer dp)
{
	if (!DsaPointerIsValid(dp))
		return;

	SLogEnsureDsaAttached();
	dsa_free(slog_dsa_handle, dp);
}

/* ================================================================
 * Transaction sLog functions
 * ================================================================
 */

/*
 * SLogTxnInsert
 *		Insert an aborted transaction entry into the sLog.
 *
 * Creates an entry in the ATM radix tree.  The tree grows on demand from
 * the DSA area, so unlike the old fixed pool this never fails for lack of
 * space; the return type is retained for API compatibility and is always
 * true.
 */
bool
SLogTxnInsert(TransactionId xid, Oid reloid, Oid dboid,
			  XLogRecPtr last_batch_lsn)
{
	slog_atm_radix_tree *tree = slog_atm_tree();
	uint64		key = slog_atm_key(xid, reloid);
	slog_atm_value_t value;

	value.last_batch_lsn = last_batch_lsn;
	value.dboid = dboid;
	value.abort_time = GetCurrentTimestamp();
	value.revert_complete = false;

	LWLockAcquire(&SLogState->txn_lock.lock, LW_EXCLUSIVE);

	/*
	 * RT_SET returns whether the key already existed.  A duplicate insert is
	 * a no-op on the (xid, reloid) identity; the original entry's data fields
	 * would be overwritten, so preserve the old behavior of leaving an
	 * existing entry untouched by finding first.
	 */
	if (slog_atm_find(tree, key) == NULL)
		(void) slog_atm_set(tree, key, &value);

	LWLockRelease(&SLogState->txn_lock.lock);
	return true;
}

/*
 * SLogTxnLookup
 *		Look up a specific (xid, reloid) entry.
 *
 * Returns true if found, copying the entry into *entry_out.
 */
bool
SLogTxnLookup(TransactionId xid, Oid reloid, SLogTxnEntry *entry_out)
{
	slog_atm_radix_tree *tree = slog_atm_tree();
	uint64		key = slog_atm_key(xid, reloid);
	slog_atm_value_t *found;

	LWLockAcquire(&SLogState->txn_lock.lock, LW_SHARED);

	found = slog_atm_find(tree, key);

	if (found != NULL && entry_out != NULL)
	{
		entry_out->xid = xid;
		entry_out->reloid = reloid;
		entry_out->last_batch_lsn = found->last_batch_lsn;
		entry_out->dboid = found->dboid;
		entry_out->abort_time = found->abort_time;
		entry_out->revert_complete = found->revert_complete;
	}

	LWLockRelease(&SLogState->txn_lock.lock);

	return (found != NULL);
}

/*
 * SLogTxnLookupByXid
 *		Find the UNDO chain for a given xid (any reloid).
 *
 * The radix tree has no seek-to-key API, so we iterate its entries in
 * ascending key order (xid-major) and return the first one whose xid
 * matches.  Because keys are xid-major we can stop as soon as we pass
 * the target xid.  The ATM only holds un-reverted aborted transactions,
 * so it is small and a full scan is acceptable on this cold path.
 */
bool
SLogTxnLookupByXid(TransactionId xid, XLogRecPtr *lsn_out)
{
	slog_atm_radix_tree *tree = slog_atm_tree();
	slog_atm_iter *iter;
	slog_atm_value_t *val;
	uint64		k;
	bool		result = false;

	LWLockAcquire(&SLogState->txn_lock.lock, LW_SHARED);

	iter = slog_atm_begin_iterate(tree);
	while ((val = slog_atm_iterate_next(iter, &k)) != NULL)
	{
		TransactionId k_xid = slog_atm_key_xid(k);

		if (k_xid < xid)
			continue;
		if (k_xid > xid)
			break;				/* passed the target range */

		if (lsn_out)
			*lsn_out = val->last_batch_lsn;
		result = true;
		break;
	}
	slog_atm_end_iterate(iter);

	LWLockRelease(&SLogState->txn_lock.lock);
	return result;
}

/*
 * SLogTxnRemove
 *		Remove a specific (xid, reloid) entry.
 */
void
SLogTxnRemove(TransactionId xid, Oid reloid)
{
	slog_atm_radix_tree *tree = slog_atm_tree();
	uint64		key = slog_atm_key(xid, reloid);

	LWLockAcquire(&SLogState->txn_lock.lock, LW_EXCLUSIVE);

	(void) slog_atm_delete(tree, key);

	LWLockRelease(&SLogState->txn_lock.lock);
}

/*
 * SLogTxnRemoveByXid
 *		Remove all sLog entries for a given transaction ID.
 *
 * Collects all keys for this xid into a local array during an ascending
 * iteration (keys are contiguous thanks to the xid-major ordering), then
 * deletes them after ending the iteration -- we do not delete while
 * iterating.  The ATM is small, so the full scan is acceptable.
 */
void
SLogTxnRemoveByXid(TransactionId xid)
{
	slog_atm_radix_tree *tree = slog_atm_tree();
	slog_atm_iter *iter;
	slog_atm_value_t *val;
	uint64		k;
	uint64	   *to_remove;
	int			nremove = 0;
	int			max_remove = 64;
	int			i;

	to_remove = (uint64 *) palloc(max_remove * sizeof(uint64));

	LWLockAcquire(&SLogState->txn_lock.lock, LW_EXCLUSIVE);

	/* Collect all keys with matching xid */
	iter = slog_atm_begin_iterate(tree);
	while ((val = slog_atm_iterate_next(iter, &k)) != NULL)
	{
		TransactionId k_xid = slog_atm_key_xid(k);

		(void) val;
		if (k_xid < xid)
			continue;
		if (k_xid > xid)
			break;

		if (nremove >= max_remove)
		{
			max_remove *= 2;
			to_remove = (uint64 *) repalloc(to_remove,
											max_remove * sizeof(uint64));
		}
		to_remove[nremove++] = k;
	}
	slog_atm_end_iterate(iter);

	/* Remove collected keys */
	for (i = 0; i < nremove; i++)
		(void) slog_atm_delete(tree, to_remove[i]);

	LWLockRelease(&SLogState->txn_lock.lock);

	pfree(to_remove);
}

/*
 * SLogTxnMarkReverted
 *		Mark all entries for a given xid as revert_complete.
 *
 * Iterates the tree in ascending key order and rewrites the value of each
 * entry for this xid with revert_complete = true.  Keys for one xid are
 * contiguous, so we stop once we pass the target xid.  We collect the
 * matching (key, value) pairs first and RT_SET them after ending the
 * iteration, so the tree is never mutated while an iterator is live.
 */
void
SLogTxnMarkReverted(TransactionId xid)
{
	slog_atm_radix_tree *tree = slog_atm_tree();
	slog_atm_iter *iter;
	slog_atm_value_t *val;
	uint64		k;
	uint64	   *keys;
	slog_atm_value_t *vals;
	int			n = 0;
	int			max_n = 64;
	int			i;

	keys = (uint64 *) palloc(max_n * sizeof(uint64));
	vals = (slog_atm_value_t *) palloc(max_n * sizeof(slog_atm_value_t));

	LWLockAcquire(&SLogState->txn_lock.lock, LW_EXCLUSIVE);

	/*
	 * Collect matching entries first, then RT_SET them after ending the
	 * iteration, to avoid mutating the tree while an iterator is live.
	 */
	iter = slog_atm_begin_iterate(tree);
	while ((val = slog_atm_iterate_next(iter, &k)) != NULL)
	{
		TransactionId k_xid = slog_atm_key_xid(k);

		if (k_xid < xid)
			continue;
		if (k_xid > xid)
			break;

		if (n >= max_n)
		{
			max_n *= 2;
			keys = (uint64 *) repalloc(keys, max_n * sizeof(uint64));
			vals = (slog_atm_value_t *) repalloc(vals,
												 max_n * sizeof(slog_atm_value_t));
		}
		keys[n] = k;
		vals[n] = *val;
		vals[n].revert_complete = true;
		n++;
	}
	slog_atm_end_iterate(iter);

	for (i = 0; i < n; i++)
		(void) slog_atm_set(tree, keys[i], &vals[i]);

	LWLockRelease(&SLogState->txn_lock.lock);

	pfree(keys);
	pfree(vals);
}

/*
 * SLogTxnGetNextUnreverted
 *		Find an entry that hasn't been reverted yet.
 *
 * Iterates the tree in ascending key order (xid-major), returning the
 * first entry with revert_complete == false.
 */
bool
SLogTxnGetNextUnreverted(TransactionId *xid_out, Oid *dboid_out,
						 XLogRecPtr *lsn_out)
{
	slog_atm_radix_tree *tree = slog_atm_tree();
	slog_atm_iter *iter;
	slog_atm_value_t *val;
	uint64		k;
	bool		result = false;

	LWLockAcquire(&SLogState->txn_lock.lock, LW_SHARED);

	iter = slog_atm_begin_iterate(tree);
	while ((val = slog_atm_iterate_next(iter, &k)) != NULL)
	{
		if (!val->revert_complete)
		{
			*xid_out = slog_atm_key_xid(k);
			*dboid_out = val->dboid;
			*lsn_out = val->last_batch_lsn;
			result = true;
			break;
		}
	}
	slog_atm_end_iterate(iter);

	LWLockRelease(&SLogState->txn_lock.lock);
	return result;
}

/*
 * SLogRecoveryFinalize
 *		Count entries after recovery for logging.
 */
void
SLogRecoveryFinalize(int *total_out, int *unreverted_out)
{
	slog_atm_radix_tree *tree = slog_atm_tree();
	slog_atm_iter *iter;
	slog_atm_value_t *val;
	uint64		k;
	int			total = 0;
	int			unreverted = 0;

	LWLockAcquire(&SLogState->txn_lock.lock, LW_SHARED);

	iter = slog_atm_begin_iterate(tree);
	while ((val = slog_atm_iterate_next(iter, &k)) != NULL)
	{
		total++;
		if (!val->revert_complete)
			unreverted++;
	}
	slog_atm_end_iterate(iter);

	LWLockRelease(&SLogState->txn_lock.lock);

	if (total_out)
		*total_out = total;
	if (unreverted_out)
		*unreverted_out = unreverted;
}

/*
 * SLogTxnGetOldestUnrevertedLSN
 *		Return the minimum last_batch_lsn across all unreverted entries.
 *
 * Used by the WAL retention logic to prevent recycling WAL segments that
 * still contain UNDO batches needed by the logical revert worker.
 * Returns InvalidXLogRecPtr if no unreverted entries exist.
 */
XLogRecPtr
SLogTxnGetOldestUnrevertedLSN(void)
{
	slog_atm_radix_tree *tree = slog_atm_tree();
	slog_atm_iter *iter;
	slog_atm_value_t *val;
	uint64		k;
	XLogRecPtr	oldest = InvalidXLogRecPtr;

	LWLockAcquire(&SLogState->txn_lock.lock, LW_SHARED);

	iter = slog_atm_begin_iterate(tree);
	while ((val = slog_atm_iterate_next(iter, &k)) != NULL)
	{
		if (!val->revert_complete &&
			XLogRecPtrIsValid(val->last_batch_lsn))
		{
			if (!XLogRecPtrIsValid(oldest) ||
				val->last_batch_lsn < oldest)
				oldest = val->last_batch_lsn;
		}
	}
	slog_atm_end_iterate(iter);

	LWLockRelease(&SLogState->txn_lock.lock);
	return oldest;
}

/*
 * SLogTxnSnapshotForCheckpoint
 *		Copy every ATM entry into a palloc'd array for durable checkpointing.
 *
 * Returns the number of entries and stores a palloc'd array of SLogTxnEntry
 * in *entries_out (NULL if there are none).  The caller owns the array and
 * must pfree it.  Runs outside any critical section (CheckPointATM), so
 * palloc is safe.
 */
int
SLogTxnSnapshotForCheckpoint(SLogTxnEntry **entries_out)
{
	slog_atm_radix_tree *tree = slog_atm_tree();
	slog_atm_iter *iter;
	slog_atm_value_t *val;
	uint64		k;
	SLogTxnEntry *arr = NULL;
	int			count = 0;
	int			capacity = 64;

	*entries_out = NULL;

	arr = (SLogTxnEntry *) palloc(sizeof(SLogTxnEntry) * capacity);

	LWLockAcquire(&SLogState->txn_lock.lock, LW_SHARED);

	iter = slog_atm_begin_iterate(tree);
	while ((val = slog_atm_iterate_next(iter, &k)) != NULL)
	{
		if (count >= capacity)
		{
			capacity *= 2;
			arr = (SLogTxnEntry *) repalloc(arr,
											sizeof(SLogTxnEntry) * capacity);
		}

		arr[count].xid = slog_atm_key_xid(k);
		arr[count].reloid = slog_atm_key_reloid(k);
		arr[count].last_batch_lsn = val->last_batch_lsn;
		arr[count].dboid = val->dboid;
		arr[count].abort_time = val->abort_time;
		arr[count].revert_complete = val->revert_complete;
		count++;
	}
	slog_atm_end_iterate(iter);

	LWLockRelease(&SLogState->txn_lock.lock);

	if (count == 0)
	{
		pfree(arr);
		return 0;
	}

	*entries_out = arr;
	return count;
}

/* ================================================================
 * Tuple sLog functions
 * ================================================================
 */

/* ----------------------------------------------------------------
 * Emergency eviction
 * ----------------------------------------------------------------
 */

/*
 * SLogTupleEvictCommitted -- evict entries for committed transactions.
 *
 * Called when the sLog flat hash is full.  Scans each partition under its
 * writer lock to collect evictable keys, then applies REMOVE_ENTRY ops.
 * (This is a cold path, run only when a partition is full; holding the
 * writer lock across the scan is simpler than a seqlock retry loop and the
 * per-op TransactionIdIsInProgress/DidCommit probes must not run repeatedly.)
 *
 * Returns the number of entries evicted.
 */
static int
SLogTupleEvictCommitted(void)
{
	SLogTupleKey *keys_to_evict;
	int			nkeys = 0;
	int			max_evict = 1024;
	int			part;

	keys_to_evict = (SLogTupleKey *)
		palloc(sizeof(SLogTupleKey) * max_evict);

	/* Phase 1: scan each partition under its writer lock to collect keys */
	for (part = 0; part < SLogNumPartitions && nkeys < max_evict; part++)
	{
		SLogFlatPartition *fp = SLogGetPartitionByIndex(part);
		SLogFlatHashScanState scan;
		const SLogFlatBucket *bucket;

		LWLockAcquire(&fp->writer_lock.lock, LW_EXCLUSIVE);

		SLogFlatHashScanInit(&scan);
		while ((bucket = SLogFlatHashScanNext(fp->hash, &scan)) != NULL)
		{
			const SLogTupleEntry *entry = &bucket->entry;
			bool		all_committed = true;
			bool		has_any_op = false;
			int			i;

			for (i = 0; i < SLOG_MAX_TUPLE_OPS; i++)
			{
				if (!entry->ops[i].in_use)
					continue;
				has_any_op = true;

				/*
				 * Skip entries that have retained UPDATE before-images.
				 * These MUST survive until SLogTupleCleanupRetained() is
				 * called by the background worker with a safe HLC horizon.
				 * Evicting them causes MVCC visibility failures: the
				 * on-page RECNO_TUPLE_UPDATED flag becomes orphaned and
				 * subsequent UPDATE...RETURNING can't find the tuple.
				 */
				if (entry->ops[i].op_type == SLOG_OP_UPDATE &&
					entry->ops[i].commit_hlc != 0)
				{
					all_committed = false;
					break;
				}

				if (TransactionIdIsInProgress(entry->ops[i].xid) ||
					!TransactionIdDidCommit(entry->ops[i].xid))
				{
					all_committed = false;
					break;
				}
			}

			if (has_any_op && all_committed)
			{
				keys_to_evict[nkeys++] = bucket->key;
				if (nkeys >= max_evict)
					break;
			}
		}

		LWLockRelease(&fp->writer_lock.lock);
	}

	/* Phase 2: apply removals grouped by partition */
	if (nkeys > 0)
	{
		int			i;

		for (part = 0; part < SLogNumPartitions; part++)
		{
			SLogFlatPartition *fp = SLogGetPartitionByIndex(part);
			bool		has_keys_for_part = false;

			/* Check if any keys belong to this partition */
			for (i = 0; i < nkeys; i++)
			{
				if (SLogFlatHashPartitionIndex(&keys_to_evict[i]) == part)
				{
					has_keys_for_part = true;
					break;
				}
			}
			if (!has_keys_for_part)
				continue;

			LWLockAcquire(&fp->writer_lock.lock, LW_EXCLUSIVE);
			SLogSeqWriteBegin(fp);

			for (i = 0; i < nkeys; i++)
			{
				SLogFlatOp	flat_op;

				if (SLogFlatHashPartitionIndex(&keys_to_evict[i]) != part)
					continue;

				memset(&flat_op, 0, sizeof(flat_op));
				flat_op.kind = SLOG_FLAT_OP_REMOVE_ENTRY;
				flat_op.key = keys_to_evict[i];
				SLogFlatHashApply(fp->hash, &flat_op, sizeof(flat_op));
			}

			SLogSeqWriteEnd(fp);
			LWLockRelease(&fp->writer_lock.lock);
		}
	}

	pfree(keys_to_evict);

	return nkeys;
}

/* ----------------------------------------------------------------
 * Core Tuple sLog API
 * ----------------------------------------------------------------
 */

/*
 * SLogTupleInsert
 *		Record a tuple operation in the sLog.
 *
 * Inserts into the seqlock-protected flat hash (wait-free reads).
 * Performs overflow handling before failing.
 *
 * Also adds the key to the backend-private tracking list for cleanup.
 */
bool
SLogTupleInsert(Oid relid, ItemPointer tid, TransactionId xid,
				SLogOpType op_type, TransactionId subxid,
				CommandId cid, TimestampTz commit_ts,
				uint32 spec_token, LockTupleMode lock_mode)
{
	SLogTupleKey key;
	SLogFlatOp	flat_op;
	SLogFlatPartition *fp;
	int			entries_before;
	int			entries_after;

	Assert(SLogState != NULL);
	Assert(TransactionIdIsValid(xid));
	Assert(ItemPointerIsValid(tid));

	/* Zero for deterministic hashing (ItemPointerData is 6 bytes) */
	memset(&key, 0, sizeof(key));
	key.relid = relid;
	ItemPointerCopy(tid, &key.tid);

	fp = SLogGetPartition(&key);

	/*
	 * Retained UPDATE entries (commit_hlc != 0) are never evicted by the
	 * on-overflow path -- SLogTupleEvictCommitted() skips them.  They are
	 * cleaned up by SLogTupleCleanupRetained(), driven by the UNDO background
	 * worker or, when it is disabled, by the access method calling
	 * SLogTupleMaybeCleanupRetained() outside any buffer-locked section.  This
	 * function never triggers that sweep itself (see note below).
	 *
	 * Per-TID ops array overflow (SLOG_MAX_TUPLE_OPS=32 slots all full of
	 * retained entries on hot rows) is handled by flat_hash_apply_insert()
	 * which reclaims the oldest retained entry when no free slot exists.
	 */

	{
		/* Build the flat hash op */
		memset(&flat_op, 0, sizeof(flat_op));
		flat_op.kind = SLOG_FLAT_OP_INSERT;
		flat_op.key = key;
		flat_op.xid = xid;
		flat_op.subxid = subxid;
		flat_op.commit_hlc = 0;		/* unused for INSERT */

		/*
		 * The xid reclaim horizon is computed LAZILY, not on every insert.  It
		 * is only consumed by flat_hash_apply_insert to reclaim a slot when a
		 * hot row's per-TID ops array is completely full; the common path (free
		 * slot, same-xid overwrite, or a slot freed by coalescing) never reads
		 * it.  Computing it here would run a ProcArrayLock-shared xid-horizon
		 * scan on every CAS-update -- a measured hot-path cost -- for a value
		 * used only in the rare full-array case.
		 *
		 * So the fast path passes InvalidTransactionId, which now fail-safe
		 * DISABLES reclamation (see flat_hash_apply_insert): an invalid horizon
		 * frees NOTHING.  This preserves the original safety invariant -- a
		 * stale or absent horizon must never cause a lost update -- in the most
		 * conservative direction possible.  If the op is then dropped because
		 * the array was genuinely full, the caller (below) recomputes the real,
		 * authoritative horizon and retries; reclamation is thus attempted with
		 * a fresh horizon exactly when, and only when, it is actually needed.
		 */
		flat_op.reclaim_xid_horizon = InvalidTransactionId;
		flat_op.tuple_op.xid = xid;
		flat_op.tuple_op.subxid = subxid;
		flat_op.tuple_op.op_type = op_type;
		flat_op.tuple_op.cid = cid;
		flat_op.tuple_op.commit_ts = commit_ts;
		flat_op.tuple_op.spec_token = spec_token;
		flat_op.tuple_op.lock_mode = lock_mode;
		flat_op.tuple_op.commit_hlc = 0;
		flat_op.tuple_op.before_image_dp = InvalidDsaPointer;
		flat_op.tuple_op.in_use = true;
	}

	/* Apply to flat hash via seqlock writer path (partition-local) */
	LWLockAcquire(&fp->writer_lock.lock, LW_EXCLUSIVE);

	/* Check capacity before insert (writer lock held: fp->hash is stable) */
	entries_before = fp->hash->num_entries;

	SLogSeqWriteBegin(fp);
	SLogFlatHashApply(fp->hash, &flat_op, sizeof(flat_op));
	SLogSeqWriteEnd(fp);

	/* Check if insert succeeded */
	entries_after = fp->hash->num_entries;

	LWLockRelease(&fp->writer_lock.lock);

	/*
	 * Verify the op was actually stored.  We must probe for THIS xid's op, not
	 * merely the bucket: on a hot row the bucket pre-exists (it holds other
	 * TIDs' / xids' markers), so num_entries is unchanged and the bucket is
	 * present even when flat_hash_apply_insert silently dropped our op because
	 * the per-TID ops array was full with nothing reclaimable.  Testing only
	 * bucket presence (the old SLogFlatHashProbe != NULL) reports success for a
	 * dropped op, so no marker exists to stamp at PRE_COMMIT and the next
	 * concurrent writer clobbers this update -- a lost update.  Probe the ops
	 * array for our xid instead.
	 */
	if (entries_after == entries_before)
	{
		bool		op_stored;

		LWLockAcquire(&fp->writer_lock.lock, LW_EXCLUSIVE);
		op_stored = SLogFlatHashHasOpForXid(fp->hash, &key, xid);
		LWLockRelease(&fp->writer_lock.lock);

		if (!op_stored)
		{
			/*
			 * The op was dropped: the per-TID ops array was full and the fast
			 * path passed an invalid horizon, which disables reclamation.  On a
			 * hot row the array is full of THIS relation's own below-horizon
			 * UPDATE markers, which the reclaim path can free WITHOUT the
			 * cross-partition SLogTupleEvictCommitted() sweep.  So recompute the
			 * real, authoritative horizon now (the ProcArrayLock scan we skipped
			 * on the fast path) and retry: flat_hash_apply_insert can now
			 * reclaim a settled marker and store our op in place.
			 */
			TransactionId horizon = GetOldestNonRemovableTransactionId(NULL);

			flat_op.reclaim_xid_horizon = horizon;

			LWLockAcquire(&fp->writer_lock.lock, LW_EXCLUSIVE);
			SLogSeqWriteBegin(fp);
			SLogFlatHashApply(fp->hash, &flat_op, sizeof(flat_op));
			SLogSeqWriteEnd(fp);
			op_stored = SLogFlatHashHasOpForXid(fp->hash, &key, xid);
			LWLockRelease(&fp->writer_lock.lock);
		}

		if (!op_stored)
		{
			/* Table or per-TID array was full — try eviction */
			int		evicted = SLogTupleEvictCommitted();

			if (evicted > 0)
			{
				/* Retry */
				LWLockAcquire(&fp->writer_lock.lock, LW_EXCLUSIVE);
				SLogSeqWriteBegin(fp);
				SLogFlatHashApply(fp->hash, &flat_op, sizeof(flat_op));
				SLogSeqWriteEnd(fp);
				op_stored = SLogFlatHashHasOpForXid(fp->hash, &key, xid);
				LWLockRelease(&fp->writer_lock.lock);
			}

			if (!op_stored)
			{
				slog_overflow_warning_count++;
				{
					TimestampTz now = GetCurrentTimestamp();

					if (slog_overflow_warning_count == 1 ||
						TimestampDifferenceExceeds(slog_overflow_last_warning,
												  now, 1000))
					{
						elog(WARNING, "sLog tuple hash partition full "
							 "(%d entries); %d overflow(s) this transaction "
							 "on rel %u (visibility relies on UNCOMMITTED "
							 "flag + UNDO replay)",
							 SLogTupleNumEntries() / SLogNumPartitions,
							 slog_overflow_warning_count, relid);
						slog_overflow_last_warning = now;
					}
				}

				SLogTupleTrackLocalOnly(relid, tid, xid, subxid);
				return false;
			}
		}
	}

	SLogTupleTrackKey(key, xid, subxid, op_type);
	return true;
}

/*
 * SLogTupleInsertRecovery
 *		Record a tuple operation during WAL replay (recovery-safe).
 *
 * This is a simplified variant of SLogTupleInsert() designed for use during
 * WAL redo on hot standbys.  Key differences:
 *   - Does NOT call SLogTupleTrackKey() (no backend-local tracking needed)
 *   - Returns false silently if the hash is full (instead of ERROR/PANIC)
 *   - No retries with pg_usleep (would delay WAL replay)
 *
 * Used by recno_xlog_insert_redo() to register UNCOMMITTED tuples in the
 * per-tuple sLog so that RECNO visibility can correctly determine
 * visibility on standbys (where the sLog is otherwise never populated).
 */
bool
SLogTupleInsertRecovery(Oid relid, ItemPointer tid, TransactionId xid,
						SLogOpType op_type)
{
	SLogTupleKey key;
	SLogFlatOp	clear_op;
	SLogFlatOp	flat_op;

	if (SLogState == NULL)
		return false;

	memset(&key, 0, sizeof(key));
	key.relid = relid;
	ItemPointerCopy(tid, &key.tid);

	/*
	 * Clear any pre-existing entry at this exact TID before registering the
	 * new op.  A physical INSERT always targets a free line pointer, so any
	 * sLog entry already present at this TID belongs to a dead prior occupant
	 * (deleted and VACUUM-recycled).  On the primary the prior occupant's sLog
	 * entry is removed by its commit/abort xact callback, but the standby redo
	 * path only ever inserts -- it never removes -- so without this the stale
	 * entry survives.  flat_hash_apply_insert() only overwrites a same-xid op,
	 * so a stale op from the prior occupant's (different) xid would persist and
	 * make RECNO visibility trip on TransactionIdDidAbort()/IsInProgress()
	 * for that dead xid, wrongly hiding the freshly inserted live tuple.
	 *
	 * Tombstoning the entry is safe and self-healing: flat_hash_apply_insert()
	 * reuses tombstoned buckets, so the immediately following INSERT recreates
	 * a clean entry holding only this insert's op.
	 */
	memset(&clear_op, 0, sizeof(clear_op));
	clear_op.kind = SLOG_FLAT_OP_REMOVE_ENTRY;
	clear_op.key = key;

	/* Apply to flat hash */
	memset(&flat_op, 0, sizeof(flat_op));
	flat_op.kind = SLOG_FLAT_OP_INSERT;
	flat_op.key = key;
	flat_op.xid = xid;
	flat_op.tuple_op.xid = xid;
	flat_op.tuple_op.subxid = InvalidTransactionId;
	flat_op.tuple_op.op_type = op_type;
	flat_op.tuple_op.cid = InvalidCommandId;
	flat_op.tuple_op.commit_ts = 0;
	flat_op.tuple_op.spec_token = 0;
	flat_op.tuple_op.commit_hlc = 0;
	flat_op.tuple_op.before_image_dp = InvalidDsaPointer;
	flat_op.tuple_op.in_use = true;

	LWLockAcquire(SLOG_PART_WRITER_LOCK(&key), LW_EXCLUSIVE);
	SLogSeqWriteBegin(SLogGetPartition(&key));
	SLogFlatHashApply(SLogGetPartition(&key)->hash, &clear_op, sizeof(clear_op));
	SLogFlatHashApply(SLogGetPartition(&key)->hash, &flat_op, sizeof(flat_op));
	SLogSeqWriteEnd(SLogGetPartition(&key));
	LWLockRelease(SLOG_PART_WRITER_LOCK(&key));

	return true;
}

/*
 * SLogTupleLookup
 *		Look up a tuple's sLog entry (copy semantics).
 *
 * Returns true if found, copying the full entry into *entry_out.
 * WAIT-FREE: uses the seqlock read-side retry loop.
 */
bool
SLogTupleLookup(Oid relid, ItemPointer tid, SLogTupleEntry *entry_out)
{
	SLogTupleKey key;
	SLogFlatPartition *fp;
	bool		found;
	uint32		slog_seq_;

	memset(&key, 0, sizeof(key));
	key.relid = relid;
	ItemPointerCopy(tid, &key.tid);

	fp = SLogGetPartition(&key);

	SLOG_SEQ_READ_BEGIN(fp, slog_seq_)
	{
		const SLogFlatBucket *bucket = SLogFlatHashProbe(fp->hash, &key);

		found = (bucket != NULL);
		if (found && entry_out)
			memcpy(entry_out, &bucket->entry, sizeof(SLogTupleEntry));
	}
	SLOG_SEQ_READ_END(fp, slog_seq_);

	return found;
}

/*
 * SLogTupleLookupFiltered
 *		Find sLog entries for a TID, optionally filtered by xid.
 *
 * WAIT-FREE: uses the seqlock read-side.  If xid_filter is valid, returns
 * only ops for that xid.  If InvalidTransactionId, returns all active
 * ops for this TID.
 *
 * Returns the number of ops written to ops_out.
 */
int
SLogTupleLookupFiltered(Oid relid, ItemPointer tid,
						TransactionId xid_filter,
						SLogTupleOp *ops_out, int max_ops)
{
	SLogTupleKey key;
	SLogFlatPartition *fp;
	int			nfound = 0;
	uint32		slog_seq_;

	memset(&key, 0, sizeof(key));
	key.relid = relid;
	ItemPointerCopy(tid, &key.tid);

	fp = SLogGetPartition(&key);

	SLOG_SEQ_READ_BEGIN(fp, slog_seq_)
	{
		const SLogFlatBucket *bucket = SLogFlatHashProbe(fp->hash, &key);

		nfound = 0;				/* reset: body may re-run on retry */
		if (bucket != NULL)
		{
			const SLogTupleEntry *entry = &bucket->entry;
			int			i;

			for (i = 0; i < SLOG_MAX_TUPLE_OPS && nfound < max_ops; i++)
			{
				if (!entry->ops[i].in_use)
					continue;

				if (TransactionIdIsValid(xid_filter) &&
					!TransactionIdEquals(entry->ops[i].xid, xid_filter))
					continue;

				memcpy(&ops_out[nfound], &entry->ops[i], sizeof(SLogTupleOp));
				nfound++;
			}
		}
	}
	SLOG_SEQ_READ_END(fp, slog_seq_);

	return nfound;
}

/*
 * SLogTupleRemove
 *		Remove operations for a specific xid from a tuple entry.
 *
 * Uses the seqlock writer path with external (writer_lock) serialization.
 */
void
SLogTupleRemove(Oid relid, ItemPointer tid, TransactionId xid)
{
	SLogTupleKey key;
	SLogFlatOp	op;

	memset(&key, 0, sizeof(key));
	key.relid = relid;
	ItemPointerCopy(tid, &key.tid);

	memset(&op, 0, sizeof(op));
	op.kind = SLOG_FLAT_OP_REMOVE_XID;
	op.key = key;
	op.xid = xid;

	SLogPartApplyOne(SLogGetPartition(&key), &op);
}

/*
 * Width of the touched-partition bitmap, in uint64 words.  Sized to cover
 * SLOG_FLAT_MAX_PARTITIONS so the bitmap is a fixed on-stack array regardless
 * of the runtime partition count.
 */
#define SLOG_PART_BITMAP_WORDS	((SLOG_FLAT_MAX_PARTITIONS + 63) / 64)

/*
 * SLogCollectTrackedPartitions
 *		Build a bitmap of the partitions touched by xid's shared tracked keys.
 *
 * Walks slog_tracked_keys once, setting one bit per distinct partition that
 * holds a non-local_only entry for xid.  Callers then iterate only the set
 * partitions instead of sweeping all SLogNumPartitions, turning the batch
 * apply from O(num_partitions * tracked_keys) into
 * O(touched_partitions * tracked_keys) -- optimal for the common single-row,
 * single-partition transaction.
 *
 * Returns the number of distinct partitions marked.
 */
static int
SLogCollectTrackedPartitions(TransactionId xid,
							 uint64 *bitmap)
{
	SLogTrackedKey *tk;
	int			nparts = 0;

	memset(bitmap, 0, sizeof(uint64) * SLOG_PART_BITMAP_WORDS);

	for (tk = slog_tracked_keys; tk != NULL; tk = tk->next)
	{
		int			part;
		uint64		mask;

		if (!TransactionIdEquals(tk->xid, xid) || tk->local_only)
			continue;

		part = SLogFlatHashPartitionIndex(&tk->key);
		mask = UINT64CONST(1) << (part & 63);
		if (!(bitmap[part >> 6] & mask))
		{
			bitmap[part >> 6] |= mask;
			nparts++;
		}
	}

	return nparts;
}

/*
 * SLogTupleRemoveByXid
 *		Remove all tuple sLog entries for a transaction.
 *
 * Uses the backend-local tracking list.  Applies REMOVE_XID ops to the
 * flat hash in a single writer batch.
 */
void
SLogTupleRemoveByXid(TransactionId xid)
{
	SLogTrackedKey *tk;
	uint64		touched[SLOG_PART_BITMAP_WORDS] = {0};
	int			part;

	if (SLogState == NULL)
		return;

	/*
	 * Collect the partitions xid actually touches in one pass.  Walking only
	 * those avoids an O(num_partitions * tracked_keys) sweep over every
	 * partition for what is usually a single-row transaction.
	 */
	if (SLogCollectTrackedPartitions(xid, touched) == 0)
		return;

	/* Batch apply REMOVE_XID ops grouped by partition */
	for (part = 0; part < SLogNumPartitions; part++)
	{
		SLogFlatPartition *fp;

		if (!(touched[part >> 6] & (UINT64CONST(1) << (part & 63))))
			continue;

		fp = SLogGetPartitionByIndex(part);

		LWLockAcquire(&fp->writer_lock.lock, LW_EXCLUSIVE);
		SLogSeqWriteBegin(fp);

		for (tk = slog_tracked_keys; tk != NULL; tk = tk->next)
		{
			SLogFlatOp	flat_op;

			if (!TransactionIdEquals(tk->xid, xid) || tk->local_only)
				continue;
			if (SLogFlatHashPartitionIndex(&tk->key) != part)
				continue;

			memset(&flat_op, 0, sizeof(flat_op));
			flat_op.kind = SLOG_FLAT_OP_REMOVE_XID;
			flat_op.key = tk->key;
			flat_op.xid = xid;
			SLogFlatHashApply(fp->hash, &flat_op, sizeof(flat_op));
		}

		SLogSeqWriteEnd(fp);
		LWLockRelease(&fp->writer_lock.lock);
	}
}

/*
 * SLogTupleCommitByXid
 *		Handle commit for tuple sLog: retain UPDATE entries with before-images,
 *		remove INSERT/DELETE/LOCK entries.
 *
 * For UPDATE ops with a valid before_image_dp: stamp commit_hlc, leave
 * in_use = true (retained for MVCC reads by other backends).
 * For INSERT/DELETE/LOCK ops: remove as before (in_use = false, free DSA).
 *
 * Uses the backend-local tracking list.  Applies COMMIT_XID ops to the
 * flat hash in batch.
 */
void
SLogTupleCommitByXid(TransactionId xid, uint64 commit_hlc)
{
	SLogTrackedKey *tk;
	uint64		touched[SLOG_PART_BITMAP_WORDS] = {0};

	if (SLogState == NULL)
		return;

	/* Fast path: INSERT-only transactions never touch the shared hash */
	if (!slog_has_shared_entries)
		return;

	/*
	 * Collect the partitions xid touches in one pass; the COMMIT_XID batch
	 * below iterates only those rather than sweeping all SLogNumPartitions.
	 */
	if (SLogCollectTrackedPartitions(xid, touched) == 0)
		return;

	/*
	 * WS-PVS3: committed-UPDATE cross-backend before-images are not published
	 * to shared DSA (Phase 1) and committed-UPDATE markers are no longer
	 * retained on the flat hash (Phase 2).  Snapshot-isolation readers
	 * reconstruct the visible version by walking the durable UNDO fork
	 * chain (RecnoReconstructVisibleVersion in recno_pvs.c); the write-write
	 * conflict probe (RecnoTupleHasCommittedUpdateAfter) reads the head
	 * verptr on the on-page tuple and resolves it in the same fork.
	 *
	 * flat_hash_apply_commit_xid therefore removes every op of xid at commit
	 * -- identical to INSERT/DELETE/LOCK -- which drains bucket table_full.
	 *
	 * The local tk->before_image (palloc'd in TopTransactionContext by
	 * SLogTupleStoreBeforeImage) is still used for intra-transaction
	 * savepoint rollback via RecnoRestoreBeforeImages and is unaffected.
	 */

	/* Batch apply COMMIT_XID ops grouped by partition */
	{
		int		part;

		for (part = 0; part < SLogNumPartitions; part++)
		{
			SLogFlatPartition *fp;

			if (!(touched[part >> 6] & (UINT64CONST(1) << (part & 63))))
				continue;

			fp = SLogGetPartitionByIndex(part);

			LWLockAcquire(&fp->writer_lock.lock, LW_EXCLUSIVE);
			SLogSeqWriteBegin(fp);

			for (tk = slog_tracked_keys; tk != NULL; tk = tk->next)
			{
				SLogFlatOp	flat_op;

				if (!TransactionIdEquals(tk->xid, xid) || tk->local_only)
					continue;
				if (SLogFlatHashPartitionIndex(&tk->key) != part)
					continue;

				memset(&flat_op, 0, sizeof(flat_op));
				flat_op.kind = SLOG_FLAT_OP_COMMIT_XID;
				flat_op.key = tk->key;
				flat_op.xid = xid;
				flat_op.commit_hlc = commit_hlc;
				SLogFlatHashApply(fp->hash, &flat_op, sizeof(flat_op));
			}

			SLogSeqWriteEnd(fp);
			LWLockRelease(&fp->writer_lock.lock);
		}
	}
}

/*
 * SLogTupleRemoveByXidSingle
 *		Remove the sLog entry for a single tuple identified by (relid, tid, xid).
 *
 * Used by the RECNO two-phase postcommit callback to clean up sLog entries
 * one at a time (since the local tracking list is unavailable in the
 * resolving backend).
 */
void
SLogTupleRemoveByXidSingle(Oid relid, ItemPointer tid, TransactionId xid)
{
	SLogTupleKey key;
	SLogFlatOp	flat_op;

	if (SLogState == NULL)
		return;

	memset(&key, 0, sizeof(key));
	key.relid = relid;
	ItemPointerCopy(tid, &key.tid);

	/* Apply to flat hash */
	memset(&flat_op, 0, sizeof(flat_op));
	flat_op.kind = SLOG_FLAT_OP_REMOVE_XID;
	flat_op.key = key;
	flat_op.xid = xid;

	SLogPartApplyOne(SLogGetPartition(&key), &flat_op);
}

/*
 * SLogTupleMarkAbortedSingle
 *		Mark the sLog entry for a single tuple as ABORTED.
 *
 * Used by the RECNO two-phase postabort callback to mark sLog entries
 * one at a time (since the local tracking list is unavailable in the
 * resolving backend).  Only operates on tuples that already have a shared
 * sLog entry (DELETE/UPDATE operations).
 */
void
SLogTupleMarkAbortedSingle(Oid relid, ItemPointer tid, TransactionId xid)
{
	SLogTupleKey key;
	SLogFlatOp	flat_op;

	if (SLogState == NULL)
		return;

	memset(&key, 0, sizeof(key));
	key.relid = relid;
	ItemPointerCopy(tid, &key.tid);

	/* Apply to flat hash */
	memset(&flat_op, 0, sizeof(flat_op));
	flat_op.kind = SLOG_FLAT_OP_MARK_ABORTED;
	flat_op.key = key;
	flat_op.xid = xid;

	SLogPartApplyOne(SLogGetPartition(&key), &flat_op);
}

/*
 * SLogTupleRemoveBySubXid
 *		Handle subtransaction abort for tuple sLog.
 *
 * For entries that have a shared sLog entry, marks matching ops as ABORTED.
 * For entries that only have local tracking (from SLogTupleTrackLocalOnly,
 * used by INSERT), creates a shared ABORTED entry so visibility code can
 * find it.
 */
void
SLogTupleRemoveBySubXid(TransactionId xid, TransactionId subxid)
{
	SLogTrackedKey *tk;

	if (SLogState == NULL)
		return;

	for (tk = slog_tracked_keys; tk != NULL; tk = tk->next)
	{
		SLogFlatOp	flat_op;

		if (!TransactionIdEquals(tk->xid, xid))
			continue;
		if (tk->subxid != subxid)
			continue;

		memset(&flat_op, 0, sizeof(flat_op));

		if (tk->local_only)
		{
			/*
			 * INSERT elision: no shared entry exists yet.  Create one with
			 * SLOG_OP_ABORTED so visibility code can find it.
			 */
			flat_op.kind = SLOG_FLAT_OP_CREATE_ABORTED;
			flat_op.key = tk->key;
			flat_op.xid = xid;
			flat_op.subxid = subxid;
		}
		else
		{
			/* Shared entry exists -- mark matching ops ABORTED */
			flat_op.kind = SLOG_FLAT_OP_MARK_ABORTED;
			flat_op.key = tk->key;
			flat_op.xid = xid;
		}

		LWLockAcquire(SLOG_PART_WRITER_LOCK(&tk->key), LW_EXCLUSIVE);
		SLogSeqWriteBegin(SLogGetPartition(&tk->key));
		SLogFlatHashApply(SLogGetPartition(&tk->key)->hash, &flat_op,
						 sizeof(flat_op));
		SLogSeqWriteEnd(SLogGetPartition(&tk->key));
		LWLockRelease(SLOG_PART_WRITER_LOCK(&tk->key));

		/*
		 * Mark the backend-local tracked key as aborted so commit-time flag
		 * clearing (recno_stamp_tuple_committed) skips it.  Without this, a
		 * local-only INSERT tracked key still reads as a live INSERT at
		 * top-level commit, and RecnoClearUncommittedFlags would clear the
		 * tuple's UNCOMMITTED flag and stamp a commit HLC -- resurrecting a
		 * tuple that the savepoint rollback was supposed to discard.
		 */
		tk->op_type = SLOG_OP_ABORTED;
	}
}

/*
 * SLogTupleUpdateSubXid
 *		Re-parent ops on subtransaction commit.
 *
 * When a subtransaction commits, its entries' subxid is updated to the
 * parent's subxid so they survive subtransaction commit but are cleaned
 * up at top-level commit.
 */
void
SLogTupleUpdateSubXid(TransactionId xid,
					  TransactionId old_subxid,
					  TransactionId new_subxid)
{
	SLogTrackedKey *tk;

	if (SLogState == NULL)
		return;

	for (tk = slog_tracked_keys; tk != NULL; tk = tk->next)
	{
		if (!TransactionIdEquals(tk->xid, xid))
			continue;
		if (tk->subxid != old_subxid)
			continue;

		/* Re-parent the local entry */
		tk->subxid = new_subxid;

		/* Also re-parent in the shared sLog if an entry exists */
		if (!tk->local_only)
		{
			SLogFlatOp	flat_op;

			memset(&flat_op, 0, sizeof(flat_op));
			flat_op.kind = SLOG_FLAT_OP_UPDATE_OP;
			flat_op.key = tk->key;
			flat_op.xid = xid;
			flat_op.subxid = new_subxid;
			flat_op.tuple_op.subxid = old_subxid;

			LWLockAcquire(SLOG_PART_WRITER_LOCK(&tk->key), LW_EXCLUSIVE);
			SLogSeqWriteBegin(SLogGetPartition(&tk->key));
			SLogFlatHashApply(SLogGetPartition(&tk->key)->hash, &flat_op,
							 sizeof(flat_op));
			SLogSeqWriteEnd(SLogGetPartition(&tk->key));
			LWLockRelease(SLOG_PART_WRITER_LOCK(&tk->key));
		}
	}
}

/*
 * SLogTupleMarkAborted
 *		Mark all ops for a transaction as SLOG_OP_ABORTED.
 *
 * Called at transaction abort.  Entries remain so visibility checks can
 * distinguish "committed (no entry)" from "aborted (UNDO pending)".
 *
 * For local-only entries (INSERT elision), we CREATE a shared ABORTED
 * entry at abort time.  This is safe because:
 *   (a) Abort is uncommon (vast majority of transactions commit)
 *   (b) The UNDO worker removes both the sLog entry and the page tuple,
 *       bounding the lifetime of these entries
 *   (c) If the hash is full, we log a warning; the UNDO worker will
 *       eventually remove the tuple physically, resolving the anomaly
 */
void
SLogTupleMarkAborted(TransactionId xid)
{
	SLogTrackedKey *tk;
	int			part;
	dsa_pointer *freedps;
	int			nfreedps = 0;
	int			maxfreedps = 16;
	int			ops_since_publish;

	if (SLogState == NULL)
		return;

	freedps = (dsa_pointer *) palloc(maxfreedps * sizeof(dsa_pointer));

	/*
	 * Process all entries grouped by partition.  For each partition, acquire
	 * its writer lock once, apply all relevant ops, then release.
	 */
	for (part = 0; part < SLogNumPartitions; part++)
	{
		SLogFlatPartition *fp = SLogGetPartitionByIndex(part);
		bool		has_entries = false;

		/* Quick check: any local-only INSERT entries at all? */
		if (slog_insert_tids != NULL && slog_insert_tids->members > 0)
			has_entries = true;	/* conservative; filtered per-entry below */

		/* Check linked-list entries */
		if (!has_entries)
		{
			for (tk = slog_tracked_keys; tk != NULL; tk = tk->next)
			{
				if (!TransactionIdEquals(tk->xid, xid))
					continue;
				if (SLogFlatHashPartitionIndex(&tk->key) == part)
				{
					has_entries = true;
					break;
				}
			}
		}

		if (!has_entries &&
			(slog_insert_tids == NULL || slog_insert_tids->members == 0))
			continue;

		LWLockAcquire(&fp->writer_lock.lock, LW_EXCLUSIVE);

		/*
		 * Collect the live before-image pointers for this transaction's
		 * shared ops in this partition.  We are the single owner of these
		 * frees: this runs under the partition writer lock, so the UNDO
		 * worker's SLogTupleRemoveByXidGlobal (which also takes the writer
		 * lock and only frees still-valid dps) cannot race us, and the
		 * MARK_ABORTED op below nulls before_image_dp so the worker sees
		 * InvalidDsaPointer afterward.  We record the dps now but free them
		 * only after the seqlock write cycle completes (seq even again), so
		 * no wait-free reader can dereference a freed pointer: any reader
		 * that copied the old dp fails its seq re-check and retries.
		 */
		{
			const SLogFlatHash *rht = fp->hash;

			for (tk = slog_tracked_keys; tk != NULL; tk = tk->next)
			{
				const SLogFlatBucket *bucket;

				if (!TransactionIdEquals(tk->xid, xid) || tk->local_only)
					continue;
				if (SLogFlatHashPartitionIndex(&tk->key) != part)
					continue;

				bucket = SLogFlatHashProbe(rht, &tk->key);
				if (bucket == NULL)
					continue;

				for (int j = 0; j < SLOG_MAX_TUPLE_OPS; j++)
				{
					if (bucket->entry.ops[j].in_use &&
						TransactionIdEquals(bucket->entry.ops[j].xid, xid) &&
						DsaPointerIsValid(bucket->entry.ops[j].before_image_dp))
					{
						if (nfreedps >= maxfreedps)
						{
							maxfreedps *= 2;
							freedps = (dsa_pointer *)
								repalloc(freedps,
										 maxfreedps * sizeof(dsa_pointer));
						}
						freedps[nfreedps++] =
							bucket->entry.ops[j].before_image_dp;
					}
				}
			}
		}

		SLogSeqWriteBegin(fp);

		/*
		 * Bound the seqlock odd-hold window on a very large rollback
		 * (hundreds of thousands of tuples).  While seq is odd every
		 * wait-free reader of this partition spins; ending and re-beginning
		 * the seqlock every SLOG_ABORT_PUBLISH_BATCH ops lets those readers
		 * make progress.  Correctness across the boundary: each tuple's
		 * abort visibility is resolved independently and each seq cycle
		 * leaves fp->hash consistent, so a reader that observes the even
		 * counter at a boundary sees a valid partial state.  The partition
		 * writer LWLock (held for the whole function) still serializes us
		 * against SLogTupleRemoveByXidGlobal, the only other before-image
		 * freer, so deferring the dsa_free calls until after the final
		 * SLogSeqWriteEnd remains safe.
		 */
		ops_since_publish = 0;

		/* Process backend-local INSERT-hash entries for this partition */
		if (slog_insert_tids != NULL)
		{
			sloginsert_iterator it;
			SLogInsertTidEntry *ie;

			sloginsert_start_iterate(slog_insert_tids, &it);
			while ((ie = sloginsert_iterate(slog_insert_tids, &it)) != NULL)
			{
				SLogFlatOp	flat_op;
				SLogTupleKey smkey;

				memset(&smkey, 0, sizeof(smkey));
				smkey.relid = ie->key.relid;
				ItemPointerSet(&smkey.tid,
							   SLOG_DECODE_BLKNO(ie->key.encoded_tid),
							   SLOG_DECODE_OFFNUM(ie->key.encoded_tid));

				/* Only process if this key belongs to current partition */
				if (SLogFlatHashPartitionIndex(&smkey) == part)
				{
					memset(&flat_op, 0, sizeof(flat_op));
					flat_op.kind = SLOG_FLAT_OP_CREATE_ABORTED;
					flat_op.key = smkey;
					flat_op.xid = xid;
					flat_op.subxid = InvalidTransactionId;
					SLogFlatHashApply(fp->hash, &flat_op, sizeof(flat_op));

					if (++ops_since_publish >= SLOG_ABORT_PUBLISH_BATCH)
					{
						SLogSeqWriteEnd(fp);
						SLogSeqWriteBegin(fp);
						ops_since_publish = 0;
					}
				}
			}
		}

		/* Process linked-list entries for this partition */
		for (tk = slog_tracked_keys; tk != NULL; tk = tk->next)
		{
			SLogFlatOp	flat_op;

			if (!TransactionIdEquals(tk->xid, xid))
				continue;
			if (SLogFlatHashPartitionIndex(&tk->key) != part)
				continue;

			memset(&flat_op, 0, sizeof(flat_op));
			if (tk->local_only)
			{
				flat_op.kind = SLOG_FLAT_OP_CREATE_ABORTED;
				flat_op.key = tk->key;
				flat_op.xid = xid;
				flat_op.subxid = tk->subxid;
			}
			else
			{
				flat_op.kind = SLOG_FLAT_OP_MARK_ABORTED;
				flat_op.key = tk->key;
				flat_op.xid = xid;
			}

			SLogFlatHashApply(fp->hash, &flat_op, sizeof(flat_op));

			if (++ops_since_publish >= SLOG_ABORT_PUBLISH_BATCH)
			{
				SLogSeqWriteEnd(fp);
				SLogSeqWriteBegin(fp);
				ops_since_publish = 0;
			}
		}

		SLogSeqWriteEnd(fp);

		/*
		 * The seqlock cycle has closed (seq even), so no wait-free reader
		 * can still hold a live copy of the now-nulled before-image
		 * pointers.  Free them while still holding the writer lock
		 * (single-owner guarantee).
		 */
		if (nfreedps > 0)
		{
			SLogEnsureDsaAttached();
			for (int j = 0; j < nfreedps; j++)
				dsa_free(slog_dsa_handle, freedps[j]);
			nfreedps = 0;
		}

		LWLockRelease(&fp->writer_lock.lock);
	}

	pfree(freedps);
}

/*
 * SLogTupleRemoveByXidGlobal
 *		Remove ALL ops for a transaction by scanning the shared flat hash.
 *
 * Unlike SLogTupleRemoveByXid, this does not use the backend-local tracking
 * list (which doesn't exist in the UNDO worker process).  Used by the UNDO
 * worker to clean up ABORTED entries after UNDO has been applied.
 */
void
SLogTupleRemoveByXidGlobal(TransactionId xid)
{
	SLogTupleKey *collected_keys;
	dsa_pointer *collected_dps;
	int			max_keys;
	int			part;

	if (SLogState == NULL)
		return;

	max_keys = SLogTupleNumEntries();
	if (max_keys <= 0)
		return;

	SLogEnsureDsaAttached();

	collected_keys = (SLogTupleKey *)
		palloc(sizeof(SLogTupleKey) * max_keys);
	collected_dps = (dsa_pointer *)
		palloc(sizeof(dsa_pointer) * max_keys);

	/*
	 * Process each partition while holding its exclusive writer lock.  The
	 * writer lock serializes all writers for the partition, which is
	 * essential: this function (run by the UNDO worker) and the inline
	 * SLogTupleCleanupRetained / abort paths run concurrently and would
	 * otherwise collect the same before_image_dp under wait-free reads and
	 * dsa_free() it more than once (a double-free that trips the dsa.c
	 * superblock/alignment assertions).
	 *
	 * Holding the LWLock guarantees no other writer mutates this partition,
	 * so the single copy is stable while we scan it.  We apply REMOVE_XID
	 * (which nulls before_image_dp) inside one seqlock cycle, then free the
	 * before-images after the cycle closes (seq even) so no wait-free reader
	 * can act on a freed dp.  Each dp is freed exactly once.
	 */
	for (part = 0; part < SLogNumPartitions; part++)
	{
		SLogFlatPartition *fp = SLogGetPartitionByIndex(part);
		SLogFlatHashScanState scan;
		const SLogFlatBucket *bucket;
		int			nkeys = 0;
		int			ndps = 0;
		int			i;

		LWLockAcquire(&fp->writer_lock.lock, LW_EXCLUSIVE);

		/* Scan the single copy; stable because we hold the writer lock. */
		SLogFlatHashScanInit(&scan);
		while ((bucket = SLogFlatHashScanNext(fp->hash, &scan)) != NULL)
		{
			const SLogTupleEntry *entry = &bucket->entry;

			for (i = 0; i < SLOG_MAX_TUPLE_OPS; i++)
			{
				if (entry->ops[i].in_use &&
					TransactionIdEquals(entry->ops[i].xid, xid))
				{
					if (nkeys < max_keys)
					{
						collected_keys[nkeys++] = bucket->key;
						if (DsaPointerIsValid(entry->ops[i].before_image_dp))
							collected_dps[ndps++] = entry->ops[i].before_image_dp;
					}
					break;
				}
			}
		}

		/* Null the dangling pointers and drop the slots in one seq cycle. */
		if (nkeys > 0)
		{
			SLogSeqWriteBegin(fp);
			for (i = 0; i < nkeys; i++)
			{
				SLogFlatOp	flat_op;

				memset(&flat_op, 0, sizeof(flat_op));
				flat_op.kind = SLOG_FLAT_OP_REMOVE_XID;
				flat_op.key = collected_keys[i];
				flat_op.xid = xid;
				SLogFlatHashApply(fp->hash, &flat_op, sizeof(flat_op));
			}
			SLogSeqWriteEnd(fp);
		}

		/*
		 * Free before-images AFTER the seq cycle closed the mutation.  With a
		 * single copy we must not free before the REMOVE_XID nulled the
		 * pointer and seq returned even: a concurrent wait-free reader may
		 * have copied the old before_image_dp into a local, and only the seq
		 * re-check (which now fails) prevents it from acting on a freed dp.
		 * Done under the writer lock (outside any spinlock, since dsa_free may
		 * take an LWLock); each dp is freed exactly once.
		 */
		for (i = 0; i < ndps; i++)
			dsa_free(slog_dsa_handle, collected_dps[i]);

		LWLockRelease(&fp->writer_lock.lock);
	}

	pfree(collected_keys);
	pfree(collected_dps);
}

/*
 * SLogTupleIterateByTid
 *		Call a callback for each active operation on a tuple.
 *
 * WAIT-FREE: uses the seqlock read-side.  The in-use ops are copied into a
 * local array under the seqlock retry loop; the callback then runs after a
 * consistent read, so it may have side effects and receives pointers into
 * the local copy (valid for the duration of this call).
 */
void
SLogTupleIterateByTid(Oid relid, ItemPointer tid,
					  SLogTupleIterCallback callback, void *arg)
{
	SLogTupleKey key;
	SLogFlatPartition *fp;
	SLogTupleOp	ops[SLOG_MAX_TUPLE_OPS];
	int			nops = 0;
	uint32		slog_seq_;
	int			i;

	memset(&key, 0, sizeof(key));
	key.relid = relid;
	ItemPointerCopy(tid, &key.tid);

	fp = SLogGetPartition(&key);

	SLOG_SEQ_READ_BEGIN(fp, slog_seq_)
	{
		const SLogFlatBucket *bucket = SLogFlatHashProbe(fp->hash, &key);

		nops = 0;				/* reset: body may re-run on retry */
		if (bucket != NULL)
		{
			const SLogTupleEntry *entry = &bucket->entry;

			for (i = 0; i < SLOG_MAX_TUPLE_OPS; i++)
				if (entry->ops[i].in_use)
					ops[nops++] = entry->ops[i];
		}
	}
	SLOG_SEQ_READ_END(fp, slog_seq_);

	/* Consistent read complete; run the (side-effecting) callback. */
	for (i = 0; i < nops; i++)
		if (!callback(&ops[i], arg))
			break;
}

/* ----------------------------------------------------------------
 * Convenience wrappers
 * ----------------------------------------------------------------
 */

/*
 * SLogTupleHasEntry -- quick probe: does ANY active entry exist for this TID?
 * WAIT-FREE: uses the seqlock read-side.
 */
bool
SLogTupleHasEntry(Oid relid, ItemPointer tid)
{
	SLogTupleKey key;
	SLogFlatPartition *fp;
	bool		has_entry = false;
	uint32		slog_seq_;

	memset(&key, 0, sizeof(key));
	key.relid = relid;
	ItemPointerCopy(tid, &key.tid);

	fp = SLogGetPartition(&key);

	SLOG_SEQ_READ_BEGIN(fp, slog_seq_)
	{
		const SLogFlatBucket *bucket = SLogFlatHashProbe(fp->hash, &key);

		has_entry = (bucket != NULL && bucket->entry.nops > 0);
	}
	SLOG_SEQ_READ_END(fp, slog_seq_);

	return has_entry;
}

/*
 * SLogTupleIsInsertedByMe -- check if current transaction inserted this tuple.
 *
 * Checks both the shared hash (normal case) and the backend-local tracking
 * list (for local-only INSERTs that have no shared entry).
 *
 * Uses the top-level XID because SLogTupleTrackLocalOnly() always stores
 * GetTopTransactionId().  This ensures correct results even when called
 * from within a subtransaction (savepoint).
 */
bool
SLogTupleIsInsertedByMe(Oid relid, ItemPointer tid)
{
	SLogTupleOp op;
	int			nfound;
	SLogTrackedKey *tk;
	TransactionId myxid = GetTopTransactionIdIfAny();

	if (!TransactionIdIsValid(myxid))
		return false;

	/*
	 * Special-marker TIDs (SpecTokenOffsetNumber 0xFFFE for speculative
	 * insertions, MovedPartitionsOffsetNumber 0xFFFD for cross-partition
	 * moves) can appear in a tuple's on-page t_ctid.  All sLog entries are
	 * keyed by an on-page (block, offnum) with offnum <= MaxOffsetNumber,
	 * so this backend cannot have "inserted" the tuple under such a key.
	 * Bail out early to avoid tripping the encode-guard elog(ERROR) below.
	 */
	if (ItemPointerGetOffsetNumber(tid) > MaxOffsetNumber)
		return false;

	/* Check shared hash first (normal case) */
	nfound = SLogTupleLookupFiltered(relid, tid, myxid, &op, 1);
	if (nfound > 0 && op.op_type == SLOG_OP_INSERT)
		return true;

	/*
	 * Check the backend-local INSERT hash (top-level local-only INSERTs).
	 */
	{
		BlockNumber blkno = ItemPointerGetBlockNumber(tid);
		OffsetNumber offnum = ItemPointerGetOffsetNumber(tid);
		SLogInsertTidKey ikey;

		if (offnum > MaxOffsetNumber)
			elog(ERROR, "offset %u exceeds page item limit %d in sLog TID encoding",
				 offnum, MaxOffsetNumber);
		ikey.relid = relid;
		ikey.encoded_tid = SLOG_ENCODE_TID(blkno, offnum);
		if (slog_insert_tids != NULL &&
			sloginsert_lookup(slog_insert_tids, ikey) != NULL)
			return true;
	}

	/*
	 * Fall back to backend-local tracking list.  This handles the overflow
	 * case where SLogTupleInsert returned false (hash full) but the INSERT
	 * was tracked locally via SLogTupleTrackLocalOnly or SLogTupleTrackKey.
	 * Also handles subtransaction local-only entries (which still use the
	 * linked list).
	 */
	for (tk = slog_tracked_keys; tk != NULL; tk = tk->next)
	{
		if (!TransactionIdEquals(tk->xid, myxid))
			continue;
		if (tk->key.relid != relid)
			continue;
		if (ItemPointerEquals(&tk->key.tid, tid))
			return true;
	}

	return false;
}

/*
 * SLogTupleIsDeletedByMe -- check if current transaction deleted this tuple.
 */
bool
SLogTupleIsDeletedByMe(Oid relid, ItemPointer tid)
{
	SLogTupleOp op;
	int			nfound;
	TransactionId myxid = GetCurrentTransactionIdIfAny();

	if (!TransactionIdIsValid(myxid))
		return false;

	nfound = SLogTupleLookupFiltered(relid, tid, myxid, &op, 1);
	return (nfound > 0 &&
			(op.op_type == SLOG_OP_DELETE ||
			 op.op_type == SLOG_OP_UPDATE));
}

/*
 * SLogTupleGetDirtyXid -- for SNAPSHOT_DIRTY, get the xid of the in-progress
 * transaction operating on this tuple.
 *
 * Returns the xid of the first in-progress INSERT or DELETE/UPDATE entry
 * found (excluding our own), or InvalidTransactionId if none.
 *
 * WAIT-FREE: uses the seqlock read-side.  The ops are snapshotted into a
 * local array inside the retry loop; the (LWLock-taking) transaction-status
 * checks then run after a consistent read, never inside the retry loop.
 */
TransactionId
SLogTupleGetDirtyXid(Oid relid, ItemPointer tid, bool *is_insert)
{
	SLogTupleKey key;
	SLogFlatPartition *fp;
	SLogTupleOp	ops[SLOG_MAX_TUPLE_OPS];
	int			nops = 0;
	TransactionId result = InvalidTransactionId;
	uint32		slog_seq_;
	int			i;

	memset(&key, 0, sizeof(key));
	key.relid = relid;
	ItemPointerCopy(tid, &key.tid);

	fp = SLogGetPartition(&key);

	SLOG_SEQ_READ_BEGIN(fp, slog_seq_)
	{
		const SLogFlatBucket *bucket = SLogFlatHashProbe(fp->hash, &key);

		nops = 0;				/* reset: body may re-run on retry */
		if (bucket != NULL)
		{
			const SLogTupleEntry *entry = &bucket->entry;

			for (i = 0; i < SLOG_MAX_TUPLE_OPS; i++)
				if (entry->ops[i].in_use)
					ops[nops++] = entry->ops[i];
		}
	}
	SLOG_SEQ_READ_END(fp, slog_seq_);

	/* Consistent snapshot taken; resolve status outside the retry loop. */
	for (i = 0; i < nops; i++)
	{
		TransactionId xid = ops[i].xid;
		SLogOpType	op = ops[i].op_type;

		if (TransactionIdIsCurrentTransactionId(xid))
			continue;
		if (!TransactionIdIsInProgress(xid))
			continue;

		if (is_insert)
			*is_insert = (op == SLOG_OP_INSERT);
		result = xid;
		break;
	}

	return result;
}

/*
 * SLogTupleGetDirtyWriterXid -- like SLogTupleGetDirtyXid, but returns only
 * the xid of an in-progress *writer* (INSERT/UPDATE/DELETE), ignoring
 * lock-only markers (LOCK_SHARE/LOCK_EXCL) and aborted markers.
 *
 * Used at the write-wait decision in the UPDATE/DELETE paths.  An updater
 * already holds the heavyweight LOCKTAG_TUPLE lock (LockTupleNoKeyExclusive ->
 * ExclusiveLock), which serializes against conflicting lockers via the
 * standard lock matrix: a key-share locker (AccessShareLock) is compatible and
 * does not block, while a share/exclusive locker conflicts and blocks the
 * updater on the lock manager.  XactLockTableWait must therefore fire only for
 * an actual in-progress writer -- never for a pure locker.  Waiting on a
 * locker's xid here while that locker is queued behind us for the same tuple
 * ExclusiveLock manufactures a deadlock cycle that heap avoids via
 * HEAP_XMAX_IS_LOCKED_ONLY.
 *
 * WAIT-FREE: uses the seqlock read-side, identical to SLogTupleGetDirtyXid.
 */
TransactionId
SLogTupleGetDirtyWriterXid(Oid relid, ItemPointer tid, bool *is_insert)
{
	SLogTupleKey key;
	SLogFlatPartition *fp;
	SLogTupleOp	ops[SLOG_MAX_TUPLE_OPS];
	int			nops = 0;
	TransactionId result = InvalidTransactionId;
	uint32		slog_seq_;
	int			i;

	memset(&key, 0, sizeof(key));
	key.relid = relid;
	ItemPointerCopy(tid, &key.tid);

	fp = SLogGetPartition(&key);

	SLOG_SEQ_READ_BEGIN(fp, slog_seq_)
	{
		const SLogFlatBucket *bucket = SLogFlatHashProbe(fp->hash, &key);

		nops = 0;				/* reset: body may re-run on retry */
		if (bucket != NULL)
		{
			const SLogTupleEntry *entry = &bucket->entry;

			for (i = 0; i < SLOG_MAX_TUPLE_OPS; i++)
				if (entry->ops[i].in_use)
					ops[nops++] = entry->ops[i];
		}
	}
	SLOG_SEQ_READ_END(fp, slog_seq_);

	/* Consistent snapshot taken; resolve status outside the retry loop. */
	for (i = 0; i < nops; i++)
	{
		TransactionId xid;
		SLogOpType	op = ops[i].op_type;

		/* Only real writers block another writer. */
		if (op != SLOG_OP_INSERT &&
			op != SLOG_OP_UPDATE &&
			op != SLOG_OP_DELETE)
			continue;

		xid = ops[i].xid;

		if (TransactionIdIsCurrentTransactionId(xid))
			continue;
		if (!TransactionIdIsInProgress(xid))
			continue;

		if (is_insert)
			*is_insert = (op == SLOG_OP_INSERT);
		result = xid;
		break;
	}

	return result;
}

/*
 * slog_tuplock_to_lockmode -- map a LockTupleMode to its heavyweight LOCKMODE.
 *
 * The four tuple-lock strengths MUST map to four distinct LOCKMODEs so the
 * real conflict matrix (DoLockModesConflict) can distinguish a compatible
 * KeyShare FK locker from a conflicting Share/Exclusive locker.
 */
static LOCKMODE
slog_tuplock_to_lockmode(LockTupleMode mode)
{
	switch (mode)
	{
		case LockTupleKeyShare:
			return AccessShareLock;
		case LockTupleShare:
			return RowShareLock;
		case LockTupleNoKeyExclusive:
			return ExclusiveLock;
		case LockTupleExclusive:
			return AccessExclusiveLock;
	}
	elog(ERROR, "invalid tuple lock mode: %d", (int) mode);
	return NoLock;				/* keep compiler quiet */
}

/*
 * SLogTupleGetWriteConflictXid -- find an in-progress transaction whose marker
 * conflicts with a writer (UPDATE/DELETE) acquiring tuple lock my_mode.
 *
 * Unlike SLogTupleGetDirtyWriterXid (which only ever reports writers and
 * silently ignores lock-only markers), this also reports a *locker* whose
 * recorded LockTupleMode conflicts with my_mode under the standard heavyweight
 * matrix.  That is required for correctness: a SELECT ... FOR UPDATE locker
 * leaves only a LOCK_EXCL marker (no on-page writer state), and an updater that
 * consults a writer-only probe sails past it and clobbers the row the locker is
 * protecting.  The four-way mapping keeps a KeyShare FK locker (AccessShareLock)
 * compatible with a NoKeyExclusive UPDATE (ExclusiveLock) while making FOR SHARE
 * (RowShareLock) and FOR UPDATE (AccessExclusiveLock) correctly block it.
 *
 * Writers take priority over lockers in the returned xid so the caller's
 * is_insert handling (TM_Invisible for an in-progress INSERT) is preserved; a
 * conflicting locker is returned only when no in-progress writer exists.
 *
 * Returns the conflicting xid, or InvalidTransactionId if none.  *is_insert is
 * set true only when the returned xid is an in-progress INSERT writer.
 *
 * WAIT-FREE: uses the seqlock read-side, identical to SLogTupleGetDirtyWriterXid.
 */
TransactionId
SLogTupleGetWriteConflictXid(Oid relid, ItemPointer tid,
							 LockTupleMode my_mode, bool *is_insert)
{
	SLogTupleKey key;
	SLogFlatPartition *fp;
	SLogTupleOp	ops[SLOG_MAX_TUPLE_OPS];
	int			nops = 0;
	TransactionId writer_xid = InvalidTransactionId;
	bool		writer_is_insert = false;
	TransactionId locker_xid = InvalidTransactionId;
	LOCKMODE	my_lockmode = slog_tuplock_to_lockmode(my_mode);
	uint32		slog_seq_;
	int			i;

	memset(&key, 0, sizeof(key));
	key.relid = relid;
	ItemPointerCopy(tid, &key.tid);

	fp = SLogGetPartition(&key);

	SLOG_SEQ_READ_BEGIN(fp, slog_seq_)
	{
		const SLogFlatBucket *bucket = SLogFlatHashProbe(fp->hash, &key);

		nops = 0;				/* reset: body may re-run on retry */
		if (bucket != NULL)
		{
			const SLogTupleEntry *entry = &bucket->entry;

			for (i = 0; i < SLOG_MAX_TUPLE_OPS; i++)
				if (entry->ops[i].in_use)
					ops[nops++] = entry->ops[i];
		}
	}
	SLOG_SEQ_READ_END(fp, slog_seq_);

	/* Consistent snapshot taken; resolve status outside the retry loop. */
	for (i = 0; i < nops; i++)
	{
		TransactionId xid;
		SLogOpType	op = ops[i].op_type;

		xid = ops[i].xid;

		if (TransactionIdIsCurrentTransactionId(xid))
			continue;
		if (!TransactionIdIsInProgress(xid))
			continue;

		if (op == SLOG_OP_INSERT ||
			op == SLOG_OP_UPDATE ||
			op == SLOG_OP_DELETE)
		{
			/* A real writer: highest priority, stop scanning. */
			writer_xid = xid;
			writer_is_insert = (op == SLOG_OP_INSERT);
			break;
		}

		if (op == SLOG_OP_LOCK_SHARE || op == SLOG_OP_LOCK_EXCL)
		{
			LOCKMODE	locker_lockmode =
				slog_tuplock_to_lockmode(ops[i].lock_mode);

			if (DoLockModesConflict(my_lockmode, locker_lockmode))
				locker_xid = xid;	/* candidate; keep seeking a writer */
		}
	}

	if (TransactionIdIsValid(writer_xid))
	{
		if (is_insert)
			*is_insert = writer_is_insert;
		return writer_xid;
	}

	if (is_insert)
		*is_insert = false;
	return locker_xid;
}

/*
 * SLogTupleHasLockConflict -- check if any active lock entries on this TID
 * conflict with the requested lock mode.
 */
bool
SLogTupleHasLockConflict(Oid relid, ItemPointer tid,
						 TransactionId my_xid,
						 SLogOpType requested_lock)
{
	SLogTupleOp ops[SLOG_MAX_TUPLE_OPS];
	int			nfound;
	int			i;

	nfound = SLogTupleLookupFiltered(relid, tid, InvalidTransactionId,
									 ops, SLOG_MAX_TUPLE_OPS);

	for (i = 0; i < nfound; i++)
	{
		if (TransactionIdEquals(ops[i].xid, my_xid))
			continue;
		if (!TransactionIdIsInProgress(ops[i].xid))
			continue;

		/* Only lock/mutating entries can conflict */
		if (ops[i].op_type != SLOG_OP_LOCK_SHARE &&
			ops[i].op_type != SLOG_OP_LOCK_EXCL &&
			ops[i].op_type != SLOG_OP_DELETE &&
			ops[i].op_type != SLOG_OP_UPDATE)
			continue;

		/*
		 * Lock compatibility matrix:
		 *   SHARE vs SHARE: compatible
		 *   SHARE vs EXCL/DELETE/UPDATE: conflict
		 *   EXCL vs anything: conflict
		 */
		if (requested_lock == SLOG_OP_LOCK_SHARE)
		{
			if (ops[i].op_type == SLOG_OP_LOCK_EXCL ||
				ops[i].op_type == SLOG_OP_DELETE ||
				ops[i].op_type == SLOG_OP_UPDATE)
				return true;
		}
		else if (requested_lock == SLOG_OP_LOCK_EXCL)
		{
			return true;
		}
	}

	return false;
}

/*
 * SLogTupleGetLockConflictXid -- like SLogTupleHasLockConflict, but also
 * returns the xid of the *conflicting* transaction (the one whose marker
 * actually conflicts with requested_lock under the matrix above).
 *
 * The waiter must XactLockTableWait on the conflicting xid specifically.
 * Waiting on the first in-progress xid found on the TID (as a broad
 * SLogTupleGetDirtyXid probe would return) can pick a compatible peer -- e.g.
 * another KeyShare locker -- which is not blocking us and may itself be queued
 * behind us, manufacturing a spurious mutual-wait deadlock cycle.
 *
 * Returns true and sets *xid_out if a conflicting in-progress transaction
 * exists; returns false otherwise (xid_out is set to InvalidTransactionId).
 */
bool
SLogTupleGetLockConflictXid(Oid relid, ItemPointer tid,
							TransactionId my_xid,
							SLogOpType requested_lock,
							TransactionId *xid_out)
{
	SLogTupleOp ops[SLOG_MAX_TUPLE_OPS];
	int			nfound;
	int			i;

	*xid_out = InvalidTransactionId;

	nfound = SLogTupleLookupFiltered(relid, tid, InvalidTransactionId,
									 ops, SLOG_MAX_TUPLE_OPS);

	for (i = 0; i < nfound; i++)
	{
		if (TransactionIdEquals(ops[i].xid, my_xid))
			continue;
		if (!TransactionIdIsInProgress(ops[i].xid))
			continue;

		/* Only lock/mutating entries can conflict */
		if (ops[i].op_type != SLOG_OP_LOCK_SHARE &&
			ops[i].op_type != SLOG_OP_LOCK_EXCL &&
			ops[i].op_type != SLOG_OP_DELETE &&
			ops[i].op_type != SLOG_OP_UPDATE)
			continue;

		/* Same matrix as SLogTupleHasLockConflict. */
		if (requested_lock == SLOG_OP_LOCK_SHARE)
		{
			if (ops[i].op_type == SLOG_OP_LOCK_EXCL ||
				ops[i].op_type == SLOG_OP_DELETE ||
				ops[i].op_type == SLOG_OP_UPDATE)
			{
				*xid_out = ops[i].xid;
				return true;
			}
		}
		else if (requested_lock == SLOG_OP_LOCK_EXCL)
		{
			*xid_out = ops[i].xid;
			return true;
		}
	}

	return false;
}

/*
 * SLogTupleHasAbortedEntry -- check if any aborted sLog op exists for a TID.
 */
bool
SLogTupleHasAbortedEntry(Oid relid, ItemPointer tid)
{
	SLogTupleOp ops[SLOG_MAX_TUPLE_OPS];
	int			nfound;
	int			i;

	nfound = SLogTupleLookupFiltered(relid, tid, InvalidTransactionId,
									 ops, SLOG_MAX_TUPLE_OPS);

	for (i = 0; i < nfound; i++)
	{
		/* Explicitly marked ABORTED */
		if (ops[i].op_type == SLOG_OP_ABORTED)
			return true;

		/* Skip our own transaction's entries for CLOG fallback */
		if (TransactionIdIsCurrentTransactionId(ops[i].xid))
			continue;

		/* CLOG fallback: completed but did not commit => aborted */
		if (!TransactionIdIsInProgress(ops[i].xid) &&
			TransactionIdDidAbort(ops[i].xid))
			return true;
	}

	return false;
}

/* ----------------------------------------------------------------
 * Backend-private tracking for tuple sLog cleanup
 * ----------------------------------------------------------------
 */

/*
 * SLogTupleTrackKey
 *		Remember a tuple key for cleanup at commit/abort.
 *
 * Allocated in TopTransactionContext so it's automatically freed
 * when the transaction ends.
 */
void
SLogTupleTrackKey(SLogTupleKey key, TransactionId xid, TransactionId subxid,
				  SLogOpType op_type)
{
	MemoryContext oldcxt;
	SLogTrackedKey *tk;

	oldcxt = MemoryContextSwitchTo(TopTransactionContext);

	tk = (SLogTrackedKey *) palloc(sizeof(SLogTrackedKey));
	memcpy(&tk->key, &key, sizeof(SLogTupleKey));
	tk->xid = xid;
	tk->subxid = subxid;
	tk->local_only = false;
	slog_has_shared_entries = true;
	tk->op_type = op_type;
	tk->before_image = NULL;
	tk->before_image_len = 0;
	tk->before_flags = 0;
	tk->before_commit_ts = 0;
	tk->before_image_dp = InvalidDsaPointer;
	tk->next = slog_tracked_keys;
	slog_tracked_keys = tk;

	MemoryContextSwitchTo(oldcxt);
}

/*
 * SLogTupleTrackLocalOnly
 *		Lightweight local-only tracking (INSERTs only).
 *
 * Records (relid, tid, xid, subxid) in the per-backend local list WITHOUT
 * creating a shared sLog entry.  On subtransaction abort,
 * SLogTupleRemoveBySubXid will create a shared ABORTED entry for visibility.
 *
 * OOM optimization: When not inside a subtransaction, records the TID in a
 * backend-local open-addressing hash (simplehash) keyed by (relid,
 * encoded_tid) instead of a 136-byte linked-list node, giving O(1) insert
 * and probe and bounded per-backend memory under bulk INSERT.  Subtransaction
 * entries still use the linked list because subtxn abort needs per-entry
 * subxid filtering.
 */
void
SLogTupleTrackLocalOnly(Oid relid, ItemPointer tid,
						TransactionId xid, TransactionId subxid)
{
	MemoryContext oldcxt;

	/*
	 * Fast path: top-level transaction with no savepoint → use the hash.
	 * The subxid is InvalidTransactionId in this case (top-level INSERTs
	 * always pass the top xid as both xid and subxid=Invalid).
	 */
	if (!IsSubTransaction())
	{
		BlockNumber blkno = ItemPointerGetBlockNumber(tid);
		OffsetNumber offnum = ItemPointerGetOffsetNumber(tid);
		SLogInsertTidKey ikey;
		bool		found;

		if (offnum > MaxOffsetNumber)
			elog(ERROR, "offset %u exceeds page item limit %d in sLog TID encoding",
				 offnum, MaxOffsetNumber);
		ikey.relid = relid;
		ikey.encoded_tid = SLOG_ENCODE_TID(blkno, offnum);

		/*
		 * Record the TID in the backend-local hash.  Insert is amortized
		 * O(1); the table and its entries live in TopTransactionContext and
		 * are destroyed at transaction end.
		 */
		(void) sloginsert_insert(slog_insert_tids_ensure(), ikey, &found);
		return;
	}

	{
		SLogTrackedKey *tk;
		SLogTupleKey key;

		memset(&key, 0, sizeof(key));
		key.relid = relid;
		ItemPointerCopy(tid, &key.tid);

		oldcxt = MemoryContextSwitchTo(TopTransactionContext);

		tk = (SLogTrackedKey *) palloc(sizeof(SLogTrackedKey));
		memcpy(&tk->key, &key, sizeof(SLogTupleKey));
		tk->xid = xid;
		tk->subxid = subxid;
		tk->local_only = true;
		tk->op_type = SLOG_OP_INSERT;
		tk->before_image = NULL;
		tk->before_image_len = 0;
		tk->before_flags = 0;
		tk->before_commit_ts = 0;
		tk->before_image_dp = InvalidDsaPointer;
		tk->next = slog_tracked_keys;
		slog_tracked_keys = tk;

		MemoryContextSwitchTo(oldcxt);
	}
}

/*
 * SLogTupleUntrackLocalOnly
 *		Remove local-only INSERT tracking for (relid, tid).
 *
 * Counterpart to SLogTupleTrackLocalOnly.  Used when a freshly inserted tuple
 * must NOT be stamped with the inserting transaction's commit HLC at commit
 * time -- e.g. VACUUM FULL / CLUSTER copies a recently-dead tombstone into the
 * new relation and rewrites it with its ORIGINAL delete timestamp.  Leaving the
 * INSERT tracked would let recno_stamp_tuple_committed clobber t_commit_ts with
 * the rewrite transaction's commit HLC, resurrecting the deleted row for any
 * reader whose snapshot predates that commit.
 *
 * Clears the backend-local INSERT-hash entry on the top-level fast path and
 * also drops any matching linked-list node (the savepoint/fallback path).
 */
void
SLogTupleUntrackLocalOnly(Oid relid, ItemPointer tid)
{
	BlockNumber blkno = ItemPointerGetBlockNumber(tid);
	OffsetNumber offnum = ItemPointerGetOffsetNumber(tid);
	SLogTrackedKey **link;

	if (offnum > MaxOffsetNumber)
		elog(ERROR, "offset %u exceeds page item limit %d in sLog TID encoding",
			 offnum, MaxOffsetNumber);

	/* Fast path: drop the backend-local INSERT-hash entry if present. */
	if (slog_insert_tids != NULL)
	{
		SLogInsertTidKey ikey;

		ikey.relid = relid;
		ikey.encoded_tid = SLOG_ENCODE_TID(blkno, offnum);
		(void) sloginsert_delete(slog_insert_tids, ikey);
	}

	/* Fallback/subtxn path: unlink any matching local-only INSERT node. */
	link = &slog_tracked_keys;
	while (*link != NULL)
	{
		SLogTrackedKey *tk = *link;

		if (tk->local_only &&
			tk->key.relid == relid &&
			ItemPointerGetBlockNumber(&tk->key.tid) == blkno &&
			ItemPointerGetOffsetNumber(&tk->key.tid) == offnum)
		{
			*link = tk->next;
			pfree(tk);
			continue;
		}
		link = &tk->next;
	}
}

/*
 * SLogTupleStoreBeforeImage
 *		Attach a before-image to the most recent tracked key for the given
 *		(relid, tid, xid) combination.
 *
 * This is called during DELETE and UPDATE operations to stash the original
 * tuple data before in-place modification.  On subtransaction abort,
 * RecnoRestoreBeforeImages() uses this data to physically restore the tuple.
 *
 * The before-image is allocated in TopTransactionContext so it survives
 * subtransaction rollback.  Memory is freed when the tracked key list is
 * reset at top-level transaction end.
 *
 * Size cap: if the tuple is larger than RECNO_BEFORE_IMAGE_MAX_SIZE (64KB),
 * we skip storing the before-image.  On savepoint rollback for such tuples,
 * the tuple cannot be restored and the operation will raise an error.
 */
#define RECNO_BEFORE_IMAGE_MAX_SIZE		(64 * 1024)

void
SLogTupleStoreBeforeImage(Oid relid, ItemPointer tid, TransactionId xid,
						  const char *data, int len,
						  uint16 flags, uint64 commit_ts,
						  RelFileLocator rlocator, char relpersistence)
{
	SLogTrackedKey *tk;
	MemoryContext oldcxt;

	/* Enforce size cap */
	if (len > RECNO_BEFORE_IMAGE_MAX_SIZE)
		return;

	/* Find the matching tracked key (most recently added = list head) */
	for (tk = slog_tracked_keys; tk != NULL; tk = tk->next)
	{
		if (!TransactionIdEquals(tk->xid, xid))
			continue;
		if (tk->key.relid != relid)
			continue;
		if (!ItemPointerEquals(&tk->key.tid, tid))
			continue;

		/* Found it — store local copy for savepoint rollback */
		oldcxt = MemoryContextSwitchTo(TopTransactionContext);

		tk->before_image = palloc(len);
		memcpy(tk->before_image, data, len);
		tk->before_image_len = len;
		tk->before_flags = flags;
		tk->before_commit_ts = commit_ts;
		tk->before_rlocator = rlocator;
		tk->before_relpersistence = relpersistence;

		MemoryContextSwitchTo(oldcxt);

		/*
		 * Only the local per-backend before-image is stored -- consumed by
		 * intra-transaction savepoint rollback (RecnoRestoreBeforeImages).
		 * Cross-backend MVCC reads no longer need a shared before-image:
		 * WS-PVS3 walks the durable UNDO fork chain via
		 * RecnoReconstructVisibleVersion, and the write-write conflict
		 * probe (RecnoTupleHasCommittedUpdateAfter) resolves the head
		 * verptr in the same fork.
		 */
		tk->before_image_dp = InvalidDsaPointer;

		return;
	}

	/* Tracked key not found — this shouldn't happen, but is non-fatal */
	elog(WARNING, "SLogTupleStoreBeforeImage: no tracked key for rel %u tid (%u,%u) xid %u",
		 relid, ItemPointerGetBlockNumber(tid),
		 ItemPointerGetOffsetNumber(tid), xid);
}

/*
 * SLogTupleIterateTrackedKeysForSubXid
 *		Iterate over tracked keys matching a given xid AND subxid.
 *
 * This is used by RecnoRestoreBeforeImages to find entries that need
 * physical restoration during savepoint rollback.
 */
void
SLogTupleIterateTrackedKeysForSubXid(TransactionId xid,
									 TransactionId subxid,
									 SLogTrackedKeyCallback callback,
									 void *arg)
{
	SLogTrackedKey *tk;

	for (tk = slog_tracked_keys; tk != NULL; tk = tk->next)
	{
		if (!TransactionIdEquals(tk->xid, xid))
			continue;
		if (tk->subxid != subxid)
			continue;

		if (!callback(&tk->key, tk->xid, tk->subxid, tk->local_only, arg))
			break;
	}
}

/*
 * SLogTupleGetBeforeImage
 *		Retrieve the before-image for a specific tracked key.
 *
 * Returns true if a before-image is available, filling in the output params.
 * Returns false if no before-image was stored (e.g., INSERT, or tuple was
 * too large).
 */
bool
SLogTupleGetBeforeImage(Oid relid, ItemPointer tid, TransactionId xid,
						TransactionId subxid,
						char **data_out, int *len_out,
						uint16 *flags_out, uint64 *commit_ts_out,
						RelFileLocator *rlocator_out, char *relpersistence_out)
{
	SLogTrackedKey *tk;

	for (tk = slog_tracked_keys; tk != NULL; tk = tk->next)
	{
		if (!TransactionIdEquals(tk->xid, xid))
			continue;
		if (tk->subxid != subxid)
			continue;
		if (tk->key.relid != relid)
			continue;
		if (!ItemPointerEquals(&tk->key.tid, tid))
			continue;

		if (tk->before_image == NULL)
			return false;

		*data_out = tk->before_image;
		*len_out = tk->before_image_len;
		*flags_out = tk->before_flags;
		*commit_ts_out = tk->before_commit_ts;
		*rlocator_out = tk->before_rlocator;
		*relpersistence_out = tk->before_relpersistence;
		return true;
	}

	return false;
}

/*
 * SLogTupleCleanupRetained
 *		Free retained sLog entries that are no longer visible to any
 *		active snapshot.
 *
 * WS-PVS3: committed UPDATE markers are no longer retained (commit_xid
 * apply removes them), so under steady-state this routine primarily
 * cleans up other retained state.  Reclamation gates on the xid horizon
 * (GetOldestNonRemovableTransactionId) so a marker is reclaimable only
 * once its committing xid precedes the oldest active snapshot's xmin.
 *
 * Scans the flat hash under read-side, then applies CLEANUP_RETAINED ops.
 * Walks every partition taking each writer lock LW_EXCLUSIVE, so callers MUST
 * NOT hold a buffer content lock or other page-level critical section across
 * this call.  Driven by the UNDO background worker, or by
 * SLogTupleMaybeCleanupRetained() when the worker is disabled.  The
 * reclamation decision is the xid horizon (GetOldestNonRemovableTransactionId)
 * computed below.
 */
void
SLogTupleCleanupRetained(void)
{
	SLogTupleKey *collected_keys;
	dsa_pointer *collected_dps;
	int			max_keys = 256;
	int			part;
	int			i;
	TransactionId reclaim_xid_horizon;

	if (SLogState == NULL)
		return;

	/*
	 * Compute the xid horizon once for this pass.  Both the read-side
	 * eligibility scan and the CLEANUP_RETAINED apply must gate on the same
	 * horizon so the set of before-image pointers we dsa_free() here exactly
	 * matches the set the apply nulls out (otherwise a dangling pointer or
	 * double free results).
	 */
	reclaim_xid_horizon = GetOldestNonRemovableTransactionId(NULL);
	if (!TransactionIdIsValid(reclaim_xid_horizon))
		return;

	SLogEnsureDsaAttached();

	collected_keys = (SLogTupleKey *)
		palloc(max_keys * sizeof(SLogTupleKey));
	collected_dps = (dsa_pointer *)
		palloc(max_keys * sizeof(dsa_pointer));

	/*
	 * Process each partition independently while holding its exclusive
	 * writer lock.  The writer lock serializes all writers (other cleanup
	 * runs and forward-path inserts) for the partition, which is essential:
	 * SLogTupleCleanupRetained may run concurrently in several backends (the
	 * UNDO worker plus any backend calling SLogTupleMaybeCleanupRetained), so
	 * without this mutual exclusion two backends would scan the same partition,
	 * collect the same before_image_dp, and dsa_free() it twice (a double-free
	 * that trips the index < DSA_MAX_SEGMENTS assertion in dsa.c).
	 *
	 * Holding the LWLock also guarantees no other writer mutates this
	 * partition, so the single copy is stable to scan.  We apply the
	 * CLEANUP_RETAINED ops (which null out the now-dangling pointers) inside
	 * seqlock cycles, then free the DSA before-images after the final cycle
	 * closes (seq even) so no wait-free reader can act on a freed dp
	 * (dsa_free may itself take an LWLock, so it runs outside any spinlock).
	 * Each before-image is therefore freed exactly once.
	 */
	for (part = 0; part < SLogNumPartitions; part++)
	{
		SLogFlatPartition *fp = SLogGetPartitionByIndex(part);
		SLogFlatHashScanState scan;
		const SLogFlatBucket *bucket;
		int			nkeys = 0;
		int			ndps = 0;

		LWLockAcquire(&fp->writer_lock.lock, LW_EXCLUSIVE);

		/*
		 * Scan the single copy to collect expired keys and their before-image
		 * pointers.  Stable because we hold the writer lock.
		 */
		SLogFlatHashScanInit(&scan);
		while ((bucket = SLogFlatHashScanNext(fp->hash, &scan)) != NULL)
		{
			const SLogTupleEntry *entry = &bucket->entry;
			bool		has_expired = false;

			for (i = 0; i < SLOG_MAX_TUPLE_OPS; i++)
			{
				if (!entry->ops[i].in_use)
					continue;
				if (entry->ops[i].op_type != SLOG_OP_UPDATE)
					continue;
				if (!TransactionIdIsValid(entry->ops[i].xid))
					continue;
				/*
				 * Reclaim once the xid precedes the oldest active snapshot's
				 * xmin (xid authority -- see the function header).  We do NOT
				 * gate on commit_hlc != 0: commit_hlc is a derived cache of
				 * CLOG state stamped lazily at PRE_COMMIT, and a missed stamp
				 * (under saturation) would otherwise pin a committed,
				 * below-horizon marker forever.  An HLC-horizon gate would
				 * race the PRE_COMMIT-stamp / ProcArray-exit skew and
				 * reintroduce the lost update; the xid horizon does not.
				 */
				if (!TransactionIdPrecedes(entry->ops[i].xid,
										   reclaim_xid_horizon))
					continue;

				if (!has_expired)
				{
					if (nkeys >= max_keys)
					{
						max_keys *= 2;
						collected_keys = (SLogTupleKey *)
							repalloc(collected_keys,
									 max_keys * sizeof(SLogTupleKey));
						collected_dps = (dsa_pointer *)
							repalloc(collected_dps,
									 max_keys * sizeof(dsa_pointer));
					}
					collected_keys[nkeys++] = bucket->key;
					has_expired = true;
				}

				if (DsaPointerIsValid(entry->ops[i].before_image_dp))
				{
					if (ndps >= max_keys)
					{
						max_keys *= 2;
						collected_keys = (SLogTupleKey *)
							repalloc(collected_keys,
									 max_keys * sizeof(SLogTupleKey));
						collected_dps = (dsa_pointer *)
							repalloc(collected_dps,
									 max_keys * sizeof(dsa_pointer));
					}
					collected_dps[ndps++] = entry->ops[i].before_image_dp;
				}
			}
		}

		/* Null the now-dangling pointers and drop the slots. */
		if (nkeys > 0)
		{
			int			ops_since_publish = 0;

			SLogSeqWriteBegin(fp);
			for (i = 0; i < nkeys; i++)
			{
				SLogFlatOp	flat_op;

				memset(&flat_op, 0, sizeof(flat_op));
				flat_op.kind = SLOG_FLAT_OP_CLEANUP_RETAINED;
				flat_op.key = collected_keys[i];
				flat_op.reclaim_xid_horizon = reclaim_xid_horizon;
				SLogFlatHashApply(fp->hash, &flat_op, sizeof(flat_op));

				/*
				 * Bound the seqlock odd-hold window so wait-free readers of
				 * this partition make progress.  On a hot row many retained
				 * UPDATE markers expire at once, so nkeys can be large; a
				 * single uninterrupted odd window would spin every reader
				 * for its whole duration.  Each key's reclamation is
				 * independent and each seq cycle leaves fp->hash consistent,
				 * so ending and re-beginning at a batch boundary is safe --
				 * the partition LWLock still serializes us against other
				 * before-image freers.
				 */
				if (++ops_since_publish >= SLOG_ABORT_PUBLISH_BATCH)
				{
					SLogSeqWriteEnd(fp);
					SLogSeqWriteBegin(fp);
					ops_since_publish = 0;
				}
			}
			SLogSeqWriteEnd(fp);
		}

		/*
		 * Free before-images AFTER the seq cycle closed: with a single copy a
		 * concurrent wait-free reader may have copied an old before_image_dp,
		 * and only its (now-failing) seq re-check stops it acting on a freed
		 * dp.  dsa_free may take an LWLock, so it runs outside any spinlock.
		 */
		for (i = 0; i < ndps; i++)
		{
			if (DsaPointerIsValid(collected_dps[i]))
				dsa_free(slog_dsa_handle, collected_dps[i]);
		}

		LWLockRelease(&fp->writer_lock.lock);
	}

	pfree(collected_keys);
	pfree(collected_dps);
}

/*
 * SLogTupleMaybeCleanupRetained
 *		Throttled, self-clocking entry point for retained-entry cleanup.
 *
 * Intended for access methods to call from their DML paths when the UNDO
 * background worker is disabled (max_logical_revert_workers = 0), so the DSA
 * before-image area does not fill up under sustained high-TPS load.  Unlike
 * SLogTupleCleanupRetained(), the heavy global sweep here runs at most once
 * every 5 seconds per backend; the common call is a counter increment that
 * returns immediately.
 *
 * CRITICAL: the caller MUST NOT hold any buffer content lock or other
 * page-level critical section across this call.  When the throttle fires this
 * walks every sLog partition taking each writer lock LW_EXCLUSIVE; running it
 * under a buffer lock would serialize all writers to a hot page behind the
 * sweep (the c>=2 hot-row UPDATE convoy this throttle was introduced to avoid).
 */
void
SLogTupleMaybeCleanupRetained(void)
{
	static TimestampTz slog_last_cleanup = 0;
	static uint32 slog_cleanup_clock = 0;
	TimestampTz now_ts;

	if (SLogState == NULL)
		return;

	/*
	 * Sample the wall clock only once every SLOG_INSERT_CLOCK_PERIOD calls.
	 * The throttle below already bounds the heavy sweep to once per 5s; this
	 * just keeps the common-case cost down to a counter increment so the
	 * post-unlock call on every UPDATE does not add a clock_gettime() syscall
	 * to the hot path (matters for the >=HEAP perf target).
	 */
	if ((slog_cleanup_clock++ & (SLOG_INSERT_CLOCK_PERIOD - 1)) != 0)
		return;

	now_ts = GetCurrentTimestamp();

	if (now_ts - slog_last_cleanup > 5000000)	/* 5 seconds */
	{
		slog_last_cleanup = now_ts;
		SLogTupleCleanupRetained();
	}
}

/*
 * SLogTupleAnyTracked
 *		True iff the current backend has tracked any tuple key (INSERT,
 *		UPDATE, or DELETE) for the current transaction -- i.e. this backend
 *		touched at least one RECNO tuple since the last SLogTupleResetTracking().
 *
 * Used by RECNO's xact callback to decide whether a non-RECNO transaction
 * (no tracked keys at all) should skip writing a durable commit-HLC map
 * entry; see recno_operations.c's RecnoSLogXactCallback.
 */
bool
SLogTupleAnyTracked(void)
{
	return slog_tracked_keys != NULL ||
		(slog_insert_tids != NULL && slog_insert_tids->members > 0);
}

/*
 * SLogTupleResetTracking
 *		Clear the backend-private tracking list and reset overflow state.
 *
 * Also frees the backend-local INSERT hash.  The table lives in
 * TopTransactionContext, so it would be freed at transaction end anyway.
 * Explicit cleanup here allows earlier memory reclaim and makes the state
 * consistent for any subsequent operations within the same backend lifetime.
 */
void
SLogTupleResetTracking(void)
{
	/* Destroy the backend-local INSERT hash */
	if (slog_insert_tids != NULL)
	{
		sloginsert_destroy(slog_insert_tids);
		slog_insert_tids = NULL;
	}

	slog_tracked_keys = NULL;
	slog_has_shared_entries = false;
	slog_overflow_warning_count = 0;
	slog_overflow_last_warning = 0;
}

/*
 * SLogTupleIterateTrackedKeys
 *		Iterate over tracked keys for a given xid.
 *
 * Calls the callback for each tracked key matching the given xid.
 * If the callback returns false, iteration stops early.
 * Used by AM-specific pre-commit callbacks that need to touch pages.
 *
 * Iterates both hash-based INSERT entries (top-level) and
 * linked-list entries.
 */
void
SLogTupleIterateTrackedKeys(TransactionId xid,
							SLogTrackedKeyCallback callback,
							void *arg)
{
	SLogTrackedKey *tk;

	/* Iterate backend-local INSERT-hash entries (top-level local-only) */
	if (slog_insert_tids != NULL)
	{
		sloginsert_iterator it;
		SLogInsertTidEntry *ie;

		sloginsert_start_iterate(slog_insert_tids, &it);
		while ((ie = sloginsert_iterate(slog_insert_tids, &it)) != NULL)
		{
			SLogTupleKey key;

			memset(&key, 0, sizeof(key));
			key.relid = ie->key.relid;
			ItemPointerSet(&key.tid,
						   SLOG_DECODE_BLKNO(ie->key.encoded_tid),
						   SLOG_DECODE_OFFNUM(ie->key.encoded_tid));

			if (!callback(&key, xid, InvalidTransactionId, true, arg))
				return;
		}
	}

	/* Iterate linked-list entries */
	for (tk = slog_tracked_keys; tk != NULL; tk = tk->next)
	{
		if (!TransactionIdEquals(tk->xid, xid))
			continue;

		if (!callback(&tk->key, tk->xid, tk->subxid, tk->local_only, arg))
			break;
	}
}

/*
 * SLogTupleIterateTrackedKeysExt
 *		Extended iteration that also passes before-image metadata.
 *
 * Used by commit-time callbacks that need the original t_commit_ts
 * to restore it on in-place-updated tuples (preserving visibility
 * for readers with older snapshots).
 *
 * Sparsemap entries are local-only INSERTs with no before-image, so
 * before_commit_ts=0 and has_before_image=false for those entries.
 */
void
SLogTupleIterateTrackedKeysExt(TransactionId xid,
							   SLogTrackedKeyExtCallback callback,
							   void *arg)
{
	SLogTrackedKey *tk;

	/* Iterate backend-local INSERT-hash entries (top-level local-only) */
	if (slog_insert_tids != NULL)
	{
		sloginsert_iterator it;
		SLogInsertTidEntry *ie;

		sloginsert_start_iterate(slog_insert_tids, &it);
		while ((ie = sloginsert_iterate(slog_insert_tids, &it)) != NULL)
		{
			SLogTupleKey key;

			memset(&key, 0, sizeof(key));
			key.relid = ie->key.relid;
			ItemPointerSet(&key.tid,
						   SLOG_DECODE_BLKNO(ie->key.encoded_tid),
						   SLOG_DECODE_OFFNUM(ie->key.encoded_tid));

			if (!callback(&key, xid, InvalidTransactionId, true,
						  0, false, arg))
				return;
		}
	}

	/* Iterate linked-list entries */
	for (tk = slog_tracked_keys; tk != NULL; tk = tk->next)
	{
		if (!TransactionIdEquals(tk->xid, xid))
			continue;

		if (!callback(&tk->key, tk->xid, tk->subxid, tk->local_only,
					  tk->before_commit_ts,
					  (tk->before_image != NULL),
					  arg))
			break;
	}
}

/*
 * SLogTupleCollectTrackedKeys
 *		Collect all tracked keys for a given xid into a palloc'd array.
 *
 * Returns the number of collected keys.  The caller can then sort the
 * array for batch processing (e.g. by relid/blockno to amortize buffer
 * I/O at commit time).
 *
 * The returned array is allocated in the current memory context; the caller
 * is responsible for pfree().
 */
int
SLogTupleCollectTrackedKeys(TransactionId xid, SLogTrackedKeyInfo **out_keys)
{
	SLogTrackedKey *tk;
	int			count = 0;
	int			capacity = 64;
	SLogTrackedKeyInfo *arr;

	/*
	 * A single very large transaction can touch tens of millions of tracked
	 * keys; at 48 bytes each the array crosses MaxAllocSize (1 GB) past ~22M
	 * entries.  Use the huge-allocation API so bulk loads do not fail at
	 * commit.  This array is transient and pfree'd by the caller.
	 */
	arr = (SLogTrackedKeyInfo *) MemoryContextAllocHuge(CurrentMemoryContext,
														sizeof(SLogTrackedKeyInfo) * (Size) capacity);

	/* Collect backend-local INSERT-hash entries (top-level local-only) */
	if (slog_insert_tids != NULL)
	{
		sloginsert_iterator it;
		SLogInsertTidEntry *ie;

		sloginsert_start_iterate(slog_insert_tids, &it);
		while ((ie = sloginsert_iterate(slog_insert_tids, &it)) != NULL)
		{
			if (count >= capacity)
			{
				capacity *= 2;
				arr = (SLogTrackedKeyInfo *)
					repalloc_huge(arr, sizeof(SLogTrackedKeyInfo) * (Size) capacity);
			}

			memset(&arr[count].key, 0, sizeof(SLogTupleKey));
			arr[count].key.relid = ie->key.relid;
			ItemPointerSet(&arr[count].key.tid,
						   SLOG_DECODE_BLKNO(ie->key.encoded_tid),
						   SLOG_DECODE_OFFNUM(ie->key.encoded_tid));
			arr[count].xid = xid;
			arr[count].subxid = InvalidTransactionId;
			arr[count].local_only = true;
			arr[count].op_type = SLOG_OP_INSERT;
			arr[count].before_commit_ts = 0;
			arr[count].has_before_image = false;
			count++;
		}
	}

	/* Collect linked-list entries */
	for (tk = slog_tracked_keys; tk != NULL; tk = tk->next)
	{
		if (!TransactionIdEquals(tk->xid, xid))
			continue;

		if (count >= capacity)
		{
			capacity *= 2;
			arr = (SLogTrackedKeyInfo *)
				repalloc_huge(arr, sizeof(SLogTrackedKeyInfo) * (Size) capacity);
		}

		arr[count].key = tk->key;
		arr[count].xid = tk->xid;
		arr[count].subxid = tk->subxid;
		arr[count].local_only = tk->local_only;
		arr[count].op_type = tk->op_type;
		arr[count].before_commit_ts = tk->before_commit_ts;
		arr[count].has_before_image = (tk->before_image != NULL);
		count++;
	}

	*out_keys = arr;
	return count;
}
