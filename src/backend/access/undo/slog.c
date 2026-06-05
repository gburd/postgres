/*-------------------------------------------------------------------------
 *
 * slog.c
 *	  Secondary Log (sLog) -- Skip-list + sparsemap shared-memory tracking
 *
 * The sLog tracks aborted transactions and per-tuple operations in shared
 * memory for the UNDO subsystem.
 *
 * Transaction sLog (skip-list + sparsemap):
 *   - A shared-memory skip-list keyed by (xid, reloid) stores full abort
 *     metadata.  Entries are ordered by xid then reloid, enabling efficient
 *     range operations for per-xid lookups.
 *   - A shared-memory sparsemap (compressed bitmap) provides O(1)
 *     SLogXidIsPresent() checks.
 *   - The skip-list is protected by a single LWLock (sLog modifications
 *     only occur on transaction abort, an uncommon path).
 *   - The sparsemap is protected by a SpinLock (operations are O(1)).
 *
 * Tuple sLog (LRLock-protected flat hash):
 *   - Keyed by (relid, tid), stores up to SLOG_MAX_TUPLE_OPS concurrent
 *     operations per tuple.  Designed for the RECNO table AM.
 *   - Uses a flat open-addressing hash table protected by LRLock for
 *     wait-free reads.  Writes are serialized via SLogTupleWriterLock.
 *   - WAL-free: entries are transient, removed at commit/abort.
 *
 * Locking: Transaction sLog uses LWTRANCHE_SLOG.  Tuple sLog uses
 * LRLock (wait-free reads) + SLogTupleWriterLock (writer serialization).
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

#include "access/recno.h"
#include "access/slog.h"
#include "access/slog_flathash.h"
#include "access/transam.h"
#include "access/xact.h"
#include "common/hashfn.h"
#include "miscadmin.h"
#include "storage/lrlock.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "utils/dsa.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"

#include "lib/sparsemap.h"

/* ----------------------------------------------------------------
 * Skip-list instantiation for transaction sLog
 *
 * The skip-list is designed for lock-free concurrent access, but we
 * use SKIPLIST_SINGLE_THREADED mode here because:
 *  (a) The pool allocator (shared-memory slab) is not itself lock-free.
 *  (b) The sparsemap is not concurrent-safe.
 *  (c) sLog modifications only happen on transaction abort -- an
 *      uncommon path -- so a single LWLock is sufficient.
 *
 * SKIPLIST_SINGLE_THREADED eliminates C11 <stdatomic.h> dependency,
 * replacing atomics with plain loads/stores.  All concurrent access
 * is serialized externally by txn_lock (LWLock).
 *
 * Max height 16 supports 2^16 = 65,536 entries; the pool holds at
 * most 256 user entries, so this provides ample headroom while
 * minimizing per-node overhead (16 level pointers per pool slot).
 * ----------------------------------------------------------------
 */
#define SKIPLIST_MAX_HEIGHT 16
#define SKIPLIST_SINGLE_THREADED
#include "lib/skiplist.h"

/*
 * struct slog_txn_node - Skip-list node for transaction sLog entries.
 *
 * Ordered by (xid ASC, reloid ASC) so all entries for a given xid
 * are contiguous in the skip-list.
 */
struct slog_txn_node
{
	/* Key fields */
	TransactionId xid;
	Oid			reloid;

	/* Data fields */
	XLogRecPtr	last_batch_lsn; /* LSN of last UNDO batch for this xid */
	Oid			dboid;
	TimestampTz abort_time;
	bool		revert_complete;

	/* Skip-list metadata */
	SKIPLIST_ENTRY(slog_txn) entries;
};

/*
 * Suppress warnings from macro-generated skip-list functions:
 * - Missing prototypes: SKIPLIST_DECL generates non-static functions
 * - Mixed declarations: the macro bodies use C99 style
 * - Unused functions: not all generated functions are called
 */
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmissing-prototypes"
#pragma GCC diagnostic ignored "-Wdeclaration-after-statement"
#pragma GCC diagnostic ignored "-Wunused-function"

/* *INDENT-OFF* */
SKIPLIST_DECL(slog_txn, sl_, entries,
	/* compare_entries: order by (xid, reloid) */
	{
		if (a->xid < b->xid)
			return -1;
		if (a->xid > b->xid)
			return 1;
		if (a->reloid < b->reloid)
			return -1;
		if (a->reloid > b->reloid)
			return 1;
		return 0;
	},
	/* free_entry: no-op (no heap resources to free) */
	{
		(void) node;
	},
	/* update_entry: copy data fields from value */
	{
		slog_txn_node_t *src = (slog_txn_node_t *) value;
		node->last_batch_lsn = src->last_batch_lsn;
		node->dboid = src->dboid;
		node->abort_time = src->abort_time;
		node->revert_complete = src->revert_complete;
	},
	/* archive_entry: deep copy */
	{
		dest->xid = src->xid;
		dest->reloid = src->reloid;
		dest->last_batch_lsn = src->last_batch_lsn;
		dest->dboid = src->dboid;
		dest->abort_time = src->abort_time;
		dest->revert_complete = src->revert_complete;
	},
	/* sizeof_entry */
	{
		bytes = sizeof(slog_txn_node_t);
	})

SKIPLIST_DECL_POOL(slog_txn, sl_, entries, SLOG_TXN_POOL_CAPACITY)
/* *INDENT-ON* */

#pragma GCC diagnostic pop

/*
 * Initial size for the DSA area used by before-image storage.
 * Grows dynamically as needed up to slog_before_image_max_mb.
 */
#define SLOG_DSA_INIT_SIZE		(512 * 1024)	/* 512 KB */
#define SLOG_DSA_MAX_SIZE_MB	256				/* default max: 256 MB */

/*
 * Maximum number of abort ops applied to an LRLock partition between
 * LRLockPublish calls in SLogTupleMarkAborted.
 *
 * The LRLock oplog lives in the fixed main shared-memory arena.  When a
 * single publish cycle overflows its initial 4 KB capacity the oplog is
 * regrown with a runtime ShmemAlloc that doubles and never frees the old
 * buffer; a very large rollback applying that many ops before one publish
 * exhausts main shared memory and crashes the whole server.  Publishing every
 * SLOG_ABORT_PUBLISH_BATCH ops resets oplog_used to 0, so we must keep one
 * batch comfortably below the oplog's initial capacity to guarantee the
 * runtime ShmemAlloc is never triggered.  Each entry is an 8-byte header plus
 * MAXALIGN(sizeof(SLogFlatOp)) (~96 bytes); 24 entries (~2.3 KB) leaves margin
 * under the 4 KB LRLOCK_OPLOG_INITIAL_CAPACITY even if the op struct grows.
 */
#define SLOG_ABORT_PUBLISH_BATCH	24

/* ----------------------------------------------------------------
 * Shared state definition
 * ----------------------------------------------------------------
 */
typedef struct SLogSharedState
{
	/* Transaction skip-list */
	slog_txn_t	txn_list;		/* skip-list head struct */
	_skip_pool_slog_txn_t txn_pool; /* pool allocator struct */
	LWLockPadded txn_lock;		/* single LWLock for skip-list */

	/* XID presence bitmap */
	sparsemap_t xid_map;		/* sparsemap struct */
	slock_t		xid_spinlock;	/* SpinLock for sparsemap */

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

/* Pointers to ShmemAlloc'd regions, set by ShmemRequestStruct framework */
static char *SLogPoolSlots = NULL;
static char *SLogPoolFreeList = NULL;
static char *SLogXidMapBuffer = NULL;
static char *SLogFlatHashBlock = NULL;	/* single allocation for all partition LRLock blocks */

/* Sentinel value for slh_ebr to redirect node deallocation */
static int	slog_ebr_sentinel = 1;

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
 * Compact INSERT tracking via sparsemap (OOM fix)
 *
 * For top-level transactions (no active savepoint), local-only INSERT
 * entries are tracked in per-relid sparsemaps instead of the linked list.
 * This reduces memory from ~136 bytes/row to ~1 bit/row for sequential
 * TIDs, preventing OOM on bulk INSERT...SELECT with 10M+ rows.
 *
 * TID encoding: ((uint64)blkno << 16) | (uint64)offnum
 * This packs BlockNumber (32-bit) + OffsetNumber (16-bit) into 48 bits.
 *
 * Subtransaction entries still use the linked list because subtxn abort
 * needs per-entry subxid filtering.
 * ----------------------------------------------------------------
 */
#define SLOG_ENCODE_TID(blkno, offnum) \
	(((uint64)(blkno) << 16) | (uint64)(offnum))
#define SLOG_DECODE_BLKNO(encoded) \
	((BlockNumber)((encoded) >> 16))
#define SLOG_DECODE_OFFNUM(encoded) \
	((OffsetNumber)((encoded) & 0xFFFF))

/* Initial sparsemap buffer size: 4KB, grows geometrically via sm_add_grow */
#define SLOG_INSERT_MAP_INIT_SIZE	4096

typedef struct SLogInsertMap
{
	Oid			relid;
	sparsemap_t *map;			/* bit per TID, run-length compressed */
	struct SLogInsertMap *next;
} SLogInsertMap;

static SLogInsertMap *slog_insert_maps = NULL;

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
#define SLOG_PART_LRLOCK(key_ptr) \
	(SLogGetPartition(key_ptr)->lrlock)
#define SLOG_PART_WRITER_LOCK(key_ptr) \
	(&SLogGetPartition(key_ptr)->writer_lock.lock)

/*
 * EBR retire callback for the skip-list.
 *
 * Instead of pfree()'ing the node (which would crash on shared memory),
 * we return it to the pool's free list.
 */
static void
slog_ebr_retire_callback(void *ebr_state, slog_txn_t *slist, slog_txn_node_t *node)
{
	(void) ebr_state;
	(void) slist;
	sl_skip_pool_free_slog_txn(&SLogState->txn_pool, node);
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
 * so sizeof(SLogSharedState) already includes SLOG_DSA_INIT_SIZE.
 */
Size
SLogShmemSize(void)
{
	Size		size;
	size_t		raw_slot_size;
	size_t		slot_size;

	size = MAXALIGN(sizeof(SLogSharedState));

	/* Pool slots: node + levels array, rounded to 64-byte alignment */
	raw_slot_size = sizeof(slog_txn_node_t) +
		sizeof(struct _skiplist_slog_txn_level) * SKIPLIST_MAX_HEIGHT;
	slot_size = (raw_slot_size + 63u) & ~(size_t) 63u;
	size = add_size(size, MAXALIGN(slot_size * SLOG_TXN_POOL_CAPACITY));

	/* Pool free-list array */
	size = add_size(size, MAXALIGN(SLOG_TXN_POOL_CAPACITY * sizeof(int32_t)));

	/* Sparsemap buffer */
	size = add_size(size, MAXALIGN(SLOG_XID_MAP_BUFSIZE));

	/* Partitioned LRLock flat hashes (32 partitions) */
	size = add_size(size, SLogFlatHashPartitionedShmemSize(SLogFlatHashCapacity(),
														  MaxBackends));

	return size;
}

/*
 * SLogShmemRequest
 *		Register shared memory needs for the sLog.
 *
 * We register the main state struct, pool regions, sparsemap buffer,
 * and tuple hash table via the ShmemRequestStruct/ShmemRequestHash
 * framework.
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
	 * Additional shared memory for pool slots, free-list, and sparsemap
	 * buffer registered as separate ShmemRequestStruct entries.
	 */
	{
		size_t		raw_slot_size;
		size_t		slot_size;

		raw_slot_size = sizeof(slog_txn_node_t) +
			sizeof(struct _skiplist_slog_txn_level) * SKIPLIST_MAX_HEIGHT;
		slot_size = (raw_slot_size + 63u) & ~(size_t) 63u;

		ShmemRequestStruct(.name = "sLog Pool Slots",
						   .size = slot_size * SLOG_TXN_POOL_CAPACITY,
						   .ptr = (void **) &SLogPoolSlots,
			);

		ShmemRequestStruct(.name = "sLog Pool FreeList",
						   .size = SLOG_TXN_POOL_CAPACITY * sizeof(int32_t),
						   .ptr = (void **) &SLogPoolFreeList,
			);

		ShmemRequestStruct(.name = "sLog XID Map Buffer",
						   .size = SLOG_XID_MAP_BUFSIZE,
						   .ptr = (void **) &SLogXidMapBuffer,
			);

		/* Single block for all partitioned LRLock flat hashes */
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
 * has already allocated SLogState.  We manually initialize the skip-list
 * pool, skip-list, sparsemap, and LRLock flat hash.
 */
void
SLogShmemInit(void)
{
	slog_txn_t *slist = &SLogState->txn_list;
	_skip_pool_slog_txn_t *pool = &SLogState->txn_pool;
	size_t		raw_slot_size;
	size_t		slot_size;
	slog_txn_node_t *head_node;
	slog_txn_node_t *tail_node;
	int			i;

	/* ---- Initialize the pool manually in shared memory ---- */

	raw_slot_size = sizeof(slog_txn_node_t) +
		sizeof(struct _skiplist_slog_txn_level) * SKIPLIST_MAX_HEIGHT;
	slot_size = (raw_slot_size + 63u) & ~(size_t) 63u;

	pool->capacity = SLOG_TXN_POOL_CAPACITY;
	pool->slot_size = slot_size;

	/* Use the framework-allocated shared memory regions */
	pool->slots = SLogPoolSlots;
	memset(pool->slots, 0, slot_size * SLOG_TXN_POOL_CAPACITY);

	pool->next_free = (int32_t *) SLogPoolFreeList;
	memset(pool->next_free, 0, SLOG_TXN_POOL_CAPACITY * sizeof(int32_t));

	/* Build the free-list chain */
	for (i = 0; i < SLOG_TXN_POOL_CAPACITY - 1; i++)
		pool->next_free[i] = i + 1;
	pool->next_free[SLOG_TXN_POOL_CAPACITY - 1] = -1;
	pool->free_head = 0;

	/* ---- Initialize the skip-list manually ---- */

	/* Allocate head and tail sentinel nodes from the pool */
	head_node = sl_skip_pool_alloc_slog_txn(pool);
	tail_node = sl_skip_pool_alloc_slog_txn(pool);

	if (head_node == NULL || tail_node == NULL)
		elog(PANIC, "sLog: failed to allocate skip-list sentinels from pool");

	/* Set up sentinel heights and forward pointers */
	head_node->entries.sle_height = 1;
	for (i = 0; i < SKIPLIST_MAX_HEIGHT; i++)
		head_node->entries.sle_levels[i].next = tail_node;
	head_node->entries.sle_prev = NULL;

	tail_node->entries.sle_height = 1;
	for (i = 0; i < SKIPLIST_MAX_HEIGHT; i++)
		tail_node->entries.sle_levels[i].next = NULL;
	tail_node->entries.sle_prev = head_node;

	/* Initialize the skip-list struct */
	slist->slh_length = 0;
	slist->slh_aux = NULL;
	slist->slh_head = head_node;
	slist->slh_tail = tail_node;

	/* Set function pointers */
	slist->slh_fns.free_entry = _skip_free_entry_fn_slog_txn;
	slist->slh_fns.update_entry = _skip_update_entry_fn_slog_txn;
	slist->slh_fns.archive_entry = _skip_archive_entry_fn_slog_txn;
	slist->slh_fns.sizeof_entry = _skip_sizeof_entry_fn_slog_txn;
	slist->slh_fns.compare_entries = _skip_compare_entries_fn_slog_txn;
	slist->slh_fns.snapshot_preserve_node = NULL;
	slist->slh_fns.snapshot_release = NULL;

	/* Snapshot fields unused */
	slist->slh_snap.cur_era = 0;
	slist->slh_snap.pres_era = 0;
	slist->slh_snap.pres = NULL;

	/* Seed PRNG */
	slist->slh_prng_state = ((uint32_t) time(NULL) ^
							 ((uint32_t) MyProcPid << 16) ^
							 (uint32_t) (uintptr_t) slist);
	slist->slh_splay_counter = 0;

	/*
	 * Set EBR hooks to redirect node deallocation to pool free-list instead
	 * of pfree().  We use a non-NULL sentinel for slh_ebr so the skip-list's
	 * remove_node code takes the EBR path.
	 */
	slist->slh_ebr = &slog_ebr_sentinel;
	slist->slh_ebr_retire = slog_ebr_retire_callback;

	/* ---- Initialize the sparsemap ---- */

	sparsemap_init(&SLogState->xid_map, (uint8 *) SLogXidMapBuffer,
				 SLOG_XID_MAP_BUFSIZE);

	/* ---- Initialize locks ---- */

	LWLockInitialize(&SLogState->txn_lock.lock, LWTRANCHE_SLOG);
	SpinLockInit(&SLogState->xid_spinlock);

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

	/* ---- Initialize partitioned LRLock flat hashes ---- */
	{
		int			total_capacity = SLogFlatHashCapacity();
		int			per_part_cap = total_capacity / SLogNumPartitions;
		Size		data_size;
		Size		oplog_capacity;
		Size		per_part_shmem_size;
		char	   *block_ptr;
		int			part;

		if (per_part_cap < 64)
			per_part_cap = 64;

		data_size = SLogFlatHashDataSize(per_part_cap);
		oplog_capacity = (Size) MaxBackends * 4 *
			(MAXALIGN(sizeof(SLogFlatOp)) + MAXALIGN(sizeof(Size)));
		oplog_capacity = Max(oplog_capacity, 65536);

		per_part_shmem_size = MAXALIGN(SLogFlatHashShmemSize(per_part_cap,
															 MaxBackends));
		block_ptr = SLogFlatHashBlock;

		for (part = 0; part < SLogNumPartitions; part++)
		{
			LRLock	   *lrl;
			void	   *write_data;
			void	   *read_data;
			char		name[64];

			snprintf(name, sizeof(name), "sLog Tuple Partition %d", part);

			/* Initialize per-partition writer lock */
			LWLockInitialize(&SLogState->tuple_partitions[part].writer_lock.lock,
							 LWTRANCHE_SLOG);

			/* Initialize per-partition LRLock in the sliced block */
			lrl = LRLockInitInPlace(block_ptr, data_size,
									SLogFlatHashApply, SLogFlatHashSync,
									MaxBackends, oplog_capacity, name);

			write_data = LRLockGetWriteData(lrl);
			SLogFlatHashInit(write_data, per_part_cap);

			read_data = (void *) LRLockGetReadData(lrl);
			SLogFlatHashInit(read_data, per_part_cap);

			LRLockMarkReady(lrl);

			SLogState->tuple_partitions[part].lrlock = lrl;

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
	dsa_detach(SLogState->dsa_area);
	SLogState->dsa_area = NULL;	/* backends re-attach lazily */
}

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
 * SLogDsaAllocateBeforeImage
 *		Allocate a before-image in the shared DSA area.
 *
 * Returns InvalidDsaPointer on failure (e.g., DSA full).
 */
dsa_pointer
SLogDsaAllocateBeforeImage(const char *data, int len,
						   uint16 flags, uint64 commit_ts)
{
	dsa_pointer dp;
	SLogBeforeImage *bi;
	Size		alloc_size;

	SLogEnsureDsaAttached();

	alloc_size = offsetof(SLogBeforeImage, data) + len;
	dp = dsa_allocate_extended(slog_dsa_handle, alloc_size, DSA_ALLOC_NO_OOM);
	if (!DsaPointerIsValid(dp))
		return InvalidDsaPointer;

	bi = (SLogBeforeImage *) dsa_get_address(slog_dsa_handle, dp);
	bi->len = len;
	bi->flags = flags;
	bi->commit_ts = commit_ts;
	memcpy(bi->data, data, len);

	return dp;
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
 * Creates an entry in the skip-list and sets the corresponding bit
 * in the XID sparsemap.  Returns false if the pool is full.
 */
bool
SLogTxnInsert(TransactionId xid, Oid reloid, Oid dboid,
			  XLogRecPtr last_batch_lsn)
{
	slog_txn_node_t *node;
	slog_txn_node_t query;
	slog_txn_node_t *existing;
	int			rc;

	LWLockAcquire(&SLogState->txn_lock.lock, LW_EXCLUSIVE);

	/* Check for duplicate first */
	memset(&query, 0, sizeof(query));
	query.xid = xid;
	query.reloid = reloid;

	existing = sl_skip_position_eq_slog_txn(&SLogState->txn_list, &query);
	if (existing != NULL)
	{
		/* Already present -- no-op */
		LWLockRelease(&SLogState->txn_lock.lock);
		return true;
	}

	/* Allocate from pool */
	node = sl_skip_pool_alloc_slog_txn(&SLogState->txn_pool);
	if (node == NULL)
	{
		/* Pool full */
		LWLockRelease(&SLogState->txn_lock.lock);
		return false;
	}

	/* Fill key and data fields */
	node->xid = xid;
	node->reloid = reloid;
	node->last_batch_lsn = last_batch_lsn;
	node->dboid = dboid;
	node->abort_time = GetCurrentTimestamp();
	node->revert_complete = false;

	/* Insert into skip-list */
	rc = sl_skip_insert_slog_txn(&SLogState->txn_list, node);
	if (rc != 0)
	{
		/* Duplicate (shouldn't happen after our check, but be safe) */
		sl_skip_pool_free_slog_txn(&SLogState->txn_pool, node);
		LWLockRelease(&SLogState->txn_lock.lock);
		return true;
	}

	/* Update sparsemap */
	SpinLockAcquire(&SLogState->xid_spinlock);
	sparsemap_add(&SLogState->xid_map, (uint64) xid);
	SpinLockRelease(&SLogState->xid_spinlock);

	LWLockRelease(&SLogState->txn_lock.lock);
	return true;
}

/*
 * SLogXidIsPresent
 *		O(1) check whether a transaction has any sLog entries.
 *
 * This is the hot-path replacement for the old O(N) ATMIsAborted scan.
 * Uses only the SpinLock-protected sparsemap, avoiding the heavier LWLock.
 */
bool
SLogXidIsPresent(TransactionId xid)
{
	bool		result;

	SpinLockAcquire(&SLogState->xid_spinlock);
	result = sparsemap_contains(&SLogState->xid_map, (uint64) xid);
	SpinLockRelease(&SLogState->xid_spinlock);

	return result;
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
	slog_txn_node_t query;
	slog_txn_node_t *found;

	memset(&query, 0, sizeof(query));
	query.xid = xid;
	query.reloid = reloid;

	LWLockAcquire(&SLogState->txn_lock.lock, LW_SHARED);

	found = sl_skip_position_eq_slog_txn(&SLogState->txn_list, &query);

	if (found != NULL && entry_out != NULL)
	{
		entry_out->xid = found->xid;
		entry_out->reloid = found->reloid;
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
 * Uses skip-list GTE positioning to find the first entry with the given
 * xid.  O(log n) instead of O(n) hash scan.
 */
bool
SLogTxnLookupByXid(TransactionId xid, XLogRecPtr *lsn_out)
{
	slog_txn_node_t query;
	slog_txn_node_t *found;

	memset(&query, 0, sizeof(query));
	query.xid = xid;
	query.reloid = 0;			/* minimum reloid */

	LWLockAcquire(&SLogState->txn_lock.lock, LW_SHARED);

	found = sl_skip_position_gte_slog_txn(&SLogState->txn_list, &query);

	if (found != NULL && found->xid == xid)
	{
		if (lsn_out)
			*lsn_out = found->last_batch_lsn;
		LWLockRelease(&SLogState->txn_lock.lock);
		return true;
	}

	LWLockRelease(&SLogState->txn_lock.lock);
	return false;
}

/*
 * SLogTxnRemove
 *		Remove a specific (xid, reloid) entry.
 *
 * After removal, checks whether any entries remain for this xid and
 * clears the sparsemap bit if not.
 */
void
SLogTxnRemove(TransactionId xid, Oid reloid)
{
	slog_txn_node_t query;
	slog_txn_node_t check;
	slog_txn_node_t *remaining;
	int			rc;

	memset(&query, 0, sizeof(query));
	query.xid = xid;
	query.reloid = reloid;

	LWLockAcquire(&SLogState->txn_lock.lock, LW_EXCLUSIVE);

	rc = sl_skip_remove_node_slog_txn(&SLogState->txn_list, &query);

	if (rc == 0)
	{
		/* Successfully removed.  Check if any entries remain for this xid. */
		memset(&check, 0, sizeof(check));
		check.xid = xid;
		check.reloid = 0;

		remaining = sl_skip_position_gte_slog_txn(&SLogState->txn_list, &check);

		if (remaining == NULL || remaining->xid != xid)
		{
			/* No entries remain -- clear sparsemap bit */
			SpinLockAcquire(&SLogState->xid_spinlock);
			sparsemap_remove(&SLogState->xid_map, (uint64) xid);
			SpinLockRelease(&SLogState->xid_spinlock);
		}
	}

	LWLockRelease(&SLogState->txn_lock.lock);
}

/*
 * SLogTxnRemoveByXid
 *		Remove all sLog entries for a given transaction ID.
 *
 * Collects all nodes for this xid into a local array, then removes
 * them.  The entries are contiguous in the skip-list thanks to the
 * (xid, reloid) ordering.
 */
void
SLogTxnRemoveByXid(TransactionId xid)
{
	slog_txn_node_t query;
	slog_txn_node_t *node;
	slog_txn_node_t *next;
	slog_txn_node_t **to_remove;
	int			nremove = 0;
	int			max_remove = 64;
	int			i;

	to_remove = (slog_txn_node_t **) palloc(max_remove * sizeof(slog_txn_node_t *));

	memset(&query, 0, sizeof(query));
	query.xid = xid;
	query.reloid = 0;

	LWLockAcquire(&SLogState->txn_lock.lock, LW_EXCLUSIVE);

	/* Collect all nodes with matching xid */
	node = sl_skip_position_gte_slog_txn(&SLogState->txn_list, &query);

	while (node != NULL && node->xid == xid)
	{
		if (nremove >= max_remove)
		{
			max_remove *= 2;
			to_remove = (slog_txn_node_t **)
				repalloc(to_remove, max_remove * sizeof(slog_txn_node_t *));
		}
		to_remove[nremove++] = node;
		next = sl_skip_next_node_slog_txn(&SLogState->txn_list, node);
		node = next;
	}

	/* Remove collected nodes */
	for (i = 0; i < nremove; i++)
	{
		sl_skip_remove_node_slog_txn(&SLogState->txn_list, to_remove[i]);
	}

	/* Clear sparsemap bit */
	if (nremove > 0)
	{
		SpinLockAcquire(&SLogState->xid_spinlock);
		sparsemap_remove(&SLogState->xid_map, (uint64) xid);
		SpinLockRelease(&SLogState->xid_spinlock);
	}

	LWLockRelease(&SLogState->txn_lock.lock);

	pfree(to_remove);
}

/*
 * SLogTxnMarkReverted
 *		Mark all entries for a given xid as revert_complete.
 *
 * Walks the contiguous range of entries for this xid in the skip-list.
 */
void
SLogTxnMarkReverted(TransactionId xid)
{
	slog_txn_node_t query;
	slog_txn_node_t *node;

	memset(&query, 0, sizeof(query));
	query.xid = xid;
	query.reloid = 0;

	LWLockAcquire(&SLogState->txn_lock.lock, LW_EXCLUSIVE);

	node = sl_skip_position_gte_slog_txn(&SLogState->txn_list, &query);

	while (node != NULL && node->xid == xid)
	{
		node->revert_complete = true;
		node = sl_skip_next_node_slog_txn(&SLogState->txn_list, node);
	}

	LWLockRelease(&SLogState->txn_lock.lock);
}

/*
 * SLogTxnGetNextUnreverted
 *		Find an entry that hasn't been reverted yet.
 *
 * Iterates the skip-list from head to tail (ordered by xid), returning
 * the first entry with revert_complete == false.
 */
bool
SLogTxnGetNextUnreverted(TransactionId *xid_out, Oid *dboid_out,
						 XLogRecPtr *lsn_out)
{
	slog_txn_node_t *node;
	size_t		iter;

	LWLockAcquire(&SLogState->txn_lock.lock, LW_SHARED);

	SKIPLIST_FOREACH_H2T(slog_txn, sl_, entries, &SLogState->txn_list, node, iter)
	{
		if (!node->revert_complete)
		{
			*xid_out = node->xid;
			*dboid_out = node->dboid;
			*lsn_out = node->last_batch_lsn;

			LWLockRelease(&SLogState->txn_lock.lock);
			return true;
		}
	}

	LWLockRelease(&SLogState->txn_lock.lock);
	return false;
}

/*
 * SLogRecoveryFinalize
 *		Count entries after recovery for logging.
 */
void
SLogRecoveryFinalize(int *total_out, int *unreverted_out)
{
	slog_txn_node_t *node;
	size_t		iter;
	int			total = 0;
	int			unreverted = 0;

	LWLockAcquire(&SLogState->txn_lock.lock, LW_SHARED);

	SKIPLIST_FOREACH_H2T(slog_txn, sl_, entries, &SLogState->txn_list, node, iter)
	{
		total++;
		if (!node->revert_complete)
			unreverted++;
	}

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
	slog_txn_node_t *node;
	size_t		iter;
	XLogRecPtr	oldest = InvalidXLogRecPtr;

	LWLockAcquire(&SLogState->txn_lock.lock, LW_SHARED);

	SKIPLIST_FOREACH_H2T(slog_txn, sl_, entries, &SLogState->txn_list, node, iter)
	{
		if (!node->revert_complete &&
			XLogRecPtrIsValid(node->last_batch_lsn))
		{
			if (!XLogRecPtrIsValid(oldest) ||
				node->last_batch_lsn < oldest)
				oldest = node->last_batch_lsn;
		}
	}

	LWLockRelease(&SLogState->txn_lock.lock);
	return oldest;
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
 * Called when the sLog flat hash is full.  Scans the flat hash under read-side
 * to collect evictable keys, then applies REMOVE_ENTRY ops under writer lock.
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

	/* Phase 1: scan each partition under read-side to collect evictable keys */
	for (part = 0; part < SLogNumPartitions && nkeys < max_evict; part++)
	{
		SLogFlatPartition *fp = SLogGetPartitionByIndex(part);
		const SLogFlatHash *ht;
		SLogFlatHashScanState scan;
		const SLogFlatBucket *bucket;

		ht = (const SLogFlatHash *) LRLockReadBegin(fp->lrlock);

		SLogFlatHashScanInit(&scan);
		while ((bucket = SLogFlatHashScanNext(ht, &scan)) != NULL)
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

		LRLockReadEnd(fp->lrlock);
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
			LRLockWriteBegin(fp->lrlock);

			for (i = 0; i < nkeys; i++)
			{
				SLogFlatOp	flat_op;

				if (SLogFlatHashPartitionIndex(&keys_to_evict[i]) != part)
					continue;

				memset(&flat_op, 0, sizeof(flat_op));
				flat_op.kind = SLOG_FLAT_OP_REMOVE_ENTRY;
				flat_op.key = keys_to_evict[i];
				LRLockApplyOp(fp->lrlock, &flat_op, sizeof(flat_op));
			}

			LRLockPublish(fp->lrlock);
			LRLockWriteEnd(fp->lrlock);
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
 * Inserts into the LRLock-protected flat hash (wait-free reads).
 * Performs overflow handling before failing.
 *
 * Also adds the key to the backend-private tracking list for cleanup.
 */
bool
SLogTupleInsert(Oid relid, ItemPointer tid, TransactionId xid,
				SLogOpType op_type, TransactionId subxid,
				CommandId cid, TimestampTz commit_ts,
				uint32 spec_token)
{
	SLogTupleKey key;
	SLogFlatOp	flat_op;
	SLogFlatPartition *fp;
	const SLogFlatHash *ht;
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
	 * on-overflow path — SLogTupleEvictCommitted() skips them.  They are
	 * cleaned up by SLogTupleCleanupRetained() called periodically from the
	 * UNDO background worker with a safe HLC horizon.
	 *
	 * Per-TID ops array overflow (SLOG_MAX_TUPLE_OPS=32 slots all full of
	 * retained entries on hot rows) is handled by flat_hash_apply_insert()
	 * which reclaims the oldest retained entry when no free slot exists.
	 */

	/*
	 * Cache the oldest active snapshot HLC for the per-TID reclamation
	 * guard in flat_hash_apply_insert.  Refreshed at most every 100ms
	 * to amortize the cost of scanning per-backend snapshot slots.
	 * Passed to the flat hash via flat_op.commit_hlc (unused for INSERT).
	 */
	{
		static uint64 slog_cached_oldest_hlc = 0;
		static TimestampTz slog_last_hlc_refresh = 0;
		static uint32 slog_insert_clock_skip = 0;

		/*
		 * GetCurrentTimestamp() is a syscall (clock_gettime); calling it on
		 * every insert dominates the CAS-update hot path under TPROC-C.  The
		 * clock is only needed to drive two coarse throttles (100ms HLC
		 * refresh, 5s cleanup), so sample it once per SLOG_INSERT_CLOCK_PERIOD
		 * inserts.  Off-cycle inserts reuse the cached horizon, which is safe:
		 * a staler (older) horizon only makes per-TID reclamation MORE
		 * conservative and never frees a before-image an active reader needs.
		 */
#define SLOG_INSERT_CLOCK_PERIOD 256
		if ((slog_insert_clock_skip++ % SLOG_INSERT_CLOCK_PERIOD) == 0)
		{
			TimestampTz now_ts = GetCurrentTimestamp();

			if (now_ts - slog_last_hlc_refresh > 100000)	/* 100ms */
			{
				slog_cached_oldest_hlc = RecnoGetOldestActiveSnapshotHLC();
				slog_last_hlc_refresh = now_ts;
			}

			/*
			 * Periodically run retained-entry cleanup to free DSA
			 * before-images that are no longer visible to any active
			 * snapshot.  This prevents the slog_dsa_max_size_mb area from
			 * filling up during sustained high-TPS workloads when
			 * max_logical_revert_workers=0 (the UNDO background worker that
			 * normally calls this is disabled).  Safe to call from any
			 * backend (acquires partition writer locks internally).
			 */
			{
				static TimestampTz slog_last_cleanup = 0;

				if (slog_cached_oldest_hlc > 0 &&
					now_ts - slog_last_cleanup > 5000000)	/* 5 seconds */
				{
					slog_last_cleanup = now_ts;
					SLogTupleCleanupRetained(slog_cached_oldest_hlc);
				}
			}
		}
#undef SLOG_INSERT_CLOCK_PERIOD

		/* Build the flat hash op */
		memset(&flat_op, 0, sizeof(flat_op));
		flat_op.kind = SLOG_FLAT_OP_INSERT;
		flat_op.key = key;
		flat_op.xid = xid;
		flat_op.subxid = subxid;
		flat_op.commit_hlc = slog_cached_oldest_hlc;	/* reclaim horizon */

		/*
		 * The xid reclaim horizon must be computed fresh on every insert, NOT
		 * cached like the HLC above.  It drives per-TID marker reclamation on a
		 * full hot row: a stale (too-old) horizon makes every committed marker
		 * look still-needed, so reclamation frees nothing, the array stays
		 * full, and the new uncommitted UPDATE registration is dropped --
		 * leaving no marker to stamp at PRE_COMMIT and letting the next
		 * concurrent writer clobber (lost update).  A 100ms-stale xmin lags
		 * thousands of xids under load and reintroduces the loss; the
		 * ProcArrayLock-shared scan is cheap enough to run unconditionally.
		 */
		flat_op.reclaim_xid_horizon = GetOldestNonRemovableTransactionId(NULL);
		flat_op.tuple_op.xid = xid;
		flat_op.tuple_op.subxid = subxid;
		flat_op.tuple_op.op_type = op_type;
		flat_op.tuple_op.cid = cid;
		flat_op.tuple_op.commit_ts = commit_ts;
		flat_op.tuple_op.spec_token = spec_token;
		flat_op.tuple_op.commit_hlc = 0;
		flat_op.tuple_op.before_image_dp = InvalidDsaPointer;
		flat_op.tuple_op.in_use = true;
	}

	/* Apply to flat hash via LRLock writer path (partition-local) */
	LWLockAcquire(&fp->writer_lock.lock, LW_EXCLUSIVE);

	/* Check capacity before insert */
	ht = (const SLogFlatHash *) LRLockGetWriteData(fp->lrlock);
	entries_before = ht->num_entries;

	LRLockWriteBegin(fp->lrlock);
	LRLockApplyOp(fp->lrlock, &flat_op, sizeof(flat_op));
	LRLockPublish(fp->lrlock);
	LRLockWriteEnd(fp->lrlock);

	/* Check if insert succeeded */
	ht = (const SLogFlatHash *) LRLockGetWriteData(fp->lrlock);
	entries_after = ht->num_entries;

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

		ht = (const SLogFlatHash *) LRLockReadBegin(fp->lrlock);
		op_stored = SLogFlatHashHasOpForXid(ht, &key, xid);
		LRLockReadEnd(fp->lrlock);

		if (!op_stored)
		{
			/* Table or per-TID array was full — try eviction */
			int		evicted = SLogTupleEvictCommitted();

			if (evicted > 0)
			{
				/* Retry */
				LWLockAcquire(&fp->writer_lock.lock, LW_EXCLUSIVE);
				LRLockWriteBegin(fp->lrlock);
				LRLockApplyOp(fp->lrlock, &flat_op, sizeof(flat_op));
				LRLockPublish(fp->lrlock);
				LRLockWriteEnd(fp->lrlock);
				LWLockRelease(&fp->writer_lock.lock);

				/* Check again */
				ht = (const SLogFlatHash *) LRLockReadBegin(fp->lrlock);
				op_stored = SLogFlatHashHasOpForXid(ht, &key, xid);
				LRLockReadEnd(fp->lrlock);
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
 * per-tuple sLog so that RecnoTupleVisibleHLC() can correctly determine
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
	 * make RecnoTupleVisibleHLC() trip on TransactionIdDidAbort()/IsInProgress()
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
	LRLockWriteBegin(SLOG_PART_LRLOCK(&key));
	LRLockApplyOp(SLOG_PART_LRLOCK(&key), &clear_op, sizeof(clear_op));
	LRLockApplyOp(SLOG_PART_LRLOCK(&key), &flat_op, sizeof(flat_op));
	LRLockPublish(SLOG_PART_LRLOCK(&key));
	LRLockWriteEnd(SLOG_PART_LRLOCK(&key));
	LWLockRelease(SLOG_PART_WRITER_LOCK(&key));

	return true;
}

/*
 * SLogTupleLookup
 *		Look up a tuple's sLog entry (copy semantics).
 *
 * Returns true if found, copying the full entry into *entry_out.
 * WAIT-FREE: uses LRLock read-side (atomic epoch increment only).
 */
bool
SLogTupleLookup(Oid relid, ItemPointer tid, SLogTupleEntry *entry_out)
{
	SLogTupleKey key;
	const SLogFlatHash *ht;
	const SLogFlatBucket *bucket;

	memset(&key, 0, sizeof(key));
	key.relid = relid;
	ItemPointerCopy(tid, &key.tid);

	ht = (const SLogFlatHash *) LRLockReadBegin(SLOG_PART_LRLOCK(&key));

	bucket = SLogFlatHashProbe(ht, &key);

	if (bucket != NULL && entry_out)
		memcpy(entry_out, &bucket->entry, sizeof(SLogTupleEntry));

	LRLockReadEnd(SLOG_PART_LRLOCK(&key));

	return (bucket != NULL);
}

/*
 * SLogTupleLookupFiltered
 *		Find sLog entries for a TID, optionally filtered by xid.
 *
 * WAIT-FREE: uses LRLock read-side.  If xid_filter is valid, returns
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
	const SLogFlatHash *ht;
	const SLogFlatBucket *bucket;
	int			nfound = 0;
	int			i;

	memset(&key, 0, sizeof(key));
	key.relid = relid;
	ItemPointerCopy(tid, &key.tid);

	ht = (const SLogFlatHash *) LRLockReadBegin(SLOG_PART_LRLOCK(&key));

	bucket = SLogFlatHashProbe(ht, &key);

	if (bucket != NULL)
	{
		const SLogTupleEntry *entry = &bucket->entry;

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

	LRLockReadEnd(SLOG_PART_LRLOCK(&key));

	return nfound;
}

/*
 * SLogTupleRemove
 *		Remove operations for a specific xid from a tuple entry.
 *
 * Uses LRLock writer path with external serialization.
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

	LWLockAcquire(SLOG_PART_WRITER_LOCK(&key), LW_EXCLUSIVE);
	LRLockWriteBegin(SLOG_PART_LRLOCK(&key));
	LRLockApplyOp(SLOG_PART_LRLOCK(&key), &op, sizeof(op));
	LRLockPublish(SLOG_PART_LRLOCK(&key));
	LRLockWriteEnd(SLOG_PART_LRLOCK(&key));
	LWLockRelease(SLOG_PART_WRITER_LOCK(&key));
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
	int			nentries = 0;
	int			part;

	if (SLogState == NULL)
		return;

	/* Count matching entries that have shared hash entries */
	for (tk = slog_tracked_keys; tk != NULL; tk = tk->next)
	{
		if (TransactionIdEquals(tk->xid, xid) && !tk->local_only)
			nentries++;
	}

	if (nentries == 0)
		return;

	/* Batch apply REMOVE_XID ops grouped by partition */
	for (part = 0; part < SLogNumPartitions; part++)
	{
		SLogFlatPartition *fp = SLogGetPartitionByIndex(part);
		bool		has_entries = false;

		/* Check if any tracked keys belong to this partition */
		for (tk = slog_tracked_keys; tk != NULL; tk = tk->next)
		{
			if (!TransactionIdEquals(tk->xid, xid) || tk->local_only)
				continue;
			if (SLogFlatHashPartitionIndex(&tk->key) == part)
			{
				has_entries = true;
				break;
			}
		}
		if (!has_entries)
			continue;

		LWLockAcquire(&fp->writer_lock.lock, LW_EXCLUSIVE);
		LRLockWriteBegin(fp->lrlock);

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
			LRLockApplyOp(fp->lrlock, &flat_op, sizeof(flat_op));
		}

		LRLockPublish(fp->lrlock);
		LRLockWriteEnd(fp->lrlock);
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
	int			nentries = 0;

	if (SLogState == NULL)
		return;

	/* Fast path: INSERT-only transactions never touch the shared hash */
	if (!slog_has_shared_entries)
		return;

	/* Count matching entries that have shared hash entries */
	for (tk = slog_tracked_keys; tk != NULL; tk = tk->next)
	{
		if (TransactionIdEquals(tk->xid, xid) && !tk->local_only)
			nentries++;
	}

	if (nentries == 0)
		return;

	/*
	 * Commit-time before-image publication (deferred from write time).
	 *
	 * The shared DSA before-image only ever serves a snapshot-isolation
	 * reader (REPEATABLE READ / SERIALIZABLE) whose snapshot HLC predates our
	 * commit HLC.  Under READ COMMITTED-only workloads no such reader exists,
	 * so we skip the allocation entirely and the COMMIT_XID step below drops
	 * every op (no retained image to stamp).
	 *
	 * The gate is sound against a reader that started after our last write:
	 * the reader bumps active_iso_readers (seq-cst) before taking its
	 * snapshot HLC (seq-cst RMW), and we read the counter here AFTER our
	 * commit HLC was stamped (also a seq-cst RMW, in RecnoClearUncommittedFlags
	 * at PRE_COMMIT).  In the single total order over those RMWs, any reader
	 * with snapshot < our commit has its increment ordered before our read,
	 * so RecnoHasActiveIsoReaders() returns true and we publish the image.
	 *
	 * We publish ONE image per distinct updated key: the deepest tracked-key
	 * node for a key (the list is newest-first, so the last match while
	 * walking) holds the true pre-transaction state.  Shallower nodes are
	 * intra-transaction post-images and must not overwrite it.
	 */
	if (RecnoHasActiveIsoReaders())
	{
		for (tk = slog_tracked_keys; tk != NULL; tk = tk->next)
		{
			SLogTrackedKey *deeper;
			SLogFlatOp	update_op;
			dsa_pointer dp;
			bool		is_deepest = true;

			if (!TransactionIdEquals(tk->xid, xid) || tk->local_only)
				continue;
			if (tk->op_type != SLOG_OP_UPDATE)
				continue;
			if (tk->before_image == NULL)
				continue;

			/* Skip unless this is the deepest (oldest) node for its key */
			for (deeper = tk->next; deeper != NULL; deeper = deeper->next)
			{
				if (!TransactionIdEquals(deeper->xid, xid) ||
					deeper->local_only ||
					deeper->op_type != SLOG_OP_UPDATE)
					continue;
				if (deeper->key.relid == tk->key.relid &&
					ItemPointerEquals(&deeper->key.tid, &tk->key.tid))
				{
					is_deepest = false;
					break;
				}
			}
			if (!is_deepest)
				continue;

			dp = SLogDsaAllocateBeforeImage(tk->before_image,
											tk->before_image_len,
											tk->before_flags,
											tk->before_commit_ts);
			if (!DsaPointerIsValid(dp))
			{
				TimestampTz now = GetCurrentTimestamp();

				if (slog_overflow_last_warning == 0 ||
					TimestampDifferenceExceeds(slog_overflow_last_warning,
											   now, 10000))
				{
					slog_overflow_last_warning = now;
					elog(WARNING, "sLog: DSA before-image allocation failed "
						 "(limit %d MB); MVCC before-image serving degraded "
						 "for rel %u tid (%u,%u)",
						 slog_dsa_max_size_mb,
						 tk->key.relid,
						 ItemPointerGetBlockNumber(&tk->key.tid),
						 ItemPointerGetOffsetNumber(&tk->key.tid));
				}
				continue;
			}

			tk->before_image_dp = dp;

			memset(&update_op, 0, sizeof(update_op));
			update_op.kind = SLOG_FLAT_OP_UPDATE_OP;
			update_op.key = tk->key;
			update_op.xid = xid;
			update_op.before_image_dp = dp;

			LWLockAcquire(SLOG_PART_WRITER_LOCK(&tk->key), LW_EXCLUSIVE);
			LRLockWriteBegin(SLOG_PART_LRLOCK(&tk->key));
			LRLockApplyOp(SLOG_PART_LRLOCK(&tk->key), &update_op,
						  sizeof(update_op));
			LRLockPublish(SLOG_PART_LRLOCK(&tk->key));
			LRLockWriteEnd(SLOG_PART_LRLOCK(&tk->key));
			LWLockRelease(SLOG_PART_WRITER_LOCK(&tk->key));
		}
	}

	/* Batch apply COMMIT_XID ops grouped by partition */
	{
		int		part;

		for (part = 0; part < SLogNumPartitions; part++)
		{
			SLogFlatPartition *fp = SLogGetPartitionByIndex(part);
			bool		has_entries = false;

			for (tk = slog_tracked_keys; tk != NULL; tk = tk->next)
			{
				if (!TransactionIdEquals(tk->xid, xid) || tk->local_only)
					continue;
				if (SLogFlatHashPartitionIndex(&tk->key) == part)
				{
					has_entries = true;
					break;
				}
			}
			if (!has_entries)
				continue;

			LWLockAcquire(&fp->writer_lock.lock, LW_EXCLUSIVE);
			LRLockWriteBegin(fp->lrlock);

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
				LRLockApplyOp(fp->lrlock, &flat_op, sizeof(flat_op));
			}

			LRLockPublish(fp->lrlock);
			LRLockWriteEnd(fp->lrlock);
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

	LWLockAcquire(SLOG_PART_WRITER_LOCK(&key), LW_EXCLUSIVE);
	LRLockWriteBegin(SLOG_PART_LRLOCK(&key));
	LRLockApplyOp(SLOG_PART_LRLOCK(&key), &flat_op, sizeof(flat_op));
	LRLockPublish(SLOG_PART_LRLOCK(&key));
	LRLockWriteEnd(SLOG_PART_LRLOCK(&key));
	LWLockRelease(SLOG_PART_WRITER_LOCK(&key));
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

	LWLockAcquire(SLOG_PART_WRITER_LOCK(&key), LW_EXCLUSIVE);
	LRLockWriteBegin(SLOG_PART_LRLOCK(&key));
	LRLockApplyOp(SLOG_PART_LRLOCK(&key), &flat_op, sizeof(flat_op));
	LRLockPublish(SLOG_PART_LRLOCK(&key));
	LRLockWriteEnd(SLOG_PART_LRLOCK(&key));
	LWLockRelease(SLOG_PART_WRITER_LOCK(&key));
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
		LRLockWriteBegin(SLOG_PART_LRLOCK(&tk->key));
		LRLockApplyOp(SLOG_PART_LRLOCK(&tk->key), &flat_op, sizeof(flat_op));
		LRLockPublish(SLOG_PART_LRLOCK(&tk->key));
		LRLockWriteEnd(SLOG_PART_LRLOCK(&tk->key));
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
			LRLockWriteBegin(SLOG_PART_LRLOCK(&tk->key));
			LRLockApplyOp(SLOG_PART_LRLOCK(&tk->key), &flat_op, sizeof(flat_op));
			LRLockPublish(SLOG_PART_LRLOCK(&tk->key));
			LRLockWriteEnd(SLOG_PART_LRLOCK(&tk->key));
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
	SLogInsertMap *im;
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

		/* Quick check: any sparsemap entries route to this partition? */
		for (im = slog_insert_maps; im != NULL && !has_entries; im = im->next)
		{
			if (!sm_is_empty(im->map))
				has_entries = true;	/* conservative; checked per-entry below */
		}

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

		if (!has_entries && slog_insert_maps == NULL)
			continue;

		LWLockAcquire(&fp->writer_lock.lock, LW_EXCLUSIVE);

		/*
		 * Collect the live before-image pointers for this transaction's
		 * shared ops in this partition.  We are the single owner of these
		 * frees: this runs under the partition writer lock, so the UNDO
		 * worker's SLogTupleRemoveByXidGlobal (which also takes the writer
		 * lock and only frees still-valid dps) cannot race us, and the
		 * MARK_ABORTED op below nulls before_image_dp in both copies so the
		 * worker sees InvalidDsaPointer afterward.  We record the dps now but
		 * free them only after LRLockPublish has drained readers off the old
		 * copy, so no wait-free reader can dereference a freed pointer.
		 */
		{
			const SLogFlatHash *rht = (const SLogFlatHash *)
				LRLockGetReadData(fp->lrlock);

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

		LRLockWriteBegin(fp->lrlock);

		/*
		 * Publish-and-recycle periodically inside the apply loops.  This
		 * bounds two resources that an unbounded single publish cycle would
		 * blow on a very large rollback (hundreds of thousands of tuples):
		 *
		 *   1. The LRLock oplog, which lives in the fixed main shared-memory
		 *      arena and is regrown (doubling, never freeing the old buffer)
		 *      via a runtime ShmemAlloc whenever a single cycle overflows its
		 *      initial 4 KB.  Each LRLockPublish resets oplog_used to 0.
		 *
		 *   2. The LRLock writer spinlock (writer_mutex), held from
		 *      LRLockWriteBegin to LRLockWriteEnd.  Holding a spinlock across
		 *      that many ops trips the stuck-spinlock detector (PANIC).  We
		 *      therefore call WriteEnd + WriteBegin at each batch boundary to
		 *      release and re-acquire it, keeping the hold time bounded.
		 *
		 * Correctness across the recycle: each tuple's abort visibility is
		 * resolved independently, and a publish leaves both copies in sync, so
		 * dropping the spinlock at a batch boundary leaves the partition
		 * consistent for any other writer that interleaves a complete cycle.
		 * The partition writer LWLock (held for the whole function) still
		 * serializes us against SLogTupleRemoveByXidGlobal, the only other
		 * before-image freer, so deferring the dsa_free calls until after the
		 * final publish remains safe.
		 */
		ops_since_publish = 0;

		/* Process sparsemap-based local-only INSERTs for this partition */
		for (im = slog_insert_maps; im != NULL; im = im->next)
		{
			uint64		idx;

			idx = sm_minimum(im->map);
			while (SM_FOUND(idx))
			{
				SLogFlatOp	flat_op;
				SLogTupleKey smkey;

				memset(&smkey, 0, sizeof(smkey));
				smkey.relid = im->relid;
				ItemPointerSet(&smkey.tid,
							   SLOG_DECODE_BLKNO(idx),
							   SLOG_DECODE_OFFNUM(idx));

				/* Only process if this key belongs to current partition */
				if (SLogFlatHashPartitionIndex(&smkey) == part)
				{
					memset(&flat_op, 0, sizeof(flat_op));
					flat_op.kind = SLOG_FLAT_OP_CREATE_ABORTED;
					flat_op.key = smkey;
					flat_op.xid = xid;
					flat_op.subxid = InvalidTransactionId;
					LRLockApplyOp(fp->lrlock, &flat_op, sizeof(flat_op));

					if (++ops_since_publish >= SLOG_ABORT_PUBLISH_BATCH)
					{
						LRLockPublish(fp->lrlock);
						LRLockWriteEnd(fp->lrlock);
						LRLockWriteBegin(fp->lrlock);
						ops_since_publish = 0;
					}
				}

				idx = sm_next_member(im->map, idx);
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

			LRLockApplyOp(fp->lrlock, &flat_op, sizeof(flat_op));

			if (++ops_since_publish >= SLOG_ABORT_PUBLISH_BATCH)
			{
				LRLockPublish(fp->lrlock);
				LRLockWriteEnd(fp->lrlock);
				LRLockWriteBegin(fp->lrlock);
				ops_since_publish = 0;
			}
		}

		LRLockPublish(fp->lrlock);
		LRLockWriteEnd(fp->lrlock);

		/*
		 * Readers have now been drained off the old copy by LRLockPublish, so
		 * the before-images are unreachable.  Free them while still holding
		 * the writer lock (single-owner guarantee).
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
	 * Holding the LWLock guarantees no LRLock publish can occur for this
	 * partition, so the read copy is stable.  We free the before-images here
	 * (under the LWLock but outside the LRLock spinlock section, since
	 * dsa_free() may take an LWLock) and then apply REMOVE_XID, which also
	 * nulls before_image_dp in both copies.  Each dp is freed exactly once.
	 */
	for (part = 0; part < SLogNumPartitions; part++)
	{
		SLogFlatPartition *fp = SLogGetPartitionByIndex(part);
		const SLogFlatHash *ht;
		SLogFlatHashScanState scan;
		const SLogFlatBucket *bucket;
		int			nkeys = 0;
		int			ndps = 0;
		int			i;

		LWLockAcquire(&fp->writer_lock.lock, LW_EXCLUSIVE);

		ht = (const SLogFlatHash *) LRLockGetReadData(fp->lrlock);
		SLogFlatHashScanInit(&scan);
		while ((bucket = SLogFlatHashScanNext(ht, &scan)) != NULL)
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

		/* Free before-images (under LWLock, outside the LRLock spinlock). */
		for (i = 0; i < ndps; i++)
			dsa_free(slog_dsa_handle, collected_dps[i]);

		/* Null the dangling pointers in both copies and drop the slots. */
		if (nkeys > 0)
		{
			LRLockWriteBegin(fp->lrlock);
			for (i = 0; i < nkeys; i++)
			{
				SLogFlatOp	flat_op;

				memset(&flat_op, 0, sizeof(flat_op));
				flat_op.kind = SLOG_FLAT_OP_REMOVE_XID;
				flat_op.key = collected_keys[i];
				flat_op.xid = xid;
				LRLockApplyOp(fp->lrlock, &flat_op, sizeof(flat_op));
			}
			LRLockPublish(fp->lrlock);
			LRLockWriteEnd(fp->lrlock);
		}

		LWLockRelease(&fp->writer_lock.lock);
	}

	pfree(collected_keys);
	pfree(collected_dps);
}

/*
 * SLogTupleIterateByTid
 *		Call a callback for each active operation on a tuple.
 *
 * WAIT-FREE: uses LRLock read-side.  The callback receives pointers
 * into the read copy; the callback must not hold the pointers beyond
 * the iteration (they are invalidated by LRLockReadEnd).
 */
void
SLogTupleIterateByTid(Oid relid, ItemPointer tid,
					  SLogTupleIterCallback callback, void *arg)
{
	SLogTupleKey key;
	const SLogFlatHash *ht;
	const SLogFlatBucket *bucket;
	int			i;

	memset(&key, 0, sizeof(key));
	key.relid = relid;
	ItemPointerCopy(tid, &key.tid);

	ht = (const SLogFlatHash *) LRLockReadBegin(SLOG_PART_LRLOCK(&key));

	bucket = SLogFlatHashProbe(ht, &key);

	if (bucket != NULL)
	{
		const SLogTupleEntry *entry = &bucket->entry;

		for (i = 0; i < SLOG_MAX_TUPLE_OPS; i++)
		{
			if (entry->ops[i].in_use)
			{
				if (!callback(&entry->ops[i], arg))
					break;
			}
		}
	}

	LRLockReadEnd(SLOG_PART_LRLOCK(&key));
}

/* ----------------------------------------------------------------
 * Convenience wrappers
 * ----------------------------------------------------------------
 */

/*
 * SLogTupleHasEntry -- quick probe: does ANY active entry exist for this TID?
 * WAIT-FREE: uses LRLock read-side.
 */
bool
SLogTupleHasEntry(Oid relid, ItemPointer tid)
{
	SLogTupleKey key;
	const SLogFlatHash *ht;
	const SLogFlatBucket *bucket;
	bool		has_entry = false;

	memset(&key, 0, sizeof(key));
	key.relid = relid;
	ItemPointerCopy(tid, &key.tid);

	ht = (const SLogFlatHash *) LRLockReadBegin(SLOG_PART_LRLOCK(&key));

	bucket = SLogFlatHashProbe(ht, &key);
	if (bucket != NULL && bucket->entry.nops > 0)
		has_entry = true;

	LRLockReadEnd(SLOG_PART_LRLOCK(&key));

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

	/* Check shared hash first (normal case) */
	nfound = SLogTupleLookupFiltered(relid, tid, myxid, &op, 1);
	if (nfound > 0 && op.op_type == SLOG_OP_INSERT)
		return true;

	/*
	 * Check sparsemap-based INSERT tracking (top-level local-only INSERTs).
	 */
	{
		SLogInsertMap *im;
		BlockNumber blkno = ItemPointerGetBlockNumber(tid);
		OffsetNumber offnum = ItemPointerGetOffsetNumber(tid);
		uint64		encoded = SLOG_ENCODE_TID(blkno, offnum);

		for (im = slog_insert_maps; im != NULL; im = im->next)
		{
			if (im->relid == relid && sm_contains(im->map, encoded))
				return true;
		}
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
 * WAIT-FREE: uses LRLock read-side (atomic epoch increment + pointer load).
 * This eliminates the buffer-lock / sLog-lock deadlock entirely.
 */
TransactionId
SLogTupleGetDirtyXid(Oid relid, ItemPointer tid, bool *is_insert)
{
	SLogTupleKey key;
	const SLogFlatHash *ht;
	const SLogFlatBucket *bucket;
	TransactionId result = InvalidTransactionId;
	int			i;

	memset(&key, 0, sizeof(key));
	key.relid = relid;
	ItemPointerCopy(tid, &key.tid);

	ht = (const SLogFlatHash *) LRLockReadBegin(SLOG_PART_LRLOCK(&key));

	bucket = SLogFlatHashProbe(ht, &key);

	if (bucket != NULL)
	{
		const SLogTupleEntry *entry = &bucket->entry;

		for (i = 0; i < SLOG_MAX_TUPLE_OPS; i++)
		{
			TransactionId xid;
			SLogOpType	op;

			if (!entry->ops[i].in_use)
				continue;

			xid = entry->ops[i].xid;
			op = entry->ops[i].op_type;

			if (TransactionIdIsCurrentTransactionId(xid))
				continue;
			if (!TransactionIdIsInProgress(xid))
				continue;

			if (is_insert)
				*is_insert = (op == SLOG_OP_INSERT);
			result = xid;
			break;
		}
	}

	LRLockReadEnd(SLOG_PART_LRLOCK(&key));

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
 * WAIT-FREE: uses LRLock read-side, identical to SLogTupleGetDirtyXid.
 */
TransactionId
SLogTupleGetDirtyWriterXid(Oid relid, ItemPointer tid, bool *is_insert)
{
	SLogTupleKey key;
	const SLogFlatHash *ht;
	const SLogFlatBucket *bucket;
	TransactionId result = InvalidTransactionId;
	int			i;

	memset(&key, 0, sizeof(key));
	key.relid = relid;
	ItemPointerCopy(tid, &key.tid);

	ht = (const SLogFlatHash *) LRLockReadBegin(SLOG_PART_LRLOCK(&key));

	bucket = SLogFlatHashProbe(ht, &key);

	if (bucket != NULL)
	{
		const SLogTupleEntry *entry = &bucket->entry;

		for (i = 0; i < SLOG_MAX_TUPLE_OPS; i++)
		{
			TransactionId xid;
			SLogOpType	op;

			if (!entry->ops[i].in_use)
				continue;

			op = entry->ops[i].op_type;

			/* Only real writers block another writer. */
			if (op != SLOG_OP_INSERT &&
				op != SLOG_OP_UPDATE &&
				op != SLOG_OP_DELETE)
				continue;

			xid = entry->ops[i].xid;

			if (TransactionIdIsCurrentTransactionId(xid))
				continue;
			if (!TransactionIdIsInProgress(xid))
				continue;

			if (is_insert)
				*is_insert = (op == SLOG_OP_INSERT);
			result = xid;
			break;
		}
	}

	LRLockReadEnd(SLOG_PART_LRLOCK(&key));

	return result;
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
 * OOM optimization: When not inside a subtransaction, uses a compact
 * sparsemap (1 bit per TID, RLE-compressed) instead of a 136-byte linked
 * list node.  This reduces memory for 10M sequential INSERTs from ~1.3 GB
 * to ~2-5 MB.  Subtransaction entries still use the linked list because
 * subtxn abort needs per-entry subxid filtering.
 */
void
SLogTupleTrackLocalOnly(Oid relid, ItemPointer tid,
						TransactionId xid, TransactionId subxid)
{
	MemoryContext oldcxt;

	/*
	 * Fast path: top-level transaction with no savepoint → use sparsemap.
	 * The subxid is InvalidTransactionId in this case (top-level INSERTs
	 * always pass the top xid as both xid and subxid=Invalid).
	 */
	if (!IsSubTransaction())
	{
		SLogInsertMap *im;
		BlockNumber blkno = ItemPointerGetBlockNumber(tid);
		OffsetNumber offnum = ItemPointerGetOffsetNumber(tid);
		uint64		encoded = SLOG_ENCODE_TID(blkno, offnum);

		oldcxt = MemoryContextSwitchTo(TopTransactionContext);

		/* Find or create the insert map for this relid */
		for (im = slog_insert_maps; im != NULL; im = im->next)
		{
			if (im->relid == relid)
				break;
		}

		if (im == NULL)
		{
			im = (SLogInsertMap *) palloc(sizeof(SLogInsertMap));
			im->relid = relid;
			im->map = sm_create(SLOG_INSERT_MAP_INIT_SIZE);
			if (unlikely(im->map == NULL))
			{
				/* Allocation failed — fall back to linked list */
				pfree(im);
				MemoryContextSwitchTo(oldcxt);
				goto fallback_linked_list;
			}
			im->next = slog_insert_maps;
			slog_insert_maps = im;
		}

		/* Add the TID bit; sm_add_grow handles buffer expansion */
		if (sm_add_grow(&im->map, encoded) == SM_IDX_MAX)
		{
			/*
			 * Extremely unlikely: sparsemap growth failed.  Fall back to
			 * linked list for this entry with a WARNING.
			 */
			MemoryContextSwitchTo(oldcxt);
			elog(WARNING, "sLog INSERT sparsemap growth failed for rel %u, "
				 "falling back to linked-list tracking", relid);
			goto fallback_linked_list;
		}

		MemoryContextSwitchTo(oldcxt);
		return;
	}

fallback_linked_list:
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
		 * The shared DSA before-image (for cross-backend MVCC reads by
		 * snapshot-isolation transactions) is NOT allocated here.  It is
		 * deferred to commit time (SLogTupleCommitByXid), gated on whether
		 * any REPEATABLE READ / SERIALIZABLE reader is actually active.
		 *
		 * The shared copy is invisible to readers until its op carries a
		 * committed commit_hlc, so allocating it at write time serves no
		 * reader before commit anyway.  Under READ COMMITTED-only workloads
		 * (the common case) no isolation reader ever exists, so the
		 * allocation plus the extra exclusive-LWLock UPDATE_OP publish cycle
		 * done here per update were pure waste.
		 *
		 * Deferring to commit is also the only SOUND gate: a reader that
		 * starts AFTER this write but BEFORE our commit still needs the
		 * image, so a write-time RecnoHasActiveIsoReaders() check would miss
		 * it.  At commit the seq-cst publish-before-snapshot handshake
		 * (reader bumps active_iso_readers before taking its snapshot HLC; we
		 * read the counter after stamping our commit HLC) guarantees we
		 * observe every reader whose snapshot precedes our commit.
		 *
		 * The local copy stored above still covers savepoint rollback, which
		 * is backend-local and does not depend on the DSA copy.
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
 * SLogTupleGetSharedBeforeImage
 *		Retrieve a committed before-image from shared DSA for MVCC reads.
 *
 * Looks for a committed UPDATE entry on (relid, tid) whose committing xid is
 * NOT visible to the reader's MVCC snapshot (it ran concurrently with, or
 * after, the snapshot).  If found, copies the DSA-resident before-image into
 * a palloc'd buffer and returns true.
 *
 * The caller should serve this data instead of the on-page (post-update)
 * data when the reader's snapshot pre-dates the update commit.
 *
 * AUTHORITY IS THE XID SNAPSHOT, NOT THE HLC: see the rationale on
 * SLogTupleHasCommittedUpdateAfter().  A commit_hlc comparison races with the
 * PRE_COMMIT-stamp / ProcArray-exit ordering and would mis-serve the on-page
 * (new) value to a snapshot that should still see the before-image.
 *
 * Safety: the DSA memory is only freed by SLogTupleCleanupRetained which
 * runs after confirming no active snapshot needs it.
 */
bool
SLogTupleGetSharedBeforeImage(Oid relid, ItemPointer tid,
							  Snapshot snapshot,
							  char **data_out, int *len_out,
							  uint16 *flags_out, uint64 *orig_commit_ts_out)
{
	SLogTupleKey key;
	const SLogFlatHash *ht;
	const SLogFlatBucket *bucket;
	bool		found = false;
	dsa_pointer target_dp = InvalidDsaPointer;

	if (SLogState == NULL || snapshot == NULL)
		return false;

	memset(&key, 0, sizeof(key));
	key.relid = relid;
	ItemPointerCopy(tid, &key.tid);

	/*
	 * Read-side probe: find the relevant before-image DSA pointer.
	 * The LRLock read-side guarantees the entry won't be freed while we
	 * hold the epoch.  We copy the dsa_pointer value during the read window.
	 */
	ht = (const SLogFlatHash *) LRLockReadBegin(SLOG_PART_LRLOCK(&key));

	bucket = SLogFlatHashProbe(ht, &key);
	if (bucket != NULL)
	{
		const SLogTupleEntry *entry = &bucket->entry;
		int		i;

		for (i = 0; i < SLOG_MAX_TUPLE_OPS; i++)
		{
			if (!entry->ops[i].in_use)
				continue;
			if (entry->ops[i].op_type != SLOG_OP_UPDATE)
				continue;
			if (entry->ops[i].commit_hlc == 0)
				continue;	/* uncommitted */
			if (!TransactionIdIsValid(entry->ops[i].xid))
				continue;
			if (!XidInMVCCSnapshot(entry->ops[i].xid, snapshot))
				continue;	/* visible to reader: on-page value is correct */
			if (!DsaPointerIsValid(entry->ops[i].before_image_dp))
				continue;

			target_dp = entry->ops[i].before_image_dp;
			break;
		}
	}

	LRLockReadEnd(SLOG_PART_LRLOCK(&key));

	/*
	 * If we found a target, access the DSA memory outside the read lock.
	 * The DSA memory is only freed by SLogTupleCleanupRetained which runs
	 * after confirming no active snapshot needs it.
	 */
	if (DsaPointerIsValid(target_dp))
	{
		SLogBeforeImage *bi;

		SLogEnsureDsaAttached();
		bi = (SLogBeforeImage *)
			dsa_get_address(slog_dsa_handle, target_dp);

		*data_out = (char *) palloc(bi->len);
		memcpy(*data_out, bi->data, bi->len);
		*len_out = (int) bi->len;
		*flags_out = bi->flags;
		*orig_commit_ts_out = bi->commit_ts;
		found = true;
	}

	return found;
}

/*
 * SLogTupleHasCommittedUpdateAfter
 *		Write-write conflict probe for the in-place UPDATE path.
 *
 * Returns true if (relid, tid) has a retained committed UPDATE marker whose
 * committing xid is NOT visible to the caller's MVCC snapshot (i.e. it ran
 * concurrently with, or after, the snapshot) and is not exclude_xid (our own).
 * Such a marker means another transaction updated-and-committed this tuple
 * after our statement snapshot was taken, so proceeding with an in-place
 * overwrite would lose that update.  The caller returns TM_Updated to drive
 * EvalPlanQual re-evaluation, matching heap's behaviour when a scanned tuple's
 * xmax committed concurrently with our snapshot.
 *
 * TWO CONFLICT ARMS, because the reader's value-read and the conflict probe
 * live in different snapshot domains and a committer that lands in the gap
 * between them must still be caught:
 *
 *   (1) XID ARM -- XidInMVCCSnapshot(xid): the committer is concurrent with
 *       (in-progress in) our core MVCC snapshot.  This is heap's own
 *       visibility authority.  A "commit_hlc <= anchor" test could NOT
 *       replace this: a committer stamps its commit HLC at PRE_COMMIT but
 *       leaves the ProcArray strictly later, so a reader still seeing it
 *       in-progress reads a current HLC >= its commit_hlc and would wrongly
 *       skip it.  HLC must never be used to *subtract* from the XID arm.
 *
 *   (2) HLC ARM -- read_anchor_hlc != 0 && commit_hlc > read_anchor_hlc:
 *       catches the complementary skew.  Under a point-in-time snapshot
 *       (REPEATABLE READ / SERIALIZABLE) the value read uses the
 *       transaction-start HLC (read_anchor_hlc), which is captured at a
 *       different instant than the core MVCC snapshot's xip list.  A
 *       committer can have ALREADY left the ProcArray (so XidInMVCCSnapshot
 *       returns false -- the XID arm sees no conflict) yet have committed
 *       AFTER our read anchor (commit_hlc > read_anchor_hlc), so the reader
 *       was served that tuple's before-image and read the stale value.
 *       Overwriting in place would then clobber the committed update (the
 *       residual CAS-path lost update verified empirically).  This arm only
 *       ADDS conflicts; it is exactly heap's REPEATABLE READ rule (a row
 *       modified after our snapshot fails an in-place UPDATE -> TM_Updated).
 *       READ COMMITTED passes read_anchor_hlc = 0 to disable this arm: its
 *       value read uses a floating HLCNow() per check, so a committed-after
 *       update is meant to become visible on re-read, not raise a conflict.
 *
 * The EPQ dedup floor applies to BOTH arms: epq_floor_hlc (0 = none)
 * suppresses re-firing on a marker a prior EvalPlanQual on this exact
 * (relid, tid, curcid) already reconciled, which would otherwise livelock
 * because EPQ reuses the same snapshot.
 *
 * RECNO needs this explicit probe because committed in-place UPDATEs rewind
 * t_commit_ts to the original insert timestamp (so mid-life snapshots still
 * see the row), erasing the "updated since you read it" signal that heap keeps
 * in xmax.  The sLog marker is the only durable record of that event.
 *
 * WAIT-FREE: uses the LRLock read-side, no buffer or heavyweight locks.
 */
bool
SLogTupleHasCommittedUpdateAfter(Oid relid, ItemPointer tid,
								 Snapshot snapshot,
								 uint64 epq_floor_hlc,
								 TransactionId exclude_xid)
{
	SLogTupleKey key;
	const SLogFlatHash *ht;
	const SLogFlatBucket *bucket;
	bool		found = false;
	TransactionId unstamped_cands[SLOG_MAX_TUPLE_OPS];
	int			n_unstamped = 0;

	if (SLogState == NULL || snapshot == NULL)
		return false;

	memset(&key, 0, sizeof(key));
	key.relid = relid;
	ItemPointerCopy(tid, &key.tid);

	ht = (const SLogFlatHash *) LRLockReadBegin(SLOG_PART_LRLOCK(&key));

	bucket = SLogFlatHashProbe(ht, &key);
	if (bucket != NULL)
	{
		const SLogTupleEntry *entry = &bucket->entry;
		int		i;

		for (i = 0; i < SLOG_MAX_TUPLE_OPS; i++)
		{
			if (!entry->ops[i].in_use)
				continue;
			if (entry->ops[i].op_type != SLOG_OP_UPDATE)
				continue;
			if (TransactionIdIsValid(exclude_xid) &&
				TransactionIdEquals(entry->ops[i].xid, exclude_xid))
				continue;		/* our own update */
			if (!TransactionIdIsValid(entry->ops[i].xid))
				continue;
			/*
			 * Conflict only if the committer is in-progress/concurrent in our
			 * core MVCC snapshot.  XidInMVCCSnapshot() returns true when the xid
			 * is NOT visible to the snapshot, which is exactly the conflict case
			 * (the committed update happened at or after our snapshot, so taking
			 * the CAS fast path here would silently clobber it).  HLC values are
			 * never used to add or subtract conflicts: the xid-in-snapshot test
			 * is authoritative.
			 */
			if (!XidInMVCCSnapshot(entry->ops[i].xid, snapshot))
				continue;

			if (entry->ops[i].commit_hlc == 0)
			{
				/*
				 * commit_hlc == 0 normally means "uncommitted, handled by the
				 * dirty-xid wait path".  But commit_hlc is a derived cache of
				 * CLOG state stamped lazily at PRE_COMMIT, and under saturation
				 * that stamp can be missed -- leaving a marker committed in
				 * CLOG but unstamped.  Such a marker is a genuine write-write
				 * conflict (XidInMVCCSnapshot already proved it invisible to
				 * our snapshot) that we must not silently drop.  We cannot call
				 * TransactionIdDidCommit() here: it may take an SLRU LWLock,
				 * and we are inside the wait-free LRLock read section (an odd
				 * epoch blocks the writer's publish).  Defer the CLOG check to
				 * after LRLockReadEnd().  EPQ dedup cannot apply (no HLC to
				 * compare), so an unstamped committed marker conservatively
				 * conflicts -- correct, and vanishingly rare.
				 */
				unstamped_cands[n_unstamped++] = entry->ops[i].xid;
				continue;
			}
			/*
			 * EPQ dedup: a prior EvalPlanQual on this (relid, tid, curcid)
			 * already reconciled commits up to epq_floor_hlc; do not re-fire
			 * for those, only for a strictly newer committer.
			 */
			if (epq_floor_hlc != 0 &&
				entry->ops[i].commit_hlc <= epq_floor_hlc)
				continue;

			found = true;
			break;
		}
	}

	LRLockReadEnd(SLOG_PART_LRLOCK(&key));

	/*
	 * Resolve any unstamped candidates against CLOG outside the read section.
	 * A committed xid is a real conflict; an in-progress one is left to the
	 * dirty-xid wait path (we already verified XidInMVCCSnapshot above).
	 */
	if (!found && n_unstamped > 0)
	{
		int		i;

		for (i = 0; i < n_unstamped; i++)
		{
			if (TransactionIdDidCommit(unstamped_cands[i]))
			{
				found = true;
				break;
			}
		}
	}

	return found;
}

/*
 * SLogTupleCleanupRetained
 *		Free retained committed UPDATE entries that are no longer visible
 *		to any active snapshot.
 *
 * A retained UPDATE marker carries two signals an active reader may still
 * need: the DSA before-image (so an old snapshot can reconstruct the
 * pre-update version via SLogTupleGetSharedBeforeImage) and the marker
 * itself (so a concurrent updater can detect the committed update via
 * SLogTupleHasCommittedUpdateAfter, i.e. lost-update/write-write
 * detection).  BOTH of those decide by XidInMVCCSnapshot against the core
 * MVCC snapshot -- xid authority, not HLC comparison.  Reclamation must
 * therefore gate on the xid horizon (GetOldestNonRemovableTransactionId),
 * exactly like the per-TID reclaim in flat_hash_apply_insert: a marker is
 * reclaimable only once its committing xid precedes the oldest active
 * snapshot's xmin, at which point it is visible to every live snapshot and
 * no probe can still need it.  An HLC-horizon gate
 * (commit_hlc < oldest_snapshot_hlc) races with the PRE_COMMIT-stamp /
 * ProcArray-exit skew and frees a marker a concurrent updater still needs,
 * reintroducing the lost update (verified: c=16 RR loses ~2/12 runs with
 * the HLC gate, 0/12 with the xid gate).
 *
 * The oldest_snapshot_hlc argument now serves only as a cheap "retention
 * subsystem is warm" guard; the actual reclamation decision is the xid
 * horizon computed below.
 *
 * Scans the flat hash under read-side, then applies CLEANUP_RETAINED ops.
 * Called periodically by the UNDO background worker.
 */
void
SLogTupleCleanupRetained(uint64 oldest_snapshot_hlc)
{
	SLogTupleKey *collected_keys;
	dsa_pointer *collected_dps;
	int			max_keys = 256;
	int			part;
	int			i;
	TransactionId reclaim_xid_horizon;

	if (SLogState == NULL || oldest_snapshot_hlc == 0)
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
	 * SLogTupleCleanupRetained runs inline in every backend, so without this
	 * mutual exclusion two backends would scan the same partition, collect
	 * the same before_image_dp, and dsa_free() it twice (a double-free that
	 * trips the index < DSA_MAX_SEGMENTS assertion in dsa.c).
	 *
	 * Holding the LWLock also guarantees no LRLock publish can occur for this
	 * partition, so the read copy is stable to scan.  We free the DSA
	 * before-images here (under the LWLock but outside the LRLock spinlock
	 * section, since dsa_free() may itself take an LWLock) and then apply the
	 * CLEANUP_RETAINED ops that null out the now-dangling pointers in both
	 * copies.  Each before-image is therefore freed exactly once.
	 */
	for (part = 0; part < SLogNumPartitions; part++)
	{
		SLogFlatPartition *fp = SLogGetPartitionByIndex(part);
		const SLogFlatHash *ht;
		SLogFlatHashScanState scan;
		const SLogFlatBucket *bucket;
		int			nkeys = 0;
		int			ndps = 0;

		LWLockAcquire(&fp->writer_lock.lock, LW_EXCLUSIVE);

		/*
		 * Scan the current read copy to collect expired keys and their
		 * before-image pointers.  Stable because we hold the writer lock.
		 */
		ht = (const SLogFlatHash *) LRLockGetReadData(fp->lrlock);
		SLogFlatHashScanInit(&scan);
		while ((bucket = SLogFlatHashScanNext(ht, &scan)) != NULL)
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

		/* Free before-images (under LWLock, outside the LRLock spinlock). */
		for (i = 0; i < ndps; i++)
		{
			if (DsaPointerIsValid(collected_dps[i]))
				dsa_free(slog_dsa_handle, collected_dps[i]);
		}

		/* Null the now-dangling pointers in both copies and drop the slots. */
		if (nkeys > 0)
		{
			int			ops_since_publish = 0;

			LRLockWriteBegin(fp->lrlock);
			for (i = 0; i < nkeys; i++)
			{
				SLogFlatOp	flat_op;

				memset(&flat_op, 0, sizeof(flat_op));
				flat_op.kind = SLOG_FLAT_OP_CLEANUP_RETAINED;
				flat_op.key = collected_keys[i];
				flat_op.reclaim_xid_horizon = reclaim_xid_horizon;
				LRLockApplyOp(fp->lrlock, &flat_op, sizeof(flat_op));

				/*
				 * Publish-and-recycle periodically so the writer spinlock
				 * hold time stays bounded.  On a hot row many retained
				 * UPDATE markers expire at once, so nkeys can be large; a
				 * single uninterrupted cycle would hold writer_mutex too
				 * long (stuck-spinlock PANIC) and let the oplog grow.  Each
				 * key's reclamation is independent and a publish leaves both
				 * copies in sync, so dropping the spinlock at a batch
				 * boundary is safe -- the partition LWLock still serializes
				 * us against other before-image freers.
				 */
				if (++ops_since_publish >= SLOG_ABORT_PUBLISH_BATCH)
				{
					LRLockPublish(fp->lrlock);
					LRLockWriteEnd(fp->lrlock);
					LRLockWriteBegin(fp->lrlock);
					ops_since_publish = 0;
				}
			}
			LRLockPublish(fp->lrlock);
			LRLockWriteEnd(fp->lrlock);
		}

		LWLockRelease(&fp->writer_lock.lock);
	}

	pfree(collected_keys);
	pfree(collected_dps);
}

/*
 * SLogTupleResetTracking
 *		Clear the backend-private tracking list and reset overflow state.
 *
 * Also frees all per-relid INSERT sparsemaps.  The sparsemap structures
 * and their data buffers were allocated in TopTransactionContext, so they
 * would be freed at transaction end anyway.  Explicit cleanup here allows
 * earlier memory reclaim and makes the state consistent for any subsequent
 * operations within the same backend lifetime.
 */
void
SLogTupleResetTracking(void)
{
	SLogInsertMap *im,
			   *im_next;

	/* Free sparsemap-based INSERT tracking */
	for (im = slog_insert_maps; im != NULL; im = im_next)
	{
		im_next = im->next;
		if (im->map != NULL)
			sm_free(im->map);
		pfree(im);
	}
	slog_insert_maps = NULL;

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
 * Iterates both sparsemap-based INSERT entries (top-level) and
 * linked-list entries.
 */
void
SLogTupleIterateTrackedKeys(TransactionId xid,
							SLogTrackedKeyCallback callback,
							void *arg)
{
	SLogTrackedKey *tk;
	SLogInsertMap *im;

	/* Iterate sparsemap-based local-only INSERTs */
	for (im = slog_insert_maps; im != NULL; im = im->next)
	{
		uint64		idx;

		idx = sm_minimum(im->map);
		while (SM_FOUND(idx))
		{
			SLogTupleKey key;

			memset(&key, 0, sizeof(key));
			key.relid = im->relid;
			ItemPointerSet(&key.tid,
						   SLOG_DECODE_BLKNO(idx),
						   SLOG_DECODE_OFFNUM(idx));

			if (!callback(&key, xid, InvalidTransactionId, true, arg))
				return;

			idx = sm_next_member(im->map, idx);
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
	SLogInsertMap *im;

	/* Iterate sparsemap-based local-only INSERTs */
	for (im = slog_insert_maps; im != NULL; im = im->next)
	{
		uint64		idx;

		idx = sm_minimum(im->map);
		while (SM_FOUND(idx))
		{
			SLogTupleKey key;

			memset(&key, 0, sizeof(key));
			key.relid = im->relid;
			ItemPointerSet(&key.tid,
						   SLOG_DECODE_BLKNO(idx),
						   SLOG_DECODE_OFFNUM(idx));

			if (!callback(&key, xid, InvalidTransactionId, true,
						  0, false, arg))
				return;

			idx = sm_next_member(im->map, idx);
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
	SLogInsertMap *im;
	int			count = 0;
	int			capacity = 64;
	SLogTrackedKeyInfo *arr;

	arr = (SLogTrackedKeyInfo *) palloc(sizeof(SLogTrackedKeyInfo) * capacity);

	/* Collect sparsemap-based local-only INSERTs */
	for (im = slog_insert_maps; im != NULL; im = im->next)
	{
		uint64		idx;

		idx = sm_minimum(im->map);
		while (SM_FOUND(idx))
		{
			if (count >= capacity)
			{
				capacity *= 2;
				arr = (SLogTrackedKeyInfo *)
					repalloc(arr, sizeof(SLogTrackedKeyInfo) * capacity);
			}

			memset(&arr[count].key, 0, sizeof(SLogTupleKey));
			arr[count].key.relid = im->relid;
			ItemPointerSet(&arr[count].key.tid,
						   SLOG_DECODE_BLKNO(idx),
						   SLOG_DECODE_OFFNUM(idx));
			arr[count].xid = xid;
			arr[count].subxid = InvalidTransactionId;
			arr[count].local_only = true;
			arr[count].op_type = SLOG_OP_INSERT;
			arr[count].before_commit_ts = 0;
			arr[count].has_before_image = false;
			count++;

			idx = sm_next_member(im->map, idx);
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
				repalloc(arr, sizeof(SLogTrackedKeyInfo) * capacity);
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
