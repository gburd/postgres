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
 * Tuple sLog (SLogTupleHash):
 *   - Keyed by (relid, tid), stores up to SLOG_MAX_TUPLE_OPS concurrent
 *     operations per tuple.  Designed for the RECNO table AM.
 *   - Partitioned by tuple_locks[hash(relid,tid) % NUM_SLOG_TUPLE_PARTITIONS].
 *   - WAL-free: entries are transient, removed at commit/abort.
 *
 * Locking: All locks are from LWTRANCHE_SLOG.
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
#include "access/transam.h"
#include "access/xact.h"
#include "common/hashfn.h"
#include "miscadmin.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "utils/dsa.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
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

	/* Tuple hash partition locks */
	LWLockPadded tuple_locks[NUM_SLOG_TUPLE_PARTITIONS];

	/* DSA area for shared before-images */
	dsa_area   *dsa_area;		/* set during SLogShmemInit, NULL until then */
	char		dsa_space[SLOG_DSA_INIT_SIZE];
} SLogSharedState;

/*
 * SLogTupleNumEntries
 *		Calculate the number of hash entries for the tuple sLog.
 *
 * Auto-sizing formula: MaxBackends * 256, clamped to [1024, 1048576].
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
static HTAB *SLogTupleHash = NULL;
static SLogSharedState *SLogState = NULL;

/* Per-backend DSA attachment (lazy, via SLogEnsureDsaAttached) */
static dsa_area *slog_dsa_handle = NULL;

/* Pointers to ShmemAlloc'd regions, set by ShmemRequestStruct framework */
static char *SLogPoolSlots = NULL;
static char *SLogPoolFreeList = NULL;
static char *SLogXidMapBuffer = NULL;

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

	/* DSA pointer to shared before-image (for abort/rollback cleanup) */
	dsa_pointer before_image_dp;	/* InvalidDsaPointer if none */

	struct SLogTrackedKey *next;
} SLogTrackedKey;

static SLogTrackedKey *slog_tracked_keys = NULL;
static bool slog_has_shared_entries = false;	/* any non-local_only entries? */

/* ----------------------------------------------------------------
 * Internal helpers
 * ----------------------------------------------------------------
 */

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

/*
 * Compute LWLock partition index for tuple operations.
 * Uses hash_bytes for deterministic partitioning matching RECNO's approach.
 */
static inline int
SLogTuplePartition(const SLogTupleKey *key)
{
	return hash_bytes((const unsigned char *) key, sizeof(SLogTupleKey))
		& SLOG_TUPLE_PARTITION_MASK;
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

	/* Tuple hash table */
	size = add_size(size, hash_estimate_size(SLogTupleNumEntries(),
											 sizeof(SLogTupleEntry)));

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
	/* Register the shared state structure */
	ShmemRequestStruct(.name = "Secondary Log State",
					   .size = sizeof(SLogSharedState),
					   .ptr = (void **) &SLogState,
		);

	/* SLogTupleHash: keyed by SLogTupleKey */
	ShmemRequestHash(.name = "sLog Tuple Hash",
					 .nelems = SLogTupleNumEntries(),
					 .ptr = &SLogTupleHash,
					 .hash_info.keysize = sizeof(SLogTupleKey),
					 .hash_info.entrysize = sizeof(SLogTupleEntry),
					 .hash_info.num_partitions = NUM_SLOG_TUPLE_PARTITIONS,
					 .hash_flags = HASH_ELEM | HASH_BLOBS |
					 HASH_PARTITION,
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
	}
}

/*
 * SLogShmemInit
 *		Initialize sLog shared memory contents.
 *
 * Called from UndoShmemInit() during the init_fn phase.  The framework
 * has already allocated SLogState and SLogTupleHash.  We manually
 * initialize the skip-list pool, skip-list, and sparsemap.
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
 * Called when the sLog hash table is full.  Scans the entire hash looking for
 * entries where ALL ops belong to already-committed transactions.  Such
 * entries are safe to remove because committed tuples are visible regardless
 * of sLog state (the commit callback just hasn't fired yet).
 *
 * Returns the number of entries evicted.
 *
 * Locking: acquires ALL partition locks EXCLUSIVE for the duration of the
 * scan, then releases them all.  This is acceptable because overflow is rare.
 */
static int
SLogTupleEvictCommitted(void)
{
	int			evicted = 0;
	int			part;
	HASH_SEQ_STATUS status;
	SLogTupleEntry *hentry;
	SLogTupleKey *keys_to_evict;
	int			nkeys = 0;
	int			max_evict = 128;

	/* Acquire all partition locks EXCLUSIVE */
	for (part = 0; part < NUM_SLOG_TUPLE_PARTITIONS; part++)
		LWLockAcquire(&SLogState->tuple_locks[part].lock, LW_EXCLUSIVE);

	keys_to_evict = (SLogTupleKey *)
		palloc(sizeof(SLogTupleKey) * max_evict);

	hash_seq_init(&status, SLogTupleHash);
	while ((hentry = (SLogTupleEntry *) hash_seq_search(&status)) != NULL)
	{
		bool		all_committed = true;
		bool		has_any_op = false;
		int			i;

		for (i = 0; i < SLOG_MAX_TUPLE_OPS; i++)
		{
			if (!hentry->ops[i].in_use)
				continue;
			has_any_op = true;
			if (TransactionIdIsInProgress(hentry->ops[i].xid) ||
				!TransactionIdDidCommit(hentry->ops[i].xid))
			{
				all_committed = false;
				break;
			}
		}

		if (has_any_op && all_committed)
		{
			keys_to_evict[nkeys++] = hentry->key;
			if (nkeys >= max_evict)
			{
				hash_seq_term(&status);
				break;
			}
		}
	}

	/* Remove collected entries */
	for (part = 0; part < nkeys; part++)
		hash_search(SLogTupleHash, &keys_to_evict[part], HASH_REMOVE, NULL);

	evicted = nkeys;

	/* Release all partition locks */
	for (part = NUM_SLOG_TUPLE_PARTITIONS - 1; part >= 0; part--)
		LWLockRelease(&SLogState->tuple_locks[part].lock);

	pfree(keys_to_evict);

	return evicted;
}

/* ----------------------------------------------------------------
 * Core Tuple sLog API
 * ----------------------------------------------------------------
 */

/*
 * SLogTupleInsert
 *		Record a tuple operation in the sLog.
 *
 * Finds or creates an entry for (relid, tid) and inserts the operation
 * into a free slot in its ops[] array.  Performs overflow handling
 * (stale-slot reclamation, eviction, retry with backoff) before failing.
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
	SLogTupleEntry *entry;
	bool		found;
	int			partition;
	int			i;

	Assert(SLogTupleHash != NULL);
	Assert(TransactionIdIsValid(xid));
	Assert(ItemPointerIsValid(tid));

	/* Zero for deterministic hashing (ItemPointerData is 6 bytes) */
	memset(&key, 0, sizeof(key));
	key.relid = relid;
	ItemPointerCopy(tid, &key.tid);

	partition = SLogTuplePartition(&key);

	LWLockAcquire(&SLogState->tuple_locks[partition].lock, LW_EXCLUSIVE);

	entry = (SLogTupleEntry *)
		hash_search(SLogTupleHash, &key, HASH_ENTER_NULL, &found);

	if (entry == NULL)
	{
		int			evicted;

		/*
		 * Hash table is full.  Release our partition lock before calling
		 * SLogTupleEvictCommitted(), which acquires ALL partition locks.
		 */
		LWLockRelease(&SLogState->tuple_locks[partition].lock);

		evicted = SLogTupleEvictCommitted();

		/* Re-acquire our partition lock and retry */
		LWLockAcquire(&SLogState->tuple_locks[partition].lock, LW_EXCLUSIVE);

		if (evicted > 0)
		{
			elog(DEBUG1, "sLog tuple: evicted %d committed entries on overflow",
				 evicted);

			entry = (SLogTupleEntry *)
				hash_search(SLogTupleHash, &key, HASH_ENTER_NULL, &found);
		}

		if (entry == NULL)
		{
			LWLockRelease(&SLogState->tuple_locks[partition].lock);

			/*
			 * Rate-limit overflow warnings: emit the first, then at most
			 * once per second.  Report total skipped count in the message.
			 */
			slog_overflow_warning_count++;
			{
				TimestampTz now = GetCurrentTimestamp();

				if (slog_overflow_warning_count == 1 ||
					TimestampDifferenceExceeds(slog_overflow_last_warning,
											  now, 1000))
				{
					elog(WARNING, "sLog tuple hash is full (%d entries); "
						 "%d overflow(s) this transaction on rel %u "
						 "(visibility relies on UNCOMMITTED flag + "
						 "UNDO replay)",
						 SLogTupleNumEntries(),
						 slog_overflow_warning_count, relid);
					slog_overflow_last_warning = now;
				}
			}

			/*
			 * Track as local-only so that SLogTupleMarkAborted() will
			 * create a shared ABORTED entry at abort time (needed for
			 * visibility correctness).  On commit,
			 * RecnoClearUncommittedFlags() clears the page flag normally.
			 */
			SLogTupleTrackLocalOnly(relid, tid, xid, subxid);

			return false;
		}
	}

	if (!found)
	{
		entry->nops = 0;
		memset(entry->ops, 0, sizeof(entry->ops));
	}

	/*
	 * Check if this xid already has an op in this entry (e.g., a transaction
	 * that does INSERT then UPDATE on same TID).  If so, overwrite.
	 */
	for (i = 0; i < SLOG_MAX_TUPLE_OPS; i++)
	{
		if (entry->ops[i].in_use &&
			TransactionIdEquals(entry->ops[i].xid, xid))
		{
			entry->ops[i].commit_ts = commit_ts;
			entry->ops[i].spec_token = spec_token;
			entry->ops[i].cid = cid;
			entry->ops[i].op_type = op_type;
			entry->ops[i].subxid = subxid;

			LWLockRelease(&SLogState->tuple_locks[partition].lock);

			SLogTupleTrackKey(key, xid, subxid);
			return true;
		}
	}

	/* Find a free slot */
	for (i = 0; i < SLOG_MAX_TUPLE_OPS; i++)
	{
		if (!entry->ops[i].in_use)
		{
			entry->ops[i].xid = xid;
			entry->ops[i].subxid = subxid;
			entry->ops[i].op_type = op_type;
			entry->ops[i].cid = cid;
			entry->ops[i].commit_ts = commit_ts;
			entry->ops[i].spec_token = spec_token;
			entry->ops[i].commit_hlc = 0;
			entry->ops[i].before_image_dp = InvalidDsaPointer;
			entry->ops[i].in_use = true;
			entry->nops++;

			LWLockRelease(&SLogState->tuple_locks[partition].lock);

			SLogTupleTrackKey(key, xid, subxid);
			return true;
		}
	}

	/*
	 * No free slot.  Attempt stale-slot reclamation: entries whose
	 * transaction has already committed (cleanup callback hasn't fired yet).
	 */
	{
		int			reclaimed = 0;

		for (i = 0; i < SLOG_MAX_TUPLE_OPS; i++)
		{
			if (!entry->ops[i].in_use)
				continue;
			if (TransactionIdEquals(entry->ops[i].xid, xid))
				continue;
			/* Don't reclaim retained committed UPDATE entries */
			if (entry->ops[i].commit_hlc != 0 &&
				DsaPointerIsValid(entry->ops[i].before_image_dp))
				continue;
			if (!TransactionIdIsInProgress(entry->ops[i].xid) &&
				TransactionIdDidCommit(entry->ops[i].xid))
			{
				entry->ops[i].in_use = false;
				entry->nops--;
				reclaimed++;
			}
		}

		if (reclaimed > 0)
		{
			elog(DEBUG2, "sLog: reclaimed %d stale slot(s) on TID (%u,%u) rel %u",
				 reclaimed, ItemPointerGetBlockNumber(tid),
				 ItemPointerGetOffsetNumber(tid), relid);

			/* Retry finding a free slot */
			for (i = 0; i < SLOG_MAX_TUPLE_OPS; i++)
			{
				if (!entry->ops[i].in_use)
				{
					entry->ops[i].xid = xid;
					entry->ops[i].subxid = subxid;
					entry->ops[i].op_type = op_type;
					entry->ops[i].cid = cid;
					entry->ops[i].commit_ts = commit_ts;
					entry->ops[i].spec_token = spec_token;
					entry->ops[i].commit_hlc = 0;
					entry->ops[i].before_image_dp = InvalidDsaPointer;
					entry->ops[i].in_use = true;
					entry->nops++;

					LWLockRelease(&SLogState->tuple_locks[partition].lock);

					SLogTupleTrackKey(key, xid, subxid);
					return true;
				}
			}
		}
	}

	/*
	 * Still no room.  Release lock and retry with backoff.
	 */
	LWLockRelease(&SLogState->tuple_locks[partition].lock);

	{
		int			retry;

		for (retry = 0; retry < 3; retry++)
		{
			pg_usleep(1000);	/* 1ms */

			LWLockAcquire(&SLogState->tuple_locks[partition].lock, LW_EXCLUSIVE);

			entry = (SLogTupleEntry *)
				hash_search(SLogTupleHash, &key, HASH_FIND, NULL);

			if (entry == NULL)
			{
				/* Entry was removed; re-create */
				entry = (SLogTupleEntry *)
					hash_search(SLogTupleHash, &key, HASH_ENTER_NULL, &found);
				if (entry == NULL)
				{
					LWLockRelease(&SLogState->tuple_locks[partition].lock);
					continue;	/* retry after sleep */
				}
				entry->nops = 0;
				memset(entry->ops, 0, sizeof(entry->ops));
			}

			for (i = 0; i < SLOG_MAX_TUPLE_OPS; i++)
			{
				if (!entry->ops[i].in_use)
				{
					entry->ops[i].xid = xid;
					entry->ops[i].subxid = subxid;
					entry->ops[i].op_type = op_type;
					entry->ops[i].cid = cid;
					entry->ops[i].commit_ts = commit_ts;
					entry->ops[i].spec_token = spec_token;
					entry->ops[i].commit_hlc = 0;
					entry->ops[i].before_image_dp = InvalidDsaPointer;
					entry->ops[i].in_use = true;
					entry->nops++;

					LWLockRelease(&SLogState->tuple_locks[partition].lock);

					SLogTupleTrackKey(key, xid, subxid);
					return true;
				}
			}

			LWLockRelease(&SLogState->tuple_locks[partition].lock);
		}
	}

	elog(WARNING, "sLog: too many concurrent operations on TID (%u,%u) in rel %u "
		 "(max %d, all slots occupied after reclamation and retries)",
		 ItemPointerGetBlockNumber(tid),
		 ItemPointerGetOffsetNumber(tid), relid, SLOG_MAX_TUPLE_OPS);
	SLogTupleTrackLocalOnly(relid, tid, xid, subxid);
	return false;
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
	SLogTupleEntry *entry;
	bool		found;
	int			partition;
	int			i;

	if (SLogTupleHash == NULL)
		return false;

	Assert(TransactionIdIsValid(xid));
	Assert(ItemPointerIsValid(tid));

	memset(&key, 0, sizeof(key));
	key.relid = relid;
	ItemPointerCopy(tid, &key.tid);

	partition = SLogTuplePartition(&key);

	LWLockAcquire(&SLogState->tuple_locks[partition].lock, LW_EXCLUSIVE);

	entry = (SLogTupleEntry *)
		hash_search(SLogTupleHash, &key, HASH_ENTER_NULL, &found);

	if (entry == NULL)
	{
		/*
		 * Hash full.  Try eviction, but if that also fails, give up
		 * silently.  The worst case is a brief visibility anomaly for
		 * aborted tuples until the CLR arrives.
		 */
		LWLockRelease(&SLogState->tuple_locks[partition].lock);

		(void) SLogTupleEvictCommitted();

		LWLockAcquire(&SLogState->tuple_locks[partition].lock, LW_EXCLUSIVE);

		entry = (SLogTupleEntry *)
			hash_search(SLogTupleHash, &key, HASH_ENTER_NULL, &found);

		if (entry == NULL)
		{
			LWLockRelease(&SLogState->tuple_locks[partition].lock);
			elog(DEBUG1, "sLog recovery: hash full, skipping entry for xid %u",
				 xid);
			return false;
		}
	}

	if (!found)
	{
		entry->nops = 0;
		memset(entry->ops, 0, sizeof(entry->ops));
	}

	/* Check if this xid already has an op for this TID (e.g., UPDATE after INSERT) */
	for (i = 0; i < SLOG_MAX_TUPLE_OPS; i++)
	{
		if (entry->ops[i].in_use &&
			TransactionIdEquals(entry->ops[i].xid, xid))
		{
			entry->ops[i].op_type = op_type;
			LWLockRelease(&SLogState->tuple_locks[partition].lock);
			return true;
		}
	}

	/* Find a free slot */
	for (i = 0; i < SLOG_MAX_TUPLE_OPS; i++)
	{
		if (!entry->ops[i].in_use)
		{
			entry->ops[i].xid = xid;
			entry->ops[i].subxid = InvalidTransactionId;
			entry->ops[i].op_type = op_type;
			entry->ops[i].cid = InvalidCommandId;
			entry->ops[i].commit_ts = 0;
			entry->ops[i].spec_token = 0;
			entry->ops[i].commit_hlc = 0;
			entry->ops[i].before_image_dp = InvalidDsaPointer;
			entry->ops[i].in_use = true;
			entry->nops++;

			LWLockRelease(&SLogState->tuple_locks[partition].lock);
			return true;
		}
	}

	/* All slots occupied — give up silently */
	LWLockRelease(&SLogState->tuple_locks[partition].lock);
	elog(DEBUG1, "sLog recovery: no free slot for xid %u on TID (%u,%u)",
		 xid, ItemPointerGetBlockNumber(tid), ItemPointerGetOffsetNumber(tid));
	return false;
}

/*
 * SLogTupleLookup
 *		Look up a tuple's sLog entry (copy semantics).
 *
 * Returns true if found, copying the full entry into *entry_out.
 */
bool
SLogTupleLookup(Oid relid, ItemPointer tid, SLogTupleEntry *entry_out)
{
	int			partition;
	SLogTupleKey key;
	SLogTupleEntry *entry;
	bool		found;

	memset(&key, 0, sizeof(key));
	key.relid = relid;
	ItemPointerCopy(tid, &key.tid);

	partition = SLogTuplePartition(&key);

	LWLockAcquire(&SLogState->tuple_locks[partition].lock, LW_SHARED);

	entry = (SLogTupleEntry *)
		hash_search(SLogTupleHash, &key, HASH_FIND, &found);

	if (found && entry_out)
		memcpy(entry_out, entry, sizeof(SLogTupleEntry));

	LWLockRelease(&SLogState->tuple_locks[partition].lock);

	return found;
}

/*
 * SLogTupleLookupFiltered
 *		Find sLog entries for a TID, optionally filtered by xid.
 *
 * Single O(1) hash probe in a single partition.  If xid_filter is valid,
 * returns only ops for that xid.  If InvalidTransactionId, returns all
 * active ops for this TID.
 *
 * Returns the number of ops written to ops_out.
 */
int
SLogTupleLookupFiltered(Oid relid, ItemPointer tid,
						TransactionId xid_filter,
						SLogTupleOp *ops_out, int max_ops)
{
	SLogTupleKey key;
	SLogTupleEntry *entry;
	int			partition;
	int			nfound = 0;
	int			i;

	Assert(SLogTupleHash != NULL);

	memset(&key, 0, sizeof(key));
	key.relid = relid;
	ItemPointerCopy(tid, &key.tid);

	partition = SLogTuplePartition(&key);

	LWLockAcquire(&SLogState->tuple_locks[partition].lock, LW_SHARED);

	entry = (SLogTupleEntry *)
		hash_search(SLogTupleHash, &key, HASH_FIND, NULL);

	if (entry != NULL)
	{
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

	LWLockRelease(&SLogState->tuple_locks[partition].lock);

	return nfound;
}

/*
 * SLogTupleRemove
 *		Remove operations for a specific xid from a tuple entry.
 *
 * Uses in_use slot model.  Removes the hash entry if nops reaches 0.
 */
void
SLogTupleRemove(Oid relid, ItemPointer tid, TransactionId xid)
{
	int			partition;
	SLogTupleKey key;
	SLogTupleEntry *entry;
	bool		found;
	int			i;

	memset(&key, 0, sizeof(key));
	key.relid = relid;
	ItemPointerCopy(tid, &key.tid);

	partition = SLogTuplePartition(&key);

	LWLockAcquire(&SLogState->tuple_locks[partition].lock, LW_EXCLUSIVE);

	entry = (SLogTupleEntry *)
		hash_search(SLogTupleHash, &key, HASH_FIND, &found);

	if (!found)
	{
		LWLockRelease(&SLogState->tuple_locks[partition].lock);
		return;
	}

	/* Remove all ops matching xid */
	for (i = 0; i < SLOG_MAX_TUPLE_OPS; i++)
	{
		if (entry->ops[i].in_use &&
			TransactionIdEquals(entry->ops[i].xid, xid))
		{
			entry->ops[i].in_use = false;
			entry->nops--;
		}
	}

	/* Remove entry if empty */
	if (entry->nops == 0)
		hash_search(SLogTupleHash, &key, HASH_REMOVE, NULL);

	LWLockRelease(&SLogState->tuple_locks[partition].lock);
}

/*
 * Sort helper for partition-optimized removal.
 */
typedef struct SLogTupleSortEntry
{
	SLogTupleKey key;
	uint32		partition;
} SLogTupleSortEntry;

static int
slog_tuple_sort_cmp(const void *a, const void *b)
{
	const SLogTupleSortEntry *ea = (const SLogTupleSortEntry *) a;
	const SLogTupleSortEntry *eb = (const SLogTupleSortEntry *) b;

	if (ea->partition < eb->partition)
		return -1;
	if (ea->partition > eb->partition)
		return 1;
	return 0;
}

/*
 * SLogTupleRemoveByXid
 *		Remove all tuple sLog entries for a transaction.
 *
 * Uses the backend-local tracking list.  Sorts entries by partition
 * so each partition lock is acquired at most once.
 */
void
SLogTupleRemoveByXid(TransactionId xid)
{
	SLogTrackedKey *tk;
	int			current_partition = -1;
	int			nentries = 0;
	int			idx;
	SLogTupleSortEntry *sorted;

	if (SLogTupleHash == NULL)
		return;

	/* Count matching entries that have shared hash entries */
	for (tk = slog_tracked_keys; tk != NULL; tk = tk->next)
	{
		if (TransactionIdEquals(tk->xid, xid) && !tk->local_only)
			nentries++;
	}

	if (nentries == 0)
		return;

	/* Collect and sort by partition (skip local_only — no hash entry) */
	sorted = (SLogTupleSortEntry *)
		palloc(nentries * sizeof(SLogTupleSortEntry));
	idx = 0;
	for (tk = slog_tracked_keys; tk != NULL; tk = tk->next)
	{
		if (TransactionIdEquals(tk->xid, xid) && !tk->local_only)
		{
			sorted[idx].key = tk->key;
			sorted[idx].partition = SLogTuplePartition(&tk->key);
			idx++;
		}
	}

	qsort(sorted, nentries, sizeof(SLogTupleSortEntry), slog_tuple_sort_cmp);

	/* Process sorted entries, locking each partition at most once */
	for (idx = 0; idx < nentries; idx++)
	{
		SLogTupleEntry *entry;
		int			part = (int) sorted[idx].partition;
		int			i;

		if (part != current_partition)
		{
			if (current_partition >= 0)
				LWLockRelease(&SLogState->tuple_locks[current_partition].lock);
			LWLockAcquire(&SLogState->tuple_locks[part].lock, LW_EXCLUSIVE);
			current_partition = part;
		}

		entry = (SLogTupleEntry *)
			hash_search(SLogTupleHash, &sorted[idx].key, HASH_FIND, NULL);

		if (entry != NULL)
		{
			for (i = 0; i < SLOG_MAX_TUPLE_OPS; i++)
			{
				if (entry->ops[i].in_use &&
					TransactionIdEquals(entry->ops[i].xid, xid))
				{
					entry->ops[i].in_use = false;
					entry->nops--;
					break;
				}
			}

			if (entry->nops == 0)
				hash_search(SLogTupleHash, &sorted[idx].key,
							HASH_REMOVE, NULL);
		}
	}

	if (current_partition >= 0)
		LWLockRelease(&SLogState->tuple_locks[current_partition].lock);

	pfree(sorted);
}

/*
 * SLogTupleCommitByXid
 *		Handle commit for tuple sLog: retain UPDATE entries with before-images,
 *		remove INSERT/DELETE/LOCK entries.
 *
 * For UPDATE ops with a valid before_image_dp: stamp commit_hlc, leave
 * in_use = true (retained for MVCC reads by other backends).
 * For INSERT/DELETE/LOCK ops: remove as before (in_use = false, free DSA).
 * Delete hash entry only if nops reaches 0.
 *
 * Uses the backend-local tracking list.  Sorts entries by partition
 * for efficient lock batching.
 */
void
SLogTupleCommitByXid(TransactionId xid, uint64 commit_hlc)
{
	SLogTrackedKey *tk;
	int			current_partition = -1;
	int			nentries = 0;
	int			idx;
	SLogTupleSortEntry *sorted;

	if (SLogTupleHash == NULL)
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

	/* Collect and sort by partition */
	sorted = (SLogTupleSortEntry *)
		palloc(nentries * sizeof(SLogTupleSortEntry));
	idx = 0;
	for (tk = slog_tracked_keys; tk != NULL; tk = tk->next)
	{
		if (TransactionIdEquals(tk->xid, xid) && !tk->local_only)
		{
			sorted[idx].key = tk->key;
			sorted[idx].partition = SLogTuplePartition(&tk->key);
			idx++;
		}
	}

	qsort(sorted, nentries, sizeof(SLogTupleSortEntry), slog_tuple_sort_cmp);

	/* Process sorted entries, locking each partition at most once */
	for (idx = 0; idx < nentries; idx++)
	{
		SLogTupleEntry *entry;
		int			part = (int) sorted[idx].partition;
		int			i;

		if (part != current_partition)
		{
			if (current_partition >= 0)
				LWLockRelease(&SLogState->tuple_locks[current_partition].lock);
			LWLockAcquire(&SLogState->tuple_locks[part].lock, LW_EXCLUSIVE);
			current_partition = part;
		}

		entry = (SLogTupleEntry *)
			hash_search(SLogTupleHash, &sorted[idx].key, HASH_FIND, NULL);

		if (entry != NULL)
		{
			for (i = 0; i < SLOG_MAX_TUPLE_OPS; i++)
			{
				if (!entry->ops[i].in_use)
					continue;
				if (!TransactionIdEquals(entry->ops[i].xid, xid))
					continue;

				if (entry->ops[i].op_type == SLOG_OP_UPDATE &&
					DsaPointerIsValid(entry->ops[i].before_image_dp))
				{
					/*
					 * Retain this UPDATE entry — stamp commit_hlc so readers
					 * can determine visibility based on snapshot comparison.
					 */
					entry->ops[i].commit_hlc = commit_hlc;
				}
				else
				{
					/*
					 * INSERT/DELETE/LOCK/UPDATE-without-before-image:
					 * remove as before.
					 */
					if (DsaPointerIsValid(entry->ops[i].before_image_dp))
						SLogDsaFreeBeforeImage(entry->ops[i].before_image_dp);
					entry->ops[i].in_use = false;
					entry->nops--;
				}
			}

			if (entry->nops == 0)
				hash_search(SLogTupleHash, &sorted[idx].key,
							HASH_REMOVE, NULL);
		}
	}

	if (current_partition >= 0)
		LWLockRelease(&SLogState->tuple_locks[current_partition].lock);

	pfree(sorted);
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
	SLogTupleEntry *entry;
	int			partition;
	int			i;

	if (SLogTupleHash == NULL)
		return;

	memset(&key, 0, sizeof(key));
	key.relid = relid;
	ItemPointerCopy(tid, &key.tid);

	partition = SLogTuplePartition(&key);
	LWLockAcquire(&SLogState->tuple_locks[partition].lock, LW_EXCLUSIVE);

	entry = (SLogTupleEntry *)
		hash_search(SLogTupleHash, &key, HASH_FIND, NULL);

	if (entry != NULL)
	{
		for (i = 0; i < SLOG_MAX_TUPLE_OPS; i++)
		{
			if (!entry->ops[i].in_use)
				continue;
			if (!TransactionIdEquals(entry->ops[i].xid, xid))
				continue;

			/* Free any DSA before-image */
			if (DsaPointerIsValid(entry->ops[i].before_image_dp))
				SLogDsaFreeBeforeImage(entry->ops[i].before_image_dp);

			entry->ops[i].in_use = false;
			entry->nops--;
		}

		if (entry->nops == 0)
			hash_search(SLogTupleHash, &key, HASH_REMOVE, NULL);
	}

	LWLockRelease(&SLogState->tuple_locks[partition].lock);
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
	SLogTupleEntry *entry;
	int			partition;
	int			i;

	if (SLogTupleHash == NULL)
		return;

	memset(&key, 0, sizeof(key));
	key.relid = relid;
	ItemPointerCopy(tid, &key.tid);

	partition = SLogTuplePartition(&key);
	LWLockAcquire(&SLogState->tuple_locks[partition].lock, LW_EXCLUSIVE);

	entry = (SLogTupleEntry *)
		hash_search(SLogTupleHash, &key, HASH_FIND, NULL);

	if (entry != NULL)
	{
		for (i = 0; i < SLOG_MAX_TUPLE_OPS; i++)
		{
			if (!entry->ops[i].in_use)
				continue;
			if (!TransactionIdEquals(entry->ops[i].xid, xid))
				continue;

			entry->ops[i].op_type = SLOG_OP_ABORTED;
		}
	}

	LWLockRelease(&SLogState->tuple_locks[partition].lock);
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

	if (SLogTupleHash == NULL)
		return;

	for (tk = slog_tracked_keys; tk != NULL; tk = tk->next)
	{
		SLogTupleEntry *entry;
		bool		found_and_marked = false;
		int			partition;
		int			i;

		if (!TransactionIdEquals(tk->xid, xid))
			continue;
		if (tk->subxid != subxid)
			continue;

		partition = SLogTuplePartition(&tk->key);

		LWLockAcquire(&SLogState->tuple_locks[partition].lock, LW_EXCLUSIVE);

		entry = (SLogTupleEntry *)
			hash_search(SLogTupleHash, &tk->key, HASH_FIND, NULL);

		if (entry != NULL)
		{
			/* Shared entry exists -- mark matching ops ABORTED */
			for (i = 0; i < SLOG_MAX_TUPLE_OPS; i++)
			{
				if (entry->ops[i].in_use &&
					TransactionIdEquals(entry->ops[i].xid, xid) &&
					entry->ops[i].subxid == subxid)
				{
					entry->ops[i].op_type = SLOG_OP_ABORTED;
					found_and_marked = true;
				}
			}
		}

		if (!found_and_marked)
		{
			/*
			 * No shared entry exists.  This happens for regular INSERTs
			 * which use lightweight local-only tracking.  Create a shared
			 * ABORTED entry so visibility code can find it.
			 */
			bool		hash_found;
			bool		slot_found = false;

			entry = (SLogTupleEntry *)
				hash_search(SLogTupleHash, &tk->key, HASH_ENTER_NULL, &hash_found);

			if (entry != NULL)
			{
				if (!hash_found)
				{
					entry->nops = 0;
					memset(entry->ops, 0, sizeof(entry->ops));
				}

				/* Add an ABORTED entry in the first free slot */
				for (i = 0; i < SLOG_MAX_TUPLE_OPS; i++)
				{
					if (!entry->ops[i].in_use)
					{
						entry->ops[i].xid = xid;
						entry->ops[i].subxid = subxid;
						entry->ops[i].op_type = SLOG_OP_ABORTED;
						entry->ops[i].in_use = true;
						entry->ops[i].commit_ts = 0;
						entry->ops[i].spec_token = 0;
						entry->ops[i].cid = InvalidCommandId;
						entry->ops[i].commit_hlc = 0;
						entry->ops[i].before_image_dp = InvalidDsaPointer;
						entry->nops++;
						slot_found = true;
						break;
					}
				}

				/* Try stale-slot reclamation if no free slot found */
				if (!slot_found)
				{
					for (i = 0; i < SLOG_MAX_TUPLE_OPS; i++)
					{
						if (!entry->ops[i].in_use)
							continue;
						if (!TransactionIdIsInProgress(entry->ops[i].xid) &&
							TransactionIdDidCommit(entry->ops[i].xid))
						{
							/* Reclaim this committed slot */
							entry->ops[i].xid = xid;
							entry->ops[i].subxid = subxid;
							entry->ops[i].op_type = SLOG_OP_ABORTED;
							entry->ops[i].commit_ts = 0;
							entry->ops[i].spec_token = 0;
							entry->ops[i].cid = InvalidCommandId;
							slot_found = true;
							break;
						}
					}
				}

				if (!slot_found)
				{
					elog(WARNING, "sLog: all %d ops slots full on TID (%u,%u) "
						 "rel %u at subxact abort for xid %u subxid %u; "
						 "relying on UNDO worker for cleanup",
						 SLOG_MAX_TUPLE_OPS,
						 ItemPointerGetBlockNumber(&tk->key.tid),
						 ItemPointerGetOffsetNumber(&tk->key.tid),
						 tk->key.relid, xid, subxid);
				}
			}
			else
			{
				elog(WARNING, "sLog: hash full at subxact abort, cannot create "
					 "ABORTED entry for xid %u subxid %u tid (%u,%u) rel %u; "
					 "relying on UNDO worker for cleanup",
					 xid, subxid,
					 ItemPointerGetBlockNumber(&tk->key.tid),
					 ItemPointerGetOffsetNumber(&tk->key.tid),
					 tk->key.relid);
			}
		}

		LWLockRelease(&SLogState->tuple_locks[partition].lock);
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

	if (SLogTupleHash == NULL)
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
			SLogTupleEntry *entry;
			int			partition;
			int			i;

			partition = SLogTuplePartition(&tk->key);

			LWLockAcquire(&SLogState->tuple_locks[partition].lock, LW_EXCLUSIVE);

			entry = (SLogTupleEntry *)
				hash_search(SLogTupleHash, &tk->key, HASH_FIND, NULL);

			if (entry != NULL)
			{
				for (i = 0; i < SLOG_MAX_TUPLE_OPS; i++)
				{
					if (entry->ops[i].in_use &&
						TransactionIdEquals(entry->ops[i].xid, xid) &&
						entry->ops[i].subxid == old_subxid)
					{
						entry->ops[i].subxid = new_subxid;
					}
				}
			}

			LWLockRelease(&SLogState->tuple_locks[partition].lock);
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

	if (SLogTupleHash == NULL)
		return;

	for (tk = slog_tracked_keys; tk != NULL; tk = tk->next)
	{
		SLogTupleEntry *entry;
		int			partition;
		int			i;

		if (!TransactionIdEquals(tk->xid, xid))
			continue;

		partition = SLogTuplePartition(&tk->key);

		LWLockAcquire(&SLogState->tuple_locks[partition].lock, LW_EXCLUSIVE);

		if (tk->local_only)
		{
			/*
			 * INSERT elision: no shared entry exists yet.  Create one with
			 * SLOG_OP_ABORTED so visibility code correctly hides this tuple
			 * until UNDO replay removes it from the page.
			 */
			bool		hash_found;
			bool		slot_found = false;

			entry = (SLogTupleEntry *)
				hash_search(SLogTupleHash, &tk->key, HASH_ENTER_NULL, &hash_found);

			if (entry != NULL)
			{
				if (!hash_found)
				{
					entry->nops = 0;
					memset(entry->ops, 0, sizeof(entry->ops));
				}

				/* Find a free slot for the ABORTED entry */
				for (i = 0; i < SLOG_MAX_TUPLE_OPS; i++)
				{
					if (!entry->ops[i].in_use)
					{
						entry->ops[i].xid = xid;
						entry->ops[i].subxid = tk->subxid;
						entry->ops[i].op_type = SLOG_OP_ABORTED;
						entry->ops[i].in_use = true;
						entry->ops[i].commit_ts = 0;
						entry->ops[i].spec_token = 0;
						entry->ops[i].cid = InvalidCommandId;
						entry->ops[i].commit_hlc = 0;
						entry->ops[i].before_image_dp = InvalidDsaPointer;
						entry->nops++;
						slot_found = true;
						break;
					}
				}

				/* Try stale-slot reclamation if no free slot found */
				if (!slot_found)
				{
					for (i = 0; i < SLOG_MAX_TUPLE_OPS; i++)
					{
						if (!entry->ops[i].in_use)
							continue;
						if (!TransactionIdIsInProgress(entry->ops[i].xid) &&
							TransactionIdDidCommit(entry->ops[i].xid))
						{
							/* Reclaim this committed slot */
							entry->ops[i].xid = xid;
							entry->ops[i].subxid = tk->subxid;
							entry->ops[i].op_type = SLOG_OP_ABORTED;
							entry->ops[i].commit_ts = 0;
							entry->ops[i].spec_token = 0;
							entry->ops[i].cid = InvalidCommandId;
							slot_found = true;
							break;
						}
					}
				}

				if (!slot_found)
				{
					elog(WARNING, "sLog: all %d ops slots full on TID (%u,%u) "
						 "rel %u at abort for xid %u; "
						 "relying on UNDO worker for cleanup",
						 SLOG_MAX_TUPLE_OPS,
						 ItemPointerGetBlockNumber(&tk->key.tid),
						 ItemPointerGetOffsetNumber(&tk->key.tid),
						 tk->key.relid, xid);
				}
			}
			else
			{
				/*
				 * Hash is full and we can't create the ABORTED entry.
				 * This is not fatal: the UNDO worker will physically
				 * remove the tuple from the page.  In the interim,
				 * the tuple may briefly appear visible (bounded anomaly
				 * until UNDO apply completes).
				 */
				elog(WARNING, "sLog: hash full at abort, cannot create "
					 "ABORTED entry for xid %u tid (%u,%u) rel %u; "
					 "relying on UNDO worker for cleanup",
					 xid,
					 ItemPointerGetBlockNumber(&tk->key.tid),
					 ItemPointerGetOffsetNumber(&tk->key.tid),
					 tk->key.relid);
			}
		}
		else
		{
			/* Shared entry exists -- mark matching ops ABORTED */
			entry = (SLogTupleEntry *)
				hash_search(SLogTupleHash, &tk->key, HASH_FIND, NULL);

			if (entry != NULL)
			{
				for (i = 0; i < SLOG_MAX_TUPLE_OPS; i++)
				{
					if (entry->ops[i].in_use &&
						TransactionIdEquals(entry->ops[i].xid, xid))
					{
						entry->ops[i].op_type = SLOG_OP_ABORTED;
						/* Clear DSA pointer (already freed by caller) */
						entry->ops[i].before_image_dp = InvalidDsaPointer;
						entry->ops[i].commit_hlc = 0;
						break;
					}
				}
			}
		}

		LWLockRelease(&SLogState->tuple_locks[partition].lock);
	}
}

/*
 * SLogTupleRemoveByXidGlobal
 *		Remove ALL ops for a transaction by scanning the shared hash table.
 *
 * Unlike SLogTupleRemoveByXid, this does not use the backend-local tracking
 * list (which doesn't exist in the UNDO worker process).  Used by the UNDO
 * worker to clean up ABORTED entries after UNDO has been applied.
 */
void
SLogTupleRemoveByXidGlobal(TransactionId xid)
{
	HASH_SEQ_STATUS status;
	SLogTupleEntry *hentry;
	int			part;
	SLogTupleKey *collected_keys;
	int			nkeys = 0;
	int			max_keys;

	if (SLogTupleHash == NULL)
		return;

	/*
	 * Phase 1: Scan under SHARED locks.  Collect keys containing target xid.
	 */
	for (part = 0; part < NUM_SLOG_TUPLE_PARTITIONS; part++)
		LWLockAcquire(&SLogState->tuple_locks[part].lock, LW_SHARED);

	max_keys = SLogTupleNumEntries();
	collected_keys = (SLogTupleKey *)
		palloc(sizeof(SLogTupleKey) * max_keys);

	hash_seq_init(&status, SLogTupleHash);
	while ((hentry = (SLogTupleEntry *) hash_seq_search(&status)) != NULL)
	{
		int			i;

		for (i = 0; i < SLOG_MAX_TUPLE_OPS; i++)
		{
			if (hentry->ops[i].in_use &&
				TransactionIdEquals(hentry->ops[i].xid, xid))
			{
				if (nkeys < max_keys)
					collected_keys[nkeys++] = hentry->key;
				break;
			}
		}
	}

	/* Release all shared locks */
	for (part = NUM_SLOG_TUPLE_PARTITIONS - 1; part >= 0; part--)
		LWLockRelease(&SLogState->tuple_locks[part].lock);

	/*
	 * Phase 2: Remove under per-partition EXCLUSIVE locks.
	 */
	for (part = 0; part < nkeys; part++)
	{
		SLogTupleEntry *entry;
		bool		modified = false;
		int			partition;
		int			i;

		partition = SLogTuplePartition(&collected_keys[part]);

		LWLockAcquire(&SLogState->tuple_locks[partition].lock, LW_EXCLUSIVE);

		entry = (SLogTupleEntry *)
			hash_search(SLogTupleHash, &collected_keys[part], HASH_FIND, NULL);

		if (entry != NULL)
		{
			for (i = 0; i < SLOG_MAX_TUPLE_OPS; i++)
			{
				if (entry->ops[i].in_use &&
					TransactionIdEquals(entry->ops[i].xid, xid))
				{
					/* Free DSA before-image if present */
					if (DsaPointerIsValid(entry->ops[i].before_image_dp))
					{
						SLogEnsureDsaAttached();
						dsa_free(slog_dsa_handle,
								 entry->ops[i].before_image_dp);
						entry->ops[i].before_image_dp = InvalidDsaPointer;
					}
					entry->ops[i].in_use = false;
					entry->nops--;
					modified = true;
				}
			}

			if (modified && entry->nops == 0)
				hash_search(SLogTupleHash, &collected_keys[part],
							HASH_REMOVE, NULL);
		}

		LWLockRelease(&SLogState->tuple_locks[partition].lock);
	}

	pfree(collected_keys);
}

/*
 * SLogTupleIterateByTid
 *		Call a callback for each active operation on a tuple.
 */
void
SLogTupleIterateByTid(Oid relid, ItemPointer tid,
					  SLogTupleIterCallback callback, void *arg)
{
	int			partition;
	SLogTupleKey key;
	SLogTupleEntry *entry;
	bool		found;
	int			i;

	memset(&key, 0, sizeof(key));
	key.relid = relid;
	ItemPointerCopy(tid, &key.tid);

	partition = SLogTuplePartition(&key);

	LWLockAcquire(&SLogState->tuple_locks[partition].lock, LW_SHARED);

	entry = (SLogTupleEntry *)
		hash_search(SLogTupleHash, &key, HASH_FIND, &found);

	if (found)
	{
		for (i = 0; i < SLOG_MAX_TUPLE_OPS; i++)
		{
			if (entry->ops[i].in_use)
			{
				if (!callback(&entry->ops[i], arg))
					break;
			}
		}
	}

	LWLockRelease(&SLogState->tuple_locks[partition].lock);
}

/* ----------------------------------------------------------------
 * Convenience wrappers
 * ----------------------------------------------------------------
 */

/*
 * SLogTupleHasEntry -- quick probe: does ANY active entry exist for this TID?
 */
bool
SLogTupleHasEntry(Oid relid, ItemPointer tid)
{
	SLogTupleKey key;
	SLogTupleEntry *entry;
	int			partition;
	bool		has_entry = false;

	Assert(SLogTupleHash != NULL);

	memset(&key, 0, sizeof(key));
	key.relid = relid;
	ItemPointerCopy(tid, &key.tid);

	partition = SLogTuplePartition(&key);

	LWLockAcquire(&SLogState->tuple_locks[partition].lock, LW_SHARED);

	entry = (SLogTupleEntry *)
		hash_search(SLogTupleHash, &key, HASH_FIND, NULL);

	if (entry != NULL && entry->nops > 0)
		has_entry = true;

	LWLockRelease(&SLogState->tuple_locks[partition].lock);

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
	 * Fall back to backend-local tracking list.  This handles the overflow
	 * case where SLogTupleInsert returned false (hash full) but the INSERT
	 * was tracked locally via SLogTupleTrackLocalOnly or SLogTupleTrackKey.
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
 * LOCK-FREE: This function does NOT acquire the partition LWLock.  It reads
 * the hash entry speculatively.  On x86-64, aligned 4-byte and 8-byte reads
 * are atomic, so we see consistent individual field values.  The worst case
 * is a momentarily stale view (missing a just-inserted entry or seeing a
 * just-removed entry), which is acceptable: the caller will detect the
 * conflict via row-level locking on the next attempt.
 *
 * This lock-free design eliminates the buffer-lock / sLog-partition-lock
 * deadlock that previously required releasing the buffer lock before calling
 * this function.
 */
TransactionId
SLogTupleGetDirtyXid(Oid relid, ItemPointer tid, bool *is_insert)
{
	SLogTupleKey key;
	SLogTupleEntry *entry;
	int			i;

	Assert(SLogTupleHash != NULL);

	memset(&key, 0, sizeof(key));
	key.relid = relid;
	ItemPointerCopy(tid, &key.tid);

	/*
	 * Speculative hash_search without lock.  HASH_FIND only traverses
	 * bucket chains via stable shared-memory pointers; it never modifies
	 * the hash structure.  Concurrent HASH_ENTER/HASH_REMOVE by writers
	 * (who hold the partition lock exclusive) may cause us to miss a
	 * just-inserted entry or see a just-removed entry — both are safe
	 * false-negatives for this best-effort dirty-check.
	 */
	entry = (SLogTupleEntry *)
		hash_search(SLogTupleHash, &key, HASH_FIND, NULL);

	if (entry == NULL)
		return InvalidTransactionId;

	for (i = 0; i < SLOG_MAX_TUPLE_OPS; i++)
	{
		TransactionId xid;
		SLogOpType	op;

		if (!entry->ops[i].in_use)
			continue;

		xid = entry->ops[i].xid;
		op = entry->ops[i].op_type;

		/* Memory barrier to ensure we read consistent xid + op_type */
		pg_read_barrier();

		if (TransactionIdIsCurrentTransactionId(xid))
			continue;
		if (!TransactionIdIsInProgress(xid))
			continue;

		if (is_insert)
			*is_insert = (op == SLOG_OP_INSERT);
		return xid;
	}

	return InvalidTransactionId;
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
SLogTupleTrackKey(SLogTupleKey key, TransactionId xid, TransactionId subxid)
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
	tk->op_type = SLOG_OP_INSERT;	/* default; caller may update */
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
 */
void
SLogTupleTrackLocalOnly(Oid relid, ItemPointer tid,
						TransactionId xid, TransactionId subxid)
{
	MemoryContext oldcxt;
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

/*
 * SLogTupleHasSharedBeforeImage
 *		Check whether the shared sLog already has a before-image DSA pointer
 *		for this (relid, tid, xid).
 *
 * Used to prevent overwriting the original pre-transaction before-image
 * when the same row is updated multiple times within a single transaction.
 */
static bool
SLogTupleHasSharedBeforeImage(Oid relid, ItemPointer tid, TransactionId xid)
{
	SLogTupleKey key;
	int			partition;
	SLogTupleEntry *entry;
	bool		has_bi = false;

	if (SLogTupleHash == NULL)
		return false;

	memset(&key, 0, sizeof(key));
	key.relid = relid;
	ItemPointerCopy(tid, &key.tid);
	partition = SLogTuplePartition(&key);

	LWLockAcquire(&SLogState->tuple_locks[partition].lock, LW_SHARED);

	entry = (SLogTupleEntry *)
		hash_search(SLogTupleHash, &key, HASH_FIND, NULL);

	if (entry != NULL)
	{
		int		i;

		for (i = 0; i < SLOG_MAX_TUPLE_OPS; i++)
		{
			if (entry->ops[i].in_use &&
				TransactionIdEquals(entry->ops[i].xid, xid) &&
				entry->ops[i].op_type == SLOG_OP_UPDATE &&
				DsaPointerIsValid(entry->ops[i].before_image_dp))
			{
				has_bi = true;
				break;
			}
		}
	}

	LWLockRelease(&SLogState->tuple_locks[partition].lock);

	return has_bi;
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
						  uint16 flags, uint64 commit_ts)
{
	SLogTrackedKey *tk;
	MemoryContext oldcxt;
	dsa_pointer dp;

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

		MemoryContextSwitchTo(oldcxt);

		/*
		 * Allocate shared DSA copy for cross-backend MVCC reads — but
		 * only if no prior update within this transaction already stored
		 * one.  The FIRST before-image is the true pre-transaction state
		 * that MVCC readers should see; subsequent in-place updates within
		 * the same transaction must not overwrite it.
		 */
		dp = InvalidDsaPointer;
		if (!SLogTupleHasSharedBeforeImage(relid, tid, xid))
		{
			dp = SLogDsaAllocateBeforeImage(data, len, flags, commit_ts);

			if (!DsaPointerIsValid(dp))
			{
				/*
				 * Rate-limit this WARNING: emit at most once per 10 seconds
				 * per backend to avoid log flooding under sustained pressure.
				 */
				TimestampTz now = GetCurrentTimestamp();

				if (slog_overflow_last_warning == 0 ||
					TimestampDifferenceExceeds(slog_overflow_last_warning, now, 10000))
				{
					slog_overflow_last_warning = now;
					elog(WARNING, "sLog: DSA before-image allocation failed "
						 "(limit %d MB); MVCC before-image serving degraded "
						 "for rel %u tid (%u,%u)",
						 slog_dsa_max_size_mb,
						 relid, ItemPointerGetBlockNumber(tid),
						 ItemPointerGetOffsetNumber(tid));
				}
			}
		}
		tk->before_image_dp = dp;

		if (DsaPointerIsValid(dp) && !tk->local_only)
		{
			/* Store dp in the shared sLog op for readers */
			SLogTupleKey key;
			int			partition;
			SLogTupleEntry *entry;

			memset(&key, 0, sizeof(key));
			key.relid = relid;
			ItemPointerCopy(tid, &key.tid);
			partition = SLogTuplePartition(&key);

			LWLockAcquire(&SLogState->tuple_locks[partition].lock,
						  LW_EXCLUSIVE);

			entry = (SLogTupleEntry *)
				hash_search(SLogTupleHash, &key, HASH_FIND, NULL);

			if (entry != NULL)
			{
				int		i;

				for (i = 0; i < SLOG_MAX_TUPLE_OPS; i++)
				{
					if (entry->ops[i].in_use &&
						TransactionIdEquals(entry->ops[i].xid, xid) &&
						entry->ops[i].op_type == SLOG_OP_UPDATE)
					{
						entry->ops[i].before_image_dp = dp;
						break;
					}
				}
			}

			LWLockRelease(&SLogState->tuple_locks[partition].lock);
		}

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
						uint16 *flags_out, uint64 *commit_ts_out)
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
		return true;
	}

	return false;
}

/*
 * SLogTupleGetSharedBeforeImage
 *		Retrieve a committed before-image from shared DSA for MVCC reads.
 *
 * Looks for a committed UPDATE entry on (relid, tid) whose commit_hlc is
 * AFTER the reader's snapshot.  If found, copies the DSA-resident
 * before-image into a palloc'd buffer and returns true.
 *
 * The caller should serve this data instead of the on-page (post-update)
 * data when the reader's snapshot pre-dates the update commit.
 *
 * Safety: copies data while holding the partition LWLock(SHARED).  The
 * cleanup path acquires EXCLUSIVE before freeing DSA, preventing
 * use-after-free.
 */
bool
SLogTupleGetSharedBeforeImage(Oid relid, ItemPointer tid,
							  uint64 reader_snapshot_hlc,
							  char **data_out, int *len_out,
							  uint16 *flags_out, uint64 *orig_commit_ts_out)
{
	SLogTupleKey key;
	int			partition;
	SLogTupleEntry *entry;
	bool		found = false;

	if (SLogTupleHash == NULL || reader_snapshot_hlc == 0)
		return false;

	memset(&key, 0, sizeof(key));
	key.relid = relid;
	ItemPointerCopy(tid, &key.tid);

	partition = SLogTuplePartition(&key);

	LWLockAcquire(&SLogState->tuple_locks[partition].lock, LW_SHARED);

	entry = (SLogTupleEntry *)
		hash_search(SLogTupleHash, &key, HASH_FIND, NULL);

	if (entry != NULL)
	{
		int		i;

		for (i = 0; i < SLOG_MAX_TUPLE_OPS; i++)
		{
			if (!entry->ops[i].in_use)
				continue;
			if (entry->ops[i].op_type != SLOG_OP_UPDATE)
				continue;
			if (entry->ops[i].commit_hlc == 0)
				continue;	/* uncommitted */
			if (entry->ops[i].commit_hlc <= reader_snapshot_hlc)
				continue;	/* committed before reader's snapshot */
			if (!DsaPointerIsValid(entry->ops[i].before_image_dp))
				continue;

			/*
			 * Found a committed UPDATE whose commit_hlc > reader snapshot.
			 * Copy the before-image from DSA while holding the lock.
			 */
			{
				SLogBeforeImage *bi;

				SLogEnsureDsaAttached();
				bi = (SLogBeforeImage *)
					dsa_get_address(slog_dsa_handle,
									entry->ops[i].before_image_dp);

				*data_out = (char *) palloc(bi->len);
				memcpy(*data_out, bi->data, bi->len);
				*len_out = (int) bi->len;
				*flags_out = bi->flags;
				*orig_commit_ts_out = bi->commit_ts;
				found = true;
			}
			break;
		}
	}

	LWLockRelease(&SLogState->tuple_locks[partition].lock);

	return found;
}

/*
 * SLogTupleCleanupRetained
 *		Free retained committed UPDATE entries that are no longer visible
 *		to any active snapshot.
 *
 * An entry can be freed when its commit_hlc < oldest_snapshot_hlc, meaning
 * all active transactions started after the update committed and will see
 * the on-page (new) data directly.
 *
 * Scans all partitions, acquiring EXCLUSIVE lock per partition.
 * Called periodically by the UNDO background worker.
 */
void
SLogTupleCleanupRetained(uint64 oldest_snapshot_hlc)
{
	HASH_SEQ_STATUS status;
	SLogTupleEntry *hentry;
	int			part;
	SLogTupleKey *to_remove;
	int			nremove = 0;
	int			max_remove = 256;

	if (SLogTupleHash == NULL || oldest_snapshot_hlc == 0)
		return;

	SLogEnsureDsaAttached();

	to_remove = (SLogTupleKey *)
		palloc(max_remove * sizeof(SLogTupleKey));

	/*
	 * Scan all partitions.  We acquire EXCLUSIVE so we can modify ops
	 * and free DSA safely.
	 */
	for (part = 0; part < NUM_SLOG_TUPLE_PARTITIONS; part++)
	{
		LWLockAcquire(&SLogState->tuple_locks[part].lock, LW_EXCLUSIVE);

		hash_seq_init(&status, SLogTupleHash);
		while ((hentry = (SLogTupleEntry *) hash_seq_search(&status)) != NULL)
		{
			int		i;
			int		entry_part = SLogTuplePartition(&hentry->key);

			/* Only process entries belonging to this partition */
			if (entry_part != part)
				continue;

			for (i = 0; i < SLOG_MAX_TUPLE_OPS; i++)
			{
				if (!hentry->ops[i].in_use)
					continue;
				if (hentry->ops[i].op_type != SLOG_OP_UPDATE)
					continue;
				if (hentry->ops[i].commit_hlc == 0)
					continue;
				if (hentry->ops[i].commit_hlc >= oldest_snapshot_hlc)
					continue;

				/* This retained entry is no longer needed */
				if (DsaPointerIsValid(hentry->ops[i].before_image_dp))
					dsa_free(slog_dsa_handle,
							 hentry->ops[i].before_image_dp);

				hentry->ops[i].in_use = false;
				hentry->ops[i].before_image_dp = InvalidDsaPointer;
				hentry->ops[i].commit_hlc = 0;
				hentry->nops--;
			}

			/* Collect empty entries for removal */
			if (hentry->nops == 0)
			{
				if (nremove >= max_remove)
				{
					max_remove *= 2;
					to_remove = (SLogTupleKey *)
						repalloc(to_remove,
								 max_remove * sizeof(SLogTupleKey));
				}
				to_remove[nremove++] = hentry->key;
			}
		}

		/* Remove collected empty entries */
		for (int r = 0; r < nremove; r++)
		{
			int		rpart = SLogTuplePartition(&to_remove[r]);

			if (rpart == part)
				hash_search(SLogTupleHash, &to_remove[r],
							HASH_REMOVE, NULL);
		}

		LWLockRelease(&SLogState->tuple_locks[part].lock);
	}

	pfree(to_remove);
}

/*
 * SLogTupleResetTracking
 *		Clear the backend-private tracking list and reset overflow state.
 */
void
SLogTupleResetTracking(void)
{
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
 */
void
SLogTupleIterateTrackedKeys(TransactionId xid,
							SLogTrackedKeyCallback callback,
							void *arg)
{
	SLogTrackedKey *tk;

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
 */
void
SLogTupleIterateTrackedKeysExt(TransactionId xid,
							   SLogTrackedKeyExtCallback callback,
							   void *arg)
{
	SLogTrackedKey *tk;

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

	arr = (SLogTrackedKeyInfo *) palloc(sizeof(SLogTrackedKeyInfo) * capacity);

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
