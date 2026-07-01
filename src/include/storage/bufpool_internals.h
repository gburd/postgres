/*-------------------------------------------------------------------------
 *
 * bufpool_internals.h
 *	  Internal definitions for the multi-pool buffer management system.
 *
 * This header defines BufferPoolDesc, the shared-memory descriptor for
 * a single buffer pool instance, and related infrastructure for managing
 * multiple buffer pools.
 *
 * Dynamic pools store their data in DSM segments.  Since DSM segments are
 * mapped at potentially different virtual addresses in each backend, the
 * shared-memory BufferPoolDesc stores OFFSETS within the DSM rather than
 * absolute pointers.  Each backend lazily attaches to the DSM and resolves
 * these offsets into virtual addresses via EnsurePoolAttached().
 *
 * The default pool (slot 0) uses the global BufferDescriptors/BufferBlocks
 * arrays and SharedBufHash, allocated during normal shared memory init.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/bufpool_internals.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef BUFPOOL_INTERNALS_H
#define BUFPOOL_INTERNALS_H

#include "pg_config_manual.h"
#include "port/atomics.h"
#include "storage/buf_internals.h"
#include "storage/condition_variable.h"
#include "storage/dsm.h"
#include "storage/dsm_impl.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"

/* Forward declarations */
struct BufferPoolRoutine;

/*
 * Maximum number of buffer pools that can exist concurrently.
 * The default pool always occupies slot 0; the RECYCLE pool slot 1.
 */
#define MAX_BUFFER_POOLS 64

/*
 * BufferPoolKind -- classification of buffer pool instances.
 *
 * BUFPOOL_DEFAULT: the main shared_buffers pool (slot 0).  Its replacement
 *   algorithm is configurable via the buffer_pool_algorithm GUC.
 *
 * BUFPOOL_RECYCLE: a shared pool for scan/VACUUM/bulk-write recycling that
 *   replaces the legacy per-backend ring buffers (slot 1).
 *
 * BUFPOOL_USER: user-created pools via CREATE BUFFER POOL DDL.
 *
 * The REMAINDER pool is process-local only and does not need a kind here.
 */
typedef enum BufferPoolKind
{
	BUFPOOL_DEFAULT,			/* main shared_buffers pool */
	BUFPOOL_RECYCLE,			/* scan/VACUUM recycling pool */
	BUFPOOL_USER,				/* user-created dynamic pools */
} BufferPoolKind;

/*
 * PoolBufHashEntry -- open-addressed hash table entry for dynamic pools.
 *
 * Dynamic pool hash tables live entirely in DSM memory as a flat array
 * of these entries.  Because entries contain no internal pointers, the
 * table works correctly when the DSM segment is mapped at different
 * virtual addresses in different backends.
 *
 * Probe strategy: "funnel" open addressing (Farach-Colton, Krapivin &
 * Kuszmaul, "Optimal Bounds for Open Addressing Without Reordering",
 * arXiv:2501.02305, 2025).  The array is split into a geometrically
 * decreasing sequence of levels A_1, A_2, ...; a key probes a bounded
 * number of slots within its level before funneling down to the next,
 * smaller level.  Compared to plain linear/uniform probing this bounds
 * worst-case probe complexity at O(log^2 1/delta) instead of O(1/delta),
 * where delta is the fraction of empty slots.
 *
 * The table is sized once at pool creation and never resized: a cache
 * pool holds at most nbuffers live keys, so the maximum load factor is
 * fixed (POOL_HASH_LOAD_NUM/POOL_HASH_LOAD_DEN) and delta is bounded for
 * the life of the pool.  This is the regime the construction targets --
 * a fixed table whose delta you control from the start, with no
 * reordering of already-placed entries.
 *
 * Concurrency: all access is protected by the pool's single mapping
 * LWLock (bp_num_partitions = 1 for dynamic pools).
 */
typedef struct PoolBufHashEntry
{
	BufferTag	key;
	int32		id;				/* buffer ID, or POOL_HASH_UNUSED /
								 * POOL_HASH_DELETED */
} PoolBufHashEntry;

#define POOL_HASH_UNUSED	(-1)
#define POOL_HASH_DELETED	(-2)

/*
 * Target maximum load factor for the funnel hash table, as a fraction.
 * With nbuffers live keys at most, the table capacity is sized so that
 * the live keys occupy at most POOL_HASH_LOAD_NUM/POOL_HASH_LOAD_DEN of
 * the slots (here 3/4), leaving delta >= 1/4 empty permanently.
 */
#define POOL_HASH_LOAD_NUM	3
#define POOL_HASH_LOAD_DEN	4

/*
 * Maximum probes attempted within a single funnel level before funneling
 * down to the next, smaller level.  Bounded by the construction.
 */
#define POOL_HASH_LEVEL_PROBES	4

/*
 * BufferPoolDesc -- shared-memory descriptor for a buffer pool instance.
 *
 * For the default pool (slot 0):
 *   - bp_dsm_handle = InvalidDsmHandle
 *   - bp_num_partitions = NUM_BUFFER_PARTITIONS
 *   - All offset fields are unused (zero)
 *   - bp_routine points to the active algorithm (clock-sweep/ARC)
 *   - Data is accessed through global arrays (BufferDescriptors, etc.)
 *
 * For dynamic pools (slots 1..N):
 *   - bp_dsm_handle is a valid DSM handle
 *   - bp_num_partitions = 1 (single mapping lock for the whole pool)
 *   - Offset fields describe the layout within the DSM segment
 *   - bp_routine points to the pool's algorithm vtable
 *   - Data is accessed through per-backend PoolLocalState (see below)
 */
typedef struct BufferPoolDesc
{
	Oid			bp_oid;			/* OID from pg_bufferpool (0 for default) */
	NameData	bp_name;		/* pool name */
	BufferPoolKind bp_kind;		/* DEFAULT, RECYCLE, or USER */
	int			bp_nbuffers;	/* number of buffers in this pool */
	int			bp_first_buf;	/* starting buffer ID (0 for default pool) */

	/*
	 * Eviction algorithm vtable.  bp_routine is a process-local pointer that
	 * must be resolved by each backend separately.  For built-in algorithms
	 * the pointer is into the postgres text segment and is valid in all
	 * backends.  For extension-provided algorithms (contrib), each backend
	 * must load the extension library and call the handler function to obtain
	 * its own valid pointer.
	 *
	 * bp_handler_oid stores the handler function OID (InvalidOid for
	 * builtins). bp_handler_library and bp_handler_function store the
	 * resolved library and function names so that processes without catalog
	 * access (e.g. the trickle writer) can load the extension and resolve
	 * bp_routine.
	 */
	Oid			bp_handler_oid; /* handler function OID (InvalidOid for
								 * builtins) */
	char		bp_handler_library[MAXPGPATH];	/* extension .so path */
	char		bp_handler_function[NAMEDATALEN];	/* handler function name */
	const struct BufferPoolRoutine *bp_routine;

	/*
	 * Mapping lock partition count.  NUM_BUFFER_PARTITIONS for the default
	 * pool; 1 for dynamic pools (single lock covers the whole pool hash).
	 */
	int			bp_num_partitions;

	/*
	 * Offsets within the DSM segment for per-pool data arrays.  Only
	 * meaningful when bp_dsm_handle != InvalidDsmHandle (dynamic pools).
	 */
	Size		bp_desc_offset; /* BufferDescPadded array */
	Size		bp_blocks_offset;	/* buffer data blocks */
	Size		bp_io_cvs_offset;	/* I/O condition variables */
	Size		bp_strategy_offset; /* algorithm-private state */
	Size		bp_hash_offset; /* PoolBufHashEntry array */
	Size		bp_locks_offset;	/* LWLockPadded array */
	Size		bp_ckpt_offset; /* CkptSortItem array */

	/* Open-addressed hash table sizing (number of entries, ~2x nbuffers) */
	int			bp_hash_nentries;

	/* DSM segment handle for cross-backend attachment */
	dsm_handle	bp_dsm_handle;	/* InvalidDsmHandle for default pool, and for
								 * reservation-backed pools */

	/*
	 * Reservation backing (same-address pools).  When bp_resv_backed is true,
	 * the pool's memory is a committed sub-range of the address-space
	 * reservation at offset bp_resv_offset (size bp_resv_size), mapped at the
	 * same virtual address in every backend.  The bp_*_offset fields below are
	 * then offsets within that sub-range and resolve to absolute addresses via
	 * BufPoolAddrAt(bp_resv_offset) + bp_*_offset -- identical in all backends,
	 * so no per-backend DSM attach is needed.  Mutually exclusive with a valid
	 * bp_dsm_handle.
	 */
	bool		bp_resv_backed;
	Size		bp_resv_offset;
	Size		bp_resv_size;
	bool		bp_resv_huge;	/* committed with huge pages */

	/*
	 * bp_resv_offset/bp_resv_size describe the pool's contiguous ADDRESS
	 * window in the reservation; bp_resv_size is the requested size (the
	 * window is rounded up to a whole number of chunks).  The window is backed
	 * physically by N fixed-size chunks that may be DISJOINT in the backing
	 * memfd -- the reservation allocator MAP_FIXEDs each chunk into the window
	 * so the window is contiguous in address space even when the physical
	 * chunks are scattered.  This makes pool creation immune to external
	 * fragmentation (any N free chunks satisfy an N-chunk pool) while keeping
	 * every pool pointer at base+offset -- the disjoint backing is invisible
	 * on the hot path (measured: no TPS difference vs a contiguous pool).  The
	 * chunk list lives in the allocator's window record, not here.
	 */

	/* Trickle writer background worker (stored inline for cross-backend use) */
	int			bp_trickle_slot;	/* BGW slot (-1 = none) */
	uint64		bp_trickle_generation;	/* BGW generation */

	/* Pool state */
	bool		bp_active;		/* true if pool is usable */

	/*
	 * Per-pool Direct I/O preference.  When true, the pool's trickle writer
	 * and I/O paths should bypass the OS page cache where possible (via
	 * PG_O_DIRECT / io_direct_flags).  Currently advisory -- actual Direct
	 * I/O depends on global io_direct_flags being enabled.  Reserved for
	 * future per-pool control.
	 */
	bool		bp_use_direct_io;

	/*
	 * Oversubscription support.  bp_target_buffers is the configured target
	 * size; bp_current_buffers is the actual count (may exceed target when
	 * unclaimed buffers were available).  The trickle writer nudges an
	 * oversubscribed pool back to its target over time by evicting excess
	 * buffers and returning them to the unclaimed pool.
	 */
	int			bp_target_buffers;	/* configured target buffer count */
	int			bp_current_buffers; /* actual allocated buffers */
	bool		bp_oversubscribed;	/* current > target */

	/* Statistics (atomics for concurrent reads without lock) */
	pg_atomic_uint64 bp_reads;
	pg_atomic_uint64 bp_hits;
	pg_atomic_uint64 bp_evictions;
} BufferPoolDesc;

/*
 * PoolLocalState -- per-backend resolved pointers for a buffer pool.
 *
 * Each backend lazily attaches to a dynamic pool's DSM segment on first
 * access and caches the resolved virtual addresses here.  This avoids
 * storing absolute pointers in the shared-memory BufferPoolDesc, which
 * would be invalid in other backends' address spaces.
 */
/* Maximum number of stat counters any algorithm needs */
#define POOL_MAX_STAT_COUNTERS	16

/* Flush local stat accumulator to shared counter after this many increments */
#define POOL_STAT_FLUSH_THRESHOLD	256

typedef struct PoolLocalState
{
	dsm_segment *seg;			/* DSM segment (NULL until attached) */
	BufferDescPadded *descriptors;
	char	   *blocks;
	ConditionVariableMinimallyPadded *io_cvs;
	PoolBufHashEntry *hash_entries;
	LWLockPadded *mapping_locks;
	CkptSortItem *ckpt_ids;
	void	   *strategy_data;
	bool		attached;

	/* Per-backend stat counter buffer (periodically flushed to shared) */
	uint64		local_stats[POOL_MAX_STAT_COUNTERS];
} PoolLocalState;

/*
 * PoolStatIncrement -- batch stat counter updates to reduce atomic contention.
 *
 * Increments a per-backend local counter.  When the local count reaches
 * POOL_STAT_FLUSH_THRESHOLD, the accumulated value is flushed to the shared
 * atomic counter and the local counter is reset.  This reduces cross-core
 * cache-line bouncing on stat counters from every-increment to every-256th.
 *
 * The slight reporting lag (bounded at POOL_STAT_FLUSH_THRESHOLD per backend
 * per counter) is acceptable for monitoring counters that reach millions.
 */
static inline void
PoolStatIncrement(uint64 *local_counter, pg_atomic_uint64 *shared_counter)
{
	if (++(*local_counter) >= POOL_STAT_FLUSH_THRESHOLD)
	{
		pg_atomic_fetch_add_u64(shared_counter, *local_counter);
		*local_counter = 0;
	}
}

/*
 * PoolStatFlush -- flush a pending per-backend counter to the shared atomic.
 *
 * Call this in stat-view functions so that the querying backend's own
 * pending increments become visible before the shared counter is read.
 */
static inline void
PoolStatFlush(uint64 *local_counter, pg_atomic_uint64 *shared_counter)
{
	if (*local_counter > 0)
	{
		pg_atomic_fetch_add_u64(shared_counter, *local_counter);
		*local_counter = 0;
	}
}

/*
 * Shared-memory array of pool descriptors and current count.
 * BufferPoolDescs[0] is always the default pool.
 *
 * NBufferPools and MaxBufferNumber must be in shared memory so that
 * all backends (including checkpointer, bgwriter) see updates when
 * dynamic pools are created or destroyed.
 */
extern PGDLLIMPORT BufferPoolDesc *BufferPoolDescs;
extern PGDLLIMPORT pg_atomic_uint32 *UnclaimedBufferCount;
extern PGDLLIMPORT int *SharedNBufferPools;
#ifndef NBufferPools
#define NBufferPools (*SharedNBufferPools)
#endif
#ifndef MaxBufferNumber
extern PGDLLIMPORT int *SharedMaxBufferNumber;
#define MaxBufferNumber (*SharedMaxBufferNumber)
#endif

/*
 * InvalidDsmHandle - sentinel for the default pool's DSM handle
 */
#ifndef InvalidDsmHandle
#define InvalidDsmHandle ((dsm_handle) 0)
#endif

/*
 * PoolIsDynamic -- check if a pool is a non-default (dynamic) pool.
 *
 * The default pool (slot 0) uses the global BufferDescriptors/BufferBlocks
 * arrays and SharedBufHash.  Every other pool -- whether backed by its own
 * DSM segment or by a committed sub-range of the address-space reservation --
 * uses the per-pool descriptor/block/hash path.  Both kinds are "dynamic."
 */
static inline bool
PoolIsDynamic(BufferPoolDesc *pool)
{
	return pool->bp_dsm_handle != InvalidDsmHandle || pool->bp_resv_backed;
}

/*
 * PoolNeedsDsmAttach -- does this pool require a per-backend DSM mapping?
 *
 * True only for legacy DSM-backed pools (fallback path).  Reservation-backed
 * pools are mapped at the same address in every backend, so they need no
 * per-backend attach -- their pointers resolve via BufPoolAddrAt().
 */
static inline bool
PoolNeedsDsmAttach(BufferPoolDesc *pool)
{
	return pool->bp_dsm_handle != InvalidDsmHandle;
}

/*
 * Pool lookup and management functions.
 */
extern BufferPoolDesc *GetBufferPoolByOid(Oid pooloid);
extern BufferPoolDesc *GetBufferPoolByName(const char *name);
extern BufferPoolDesc *GetBufferPoolByKind(BufferPoolKind kind);
extern BufferPoolDesc *GetDefaultBufferPool(void);

/*
 * Dynamic pool buffer descriptor/block lookup functions.
 * These are called from the slow path when buffer IDs fall outside the
 * default pool's range (id >= NBuffers).
 */
extern BufferDesc *GetDynamicPoolBufferDescriptor(uint32 id);
extern Block GetDynamicPoolBlock(Buffer buffer);
extern BufferPoolDesc *GetPoolForBufferId(int buf_id);

/*
 * Per-backend DSM attachment for dynamic pools.
 * EnsurePoolAttached lazily attaches to the pool's DSM segment and
 * resolves all offset fields into virtual addresses.
 */
extern PoolLocalState *EnsurePoolAttached(BufferPoolDesc *pool);
extern PoolLocalState *TryGetPoolAttached(BufferPoolDesc *pool);
extern void DetachFromPool(int pool_slot);

/*
 * NUMA topology and placement layer (bufpool_numa.c).  Algorithm-agnostic:
 * answers node count, per-buffer node assignment, and binds memory ranges to
 * nodes (content-aware: buffer + its descriptor co-located).  Victim-selection
 * locality is NOT here -- that is the NUMA-partitioned clock sweep's job.
 */
extern int	BufPoolNumaInit(void);
extern int	BufPoolNumaNodes(void);
extern bool BufPoolNumaActive(void);
extern int	BufPoolNumaNodeForBuffer(int local_id, int nbuffers);
extern void BufPoolNumaBufferRange(int node, int nbuffers, int *start, int *end);
extern void BufPoolNumaBindRange(void *addr, Size size, int node);
extern void BufPoolNumaDistribute(char *blocks, char *descriptors,
								  Size desc_elem_size, int nbuffers);
extern int	BufPoolNumaNodeForProc(void);

/*
 * Dynamic pool lifecycle functions.
 */
extern BufferPoolDesc *CreateDynamicBufferPool(Oid bp_oid, const char *name,
											   int nbuffers,
											   const struct BufferPoolRoutine *routine,
											   Oid handler_oid,
											   bool use_huge_pages);
extern void DestroyDynamicBufferPool(BufferPoolDesc *pool);
extern BufferPoolDesc *ResizeDynamicBufferPool(BufferPoolDesc *pool,
											   int new_nbuffers);
extern BufferPoolDesc *SwapDynamicBufferPoolAlgorithm(BufferPoolDesc *pool,
													  Oid new_handler_oid);
extern void BufferPoolStartupInit(void);

/*
 * PROCSIGNAL_BARRIER_BUFPOOL_DETACH handler: drop this backend's references
 * to any pool being destroyed/resized.  Defined in bufpool.c.
 */
extern bool ProcessBarrierBufferPoolDetach(void);

/* ----------------------------------------------------------------
 * Same-address pool memory reservation (bufpool_reserve.c)
 *
 * Reserve one address-space region in the postmaster (pre-fork) so that
 * every pool's pages appear at the same virtual address in every backend.
 * Pools are committed sub-ranges of the reservation.
 * ----------------------------------------------------------------
 */
extern Size BufPoolReserveShmemSize(void);
extern PGDLLIMPORT void *BufPoolReserveCtlPtr;
extern void BufPoolReserveInit(void);
extern bool BufPoolReserveActive(void);
extern Size BufPoolReserveAlloc(Size size);
extern void BufPoolReserveFree(Size offset);
extern void *BufPoolAddrAt(Size offset);
extern void *BufPoolAttachLocal(Size offset, Size size);
extern void BufPoolAttachReservationPools(void);
extern bool BufPoolCommit(Size offset, Size size, bool huge);
extern void BufPoolDecommit(Size offset, Size size);

/*
 * Open-addressed hash table functions for dynamic pool buffer mapping.
 *
 * These implement a simple open-addressed hash table with linear probing
 * and tombstone deletion.  The table is stored in DSM as a flat array
 * of PoolBufHashEntry with no internal pointers.
 *
 * All operations require the caller to hold the appropriate mapping lock.
 */
extern int	PoolBufHashNEntries(int nbuffers);
extern Size PoolBufHashSize(int nbuffers);
extern void PoolBufHashInit(PoolBufHashEntry *entries, int nentries);
extern int	PoolBufHashLookup(PoolBufHashEntry *entries, int nentries,
							  BufferTag *tag, uint32 hashcode);
extern int	PoolBufHashInsert(PoolBufHashEntry *entries, int nentries,
							  BufferTag *tag, uint32 hashcode, int buf_id);
extern void PoolBufHashDelete(PoolBufHashEntry *entries, int nentries,
							  BufferTag *tag, uint32 hashcode);

/*
 * Per-pool trickle writer functions.
 */
extern void TrickleWriterMain(Datum main_arg);
extern void RegisterPoolTrickleWriter(BufferPoolDesc *pool, int slot);
extern void TerminatePoolTrickleWriter(BufferPoolDesc *pool);

/*
 * PoolHashPartition -- weighted-range hash partition entry.
 *
 * Maps a contiguous range of hash values to a pool slot.  Used for
 * proportional dispatch of operations across pools based on their
 * relative buffer counts.
 */
typedef struct PoolHashPartition
{
	uint64		lower_bound;	/* start of hash range (inclusive) */
	uint64		interval_size;	/* width of hash range */
	int			pool_slot;		/* index into BufferPoolDescs */
} PoolHashPartition;

/*
 * PoolHashPartitions -- collection of weighted hash partitions.
 *
 * Backend-local structure rebuilt when pools are created or destroyed.
 * Currently advisory -- actual routing uses relation-level rd_bufpool
 * assignment.  Future: lock partition dispatch within large pools.
 */
typedef struct PoolHashPartitions
{
	PoolHashPartition *entries;
	int			count;
	int			capacity;
} PoolHashPartitions;

extern void ComputeCrossPoolPartitions(PoolHashPartitions *parts);
extern int	GetPoolSlotForHash(PoolHashPartitions *parts, uint64 hash);
extern void RebuildPoolPartitions(void);

#endif							/* BUFPOOL_INTERNALS_H */
