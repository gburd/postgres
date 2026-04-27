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
 * The default pool always occupies slot 0.
 */
#define MAX_BUFFER_POOLS 64

/*
 * PoolBufHashEntry -- open-addressed hash table entry for dynamic pools.
 *
 * Dynamic pool hash tables live entirely in DSM memory as a flat array
 * of these entries, using open addressing with linear probing.  Because
 * entries contain no internal pointers, the table works correctly when
 * the DSM segment is mapped at different virtual addresses in different
 * backends.
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
	int			bp_nbuffers;	/* number of buffers in this pool */
	int			bp_first_buf;	/* starting buffer ID (0 for default pool) */

	/*
	 * Eviction algorithm vtable.  bp_routine is a process-local pointer
	 * that must be resolved by each backend separately.  For built-in
	 * algorithms the pointer is into the postgres text segment and is valid
	 * in all backends.  For extension-provided algorithms (contrib), each
	 * backend must load the extension library and call the handler function
	 * to obtain its own valid pointer.
	 *
	 * bp_handler_oid stores the handler function OID (InvalidOid for builtins).
	 * bp_handler_library and bp_handler_function store the resolved library
	 * and function names so that processes without catalog access (e.g. the
	 * trickle writer) can load the extension and resolve bp_routine.
	 */
	Oid			bp_handler_oid;		/* handler function OID (InvalidOid for builtins) */
	char		bp_handler_library[MAXPGPATH];	/* extension .so path */
	char		bp_handler_function[NAMEDATALEN]; /* handler function name */
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
	Size		bp_desc_offset;		/* BufferDescPadded array */
	Size		bp_blocks_offset;	/* buffer data blocks */
	Size		bp_io_cvs_offset;	/* I/O condition variables */
	Size		bp_strategy_offset; /* algorithm-private state */
	Size		bp_hash_offset;		/* PoolBufHashEntry array */
	Size		bp_locks_offset;	/* LWLockPadded array */
	Size		bp_ckpt_offset;		/* CkptSortItem array */

	/* Open-addressed hash table sizing (number of entries, ~2x nbuffers) */
	int			bp_hash_nentries;

	/* DSM segment handle for cross-backend attachment */
	dsm_handle	bp_dsm_handle;		/* InvalidDsmHandle for default pool */

	/* Trickle writer background worker (stored inline for cross-backend use) */
	int			bp_trickle_slot;	/* BGW slot (-1 = none) */
	uint64		bp_trickle_generation;	/* BGW generation */

	/* Pool state */
	bool		bp_active;		/* true if pool is usable */

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
} PoolLocalState;

/*
 * Shared-memory array of pool descriptors and current count.
 * BufferPoolDescs[0] is always the default pool.
 *
 * NBufferPools and MaxBufferNumber must be in shared memory so that
 * all backends (including checkpointer, bgwriter) see updates when
 * dynamic pools are created or destroyed.
 */
extern PGDLLIMPORT BufferPoolDesc *BufferPoolDescs;
extern int *SharedNBufferPools;
#ifndef NBufferPools
#define NBufferPools (*SharedNBufferPools)
#endif
#ifndef MaxBufferNumber
extern int *SharedMaxBufferNumber;
#define MaxBufferNumber (*SharedMaxBufferNumber)
#endif

/*
 * InvalidDsmHandle - sentinel for the default pool's DSM handle
 */
#ifndef InvalidDsmHandle
#define InvalidDsmHandle ((dsm_handle) 0)
#endif

/*
 * PoolIsDynamic -- check if a pool is a DSM-backed dynamic pool.
 *
 * The default pool has bp_dsm_handle == InvalidDsmHandle.
 * Dynamic pools have a valid DSM handle.
 */
static inline bool
PoolIsDynamic(BufferPoolDesc *pool)
{
	return pool->bp_dsm_handle != InvalidDsmHandle;
}

/*
 * Pool lookup and management functions.
 */
extern BufferPoolDesc *GetBufferPoolByOid(Oid pooloid);
extern BufferPoolDesc *GetBufferPoolByName(const char *name);
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
 * Pool partition sizing.
 */
extern int	ComputePoolPartitions(int nbuffers, bool scan_only);

/*
 * Dynamic pool lifecycle functions.
 */
extern BufferPoolDesc *CreateDynamicBufferPool(Oid bp_oid, const char *name,
											   int nbuffers,
											   const struct BufferPoolRoutine *routine,
											   Oid handler_oid);
extern void DestroyDynamicBufferPool(BufferPoolDesc *pool);
extern void BufferPoolStartupInit(void);

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

#endif							/* BUFPOOL_INTERNALS_H */
