/*-------------------------------------------------------------------------
 *
 * bufpool.c
 *	  Multi-pool buffer management infrastructure.
 *
 * This file manages the shared-memory array of BufferPoolDesc descriptors
 * and provides lookup functions for pool routing.  The default pool
 * (slot 0) always uses clock-sweep and owns all of shared_buffers.
 * Dynamic pools are created via DSM segments with per-backend lazy
 * attachment for cross-backend access.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/buffer/bufpool.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "postmaster/bgwriter.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "storage/buf_internals.h"
#include "storage/aio.h"
#include "storage/proclist.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "utils/guc.h"
#include "utils/injection_point.h"
#include "utils/wait_event.h"
#include "storage/bufmgr.h"
#include "storage/bufpool.h"
#include "storage/bufpool_internals.h"
#include "storage/condition_variable.h"
#include "storage/dsm.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/smgr.h"
#include "storage/spin.h"
#include "storage/subsystems.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "catalog/pg_bufferpool.h"
#include "catalog/pg_proc.h"
#include "funcapi.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/syscache.h"
#include "utils/resowner.h"
#include "utils/tuplestore.h"

/* Shared-memory array of pool descriptors */
BufferPoolDesc *BufferPoolDescs = NULL;

/*
 * NBufferPools and MaxBufferNumber live in shared memory so all backends
 * (including checkpointer, bgwriter, etc.) see updates when dynamic
 * pools are created or destroyed.
 *
 * Before shared memory is initialized (early startup), the pointers
 * reference process-local fallback variables.
 */
static int	LocalNBufferPools = 0;
static int	LocalMaxBufferNumber = 0;
int		   *SharedNBufferPools = &LocalNBufferPools;
int		   *SharedMaxBufferNumber = &LocalMaxBufferNumber;

/* Global count of unclaimed buffer slots available for pool expansion */
pg_atomic_uint32 *UnclaimedBufferCount = NULL;

/*
 * Per-backend local state for each pool slot.  Lazily populated by
 * EnsurePoolAttached() when a backend first accesses a dynamic pool.
 */
static PoolLocalState PoolLocalStates[MAX_BUFFER_POOLS];

/*
 * Shared-memory flag for one-time startup initialization of dynamic pools.
 * 0 = not started, 1 = in progress, 2 = done.
 */
static pg_atomic_uint32 *BufferPoolStartupFlag = NULL;

/* GUC variables for trickle writer tuning */
int			trickle_flush_after = DEFAULT_TRICKLE_FLUSH_AFTER;
int			trickle_write_batch_size = 128;

/* GUC variable for RECYCLE pool sizing (0 = disabled) */
int			recycle_pool_buffers = 0;

static void BufferPoolShmemRequest(void *arg);
static void BufferPoolShmemInit(void *arg);

const ShmemCallbacks BufferPoolShmemCallbacks = {
	.request_fn = BufferPoolShmemRequest,
	.init_fn = BufferPoolShmemInit,
};

/*
 * BufferPoolShmemRequest -- request shared memory for pool descriptors.
 */
static void
BufferPoolShmemRequest(void *arg)
{
	ShmemRequestStruct(.name = "Buffer Pool Descriptors",
					   .size = MAX_BUFFER_POOLS * sizeof(BufferPoolDesc),
					   .alignment = PG_CACHE_LINE_SIZE,
					   .ptr = (void **) &BufferPoolDescs,
		);
	ShmemRequestStruct(.name = "Buffer Pool Startup Flag",
					   .size = sizeof(pg_atomic_uint32),
					   .alignment = sizeof(pg_atomic_uint32),
					   .ptr = (void **) &BufferPoolStartupFlag,
		);
	ShmemRequestStruct(.name = "Buffer Pool NBufferPools",
					   .size = sizeof(int),
					   .alignment = sizeof(int),
					   .ptr = (void **) &SharedNBufferPools,
		);
	ShmemRequestStruct(.name = "Buffer Pool MaxBufferNumber",
					   .size = sizeof(int),
					   .alignment = sizeof(int),
					   .ptr = (void **) &SharedMaxBufferNumber,
		);
	ShmemRequestStruct(.name = "Buffer Pool Unclaimed Count",
					   .size = sizeof(pg_atomic_uint32),
					   .alignment = sizeof(pg_atomic_uint32),
					   .ptr = (void **) &UnclaimedBufferCount,
		);
	ShmemRequestStruct(.name = "Buffer Pool Reservation Control",
					   .size = BufPoolReserveShmemSize(),
					   .alignment = PG_CACHE_LINE_SIZE,
					   .ptr = (void **) &BufPoolReserveCtlPtr,
		);
}

/*
 * BufferPoolShmemInit -- initialize pool descriptor array.
 *
 * Sets up the default pool (slot 0).  The default pool uses the global
 * BufferDescriptors/BufferBlocks arrays and SharedBufHash, so its offset
 * fields are all zero and bp_dsm_handle is InvalidDsmHandle.
 */
static void
BufferPoolShmemInit(void *arg)
{
	BufferPoolDesc *defpool;

	/* Zero the entire array */
	MemSet(BufferPoolDescs, 0, MAX_BUFFER_POOLS * sizeof(BufferPoolDesc));

	/* Initialize the default pool at slot 0 */
	defpool = &BufferPoolDescs[0];
	defpool->bp_oid = InvalidOid;	/* default pool has no catalog OID */
	namestrcpy(&defpool->bp_name, "default");
	defpool->bp_nbuffers = NBuffers;
	defpool->bp_first_buf = 0;
	defpool->bp_routine = ActivePoolRoutine;

	/*
	 * The default pool uses the global buffer mapping lock array in
	 * MainLWLockArray.  NUM_BUFFER_PARTITIONS is the partition count.
	 */
	defpool->bp_num_partitions = NUM_BUFFER_PARTITIONS;

	/* All offset fields are zero (unused for default pool) */
	defpool->bp_desc_offset = 0;
	defpool->bp_blocks_offset = 0;
	defpool->bp_io_cvs_offset = 0;
	defpool->bp_strategy_offset = 0;
	defpool->bp_hash_offset = 0;
	defpool->bp_hash_nentries = 0;
	defpool->bp_locks_offset = 0;
	defpool->bp_ckpt_offset = 0;

	defpool->bp_dsm_handle = InvalidDsmHandle;
	defpool->bp_trickle_slot = -1;
	defpool->bp_use_direct_io = false;
	defpool->bp_active = true;

	/* Oversubscription: default pool owns all of shared_buffers */
	defpool->bp_target_buffers = NBuffers;
	defpool->bp_current_buffers = NBuffers;
	defpool->bp_oversubscribed = false;

	pg_atomic_init_u64(&defpool->bp_reads, 0);
	pg_atomic_init_u64(&defpool->bp_hits, 0);
	pg_atomic_init_u64(&defpool->bp_evictions, 0);

	NBufferPools = 1;
	MaxBufferNumber = NBuffers;

	/* No unclaimed buffers at startup -- default pool owns everything */
	pg_atomic_init_u32(UnclaimedBufferCount, 0);

	/* Initialize per-backend local state array */
	MemSet(PoolLocalStates, 0, sizeof(PoolLocalStates));

	/*
	 * Populate PoolLocalStates[0] from global shmem pointers so the DEFAULT
	 * pool can be accessed through the same PoolLocalState interface as
	 * dynamic pools.  No DSM segment -- these point into main shared memory.
	 */
	PoolLocalStates[0].seg = NULL;	/* not DSM-backed */
	PoolLocalStates[0].descriptors = BufferDescriptors;
	PoolLocalStates[0].blocks = BufferBlocks;
	PoolLocalStates[0].io_cvs = BufferIOCVArray;
	PoolLocalStates[0].hash_entries = NULL; /* uses SharedBufHash */
	PoolLocalStates[0].mapping_locks = NULL;	/* uses MainLWLockArray */
	PoolLocalStates[0].ckpt_ids = CkptBufferIds;
	PoolLocalStates[0].strategy_data = ActivePoolData;
	PoolLocalStates[0].attached = true;

	/* Initialize startup flag for dynamic pool recreation */
	pg_atomic_init_u32(BufferPoolStartupFlag, 0);
}

/*
 * EnsurePoolAttached -- lazily attach to a dynamic pool's DSM segment.
 *
 * On first call for a given pool, attaches to the pool's DSM segment and
 * resolves all offset fields into virtual addresses.  Subsequent calls
 * return the cached PoolLocalState immediately.
 *
 * Must only be called for dynamic pools (PoolIsDynamic(pool) == true).
 * The default pool uses global arrays directly.
 */
PoolLocalState *
EnsurePoolAttached(BufferPoolDesc *pool)
{
	int			slot = pool - BufferPoolDescs;
	PoolLocalState *local = &PoolLocalStates[slot];
	char	   *base;

	Assert(slot > 0 && slot < MAX_BUFFER_POOLS);
	Assert(PoolIsDynamic(pool));
	Assert(pool->bp_active);

	if (likely(local->attached))
		return local;

	if (pool->bp_resv_backed)
	{
		/*
		 * Reservation-backed pool: the memory is at the same virtual address
		 * in every backend, but a MAP_FIXED commit only updates the
		 * committing backend's page tables.  A backend that mapped the
		 * reservation (PROT_NONE) before this pool was created must re-map
		 * the committed sub-range read/write in its own address space before
		 * touching the pool.  BufPoolAttachLocal does that at the same
		 * address (idempotent for the creating backend).
		 */
		base = (char *) BufPoolAttachLocal(pool->bp_resv_offset,
										   pool->bp_resv_size);
		if (base == NULL)
			elog(ERROR, "could not map reservation for buffer pool \"%s\"",
				 NameStr(pool->bp_name));
		local->seg = NULL;
	}
	else
	{
		/* First access from this backend: attach to the DSM segment */
		local->seg = dsm_attach(pool->bp_dsm_handle);
		if (local->seg == NULL)
			elog(ERROR, "could not attach to DSM segment for buffer pool \"%s\"",
				 NameStr(pool->bp_name));
		dsm_pin_mapping(local->seg);

		base = dsm_segment_address(local->seg);
	}

	/* Resolve offsets to virtual addresses */
	local->descriptors = (BufferDescPadded *) (base + pool->bp_desc_offset);
	local->blocks = base + pool->bp_blocks_offset;
	local->io_cvs = (ConditionVariableMinimallyPadded *) (base + pool->bp_io_cvs_offset);
	local->hash_entries = (PoolBufHashEntry *) (base + pool->bp_hash_offset);
	local->mapping_locks = (LWLockPadded *) (base + pool->bp_locks_offset);
	local->ckpt_ids = (CkptSortItem *) (base + pool->bp_ckpt_offset);
	local->strategy_data = pool->bp_strategy_offset > 0 ?
		base + pool->bp_strategy_offset : NULL;

	local->attached = true;

	return local;
}

/*
 * TryGetPoolAttached -- return the pool's local state if already attached,
 * or NULL if not yet attached.
 *
 * Unlike EnsurePoolAttached(), this never attempts to attach to the DSM
 * segment.  Use this in code paths where DSM attachment is unsafe (e.g.,
 * during ResourceOwner release) or when scanning pools speculatively and
 * it's acceptable to skip unattached pools.
 */
PoolLocalState *
TryGetPoolAttached(BufferPoolDesc *pool)
{
	int			slot = pool - BufferPoolDescs;

	Assert(slot > 0 && slot < MAX_BUFFER_POOLS);

	if (PoolLocalStates[slot].attached)
		return &PoolLocalStates[slot];
	return NULL;
}

/*
 * DetachFromPool -- detach from a dynamic pool's DSM segment.
 *
 * Called when a pool is being destroyed to clean up this backend's
 * local state.
 */
void
DetachFromPool(int pool_slot)
{
	PoolLocalState *local;

	Assert(pool_slot > 0 && pool_slot < MAX_BUFFER_POOLS);
	local = &PoolLocalStates[pool_slot];

	if (!local->attached)
		return;

	/*
	 * Don't detach the DSM if it was created by this backend via dsm_create;
	 * in that case DestroyDynamicBufferPool handles cleanup.
	 */
	if (local->seg)
	{
		dsm_detach(local->seg);
		local->seg = NULL;
	}

	local->descriptors = NULL;
	local->blocks = NULL;
	local->io_cvs = NULL;
	local->hash_entries = NULL;
	local->mapping_locks = NULL;
	local->ckpt_ids = NULL;
	local->strategy_data = NULL;
	local->attached = false;
}

/*
 * ProcessBarrierBufferPoolDetach -- handle PROCSIGNAL_BARRIER_BUFPOOL_DETACH.
 *
 * Invoked in every backend when a pool is being destroyed or resized.  The
 * destroying backend marks the pool inactive (bp_active = false) with a
 * write barrier before emitting the ProcSignalBarrier, so by the time we run
 * here every slot whose pool is gone reads as inactive.
 *
 * This backend drops its cached pointer to any now-inactive pool
 * (CurrentBufferPool) and detaches from the pool's DSM mapping.  After
 * WaitForProcSignalBarrier() returns in the destroyer, no backend holds a
 * live reference into the pool's DSM, so it is safe to dsm_detach/unpin and
 * tear the segment down.
 *
 * Returns true (always processed; never needs to be retried).  Must not
 * throw -- a barrier handler that ERRORs would be retried indefinitely.
 */
bool
ProcessBarrierBufferPoolDetach(void)
{
	/* Forget any per-relation pool pointer that may target a dying pool. */
	ResetCurrentBufferPool();

	/*
	 * Detach from every dynamic pool slot whose pool is no longer active. The
	 * creating backend's own slot is cleared directly in
	 * DestroyDynamicBufferPool after the barrier completes, so skipping an
	 * already-detached slot here is harmless.
	 */
	for (int slot = 1; slot < MAX_BUFFER_POOLS; slot++)
	{
		BufferPoolDesc *pool = &BufferPoolDescs[slot];

		if (!PoolLocalStates[slot].attached)
			continue;

		/*
		 * pg_read_barrier pairs with the pg_write_barrier in
		 * DestroyDynamicBufferPool so we observe bp_active = false here.
		 */
		pg_read_barrier();
		if (!pool->bp_active)
			DetachFromPool(slot);
	}

	return true;
}

/*
 * GetBufferPoolByOid -- look up a pool descriptor by catalog OID.
 *
 * Returns NULL if no pool with the given OID exists.
 */
BufferPoolDesc *
GetBufferPoolByOid(Oid pooloid)
{
	for (int i = 0; i < NBufferPools; i++)
	{
		if (BufferPoolDescs[i].bp_active &&
			BufferPoolDescs[i].bp_oid == pooloid)
			return &BufferPoolDescs[i];
	}
	return NULL;
}

/*
 * GetBufferPoolByName -- look up a pool descriptor by name.
 *
 * Returns NULL if no pool with the given name exists.
 */
BufferPoolDesc *
GetBufferPoolByName(const char *name)
{
	for (int i = 0; i < NBufferPools; i++)
	{
		if (BufferPoolDescs[i].bp_active &&
			strcmp(NameStr(BufferPoolDescs[i].bp_name), name) == 0)
			return &BufferPoolDescs[i];
	}
	return NULL;
}

/*
 * GetBufferPoolByKind -- return the first active pool of the given kind.
 *
 * Intended for looking up the well-known system pools (RECYCLE) without
 * relying on a fixed name that could collide with a user-created pool.
 * Returns NULL if no active pool of this kind exists.
 */
BufferPoolDesc *
GetBufferPoolByKind(BufferPoolKind kind)
{
	for (int i = 0; i < NBufferPools; i++)
	{
		if (BufferPoolDescs[i].bp_active && BufferPoolDescs[i].bp_kind == kind)
			return &BufferPoolDescs[i];
	}
	return NULL;
}

/*
 * GetDefaultBufferPool -- return the default pool descriptor (slot 0).
 */
BufferPoolDesc *
GetDefaultBufferPool(void)
{
	Assert(NBufferPools > 0);
	Assert(BufferPoolDescs[0].bp_active);
	return &BufferPoolDescs[0];
}

/*
 * BufferPoolStartupInit -- recreate dynamic buffer pools after server restart.
 *
 * After a server restart, the DSM segments backing dynamic pools are gone.
 * This function scans the pg_bufferpool catalog and recreates any non-default
 * pools.  It's called from InitPostgres once catalog access is available.
 *
 * Uses an atomic flag in shared memory to ensure only one backend performs
 * the recreation, while other backends wait for it to complete.
 */
void
BufferPoolStartupInit(void)
{
	uint32		expected;
	Relation	rel;
	TableScanDesc scan;
	HeapTuple	tup;

	/* Fast path: already done */
	if (pg_atomic_read_u32(BufferPoolStartupFlag) == 2)
		return;

	/* Try to claim initialization responsibility (CAS 0 -> 1) */
	expected = 0;
	if (!pg_atomic_compare_exchange_u32(BufferPoolStartupFlag, &expected, 1))
	{
		/* Someone else is doing it or already did it; wait for completion */
		while (pg_atomic_read_u32(BufferPoolStartupFlag) == 1)
			pg_usleep(1000);	/* 1ms */
		return;
	}

	/*
	 * Create the RECYCLE pool if configured.  This is a well-known system
	 * pool (not catalog-backed) that replaces per-backend ring buffers with a
	 * shared pool for bulk reads, bulk writes, and VACUUM operations.
	 */
	if (recycle_pool_buffers > 0 && GetBufferPoolByKind(BUFPOOL_RECYCLE) == NULL)
	{
		BufferPoolDesc *recycle_pool;

		recycle_pool = CreateDynamicBufferPool(InvalidOid, "recycle",
											   recycle_pool_buffers,
											   &recycle_pool_routine,
											   InvalidOid, false);
		recycle_pool->bp_kind = BUFPOOL_RECYCLE;
		elog(LOG, "created RECYCLE pool with %d buffers", recycle_pool_buffers);
	}

	/* Scan pg_bufferpool and recreate dynamic pools */
	rel = table_open(BufferPoolRelationId, AccessShareLock);
	scan = table_beginscan_catalog(rel, 0, NULL);

	while ((tup = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		Form_pg_bufferpool bpform = (Form_pg_bufferpool) GETSTRUCT(tup);
		const char *poolname = NameStr(bpform->bpname);

		/* Skip the default pool -- it's always set up in shmem init */
		if (strcmp(poolname, "default") == 0)
			continue;

		/* Skip if this pool already exists in shared memory */
		if (GetBufferPoolByOid(bpform->oid) != NULL)
			continue;

		/* Recreate the dynamic pool */
		{
			const BufferPoolRoutine *routine;
			Datum		datum;
			int			pool_nbuffers;

			datum = OidFunctionCall0(bpform->bphandler);
			routine = (const BufferPoolRoutine *) DatumGetPointer(datum);

			pool_nbuffers = (int) (bpform->bpsize / BLCKSZ);
			if (pool_nbuffers >= 16)
				CreateDynamicBufferPool(bpform->oid, poolname,
										pool_nbuffers, routine,
										bpform->bphandler, false);
			else
				elog(WARNING, "buffer pool \"%s\" has too few buffers (%d), skipping",
					 poolname, pool_nbuffers);
		}
	}

	table_endscan(scan);
	table_close(rel, AccessShareLock);

	/* Signal completion */
	pg_write_barrier();
	pg_atomic_write_u32(BufferPoolStartupFlag, 2);
}

/*
 * GetDynamicPoolBufferDescriptor -- look up a buffer descriptor by ID
 * in dynamic pools.
 *
 * This is the slow path called from GetBufferDescriptor() when the buffer
 * ID falls outside the default pool's range (id >= NBuffers).
 */
BufferDesc *
GetDynamicPoolBufferDescriptor(uint32 id)
{
	for (int i = 1; i < NBufferPools; i++)
	{
		BufferPoolDesc *pool = &BufferPoolDescs[i];
		PoolLocalState *local;

		if (!pool->bp_active)
			continue;

		if (id >= (uint32) pool->bp_first_buf &&
			id < (uint32) (pool->bp_first_buf + pool->bp_nbuffers))
		{
			uint32		local_id = id - pool->bp_first_buf;

			local = EnsurePoolAttached(pool);
			return &local->descriptors[local_id].bufferdesc;
		}
	}

	elog(ERROR, "buffer descriptor %u not found in any pool (NBuffers=%d, NBufferPools=%d, as_int=%d)",
		 id, NBuffers, NBufferPools, (int) id);
	pg_unreachable();
}

/*
 * GetDynamicPoolIOCV -- return the I/O condition variable for a buffer in
 * a dynamic pool.
 *
 * This is the slow path called from BufferDescriptorGetIOCV() when the
 * buffer ID falls outside the default pool's range.
 */
ConditionVariable *
GetDynamicPoolIOCV(int buf_id)
{
	for (int i = 1; i < NBufferPools; i++)
	{
		BufferPoolDesc *pool = &BufferPoolDescs[i];
		PoolLocalState *local;

		if (!pool->bp_active)
			continue;

		if (buf_id >= pool->bp_first_buf &&
			buf_id < pool->bp_first_buf + pool->bp_nbuffers)
		{
			int			local_id = buf_id - pool->bp_first_buf;

			local = EnsurePoolAttached(pool);
			return &local->io_cvs[local_id].cv;
		}
	}

	elog(ERROR, "buffer IO CV for buf_id %d not found in any pool", buf_id);
	pg_unreachable();
}

/*
 * GetDynamicPoolBlock -- return the data block for a buffer in a dynamic pool.
 *
 * This is the slow path called from BufferGetBlock() when the buffer number
 * falls outside the default pool's range.
 */
Block
GetDynamicPoolBlock(Buffer buffer)
{
	int			buf_id = buffer - 1;

	for (int i = 1; i < NBufferPools; i++)
	{
		BufferPoolDesc *pool = &BufferPoolDescs[i];
		PoolLocalState *local;

		if (!pool->bp_active)
			continue;

		if (buf_id >= pool->bp_first_buf &&
			buf_id < pool->bp_first_buf + pool->bp_nbuffers)
		{
			int			local_id = buf_id - pool->bp_first_buf;

			local = EnsurePoolAttached(pool);
			return (Block) (local->blocks + ((Size) local_id) * BLCKSZ);
		}
	}

	elog(ERROR, "buffer %d not found in any pool", buffer);
	pg_unreachable();
}

/*
 * GetPoolForBufferId -- find which pool owns a given buffer ID.
 *
 * Returns the default pool descriptor for buf_id < NBuffers.
 * Returns the owning dynamic pool for higher buffer IDs.
 * Returns NULL only if the buffer ID doesn't belong to any active pool.
 */
BufferPoolDesc *
GetPoolForBufferId(int buf_id)
{
	if (buf_id < NBuffers)
		return &BufferPoolDescs[0];

	for (int i = 1; i < NBufferPools; i++)
	{
		BufferPoolDesc *pool = &BufferPoolDescs[i];

		if (!pool->bp_active)
			continue;

		if (buf_id >= pool->bp_first_buf &&
			buf_id < pool->bp_first_buf + pool->bp_nbuffers)
			return pool;
	}

	return NULL;
}

/*
 * BufPoolAttachReservationPools -- map all active reservation-backed pools
 *		into this process's address space.
 *
 * Called by IO workers before performing buffer I/O.  An IO worker reads and
 * writes buffer memory using the issuer's virtual addresses (carried in the
 * shared iovec).  For reservation-backed pools those addresses are the same
 * in every process, but a MAP_FIXED commit only updates the committing
 * backend's page tables: an IO worker that forked before a pool was created
 * must re-map the committed sub-range before dereferencing its addresses, or
 * it SIGSEGVs.  This attaches every active reservation pool (idempotent and
 * cheap -- one mmap per not-yet-mapped pool).  Legacy DSM pools are skipped;
 * their buffers are never handed to IO workers (forced synchronous I/O).
 */
void
BufPoolAttachReservationPools(void)
{
	for (int i = 1; i < NBufferPools; i++)
	{
		BufferPoolDesc *pool = &BufferPoolDescs[i];

		if (!pool->bp_active || !pool->bp_resv_backed)
			continue;
		if (PoolLocalStates[i].attached)
			continue;
		(void) EnsurePoolAttached(pool);
	}
}

/*
 * Compute the next available buffer ID offset for dynamic pools.
 * Dynamic pool buffer IDs start at NBuffers and grow from there.
 */
static int
ComputeNextBufferIdBase(void)
{
	int			next = NBuffers;

	for (int i = 1; i < NBufferPools; i++)
	{
		BufferPoolDesc *pool = &BufferPoolDescs[i];

		if (!pool->bp_active)
			continue;

		if (pool->bp_first_buf + pool->bp_nbuffers > next)
			next = pool->bp_first_buf + pool->bp_nbuffers;
	}

	return next;
}

/*
 * CreateDynamicBufferPool
 *		Allocate a DSM-backed dynamic buffer pool.
 *
 * Creates a DSM segment containing buffer descriptors, data blocks,
 * I/O condition variables, algorithm state, checkpoint sort array,
 * mapping lock, and an open-addressed hash table.
 *
 * All per-pool data is accessed through offsets stored in BufferPoolDesc.
 * Each backend lazily attaches to the DSM via EnsurePoolAttached().
 *
 * Dynamic pools use a single mapping lock (bp_num_partitions = 1)
 * to avoid cross-partition interference in the open-addressed hash table.
 *
 * Returns the pool descriptor on success.
 */
BufferPoolDesc *
CreateDynamicBufferPool(Oid bp_oid, const char *name, int nbuffers,
						const BufferPoolRoutine *routine, Oid handler_oid,
						bool use_huge_pages)
{
	BufferPoolDesc *pool;
	dsm_segment *seg;
	char	   *dsm_base;
	Size		total_size;
	Size		descs_size;
	Size		blocks_size;
	Size		io_cvs_size;
	Size		algo_size;
	Size		ckpt_size;
	Size		locks_size;
	Size		hash_size;
	Size		offset;
	int			slot = -1;
	int			hash_nentries;
	PoolLocalState *local;
	BufferDescPadded *descs;
	char	   *blocks;
	ConditionVariableMinimallyPadded *io_cvs;
	PoolBufHashEntry *hash_entries;
	LWLockPadded *mapping_locks;
	CkptSortItem *ckpt_ids;
	void	   *strategy_data;

	/*
	 * Check that we haven't run out of pool slots.  NBufferPools is a
	 * high-water mark; the real check is finding a free (inactive) slot
	 * below.
	 */
	if (NBufferPools >= MAX_BUFFER_POOLS)
	{
		/* All slots at or below the high-water mark are taken; can we reuse? */
		bool		has_free = false;

		for (int i = 1; i < MAX_BUFFER_POOLS; i++)
		{
			if (!BufferPoolDescs[i].bp_active)
			{
				has_free = true;
				break;
			}
		}
		if (!has_free)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("maximum number of buffer pools (%d) exceeded",
							MAX_BUFFER_POOLS)));
	}

	/* Find a free slot */
	for (int i = 1; i < MAX_BUFFER_POOLS; i++)
	{
		if (!BufferPoolDescs[i].bp_active)
		{
			slot = i;
			break;
		}
	}
	if (slot < 0)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("no free buffer pool slots")));

	/*
	 * Dynamic pools use a single mapping lock partition.  This avoids
	 * cross-partition interference in the open-addressed hash table, since
	 * entries from different hash partitions can share probe chains in open
	 * addressing.  For the typical dynamic pool size (<= a few thousand
	 * buffers), a single lock provides adequate concurrency.
	 */

	/*
	 * Compute DSM segment layout sizes with alignment.
	 */
	hash_nentries = PoolBufHashNEntries(nbuffers);

	descs_size = MAXALIGN(sizeof(BufferDescPadded) * nbuffers);
	blocks_size = TYPEALIGN(PG_IO_ALIGN_SIZE, (Size) nbuffers * BLCKSZ);
	io_cvs_size = MAXALIGN(sizeof(ConditionVariableMinimallyPadded) * nbuffers);
	algo_size = routine->shmem_size ? MAXALIGN(routine->shmem_size(nbuffers)) : 0;
	ckpt_size = MAXALIGN(sizeof(CkptSortItem) * nbuffers);
	locks_size = MAXALIGN(sizeof(LWLockPadded));	/* single lock */
	hash_size = MAXALIGN(PoolBufHashSize(nbuffers));

	/*
	 * The blocks array needs PG_IO_ALIGN_SIZE alignment for direct I/O. Add
	 * padding after descriptors to ensure blocks start aligned.
	 */
	total_size = descs_size + PG_IO_ALIGN_SIZE + blocks_size + io_cvs_size +
		algo_size + ckpt_size + locks_size + hash_size;

	/*
	 * Acquire the pool's base memory.  Preferred path: a committed sub-range
	 * of the address-space reservation, which maps at the same virtual
	 * address in every backend (enables same-address pointers, AIO on pool
	 * buffers, and online resize).  Fallback path (reservation disabled or
	 * unsupported, or reservation exhausted): a per-pool DSM segment,
	 * attached per backend via offsets.
	 */
	pool = &BufferPoolDescs[slot];
	MemSet(pool, 0, sizeof(BufferPoolDesc));

	if (BufPoolReserveActive())
	{
		Size		resv_off = BufPoolReserveAlloc(total_size);

		if (resv_off != (Size) -1)
		{
			if (!BufPoolCommit(resv_off, total_size, use_huge_pages))
			{
				BufPoolReserveFree(resv_off);
				ereport(ERROR,
						(errcode(ERRCODE_OUT_OF_MEMORY),
						 errmsg("could not commit memory for buffer pool \"%s\"",
								name)));
			}
			pool->bp_resv_backed = true;
			pool->bp_resv_offset = resv_off;
			pool->bp_resv_size = total_size;
			pool->bp_resv_huge = use_huge_pages;
			pool->bp_dsm_handle = InvalidDsmHandle;
			seg = NULL;
			dsm_base = (char *) BufPoolAddrAt(resv_off);
		}
		else
		{
			/* reservation exhausted: fall back to a private DSM segment */
			seg = dsm_create(total_size, 0);
			dsm_pin_segment(seg);
			dsm_pin_mapping(seg);
			dsm_base = dsm_segment_address(seg);
		}
	}
	else
	{
		/* Create and pin DSM segment (legacy / unsupported-platform path) */
		seg = dsm_create(total_size, 0);
		dsm_pin_segment(seg);
		dsm_pin_mapping(seg);
		dsm_base = dsm_segment_address(seg);
	}

	/* Carve the base region into per-pool arrays and record offsets */

	offset = 0;

	pool->bp_desc_offset = offset;
	descs = (BufferDescPadded *) (dsm_base + offset);
	offset += descs_size;

	/* Align blocks to PG_IO_ALIGN_SIZE for direct I/O support */
	offset = TYPEALIGN(PG_IO_ALIGN_SIZE, (uintptr_t) (dsm_base + offset)) -
		(uintptr_t) dsm_base;
	pool->bp_blocks_offset = offset;
	blocks = dsm_base + offset;
	offset += blocks_size;

	pool->bp_io_cvs_offset = offset;
	io_cvs = (ConditionVariableMinimallyPadded *) (dsm_base + offset);
	offset += io_cvs_size;

	if (algo_size > 0)
	{
		pool->bp_strategy_offset = offset;
		strategy_data = dsm_base + offset;
	}
	else
	{
		pool->bp_strategy_offset = 0;
		strategy_data = NULL;
	}
	offset += algo_size;

	pool->bp_ckpt_offset = offset;
	ckpt_ids = (CkptSortItem *) (dsm_base + offset);
	offset += ckpt_size;

	pool->bp_locks_offset = offset;
	mapping_locks = (LWLockPadded *) (dsm_base + offset);
	offset += locks_size;

	pool->bp_hash_offset = offset;
	hash_entries = (PoolBufHashEntry *) (dsm_base + offset);
	offset += hash_size;

	/* Set buffer ID range */
	pool->bp_first_buf = ComputeNextBufferIdBase();
	pool->bp_nbuffers = nbuffers;
	pool->bp_num_partitions = 1;	/* single lock for open-addressed hash */
	pool->bp_hash_nentries = hash_nentries;

	/* Initialize buffer descriptors */
	for (int i = 0; i < nbuffers; i++)
	{
		BufferDesc *buf = &descs[i].bufferdesc;

		ClearBufferTag(&buf->tag);
		buf->buf_id = pool->bp_first_buf + i;
		pg_atomic_init_u64(&buf->state, 0);
		buf->wait_backend_pgprocno = INVALID_PROC_NUMBER;

		/*
		 * Initialize the AIO wait reference and the content-lock waiter list,
		 * exactly as buf_init.c does for the default pool.  These were
		 * previously omitted; it was latent only because pool buffers forced
		 * synchronous I/O (io_wref unused) -- once AIO is enabled for
		 * same-address pools, an uninitialized io_wref / lock_waiters
		 * corrupts the buffer content-lock waitlist (Assert failure in
		 * UnlockBuffer).
		 */
		pgaio_wref_clear(&buf->io_wref);
		proclist_init(&buf->lock_waiters);
	}

	/* Initialize I/O condition variables */
	for (int i = 0; i < nbuffers; i++)
		ConditionVariableInit(&io_cvs[i].cv);

	/* Zero data blocks */
	MemSet(blocks, 0, (Size) nbuffers * BLCKSZ);

	/* Initialize per-pool open-addressed hash table */
	PoolBufHashInit(hash_entries, hash_nentries);

	/* Initialize mapping LWLock (single partition) */
	{
		char		tranche_name[64];
		int			tranche_id;

		snprintf(tranche_name, sizeof(tranche_name), "bufpool_%s_mapping", name);
		tranche_id = LWLockNewTrancheId(pstrdup(tranche_name));

		LWLockInitialize(&mapping_locks[0].lock, tranche_id);
	}

	/* Initialize algorithm state */
	pool->bp_routine = routine;
	if (routine->shmem_init)
		routine->shmem_init(strategy_data, nbuffers, pool->bp_first_buf, true);

	/* Fill remaining pool descriptor fields */
	pool->bp_oid = bp_oid;
	pool->bp_handler_oid = handler_oid;
	pool->bp_handler_library[0] = '\0';
	pool->bp_handler_function[0] = '\0';

	/*
	 * For extension-provided handlers, look up the library path (probin) and
	 * symbol name (prosrc) from pg_proc and store them in the pool
	 * descriptor.  This allows processes without catalog access (like the
	 * trickle writer) to load the extension library and resolve bp_routine in
	 * their own address space.
	 */
	if (OidIsValid(handler_oid))
	{
		HeapTuple	procTup;

		procTup = SearchSysCache1(PROCOID, ObjectIdGetDatum(handler_oid));
		if (HeapTupleIsValid(procTup))
		{
			Datum		prosrc,
						probin;
			bool		isnull;

			prosrc = SysCacheGetAttr(PROCOID, procTup,
									 Anum_pg_proc_prosrc, &isnull);
			if (!isnull)
				strlcpy(pool->bp_handler_function,
						TextDatumGetCString(prosrc),
						sizeof(pool->bp_handler_function));

			probin = SysCacheGetAttr(PROCOID, procTup,
									 Anum_pg_proc_probin, &isnull);
			if (!isnull)
				strlcpy(pool->bp_handler_library,
						TextDatumGetCString(probin),
						sizeof(pool->bp_handler_library));

			ReleaseSysCache(procTup);
		}
	}

	namestrcpy(&pool->bp_name, name);

	/*
	 * Record the DSM handle only for the fallback DSM path.  Reservation-
	 * backed pools already set bp_resv_* above and keep bp_dsm_handle =
	 * InvalidDsmHandle (PoolNeedsDsmAttach stays false for them).
	 */
	if (seg != NULL)
		pool->bp_dsm_handle = dsm_segment_handle(seg);
	pool->bp_trickle_slot = -1;
	pool->bp_use_direct_io = false;

	/* Oversubscription tracking: target = configured, current = actual */
	pool->bp_target_buffers = nbuffers;
	pool->bp_current_buffers = nbuffers;
	pool->bp_oversubscribed = false;

	pg_atomic_init_u64(&pool->bp_reads, 0);
	pg_atomic_init_u64(&pool->bp_hits, 0);
	pg_atomic_init_u64(&pool->bp_evictions, 0);

	/*
	 * Set up the creating backend's local state.  This backend already has
	 * the DSM mapped from dsm_create(), so we cache the resolved pointers
	 * directly rather than going through dsm_attach().
	 */
	local = &PoolLocalStates[slot];
	local->seg = seg;
	local->descriptors = descs;
	local->blocks = blocks;
	local->io_cvs = io_cvs;
	local->hash_entries = hash_entries;
	local->mapping_locks = mapping_locks;
	local->ckpt_ids = ckpt_ids;
	local->strategy_data = strategy_data;
	local->attached = true;

	/* Mark active last (acts as a memory barrier for readers) */
	pg_write_barrier();
	pool->bp_active = true;

	/* Update high-water mark so lookup loops cover this slot */
	if (slot + 1 > NBufferPools)
		NBufferPools = slot + 1;

	/* Update MaxBufferNumber so BufferIsValid() allows these buffer IDs */
	{
		int			pool_max = pool->bp_first_buf + pool->bp_nbuffers;

		if (pool_max > MaxBufferNumber)
			MaxBufferNumber = pool_max;
	}

	elog(LOG, "created dynamic buffer pool \"%s\" with %d buffers (buf_id %d..%d)",
		 name, nbuffers, pool->bp_first_buf,
		 pool->bp_first_buf + nbuffers - 1);

	/* Register a per-pool trickle writer background worker */
	RegisterPoolTrickleWriter(pool, slot);

	/* Rebuild cross-pool hash partition map */
	RebuildPoolPartitions();

	return pool;
}

/*
 * DestroyDynamicBufferPool
 *		Destroy a dynamic buffer pool and release its DSM segment.
 *
 * All buffers in the pool must be unpinned before calling this.
 * Dirty buffers are flushed before destruction.
 */
void
DestroyDynamicBufferPool(BufferPoolDesc *pool)
{
	int			slot;
	PoolLocalState *local;

	Assert(pool != NULL);
	Assert(pool->bp_active);
	Assert(PoolIsDynamic(pool));

	slot = pool - BufferPoolDescs;
	local = &PoolLocalStates[slot];

	elog(LOG, "destroying dynamic buffer pool \"%s\"", NameStr(pool->bp_name));

	/* Check that no buffers are still pinned before proceeding */
	{
		PoolLocalState *plocal = EnsurePoolAttached(pool);

		for (int i = 0; i < pool->bp_nbuffers; i++)
		{
			BufferDesc *bufHdr = &plocal->descriptors[i].bufferdesc;
			uint64		buf_state = pg_atomic_read_u64(&bufHdr->state);

			if (BUF_STATE_GET_REFCOUNT(buf_state) != 0)
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_IN_USE),
						 errmsg("cannot destroy buffer pool \"%s\": buffer %d is still pinned (refcount %u)",
								NameStr(pool->bp_name),
								bufHdr->buf_id,
								BUF_STATE_GET_REFCOUNT(buf_state))));
		}
	}

	/* Flush all dirty buffers before tearing down the pool */
	FlushBufferPoolDirtyBuffers(pool);

	/*
	 * Injection point: lets a TAP test pause a backend here -- after the
	 * pinned-buffer check but before quiescence -- to drive a concurrent
	 * BufferAllocInPool against the pool and prove the barrier prevents a
	 * use-after-detach.
	 */
	INJECTION_POINT("bufpool-destroy-before-quiesce", NULL);

	/*
	 * Mark inactive with a memory barrier so the trickle writer (and any
	 * other code checking bp_active) sees the pool is going away before we
	 * clear bp_routine and other fields.  This prevents a race where the
	 * trickle writer passes the bp_active check but then dereferences a NULL
	 * bp_routine.
	 */
	pool->bp_active = false;
	pg_write_barrier();

	/*
	 * Quiesce all backends with respect to this pool before tearing down its
	 * DSM.  Emit a ProcSignalBarrier and wait for every backend to process
	 * it; each backend's handler (ProcessBarrierBufferPoolDetach) drops its
	 * CurrentBufferPool pointer and detaches from any now-inactive pool's DSM
	 * mapping.  When WaitForProcSignalBarrier returns, no other backend holds
	 * a live reference into this pool's DSM or can be mid-get_victim against
	 * its strategy_data, so the dsm_detach/dsm_unpin_segment below cannot
	 * race a concurrent BufferAllocInPool.  This is the same quiescence
	 * pattern used by the online data-checksum enable/disable barriers.
	 */
	{
		uint64		generation;

		generation = EmitProcSignalBarrier(PROCSIGNAL_BARRIER_BUFPOOL_DETACH);
		WaitForProcSignalBarrier(generation);
	}

	/* Terminate the trickle writer and wait for it to exit */
	TerminatePoolTrickleWriter(pool);

	/* Call algorithm shutdown if provided */
	if (pool->bp_routine->shutdown && local->attached && local->strategy_data)
		pool->bp_routine->shutdown(local->strategy_data);

	/* Release the pool's backing memory. */
	if (pool->bp_resv_backed)
	{
		/*
		 * Reservation-backed: decommit the sub-range (reclaims physical
		 * memory and makes stale accesses fault) and return it to the
		 * reservation allocator.  The barrier above guarantees no backend is
		 * still touching it.
		 */
		BufPoolDecommit(pool->bp_resv_offset, pool->bp_resv_size);
		BufPoolReserveFree(pool->bp_resv_offset);
	}
	else
	{
		/* DSM-backed (fallback path): unpin and detach the DSM segment. */
		dsm_unpin_segment(pool->bp_dsm_handle);
		if (local->seg)
		{
			dsm_detach(local->seg);
			local->seg = NULL;
		}
	}

	/* Clear the creating backend's local state */
	local->descriptors = NULL;
	local->blocks = NULL;
	local->io_cvs = NULL;
	local->hash_entries = NULL;
	local->mapping_locks = NULL;
	local->ckpt_ids = NULL;
	local->strategy_data = NULL;
	local->attached = false;

	/* Clear the descriptor */
	pool->bp_dsm_handle = InvalidDsmHandle;
	pool->bp_resv_backed = false;
	pool->bp_resv_offset = 0;
	pool->bp_resv_size = 0;
	pool->bp_resv_huge = false;
	pool->bp_trickle_slot = -1;
	pool->bp_routine = NULL;
	pool->bp_oid = InvalidOid;

	/*
	 * NBufferPools is a high-water mark, not a live count.  We do NOT
	 * decrement it here because other backends (e.g. trickle writers) use it
	 * as the upper bound when scanning BufferPoolDescs.  Decrementing could
	 * cause them to miss active pools at higher slot indices. Inactive slots
	 * are simply skipped via bp_active checks.
	 */

	/* Recompute MaxBufferNumber across remaining active pools */
	{
		int			new_max = NBuffers;

		for (int i = 1; i < MAX_BUFFER_POOLS; i++)
		{
			BufferPoolDesc *p = &BufferPoolDescs[i];
			int			pool_max;

			if (!p->bp_active)
				continue;
			pool_max = p->bp_first_buf + p->bp_nbuffers;
			if (pool_max > new_max)
				new_max = pool_max;
		}
		MaxBufferNumber = new_max;
	}

	/* Rebuild cross-pool hash partition map */
	RebuildPoolPartitions();
}

/*
 * ResizeDynamicBufferPool
 *		Resize a dynamic buffer pool by destroying and recreating it.
 *
 * DSM has no resize API, so this is implemented as an atomic
 * destroy-and-recreate.  The pool's cached pages are evicted (they're
 * a cache -- data is safe on disk).  The pool keeps its OID, name,
 * and handler.
 *
 * The caller must ensure no buffers in the pool are pinned.
 * Returns the new pool descriptor (may be at the same slot).
 */
BufferPoolDesc *
ResizeDynamicBufferPool(BufferPoolDesc *pool, int new_nbuffers)
{
	Oid			bp_oid;
	Oid			handler_oid;
	NameData	pool_name;
	bool		save_huge;
	const BufferPoolRoutine *routine;
	Datum		datum;

	Assert(pool != NULL);
	Assert(pool->bp_active);
	Assert(PoolIsDynamic(pool));
	Assert(new_nbuffers >= 16);

	/* Save pool identity before destruction */
	bp_oid = pool->bp_oid;
	handler_oid = pool->bp_handler_oid;
	save_huge = pool->bp_resv_huge;
	namestrcpy(&pool_name, NameStr(pool->bp_name));

	/* Resolve the handler to get the routine vtable */
	if (OidIsValid(handler_oid))
	{
		datum = OidFunctionCall0(handler_oid);
		routine = (const BufferPoolRoutine *) DatumGetPointer(datum);
	}
	else
		routine = pool->bp_routine;

	elog(LOG, "resizing buffer pool \"%s\" from %d to %d buffers",
		 NameStr(pool_name), pool->bp_nbuffers, new_nbuffers);

	/* Destroy: flushes dirty buffers, terminates trickle writer, frees DSM */
	DestroyDynamicBufferPool(pool);

	/* Recreate with new size, preserving the huge-pages choice. */
	pool = CreateDynamicBufferPool(bp_oid, NameStr(pool_name), new_nbuffers,
								   routine, handler_oid, save_huge);

	return pool;
}

/*
 * SwapDynamicBufferPoolAlgorithm
 *		Change a dynamic pool's replacement algorithm at runtime.
 *
 * Implemented as a barrier-quiesced destroy-and-recreate at the same size
 * with the new handler's routine.  This is the safe form of the runtime
 * algorithm swap that an earlier draft of the series attempted with an
 * unquiesced in-place memset of the strategy region (which could corrupt a
 * concurrent get_victim).  DestroyDynamicBufferPool emits
 * PROCSIGNAL_BARRIER_BUFPOOL_DETACH and waits for every backend to drop the
 * pool before the old strategy state is freed, so no backend is mid-access
 * against it when the new algorithm initializes fresh state.
 *
 * The pool keeps its OID, name, size, and huge-pages choice; its cached
 * pages are dropped (a cache -- on-disk data is unaffected).  The caller must
 * ensure no buffers in the pool are pinned.
 */
BufferPoolDesc *
SwapDynamicBufferPoolAlgorithm(BufferPoolDesc *pool, Oid new_handler_oid)
{
	Oid			bp_oid;
	NameData	pool_name;
	int			nbuffers;
	bool		save_huge;
	const BufferPoolRoutine *routine;
	Datum		datum;

	Assert(pool != NULL);
	Assert(pool->bp_active);
	Assert(PoolIsDynamic(pool));
	Assert(OidIsValid(new_handler_oid));

	bp_oid = pool->bp_oid;
	nbuffers = pool->bp_nbuffers;
	save_huge = pool->bp_resv_huge;
	namestrcpy(&pool_name, NameStr(pool->bp_name));

	/* Resolve the new handler to its routine vtable. */
	datum = OidFunctionCall0(new_handler_oid);
	routine = (const BufferPoolRoutine *) DatumGetPointer(datum);

	elog(LOG, "changing algorithm of buffer pool \"%s\" (%d buffers)",
		 NameStr(pool_name), nbuffers);

	/* Destroy: barrier-quiesce, flush, terminate trickle writer, free memory. */
	DestroyDynamicBufferPool(pool);

	/* Recreate at the same size under the new algorithm. */
	pool = CreateDynamicBufferPool(bp_oid, NameStr(pool_name), nbuffers,
								   routine, new_handler_oid, save_huge);

	return pool;
}

/*
 * BufferPoolResizeShared
 *		Online resize of the DEFAULT pool (shared_buffers) -- NOT YET REAL.
 *
 * This is the headline D5 feature ("shared_buffers without a restart").  The
 * same-address reservation primitives it must be built on ALREADY EXIST and
 * are exercised for dynamic pools:
 *
 *	 BufPoolReserveInit()   reserve address space at postmaster start
 *	 BufPoolReserveAlloc()  carve a contiguous same-address window
 *	 BufPoolCommit()        back a window with real (optionally huge) pages
 *	 BufPoolDecommit()      release physical memory, fault on stale access
 *	 BufPoolAttachLocal()   re-map a committed window in a pre-existing backend
 *
 * The reason this is a stub and not a call to those primitives is that the
 * DEFAULT pool (slot 0) does NOT live in the reservation today: it still owns
 * the classic main-shmem BufferBlocks/BufferDescriptors arrays sized once at
 * boot from shared_buffers (PGC_POSTMASTER).  Making it online-resizable is
 * the multi-month, uncommitted-upstream effort (Vondra/Andres) the hardening
 * plan calls research-grade; the genuinely hard, still-missing pieces are:
 *
 *   1. Relocate pool 0's descriptors/blocks/hash into a reservation window so
 *		its memory can grow/shrink (today they are fixed main-shmem arrays).
 *   2. A ProcSignalBarrier phase protocol so every backend quiesces, re-maps
 *		the grown/shrunk window (BufPoolAttachLocal), and agrees on the new
 *		NBuffers/MaxBufferNumber before any I/O resumes -- shrink additionally
 *		must evict + flush the buffers being removed with nothing pinned.
 *   3. Making shared_buffers something other than PGC_POSTMASTER, with a
 *		check/assign hook that drives this path instead of demanding a restart.
 *
 * Until those land, calling this is an error rather than a silent no-op or a
 * fabricated success: shared_buffers still requires a restart to change.
 *
 * ponytail: honest stub. The reservation/NUMA/huge-page primitives it needs
 * are done (bufpool_reserve.c, bufpool_numa.c); what remains is moving pool 0
 * into the reservation + the cross-backend barrier resize protocol above.
 * Wire this to BufPoolReserveAlloc/Commit/AttachLocal once pool 0 is
 * reservation-backed.
 */
void
BufferPoolResizeShared(int new_nbuffers)
{
	(void) new_nbuffers;

	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("online resize of shared_buffers is not supported"),
			 errdetail("The default buffer pool is sized at server start; changing shared_buffers requires a restart."),
			 errhint("The same-address reservation this feature is built on is active when max_buffer_pool_memory is set; only the default pool is not yet reservation-backed.")));
}

/*
 * TrickleWriterMain -- background worker entry point for a per-pool
 * trickle writer.
 *
 * main_arg is the slot index in BufferPoolDescs.
 */
void
TrickleWriterMain(Datum main_arg)
{
	int			pool_slot = DatumGetInt32(main_arg);
	BufferPoolDesc *pool;
	const BufferPoolRoutine *routine;
	PoolLocalState *local;
	WritebackContext wb_context;

	Assert(pool_slot >= 0 && pool_slot < MAX_BUFFER_POOLS);
	pool = &BufferPoolDescs[pool_slot];

	if (!pool->bp_active)
	{
		elog(LOG, "trickle writer for pool slot %d: pool no longer active", pool_slot);
		proc_exit(0);
	}

	/*
	 * Set up signal handlers.  SIGHUP triggers config reload, SIGTERM
	 * triggers graceful shutdown.
	 */
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	BackgroundWorkerUnblockSignals();

	/*
	 * Create a resource owner for buffer pin management.  Dynamic background
	 * workers don't go through AuxiliaryProcessMainCommon, so we need to
	 * create one ourselves.
	 */
	CreateAuxProcessResourceOwner();

	/*
	 * Resolve the pool's algorithm routine in this process.
	 *
	 * For extension-provided handlers (bp_handler_library is set), we must
	 * load the extension library in this process and call the handler to get
	 * a valid local pointer.  The shared-memory bp_routine pointer was set by
	 * the backend that created the pool and points into that backend's .so
	 * mapping, which is not loaded in the trickle writer (forked from the
	 * postmaster).
	 *
	 * For built-in algorithms (bp_handler_library is empty), bp_routine
	 * points into the postgres binary text segment and is valid in all
	 * processes, so we can use it directly.
	 */
	if (pool->bp_handler_library[0] != '\0' &&
		pool->bp_handler_function[0] != '\0')
	{
		PGFunction	handler_fn;
		Datum		result;
		FunctionCallInfoBaseData fcinfo;

		handler_fn = (PGFunction) load_external_function(
														 pool->bp_handler_library,
														 pool->bp_handler_function,
														 true, NULL);

		if (handler_fn == NULL)
		{
			elog(LOG, "trickle writer: could not load handler for pool \"%s\"",
				 NameStr(pool->bp_name));
			proc_exit(1);
		}

		InitFunctionCallInfoData(fcinfo, NULL, 0, InvalidOid, NULL, NULL);
		result = handler_fn(&fcinfo);
		routine = (const BufferPoolRoutine *) DatumGetPointer(result);
	}
	else
	{
		routine = pool->bp_routine;
	}

	/*
	 * Re-check bp_active before attaching.  The pool could have been
	 * destroyed during our startup (signal handler setup, library loading,
	 * resource owner creation).  This race is expected: the pool destroyer
	 * sets bp_active = false then terminates us via SIGTERM, but we may not
	 * have processed that signal yet.
	 */
	if (!pool->bp_active)
	{
		elog(LOG, "trickle writer for pool slot %d: pool deactivated during startup",
			 pool_slot);
		proc_exit(0);
	}


	/*
	 * Attach to the pool's storage.  Dynamic pools live in a DSM segment that
	 * must be mapped in this process; the default pool (slot 0) uses the
	 * global buffer arrays, whose PoolLocalStates[0] entry was populated at
	 * shmem init and inherited across the fork, so it needs no attach.
	 */
	if (PoolIsDynamic(pool))
		local = EnsurePoolAttached(pool);
	else
		local = &PoolLocalStates[0];

	elog(LOG, "trickle writer started for buffer pool \"%s\"", NameStr(pool->bp_name));

	WritebackContextInit(&wb_context, &trickle_flush_after);

	/*
	 * Main loop: scan pool's buffers for dirty pages and write them out.
	 */
	while (!ShutdownRequestPending)
	{
		int			num_written = 0;
		bool		hibernate = true;

		ResetLatch(MyLatch);

		/*
		 * Handle config reload, shutdown, AND ProcSignalBarriers.  The last
		 * is essential: DROP/RESIZE of a pool emits
		 * PROCSIGNAL_BARRIER_BUFPOOL_DETACH and waits for every process --
		 * including this trickle writer -- to absorb it before tearing down
		 * the pool's memory.  Absorbing it here (rather than only the old
		 * ConfigReload/Shutdown checks) prevents the destroyer's
		 * WaitForProcSignalBarrier from stalling on us.
		 */
		ProcessMainLoopInterrupts();

		/*
		 * Once the pool is marked inactive (by DestroyDynamicBufferPool,
		 * before it emits the detach barrier), stop touching the pool's
		 * memory but do NOT exit on our own: exiting here races the
		 * destroyer's
		 * TerminateBackgroundWorker/WaitForBackgroundWorkerShutdown using a
		 * reconstructed handle.  Idle until we receive SIGTERM from the
		 * destroyer (ShutdownRequestPending, handled by
		 * ProcessMainLoopInterrupts above), which is the single, well-defined
		 * exit path.
		 */
		if (!pool->bp_active)
		{
			(void) WaitLatch(MyLatch,
							 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
							 1000L, WAIT_EVENT_BGWRITER_HIBERNATE);
			continue;
		}

		/*
		 * Use the algorithm's trickle iterator if available.  This lets the
		 * replacement algorithm direct us to the best flush candidates (e.g.,
		 * LRU tail for ARC, cold pages for CAR, HIR entries for LIRS) rather
		 * than doing a blind linear scan.
		 *
		 * Note: we use the local 'routine' pointer resolved at startup, not
		 * pool->bp_routine from shared memory, since the latter may point to
		 * an extension .so address valid only in the creating backend's
		 * address space.
		 */
		if (routine->trickle_iter_begin != NULL)
		{
			void	   *iter;
			int			buf_id;
			int			batch_limit = trickle_write_batch_size;

			iter = routine->trickle_iter_begin(
											   local->strategy_data, batch_limit);

			while ((buf_id = routine->trickle_iter_next(
														local->strategy_data, iter)) >= 0)
			{
				SyncOneBuffer(buf_id, true, &wb_context);
				num_written++;
				hibernate = false;

				if (num_written >= batch_limit)
					break;
			}

			routine->trickle_iter_end(
									  local->strategy_data, iter);
		}
		else
		{
			/* Fallback: linear scan of pool's buffer descriptors */
			int			batch_limit = trickle_write_batch_size;

			for (int i = 0; i < pool->bp_nbuffers; i++)
			{
				BufferDesc *bufHdr = &local->descriptors[i].bufferdesc;
				uint64		buf_state;

				buf_state = pg_atomic_read_u64(&bufHdr->state);

				/* Skip buffers that are not valid or not dirty */
				if (!(buf_state & BM_VALID) || !(buf_state & BM_DIRTY))
					continue;

				/* Skip buffers that are in use (pinned) */
				if (BUF_STATE_GET_REFCOUNT(buf_state) > 0)
					continue;

				SyncOneBuffer(bufHdr->buf_id, true, &wb_context);
				num_written++;
				hibernate = false;

				if (num_written >= batch_limit)
					break;
			}
		}

		IssuePendingWritebacks(&wb_context, IOCONTEXT_NORMAL);

		/*
		 * Oversubscription nudging: if this pool's current buffer count
		 * exceeds its target, try to evict excess buffers and return them to
		 * the unclaimed pool.  We evict up to 10% of the excess per cycle to
		 * avoid bursts.
		 */
		if (pool->bp_oversubscribed)
		{
			int			excess = pool->bp_current_buffers - pool->bp_target_buffers;

			if (excess > 0)
			{
				int			evict_per_cycle = Max(1, excess / 10);
				int			evicted = 0;

				for (int i = pool->bp_nbuffers - 1;
					 i >= 0 && evicted < evict_per_cycle;
					 i--)
				{
					BufferDesc *bufHdr = &local->descriptors[i].bufferdesc;
					uint64		state = pg_atomic_read_u64(&bufHdr->state);

					/* Skip pinned or locked buffers */
					if (BUF_STATE_GET_REFCOUNT(state) > 0 ||
						(state & BM_LOCKED))
						continue;

					/* Write out dirty buffers first */
					if ((state & BM_VALID) && (state & BM_DIRTY))
					{
						SyncOneBuffer(bufHdr->buf_id, true, &wb_context);
						hibernate = false;
					}

					evicted++;
				}

				IssuePendingWritebacks(&wb_context, IOCONTEXT_NORMAL);

				/*
				 * Adjust current count by the number we attempted to release.
				 * In practice, some may still be pinned, so this is a
				 * best-effort approach.
				 */
				pool->bp_current_buffers -= evicted;
				pg_atomic_fetch_add_u32(UnclaimedBufferCount, evicted);

				if (pool->bp_current_buffers <= pool->bp_target_buffers)
					pool->bp_oversubscribed = false;
			}
			else
				pool->bp_oversubscribed = false;
		}

		/*
		 * After any checkpoint, free all smgr objects.  Like the former
		 * background writer, a trickle writer does not process shared
		 * invalidation messages or call AtEOXact_SMgr(), so without this it
		 * would keep smgr entries for dropped relations forever.
		 */
		if (FirstCallSinceLastCheckpoint())
			smgrdestroyall();

		/* Sleep longer if no work was done */
		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 hibernate ? 5000L : 200L,
						 WAIT_EVENT_BGWRITER_MAIN);
	}

	elog(LOG, "trickle writer shutting down for buffer pool \"%s\"",
		 NameStr(pool->bp_name));

	proc_exit(0);
}

/*
 * RegisterDefaultPoolTrickleWriter -- register the trickle writer for the
 * default buffer pool (slot 0).
 *
 * Called once, early, by the startup process before WAL replay so the writer
 * launches at PM_STARTUP and can flush buffers dirtied during recovery.  The
 * default pool's shared state (PoolLocalStates[0]) was established at buffer-
 * pool shmem init at postmaster start, well before this runs.
 */
void
RegisterDefaultPoolTrickleWriter(void)
{
	BufferPoolDesc *defpool = &BufferPoolDescs[0];

	/* Only register once; guard against repeated calls. */
	if (defpool->bp_trickle_slot >= 0)
		return;

	/*
	 * Dynamic background workers can only be launched under the postmaster.
	 * In single-user mode there is no writer; the single backend flushes its
	 * own dirty victims, which is correct (just unassisted).
	 */
	if (!IsUnderPostmaster)
		return;

	RegisterPoolTrickleWriter(defpool, 0);
}

/*
 * RegisterPoolTrickleWriter -- register a background worker to serve as
 * the trickle writer for a buffer pool.
 *
 * The DEFAULT pool (slot 0) is the primary dirty-buffer writeback path (there
 * is no global background writer anymore).  It must run DURING recovery, since
 * the startup process dirties buffers while replaying WAL, so it starts at
 * BgWorkerStart_PostmasterStart (launched at PM_STARTUP, before recovery
 * reaches a consistent state) and auto-restarts, rather than the
 * BgWorkerStart_RecoveryFinished / one-shot policy used for dynamic pools.
 *
 * Recovery correctness never depends on this worker: any process that must
 * evict a dirty victim writes it out inline in GetVictimBuffer(), and the
 * end-of-recovery checkpoint flushes the rest.  The trickle writer only
 * offloads that work from the startup process.
 */
void
RegisterPoolTrickleWriter(BufferPoolDesc *pool, int slot)
{
	BackgroundWorker bgw;
	BackgroundWorkerHandle *handle;
	bool		is_default = (slot == 0);

	memset(&bgw, 0, sizeof(bgw));
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS;
	bgw.bgw_start_time = is_default ? BgWorkerStart_PostmasterStart
		: BgWorkerStart_RecoveryFinished;
	snprintf(bgw.bgw_library_name, MAXPGPATH, "postgres");
	snprintf(bgw.bgw_function_name, BGW_MAXLEN, "TrickleWriterMain");
	snprintf(bgw.bgw_name, BGW_MAXLEN, "trickle writer for pool %s",
			 NameStr(pool->bp_name));
	snprintf(bgw.bgw_type, BGW_MAXLEN, "buffer pool trickle writer");
	/*
	 * The default pool's writer is the standing writeback path: restart it
	 * promptly if it dies.  Dynamic-pool writers are one-shot (drop/resize
	 * re-registers).
	 */
	bgw.bgw_restart_time = is_default ? 5 : BGW_NEVER_RESTART;
	bgw.bgw_notify_pid = MyProcPid;
	bgw.bgw_main_arg = Int32GetDatum(slot);

	if (!RegisterDynamicBackgroundWorker(&bgw, &handle))
	{
		ereport(WARNING,
				(errmsg("could not register trickle writer for buffer pool \"%s\"",
						NameStr(pool->bp_name)),
				 errhint("Consider increasing max_worker_processes.")));
		return;
	}

	/* Store handle fields in shared memory so any backend can terminate it */
	pool->bp_trickle_slot = GetBackgroundWorkerHandleSlot(handle);
	pool->bp_trickle_generation = GetBackgroundWorkerHandleGeneration(handle);
	pfree(handle);
}

/*
 * TerminatePoolTrickleWriter -- terminate the trickle writer for a pool.
 */
void
TerminatePoolTrickleWriter(BufferPoolDesc *pool)
{
	BackgroundWorkerHandle *handle;
	pid_t		pid;

	if (pool->bp_trickle_slot < 0)
		return;

	/* Reconstruct a handle from the shared memory fields */
	handle = CreateBackgroundWorkerHandle(pool->bp_trickle_slot,
										  pool->bp_trickle_generation);

	TerminateBackgroundWorker(handle);

	/*
	 * Poll for shutdown rather than WaitForBackgroundWorkerShutdown(): the
	 * latter blocks on a notify latch delivered to the worker's registered
	 * bgw_notify_pid, which is the backend that CREATED the pool, not
	 * necessarily the one running DROP/RESIZE.  A reconstructed handle in a
	 * different backend would wait forever.  GetBackgroundWorkerPid() reads
	 * the slot state directly and needs no notify registration.
	 */
	for (;;)
	{
		BgwHandleStatus status = GetBackgroundWorkerPid(handle, &pid);

		if (status == BGWH_STOPPED)
			break;

		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 10L, WAIT_EVENT_BGWORKER_SHUTDOWN);
		ResetLatch(MyLatch);
		CHECK_FOR_INTERRUPTS();
	}

	pfree(handle);
	pool->bp_trickle_slot = -1;
}


/* ----------------------------------------------------------------
 *		VACUUM hint dispatch for pools
 * ----------------------------------------------------------------
 */

/*
 * PoolHintVacuum -- hint that VACUUM is starting or ending for a pool.
 *
 * If pool_oid is InvalidOid, dispatches to StrategyHintVacuum() for the
 * default pool.  Otherwise, resolves the pool by OID and calls its
 * hint_vacuum callback.
 */
void
PoolHintVacuum(Oid pool_oid, bool vacuum_active)
{
	BufferPoolDesc *pool;
	PoolLocalState *local;

	if (!OidIsValid(pool_oid))
	{
		StrategyHintVacuum(vacuum_active);
		return;
	}

	pool = GetBufferPoolByOid(pool_oid);
	if (pool == NULL)
	{
		/* Pool doesn't exist (yet); fall back to default */
		StrategyHintVacuum(vacuum_active);
		return;
	}

	if (!pool->bp_routine || !pool->bp_routine->hint_vacuum)
		return;

	if (PoolIsDynamic(pool))
	{
		local = EnsurePoolAttached(pool);
		pool->bp_routine->hint_vacuum(local->strategy_data, vacuum_active);
	}
	else
	{
		/* Default pool uses ActivePoolData */
		StrategyHintVacuum(vacuum_active);
	}
}


/* ----------------------------------------------------------------
 *		Weighted-range hash partition infrastructure
 *
 * Maps the full uint64 hash space proportionally across active pools
 * based on their buffer counts.  Used for proportional dispatch of
 * operations; currently advisory -- actual routing uses relation-level
 * rd_bufpool assignment.
 * ----------------------------------------------------------------
 */

/*
 * Backend-local partition map, rebuilt on pool create/destroy.
 */
static PoolHashPartitions LocalPartitions = {NULL, 0, 0};

/*
 * ComputeCrossPoolPartitions -- build a weighted hash partition map
 * from all active pools, proportional to their buffer counts.
 */
void
ComputeCrossPoolPartitions(PoolHashPartitions *parts)
{
	int			total_buffers = 0;
	int			active_count = 0;
	uint64		next_lower_bound = 0;
	int			i;

	/* Count total buffers across active pools */
	for (i = 0; i < NBufferPools; i++)
	{
		if (BufferPoolDescs[i].bp_active)
		{
			total_buffers += BufferPoolDescs[i].bp_nbuffers;
			active_count++;
		}
	}

	if (active_count == 0 || total_buffers == 0)
	{
		parts->count = 0;
		return;
	}

	/* Ensure capacity */
	if (parts->capacity < active_count)
	{
		if (parts->entries)
			pfree(parts->entries);
		parts->entries = (PoolHashPartition *)
			MemoryContextAlloc(TopMemoryContext,
							   active_count * sizeof(PoolHashPartition));
		parts->capacity = active_count;
	}

	parts->count = 0;

	for (i = 0; i < NBufferPools; i++)
	{
		BufferPoolDesc *pool = &BufferPoolDescs[i];
		PoolHashPartition *entry;
		uint64		interval;

		if (!pool->bp_active)
			continue;

		entry = &parts->entries[parts->count];
		entry->pool_slot = i;
		entry->lower_bound = next_lower_bound;

		/* Proportional interval: (buffers / total) * UINT64_MAX */
		interval = (uint64) ((double) UINT64_MAX *
							 ((double) pool->bp_nbuffers / (double) total_buffers));

		entry->interval_size = interval;
		next_lower_bound += interval;
		parts->count++;
	}

	/* Adjust last entry to cover any remainder from rounding */
	if (parts->count > 0)
	{
		uint64		sum = 0;

		for (i = 0; i < parts->count; i++)
			sum += parts->entries[i].interval_size;

		parts->entries[parts->count - 1].interval_size += (UINT64_MAX - sum);
	}
}

/*
 * GetPoolSlotForHash -- binary search to find which pool slot owns a hash.
 *
 * Returns the pool_slot index into BufferPoolDescs, or -1 if the
 * partition map is empty.
 */
int
GetPoolSlotForHash(PoolHashPartitions *parts, uint64 hash)
{
	int			left,
				right,
				mid;

	if (parts == NULL || parts->count == 0)
		return -1;

	/* Fast path: hash falls in last partition */
	if (hash >= parts->entries[parts->count - 1].lower_bound)
		return parts->entries[parts->count - 1].pool_slot;

	/* Binary search for the partition containing this hash */
	left = 0;
	right = parts->count - 1;

	while (left <= right)
	{
		mid = left + (right - left) / 2;

		if (hash >= parts->entries[mid].lower_bound)
		{
			if (mid == parts->count - 1 ||
				hash < parts->entries[mid + 1].lower_bound)
				return parts->entries[mid].pool_slot;
			left = mid + 1;
		}
		else
		{
			if (mid == 0)
				break;
			right = mid - 1;
		}
	}

	return -1;
}

/*
 * RebuildPoolPartitions -- rebuild the backend-local partition map.
 *
 * Called from CreateDynamicBufferPool/DestroyDynamicBufferPool to keep
 * the partition map current.
 */
void
RebuildPoolPartitions(void)
{
	ComputeCrossPoolPartitions(&LocalPartitions);
}


/* ----------------------------------------------------------------
 *		Statistics view support
 * ----------------------------------------------------------------
 */

/*
 * pg_stat_get_bufferpool -- return per-pool statistics as a SRF.
 *
 * Returns one row per active buffer pool with columns:
 *   name, oid, nbuffers, target_buffers, current_buffers,
 *   oversubscribed, reads, hits, evictions
 */
#define PG_STAT_GET_BUFFERPOOL_COLS 9

/*
 * pg_stat_get_bufferpool is a built-in function (listed in pg_proc.dat), so
 * its prototype comes from the generated fmgrprotos.h and it is dispatched
 * via fmgrtab.  Built-ins must NOT use PG_FUNCTION_INFO_V1 (that macro is for
 * dynamically-loaded/extension functions); doing so emits a conflicting
 * declaration that MSVC rejects as "redefinition; different linkage".
 */
Datum
pg_stat_get_bufferpool(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	InitMaterializedSRF(fcinfo, 0);

	for (int i = 0; i < NBufferPools; i++)
	{
		Datum		values[PG_STAT_GET_BUFFERPOOL_COLS] = {0};
		bool		nulls[PG_STAT_GET_BUFFERPOOL_COLS] = {0};
		BufferPoolDesc *pool = &BufferPoolDescs[i];

		if (!pool->bp_active)
			continue;

		values[0] = NameGetDatum(&pool->bp_name);

		if (OidIsValid(pool->bp_oid))
			values[1] = ObjectIdGetDatum(pool->bp_oid);
		else
			nulls[1] = true;

		values[2] = Int32GetDatum(pool->bp_nbuffers);
		values[3] = Int32GetDatum(pool->bp_target_buffers);
		values[4] = Int32GetDatum(pool->bp_current_buffers);
		values[5] = BoolGetDatum(pool->bp_oversubscribed);
		values[6] = Int64GetDatum(pg_atomic_read_u64(&pool->bp_reads));
		values[7] = Int64GetDatum(pg_atomic_read_u64(&pool->bp_hits));
		values[8] = Int64GetDatum(pg_atomic_read_u64(&pool->bp_evictions));

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
							 values, nulls);
	}

	return (Datum) 0;
}

/*
 * pg_stat_get_bufferpool_numa -- per-(node,stripe) clock-sweep state SRF.
 *
 * Surfaces the NUMA-partitioned / striped-cooling default-pool sweep state
 * that pg_stat_get_bufferpool does not: one row per NUMA node (and per stripe
 * when cooling is on) with the buffers owned by that range, the clock-hand
 * position, and completed passes.  With NUMA off it returns a single node=0,
 * stripe=0 row describing the plain global clock sweep, so the companion view
 * is always non-empty and default behavior is unchanged.
 *
 * Read-only and cheap (atomic reads + one spinlock snapshot).  Built-in like
 * pg_stat_get_bufferpool -- no PG_FUNCTION_INFO_V1.
 */
#define PG_STAT_GET_BUFFERPOOL_NUMA_COLS 5

Datum
pg_stat_get_bufferpool_numa(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	int			maxrows;
	BufPoolNumaStat *rows;
	int			nrows;

	InitMaterializedSRF(fcinfo, 0);

	maxrows = BufPoolNumaClockStatsMax();
	rows = (BufPoolNumaStat *) palloc(sizeof(BufPoolNumaStat) * maxrows);
	nrows = BufPoolNumaClockStats(rows, maxrows);

	for (int i = 0; i < nrows; i++)
	{
		Datum		values[PG_STAT_GET_BUFFERPOOL_NUMA_COLS] = {0};
		bool		nulls[PG_STAT_GET_BUFFERPOOL_NUMA_COLS] = {0};

		values[0] = Int32GetDatum(rows[i].node);
		values[1] = Int32GetDatum(rows[i].stripe);
		values[2] = Int32GetDatum(rows[i].nbuffers);
		values[3] = Int64GetDatum((int64) rows[i].clock_hand);
		values[4] = Int64GetDatum((int64) rows[i].complete_passes);

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
							 values, nulls);
	}

	pfree(rows);
	return (Datum) 0;
}
