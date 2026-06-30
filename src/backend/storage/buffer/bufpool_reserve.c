/*-------------------------------------------------------------------------
 *
 * bufpool_reserve.c
 *	  Address-space reservation for same-address buffer pools.
 *
 * To make every buffer pool's memory appear at the SAME virtual address in
 * every backend -- the property the main shared-memory segment has and that
 * raw cross-backend pointers depend on -- we reserve one large contiguous
 * region of address space in the postmaster, before any backend is forked,
 * and carve pools out of it as committed sub-ranges.
 *
 * Mechanism (Linux):
 *
 *	1. BufPoolReserveInit() (postmaster, pre-fork): create an anonymous
 *	   shared backing object with memfd_create(), size it to
 *	   max_buffer_pool_memory, and mmap() the whole thing PROT_NONE,
 *	   MAP_SHARED|MAP_NORESERVE.  PROT_NONE + MAP_NORESERVE means the address
 *	   space is owned but no physical memory is charged until committed.
 *	   Because this mapping exists before fork(), every backend inherits it at
 *	   the identical address for free; the backing memfd is inherited too, so
 *	   MAP_FIXED commits done by any backend map the same underlying pages in
 *	   all of them.
 *
 *	2. BufPoolCommit(offset, size): mmap() the sub-range PROT_READ|PROT_WRITE,
 *	   MAP_SHARED|MAP_FIXED over the reservation we already own, backed by the
 *	   same memfd at the same offset.  Safe MAP_FIXED -- we own the range, so
 *	   nothing is clobbered.  The pages are now writable and shared.
 *
 *	3. BufPoolDecommit(offset, size): punch a hole in the memfd to reclaim
 *	   physical memory, then remap the sub-range PROT_NONE so a stale access
 *	   faults instead of silently reading another pool's data.
 *
 * Portability: memfd_create is Linux-specific.  On other platforms (or
 * EXEC_BACKEND, where fork inheritance is unavailable) the reservation is
 * disabled and pools fall back to per-DSM mappings; the code still builds and
 * runs, just without same-address pools or online pool resize.  See
 * BufPoolReserveActive().
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/storage/buffer/bufpool_reserve.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/mman.h>
#include <unistd.h>
#include <fcntl.h>
#ifdef __linux__
#include <linux/falloc.h>
#endif
#ifdef USE_LIBNUMA
#include <numa.h>
#include <numaif.h>
#endif

#include "miscadmin.h"
#include "port/pg_bitutils.h"
#include "port/pg_numa.h"
#include "storage/bufpool.h"
#include "storage/bufpool_internals.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/guc.h"

/*
 * Whether the platform supports the same-address reservation mechanism.
 * Requires memfd_create (Linux) and a non-EXEC_BACKEND build (fork
 * inheritance of the reservation mapping).
 */
#if defined(__linux__) && !defined(EXEC_BACKEND)
#define BUFPOOL_RESERVE_SUPPORTED 1
#endif

/* GUC: maximum total memory reservable across all buffer pools (in blocks) */
int			max_buffer_pool_memory = 0;

/* GUC: interleave pool memory across NUMA nodes on multi-node systems */
bool		buffer_pool_numa_interleave = false;

/*
 * Per-backend pointers to the reservation.  Inherited across fork(), so these
 * are valid in every backend at the same address without re-initialization.
 * resv_fd is the backing memfd; resv_base is the start of the PROT_NONE
 * reservation.
 */
static int	resv_fd = -1;
static char *resv_base = NULL;
static Size resv_size = 0;

/*
 * Sub-range allocator state lives in the main shared-memory segment so all
 * backends agree on which parts of the reservation are committed to which
 * pool.  A simple bump allocator with a free list of returned extents is
 * adequate: pools are created/destroyed rarely and the extent count is tiny
 * (<= MAX_BUFFER_POOLS).
 */
typedef struct BufPoolExtent
{
	Size		offset;			/* offset into the reservation */
	Size		size;			/* committed size */
	bool		in_use;
} BufPoolExtent;

typedef struct BufPoolReserveControl
{
	slock_t		mutex;			/* protects the allocator state */
	Size		total_size;		/* reservation size (bytes) */
	Size		bump;			/* next free offset for fresh allocations */
	int			nextents;
	BufPoolExtent extents[MAX_BUFFER_POOLS];
} BufPoolReserveControl;

static BufPoolReserveControl *ReserveCtl = NULL;

/*
 * BufPoolReserveShmemSize -- shared memory needed for the allocator control.
 */
Size
BufPoolReserveShmemSize(void)
{
	return sizeof(BufPoolReserveControl);
}

/*
 * BufPoolReserveActive -- is the same-address reservation in effect?
 *
 * When false, callers must fall back to the legacy per-pool DSM path.
 */
bool
BufPoolReserveActive(void)
{
	return resv_base != NULL;
}

/*
 * BufPoolReserveInit -- reserve the address range (postmaster, pre-fork).
 *
 * Called from CreateSharedMemoryAndSemaphores after the main segment is set
 * up but before any backend is forked.  Must run exactly once in the
 * postmaster.  On unsupported platforms this is a no-op and pools use the
 * legacy DSM path.
 */
void
BufPoolReserveInit(void)
{
	Assert(!IsUnderPostmaster);

	/* Allocate the shared allocator control in the main segment. */
	ReserveCtl = (BufPoolReserveControl *)
		ShmemAlloc(sizeof(BufPoolReserveControl));
	memset(ReserveCtl, 0, sizeof(BufPoolReserveControl));
	SpinLockInit(&ReserveCtl->mutex);
	ReserveCtl->nextents = 0;
	ReserveCtl->bump = 0;

	/* A zero budget disables the feature explicitly. */
	if (max_buffer_pool_memory <= 0)
	{
		ReserveCtl->total_size = 0;
		return;
	}

#ifdef BUFPOOL_RESERVE_SUPPORTED
	{
		Size		want = (Size) max_buffer_pool_memory * BLCKSZ;
		char	   *base;
		int			fd;

		/* Page-align the reservation up to the system page size. */
		{
			long		pgsz = sysconf(_SC_PAGESIZE);

			if (pgsz > 0 && (want % (Size) pgsz) != 0)
				want += (Size) pgsz - (want % (Size) pgsz);
		}

		fd = memfd_create("postgres_bufpool_reservation", MFD_CLOEXEC);
		if (fd < 0)
		{
			ereport(LOG,
					(errmsg("could not create buffer-pool reservation backing object: %m"),
					 errdetail("Same-address buffer pools are disabled; pools will use per-segment mappings.")));
			ReserveCtl->total_size = 0;
			return;
		}
		if (ftruncate(fd, want) != 0)
		{
			ereport(LOG,
					(errmsg("could not size buffer-pool reservation (%zu bytes): %m",
							want)));
			close(fd);
			ReserveCtl->total_size = 0;
			return;
		}

		/*
		 * Reserve the whole range PROT_NONE.  MAP_NORESERVE so no commit
		 * charge accrues until BufPoolCommit touches a sub-range.
		 */
		base = mmap(NULL, want, PROT_NONE,
					MAP_SHARED | MAP_NORESERVE, fd, 0);
		if (base == MAP_FAILED)
		{
			ereport(LOG,
					(errmsg("could not reserve %zu bytes of address space for buffer pools: %m",
							want)));
			close(fd);
			ReserveCtl->total_size = 0;
			return;
		}

		resv_fd = fd;
		resv_base = base;
		resv_size = want;
		ReserveCtl->total_size = want;

		ereport(LOG,
				(errmsg("reserved %zu MB of address space for same-address buffer pools at %p",
						want >> 20, base)));
	}
#else
	/* Unsupported platform: feature off, legacy DSM path used. */
	ReserveCtl->total_size = 0;
	ereport(LOG,
			(errmsg("same-address buffer pools are not supported on this platform; using per-segment mappings")));
#endif
}

/*
 * bufpool_reserve_coalesce -- merge adjacent free extents (caller holds mutex).
 *
 * The naive bump+exact-reuse scheme suffers external fragmentation: after a
 * create/drop churn, free space can be split into several small extents so a
 * larger pool is rejected despite sufficient aggregate free space.  Coalescing
 * adjacent free extents (and best-fit + remainder splitting in the allocator)
 * recovers that space without changing the on-the-hot-path representation
 * (each pool still owns one contiguous sub-range, so BufPoolAddrAt stays
 * base+offset).
 *
 * A contiguous request can still fail if a live pool physically separates two
 * free regions; the complete fix for that is disjoint-extent pools, noted as
 * the upgrade path in bufpool_internals.h.  Coalescing handles the common case
 * (adjacent frees) cheaply.
 */
static void
bufpool_reserve_coalesce(void)
{
	int			n = ReserveCtl->nextents;
	BufPoolExtent *e = ReserveCtl->extents;

	/* Insertion sort by offset (n <= MAX_BUFFER_POOLS, tiny). */
	for (int i = 1; i < n; i++)
	{
		BufPoolExtent key = e[i];
		int			j = i - 1;

		while (j >= 0 && e[j].offset > key.offset)
		{
			e[j + 1] = e[j];
			j--;
		}
		e[j + 1] = key;
	}

	/* Merge adjacent free extents. */
	for (int i = 0; i + 1 < ReserveCtl->nextents;)
	{
		if (!e[i].in_use && !e[i + 1].in_use &&
			e[i].offset + e[i].size == e[i + 1].offset)
		{
			e[i].size += e[i + 1].size;
			memmove(&e[i + 1], &e[i + 2],
					(ReserveCtl->nextents - i - 2) * sizeof(BufPoolExtent));
			ReserveCtl->nextents--;
		}
		else
			i++;
	}
}

/*
 * BufPoolReserveAlloc -- reserve a sub-range of the requested size.
 *
 * Returns the offset into the reservation, or (Size) -1 if the feature is
 * off or there is no room.  Does NOT commit memory; call BufPoolCommit next.
 * The caller (pool create) records the offset in the pool descriptor.
 *
 * Uses coalescing + best-fit + remainder splitting to resist external
 * fragmentation; see bufpool_reserve_coalesce.
 */
Size
BufPoolReserveAlloc(Size size)
{
	Size		offset;
	long		pgsz = sysconf(_SC_PAGESIZE);
	int			best;

	if (!BufPoolReserveActive())
		return (Size) -1;

	if (pgsz > 0 && (size % (Size) pgsz) != 0)
		size += (Size) pgsz - (size % (Size) pgsz);

	SpinLockAcquire(&ReserveCtl->mutex);

	/* Recover adjacent free space before searching. */
	bufpool_reserve_coalesce();

	/*
	 * Best-fit among free extents: the smallest free extent that still fits,
	 * to leave larger holes intact for larger future pools.
	 */
	best = -1;
	for (int i = 0; i < ReserveCtl->nextents; i++)
	{
		BufPoolExtent *e = &ReserveCtl->extents[i];

		if (!e->in_use && e->size >= size &&
			(best < 0 || e->size < ReserveCtl->extents[best].size))
			best = i;
	}

	if (best >= 0)
	{
		BufPoolExtent *e = &ReserveCtl->extents[best];

		offset = e->offset;

		/* Split the remainder off as a new free extent, if room to track it. */
		if (e->size > size && ReserveCtl->nextents < MAX_BUFFER_POOLS)
		{
			BufPoolExtent *rem = &ReserveCtl->extents[ReserveCtl->nextents++];

			rem->offset = offset + size;
			rem->size = e->size - size;
			rem->in_use = false;
			e->size = size;
		}
		e->in_use = true;
		SpinLockRelease(&ReserveCtl->mutex);
		return offset;
	}

	/* No suitable hole: bump-allocate from the tail. */
	if (ReserveCtl->bump + size > resv_size ||
		ReserveCtl->nextents >= MAX_BUFFER_POOLS)
	{
		SpinLockRelease(&ReserveCtl->mutex);
		return (Size) -1;		/* out of reservation */
	}

	offset = ReserveCtl->bump;
	ReserveCtl->bump += size;
	ReserveCtl->extents[ReserveCtl->nextents].offset = offset;
	ReserveCtl->extents[ReserveCtl->nextents].size = size;
	ReserveCtl->extents[ReserveCtl->nextents].in_use = true;
	ReserveCtl->nextents++;

	SpinLockRelease(&ReserveCtl->mutex);
	return offset;
}

/*
 * BufPoolReserveFree -- mark a sub-range free for reuse (after decommit).
 */
void
BufPoolReserveFree(Size offset)
{
	if (!BufPoolReserveActive())
		return;

	SpinLockAcquire(&ReserveCtl->mutex);
	for (int i = 0; i < ReserveCtl->nextents; i++)
	{
		if (ReserveCtl->extents[i].offset == offset)
		{
			ReserveCtl->extents[i].in_use = false;
			break;
		}
	}
	SpinLockRelease(&ReserveCtl->mutex);
}

/*
 * BufPoolAddrAt -- resolve a reservation offset to this backend's address.
 *
 * Because the reservation is at the same address in every backend, this is
 * just base + offset, valid in all backends without per-backend bookkeeping.
 */
void *
BufPoolAddrAt(Size offset)
{
	Assert(BufPoolReserveActive());
	Assert(offset < resv_size);
	return resv_base + offset;
}

/*
 * BufPoolAttachLocal -- map a committed sub-range read/write in THIS backend.
 *
 * A MAP_FIXED commit changes only the committing process's page tables, not
 * those of processes that already mapped the reservation (PROT_NONE) before
 * the commit -- e.g. backends or IO workers forked before the pool existed.
 * Such a process must re-map the committed sub-range in its own address space
 * before touching the pool.  Because we map at the same address backed by the
 * same memfd offset, the result is the identical shared pages at the same
 * virtual address.  Idempotent and cheap (one mmap, no segment registration).
 *
 * Returns the (unchanged) base address of the sub-range, or NULL on failure.
 */
void *
BufPoolAttachLocal(Size offset, Size size)
{
#ifdef BUFPOOL_RESERVE_SUPPORTED
	void	   *want;
	void	   *p;

	Assert(BufPoolReserveActive());
	Assert(offset + size <= resv_size);

	want = resv_base + offset;
	p = mmap(want, size, PROT_READ | PROT_WRITE,
			 MAP_SHARED | MAP_FIXED, resv_fd, offset);
	if (p == MAP_FAILED)
		return NULL;
	Assert(p == want);
	return p;
#else
	(void) offset;
	(void) size;
	return NULL;
#endif
}

/*
 * BufPoolCommit -- back a sub-range with real, shared, writable pages.
 *
 * Returns true on success.  huge requests MAP_HUGETLB where available; on
 * failure with huge it is the caller's choice whether to retry without.
 */
/*
 * BufPoolNumaInterleave -- spread a committed range across NUMA nodes.
 *
 * Generic, algorithm-agnostic placement: it operates on the pool's MEMORY
 * (the committed reservation sub-range), independent of which replacement
 * algorithm the pool uses, so every pool/algorithm benefits uniformly.  On a
 * multi-node system it sets an MPOL_INTERLEAVE policy over the range so the
 * pool's pages (and the access traffic to them) are distributed across nodes
 * rather than concentrated on the allocating backend's local node -- the same
 * rationale as interleaving the main shared_buffers on NUMA hardware.
 *
 * Gated three ways: compiled only with USE_LIBNUMA, enabled only when the
 * buffer_pool_numa_interleave GUC is on, and a no-op unless the running system
 * actually has more than one NUMA node (numa_available() == 0 and
 * numa_max_node() > 0).  So it does nothing on non-NUMA or single-node hosts.
 */
void
BufPoolNumaInterleave(void *addr, Size size)
{
#ifdef USE_LIBNUMA
	if (!buffer_pool_numa_interleave)
		return;

	/* numa_available() returns 0 when NUMA is available, -1 otherwise. */
	if (numa_available() < 0)
		return;
	if (numa_max_node() <= 0)
		return;					/* single node: nothing to spread */

	{
		struct bitmask *nodes = numa_get_mems_allowed();

		if (nodes != NULL)
		{
			/*
			 * MPOL_INTERLEAVE over the range.  numa_interleave_memory touches
			 * page placement policy only; pages fault in interleaved on first
			 * use.  Failures are advisory -- the pool still works, just
			 * node-local.
			 */
			numa_interleave_memory(addr, size, nodes);
			numa_free_nodemask(nodes);
		}
	}
#else
	(void) addr;
	(void) size;
#endif
}

bool
BufPoolCommit(Size offset, Size size, bool huge)
{
#ifdef BUFPOOL_RESERVE_SUPPORTED
	void	   *want = resv_base + offset;
	void	   *p;
	int			flags = MAP_SHARED | MAP_FIXED;

	Assert(BufPoolReserveActive());
	Assert(offset + size <= resv_size);

#ifdef MAP_HUGETLB
	if (huge)
		flags |= MAP_HUGETLB;
#endif

	p = mmap(want, size, PROT_READ | PROT_WRITE, flags, resv_fd, offset);
	if (p == MAP_FAILED && huge)
	{
		/* retry without huge pages */
		p = mmap(want, size, PROT_READ | PROT_WRITE,
				 MAP_SHARED | MAP_FIXED, resv_fd, offset);
	}
	if (p == MAP_FAILED)
	{
		ereport(LOG,
				(errmsg("could not commit %zu bytes of buffer-pool memory at offset %zu: %m",
						size, offset)));
		return false;
	}
	Assert(p == want);

	/* Spread the committed pages across NUMA nodes (no-op on 1-node systems). */
	BufPoolNumaInterleave(p, size);

	return true;
#else
	(void) offset;
	(void) size;
	(void) huge;
	return false;
#endif
}

/*
 * BufPoolDecommit -- reclaim a sub-range and make it fault on access.
 *
 * Punches a hole in the backing memfd to release physical memory, then
 * remaps the sub-range PROT_NONE so any stale pointer into the freed pool
 * faults rather than reading another pool's pages.
 */
void
BufPoolDecommit(Size offset, Size size)
{
#ifdef BUFPOOL_RESERVE_SUPPORTED
	void	   *want = resv_base + offset;

	Assert(BufPoolReserveActive());
	Assert(offset + size <= resv_size);

#if defined(FALLOC_FL_PUNCH_HOLE) && defined(FALLOC_FL_KEEP_SIZE)
	/* Reclaim physical memory; ignore failure (still PROT_NONE below). */
	(void) fallocate(resv_fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
					 (off_t) offset, (off_t) size);
#endif

	if (mmap(want, size, PROT_NONE, MAP_SHARED | MAP_NORESERVE | MAP_FIXED,
			 resv_fd, offset) == MAP_FAILED)
		ereport(LOG,
				(errmsg("could not decommit %zu bytes of buffer-pool memory at offset %zu: %m",
						size, offset)));
#else
	(void) offset;
	(void) size;
#endif
}
