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

#include "miscadmin.h"
#include "port/pg_bitutils.h"
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
 * BufPoolReserveAlloc -- reserve a sub-range of the requested size.
 *
 * Returns the offset into the reservation, or (Size) -1 if the feature is
 * off or there is no room.  Does NOT commit memory; call BufPoolCommit next.
 * The caller (pool create) records the offset in the pool descriptor.
 */
Size
BufPoolReserveAlloc(Size size)
{
	Size		offset;
	long		pgsz = sysconf(_SC_PAGESIZE);

	if (!BufPoolReserveActive())
		return (Size) -1;

	if (pgsz > 0 && (size % (Size) pgsz) != 0)
		size += (Size) pgsz - (size % (Size) pgsz);

	SpinLockAcquire(&ReserveCtl->mutex);

	/* First try to reuse a freed extent that is big enough. */
	for (int i = 0; i < ReserveCtl->nextents; i++)
	{
		BufPoolExtent *e = &ReserveCtl->extents[i];

		if (!e->in_use && e->size >= size)
		{
			e->in_use = true;
			offset = e->offset;
			SpinLockRelease(&ReserveCtl->mutex);
			return offset;
		}
	}

	/* Otherwise bump-allocate from the tail. */
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
 * BufPoolCommit -- back a sub-range with real, shared, writable pages.
 *
 * Returns true on success.  huge requests MAP_HUGETLB where available; on
 * failure with huge it is the caller's choice whether to retry without.
 */
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
