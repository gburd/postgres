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

/*
 * Whether the platform supports the same-address reservation mechanism.
 * Requires memfd_create (Linux) and a non-EXEC_BACKEND build (fork
 * inheritance of the reservation mapping).  Defined here, before the
 * POSIX-only headers, so they are only pulled in where they exist -- on
 * Windows/MSVC (EXEC_BACKEND) sys/mman.h etc. do not exist.
 */
#if defined(__linux__) && !defined(EXEC_BACKEND)
#define BUFPOOL_RESERVE_SUPPORTED 1
#endif

#ifdef BUFPOOL_RESERVE_SUPPORTED
#include <sys/mman.h>
#include <unistd.h>
#include <fcntl.h>
#include <linux/falloc.h>
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
 * Reservation memory model (supports disjoint physical backing).
 *
 * The scarce resource is committed PHYSICAL memory (memfd offsets), not
 * address space: the PROT_NONE reservation is large and costs nothing until
 * committed.  We therefore separate the two:
 *
 *   - Address WINDOWS are carved contiguously from the reservation by a bump
 *     pointer.  A pool always gets ONE contiguous window, so every pool
 *     pointer stays base+offset with no hot-path indirection.
 *
 *   - Physical CHUNKS are fixed-size pieces of the backing memfd, tracked in
 *     a free list.  A pool's window is backed by N chunks MAP_FIXED into it,
 *     and those chunks may be DISJOINT in the memfd.  This is what makes the
 *     allocator immune to external fragmentation: any N free chunks satisfy a
 *     pool needing N chunks, regardless of their memfd positions.
 *
 * Chunk granularity is a tradeoff: smaller chunks pack tighter (less internal
 * fragmentation) but need more map operations and more free-list slots.
 */
#define BUFPOOL_CHUNK_SIZE		((Size) 2 * 1024 * 1024)	/* 2MB, == common
															 * hugepage */
#define BUFPOOL_MAX_CHUNKS		4096	/* caps total reservable at CHUNK*this */

/* GUC: maximum total memory reservable across all buffer pools (in blocks) */
int			max_buffer_pool_memory = 0;

/*
 * Per-backend pointers to the reservation.  Inherited across fork(), so these
 * are valid in every backend at the same address without re-initialization.
 * resv_fd is the backing memfd; resv_base is the start of the PROT_NONE
 * reservation.
 */
static int	resv_fd pg_attribute_unused() = -1;
static char *resv_base = NULL;
static Size resv_size = 0;

/*
 * Allocator state, in the main shared-memory segment so all backends agree.
 *
 * A pool allocation is described by a BufPoolWindow: a contiguous address
 * window (win_offset, win_size) plus the list of physical chunk indices that
 * back it, in window order.  chunk_state[] is the global free/used bitmap of
 * physical chunks.
 */
/*
 * Maximum chunks a single pool window can span.  A pool this large
 * (BUFPOOL_MAX_POOL_CHUNKS * 2MB) is already enormous; bounding it keeps the
 * per-window chunk list a fixed, small array.
 */
#define BUFPOOL_MAX_POOL_CHUNKS	512 /* up to 1GB per pool at 2MB chunks */

typedef struct BufPoolWindow
{
	Size		win_offset;		/* offset of the contiguous window in the resv */
	Size		win_size;		/* window size (nchunks * BUFPOOL_CHUNK_SIZE) */
	int			nchunks;		/* number of backing chunks */
	int			chunks[BUFPOOL_MAX_POOL_CHUNKS];	/* backing chunk indices,
													 * in order */
	bool		in_use;
}			BufPoolWindow;

typedef struct BufPoolReserveControl
{
	slock_t		mutex;			/* protects the allocator state */
	Size		total_size;		/* reservation size (bytes) */
	Size		win_bump;		/* next free address-window offset */
	int			nchunks_total;	/* physical chunks the memfd provides */
	int			nwindows;
	BufPoolWindow windows[MAX_BUFFER_POOLS];
	bool		chunk_used[BUFPOOL_MAX_CHUNKS];
}			BufPoolReserveControl;

static BufPoolReserveControl * ReserveCtl = NULL;

/*
 * Pointer filled by the shmem request (BufferPoolShmemRequest) with the
 * pre-allocated control block, so BufPoolReserveInit need not ShmemAlloc a
 * 100KB+ struct at startup (which overflows the post-init allocator slop).
 * Typed void * because the struct is private to this file.
 */
void	   *BufPoolReserveCtlPtr = NULL;

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

	/* Use the control block pre-allocated by the shmem request. */
	ReserveCtl = (BufPoolReserveControl *) BufPoolReserveCtlPtr;
	Assert(ReserveCtl != NULL);
	memset(ReserveCtl, 0, sizeof(BufPoolReserveControl));
	SpinLockInit(&ReserveCtl->mutex);
	ReserveCtl->nwindows = 0;
	ReserveCtl->win_bump = 0;
	ReserveCtl->nchunks_total = 0;

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

		/*
		 * Physical backing is handed out in fixed BUFPOOL_CHUNK_SIZE chunks
		 * from a free list, so a pool can be backed by disjoint chunks
		 * (immune to external fragmentation).  Address WINDOWS are still
		 * contiguous, so pool pointers stay base+offset.
		 */
		ReserveCtl->nchunks_total = (int) Min((Size) BUFPOOL_MAX_CHUNKS,
											  want / BUFPOOL_CHUNK_SIZE);

		ereport(LOG,
				(errmsg("reserved %zu MB of address space for same-address buffer pools at %p (%d chunks of %zu MB)",
						want >> 20, base, ReserveCtl->nchunks_total,
						BUFPOOL_CHUNK_SIZE >> 20)));
	}
#else
	/* Unsupported platform: feature off, legacy DSM path used. */
	ReserveCtl->total_size = 0;
	ereport(LOG,
			(errmsg("same-address buffer pools are not supported on this platform; using per-segment mappings")));
#endif
}

/*
 * bufpool_find_window -- locate a window by its address offset (holds mutex).
 */
static BufPoolWindow *
bufpool_find_window(Size win_offset)
{
	for (int i = 0; i < ReserveCtl->nwindows; i++)
	{
		if (ReserveCtl->windows[i].in_use &&
			ReserveCtl->windows[i].win_offset == win_offset)
			return &ReserveCtl->windows[i];
	}
	return NULL;
}

/*
 * BufPoolReserveAlloc -- reserve a contiguous address window of the given
 *		size, backed by N (possibly disjoint) physical chunks.
 *
 * Returns the window's offset into the reservation, or (Size) -1 if the
 * feature is off or there is not enough free physical memory.  Does NOT map
 * anything; call BufPoolCommit next.
 *
 * Fragmentation immunity: the address window is bump-allocated and always
 * contiguous (so pool pointers stay base+offset), but the backing chunks are
 * pulled from a free list and may be disjoint in the memfd.  A request for N
 * chunks succeeds whenever N chunks are free anywhere, regardless of their
 * positions -- so a pool can never be denied while aggregate free memory
 * suffices, the defect of the old single-extent allocator.
 */
Size
BufPoolReserveAlloc(Size size)
{
	Size		win_offset;
	int			need_chunks;
	BufPoolWindow *w;

	if (!BufPoolReserveActive())
		return (Size) -1;

	/* Round the window up to a whole number of chunks. */
	need_chunks = (int) ((size + BUFPOOL_CHUNK_SIZE - 1) / BUFPOOL_CHUNK_SIZE);
	if (need_chunks <= 0)
		need_chunks = 1;
	if (need_chunks > BUFPOOL_MAX_POOL_CHUNKS)
		return (Size) -1;		/* pool larger than a window can describe */

	SpinLockAcquire(&ReserveCtl->mutex);

	if (ReserveCtl->nwindows >= MAX_BUFFER_POOLS)
	{
		SpinLockRelease(&ReserveCtl->mutex);
		return (Size) -1;
	}

	/* Count free chunks first; fail cleanly if not enough. */
	{
		int			free_chunks = 0;

		for (int c = 0; c < ReserveCtl->nchunks_total; c++)
			if (!ReserveCtl->chunk_used[c])
				free_chunks++;
		if (free_chunks < need_chunks)
		{
			SpinLockRelease(&ReserveCtl->mutex);
			return (Size) -1;	/* out of physical memory (but not address
								 * space) */
		}
	}

	/*
	 * Address windows are carved contiguously by a bump pointer.  Reuse a
	 * freed window of the exact chunk count if one exists (windows are only
	 * recycled wholesale, so the address space does not meaningfully
	 * fragment); otherwise bump a fresh window.  Either way the physical
	 * chunks come from the free list and may be disjoint.
	 */
	w = NULL;
	for (int i = 0; i < ReserveCtl->nwindows; i++)
	{
		BufPoolWindow *cand = &ReserveCtl->windows[i];

		if (!cand->in_use && cand->nchunks == need_chunks)
		{
			w = cand;
			break;
		}
	}
	if (w == NULL)
	{
		Size		want = (Size) need_chunks * BUFPOOL_CHUNK_SIZE;

		if (ReserveCtl->win_bump + want > resv_size)
		{
			SpinLockRelease(&ReserveCtl->mutex);
			return (Size) -1;	/* out of address space (very unlikely) */
		}
		w = &ReserveCtl->windows[ReserveCtl->nwindows++];
		w->win_offset = ReserveCtl->win_bump;
		ReserveCtl->win_bump += want;
	}

	/* Claim need_chunks free physical chunks (first-free; order recorded). */
	w->win_size = (Size) need_chunks * BUFPOOL_CHUNK_SIZE;
	w->nchunks = need_chunks;
	{
		int			assigned = 0;

		for (int c = 0; c < ReserveCtl->nchunks_total && assigned < need_chunks; c++)
		{
			if (!ReserveCtl->chunk_used[c])
			{
				ReserveCtl->chunk_used[c] = true;
				w->chunks[assigned++] = c;
			}
		}
		Assert(assigned == need_chunks);
	}
	w->in_use = true;
	win_offset = w->win_offset;

	SpinLockRelease(&ReserveCtl->mutex);
	return win_offset;
}

/*
 * BufPoolReserveFree -- free a window and return its chunks to the free list.
 */
void
BufPoolReserveFree(Size offset)
{
	BufPoolWindow *w;

	if (!BufPoolReserveActive())
		return;

	SpinLockAcquire(&ReserveCtl->mutex);
	w = bufpool_find_window(offset);
	if (w != NULL)
	{
		for (int i = 0; i < w->nchunks; i++)
			ReserveCtl->chunk_used[w->chunks[i]] = false;
		w->in_use = false;
		/* keep w->nchunks so a same-size window can be reused */
	}
	SpinLockRelease(&ReserveCtl->mutex);
}

/*
 * BufPoolAddrAt -- resolve a reservation offset to this backend's address.
 *
 * Because the reservation is at the same address in every backend, this is
 * just base + offset, valid in all backends without per-backend bookkeeping.
 * Disjoint physical backing is invisible here: a pool's window is contiguous
 * address space, so block i is always at window_base + offset.
 */
void *
BufPoolAddrAt(Size offset)
{
	Assert(BufPoolReserveActive());
	Assert(offset < resv_size);
	return resv_base + offset;
}

/*
 * bufpool_map_window -- MAP_FIXED each backing chunk of a window into the
 *		window's contiguous address range in THIS backend.
 *
 * prot/extra_flags select commit (PROT_READ|WRITE) vs decommit (PROT_NONE +
 * MAP_NORESERVE).  Each chunk c is mapped at win_base + k*CHUNK_SIZE backed by
 * memfd offset c*CHUNK_SIZE, so disjoint physical chunks form one contiguous
 * window.  Returns true if every chunk mapped successfully.
 *
 * Defined only on the reservation-supported platform; its callers (Commit /
 * Decommit / AttachLocal) are all compiled out otherwise, so defining it
 * unconditionally would draw an unused-function warning (e.g. clang on
 * FreeBSD, which has no libnuma / memfd path).
 */
#ifdef BUFPOOL_RESERVE_SUPPORTED
static bool
bufpool_map_window(BufPoolWindow * w, int prot, int extra_flags, bool huge)
{
	char	   *win_base = resv_base + w->win_offset;

	for (int k = 0; k < w->nchunks; k++)
	{
		void	   *want = win_base + (Size) k * BUFPOOL_CHUNK_SIZE;
		Size		chunk_memfd_off = (Size) w->chunks[k] * BUFPOOL_CHUNK_SIZE;
		int			flags = MAP_SHARED | MAP_FIXED | extra_flags;
		void	   *p;

#ifdef MAP_HUGETLB
		if (huge && prot != PROT_NONE)
			flags |= MAP_HUGETLB;
#endif
		p = mmap(want, BUFPOOL_CHUNK_SIZE, prot, flags, resv_fd, chunk_memfd_off);
		if (p == MAP_FAILED && huge && prot != PROT_NONE)
			p = mmap(want, BUFPOOL_CHUNK_SIZE, prot,
					 MAP_SHARED | MAP_FIXED | extra_flags, resv_fd,
					 chunk_memfd_off);
		if (p == MAP_FAILED)
			return false;
		Assert(p == want);
	}
	return true;
}
#endif							/* BUFPOOL_RESERVE_SUPPORTED */

/*
 * BufPoolAttachLocal -- map a pool's window read/write in THIS backend.
 *
 * A MAP_FIXED commit changes only the committing process's page tables, not
 * those of processes that already mapped the reservation (PROT_NONE) before
 * the commit -- e.g. backends or IO workers forked before the pool existed.
 * Such a process must re-map the window's chunks in its own address space
 * before touching the pool.  Because each chunk maps at the same window
 * address backed by the same memfd offset, the result is the identical shared
 * pages at the same virtual address.  Idempotent and cheap.
 *
 * Returns the window base address, or NULL on failure.
 */
void *
BufPoolAttachLocal(Size offset, Size size)
{
#ifdef BUFPOOL_RESERVE_SUPPORTED
	BufPoolWindow *w;
	void	   *win_base;

	Assert(BufPoolReserveActive());
	(void) size;

	SpinLockAcquire(&ReserveCtl->mutex);
	w = bufpool_find_window(offset);
	SpinLockRelease(&ReserveCtl->mutex);
	if (w == NULL)
		return NULL;

	if (!bufpool_map_window(w, PROT_READ | PROT_WRITE, 0, false))
		return NULL;

	win_base = resv_base + offset;
	return win_base;
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
#ifdef BUFPOOL_RESERVE_SUPPORTED
/*
 * bufpool_numa_distribute_window -- spread a dynamic pool's committed window
 *		across NUMA nodes, chunk by chunk.
 *
 * Delegates to the shared NUMA layer (BufPoolNumaBindRange).  Unlike the
 * default pool, a dynamic pool's window interleaves descriptors, blocks, hash,
 * and strategy state, so we cannot cleanly co-locate a buffer with its
 * descriptor; we instead bind successive window chunks round-robin to nodes so
 * the pool's pages and traffic are spread rather than concentrated on the
 * creating backend's node.  No-op unless NUMA distribution is active.  Only
 * needed on the reservation-supported path (its sole caller, BufPoolCommit, is
 * itself compiled out elsewhere).
 */
static void
bufpool_numa_distribute_window(void *addr, Size size)
{
	int			nodes = BufPoolNumaNodes();
	Size		off = 0;
	int			k = 0;

	if (nodes <= 1)
		return;

	while (off < size)
	{
		Size		this_sz = Min(BUFPOOL_CHUNK_SIZE, size - off);

		BufPoolNumaBindRange((char *) addr + off, this_sz, k % nodes);
		off += this_sz;
		k++;
	}
}
#endif							/* BUFPOOL_RESERVE_SUPPORTED */

bool
BufPoolCommit(Size offset, Size size, bool huge)
{
#ifdef BUFPOOL_RESERVE_SUPPORTED
	BufPoolWindow *w;

	Assert(BufPoolReserveActive());
	(void) size;

	SpinLockAcquire(&ReserveCtl->mutex);
	w = bufpool_find_window(offset);
	SpinLockRelease(&ReserveCtl->mutex);
	if (w == NULL)
		return false;

	/*
	 * Map every backing chunk read/write into the window.  The chunks may be
	 * disjoint in the memfd; they form one contiguous address window.
	 */
	if (!bufpool_map_window(w, PROT_READ | PROT_WRITE, 0, huge))
	{
		ereport(LOG,
				(errmsg("could not commit %d-chunk buffer-pool window at offset %zu: %m",
						w->nchunks, offset)));
		return false;
	}

	/* Spread the committed pages across NUMA nodes (no-op on 1-node systems). */
	bufpool_numa_distribute_window(resv_base + offset, w->win_size);

	return true;
#else
	(void) offset;
	(void) size;
	(void) huge;
	return false;
#endif
}

/*
 * BufPoolDecommit -- reclaim a window's memory and make it fault on access.
 *
 * Punches a hole in the backing memfd for each backing chunk to release
 * physical memory, then remaps the whole window PROT_NONE so any stale
 * pointer into the freed pool faults rather than reading another pool's pages.
 */
void
BufPoolDecommit(Size offset, Size size)
{
#ifdef BUFPOOL_RESERVE_SUPPORTED
	BufPoolWindow *w;

	Assert(BufPoolReserveActive());
	(void) size;

	SpinLockAcquire(&ReserveCtl->mutex);
	w = bufpool_find_window(offset);
	SpinLockRelease(&ReserveCtl->mutex);
	if (w == NULL)
		return;

#if defined(FALLOC_FL_PUNCH_HOLE) && defined(FALLOC_FL_KEEP_SIZE)
	/* Reclaim physical memory for each backing chunk (disjoint in the memfd). */
	for (int k = 0; k < w->nchunks; k++)
	{
		off_t		chunk_off = (off_t) w->chunks[k] * BUFPOOL_CHUNK_SIZE;

		(void) fallocate(resv_fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
						 chunk_off, (off_t) BUFPOOL_CHUNK_SIZE);
	}
#endif

	/* Remap the whole window PROT_NONE so stale accesses fault. */
	if (!bufpool_map_window(w, PROT_NONE, MAP_NORESERVE, false))
		ereport(LOG,
				(errmsg("could not decommit buffer-pool window at offset %zu: %m",
						offset)));
#else
	(void) offset;
	(void) size;
#endif
}
