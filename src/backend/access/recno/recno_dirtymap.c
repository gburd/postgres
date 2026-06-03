/*-------------------------------------------------------------------------
 *
 * recno_dirtymap.c
 *	  Shared-memory dirty block map for the RECNO table access method.
 *
 * This module tracks which heap pages have ever carried an in-place
 * modification whose before-image a scanner might still need from the sLog.
 * The scan path uses it as a fast-path filter: if a page's bit is CLEAR,
 * every tuple on it is plain-committed with no retained before-image, so the
 * per-tuple sLog before-image probe can be skipped for the whole page.
 *
 * Implementation:
 *   - A single process-wide sparsemap (one bit per page) in a dedicated
 *     shared-memory buffer, protected by a spinlock.  This mirrors the sLog
 *     xid_map (see slog.c).
 *   - The bit index is the collision-free composite key
 *     ((uint64) relid << 32) | blkno, so every page owns a distinct bit and
 *     there is no false sharing between different pages.  The sparsemap is
 *     run-length compressed, so relations with no in-place modifications
 *     cost almost no memory.
 *   - The map is GROW-ONLY: a bit, once set, is never cleared during normal
 *     operation.  See recno_dirtymap.h for the correctness rationale.
 *   - If a set ever fails to fit the fixed buffer, a sticky "full" flag is
 *     latched and every check returns dirty (safe degradation).
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/recno/recno_dirtymap.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/recno_dirtymap.h"
#include "lib/sparsemap.h"
#include "storage/shmem.h"
#include "storage/spin.h"

/*
 * Size of the sparsemap backing buffer, in bytes.  The map is RLE-compressed,
 * so this comfortably covers a large number of in-place-modified pages spread
 * across many relations; the overflow latch is the safety net if it is ever
 * exhausted.
 */
#define RECNO_DIRTYMAP_BUFSIZE	(256 * 1024)

/*
 * Compose the collision-free sparsemap bit index for a page.  relid is a
 * 32-bit Oid and blkno a 32-bit BlockNumber, so the 64-bit index is unique
 * per (relid, blkno).
 */
#define RECNO_DIRTYMAP_KEY(relid, blkno) \
	(((uint64) (relid) << 32) | (uint64) (blkno))

/*
 * Shared state: the sparsemap struct, its spinlock, the sticky overflow flag,
 * and the inline backing buffer.
 */
typedef struct RecnoDirtyMapShared
{
	sparsemap_t map;			/* sparsemap struct (header) */
	slock_t		mutex;			/* protects map and full */
	bool		full;			/* sticky: set failed to fit -> probe always */
	char		buffer[RECNO_DIRTYMAP_BUFSIZE]; /* sparsemap backing store */
}			RecnoDirtyMapShared;

static RecnoDirtyMapShared *DirtyMap = NULL;

/* ----------------------------------------------------------------
 * Shared memory initialization
 * ----------------------------------------------------------------
 */

Size
RecnoDirtyMapShmemSize(void)
{
	return MAXALIGN(sizeof(RecnoDirtyMapShared));
}

static void
RecnoDirtyMapShmemRequest(void *arg)
{
	ShmemRequestStruct(.name = "RECNO DirtyMap",
					   .size = RecnoDirtyMapShmemSize(),
					   .ptr = (void **) &DirtyMap,
		);
}

static void
RecnoDirtyMapShmemInit_cb(void *arg)
{
	Assert(DirtyMap != NULL);

	sparsemap_init(&DirtyMap->map, (uint8 *) DirtyMap->buffer,
				   RECNO_DIRTYMAP_BUFSIZE);
	SpinLockInit(&DirtyMap->mutex);
	DirtyMap->full = false;
}

void
RecnoDirtyMapShmemInit(void)
{
	/* Initialization is handled by RecnoDirtyMapShmemCallbacks */
}

const ShmemCallbacks RecnoDirtyMapShmemCallbacks = {
	.request_fn = RecnoDirtyMapShmemRequest,
	.init_fn = RecnoDirtyMapShmemInit_cb,
};

/* ----------------------------------------------------------------
 * Mark and query
 * ----------------------------------------------------------------
 */

/*
 * RecnoDirtyMapMark
 *		Set the bit for (relid, blkno).
 *
 * Must be called while the page's buffer is exclusively locked, before that
 * lock is released, so a concurrent scanner always observes the set bit.
 * Idempotent.  If the sparsemap cannot grow to hold the bit, the sticky full
 * flag is latched so every subsequent check conservatively returns dirty.
 */
void
RecnoDirtyMapMark(Oid relid, BlockNumber blkno)
{
	uint64		idx = RECNO_DIRTYMAP_KEY(relid, blkno);

	SpinLockAcquire(&DirtyMap->mutex);
	if (!DirtyMap->full)
	{
		if (sparsemap_add(&DirtyMap->map, idx) == SPARSEMAP_IDX_MAX)
			DirtyMap->full = true;
	}
	SpinLockRelease(&DirtyMap->mutex);
}

/*
 * RecnoDirtyMapCheck
 *		Return true if the page's bit is set (or the map has overflowed).
 *
 * A false result means the page is provably clean and the scan path may skip
 * the per-tuple sLog before-image probe for the whole page.
 */
bool
RecnoDirtyMapCheck(Oid relid, BlockNumber blkno)
{
	uint64		idx = RECNO_DIRTYMAP_KEY(relid, blkno);
	bool		result;

	SpinLockAcquire(&DirtyMap->mutex);
	if (DirtyMap->full)
		result = true;
	else
		result = sparsemap_contains(&DirtyMap->map, idx);
	SpinLockRelease(&DirtyMap->mutex);

	return result;
}
