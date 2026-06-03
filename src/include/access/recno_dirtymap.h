/*-------------------------------------------------------------------------
 *
 * recno_dirtymap.h
 *	  Shared-memory dirty block map for the RECNO table access method.
 *
 * The dirty map tracks which heap pages have ever carried an in-place
 * modification (in-place UPDATE or in-place DELETE) whose before-image a
 * scanner might still need from the sLog.  If a page's bit is CLEAR, every
 * tuple on it is plain-committed with no retained before-image, and the scan
 * path can skip the per-tuple sLog before-image probe for the whole page
 * (the fast path).
 *
 * Implementation: a single process-wide sparsemap (one bit per page) held in
 * a dedicated shared-memory buffer, protected by a spinlock.  This mirrors
 * the sLog xid_map (see slog.c).  The bit index is the collision-free
 * composite key ((uint64) relid << 32) | blkno, so each (relid, blkno) page
 * owns a distinct bit -- there is no hashing and therefore no false sharing
 * between different pages.  The sparsemap is run-length compressed, so the
 * common case of a relation with no in-place modifications costs almost no
 * memory.
 *
 * Correctness invariant:
 *   - SET is MANDATORY.  Every in-place modification must set the page's bit
 *     before the buffer lock that covers the modification is released, so a
 *     concurrent scanner on another backend always observes it.  A scanner
 *     that pins-and-locks the page after the writer released the lock is
 *     guaranteed to see the set bit.
 *   - The map is GROW-ONLY during normal operation.  A bit, once set, is not
 *     cleared by commit or abort.  This is the safe direction: a stale set
 *     bit only costs an unnecessary sLog probe (which then returns the
 *     on-page value), whereas a wrongly-cleared bit while an old snapshot
 *     still needs the before-image would be a false negative -> WRONG
 *     RESULTS.  Reclaiming bits safely requires rebuilding the clear set from
 *     the sLog's retained-entry set under the same xid horizon that gates
 *     before-image reclamation; that is a future optimization, not required
 *     for correctness.
 *
 * Overflow: the sparsemap buffer is fixed-size.  If a set ever fails to fit,
 * a sticky "full" flag is latched and every subsequent check returns dirty,
 * degrading safely to the pre-fast-path behavior (always probe the sLog).
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/recno_dirtymap.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RECNO_DIRTYMAP_H
#define RECNO_DIRTYMAP_H

#include "storage/block.h"
#include "storage/shmem.h"

/* Shared memory sizing and initialization */
extern Size RecnoDirtyMapShmemSize(void);
extern void RecnoDirtyMapShmemInit(void);
extern const ShmemCallbacks RecnoDirtyMapShmemCallbacks;

/*
 * Mark a page's bit dirty (called from the INSERT/UPDATE/DELETE in-place
 * paths while the page's buffer is exclusively locked).  Idempotent; safe to
 * call repeatedly for the same page.
 */
extern void RecnoDirtyMapMark(Oid relid, BlockNumber blkno);

/*
 * Query: might the page carry a retained in-place modification?
 *
 * Returns true if the page's bit is set (or the map has overflowed), in which
 * case the scan path must run the per-tuple sLog probe.  Returns false only
 * when the bit is provably clear, in which case the scan path may skip the
 * probe for the whole page.
 */
extern bool RecnoDirtyMapCheck(Oid relid, BlockNumber blkno);

#endif							/* RECNO_DIRTYMAP_H */
