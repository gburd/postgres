/*-------------------------------------------------------------------------
 *
 * recno_dirtymap.h
 *	  Shared-memory dirty block map for the RECNO table access method.
 *
 * The dirty map tracks which heap pages have uncommitted in-place updates.
 * If a page is NOT in the dirty map, all tuples on it are committed and
 * the scan path can skip per-tuple sLog lookups (fast path).
 *
 * The map is a shared hash table keyed on (relid, blkno) with a reference
 * count (dirty_count) tracking how many uncommitted modifications target
 * that page.  Each backend maintains a local tracking list so it can
 * decrement on commit or discard on abort.
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
#include "access/xact.h"

/* Shared memory sizing and initialization */
extern Size RecnoDirtyMapShmemSize(void);
extern void RecnoDirtyMapShmemInit(void);
extern const ShmemCallbacks RecnoDirtyMapShmemCallbacks;

/*
 * Per-relation map lifecycle.
 *
 * Open/Close maintain a per-relation reference count so we know when the
 * last scan finishes and can potentially prune stale entries.  Extend is
 * called when new blocks are added to a relation (e.g., by the FSM layer).
 */
extern void RecnoDirtyMapOpen(Oid relid, BlockNumber nblocks);
extern void RecnoDirtyMapClose(Oid relid);
extern void RecnoDirtyMapExtend(Oid relid, BlockNumber nblocks);

/*
 * Mark a block as dirty (called during INSERT/UPDATE).
 *
 * RecnoDirtyMapIncrement increments the shared dirty_count for the page.
 * RecnoDirtyMapTrackIncrement records the increment in the backend-local
 * tracking list so we can reverse it at commit time.
 */
extern void RecnoDirtyMapIncrement(Oid relid, BlockNumber blkno);
extern void RecnoDirtyMapTrackIncrement(Oid relid, BlockNumber blkno);

/*
 * Per-transaction cleanup.
 *
 * RecnoDirtyMapDecrementTracked: at COMMIT, decrement dirty_count for
 * each block this transaction dirtied and remove entries that reach zero.
 *
 * RecnoDirtyMapDiscardTracked: at ABORT, discard the backend-local tracking
 * without decrementing (the sLog will detect aborted state; the dirty map
 * entries remain until some other transaction's commit cleans them, or
 * until a future VACUUM pass).
 *
 * Note on abort semantics: we do NOT decrement on abort because the block
 * may still have uncommitted tuples from OTHER transactions.  Leaving the
 * count slightly elevated is conservative-correct -- it just means the scan
 * path will consult the sLog for that page until the count drops to zero.
 */
extern void RecnoDirtyMapDecrementTracked(void);
extern void RecnoDirtyMapDiscardTracked(void);

/*
 * Subtransaction support.
 *
 * On subtransaction abort, discard tracking entries for that subtxn.
 * On subtransaction commit, reparent entries to the parent subtxn.
 */
extern void RecnoDirtyMapDiscardTrackedSubXact(SubTransactionId subxid);
extern void RecnoDirtyMapReparentTrackedSubXact(SubTransactionId child,
												SubTransactionId parent);

/*
 * Query: is block dirty?
 *
 * Returns true if the block has one or more uncommitted modifications.
 * Used by the scan path to decide whether per-tuple sLog lookups are needed.
 */
extern bool RecnoDirtyMapCheck(Oid relid, BlockNumber blkno);

#endif							/* RECNO_DIRTYMAP_H */
