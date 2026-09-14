/*-------------------------------------------------------------------------
 *
 * flux_lock.c
 *	  FLUX locking mechanisms for concurrent access
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/flux/flux_lock.c
 *
 * NOTES
 *	  This implements proper locking for FLUX operations to ensure
 *	  data consistency under concurrent access. Uses both buffer locks
 *	  and tuple-level locks with deadlock detection.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/flux.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "utils/rel.h"
#include "miscadmin.h"
#include "access/tableam.h"
#include "utils/memutils.h"

/*
 * Deferred release of the lost-update SERIALIZATION tuple locks.
 *
 * flux_tuple_update_inplace takes a heavyweight LOCKTAG_TUPLE
 * (NoKeyExclusive) on the row it is about to modify and HOLDS it until the
 * transaction commits, so a concurrent updater of the same row blocks until
 * this transaction is fully committed and then re-evaluates via EvalPlanQual
 * (FLUX has no persistent per-tuple xmax to carry that signal, so the tuple
 * lock does -- the same transaction-duration row lock SELECT ... FOR UPDATE
 * uses).  These locks must be released at end of transaction, but NOT by the
 * generic LockReleaseAll: it asserts (under cassert) that no LOCKTAG_TUPLE is
 * still held at commit (tuple locks are normally short-lived).  We therefore
 * release them ourselves in FLUX's XACT_EVENT_COMMIT / _ABORT callback, which
 * runs AFTER RecordTransactionCommit + ProcArrayEndTransaction (so the waiter
 * stays blocked until we are fully committed and out of the proc array --
 * conserving the update) but BEFORE ResourceOwnerRelease drops locks and
 * runs the held-at-commit assertion.
 *
 * The set is keyed by (relnumber, block, offset); the same row updated twice
 * in one transaction is deduplicated so we release exactly the one lock ref
 * we hold.  Backend-local, reset per transaction.
 */
typedef struct FluxSerializeLockEnt
{
	RelFileNumber relnumber;
	BlockNumber blkno;
	OffsetNumber offnum;
	Oid			dbid;
} FluxSerializeLockEnt;

#define FLUX_SERLOCK_INLINE 8
static FluxSerializeLockEnt flux_serlock_inline[FLUX_SERLOCK_INLINE];
static FluxSerializeLockEnt *flux_serlocks = flux_serlock_inline;
static int	flux_serlock_cap = FLUX_SERLOCK_INLINE;
static int	flux_serlock_n = 0;

/*
 * FluxTrackSerializeLock -- remember a serialization tuple lock we hold to
 * commit, so FluxReleaseSerializeLocks() can drop it in the commit callback.
 * Deduplicates: a repeat lock of the same row in this transaction is a
 * lock-manager refcount bump, but we release exactly one ref per distinct row
 * (the extra refs release naturally at LockReleaseAll, which does not assert
 * once our explicit release has cleared the last one -- so we only track and
 * release the first).  Returns nothing.
 */
void
FluxTrackSerializeLock(Oid dbid, RelFileNumber relnumber,
					   BlockNumber blkno, OffsetNumber offnum)
{
	int			i;

	for (i = 0; i < flux_serlock_n; i++)
		if (flux_serlocks[i].relnumber == relnumber &&
			flux_serlocks[i].blkno == blkno &&
			flux_serlocks[i].offnum == offnum &&
			flux_serlocks[i].dbid == dbid)
			return;				/* already tracked */

	if (flux_serlock_n >= flux_serlock_cap)
	{
		int			newcap = flux_serlock_cap * 2;
		FluxSerializeLockEnt *newarr =
			MemoryContextAlloc(TopMemoryContext,
							   newcap * sizeof(FluxSerializeLockEnt));

		memcpy(newarr, flux_serlocks,
			   flux_serlock_n * sizeof(FluxSerializeLockEnt));
		if (flux_serlocks != flux_serlock_inline)
			pfree(flux_serlocks);
		flux_serlocks = newarr;
		flux_serlock_cap = newcap;
	}

	flux_serlocks[flux_serlock_n].dbid = dbid;
	flux_serlocks[flux_serlock_n].relnumber = relnumber;
	flux_serlocks[flux_serlock_n].blkno = blkno;
	flux_serlocks[flux_serlock_n].offnum = offnum;
	flux_serlock_n++;
}

/*
 * FluxReleaseSerializeLocks -- release every serialization tuple lock tracked
 * this transaction.  Called from FLUX's XACT_EVENT_COMMIT callback, which runs
 * after the transaction is fully committed but before ResourceOwnerRelease
 * drops locks and runs the held-at-commit assertion.
 *
 * On ABORT we must NOT release here: the abort path calls
 * LockReleaseAll(DEFAULT_LOCKMETHOD, allLocks=true), which both releases these
 * locks and skips the held-at-commit assertion, so a manual release would be a
 * double release ("you don't own a lock").  Abort therefore only clears the
 * tracking set via FluxForgetSerializeLocks().
 */
void
FluxReleaseSerializeLocks(void)
{
	int			i;

	for (i = 0; i < flux_serlock_n; i++)
	{
		LOCKTAG		tag;

		SET_LOCKTAG_TUPLE(tag,
						  flux_serlocks[i].dbid,
						  flux_serlocks[i].relnumber,
						  flux_serlocks[i].blkno,
						  flux_serlocks[i].offnum);

		/*
		 * Release only if we still hold it.  A serialization lock acquired
		 * inside a subtransaction that later rolled back was already dropped
		 * by that subxact's LockReleaseAll; releasing again would warn ("you
		 * don't own a lock").  LockHeldByMe keeps the commit-time release
		 * idempotent across savepoint rollback.
		 */
		if (LockHeldByMe(&tag, ExclusiveLock, false))
			LockRelease(&tag, ExclusiveLock, false);
	}

	FluxForgetSerializeLocks();
}

/*
 * FluxForgetSerializeLocks -- drop the tracking set without releasing (abort
 * path: LockReleaseAll has already/​will release the locks themselves).
 */
void
FluxForgetSerializeLocks(void)
{
	if (flux_serlocks != flux_serlock_inline)
		pfree(flux_serlocks);
	flux_serlocks = flux_serlock_inline;
	flux_serlock_cap = FLUX_SERLOCK_INLINE;
	flux_serlock_n = 0;
}

/*
 * FluxLockTuple
 *
 * Acquire a tuple-level lock on the specified tuple using PostgreSQL's
 * standard LOCKTAG_TUPLE mechanism.  The lock mode is converted from
 * LockTupleMode to the corresponding LOCKMODE (ShareLock for read modes,
 * ExclusiveLock for write modes).
 *
 * Parameters:
 *   rel             - open relation containing the tuple
 *   tid             - ItemPointer identifying the tuple (block + offset)
 *   mode            - desired lock strength (LockTupleKeyShare through
 *                     LockTupleExclusive)
 *   wait            - if true, block until the lock is available; if false,
 *                     return false immediately if the lock cannot be acquired
 *   have_tuple_lock - output: set to true if the lock was successfully acquired
 *
 * Returns true if the lock was acquired, false if 'wait' was false and the
 * lock was not available.
 *
 * The caller is responsible for calling FluxUnlockTuple() to release the
 * lock when done.
 */
bool
FluxLockTuple(Relation rel, ItemPointer tid, LockTupleMode mode,
			  bool wait, bool *have_tuple_lock)
{
	LOCKTAG		tag;
	LOCKMODE	lockmode;
	bool		result;

	*have_tuple_lock = false;

	/*
	 * Convert tuple lock mode to standard lock mode using the same mapping as
	 * heap (tupleLockExtraInfo in heapam.c).  The four modes MUST map to four
	 * distinct LOCKMODEs: collapsing KeyShare/Share or
	 * NoKeyExclusive/Exclusive makes an FK key-share lock conflict with a
	 * concurrent no-key UPDATE, manufacturing deadlocks that heap never
	 * suffers.
	 */
	switch (mode)
	{
		case LockTupleKeyShare:
			lockmode = AccessShareLock;
			break;
		case LockTupleShare:
			lockmode = RowShareLock;
			break;
		case LockTupleNoKeyExclusive:
			lockmode = ExclusiveLock;
			break;
		case LockTupleExclusive:
			lockmode = AccessExclusiveLock;
			break;
		default:
			elog(ERROR, "invalid tuple lock mode: %d", mode);
	}

	/* Set up lock tag for tuple */
	SET_LOCKTAG_TUPLE(tag,
					  rel->rd_locator.dbOid,
					  rel->rd_locator.relNumber,
					  ItemPointerGetBlockNumber(tid),
					  ItemPointerGetOffsetNumber(tid));

	/* Acquire the lock */
	if (wait)
	{
		LockAcquire(&tag, lockmode, false, false);
		result = true;
	}
	else
	{
		result = (LockAcquireExtended(&tag, lockmode, false, true, true, NULL, false) != LOCKACQUIRE_NOT_AVAIL);
	}

	if (result)
		*have_tuple_lock = true;

	return result;
}

/*
 * FluxUnlockTuple
 *
 * Release a tuple-level lock previously acquired by FluxLockTuple().
 *
 * Parameters:
 *   rel  - open relation containing the tuple
 *   tid  - ItemPointer identifying the locked tuple
 *   mode - lock mode that was used when acquiring (must match)
 */
void
FluxUnlockTuple(Relation rel, ItemPointer tid, LockTupleMode mode)
{
	LOCKTAG		tag;
	LOCKMODE	lockmode;

	/*
	 * Convert tuple lock mode to standard lock mode using the same mapping as
	 * heap (tupleLockExtraInfo in heapam.c).  The four modes MUST map to four
	 * distinct LOCKMODEs: collapsing KeyShare/Share or
	 * NoKeyExclusive/Exclusive makes an FK key-share lock conflict with a
	 * concurrent no-key UPDATE, manufacturing deadlocks that heap never
	 * suffers.
	 */
	switch (mode)
	{
		case LockTupleKeyShare:
			lockmode = AccessShareLock;
			break;
		case LockTupleShare:
			lockmode = RowShareLock;
			break;
		case LockTupleNoKeyExclusive:
			lockmode = ExclusiveLock;
			break;
		case LockTupleExclusive:
			lockmode = AccessExclusiveLock;
			break;
		default:
			elog(ERROR, "invalid tuple lock mode: %d", mode);
	}

	/* Set up lock tag for tuple */
	SET_LOCKTAG_TUPLE(tag,
					  rel->rd_locator.dbOid,
					  rel->rd_locator.relNumber,
					  ItemPointerGetBlockNumber(tid),
					  ItemPointerGetOffsetNumber(tid));

	/* Release the lock */
	LockRelease(&tag, lockmode, false);
}

/*
 * FluxLockPage
 *
 * Acquire a page-level lock using LOCKTAG_PAGE.  This is used for operations
 * that need exclusive access to an entire page's structure, such as
 * defragmentation or cross-page tuple moves.
 *
 * Note: This is distinct from buffer-level locking (LockBuffer).  Buffer
 * locks protect the in-memory page image; page-level locks here protect
 * the logical page structure across multiple buffer accesses.
 *
 * Parameters:
 *   rel   - open relation containing the page
 *   blkno - block number to lock
 *   mode  - lock mode (typically ShareLock or ExclusiveLock)
 */
void
FluxLockPage(Relation rel, BlockNumber blkno, LOCKMODE mode)
{
	LOCKTAG		tag;

	/* Set up lock tag for page */
	SET_LOCKTAG_PAGE(tag,
					 rel->rd_locator.dbOid,
					 rel->rd_locator.relNumber,
					 blkno);

	/* Acquire the lock */
	LockAcquire(&tag, mode, false, false);
}

/*
 * FluxUnlockPage
 *
 * Release a page-level lock previously acquired by FluxLockPage().
 *
 * Parameters:
 *   rel   - open relation containing the page
 *   blkno - block number to unlock
 *   mode  - lock mode that was used when acquiring (must match)
 */
void
FluxUnlockPage(Relation rel, BlockNumber blkno, LOCKMODE mode)
{
	LOCKTAG		tag;

	/* Set up lock tag for page */
	SET_LOCKTAG_PAGE(tag,
					 rel->rd_locator.dbOid,
					 rel->rd_locator.relNumber,
					 blkno);

	/* Release the lock */
	LockRelease(&tag, mode, false);
}


/*
 * FluxLockMultipleTuples
 *
 * Acquire tuple-level locks on multiple tuples in a consistent order to
 * prevent deadlocks.  The TIDs are sorted (using bubble sort, which is
 * adequate since N is typically small) before acquiring locks, ensuring
 * that all callers acquire locks in the same global order.
 *
 * If any lock acquisition fails (when wait=false), all previously acquired
 * locks are released and the function returns false.
 *
 * Note: The tids array is sorted in-place, which modifies the caller's array.
 *
 * Parameters:
 *   rel   - open relation containing the tuples
 *   tids  - array of ItemPointerData identifying tuples to lock (sorted in-place)
 *   ntids - number of entries in tids array
 *   mode  - desired lock strength for all tuples
 *   wait  - if true, block until all locks are available
 *
 * Returns true if all locks were acquired, false if any could not be acquired.
 */
bool
FluxLockMultipleTuples(Relation rel, ItemPointerData *tids, int ntids,
					   LockTupleMode mode, bool wait)
{
	int			i,
				j;
	bool		all_locked = true;
	bool	   *locked = palloc0(sizeof(bool) * ntids);

	/* Sort TIDs to ensure consistent lock ordering */
	for (i = 0; i < ntids - 1; i++)
	{
		for (j = i + 1; j < ntids; j++)
		{
			if (ItemPointerCompare(&tids[i], &tids[j]) > 0)
			{
				ItemPointerData temp = tids[i];

				tids[i] = tids[j];
				tids[j] = temp;
			}
		}
	}

	/* Acquire locks in sorted order */
	for (i = 0; i < ntids; i++)
	{
		bool		have_lock;

		if (!FluxLockTuple(rel, &tids[i], mode, wait, &have_lock))
		{
			all_locked = false;
			break;
		}
		locked[i] = have_lock;
	}

	/* If we failed to get all locks, release what we got */
	if (!all_locked)
	{
		for (j = 0; j < i; j++)
		{
			if (locked[j])
				FluxUnlockTuple(rel, &tids[j], mode);
		}
	}

	pfree(locked);
	return all_locked;
}

/*
 * FluxLockRelationForDDL
 *
 * Acquire a relation-level lock for DDL operations (e.g., ALTER TABLE,
 * DROP TABLE).  Delegates to PostgreSQL's standard LockRelationOid().
 *
 * Parameters:
 *   rel      - open relation to lock
 *   lockmode - lock mode (typically AccessExclusiveLock for DDL)
 */
void
FluxLockRelationForDDL(Relation rel, LOCKMODE lockmode)
{
	/* Use standard relation locking */
	LockRelationOid(RelationGetRelid(rel), lockmode);
}

/*
 * FluxHoldsTupleLock
 *
 * Check whether the current transaction already holds a lock on the
 * specified tuple at the given mode.  This is useful for avoiding redundant
 * lock acquisitions and for assertions in debug builds.
 *
 * Parameters:
 *   rel  - open relation containing the tuple
 *   tid  - ItemPointer identifying the tuple
 *   mode - lock mode to check for
 *
 * Returns true if the current transaction holds the specified lock.
 */
bool
FluxHoldsTupleLock(Relation rel, ItemPointer tid, LockTupleMode mode)
{
	LOCKTAG		tag;
	LOCKMODE	lockmode;

	/* Convert tuple lock mode to standard lock mode */
	switch (mode)
	{
		case LockTupleKeyShare:
		case LockTupleShare:
			lockmode = ShareLock;
			break;
		case LockTupleNoKeyExclusive:
		case LockTupleExclusive:
			lockmode = ExclusiveLock;
			break;
		default:
			return false;
	}

	/* Set up lock tag for tuple */
	SET_LOCKTAG_TUPLE(tag,
					  rel->rd_locator.dbOid,
					  rel->rd_locator.relNumber,
					  ItemPointerGetBlockNumber(tid),
					  ItemPointerGetOffsetNumber(tid));

	/* Check if we hold the lock */
	return LockHeldByMe(&tag, lockmode, false);
}
