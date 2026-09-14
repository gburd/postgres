/*-------------------------------------------------------------------------
 *
 * recno_lock.c
 *	  RECNO locking mechanisms for concurrent access
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/recno/recno_lock.c
 *
 * NOTES
 *	  This implements proper locking for RECNO operations to ensure
 *	  data consistency under concurrent access. Uses both buffer locks
 *	  and tuple-level locks with deadlock detection.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/recno.h"
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
 * recno_tuple_update takes a heavyweight LOCKTAG_TUPLE (NoKeyExclusive) on the
 * row it is about to modify and HOLDS it until the transaction commits, so a
 * concurrent updater of the same row blocks until this transaction is fully
 * committed and then re-evaluates via EvalPlanQual.  RECNO stamps an in-place
 * UPDATE with the transaction's START HLC (RecnoGetDmlTimestamp), not the
 * commit HLC, so a winner that started before the loser can appear
 * "visible" to the loser's snapshot even though it committed AFTER the loser's
 * snapshot -- the HLC comparison alone cannot carry the lost-update signal.
 * The transaction-duration tuple lock (the same lock SELECT ... FOR UPDATE
 * takes) plus the version-chain committer-xid probe
 * (RecnoTupleHasCommittedUpdateAfter) supply what a persistent per-tuple xmax
 * gives heap.
 *
 * These locks must be released at end of transaction, but NOT by the generic
 * LockReleaseAll: it asserts (under cassert) that no LOCKTAG_TUPLE is still
 * held at commit (tuple locks are normally short-lived).  We therefore release
 * them ourselves in RECNO's XACT_EVENT_COMMIT callback, which runs AFTER
 * RecordTransactionCommit + ProcArrayEndTransaction (so the waiter stays
 * blocked until we are fully committed and out of the proc array --
 * conserving the update) but BEFORE ResourceOwnerRelease drops locks and runs
 * the held-at-commit assertion.
 *
 * The set is keyed by (dbid, relnumber, block, offset); the same row updated
 * twice in one transaction is deduplicated so we release exactly the one lock
 * ref we hold.  Backend-local, reset per transaction.
 */
typedef struct RecnoSerializeLockEnt
{
	RelFileNumber relnumber;
	BlockNumber blkno;
	OffsetNumber offnum;
	Oid			dbid;
} RecnoSerializeLockEnt;

#define RECNO_SERLOCK_INLINE 8
static RecnoSerializeLockEnt recno_serlock_inline[RECNO_SERLOCK_INLINE];
static RecnoSerializeLockEnt *recno_serlocks = recno_serlock_inline;
static int	recno_serlock_cap = RECNO_SERLOCK_INLINE;
static int	recno_serlock_n = 0;

/*
 * RecnoTrackSerializeLock -- remember a serialization tuple lock we hold to
 * commit, so RecnoReleaseSerializeLocks() can drop it in the commit callback.
 * Deduplicates: a repeat lock of the same row in this transaction is a
 * lock-manager refcount bump, but we release exactly one ref per distinct row
 * (extra refs release naturally at LockReleaseAll once our explicit release
 * has cleared the last held ref), so we only track and release the first.
 */
void
RecnoTrackSerializeLock(Oid dbid, RelFileNumber relnumber,
						BlockNumber blkno, OffsetNumber offnum)
{
	int			i;

	for (i = 0; i < recno_serlock_n; i++)
		if (recno_serlocks[i].relnumber == relnumber &&
			recno_serlocks[i].blkno == blkno &&
			recno_serlocks[i].offnum == offnum &&
			recno_serlocks[i].dbid == dbid)
			return;				/* already tracked */

	if (recno_serlock_n >= recno_serlock_cap)
	{
		int			newcap = recno_serlock_cap * 2;
		RecnoSerializeLockEnt *newarr =
			MemoryContextAlloc(TopMemoryContext,
							   newcap * sizeof(RecnoSerializeLockEnt));

		memcpy(newarr, recno_serlocks,
			   recno_serlock_n * sizeof(RecnoSerializeLockEnt));
		if (recno_serlocks != recno_serlock_inline)
			pfree(recno_serlocks);
		recno_serlocks = newarr;
		recno_serlock_cap = newcap;
	}

	recno_serlocks[recno_serlock_n].dbid = dbid;
	recno_serlocks[recno_serlock_n].relnumber = relnumber;
	recno_serlocks[recno_serlock_n].blkno = blkno;
	recno_serlocks[recno_serlock_n].offnum = offnum;
	recno_serlock_n++;
}

/*
 * RecnoReleaseSerializeLocks -- release every serialization tuple lock tracked
 * this transaction.  Called from RECNO's XACT_EVENT_COMMIT callback, which
 * runs after the transaction is fully committed but before ResourceOwnerRelease
 * drops locks and runs the held-at-commit assertion.
 *
 * On ABORT we must NOT release here: the abort path calls
 * LockReleaseAll(DEFAULT_LOCKMETHOD, allLocks=true), which both releases these
 * locks and skips the held-at-commit assertion, so a manual release would be a
 * double release ("you don't own a lock").  Abort therefore only clears the
 * tracking set via RecnoForgetSerializeLocks().
 */
void
RecnoReleaseSerializeLocks(void)
{
	int			i;

	for (i = 0; i < recno_serlock_n; i++)
	{
		LOCKTAG		tag;

		SET_LOCKTAG_TUPLE(tag,
						  recno_serlocks[i].dbid,
						  recno_serlocks[i].relnumber,
						  recno_serlocks[i].blkno,
						  recno_serlocks[i].offnum);

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

	RecnoForgetSerializeLocks();
}

/*
 * RecnoForgetSerializeLocks -- drop the tracking set without releasing (abort
 * path: LockReleaseAll has already/will release the locks themselves).
 */
void
RecnoForgetSerializeLocks(void)
{
	if (recno_serlocks != recno_serlock_inline)
		pfree(recno_serlocks);
	recno_serlocks = recno_serlock_inline;
	recno_serlock_cap = RECNO_SERLOCK_INLINE;
	recno_serlock_n = 0;
}

/*
 * RecnoLockTuple
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
 * The caller is responsible for calling RecnoUnlockTuple() to release the
 * lock when done.
 */
bool
RecnoLockTuple(Relation rel, ItemPointer tid, LockTupleMode mode,
			   bool wait, bool *have_tuple_lock)
{
	LOCKTAG		tag;
	LOCKMODE	lockmode;
	bool		result;

	*have_tuple_lock = false;

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
 * RecnoUnlockTuple
 *
 * Release a tuple-level lock previously acquired by RecnoLockTuple().
 *
 * Parameters:
 *   rel  - open relation containing the tuple
 *   tid  - ItemPointer identifying the locked tuple
 *   mode - lock mode that was used when acquiring (must match)
 */
void
RecnoUnlockTuple(Relation rel, ItemPointer tid, LockTupleMode mode)
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
 * RecnoLockPage
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
RecnoLockPage(Relation rel, BlockNumber blkno, LOCKMODE mode)
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
 * RecnoUnlockPage
 *
 * Release a page-level lock previously acquired by RecnoLockPage().
 *
 * Parameters:
 *   rel   - open relation containing the page
 *   blkno - block number to unlock
 *   mode  - lock mode that was used when acquiring (must match)
 */
void
RecnoUnlockPage(Relation rel, BlockNumber blkno, LOCKMODE mode)
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
 * RecnoCheckDeadlock
 *
 * Check whether acquiring a tuple lock would cause a deadlock.
 *
 * This is currently a simplified stub that always returns false (no deadlock).
 * A full implementation would consult the PostgreSQL deadlock detector or
 * maintain RECNO-specific wait-for graph information.
 *
 * Parameters:
 *   rel  - open relation containing the tuple
 *   tid  - ItemPointer identifying the tuple
 *   mode - desired lock mode
 *
 * Returns true if a deadlock would result, false otherwise.
 */
bool
RecnoCheckDeadlock(Relation rel, ItemPointer tid, LockTupleMode mode)
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

	/* Check if acquiring this lock would cause deadlock */
	/* Simplified deadlock check - in practice would use deadlock detector */
	(void) tag;					/* suppress unused warning */
	(void) lockmode;			/* suppress unused warning */
	return false;
}

/*
 * RecnoLockMultipleTuples
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
RecnoLockMultipleTuples(Relation rel, ItemPointerData *tids, int ntids,
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

		if (!RecnoLockTuple(rel, &tids[i], mode, wait, &have_lock))
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
				RecnoUnlockTuple(rel, &tids[j], mode);
		}
	}

	pfree(locked);
	return all_locked;
}

/*
 * RecnoLockRelationForDDL
 *
 * Acquire a relation-level lock for DDL operations (e.g., ALTER TABLE,
 * DROP TABLE).  Delegates to PostgreSQL's standard LockRelationOid().
 *
 * Parameters:
 *   rel      - open relation to lock
 *   lockmode - lock mode (typically AccessExclusiveLock for DDL)
 */
void
RecnoLockRelationForDDL(Relation rel, LOCKMODE lockmode)
{
	/* Use standard relation locking */
	LockRelationOid(RelationGetRelid(rel), lockmode);
}

/*
 * RecnoHoldsTupleLock
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
RecnoHoldsTupleLock(Relation rel, ItemPointer tid, LockTupleMode mode)
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
