/*-------------------------------------------------------------------------
 *
 * barkvacuum.c
 *	  BARK's registry of the vacuum cycle IDs of active VACUUMs, and the
 *	  horizon after which simple deletion trusts LP_DEAD marks again.
 *
 * A leaf split that happens while VACUUM is scanning an index stamps both
 * halves with that VACUUM's cycle ID, so that VACUUM can tell entries moved
 * to a right sibling at a block it has already passed (see "VACUUM" in the
 * README).  Each active VACUUM of a BARK index gets a cycle ID here; a split
 * asks for the one of its index, and gets zero when no VACUUM is running.
 *
 * nbtree keeps a dense array of (index, cycle ID) pairs (nbtutils.c's
 * btvacinfo); BARK keeps one slot per backend instead, indexed by
 * MyProcNumber, so that starting and ending a VACUUM are a store and there
 * is no slot to run out of.  A split's lookup reads every slot under a
 * shared LWLock; that is one word per PGPROC, and leaf splits are rare.
 * BARK depends on no nbtree internals for any of it.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/bark/barkvacuum.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <time.h>

#include "access/bark.h"
#include "access/transam.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "storage/subsystems.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

typedef struct BarkOneVacInfo
{
	uint64		relkey;			/* the index (bark_vac_key), or 0: no VACUUM */
	BarkCycleId cycleid;		/* its VACUUM's cycle ID */
}			BarkOneVacInfo;

typedef struct BarkVacInfo
{
	/* bark_end_of_recovery's FullTransactionId, or 0 (see bark_marks_trusted) */
	pg_atomic_uint64 marks_horizon;
	/* level-1 pages deleted so far, by any index (see bark_l1_epoch) */
	pg_atomic_uint64 l1epoch;
	uint32		cycle_ctr;		/* how many cycle IDs have been given out */
	BarkOneVacInfo vacuums[FLEXIBLE_ARRAY_MEMBER];	/* by ProcNumber */
}			BarkVacInfo;

static BarkVacInfo * barkvacinfo;

static void BarkShmemRequest(void *arg);
static void BarkShmemInit(void *arg);

const ShmemCallbacks BarkShmemCallbacks = {
	.request_fn = BarkShmemRequest,
	.init_fn = BarkShmemInit,
};

/* Every PGPROC, so any backend's MyProcNumber has a slot. */
#define BARK_VAC_SLOTS	(MaxBackends + NUM_AUXILIARY_PROCS)

/*
 * The index's LockRelId as one word.  No BARK index is a shared relation
 * (dbId 0) with relId 0, so 0 means a free slot.
 */
static inline uint64
bark_vac_key(Relation rel)
{
	LockRelId	relid = rel->rd_lockInfo.lockRelId;

	return ((uint64) relid.dbId << 32) | relid.relId;
}

static Size
BarkVacInfoSize(void)
{
	return add_size(offsetof(BarkVacInfo, vacuums),
					mul_size(BARK_VAC_SLOTS, sizeof(BarkOneVacInfo)));
}

/*
 * The cycle ID of the VACUUM running on rel, or zero when none is.
 *
 * The caller holds pin and exclusive lock on every buffer it will stamp, so
 * a VACUUM that starts just after this returns cannot read those pages until
 * the split is done.
 */
BarkCycleId
bark_vacuum_cycleid(Relation rel)
{
	uint64		key = bark_vac_key(rel);
	BarkCycleId result = 0;

	LWLockAcquire(BarkVacuumLock, LW_SHARED);
	for (int i = 0; i < BARK_VAC_SLOTS; i++)
	{
		if (barkvacinfo->vacuums[i].relkey == key)
		{
			result = barkvacinfo->vacuums[i].cycleid;
			break;
		}
	}
	LWLockRelease(BarkVacuumLock);
	return result;
}

/*
 * Give a VACUUM of rel that is starting a cycle ID, in this backend's slot.
 * The caller must call bark_end_vacuum, even on FATAL, which
 * PG_ENSURE_ERROR_CLEANUP with bark_end_vacuum_callback guarantees.  (Only
 * one backend can VACUUM an index at a time, so one slot per backend is
 * enough and no two slots name the same index.)
 */
BarkCycleId
bark_start_vacuum(Relation rel)
{
	BarkOneVacInfo *vac = &barkvacinfo->vacuums[MyProcNumber];
	BarkCycleId result;

	Assert(MyProcNumber >= 0 && MyProcNumber < BARK_VAC_SLOTS);
	LWLockAcquire(BarkVacuumLock, LW_EXCLUSIVE);
	/* 1 .. BARK_MAX_CYCLE_ID: never zero (no VACUUM) */
	result = (BarkCycleId) (barkvacinfo->cycle_ctr++ % BARK_MAX_CYCLE_ID + 1);
	vac->relkey = bark_vac_key(rel);
	vac->cycleid = result;
	LWLockRelease(BarkVacuumLock);
	return result;
}

/*
 * This backend's VACUUM is done.  Its slot may already be clear, so that the
 * caller may call this whether or not bark_start_vacuum ran.
 */
void
bark_end_vacuum(void)
{
	Assert(MyProcNumber >= 0 && MyProcNumber < BARK_VAC_SLOTS);
	LWLockAcquire(BarkVacuumLock, LW_EXCLUSIVE);
	barkvacinfo->vacuums[MyProcNumber].relkey = 0;
	LWLockRelease(BarkVacuumLock);
}

/* bark_end_vacuum as a PG_ENSURE_ERROR_CLEANUP callback. */
void
bark_end_vacuum_callback(int code, Datum arg)
{
	bark_end_vacuum();
}

static void
BarkShmemRequest(void *arg)
{
	ShmemRequestStruct(.name = "BARK Vacuum State",
					   .size = BarkVacInfoSize(),
					   .ptr = (void **) &barkvacinfo,
		);
}

static void
BarkShmemInit(void *arg)
{
	memset(barkvacinfo, 0, BarkVacInfoSize());
	pg_atomic_init_u64(&barkvacinfo->marks_horizon, 0);
	pg_atomic_init_u64(&barkvacinfo->l1epoch, 0);
	/* Start the counter somewhere that differs from run to run. */
	barkvacinfo->cycle_ctr = (uint32) time(NULL);
}

/*
 * The level-1 epoch: a count of the deletions that removed a level-1 page,
 * in any BARK index of the cluster.  A backend's level-1 entry table
 * (BarkAmCache) remembers level-1 pages across transactions, so no snapshot
 * keeps their blocks from being recycled; a slot whose block was deleted and
 * then reused as another level-1 page, further right, would send a descent to
 * the wrong page.  So the table records the epoch it was filled under, and a
 * descent that starts at a slot reads the epoch again once it holds the
 * slot's page locked, and forgets the table when it changed.
 *
 * A deletion advances the epoch while it still holds the deleted level-1 page
 * locked, and so does its redo.  A backend that finds the block recycled
 * locked it after the recycler did, who locked it after the deleter unlocked
 * it, so it reads the advanced value: the atomic increment and the buffer
 * locks are full barriers.  One counter serves every index; a deletion makes
 * every backend refill its tables once, which costs a descent from the root
 * per level-1 page each backend goes on to use.
 */
uint64
bark_l1_epoch(void)
{
	return pg_atomic_read_u64(&barkvacinfo->l1epoch);
}

/* A level-1 page is being deleted (see bark_l1_epoch). */
void
bark_l1_epoch_advance(void)
{
	pg_atomic_fetch_add_u64(&barkvacinfo->l1epoch, 1);
}

/*
 * Recovery that let transactions begin (hot standby) is ending, and the next
 * XID is `next` (bark_xlog_cleanup, in the startup process).  See
 * bark_marks_trusted.
 */
void
bark_end_of_recovery(FullTransactionId next)
{
	pg_atomic_write_u64(&barkvacinfo->marks_horizon,
						U64FromFullTransactionId(next));
}

/*
 * May simple deletion take the LP_DEAD marks on index pages as the table
 * AM's word that their rows are dead to every snapshot?
 *
 * A mark is set by the horizon of the server that sets it.  A standby gets
 * its primary's marks in page images, and the primary's horizon does not
 * count the standby's snapshots, so a transaction that began during
 * recovery does not skip marked entries (RelationGetIndexScan sets
 * ignore_killed_tuples false).  Such a transaction goes on after promotion.
 * If simple deletion then deleted a marked entry without asking the table
 * AM, that transaction's index scans would miss a row its snapshot sees.
 *
 * Every snapshot taken during recovery has an xmin no newer than the next
 * XID at its end, which bark_end_of_recovery recorded; one whose xmin is
 * exactly that XID saw every deletion the old primary's marks stand for.
 * So once the XID before it is removable by the shared horizon, no snapshot
 * that could see a marked row is left, and the marks are trustworthy again,
 * for good: the value is cleared.  It must be the shared horizon, which
 * counts every database's snapshots: the value is not per database, and a
 * transaction started during recovery may be in any of them.  Until then
 * simple deletion leaves the page alone.  (A transaction prepared before
 * the promotion, or a replication slot, holds the horizon back too, as it
 * holds back pruning.)
 */
bool
bark_marks_trusted(void)
{
	uint64		horizon = pg_atomic_read_u64(&barkvacinfo->marks_horizon);
	FullTransactionId last;

	if (horizon == 0)
		return true;			/* no hot standby recovery ended here */
	last = FullTransactionIdFromU64(horizon);
	FullTransactionIdRetreat(&last);
	if (!GlobalVisCheckRemovableFullXid(NULL, last))
		return false;
	pg_atomic_write_u64(&barkvacinfo->marks_horizon, 0);
	return true;
}
