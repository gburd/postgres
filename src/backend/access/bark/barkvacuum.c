/*-------------------------------------------------------------------------
 *
 * barkvacuum.c
 *	  BARK's registry of the vacuum cycle IDs of active VACUUMs.
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
#include "miscadmin.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "storage/subsystems.h"
#include "utils/rel.h"

typedef struct BarkOneVacInfo
{
	uint64		relkey;			/* the index (bark_vac_key), or 0: no VACUUM */
	BarkCycleId cycleid;		/* its VACUUM's cycle ID */
} BarkOneVacInfo;

typedef struct BarkVacInfo
{
	uint32		cycle_ctr;		/* how many cycle IDs have been given out */
	BarkOneVacInfo vacuums[FLEXIBLE_ARRAY_MEMBER];	/* by ProcNumber */
} BarkVacInfo;

static BarkVacInfo *barkvacinfo;

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
	/* Start the counter somewhere that differs from run to run. */
	barkvacinfo->cycle_ctr = (uint32) time(NULL);
}
