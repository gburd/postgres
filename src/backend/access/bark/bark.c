/*-------------------------------------------------------------------------
 *
 * bark.c
 *	  Implementation of the BARK index access method.
 *
 * This is the skeleton registration of the BARK index access method (see
 * BARK-Design.mediawiki).  It declares BARK's capabilities and passes
 * opclass validation so that CREATE INDEX ... USING bark is accepted and
 * validated, but every operation that would read or write index data errors
 * out: the storage format and the search/build machinery arrive in later
 * commits of the series.  Registering the AM first, on its own, keeps each
 * commit independently buildable and testable.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/bark/bark.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/amapi.h"
#include "access/amlocator.h"
#include "access/bark.h"
#include "commands/vacuum.h"
#include "storage/bufmgr.h"
#include "utils/fmgrprotos.h"
#include "utils/selfuncs.h"

/*
 * Every data-touching entry point routes through this: BARK accepts and
 * validates an index definition but cannot yet build, populate, or scan one.
 */
#define BARK_NOT_IMPLEMENTED() \
	ereport(ERROR, \
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED), \
			 errmsg("BARK index access method is not yet implemented")))

static IndexBuildResult *
barkbuild(Relation heap, Relation index, IndexInfo *indexInfo)
{
	return bark_build(heap, index, indexInfo);
}

static void
barkbuildempty(Relation index)
{
	bark_buildempty(index);
}

static bool
barkinsert(Relation index, Datum *values, bool *isnull,
		   ItemPointer ht_ctid, Relation heapRel,
		   IndexUniqueCheck checkUnique, bool indexUnchanged,
		   IndexInfo *indexInfo)
{
	return bark_insert(index, values, isnull, ht_ctid, heapRel,
					   checkUnique, indexUnchanged, indexInfo);
}

static IndexBulkDeleteResult *
barkbulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
			   IndexBulkDeleteCallback callback, void *callback_state)
{
	BARK_NOT_IMPLEMENTED();
	return NULL;
}

static IndexBulkDeleteResult *
barkvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
	Relation	index = info->index;

	/* ANALYZE has nothing to clean up. */
	if (info->analyze_only)
		return stats;

	/*
	 * BARK does not reclaim space or delete pages yet (that arrives with
	 * bulk deletion in a later commit), so cleanup only reports index-wide
	 * statistics.  Returning valid stats lets VACUUM finish and set the heap
	 * visibility map, which is what makes index-only scans worthwhile.
	 *
	 * This is reached only when there were no dead tuples to remove (VACUUM
	 * calls ambulkdelete first otherwise); barkbulkdelete still errors until
	 * deletion is implemented, so a cleanup here never has to account for
	 * tuples a bulk delete claimed to have removed.
	 *
	 * ponytail: stats-only cleanup, no page/FSM reclamation; real reclamation
	 * lands with VACUUM support (A13).
	 */
	if (stats == NULL)
		stats = palloc0_object(IndexBulkDeleteResult);

	stats->num_pages = RelationGetNumberOfBlocks(index);

	return stats;
}

/*
 * Cost estimator.  Use the generic btree-style estimator so the planner can
 * choose a BARK index when it is cheaper than a sequential scan.
 */
static void
barkcostestimate(PlannerInfo *root, IndexPath *path, double loop_count,
				 Cost *indexStartupCost, Cost *indexTotalCost,
				 Selectivity *indexSelectivity, double *indexCorrelation,
				 double *indexPages)
{
	GenericCosts costs = {0};

	genericcostestimate(root, path, loop_count, &costs);

	*indexStartupCost = costs.indexStartupCost;
	*indexTotalCost = costs.indexTotalCost;
	*indexSelectivity = costs.indexSelectivity;
	*indexCorrelation = costs.indexCorrelation;
	*indexPages = costs.numIndexPages;
}

/*
 * No AM-specific reloptions yet.
 */
static bytea *
barkoptions(Datum reloptions, bool validate)
{
	return NULL;
}

/*
 * Opclass validation lives in barkvalidate.c.
 */

static IndexScanDesc
barkbeginscan(Relation r, int nkeys, int norderbys)
{
	return bark_beginscan(r, nkeys, norderbys);
}

static void
barkrescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
		   ScanKey orderbys, int norderbys)
{
	bark_rescan(scan, scankey, nscankeys, orderbys, norderbys);
}

static bool
barkgettuple(IndexScanDesc scan, ScanDirection dir)
{
	return bark_gettuple(scan, dir);
}

static int64
barkgetbitmap(IndexScanDesc scan, TIDBitmap *tbm)
{
	return bark_getbitmap(scan, tbm);
}

static void
barkendscan(IndexScanDesc scan)
{
	bark_endscan(scan);
}

/*
 * BARK index access method handler.
 *
 * Returns an IndexAmRoutine that declares BARK's capabilities.  BARK is an
 * ordered index whose operator families are btree operator families (like
 * the btree AM's), it stores the heap-TID locator, and reserves the KNN
 * (amcanorderbyop) capability for a later phase.  The interface functions are
 * present so opclass validation and CREATE INDEX planning work; the ones that
 * would touch index data error out until their implementing commits land.
 */
Datum
barkhandler(PG_FUNCTION_ARGS)
{
	static const IndexAmRoutine amroutine = {
		.type = T_IndexAmRoutine,
		.amstrategies = 5,
		.amsupport = BARK_NPROCS,
		.amoptsprocnum = 0,
		.amcanorder = true,
		.ambtreeopfamilies = true,
		.amcanorderbyop = false,	/* reserved; implemented in a later phase */
		.amcanhash = false,
		.amconsistentequality = true,
		.amconsistentordering = true,
		.amcanbackward = true,
		.amcanunique = true,
		.amcanmulticol = true,
		.amoptionalkey = true,
		.amsearcharray = true,
		.amsearchnulls = true,
		.amstorage = false,
		.amclusterable = true,
		.ampredlocks = true,
		.amcanparallel = false,
		.amcanbuildparallel = false,
		.amcaninclude = true,
		.amusemaintenanceworkmem = false,
		.amsummarizing = false,
		.amcanlocators = LOCATOR_CAP_MASK(LOCATOR_CAP_TID),
		.amparallelvacuumoptions = VACUUM_OPTION_NO_PARALLEL,
		.amkeytype = InvalidOid,

		.ambuild = barkbuild,
		.ambuildempty = barkbuildempty,
		.aminsert = barkinsert,
		.aminsertcleanup = NULL,
		.ambulkdelete = barkbulkdelete,
		.amvacuumcleanup = barkvacuumcleanup,
		.amcanreturn = bark_canreturn,
		.amcostestimate = barkcostestimate,
		.amgettreeheight = NULL,
		.amoptions = barkoptions,
		.amproperty = NULL,
		.ambuildphasename = NULL,
		.amvalidate = barkvalidate,
		.amadjustmembers = NULL,
		.ambeginscan = barkbeginscan,
		.amrescan = barkrescan,
		.amgettuple = barkgettuple,
		.amgetbitmap = barkgetbitmap,
		.amendscan = barkendscan,
		.ammarkpos = NULL,
		.amrestrpos = NULL,
		.amestimateparallelscan = NULL,
		.aminitparallelscan = NULL,
		.amparallelrescan = NULL,
		.amtranslatestrategy = bark_translate_strategy,
		.amtranslatecmptype = bark_translate_cmptype,
	};

	PG_RETURN_POINTER(&amroutine);
}
