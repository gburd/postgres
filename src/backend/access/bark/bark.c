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

#include <math.h>

#include "access/amapi.h"
#include "access/amlocator.h"
#include "access/bark.h"
#include "access/generic_xlog.h"
#include "commands/vacuum.h"
#include "storage/bufmgr.h"
#include "utils/fmgrprotos.h"
#include "utils/selfuncs.h"
#include "utils/spccache.h"

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
	Relation	index = info->index;
	BlockNumber npages;

	if (stats == NULL)
		stats = palloc0_object(IndexBulkDeleteResult);

	/*
	 * Scan every leaf page and delete the entries whose heap TID the callback
	 * reports dead.  Only leaf pages hold heap TIDs; the meta page and
	 * internal (pivot) pages are skipped.  Each page is modified and WAL-
	 * logged under its own generic-WAL record.
	 *
	 * ponytail: a linear scan of the whole index (like bloom and GIN) rather
	 * than tracking which pages hold dead TIDs; and no page is emptied/
	 * recycled yet -- an all-dead leaf is left in place.  Page reclamation and
	 * right-link repair are a later commit; leaving a now-empty but still
	 * linked leaf is correct, just not space-optimal.
	 */
	npages = RelationGetNumberOfBlocks(index);
	for (BlockNumber blkno = BARK_METAPAGE + 1; blkno < npages; blkno++)
	{
		Buffer		buf;
		Page		page;
		BarkPageOpaque opaque;
		OffsetNumber maxoff;
		OffsetNumber todelete[MaxOffsetNumber];
		int			ndelete = 0;
		int			ndelete_single = 0;
		BlockNumber oversized_free[MaxOffsetNumber];
		int			noversized_free = 0;
		GenericXLogState *gstate = NULL;
		Page		p = NULL;

		vacuum_delay_point(false);

		buf = ReadBufferExtended(index, MAIN_FORKNUM, blkno, RBM_NORMAL,
								 info->strategy);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
		page = BufferGetPage(buf);

		/* Only leaf pages carry heap TIDs (the meta page, block 0, is skipped). */
		if (PageIsNew(page) || !BarkPageIsLeaf(BarkPageGetOpaque(page)))
		{
			UnlockReleaseBuffer(buf);
			continue;
		}

		opaque = BarkPageGetOpaque(page);
		maxoff = PageGetMaxOffsetNumber(page);

		/*
		 * First pass: shrink LIST entries that lost some (but not all) members
		 * in place with PageIndexTupleOverwrite, which keeps every offset
		 * number stable (it only moves the item data).  Fully-dead entries
		 * (SINGLE whose TID is dead, or LIST whose every member is dead) are
		 * recorded for the MultiDelete pass below.  Doing the shrinks first and
		 * the deletes last means the recorded offsets stay valid until the
		 * single MultiDelete renumbers them.
		 */
		for (OffsetNumber off = BarkPageFirstDataKey(opaque);
			 off <= maxoff; off = OffsetNumberNext(off))
		{
			ItemId		iid = PageGetItemId(page, off);
			IndexTuple	itup = (IndexTuple) PageGetItem(page, iid);

			if (BarkEntryGetShape(itup) == BARK_SHAPE_SINGLE)
			{
				if (callback(&itup->t_tid, callback_state))
				{
					todelete[ndelete++] = off;
					ndelete_single++;
				}
			}
			else if (BarkEntryGetShape(itup) == BARK_SHAPE_OVERSIZED)
			{
				/*
				 * An OVERSIZED entry holds exactly one heap locator (inline in
				 * its ref).  If it is dead, delete the leaf entry and remember
				 * its overflow chain to free after the leaf WAL record finishes
				 * (freeing the chain starts its own WAL records, which cannot
				 * nest inside the leaf's generic-WAL state).
				 */
				ItemPointerData loc = BarkOverflowGetRef(itup)->locator;

				if (callback(&loc, callback_state))
				{
					oversized_free[noversized_free++] =
						BarkOverflowGetFirstBlock(itup);
					todelete[ndelete++] = off;
					ndelete_single++;
				}
			}
			else
			{
				int			ntids = bark_entry_count_tids(itup);
				ItemPointer tids = (ItemPointer)
					palloc(ntids * sizeof(ItemPointerData));
				int			nlive = 0;

				ntids = bark_entry_get_tids(itup, tids, ntids);
				for (int i = 0; i < ntids; i++)
				{
					if (!callback(&tids[i], callback_state))
						tids[nlive++] = tids[i];
				}

				if (nlive == ntids)
				{
					pfree(tids);
					continue;	/* nothing dead in this entry */
				}

				stats->tuples_removed += ntids - nlive;

				if (nlive == 0)
				{
					todelete[ndelete++] = off;	/* whole entry dies */
					pfree(tids);
					continue;
				}

				if (gstate == NULL)
				{
					gstate = GenericXLogStart(index);
					p = GenericXLogRegisterBuffer(gstate, buf, 0);
					page = p;	/* overwrite/delete on the registered copy */
				}

				{
					/*
					 * Rebuild with the surviving members, re-choosing the shape:
					 * a POSTING if its sbm still wins, else a LIST, else a plain
					 * SINGLE when exactly one survives.  Removing members only
					 * shrinks the set, so the new entry is no larger than the
					 * original and PageIndexTupleOverwrite fits in place.
					 */
					IndexTuple	key = bark_single_from_list(index, itup, NULL);
					IndexTuple	newentry;
					IndexTuple	posting;

					if (nlive == 1)
					{
						newentry = key;
						newentry->t_tid = tids[0];
					}
					else if ((posting = bark_form_posting(RelationGetDescr(index),
															  key, tids, nlive)) != NULL)
					{
						newentry = posting;
						pfree(key);
					}
					else
					{
						newentry = bark_form_list(RelationGetDescr(index), key,
												  tids, nlive);
						pfree(key);
					}
					if (!PageIndexTupleOverwrite(page, off, (char *) newentry,
												 IndexTupleSize(newentry)))
						elog(ERROR, "failed to shrink BARK leaf entry during vacuum");
					pfree(newentry);
				}
				pfree(tids);
			}
		}

		if (ndelete > 0)
		{
			if (gstate == NULL)
			{
				gstate = GenericXLogStart(index);
				p = GenericXLogRegisterBuffer(gstate, buf, 0);
				page = p;
			}
			PageIndexMultiDelete(page, todelete, ndelete);
			stats->tuples_removed += ndelete_single;
		}

		if (gstate != NULL)
			GenericXLogFinish(gstate);

		UnlockReleaseBuffer(buf);

		/*
		 * Reclaim the overflow chains of the OVERSIZED entries just deleted,
		 * now that the leaf no longer references them and its WAL record is
		 * durable.  Each chain is freed under its own generic-WAL records.
		 */
		for (int i = 0; i < noversized_free; i++)
		{
			ItemPointerData dummy;
			IndexTuple	stub;

			/*
			 * bark_free_oversized reads only the first-block field of the
			 * entry's t_tid, so a tiny stub carrying that block is enough.
			 */
			ItemPointerSetBlockNumber(&dummy, oversized_free[i]);
			ItemPointerSetOffsetNumber(&dummy, (OffsetNumber) BARK_IS_OVERFLOW);
			stub = (IndexTuple) palloc0(sizeof(IndexTupleData));
			stub->t_info = INDEX_AM_RESERVED_BIT | sizeof(IndexTupleData);
			stub->t_tid = dummy;
			bark_free_oversized(index, stub);
			pfree(stub);
		}
	}

	return stats;
}

static IndexBulkDeleteResult *
barkvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
	Relation	index = info->index;
	BlockNumber npages;

	/* ANALYZE has nothing to clean up. */
	if (info->analyze_only)
		return stats;

	if (stats == NULL)
		stats = palloc0_object(IndexBulkDeleteResult);

	/*
	 * Report index-wide statistics.  When barkbulkdelete did not run (no dead
	 * tuples this cycle) we count the live leaf entries here so the planner
	 * has an up-to-date tuple count; when it did run, num_index_tuples is
	 * recomputed the same way.  Returning valid stats also lets VACUUM finish
	 * and set the heap visibility map, which is what makes index-only scans
	 * worthwhile.
	 *
	 * ponytail: no page/FSM reclamation -- empty leaves are left linked in
	 * place; recycling freed pages is a later space optimization.
	 */
	npages = RelationGetNumberOfBlocks(index);
	stats->num_pages = npages;
	stats->num_index_tuples = 0;

	for (BlockNumber blkno = BARK_METAPAGE + 1; blkno < npages; blkno++)
	{
		Buffer		buf;
		Page		page;
		BarkPageOpaque opaque;

		vacuum_delay_point(false);

		buf = ReadBufferExtended(index, MAIN_FORKNUM, blkno, RBM_NORMAL,
								 info->strategy);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);

		if (!PageIsNew(page) && BarkPageIsLeaf(BarkPageGetOpaque(page)))
		{
			opaque = BarkPageGetOpaque(page);
			stats->num_index_tuples +=
				PageGetMaxOffsetNumber(page) - BarkPageFirstDataKey(opaque) + 1;
		}
		UnlockReleaseBuffer(buf);
	}

	return stats;
}

/*
 * barkcostestimate -- the C-STATS cost model for a BARK index scan.
 *
 * Starts from genericcostestimate but tunes two things BARK does differently
 * from a plain one-tuple-per-row index, keeping the estimate honest rather than
 * elaborate:
 *
 *  1. Entry coalescing (LIST / POSTING).  A key with many duplicate rows is one
 *     leaf entry, not N, so a scan touches far fewer leaf tuples -- and pages --
 *     than the number of heap rows it returns.  index->tuples is the number of
 *     leaf entries (barkvacuumcleanup counts entries, not heap rows), so we feed
 *     genericcostestimate the number of *entries* a scan visits
 *     (indexSelectivity * entries) instead of letting it derive leaf tuples from
 *     the heap row count; its pro-rata page formula then reflects the
 *     compression.  (genericcostestimate still caps heap-row fetch cost
 *     elsewhere; here we only correct the index-page side.)
 *
 *  2. Oversized-key / oversized-INCLUDE overflow I/O.  An oversized entry's full
 *     value lives on an overflow chain the scan must read in addition to the
 *     leaf page.  We estimate the average overflow chain length from the index's
 *     own size -- bytes per entry beyond what a leaf slot holds -- and add a
 *     random-page charge per visited entry for those extra reads.  An index with
 *     no oversized entries (average entry well under the item cap) adds nothing,
 *     so a normal index costs exactly as genericcostestimate says.
 *
 * ponytail: the overflow surcharge is derived from the index's average entry
 * size, not from a count of how many visited entries are actually oversized
 * (which would need a per-index oversized-entry statistic the AM does not keep).
 * It is a correct expected-value charge for an index whose entries are
 * uniformly large, and zero for an index with none; a mixed index is charged
 * the average.  A dedicated oversized-entry count in the meta/stats is the
 * upgrade path if the planner ever misjudges a skewed mix.
 */
static void
barkcostestimate(PlannerInfo *root, IndexPath *path, double loop_count,
				 Cost *indexStartupCost, Cost *indexTotalCost,
				 Selectivity *indexSelectivity, double *indexCorrelation,
				 double *indexPages)
{
	IndexOptInfo *index = path->indexinfo;
	GenericCosts costs = {0};
	double		entries = index->tuples;
	double		nvisited;

	/*
	 * (1) Model leaf *entries* visited, not heap rows.  genericcostestimate
	 * derives numIndexTuples from indexSelectivity * heap-rows when we leave it
	 * zero; for a coalescing index that overcounts whenever duplicates are
	 * packed into LIST/POSTING entries.  Supply the entry estimate ourselves.
	 * (genericcostestimate clamps it to [1, index->tuples] internally.)
	 */
	if (entries > 0)
	{
		Selectivity sel;

		/*
		 * Reuse genericcostestimate's own selectivity by a cheap pre-pass: run
		 * it once to obtain indexSelectivity, then convert to entries.  (A
		 * second call with numIndexTuples set is cheap; the quals are already
		 * cached.)
		 */
		genericcostestimate(root, path, loop_count, &costs);
		sel = costs.indexSelectivity;
		nvisited = rint(sel * entries);
		if (nvisited < 1.0)
			nvisited = 1.0;

		memset(&costs, 0, sizeof(costs));
		costs.numIndexTuples = nvisited;
		genericcostestimate(root, path, loop_count, &costs);
	}
	else
	{
		genericcostestimate(root, path, loop_count, &costs);
		nvisited = costs.numIndexTuples;
	}

	/*
	 * (2) Overflow-page surcharge.  Estimate the average bytes per entry from
	 * the index's physical size; entries larger than a leaf slot (BarkMaxItemSize)
	 * carry the overage on an overflow chain of ~overage / BarkOverflowChunkSize
	 * pages.  Charge one random page read per such page per visited entry.  For
	 * an index with small entries this is zero.
	 */
	if (entries > 0 && index->pages > 0)
	{
		double		avg_entry_bytes = (double) index->pages * BLCKSZ / entries;

		if (avg_entry_bytes > BarkMaxItemSize)
		{
			double		overflow_bytes = avg_entry_bytes - BarkMaxItemSize;
			double		chain_pages = ceil(overflow_bytes / BarkOverflowChunkSize);
			double		spc_random_page_cost;
			Cost		surcharge;

			get_tablespace_page_costs(index->reltablespace,
									  &spc_random_page_cost, NULL);
			surcharge = nvisited * chain_pages * spc_random_page_cost;
			costs.indexTotalCost += surcharge;
			/* First overflow read is part of fetching the first matching entry. */
			costs.indexStartupCost += chain_pages * spc_random_page_cost;
		}
	}

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
 * the btree AM's), it stores the heap-TID locator, and it answers ordered-
 * operator (KNN) scans -- ORDER BY col <~> const -- over its scalar key
 * (amcanorderbyop; see barkknn.c).
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
		.amcanorderbyop = true,	/* KNN: ORDER BY col <~> const (see barkknn.c) */
		.amcanhash = false,
		.amconsistentequality = true,
		.amconsistentordering = true,
		.amcanbackward = true,
		.amcanunique = true,
		.amcanmulticol = true,
		.amoptionalkey = true,
		.amsearcharray = false,	/* no ScalarArrayOp (SAOP) scan support yet */
		.amsearchnulls = true,
		.amstorage = false,
		.amclusterable = true,
		.ampredlocks = true,
		.amcanparallel = true,
		.amcanbuildparallel = true,
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
		.amestimateparallelscan = bark_estimateparallelscan,
		.aminitparallelscan = bark_initparallelscan,
		.amparallelrescan = bark_parallelrescan,
		.amtranslatestrategy = bark_translate_strategy,
		.amtranslatecmptype = bark_translate_cmptype,
	};

	PG_RETURN_POINTER(&amroutine);
}
