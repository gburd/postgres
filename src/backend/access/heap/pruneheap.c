/*-------------------------------------------------------------------------
 *
 * pruneheap.c
 *	  heap page pruning and HOT-chain management code
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/heap/pruneheap.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/heapam_xlog.h"
#include "access/htup_details.h"
#include "access/transam.h"
#include "access/xlog.h"
#include "catalog/catalog.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "utils/snapmgr.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

/* Working data for heap_page_prune and subroutines */
typedef struct
{
	Relation	rel;

	/* tuple visibility test, initialized for the relation */
	GlobalVisState *vistest;

	/*
	 * Thresholds set by TransactionIdLimitedForOldSnapshots() if they have
	 * been computed (done on demand, and only if
	 * OldSnapshotThresholdActive()). The first time a tuple is about to be
	 * removed based on the limited horizon, old_snap_used is set to true, and
	 * SetOldSnapshotThresholdTimestamp() is called. See
	 * heap_prune_satisfies_vacuum().
	 */
	TimestampTz old_snap_ts;
	TransactionId old_snap_xmin;
	bool		old_snap_used;

	TransactionId new_prune_xid;	/* new prune hint value for page */
	TransactionId latestRemovedXid; /* latest xid to be removed by this prune */
	int			nredirected;	/* numbers of entries in arrays below */
	int			nredirected_data;
	int			ndead;
	int			nunused;
	/* arrays that accumulate indexes of items to be changed */
	OffsetNumber redirected[MaxHeapTuplesPerPage * 2];
	OffsetNumber redirected_data[MaxHeapTuplesPerPage * 2];
	bits8	   *redirect_data[MaxHeapTuplesPerPage];
	OffsetNumber nowdead[MaxHeapTuplesPerPage];
	OffsetNumber nowunused[MaxHeapTuplesPerPage];
	/* marked[i] is true if item i is entered in one of the above arrays */
	bool		marked[MaxHeapTuplesPerPage + 1];
} PruneState;

/* Local functions */
static int	heap_prune_chain(Relation relation,
							 Buffer buffer,
							 OffsetNumber rootoffnum,
							 PruneState *prstate);
static void heap_prune_record_prunable(PruneState *prstate, TransactionId xid);
static void heap_prune_record_redirect_with_data(PruneState *prstate,
												 OffsetNumber offnum,
												 OffsetNumber rdoffnum,
												 int natts, Bitmapset *data);
static void heap_prune_record_redirect(PruneState *prstate,
									   OffsetNumber offnum, OffsetNumber rdoffnum);
static void heap_prune_record_dead(PruneState *prstate, OffsetNumber offnum);
static void heap_prune_record_unused(PruneState *prstate, OffsetNumber offnum);
static Bitmapset *GetModifiedColumnsBitmap(Relation rel, Buffer buffer, Page dp,
										   OffsetNumber oldlp, OffsetNumber newlp,
										   bool newlp_is_phot,
										   Bitmapset *interesting_attrs);
static void StoreModifiedColumnsBitmap(Bitmapset *data, int natts, bits8 **bits);


/*
 * Optionally prune and repair fragmentation in the specified page.
 *
 * This is an opportunistic function.  It will perform housekeeping
 * only if the page heuristically looks like a candidate for pruning and we
 * can acquire buffer cleanup lock without blocking.
 *
 * Note: this is called quite often.  It's important that it fall out quickly
 * if there's not any use in pruning.
 *
 * Caller must have pin on the buffer, and must *not* have a lock on it.
 */
void
heap_page_prune_opt(Relation relation, Buffer buffer)
{
	Page		page = BufferGetPage(buffer);
	TransactionId prune_xid;
	GlobalVisState *vistest;
	TransactionId limited_xmin = InvalidTransactionId;
	TimestampTz limited_ts = 0;
	Size		minfree;

	/*
	 * We can't write WAL in recovery mode, so there's no point trying to
	 * clean the page. The primary will likely issue a cleaning WAL record soon
	 * anyway, so this is no particular loss.
	 */
	if (RecoveryInProgress())
		return;

	/*
	 * XXX: Magic to keep old_snapshot_threshold tests appear "working". They
	 * currently are broken, and discussion of what to do about them is
	 * ongoing. See
	 * https://www.postgresql.org/message-id/20200403001235.e6jfdll3gh2ygbuc%40alap3.anarazel.de
	 */
	if (old_snapshot_threshold == 0)
		SnapshotTooOldMagicForTest();

	/*
	 * First check whether there's any chance there's something to prune,
	 * determining the appropriate horizon is a waste if there's no prune_xid
	 * (i.e. no updates/deletes left potentially dead tuples around).
	 */
	prune_xid = ((PageHeader) page)->pd_prune_xid;
	if (!TransactionIdIsValid(prune_xid))
		return;

	/*
	 * Check whether prune_xid indicates that there may be dead rows that can
	 * be cleaned up.
	 *
	 * It is OK to check the old snapshot limit before acquiring the cleanup
	 * lock because the worst that can happen is that we are not quite as
	 * aggressive about the cleanup (by however many transaction IDs are
	 * consumed between this point and acquiring the lock).  This allows us to
	 * save significant overhead in the case where the page is found not to be
	 * prunable.
	 *
	 * Even if old_snapshot_threshold is set, we first check whether the page
	 * can be pruned without. Both because
	 * TransactionIdLimitedForOldSnapshots() is not cheap, and because not
	 * unnecessarily relying on old_snapshot_threshold avoids causing
	 * conflicts.
	 */
	vistest = GlobalVisTestFor(relation);

	if (!GlobalVisTestIsRemovableXid(vistest, prune_xid))
	{
		if (!OldSnapshotThresholdActive())
			return;

		if (!TransactionIdLimitedForOldSnapshots(GlobalVisTestNonRemovableHorizon(vistest),
												 relation,
												 &limited_xmin, &limited_ts))
			return;

		if (!TransactionIdPrecedes(prune_xid, limited_xmin))
			return;
	}

	/*
	 * We prune when a previous UPDATE failed to find enough space on the page
	 * for a new tuple version, or when free space falls below the relation's
	 * fill-factor target (but not less than 10%).
	 *
	 * Checking free space here is questionable since we aren't holding any
	 * lock on the buffer; in the worst case we could get a bogus answer. It's
	 * unlikely to be *seriously* wrong, though, since reading either pd_lower
	 * or pd_upper is probably atomic.  Avoiding taking a lock seems more
	 * important than sometimes getting a wrong answer in what is after all
	 * just a heuristic estimate.
	 */
	minfree = RelationGetTargetPageFreeSpace(relation,
											 HEAP_DEFAULT_FILLFACTOR);
	minfree = Max(minfree, BLCKSZ / 10);

	if (PageIsFull(page) || PageGetHeapFreeSpace(page) < minfree)
	{
		/* OK, try to get exclusive buffer lock */
		if (!ConditionalLockBufferForCleanup(buffer))
			return;

		/*
		 * Now that we have buffer lock, get accurate information about the
		 * page's free space, and recheck the heuristic about whether to
		 * prune. (We needn't recheck PageIsPrunable, since no one else could
		 * have pruned while we hold pin.)
		 */
		if (PageIsFull(page) || PageGetHeapFreeSpace(page) < minfree)
		{
			TransactionId ignore = InvalidTransactionId;	/* return value not
															 * needed */

			/* OK to prune */
			(void) heap_page_prune(relation, buffer, vistest,
								   limited_xmin, limited_ts,
								   true, &ignore, NULL);
		}

		/* And release buffer lock */
		LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
	}
}


/*
 * Prune and repair fragmentation in the specified page.
 *
 * Caller must have pin and buffer cleanup lock on the page.
 *
 * vistest is used to distinguish whether tuples are DEAD or RECENTLY_DEAD
 * (see heap_prune_satisfies_vacuum and
 * HeapTupleSatisfiesVacuum). old_snap_xmin / old_snap_ts need to
 * either have been set by TransactionIdLimitedForOldSnapshots, or
 * InvalidTransactionId/0 respectively.
 *
 * If report_stats is true then we send the number of reclaimed heap-only
 * tuples to pgstats.  (This must be false during vacuum, since vacuum will
 * send its own new total to pgstats, and we don't want this delta applied
 * on top of that.)
 *
 * Sets latestRemovedXid for caller on return.
 *
 * off_loc is the offset location required by the caller to use in error
 * callback.
 *
 * Returns the number of tuples deleted from the page during this call.
 */
int
heap_page_prune(Relation relation, Buffer buffer,
				GlobalVisState *vistest,
				TransactionId old_snap_xmin,
				TimestampTz old_snap_ts,
				bool report_stats, TransactionId *latestRemovedXid,
				OffsetNumber *off_loc)
{
	int			ndeleted = 0;
	Page		page = BufferGetPage(buffer);
	OffsetNumber offnum,
				maxoff;
	PruneState	prstate;

	/*
	 * Our strategy is to scan the page and make lists of items to change,
	 * then apply the changes within a critical section.  This keeps as much
	 * logic as possible out of the critical section, and also ensures that
	 * WAL replay will work the same as the normal case.
	 *
	 * First, initialize the new pd_prune_xid value to zero (indicating no
	 * prunable tuples).  If we find any tuples which may soon become
	 * prunable, we will save the lowest relevant XID in new_prune_xid. Also
	 * initialize the rest of our working state.
	 */
	prstate.new_prune_xid = InvalidTransactionId;
	prstate.rel = relation;
	prstate.vistest = vistest;
	prstate.old_snap_xmin = old_snap_xmin;
	prstate.old_snap_ts = old_snap_ts;
	prstate.old_snap_used = false;
	prstate.latestRemovedXid = *latestRemovedXid;
	prstate.nredirected = prstate.nredirected_data = prstate.ndead = prstate.nunused = 0;
	memset(prstate.marked, 0, sizeof(prstate.marked));

	/* Scan the page */
	maxoff = PageGetMaxOffsetNumber(page);
	for (offnum = FirstOffsetNumber;
		 offnum <= maxoff;
		 offnum = OffsetNumberNext(offnum))
	{
		ItemId		itemid;

		/* Ignore items already processed as part of an earlier chain */
		if (prstate.marked[offnum])
			continue;

		/*
		 * Set the offset number so that we can display it along with any
		 * error that occurred while processing this tuple.
		 */
		if (off_loc)
			*off_loc = offnum;

		/* Nothing to do if slot is empty or already dead */
		itemid = PageGetItemId(page, offnum);
		if (!ItemIdIsUsed(itemid) || ItemIdIsDead(itemid))
			continue;

		/* Process this item or chain of items */
		ndeleted += heap_prune_chain(relation, buffer, offnum, &prstate);
	}

	/* Clear the offset information once we have processed the given page. */
	if (off_loc)
		*off_loc = InvalidOffsetNumber;

	/* Any error while applying the changes is critical */
	START_CRIT_SECTION();

	/* Have we found any prunable items? */
	if (prstate.nredirected > 0 ||
		prstate.nredirected_data > 0 ||
		prstate.ndead > 0 ||
		prstate.nunused > 0)
	{
		/*
		 * Apply the planned item changes, then repair page fragmentation, and
		 * update the page's hint bit about whether it has free line pointers.
		 */
		heap_page_prune_execute(buffer,
								prstate.redirected, prstate.nredirected,
								prstate.redirected_data, prstate.nredirected_data,
								prstate.redirect_data,
								prstate.nowdead, prstate.ndead,
								prstate.nowunused, prstate.nunused);

		/*
		 * Update the page's pd_prune_xid field to either zero, or the lowest
		 * XID of any soon-prunable tuple.
		 */
		((PageHeader) page)->pd_prune_xid = prstate.new_prune_xid;

		/*
		 * Also clear the "page is full" flag, since there's no point in
		 * repeating the prune/defrag process until something else happens to
		 * the page.
		 */
		PageClearFull(page);

		MarkBufferDirty(buffer);

		/*
		 * Emit a WAL XLOG_HEAP2_CLEAN record showing what we did
		 */
		if (RelationNeedsWAL(relation))
		{
			XLogRecPtr	recptr;

			recptr = log_heap_clean(relation, buffer,
									prstate.redirected, prstate.nredirected,
									prstate.nowdead, prstate.ndead,
									prstate.nowunused, prstate.nunused,
									prstate.latestRemovedXid);

			PageSetLSN(BufferGetPage(buffer), recptr);
		}
	}
	else
	{
		/*
		 * If we didn't prune anything, but have found a new value for the
		 * pd_prune_xid field, update it and mark the buffer dirty. This is
		 * treated as a non-WAL-logged hint.
		 *
		 * Also clear the "page is full" flag if it is set, since there's no
		 * point in repeating the prune/defrag process until something else
		 * happens to the page.
		 */
		if (((PageHeader) page)->pd_prune_xid != prstate.new_prune_xid ||
			PageIsFull(page))
		{
			((PageHeader) page)->pd_prune_xid = prstate.new_prune_xid;
			PageClearFull(page);
			MarkBufferDirtyHint(buffer, true);
		}
	}

	END_CRIT_SECTION();

	/*
	 * If requested, report the number of tuples reclaimed to pgstats. This is
	 * ndeleted minus ndead, because we don't want to count a now-DEAD root
	 * item as a deletion for this purpose.
	 */
	if (report_stats && ndeleted > prstate.ndead)
		pgstat_update_heap_dead_tuples(relation, ndeleted - prstate.ndead);

	*latestRemovedXid = prstate.latestRemovedXid;

	/*
	 * XXX Should we update the FSM information of this page ?
	 *
	 * There are two schools of thought here. We may not want to update FSM
	 * information so that the page is not used for unrelated UPDATEs/INSERTs
	 * and any free space in this page will remain available for further
	 * UPDATEs in *this* page, thus improving chances for doing HOT updates.
	 *
	 * But for a large table and where a page does not receive further UPDATEs
	 * for a long time, we might waste this space by not updating the FSM
	 * information. The relation may get extended and fragmented further.
	 *
	 * One possibility is to leave "fillfactor" worth of space in this page
	 * and update FSM with the remaining space.
	 */

	return ndeleted;
}


/*
 * Perform visibility checks for heap pruning.
 *
 * This is more complicated than just using GlobalVisTestIsRemovableXid()
 * because of old_snapshot_threshold. We only want to increase the threshold
 * that triggers errors for old snapshots when we actually decide to remove a
 * row based on the limited horizon.
 *
 * Due to its cost we also only want to call
 * TransactionIdLimitedForOldSnapshots() if necessary, i.e. we might not have
 * done so in heap_hot_prune_opt() if pd_prune_xid was old enough. But we
 * still want to be able to remove rows that are too new to be removed
 * according to prstate->vistest, but that can be removed based on
 * old_snapshot_threshold. So we call TransactionIdLimitedForOldSnapshots() on
 * demand in here, if appropriate.
 */
static HTSV_Result
heap_prune_satisfies_vacuum(PruneState *prstate, HeapTuple tup, Buffer buffer)
{
	HTSV_Result res;
	TransactionId dead_after;

	res = HeapTupleSatisfiesVacuumHorizon(tup, buffer, &dead_after);

	if (res != HEAPTUPLE_RECENTLY_DEAD)
		return res;

	/*
	 * If we are already relying on the limited xmin, there is no need to
	 * delay doing so anymore.
	 */
	if (prstate->old_snap_used)
	{
		Assert(TransactionIdIsValid(prstate->old_snap_xmin));

		if (TransactionIdPrecedes(dead_after, prstate->old_snap_xmin))
			res = HEAPTUPLE_DEAD;
		return res;
	}

	/*
	 * First check if GlobalVisTestIsRemovableXid() is sufficient to find the
	 * row dead. If not, and old_snapshot_threshold is enabled, try to use the
	 * lowered horizon.
	 */
	if (GlobalVisTestIsRemovableXid(prstate->vistest, dead_after))
		res = HEAPTUPLE_DEAD;
	else if (OldSnapshotThresholdActive())
	{
		/* haven't determined limited horizon yet, requests */
		if (!TransactionIdIsValid(prstate->old_snap_xmin))
		{
			TransactionId horizon =
			GlobalVisTestNonRemovableHorizon(prstate->vistest);

			TransactionIdLimitedForOldSnapshots(horizon, prstate->rel,
												&prstate->old_snap_xmin,
												&prstate->old_snap_ts);
		}

		if (TransactionIdIsValid(prstate->old_snap_xmin) &&
			TransactionIdPrecedes(dead_after, prstate->old_snap_xmin))
		{
			/*
			 * About to remove row based on snapshot_too_old. Need to raise
			 * the threshold so problematic accesses would error.
			 */
			Assert(!prstate->old_snap_used);
			SetOldSnapshotThresholdTimestamp(prstate->old_snap_ts,
											 prstate->old_snap_xmin);
			prstate->old_snap_used = true;
			res = HEAPTUPLE_DEAD;
		}
	}

	return res;
}


/*
 * Prune specified line pointer or a (P)HOT chain originating at line pointer.
 *
 * If the item is an index-referenced tuple (i.e. not a heap-only tuple),
 * the HOT chain is pruned by removing all DEAD tuples at the start of the HOT
 * chain.  We also prune any RECENTLY_DEAD tuples preceding a DEAD tuple.
 * This is OK because a RECENTLY_DEAD tuple preceding a DEAD tuple is really
 * DEAD, the OldestXmin test is just too coarse to detect it.
 *
 * The root line pointer is redirected to the tuple immediately after the
 * latest DEAD tuple.  If all tuples in the chain are DEAD, the root line
 * pointer is marked LP_DEAD.  (This includes the case of a DEAD simple
 * tuple, which we treat as a chain of length 1.)
 *
 * OldestXmin is the cutoff XID used to identify dead tuples.
 *
 * We don't actually change the page here, except perhaps for hint-bit updates
 * caused by HeapTupleSatisfiesVacuum.  We just add entries to the arrays in
 * prstate showing the changes to be made.  Items to be redirected are added
 * to the redirected[] array (two entries per redirection); items to be set to
 * LP_DEAD state are added to nowdead[]; and items to be set to LP_UNUSED
 * state are added to nowunused[].
 *
 * Returns the number of tuples (to be) deleted from the page.
 *
 * TODO: Update this description for PHOT.
 */
static int
heap_prune_chain(Relation rel, Buffer buffer, OffsetNumber rootoffnum,
				 PruneState *prstate)
{
	int			ndeleted = 0;
	Page		dp = (Page) BufferGetPage(buffer);
	TransactionId priorXmax = InvalidTransactionId;
	ItemId		rootlp;
	HeapTupleHeader htup;
	OffsetNumber latestdead = InvalidOffsetNumber,
				maxoff = PageGetMaxOffsetNumber(dp),
				offnum;
	OffsetNumber chainitems[MaxHeapTuplesPerPage];
	int			nchain = 0,
				i;
	HeapTupleData tup;
	bool		phot_items[MaxHeapTuplesPerPage];

	memset(phot_items, false, sizeof(phot_items));

	tup.t_tableOid = RelationGetRelid(prstate->rel);

	rootlp = PageGetItemId(dp, rootoffnum);

	/*
	 * If it's a heap-only tuple or a partial heap-only tuple, then it is not
	 * the start of a HOT or PHOT chain.
	 */
	if (ItemIdIsNormal(rootlp))
	{
		htup = (HeapTupleHeader) PageGetItem(dp, rootlp);

		tup.t_data = htup;
		tup.t_len = ItemIdGetLength(rootlp);
		ItemPointerSet(&(tup.t_self), BufferGetBlockNumber(buffer), rootoffnum);

		if (HeapTupleHeaderIsHeapOnly(htup) ||
			HeapTupleHeaderIsPartialHeapOnly(htup))
		{
			/*
			 * If the tuple is DEAD and doesn't chain to anything else, mark
			 * it unused or dead immediately.  Heap-only tuples can be marked
			 * unused because there will be no index entries that point to it,
			 * but partial heap-only tuples can only be marked dead since there
			 * might be associated index tuples.  (If the tuple does chain, we
			 * can only remove it as part of pruning its chain.)
			 *
			 * We need this primarily to handle aborted (P)HOT updates, that is,
			 * XMIN_INVALID heap-only or partial heap-only tuples.  Those might
			 * not be linked to by any chain, since the parent tuple might be
			 * re-updated before any pruning occurs.  So we have to be able to
			 * reap them separately from chain-pruning.  (Note that
			 * HeapTupleHeaderIsHotUpdated and
			 * HeapTupleHeaderIsPartialHotUpdated will never return true for an
			 * XMIN_INVALID tuple, so this code will work even when there were
			 * sequential updates within the aborted transaction.)
			 *
			 * Note that we might first arrive at a dead heap-only or partial
			 * heap-only tuple either here or while following a chain below.
			 * Whichever path gets there first will mark the tuple unused or
			 * dead.
			 */
			if (heap_prune_satisfies_vacuum(prstate, &tup, buffer)
				== HEAPTUPLE_DEAD && !HeapTupleHeaderIsHotUpdated(htup) &&
				!HeapTupleHeaderIsPartialHotUpdated(htup))
			{
				if (HeapTupleHeaderIsHeapOnly(htup))
					heap_prune_record_unused(prstate, rootoffnum);
				else if (HeapTupleHeaderIsPartialHeapOnly(htup))
					heap_prune_record_dead(prstate, rootoffnum);

				HeapTupleHeaderAdvanceLatestRemovedXid(htup,
													   &prstate->latestRemovedXid);
				ndeleted++;
			}

			/* Nothing more to do */
			return ndeleted;
		}
	}

	/* Start from the root tuple */
	offnum = rootoffnum;

	/* while not end of the chain */
	for (;;)
	{
		ItemId		lp;
		bool		tupdead,
					recent_dead;

		/* Some sanity checks */
		if (offnum < FirstOffsetNumber || offnum > maxoff)
			break;

		/* If item is already processed, stop --- it must not be same chain */
		if (prstate->marked[offnum])
			break;

		lp = PageGetItemId(dp, offnum);

		/* Unused item obviously isn't part of the chain */
		if (!ItemIdIsUsed(lp))
			break;

		/*
		 * If we are looking at the redirected root line pointer, jump to the
		 * first normal tuple in the chain.  If we find a redirect somewhere
		 * else, stop --- it must not be same chain.
		 *
		 * XXX: update this comment
		 */
		if (ItemIdIsRedirected(lp))
		{
			chainitems[nchain] = offnum;
			offnum = ItemIdGetRedirect(lp);

			if (rootlp == lp)
				phot_items[nchain] = ItemIdIsPartialHotRedirected(dp, lp);
			else
			{
				ItemId prev = PageGetItemId(dp, chainitems[nchain - 2]);
				phot_items[nchain] = ItemIdIsPartialHotRedirected(dp, prev);
			}

			nchain++;
			continue;
		}

		/*
		 * Likewise, a dead line pointer can't be part of the chain. (We
		 * already eliminated the case of dead root tuple outside this
		 * function.)
		 */
		if (ItemIdIsDead(lp))
			break;

		Assert(ItemIdIsNormal(lp));
		htup = (HeapTupleHeader) PageGetItem(dp, lp);

		tup.t_data = htup;
		tup.t_len = ItemIdGetLength(lp);
		ItemPointerSet(&(tup.t_self), BufferGetBlockNumber(buffer), offnum);

		/*
		 * Check the tuple XMIN against prior XMAX, if any
		 */
		if (TransactionIdIsValid(priorXmax) &&
			!TransactionIdEquals(HeapTupleHeaderGetXmin(htup), priorXmax))
			break;

		/*
		 * OK, this tuple is indeed a member of the chain.
		 */
		chainitems[nchain] = offnum;
		if (HeapTupleHeaderIsPartialHeapOnly(htup) ||
			(!HeapTupleHeaderIsHeapOnly(htup) &&
			 HeapTupleHeaderIsPartialHotUpdated(htup)))
			phot_items[nchain] = true;
		nchain++;

		/*
		 * Check tuple's visibility status.
		 */
		tupdead = recent_dead = false;

		switch (heap_prune_satisfies_vacuum(prstate, &tup, buffer))
		{
			case HEAPTUPLE_DEAD:
				tupdead = true;
				break;

			case HEAPTUPLE_RECENTLY_DEAD:
				recent_dead = true;

				/*
				 * This tuple may soon become DEAD.  Update the hint field so
				 * that the page is reconsidered for pruning in future.
				 */
				heap_prune_record_prunable(prstate,
										   HeapTupleHeaderGetUpdateXid(htup));
				break;

			case HEAPTUPLE_DELETE_IN_PROGRESS:

				/*
				 * This tuple may soon become DEAD.  Update the hint field so
				 * that the page is reconsidered for pruning in future.
				 */
				heap_prune_record_prunable(prstate,
										   HeapTupleHeaderGetUpdateXid(htup));
				break;

			case HEAPTUPLE_LIVE:
			case HEAPTUPLE_INSERT_IN_PROGRESS:

				/*
				 * If we wanted to optimize for aborts, we might consider
				 * marking the page prunable when we see INSERT_IN_PROGRESS.
				 * But we don't.  See related decisions about when to mark the
				 * page prunable in heapam.c.
				 */
				break;

			default:
				elog(ERROR, "unexpected HeapTupleSatisfiesVacuum result");
				break;
		}

		/*
		 * Remember the last DEAD tuple seen.
		 */
		if (tupdead)
		{
			latestdead = offnum;
			HeapTupleHeaderAdvanceLatestRemovedXid(htup,
												   &prstate->latestRemovedXid);
		}
		else
			break;

		/*
		 * If the tuple is not (P)HOT-updated, then we are at the end of this
		 * (P)HOT-update chain.
		 */
		if (!HeapTupleHeaderIsHotUpdated(htup) &&
			!HeapTupleHeaderIsPartialHotUpdated(htup))
			break;

		/* (P)HOT implies it can't have moved to different partition */
		Assert(!HeapTupleHeaderIndicatesMovedPartitions(htup));

		/*
		 * Advance to next chain member.
		 */
		Assert(ItemPointerGetBlockNumber(&htup->t_ctid) ==
			   BufferGetBlockNumber(buffer));
		offnum = ItemPointerGetOffsetNumber(&htup->t_ctid);
		priorXmax = HeapTupleHeaderGetUpdateXid(htup);
	}

	/*
	 * If we found a DEAD tuple in the chain, adjust the HOT chain so that all
	 * the DEAD tuples at the start of the chain are removed and the root line
	 * pointer is appropriately redirected.
	 *
	 * XXX: Update this documentation for PHOT.
	 */
	if (OffsetNumberIsValid(latestdead))
	{
		Bitmapset  *modified_attrs = NULL;
		Bitmapset  *interesting_attrs = NULL;
		Bitmapset  *intermediate = NULL;
		Bitmapset  *modified = NULL;
		OffsetNumber keyitems[MaxHeapTuplesPerPage];
		int			natts = RelationGetNumberOfAttributes(rel);
		int			lastoff = chainitems[nchain - 1];
		int			nkeys = 0;
		bool		has_phot = phot_items[nchain - 1];
		bool		chain_dead = (lastoff == latestdead);

		// TODO: think about column additions/removals

		/*
		 * First, evaluate the last tuple in the chain.  The only time we
		 * modify it is the special case where it is dead.  In this special case,
		 * the whole chain is dead, and we can quickly scan through it.
		 */
		if (chain_dead)
		{
			if (ItemIdIsNormal(PageGetItemId(dp, lastoff)))
				ndeleted++;

			if (nchain - 1 == 0 || has_phot)
				heap_prune_record_dead(prstate, lastoff);
			else
				heap_prune_record_unused(prstate, lastoff);
		}
		else if (has_phot && nchain > 1)
		{
			keyitems[nkeys++] = lastoff;
			intermediate = GetModifiedColumnsBitmap(rel, buffer, dp,
													chainitems[nchain - 2],
													lastoff,
													true, interesting_attrs);
			modified_attrs = bms_copy(intermediate);
		}

		/*
		 * Now, go through all chain items except for the first and last ones.
		 *
		 * TODO: expand
		 */
		for (i = nchain - 2; i > 0; i--)
		{
			/*
			 * We're either reclaiming the line pointer (and any associated
			 * storage), reclaiming the storage, or replacing the storage with a
			 * small amount of "redirect data,"  We consider each of these as
			 * deleting the item.
			 */
			if (ItemIdIsNormal(PageGetItemId(dp, chainitems[i])))
				ndeleted++;

			/*
			 * If the rest of the chain is dead or we've only seen HOT items so
			 * far, just mark the item as dead/unused and move on.  We are
			 * careful to do this before GetModifiedColumnsBitmap() so that we
			 * avoid the expense of that call whenever possible.  Presumably we
			 * could also mark PHOT items as unused if we knew they no longer
			 * had index entries, but that is not strictly necessary, and the
			 * benefit might outweigh the expense.
			 */
			if (chain_dead || (!has_phot && !phot_items[i]))
			{
				if (phot_items[i])
					heap_prune_record_dead(prstate, chainitems[i]);
				else
					heap_prune_record_unused(prstate, chainitems[i]);
				continue;
			}

			/*
			 * We wait until the last minute to generate the bitmap of indexed
			 * attributes so that we don't incur the expense in the fast paths.
			 *
			 * Ideally we'd be able to use RelationGetIndexAttrBitmap() to get
			 * just the indexed columns here.  However, there's a deadlock risk
			 * with the buffer lock we already have.  If we did use such a
			 * function, we'd also have to prepare for the possibility that this
			 * bitmap will be empty.
			 */
			if (interesting_attrs == NULL)
				interesting_attrs = bms_add_range(NULL,
												  1 - FirstLowInvalidHeapAttributeNumber,
												  natts - FirstLowInvalidHeapAttributeNumber);

			/*
			 * Retrieve the set of indexed columns that were modified between
			 * the current tuple and the preceding one in the chain.
			 */
			bms_free(modified);
			modified = GetModifiedColumnsBitmap(rel, buffer, dp,
												chainitems[i - 1],
												chainitems[i],
												phot_items[i],
												interesting_attrs);

			/*
			 * If there are definitely no index entries pointing to this item,
			 * then we can just mark it unused.  This is unlikely to ever be
			 * true for now, but in the future we might set interesting_attrs to
			 * the set of indexed columns (in which case it will be far more
			 * likely).
			 */
			if (bms_is_empty(modified))
			{
				heap_prune_record_unused(prstate, chainitems[i]);
				continue;
			}

			/*
			 * If this is the first PHOT item that we've encountered that still
			 * has corresponding index entries, redirect it to the last item in
			 * the chain (which must be heap-only).  This item must also be a
			 * key item for PHOT, too.
			 */
			if (phot_items[i] && !has_phot)
			{
				heap_prune_record_redirect(prstate, chainitems[i], lastoff);
				keyitems[nkeys++] = chainitems[i];
				intermediate = modified;
				modified_attrs = bms_copy(modified);
				modified = NULL;
				has_phot = true;
				continue;
			}

			/*
			 * If we find a heap-only item in the middle of a chain that
			 * contains PHOT items, we know that we can get rid of it right
			 * away.
			 */
			if (!phot_items[i] && has_phot)
			{
				heap_prune_record_unused(prstate, chainitems[i]);
				continue;
			}

			/*
			 * At this point, we know that we've found a PHOT item somewhere in
			 * the middle of a chain that we already know has PHOT items.  If
			 * the set of modified columns between this item and the preceding
			 * item fit within our top-level modified columns bitmap for the
			 * chain, we don't need to keep the item around.
			 */
			if (bms_is_subset(modified, modified_attrs))
			{
				heap_prune_record_dead(prstate, chainitems[i]);
				intermediate = bms_union(intermediate, modified);
				continue;
			}

			/*
			 * If all else has failed, we must have a new key item.  Mark it as
			 * redirected-with-data and store the modified-columns bitmap in the
			 * tuple storage.
			 */
			heap_prune_record_redirect_with_data(prstate, chainitems[i],
												 keyitems[nkeys - 1],
												 natts, intermediate);
			keyitems[nkeys++] = chainitems[i];
			bms_free(intermediate);
			intermediate = modified;
			modified_attrs = bms_union(modified_attrs, modified);
			chain_dead = bms_equal(modified_attrs, interesting_attrs);
			modified = NULL;
		}

		/*
		 * Finally, handle the root item.  We can only mark it dead if the whole
		 * chain is dead, otherwise we have to mark it redirected in some form.
		 * If this is a one-item chain, then we've already handled the root item
		 * above, and we can skip this.
		 */
		if (nchain > 1)
		{
			if (ItemIdIsNormal(rootlp))
				ndeleted++;

			if (chain_dead)
				heap_prune_record_dead(prstate, rootoffnum);
			else if (nkeys > 0)
				heap_prune_record_redirect_with_data(prstate, rootoffnum,
													 keyitems[nkeys - 1], natts,
													 intermediate);
			else
				heap_prune_record_redirect(prstate, rootoffnum, lastoff);
		}

		bms_free(modified_attrs);
		bms_free(interesting_attrs);
		bms_free(intermediate);
		bms_free(modified);
	}
	else if (nchain < 2 && ItemIdIsRedirected(rootlp))
	{
		/*
		 * We found a redirect item that doesn't point to a valid follow-on
		 * item.  This can happen if the loop in heap_page_prune caused us to
		 * visit the dead successor of a redirect item before visiting the
		 * redirect item.  We can clean up by setting the redirect item to
		 * DEAD state.
		 */
		heap_prune_record_dead(prstate, rootoffnum);
	}

	return ndeleted;
}

/* Record lowest soon-prunable XID */
static void
heap_prune_record_prunable(PruneState *prstate, TransactionId xid)
{
	/*
	 * This should exactly match the PageSetPrunable macro.  We can't store
	 * directly into the page header yet, so we update working state.
	 */
	Assert(TransactionIdIsNormal(xid));
	if (!TransactionIdIsValid(prstate->new_prune_xid) ||
		TransactionIdPrecedes(xid, prstate->new_prune_xid))
		prstate->new_prune_xid = xid;
}

/*
 * Record line pointer to be redirected with data */
static void
heap_prune_record_redirect_with_data(PruneState *prstate,
									 OffsetNumber offnum, OffsetNumber rdoffnum,
									 int natts, Bitmapset *data)
{
	Assert(prstate->nredirected_data < MaxHeapTuplesPerPage);
	prstate->redirected_data[prstate->nredirected_data * 2] = offnum;
	prstate->redirected_data[prstate->nredirected_data * 2 + 1] = rdoffnum;
	StoreModifiedColumnsBitmap(data, natts,
							   &prstate->redirect_data[prstate->nredirected_data]);
	prstate->nredirected_data++;
	Assert(!prstate->marked[offnum]);
	prstate->marked[offnum] = true;
	Assert(!prstate->marked[rdoffnum]);
	prstate->marked[rdoffnum] = true;
}

/* Record line pointer to be redirected */
static void
heap_prune_record_redirect(PruneState *prstate,
						   OffsetNumber offnum, OffsetNumber rdoffnum)
{
	Assert(prstate->nredirected < MaxHeapTuplesPerPage);
	prstate->redirected[prstate->nredirected * 2] = offnum;
	prstate->redirected[prstate->nredirected * 2 + 1] = rdoffnum;
	prstate->nredirected++;
	Assert(!prstate->marked[offnum]);
	prstate->marked[offnum] = true;
	Assert(!prstate->marked[rdoffnum]);
	prstate->marked[rdoffnum] = true;
}

/* Record line pointer to be marked dead */
static void
heap_prune_record_dead(PruneState *prstate, OffsetNumber offnum)
{
	Assert(prstate->ndead < MaxHeapTuplesPerPage);
	prstate->nowdead[prstate->ndead] = offnum;
	prstate->ndead++;
	Assert(!prstate->marked[offnum]);
	prstate->marked[offnum] = true;
}

/* Record line pointer to be marked unused */
static void
heap_prune_record_unused(PruneState *prstate, OffsetNumber offnum)
{
	Assert(prstate->nunused < MaxHeapTuplesPerPage);
	prstate->nowunused[prstate->nunused] = offnum;
	prstate->nunused++;
	Assert(!prstate->marked[offnum]);
	prstate->marked[offnum] = true;
}


/*
 * Perform the actual page changes needed by heap_page_prune.
 * It is expected that the caller has suitable pin and lock on the
 * buffer, and is inside a critical section.
 *
 * This is split out because it is also used by heap_xlog_clean()
 * to replay the WAL record when needed after a crash.  Note that the
 * arguments are identical to those of log_heap_clean().
 */
void
heap_page_prune_execute(Buffer buffer,
						OffsetNumber *redirected, int nredirected,
						OffsetNumber *redirected_data, int nredirected_data,
						bits8 **redirect_data,
						OffsetNumber *nowdead, int ndead,
						OffsetNumber *nowunused, int nunused)
{
	Page		page = (Page) BufferGetPage(buffer);
	OffsetNumber *offnum;
	int			i;

	/* Update all redirected line pointers */
	offnum = redirected;
	for (i = 0; i < nredirected; i++)
	{
		OffsetNumber fromoff = *offnum++;
		OffsetNumber tooff = *offnum++;
		ItemId		fromlp = PageGetItemId(page, fromoff);

		ItemIdSetRedirect(fromlp, tooff);
	}

	offnum = redirected_data;
	for (i = 0; i < nredirected_data; i++)
	{
		OffsetNumber fromoff = *offnum++;
		OffsetNumber tooff = *offnum++;
		ItemId		fromlp = PageGetItemId(page, fromoff);
		OffsetNumber origoff = ItemIdGetOffset(fromlp);

		ItemIdSetRedirectWithData(fromlp, tooff);

		memcpy((char *) page + origoff,
			   redirect_data[i],
			   ((RedirectHeader) redirect_data[i])->rlp_len);
	}

	/* Update all now-dead line pointers */
	offnum = nowdead;
	for (i = 0; i < ndead; i++)
	{
		OffsetNumber off = *offnum++;
		ItemId		lp = PageGetItemId(page, off);

		ItemIdSetDead(lp);
	}

	/* Update all now-unused line pointers */
	offnum = nowunused;
	for (i = 0; i < nunused; i++)
	{
		OffsetNumber off = *offnum++;
		ItemId		lp = PageGetItemId(page, off);

		ItemIdSetUnused(lp);
	}

	/*
	 * Finally, repair any fragmentation, and update the page's hint bit about
	 * whether it has free pointers.
	 */
	PageRepairFragmentation(page);
}


/*
 * For all items in this page, find their respective root line pointers.
 * If item k is part of a HOT-chain with root at item j, then we set
 * root_offsets[k - 1] = j.
 *
 * The passed-in root_offsets array must have MaxHeapTuplesPerPage entries.
 * Unused entries are filled with InvalidOffsetNumber (zero).
 *
 * The function must be called with at least share lock on the buffer, to
 * prevent concurrent prune operations.
 *
 * Note: The information collected here is valid only as long as the caller
 * holds a pin on the buffer. Once pin is released, a tuple might be pruned
 * and reused by a completely unrelated tuple.
 */
void
heap_get_root_tuples(Page page, OffsetNumber *root_offsets)
{
	OffsetNumber offnum,
				maxoff;

	MemSet(root_offsets, InvalidOffsetNumber,
		   MaxHeapTuplesPerPage * sizeof(OffsetNumber));

	maxoff = PageGetMaxOffsetNumber(page);
	for (offnum = FirstOffsetNumber; offnum <= maxoff; offnum = OffsetNumberNext(offnum))
	{
		ItemId		lp = PageGetItemId(page, offnum);
		HeapTupleHeader htup;
		OffsetNumber nextoffnum;
		TransactionId priorXmax;

		/* skip unused and dead items */
		if (!ItemIdIsUsed(lp) || ItemIdIsDead(lp))
			continue;

		if (ItemIdIsNormal(lp))
		{
			htup = (HeapTupleHeader) PageGetItem(page, lp);

			/*
			 * Check if this tuple is part of a HOT-chain rooted at some other
			 * tuple. If so, skip it for now; we'll process it when we find
			 * its root.
			 */
			if (HeapTupleHeaderIsHeapOnly(htup))
				continue;

			/*
			 * This is either a plain tuple or the root of a HOT-chain.
			 * Remember it in the mapping.
			 */
			root_offsets[offnum - 1] = offnum;

			/* If it's not the start of a HOT-chain, we're done with it */
			if (!HeapTupleHeaderIsHotUpdated(htup))
				continue;

			/* Set up to scan the HOT-chain */
			nextoffnum = ItemPointerGetOffsetNumber(&htup->t_ctid);
			priorXmax = HeapTupleHeaderGetUpdateXid(htup);
		}
		else
		{
			/* Must be a redirect item. We do not set its root_offsets entry */
			Assert(ItemIdIsRedirected(lp));
			/* Set up to scan the HOT-chain */
			nextoffnum = ItemIdGetRedirect(lp);
			priorXmax = InvalidTransactionId;
		}

		/*
		 * Now follow the HOT-chain and collect other tuples in the chain.
		 *
		 * Note: Even though this is a nested loop, the complexity of the
		 * function is O(N) because a tuple in the page should be visited not
		 * more than twice, once in the outer loop and once in HOT-chain
		 * chases.
		 */
		for (;;)
		{
			lp = PageGetItemId(page, nextoffnum);

			/* Check for broken chains */
			if (!ItemIdIsNormal(lp))
				break;

			htup = (HeapTupleHeader) PageGetItem(page, lp);

			if (TransactionIdIsValid(priorXmax) &&
				!TransactionIdEquals(priorXmax, HeapTupleHeaderGetXmin(htup)))
				break;

			/* Remember the root line pointer for this item */
			root_offsets[nextoffnum - 1] = offnum;

			/* Advance to next chain member, if any */
			if (!HeapTupleHeaderIsHotUpdated(htup))
				break;

			/* HOT implies it can't have moved to different partition */
			Assert(!HeapTupleHeaderIndicatesMovedPartitions(htup));

			nextoffnum = ItemPointerGetOffsetNumber(&htup->t_ctid);
			priorXmax = HeapTupleHeaderGetUpdateXid(htup);
		}
	}
}

/*
 * GetModifiedColumnsBitmap
 *
 * TODO
 */
static Bitmapset *
GetModifiedColumnsBitmap(Relation rel, Buffer buffer, Page dp,
						 OffsetNumber oldlp, OffsetNumber newlp,
						 bool newlp_is_phot,
						 Bitmapset *interesting_attrs)
{
	Bitmapset  *modified = NULL;
	ItemId		oldid;
	ItemId		newid;
	Oid			relid;
	BlockNumber	blkno;

	relid = RelationGetRelid(rel);
	oldid = PageGetItemId(dp, oldlp);
	newid = PageGetItemId(dp, newlp);
	blkno = BufferGetBlockNumber(buffer);

	/*
	 * If all the indexes are gone, there's no way that there are any modified
	 * columns that we care about.
	 */
	if (bms_is_empty(interesting_attrs))
		return NULL;

	/*
	 * If the new tuple is a heap-only tuple but the previous one was already
	 * redirected, there's no way to get the modified columns data between the
	 * two.  This should be alright because we cannot get into a situation where
	 * this missing data would be necessary for PHOT, even if we just created a
	 * new index for a previously unindexed column.
	 */
	if (!newlp_is_phot && !ItemIdIsNormal(oldid))
		return NULL;

	if (ItemIdIsNormal(oldid))
	{
		HeapTupleData	oldtup;
		HeapTupleData	newtup;
		Bitmapset	   *interesting_copy;

		/* if the old LP is normal, the new one better be, too */
		Assert(ItemIdIsNormal(newid));

		/* prepare old tuple */
		oldtup.t_tableOid = relid;
		oldtup.t_data = (HeapTupleHeader) PageGetItem(dp, oldid);
		oldtup.t_len = ItemIdGetLength(oldid);
		ItemPointerSet(&oldtup.t_self, blkno, oldlp);

		/* prepare new tuple */
		newtup.t_tableOid = relid;
		newtup.t_data = (HeapTupleHeader) PageGetItem(dp, newid);
		newtup.t_len = ItemIdGetLength(newid);
		ItemPointerSet(&newtup.t_self, blkno, newlp);

		/*
		 * Build the return Bitmapset.  Note that we must make a copy of
		 * interesting_attrs since HeapDetermineModifiedColumns destructively
		 * modifies it.
		 */
		interesting_copy = bms_copy(interesting_attrs);
		modified = HeapDetermineModifiedColumns(rel, interesting_attrs,
												&oldtup, &newtup);
		bms_free(interesting_copy);
	}
	else
	{
		bits8  *bits;
		int		len;

		/* if the old LP isn't normal, it better be redirected-with-data */
		Assert(ItemIdIsPartialHotRedirected(dp, oldid));

		/* find the bitmap on the page */
		len = ItemIdGetRedirectDataLength(dp, oldid);
		len -= sizeof(RedirectHeaderData);
		bits = (bits8 *) ItemIdGetRedirectData(dp, oldid);

		/* build the return Bitmapset */
		for (int i = 0; i < len * 8; i++)
		{
			if (bits[i / 8] & (1 << (i % 8)))
				modified = bms_add_member(modified,
							   i - FirstLowInvalidHeapAttributeNumber);
		}

		/* the indexed columns might've changed */
		modified = bms_intersect(modified, interesting_attrs);
	}

	return modified;
}

/*
 * StoreModifiedColumnsBitmap
 *
 * TODO: describe and WAL-log!
 */
static void
StoreModifiedColumnsBitmap(Bitmapset *data, int natts, bits8 **bits)
{
	int		attr;
	int		len;

	/* prepare some memory */
	len = sizeof(RedirectHeaderData) + ((natts + 7) / 8);
	*bits = (bits8 *) palloc0(len);

	/* adjust the header */
	((RedirectHeader) *bits)->rlp_type = RLP_PHOT;
	((RedirectHeader) *bits)->rlp_len = len;

	/* scooch forward to the data portion */
	*bits += sizeof(RedirectHeaderData);

	/* store the bitmap */
	while ((attr = bms_first_member(data)) != -1)
	{
		attr += FirstLowInvalidHeapAttributeNumber;
		(*bits)[attr / 8] |= (1 << (attr % 8));
	}

	/* reset the pointer to the header */
	*bits -= sizeof(RedirectHeaderData);
}
