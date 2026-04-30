/*-------------------------------------------------------------------------
 *
 * index_prune.c
 *	  UNDO-informed index pruning infrastructure
 *
 * This module implements the core notification and callback dispatch system
 * for UNDO-informed index pruning. When the UNDO discard worker determines
 * that UNDO records are no longer visible, it notifies all indexes on the
 * relation, allowing them to proactively mark dead entries.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/common/index_prune.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/index_prune.h"
#include "access/relation.h"
#include "access/relundo.h"
#include "catalog/index.h"
#include "portability/instr_time.h"
#include "utils/rel.h"
#include "utils/relcache.h"

/* Maximum number of index AM handlers we support */
#define MAX_INDEX_HANDLERS 16

/*
 * Global handler registry
 *
 * Index AMs register their pruning callbacks here during initialization.
 * The registry is protected by a simple array since registration happens
 * only at startup and lookups are read-only during normal operation.
 */
static IndexPruneHandler handlers[MAX_INDEX_HANDLERS];
static int	num_handlers = 0;

/*
 * Targeted handler registry
 */
typedef struct IndexPruneTargetedHandler
{
	Oid			indexam_oid;
	IndexPruneTargetedCallback callback;
}			IndexPruneTargetedHandler;

static IndexPruneTargetedHandler targeted_handlers[MAX_INDEX_HANDLERS];
static int	num_targeted_handlers = 0;

/*
 * Global pruning statistics
 *
 * Tracks cumulative statistics for monitoring and performance analysis.
 */
static IndexPruneStats prune_stats;

/*
 * IndexPruneRegisterHandler
 *
 * Registers a pruning callback handler for a specific index AM.
 * Called during index AM initialization.
 */
void
IndexPruneRegisterHandler(Oid indexam_oid, IndexPruneCallback callback)
{
	if (num_handlers >= MAX_INDEX_HANDLERS)
	{
		elog(ERROR, "too many index pruning handlers registered");
		return;
	}

	handlers[num_handlers].indexam_oid = indexam_oid;
	handlers[num_handlers].callback = callback;
	num_handlers++;

	elog(DEBUG2, "registered index pruning handler for AM OID %u", indexam_oid);
}

/*
 * IndexPruneFindHandler
 *
 * Looks up the pruning callback for a given index AM OID.
 * Returns NULL if no handler is registered.
 */
static IndexPruneCallback
IndexPruneFindHandler(Oid indexam_oid)
{
	int			i;

	for (i = 0; i < num_handlers; i++)
	{
		if (handlers[i].indexam_oid == indexam_oid)
			return handlers[i].callback;
	}

	return NULL;
}

/*
 * IndexPruneNotifyDiscard
 *
 * Notifies all indexes on a relation that UNDO records have been discarded.
 * Called by RelUndoDiscard() after determining the discard counter.
 *
 * This function:
 *   1. Opens all indexes on the heap relation
 *   2. For each index, invokes the registered pruning callback
 *   3. Updates global statistics
 *   4. Closes all indexes
 */
void
IndexPruneNotifyDiscard(Relation heaprel, uint16 discard_counter)
{
	List	   *indexoidlist;
	ListCell   *lc;
	int			num_indexes_pruned = 0;
	uint64		total_entries_pruned = 0;
	instr_time	start_time,
				end_time;

	/* Get list of index OIDs for this relation */
	indexoidlist = RelationGetIndexList(heaprel);

	if (indexoidlist == NIL)
	{
		/* No indexes, nothing to do */
		return;
	}

	INSTR_TIME_SET_CURRENT(start_time);

	/*
	 * Iterate through each index and invoke its pruning callback.
	 */
	foreach(lc, indexoidlist)
	{
		Oid			indexoid = lfirst_oid(lc);
		Relation	indexrel;
		IndexPruneCallback callback;
		uint64		entries_pruned;

		/* Open the index relation */
		indexrel = index_open(indexoid, AccessShareLock);

		/* Find the handler for this index AM */
		callback = IndexPruneFindHandler(indexrel->rd_rel->relam);

		if (callback != NULL)
		{
			/* Invoke the pruning callback */
			entries_pruned = callback(heaprel, indexrel, discard_counter);

			total_entries_pruned += entries_pruned;
			num_indexes_pruned++;

			if (entries_pruned > 0)
			{
				elog(DEBUG2, "index %s: marked %lu entries as dead for counter %u",
					 RelationGetRelationName(indexrel),
					 (unsigned long) entries_pruned,
					 discard_counter);
			}
		}
		else
		{
			/*
			 * No handler registered for this index AM. This is expected for
			 * BRIN and other index types that don't support UNDO-informed
			 * pruning.
			 */
			elog(DEBUG2, "no pruning handler for index %s (AM OID %u)",
				 RelationGetRelationName(indexrel),
				 indexrel->rd_rel->relam);
		}

		/* Close the index */
		index_close(indexrel, AccessShareLock);
	}

	INSTR_TIME_SET_CURRENT(end_time);
	INSTR_TIME_SUBTRACT(end_time, start_time);

	/* Update global statistics */
	prune_stats.total_entries_pruned += total_entries_pruned;
	prune_stats.total_indexes_scanned += num_indexes_pruned;
	prune_stats.total_prune_calls++;
	prune_stats.total_prune_time_ms += (uint64) INSTR_TIME_GET_MILLISEC(end_time);

	if (total_entries_pruned > 0)
	{
		elog(DEBUG1, "UNDO discard: pruned %lu index entries across %d indexes (counter %u)",
			 (unsigned long) total_entries_pruned,
			 num_indexes_pruned,
			 discard_counter);
	}

	list_free(indexoidlist);
}

/*
 * IndexPruneGetStats
 *
 * Returns a pointer to the global pruning statistics structure.
 */
IndexPruneStats *
IndexPruneGetStats(void)
{
	return &prune_stats;
}

/*
 * IndexPruneResetStats
 *
 * Resets all pruning statistics to zero.
 */
void
IndexPruneResetStats(void)
{
	memset(&prune_stats, 0, sizeof(IndexPruneStats));
	elog(DEBUG1, "index pruning statistics reset");
}

/*
 * IndexPruneRegisterTargetedHandler
 *
 * Registers a targeted pruning callback for a specific index AM.
 */
void
IndexPruneRegisterTargetedHandler(Oid indexam_oid,
								  IndexPruneTargetedCallback callback)
{
	if (num_targeted_handlers >= MAX_INDEX_HANDLERS)
	{
		elog(ERROR, "too many targeted index pruning handlers registered");
		return;
	}

	targeted_handlers[num_targeted_handlers].indexam_oid = indexam_oid;
	targeted_handlers[num_targeted_handlers].callback = callback;
	num_targeted_handlers++;

	elog(DEBUG2, "registered targeted index pruning handler for AM OID %u",
		 indexam_oid);
}

/*
 * IndexPruneFindTargetedHandler
 *
 * Looks up the targeted pruning callback for a given index AM OID.
 */
static IndexPruneTargetedCallback
IndexPruneFindTargetedHandler(Oid indexam_oid)
{
	int			i;

	for (i = 0; i < num_targeted_handlers; i++)
	{
		if (targeted_handlers[i].indexam_oid == indexam_oid)
			return targeted_handlers[i].callback;
	}

	return NULL;
}

/*
 * IndexPruneNotifyTargeted
 *
 * Targeted index pruning: instead of scanning all leaf pages of every
 * index, visit only the specific (index_oid, blkno, offset) targets
 * extracted from UNDO records in the discarded segment range.
 *
 * Complexity: O(N_dead_entries) instead of O(N_total_entries).
 *
 * Targets are grouped by index_oid, then each group is dispatched to
 * the appropriate AM's targeted callback.
 */
uint64
IndexPruneNotifyTargeted(Relation heaprel,
						 IndexPruneTarget * targets, int ntargets)
{
	uint64		total_entries_pruned = 0;
	int			i;
	instr_time	start_time,
				end_time;

	if (ntargets <= 0)
		return 0;

	INSTR_TIME_SET_CURRENT(start_time);

	/*
	 * Simple approach: sort targets by index_oid, then process each group.
	 * For the common case (small number of targets), linear scan is fine.
	 *
	 * We batch targets by index_oid and dispatch to the targeted callback.
	 */
	i = 0;
	while (i < ntargets)
	{
		Oid			cur_oid = targets[i].index_oid;
		int			group_start = i;
		int			group_count;
		Relation	indexrel;
		IndexPruneTargetedCallback callback;

		/* Find the end of this group (same index_oid) */
		while (i < ntargets && targets[i].index_oid == cur_oid)
			i++;
		group_count = i - group_start;

		/* Open the index */
		indexrel = try_relation_open(cur_oid, AccessShareLock);
		if (indexrel == NULL)
		{
			/* Index dropped -- skip this group */
			continue;
		}

		callback = IndexPruneFindTargetedHandler(indexrel->rd_rel->relam);
		if (callback != NULL)
		{
			uint64		pruned;

			pruned = callback(heaprel, indexrel,
							  &targets[group_start], group_count);
			total_entries_pruned += pruned;

			if (pruned > 0)
				elog(DEBUG2, "targeted prune: index %s, %lu entries pruned",
					 RelationGetRelationName(indexrel),
					 (unsigned long) pruned);
		}

		relation_close(indexrel, AccessShareLock);
	}

	INSTR_TIME_SET_CURRENT(end_time);
	INSTR_TIME_SUBTRACT(end_time, start_time);

	prune_stats.total_entries_pruned += total_entries_pruned;
	prune_stats.total_prune_calls++;
	prune_stats.total_prune_time_ms += (uint64) INSTR_TIME_GET_MILLISEC(end_time);

	return total_entries_pruned;
}
