/*-------------------------------------------------------------------------
 *
 * index_prune.h
 *	  UNDO-informed index pruning infrastructure
 *
 * This module provides callbacks that allow the UNDO discard worker to
 * proactively mark index entries as dead when UNDO records are discarded.
 * This reduces VACUUM work by pre-marking LP_DEAD entries before index
 * scanning occurs.
 *
 * ARCHITECTURE:
 * -------------
 * When the UNDO discard worker determines that UNDO records with a certain counter
 * are no longer visible to any snapshot, it calls IndexPruneNotifyDiscard().
 * This function invokes registered callback functions for each index on the
 * relation, allowing each index AM to mark its entries as dead.
 *
 * Index AMs register pruning callbacks via IndexPruneRegisterHandler().
 * The callback receives the relation, index, and discard counter, and is
 * responsible for scanning the index and marking dead entries.
 *
 * VACUUM integration:
 * ------------------
 * During heap scanning, VACUUM checks if entries are already marked LP_DEAD
 * by the UNDO pruning system. If so, it skips those entries, avoiding
 * redundant index scanning work.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/index_prune.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef INDEX_PRUNE_H
#define INDEX_PRUNE_H

#include "postgres.h"
#include "utils/rel.h"

/*
 * IndexPruneCallback
 *
 * Callback function signature for index AM pruning handlers.
 *
 * Parameters:
 *   heaprel      - The heap relation being processed
 *   indexrel     - The index relation to prune
 *   discard_counter - UNDO counter value; entries referencing UNDO records
 *                     with counter < discard_counter should be marked dead
 *
 * Returns:
 *   Number of index entries marked as dead
 *
 * The callback should:
 *   1. Scan the index for entries that reference the heap relation
 *   2. For each entry, check if its UNDO counter < discard_counter
 *   3. Mark qualifying entries as LP_DEAD
 *   4. Return the count of marked entries
 *
 * Implementation notes:
 *   - Must be lightweight and not hold locks for extended periods
 *   - Should use buffer locking to avoid conflicts with concurrent scans
 *   - Should maintain statistics for monitoring effectiveness
 */
typedef uint64 (*IndexPruneCallback) (Relation heaprel, Relation indexrel,
									  uint16 discard_counter);

/*
 * IndexPruneHandler
 *
 * Structure representing a registered index pruning handler for an index AM.
 * Each index type (btree, gin, gist, hash, spgist) registers its own handler
 * during initialization.
 */
typedef struct IndexPruneHandler
{
	Oid			indexam_oid;	/* Index AM OID (e.g., BTREE_AM_OID) */
	IndexPruneCallback callback;	/* Callback function for this AM */
}			IndexPruneHandler;

/*
 * IndexPruneStats
 *
 * Statistics tracking for index pruning operations. Used to monitor
 * effectiveness and performance of UNDO-informed pruning.
 */
typedef struct IndexPruneStats
{
	uint64		total_entries_pruned;	/* Total entries marked dead */
	uint64		total_indexes_scanned;	/* Total indexes processed */
	uint64		total_prune_calls;	/* Number of prune operations */
	uint64		total_prune_time_ms;	/* Cumulative time spent pruning */
}			IndexPruneStats;

/*
 * IndexPruneTarget
 *
 * A targeted index pruning entry.  Instead of scanning all leaf pages,
 * the discard worker can provide a list of specific (index_oid, blkno,
 * offset) targets extracted from UNDO records in the discarded range.
 * This reduces complexity from O(N_total_index_entries) to
 * O(N_dead_entries).
 */
typedef struct IndexPruneTarget
{
	Oid			index_oid;		/* Index relation OID */
	BlockNumber blkno;			/* Index page containing the entry */
	OffsetNumber offset;		/* Offset of the entry within the page */
	ItemPointerData heap_tid;	/* Referenced heap TID for verification */
}			IndexPruneTarget;

/*
 * IndexPruneTargetedCallback
 *
 * Callback for targeted index pruning.  Receives a batch of targets
 * for a single index relation and prunes only those specific entries.
 */
typedef uint64 (*IndexPruneTargetedCallback) (Relation heaprel,
											  Relation indexrel,
											  IndexPruneTarget * targets,
											  int ntargets);

/*
 * Public API functions
 */

/*
 * IndexPruneNotifyDiscard
 *
 * Called by the UNDO discard worker to notify all indexes on a relation that
 * UNDO records with counter < discard_counter have been discarded.
 *
 * This function iterates through all indexes on heaprel and invokes
 * the registered pruning callback for each index AM type.
 *
 * Parameters:
 *   heaprel          - Heap relation whose UNDO was discarded
 *   discard_counter  - UNDO counter; records with counter < this are dead
 */
extern void IndexPruneNotifyDiscard(Relation heaprel, uint16 discard_counter);

/*
 * IndexPruneNotifyTargeted
 *
 * Called by the cluster-wide UNDO discard worker with specific targets
 * extracted from nbtree UNDO records in the discarded segment range.
 * Only visits the specified index pages, avoiding full index scans.
 *
 * Complexity: O(N_dead_entries) instead of O(N_total_entries).
 */
extern uint64 IndexPruneNotifyTargeted(Relation heaprel,
									   IndexPruneTarget * targets,
									   int ntargets);

/*
 * IndexPruneRegisterHandler
 *
 * Registers a pruning callback handler for a specific index AM.
 * Called during index AM initialization (e.g., in _bt_init() for btree).
 *
 * Parameters:
 *   indexam_oid - OID of the index access method
 *   callback    - Callback function to invoke for pruning
 */
extern void IndexPruneRegisterHandler(Oid indexam_oid,
									  IndexPruneCallback callback);

/*
 * IndexPruneGetStats
 *
 * Returns cumulative pruning statistics. Used for monitoring and
 * performance analysis.
 *
 * Returns:
 *   Pointer to the global IndexPruneStats structure
 */
extern IndexPruneStats * IndexPruneGetStats(void);

/*
 * IndexPruneResetStats
 *
 * Resets pruning statistics to zero. Called by pg_stat_reset().
 */
extern void IndexPruneResetStats(void);

/*
 * IndexPruneRegisterTargetedHandler
 *
 * Registers a targeted pruning callback handler for a specific index AM.
 */
extern void IndexPruneRegisterTargetedHandler(Oid indexam_oid,
											  IndexPruneTargetedCallback callback);

/*
 * Index AM-specific pruning functions
 *
 * These are the actual implementation functions for each index AM.
 * They are called via the callback mechanism by IndexPruneNotifyDiscard().
 */
extern uint64 _bt_prune_by_undo_counter(Relation heaprel, Relation indexrel,
										uint16 discard_counter);
extern uint64 _bt_prune_by_targets(Relation heaprel, Relation indexrel,
								   IndexPruneTarget * targets, int ntargets);
extern uint64 gin_prune_by_undo_counter(Relation heaprel, Relation indexrel,
										uint16 discard_counter);
extern uint64 gist_prune_by_undo_counter(Relation heaprel, Relation indexrel,
										 uint16 discard_counter);
extern uint64 hash_prune_by_undo_counter(Relation heaprel, Relation indexrel,
										 uint16 discard_counter);
extern uint64 spg_prune_by_undo_counter(Relation heaprel, Relation indexrel,
										uint16 discard_counter);

#endif							/* INDEX_PRUNE_H */
