# UNDO-Informed Index Pruning: Design Notes

## Overview

UNDO-informed index pruning reduces VACUUM overhead by allowing index access
methods to proactively remove index entries that point to UNDO-rolled-back
tuples. When a per-relation UNDO discard event occurs, registered index AMs
receive a notification and can prune stale entries without waiting for VACUUM.

## Architecture

### Infrastructure (`src/backend/access/common/index_prune.c`)

The index pruning infrastructure provides:

1. **Handler registration**: Each index AM registers a pruning callback via
   `IndexPruneRegisterHandler(amoid, callback)` during `_PG_init()` or handler
   initialization.

2. **Discard notification**: When `RelUndoDiscard()` processes completed UNDO
   chains, it calls `IndexPruneNotifyDiscard(rel, discarded_tids, ntids)`.
   This iterates registered handlers for each index on the relation.

3. **Graceful degradation**: If no handler is registered for an index AM,
   the notification is silently skipped. This means adding a new index AM
   never breaks existing functionality.

### Per-AM Pruning Handlers

Each index AM implements pruning differently based on its internal structure:

**NBTREE** (`nbtprune.c`):
- Descends the B-tree to find leaf pages containing target TIDs
- Uses `_bt_search()` to locate entries efficiently
- Marks entries as dead via `_bt_killitems()` pattern
- O(log N) per TID due to tree traversal

**HASH** (`hashprune.c`):
- Computes bucket from index key
- Scans overflow chain for matching TIDs
- Removes entries and compacts pages
- O(1) amortized per TID (direct bucket access)

**GIN** (`ginprune.c`):
- Searches posting trees/lists for target TIDs
- Handles both posting list (inline) and posting tree (B-tree) cases
- Removes TIDs from posting lists, potentially compacting entries
- O(log N) per TID for posting trees

**GiST** (`gistprune.c`):
- Descends internal pages following consistent() checks
- Marks leaf entries as dead
- Does not immediately restructure (deferred to VACUUM split)
- O(log N) per TID

**SP-GiST** (`spgprune.c`):
- Follows inner tuples via choose() to reach leaf pages
- Handles redirect and placeholder tuples
- Marks leaf tuples as dead
- O(log N) per TID

### VACUUM Integration

The VACUUM integration in `vacuumlazy.c` tracks statistics from UNDO-informed
pruning:

- `index_undo_pruned`: Count of index entries pruned by UNDO notifications
- Reported in VACUUM VERBOSE output
- Reduces the number of entries VACUUM itself needs to process

## Design Decisions

### Why per-AM handlers instead of a generic approach?

Each index AM has different internal structure (B-tree, hash buckets, posting
lists, etc.). A generic approach would either be too conservative (not pruning
when safe) or require exposing internal details through a complex abstraction.
Per-AM handlers let each AM use its native operations for maximum efficiency.

### Why notifications instead of polling?

Push-based notifications (from UNDO discard to index AM) are more efficient
than polling because:
1. UNDO discard already knows which TIDs are affected
2. Polling would require scanning index entries to check UNDO status
3. Notifications integrate naturally with the existing UNDO lifecycle

### Why not prune during UNDO apply?

UNDO apply runs in a background worker that may not have appropriate locks
on all indexes. The discard phase runs after UNDO is fully applied, when
the transaction is complete and index locks can be acquired normally.
