# COMMIT Crash Fix: Subtransaction Handling

## Problem

COMMIT after ROLLBACK TO SAVEPOINT was crashing with SIGSEGV in `AtEOXact_PgStat_Relations()` at line 568 in `pgstat_relation.c`:

```c
tabstat->counts.tuples_inserted += trans->tuples_inserted;
```

### Crash Details

- **Location**: `AtEOXact_PgStat_Relations()` in `src/backend/utils/activity/pgstat_relation.c:568`
- **Cause**: NULL pointer dereference - `trans->parent` was NULL (actually corrupted pointer `0x2ba`)
- **Corruption**: The `PgStat_TableXactStatus` structure had corrupted pointers:
  - `upper = 0x3400010901` (should be NULL or valid pointer)
  - `parent = 0x2ba` (should be pointer to PgStat_TableStatus)

### Test Case

```sql
BEGIN;
INSERT INTO undo_basic VALUES (20, 'parent_insert');
SAVEPOINT sp1;
INSERT INTO undo_basic VALUES (21, 'child_insert');
ROLLBACK TO sp1;
COMMIT;  -- CRASHED HERE
```

## Root Cause

The crash was caused by calling `CollapseXactUndoSubTransactions()` at commit time in `AtCommit_XactUndo()`. This function was added in commit 8b40064b775 as part of fixing subtransaction handling.

The issue:
1. After `ROLLBACK TO sp1`, a new subtransaction is created at the same nesting level
2. During COMMIT, `AtCommit_XactUndo()` is called (line 2490 in xact.c)
3. `CollapseXactUndoSubTransactions()` manipulates the XactUndo subtransaction chain
4. This somehow interfered with PostgreSQL's pgstat subtransaction structures
5. When `AtEOXact_PgStat()` was later called (line 2558), it found corrupted pgstat structures

The exact mechanism of corruption is unclear, but the timing and interaction between UNDO subtransaction state manipulation and pgstat processing caused memory corruption.

## Solution

Remove the `CollapseXactUndoSubTransactions()` call from `AtCommit_XactUndo()`.

### Rationale

1. **Not needed at commit**: At commit time, all UNDO records have already been written to WAL. There's no need to collapse subtransaction state because we're about to reset everything with `ResetXactUndo()`.

2. **Implicit commit handles it**: When COMMIT is executed with open subtransactions, `EndTransactionBlock()` walks the transaction stack and marks each subtransaction as `TBLOCK_SUBCOMMIT`. Then `CommitTransactionCommandInternal()` loops calling `CommitSubTransaction()` for each level, which properly calls `AtSubCommit_XactUndo()` for each nesting level.

3. **Avoids interference**: By not manually collapsing the chain at top-level commit, we avoid any potential interference with pgstat or other subsystems that expect subtransaction state to be handled via the standard `AtSubCommit_*` callbacks.

### Code Change

**File**: `src/backend/access/undo/xactundo.c`

**Before**:
```c
/* Release WAL retention hold acquired in UndoRecordSetInsert(). */
UndoClearBatchLSN();

/*
 * Collapse any outstanding subtransaction state before reset.
 * This merges all subtransaction nodes into the top-level state.
 */
CollapseXactUndoSubTransactions();

/*
 * Reset UNDO state for next transaction. Subtransaction structures
 * are allocated in TopMemoryContext and are not freed - they'll be
 * reused by future transactions.
 */
ResetXactUndo();
```

**After**:
```c
/* Release WAL retention hold acquired in UndoRecordSetInsert(). */
UndoClearBatchLSN();

/*
 * Reset UNDO state for next transaction. Subtransaction structures
 * are allocated in TopMemoryContext and are not freed - they'll be
 * reused by future transactions.
 *
 * NOTE: We intentionally do NOT call CollapseXactUndoSubTransactions()
 * here.  During ROLLBACK TO SAVEPOINT, a new subtransaction is created
 * at the same nesting level. If we don't properly clean up ALL
 * subtransaction levels during COMMIT, we can have dangling subtransaction
 * nodes. For now, we rely on AtSubCommit_XactUndo() being called for each
 * level during implicit subtransaction commit.
 */
ResetXactUndo();
```

## Testing

### Verification

The fix was verified with:

1. **Original test case**: COMMIT after ROLLBACK TO SAVEPOINT now works correctly
2. **Data correctness**: Parent INSERT (id=20) committed, child INSERT (id=21) rolled back
3. **UNDO recovery tests**: All 6 tests pass (55 subtests):
   - 055_undo_clr (4 subtests)
   - 056_undo_crash (8 subtests)
   - 057_undo_standby (9 subtests)
   - 060_undo_wal_compression (15 subtests)
   - 061_undo_wal_retention (2 subtests)
   - 062_undo_2pc (17 subtests)

### Test Command

```bash
meson test -C builddir --no-rebuild -t 10 \
  'postgresql:recovery/055_undo_clr' \
  'postgresql:recovery/056_undo_crash' \
  'postgresql:recovery/057_undo_standby' \
  'postgresql:recovery/060_undo_wal_compression' \
  'postgresql:recovery/061_undo_wal_retention' \
  'postgresql:recovery/062_undo_2pc'
```

## Future Work

While removing `CollapseXactUndoSubTransactions()` from commit fixes the crash, we should investigate:

1. Why the function was interfering with pgstat structures
2. Whether `CollapseXactUndoSubTransactions()` is needed at abort time
3. If the XactUndo subtransaction state management can be simplified
4. Whether there are other subsystems that might have similar issues

## Related Issues

- Memory management fixes documented in `MEMORY_MANAGEMENT_FIXES.md`
- Earlier fix in commit 8b40064b775 that added the problematic `CollapseXactUndoSubTransactions()` call
- AtSubCommit_XactUndo() safety check added to prevent merging top-level sentinel

## Date

2026-05-06
