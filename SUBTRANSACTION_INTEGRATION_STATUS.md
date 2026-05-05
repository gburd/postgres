# Subtransaction Integration Status

## Date
2026-05-06

## Problem Statement

The UNDO subsystem has functions `AtSubCommit_XactUndo()` and `AtSubAbort_XactUndo()` that are never called from `xact.c`. The code comment in `xactundo.c` line 554 explicitly says "we rely on AtSubCommit_XactUndo() being called for each level during implicit subtransaction commit", but these function calls don't exist.

## Root Cause

The UNDO subsystem was designed to integrate with PostgreSQL's subtransaction commit/abort hooks, but the integration was never completed. The functions exist and are declared in `xactundo.h`, but are not wired up in `xact.c`.

## Impact

Without proper subtransaction integration:
1. **Savepoint+COMMIT crashes**: COMMIT after ROLLBACK TO SAVEPOINT causes segfaults
2. **TOAST operations crash**: Parallel regression tests involving TOAST tables crash  
3. **Test failures**: 4 regression test suites fail (test_plan_advice, pg_upgrade, recovery/027_stream_regress, regress)

## Attempted Fix

Added calls to `AtSubCommit_XactUndo()` and `AtSubAbort_XactUndo()` in `xact.c`:
- In `CommitSubTransaction()` after `AtEOSubXact_PgStat()`
- In `AbortSubTransaction()` after `AtEOSubXact_PgStat()`

## Current Status

Adding the function calls causes crashes. The issue is that after ROLLBACK TO SAVEPOINT:
1. `AbortSubTransaction()` is called, which calls `AtSubAbort_XactUndo()` and pops the UNDO subtxn node
2. `DefineSavepoint(NULL)` creates a new TransactionState at the same nesting level
3. `StartSubTransaction()` initializes it, but UNDO subsystem doesn't get notified
4. During COMMIT, `AtSubCommit_XactUndo()` is called for this new subtxn node
5. The node has no proper UNDO state initialization, causing crashes

## The Fundamental Problem

The UNDO subsystem doesn't use PostgreSQL's standard `SubXactCallback` registration mechanism. Instead, it expects direct calls from `xact.c`. But ROLLBACK TO SAVEPOINT creates new subtransactions that the UNDO subsystem never learns about, leading to state inconsistency.

## Possible Solutions

### Option 1: Register SubXactCallback  
Register a proper `SubXactCallback` that gets called for SUBXACT_EVENT_START_SUB, SUBXACT_EVENT_COMMIT_SUB, etc. This would properly track all subtransaction state changes including post-ROLLBACK subtransactions.

### Option 2: Lazy Initialization
Modify `AtSubCommit_XactUndo()` to handle uninit

ialized subtransaction nodes by checking if `subxact->next == NULL` and treating them as no-ops.

### Option 3: Different Integration Point
Don't call these functions from `CommitSubTransaction`/`AbortSubTransaction` at all. Instead, handle subtransaction management entirely within the UndoBufferBegin/UndoBufferEnd layer.

## What Was Tried

Added defensive code to `AtSubCommit_XactUndo()`:
```c
if (subxact->next == NULL)
{
    XactUndo.subxact = &XactUndoTopState;
    return;
}
```

This still crashes, suggesting deeper state corruption issues.

## Next Steps Required

1. Understand why TOAST operations specifically trigger crashes
2. Investigate if RegisterSubXactCallback is the right approach
3. Check if the UNDO tier2 buffer state needs subtransaction tracking
4. Consider if UNDO subtransaction management should be deferred entirely

## Files Modified

- `src/backend/access/transam/xact.c`: Added AtSubCommit/Abort_XactUndo calls (causes crashes)
- `src/backend/access/undo/xactundo.c`: Added defensive null check in AtSubCommit_XactUndo

## Test Case

```sql
CREATE TABLE undo_basic (id int, data text) WITH (enable_undo = on);
INSERT INTO undo_basic VALUES (1, 'initial');

BEGIN;
INSERT INTO undo_basic VALUES (20, 'parent_insert');
SAVEPOINT sp1;
INSERT INTO undo_basic VALUES (21, 'child_insert');
ROLLBACK TO sp1;
COMMIT;  -- CRASH HERE
```

## References

- COMMIT_CRASH_FIX.md: Documents earlier attempt to fix COMMIT crashes
- xactundo.c line 554: Comment stating reliance on AtSubCommit_XactUndo
- xact.c line 3512-3524: ROLLBACK TO SAVEPOINT implementation (TBLOCK_SUBABORT_RESTART)
