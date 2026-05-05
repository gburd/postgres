# Subtransaction Integration Progress - 2026-05-06

## Status: SubXactCallback Working, But Causes PgStat Corruption

### What Works

1. **SubXactCallback registration**: Successfully implemented `XactUndo_SubXactCallback()` and registered it via `RegisterSubXactCallback()` in `InitializeXactUndo()`

2. **Callback invocation**: All subtransaction events are correctly received:
   - `START_SUB` - Creates new subtransaction node
   - `COMMIT_SUB` - Calls `AtSubCommit_XactUndo()`
   - `ABORT_SUB` - Calls `AtSubAbort_XactUndo()`

3. **UNDO operations complete successfully**: Both `AtSubCommit_XactUndo()` and `AtCommit_XactUndo()` run to completion without errors

4. **Savepoint test case executes correctly**:
   ```sql
   BEGIN;
   INSERT INTO undo_basic VALUES (20, 'parent');
   SAVEPOINT sp1;
   INSERT INTO undo_basic VALUES (21, 'child');
   ROLLBACK TO sp1;  -- Correctly triggers ABORT_SUB + new START_SUB
   COMMIT;           -- Triggers COMMIT_SUB for new subtxn, then crashes
   ```

###The Crash

**Location**: `src/backend/utils/activity/pgstat_relation.c:568`

**Function**: `AtEOXact_PgStat_Relations()`

**Line**: `tabstat->counts.tuples_inserted += trans->tuples_inserted;`

**Backtrace**:
```
#0  AtEOXact_PgStat_Relations (xact_state=0x55555645ba78, isCommit=true) at pgstat_relation.c:568
#1  AtEOXact_PgStat (isCommit=true, parallel=false) at pgstat_xact.c:53
#2  CommitTransaction () at xact.c:2558
#3  CommitTransactionCommandInternal () at xact.c:3457
```

**Root Cause**: `tabstat` pointer is NULL or invalid. The pgstat transaction parent pointer (`trans->parent`) is corrupted.

### Call Order During COMMIT

1. `CallSubXactCallbacks(SUBXACT_EVENT_COMMIT_SUB, ...)` - line 5297 xact.c
   - Our `XactUndo_SubXactCallback()` fires
   - `AtSubCommit_XactUndo()` runs successfully
   - Pops subtransaction node: `XactUndo.subxact = subxact->next`

2. `AtEOSubXact_PgStat(true, s->nestingLevel)` - line 5310 xact.c
   - Merges subtransaction statistics into parent
   - Runs successfully (no crash here)

3. Later during main transaction commit:
   - `AtCommit_XactUndo()` runs successfully
   - `AtEOXact_PgStat()` called
   - `AtEOXact_PgStat_Relations()` crashes accessing `tabstat->counts`

### Key Findings

1. **UNDO subsystem works correctly**: All UNDO functions complete without errors

2. **Crash is in pgstat, not UNDO**: The crash happens in PostgreSQL's statistics subsystem during transaction-end processing

3. **Timing**: Crash occurs ~325ms after UNDO cleanup completes, suggesting pgstat is accessing freed or corrupted memory structures

4. **Hypothesis**: The subtransaction state changes we're making (creating/destroying UNDO subtxn nodes) are somehow corrupting pgstat's parallel subtransaction tracking structures

### Files Modified

- `src/backend/access/undo/xactundo.c`:
  - Added `subxact_callback_registered` flag
  - Implemented `XactUndo_SubXactCallback()` with START_SUB/COMMIT_SUB/ABORT_SUB handling
  - Modified `InitializeXactUndo()` to register callback
  - Added extensive DEBUG logging to trace execution

- `src/backend/access/undo/undo.c`:
  - Added WARNING and fprintf() logging to `InitializeUndo()`

- `src/backend/utils/init/postinit.c`:
  - Added logging around `InitializeUndo()` call (for debugging)

### Next Steps

1. **Investigate pgstat corruption**: Understand why our subtransaction handling corrupts pgstat state
   - Check if memory context issues (we allocate in TopMemoryContext)
   - Check if there's shared state between UNDO and pgstat
   - Check if subtransaction ID reuse is causing confusion

2. **Possible fixes**:
   - Change memory allocation strategy for subtxn nodes
   - Ensure proper cleanup/reset of state that pgstat might depend on
   - Check if we need to handle PRE_COMMIT_SUB event

3. **Test isolation**: Create minimal test case that reproduces pgstat corruption without UNDO operations to isolate the root cause

### References

- Original issue: SUBTRANSACTION_INTEGRATION_STATUS.md
- Test script: /tmp/test_sp_debug.sh
- GDB backtrace: /tmp/gdb_output.txt (from test_with_gdb.sh)
