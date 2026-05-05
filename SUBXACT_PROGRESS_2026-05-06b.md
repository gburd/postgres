# Subtransaction Integration Progress - 2026-05-06 (Continued)

## Investigation Summary

Continued investigating the pgstat corruption crash that occurs during COMMIT after ROLLBACK TO SAVEPOINT with UNDO enabled.

### Key Findings

1. **Crash occurs ONLY with UNDO enabled**: Test passes without `enable_undo`, confirming our SubXactCallback causes the issue

2. **Memory context experiments**:
   - Tried `CurTransactionContext`: Still crashes
   - Reverted to `TopTransactionContext` (matching pgstat): Still crashes
   - Added explicit `pfree()` calls: Still crashes

3. **The crash is NOT a memory leak**: It's corruption of pgstat's level-1 trans structure during subtransaction processing

4. **Crash location**: `src/backend/utils/activity/pgstat_relation.c:574`
   ```c
   tabstat->counts.tuples_inserted += trans->tuples_inserted;
   ```
   Where `tabstat = trans->parent` is corrupted (typically shows value like 0x2b7)

### What We Tried

1. **Using CurTransactionContext** (lines 939, result: still crashes)
   - Each subtransaction creates its own CurTransactionContext
   - Nodes allocated there survive COMMIT (context kept if not empty)
   - Nodes freed during ABORT cleanup (context deleted)
   - Theory: Should avoid conflicts with pgstat's TopTransactionContext
   - Reality: Crash persists

2. **Using TopTransactionContext with explicit pfree()** (current state)
   - Matches what pgstat does for its trans structures
   - Added `pfree(subxact)` in both AtSubCommit_XactUndo and AtSubAbort_XactUndo
   - Theory: Explicit cleanup should prevent corruption
   - Reality: Crash persists

3. **Diagnostic logging**:
   - Added DEBUG1 logging to pgstat functions (committed in ec1c482f88e)
   - Confirmed UNDO operations complete successfully before crash
   - Crash occurs during top-level COMMIT, not during subtransaction processing

### Current Status

**Problem**: Something about allocating XactUndoSubTransaction nodes during SubXactCallback corrupts pgstat's memory structures, even with proper cleanup.

**Hypothesis**: The issue may not be WHERE we allocate (which context) or HOW we free, but WHEN we allocate (during the SubXactCallback) or WHAT we're allocating (size/alignment interactions with pgstat structures).

### Files Modified

- `src/backend/access/undo/xactundo.c`:
  - Changed `TopMemoryContext` → `TopTransactionContext` at line 939
  - Added `pfree(subxact)` in `AtSubCommit_XactUndo()` at line 773
  - Added `pfree(subxact)` in `AtSubAbort_XactUndo()` at line 879
  - Updated comments explaining memory management strategy

- `src/backend/utils/activity/pgstat_relation.c`:
  - Added DEBUG1 logging (committed separately in ec1c482f88e)

### Next Steps

**Option 1: Don't allocate during callback**
- Store subtransaction state in a fixed-size array instead of linked list
- Avoid any memory allocation during SubXactCallback
- Track nest levels using array indices

**Option 2: Investigate callback ordering**
- Check if our callback fires at the wrong time relative to pgstat setup
- Maybe we need to use a different callback event (PRE_COMMIT_SUB?)
- Or defer node creation until first UNDO operation, not at START_SUB

**Option 3: Deep debug with GDB**
- Set breakpoint at memory allocation in TopTransactionContext
- Watch for writes to pgstat trans structures
- Identify exact moment of corruption

**Option 4: Consult PostgreSQL internals experts**
- This may be hitting an undocumented limitation of SubXactCallback
- Need guidance on safe memory operations during callbacks

### References

- Previous investigation: `SUBXACT_PROGRESS_2026-05-06.md`
- Diagnostic commit: ec1c482f88e "pgstat: Add diagnostic logging"
- Test script: `/tmp/test_with_gdb.sh`
- Working test (no UNDO): Passes with `enable_undo = off`
