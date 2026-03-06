# Orvos Codebase Disabled Code (#if 0) Analysis Report

**Analysis Date:** 2026-03-06
**Total Disabled Blocks Found:** 6
**Scope:** All Orvos source files in `/src/backend/access/orvos/`

---

## Executive Summary

Six blocks of code are disabled using `#if 0` preprocessor directives. These range from CRITICAL (data integrity bugs) to LOW (debugging/development code). Recommendations provided for each block.

---

## Disabled Code Blocks

### 1. CRITICAL: Delta UPDATE Materialization - orvos_undorec.c:655
**Location:** `/src/backend/access/orvos/orvos_undorec.c:655-662`
**Status:** DISABLED (data integrity bug)
**Priority:** CRITICAL
**Lines:** 8 lines of code

```c
#if 0
ov_materialize_delta_columns(
    rel,
    deltarec->firsttid,
    deltarec->predecessor_tid,
    deltarec->natts,
    deltarec->changed_cols);
#endif
```

**Context:**
```c
// Lines 645-654 (comment explaining the purpose)
* TODO: Materialize carried-forward columns.
* The predecessor's column values need
* to be copied into the new TID's
* column B-trees before the predecessor
* can be vacuumed away.
*
* FIXME: Currently disabled due to "corrupt item array"
* bug. This means delta UPDATEs may leave incomplete
* tuples after VACUUM.
```

**Analysis:**

This is the **most critical disabled code** in the entire codebase. Here's why:

1. **Purpose:** During VACUUM of UPDATEs that modify only subset of columns (delta UPDATEs), the old row's column values must be copied to the new row before the old row is removed. This function performs that materialization.

2. **Bug:** Currently disabled due to "corrupt item array" bug (not specified further in code).

3. **Impact:**
   - **DATA INTEGRITY RISK**: Delta UPDATEs followed by VACUUM leave incomplete tuples
   - Orphaned predecessor references may exist
   - Data may become inconsistent
   - This affects correctness, not just performance

4. **Workaround:** None. The feature simply doesn't work.

**Recommendation:** **RE-ENABLE WITH PRIORITY**

**Action Items:**
1. Investigate root cause of "corrupt item array" bug
   - May be in `ov_materialize_delta_columns()` itself
   - May be in surrounding VACUUM context
   - Check for off-by-one errors, buffer overflow, type confusion
2. Add debugging/assertions to isolate corruption
3. Create comprehensive test cases for:
   - Delta UPDATEs (UPDATE with subset of columns changed)
   - VACUUM immediately after delta UPDATEs
   - Query results after delta UPDATE + VACUUM
   - UPDATE chains with mixed full/partial updates
4. Consider adding checksums or validation to detect incomplete tuples
5. Add production monitoring for delta UPDATE chains

**Estimated Effort:** HIGH (unknown root cause requires investigation)

---

### 2. HIGH: Validation Check - orvos_undolog.c:312
**Location:** `/src/backend/access/orvos/orvos_undolog.c:312-322`
**Status:** DISABLED (moved to callers per comment)
**Priority:** HIGH (for correctness)
**Lines:** 11 lines of code

```c
#if 0							/* FIXME: move this to the callers? */
if (memcmp(&undorec->undorecptr, &undoptr, sizeof(OVUndoRecPtr)) != 0)
{
    /*
     * this should not happen in the case that the page was recycled for
     * other use, so error even if 'fail_ok' is true
     */
    elog(ERROR, "could not find UNDO record " UINT64_FORMAT " at blk %u offset %u",
         undoptr.counter, undoptr.blkno, undoptr.offset);
}
#endif
```

**Context:**
Located in UNDO record fetching function. This block validates that the UNDO record found at the expected location actually matches what we expected.

**Analysis:**

1. **Purpose:** Safety check to detect:
   - Page recycling issues
   - Incorrect UNDO record lookups
   - Corruption or misalignment

2. **Why Disabled:** Comment suggests "move this to the callers". This indicates:
   - Validation may be redundant if callers check properly
   - Or it was moved but never completed
   - Or it's a TODO to refactor the responsibility

3. **Current State:** The validation is **completely missing** - not in this function AND unclear if moved to callers.

4. **Risk:**
   - Silent failures if UNDO record lookup is wrong
   - Page recycling could go undetected
   - Could lead to applying wrong UNDO records

**Recommendation:** **INVESTIGATE AND EITHER RE-ENABLE OR DOCUMENT**

**Action Items:**
1. Verify if validation was actually moved to callers
   - Search for similar validation in callers of this function
   - Check if any validation exists
2. If NOT moved:
   - Re-enable the check
   - Add unit tests for page recycling scenarios
3. If moved:
   - Document where responsibility moved to
   - Add comment referencing the callers
   - Verify callers actually perform validation
4. Consider making this a debug-time check that's always enabled
5. Add metrics/logging for when this check would trigger

**Estimated Effort:** MEDIUM (investigation + possible re-enabling)

---

### 3. MEDIUM: Unused Function - orvos_handler.c:1934
**Location:** `/src/backend/access/orvos/orvos_handler.c:1934-1944`
**Status:** DISABLED (not currently used)
**Priority:** LOW/MEDIUM
**Lines:** 11 lines of code

```c
#if 0						/* not currently used */
static TransactionId
orvosam_compute_xid_horizon_for_tuples(Relation rel,
                                       ItemPointerData *items,
                                       int nitems)
{
    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("function %s not implemented yet", __func__)));
}
#endif
```

**Analysis:**

1. **Purpose:** Table AM method stub for computing transaction ID horizons
2. **Status:** Not implemented, just raises ERROR
3. **Why Disabled:** Not part of currently active code path
4. **Dead Code:** This is a stub that would crash if called. It's defensive code kept for potential future use.

**Recommendation:** **DELETE**

**Reasoning:**
- Function only raises ERROR - completely non-functional
- If/when needed, can be properly implemented
- Keeping stub code adds maintenance burden
- Clear from git history what was removed
- Comment "not currently used" doesn't provide value

**Action Items:**
1. Remove the entire disabled function
2. If feature becomes needed, implement it properly rather than use this stub
3. Similar stubs should be removed (see #4 below)

**Estimated Effort:** LOW (just delete)

---

### 4. MEDIUM: Unused Function - orvos_handler.c:1957
**Location:** `/src/backend/access/orvos/orvos_handler.c:1957-1966`
**Status:** DISABLED (not currently used)
**Priority:** LOW/MEDIUM
**Lines:** 10 lines of code

```c
#if 0						/* not currently used */
static void
orvosam_fetch_set_column_projection(struct IndexFetchTableData *scan,
                                    Bitmapset *project_columns)
{
    OrvosIndexFetch zscan = (OrvosIndexFetch) scan;

    zscan->proj_data.project_columns = project_columns;
}
#endif
```

**Analysis:**

1. **Purpose:** Sets column projection on index fetch scan
2. **Status:** Stub implementation, not called
3. **Context:** Similar table AM interface stub
4. **Dead Code:** Infrastructure exists (`proj_data.project_columns` field), but setting it through this function isn't used

**Recommendation:** **DELETE**

**Reasoning:**
- Incomplete implementation (just assigns one field)
- No active code path uses this
- If needed, can be properly implemented
- Keeping stubs adds maintenance burden

**Action Items:**
1. Remove the entire disabled function
2. Note: Column projection infrastructure exists and might be used directly
3. If feature becomes needed, implement full column projection support

**Estimated Effort:** LOW (just delete)

---

### 5. MEDIUM: Debug Code - orvos_handler.c:3049
**Location:** `/src/backend/access/orvos/orvos_handler.c:3049-3052`
**Status:** DISABLED (TODO comment suggests incomplete)
**Priority:** MEDIUM
**Lines:** 4 lines of code

```c
/* Set total heap blocks */
/* TODO */
#if 0
    pgstat_progress_update_param(PROGRESS_CLUSTER_TOTAL_HEAP_BLKS,
                                 heapScan->rs_nblocks);
#endif
```

**Analysis:**

1. **Purpose:** Update progress tracking for CLUSTER operations
2. **Status:** Disabled with "TODO" comment
3. **Context:** Part of CLUSTER implementation (in 2-phase CLUSTER rewrite)
4. **Why Disabled:**
   - `heapScan` variable doesn't exist in this context (Orvos doesn't have "heap" scans)
   - Progress tracking for Orvos CLUSTER not implemented
   - This is partial code from heap implementation adaptation

5. **Impact:**
   - CLUSTER operations don't report progress properly
   - Not critical but poor user experience
   - Orvos-specific approach needed

**Recommendation:** **REPLACE WITH PROPER IMPLEMENTATION**

**Action Items:**
1. Implement Orvos-specific progress tracking for CLUSTER
   - Get total row count from Orvos metadata
   - Update `PROGRESS_CLUSTER_TOTAL_HEAP_BLKS` with equivalent metric
   - Or use `PROGRESS_CLUSTER_TOTAL_TUPLES`
2. Track progress during cluster operation
3. Delete this disabled stub once new implementation complete
4. Add similar progress tracking for other long-running operations

**Estimated Effort:** MEDIUM (requires Orvos-specific implementation)

---

### 6. LOW: Disabled Alternative Implementation - orvos_handler.c:3558
**Location:** `/src/backend/access/orvos/orvos_handler.c:3558-3667`
**Status:** DISABLED (not currently used, alternative implementation)
**Priority:** LOW
**Lines:** 110 lines of code

```c
#if 0						/* not currently used */
static bool
orvosam_scan_bitmap_next_block(TableScanDesc sscan,
                               TBMIterateResult *tbmres)
{
    // ... 110 lines of bitmap scan implementation

    // Comments indicate implementation details and FIXME notes
    FIXME: heapam acquires the predicate lock first, and then calls
    CheckForSerializableConflictOut(). We do it in the opposite order,
    because CheckForSerializableConflictOut() call as done in
    ovbt_get_last_tid() already. Does it matter? I'm not sure.
}
#endif

// Followed by active implementation:
static bool
orvosam_scan_bitmap_next_tuple(TableScanDesc sscan, ...)
{
    // Current, simpler implementation
}
```

**Analysis:**

1. **Purpose:** Original bitmap scan implementation for Orvos
2. **Status:** Superseded by simpler implementation at line 3678
3. **Reason Disabled:** Current (simpler) implementation preferred
   - Line 3669-3677 comments explain:
     - Orvos doesn't have traditional block-oriented structure
     - Current approach does sequential scan + visibility checks
     - Future: could use bitmap to skip TID ranges more efficiently

4. **Code Quality:** Well-implemented but complex:
   - Handles predicate locking
   - Complex TID range scanning
   - Multiple FIXME notes about ordering and optimizations
   - ~110 lines of sophisticated logic

5. **When Would Be Useful:**
   - If need to optimize bitmap scans with TID range skipping
   - If predicate locking issues arise with current implementation
   - If performance testing shows current approach is bottleneck

**Recommendation:** **DELETE**

**Reasoning:**
- Current implementation is simpler and working
- If future optimization needed, can be re-implemented properly
- Git history preserves the deleted code
- Disabled code adds maintenance burden
- Comments in current implementation explain future optimization opportunities

**Caveats:**
- Before deleting, verify current implementation handles:
  - Predicate locking correctly
  - Bitmap scan semantics fully
  - All edge cases
- Consider leaving comments that reference commit hash if deleted

**Action Items:**
1. Verify current implementation is production-ready
2. Run bitmap scan regression tests
3. Delete disabled code
4. Keep comments about future optimization opportunities

**Estimated Effort:** LOW (just delete after verification)

---

## Summary Table

| Location | Type | Priority | Action | Effort |
|----------|------|----------|--------|--------|
| orvos_undorec.c:655 | Data Integrity Bug | CRITICAL | Re-enable + debug | HIGH |
| orvos_undolog.c:312 | Validation Check | HIGH | Investigate/Re-enable | MEDIUM |
| orvos_handler.c:1934 | Unused Stub | LOW | Delete | LOW |
| orvos_handler.c:1957 | Unused Stub | LOW | Delete | LOW |
| orvos_handler.c:3049 | Incomplete Progress Tracking | MEDIUM | Replace | MEDIUM |
| orvos_handler.c:3558 | Alternative Implementation | LOW | Delete | LOW |

---

## Recommended Action Plan

### Phase 1: CRITICAL (Do Immediately)
1. **orvos_undorec.c:655** - Investigate "corrupt item array" bug
   - This affects data integrity for delta UPDATEs
   - Estimated: 2-4 hours investigation

### Phase 2: HIGH (Next Sprint)
2. **orvos_undolog.c:312** - Verify validation moved to callers or re-enable
   - Important for catching UNDO record corruption
   - Estimated: 1-2 hours

### Phase 3: MEDIUM (Maintenance Phase)
3. **orvos_handler.c:3049** - Implement proper Orvos CLUSTER progress tracking
   - Improves user experience
   - Estimated: 2-3 hours

### Phase 4: LOW (Code Cleanup)
4. **orvos_handler.c:1934** - Delete unused stub
5. **orvos_handler.c:1957** - Delete unused stub
6. **orvos_handler.c:3558** - Delete alternative implementation after verification
   - Estimated: 30 minutes (after verification)

---

## Notes

- **No show-stoppers except #1**: The delta UPDATE materialization bug is the only critical issue that affects data integrity
- **Dead code burden**: Four disabled code blocks are truly dead code that should be deleted (stubs, alternatives)
- **Verification needed**: Two blocks need investigation to understand their current status (validation check, progress tracking)
- **Git preservation**: Modern version control means deleted code can always be recovered from history - no need to keep it as #if 0 blocks
