# Orvos Codebase TODO/FIXME Analysis Report

**Analysis Date:** 2026-03-06
**Total Comments Found:** 99 TODO/FIXME/XXX/HACK comments
**Scope:** All Orvos source files in `/src/backend/access/orvos/`

---

## Executive Summary

The Orvos columnar storage implementation contains 99 comments marking incomplete work, potential improvements, and known issues. These are categorized below by priority and impact. The most critical issue is a data integrity problem with delta UPDATE materialization during VACUUM (disabled #if 0 block in orvos_undorec.c).

---

## CRITICAL Priority Issues (Data Integrity / Correctness)

### 1. Delta UPDATE Materialization Bug - orvos_undorec.c:655
**File:** `/src/backend/access/orvos/orvos_undorec.c:645-662`
**Status:** DISABLED (#if 0 block)
**Impact:** HIGH - Data integrity
**Severity:** CRITICAL

```c
// Lines 645-662
* TODO: Materialize carried-forward columns.
* The predecessor's column values need
* to be copied into the new TID's
* column B-trees before the predecessor
* can be vacuumed away.
*
* FIXME: Currently disabled due to "corrupt item array"
* bug. This means delta UPDATEs may leave incomplete
* tuples after VACUUM.
#if 0
ov_materialize_delta_columns(
    rel,
    deltarec->firsttid,
    deltarec->predecessor_tid,
    deltarec->natts,
    deltarec->changed_cols);
#endif
```

**Description:** When UPDATEs modify only a subset of columns (delta UPDATEs), Orvos creates a new row pointing to the old row as its predecessor. During VACUUM, the old row should be cleaned up after copying its column values. This materialization is currently **completely disabled** due to an "item array corruption" bug. This means:
- Incomplete tuples can remain after VACUUM
- Predecessor references may dangle
- Potential for data inconsistency

**Recommendation:**
- **RE-ENABLE with caution** - This is critical for correctness but currently causes data corruption
- Must investigate and fix the "corrupt item array" bug first
- Add comprehensive testing for delta UPDATE + VACUUM scenarios
- Consider adding assertions to catch dangling predecessors

**Blocking:** Yes - This affects VACUUM correctness and data integrity

---

### 2. Multiple FIXME Comments in orvos_visibility.c (Lines 192, 236)
**File:** `/src/backend/access/orvos/orvos_visibility.c:192, 236`
**Status:** NOT IMPLEMENTED
**Impact:** MEDIUM - Potential visibility bugs
**Severity:** HIGH

```c
// Line 192
* FIXME: Shouldn't we drill down to the INSERT record and check if
* [visibility determination for AFTER triggers]

// Line 236
* FIXME: Shouldn't we drill down to the INSERT record and check if
* [visibility determination for AFTER triggers]
```

**Description:** Visibility determination doesn't drill down to INSERT records for proper AFTER trigger handling. This could affect:
- Trigger visibility semantics
- Transaction isolation correctness
- Data modification visibility

**Recommendation:**
- **HIGH PRIORITY** - Investigate whether this affects trigger correctness
- Review PostgreSQL's visibility semantics for AFTER triggers
- May require significant visibility code refactoring

---

### 3. Predicate Lock Handling FIXME - Multiple Files
**Files:**
- `orvos_handler.c:151, 2017, 3615`
- `orvos_handler.c:3615`

**Status:** POTENTIAL BUG
**Impact:** MEDIUM - Serialization conflict detection
**Severity:** MEDIUM

```c
// Multiple locations
FIXME: heapam acquires the predicate lock first, and then calls
CheckForSerializableConflictOut(). We do it in the opposite order,
because CheckForSerializableConflictOut() call as done in
ovbt_get_last_tid() already. Does it matter? I'm not sure.
```

**Description:** Predicate locking order differs from heap implementation. While the code works, the order may affect:
- Serialization conflict detection timing
- Phantom read detection under edge conditions

**Recommendation:**
- **INVESTIGATE** - Compare with heapam's predicate lock behavior
- Document the difference if intentional
- Add test cases for serialization conflicts

---

## HIGH Priority Issues (Performance / Important Correctness)

### 4. Heap Lock Shared Lock TODO - orvos_btree.c:194
**File:** `/src/backend/access/orvos/orvos_btree.c:194`
**Status:** NOT IMPLEMENTED
**Impact:** MEDIUM - Lock contention
**Severity:** MEDIUM

```c
LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE); /* TODO: shared */
```

**Description:** B-tree page locking always uses EXCLUSIVE locks where SHARED locks might suffice. This reduces parallelism unnecessarily.

**Recommendation:**
- **MEDIUM PRIORITY** - Improves concurrency but not critical
- Review B-tree operation requirements
- May reduce lock contention for read-heavy workloads

---

### 5. Update Chain Traversal FIXME - orvos_handler.c:1120
**File:** `/src/backend/access/orvos/orvos_handler.c:1120`
**Status:** INCOMPLETE
**Impact:** MEDIUM - Performance
**Severity:** MEDIUM

```c
* FIXME: if we have to follow the update chain, we should look at the
* [missing TID optimization]
```

**Description:** When following UPDATE chains, could optimize by looking at a value that's not tracked currently.

**Recommendation:**
- **MEDIUM PRIORITY** - Performance optimization
- Would improve UPDATE chain traversal performance
- Requires data structure changes

---

### 6. Visibility Information Not Preserved - orvos_handler.c:1737
**File:** `/src/backend/access/orvos/orvos_handler.c:1737`
**Status:** NOT IMPLEMENTED
**Impact:** MEDIUM - Performance
**Severity:** MEDIUM

```c
* TODO: we didn't keep any visibility information about the tuple in the
* [missing information that could optimize visibility checks]
```

**Description:** Could cache visibility information to avoid repeated visibility determination.

**Recommendation:**
- **MEDIUM PRIORITY** - Performance optimization
- Consider caching tuple visibility info in scan descriptors

---

## MEDIUM Priority Issues (Performance Optimizations / Nice-to-Have)

### 7. WAL Optimization Opportunities
**Files:** Multiple locations
- `orvos_btree.c:938` - Add 'recycle' flags to WAL records
- `orvos_btree.c:941` - Missing attno in WAL logging

**Impact:** LOW - WAL efficiency
**Status:** NOT IMPLEMENTED
**Recommendation:** Can defer to maintenance release

---

### 8. Space/Memory Optimizations (14 comments)
**Files:**
- `orvos_attitem.c:202` - Account for TID codewords in size calculation
- `orvos_attitem.c:1036` - Account for tids and null bitmap accurately
- Multiple null bitmap copying optimizations (orvos_attitem.c)
- `orvos_attpage.c:228` - Handle page splits better
- `orvos_tidpage.c:1309` - Optimize undo pointer handling

**Impact:** LOW-MEDIUM - Memory/storage efficiency
**Status:** NOT IMPLEMENTED
**Recommendation:** Batch for performance optimization phase

---

### 9. Sorting Not Implemented - orvos_handler.c:3012
**File:** `/src/backend/access/orvos/orvos_handler.c:3012`
**Status:** NOT IMPLEMENTED
**Impact:** MEDIUM - Query functionality
**Severity:** MEDIUM

```c
* TODO: sorting not implemented yet. (it would require materializing each
* [rows for sort operation]
```

**Description:** CLUSTER/sort operations require materialization but aren't implemented.

**Recommendation:**
- **MEDIUM PRIORITY** - Needed for full feature support
- Requires row materialization infrastructure
- Consider for next feature release

---

### 10. Tuple Locking Not Implemented - orvos_tidpage.c:826
**File:** `/src/backend/access/orvos/orvos_tidpage.c:826`
**Status:** NOT IMPLEMENTED
**Impact:** MEDIUM - Transaction semantics
**Severity:** MEDIUM

```c
* TODO: tuple-locking not implemented. Pray that there is no competing
* [concurrent modifications of same tuple]
```

**Description:** Fine-grained tuple locking is not implemented. Works for single-user scenarios only.

**Recommendation:**
- **MEDIUM-HIGH PRIORITY** - Required for production multi-user workloads
- Implement row-level locking with FOR UPDATE/SHARE semantics

---

## LOW Priority Issues (Code Quality / Future Improvements)

### 11. B-tree Split Ratio - orvos_btree.c:583
**File:** `/src/backend/access/orvos/orvos_btree.c:583`
**Status:** HARDCODED
**Impact:** LOW - Performance tuning
**Severity:** LOW

```c
/* XXX: currently, we always do 90/10 splits */
```

**Description:** Split ratio is hardcoded to 90/10. Could be configurable.

**Recommendation:**
- **LOW PRIORITY** - Can use default for now
- Consider making configurable if performance tuning needed

---

### 12. Unspecified Issues (Placeholder Comments)
**Files:**
- `orvos_meta.c:8` - "TODO:" (no description)
- `orvos_freepagemap.c:30` - "TODO:" (no description)

**Impact:** LOW - Need clarification
**Status:** UNCLEAR
**Recommendation:** Clarify intent in comments or remove if obsolete

---

### 13. Simple Optimization Comments (19 comments)
**Examples:**
- `orvos_attpage.c:154` - Check last offset as optimization
- `orvos_attpage.c:185` - Remember next block for future scans
- `orvos_btree.c:482` - Inefficient descent logic
- `orvos_btree.c:1077` - Size assumptions

**Impact:** LOW - Code quality
**Status:** NOT IMPLEMENTED
**Recommendation:** Batch these for performance tuning phase

---

### 14. XXX Comments (Code Quality Notes)
**Count:** ~18 XXX comments
**Files:** Spread across all major files
**Impact:** LOW - Code documentation
**Status:** INFORMATIONAL
**Recommendation:** These are mostly "note to maintainer" comments. Can leave in place.

---

## Summary by File

| File | Count | Critical | High | Medium | Low |
|------|-------|----------|------|--------|-----|
| orvos_handler.c | 28 | 0 | 2 | 8 | 18 |
| orvos_attitem.c | 14 | 0 | 0 | 4 | 10 |
| orvos_tidpage.c | 12 | 0 | 1 | 4 | 7 |
| orvos_btree.c | 8 | 0 | 1 | 2 | 5 |
| orvos_undorec.c | 7 | 1 | 0 | 2 | 4 |
| orvos_visibility.c | 6 | 0 | 1 | 1 | 4 |
| orvos_undolog.c | 6 | 0 | 0 | 1 | 5 |
| orvos_attpage.c | 4 | 0 | 0 | 1 | 3 |
| orvos_meta.c | 4 | 0 | 0 | 1 | 3 |
| orvos_overflow.c | 1 | 0 | 0 | 1 | 0 |
| orvos_freepagemap.c | 3 | 0 | 0 | 1 | 2 |
| orvos_simple8b.c | 3 | 0 | 0 | 0 | 3 |
| **TOTAL** | **99** | **1** | **5** | **26** | **67** |

---

## Recommendations for Action

### Phase 1: Critical Issues (Do Now)
1. **Investigate and fix the delta UPDATE materialization bug** (orvos_undorec.c)
   - This is the only CRITICAL issue affecting data integrity
   - Estimated effort: HIGH (requires debugging and testing)

### Phase 2: High Priority (Next Sprint)
2. **Investigate visibility determination for AFTER triggers** (orvos_visibility.c)
3. **Verify predicate lock ordering** doesn't impact correctness (orvos_handler.c)
4. **Implement tuple-level locking** for multi-user scenarios (orvos_tidpage.c)

### Phase 3: Medium Priority (Maintenance)
5. **Optimize B-tree locking** to use SHARED locks where appropriate
6. **Implement sorting/CLUSTER** support
7. **Batch space/memory optimizations**

### Phase 4: Low Priority (Future)
8. Code quality improvements and documentation
9. Performance tuning of hardcoded values
10. Non-critical XXX comments

---

## Notes

- Most comments (67%) are LOW priority code quality notes or minor optimizations
- Only 1 CRITICAL issue (delta UPDATE materialization) that affects data integrity
- 5 HIGH priority issues require investigation or implementation
- No show-stoppers besides the delta UPDATE bug
- Code is generally well-commented with clear intent
