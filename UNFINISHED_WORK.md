# Orvos Unfinished Work

**Date**: 2026-03-13
**Status**: Comprehensive audit of TODOs, FIXMEs, and incomplete features

## Summary

This document lists all known unfinished work in the Orvos codebase based on a systematic search for TODO, FIXME, XXX comments, and incomplete implementations.

**Total Items**: 100+ comments across 17 files
**Created Tasks**: #39-#53 (15 priority tasks)

---

## High Priority (Functionality Gaps)

### 1. Missing Core Features

**Task #40**: Implement ReadStream API for ANALYZE support
- **File**: orvos_handler.c (scan_analyze_next_block)
- **Impact**: Cannot run ANALYZE on orvos tables
- **Status**: Returns error "ANALYZE is not yet supported"

**Task #41**: Implement bitmap scan API
- **File**: orvos_handler.c (scan_bitmap_next_tuple)
- **Impact**: Bitmap index scans don't work
- **Status**: Returns error "bitmap scan is not yet supported"

**Task #42**: Implement tuple locking (SELECT FOR UPDATE/SHARE)
- **File**: orvos_tidpage.c:832
- **Impact**: Row-level locking may not work correctly
- **Status**: Comment says "Pray that there is no competing locks"

**Task #49**: Implement SnapshotToast and SnapshotHistoricMVCC
- **Files**: orvos_visibility.c:930, 937
- **Impact**: Operations requiring these snapshots will error
- **Status**: Not implemented

**Task #51**: Implement inline compressed datum support
- **File**: orvos_attitem.c:170
- **Impact**: Inline compressed values will cause errors
- **Status**: Returns error "inline compressed datums not implemented"

### 2. Data Integrity Issues

**Task #43**: Fix TM_FailureData population
- **Files**: orvos_tidpage.c (multiple locations)
- **Impact**: Error handling may not work correctly
- **Status**: Multiple FIXME comments about incorrect population

**Task #48**: Improve visibility checking for INSERT records
- **Files**: orvos_visibility.c:192, 236
- **Impact**: May return incorrect visibility in edge cases
- **Status**: Should drill down to INSERT record to check if aborted

**Task #50**: Fix CLUSTER breaking UPDATE chains
- **File**: orvos_handler.c:2813
- **Impact**: MVCC correctness issues after CLUSTER
- **Status**: FIXME comment says "This breaks UPDATE chains"

### 3. Missing WAL Logging

**Task #52**: Add WAL logging to Free Space Map
- **File**: orvos_freepagemap.c:317
- **Impact**: FSM changes not crash-safe
- **Status**: FIXME: WAL-logging not implemented

---

## Medium Priority (Performance & Efficiency)

**Task #39**: Implement GlobalVisState in VACUUM
- **File**: orvos_handler.c:3811
- **Impact**: VACUUM less efficient than it could be
- **Status**: Uses InvalidTransactionId placeholder

**Task #44**: Implement CLUSTER with sorting
- **File**: orvos_handler.c:3034
- **Impact**: CLUSTER may not properly sort data
- **Status**: "sorting not implemented yet"

**Task #45**: Optimize UNDO record caching
- **Files**: orvos_undorec.c:12, 183
- **Impact**: UNDO operations less efficient
- **Status**: Needs hash table or better structure

**Task #46**: Fix VACUUM statistics parameters
- **Files**: orvos_undorec.c:993, 1010, 1011
- **Impact**: Inaccurate pg_class statistics
- **Status**: Passes placeholder zeros

**Task #47**: Deduplicate Simple8b encoding code
- **File**: orvos_simple8b.c:5
- **Impact**: Code duplication, maintenance burden
- **Status**: Copy-pasted from integerset.c

**Task #53**: Performance optimizations
- **Multiple files**: See task description
- **Impact**: Various performance improvements
- **Status**: 9+ optimization opportunities identified

---

## Low Priority (Code Quality)

### Memory Management Optimizations

Multiple TODO comments about improving memory context usage:
- orvos_handler.c:198: "TODO: in long term try if can avoid creating context"
- orvos_handler.c:1115: Same issue in different function

### Inefficiency Notes

Multiple XXX comments about inefficient code that could be improved:
- orvos_handler.c:786: "XXX this is pretty inefficient if..."
- orvos_handler.c:2736: "XXX this could be refined further"
- orvos_attitem.c:611: "XXX: This always copies the data... that can be wasteful"

### Cross-Check TODOs

Multiple FIXME comments about cross-checking with heap behavior:
- orvos_handler.c:641: "FIXME: cross-check the cmax like heapam does"
- orvos_handler.c:151: "FIXME: heapam acquires the predicate lock first"
- orvos_handler.c:2025: Same issue

### Documentation TODOs

- orvos_meta.c:8: "TODO:" (incomplete documentation)
- orvos_freepagemap.c:30: "TODO:" (incomplete documentation)
- README:167: "TODO: Currently, each attribute is stored in a separate B-tree"
- README:304: "TODO: That doesn't scale very well" (page reuse)

---

## Deferred / Design Questions

### Hybrid Row-Column Storage
- **File**: README:35
- **Status**: "Hybrid row-column storage not yet implemented (all columns stored separately)"
- **Note**: This may be a deliberate design choice

### Parallel Backward Scan
- **File**: orvos_handler.c:1551
- **Status**: Returns error "parallel backward scan not implemented"
- **Note**: May be low priority - backward scans are rare

### UNDO Log Replacement
- **File**: orvos_undolog.c:5
- **Status**: "XXX: This is hopefully replaced with an upstream UNDO facility later"
- **Note**: Depends on PostgreSQL upstream development

---

## Code Quality Issues (Non-Functional)

### Dummy/Placeholder Code

Multiple instances of "dummy scan" comments:
- orvos_tidpage.c:567: "/* FIXME: dummmy scan */"
- orvos_tidpage.c:628: "/* FIXME: dummmy scan */"
- orvos_tidpage.c:813: "/* FIXME: dummmy scan */"

### Questionable Patterns

Comments expressing uncertainty about correctness:
- orvos_handler.c:252: "/* XXX: should we set visi_info here? */"
- orvos_handler.c:706: "/* FIXME: should we release the hwlock here? */"
- orvos_attitem.c:168: "/* TODO: what to do? */"

---

## Statistics

**By File (number of TODO/FIXME/XXX comments)**:
1. orvos_handler.c: 35 comments
2. orvos_attitem.c: 14 comments
3. orvos_tidpage.c: 13 comments
4. orvos_btree.c: 10 comments
5. orvos_undorec.c: 9 comments
6. orvos_visibility.c: 7 comments
7. orvos_attpage.c: 4 comments
8. Other files: <4 each

**By Type**:
- TODO: ~45 comments
- FIXME: ~40 comments
- XXX: ~15 comments

**By Category**:
- Performance optimizations: ~20 items
- Missing implementations: ~15 items
- Data integrity fixes: ~10 items
- Code quality improvements: ~55 items

---

## Completed Work (Previously Unfinished)

✅ **VACUUM Delta UPDATE Bug** - Fixed in commit c65c6e577e0
- Was: "corrupt item array" crash during VACUUM
- Now: VACUUM works correctly with delta UPDATEs

✅ **C90 Compliance** - Fixed in commits 2b19a8fc49e, d1657f81283
- Was: 13 declaration-after-statement warnings
- Now: Zero C90 warnings, fully compliant

✅ **Build System Integration** - Fixed in commit 72c5fb3dc1f
- Was: Conditionally included, often not built
- Now: Unconditionally included in build

---

## Recommendations

### Immediate Action (Before Production)
1. **Task #42**: Implement tuple locking - critical for correctness
2. **Task #43**: Fix TM_FailureData - affects error handling
3. **Task #48**: Improve visibility checking - affects MVCC correctness
4. **Task #52**: Add FSM WAL logging - affects crash safety

### Short Term (1-2 months)
1. **Task #40**: Implement ANALYZE support - needed for query planning
2. **Task #41**: Implement bitmap scans - common query pattern
3. **Task #49**: Implement missing snapshot types - may be needed
4. **Task #50**: Fix CLUSTER UPDATE chains - affects CLUSTER users

### Medium Term (3-6 months)
1. **Task #39**: GlobalVisState in VACUUM - improves efficiency
2. **Task #44**: CLUSTER sorting - completes feature
3. **Task #45**: Optimize UNDO caching - performance improvement
4. **Task #46**: Fix VACUUM stats - better pg_class data
5. **Task #51**: Inline compressed datums - wider data type support

### Long Term (6+ months)
1. **Task #47**: Deduplicate Simple8b code - code quality
2. **Task #53**: Performance optimizations - incremental improvements
3. Consider hybrid row-column storage (README note)
4. Consider upstream UNDO facility integration (if PostgreSQL adds it)

---

## Notes

- All #if 0 blocks were removed during cleanup (none remain)
- No "hack" keywords found beyond one legitimate use case
- Most TODOs are optimization opportunities, not bugs
- Core functionality is solid despite unfinished features

**Last Updated**: 2026-03-13
