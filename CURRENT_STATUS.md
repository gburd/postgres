# Orvos Project - Current Status

**Date**: 2026-03-13 (continued)
**Branch**: orvos
**Commits ahead of origin**: 12 commits
**Build status**: Compiles cleanly

## Recent Work Completed

### 1. Fixed Critical Use-After-Free Bug ✅
**Commit**: 483f8ff070b

Fixed critical use-after-free bug in `ovbt_tid_update_lock_old()` function in `orvos_tidpage.c`.

**The Bug**:
- Function was accessing `lock_undo_op->reservation.undorecptr` AFTER `ovbt_tid_replace_item()` freed the structure
- Caused infinite retry loop and massive WAL generation (~4GB for single UPDATE, 540MB every 15 seconds)
- Resulted in regression test timeouts

**The Fix**:
- Save `undorecptr` to local variable BEFORE calling `ovbt_tid_replace_item()`
- Use saved value instead of accessing freed pointer
- Lines 846, 859, 866, 872 in orvos_tidpage.c

## Remaining Issues (2)

### Issue 1: VACUUM Corrupt Item Array ❌
**Test**: `orvos.sql` line 222: `vacuum t_delta;`
**Error**: `ERROR: corrupt item array`
**Location**: `orvos_attitem.c:958` in `fetch_att_array()`

**Root Cause Analysis**:
The error occurs when `p - (unsigned char *) src != srcSize`, meaning the read pointer didn't advance by exactly the expected size when reading attribute data from an item.

**Likely Cause**:
Related to commit `bd874cbc142 "Add inline compressed datum support"` which:
- Decompresses `VARATT_IS_COMPRESSED` datums before storage
- Modifies input `datums` array in place (line 167)
- Size calculation mismatch between write and read paths

**Context**:
- Occurs when VACUUM materializes delta-updated columns
- Delta UPDATEs skip unchanged columns and carry them forward from predecessor
- VACUUM must materialize these carried-forward columns before vacuuming predecessor

**Investigation Needed**:
- Check if short varlenas (1-byte header) vs standard varlenas (4-byte header) cause size mismatch
- Verify `attstorage` handling in `fetch_att_array` lines 901-913
- Test with tables that have `attstorage = 'p'` (PLAIN) vs other storage types

### Issue 2: Btree Concurrency Count Mismatch ❌
**Test**: `orvos.sql` lines 417-428
**Expected**: COUNT(*) = 4334
**Actual**: COUNT(*) = 6000
**Root Cause**: DELETE operations not removing rows, or deleted rows still visible

**Test Sequence**:
1. INSERT 5000 rows (values 1-5000)
2. DELETE WHERE a % 3 = 0 (should delete 1666 rows)
3. INSERT 1000 rows (values 5001-6000)
4. SELECT COUNT(*) should return 4334 (5000 - 1666 + 1000)
5. Actually returns 6000 (all rows still counted)

**Analysis**:
- Count is exactly 6000 = 5000 + 1000, suggesting DELETE had NO effect
- Deleted rows are either:
  - Not being marked as dead in TID tree
  - Still visible to queries (visibility check issue)
  - Not being cleaned up by VACUUM

**Related Code**:
- `orvosam_delete()` in orvos_handler.c
- `ovbt_tid_mark_dead()` in orvos_tidpage.c
- `ov_SatisfiesUpdate()` and visibility checking in orvos_visibility.c

## Commits Ready to Push

```
483f8ff070b Fix critical use-after-free bug in tuple locking
5d5e8e483c7 Complete remaining tasks: B-tree WAL recycle flags and MVCC improvements
22f4a824a97 Fix metabuf locking bugs in ovundo_insert_reserve
0a471ce63aa Fix build errors: add pg_lfind.h include and deferred_updates parameter
6b833dc0d0a Fix visibility checking for INSERT records in ov_SatisfiesUpdate
0a61f31b41b Implement CLUSTER sorting and deduplicate Simple8b encoding
115d4d7bf9f Fix VACUUM statistics parameters in Orvos
bd874cbc142 Add inline compressed datum support in attribute items
2dc11148069 Implement hash table for UNDO record caching
dc2068c4af9 Add WAL logging to Free Space Map operations
c08e084d958 Performance optimizations for Orvos attribute and TID page operations
6fe9db0a758 Use GlobalVisState instead of TransactionId in VACUUM
```

## Build Quality

- **Compilation**: ✅ 0 errors
- **C90 Compliance**: ⚠️ Some implicit conversion warnings (non-critical)
- **Code Changes**: 12 commits with significant MVCC, performance, and bug fixes
- **Regression Tests**: ❌ 2 failures (corrupt item array, count mismatch)

## Next Steps

1. **Fix VACUUM corrupt item array** (High Priority)
   - Add debug logging to identify exact size mismatch
   - Check inline compressed datum handling in size calculations
   - Test with different `attstorage` settings

2. **Fix DELETE visibility** (High Priority)
   - Verify `ovbt_tid_mark_dead()` is being called
   - Check visibility rules in `ov_SatisfiesUpdate()`
   - Add debug logging to track deleted tuple visibility

3. **Push commits** when network is available:
   ```bash
   git push origin orvos
   ```

4. **Run full regression test suite** after fixes:
   ```bash
   cd src/test/regress
   make check TESTS=orvos
   ```

5. **Squash commits** after tests pass (as per user request)

## Critical Use-After-Free Fix Details

The fix resolves the primary blocker that was causing massive WAL generation. With this fix:
- Single UPDATE should take ~376ms (was hanging 11+ minutes)
- WAL generation should be normal (was 4GB for single UPDATE)
- Regression tests should run without timeout

However, the two remaining issues prevent full test suite from passing.
