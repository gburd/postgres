# Investigation Plan for Remaining Test Failures

## Issue #1: DELETE Not Working (Count Mismatch)

**Test**: `t_btree_concurrency` in orvos.sql lines 412-431
**Problem**: COUNT(*) returns 6000 instead of expected 4334

### Evidence:
- Each SQL statement auto-commits (no explicit BEGIN/COMMIT)
- DELETE FROM t_btree_concurrency WHERE a % 3 = 0  (should delete 1666 rows)
- Getting COUNT = 6000 = 5000 + 1000 (as if DELETE had no effect)

### Code Review Complete ✅:
The DELETE and visibility logic appears correct:
- `orvosam_delete()` calls `ovbt_tid_delete()`
- `ovbt_tid_delete()` creates DELETE UNDO record
- `ov_SatisfiesMVCC()` returns false for visible deletes (lines 578-581)
- `ov_SatisfiesSelf()` returns false for self-transaction deletes (lines 665-668)

### Hypothesis:
The issue may not be in the DELETE implementation itself, but in:
1. **Sequential scan not calling visibility check** - Maybe scans are bypassing visibility
2. **Index scan returning wrong TIDs** - Index may include deleted TIDs
3. **Recent commits broke DELETE** - Check commit 5d5e8e483c7 "Complete remaining tasks"

### Investigation Steps:
1. Add debug logging to `ovbt_tid_delete()` to verify UNDO records are created
2. Add debug logging to `ov_SatisfiesMVCC()` to verify visibility checks happen
3. Check if recent B-tree optimizations (commit c08e084d958) broke DELETE
4. Run standalone DELETE test:
   ```sql
   CREATE TABLE test_delete(a int) USING orvos;
   INSERT INTO test_delete VALUES (1), (2), (3);
   SELECT COUNT(*) FROM test_delete;  -- Should be 3
   DELETE FROM test_delete WHERE a = 2;
   SELECT COUNT(*) FROM test_delete;  -- Should be 2
   SELECT * FROM test_delete ORDER BY a;  -- Should show 1, 3
   ```

## Issue #2: VACUUM Corrupt Item Array

**Test**: `vacuum t_delta;` at orvos.sql line 222
**Error**: "ERROR: corrupt item array" in orvos_attitem.c:958

### Evidence:
- Error occurs in `fetch_att_array()` at line 958
- Condition: `p - (unsigned char *) src != srcSize`
- Means: Read pointer didn't advance by expected size
- Context: VACUUM materializing delta-updated columns

### Code Review:
Likely related to commit bd874cbc142 "Add inline compressed datum support":
- Line 167: Modifies input `datums` array in place
- Decompresses `VARATT_IS_COMPRESSED` datums
- Size calculation may not account for decompressed size

### Hypothesis:
Size mismatch between write path and read path:
1. **Write path** (ovbt_attr_create_item, lines 140-183):
   - Calculates size based on DECOMPRESSED data (after line 167)
   - Stores decompressed data in item
2. **Read path** (fetch_att_array, lines 851-959):
   - Expects size to match stored data
   - May not handle all varlena header types (1-byte vs 4-byte)

### Root Cause Candidates:
1. `attstorage = 'p'` (PLAIN) handling differs (lines 901-913 vs 908-913)
2. Short varlenas (1-byte header) size calculation incorrect
3. Alignment padding not accounted for in size calc
4. Inline compression changed write format but read path unchanged

### Investigation Steps:
1. Add logging to `ovbt_attr_create_item()` to log actual `itemsz`
2. Add logging to `fetch_att_array()` to log `srcSize` and actual bytes read
3. Create minimal test case:
   ```sql
   CREATE TABLE test_delta(a int, b text, c text, d int) USING orvos;
   INSERT INTO test_delta VALUES (1, 'short', 'text', 10);
   UPDATE test_delta SET b = 'new' WHERE a = 1;  -- Delta update
   VACUUM test_delta;  -- Should trigger materialization
   ```
4. Test with different data types and attstorage settings

## Priority Order:
1. **DELETE issue** (Higher priority) - Affects all DELETE operations
2. **VACUUM corrupt array** (High priority) - Affects delta updates

## Next Actions:
1. Add debug logging to isolate DELETE issue
2. Create minimal reproduction test cases
3. Review recent commits for unintended breakage
4. Consider reverting commit bd874cbc142 if inline compression is root cause

## Commits Since Last Working State:
```
483f8ff070b Fix critical use-after-free bug in tuple locking [JUST COMMITTED]
5d5e8e483c7 Complete remaining tasks: B-tree WAL recycle flags and MVCC improvements
22f4a824a97 Fix metabuf locking bugs in ovundo_insert_reserve
0a471ce63aa Fix build errors: add pg_lfind.h include and deferred_updates parameter
6b833dc0d0a Fix visibility checking for INSERT records in ov_SatisfiesUpdate
0a61f31b41b Implement CLUSTER sorting and deduplicate Simple8b encoding
115d4d7bf9f Fix VACUUM statistics parameters in Orvos
bd874cbc142 Add inline compressed datum support in attribute items [SUSPECT for Issue #2]
2dc11148069 Implement hash table for UNDO record caching
dc2068c4af9 Add WAL logging to Free Space Map operations
c08e084d958 Performance optimizations for Orvos attribute and TID page operations [SUSPECT for Issue #1]
6fe9db0a758 Use GlobalVisState instead of TransactionId in VACUUM
```

Commits marked [SUSPECT] may have introduced bugs.
