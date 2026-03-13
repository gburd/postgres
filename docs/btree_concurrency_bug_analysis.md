# B-tree Concurrency Bug Analysis

**Date**: 2026-03-04
**Location**: `src/backend/access/orvos/orvos_btree.c:399`
**Severity**: **CRITICAL** - Causes deadlock under concurrent load
**Status**: Partial fix applied, but insufficient

## The Bug

### Symptom
**Deadlock with self** - A backend tries to lock a buffer it already holds, causing a deadlock.

**Observed**: Running `make installcheck-parallel` repeatedly triggers this bug.

### Root Cause

**Stale metacache causing self-deadlock during B-tree splits.**

---

## Detailed Analysis

### The Scenario

1. **Thread A** performs a B-tree operation that splits a leaf page:
   - Holds exclusive lock on `leftbuf` (block `leftblkno`) at level 0
   - Calls `ovbt_insert_downlinks(rel, attno, leftlokey, leftblkno, level=0, downlinks)`
   - Needs to find parent to insert downlink

2. **Meanwhile, Thread B** (or earlier operation) has modified the tree:
   - Tree has grown, new root created at level 1
   - Old root (was at level 0) becomes a child
   - Metapage updated with new root block

3. **Thread A's metacache is stale**:
   - `metacache->cache_attrs[attno].root` still points to `leftblkno`
   - This is wrong - `leftblkno` is no longer the root!

4. **Thread A calls** `ovbt_descend(rel, attno, leftlokey, level, false)` (line 421):
   ```c
   parentbuf = ovbt_descend(rel, attno, leftlokey, level, false);
   ```

5. **Inside `ovbt_descend()`**:
   - Calls `ovmeta_get_root_for_attribute(rel, attno, readonly=false)`
   - Reads from stale metacache (line 411):
     ```c
     rootblk = metacache->cache_attrs[attno].root;
     ```
   - **Returns `leftblkno`** (wrong! it's not the root)
   - Tries to lock `leftblkno` at line 105:
     ```c
     buf = ReadBuffer(rel, next);
     LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
     ```

6. **DEADLOCK**: Thread A already holds exclusive lock on `leftblkno`!

---

## Current "Fix" (Lines 410-419)

```c
{
    OVMetaCacheData *metacache;

    metacache = ovmeta_get_cache(rel);
    if (attno < metacache->cache_nattributes &&
        metacache->cache_attrs[attno].root == leftblkno)
    {
        metacache->cache_attrs[attno].root = InvalidBlockNumber;
    }
}
```

**What it does**:
- Checks if the child block (`leftblkno`) is incorrectly cached as the root
- If so, invalidates the cache (sets to `InvalidBlockNumber`)
- Forces `ovmeta_get_root_for_attribute()` to re-read from metapage

**Why this helps**:
- Handles the specific case where a leaf page that was split is still cached as root
- Forces cache refresh before calling `ovbt_descend()`

**Why it's insufficient** (per original comment):
> "I'm not sure this fixes the whole general problem though, so this needs some more thought..."

---

## Why The Fix Is Incomplete

### Problem 1: Race Condition Still Exists

The fix only checks if `leftblkno == root`. But the cache could be stale in other ways:

**Scenario A**: Parent block is stale
- Thread A descends from level 2 to level 1
- Holds lock on parent at level 1
- Cache has stale pointer to this parent as a grandparent
- Tries to descend again → deadlock

**Scenario B**: Rightmost block is stale
- `metacache->cache_attrs[attno].rightmost` could also be stale
- If Thread A holds lock on rightmost page and cache is wrong
- Fast path in `ovbt_descend()` (lines 72-79) could cause same deadlock

**Scenario C**: Multi-level staleness
- Cache could be correct about root but stale about internal nodes
- Complex multi-level splits could still trigger deadlock

### Problem 2: Cache Invalidation is Per-Relation, Not Global

The cache is stored in `rel->rd_amcache` (per-relation descriptor).

**Issue**: Cache invalidation only affects the current backend's relation descriptor.

**Concurrent backends**:
- Backend A updates metapage with new root
- Backend B still has old root cached
- Backend B could use stale cache for extended period

**Shared cache problem**:
- Relation cache is backend-local
- No mechanism to invalidate caches across backends
- Concurrent operations can all have different stale views

### Problem 3: Incomplete Cache Invalidation Strategy

Current invalidation points:
1. Line 417: Invalidate if child is cached as root (ovbt_insert_downlinks)
2. Line 125: Invalidate on unexpected page (ovbt_descend)

**Missing**:
- No invalidation after root split
- No invalidation after internal node reorg
- No invalidation after page deletion
- No proactive cache refresh mechanism

---

## The Complete Problem

### Fundamental Issue: Stale Cache + Held Locks = Deadlock

The pattern is:
1. Backend holds exclusive lock on buffer B
2. Cache incorrectly says B is at position P in tree
3. Backend tries to navigate tree using cache
4. Ends up trying to lock B again
5. **DEADLOCK**

### Why This Is Hard to Fix

**Option A**: Never cache anything
- **Pro**: No stale cache issues
- **Con**: Performance hit on every tree descent (must read metapage every time)

**Option B**: Invalidate cache more aggressively
- **Pro**: Reduces staleness
- **Con**: Still has race windows, cache becomes less useful

**Option C**: Never descend to a page we already hold
- **Pro**: Prevents self-deadlock
- **Con**: Requires tracking all held locks, complex implementation

**Option D**: Use shared locks for descent, upgrade when needed
- **Pro**: Reduces deadlock window
- **Con**: Upgrade protocol is complex, could still deadlock

**Option E**: Change algorithm to not need parent lookup
- **Pro**: Avoids the whole problem
- **Con**: Major redesign of B-tree operations

---

## Recommended Fix

### Short-term (Defensive): Enhanced Cache Invalidation

**Goal**: Make the current fix more comprehensive

**Implementation**:

```c
/*
 * Defensive cache invalidation before descending the tree.
 *
 * If we're holding any buffer lock and the cache might point to that
 * buffer anywhere in the tree structure, invalidate the cache to force
 * a fresh read from the metapage.
 *
 * This prevents self-deadlock where we try to lock a buffer we already hold.
 */
static void
ovbt_invalidate_cache_if_needed(Relation rel, AttrNumber attno,
                                 BlockNumber held_block)
{
    OVMetaCacheData *metacache;

    if (held_block == InvalidBlockNumber)
        return;  /* No buffer held, no risk */

    metacache = ovmeta_get_cache(rel);
    if (attno >= metacache->cache_nattributes)
        return;

    /*
     * Invalidate if ANY cached value matches the block we're holding:
     * - Root block
     * - Rightmost block
     *
     * We don't track parent/internal nodes in cache, so those should be safe.
     * But to be absolutely safe, we could just invalidate the entire cache.
     */
    if (metacache->cache_attrs[attno].root == held_block ||
        metacache->cache_attrs[attno].rightmost == held_block)
    {
        /* Invalidate this attribute's cache */
        metacache->cache_attrs[attno].root = InvalidBlockNumber;
        metacache->cache_attrs[attno].rightmost = InvalidBlockNumber;
        metacache->cache_attrs[attno].rightmost_lokey = InvalidOVTid;
    }
}
```

**Usage**: Call before any `ovbt_descend()` when holding a buffer lock:

```c
// In ovbt_insert_downlinks() (around line 410)
ovbt_invalidate_cache_if_needed(rel, attno, leftblkno);
parentbuf = ovbt_descend(rel, attno, leftlokey, level, false);

// In ovbt_merge_pages() (around line 684)
ovbt_invalidate_cache_if_needed(rel, attno, BufferGetBlockNumber(leftbuf));
ovbt_invalidate_cache_if_needed(rel, attno, BufferGetBlockNumber(rightbuf));
parentbuf = ovbt_descend(rel, attno, rightopaque->ov_lokey, ...);
```

**Benefits**:
- Handles root cache
- Handles rightmost cache
- More explicit about what we're checking
- Easier to extend if we add more cache fields

**Limitations**:
- Still has race window (cache could become stale after check)
- Doesn't prevent other backends from using stale cache
- Performance hit from more aggressive invalidation

---

### Medium-term (Robust): Deadlock Detection in Descend

**Goal**: Detect and prevent self-deadlock during tree descent

**Implementation**:

```c
/*
 * ovbt_descend - Navigate down the B-tree to find target page at level.
 *
 * If 'held_buf' is not InvalidBuffer, we are holding a lock on that buffer
 * and must not try to lock it again (would cause self-deadlock).
 */
Buffer
ovbt_descend_safe(Relation rel, AttrNumber attno, ovtid key, int level,
                  bool readonly, Buffer held_buf)
{
    BlockNumber held_block = InvalidBlockNumber;

    if (BufferIsValid(held_buf))
        held_block = BufferGetBlockNumber(held_buf);

    // ... existing logic, but add check before locking:

    for (;;)
    {
        buf = ReadBuffer(rel, next);

        /* CRITICAL: Check for self-deadlock before locking */
        if (next == held_block)
        {
            ReleaseBuffer(buf);

            /* Cache must be stale, invalidate and retry from root */
            elog(WARNING, "avoided self-deadlock in B-tree descent: "
                         "tried to lock block %u which is already held",
                         next);
            ovmeta_invalidate_cache(rel);
            next = ovmeta_get_root_for_attribute(rel, attno, readonly);
            continue;
        }

        LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
        // ... rest of logic
    }
}
```

**Usage**: All callers pass the buffer they're holding:

```c
// In ovbt_insert_downlinks()
parentbuf = ovbt_descend_safe(rel, attno, leftlokey, level, false, leftbuf);

// In ovbt_merge_pages()
parentbuf = ovbt_descend_safe(rel, attno, rightopaque->ov_lokey,
                              origleftopaque->ov_level + 1, false,
                              leftbuf);  // or rightbuf, depending
```

**Benefits**:
- **Guarantees no self-deadlock** (detects before it happens)
- Handles all cache staleness scenarios
- Automatic recovery (invalidate + retry)
- Clear warning message for debugging

**Limitations**:
- Requires API change (all callers must pass held buffer)
- Retry logic could cause performance impact under high concurrency
- Doesn't prevent cross-backend deadlocks (A waits for B, B waits for A)

---

### Long-term (Redesign): Avoid Parent Lookup Altogether

**Goal**: Change B-tree algorithm to not need parent lookup after split

**Current approach**:
1. Split leaf page (hold locks)
2. Release locks
3. Re-find parent to insert downlink

**Problem**: Step 3 requires descending the tree while holding locks from step 1

**Alternative approach**: **Stack-based descent**

```c
typedef struct OVBtreeDescent
{
    int         depth;
    Buffer      path[OVBT_MAX_LEVEL];  /* Buffers along descent path */
    OffsetNumber offsets[OVBT_MAX_LEVEL];  /* Offsets at each level */
} OVBtreeDescent;
```

**Algorithm**:
1. Descend tree, recording path in stack
2. Perform operation at leaf (split, etc.)
3. Walk back up the stack (already have parent buffer!)
4. Insert downlink in parent
5. If parent splits, walk up one more level (from stack)

**Benefits**:
- No need to re-find parent
- No possibility of self-deadlock (we know exactly where we came from)
- More efficient (one descent instead of two)
- Similar to how heap's btree index works

**Drawbacks**:
- Major refactoring of B-tree code
- Must hold locks on entire descent path (could increase contention)
- Complicates concurrent operations
- Estimated effort: 80-120 hours

---

## Testing Strategy

### Reproduce the Bug

1. **Stress test** with parallel operations:
   ```bash
   # Terminal 1-10: Concurrent inserts
   for i in {1..10}; do
       psql -d test -c "INSERT INTO orvos_test SELECT generate_series(1, 10000);" &
   done

   # Should trigger deadlock within minutes
   ```

2. **Targeted test** - Force root split:
   ```sql
   CREATE TABLE t_btree_bug(id int, data text) USING orvos;

   -- Insert enough to cause multiple root splits
   INSERT INTO t_btree_bug SELECT i, repeat('x', 100)
   FROM generate_series(1, 100000) i;
   ```

3. **Regression test** with `make installcheck-parallel`:
   ```bash
   cd src/test/regress
   make installcheck-parallel -j8
   # Run 10-20 times to catch race
   ```

### Verify the Fix

After implementing fix:

1. **No self-deadlocks**:
   - Monitor for "avoided self-deadlock" warnings
   - Should see warnings but no actual deadlocks

2. **Cache invalidation rate**:
   - Add counters for cache hits/misses/invalidations
   - Ensure invalidations are reasonable (<10% of operations)

3. **Performance**:
   - Benchmark before/after
   - Should see minimal regression (<5%)

4. **Correctness**:
   - All existing regression tests pass
   - Concurrent stress tests pass

---

## Recommendation

**Immediate action (next 8-16 hours)**:
1. Implement **Short-term fix** (enhanced cache invalidation)
2. Add **Medium-term fix** (deadlock detection in descend)
3. Add regression tests
4. Run stress tests to verify

**This solves the immediate production-blocking issue.**

**Future work** (when time permits):
- Consider **Long-term redesign** (stack-based descent)
- Improves performance and robustness
- Estimated: 80-120 hours

---

## Priority

**CRITICAL - Blocks production use**

This bug causes deadlocks under concurrent load and was observed in testing. It must be fixed before Orvos can be used in production.

**Estimated effort**:
- Short-term fix: 4-6 hours
- Medium-term fix: 4-8 hours
- Testing: 2-4 hours
- **Total**: 10-18 hours

