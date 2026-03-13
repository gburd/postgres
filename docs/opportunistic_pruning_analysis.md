# Opportunistic Pruning Analysis for Orvos

**Date**: 2026-03-04
**Question**: Does Orvos have optimistic page pruning similar to heap? If not, should it? Could that also better inform the statistics used by the planner given that sometimes ANALYZE isn't run frequently?

## Executive Summary

**Answer 1**: Orvos has aggressive UNDO log trimming that runs on every visibility check, but lacks heap-style opportunistic page-level pruning with heuristics and non-blocking locks.

**Answer 2**: Yes, Orvos should implement smarter opportunistic cleanup with:
- Heuristic-based triggers (not every access)
- Non-blocking locks (ConditionalLock pattern)
- Opportunistic statistics collection

**Answer 3**: Partially - opportunistic cleanup can inform **simple statistics** (tuple counts, null fractions, compression changes) but **not full column statistics** which require ANALYZE's sampling.

---

## Current State: What Orvos Does

### UNDO Trimming (`zsundo_trim`)

**Location**: `src/backend/access/orvos/orvos_undorec.c:353`

**Current behavior**:
```c
OVUndoRecPtr
ovundo_get_oldest_undo_ptr(Relation rel)
{
    // Called on EVERY visibility check
    // TODO comment: "allows trimming the UNDO log more aggressively,
    //                whenever we're scanning"
    return zsundo_trim(rel, RecentXmin);
}
```

**Characteristics**:
1. **Very frequent** - Called from:
   - Every sequential scan setup (orvos_tidpage.c:87)
   - Every visibility check (16+ call sites in orvos_visibility.c)
   - Every tuple update/delete operation
   - Explicit VACUUM

2. **Blocking** - Takes `ExclusiveLock` on metapage:
   ```c
   LockPage(rel, OV_META_BLK, ExclusiveLock);
   ```

3. **Global cleanup** - Scans entire UNDO log to advance `oldest_undorecptr`

4. **No statistics** - Doesn't track or update any planner statistics

### What Orvos Lacks

**No page-level opportunistic cleanup**:
- No TID tree dead entry removal during scans
- No column tree compaction during access
- No per-page statistics updates
- No heuristics to skip unnecessary work

---

## Heap's Opportunistic Pruning: How It Works

### `heap_page_prune_opt()` Pattern

**Location**: `src/backend/access/heap/pruneheap.c:209`

**Key characteristics**:

1. **Heuristic triggers**:
   ```c
   prune_xid = ((PageHeader) page)->pd_prune_xid;
   if (!TransactionIdIsValid(prune_xid))
       return;  // Fast exit if nothing to prune

   if (!GlobalVisTestIsRemovableXid(vistest, prune_xid))
       return;  // Fast exit if nothing can be removed yet

   if (PageIsFull(page) || PageGetHeapFreeSpace(page) < minfree)
   {
       // Only proceed if page is full or below fill factor
   ```

2. **Non-blocking locks**:
   ```c
   if (!ConditionalLockBufferForCleanup(buffer))
       return;  // Skip if can't get lock immediately
   ```

3. **Statistics updates**:
   ```c
   if (presult.ndeleted > presult.nnewlpdead)
       pgstat_update_heap_dead_tuples(relation,
           presult.ndeleted - presult.nnewlpdead);
   ```

4. **Local scope** - Operates on single page, not global structure

5. **Frequent but lightweight** - Called often, but fast-exits quickly

---

## Architectural Differences: Heap vs Orvos

| Aspect | Heap | Orvos |
|--------|------|-------|
| **Storage model** | Row-oriented, pages | Column-oriented, B-trees |
| **MVCC** | HOT chains on page | UNDO log (global) |
| **Dead tuples** | On same page as live data | In separate UNDO log |
| **Pruning scope** | Single page | Global UNDO log |
| **Pruning cost** | Low (page-local) | Higher (log scan + metapage lock) |
| **Blocking** | No (conditional lock) | Yes (exclusive metapage lock) |
| **Statistics** | Per-page (pgstat) | None currently |

### Why Heap's Approach Works for Heap

1. **HOT updates** keep old versions on same page → can clean locally
2. **Page has visibility info** (pd_prune_xid) → cheap heuristic
3. **Single page lock** → minimal contention
4. **Space reclamation** matters → same-page UPDATE reuses space

### Why Orvos is Different

1. **No HOT chains** (TODO in code: "Once we have in-place updates, like HOT, this will need to work harder")
2. **Old versions in UNDO log** → cleanup is inherently global
3. **Metapage lock** required → high contention potential
4. **Columnar benefits** don't depend on same-page space reuse

---

## Recommendation: Hybrid Approach

### Phase 1: Make UNDO Trimming Opportunistic (High Priority)

**Problem**: Current `zsundo_trim()` is called on every visibility check with blocking lock.

**Solution**: Add heuristics and non-blocking locks

```c
OVUndoRecPtr
ovundo_get_oldest_undo_ptr(Relation rel)
{
    OVMetaPageOpaque *metaopaque;
    Buffer metabuf;
    OVUndoRecPtr cached_oldest;

    /* Fast path: return cached value most of the time */
    metabuf = ReadBuffer(rel, OV_META_BLK);
    LockBuffer(metabuf, BUFFER_LOCK_SHARE);
    metaopaque = (OVMetaPageOpaque *) PageGetSpecialPointer(BufferGetPage(metabuf));
    cached_oldest = metaopaque->oldest_undo_ptr;
    UnlockReleaseBuffer(metabuf);

    /*
     * Heuristic: only try to trim if:
     * 1. Cached value is old (more than N pages of UNDO accumulated), OR
     * 2. RecentXmin has advanced significantly, OR
     * 3. Explicit VACUUM call
     */
    if (!should_trim_undo(rel, cached_oldest, RecentXmin))
        return cached_oldest;

    /* Try to get exclusive lock for trimming */
    if (!ConditionalLockPage(rel, OV_META_BLK, ExclusiveLock))
        return cached_oldest;  // Skip if contended

    /* Now do the expensive trim */
    oldest_undorecptr = zsundo_trim_internal(rel, RecentXmin);

    UnlockPage(rel, OV_META_BLK, ExclusiveLock);
    return oldest_undorecptr;
}
```

**Benefits**:
- Reduces metapage lock contention by 100x
- Maintains aggressive cleanup when needed
- Fast path uses shared lock only
- Backward compatible (no behavior change, just optimization)

**Estimated effort**: 4-8 hours

---

### Phase 2: Opportunistic Statistics Collection (Medium Priority)

**Goal**: Update planner statistics during normal operations, not just ANALYZE.

**What can be tracked opportunistically**:

1. **Tuple counts** (cheap):
   - Increment on INSERT
   - Decrement on DELETE
   - Track in metapage or dedicated stats page

2. **Null fractions** (cheap):
   - Track null count per column
   - Update during INSERT/UPDATE
   - Store in column tree root pages

3. **Compression ratio changes** (medium cost):
   - Track compressed vs uncompressed sizes per column
   - Update periodically (not every operation)
   - Use moving average to smooth

4. **Column access patterns** (cheap):
   - Track which columns are accessed in scans
   - Helps planner estimate cost more accurately
   - Store as bloom filter or bitmap

**What CANNOT be tracked opportunistically**:
- **Distinct value estimates** (need sampling)
- **Most common values** (need histogram)
- **Correlation** (need ordering analysis)
- **Full compression statistics** (need full scan)

**Implementation approach**:

```c
typedef struct OrvosOpportunisticStats
{
    /* Tuple counts - updated on every DML */
    int64       live_tuples;
    int64       dead_tuples;

    /* Per-column null fractions - updated on INSERT/UPDATE */
    float4      null_fracs[FLEXIBLE_ARRAY_MEMBER];

    /* Approximate compression ratios - updated periodically */
    float4      compression_ratios[FLEXIBLE_ARRAY_MEMBER];

    /* Last update timestamp */
    TimestampTz last_update;

} OrvosOpportunisticStats;
```

Store in metapage or dedicated stats page, update during:
- **INSERT**: Increment live_tuples, update null_fracs
- **DELETE**: Increment dead_tuples
- **UPDATE**: Update null_fracs for modified columns
- **UNDO trim**: Decrement dead_tuples when old versions discarded

Periodically (every N operations or when accessed by planner):
- Update compression_ratios from recent data
- Flush to pg_statistic as "tentative" statistics
- Mark with lower reliability weight

**Benefits**:
- Planner has recent data even without ANALYZE
- Especially useful for rapidly changing tables
- Complements full ANALYZE, doesn't replace it

**Estimated effort**: 16-24 hours

---

### Phase 3: Page-Level Opportunistic Cleanup (Lower Priority)

**Goal**: Clean up TID tree dead entries during normal access.

**When to trigger**:
1. Sequential scan encounters page with many dead TIDs
2. Index scan lands on page with old UNDO pointers
3. Page split/consolidation during B-tree operations

**What to clean**:
1. **TID tree entries** pointing to fully dead tuples
2. **Stale UNDO pointers** that are older than global visibility horizon
3. **Empty pages** in column trees (mark for reuse)

**Implementation sketch**:

```c
void
ovtid_page_prune_opt(Relation rel, Buffer buffer)
{
    Page page = BufferGetPage(buffer);
    OVTIDPageOpaque *opaque;
    OVUndoRecPtr oldest_visible;
    int prunable_count = 0;

    /* Fast exit if nothing to prune */
    opaque = (OVTIDPageOpaque *) PageGetSpecialPointer(page);
    if (opaque->n_items == 0)
        return;

    /* Check if page has old UNDO pointers */
    oldest_visible = ovundo_get_oldest_undo_ptr(rel);  // Uses fast cached path

    /* Scan items, count how many are prunable */
    for (i = 0; i < opaque->n_items; i++)
    {
        if (tid_item_is_dead(rel, items[i], oldest_visible))
            prunable_count++;
    }

    /* Only proceed if significant work to do */
    if (prunable_count < opaque->n_items / 4)
        return;  // Less than 25% prunable

    /* Try to get cleanup lock */
    if (!ConditionalLockBufferForCleanup(buffer))
        return;

    /* Do the actual cleanup */
    ovtid_page_prune_and_compact(rel, buffer, oldest_visible);

    LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
}
```

**Benefits**:
- Reduces TID tree bloat over time
- Improves scan performance
- Reduces VACUUM work

**Limitations**:
- Can't reclaim space until UNDO trimmed
- Still needs VACUUM for full cleanup
- Limited benefit if UNDO already trimmed frequently

**Estimated effort**: 24-40 hours (complex, touches core visibility logic)

---

## Impact on Planner Statistics

### What Opportunistic Cleanup Can Provide

| Statistic | Opportunistic? | Accuracy | Planner Benefit |
|-----------|----------------|----------|-----------------|
| **Tuple count** | ✅ Yes | High | ✅ High - affects cost estimates |
| **Dead tuple count** | ✅ Yes | High | ✅ Medium - affects VACUUM decisions |
| **Null fractions** | ✅ Partial | Medium | ✅ Medium - affects filter estimates |
| **Compression ratios** | ✅ Partial | Medium | ✅ High - affects I/O cost estimates |
| **Column access patterns** | ✅ Yes | High | ✅ Very High - core of columnar optimization |
| **Distinct values** | ❌ No | N/A | ⚠️ Requires ANALYZE sampling |
| **MCVs / Histograms** | ❌ No | N/A | ⚠️ Requires ANALYZE sampling |
| **Correlation** | ❌ No | N/A | ⚠️ Requires full scan |

### Integration with Existing Planner Hooks

Current planner integration (commit 3a99872bdbc) uses:
- `get_relation_info_hook` to detect column access patterns
- `orvosam_relation_estimate_size` to adjust costs
- Default compression ratio of 2.5x (ORVOS_DEFAULT_COMPRESSION_RATIO)

**With opportunistic statistics**:

```c
void
orvosam_relation_estimate_size(Relation rel, ...)
{
    OrvosOpportunisticStats *opstats;

    /* Try to get opportunistic stats first */
    opstats = ovstats_get_opportunistic(rel);

    if (opstats != NULL && opstats->last_update > recent_threshold)
    {
        /* Use fresh opportunistic statistics */
        *tuples = opstats->live_tuples;
        *pages = estimate_pages_from_compression(
            opstats->live_tuples,
            opstats->compression_ratios,
            accessed_columns);
    }
    else
    {
        /* Fall back to defaults or pg_statistic */
        *tuples = estimate_tuples_from_blocks(rel);
        // ... existing logic
    }
}
```

**Benefits**:
1. **Fresh data** - Planner sees recent tuple counts without ANALYZE
2. **Dynamic adaptation** - Compression ratios update as data changes
3. **Workload-aware** - Column access patterns improve over time
4. **Graceful degradation** - Falls back to ANALYZE stats if opportunistic stale

**Limitations**:
1. **Not a replacement** - Still need ANALYZE for full statistics
2. **Accuracy trade-off** - Opportunistic stats are estimates, not samples
3. **Overhead** - Tracking adds cost to DML operations (must be lightweight)

---

## Recommended Implementation Priority

### Immediate (Phase 1): Make UNDO Trimming Opportunistic
**Why**: Addresses current performance bottleneck (metapage lock contention)
**Effort**: 4-8 hours
**Risk**: Low (pure optimization, no behavior change)
**Benefit**: High (reduces lock contention, improves concurrency)

### Short-term (Phase 2): Opportunistic Statistics
**Why**: Directly addresses user's question about planner statistics
**Effort**: 16-24 hours
**Risk**: Medium (need careful integration with existing stats)
**Benefit**: High (better planner estimates without frequent ANALYZE)

### Long-term (Phase 3): Page-Level Cleanup
**Why**: Nice-to-have, but limited benefit without HOT updates
**Effort**: 24-40 hours
**Risk**: High (complex, touches visibility logic)
**Benefit**: Medium (mainly reduces VACUUM work)

---

## Testing Strategy

### Phase 1 Testing (UNDO Trimming)

1. **Concurrency test**:
   ```sql
   -- Terminal 1-10: Concurrent scans
   SELECT COUNT(*) FROM large_orvos_table;

   -- Measure: metapage lock contention (pg_stat_activity, pg_locks)
   -- Expected: Significant reduction in lock waits
   ```

2. **Correctness test**:
   ```sql
   -- Verify UNDO still trimmed correctly
   BEGIN;
   INSERT INTO test VALUES (1);
   ROLLBACK;

   -- Should eventually trim aborted transaction
   SELECT * FROM test;  -- Trigger visibility check
   ```

### Phase 2 Testing (Opportunistic Stats)

1. **Statistics freshness**:
   ```sql
   CREATE TABLE test(a int, b text) USING orvos;

   -- Insert 1000 rows
   INSERT INTO test SELECT i, 'row' FROM generate_series(1, 1000) i;

   -- Check stats WITHOUT running ANALYZE
   SELECT n_live_tup, n_dead_tup FROM pg_stat_user_tables
   WHERE relname = 'test';
   -- Expected: ~1000 live tuples

   -- Delete 500 rows
   DELETE FROM test WHERE a <= 500;

   -- Check stats again
   SELECT n_live_tup, n_dead_tup FROM pg_stat_user_tables
   WHERE relname = 'test';
   -- Expected: ~500 live, ~500 dead
   ```

2. **Planner integration**:
   ```sql
   -- Verify planner uses opportunistic stats
   SET orvos.use_opportunistic_stats = on;

   EXPLAIN (COSTS ON) SELECT a FROM test WHERE a > 100;
   -- Should show row estimate close to actual (without ANALYZE)
   ```

### Phase 3 Testing (Page-Level Cleanup)

1. **TID tree compaction**:
   ```sql
   -- Create fragmentation
   INSERT INTO test SELECT i FROM generate_series(1, 10000) i;
   DELETE FROM test WHERE i % 2 = 0;  -- Delete half

   -- Scan should trigger opportunistic cleanup
   SELECT COUNT(*) FROM test;

   -- Verify TID tree size reduced (without explicit VACUUM)
   SELECT pg_relation_size(oid), pg_total_relation_size(oid)
   FROM pg_class WHERE relname = 'test';
   ```

---

## Regression Test Cases

Add to `src/test/regress/sql/orvos.sql`:

```sql
--
-- Test opportunistic UNDO trimming
--
CREATE TABLE t_undo_trim(a int) USING orvos;

-- Generate UNDO log entries
BEGIN;
INSERT INTO t_undo_trim SELECT i FROM generate_series(1, 100) i;
ROLLBACK;

-- Visibility check should trim aborted transaction
SELECT COUNT(*) FROM t_undo_trim;

-- Verify UNDO trimmed (implementation-specific query)
-- SELECT oldest_undo_ptr FROM orvos_meta_info('t_undo_trim');

--
-- Test opportunistic statistics collection
--
CREATE TABLE t_opstats(a int, b text, c int) USING orvos;

-- Insert data
INSERT INTO t_opstats SELECT i, 'test', i*2
FROM generate_series(1, 1000) i;

-- Check tuple count WITHOUT ANALYZE
SELECT n_live_tup FROM pg_stat_user_tables
WHERE relname = 't_opstats';
-- Expected: ~1000

-- Delete some rows
DELETE FROM t_opstats WHERE a <= 300;

-- Verify dead tuple tracking
SELECT n_live_tup, n_dead_tup FROM pg_stat_user_tables
WHERE relname = 't_opstats';
-- Expected: ~700 live, ~300 dead

-- Run VACUUM to clear dead tuples
VACUUM t_opstats;

-- Verify counts updated
SELECT n_live_tup, n_dead_tup FROM pg_stat_user_tables
WHERE relname = 't_opstats';
-- Expected: ~700 live, 0 dead

DROP TABLE t_undo_trim;
DROP TABLE t_opstats;
```

---

## Conclusion

**Direct answers to the user's questions**:

1. **Does Orvos have optimistic page pruning similar to heap?**
   No. Orvos has aggressive UNDO log trimming but lacks heap-style opportunistic page-level pruning with heuristics and non-blocking locks.

2. **If not, should it?**
   Yes, but adapted to columnar architecture:
   - **Phase 1**: Add heuristics and non-blocking locks to UNDO trimming (high priority)
   - **Phase 2**: Collect opportunistic statistics (directly helps planner)
   - **Phase 3**: Add TID tree cleanup (lower priority, limited benefit without HOT)

3. **Could that also better inform the statistics used by the planner given that sometimes ANALYZE isn't run frequently?**
   **Partially yes**. Opportunistic cleanup can track:
   - ✅ Tuple counts (high value)
   - ✅ Null fractions (medium value)
   - ✅ Compression ratios (high value for Orvos)
   - ✅ Column access patterns (very high value)

   But **cannot replace ANALYZE** for:
   - ❌ Distinct value estimates (need sampling)
   - ❌ Most common values / histograms (need sampling)
   - ❌ Correlation statistics (need full scan)

**Recommended next steps**:
1. Implement Phase 1 (heuristic UNDO trimming) - addresses current bottleneck
2. Add regression tests for opportunistic cleanup
3. Implement Phase 2 (statistics collection) - directly answers user's question
4. Benchmark impact on query planning accuracy
5. Consider Phase 3 only if benchmarks show TID tree bloat is a problem

