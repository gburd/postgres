# UPDATE WAL Efficiency Analysis: Orvos vs Heap

**Date**: 2026-03-04
**Question**: If UNDO log records are not sent to logical replicas, is Orvos more WAL-efficient on UPDATE than HEAP without sacrificing any functionality?

## Short Answer

**No, Orvos is currently LESS WAL-efficient than heap for UPDATEs, especially partial UPDATEs.**

However, this is **fixable** with column-delta optimization that would make Orvos significantly more efficient.

---

## Detailed Analysis

### Current Orvos UPDATE Behavior

**Code location**: `src/backend/access/orvos/orvos_handler.c:935-953`

```c
for (attno = 1; attno <= relation->rd_att->natts; attno++)
{
    Form_pg_attribute attr = TupleDescAttr(relation->rd_att, attno - 1);
    Datum newdatum = d[attno - 1];
    bool newisnull = isnulls[attno - 1];

    // ... handle TOAST ...

    // INSERT ALL COLUMNS, not just changed ones
    ovbt_attr_multi_insert(relation, attno,
                          &newdatum, &newisnull, &newtid, 1);
}
```

**Key problem**: Orvos inserts **ALL columns** for the new tuple version, regardless of which columns actually changed.

---

### Heap UPDATE Behavior

Heap has three UPDATE variants with different WAL overhead:

#### 1. HOT Update (Heap-Only Tuple) - Most Efficient

**When it applies**:
- Updated tuple fits on same page
- No indexed columns changed
- Page has free space

**WAL logged**:
```c
typedef struct xl_heap_update
{
    TransactionId old_xmax;    // 4 bytes
    OffsetNumber old_offnum;   // 2 bytes
    uint8 old_infobits_set;    // 1 byte
    uint8 flags;               // 1 byte
    TransactionId new_xmax;    // 4 bytes
    OffsetNumber new_offnum;   // 2 bytes

    /* NEW TUPLE DATA - only if not FPI */
    // Full tuple with changed columns
}
```

**Total WAL for HOT update** (no FPI):
- Header: ~14 bytes
- New tuple: ~32 bytes (header) + changed data
- **Example**: Update 1 int column in 10-column table → ~50 bytes WAL

**Advantages**:
- No index updates needed
- Small WAL footprint
- Fast

#### 2. Non-HOT Update (requires index update)

**When it applies**:
- Indexed columns changed, OR
- Updated tuple doesn't fit on same page

**WAL logged**:
- Same as HOT update structure
- But flags indicate indexes need updating
- Plus: Index update WAL records

**Total WAL**:
- ~50-100 bytes for tuple update
- ~20-50 bytes per index update

#### 3. In-place Update (rare, for special cases)

**When it applies**:
- Only tuple header changes (xmin/xmax/cmin/cmax)
- No column data changes

**WAL logged**:
- Just the tuple header changes
- ~30 bytes

---

### Orvos UPDATE Overhead Breakdown

For a table with N columns, updating M columns:

#### What Orvos currently logs:

1. **TID tree update** (`WAL_ORVOS_BTREE_REPLACE_LEAF_ITEM`):
   ```c
   typedef struct wal_orvos_btree_leaf_items
   {
       AttrNumber attno;    // 2 bytes (0 = TID tree)
       int16 nitems;        // 2 bytes
       OffsetNumber off;    // 2 bytes
       /* TID item data follows */
   }
   ```
   - Fixed header: 6 bytes
   - TID item: ~32 bytes (TID + visibility info + UNDO pointer)
   - **Total**: ~38 bytes

2. **Column tree updates** for ALL N columns (`WAL_ORVOS_BTREE_ADD_LEAF_ITEMS`):
   - Per column:
     - Header: 6 bytes
     - Column item: 8 bytes (TID) + compressed value
   - **Total per column**: ~14 bytes + compressed value size

3. **UNDO log** (if new page needed):
   - `WAL_ORVOS_UNDO_NEWPAGE`: ~16 bytes

#### Example: 10-column table, update 1 int column

**Heap**:
- HOT update: ~50 bytes (if fits on same page)
- Non-HOT update: ~70 bytes + index updates

**Orvos (current)**:
1. TID tree: 38 bytes
2. 10 column trees: 10 × (14 + ~4 bytes for int) = 180 bytes
3. UNDO (if needed): 16 bytes
4. **Total**: ~234 bytes

**Orvos is 4-5x less efficient for partial UPDATEs.**

---

## Why This Happens

### Design Trade-off

**Orvos's columnar architecture**:
- Each column stored in separate B-tree
- TID is the row identifier across all trees
- UPDATE creates new TID with new column values

**Current implementation**:
- Treats UPDATE like INSERT: writes all columns
- Simpler implementation (reuses INSERT code)
- But inefficient for partial updates

### What Orvos COULD Do (Not Implemented)

**Column-delta optimization**:
```c
// Compare old and new values
for (attno = 1; attno <= relation->rd_att->natts; attno++)
{
    if (!datumIsEqual(oldslot->tts_values[attno-1],
                     newslot->tts_values[attno-1], ...))
    {
        // Only insert changed columns
        ovbt_attr_multi_insert(relation, attno,
                              &newdatum, &newisnull, &newtid, 1);
    }
}
```

**With this optimization**:
- Update 1 int column in 10-column table
- TID tree: 38 bytes
- 1 column tree: 18 bytes
- **Total**: 56 bytes

**Now Orvos would be COMPETITIVE with heap HOT updates.**

---

## Functionality Trade-offs

### Does Orvos sacrifice functionality?

**NO - Orvos provides full MVCC compliance:**

1. **Snapshot isolation**: ✅ Works via UNDO log + visibility checks
2. **Concurrent transactions**: ✅ Multiple transactions can read/write
3. **Transaction rollback**: ✅ UNDO log allows rollback
4. **Crash recovery**: ✅ WAL replay reconstructs state
5. **Point-in-time recovery**: ✅ WAL-based PITR works
6. **Hot Standby**: ✅ Physical replicas can serve queries

**Logical replication**: ❌ Not implemented yet (but could be added)

### What Orvos DOES sacrifice (vs heap)

1. **HOT updates**: ❌ Not implemented yet
   - Orvos always creates new TID for UPDATE
   - No same-page optimization
   - All UPDATEs require index updates (if indexed)

2. **In-place header updates**: ❌ Not applicable
   - Orvos doesn't have tuple headers in the heap sense
   - Visibility info in separate TID tree

3. **Update chains on same page**: ❌ Not applicable
   - Columnar storage doesn't have "pages" with multiple tuple versions

---

## Comparative WAL Efficiency Matrix

| Scenario | Heap WAL | Orvos WAL (current) | Orvos WAL (with column-delta) |
|----------|----------|---------------------|--------------------------------|
| **Update 1 column (10 total, HOT possible)** | ~50 bytes | ~234 bytes | ~56 bytes |
| **Update 1 column (10 total, no HOT)** | ~70 bytes | ~234 bytes | ~56 bytes |
| **Update 5 columns (10 total)** | ~100 bytes | ~234 bytes | ~128 bytes |
| **Update all 10 columns** | ~150 bytes | ~234 bytes | ~234 bytes |
| **Update with TOAST (large value)** | ~100 bytes + FPI | ~250 bytes + FPI | ~250 bytes + FPI |

**Key insights**:

1. **Current Orvos is worse for partial UPDATEs**:
   - 4-5x more WAL for single-column updates
   - Overhead increases with table width

2. **With column-delta optimization, Orvos becomes competitive**:
   - Similar or better WAL usage for partial UPDATEs
   - Especially good for wide tables (OLAP workloads)

3. **Orvos has advantage for OLAP workloads**:
   - WHERE clause on few columns → less I/O
   - Compression reduces storage
   - But UPDATE is less common in OLAP

---

## Why Heap is More Efficient (Currently)

### 1. HOT Updates

**Heap's killer optimization**:
- ~80% of UPDATEs can be HOT (if tuned correctly)
- Only ~50 bytes WAL per update
- No index updates needed

**Orvos lacks this**:
- Always creates new TID
- Always updates indexes
- Always logs all columns (currently)

### 2. Row-oriented storage

**Heap advantage**:
- One page write per update (if HOT)
- All columns physically adjacent
- Single WAL record captures full row

**Orvos reality**:
- N separate B-tree operations (one per column)
- Each tree operation → separate WAL record
- More WAL overhead

### 3. Mature optimizations

**Heap has decades of tuning**:
- HOT updates (2007)
- In-place header updates
- Pruning and defragmentation
- Fillfactor tuning

**Orvos is newer**:
- Original Zedstore (2019-2021)
- Not production-tested at scale
- Optimizations not yet implemented

---

## What Orvos GAINS (Beyond WAL Efficiency)

Despite higher WAL usage, Orvos has advantages:

### 1. Column Projection Wins

**Heap**:
```sql
SELECT col1, col2 FROM wide_table WHERE col1 > 100;
-- Reads ALL columns from disk, discards unwanted ones
```

**Orvos**:
```sql
SELECT col1, col2 FROM wide_table WHERE col1 > 100;
-- Only reads col1 and col2 B-trees, skips other 98 columns
```

**WAL is small part of total I/O** in analytical workloads.

### 2. Compression

**Heap**: Limited compression (TOAST for large values)

**Orvos**: Per-column compression
- Similar values compress better together
- Especially good for:
  - Enum-like columns (few distinct values)
  - Timestamps (temporal locality)
  - Repeated strings

**Example**: 1TB heap table → 400GB Orvos table (2.5x compression)

**WAL overhead is negligible** compared to storage savings.

### 3. VACUUM Efficiency

**Heap**:
- VACUUM must scan entire table
- Updates visibility map
- Reorganizes tuples

**Orvos**:
- VACUUM only trims UNDO log
- Dead tuple space immediately reusable
- Less I/O intensive

**Faster VACUUM means less WAL churns.**

---

## Optimization Path Forward

### Phase 1: Column-Delta Updates (High Priority)

**Goal**: Only log changed columns

**Implementation**:
```c
// In orvosam_update():
for (attno = 1; attno <= relation->rd_att->natts; attno++)
{
    // NEW: Compare old vs new value
    if (!ov_tuple_attr_equals(attno, oldslot, slot))
    {
        // Only insert changed columns
        ovbt_attr_multi_insert(relation, attno,
                              &newdatum, &newisnull, &newtid, 1);
    }
    else
    {
        // Reuse old column value (no WAL, no I/O)
        // TID tree points to new TID, column trees keep old TID's value
    }
}
```

**Challenge**: Column trees use TID as key
- New TID, but some columns point to old TID's value?
- Need indirection or column versioning

**Better approach**: Multi-version column trees
- Store (TID, version) → value
- Unchanged columns: increment version, point to same value
- Changed columns: store new value

**Estimated effort**: 40-60 hours

**WAL savings**: 50-80% for typical UPDATE workloads

---

### Phase 2: HOT-like Updates for Orvos (Medium Priority)

**Goal**: Avoid index updates when possible

**Implementation**:
```c
// Check if indexed columns changed
if (!key_update)
{
    // Same-page update (if column value fits)
    // Don't propagate to indexes
    // Similar to heap's HOT
}
```

**Challenge**: "Same page" concept doesn't map directly to columnar
- Could mean: same B-tree leaf page in TID tree
- Or: same chunk of column data

**Estimated effort**: 60-80 hours

**Index update savings**: 20-40% reduction

---

### Phase 3: Partial Column Reconstruction (Lower Priority)

**Goal**: Avoid fetching all columns to determine which changed

**Current overhead**:
```c
// Must fetch old row to compare
orvosam_fetch_row(..., oldslot);  // Fetches ALL columns
key_update = is_key_update(relation, oldslot, slot);  // Compares
```

**Optimization**:
- Only fetch indexed columns for key_update check
- Only fetch changed columns (from executor's modified_attrs)
- Avoid full row reconstruction

**Estimated effort**: 20-30 hours

**CPU savings**: 10-20% on UPDATE-heavy workloads

---

## Practical Recommendations

### When Orvos's Current WAL Overhead Matters

**High-UPDATE workloads**:
- OLTP with frequent updates
- Real-time data ingestion with corrections
- Replication lag sensitivity (WAL volume matters)

**When to avoid Orvos (for now)**:
- Write-heavy OLTP
- Tables with frequent partial UPDATEs
- Workloads where replication lag is critical

### When Orvos's WAL Overhead Doesn't Matter

**OLAP workloads**:
- UPDATEs are rare (mostly INSERT + SELECT)
- Wide tables (column projection wins)
- Compression saves more than WAL costs

**Example**: Data warehouse
- Nightly batch UPDATEs: 1000 rows
- Daily queries: scan 100M rows, access 5 of 100 columns
- Column projection saves 95% of read I/O
- WAL overhead (extra 200 bytes/update) is negligible

---

## Benchmarking Plan

To quantify the difference:

### Test 1: Partial UPDATE on Wide Table

```sql
-- Heap
CREATE TABLE t_heap (
    id int PRIMARY KEY,
    c1 int, c2 int, ..., c100 int  -- 100 columns
);

-- Orvos
CREATE TABLE t_orvos (
    id int PRIMARY KEY,
    c1 int, c2 int, ..., c100 int
) USING orvos;

-- Benchmark: Update 1 column, 10000 times
UPDATE t_heap SET c1 = c1 + 1 WHERE id = ...;
UPDATE t_orvos SET c1 = c1 + 1 WHERE id = ...;

-- Measure:
-- 1. WAL volume (pg_current_wal_lsn())
-- 2. TPS (pgbench)
-- 3. Replication lag
```

### Test 2: Full UPDATE on Wide Table

```sql
-- Update all columns
UPDATE t_heap SET c1 = ..., c2 = ..., ..., c100 = ...;
UPDATE t_orvos SET c1 = ..., c2 = ..., ..., c100 = ...;

-- Expect: Orvos WAL overhead much smaller (all columns changed anyway)
```

### Test 3: Mixed Workload (OLAP-like)

```sql
-- 90% SELECT (column projection benefit)
-- 10% UPDATE (partial updates)

-- Measure total I/O (reads + WAL writes)
-- Orvos may still win overall due to read efficiency
```

---

## Conclusion

**Direct answer to the question:**

**Q**: Is Orvos more WAL-efficient on UPDATE than HEAP without sacrificing functionality?

**A**: **No, currently Orvos is LESS WAL-efficient for UPDATEs:**

1. **Orvos currently logs ALL columns** for every UPDATE, not just changed ones
2. **Heap logs only changed data** (plus small overhead)
3. **For partial UPDATEs**, Orvos uses 4-5x more WAL
4. **Orvos does NOT sacrifice MVCC functionality** - full transaction semantics preserved

**However:**

1. **This is fixable**: Column-delta optimization would make Orvos competitive
2. **Context matters**: OLAP workloads (rare UPDATEs, wide tables) - Orvos still wins overall
3. **Total I/O matters**: WAL is small fraction of total I/O; column projection saves more
4. **Logical replication**: UNDO records not needed for logical replication (already efficient in that respect)

**Recommendation**:
- Implement column-delta updates (Phase 1) to make Orvos WAL-competitive
- Ideal for OLAP/analytical workloads where UPDATE frequency is low
- Not optimal for write-heavy OLTP until optimizations implemented

**Total effort to fix**: 40-60 hours for column-delta optimization

