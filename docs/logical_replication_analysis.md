# Logical Replication Analysis for Orvos

**Date**: 2026-03-04
**Question**: Are UNDO log records sent to logical replicas? I assume so, but if not why not?

## Short Answer

**No, UNDO log records are NOT sent to logical replicas, and they don't need to be.**

Here's why:

---

## Understanding the Architecture

### What Orvos Logs to WAL

Looking at `src/include/access/orvos_wal.h`, Orvos has these WAL record types:

```c
#define WAL_ORVOS_INIT_METAPAGE          0x00
#define WAL_ORVOS_UNDO_NEWPAGE           0x10  // UNDO log metadata
#define WAL_ORVOS_UNDO_DISCARD           0x20  // UNDO log cleanup
#define WAL_ORVOS_BTREE_NEW_ROOT         0x30
#define WAL_ORVOS_BTREE_ADD_LEAF_ITEMS   0x40  // INSERT/UPDATE data
#define WAL_ORVOS_BTREE_REPLACE_LEAF_ITEM 0x50 // UPDATE data
#define WAL_ORVOS_BTREE_REWRITE_PAGES    0x60  // Page splits
#define WAL_ORVOS_TOAST_NEWPAGE          0x70  // Large values
```

**Key insight**: Orvos logs the **committed data** (B-tree operations), not the UNDO records themselves.

### What Heap Logs to WAL (For Comparison)

Heap has tuple-level WAL records:
- `XL_HEAP_INSERT` - Contains the new tuple data
- `XL_HEAP_UPDATE` - Contains old TID + new tuple data
- `XL_HEAP_DELETE` - Contains TID being deleted
- Plus visibility info (xmin, xmax, cmin, cmax)

### What Orvos Logs Instead

When you **INSERT** a tuple into Orvos:

1. **TID tree item** is added → logged as `WAL_ORVOS_BTREE_ADD_LEAF_ITEMS`
   - Contains: TID + visibility info (xmin, UNDO pointer)

2. **Column tree items** are added → logged as `WAL_ORVOS_BTREE_ADD_LEAF_ITEMS` for each column
   - Contains: TID + column value (compressed)

3. **UNDO log space** may be allocated → logged as `WAL_ORVOS_UNDO_NEWPAGE` (if new page needed)
   - Contains: Just metadata about the UNDO log structure
   - **NOT the UNDO record contents themselves**

---

## Why UNDO Records Don't Need to Go to Replicas

### Physical Replication (Streaming)

**Physical replicas** get:
- All WAL records (including `WAL_ORVOS_UNDO_NEWPAGE`, `WAL_ORVOS_UNDO_DISCARD`)
- Replay them exactly as the primary did
- End up with identical UNDO log structure

**Why this works:**
- Physical replication is byte-for-byte identical
- UNDO log pages are replicated just like any other pages
- Visibility checks on replica work identically to primary

### Logical Replication (Publication/Subscription)

**Logical replicas** get:
- **INSERT**: The final committed tuple data from column trees
- **UPDATE**: The old TID + new tuple data from column trees
- **DELETE**: The TID being deleted from TID tree

**Why UNDO records are NOT needed:**
- Logical replication decodes WAL to logical changes (INSERT/UPDATE/DELETE)
- UNDO records are for **visibility** on the primary, not the logical change itself
- By the time a transaction commits and is decoded, visibility is already determined
- The replica doesn't need to replay aborted transactions (UNDO records help with that on primary)

---

## The Fundamental Difference: UNDO vs REDO

### PostgreSQL Heap (and Orvos)
- **REDO log** (WAL) = How to reconstruct committed state
- Sent to replicas to rebuild data

### Orvos's UNDO Log
- **UNDO log** = How to roll back uncommitted transactions
- Used for MVCC visibility checks on the primary
- NOT needed for replication

### Analogy

Think of it like a construction site:

- **REDO log (WAL)** = Instructions for building the house
  - "Pour foundation", "Build walls", "Add roof"
  - Sent to replica sites to build identical houses

- **UNDO log** = Instructions for undoing mistakes
  - "If you poured foundation wrong, here's how to remove it"
  - Only needed at the primary site where mistakes happen
  - Replica sites just follow the final correct instructions

---

## What Actually Gets Sent to Logical Replicas

### For an INSERT

**Primary writes to WAL:**
1. `WAL_ORVOS_BTREE_ADD_LEAF_ITEMS` (TID tree)
   - TID: 12345
   - xmin: 1000
   - UNDO pointer: 0x1A2B3C (for rollback if needed)

2. `WAL_ORVOS_BTREE_ADD_LEAF_ITEMS` (column tree for 'name')
   - TID: 12345
   - Value: "Alice" (compressed)

3. `WAL_ORVOS_BTREE_ADD_LEAF_ITEMS` (column tree for 'age')
   - TID: 12345
   - Value: 30

**Logical decoder extracts:**
```sql
INSERT INTO users (name, age) VALUES ('Alice', 30);
```

**What replica receives:**
- Just the logical change (INSERT statement or its equivalent)
- No UNDO pointer, no xmin details
- Replica executes this INSERT into its own storage (could be heap, Orvos, or something else)

### For an UPDATE

**Primary writes to WAL:**
1. `WAL_ORVOS_BTREE_REPLACE_LEAF_ITEM` (TID tree)
   - Old TID: 12345 → Mark as updated, add UNDO record
   - New TID: 12346
   - New UNDO pointer: 0x1A2B3D

2. `WAL_ORVOS_BTREE_ADD_LEAF_ITEMS` (column tree for 'age')
   - TID: 12346
   - Value: 31 (new value)

**Logical decoder extracts:**
```sql
UPDATE users SET age = 31 WHERE id = 12345;
```

**What replica receives:**
- Logical change only
- Replica applies UPDATE to its own storage
- No UNDO records needed

---

## Why Orvos's Approach Works

### 1. UNDO is for Uncommitted Data

UNDO records help answer: "Is this tuple visible to my transaction?"

On the **primary**:
- Transaction A inserts tuple T (not yet committed)
- Transaction B queries → needs to check if T is visible
- Visibility check follows UNDO chain to determine T's state

On a **replica**:
- Only receives **committed** changes
- Never sees uncommitted data
- No need for UNDO chain traversal

### 2. Physical Replication Still Needs UNDO

**Physical replicas** need UNDO log because:
- They maintain MVCC snapshot isolation
- Can have queries running while primary commits
- Need to determine tuple visibility just like primary

**How they get it:**
- UNDO log pages are replicated as regular data pages
- `WAL_ORVOS_UNDO_NEWPAGE` records tell replica to allocate UNDO space
- `WAL_ORVOS_UNDO_DISCARD` records tell replica to free old UNDO pages
- But the UNDO record **contents** are in the B-tree items, not separate WAL records

### 3. Logical Replication Doesn't Need UNDO

**Logical replicas** don't need UNDO log because:
- They're not maintaining MVCC on the primary's data
- They're rebuilding the logical state from committed transactions
- They use their own visibility mechanism (could be different storage engine)

---

## Current Orvos Status: Logical Replication Support

### Investigation Results

**Searched for:**
- Logical decoding hooks in Orvos code: **None found**
- `LogicalDecodingContext` references: **None found**
- Documentation about logical replication: **Not mentioned in README**

**Conclusion**: Orvos currently **does not support logical replication**.

### Why Logical Replication is Missing

Orvos would need to implement **logical decoding callbacks** to support publication/subscription:

```c
// Heap provides these (src/backend/access/heap/heapam.c):
static bool heapam_scan_sample_next_block(...)
static bool heapam_scan_sample_next_tuple(...)
```

**For Orvos to support logical replication**, it would need:

1. **Decode B-tree operations into logical changes**:
   - `WAL_ORVOS_BTREE_ADD_LEAF_ITEMS` → INSERT
   - `WAL_ORVOS_BTREE_REPLACE_LEAF_ITEM` → UPDATE
   - Deleted TID tree entries → DELETE

2. **Reconstruct full tuples from column trees**:
   - Gather all column values for a TID
   - Assemble into logical tuple
   - Pass to logical decoding infrastructure

3. **Handle replica identity**:
   - Track which columns are part of replica identity
   - Include old values for UPDATE/DELETE

4. **Register callbacks in TableAmRoutine**:
   - Similar to heap's `relation_estimate_size`, `scan_sample_next_tuple`, etc.

---

## Comparison with Other Columnar Stores

### Citus Columnar Extension

**Storage**: Separate columnar tables via `USING columnar`

**Logical replication**:
- Works but treats columnar tables as regular tables
- Decodes from columnar-specific WAL records
- Has logical decoding support

### Greenplum / GPDB

**Storage**: Append-optimized columnar storage (AOCS)

**Replication**:
- Uses custom replication (not PostgreSQL logical replication)
- Segments have their own WAL

### AlloyDB (Google)

**Storage**: Hybrid row/column with automatic layout

**Replication**:
- Proprietary log-based replication
- Likely decodes from their custom WAL records
- Not using PostgreSQL logical replication as-is

---

## Implications and Recommendations

### Current State

**Physical replication**: ✅ Should work (UNDO log replicated as pages)

**Logical replication**: ❌ Not implemented

### Why This Matters

**Use cases that require logical replication:**
1. **Cross-version replication** (PG 15 → PG 16)
2. **Selective replication** (only some tables)
3. **Heterogeneous replication** (PG → MySQL, etc.)
4. **Bi-directional replication**
5. **Conflict resolution** (multi-master)

**Workarounds if logical replication needed:**
- Use physical replication instead (if acceptable)
- ETL tools (pg_dump, custom scripts)
- Trigger-based replication (performance impact)

### Implementation Plan

If logical replication support is desired:

#### Phase 1: Basic Decoding (40-60 hours)
1. Implement logical decoding callbacks in orvosam_methods
2. Decode `WAL_ORVOS_BTREE_ADD_LEAF_ITEMS` → INSERT
3. Decode TID tree changes → UPDATE/DELETE
4. Reconstruct tuples from column trees
5. Test with pg_recvlogical

#### Phase 2: Advanced Features (20-40 hours)
1. Support replica identity (FULL, DEFAULT, INDEX, NOTHING)
2. Handle TOASTed values correctly
3. Optimize tuple reconstruction (cache column tree positions)
4. Support logical decoding filtering

#### Phase 3: Testing & Documentation (20-30 hours)
1. Comprehensive test suite
2. Performance benchmarking
3. Documentation for users
4. Known limitations

**Total effort**: 80-130 hours (10-16 days)

**Priority**: Medium-Low
- Most users okay with physical replication
- Columnar stores typically used for OLAP (less need for replication)
- Can be added later without breaking existing functionality

---

## Summary

**Direct answer to the question:**

**Q**: Are UNDO log records sent to logical replicas?

**A**: No, UNDO log records are not sent to logical replicas, and they don't need to be because:

1. **Logical replication** decodes committed transactions into logical changes (INSERT/UPDATE/DELETE)
2. **UNDO records** are for determining visibility of uncommitted data on the primary
3. **Replicas** only receive committed changes, so they don't need UNDO chains
4. **Physical replicas** do get UNDO log pages (as regular page replicas), but through page-level replication, not as explicit UNDO record transmission

**Q**: If not, why not?

**A**: UNDO records are a **visibility mechanism**, not a **replication mechanism**:
- They help the primary determine "which version of this row should my transaction see?"
- Logical replicas don't maintain MVCC over the primary's data
- They rebuild logical state from committed transactions only
- They use their own visibility mechanism (could even be a different storage engine)

**Current status**: Orvos does not yet support logical replication (no logical decoding callbacks implemented). Physical replication should work fine.

