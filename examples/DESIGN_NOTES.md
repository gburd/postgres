# PostgreSQL UNDO Subsystems: Design Notes

This document explains the architectural decisions, trade-offs, and design
rationale for PostgreSQL's dual UNDO subsystems.

## Table of Contents

1. Overview of UNDO Subsystems
2. Cluster-wide UNDO Architecture
3. Per-Relation UNDO Architecture
4. FILEOPS Infrastructure
5. Async vs Synchronous Rollback
6. Performance Characteristics
7. When to Use Which System
8. Future Directions

---

## 1. Overview of UNDO Subsystems

PostgreSQL implements **two complementary UNDO subsystems**:

### Cluster-wide UNDO (`src/backend/access/undo/`)
- **Purpose**: Physical rollback and UNDO-based MVCC for standard heap tables
- **Storage**: Global UNDO logs in `base/undo/`
- **Integration**: Opt-in for heap AM via `enable_undo` storage parameter
- **Rollback**: Synchronous via `UndoReplay()` during transaction abort
- **Space management**: Global, shared across all UNDO-enabled tables

### Per-Relation UNDO (`src/backend/access/undo/relundo*.c`)
- **Purpose**: MVCC visibility and rollback for custom table access methods
- **Storage**: Per-table UNDO fork (`.undo` files)
- **Integration**: Table AMs implement callbacks (e.g., `test_undo_tam`)
- **Rollback**: Asynchronous via background workers (`relundo_worker.c`)
- **Space management**: Per-table, independent UNDO space

**Key Insight**: These systems serve different use cases and can coexist. A
database can have heap tables with cluster-wide UNDO and custom AM tables
with per-relation UNDO simultaneously.

---

## 2. Cluster-wide UNDO Architecture

### Design Goals
1. Enable faster transaction rollback without heap scans
2. Support UNDO-based MVCC for reducing bloat
3. Provide foundation for advanced features (time-travel, faster VACUUM)

### Core Components

**UNDO Logs** (`undolog.c`):
- Fixed-size segments (default 16MB, configurable via `undo_log_segment_size`)
- Circular buffer architecture: old segments reused when no longer needed
- Per-persistence-level logs (permanent, unlogged, temporary)

**UNDO Records** (`undorecord.c`):
- Self-contained: transaction ID + complete tuple data + metadata
- Chained: each record points to previous record in transaction
- Types: INSERT (stores nothing), UPDATE/DELETE (store old tuple version)

**Transaction Integration** (`xactundo.c`):
- `PrepareXactUndoData()`: Reserve UNDO space before DML
- `InsertXactUndoData()`: Write UNDO record
- `UndoReplay()`: Apply UNDO during rollback (synchronous)

**Background Workers** (`undoworker.c`):
- **Purpose**: Discard old UNDO records (cleanup/space reclamation)
- **NOT for rollback**: Rollback is synchronous in transaction abort path
- Periodically trim UNDO logs based on `undo_retention` and snapshot visibility

### Write Amplification
- Every DML writes: heap page + UNDO record ≈ 2x write amplification
- UNDO records persist until no transaction needs them (visibility horizon)

### When Beneficial
- Workloads with >5% abort rate (rollback is faster)
- Long-running transactions needing old snapshots (UNDO provides history)
- UPDATE-heavy workloads (cleaner rollback vs. heap scan)

### When Not Recommended
- Bulk load (COPY): 2x writes without abort benefit
- Append-only tables: rare aborts = pure overhead
- Space-constrained systems: UNDO retention increases storage

---

## 3. Per-Relation UNDO Architecture

### Design Goals
1. Enable custom table AMs to implement MVCC without heap overhead
2. Avoid global coordination (per-table independence)
3. Support async rollback (catalog access safe in background worker)

### Core Components

**UNDO Fork Management** (`relundo.c`):
- Each table has separate UNDO fork (relfilenode.undo)
- Metapage (block 0): head/tail/free chain pointers, generation counter
- Data pages: UNDO records stored sequentially
- Two-phase protocol: Reserve → Finish/Cancel

**Record Types**:
- `RELUNDO_INSERT`: Tracks inserted TID range
- `RELUNDO_DELETE`: Tracks deleted TID + optional tuple data
- `RELUNDO_UPDATE`: Tracks old/new TID pair + optional tuple data
- `RELUNDO_TUPLE_LOCK`: Tracks tuple lock acquisition
- `RELUNDO_DELTA_INSERT`: Tracks columnar delta (column store support)

**Async Rollback** (`relundo_worker.c`, `relundo_apply.c`):
- **Why async?**: Cannot call `relation_open()` during `TRANS_ABORT` state
- Background workers execute in proper transaction context
- Work queue: Abort queues per-relation UNDO chains for workers
- Workers apply UNDO, write CLRs (Compensation Log Records)

**Transaction Integration** (`xactundo.c`):
- `RegisterPerRelUndo()`: Track relation UNDO chains per transaction
- `GetPerRelUndoPtr()`: Chain UNDO records within relation
- `ApplyPerRelUndo()`: Queue work for background workers on abort

### Why Async-Only for Per-Relation UNDO?

**Problem**: During transaction abort (`AbortTransaction()`), PostgreSQL is in
`TRANS_ABORT` state where catalog access is forbidden. `relation_open()` has:
```c
Assert(IsTransactionState());  // Fails in TRANS_ABORT
```

**Failed approach**: Synchronous rollback with `PG_TRY/PG_CATCH`
- Attempted to apply UNDO synchronously, fall back to async on failure
- Result: Crash due to assertion failure (cannot open relation)

**Solution**: Pure async architecture
- Abort queues work: `RelUndoQueueAdd(dboid, reloid, undo_ptr, xid)`
- Worker applies UNDO: `RelUndoApplyChain(rel, start_ptr)` in clean transaction
- Matches ZHeap architecture (deferred UNDO application)

### ZHeap TPD vs. Per-Relation UNDO

**ZHeap TPD (Transaction Page Directory)**:
- Per-page transaction metadata (slots co-located with heap pages)
- No separate UNDO fork
- Page-resident transaction history
- Trade-off: Page bloat vs. fewer page reads

**Per-Relation UNDO (this implementation)**:
- Separate UNDO fork (no heap page overhead)
- Centralized metadata storage
- Chain walking for visibility
- Trade-off: Separate I/O vs. no page bloat

**Why not TPD?**:
1. Non-invasive: No page layout changes required
2. Optionality: Table AMs opt-in via callbacks
3. Scalability: Works for 1B+ block tables
4. Evolution path: Can optimize to per-page later if proven beneficial

### When to Use Per-Relation UNDO
- Custom table AMs (columnar, log-structured, etc.)
- MVCC needs without heap overhead
- Per-table UNDO isolation requirements
- Workloads benefiting from async rollback

---

## 4. FILEOPS Infrastructure

### Purpose
WAL-logged file system operations that integrate with PostgreSQL transactions.

### Operations
- `FileOpsCreate(rel, forknum)`: Create new fork
- `FileOpsExtend(rel, forknum, nblocks)`: Extend fork
- `FileOpsDrop(rel, forknum)`: Mark fork for deletion
- `FileOpsTruncate(rel, forknum, nblocks)`: Truncate fork

### Benefits
- **Atomic**: File operations commit/rollback with transaction
- **Crash-safe**: WAL-logged (RM_FILEOPS_ID)
- **Correct standby replay**: File operations replayed on replicas

### Use Cases
- Per-relation UNDO fork lifecycle
- Custom table AM fork management
- Extension developers needing transactional file operations

---

## 5. Async vs Synchronous Rollback

### Cluster-wide UNDO: Synchronous
- Rollback happens in `AbortTransaction()` via `UndoReplay()`
- Sequential UNDO log scan (fast, cache-friendly)
- Completes before returning control to user
- No background worker coordination needed

### Per-Relation UNDO: Asynchronous
- Rollback queued to background worker
- Worker applies UNDO in clean transaction context
- User transaction completes immediately
- Eventual consistency: UNDO applied asynchronously

**Testing**: For determinism, test_undo_tam provides `test_undo_tam_process_pending()`
to drain worker queue synchronously.

---

## 6. Performance Characteristics

### Cluster-wide UNDO
| Operation | Cost | Notes |
|-----------|------|-------|
| INSERT | +100% writes | Heap + UNDO record |
| UPDATE | +100% writes | Heap + old tuple in UNDO |
| DELETE | +100% writes | Heap + deleted tuple in UNDO |
| Rollback | O(n) sequential | UNDO log scan (cache-friendly) |
| Space | Retention-based | `undo_retention` seconds |

### Per-Relation UNDO
| Operation | Cost | Notes |
|-----------|------|-------|
| INSERT | +50% writes | Heap + metadata-only UNDO |
| UPDATE | +100% writes | Heap + old tuple in UNDO (if stored) |
| DELETE | +100% writes | Heap + deleted tuple in UNDO (if stored) |
| Rollback | Async | Background worker applies UNDO |
| Space | Per-table | Independent UNDO fork |

---

## 7. When to Use Which System

### Use Cluster-wide UNDO (Heap + enable_undo=on)
✅ OLTP with frequent aborts (>5%)
✅ UPDATE-heavy workloads
✅ Long-running transactions needing old snapshots
✅ Workloads benefiting from cleaner rollback
❌ Bulk load (COPY) workloads
❌ Append-only tables
❌ Space-constrained systems

### Use Per-Relation UNDO (Custom Table AM)
✅ Custom table AMs (columnar, log-structured)
✅ MVCC without heap overhead
✅ Per-table UNDO isolation
✅ Async rollback requirements
❌ Standard heap tables (use cluster-wide UNDO instead)

### Use Neither
✅ Append-only workloads (minimal aborts)
✅ Bulk load scenarios (COPY)
✅ Read-only replicas
✅ Space-critical deployments

---

## 8. Future Directions

### Cluster-wide UNDO
1. **Undo-based MVCC**: Reduce bloat by storing old versions in UNDO
2. **Time-travel queries**: `SELECT * FROM t AS OF SYSTEM TIME '...'`
3. **Faster VACUUM**: Discard entire UNDO segments instead of scanning heap
4. **Parallel rollback**: Multi-worker UNDO application

### Per-Relation UNDO
1. **Subtransaction support**: ROLLBACK TO SAVEPOINT via UNDO
2. **Per-page compression**: Optimize UNDO space via page-level compression
3. **Hybrid architecture**: Hot pages in memory, cold pages in UNDO fork
4. **Columnar integration**: Delta UNDO records for column stores

### FILEOPS
1. **Directory operations**: Transactional mkdir/rmdir
2. **Atomic rename**: WAL-logged file rename
3. **Extended attributes**: Transactional metadata storage

---

## Conclusion

PostgreSQL's dual UNDO subsystems provide flexibility:
- **Cluster-wide UNDO** enables faster rollback and UNDO-based MVCC for standard heap
- **Per-Relation UNDO** enables custom table AMs to implement MVCC independently
- **FILEOPS** provides transactional file operations as foundational infrastructure

Choose the system that matches your workload characteristics and requirements.
