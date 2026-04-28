# Cluster-wide UNDO with Heap Table AM: Design Notes

## Cluster-wide UNDO Architecture

### Design Goals
1. Enable faster transaction rollback without heap scans
2. Support UNDO-based MVCC for reducing bloat
3. Provide foundation for advanced features (time-travel, faster VACUUM)

### Core Components

**UNDO Logs** (`undolog.c`):
- Multi-segment architecture (default 16MB per segment, configurable via `undo_log_segment_size`)
- Managed segment lifecycle: FREE → ACTIVE → SEALED → DISCARDABLE
- Rotation at 85% capacity, 50% at checkpoint, 95% under pressure
- Backpressure mechanism when free segments are exhausted
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
- Every DML writes: heap page + UNDO record = approximately 2x write amplification
- UNDO records persist until no transaction needs them (visibility horizon)

### When Beneficial
- Workloads with >5% abort rate (rollback is faster)
- Long-running transactions needing old snapshots (UNDO provides history)
- UPDATE-heavy workloads (cleaner rollback vs. heap scan)

### When Not Recommended
- Append-only tables: rare aborts = pure overhead
- Space-constrained systems: UNDO retention increases storage

Note: Bulk DML operations (COPY, large INSERT/UPDATE/DELETE) benefit from
the bulk UNDO hints mechanism, which amortizes per-row overhead by batching
UNDO records.  The `begin_bulk_insert` table AM callback activates batched
recording for operations with >1000 estimated rows.  This significantly
reduces the write amplification penalty for bulk workloads.

## Heap AM Integration

The heap table AM integration (`heapam_undo.c`) connects the cluster-wide
UNDO infrastructure to PostgreSQL's standard heap storage:

### Tuple Operations
- **heap_insert**: Writes UNDO record with INSERT type (metadata only)
- **heap_delete**: Writes UNDO record with old tuple data
- **heap_update**: Writes UNDO record with old tuple version

### Visibility
- UNDO-enabled heap uses UNDO records for MVCC visibility checks
- Old tuple versions retrieved from UNDO logs instead of heap pages
- Reduces heap bloat by not maintaining multiple tuple versions in-page

### Rollback Path
1. Transaction abort triggers `UndoReplay()`
2. UNDO log scanned backwards (most recent record first)
3. Each record's inverse operation applied to heap pages
4. Heap pages restored to pre-transaction state

### Configuration
- `enable_undo = on` (postmaster-level GUC)
- `undo_retention` (seconds to keep UNDO records)
- `undo_log_segment_size` (segment size for UNDO logs)

## Segment Lifecycle Management

UNDO log segments follow a managed lifecycle rather than a fixed circular
buffer.  Each segment transitions through four states:

| State       | Description                                          |
|-------------|------------------------------------------------------|
| FREE        | Empty segment, available for allocation               |
| ACTIVE      | Currently receiving UNDO records from active writers  |
| SEALED      | Full; no new writes accepted. Awaiting discard        |
| DISCARDABLE | All records beyond visibility horizon; safe to recycle|

Rotation is triggered by `UndoLogSealAndRotate()` at configurable thresholds:
- **85% capacity**: Normal rotation to the next free segment
- **50% at checkpoint**: Bounds recovery replay length
- **95% pressure**: Emergency rotation with backpressure on writers

The `pg_undo_force_discard()` SQL function allows manual segment reclamation.

## Bulk UNDO Batching

For large DML operations, per-row UNDO overhead (UndoLogAllocate + WAL insert
+ UndoLogWrite per row) becomes a bottleneck.  The bulk UNDO hints mechanism
amortizes this cost across many rows:

### Activation
1. `ExecInitModifyTable` calls `table_begin_bulk_insert()` when the planner
   estimates >1000 affected rows
2. The heap AM callback `heapam_begin_bulk_insert` activates
   `HeapBeginBulkUndo()`, creating a persistent `UndoRecordSet`

### Operation
- `HeapBulkUndoAddRecord()` accumulates records in the persistent set
- Auto-flush at 256KB or 1000 records (whichever comes first)
- Single batch: one UndoLogAllocate, one WAL record, one UndoLogWrite

### Deactivation
- `ExecEndModifyTable` calls `table_finish_bulk_insert()` which flushes
  remaining records and frees the UndoRecordSet
- Memory context reset callback protects against dangling pointers on abort
