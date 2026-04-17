# Cluster-wide UNDO with Heap Table AM: Design Notes

## Cluster-wide UNDO Architecture

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
- Every DML writes: heap page + UNDO record = approximately 2x write amplification
- UNDO records persist until no transaction needs them (visibility horizon)

### When Beneficial
- Workloads with >5% abort rate (rollback is faster)
- Long-running transactions needing old snapshots (UNDO provides history)
- UPDATE-heavy workloads (cleaner rollback vs. heap scan)

### When Not Recommended
- Bulk load (COPY): 2x writes without abort benefit
- Append-only tables: rare aborts = pure overhead
- Space-constrained systems: UNDO retention increases storage

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
