# RECNO Implementation Guide

This document provides a code walkthrough of the RECNO storage access method
for developers who want to understand, modify, or extend the implementation.
All file paths are relative to the PostgreSQL source root unless noted.


## Source File Map

### Core Files

| File | Lines | Purpose |
|------|-------|---------|
| `src/backend/access/recno/recno_handler.c` | ~800 | Table AM interface: scan, fetch, DDL callbacks |
| `src/backend/access/recno/recno_tuple.c` | ~500 | Tuple formation, deformation, page operations |
| `src/backend/access/recno/recno_operations.c` | ~686 | Insert, update, delete, multi-insert, vacuum |
| `src/backend/access/recno/recno_mvcc.c` | ~459 | Time-based MVCC, serializable isolation |
| `src/backend/access/recno/recno_xlog.c` | ~554 | WAL logging and REDO replay |
| `src/backend/access/recno/recno_fsm.c` | ~580 | Free space management, defragmentation |
| `src/backend/access/recno/recno_overflow.c` | ~525 | Overflow page chains for large attributes |
| `src/backend/access/recno/recno_compress.c` | ~551 | Attribute-level compression framework |
| `src/backend/access/recno/recno_lock.c` | ~289 | Tuple and page locking |
| `src/backend/access/recno/recno_hlc.c` | ~911 | HLC + DVV implementation with TSC support |
| `src/backend/access/recno/recno_slot.c` | ~600 | Custom TupleTableSlot (RecnoTupleTableSlot) |
| `src/backend/access/recno/recno_stats.c` | ~276 | RECNO-specific statistics for ANALYZE |

### Header Files

| File | Purpose |
|------|---------|
| `src/include/access/recno.h` | All data structures, constants, function prototypes |
| `src/include/access/recno_xlog.h` | WAL record types and structures |

### Build Files

| File | Purpose |
|------|---------|
| `src/backend/access/recno/Makefile` | Make build (OBJS list) |
| `src/backend/access/recno/meson.build` | Meson build (backend_sources) |


## Key Data Structures

### RecnoTupleHeader (recno.h:52)

The on-disk tuple header. Every tuple stored on a RECNO page begins with
this structure:

```c
typedef struct RecnoTupleHeader
{
    uint32      t_len;          /* Total tuple length including header */
    uint16      t_natts;        /* Number of attributes */
    uint16      t_flags;        /* RECNO_TUPLE_DELETED, _UPDATED, etc. */
    uint64      t_commit_ts;    /* Commit timestamp (microseconds) */
    uint64      t_xact_ts;      /* Transaction start timestamp */
    ItemPointerData t_ctid;     /* Points to updated version (if any) */
    CommandId   t_cid;          /* Command ID within transaction */
    uint16      t_infomask;     /* HASNULL, HASVARWIDTH, COMPRESSED, etc. */
    uint8       t_attrs_bitmap[FLEXIBLE_ARRAY_MEMBER];
} RecnoTupleHeader;
```

**Tuple flags** (recno.h:67-72):
- `RECNO_TUPLE_COMPRESSED` (0x0001) -- Has compressed attributes
- `RECNO_TUPLE_HAS_OVERFLOW` (0x0002) -- Has overflow page references
- `RECNO_TUPLE_DELETED` (0x0004) -- Tombstone: logically deleted
- `RECNO_TUPLE_UPDATED` (0x0008) -- Old version; t_ctid points to new
- `RECNO_TUPLE_LOCKED` (0x0010) -- Locked by concurrent transaction
- `RECNO_TUPLE_SPECULATIVE` (0x0020) -- Speculative insertion (unused)

### RecnoTupleData (recno.h:84)

The in-memory representation wrapping a tuple header:

```c
typedef struct RecnoTupleData
{
    uint32              t_len;      /* Length of tuple */
    ItemPointerData     t_self;     /* TID of this tuple */
    Oid                 t_tableOid; /* Table OID */
    RecnoTupleHeader   *t_data;     /* Pointer to header + data */
} RecnoTupleData;
```

### RecnoPageOpaqueData (recno.h:30)

Stored in each page's special space (accessed via `PageGetSpecialPointer`):

```c
typedef struct RecnoPageOpaqueData
{
    uint64  pd_commit_ts;       /* Highest commit timestamp on page */
    uint16  pd_free_space;      /* Cached free space amount */
    uint16  pd_defrag_counter;  /* How many times page was defragmented */
    uint32  pd_flags;           /* RECNO_PAGE_COMPRESSED, _OVERFLOW, etc. */
} RecnoPageOpaqueData;
```

### RecnoScanDescData (recno.h:218)

Scan descriptor extending PostgreSQL's TableScanDescData:

```c
typedef struct RecnoScanDescData
{
    TableScanDescData rs_base;      /* Base scan descriptor */
    Buffer      rs_cbuf;            /* Current buffer */
    BlockNumber rs_cblock;          /* Current block */
    OffsetNumber rs_cindex;         /* Current offset in page */
    OffsetNumber rs_coffset;        /* Current offset number */
    int         rs_ntuples;         /* Visible tuples on current page */
    OffsetNumber *rs_vistuples;     /* Offset numbers of visible tuples */
    uint64      rs_snapshot_ts;     /* Snapshot timestamp */
    uint64      rs_xact_ts;        /* Transaction timestamp */
    RecnoMvccData *rs_mvcc;         /* MVCC state */
} RecnoScanDescData;
```


## Key Algorithms

### 1. Tuple Visibility (recno_tuple.c)

RECNO's visibility check is a timestamp comparison, much simpler than heap's
XID-based approach:

```
RecnoTupleVisible(tuple, snapshot_ts, xact_ts):
  if snapshot_ts == 0:
      return true                        // SnapshotAny
  if tuple.t_flags & RECNO_TUPLE_DELETED:
      return tuple.t_commit_ts > snapshot_ts  // Deleted after snapshot
  return tuple.t_commit_ts <= snapshot_ts     // Inserted before snapshot
```

The snapshot timestamp is the transaction's start timestamp, obtained from
`RecnoGetSnapshotTimestamp()` (recno_mvcc.c:333). For MVCC snapshots, this
is the value assigned when the per-transaction state was initialized.

### 2. Insert Path (recno_operations.c:50-166)

```
recno_tuple_insert(relation, slot, cid, options, bistate):
  1. slot_getallattrs(slot)
  2. RecnoFormTuple() -- create RecnoTuple from slot values/nulls
  3. Set MVCC fields (t_commit_ts, t_xact_ts, t_cid)
  4. RecnoGetPageWithFreeSpace() -- find page via FSM
  5. ReadBuffer + LockBuffer(EXCLUSIVE)
  6. Verify PageGetFreeSpace >= tuple_size
     - If stale FSM: update FSM, retry once
  7. START_CRIT_SECTION()
  8. RecnoPageAddTuple() -- add to page item array
  9. ItemPointerSet() -- set TID in slot
  10. MarkBufferDirty()
  11. RecnoXLogInsert() -- WAL log
  12. END_CRIT_SECTION()
  13. RecnoRecordFreeSpace() -- update FSM
  14. UnlockReleaseBuffer()
```

### 3. Delete Path (recno_operations.c:171-301)

Deletes use tombstoning rather than physical removal:

```
recno_tuple_delete(relation, tid, cid, snapshot, ...):
  1. Validate TID (block exists, offset valid)
  2. RecnoLockTuple(LockTupleExclusive)
  3. LockBuffer(EXCLUSIVE)
  4. Check: already deleted? -> TM_Deleted
  5. Check: visible to snapshot? -> TM_Invisible if not
  6. START_CRIT_SECTION()
  7. Set RECNO_TUPLE_DELETED flag on tuple header
  8. Update t_commit_ts, t_cid
  9. MarkBufferDirty()
  10. RecnoXLogDelete() -- WAL log (includes old tuple for UNDO)
  11. END_CRIT_SECTION()
  12. Release locks and buffer
  13. RecnoRecordFreeSpace() -- update FSM
```

### 4. Update Path (recno_operations.c:306-636)

Updates attempt in-place modification first, then fall back to out-of-place:

```
recno_tuple_update(relation, otid, slot, cid, snapshot, ...):
  1. Read page containing old tuple
  2. RecnoLockTuple(LockTupleExclusive)
  3. Visibility and deletion checks
  4. RecnoFormTuple() for new version
  5. Check: new_tuple_size <= ItemIdGetLength(old)?
     YES -> In-place update:
       - memcpy new data over old
       - Update ItemId length
       - Same TID returned
     NO -> Out-of-place update:
       a. Try RecnoPageAddTuple on same page
       b. If no space: RecnoGetPageWithFreeSpace for new page
       c. Set RECNO_TUPLE_UPDATED flag on old
       d. Set old tuple's t_ctid to point to new
  6. START_CRIT_SECTION()
  7. Apply changes to page(s)
  8. MarkBufferDirty()
  9. RecnoXLogUpdate() -- WAL with before/after images
  10. END_CRIT_SECTION()
  11. Release locks and buffers
```

### 5. Overflow Page Management (recno_overflow.c)

Overflow pages store large attributes that don't fit inline:

```
RecnoOverflowWrite(buffer, value, value_len):
  1. If value_len > RECNO_OVERFLOW_THRESHOLD:
     a. Allocate overflow page via FSM
     b. Create overflow header with:
        - Magic number (RECNO_OVERFLOW_MAGIC)
        - Total length
        - Chain pointer (initially InvalidBlockNumber)
     c. Write data chunks across pages
     d. Return OverflowPointer structure
  2. Store OverflowPointer in main tuple
  3. WAL log with XLOG_RECNO_OVERFLOW_WRITE

RecnoOverflowRead(relation, overflow_ptr):
  1. Validate magic number
  2. Follow chain pointers reading chunks
  3. Reassemble complete value
  4. Return reconstructed datum
```

### 6. Compression Framework (recno_compress.c)

Attribute-level compression with algorithm selection:

```
RecnoCompressAttribute(attr_data, attr_len, algorithm):
  1. Select algorithm based on:
     - Data type (numeric -> delta, text -> dictionary/LZ4)
     - Size (small -> dictionary, large -> LZ4/ZSTD)
  2. Attempt compression
  3. Check compression ratio (must save >20%)
  4. If beneficial:
     - Store compressed data
     - Prepend RecnoCompressionHeader:
       * Algorithm ID
       * Original size
       * Compressed size
       * Compression level
  5. Set RECNO_TUPLE_COMPRESSED flag
  6. WAL log with XLOG_RECNO_COMPRESS

RecnoDecompressAttribute(compressed_data):
  1. Read RecnoCompressionHeader
  2. Dispatch to algorithm handler
  3. Decompress and return original data
```

### 7. MVCC and Snapshot Management (recno_mvcc.c)

Time-based MVCC with monotonic timestamp generation:

```
RecnoGetCommitTimestamp():
  1. LWLockAcquire(RecnoTimestampLock, LW_EXCLUSIVE)
  2. current_ts = GetCurrentTimestamp()
  3. If current_ts <= last_commit_ts:
     current_ts = last_commit_ts + 1  // Ensure monotonicity
  4. last_commit_ts = current_ts
  5. LWLockRelease(RecnoTimestampLock)
  6. Return current_ts

RecnoCheckVisible(tuple, snapshot):
  1. If SnapshotAny: return true
  2. If tuple deleted:
     return tuple->t_commit_ts > snapshot->ts
  3. Else:
     return tuple->t_commit_ts <= snapshot->ts
```

### 8. Free Space Management (recno_fsm.c)

Multi-level free space categorization:

```
RecnoCategorizePage(page):
  free_pct = PageGetFreeSpace(page) * 100 / BLCKSZ
  if free_pct < 5: return FSM_FULL
  if free_pct < 25: return FSM_TIGHT
  if free_pct < 50: return FSM_MEDIUM
  if free_pct < 75: return FSM_LOOSE
  return FSM_EMPTY

RecnoGetPageWithFreeSpace(relation, needed_space):
  1. Check FSM for page with sufficient space
  2. If found: return page
  3. Else: extend relation with new page
  4. Initialize new page
  5. Update FSM
  6. Return new page
```

### 9. WAL Logging and Recovery (recno_xlog.c)

Custom WAL resource manager for RECNO operations:

```
RecnoXLogInsert(relation, buffer, tuple):
  xl_recno_insert xlrec = {
    .target = BufferGetBlockNumber(buffer),
    .offnum = tuple_offnum
  }
  XLogBeginInsert()
  XLogRegisterData(&xlrec, sizeof(xlrec))
  XLogRegisterBuffer(0, buffer, REGBUF_STANDARD)
  XLogRegisterBufData(0, tuple_data, tuple_len)
  recptr = XLogInsert(RM_RECNO_ID, XLOG_RECNO_INSERT)
  PageSetLSN(page, recptr)

recno_redo(XLogReaderState *record):
  switch (info & XLOG_RECNO_OPMASK):
    case XLOG_RECNO_INSERT:
      RecnoRedoInsert(record)
    case XLOG_RECNO_UPDATE_INPLACE:
      RecnoRedoUpdateInPlace(record)
    case XLOG_RECNO_DELETE:
      RecnoRedoDelete(record)
    // ... other cases
```

### 10. HLC/DVV Implementation (recno_hlc.c)

Hybrid Logical Clock with Dotted Version Vectors:

```
RecnoHLCNow():
  1. Read system clock (milliseconds)
  2. If physical <= last_physical:
     logical++  // Increment logical counter
  3. Else:
     logical = 0  // Reset on new millisecond
     last_physical = physical
  4. Pack into uint64: (physical << 16) | logical
  5. Return HLC timestamp

RecnoDVVGenerate(node_id):
  1. event_counter++
  2. Pack into uint64:
     - Bits 63-52: node_id (12 bits)
     - Bits 51-48: flags (4 bits)
     - Bits 47-0: event_counter (48 bits)
  3. Return DVV dot
```

## Key Debugging Points

### Common Issues and Solutions

1. **Tuple Not Visible After Insert**
   - Check: Commit timestamp vs snapshot timestamp
   - Debug: Add elog() in RecnoTupleVisible()
   - Solution: Ensure RecnoSetCommitTimestamp() called

2. **Update Hangs**
   - Check: Lock acquisition in RecnoLockTuple()
   - Debug: pg_locks view for lock waits
   - Solution: Check for deadlocks, lock timeout

3. **WAL Replay Failures**
   - Check: WAL record completeness
   - Debug: Enable wal_debug, trace_recovery_messages
   - Solution: Ensure all modified buffers registered

4. **Space Leak (Table Grows But Not Reclaimed)**
   - Check: FSM accuracy with RecnoGetFSMStats()
   - Debug: Track page free space over time
   - Solution: Run VACUUM, check defragmentation

5. **Compression Not Working**
   - Check: recno_enable_compression GUC
   - Debug: Add elog() in RecnoCompressAttribute()
   - Solution: Verify algorithm implementation

### Performance Profiling Points

```sql
-- Check timestamp generation bottleneck
SELECT wait_event, count(*)
FROM pg_stat_activity
WHERE wait_event LIKE '%Recno%'
GROUP BY wait_event;

-- Monitor compression effectiveness
SELECT
  schemaname,
  tablename,
  pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) as size,
  (SELECT COUNT(*) FROM recno_compression_stats(schemaname||'.'||tablename)) as compressed_attrs
FROM pg_tables
WHERE tablename IN (SELECT tablename FROM pg_tables WHERE schemaname || '.' || tablename IN
  (SELECT c.relname FROM pg_class c WHERE c.relam = (SELECT oid FROM pg_am WHERE amname = 'recno')));

-- Track in-place vs out-of-place updates
CREATE TABLE recno_update_stats (
  timestamp timestamptz DEFAULT now(),
  in_place_count bigint,
  out_of_place_count bigint
);
```

## Testing Specific Features

### Test In-Place Updates

```sql
-- Create test table
CREATE TABLE test_inplace (id int, data text) USING recno;
INSERT INTO test_inplace VALUES (1, 'short');

-- This should be in-place (same size)
UPDATE test_inplace SET data = 'short' WHERE id = 1;

-- This should be out-of-place (larger)
UPDATE test_inplace SET data = repeat('x', 1000) WHERE id = 1;

-- Check TID changes
SELECT ctid, * FROM test_inplace;
```

### Test Overflow Pages

```sql
-- Force overflow
CREATE TABLE test_overflow (id int, large text) USING recno;
INSERT INTO test_overflow VALUES (1, repeat('A', 10000));

-- Verify overflow created
SELECT * FROM recno_overflow_stats('test_overflow');
```

### Test Compression

```sql
-- Enable compression
SET recno_enable_compression = on;
SET recno_compression_level = 6;

CREATE TABLE test_compress (id int, data text) USING recno;
INSERT INTO test_compress VALUES (1, repeat('AAAA', 1000));

-- Check compression
SELECT * FROM recno_compression_stats('test_compress');
          - Lock ordering: lower block number first
       c. Set RECNO_TUPLE_UPDATED on old tuple
       d. Set old tuple's t_ctid -> new location
       e. Mark both pages dirty, WAL log
  6. Return TM_Ok, set update_indexes hint
```

The cross-page lock ordering (recno_operations.c:464-483) is critical for
deadlock prevention. When the new page has a lower block number than the old
page, the implementation releases the old page lock, locks the new page, then
re-locks the old page.

### 5. Sequential Scan (recno_handler.c)

The scan path follows standard PostgreSQL table AM patterns:

```
recno_scan_begin():
  - Allocate RecnoScanDesc
  - Initialize snapshot timestamp from RecnoGetSnapshotTimestamp()
  - Set rs_cblock = 0, rs_coffset = InvalidOffsetNumber

recno_scan_getnextslot():
  - Iterate blocks from rs_cblock
  - For each page:
    - ReadBuffer, LockBuffer(SHARE)
    - Iterate offsets from FirstOffsetNumber to MaxOffsetNumber
    - For each ItemId: check ItemIdIsNormal
    - Get RecnoTupleHeader, call RecnoTupleVisible()
    - If visible: fill slot via RecnoTupleToSlot(), return true
  - Return false when all blocks exhausted
```

### 6. WAL Record Format (recno_xlog.c, recno_xlog.h)

WAL record types (recno_xlog.h:27-36):

| Code | Name | Content |
|------|------|---------|
| 0x00 | XLOG_RECNO_INSERT | xl_recno_insert + tuple data |
| 0x10 | XLOG_RECNO_UPDATE_INPLACE | xl_recno_update + old tuple + new tuple |
| 0x20 | XLOG_RECNO_DELETE | xl_recno_delete + old tuple (for UNDO) |
| 0x30 | XLOG_RECNO_DEFRAG | xl_recno_defrag + offset mappings |
| 0x40 | XLOG_RECNO_OVERFLOW_WRITE | xl_recno_overflow_write + data |
| 0x50 | XLOG_RECNO_COMPRESS | xl_recno_compress + compressed data |
| 0x60 | XLOG_RECNO_INIT_PAGE | xl_recno_init_page |

**REDO replay** (recno_xlog.c:262-531):
The `recno_redo()` function handles each record type. For updates and deletes,
it implements UNDO/REDO logic by comparing the page LSN to the record's LSN:
- If `PageGetLSN(page) > record->EndRecPtr`: UNDO (restore old state)
- Otherwise: REDO (apply new state)

**Insert WAL format** (recno_xlog.c:38-63):
```
[xl_recno_insert header]  -- offnum, flags, commit_ts, xact_ts
[tuple data]              -- complete RecnoTupleHeader + attribute data
[buffer reference]        -- REGBUF_STANDARD
```

**Update WAL format** (recno_xlog.c:68-99):
```
[xl_recno_update header]  -- offnum, flags, old/new commit_ts, xact_ts, old_tuple_len
[old tuple data]          -- before-image for UNDO
[new tuple data]          -- after-image for REDO
[buffer reference]        -- REGBUF_STANDARD
```

### 7. Free Space Management (recno_fsm.c)

FSM uses 5 categories based on free space ratio:

| Category | Free Space Ratio |
|----------|-----------------|
| 0 (FULL) | 0% |
| 1 | 25% |
| 2 | 50% |
| 3 | 75% |
| 4 (EMPTY) | 100% |

Page allocation (RecnoGetPageWithFreeSpace, recno_fsm.c:121-177):
1. Query PostgreSQL's standard FSM via `GetPageWithFreeSpace()`
2. Verify the returned page actually has enough space
3. If stale: update FSM with actual free space, try again
4. If no suitable page: extend relation with `ReadBufferExtended(P_NEW)`
5. Initialize new page with `RecnoInitPage()` and WAL-log it

Defragmentation scheduling (recno_fsm.c:192-196):
Pages are marked for defragmentation when:
- FSM category > 0 (some space) AND free space < 60% of page size
The defrag queue is a circular buffer of BlockNumbers.

### 8. Overflow Page Chains (recno_overflow.c)

Attributes larger than `RECNO_OVERFLOW_THRESHOLD` (which is
`RECNO_MAX_TUPLE_SIZE / 4`) are stored in overflow page chains.

**Store** (recno_overflow.c:49-154):
```
RecnoStoreOverflow(rel, value, attnum):
  1. Extract data pointer and length from varlena
  2. Allocate RecnoOverflowRef
  3. Loop while remaining > 0:
     a. RecnoAllocateOverflowPage() -- from FSM or extend relation
     b. RecnoInitOverflowPage() -- set RECNO_PAGE_OVERFLOW flag
     c. Copy min(remaining, RECNO_OVERFLOW_PAGE_SIZE) bytes
     d. Link to previous page via RecnoLinkOverflowPages()
     e. MarkBufferDirty(), WAL log
  4. Return overflow_ref with first_page and total_length
```

**Fetch** (recno_overflow.c:159-225):
Follows the `next_overflow_page` chain, validating `data_offset` at each
page to detect corruption. Assembles the full attribute into a single
varlena allocation.

**Inline references** (recno.h:123-163):
When a tuple has overflow data, the main tuple stores a `RecnoOverflowInline`
structure wrapped as a varlena. The magic value `0xDEAD0F10` identifies it
during deformation. Check with `RecnoIsOverflowRef()`.

### 9. Compression Framework (recno_compress.c)

**Algorithm selection** (recno_compress.c:228-259):
```
RecnoChooseCompressionType(typid, value, value_size):
  TEXT/VARCHAR/BPCHAR:
    size < 1024 -> DICTIONARY
    else -> LZ4
  NUMERIC/INT4/INT8/FLOAT4/FLOAT8:
    -> DELTA
  BYTEA:
    -> ZSTD
  default:
    -> LZ4
```

**Compression flow** (recno_compress.c:87-183):
1. Skip if `!recno_enable_compression` or size < 32 bytes
2. Choose algorithm (or use caller-specified type)
3. Call algorithm-specific compress function
4. Check if compressed size < original * 0.8 (20% savings minimum)
5. If beneficial: prepend `RecnoCompressionHeader` (8 bytes) + compressed data
6. If not: return original value unchanged

**Dictionary compression** (recno_compress.c:385-516):
Maintains a per-backend dictionary in `CacheMemoryContext` with up to 1024
entries. Exact-match lookup replaces the value with a 4-byte dictionary ID.
The dictionary is not persisted -- it is lost on backend restart.

### 10. Locking Protocol (recno_lock.c)

**Tuple locking** (recno_lock.c:33-80):
Uses PostgreSQL's standard `LOCKTAG_TUPLE` mechanism. Lock modes:
- `LockTupleKeyShare` / `LockTupleShare` -> `ShareLock`
- `LockTupleNoKeyExclusive` / `LockTupleExclusive` -> `ExclusiveLock`

**Multi-tuple locking** (recno_lock.c:192-241):
`RecnoLockMultipleTuples()` sorts TIDs before acquiring locks to prevent
deadlocks. Uses bubble sort (adequate for small N). On failure, releases
all acquired locks.

**Page locking** (recno_lock.c:120-151):
Uses `LOCKTAG_PAGE` for page-level exclusive access during operations that
modify page structure.


## Concurrency Control Details

### Buffer Lock Protocol

All page reads acquire `BUFFER_LOCK_SHARE`. All page modifications acquire
`BUFFER_LOCK_EXCLUSIVE`. The lock is held for the minimum necessary duration.

For cross-page updates (recno_operations.c:464-483):
- If new_block < old_block: release old, lock new, re-lock old
- If new_block > old_block: keep old locked, lock new

### Critical Sections

All page modifications are wrapped in `START_CRIT_SECTION()` /
`END_CRIT_SECTION()`. Within a critical section, `ereport(ERROR)` is
prohibited -- only `elog(PANIC)` is safe. This ensures that partial page
modifications cannot occur: either the full operation completes and is
WAL-logged, or the system panics and recovery replays the WAL.

### MVCC Timestamp Assignment

`RecnoGetCommitTimestamp()` (recno_mvcc.c:138-162) is the single point of
timestamp generation. It:
1. Gets wall-clock time via `GetCurrentTimestamp()`
2. Acquires `RecnoMvccShmem->mvcc_lock` exclusively
3. Ensures the new timestamp > `global_commit_ts`
4. Updates `global_commit_ts`
5. Releases the lock

This guarantees strict ordering but creates a serialization point. Under
extreme write concurrency, this lock may become a bottleneck.


## Debugging Tips

### Common Pitfalls

1. **Missing WAL logging**: Any page modification not WAL-logged will cause
   data loss on crash. Always call the appropriate `RecnoXLog*` function
   inside the critical section.

2. **Buffer lock ordering**: Never acquire two buffer locks in arbitrary
   order. Always lock lower block numbers first.

3. **Critical section violations**: Never call `ereport(ERROR)` inside
   `START_CRIT_SECTION()`. Use `elog(PANIC)` for truly unrecoverable
   conditions.

4. **FSM staleness**: Always verify free space after reading a page suggested
   by FSM. Update FSM with actual values when stale.

5. **Overflow page leaks**: Ensure overflow pages are freed when tuples are
   deleted. Currently this happens in VACUUM (when implemented) by scanning
   for unreferenced overflow pages.

6. **Slot type mismatch**: The handler returns `TTSOpsRecnoTuple` (defined
   in recno_slot.c). Ensure all code paths that create or manipulate slots
   use `TTSOpsRecnoTuple`, not `TTSOpsBufferHeapTuple` or `TTSOpsVirtual`.
   The `tts_recno_copy_heap_tuple()` function handles conversion when heap
   format is needed (e.g., index building).

### Useful Debugging Queries

```sql
-- Check page-level details using pageinspect (if extended for RECNO)
CREATE EXTENSION pageinspect;

-- Check relation size and page count
SELECT pg_relation_size('my_recno_table') as bytes,
       pg_relation_size('my_recno_table') / 8192 as pages;

-- Check FSM state
SELECT * FROM pg_freespace('my_recno_table');

-- Enable per-operation logging (already in code as elog WARNING)
-- The current code logs at WARNING level for insert/update/delete
-- To suppress, change elog(WARNING, ...) to elog(DEBUG1, ...) in:
--   recno_operations.c:66,136,296,575,630,651,656,659
```

### GDB Breakpoints for Debugging

```gdb
# Break on tuple insert
b recno_tuple_insert

# Break on in-place update decision
b recno_operations.c:407

# Break on cross-page update
b recno_operations.c:452

# Break on visibility check
b RecnoTupleVisible

# Break on WAL replay
b recno_redo

# Break on page initialization
b RecnoInitPage

# Break on timestamp generation
b RecnoGetCommitTimestamp
```


### 10. Hybrid Logical Clocks (recno_hlc.c)

RECNO implements HLC based on Kulkarni et al., 2014. The HLC timestamp packs
a 48-bit physical time (milliseconds since epoch) and a 16-bit logical counter
into a single uint64:

```
Bits 63-16: physical component (milliseconds)
Bits 15-0:  logical counter
```

**Core operations**:

- `HLCNow()` (recno_hlc.c:~320): Returns the current HLC timestamp. Acquires
  `RecnoHLCShmem->hlc_lock`, reads wall clock, advances the physical component
  if wall clock is ahead, otherwise increments the logical counter. For the
  "receive" variant, it also merges with a remote timestamp (max of local
  and remote physical, then advance logical).

- `HLCCompare()` (recno_hlc.c:~280): Compares two HLC timestamps. Returns
  -1, 0, or 1. First compares physical components; if equal, compares logical
  counters.

**DVV operations**:

- `DVVGetNext()` (recno_hlc.c:~420): Generates a new DVV dot. Encodes the
  12-bit node ID (from `recno_node_id` GUC) in the upper bits and a 48-bit
  monotonic counter in the lower bits. On x86_64 with invariant TSC, uses
  `rdtscp` + `cmpxchg` for lock-free generation. Falls back to
  `pg_atomic_fetch_add_u64` otherwise.

- `DVVCanPrune()` (recno_hlc.c:~490): Determines if a DVV dot is dominated
  by all active contexts, meaning the associated tuple version can be safely
  garbage-collected.

**TSC support** (recno_hlc.c:~150-250):
- TSC availability detected via CPUID leaf 0x80000007 bit 8 (invariant TSC)
- Calibration: performs a 10ms sleep, measures TSC ticks, computes ticks/ms
- Used for lock-free DVV counter generation, not for wall-clock time

**Uncertainty intervals**:
- `HLCGetUncertaintyInterval()` (recno_hlc.c:~550): Returns [commit_hlc -
  max_offset, commit_hlc + max_offset] using `recno_max_clock_offset_ms`
- `HLCInUncertaintyWindow()` (recno_hlc.c:~600): Checks if a timestamp
  falls within the uncertainty window around the current HLC time
- `RecnoFillHLCInfo()` (recno_hlc.c:~700): Populates an `xl_recno_hlc_info`
  struct for inclusion in WAL records when `RECNO_WAL_HAS_HLC` is set

**Shared memory state** (`RecnoHLCShmemData`):
- `hlc_lock` (LWLock): Protects HLC state
- `current_physical` / `current_logical`: Current HLC components
- `dvv_counter`: Global DVV counter (atomic on TSC-capable systems)
- `tsc_ticks_per_ms`: Calibrated TSC frequency
- `has_invariant_tsc`: Whether TSC is available

### 11. Custom TupleTableSlot (recno_slot.c)

RECNO uses a custom `RecnoTupleTableSlot` that extends PostgreSQL's
TupleTableSlot for native RECNO tuple handling without heap conversion.

**Slot structure** (recno_slot.c:~30):
```c
typedef struct RecnoTupleTableSlot
{
    TupleTableSlot base;        /* Standard TTS fields */
    RecnoTuple     tuple;       /* RECNO tuple pointer */
    uint32         tuple_len;   /* Tuple data length */
    uint32         off;         /* Deform offset (incremental) */
    Buffer         buffer;      /* Buffer pin (if buffer-pinned mode) */
} RecnoTupleTableSlot;
```

**Key operations** (`TTSOpsRecnoTuple`):
- `tts_recno_init()`: Initializes the slot, sets `off = 0`
- `tts_recno_release()`: Releases buffer pin if held
- `tts_recno_clear()`: Clears slot, releases buffer, resets deform state
- `tts_recno_getsomeattrs()`: Calls `tts_recno_deform()` to extract attributes
- `tts_recno_materialize()`: Copies tuple data into slot's memory context
  so the slot is independent of the buffer
- `tts_recno_copy_heap_tuple()`: Converts RECNO tuple to HeapTuple for
  callers that need heap format (e.g., index building)

**Deforming** (`tts_recno_deform()`, recno_slot.c:~200):
- Reads attributes sequentially from the tuple data area
- Handles null bitmap (RECNO_INFOMASK_HASNULL)
- Handles fixed-length, variable-length (varlena), and C-string attributes
- Uses incremental deforming: saves the byte offset (`off`) between calls
  so repeated `slot_getattr()` calls only decode new attributes
- Attribute alignment follows PostgreSQL conventions (TYPALIGN)

**Buffer management**:
- `RecnoSlotStoreTuple()` (recno_slot.c:~350): Stores a buffer-pinned tuple.
  Increments the buffer pin count so the slot holds a reference.
- `RecnoSlotStoreMaterializedTuple()` (recno_slot.c:~400): Stores a
  tuple that has already been copied into the slot's memory context.

### 12. Statistics Collection (recno_stats.c)

RECNO-specific statistics supplement the standard per-column statistics that
PostgreSQL collects via `scan_analyze_next_block`/`scan_analyze_next_tuple`.

**RecnoCollectRelationStats()** (recno_stats.c:49-225):
Performs a full sequential scan of every page in the relation (under shared
buffer locks) to measure:
- `total_pages`, `total_live_tuples`, `total_dead_tuples`
- `avg_tuple_size`: Mean live tuple size in bytes
- `pct_compressed`: Fraction of live tuples with RECNO_TUPLE_COMPRESSED
- `compression_ratio`: total_uncompressed / total_compressed (from
  RecnoCompressionHeader embedded after tuple header)
- `pct_overflow`: Fraction of live tuples with RECNO_TUPLE_HAS_OVERFLOW
- `avg_overflow_chain_len`: Mean overflow chains per overflow tuple
- `total_overflow_bytes`: Sum of all overflow record sizes
- `free_space_frac`: Average free space per page as fraction of BLCKSZ
- `bloat_factor`: (nblocks * BLCKSZ) / total_tuple_bytes
- `hlc_min`, `hlc_max`: Range of HLC timestamps (when `recno_use_hlc`)

**RecnoLogRelationStats()** (recno_stats.c:233-275):
Emits the collected statistics at a configurable log level (typically DEBUG1
during ANALYZE). Produces three ereport messages covering page/tuple counts,
compression/overflow percentages, and space efficiency metrics.

### 13. WAL Record HLC Extension (recno_xlog.h)

DML WAL records (insert, update, delete) can optionally carry HLC uncertainty
information via the `RECNO_WAL_HAS_HLC` flag in the record's `flags` field.

When set, an `xl_recno_hlc_info` structure (32 bytes) is appended after the
tuple data:

| Field | Size | Description |
|-------|------|-------------|
| `commit_hlc` | 8 bytes | Commit HLC timestamp |
| `commit_dvv` | 8 bytes | Commit DVV dot |
| `uncertainty_lower` | 8 bytes | Lower bound of uncertainty interval |
| `uncertainty_upper` | 8 bytes | Upper bound of uncertainty interval |

HLC-aware WAL logging functions (`RecnoXLogInsertHLC`, `RecnoXLogUpdateHLC`,
`RecnoXLogDeleteHLC`) accept a `const xl_recno_hlc_info *` parameter. When
non-NULL, they set the `RECNO_WAL_HAS_HLC` flag and append the info struct.

The `0x70` opcode (`XLOG_RECNO_CROSS_PAGE_DEFRAG`) supports cross-page tuple
moves during defragmentation, logging the source and destination offsets plus
the tuple data.


## Adding New Functionality

### Adding a New WAL Record Type

1. Define the record type code in `src/include/access/recno_xlog.h`
   (use the next available value in the 0x00-0x70 range)
2. Define the record data structure in `recno_xlog.h` (both backend and
   frontend versions if needed)
3. Add the logging function in `recno_xlog.c`
4. Add the REDO case in `recno_redo()` (recno_xlog.c:262)
5. Add description in `recno_desc()` and identification in `recno_identify()`
6. Test crash recovery with the new operation

### Adding a New Compression Algorithm

1. Add the type to `RecnoCompressionType` enum in `recno.h`
2. Add compress/decompress functions in `recno_compress.c`
3. Update `RecnoChooseCompressionType()` to select the new algorithm
4. Update `RecnoCompressAttribute()` switch statement
5. Update `RecnoDecompressAttribute()` switch statement
6. Add test coverage

### Adding a New Page Type

1. Define the flag in `recno.h` (e.g., `RECNO_PAGE_MYTYPE 0x0010`)
2. Define any page-type-specific structures
3. Initialize the page type in an init function
4. Add WAL logging for page creation/modification
5. Update `RecnoShouldDefragPage()` if defragmentation applies
6. Update VACUUM to handle the new page type
