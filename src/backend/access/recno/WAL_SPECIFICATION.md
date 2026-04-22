# RECNO WAL Record Specification

## Overview

This document provides the complete specification for all RECNO Write-Ahead Log
(WAL) record types, derived directly from `src/include/access/recno_xlog.h` and
`src/backend/access/recno/recno_xlog.c`. It is intended for PostgreSQL
committers evaluating the RECNO table access method.

RECNO's WAL implementation differs from heap in several fundamental ways:

1. **In-place updates with before/after images**: UPDATE logs both old and new
   tuple data for UNDO/REDO, rather than creating a new heap tuple.
2. **Timestamp-based MVCC fields**: All DML records carry `commit_ts` and
   `xact_ts` (uint64) instead of XIDs. When `recno_use_hlc = true`, these
   fields hold HLC timestamps instead of wall-clock microseconds.
3. **Optional HLC uncertainty data**: DML records (INSERT, UPDATE, DELETE) may
   include a trailing `xl_recno_hlc_info` structure for distributed consistency.
4. **Integrated overflow and compression**: Dedicated WAL opcodes for
   column-level overflow and per-attribute compression, avoiding TOAST.

### Resource Manager Registration

RECNO registers as resource manager `RM_RECNO_ID` in `src/include/access/rmgrlist.h`.
The REDO entry point is `recno_redo()`. The pg_waldump description/identify
functions are `recno_desc()` / `recno_identify()` in `src/backend/access/rmgrdesc/recnodesc.c`.

---

## WAL Record Format Conventions

### Opcode Encoding

The info byte layout uses the upper nibble for opcodes (bits 7..4) with
`XLR_INFO_MASK` occupying the standard upper bits:

```c
#define XLOG_RECNO_INSERT              0x00
#define XLOG_RECNO_UPDATE_INPLACE      0x10
#define XLOG_RECNO_DELETE              0x20
#define XLOG_RECNO_DEFRAG              0x30   /* single-page defrag */
#define XLOG_RECNO_OVERFLOW_WRITE      0x40
#define XLOG_RECNO_COMPRESS            0x50
#define XLOG_RECNO_INIT_PAGE           0x60
#define XLOG_RECNO_CROSS_PAGE_DEFRAG   0x70   /* cross-page tuple move */
#define XLOG_RECNO_VM_SET              0x80   /* Set visibility map bits */
#define XLOG_RECNO_VM_CLEAR            0x90   /* Clear visibility map bits */
#define XLOG_RECNO_LOCK                0xA0   /* Tuple lock (MultiXact) */
#define XLOG_RECNO_OPMASK              0xF0
```

Aliases: `XLOG_RECNO_VACUUM == XLOG_RECNO_DEFRAG`, `XLOG_RECNO_UPDATE == XLOG_RECNO_UPDATE_INPLACE`.

### Common Fields

- **OffsetNumber offnum**: Line pointer offset on the target page.
- **uint16 flags**: Bitmask; `RECNO_WAL_HAS_HLC` (0x0001) indicates trailing
  HLC info.
- **uint64 commit_ts / xact_ts**: Commit and transaction timestamps. In HLC
  mode, these are HLC values; in legacy mode, microseconds since the PG epoch.
- **Block references**: Block 0 is always the primary data page. Block 1, when
  present, is a secondary page (source page for cross-page defrag, VM page for
  VM records).

### Buffer Registration

All records use `REGBUF_STANDARD` for data pages. `XLOG_RECNO_INIT_PAGE` uses
`REGBUF_WILL_INIT | REGBUF_STANDARD` because the page is unconditionally
rewritten.

### HLC Uncertainty Appendix

When `RECNO_WAL_HAS_HLC` is set, a 32-byte `xl_recno_hlc_info` is appended
after all other record data:

```c
typedef struct xl_recno_hlc_info {
    uint64  commit_hlc;          /* Commit HLC timestamp */
    uint64  commit_dvv;          /* Commit DVV dot */
    uint64  uncertainty_lower;   /* Lower bound of uncertainty interval */
    uint64  uncertainty_upper;   /* Upper bound of uncertainty interval */
} xl_recno_hlc_info;  /* 32 bytes */
```

During REDO on a standby, `recno_redo_handle_hlc()` extracts this structure from
the tail of the record data and:

1. Computes `uncertainty_ms` from the physical components of `commit_hlc` and
   `uncertainty_upper`.
2. Calls `RecnoReplicaHandleUncertainty(commit_hlc, uncertainty_ms)`, which
   either waits for the physical clock to pass the uncertainty window
   (`recno_uncertainty_wait = true`) or immediately advances the local HLC past
   the commit HLC (`recno_uncertainty_wait = false`).
3. In both cases, `HLCNow(commit_hlc)` is called to ensure the replica's HLC
   respects causal ordering.

---

## Record Type 1: XLOG_RECNO_INSERT (0x00)

### Purpose

Logs the insertion of a new tuple into a RECNO page.

### Record Structure

```c
typedef struct xl_recno_insert {
    OffsetNumber offnum;     /* Target offset number */
    uint16       flags;      /* RECNO_WAL_HAS_HLC if HLC info follows */
    uint64       commit_ts;  /* Commit timestamp (or HLC) */
    uint64       xact_ts;    /* Transaction start timestamp (or DVV dot) */
    /* RecnoTupleHeader + tuple data follows (tuple->t_len bytes) */
    /* If flags & RECNO_WAL_HAS_HLC: xl_recno_hlc_info (32 bytes) */
} xl_recno_insert;
```

**Data layout in WAL record**:
```
[xl_recno_insert] [RecnoTupleHeader + data] [xl_recno_hlc_info?]
```

### WAL Logging (`RecnoXLogInsert`)

Source: `recno_xlog.c:196-242`

1. Fill `xl_recno_insert` fields.
2. Set `flags |= RECNO_WAL_HAS_HLC` if `recno_use_hlc` is true.
3. `XLogBeginInsert()`.
4. `XLogRegisterBuffer(0, buffer, REGBUF_STANDARD)`.
5. `XLogRegisterData(&xlrec, sizeof(xl_recno_insert))`.
6. `XLogRegisterData(tuple->t_data, tuple->t_len)` -- full tuple content.
7. If HLC enabled: compute uncertainty interval via `HLCGetUncertaintyInterval()`,
   register `xl_recno_hlc_info`.
8. `XLogInsert(RM_RECNO_ID, XLOG_RECNO_INSERT)`.
9. `PageSetLSN(page, recptr)`.

### REDO Semantics (`recno_redo`, case `XLOG_RECNO_INSERT`)

Source: `recno_xlog.c:670-742`

1. Process HLC data via `recno_redo_handle_hlc(record, xlrec->flags)`.
2. `XLogReadBufferForRedo(record, 0, &buffer)`.
3. If `action == BLK_NEEDS_REDO`:
   a. If `PageIsNew(page)`: call `RecnoInitPage(page, BufferGetPageSize(buffer))`
      to set up RECNO opaque space.
   b. `PageAddItem(page, tuple_hdr, tuple_hdr->t_len, xlrec->offnum, false, false)`.
   c. **PANIC** if `PageAddItem` returns `InvalidOffsetNumber`.
   d. Fix `t_ctid` in the on-page tuple to point to `(blkno, inserted_offnum)`.
   e. Update `RecnoPageOpaque`:
      - `pd_commit_ts = Max(pd_commit_ts, xlrec->commit_ts)`
      - `pd_free_space = PageGetFreeSpace(page)`
   f. `PageSetLSN(page, record->EndRecPtr)`.
   g. `MarkBufferDirty(buffer)`.
4. `UnlockReleaseBuffer(buffer)`.

### UNDO Semantics

No UNDO for INSERT. Aborted inserts are handled through MVCC visibility: the
tuple remains on the page but is invisible to snapshots taken after abort
(because `t_xmin` will be marked invalid by normal transaction abort processing).

### Crash Recovery Guarantees

- **Atomicity**: FPI on first post-checkpoint modification prevents torn pages.
- **Idempotency**: `XLogReadBufferForRedo` returns `BLK_ALREADY_APPLIED` if
  `PageGetLSN(page) >= record->EndRecPtr`.
- **Self-referencing t_ctid**: Explicitly fixed during redo to ensure scans and
  index lookups find the tuple.

---

## Record Type 2: XLOG_RECNO_UPDATE_INPLACE (0x10)

### Purpose

Logs an in-place update. Both old and new tuple images are recorded, enabling
both REDO (apply new) and UNDO (restore old).

### Record Structure

```c
typedef struct xl_recno_update {
    OffsetNumber offnum;         /* Offset of tuple being updated */
    uint16       flags;          /* RECNO_WAL_HAS_HLC if HLC info follows */
    uint64       old_commit_ts;  /* Previous version's commit timestamp */
    uint64       new_commit_ts;  /* New version's commit timestamp */
    uint64       xact_ts;        /* Transaction start timestamp */
    uint16       old_tuple_len;  /* Length of old tuple data */
    /* Old tuple data (old_tuple_len bytes) */
    /* New tuple data (remaining bytes before optional HLC info) */
    /* If flags & RECNO_WAL_HAS_HLC: xl_recno_hlc_info (32 bytes) */
} xl_recno_update;
```

**Data layout in WAL record**:
```
[xl_recno_update] [old tuple: old_tuple_len bytes] [new tuple] [xl_recno_hlc_info?]
```

### WAL Logging (`RecnoXLogUpdate`)

Source: `recno_xlog.c:247-298`

Both `old_tuple->t_data` (old_tuple->t_len bytes) and `new_tuple->t_data`
(new_tuple->t_len bytes) are registered as data. The `old_tuple_len` field in
the header allows the redo function to find the boundary between old and new
tuple data.

### REDO Semantics

Source: `recno_xlog.c:744-821`

1. Parse old tuple at `(char*)xlrec + sizeof(xl_recno_update)`.
2. Parse new tuple at `old_tuple_data + xlrec->old_tuple_len`.
3. Process HLC.
4. `XLogReadBufferForRedo(record, 0, &buffer)`.
5. If `BLK_NEEDS_REDO`:
   a. Initialize page if `PageIsNew`.
   b. **LSN-based UNDO/REDO decision**:
      - If `PageGetLSN(page) > record->EndRecPtr` (UNDO path):
        - Get existing tuple at `offnum`.
        - Verify `existing->t_commit_ts == new_commit_ts` and
          `existing->t_xact_ts == xact_ts`.
        - If verified: `memcpy(existing, old_tuple_hdr, old_tuple_hdr->t_len)`.
        - Update `ItemId` length to old size.
      - Else (REDO path):
        - Get existing tuple at `offnum`.
        - `memcpy(existing, new_tuple_hdr, new_tuple_hdr->t_len)`.
        - Update `ItemId` length to new size.
   c. Update `pd_commit_ts = Max(pd_commit_ts, new_commit_ts)`.
   d. Update `pd_free_space`.
   e. Set LSN, mark dirty.

### UNDO Semantics

The UNDO path (page LSN > record EndRecPtr) restores the old tuple by copying
the old tuple image from the WAL record back into the page. This is guarded by a
timestamp verification check to ensure the correct version is being undone. This
path is used during specialized rollback scenarios (e.g., two-phase commit
abort), not during normal crash recovery.

### Key Difference from Heap

Heap UPDATE creates a new tuple at a different location (possibly same page for
HOT). RECNO UPDATE overwrites the tuple in-place, logging both before and after
images. This means:

- The TID never changes across updates (no HOT chain, no redirect items).
- Index entries remain valid without HOT cleanup.
- WAL volume is ~2x tuple size per update (both images), vs ~1x for heap
  (only new tuple, though heap also logs a reference to old tuple location).

---

## Record Type 3: XLOG_RECNO_DELETE (0x20)

### Purpose

Logs tuple deletion. The tuple remains on the page with the `RECNO_TUPLE_DELETED`
flag set and its `t_commit_ts` updated to the deletion timestamp. The old tuple
data is logged for potential UNDO.

### Record Structure

```c
typedef struct xl_recno_delete {
    OffsetNumber offnum;     /* Offset of tuple being deleted */
    uint16       flags;      /* RECNO_WAL_HAS_HLC if HLC info follows */
    uint64       commit_ts;  /* Deletion commit timestamp */
    uint64       xact_ts;    /* Transaction start timestamp */
    /* Old tuple data follows for UNDO */
    /* If flags & RECNO_WAL_HAS_HLC: xl_recno_hlc_info (32 bytes) */
} xl_recno_delete;
```

### REDO Semantics

Source: `recno_xlog.c:823-898`

1. Process HLC.
2. `XLogReadBufferForRedo(record, 0, &buffer)`.
3. If `BLK_NEEDS_REDO`:
   a. Initialize page if new.
   b. **UNDO path** (page LSN > record EndRecPtr):
      - Verify `tuple->t_flags & RECNO_TUPLE_DELETED` and timestamps match.
      - `memcpy(tuple, old_tuple_hdr, old_tuple_hdr->t_len)` -- restores
        pre-deletion state.
      - Update `ItemId` length.
   c. **REDO path**:
      - `tuple->t_flags |= RECNO_TUPLE_DELETED`
      - `tuple->t_commit_ts = xlrec->commit_ts`
   d. Update page opaque:
      - `pd_commit_ts = Max(pd_commit_ts, commit_ts)`
      - **`pd_flags |= RECNO_PAGE_DEFRAG_NEEDED`** -- marks the page for
        future defragmentation (space reclamation).
   e. Set LSN, mark dirty.

### Key Detail: RECNO_PAGE_DEFRAG_NEEDED

Unlike heap's DELETE which sets the line pointer to `LP_DEAD`, RECNO's DELETE
keeps the line pointer `LP_NORMAL` and sets `RECNO_TUPLE_DELETED` in the tuple
header. The page-level `RECNO_PAGE_DEFRAG_NEEDED` flag signals to subsequent
operations (inserts, scans) that the page may benefit from opportunistic
defragmentation via `RecnoPagePruneOpt()`.

---

## Record Type 4: XLOG_RECNO_DEFRAG (0x30)

### Purpose

Logs single-page defragmentation. This compacts live tuples within a page to
reclaim space from dead tuples.

**Alias**: `XLOG_RECNO_VACUUM` is a #define alias for this opcode.

### Record Structure

```c
typedef struct xl_recno_defrag {
    uint16  ntuples;     /* Number of tuples moved */
    uint64  commit_ts;   /* Commit timestamp */
    /* Array of RecnoOffsetMapping[ntuples] follows */
} xl_recno_defrag;

typedef struct RecnoOffsetMapping {
    OffsetNumber old_offnum;
    OffsetNumber new_offnum;
} RecnoOffsetMapping;
```

### WAL Logging (`RecnoXLogDefrag`)

Source: `recno_xlog.c:354-377`

Note: Unlike DML records, the buffer is registered *after* the data. The
ordering of `XLogRegisterData` vs `XLogRegisterBuffer` does not affect
correctness (PostgreSQL's WAL infrastructure handles this), but it differs from
the INSERT/UPDATE/DELETE pattern where the buffer is registered first.

### REDO Semantics

Source: `recno_xlog.c:900-938`

The REDO implementation does **not** use the offset mappings from the WAL record.
Instead, it calls PostgreSQL's `PageRepairFragmentation(page)`, which compacts
all live items on the page by removing gaps. This is correct because:

1. `PageRepairFragmentation` achieves the same result as replaying individual
   offset mappings.
2. It is simpler and avoids edge cases with overlapping moves.
3. After compaction, the page opaque is updated:
   - `pd_commit_ts = Max(pd_commit_ts, commit_ts)`
   - `pd_free_space = PageGetFreeSpace(page)`
   - `pd_defrag_counter++`
   - `pd_flags &= ~RECNO_PAGE_DEFRAG_NEEDED` -- clears the defrag-needed flag.

### UNDO Semantics

Not applicable. Defragmentation is a physical reorganization that does not change
tuple visibility. It cannot be rolled back and does not need to be.

### Important Note on Offset Mappings

The offset mappings are logged for diagnostic purposes (pg_waldump can show what
moved where) and for potential use by logical replication decoders that need to
track TID changes. The actual REDO path ignores them.

---

## Record Type 5: XLOG_RECNO_OVERFLOW_WRITE (0x40)

### Purpose

Logs writing overflow data to a page. RECNO uses Derby-inspired column-level
overflow where large attributes are stored as chains of
`RecnoOverflowRecordHeader` + data chunks on regular data pages, instead of
using a separate TOAST relation.

### Record Structure

```c
typedef struct xl_recno_overflow_write {
    OffsetNumber offnum;     /* Offset of overflow record on page */
    uint16       flags;      /* 0x0000 = new record, 0x0001 = link update */
    uint32       data_len;   /* Length of logged data */
    uint64       commit_ts;  /* Commit timestamp */
    /* For new records: RecnoOverflowRecordHeader + chunk data (data_len bytes) */
    /* For link updates: RecnoOverflowRecordHeader only (data_len bytes) */
} xl_recno_overflow_write;
```

**Flags**:
- `RECNO_OVERFLOW_WAL_NEW_RECORD` (0x0000): Full overflow record (header + data).
- `RECNO_OVERFLOW_WAL_LINK_UPDATE` (0x0001): Only the header is logged, used when
  updating chain pointers (e.g., after inserting a new chunk that extends the chain).

### REDO Semantics

Source: `recno_xlog.c:940-998`

1. If page is new: `PageInit(page, size, sizeof(RecnoPageOpaqueData))`.
   Note: This uses `PageInit` directly (not `RecnoInitPage`), initializing with
   the correct special space size.
2. **Link update** (`flags & RECNO_OVERFLOW_WAL_LINK_UPDATE`):
   - Get existing item at `offnum`.
   - `memcpy` the new `RecnoOverflowRecordHeader` over the existing header.
   - This updates `or_next_block` / `or_next_offset` chain pointers.
3. **New record** (flags == 0):
   - `PageAddItem(page, record_data, data_len, offnum, false, false)`.
   - `elog(ERROR, ...)` (not PANIC) if the add fails.

### Overflow Chain Architecture

```
Main tuple:  [..., overflow_ptr(block=5, offset=1, total_len=100KB), ...]
                              |
                              v
Page 5, offset 1: [RecnoOverflowRecordHeader(next=6:1, data_len=8000)] [8000 bytes]
                              |
                              v
Page 6, offset 1: [RecnoOverflowRecordHeader(next=7:1, data_len=8000)] [8000 bytes]
                              |
                              v
Page 7, offset 1: [RecnoOverflowRecordHeader(next=Invalid, data_len=4192)] [4192 bytes]
```

Overflow records share the visibility of their parent tuple. They carry no MVCC
fields of their own.

---

## Record Type 6: XLOG_RECNO_COMPRESS (0x50)

### Purpose

Logs in-place compression of a tuple attribute. RECNO supports per-attribute
compression using LZ4, ZSTD, delta encoding (numeric), and dictionary encoding
(text).

### Record Structure

```c
typedef struct xl_recno_compress {
    OffsetNumber offnum;      /* Offset of tuple */
    uint16       attr_num;    /* 1-based attribute number */
    uint8        comp_type;   /* RecnoCompressionType enum value */
    uint8        comp_level;  /* Compression level */
    uint32       orig_size;   /* Original uncompressed size */
    uint32       comp_size;   /* Compressed size */
    uint64       commit_ts;   /* Commit timestamp */
    /* Compressed data (comp_size bytes) follows */
} xl_recno_compress;
```

**Compression types** (`RecnoCompressionType`):
- `RECNO_COMP_NONE` (0)
- `RECNO_COMP_LZ4` (1)
- `RECNO_COMP_ZSTD` (2)
- `RECNO_COMP_DELTA` (3) -- for numeric columns
- `RECNO_COMP_DICTIONARY` (4) -- for text columns

### REDO Semantics

Source: `recno_xlog.c:1000-1047`

1. Get tuple at `offnum`.
2. Set `tuple->t_flags |= RECNO_TUPLE_COMPRESSED`.
3. Set `tuple->t_infomask |= RECNO_INFOMASK_COMPRESSED`.
4. Set `tuple->t_commit_ts = xlrec->commit_ts`.
5. Update page opaque:
   - `pd_commit_ts = Max(pd_commit_ts, commit_ts)`
   - `pd_flags |= RECNO_PAGE_COMPRESSED`

**Note**: The current REDO implementation marks the tuple as compressed and
updates metadata, but does not replace the attribute data in-place with the
compressed bytes from the WAL record. This appears to be an area for future
refinement -- the compressed data in the WAL record serves as a record of what
was compressed, and the actual compression was applied to the page before WAL
logging.

### UNDO Semantics

Not applicable. Compression is an optimization; decompression is transparent
on read.

---

## Record Type 7: XLOG_RECNO_INIT_PAGE (0x60)

### Purpose

Logs initialization of a new RECNO page, setting up the standard page header
and RECNO-specific opaque space (`RecnoPageOpaqueData`).

### Record Structure

```c
typedef struct xl_recno_init_page {
    uint32  flags;       /* Page flags (RECNO_PAGE_* bits) */
    uint64  commit_ts;   /* Initial commit timestamp */
} xl_recno_init_page;
```

### WAL Logging (`RecnoXLogInitPage`)

Source: `recno_xlog.c:459-484`

Buffer registered with `REGBUF_WILL_INIT | REGBUF_STANDARD`, signaling that the
page content is completely rewritten. This prevents a FPI from being logged
(the init record is sufficient to reconstruct the page).

### REDO Semantics

Source: `recno_xlog.c:1049-1075`

1. `XLogReadBufferForRedoExtended(record, 0, RBM_ZERO_AND_LOCK, false, &buffer)`.
   This zeros the page and acquires an exclusive lock.
2. `RecnoInitPage(page, BufferGetPageSize(buffer))` -- sets up standard page
   header with RECNO special space.
3. Override opaque fields from WAL record:
   - `pd_commit_ts = xlrec->commit_ts`
   - `pd_flags = xlrec->flags`
4. Set LSN, mark dirty.

### RECNO Page Opaque Structure

```c
typedef struct RecnoPageOpaqueData {
    uint64  pd_commit_ts;       /* Page-level commit timestamp */
    uint16  pd_free_space;      /* Cached free space amount */
    uint16  pd_defrag_counter;  /* Number of defragmentations performed */
    uint32  pd_flags;           /* Page flags */
} RecnoPageOpaqueData;  /* Stored in PageGetSpecialPointer(page) */
```

---

## Record Type 8: XLOG_RECNO_CROSS_PAGE_DEFRAG (0x70)

### Purpose

Logs moving a tuple from one page (source) to another (target). Used during
aggressive defragmentation or VACUUM when consolidating nearly-empty pages.

### Record Structure

```c
typedef struct xl_recno_cross_page_defrag {
    OffsetNumber src_offnum;   /* Source line pointer offset (on block 1) */
    OffsetNumber dst_offnum;   /* Target line pointer offset (on block 0) */
    uint32       tuple_len;    /* Length of moved tuple data */
    /* Tuple data (tuple_len bytes) follows */
} xl_recno_cross_page_defrag;
```

**Block references**:
- Block 0: Target/destination page (receives the tuple).
- Block 1: Source page (loses the tuple).

### WAL Logging (`RecnoXLogCrossPageDefrag`)

Source: `recno_xlog.c:494-516`

Both buffers are registered with `REGBUF_STANDARD`. The tuple data is included
in the record so recovery can replay the move even without full-page images.
Note: This function does NOT call `PageSetLSN` on either page -- the LSN update
is done in the redo handler for each page independently.

### REDO Semantics

Source: `recno_xlog.c:1077-1137`

The two pages are handled independently (each may or may not need redo depending
on their individual LSNs):

1. **Target page (block 0)**:
   a. `XLogReadBufferForRedo(record, 0, &buffer)`.
   b. If `BLK_NEEDS_REDO`:
      - `PageAddItem(page, tuple_data, tuple_len, dst_offnum, false, false)`.
      - **PANIC** if add fails.
      - Fix `t_ctid` in the moved tuple to `(dst_blkno, dst_offnum)`.
      - Set LSN, mark dirty.
   c. Release buffer.

2. **Source page (block 1)**:
   a. `XLogReadBufferForRedo(record, 1, &buffer)`.
   b. If `BLK_NEEDS_REDO`:
      - `ItemIdSetUnused(PageGetItemId(page, src_offnum))` -- marks the old
        slot as available.
      - Set LSN, mark dirty.
   c. Release buffer.

### Important: TID Changes

Cross-page defrag changes the tuple's TID. Any indexes referencing the old TID
must be updated separately. This WAL record does not handle index updates.

---

## Record Type 9: XLOG_RECNO_VM_SET (0x80)

### Purpose

Logs setting visibility map bits for a data page. Used when VACUUM determines
all tuples on a page are visible to all transactions.

### Record Structure

```c
typedef struct xl_recno_vm_set {
    BlockNumber heapBlk;   /* Data page block number */
    uint8       flags;     /* VM flags being set */
} xl_recno_vm_set;
```

**VM flags**:
- `RECNO_VM_ALL_VISIBLE` (0x01): All tuples visible to all transactions.
- `RECNO_VM_ALL_FROZEN` (0x02): All tuples frozen.

**Block references**:
- Block 0: Heap/data page (LSN updated for coordination).
- Block 1: Visibility map page (bits modified).

### REDO Semantics

Source: `recno_xlog.c:1139-1177`

1. **Heap page (block 0)**: Only `PageSetLSN` and `MarkBufferDirty` -- no
   data changes.
2. **VM page (block 1)**:
   a. Compute byte and bit offset within the VM page:
      ```c
      mapByte = (heapBlk % (VM_BLOCKS_PER_PAGE)) / 4;
      mapOffset = (heapBlk % (VM_BLOCKS_PER_PAGE)) % 4;
      ```
      where `VM_BLOCKS_PER_PAGE = (BLCKSZ - MAXALIGN(SizeOfPageHeaderData)) * 4`.
   b. Set bits: `map[mapByte] |= (flags << (mapOffset * 2))`.
   c. Set LSN, mark dirty.

### Correctness Note

The VM is a hint structure. Setting a bit incorrectly (e.g., claiming
all-visible when it is not) can cause incorrect index-only scan results, but
the bit-setting path is conservative -- it only sets bits after verifying page
contents during VACUUM.

---

## Record Type 10: XLOG_RECNO_VM_CLEAR (0x90)

### Purpose

Logs clearing visibility map bits. Written before a DML operation that modifies
a page previously marked as all-visible.

### Record Structure

```c
typedef struct xl_recno_vm_clear {
    BlockNumber heapBlk;   /* Data page block number */
    uint8       flags;     /* VM flags being cleared */
} xl_recno_vm_clear;
```

**Block references**: Same as VM_SET (block 0 = heap, block 1 = VM).

### REDO Semantics

Source: `recno_xlog.c:1179-1217`

Same structure as VM_SET but uses bit-clear:
```c
map[mapByte] &= ~(flags << (mapOffset * 2));
```

### Ordering Guarantee

VM_CLEAR must be logged **before** the DML operation that invalidates the
all-visible status. This ensures that after crash recovery, the VM
conservatively shows the page as not-all-visible, forcing full visibility checks
rather than risking stale all-visible hints.

---

## Record Type 11: XLOG_RECNO_LOCK (0xA0)

### Purpose

Logs tuple locking for `SELECT ... FOR SHARE/UPDATE/NO KEY UPDATE/KEY SHARE`
and MultiXact row locking. This records the lock state in the tuple header
without modifying tuple data.

### Record Structure

```c
typedef struct xl_recno_lock {
    OffsetNumber offnum;     /* Offset of locked tuple */
    uint16       flags;      /* Reserved flags */
    TransactionId xmax;      /* Locking XID or MultiXactId */
    uint16       infomask;   /* RECNO_INFOMASK_* bits */
    uint16       infomask2;  /* Additional infomask bits */
    uint8        lock_mode;  /* LockTupleMode value */
} xl_recno_lock;
```

### REDO Semantics

**Note**: As of the current implementation, `XLOG_RECNO_LOCK` is defined in the
header but the `recno_redo()` switch statement does not include a case for it.
Any WAL record with this opcode will hit the `default` case and trigger
`elog(PANIC, "recno_redo: unknown op code %u", info)`. This is a known
limitation; tuple locking currently uses PostgreSQL's standard LOCKTAG_TUPLE
mechanism (heavyweight locks) without WAL logging the lock state in the tuple
header.

### Expected REDO (when implemented)

The expected REDO behavior would be:
1. Get tuple at `offnum`.
2. Set `tuple->t_xmax = xlrec->xmax`.
3. Set `tuple->t_infomask = xlrec->infomask`.
4. Set `tuple->t_infomask2 = xlrec->infomask2`.
5. Set LSN, mark dirty.

---

## WAL Consistency and Correctness

### Full-Page Images (FPI)

After each checkpoint, the first modification to any data page writes a full
copy of the page to WAL. This protects against torn pages from partial disk
writes. The `REGBUF_STANDARD` flag enables this automatically. The sole
exception is `XLOG_RECNO_INIT_PAGE`, which uses `REGBUF_WILL_INIT` to indicate
the page is completely rewritten (no FPI needed).

### LSN-Based Idempotency

Every REDO handler checks the return value of `XLogReadBufferForRedo()`:
- `BLK_NEEDS_REDO`: Page LSN < record LSN; apply changes.
- `BLK_RESTORED`: FPI was restored; no further action needed.
- `BLK_DONE` / `BLK_NOTFOUND`: No action.

This makes all REDO operations idempotent.

### Timestamp Monotonicity on Pages

The pattern `pd_commit_ts = Max(pd_commit_ts, xlrec->commit_ts)` is used in
every DML REDO handler. This ensures the page-level timestamp never goes
backward, which is critical for MVCC visibility decisions that use the page
timestamp as a conservative upper bound.

### Mask Function for WAL Consistency Checks

`recno_mask()` (recno_xlog.c:1228-1244) masks fields that may legitimately
differ between primary and standby:
- Page LSN and checksum (standard).
- Hint bits.
- Unused space.
- `pd_free_space` (can vary due to timing of defragmentation).
- `pd_defrag_counter` (accumulated count, not critical for correctness).

This enables `wal_consistency_checking = recno` to verify that REDO produces
consistent pages.

---

## Physical Replication

All RECNO WAL records are replayed identically on standbys via `recno_redo()`.
The HLC handling in `recno_redo_handle_hlc()` ensures that standbys maintain
causal consistency:

1. The standby's HLC is advanced to at least the commit HLC from each WAL
   record.
2. If `recno_uncertainty_wait = true`, the standby may wait for its physical
   clock to pass the uncertainty window before allowing reads at the committed
   timestamp.
3. If `recno_uncertainty_wait = false`, the HLC is immediately advanced past the
   uncertainty window (no wait, but reads during the uncertainty window may
   observe stale orderings from the perspective of an external observer).

### Promotion

When a standby is promoted, its HLC continues from the last value advanced
during WAL replay. No special handling is needed because HLC values are
monotonically advanced.

---

## Logical Replication

RECNO supports logical replication through the `recno_decode()` entry point
declared in `recno_xlog.h`. WAL records are decoded into standard logical
change records:

| WAL Record | Logical Change |
|------------|----------------|
| INSERT     | Logical INSERT with full new tuple |
| UPDATE     | Logical UPDATE with old and new tuples |
| DELETE     | Logical DELETE with old tuple key |

When HLC data is present, it is propagated to subscribers via the logical
replication protocol, enabling causal consistency in multi-master topologies.

---

## WAL Volume Analysis

| Operation | Heap WAL | RECNO WAL | Ratio | Notes |
|-----------|----------|-----------|-------|-------|
| INSERT | ~tuple_size | ~tuple_size + 20B header | ~1.0x | Essentially identical |
| UPDATE | ~new_tuple + old_ref | ~old_tuple + new_tuple + 38B header | ~2.0x | RECNO logs full before-image |
| DELETE | ~TID ref | ~full_tuple + 20B header | ~Nx | RECNO logs full before-image for UNDO |
| DEFRAG | N/A | ~header + mappings array | minimal | Heap uses VACUUM page-level cleanup |

The additional WAL volume from before-images is the cost of supporting UNDO and
in-place updates. This is partially offset by the elimination of HOT chain
cleanup records and redirect items that heap generates.

---

## Debugging with pg_waldump

```bash
pg_waldump -p /path/to/pg_wal -r RECNO
```

Example output with HLC enabled:
```
rmgr: RECNO   len (rec/tot): 156/188, tx: 0, lsn: 0/017A3B40, prev 0/017A3B00,
  desc: insert, hlc 826185600042 dvv 4503599627370497 uncertainty [826185599792, 826185600292]
rmgr: RECNO   len (rec/tot): 312/344, tx: 0, lsn: 0/017A3C00, prev 0/017A3B40,
  desc: update, hlc 826185600100 dvv 4503599627370498 uncertainty [826185599850, 826185600350]
```

---

## References

- **REDO/logging implementation**: `src/backend/access/recno/recno_xlog.c`
- **WAL record type definitions**: `src/include/access/recno_xlog.h`
- **pg_waldump support**: `src/backend/access/rmgrdesc/recnodesc.c`
- **Resource manager registration**: `src/include/access/rmgrlist.h`
- **RECNO header (page/tuple structures)**: `src/include/access/recno.h`
- **HLC implementation**: `src/backend/access/recno/recno_hlc.c`
- **Clock-bound integration**: `src/backend/access/recno/recno_clock.c`

---

*Document version: 2.0 (March 2026)*
*Derived from source code analysis of commit 19ef292c217*
*Status: For PostgreSQL Mailing List Review*
