# Noxu Free Pages Map and Metapage Format

This document provides comprehensive design documentation for two core
subsystems of the Noxu columnar table access method: the **Free Pages
Map (FPM)** for space management, and the **Metapage** that stores
relation-level metadata.

## Table of Contents

1. [Metapage Format](#1-metapage-format)
2. [Free Pages Map Design](#2-free-pages-map-design)
3. [Interaction Between Metapage and FPM](#3-interaction-between-metapage-and-fpm)
4. [WAL Support](#4-wal-support)
5. [Future Directions](#5-future-directions)

---

## 1. Metapage Format

### 1.1 Overview

Every Noxu relation begins with a metapage at **block 0**
(`NX_META_BLK`).  The metapage serves as the root of all metadata
for the relation: it contains a directory of B-tree root block numbers
(one per attribute), the head of the Free Page Map linked list, and
(deprecated) UNDO log pointers that are retained for structural
compatibility.

Source: `src/backend/access/noxu/noxu_meta.c`
Header: `src/include/access/noxu_internal.h`

### 1.2 Page Layout

The metapage uses the standard PostgreSQL `PageHeaderData` prefix
(24 bytes) with two distinct regions:

- **Page body** (`PageGetContents`): Contains `NXMetaPage`, which holds
  the attribute count and root directory.
- **Special area** (`PageGetSpecialPointer`): Contains `NXMetaPageOpaque`,
  which holds the FPM head, deprecated UNDO pointers, and the page type
  identifier.

```
 Byte offset   Contents
 ───────────   ────────────────────────────────────────
 0             PageHeaderData (24 bytes)
               ┌─ pd_lsn, pd_checksum, pd_lower,
               │  pd_upper, pd_special, pd_flags,
               └─ pd_pagesize_version
 24            NXMetaPage  ◄── PageGetContents(page)
               ┌──────────────────────────────────────┐
               │ int32  nattributes                    │
               ├──────────────────────────────────────┤
               │ NXRootDirItem  tree_root_dir[0]       │  ◄── TID tree root
               │ NXRootDirItem  tree_root_dir[1]       │  ◄── Column 1 root
               │ NXRootDirItem  tree_root_dir[2]       │  ◄── Column 2 root
               │ ...                                   │
               │ NXRootDirItem  tree_root_dir[N-1]     │
               └──────────────────────────────────────┘
               (free space)
 pd_special    NXMetaPageOpaque  ◄── PageGetSpecialPointer(page)
               ┌──────────────────────────────────────┐
               │ BlockNumber   nx_undo_head       [D]  │
               │ BlockNumber   nx_undo_tail       [D]  │
               │ uint64        nx_undo_tail_first_counter [D]│
               │ RelUndoRecPtr nx_undo_oldestptr  [D]  │
               │ BlockNumber   nx_fpm_head             │
               │ uint16        nx_flags                 │
               │ uint16        nx_page_id (0xF083)      │
               └──────────────────────────────────────┘

 [D] = Deprecated. Retained for structural compatibility; initialized
       to zero on new relations. Per-relation UNDO is now handled by
       the RelUndo subsystem in a separate UNDO fork.
```

### 1.3 Data Structures

**NXRootDirItem** -- Single entry in the root directory:

```c
typedef struct NXRootDirItem
{
    BlockNumber root;       /* B-tree root block, or InvalidBlockNumber */
} NXRootDirItem;
```

**NXMetaPage** -- Page body contents:

```c
typedef struct NXMetaPage
{
    int         nattributes;
    NXRootDirItem tree_root_dir[FLEXIBLE_ARRAY_MEMBER];
} NXMetaPage;
```

- `nattributes`: Number of B-trees tracked.  This equals the number of
  user-visible columns plus 1 (for the TID tree at index 0).
- `tree_root_dir[0]`: Root of the TID tree (`NX_META_ATTRIBUTE_NUM = 0`).
- `tree_root_dir[1..N]`: Roots of per-column attribute B-trees.

**NXMetaPageOpaque** -- Special area:

```c
typedef struct NXMetaPageOpaque
{
    BlockNumber   nx_undo_head;                 /* [deprecated] */
    BlockNumber   nx_undo_tail;                 /* [deprecated] */
    uint64        nx_undo_tail_first_counter;   /* [deprecated] */
    RelUndoRecPtr nx_undo_oldestptr;            /* [deprecated] */
    BlockNumber   nx_fpm_head;    /* Head of the FPM linked list */
    uint16        nx_flags;
    uint16        nx_page_id;     /* Always NX_META_PAGE_ID (0xF083) */
} NXMetaPageOpaque;
```

### 1.4 Root Directory Capacity and Overflow

The root directory is a variable-length array embedded in the metapage
body.  Its size is limited by the space between `pd_lower` (end of
body) and `pd_upper` (start of free space / special area):

```
max_attributes = (pd_upper - SizeOfPageHeaderData) / sizeof(NXRootDirItem)
```

With an 8 KB page, `sizeof(NXMetaPageOpaque)` ~ 50 bytes, and
`sizeof(NXRootDirItem)` = 4 bytes, the theoretical maximum is
approximately **(8192 - 24 - 50 - 4) / 4 ~ 2028 attributes**.

When the table gains new columns (e.g., via `ALTER TABLE ADD COLUMN`),
`nxmeta_expand_metapage_for_new_attributes()` extends the root
directory in place.  If the expanded directory would exceed the
available space, the current implementation raises an error:

```c
/* From noxu_meta.c:120 and noxu_meta.c:191 */
if (new_pd_lower > ((PageHeader) page)->pd_upper)
    elog(ERROR, "too many attributes for noxu");
```

**Overflow strategy (TODO):** The planned approach is to chain the root
directory to an overflow page when a single metapage cannot hold all
attributes.  The overflow page would:

1. Be allocated via `nxpage_getnewbuf()` and linked from a new field
   in `NXMetaPageOpaque` (e.g., `nx_rootdir_overflow`).
2. Use the same `NXRootDirItem` array format.
3. Store attribute indices starting from the first attribute that did
   not fit on the metapage.
4. Support further chaining for very wide tables (thousands of columns).

### 1.5 Metapage Cache

To avoid repeated I/O for metapage reads, Noxu maintains a
backend-private cache in `RelationData->rd_amcache`:

```c
typedef struct NXMetaCacheData
{
    int         cache_nattributes;
    struct
    {
        BlockNumber root;            /* B-tree root block */
        BlockNumber rightmost;       /* Rightmost leaf (for fast appends) */
        nxtid       rightmost_lokey; /* Lokey of the rightmost leaf */
    } cache_attrs[FLEXIBLE_ARRAY_MEMBER];
} NXMetaCacheData;
```

The cache is populated by `nxmeta_populate_cache()` (reads block 0)
and accessed via `nxmeta_get_cache()`.  It is invalidated when
`RelationGetTargetBlock(rel)` returns `InvalidBlockNumber` (triggered
by smgr invalidation events, e.g., when another backend extends the
relation).

### 1.6 Metapage Initialization

When a new Noxu relation is created or truncated:

1. `nxmeta_initmetapage()` extends the relation to create block 0.
2. `nxmeta_initmetapage_internal()` builds the page in temporary memory:
   - Initializes `PageHeaderData` with `sizeof(NXMetaPageOpaque)` special.
   - Sets `nx_page_id = NX_META_PAGE_ID`.
   - Sets `nx_fpm_head = InvalidBlockNumber` (no free pages yet).
   - Initializes deprecated UNDO fields to zero.
   - Sets all `tree_root_dir[i].root = InvalidBlockNumber`.
3. The temporary page is copied into the buffer with `PageRestoreTempPage()`.
4. A WAL record (`WAL_NOXU_INIT_METAPAGE`) with a full-page image is written.

### 1.7 Locking Protocol

The metapage follows a strict locking order:

- **SHARE lock**: For reading root block numbers (read-only scans).
- **EXCLUSIVE lock**: For modifying the root directory (new B-tree roots,
  attribute expansion) or updating `nx_fpm_head` (page allocation/deallocation).
- The metapage lock is always acquired **first** when multiple buffer locks
  are needed (see lock ordering in `noxu_internal.h`).

---

## 2. Free Pages Map Design

### 2.1 Overview

The Free Pages Map (FPM) tracks unused pages in a Noxu relation.  Pages
become free when B-tree pages are merged, overflow datum chains are
deleted, or UNDO pages are discarded.  The FPM allows these pages to be
recycled for new allocations, avoiding unnecessary relation extension.

Source: `src/backend/access/noxu/noxu_freepagemap.c`

### 2.2 Current Architecture: Singly-Linked List

The FPM is implemented as a **singly-linked list** of free pages.  Each
free page carries a small opaque area pointing to the next free page:

```
 Free page layout:
 ─────────────────
 0             PageHeaderData (24 bytes)
               (page body is unused)
 pd_special    NXFreePageOpaque
               ┌──────────────────────────────────────┐
               │ BlockNumber nx_next                   │
               │ uint16      padding                   │
               │ uint16      nx_page_id (0xF087)       │
               └──────────────────────────────────────┘
```

```c
typedef struct NXFreePageOpaque
{
    BlockNumber nx_next;
    uint16      padding;
    uint16      nx_page_id;     /* NX_FREE_PAGE_ID (0xF087) */
} NXFreePageOpaque;
```

The metapage's `nx_fpm_head` field points to the first page in the
list.  Pages are allocated from the head and freed to the head, giving
**LIFO (stack) ordering**.

### 2.3 Page Allocation: `nxpage_getnewbuf()`

When a new page is needed (B-tree split, overflow, new root):

```
    nxpage_getnewbuf(rel, metabuf)
    │
    ├── Read metapage (if metabuf == InvalidBuffer)
    │   Lock metapage EXCLUSIVE
    │
    ├── fpm_head = metaopaque->nx_fpm_head
    │
    ├── if fpm_head == InvalidBlockNumber:
    │   └── nxpage_extendrel_newbuf(rel, metabuf)
    │       └── Extend relation by multiple pages (see 2.5)
    │
    └── else:
        ├── Read and lock fpm_head page
        ├── Verify page is unused (nxpage_is_unused)
        ├── metaopaque->nx_fpm_head = opaque->nx_next
        └── Return the page (exclusive-locked, not initialized)
```

The caller is responsible for initializing the returned page for its
intended purpose (B-tree leaf, overflow page, etc.).

### 2.4 Page Deallocation: Batch Queue

Rather than acquiring the metapage EXCLUSIVE lock for every individual
page deletion, Noxu accumulates page frees in a **transaction-local
deallocation queue** and flushes them all at commit time.

#### 2.4.1 Queue Data Structure

```c
typedef struct NXDeallocQueueEntry
{
    RelFileLocator locator;   /* Identifies the relation */
    BlockNumber    blkno;     /* Page to add to FPM */
    bool           needs_wal; /* Does this relation need WAL? */
    struct NXDeallocQueueEntry *next;
} NXDeallocQueueEntry;
```

The queue is a singly-linked list headed by the static variable
`nxfpm_dealloc_queue`.  Entries are allocated in `TopTransactionContext`
so they survive until commit or abort.

#### 2.4.2 Enqueueing: `nxpage_delete_page()`

When a page is freed during a transaction:

```
    nxpage_delete_page(rel, buf)
    │
    ├── Register transaction callback (first time only)
    │   RegisterXactCallback(nxfpm_xact_callback)
    │
    ├── Mark page as deleted immediately:
    │   nxpage_mark_page_deleted(page, InvalidBlockNumber)
    │   MarkBufferDirty(buf)
    │
    └── Enqueue for batch FPM insertion:
        Allocate NXDeallocQueueEntry in TopTransactionContext
        entry->blkno = BufferGetBlockNumber(buf)
        entry->locator = rel->rd_locator
        Prepend to nxfpm_dealloc_queue
```

The page is immediately marked as deleted (with `NX_FREE_PAGE_ID` and
`nx_next = InvalidBlockNumber`) so its deleted state is durable even
before the FPM linkage is established.

#### 2.4.3 Flushing: `nxfpm_flush_dealloc_queue()`

At pre-commit time, the transaction callback triggers a batch flush:

```
    nxfpm_flush_dealloc_queue()
    │
    └── while queue is not empty:
        │
        ├── Pick first entry's relation (cur_locator)
        │
        ├── Read and EXCLUSIVE-lock metapage for cur_locator
        │   fpm_head = metaopaque->nx_fpm_head
        │
        ├── Walk queue, processing all entries matching cur_locator:
        │   for each matching entry:
        │   ├── Re-read the freed page
        │   ├── Lock it EXCLUSIVE
        │   ├── START_CRIT_SECTION
        │   ├── nxpage_mark_page_deleted(page, old_fpm_head)
        │   ├── fpm_head = entry->blkno
        │   ├── metaopaque->nx_fpm_head = fpm_head
        │   ├── MarkBufferDirty(metabuf, pagebuf)
        │   ├── Write WAL_NOXU_FPM_DELETE record
        │   ├── END_CRIT_SECTION
        │   └── UnlockReleaseBuffer(pagebuf)
        │
        ├── Entries for other relations are kept in queue
        │
        └── UnlockReleaseBuffer(metabuf)
```

This reduces metapage lock contention from **O(N)** lock acquisitions
(one per freed page) to **O(R)** (one per distinct relation that had
pages freed in the transaction).

#### 2.4.4 Transaction Callback

```c
static void nxfpm_xact_callback(XactEvent event, void *arg)
{
    switch (event)
    {
        case XACT_EVENT_PRE_COMMIT:
        case XACT_EVENT_PARALLEL_PRE_COMMIT:
        case XACT_EVENT_PRE_PREPARE:
            nxfpm_flush_dealloc_queue();
            break;

        case XACT_EVENT_ABORT:
        case XACT_EVENT_PARALLEL_ABORT:
            /* Discard the queue; pages remain allocated (correct). */
            nxfpm_dealloc_queue = NULL;
            break;

        default:
            break;
    }
}
```

On **abort**, the queue is simply discarded.  Pages that were marked as
deleted but never linked into the FPM remain allocated.  This is
correct because the deleting transaction rolled back -- the pages
should not be recycled.  (The pages will be properly reclaimed on the
next VACUUM or by future operations that notice they are deletable.)

### 2.5 Relation Extension: `nxpage_extendrel_newbuf()`

When the FPM is empty, the relation must be extended.  To amortize
extension lock overhead and improve spatial locality, multiple pages
are allocated at once:

```
    Relation size         Extra pages allocated
    ──────────────        ─────────────────────
    < 10 MB (1280 blks)         8
    < 100 MB (12800 blks)      32
    < 1 GB (128000 blks)      128
    >= 1 GB                    512
```

The first page is returned to the caller.  The remaining extra pages
are immediately added to the FPM linked list:

```
    nxpage_extendrel_newbuf(rel, metabuf)
    │
    ├── Determine num_extra_pages from relation size
    │
    ├── ExtendBufferedRelBy(rel, 1 + num_extra_pages, buffers)
    │   Returns: buffers[0] = locked return page
    │            buffers[1..N] = pinned extra pages
    │
    └── for each extra page (i = 1..N):
        ├── Lock page EXCLUSIVE
        ├── START_CRIT_SECTION
        ├── nxpage_mark_page_deleted(page, old_fpm_head)
        ├── metaopaque->nx_fpm_head = extrablk
        ├── MarkBufferDirty(metabuf, extrabuf)
        ├── Write WAL_NOXU_FPM_DELETE record
        ├── END_CRIT_SECTION
        └── UnlockReleaseBuffer(extrabuf)
```

### 2.6 Inline Recycling During Page Splits

When a B-tree page split or merge results in pages that should be
freed, the recycling happens **inline** within `nx_apply_split_changes()`
rather than through the deallocation queue:

```c
/* From noxu_btree.c:1260-1284 */
if (has_recycle)
{
    /* ... lock metapage ... */
    stack = head;
    while (stack)
    {
        if (stack->recycle)
        {
            nxpage_mark_page_deleted(page, fpm_head);
            fpm_head = blk;
        }
        stack = stack->next;
    }
    metaopaque->nx_fpm_head = fpm_head;
}
```

This approach chains multiple freed pages into the FPM list in a
single critical section, and includes the recycling information in
the `WAL_NOXU_BTREE_REWRITE_PAGES` WAL record (via a `recycle_bitmap`
and `old_fpm_head` field).

### 2.7 Page Reuse Safety

A page in the FPM is verified before reuse:

```c
static bool nxpage_is_unused(Buffer buf)
{
    Page page = BufferGetPage(buf);
    NXFreePageOpaque *opaque;

    if (PageIsNew(page))
        return false;
    if (PageGetSpecialSize(page) != sizeof(NXFreePageOpaque))
        return false;
    opaque = (NXFreePageOpaque *) PageGetSpecialPointer(page);
    if (opaque->nx_page_id != NX_FREE_PAGE_ID)
        return false;
    return true;
}
```

Design principles (from the source header):

- **False positives are acceptable**: A page may be incorrectly listed
  in the FPM; the check before reuse catches this.
- **Detectability**: A deletable page must be detectable by examining
  the page itself (and perhaps a few neighbors), without scanning the
  entire table.
- **Immediate reusability**: Once a page is marked as deletable, it
  should be immediately reusable.  Code that follows links must hold
  locks or be prepared to retry if landing on an unexpected page.

### 2.8 Explicit Flush Interface

`nxfpm_flush_pending_deletes()` provides a public interface to flush
the deallocation queue within a transaction.  This can be called before
allocating new pages from the same relation, to ensure recently freed
pages are available for reuse:

```c
void nxfpm_flush_pending_deletes(void)
{
    nxfpm_flush_dealloc_queue();
}
```

---

## 3. Interaction Between Metapage and FPM

### 3.1 Lock Contention Points

The metapage is a single shared resource that mediates all page
allocations and deallocations.  The following operations require an
EXCLUSIVE metapage lock:

| Operation                        | Metapage access |
|----------------------------------|-----------------|
| `nxpage_getnewbuf()` allocation  | Read + update `nx_fpm_head` |
| `nxpage_extendrel_newbuf()`      | Update `nx_fpm_head` for extras |
| `nxfpm_flush_dealloc_queue()`    | Update `nx_fpm_head` for frees |
| `nx_apply_split_changes()` recycle | Update `nx_fpm_head` |
| New B-tree root allocation       | Read `nx_fpm_head` + update `tree_root_dir` |
| Attribute expansion              | Update `nattributes` + `pd_lower` |

### 3.2 Allocation / Deallocation Flow

```
                ┌──────────────┐
                │   Metapage   │
                │  (block 0)   │
                └──────┬───────┘
                       │ nx_fpm_head
                       ▼
              ┌─────────────────┐     ┌─────────────────┐     ┌──────────┐
              │  Free Page A    │────▶│  Free Page B    │────▶│  ...     │
              │  nx_page_id:    │     │  nx_page_id:    │     │          │
              │  0xF087         │     │  0xF087         │     │          │
              │  nx_next: B     │     │  nx_next: ...   │     │ Invalid  │
              └─────────────────┘     └─────────────────┘     └──────────┘

  Allocation (nxpage_getnewbuf):
    1. Lock metapage EXCLUSIVE
    2. Pop head: return Page A, set nx_fpm_head = B

  Deallocation (nxpage_delete_page + flush):
    1. Mark page as NX_FREE_PAGE_ID (nx_next = InvalidBlockNumber)
    2. At commit: lock metapage EXCLUSIVE
    3. Set page's nx_next = current nx_fpm_head
    4. Set nx_fpm_head = freed page's block number
```

---

## 4. WAL Support

### 4.1 WAL_NOXU_FPM_DELETE (0x80)

Generated when a page is added to the FPM linked list.

**Block references:**
- `blkref #0`: The metapage (update `nx_fpm_head`)
- `blkref #1`: The freed page (`REGBUF_WILL_INIT` -- re-initialized)

**Payload:**

```c
typedef struct wal_noxu_fpm_delete
{
    BlockNumber old_fpm_head;   /* Previous FPM head, becomes nx_next */
} wal_noxu_fpm_delete;
```

**Redo (`nxfpm_delete_redo`):**

1. If metapage needs redo: set `metaopaque->nx_fpm_head = freeblk`.
2. Always re-initialize the freed page via `XLogInitBufferForRedo`:
   call `nxpage_mark_page_deleted(freepage, old_fpm_head)`.

### 4.2 WAL_NOXU_INIT_METAPAGE (0x00)

Generated when the metapage is initialized (relation creation or truncate).

**Block references:**
- `blkref #0`: The metapage (`REGBUF_FORCE_IMAGE` -- full-page image)

**Payload:**

```c
typedef struct wal_noxu_init_metapage
{
    int32 natts;    /* Number of attributes (for debugging) */
} wal_noxu_init_metapage;
```

**Redo (`nxmeta_initmetapage_redo`):**

The metapage is restored entirely from the full-page image.  The payload
is only for debugging/diagnostic purposes.

### 4.3 WAL_NOXU_BTREE_REWRITE_PAGES (0x60) -- Recycling

When pages are recycled inline during B-tree splits:

```c
typedef struct wal_noxu_btree_rewrite_pages
{
    AttrNumber  attno;
    int         numpages;
    uint32      recycle_bitmap;   /* Bit i = page at block_id (i+1) recycled */
    BlockNumber old_fpm_head;     /* FPM head before recycling */
} wal_noxu_btree_rewrite_pages;
```

During redo, pages whose bit is set in `recycle_bitmap` are marked as
free and chained into the FPM list.

---

## 5. Future Directions

### 5.1 Metapage Root Directory Overflow (TODO)

As noted in `noxu_meta.c:9` and `noxu_meta.c:125`:

> *"extend the root block dir to an overflow page if there are too
> many attributes to fit on one page"*

The planned approach:

1. Add an `nx_rootdir_overflow` field to `NXMetaPageOpaque`.
2. When the root directory cannot fit on the metapage, allocate an
   overflow page via `nxpage_getnewbuf()`.
3. The overflow page uses the same `NXRootDirItem` array layout,
   with its own overflow pointer for further chaining.
4. `nxmeta_get_root_for_attribute()` follows the overflow chain when
   the requested attribute index exceeds the metapage's capacity.

### 5.2 FPM Fragmentation Avoidance (TODO)

As noted in `noxu_freepagemap.c:32-34`:

> *"Avoid fragmentation. If B-tree page is split, try to hand out a
> page that's close to the old page. When the relation is extended,
> allocate a larger chunk at once."*

The current LIFO allocation order means that recently freed pages
(which may be scattered) are reused before contiguous pages from
relation extension.  This can lead to fragmentation where a single
B-tree's pages are scattered across the file, defeating OS readahead.

Potential improvements:

- **Locality-aware allocation**: Maintain per-region free lists or
  sort the FPM list by block number to favor allocating nearby pages.
- **Attribute affinity**: Track which attribute tree a page belongs to,
  and prefer handing out pages near that tree's existing pages.
- **Bulk allocation**: Already partially implemented via
  `nxpage_extendrel_newbuf()` which pre-allocates 8-512 extra pages
  based on relation size.

### 5.3 Hierarchical FSM-like Structure (TODO)

As noted in the README (`noxu_freepagemap.c` TODO section and
`src/backend/access/noxu/README:959-963`):

> *"That doesn't scale very well ... We'll probably want to do
> something smarter to avoid making the metapage a bottleneck"*

The current linked-list design has these scalability limitations:

1. **Metapage contention**: Every allocation and deallocation requires
   an EXCLUSIVE lock on the metapage (mitigated by batch deallocation).
2. **O(1) allocation but no locality**: The head page may be far from
   where we want to allocate.
3. **No partial-page tracking**: The FPM only tracks completely empty
   pages, not partially-used ones.

A future FSM-like hierarchical approach could:

1. Use a **tree of summary pages** (similar to PostgreSQL's built-in
   FSM for heap tables), where each internal node summarizes the
   availability of pages in its subtree.
2. Partition the block space into **regions** with independent free
   lists, reducing contention to the region level.
3. Use the summary tree to find free pages **near a target block**,
   improving spatial locality for readahead.
4. Support **lock-free or fine-grained locking** at the region level,
   allowing concurrent allocations to different parts of the file.

The batch deallocation queue (section 2.4) already provides significant
mitigation for the write-side contention.  The read-side (allocation)
is the primary remaining bottleneck for high-concurrency workloads
with many concurrent page splits.

---

## Appendix: Key Source Files

| File | Description |
|------|-------------|
| `src/backend/access/noxu/noxu_freepagemap.c` | FPM implementation: allocation, deallocation, batch queue, WAL redo |
| `src/backend/access/noxu/noxu_meta.c` | Metapage initialization, root directory, attribute expansion, cache |
| `src/include/access/noxu_internal.h` | Data structure definitions: `NXMetaPage`, `NXMetaPageOpaque`, `NXMetaCacheData`, `NXFreePageOpaque` (implicitly, via the FPM code) |
| `src/include/access/noxu_wal.h` | WAL record type codes and payload structs |
| `src/backend/access/noxu/noxu_btree.c` | Inline page recycling during B-tree splits (`nx_apply_split_changes`) |
| `src/backend/access/noxu/README` | High-level architecture overview |

## Appendix: Quick Reference -- Page Type Identifiers

| ID | Constant | Description |
|----|----------|-------------|
| `0xF083` | `NX_META_PAGE_ID` | Metapage (always block 0) |
| `0xF084` | `NX_BTREE_PAGE_ID` | B-tree page (internal or leaf) |
| `0xF085` | `NX_UNDO_PAGE_ID` | UNDO log page |
| `0xF086` | `NX_OVERFLOW_PAGE_ID` | Overflow page (oversized datums) |
| `0xF087` | `NX_FREE_PAGE_ID` | Free Page Map entry |
