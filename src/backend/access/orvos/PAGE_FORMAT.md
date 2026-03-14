# Orvos Page Format Reference

This document describes the on-disk page layout for every page type in
an Orvos relation.  All page types share the standard PostgreSQL
`PageHeaderData` prefix (24 bytes) and store a page-type-specific
"opaque" area at the end of the page via `pd_special`.

Page types are identified by the `ov_page_id` field in the opaque area:

| ID       | Constant              | Description                          |
|----------|-----------------------|--------------------------------------|
| `0xF083` | `OV_META_PAGE_ID`     | Metapage (always block 0)            |
| `0xF084` | `OV_BTREE_PAGE_ID`    | B-tree page (internal or leaf)       |
| `0xF085` | `OV_UNDO_PAGE_ID`     | UNDO log page                        |
| `0xF086` | `OV_TOAST_PAGE_ID`    | Toast page (oversized datums)        |
| `0xF087` | `OV_FREE_PAGE_ID`     | Free Page Map (FPM) entry            |


## 1  Metapage (block 0)

Every Orvos relation begins with a single metapage at block 0.

```
 0                   PageHeaderData  (24 B)
24                   OVMetaPage
                     +---------------------------------+
                     | int32  nattributes              |  number of per-attribute trees
                     +---------------------------------+
                     | OVRootDirItem  tree_root_dir[0] |  { BlockNumber root }
                     | OVRootDirItem  tree_root_dir[1] |
                     | ...                             |
                     | tree_root_dir[nattributes]      |  index 0 = TID tree
                     +---------------------------------+
                              ...
pd_special -->       OVMetaPageOpaque
                     +---------------------------------+
                     | BlockNumber  ov_undo_head       |  oldest UNDO page
                     | BlockNumber  ov_undo_tail       |  newest UNDO page
                     | uint64 ov_undo_tail_first_counter |
                     | OVUndoRecPtr ov_undo_oldestptr  |  oldest live UNDO record
                     | BlockNumber  ov_fpm_head        |  head of Free Page Map
                     | uint16  ov_flags                |
                     | uint16  ov_page_id (0xF083)     |
                     +---------------------------------+
```

The `tree_root_dir` array is indexed by attribute number.  Index 0
(`OV_META_ATTRIBUTE_NUM`) holds the root of the TID tree.  Indices
1..nattributes hold the roots of the per-column attribute B-trees.

`OVRootDirItem` contains a single `BlockNumber root` field pointing to
the root page of the corresponding B-tree.


## 2  B-tree Pages

Both the TID tree and the attribute trees use the same physical page
format.  Internal and leaf pages are distinguished by the `ov_level`
field in the opaque area (0 = leaf).

### 2.1  Opaque Area (`OVBtreePageOpaque`)

```
pd_special -->       OVBtreePageOpaque
                     +---------------------------------+
                     | AttrNumber   ov_attno           |  attribute number (0 = TID tree)
                     | BlockNumber  ov_next            |  right sibling
                     | ovtid        ov_lokey           |  inclusive lower bound
                     | ovtid        ov_hikey           |  exclusive upper bound
                     | uint16       ov_level           |  0 = leaf
                     | uint16       ov_flags           |  OVBT_ROOT etc.
                     | uint16       padding            |
                     | uint16       ov_page_id (0xF084)|
                     +---------------------------------+
```

Every B-tree page is self-identifying: the `ov_attno`, `ov_lokey`, and
`ov_hikey` fields allow the page's parent downlink to be located
without additional state.

### 2.2  Internal Page Layout

The page contents (between `pd_upper` and `pd_special`) are an array of
`OVBtreeInternalPageItem`:

```
  +-----------------------------+
  | ovtid       tid             |  separator key
  | BlockNumber childblk        |  child page
  +-----------------------------+
  | ...                         |
  +-----------------------------+
```

The number of items is deduced from `pd_lower`:

```
  num_items = (pd_lower - SizeOfPageHeaderData) / sizeof(OVBtreeInternalPageItem)
```

Internal pages look identical for TID trees and attribute trees.

### 2.3  TID Tree Leaf Page Layout

TID tree leaf pages contain `OVTidArrayItem` entries.  Each item covers
a contiguous range of TIDs and encodes both the TID deltas and UNDO
slot information.

```
  OVTidArrayItem
  +-----------------------------------------+
  | uint16  t_size                           |  total size of this item
  | uint16  t_num_tids                       |  number of TIDs encoded
  | uint16  t_num_codewords                  |  number of Simple-8b codewords
  | uint16  t_num_undo_slots                 |  total UNDO slots (incl. 2 implicit)
  | ovtid   t_firsttid                       |  first TID in range
  | ovtid   t_endtid                         |  one past last TID (exclusive)
  +-----------------------------------------+
  |         t_payload[]                      |
  |  [ t_num_codewords x uint64 codewords ] |  Simple-8b encoded TID deltas
  |  [ (t_num_undo_slots - 2) x UndoRecPtr ]|  explicit UNDO slot values
  |  [ ceil(t_num_tids / 32) x uint64 ]     |  UNDO slot-number words (2 bits/tid)
  +-----------------------------------------+
```

**TID encoding:**  TID deltas (gaps between consecutive TIDs) are
packed using Simple-8b encoding.  The first encoded value is always 0
(the absolute TID is in `t_firsttid`).  Small gaps (common on newly
loaded tables) compress to a few bits per tuple.

**UNDO slot encoding:**  There are logically 4 UNDO slots per item:

| Slot | Meaning                                              |
|------|------------------------------------------------------|
| 0    | `OVBT_OLD_UNDO_SLOT` -- tuple visible to everyone    |
| 1    | `OVBT_DEAD_UNDO_SLOT` -- tuple is dead               |
| 2-3  | Normal UNDO pointers (physically stored in the item) |

Slots 0 and 1 are implicit (never stored on disk).  Each tuple's
2-bit slot number is packed into 64-bit "slotwords", 32 slot numbers
per word.

**Size calculation:**
```c
SizeOfOVTidArrayItem(num_tids, num_undo_slots, num_codewords)
  = offsetof(OVTidArrayItem, t_payload)
  + num_codewords * 8
  + (num_undo_slots - 2) * sizeof(OVUndoRecPtr)
  + ceil(num_tids / 32) * 8
```

**Limits:**  `OVBT_MAX_ITEM_CODEWORDS` = 16, `OVBT_MAX_ITEM_TIDS` = 128.

### 2.4  Attribute Tree Leaf Page Layout

Attribute tree leaf pages contain `OVAttributeArrayItem` entries (or
their compressed variant, `OVAttributeCompressedItem`).

#### Uncompressed Item (`OVAttributeArrayItem`)

```
  OVAttributeArrayItem
  +-----------------------------------------+
  | uint16  t_size                           |  total size of this item
  | uint16  t_flags                          |  OVBT_ATTR_COMPRESSED, OVBT_HAS_NULLS
  | uint16  t_num_elements                   |  number of datums
  | uint16  t_num_codewords                  |  Simple-8b codewords for TID deltas
  | ovtid   t_firsttid                       |  first TID in range
  | ovtid   t_endtid                         |  one past last TID (exclusive)
  +-----------------------------------------+
  | uint64  t_tid_codewords[]                |  Simple-8b encoded TID deltas
  +-----------------------------------------+
  | bits8   null_bitmap[]                    |  only if OVBT_HAS_NULLS set
  |         (size = ceil(t_num_elements/8))  |
  +-----------------------------------------+
  | <datum data>                             |  packed datums (see below)
  +-----------------------------------------+
```

#### Compressed Item (`OVAttributeCompressedItem`)

When the `OVBT_ATTR_COMPRESSED` flag is set in `t_flags`:

```
  OVAttributeCompressedItem
  +-----------------------------------------+
  | uint16  t_size                           |  total size (compressed)
  | uint16  t_flags                          |  OVBT_ATTR_COMPRESSED set
  | uint16  t_num_elements                   |
  | uint16  t_num_codewords                  |
  | ovtid   t_firsttid                       |
  | ovtid   t_endtid                         |
  | uint16  t_uncompressed_size              |  size before compression
  +-----------------------------------------+
  | char    t_payload[]                      |  compressed TID codewords +
  |                                          |  null bitmap + datum data
  +-----------------------------------------+
```

Compression is applied to the variable-length portion (TID codewords,
null bitmap, and datum data combined).  The compression algorithm is
selected at build time: zstd (preferred), LZ4, or pglz (fallback).

The buffer cache stores compressed blocks.  Decompression happens
on-the-fly in backend-private memory.

#### Datum Encoding

Fixed-width types are stored without alignment padding.  Variable-length
types use a custom encoding (not standard PostgreSQL varlena):

```
  0xxxxxxx                        -- 1-byte header, up to 128 bytes of data
  1xxxxxxx xxxxxxxx               -- 2-byte header, up to 32767 bytes
  11111111 11111111 <BlockNumber> -- orvos toast pointer
```

This compact encoding avoids the 4-byte varlena overhead for short
values.

#### In-Memory Representation (`OVExplodedItem`)

During page repacking, items are decoded into `OVExplodedItem`:

```
  OVExplodedItem
  +-----------------------------------------+
  | uint16  t_size = 0  (sentinel)          |  distinguishes from on-disk items
  | uint16  t_flags                         |
  | uint16  t_num_elements                  |
  | ovtid  *tids                            |  expanded TID array
  | bits8  *nullbitmap                       |
  | char   *datumdata                        |  raw datum bytes
  | int     datumdatasz                      |
  +-----------------------------------------+
```


## 3  UNDO Log Pages

UNDO pages form a singly-linked list (head = oldest, tail = newest).

```
 0                   PageHeaderData  (24 B)
24                   <UNDO records, packed sequentially>
                     ...
pd_special -->       OVUndoPageOpaque
                     +-----------------------------------------+
                     | BlockNumber       next                  |
                     | OVUndoRecPtr      first_undorecptr      |
                     | OVUndoRecPtr      last_undorecptr       |
                     | uint16 padding x3                       |
                     | uint16 ov_page_id (0xF085)              |
                     +-----------------------------------------+
```

### 3.1  UNDO Record Pointer (`OVUndoRecPtr`)

```
  OVUndoRecPtr
  +-----------------------------------+
  | uint64      counter               |  monotonically increasing sequence
  | BlockNumber blkno                 |  physical block of the UNDO record
  | int32       offset                |  byte offset within the page
  +-----------------------------------+
```

Special pointer values:

| Name              | Counter | BlockNumber       | Meaning                     |
|-------------------|---------|-------------------|-----------------------------|
| `InvalidUndoPtr`  | 0       | `InvalidBlockNumber` | Visible to everyone      |
| `DeadUndoPtr`     | 1       | `InvalidBlockNumber` | Not visible to anyone    |

### 3.2  UNDO Record Types

All UNDO records share a common header (`OVUndoRec`):

```
  OVUndoRec  (common header)
  +-----------------------------------+
  | int16        size                 |  total record size including header
  | uint8        type                 |  OVUNDO_TYPE_* constant
  | OVUndoRecPtr undorecptr           |  this record's location
  | TransactionId xid                 |  transaction that created this record
  | CommandId    cid                  |  command ID within the transaction
  | OVUndoRecPtr prevundorec          |  previous UNDO record in chain
  +-----------------------------------+
```

| Type ID | Constant                    | Extension Structure          |
|---------|-----------------------------|------------------------------|
| 1       | `OVUNDO_TYPE_INSERT`        | `OVUndoRec_Insert`           |
| 2       | `OVUNDO_TYPE_DELETE`        | `OVUndoRec_Delete`           |
| 3       | `OVUNDO_TYPE_UPDATE`        | `OVUndoRec_Update`           |
| 4       | `OVUNDO_TYPE_TUPLE_LOCK`    | `OVUndoRec_TupleLock`        |
| 5       | `OVUNDO_TYPE_DELTA_INSERT`  | `OVUndoRec_DeltaInsert`      |

#### INSERT Record

```
  OVUndoRec_Insert
  +-----------------------------------+
  | OVUndoRec   rec                   |
  | ovtid       firsttid              |
  | ovtid       endtid                |  exclusive
  | uint32      speculative_token     |
  +-----------------------------------+
```

#### DELETE Record

```
  OVUndoRec_Delete
  +-----------------------------------+
  | OVUndoRec   rec                   |
  | bool        changedPart           |
  | uint16      num_tids              |  up to OVUNDO_NUM_TIDS_PER_DELETE (50)
  | ovtid       tids[50]              |
  +-----------------------------------+
```

#### UPDATE Record

```
  OVUndoRec_Update
  +-----------------------------------+
  | OVUndoRec   rec                   |
  | ovtid       oldtid                |
  | ovtid       newtid                |
  | bool        key_update            |
  +-----------------------------------+
```

#### Column-Delta INSERT Record

Used when an UPDATE only changes a subset of columns.  Unchanged columns
are fetched from `predecessor_tid` instead of being stored redundantly.

```
  OVUndoRec_DeltaInsert
  +-----------------------------------+
  | OVUndoRec   rec                   |
  | ovtid       firsttid              |
  | ovtid       endtid                |
  | uint32      speculative_token     |
  | ovtid       predecessor_tid       |
  | int16       natts                 |
  | int16       nchanged              |
  | uint32      changed_cols[]        |  bitmap, 1 bit per column
  +-----------------------------------+
```

The bitmap uses `ceil(natts/32)` words.  Bit `(attno-1)` set means
column `attno` was modified and has a B-tree entry under this TID.

#### Tuple Lock Record

```
  OVUndoRec_TupleLock
  +-----------------------------------+
  | OVUndoRec   rec                   |
  | ovtid       tid                   |
  | LockTupleMode lockmode            |
  +-----------------------------------+
```


## 4  Toast Pages

Large datums that exceed `MaxOrvosDatumSize` (approximately
`BLCKSZ - 500`) are split into chunks stored on dedicated toast pages.
The pages form a doubly-linked list.

```
 0                   PageHeaderData  (24 B)
24                   <toast chunk data>
                     ...
pd_special -->       OVToastPageOpaque
                     +-----------------------------------------+
                     | AttrNumber  ov_attno                    |
                     | ovtid       ov_tid       (first page)   |
                     | uint32      ov_total_size (first page)  |
                     | uint32      ov_slice_offset             |
                     | BlockNumber ov_prev                     |
                     | BlockNumber ov_next                     |
                     | uint16      ov_flags                    |
                     | uint16      padding x2                  |
                     | uint16      ov_page_id (0xF086)         |
                     +-----------------------------------------+
```

`ov_tid` and `ov_total_size` are only set on the first page of a toast
chain.  `ov_slice_offset` records the byte offset of this chunk within
the complete datum.

An in-tree toast pointer (`varatt_ov_toastptr`) is stored in place of
the datum:

```
  varatt_ov_toastptr
  +-----------------------------------+
  | uint8       va_header             |
  | uint8       va_tag = VARTAG_ORVOS (10) |
  | BlockNumber ovt_block             |  first toast page
  +-----------------------------------+
```


## 5  Free Page Map (FPM)

Unused pages are tracked via a singly-linked list.  The metapage's
`ov_fpm_head` field points to the first free page.

```
 0                   PageHeaderData  (24 B)
                     (page contents unused)
pd_special -->       OVFreePageOpaque
                     +-----------------------------------------+
                     | BlockNumber ov_next                     |  next free page
                     | uint16      padding                     |
                     | uint16      ov_page_id (0xF087)         |
                     +-----------------------------------------+
```

Pages are allocated from the head (LIFO order).  When a page is freed,
it is added to the head of the list.


## 6  TID Addressing

Throughout Orvos, TIDs are carried as 64-bit unsigned integers (`ovtid`)
rather than the standard `ItemPointerData`.  Conversions are defined in
`orvos_tid.h`.

```
  ovtid = blk * (MaxOVTidOffsetNumber - 1) + off
```

Where `MaxOVTidOffsetNumber` = 129.

Special values:

| Name              | Value | Meaning                 |
|-------------------|-------|-------------------------|
| `InvalidOVTid`    | 0     | No valid TID            |
| `MinOVTid`        | 1     | Smallest valid TID      |
| `MaxOVTid`        | ~2^48 | Largest valid TID       |

TIDs are logical, not physical.  Nearby TIDs tend to reside on nearby
pages, so block-range based optimizations (BRIN, bitmap scans) still
provide benefit.


## 7  Simple-8b Encoding

TID deltas throughout Orvos are compressed using Simple-8b encoding.
Each 64-bit codeword packs multiple small integers.  The selector (top
4 bits) determines how many integers are packed and their bit width:

| Selector | Count | Bits each | Max value  |
|----------|-------|-----------|------------|
| 0        | 240   | 0         | 0          |
| 1        | 60    | 1         | 1          |
| 2        | 30    | 2         | 3          |
| 3        | 20    | 3         | 7          |
| 4        | 15    | 4         | 15         |
| 5        | 12    | 5         | 31         |
| 6        | 10    | 6         | 63         |
| 7        | 8     | 7         | 127        |
| 8        | 7     | 8         | 255        |
| 9        | 6     | 10        | 1023       |
| 10       | 5     | 12        | 4095       |
| 11       | 4     | 15        | 32767      |
| 12       | 3     | 20        | 1048575    |
| 13       | 2     | 30        | 1073741823 |
| 14       | 1     | 60        | 2^60 - 1   |

For consecutive TIDs with no gaps (delta = 1), selector 1 packs 60
TIDs per codeword, yielding ~1 bit per TID.


## 8  Compression

Orvos compresses attribute tree leaf pages using one of three algorithms,
selected at PostgreSQL build time:

| Priority | Algorithm | Configure flag | Notes                       |
|----------|-----------|----------------|-----------------------------|
| 1        | zstd      | `--with-zstd`  | Best ratio and speed        |
| 2        | LZ4       | `--with-lz4`   | Very fast, good ratio       |
| 3        | pglz      | (built-in)     | Fallback, significantly slower |

Compression is applied to the variable-length portion of attribute items
(TID codewords + null bitmap + datum data).  The buffer cache stores
compressed pages; decompression is performed on-the-fly in
backend-private memory.

Only attribute tree leaf pages are compressed.  TID tree pages and
internal B-tree pages are not compressed.


## 9  WAL Record Types

| ID     | Constant                              | Description                          |
|--------|---------------------------------------|--------------------------------------|
| `0x00` | `WAL_ORVOS_INIT_METAPAGE`             | Initialize metapage                  |
| `0x10` | `WAL_ORVOS_UNDO_NEWPAGE`              | Extend UNDO log                      |
| `0x20` | `WAL_ORVOS_UNDO_DISCARD`              | Discard old UNDO records             |
| `0x30` | `WAL_ORVOS_BTREE_NEW_ROOT`            | Create new B-tree root               |
| `0x40` | `WAL_ORVOS_BTREE_ADD_LEAF_ITEMS`      | Add items to B-tree leaf             |
| `0x50` | `WAL_ORVOS_BTREE_REPLACE_LEAF_ITEM`   | Replace item on B-tree leaf          |
| `0x60` | `WAL_ORVOS_BTREE_REWRITE_PAGES`       | Page split / rewrite                 |
| `0x70` | `WAL_ORVOS_TOAST_NEWPAGE`             | Add toast page                       |
| `0x80` | `WAL_ORVOS_FPM_DELETE`                | Add page to Free Page Map            |
