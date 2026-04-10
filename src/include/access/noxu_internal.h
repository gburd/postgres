/**
 * @file noxu_internal.h
 * @brief Internal declarations for Noxu columnar table access method.
 *
 * This header defines the core data structures for Noxu's on-disk page
 * formats, B-tree page layouts, TID and attribute array items, metapage
 * structures, scan state, and cache structures.  It is the central header
 * for all Noxu backend code.
 *
 * @par Architecture Overview
 * An Noxu relation consists of multiple B-trees stored in a single
 * physical file.  Block 0 is always a metapage.  The TID tree (attribute
 * number 0) stores visibility/UNDO information.  Each user column has its
 * own attribute B-tree.  UNDO log pages, overflow pages, and free pages are
 * also stored in the same file, distinguished by page type IDs in their
 * opaque areas.
 *
 * @par Lock Ordering
 * When acquiring multiple buffer locks:
 * - Metapage lock is acquired first when needed.
 * - B-tree pages are locked top-down (parent before child).
 * - Within a level, pages are locked left-to-right.
 * - UNDO buffer locks are acquired after B-tree page locks.
 * - Split stack entries hold exclusive locks on all modified pages;
 *   changes are applied atomically via nx_apply_split_changes().
 *
 * @par Memory Context
 * Scan structures (NXTidTreeScan, NXAttrTreeScan) carry a MemoryContext
 * field that must be used for any allocations that outlive a single
 * getnext() call.  The caller's CurrentMemoryContext may be short-lived.
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/include/access/noxu_internal.h
 */
#ifndef NOXU_INTERNAL_H
#define NOXU_INTERNAL_H

#include "access/tableam.h"
#include "access/noxu_compression.h"
#include "access/noxu_tid.h"
#include "access/relundo.h"
#include "lib/integerset.h"
#include "storage/bufmgr.h"
#include "storage/smgr.h"
#include "utils/datum.h"

/*
 * nx_undo_reservation - UNDO buffer reservation structure
 *
 * Used by the bridge layer in noxu_tidpage.c to maintain compatibility
 * with existing UNDO creation patterns while using RelUndo API underneath.
 */
typedef struct nx_undo_reservation
{
	Buffer		undobuf;		/* UNDO buffer */
	RelUndoRecPtr undorecptr;	/* UNDO record pointer */
	uint16		length;			/* Length of UNDO record */
	char	   *ptr;			/* Direct pointer to UNDO buffer location */
}			nx_undo_reservation;

/*
 * nx_pending_undo_op - Pending UNDO operation structure
 *
 * Used by the bridge layer in noxu_tidpage.c to maintain compatibility
 * with existing UNDO creation patterns while using RelUndo API underneath.
 */
typedef struct nx_pending_undo_op
{
	nx_undo_reservation reservation;
	bool		is_update;
	uint64		payload[FLEXIBLE_ARRAY_MEMBER];
}			nx_pending_undo_op;

/*
 * Noxu-specific UNDO payload for DELTA_INSERT operations.
 * This extends the generic RelUndoDeltaInsertPayload with Noxu-specific
 * fields needed for delta updates, including a predecessor TID for following
 * update chains and a variable-length changed-columns bitmap.
 */
typedef struct NXRelUndoDeltaInsertPayload
{
	ItemPointerData firsttid;	/* First TID in range (inclusive) */
	ItemPointerData endtid;		/* End TID (exclusive) */
	uint32		speculative_token;	/* Speculative insertion token */
	nxtid		predecessor_tid;	/* Previous version TID */
	int16		natts;			/* Number of attributes */
	int16		nchanged;		/* Number of changed columns */
	uint32		changed_cols[FLEXIBLE_ARRAY_MEMBER];
}			NXRelUndoDeltaInsertPayload;

/* Number of uint32 words needed for a changed-column bitmap with natts attributes */
#define NXUNDO_DELTA_BITMAP_WORDS(natts) \
	(((natts) + 31) / 32)

#define SizeOfNXRelUndoDeltaInsertPayload(natts) \
	(offsetof(NXRelUndoDeltaInsertPayload, changed_cols) + \
	 NXUNDO_DELTA_BITMAP_WORDS(natts) * sizeof(uint32))

/*
 * Helper function to check if a column was changed in a delta update.
 */
static inline bool
nx_relundo_delta_col_is_changed(const NXRelUndoDeltaInsertPayload * delta, int attno)
{
	int			idx = (attno - 1) / 32;
	int			bit = (attno - 1) % 32;

	return (delta->changed_cols[idx] & (1U << bit)) != 0;
}

/**
 * @brief Dead UNDO pointer: marks a tuple as not visible to anyone.
 *
 * Used in TID items to mark dead tuples awaiting VACUUM cleanup.
 * The counter value of 1 is reserved for this purpose and will never
 * collide with real UNDO records (whose counters start at higher values).
 *
 * Note: With RelUndoRecPtr's 16-bit counter, the "dead" sentinel is simply
 * the value 1 packed entirely in the counter field (block=0, offset=0).
 */
#define DeadRelUndoRecPtr	MakeRelUndoRecPtr(1, 0, 0)

/** @brief Attribute number used for the TID tree (visibility metadata). */
#define NX_META_ATTRIBUTE_NUM 0

/** @brief Sentinel value indicating no speculative insertion token. */
#define INVALID_SPECULATIVE_TOKEN 0

/**
 * @name Page Type Identifiers
 * @brief Magic numbers stored in the opaque area of each page to identify
 *        the page type.  Every page in an Noxu relation carries one of
 *        these in its nx_page_id field.
 * @{
 */
#define	NX_META_PAGE_ID		0xF083
#define	NX_BTREE_PAGE_ID	0xF084
#define	NX_UNDO_PAGE_ID		0xF085
#define	NX_OVERFLOW_PAGE_ID	0xF086
#define	NX_FREE_PAGE_ID		0xF087
#define	NX_DICT_PAGE_ID		0xF088	/**< Shared dictionary page. */
#define	NX_LSM_ROW_PAGE_ID	0xF089	/**< LSM row-oriented segment page. */
#define	NX_LSM_META_PAGE_ID	0xF08A	/**< LSM level metadata page. */
/** @} */

/** @brief Flag indicating this B-tree page is the root of its tree. */
#define NXBT_ROOT				0x0001

/**
 * @brief Opaque area at the end of every Noxu B-tree page.
 *
 * Stored in the pd_special region of the standard PageHeaderData.
 * Contains enough information to identify the page (attribute number,
 * key range, level) so that the page's parent downlink can be relocated
 * after a concurrent split, and so that corruption can be detected.
 *
 * @param nx_attno   Attribute number (0 = TID tree, 1..N = user columns).
 * @param nx_next    Right sibling block number (InvalidBlockNumber if rightmost).
 * @param nx_lokey   Inclusive lower bound TID for keys on this page.
 * @param nx_hikey   Exclusive upper bound TID for keys on this page.
 * @param nx_level   B-tree level: 0 = leaf, >0 = internal.
 * @param nx_flags   Combination of NXBT_ROOT and other flags.
 * @param nx_page_id Always NX_BTREE_PAGE_ID (0xF084).
 */
typedef struct NXBtreePageOpaque
{
	AttrNumber	nx_attno;
	BlockNumber nx_next;
	nxtid		nx_lokey;		/* inclusive */
	nxtid		nx_hikey;		/* exclusive */
	uint16		nx_level;		/* 0 = leaf */
	uint16		nx_flags;
	uint16		padding;		/* padding, to put nx_page_id last */
	uint16		nx_page_id;		/* always NX_BTREE_PAGE_ID */
} NXBtreePageOpaque;

/**
 * @brief Extract the NXBtreePageOpaque from a page's special area.
 * @param page  A Page pointer to a B-tree page.
 * @return Pointer to the NXBtreePageOpaque structure.
 */
#define NXBtreePageGetOpaque(page) ((NXBtreePageOpaque *) PageGetSpecialPointer(page))

/**
 * @brief Internal (non-leaf) B-tree page item.
 *
 * The page contents between pd_upper and pd_special consist of an array
 * of these items.  The number of items is deduced from pd_lower:
 *   num = (pd_lower - SizeOfPageHeaderData) / sizeof(NXBtreeInternalPageItem)
 *
 * @param tid       Separator key (first TID in the right subtree).
 * @param childblk  Block number of the child page.
 */
typedef struct NXBtreeInternalPageItem
{
	nxtid		tid;
	BlockNumber childblk;
} NXBtreeInternalPageItem;

/**
 * @brief Get pointer to the array of internal page items.
 * @param page  A Page containing internal B-tree items.
 * @return Pointer to the first NXBtreeInternalPageItem.
 */
static inline NXBtreeInternalPageItem *
NXBtreeInternalPageGetItems(Page page)
{
	NXBtreeInternalPageItem *items;

	items = (NXBtreeInternalPageItem *) PageGetContents(page);

	return items;
}

/**
 * @brief Get the number of items on an internal B-tree page.
 * @param page  A Page containing internal B-tree items.
 * @return Number of NXBtreeInternalPageItem entries on the page.
 */
static inline int
NXBtreeInternalPageGetNumItems(Page page)
{
	NXBtreeInternalPageItem *begin;
	NXBtreeInternalPageItem *end;

	begin = (NXBtreeInternalPageItem *) PageGetContents(page);
	end = (NXBtreeInternalPageItem *) ((char *) page + ((PageHeader) page)->pd_lower);

	return end - begin;
}

/**
 * @brief Check whether an internal B-tree page has room for another item.
 * @param page  A Page containing internal B-tree items.
 * @return true if pd_upper - pd_lower is too small for another item.
 */
static inline bool
NXBtreeInternalPageIsFull(Page page)
{
	PageHeader	phdr = (PageHeader) page;

	return phdr->pd_upper - phdr->pd_lower < sizeof(NXBtreeInternalPageItem);
}

/**
 * @brief Uncompressed attribute B-tree leaf page item.
 *
 * Leaf pages in the attribute trees are packed with "array items" that
 * contain the actual user data for a column in a compact format.  Each
 * item contains datums for a contiguous range of TIDs [t_firsttid,
 * t_endtid).  Ranges of different items never overlap, though gaps may
 * exist due to deletions or updates.
 *
 * @par Layout (variable-length)
 * - Fixed header (this struct up to t_tid_codewords)
 * - t_num_codewords x uint64: Simple-8b encoded TID deltas
 * - NULL bitmap (ceil(t_num_elements/8) bytes), if NXBT_HAS_NULLS
 * - Packed datum data (see below)
 *
 * @par Datum Encoding
 * Fixed-width types are stored without alignment padding.  Variable-length
 * types use a custom compact encoding instead of standard PostgreSQL
 * varlena format:
 * - @c 0xxxxxxx : 1-byte header, up to 128 bytes of data follow.
 * - @c 1xxxxxxx @c xxxxxxxx : 2-byte header, up to 32767 bytes.
 * - @c 0xFF @c 0xFF @c <BlockNumber> : Noxu overflow pointer (datum on
 *   separate overflow pages within the same relation file).
 *
 * @param t_size          Total on-disk size of this item in bytes.
 * @param t_flags         Bitmask: NXBT_ATTR_COMPRESSED, NXBT_HAS_NULLS.
 * @param t_num_elements  Number of datums (tuples) in this item.
 * @param t_num_codewords Number of Simple-8b codewords for TID deltas.
 * @param t_firsttid      First TID in the range (inclusive).
 * @param t_endtid        One past the last TID in the range (exclusive).
 * @param t_tid_codewords Flexible array of Simple-8b encoded TID deltas.
 */
typedef struct NXAttributeArrayItem
{
	uint16		t_size;
	uint16		t_flags;

	uint16		t_num_elements;
	uint16		t_num_codewords;

	nxtid		t_firsttid;
	nxtid		t_endtid;

	uint64		t_tid_codewords[FLEXIBLE_ARRAY_MEMBER];

	/* NULL bitmap follows, if NXBT_HAS_NULLS is set */

	/* The Datum data follows */
}			NXAttributeArrayItem;

/**
 * @brief Compressed attribute B-tree leaf page item.
 *
 * When the NXBT_ATTR_COMPRESSED flag is set in t_flags, the item uses this
 * layout instead of NXAttributeArrayItem.  The TID codewords, null bitmap,
 * and datum data are compressed together into t_payload using the
 * build-time-selected algorithm (zstd > LZ4 > pglz).
 *
 * The buffer cache stores pages in compressed form; decompression is done
 * on-the-fly in backend-private memory.
 *
 * @param t_size              Total on-disk size (compressed).
 * @param t_flags             Must have NXBT_ATTR_COMPRESSED set.
 * @param t_num_elements      Number of datums.
 * @param t_num_codewords     Number of Simple-8b codewords (before compression).
 * @param t_firsttid          First TID (inclusive).
 * @param t_endtid            One past last TID (exclusive).
 * @param t_uncompressed_size Size of the data before compression.
 * @param t_payload           Compressed data (flexible array).
 */
typedef struct NXAttributeCompressedItem
{
	uint16		t_size;
	uint16		t_flags;

	uint16		t_num_elements;
	uint16		t_num_codewords;

	nxtid		t_firsttid;
	nxtid		t_endtid;

	uint16		t_uncompressed_size;

	/* compressed data follows */
	char		t_payload[FLEXIBLE_ARRAY_MEMBER];

} NXAttributeCompressedItem;

/**
 * @brief In-memory "exploded" representation of an attribute array item.
 *
 * Used during page repacking operations (splits, merges) when items need
 * to be manipulated individually.  Distinguished from on-disk items by
 * t_size == 0.
 *
 * @param t_size         Always 0 (sentinel to distinguish from on-disk items).
 * @param t_flags        Same flag bits as NXAttributeArrayItem.
 * @param t_num_elements Number of datums.
 * @param tids           Expanded array of TIDs.
 * @param nullbitmap     NULL bitmap (or NULL if no NULLs).
 * @param datumdata      Raw packed datum bytes.
 * @param datumdatasz    Size of datumdata in bytes.
 */
typedef struct NXExplodedItem
{
	uint16		t_size;			/* dummy 0 */
	uint16		t_flags;

	uint16		t_num_elements;

	nxtid	   *tids;

	uint8	   *nullbitmap;

	char	   *datumdata;
	int			datumdatasz;
}			NXExplodedItem;

/** @brief Flag: this attribute item is compressed (use NXAttributeCompressedItem). */
#define NXBT_ATTR_COMPRESSED		0x0001
/** @brief Flag: this attribute item contains NULLs (a null bitmap follows the TID codewords). */
#define NXBT_HAS_NULLS				0x0002
/*
 * When set, short varlena values (attlen == -1, attstorage != 'p') in this
 * item are stored in PostgreSQL's native 1-byte short varlena format rather
 * than the custom noxu length-prefix encoding. This allows the read path
 * to return a direct pointer into the decompressed buffer without copying
 * or reformatting the data, eliminating per-datum conversion overhead.
 *
 * Long varlenas (> 126 data bytes) and noxu overflow pointers are still stored
 * in the original noxu encoding even when this flag is set.
 */
#define NXBT_ATTR_FORMAT_NATIVE_VARLENA	0x0004
#define NXBT_ATTR_FORMAT_FOR			0x0008	/* Frame of Reference encoding */
#define NXBT_ATTR_BITPACKED				0x0010	/* boolean values bit-packed,
												 * 8 per byte */
#define NXBT_ATTR_NO_NULLS				0x0020	/* no NULLs present, bitmap
												 * omitted entirely */
#define NXBT_ATTR_SPARSE_NULLS			0x0040	/* sparse NULL encoding:
												 * (offset, count) pairs */
#define NXBT_ATTR_RLE_NULLS				0x0080	/* RLE encoding for sequential
												 * NULL runs */
#define NXBT_ATTR_FORMAT_DICT			0x0100	/* dictionary-encoded for
												 * low-cardinality columns */
#define NXBT_ATTR_FORMAT_FIXED_BIN		0x0200	/* fixed-binary storage (e.g.
												 * UUID as 16 bytes) */
#define NXBT_ATTR_FORMAT_FSST			0x0400	/* FSST string compression
												 * applied */
#define NXBT_ATTR_SHARED_DICT			0x0800	/* compressed with shared
												 * dictionary */
#define NXBT_ATTR_FORMAT_ARRAY_DECOMPOSED 0x1000 /* array element compression */
#define NXBT_ATTR_FORMAT_DELTA_OF_DELTA	0x2000	/* timestamp delta-of-delta */
#define NXBT_ATTR_FORMAT_CHIMP			0x4000	/* float Chimp compression */
#define NXBT_ATTR_FORMAT_UUID_V7_DELTA	0x8000	/* UUID v7 delta encoding */
#define NXBT_ATTR_VECTOR_QUANTIZED_F16	0x10000	/* vector float16 quantization */

/*
 * Mask of all format flags that describe how datum data is encoded within an
 * attribute array item.  These must be preserved whenever an item is split,
 * merged, exploded, or repacked so that the read path can correctly decode
 * the datum payload.
 */
#define NXBT_ATTR_FORMAT_MASK \
	(NXBT_ATTR_FORMAT_NATIVE_VARLENA | \
	 NXBT_ATTR_FORMAT_FOR | \
	 NXBT_ATTR_BITPACKED | \
	 NXBT_ATTR_FORMAT_DICT | \
	 NXBT_ATTR_FORMAT_FIXED_BIN | \
	 NXBT_ATTR_FORMAT_FSST | \
	 NXBT_ATTR_FORMAT_ARRAY_DECOMPOSED | \
	 NXBT_ATTR_FORMAT_DELTA_OF_DELTA | \
	 NXBT_ATTR_FORMAT_CHIMP | \
	 NXBT_ATTR_FORMAT_UUID_V7_DELTA | \
	 NXBT_ATTR_VECTOR_QUANTIZED_F16)

#define NXBT_ATTR_BITMAPLEN(nelems)		(((int) (nelems) + 7) / 8)

/*
 * Sparse NULL entry: stores the byte offset into the datum data and the
 * number of consecutive NULLs at that logical position.
 */
typedef struct NXSparseNullEntry
{
	uint16		sn_position;	/* element index where the NULL(s) start */
	uint16		sn_count;		/* number of consecutive NULLs */
}			NXSparseNullEntry;

/*
 * RLE NULL entry: encodes runs of NULLs and non-NULLs.
 * The high bit of rle_count indicates NULL (1) vs non-NULL (0).
 * The remaining 15 bits store the run length.
 */
#define NXBT_RLE_NULL_FLAG		0x8000
#define NXBT_RLE_COUNT_MASK		0x7FFF

typedef struct NXRleNullEntry
{
	uint16		rle_count;		/* high bit = is_null, low 15 bits = run
								 * length */
}			NXRleNullEntry;

/*
 * Frame of Reference (FOR) encoding header.
 *
 * When NXBT_ATTR_FORMAT_FOR is set in t_flags, the datum data section begins
 * with this header followed by bit-packed deltas.  Each non-null value is
 * stored as (value - for_frame_min) using for_bits_per_value bits.  Deltas
 * are packed into bytes LSB-first (little-endian bit order).
 *
 * FOR encoding is used only for pass-by-value fixed-width integer types
 * (attlen 1, 2, 4, or 8 with attbyval true) when the range (max - min) can
 * be represented in significantly fewer bits than the original width.
 */
typedef struct NXForHeader
{
	uint64		for_frame_min;	/* minimum value in the frame */
	uint8		for_bits_per_value; /* bits per delta (0..64) */
	uint8		for_attlen;		/* original attribute length (1,2,4,8) */
}			NXForHeader;

/* Packed byte size for n values at given bits-per-value */
#define NXBT_FOR_PACKED_SIZE(nelems, bpv) \
	(((uint64)(nelems) * (bpv) + 7) / 8)

/*
 * Delta-of-delta encoding header for timestamp columns.
 *
 * When NXBT_ATTR_FORMAT_DELTA_OF_DELTA is set in t_flags, the datum data
 * section begins with this header followed by bit-packed delta-of-deltas.
 *
 * Algorithm:
 *   delta[i] = ts[i] - ts[i-1]           (first-order deltas)
 *   dod[i]   = delta[i] - delta[i-1]     (second-order deltas)
 *
 * For regularly-spaced timestamps (e.g. every second), the delta-of-deltas
 * are all zero, yielding extreme compression. The values are stored as
 * signed integers mapped to unsigned via zigzag encoding, then bit-packed.
 *
 * Only applicable to 8-byte pass-by-value types (timestamp, timestamptz,
 * int8) where the data is monotonically increasing and the first-order
 * deltas have low variance.
 */
typedef struct NXDeltaOfDeltaHeader
{
	uint64		dod_initial_value;	/* first timestamp value */
	int64		dod_initial_delta;	/* delta[0] = ts[1] - ts[0] */
	uint8		dod_bits_per_value;	/* bits per zigzag-encoded delta-of-delta */
}			NXDeltaOfDeltaHeader;

/* Packed byte size for delta-of-delta encoded values */
#define NXBT_DOD_PACKED_SIZE(nelems, bpv) \
	(((uint64)(nelems) * (bpv) + 7) / 8)

static inline void
nxbt_attr_item_setnull(uint8 *nullbitmap, int n)
{
	nullbitmap[n / 8] |= (1 << (n % 8));
}

static inline bool
nxbt_attr_item_isnull(uint8 *nullbitmap, int n)
{
	return (nullbitmap[n / 8] & (1 << (n % 8))) != 0;
}

/**
 * @brief TID B-tree leaf page item.
 *
 * Leaf pages in the TID tree are packed with NXTidArrayItems.  Each item
 * represents a group of tuples in the TID range [t_firsttid, t_endtid).
 * For each tuple, the item encodes both the TID (via Simple-8b delta
 * encoding) and an UNDO slot number (2 bits per tuple).
 *
 * @par Physical Layout (variable-length)
 * @code
 *   Header  |  1-16 TID codewords | 0-2 UNDO pointers | UNDO slotwords
 * @endcode
 *
 * @par TID Encoding
 * TID deltas (gaps between consecutive TIDs) are packed into 64-bit
 * Simple-8b codewords.  The first encoded delta is always 0 (the
 * absolute first TID is in t_firsttid).  For consecutive TIDs with
 * no gaps, 60 TIDs fit per codeword (~1 bit/tuple).
 *
 * @par UNDO Slot Encoding
 * There are logically 4 UNDO slots per item:
 * - Slot 0 (NXBT_OLD_UNDO_SLOT): tuple visible to everyone (implicit).
 * - Slot 1 (NXBT_DEAD_UNDO_SLOT): tuple is dead (implicit).
 * - Slots 2-3: explicit UNDO pointer values stored in the item.
 *
 * Each tuple's 2-bit slot number is packed into 64-bit "slotwords"
 * (32 slot numbers per word).  During scans, only the few distinct
 * UNDO pointers in the slots need visibility checking, not every tuple.
 *
 * @param t_size            Total on-disk size of this item in bytes.
 * @param t_num_tids        Number of TIDs encoded in this item.
 * @param t_num_codewords   Number of Simple-8b codewords.
 * @param t_num_undo_slots  Total UNDO slots (including 2 implicit ones).
 * @param t_firsttid        First TID in range (inclusive).
 * @param t_endtid          One past last TID (exclusive).
 * @param t_payload         Flexible array: codewords, then UNDO slots,
 *                          then slotwords.
 */
typedef struct
{
	uint16		t_size;
	uint16		t_num_tids;
	uint16		t_num_codewords;
	uint16		t_num_undo_slots;

	nxtid		t_firsttid;
	nxtid		t_endtid;

	/* Followed by UNDO slots, and then followed by codewords */
	uint64		t_payload[FLEXIBLE_ARRAY_MEMBER];

} NXTidArrayItem;

/**
 * @name UNDO Slot Constants
 * @brief Parameters for the 2-bit UNDO slot encoding used in NXTidArrayItem.
 * @{
 */
#define NXBT_ITEM_UNDO_SLOT_BITS	2	/**< Bits per UNDO slot number. */
#define NXBT_MAX_ITEM_UNDO_SLOTS	(1 << (NXBT_ITEM_UNDO_SLOT_BITS))	/**< Max 4 slots. */
#define NXBT_ITEM_UNDO_SLOT_MASK	(NXBT_MAX_ITEM_UNDO_SLOTS - 1)	/**< 2-bit mask. */
#define NXBT_SLOTNOS_PER_WORD		(64 / NXBT_ITEM_UNDO_SLOT_BITS) /**< 32 slots per uint64. */
/** @} */

/**
 * @name TID Array Item Limits
 * @brief Maximum sizes for NXTidArrayItem to keep item manipulation fast.
 * @{
 */
#define NXBT_MAX_ITEM_CODEWORDS		16	/**< Max Simple-8b codewords per item. */
#define NXBT_MAX_ITEM_TIDS			128 /**< Max TIDs per item. */
/** @} */

/** @brief Implicit slot: tuple is "old" and visible to everyone. */
#define NXBT_OLD_UNDO_SLOT			0
/** @brief Implicit slot: tuple is dead (not visible to anyone). */
#define NXBT_DEAD_UNDO_SLOT			1
/** @brief First physically-stored UNDO slot index. */
#define NXBT_FIRST_NORMAL_UNDO_SLOT	2

/** @brief Number of uint64 slotwords needed for @a num_tids tuples. */
#define NXBT_NUM_SLOTWORDS(num_tids) ((num_tids + NXBT_SLOTNOS_PER_WORD - 1) / NXBT_SLOTNOS_PER_WORD)

static inline size_t
SizeOfNXTidArrayItem(int num_tids, int num_undo_slots, int num_codewords)
{
	Size		sz;

	sz = offsetof(NXTidArrayItem, t_payload);
	sz += num_codewords * sizeof(uint64);
	sz += (num_undo_slots - NXBT_FIRST_NORMAL_UNDO_SLOT) * sizeof(RelUndoRecPtr);
	sz += NXBT_NUM_SLOTWORDS(num_tids) * sizeof(uint64);

	return sz;
}

/*
 * Get pointers to the TID codewords, UNDO slots, and slotwords from an item.
 *
 * Note: this is also used to get the pointers when constructing a new item, so
 * don't assert here that the data is valid!
 */
static inline void
NXTidArrayItemDecode(NXTidArrayItem *item, uint64 **codewords,
					 RelUndoRecPtr **slots, uint64 **slotwords)
{
	char	   *p = (char *) item->t_payload;

	*codewords = (uint64 *) p;
	p += item->t_num_codewords * sizeof(uint64);
	*slots = (RelUndoRecPtr *) p;
	p += (item->t_num_undo_slots - NXBT_FIRST_NORMAL_UNDO_SLOT) * sizeof(RelUndoRecPtr);
	*slotwords = (uint64 *) p;
}

/**
 * @brief Maximum size of a single non-overflow datum in Noxu.
 *
 * Datums exceeding this size are "noxu-overflow": split into chunks and
 * stored on dedicated overflow pages within the same relation file.
 * The threshold accounts for page header, item header, and opaque area.
 */
#define		MaxNoxuDatumSize		(BLCKSZ - 500)

/**
 * @brief Opaque area for Noxu overflow pages.
 *
 * Overflow pages form a doubly-linked list per datum.  The first page in the
 * chain stores the attribute number, owning TID, and total datum size.
 * Subsequent pages store slice offsets.
 *
 * @param nx_attno        Attribute number of the overflow column.
 * @param nx_tid          TID of the owning tuple (first page only).
 * @param nx_total_size   Total uncompressed datum size (first page only).
 * @param nx_slice_offset Byte offset of this chunk within the full datum.
 * @param nx_prev         Previous overflow page (InvalidBlockNumber if first).
 * @param nx_next         Next overflow page (InvalidBlockNumber if last).
 * @param nx_page_id      Always NX_OVERFLOW_PAGE_ID (0xF086).
 */
typedef struct NXOverflowPageOpaque
{
	AttrNumber	nx_attno;

	/* these are only set on the first page. */
	nxtid		nx_tid;
	uint32		nx_total_size;

	uint32		nx_slice_offset;
	BlockNumber nx_prev;
	BlockNumber nx_next;
	uint16		nx_flags;
	uint16		padding1;		/* padding, to put nx_page_id last */
	uint16		padding2;		/* padding, to put nx_page_id last */
	uint16		nx_page_id;
} NXOverflowPageOpaque;

/**
 * @brief In-tree overflow pointer for oversized datums.
 *
 * Stored in place of the actual datum in an attribute array item when the
 * datum has been noxu-overflow.  Must be layout-compatible with
 * varattrib_1b_e so that VARATT_IS_EXTERNAL() recognizes it.
 *
 * @warning These must never escape Noxu code; the rest of PostgreSQL
 *          cannot dereference them.
 *
 * @param va_header  Standard 1-byte varlena header.
 * @param va_tag     Always VARTAG_NOXU (10).
 * @param nxt_block  Block number of the first overflow page.
 */
typedef struct varatt_nx_overflowptr
{
	/* varattrib_1b_e */
	uint8		va_header;
	uint8		va_tag;			/* VARTAG_NOXU in noxu overflow datums */

	/* first block */
	BlockNumber nxt_block;
}			varatt_nx_overflowptr;

/*
 * va_tag value. this should be distinguishable from the values in
 * vartag_external
 */
#define		VARTAG_NOXU		10

/**
 * @brief Noxu-aware version of datumGetSize().
 *
 * Handles Noxu overflow pointers (VARTAG_NOXU) in addition to standard
 * PostgreSQL datum types.
 *
 * @param value    The Datum to measure.
 * @param typByVal Whether the type is pass-by-value.
 * @param typLen   The type's declared length (-1 for varlena, -2 for cstring).
 * @return Size of the datum in bytes.
 */
static inline Size
nx_datumGetSize(Datum value, bool typByVal, int typLen)
{
	if (typLen > 0)
		return typLen;
	else if (typLen == -1)
	{
		struct varlena *vl = (struct varlena *) DatumGetPointer(value);

		if (VARATT_IS_EXTERNAL(vl) && VARTAG_EXTERNAL(vl) == VARTAG_NOXU)
			return sizeof(varatt_nx_overflowptr);
		else
			return VARSIZE_ANY(vl);
	}
	else
		return datumGetSize(value, typByVal, typLen);
}

static inline Datum
nx_datumCopy(Datum value, bool typByVal, int typLen)
{
	if (typLen < 0)
	{
		struct varlena *vl = (struct varlena *) DatumGetPointer(value);

		if (VARATT_IS_EXTERNAL(vl) && VARTAG_EXTERNAL(vl) == VARTAG_NOXU)
		{
			char	   *result = palloc(sizeof(varatt_nx_overflowptr));

			memcpy(result, DatumGetPointer(value), sizeof(varatt_nx_overflowptr));

			return PointerGetDatum(result);
		}
	}
	return datumCopy(value, typByVal, typLen);
}

/** @brief Block number of the metapage (always 0). */
#define NX_META_BLK		0

/**
 * @brief Entry in the metapage's B-tree root directory.
 *
 * The metapage stores one NXRootDirItem per attribute (including the TID
 * tree at index 0).  Each entry points to the root page of the
 * corresponding B-tree and optionally to a shared compression dictionary.
 *
 * @param root            Block number of the B-tree root page.
 * @param dict_page       First page of the shared dictionary chain
 *                        (InvalidBlockNumber if no dictionary).
 * @param dict_generation Monotonically increasing dictionary version counter
 *                        (0 = no dictionary).
 */
typedef struct NXRootDirItem
{
	BlockNumber root;
	BlockNumber dict_page;			/* first page of dictionary chain */
	uint32		dict_generation;	/* dictionary version counter */
} NXRootDirItem;

/**
 * @brief Metapage contents (stored in the page body area).
 *
 * Contains the number of attributes and a flexible array of root directory
 * entries, one per attribute.  Index 0 is the TID tree root.
 *
 * @param nattributes   Number of B-trees (TID tree + user columns).
 * @param tree_root_dir Array of root block pointers, indexed by attno.
 */
typedef struct NXMetaPage
{
	int			nattributes;
	NXRootDirItem tree_root_dir[FLEXIBLE_ARRAY_MEMBER]; /* one for each
														 * attribute */
} NXMetaPage;

/**
 * @brief Metapage opaque area (stored in pd_special).
 *
 * Contains UNDO log head/tail pointers, the oldest live UNDO record,
 * and the Free Page Map head.  The nx_page_id field allows tools like
 * pg_filedump to identify the page type.
 *
 * @param nx_undo_head                Oldest UNDO log page.
 * @param nx_undo_tail                Newest UNDO log page (insertion point).
 * @param nx_undo_tail_first_counter  Counter of the first record on tail page.
 * @param nx_undo_oldestptr           Oldest UNDO record still needed by any snapshot.
 * @param nx_fpm_head                 Head of the Free Page Map linked list.
 * @param nx_page_id                  Always NX_META_PAGE_ID (0xF083).
 */
typedef struct NXMetaPageOpaque
{
	/*
	 * Deprecated: These fields are no longer used. Per-relation UNDO is now
	 * handled by the RelUndo subsystem in a separate UNDO fork.
	 *
	 * Head and tail page of the UNDO log.
	 *
	 * 'nx_undo_tail' is the newest page, where new UNDO records will be
	 * inserted, and 'nx_undo_head' is the oldest page.
	 * 'nx_undo_tail_first_counter' is the UNDO counter value of the first
	 * record on the tail page (or if the tail page is empty, the counter
	 * value the first record on the tail page will have, when it's inserted.)
	 * If there is no UNDO log at all, 'nx_undo_tail_first_counter' is the new
	 * counter value to use. It's actually redundant, except when there is no
	 * UNDO log at all, but it's a nice cross-check at other times.
	 */
	BlockNumber nx_undo_head;
	BlockNumber nx_undo_tail;
	uint64		nx_undo_tail_first_counter;

	/*
	 * Deprecated: Oldest UNDO record that is still needed. Anything older
	 * than this can be discarded, and considered as visible to everyone.
	 */
	RelUndoRecPtr nx_undo_oldestptr;

	BlockNumber nx_fpm_head;	/* head of the Free Page Map list */

	uint16		nx_flags;
	uint16		nx_page_id;
} NXMetaPageOpaque;

/*
 * Access macros for the LSM metadata block pointer.
 *
 * The LSM metadata block number is stored in the deprecated nx_undo_head
 * field of NXMetaPageOpaque, repurposing it without changing the struct
 * size.  This preserves binary compatibility with existing on-disk pages.
 */
#define NXMetaGetLSMMetaBlock(opaque)		((opaque)->nx_undo_head)
#define NXMetaSetLSMMetaBlock(opaque, blk)	((opaque)->nx_undo_head = (blk))

/**
 * @brief Non-vacuumable status codes for Noxu visibility checks.
 */
typedef enum
{
	NXNV_NONE,					/**< Tuple is vacuumable or live. */
	NXNV_RECENTLY_DEAD			/**< Tuple is dead but not yet deletable. */
} NXNV_Result;

/**
 * @brief Cached visibility information for an UNDO slot.
 *
 * During TID tree scans, the few distinct UNDO pointers in each item's
 * slots are checked against the snapshot once, and the results are cached
 * here.  This avoids per-tuple UNDO record lookups.
 *
 * @param xmin               Inserting transaction ID.
 * @param xmax               Deleting/updating transaction ID.
 * @param cmin               Command ID within xmin's transaction.
 * @param speculativeToken   Token for speculative insertions (0 if none).
 * @param nonvacuumable_status Whether the tuple is recently dead.
 */
typedef struct NXUndoSlotVisibility
{
	TransactionId xmin;
	TransactionId xmax;
	CommandId	cmin;
	uint32		speculativeToken;
	NXNV_Result nonvacuumable_status;
} NXUndoSlotVisibility;

static const NXUndoSlotVisibility InvalidUndoSlotVisibility = {
	.xmin = InvalidTransactionId,
	.xmax = InvalidTransactionId,
	.cmin = InvalidCommandId,
	.speculativeToken = INVALID_SPECULATIVE_TOKEN,
	.nonvacuumable_status = NXNV_NONE
};

/**
 * @brief Iterator state for unpacking a single NXTidArrayItem.
 *
 * Holds the decoded TIDs, their UNDO slot assignments, and cached
 * visibility for each slot.
 */
typedef struct NXTidItemIterator
{
	int			tids_allocated_size;
	nxtid	   *tids;
	uint8	   *tid_undoslotnos;
	int			num_tids;
	MemoryContext context;

	RelUndoRecPtr undoslots[NXBT_MAX_ITEM_UNDO_SLOTS];
	NXUndoSlotVisibility undoslot_visibility[NXBT_MAX_ITEM_UNDO_SLOTS];
} NXTidItemIterator;

/**
 * @brief State for an in-progress scan on the TID tree.
 *
 * Created by nxbt_tid_begin_scan() and destroyed by nxbt_tid_end_scan().
 * The scan walks TID tree leaf pages, decoding NXTidArrayItems and
 * checking visibility against the provided snapshot.
 *
 * @param rel         The relation being scanned.
 * @param context     Long-lived memory context for scan allocations.
 * @param active      Whether the scan is currently positioned.
 * @param lastbuf     Last buffer accessed (held with share lock during scan).
 * @param snapshot    Visibility snapshot for tuple filtering.
 * @param starttid    Lower bound of the TID range to scan (inclusive).
 * @param endtid      Upper bound of the TID range to scan (exclusive).
 * @param currtid     Last TID returned by nxbt_tid_scan_next().
 * @param recent_oldest_undo  Oldest UNDO record still needed.
 * @param serializable        Whether to acquire predicate locks.
 */
typedef struct NXTidTreeScan
{
	Relation	rel;

	/*
	 * memory context that should be used for any allocations that go with the
	 * scan, like the decompression buffers. This isn't a dedicated context,
	 * you must still free everything to avoid leaking! We need this because
	 * the getnext function might be called in a short-lived memory context
	 * that is reset between calls.
	 */
	MemoryContext context;

	bool		active;
	Buffer		lastbuf;
	OffsetNumber lastoff;
	Snapshot	snapshot;

	/*
	 * starttid and endtid define a range of TIDs to scan. currtid is the
	 * previous TID that was returned from the scan. They determine what
	 * nxbt_tid_scan_next() will return.
	 */
	nxtid		starttid;
	nxtid		endtid;
	nxtid		currtid;

	/* in the "real" UNDO-log, this would probably be a global variable */
	RelUndoRecPtr recent_oldest_undo;

	/* should this scan do predicate locking? Or check for conflicts? */
	bool		serializable;
	bool		acquire_predicate_tuple_locks;

	/*
	 * These fields are used, when the scan is processing an array item.
	 */
	NXTidItemIterator array_iter;
	int			array_curr_idx;
}			NXTidTreeScan;

/**
 * @brief Get the UNDO slot number of the current TID in a TID tree scan.
 *
 * Must be called after nxbt_tid_scan_next() has returned a valid TID.
 * The result indexes into scan->array_iter.undoslots[] and
 * scan->array_iter.undoslot_visibility[].
 *
 * @param scan  Active TID tree scan.
 * @return The 2-bit UNDO slot number (0-3) for the current TID.
 */
static inline uint8
NXTidScanCurUndoSlotNo(NXTidTreeScan * scan)
{
	Assert(scan->array_curr_idx >= 0 && scan->array_curr_idx < scan->array_iter.num_tids);
	Assert(scan->array_iter.tid_undoslotnos != NULL);
	return (scan->array_iter.tid_undoslotnos[scan->array_curr_idx]);
}

/**
 * @brief State for an in-progress scan on an Noxu attribute B-tree.
 *
 * Created by nxbt_attr_begin_scan() and destroyed by nxbt_attr_end_scan().
 * The scan walks attribute tree leaf pages, decompressing and decoding
 * NXAttributeArrayItem entries into arrays of Datums.
 *
 * @param rel      The relation being scanned.
 * @param attno    Attribute number (1-based, matching pg_attribute).
 * @param attdesc  Cached attribute descriptor from the tuple descriptor.
 * @param context  Long-lived memory context for decompression buffers.
 * @param active   Whether the scan is currently positioned.
 * @param lastbuf  Last buffer accessed.
 * @param array_datums      Decoded datum values for the current item.
 * @param array_isnulls     NULL flags for the current item.
 * @param array_tids        TIDs for the current item.
 * @param array_num_elements Number of elements in the current decoded item.
 * @param decompress_buf    Working buffer for page decompression.
 * @param attr_buf          Working buffer for item extraction.
 */
typedef struct NXAttrTreeScan
{
	Relation	rel;
	AttrNumber	attno;
	Form_pg_attribute attdesc;

	/*
	 * memory context that should be used for any allocations that go with the
	 * scan, like the decompression buffers. This isn't a dedicated context,
	 * you must still free everything to avoid leaking! We need this because
	 * the getnext function might be called in a short-lived memory context
	 * that is reset between calls.
	 */
	MemoryContext context;

	bool		active;
	Buffer		lastbuf;
	OffsetNumber lastoff;

	/*
	 * These fields are used, when the scan is processing an array tuple. They
	 * are filled in by nxbt_attr_item_extract().
	 */
	int			array_datums_allocated_size;
	Datum	   *array_datums;
	bool	   *array_isnulls;
	nxtid	   *array_tids;
	int			array_num_elements;

	int			array_curr_idx;

	/*
	 * Hint TID: when set, nxbt_attr_item_extract() uses binary search
	 * to pre-position array_curr_idx so that subsequent linear scans in
	 * nxbt_attr_fetch() can skip leading elements.  Set to InvalidNXTid
	 * to disable.
	 */
	nxtid		extract_hint_tid;

	/* working areas for nxbt_attr_item_extract() */
	char	   *decompress_buf;
	int			decompress_buf_size;
	char	   *attr_buf;
	int			attr_buf_size;

}			NXAttrTreeScan;

/**
 * @brief Backend-private cache of metapage information.
 *
 * Stored in RelationData->rd_amcache.  Contains B-tree root block numbers
 * and rightmost leaf pointers for fast lookups and end-of-tree insertions.
 *
 * Validity is tied to smgr_targblock: the cache is invalidated whenever
 * an smgr invalidation occurs (e.g., relation extension by another backend).
 * Use nxmeta_get_cache() to access; it auto-populates on first use.
 *
 * @param cache_nattributes  Number of attributes (including TID tree).
 * @param cache_attrs        Per-attribute root, rightmost leaf, and lokey.
 */
typedef struct NXMetaCacheData
{
	int			cache_nattributes;
	BlockNumber cache_lsm_meta;	/**< Block of LSM metadata page, or Invalid. */

	/** @brief Per-attribute cache entry. */
	struct
	{
		BlockNumber root;		/**< Root block of this attribute's B-tree. */
		BlockNumber rightmost;	/**< Rightmost leaf page (for fast appends). */
		nxtid		rightmost_lokey;	/**< Lokey of the rightmost leaf. */
		BlockNumber dict_page;	/**< First page of shared dictionary chain. */
		uint32		dict_generation;	/**< Dictionary version counter. */
	}			cache_attrs[FLEXIBLE_ARRAY_MEMBER];

} NXMetaCacheData;

/**
 * @brief Populate the metapage cache by reading block 0.
 * @param rel  The Noxu relation.
 * @return Pointer to the newly populated NXMetaCacheData.
 */
extern NXMetaCacheData *nxmeta_populate_cache(Relation rel);

/**
 * @brief Get the cached metapage data, populating it if necessary.
 * @param rel  The Noxu relation.
 * @return Pointer to the NXMetaCacheData in rel->rd_amcache.
 */
static inline NXMetaCacheData *
nxmeta_get_cache(Relation rel)
{
	if (rel->rd_amcache == NULL || RelationGetTargetBlock(rel) == InvalidBlockNumber)
		nxmeta_populate_cache(rel);
	return (NXMetaCacheData *) rel->rd_amcache;
}

/**
 * @brief Invalidate the cached metapage data.
 *
 * The next call to nxmeta_get_cache() will re-read the metapage.
 *
 * @param rel  The Noxu relation.
 */
static inline void
nxmeta_invalidate_cache(Relation rel)
{
	if (rel->rd_amcache != NULL)
	{
		pfree(rel->rd_amcache);
		rel->rd_amcache = NULL;
	}
}

/**
 * @brief Linked list of pages modified during a B-tree page split or merge.
 *
 * Split/merge routines construct a list of nx_split_stack entries rather
 * than modifying pages directly.  Each entry holds an exclusively-locked
 * buffer and a temporary in-memory copy of the new page contents.  Once
 * the entire operation is prepared, nx_apply_split_changes() writes all
 * pages atomically with WAL protection.
 *
 * @param next     Next entry in the stack.
 * @param buf      Exclusively-locked buffer.
 * @param page     Temporary in-memory copy of the page to write.
 * @param recycle  If true, add this page to the FPM after the operation.
 */
typedef struct nx_split_stack nx_split_stack;

struct nx_split_stack
{
	nx_split_stack *next;

	Buffer		buf;
	Page		page;			/* temp in-memory copy of page */
	bool		recycle;		/* should the page be added to the FPM? */
};

/*
 * NXBtreePathEntry - B-tree descent path tracking
 *
 * Tracks the sequence of pages visited during a B-tree descent so that
 * after a leaf-level split, we can walk back up the remembered path to
 * find the parent instead of re-descending from the root.
 *
 * Entries are allocated in CurrentMemoryContext and form a singly-linked
 * list from leaf towards root (child->parent).  The caller is responsible
 * for freeing the list with nxbt_free_path().
 */
typedef struct NXBtreePathEntry
{
	BlockNumber blkno;			/* block number of the visited page */
	int			level;			/* B-tree level (0 = leaf) */
	struct NXBtreePathEntry *parent;	/* next entry towards root */
} NXBtreePathEntry;

/* Free a path stack */
static inline void
nxbt_free_path(NXBtreePathEntry *path)
{
	while (path)
	{
		NXBtreePathEntry *parent = path->parent;

		pfree(path);
		path = parent;
	}
}

/* prototypes for functions in noxu_tidpage.c */
extern void nxbt_tid_begin_scan(Relation rel, nxtid starttid, nxtid endtid,
								Snapshot snapshot, NXTidTreeScan * scan);
extern void nxbt_tid_reset_scan(Relation rel, NXTidTreeScan * scan, nxtid starttid, nxtid endtid, nxtid currtid);
extern void nxbt_tid_end_scan(NXTidTreeScan * scan);
extern bool nxbt_tid_scan_next_array(NXTidTreeScan * scan, nxtid nexttid, ScanDirection direction);

/*
 * Return the next TID in the scan.
 *
 * The next TID means the first TID > scan->currtid. Each call moves
 * scan->currtid to the last returned TID. You can call nxbt_tid_reset_scan()
 * to change the position, scan->starttid and scan->endtid define the
 * boundaries of the search.
 */
static inline nxtid
nxbt_tid_scan_next(NXTidTreeScan * scan, ScanDirection direction)
{
	nxtid		nexttid;
	int			idx;

	Assert(scan->active);

	if (direction == ForwardScanDirection)
		nexttid = scan->currtid + 1;
	else if (direction == BackwardScanDirection)
		nexttid = scan->currtid - 1;
	else
		nexttid = scan->currtid;

	if (scan->array_iter.num_tids == 0 ||
		nexttid < scan->array_iter.tids[0] ||
		nexttid > scan->array_iter.tids[scan->array_iter.num_tids - 1])
	{
		scan->array_curr_idx = -1;
		if (!nxbt_tid_scan_next_array(scan, nexttid, direction))
		{
			scan->currtid = nexttid;
			return InvalidNXTid;
		}
	}

	/*
	 * Optimize for the common case that we're scanning forward from the
	 * previous TID.
	 */
	if (scan->array_curr_idx >= 0 && scan->array_iter.tids[scan->array_curr_idx] < nexttid)
		idx = scan->array_curr_idx + 1;
	else
		idx = 0;

	for (; idx < scan->array_iter.num_tids; idx++)
	{
		nxtid		this_tid = scan->array_iter.tids[idx];

		if (this_tid >= scan->endtid)
		{
			scan->currtid = nexttid;
			return InvalidNXTid;
		}

		if (this_tid >= nexttid)
		{
			/*
			 * Callers using SnapshotDirty need some extra visibility
			 * information.
			 */
			if (scan->snapshot->snapshot_type == SNAPSHOT_DIRTY)
			{
				int			slotno = scan->array_iter.tid_undoslotnos[idx];
				NXUndoSlotVisibility *visi_info = &scan->array_iter.undoslot_visibility[slotno];

				if (visi_info->xmin != FrozenTransactionId)
					scan->snapshot->xmin = visi_info->xmin;
				scan->snapshot->xmax = visi_info->xmax;
				scan->snapshot->speculativeToken = visi_info->speculativeToken;
			}

			/* on next call, continue the scan at the next TID */
			scan->currtid = this_tid;
			scan->array_curr_idx = idx;
			return this_tid;
		}
	}

	/*
	 * unreachable, because nxbt_tid_scan_next_array() should never return an
	 * array that doesn't contain a matching TID.
	 */
	Assert(false);
	return InvalidNXTid;
}


extern TM_Result nxbt_tid_delta_update(Relation rel, nxtid otid,
									   TransactionId xid, CommandId cid,
									   bool key_update, Snapshot snapshot,
									   Snapshot crosscheck, bool wait,
									   TM_FailureData *hufd,
									   nxtid * newtid_p,
									   bool *this_xact_has_lock,
									   int natts, const bool *changed_cols);
extern void nxbt_tid_delta_insert(Relation rel, nxtid * tids,
								  TransactionId xid, CommandId cid,
								  nxtid predecessor_tid,
								  int natts, const bool *changed_cols,
								  RelUndoRecPtr prevundoptr);
extern void nxbt_tid_multi_insert(Relation rel,
								  nxtid * tids, int ntuples,
								  TransactionId xid, CommandId cid,
								  uint32 speculative_token, RelUndoRecPtr prevundoptr);
extern TM_Result nxbt_tid_delete(Relation rel, nxtid tid,
								 TransactionId xid, CommandId cid,
								 Snapshot snapshot, Snapshot crosscheck, bool wait,
								 TM_FailureData *hufd, bool changingPart, bool *this_xact_has_lock);
extern TM_Result nxbt_tid_update(Relation rel, nxtid otid,
								 TransactionId xid,
								 CommandId cid, bool key_update, Snapshot snapshot, Snapshot crosscheck,
								 bool wait, TM_FailureData *hufd, nxtid * newtid_p, bool *this_xact_has_lock);
extern void nxbt_tid_clear_speculative_token(Relation rel, nxtid tid, uint32 spectoken, bool forcomplete);
extern void nxbt_tid_mark_dead(Relation rel, nxtid tid, RelUndoRecPtr recent_oldest_undo);
extern IntegerSet *nxbt_collect_dead_tids(Relation rel, nxtid starttid, nxtid * endtid, uint64 *num_live_tuples);
extern void nxbt_tid_remove(Relation rel, IntegerSet *tids);
extern TM_Result nxbt_tid_lock(Relation rel, nxtid tid,
							   TransactionId xid, CommandId cid,
							   LockTupleMode lockmode, bool follow_updates,
							   Snapshot snapshot, TM_FailureData *hufd,
							   nxtid * next_tid, bool *this_xact_has_lock,
							   NXUndoSlotVisibility *visi_info);
extern void nxbt_tid_undo_deletion(Relation rel, nxtid tid, RelUndoRecPtr undoptr, RelUndoRecPtr recent_oldest_undo);
extern nxtid nxbt_get_last_tid(Relation rel);
extern void nxbt_find_latest_tid(Relation rel, nxtid * tid, Snapshot snapshot);
extern void nxbt_tid_mark_updated_for_cluster(Relation rel, nxtid otid,
											  nxtid newtid, TransactionId xid,
											  CommandId cid, bool key_update);

/* prototypes for functions in noxu_tiditem.c */
extern List *nxbt_tid_item_create_for_range(nxtid tid, int nelements, RelUndoRecPtr undo_ptr);
extern List *nxbt_tid_item_add_tids(NXTidArrayItem *orig, nxtid firsttid, int nelements,
									RelUndoRecPtr undo_ptr, bool *modified_orig);
extern void nxbt_tid_item_unpack(NXTidArrayItem *item, NXTidItemIterator *iter);
extern List *nxbt_tid_item_change_undoptr(NXTidArrayItem *orig, nxtid target_tid, RelUndoRecPtr undoptr, RelUndoRecPtr recent_oldest_undo);
extern List *nxbt_tid_item_remove_tids(NXTidArrayItem *orig, nxtid * nexttid, IntegerSet *remove_tids,
									   RelUndoRecPtr recent_oldest_undo);
extern List *nxbt_tid_combine_adjacent_items(List *items);


/* prototypes for functions in noxu_attpage.c */
extern void nxbt_attr_begin_scan(Relation rel, TupleDesc tdesc, AttrNumber attno,
								 NXAttrTreeScan * scan);
extern void nxbt_attr_end_scan(NXAttrTreeScan * scan);
extern bool nxbt_attr_scan_fetch_array(NXAttrTreeScan * scan, nxtid tid);

extern void nxbt_attr_multi_insert(Relation rel, AttrNumber attno,
								   Datum *datums, bool *isnulls, nxtid * tids, int ndatums);

/* prototypes for functions in noxu_attitem.c */
extern List *nxbt_attr_create_items(Form_pg_attribute att,
									Datum *datums, bool *isnulls, nxtid * tids, int nelements);
extern void nxbt_split_item(Form_pg_attribute attr, NXExplodedItem * origitem, nxtid first_right_tid,
							NXExplodedItem * *leftitem_p, NXExplodedItem * *rightitem_p,
							Relation rel, AttrNumber attno);
extern NXExplodedItem * nxbt_attr_remove_from_item(Form_pg_attribute attr,
												   NXAttributeArrayItem * olditem,
												   nxtid * removetids,
												   Relation rel, AttrNumber attno);
extern List *nxbt_attr_recompress_items(Form_pg_attribute attr, List *olditems,
										Relation rel, AttrNumber attno);

extern void nxbt_attr_item_extract(NXAttrTreeScan * scan, NXAttributeArrayItem * item);


/* prototypes for functions in noxu_btree.c */
extern nx_split_stack * nxbt_newroot(Relation rel, AttrNumber attno, int level, List *downlinks);
extern nx_split_stack * nxbt_insert_downlinks(Relation rel, AttrNumber attno,
											  nxtid leftlokey, BlockNumber leftblkno, int level,
											  List *downlinks, Buffer held_buf,
											  NXBtreePathEntry *path);
extern void nxbt_attr_remove(Relation rel, AttrNumber attno, IntegerSet *tids);
extern nx_split_stack * nxbt_unlink_page(Relation rel, AttrNumber attno, Buffer buf, int level);
extern nx_split_stack * nx_new_split_stack_entry(Buffer buf, Page page);
extern void nx_apply_split_changes(Relation rel, nx_split_stack * stack, nx_pending_undo_op * undo_op);
extern Buffer nxbt_descend(Relation rel, AttrNumber attno, nxtid key, int level, bool readonly, bool for_update, Buffer held_buf, Buffer held_buf2);
extern Buffer nxbt_descend_with_path(Relation rel, AttrNumber attno, nxtid key,
									 int level, bool readonly, bool for_update,
									 Buffer held_buf, Buffer held_buf2,
									 NXBtreePathEntry **path_out);
extern Buffer nxbt_find_and_lock_leaf_containing_tid(Relation rel, AttrNumber attno,
													 Buffer buf, nxtid nexttid, int lockmode);
extern bool nxbt_page_is_expected(Relation rel, AttrNumber attno, nxtid key, int level, Buffer buf);
extern void nxbt_wal_log_leaf_items(Relation rel, AttrNumber attno, Buffer buf, OffsetNumber off, bool replace, List *items, nx_pending_undo_op * undo_op);
extern void nxbt_wal_log_rewrite_pages(Relation rel, AttrNumber attno, List *buffers, nx_pending_undo_op * undo_op, uint32 recycle_bitmap, BlockNumber old_fpm_head, Buffer metabuf);

/*
 * WAL UNDO operation support functions
 * These handle UNDO operations during WAL logging and replay.
 */
typedef struct nx_wal_undo_op
{
	RelUndoRecPtr undoptr;
	uint16		length;
	bool		is_update;
	char		payload[FLEXIBLE_ARRAY_MEMBER];
}			pg_attribute_packed() nx_wal_undo_op;
#define SizeOfNXWalUndoOp	offsetof(nx_wal_undo_op, payload)

extern void XLogRegisterUndoOp(uint8 block_id, nx_pending_undo_op * undo_op);
extern Buffer XLogRedoUndoOp(XLogReaderState *record, uint8 block_id);

/*
 * UNDO visibility helpers
 */
extern RelUndoRecPtr nx_get_oldest_visible_undo_ptr(Relation rel);

/*
 * Stub functions for unimplemented UNDO operations
 */
extern void nxundo_clear_speculative_token(Relation rel, RelUndoRecPtr undoptr);
extern void nxundo_vacuum(Relation rel, struct VacuumParams *params, BufferAccessStrategy bstrategy);

/*
 * Return the value of row identified with 'tid' in a scan.
 *
 * 'tid' must be greater than any previously returned item.
 *
 * Returns true if a matching item is found, false otherwise. After
 * a false return, it's OK to call this again with another greater TID.
 */
static inline bool
nxbt_attr_fetch(NXAttrTreeScan * scan, Datum *datum, bool *isnull, nxtid tid)
{
	int			idx;

	/*
	 * Fetch the next item from the scan. The item we're looking for might
	 * already be in scan->array_*.
	 */
	if (scan->array_num_elements == 0 ||
		tid < scan->array_tids[0] ||
		scan->array_tids[scan->array_num_elements - 1] < tid)
	{
		if (!nxbt_attr_scan_fetch_array(scan, tid))
			return false;
		scan->array_curr_idx = -1;
	}
	Assert(scan->array_num_elements > 0 &&
		   scan->array_tids[0] <= tid &&
		   scan->array_tids[scan->array_num_elements - 1] >= tid);

	/*
	 * Optimize for the common case that we're scanning forward from the
	 * previous TID.
	 */
	if (scan->array_curr_idx != -1 && scan->array_tids[scan->array_curr_idx] < tid)
		idx = scan->array_curr_idx + 1;
	else
		idx = 0;

	for (; idx < scan->array_num_elements; idx++)
	{
		nxtid		this_tid = scan->array_tids[idx];

		if (this_tid == tid)
		{
			*isnull = scan->array_isnulls[idx];
			*datum = scan->array_datums[idx];
			scan->array_curr_idx = idx;
			return true;
		}
		if (this_tid > tid)
			return false;
	}

	return false;
}

extern PGDLLIMPORT const TupleTableSlotOps TTSOpsNoxu;

/* prototypes for functions in noxu_meta.c */
extern void nxmeta_initmetapage(Relation rel);
extern void nxmeta_initmetapage_redo(XLogReaderState *record);
extern BlockNumber nxmeta_get_root_for_attribute(Relation rel, AttrNumber attno, bool for_update);
extern void nxmeta_add_root_for_new_attributes(Relation rel, Page page);

/* prototypes for functions in noxu_visibility.c */
extern TM_Result nx_SatisfiesUpdate(Relation rel, Snapshot snapshot,
									RelUndoRecPtr recent_oldest_undo,
									nxtid item_tid, RelUndoRecPtr item_undoptr,
									LockTupleMode mode,
									bool *undo_record_needed, bool *this_xact_has_lock,
									TM_FailureData *tmfd, nxtid * next_tid,
									NXUndoSlotVisibility *visi_info);
extern bool nx_SatisfiesVisibility(NXTidTreeScan * scan, RelUndoRecPtr item_undoptr,
								   TransactionId *obsoleting_xid, nxtid * next_tid,
								   NXUndoSlotVisibility *visi_info);

/* prototypes for functions in noxu_overflow.c */
extern Datum noxu_overflow_datum(Relation rel, AttrNumber attno, Datum value, nxtid tid);
extern Datum noxu_overflow_flatten(Relation rel, AttrNumber attno, nxtid tid, Datum overflowed);

/* prototypes for column-delta UPDATE support in noxu_handler.c */
extern void nx_materialize_delta_columns(Relation rel,
										 nxtid newtid,
										 nxtid predecessor_tid,
										 int natts,
										 const uint32 *changed_cols);

/* prototypes for functions in noxu_freepagemap.c */
extern Buffer nxpage_getnewbuf(Relation rel, Buffer metabuf);
extern Buffer nxpage_extendrel_newbuf(Relation rel, Buffer metabuf);
extern void nxpage_mark_page_deleted(Page page, BlockNumber next_free_blk);
extern void nxpage_delete_page(Relation rel, Buffer buf);
extern void nxfpm_flush_pending_deletes(void);

typedef struct NoxuTupleTableSlot
{
	TupleTableSlot base;

	char	   *data;			/* data for materialized slots */

	/*
	 * Extra visibility information. The tuple's xmin and cmin can be
	 * extracted from here; this is used by trigger code to populate the
	 * "old" tuple's system columns (tg_trigtuple->t_data->t_choice.t_heap).
	 * There's also a flag to indicate if a tuple is vacuumable or not, which
	 * can be useful if you're scanning with SnapshotAny. That's currently
	 * used in index build.
	 */
	NXUndoSlotVisibility *visi_info;

	/*
	 * Normally, when a tuple is retrieved from a table, 'visi_info' points to
	 * TID tree scan's data structures. But sometimes it's useful to keep the
	 * information together with the slot, e.g. whe a slot is copied, so that
	 * it doesn't depend on any data outside the slot. In that case, you can
	 * fill in 'visi_info_buf', and set visi_info = &visi_info_buf.
	 */
	NXUndoSlotVisibility visi_info_buf;
}			NoxuTupleTableSlot;

/* TableAM methods (defined in noxu_handler.c) */
extern const TableAmRoutine noxuam_methods;

/* prototypes for functions in noxu_rollback.c */
extern void NoxuRelUndoApplyChain(Relation rel, RelUndoRecPtr start_ptr);

/*
 * UNDO compatibility layer - forward declarations for functions still using
 * bespoke UNDO implementation. These should be converted to RelUndo API.
 */
struct NXUndoRec;
struct VacuumParams;
extern struct NXUndoRec *nxundo_fetch_record(Relation rel, RelUndoRecPtr undoptr);

#endif							/* NOXU_INTERNAL_H */
