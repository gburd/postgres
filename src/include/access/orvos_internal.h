/**
 * @file orvos_internal.h
 * @brief Internal declarations for Orvos columnar table access method.
 *
 * This header defines the core data structures for Orvos's on-disk page
 * formats, B-tree page layouts, TID and attribute array items, metapage
 * structures, scan state, and cache structures.  It is the central header
 * for all Orvos backend code.
 *
 * @par Architecture Overview
 * An Orvos relation consists of multiple B-trees stored in a single
 * physical file.  Block 0 is always a metapage.  The TID tree (attribute
 * number 0) stores visibility/UNDO information.  Each user column has its
 * own attribute B-tree.  UNDO log pages, toast pages, and free pages are
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
 *   changes are applied atomically via ov_apply_split_changes().
 *
 * @par Memory Context
 * Scan structures (OVTidTreeScan, OVAttrTreeScan) carry a MemoryContext
 * field that must be used for any allocations that outlive a single
 * getnext() call.  The caller's CurrentMemoryContext may be short-lived.
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/include/access/orvos_internal.h
 */
#ifndef ORVOS_INTERNAL_H
#define ORVOS_INTERNAL_H

#include "access/tableam.h"
#include "access/orvos_compression.h"
#include "access/orvos_tid.h"
#include "access/orvos_undolog.h"
#include "lib/integerset.h"
#include "storage/bufmgr.h"
#include "storage/smgr.h"
#include "utils/datum.h"

struct ov_pending_undo_op;

/** @brief Attribute number used for the TID tree (visibility metadata). */
#define OV_META_ATTRIBUTE_NUM 0

/** @brief Sentinel value indicating no speculative insertion token. */
#define INVALID_SPECULATIVE_TOKEN 0

/**
 * @name Page Type Identifiers
 * @brief Magic numbers stored in the opaque area of each page to identify
 *        the page type.  Every page in an Orvos relation carries one of
 *        these in its ov_page_id field.
 * @{
 */
#define	OV_META_PAGE_ID		0xF083
#define	OV_BTREE_PAGE_ID	0xF084
#define	OV_UNDO_PAGE_ID		0xF085
#define	OV_TOAST_PAGE_ID	0xF086
#define	OV_FREE_PAGE_ID		0xF087
/** @} */

/** @brief Flag indicating this B-tree page is the root of its tree. */
#define OVBT_ROOT				0x0001

/**
 * @brief Opaque area at the end of every Orvos B-tree page.
 *
 * Stored in the pd_special region of the standard PageHeaderData.
 * Contains enough information to identify the page (attribute number,
 * key range, level) so that the page's parent downlink can be relocated
 * after a concurrent split, and so that corruption can be detected.
 *
 * @param ov_attno   Attribute number (0 = TID tree, 1..N = user columns).
 * @param ov_next    Right sibling block number (InvalidBlockNumber if rightmost).
 * @param ov_lokey   Inclusive lower bound TID for keys on this page.
 * @param ov_hikey   Exclusive upper bound TID for keys on this page.
 * @param ov_level   B-tree level: 0 = leaf, >0 = internal.
 * @param ov_flags   Combination of OVBT_ROOT and other flags.
 * @param ov_page_id Always OV_BTREE_PAGE_ID (0xF084).
 */
typedef struct OVBtreePageOpaque
{
	AttrNumber	ov_attno;
	BlockNumber ov_next;
	ovtid		ov_lokey;		/* inclusive */
	ovtid		ov_hikey;		/* exclusive */
	uint16		ov_level;		/* 0 = leaf */
	uint16		ov_flags;
	uint16		padding;		/* padding, to put ov_page_id last */
	uint16		ov_page_id;		/* always OV_BTREE_PAGE_ID */
} OVBtreePageOpaque;

/**
 * @brief Extract the OVBtreePageOpaque from a page's special area.
 * @param page  A Page pointer to a B-tree page.
 * @return Pointer to the OVBtreePageOpaque structure.
 */
#define OVBtreePageGetOpaque(page) ((OVBtreePageOpaque *) PageGetSpecialPointer(page))

/**
 * @brief Internal (non-leaf) B-tree page item.
 *
 * The page contents between pd_upper and pd_special consist of an array
 * of these items.  The number of items is deduced from pd_lower:
 *   num = (pd_lower - SizeOfPageHeaderData) / sizeof(OVBtreeInternalPageItem)
 *
 * @param tid       Separator key (first TID in the right subtree).
 * @param childblk  Block number of the child page.
 */
typedef struct OVBtreeInternalPageItem
{
	ovtid		tid;
	BlockNumber childblk;
} OVBtreeInternalPageItem;

/**
 * @brief Get pointer to the array of internal page items.
 * @param page  A Page containing internal B-tree items.
 * @return Pointer to the first OVBtreeInternalPageItem.
 */
static inline OVBtreeInternalPageItem *
OVBtreeInternalPageGetItems(Page page)
{
	OVBtreeInternalPageItem *items;

	items = (OVBtreeInternalPageItem *) PageGetContents(page);

	return items;
}

/**
 * @brief Get the number of items on an internal B-tree page.
 * @param page  A Page containing internal B-tree items.
 * @return Number of OVBtreeInternalPageItem entries on the page.
 */
static inline int
OVBtreeInternalPageGetNumItems(Page page)
{
	OVBtreeInternalPageItem *begin;
	OVBtreeInternalPageItem *end;

	begin = (OVBtreeInternalPageItem *) PageGetContents(page);
	end = (OVBtreeInternalPageItem *) ((char *) page + ((PageHeader) page)->pd_lower);

	return end - begin;
}

/**
 * @brief Check whether an internal B-tree page has room for another item.
 * @param page  A Page containing internal B-tree items.
 * @return true if pd_upper - pd_lower is too small for another item.
 */
static inline bool
OVBtreeInternalPageIsFull(Page page)
{
	PageHeader	phdr = (PageHeader) page;

	return phdr->pd_upper - phdr->pd_lower < sizeof(OVBtreeInternalPageItem);
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
 * - NULL bitmap (ceil(t_num_elements/8) bytes), if OVBT_HAS_NULLS
 * - Packed datum data (see below)
 *
 * @par Datum Encoding
 * Fixed-width types are stored without alignment padding.  Variable-length
 * types use a custom compact encoding instead of standard PostgreSQL
 * varlena format:
 * - @c 0xxxxxxx : 1-byte header, up to 128 bytes of data follow.
 * - @c 1xxxxxxx @c xxxxxxxx : 2-byte header, up to 32767 bytes.
 * - @c 0xFF @c 0xFF @c <BlockNumber> : Orvos toast pointer (datum on
 *   separate toast pages within the same relation file).
 *
 * @param t_size          Total on-disk size of this item in bytes.
 * @param t_flags         Bitmask: OVBT_ATTR_COMPRESSED, OVBT_HAS_NULLS.
 * @param t_num_elements  Number of datums (tuples) in this item.
 * @param t_num_codewords Number of Simple-8b codewords for TID deltas.
 * @param t_firsttid      First TID in the range (inclusive).
 * @param t_endtid        One past the last TID in the range (exclusive).
 * @param t_tid_codewords Flexible array of Simple-8b encoded TID deltas.
 */
typedef struct OVAttributeArrayItem
{
	uint16		t_size;
	uint16		t_flags;

	uint16		t_num_elements;
	uint16		t_num_codewords;

	ovtid		t_firsttid;
	ovtid		t_endtid;

	uint64		t_tid_codewords[FLEXIBLE_ARRAY_MEMBER];

	/* NULL bitmap follows, if OVBT_HAS_NULLS is set */

	/* The Datum data follows */
}			OVAttributeArrayItem;

/**
 * @brief Compressed attribute B-tree leaf page item.
 *
 * When the OVBT_ATTR_COMPRESSED flag is set in t_flags, the item uses this
 * layout instead of OVAttributeArrayItem.  The TID codewords, null bitmap,
 * and datum data are compressed together into t_payload using the
 * build-time-selected algorithm (zstd > LZ4 > pglz).
 *
 * The buffer cache stores pages in compressed form; decompression is done
 * on-the-fly in backend-private memory.
 *
 * @param t_size              Total on-disk size (compressed).
 * @param t_flags             Must have OVBT_ATTR_COMPRESSED set.
 * @param t_num_elements      Number of datums.
 * @param t_num_codewords     Number of Simple-8b codewords (before compression).
 * @param t_firsttid          First TID (inclusive).
 * @param t_endtid            One past last TID (exclusive).
 * @param t_uncompressed_size Size of the data before compression.
 * @param t_payload           Compressed data (flexible array).
 */
typedef struct OVAttributeCompressedItem
{
	uint16		t_size;
	uint16		t_flags;

	uint16		t_num_elements;
	uint16		t_num_codewords;

	ovtid		t_firsttid;
	ovtid		t_endtid;

	uint16		t_uncompressed_size;

	/* compressed data follows */
	char		t_payload[FLEXIBLE_ARRAY_MEMBER];

} OVAttributeCompressedItem;

/**
 * @brief In-memory "exploded" representation of an attribute array item.
 *
 * Used during page repacking operations (splits, merges) when items need
 * to be manipulated individually.  Distinguished from on-disk items by
 * t_size == 0.
 *
 * @param t_size         Always 0 (sentinel to distinguish from on-disk items).
 * @param t_flags        Same flag bits as OVAttributeArrayItem.
 * @param t_num_elements Number of datums.
 * @param tids           Expanded array of TIDs.
 * @param nullbitmap     NULL bitmap (or NULL if no NULLs).
 * @param datumdata      Raw packed datum bytes.
 * @param datumdatasz    Size of datumdata in bytes.
 */
typedef struct OVExplodedItem
{
	uint16		t_size;			/* dummy 0 */
	uint16		t_flags;

	uint16		t_num_elements;

	ovtid	   *tids;

	bits8	   *nullbitmap;

	char	   *datumdata;
	int			datumdatasz;
}			OVExplodedItem;

/** @brief Flag: this attribute item is compressed (use OVAttributeCompressedItem). */
#define OVBT_ATTR_COMPRESSED		0x0001
/** @brief Flag: this attribute item contains NULLs (a null bitmap follows the TID codewords). */
#define OVBT_HAS_NULLS				0x0002
/*
 * When set, short varlena values (attlen == -1, attstorage != 'p') in this
 * item are stored in PostgreSQL's native 1-byte short varlena format rather
 * than the custom orvos length-prefix encoding. This allows the read path
 * to return a direct pointer into the decompressed buffer without copying
 * or reformatting the data, eliminating per-datum conversion overhead.
 *
 * Long varlenas (> 126 data bytes) and orvos toast pointers are still stored
 * in the original orvos encoding even when this flag is set.
 */
#define OVBT_ATTR_FORMAT_NATIVE_VARLENA	0x0004
#define OVBT_ATTR_FORMAT_FOR			0x0008	/* Frame of Reference encoding */
#define OVBT_ATTR_BITPACKED				0x0010	/* boolean values bit-packed, 8 per byte */
#define OVBT_ATTR_NO_NULLS				0x0020	/* no NULLs present, bitmap omitted entirely */
#define OVBT_ATTR_SPARSE_NULLS			0x0040	/* sparse NULL encoding: (offset, count) pairs */
#define OVBT_ATTR_RLE_NULLS				0x0080	/* RLE encoding for sequential NULL runs */
#define OVBT_ATTR_FORMAT_DICT			0x0100	/* dictionary-encoded for low-cardinality columns */
#define OVBT_ATTR_FORMAT_FIXED_BIN		0x0200	/* fixed-binary storage (e.g. UUID as 16 bytes) */
#define OVBT_ATTR_FORMAT_FSST			0x0400	/* FSST string compression applied */

#define OVBT_ATTR_BITMAPLEN(nelems)		(((int) (nelems) + 7) / 8)

/*
 * Sparse NULL entry: stores the byte offset into the datum data and the
 * number of consecutive NULLs at that logical position.
 */
typedef struct OVSparseNullEntry
{
	uint16		sn_position;	/* element index where the NULL(s) start */
	uint16		sn_count;		/* number of consecutive NULLs */
} OVSparseNullEntry;

/*
 * RLE NULL entry: encodes runs of NULLs and non-NULLs.
 * The high bit of rle_count indicates NULL (1) vs non-NULL (0).
 * The remaining 15 bits store the run length.
 */
#define OVBT_RLE_NULL_FLAG		0x8000
#define OVBT_RLE_COUNT_MASK		0x7FFF

typedef struct OVRleNullEntry
{
	uint16		rle_count;		/* high bit = is_null, low 15 bits = run length */
} OVRleNullEntry;

/*
 * Frame of Reference (FOR) encoding header.
 *
 * When OVBT_ATTR_FORMAT_FOR is set in t_flags, the datum data section begins
 * with this header followed by bit-packed deltas.  Each non-null value is
 * stored as (value - for_frame_min) using for_bits_per_value bits.  Deltas
 * are packed into bytes LSB-first (little-endian bit order).
 *
 * FOR encoding is used only for pass-by-value fixed-width integer types
 * (attlen 1, 2, 4, or 8 with attbyval true) when the range (max - min) can
 * be represented in significantly fewer bits than the original width.
 */
typedef struct OVForHeader
{
	uint64		for_frame_min;		/* minimum value in the frame */
	uint8		for_bits_per_value;	/* bits per delta (0..64) */
	uint8		for_attlen;			/* original attribute length (1,2,4,8) */
} OVForHeader;

/* Packed byte size for n values at given bits-per-value */
#define OVBT_FOR_PACKED_SIZE(nelems, bpv) \
	(((uint64)(nelems) * (bpv) + 7) / 8)

static inline void
ovbt_attr_item_setnull(bits8 *nullbitmap, int n)
{
	nullbitmap[n / 8] |= (1 << (n % 8));
}

static inline bool
ovbt_attr_item_isnull(bits8 *nullbitmap, int n)
{
	return (nullbitmap[n / 8] & (1 << (n % 8))) != 0;
}

/**
 * @brief TID B-tree leaf page item.
 *
 * Leaf pages in the TID tree are packed with OVTidArrayItems.  Each item
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
 * - Slot 0 (OVBT_OLD_UNDO_SLOT): tuple visible to everyone (implicit).
 * - Slot 1 (OVBT_DEAD_UNDO_SLOT): tuple is dead (implicit).
 * - Slots 2-3: explicit OVUndoRecPtr values stored in the item.
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

	ovtid		t_firsttid;
	ovtid		t_endtid;

	/* Followed by UNDO slots, and then followed by codewords */
	uint64		t_payload[FLEXIBLE_ARRAY_MEMBER];

} OVTidArrayItem;

/**
 * @name UNDO Slot Constants
 * @brief Parameters for the 2-bit UNDO slot encoding used in OVTidArrayItem.
 * @{
 */
#define OVBT_ITEM_UNDO_SLOT_BITS	2           /**< Bits per UNDO slot number. */
#define OVBT_MAX_ITEM_UNDO_SLOTS	(1 << (OVBT_ITEM_UNDO_SLOT_BITS))  /**< Max 4 slots. */
#define OVBT_ITEM_UNDO_SLOT_MASK	(OVBT_MAX_ITEM_UNDO_SLOTS - 1)     /**< 2-bit mask. */
#define OVBT_SLOTNOS_PER_WORD		(64 / OVBT_ITEM_UNDO_SLOT_BITS)    /**< 32 slots per uint64. */
/** @} */

/**
 * @name TID Array Item Limits
 * @brief Maximum sizes for OVTidArrayItem to keep item manipulation fast.
 * @{
 */
#define OVBT_MAX_ITEM_CODEWORDS		16  /**< Max Simple-8b codewords per item. */
#define OVBT_MAX_ITEM_TIDS			128 /**< Max TIDs per item. */
/** @} */

/** @brief Implicit slot: tuple is "old" and visible to everyone. */
#define OVBT_OLD_UNDO_SLOT			0
/** @brief Implicit slot: tuple is dead (not visible to anyone). */
#define OVBT_DEAD_UNDO_SLOT			1
/** @brief First physically-stored UNDO slot index. */
#define OVBT_FIRST_NORMAL_UNDO_SLOT	2

/** @brief Number of uint64 slotwords needed for @a num_tids tuples. */
#define OVBT_NUM_SLOTWORDS(num_tids) ((num_tids + OVBT_SLOTNOS_PER_WORD - 1) / OVBT_SLOTNOS_PER_WORD)

static inline size_t
SizeOfOVTidArrayItem(int num_tids, int num_undo_slots, int num_codewords)
{
	Size		sz;

	sz = offsetof(OVTidArrayItem, t_payload);
	sz += num_codewords * sizeof(uint64);
	sz += (num_undo_slots - OVBT_FIRST_NORMAL_UNDO_SLOT) * sizeof(OVUndoRecPtr);
	sz += OVBT_NUM_SLOTWORDS(num_tids) * sizeof(uint64);

	return sz;
}

/*
 * Get pointers to the TID codewords, UNDO slots, and slotwords from an item.
 *
 * Note: this is also used to get the pointers when constructing a new item, so
 * don't assert here that the data is valid!
 */
static inline void
OVTidArrayItemDecode(OVTidArrayItem *item, uint64 **codewords,
					 OVUndoRecPtr **slots, uint64 **slotwords)
{
	char	   *p = (char *) item->t_payload;

	*codewords = (uint64 *) p;
	p += item->t_num_codewords * sizeof(uint64);
	*slots = (OVUndoRecPtr *) p;
	p += (item->t_num_undo_slots - OVBT_FIRST_NORMAL_UNDO_SLOT) * sizeof(OVUndoRecPtr);
	*slotwords = (uint64 *) p;
}

/**
 * @brief Maximum size of a single untoasted datum in Orvos.
 *
 * Datums exceeding this size are "orvos-toasted": split into chunks and
 * stored on dedicated toast pages within the same relation file.
 * The threshold accounts for page header, item header, and opaque area.
 */
#define		MaxOrvosDatumSize		(BLCKSZ - 500)

/**
 * @brief Opaque area for Orvos toast pages.
 *
 * Toast pages form a doubly-linked list per datum.  The first page in the
 * chain stores the attribute number, owning TID, and total datum size.
 * Subsequent pages store slice offsets.
 *
 * @param ov_attno        Attribute number of the toasted column.
 * @param ov_tid          TID of the owning tuple (first page only).
 * @param ov_total_size   Total uncompressed datum size (first page only).
 * @param ov_slice_offset Byte offset of this chunk within the full datum.
 * @param ov_prev         Previous toast page (InvalidBlockNumber if first).
 * @param ov_next         Next toast page (InvalidBlockNumber if last).
 * @param ov_page_id      Always OV_TOAST_PAGE_ID (0xF086).
 */
typedef struct OVToastPageOpaque
{
	AttrNumber	ov_attno;

	/* these are only set on the first page. */
	ovtid		ov_tid;
	uint32		ov_total_size;

	uint32		ov_slice_offset;
	BlockNumber ov_prev;
	BlockNumber ov_next;
	uint16		ov_flags;
	uint16		padding1;		/* padding, to put ov_page_id last */
	uint16		padding2;		/* padding, to put ov_page_id last */
	uint16		ov_page_id;
} OVToastPageOpaque;

/**
 * @brief In-tree toast pointer for oversized datums.
 *
 * Stored in place of the actual datum in an attribute array item when the
 * datum has been orvos-toasted.  Must be layout-compatible with
 * varattrib_1b_e so that VARATT_IS_EXTERNAL() recognizes it.
 *
 * @warning These must never escape Orvos code; the rest of PostgreSQL
 *          cannot dereference them.
 *
 * @param va_header  Standard 1-byte varlena header.
 * @param va_tag     Always VARTAG_ORVOS (10).
 * @param ovt_block  Block number of the first toast page.
 */
typedef struct varatt_ov_toastptr
{
	/* varattrib_1b_e */
	uint8		va_header;
	uint8		va_tag;			/* VARTAG_ORVOS in orvos toast datums */

	/* first block */
	BlockNumber ovt_block;
}			varatt_ov_toastptr;

/*
 * va_tag value. this should be distinguishable from the values in
 * vartag_external
 */
#define		VARTAG_ORVOS		10

/**
 * @brief Orvos-aware version of datumGetSize().
 *
 * Handles Orvos toast pointers (VARTAG_ORVOS) in addition to standard
 * PostgreSQL datum types.
 *
 * @param value    The Datum to measure.
 * @param typByVal Whether the type is pass-by-value.
 * @param typLen   The type's declared length (-1 for varlena, -2 for cstring).
 * @return Size of the datum in bytes.
 */
static inline Size
ov_datumGetSize(Datum value, bool typByVal, int typLen)
{
	if (typLen > 0)
		return typLen;
	else if (typLen == -1)
	{
		struct varlena *vl = (struct varlena *) DatumGetPointer(value);

		if (VARATT_IS_EXTERNAL(vl) && VARTAG_EXTERNAL(vl) == VARTAG_ORVOS)
			return sizeof(varatt_ov_toastptr);
		else
			return VARSIZE_ANY(vl);
	}
	else
		return datumGetSize(value, typByVal, typLen);
}

static inline Datum
ov_datumCopy(Datum value, bool typByVal, int typLen)
{
	if (typLen < 0)
	{
		struct varlena *vl = (struct varlena *) DatumGetPointer(value);

		if (VARATT_IS_EXTERNAL(vl) && VARTAG_EXTERNAL(vl) == VARTAG_ORVOS)
		{
			char	   *result = palloc(sizeof(varatt_ov_toastptr));

			memcpy(result, DatumGetPointer(value), sizeof(varatt_ov_toastptr));

			return PointerGetDatum(result);
		}
	}
	return datumCopy(value, typByVal, typLen);
}

/** @brief Block number of the metapage (always 0). */
#define OV_META_BLK		0

/**
 * @brief Entry in the metapage's B-tree root directory.
 *
 * The metapage stores one OVRootDirItem per attribute (including the TID
 * tree at index 0).  Each entry points to the root page of the
 * corresponding B-tree.
 *
 * @param root  Block number of the B-tree root page.
 */
typedef struct OVRootDirItem
{
	BlockNumber root;
} OVRootDirItem;

/**
 * @brief Metapage contents (stored in the page body area).
 *
 * Contains the number of attributes and a flexible array of root directory
 * entries, one per attribute.  Index 0 is the TID tree root.
 *
 * @param nattributes   Number of B-trees (TID tree + user columns).
 * @param tree_root_dir Array of root block pointers, indexed by attno.
 */
typedef struct OVMetaPage
{
	int			nattributes;
	OVRootDirItem tree_root_dir[FLEXIBLE_ARRAY_MEMBER]; /* one for each
														 * attribute */
} OVMetaPage;

/**
 * @brief Metapage opaque area (stored in pd_special).
 *
 * Contains UNDO log head/tail pointers, the oldest live UNDO record,
 * and the Free Page Map head.  The ov_page_id field allows tools like
 * pg_filedump to identify the page type.
 *
 * @param ov_undo_head                Oldest UNDO log page.
 * @param ov_undo_tail                Newest UNDO log page (insertion point).
 * @param ov_undo_tail_first_counter  Counter of the first record on tail page.
 * @param ov_undo_oldestptr           Oldest UNDO record still needed by any snapshot.
 * @param ov_fpm_head                 Head of the Free Page Map linked list.
 * @param ov_page_id                  Always OV_META_PAGE_ID (0xF083).
 */
typedef struct OVMetaPageOpaque
{
	/*
	 * Head and tail page of the UNDO log.
	 *
	 * 'ov_undo_tail' is the newest page, where new UNDO records will be
	 * inserted, and 'ov_undo_head' is the oldest page.
	 * 'ov_undo_tail_first_counter' is the UNDO counter value of the first
	 * record on the tail page (or if the tail page is empty, the counter
	 * value the first record on the tail page will have, when it's inserted.)
	 * If there is no UNDO log at all, 'ov_undo_tail_first_counter' is the new
	 * counter value to use. It's actually redundant, except when there is no
	 * UNDO log at all, but it's a nice cross-check at other times.
	 */
	BlockNumber ov_undo_head;
	BlockNumber ov_undo_tail;
	uint64		ov_undo_tail_first_counter;

	/*
	 * Oldest UNDO record that is still needed. Anything older than this can
	 * be discarded, and considered as visible to everyone.
	 */
	OVUndoRecPtr ov_undo_oldestptr;

	BlockNumber ov_fpm_head;	/* head of the Free Page Map list */

	uint16		ov_flags;
	uint16		ov_page_id;
} OVMetaPageOpaque;

/**
 * @brief Non-vacuumable status codes for Orvos visibility checks.
 */
typedef enum
{
	OVNV_NONE,                  /**< Tuple is vacuumable or live. */
	OVNV_RECENTLY_DEAD          /**< Tuple is dead but not yet deletable. */
} OVNV_Result;

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
typedef struct OVUndoSlotVisibility
{
	TransactionId xmin;
	TransactionId xmax;
	CommandId	cmin;
	uint32		speculativeToken;
	OVNV_Result nonvacuumable_status;
} OVUndoSlotVisibility;

static const OVUndoSlotVisibility InvalidUndoSlotVisibility = {
	.xmin = InvalidTransactionId,
	.xmax = InvalidTransactionId,
	.cmin = InvalidCommandId,
	.speculativeToken = INVALID_SPECULATIVE_TOKEN,
	.nonvacuumable_status = OVNV_NONE
};

/**
 * @brief Iterator state for unpacking a single OVTidArrayItem.
 *
 * Holds the decoded TIDs, their UNDO slot assignments, and cached
 * visibility for each slot.
 */
typedef struct OVTidItemIterator
{
	int			tids_allocated_size;
	ovtid	   *tids;
	uint8	   *tid_undoslotnos;
	int			num_tids;
	MemoryContext context;

	OVUndoRecPtr undoslots[OVBT_MAX_ITEM_UNDO_SLOTS];
	OVUndoSlotVisibility undoslot_visibility[OVBT_MAX_ITEM_UNDO_SLOTS];
} OVTidItemIterator;

/**
 * @brief State for an in-progress scan on the TID tree.
 *
 * Created by ovbt_tid_begin_scan() and destroyed by ovbt_tid_end_scan().
 * The scan walks TID tree leaf pages, decoding OVTidArrayItems and
 * checking visibility against the provided snapshot.
 *
 * @param rel         The relation being scanned.
 * @param context     Long-lived memory context for scan allocations.
 * @param active      Whether the scan is currently positioned.
 * @param lastbuf     Last buffer accessed (held with share lock during scan).
 * @param snapshot    Visibility snapshot for tuple filtering.
 * @param starttid    Lower bound of the TID range to scan (inclusive).
 * @param endtid      Upper bound of the TID range to scan (exclusive).
 * @param currtid     Last TID returned by ovbt_tid_scan_next().
 * @param recent_oldest_undo  Oldest UNDO record still needed.
 * @param serializable        Whether to acquire predicate locks.
 */
typedef struct OVTidTreeScan
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
	 * ovbt_tid_scan_next() will return.
	 */
	ovtid		starttid;
	ovtid		endtid;
	ovtid		currtid;

	/* in the "real" UNDO-log, this would probably be a global variable */
	OVUndoRecPtr recent_oldest_undo;

	/* should this scan do predicate locking? Or check for conflicts? */
	bool		serializable;
	bool		acquire_predicate_tuple_locks;

	/*
	 * These fields are used, when the scan is processing an array item.
	 */
	OVTidItemIterator array_iter;
	int			array_curr_idx;
}			OVTidTreeScan;

/**
 * @brief Get the UNDO slot number of the current TID in a TID tree scan.
 *
 * Must be called after ovbt_tid_scan_next() has returned a valid TID.
 * The result indexes into scan->array_iter.undoslots[] and
 * scan->array_iter.undoslot_visibility[].
 *
 * @param scan  Active TID tree scan.
 * @return The 2-bit UNDO slot number (0-3) for the current TID.
 */
static inline uint8
OVTidScanCurUndoSlotNo(OVTidTreeScan * scan)
{
	Assert(scan->array_curr_idx >= 0 && scan->array_curr_idx < scan->array_iter.num_tids);
	Assert(scan->array_iter.tid_undoslotnos != NULL);
	return (scan->array_iter.tid_undoslotnos[scan->array_curr_idx]);
}

/**
 * @brief State for an in-progress scan on an Orvos attribute B-tree.
 *
 * Created by ovbt_attr_begin_scan() and destroyed by ovbt_attr_end_scan().
 * The scan walks attribute tree leaf pages, decompressing and decoding
 * OVAttributeArrayItem entries into arrays of Datums.
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
typedef struct OVAttrTreeScan
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
	 * are filled in by ovbt_attr_item_extract().
	 */
	int			array_datums_allocated_size;
	Datum	   *array_datums;
	bool	   *array_isnulls;
	ovtid	   *array_tids;
	int			array_num_elements;

	int			array_curr_idx;

	/* working areas for ovbt_attr_item_extract() */
	char	   *decompress_buf;
	int			decompress_buf_size;
	char	   *attr_buf;
	int			attr_buf_size;

}			OVAttrTreeScan;

/**
 * @brief Backend-private cache of metapage information.
 *
 * Stored in RelationData->rd_amcache.  Contains B-tree root block numbers
 * and rightmost leaf pointers for fast lookups and end-of-tree insertions.
 *
 * Validity is tied to smgr_targblock: the cache is invalidated whenever
 * an smgr invalidation occurs (e.g., relation extension by another backend).
 * Use ovmeta_get_cache() to access; it auto-populates on first use.
 *
 * @param cache_nattributes  Number of attributes (including TID tree).
 * @param cache_attrs        Per-attribute root, rightmost leaf, and lokey.
 */
typedef struct OVMetaCacheData
{
	int			cache_nattributes;

	/** @brief Per-attribute cache entry. */
	struct
	{
		BlockNumber root;		/**< Root block of this attribute's B-tree. */
		BlockNumber rightmost;	/**< Rightmost leaf page (for fast appends). */
		ovtid		rightmost_lokey;	/**< Lokey of the rightmost leaf. */
	}			cache_attrs[FLEXIBLE_ARRAY_MEMBER];

} OVMetaCacheData;

/**
 * @brief Populate the metapage cache by reading block 0.
 * @param rel  The Orvos relation.
 * @return Pointer to the newly populated OVMetaCacheData.
 */
extern OVMetaCacheData *ovmeta_populate_cache(Relation rel);

/**
 * @brief Get the cached metapage data, populating it if necessary.
 * @param rel  The Orvos relation.
 * @return Pointer to the OVMetaCacheData in rel->rd_amcache.
 */
static inline OVMetaCacheData *
ovmeta_get_cache(Relation rel)
{
	if (rel->rd_amcache == NULL || RelationGetTargetBlock(rel) == InvalidBlockNumber)
		ovmeta_populate_cache(rel);
	return (OVMetaCacheData *) rel->rd_amcache;
}

/**
 * @brief Invalidate the cached metapage data.
 *
 * The next call to ovmeta_get_cache() will re-read the metapage.
 *
 * @param rel  The Orvos relation.
 */
static inline void
ovmeta_invalidate_cache(Relation rel)
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
 * Split/merge routines construct a list of ov_split_stack entries rather
 * than modifying pages directly.  Each entry holds an exclusively-locked
 * buffer and a temporary in-memory copy of the new page contents.  Once
 * the entire operation is prepared, ov_apply_split_changes() writes all
 * pages atomically with WAL protection.
 *
 * @param next     Next entry in the stack.
 * @param buf      Exclusively-locked buffer.
 * @param page     Temporary in-memory copy of the page to write.
 * @param recycle  If true, add this page to the FPM after the operation.
 */
typedef struct ov_split_stack ov_split_stack;

struct ov_split_stack
{
	ov_split_stack *next;

	Buffer		buf;
	Page		page;			/* temp in-memory copy of page */
	bool		recycle;		/* should the page be added to the FPM? */
};

/* prototypes for functions in orvos_tidpage.c */
extern void ovbt_tid_begin_scan(Relation rel, ovtid starttid, ovtid endtid,
								Snapshot snapshot, OVTidTreeScan * scan);
extern void ovbt_tid_reset_scan(OVTidTreeScan * scan, ovtid starttid, ovtid endtid, ovtid currtid);
extern void ovbt_tid_end_scan(OVTidTreeScan * scan);
extern bool ovbt_tid_scan_next_array(OVTidTreeScan * scan, ovtid nexttid, ScanDirection direction);

/*
 * Return the next TID in the scan.
 *
 * The next TID means the first TID > scan->currtid. Each call moves
 * scan->currtid to the last returned TID. You can call ovbt_tid_reset_scan()
 * to change the position, scan->starttid and scan->endtid define the
 * boundaries of the search.
 */
static inline ovtid
ovbt_tid_scan_next(OVTidTreeScan * scan, ScanDirection direction)
{
	ovtid		nexttid;
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
		if (!ovbt_tid_scan_next_array(scan, nexttid, direction))
		{
			scan->currtid = nexttid;
			return InvalidOVTid;
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
		ovtid		this_tid = scan->array_iter.tids[idx];

		if (this_tid >= scan->endtid)
		{
			scan->currtid = nexttid;
			return InvalidOVTid;
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
				OVUndoSlotVisibility *visi_info = &scan->array_iter.undoslot_visibility[slotno];

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
	 * unreachable, because ovbt_tid_scan_next_array() should never return an
	 * array that doesn't contain a matching TID.
	 */
	Assert(false);
	return InvalidOVTid;
}


extern TM_Result ovbt_tid_delta_update(Relation rel, ovtid otid,
									   TransactionId xid, CommandId cid,
									   bool key_update, Snapshot snapshot,
									   Snapshot crosscheck, bool wait,
									   TM_FailureData *hufd,
									   ovtid *newtid_p,
									   bool *this_xact_has_lock,
									   int natts, const bool *changed_cols);
extern void ovbt_tid_delta_insert(Relation rel, ovtid *tids,
								  TransactionId xid, CommandId cid,
								  ovtid predecessor_tid,
								  int natts, const bool *changed_cols,
								  OVUndoRecPtr prevundoptr);
extern void ovbt_tid_multi_insert(Relation rel,
								  ovtid *tids, int ntuples,
								  TransactionId xid, CommandId cid,
								  uint32 speculative_token, OVUndoRecPtr prevundoptr);
extern TM_Result ovbt_tid_delete(Relation rel, ovtid tid,
								 TransactionId xid, CommandId cid,
								 Snapshot snapshot, Snapshot crosscheck, bool wait,
								 TM_FailureData *hufd, bool changingPart, bool *this_xact_has_lock);
extern TM_Result ovbt_tid_update(Relation rel, ovtid otid,
								 TransactionId xid,
								 CommandId cid, bool key_update, Snapshot snapshot, Snapshot crosscheck,
								 bool wait, TM_FailureData *hufd, ovtid *newtid_p, bool *this_xact_has_lock);
extern void ovbt_tid_clear_speculative_token(Relation rel, ovtid tid, uint32 spectoken, bool forcomplete);
extern void ovbt_tid_mark_dead(Relation rel, ovtid tid, OVUndoRecPtr recent_oldest_undo);
extern IntegerSet *ovbt_collect_dead_tids(Relation rel, ovtid starttid, ovtid *endtid, uint64 *num_live_tuples);
extern void ovbt_tid_remove(Relation rel, IntegerSet *tids);
extern TM_Result ovbt_tid_lock(Relation rel, ovtid tid,
							   TransactionId xid, CommandId cid,
							   LockTupleMode lockmode, bool follow_updates,
							   Snapshot snapshot, TM_FailureData *hufd,
							   ovtid *next_tid, bool *this_xact_has_lock,
							   OVUndoSlotVisibility *visi_info);
extern void ovbt_tid_undo_deletion(Relation rel, ovtid tid, OVUndoRecPtr undoptr, OVUndoRecPtr recent_oldest_undo);
extern ovtid ovbt_get_last_tid(Relation rel);
extern void ovbt_find_latest_tid(Relation rel, ovtid *tid, Snapshot snapshot);
extern void ovbt_tid_mark_updated_for_cluster(Relation rel, ovtid otid,
											  ovtid newtid, TransactionId xid,
											  CommandId cid, bool key_update);

/* prototypes for functions in orvos_tiditem.c */
extern List *ovbt_tid_item_create_for_range(ovtid tid, int nelements, OVUndoRecPtr undo_ptr);
extern List *ovbt_tid_item_add_tids(OVTidArrayItem *orig, ovtid firsttid, int nelements,
									OVUndoRecPtr undo_ptr, bool *modified_orig);
extern void ovbt_tid_item_unpack(OVTidArrayItem *item, OVTidItemIterator *iter);
extern List *ovbt_tid_item_change_undoptr(OVTidArrayItem *orig, ovtid target_tid, OVUndoRecPtr undoptr, OVUndoRecPtr recent_oldest_undo);
extern List *ovbt_tid_item_remove_tids(OVTidArrayItem *orig, ovtid *nexttid, IntegerSet *remove_tids,
									   OVUndoRecPtr recent_oldest_undo);


/* prototypes for functions in orvos_attpage.c */
extern void ovbt_attr_begin_scan(Relation rel, TupleDesc tdesc, AttrNumber attno,
								 OVAttrTreeScan * scan);
extern void ovbt_attr_end_scan(OVAttrTreeScan * scan);
extern bool ovbt_attr_scan_fetch_array(OVAttrTreeScan * scan, ovtid tid);

extern void ovbt_attr_multi_insert(Relation rel, AttrNumber attno,
								   Datum *datums, bool *isnulls, ovtid *tids, int ndatums);

/* prototypes for functions in orvos_attitem.c */
extern List *ovbt_attr_create_items(Form_pg_attribute att,
									Datum *datums, bool *isnulls, ovtid *tids, int nelements);
extern void ovbt_split_item(Form_pg_attribute attr, OVExplodedItem * origitem, ovtid first_right_tid,
							OVExplodedItem * *leftitem_p, OVExplodedItem * *rightitem_p);
extern OVExplodedItem * ovbt_attr_remove_from_item(Form_pg_attribute attr,
												   OVAttributeArrayItem * olditem,
												   ovtid *removetids);
extern List *ovbt_attr_recompress_items(Form_pg_attribute attr, List *olditems);

extern void ovbt_attr_item_extract(OVAttrTreeScan * scan, OVAttributeArrayItem * item);


/* prototypes for functions in orvos_btree.c */
extern ov_split_stack * ovbt_newroot(Relation rel, AttrNumber attno, int level, List *downlinks);
extern ov_split_stack * ovbt_insert_downlinks(Relation rel, AttrNumber attno,
											  ovtid leftlokey, BlockNumber leftblkno, int level,
											  List *downlinks, Buffer held_buf);
extern void ovbt_attr_remove(Relation rel, AttrNumber attno, IntegerSet *tids);
extern ov_split_stack * ovbt_unlink_page(Relation rel, AttrNumber attno, Buffer buf, int level);
extern ov_split_stack * ov_new_split_stack_entry(Buffer buf, Page page);
extern void ov_apply_split_changes(Relation rel, ov_split_stack * stack, struct ov_pending_undo_op *undo_op);
extern Buffer ovbt_descend(Relation rel, AttrNumber attno, ovtid key, int level, bool readonly, Buffer held_buf, Buffer held_buf2);
extern Buffer ovbt_find_and_lock_leaf_containing_tid(Relation rel, AttrNumber attno,
													 Buffer buf, ovtid nexttid, int lockmode);
extern bool ovbt_page_is_expected(Relation rel, AttrNumber attno, ovtid key, int level, Buffer buf);
extern void ovbt_wal_log_leaf_items(Relation rel, AttrNumber attno, Buffer buf, OffsetNumber off, bool replace, List *items, struct ov_pending_undo_op *undo_op);
extern void ovbt_wal_log_rewrite_pages(Relation rel, AttrNumber attno, List *buffers, struct ov_pending_undo_op *undo_op, uint32 recycle_bitmap, BlockNumber old_fpm_head, Buffer metabuf);

/*
 * Return the value of row identified with 'tid' in a scan.
 *
 * 'tid' must be greater than any previously returned item.
 *
 * Returns true if a matching item is found, false otherwise. After
 * a false return, it's OK to call this again with another greater TID.
 */
static inline bool
ovbt_attr_fetch(OVAttrTreeScan * scan, Datum *datum, bool *isnull, ovtid tid)
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
		if (!ovbt_attr_scan_fetch_array(scan, tid))
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
		ovtid		this_tid = scan->array_tids[idx];

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

extern PGDLLIMPORT const TupleTableSlotOps TTSOpsOrvos;

/* prototypes for functions in orvos_meta.c */
extern void ovmeta_initmetapage(Relation rel);
extern void ovmeta_initmetapage_redo(XLogReaderState *record);
extern BlockNumber ovmeta_get_root_for_attribute(Relation rel, AttrNumber attno, bool for_update);
extern void ovmeta_add_root_for_new_attributes(Relation rel, Page page);

/* prototypes for functions in orvos_visibility.c */
extern TM_Result ov_SatisfiesUpdate(Relation rel, Snapshot snapshot,
									OVUndoRecPtr recent_oldest_undo,
									ovtid item_tid, OVUndoRecPtr item_undoptr,
									LockTupleMode mode,
									bool *undo_record_needed, bool *this_xact_has_lock,
									TM_FailureData *tmfd, ovtid *next_tid,
									OVUndoSlotVisibility *visi_info);
extern bool ov_SatisfiesVisibility(OVTidTreeScan * scan, OVUndoRecPtr item_undoptr,
								   TransactionId *obsoleting_xid, ovtid *next_tid,
								   OVUndoSlotVisibility *visi_info);

/* prototypes for functions in orvos_toast.c */
extern Datum orvos_toast_datum(Relation rel, AttrNumber attno, Datum value, ovtid tid);
extern Datum orvos_toast_flatten(Relation rel, AttrNumber attno, ovtid tid, Datum toasted);

/* prototypes for column-delta UPDATE support in orvos_handler.c */
extern void ov_materialize_delta_columns(Relation rel,
										 ovtid newtid,
										 ovtid predecessor_tid,
										 int natts,
										 const uint32 *changed_cols);

/* prototypes for functions in orvos_freepagemap.c */
extern Buffer ovpage_getnewbuf(Relation rel, Buffer metabuf);
extern Buffer ovpage_extendrel_newbuf(Relation rel, Buffer metabuf);
extern void ovpage_mark_page_deleted(Page page, BlockNumber next_free_blk);
extern void ovpage_delete_page(Relation rel, Buffer buf);

typedef struct OrvosTupleTableSlot
{
	TupleTableSlot base;

	char	   *data;			/* data for materialized slots */

	/*
	 * Extra visibility information. The tuple's xmin and cmin can be
	 * extracted from here, used e.g. for triggers (XXX is that true?).
	 * There's also a flag to indicate if a tuple is vacuumable or not, which
	 * can be useful if you're scanning with SnapshotAny. That's currently
	 * used in index build.
	 */
	OVUndoSlotVisibility *visi_info;

	/*
	 * Normally, when a tuple is retrieved from a table, 'visi_info' points to
	 * TID tree scan's data structures. But sometimes it's useful to keep the
	 * information together with the slot, e.g. whe a slot is copied, so that
	 * it doesn't depend on any data outside the slot. In that case, you can
	 * fill in 'visi_info_buf', and set visi_info = &visi_info_buf.
	 */
	OVUndoSlotVisibility visi_info_buf;
}			OrvosTupleTableSlot;

/* TableAM methods (defined in orvos_handler.c) */
extern const TableAmRoutine orvosam_methods;

#endif							/* ORVOS_INTERNAL_H */
