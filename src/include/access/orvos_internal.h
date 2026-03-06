/*
 * orvos_internal.h
 *		internal declarations for Orvos tables
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

#define OV_META_ATTRIBUTE_NUM 0



#define INVALID_SPECULATIVE_TOKEN 0

/*
 * A Orvos table contains different kinds of pages, all in the same file.
 *
 * Block 0 is always a metapage. It contains the block numbers of the other
 * data structures stored within the file, like the per-attribute B-trees,
 * and the UNDO log. In addition, if there are overly large datums in the
 * the table, they are chopped into separate "toast" pages.
 */
#define	OV_META_PAGE_ID		0xF083
#define	OV_BTREE_PAGE_ID	0xF084
#define	OV_UNDO_PAGE_ID		0xF085
#define	OV_TOAST_PAGE_ID	0xF086
#define	OV_FREE_PAGE_ID		0xF087

/* flags for orvos b-tree pages */
#define OVBT_ROOT				0x0001

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

#define OVBtreePageGetOpaque(page) ((OVBtreePageOpaque *) PageGetSpecialPointer(page))

/*
 * Internal B-tree page layout.
 *
 * The "contents" of the page is an array of OVBtreeInternalPageItem. The number
 * of items can be deduced from pd_lower.
 */
typedef struct OVBtreeInternalPageItem
{
	ovtid		tid;
	BlockNumber childblk;
} OVBtreeInternalPageItem;

static inline OVBtreeInternalPageItem *
OVBtreeInternalPageGetItems(Page page)
{
	OVBtreeInternalPageItem *items;

	items = (OVBtreeInternalPageItem *) PageGetContents(page);

	return items;
}
static inline int
OVBtreeInternalPageGetNumItems(Page page)
{
	OVBtreeInternalPageItem *begin;
	OVBtreeInternalPageItem *end;

	begin = (OVBtreeInternalPageItem *) PageGetContents(page);
	end = (OVBtreeInternalPageItem *) ((char *) page + ((PageHeader) page)->pd_lower);

	return end - begin;
}

static inline bool
OVBtreeInternalPageIsFull(Page page)
{
	PageHeader	phdr = (PageHeader) page;

	return phdr->pd_upper - phdr->pd_lower < sizeof(OVBtreeInternalPageItem);
}

/*
 * Attribute B-tree leaf page layout
 *
 * Leaf pages in the attribute trees are packed with "array items", which
 * contain the actual user data for the column, in a compact format. Each
 * array item contains the datums for a range of TIDs. The ranges of two
 * items never overlap, but there can be gaps, if a row has been deleted
 * or updated.
 *
 * Each array item consists of a fixed header, a list of TIDs of the rows
 * contained in it, a NULL bitmap (if there are any NULLs), and the actual
 * Datum data. The TIDs are encoded using Simple-8b encoding, like in the
 * TID tree.
 *
 * The data (including the TID codewords) can be compressed. In that case,
 * OVAttributeCompressedItem is used. The fields are mostly the same as in
 * OVAttributeArrayItem, and we cast between the two liberally.
 *
 * The datums are packed in a custom format. Fixed-width datatypes are
 * stored as is, but without any alignment padding. Variable-length
 * datatypes are *not* stored in the usual Postgres varlen format; the
 * following encoding is used instead:
 *
 * Each varlen datum begins with a one or two byte header, to store the
 * size. If the size of the datum, excluding the varlen header, is <=
 * 128, then a one byte header is used. Otherwise, the high bit of the
 * first byte is set, and two bytes are used to represent the size.
 * Two bytes is always enough, because if a datum is larger than a page,
 * it must be toasted.
 *
 * Traditional Postgres toasted datums should not be seen on-disk in
 * orvos. However, "orvos-toasted" datums, i.e. datums that have been
 * stored on separate toast blocks within orvos, are possible. They
 * are stored with magic 0xFF 0xFF as the two header bytes, followed by
 * the block number of the first toast block.
 *
 * 0xxxxxxx [up to 128 bytes of data follows]
 * 1xxxxxxx xxxxxxxx [data]
 * 11111111 11111111 toast pointer.
 *
 * XXX Heikki: I'm not sure if this special encoding makes sense. Perhaps
 * just storing normal Postgres varlenas would be better. Having a custom
 * encoding felt like a good idea, but I'm not sure we're actually gaining
 * anything. If we also did alignment padding, like the rest of Postgres
 * does, then we could avoid some memory copies when decoding the array.
 *
 * TODO: squeeze harder: eliminate padding, use high bits of t_tid for flags or size
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

/*
 * The two structs above are stored on disk. OVExplodedItem is a third
 * representation of an array item that is only used in memory, when
 * repacking items on a page. It is distinguished by t_size == 0.
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

#define OVBT_ATTR_COMPRESSED		0x0001
#define OVBT_HAS_NULLS				0x0002

#define OVBT_ATTR_BITMAPLEN(nelems)		(((int) (nelems) + 7) / 8)

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

/*
 * TID B-tree leaf page layout
 *
 * Leaf pages are packed with ZSTidArrayItems. Each OVTidArrayItem represents
 * a range of tuples, starting at 't_firsttid', up to 't_endtid' - 1. For each
 * tuple, we its TID and the UNDO pointer. The TIDs and UNDO pointers are specially
 * encoded, so that they take less space.
 *
 * Item format:
 *
 * We make use of some assumptions / observations on the TIDs and UNDO pointers
 * to pack them tightly:
 *
 * - TIDs are kept in ascending order, and the gap between two TIDs
 *   is usually very small. On a newly loaded table, all TIDs are
 *   consecutive.
 *
 * - It's common for the UNDO pointer to be old so that the tuple is
 *   visible to everyone. In that case we don't need to keep the exact value.
 *
 * - Nearby TIDs are likely to have only a few distinct UNDO pointer values.
 *
 *
 * Each item looks like this:
 *
 *  Header  |  1-16 TID codewords | 0-2 UNDO pointers | UNDO "slotwords"
 *
 * The fixed-size header contains the start and end of the TID range that
 * this item represents, and information on how many UNDO slots and codewords
 * follow in the variable-size part.
 *
 * After the fixed-size header comes the list of TIDs. They are encoded in
 * Simple-8b codewords. Simple-8b is an encoding scheme to pack multiple
 * integers in 64-bit codewords. A single codeword can pack e.g. three 20-bit
 * integers, or 20 3-bit integers, or a number of different combinations.
 * Therefore, small integers pack more tightly than larger integers. We encode
 * the difference between each TID, so in the common case that there are few
 * gaps between the TIDs, we only need a few bits per tuple. The first encoded
 * integer is always 0, because the first TID is stored explicitly in
 * t_firsttid. (TODO: storing the first constant 0 is obviously a waste of
 * space. Also, since there cannot be duplicates, we could store "delta - 1",
 * which would allow a more tight representation in some cases.)
 *
 * After the TID codeword, are so called "UNDO slots". They represent all the
 * distinct UNDO pointers in the group of TIDs that this item covers.
 * Logically, there are 4 slots. Slots 0 and 1 are special, representing
 * all-visible "old" TIDs, and "dead" TIDs. They are not stored in the item
 * itself, to save space, but logically, they can be thought to be part of
 * every item. They are included in 't_num_undo_slots', so the number of UNDO
 * pointers physically stored on an item is actually 't_num_undo_slots - 2'.
 *
 * With the 4 UNDO slots, we can represent an UNDO pointer using a 2-bit
 * slot number. If you update a tuple with a new UNDO pointer, and all four
 * slots are already in use, the item needs to be split. Hopefully that doesn't
 * happen too often (see assumptions above).
 *
 * After the UNDO slots come "UNDO slotwords". The slotwords contain the slot
 * number of each tuple in the item. The slot numbers are packed in 64 bit
 * integers, with 2 bits for each tuple.
 *
 * Representing UNDO pointers as distinct slots also has the advantage that
 * when we're scanning the TID array, we can check the few UNDO pointers in
 * the slots against the current snapshot, and remember the visibility of
 * each slot, instead of checking every UNDO pointer separately. That
 * considerably speeds up visibility checks when reading. That's one
 * advantage of this special encoding scheme, compared to e.g. using a
 * general-purpose compression algorithm on an array of TIDs and UNDO pointers.
 *
 * The physical size of an item depends on how many tuples it covers, the
 * number of codewords needed to encode the TIDs, and many distinct UNDO
 * pointers they have.
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

/*
 * We use 2 bits for the UNDO slot number for every tuple. We can therefore
 * fit 32 slot numbers in each 64-bit "slotword".
 */
#define OVBT_ITEM_UNDO_SLOT_BITS	2
#define OVBT_MAX_ITEM_UNDO_SLOTS	(1 << (OVBT_ITEM_UNDO_SLOT_BITS))
#define OVBT_ITEM_UNDO_SLOT_MASK	(OVBT_MAX_ITEM_UNDO_SLOTS - 1)
#define OVBT_SLOTNOS_PER_WORD		(64 / OVBT_ITEM_UNDO_SLOT_BITS)

/*
 * To keep the item size and time needed to work with them reasonable,
 * limit the size of an item to max 16 codewords and 128 TIDs.
 */
#define OVBT_MAX_ITEM_CODEWORDS		16
#define OVBT_MAX_ITEM_TIDS			128

#define OVBT_OLD_UNDO_SLOT			0
#define OVBT_DEAD_UNDO_SLOT			1
#define OVBT_FIRST_NORMAL_UNDO_SLOT	2

/* Number of UNDO slotwords needed for a given number of tuples */
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

/*
 * Toast page layout.
 *
 * When an overly large datum is stored, it is divided into chunks, and each
 * chunk is stored on a dedicated toast page. The toast pages of a datum form
 * list, each page has a next/prev pointer.
 */
/*
 * Maximum size of an individual untoasted Datum stored in Orvos. Datums
 * larger than this need to be toasted.
 *
 * A datum needs to fit on a B-tree page, with page and item headers.
 *
 * XXX: 500 accounts for all the headers. Need to compute this correctly...
 */
#define		MaxOrvosDatumSize		(BLCKSZ - 500)

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

/*
 * "Toast pointer" of a datum that's stored in orvos toast pages.
 *
 * This looks somewhat like a normal TOAST pointer, but we mustn't let these
 * escape out of orvos code, because the rest of the system doesn't know
 * how to deal with them.
 *
 * This must look like varattrib_1b_e!
 */
typedef struct varatt_ov_toastptr
{
	/* varattrib_1b_e */
	uint8		va_header;
	uint8		va_tag;			/* VARTAG_ORVOS in orvos toast datums */

	/* first block */
	BlockNumber zst_block;
}			varatt_ov_toastptr;

/*
 * va_tag value. this should be distinguishable from the values in
 * vartag_external
 */
#define		VARTAG_ORVOS		10

/*
 * Versions of datumGetSize and datumCopy that know about Orvos-toasted
 * datums.
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

/*
 * Block 0 on every Orvos table is a metapage.
 *
 * It contains a directory of b-tree roots for each attribute, and lots more.
 */
#define OV_META_BLK		0

/*
 * The metapage stores one of these for each attribute.
 */
typedef struct OVRootDirItem
{
	BlockNumber root;
} OVRootDirItem;

typedef struct OVMetaPage
{
	int			nattributes;
	OVRootDirItem tree_root_dir[FLEXIBLE_ARRAY_MEMBER]; /* one for each
														 * attribute */
} OVMetaPage;

/*
 * it's not clear what we should store in the "opaque" special area, and what
 * as page contents, on a metapage. But have at least the page_id field here,
 * so that tools like pg_filedump can recognize it as a orvos metapage.
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

/*
 * Codes populated by ov_SatisfiesNonVacuumable. This has minimum values
 * defined based on what's needed. Heap equivalent has more states.
 */
typedef enum
{
	OVNV_NONE,
	OVNV_RECENTLY_DEAD			/* tuple is dead, but not deletable yet */
} OVNV_Result;

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

/*
 * Holds the state of an in-progress scan on a orvos Tid tree.
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

/*
 * This is convenience function to get the index aka slot number for undo and
 * visibility array. Important to note this performs "next_idx - 1" means
 * works after returning from TID scan function when the next_idx has been
 * incremented.
 */
static inline uint8
OVTidScanCurUndoSlotNo(OVTidTreeScan * scan)
{
	Assert(scan->array_curr_idx >= 0 && scan->array_curr_idx < scan->array_iter.num_tids);
	Assert(scan->array_iter.tid_undoslotnos != NULL);
	return (scan->array_iter.tid_undoslotnos[scan->array_curr_idx]);
}

/*
 * Holds the state of an in-progress scan on a orvos attribute tree.
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

/*
 * We keep a this cached copy of the information in the metapage in
 * backend-private memory. In RelationData->rd_amcache.
 *
 * The cache contains the block numbers of the roots of all the tree
 * structures, for quick searches, as well as the rightmost leaf page, for
 * quick insertions to the end.
 *
 * Use ovmeta_get_cache() to get the cached struct.
 *
 * This is used together with smgr_targblock. smgr_targblock tracks the
 * physical size of the relation file. This struct is only considered valid
 * when smgr_targblock is valid. So in effect, we invalidate this whenever
 * a smgr invalidation happens. Logically, the lifetime of this is the same
 * as smgr_targblocks/smgr_fsm_nblocks/smgr_vm_nblocks, but there's no way
 * to attach an AM-specific struct directly to SmgrRelation.
 */
typedef struct OVMetaCacheData
{
	int			cache_nattributes;

	/* For each attribute */
	struct
	{
		BlockNumber root;		/* root of the b-tree */
		BlockNumber rightmost;	/* right most leaf page */
		ovtid		rightmost_lokey;	/* lokey of rightmost leaf */
	}			cache_attrs[FLEXIBLE_ARRAY_MEMBER];

} OVMetaCacheData;

extern OVMetaCacheData *ovmeta_populate_cache(Relation rel);

static inline OVMetaCacheData *
ovmeta_get_cache(Relation rel)
{
	if (rel->rd_amcache == NULL || RelationGetTargetBlock(rel) == InvalidBlockNumber)
		ovmeta_populate_cache(rel);
	return (OVMetaCacheData *) rel->rd_amcache;
}

/*
 * Blow away the cached OVMetaCacheData struct. Next call to ovmeta_get_cache()
 * will reload it from the metapage.
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

/*
 * ov_split_stack is used during page split, or page merge, to keep track
 * of all the modified pages. The page split (or merge) routines don't
 * modify pages directly, but they construct a list of 'ov_split_stack'
 * entries. Each entry holds a buffer, and a temporary in-memory copy of
 * a page that should be written to the buffer, once everything is completed.
 * All the buffers are exclusively-locked.
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
extern void ovbt_wal_log_rewrite_pages(Relation rel, AttrNumber attno, List *buffers, struct ov_pending_undo_op *undo_op);

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
