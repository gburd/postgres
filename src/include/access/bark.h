/*-------------------------------------------------------------------------
 *
 * bark.h
 *	  On-disk format and shared definitions for the BARK index access method.
 *
 * This header defines the page layout, the meta page, and the index-entry
 * encoding for BARK (see BARK-Design.mediawiki and the README in this
 * directory).  It describes the format only; the code that reads and writes
 * it arrives in later commits of the series.  Defining the format on its own
 * keeps the series bisectable and gives every later commit a single place to
 * refer to.
 *
 * BARK's page structure follows nbtree's Lehman & Yao layout (right-links and
 * high keys), so the concurrency algorithm can be shared.  What BARK adds on
 * top is the C-KEYSHAPE entry encoding: an entry is a (key, value) pair whose
 * value is a single locator, a sorted list of locators, or a locator set
 * stored as a posting list (see BarkEntryShape).
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/bark.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef BARK_H
#define BARK_H

#include "access/amapi.h"
#include "access/itup.h"
#include "storage/block.h"
#include "storage/bufpage.h"
#include "storage/condition_variable.h"
#include "storage/lwlock.h"

/*
 * BARK uses the same page size and block-addressing as the rest of the
 * system.  P_NONE marks the absent sibling of an edge page, exactly as in
 * nbtree.
 */
#define BARK_P_NONE			0

/* ----------------------------------------------------------------------------
 * Page layout
 *
 * Every BARK page is a standard PostgreSQL page (PageHeaderData + line
 * pointers + tuples growing toward each other) with a BarkPageOpaqueData in
 * the special space.  The opaque area carries the Lehman & Yao sibling links,
 * the tree level, and the page-role flags.  The last two bytes of the special
 * space hold a page-type identifier so pg_filedump and amcheck-style tools
 * can recognize a BARK page, mirroring nbtree's use of the cycle-id slot.
 * ----------------------------------------------------------------------------
 */
typedef struct BarkPageOpaqueData
{
	BlockNumber bark_prev;		/* left sibling, or BARK_P_NONE if leftmost */
	BlockNumber bark_next;		/* right sibling, or BARK_P_NONE if rightmost */
	uint32		bark_level;		/* tree level; zero for leaf pages */
	uint16		bark_flags;		/* flag bits, see below */
	uint16		bark_page_id;	/* BARK_PAGE_ID, for tool identification */
} BarkPageOpaqueData;

typedef BarkPageOpaqueData *BarkPageOpaque;

#define BarkPageGetOpaque(page) \
	((BarkPageOpaque) PageGetSpecialPointer(page))

/*
 * A fixed marker in the last two bytes of the special space, identifying the
 * page as belonging to a BARK index.  Any distinctive value works; this one
 * spells "BK".
 */
#define BARK_PAGE_ID		0xB43C

/* Bits defined in bark_flags */
#define BARK_LEAF			(1 << 0)	/* leaf page (else internal) */
#define BARK_ROOT			(1 << 1)	/* root page (no parent) */
#define BARK_DELETED		(1 << 2)	/* page deleted from the tree */
#define BARK_META			(1 << 3)	/* meta page */
#define BARK_HALF_DEAD		(1 << 4)	/* empty but still linked in the tree */
#define BARK_INCOMPLETE_SPLIT (1 << 5)	/* right sibling's downlink is missing */
#define BARK_HAS_GARBAGE	(1 << 6)	/* page has known-dead entries */
#define BARK_OVERFLOW		(1 << 7)	/* holds a chunk of an oversized value */

#define BarkPageIsLeaf(opaque)		(((opaque)->bark_flags & BARK_LEAF) != 0)
#define BarkPageIsRoot(opaque)		(((opaque)->bark_flags & BARK_ROOT) != 0)
#define BarkPageIsDeleted(opaque)	(((opaque)->bark_flags & BARK_DELETED) != 0)
#define BarkPageIsMeta(opaque)		(((opaque)->bark_flags & BARK_META) != 0)
#define BarkPageIsOverflow(opaque)	(((opaque)->bark_flags & BARK_OVERFLOW) != 0)

/*
 * A page is leftmost / rightmost at its level when it has no left / right
 * sibling.  A non-rightmost page carries a high key -- an upper bound on the
 * keys it may hold -- as its first item (BARK_P_HIKEY); real data then starts
 * at BARK_P_FIRSTKEY.  The rightmost page has no high key (its implicit upper
 * bound is +infinity), so its data starts at BARK_P_HIKEY.  This is nbtree's
 * Lehman & Yao layout, which lets a scan detect a concurrent split by
 * comparing its key against the high key and following the right link.
 */
#define BarkPageLeftmost(opaque)	((opaque)->bark_prev == BARK_P_NONE)
#define BarkPageRightmost(opaque)	((opaque)->bark_next == BARK_P_NONE)

#define BARK_P_HIKEY		((OffsetNumber) 1)	/* high key, if present */
#define BARK_P_FIRSTKEY		((OffsetNumber) 2)	/* first data item after it */
#define BarkPageFirstDataKey(opaque) \
	(BarkPageRightmost(opaque) ? BARK_P_HIKEY : BARK_P_FIRSTKEY)

/*
 * The largest item BARK will place on a page.  Like nbtree's BTMaxItemSize,
 * this bounds a single entry to roughly a third of the usable page so at
 * least three entries fit, keeping the tree from degenerating.  A LIST or
 * POSTING entry that would exceed this is split (A10) or, for POSTING,
 * compacted; see the LIST/POSTING growth paths in barkinsert.c.
 */
#define BarkMaxItemSize \
	MAXALIGN_DOWN((BLCKSZ - \
				   MAXALIGN(SizeOfPageHeaderData + 3 * sizeof(ItemIdData)) - \
				   MAXALIGN(sizeof(BarkPageOpaqueData))) / 3)

/* ----------------------------------------------------------------------------
 * Meta page
 *
 * Block 0 of a BARK index is always the meta page.  It points to the current
 * root and records the on-disk version so the format can evolve.
 * ----------------------------------------------------------------------------
 */
#define BARK_METAPAGE		0	/* block number of the meta page */
#define BARK_MAGIC			0x5241424B	/* "BARK" as a big-endian uint32 */
#define BARK_VERSION		1	/* current on-disk version */

/*
 * BARK uses the btree strategy numbers and support-function convention: its
 * operator classes live in the btree operator families (see
 * ambtreeopfamilies), so it shares btree's numbering.  Strategies 1..5 are
 * <, <=, =, >=, >.  Support function 1 is the ordering comparator (btree's
 * BTORDER_PROC), the only one BARK requires; the remaining btree support
 * functions (2..6: sortsupport, in_range, equalimage, options, skipsupport)
 * are optional and may be present in the shared family without BARK using
 * them, so amsupport covers the whole btree range.
 */
#define BARK_NSTRATEGIES	5	/* number of strategies (btree's set) */
#define BARK_NPROCS			6	/* btree's support-function range (BTNProcs) */
#define BARK_ORDER_PROC		1	/* support function 1: 3-way comparator */

/*
 * Ordered-operator (KNN) scans.  Strategy 6 is BARK's distance ordering
 * operator (`<->`): `ORDER BY col <-> const` returns the key values nearest
 * to const, in increasing distance.  Unlike the search strategies 1..5 (which
 * BARK shares with btree), this one is BARK-specific: it carries
 * amoppurpose='o' and sorts by the btree float_ops family (distances are
 * float8).  btree does not define it, so the amop rows that register it in
 * btree's operator families name BARK as their access method -- the same
 * "a btree-compatible AM may live in btree's families" rule F07 established
 * for opclasses, extended here to the ordering amop rows.
 */
#define BARK_KNN_STRATEGY	6

typedef struct BarkMetaPageData
{
	uint32		bark_magic;		/* should equal BARK_MAGIC */
	uint32		bark_version;	/* on-disk version (<= BARK_VERSION) */
	BlockNumber bark_root;		/* current root block, or BARK_P_NONE */
	uint32		bark_level;		/* tree level of the root page */
	bool		bark_allequalimage; /* are all key columns "equalimage"? */
} BarkMetaPageData;

#define BarkPageGetMeta(page) \
	((BarkMetaPageData *) PageGetContents(page))

/* ----------------------------------------------------------------------------
 * Index-entry encoding (the C-KEYSHAPE contract)
 *
 * A BARK entry is a standard IndexTupleData: the key columns follow the
 * header, and the header's t_tid normally holds the entry's locator.  BARK
 * reuses PostgreSQL's per-AM reserved bit, INDEX_AM_RESERVED_BIT in t_info, as
 * an "alternate t_tid" marker, exactly as nbtree does: when it is set, t_tid
 * does not hold a plain heap locator but is repurposed to carry entry
 * metadata in its offset-number field.
 *
 * The metadata lives in the top four bits of the offset number
 * (BARK_STATUS_OFFSET_MASK), leaving the low twelve bits
 * (BARK_OFFSET_MASK) for a small count -- the number of key attributes in a
 * pivot tuple, or the number of locators in a posting entry.  This is the
 * same partition of the offset field that nbtree uses, so the two AMs can
 * share the reserved-bit conventions in itup.h and storage/itemptr.h.
 *
 * Entry shapes (C-KEYSHAPE value side):
 *
 *   - SINGLE   : the plain nbtree shape.  INDEX_AM_RESERVED_BIT clear; t_tid
 *				  is one heap locator.  This is the only shape a scalar unique
 *				  or ordinary index ever produces.
 *
 *   - LIST     : sorted duplicates (the libdb shape).  The alt-TID bit is set
 *				  with BARK_IS_LIST; the low offset bits hold the count and the
 *				  locators are stored, in ascending order, in the tuple body
 *				  after the key.  Used when a key maps to a modest number of
 *				  rows.
 *
 *   - POSTING  : an inverted locator set.  The alt-TID bit is set with
 *				  BARK_IS_POSTING; the set is stored as an sbm serialization
 *				  (see lib/sbm.h) in the tuple body after the key.  Chosen when
 *				  the set is large and clustered enough for sbm's density
 *				  envelope to win; otherwise LIST is used.
 *
 *   - PIVOT    : a downlink/high-key tuple on an internal page (or a leaf high
 *				  key).  The alt-TID bit is set with neither list nor posting
 *				  status; t_tid carries the child block number, and the low
 *				  offset bits hold the number of key attributes present (a
 *				  pivot may be truncated by suffix truncation).
 *
 * A leaf entry of any shape may additionally be delete-marked (the C-DELETE
 * contract): BARK_IS_DELETE_MARKED records that the entry is logically
 * deleted but retained for UNDO rollback, matching nbtree's
 * BT_IS_DELETE_MARKED.  It is a property of an entry, orthogonal to its shape,
 * so it shares the status-bit region and must never be set on a PIVOT.
 * ----------------------------------------------------------------------------
 */

/*
 * Partition of the t_tid offset-number field for an alt-TID entry, identical
 * to nbtree's BT_OFFSET_MASK / BT_STATUS_OFFSET_MASK split.
 */
#define BARK_OFFSET_MASK			0x0FFF
#define BARK_STATUS_OFFSET_MASK		0xF000

/*
 * Status bits carried in the high nibble of an alt-TID entry's offset number.
 * These deliberately match nbtree's values so the shared reserved-bit
 * conventions (and BARK_IS_DELETE_MARKED in particular, which is consumed by
 * the generic index delete-marking path) stay aligned across the two AMs.
 */
#define BARK_PIVOT_META				0x1000	/* pivot carries extra metadata */
#define BARK_IS_POSTING				0x2000	/* leaf entry is a posting set */
#define BARK_IS_DELETE_MARKED		0x4000	/* leaf entry is delete-marked */
#define BARK_IS_LIST				0x8000	/* leaf entry is a sorted list */

/*
 * An oversized entry (a key, or key + INCLUDE payload, too large to fit on a
 * page under the BarkMaxItemSize ceiling) is marked with BARK_IS_LIST and
 * BARK_IS_POSTING set together -- a combination no SINGLE/LIST/POSTING/PIVOT
 * entry ever uses, so it is a free sixteenth code point in the status nibble.
 * Its full index tuple lives on a BARK_OVERFLOW page chain; the leaf (or pivot)
 * entry keeps only a fixed-size reference (BarkOverflowRef) and the heap
 * locator, so the entry is tiny regardless of the value's size.  See the
 * OVERSIZED accessor section below.
 */
#define BARK_IS_OVERFLOW			(BARK_IS_LIST | BARK_IS_POSTING)

StaticAssertDecl(BARK_OFFSET_MASK >= INDEX_MAX_KEYS,
				 "BARK_OFFSET_MASK can't fit INDEX_MAX_KEYS");

/*
 * The C-KEYSHAPE value shapes, as a decoded enum for the read/write code to
 * switch on.  Derived from t_info's alt-TID bit plus the status bits above.
 */
typedef enum BarkEntryShape
{
	BARK_SHAPE_SINGLE = 0,		/* one heap locator in t_tid (no alt-TID) */
	BARK_SHAPE_LIST,			/* sorted list of locators in the body */
	BARK_SHAPE_POSTING,			/* sbm-serialized locator set in the body */
	BARK_SHAPE_OVERSIZED,		/* full tuple stored on an overflow chain */
	BARK_SHAPE_PIVOT,			/* internal downlink / high key */
} BarkEntryShape;

/*
 * True when the entry uses the alternate-t_tid representation (any shape other
 * than SINGLE).  The read path calls this before trusting t_tid as a locator.
 */
static inline bool
BarkEntryIsAltTID(const IndexTupleData *itup)
{
	return (itup->t_info & INDEX_AM_RESERVED_BIT) != 0;
}

/* Decode the shape of a leaf or pivot entry from its status bits. */
static inline BarkEntryShape
BarkEntryGetShape(const IndexTupleData *itup)
{
	OffsetNumber status;

	if (!BarkEntryIsAltTID(itup))
		return BARK_SHAPE_SINGLE;
	status = ItemPointerGetOffsetNumberNoCheck(&itup->t_tid) &
		BARK_STATUS_OFFSET_MASK;
	/* OVERFLOW first: it sets LIST and POSTING together, so test it before them. */
	if ((status & BARK_IS_OVERFLOW) == BARK_IS_OVERFLOW)
		return BARK_SHAPE_OVERSIZED;
	if (status & BARK_IS_LIST)
		return BARK_SHAPE_LIST;
	if (status & BARK_IS_POSTING)
		return BARK_SHAPE_POSTING;
	return BARK_SHAPE_PIVOT;
}

/* True for a leaf entry that holds one or more heap locators (not a pivot). */
static inline bool
BarkEntryIsLeafData(const IndexTupleData *itup)
{
	BarkEntryShape shape = BarkEntryGetShape(itup);

	return shape != BARK_SHAPE_PIVOT;
}

/* ----------------------------------------------------------------------------
 * PIVOT entry accessors (internal downlinks and page high keys)
 *
 * A pivot tuple sets the alt-TID bit; its t_tid offset-number field carries
 * BARK_PIVOT_META in the status bits and the number of key attributes present
 * (after suffix truncation) in the low BARK_OFFSET_MASK bits, and the t_tid
 * block-number field carries the child block for a downlink.  This mirrors
 * nbtree's pivot encoding exactly, so the bit layout in itemptr.h is shared.
 * ----------------------------------------------------------------------------
 */

/* Number of key attributes recorded in a pivot tuple. */
static inline uint16
BarkPivotGetNAtts(const IndexTupleData *itup)
{
	return (ItemPointerGetOffsetNumberNoCheck(&itup->t_tid) &
			BARK_OFFSET_MASK);
}

/* Stamp a pivot tuple's status bits and attribute count into t_tid. */
static inline void
BarkPivotSetNAtts(IndexTupleData *itup, uint16 natts)
{
	Assert((natts & BARK_STATUS_OFFSET_MASK) == 0);
	itup->t_info |= INDEX_AM_RESERVED_BIT;
	ItemPointerSetOffsetNumber(&itup->t_tid,
							   (OffsetNumber) (natts | BARK_PIVOT_META));
}

/* Downlink: the child block a pivot on an internal page points at. */
static inline BlockNumber
BarkPivotGetDownLink(const IndexTupleData *itup)
{
	return ItemPointerGetBlockNumberNoCheck(&itup->t_tid);
}

static inline void
BarkPivotSetDownLink(IndexTupleData *itup, BlockNumber blkno)
{
	ItemPointerSetBlockNumber(&itup->t_tid, blkno);
}

/* ----------------------------------------------------------------------------
 * LIST entry accessors (sorted duplicates)
 *
 * A LIST entry is a normal IndexTuple -- the key attributes follow the header
 * exactly as in a SINGLE entry -- extended with a packed, ascending array of
 * ItemPointerData locators appended after the key data.  The alt-TID bit is
 * set with BARK_IS_LIST; the low BARK_OFFSET_MASK bits of the t_tid offset
 * field hold the locator count.  The t_tid block field is unused (left zero).
 *
 * The locator array begins at the first MAXALIGN boundary after the key data,
 * which is where index_form_tuple leaves the tuple's used size; locators are
 * themselves naturally aligned (ItemPointerData is 6 bytes but the array base
 * is MAXALIGNed, matching how heap TID arrays are laid out elsewhere).
 * ----------------------------------------------------------------------------
 */

/*
 * The number of locators a LIST or POSTING entry carries can be at most the
 * low twelve bits of the offset field.  LIST uses the field as a live count;
 * when a LIST would exceed BARK_LIST_MAX_COUNT members it is converted to a
 * POSTING entry (A11), whose count field instead records that it is a set.
 */
#define BARK_LIST_MAX_COUNT		BARK_OFFSET_MASK

/* Number of locators recorded in a LIST entry. */
static inline uint16
BarkListGetCount(const IndexTupleData *itup)
{
	return (ItemPointerGetOffsetNumberNoCheck(&itup->t_tid) &
			BARK_OFFSET_MASK);
}

/*
 * Byte offset within a LIST or POSTING entry at which its appended body (the
 * locator array or the sbm blob) begins.  A LIST/POSTING entry is a key tuple
 * (header + attribute data) with a body appended after it; the body starts at
 * the MAXALIGNed end of the key prefix.  Because the entry's t_info size
 * covers the whole entry (key + body), the split point cannot be recovered
 * from t_info alone, so the constructor records it in the t_tid block-number
 * field (which a LIST/POSTING entry does not otherwise use), giving O(1)
 * recovery -- the same device nbtree uses for its posting-list offset.
 */
static inline uint16
BarkEntryGetBodyOffset(const IndexTupleData *itup)
{
	return (uint16) ItemPointerGetBlockNumberNoCheck(&itup->t_tid);
}

static inline void
BarkEntrySetBodyOffset(IndexTupleData *itup, uint16 bodyoff)
{
	ItemPointerSetBlockNumber(&itup->t_tid, (BlockNumber) bodyoff);
}

/* Pointer to the first locator of a LIST entry. */
static inline ItemPointer
BarkListGetTIDArray(IndexTupleData *itup)
{
	return (ItemPointer) ((char *) itup + BarkEntryGetBodyOffset(itup));
}

/* The n'th locator (0-based) of a LIST entry. */
static inline ItemPointer
BarkListGetTID(IndexTupleData *itup, int n)
{
	return &BarkListGetTIDArray(itup)[n];
}

/* ----------------------------------------------------------------------------
 * POSTING entry accessors (sbm-backed inverted locator set)
 *
 * A POSTING entry has the same key prefix as a SINGLE/LIST entry, extended
 * with an sbm serialization (see lib/sbm.h) of its locator set in the body.
 * The alt-TID bit is set with BARK_IS_POSTING; the body offset is in the t_tid
 * block field (as for LIST), and the body occupies the rest of the entry (its
 * length is the entry size minus the body offset).  The member count is not
 * stored in the offset field -- it can exceed BARK_OFFSET_MASK -- but is read
 * back from the sbm; the offset low bits are left zero.
 *
 * A heap TID maps to an sbm index with a reversible, block-clustered encoding
 * so a run of TIDs on one heap block becomes a dense sbm run:
 *
 *     key    = (uint64) block * MaxHeapTuplesPerPage + (offset - 1)
 *     block  = key / MaxHeapTuplesPerPage
 *     offset = (key % MaxHeapTuplesPerPage) + 1
 *
 * (offset is 1-based on a heap page, so it is biased by one into the set.)
 * ----------------------------------------------------------------------------
 */

/* Pointer to a POSTING entry's serialized sbm body. */
static inline uint8 *
BarkPostingGetData(IndexTupleData *itup)
{
	return (uint8 *) ((char *) itup + BarkEntryGetBodyOffset(itup));
}

/* Length in bytes of a POSTING entry's serialized sbm body. */
static inline Size
BarkPostingGetDataSize(IndexTupleData *itup)
{
	return IndexTupleSize(itup) - BarkEntryGetBodyOffset(itup);
}

/* ----------------------------------------------------------------------------
 * OVERSIZED entry accessors and overflow page format
 *
 * A key (or key + INCLUDE payload) whose formed index tuple exceeds
 * BarkMaxItemSize cannot sit inline on a page.  BARK stores the full index
 * tuple out-of-line on a chain of BARK_OVERFLOW pages and leaves a small,
 * fixed-size OVERSIZED entry inline, so the page stays well within its item
 * ceiling no matter how large the value is.  (This is BARK's libdb-class
 * capability: nbtree errors on such a key; BARK indexes it transparently.)
 *
 * The inline entry carries no attribute data; its t_tid offset field holds the
 * BARK_IS_OVERFLOW status and its t_tid block field holds the first overflow
 * block.  A BarkOverflowRef appended after the (empty) key records the full
 * tuple's byte length, the entry's locator, and -- for a pivot -- its key-
 * attribute count.  Everything the hot paths need (locator, downlink, pivot
 * natts) is read inline from the ref; only key comparison fetches the full
 * tuple from the overflow chain (bark_compare_itups), so two keys that share a
 * long prefix still order on their full value.
 *
 * The overflow chain reuses the standard BARK page: each BARK_OVERFLOW page's
 * bark_next links to the next chunk (BARK_P_NONE at the tail), and the page's
 * data area (PageHeader .. special) holds up to BarkOverflowChunkSize raw bytes
 * of the full tuple.  The chunks concatenate in chain order to the full tuple.
 * ----------------------------------------------------------------------------
 */

/* Marks a BarkOverflowRef as belonging to a leaf entry rather than a pivot. */
#define BARK_OVERFLOW_LEAF		0xFFFF

/*
 * The fixed metadata an OVERSIZED entry keeps inline, appended after its (zero-
 * attribute) key prefix.  locator is the heap TID for a leaf entry; for a pivot
 * its block field is the downlink and natts is the key-attribute count.
 */
typedef struct BarkOverflowRef
{
	uint32		fulllen;		/* byte length of the full IndexTuple in overflow */
	ItemPointerData locator;	/* heap TID (leaf) or downlink block (pivot) */
	uint16		natts;			/* pivot key-attr count, or BARK_OVERFLOW_LEAF */
} BarkOverflowRef;

/* The first overflow block of an OVERSIZED entry's chain. */
static inline BlockNumber
BarkOverflowGetFirstBlock(const IndexTupleData *itup)
{
	return ItemPointerGetBlockNumberNoCheck(&itup->t_tid);
}

/*
 * The BarkOverflowRef appended after the entry's (empty) key prefix.  Unlike a
 * LIST/POSTING entry, an OVERSIZED entry uses the t_tid block field for its
 * first overflow block, so the body offset cannot be read from there; it is the
 * MAXALIGNed header offset (the entry stores no inline attribute data).
 */
static inline BarkOverflowRef *
BarkOverflowGetRef(IndexTupleData *itup)
{
	return (BarkOverflowRef *) ((char *) itup +
							   MAXALIGN(IndexInfoFindDataOffset(itup->t_info)));
}

/* True when an OVERSIZED entry is a leaf (heap-locator) rather than a pivot. */
static inline bool
BarkOverflowIsLeaf(IndexTupleData *itup)
{
	return BarkOverflowGetRef(itup)->natts == BARK_OVERFLOW_LEAF;
}

/*
 * The downlink child block of a pivot entry, whatever its shape.  A plain PIVOT
 * keeps it in t_tid; an OVERSIZED pivot keeps it in its ref (t_tid holds the
 * first overflow block instead), so descent must read it the shape-aware way.
 */
static inline BlockNumber
BarkEntryGetDownLink(IndexTupleData *itup)
{
	if (BarkEntryGetShape(itup) == BARK_SHAPE_OVERSIZED)
		return ItemPointerGetBlockNumberNoCheck(&BarkOverflowGetRef(itup)->locator);
	return ItemPointerGetBlockNumberNoCheck(&itup->t_tid);
}

/* The key-attribute count of a pivot entry, whatever its shape. */
static inline uint16
BarkEntryGetPivotNAtts(IndexTupleData *itup)
{
	if (BarkEntryGetShape(itup) == BARK_SHAPE_OVERSIZED)
		return BarkOverflowGetRef(itup)->natts;
	return BarkPivotGetNAtts(itup);
}

/*
 * Bytes of the full tuple a single overflow page can hold: the page's whole
 * data area between the standard header and the BARK page opaque.
 */
#define BarkOverflowChunkSize \
	(BLCKSZ - MAXALIGN(SizeOfPageHeaderData) - MAXALIGN(sizeof(BarkPageOpaqueData)))

/* ----------------------------------------------------------------------------
 * Shared prototypes (bark.c, barkutils.c, barksort.c, barkvalidate.c)
 * ----------------------------------------------------------------------------
 */

/*
 * Per-column comparison state for a BARK index: the ordering comparator
 * FmgrInfo for each key column, with its collation and sort direction, built
 * once from the index's operator class and reused for every comparison during
 * a build or search.
 */
typedef struct BarkKeyColumn
{
	FmgrInfo	cmp;			/* support function 1 (3-way comparator) */
	Oid			collation;		/* collation to pass to the comparator */
	bool		reverse;		/* DESC: invert the comparison result */
	bool		nulls_first;	/* NULLs sort before non-NULLs */
} BarkKeyColumn;

typedef struct BarkKeyInfo
{
	int			nkeys;			/* number of key columns */
	BarkKeyColumn cols[FLEXIBLE_ARRAY_MEMBER];
} BarkKeyInfo;

extern BarkKeyInfo *bark_build_keyinfo(Relation index);
extern int	bark_compare_itups(BarkKeyInfo *keyinfo, Relation index,
							   IndexTuple a, IndexTuple b);
extern CompareType bark_translate_strategy(StrategyNumber strategy, Oid opfamily);
extern StrategyNumber bark_translate_cmptype(CompareType cmptype, Oid opfamily);

/* ----------------------------------------------------------------------------
 * LIST / POSTING entry construction and reading (barkutils.c)
 * ----------------------------------------------------------------------------
 */

/*
 * Build a LIST entry: the key columns of `key` (a SINGLE-shape index tuple)
 * extended with the `ntids` locators in `tids` (which must be sorted
 * ascending and distinct) stored in the body.  Returns a palloc'd entry.
 */
extern IndexTuple bark_form_list(TupleDesc tupdesc, IndexTuple key,
								 ItemPointer tids, int ntids);

/*
 * Collect the heap locators of a leaf entry (SINGLE, LIST, or POSTING) into
 * `out` in ascending order, returning the count.  `out` must have room for at
 * least bark_entry_count_tids(itup) locators.  A POSTING entry is iterated
 * through its sbm.
 */
extern int	bark_entry_get_tids(IndexTuple itup, ItemPointer out, int maxout);

/* Number of locators a leaf entry holds. */
extern int	bark_entry_count_tids(IndexTuple itup);

/*
 * Reform a clean SINGLE-shape key tuple from a leaf entry (dropping any LIST
 * or POSTING body and alt-TID status).  When `tid` is non-NULL the result's
 * t_tid is set to it; otherwise t_tid is left as index_form_tuple leaves it.
 * Used by VACUUM to collapse a LIST/POSTING down to a SINGLE and to recover a
 * plain key.
 */
extern IndexTuple bark_single_from_list(Relation index, IndexTuple entry,
										ItemPointer tid);

/*
 * POSTING shape: an inverted locator set stored as an sbm serialization.
 *
 * bark_tid_to_key / bark_key_to_tid are the reversible, block-clustered TID
 * <-> uint64 mapping documented in the POSTING accessor section above.
 *
 * bark_form_posting builds a POSTING entry from the key columns of `key` and
 * the `ntids` ascending locators in `tids`; it returns NULL when the sbm
 * envelope would not be smaller than the equivalent LIST (caller keeps the
 * LIST).  bark_posting_count / bark_posting_get_tids read a POSTING entry's
 * set back.
 */
extern uint64 bark_tid_to_key(ItemPointer tid);
extern void bark_key_to_tid(uint64 key, ItemPointer tid);
extern IndexTuple bark_form_posting(TupleDesc tupdesc, IndexTuple key,
									ItemPointer tids, int ntids);
extern int	bark_posting_count(IndexTuple itup);
extern int	bark_posting_get_tids(IndexTuple itup, ItemPointer out, int maxout);

/* ----------------------------------------------------------------------------
 * OVERSIZED entry construction and overflow-chain I/O (barkutils.c)
 * ----------------------------------------------------------------------------
 */

/*
 * Form the complete index tuple for a row's values, WITHOUT nbtree's 1/3-page
 * (or index_form_tuple's 8191-byte) ceiling: an oversized key or payload is
 * returned as a full, correctly-laid-out IndexTuple whose byte length is
 * written to *fulllen (the t_info size field wraps past 8191 and must not be
 * read for such a tuple -- use *fulllen).  When the result fits inline
 * (*fulllen <= BarkMaxItemSize) it is byte-identical to index_form_tuple, so
 * the common no-overflow path is unchanged.  The caller stores an oversized
 * result on an overflow chain and places a small OVERSIZED entry on the page.
 */
extern IndexTuple bark_form_full_tuple(TupleDesc tupdesc, const Datum *values,
									   const bool *isnull, Size *fulllen);

/* True when `fulllen` bytes cannot sit inline and need overflow storage. */
extern bool bark_len_is_oversized(Size fulllen);

/*
 * Number of overflow pages the chain for a tuple of `fulllen` bytes needs.
 */
extern BlockNumber bark_overflow_nchunks(Size fulllen);

/*
 * Form the small fixed-size OVERSIZED entry that references an already-written
 * overflow chain beginning at `firstblk`.  `locator` is the entry's heap TID
 * (leaf) or its downlink block as a TID (pivot); pass is_leaf=false with the
 * key-attribute count in `natts` for a pivot.  Returns a palloc'd entry.
 */
extern IndexTuple bark_form_oversized_entry(ItemPointer locator, Size fulllen,
											BlockNumber firstblk,
											bool is_leaf, uint16 natts);

/*
 * Write `full` (fulllen bytes) across a chain of BARK_OVERFLOW pages via the
 * buffer pool (P_NEW + generic WAL), returning the first block.  Used by the
 * insert path; the build path uses bark_init_overflow_page directly against
 * its bulk-write buffers.
 */
extern BlockNumber bark_write_overflow_chain(Relation index, IndexTuple full,
											 Size fulllen);

/*
 * Lay out the `which`'th overflow chunk of a tuple of `fulllen` bytes into
 * `page` (already palloc'd / bulk-reserved), copying its slice of `full` and
 * linking it to `nextblk`.  The build path calls this to format bulk-write
 * buffers; the insert path uses bark_write_overflow_chain.
 */
extern void bark_init_overflow_page(Page page, const char *full, Size fulllen,
									BlockNumber which, BlockNumber nextblk);

/*
 * Reconstruct the full index tuple an OVERSIZED entry references by walking its
 * overflow chain.  Returns a palloc'd tuple the caller pfrees.  Its byte length
 * is the entry's recorded fulllen (available via BarkOverflowGetRef); the
 * t_info size field may be wrapped, so callers that need the length use the
 * ref.  Used by key comparison, scans, and amcheck.
 */
extern IndexTuple bark_fetch_oversized(Relation index, IndexTuple entry);

/*
 * Free the overflow chain an OVERSIZED entry references (its pages become
 * BARK_DELETED), as part of VACUUM removing the owning leaf entry.  WAL-logged
 * under its own generic-WAL records.
 */
extern void bark_free_oversized(Relation index, IndexTuple entry);

extern IndexBuildResult *bark_build(Relation heap, Relation index,
									IndexInfo *indexInfo);
extern void bark_buildempty(Relation index);
extern bool barkvalidate(Oid opclassoid);

/*
 * Parallel index-build worker entry point.  Reachable by name from
 * parallel.c's internal worker table (see CreateParallelContext with the
 * "postgres" library in barksort.c).
 */
struct dsm_segment;
struct shm_toc;
extern void _bark_parallel_build_main(struct dsm_segment *seg,
									  struct shm_toc *toc);

/*
 * Search descent stack: the path from the root to a leaf, recorded during a
 * search so an insert that splits the leaf can walk back up inserting the
 * downlinks.  Each entry names the block visited and the offset of the
 * downlink followed out of it.
 */
typedef struct BarkStackData
{
	BlockNumber bark_blkno;		/* internal page visited */
	OffsetNumber bark_offset;	/* offset of the downlink followed */
	struct BarkStackData *bark_parent;	/* next level up, or NULL at the root */
} BarkStackData;

typedef BarkStackData *BarkStack;

/*
 * Descend to the leaf that should contain `key`, returning that leaf's buffer
 * (write-locked when forwrite) and, when stack is non-NULL, the parent path.
 * key is an index tuple whose key columns are compared with bark_compare_itups.
 *
 * nextkey chooses which leaf a run of equal keys lands on: true (insert / true
 * key) descends to the rightmost leaf that can hold the key; false (lower-bound
 * scan) descends to the leftmost such leaf, so a forward equality or
 * lower-bound scan does not skip earlier duplicates when a run of equal keys
 * spans several leaves.
 */
extern Buffer bark_search(Relation index, BarkKeyInfo *keyinfo,
						  IndexTuple key, bool forwrite, bool nextkey,
						  BarkStack *stack);
extern void bark_freestack(BarkStack stack);

extern bool bark_insert(Relation index, Datum *values, bool *isnull,
						 ItemPointer ht_ctid, Relation heapRel,
						 IndexUniqueCheck checkUnique,
						 bool indexUnchanged, IndexInfo *indexInfo);

/*
 * KNN (ordered-operator) scan state (barkknn.c).
 *
 * `ORDER BY col <-> const` over a scalar B-tree key is answered by descending
 * to const and expanding OUTWARD in both directions: the nearest key values
 * to const are the ones immediately at/after it (walking the leaf chain
 * forward) merged with the ones immediately before it (walking backward).  At
 * each step the side whose next candidate is closer to const wins; a tie is
 * broken toward the forward (>= const) side.  This is a two-way merge of two
 * monotonic distance streams, so it returns keys in exact increasing distance
 * and stops as soon as the caller's LIMIT is satisfied -- it never scans the
 * whole index.
 *
 * Each side is an independent position cursor over the leaf chain: a pinned
 * leaf buffer and the offset of the entry it last produced.  The forward
 * cursor starts at the first entry whose key is >= const on the center leaf
 * and steps toward higher keys; the backward cursor starts just before it and
 * steps toward lower keys.  A matched entry may be a LIST/POSTING holding
 * several heap TIDs at one key (hence one distance): those are emitted one per
 * gettuple call, all before the merge advances.
 *
 * ponytail: one ordering key only (the first ORDER BY <-> clause); multi-key
 * KNN would need a priority queue like GiST's.  A scalar B-tree has a single
 * distance axis, so one key is the whole useful case here.
 */
typedef struct BarkKnnCursor
{
	Buffer		buf;			/* pinned leaf, or InvalidBuffer when exhausted */
	OffsetNumber off;			/* offset last examined on buf */
	bool		backward;		/* true: this cursor walks toward lower keys */
	bool		primed;			/* true once positioned on its first entry */
	bool		centerleaf;		/* buf is still the shared center leaf (the one
								 * step where backward must start one entry before
								 * the forward cursor so the sides don't overlap) */
	OffsetNumber splitoff;		/* on the center leaf, the first offset whose key
								 * is >= center: forward starts here, backward one
								 * entry earlier (meaningful only when centerleaf) */
	bool		have;			/* a buffered candidate is ready in dist/key */
	double		dist;			/* distance of the buffered candidate */
	ItemPointer tids;			/* buffered candidate's heap locators */
	int			ntids;			/* number of them */
	int			ntidsAlloc;		/* capacity of tids */
	char	   *keytup;			/* buffered candidate's key tuple (IOS), or NULL */
} BarkKnnCursor;

typedef struct BarkKnnScanState
{
	FmgrInfo	distfn;			/* the ordering operator's distance function */
	Oid			distcollation;	/* its input collation */
	Datum		center;			/* the ORDER BY constant (sk_argument) */
	bool		centernull;		/* the constant is NULL (no rows ordered) */
	AttrNumber	attno;			/* index column being ordered (1-based) */
	BarkKnnCursor fwd;			/* forward (>= center) cursor */
	BarkKnnCursor bwd;			/* backward (< center) cursor */

	/* Members of the entry currently being emitted (one TID per gettuple). */
	ItemPointer emitTids;		/* locators of the entry being emitted */
	int			nEmit;			/* number of them */
	int			emitIdx;		/* next one to return */
	double		emitDist;		/* their shared distance */
	char	   *emitKey;			/* their shared key tuple (IOS), or NULL */
} BarkKnnScanState;

/*
 * Scan state (scan->opaque).  A BARK scan positions on a leaf and walks the
 * right-link chain, returning the heap TID of each entry that satisfies the
 * scan keys.  currentBuffer is the pinned (and, while reading, share-locked)
 * leaf; lastOffset is the offset of the item most recently returned on it
 * (InvalidOffsetNumber before the first item), so the next item is found by
 * stepping from lastOffset in the current scan direction -- which keeps scroll
 * cursors correct when the direction reverses.
 */
typedef struct BarkScanOpaqueData
{
	BarkKeyInfo *keyinfo;		/* key comparison state for this index */
	Buffer		currentBuffer;	/* current leaf, or InvalidBuffer */
	OffsetNumber lastOffset;	/* offset last returned on currentBuffer, or
								 * InvalidOffsetNumber before the first item */
	bool		firstCall;		/* true until the scan has been positioned */
	char	   *currTuple;		/* scratch copy of the returned index tuple for
								 * index-only scans (NULL when not wanted) */
	Size		currTupleSize;	/* allocated capacity of currTuple */

	/*
	 * Within-entry iteration for multi-locator entries (LIST, POSTING): a
	 * single leaf entry at lastOffset may expand into several heap TIDs, one
	 * returned per bark_gettuple call.  memberTids holds the entry's locators
	 * in ascending order; nMembers is how many, and memberIdx is the next one
	 * to return in the scan direction (-1 or nMembers means the entry is
	 * exhausted and the scan should advance to the next offset).  A SINGLE
	 * entry has nMembers == 1 and uses the same machinery.
	 */
	ItemPointer memberTids;		/* palloc'd locator buffer, or NULL */
	int			nMembersAlloc;	/* capacity of memberTids */
	int			nMembers;		/* locators in the current entry */
	int			memberIdx;		/* next locator to return */

	/*
	 * KNN (ordered-operator) scan state, allocated lazily on the first
	 * ordered gettuple when the scan has ORDER BY <-> keys; NULL for a plain
	 * scan, which takes exactly the same path as before.
	 */
	BarkKnnScanState *knn;
} BarkScanOpaqueData;

typedef BarkScanOpaqueData *BarkScanOpaque;

/*
 * Shared state for a parallel BARK scan, stored in DSM.  A BARK scan walks the
 * leaf right-link (or left-link, backward) chain; in parallel, workers claim
 * leaf pages one at a time from a shared cursor rather than each walking the
 * whole chain.  Only one worker advances the cursor at a time (seize/release),
 * so the next block each worker gets is distinct and every leaf is scanned by
 * exactly one worker.  Modeled on nbtree's BTParallelScanDescData, without the
 * ScalarArrayOp primitive-scan machinery BARK does not have.
 */
typedef enum BarkParallelState
{
	BARK_PARALLEL_NOT_INITIALIZED,	/* no worker has positioned the scan yet */
	BARK_PARALLEL_ADVANCING,		/* a worker is advancing the cursor */
	BARK_PARALLEL_IDLE,				/* cursor holds the next page to hand out */
	BARK_PARALLEL_DONE,				/* no pages remain (or error exit) */
} BarkParallelState;

typedef struct BarkParallelScanDescData
{
	BlockNumber bps_nextPage;	/* next leaf block to hand out, or BARK_P_NONE
								 * at the end of the chain */
	BarkParallelState bps_state;	/* coordination state (see above) */
	LWLock		bps_lock;		/* protects the fields above */
	ConditionVariable bps_cv;	/* workers wait here while another advances */
} BarkParallelScanDescData;

typedef struct BarkParallelScanDescData *BarkParallelScanDesc;

extern IndexScanDesc bark_beginscan(Relation index, int nkeys, int norderbys);
extern void bark_rescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
						ScanKey orderbys, int norderbys);
extern bool bark_gettuple(IndexScanDesc scan, ScanDirection dir);
extern bool bark_canreturn(Relation index, int attno);
extern int64 bark_getbitmap(IndexScanDesc scan, TIDBitmap *tbm);
extern void bark_endscan(IndexScanDesc scan);

/* KNN (ordered-operator) scan (barkknn.c). */
extern void bark_knn_rescan(IndexScanDesc scan, ScanKey orderbys, int norderbys);
extern bool bark_knn_gettuple(IndexScanDesc scan);
extern void bark_knn_endscan(IndexScanDesc scan);

/* Parallel scan (barkscan.c). */
extern Size bark_estimateparallelscan(Relation index, int nkeys, int norderbys);
extern void bark_initparallelscan(void *target);
extern void bark_parallelrescan(IndexScanDesc scan);

#endif							/* BARK_H */
