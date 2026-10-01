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
#include "storage/bufpage.h"
#include "storage/block.h"

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

#define BarkPageIsLeaf(opaque)		(((opaque)->bark_flags & BARK_LEAF) != 0)
#define BarkPageIsRoot(opaque)		(((opaque)->bark_flags & BARK_ROOT) != 0)
#define BarkPageIsDeleted(opaque)	(((opaque)->bark_flags & BARK_DELETED) != 0)
#define BarkPageIsMeta(opaque)		(((opaque)->bark_flags & BARK_META) != 0)

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
 * BARK uses the btree strategy numbers and support-function convention (its
 * operator families are btree operator families; see ambtreeopfamilies).
 * Strategies 1..5 are <, <=, =, >=, >; support function 1 is the ordering
 * comparator (btree's BTORDER_PROC).
 */
#define BARK_NSTRATEGIES	5	/* number of strategies (btree's set) */
#define BARK_NPROCS			1	/* number of support functions */
#define BARK_ORDER_PROC		1	/* support function 1: 3-way comparator */

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

extern IndexBuildResult *bark_build(Relation heap, Relation index,
									IndexInfo *indexInfo);
extern void bark_buildempty(Relation index);
extern bool barkvalidate(Oid opclassoid);

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
 */
extern Buffer bark_search(Relation index, BarkKeyInfo *keyinfo,
						  IndexTuple key, bool forwrite, BarkStack *stack);
extern void bark_freestack(BarkStack stack);

extern bool bark_insert(Relation index, Datum *values, bool *isnull,
						 ItemPointer ht_ctid, Relation heapRel,
						 IndexUniqueCheck checkUnique,
						 bool indexUnchanged, IndexInfo *indexInfo);

#endif							/* BARK_H */
