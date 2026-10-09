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
#include "access/transam.h"
#include "catalog/pg_am_d.h"
#include "catalog/pg_class.h"
#include "storage/block.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/condition_variable.h"
#include "storage/lwlock.h"
#include "utils/snapmgr.h"

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
 * the tree level, the vacuum cycle ID, and the page-role flags.  The last two
 * bytes of the special space hold a page-type identifier so pg_filedump and
 * amcheck-style tools can recognize a BARK page.
 *
 * bark_cycleid works as nbtree's btpo_cycleid does: a leaf split that happens
 * while VACUUM is scanning the index stamps both halves with that VACUUM's
 * cycle ID (zero when no VACUUM is running), so VACUUM can tell that entries
 * moved to a right sibling it may already have passed, and go back for them.
 * The values come from nbtree's cycle-ID registry (a BTCycleId).  nbtree keeps
 * its cycle ID in the last two bytes of the special space; BARK keeps
 * BARK_PAGE_ID there, so the cycle ID takes half of the old 32-bit level
 * field instead.  A 16-bit level is ample (a tree of 65536 levels cannot be
 * built), and the struct stays at 16 bytes, so the special space and
 * BarkMaxItemSize do not change.
 * ----------------------------------------------------------------------------
 */
typedef struct BarkPageOpaqueData
{
	BlockNumber bark_prev;		/* left sibling, or BARK_P_NONE if leftmost */
	BlockNumber bark_next;		/* right sibling, or BARK_P_NONE if rightmost */
	uint16		bark_level;		/* tree level; zero for leaf pages */
	uint16		bark_cycleid;	/* vacuum cycle ID of latest leaf split */
	uint16		bark_flags;		/* flag bits, see below */
	uint16		bark_page_id;	/* BARK_PAGE_ID, for tool identification */
} BarkPageOpaqueData;

typedef BarkPageOpaqueData *BarkPageOpaque;

#define BarkPageGetOpaque(page) \
	((BarkPageOpaque) PageGetSpecialPointer(page))

/*
 * The page ID is for the convenience of pg_filedump and similar utilities,
 * which otherwise would have a hard time telling pages of different index
 * types apart.  It is the last 2 bytes of the page.  The value must be above
 * MAX_BT_CYCLE_ID (0xFF7F): tools recognize an nbtree page by its last two
 * bytes, which hold the vacuum cycle ID and so never exceed that value, and
 * the other index AMs (hash 0xFF80, GiST 0xFF81, SP-GiST 0xFF82, bloom
 * 0xFF83) take the values just above it.
 */
#define BARK_PAGE_ID		0xFF84

/* Bits defined in bark_flags */
#define BARK_LEAF			(1 << 0)	/* leaf page (else internal) */
#define BARK_ROOT			(1 << 1)	/* root page (no parent) */
#define BARK_DELETED		(1 << 2)	/* page deleted from the tree */
#define BARK_META			(1 << 3)	/* meta page */
#define BARK_HALF_DEAD		(1 << 4)	/* empty but still linked in the tree */
#define BARK_INCOMPLETE_SPLIT (1 << 5)	/* right sibling's downlink is missing */
#define BARK_HAS_GARBAGE	(1 << 6)	/* page has known-dead entries */
#define BARK_OVERFLOW		(1 << 7)	/* holds a chunk of an oversized value */
#define BARK_PREFIX			(1 << 10)	/* leaf keys coded against a prefix */

#define BarkPageIsLeaf(opaque)		(((opaque)->bark_flags & BARK_LEAF) != 0)
#define BarkPageIsRoot(opaque)		(((opaque)->bark_flags & BARK_ROOT) != 0)
#define BarkPageIsDeleted(opaque)	(((opaque)->bark_flags & BARK_DELETED) != 0)
#define BarkPageIsMeta(opaque)		(((opaque)->bark_flags & BARK_META) != 0)
#define BarkPageIsOverflow(opaque)	(((opaque)->bark_flags & BARK_OVERFLOW) != 0)

/*
 * A deleted or half-dead page keeps valid sibling links but holds nothing a
 * reader should return; readers step over it, as nbtree does with P_IGNORE.
 */
#define BarkPageIgnore(opaque) \
	(((opaque)->bark_flags & (BARK_DELETED | BARK_HALF_DEAD)) != 0)

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

/*
 * BarkDeletedPageData is the page contents of a deleted page, as nbtree's
 * BTDeletedPageData is.  It records the next full transaction ID at the time
 * of deletion (safexid): the page cannot be reused until no snapshot that
 * might still hold a link to it exists, that is, until safexid is older than
 * every running transaction's xmin.
 *
 * nbtree stores this struct at the start of the tuple area and covers it with
 * pd_lower, so a deleted nbtree page appears to have line pointers and every
 * reader must test the deleted flag before reading items.  BARK stores it at
 * the end of the tuple area instead (pd_upper = pd_special minus its size)
 * and leaves the line pointer array empty (pd_lower = SizeOfPageHeaderData),
 * so a reader that reaches a deleted page through a stale link and does not
 * test the flag still sees an empty page, whose sibling links lead on into
 * the live tree.  The struct lies between pd_upper and pd_special, so a
 * full-page image, which omits the hole between pd_lower and pd_upper, keeps
 * it.
 *
 * A deleted page keeps the sibling links it had when it was deleted: a scan
 * or descent that read a link to it before the deletion moves right through
 * it, as nbtree's readers do through P_IGNORE pages.  A deleted overflow page
 * keeps its link to the next chunk of the chain it belonged to.
 */
typedef struct BarkDeletedPageData
{
	FullTransactionId safexid;	/* see BarkPageIsRecyclable() */
} BarkDeletedPageData;

static inline BarkDeletedPageData *
BarkPageGetDeletedContents(Page page)
{
	Assert(((PageHeader) page)->pd_upper ==
		   ((PageHeader) page)->pd_special -
		   MAXALIGN(sizeof(BarkDeletedPageData)));
	return (BarkDeletedPageData *) ((char *) page +
									((PageHeader) page)->pd_upper);
}

/*
 * Initialize `page` as an empty BARK page with the given sibling links,
 * level, flags and vacuum cycle ID.  Splits, new roots and their WAL redo all
 * build pages here, so a replayed page gets the opaque the primary wrote.
 */
static inline void
BarkPageInit(Page page, BlockNumber prev, BlockNumber next, uint32 level,
			 uint16 flags, uint16 cycleid)
{
	BarkPageOpaque opaque;

	PageInit(page, BLCKSZ, sizeof(BarkPageOpaqueData));
	opaque = BarkPageGetOpaque(page);
	opaque->bark_prev = prev;
	opaque->bark_next = next;
	opaque->bark_level = level;
	opaque->bark_cycleid = cycleid;
	opaque->bark_flags = flags;
	opaque->bark_page_id = BARK_PAGE_ID;
}

/*
 * Reinitialize `page` as a deleted page with sibling links `prev` and `next`
 * and deletion XID `safexid`.  BARK deletes only leaves and overflow pages, so
 * the level is zero, and BARK_DELETED is the only flag.  VACUUM and WAL redo
 * both build a deleted page here, from the same inputs, so the page a standby
 * replays is the page the primary wrote.
 */
static inline void
BarkPageSetDeleted(Page page, BlockNumber prev, BlockNumber next,
				   FullTransactionId safexid)
{
	BarkPageOpaque opaque;
	PageHeader	header = (PageHeader) page;

	PageInit(page, BLCKSZ, sizeof(BarkPageOpaqueData));
	opaque = BarkPageGetOpaque(page);
	opaque->bark_prev = prev;
	opaque->bark_next = next;
	opaque->bark_flags = BARK_DELETED;
	opaque->bark_page_id = BARK_PAGE_ID;
	header->pd_lower = SizeOfPageHeaderData;
	header->pd_upper = header->pd_special -
		MAXALIGN(sizeof(BarkDeletedPageData));

	BarkPageGetDeletedContents(page)->safexid = safexid;
}

static inline FullTransactionId
BarkPageGetDeleteXid(Page page)
{
	/* We only expect to be called with a deleted page */
	Assert(!PageIsNew(page));
	Assert(BarkPageIsDeleted(BarkPageGetOpaque(page)));

	return BarkPageGetDeletedContents(page)->safexid;
}

/*
 * Is a deleted page safe to reuse?  As nbtree's BTPageIsRecyclable.
 *
 * heaprel is the index's heap, which selects the visibility horizon.  It may
 * be NULL where the caller has none; the check then uses the horizon of
 * shared relations, which takes every database's snapshots into account and
 * so is never less safe, only slower to allow reuse.
 */
static inline bool
BarkPageIsRecyclable(Page page, Relation heaprel)
{
	Assert(!PageIsNew(page));

	/* Recycling okay iff page is deleted and safexid is old enough */
	if (BarkPageIsDeleted(BarkPageGetOpaque(page)))
	{
		FullTransactionId safexid = BarkPageGetDeleteXid(page);

		/*
		 * The page was deleted, but when?  If it was just deleted, a scan
		 * might have seen a link to it (a downlink, a sibling link or an
		 * overflow-chain link), and will read the page later.  As long as
		 * that can happen, the deleted page must stay as a tombstone.
		 *
		 * So check whether the deletion XID could still be visible to anyone.
		 * If not, no scan still in progress can have seen a link to the page,
		 * and it can be recycled.
		 */
		return GlobalVisCheckRemovableFullXid(heaprel, safexid);
	}

	return false;
}

#define BARK_P_HIKEY		((OffsetNumber) 1)	/* high key, if present */
#define BARK_P_FIRSTKEY		((OffsetNumber) 2)	/* first data item after it */

/*
 * Leaf prefix compression.
 *
 * A leaf of an index with the prefix_compression reloption, whose first key
 * column is of a varlena type, may be flagged BARK_PREFIX.  Such a page holds
 * one extra item, the PREFIX item, at BarkPagePrefixOff (where its first data
 * item would otherwise be), and its data items start one offset later; so
 * BarkPageFirstDataKey, which every loop over a page's data items starts
 * from, accounts for the flag.  The PREFIX item is an IndexTupleData header
 * (t_tid zero, t_info its size and nothing else, so that code that walks a
 * page's items by IndexTupleSize, such as split redo, steps over it) followed
 * by 1..BARK_PREFIX_MAX prefix bytes.  The prefix is chosen when the page is
 * written as a whole (by CREATE INDEX, or as one half of a split) and never
 * changes after that; a page's high key, which fixes the prefix item's
 * offset, also changes only in a split.
 *
 * On a BARK_PREFIX page, every SINGLE, LIST and POSTING entry whose first key
 * column is not NULL stores that column coded, as a varlena (1-byte header
 * when it fits one, else 4-byte) whose payload starts with a uint8 `shared`:
 *
 *   shared <= BARK_PREFIX_MAX: the value's data bytes are the page prefix's
 *                first `shared` bytes followed by the rest of the payload.
 *                The value's own varlena header is not stored: it is the
 *                1-byte header when the value fits one and the 4-byte header
 *                otherwise, the form index_form_tuple gives a value of a
 *                type it may pack.
 *   shared == BARK_PREFIX_RAW: the rest of the payload is the value exactly
 *                as it was stored, header included.  Used for a value that
 *                is compressed inline (index_form_tuple compresses values
 *                larger than TOAST_INDEX_TARGET), or whose header is not in
 *                the form above (a type that may not be packed).
 *
 * The bytes after the first column (later key columns, INCLUDE columns and
 * alignment padding) are kept as they are, moved to the first position after
 * the coded column whose distance from the original position is a multiple
 * of MAXALIGN, so that every alignment within them still holds and they need
 * no tuple descriptor to move.  When they are only the zero padding that
 * ends the key, they are left out, and decoding puts them back.  A LIST or
 * POSTING body follows the key at the new key end, and the body offset in
 * t_tid is changed to match.  NULL first columns, OVERSIZED entries, the
 * high key and pivots are stored as on any other page.
 *
 * Coding and decoding therefore depend only on the page's prefix and the
 * entry, not on the index's catalog entries, so WAL redo can repeat them,
 * and decoding gives back the plain entry byte for byte.  A coded entry is
 * at most MAXALIGN bytes larger than the plain one when it shares nothing
 * with the prefix (the count byte can cross an alignment boundary), and a
 * few more for a raw value.  Every reader goes through
 * BarkPageGetItem, which returns the plain entry; writers code an entry with
 * bark_prefix_encode just before placing it, and size it with
 * bark_coded_size.  An entry that shares less of the prefix than the others,
 * or none, is still stored correctly, so no insert, VACUUM or change of the
 * page's key range ever has to rewrite the page.
 */
#define BARK_PREFIX_MAX			126
#define BARK_PREFIX_RAW			127 /* shared count of a raw value */
#define BARK_PREFIX_SHARED_MASK	0x7F	/* shared count bits */
#define BARK_PREFIX_REST		0x80	/* bytes after the column follow */

/* A leaf whose items share less of its prefix than this, on average, has none. */
#define BARK_PREFIX_MIN_SHARED	4

#define BarkPageHasPrefix(opaque)	(((opaque)->bark_flags & BARK_PREFIX) != 0)
#define BarkPagePrefixOff(opaque) \
	(BarkPageRightmost(opaque) ? BARK_P_HIKEY : BARK_P_FIRSTKEY)
#define BarkPageFirstDataKey(opaque) \
	(BarkPagePrefixOff(opaque) + (BarkPageHasPrefix(opaque) ? 1 : 0))

/*
 * Workspace for BarkPageGetItem.  Every read of a data item (any item but
 * the high key) goes through that function rather than PageGetItem, so that
 * a page format whose items are stored encoded (leaf prefix compression) has
 * one place to decode them.  The workspace holds a decoded copy; the
 * returned tuple is valid until the workspace is reused or the page is
 * unlocked.  High keys, and items a caller changes in place, are read with
 * PageGetItem.
 */
typedef PGAlignedBlock BarkItemBuf;

extern IndexTuple bark_prefix_decode(Page page, IndexTuple itup,
									 BarkItemBuf *buf);

/*
 * Return the data item at `off` on `page` as a BARK entry: on a BARK_PREFIX
 * page, the entry decoded into `buf`; otherwise the on-page tuple, and `buf`
 * is not used.  Callers must not write through the result.
 */
static inline IndexTuple
BarkPageGetItem(Page page, OffsetNumber off, BarkItemBuf *buf)
{
	IndexTuple	itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, off));
	BarkPageOpaque opaque = BarkPageGetOpaque(page);

	if (unlikely(BarkPageHasPrefix(opaque)))
	{
		Assert(off > BarkPagePrefixOff(opaque));
		return bark_prefix_decode(page, itup, buf);
	}
	return itup;
}

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

/*
 * Fill factors, as in nbtree.  CREATE INDEX packs leaf pages to the index's
 * fillfactor reloption and internal pages to BARK_NONLEAF_FILLFACTOR, so the
 * first inserts after a build do not split every page they touch.  A split of
 * the rightmost page on a level leaves the left page at the same fill factor,
 * so ascending inserts fill pages as full as a build does; other splits divide
 * the space evenly (barksplitloc.c).
 *
 * A leaf page holding a single key value is split leaving the left page
 * BARK_SINGLEVAL_FILLFACTOR full, whether or not it is rightmost, provided no
 * later page holds the same key, as nbtree's single-value strategy does.
 * Equal keys are in heap TID order, and new rows mostly take higher heap
 * TIDs, so such a page mostly receives appends, which after the split go to
 * the right page; space left free on the left would mostly stay free.  Its
 * last LIST or POSTING entry is cut to reach that fill (bark_singleval_cut):
 * a split point falls between entries, and with entries a third of a page
 * wide the left page would otherwise be left nearly full.
 */
#define BARK_MIN_FILLFACTOR		10
#define BARK_DEFAULT_FILLFACTOR	90
#define BARK_NONLEAF_FILLFACTOR	70
#define BARK_SINGLEVAL_FILLFACTOR	96

/*
 * Parsed reloptions (barkoptions), stored in rd_options.  New options are
 * appended here and to the parse table in barkoptions.
 */
typedef struct BarkOptions
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int			fillfactor;		/* leaf page fill factor in percent (10..100) */
	bool		prefix_compression; /* code leaf keys against a page prefix? */
} BarkOptions;

#define BarkGetPrefixCompression(relation) \
	(AssertMacro(relation->rd_rel->relkind == RELKIND_INDEX && \
				 relation->rd_rel->relam == BARK_AM_OID), \
	 (relation)->rd_options ? \
	 ((BarkOptions *) (relation)->rd_options)->prefix_compression : false)

#define BarkGetFillFactor(relation) \
	(AssertMacro(relation->rd_rel->relkind == RELKIND_INDEX && \
				 relation->rd_rel->relam == BARK_AM_OID), \
	 (relation)->rd_options ? \
	 ((BarkOptions *) (relation)->rd_options)->fillfactor : \
	 BARK_DEFAULT_FILLFACTOR)
#define BarkGetTargetPageFreeSpace(relation) \
	(BLCKSZ * (100 - BarkGetFillFactor(relation)) / 100)

/* ----------------------------------------------------------------------------
 * Meta page
 *
 * Block 0 of a BARK index is always the meta page.  It points to the current
 * root and records the on-disk version so the format can evolve.
 * ----------------------------------------------------------------------------
 */
#define BARK_METAPAGE		0	/* block number of the meta page */
#define BARK_MAGIC			0x5241424B	/* "BARK" as a big-endian uint32 */
#define BARK_VERSION		2	/* current on-disk version */

/*
 * The oldest version this code reads.  Version 2 orders the entries of a run
 * of equal keys by heap TID and gives pivots a heap TID; a version 1 index
 * has neither, so it must be rebuilt with REINDEX (bark_get_root refuses it).
 */
#define BARK_MIN_VERSION	2

/*
 * BARK uses the btree strategy numbers and support-function convention: its
 * scalar operator classes live in the btree operator families (see
 * ambtreeopfamilies), so it shares btree's numbering.  Strategies 1..5 are
 * <, <=, =, >=, >.  Support function 1 is the ordering comparator (btree's
 * BTORDER_PROC), the only one BARK requires; the remaining btree support
 * functions (2..6: sortsupport, in_range, equalimage, options, skipsupport)
 * are optional and may be present in the shared family without BARK using
 * them.
 */
#define BARK_NSTRATEGIES	5	/* number of strategies (btree's set) */
#define BARK_NPROCS			6	/* btree's support-function range (BTNProcs) */
#define BARK_ORDER_PROC		1	/* support function 1: 3-way comparator */
#define BARK_SORTSUPPORT_PROC 2 /* support function 2: sortsupport */
#define BARK_INRANGE_PROC	3	/* support function 3: in_range */
#define BARK_EQUALIMAGE_PROC 4	/* support function 4: equalimage (BTEQUALIMAGE_PROC) */
#define BARK_OPTIONS_PROC	5	/* support function 5: options */
#define BARK_SKIPSUPPORT_PROC 6 /* support function 6: skipsupport */

/*
 * Multikey operator classes ("M1: multikey keys" in BARK-Design.mediawiki).
 * A class in an operator family of BARK's own (opfmethod = bark, never a
 * btree family) that has procedure 7 extracts several keys of its storage
 * type from one column value; such a column is an extracted column.
 * Procedures 1, 2, 4, 5 and 6 keep btree's meanings over the key type.
 * Procedures 7-9 are DocumentDB's pg_extended_btree interface (its 101-103),
 * 10-13 GIN's extractQuery, consistent, comparePartial and triConsistent
 * (its 3-6), and 14 GiST's fetch.  amsupport covers all fourteen, and
 * amstrategies is 0, so a BARK family's operators may carry any strategy
 * number; the number only reaches procedures 8 and 9.
 */
#define BARK_EXTRACTVALUE_PROC			7
#define BARK_EXTRACTQUERY_PROC			8
#define BARK_INDEXRECHECK_PROC			9
#define BARK_GIN_EXTRACTQUERY_PROC		10
#define BARK_GIN_CONSISTENT_PROC		11
#define BARK_GIN_COMPAREPARTIAL_PROC	12
#define BARK_GIN_TRICONSISTENT_PROC		13
#define BARK_FETCH_PROC					14
#define BARK_MULTIKEY_NPROCS			14

/*
 * The types procedures 8 and 9 exchange with BARK.  They are laid out as
 * pg_extended_btree's, so a class written for it ports by renumbering its
 * procedures.
 *
 * Procedure 8 returns an array of boundaries.  Each is a lower and an upper
 * search element; a NULL pointer means unbounded on that side, and a point
 * has BTEqualStrategyNumber in both.  The argument is of the class's
 * storage (key) type and the strategy is a btree strategy, 1-5.
 */
typedef struct BarkSearchElement
{
	Datum		argument;
	StrategyNumber strategy;
} BarkSearchElement;

typedef struct BarkBoundary
{
	BarkSearchElement *lower;
	BarkSearchElement *upper;
} BarkBoundary;

/*
 * Procedure 8's optional seventh argument.  BARK zeroes it before the call;
 * a class declared with six arguments never sees it.  backward asks an
 * ordering operator's scan to walk its boundaries in descending key order.
 * searchnulls asks the scan to read the NULL entries as well (a NULL value,
 * an empty value or a NULL element), where the column's NULLS option puts
 * them, and to return those rows with xs_recheck set, as GIN's
 * INCLUDE_EMPTY search mode does.  Fields are only ever appended.
 */
typedef struct BarkQueryFlags
{
	bool		backward;
	bool		searchnulls;
} BarkQueryFlags;

/* Procedure 9's answer for one entry, as an int2. */
typedef enum BarkRecheckResult
{
	BARK_RECHECK_TRUE = 0,
	BARK_RECHECK_MAYBE = 1,
	BARK_RECHECK_FALSE = 2,
} BarkRecheckResult;

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
 * so it shares the status-bit region and must never be set on a PIVOT: on a
 * pivot the same bit is BARK_PIVOT_HEAP_TID, so the two meanings never meet.
 *
 * Within a run of equal keys, leaf entries are ordered by heap TID, as
 * nbtree's are since version 4: each entry covers a range of heap TIDs (see
 * bark_entry_tid_range), and the ranges of successive entries of one key are
 * disjoint and ascending, across pages too.  A pivot separating two entries
 * of the same key carries the first right entry's lowest TID.
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
#define BARK_PIVOT_HEAP_TID			0x4000	/* pivot carries a heap TID; the
											 * same bit as
											 * BARK_IS_DELETE_MARKED, which is
											 * never set on a pivot */
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

/* Number of key attributes recorded in a pivot tuple (not the heap TID). */
static inline uint16
BarkPivotGetNAtts(const IndexTupleData *itup)
{
	return (ItemPointerGetOffsetNumberNoCheck(&itup->t_tid) &
			BARK_OFFSET_MASK);
}

/*
 * The heap TID of a plain PIVOT, or NULL when it has none.  A pivot keeps one
 * only when it separates two entries equal on every key attribute (so it has
 * all of them); it is the last ItemPointerData of the MAXALIGNed tuple, after
 * the key data, as in nbtree.  A pivot without one has its heap TID truncated
 * away: minus infinity.
 */
static inline ItemPointer
BarkPivotGetHeapTID(IndexTupleData *itup)
{
	if ((ItemPointerGetOffsetNumberNoCheck(&itup->t_tid) &
		 BARK_PIVOT_HEAP_TID) == 0)
		return NULL;
	return (ItemPointer) ((char *) itup + IndexTupleSize(itup) -
						  sizeof(ItemPointerData));
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
 * block field (as for LIST).  The member count is not stored in the offset
 * field -- it can exceed BARK_OFFSET_MASK -- but is read back from the sbm;
 * the offset low bits are left zero.
 *
 * The body is a uint16 holding the serialization's length, the serialization,
 * and zero padding to the end of the entry.  The entry is sized for the sbm's
 * removal bound (sbm_removal_bound), the largest serialization any subset of
 * the set can have, rather than for the serialization itself: removing
 * members can enlarge an sbm, and this way VACUUM can always rewrite the
 * entry in place.  The sbm format does not record its own length, so it is
 * stored ahead of it.  sbm_deserialize copies the bytes before reading them,
 * so the serialization needs no alignment.
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

/*
 * Size of a POSTING entry whose key tuple is keysz bytes and whose set has
 * removal bound `bound`.
 */
static inline Size
BarkPostingEntrySize(Size keysz, Size bound)
{
	return MAXALIGN(keysz) + MAXALIGN(sizeof(uint16) + bound);
}

/* Pointer to a POSTING entry's serialized sbm. */
static inline uint8 *
BarkPostingGetData(IndexTupleData *itup)
{
	return (uint8 *) itup + BarkEntryGetBodyOffset(itup) + sizeof(uint16);
}

/*
 * Length in bytes of a POSTING entry's serialized sbm.  Readers check it
 * against the entry size before using it.
 */
static inline Size
BarkPostingGetDataSize(IndexTupleData *itup)
{
	return *(uint16 *) ((char *) itup + BarkEntryGetBodyOffset(itup));
}

/* True when a POSTING entry's length word and sbm lie within the entry. */
static inline bool
BarkPostingDataFits(IndexTupleData *itup)
{
	Size		bodyoff = BarkEntryGetBodyOffset(itup);

	return bodyoff + sizeof(uint16) <= IndexTupleSize(itup) &&
		bodyoff + sizeof(uint16) + BarkPostingGetDataSize(itup) <=
		IndexTupleSize(itup);
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
 * Bytes of the first key column kept inline in an OVERSIZED entry for the
 * compare fast path (see BarkOverflowRef.prefix).  A few dozen bytes resolve
 * the vast majority of oversized-key orderings without an overflow fetch while
 * keeping the inline entry tiny.
 */
#define BARK_OVERSIZED_PREFIX_LEN	32

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

	/*
	 * A pivot's heap TID, as BarkPivotGetHeapTID's for a plain pivot; invalid
	 * when the pivot has none (or for a leaf entry, whose TID is locator).
	 */
	ItemPointerData pivottid;

	/*
	 * Inline comparison prefix: the leading bytes of the first key column's
	 * datum, used to order two OVERSIZED entries without fetching the overflow
	 * chain when the prefixes already decide the order.  Only populated (and
	 * only consulted) when the first key column compares bytewise -- text
	 * under a C/POSIX collation -- where a leading-byte difference
	 * determines the full order.  For any other column (locale-aware text,
	 * bpchar, arrays, name, etc.) prefixlen is 0 and comparison always
	 * fetches the full tuple.  prefixcomplete is true when the whole first
	 * column fit in the prefix, so an exhausted prefix with equal bytes is a
	 * genuine tie on that column rather than a truncation.
	 */
	uint16		prefixlen;		/* bytes of prefix stored (0 = no prefix) */
	bool		prefixcomplete; /* the whole first column fit in the prefix */
	uint8		prefix[BARK_OVERSIZED_PREFIX_LEN];
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

/* Point a pivot entry, whatever its shape, at child block `blkno`. */
static inline void
BarkEntrySetDownLink(IndexTupleData *itup, BlockNumber blkno)
{
	if (BarkEntryGetShape(itup) == BARK_SHAPE_OVERSIZED)
		ItemPointerSetBlockNumber(&BarkOverflowGetRef(itup)->locator, blkno);
	else
		ItemPointerSetBlockNumber(&itup->t_tid, blkno);
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
	bool		bytewise;		/* first-column prefix compare is byte-exact
								 * (text under a C/POSIX collation); enables
								 * the OVERSIZED inline-prefix fast path */
} BarkKeyColumn;

typedef struct BarkKeyInfo
{
	Relation	heaprel;		/* the index's heap, or NULL; set by writers
								 * for the pages a split allocates (see
								 * bark_get_free_page) */
	int			nkeys;			/* number of key columns */
	BarkKeyColumn cols[FLEXIBLE_ARRAY_MEMBER];
} BarkKeyInfo;

extern BarkKeyInfo *bark_build_keyinfo(Relation index);
extern bool bark_allequalimage(Relation index);
extern bool bark_column_is_extracted(Relation index, int attno);
extern int	bark_index_extracted_column(Relation index);
extern bool bark_opfamily_extracts(Oid opfamily, Oid opcintype);
extern void bark_check_multikey_index(Relation index, IndexInfo *indexInfo);
extern int	bark_compare_itups(BarkKeyInfo *keyinfo, Relation index,
							   IndexTuple a, IndexTuple b);
extern int	bark_compare_itups_tid(BarkKeyInfo *keyinfo, Relation index,
								   IndexTuple key, ItemPointer scantid,
								   IndexTuple itup);
extern ItemPointer bark_pivot_heap_tid(IndexTuple pivot);
extern void bark_entry_tid_range(IndexTuple itup, ItemPointer lo,
								 ItemPointer hi);
extern int	bark_keep_natts(Relation index, BarkKeyInfo *keyinfo,
							IndexTuple lastleft, IndexTuple firstright);

/*
 * Allocate a page for the index, preferring a page the FSM says is free
 * (recorded by VACUUM once a page it deleted became safe to reuse) over
 * extending the relation.  Returns a pinned, exclusive-locked buffer whose page
 * the caller must (re)initialize; a recycled page is handed back still flagged
 * BARK_DELETED (or PageIsNew), so the caller's PageInit overwrites it.  This is
 * what keeps a delete-heavy index from growing the relation without bound: a
 * split or overflow write reuses a reclaimed page instead of extending.
 * heaprel is the index's heap relation; every caller has it, through
 * BarkKeyInfo.heaprel where the call is reached from a descent.
 */
extern Buffer bark_get_free_page(Relation index, Relation heaprel);
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

/*
 * Leaf prefix compression (barkutils.c; see BARK_PREFIX for the format).
 *
 * bark_prefix_encode returns `itup` coded for `page`, palloc'd, or `itup`
 * itself when the page has no prefix or the entry is stored plain.
 * bark_coded_size is the MAXALIGNed size the entry takes on `page`.
 * bark_page_set_prefix adds the PREFIX item to a page that holds nothing
 * after its high key yet, and flags the page.  bark_prefix_shared is the
 * length of the common prefix of `prefix` and the entry's first-column
 * bytes, or -1 when the entry would not be coded; bark_prefix_candidate
 * gives the bytes a page prefix may be taken from (NULL when it has none).
 * bark_prefix_choose decides, for the items a page is about to be built
 * from, whether to give it a prefix, and returns the prefix's length (0 for
 * none) with *prefix pointing into items[0].
 */
extern IndexTuple bark_prefix_encode(Page page, IndexTuple itup);
extern Size bark_coded_size(Page page, IndexTuple itup);
extern void bark_page_set_prefix(Page page, const char *prefix, Size len);
extern const char *bark_page_get_prefix(Page page, Size *len);
extern int	bark_prefix_shared(const char *prefix, Size prefixlen,
							   IndexTuple itup);
extern const char *bark_prefix_candidate(IndexTuple itup, Size *len);
extern bool bark_prefix_enabled(Relation index);
extern Size bark_prefix_choose(Relation index, IndexTuple *items, int n,
							   const char **prefix);

/* The left half of a page split, for bark_split and its redo (barkutils.c). */
extern bool bark_split_build_left(Page page, Page origpage, IndexTuple hikey,
								  OffsetNumber firstrightoff,
								  IndexTuple newitem, OffsetNumber newitemoff,
								  IndexTuple replaceitem,
								  OffsetNumber replaceoff);

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
 * the `ntids` ascending locators in `tids`, sized for the set's removal
 * bound; it returns NULL when that entry would not be smaller than the
 * equivalent LIST (caller keeps the LIST).  bark_posting_count /
 * bark_posting_get_tids read a POSTING entry's set back.
 */
extern uint64 bark_tid_to_key(ItemPointer tid);
extern void bark_key_to_tid(uint64 key, ItemPointer tid);
extern IndexTuple bark_form_posting(TupleDesc tupdesc, IndexTuple key,
									ItemPointer tids, int ntids);
extern IndexTuple bark_posting_add_tid(IndexTuple key, IndexTuple posting,
									   ItemPointer newtid, Size maxsz);

/*
 * A LIST or POSTING entry with one more heap TID, or NULL if it would not fit
 * in maxsz; deterministic in its arguments, so WAL replay can repeat it.
 */
extern IndexTuple bark_entry_add_tid(IndexTuple entry, ItemPointer tid,
									 Size maxsz);
extern bool bark_entry_has_tid(IndexTuple itup, ItemPointer tid);
extern void bark_entry_swap_tid(IndexTuple entry, ItemPointer tid,
								IndexTuple *left, IndexTuple *right);
extern bool bark_entry_cut(IndexTuple entry, ItemPointer tid, Size leftmax,
						   IndexTuple *left, IndexTuple *right);
extern IndexTuple bark_form_entry(IndexTuple key, ItemPointer tids, int n);
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
extern void bark_set_oversized_prefix(IndexTuple entry, Relation index,
									  IndexTuple full);

/*
 * Write `full` (fulllen bytes) across a chain of BARK_OVERFLOW pages via the
 * buffer pool (bark_get_free_page, XLOG_BARK_OVERFLOW records), returning
 * the first block.  heaprel is passed on to bark_get_free_page.
 */
extern BlockNumber bark_write_overflow_chain(Relation index, Relation heaprel,
											 IndexTuple full, Size fulllen);

/*
 * Lay out an overflow page holding `len` bytes of a tuple, `chunk`, linked to
 * `nextblk`.  Used by bark_write_overflow_chain and its redo.
 */
extern void bark_init_overflow_page(Page page, const char *chunk, Size len,
									BlockNumber nextblk);

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
 * deleted pages, recycled once safe), as part of VACUUM removing the owning
 * leaf entry.  Each page is freed in its own XLOG_BARK_MARK_DELETED record.
 * Returns the number of pages freed.
 */
extern BlockNumber bark_free_oversized(Relation index, IndexTuple entry);

/*
 * Bottom-up deletion, re-forming entries that lose members, and merging a
 * leaf's equal-key entries before a split (barkdelete.c).
 */
extern Size bark_leaf_free_space(Page page);
extern bool bark_bottomup_delete(Relation index, Relation heapRel,
								 BarkKeyInfo *keyinfo, Buffer buf,
								 IndexTuple newitem, Size newitemsz);
extern IndexTuple bark_reform_entry(Relation index, Buffer buf,
									OffsetNumber off, IndexTuple itup,
									ItemPointer tids, int nlive);
extern bool bark_merge_page(Relation index, BarkKeyInfo *keyinfo, Buffer buf,
							OffsetNumber newitemoff, Size newitemsz);

extern IndexBuildResult *bark_build(Relation heap, Relation index,
									IndexInfo *indexInfo);
extern void bark_buildempty(Relation index);
extern bool barkvalidate(Oid opclassoid);
extern void barkadjustmembers(Oid opfamilyoid, Oid opclassoid,
							  List *operators, List *functions);

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
 * key is an index tuple whose key columns are compared with bark_compare_itups;
 * with a heap TID `scantid`, (key, scantid) is compared with
 * bark_compare_itups_tid, which places it within a run of equal keys, and
 * nextkey must be true.
 *
 * nextkey chooses which leaf a run of equal keys lands on: true (insert / true
 * key) descends to the rightmost leaf that can hold the key; false (lower-bound
 * scan) descends to the leftmost such leaf, so a forward equality or
 * lower-bound scan does not skip earlier duplicates when a run of equal keys
 * spans several leaves.
 */
extern Buffer bark_search(Relation index, BarkKeyInfo *keyinfo,
						  IndexTuple key, ItemPointer scantid, bool forwrite,
						  bool nextkey, BarkStack *stack);

/*
 * A scan's bound on the leading key columns, for descending to the leaf
 * where the scan starts (nbtree's insertion scan key built by _bt_first).
 * Column i (0-based) is compared as procs[i](index value, args[i]), the
 * opfamily's ORDER proc for the column type and the argument's type, so an
 * argument of another type in the opfamily (an int4 constant against an
 * int8 column) compares correctly.  The bound has a value for the first
 * nkeys columns only; on the columns after them it is minus infinity, or
 * plus infinity when `upper` (so that every entry equal to it on the bounded
 * columns sorts before it).
 */
typedef struct BarkScanBound
{
	int			nkeys;
	bool		upper;
	Datum		args[INDEX_MAX_KEYS];
	FmgrInfo   *procs[INDEX_MAX_KEYS];
	Oid			collations[INDEX_MAX_KEYS];
} BarkScanBound;

/*
 * Descend to the leaf where a scan with this bound starts, share-locked;
 * InvalidBuffer for an empty index.  nextkey as for bark_search.
 */
extern OffsetNumber bark_binsrch_bound(Relation index, BarkKeyInfo *keyinfo,
									   const BarkScanBound *bound, Page page);
extern Buffer bark_search_bound(Relation index, BarkKeyInfo *keyinfo,
								const BarkScanBound *bound, bool nextkey);
extern int	bark_compare_bound(Relation index, BarkKeyInfo *keyinfo,
							   const BarkScanBound *bound, IndexTuple itup);
/*
 * What a backend caches about an index in its relcache entry's rd_amcache.
 * A relcache rebuild (REINDEX, TRUNCATE, an invalidation) drops it.
 *
 * root and level are the root as last read from the meta page (root is
 * BARK_P_NONE until then), so that a descent need not read the meta page,
 * as nbtree caches its meta page.  They may be stale: bark_get_root_buffer
 * checks the page it names before using it.
 *
 * extracted is the key column whose operator class extracts several keys
 * from a value (bark_index_extracted_column), 0 for none, or -1 until first
 * asked.  It comes from the catalog, which an index keeps for its life.
 */
typedef struct BarkAmCache
{
	BlockNumber root;
	uint32		level;
	int			extracted;
} BarkAmCache;

extern BarkAmCache *bark_get_amcache(Relation index);
extern BlockNumber bark_get_root(Relation index, uint32 *level_out);
extern uint32 bark_get_root_level(Relation index);
extern Buffer bark_get_root_buffer(Relation index, BufferLockMode access);
extern void bark_freestack(BarkStack stack);

extern bool bark_insert(Relation index, Datum *values, bool *isnull,
						 ItemPointer ht_ctid, Relation heapRel,
						 IndexUniqueCheck checkUnique,
						 bool indexUnchanged, IndexInfo *indexInfo);

/* Leaf high key for a split between lastleft and firstright (barkinsert.c). */
extern IndexTuple bark_truncate_pivot(Relation index, BarkKeyInfo *keyinfo,
									  IndexTuple lastleft,
									  IndexTuple firstright);

/* Split-point choice for bark_split (barksplitloc.c). */
extern int	bark_findsplitloc(Relation index, BarkKeyInfo *keyinfo,
							  IndexTuple *items, const Size *sizes,
							  Size reserve, int n, int newitemidx,
							  bool isleaf, IndexTuple orighikey);
extern bool bark_singleval_cut(Relation index, BarkKeyInfo *keyinfo,
							   Page page, OffsetNumber off, IndexTuple newitem,
							   IndexTuple *left, IndexTuple *right);

/*
 * Finish the interrupted split of `lbuf` (write-locked, flagged
 * BARK_INCOMPLETE_SPLIT) by inserting its right sibling's downlink into the
 * parent; `stack` is the parent path to `lbuf`.  Releases `lbuf`.
 */
extern void bark_finish_split(Relation index, BarkKeyInfo *keyinfo,
							  Buffer lbuf, BarkStack stack);

/*
 * KNN (ordered-operator) scan state (barkknn.c).
 *
 * `ORDER BY col <~> const` over a scalar B-tree key is answered by descending
 * to const and expanding OUTWARD in both directions: the nearest key values
 * to const are the ones immediately at/after it (walking the leaf chain
 * forward) merged with the ones immediately before it (walking backward).  At
 * each step the side whose next candidate is closer to const wins; a tie is
 * broken toward the forward (>= const) side.  This is a two-way merge of two
 * monotonic distance streams, so it returns keys in exact increasing distance
 * and stops as soon as the caller's LIMIT is satisfied -- it never scans the
 * whole index.
 *
 * Each side is a cursor that reads the leaf chain a page at a time, as a
 * plain scan's BarkScanPosData does: under one share lock it copies every
 * matching entry on its current page, in its own direction, into items[]
 * (with the entry's heap TIDs in tids[] and, for an index-only scan, its key
 * in the tuples workspace), records the page's sibling link in its direction,
 * and releases the lock.  The merge then takes candidates from these copies
 * without touching the page again, so a concurrent insert or split on it
 * cannot make the scan repeat or skip entries; the cursor moves on by the
 * saved link once its copy is used up.  On the center leaf the forward cursor
 * copies the entries from the first one whose key is >= const upward and the
 * backward cursor the entries before it, downward, both under the lock the
 * descent left on the page.
 *
 * A matched entry may be a LIST/POSTING holding several heap TIDs at one key
 * (hence one distance): those are emitted one per gettuple call, all before
 * the merge advances.
 *
 * This handles one ordering key (the first ORDER BY <~> clause); multi-key
 * KNN would need a priority queue like GiST's, but a scalar B-tree has a
 * single distance axis, so one key is the whole useful case here.  KNN is
 * likewise never parallel -- that is intrinsic to a single-center outward
 * merge, not a deferred optimization; see the barkknn.c header for why a
 * two-sided split would buy nothing on an output-bounded scan.
 */
typedef struct BarkKnnItem
{
	double		dist;			/* distance of the entry's key from const */
	int			firstTid;		/* the entry's heap TIDs are tids[firstTid]... */
	int			ntids;			/* ...and there are this many, ascending */
	uint32		tupleOffset;	/* entry's key copy in tuples (IOS only) */
} BarkKnnItem;

typedef struct BarkKnnCursor
{
	bool		backward;		/* true: this cursor walks toward lower keys */
	Buffer		buf;			/* currPage, pinned; InvalidBuffer if unpinned */

	/* page details as of the read that filled items[] */
	BlockNumber currPage;		/* page read, or InvalidBlockNumber if none */
	BlockNumber nextPage;		/* its sibling in our direction when read;
								 * BARK_P_NONE when no page is left to read */

	/*
	 * Distance of the last entry examined on currPage, matching or not: the
	 * entries on the pages not yet read are no closer to const than this.
	 * Minus infinity before any entry has been examined.
	 */
	double		bound;

	BarkKnnItem *items;			/* matching entries of currPage, nearest first */
	int			nitems;			/* number of them */
	int			itemIndex;		/* next one to merge (== nitems: used up) */
	int			maxItems;		/* allocated size of items */

	ItemPointer tids;			/* heap TIDs of all the items */
	int			ntids;			/* number of them */
	int			maxTids;		/* allocated size of tids */

	char	   *tuples;			/* IOS: key copies of the items, or NULL */
	uint32		tuplesSize;		/* allocated size of tuples */
	uint32		nextTupleOffset;	/* first free byte in tuples */
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

	/*
	 * The cursor whose current item (items[itemIndex]) is being returned, one
	 * heap TID per gettuple call, or NULL; emitIdx is the member returned
	 * last.  The cursor moves past the item only once all its members are
	 * returned, so it never reads its next page under an item in flight.
	 */
	BarkKnnCursor *emitCur;
	int			emitIdx;
} BarkKnnScanState;

/*
 * ScalarArrayOp (SAOP) scan state.  When amsearcharray lets the planner push a
 * `col = ANY(array)` / `col IN (...)` qual into a BARK scan, the executor hands
 * us the scankey with SK_SEARCHARRAY set and sk_argument carrying the array
 * Datum.  bark_rescan preprocesses each such key into one BarkArrayKeyState:
 * the array's elements, sorted into the index's key order for that column and
 * de-duplicated, so a scan can visit the matching keys in index order (ORDER
 * BY stays correct) exactly as a merged sequence of equality scans would.
 *
 * Every array key filters per tuple by membership (bark_array_contains, a
 * binary search over `elems`), which alone makes the scan correct.  The
 * required keys also drive positioning (nbtree's required array keys): on
 * index columns 1..m, each with an equality array or a scalar equality (an
 * array of one element, built for the purpose), stopping at the first column
 * with neither, each key's `cur` is one digit of an odometer.  The
 * combinations of the digits' elements, in column order, are in index order,
 * and the scan visits them in turn, descending to a combination when it
 * starts beyond the page being read, so it skips the gaps between them
 * instead of filtering every tuple in between.
 *
 * The elements may be of another type than the column (an int4 column with
 * an int8[] array), so they are compared through two ORDER procs from the
 * column's opfamily, as in nbtree's _bt_setup_array_cmp: sortproc compares
 * two elements and only sorts and de-duplicates them; cmpproc compares a
 * column value (first argument) with an element, for everything the scan
 * does with them.  Both are the column's own comparator when the types
 * agree.  Only equality arrays get this state: bark_rescan reduces an
 * inequality array (col < ANY(array)) to a plain key on its extreme element,
 * unless it has no non-NULL element, when it stays an array with no elements.
 */
typedef struct BarkArrayKeyState
{
	int			scankeyidx;		/* index into scan->keyData of the SAOP key */
	AttrNumber	attno;			/* 1-based index column the array constrains */
	FmgrInfo	sortproc;		/* ORDER proc for (element, element) */
	FmgrInfo	cmpproc;		/* ORDER proc for (column, element) */
	Datum	   *elems;			/* sorted, de-duplicated array elements */
	int			nelems;			/* number of them (0: empty array, no matches) */
	int			cur;			/* required key: element the scan is on */
} BarkArrayKeyState;

/*
 * Scan position (modeled on nbtree's BTScanPosData).  A scan reads one leaf at
 * a time: under a single share lock bark_readpage copies every matching heap
 * TID on the page into items[], then releases the lock, and bark_gettuple
 * returns items from that local copy without touching the page again.  A
 * concurrent insert or split on the leaf therefore cannot shift the scan's
 * place on it; the scan moves on by the sibling links it saved when it read
 * the page.
 *
 * items[] is always in index order (ascending offset, and ascending TID within
 * one LIST or POSTING entry), whichever direction the page was read in;
 * itemIndex is the item most recently returned.  It is grown as needed rather
 * than sized like nbtree's MaxTIDsPerBTreePage, because one POSTING entry can
 * expand to far more TIDs than that bound.
 *
 * For an index-only scan each matching entry's key tuple is copied once into
 * the scan's tuple workspace (currTuples), and every member item refers to it
 * by tupleOffset.  The workspace can exceed 64kB when a page holds OVERSIZED
 * entries (each resolves to its full tuple), so tupleOffset is wider than
 * nbtree's LocationIndex.
 */
typedef struct BarkScanPosItem
{
	ItemPointerData heapTid;	/* one member TID */
	OffsetNumber indexOffset;	/* entry's offset on the page when read */
	uint32		tupleOffset;	/* entry's copy in currTuples (IOS only) */
} BarkScanPosItem;

typedef struct BarkScanPosData
{
	Buffer		buf;			/* currPage, pinned; InvalidBuffer if unpinned */

	/* page details as of the bark_readpage call that filled items[] */
	BlockNumber currPage;		/* page read, or InvalidBlockNumber if none */
	BlockNumber prevPage;		/* currPage's bark_prev when read */
	BlockNumber nextPage;		/* currPage's bark_next when read */
	ScanDirection dir;			/* direction the page was read in */

	/* may there be matching entries left / right of currPage? */
	bool		moreLeft;
	bool		moreRight;

	/*
	 * The required keys' cursors (so->reqKeys[k]->cur) as of the end of the
	 * read, and whether the next step in dir should re-descend to their
	 * combination rather than read the sibling page.  Kept in the position,
	 * not only in the array state, so that restoring a mark restores where
	 * the scan goes next.  A skip scan also sets arrayReseek.
	 */
	int			arrayCur[INDEX_MAX_KEYS];
	bool		arrayReseek;

	int			firstItem;		/* first valid index in items[] */
	int			lastItem;		/* last valid index in items[] */
	int			itemIndex;		/* item most recently returned */
	uint32		nextTupleOffset;	/* first free byte in currTuples */

	BarkScanPosItem *items;		/* palloc'd, maxItems entries */
	int			maxItems;
} BarkScanPosData;

typedef BarkScanPosData *BarkScanPos;

/* A position is valid once a page has been read into it (nbtree's rules). */
#define BarkScanPosIsValid(pos)		BlockNumberIsValid((pos).currPage)
#define BarkScanPosIsPinned(pos)	BufferIsValid((pos).buf)
#define BarkScanPosUnpinIfPinned(pos) \
	do { \
		if (BarkScanPosIsPinned(pos)) \
		{ \
			ReleaseBuffer((pos).buf); \
			(pos).buf = InvalidBuffer; \
		} \
	} while (0)
#define BarkScanPosInvalidate(pos) \
	do { \
		(pos).buf = InvalidBuffer; \
		(pos).currPage = InvalidBlockNumber; \
	} while (0)

/*
 * Scan state (scan->opaque).
 */
typedef struct BarkScanOpaqueData
{
	BarkKeyInfo *keyinfo;		/* key comparison state for this index */
	bool		firstCall;		/* KNN: true until the scan has been positioned */
	MemoryContext scanCxt;		/* context the scan was begun in */

	/*
	 * Per scan key, the three-way ORDER proc that compares the indexed column
	 * with the key's argument, used to stop the scan at a bound; fn_oid is
	 * InvalidOid for a key that only filters.  See bark_setup_key_procs.
	 * Built on the first rescan: the keys' types do not change after that.
	 */
	FmgrInfo   *keyCmp;
	bool		keyCmpReady;

	/*
	 * Drop the leaf pin, not only the lock, once a page has been read?  See
	 * bark_rescan for when this is safe.
	 */
	bool		dropPin;

	BarkScanPosData currPos;	/* current position */

	/*
	 * Mark/restore (nbtree's markItemIndex/markPos).  A mark on the page in
	 * currPos only records markItemIndex; bark_steppage copies currPos into
	 * markPos when the scan is about to leave that page.  So a mark is in
	 * markItemIndex when it is >= 0, else in markPos.  markPos has its own
	 * items array and, for an index-only scan, its own tuple workspace.
	 */
	int			markItemIndex;	/* itemIndex, or -1 if not valid */
	BarkScanPosData markPos;	/* marked position, if any */

	/* index-only scans: key tuples of the entries in currPos/markPos.items */
	char	   *currTuples;		/* palloc'd workspace, or NULL */
	uint32		currTuplesSize; /* its allocated size */
	char	   *markTuples;		/* palloc'd workspace, or NULL */
	uint32		markTuplesSize; /* its allocated size */

	/* scratch buffer for one entry's member TIDs while reading a page */
	ItemPointer entryTids;
	int			entryTidsAlloc;

	/*
	 * ScalarArrayOp (SAOP) state: one BarkArrayKeyState per SK_SEARCHARRAY
	 * scankey, built by bark_rescan.  numArrayKeys == 0 for a plain scan.
	 * reqKeys[k] is the required key on index column k + 1, for k <
	 * numReqKeys (see BarkArrayKeyState); numReqKeys is 0 unless one of them
	 * is an array.  All of it lives in arrayCxt, which each rescan resets.
	 */
	MemoryContext arrayCxt;		/* child of scanCxt, or NULL if never needed */
	BarkArrayKeyState *arrayKeys;	/* palloc'd array, or NULL */
	int			numArrayKeys;	/* number of SAOP keys */
	BarkArrayKeyState **reqKeys;	/* required keys, or NULL */
	int			numReqKeys;		/* number of them */

	/*
	 * Skip scan: no key on column 1, but a key that bounds column 2.  A
	 * forward read whose page ends inside a column-1 group, on an entry past
	 * column 2's bounds (or before them), re-descends to the next group (or
	 * to the bounds within the same group) rather than reading every page in
	 * between; bark_readpage builds that descent's bound in skipBound and
	 * sets the position's arrayReseek.  skipValue is skipBound's copy of the
	 * column-1 value, freed when replaced unless skipValueByVal.
	 */
	bool		skip;
	bool		skipReseeking;	/* the next descent uses skipBound */
	bool		skipValueByVal;
	Datum		skipValue;
	BarkScanBound skipBound;

	/*
	 * Column 1's skip support (BTSKIPSUPPORT_PROC), or NULL when its opclass
	 * has none: for a discrete type it gives the next possible value, so the
	 * scan can descend straight to (next value, column 2's lower bound).
	 * skipSupportReady says whether it has been looked up yet.
	 */
	struct SkipSupportData *skipSupport;
	bool		skipSupportReady;

	/*
	 * KNN (ordered-operator) scan state, allocated by the first rescan of a
	 * scan with ORDER BY <~> keys; NULL for a plain scan.
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
	BlockNumber bps_nextPage;	/* next leaf block to hand out, BARK_P_NONE at
								 * the end of the chain, or InvalidBlockNumber
								 * before the scan is positioned */
	BlockNumber bps_lastPage;	/* the page bps_nextPage was linked from; a
								 * backward step validates against it */
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
extern bool bark_tuple_matches(IndexScanDesc scan, IndexTuple itup,
							   uint64 skipkeys);
extern int64 bark_getbitmap(IndexScanDesc scan, TIDBitmap *tbm);
extern void bark_endscan(IndexScanDesc scan);
extern void bark_markpos(IndexScanDesc scan);
extern void bark_restrpos(IndexScanDesc scan);

/* Leaf-read helpers shared by the plain and KNN scans (barkscan.c). */
extern IndexTuple bark_scan_resolve(Relation index, IndexTuple itup,
									bool *fetched);
extern uint32 bark_save_tuple(BarkScanOpaque so, char **tuples,
							  uint32 *tuplesSize, uint32 *nextoff,
							  IndexTuple entry, IndexTuple resolved);
extern Buffer bark_lock_and_validate_left(Relation index, BlockNumber *blkno,
										  BlockNumber lastcurrblkno);

/* KNN (ordered-operator) scan (barkknn.c). */
extern void bark_knn_rescan(IndexScanDesc scan, ScanKey orderbys, int norderbys);
extern bool bark_knn_gettuple(IndexScanDesc scan);
extern void bark_knn_endscan(IndexScanDesc scan);

/* Parallel scan (barkscan.c). */
extern Size bark_estimateparallelscan(Relation index, int nkeys, int norderbys);
extern void bark_initparallelscan(void *target);
extern void bark_parallelrescan(IndexScanDesc scan);

#endif							/* BARK_H */
