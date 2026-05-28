/*-------------------------------------------------------------------------
 *
 * hot_indexed.h
 *	  Definitions for HOT-indexed tombstone items.
 *
 * A HOT-indexed update is an update that modifies one or more indexed
 * columns but is stored as a heap-only tuple on the same page as the
 * old tuple.  Index entries pointing to the root of the HOT chain can
 * become "stale" relative to the new indexed-column values; index scans
 * use a per-update bitmap of modified indexed attributes to detect
 * stale entries during chain following.
 *
 * The bitmap is carried by a "tombstone" LP_NORMAL line pointer placed
 * adjacent to the live hot-indexed tuple on the same page.  The tombstone is
 * marked invisible (HEAP_XMIN_INVALID) so generic visibility checks
 * skip it, and is distinguished from a real tuple by
 *
 *   (t_infomask2 & HEAP_INDEXED_UPDATED) != 0 AND
 *   HeapTupleHeaderGetNatts(tup) == 0
 *
 * The natts==0 predicate is safe in conjunction with HEAP_INDEXED_UPDATED:
 * no real heap tuple ever sets HEAP_INDEXED_UPDATED in t_infomask2, so the
 * pair (HEAP_INDEXED_UPDATED set AND natts == 0) is a signature no genuine
 * tuple can produce.  Note that natts == 0 on its own is NOT exclusive to
 * tombstones: a zero-column relation (CREATE TABLE t()) stores rows whose
 * bodies have natts == 0.  Such relations have no indexable attributes and
 * cannot be the subject of a HOT-indexed update, so they never carry the
 * HEAP_INDEXED_UPDATED bit; the conjunction is what makes the discriminator
 * unambiguous.
 *
 * On-disk layout of a tombstone item (starting at PageGetItem):
 *
 *   HeapTupleHeaderData
 *     t_ctid.blockno  = InvalidBlockNumber  (tombstone is not part of any
 *                                             HOT chain or visibility walk)
 *     t_ctid.offnum   = back-pointer to the live hot-indexed tuple's offset
 *     t_infomask      = HEAP_XMIN_INVALID | HEAP_XMAX_INVALID
 *     t_infomask2     = HEAP_INDEXED_UPDATED   (natts bits zero)
 *     t_hoff          = MAXALIGN(SizeofHeapTupleHeader)
 *     t_bits[]        = absent (HEAP_HASNULL not set)
 *
 *   Starting at t_hoff:
 *     uint16 t_nbytes    -- bitmap byte count
 *     uint8  t_bitmap[t_nbytes]
 *
 *   The target offset (the live hot-indexed tuple this tombstone describes)
 *   is carried in t_ctid.offnum; it is not duplicated in the payload.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/hot_indexed.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef HOT_INDEXED_H
#define HOT_INDEXED_H

#include "access/htup_details.h"
#include "nodes/bitmapset.h"
#include "storage/bufpage.h"
#include "storage/itemid.h"
#include "storage/itemptr.h"

/*
 * HotIndexedRedirectData -- data carried by a HOT-indexed "data redirect".
 *
 * At a page prune that collapses a HOT-indexed chain, the dead chain root is
 * collapsed to an LP_REDIRECT that, instead of being a plain zero-length
 * redirect, reuses the dead root tuple's freed bytes to store this structure:
 * the redirect target plus the union of the modified-attrs bitmaps of every
 * hop from the root to the target.  The target is always first_live (a live
 * tuple, never a reclaimable bridge), so the redirect never dangles when an
 * intermediate hop is later reclaimed.  A leaf pointing at the root reads the
 * union directly to decide staleness, without traversing the chain; for a
 * single-hop chain the live successor's adjacent tombstone is then reclaimed,
 * its bitmap now living here.  The line pointer has lp_flags = LP_REDIRECT and
 * lp_len > 0 (a plain HOT redirect has lp_len == 0); lp_off addresses these
 * bytes, so PageRepairFragmentation relocates them like any stored item.
 * Because lp_off no longer holds the target offset, the target is read from
 * rd_target via HotIndexedRedirectGetTarget().
 */
typedef struct HotIndexedRedirectData
{
	OffsetNumber rd_target;		/* redirect target (live successor offset) */
	uint16		rd_nbytes;		/* modified-attrs bitmap byte count */
	uint8		rd_bitmap[FLEXIBLE_ARRAY_MEMBER];
} HotIndexedRedirectData;

#define SizeOfHotIndexedRedirectData \
	offsetof(HotIndexedRedirectData, rd_bitmap)

static inline Size
HotIndexedRedirectSize(int natts)
{
	return MAXALIGN(SizeOfHotIndexedRedirectData + ((natts + 7) / 8));
}

/*
 * HotIndexedRedirectIsData
 *		True iff a line pointer is a HOT-indexed data redirect (an
 *		LP_REDIRECT carrying a HotIndexedRedirectData blob).  A plain HOT
 *		redirect has lp_len == 0; a data redirect has lp_len > 0.
 */
static inline bool
HotIndexedRedirectIsData(ItemId lp)
{
	return ItemIdIsRedirected(lp) && ItemIdHasStorage(lp);
}

static inline HotIndexedRedirectData *
HotIndexedRedirectGetData(Page page, ItemId lp)
{
	return (HotIndexedRedirectData *) PageGetItem(page, lp);
}

/*
 * HotIndexedRedirectGetTarget
 *		Offset the redirect resolves to, for either a plain HOT redirect
 *		(lp_off) or a HOT-indexed data redirect (rd_target in the blob).
 *		Use this in place of ItemIdGetRedirect() wherever a same-page chain
 *		walk may encounter a data redirect.
 */
static inline OffsetNumber
HotIndexedRedirectGetTarget(Page page, ItemId lp)
{
	if (HotIndexedRedirectIsData(lp))
		return HotIndexedRedirectGetData(page, lp)->rd_target;
	return ItemIdGetRedirect(lp);
}

/*
 * HotIndexedTombstonePayload -- the bytes that follow a tombstone's
 * HeapTupleHeader, starting at t_hoff.
 *
 * Writers must MAXALIGN the structure when computing its on-page size.
 */
typedef struct HotIndexedTombstonePayload
{
	uint16		t_nbytes;		/* bitmap byte count */
	uint8		t_bitmap[FLEXIBLE_ARRAY_MEMBER];
} HotIndexedTombstonePayload;

#define SizeOfHotIndexedTombstonePayload \
	offsetof(HotIndexedTombstonePayload, t_bitmap)

/*
 * HotIndexedTombstoneSize
 *		On-page size (including header, payload, and MAXALIGN padding)
 *		of a tombstone carrying a natts-wide bitmap.
 */
static inline Size
HotIndexedTombstoneSize(int natts)
{
	Size		hoff = MAXALIGN(SizeofHeapTupleHeader);
	Size		payload = SizeOfHotIndexedTombstonePayload + ((natts + 7) / 8);

	return MAXALIGN(hoff + payload);
}

/*
 * HeapTupleHeaderIsHotIndexedTombstone
 *		True iff a HeapTupleHeader describes a tombstone item (of either
 *		variant: adjacent or bridge).
 *
 * Callers must first establish that the item is LP_NORMAL (so the bytes
 * at PageGetItem() can be interpreted as a HeapTupleHeader).
 */
static inline bool
HeapTupleHeaderIsHotIndexedTombstone(const HeapTupleHeaderData *tup)
{
	return (tup->t_infomask2 & HEAP_INDEXED_UPDATED) != 0 &&
		HeapTupleHeaderGetNatts(tup) == 0;
}

/*
 * HeapTupleHeaderIsHotIndexedBridge
 *		True iff a HeapTupleHeader describes a bridge tombstone.
 *
 * Bridges are written by pruneheap in place of a dead mid-chain
 * HOT-indexed heap-only tuple: the LP stays LP_NORMAL with
 * HeapTupleHeaderIsHotIndexedTombstone, but t_ctid carries a valid
 * forward link (same-page blockno, real offset) so chain walkers can
 * continue through the hop.  Adjacent-to-live tombstones, by contrast,
 * set t_ctid.blockno = InvalidBlockNumber; that is the discriminator.
 *
 * Callers that need to tell the two variants apart (the chain walker,
 * vacuum's bridge reclaim, pageinspect) use this predicate.  The plain
 * "is tombstone" predicate above still matches both variants, which is
 * what prune_handle_tombstones() and the adjacent-tombstone post-
 * processing want.
 */
static inline bool
HeapTupleHeaderIsHotIndexedBridge(const HeapTupleHeaderData *tup)
{
	return HeapTupleHeaderIsHotIndexedTombstone(tup) &&
		BlockNumberIsValid(ItemPointerGetBlockNumberNoCheck(&tup->t_ctid));
}

/*
 * HotIndexedBridgeGetForward
 *		Return the on-page offset that a bridge tombstone forwards to.
 *
 * Caller must have verified HeapTupleHeaderIsHotIndexedBridge(tup).
 * The block number is implicit (same page as the bridge itself); callers
 * only need the offset to continue the chain walk.
 */
static inline OffsetNumber
HotIndexedBridgeGetForward(const HeapTupleHeaderData *tup)
{
	return ItemPointerGetOffsetNumberNoCheck(&tup->t_ctid);
}

/*
 * HotIndexedTombstoneGetPayload
 *		Return the payload pointer within a tombstone HeapTupleHeader.
 *
 * Caller must have verified HeapTupleHeaderIsHotIndexedTombstone(tup).
 */
static inline HotIndexedTombstonePayload *
HotIndexedTombstoneGetPayload(HeapTupleHeaderData *tup)
{
	return (HotIndexedTombstonePayload *) ((char *) tup + tup->t_hoff);
}

static inline const HotIndexedTombstonePayload *
HotIndexedTombstoneGetPayloadConst(const HeapTupleHeaderData *tup)
{
	return (const HotIndexedTombstonePayload *) ((const char *) tup + tup->t_hoff);
}

/*
 * HotIndexedTombstoneGetTarget
 *		Offset number of the live hot-indexed tuple this tombstone describes.
 */
static inline OffsetNumber
HotIndexedTombstoneGetTarget(const HeapTupleHeaderData *tup)
{
	return ItemPointerGetOffsetNumberNoCheck(&tup->t_ctid);
}

/*
 * HotIndexedTombstoneGetBitmap
 *		Pointer to the raw bitmap bytes in a tombstone.
 */
static inline const uint8 *
HotIndexedTombstoneGetBitmap(const HeapTupleHeaderData *tup)
{
	return HotIndexedTombstoneGetPayloadConst(tup)->t_bitmap;
}

/*
 * HotIndexedTombstoneGetNbytes
 *		Size of the bitmap in bytes.
 */
static inline uint16
HotIndexedTombstoneGetNbytes(const HeapTupleHeaderData *tup)
{
	return HotIndexedTombstoneGetPayloadConst(tup)->t_nbytes;
}

/*
 * Write-side API (implemented in src/backend/access/heap/hot_indexed.c).
 */
extern Size heap_build_hot_indexed_tombstone(char *buf,
											 OffsetNumber target_offnum,
											 int natts,
											 const Bitmapset *modified_attrs);

extern bool heap_hot_indexed_tombstone_attr_modified(const HotIndexedTombstonePayload *p,
													 AttrNumber attnum);

/*
 * heap_hot_indexed_payload_overlaps
 *		True iff any heap attribute in index_attrs is marked modified by the
 *		tombstone payload.  index_attrs uses the FirstLowInvalidHeapAttribute-
 *		Number offset convention (as returned by RelationGetIndexedAttrs); the
 *		read-side recheck uses this to decide whether a chain hop changed an
 *		attribute the arriving index covers.
 */
extern bool heap_hot_indexed_payload_overlaps(const HotIndexedTombstonePayload *p,
											   const Bitmapset *index_attrs);

/*
 * heap_hot_indexed_redirect_overlaps
 *		Like heap_hot_indexed_payload_overlaps but for a HOT-indexed data
 *		redirect's bitmap (HotIndexedRedirectData.rd_bitmap).
 */
extern bool heap_hot_indexed_redirect_overlaps(const HotIndexedRedirectData *rd,
											   const Bitmapset *index_attrs);

/*
 * heap_build_hot_indexed_bridge
 *		Populate *buf with a bridge tombstone that forwards chain walkers
 *		from a dead mid-chain HOT-indexed LP to the next on-page chain
 *		member.
 *
 * Arguments:
 *	 buf			- output buffer; caller must guarantee at least
 *					  HOT_INDEXED_BRIDGE_SIZE bytes of addressable,
 *					  writable memory.
 *	 blkno			- block number of the page the bridge will occupy.
 *					  Used to build a same-page forward ItemPointer that
 *					  chain walkers can consume without an extra lookup.
 *	 forward_offnum - offset of the next chain member on the same page.
 *
 * Returns the total number of bytes written (HOT_INDEXED_BRIDGE_SIZE).
 *
 * Bridges carry no modified-attrs bitmap; readers arriving via a stale
 * btree entry at the bridge's LP follow the forward link to the live
 * tuple and recheck the key against the live tuple's current index
 * form.  The per-hop bitmap that adjacent tombstones carry is not needed
 * here because the bridge did not emit that update; it is merely a
 * forwarding vestige of one.
 */
extern Size heap_build_hot_indexed_bridge(char *buf,
										  BlockNumber blkno,
										  OffsetNumber forward_offnum);

/*
 * HOT_INDEXED_BRIDGE_SIZE
 *		On-page size of a bridge tombstone.  No payload beyond the
 *		header, so a bridge is exactly MAXALIGN(SizeofHeapTupleHeader)
 *		bytes regardless of the owning relation's attribute count.
 */
#define HOT_INDEXED_BRIDGE_SIZE		(MAXALIGN(SizeofHeapTupleHeader))

/*
 * Compile-time layout sanity:
 *   - t_nbytes is at offset 0 of the payload (so at page offset t_hoff of
 *     the tombstone item).
 *   - The payload header is exactly 2 bytes (one uint16 field); the target
 *     offset lives in t_ctid.offnum, not the payload.
 *   - A tombstone carrying a bitmap for MaxHeapAttributeNumber attributes
 *     still fits within a uint16 byte-count and within a uint8 t_hoff.
 */
StaticAssertDecl(offsetof(HotIndexedTombstonePayload, t_nbytes) == 0,
				 "HotIndexedTombstonePayload layout changed");
StaticAssertDecl(SizeOfHotIndexedTombstonePayload == 2,
				 "HotIndexedTombstonePayload header size changed");
StaticAssertDecl(MAXALIGN(SizeofHeapTupleHeader) <= UINT8_MAX,
				 "tombstone t_hoff will overflow");

#endif							/* HOT_INDEXED_H */
