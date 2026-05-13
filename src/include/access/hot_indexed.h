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
 * The natts==0 predicate is safe because every heap tuple body has at
 * least one user attribute serialised into it: system attributes have
 * negative attnums and are never stored in the heap tuple body, so a
 * legitimate user-data tuple always has HeapTupleHeaderGetNatts >= 1.
 * Tombstones therefore carry the unique signature natts == 0 +
 * HEAP_INDEXED_UPDATED that no real tuple can produce.
 *
 * (Pedantic note: pg_attribute itself contains entries with attnum < 1
 * for system attrs.  Those are pg_attribute *rows*, each row's body
 * still has natts >= 1 -- the row is describing a system attribute, not
 * stored as one.)
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
 *     t_target        = back-pointer to the live hot-indexed tuple's offset
 *                       (duplicate of t_ctid.offnum; t_ctid is read by
 *                       amcheck/verify_heapam during structural validation
 *                       while t_target is the cheap-access path used by
 *                       reader code that already has the tombstone in
 *                       hand)
 *     uint16 t_nbytes    -- bitmap byte count
 *     uint8  t_bitmap[t_nbytes]
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
#include "storage/itemptr.h"

/*
 * HotIndexedTombstonePayload -- the bytes that follow a tombstone's
 * HeapTupleHeader, starting at t_hoff.
 *
 * Writers must MAXALIGN the structure when computing its on-page size.
 */
typedef struct HotIndexedTombstonePayload
{
	uint16		t_target;		/* offnum of the live hot-indexed tuple */
	uint16		t_nbytes;		/* bitmap byte count */
	uint8		t_bitmap[FLEXIBLE_ARRAY_MEMBER];
}			HotIndexedTombstonePayload;

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
	return HotIndexedTombstoneGetPayloadConst(tup)->t_target;
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

extern bool heap_hot_indexed_tombstone_attr_modified(const HotIndexedTombstonePayload * p,
													 AttrNumber attnum);

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
 *   - HotIndexedTombstonePayload.t_target is at offset 0 of the payload
 *     (so at page offset t_hoff of the tombstone item).
 *   - The payload header is exactly 4 bytes (two uint16 fields).
 *   - A tombstone carrying a bitmap for MaxHeapAttributeNumber attributes
 *     still fits within a uint16 byte-count and within a uint8 t_hoff.
 */
StaticAssertDecl(offsetof(HotIndexedTombstonePayload, t_target) == 0,
				 "HotIndexedTombstonePayload layout changed");
StaticAssertDecl(SizeOfHotIndexedTombstonePayload == 4,
				 "HotIndexedTombstonePayload header size changed");
StaticAssertDecl(MAXALIGN(SizeofHeapTupleHeader) <= UINT8_MAX,
				 "tombstone t_hoff will overflow");

#endif							/* HOT_INDEXED_H */
