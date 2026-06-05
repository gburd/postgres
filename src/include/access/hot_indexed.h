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
 * HeapPageItemKind -- classification of an LP_NORMAL heap page item.
 *
 * Every place that interprets the bytes behind an LP_NORMAL line pointer
 * MUST decide what kind of item it is through heap_page_item_kind() (or the
 * thin Is*() wrappers below), so the discriminator lives in exactly one
 * place.  The kinds are:
 *
 *   HPIK_TUPLE             ordinary heap tuple (including a plain HOT tuple);
 *                          HEAP_INDEXED_UPDATED is clear.
 *   HPIK_HOT_INDEXED_TUPLE live heap tuple produced by a HOT-indexed update
 *                          (HEAP_INDEXED_UPDATED set, natts > 0).
 *   HPIK_TOMBSTONE         meta-item carrying a modified-attrs bitmap for a
 *                          live hot-indexed tuple (natts == 0, t_ctid.blockno
 *                          = InvalidBlockNumber).
 *   HPIK_BRIDGE            meta-item left by pruneheap in a dead mid-chain
 *                          slot (natts == 0, t_ctid = valid same-page forward
 *                          link).
 *
 * The (HEAP_INDEXED_UPDATED set AND natts == 0) pair is a signature no real
 * tuple can produce (no genuine tuple sets HEAP_INDEXED_UPDATED with natts ==
 * 0).  Meta-items are additionally always written HEAP_XMIN_INVALID |
 * HEAP_XMAX_INVALID; the classifier asserts that invariant so a corrupt or
 * mis-cast header is caught rather than silently treated as a tombstone
 * (amcheck enforces it in production builds).
 */
typedef enum HeapPageItemKind
{
	HPIK_TUPLE,
	HPIK_HOT_INDEXED_TUPLE,
	HPIK_TOMBSTONE,
	HPIK_BRIDGE,
} HeapPageItemKind;

/*
 * heap_page_item_kind
 *		The single discriminator for LP_NORMAL heap page items.
 *
 * Caller must first establish that the item is LP_NORMAL (so the bytes at
 * PageGetItem() can be interpreted as a HeapTupleHeader).
 */
static inline HeapPageItemKind
heap_page_item_kind(const HeapTupleHeaderData *tup)
{
	if ((tup->t_infomask2 & HEAP_INDEXED_UPDATED) == 0)
		return HPIK_TUPLE;

	if (HeapTupleHeaderGetNatts(tup) != 0)
		return HPIK_HOT_INDEXED_TUPLE;

	/*
	 * natts == 0 with HEAP_INDEXED_UPDATED is the meta-item signature.
	 * Genuine meta-items are always written XMIN/XMAX-invalid; assert that
	 * here so the discriminator is robust against a corrupt header.
	 */
	Assert((tup->t_infomask & (HEAP_XMIN_INVALID | HEAP_XMAX_INVALID)) ==
		   (HEAP_XMIN_INVALID | HEAP_XMAX_INVALID));

	if (BlockNumberIsValid(ItemPointerGetBlockNumberNoCheck(&tup->t_ctid)))
		return HPIK_BRIDGE;

	return HPIK_TOMBSTONE;
}

/*
 * HeapTupleHeaderIsHotIndexedTombstone
 *		True iff a HeapTupleHeader describes a meta-item (tombstone or bridge).
 *
 * Thin wrapper over heap_page_item_kind().  Callers must first establish that
 * the item is LP_NORMAL.
 */
static inline bool
HeapTupleHeaderIsHotIndexedTombstone(const HeapTupleHeaderData *tup)
{
	HeapPageItemKind kind = heap_page_item_kind(tup);

	return kind == HPIK_TOMBSTONE || kind == HPIK_BRIDGE;
}

/*
 * HeapTupleHeaderIsHotIndexedBridge
 *		True iff a HeapTupleHeader describes a bridge meta-item.
 *
 * Bridges are written by pruneheap in place of a dead mid-chain HOT-indexed
 * heap-only tuple: the LP stays LP_NORMAL but t_ctid carries a valid forward
 * link (same-page blockno, real offset) so chain walkers can continue through
 * the hop.  Adjacent-to-live tombstones, by contrast, set t_ctid.blockno =
 * InvalidBlockNumber; that is the discriminator between the two meta-item
 * variants.  Thin wrapper over heap_page_item_kind().
 */
static inline bool
HeapTupleHeaderIsHotIndexedBridge(const HeapTupleHeaderData *tup)
{
	return heap_page_item_kind(tup) == HPIK_BRIDGE;
}

/*
 * AssertIsGenuineHeapTuple
 *		Assert-the-negative guard for paths that are about to interpret an
 *		LP_NORMAL item as a real tuple (deform, visibility, key extraction):
 *		the item must be a genuine tuple, never a tombstone/bridge meta-item.
 *		Compiles away entirely in non-assert builds.
 */
static inline void
AssertIsGenuineHeapTuple(const HeapTupleHeaderData *tup)
{
	Assert(!HeapTupleHeaderIsHotIndexedTombstone(tup));
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
 * Inline-trailing modified-attrs bitmap on a live HOT-indexed version.
 *
 * A live heap tuple produced by a HOT-indexed update (HPIK_HOT_INDEXED_TUPLE:
 * HEAP_INDEXED_UPDATED set, natts > 0) carries its producing hop's
 * modified-attrs bitmap appended after the user data:
 *
 *   [HeapTupleHeader][null bitmap][user data][modified-attrs bitmap]
 *
 * The bitmap width is derived from the tuple's natts (one bit per attribute,
 * attnum-1 indexing -- identical layout to the tombstone payload bitmap), so
 * no length field is stored; the bitmap occupies the final
 * HotIndexedInlineBitmapNbytes() bytes of the on-page item.  Reading it needs
 * only the item length (lp_len, == the chain walker's heapTuple->t_len), so
 * lookup is O(1) and needs no tuple descriptor.  heap_deform_tuple ignores the
 * trailing bytes (it bounds attribute extraction by the descriptor and null
 * bitmap, never by lp_len).
 */
static inline uint16
HotIndexedInlineBitmapNbytes(const HeapTupleHeaderData *tup)
{
	return (HeapTupleHeaderGetNatts(tup) + 7) / 8;
}

/*
 * HotIndexedInlineGetBitmap
 *		Pointer to the trailing modified-attrs bitmap of a live HOT-indexed
 *		version.  item_len is the on-page item length (lp_len / t_len).
 *
 * Caller must have verified heap_page_item_kind(tup) == HPIK_HOT_INDEXED_TUPLE.
 */
static inline const uint8 *
HotIndexedInlineGetBitmap(const HeapTupleHeaderData *tup, uint32 item_len)
{
	return (const uint8 *) tup + item_len - HotIndexedInlineBitmapNbytes(tup);
}

/*
 * Write-side API (implemented in src/backend/access/heap/hot_indexed.c).
 */
extern Size heap_build_hot_indexed_tombstone(char *buf,
											 OffsetNumber target_offnum,
											 int natts,
											 const Bitmapset *modified_attrs);

/*
 * heap_fill_hot_indexed_inline_bitmap
 *		Write a natts-wide modified-attrs bitmap into dest (which must have
 *		HotIndexedInlineBitmapNbytes() bytes for an natts-attribute relation),
 *		using the same attnum-1 bit layout as the tombstone payload.
 */
extern void heap_fill_hot_indexed_inline_bitmap(uint8 *dest, int natts,
												const Bitmapset *modified_attrs);

extern bool heap_hot_indexed_tombstone_attr_modified(const HotIndexedTombstonePayload *p,
													 AttrNumber attnum);

/*
 * heap_hot_indexed_bitmap_overlaps
 *		True iff any heap attribute in index_attrs is marked modified by the
 *		raw modified-attrs bitmap (nbytes wide), using the same attnum-1 layout
 *		and FirstLowInvalidHeapAttributeNumber-offset index_attrs convention as
 *		heap_hot_indexed_payload_overlaps.  Serves both the tombstone payload
 *		and a live version's inline-trailing bitmap.
 */
extern bool heap_hot_indexed_bitmap_overlaps(const uint8 *bitmap, uint16 nbytes,
											 const Bitmapset *index_attrs);

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
