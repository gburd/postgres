/*-------------------------------------------------------------------------
 *
 * hot_indexed.h
 *	  Definitions for HOT-indexed (SIU) tombstone items.
 *
 * A HOT-indexed update is an update that modifies one or more indexed
 * columns but is stored as a heap-only tuple on the same page as the
 * old tuple.  Index entries pointing to the root of the HOT chain can
 * become "stale" relative to the new indexed-column values; index scans
 * use a per-update bitmap of modified indexed attributes to detect
 * stale entries during chain following.
 *
 * The bitmap is carried by a "tombstone" LP_NORMAL line pointer placed
 * adjacent to the live SIU tuple on the same page.  The tombstone is
 * marked invisible (HEAP_XMIN_INVALID) so generic visibility checks
 * skip it, and is distinguished from a real tuple by
 *
 *   (t_infomask2 & HEAP_INDEXED_UPDATED) != 0 AND
 *   HeapTupleHeaderGetNatts(tup) == 0
 *
 * The natts==0 predicate is safe because every relation must have at
 * least one user attribute.
 *
 * On-disk layout of a tombstone item (starting at PageGetItem):
 *
 *   HeapTupleHeaderData
 *     t_ctid.blockno  = InvalidBlockNumber  (tombstone is not part of any
 *                                             HOT chain or visibility walk)
 *     t_ctid.offnum   = back-pointer to the live SIU tuple's offset
 *     t_infomask      = HEAP_XMIN_INVALID | HEAP_XMAX_INVALID
 *     t_infomask2     = HEAP_INDEXED_UPDATED   (natts bits zero)
 *     t_hoff          = MAXALIGN(SizeofHeapTupleHeader)
 *     t_bits[]        = absent (HEAP_HASNULL not set)
 *
 *   Starting at t_hoff:
 *     uint16 t_target    -- duplicate of t_ctid.offnum for cheap access
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
	uint16		t_target;		/* offnum of the live SIU tuple */
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
 *		True iff a HeapTupleHeader describes a tombstone item.
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
 *		Offset number of the live SIU tuple this tombstone describes.
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
