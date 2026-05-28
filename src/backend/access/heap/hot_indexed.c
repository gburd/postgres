/*-------------------------------------------------------------------------
 *
 * hot_indexed.c
 *	  Helpers for HOT-indexed (HOT-indexed update) tombstone items.
 *
 * See access/hot_indexed.h for the on-disk layout and design rationale.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/heap/hot_indexed.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/hot_indexed.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "nodes/bitmapset.h"
#include "storage/block.h"
#include "storage/itemptr.h"

/*
 * Compile-time bound on the tombstone item size for the worst-case
 * attribute count (MaxHeapAttributeNumber user columns => 200-byte bitmap
 * + 4-byte payload header + MAXALIGN(SizeofHeapTupleHeader) header,
 * MAXALIGN'ed).  RelationGetHotIndexedChainMax() in relcache.c sizes its
 * page-budget heuristic against this same upper bound, so the assertion
 * also pins the relcache.c estimate to the actual on-disk format.
 *
 * HotIndexedTombstoneSize() is a static inline, so we expand its body
 * here rather than calling it (StaticAssertDecl requires a constant
 * expression).
 */
StaticAssertDecl(MAXALIGN(MAXALIGN(SizeofHeapTupleHeader) +
						  SizeOfHotIndexedTombstonePayload +
						  ((MaxHeapAttributeNumber + 7) / 8)) <= 256,
				 "HotIndexedTombstoneSize upper bound has grown");

/*
 * heap_build_hot_indexed_tombstone
 *		Populate *buf with a tombstone item (header + payload) describing
 *		the per-update modified-indexed-attrs bitmap for a HOT-indexed
 *		update.
 *
 * Arguments:
 *   buf			 - output buffer; caller must guarantee at least
 *					   HotIndexedTombstoneSize(natts) bytes of addressable,
 *					   writable memory.
 *   target_offnum	 - offset number of the live hot-indexed tuple this tombstone
 *					   describes (must be a valid OffsetNumber).
 *   natts			 - number of user attributes in the owning relation;
 *					   must match RelationGetNumberOfAttributes at the call
 *					   site.  Governs bitmap byte width.
 *   modified_attrs	 - Bitmapset of attribute numbers offset by
 *					   FirstLowInvalidHeapAttributeNumber (the usual
 *					   RelationGetIndexAttrBitmap convention).  System
 *					   attributes (attnum <= 0) are ignored for the bitmap;
 *					   they cannot be updated by DML in any case.
 *
 * Returns the total number of bytes written into buf (always equal to
 * HotIndexedTombstoneSize(natts), including MAXALIGN padding).
 *
 * This routine does not palloc; it is safe to call inside a critical
 * section provided the caller has preallocated the buffer.
 */
Size
heap_build_hot_indexed_tombstone(char *buf,
								 OffsetNumber target_offnum,
								 int natts,
								 const Bitmapset *modified_attrs)
{
	HeapTupleHeader tup = (HeapTupleHeader) buf;
	HotIndexedTombstonePayload *payload;
	Size		hoff = MAXALIGN(SizeofHeapTupleHeader);
	Size		nbytes = (natts + 7) / 8;
	Size		total = HotIndexedTombstoneSize(natts);

	Assert(buf != NULL);
	Assert(natts >= 1);
	Assert(natts <= MaxHeapAttributeNumber);
	Assert(OffsetNumberIsValid(target_offnum));

	/*
	 * Zero the entire item so alignment padding and the unused tail of the
	 * bitmap byte are deterministic.  Callers rely on this for FPI stability
	 * and for amcheck.
	 */
	memset(buf, 0, total);

	/*
	 * Header: invisible to every visibility routine, flagged as a HOT-indexed
	 * item, natts = 0 so HeapTupleHeaderIsHotIndexedTombstone returns true.
	 * t_ctid points "nowhere" (InvalidBlockNumber); the target offset of the
	 * live hot-indexed tuple is carried in t_ctid.offnum (read back via
	 * HotIndexedTombstoneGetTarget).
	 */
	ItemPointerSet(&tup->t_ctid, InvalidBlockNumber, target_offnum);
	tup->t_infomask = HEAP_XMIN_INVALID | HEAP_XMAX_INVALID;
	tup->t_infomask2 = HEAP_INDEXED_UPDATED;
	HeapTupleHeaderSetNatts(tup, 0);
	tup->t_hoff = (uint8) hoff;

	/* xmin/xmax are irrelevant (frozen-invalid already set) but zero them. */
	HeapTupleHeaderSetXmin(tup, InvalidTransactionId);
	HeapTupleHeaderSetXmax(tup, InvalidTransactionId);
	HeapTupleHeaderSetCmin(tup, InvalidCommandId);

	/* Payload: bitmap width and bits.  Target offset lives in t_ctid.offnum. */
	payload = (HotIndexedTombstonePayload *) (buf + hoff);
	payload->t_nbytes = (uint16) nbytes;

	if (modified_attrs != NULL)
	{
		for (int attnum = 1; attnum <= natts; attnum++)
		{
			int			attidx = attnum - FirstLowInvalidHeapAttributeNumber;

			if (bms_is_member(attidx, modified_attrs))
			{
				int			bit = attnum - 1;

				payload->t_bitmap[bit >> 3] |= (uint8) (1u << (bit & 7));
			}
		}
	}

	return total;
}

/*
 * heap_hot_indexed_tombstone_attr_modified
 *		Return true iff user attribute `attnum` (1-based) is marked modified
 *		by the given tombstone payload.
 *
 * Callers are expected to have validated HeapTupleHeaderIsHotIndexedTombstone
 * on the enclosing tuple header and, in particular, that attnum is within
 * the relation's attribute range.  Out-of-range attnums return false.
 */
bool
heap_hot_indexed_tombstone_attr_modified(const HotIndexedTombstonePayload *p,
										 AttrNumber attnum)
{
	int			bit;

	if (attnum < 1)
		return false;

	bit = attnum - 1;
	if ((bit >> 3) >= p->t_nbytes)
		return false;

	return (p->t_bitmap[bit >> 3] & (1u << (bit & 7))) != 0;
}

/*
 * heap_hot_indexed_payload_overlaps
 *		See header comment.  index_attrs members are attribute numbers offset
 *		by FirstLowInvalidHeapAttributeNumber; only user attributes (attnum >=
 *		1) can be carried by a tombstone bitmap, so system attributes are
 *		ignored.
 */
bool
heap_hot_indexed_payload_overlaps(const HotIndexedTombstonePayload *p,
								  const Bitmapset *index_attrs)
{
	int			m = -1;

	while ((m = bms_next_member(index_attrs, m)) >= 0)
	{
		AttrNumber	attnum = m + FirstLowInvalidHeapAttributeNumber;

		if (attnum >= 1 &&
			heap_hot_indexed_tombstone_attr_modified(p, attnum))
			return true;
	}
	return false;
}

/*
 * heap_hot_indexed_redirect_overlaps
 *		True iff any heap attribute in index_attrs is marked modified by a
 *		HOT-indexed data redirect's bitmap.  Same bit layout and convention as
 *		heap_hot_indexed_payload_overlaps; used by the read path when a chain
 *		walk enters through a data redirect.
 */
bool
heap_hot_indexed_redirect_overlaps(const HotIndexedRedirectData *rd,
								   const Bitmapset *index_attrs)
{
	int			m = -1;

	while ((m = bms_next_member(index_attrs, m)) >= 0)
	{
		AttrNumber	attnum = m + FirstLowInvalidHeapAttributeNumber;
		int			bit = attnum - 1;

		if (attnum >= 1 && (bit >> 3) < rd->rd_nbytes &&
			(rd->rd_bitmap[bit >> 3] & (1u << (bit & 7))) != 0)
			return true;
	}
	return false;
}

/*
 * heap_build_hot_indexed_bridge
 *		Populate *buf with a bridge tombstone that carries no payload and
 *		just forwards a chain walker to forward_offnum on the same page.
 *
 * See access/hot_indexed.h for the design rationale.  In brief, a bridge
 * replaces a dead mid-chain HOT-indexed heap-only tuple whose LP is not
 * yet safe to reclaim (stale btree entries may still point at it).  The
 * resulting item is LP_NORMAL, natts==0, HEAP_INDEXED_UPDATED, with t_ctid
 * = (blkno, forward_offnum).  HeapTupleHeaderIsHotIndexedBridge matches
 * it.  Size is fixed at MAXALIGN(SizeofHeapTupleHeader).
 *
 * This routine does not palloc and is safe to call inside a critical
 * section provided the caller has preallocated the buffer.
 */
Size
heap_build_hot_indexed_bridge(char *buf,
							  BlockNumber blkno,
							  OffsetNumber forward_offnum)
{
	HeapTupleHeader tup = (HeapTupleHeader) buf;
	Size		hoff = MAXALIGN(SizeofHeapTupleHeader);
	Size		total = HOT_INDEXED_BRIDGE_SIZE;

	Assert(buf != NULL);
	Assert(BlockNumberIsValid(blkno));
	Assert(OffsetNumberIsValid(forward_offnum));

	/*
	 * Zero the whole item so alignment padding is deterministic.  Important
	 * for FPI stability and for amcheck.
	 */
	memset(buf, 0, total);

	/*
	 * Bridge header: invisible to every visibility routine, flagged as a
	 * HOT-indexed item, natts = 0 so HeapTupleHeaderIsHotIndexedTombstone
	 * returns true, forward link in t_ctid with a valid blockno so
	 * HeapTupleHeaderIsHotIndexedBridge returns true.
	 *
	 * We deliberately do NOT set HEAP_HOT_UPDATED: a bridge has
	 * HEAP_XMIN_INVALID|HEAP_XMAX_INVALID, for which
	 * HeapTupleHeaderIsHotUpdated always returns false, so that bit would be
	 * inert and misleading.  Chain walkers recognise and follow bridges via
	 * the explicit HeapTupleHeaderIsHotIndexedBridge predicate (see
	 * heap_hot_search_buffer), not via HEAP_HOT_UPDATED.
	 */
	ItemPointerSet(&tup->t_ctid, blkno, forward_offnum);
	tup->t_infomask = HEAP_XMIN_INVALID | HEAP_XMAX_INVALID;
	tup->t_infomask2 = HEAP_INDEXED_UPDATED;
	HeapTupleHeaderSetNatts(tup, 0);
	tup->t_hoff = (uint8) hoff;

	HeapTupleHeaderSetXmin(tup, InvalidTransactionId);
	HeapTupleHeaderSetXmax(tup, InvalidTransactionId);
	HeapTupleHeaderSetCmin(tup, InvalidCommandId);

	return total;
}
