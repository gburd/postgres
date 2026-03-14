/**
 * @file orvos_tid.h
 * @brief Conversions between ItemPointers and uint64 TID representation.
 *
 * Throughout Orvos, TIDs are carried as 64-bit unsigned integers (ovtid)
 * rather than the standard PostgreSQL ItemPointerData.  This avoids the
 * overhead of packing/unpacking block+offset pairs and simplifies
 * arithmetic comparisons during B-tree operations.
 *
 * The conversion formula is:
 * @code
 *   ovtid = blk * (MaxOVTidOffsetNumber - 1) + off
 * @endcode
 *
 * where MaxOVTidOffsetNumber = 129.  This ensures that every valid
 * ItemPointer (with off >= 1) maps to a unique ovtid >= 1, and the
 * reverse mapping always produces a valid ItemPointer.
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/include/access/orvos_tid.h
 */
#ifndef ORVOS_TID_H
#define ORVOS_TID_H

#include "c.h"					/* for uint64, uint32, Assert, etc. */
#include "storage/itemptr.h"

/**
 * @brief Orvos TID type: a 64-bit logical row identifier.
 *
 * Used throughout Orvos in place of ItemPointerData for efficiency.
 * The value is a linear encoding of (block, offset) that preserves
 * ordering: nearby TIDs correspond to nearby physical locations.
 */
typedef uint64 ovtid;

#define InvalidOVTid		0       /**< @brief No valid TID. */
#define MinOVTid			1       /**< @brief Smallest valid TID (blk 0, off 1). */
#define MaxOVTid			((uint64) MaxBlockNumber << 16 | 0xffff)  /**< @brief Largest valid TID. */
#define MaxPlusOneOVTid		(MaxOVTid + 1)  /**< @brief Sentinel: one past the largest valid TID. */

/** @brief Maximum offset number used in the TID encoding scheme. */
#define MaxOVTidOffsetNumber	129

/**
 * @brief Convert a (block, offset) pair to an ovtid.
 * @param blk  Block number.
 * @param off  Offset number (must be >= 1).
 * @return The corresponding ovtid.
 */
static inline ovtid
OVTidFromBlkOff(BlockNumber blk, OffsetNumber off)
{
	Assert(off != 0);

	return (uint64) blk * (MaxOVTidOffsetNumber - 1) + off;
}

/**
 * @brief Convert an ItemPointerData to an ovtid.
 * @param iptr  A valid ItemPointerData.
 * @return The corresponding ovtid.
 */
static inline ovtid
OVTidFromItemPointer(ItemPointerData iptr)
{
	Assert(ItemPointerIsValid(&iptr));
	return OVTidFromBlkOff(ItemPointerGetBlockNumber(&iptr),
						   ItemPointerGetOffsetNumber(&iptr));
}

/**
 * @brief Convert an ovtid back to an ItemPointerData.
 * @param tid  A valid ovtid (>= MinOVTid).
 * @return The corresponding ItemPointerData with a valid block and offset.
 */
static inline ItemPointerData
ItemPointerFromOVTid(ovtid tid)
{
	ItemPointerData iptr;
	BlockNumber blk;
	OffsetNumber off;

	blk = (tid - 1) / (MaxOVTidOffsetNumber - 1);
	off = (tid - 1) % (MaxOVTidOffsetNumber - 1) + 1;

	ItemPointerSet(&iptr, blk, off);
	Assert(ItemPointerIsValid(&iptr));
	return iptr;
}

/**
 * @brief Extract the logical block number from an ovtid.
 * @param tid  A valid ovtid.
 * @return The block number component.
 */
static inline BlockNumber
OVTidGetBlockNumber(ovtid tid)
{
	return (BlockNumber) ((tid - 1) / (MaxOVTidOffsetNumber - 1));
}

/**
 * @brief Extract the logical offset number from an ovtid.
 * @param tid  A valid ovtid.
 * @return The offset number component (>= 1).
 */
static inline OffsetNumber
OVTidGetOffsetNumber(ovtid tid)
{
	return (OffsetNumber) ((tid - 1) % (MaxOVTidOffsetNumber - 1) + 1);
}

#endif							/* ORVOS_TID_H */
