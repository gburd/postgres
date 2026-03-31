/**
 * @file noxu_tid.h
 * @brief Conversions between ItemPointers and uint64 TID representation.
 *
 * Throughout Noxu, TIDs are carried as 64-bit unsigned integers (nxtid)
 * rather than the standard PostgreSQL ItemPointerData.  This avoids the
 * overhead of packing/unpacking block+offset pairs and simplifies
 * arithmetic comparisons during B-tree operations.
 *
 * The conversion formula is:
 * @code
 *   nxtid = blk * (MaxNXTidOffsetNumber - 1) + off
 * @endcode
 *
 * where MaxNXTidOffsetNumber = 129.  This ensures that every valid
 * ItemPointer (with off >= 1) maps to a unique nxtid >= 1, and the
 * reverse mapping always produces a valid ItemPointer.
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/include/access/noxu_tid.h
 */
#ifndef NOXU_TID_H
#define NOXU_TID_H

#include "c.h"					/* for uint64, uint32, Assert, etc. */
#include "storage/itemptr.h"

/**
 * @brief Noxu TID type: a 64-bit logical row identifier.
 *
 * Used throughout Noxu in place of ItemPointerData for efficiency.
 * The value is a linear encoding of (block, offset) that preserves
 * ordering: nearby TIDs correspond to nearby physical locations.
 */
typedef uint64 nxtid;

#define InvalidNXTid		0       /**< @brief No valid TID. */
#define MinNXTid			1       /**< @brief Smallest valid TID (blk 0, off 1). */
#define MaxNXTid			((uint64) MaxBlockNumber << 16 | 0xffff)  /**< @brief Largest valid TID. */
#define MaxPlusOneNXTid		(MaxNXTid + 1)  /**< @brief Sentinel: one past the largest valid TID. */

/** @brief Maximum offset number used in the TID encoding scheme. */
#define MaxNXTidOffsetNumber	129

/**
 * @brief Convert a (block, offset) pair to an nxtid.
 * @param blk  Block number.
 * @param off  Offset number (must be >= 1).
 * @return The corresponding nxtid.
 */
static inline nxtid
NXTidFromBlkOff(BlockNumber blk, OffsetNumber off)
{
	Assert(off != 0);

	return (uint64) blk * (MaxNXTidOffsetNumber - 1) + off;
}

/**
 * @brief Convert an ItemPointerData to an nxtid.
 * @param iptr  A valid ItemPointerData.
 * @return The corresponding nxtid.
 */
static inline nxtid
NXTidFromItemPointer(ItemPointerData iptr)
{
	Assert(ItemPointerIsValid(&iptr));
	return NXTidFromBlkOff(ItemPointerGetBlockNumber(&iptr),
						   ItemPointerGetOffsetNumber(&iptr));
}

/**
 * @brief Convert an nxtid back to an ItemPointerData.
 * @param tid  A valid nxtid (>= MinNXTid).
 * @return The corresponding ItemPointerData with a valid block and offset.
 */
static inline ItemPointerData
ItemPointerFromNXTid(nxtid tid)
{
	ItemPointerData iptr;
	BlockNumber blk;
	OffsetNumber off;

	blk = (tid - 1) / (MaxNXTidOffsetNumber - 1);
	off = (tid - 1) % (MaxNXTidOffsetNumber - 1) + 1;

	ItemPointerSet(&iptr, blk, off);
	Assert(ItemPointerIsValid(&iptr));
	return iptr;
}

/**
 * @brief Extract the logical block number from an nxtid.
 * @param tid  A valid nxtid.
 * @return The block number component.
 */
static inline BlockNumber
NXTidGetBlockNumber(nxtid tid)
{
	return (BlockNumber) ((tid - 1) / (MaxNXTidOffsetNumber - 1));
}

/**
 * @brief Extract the logical offset number from an nxtid.
 * @param tid  A valid nxtid.
 * @return The offset number component (>= 1).
 */
static inline OffsetNumber
NXTidGetOffsetNumber(nxtid tid)
{
	return (OffsetNumber) ((tid - 1) % (MaxNXTidOffsetNumber - 1) + 1);
}

#endif							/* NOXU_TID_H */
