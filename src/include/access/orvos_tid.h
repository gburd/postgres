/*
 * orvos_tid.h
 *		Conversions between ItemPointers and uint64.
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/include/access/orvos_tid.h
 */
#ifndef ORVOS_TID_H
#define ORVOS_TID_H

#include "storage/itemptr.h"

/*
 * Throughout Orvos, we pass around TIDs as uint64's, rather than ItemPointers,
 * for speed.
 */
typedef uint64 ovtid;

#define InvalidOVTid		0
#define MinOVTid			1	/* blk 0, off 1 */
#define MaxOVTid			((uint64) MaxBlockNumber << 16 | 0xffff)
/* note: if this is converted to ItemPointer, it is invalid */
#define MaxPlusOneOVTid		(MaxOVTid + 1)

#define MaxOVTidOffsetNumber	129

static inline ovtid
OVTidFromBlkOff(BlockNumber blk, OffsetNumber off)
{
	Assert(off != 0);

	return (uint64) blk * (MaxOVTidOffsetNumber - 1) + off;
}

static inline ovtid
OVTidFromItemPointer(ItemPointerData iptr)
{
	Assert(ItemPointerIsValid(&iptr));
	return OVTidFromBlkOff(ItemPointerGetBlockNumber(&iptr),
						   ItemPointerGetOffsetNumber(&iptr));
}

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

static inline BlockNumber
OVTidGetBlockNumber(ovtid tid)
{
	return (BlockNumber) ((tid - 1) / (MaxOVTidOffsetNumber - 1));
}

static inline OffsetNumber
OVTidGetOffsetNumber(ovtid tid)
{
	return (OffsetNumber) ((tid - 1) % (MaxOVTidOffsetNumber - 1) + 1);
}

#endif							/* ORVOS_TID_H */
