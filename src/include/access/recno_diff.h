/*-------------------------------------------------------------------------
 *
 * recno_diff.h
 *	  RECNO byte-level diff (delta) encoding for in-place UPDATE.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * Author: Greg Burd <greg@burd.me>
 *
 * src/include/access/recno_diff.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RECNO_DIFF_H
#define RECNO_DIFF_H

#include "postgres.h"

#define RECNO_MAX_DIFF_SEGMENTS		32
#define RECNO_DIFF_THRESHOLD_PCT	50	/* diff must be < 50% of tuple */
#define RECNO_INLINE_DIFF_MAX_BYTES	64

/*
 * One changed run in a full diff record.
 */
typedef struct RecnoDiffSegment
{
	uint16		offset;			/* byte offset in tuple */
	uint16		length;			/* run length; old bytes follow */
} RecnoDiffSegment;

#define SizeOfRecnoDiffSegment	(sizeof(RecnoDiffSegment))

/*
 * Variable-length diff record: header then ndiffs (segment + old bytes).
 */
typedef struct RecnoDiffRecord
{
	uint16		ndiffs;			/* number of segments */
	uint16		total_size;		/* total record size in bytes */
	/* RecnoDiffSegment[ndiffs] + old bytes follow */
} RecnoDiffRecord;

#define SizeOfRecnoDiffRecord	(sizeof(RecnoDiffRecord))

/*
 * A single small changed run stored inline in the tuple header for fast
 * in-place UPDATE reverse.
 */
typedef struct RecnoInlineDiff
{
	uint16		id_offset;		/* offset of changed run within tuple */
	uint16		id_length;		/* length of changed run */
	char		id_old_bytes[RECNO_INLINE_DIFF_MAX_BYTES];	/* old bytes */
} RecnoInlineDiff;

extern RecnoDiffRecord *RecnoComputeTupleDiff(const char *old_data, Size old_len,
											  const char *new_data, Size new_len);
extern bool RecnoApplyDiffReverse(const char *new_data, Size new_len,
								  const RecnoDiffRecord *diff,
								  char *out_old_data, Size *out_old_len);
extern bool RecnoDiffIsCompact(const RecnoDiffRecord *diff, Size tuple_len);
extern bool RecnoApplyInlineDiffReverse(const char *tuple_data, Size tuple_len,
										const RecnoInlineDiff *diff,
										char *out_data);

#endif							/* RECNO_DIFF_H */
