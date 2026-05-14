/*-------------------------------------------------------------------------
 *
 * recno_diff.h
 *	  Byte-diff computation and application for RECNO in-row versioning
 *
 * Instead of storing full old tuples in the UNDO fork, compute and store
 * compact byte-diffs. For an UPDATE that changes 4 bytes in a 200-byte
 * tuple, store only the 4-byte diff + offset metadata (~12 bytes) instead
 * of the full 200-byte old tuple.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/recno_diff.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RECNO_DIFF_H
#define RECNO_DIFF_H

#include "postgres.h"

/*
 * Inline diff for small updates (SQL Server-style 14-byte model).
 *
 * When an UPDATE changes at most RECNO_INLINE_DIFF_MAX_BYTES of tuple data,
 * the old bytes are stored directly in the tuple header instead of writing
 * an UNDO fork record.  This avoids UNDO I/O entirely for small changes
 * like status flag updates, boolean toggles, or small counter increments.
 *
 * Layout: offset(2) + length(2) + old_bytes(10) = 14 bytes.
 */
#define RECNO_INLINE_DIFF_MAX_BYTES		10

typedef struct RecnoInlineDiff
{
	uint16		id_offset;		/* Byte offset within tuple data */
	uint16		id_length;		/* Length of changed bytes (0 = no inline
								 * diff) */
	uint8		id_old_bytes[RECNO_INLINE_DIFF_MAX_BYTES];	/* Original bytes */
} RecnoInlineDiff;

#define SizeOfRecnoInlineDiff	sizeof(RecnoInlineDiff)

/*
 * Check if an inline diff is valid (has actual diff data).
 */
#define RecnoInlineDiffIsValid(d)	((d)->id_length > 0 && \
									 (d)->id_length <= RECNO_INLINE_DIFF_MAX_BYTES)

/*
 * A single diff segment: stores the old bytes at a specific offset.
 * This is the unit of change between two tuple versions.
 */
typedef struct RecnoDiffSegment
{
	uint16		offset;			/* Byte offset within tuple data */
	uint16		length;			/* Number of bytes that differ */
	/* old_bytes follow immediately (variable length) */
} RecnoDiffSegment;

#define SizeOfRecnoDiffSegment	offsetof(RecnoDiffSegment, length) + sizeof(uint16)

/*
 * RecnoDiffRecord: a compact representation of the difference between
 * two tuple versions. Contains an array of diff segments.
 */
typedef struct RecnoDiffRecord
{
	uint16		ndiffs;			/* Number of diff segments */
	uint16		total_size;		/* Total size of this record (header +
								 * segments) */
	/* RecnoDiffSegment entries follow, each with variable-length old_bytes */
} RecnoDiffRecord;

#define SizeOfRecnoDiffRecord	offsetof(RecnoDiffRecord, total_size) + sizeof(uint16)

/*
 * Threshold: if the diff exceeds this fraction of the tuple size,
 * fall back to storing the full tuple. Value is a percentage (0-100).
 */
#define RECNO_DIFF_THRESHOLD_PCT	50

/*
 * Maximum number of diff segments we'll track. If the tuple has more
 * disjoint changed regions than this, we fall back to full tuple storage.
 */
#define RECNO_MAX_DIFF_SEGMENTS		64

/*
 * Function prototypes
 */

/*
 * RecnoComputeTupleDiff - Compute the byte-level diff between old and new tuple.
 *
 * Returns a palloc'd RecnoDiffRecord, or NULL if the diff exceeds the
 * threshold (caller should store the full old tuple instead).
 *
 * old_data/new_data: raw tuple data pointers (after tuple header)
 * old_len/new_len: lengths of the tuple data
 */
extern RecnoDiffRecord *RecnoComputeTupleDiff(const char *old_data, Size old_len,
											  const char *new_data, Size new_len);

/*
 * RecnoApplyDiffReverse - Reconstruct old tuple from new tuple + diff.
 *
 * Given the current (new) tuple data and a diff record, produces the
 * old tuple by applying the diff segments in reverse.
 *
 * new_data: current tuple data
 * new_len: length of current tuple data
 * diff: the diff record
 * out_old_data: output buffer (must be at least new_len bytes)
 * out_old_len: output length of reconstructed old tuple
 *
 * Returns true on success, false on error.
 */
extern bool RecnoApplyDiffReverse(const char *new_data, Size new_len,
								  const RecnoDiffRecord *diff,
								  char *out_old_data, Size *out_old_len);

/*
 * RecnoDiffIsCompact - Check if a diff is compact enough to store.
 *
 * Returns true if the diff record is smaller than the threshold
 * percentage of the original tuple size.
 */
extern bool RecnoDiffIsCompact(const RecnoDiffRecord *diff, Size tuple_len);

/*
 * RecnoApplyInlineDiffReverse - Reconstruct old tuple from inline diff.
 *
 * Copies the current tuple to out_data, then overwrites the changed
 * region with old bytes from the inline diff.
 */
extern bool RecnoApplyInlineDiffReverse(const char *tuple_data, Size tuple_len,
										const RecnoInlineDiff *diff,
										char *out_data);

#endif							/* RECNO_DIFF_H */
