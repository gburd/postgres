/*-------------------------------------------------------------------------
 *
 * recno_diff.c
 *	  Byte-diff computation and application for RECNO in-row versioning
 *
 * This module implements compact byte-level differencing between tuple
 * versions. Instead of storing full old tuples in the UNDO fork, we
 * compute and store only the bytes that changed.
 *
 * For example, an UPDATE that changes 4 bytes in a 200-byte tuple stores
 * only ~12 bytes (4 bytes data + 4 bytes offset + 4 bytes header) instead
 * of the full 200 bytes.
 *
 * The diff format is a sequence of RecnoDiffSegment entries, each
 * recording an offset, length, and the old bytes at that location.
 * To reconstruct the old tuple, we start with the new tuple and
 * overwrite the segments with their old values.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/recno/recno_diff.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/recno_diff.h"
#include "utils/memutils.h"

/*
 * RecnoComputeTupleDiff - Compute byte-level diff between old and new tuple.
 *
 * Scans both buffers and identifies contiguous regions that differ.
 * Adjacent changed bytes are merged into a single segment to minimize
 * overhead.
 *
 * Returns NULL if:
 *   - Tuples have different lengths (length-changing updates use full tuple)
 *   - The diff exceeds RECNO_DIFF_THRESHOLD_PCT of the tuple size
 *   - There are more than RECNO_MAX_DIFF_SEGMENTS disjoint changes
 */
RecnoDiffRecord *
RecnoComputeTupleDiff(const char *old_data, Size old_len,
					  const char *new_data, Size new_len)
{
	RecnoDiffSegment segments[RECNO_MAX_DIFF_SEGMENTS];
	int			nsegments = 0;
	Size		total_diff_bytes = 0;
	Size		pos = 0;
	Size		compare_len;
	RecnoDiffRecord *result;
	char	   *ptr;
	Size		result_size;
	int			i;

	/*
	 * If tuple lengths differ, we can't use byte-diff (the offset-based
	 * approach doesn't handle length changes). Fall back to full tuple.
	 */
	if (old_len != new_len)
		return NULL;

	compare_len = old_len;

	/*
	 * Scan through both tuples finding regions that differ.
	 */
	while (pos < compare_len)
	{
		Size		start;
		Size		end;

		/* Skip identical bytes */
		while (pos < compare_len && old_data[pos] == new_data[pos])
			pos++;

		if (pos >= compare_len)
			break;

		/* Found a difference - find the end of this different region */
		start = pos;
		while (pos < compare_len && old_data[pos] != new_data[pos])
			pos++;
		end = pos;

		/* Check segment count limit */
		if (nsegments >= RECNO_MAX_DIFF_SEGMENTS)
			return NULL;

		/* Record this segment */
		segments[nsegments].offset = (uint16) start;
		segments[nsegments].length = (uint16) (end - start);
		total_diff_bytes += (end - start);
		nsegments++;

		/* Quick check: if diff is already too large, bail out early */
		if (total_diff_bytes > (compare_len * RECNO_DIFF_THRESHOLD_PCT / 100))
			return NULL;
	}

	/* No differences found - this shouldn't happen for a real UPDATE */
	if (nsegments == 0)
		return NULL;

	/* Final threshold check */
	result_size = SizeOfRecnoDiffRecord;
	for (i = 0; i < nsegments; i++)
		result_size += SizeOfRecnoDiffSegment + segments[i].length;

	if (result_size > (compare_len * RECNO_DIFF_THRESHOLD_PCT / 100))
		return NULL;

	/*
	 * Build the result. Layout:
	 *   RecnoDiffRecord header
	 *   RecnoDiffSegment[0] header + old_bytes[0]
	 *   RecnoDiffSegment[1] header + old_bytes[1]
	 *   ...
	 */
	result = (RecnoDiffRecord *) palloc(result_size);
	result->ndiffs = (uint16) nsegments;
	result->total_size = (uint16) result_size;

	ptr = (char *) result + SizeOfRecnoDiffRecord;
	for (i = 0; i < nsegments; i++)
	{
		RecnoDiffSegment *seg = (RecnoDiffSegment *) ptr;

		seg->offset = segments[i].offset;
		seg->length = segments[i].length;
		ptr += SizeOfRecnoDiffSegment;

		/* Copy the old bytes */
		memcpy(ptr, old_data + segments[i].offset, segments[i].length);
		ptr += segments[i].length;
	}

	Assert((Size) (ptr - (char *) result) == result_size);

	return result;
}

/*
 * RecnoApplyDiffReverse - Reconstruct old tuple from new tuple + diff.
 *
 * Copies the new tuple data to the output buffer, then overwrites the
 * changed segments with their old values from the diff record.
 */
bool
RecnoApplyDiffReverse(const char *new_data, Size new_len,
					  const RecnoDiffRecord *diff,
					  char *out_old_data, Size *out_old_len)
{
	const char *ptr;
	int			i;

	if (diff == NULL || new_data == NULL || out_old_data == NULL)
		return false;

	/* Start with a copy of the new tuple */
	memcpy(out_old_data, new_data, new_len);
	*out_old_len = new_len;

	/* Apply each diff segment: overwrite with old bytes */
	ptr = (const char *) diff + SizeOfRecnoDiffRecord;
	for (i = 0; i < diff->ndiffs; i++)
	{
		const RecnoDiffSegment *seg = (const RecnoDiffSegment *) ptr;

		ptr += SizeOfRecnoDiffSegment;

		/* Bounds check */
		if ((Size) (seg->offset + seg->length) > new_len)
		{
			elog(WARNING, "RecnoApplyDiffReverse: segment %d out of bounds "
				 "(offset=%u, length=%u, tuple_len=%zu)",
				 i, seg->offset, seg->length, new_len);
			return false;
		}

		/* Overwrite with old bytes */
		memcpy(out_old_data + seg->offset, ptr, seg->length);
		ptr += seg->length;
	}

	return true;
}

/*
 * RecnoDiffIsCompact - Check if a diff is compact enough to justify storage.
 *
 * Returns true if the diff record size is less than the threshold
 * percentage of the original tuple size.
 */
bool
RecnoDiffIsCompact(const RecnoDiffRecord *diff, Size tuple_len)
{
	if (diff == NULL || tuple_len == 0)
		return false;

	return ((Size) diff->total_size <
			(tuple_len * RECNO_DIFF_THRESHOLD_PCT / 100));
}

/*
 * RecnoApplyInlineDiffReverse - Reconstruct old tuple data using inline diff.
 *
 * The inline diff stores the old bytes at a specific offset within the tuple.
 * To reconstruct the old version, we copy the current tuple and overwrite
 * the changed region with the saved old bytes.
 *
 * tuple_data: pointer to the current tuple data on the page
 * tuple_len: length of the current tuple data
 * diff: the inline diff from the tuple header
 * out_data: output buffer (must be at least tuple_len bytes)
 *
 * Returns true on success, false if the diff is invalid or out of bounds.
 */
bool
RecnoApplyInlineDiffReverse(const char *tuple_data, Size tuple_len,
							const RecnoInlineDiff *diff,
							char *out_data)
{
	if (diff == NULL || tuple_data == NULL || out_data == NULL)
		return false;

	if (diff->id_length == 0 || diff->id_length > RECNO_INLINE_DIFF_MAX_BYTES)
		return false;

	if ((Size) (diff->id_offset + diff->id_length) > tuple_len)
	{
		elog(WARNING, "RecnoApplyInlineDiffReverse: inline diff out of bounds "
			 "(offset=%u, length=%u, tuple_len=%zu)",
			 diff->id_offset, diff->id_length, tuple_len);
		return false;
	}

	/* Copy current tuple, then overwrite the changed region with old bytes */
	memcpy(out_data, tuple_data, tuple_len);
	memcpy(out_data + diff->id_offset, diff->id_old_bytes, diff->id_length);

	return true;
}
