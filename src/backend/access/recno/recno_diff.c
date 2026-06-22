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
 * The result is a sequence of splice segments plus old_total_len, sufficient
 * to reconstruct the old tuple from the new one regardless of whether the
 * update changed the row length.
 *
 * For an equal-length update we scan for contiguous differing regions and
 * emit one same-length splice (del_len == ins_len) per region, merging
 * adjacent changed bytes.  For a length-changing update we emit a single
 * splice covering the differing middle, anchored by the longest common prefix
 * and suffix shared by the two tuples -- minimal for the common case of one
 * varlen column changing length.
 *
 * Returns NULL (caller stores the full old tuple instead) if:
 *   - either tuple exceeds the uint16 offset domain
 *   - the diff exceeds RECNO_DIFF_THRESHOLD_PCT of the old tuple size
 *   - an equal-length diff has more than RECNO_MAX_DIFF_SEGMENTS regions
 */
RecnoDiffRecord *
RecnoComputeTupleDiff(const char *old_data, Size old_len,
					  const char *new_data, Size new_len)
{
	RecnoDiffSegment segments[RECNO_MAX_DIFF_SEGMENTS];
	const char *seg_old_bytes[RECNO_MAX_DIFF_SEGMENTS];
	int			nsegments = 0;
	Size		total_old_bytes = 0;
	RecnoDiffRecord *result;
	char	   *ptr;
	Size		result_size;
	int			i;

	/*
	 * Offsets and lengths are stored as uint16, so both tuples must fit the
	 * 16-bit domain.  RECNO tuples live within a single page (< BLCKSZ), so
	 * this is only a defensive guard; fall back to a full-tuple record if it
	 * is ever violated.
	 */
	if (old_len > PG_UINT16_MAX || new_len > PG_UINT16_MAX)
		return NULL;

	if (old_len == new_len)
	{
		/*
		 * Equal-length fast path: scan for contiguous differing regions and
		 * record each as a same-length splice (del_len == ins_len).
		 */
		Size		pos = 0;

		while (pos < old_len)
		{
			Size		start;
			Size		end;

			/* Skip identical bytes */
			while (pos < old_len && old_data[pos] == new_data[pos])
				pos++;

			if (pos >= old_len)
				break;

			/* Found a difference - find the end of this differing region */
			start = pos;
			while (pos < old_len && old_data[pos] != new_data[pos])
				pos++;
			end = pos;

			if (nsegments >= RECNO_MAX_DIFF_SEGMENTS)
				return NULL;

			segments[nsegments].offset = (uint16) start;
			segments[nsegments].del_len = (uint16) (end - start);
			segments[nsegments].ins_len = (uint16) (end - start);
			seg_old_bytes[nsegments] = old_data + start;
			total_old_bytes += (end - start);
			nsegments++;

			/* Bail early if the diff is already too large */
			if (total_old_bytes > (old_len * RECNO_DIFF_THRESHOLD_PCT / 100))
				return NULL;
		}
	}
	else
	{
		/*
		 * Length-changing update: emit a single splice for the differing
		 * middle, bounded by the longest shared prefix and suffix.
		 */
		Size		prefix = 0;
		Size		suffix = 0;
		Size		max_prefix;
		Size		min_len = Min(old_len, new_len);

		while (prefix < min_len && old_data[prefix] == new_data[prefix])
			prefix++;

		/*
		 * Match a common suffix without overrunning into the prefix on either
		 * tuple.
		 */
		max_prefix = min_len - prefix;
		while (suffix < max_prefix &&
			   old_data[old_len - 1 - suffix] == new_data[new_len - 1 - suffix])
			suffix++;

		segments[0].offset = (uint16) prefix;
		segments[0].del_len = (uint16) (new_len - prefix - suffix);
		segments[0].ins_len = (uint16) (old_len - prefix - suffix);
		seg_old_bytes[0] = old_data + prefix;
		total_old_bytes = old_len - prefix - suffix;
		nsegments = 1;
	}

	/* No differences found - this shouldn't happen for a real UPDATE */
	if (nsegments == 0)
		return NULL;

	/* Final threshold check against the old tuple size */
	result_size = SizeOfRecnoDiffRecord;
	for (i = 0; i < nsegments; i++)
		result_size += SizeOfRecnoDiffSegment + segments[i].ins_len;

	if (result_size > (old_len * RECNO_DIFF_THRESHOLD_PCT / 100))
		return NULL;

	/*
	 * Build the result.  Layout: RecnoDiffRecord header, then for each
	 * segment a RecnoDiffSegment header followed by its ins_len old bytes.
	 */
	result = (RecnoDiffRecord *) palloc(result_size);
	result->ndiffs = (uint16) nsegments;
	result->total_size = (uint16) result_size;
	result->old_total_len = (uint16) old_len;

	ptr = (char *) result + SizeOfRecnoDiffRecord;
	for (i = 0; i < nsegments; i++)
	{
		RecnoDiffSegment *seg = (RecnoDiffSegment *) ptr;

		seg->offset = segments[i].offset;
		seg->del_len = segments[i].del_len;
		seg->ins_len = segments[i].ins_len;
		ptr += SizeOfRecnoDiffSegment;

		memcpy(ptr, seg_old_bytes[i], segments[i].ins_len);
		ptr += segments[i].ins_len;
	}

	Assert((Size) (ptr - (char *) result) == result_size);

	return result;
}

/*
 * RecnoApplyDiffReverse - Reconstruct old tuple from new tuple + diff.
 *
 * Walks the splice segments left-to-right, copying the unchanged runs of the
 * new tuple and substituting the stored old bytes for each spliced region.
 * The reconstructed tuple is old_total_len bytes, which may differ from
 * new_len when the update changed the row length.
 *
 * out_old_data must have room for diff->old_total_len bytes.
 */
bool
RecnoApplyDiffReverse(const char *new_data, Size new_len,
					  const RecnoDiffRecord *diff,
					  char *out_old_data, Size *out_old_len)
{
	const char *ptr;
	Size		new_pos = 0;
	Size		out_pos = 0;
	Size		old_total_len;
	int			i;

	if (diff == NULL || new_data == NULL || out_old_data == NULL)
		return false;

	old_total_len = diff->old_total_len;

	ptr = (const char *) diff + SizeOfRecnoDiffRecord;
	for (i = 0; i < diff->ndiffs; i++)
	{
		const RecnoDiffSegment *seg = (const RecnoDiffSegment *) ptr;
		const char *old_bytes;
		Size		gap;

		ptr += SizeOfRecnoDiffSegment;
		old_bytes = ptr;
		ptr += seg->ins_len;

		/* Segments must be ordered and stay within the new tuple. */
		if (seg->offset < new_pos ||
			(Size) seg->offset > new_len ||
			(Size) (seg->offset + seg->del_len) > new_len)
		{
			elog(DEBUG1, "RecnoApplyDiffReverse: segment %d out of bounds "
				 "(offset=%u, del_len=%u, new_len=%zu)",
				 i, seg->offset, seg->del_len, new_len);
			return false;
		}

		/* Copy the unchanged new bytes preceding this splice. */
		gap = seg->offset - new_pos;
		if (out_pos + gap + seg->ins_len > old_total_len)
		{
			elog(DEBUG1, "RecnoApplyDiffReverse: reconstruction overflows "
				 "old_total_len=%zu at segment %d", old_total_len, i);
			return false;
		}
		memcpy(out_old_data + out_pos, new_data + new_pos, gap);
		out_pos += gap;
		new_pos += gap;

		/* Substitute the spliced-in old bytes for the deleted new bytes. */
		memcpy(out_old_data + out_pos, old_bytes, seg->ins_len);
		out_pos += seg->ins_len;
		new_pos += seg->del_len;
	}

	/* Copy any unchanged new bytes after the last splice. */
	if (out_pos + (new_len - new_pos) != old_total_len)
	{
		elog(DEBUG1, "RecnoApplyDiffReverse: reconstructed length mismatch "
			 "(got %zu, expected %zu)",
			 out_pos + (new_len - new_pos), old_total_len);
		return false;
	}
	memcpy(out_old_data + out_pos, new_data + new_pos, new_len - new_pos);
	out_pos += (new_len - new_pos);

	*out_old_len = out_pos;
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
		elog(DEBUG1, "RecnoApplyInlineDiffReverse: inline diff out of bounds "
			 "(offset=%u, length=%u, tuple_len=%zu)",
			 diff->id_offset, diff->id_length, tuple_len);
		return false;
	}

	/* Copy current tuple, then overwrite the changed region with old bytes */
	memcpy(out_data, tuple_data, tuple_len);
	memcpy(out_data + diff->id_offset, diff->id_old_bytes, diff->id_length);

	return true;
}
