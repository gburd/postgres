/*-------------------------------------------------------------------------
 *
 * blob_diff.c
 *	  Binary diff algorithm for external BLOB updates
 *
 * Implements a simplified bsdiff-inspired algorithm for generating binary
 * deltas between old and new blob versions.  Uses suffix array search to
 * find matching blocks, then generates COPY/ADD commands.
 *
 * Algorithm overview:
 *   1. Build suffix array for old data (for fast substring matching)
 *   2. Scan through new data, finding longest matches in old data
 *   3. Generate COPY commands for matches >= MIN_MATCH_LENGTH bytes
 *   4. Generate ADD commands for unmatched bytes
 *
 * The delta format is:
 *   ExternalBlobDeltaHeader   (16 bytes)
 *   ExternalBlobDeltaOp[]     (array of operations, in-memory struct size)
 *   uint8[]                   (ADD operation data, concatenated)
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/blob_diff.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "lib/stringinfo.h"
#include "utils/blob.h"
#include "utils/memutils.h"

/*
 * SuffixEntry - Entry in the suffix array for substring matching.
 *
 * We store both the offset and a pointer to the data at that offset
 * for quick comparison.
 */
typedef struct SuffixEntry
{
	uint32		offset;			/* Offset in old data */
	const uint8 *data;			/* Pointer to old_data + offset */
	Size		remaining;		/* Bytes remaining from this offset */
} SuffixEntry;

/* Context passed to qsort comparator */
static Size suffix_old_size;

/* Forward declarations */
static int	suffix_compare(const void *a, const void *b);
static int	find_longest_match(const uint8 *old_data, Size old_size,
							   SuffixEntry *suffix_array, Size num_suffixes,
							   const uint8 *search_bytes, Size search_len,
							   uint32 *match_offset_out);
static void write_delta_op(StringInfo buf, uint8 type,
						   uint32 offset, uint32 length);

/*
 * ExternalBlobComputeDelta - Generate binary diff
 *
 * Produces a delta that transforms old_data into new_data.  The delta
 * is appended to delta_out.
 */
void
ExternalBlobComputeDelta(const void *old_data, Size old_size,
						 const void *new_data, Size new_size,
						 StringInfo delta_out)
{
	const uint8 *old_bytes = (const uint8 *) old_data;
	const uint8 *new_bytes = (const uint8 *) new_data;
	SuffixEntry *suffix_array;
	Size		num_suffixes;
	ExternalBlobDeltaHeader header;
	StringInfoData ops_buf;
	StringInfoData add_buf;
	Size		new_offset = 0;
	uint32		num_ops = 0;

	initStringInfo(&ops_buf);
	initStringInfo(&add_buf);

	/*
	 * Build suffix array for old data.  For very large data we limit the
	 * number of suffix entries to avoid excessive memory use and sort time.
	 */
	num_suffixes = Min(old_size, (Size) EXTBLOB_MAX_SEARCH_DISTANCE);
	if (num_suffixes > 0)
	{
		suffix_array = (SuffixEntry *) palloc(num_suffixes * sizeof(SuffixEntry));
		for (Size i = 0; i < num_suffixes; i++)
		{
			suffix_array[i].offset = (uint32) i;
			suffix_array[i].data = old_bytes + i;
			suffix_array[i].remaining = old_size - i;
		}

		/* Sort suffix array for binary search matching */
		suffix_old_size = old_size;
		qsort(suffix_array, num_suffixes, sizeof(SuffixEntry), suffix_compare);
	}
	else
	{
		suffix_array = NULL;
	}

	/*
	 * Scan through new data finding matches in old data.
	 */
	while (new_offset < new_size)
	{
		uint32		match_offset = 0;
		int			match_length = 0;
		Size		remaining = new_size - new_offset;

		if (suffix_array != NULL)
			match_length = find_longest_match(old_bytes, old_size,
											  suffix_array, num_suffixes,
											  new_bytes + new_offset,
											  remaining,
											  &match_offset);

		if (match_length >= EXTBLOB_MIN_MATCH_LENGTH)
		{
			/* Emit COPY operation */
			write_delta_op(&ops_buf, DELTA_OP_COPY,
						   match_offset, (uint32) match_length);
			num_ops++;
			new_offset += match_length;
		}
		else
		{
			/*
			 * No good match.  Accumulate bytes for an ADD operation.
			 * Continue scanning until we find a match or hit end/limit.
			 */
			Size		add_start = new_offset;
			Size		add_length = 0;

			while (new_offset < new_size)
			{
				remaining = new_size - new_offset;

				if (suffix_array != NULL)
					match_length = find_longest_match(old_bytes, old_size,
													  suffix_array,
													  num_suffixes,
													  new_bytes + new_offset,
													  remaining,
													  &match_offset);
				else
					match_length = 0;

				if (match_length >= EXTBLOB_MIN_MATCH_LENGTH)
					break;

				add_length++;
				new_offset++;

				/* Cap individual ADD ops at 4 KB */
				if (add_length >= 4096)
					break;
			}

			write_delta_op(&ops_buf, DELTA_OP_ADD,
						   (uint32) add_buf.len, (uint32) add_length);
			appendBinaryStringInfo(&add_buf,
								   (const char *) (new_bytes + add_start),
								   add_length);
			num_ops++;
		}
	}

	/* Assemble delta: header + ops + add_data */
	memset(&header, 0, sizeof(header));
	header.old_size = (uint32) old_size;
	header.new_size = (uint32) new_size;
	header.num_ops = num_ops;

	appendBinaryStringInfo(delta_out, (const char *) &header, sizeof(header));
	appendBinaryStringInfo(delta_out, ops_buf.data, ops_buf.len);
	appendBinaryStringInfo(delta_out, add_buf.data, add_buf.len);

	if (suffix_array != NULL)
		pfree(suffix_array);
	pfree(ops_buf.data);
	pfree(add_buf.data);
}

/*
 * ExternalBlobApplyDelta - Apply binary diff to reconstruct new version
 *
 * Given old data and a serialized delta, produces the new version.
 * Returns palloc'd data and sets *new_size_out.
 */
void *
ExternalBlobApplyDelta(const void *old_data, Size old_size,
					   const void *delta_data, Size delta_size,
					   Size *new_size_out)
{
	const uint8 *old_bytes = (const uint8 *) old_data;
	const uint8 *delta_bytes = (const uint8 *) delta_data;
	const ExternalBlobDeltaHeader *header;
	const ExternalBlobDeltaOp *ops;
	const uint8 *add_data;
	uint8	   *new_data;
	Size		new_offset = 0;
	Size		ops_total_size;

	if (delta_size < sizeof(ExternalBlobDeltaHeader))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("invalid delta: too small for header")));

	header = (const ExternalBlobDeltaHeader *) delta_bytes;

	if ((Size) header->old_size != old_size)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("delta old_size mismatch: expected %zu, got %u",
						old_size, header->old_size)));

	/* Locate operations and add-data */
	ops_total_size = (Size) header->num_ops * sizeof(ExternalBlobDeltaOp);
	if (delta_size < sizeof(ExternalBlobDeltaHeader) + ops_total_size)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("invalid delta: truncated operations")));

	ops = (const ExternalBlobDeltaOp *)
		(delta_bytes + sizeof(ExternalBlobDeltaHeader));
	add_data = delta_bytes + sizeof(ExternalBlobDeltaHeader) + ops_total_size;

	new_data = (uint8 *) palloc(header->new_size);
	*new_size_out = header->new_size;

	for (uint32 i = 0; i < header->num_ops; i++)
	{
		const ExternalBlobDeltaOp *op = &ops[i];

		switch (op->type)
		{
			case DELTA_OP_COPY:
				if ((Size) op->offset + op->length > old_size)
					ereport(ERROR,
							(errcode(ERRCODE_DATA_CORRUPTED),
							 errmsg("delta COPY out of bounds")));
				if (new_offset + op->length > header->new_size)
					ereport(ERROR,
							(errcode(ERRCODE_DATA_CORRUPTED),
							 errmsg("delta COPY exceeds new size")));
				memcpy(new_data + new_offset,
					   old_bytes + op->offset, op->length);
				new_offset += op->length;
				break;

			case DELTA_OP_ADD:
				{
					Size		add_avail = delta_size
						- sizeof(ExternalBlobDeltaHeader) - ops_total_size;

					if ((Size) op->offset + op->length > add_avail)
						ereport(ERROR,
								(errcode(ERRCODE_DATA_CORRUPTED),
								 errmsg("delta ADD out of bounds")));
					if (new_offset + op->length > header->new_size)
						ereport(ERROR,
								(errcode(ERRCODE_DATA_CORRUPTED),
								 errmsg("delta ADD exceeds new size")));
					memcpy(new_data + new_offset,
						   add_data + op->offset, op->length);
					new_offset += op->length;
				}
				break;

			default:
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("unknown delta op type %u", op->type)));
		}
	}

	if (new_offset != (Size) header->new_size)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("delta reconstruction size mismatch: %zu vs %u",
						new_offset, header->new_size)));

	return new_data;
}

/* ----------------------------------------------------------------
 * Internal helpers
 * ----------------------------------------------------------------
 */

/*
 * suffix_compare - qsort comparator for suffix array entries
 *
 * Compares binary data (not strcmp, which stops at null bytes).
 */
static int
suffix_compare(const void *a, const void *b)
{
	const SuffixEntry *sa = (const SuffixEntry *) a;
	const SuffixEntry *sb = (const SuffixEntry *) b;
	Size		cmp_len = Min(sa->remaining, sb->remaining);
	int			result;

	result = memcmp(sa->data, sb->data, cmp_len);
	if (result != 0)
		return result;

	/* Shorter suffix sorts first */
	if (sa->remaining < sb->remaining)
		return -1;
	if (sa->remaining > sb->remaining)
		return 1;
	return 0;
}

/*
 * find_longest_match - Find the longest match for search_bytes in old data
 *
 * Uses linear scan over the sorted suffix array.  Returns match length
 * and sets *match_offset_out.
 */
static int
find_longest_match(const uint8 *old_data, Size old_size,
				   SuffixEntry *suffix_array, Size num_suffixes,
				   const uint8 *search_bytes, Size search_len,
				   uint32 *match_offset_out)
{
	int			best_length = 0;
	uint32		best_offset = 0;
	Size		limit;

	/*
	 * Linear scan with early termination.  Checking up to
	 * EXTBLOB_MAX_SEARCH_DISTANCE entries keeps scan cost bounded.
	 */
	limit = Min(num_suffixes, (Size) EXTBLOB_MAX_SEARCH_DISTANCE);

	for (Size i = 0; i < limit; i++)
	{
		Size		max_cmp = Min(search_len, suffix_array[i].remaining);
		int			match_len = 0;

		while ((Size) match_len < max_cmp &&
			   search_bytes[match_len] == suffix_array[i].data[match_len])
			match_len++;

		if (match_len > best_length)
		{
			best_length = match_len;
			best_offset = suffix_array[i].offset;

			/* Early exit on excellent match */
			if (best_length >= 256)
				break;
		}
	}

	*match_offset_out = best_offset;
	return best_length;
}

/*
 * write_delta_op - Serialize a delta operation into a StringInfo
 *
 * Writes the in-memory struct directly (including padding).  The
 * reader must parse using the same struct layout.
 */
static void
write_delta_op(StringInfo buf, uint8 type, uint32 offset, uint32 length)
{
	ExternalBlobDeltaOp op;

	memset(&op, 0, sizeof(op));
	op.type = type;
	op.offset = offset;
	op.length = length;

	appendBinaryStringInfo(buf, (const char *) &op, sizeof(op));
}
