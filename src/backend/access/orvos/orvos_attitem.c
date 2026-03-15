/*
 * orvos_attitem.c
 *		Routines for packing datums into "items", in the attribute trees.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/orvos/orvos_attitem.c
 */
#include "postgres.h"

#include "access/detoast.h"
#include "access/orvos_compression.h"
#include "access/orvos_dict.h"
#include "access/orvos_internal.h"
#include "access/orvos_simple8b.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "utils/datum.h"
#include "utils/uuid.h"

/*
 * We avoid creating items that are "too large". An item can legitimately use
 * up a whole page, but we try not to create items that large, because they
 * could lead to fragmentation. For example, if we routinely created items
 * that are 3/4 of page size, we could only fit one item per page, and waste
 * 1/4 of the disk space.
 *
 * MAX_ATTR_ITEM_SIZE is a soft limit on how large we make items. If there's
 * a very large datum on a row, we store it on a single item of its own
 * that can be larger, because we don't have much choice. But we don't pack
 * multiple datums into a single item so that it would exceed the limit.
 * NOTE: This soft limit is on the *uncompressed* item size. So in practice,
 * when compression is effective, the items we actually store are smaller
 * than this.
 *
 * MAX_TIDS_PER_ATTR_ITEM is the max number of TIDs that can be represented
 * by a single array item. Unlike MAX_ATTR_ITEM_SIZE, it is a hard limit.
 */
#define		MAX_ATTR_ITEM_SIZE		(MaxOrvosDatumSize / 4)
#define		MAX_TIDS_PER_ATTR_ITEM	((BLCKSZ / 2) / sizeof(ovtid))

static void fetch_att_array(char *src, int srcSize, bool hasnulls,
							int numelements, uint16 item_flags,
							OVAttrTreeScan * scan);
static void fetch_att_array_for(char *src, int srcSize, bool hasnulls,
								int numelements,
				OVAttrTreeScan * scan);
static void fetch_att_array_bitpacked(char *src, int srcSize, bool hasnulls,
									  int numelements,
				OVAttrTreeScan * scan);
static void fetch_att_array_fixed_bin(char *src, int srcSize, bool hasnulls,
									  int numelements,
				OVAttrTreeScan * scan);

/*
 * Maximum varlena data size (excluding header) for which we use native
 * PostgreSQL 1-byte short varlena format.  Capped at 125 to keep the PG 1B
 * header byte <= 0xFD, avoiding collision with the 0xFF byte used by orvos
 * toast pointers.
 */
#define NATIVE_VARLENA_MAX_DATA		125

static OVAttributeArrayItem * ovbt_attr_create_item(Form_pg_attribute att,
													Datum *datums, bool *isnulls, ovtid *tids, int nitems,
																bool has_nulls, int datasz,
																bool use_native_varlena);
static OVExplodedItem * ovbt_attr_explode_item(OVAttributeArrayItem * item);

/*
 * Compute the on-disk size of a single varlena datum, understanding native
 * format items where short varlenas use PG 1-byte headers.
 */
static inline int
ovbt_attr_datasize_ex(int attlen, char *src, uint16 item_flags)
{
	unsigned char *p = (unsigned char *) src;

	if (attlen > 0)
		return attlen;

	/*
	 * Native varlena format: short varlenas are stored with PG 1-byte
	 * headers where the low bit is always 1.
	 */
	if ((item_flags & OVBT_ATTR_FORMAT_NATIVE_VARLENA) != 0)
	{
		if (p[0] == 0xFF && p[1] == 0xFF)
			return 6;			/* orvos toast pointer */
		if ((*p & 0x01) != 0)
			return *p >> 1;		/* PG 1B: total_len = header >> 1 */
		/* orvos 2-byte header (data > NATIVE_VARLENA_MAX_DATA) */
		return ((p[0] & 0x7F) << 8 | p[1]) + 1;
	}

	/* Original orvos format */
	if ((p[0] & 0x80) == 0)
		return p[0];			/* single-byte header */
	else if (p[0] == 0xFF && p[1] == 0xFF)
		return 6;				/* orvos-toast pointer */
	else
		return ((p[0] & 0x7F) << 8 | p[1]) + 1;	/* two-byte header */
}

/*
 * Check whether an attribute is a boolean column suitable for bit-packing.
 * Boolean columns in PostgreSQL have OID 16 (BOOLOID), attlen=1, attbyval=true.
 */
static inline bool
ovbt_attr_is_boolean(Form_pg_attribute att)
{
	return (att->atttypid == BOOLOID && att->attlen == 1 && att->attbyval);
}

/*
 * Helper function to pack boolean datum values into a bitpacked format.
 * Each boolean is stored as a single bit: 1 for true, 0 for false.
 * NULL values are skipped (they are tracked via the NULL bitmap).
 * Returns the number of bytes written.
 */
static int
write_bool_bitpacked(Datum *datums, bool *isnulls, int num_elements, char *dst)
{
	bits8		bits = 0;
	int			x = 0;
	char	   *start = dst;

	for (int j = 0; j < num_elements; j++)
	{
		if (isnulls[j])
			continue;

		if (x == 8)
		{
			*dst = bits;
			dst++;
			bits = 0;
			x = 0;
		}

		if (DatumGetBool(datums[j]))
			bits |= 1 << x;
		x++;
	}
	if (x > 0)
	{
		*dst = bits;
		dst++;
	}
	return dst - start;
}

/*
 * NULL handling optimization helpers.
 *
 * These functions implement three NULL representation strategies:
 *
 * 1. NO_NULLS: When no NULLs are present, the bitmap is omitted entirely
 *    (flag OVBT_ATTR_NO_NULLS is set, OVBT_HAS_NULLS is not set).
 *
 * 2. SPARSE_NULLS: For <5% NULL density, store (position, count) pairs
 *    rather than a full bitmap. Each pair is an OVSparseNullEntry.
 *    The data begins with a uint16 count of entries, followed by the entries.
 *
 * 3. RLE_NULLS: For sequential NULL runs of 8+, use run-length encoding.
 *    Each run is an OVRleNullEntry. Data begins with uint16 count of entries.
 */

/*
 * Analyze NULL distribution and choose the best encoding.
 * Returns one of OVBT_ATTR_NO_NULLS, OVBT_ATTR_SPARSE_NULLS,
 * OVBT_ATTR_RLE_NULLS, or OVBT_HAS_NULLS (standard bitmap).
 * Also returns the encoded size in *encoded_size.
 */
static uint16
choose_null_encoding(bool *isnulls, int num_elements, bool has_nulls,
					 int *encoded_size)
{
	int			bitmap_size = OVBT_ATTR_BITMAPLEN(num_elements);

	if (!has_nulls)
	{
		*encoded_size = 0;
		return OVBT_ATTR_NO_NULLS;
	}

	/* Count total NULLs and analyze runs */
	{
		int			null_count = 0;
		int			num_sparse_entries = 0;
		int			num_rle_entries = 0;
		int			sparse_size;
		int			rle_size;
		int			i;

		/* Count NULLs and sparse entries */
		i = 0;
		while (i < num_elements)
		{
			if (isnulls[i])
			{
				while (i < num_elements && isnulls[i])
				{
					null_count++;
					i++;
				}
				num_sparse_entries++;
			}
			else
				i++;
		}

		/* Count RLE entries (alternating runs of NULL and non-NULL) */
		i = 0;
		while (i < num_elements)
		{
			bool	cur_null = isnulls[i];
			int		run_len = 0;

			while (i < num_elements && isnulls[i] == cur_null)
			{
				run_len++;
				i++;
			}
			/* If run is too long for 15 bits, split into multiple entries */
			num_rle_entries += (run_len + OVBT_RLE_COUNT_MASK - 1) / OVBT_RLE_COUNT_MASK;
		}

		/* Compute sizes for each encoding */
		sparse_size = sizeof(uint16) + num_sparse_entries * sizeof(OVSparseNullEntry);
		rle_size = sizeof(uint16) + num_rle_entries * sizeof(OVRleNullEntry);

		/* Use sparse encoding if <5% NULL density and it saves space */
		if (null_count * 20 < num_elements && sparse_size < bitmap_size)
		{
			*encoded_size = sparse_size;
			return OVBT_ATTR_SPARSE_NULLS;
		}

		/* Use RLE if there are long runs (at least one run of 8+) and it saves space */
		if (rle_size < bitmap_size)
		{
			bool	has_long_run = false;

			i = 0;
			while (i < num_elements)
			{
				bool	cur_null = isnulls[i];
				int		run_len = 0;

				while (i < num_elements && isnulls[i] == cur_null)
				{
					run_len++;
					i++;
				}
				if (cur_null && run_len >= 8)
				{
					has_long_run = true;
					break;
				}
			}

			if (has_long_run)
			{
				*encoded_size = rle_size;
				return OVBT_ATTR_RLE_NULLS;
			}
		}

		/* Fall back to standard bitmap */
		*encoded_size = bitmap_size;
		return OVBT_HAS_NULLS;
	}
}

/*
 * Write sparse NULL encoding into dst.
 * Format: uint16 num_entries, followed by OVSparseNullEntry[num_entries].
 * Returns pointer past the written data.
 */
static char *
write_sparse_nulls(bool *isnulls, int num_elements, char *dst)
{
	uint16		num_entries = 0;
	char	   *count_ptr = dst;
	OVSparseNullEntry *entries;
	int			i;

	/* Reserve space for the entry count */
	dst += sizeof(uint16);
	entries = (OVSparseNullEntry *) dst;

	i = 0;
	while (i < num_elements)
	{
		if (isnulls[i])
		{
			int run_start = i;
			int run_count = 0;

			while (i < num_elements && isnulls[i])
			{
				run_count++;
				i++;
			}
			entries[num_entries].sn_position = run_start;
			entries[num_entries].sn_count = run_count;
			num_entries++;
		}
		else
			i++;
	}

	memcpy(count_ptr, &num_entries, sizeof(uint16));
	dst += num_entries * sizeof(OVSparseNullEntry);
	return dst;
}

/*
 * Write RLE NULL encoding into dst.
 * Format: uint16 num_entries, followed by OVRleNullEntry[num_entries].
 * Returns pointer past the written data.
 */
static char *
write_rle_nulls(bool *isnulls, int num_elements, char *dst)
{
	uint16		num_entries = 0;
	char	   *count_ptr = dst;
	OVRleNullEntry *entries;
	int			i;

	/* Reserve space for the entry count */
	dst += sizeof(uint16);
	entries = (OVRleNullEntry *) dst;

	i = 0;
	while (i < num_elements)
	{
		bool	cur_null = isnulls[i];
		int		run_len = 0;

		while (i < num_elements && isnulls[i] == cur_null)
		{
			run_len++;
			i++;
		}

		/* Split long runs into multiple entries */
		while (run_len > 0)
		{
			int		this_len = Min(run_len, OVBT_RLE_COUNT_MASK);

			entries[num_entries].rle_count = this_len;
			if (cur_null)
				entries[num_entries].rle_count |= OVBT_RLE_NULL_FLAG;
			num_entries++;
			run_len -= this_len;
		}
	}

	memcpy(count_ptr, &num_entries, sizeof(uint16));
	dst += num_entries * sizeof(OVRleNullEntry);
	return dst;
}

/*
 * Expand sparse NULL encoding into a boolean isnull array.
 * Returns pointer past the consumed data.
 */
static unsigned char *
read_sparse_nulls(unsigned char *src, bool *isnulls, int num_elements)
{
	uint16		num_entries;
	OVSparseNullEntry *entries;

	memset(isnulls, 0, num_elements * sizeof(bool));

	memcpy(&num_entries, src, sizeof(uint16));
	src += sizeof(uint16);
	entries = (OVSparseNullEntry *) src;

	for (int i = 0; i < num_entries; i++)
	{
		for (int j = 0; j < entries[i].sn_count; j++)
		{
			int pos = entries[i].sn_position + j;

			if (pos < num_elements)
				isnulls[pos] = true;
		}
	}

	src += num_entries * sizeof(OVSparseNullEntry);
	return src;
}

/*
 * Expand RLE NULL encoding into a boolean isnull array.
 * Returns pointer past the consumed data.
 */
static unsigned char *
read_rle_nulls(unsigned char *src, bool *isnulls, int num_elements)
{
	uint16		num_entries;
	OVRleNullEntry *entries;
	int			pos = 0;

	memcpy(&num_entries, src, sizeof(uint16));
	src += sizeof(uint16);
	entries = (OVRleNullEntry *) src;

	for (int i = 0; i < num_entries && pos < num_elements; i++)
	{
		bool	is_null = (entries[i].rle_count & OVBT_RLE_NULL_FLAG) != 0;
		int		run_len = entries[i].rle_count & OVBT_RLE_COUNT_MASK;

		for (int j = 0; j < run_len && pos < num_elements; j++)
		{
			isnulls[pos] = is_null;
			pos++;
		}
	}

	/* Fill remainder if any */
	while (pos < num_elements)
	{
		isnulls[pos] = false;
		pos++;
	}

	src += num_entries * sizeof(OVRleNullEntry);
	return src;
}

/*
 * Convert sparse or RLE NULL encoding into a standard bitmap.
 * Used by ovbt_attr_explode_item() to normalize the representation.
 */
static bits8 *
decode_nulls_to_bitmap(unsigned char *src, int num_elements, uint16 null_flags,
					   int *bytes_consumed)
{
	bool	   *isnulls;
	bits8	   *bitmap;
	unsigned char *start = src;

	isnulls = palloc(num_elements * sizeof(bool));

	if (null_flags & OVBT_ATTR_SPARSE_NULLS)
		src = read_sparse_nulls(src, isnulls, num_elements);
	else if (null_flags & OVBT_ATTR_RLE_NULLS)
		src = read_rle_nulls(src, isnulls, num_elements);
	else
	{
		/* should not be called for standard bitmap or no-nulls */
		pfree(isnulls);
		*bytes_consumed = 0;
		return NULL;
	}

	bitmap = palloc0(OVBT_ATTR_BITMAPLEN(num_elements));
	for (int i = 0; i < num_elements; i++)
	{
		if (isnulls[i])
			ovbt_attr_item_setnull(bitmap, i);
	}

	pfree(isnulls);
	*bytes_consumed = src - start;
	return bitmap;
}

/*
 * Compute the number of bits needed to represent the value 'range'.
 * Returns 0 if range == 0, meaning all values are identical.
 */
static inline int
for_bits_needed(uint64 range)
{
	if (range == 0)
		return 0;
	return 64 - __builtin_clzll(range);
}

/*
 * Check whether FOR encoding is beneficial for the given attribute and data.
 *
 * Returns true if FOR encoding should be used, and fills in *frame_min_p,
 * *bits_per_value_p, and *for_datasz_p with the encoding parameters and
 * the size of the FOR-encoded datum data section.
 *
 * FOR is only used when it saves at least 25% of space compared to raw
 * storage, and only for pass-by-value fixed-width integer types.
 */
static bool
for_should_encode(Form_pg_attribute att, Datum *datums, bool *isnulls,
				  int num_elements, int raw_datasz,
				  uint64 *frame_min_p, int *bits_per_value_p, int *for_datasz_p)
{
	uint64		minval = PG_UINT64_MAX;
	uint64		maxval = 0;
	uint64		range;
	int			bpv;
	int			num_nonnull = 0;
	int			for_datasz;

	/* FOR only applies to pass-by-value fixed-width integer types */
	if (att->attlen <= 0 || !att->attbyval)
		return false;

	/* Need at least 2 non-null values for FOR to be worthwhile */
	for (int j = 0; j < num_elements; j++)
	{
		uint64		val;

		if (isnulls[j])
			continue;

		num_nonnull++;

		switch (att->attlen)
		{
			case sizeof(int64):
				val = (uint64) DatumGetInt64(datums[j]);
				break;
			case sizeof(int32):
				val = (uint64) (uint32) DatumGetInt32(datums[j]);
				break;
			case sizeof(int16):
				val = (uint64) (uint16) DatumGetInt16(datums[j]);
				break;
			default:
				/* 1-byte values: FOR is never useful */
				return false;
		}

		if (val < minval)
			minval = val;
		if (val > maxval)
			maxval = val;
	}

	if (num_nonnull < 2)
		return false;

	range = maxval - minval;
	bpv = for_bits_needed(range);

	/* Compute FOR-encoded data size: header + bit-packed values */
	for_datasz = sizeof(OVForHeader) + (int) OVBT_FOR_PACKED_SIZE(num_nonnull, bpv);

	/* Only use FOR if we save at least 25% compared to raw storage */
	if (for_datasz >= raw_datasz * 3 / 4)
		return false;

	*frame_min_p = minval;
	*bits_per_value_p = bpv;
	*for_datasz_p = for_datasz;
	return true;
}

/*
 * Bit-pack an array of deltas (value - frame_min) into a byte buffer.
 * Values are packed LSB-first into successive bytes.
 */
static void
for_pack_values(unsigned char *dst, uint64 *values, int nvalues, int bpv)
{
	int			bitpos = 0;

	if (bpv == 0)
		return;

	memset(dst, 0, (int) OVBT_FOR_PACKED_SIZE(nvalues, bpv));

	for (int i = 0; i < nvalues; i++)
	{
		uint64		val = values[i];
		int			byte_idx = bitpos / 8;
		int			bit_offset = bitpos % 8;
		int			bits_remaining = bpv;

		while (bits_remaining > 0)
		{
			int		bits_in_this_byte = 8 - bit_offset;

			if (bits_in_this_byte > bits_remaining)
				bits_in_this_byte = bits_remaining;

			dst[byte_idx] |= (unsigned char) ((val & ((1ULL << bits_in_this_byte) - 1)) << bit_offset);
			val >>= bits_in_this_byte;
			bits_remaining -= bits_in_this_byte;
			byte_idx++;
			bit_offset = 0;
		}

		bitpos += bpv;
	}
}

/*
 * Unpack bit-packed FOR deltas from a byte buffer.
 */
static void
for_unpack_values(const unsigned char *src, uint64 *values, int nvalues, int bpv)
{
	int			bitpos = 0;

	if (bpv == 0)
	{
		memset(values, 0, nvalues * sizeof(uint64));
		return;
	}

	for (int i = 0; i < nvalues; i++)
	{
		uint64		val = 0;
		int			byte_idx = bitpos / 8;
		int			bit_offset = bitpos % 8;
		int			bits_remaining = bpv;
		int			shift = 0;

		while (bits_remaining > 0)
		{
			int		bits_in_this_byte = 8 - bit_offset;

			if (bits_in_this_byte > bits_remaining)
				bits_in_this_byte = bits_remaining;

			val |= (uint64) ((src[byte_idx] >> bit_offset) & ((1U << bits_in_this_byte) - 1)) << shift;
			shift += bits_in_this_byte;
			bits_remaining -= bits_in_this_byte;
			byte_idx++;
			bit_offset = 0;
		}

		values[i] = val;
		bitpos += bpv;
	}
}

/*
 * Create an attribute item, or items, from an array of tids and datums.
 */
List *
ovbt_attr_create_items(Form_pg_attribute att,
					   Datum *datums, bool *isnulls, ovtid *tids, int nitems)
{
	List	   *newitems;
	int			i;
	int			max_items_with_nulls = -1;
	int			max_items_without_nulls = -1;

	if (att->attlen > 0)
	{
		max_items_without_nulls = MAX_ATTR_ITEM_SIZE / att->attlen;
		Assert(max_items_without_nulls > 0);

		max_items_with_nulls = (MAX_ATTR_ITEM_SIZE * 8) / (att->attlen * 8 + 1);

		/* clamp at maximum number of tids */
		if ((size_t) max_items_without_nulls > MAX_TIDS_PER_ATTR_ITEM)
			max_items_without_nulls = MAX_TIDS_PER_ATTR_ITEM;
		if ((size_t) max_items_with_nulls > MAX_TIDS_PER_ATTR_ITEM)
			max_items_with_nulls = MAX_TIDS_PER_ATTR_ITEM;
	}

	/*
	 * Loop until we have packed each input datum.
	 */
	newitems = NIL;
	i = 0;
	while (i < nitems)
	{
		size_t		datasz;
		OVAttributeArrayItem *item;
		int			num_elements;
		bool		use_native_varlena = false;
		bool		has_nulls = false;

		/*
		 * Compute how many input datums we can pack into the next item,
		 * without exceeding MAX_ATTR_ITEM_SIZE or MAX_TIDS_PER_ATTR_ITEM.
		 *
		 * To do that, we have to loop through the datums and compute how much
		 * space they will take when packed.
		 */
		if (att->attlen > 0)
		{
			int			j;
			int			num_nonnull_items;

			for (j = i; j < nitems && j - i < max_items_without_nulls; j++)
			{
				if (isnulls[j])
				{
					has_nulls = true;
					break;
				}
			}
			num_nonnull_items = (j - i);
			datasz = num_nonnull_items * att->attlen;

			if (has_nulls)
			{
				for (; j < nitems && num_nonnull_items < max_items_with_nulls &&
					 (size_t) (j - i) < MAX_TIDS_PER_ATTR_ITEM; j++)
				{
					if (!isnulls[j])
					{
						datasz += att->attlen;
						num_nonnull_items++;
					}
				}
			}
			num_elements = (j - i);
		}
		else
		{
			int			j;
			bool		all_short_varlena = true;

			datasz = 0;
			for (j = i; j < nitems && (size_t) (j - i) < MAX_TIDS_PER_ATTR_ITEM; j++)
			{
				size_t		this_sz;

				if (isnulls[j])
				{
					has_nulls = true;
					this_sz = 0;
				}
				else
				{
					if (att->attlen == -1)
					{
						struct varlena *vl = (struct varlena *) DatumGetPointer(datums[j]);

						if (VARATT_IS_EXTERNAL(vl))
						{
							/*
							 * Any toasted datums should've been taken care of
							 * before we get here. We might see
							 * "orvos-toasted" datums, but nothing else.
							 */
							if (VARTAG_EXTERNAL(vl) != VARTAG_ORVOS)
								elog(ERROR, "unrecognized toast tag");
							this_sz = 2 + sizeof(BlockNumber);

							/*
							 * Toast pointers use a special format (0xFFFF header),
							 * not compatible with native varlena format.
							 */
							all_short_varlena = false;
						}
						else if (VARATT_IS_COMPRESSED(vl))
						{
							/*
							 * Inline compressed datum. Decompress it so we
							 * can store the raw data in the attribute item.
							 * The attribute item itself will be compressed as
							 * a whole by orvos, so keeping individual datums
							 * compressed is redundant.
							 */
							struct varlena *detoasted = detoast_attr(vl);

							datums[j] = PointerGetDatum(detoasted);
							this_sz = VARSIZE_ANY_EXHDR(detoasted);

							if (this_sz > NATIVE_VARLENA_MAX_DATA)
								all_short_varlena = false;

							if ((this_sz + 1) > 0x7F)
								this_sz += 2;
							else
								this_sz += 1;
						}
						else
						{
							this_sz = VARSIZE_ANY_EXHDR(DatumGetPointer(datums[j]));

							if (this_sz > NATIVE_VARLENA_MAX_DATA)
								all_short_varlena = false;

							if ((this_sz + 1) > 0x7F)
								this_sz += 2;
							else
								this_sz += 1;
						}
					}
					else
					{
						Assert(att->attlen == -2);
						this_sz = strlen((char *) DatumGetPointer(datums[j]));

						if (this_sz > NATIVE_VARLENA_MAX_DATA)
							all_short_varlena = false;

						if ((this_sz + 1) > 0x7F)
							this_sz += 2;
						else
							this_sz += 1;
					}
				}

				if (j != i && datasz + this_sz > MAX_ATTR_ITEM_SIZE)
					break;

				datasz += this_sz;
			}
			num_elements = j - i;

			/*
			 * Use native PG 1-byte short varlena format if all varlena
			 * values in this batch are short enough and the attribute
			 * supports it (not plain storage).  The on-disk size is
			 * identical; only the header byte encoding differs, allowing
			 * the read path to return direct pointers without copying.
			 */
			if (att->attlen == -1 && att->attstorage != 'p' && all_short_varlena)
				use_native_varlena = true;
		}

		/* FIXME: account for TID codewords in size calculation. */

		item = ovbt_attr_create_item(att,
									 &datums[i], &isnulls[i], &tids[i], num_elements,
									 has_nulls, datasz, use_native_varlena);

		newitems = lappend(newitems, item);
		i += num_elements;
	}

	return newitems;
}

/* helper function to pack an array of bools into a NULL bitmap */
static bits8 *
write_null_bitmap(bool *isnulls, int num_elements, bits8 *dst)
{
	bits8		bits = 0;
	int			x = 0;

	for (int j = 0; j < num_elements; j++)
	{
		if (x == 8)
		{
			*dst = bits;
			dst++;
			bits = 0;
			x = 0;
		}

		if (isnulls[j])
			bits |= 1 << x;
		x++;
	}
	if (x > 0)
	{
		*dst = bits;
		dst++;
	}
	return dst;
}

/*
 * Create an array item from given datums and tids.
 *
 * The caller has already computed the size the datums will require.
 */
static OVAttributeArrayItem *
ovbt_attr_create_item(Form_pg_attribute att,
					  Datum *datums, bool *isnulls, ovtid *tids, int num_elements,
					  bool has_nulls, int datasz,
					  bool use_native_varlena)
{
	uint64		deltas[MAX_TIDS_PER_ATTR_ITEM];
	uint64		codewords[MAX_TIDS_PER_ATTR_ITEM];
	int			num_codewords;
	int			total_encoded;
	char	   *p;
	char	   *pend;
	size_t		itemsz;
	OVAttributeArrayItem *item;
	bool		use_for = false;
	uint64		for_frame_min = 0;
	int			for_bpv = 0;
	int			for_datasz = 0;
	bool		use_bitpacked = false;
	int			bitpacked_datasz = 0;
	bool		use_dict = false;
	char	   *dict_encoded = NULL;
	int			dict_encoded_size = 0;
	bool		use_fixed_bin = false;
	uint16		null_encoding;
	int			null_encoded_size;
	int			effective_datasz;

	Assert(num_elements > 0);
	Assert((size_t) num_elements <= MAX_TIDS_PER_ATTR_ITEM);

	/*
	 * Check if this is a boolean column that benefits from bit-packing.
	 * Bit-packing gives 8x compression (1 bit vs 1 byte per boolean),
	 * so it takes priority over FOR encoding for booleans.
	 */
	if (ovbt_attr_is_boolean(att))
	{
		int		num_nonnull = 0;

		for (int j = 0; j < num_elements; j++)
		{
			if (!isnulls[j])
				num_nonnull++;
		}
		bitpacked_datasz = OVBT_ATTR_BITMAPLEN(num_nonnull);

		if (bitpacked_datasz < datasz)
			use_bitpacked = true;
	}

	/* Check if FOR encoding is beneficial (skip if bitpacked) */
	if (!use_bitpacked)
		use_for = for_should_encode(att, datums, isnulls, num_elements, datasz,
									&for_frame_min, &for_bpv, &for_datasz);

	/*
	 * Check if dictionary encoding is beneficial. Dictionary encoding is
	 * most effective for low-cardinality columns (few distinct values).
	 * Skip if another encoding was already selected.
	 */
	if (!use_bitpacked && !use_for &&
		ov_dict_should_encode(att, datums, isnulls, num_elements))
	{
		dict_encoded = ov_dict_encode(att, datums, isnulls, num_elements,
									  &dict_encoded_size);
		if (dict_encoded != NULL && dict_encoded_size < datasz)
			use_dict = true;
		else if (dict_encoded != NULL)
		{
			pfree(dict_encoded);
			dict_encoded = NULL;
		}
	}

	/*
	 * Check for UUID fixed-binary storage. UUID (typid=2950, typlen=16,
	 * pass-by-ref, char-aligned) benefits from an optimized read path.
	 */
	if (!use_bitpacked && !use_for && !use_dict &&
		att->attlen == UUID_LEN && !att->attbyval &&
		att->atttypid == 2950)
	{
		use_fixed_bin = true;
	}

	/* Choose the best NULL encoding strategy */
	null_encoding = choose_null_encoding(isnulls, num_elements, has_nulls,
										 &null_encoded_size);

	/*
	 * For dictionary encoding, NULL info is embedded in the dictionary
	 * indices (OV_DICT_NULL_INDEX), so skip the separate NULL encoding.
	 */
	if (use_dict)
	{
		null_encoding = OVBT_ATTR_NO_NULLS;
		null_encoded_size = 0;
	}

	/* Determine effective data size */
	if (use_dict)
		effective_datasz = dict_encoded_size;
	else if (use_bitpacked)
		effective_datasz = bitpacked_datasz;
	else if (use_for)
		effective_datasz = for_datasz;
	else
		effective_datasz = datasz;

	/* Compute TID distances */
	for (int i = 1; i < num_elements; i++)
		deltas[i] = tids[i] - tids[i - 1];

	deltas[0] = 0;
	num_codewords = 0;
	total_encoded = 0;
	while (total_encoded < num_elements)
	{
		int			num_encoded;

		codewords[num_codewords] =
			simple8b_encode(&deltas[total_encoded], num_elements - total_encoded, &num_encoded);

		total_encoded += num_encoded;
		num_codewords++;
	}

	itemsz = offsetof(OVAttributeArrayItem, t_tid_codewords);
	itemsz += num_codewords * sizeof(uint64);
	itemsz += null_encoded_size;
	itemsz += effective_datasz;

	item = palloc(itemsz);
	item->t_size = itemsz;
	item->t_flags = 0;

	/* Set NULL encoding flags */
	if (null_encoding == OVBT_HAS_NULLS)
		item->t_flags |= OVBT_HAS_NULLS;
	else if (null_encoding == OVBT_ATTR_NO_NULLS)
		item->t_flags |= OVBT_ATTR_NO_NULLS;
	else if (null_encoding == OVBT_ATTR_SPARSE_NULLS)
		item->t_flags |= OVBT_ATTR_SPARSE_NULLS | OVBT_HAS_NULLS;
	else if (null_encoding == OVBT_ATTR_RLE_NULLS)
		item->t_flags |= OVBT_ATTR_RLE_NULLS | OVBT_HAS_NULLS;

	/* Set data encoding flags */
	if (use_bitpacked)
		item->t_flags |= OVBT_ATTR_BITPACKED;
	if (use_dict)
		item->t_flags |= OVBT_ATTR_FORMAT_DICT;
	if (use_fixed_bin)
		item->t_flags |= OVBT_ATTR_FORMAT_FIXED_BIN;
	if (use_for)
		item->t_flags |= OVBT_ATTR_FORMAT_FOR;
	if (use_native_varlena)
		item->t_flags |= OVBT_ATTR_FORMAT_NATIVE_VARLENA;
	item->t_num_elements = num_elements;
	item->t_num_codewords = num_codewords;
	item->t_firsttid = tids[0];
	item->t_endtid = tids[num_elements - 1] + 1;

	for (int j = 0; j < num_codewords; j++)
		item->t_tid_codewords[j] = codewords[j];

	p = (char *) &item->t_tid_codewords[num_codewords];
	pend = ((char *) item) + itemsz;

	/* Write NULL information using the chosen encoding */
	if (null_encoding == OVBT_HAS_NULLS)
		p = (char *) write_null_bitmap(isnulls, num_elements, (bits8 *) p);
	else if (null_encoding == OVBT_ATTR_SPARSE_NULLS)
		p = write_sparse_nulls(isnulls, num_elements, p);
	else if (null_encoding == OVBT_ATTR_RLE_NULLS)
		p = write_rle_nulls(isnulls, num_elements, p);
	/* OVBT_ATTR_NO_NULLS: nothing to write */

	if (use_dict)
	{
		/*
		 * Dictionary-encoded data: copy the pre-encoded buffer which
		 * contains [OVDictHeader][offsets][values][indices].
		 */
		memcpy(p, dict_encoded, dict_encoded_size);
		p += dict_encoded_size;
		pfree(dict_encoded);
	}
	else if (use_bitpacked)
	{
		/* Pack boolean values as bits: 8 booleans per byte */
		int written = write_bool_bitpacked(datums, isnulls, num_elements, p);
		p += written;
	}
	else if (use_for)
	{
		/*
		 * Write FOR-encoded data: header followed by bit-packed deltas.
		 */
		OVForHeader *forhdr = (OVForHeader *) p;
		uint64		for_vals[MAX_TIDS_PER_ATTR_ITEM];
		int			nvals = 0;

		forhdr->for_frame_min = for_frame_min;
		forhdr->for_bits_per_value = for_bpv;
		forhdr->for_attlen = att->attlen;
		p += sizeof(OVForHeader);

		for (int j = 0; j < num_elements; j++)
		{
			uint64		val;

			if (isnulls[j])
				continue;

			switch (att->attlen)
			{
				case sizeof(int64):
					val = (uint64) DatumGetInt64(datums[j]);
					break;
				case sizeof(int32):
					val = (uint64) (uint32) DatumGetInt32(datums[j]);
					break;
				case sizeof(int16):
					val = (uint64) (uint16) DatumGetInt16(datums[j]);
					break;
				default:
					val = (uint64) (uint8) DatumGetChar(datums[j]);
					break;
			}
			for_vals[nvals++] = val - for_frame_min;
		}

		for_pack_values((unsigned char *) p, for_vals, nvals, for_bpv);
		p += OVBT_FOR_PACKED_SIZE(nvals, for_bpv);
	}
	else if (att->attlen > 0)
	{
		if (att->attbyval)
		{
			for (int j = 0; j < num_elements; j++)
			{
				if (!isnulls[j])
				{
					store_att_byval(p, datums[j], att->attlen);
					p += att->attlen;
				}
			}
		}
		else
		{
			for (int j = 0; j < num_elements; j++)
			{
				if (!isnulls[j])
				{
					memcpy(p, DatumGetPointer(datums[j]), att->attlen);
					p += att->attlen;
				}
			}
		}
	}
	else
	{
		for (int j = 0; j < num_elements; j++)
		{
			if (!isnulls[j])
			{
				struct varlena *vl;

				if (att->attlen == -1)
					vl = (struct varlena *) DatumGetPointer(datums[j]);

				if (att->attlen == -1 && VARATT_IS_EXTERNAL(vl))
				{
					varatt_ov_toastptr *ovtoast;

					/*
					 * Any toasted datums should've been taken care of before
					 * we get here. We might see "orvos-toasted" datums, but
					 * nothing else.
					 */
					if (VARTAG_EXTERNAL(vl) != VARTAG_ORVOS)
						elog(ERROR, "unrecognized toast tag");

					ovtoast = (varatt_ov_toastptr *) DatumGetPointer(datums[j]);

					/*
					 * 0xFFFF identifies a toast pointer. Followed by the
					 * block number of the first toast page.
					 */
					*(p++) = 0xFF;
					*(p++) = 0xFF;
					memcpy(p, &ovtoast->ovt_block, sizeof(BlockNumber));
					p += sizeof(BlockNumber);
				}
				else
				{
					size_t		this_sz;
					char	   *src;

					if (att->attlen == -1)
					{
						this_sz = VARSIZE_ANY_EXHDR(DatumGetPointer(datums[j]));
						src = VARDATA_ANY(DatumGetPointer(datums[j]));
					}
					else
					{
						Assert(att->attlen == -2);
						this_sz = strlen((char *) DatumGetPointer(datums[j]));
						src = (char *) DatumGetPointer(datums[j]);
					}
					if (use_native_varlena)
					{
						/*
						 * Store in PG native 1-byte short varlena format.
						 * The read path can return a direct pointer without
						 * copying.
						 */
						Assert(this_sz <= NATIVE_VARLENA_MAX_DATA);
						SET_VARSIZE_1B(p, 1 + this_sz);
						memcpy(p + 1, src, this_sz);
						p += 1 + this_sz;
					}
					else if ((this_sz + 1) > 0x7F)
					{
						*(p++) = 0x80 | ((this_sz + 1) >> 8);
						*(p++) = (this_sz + 1) & 0xFF;
						memcpy(p, src, this_sz);
						p += this_sz;
					}
					else
					{
						*(p++) = (this_sz + 1);
						memcpy(p, src, this_sz);
						p += this_sz;
					}
				}
				Assert(p <= pend);
			}
		}
	}
	if (p != pend)
		elog(ERROR, "mismatch in item size calculation");

	return item;
}

static inline int
ovbt_attr_datasize(int attlen, char *src)
{
	unsigned char *p = (unsigned char *) src;

	if (attlen > 0)
		return attlen;
	else if ((p[0] & 0x80) == 0)
	{
		/* single-byte header */
		return p[0];
	}
	else if (p[0] == 0xFF && p[1] == 0xFF)
	{
		/* orvos-toast pointer. */
		return 6;
	}
	else
	{
		/* two-byte header */
		return ((p[0] & 0x7F) << 8 | p[1]) + 1;
	}
}

/*
 * Remove elements with given TIDs from an array item.
 *
 * Returns NULL, if all elements were removed.
 */
OVExplodedItem *
ovbt_attr_remove_from_item(Form_pg_attribute attr,
						   OVAttributeArrayItem * olditem,
						   ovtid *removetids)
{
	OVExplodedItem *origitem;
	OVExplodedItem *newitem;
	int			i;
	int			j;
	char	   *src;
	char	   *dst;

	origitem = ovbt_attr_explode_item(olditem);

	newitem = palloc(sizeof(OVExplodedItem));
	newitem->tids = palloc(origitem->t_num_elements * sizeof(ovtid));
	newitem->nullbitmap = palloc0(OVBT_ATTR_BITMAPLEN(origitem->t_num_elements));
	newitem->datumdata = palloc(origitem->datumdatasz);

	/* walk through every element */
	j = 0;
	src = origitem->datumdata;
	dst = newitem->datumdata;
	for (i = 0; i < origitem->t_num_elements; i++)
	{
		int			this_datasz;
		bool		this_isnull;

		while (origitem->tids[i] > *removetids)
			removetids++;

		this_isnull = ovbt_attr_item_isnull(origitem->nullbitmap, i);
		if (!this_isnull)
			this_datasz = ovbt_attr_datasize_ex(attr->attlen, src, origitem->t_flags);
		else
			this_datasz = 0;

		if (origitem->tids[i] == *removetids)
		{
			/* leave this one out */
			removetids++;
		}
		else
		{
			newitem->tids[j] = origitem->tids[i];
			if (this_isnull)
			{
				ovbt_attr_item_setnull(newitem->nullbitmap, j);
			}
			else
			{
				memcpy(dst, src, this_datasz);
				dst += this_datasz;
			}
			j++;
		}
		src += this_datasz;
	}

	if (j == 0)
	{
		pfree(newitem);
		return NULL;
	}

	newitem->t_size = 0;
	newitem->t_flags = origitem->t_flags & OVBT_ATTR_FORMAT_NATIVE_VARLENA;
	newitem->t_num_elements = j;
	newitem->datumdatasz = dst - newitem->datumdata;

	Assert(newitem->datumdatasz <= origitem->datumdatasz);

	return newitem;
}

/*
 *
 * Extract TID and Datum/isnull arrays the given array item.
 *
 * The arrays are stored directly into the scan->array_* fields.
 *
 * TODO: avoid extracting elements we're not interested in, by passing starttid/endtid.
 */
void
ovbt_attr_item_extract(OVAttrTreeScan * scan, OVAttributeArrayItem * item)
{
	int			nelements = item->t_num_elements;
	char	   *p;
	char	   *pend;
	ovtid		currtid;
	ovtid	   *tids;
	uint64	   *codewords;

	if (nelements > scan->array_datums_allocated_size)
	{
		int			newsize = nelements * 2;

		if (scan->array_datums)
			pfree(scan->array_datums);
		if (scan->array_isnulls)
			pfree(scan->array_isnulls);
		if (scan->array_tids)
			pfree(scan->array_tids);
		scan->array_datums = MemoryContextAlloc(scan->context, newsize * sizeof(Datum));
		scan->array_isnulls = MemoryContextAlloc(scan->context, newsize * sizeof(bool) + 7);
		scan->array_tids = MemoryContextAlloc(scan->context, newsize * sizeof(ovtid));
		scan->array_datums_allocated_size = newsize;
	}

	/* decompress if needed */
	if ((item->t_flags & OVBT_ATTR_COMPRESSED) != 0)
	{
		OVAttributeCompressedItem *citem = (OVAttributeCompressedItem *) item;

		if (scan->decompress_buf_size < citem->t_uncompressed_size)
		{
			size_t		newsize = citem->t_uncompressed_size * 2;

			if (scan->decompress_buf != NULL)
				pfree(scan->decompress_buf);
			scan->decompress_buf = MemoryContextAlloc(scan->context, newsize);
			scan->decompress_buf_size = newsize;
		}

		p = (char *) citem->t_payload;
		ov_decompress(p, scan->decompress_buf,
					  citem->t_size - offsetof(OVAttributeCompressedItem, t_payload),
					  citem->t_uncompressed_size);
		p = scan->decompress_buf;
		pend = p + citem->t_uncompressed_size;
	}
	else
	{
		p = (char *) item->t_tid_codewords;
		pend = ((char *) item) + item->t_size;
	}

	/* Decode TIDs from codewords */
	tids = scan->array_tids;
	codewords = (uint64 *) p;
	p += item->t_num_codewords * sizeof(uint64);

	simple8b_decode_words(codewords, item->t_num_codewords, tids, nelements);

	currtid = item->t_firsttid;
	for (int i = 0; i < nelements; i++)
	{
		currtid += tids[i];
		tids[i] = currtid;
	}

	/*
	 * Handle enhanced NULL encodings before the datum dispatch.
	 * Sparse/RLE NULLs are decoded here, advancing p past the encoded data,
	 * and the isnulls array is pre-filled in scan->array_isnulls.
	 */
	if ((item->t_flags & OVBT_ATTR_SPARSE_NULLS) != 0)
	{
		p = (char *) read_sparse_nulls((unsigned char *) p,
									   scan->array_isnulls, nelements);
	}
	else if ((item->t_flags & OVBT_ATTR_RLE_NULLS) != 0)
	{
		p = (char *) read_rle_nulls((unsigned char *) p,
									scan->array_isnulls, nelements);
	}
	else if ((item->t_flags & OVBT_ATTR_NO_NULLS) != 0)
	{
		memset(scan->array_isnulls, 0, nelements * sizeof(bool));
	}

	/*
	 * Determine whether a standard inline NULL bitmap remains in the data
	 * stream. Enhanced NULL encodings (sparse, RLE, no-nulls) were already
	 * consumed above, so only standard OVBT_HAS_NULLS has an inline bitmap.
	 */
	{
	bool		has_inline_bitmap;

	has_inline_bitmap = ((item->t_flags & OVBT_HAS_NULLS) != 0) &&
						((item->t_flags & (OVBT_ATTR_SPARSE_NULLS |
										   OVBT_ATTR_RLE_NULLS |
										   OVBT_ATTR_NO_NULLS)) == 0);

	/*
	 * Expand the packed array data into an array of Datums.
	 *
	 * It would perhaps be more natural to loop through the elements with
	 * datumGetSize() and fetch_att(), but this is a pretty hot loop, so it's
	 * better to avoid checking attlen/attbyval in the loop.
	 *
	 * TODO: a different on-disk representation might make this better still,
	 * for varlenas (this is pretty optimal for fixed-lengths already). For
	 * example, storing an array of sizes or an array of offsets, followed by
	 * the data itself, might incur fewer pipeline stalls in the CPU.
	 */
	if ((item->t_flags & OVBT_ATTR_FORMAT_DICT) != 0)
	{
		/*
		 * Dictionary-encoded data: the datum data section contains a
		 * dictionary header, offsets, values, and uint16 indices.
		 */
		int			data_size = pend - p;
		int			buf_needed;

		/* Conservative estimate for reconstructing varlena datums */
		buf_needed = data_size + nelements * VARHDRSZ;
		if (scan->attr_buf_size < buf_needed)
		{
			if (scan->attr_buf)
				pfree(scan->attr_buf);
			scan->attr_buf = MemoryContextAlloc(scan->context, buf_needed);
			scan->attr_buf_size = buf_needed;
		}

		ov_dict_decode(scan->attdesc, p, data_size,
					   scan->array_datums, scan->array_isnulls,
					   nelements, scan->attr_buf, buf_needed);
	}
	else if ((item->t_flags & OVBT_ATTR_FORMAT_FIXED_BIN) != 0)
	{
		/*
		 * Fixed-binary storage (e.g. UUID stored as 16 raw bytes).
		 * Reconstruct pass-by-ref Datum values from packed binary data.
		 */
		fetch_att_array_fixed_bin(p, pend - p,
								 has_inline_bitmap,
								 nelements, scan);
	}
	else if ((item->t_flags & OVBT_ATTR_FORMAT_FOR) != 0)
	{
		fetch_att_array_for(p, pend - p,
							has_inline_bitmap,
							nelements,
							scan);
	}
	else if ((item->t_flags & OVBT_ATTR_BITPACKED) != 0)
	{
		fetch_att_array_bitpacked(p, pend - p,
								 has_inline_bitmap,
								 nelements,
								 scan);
	}
	else
	{
		fetch_att_array(p, pend - p,
						has_inline_bitmap,
						nelements, item->t_flags,
						scan);
	}
	} /* end has_inline_bitmap scope */
	scan->array_num_elements = nelements;
}


/*
 * Subroutine of ovbt_attr_item_extract(). Unpack an array item into an array of
 * TIDs, and an array of Datums and nulls.
 *
 * XXX: This always copies the data to a working area in 'scan'. That can be
 * wasteful, if the data already happened to be correctly aligned. The caller
 * relies on the copying, though, unless it already made a copy of it when
 * decompressing it. So take that into account if you try to avoid this by
 * avoiding the memcpys.
 */
static void
fetch_att_array(char *src, int srcSize, bool hasnulls,
				int numelements, uint16 item_flags,
				OVAttrTreeScan * scan)
{
	Form_pg_attribute attr = scan->attdesc;
	int			attlen = attr->attlen;
	bool		attbyval = attr->attbyval;
	char		attalign = attr->attalign;
	bool	   *nulls = scan->array_isnulls;
	Datum	   *datums = scan->array_datums;
	unsigned char *p = (unsigned char *) src;

	if (hasnulls)
	{
		/* expand null bitmap */
		for (int i = 0; i < numelements; i += 8)
		{
			bits8		nullbits = *(bits8 *) (p++);

			/*
			 * NOTE: we always overallocate the nulls array, so that we don't
			 * need to check for out of bounds here!
			 */
			nulls[i] = nullbits & 1;
			nulls[i + 1] = (nullbits & (1 << 1)) >> 1;
			nulls[i + 2] = (nullbits & (1 << 2)) >> 2;
			nulls[i + 3] = (nullbits & (1 << 3)) >> 3;
			nulls[i + 4] = (nullbits & (1 << 4)) >> 4;
			nulls[i + 5] = (nullbits & (1 << 5)) >> 5;
			nulls[i + 6] = (nullbits & (1 << 6)) >> 6;
			nulls[i + 7] = (nullbits & (1 << 7)) >> 7;
		}
	}
	else
		memset(nulls, 0, numelements);

	if (attlen > 0 && !hasnulls && attbyval)
	{
		memset(nulls, 0, numelements * sizeof(bool));

		/* this looks a lot like fetch_att... */
		if (attlen == sizeof(Datum))
		{
			memcpy(datums, p, sizeof(Datum) * numelements);
			p += sizeof(Datum) * numelements;
		}
		else if (attlen == sizeof(int32))
		{
			for (int i = 0; i < numelements; i++)
			{
				uint32		x;

				memcpy(&x, p, sizeof(int32));
				p += sizeof(int32);
				datums[i] = Int32GetDatum(x);
			}
		}
		else if (attlen == sizeof(int16))
		{
			for (int i = 0; i < numelements; i++)
			{
				uint16		x;

				memcpy(&x, p, sizeof(int16));
				p += sizeof(int16);
				datums[i] = Int16GetDatum(x);
			}
		}
		else
		{
			Assert(attlen == 1);

			for (int i = 0; i < numelements; i++)
			{
				datums[i] = CharGetDatum(*p);
				p++;
			}
		}
	}
	else if (attlen > 0 && attbyval)
	{
		/*
		 * this looks a lot like fetch_att... but the source might not be
		 * aligned
		 */
		if (attlen == sizeof(int64))
		{
			for (int i = 0; i < numelements; i++)
			{
				if (nulls[i])
					datums[i] = (Datum) 0;
				else
				{
					uint64		x;

					memcpy(&x, p, sizeof(int64));
					p += sizeof(int64);
					datums[i] = Int64GetDatum(x);
				}
			}
		}
		else if (attlen == sizeof(int32))
		{
			for (int i = 0; i < numelements; i++)
			{
				if (nulls[i])
					datums[i] = (Datum) 0;
				else
				{
					uint32		x;

					memcpy(&x, p, sizeof(int32));
					p += sizeof(int32);
					datums[i] = Int32GetDatum(x);
				}
			}
		}
		else if (attlen == sizeof(int16))
		{
			for (int i = 0; i < numelements; i++)
			{
				if (nulls[i])
					datums[i] = (Datum) 0;
				else
				{
					uint16		x;

					memcpy(&x, p, sizeof(int16));
					p += sizeof(int16);
					datums[i] = Int16GetDatum(x);
				}
			}
		}
		else
		{
			Assert(attlen == 1);

			for (int i = 0; i < numelements; i++)
			{
				if (nulls[i])
					datums[i] = (Datum) 0;
				else
				{
					datums[i] = CharGetDatum(*p);
					p++;
				}
			}
		}
	}
	else if (attlen > 0 && !attbyval)
	{
		/*
		 * pass-by-ref fixed size.
		 *
		 * Because the on-disk format doesn't guarantee any alignment, we need
		 * to take care of that here. When attalign='c', no alignment padding
		 * is needed so we skip the per-element att_align_nominal calls.
		 */
		int			buf_needed;
		int			alignlen;
		char	   *bufp;

		switch (attalign)
		{
			case 'd':
				alignlen = ALIGNOF_DOUBLE;
				break;
			case 'i':
				alignlen = ALIGNOF_INT;
				break;
			case 's':
				alignlen = ALIGNOF_SHORT;
				break;
			case 'c':
				alignlen = 1;
				break;
			default:
				elog(ERROR, "invalid alignment '%c'", attalign);
		}

		buf_needed = srcSize + (alignlen - 1) * numelements;

		if (scan->attr_buf_size < buf_needed)
		{
			if (scan->attr_buf)
				pfree(scan->attr_buf);
			scan->attr_buf = MemoryContextAlloc(scan->context, buf_needed);
			scan->attr_buf_size = buf_needed;
		}

		bufp = scan->attr_buf;

		if (alignlen == 1)
		{
			/*
			 * char-aligned: no alignment padding needed, so we can skip the
			 * per-element att_align_nominal call and just memcpy sequentially.
			 */
			for (int i = 0; i < numelements; i++)
			{
				if (nulls[i])
					datums[i] = (Datum) 0;
				else
				{
					memcpy(bufp, p, attlen);
					datums[i] = PointerGetDatum(bufp);
					p += attlen;
					bufp += attlen;
				}
			}
		}
		else
		{
			for (int i = 0; i < numelements; i++)
			{
				if (nulls[i])
					datums[i] = (Datum) 0;
				else
				{
					bufp = (char *) att_align_nominal(bufp, attalign);

					Assert(bufp + attlen - scan->attr_buf <= buf_needed);

					memcpy(bufp, p, attlen);
					datums[i] = PointerGetDatum(bufp);
					p += attlen;
					bufp += attlen;
				}
			}
		}
	}
	else if (attlen == -1)
	{
		/*
		 * Decode varlenas. Because we store varlenas unaligned, we need
		 * a buffer for them, like for pass-by-ref fixed-widths above.
		 * The on-disk format uses a different header encoding than
		 * PostgreSQL's standard varlena headers, so we always need to
		 * transform the data during decoding.
		 */
		int			buf_needed;
		char	   *bufp;

		/*
		 * Calculate buffer size needed for decoded varlenas:
		 * - srcSize: input data size with orvos 1-2 byte headers
		 * - (VARHDRSZ * 2) * numelements: extra space for header expansion and safety margin
		 * - (sizeof(int32) * 2) * numelements: worst-case alignment padding before each element
		 *
		 * Conservative calculation to handle all cases:
		 * - 1-byte native varlena headers expanding to 4-byte VARHDRSZ
		 * - 2-byte orvos headers expanding to 4-byte VARHDRSZ
		 * - Up to 3 bytes alignment padding before each element
		 * - Additional safety margin for complex compression scenarios (FSST, etc.)
		 */
		buf_needed = srcSize + (VARHDRSZ * 2 + sizeof(int32) * 2) * numelements;

		if (scan->attr_buf_size < buf_needed)
		{
			if (scan->attr_buf)
				pfree(scan->attr_buf);
			scan->attr_buf = MemoryContextAlloc(scan->context, buf_needed);
			scan->attr_buf_size = buf_needed;
		}

		bufp = scan->attr_buf;

		for (int i = 0; i < numelements; i++)
		{
			if (nulls[i])
				datums[i] = (Datum) 0;
			else
			{
				if (*p == 0)
					elog(ERROR, "invalid zs varlen header");

				if ((item_flags & OVBT_ATTR_FORMAT_NATIVE_VARLENA) != 0 &&
					(*p & 0x01) != 0)
				{
					/*
					 * Native PG 1-byte short varlena format.  The data is
					 * already a valid PG 1B varlena in the source buffer,
					 * so we can return a direct pointer without copying.
					 */
					int			total_len = (unsigned char) *p >> 1;

					datums[i] = PointerGetDatum(p);
					p += total_len;
				}
				else if ((*p & 0x80) == 0)
				{
					/*
					 * XXX: it would be nice if these were identical to the
					 * short varlen format used elsewhere in PostgreSQL, so we
					 * wouldn't need to copy these.
					 */
					int			this_sz = *p - 1;

					datums[i] = PointerGetDatum(bufp);

					/*
					 * XXX: I'm not sure if it makes sense to use the short
					 * varlen format, since this is just an in-memory copy. I
					 * think it's a good way to shake out bugs, though, so do
					 * it for now.
					 */
					if (attr->attstorage != 'p')
					{
						SET_VARSIZE_1B(bufp, 1 + this_sz);
						memcpy(bufp + 1, p + 1, this_sz);
						p += 1 + this_sz;
						bufp += 1 + this_sz;
					}
					else
					{
						SET_VARSIZE(bufp, VARHDRSZ + this_sz);
						memcpy(VARDATA(bufp), p + 1, this_sz);
						p += 1 + this_sz;
						bufp += VARHDRSZ + this_sz;
					}
				}
				else if (p[0] == 0xFF && p[1] == 0xFF)
				{
					/*
					 * orvos toast pointer.
					 *
					 * Note that the orvos toast pointer is stored unaligned.
					 * That's OK. Per postgres.h, varatts with 1-byte header
					 * don't need to aligned, and that applies to toast
					 * pointers, too.
					 */
					varatt_ov_toastptr toastptr;

					datums[i] = PointerGetDatum(bufp);

					SET_VARTAG_1B_E(&toastptr, VARTAG_ORVOS);
					memcpy(&toastptr.ovt_block, p + 2, sizeof(BlockNumber));
					memcpy(bufp, &toastptr, sizeof(varatt_ov_toastptr));
					p += 2 + sizeof(BlockNumber);
					bufp += sizeof(varatt_ov_toastptr);
				}
				else
				{
					int			this_sz = (((p[0] & 0x7f) << 8) | p[1]) - 1;

					bufp = (char *) att_align_nominal(bufp, 'i');
					datums[i] = PointerGetDatum(bufp);

					Assert(bufp + VARHDRSZ + this_sz - scan->attr_buf <= buf_needed);

					SET_VARSIZE(bufp, VARHDRSZ + this_sz);
					memcpy(VARDATA(bufp), p + 2, this_sz);

					p += 2 + this_sz;
					bufp += VARHDRSZ + this_sz;
				}
			}
		}
	}
	else
		elog(ERROR, "not implemented");

	if (p - (unsigned char *) src != srcSize)
		elog(ERROR, "corrupt item array: consumed %d of %d bytes, numelements=%d, attlen=%d, attbyval=%d, hasnulls=%d, attno=%d",
			 (int)(p - (unsigned char *) src), srcSize, numelements,
			 attlen, attbyval, hasnulls, attr->attnum);
}

/*
 * Decode bit-packed boolean datum data for ovbt_attr_item_extract().
 *
 * Boolean values are packed 8 per byte. Only non-NULL values are stored
 * in the bitpacked data. This gives 8x compression over the standard
 * 1-byte-per-boolean storage.
 */
static void
fetch_att_array_bitpacked(char *src, int srcSize, bool hasnulls,
						  int numelements, OVAttrTreeScan *scan)
{
	bool	   *nulls = scan->array_isnulls;
	Datum	   *datums = scan->array_datums;
	unsigned char *p = (unsigned char *) src;

	/* Decode inline NULL bitmap if present */
	if (hasnulls)
	{
		for (int i = 0; i < numelements; i += 8)
		{
			bits8		nullbits = *(bits8 *) (p++);

			nulls[i] = nullbits & 1;
			nulls[i + 1] = (nullbits & (1 << 1)) >> 1;
			nulls[i + 2] = (nullbits & (1 << 2)) >> 2;
			nulls[i + 3] = (nullbits & (1 << 3)) >> 3;
			nulls[i + 4] = (nullbits & (1 << 4)) >> 4;
			nulls[i + 5] = (nullbits & (1 << 5)) >> 5;
			nulls[i + 6] = (nullbits & (1 << 6)) >> 6;
			nulls[i + 7] = (nullbits & (1 << 7)) >> 7;
		}
	}
	else
		memset(nulls, 0, numelements);

	/*
	 * Unpack boolean values from the bitpacked format.
	 * Non-NULL booleans are packed sequentially, 8 per byte.
	 */
	{
		int			bit_idx = 0;
		bits8		cur_byte = 0;

		for (int i = 0; i < numelements; i++)
		{
			if (nulls[i])
			{
				datums[i] = (Datum) 0;
				continue;
			}

			if (bit_idx % 8 == 0)
				cur_byte = *p++;

			datums[i] = BoolGetDatum((cur_byte >> (bit_idx % 8)) & 1);
			bit_idx++;
		}
	}

	if (p - (unsigned char *) src != srcSize)
		elog(ERROR, "corrupt bitpacked item: consumed %d of %d bytes",
			 (int)(p - (unsigned char *) src), srcSize);
}

/*
 * Decode FOR-encoded datum data for ovbt_attr_item_extract().
 */
static void
fetch_att_array_for(char *src, int srcSize, bool hasnulls,
					int numelements, OVAttrTreeScan *scan)
{
	Form_pg_attribute attr = scan->attdesc;
	int			attlen = attr->attlen;
	bool	   *nulls = scan->array_isnulls;
	Datum	   *datums = scan->array_datums;
	unsigned char *p = (unsigned char *) src;
	OVForHeader forhdr;
	uint64		unpacked[MAX_TIDS_PER_ATTR_ITEM];
	int			num_nonnull;
	int			val_idx;

	if (hasnulls)
	{
		for (int i = 0; i < numelements; i += 8)
		{
			bits8		nullbits = *(bits8 *) (p++);
			nulls[i] = nullbits & 1;
			nulls[i + 1] = (nullbits & (1 << 1)) >> 1;
			nulls[i + 2] = (nullbits & (1 << 2)) >> 2;
			nulls[i + 3] = (nullbits & (1 << 3)) >> 3;
			nulls[i + 4] = (nullbits & (1 << 4)) >> 4;
			nulls[i + 5] = (nullbits & (1 << 5)) >> 5;
			nulls[i + 6] = (nullbits & (1 << 6)) >> 6;
			nulls[i + 7] = (nullbits & (1 << 7)) >> 7;
		}
	}
	else
		memset(nulls, 0, numelements);

	num_nonnull = 0;
	for (int i = 0; i < numelements; i++)
		if (!nulls[i])
			num_nonnull++;

	memcpy(&forhdr, p, sizeof(OVForHeader));
	p += sizeof(OVForHeader);

	for_unpack_values(p, unpacked, num_nonnull, forhdr.for_bits_per_value);
	p += OVBT_FOR_PACKED_SIZE(num_nonnull, forhdr.for_bits_per_value);

	val_idx = 0;
	for (int i = 0; i < numelements; i++)
	{
		if (nulls[i])
			datums[i] = (Datum) 0;
		else
		{
			uint64		val = unpacked[val_idx++] + forhdr.for_frame_min;
			switch (attlen)
			{
				case sizeof(int64):
					datums[i] = Int64GetDatum((int64) val);
					break;
				case sizeof(int32):
					datums[i] = Int32GetDatum((int32) (uint32) val);
					break;
				case sizeof(int16):
					datums[i] = Int16GetDatum((int16) (uint16) val);
					break;
				default:
					datums[i] = CharGetDatum((char) (uint8) val);
					break;
			}
		}
	}
	Assert(val_idx == num_nonnull);
	if ((int)(p - (unsigned char *) src) != srcSize)
		elog(ERROR, "corrupt FOR item: consumed %d of %d bytes",
			 (int)(p - (unsigned char *) src), srcSize);
}

/*
 * Decode fixed-binary encoded datum data for ovbt_attr_item_extract().
 *
 * Used for types like UUID where we store raw fixed-size binary data
 * without varlena headers. The data is stored as tightly packed binary
 * values (e.g., 16 bytes per UUID) with NULLs skipped.
 */
static void
fetch_att_array_fixed_bin(char *src, int srcSize, bool hasnulls,
						  int numelements, OVAttrTreeScan *scan)
{
	Form_pg_attribute attr = scan->attdesc;
	int			attlen = attr->attlen;
	bool	   *nulls = scan->array_isnulls;
	Datum	   *datums = scan->array_datums;
	unsigned char *p = (unsigned char *) src;
	int			buf_needed;
	char	   *bufp;

	Assert(attlen > 0);
	Assert(!attr->attbyval);

	/* Handle NULL bitmap if present */
	if (hasnulls)
	{
		for (int i = 0; i < numelements; i += 8)
		{
			bits8		nullbits = *(bits8 *) (p++);

			nulls[i] = nullbits & 1;
			nulls[i + 1] = (nullbits & (1 << 1)) >> 1;
			nulls[i + 2] = (nullbits & (1 << 2)) >> 2;
			nulls[i + 3] = (nullbits & (1 << 3)) >> 3;
			nulls[i + 4] = (nullbits & (1 << 4)) >> 4;
			nulls[i + 5] = (nullbits & (1 << 5)) >> 5;
			nulls[i + 6] = (nullbits & (1 << 6)) >> 6;
			nulls[i + 7] = (nullbits & (1 << 7)) >> 7;
		}
	}
	else
		memset(nulls, 0, numelements * sizeof(bool));

	/*
	 * Allocate buffer for pass-by-ref values. Fixed-binary values are
	 * stored tightly packed without alignment, so we need a working buffer.
	 */
	buf_needed = srcSize + numelements;
	if (scan->attr_buf_size < buf_needed)
	{
		if (scan->attr_buf)
			pfree(scan->attr_buf);
		scan->attr_buf = MemoryContextAlloc(scan->context, buf_needed);
		scan->attr_buf_size = buf_needed;
	}
	bufp = scan->attr_buf;

	for (int i = 0; i < numelements; i++)
	{
		if (nulls[i])
		{
			datums[i] = (Datum) 0;
		}
		else
		{
			memcpy(bufp, p, attlen);
			datums[i] = PointerGetDatum(bufp);
			p += attlen;
			bufp += attlen;
		}
	}

	if ((int) (p - (unsigned char *) src) != srcSize)
		elog(ERROR, "corrupt fixed-binary item: consumed %d of %d bytes",
			 (int) (p - (unsigned char *) src), srcSize);
}

/*
 * Routines to split, merge, and recompress items.
 */

static OVExplodedItem *
ovbt_attr_explode_item(OVAttributeArrayItem * item)
{
	OVExplodedItem *eitem;
	int			tidno;
	ovtid		currtid;
	ovtid	   *tids;
	char	   *databuf;
	char	   *p;
	char	   *pend;
	uint64	   *codewords;

	eitem = palloc(sizeof(OVExplodedItem));
	eitem->t_size = 0;
	/* Preserve the native varlena flag so datum data can be navigated */
	eitem->t_flags = item->t_flags & OVBT_ATTR_FORMAT_NATIVE_VARLENA;
	eitem->t_num_elements = item->t_num_elements;

	if ((item->t_flags & OVBT_ATTR_COMPRESSED) != 0)
	{
		OVAttributeCompressedItem *citem = (OVAttributeCompressedItem *) item;
		int			payloadsz;

		payloadsz = citem->t_uncompressed_size;
		Assert(payloadsz > 0);

		databuf = palloc(payloadsz);

		ov_decompress(citem->t_payload, databuf,
					  citem->t_size - offsetof(OVAttributeCompressedItem, t_payload),
					  payloadsz);

		p = databuf;
		pend = databuf + payloadsz;
	}
	else
	{
		p = (char *) item->t_tid_codewords;
		pend = ((char *) item) + item->t_size;
	}

	/* Decode TIDs from codewords */
	tids = eitem->tids = palloc(item->t_num_elements * sizeof(ovtid));
	tidno = 0;
	currtid = item->t_firsttid;
	codewords = (uint64 *) p;
	for (int i = 0; i < item->t_num_codewords; i++)
	{
		int			ntids;

		ntids = simple8b_decode(codewords[i], &tids[tidno]);

		for (int j = 0; j < ntids; j++)
		{
			currtid += tids[tidno];
			tids[tidno] = currtid;
			tidno++;
		}
	}
	p += item->t_num_codewords * sizeof(uint64);

	/* nulls -- handle all NULL encoding formats */
	if ((item->t_flags & OVBT_ATTR_SPARSE_NULLS) != 0)
	{
		int		bytes_consumed;
		eitem->nullbitmap = decode_nulls_to_bitmap((unsigned char *) p,
												   item->t_num_elements,
												   OVBT_ATTR_SPARSE_NULLS,
												   &bytes_consumed);
		p += bytes_consumed;
	}
	else if ((item->t_flags & OVBT_ATTR_RLE_NULLS) != 0)
	{
		int		bytes_consumed;
		eitem->nullbitmap = decode_nulls_to_bitmap((unsigned char *) p,
												   item->t_num_elements,
												   OVBT_ATTR_RLE_NULLS,
												   &bytes_consumed);
		p += bytes_consumed;
	}
	else if ((item->t_flags & OVBT_ATTR_NO_NULLS) != 0)
	{
		eitem->nullbitmap = palloc0(OVBT_ATTR_BITMAPLEN(item->t_num_elements));
	}
	else if ((item->t_flags & OVBT_HAS_NULLS) != 0)
	{
		eitem->nullbitmap = (bits8 *) p;
		p += OVBT_ATTR_BITMAPLEN(item->t_num_elements);
	}
	else
	{
		eitem->nullbitmap = palloc0(OVBT_ATTR_BITMAPLEN(item->t_num_elements));
	}

	/* Bitpacked booleans: expand to 1-byte-per-value raw format */
	if ((item->t_flags & OVBT_ATTR_BITPACKED) != 0)
	{
		int		nonnull_count = 0;
		int		bit_idx = 0;
		bits8	cur_byte = 0;
		char   *rawbuf;
		char   *wp;

		for (int i = 0; i < item->t_num_elements; i++)
			if (!ovbt_attr_item_isnull(eitem->nullbitmap, i))
				nonnull_count++;

		rawbuf = palloc(nonnull_count);
		wp = rawbuf;
		for (int i = 0; i < item->t_num_elements; i++)
		{
			if (ovbt_attr_item_isnull(eitem->nullbitmap, i))
				continue;
			if (bit_idx % 8 == 0)
				cur_byte = *(unsigned char *) p++;
			*wp++ = (cur_byte >> (bit_idx % 8)) & 1;
			bit_idx++;
		}

		eitem->datumdata = rawbuf;
		eitem->datumdatasz = nonnull_count;
		return eitem;
	}

	/* datum data -- decode FOR back to raw format if needed */
	if ((item->t_flags & OVBT_ATTR_FORMAT_FOR) != 0)
	{
		OVForHeader forhdr;
		uint64		unpacked_vals[MAX_TIDS_PER_ATTR_ITEM];
		int			nonnull_count = 0;
		int			for_attlen;
		char	   *rawbuf;
		char	   *wp;

		for (int i = 0; i < item->t_num_elements; i++)
			if (!ovbt_attr_item_isnull(eitem->nullbitmap, i))
				nonnull_count++;

		memcpy(&forhdr, p, sizeof(OVForHeader));
		p += sizeof(OVForHeader);
		for_attlen = forhdr.for_attlen;

		for_unpack_values((unsigned char *) p, unpacked_vals, nonnull_count,
						  forhdr.for_bits_per_value);

		rawbuf = palloc(nonnull_count * for_attlen);
		wp = rawbuf;
		for (int i = 0; i < nonnull_count; i++)
		{
			uint64 val = unpacked_vals[i] + forhdr.for_frame_min;
			switch (for_attlen)
			{
				case 8: memcpy(wp, &val, 8); break;
				case 4: { uint32 v = (uint32) val; memcpy(wp, &v, 4); } break;
				case 2: { uint16 v = (uint16) val; memcpy(wp, &v, 2); } break;
				default: { uint8 v = (uint8) val; memcpy(wp, &v, 1); } break;
			}
			wp += for_attlen;
		}
		eitem->datumdata = rawbuf;
		eitem->datumdatasz = nonnull_count * for_attlen;
	}
	else
	{
		eitem->datumdata = p;
		eitem->datumdatasz = pend - p;
	}

	return eitem;
}

/*
 * Estimate how much space an array item takes, when it's uncompressed.
 */
static int
ovbt_item_uncompressed_size(OVAttributeArrayItem * item)
{
	if (item->t_size == 0)
	{
		OVExplodedItem *eitem = (OVExplodedItem *) item;
		size_t		sz = 0;

		/* FIXME: account for tids and null bitmap accurately. */

		sz += eitem->t_num_elements * 2;
		//Conservatively estimate 2 bytes per TID.
			sz += eitem->datumdatasz;

		return sz;
	}
	else if (item->t_flags & OVBT_ATTR_COMPRESSED)
	{
		OVAttributeCompressedItem *citem = (OVAttributeCompressedItem *) item;

		return offsetof(OVAttributeCompressedItem, t_payload) + citem->t_uncompressed_size;
	}
	else
		return item->t_size;
}

void
ovbt_split_item(Form_pg_attribute attr, OVExplodedItem * origitem, ovtid first_right_tid,
				OVExplodedItem * *leftitem_p, OVExplodedItem * *rightitem_p)
{
	int			i;
	int			left_num_elements;
	int			left_datasz;
	int			right_num_elements;
	int			right_datasz;
	char	   *p;
	OVExplodedItem *leftitem;
	OVExplodedItem *rightitem;

	if (origitem->t_size != 0)
		origitem = ovbt_attr_explode_item((OVAttributeArrayItem *) origitem);

	p = origitem->datumdata;
	for (i = 0; i < origitem->t_num_elements; i++)
	{
		if (origitem->tids[i] >= first_right_tid)
			break;

		if (!ovbt_attr_item_isnull(origitem->nullbitmap, i))
			p += ovbt_attr_datasize_ex(attr->attlen, p, origitem->t_flags);
	}
	left_num_elements = i;
	left_datasz = p - origitem->datumdata;

	right_num_elements = origitem->t_num_elements - left_num_elements;
	right_datasz = origitem->datumdatasz - left_datasz;

	if (left_num_elements == origitem->t_num_elements)
		elog(ERROR, "item split failed");

	leftitem = palloc(sizeof(OVExplodedItem));
	leftitem->t_size = 0;
	leftitem->t_flags = origitem->t_flags & OVBT_ATTR_FORMAT_NATIVE_VARLENA;
	leftitem->t_num_elements = left_num_elements;
	leftitem->tids = palloc(left_num_elements * sizeof(ovtid));
	leftitem->nullbitmap = palloc0(OVBT_ATTR_BITMAPLEN(left_num_elements));
	leftitem->datumdata = palloc(left_datasz);
	leftitem->datumdatasz = left_datasz;

	memcpy(leftitem->tids, &origitem->tids[0], left_num_elements * sizeof(ovtid));
	/* XXX: should copy the null bitmap in a smarter way */
	for (i = 0; i < left_num_elements; i++)
	{
		if (ovbt_attr_item_isnull(origitem->nullbitmap, i))
			ovbt_attr_item_setnull(leftitem->nullbitmap, i);
	}
	memcpy(leftitem->datumdata, &origitem->datumdata[0], left_datasz);

	rightitem = palloc(sizeof(OVExplodedItem));
	rightitem->t_size = 0;
	rightitem->t_flags = origitem->t_flags & OVBT_ATTR_FORMAT_NATIVE_VARLENA;
	rightitem->t_num_elements = right_num_elements;
	rightitem->tids = palloc(right_num_elements * sizeof(ovtid));
	rightitem->nullbitmap = palloc0(OVBT_ATTR_BITMAPLEN(right_num_elements));
	rightitem->datumdata = palloc(right_datasz);
	rightitem->datumdatasz = right_datasz;

	memcpy(rightitem->tids, &origitem->tids[left_num_elements], right_num_elements * sizeof(ovtid));
	/* XXX: should copy the null bitmap in a smarter way */
	for (i = 0; i < right_num_elements; i++)
	{
		if (ovbt_attr_item_isnull(origitem->nullbitmap, left_num_elements + i))
			ovbt_attr_item_setnull(rightitem->nullbitmap, i);
	}
	memcpy(rightitem->datumdata, &origitem->datumdata[left_datasz], right_datasz);

	*leftitem_p = leftitem;
	*rightitem_p = rightitem;
}

static OVExplodedItem *
ovbt_combine_items(List *items, int start, int end)
{
	OVExplodedItem *newitem;
	int			total_elements;
	int			total_datumdatasz;
	List	   *exploded_items = NIL;

	total_elements = 0;
	total_datumdatasz = 0;
	{
		bool		all_native = true;

		for (int i = start; i < end; i++)
		{
			ListCell   *lc = list_nth_cell(items, i);
			OVAttributeArrayItem *item = lfirst(lc);
			OVExplodedItem *eitem;

			if (item->t_size != 0)
			{
				eitem = ovbt_attr_explode_item(item);
				lfirst(lc) = eitem;
			}
			else
				eitem = (OVExplodedItem *) item;

			if ((eitem->t_flags & OVBT_ATTR_FORMAT_NATIVE_VARLENA) == 0)
				all_native = false;

			exploded_items = lappend(exploded_items, eitem);

			total_elements += eitem->t_num_elements;
			total_datumdatasz += eitem->datumdatasz;
		}
		Assert((size_t) total_elements <= MAX_TIDS_PER_ATTR_ITEM);

		newitem = palloc(sizeof(OVExplodedItem));
		newitem->t_size = 0;
		/* Preserve native varlena flag only if all combined items have it */
		newitem->t_flags = all_native ? OVBT_ATTR_FORMAT_NATIVE_VARLENA : 0;
	}
	newitem->t_num_elements = total_elements;

	newitem->tids = palloc(total_elements * sizeof(ovtid));
	newitem->nullbitmap = palloc0(OVBT_ATTR_BITMAPLEN(total_elements));
	newitem->datumdata = palloc(total_datumdatasz);
	newitem->datumdatasz = total_datumdatasz;

	{
		char	   *p = newitem->datumdata;
		int			elemno = 0;

		for (int i = start; i < end; i++)
		{
			OVExplodedItem *eitem = list_nth(items, i);

			memcpy(&newitem->tids[elemno], eitem->tids, eitem->t_num_elements * sizeof(ovtid));

			/* XXX: should copy the null bitmap in a smarter way */
			for (int j = 0; j < eitem->t_num_elements; j++)
			{
				if (ovbt_attr_item_isnull(eitem->nullbitmap, j))
					ovbt_attr_item_setnull(newitem->nullbitmap, elemno + j);
			}

			memcpy(p, eitem->datumdata, eitem->datumdatasz);
			p += eitem->datumdatasz;
			elemno += eitem->t_num_elements;
		}
	}

	return newitem;
}

static OVAttributeArrayItem *
ovbt_pack_item(Form_pg_attribute att, OVExplodedItem * eitem)
{
	OVAttributeArrayItem *newitem;
	int			num_elements = eitem->t_num_elements;
	ovtid		firsttid;
	ovtid		prevtid;
	uint64		deltas[MAX_TIDS_PER_ATTR_ITEM];
	uint64		codewords[MAX_TIDS_PER_ATTR_ITEM];
	int			num_codewords;
	int			total_encoded;
	size_t		itemsz;
	char	   *p;
	bool		has_nulls;
	int			nullbitmapsz;

	(void) att;

	Assert(num_elements > 0);
	Assert((size_t) num_elements <= MAX_TIDS_PER_ATTR_ITEM);

	/* compute deltas */
	firsttid = eitem->tids[0];
	prevtid = firsttid;
	deltas[0] = 0;
	for (int i = 1; i < num_elements; i++)
	{
		ovtid		this_tid = eitem->tids[i];

		deltas[i] = this_tid - prevtid;
		prevtid = this_tid;
	}

	/* pack into codewords */
	num_codewords = 0;
	total_encoded = 0;
	while (total_encoded < num_elements)
	{
		int			num_encoded;

		codewords[num_codewords] =
			simple8b_encode(&deltas[total_encoded], num_elements - total_encoded, &num_encoded);

		total_encoded += num_encoded;
		num_codewords++;
	}

	nullbitmapsz = OVBT_ATTR_BITMAPLEN(num_elements);
	has_nulls = false;
	for (int i = 0; i < nullbitmapsz; i++)
	{
		if (eitem->nullbitmap[i] != 0)
		{
			has_nulls = true;
			break;
		}
	}

	itemsz = offsetof(OVAttributeArrayItem, t_tid_codewords);
	itemsz += num_codewords * sizeof(uint64);
	if (has_nulls)
	{
		/* reserve space for NULL bitmap */
		itemsz += nullbitmapsz;
	}
	itemsz += eitem->datumdatasz;

	Assert(has_nulls || eitem->datumdatasz > 0);

	newitem = palloc(itemsz);
	newitem->t_size = itemsz;
	newitem->t_flags = eitem->t_flags & OVBT_ATTR_FORMAT_NATIVE_VARLENA;
	if (has_nulls)
		newitem->t_flags |= OVBT_HAS_NULLS;
	newitem->t_num_elements = num_elements;
	newitem->t_num_codewords = num_codewords;
	newitem->t_firsttid = eitem->tids[0];
	newitem->t_endtid = eitem->tids[num_elements - 1] + 1;

	memcpy(newitem->t_tid_codewords, codewords, num_codewords * sizeof(uint64));

	p = (char *) &newitem->t_tid_codewords[num_codewords];

	if (has_nulls)
	{
		memcpy(p, eitem->nullbitmap, nullbitmapsz);
		p += nullbitmapsz;
	}

	memcpy(p, eitem->datumdata, eitem->datumdatasz);
	p += eitem->datumdatasz;

	Assert((size_t) (p - ((char *) newitem)) == itemsz);

	return newitem;
}

static OVAttributeArrayItem *
ovbt_compress_item(OVAttributeArrayItem * item)
{
	OVAttributeCompressedItem *citem;
	char	   *uncompressed_payload;
	int			uncompressed_size;
	int			compressed_size;
	int			item_allocsize;

	Assert(item->t_size > 0);

	uncompressed_payload = (char *) &item->t_tid_codewords;
	uncompressed_size = ((char *) item) + item->t_size - uncompressed_payload;

	item_allocsize = item->t_size;

	/*
	 * XXX: because pglz requires a slightly larger buffer to even try
	 * compressing, make a slightly larger allocation. If the compression
	 * succeeds but with a poor ratio, so that we actually use the extra
	 * space, then we will store it uncompressed, but pglz refuses to even try
	 * if the destination buffer is not large enough.
	 */
	item_allocsize += 10;

	citem = palloc(item_allocsize);
	citem->t_flags = OVBT_ATTR_COMPRESSED;
	/* Preserve all encoding flags through compression */
	citem->t_flags |= (item->t_flags & (OVBT_HAS_NULLS |
										 OVBT_ATTR_FORMAT_FOR |
										 OVBT_ATTR_BITPACKED |
										 OVBT_ATTR_NO_NULLS |
										 OVBT_ATTR_SPARSE_NULLS |
										 OVBT_ATTR_RLE_NULLS |
										 OVBT_ATTR_FORMAT_NATIVE_VARLENA |
										 OVBT_ATTR_FORMAT_DICT |
										 OVBT_ATTR_FORMAT_FIXED_BIN |
										 OVBT_ATTR_FORMAT_FSST));
	citem->t_num_elements = item->t_num_elements;
	citem->t_num_codewords = item->t_num_codewords;
	citem->t_uncompressed_size = uncompressed_size;
	citem->t_firsttid = item->t_firsttid;
	citem->t_endtid = item->t_endtid;

	/* try compressing */
	compressed_size = ov_try_compress(uncompressed_payload,
									  citem->t_payload,
									  uncompressed_size,
									  item_allocsize - offsetof(OVAttributeCompressedItem, t_payload));

	/*
	 * Skip compression if it wouldn't save at least 8 bytes. There are some
	 * extra header bytes on compressed items, so if we didn't check for this,
	 * the compressed item might actually be larger than the original item,
	 * even if the size of the compressed portion was the same as uncompressed
	 * size, (or 1-2 bytes less). The 8 byte marginal fixes that problem.
	 * Besides, it's hardly worth the CPU overhead of having to decompress on
	 * reading, for a saving of a few bytes.
	 */
	if (compressed_size > 0 && compressed_size + 8 < uncompressed_size)
	{
		citem->t_size = offsetof(OVAttributeCompressedItem, t_payload) + compressed_size;
		Assert(citem->t_size < item->t_size);
		return (OVAttributeArrayItem *) citem;
	}
	else
		return item;
}


/*
 * Re-pack and compress a list of items.
 *
 * If there are small items in the input list, such that they can be merged
 * together into larger items, we'll do that. And if there are uncompressed
 * items, we'll try to compress them. If the input list contains "exploded"
 * in-memory items, they will be packed into proper items suitable for
 * storing on-disk.
 */
List *
ovbt_attr_recompress_items(Form_pg_attribute attr, List *items)
{
	List	   *newitems = NIL;
	int			i;

	/*
	 * Heuristics needed on when to try recompressing or merging existing
	 * items. Some musings on that:
	 *
	 * - If an item is already compressed, and close to maximum size, then it
	 * probably doesn't make sense to recompress. - If there are two adjacent
	 * items that are short, then it is probably worth trying to merge them.
	 */

	/* loop through items, and greedily pack them */

	i = 0;
	while (i < list_length(items))
	{
		int			total_num_elements = 0;
		size_t		total_size = 0;
		int			j;
		OVAttributeArrayItem *newitem;

		for (j = i; j < list_length(items); j++)
		{
			OVAttributeArrayItem *this_item = (OVAttributeArrayItem *) list_nth(items, j);
			size_t		this_size;
			int			this_num_elements;

			this_size = ovbt_item_uncompressed_size(this_item);
			this_num_elements = this_item->t_num_elements;

			/*
			 * don't create an item that's too large, in terms of size, or in
			 * # of tids
			 */
			if ((size_t) (total_num_elements + this_num_elements) > MAX_TIDS_PER_ATTR_ITEM)
				break;
			if (total_size + this_size > MAX_ATTR_ITEM_SIZE)
				break;
			total_size += this_size;
			total_num_elements += this_num_elements;
		}
		if (j == i)
			j++;				/* tolerate existing oversized items */

		/* i - j are the items to pack */
		if (j - i > 1)
		{
			OVAttributeArrayItem *packeditem;
			OVExplodedItem *combineditem;

			combineditem = ovbt_combine_items(items, i, j);
			packeditem = ovbt_pack_item(attr, combineditem);
			newitem = ovbt_compress_item(packeditem);
		}
		else
		{
			OVAttributeArrayItem *olditem = list_nth(items, i);

			if (olditem->t_size == 0)
			{
				newitem = ovbt_pack_item(attr, (OVExplodedItem *) olditem);
				newitem = ovbt_compress_item(newitem);
			}
			else if (olditem->t_flags & OVBT_ATTR_COMPRESSED)
				newitem = olditem;
			else
				newitem = ovbt_compress_item(olditem);
		}

		newitems = lappend(newitems, newitem);

		i = j;
	}

	/* Check that the resulting items are in correct order, and don't overlap. */
#ifdef USE_ASSERT_CHECKING
	{
		ovtid		endtid = 0;
		ListCell   *lc;

		foreach(lc, newitems)
		{
			OVAttributeArrayItem *i = (OVAttributeArrayItem *) lfirst(lc);

			Assert(i->t_firsttid >= endtid);
			Assert(i->t_endtid > i->t_firsttid);
			endtid = i->t_endtid;

			/* there should be no exploded items left */
			Assert(i->t_size != 0);
		}
	}
#endif

	return newitems;
}
