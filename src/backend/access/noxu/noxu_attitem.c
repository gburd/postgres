/*
 * noxu_attitem.c
 *		Routines for packing datums into "items", in the attribute trees.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/noxu/noxu_attitem.c
 */
#include "postgres.h"

#include "access/detoast.h"
#include "access/noxu_array.h"
#include "access/noxu_compression.h"
#include "access/noxu_dict.h"
#include "access/noxu_internal.h"
#include "access/noxu_shared_dict.h"
#include "access/noxu_simple8b.h"
#include "access/noxu_uuid.h"
#include "catalog/pg_type.h"
#include "lib/chimp.h"
#include "lib/float16.h"
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
#define		MAX_ATTR_ITEM_SIZE		(MaxNoxuDatumSize / 4)
#define		MAX_TIDS_PER_ATTR_ITEM	((BLCKSZ / 2) / sizeof(nxtid))

static void fetch_att_array(char *src, int srcSize, bool hasnulls,
							int numelements, uint16 item_flags,
							NXAttrTreeScan * scan);
static void fetch_att_array_for(char *src, int srcSize, bool hasnulls,
								int numelements,
				NXAttrTreeScan * scan);
static void fetch_att_array_bitpacked(char *src, int srcSize, bool hasnulls,
									  int numelements,
				NXAttrTreeScan * scan);
static void fetch_att_array_fixed_bin(char *src, int srcSize, bool hasnulls,
									  int numelements,
				NXAttrTreeScan * scan);
static void fetch_att_array_uuid_v7_delta(char *src, int srcSize,
										  bool hasnulls, int numelements,
				NXAttrTreeScan * scan);
static void fetch_att_array_dod(char *src, int srcSize, bool hasnulls,
								int numelements,
				NXAttrTreeScan * scan);
static void fetch_att_array_chimp(char *src, int srcSize, bool hasnulls,
								  int numelements,
				NXAttrTreeScan * scan);

/*
 * Maximum varlena data size (excluding header) for which we use native
 * PostgreSQL 1-byte short varlena format.  Capped at 125 to keep the PG 1B
 * header byte <= 0xFD, avoiding collision with the 0xFE escape byte and
 * the 0xFF byte used by noxu overflow pointers.
 */
#define NATIVE_VARLENA_MAX_DATA		125

/*
 * In native varlena items, long values (data > 125 bytes) use a 3-byte
 * header: escape byte 0xFE, followed by a 2-byte big-endian data length.
 * This avoids ambiguity with PG 1B headers (low bit set) and overflow
 * pointers (0xFFFF).
 */
#define NATIVE_VARLENA_LONG_ESCAPE	0xFE

static NXAttributeArrayItem * nxbt_attr_create_item(Form_pg_attribute att,
													Datum *datums, bool *isnulls, nxtid *tids, int nitems,
																bool has_nulls, int datasz,
																bool use_native_varlena);
static NXExplodedItem * nxbt_attr_explode_item(Form_pg_attribute att,
											   NXAttributeArrayItem * item,
											   Relation rel, AttrNumber attno);

/*
 * Compute the on-disk size of a single varlena datum, understanding native
 * format items where short varlenas use PG 1-byte headers.
 */
static inline int
nxbt_attr_datasize_ex(int attlen, char *src, uint16 item_flags)
{
	unsigned char *p = (unsigned char *) src;

	if (attlen > 0)
		return attlen;

	/*
	 * Native varlena format: short varlenas are stored with PG 1-byte
	 * headers where the low bit is always 1.  Long varlenas use a 3-byte
	 * header: 0xFE escape + 2-byte BE data length.
	 */
	if ((item_flags & NXBT_ATTR_FORMAT_NATIVE_VARLENA) != 0)
	{
		if (p[0] == 0xFF && p[1] == 0xFF)
			return 6;			/* noxu overflow pointer */
		if (p[0] == NATIVE_VARLENA_LONG_ESCAPE)
		{
			/* 3-byte header: 0xFE + 2-byte BE data length */
			uint16		data_len = (p[1] << 8) | p[2];
			return 3 + data_len;
		}
		if ((*p & 0x01) != 0)
			return *p >> 1;		/* PG 1B: total_len = header >> 1 */
		/* Should not reach here in a well-formed native item */
		elog(ERROR, "invalid native varlena header byte 0x%02x", p[0]);
	}

	/* Original noxu format */
	if ((p[0] & 0x80) == 0)
		return p[0];			/* single-byte header */
	else if (p[0] == 0xFF && p[1] == 0xFF)
		return 6;				/* noxu-overflow pointer */
	else
		return ((p[0] & 0x7F) << 8 | p[1]) + 1;	/* two-byte header */
}

/*
 * Check whether an attribute is a boolean column suitable for bit-packing.
 * Boolean columns in PostgreSQL have OID 16 (BOOLOID), attlen=1, attbyval=true.
 */
static inline bool
nxbt_attr_is_boolean(Form_pg_attribute att)
{
	return (att->atttypid == BOOLOID && att->attlen == 1 && att->attbyval);
}

/*
 * Check whether an attribute looks like a float vector/embedding column
 * suitable for bfloat16 scalar quantization.
 *
 * Detection heuristics (any of the following):
 *   1. Type is float4[] (OID 1021) or float8[] (OID 1022)
 *   2. Column name contains "embedding", "vector", or "vec_"
 *      AND type is variable-length (attlen == -1)
 *   3. Type is variable-length with attlen == -1 and avg datum width > 2KB
 *      (suggests large float arrays from pg_vector extension)
 *
 * The actual quantization step validates float contents before proceeding.
 */
static bool
is_vector_column(Form_pg_attribute att, Datum *datums, bool *isnulls,
				 int num_elements)
{
	Oid			typid = att->atttypid;

	/* Direct match: float4[] or float8[] */
	if (typid == FLOAT4ARRAYOID || typid == FLOAT8ARRAYOID)
		return true;

	/* Column name heuristic for pg_vector or similar extension types */
	if (att->attlen == -1)
	{
		const char *name = NameStr(att->attname);

		if (strstr(name, "embedding") != NULL ||
			strstr(name, "vector") != NULL ||
			strstr(name, "vec_") != NULL)
			return true;
	}

	/*
	 * Large variable-length data heuristic: if average non-null datum is
	 * > 2KB, it might be a vector type from an extension like pg_vector.
	 * Sample up to 32 datums to estimate average width.
	 */
	if (att->attlen == -1 && num_elements >= 4)
	{
		int64		total_size = 0;
		int			nonnull = 0;
		int			limit = Min(num_elements, 32);

		for (int i = 0; i < limit; i++)
		{
			if (!isnulls[i])
			{
				struct varlena *vl = (struct varlena *) DatumGetPointer(datums[i]);

				if (!VARATT_IS_EXTERNAL(vl))
				{
					total_size += VARSIZE_ANY_EXHDR(vl);
					nonnull++;
				}
			}
		}

		if (nonnull > 0 && (total_size / nonnull) > 2048)
			return true;
	}

	return false;
}

/*
 * Quantize float32 data within a varlena datum to bfloat16.
 *
 * For float4[] arrays and pg_vector types, extracts the packed float32
 * elements, converts each to bfloat16, and repacks into a smaller varlena.
 *
 * Returns a palloc'd Datum with quantized data, or the original datum
 * if quantization is not applicable.  Sets *quantized to true on success.
 *
 * The quantized format is: [uint16 num_floats][bfloat16 values...]
 * packed into a varlena.
 */
static Datum
quantize_vector_datum(Datum original, Form_pg_attribute att, bool *quantized)
{
	struct varlena *vl;
	char	   *data;
	int			data_len;
	int			num_floats;
	const float *floats;
	bfloat16   *bf_data;
	int			bf_size;
	struct varlena *result;

	*quantized = false;

	vl = (struct varlena *) DatumGetPointer(original);
	if (VARATT_IS_EXTERNAL(vl))
		return original;

	data = VARDATA_ANY(vl);
	data_len = VARSIZE_ANY_EXHDR(vl);

	/*
	 * Heuristic: if the data length is a multiple of sizeof(float) and
	 * large enough, treat it as a packed float array.
	 */
	if (data_len < 16 || (data_len % sizeof(float)) != 0)
		return original;

	num_floats = data_len / sizeof(float);
	floats = (const float *) data;

	/* Validate the floats are quantization-worthy */
	if (!float_array_should_quantize(floats, num_floats))
		return original;

	/* Allocate: 2-byte count + bfloat16 array, wrapped in varlena */
	bf_size = sizeof(uint16) + num_floats * sizeof(bfloat16);
	result = (struct varlena *) palloc(VARHDRSZ + bf_size);
	SET_VARSIZE(result, VARHDRSZ + bf_size);

	/* Write num_floats count */
	*((uint16 *) VARDATA(result)) = (uint16) num_floats;

	/* Quantize */
	bf_data = (bfloat16 *) (VARDATA(result) + sizeof(uint16));
	quantize_float_array(floats, num_floats, bf_data);

	*quantized = true;
	return PointerGetDatum(result);
}

/*
 * Dequantize a bfloat16-quantized varlena back to float32.
 *
 * Input format: [uint16 num_floats][bfloat16 values...]
 * Returns a new varlena with original-sized float32 data.
 */
static Datum
dequantize_vector_datum(Datum quantized_datum)
{
	struct varlena *vl;
	char	   *data;
	int			data_len;
	uint16		num_floats;
	const bfloat16 *bf_data;
	struct varlena *result;
	float	   *floats;

	vl = (struct varlena *) DatumGetPointer(quantized_datum);
	data = VARDATA_ANY(vl);
	data_len = VARSIZE_ANY_EXHDR(vl);

	/* Read count */
	num_floats = *((const uint16 *) data);
	bf_data = (const bfloat16 *) (data + sizeof(uint16));

	/* Sanity check */
	if (data_len != (int) (sizeof(uint16) + num_floats * sizeof(bfloat16)))
		elog(ERROR, "invalid quantized vector datum size: expected %d, got %d",
			 (int) (sizeof(uint16) + num_floats * sizeof(bfloat16)), data_len);

	/* Allocate output varlena with float32 data */
	result = (struct varlena *) palloc(VARHDRSZ + num_floats * sizeof(float));
	SET_VARSIZE(result, VARHDRSZ + num_floats * sizeof(float));

	floats = (float *) VARDATA(result);
	dequantize_float_array(bf_data, num_floats, floats);

	return PointerGetDatum(result);
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
	uint8		bits = 0;
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
 *    (flag NXBT_ATTR_NO_NULLS is set, NXBT_HAS_NULLS is not set).
 *
 * 2. SPARSE_NULLS: For <5% NULL density, store (position, count) pairs
 *    rather than a full bitmap. Each pair is an NXSparseNullEntry.
 *    The data begins with a uint16 count of entries, followed by the entries.
 *
 * 3. RLE_NULLS: For sequential NULL runs of 8+, use run-length encoding.
 *    Each run is an NXRleNullEntry. Data begins with uint16 count of entries.
 */

/*
 * Analyze NULL distribution and choose the best encoding.
 * Returns one of NXBT_ATTR_NO_NULLS, NXBT_ATTR_SPARSE_NULLS,
 * NXBT_ATTR_RLE_NULLS, or NXBT_HAS_NULLS (standard bitmap).
 * Also returns the encoded size in *encoded_size.
 */
static uint16
choose_null_encoding(bool *isnulls, int num_elements, bool has_nulls,
					 int *encoded_size)
{
	int			bitmap_size = NXBT_ATTR_BITMAPLEN(num_elements);

	if (!has_nulls)
	{
		*encoded_size = 0;
		return NXBT_ATTR_NO_NULLS;
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
			num_rle_entries += (run_len + NXBT_RLE_COUNT_MASK - 1) / NXBT_RLE_COUNT_MASK;
		}

		/* Compute sizes for each encoding */
		sparse_size = sizeof(uint16) + num_sparse_entries * sizeof(NXSparseNullEntry);
		rle_size = sizeof(uint16) + num_rle_entries * sizeof(NXRleNullEntry);

		/* Use sparse encoding if <5% NULL density and it saves space */
		if (null_count * 20 < num_elements && sparse_size < bitmap_size)
		{
			*encoded_size = sparse_size;
			return NXBT_ATTR_SPARSE_NULLS;
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
				return NXBT_ATTR_RLE_NULLS;
			}
		}

		/* Fall back to standard bitmap */
		*encoded_size = bitmap_size;
		return NXBT_HAS_NULLS;
	}
}

/*
 * Write sparse NULL encoding into dst.
 * Format: uint16 num_entries, followed by NXSparseNullEntry[num_entries].
 * Returns pointer past the written data.
 */
static char *
write_sparse_nulls(bool *isnulls, int num_elements, char *dst)
{
	uint16		num_entries = 0;
	char	   *count_ptr = dst;
	NXSparseNullEntry *entries;
	int			i;

	/* Reserve space for the entry count */
	dst += sizeof(uint16);
	entries = (NXSparseNullEntry *) dst;

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
	dst += num_entries * sizeof(NXSparseNullEntry);
	return dst;
}

/*
 * Write RLE NULL encoding into dst.
 * Format: uint16 num_entries, followed by NXRleNullEntry[num_entries].
 * Returns pointer past the written data.
 */
static char *
write_rle_nulls(bool *isnulls, int num_elements, char *dst)
{
	uint16		num_entries = 0;
	char	   *count_ptr = dst;
	NXRleNullEntry *entries;
	int			i;

	/* Reserve space for the entry count */
	dst += sizeof(uint16);
	entries = (NXRleNullEntry *) dst;

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
			int		this_len = Min(run_len, NXBT_RLE_COUNT_MASK);

			entries[num_entries].rle_count = this_len;
			if (cur_null)
				entries[num_entries].rle_count |= NXBT_RLE_NULL_FLAG;
			num_entries++;
			run_len -= this_len;
		}
	}

	memcpy(count_ptr, &num_entries, sizeof(uint16));
	dst += num_entries * sizeof(NXRleNullEntry);
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
	NXSparseNullEntry *entries;

	memset(isnulls, 0, num_elements * sizeof(bool));

	memcpy(&num_entries, src, sizeof(uint16));
	src += sizeof(uint16);
	entries = (NXSparseNullEntry *) src;

	for (int i = 0; i < num_entries; i++)
	{
		for (int j = 0; j < entries[i].sn_count; j++)
		{
			int pos = entries[i].sn_position + j;

			if (pos < num_elements)
				isnulls[pos] = true;
		}
	}

	src += num_entries * sizeof(NXSparseNullEntry);
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
	NXRleNullEntry *entries;
	int			pos = 0;

	memcpy(&num_entries, src, sizeof(uint16));
	src += sizeof(uint16);
	entries = (NXRleNullEntry *) src;

	for (int i = 0; i < num_entries && pos < num_elements; i++)
	{
		bool	is_null = (entries[i].rle_count & NXBT_RLE_NULL_FLAG) != 0;
		int		run_len = entries[i].rle_count & NXBT_RLE_COUNT_MASK;

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

	src += num_entries * sizeof(NXRleNullEntry);
	return src;
}

/*
 * Convert sparse or RLE NULL encoding into a standard bitmap.
 * Used by nxbt_attr_explode_item() to normalize the representation.
 */
static uint8 *
decode_nulls_to_bitmap(unsigned char *src, int num_elements, uint16 null_flags,
					   int *bytes_consumed)
{
	bool	   *isnulls;
	uint8	   *bitmap;
	unsigned char *start = src;

	isnulls = palloc(num_elements * sizeof(bool));

	if (null_flags & NXBT_ATTR_SPARSE_NULLS)
		src = read_sparse_nulls(src, isnulls, num_elements);
	else if (null_flags & NXBT_ATTR_RLE_NULLS)
		src = read_rle_nulls(src, isnulls, num_elements);
	else
	{
		/* should not be called for standard bitmap or no-nulls */
		pfree(isnulls);
		*bytes_consumed = 0;
		return NULL;
	}

	bitmap = palloc0(NXBT_ATTR_BITMAPLEN(num_elements));
	for (int i = 0; i < num_elements; i++)
	{
		if (isnulls[i])
			nxbt_attr_item_setnull(bitmap, i);
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
	for_datasz = sizeof(NXForHeader) + (int) NXBT_FOR_PACKED_SIZE(num_nonnull, bpv);

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

	memset(dst, 0, (int) NXBT_FOR_PACKED_SIZE(nvalues, bpv));

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
 * Zigzag encoding: map signed int64 to unsigned uint64 so that small
 * absolute values (positive or negative) produce small unsigned values.
 *   0 -> 0,  -1 -> 1,  1 -> 2,  -2 -> 3, ...
 */
static inline uint64
zigzag_encode(int64 val)
{
	return (uint64) ((val << 1) ^ (val >> 63));
}

static inline int64
zigzag_decode(uint64 val)
{
	return (int64) ((val >> 1) ^ -(int64) (val & 1));
}

/*
 * Check whether delta-of-delta encoding is beneficial for the given data.
 *
 * Delta-of-delta is most effective for monotonically increasing 8-byte
 * integer types (timestamps) where the first-order deltas have low
 * variance.  The second-order deltas (delta-of-deltas) are then near zero,
 * and can be bit-packed very tightly.
 *
 * Returns true if delta-of-delta should be used, filling in the output
 * parameters with encoding parameters and the encoded data size.
 */
static bool
dod_should_encode(Form_pg_attribute att, Datum *datums, bool *isnulls,
				  int num_elements, int raw_datasz,
				  uint64 *initial_value_p, int64 *initial_delta_p,
				  int *bits_per_value_p, int *dod_datasz_p)
{
	uint64		vals[MAX_TIDS_PER_ATTR_ITEM];
	uint64		max_zigzag;
	int			nvals;
	int			bpv;
	int			dod_datasz;

	/* Only 8-byte pass-by-value types */
	if (att->attlen != sizeof(int64) || !att->attbyval)
		return false;

	/* Collect non-null values */
	nvals = 0;
	for (int j = 0; j < num_elements; j++)
	{
		if (!isnulls[j])
			vals[nvals++] = (uint64) DatumGetInt64(datums[j]);
	}

	/* Need at least 3 non-null values (2 deltas to form 1 delta-of-delta) */
	if (nvals < 3)
		return false;

	/* Check monotonicity */
	for (int i = 1; i < nvals; i++)
	{
		if (vals[i] < vals[i - 1])
			return false;	/* not monotonically increasing */
	}

	/*
	 * Compute delta-of-deltas and find the maximum zigzag-encoded value
	 * to determine bits-per-value.
	 */
	max_zigzag = 0;
	for (int i = 1; i < nvals - 1; i++)
	{
		int64		delta_prev = (int64) (vals[i] - vals[i - 1]);
		int64		delta_curr = (int64) (vals[i + 1] - vals[i]);
		int64		dod = delta_curr - delta_prev;
		uint64		z = zigzag_encode(dod);

		if (z > max_zigzag)
			max_zigzag = z;
	}

	bpv = for_bits_needed(max_zigzag);

	/*
	 * Compute encoded size: header + bit-packed delta-of-deltas.
	 * We encode (nvals - 2) delta-of-delta values (the first two values
	 * are stored in the header as initial_value and initial_delta).
	 */
	dod_datasz = sizeof(NXDeltaOfDeltaHeader) +
		(int) NXBT_DOD_PACKED_SIZE(nvals - 2, bpv);

	/* Only use delta-of-delta if we save at least 25% compared to raw */
	if (dod_datasz >= raw_datasz * 3 / 4)
		return false;

	*initial_value_p = vals[0];
	*initial_delta_p = (int64) (vals[1] - vals[0]);
	*bits_per_value_p = bpv;
	*dod_datasz_p = dod_datasz;
	return true;
}

/*
 * Bit-pack an array of zigzag-encoded delta-of-delta values into a byte
 * buffer.  Uses the same LSB-first packing as FOR encoding.
 */
static void
dod_pack_values(unsigned char *dst, int64 *dod_values, int nvalues, int bpv)
{
	int			bitpos = 0;

	if (bpv == 0)
		return;

	memset(dst, 0, (int) NXBT_DOD_PACKED_SIZE(nvalues, bpv));

	for (int i = 0; i < nvalues; i++)
	{
		uint64		val = zigzag_encode(dod_values[i]);
		int			byte_idx = bitpos / 8;
		int			bit_offset = bitpos % 8;
		int			bits_remaining = bpv;

		while (bits_remaining > 0)
		{
			int			bits_in_this_byte = 8 - bit_offset;

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
 * Unpack bit-packed zigzag-encoded delta-of-delta values from a byte buffer.
 */
static void
dod_unpack_values(const unsigned char *src, int64 *dod_values, int nvalues,
				  int bpv)
{
	int			bitpos = 0;

	if (bpv == 0)
	{
		memset(dod_values, 0, nvalues * sizeof(int64));
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
			int			bits_in_this_byte = 8 - bit_offset;

			if (bits_in_this_byte > bits_remaining)
				bits_in_this_byte = bits_remaining;

			val |= (uint64) ((src[byte_idx] >> bit_offset) & ((1U << bits_in_this_byte) - 1)) << shift;
			shift += bits_in_this_byte;
			bits_remaining -= bits_in_this_byte;
			byte_idx++;
			bit_offset = 0;
		}

		dod_values[i] = zigzag_decode(val);
		bitpos += bpv;
	}
}

/*
 * Create an attribute item, or items, from an array of tids and datums.
 */
List *
nxbt_attr_create_items(Form_pg_attribute att,
					   Datum *datums, bool *isnulls, nxtid *tids, int nitems)
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
		NXAttributeArrayItem *item;
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
			int			num_long_varlena = 0;

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
							 * Any overflow datums should've been taken care of
							 * before we get here. We might see
							 * "noxu-overflow" datums, but nothing else.
							 */
							if (VARTAG_EXTERNAL(vl) != VARTAG_NOXU)
								elog(ERROR, "unrecognized overflow tag");
							this_sz = 2 + sizeof(BlockNumber);
						}
						else if (VARATT_IS_COMPRESSED(vl))
						{
							/*
							 * Inline compressed datum. Decompress it so we
							 * can store the raw data in the attribute item.
							 * The attribute item itself will be compressed as
							 * a whole by noxu, so keeping individual datums
							 * compressed is redundant.
							 */
							struct varlena *detoasted = detoast_attr(vl);

							datums[j] = PointerGetDatum(detoasted);
							this_sz = VARSIZE_ANY_EXHDR(detoasted);

							if (this_sz > NATIVE_VARLENA_MAX_DATA)
								num_long_varlena++;

							if ((this_sz + 1) > 0x7F)
								this_sz += 2;
							else
								this_sz += 1;
						}
						else
						{
							this_sz = VARSIZE_ANY_EXHDR(DatumGetPointer(datums[j]));

							if (this_sz > NATIVE_VARLENA_MAX_DATA)
								num_long_varlena++;

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
							num_long_varlena++;

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
			 * Use native varlena format when the attribute supports it
			 * (attlen == -1, not plain storage).  In native mode, short
			 * values (<= 125 data bytes) use PG 1-byte headers for
			 * zero-copy reads, long values use a 3-byte escape header
			 * (0xFE + 2-byte BE length), and overflow pointers keep their
			 * 0xFFFF format (checked first in the read dispatch, before
			 * any header-byte ambiguity).
			 *
			 * Long values cost 1 extra byte each (3-byte native header
			 * vs 2-byte noxu header), so we account for that.
			 */
			if (att->attlen == -1 && att->attstorage != 'p')
			{
				use_native_varlena = true;
				datasz += num_long_varlena;	/* 1 extra byte per long value */
			}
		}

		/*
		 * datasz now holds only the raw datum-data bytes.  The item
		 * header, TID codeword array, and null bitmap overhead are added
		 * inside nxbt_attr_create_item() where the actual codeword count
		 * is known (the estimate here could differ from reality).
		 */
		item = nxbt_attr_create_item(att,
									 &datums[i], &isnulls[i], &tids[i], num_elements,
									 has_nulls, datasz, use_native_varlena);

		newitems = lappend(newitems, item);
		i += num_elements;
	}

	return newitems;
}

/*
 * Pack an array of bools into a NULL bitmap.
 *
 * Processes 8 bools at a time to reduce per-element branching overhead.
 * Each group of 8 bools is combined into a single byte using arithmetic
 * rather than individual bit-set operations.
 */
static uint8 *
write_null_bitmap(bool *isnulls, int num_elements, uint8 *dst)
{
	int			full_bytes = num_elements / 8;
	int			remainder = num_elements % 8;

	/* Process complete groups of 8 bools */
	for (int i = 0; i < full_bytes; i++)
	{
		bool	   *b = &isnulls[i * 8];

		*dst++ = (b[0])
			| (b[1] << 1)
			| (b[2] << 2)
			| (b[3] << 3)
			| (b[4] << 4)
			| (b[5] << 5)
			| (b[6] << 6)
			| (b[7] << 7);
	}

	/* Handle remaining bits */
	if (remainder > 0)
	{
		bool	   *b = &isnulls[full_bytes * 8];
		uint8		bits = 0;

		for (int j = 0; j < remainder; j++)
		{
			if (b[j])
				bits |= 1 << j;
		}
		*dst++ = bits;
	}

	return dst;
}

/*
 * Copy a range of bits from a source bitmap to the beginning of a destination
 * bitmap.  The destination must be pre-zeroed.
 *
 * When src_bitoff is 0 this reduces to memcpy for full bytes, making the
 * left-split case essentially free.  For nonzero offsets (right-split) we
 * shift bytes rather than testing individual bits.
 */
static void
copy_null_bitmap(uint8 *dst, const uint8 *src, int src_bitoff, int nbits)
{
	int			src_byteoff = src_bitoff / 8;
	int			bitshift = src_bitoff % 8;
	int			nbytes = NXBT_ATTR_BITMAPLEN(nbits);

	if (bitshift == 0)
	{
		/* Aligned case: straight memcpy, mask the trailing bits */
		memcpy(dst, src + src_byteoff, nbytes);
		if (nbits % 8 != 0)
			dst[nbytes - 1] &= (1 << (nbits % 8)) - 1;
	}
	else
	{
		/*
		 * Unaligned case: shift pairs of source bytes.  Process the body
		 * without a bounds check (we know the next byte exists), then
		 * handle the last byte separately to avoid reading past the end.
		 */
		const uint8 *sp = src + src_byteoff;
		int			body_bytes = nbytes - 1;

		for (int i = 0; i < body_bytes; i++)
			dst[i] = (uint8) ((sp[i] | ((uint16) sp[i + 1] << 8)) >> bitshift);

		/* Last destination byte: may not have a next source byte */
		{
			uint16		w = sp[body_bytes];
			int			src_total_bytes = NXBT_ATTR_BITMAPLEN(src_bitoff + nbits);

			if (src_byteoff + body_bytes + 1 < src_total_bytes)
				w |= (uint16) sp[body_bytes + 1] << 8;
			dst[body_bytes] = (uint8) (w >> bitshift);
		}

		if (nbits % 8 != 0)
			dst[nbytes - 1] &= (1 << (nbits % 8)) - 1;
	}
}

/*
 * OR a source bitmap (starting at bit 0) into a destination bitmap at
 * dst_bitoff.  The destination must be pre-zeroed for the affected range.
 * Used by nxbt_combine_items() to merge multiple bitmaps at successive
 * offsets.
 */
static void
or_null_bitmap(uint8 *dst, const uint8 *src, int dst_bitoff, int nbits)
{
	int			dst_byteoff = dst_bitoff / 8;
	int			bitshift = dst_bitoff % 8;
	int			src_nbytes = NXBT_ATTR_BITMAPLEN(nbits);

	if (bitshift == 0)
	{
		/*
		 * Aligned: OR source bytes directly.  Process 8 bytes at a time
		 * using uint64 word operations when possible for fewer loop
		 * iterations on large bitmaps.
		 */
		uint8	   *dp = dst + dst_byteoff;
		int			i = 0;

		/* Word-at-a-time OR for the bulk */
		for (; i + (int) sizeof(uint64) <= src_nbytes; i += sizeof(uint64))
		{
			uint64		s;
			uint64		d;

			memcpy(&s, src + i, sizeof(uint64));
			memcpy(&d, dp + i, sizeof(uint64));
			d |= s;
			memcpy(dp + i, &d, sizeof(uint64));
		}

		/* Remaining bytes */
		for (; i < src_nbytes; i++)
			dp[i] |= src[i];
	}
	else
	{
		/*
		 * Unaligned: spread each source byte across two destination bytes.
		 */
		int			rshift = 8 - bitshift;

		for (int i = 0; i < src_nbytes; i++)
		{
			uint8		b = src[i];

			dst[dst_byteoff + i] |= (b << bitshift);
			dst[dst_byteoff + i + 1] |= (b >> rshift);
		}
	}
}

/*
 * Create an array item from given datums and tids.
 *
 * The caller has already computed the size the datums will require.
 */
static NXAttributeArrayItem *
nxbt_attr_create_item(Form_pg_attribute att,
					  Datum *datums, bool *isnulls, nxtid *tids, int num_elements,
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
	NXAttributeArrayItem *item;
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
	bool		use_array_decomposed = false;
	char	   *array_encoded = NULL;
	int			array_encoded_size = 0;
	bool		use_uuid_v7_delta = false;
	char	   *uuid_v7_encoded = NULL;
	int			uuid_v7_encoded_size = 0;
	bool		use_dod = false;
	uint64		dod_initial_value = 0;
	int64		dod_initial_delta = 0;
	int			dod_bpv = 0;
	int			dod_datasz = 0;
	bool		use_chimp = false;
	int			chimp_datasz = 0;
	bool		use_vector_quantized = false;
	uint16		null_encoding;
	int			null_encoded_size;
	int			effective_datasz;

	Assert(num_elements > 0);
	Assert((size_t) num_elements <= MAX_TIDS_PER_ATTR_ITEM);

	/*
	 * Check if this is a vector/embedding column suitable for bfloat16
	 * scalar quantization.  This is a pre-processing step that replaces
	 * the datums with quantized versions before any other encoding.
	 * The quantized datums are roughly half the size of the originals.
	 */
	if (att->attlen == -1 && is_vector_column(att, datums, isnulls, num_elements))
	{
		int			quantized_count = 0;
		int			new_datasz = 0;

		for (int j = 0; j < num_elements; j++)
		{
			if (!isnulls[j])
			{
				bool	q = false;

				datums[j] = quantize_vector_datum(datums[j], att, &q);
				if (q)
					quantized_count++;

				/* Recompute datum size for the (possibly quantized) datum */
				{
					size_t	this_sz = VARSIZE_ANY_EXHDR(DatumGetPointer(datums[j]));

					if ((this_sz + 1) > 0x7F)
						this_sz += 2;
					else
						this_sz += 1;
					new_datasz += this_sz;
				}
			}
		}

		/* Only use quantization if at least half the datums were quantized */
		if (quantized_count > 0 && quantized_count * 2 >= num_elements)
		{
			use_vector_quantized = true;
			datasz = new_datasz;
		}
	}

	/*
	 * Check if this is a boolean column that benefits from bit-packing.
	 * Bit-packing gives 8x compression (1 bit vs 1 byte per boolean),
	 * so it takes priority over FOR encoding for booleans.
	 */
	if (nxbt_attr_is_boolean(att))
	{
		int		num_nonnull = 0;

		for (int j = 0; j < num_elements; j++)
		{
			if (!isnulls[j])
				num_nonnull++;
		}
		bitpacked_datasz = NXBT_ATTR_BITMAPLEN(num_nonnull);

		if (bitpacked_datasz < datasz)
			use_bitpacked = true;
	}

	/* Check if FOR encoding is beneficial (skip if bitpacked) */
	if (!use_bitpacked)
		use_for = for_should_encode(att, datums, isnulls, num_elements, datasz,
									&for_frame_min, &for_bpv, &for_datasz);

	/*
	 * Check if delta-of-delta encoding beats FOR for monotonic 8-byte types.
	 * Delta-of-delta exploits the regularity of timestamp intervals: for
	 * evenly spaced timestamps, the second-order deltas are all zero.
	 * Compare against the better of raw and FOR-encoded sizes.
	 */
	if (!use_bitpacked)
	{
		int		compare_sz = use_for ? for_datasz : datasz;

		if (dod_should_encode(att, datums, isnulls, num_elements, compare_sz,
							  &dod_initial_value, &dod_initial_delta,
							  &dod_bpv, &dod_datasz))
		{
			use_dod = true;
			use_for = false;	/* delta-of-delta supersedes FOR */
		}
	}

	/*
	 * Check if Chimp float compression is beneficial.  Chimp uses XOR-based
	 * encoding that exploits similarity between consecutive float values.
	 * It is most effective for time-series float data with gradual changes.
	 * Skip if bitpacked or DOD was already selected.  Chimp can supersede
	 * FOR for float types since it typically achieves better compression.
	 */
	if (!use_bitpacked && !use_dod)
	{
		int		compare_sz = use_for ? for_datasz : datasz;

		if (chimp_should_encode(att, datums, isnulls, num_elements,
								compare_sz, &chimp_datasz))
		{
			use_chimp = true;
			use_for = false;	/* Chimp supersedes FOR for floats */
		}
	}

	/*
	 * Check if dictionary encoding is beneficial. Dictionary encoding is
	 * most effective for low-cardinality columns (few distinct values).
	 * Skip if another encoding was already selected.
	 */
	if (!use_bitpacked && !use_for && !use_dod && !use_chimp &&
		nx_dict_should_encode(att, datums, isnulls, num_elements))
	{
		dict_encoded = nx_dict_encode(att, datums, isnulls, num_elements,
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
	 * Check if array decomposition is beneficial.  Array decomposition
	 * extracts elements from PostgreSQL arrays and stores them contiguously,
	 * which compresses dramatically better than opaque ArrayType blobs.
	 * Skip if another encoding was already selected.
	 */
	if (!use_bitpacked && !use_for && !use_dod && !use_dict)
	{
		array_encoded = nx_array_decompose_and_encode(att, datums, isnulls,
													  num_elements,
													  &array_encoded_size);
		if (array_encoded != NULL)
			use_array_decomposed = true;
	}

	/*
	 * Check for UUID fixed-binary storage and UUID v7 delta encoding.
	 * UUID (typid=2950, typlen=16, pass-by-ref, char-aligned) benefits
	 * from an optimized read path.  When UUIDs are time-ordered (v1/v6/v7),
	 * delta encoding of the 48-bit timestamp prefix gives 3-5x compression.
	 */
	if (!use_bitpacked && !use_for && !use_dict &&
		att->attlen == UUID_LEN && !att->attbyval &&
		att->atttypid == 2950)
	{
		int		raw_uuid_size;
		int		nonnull = 0;

		for (int j = 0; j < num_elements; j++)
		{
			if (!isnulls[j])
				nonnull++;
		}
		raw_uuid_size = nonnull * UUID_LEN;

		/* Try UUID v7 delta encoding first */
		if (uuid_is_time_ordered(datums, isnulls, num_elements))
		{
			uuid_v7_encoded = uuid_compress_time_ordered(datums, isnulls,
														 num_elements,
														 &uuid_v7_encoded_size,
														 raw_uuid_size);
			if (uuid_v7_encoded != NULL)
				use_uuid_v7_delta = true;
		}

		/* Fall back to fixed-binary storage */
		if (!use_uuid_v7_delta)
			use_fixed_bin = true;
	}

	/* Choose the best NULL encoding strategy */
	null_encoding = choose_null_encoding(isnulls, num_elements, has_nulls,
										 &null_encoded_size);

	/*
	 * For dictionary encoding, NULL info is embedded in the dictionary
	 * indices (NX_DICT_NULL_INDEX), so skip the separate NULL encoding.
	 * For array decomposition, NULL arrays are indicated by an element
	 * count of 0, and element-level NULLs are in the decomposed header's
	 * bitmap, so separate NULL encoding is also unnecessary.
	 */
	if (use_dict || use_array_decomposed)
	{
		null_encoding = NXBT_ATTR_NO_NULLS;
		null_encoded_size = 0;
	}

	/* Determine effective data size */
	if (use_dict)
		effective_datasz = dict_encoded_size;
	else if (use_array_decomposed)
		effective_datasz = array_encoded_size;
	else if (use_bitpacked)
		effective_datasz = bitpacked_datasz;
	else if (use_chimp)
		effective_datasz = chimp_datasz;
	else if (use_dod)
		effective_datasz = dod_datasz;
	else if (use_for)
		effective_datasz = for_datasz;
	else if (use_uuid_v7_delta)
		effective_datasz = uuid_v7_encoded_size;
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

	itemsz = offsetof(NXAttributeArrayItem, t_tid_codewords);
	itemsz += num_codewords * sizeof(uint64);
	itemsz += null_encoded_size;
	itemsz += effective_datasz;

	item = palloc(itemsz);
	item->t_size = itemsz;
	item->t_flags = 0;

	/* Set NULL encoding flags */
	if (null_encoding == NXBT_HAS_NULLS)
		item->t_flags |= NXBT_HAS_NULLS;
	else if (null_encoding == NXBT_ATTR_NO_NULLS)
		item->t_flags |= NXBT_ATTR_NO_NULLS;
	else if (null_encoding == NXBT_ATTR_SPARSE_NULLS)
		item->t_flags |= NXBT_ATTR_SPARSE_NULLS | NXBT_HAS_NULLS;
	else if (null_encoding == NXBT_ATTR_RLE_NULLS)
		item->t_flags |= NXBT_ATTR_RLE_NULLS | NXBT_HAS_NULLS;

	/* Set data encoding flags */
	if (use_bitpacked)
		item->t_flags |= NXBT_ATTR_BITPACKED;
	if (use_dict)
		item->t_flags |= NXBT_ATTR_FORMAT_DICT;
	if (use_uuid_v7_delta)
		item->t_flags |= NXBT_ATTR_FORMAT_UUID_V7_DELTA;
	if (use_fixed_bin)
		item->t_flags |= NXBT_ATTR_FORMAT_FIXED_BIN;
	if (use_dod)
		item->t_flags |= NXBT_ATTR_FORMAT_DELTA_OF_DELTA;
	if (use_chimp)
		item->t_flags |= NXBT_ATTR_FORMAT_CHIMP;
	if (use_for)
		item->t_flags |= NXBT_ATTR_FORMAT_FOR;
	if (use_native_varlena)
		item->t_flags |= NXBT_ATTR_FORMAT_NATIVE_VARLENA;
	if (use_array_decomposed)
		item->t_flags |= NXBT_ATTR_FORMAT_ARRAY_DECOMPOSED;
	if (use_vector_quantized)
		item->t_flags |= NXBT_ATTR_VECTOR_QUANTIZED_F16;
	item->t_num_elements = num_elements;
	item->t_num_codewords = num_codewords;
	item->t_firsttid = tids[0];
	item->t_endtid = tids[num_elements - 1] + 1;

	for (int j = 0; j < num_codewords; j++)
		item->t_tid_codewords[j] = codewords[j];

	p = (char *) &item->t_tid_codewords[num_codewords];
	pend = ((char *) item) + itemsz;

	/* Write NULL information using the chosen encoding */
	if (null_encoding == NXBT_HAS_NULLS)
		p = (char *) write_null_bitmap(isnulls, num_elements, (uint8 *) p);
	else if (null_encoding == NXBT_ATTR_SPARSE_NULLS)
		p = write_sparse_nulls(isnulls, num_elements, p);
	else if (null_encoding == NXBT_ATTR_RLE_NULLS)
		p = write_rle_nulls(isnulls, num_elements, p);
	/* NXBT_ATTR_NO_NULLS: nothing to write */

	if (use_uuid_v7_delta)
	{
		/*
		 * UUID v7 delta-encoded data: copy the pre-encoded buffer which
		 * contains [NXUUIDDeltaHeader][packed deltas][suffixes].
		 */
		memcpy(p, uuid_v7_encoded, uuid_v7_encoded_size);
		p += uuid_v7_encoded_size;
		pfree(uuid_v7_encoded);
	}
	else if (use_array_decomposed)
	{
		/*
		 * Array-decomposed data: copy the pre-encoded buffer which
		 * contains [NXArrayDecomposedHeader][elem_counts][nullbits][elements].
		 */
		memcpy(p, array_encoded, array_encoded_size);
		p += array_encoded_size;
		pfree(array_encoded);
	}
	else if (use_dict)
	{
		/*
		 * Dictionary-encoded data: copy the pre-encoded buffer which
		 * contains [NXDictHeader][offsets][values][indices].
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
	else if (use_dod)
	{
		/*
		 * Write delta-of-delta encoded data: header followed by bit-packed
		 * zigzag-encoded second-order deltas.
		 */
		NXDeltaOfDeltaHeader *dodhdr = (NXDeltaOfDeltaHeader *) p;
		uint64		ts_vals[MAX_TIDS_PER_ATTR_ITEM];
		int64		dod_vals[MAX_TIDS_PER_ATTR_ITEM];
		int			nvals = 0;
		int			ndod;

		dodhdr->dod_initial_value = dod_initial_value;
		dodhdr->dod_initial_delta = dod_initial_delta;
		dodhdr->dod_bits_per_value = dod_bpv;
		p += sizeof(NXDeltaOfDeltaHeader);

		/* Collect non-null values */
		for (int j = 0; j < num_elements; j++)
		{
			if (!isnulls[j])
				ts_vals[nvals++] = (uint64) DatumGetInt64(datums[j]);
		}

		/* Compute delta-of-deltas */
		ndod = nvals - 2;
		for (int i = 0; i < ndod; i++)
		{
			int64		delta_prev = (int64) (ts_vals[i + 1] - ts_vals[i]);
			int64		delta_curr = (int64) (ts_vals[i + 2] - ts_vals[i + 1]);

			dod_vals[i] = delta_curr - delta_prev;
		}

		dod_pack_values((unsigned char *) p, dod_vals, ndod, dod_bpv);
		p += NXBT_DOD_PACKED_SIZE(ndod, dod_bpv);
	}
	else if (use_chimp)
	{
		/*
		 * Write Chimp-encoded float data: header followed by variable-length
		 * XOR-encoded values. Chimp exploits similarity between consecutive
		 * floats via leading/trailing zero compression of XOR deltas.
		 */
		int			bytes_written;
		int			capacity = pend - p;

		if (att->atttypid == FLOAT8OID)
			bytes_written = chimp_encode_float8(datums, isnulls, num_elements,
												p, capacity);
		else
			bytes_written = chimp_encode(datums, isnulls, num_elements,
										 p, capacity);
		Assert(bytes_written > 0 && bytes_written == chimp_datasz);
		p += bytes_written;
	}
	else if (use_for)
	{
		/*
		 * Write FOR-encoded data: header followed by bit-packed deltas.
		 */
		NXForHeader *forhdr = (NXForHeader *) p;
		uint64		for_vals[MAX_TIDS_PER_ATTR_ITEM];
		int			nvals = 0;

		forhdr->for_frame_min = for_frame_min;
		forhdr->for_bits_per_value = for_bpv;
		forhdr->for_attlen = att->attlen;
		p += sizeof(NXForHeader);

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
		p += NXBT_FOR_PACKED_SIZE(nvals, for_bpv);
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
					varatt_nx_overflowptr *nxoverflow;

					/*
					 * Any overflow datums should've been taken care of before
					 * we get here. We might see "noxu-overflow" datums, but
					 * nothing else.
					 */
					if (VARTAG_EXTERNAL(vl) != VARTAG_NOXU)
						elog(ERROR, "unrecognized overflow tag");

					nxoverflow = (varatt_nx_overflowptr *) DatumGetPointer(datums[j]);

					/*
					 * 0xFFFF identifies a overflow pointer. Followed by the
					 * block number of the first overflow page.
					 */
					*(p++) = 0xFF;
					*(p++) = 0xFF;
					memcpy(p, &nxoverflow->nxt_block, sizeof(BlockNumber));
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
						if (this_sz <= NATIVE_VARLENA_MAX_DATA)
						{
							/*
							 * Store in PG native 1-byte short varlena
							 * format.  The read path can return a direct
							 * pointer without copying.
							 */
							SET_VARSIZE_1B(p, 1 + this_sz);
							memcpy(p + 1, src, this_sz);
							p += 1 + this_sz;
						}
						else
						{
							/*
							 * Long value in native mode: 3-byte header
							 * (0xFE escape + 2-byte BE data length).
							 */
							*(p++) = NATIVE_VARLENA_LONG_ESCAPE;
							*(p++) = (this_sz >> 8) & 0xFF;
							*(p++) = this_sz & 0xFF;
							memcpy(p, src, this_sz);
							p += this_sz;
						}
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
nxbt_attr_datasize(int attlen, char *src)
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
		/* noxu-overflow pointer. */
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
NXExplodedItem *
nxbt_attr_remove_from_item(Form_pg_attribute attr,
						   NXAttributeArrayItem * olditem,
						   nxtid *removetids,
						   Relation rel, AttrNumber attno)
{
	NXExplodedItem *origitem;
	NXExplodedItem *newitem;
	int			i;
	int			j;
	char	   *src;
	char	   *dst;

	origitem = nxbt_attr_explode_item(attr, olditem, rel, attno);

	newitem = palloc(sizeof(NXExplodedItem));
	newitem->tids = palloc(origitem->t_num_elements * sizeof(nxtid));
	newitem->nullbitmap = palloc0(NXBT_ATTR_BITMAPLEN(origitem->t_num_elements));
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

		this_isnull = nxbt_attr_item_isnull(origitem->nullbitmap, i);
		if (!this_isnull)
			this_datasz = nxbt_attr_datasize_ex(attr->attlen, src, origitem->t_flags);
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
				nxbt_attr_item_setnull(newitem->nullbitmap, j);
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
		/* Free newitem sub-allocations before freeing newitem itself */
		pfree(newitem->tids);
		pfree(newitem->nullbitmap);
		pfree(newitem->datumdata);
		pfree(newitem);
		/* Free origitem if it's an exploded copy */
		if (origitem->t_size == 0)
		{
			pfree(origitem->tids);
			pfree(origitem->nullbitmap);
			pfree(origitem->datumdata);
			pfree(origitem);
		}
		return NULL;
	}

	newitem->t_size = 0;
	newitem->t_flags = origitem->t_flags & NXBT_ATTR_FORMAT_MASK;
	newitem->t_num_elements = j;
	newitem->datumdatasz = dst - newitem->datumdata;

	Assert(newitem->datumdatasz <= origitem->datumdatasz);

	/* Free origitem if it's an exploded copy */
	if (origitem->t_size == 0)
	{
		pfree(origitem->tids);
		pfree(origitem->nullbitmap);
		pfree(origitem->datumdata);
		pfree(origitem);
	}

	return newitem;
}

/*
 *
 * Extract TID and Datum/isnull arrays the given array item.
 *
 * The arrays are stored directly into the scan->array_* fields.
 *
 * When extract_hint_tid is set in the scan, we use binary search after
 * TID decoding to pre-position array_curr_idx, so the caller's linear
 * scan in nxbt_attr_fetch() skips leading elements efficiently.
 */
void
nxbt_attr_item_extract(NXAttrTreeScan * scan, NXAttributeArrayItem * item)
{
	int			nelements = item->t_num_elements;
	char	   *p;
	char	   *pend;
	nxtid		currtid;
	nxtid	   *tids;
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
		scan->array_tids = MemoryContextAlloc(scan->context, newsize * sizeof(nxtid));
		scan->array_datums_allocated_size = newsize;
	}

	/* decompress if needed */
	if ((item->t_flags & NXBT_ATTR_COMPRESSED) != 0)
	{
		NXAttributeCompressedItem *citem = (NXAttributeCompressedItem *) item;

		if (scan->decompress_buf_size < citem->t_uncompressed_size)
		{
			size_t		newsize = citem->t_uncompressed_size * 2;

			if (scan->decompress_buf != NULL)
				pfree(scan->decompress_buf);
			scan->decompress_buf = MemoryContextAlloc(scan->context, newsize);
			scan->decompress_buf_size = newsize;
		}

		p = (char *) citem->t_payload;
		if ((item->t_flags & NXBT_ATTR_SHARED_DICT) != 0)
		{
			/*
			 * Item was compressed with a shared dictionary.  Load the
			 * dictionary and use dictionary-aware decompression.
			 */
			NXSharedDictData *dict = nx_shared_dict_load(scan->rel, scan->attno);

			if (dict != NULL)
				nx_decompress_with_shared_dict(p, scan->decompress_buf,
											   citem->t_size - offsetof(NXAttributeCompressedItem, t_payload),
											   citem->t_uncompressed_size, dict);
			else
				elog(ERROR, "shared dictionary required for decompression but not found "
					 "for attribute %d", scan->attno);
		}
		else if ((item->t_flags & NXBT_ATTR_FORMAT_FSST) != 0)
			nx_decompress_with_fsst(p, scan->decompress_buf,
									citem->t_size - offsetof(NXAttributeCompressedItem, t_payload),
									citem->t_uncompressed_size, NULL);
		else
			nx_decompress(p, scan->decompress_buf,
						  citem->t_size - offsetof(NXAttributeCompressedItem, t_payload),
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
	 *
	 * We ensure array_isnulls is always populated by this point so that
	 * the downstream fetch_att_array* functions never need to redundantly
	 * zero it for the no-nulls case.
	 */
	if ((item->t_flags & NXBT_ATTR_SPARSE_NULLS) != 0)
	{
		p = (char *) read_sparse_nulls((unsigned char *) p,
									   scan->array_isnulls, nelements);
	}
	else if ((item->t_flags & NXBT_ATTR_RLE_NULLS) != 0)
	{
		p = (char *) read_rle_nulls((unsigned char *) p,
									scan->array_isnulls, nelements);
	}
	else if ((item->t_flags & NXBT_ATTR_NO_NULLS) != 0)
	{
		memset(scan->array_isnulls, 0, nelements * sizeof(bool));
	}
	else if ((item->t_flags & NXBT_HAS_NULLS) == 0)
	{
		/*
		 * Legacy items without explicit NXBT_ATTR_NO_NULLS flag but also
		 * without NXBT_HAS_NULLS: treat as no nulls.
		 */
		memset(scan->array_isnulls, 0, nelements * sizeof(bool));
	}

	/*
	 * Determine whether a standard inline NULL bitmap remains in the data
	 * stream. Enhanced NULL encodings (sparse, RLE, no-nulls) were already
	 * consumed above, so only standard NXBT_HAS_NULLS has an inline bitmap.
	 */
	{
	bool		has_inline_bitmap;

	has_inline_bitmap = ((item->t_flags & NXBT_HAS_NULLS) != 0) &&
						((item->t_flags & (NXBT_ATTR_SPARSE_NULLS |
										   NXBT_ATTR_RLE_NULLS |
										   NXBT_ATTR_NO_NULLS)) == 0);

	/*
	 * Expand the packed array data into an array of Datums.
	 *
	 * It would perhaps be more natural to loop through the elements with
	 * datumGetSize() and fetch_att(), but this is a pretty hot loop, so it's
	 * better to avoid checking attlen/attbyval in the loop.
	 *
	 * For fixed-length types, the packed array format is near-optimal. For
	 * variable-length types, an alternative layout with a separate offset
	 * array followed by contiguous data could reduce CPU pipeline stalls by
	 * making element boundaries predictable. However, the current inline
	 * varlena format is simpler and the dictionary/FSST encoding paths
	 * handle the most common high-cardinality varlena cases well.
	 */
	if ((item->t_flags & NXBT_ATTR_FORMAT_ARRAY_DECOMPOSED) != 0)
	{
		/*
		 * Array-decomposed data: elements from PostgreSQL arrays are stored
		 * contiguously.  Reconstruct ArrayType datums from the packed
		 * element stream.  NULLs are encoded in the decomposed header
		 * (NULL arrays have elem_count=0, element NULLs in a bitmap).
		 */
		int			data_size = pend - p;
		int			buf_needed;

		/*
		 * Conservative estimate for attr_buf: each element may need a
		 * varlena header plus its data, and each array needs ArrayType
		 * overhead.  The decomposed data size plus generous per-element
		 * overhead covers worst-case reconstruction.
		 */
		buf_needed = data_size * 2 + nelements * 64;
		if (scan->attr_buf_size < buf_needed)
		{
			if (scan->attr_buf)
				pfree(scan->attr_buf);
			scan->attr_buf = MemoryContextAlloc(scan->context, buf_needed);
			scan->attr_buf_size = buf_needed;
		}

		nx_array_decomposed_decode(scan->attdesc, p, data_size,
								   scan->array_datums, scan->array_isnulls,
								   nelements, scan->attr_buf, buf_needed);
	}
	else if ((item->t_flags & NXBT_ATTR_FORMAT_DICT) != 0)
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

		nx_dict_decode(scan->attdesc, p, data_size,
					   scan->array_datums, scan->array_isnulls,
					   nelements, scan->attr_buf, buf_needed);
	}
	else if ((item->t_flags & NXBT_ATTR_FORMAT_UUID_V7_DELTA) != 0)
	{
		/*
		 * UUID v7 delta-encoded storage: timestamps are delta-encoded
		 * and suffixes stored verbatim.
		 */
		fetch_att_array_uuid_v7_delta(p, pend - p,
									  has_inline_bitmap,
									  nelements, scan);
	}
	else if ((item->t_flags & NXBT_ATTR_FORMAT_FIXED_BIN) != 0)
	{
		/*
		 * Fixed-binary storage (e.g. UUID stored as 16 raw bytes).
		 * Reconstruct pass-by-ref Datum values from packed binary data.
		 */
		fetch_att_array_fixed_bin(p, pend - p,
								 has_inline_bitmap,
								 nelements, scan);
	}
	else if ((item->t_flags & NXBT_ATTR_FORMAT_DELTA_OF_DELTA) != 0)
	{
		/*
		 * Delta-of-delta encoded timestamps: reconstruct values from
		 * second-order deltas stored as zigzag-encoded bit-packed values.
		 */
		fetch_att_array_dod(p, pend - p,
							has_inline_bitmap,
							nelements,
							scan);
	}
	else if ((item->t_flags & NXBT_ATTR_FORMAT_CHIMP) != 0)
	{
		/*
		 * Chimp XOR-compressed float data: decode float4 or float8 values
		 * from the packed bit stream.
		 */
		fetch_att_array_chimp(p, pend - p,
							  has_inline_bitmap,
							  nelements,
							  scan);
	}
	else if ((item->t_flags & NXBT_ATTR_FORMAT_FOR) != 0)
	{
		fetch_att_array_for(p, pend - p,
							has_inline_bitmap,
							nelements,
							scan);
	}
	else if ((item->t_flags & NXBT_ATTR_BITPACKED) != 0)
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

	/*
	 * Dequantize bfloat16-quantized vector datums back to float32.
	 * This runs after datum extraction so varlena datums are in array_datums.
	 */
	if ((item->t_flags & NXBT_ATTR_VECTOR_QUANTIZED_F16) != 0)
	{
		Datum	   *dq_datums = scan->array_datums;
		bool	   *dq_nulls = scan->array_isnulls;

		for (int i = 0; i < nelements; i++)
		{
			if (!dq_nulls[i])
				dq_datums[i] = dequantize_vector_datum(dq_datums[i]);
		}
	}

	scan->array_num_elements = nelements;

	/*
	 * If a hint TID was set, use binary search to pre-position
	 * array_curr_idx so the caller's linear scan in nxbt_attr_fetch()
	 * can skip leading elements it doesn't care about.  We position
	 * one slot *before* the first TID >= hint, so the caller's
	 * "idx = array_curr_idx + 1" logic starts at the right place.
	 */
	if (scan->extract_hint_tid != InvalidNXTid && nelements > 1)
	{
		nxtid		hint = scan->extract_hint_tid;
		int			lo = 0;
		int			hi = nelements;

		while (lo < hi)
		{
			int			mid = lo + (hi - lo) / 2;

			if (tids[mid] < hint)
				lo = mid + 1;
			else
				hi = mid;
		}
		/* lo is now the first index where tids[lo] >= hint */
		if (lo > 0)
			scan->array_curr_idx = lo - 1;

		scan->extract_hint_tid = InvalidNXTid;
	}
}


/*
 * Subroutine of nxbt_attr_item_extract(). Unpack an array item into an array of
 * TIDs, and an array of Datums and nulls.
 *
 * This always copies the data to a working area in 'scan'. This is
 * potentially wasteful if the data is already correctly aligned, but the
 * caller relies on the working area being populated (e.g., scan->array_datums
 * and scan->array_isnulls). The decompression path may have already copied
 * the data once, making this a double-copy. Eliminating the redundant copy
 * would require tracking whether the source is already in the working area,
 * adding complexity for modest gains.
 */
static void
fetch_att_array(char *src, int srcSize, bool hasnulls,
				int numelements, uint16 item_flags,
				NXAttrTreeScan * scan)
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
			uint8		nullbits = *(uint8 *) (p++);

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

	/*
	 * The caller (nxbt_attr_item_extract) has already populated
	 * scan->array_isnulls for non-inline-bitmap cases, so we don't
	 * need to memset here.
	 */

	if (attlen > 0 && !hasnulls && attbyval)
	{
		/*
		 * The caller already zeroed array_isnulls for the no-nulls case
		 * (either via the NXBT_ATTR_NO_NULLS path or the else branch in
		 * the NULL encoding dispatch), so skip the redundant memset here.
		 */

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
		 * - srcSize: input data size with noxu 1-2 byte headers
		 * - (VARHDRSZ * 2) * numelements: extra space for header expansion and safety margin
		 * - (sizeof(int32) * 2) * numelements: worst-case alignment padding before each element
		 *
		 * Conservative calculation to handle all cases:
		 * - 1-byte native varlena headers expanding to 4-byte VARHDRSZ
		 * - 2-byte noxu headers expanding to 4-byte VARHDRSZ
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
			else if ((item_flags & NXBT_ATTR_FORMAT_NATIVE_VARLENA) != 0)
			{
				/*
				 * Native varlena format dispatch.  Short values are stored
				 * as PG 1-byte headers (zero-copy).  Long values use a
				 * 3-byte escape header (0xFE + 2B BE length).  Overflow
				 * pointers use 0xFFFF as before.
				 */
				if (p[0] == 0xFF && p[1] == 0xFF)
				{
					/* noxu overflow pointer (same format in all modes) */
					varatt_nx_overflowptr overflowptr;

					datums[i] = PointerGetDatum(bufp);
					SET_VARTAG_1B_E(&overflowptr, VARTAG_NOXU);
					memcpy(&overflowptr.nxt_block, p + 2, sizeof(BlockNumber));
					memcpy(bufp, &overflowptr, sizeof(varatt_nx_overflowptr));
					p += 2 + sizeof(BlockNumber);
					bufp += sizeof(varatt_nx_overflowptr);
				}
				else if ((unsigned char) *p == NATIVE_VARLENA_LONG_ESCAPE)
				{
					/*
					 * Long value: 3-byte header (0xFE + 2B BE data len).
					 * Reconstruct a standard PG 4-byte varlena header.
					 */
					uint16		data_len = ((unsigned char) p[1] << 8) |
										   (unsigned char) p[2];

					bufp = (char *) att_align_nominal(bufp, 'i');
					datums[i] = PointerGetDatum(bufp);

					Assert(bufp + VARHDRSZ + data_len - scan->attr_buf <= buf_needed);

					SET_VARSIZE(bufp, VARHDRSZ + data_len);
					memcpy(VARDATA(bufp), p + 3, data_len);
					p += 3 + data_len;
					bufp += VARHDRSZ + data_len;
				}
				else if ((*p & 0x01) != 0)
				{
					/*
					 * PG 1-byte short varlena.  Zero-copy: return a
					 * direct pointer into the source buffer.
					 */
					int			total_len = (unsigned char) *p >> 1;

					datums[i] = PointerGetDatum(p);
					p += total_len;
				}
				else
					elog(ERROR, "invalid native varlena header byte 0x%02x",
						 (unsigned char) *p);
			}
			else
			{
				if (*p == 0)
					elog(ERROR, "invalid zs varlen header");

				if ((*p & 0x80) == 0)
				{
					/*
					 * Original noxu 1-byte header format.  Requires a
					 * copy to reformat into PG varlena headers.
					 */
					int			this_sz = *p - 1;

					datums[i] = PointerGetDatum(bufp);

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
					 * noxu overflow pointer.
					 *
					 * Note that the noxu overflow pointer is stored unaligned.
					 * That's OK. Per postgres.h, varatts with 1-byte header
					 * don't need to aligned, and that applies to overflow
					 * pointers, too.
					 */
					varatt_nx_overflowptr overflowptr;

					datums[i] = PointerGetDatum(bufp);

					SET_VARTAG_1B_E(&overflowptr, VARTAG_NOXU);
					memcpy(&overflowptr.nxt_block, p + 2, sizeof(BlockNumber));
					memcpy(bufp, &overflowptr, sizeof(varatt_nx_overflowptr));
					p += 2 + sizeof(BlockNumber);
					bufp += sizeof(varatt_nx_overflowptr);
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
 * Decode bit-packed boolean datum data for nxbt_attr_item_extract().
 *
 * Boolean values are packed 8 per byte. Only non-NULL values are stored
 * in the bitpacked data. This gives 8x compression over the standard
 * 1-byte-per-boolean storage.
 */
static void
fetch_att_array_bitpacked(char *src, int srcSize, bool hasnulls,
						  int numelements, NXAttrTreeScan *scan)
{
	bool	   *nulls = scan->array_isnulls;
	Datum	   *datums = scan->array_datums;
	unsigned char *p = (unsigned char *) src;

	/* Decode inline NULL bitmap if present */
	if (hasnulls)
	{
		for (int i = 0; i < numelements; i += 8)
		{
			uint8		nullbits = *(uint8 *) (p++);

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
	/* else: caller already populated scan->array_isnulls */

	/*
	 * Unpack boolean values from the bitpacked format.
	 * Non-NULL booleans are packed sequentially, 8 per byte.
	 */
	{
		int			bit_idx = 0;
		uint8		cur_byte = 0;

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
 * Decode FOR-encoded datum data for nxbt_attr_item_extract().
 */
static void
fetch_att_array_for(char *src, int srcSize, bool hasnulls,
					int numelements, NXAttrTreeScan *scan)
{
	Form_pg_attribute attr = scan->attdesc;
	int			attlen = attr->attlen;
	bool	   *nulls = scan->array_isnulls;
	Datum	   *datums = scan->array_datums;
	unsigned char *p = (unsigned char *) src;
	NXForHeader forhdr;
	uint64		unpacked[MAX_TIDS_PER_ATTR_ITEM];
	int			num_nonnull;
	int			val_idx;

	if (hasnulls)
	{
		for (int i = 0; i < numelements; i += 8)
		{
			uint8		nullbits = *(uint8 *) (p++);
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
	/* else: caller already populated scan->array_isnulls */

	num_nonnull = 0;
	for (int i = 0; i < numelements; i++)
		if (!nulls[i])
			num_nonnull++;

	memcpy(&forhdr, p, sizeof(NXForHeader));
	p += sizeof(NXForHeader);

	for_unpack_values(p, unpacked, num_nonnull, forhdr.for_bits_per_value);
	p += NXBT_FOR_PACKED_SIZE(num_nonnull, forhdr.for_bits_per_value);

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
 * Decode delta-of-delta encoded datum data for nxbt_attr_item_extract().
 *
 * Reconstructs the original 8-byte integer values (typically timestamps)
 * from a NXDeltaOfDeltaHeader followed by bit-packed zigzag-encoded
 * second-order deltas.  The first two values are stored in the header
 * (initial_value and initial_value + initial_delta), and the remaining
 * values are reconstructed by integrating the delta-of-deltas twice.
 */
static void
fetch_att_array_dod(char *src, int srcSize, bool hasnulls,
					int numelements, NXAttrTreeScan *scan)
{
	bool	   *nulls = scan->array_isnulls;
	Datum	   *datums = scan->array_datums;
	unsigned char *p = (unsigned char *) src;
	NXDeltaOfDeltaHeader dodhdr;
	int64		dod_unpacked[MAX_TIDS_PER_ATTR_ITEM];
	int			num_nonnull;
	int			ndod;
	int			val_idx;
	int64		running_val;
	int64		running_delta;

	if (hasnulls)
	{
		for (int i = 0; i < numelements; i += 8)
		{
			uint8		nullbits = *(uint8 *) (p++);

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
	/* else: caller already populated scan->array_isnulls */

	num_nonnull = 0;
	for (int i = 0; i < numelements; i++)
		if (!nulls[i])
			num_nonnull++;

	memcpy(&dodhdr, p, sizeof(NXDeltaOfDeltaHeader));
	p += sizeof(NXDeltaOfDeltaHeader);

	/* Unpack the zigzag-encoded delta-of-deltas */
	ndod = num_nonnull - 2;
	if (ndod > 0)
	{
		dod_unpack_values(p, dod_unpacked, ndod, dodhdr.dod_bits_per_value);
		p += NXBT_DOD_PACKED_SIZE(ndod, dodhdr.dod_bits_per_value);
	}

	/*
	 * Reconstruct original values by integrating delta-of-deltas twice:
	 *   val[0] = initial_value
	 *   val[1] = initial_value + initial_delta
	 *   delta[i] = delta[i-1] + dod[i-2]
	 *   val[i] = val[i-1] + delta[i]
	 */
	running_val = (int64) dodhdr.dod_initial_value;
	running_delta = dodhdr.dod_initial_delta;
	val_idx = 0;
	for (int i = 0; i < numelements; i++)
	{
		if (nulls[i])
		{
			datums[i] = (Datum) 0;
		}
		else
		{
			if (val_idx == 0)
			{
				/* First value: stored directly in header */
				datums[i] = Int64GetDatum(running_val);
			}
			else if (val_idx == 1)
			{
				/* Second value: initial_value + initial_delta */
				running_val += running_delta;
				datums[i] = Int64GetDatum(running_val);
			}
			else
			{
				/* Subsequent: integrate delta-of-delta */
				running_delta += dod_unpacked[val_idx - 2];
				running_val += running_delta;
				datums[i] = Int64GetDatum(running_val);
			}
			val_idx++;
		}
	}
	Assert(val_idx == num_nonnull);
	if ((int)(p - (unsigned char *) src) != srcSize)
		elog(ERROR, "corrupt delta-of-delta item: consumed %d of %d bytes",
			 (int)(p - (unsigned char *) src), srcSize);
}

/*
 * Decode Chimp XOR-compressed float datum data for nxbt_attr_item_extract().
 *
 * Chimp stores floats via XOR encoding: the first value is stored verbatim,
 * then each subsequent value is XORed with its predecessor and the result
 * is variable-length encoded using leading-zero buckets.  This achieves
 * excellent compression for time-series float data with gradual changes.
 *
 * The encoded payload starts with a ChimpBlockHeader that identifies
 * whether the data is float4 or float8, followed by the packed bit stream.
 */
static void
fetch_att_array_chimp(char *src, int srcSize, bool hasnulls,
					  int numelements, NXAttrTreeScan *scan)
{
	bool	   *nulls = scan->array_isnulls;
	Datum	   *datums = scan->array_datums;
	unsigned char *p = (unsigned char *) src;
	ChimpBlockHeader hdr;

	/* Decode NULL bitmap if present */
	if (hasnulls)
	{
		for (int i = 0; i < numelements; i += 8)
		{
			uint8		nullbits = *(uint8 *) (p++);

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
	/* else: caller already populated scan->array_isnulls */

	/*
	 * Peek at the header to determine float width, then dispatch to the
	 * appropriate decoder.  The chimp_decode/chimp_decode_float8 functions
	 * read the full header internally.
	 */
	if (srcSize - (int)((char *) p - src) < (int) sizeof(ChimpBlockHeader))
		elog(ERROR, "corrupt Chimp item: data too short");

	memcpy(&hdr, p, sizeof(ChimpBlockHeader));

	if (hdr.chimp_value_width == 8)
		chimp_decode_float8((const char *) p,
							srcSize - (int)((char *) p - src),
							datums, nulls, numelements);
	else
		chimp_decode((const char *) p,
					 srcSize - (int)((char *) p - src),
					 datums, nulls, numelements);
}

/*
 * Decode fixed-binary encoded datum data for nxbt_attr_item_extract().
 *
 * Used for types like UUID where we store raw fixed-size binary data
 * without varlena headers. The data is stored as tightly packed binary
 * values (e.g., 16 bytes per UUID) with NULLs skipped.
 */
static void
fetch_att_array_fixed_bin(char *src, int srcSize, bool hasnulls,
						  int numelements, NXAttrTreeScan *scan)
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
			uint8		nullbits = *(uint8 *) (p++);

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
	/* else: caller already populated scan->array_isnulls */

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
 * Subroutine of nxbt_attr_item_extract(). Unpack a UUID v7 delta-encoded
 * array item into an array of Datums.
 *
 * The encoded format is:
 *   [NXUUIDDeltaHeader] [packed deltas] [suffixes: 10 bytes each]
 *
 * Each non-null UUID is reconstructed from the base timestamp + delta
 * and the stored suffix bytes.
 */
static void
fetch_att_array_uuid_v7_delta(char *src, int srcSize,
							  bool hasnulls, int numelements,
							  NXAttrTreeScan *scan)
{
	bool	   *nulls = scan->array_isnulls;
	Datum	   *datums = scan->array_datums;
	unsigned char *p = (unsigned char *) src;
	int			buf_needed;

	/* Handle NULL bitmap if present */
	if (hasnulls)
	{
		for (int i = 0; i < numelements; i += 8)
		{
			uint8		nullbits = *(uint8 *) (p++);

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
	/* else: caller already populated scan->array_isnulls */

	/*
	 * Allocate buffer for reconstructed UUID values.
	 * Each UUID is 16 bytes (sizeof(pg_uuid_t)).
	 */
	buf_needed = numelements * sizeof(pg_uuid_t);
	if (scan->attr_buf_size < buf_needed)
	{
		if (scan->attr_buf)
			pfree(scan->attr_buf);
		scan->attr_buf = MemoryContextAlloc(scan->context, buf_needed);
		scan->attr_buf_size = buf_needed;
	}

	uuid_decompress_delta((const char *) p,
						  (int) ((unsigned char *) src + srcSize - p),
						  datums, nulls, numelements,
						  scan->attr_buf, buf_needed);
}

/*
 * Routines to split, merge, and recompress items.
 */

static NXExplodedItem *
nxbt_attr_explode_item(Form_pg_attribute att, NXAttributeArrayItem * item,
					   Relation rel, AttrNumber attno)
{
	NXExplodedItem *eitem;
	int			tidno;
	nxtid		currtid;
	nxtid	   *tids;
	char	   *databuf;
	char	   *p;
	char	   *pend;
	uint64	   *codewords;

	eitem = palloc(sizeof(NXExplodedItem));
	eitem->t_size = 0;
	/* Preserve all format flags so datum data can be navigated */
	eitem->t_flags = item->t_flags & NXBT_ATTR_FORMAT_MASK;
	eitem->t_num_elements = item->t_num_elements;

	if ((item->t_flags & NXBT_ATTR_COMPRESSED) != 0)
	{
		NXAttributeCompressedItem *citem = (NXAttributeCompressedItem *) item;
		int			payloadsz;

		payloadsz = citem->t_uncompressed_size;
		Assert(payloadsz > 0);

		databuf = palloc(payloadsz);

		if ((item->t_flags & NXBT_ATTR_SHARED_DICT) != 0)
		{
			NXSharedDictData *dict = nx_shared_dict_load(rel, attno);

			if (dict != NULL)
				nx_decompress_with_shared_dict(citem->t_payload, databuf,
											   citem->t_size - offsetof(NXAttributeCompressedItem, t_payload),
											   payloadsz, dict);
			else
				elog(ERROR, "shared dictionary required for decompression but not found "
					 "for attribute %d", attno);
		}
		else if ((item->t_flags & NXBT_ATTR_FORMAT_FSST) != 0)
			nx_decompress_with_fsst(citem->t_payload, databuf,
									citem->t_size - offsetof(NXAttributeCompressedItem, t_payload),
									payloadsz, NULL);
		else
			nx_decompress(citem->t_payload, databuf,
						  citem->t_size - offsetof(NXAttributeCompressedItem, t_payload),
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
	tids = eitem->tids = palloc(item->t_num_elements * sizeof(nxtid));
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
	if ((item->t_flags & NXBT_ATTR_SPARSE_NULLS) != 0)
	{
		int		bytes_consumed;
		eitem->nullbitmap = decode_nulls_to_bitmap((unsigned char *) p,
												   item->t_num_elements,
												   NXBT_ATTR_SPARSE_NULLS,
												   &bytes_consumed);
		p += bytes_consumed;
	}
	else if ((item->t_flags & NXBT_ATTR_RLE_NULLS) != 0)
	{
		int		bytes_consumed;
		eitem->nullbitmap = decode_nulls_to_bitmap((unsigned char *) p,
												   item->t_num_elements,
												   NXBT_ATTR_RLE_NULLS,
												   &bytes_consumed);
		p += bytes_consumed;
	}
	else if ((item->t_flags & NXBT_ATTR_NO_NULLS) != 0)
	{
		eitem->nullbitmap = palloc0(NXBT_ATTR_BITMAPLEN(item->t_num_elements));
	}
	else if ((item->t_flags & NXBT_HAS_NULLS) != 0)
	{
		eitem->nullbitmap = (uint8 *) p;
		p += NXBT_ATTR_BITMAPLEN(item->t_num_elements);
	}
	else
	{
		eitem->nullbitmap = palloc0(NXBT_ATTR_BITMAPLEN(item->t_num_elements));
	}

	/* Bitpacked booleans: expand to 1-byte-per-value raw format */
	if ((item->t_flags & NXBT_ATTR_BITPACKED) != 0)
	{
		int		nonnull_count = 0;
		int		bit_idx = 0;
		uint8	cur_byte = 0;
		char   *rawbuf;
		char   *wp;

		for (int i = 0; i < item->t_num_elements; i++)
			if (!nxbt_attr_item_isnull(eitem->nullbitmap, i))
				nonnull_count++;

		rawbuf = palloc(nonnull_count);
		wp = rawbuf;
		for (int i = 0; i < item->t_num_elements; i++)
		{
			if (nxbt_attr_item_isnull(eitem->nullbitmap, i))
				continue;
			if (bit_idx % 8 == 0)
				cur_byte = *(unsigned char *) p++;
			*wp++ = (cur_byte >> (bit_idx % 8)) & 1;
			bit_idx++;
		}

		eitem->datumdata = rawbuf;
		eitem->datumdatasz = nonnull_count;
		/* Clear bitpacked flag: data is now raw 1-byte-per-bool format */
		eitem->t_flags &= ~NXBT_ATTR_BITPACKED;
		return eitem;
	}

	/*
	 * UUID v7 delta-encoded data: decode back to raw fixed-binary format
	 * so that downstream code can navigate datums with
	 * nxbt_attr_datasize_ex().
	 */
	if ((item->t_flags & NXBT_ATTR_FORMAT_UUID_V7_DELTA) != 0)
	{
		int			data_size = pend - p;
		int			nonnull_count = 0;
		int			raw_data_size;
		char	   *rawbuf;
		char	   *wp;
		Datum	   *datums_tmp;
		bool	   *isnulls_tmp;

		for (int i = 0; i < item->t_num_elements; i++)
			if (!nxbt_attr_item_isnull(eitem->nullbitmap, i))
				nonnull_count++;

		raw_data_size = nonnull_count * UUID_LEN;

		/* Allocate temporary arrays for decoding */
		datums_tmp = palloc(item->t_num_elements * sizeof(Datum));
		isnulls_tmp = palloc(item->t_num_elements * sizeof(bool));
		rawbuf = palloc(nonnull_count * sizeof(pg_uuid_t));

		/* Populate isnulls from the bitmap */
		for (int i = 0; i < item->t_num_elements; i++)
			isnulls_tmp[i] = nxbt_attr_item_isnull(eitem->nullbitmap, i);

		uuid_decompress_delta(p, data_size, datums_tmp, isnulls_tmp,
							  item->t_num_elements, rawbuf,
							  nonnull_count * sizeof(pg_uuid_t));

		/*
		 * Re-encode non-null UUIDs into raw fixed-binary format for the
		 * exploded item.
		 */
		{
			char   *raw_data = palloc(raw_data_size);

			wp = raw_data;
			for (int i = 0; i < item->t_num_elements; i++)
			{
				if (!isnulls_tmp[i])
				{
					memcpy(wp, DatumGetPointer(datums_tmp[i]), UUID_LEN);
					wp += UUID_LEN;
				}
			}

			eitem->datumdata = raw_data;
			eitem->datumdatasz = raw_data_size;
		}

		pfree(datums_tmp);
		pfree(isnulls_tmp);
		pfree(rawbuf);
		/* Clear UUID v7 delta flag: data is now raw fixed-binary format */
		eitem->t_flags &= ~NXBT_ATTR_FORMAT_UUID_V7_DELTA;
		return eitem;
	}

	/*
	 * Dictionary-encoded data: decode back to raw varlena/fixed-length
	 * format so that downstream code can navigate datums with
	 * nxbt_attr_datasize_ex().
	 */
	if ((item->t_flags & NXBT_ATTR_FORMAT_DICT) != 0)
	{
		int			data_size = pend - p;
		Datum	   *datums;
		bool	   *isnulls;
		int			consumed;
		int			nonnull_count = 0;
		int			raw_data_size;
		int			buf_size;
		char	   *rawbuf;
		char	   *wp;

		/* Allocate temporary arrays for decoding */
		buf_size = data_size + item->t_num_elements * (VARHDRSZ + 4);
		datums = palloc(item->t_num_elements * sizeof(Datum));
		isnulls = palloc(item->t_num_elements * sizeof(bool));
		rawbuf = palloc(buf_size);

		consumed = nx_dict_decode(att, p, data_size,
								  datums, isnulls,
								  item->t_num_elements,
								  rawbuf, buf_size);
		(void) consumed;

		/* Rebuild the NULL bitmap from dictionary-decoded isnulls */
		pfree(eitem->nullbitmap);
		eitem->nullbitmap = palloc0(NXBT_ATTR_BITMAPLEN(item->t_num_elements));
		for (int i = 0; i < item->t_num_elements; i++)
		{
			if (isnulls[i])
				nxbt_attr_item_setnull(eitem->nullbitmap, i);
			else
				nonnull_count++;
		}

		/*
		 * Re-encode non-null values into raw noxu varlena format so the
		 * exploded item can be navigated by nxbt_attr_datasize_ex().
		 */
		raw_data_size = 0;
		if (att->attlen > 0)
		{
			raw_data_size = nonnull_count * att->attlen;
		}
		else
		{
			for (int i = 0; i < item->t_num_elements; i++)
			{
				if (!isnulls[i])
				{
					if (att->attlen == -1)
					{
						int		data_len = (int) VARSIZE_ANY_EXHDR(DatumGetPointer(datums[i]));

						if ((data_len + 1) > 0x7F)
							raw_data_size += 2 + data_len;
						else
							raw_data_size += 1 + data_len;
					}
					else
					{
						/* cstring */
						int		slen = (int) strlen(DatumGetCString(datums[i]));

						if ((slen + 1) > 0x7F)
							raw_data_size += 2 + slen;
						else
							raw_data_size += 1 + slen;
					}
				}
			}
		}

		{
			char	   *out = palloc(raw_data_size);

			wp = out;
			for (int i = 0; i < item->t_num_elements; i++)
			{
				if (isnulls[i])
					continue;

				if (att->attlen > 0 && att->attbyval)
				{
					store_att_byval(wp, datums[i], att->attlen);
					wp += att->attlen;
				}
				else if (att->attlen > 0)
				{
					memcpy(wp, DatumGetPointer(datums[i]), att->attlen);
					wp += att->attlen;
				}
				else if (att->attlen == -1)
				{
					int		data_len = (int) VARSIZE_ANY_EXHDR(DatumGetPointer(datums[i]));
					char   *src_data = VARDATA_ANY(DatumGetPointer(datums[i]));

					if ((data_len + 1) > 0x7F)
					{
						*(wp++) = 0x80 | ((data_len + 1) >> 8);
						*(wp++) = (data_len + 1) & 0xFF;
					}
					else
					{
						*(wp++) = (data_len + 1);
					}
					memcpy(wp, src_data, data_len);
					wp += data_len;
				}
				else
				{
					/* cstring (attlen == -2) */
					int		slen = (int) strlen(DatumGetCString(datums[i]));

					if ((slen + 1) > 0x7F)
					{
						*(wp++) = 0x80 | ((slen + 1) >> 8);
						*(wp++) = (slen + 1) & 0xFF;
					}
					else
					{
						*(wp++) = (slen + 1);
					}
					memcpy(wp, DatumGetCString(datums[i]), slen);
					wp += slen;
				}
			}

			eitem->datumdata = out;
			eitem->datumdatasz = wp - out;
		}

		pfree(datums);
		pfree(isnulls);
		pfree(rawbuf);
		/* Clear dict flag: data is now raw varlena/fixed format */
		eitem->t_flags &= ~NXBT_ATTR_FORMAT_DICT;
		return eitem;
	}

	/*
	 * Array-decomposed data: decode back to raw varlena format (standard
	 * PostgreSQL ArrayType blobs) so downstream code can navigate datums
	 * with nxbt_attr_datasize_ex().
	 */
	if ((item->t_flags & NXBT_ATTR_FORMAT_ARRAY_DECOMPOSED) != 0)
	{
		int			data_size = pend - p;
		Datum	   *datums;
		bool	   *isnulls;
		int			nonnull_count = 0;
		int			raw_data_size;
		int			buf_size;
		char	   *rawbuf;
		char	   *wp;

		/* Allocate temporary arrays for decoding */
		buf_size = data_size * 2 + item->t_num_elements * 64;
		datums = palloc(item->t_num_elements * sizeof(Datum));
		isnulls = palloc(item->t_num_elements * sizeof(bool));
		rawbuf = palloc(buf_size);

		nx_array_decomposed_decode(att, p, data_size,
								   datums, isnulls,
								   item->t_num_elements,
								   rawbuf, buf_size);

		/* Rebuild the NULL bitmap from decoded isnulls */
		pfree(eitem->nullbitmap);
		eitem->nullbitmap = palloc0(NXBT_ATTR_BITMAPLEN(item->t_num_elements));
		for (int i = 0; i < item->t_num_elements; i++)
		{
			if (isnulls[i])
				nxbt_attr_item_setnull(eitem->nullbitmap, i);
			else
				nonnull_count++;
		}

		/*
		 * Re-encode non-null ArrayType values into noxu varlena format.
		 * Arrays are always varlena (attlen == -1).
		 */
		raw_data_size = 0;
		for (int i = 0; i < item->t_num_elements; i++)
		{
			if (!isnulls[i])
			{
				int		data_len = (int) VARSIZE_ANY_EXHDR(DatumGetPointer(datums[i]));

				if ((data_len + 1) > 0x7F)
					raw_data_size += 2 + data_len;
				else
					raw_data_size += 1 + data_len;
			}
		}

		{
			char	   *out = palloc(raw_data_size);

			wp = out;
			for (int i = 0; i < item->t_num_elements; i++)
			{
				if (isnulls[i])
					continue;

				{
					int		data_len = (int) VARSIZE_ANY_EXHDR(DatumGetPointer(datums[i]));
					char   *src_data = VARDATA_ANY(DatumGetPointer(datums[i]));

					if ((data_len + 1) > 0x7F)
					{
						*(wp++) = 0x80 | ((data_len + 1) >> 8);
						*(wp++) = (data_len + 1) & 0xFF;
					}
					else
					{
						*(wp++) = (data_len + 1);
					}
					memcpy(wp, src_data, data_len);
					wp += data_len;
				}
			}

			eitem->datumdata = out;
			eitem->datumdatasz = wp - out;
		}

		pfree(datums);
		pfree(isnulls);
		pfree(rawbuf);

		/*
		 * Clear the ARRAY_DECOMPOSED flag: the exploded item now contains
		 * raw varlena ArrayType blobs in noxu compact format, not decomposed
		 * element data.  If we leave the flag set, nxbt_combine_items will
		 * produce a packed item whose datum data is raw varlena but flagged
		 * as array-decomposed, causing the decode path to misinterpret the
		 * data as an NXArrayDecomposedHeader and crash.
		 *
		 * Also clear NATIVE_VARLENA: the re-encoded data uses noxu compact
		 * varlena format, not PG native 1-byte headers.  If the original
		 * item had both flags, leaving NATIVE_VARLENA would cause the reader
		 * to misinterpret the noxu compact headers.
		 */
		eitem->t_flags &= ~(NXBT_ATTR_FORMAT_ARRAY_DECOMPOSED |
							NXBT_ATTR_FORMAT_NATIVE_VARLENA);

		return eitem;
	}

	/* datum data -- decode Chimp back to raw fixed-width floats if needed */
	if ((item->t_flags & NXBT_ATTR_FORMAT_CHIMP) != 0)
	{
		int			data_size = pend - p;
		int			nonnull_count = 0;
		int			attlen;
		char	   *rawbuf;
		char	   *wp;
		Datum	   *tmp_datums;
		bool	   *tmp_isnulls;
		ChimpBlockHeader chimp_hdr;

		for (int i = 0; i < item->t_num_elements; i++)
			if (!nxbt_attr_item_isnull(eitem->nullbitmap, i))
				nonnull_count++;

		/* Peek at the header to determine float width */
		memcpy(&chimp_hdr, p, sizeof(ChimpBlockHeader));
		attlen = chimp_hdr.chimp_value_width;	/* 4 or 8 */

		/* Decode into temporary datum/isnull arrays */
		tmp_datums = palloc(item->t_num_elements * sizeof(Datum));
		tmp_isnulls = palloc(item->t_num_elements * sizeof(bool));
		for (int i = 0; i < item->t_num_elements; i++)
			tmp_isnulls[i] = nxbt_attr_item_isnull(eitem->nullbitmap, i);

		if (attlen == 8)
			chimp_decode_float8(p, data_size, tmp_datums, tmp_isnulls,
								item->t_num_elements);
		else
			chimp_decode(p, data_size, tmp_datums, tmp_isnulls,
						 item->t_num_elements);

		/* Re-encode non-null values as raw fixed-width bytes */
		rawbuf = palloc(nonnull_count * attlen);
		wp = rawbuf;
		for (int i = 0; i < item->t_num_elements; i++)
		{
			if (!tmp_isnulls[i])
			{
				if (attlen == 8)
				{
					float8		fval = DatumGetFloat8(tmp_datums[i]);

					memcpy(wp, &fval, 8);
				}
				else
				{
					float4		fval = DatumGetFloat4(tmp_datums[i]);

					memcpy(wp, &fval, 4);
				}
				wp += attlen;
			}
		}

		eitem->datumdata = rawbuf;
		eitem->datumdatasz = nonnull_count * attlen;
		/* Clear the chimp flag so the exploded item uses raw format */
		eitem->t_flags &= ~NXBT_ATTR_FORMAT_CHIMP;

		pfree(tmp_datums);
		pfree(tmp_isnulls);
		return eitem;
	}

	/* datum data -- decode FOR back to raw format if needed */
	if ((item->t_flags & NXBT_ATTR_FORMAT_FOR) != 0)
	{
		NXForHeader forhdr;
		uint64		unpacked_vals[MAX_TIDS_PER_ATTR_ITEM];
		int			nonnull_count = 0;
		int			for_attlen;
		char	   *rawbuf;
		char	   *wp;

		for (int i = 0; i < item->t_num_elements; i++)
			if (!nxbt_attr_item_isnull(eitem->nullbitmap, i))
				nonnull_count++;

		memcpy(&forhdr, p, sizeof(NXForHeader));
		p += sizeof(NXForHeader);
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
		/* Clear FOR flag: data is now raw fixed-width format */
		eitem->t_flags &= ~NXBT_ATTR_FORMAT_FOR;
	}
	else if ((item->t_flags & NXBT_ATTR_FORMAT_DELTA_OF_DELTA) != 0)
	{
		/*
		 * Decode delta-of-delta back to raw 8-byte values.  This reverses
		 * the second-order delta encoding so the exploded item contains
		 * plain int64 values suitable for re-encoding during recompression.
		 */
		NXDeltaOfDeltaHeader dodhdr;
		int64		dod_vals[MAX_TIDS_PER_ATTR_ITEM];
		int			nonnull_count = 0;
		int			ndod;
		char	   *rawbuf;
		char	   *wp;
		int64		running_val;
		int64		running_delta;

		for (int i = 0; i < item->t_num_elements; i++)
			if (!nxbt_attr_item_isnull(eitem->nullbitmap, i))
				nonnull_count++;

		memcpy(&dodhdr, p, sizeof(NXDeltaOfDeltaHeader));
		p += sizeof(NXDeltaOfDeltaHeader);

		ndod = nonnull_count - 2;
		if (ndod > 0)
			dod_unpack_values((unsigned char *) p, dod_vals, ndod,
							  dodhdr.dod_bits_per_value);

		/* Reconstruct raw int64 values */
		rawbuf = palloc(nonnull_count * sizeof(int64));
		wp = rawbuf;
		running_val = (int64) dodhdr.dod_initial_value;
		running_delta = dodhdr.dod_initial_delta;

		for (int i = 0; i < nonnull_count; i++)
		{
			int64		val;

			if (i == 0)
				val = running_val;
			else if (i == 1)
			{
				running_val += running_delta;
				val = running_val;
			}
			else
			{
				running_delta += dod_vals[i - 2];
				running_val += running_delta;
				val = running_val;
			}
			memcpy(wp, &val, sizeof(int64));
			wp += sizeof(int64);
		}
		eitem->datumdata = rawbuf;
		eitem->datumdatasz = nonnull_count * sizeof(int64);
		/* Clear DOD flag: data is now raw int64 values */
		eitem->t_flags &= ~NXBT_ATTR_FORMAT_DELTA_OF_DELTA;
	}
	else
	{
		eitem->datumdata = p;
		eitem->datumdatasz = pend - p;
	}

	/*
	 * Free databuf if datumdata points to a different allocation.
	 * For dict/FOR/bitpacked paths, datumdata points to a new buffer
	 * (rawbuf/out) and databuf is orphaned. For normal compressed path,
	 * datumdata points into databuf so we can't free it.
	 */
	/*
	 * DISABLED: Freeing databuf causes double-free errors because nullbitmap
	 * may also point into databuf. The check below only verified datumdata.
	 * The small memory leak will be cleaned up at transaction end.
	 *
	 * if (databuf != NULL)
	 * {
	 *     NXAttributeCompressedItem *citem = (NXAttributeCompressedItem *) item;
	 *     char *databuf_end = databuf + citem->t_uncompressed_size;
	 *     if (eitem->datumdata < databuf || eitem->datumdata >= databuf_end)
	 *         pfree(databuf);
	 * }
	 */
	(void) databuf;  /* unused */

	return eitem;
}

/*
 * Compute (or estimate) how much space an array item takes when uncompressed.
 *
 * For exploded items we trial-encode the TID deltas to get the exact
 * codeword count rather than using the optimistic 240-per-codeword
 * estimate which underestimates size for sparse TID patterns.
 */
static int
nxbt_item_uncompressed_size(NXAttributeArrayItem * item)
{
	if (item->t_size == 0)
	{
		NXExplodedItem *eitem = (NXExplodedItem *) item;
		size_t		sz = 0;
		int			num_codewords;
		int			total_encoded;
		int			num_elements = eitem->t_num_elements;

		/*
		 * Trial-encode TID deltas to get the exact codeword count.
		 * This is the same loop used in nxbt_pack_item(), but we only
		 * need the count, not the actual codewords.
		 */
		if (num_elements > 0)
		{
			uint64		deltas[MAX_TIDS_PER_ATTR_ITEM];
			nxtid		prevtid = eitem->tids[0];

			deltas[0] = 0;
			for (int i = 1; i < num_elements; i++)
			{
				deltas[i] = eitem->tids[i] - prevtid;
				prevtid = eitem->tids[i];
			}

			num_codewords = 0;
			total_encoded = 0;
			while (total_encoded < num_elements)
			{
				int		num_encoded;

				(void) simple8b_encode(&deltas[total_encoded],
									   num_elements - total_encoded,
									   &num_encoded);
				total_encoded += num_encoded;
				num_codewords++;
			}
		}
		else
			num_codewords = 0;

		sz += offsetof(NXAttributeArrayItem, t_tid_codewords)
			+ num_codewords * sizeof(uint64);

		/* Add null bitmap size */
		sz += NXBT_ATTR_BITMAPLEN(num_elements);

		/* Add datum data */
		sz += eitem->datumdatasz;

		return sz;
	}
	else if (item->t_flags & NXBT_ATTR_COMPRESSED)
	{
		NXAttributeCompressedItem *citem = (NXAttributeCompressedItem *) item;

		/*
		 * t_uncompressed_size is the size of the payload (codewords +
		 * null bitmap + datum data).  The full uncompressed item size
		 * uses the NXAttributeArrayItem header, not the larger
		 * NXAttributeCompressedItem header.
		 */
		return offsetof(NXAttributeArrayItem, t_tid_codewords) + citem->t_uncompressed_size;
	}
	else
		return item->t_size;
}

void
nxbt_split_item(Form_pg_attribute attr, NXExplodedItem * origitem, nxtid first_right_tid,
				NXExplodedItem * *leftitem_p, NXExplodedItem * *rightitem_p,
				Relation rel, AttrNumber attno)
{
	int			i;
	int			left_num_elements;
	int			left_datasz;
	int			right_num_elements;
	int			right_datasz;
	char	   *p;
	NXExplodedItem *leftitem;
	NXExplodedItem *rightitem;

	if (origitem->t_size != 0)
		origitem = nxbt_attr_explode_item(attr, (NXAttributeArrayItem *) origitem, rel, attno);

	p = origitem->datumdata;
	for (i = 0; i < origitem->t_num_elements; i++)
	{
		if (origitem->tids[i] >= first_right_tid)
			break;

		if (!nxbt_attr_item_isnull(origitem->nullbitmap, i))
			p += nxbt_attr_datasize_ex(attr->attlen, p, origitem->t_flags);
	}
	left_num_elements = i;
	left_datasz = p - origitem->datumdata;

	right_num_elements = origitem->t_num_elements - left_num_elements;
	right_datasz = origitem->datumdatasz - left_datasz;

	if (left_num_elements == origitem->t_num_elements)
		elog(ERROR, "item split failed");

	leftitem = palloc(sizeof(NXExplodedItem));
	leftitem->t_size = 0;
	leftitem->t_flags = origitem->t_flags & NXBT_ATTR_FORMAT_MASK;
	leftitem->t_num_elements = left_num_elements;
	leftitem->tids = palloc(left_num_elements * sizeof(nxtid));
	leftitem->nullbitmap = palloc0(NXBT_ATTR_BITMAPLEN(left_num_elements));
	leftitem->datumdata = palloc(left_datasz);
	leftitem->datumdatasz = left_datasz;

	memcpy(leftitem->tids, &origitem->tids[0], left_num_elements * sizeof(nxtid));
	copy_null_bitmap(leftitem->nullbitmap, origitem->nullbitmap,
					 0, left_num_elements);
	memcpy(leftitem->datumdata, &origitem->datumdata[0], left_datasz);

	rightitem = palloc(sizeof(NXExplodedItem));
	rightitem->t_size = 0;
	rightitem->t_flags = origitem->t_flags & NXBT_ATTR_FORMAT_MASK;
	rightitem->t_num_elements = right_num_elements;
	rightitem->tids = palloc(right_num_elements * sizeof(nxtid));
	rightitem->nullbitmap = palloc0(NXBT_ATTR_BITMAPLEN(right_num_elements));
	rightitem->datumdata = palloc(right_datasz);
	rightitem->datumdatasz = right_datasz;

	memcpy(rightitem->tids, &origitem->tids[left_num_elements], right_num_elements * sizeof(nxtid));
	copy_null_bitmap(rightitem->nullbitmap, origitem->nullbitmap,
					 left_num_elements, right_num_elements);
	memcpy(rightitem->datumdata, &origitem->datumdata[left_datasz], right_datasz);

	*leftitem_p = leftitem;
	*rightitem_p = rightitem;
}

static NXExplodedItem *
nxbt_combine_items(Form_pg_attribute att, List *items, int start, int end,
				   Relation rel, AttrNumber attno)
{
	NXExplodedItem *newitem;
	int			total_elements;
	int			total_datumdatasz;
	List	   *exploded_items = NIL;

	total_elements = 0;
	total_datumdatasz = 0;
	{
		uint32		common_flags = NXBT_ATTR_FORMAT_MASK;

		for (int i = start; i < end; i++)
		{
			ListCell   *lc = list_nth_cell(items, i);
			NXAttributeArrayItem *item = lfirst(lc);
			NXExplodedItem *eitem;

			if (item->t_size != 0)
			{
				eitem = nxbt_attr_explode_item(att, item, rel, attno);
				lfirst(lc) = eitem;
			}
			else
				eitem = (NXExplodedItem *) item;

			elog(DEBUG1, "nxbt_combine_items: item %d has t_flags=0x%04X, common_flags before AND=0x%04X",
				 i - start, eitem->t_flags, common_flags);
			common_flags &= eitem->t_flags;
			elog(DEBUG1, "nxbt_combine_items: common_flags after AND=0x%04X", common_flags);

			exploded_items = lappend(exploded_items, eitem);

			total_elements += eitem->t_num_elements;
			total_datumdatasz += eitem->datumdatasz;
		}
		Assert((size_t) total_elements <= MAX_TIDS_PER_ATTR_ITEM);

		newitem = palloc(sizeof(NXExplodedItem));
		newitem->t_size = 0;
		/* Preserve format flags only if all combined items share them */
		newitem->t_flags = common_flags & NXBT_ATTR_FORMAT_MASK;
	}
	newitem->t_num_elements = total_elements;

	newitem->tids = palloc(total_elements * sizeof(nxtid));
	/* Allocate 1 extra byte for or_null_bitmap's unaligned write spill */
	newitem->nullbitmap = palloc0(NXBT_ATTR_BITMAPLEN(total_elements) + 1);
	newitem->datumdata = palloc(total_datumdatasz);
	newitem->datumdatasz = total_datumdatasz;

	{
		char	   *p = newitem->datumdata;
		int			elemno = 0;

		for (int i = start; i < end; i++)
		{
			NXExplodedItem *eitem = list_nth(items, i);

			memcpy(&newitem->tids[elemno], eitem->tids, eitem->t_num_elements * sizeof(nxtid));

			or_null_bitmap(newitem->nullbitmap, eitem->nullbitmap,
						   elemno, eitem->t_num_elements);

			memcpy(p, eitem->datumdata, eitem->datumdatasz);
			p += eitem->datumdatasz;
			elemno += eitem->t_num_elements;
		}
	}

	return newitem;
}

static NXAttributeArrayItem *
nxbt_pack_item(Form_pg_attribute att, NXExplodedItem * eitem)
{
	NXAttributeArrayItem *newitem;
	int			num_elements = eitem->t_num_elements;
	nxtid		firsttid;
	nxtid		prevtid;
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
		nxtid		this_tid = eitem->tids[i];

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

	nullbitmapsz = NXBT_ATTR_BITMAPLEN(num_elements);
	has_nulls = false;
	for (int i = 0; i < nullbitmapsz; i++)
	{
		if (eitem->nullbitmap[i] != 0)
		{
			has_nulls = true;
			break;
		}
	}

	itemsz = offsetof(NXAttributeArrayItem, t_tid_codewords);
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
	newitem->t_flags = eitem->t_flags & NXBT_ATTR_FORMAT_MASK;
	if (has_nulls)
		newitem->t_flags |= NXBT_HAS_NULLS;
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

/*
 * Check whether an item is a candidate for FSST string compression.
 *
 * FSST is beneficial for items containing varlena string data.  We skip
 * items that use specialized encodings (bitpacked, FOR, dict, fixed-bin)
 * since those are not string-oriented.
 */
static inline bool
nxbt_item_is_fsst_candidate(uint16 flags)
{
	if (flags & (NXBT_ATTR_BITPACKED |
				 NXBT_ATTR_FORMAT_FOR |
				 NXBT_ATTR_FORMAT_DELTA_OF_DELTA |
				 NXBT_ATTR_FORMAT_CHIMP |
				 NXBT_ATTR_FORMAT_DICT |
				 NXBT_ATTR_FORMAT_FIXED_BIN))
		return false;

	/*
	 * Only items with varlena data benefit from FSST.  The native varlena
	 * flag is a strong signal; absence of all fixed-width encoding flags
	 * with presence of data also qualifies.
	 */
	return true;
}

static NXAttributeArrayItem *
nxbt_compress_item(NXAttributeArrayItem * item, Relation rel, AttrNumber attno)
{
	NXAttributeCompressedItem *citem;
	char	   *uncompressed_payload;
	int			uncompressed_size;
	int			compressed_size;
	int			item_allocsize;
	bool		used_fsst = false;
	bool		used_shared_dict = false;
	bool		try_fsst;

	Assert(item->t_size > 0);

	uncompressed_payload = (char *) &item->t_tid_codewords;
	uncompressed_size = ((char *) item) + item->t_size - uncompressed_payload;

	item_allocsize = item->t_size;

	/*
	 * pglz_compress() requires the destination buffer to be slightly larger
	 * than the source to even attempt compression (due to its internal
	 * bookkeeping). We add a small margin so pglz will try. If compression
	 * results in output larger than the original, we discard it and store
	 * uncompressed -- the margin never appears in the final on-disk item.
	 */
	item_allocsize += 10;

	/*
	 * For FSST, we need extra room for the serialized symbol table.
	 * A conservative upper bound: 2 + 255 * (1 + 8) = 2297 bytes.
	 * But the compressed output + table still needs to beat srcSize.
	 */
	try_fsst = nxbt_item_is_fsst_candidate(item->t_flags);
	if (try_fsst)
		item_allocsize = Max(item_allocsize, uncompressed_size + 2500);

	citem = palloc(item_allocsize);
	citem->t_flags = NXBT_ATTR_COMPRESSED;
	/* Preserve all encoding flags through compression */
	citem->t_flags |= (item->t_flags & (NXBT_HAS_NULLS |
										 NXBT_ATTR_FORMAT_FOR |
										 NXBT_ATTR_FORMAT_DELTA_OF_DELTA |
										 NXBT_ATTR_BITPACKED |
										 NXBT_ATTR_NO_NULLS |
										 NXBT_ATTR_SPARSE_NULLS |
										 NXBT_ATTR_RLE_NULLS |
										 NXBT_ATTR_FORMAT_NATIVE_VARLENA |
										 NXBT_ATTR_FORMAT_DICT |
										 NXBT_ATTR_FORMAT_FIXED_BIN |
										 NXBT_ATTR_FORMAT_FSST |
										 NXBT_ATTR_FORMAT_CHIMP |
										 NXBT_ATTR_FORMAT_UUID_V7_DELTA |
										 NXBT_ATTR_FORMAT_ARRAY_DECOMPOSED |
										 NXBT_ATTR_VECTOR_QUANTIZED_F16));
	citem->t_num_elements = item->t_num_elements;
	citem->t_num_codewords = item->t_num_codewords;
	citem->t_uncompressed_size = uncompressed_size;
	citem->t_firsttid = item->t_firsttid;
	citem->t_endtid = item->t_endtid;

	/*
	 * Try shared dictionary compression first, if available.
	 *
	 * Shared dictionary compression operates at the zstd level and
	 * complements existing pre-encodings (FSST, FOR, etc.).  When a
	 * shared dictionary exists, we try it first because it can capture
	 * cross-item patterns that per-item compression misses.
	 */
	if (rel != NULL && attno != InvalidAttrNumber)
	{
		NXSharedDictData *dict = nx_shared_dict_load(rel, attno);

		if (dict != NULL)
		{
			int		dict_compressed_size;

			dict_compressed_size = nx_try_compress_with_shared_dict(
				uncompressed_payload,
				citem->t_payload,
				uncompressed_size,
				item_allocsize - offsetof(NXAttributeCompressedItem, t_payload),
				dict);

			if (dict_compressed_size > 0 &&
				dict_compressed_size + 8 < uncompressed_size)
			{
				compressed_size = dict_compressed_size;
				used_shared_dict = true;
			}
		}
	}

	/*
	 * If shared dictionary didn't help (or isn't available), fall back to
	 * the standard compression path.
	 */
	if (!used_shared_dict)
	{
		/*
		 * Try compression.  For varlena items that are FSST candidates, use
		 * nx_try_compress_auto_fsst() which builds a symbol table from the
		 * data and tries FSST+general compression, falling back to plain
		 * compression if FSST doesn't help.
		 */
		if (try_fsst)
		{
			compressed_size = nx_try_compress_auto_fsst(uncompressed_payload,
														citem->t_payload,
														uncompressed_size,
														item_allocsize - offsetof(NXAttributeCompressedItem, t_payload),
														&used_fsst);
		}
		else
		{
			compressed_size = nx_try_compress(uncompressed_payload,
											  citem->t_payload,
											  uncompressed_size,
											  item_allocsize - offsetof(NXAttributeCompressedItem, t_payload));
		}
	}

	/* Set FSST flag if FSST encoding was used */
	if (used_fsst)
		citem->t_flags |= NXBT_ATTR_FORMAT_FSST;

	/* Set shared dictionary flag if dictionary compression was used */
	if (used_shared_dict)
		citem->t_flags |= NXBT_ATTR_SHARED_DICT;

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
		citem->t_size = offsetof(NXAttributeCompressedItem, t_payload) + compressed_size;
		Assert(citem->t_size < item->t_size);
		return (NXAttributeArrayItem *) citem;
	}
	else
	{
		/* Compression didn't save enough space - free citem and return original */
		pfree(citem);
		return item;
	}
}


/*
 * Re-pack and compress a list of items.
 *
 * If there are small items in the input list, such that they can be merged
 * together into larger items, we'll do that. And if there are uncompressed
 * items, we'll try to compress them. If the input list contains "exploded"
 * in-memory items, they will be packed into proper items suitable for
 * storing on-disk.
 *
 * The "try-compression-first" optimization allows combining items up to
 * a larger uncompressed budget (2x MAX_ATTR_ITEM_SIZE) when compression
 * is available. The combined item is then compressed; if the compressed
 * size fits in MAX_ATTR_ITEM_SIZE, the larger batch is kept. Otherwise
 * we fall back to the conservative uncompressed-size limit.
 */
List *
nxbt_attr_recompress_items(Form_pg_attribute attr, List *items,
						   Relation rel, AttrNumber attno)
{
	List	   *newitems = NIL;
	int			i;

	/*
	 * Determine whether aggressive combining is worthwhile.  When
	 * compression is available (zstd/LZ4) we allow combining items up to
	 * 2x the normal size budget, betting that compression will bring the
	 * result back within budget.
	 */
	size_t		aggressive_limit = MAX_ATTR_ITEM_SIZE * 2;

	/* loop through items, and greedily pack them */

	i = 0;
	while (i < list_length(items))
	{
		int			total_num_elements = 0;
		size_t		total_size = 0;
		int			j;
		int			conservative_j = -1;	/* fallback split point */
		NXAttributeArrayItem *newitem;

		for (j = i; j < list_length(items); j++)
		{
			NXAttributeArrayItem *this_item = (NXAttributeArrayItem *) list_nth(items, j);
			size_t		this_size;
			int			this_num_elements;

			this_size = nxbt_item_uncompressed_size(this_item);
			this_num_elements = this_item->t_num_elements;

			/*
			 * Don't create an item that's too large in # of tids.
			 */
			if ((size_t) (total_num_elements + this_num_elements) > MAX_TIDS_PER_ATTR_ITEM)
				break;

			/*
			 * Track the conservative split point (uncompressed limit),
			 * but continue accumulating up to the aggressive limit so
			 * compression can pack more data into each item.
			 */
			if (conservative_j < 0 &&
				total_size + this_size > MAX_ATTR_ITEM_SIZE)
				conservative_j = j;

			if (total_size + this_size > aggressive_limit)
				break;

			total_size += this_size;
			total_num_elements += this_num_elements;
		}
		if (j == i)
			j++;				/* tolerate existing oversized items */

		/* i - j are the items to pack */
		if (j - i > 1)
		{
			NXAttributeArrayItem *packeditem;
			NXExplodedItem *combineditem;

			combineditem = nxbt_combine_items(attr, items, i, j, rel, attno);
			packeditem = nxbt_pack_item(attr, combineditem);
			newitem = nxbt_compress_item(packeditem, rel, attno);

			/*
			 * If we exceeded the conservative limit but compression didn't
			 * help enough (item is still too large), fall back to the
			 * conservative split point and retry with fewer items.
			 */
			if (conservative_j > 0 && newitem->t_size > MAX_ATTR_ITEM_SIZE)
			{
				/*
				 * Free first-attempt allocations to prevent memory leak.
				 * combineditem's sub-allocations (tids, nullbitmap, datumdata)
				 * need to be freed, then combineditem itself.
				 * packeditem may be the same as newitem if compression failed,
				 * or different if compression succeeded.
				 */
				if (combineditem->t_size == 0)
				{
					/* combineditem is an exploded item */
					pfree(combineditem->tids);
					pfree(combineditem->nullbitmap);
					pfree(combineditem->datumdata);
					pfree(combineditem);
				}
				if (newitem != packeditem)
				{
					/* packeditem was allocated separately and not returned */
					pfree(packeditem);
				}
				if (newitem->t_flags & NXBT_ATTR_COMPRESSED)
				{
					/* newitem is a compressed item (citem), free it */
					pfree(newitem);
				}

				j = conservative_j;
				if (j - i > 1)
				{
					combineditem = nxbt_combine_items(attr, items, i, j, rel, attno);
					packeditem = nxbt_pack_item(attr, combineditem);
					newitem = nxbt_compress_item(packeditem, rel, attno);

				/* Free second-attempt allocations */
				if (combineditem->t_size == 0)
				{
					pfree(combineditem->tids);
					pfree(combineditem->nullbitmap);
					pfree(combineditem->datumdata);
					pfree(combineditem);
				}
				if (newitem != packeditem)
					pfree(packeditem);
				}
				else
				{
					NXAttributeArrayItem *olditem = list_nth(items, i);

					if (olditem->t_size == 0)
					{
						packeditem = nxbt_pack_item(attr, (NXExplodedItem *) olditem);
						newitem = nxbt_compress_item(packeditem, rel, attno);
					if (newitem != packeditem)
						pfree(packeditem);
					}
					else if (olditem->t_flags & NXBT_ATTR_COMPRESSED)
						newitem = olditem;
					else
						newitem = nxbt_compress_item(olditem, rel, attno);
				}
			}
		}
		else
		{
			NXAttributeArrayItem *olditem = list_nth(items, i);
		NXAttributeArrayItem *packeditem;

			if (olditem->t_size == 0)
			{
				packeditem = nxbt_pack_item(attr, (NXExplodedItem *) olditem);
				newitem = nxbt_compress_item(packeditem, rel, attno);
				if (newitem != packeditem)
					pfree(packeditem);
			}
			else if (olditem->t_flags & NXBT_ATTR_COMPRESSED)
				newitem = olditem;
			else
				newitem = nxbt_compress_item(olditem, rel, attno);
		}

		newitems = lappend(newitems, newitem);

		i = j;
	}

	/* Check that the resulting items are in correct order, and don't overlap. */
#ifdef USE_ASSERT_CHECKING
	{
		nxtid		endtid = 0;
		ListCell   *lc;

		foreach(lc, newitems)
		{
			NXAttributeArrayItem *i = (NXAttributeArrayItem *) lfirst(lc);

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
