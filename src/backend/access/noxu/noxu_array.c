/*
 * noxu_array.c
 *		Array element-level compression for Noxu attribute pages
 *
 * This module implements decomposition of PostgreSQL array values into
 * their individual elements for more efficient columnar compression.
 * Instead of storing the opaque binary ArrayType blobs (which include
 * headers, dimension info, and alignment padding), we extract the raw
 * elements and store them contiguously.  The element stream then benefits
 * from Noxu's general-purpose compression (zstd/LZ4/pglz) because
 * homogeneous typed data compresses much better.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/noxu/noxu_array.c
 */
#include "postgres.h"

#include "access/noxu_array.h"
#include "access/noxu_internal.h"
#include "catalog/pg_type_d.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

/*
 * Minimum number of total elements across all arrays in an item before
 * we consider decomposition worthwhile.  Below this threshold the header
 * overhead exceeds any compression benefit.
 */
#define NX_ARRAY_MIN_TOTAL_ELEMS	8

/*
 * nx_array_should_decompose
 *		Determine whether a column type is a supported array type for
 *		element-level decomposition.
 *
 * We support 1-D arrays of: bool, int2, int4, int8, float4, float8,
 * timestamp, text, uuid.  Multi-dimensional arrays are not decomposed
 * because the dimension metadata overhead is complex to preserve.
 */
bool
nx_array_should_decompose(Form_pg_attribute att,
						  Oid *elem_typid,
						  int16 *elem_typlen,
						  bool *elem_typbyval,
						  char *elem_typalign)
{
	Oid		typid = att->atttypid;
	Oid		eid;
	int16	elen;
	bool	ebyval;
	char	ealign;

	/*
	 * Array types in PostgreSQL are varlena (attlen == -1).
	 * Quick reject for non-varlena columns.
	 */
	if (att->attlen != -1)
		return false;

	/*
	 * Check for known array type OIDs directly to avoid syscache lookups
	 * on the hot path.  These are the array types whose elements we know
	 * how to handle efficiently.
	 */
	switch (typid)
	{
		case BOOLARRAYOID:
			eid = BOOLOID;
			elen = 1;
			ebyval = true;
			ealign = 'c';
			break;
		case INT2ARRAYOID:
			eid = INT2OID;
			elen = 2;
			ebyval = true;
			ealign = 's';
			break;
		case INT4ARRAYOID:
			eid = INT4OID;
			elen = 4;
			ebyval = true;
			ealign = 'i';
			break;
		case INT8ARRAYOID:
			eid = INT8OID;
			elen = 8;
			ebyval = true;
			ealign = 'd';
			break;
		case FLOAT4ARRAYOID:
			eid = FLOAT4OID;
			elen = 4;
			ebyval = true;
			ealign = 'i';
			break;
		case FLOAT8ARRAYOID:
			eid = FLOAT8OID;
			elen = 8;
			ebyval = true;
			ealign = 'd';
			break;
		case TIMESTAMPARRAYOID:
			eid = TIMESTAMPOID;
			elen = 8;
			ebyval = true;
			ealign = 'd';
			break;
		case TEXTARRAYOID:
			eid = TEXTOID;
			elen = -1;
			ebyval = false;
			ealign = 'i';
			break;
		case UUIDARRAYOID:
			eid = UUIDOID;
			elen = 16;
			ebyval = false;
			ealign = 'c';
			break;
		default:
			return false;
	}

	*elem_typid = eid;
	*elem_typlen = elen;
	*elem_typbyval = ebyval;
	*elem_typalign = ealign;
	return true;
}

/*
 * Write a single element value into the output buffer using Noxu's
 * compact encoding.  For fixed-length pass-by-value types, the raw
 * bytes are written directly.  For fixed-length pass-by-ref types
 * (e.g., UUID), the pointed-to bytes are copied.  For varlena types,
 * we use Noxu's 1-byte or 2-byte length prefix encoding.
 *
 * Returns the number of bytes written.
 */
static int
write_element(char *dst, Datum val, int16 elem_typlen, bool elem_typbyval)
{
	if (elem_typbyval)
	{
		/* Pass-by-value: store raw bytes */
		switch (elem_typlen)
		{
			case 1:
				*dst = DatumGetChar(val);
				return 1;
			case 2:
				{
					int16 v = DatumGetInt16(val);
					memcpy(dst, &v, 2);
					return 2;
				}
			case 4:
				{
					int32 v = DatumGetInt32(val);
					memcpy(dst, &v, 4);
					return 4;
				}
			case 8:
				{
					int64 v = DatumGetInt64(val);
					memcpy(dst, &v, 8);
					return 8;
				}
			default:
				elog(ERROR, "unsupported element typlen %d for byval", elem_typlen);
				return 0;
		}
	}
	else if (elem_typlen > 0)
	{
		/* Fixed-length pass-by-ref (e.g., UUID) */
		memcpy(dst, DatumGetPointer(val), elem_typlen);
		return elem_typlen;
	}
	else
	{
		/* Varlena: use Noxu compact encoding (1 or 2 byte header) */
		struct varlena *vl = (struct varlena *) DatumGetPointer(val);
		int		data_len = VARSIZE_ANY_EXHDR(vl);
		char   *data = VARDATA_ANY(vl);
		int		written = 0;

		if (data_len < 0x80)
		{
			/* 1-byte header */
			dst[0] = (char) data_len;
			written = 1;
		}
		else
		{
			/* 2-byte header */
			int		encoded = data_len - 1;
			dst[0] = (char) (0x80 | (encoded >> 8));
			dst[1] = (char) (encoded & 0xFF);
			written = 2;
		}
		memcpy(dst + written, data, data_len);
		return written + data_len;
	}
}

/*
 * nx_array_decompose_and_encode
 *		Main encoding function: decompose arrays and produce a packed buffer.
 *
 * For each non-NULL array datum, we deconstruct it into elements and
 * write them contiguously.  NULL arrays get an element count of 0.
 *
 * Returns a palloc'd buffer, or NULL if decomposition isn't worthwhile.
 */
char *
nx_array_decompose_and_encode(Form_pg_attribute att,
							  Datum *datums, bool *isnulls,
							  int num_rows,
							  int *encoded_size)
{
	Oid			elem_typid;
	int16		elem_typlen;
	bool		elem_typbyval;
	char		elem_typalign;
	uint32		total_elems = 0;
	bool		has_elem_nulls = false;
	int			original_size = 0;

	/* Per-row element info */
	uint16	   *elem_counts;
	Datum	  **row_elems;
	bool	  **row_elem_nulls;
	int		   *row_nelems;

	char	   *buf;
	char	   *p;
	int			buf_size;
	NXArrayDecomposedHeader *hdr;

	if (!nx_array_should_decompose(att, &elem_typid, &elem_typlen,
								   &elem_typbyval, &elem_typalign))
		return NULL;

	/* First pass: deconstruct all arrays and count elements */
	elem_counts = palloc(num_rows * sizeof(uint16));
	row_elems = palloc(num_rows * sizeof(Datum *));
	row_elem_nulls = palloc(num_rows * sizeof(bool *));
	row_nelems = palloc(num_rows * sizeof(int));

	for (int i = 0; i < num_rows; i++)
	{
		if (isnulls[i])
		{
			elem_counts[i] = 0;
			row_elems[i] = NULL;
			row_elem_nulls[i] = NULL;
			row_nelems[i] = 0;

			/*
			 * For original_size, a NULL varlena contributes 0 datum data
			 * bytes in the standard item encoding (NULLs are tracked in
			 * the NULL bitmap only).
			 */
			continue;
		}

		{
			ArrayType  *arr;
			struct varlena *vl = (struct varlena *) DatumGetPointer(datums[i]);

			/* Track original array blob sizes for benefit estimation */
			original_size += VARSIZE_ANY(vl);

			arr = DatumGetArrayTypeP(datums[i]);

			/* Only decompose 1-D arrays */
			if (ARR_NDIM(arr) != 1)
			{
				/* Not 1-D; bail out entirely */
				pfree(elem_counts);
				for (int j = 0; j < i; j++)
				{
					if (row_elems[j])
					{
						pfree(row_elems[j]);
						pfree(row_elem_nulls[j]);
					}
				}
				pfree(row_elems);
				pfree(row_elem_nulls);
				pfree(row_nelems);
				return NULL;
			}

			deconstruct_array(arr, elem_typid,
							  elem_typlen, elem_typbyval, elem_typalign,
							  &row_elems[i], &row_elem_nulls[i],
							  &row_nelems[i]);

			if (row_nelems[i] > UINT16_MAX)
			{
				/* Array too large -- don't decompose */
				pfree(elem_counts);
				for (int j = 0; j <= i; j++)
				{
					if (row_elems[j])
					{
						pfree(row_elems[j]);
						pfree(row_elem_nulls[j]);
					}
				}
				pfree(row_elems);
				pfree(row_elem_nulls);
				pfree(row_nelems);
				return NULL;
			}

			elem_counts[i] = (uint16) row_nelems[i];
			total_elems += row_nelems[i];

			/* Check for element-level NULLs */
			if (row_elem_nulls[i] != NULL)
			{
				for (int j = 0; j < row_nelems[i]; j++)
				{
					if (row_elem_nulls[i][j])
					{
						has_elem_nulls = true;
						break;
					}
				}
			}
		}
	}

	/* Check if there are enough elements to make decomposition worthwhile */
	if (total_elems < NX_ARRAY_MIN_TOTAL_ELEMS)
	{
		pfree(elem_counts);
		for (int i = 0; i < num_rows; i++)
		{
			if (row_elems[i])
			{
				pfree(row_elems[i]);
				pfree(row_elem_nulls[i]);
			}
		}
		pfree(row_elems);
		pfree(row_elem_nulls);
		pfree(row_nelems);
		return NULL;
	}

	/*
	 * Allocate output buffer.  Conservative estimate:
	 *   header + elem_counts + elem null bitmap + element data
	 * For varlena elements, worst case is 2-byte header + data per element.
	 * For fixed-length, it's exact.
	 */
	buf_size = sizeof(NXArrayDecomposedHeader);
	buf_size += num_rows * sizeof(uint16);	/* elem_counts */
	if (has_elem_nulls)
		buf_size += NXBT_ATTR_BITMAPLEN(total_elems);

	if (elem_typlen > 0)
		buf_size += total_elems * elem_typlen;
	else
		buf_size += original_size * 2;	/* conservative for varlena */

	buf = palloc(buf_size);
	p = buf;

	/* Write header */
	hdr = (NXArrayDecomposedHeader *) p;
	hdr->elem_typid = elem_typid;
	hdr->elem_typlen = elem_typlen;
	hdr->elem_typbyval = elem_typbyval;
	hdr->has_elem_nulls = has_elem_nulls;
	hdr->total_elems = total_elems;
	p += sizeof(NXArrayDecomposedHeader);

	/* Write per-row element counts */
	memcpy(p, elem_counts, num_rows * sizeof(uint16));
	p += num_rows * sizeof(uint16);

	/* Write element null bitmap if needed */
	if (has_elem_nulls)
	{
		uint8	   *nullbits = (uint8 *) p;
		int			bit_idx = 0;

		memset(nullbits, 0, NXBT_ATTR_BITMAPLEN(total_elems));

		for (int i = 0; i < num_rows; i++)
		{
			for (int j = 0; j < row_nelems[i]; j++)
			{
				if (row_elem_nulls[i] && row_elem_nulls[i][j])
					nullbits[bit_idx / 8] |= (1 << (bit_idx % 8));
				bit_idx++;
			}
		}

		p += NXBT_ATTR_BITMAPLEN(total_elems);
	}

	/* Write element data */
	{
		int		elem_idx = 0;

		for (int i = 0; i < num_rows; i++)
		{
			for (int j = 0; j < row_nelems[i]; j++)
			{
				if (has_elem_nulls && row_elem_nulls[i] && row_elem_nulls[i][j])
				{
					elem_idx++;
					continue;	/* NULL element: no data */
				}

				p += write_element(p, row_elems[i][j],
								   elem_typlen, elem_typbyval);
				elem_idx++;
			}
		}
	}

	/* Clean up temporary arrays */
	pfree(elem_counts);
	for (int i = 0; i < num_rows; i++)
	{
		if (row_elems[i])
		{
			pfree(row_elems[i]);
			pfree(row_elem_nulls[i]);
		}
	}
	pfree(row_elems);
	pfree(row_elem_nulls);
	pfree(row_nelems);

	*encoded_size = p - buf;

	/*
	 * Only use decomposed encoding if it produces a smaller result.
	 * Compare against the original varlena encoding size.
	 */
	if (*encoded_size >= original_size && original_size > 0)
	{
		pfree(buf);
		return NULL;
	}

	return buf;
}

/*
 * Read a single element value from the input buffer.
 *
 * Returns the Datum and advances *pp past the consumed bytes.
 * For varlena types, the output datum points into attr_buf.
 */
static Datum
read_element(char **pp, int16 elem_typlen, bool elem_typbyval,
			 char **attr_buf_p)
{
	char	   *p = *pp;

	if (elem_typbyval)
	{
		Datum		val = 0;

		switch (elem_typlen)
		{
			case 1:
				val = CharGetDatum(*p);
				p += 1;
				break;
			case 2:
				{
					int16 v;
					memcpy(&v, p, 2);
					val = Int16GetDatum(v);
					p += 2;
				}
				break;
			case 4:
				{
					int32 v;
					memcpy(&v, p, 4);
					val = Int32GetDatum(v);
					p += 4;
				}
				break;
			case 8:
				{
					int64 v;
					memcpy(&v, p, 8);
					val = Int64GetDatum(v);
					p += 8;
				}
				break;
			default:
				elog(ERROR, "unsupported element typlen %d", elem_typlen);
		}
		*pp = p;
		return val;
	}
	else if (elem_typlen > 0)
	{
		/* Fixed-length pass-by-ref (e.g., UUID) */
		char	   *dest = *attr_buf_p;

		memcpy(dest, p, elem_typlen);
		p += elem_typlen;
		*attr_buf_p += elem_typlen;
		*pp = p;
		return PointerGetDatum(dest);
	}
	else
	{
		/* Varlena: decode Noxu compact header */
		unsigned char *up = (unsigned char *) p;
		int		data_len;
		int		hdr_len;
		char   *dest;

		if ((up[0] & 0x80) == 0)
		{
			/* 1-byte header */
			data_len = up[0];
			hdr_len = 1;
		}
		else
		{
			/* 2-byte header */
			data_len = ((up[0] & 0x7F) << 8 | up[1]) + 1;
			hdr_len = 2;
		}

		p += hdr_len;

		/*
		 * Reconstruct a standard PostgreSQL varlena value in attr_buf.
		 * Use short (1-byte header) format when possible.
		 */
		dest = *attr_buf_p;
		if (data_len + VARHDRSZ_SHORT <= VARATT_SHORT_MAX + VARHDRSZ_SHORT)
		{
			SET_VARSIZE_SHORT(dest, data_len + VARHDRSZ_SHORT);
			memcpy(dest + VARHDRSZ_SHORT, p, data_len);
			*attr_buf_p += data_len + VARHDRSZ_SHORT;
		}
		else
		{
			SET_VARSIZE(dest, data_len + VARHDRSZ);
			memcpy(dest + VARHDRSZ, p, data_len);
			*attr_buf_p += data_len + VARHDRSZ;
		}

		p += data_len;
		*pp = (char *) p;
		return PointerGetDatum(dest);
	}
}

/*
 * nx_array_decomposed_decode
 *		Reconstruct PostgreSQL array Datums from decomposed element data.
 *
 * Reads the NXArrayDecomposedHeader and packed elements, then uses
 * construct_md_array() to rebuild each array.
 */
void
nx_array_decomposed_decode(Form_pg_attribute att,
						   char *src, int srcSize,
						   Datum *datums, bool *isnulls,
						   int num_rows,
						   char *attr_buf, int attr_buf_size)
{
	NXArrayDecomposedHeader *hdr;
	uint16	   *elem_counts;
	uint8	   *elem_nullbits = NULL;
	char	   *p;
	char	   *attr_buf_p = attr_buf;
	int			global_elem_idx = 0;

	hdr = (NXArrayDecomposedHeader *) src;
	p = src + sizeof(NXArrayDecomposedHeader);

	/* Read per-row element counts */
	elem_counts = (uint16 *) p;
	p += num_rows * sizeof(uint16);

	/* Read element null bitmap if present */
	if (hdr->has_elem_nulls)
	{
		elem_nullbits = (uint8 *) p;
		p += NXBT_ATTR_BITMAPLEN(hdr->total_elems);
	}

	/* Reconstruct each array */
	for (int i = 0; i < num_rows; i++)
	{
		int		nelems = elem_counts[i];

		if (nelems == 0)
		{
			/*
			 * An element count of 0 means this row had a NULL array.
			 */
			datums[i] = (Datum) 0;
			isnulls[i] = true;
			continue;
		}

		{
			Datum	   *elems;
			bool	   *enulls;
			bool		any_null = false;
			ArrayType  *result;
			int			dims[1];
			int			lbs[1];

			elems = palloc(nelems * sizeof(Datum));
			enulls = palloc(nelems * sizeof(bool));

			for (int j = 0; j < nelems; j++)
			{
				bool	this_null = false;

				if (elem_nullbits != NULL)
				{
					this_null = (elem_nullbits[global_elem_idx / 8] &
								 (1 << (global_elem_idx % 8))) != 0;
				}

				if (this_null)
				{
					elems[j] = (Datum) 0;
					enulls[j] = true;
					any_null = true;
				}
				else
				{
					elems[j] = read_element(&p, hdr->elem_typlen,
											hdr->elem_typbyval,
											&attr_buf_p);
					enulls[j] = false;
				}
				global_elem_idx++;
			}

			dims[0] = nelems;
			lbs[0] = 1;	/* PostgreSQL default lower bound */

			/*
			 * Get element alignment.  For built-in types we can determine
			 * this from the type OID without a syscache lookup.
			 */
			{
				char	ealign;

				switch (hdr->elem_typid)
				{
					case BOOLOID:
						ealign = 'c';
						break;
					case INT2OID:
						ealign = 's';
						break;
					case INT4OID:
					case FLOAT4OID:
					case TEXTOID:
						ealign = 'i';
						break;
					case INT8OID:
					case FLOAT8OID:
					case TIMESTAMPOID:
						ealign = 'd';
						break;
					case UUIDOID:
						ealign = 'c';
						break;
					default:
						ealign = 'i';
						break;
				}

				result = construct_md_array(elems,
											any_null ? enulls : NULL,
											1, dims, lbs,
											hdr->elem_typid,
											hdr->elem_typlen,
											hdr->elem_typbyval,
											ealign);
			}

			datums[i] = PointerGetDatum(result);
			isnulls[i] = false;

			pfree(elems);
			pfree(enulls);
		}
	}
}
