/*
 * noxu_array.h
 *		Array element-level compression for Noxu attribute pages
 *
 * When a column stores PostgreSQL arrays of a supported element type,
 * Noxu can decompose the arrays into their individual elements and store
 * those elements contiguously.  This dramatically improves compression
 * because homogeneous element data compresses far better than the
 * PostgreSQL ArrayType binary layout (which includes headers, dimensions,
 * alignment padding, etc.).
 *
 * Supported element types: bool, int2, int4, int8, float4, float8,
 * timestamp, text, uuid.
 *
 * On-disk layout when NXBT_ATTR_FORMAT_ARRAY_DECOMPOSED is set:
 *
 *   [NXArrayDecomposedHeader]          -- element type info
 *   [uint16 elem_counts[num_rows]]     -- element count per original array
 *                                         (0 for NULL arrays)
 *   [uint8 elem_nullbitmap[...]]       -- one bit per element (if has_elem_nulls)
 *   [element data]                     -- packed element values
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * src/include/access/noxu_array.h
 */
#ifndef NOXU_ARRAY_H
#define NOXU_ARRAY_H

#include "postgres.h"
#include "access/tupdesc.h"

/*
 * NXArrayDecomposedHeader - header for decomposed array storage.
 *
 * Stored at the beginning of the datum data section when
 * NXBT_ATTR_FORMAT_ARRAY_DECOMPOSED is set in the item flags.
 */
typedef struct NXArrayDecomposedHeader
{
	Oid			elem_typid;		/* element type OID */
	int16		elem_typlen;	/* element type length */
	bool		elem_typbyval;	/* element type pass-by-value? */
	bool		has_elem_nulls; /* any NULL elements? */
	uint32		total_elems;	/* total element count across all arrays */
} NXArrayDecomposedHeader;

/*
 * nx_array_should_decompose
 *		Check whether a column is a candidate for array decomposition.
 *
 * Returns true if the column is a PostgreSQL array type with supported
 * elements.  The element type info is returned via the output parameters.
 */
extern bool nx_array_should_decompose(Form_pg_attribute att,
									  Oid *elem_typid,
									  int16 *elem_typlen,
									  bool *elem_typbyval,
									  char *elem_typalign);

/*
 * nx_array_decompose_and_encode
 *		Decompose arrays into elements and pack into a datum data buffer.
 *
 * Takes the array Datums for a set of rows and produces a contiguous
 * buffer of decomposed element data.  Returns the buffer and its size
 * via output parameters.
 *
 * Returns the encoded data buffer (palloc'd), or NULL if decomposition
 * would not be beneficial.  Sets *encoded_size to the buffer size.
 */
extern char *nx_array_decompose_and_encode(Form_pg_attribute att,
										   Datum *datums, bool *isnulls,
										   int num_rows,
										   int *encoded_size);

/*
 * nx_array_decomposed_decode
 *		Reconstruct array Datums from decomposed element data.
 *
 * Reads the NXArrayDecomposedHeader and element data from src, and
 * fills in the datums and isnulls arrays with reconstructed PostgreSQL
 * ArrayType values.
 *
 * attr_buf/attr_buf_size are used for reconstructed array storage.
 */
extern void nx_array_decomposed_decode(Form_pg_attribute att,
									   char *src, int srcSize,
									   Datum *datums, bool *isnulls,
									   int num_rows,
									   char *attr_buf, int attr_buf_size);

#endif							/* NOXU_ARRAY_H */
