/*
 * noxu_dict.h
 *		Dictionary encoding for low-cardinality columns in Noxu tables
 *
 * When a column has very few distinct values relative to the total number
 * of rows (distinct_count / total_rows < 0.01), we can replace each value
 * with a small integer index into a dictionary of distinct values. This
 * achieves 10-100x compression for low-cardinality string columns.
 *
 * On-Disk Format:
 * When NXBT_ATTR_FORMAT_DICT is set in t_flags, the datum data section
 * of an NXAttributeArrayItem is replaced with:
 *
 *   [NXDictHeader]
 *   [offsets: uint32 * num_entries]       -- byte offsets into values data
 *   [values data: total_data_size bytes]  -- packed distinct values
 *   [indices: uint16 * num_elements]      -- one index per element
 *
 * NULL values use the sentinel index NX_DICT_NULL_INDEX (0xFFFF).
 *
 * Limitations:
 * - Maximum 65,534 distinct entries (uint16 indices, minus NULL sentinel)
 * - Maximum 64 KB total dictionary value data
 * - Only applied when cardinality ratio < NX_DICT_CARDINALITY_THRESHOLD
 *
 * Copyright (c) 2019-2026, PostgreSQL Global Development Group
 *
 * src/include/access/noxu_dict.h
 */
#ifndef NOXU_DICT_H
#define NOXU_DICT_H

#include "c.h"					/* for uint16, uint32, bool, Datum, etc. */
#include "access/tupdesc.h"		/* for Form_pg_attribute */

/*
 * Cardinality threshold for dictionary encoding. If distinct_count / total_rows
 * is less than this value, dictionary encoding is considered beneficial.
 */
#define NX_DICT_CARDINALITY_THRESHOLD	0.01

/*
 * Maximum number of dictionary entries. We use uint16 indices, so the maximum
 * is 65534 (0xFFFF is reserved as a NULL marker).
 */
#define NX_DICT_MAX_ENTRIES				65534

/* Sentinel index value representing a NULL datum */
#define NX_DICT_NULL_INDEX				0xFFFF

/*
 * Maximum total size of dictionary values in bytes. Prevents memory blowup
 * for columns with very wide values.
 */
#define NX_DICT_MAX_TOTAL_SIZE			(64 * 1024)

/*
 * NXDictionary
 *		In-memory dictionary structure used during encoding/decoding
 *
 * The on-disk format is: [NXDictHeader] [offsets array] [values data].
 *
 * num_entries: number of distinct values in the dictionary
 * entry_size: fixed entry size if > 0; 0 means variable-length
 * total_data_size: total size of all packed value data in bytes
 * values: packed value data buffer
 * offsets: byte offsets into values for each entry
 */
typedef struct NXDictionary
{
	uint16		num_entries;	/* number of distinct values */
	uint16		entry_size;		/* fixed entry size if > 0, else variable */
	uint32		total_data_size; /* total size of all value data */
	char	   *values;			/* packed value data */
	uint32	   *offsets;		/* offsets[i] = start of entry i in values */
} NXDictionary;

/*
 * NXDictHeader
 *		On-disk header for a dictionary-encoded attribute item
 *
 * Stored as the first bytes of the datum data region, replacing raw datums.
 *
 * On-Disk Layout (following this header):
 *
 *   [offsets: uint32 * num_entries]       -- byte offsets into values data
 *   [values data: total_data_size bytes]
 *   [indices: uint16 * num_elements]      -- one index per element
 *
 * num_entries: number of distinct values
 * entry_size: fixed entry size, or 0 for variable-length entries
 * total_data_size: total size of all value data in bytes
 */
typedef struct NXDictHeader
{
	uint16		num_entries;
	uint16		entry_size;		/* 0 = variable-length entries */
	uint32		total_data_size;
} NXDictHeader;

/* --- Public API --- */

/*
 * nx_dict_should_encode
 *		Check whether dictionary encoding would be beneficial
 *
 * Returns true if the number of distinct values in datums is below
 * NX_DICT_CARDINALITY_THRESHOLD relative to nitems, and the dictionary
 * fits within size limits.
 *
 * att: attribute descriptor (type information)
 * datums: array of datum values
 * isnulls: array of NULL flags
 * nitems: number of elements
 */
extern bool nx_dict_should_encode(Form_pg_attribute att,
								  Datum *datums, bool *isnulls,
								  int nitems);

/*
 * nx_dict_encode
 *		Encode an array of datums using dictionary encoding
 *
 * Returns a palloc'd buffer containing the complete encoded representation:
 * [NXDictHeader] [offsets] [values] [indices].
 *
 * att: attribute descriptor (type information)
 * datums: array of datum values to encode
 * isnulls: array of NULL flags
 * nitems: number of elements
 * encoded_size: output - total size of the encoded buffer in bytes
 */
extern char *nx_dict_encode(Form_pg_attribute att,
							Datum *datums, bool *isnulls,
							int nitems, int *encoded_size);

/*
 * nx_dict_decode
 *		Decode dictionary-encoded data back into an array of Datums
 *
 * Reads from the encoded buffer starting at src and populates
 * datums and isnulls arrays.
 *
 * Returns the number of bytes consumed from src.
 *
 * att: attribute descriptor (type information)
 * src: pointer to the encoded data (starts with NXDictHeader)
 * src_size: total size of the encoded data buffer
 * datums: output - array of decoded datum values
 * isnulls: output - array of NULL flags
 * nitems: number of elements to decode
 * buf: working buffer for variable-length value reconstruction
 * buf_size: size of the working buffer
 */
extern int nx_dict_decode(Form_pg_attribute att,
						  const char *src, int src_size,
						  Datum *datums, bool *isnulls,
						  int nitems,
						  char *buf, int buf_size);

/*
 * nx_dict_encoded_size
 *		Estimate the encoded size without actually encoding
 *
 * Useful for size estimation during page split decisions.
 *
 * Returns estimated encoded size in bytes.
 *
 * att: attribute descriptor (type information)
 * datums: array of datum values
 * isnulls: array of NULL flags
 * nitems: number of elements
 */
extern int nx_dict_encoded_size(Form_pg_attribute att,
								Datum *datums, bool *isnulls,
								int nitems);

#endif							/* NOXU_DICT_H */
