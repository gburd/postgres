/*
 * orvos_dict.h
 *		Dictionary encoding for low-cardinality columns in Orvos tables
 *
 * When a column has very few distinct values relative to the total number
 * of rows (distinct_count / total_rows < 0.01), we can replace each value
 * with a small integer index into a dictionary of distinct values. This
 * achieves 10-100x compression for low-cardinality string columns.
 *
 * Copyright (c) 2019-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/include/access/orvos_dict.h
 */
#ifndef ORVOS_DICT_H
#define ORVOS_DICT_H

#include "c.h"					/* for uint16, uint32, bool, Datum, etc. */
#include "access/tupdesc.h"		/* for Form_pg_attribute */

/*
 * Cardinality threshold for dictionary encoding.
 * If distinct_count / total_rows < this value, use dictionary encoding.
 */
#define OV_DICT_CARDINALITY_THRESHOLD	0.01

/*
 * Maximum number of dictionary entries. We use uint16 indices, so the
 * maximum is 65535 (0xFFFF is reserved as a NULL marker).
 */
#define OV_DICT_MAX_ENTRIES				65534
#define OV_DICT_NULL_INDEX				0xFFFF

/*
 * Maximum total size of dictionary values (to avoid blowing up memory
 * for very wide values). 64KB should be generous.
 */
#define OV_DICT_MAX_TOTAL_SIZE			(64 * 1024)

/*
 * Dictionary structure stored in memory during encoding/decoding.
 * The on-disk format is: [OVDictHeader] [offsets array] [values data]
 */
typedef struct OVDictionary
{
	uint16		num_entries;	/* number of distinct values */
	uint16		entry_size;		/* fixed entry size if > 0, else variable */
	uint32		total_data_size; /* total size of all value data */
	char	   *values;			/* packed value data */
	uint32	   *offsets;		/* offsets[i] = start of entry i in values */
} OVDictionary;

/*
 * On-disk header for a dictionary-encoded item.
 * Stored as the first bytes of the datum data region in the attribute
 * array item, replacing the raw datums.
 *
 * Layout:
 *   [OVDictHeader]
 *   [offsets: uint32 * num_entries]    -- byte offsets into values data
 *   [values data: total_data_size bytes]
 *   [indices: uint16 * num_elements]   -- one index per element
 */
typedef struct OVDictHeader
{
	uint16		num_entries;
	uint16		entry_size;		/* 0 = variable-length entries */
	uint32		total_data_size;
} OVDictHeader;

/* --- Public API --- */

/*
 * Check whether dictionary encoding would be beneficial for a set of datums.
 * Returns true if the cardinality is below the threshold.
 */
extern bool ov_dict_should_encode(Form_pg_attribute att,
								  Datum *datums, bool *isnulls,
								  int nitems);

/*
 * Encode an array of datums using dictionary encoding.
 * Returns a palloc'd buffer containing:
 *   [OVDictHeader] [offsets] [values] [indices]
 * and sets *encoded_size to the total size.
 */
extern char *ov_dict_encode(Form_pg_attribute att,
							Datum *datums, bool *isnulls,
							int nitems, int *encoded_size);

/*
 * Decode dictionary-encoded data back into an array of Datums.
 * Reads from the encoded buffer and populates datums[] and isnulls[].
 * Returns the number of bytes consumed from src.
 */
extern int ov_dict_decode(Form_pg_attribute att,
						  const char *src, int src_size,
						  Datum *datums, bool *isnulls,
						  int nitems,
						  char *buf, int buf_size);

/*
 * Compute the encoded size of dictionary data without actually encoding.
 * Useful for size estimation.
 */
extern int ov_dict_encoded_size(Form_pg_attribute att,
								Datum *datums, bool *isnulls,
								int nitems);

#endif							/* ORVOS_DICT_H */
