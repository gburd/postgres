/*
 * chimp.h
 *		Chimp float compression for Noxu columnar storage
 *
 * Chimp is an XOR-based float compression algorithm that extends the
 * Gorilla algorithm with improved leading/trailing zero tracking.  It
 * XORs consecutive float values and encodes the result using variable-
 * length bit patterns that exploit the observation that successive
 * floats in time-series and analytical workloads tend to share many
 * bits.
 *
 * References:
 *   Panagiotis Liakos, Katia Papakonstantinopoulou, Yannis Kotidis.
 *   "Chimp: Efficient Lossless Floating Point Compression for Time
 *   Series Databases." PVLDB 15(2022).
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * src/include/lib/chimp.h
 */
#ifndef CHIMP_H
#define CHIMP_H

#include "postgres.h"

/*
 * ChimpBlockHeader describes the encoded block layout stored in the
 * datum-data section of an NXAttributeArrayItem when the
 * NXBT_ATTR_FORMAT_CHIMP flag is set.
 *
 * On-disk layout:
 *   [ChimpBlockHeader] [packed bit stream]
 *
 * The bit stream is written LSB-first into successive bytes.
 */
typedef struct ChimpBlockHeader
{
	uint8		chimp_value_width;	/* 4 = float4, 8 = float8 */
	uint8		chimp_leading_bits;	/* bits used to encode leading zeros */
	uint16		chimp_num_values;	/* number of encoded values */
	uint32		chimp_first_value_lo;	/* first value, low 32 bits */
	uint32		chimp_first_value_hi;	/* first value, high 32 bits (float8) */
	uint32		chimp_packed_bytes;	/* size of packed bit stream in bytes */
} ChimpBlockHeader;

/*
 * chimp_encode
 *		Encode float4 values using Chimp compression.
 *
 * datums/isnulls: input datum array (NULLs are skipped)
 * num_elements: number of elements in the array
 * dst: output buffer (must be large enough)
 * dst_capacity: size of output buffer in bytes
 *
 * Returns the number of bytes written, or 0 on failure.
 */
extern int	chimp_encode(Datum *datums, bool *isnulls,
						 int num_elements, char *dst, int dst_capacity);

/*
 * chimp_decode
 *		Decode Chimp-compressed float4 values.
 *
 * src: compressed data (starts with ChimpBlockHeader)
 * src_size: size of compressed data in bytes
 * datums: output datum array
 * isnulls: NULL flags (caller must pre-populate; NULLs are skipped)
 * num_elements: total number of elements including NULLs
 */
extern void chimp_decode(const char *src, int src_size,
						 Datum *datums, bool *isnulls, int num_elements);

/*
 * chimp_encode_float8
 *		Encode float8 values using Chimp compression.
 *
 * Same interface as chimp_encode but for 8-byte doubles.
 */
extern int	chimp_encode_float8(Datum *datums, bool *isnulls,
								int num_elements, char *dst, int dst_capacity);

/*
 * chimp_decode_float8
 *		Decode Chimp-compressed float8 values.
 *
 * Same interface as chimp_decode but for 8-byte doubles.
 */
extern void chimp_decode_float8(const char *src, int src_size,
								Datum *datums, bool *isnulls, int num_elements);

/*
 * chimp_should_encode
 *		Decide whether Chimp encoding is worthwhile for a set of datums.
 *
 * Returns true if the column type is float4 or float8, there are enough
 * non-null values, and trial encoding shows the Chimp representation is
 * smaller than the raw datum storage.
 *
 * att: attribute descriptor (used to check type OID and length)
 * datums: array of Datum values
 * isnulls: per-datum NULL flags
 * num_elements: total number of elements
 * raw_datasz: size of raw (unencoded) datum data in bytes
 * chimp_datasz_p: (out) size of Chimp-encoded data in bytes
 */
extern bool chimp_should_encode(Form_pg_attribute att,
								Datum *datums, bool *isnulls,
								int num_elements, int raw_datasz,
								int *chimp_datasz_p);

#endif							/* CHIMP_H */
