/*
 * noxu_uuid.h
 *		UUID v7 time-ordered compression for Noxu tables
 *
 * UUIDv7 (RFC 9562) embeds a 48-bit Unix timestamp in milliseconds in the
 * leading 6 bytes, followed by a 4-bit version nibble and 74 random bits.
 * When UUIDs are inserted in time order, the leading bytes are highly
 * redundant, enabling effective delta compression.
 *
 * Algorithm:
 * 1. Detect UUID version from the version nibble (byte 6, high nibble).
 * 2. For v7 (and v1/v6) UUIDs that are monotonically ordered, extract
 *    the 48-bit timestamp prefix.
 * 3. Delta-encode the timestamps: store a base timestamp and per-element
 *    deltas.  The random suffix bytes (bytes 6-15) are stored verbatim
 *    but with the common prefix deduplicated where possible.
 *
 * On-Disk Format:
 * When NXBT_ATTR_FORMAT_UUID_V7_DELTA is set in t_flags, the datum data
 * section of an NXAttributeArrayItem is replaced with:
 *
 *   [NXUUIDDeltaHeader]
 *   [base UUID: 16 bytes]
 *   [timestamp deltas: variable, bit-packed]
 *   [suffixes: 10 bytes per non-null element]
 *
 * Expected compression: 3-5x for time-ordered UUID columns.
 *
 * Copyright (c) 2019-2026, PostgreSQL Global Development Group
 *
 * src/include/access/noxu_uuid.h
 */
#ifndef NOXU_UUID_H
#define NOXU_UUID_H

#include "c.h"
#include "access/tupdesc.h"
#include "utils/uuid.h"

/*
 * UUID version constants (RFC 9562).
 * The version nibble is in byte 6 (bits 7..4).
 */
#define UUID_VERSION_1		1		/* time-based (Gregorian epoch) */
#define UUID_VERSION_4		4		/* random */
#define UUID_VERSION_6		6		/* reordered time-based */
#define UUID_VERSION_7		7		/* Unix epoch time-ordered */

/*
 * Byte offsets within a 16-byte UUID.
 * Bytes 0-5: timestamp (v7) or time_hi/time_mid (v1/v6)
 * Byte 6: version nibble (high 4 bits) + clock_seq_hi or rand_a
 * Byte 7: variant bits + clock_seq_lo or rand_a (cont.)
 * Bytes 8-15: node/random
 */
#define UUID_TIMESTAMP_LEN		6	/* 48-bit timestamp prefix */
#define UUID_SUFFIX_LEN			10	/* bytes 6-15 (version+random) */

/*
 * Minimum number of non-null UUIDs required for delta encoding to be
 * considered.  Below this threshold, the header overhead dominates.
 */
#define NX_UUID_MIN_FOR_DELTA	4

/*
 * Maximum bits per timestamp delta.  If the range of timestamps exceeds
 * this, we fall back to fixed-binary storage.
 */
#define NX_UUID_MAX_DELTA_BITS	48

/*
 * NXUUIDDeltaHeader
 *		On-disk header for UUID v7 delta-encoded data
 *
 * base_uuid: the first non-null UUID in the item (full 16 bytes)
 * bits_per_delta: number of bits per timestamp delta (0..48)
 * num_nonnull: count of non-null UUIDs encoded
 */
typedef struct NXUUIDDeltaHeader
{
	pg_uuid_t	base_uuid;			/* first UUID (reference point) */
	uint8		bits_per_delta;		/* bits per timestamp delta */
	uint8		padding;			/* alignment padding */
	uint16		num_nonnull;		/* number of non-null UUIDs */
} NXUUIDDeltaHeader;

/* --- Public API --- */

/*
 * uuid_get_version
 *		Extract the 4-bit version number from a UUID.
 *
 * Returns the version (1, 4, 6, 7, etc.) or 0 if the UUID is NULL.
 */
extern int uuid_get_version(const pg_uuid_t *uuid);

/*
 * uuid_is_time_ordered
 *		Check whether an array of UUIDs is suitable for delta compression.
 *
 * Returns true if the UUIDs are all the same version (v1, v6, or v7)
 * and their timestamps are monotonically non-decreasing.
 *
 * datums: array of UUID datum values
 * isnulls: array of NULL flags
 * nitems: number of elements
 */
extern bool uuid_is_time_ordered(Datum *datums, bool *isnulls, int nitems);

/*
 * uuid_compress_time_ordered
 *		Delta-encode an array of time-ordered UUIDs.
 *
 * Returns a palloc'd buffer containing [NXUUIDDeltaHeader][deltas][suffixes],
 * or NULL if compression is not beneficial.
 *
 * datums: array of UUID datum values
 * isnulls: array of NULL flags
 * nitems: number of elements
 * encoded_size: output - total size of the encoded buffer
 * raw_size: the raw fixed-binary size for comparison
 */
extern char *uuid_compress_time_ordered(Datum *datums, bool *isnulls,
										int nitems, int *encoded_size,
										int raw_size);

/*
 * uuid_decompress_delta
 *		Decode delta-encoded UUIDs back into an array of Datums.
 *
 * Reads from the encoded buffer starting at src and populates
 * datums and isnulls arrays.  UUID values are reconstructed into buf.
 *
 * src: pointer to encoded data (starts with NXUUIDDeltaHeader)
 * src_size: total size of encoded data
 * datums: output array of Datum values
 * isnulls: input array of NULL flags (already decoded by caller)
 * nitems: number of elements
 * buf: working buffer for reconstructed UUID values
 * buf_size: size of working buffer
 */
extern void uuid_decompress_delta(const char *src, int src_size,
								  Datum *datums, bool *isnulls,
								  int nitems, char *buf, int buf_size);

#endif							/* NOXU_UUID_H */
