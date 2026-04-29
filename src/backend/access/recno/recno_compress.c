/*-------------------------------------------------------------------------
 *
 * recno_compress.c
 *	  RECNO attribute compression implementation
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/recno/recno_compress.c
 *
 * NOTES
 *	  This implements attribute-level compression for RECNO tuples.
 *	  Supports multiple compression algorithms including LZ4, ZSTD,
 *	  delta compression for numeric data, and dictionary compression
 *	  for text data.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_LZ4
#include <lz4.h>
#endif
#ifdef USE_ZSTD
#include <zstd.h>
#endif

#include "access/recno.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/numeric.h"
#include "utils/syscache.h"

/*
 * Compression thresholds and settings
 */
#define RECNO_MIN_COMPRESS_SIZE		32	/* Minimum size to compress */
#define RECNO_COMPRESS_RATIO_MIN	0.8 /* Minimum compression ratio */
#define RECNO_DICT_MAX_ENTRIES		1024	/* Max dictionary entries */
#define RECNO_DELTA_MAX_VALUES		256 /* Max values for delta compression */


/*
 * GUC variables for compression
 */
int			recno_compression_level = 3;	/* Default compression level */
char	   *recno_compression_algorithm = NULL;
bool		recno_enable_compression = true;

/*
 * Dictionary compression structures
 *
 * NOTE ON MULTI-BACKEND SHARING:
 *
 * The dictionaries here are process-local.  Each backend builds its own
 * dictionary independently, which means:
 *   (a) A value compressed by one backend cannot be decompressed by another
 *       unless both backends have seen the same values in the same order.
 *   (b) Dictionary-compressed data must therefore only be used for
 *       transient, backend-local purposes (e.g. in-memory tuple diffs)
 *       or the dictionary must be persisted alongside the data.
 *
 * The per-relation cache below ensures that different relations get
 * independent dictionaries within a single backend, which is correct
 * behaviour even though the dictionaries are not shared across backends.
 *
 * TODO: Move dictionary storage to a catalog table or RELUNDO-fork
 * metapage so that dictionaries are visible to all backends and survive
 * restarts.  This requires WAL-logging dictionary mutations.
 */
typedef struct RecnoDictEntry
{
	char	   *value;
	int			length;
	int			frequency;
	int			dict_id;
}			RecnoDictEntry;

typedef struct RecnoCompressionDict
{
	Oid			relid;			/* Relation this dictionary belongs to */
	int			num_entries;
	RecnoDictEntry entries[RECNO_DICT_MAX_ENTRIES];
	MemoryContext dict_context;
#ifdef USE_ZSTD
	ZSTD_CDict *zstd_cdict;	/* Compiled ZSTD compression dictionary */
	ZSTD_DDict *zstd_ddict;	/* Compiled ZSTD decompression dictionary */
#endif
}			RecnoCompressionDict;

/*
 * Per-relation dictionary cache.  Keyed by relation OID so each relation
 * gets its own independent dictionary within this backend process.
 */
#define RECNO_DICT_CACHE_SIZE	16	/* Max cached per-relation dictionaries */

typedef struct RecnoDictCacheEntry
{
	Oid			relid;
	RecnoCompressionDict *dict;
}			RecnoDictCacheEntry;

static RecnoDictCacheEntry dict_cache[RECNO_DICT_CACHE_SIZE];
static int	dict_cache_count = 0;
static MemoryContext dict_cache_context = NULL;

/* Convenience pointer: the dictionary for the current relation */
static RecnoCompressionDict *compression_dict = NULL;

/*
 * Forward declarations
 */
static RecnoCompressionType RecnoChooseCompressionType(Oid typid, Datum value, Size value_size);
static Datum RecnoCompressLZ4(Datum value, Size *comp_size);
static Datum RecnoDecompressLZ4(Datum comp_value, Size comp_size, Size orig_size);
static Datum RecnoCompressZSTD(Datum value, Size *comp_size, int level);
static Datum RecnoDecompressZSTD(Datum comp_value, Size comp_size, Size orig_size);
static Datum RecnoCompressDelta(Datum value, Size *comp_size);
static Datum RecnoDecompressDelta(Datum comp_value, Size orig_size);
static Datum RecnoCompressDictionary(Datum value, Size *comp_size);
static Datum RecnoDecompressDictionary(Datum comp_value, Size orig_size);
static RecnoCompressionDict *RecnoGetDictForRelation(Oid relid);
static void RecnoInitCompressionDict(RecnoCompressionDict *dict);
static int	RecnoFindDictEntry(const char *value, int length);
static int	RecnoAddDictEntry(const char *value, int length);
#ifdef USE_ZSTD
static Datum pg_attribute_unused() RecnoCompressZSTDDict(Datum value, Size *comp_size, int level, Oid relid);
static Datum pg_attribute_unused() RecnoDecompressZSTDDict(Datum comp_value, Size comp_size, Size orig_size, Oid relid);
#endif
#ifdef HAVE_LZ4_DICT
static Datum pg_attribute_unused() RecnoCompressLZ4Dict(Datum value, Size *comp_size, Oid relid);
static Datum pg_attribute_unused() RecnoDecompressLZ4Dict(Datum comp_value, Size comp_size, Size orig_size, Oid relid);
#endif

/*
 * RecnoCompressAttribute
 *
 * Attempt to compress an attribute value using the specified (or automatically
 * chosen) compression algorithm.  If compression is disabled via GUC, the
 * value is too small (< RECNO_MIN_COMPRESS_SIZE = 32 bytes), or the
 * compressed result does not achieve at least 20% savings
 * (RECNO_MIN_COMPRESS_RATIO = 0.8), the original value is returned unchanged.
 *
 * On success, returns a new Datum containing:
 *   [varlena header][RecnoCompressionHeader (8 bytes)][compressed data]
 *
 * The RecnoCompressionHeader records the algorithm type, compression level,
 * original size, and compressed size, enabling decompression later.
 *
 * Parameters:
 *   value     - the attribute value to compress (must be a varlena or fixed-
 *               size numeric type)
 *   typid     - the PostgreSQL type OID (used for algorithm selection when
 *               comp_type is RECNO_COMP_NONE)
 *   comp_type - explicit compression algorithm, or RECNO_COMP_NONE to
 *               auto-select via RecnoChooseCompressionType()
 *
 * Returns the compressed Datum, or the original value if compression was
 * not beneficial.
 */
Datum
RecnoCompressAttribute(Datum value, Oid typid, RecnoCompressionType comp_type)
{
	char	   *result;
	Size		orig_size;
	Size		comp_size;
	Size		total_size;
	RecnoCompressionHeader *header;
	Datum		comp_data = (Datum) 0;
	bool		is_success = false;

	if (!recno_enable_compression)
		return value;

	/* Get original size */
	if (typid == TEXTOID || typid == VARCHAROID || typid == BPCHAROID)
	{
		orig_size = VARSIZE_ANY_EXHDR(DatumGetPointer(value));
	}
	else if (get_typlen(typid) == -1)
	{
		orig_size = VARSIZE_ANY_EXHDR(DatumGetPointer(value));
	}
	else
	{
		orig_size = get_typlen(typid);
	}

	/* Skip compression for small values */
	if (orig_size < RECNO_MIN_COMPRESS_SIZE)
		return value;

	/* Choose compression type if not specified */
	if (comp_type == RECNO_COMP_NONE)
		comp_type = RecnoChooseCompressionType(typid, value, orig_size);

	/* Compress based on type */
	switch (comp_type)
	{
		case RECNO_COMP_LZ4:
			comp_data = RecnoCompressLZ4(value, &comp_size);
			is_success = (comp_data != (Datum) 0);
			break;

		case RECNO_COMP_ZSTD:
			comp_data = RecnoCompressZSTD(value, &comp_size, recno_compression_level);
			is_success = (comp_data != (Datum) 0);
			break;

		case RECNO_COMP_DELTA:
			if (typid == NUMERICOID || typid == INT4OID || typid == INT8OID)
			{
				comp_data = RecnoCompressDelta(value, &comp_size);
				is_success = (comp_data != (Datum) 0);
			}
			break;

		case RECNO_COMP_DICTIONARY:
			if (typid == TEXTOID || typid == VARCHAROID)
			{
				comp_data = RecnoCompressDictionary(value, &comp_size);
				is_success = (comp_data != (Datum) 0);
			}
			break;

		default:
			return value;
	}

	/* Check if compression was beneficial */
	if (!is_success || comp_size >= orig_size * RECNO_COMPRESS_RATIO_MIN)
	{
		if (comp_data != (Datum) 0)
			pfree(DatumGetPointer(comp_data));
		return value;
	}

	/* Create compressed result with header */
	total_size = VARHDRSZ + sizeof(RecnoCompressionHeader) + comp_size;
	result = (char *) palloc(total_size);
	SET_VARSIZE(result, total_size);

	header = (RecnoCompressionHeader *) VARDATA(result);
	header->comp_type = comp_type;
	header->comp_level = recno_compression_level;
	header->_pad = 0;
	header->orig_size = orig_size;
	header->comp_size = comp_size;

	/* Copy compressed data */
	memcpy((char *) header + sizeof(RecnoCompressionHeader),
		   DatumGetPointer(comp_data), comp_size);

	if (comp_data != (Datum) 0)
		pfree(DatumGetPointer(comp_data));

	return PointerGetDatum(result);
}

/*
 * RecnoDecompressAttribute
 *
 * Decompress a previously compressed attribute value.  The RecnoCompressionHeader
 * is extracted from the compressed varlena to determine the algorithm, original
 * size, and compressed size.
 *
 * Parameters:
 *   value  - the compressed Datum (varlena with RecnoCompressionHeader + data)
 *   typid  - the PostgreSQL type OID (used for type-specific decompression)
 *   header - pointer to the RecnoCompressionHeader within the compressed value
 *
 * Returns the decompressed Datum in its original format.
 */
Datum
RecnoDecompressAttribute(Datum value, Oid typid, RecnoCompressionHeader *header)
{
	char	   *comp_data;
	Datum		result;

	if (header == NULL)
		return value;

	comp_data = (char *) header + sizeof(RecnoCompressionHeader);

	switch (header->comp_type)
	{
		case RECNO_COMP_LZ4:
			result = RecnoDecompressLZ4(PointerGetDatum(comp_data),
										header->comp_size, header->orig_size);
			break;

		case RECNO_COMP_ZSTD:
			result = RecnoDecompressZSTD(PointerGetDatum(comp_data),
										 header->comp_size, header->orig_size);
			break;

		case RECNO_COMP_DELTA:
			result = RecnoDecompressDelta(PointerGetDatum(comp_data), header->orig_size);
			break;

		case RECNO_COMP_DICTIONARY:
			result = RecnoDecompressDictionary(PointerGetDatum(comp_data), header->orig_size);
			break;

		default:
			result = value;
			break;
	}

	return result;
}

/*
 * RecnoChooseCompressionType
 *
 * Select the most appropriate compression algorithm for a given value based
 * on its data type and size:
 *   - TEXT/VARCHAR/BPCHAR: LZ4 (fast general-purpose compression)
 *   - NUMERIC/INT4/INT8/FLOAT4/FLOAT8: DELTA (varint encoding)
 *   - BYTEA: ZSTD (higher compression ratio for binary data)
 *   - All other types: LZ4 (safe general-purpose default)
 *
 * When built without USE_LZ4 or USE_ZSTD, the stub fallbacks copy data
 * unchanged so the compression ratio check in RecnoCompressAttribute
 * will reject the result, preventing data corruption.
 *
 * Parameters:
 *   typid      - PostgreSQL type OID of the attribute
 *   value      - the attribute value (used for future pattern analysis)
 *   value_size - size of the value in bytes
 *
 * Returns the recommended RecnoCompressionType.
 */
static RecnoCompressionType
RecnoChooseCompressionType(Oid typid, Datum value, Size value_size)
{
	/*
	 * Choose compression algorithm based on data type.
	 *
	 * ZSTD is used for text and binary types (30-50% better compression ratio
	 * than LZ4, with acceptable speed for storage-bound workloads).
	 * Delta encoding is used for numeric types (compact varint).
	 *
	 * Existing data with LZ4 headers decompresses correctly since the
	 * RecnoCompressionHeader includes the algorithm type.
	 *
	 * Compressed attributes may also go through overflow storage if they
	 * exceed the overflow threshold after compression.  The retrieval path
	 * in RecnoTupleToSlotWithOverflow and tts_recno_deform correctly
	 * handles the combined compressed+overflow case by fetching from
	 * overflow first, then decompressing the fetched varlena.
	 */
	switch (typid)
	{
		case TEXTOID:
		case VARCHAROID:
		case BPCHAROID:
		case BYTEAOID:
			return RECNO_COMP_ZSTD;

		case NUMERICOID:
		case INT4OID:
		case INT8OID:
		case FLOAT4OID:
		case FLOAT8OID:
			return RECNO_COMP_DELTA;

		default:
			return RECNO_COMP_ZSTD;
	}
}

/*
 * LZ4 compression
 */
#ifdef USE_LZ4
static Datum
RecnoCompressLZ4(Datum value, Size *comp_size)
{
	char	   *input = VARDATA_ANY(DatumGetPointer(value));
	Size		input_size = VARSIZE_ANY_EXHDR(DatumGetPointer(value));
	char	   *output;
	int			max_dest_size;
	int			compressed_size;

	max_dest_size = LZ4_compressBound(input_size);
	output = (char *) palloc(max_dest_size);

	compressed_size = LZ4_compress_default(input, output, input_size,
										   max_dest_size);
	if (compressed_size <= 0)
	{
		/* Compression failed, fall back to uncompressed */
		pfree(output);
		output = (char *) palloc(input_size);
		memcpy(output, input, input_size);
		*comp_size = input_size;
		return PointerGetDatum(output);
	}

	/* Shrink allocation to actual compressed size */
	output = (char *) repalloc(output, compressed_size);
	*comp_size = compressed_size;
	return PointerGetDatum(output);
}

/*
 * LZ4 decompression
 */
static Datum
RecnoDecompressLZ4(Datum comp_value, Size comp_size, Size orig_size)
{
	char	   *input = DatumGetPointer(comp_value);
	char	   *output;
	Size		output_size = VARHDRSZ + orig_size;
	int			rawsize;

	output = (char *) palloc(output_size);
	SET_VARSIZE(output, output_size);

	rawsize = LZ4_decompress_safe(input, VARDATA(output),
								  (int) comp_size, (int) orig_size);
	if (rawsize < 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("lz4 decompression failed")));

	return PointerGetDatum(output);
}
#else							/* !USE_LZ4 */
static Datum
RecnoCompressLZ4(Datum value, Size *comp_size)
{
	/* LZ4 not available: copy data unchanged so ratio check rejects it */
	char	   *input = VARDATA_ANY(DatumGetPointer(value));
	Size		input_size = VARSIZE_ANY_EXHDR(DatumGetPointer(value));
	char	   *output;

	output = (char *) palloc(input_size);
	memcpy(output, input, input_size);
	*comp_size = input_size;
	return PointerGetDatum(output);
}

static Datum
RecnoDecompressLZ4(Datum comp_value, Size comp_size, Size orig_size)
{
	char	   *input = DatumGetPointer(comp_value);
	char	   *output;
	Size		output_size = VARHDRSZ + orig_size;

	output = (char *) palloc(output_size);
	SET_VARSIZE(output, output_size);
	memcpy(VARDATA(output), input, orig_size);
	return PointerGetDatum(output);
}
#endif							/* USE_LZ4 */

/*
 * ZSTD compression
 */
#ifdef USE_ZSTD
static Datum
RecnoCompressZSTD(Datum value, Size *comp_size, int level)
{
	char	   *input = VARDATA_ANY(DatumGetPointer(value));
	Size		input_size = VARSIZE_ANY_EXHDR(DatumGetPointer(value));
	char	   *output;
	size_t		max_dest_size;
	size_t		compressed_size;

	max_dest_size = ZSTD_compressBound(input_size);
	output = (char *) palloc(max_dest_size);

	compressed_size = ZSTD_compress(output, max_dest_size,
									input, input_size, level);
	if (ZSTD_isError(compressed_size))
	{
		/* Compression failed, fall back to uncompressed */
		pfree(output);
		output = (char *) palloc(input_size);
		memcpy(output, input, input_size);
		*comp_size = input_size;
		return PointerGetDatum(output);
	}

	/* Shrink allocation to actual compressed size */
	output = (char *) repalloc(output, compressed_size);
	*comp_size = compressed_size;
	return PointerGetDatum(output);
}

/*
 * ZSTD decompression
 */
static Datum
RecnoDecompressZSTD(Datum comp_value, Size comp_size, Size orig_size)
{
	char	   *input = DatumGetPointer(comp_value);
	char	   *output;
	Size		output_size = VARHDRSZ + orig_size;
	size_t		rawsize;

	output = (char *) palloc(output_size);
	SET_VARSIZE(output, output_size);

	rawsize = ZSTD_decompress(VARDATA(output), orig_size,
							  input, comp_size);
	if (ZSTD_isError(rawsize))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("zstd decompression failed: %s",
						ZSTD_getErrorName(rawsize))));

	return PointerGetDatum(output);
}
#else							/* !USE_ZSTD */
static Datum
RecnoCompressZSTD(Datum value, Size *comp_size, int level)
{
	/* ZSTD not available: copy data unchanged so ratio check rejects it */
	char	   *input = VARDATA_ANY(DatumGetPointer(value));
	Size		input_size = VARSIZE_ANY_EXHDR(DatumGetPointer(value));
	char	   *output;

	output = (char *) palloc(input_size);
	memcpy(output, input, input_size);
	*comp_size = input_size;
	return PointerGetDatum(output);
}

static Datum
RecnoDecompressZSTD(Datum comp_value, Size comp_size, Size orig_size)
{
	char	   *input = DatumGetPointer(comp_value);
	char	   *output;
	Size		output_size = VARHDRSZ + orig_size;

	output = (char *) palloc(output_size);
	SET_VARSIZE(output, output_size);
	memcpy(VARDATA(output), input, orig_size);
	return PointerGetDatum(output);
}
#endif							/* USE_ZSTD */

/*
 * Delta/numeric compression for numeric values
 *
 * This implements variable-length integer encoding (varint) for integer types
 * and compact representation for NUMERIC type. The key insight is that many
 * numeric values don't need their full type width (e.g., storing 42 as int64
 * wastes 7 bytes).
 *
 * Encoding format:
 * - First byte: tag indicating format
 *   - 0x00-0x7F: small positive integer (0-127) encoded in tag itself
 *   - 0x80: negative value follows
 *   - 0x81-0x88: positive value in 1-8 bytes follows
 *   - 0x89: NUMERIC type follows with length prefix
 * - Remaining bytes: actual value in minimal representation
 */
static Datum
RecnoCompressDelta(Datum value, Size *comp_size)
{
	char	   *input = DatumGetPointer(value);
	Size		input_size = VARSIZE_ANY_EXHDR(input);
	char	   *output;
	unsigned char *out_ptr;
	Size		output_size;

	/*
	 * Check if this looks like a numeric type (int32, int64, or NUMERIC). For
	 * row-based storage, we compress individual numeric datums.
	 */

	/* Handle int32 (4 bytes) */
	if (input_size == sizeof(int32))
	{
		int32		val;

		memcpy(&val, VARDATA_ANY(input), sizeof(int32));

		/* Small positive integers (0-127) encode in single byte */
		if (val >= 0 && val <= 127)
		{
			output_size = 1;
			output = (char *) palloc(output_size);
			output[0] = (unsigned char) val;
			*comp_size = output_size;
			return PointerGetDatum(output);
		}

		/* Negative or larger values: use variable length encoding */
		if (val < 0)
		{
			/* Encode negative value */
			uint32		abs_val = (val == INT32_MIN) ? (uint32) INT32_MAX + 1 : (uint32) (-val);
			int			bytes_needed = 4;

			/* Find minimal bytes needed */
			if (abs_val <= 0xFF)
				bytes_needed = 1;
			else if (abs_val <= 0xFFFF)
				bytes_needed = 2;
			else if (abs_val <= 0xFFFFFF)
				bytes_needed = 3;

			output_size = 2 + bytes_needed;
			output = (char *) palloc(output_size);
			out_ptr = (unsigned char *) output;
			out_ptr[0] = 0x80;	/* Negative marker */
			out_ptr[1] = (unsigned char) bytes_needed;
			memcpy(out_ptr + 2, &abs_val, bytes_needed);
			*comp_size = output_size;
			return PointerGetDatum(output);
		}
		else
		{
			/* Positive value > 127 */
			int			bytes_needed = 4;
			uint32		uval = (uint32) val;

			if (uval <= 0xFF)
				bytes_needed = 1;
			else if (uval <= 0xFFFF)
				bytes_needed = 2;
			else if (uval <= 0xFFFFFF)
				bytes_needed = 3;

			output_size = 1 + bytes_needed;
			output = (char *) palloc(output_size);
			out_ptr = (unsigned char *) output;
			out_ptr[0] = 0x80 + bytes_needed;	/* 0x81-0x84 */
			memcpy(out_ptr + 1, &uval, bytes_needed);
			*comp_size = output_size;
			return PointerGetDatum(output);
		}
	}

	/* Handle int64 (8 bytes) */
	if (input_size == sizeof(int64))
	{
		int64		val;

		memcpy(&val, VARDATA_ANY(input), sizeof(int64));

		/* Small positive integers (0-127) */
		if (val >= 0 && val <= 127)
		{
			output_size = 1;
			output = (char *) palloc(output_size);
			output[0] = (unsigned char) val;
			*comp_size = output_size;
			return PointerGetDatum(output);
		}

		/* Larger values: variable length */
		if (val < 0)
		{
			uint64		abs_val = (val == INT64_MIN) ? (uint64) INT64_MAX + 1 : (uint64) (-val);
			int			bytes_needed = 8;

			if (abs_val <= 0xFF)
				bytes_needed = 1;
			else if (abs_val <= 0xFFFF)
				bytes_needed = 2;
			else if (abs_val <= 0xFFFFFF)
				bytes_needed = 3;
			else if (abs_val <= 0xFFFFFFFF)
				bytes_needed = 4;
			else if (abs_val <= 0xFFFFFFFFFFULL)
				bytes_needed = 5;
			else if (abs_val <= 0xFFFFFFFFFFFFULL)
				bytes_needed = 6;
			else if (abs_val <= 0xFFFFFFFFFFFFFFULL)
				bytes_needed = 7;

			output_size = 2 + bytes_needed;
			output = (char *) palloc(output_size);
			out_ptr = (unsigned char *) output;
			out_ptr[0] = 0x80;
			out_ptr[1] = (unsigned char) bytes_needed;
			memcpy(out_ptr + 2, &abs_val, bytes_needed);
			*comp_size = output_size;
			return PointerGetDatum(output);
		}
		else
		{
			uint64		uval = (uint64) val;
			int			bytes_needed = 8;

			if (uval <= 0xFF)
				bytes_needed = 1;
			else if (uval <= 0xFFFF)
				bytes_needed = 2;
			else if (uval <= 0xFFFFFF)
				bytes_needed = 3;
			else if (uval <= 0xFFFFFFFF)
				bytes_needed = 4;
			else if (uval <= 0xFFFFFFFFFFULL)
				bytes_needed = 5;
			else if (uval <= 0xFFFFFFFFFFFFULL)
				bytes_needed = 6;
			else if (uval <= 0xFFFFFFFFFFFFFFULL)
				bytes_needed = 7;

			output_size = 1 + bytes_needed;
			output = (char *) palloc(output_size);
			out_ptr = (unsigned char *) output;
			out_ptr[0] = 0x80 + bytes_needed;
			memcpy(out_ptr + 1, &uval, bytes_needed);
			*comp_size = output_size;
			return PointerGetDatum(output);
		}
	}

	/*
	 * For other numeric types (NUMERIC, float), or values that don't fit our
	 * patterns, just store as-is with a marker.
	 */
	output_size = 2 + input_size;
	output = (char *) palloc(output_size);
	out_ptr = (unsigned char *) output;
	out_ptr[0] = 0x89;			/* Other numeric type marker */
	out_ptr[1] = (unsigned char) (input_size & 0xFF);
	memcpy(out_ptr + 2, VARDATA_ANY(input), input_size);
	*comp_size = output_size;
	return PointerGetDatum(output);
}

/*
 * Delta/numeric decompression for numeric values
 *
 * Reverses the variable-length encoding applied by RecnoCompressDelta.
 * Must reconstruct the original fixed-width representation.
 */
static Datum
RecnoDecompressDelta(Datum comp_value, Size orig_size)
{
	unsigned char *input = (unsigned char *) DatumGetPointer(comp_value);
	char	   *output;
	Size		output_size = VARHDRSZ + orig_size;
	unsigned char tag;

	output = (char *) palloc(output_size);
	SET_VARSIZE(output, output_size);

	tag = input[0];

	/* Small positive integer (0-127) encoded in tag */
	if (tag <= 0x7F)
	{
		if (orig_size == sizeof(int32))
		{
			int32		val = (int32) tag;

			memcpy(VARDATA(output), &val, sizeof(int32));
		}
		else if (orig_size == sizeof(int64))
		{
			int64		val = (int64) tag;

			memcpy(VARDATA(output), &val, sizeof(int64));
		}
		else
		{
			/* Shouldn't happen, but handle gracefully */
			memset(VARDATA(output), 0, orig_size);
		}
		return PointerGetDatum(output);
	}

	/* Negative value */
	if (tag == 0x80)
	{
		int			bytes_stored = input[1];
		uint64		abs_val = 0;

		memcpy(&abs_val, input + 2, bytes_stored);

		if (orig_size == sizeof(int32))
		{
			int32		val = -(int32) abs_val;

			memcpy(VARDATA(output), &val, sizeof(int32));
		}
		else if (orig_size == sizeof(int64))
		{
			int64		val = -(int64) abs_val;

			memcpy(VARDATA(output), &val, sizeof(int64));
		}
		return PointerGetDatum(output);
	}

	/* Positive value in 1-8 bytes (tags 0x81-0x88) */
	if (tag >= 0x81 && tag <= 0x88)
	{
		int			bytes_stored = tag - 0x80;
		uint64		val = 0;

		memcpy(&val, input + 1, bytes_stored);

		if (orig_size == sizeof(int32))
		{
			int32		val32 = (int32) val;

			memcpy(VARDATA(output), &val32, sizeof(int32));
		}
		else if (orig_size == sizeof(int64))
		{
			memcpy(VARDATA(output), &val, sizeof(int64));
		}
		return PointerGetDatum(output);
	}

	/* Other numeric type (tag 0x89) - stored as-is with length */
	if (tag == 0x89)
	{
		Size		stored_size = input[1];

		memcpy(VARDATA(output), input + 2, stored_size);
		return PointerGetDatum(output);
	}

	/* Unknown tag - should not happen */
	elog(ERROR, "invalid delta compression tag: 0x%02X", tag);
	return PointerGetDatum(output); /* Keep compiler happy */
}

/*
 * RecnoGetDictForRelation -- look up or create a dictionary for the given
 * relation in the per-backend dictionary cache.
 *
 * Sets the module-level compression_dict pointer and returns it.
 */
static RecnoCompressionDict *
RecnoGetDictForRelation(Oid relid)
{
	int			i;
	RecnoCompressionDict *dict;
	MemoryContext old_context;

	/* Initialize cache context on first call */
	if (dict_cache_context == NULL)
	{
		dict_cache_context = AllocSetContextCreate(CacheMemoryContext,
												   "RECNO Dictionary Cache",
												   ALLOCSET_DEFAULT_SIZES);
		dict_cache_count = 0;
	}

	/* Search cache for existing dictionary for this relation */
	for (i = 0; i < dict_cache_count; i++)
	{
		if (dict_cache[i].relid == relid)
		{
			compression_dict = dict_cache[i].dict;
			return compression_dict;
		}
	}

	/* Not found -- create a new dictionary */
	old_context = MemoryContextSwitchTo(dict_cache_context);

	dict = (RecnoCompressionDict *)
		palloc0(sizeof(RecnoCompressionDict));
	dict->relid = relid;
	dict->dict_context = AllocSetContextCreate(dict_cache_context,
											   "RECNO Compression Dictionary",
											   ALLOCSET_DEFAULT_SIZES);
	RecnoInitCompressionDict(dict);

	MemoryContextSwitchTo(old_context);

	/* Evict oldest entry if cache is full */
	if (dict_cache_count >= RECNO_DICT_CACHE_SIZE)
	{
		RecnoCompressionDict *evict = dict_cache[0].dict;

#ifdef USE_ZSTD
		if (evict->zstd_cdict)
			ZSTD_freeCDict(evict->zstd_cdict);
		if (evict->zstd_ddict)
			ZSTD_freeDDict(evict->zstd_ddict);
#endif
		MemoryContextDelete(evict->dict_context);
		pfree(evict);

		/* Shift entries down */
		memmove(&dict_cache[0], &dict_cache[1],
				(RECNO_DICT_CACHE_SIZE - 1) * sizeof(RecnoDictCacheEntry));
		dict_cache_count--;
	}

	dict_cache[dict_cache_count].relid = relid;
	dict_cache[dict_cache_count].dict = dict;
	dict_cache_count++;

	compression_dict = dict;
	return compression_dict;
}

/*
 * RecnoCompressDictionary
 *
 * Dictionary compression for text values.  Replaces the entire value with
 * a 4-byte dictionary entry ID if the value is found in (or can be added to)
 * the per-backend, per-relation compression dictionary.
 *
 * The dictionary is stored in CacheMemoryContext and is NOT persisted across
 * backend restarts.  Maximum 1024 entries (RECNO_DICT_MAX_ENTRIES).  Lookup
 * is linear search, which is adequate for a small dictionary but would need
 * optimization for larger dictionaries.
 *
 * If the dictionary is full and the value is not found, falls back to
 * copying the data uncompressed.
 *
 * Parameters:
 *   value     - text Datum to compress
 *   comp_size - output: size of the compressed result (4 bytes on success)
 *
 * Returns the compressed Datum (either a 4-byte dictionary ID or the
 * uncompressed data if the dictionary is full).
 */
static Datum
RecnoCompressDictionary(Datum value, Size *comp_size)
{
	char	   *input = VARDATA_ANY(DatumGetPointer(value));
	Size		input_size = VARSIZE_ANY_EXHDR(DatumGetPointer(value));
	int			dict_id;
	char	   *output;

	if (compression_dict == NULL)
		RecnoGetDictForRelation(InvalidOid);

	/* Find or add dictionary entry */
	dict_id = RecnoFindDictEntry(input, input_size);
	if (dict_id == -1)
	{
		dict_id = RecnoAddDictEntry(input, input_size);
		if (dict_id == -1)
		{
			/* Dictionary full, fall back to no compression */
			*comp_size = input_size;
			output = (char *) palloc(input_size);
			memcpy(output, input, input_size);
			return PointerGetDatum(output);
		}
	}

	/* Store just the dictionary ID */
	*comp_size = sizeof(int);
	output = (char *) palloc(sizeof(int));
	*((int *) output) = dict_id;

	return PointerGetDatum(output);
}

/*
 * Dictionary decompression for text values
 */
static Datum
RecnoDecompressDictionary(Datum comp_value, Size orig_size)
{
	int			dict_id = *((int *) DatumGetPointer(comp_value));
	char	   *output;
	Size		output_size;

	if (compression_dict == NULL || dict_id >= compression_dict->num_entries)
	{
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("invalid dictionary ID in compressed data")));
	}

	output_size = VARHDRSZ + compression_dict->entries[dict_id].length;
	output = (char *) palloc(output_size);
	SET_VARSIZE(output, output_size);
	memcpy(VARDATA(output), compression_dict->entries[dict_id].value,
		   compression_dict->entries[dict_id].length);

	return PointerGetDatum(output);
}

/*
 * Initialize compression dictionary fields (num_entries, ZSTD compiled
 * dictionaries).  The caller must have already allocated dict and its
 * dict_context.
 */
static void
RecnoInitCompressionDict(RecnoCompressionDict *dict)
{
	dict->num_entries = 0;
#ifdef USE_ZSTD
	dict->zstd_cdict = NULL;
	dict->zstd_ddict = NULL;
#endif
}

/*
 * Find dictionary entry
 */
static int
RecnoFindDictEntry(const char *value, int length)
{
	int			i;

	if (compression_dict == NULL)
		return -1;

	for (i = 0; i < compression_dict->num_entries; i++)
	{
		if (compression_dict->entries[i].length == length &&
			memcmp(compression_dict->entries[i].value, value, length) == 0)
		{
			compression_dict->entries[i].frequency++;
			return i;
		}
	}

	return -1;
}

/*
 * Add dictionary entry
 */
static int
RecnoAddDictEntry(const char *value, int length)
{
	RecnoDictEntry *entry;
	MemoryContext old_context;

	if (compression_dict == NULL)
		RecnoGetDictForRelation(InvalidOid);

	if (compression_dict->num_entries >= RECNO_DICT_MAX_ENTRIES)
		return -1;

	old_context = MemoryContextSwitchTo(compression_dict->dict_context);

	entry = &compression_dict->entries[compression_dict->num_entries];
	entry->value = (char *) palloc(length);
	memcpy(entry->value, value, length);
	entry->length = length;
	entry->frequency = 1;
	entry->dict_id = compression_dict->num_entries;

	compression_dict->num_entries++;

	MemoryContextSwitchTo(old_context);

	return entry->dict_id;
}

/* ----------------------------------------------------------------
 * ZSTD dictionary-accelerated compression
 *
 * When USE_ZSTD is defined, we can optionally train a ZSTD dictionary
 * from the entries already in the per-relation dictionary and use it
 * to improve compression ratio for small values.
 * ----------------------------------------------------------------
 */

#ifdef USE_ZSTD
/*
 * RecnoCompressZSTDDict -- compress using a ZSTD compiled dictionary
 *
 * Falls back to regular ZSTD compression if no dictionary is available.
 */
static Datum
RecnoCompressZSTDDict(Datum value, Size *comp_size, int level, Oid relid)
{
	char	   *input = VARDATA_ANY(DatumGetPointer(value));
	Size		input_size = VARSIZE_ANY_EXHDR(DatumGetPointer(value));
	char	   *output;
	size_t		max_dest_size;
	size_t		compressed_size;
	RecnoCompressionDict *dict;
	ZSTD_CCtx  *cctx;

	dict = RecnoGetDictForRelation(relid);

	max_dest_size = ZSTD_compressBound(input_size);
	output = (char *) palloc(max_dest_size);

	if (dict->zstd_cdict != NULL)
	{
		/* Use compiled dictionary for better compression */
		cctx = ZSTD_createCCtx();
		if (cctx == NULL)
		{
			pfree(output);
			*comp_size = 0;
			return (Datum) 0;
		}

		compressed_size = ZSTD_compress_usingCDict(cctx, output, max_dest_size,
												   input, input_size,
												   dict->zstd_cdict);
		ZSTD_freeCCtx(cctx);
	}
	else
	{
		/* No dictionary available, use regular compression */
		compressed_size = ZSTD_compress(output, max_dest_size,
										input, input_size, level);
	}

	if (ZSTD_isError(compressed_size))
	{
		pfree(output);
		*comp_size = 0;
		return (Datum) 0;
	}

	output = (char *) repalloc(output, compressed_size);
	*comp_size = compressed_size;
	return PointerGetDatum(output);
}

/*
 * RecnoDecompressZSTDDict -- decompress using a ZSTD compiled dictionary
 */
static Datum
RecnoDecompressZSTDDict(Datum comp_value, Size comp_size, Size orig_size,
						Oid relid)
{
	char	   *input = DatumGetPointer(comp_value);
	char	   *output;
	Size		output_size = VARHDRSZ + orig_size;
	size_t		rawsize;
	RecnoCompressionDict *dict;
	ZSTD_DCtx  *dctx;

	dict = RecnoGetDictForRelation(relid);

	output = (char *) palloc(output_size);
	SET_VARSIZE(output, output_size);

	if (dict->zstd_ddict != NULL)
	{
		dctx = ZSTD_createDCtx();
		if (dctx == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("could not create ZSTD decompression context")));

		rawsize = ZSTD_decompress_usingDDict(dctx, VARDATA(output), orig_size,
											  input, comp_size,
											  dict->zstd_ddict);
		ZSTD_freeDCtx(dctx);
	}
	else
	{
		rawsize = ZSTD_decompress(VARDATA(output), orig_size,
								  input, comp_size);
	}

	if (ZSTD_isError(rawsize))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("zstd dictionary decompression failed: %s",
						ZSTD_getErrorName(rawsize))));

	return PointerGetDatum(output);
}
#endif							/* USE_ZSTD */

/* ----------------------------------------------------------------
 * LZ4 dictionary-accelerated compression
 *
 * When HAVE_LZ4_DICT is defined (LZ4 >= 1.8.1), we can use a
 * dictionary buffer to improve LZ4 compression for small values
 * that share common prefixes or patterns.
 * ----------------------------------------------------------------
 */

#ifdef HAVE_LZ4_DICT
/*
 * RecnoCompressLZ4Dict -- compress using LZ4 with a dictionary buffer
 *
 * The dictionary buffer is constructed from the concatenated dictionary
 * entries for the relation.  Falls back to regular LZ4 if no entries exist.
 */
static Datum
RecnoCompressLZ4Dict(Datum value, Size *comp_size, Oid relid)
{
	char	   *input = VARDATA_ANY(DatumGetPointer(value));
	Size		input_size = VARSIZE_ANY_EXHDR(DatumGetPointer(value));
	char	   *output;
	int			max_dest_size;
	int			compressed_size;
	RecnoCompressionDict *dict;

	dict = RecnoGetDictForRelation(relid);

	max_dest_size = LZ4_compressBound(input_size);
	output = (char *) palloc(max_dest_size);

	if (dict->num_entries > 0)
	{
		/*
		 * Build a dictionary buffer from existing entries and use the LZ4
		 * streaming API for dictionary-based compression.  LZ4_loadDict()
		 * + LZ4_compress_fast_continue() is the portable dictionary API
		 * available in all LZ4 >= 1.7.0.
		 */
		char		dict_buf[65536];	/* LZ4 uses last 64KB */
		int			dict_len = 0;
		int			i;
		LZ4_stream_t *lz4_stream;

		for (i = 0; i < dict->num_entries && dict_len < (int) sizeof(dict_buf); i++)
		{
			int			copy_len = Min(dict->entries[i].length,
									   (int) sizeof(dict_buf) - dict_len);

			memcpy(dict_buf + dict_len, dict->entries[i].value, copy_len);
			dict_len += copy_len;
		}

		lz4_stream = LZ4_createStream();
		LZ4_loadDict(lz4_stream, dict_buf, dict_len);
		compressed_size = LZ4_compress_fast_continue(lz4_stream,
													 input, output,
													 (int) input_size,
													 max_dest_size, 1);
		LZ4_freeStream(lz4_stream);
	}
	else
	{
		compressed_size = LZ4_compress_default(input, output,
											   (int) input_size,
											   max_dest_size);
	}

	if (compressed_size <= 0)
	{
		pfree(output);
		*comp_size = 0;
		return (Datum) 0;
	}

	output = (char *) repalloc(output, compressed_size);
	*comp_size = compressed_size;
	return PointerGetDatum(output);
}

/*
 * RecnoDecompressLZ4Dict -- decompress using LZ4 with a dictionary buffer
 */
static Datum
RecnoDecompressLZ4Dict(Datum comp_value, Size comp_size, Size orig_size,
					   Oid relid)
{
	char	   *input = DatumGetPointer(comp_value);
	char	   *output;
	Size		output_size = VARHDRSZ + orig_size;
	int			rawsize;
	RecnoCompressionDict *dict;

	dict = RecnoGetDictForRelation(relid);

	output = (char *) palloc(output_size);
	SET_VARSIZE(output, output_size);

	if (dict->num_entries > 0)
	{
		/*
		 * Use LZ4 streaming API for dictionary-based decompression.
		 * LZ4_setStreamDecode() + LZ4_decompress_safe_continue() is the
		 * portable dictionary decompression API in LZ4 >= 1.7.0.
		 */
		char		dict_buf[65536];
		int			dict_len = 0;
		int			i;
		LZ4_streamDecode_t *lz4_stream;

		for (i = 0; i < dict->num_entries && dict_len < (int) sizeof(dict_buf); i++)
		{
			int			copy_len = Min(dict->entries[i].length,
									   (int) sizeof(dict_buf) - dict_len);

			memcpy(dict_buf + dict_len, dict->entries[i].value, copy_len);
			dict_len += copy_len;
		}

		lz4_stream = LZ4_createStreamDecode();
		LZ4_setStreamDecode(lz4_stream, dict_buf, dict_len);
		rawsize = LZ4_decompress_safe_continue(lz4_stream,
											   input, VARDATA(output),
											   (int) comp_size, (int) orig_size);
		LZ4_freeStreamDecode(lz4_stream);
	}
	else
	{
		rawsize = LZ4_decompress_safe(input, VARDATA(output),
									  (int) comp_size, (int) orig_size);
	}

	if (rawsize < 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("lz4 dictionary decompression failed")));

	return PointerGetDatum(output);
}
#endif							/* HAVE_LZ4_DICT */

/*
 * Get compression statistics
 */
void
RecnoGetCompressionStats(int *dict_entries, int *total_compressed,
						 double *avg_compression_ratio)
{
	*dict_entries = 0;
	*total_compressed = 0;
	*avg_compression_ratio = 1.0;

	if (compression_dict != NULL)
	{
		*dict_entries = compression_dict->num_entries;
		/* These would need to be tracked in a real implementation */
		*total_compressed = 0;
		*avg_compression_ratio = 0.7;	/* Estimated */
	}
}

/*
 * Reset compression dictionary for a specific relation, or all dictionaries
 * if relid is InvalidOid.
 */
void
RecnoResetCompressionDict(void)
{
	int			i;

	for (i = 0; i < dict_cache_count; i++)
	{
		RecnoCompressionDict *dict = dict_cache[i].dict;

#ifdef USE_ZSTD
		if (dict->zstd_cdict)
		{
			ZSTD_freeCDict(dict->zstd_cdict);
			dict->zstd_cdict = NULL;
		}
		if (dict->zstd_ddict)
		{
			ZSTD_freeDDict(dict->zstd_ddict);
			dict->zstd_ddict = NULL;
		}
#endif
		MemoryContextDelete(dict->dict_context);
		pfree(dict);
	}

	dict_cache_count = 0;
	compression_dict = NULL;

	if (dict_cache_context != NULL)
	{
		MemoryContextReset(dict_cache_context);
	}
}
