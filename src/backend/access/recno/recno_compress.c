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
#include <zdict.h>
#endif

#include "access/recno.h"
#include "access/recno_dict.h"
#include "access/relscan.h"
#include "access/tableam.h"
#include "catalog/pg_type.h"
#include "executor/tuptable.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/fmgrprotos.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/numeric.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
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
int			recno_compression_algorithm = RECNO_COMP_ALGO_AUTO;
bool		recno_enable_compression = true;
double		recno_compression_min_ratio = 0.8;	/* Minimum compression ratio */

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
 * Note: Dictionary storage is currently backend-local and non-persistent.
 * A future enhancement could store dictionaries in a catalog table or
 * shared UNDO-log metadata (requiring WAL-logged dictionary mutations)
 * so they are visible to all backends and survive restarts.
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
	ZSTD_CDict *zstd_cdict;		/* Compiled ZSTD compression dictionary */
	ZSTD_DDict *zstd_ddict;		/* Compiled ZSTD decompression dictionary */
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
static RecnoCompressionDict * compression_dict = NULL;

/*
 * Per-backend cache of trained, persisted dictionary blobs.
 *
 * Unlike the process-local sampled dictionary above, these blobs are loaded
 * from the relation's RECNO_DICT_FORKNUM fork and are identical across all
 * backends, so dictionary-compressed data written by one backend is
 * decompressable by any other.  Keyed by (relid, dictid).
 */
typedef struct RecnoTrainedDictKey
{
	Oid			relid;
	uint32		dictid;
} RecnoTrainedDictKey;

typedef struct RecnoTrainedDict
{
	RecnoTrainedDictKey key;
	uint8		codec;			/* RecnoCompressionType the blob was trained for */
#ifdef USE_ZSTD
	ZSTD_CDict *zstd_cdict;		/* built lazily for compress */
	ZSTD_DDict *zstd_ddict;		/* built lazily for decompress */
#endif
	char	   *blob;			/* raw trained blob (LZ4 dict buffer / fallback) */
	uint32		blob_len;
} RecnoTrainedDict;

static RecnoTrainedDict *trained_cache[RECNO_DICT_CACHE_SIZE];
static int	trained_cache_count = 0;
static MemoryContext trained_cache_context = NULL;

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
static RecnoCompressionDict * RecnoGetDictForRelation(Oid relid);
static void RecnoInitCompressionDict(RecnoCompressionDict * dict);
static int	RecnoFindDictEntry(const char *value, int length);
static int	RecnoAddDictEntry(const char *value, int length);
static RecnoTrainedDict *RecnoGetTrainedDict(Oid relid, uint32 dictid);
#ifdef USE_ZSTD
static Datum RecnoCompressZSTDDict(Datum value, Size *comp_size, int level, RecnoTrainedDict *td);
static Datum RecnoDecompressZSTDDict(Datum comp_value, Size comp_size, Size orig_size, RecnoTrainedDict *td);
#endif
#ifdef HAVE_LZ4_DICT
static Datum RecnoCompressLZ4Dict(Datum value, Size *comp_size, RecnoTrainedDict *td);
static Datum RecnoDecompressLZ4Dict(Datum comp_value, Size comp_size, Size orig_size, RecnoTrainedDict *td);
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
RecnoCompressAttribute(Relation rel, Datum value, Oid typid,
					   RecnoCompressionType comp_type)
{
	char	   *result;
	Size		orig_size;
	Size		comp_size;
	Size		total_size;
	RecnoCompressionHeader *header;
	Datum		comp_data = (Datum) 0;
	bool		is_success = false;
	uint32		dictid = RECNO_DICT_INVALID_ID;

	if (!recno_enable_compression)
		return value;

	/*
	 * recno_compression_algorithm = 'none' disables compression regardless of
	 * the requested codec.  Checked here (not only in the chooser) so an
	 * explicit comp_type from a future caller is also honored.
	 */
	if (recno_compression_algorithm == RECNO_COMP_ALGO_OFF)
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

	/*
	 * For ZSTD/LZ4 codecs, consult the relation's active trained dictionary.
	 * dict_id stays RECNO_DICT_INVALID_ID (0) when there is no relation
	 * context or no active dictionary, which routes to the plain codec.
	 */
	if (rel != NULL &&
		(comp_type == RECNO_COMP_ZSTD || comp_type == RECNO_COMP_LZ4))
		dictid = recno_dict_get_active(rel);

	/* Compress based on type */
	switch (comp_type)
	{
		case RECNO_COMP_LZ4:
#ifdef HAVE_LZ4_DICT
			if (dictid != RECNO_DICT_INVALID_ID)
			{
				RecnoTrainedDict *td =
					RecnoGetTrainedDict(RelationGetRelid(rel), dictid);

				comp_data = RecnoCompressLZ4Dict(value, &comp_size, td);
				is_success = (comp_data != (Datum) 0);
				break;
			}
#endif
			dictid = RECNO_DICT_INVALID_ID;
			comp_data = RecnoCompressLZ4(value, &comp_size);
			is_success = (comp_data != (Datum) 0);
			break;

		case RECNO_COMP_ZSTD:
#ifdef USE_ZSTD
			if (dictid != RECNO_DICT_INVALID_ID)
			{
				RecnoTrainedDict *td =
					RecnoGetTrainedDict(RelationGetRelid(rel), dictid);

				comp_data = RecnoCompressZSTDDict(value, &comp_size,
												  recno_compression_level, td);
				is_success = (comp_data != (Datum) 0);
				break;
			}
#endif
			dictid = RECNO_DICT_INVALID_ID;
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
	if (!is_success || comp_size >= orig_size * recno_compression_min_ratio)
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
	header->dict_id = (uint16) dictid;
	header->orig_size = orig_size;
	header->comp_size = (uint32) comp_size;

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
RecnoDecompressAttribute(Oid relid, Datum value, Oid typid,
						 RecnoCompressionHeader *header)
{
	char	   *comp_data;
	Datum		result;

	if (header == NULL)
		return value;

	comp_data = (char *) header + sizeof(RecnoCompressionHeader);

	/*
	 * Fast path: ordinary (non-dictionary) compressed data.  This is the
	 * overwhelmingly common case and must touch neither the trained-dict
	 * cache nor open a relation.
	 */
	if (header->dict_id == RECNO_DICT_INVALID_ID)
	{
		switch (header->comp_type)
		{
			case RECNO_COMP_LZ4:
				return RecnoDecompressLZ4(PointerGetDatum(comp_data),
										  header->comp_size, header->orig_size);

			case RECNO_COMP_ZSTD:
				return RecnoDecompressZSTD(PointerGetDatum(comp_data),
										   header->comp_size, header->orig_size);

			case RECNO_COMP_DELTA:
				return RecnoDecompressDelta(PointerGetDatum(comp_data),
											header->orig_size);

			case RECNO_COMP_DICTIONARY:
				return RecnoDecompressDictionary(PointerGetDatum(comp_data),
												 header->orig_size);

			default:
				return value;
		}
	}

	/*
	 * Dictionary-compressed data: we need the relation's trained blob to
	 * decompress.  A missing relation context here means the datum is
	 * corrupt or came from a transient slot that cannot supply one.
	 */
	if (relid == InvalidOid)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("RECNO dict-compressed datum needs relation context")));

	switch (header->comp_type)
	{
		case RECNO_COMP_ZSTD:
#ifdef USE_ZSTD
			{
				RecnoTrainedDict *td = RecnoGetTrainedDict(relid, header->dict_id);

				result = RecnoDecompressZSTDDict(PointerGetDatum(comp_data),
												 header->comp_size,
												 header->orig_size, td);
			}
#else
			result = RecnoDecompressZSTD(PointerGetDatum(comp_data),
										 header->comp_size, header->orig_size);
#endif
			break;

		case RECNO_COMP_LZ4:
#ifdef HAVE_LZ4_DICT
			{
				RecnoTrainedDict *td = RecnoGetTrainedDict(relid, header->dict_id);

				result = RecnoDecompressLZ4Dict(PointerGetDatum(comp_data),
												header->comp_size,
												header->orig_size, td);
			}
#else
			result = RecnoDecompressLZ4(PointerGetDatum(comp_data),
										header->comp_size, header->orig_size);
#endif
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
 * The recno_compression_algorithm GUC overrides this: 'lz4' or 'zstd' force
 * that codec for every attribute, while 'auto' uses the per-type heuristic
 * below.  ('none' is handled earlier in RecnoCompressAttribute.)  Selecting a
 * codec whose library is not compiled in is impossible because the GUC enum
 * table omits unavailable codecs.
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
	/* An explicit codec from the GUC wins over the per-type heuristic. */
	if (recno_compression_algorithm == RECNO_COMP_ALGO_LZ4)
		return RECNO_COMP_LZ4;
	if (recno_compression_algorithm == RECNO_COMP_ALGO_ZSTD)
		return RECNO_COMP_ZSTD;

	/*
	 * Choose compression algorithm based on data type.
	 *
	 * ZSTD is used for text and binary types (30-50% better compression ratio
	 * than LZ4, with acceptable speed for storage-bound workloads). Delta
	 * encoding is used for numeric types (compact varint).
	 *
	 * Existing data with LZ4 headers decompresses correctly since the
	 * RecnoCompressionHeader includes the algorithm type.
	 *
	 * Compressed attributes may also go through overflow storage if they
	 * exceed the overflow threshold after compression.  The retrieval path in
	 * RecnoTupleToSlotWithOverflow and tts_recno_deform correctly handles the
	 * combined compressed+overflow case by fetching from overflow first, then
	 * decompressing the fetched varlena.
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
 * WARNING: Dictionary-compressed data MUST NOT be written to persistent
 * storage (pages on disk) because the dictionary is backend-local and
 * non-persistent.  Another backend or a restarted backend cannot decompress
 * the data.  This algorithm is currently safe because RecnoChooseCompressionType
 * never auto-selects RECNO_COMP_DICTIONARY — it can only be triggered by an
 * explicit caller passing comp_type=RECNO_COMP_DICTIONARY.
 *
 * Note: To enable dictionary compression for persistent storage, the
 * dictionary would need to be stored in a catalog table or shared-memory
 * structure that is WAL-logged and visible to all backends.
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
RecnoInitCompressionDict(RecnoCompressionDict * dict)
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
 * Persistent trained-dictionary cache
 *
 * Loads trained dictionary blobs from a relation's RECNO_DICT_FORKNUM fork
 * and caches them per backend, keyed by (relid, dictid).  The blobs are
 * identical across backends, so dictionary-compressed data is portable.
 * ----------------------------------------------------------------
 */

/*
 * RecnoGetTrainedDict -- look up, loading on miss, the trained dictionary
 * blob for (relid, dictid).  Never called for dictid 0.
 */
static RecnoTrainedDict *
RecnoGetTrainedDict(Oid relid, uint32 dictid)
{
	int			i;
	RecnoTrainedDict *td;
	MemoryContext old_context;
	Relation	rel;
	char	   *blob;
	uint8		codec;
	uint32		length;

	Assert(dictid != RECNO_DICT_INVALID_ID);

	if (trained_cache_context == NULL)
	{
		trained_cache_context =
			AllocSetContextCreate(CacheMemoryContext,
								  "RECNO Trained Dictionary Cache",
								  ALLOCSET_DEFAULT_SIZES);
		trained_cache_count = 0;
	}

	for (i = 0; i < trained_cache_count; i++)
	{
		if (trained_cache[i]->key.relid == relid &&
			trained_cache[i]->key.dictid == dictid)
			return trained_cache[i];
	}

	/* Miss: load the blob from the relation's dict fork. */
	rel = relation_open(relid, NoLock);

	old_context = MemoryContextSwitchTo(trained_cache_context);
	blob = recno_dict_read(rel, dictid, &codec, &length);

	td = (RecnoTrainedDict *) palloc0(sizeof(RecnoTrainedDict));
	td->key.relid = relid;
	td->key.dictid = dictid;
	td->codec = codec;
	td->blob = blob;
	td->blob_len = length;
#ifdef USE_ZSTD
	td->zstd_cdict = NULL;
	td->zstd_ddict = NULL;
#endif
	MemoryContextSwitchTo(old_context);

	relation_close(rel, NoLock);

	/* Evict the oldest entry if the cache is full. */
	if (trained_cache_count >= RECNO_DICT_CACHE_SIZE)
	{
		RecnoTrainedDict *evict = trained_cache[0];

#ifdef USE_ZSTD
		if (evict->zstd_cdict)
			ZSTD_freeCDict(evict->zstd_cdict);
		if (evict->zstd_ddict)
			ZSTD_freeDDict(evict->zstd_ddict);
#endif
		if (evict->blob)
			pfree(evict->blob);
		pfree(evict);

		memmove(&trained_cache[0], &trained_cache[1],
				(RECNO_DICT_CACHE_SIZE - 1) * sizeof(RecnoTrainedDict *));
		trained_cache_count--;
	}

	trained_cache[trained_cache_count++] = td;

	return td;
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
RecnoCompressZSTDDict(Datum value, Size *comp_size, int level,
					  RecnoTrainedDict *td)
{
	char	   *input = VARDATA_ANY(DatumGetPointer(value));
	Size		input_size = VARSIZE_ANY_EXHDR(DatumGetPointer(value));
	char	   *output;
	size_t		max_dest_size;
	size_t		compressed_size;
	ZSTD_CCtx  *cctx;

	/* Build the compiled compression dictionary lazily. */
	if (td->zstd_cdict == NULL && td->blob != NULL && td->blob_len > 0)
	{
		MemoryContext old_context = MemoryContextSwitchTo(trained_cache_context);

		td->zstd_cdict = ZSTD_createCDict(td->blob, td->blob_len, level);
		MemoryContextSwitchTo(old_context);
	}

	max_dest_size = ZSTD_compressBound(input_size);
	output = (char *) palloc(max_dest_size);

	if (td->zstd_cdict != NULL)
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
												   td->zstd_cdict);
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
						RecnoTrainedDict *td)
{
	char	   *input = DatumGetPointer(comp_value);
	char	   *output;
	Size		output_size = VARHDRSZ + orig_size;
	size_t		rawsize;
	ZSTD_DCtx  *dctx;

	/* Build the compiled decompression dictionary lazily. */
	if (td->zstd_ddict == NULL && td->blob != NULL && td->blob_len > 0)
	{
		MemoryContext old_context = MemoryContextSwitchTo(trained_cache_context);

		td->zstd_ddict = ZSTD_createDDict(td->blob, td->blob_len);
		MemoryContextSwitchTo(old_context);
	}

	output = (char *) palloc(output_size);
	SET_VARSIZE(output, output_size);

	if (td->zstd_ddict != NULL)
	{
		dctx = ZSTD_createDCtx();
		if (dctx == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("could not create ZSTD decompression context")));

		rawsize = ZSTD_decompress_usingDDict(dctx, VARDATA(output), orig_size,
											 input, comp_size,
											 td->zstd_ddict);
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
RecnoCompressLZ4Dict(Datum value, Size *comp_size, RecnoTrainedDict *td)
{
	char	   *input = VARDATA_ANY(DatumGetPointer(value));
	Size		input_size = VARSIZE_ANY_EXHDR(DatumGetPointer(value));
	char	   *output;
	int			max_dest_size;
	int			compressed_size;

	max_dest_size = LZ4_compressBound(input_size);
	output = (char *) palloc(max_dest_size);

	if (td->blob != NULL && td->blob_len > 0)
	{
		/*
		 * Use the trained blob as an LZ4 dictionary buffer via the streaming
		 * API: LZ4_loadDict() + LZ4_compress_fast_continue().
		 */
		LZ4_stream_t *lz4_stream;

		lz4_stream = LZ4_createStream();
		LZ4_loadDict(lz4_stream, td->blob, (int) td->blob_len);
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
					   RecnoTrainedDict *td)
{
	char	   *input = DatumGetPointer(comp_value);
	char	   *output;
	Size		output_size = VARHDRSZ + orig_size;
	int			rawsize;

	output = (char *) palloc(output_size);
	SET_VARSIZE(output, output_size);

	if (td->blob != NULL && td->blob_len > 0)
	{
		LZ4_streamDecode_t *lz4_stream;

		lz4_stream = LZ4_createStreamDecode();
		LZ4_setStreamDecode(lz4_stream, td->blob, (int) td->blob_len);
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

	/* Tear down the persistent trained-dictionary cache as well. */
	for (i = 0; i < trained_cache_count; i++)
	{
		RecnoTrainedDict *td = trained_cache[i];

#ifdef USE_ZSTD
		if (td->zstd_cdict)
			ZSTD_freeCDict(td->zstd_cdict);
		if (td->zstd_ddict)
			ZSTD_freeDDict(td->zstd_ddict);
#endif
		if (td->blob)
			pfree(td->blob);
		pfree(td);
	}

	trained_cache_count = 0;

	if (trained_cache_context != NULL)
		MemoryContextReset(trained_cache_context);
}

/*
 * build_zstd_dict_for_attribute(rel regclass, attnum int4) RETURNS int4
 *
 * Sample the existing (decompressed) values of a varlena attribute, train a
 * ZSTD compression dictionary over them, store it append-only in the
 * relation's dictionary fork, and publish it as the active dictionary for
 * new writes.  Returns the new dictionary id, or 0 if training was declined
 * (insufficient sample data) or ZSTD support is not compiled in.
 */
PG_FUNCTION_INFO_V1(build_zstd_dict_for_attribute);

Datum
build_zstd_dict_for_attribute(PG_FUNCTION_ARGS)
{
#ifdef USE_ZSTD
	Oid			relid = PG_GETARG_OID(0);
	int16		attnum = (int16) PG_GETARG_INT32(1);
	Relation	rel;
	TupleDesc	tupdesc;
	TableScanDesc scan;
	TupleTableSlot *slot;
	Form_pg_attribute att;

	/* Sample accumulation, capped to bound memory and training time. */
	const Size	max_total = 4 * 1024 * 1024;	/* ~4MB of sample bytes */
	const int	max_samples = 100000;
	char	   *sample_buf;
	Size		sample_cap;
	Size		total = 0;
	size_t	   *sample_sizes;
	int			nsamples = 0;

	/* Training output. */
	size_t		dict_cap;
	char	   *dict_buf;
	size_t		trained;
	uint32		dictid = 0;

	rel = relation_open(relid, AccessShareLock);
	tupdesc = RelationGetDescr(rel);

	if (attnum < 1 || attnum > tupdesc->natts)
	{
		relation_close(rel, AccessShareLock);
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid attribute number %d for relation \"%s\"",
						attnum, RelationGetRelationName(rel))));
	}

	att = TupleDescAttr(tupdesc, attnum - 1);
	if (att->attlen != -1)
	{
		relation_close(rel, AccessShareLock);
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("attribute %d of relation \"%s\" is not a varlena type",
						attnum, RelationGetRelationName(rel))));
	}

	sample_cap = max_total;
	sample_buf = (char *) palloc(sample_cap);
	sample_sizes = (size_t *) palloc(sizeof(size_t) * max_samples);

	slot = table_slot_create(rel, NULL);
	scan = table_beginscan(rel, GetActiveSnapshot(), 0, NULL, 0);

	while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
	{
		Datum		value;
		bool		isnull;
		struct varlena *detoasted;
		Size		len;

		CHECK_FOR_INTERRUPTS();

		value = slot_getattr(slot, attnum, &isnull);
		if (isnull)
			continue;

		detoasted = (struct varlena *) PG_DETOAST_DATUM(value);
		len = VARSIZE_ANY_EXHDR(detoasted);

		if (len == 0)
		{
			if ((Pointer) detoasted != DatumGetPointer(value))
				pfree(detoasted);
			continue;
		}

		if (total + len > sample_cap || nsamples >= max_samples)
		{
			if ((Pointer) detoasted != DatumGetPointer(value))
				pfree(detoasted);
			break;
		}

		memcpy(sample_buf + total, VARDATA_ANY(detoasted), len);
		sample_sizes[nsamples++] = len;
		total += len;

		if ((Pointer) detoasted != DatumGetPointer(value))
			pfree(detoasted);
	}

	table_endscan(scan);
	ExecDropSingleTupleTableSlot(slot);

	/* Need a reasonable corpus to train a useful dictionary. */
	if (nsamples < 10 || total == 0)
	{
		relation_close(rel, AccessShareLock);
		PG_RETURN_INT32(0);
	}

	dict_cap = Min((size_t) 112640, total / 100);
	if (dict_cap < 4096)
		dict_cap = 4096;
	dict_buf = (char *) palloc(dict_cap);

	trained = ZDICT_trainFromBuffer(dict_buf, dict_cap,
									sample_buf, sample_sizes,
									(unsigned) nsamples);

	if (ZDICT_isError(trained) || trained == 0)
	{
		relation_close(rel, AccessShareLock);
		PG_RETURN_INT32(0);
	}

	dictid = recno_dict_append(rel, RECNO_COMP_ZSTD, dict_buf,
							   (uint32) trained, (uint32) total, 0);
	recno_dict_set_active(rel, dictid);

	relation_close(rel, AccessShareLock);

	PG_RETURN_INT32((int32) dictid);
#else
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("ZSTD compression is not supported by this build")));
	PG_RETURN_INT32(0);
#endif
}
