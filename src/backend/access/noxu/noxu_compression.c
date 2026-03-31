/*
 * noxu_compression.c
 *		Routines for compression
 *
 * There are three implementations: zstd (preferred), LZ4, and the Postgres
 * pg_lzcompress() fallback. Zstd support requires --with-zstd, LZ4 requires
 * --with-lz4. If neither is available, pglz is used as a fallback.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/noxu/noxu_compression.c
 */
#include "postgres.h"

#ifdef USE_ZSTD
#include <zstd.h>
#endif

#ifdef USE_LZ4
#include <lz4.h>
#endif

#include "access/noxu_compression.h"
#include "common/pg_lzcompress.h"
#include "utils/datum.h"

/*
 * Compression preference order: zstd > lz4 > pglz
 * Zstd provides best compression ratio and speed for columnar data.
 * LZ4 is very fast with good compression.
 * pglz is the fallback when neither is available.
 */

#ifdef USE_ZSTD
/* Zstd implementation - preferred */

int
nx_try_compress(const char *src, char *dst, int srcSize, int dstCapacity)
{
	size_t		compressed_size;

	/*
	 * Use ZSTD_CLEVEL_DEFAULT (3) for a good balance of speed and compression.
	 * Columnar data compresses very well even at lower levels.
	 */
	compressed_size = ZSTD_compress(dst, dstCapacity, src, srcSize,
									ZSTD_CLEVEL_DEFAULT);

	if (ZSTD_isError(compressed_size))
		return 0;				/* compression failed */

	/*
	 * Only return compressed data if it's smaller than the original.
	 * This matches behavior of other compression methods.
	 */
	if (compressed_size >= (size_t) srcSize)
		return 0;

	return (int) compressed_size;
}

void
nx_decompress(const char *src, char *dst, int compressedSize, int uncompressedSize)
{
	size_t		decompressed_size;

	decompressed_size = ZSTD_decompress(dst, uncompressedSize, src, compressedSize);

	if (ZSTD_isError(decompressed_size))
		elog(ERROR, "zstd decompression failed: %s",
			 ZSTD_getErrorName(decompressed_size));

	if (decompressed_size != (size_t) uncompressedSize)
		elog(ERROR, "unexpected decompressed size: got %zu, expected %d",
			 decompressed_size, uncompressedSize);
}

#elif defined(USE_LZ4)
/* LZ4 implementation - second choice */

int
nx_try_compress(const char *src, char *dst, int srcSize, int dstCapacity)
{
	int			compressed_size;

	compressed_size = LZ4_compress_default(src, dst, srcSize, dstCapacity);

	if (compressed_size <= 0)
		return 0;				/* compression failed */

	/*
	 * Only return compressed data if it's smaller than the original.
	 */
	if (compressed_size >= srcSize)
		return 0;

	return compressed_size;
}

void
nx_decompress(const char *src, char *dst, int compressedSize, int uncompressedSize)
{
	int			decompressed_size;

	decompressed_size = LZ4_decompress_safe(src, dst, compressedSize, uncompressedSize);

	if (decompressed_size < 0)
		elog(ERROR, "lz4 decompression failed");

	if (decompressed_size != uncompressedSize)
		elog(ERROR, "unexpected decompressed size: got %d, expected %d",
			 decompressed_size, uncompressedSize);
}

#else
/* PGLZ implementation - fallback */

int
nx_try_compress(const char *src, char *dst, int srcSize, int dstCapacity)
{
	int			compressed_size;

	if (dstCapacity < PGLZ_MAX_OUTPUT(srcSize))
		return -1;

	compressed_size = pglz_compress(src, srcSize, dst, PGLZ_strategy_always);

	/*
	 * pglz_compress returns -1 on failure, or the compressed size.
	 * It may return a size >= srcSize if compression didn't help.
	 */
	if (compressed_size < 0 || compressed_size >= srcSize)
		return 0;

	return compressed_size;
}

void
nx_decompress(const char *src, char *dst, int compressedSize, int uncompressedSize)
{
	int			decompressed_size;

	decompressed_size = pglz_decompress(src, compressedSize, dst, uncompressedSize, true);

	if (decompressed_size < 0)
		elog(ERROR, "pglz decompression failed");

	if (decompressed_size != uncompressedSize)
		elog(ERROR, "unexpected decompressed size: got %d, expected %d",
			 decompressed_size, uncompressedSize);
}

#endif							/* compression implementation */

/*
 * FSST-aware compression for string columns.
 *
 * These functions apply FSST encoding as a pre-filter before the
 * general-purpose compressor (zstd/lz4/pglz).  The compressed format
 * when FSST is active:
 *
 *   [serialized symbol table] [int32: fsst_encoded_size]
 *   [general-compressed FSST-encoded data]
 *
 * The symbol table is embedded in the compressed payload so that
 * decompression is self-contained (no external symbol table storage
 * needed).  The caller is responsible for tracking whether FSST was
 * used (via the NXBT_ATTR_FORMAT_FSST flag in the item header).
 */
#include "access/noxu_fsst.h"

int
nx_try_compress_with_fsst(const char *src, char *dst, int srcSize,
						  int dstCapacity, const FsstSymbolTable *table)
{
	char	   *fsst_buf;
	int			fsst_size;
	int			table_size;
	int			final_size;
	int			hdr_size;

	if (table == NULL || table->num_symbols == 0)
		return nx_try_compress(src, dst, srcSize, dstCapacity);

	/* Allocate buffer for FSST-encoded data (worst case: 2x original) */
	fsst_buf = palloc(srcSize * 2);

	/* Apply FSST encoding */
	fsst_size = fsst_compress(src, srcSize, fsst_buf, srcSize * 2, table);

	if (fsst_size <= 0 || fsst_size >= srcSize)
	{
		/* FSST didn't help, fall back to direct compression */
		pfree(fsst_buf);
		return nx_try_compress(src, dst, srcSize, dstCapacity);
	}

	/*
	 * Serialize the symbol table as a prefix, followed by the
	 * FSST-encoded size, then the general-compressed FSST-encoded data.
	 */
	table_size = fsst_serialize_table(dst, dstCapacity, table);
	if (table_size <= 0)
	{
		pfree(fsst_buf);
		return 0;
	}

	hdr_size = table_size + (int) sizeof(int32);
	if (dstCapacity < hdr_size + 1)
	{
		pfree(fsst_buf);
		return 0;
	}

	memcpy(dst + table_size, &fsst_size, sizeof(int32));

	final_size = nx_try_compress(fsst_buf, dst + hdr_size,
								 fsst_size,
								 dstCapacity - hdr_size);

	pfree(fsst_buf);

	if (final_size <= 0)
		return 0;

	final_size += hdr_size;

	/* Only report success if we beat the original size */
	if (final_size >= srcSize)
		return 0;

	return final_size;
}

void
nx_decompress_with_fsst(const char *src, char *dst,
						int compressedSize, int uncompressedSize,
						const FsstSymbolTable *table_unused)
{
	FsstSymbolTable *table;
	int			table_bytes;
	int32		fsst_encoded_size;
	char	   *fsst_buf;
	int			decompressed_size;

	/*
	 * Deserialize the embedded symbol table from the compressed payload.
	 * The table_unused parameter is ignored; we always read the table
	 * from the payload for self-contained decompression.
	 */
	table = fsst_deserialize_table(src, compressedSize, &table_bytes);
	if (table == NULL)
	{
		/*
		 * If deserialization fails, this data was not FSST-compressed
		 * (shouldn't happen if the FSST flag is set correctly).
		 */
		nx_decompress(src, dst, compressedSize, uncompressedSize);
		return;
	}

	src += table_bytes;
	compressedSize -= table_bytes;

	/* Read the FSST-encoded size */
	if (compressedSize < (int) sizeof(int32))
		elog(ERROR, "FSST: truncated compressed data (no encoded size)");

	memcpy(&fsst_encoded_size, src, sizeof(int32));
	src += sizeof(int32);
	compressedSize -= sizeof(int32);

	/* Decompress the general-compressed FSST-encoded data */
	fsst_buf = palloc(fsst_encoded_size);
	nx_decompress(src, fsst_buf, compressedSize, fsst_encoded_size);

	/* Apply FSST decoding */
	decompressed_size = fsst_decompress(fsst_buf, fsst_encoded_size,
										dst, uncompressedSize, table);

	pfree(fsst_buf);
	pfree(table);

	if (decompressed_size != uncompressedSize)
		elog(ERROR, "FSST decompression size mismatch: got %d, expected %d",
			 decompressed_size, uncompressedSize);
}

/*
 * Self-contained FSST compression for an item payload.
 *
 * Builds an FSST symbol table from the data, applies FSST encoding as a
 * pre-filter, then compresses with the general-purpose compressor.
 * The symbol table is embedded in the output.
 *
 * Returns the compressed size, or 0 if compression didn't help.
 * Sets *used_fsst to true if FSST was applied.
 */
int
nx_try_compress_auto_fsst(const char *src, char *dst, int srcSize,
						  int dstCapacity, bool *used_fsst)
{
	FsstSymbolTable *table;
	int			fsst_compressed;
	int			plain_compressed;

	*used_fsst = false;

	/*
	 * Don't bother with FSST for small payloads -- the symbol table
	 * overhead would negate any savings.
	 */
	if (srcSize < 128)
		return nx_try_compress(src, dst, srcSize, dstCapacity);

	/* Build a symbol table from the payload data */
	table = fsst_build_symbol_table_from_buffer(src, srcSize);
	if (table == NULL)
		return nx_try_compress(src, dst, srcSize, dstCapacity);

	/* Try FSST + general compression */
	fsst_compressed = nx_try_compress_with_fsst(src, dst, srcSize,
												dstCapacity, table);

	if (fsst_compressed > 0)
	{
		/*
		 * Also try plain compression to see which is better.
		 * Use a temporary buffer for the comparison.
		 */
		char	   *plain_buf = palloc(dstCapacity);

		plain_compressed = nx_try_compress(src, plain_buf, srcSize,
										   dstCapacity);

		if (plain_compressed > 0 && plain_compressed <= fsst_compressed)
		{
			/* Plain compression is as good or better; use it instead */
			memcpy(dst, plain_buf, plain_compressed);
			pfree(plain_buf);
			pfree(table);
			return plain_compressed;
		}

		pfree(plain_buf);
		pfree(table);
		*used_fsst = true;
		return fsst_compressed;
	}

	pfree(table);

	/* FSST didn't help, fall back to plain compression */
	return nx_try_compress(src, dst, srcSize, dstCapacity);
}
