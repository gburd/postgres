/*
 * orvos_compression.c
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
 *	  src/backend/access/orvos/orvos_compression.c
 */
#include "postgres.h"

#ifdef USE_ZSTD
#include <zstd.h>
#endif

#ifdef USE_LZ4
#include <lz4.h>
#endif

#include "access/orvos_compression.h"
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
ov_try_compress(const char *src, char *dst, int srcSize, int dstCapacity)
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
ov_decompress(const char *src, char *dst, int compressedSize, int uncompressedSize)
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
ov_try_compress(const char *src, char *dst, int srcSize, int dstCapacity)
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
ov_decompress(const char *src, char *dst, int compressedSize, int uncompressedSize)
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
ov_try_compress(const char *src, char *dst, int srcSize, int dstCapacity)
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
ov_decompress(const char *src, char *dst, int compressedSize, int uncompressedSize)
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
