/**
 * @file orvos_compression.h
 * @brief Compression/decompression interface for Orvos attribute pages.
 *
 * Orvos compresses the variable-length portion of attribute B-tree leaf
 * pages (TID codewords + null bitmap + datum data).  The compression
 * algorithm is selected at build time based on configure flags:
 *
 * - zstd (preferred, --with-zstd): best compression ratio and speed.
 * - LZ4 (--with-lz4): very fast with good ratios.
 * - pglz (built-in fallback): significantly slower.
 *
 * The buffer cache stores compressed blocks; decompression is done
 * on-the-fly in backend-private memory when reading.
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/include/access/orvos_compression.h
 */
#ifndef ORVOS_COMPRESSION_H
#define ORVOS_COMPRESSION_H

/**
 * @brief Attempt to compress data from @a src into @a dst.
 *
 * Uses the build-time-selected algorithm (zstd > LZ4 > pglz).
 * Compression is only considered successful if the compressed output
 * is strictly smaller than the input.
 *
 * @param src         Source data buffer.
 * @param dst         Destination buffer for compressed output.
 * @param srcSize     Size of source data in bytes.
 * @param dstCapacity Maximum size of the destination buffer.
 * @return Compressed size in bytes, or 0 if compression did not reduce
 *         size (or failed).  Negative on allocation error (pglz only).
 */
extern int	ov_try_compress(const char *src, char *dst, int srcSize, int dstCapacity);

/**
 * @brief Decompress data from @a src into @a dst.
 *
 * The caller must provide the exact uncompressed size.  Raises an
 * ERROR on decompression failure or size mismatch.
 *
 * @param src              Compressed data buffer.
 * @param dst              Destination buffer (must be at least @a uncompressedSize bytes).
 * @param compressedSize   Size of compressed data in bytes.
 * @param uncompressedSize Expected size of decompressed output.
 */
extern void ov_decompress(const char *src, char *dst, int compressedSize, int uncompressedSize);

/*
 * FSST-aware compression for string columns.
 *
 * These apply FSST encoding as a pre-filter before the general-purpose
 * compressor.  The FsstSymbolTable must have been built during the
 * B-tree build phase by analyzing string column values.
 *
 * When table is NULL or has no symbols, these fall back to the
 * standard ov_try_compress / ov_decompress.
 */
struct FsstSymbolTable;

extern int	ov_try_compress_with_fsst(const char *src, char *dst,
									  int srcSize, int dstCapacity,
									  const struct FsstSymbolTable *table);

extern void ov_decompress_with_fsst(const char *src, char *dst,
									int compressedSize, int uncompressedSize,
									const struct FsstSymbolTable *table);

#endif							/* ORVOS_COMPRESSION_H */
