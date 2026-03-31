/*
 * noxu_compression.h
 *		Compression/decompression interface for Noxu attribute pages
 *
 * Noxu compresses the variable-length portion of attribute B-tree leaf
 * pages (TID codewords + null bitmap + datum data).  The compression
 * algorithm is selected at build time based on configure flags:
 *
 * - zstd (preferred, --with-zstd): best compression ratio and speed
 * - LZ4 (--with-lz4): very fast with good ratios
 * - pglz (built-in fallback): significantly slower
 *
 * The buffer cache stores compressed blocks; decompression is done
 * on-the-fly in backend-private memory when reading.
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * src/include/access/noxu_compression.h
 */
#ifndef NOXU_COMPRESSION_H
#define NOXU_COMPRESSION_H

/*
 * nx_try_compress
 *		Attempt to compress data from src into dst
 *
 * Uses the build-time-selected algorithm (zstd > LZ4 > pglz).
 * Compression is only considered successful if the compressed output
 * is strictly smaller than the input.
 *
 * Returns compressed size in bytes, or 0 if compression did not reduce
 * size (or failed). Negative on allocation error (pglz only).
 *
 * src: source data buffer
 * dst: destination buffer for compressed output
 * srcSize: size of source data in bytes
 * dstCapacity: maximum size of the destination buffer
 */
extern int	nx_try_compress(const char *src, char *dst, int srcSize, int dstCapacity);

/*
 * nx_decompress
 *		Decompress data from src into dst
 *
 * The caller must provide the exact uncompressed size. Raises an
 * ERROR on decompression failure or size mismatch.
 *
 * src: compressed data buffer
 * dst: destination buffer (must be at least uncompressedSize bytes)
 * compressedSize: size of compressed data in bytes
 * uncompressedSize: expected size of decompressed output
 */
extern void nx_decompress(const char *src, char *dst, int compressedSize, int uncompressedSize);

/*
 * FSST-aware compression for string columns.
 *
 * These apply FSST encoding as a pre-filter before the general-purpose
 * compressor.  The symbol table is embedded in the compressed payload
 * so that decompression is self-contained.
 *
 * nx_try_compress_with_fsst: applies FSST encoding using the provided
 * symbol table, then compresses with the general compressor.  The symbol
 * table is serialized into the compressed output so it can be recovered
 * during decompression.  When table is NULL or has no symbols, falls
 * back to plain nx_try_compress().
 *
 * nx_decompress_with_fsst: reads the embedded symbol table from the
 * compressed payload and reverses the FSST encoding after general
 * decompression.  The table parameter is unused (the embedded table
 * is always used).
 */
struct FsstSymbolTable;

extern int	nx_try_compress_with_fsst(const char *src, char *dst,
									  int srcSize, int dstCapacity,
									  const struct FsstSymbolTable *table);

extern void nx_decompress_with_fsst(const char *src, char *dst,
									int compressedSize, int uncompressedSize,
									const struct FsstSymbolTable *table);

/*
 * Self-contained FSST compression for an item payload.
 *
 * Builds an FSST symbol table from the data itself, then applies FSST
 * encoding + general compression.  Returns the compressed size, or 0
 * if compression did not help.  Sets *used_fsst to true if FSST was
 * actually applied (vs. falling back to plain compression).
 *
 * This is the main entry point used by nxbt_compress_item() for
 * varlena string columns.
 */
extern int	nx_try_compress_auto_fsst(const char *src, char *dst,
									  int srcSize, int dstCapacity,
									  bool *used_fsst);

#endif							/* NOXU_COMPRESSION_H */
