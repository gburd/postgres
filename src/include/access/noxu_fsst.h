/**
 * @file noxu_fsst.h
 * @brief FSST (Fast Static Symbol Table) string compression for Noxu.
 *
 * FSST compresses string data by building a 256-entry symbol table of
 * frequently occurring byte sequences (1-8 bytes each).  During encoding,
 * multi-byte sequences in the input are replaced with single-byte codes,
 * achieving 30-60% additional compression on top of general-purpose
 * compressors like zstd.
 *
 * The symbol table is built by analyzing a sample of strings from the
 * column during B-tree build.  It is stored in the attribute metapage
 * and used for all items in that attribute tree.
 *
 * This is a self-contained implementation inspired by the FSST algorithm
 * described in Boncz et al., "FSST: Fast Random Access String Compression"
 * (VLDB 2020).
 *
 * @par Usage
 * 1. Build a symbol table from a representative sample of strings using
 *    fsst_build_symbol_table().
 * 2. Compress individual buffers using fsst_compress() with the table.
 * 3. Decompress using fsst_decompress() with the same table.
 *
 * @par Integration with Noxu
 * When NXBT_ATTR_FORMAT_FSST is set in an attribute item's t_flags,
 * the datum data has been FSST-encoded before general-purpose compression.
 * The compression pipeline calls nx_try_compress_with_fsst() and
 * nx_decompress_with_fsst() (declared in noxu_compression.h) which
 * apply FSST as a pre-filter.
 *
 * @par Serialization
 * Symbol tables can be serialized to a compact binary format for
 * persistent storage using fsst_serialize_table() and deserialized
 * with fsst_deserialize_table().
 *
 * Copyright (c) 2019-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/include/access/noxu_fsst.h
 */
#ifndef NOXU_FSST_H
#define NOXU_FSST_H

#include "c.h"					/* for uint8, uint16, uint32 */

/** @brief Maximum symbol length in bytes.  FSST uses up to 8-byte symbols. */
#define FSST_MAX_SYMBOL_LEN		8

/**
 * @brief Number of entries in the symbol table.
 *
 * Codes 0-254 map to symbols.  Code 255 is reserved as an escape byte:
 * the next byte in the compressed stream is a literal (unencoded) byte.
 */
#define FSST_NUM_SYMBOLS		256

/** @brief Escape code indicating the next byte is a literal. */
#define FSST_ESCAPE				255

/**
 * @brief A single FSST symbol table entry.
 *
 * Maps a single-byte code to a multi-byte sequence of up to
 * FSST_MAX_SYMBOL_LEN bytes.
 *
 * @param len    Symbol length (1-8 bytes), or 0 if the entry is unused.
 * @param bytes  The symbol byte sequence.
 */
typedef struct FsstSymbol
{
	uint8		len;						/* symbol length (1-8), 0 = unused */
	uint8		bytes[FSST_MAX_SYMBOL_LEN]; /* the symbol bytes */
} FsstSymbol;

/**
 * @brief Complete FSST symbol table.
 *
 * Stored persistently in the attribute metapage and used for both
 * encoding and decoding of string column data.
 *
 * @param magic        Validation magic number (FSST_MAGIC = 'FSST').
 * @param num_symbols  Number of valid symbols (at most 255; code 255
 *                     is reserved for escape).
 * @param symbols      Array of symbol entries indexed by code value.
 */
typedef struct FsstSymbolTable
{
	uint32		magic;			/* FSST_MAGIC for validation */
	uint16		num_symbols;	/* number of valid symbols (max 255) */
	uint16		padding;
	FsstSymbol	symbols[FSST_NUM_SYMBOLS];
} FsstSymbolTable;

/** @brief Magic number for FsstSymbolTable validation ('FSST' in ASCII). */
#define FSST_MAGIC		0x46535354	/* 'FSST' */

/**
 * @brief Build a symbol table from a set of input strings.
 *
 * Analyzes the given strings to find frequently occurring byte sequences
 * and constructs a symbol table optimized for compressing similar data.
 * The algorithm iteratively refines the symbol table over multiple passes.
 *
 * @param strings   Array of pointers to string data.
 * @param lengths   Array of string lengths (in bytes).
 * @param nstrings  Number of strings in the sample.
 * @return A newly allocated FsstSymbolTable (in CurrentMemoryContext).
 *         The caller is responsible for freeing it.
 */
extern FsstSymbolTable *fsst_build_symbol_table(const char **strings,
												const int *lengths,
												int nstrings);

/**
 * @brief Compress a buffer using the given symbol table.
 *
 * Replaces multi-byte sequences matching symbol table entries with
 * single-byte codes.  Unmatched bytes are escaped with FSST_ESCAPE
 * followed by the literal byte.
 *
 * @param src          Input data buffer.
 * @param srcSize      Size of input data in bytes.
 * @param dst          Output buffer (must be at least srcSize * 2 bytes
 *                     to handle worst-case expansion from escaping).
 * @param dstCapacity  Size of output buffer in bytes.
 * @param table        The symbol table to use for encoding.
 * @return Compressed size in bytes, or 0 if compression did not reduce
 *         size (compressed >= original).
 */
extern int	fsst_compress(const char *src, int srcSize,
						  char *dst, int dstCapacity,
						  const FsstSymbolTable *table);

/**
 * @brief Decompress a buffer using the given symbol table.
 *
 * Reverses the FSST encoding by expanding single-byte codes back to
 * their multi-byte symbol sequences.
 *
 * @param src              Compressed data buffer.
 * @param compressedSize   Size of compressed data in bytes.
 * @param dst              Output buffer for decompressed data.
 * @param dstCapacity      Size of output buffer in bytes.
 * @param table            The symbol table used during compression.
 * @return Decompressed size in bytes.  Raises ERROR on failure.
 */
extern int	fsst_decompress(const char *src, int compressedSize,
							char *dst, int dstCapacity,
							const FsstSymbolTable *table);

/**
 * @brief Serialize a symbol table into a compact binary format.
 *
 * The serialized format is:
 * @code
 *   [uint16 num_symbols] [for each symbol: uint8 len, uint8[len] bytes]
 * @endcode
 *
 * This compact format is used for persistent storage of the symbol table
 * in the attribute metapage.
 *
 * @param dst          Output buffer for the serialized data.
 * @param dstCapacity  Size of the output buffer in bytes.
 * @param table        The symbol table to serialize.
 * @return Serialized size in bytes, or 0 if the buffer is too small.
 */
extern int	fsst_serialize_table(char *dst, int dstCapacity,
								 const FsstSymbolTable *table);

/**
 * @brief Deserialize a symbol table from its compact binary format.
 *
 * Reconstructs a FsstSymbolTable from data produced by
 * fsst_serialize_table().
 *
 * @param src         Serialized symbol table data.
 * @param srcSize     Size of the serialized data in bytes.
 * @param bytes_read  Output: number of bytes consumed from @a src.
 * @return A newly allocated FsstSymbolTable (in CurrentMemoryContext),
 *         or NULL on failure (malformed data, buffer too small).
 */
extern FsstSymbolTable *fsst_deserialize_table(const char *src, int srcSize,
											   int *bytes_read);

/**
 * @brief Build a symbol table from a single contiguous buffer.
 *
 * Convenience wrapper around fsst_build_symbol_table() for the common
 * case where all strings are concatenated in a single buffer (e.g. the
 * datum data region of an attribute item).  Treats the entire buffer as
 * a single "string" for n-gram frequency analysis.
 *
 * @param data     Pointer to the string data buffer.
 * @param datalen  Length of the data in bytes.
 * @return A newly allocated FsstSymbolTable, or NULL if no useful
 *         symbols were found.
 */
extern FsstSymbolTable *fsst_build_symbol_table_from_buffer(const char *data,
															int datalen);

#endif							/* NOXU_FSST_H */
