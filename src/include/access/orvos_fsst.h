/*
 * orvos_fsst.h
 *		FSST (Fast Static Symbol Table) string compression for orvos.
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
 * Copyright (c) 2019-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/include/access/orvos_fsst.h
 */
#ifndef ORVOS_FSST_H
#define ORVOS_FSST_H

#include "c.h"					/* for uint8, uint16, uint32 */

/*
 * Maximum symbol length (bytes).  FSST uses up to 8-byte symbols.
 */
#define FSST_MAX_SYMBOL_LEN		8

/*
 * Number of entries in the symbol table.  Codes 0-255 map to symbols.
 * Code 255 is reserved as an escape byte: the next byte is a literal.
 */
#define FSST_NUM_SYMBOLS		256
#define FSST_ESCAPE				255

/*
 * Symbol table entry.  Each entry maps a single-byte code to a multi-byte
 * sequence.
 */
typedef struct FsstSymbol
{
	uint8		len;						/* symbol length (1-8), 0 = unused */
	uint8		bytes[FSST_MAX_SYMBOL_LEN]; /* the symbol bytes */
} FsstSymbol;

/*
 * Complete FSST symbol table.  This is stored persistently and used
 * for both encoding and decoding.
 */
typedef struct FsstSymbolTable
{
	uint32		magic;			/* FSST_MAGIC for validation */
	uint16		num_symbols;	/* number of valid symbols (max 255) */
	uint16		padding;
	FsstSymbol	symbols[FSST_NUM_SYMBOLS];
} FsstSymbolTable;

#define FSST_MAGIC		0x46535354	/* 'FSST' */

/*
 * Build a symbol table from a set of input strings.
 *
 * Analyzes the given strings to find frequently occurring byte sequences
 * and constructs a symbol table optimized for compressing similar data.
 *
 * strings:    Array of pointers to string data.
 * lengths:    Array of string lengths.
 * nstrings:   Number of strings.
 *
 * Returns a newly allocated FsstSymbolTable (in CurrentMemoryContext).
 */
extern FsstSymbolTable *fsst_build_symbol_table(const char **strings,
												const int *lengths,
												int nstrings);

/*
 * Compress a buffer using the given symbol table.
 *
 * src:         Input data.
 * srcSize:     Size of input data.
 * dst:         Output buffer (must be at least srcSize * 2 bytes).
 * dstCapacity: Size of output buffer.
 * table:       The symbol table to use.
 *
 * Returns the compressed size, or 0 if compression did not help
 * (compressed >= original).
 */
extern int	fsst_compress(const char *src, int srcSize,
						  char *dst, int dstCapacity,
						  const FsstSymbolTable *table);

/*
 * Decompress a buffer using the given symbol table.
 *
 * src:              Compressed data.
 * compressedSize:   Size of compressed data.
 * dst:              Output buffer.
 * dstCapacity:      Size of output buffer.
 * table:            The symbol table used for compression.
 *
 * Returns the decompressed size.  Raises ERROR on failure.
 */
extern int	fsst_decompress(const char *src, int compressedSize,
							char *dst, int dstCapacity,
							const FsstSymbolTable *table);

#endif							/* ORVOS_FSST_H */
