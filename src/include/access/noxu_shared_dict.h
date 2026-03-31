/*
 * noxu_shared_dict.h
 *		Shared dictionary compression for Noxu attribute B-trees
 *
 * Each attribute B-tree can optionally have a shared compression dictionary
 * trained from representative column data.  The dictionary is stored on
 * dedicated pages within the relation file, referenced from the metapage's
 * NXRootDirItem.  When present, the general-purpose compressor (zstd) uses
 * the dictionary for all items in that attribute's B-tree, complementing
 * (not replacing) existing pre-encodings (FSST, FOR, dict, etc.).
 *
 * The dictionary is compiled into ZSTD_CDict/ZSTD_DDict objects and cached
 * per-backend for the lifetime of the backend or until the dictionary
 * generation counter changes.
 *
 * On-disk storage:
 *   - Dictionary pages form a linked list (NXDictPageOpaque.nx_next)
 *   - Each page stores up to ~8100 bytes of dictionary data
 *   - A 100 KB dictionary requires ~13 pages
 *
 * Compressed items record which dictionary generation was used via
 * the NXBT_ATTR_SHARED_DICT flag and t_dict_generation field.
 *
 * Copyright (c) 2019-2026, PostgreSQL Global Development Group
 *
 * src/include/access/noxu_shared_dict.h
 */
#ifndef NOXU_SHARED_DICT_H
#define NOXU_SHARED_DICT_H

#include "c.h"
#include "access/noxu_tid.h"
#include "storage/block.h"
#include "storage/buf.h"
#include "utils/relcache.h"

#ifdef USE_ZSTD
#include <zstd.h>
#endif

/*
 * NX_DICT_PAGE_ID is defined in noxu_internal.h (0xF088).
 */

/*
 * NXBT_ATTR_SHARED_DICT (0x0800) is defined in noxu_internal.h.
 */

/*
 * Default and limit values for dictionary size.
 *
 * The zstd documentation recommends ~100 KB dictionaries.  We cap at
 * 256 KB to limit memory usage.
 */
#define NX_DICT_DEFAULT_SIZE	(100 * 1024)
#define NX_DICT_MIN_SIZE		(32 * 1024)
#define NX_DICT_MAX_SIZE		(256 * 1024)

/*
 * Minimum number of samples needed before training a dictionary.
 * The zstd documentation recommends a few thousand samples.
 */
#define NX_DICT_MIN_SAMPLES		100

/*
 * Target total sample size as a multiple of dictionary size.
 * Zstd recommends ~100x the dictionary size.
 */
#define NX_DICT_SAMPLE_MULTIPLIER	100

/*
 * Maximum data per dictionary page.
 * This is the usable space on a page after headers and opaque area.
 */
#define NX_DICT_PAGE_DATA_SIZE \
	(BLCKSZ - MAXALIGN(SizeOfPageHeaderData) - \
	 MAXALIGN(sizeof(NXDictPageOpaque)) - \
	 MAXALIGN(sizeof(NXDictPageHeader)))

/*
 * NXDictPageHeader
 *		Header at the beginning of each dictionary page's content area
 *
 * dict_generation: matches the generation counter in NXRootDirItem
 * total_dict_size: total dictionary size across all pages in the chain
 * chunk_offset: byte offset of this chunk within the full dictionary
 * num_pages: total number of pages in the dictionary chain
 */
typedef struct NXDictPageHeader
{
	uint32		dict_generation;
	uint32		total_dict_size;
	uint16		chunk_offset;
	uint16		num_pages;
} NXDictPageHeader;

/*
 * NXDictPageOpaque
 *		Opaque area at the end of each dictionary page
 *
 * nx_next: next page in the dictionary chain (InvalidBlockNumber if last)
 * nx_page_id: always NX_DICT_PAGE_ID
 */
typedef struct NXDictPageOpaque
{
	BlockNumber nx_next;
	uint16		padding;
	uint16		nx_page_id;		/* always NX_DICT_PAGE_ID */
} NXDictPageOpaque;

#define NXDictPageGetOpaque(page) \
	((NXDictPageOpaque *) PageGetSpecialPointer(page))

#define NXDictPageGetHeader(page) \
	((NXDictPageHeader *) PageGetContents(page))

/*
 * NXSharedDictData
 *		In-memory representation of a loaded shared dictionary
 *
 * Loaded lazily per-backend and cached.  The raw dictionary bytes are
 * kept so that compiled CDict/DDict can be rebuilt if needed.
 */
typedef struct NXSharedDictData
{
	Oid			relid;			/* relation OID */
	AttrNumber	attno;			/* attribute number */
	uint32		generation;		/* dictionary generation counter */
	char	   *raw_dict;		/* raw dictionary bytes (palloc'd) */
	size_t		dict_size;		/* size of raw_dict */
#ifdef USE_ZSTD
	ZSTD_CDict *cdict;			/* compiled compression dictionary */
	ZSTD_DDict *ddict;			/* compiled decompression dictionary */
	ZSTD_CCtx  *cctx;			/* reusable compression context */
	ZSTD_DCtx  *dctx;			/* reusable decompression context */
#endif
} NXSharedDictData;

/* ---- Public API ---- */

/*
 * nx_shared_dict_train
 *		Train a shared dictionary for the specified attribute
 *
 * Scans the attribute B-tree to collect sample data, then trains a
 * zstd dictionary using ZDICT_trainFromBuffer().  The resulting dictionary
 * is stored on dedicated pages and referenced from the metapage.
 *
 * rel: the Noxu relation (must be open with at least AccessShareLock)
 * attno: attribute number (1-based)
 * dict_capacity: target dictionary size in bytes (0 = default 100 KB)
 *
 * Returns the new dictionary generation counter, or 0 on failure.
 */
extern uint32 nx_shared_dict_train(Relation rel, AttrNumber attno,
								   size_t dict_capacity);

/*
 * nx_shared_dict_load
 *		Load the shared dictionary for an attribute into backend-local cache
 *
 * Returns a pointer to the cached NXSharedDictData, or NULL if no
 * dictionary exists for this attribute.  The returned pointer is valid
 * for the backend's lifetime or until the dictionary is invalidated.
 *
 * rel: the Noxu relation
 * attno: attribute number (1-based)
 */
extern NXSharedDictData *nx_shared_dict_load(Relation rel, AttrNumber attno);

/*
 * nx_shared_dict_invalidate
 *		Invalidate the cached dictionary for an attribute
 *
 * Called when the dictionary generation counter changes (e.g., after
 * dictionary rebuild).
 *
 * relid: relation OID
 * attno: attribute number (1-based), or InvalidAttrNumber to invalidate all
 */
extern void nx_shared_dict_invalidate(Oid relid, AttrNumber attno);

/*
 * nx_try_compress_with_shared_dict
 *		Compress data using a shared dictionary
 *
 * Uses ZSTD_compress_usingCDict() for compression with the pre-compiled
 * dictionary.  Returns compressed size, or 0 if compression didn't help.
 *
 * src: source data buffer
 * dst: destination buffer
 * srcSize: source data size in bytes
 * dstCapacity: destination buffer capacity
 * dict: shared dictionary data (must not be NULL)
 */
extern int nx_try_compress_with_shared_dict(const char *src, char *dst,
											int srcSize, int dstCapacity,
											const NXSharedDictData *dict);

/*
 * nx_decompress_with_shared_dict
 *		Decompress data using a shared dictionary
 *
 * Uses ZSTD_decompress_usingDDict() for decompression with the pre-compiled
 * dictionary.
 *
 * src: compressed data buffer
 * dst: destination buffer
 * compressedSize: size of compressed data
 * uncompressedSize: expected decompressed size
 * dict: shared dictionary data (must not be NULL)
 */
extern void nx_decompress_with_shared_dict(const char *src, char *dst,
										   int compressedSize,
										   int uncompressedSize,
										   const NXSharedDictData *dict);

/*
 * nx_shared_dict_write_pages
 *		Write a trained dictionary to dedicated pages
 *
 * Allocates dictionary pages from the FPM and writes the dictionary data
 * in chunks.  Updates the metapage's NXRootDirItem for the attribute.
 *
 * rel: the Noxu relation (must hold appropriate locks)
 * attno: attribute number
 * dict_data: raw dictionary bytes
 * dict_size: size of dictionary data
 * generation: dictionary generation counter to assign
 *
 * Returns the block number of the first dictionary page.
 */
extern BlockNumber nx_shared_dict_write_pages(Relation rel, AttrNumber attno,
											  const char *dict_data,
											  size_t dict_size,
											  uint32 generation);

/*
 * nx_shared_dict_read_pages
 *		Read a dictionary from its dedicated pages
 *
 * Follows the page chain starting from first_page, assembling the
 * dictionary from chunks stored on each page.
 *
 * rel: the Noxu relation
 * first_page: block number of the first dictionary page
 * dict_size_out: output - total dictionary size
 *
 * Returns a palloc'd buffer containing the raw dictionary bytes,
 * or NULL if the pages are invalid.
 */
extern char *nx_shared_dict_read_pages(Relation rel, BlockNumber first_page,
									   size_t *dict_size_out);

/*
 * nx_shared_dict_get_generation
 *		Get the current dictionary generation for an attribute
 *
 * Returns 0 if no dictionary exists.
 */
extern uint32 nx_shared_dict_get_generation(Relation rel, AttrNumber attno);

/*
 * nx_shared_dict_exists
 *		Check whether a shared dictionary exists for an attribute
 */
extern bool nx_shared_dict_exists(Relation rel, AttrNumber attno);

#endif							/* NOXU_SHARED_DICT_H */
