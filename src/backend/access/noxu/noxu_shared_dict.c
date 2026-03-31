/*
 * noxu_shared_dict.c
 *		Shared dictionary compression for Noxu attribute B-trees
 *
 * This module implements per-attribute shared compression dictionaries that
 * improve compression ratios by training a zstd dictionary from column data
 * and reusing it across all items in an attribute B-tree.
 *
 * The dictionary is stored on dedicated pages within the relation file,
 * linked from the metapage's NXRootDirItem.  Each backend lazily loads and
 * caches the compiled CDict/DDict for the lifetime of the backend or until
 * the dictionary generation counter changes.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/noxu/noxu_shared_dict.c
 */
#include "postgres.h"

#ifdef USE_ZSTD
#include <zstd.h>
#include <zdict.h>
#endif

#include "access/noxu_compression.h"
#include "access/noxu_internal.h"
#include "access/noxu_shared_dict.h"
#include "access/noxu_wal.h"
#include "access/xloginsert.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "utils/memutils.h"
#include "utils/rel.h"

/*
 * Backend-local dictionary cache.
 *
 * We maintain a small fixed-size cache of compiled dictionaries.  Each
 * entry is keyed by (relid, attno, generation).  The cache is allocated
 * in TopMemoryContext so it persists for the backend's lifetime.
 *
 * The cache size is deliberately small: most backends only access a few
 * tables at a time, and dictionary loading is infrequent (once per
 * backend per attribute).
 */
#define NX_DICT_CACHE_SIZE	16

static NXSharedDictData dict_cache[NX_DICT_CACHE_SIZE];
static int	dict_cache_count = 0;
static bool dict_cache_initialized = false;

static void nx_dict_cache_init(void);
static NXSharedDictData *nx_dict_cache_lookup(Oid relid, AttrNumber attno);
static NXSharedDictData *nx_dict_cache_insert(Oid relid, AttrNumber attno,
											  uint32 generation,
											  char *raw_dict, size_t dict_size);
static void nx_dict_cache_evict(NXSharedDictData *entry);

/*
 * Initialize the dictionary cache on first use.
 */
static void
nx_dict_cache_init(void)
{
	if (!dict_cache_initialized)
	{
		memset(dict_cache, 0, sizeof(dict_cache));
		dict_cache_count = 0;
		dict_cache_initialized = true;
	}
}

/*
 * Look up a dictionary in the cache.
 *
 * Returns the cached entry if found and the generation matches, NULL otherwise.
 */
static NXSharedDictData *
nx_dict_cache_lookup(Oid relid, AttrNumber attno)
{
	nx_dict_cache_init();

	for (int i = 0; i < dict_cache_count; i++)
	{
		if (dict_cache[i].relid == relid &&
			dict_cache[i].attno == attno &&
			dict_cache[i].raw_dict != NULL)
		{
			return &dict_cache[i];
		}
	}
	return NULL;
}

/*
 * Insert a dictionary into the cache, evicting the oldest entry if full.
 */
static NXSharedDictData *
nx_dict_cache_insert(Oid relid, AttrNumber attno, uint32 generation,
					 char *raw_dict, size_t dict_size)
{
	NXSharedDictData *entry;

	nx_dict_cache_init();

	/* Check if we already have an entry for this (relid, attno) */
	entry = nx_dict_cache_lookup(relid, attno);
	if (entry != NULL)
	{
		/* Evict old entry first */
		nx_dict_cache_evict(entry);
	}
	else if (dict_cache_count < NX_DICT_CACHE_SIZE)
	{
		entry = &dict_cache[dict_cache_count++];
	}
	else
	{
		/* Evict the first entry (simple FIFO) */
		entry = &dict_cache[0];
		nx_dict_cache_evict(entry);
	}

	entry->relid = relid;
	entry->attno = attno;
	entry->generation = generation;

	/* Copy raw dictionary into TopMemoryContext */
	entry->raw_dict = MemoryContextAlloc(TopMemoryContext, dict_size);
	memcpy(entry->raw_dict, raw_dict, dict_size);
	entry->dict_size = dict_size;

#ifdef USE_ZSTD
	/* Compile the dictionary for compression and decompression */
	entry->cdict = ZSTD_createCDict(entry->raw_dict, entry->dict_size,
									ZSTD_CLEVEL_DEFAULT);
	entry->ddict = ZSTD_createDDict(entry->raw_dict, entry->dict_size);
	/* Create reusable contexts to avoid per-operation malloc/free cycles */
	entry->cctx = ZSTD_createCCtx();
	entry->dctx = ZSTD_createDCtx();

	if (entry->cdict == NULL || entry->ddict == NULL ||
		entry->cctx == NULL || entry->dctx == NULL)
	{
		elog(WARNING, "failed to compile shared dictionary or create contexts for attribute %d",
			 attno);
		nx_dict_cache_evict(entry);
		return NULL;
	}
#endif

	return entry;
}

/*
 * Evict a dictionary cache entry, freeing all resources.
 */
static void
nx_dict_cache_evict(NXSharedDictData *entry)
{
#ifdef USE_ZSTD
	if (entry->cdict != NULL)
	{
		ZSTD_freeCDict(entry->cdict);
		entry->cdict = NULL;
	}
	if (entry->ddict != NULL)
	{
		ZSTD_freeDDict(entry->ddict);
		entry->ddict = NULL;
	}
	if (entry->cctx != NULL)
	{
		ZSTD_freeCCtx(entry->cctx);
		entry->cctx = NULL;
	}
	if (entry->dctx != NULL)
	{
		ZSTD_freeDCtx(entry->dctx);
		entry->dctx = NULL;
	}
#endif
	if (entry->raw_dict != NULL)
	{
		pfree(entry->raw_dict);
		entry->raw_dict = NULL;
	}
	entry->relid = InvalidOid;
	entry->attno = InvalidAttrNumber;
	entry->generation = 0;
	entry->dict_size = 0;
}

/*
 * Write dictionary data to dedicated pages in the relation.
 *
 * Allocates pages from the FPM, writes dictionary data in chunks,
 * and returns the block number of the first page.
 */
BlockNumber
nx_shared_dict_write_pages(Relation rel, AttrNumber attno,
						   const char *dict_data, size_t dict_size,
						   uint32 generation)
{
	BlockNumber first_page = InvalidBlockNumber;
	Buffer		prev_buf = InvalidBuffer;
	size_t		offset = 0;
	uint16		num_pages;
	uint16		page_idx = 0;
	Buffer		metabuf;

	/* Calculate number of pages needed */
	num_pages = (dict_size + NX_DICT_PAGE_DATA_SIZE - 1) / NX_DICT_PAGE_DATA_SIZE;
	if (num_pages == 0)
		num_pages = 1;

	/* Read metapage for FPM allocation */
	metabuf = ReadBuffer(rel, NX_META_BLK);
	LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);

	while (offset < dict_size)
	{
		Buffer		buf;
		Page		page;
		NXDictPageHeader *hdr;
		NXDictPageOpaque *opaque;
		size_t		chunk_size;
		char	   *data_area;

		/* Allocate a new page */
		buf = nxpage_getnewbuf(rel, metabuf);

		page = BufferGetPage(buf);
		PageInit(page, BLCKSZ, sizeof(NXDictPageOpaque));

		/* Set up opaque area */
		opaque = NXDictPageGetOpaque(page);
		opaque->nx_next = InvalidBlockNumber;
		opaque->nx_page_id = NX_DICT_PAGE_ID;
		opaque->padding = 0;

		/* Write header */
		hdr = NXDictPageGetHeader(page);
		hdr->dict_generation = generation;
		hdr->total_dict_size = (uint32) dict_size;
		hdr->chunk_offset = (uint16) page_idx;
		hdr->num_pages = num_pages;

		/* Calculate chunk size */
		chunk_size = Min(dict_size - offset, NX_DICT_PAGE_DATA_SIZE);

		/* Write dictionary data after the header */
		data_area = (char *) hdr + MAXALIGN(sizeof(NXDictPageHeader));
		memcpy(data_area, dict_data + offset, chunk_size);

		/* Update pd_lower to reflect used space */
		((PageHeader) page)->pd_lower = (data_area + chunk_size) - (char *) page;

		START_CRIT_SECTION();

		MarkBufferDirty(buf);

		/* Link previous page to this one */
		if (prev_buf != InvalidBuffer)
		{
			Page		prev_page_ptr = BufferGetPage(prev_buf);
			NXDictPageOpaque *prev_opaque = NXDictPageGetOpaque(prev_page_ptr);

			prev_opaque->nx_next = BufferGetBlockNumber(buf);
			MarkBufferDirty(prev_buf);
		}

		END_CRIT_SECTION();

		if (first_page == InvalidBlockNumber)
			first_page = BufferGetBlockNumber(buf);

		if (prev_buf != InvalidBuffer)
			UnlockReleaseBuffer(prev_buf);

		prev_buf = buf;

		offset += chunk_size;
		page_idx++;
	}

	/* Release last page buffer */
	if (prev_buf != InvalidBuffer)
		UnlockReleaseBuffer(prev_buf);

	/* Update metapage with dictionary reference */
	{
		Page		metapage = BufferGetPage(metabuf);
		NXMetaPage *metapg = (NXMetaPage *) PageGetContents(metapage);

		START_CRIT_SECTION();

		metapg->tree_root_dir[attno].dict_page = first_page;
		metapg->tree_root_dir[attno].dict_generation = generation;

		MarkBufferDirty(metabuf);

		END_CRIT_SECTION();
	}

	UnlockReleaseBuffer(metabuf);

	/* Invalidate the metapage cache so it gets refreshed */
	nxmeta_invalidate_cache(rel);

	return first_page;
}

/*
 * Read dictionary data from dedicated pages.
 *
 * Follows the page chain, assembling the dictionary from chunks.
 */
char *
nx_shared_dict_read_pages(Relation rel, BlockNumber first_page,
						  size_t *dict_size_out)
{
	char	   *dict_data;
	size_t		total_size;
	size_t		offset = 0;
	BlockNumber blkno = first_page;

	if (first_page == InvalidBlockNumber)
	{
		*dict_size_out = 0;
		return NULL;
	}

	/* Read first page to get total size */
	{
		Buffer		buf = ReadBuffer(rel, first_page);
		Page		page;
		NXDictPageHeader *hdr;
		NXDictPageOpaque *opaque;

		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);
		opaque = NXDictPageGetOpaque(page);

		if (opaque->nx_page_id != NX_DICT_PAGE_ID)
		{
			UnlockReleaseBuffer(buf);
			*dict_size_out = 0;
			return NULL;
		}

		hdr = NXDictPageGetHeader(page);
		total_size = hdr->total_dict_size;
		UnlockReleaseBuffer(buf);
	}

	if (total_size == 0 || total_size > NX_DICT_MAX_SIZE)
	{
		*dict_size_out = 0;
		return NULL;
	}

	dict_data = palloc(total_size);

	/* Follow the page chain and assemble the dictionary */
	while (blkno != InvalidBlockNumber && offset < total_size)
	{
		Buffer		buf;
		Page		page;
		NXDictPageHeader *hdr;
		NXDictPageOpaque *opaque;
		char	   *data_area;
		size_t		chunk_size;

		buf = ReadBuffer(rel, blkno);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);

		opaque = NXDictPageGetOpaque(page);
		if (opaque->nx_page_id != NX_DICT_PAGE_ID)
		{
			UnlockReleaseBuffer(buf);
			pfree(dict_data);
			*dict_size_out = 0;
			return NULL;
		}

		hdr = NXDictPageGetHeader(page);
		data_area = (char *) hdr + MAXALIGN(sizeof(NXDictPageHeader));

		/* Calculate how much data is on this page */
		chunk_size = Min(total_size - offset, NX_DICT_PAGE_DATA_SIZE);

		memcpy(dict_data + offset, data_area, chunk_size);

		blkno = opaque->nx_next;
		offset += chunk_size;

		UnlockReleaseBuffer(buf);
	}

	*dict_size_out = total_size;
	return dict_data;
}

/*
 * Load and cache a shared dictionary for an attribute.
 */
NXSharedDictData *
nx_shared_dict_load(Relation rel, AttrNumber attno)
{
	NXMetaCacheData *metacache;
	NXSharedDictData *cached;
	BlockNumber dict_page;
	uint32		dict_gen;
	char	   *raw_dict;
	size_t		dict_size;

	metacache = nxmeta_get_cache(rel);

	if (attno >= metacache->cache_nattributes)
		return NULL;

	dict_page = metacache->cache_attrs[attno].dict_page;
	dict_gen = metacache->cache_attrs[attno].dict_generation;

	if (dict_page == InvalidBlockNumber || dict_gen == 0)
		return NULL;

	/* Check the cache first */
	cached = nx_dict_cache_lookup(RelationGetRelid(rel), attno);
	if (cached != NULL && cached->generation == dict_gen)
		return cached;

	/* Cache miss or stale: load from pages */
	raw_dict = nx_shared_dict_read_pages(rel, dict_page, &dict_size);
	if (raw_dict == NULL)
		return NULL;

	cached = nx_dict_cache_insert(RelationGetRelid(rel), attno,
								  dict_gen, raw_dict, dict_size);
	pfree(raw_dict);

	return cached;
}

/*
 * Invalidate cached dictionaries for a relation.
 */
void
nx_shared_dict_invalidate(Oid relid, AttrNumber attno)
{
	nx_dict_cache_init();

	for (int i = 0; i < dict_cache_count; i++)
	{
		if (dict_cache[i].relid == relid &&
			(attno == InvalidAttrNumber || dict_cache[i].attno == attno))
		{
			nx_dict_cache_evict(&dict_cache[i]);
		}
	}
}

/*
 * Check if a shared dictionary exists for an attribute.
 */
bool
nx_shared_dict_exists(Relation rel, AttrNumber attno)
{
	NXMetaCacheData *metacache = nxmeta_get_cache(rel);

	if (attno >= metacache->cache_nattributes)
		return false;

	return (metacache->cache_attrs[attno].dict_page != InvalidBlockNumber &&
			metacache->cache_attrs[attno].dict_generation > 0);
}

/*
 * Get the current dictionary generation for an attribute.
 */
uint32
nx_shared_dict_get_generation(Relation rel, AttrNumber attno)
{
	NXMetaCacheData *metacache = nxmeta_get_cache(rel);

	if (attno >= metacache->cache_nattributes)
		return 0;

	return metacache->cache_attrs[attno].dict_generation;
}

#ifdef USE_ZSTD

/*
 * Compress data using a shared dictionary.
 *
 * Uses the cached compression context from the dictionary cache to avoid
 * creating/destroying a new context on every compression operation, which
 * would cause severe heap fragmentation.
 */
int
nx_try_compress_with_shared_dict(const char *src, char *dst,
								 int srcSize, int dstCapacity,
								 const NXSharedDictData *dict)
{
	size_t		compressed_size;

	if (dict == NULL || dict->cdict == NULL || dict->cctx == NULL)
		return 0;

	/*
	 * Reset the context to clear any previous state while preserving
	 * parameters. This ensures consistent compression and better performance.
	 */
	ZSTD_CCtx_reset(dict->cctx, ZSTD_reset_session_only);

	/* Use the cached context - no malloc/free per operation */
	compressed_size = ZSTD_compress_usingCDict(dict->cctx, dst, dstCapacity,
											   src, srcSize, dict->cdict);

	if (ZSTD_isError(compressed_size))
		return 0;

	if (compressed_size >= (size_t) srcSize)
		return 0;

	return (int) compressed_size;
}

/*
 * Decompress data using a shared dictionary.
 *
 * Uses the cached decompression context from the dictionary cache to avoid
 * creating/destroying a new context on every decompression operation.
 */
void
nx_decompress_with_shared_dict(const char *src, char *dst,
							   int compressedSize, int uncompressedSize,
							   const NXSharedDictData *dict)
{
	size_t		decompressed_size;

	if (dict == NULL || dict->ddict == NULL || dict->dctx == NULL)
		elog(ERROR, "shared dictionary decompression requires a valid dictionary");

	/*
	 * Reset the context to clear any previous state while preserving
	 * parameters. This ensures consistent decompression and better performance.
	 */
	ZSTD_DCtx_reset(dict->dctx, ZSTD_reset_session_only);

	/* Use the cached context - no malloc/free per operation */
	decompressed_size = ZSTD_decompress_usingDDict(dict->dctx, dst, uncompressedSize,
												   src, compressedSize,
												   dict->ddict);

	if (ZSTD_isError(decompressed_size))
		elog(ERROR, "shared dictionary decompression failed: %s",
			 ZSTD_getErrorName(decompressed_size));

	if (decompressed_size != (size_t) uncompressedSize)
		elog(ERROR, "shared dictionary decompressed size mismatch: got %zu, expected %d",
			 decompressed_size, uncompressedSize);
}

/*
 * Train a shared dictionary from attribute B-tree data.
 *
 * Scans leaf pages of the attribute B-tree, collecting sample data from
 * compressed/uncompressed items.  When enough data is collected, trains
 * a zstd dictionary and stores it on dedicated pages.
 */
uint32
nx_shared_dict_train(Relation rel, AttrNumber attno, size_t dict_capacity)
{
	Buffer		buf;
	Page		page;
	NXBtreePageOpaque *opaque;
	BlockNumber rootblk;
	BlockNumber blkno;
	uint32		new_generation;
	uint32		result_generation = 0;
	NXMetaCacheData *metacache;
	int			i;

	/* Sample collection */
	char	  **samples = NULL;
	size_t	   *sample_sizes = NULL;
	int			num_samples = 0;
	int			max_samples = 10000;
	size_t		total_sample_size = 0;
	size_t		target_sample_size;

	/* Dictionary training result */
	char	   *dict_buf = NULL;
	size_t		dict_size;

	if (dict_capacity == 0)
		dict_capacity = NX_DICT_DEFAULT_SIZE;
	if (dict_capacity < NX_DICT_MIN_SIZE)
		dict_capacity = NX_DICT_MIN_SIZE;
	if (dict_capacity > NX_DICT_MAX_SIZE)
		dict_capacity = NX_DICT_MAX_SIZE;

	target_sample_size = dict_capacity * NX_DICT_SAMPLE_MULTIPLIER;

	/* Get the root of the attribute B-tree */
	rootblk = nxmeta_get_root_for_attribute(rel, attno, true);
	if (rootblk == InvalidBlockNumber)
		return 0;

	/* Allocate sample arrays */
	samples = palloc(sizeof(char *) * max_samples);
	sample_sizes = palloc(sizeof(size_t) * max_samples);

	/*
	 * Descend to the leftmost leaf page and scan leaf pages to collect
	 * sample data.  We collect the raw (decompressed) payload from each
	 * item as a sample.
	 */
	buf = nxbt_descend(rel, attno, MinNXTid, 0, true, false,
					   InvalidBuffer, InvalidBuffer);

	while (BufferIsValid(buf) &&
		   num_samples < max_samples &&
		   total_sample_size < target_sample_size)
	{
		page = BufferGetPage(buf);
		opaque = NXBtreePageGetOpaque(page);

		Assert(opaque->nx_level == 0);
		Assert(opaque->nx_page_id == NX_BTREE_PAGE_ID);

		/*
		 * Walk the leaf page items.  Each item is either an
		 * NXAttributeArrayItem or NXAttributeCompressedItem.  We collect
		 * the uncompressed payload as a sample.
		 */
		{
			char	   *item_ptr;
			char	   *page_end;

			item_ptr = (char *) page + ((PageHeader) page)->pd_upper;
			page_end = (char *) page + ((PageHeader) page)->pd_special;

			while (item_ptr < page_end &&
				   num_samples < max_samples &&
				   total_sample_size < target_sample_size)
			{
				NXAttributeArrayItem *item = (NXAttributeArrayItem *) item_ptr;
				char	   *payload;
				int			payload_size;

				if (item->t_size == 0)
					break;

				if (item->t_flags & NXBT_ATTR_COMPRESSED)
				{
					NXAttributeCompressedItem *citem =
						(NXAttributeCompressedItem *) item;

					/*
					 * For compressed items, the compressed payload itself
					 * is a good sample for dictionary training.  The
					 * dictionary will learn the byte patterns common in
					 * the pre-encoded data.
					 */
					payload = citem->t_payload;
					payload_size = (int) (citem->t_size -
						offsetof(NXAttributeCompressedItem, t_payload));
				}
				else
				{
					payload = (char *) &item->t_tid_codewords;
					payload_size = (int) (item->t_size -
						((char *) &item->t_tid_codewords - (char *) item));
				}

				if (payload_size > 0 && payload_size < BLCKSZ)
				{
					samples[num_samples] = palloc(payload_size);
					memcpy(samples[num_samples], payload, payload_size);
					sample_sizes[num_samples] = payload_size;
					total_sample_size += payload_size;
					num_samples++;
				}

				item_ptr += MAXALIGN(item->t_size);
			}
		}

		/* Move to the next leaf page */
		blkno = opaque->nx_next;
		UnlockReleaseBuffer(buf);

		if (blkno == InvalidBlockNumber)
			buf = InvalidBuffer;
		else
		{
			buf = ReadBuffer(rel, blkno);
			LockBuffer(buf, BUFFER_LOCK_SHARE);
		}
	}

	if (BufferIsValid(buf))
		UnlockReleaseBuffer(buf);

	/* Need minimum samples to train a useful dictionary */
	if (num_samples < NX_DICT_MIN_SAMPLES)
	{
		elog(NOTICE, "insufficient samples (%d) for dictionary training on attribute %d",
			 num_samples, attno);
		goto cleanup;
	}

	/*
	 * Build a contiguous sample buffer for ZDICT_trainFromBuffer.
	 * The API expects all samples concatenated in a single buffer.
	 */
	{
		char	   *concat_buf;
		size_t		concat_offset = 0;

		concat_buf = palloc(total_sample_size);
		for (i = 0; i < num_samples; i++)
		{
			memcpy(concat_buf + concat_offset, samples[i], sample_sizes[i]);
			concat_offset += sample_sizes[i];
		}

		/* Allocate output buffer for the dictionary */
		dict_buf = palloc(dict_capacity);

		/* Train the dictionary */
		dict_size = ZDICT_trainFromBuffer(dict_buf, dict_capacity,
										  concat_buf, sample_sizes,
										  (unsigned) num_samples);

		pfree(concat_buf);

		if (ZSTD_isError(dict_size))
		{
			elog(NOTICE, "dictionary training failed for attribute %d: %s",
				 attno, ZSTD_getErrorName(dict_size));
			pfree(dict_buf);
			dict_buf = NULL;
			goto cleanup;
		}
	}

	/*
	 * Get the next generation counter.  This must be done under the
	 * metapage lock to avoid races with concurrent trainers.
	 */
	metacache = nxmeta_get_cache(rel);
	new_generation = metacache->cache_attrs[attno].dict_generation + 1;
	if (new_generation == 0)
		new_generation = 1;		/* skip 0 which means "no dictionary" */

	/* Write dictionary to dedicated pages */
	nx_shared_dict_write_pages(rel, attno, dict_buf, dict_size, new_generation);

	/* Invalidate any cached dictionary for this attribute */
	nx_shared_dict_invalidate(RelationGetRelid(rel), attno);

	result_generation = new_generation;

	elog(NOTICE, "trained shared dictionary for attribute %d: %zu bytes, "
		 "%d samples, generation %u",
		 attno, dict_size, num_samples, new_generation);

cleanup:
	/* Free sample data */
	for (i = 0; i < num_samples; i++)
		pfree(samples[i]);
	pfree(samples);
	pfree(sample_sizes);
	if (dict_buf != NULL)
		pfree(dict_buf);

	return result_generation;
}

#else							/* !USE_ZSTD */

/*
 * Stub implementations when zstd is not available.
 * Shared dictionary compression requires zstd.
 */
int
nx_try_compress_with_shared_dict(const char *src, char *dst,
								 int srcSize, int dstCapacity,
								 const NXSharedDictData *dict)
{
	/* Without zstd, shared dictionary compression is not available */
	return 0;
}

void
nx_decompress_with_shared_dict(const char *src, char *dst,
							   int compressedSize, int uncompressedSize,
							   const NXSharedDictData *dict)
{
	elog(ERROR, "shared dictionary decompression requires zstd support");
}

uint32
nx_shared_dict_train(Relation rel, AttrNumber attno, size_t dict_capacity)
{
	elog(ERROR, "shared dictionary training requires zstd support");
	return 0;					/* unreachable */
}

#endif							/* USE_ZSTD */
