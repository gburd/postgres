/*-------------------------------------------------------------------------
 *
 * umem_palloc.c
 *	  libumem-backed MemoryContext implementation for PostgreSQL.
 *
 * This module provides an alternative MemoryContext that routes allocations
 * through libumem's slab allocator.  It is intended for development and
 * debugging -- libumem's audit trails, freed-buffer checking, and leak
 * detection complement PostgreSQL's own palloc debugging.
 *
 * To activate, compile with -DUSE_LIBUMEM and set the UMEM_MEMORY_CONTEXT
 * environment variable to "1" before starting the backend.  When active,
 * contexts created via UmemMemoryContextCreate() use libumem internally
 * while preserving the standard PostgreSQL MemoryContext interface.
 *
 * This file is only compiled when USE_LIBUMEM is defined.
 *
 * Copyright (c) 2024-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/mmgr/umem_palloc.c
 *
 *-------------------------------------------------------------------------
 */
#ifdef USE_LIBUMEM

#include "postgres.h"

#include <dlfcn.h>
#include <string.h>
#include <stdlib.h>

#include "utils/memutils.h"
#include "utils/memutils_internal.h"

/* --- libumem types (avoid hard header dependency) ----------------------- */

typedef struct umem_cache umem_cache_t;

/* Function pointers resolved at runtime via dlsym(). */
static umem_cache_t *(*umem_cache_create_fn)(const char *, size_t, size_t,
											  void *(*)(void *, void *, int),
											  void (*)(void *, void *),
											  void (*)(void *),
											  void *, void *, int);
static void		   *(*umem_cache_alloc_fn)(umem_cache_t *, int);
static void			(*umem_cache_free_fn)(umem_cache_t *, void *);
static void			(*umem_cache_destroy_fn)(umem_cache_t *);
static void		   *(*umem_alloc_fn)(size_t, int);
static void			(*umem_free_fn)(void *, size_t);

/* Flags passed to umem allocation functions. */
#define UMEM_DEFAULT	0
#define UMEM_NOFAIL		0x0100

/* --- UmemMemoryContext structure ---------------------------------------- */

/*
 * We keep a small set of per-size caches for common allocation sizes (the
 * "small" and "medium" buckets).  Anything larger falls through to
 * umem_alloc() directly.
 */
#define UMEM_NUM_CACHES		8
#define UMEM_SMALL_SIZES		{ 32, 64, 128, 256, 512, 1024, 2048, 4096 }

/* Track individual allocations so we can free them on context reset/delete. */
typedef struct UmemAllocation
{
	void	   *ptr;
	Size		size;
} UmemAllocation;

#define UMEM_INITIAL_TRACK_CAPACITY	 64

typedef struct UmemMemoryContextData
{
	MemoryContextData	header;				/* standard header */

	/* Per-size slab caches. */
	umem_cache_t	   *caches[UMEM_NUM_CACHES];
	Size				cache_sizes[UMEM_NUM_CACHES];

	/* Allocation tracking array (grown with realloc). */
	UmemAllocation	   *allocations;
	int					num_allocations;
	int					capacity;
} UmemMemoryContextData;

typedef UmemMemoryContextData *UmemMemoryContext;

/* --- Forward declarations ---------------------------------------------- */

static void		umem_mcxt_init(UmemMemoryContext umcxt);
static void		umem_mcxt_reset(MemoryContext context);
static void		umem_mcxt_delete_context(MemoryContext context);
static void	   *umem_mcxt_alloc(MemoryContext context, Size size);
static void		umem_mcxt_free_p(MemoryContext context, void *pointer);
static void	   *umem_mcxt_realloc(MemoryContext context, void *pointer, Size size);
static Size		umem_mcxt_get_chunk_space(MemoryContext context, void *pointer);
static bool		umem_mcxt_is_empty(MemoryContext context);
static void		umem_mcxt_stats(MemoryContext context,
								MemoryStatsPrintFunc printfunc, void *passthru,
								MemoryContextCounters *totals, bool print_to_stderr);

/* --- MemoryContext method table ---------------------------------------- */

static const MemoryContextMethods umem_methods = {
	umem_mcxt_alloc,
	umem_mcxt_free_p,
	umem_mcxt_realloc,
	umem_mcxt_reset,
	umem_mcxt_delete_context,
	umem_mcxt_get_chunk_space,
	umem_mcxt_is_empty,
	umem_mcxt_stats,
#ifdef MEMORY_CONTEXT_CHECKING
	NULL,						/* check -- not implemented */
#endif
};

/* --- Runtime resolution of libumem symbols ----------------------------- */

static bool umem_resolved = false;

static bool
resolve_umem_symbols(void)
{
	void   *handle;

	if (umem_resolved)
		return true;

	handle = dlopen("libumem.so", RTLD_NOW | RTLD_GLOBAL);
	if (!handle)
	{
		elog(WARNING, "umem_palloc: cannot load libumem.so: %s", dlerror());
		return false;
	}

	umem_cache_create_fn  = dlsym(handle, "umem_cache_create");
	umem_cache_alloc_fn   = dlsym(handle, "umem_cache_alloc");
	umem_cache_free_fn    = dlsym(handle, "umem_cache_free");
	umem_cache_destroy_fn = dlsym(handle, "umem_cache_destroy");
	umem_alloc_fn         = dlsym(handle, "umem_alloc");
	umem_free_fn          = dlsym(handle, "umem_free");

	if (!umem_cache_create_fn || !umem_cache_alloc_fn ||
		!umem_cache_free_fn || !umem_cache_destroy_fn ||
		!umem_alloc_fn || !umem_free_fn)
	{
		elog(WARNING, "umem_palloc: missing symbols in libumem.so");
		dlclose(handle);
		return false;
	}

	umem_resolved = true;
	return true;
}

/* --- Public API -------------------------------------------------------- */

bool
UmemIsAvailable(void)
{
	return resolve_umem_symbols();
}

MemoryContext
UmemMemoryContextCreate(MemoryContext parent, const char *name)
{
	UmemMemoryContext umcxt;

	if (!resolve_umem_symbols())
	{
		elog(WARNING, "umem_palloc: libumem not available, falling back to AllocSet");
		return AllocSetContextCreate(parent, name,
									 ALLOCSET_DEFAULT_SIZES);
	}

	umcxt = (UmemMemoryContext) malloc(sizeof(UmemMemoryContextData));
	if (!umcxt)
		elog(ERROR, "umem_palloc: out of memory for context");

	memset(umcxt, 0, sizeof(UmemMemoryContextData));

	/* Initialize the standard MemoryContext header. */
	umcxt->header.type = T_AllocSetContext;		/* close enough for now */
	umcxt->header.methods = &umem_methods;
	umcxt->header.parent = parent;
	umcxt->header.name = name;

	umem_mcxt_init(umcxt);

	return (MemoryContext) umcxt;
}

/* --- Context lifecycle ------------------------------------------------- */

static void
umem_mcxt_init(UmemMemoryContext umcxt)
{
	static const Size sizes[] = UMEM_SMALL_SIZES;
	char	name_buf[64];

	for (int i = 0; i < UMEM_NUM_CACHES; i++)
	{
		umcxt->cache_sizes[i] = sizes[i];
		snprintf(name_buf, sizeof(name_buf), "umem_%s_%zu",
				 umcxt->header.name, sizes[i]);
		umcxt->caches[i] = umem_cache_create_fn(name_buf, sizes[i], 0,
												 NULL, NULL, NULL,
												 NULL, NULL, 0);
	}

	umcxt->capacity = UMEM_INITIAL_TRACK_CAPACITY;
	umcxt->num_allocations = 0;
	umcxt->allocations = malloc(sizeof(UmemAllocation) * umcxt->capacity);
	if (!umcxt->allocations)
		elog(ERROR, "umem_palloc: out of memory for allocation tracking");
}

static void
track_allocation(UmemMemoryContext umcxt, void *ptr, Size size)
{
	if (umcxt->num_allocations >= umcxt->capacity)
	{
		umcxt->capacity *= 2;
		umcxt->allocations = realloc(umcxt->allocations,
									 sizeof(UmemAllocation) * umcxt->capacity);
		if (!umcxt->allocations)
			elog(ERROR, "umem_palloc: out of memory growing tracking array");
	}
	umcxt->allocations[umcxt->num_allocations].ptr = ptr;
	umcxt->allocations[umcxt->num_allocations].size = size;
	umcxt->num_allocations++;
}

/* --- Allocation -------------------------------------------------------- */

static void *
umem_mcxt_alloc(MemoryContext context, Size size)
{
	UmemMemoryContext umcxt = (UmemMemoryContext) context;
	void   *ptr = NULL;

	/* Try to satisfy from a slab cache. */
	for (int i = 0; i < UMEM_NUM_CACHES; i++)
	{
		if (size <= umcxt->cache_sizes[i])
		{
			ptr = umem_cache_alloc_fn(umcxt->caches[i], UMEM_DEFAULT);
			if (ptr)
			{
				track_allocation(umcxt, ptr, umcxt->cache_sizes[i]);
				return ptr;
			}
			break;
		}
	}

	/* Large allocation -- fall through to umem_alloc(). */
	ptr = umem_alloc_fn(size, UMEM_DEFAULT);
	if (!ptr)
		elog(ERROR, "umem_palloc: out of memory allocating %zu bytes", size);

	track_allocation(umcxt, ptr, size);
	return ptr;
}

/* --- Free -------------------------------------------------------------- */

static void
umem_mcxt_free_p(MemoryContext context, void *pointer)
{
	UmemMemoryContext umcxt = (UmemMemoryContext) context;

	for (int i = 0; i < umcxt->num_allocations; i++)
	{
		if (umcxt->allocations[i].ptr == pointer)
		{
			Size alloc_size = umcxt->allocations[i].size;
			bool from_cache = false;

			/* Return to appropriate cache or umem_free. */
			for (int c = 0; c < UMEM_NUM_CACHES; c++)
			{
				if (alloc_size == umcxt->cache_sizes[c])
				{
					umem_cache_free_fn(umcxt->caches[c], pointer);
					from_cache = true;
					break;
				}
			}

			if (!from_cache)
				umem_free_fn(pointer, alloc_size);

			/* Remove from tracking (swap with last). */
			umcxt->allocations[i] = umcxt->allocations[umcxt->num_allocations - 1];
			umcxt->num_allocations--;
			return;
		}
	}

	elog(WARNING, "umem_palloc: free of untracked pointer %p", pointer);
}

/* --- Reset (free all allocations, keep caches) ------------------------- */

static void
umem_mcxt_reset(MemoryContext context)
{
	UmemMemoryContext umcxt = (UmemMemoryContext) context;

	for (int i = 0; i < umcxt->num_allocations; i++)
	{
		Size alloc_size = umcxt->allocations[i].size;
		void *ptr = umcxt->allocations[i].ptr;
		bool from_cache = false;

		for (int c = 0; c < UMEM_NUM_CACHES; c++)
		{
			if (alloc_size == umcxt->cache_sizes[c])
			{
				umem_cache_free_fn(umcxt->caches[c], ptr);
				from_cache = true;
				break;
			}
		}

		if (!from_cache)
			umem_free_fn(ptr, alloc_size);
	}

	umcxt->num_allocations = 0;
}

/* --- Delete (destroy caches and tracking) ------------------------------ */

static void
umem_mcxt_delete_context(MemoryContext context)
{
	UmemMemoryContext umcxt = (UmemMemoryContext) context;

	/* First reset to free all outstanding allocations. */
	umem_mcxt_reset(context);

	/* Destroy slab caches. */
	for (int i = 0; i < UMEM_NUM_CACHES; i++)
	{
		if (umcxt->caches[i])
			umem_cache_destroy_fn(umcxt->caches[i]);
	}

	free(umcxt->allocations);
	umcxt->allocations = NULL;
	umcxt->capacity = 0;
}

/* --- Utility methods --------------------------------------------------- */

static void *
umem_mcxt_realloc(MemoryContext context, void *pointer, Size size)
{
	void   *new_ptr;

	new_ptr = umem_mcxt_alloc(context, size);

	if (pointer)
	{
		UmemMemoryContext umcxt = (UmemMemoryContext) context;

		/* Find the old size so we know how much to copy. */
		for (int i = 0; i < umcxt->num_allocations; i++)
		{
			if (umcxt->allocations[i].ptr == pointer)
			{
				Size old_size = umcxt->allocations[i].size;
				memcpy(new_ptr, pointer, Min(old_size, size));
				break;
			}
		}
		umem_mcxt_free_p(context, pointer);
	}

	return new_ptr;
}

static Size
umem_mcxt_get_chunk_space(MemoryContext context, void *pointer)
{
	UmemMemoryContext umcxt = (UmemMemoryContext) context;

	for (int i = 0; i < umcxt->num_allocations; i++)
	{
		if (umcxt->allocations[i].ptr == pointer)
			return umcxt->allocations[i].size;
	}

	return 0;
}

static bool
umem_mcxt_is_empty(MemoryContext context)
{
	UmemMemoryContext umcxt = (UmemMemoryContext) context;
	return umcxt->num_allocations == 0;
}

static void
umem_mcxt_stats(MemoryContext context,
				MemoryStatsPrintFunc printfunc, void *passthru,
				MemoryContextCounters *totals, bool print_to_stderr)
{
	UmemMemoryContext umcxt = (UmemMemoryContext) context;
	Size	total_bytes = 0;

	for (int i = 0; i < umcxt->num_allocations; i++)
		total_bytes += umcxt->allocations[i].size;

	if (printfunc)
	{
		char	buf[256];
		snprintf(buf, sizeof(buf),
				 "umem: %d allocations, %zu bytes total",
				 umcxt->num_allocations, total_bytes);
		printfunc(context, passthru, buf, print_to_stderr);
	}

	if (totals)
	{
		totals->nblocks += umcxt->num_allocations;
		totals->totalspace += total_bytes;
		totals->freespace += 0;	/* slab internals not tracked here */
	}
}

#endif							/* USE_LIBUMEM */
