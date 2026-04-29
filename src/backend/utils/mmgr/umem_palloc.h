/*-------------------------------------------------------------------------
 *
 * umem_palloc.h
 *	  Budget-based memory contexts backed by libumem's slab allocator.
 *
 * This header declares the public interface for the experimental
 * libumem-backed MemoryContext.  It is only active when compiled with
 * -DUSE_LIBUMEM and when the UMEM_MEMORY_CONTEXT environment variable
 * is set to "1" at backend startup.
 *
 * The implementation provides:
 *   - Per-size slab caches for common allocation sizes
 *   - Allocation tracking for context reset/delete
 *   - Runtime symbol resolution (no hard libumem link dependency)
 *   - Full MemoryContext interface compatibility
 *
 * Copyright (c) 2024-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/mmgr/umem_palloc.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef UMEM_PALLOC_H
#define UMEM_PALLOC_H

#ifdef USE_LIBUMEM

#include "utils/memutils.h"

/*
 * UmemMemoryContextCreate -- create a new libumem-backed memory context.
 *
 * Returns a standard MemoryContext that routes allocations through
 * libumem's slab allocator.  If libumem cannot be loaded, falls back
 * to AllocSetContextCreate behavior.
 */
extern MemoryContext UmemMemoryContextCreate(MemoryContext parent,
											 const char *name);

/*
 * UmemIsAvailable -- check whether libumem is loaded and usable.
 */
extern bool UmemIsAvailable(void);

#endif							/* USE_LIBUMEM */

#endif							/* UMEM_PALLOC_H */
