/*-------------------------------------------------------------------------
 *
 * palloc_integration.c
 *	  Hook integration demonstration for libumem-backed MemoryContext.
 *
 * This file shows how to wire the UmemMemoryContext into PostgreSQL's
 * memory management system.  It is experimental code and is NOT wired
 * into the build system -- it serves as a reference for future
 * integration work.
 *
 * When UMEM_ENABLE_EXPERIMENTAL is defined (in addition to USE_LIBUMEM),
 * this module provides:
 *
 *   - A _PG_init() function that installs a MemoryContext reset callback
 *     to optionally replace TopMemoryContext with a libumem-backed context.
 *
 *   - Environment variable UMEM_MEMORY_CONTEXT=1 to activate at runtime.
 *
 * Build manually with:
 *   cc -DUSE_LIBUMEM -DUMEM_ENABLE_EXPERIMENTAL -shared -o palloc_integration.so \
 *      palloc_integration.c umem_palloc.c -I../../../include -ldl
 *
 * Copyright (c) 2024-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/mmgr/palloc_integration.c
 *
 *-------------------------------------------------------------------------
 */

#if defined(USE_LIBUMEM) && defined(UMEM_ENABLE_EXPERIMENTAL)

#include "postgres.h"

#include <stdlib.h>

#include "fmgr.h"
#include "utils/memutils.h"
#include "umem_palloc.h"

PG_MODULE_MAGIC;

/*
 * _PG_init -- module load callback.
 *
 * If UMEM_MEMORY_CONTEXT=1 is set in the environment, replace the
 * TopMemoryContext with a libumem-backed context.  This is a
 * proof-of-concept; a production integration would need more care
 * around the bootstrap sequence and child context handling.
 */
void
_PG_init(void)
{
	const char *env;

	env = getenv("UMEM_MEMORY_CONTEXT");
	if (env == NULL || strcmp(env, "1") != 0)
	{
		elog(LOG, "palloc_integration: UMEM_MEMORY_CONTEXT not set, skipping");
		return;
	}

	if (!UmemIsAvailable())
	{
		elog(WARNING, "palloc_integration: libumem not available");
		return;
	}

	elog(LOG, "palloc_integration: libumem memory context integration active");

	/*
	 * NOTE: This is a demonstration.  Actually replacing TopMemoryContext
	 * at this stage is not safe for production use.  A real integration
	 * would hook into MemoryContextCreate or provide a custom allocator
	 * that is selected during the bootstrap sequence.
	 *
	 * What we CAN do safely is create a child context backed by libumem
	 * for specific subsystems (e.g., a dedicated cache or work area):
	 *
	 *   MemoryContext umem_work;
	 *   umem_work = UmemMemoryContextCreate(TopMemoryContext,
	 *                                       "UmemWorkArea");
	 */
}

#endif							/* USE_LIBUMEM && UMEM_ENABLE_EXPERIMENTAL */
