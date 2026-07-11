/*-------------------------------------------------------------------------
 *
 * backend_runtime_regex.c
 *	  Runtime bridge accessors for regex-owned state.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/regex/backend_runtime_regex.c
 *
 *-------------------------------------------------------------------------
 */
#define BACKEND_RUNTIME_NO_INLINE_BUCKET_ACCESSORS
#include "postgres.h"

#include "utils/backend_runtime.h"
#include "utils/memutils.h"
#include "../utils/init/backend_runtime_internal.h"

PgSessionRegexState *
PgCurrentSessionRegexState(void)
{
	PG_RUNTIME_RETURN_CURRENT_SESSION_BUCKET(CurrentPgSessionRegexRuntimeState,
											 regex);
}

PgExecutionRegexState *
PgCurrentExecutionRegexState(void)
{
	PG_RUNTIME_RETURN_CURRENT_EXECUTION_BUCKET(CurrentPgExecutionRegexRuntimeState,
											   regex);
}

struct pg_ctype_cache **
PgCurrentRegexCtypeCacheListRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgSessionRegexRuntimeState, PgCurrentSessionRegexState)->ctype_cache_list;
}

MemoryContext *
PgCurrentRegexpCacheMemoryContextRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgSessionRegexRuntimeState, PgCurrentSessionRegexState)->regexp_cache_context;
}

int *
PgCurrentRegexpNumCachedResRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgSessionRegexRuntimeState, PgCurrentSessionRegexState)->num_cached_res;
}

PgSessionRegexCachedEntry *
PgCurrentRegexpCachedResArray(void)
{
	PgSessionRegexState *regex;

	regex = PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgSessionRegexRuntimeState,
											PgCurrentSessionRegexState);
	if (unlikely(regex->cached_res == NULL))
	{
		if (regex->regexp_cache_context == NULL)
			regex->regexp_cache_context =
				AllocSetContextCreate(TopMemoryContext,
									  "RegexpCacheMemoryContext",
									  ALLOCSET_SMALL_SIZES);
		regex->cached_res =
			MemoryContextAllocZero(regex->regexp_cache_context,
								   sizeof(PgSessionRegexCachedEntry) *
								   PG_SESSION_MAX_CACHED_REGEX);
	}

	return regex->cached_res;
}

void **
PgCurrentRegexLocaleRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionRegexRuntimeState, PgCurrentExecutionRegexState)->regex_locale;
}
