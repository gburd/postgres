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
#include "postgres.h"

#include "utils/backend_runtime.h"
#include "../utils/init/backend_runtime_internal.h"

struct pg_ctype_cache **
PgCurrentRegexCtypeCacheListRef(void)
{
	return &PgCurrentSessionRegexState()->ctype_cache_list;
}

MemoryContext *
PgCurrentRegexpCacheMemoryContextRef(void)
{
	return &PgCurrentSessionRegexState()->regexp_cache_context;
}

int *
PgCurrentRegexpNumCachedResRef(void)
{
	return &PgCurrentSessionRegexState()->num_cached_res;
}

PgSessionRegexCachedEntry *
PgCurrentRegexpCachedResArray(void)
{
	return PgCurrentSessionRegexState()->cached_res;
}

void **
PgCurrentRegexLocaleRef(void)
{
	return &PgCurrentExecutionRegexState()->regex_locale;
}
