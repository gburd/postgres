/*-------------------------------------------------------------------------
 *
 * backend_runtime_parser.c
 *	  Runtime bridge accessors for parser-owned session state.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/parser/backend_runtime_parser.c
 *
 *-------------------------------------------------------------------------
 */
#define BACKEND_RUNTIME_NO_INLINE_BUCKET_ACCESSORS
#include "postgres.h"

#include "parser/parse_expr.h"
#include "parser/parser.h"
#include "utils/backend_runtime.h"
#include "../utils/init/backend_runtime_internal.h"

PgSessionParserState *
PgCurrentSessionParserState(void)
{
	PgSessionParserState *parser;

	if (likely(CurrentPgSessionParserRuntimeState != NULL &&
			   CurrentPgSessionParserRuntimeState->initialized))
		return CurrentPgSessionParserRuntimeState;

	parser = &PgCurrentOrEarlySession()->parser;

	if (!parser->initialized)
		PgSessionInitializeParserState(parser);

	return parser;
}

bool *
PgCurrentTransformNullEqualsRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionParserRuntimeState, PgCurrentSessionParserState)->transform_null_equals_value;
}

int *
PgCurrentBackslashQuoteRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionParserRuntimeState, PgCurrentSessionParserState)->backslash_quote_value;
}

HTAB **
PgCurrentOperatorLookupCacheRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionParserRuntimeState, PgCurrentSessionParserState)->operator_lookup_cache;
}
