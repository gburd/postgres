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
#include "postgres.h"

#include "parser/parse_expr.h"
#include "parser/parser.h"
#include "utils/backend_runtime.h"
#include "../utils/init/backend_runtime_internal.h"

PgSessionParserState *
PgCurrentSessionParserState(void)
{
	PgSessionParserState *parser;

	parser = &PgCurrentOrEarlySession()->parser;

	if (!parser->initialized)
		PgSessionInitializeParserState(parser);

	return parser;
}

bool *
PgCurrentTransformNullEqualsRef(void)
{
	return &PgCurrentSessionParserState()->transform_null_equals_value;
}

int *
PgCurrentBackslashQuoteRef(void)
{
	return &PgCurrentSessionParserState()->backslash_quote_value;
}

HTAB **
PgCurrentOperatorLookupCacheRef(void)
{
	return &PgCurrentSessionParserState()->operator_lookup_cache;
}
