/*-------------------------------------------------------------------------
 *
 * compose_ext_echo.c
 *	  grammar_ext_compose: extension "echo".
 *
 * Re-declares K_GRAMMAR_ALPHA with the SAME lexeme + category as
 * alpha.  Per the parser_extension.h API contract: "If a token with
 * the same name already exists and `lexeme` and `category` match, the
 * call is a no-op (extensions can re-declare canonical tokens without
 * conflict)."  This extension exercises that branch: registering
 * alpha + echo must succeed.
 *
 * Portions Copyright (c) 2026, PostgreSQL Global Development Group
 *-------------------------------------------------------------------------
 */
#include "compose_ext_helpers.h"

#include "fmgr.h"

PG_MODULE_MAGIC;

void		_PG_init(void);

static const ComposeToken tokens[] = {
	{"K_GRAMMAR_ALPHA", "grammar_alpha", UNRESERVED_KEYWORD},
};

static const ComposeRule rules[] = {
	{"stmt", {"K_GRAMMAR_ALPHA", "K_GRAMMAR_ALPHA", NULL},
	"echo:alpha-alpha"},
};

void
_PG_init(void)
{
	const ComposeSpec spec = {
		.name = "grammar_ext_compose_echo",
		.version = "1.0",
		.tokens = tokens,
		.ntokens = lengthof(tokens),
		.rules = rules,
		.nrules = lengthof(rules),
		.precs = NULL,
		.nprecs = 0,
		.expect_failure = false,
	};

	register_compose_extension(&spec);
}
