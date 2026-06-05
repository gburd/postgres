/*-------------------------------------------------------------------------
 *
 * compose_ext_alpha.c
 *	  grammar_ext_compose: extension "alpha".
 *
 * Adds two unique tokens (K_GRAMMAR_ALPHA, K_GRAMMAR_BRAVO) and two
 * orphan rules off `stmt`.  Baseline of the torture test matrix: a
 * straightforward two-token / two-rule extension that should always
 * register cleanly.
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
	{"K_GRAMMAR_BRAVO", "grammar_bravo", UNRESERVED_KEYWORD},
};

static const ComposeRule rules[] = {
	{"stmt", {"K_GRAMMAR_ALPHA", NULL}, "alpha:stmt-alpha"},
	{"stmt", {"K_GRAMMAR_BRAVO", NULL}, "alpha:stmt-bravo"},
};

void
_PG_init(void)
{
	const ComposeSpec spec = {
		.name = "grammar_ext_compose_alpha",
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
