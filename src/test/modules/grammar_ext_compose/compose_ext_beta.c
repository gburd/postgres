/*-------------------------------------------------------------------------
 *
 * compose_ext_beta.c
 *	  grammar_ext_compose: extension "beta".
 *
 * Disjoint from alpha: distinct tokens (K_GRAMMAR_CHARLIE,
 * K_GRAMMAR_DELTA), distinct rules.  Tests the multi-extension load
 * path: alpha + beta together rebuild a single parser .so containing
 * BOTH extensions' rules.
 *
 * Also sets one precedence directive to exercise the %nonassoc path
 * in the serializer.
 *
 * Portions Copyright (c) 2026, PostgreSQL Global Development Group
 *-------------------------------------------------------------------------
 */
#include "compose_ext_helpers.h"

#include "fmgr.h"

PG_MODULE_MAGIC;

void		_PG_init(void);

static const ComposeToken tokens[] = {
	{"K_GRAMMAR_CHARLIE", "grammar_charlie", UNRESERVED_KEYWORD},
	{"K_GRAMMAR_DELTA", "grammar_delta", UNRESERVED_KEYWORD},
};

static const ComposeRule rules[] = {
	{"stmt", {"K_GRAMMAR_CHARLIE", "K_GRAMMAR_DELTA", NULL},
	"beta:charlie-delta"},
};

static const ComposePrec precs[] = {
	{"K_GRAMMAR_CHARLIE", 100, PG_GRAMMAR_ASSOC_NONASSOC},
};

void
_PG_init(void)
{
	const ComposeSpec spec = {
		.name = "grammar_ext_compose_beta",
		.version = "1.0",
		.tokens = tokens,
		.ntokens = lengthof(tokens),
		.rules = rules,
		.nrules = lengthof(rules),
		.precs = precs,
		.nprecs = lengthof(precs),
		.expect_failure = false,
	};

	register_compose_extension(&spec);
}
