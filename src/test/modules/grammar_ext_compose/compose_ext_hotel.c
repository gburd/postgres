/*-------------------------------------------------------------------------
 *
 * compose_ext_hotel.c
 *	  grammar_ext_compose: extension "hotel".
 *
 * Sets %left precedence on K_GRAMMAR_BRAVO at level 200 (higher than
 * beta's nonassoc 100 on K_GRAMMAR_CHARLIE).  When hotel + alpha load
 * together, K_GRAMMAR_BRAVO comes from alpha; hotel only sets its
 * precedence.  This exercises the cross-extension precedence-reference
 * path: hotel's rule uses no new tokens but raises an existing
 * non-precedented one to a binding level.
 *
 * Hotel's only RULE re-declares the same alpha-shaped pattern; not
 * functionally interesting but tests that two extensions can both
 * have rules with the same LHS (`stmt`) and DIFFERENT RHS without
 * grammar-level conflict.
 *
 * Portions Copyright (c) 2026, PostgreSQL Global Development Group
 *-------------------------------------------------------------------------
 */
#include "compose_ext_helpers.h"

#include "fmgr.h"

PG_MODULE_MAGIC;

void		_PG_init(void);

static const ComposeToken tokens[] = {
	{"K_GRAMMAR_HOTEL", "grammar_hotel", UNRESERVED_KEYWORD},
};

static const ComposeRule rules[] = {
	{"stmt", {"K_GRAMMAR_HOTEL", NULL}, "hotel:stmt-hotel"},
};

static const ComposePrec precs[] = {
	/*
	 * Reference an alpha-side token by name.  When loaded with alpha the
	 * precedence applies; when loaded standalone the precedence names a
	 * symbol Lime doesn't know yet.  Lime treats unknown precedence-symbol
	 * names as a forward reference and the rebuild either resolves them later
	 * or errors at compile time -- either way the test asserts against the
	 * postmaster's log.
	 */
	{"K_GRAMMAR_BRAVO", 200, PG_GRAMMAR_ASSOC_LEFT},
};

void
_PG_init(void)
{
	const ComposeSpec spec = {
		.name = "grammar_ext_compose_hotel",
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
