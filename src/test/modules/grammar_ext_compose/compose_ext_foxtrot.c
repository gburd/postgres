/*-------------------------------------------------------------------------
 *
 * compose_ext_foxtrot.c
 *	  grammar_ext_compose: extension "foxtrot".
 *
 * Re-declares K_GRAMMAR_ALPHA with a DIFFERENT lexeme.  Per the API
 * contract, this should fail register() with a clear error.  expect_-
 * failure=true so the helper logs the failure as expected (NOTICE)
 * rather than as a regression (WARNING).
 *
 * Used only in conjunction with alpha; standalone foxtrot has no
 * conflicting prior token and would register successfully on its own.
 *
 * Portions Copyright (c) 2026, PostgreSQL Global Development Group
 *-------------------------------------------------------------------------
 */
#include "compose_ext_helpers.h"

#include "fmgr.h"

PG_MODULE_MAGIC;

void		_PG_init(void);

static const ComposeToken tokens[] = {
	{"K_GRAMMAR_ALPHA", "GRAMMAR_ALPHA_DIFFERENT_LEXEME",
	UNRESERVED_KEYWORD},
};

static const ComposeRule rules[] = {
	{"stmt", {"K_GRAMMAR_ALPHA", NULL}, "foxtrot:stmt-alpha"},
};

void
_PG_init(void)
{
	const ComposeSpec spec = {
		.name = "grammar_ext_compose_foxtrot",
		.version = "1.0",
		.tokens = tokens,
		.ntokens = lengthof(tokens),
		.rules = rules,
		.nrules = lengthof(rules),
		.precs = NULL,
		.nprecs = 0,

		/*
		 * expect_failure depends on whether alpha is loaded first. In
		 * standalone load, foxtrot succeeds; in alpha+foxtrot load, it should
		 * fail.  We mark it as expect_failure=false so the WARNING fires when
		 * standalone-loaded; the alpha+ foxtrot test's TAP harness greps for
		 * the API's collision error directly (NOT for our expected-failure
		 * message).
		 */
		.expect_failure = false,
	};

	register_compose_extension(&spec);
}
