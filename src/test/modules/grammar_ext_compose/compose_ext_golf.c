/*-------------------------------------------------------------------------
 *
 * compose_ext_golf.c
 *	  grammar_ext_compose: extension "golf".
 *
 * Cross-extension dependency: declares its own token K_GRAMMAR_GOLF
 * but ITS RULE uses K_GRAMMAR_ALPHA from extension alpha.  Tests that
 * extensions in the same shared_preload_libraries can share token
 * vocabulary -- a token introduced by one extension is reachable from
 * another's rules during the same compose-and-rebuild cycle.
 *
 * Load order matters here: alpha must register BEFORE golf so
 * K_GRAMMAR_ALPHA is in the symbol table when golf's rule references
 * it.  shared_preload_libraries = 'compose_ext_alpha,compose_ext_golf'
 * gets the order right; reversed order should produce a clear error.
 *
 * Portions Copyright (c) 2026, PostgreSQL Global Development Group
 *-------------------------------------------------------------------------
 */
#include "compose_ext_helpers.h"

#include "fmgr.h"

PG_MODULE_MAGIC;

void		_PG_init(void);

static const ComposeToken tokens[] = {
	{"K_GRAMMAR_GOLF", "grammar_golf", UNRESERVED_KEYWORD},
};

static const ComposeRule rules[] = {
	/*
	 * Rule references K_GRAMMAR_ALPHA which alpha must have declared first.
	 * If alpha isn't loaded, this rule's reference to an unknown token will
	 * surface either at register() (if we add the symbol-table check) or at
	 * lime-rebuild time (lime errors on undefined RHS symbol).
	 */
	{"stmt", {"K_GRAMMAR_GOLF", "K_GRAMMAR_ALPHA", NULL},
	"golf:golf-alpha"},
};

void
_PG_init(void)
{
	const ComposeSpec spec = {
		.name = "grammar_ext_compose_golf",
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
