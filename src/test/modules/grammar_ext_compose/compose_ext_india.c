/*-------------------------------------------------------------------------
 *
 * compose_ext_india.c
 *	  grammar_ext_compose: extension "india".
 *
 * Introduces a SHIFT/REDUCE conflict (the classic dangling-else) that
 * the LALR table builder resolves SILENTLY (keep-shift) and still builds
 * a snapshot from -- exactly the case Lime letter-35 Q1 flagged as the
 * dangerous one.  Lime v1.8.1's lime_compile_grammar_in_process_ex
 * surfaces it via nconflict > 0.
 *
 * Used by 001_compose.pl to verify that pg_grammar_compose_install gates
 * on that conflict count and REFUSES to install the composed parser
 * (a FATAL at prewarm), rather than silently shipping a parser whose
 * conflict was resolved by table-build order instead of author intent.
 *
 *   stmt   ::= india_if
 *   india_if ::= K_GRAMMAR_INDIA X K_GRAMMAR_THEN india_if
 *   india_if ::= K_GRAMMAR_INDIA X K_GRAMMAR_THEN india_if K_GRAMMAR_ELSE india_if
 *   india_if ::= X
 *
 * The two K_GRAMMAR_THEN-led productions create the dangling-else
 * shift/reduce on lookahead K_GRAMMAR_ELSE.
 *
 * Portions Copyright (c) 2026, PostgreSQL Global Development Group
 *-------------------------------------------------------------------------
 */
#include "compose_ext_helpers.h"

#include "fmgr.h"

PG_MODULE_MAGIC;

void		_PG_init(void);

static const ComposeToken tokens[] = {
	{"K_GRAMMAR_INDIA", "grammar_india", UNRESERVED_KEYWORD},
	{"K_GRAMMAR_THEN", "grammar_then", UNRESERVED_KEYWORD},
	{"K_GRAMMAR_ELSE", "grammar_else", UNRESERVED_KEYWORD},
	{"K_GRAMMAR_XX", "grammar_xx", UNRESERVED_KEYWORD},
};

static const ComposeType types[] = {
	{"india_if", "Node *"},
};

static const ComposeRule rules[] = {
	{"stmt", {"india_if", NULL}, "india:stmt"},
	{"india_if", {"K_GRAMMAR_INDIA", "K_GRAMMAR_XX", "K_GRAMMAR_THEN",
	"india_if", NULL}, "india:if-then"},
	{"india_if", {"K_GRAMMAR_INDIA", "K_GRAMMAR_XX", "K_GRAMMAR_THEN",
	"india_if", "K_GRAMMAR_ELSE", "india_if", NULL}, "india:if-then-else"},
	{"india_if", {"K_GRAMMAR_XX", NULL}, "india:x"},
};

void
_PG_init(void)
{
	const ComposeSpec spec = {
		.name = "grammar_ext_compose_india",
		.version = "1.0",
		.tokens = tokens,
		.ntokens = lengthof(tokens),
		.types = types,
		.ntypes = lengthof(types),
		.rules = rules,
		.nrules = lengthof(rules),
		.precs = NULL,
		.nprecs = 0,
		/* register() succeeds; the conflict surfaces at compose/prewarm. */
		.expect_failure = false,
	};

	register_compose_extension(&spec);
}
