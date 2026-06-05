/*-------------------------------------------------------------------------
 *
 * compose_ext_helpers.h
 *	  Shared helpers for the six grammar_ext_compose .so test extensions.
 *
 * Each extension's _PG_init() calls register_compose_extension() with
 * a struct describing its tokens and rules.  Centralizing the wiring
 * keeps each compose_ext_*.c under 100 lines and makes the matrix of
 * load-time behaviour easy to scan.
 *
 * Header-only; no .c counterpart.  Each extension TU includes this
 * header and gets its own static copies of the helpers.  That's
 * deliberate: the helpers are tiny and we want each .so to be
 * self-contained.
 *
 * Portions Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * src/test/modules/grammar_ext_compose/compose_ext_helpers.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef COMPOSE_EXT_HELPERS_H
#define COMPOSE_EXT_HELPERS_H

#include "postgres.h"

#include "common/keywords.h"
#include "miscadmin.h"
#include "parser/parser_extension.h"
#include "utils/elog.h"

typedef struct ComposeToken
{
	const char *name;
	const char *lexeme;
	int			category;		/* UNRESERVED_KEYWORD etc. */
} ComposeToken;

typedef struct ComposeRule
{
	const char *lhs;
	const char *rhs[8];			/* NULL-terminated; max 7 RHS symbols */
	const char *label;			/* logged when reduce fires */
} ComposeRule;

typedef struct ComposePrec
{
	const char *symbol;
	int			level;
	PgGrammarExtAssoc assoc;
} ComposePrec;

typedef struct ComposeSpec
{
	const char *name;
	const char *version;
	const ComposeToken *tokens;
	int			ntokens;
	const ComposeRule *rules;
	int			nrules;
	const ComposePrec *precs;
	int			nprecs;
	bool		expect_failure; /* true means register() should fail */
} ComposeSpec;

/*
 * compose_reduce
 *	  Reduce-callback shared by every test rule.  user_data is the
 *	  rule's `label` string from the spec, so the NOTICE that fires
 *	  identifies which rule reduced.  Cleared lhs_out so the rebuilt
 *	  parser sees a defined value on the stack.
 */
static inline void
compose_reduce(void *user_data, void *extra_arg, int nrhs,
			   const void *const *rhs_values, const int *rhs_locs,
			   void *lhs_out)
{
	const char *label = (const char *) user_data;

	(void) extra_arg;
	(void) rhs_values;
	(void) rhs_locs;
	ereport(NOTICE,
			(errmsg("grammar_ext_compose: %s reduce fired (nrhs=%d)",
					label ? label : "(?)", nrhs)));
	*(void **) lhs_out = NULL;
}

/*
 * register_compose_extension
 *	  Apply a ComposeSpec to a fresh PgGrammarExtension and call
 *	  pg_grammar_ext_register().  Logs NOTICE / WARNING based on
 *	  whether the spec's expect_failure matches reality so the TAP
 *	  test can verify either branch with the same psql output capture.
 */
static inline void
register_compose_extension(const ComposeSpec *spec)
{
	PgGrammarExtension *ext;
	char	   *err = NULL;
	bool		ok;

	if (!process_shared_preload_libraries_in_progress)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("%s must be loaded via shared_preload_libraries",
						spec->name)));

	ext = pg_grammar_ext_create(spec->name, spec->version);

	for (int i = 0; i < spec->ntokens; i++)
	{
		const ComposeToken *t = &spec->tokens[i];

		pg_grammar_ext_add_token(ext, t->name, t->lexeme, t->category);
	}
	for (int i = 0; i < spec->nprecs; i++)
	{
		const ComposePrec *p = &spec->precs[i];

		pg_grammar_ext_set_precedence(ext, p->symbol, p->level, p->assoc);
	}
	for (int i = 0; i < spec->nrules; i++)
	{
		const ComposeRule *r = &spec->rules[i];

		pg_grammar_ext_add_rule(ext, r->lhs, (const char **) r->rhs,
								compose_reduce, (void *) r->label);
	}

	ok = pg_grammar_ext_register(ext, &err);
	if (ok && spec->expect_failure)
		ereport(WARNING,
				(errmsg("grammar_ext_compose: %s registered but was "
						"expected to FAIL", spec->name)));
	else if (!ok && !spec->expect_failure)
		ereport(WARNING,
				(errmsg("grammar_ext_compose: %s register() failed: %s",
						spec->name, err ? err : "(no error)")));
	else if (ok)
		ereport(NOTICE,
				(errmsg("grammar_ext_compose: %s registered (%d tokens, "
						"%d rules, %d prec)",
						spec->name, spec->ntokens, spec->nrules,
						spec->nprecs)));
	else
		ereport(NOTICE,
				(errmsg("grammar_ext_compose: %s register() failed as "
						"expected: %s",
						spec->name, err ? err : "(no error)")));

	if (!ok)
		pg_grammar_ext_unregister(ext);
}

#endif							/* COMPOSE_EXT_HELPERS_H */
