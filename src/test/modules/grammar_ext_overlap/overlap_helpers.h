/*-------------------------------------------------------------------------
 *
 * overlap_helpers.h
 *	  Shared helpers for the multi-extension overlap torture test.
 *
 * The test builds five small extensions that simulate real-world
 * grammar additions PG extensions might contribute:
 *
 *   ext_duckdb_compat   -- PIVOT, UNPIVOT, QUALIFY (DuckDB SQL)
 *   ext_mysql_compat    -- DESCRIBE, USE (MySQL aliases)
 *   ext_mongo_jsonb     -- FIND, AGGPIPE (Mongo-style JSONB query)
 *   ext_pg_infer        -- INFER, PREDICT, TRAIN (inference DSL)
 *   ext_quel_lite       -- minimal QUEL keywords
 *
 * Each .so is a self-contained shared_module that registers via
 * _PG_init.  The TAP test loads various combinations to exercise
 * what happens when multiple extensions touch overlapping grammar
 * surface (same LHS, cross-extension token references, precedence
 * collisions, etc.).
 *
 * Portions Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * src/test/modules/grammar_ext_overlap/overlap_helpers.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef OVERLAP_HELPERS_H
#define OVERLAP_HELPERS_H

#include "postgres.h"

#include "common/keywords.h"
#include "miscadmin.h"
#include "parser/parser_extension.h"
#include "utils/elog.h"

typedef struct OvlToken
{
	const char *name;
	const char *lexeme;
} OvlToken;

typedef struct OvlRule
{
	const char *lhs;
	const char *rhs[8];
	const char *label;
} OvlRule;

typedef struct OvlSpec
{
	const char *name;
	const OvlToken *tokens;
	int			ntokens;
	const OvlRule *rules;
	int			nrules;
} OvlSpec;

/*
 * Shared reduce callback.  Logs which production fired with a NOTICE
 * carrying the rule's label.  The TAP test asserts on these NOTICEs
 * to confirm each extension's grammar reaches user input.
 */
static inline void
overlap_reduce(void *user_data, void *extra_arg, int nrhs,
			   const void *const *rhs_values, const int *rhs_locs,
			   void *lhs_out)
{
	const char *label = (const char *) user_data;

	(void) extra_arg;
	(void) rhs_values;
	(void) rhs_locs;

	ereport(NOTICE,
			(errmsg("overlap: %s reduced (nrhs=%d)",
					label ? label : "(?)", nrhs)));
	*(void **) lhs_out = NULL;
}

static inline void
register_overlap_extension(const OvlSpec *spec)
{
	PgGrammarExtension *ext;
	char	   *err = NULL;
	bool		ok;

	if (!process_shared_preload_libraries_in_progress)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("%s must be loaded via shared_preload_libraries",
						spec->name)));

	ext = pg_grammar_ext_create(spec->name, "1.0");

	for (int i = 0; i < spec->ntokens; i++)
	{
		const OvlToken *t = &spec->tokens[i];

		pg_grammar_ext_add_token(ext, t->name, t->lexeme,
								 UNRESERVED_KEYWORD);
	}
	for (int i = 0; i < spec->nrules; i++)
	{
		const OvlRule *r = &spec->rules[i];

		pg_grammar_ext_add_rule(ext, r->lhs, (const char **) r->rhs,
								overlap_reduce, (void *) r->label);
	}

	ok = pg_grammar_ext_register(ext, &err);
	if (ok)
		ereport(NOTICE,
				(errmsg("%s: registered (%d tokens, %d rules)",
						spec->name, spec->ntokens, spec->nrules)));
	else
	{
		ereport(WARNING,
				(errmsg("%s: register() failed: %s",
						spec->name, err ? err : "(no err)")));
		pg_grammar_ext_unregister(ext);
	}
}

#endif							/* OVERLAP_HELPERS_H */
