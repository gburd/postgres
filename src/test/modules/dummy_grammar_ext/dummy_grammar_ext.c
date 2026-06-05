/*-------------------------------------------------------------------------
 *
 * dummy_grammar_ext.c
 *	  Smoke test for the parser_extension.h API.
 *
 * Loads via shared_preload_libraries.  Calls the API in _PG_init() and
 * exercises pg_grammar_ext_register(), which in Phase 4 Track A queues
 * the extension for rebuild.  The actual subprocess pipeline runs the
 * first time raw_parser() is called -- by that time the postmaster's
 * forked backend has DataDir set, so the cache directory under
 * $PGDATA/pg_parser_cache is reachable.
 *
 * The extension declares a noisy keyword-flavored token (K_DUMMY,
 * lexeme "dummy") plus a placeholder rule so the rebuilt .lime
 * exercises Lime's emit path for both directives.  The rule is
 * intentionally an unreachable orphan: its LHS is a fresh non-terminal
 * ("dummy_stmt") that no base-grammar production references, which
 * means the rebuilt parser accepts exactly the same input set as the
 * static one.  That keeps the smoke test from turning into a sweeping
 * grammar change while still proving that:
 *
 *   1. register() succeeds (returns true, err is NULL),
 *   2. the queued fragment hashes into a cache key,
 *   3. the cached .so dlopens, and
 *   4. raw_parser() dispatches through the dlopen'd base_yyparse on
 *      every subsequent parse.
 *
 * The pipeline-level signals (LOG entries from parser_extension.c) are
 * the primary verification.  The NOTICE messages here cross-check
 * register() succeeded so the test harness can grep for them.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/test/modules/dummy_grammar_ext/dummy_grammar_ext.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "common/keywords.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "parser/parser_extension.h"
#include "utils/elog.h"
#include "utils/guc.h"

PG_MODULE_MAGIC;

void		_PG_init(void);

/*
 * dummy_reduce
 *	  Reduce callback for the test rule.  Track A's subprocess pipeline
 *	  does not yet implement the dispatch trampoline that calls back
 *	  into PgGrammarReduceFn from the rebuilt .so, so this is wired but
 *	  unreachable at runtime.  Track B (in-process snapshot patching)
 *	  will exercise this path; until then the body is documentation.
 */
static void
dummy_reduce(void *user_data, void *extra_arg, int nrhs,
			 const void *const *rhs_values, const int *rhs_locs,
			 void *lhs_out)
{
	/*
	 * The trampoline now actually fires this callback when the rebuilt parser
	 * sees `dummy` as a statement.  We log a NOTICE so the standalone-backend
	 * smoke test can verify dispatch and clear the LHS slot (the orphan
	 * rule's LHS is `stmt` which carries Node *).
	 */
	ereport(NOTICE,
			(errmsg("dummy_grammar_ext: dummy_reduce fired "
					"(nrhs=%d, user_data=%p, extra_arg=%p)",
					nrhs, user_data, extra_arg)));
	(void) rhs_values;
	(void) rhs_locs;
	*(void **) lhs_out = NULL;
}

void
_PG_init(void)
{
	PgGrammarExtension *ext;

	/*
	 * The new alternative wires K_DUMMY (a brand-new token) into the base
	 * grammar's `stmt` non-terminal.  K_DUMMY is unreachable from real input
	 * -- the scanner's keyword table is unchanged -- so the rule is reachable
	 * from Lime's start symbol (which is what Lime's reachability check
	 * requires) without altering the parsed-input set.  Track A's job here is
	 * to prove the rebuild pipeline runs end-to-end and the dlopen'd parser
	 * drops in cleanly; full token-table integration is Track B's problem.
	 */
	const char *rhs[] = {"K_DUMMY", NULL};
	char	   *err = NULL;
	bool		ok;

	if (!process_shared_preload_libraries_in_progress)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("dummy_grammar_ext must be loaded via shared_preload_libraries")));

	ext = pg_grammar_ext_create("dummy_grammar_ext", "1.0");

	pg_grammar_ext_add_token(ext, "K_DUMMY", "dummy", UNRESERVED_KEYWORD);
	pg_grammar_ext_add_rule(ext, "stmt", rhs, dummy_reduce, NULL);

	ok = pg_grammar_ext_register(ext, &err);
	if (ok)
	{
		const char *frag = pg_grammar_ext_get_serialized_lime(ext);

		ereport(NOTICE,
				(errmsg("dummy_grammar_ext: register() succeeded; "
						"rebuild will run on first parse")));
		if (frag != NULL)
			ereport(DEBUG1,
					(errmsg("dummy_grammar_ext: serialized .lime fragment:\n%s",
							frag)));
	}
	else
	{
		ereport(WARNING,
				(errmsg("dummy_grammar_ext: register() failed: %s",
						err ? err : "(no error message)")));
		pg_grammar_ext_unregister(ext);
	}
}
