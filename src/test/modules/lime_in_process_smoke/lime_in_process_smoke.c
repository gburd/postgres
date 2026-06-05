/*-------------------------------------------------------------------------
 *
 * lime_in_process_smoke.c
 *	  Smoke test for Lime's in-process compile API (the foundation
 *	  for Phase 4 Track B Phase 2).
 *
 *	  Exposes one SQL function:
 *	    lime_in_process_compile(grammar_text text) RETURNS text
 *
 *	  Calls lime_compile_grammar_in_process() on the supplied
 *	  grammar text and returns either "ok: snapshot built" or
 *	  "error: <msg>".  Verifies the runtime library is linked,
 *	  the API entry point resolves, and a trivial grammar
 *	  round-trips through the in-process compile path.
 *
 *	  This module is the smallest unit of forward progress on
 *	  Phase 4 Track B Phase 2 -- it links liblime_parser.a into a
 *	  test .so without touching parser.c or parser_extension.c,
 *	  proving the build wiring works before we attempt the much
 *	  more invasive parser.c surgery.
 *
 *	  See .agent/notes/track-b-phase2-design.md for the full
 *	  refactor plan.
 *
 * Portions Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * src/test/modules/lime_in_process_smoke/lime_in_process_smoke.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "varatt.h"

#include <lime/lime_compiler.h>
#include <lime/parser.h>

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(lime_in_process_compile);

Datum
lime_in_process_compile(PG_FUNCTION_ARGS)
{
	text	   *grammar_text = PG_GETARG_TEXT_PP(0);
	char	   *grammar;
	size_t		len;
	struct ParserSnapshot *snap = NULL;
	char	   *err = NULL;
	int			rc;
	StringInfoData out;

	/*
	 * lime_compile_grammar_in_process is the TRUE in-process compile API
	 * exposed by liblime_compiler.a (NOT the lime_compile_grammar_- text
	 * wrapper in liblime_parser.a, which falls back to the subprocess
	 * pipeline when the compiler library isn't linked).
	 *
	 * The build wires both via dependency('lime') +
	 * dependency('lime-compiler') so the strong definition resolves here
	 * rather than the weak no-op stub.
	 *
	 * NUL-termination required.  text_to_cstring palloc's a copy.
	 */
	grammar = text_to_cstring(grammar_text);
	len = strlen(grammar);

	rc = lime_compile_grammar_in_process(grammar, len, &snap, &err);

	initStringInfo(&out);
	if (rc == 0 && snap != NULL)
	{
		appendStringInfo(&out, "ok: snapshot built (snap=%p)",
						 (void *) snap);
		lime_snapshot_release(snap);
	}
	else
	{
		appendStringInfo(&out, "error: rc=%d msg=%s",
						 rc, err ? err : "(no message)");
		if (err)
			free(err);
	}

	PG_RETURN_TEXT_P(cstring_to_text(out.data));
}
