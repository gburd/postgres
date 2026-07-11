/*-------------------------------------------------------------------------
 *
 * jit.c
 *	  Provider independent JIT infrastructure.
 *
 * Code related to loading JIT providers, redirecting calls into JIT providers
 * and error handling.  No code specific to a specific JIT implementation
 * should end up here.
 *
 *
 * Copyright (c) 2016-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/jit/jit.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "fmgr.h"
#include "jit/jit.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "portability/instr_time.h"
#include "storage/fd.h"
#include "utils/fmgrprotos.h"

/*
 * JIT GUC state lives in PgSessionJitGUCState.  Public names remain available
 * through compatibility macros in jit/jit.h.
 */
#define jit_provider_callbacks (*PgCurrentJitProviderCallbacksRef())
#define jit_provider_callbacks_loaded (*PgCurrentJitProviderSuccessfullyLoadedRef())
#define jit_provider_callbacks_failed_loading (*PgCurrentJitProviderFailedLoadingRef())


static bool provider_init(void);


/*
 * SQL level function returning whether JIT is available in the current
 * backend. Will attempt to load JIT provider if necessary.
 */
Datum
pg_jit_available(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(provider_init());
}


/*
 * Return whether a JIT provider has successfully been loaded, caching the
 * result.
 */
static bool
provider_init(void)
{
	char		path[MAXPGPATH];
	JitProviderInit init;

	/* don't even try to load if not enabled */
	if (!jit_enabled)
		return false;

	/*
	 * Don't retry loading after failing - attempting to load JIT provider
	 * isn't cheap.
	 */
	if (jit_provider_callbacks_failed_loading)
		return false;
	if (jit_provider_callbacks_loaded)
		return true;

	/*
	 * Check whether shared library exists. We do that check before actually
	 * attempting to load the shared library (via load_external_function()),
	 * because that'd error out in case the shlib isn't available.
	 */
	snprintf(path, MAXPGPATH, "%s/%s%s", pkglib_path, jit_provider, DLSUFFIX);
	elog(DEBUG1, "probing availability of JIT provider at %s", path);
	if (!pg_file_exists(path))
	{
		elog(DEBUG1,
			 "JIT provider not available, disabling JIT for current session");
		jit_provider_callbacks_failed_loading = true;
		return false;
	}

	/*
	 * If loading functions fails, signal failure. We do so because
	 * load_external_function() might error out despite the above check if
	 * e.g. the library's dependencies aren't installed. We want to signal
	 * ERROR in that case, so the user is notified, but we don't want to
	 * continually retry.
	 */
	jit_provider_callbacks_failed_loading = true;

	/* and initialize */
	init = (JitProviderInit)
		load_external_function(path, "_PG_jit_provider_init", true, NULL);
	init(&jit_provider_callbacks);

	jit_provider_callbacks_loaded = true;
	jit_provider_callbacks_failed_loading = false;

	elog(DEBUG1, "successfully loaded JIT provider in current session");

	return true;
}

/*
 * Reset JIT provider's error handling. This'll be called after an error has
 * been thrown and the main-loop has re-established control.
 */
void
jit_reset_after_error(void)
{
	if (jit_provider_callbacks_loaded)
		jit_provider_callbacks.reset_after_error();
}

/*
 * Release resources required by one JIT context.
 */
void
jit_release_context(JitContext *context)
{
	if (jit_provider_callbacks_loaded)
		jit_provider_callbacks.release_context(context);

	pfree(context);
}

/*
 * Ask the provider to JIT compile an expression.
 *
 * Returns true if successful, false if not.
 */
bool
jit_compile_expr(struct ExprState *state)
{
	/*
	 * We can easily create a one-off context for functions without an
	 * associated PlanState (and thus EState). But because there's no executor
	 * shutdown callback that could deallocate the created function, they'd
	 * live to the end of the transactions, where they'd be cleaned up by the
	 * resowner machinery. That can lead to a noticeable amount of memory
	 * usage, and worse, trigger some quadratic behaviour in gdb. Therefore,
	 * at least for now, don't create a JITed function in those circumstances.
	 */
	if (!state->parent)
		return false;

	/* if no jitting should be performed at all */
	if (!(state->parent->state->es_jit_flags & PGJIT_PERFORM))
		return false;

	/* or if expressions aren't JITed */
	if (!(state->parent->state->es_jit_flags & PGJIT_EXPR))
		return false;

	/* this also takes !jit_enabled into account */
	if (provider_init())
		return jit_provider_callbacks.compile_expr(state);

	return false;
}

/* Aggregate JIT instrumentation information */
void
InstrJitAgg(JitInstrumentation *dst, JitInstrumentation *add)
{
	dst->created_functions += add->created_functions;
	INSTR_TIME_ADD(dst->generation_counter, add->generation_counter);
	INSTR_TIME_ADD(dst->deform_counter, add->deform_counter);
	INSTR_TIME_ADD(dst->inlining_counter, add->inlining_counter);
	INSTR_TIME_ADD(dst->optimization_counter, add->optimization_counter);
	INSTR_TIME_ADD(dst->emission_counter, add->emission_counter);
}
