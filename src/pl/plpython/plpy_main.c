/*
 * PL/Python main entry points
 *
 * src/pl/plpython/plpy_main.c
 */

#include "postgres.h"

#include "catalog/pg_proc.h"
#include "commands/event_trigger.h"
#include "commands/trigger.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "plpy_elog.h"
#include "plpy_exec.h"
#include "plpy_main.h"
#include "plpy_plpymodule.h"
#include "plpy_subxactobject.h"
#include "plpy_util.h"
#include "utils/backend_runtime.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/syscache.h"

/*
 * exported functions
 */

/*
 * Backend model: process (the default).  plpython embeds a single CPython
 * interpreter whose state is process-global and GIL-serialized: the embedded
 * interpreter, the PLy_* type objects, PLy_interp_globals, the
 * PLy_execution_contexts stack, and explicit_subtransactions are all
 * unrelocated file-scope globals shared across the whole process.  None of
 * this is per-session or per-thread, so plpython is unsafe under any threaded
 * backend model and the backend-model gate correctly keeps it in
 * process-backed backends only.
 *
 * Defer with invariant: threaded plpython would need either per-session Python
 * sub-interpreters (PEP 684 / 3.12+ per-interpreter GIL) or the whole embedded
 * interpreter and PLy_* globals relocated per session -- a substantial effort
 * gated behind this PROCESS marker.  Owned by Phase 16 (bundled procedural
 * languages beyond PL/pgSQL); intentionally last of the three PLs given the
 * GIL/embedded-interpreter constraints.
 */
PG_MODULE_MAGIC_EXT(
					.name = "plpython",
					.version = PG_VERSION,
					PG_MODULE_MAGIC_BACKEND_MODEL_POOLED_PROTOCOL_AFFINE
);

PG_FUNCTION_INFO_V1(plpython3_validator);
PG_FUNCTION_INFO_V1(plpython3_call_handler);
PG_FUNCTION_INFO_V1(plpython3_inline_handler);


static PLyTrigType PLy_procedure_is_trigger(Form_pg_proc procStruct);
static void plpython_error_callback(void *arg);
static void plpython_inline_error_callback(void *arg);

static PLyExecutionContext *PLy_push_execution_context(bool atomic_context);
static void PLy_pop_execution_context(void);

/* initialize global variables */
PyObject   *PLy_interp_globals = NULL;

/*
 * Process-wide (NOT per-session): true once _PG_init has built the interpreter
 * (and released the GIL via PyEval_SaveThread).  Guards the whole _PG_init body
 * so the per-session re-invocation the loader does under multithreaded=on is a
 * clean no-op, and so PyEval_SaveThread runs exactly once per process.
 */
static bool plpython_process_inited = false;

#define plpython_reset_registered (*PgCurrentPLpythonResetRegisteredRef())

/* this doesn't need to be global; use PLy_current_execution_context() */
/*
 * Option C (threaded affine): the exec-context stack head is per-session.  The
 * frames themselves already live in TopTransactionContext/PortalContext (both
 * per-session), so only the head pointer needs relocating.  Aliased over the
 * backend_runtime per-session accessor; a no-op indirection in process mode.
 */
#define PLy_execution_contexts \
	(*(PLyExecutionContext **) PgCurrentPLpythonExecutionContextsRef())

/*
 * Session reset: drop this session's Python state.  Registered lazily on first
 * PL/Python touch (PLy_ensure_session_reset_callback).  Runs at session
 * teardown, BEFORE the transaction/portal memory contexts that hold the
 * exec-context and subxact-cell frames are destroyed, so we only null the heads
 * (the frames free with their contexts).  Takes the GIL to DECREF the session GD.
 * (Placed after the PLy_execution_contexts / explicit_subtransactions aliases so
 * those macros are in scope here.)
 */
static void
plpython_session_reset_callback(void *arg)
{
	PyObject  **gdref = (PyObject **) PgCurrentPLpythonGDRef();

	(void) arg;
	if (*gdref != NULL)
	{
		PyGILState_STATE gilstate = PyGILState_Ensure();

		Py_CLEAR(*gdref);		/* Py_XDECREF + set NULL */
		PyGILState_Release(gilstate);
	}
	/* Heads only; the frames free with their transaction/portal contexts. */
	PLy_execution_contexts = NULL;
	explicit_subtransactions = NIL;
}

/* Register the session reset callback once per session (first PL touch). */
void
PLy_ensure_session_reset_callback(void)
{
	if (!plpython_reset_registered)
	{
		PgSessionRegisterResetCallback(plpython_session_reset_callback, NULL);
		plpython_reset_registered = true;
	}
}


void
_PG_init(void)
{
	PyObject   *main_mod;
	PyObject   *main_dict;
	PyObject   *GD;
	PyObject   *plpy_mod;

	/*
	 * Do the interpreter construction exactly ONCE per process.
	 *
	 * Under multithreaded=on the loader re-invokes _PG_init once per session
	 * (dfmgr module_needs_session_init), the same way it does for plperl.  But
	 * plpython's _PG_init is entirely PROCESS-once interpreter construction
	 * (Py_Initialize, the plpy module, the GD template) -- it registers no
	 * per-session custom GUCs -- so a per-session re-run must be a clean no-op:
	 * re-running it would re-Py_Initialize/re-import/re-INCREF WITHOUT the GIL
	 * (already released by the first session's PyEval_SaveThread below) and
	 * double-release the GIL.  plpython_process_inited guards the whole body; it
	 * is flipped while the init thread still holds the GIL (before
	 * PyEval_SaveThread), so it is set-once-then-read and never re-entered
	 * mid-construction.  (The very first load runs single-threaded via the dfmgr
	 * load path; subsequent per-session touches hit this guard and return.)
	 */
	if (plpython_process_inited)
		return;

	pg_bindtextdomain(TEXTDOMAIN);

	/* Add plpy to table of built-in modules. */
	PyImport_AppendInittab("plpy", PyInit_plpy);

	/* Initialize Python interpreter. */
	Py_Initialize();

	main_mod = PyImport_AddModule("__main__");
	if (main_mod == NULL || PyErr_Occurred())
		PLy_elog(ERROR, "could not import \"%s\" module", "__main__");
	Py_INCREF(main_mod);

	main_dict = PyModule_GetDict(main_mod);
	if (main_dict == NULL)
		PLy_elog(ERROR, NULL);

	/*
	 * Set up GD.
	 */
	GD = PyDict_New();
	if (GD == NULL)
		PLy_elog(ERROR, NULL);
	PyDict_SetItemString(main_dict, "GD", GD);

	/*
	 * Import plpy.
	 */
	plpy_mod = PyImport_ImportModule("plpy");
	if (plpy_mod == NULL)
		PLy_elog(ERROR, "could not import \"%s\" module", "plpy");
	if (PyDict_SetItemString(main_dict, "plpy", plpy_mod) == -1)
		PLy_elog(ERROR, NULL);

	if (PyErr_Occurred())
		PLy_elog(FATAL, "untrapped error in initialization");

	Py_INCREF(main_dict);
	PLy_interp_globals = main_dict;

	Py_DECREF(main_mod);

	/*
	 * Option C: explicit_subtransactions and PLy_execution_contexts are now
	 * per-session (backend_runtime accessors), auto-initialized to NIL/NULL for
	 * each session, so no process-load-time reset is needed here.
	 *
	 * GIL: interpreter + exception objects + GD template are now fully built,
	 * and the init OS thread holds the GIL.  Flip the process-once guard while we
	 * still hold the GIL, then release it exactly once so any carrier OS thread
	 * can acquire it via PyGILState_Ensure() at PL entry.  We discard the
	 * returned PyThreadState* on purpose: from here on ALL Python access is
	 * bracketed by PyGILState_Ensure()/Release(), never the save/restore pairing
	 * (carriers are distinct OS threads).
	 */
	plpython_process_inited = true;
	(void) PyEval_SaveThread();
}

Datum
plpython3_validator(PG_FUNCTION_ARGS)
{
	LOCAL_FCINFO(fake_fcinfo, 0);
	Oid			funcoid = PG_GETARG_OID(0);
	HeapTuple	tuple;
	Form_pg_proc procStruct;
	PLyTrigType is_trigger;
	TriggerData trigdata;
	EventTriggerData etrigdata;
	FmgrInfo	flinfo;
	PLyProcedureCache *pcache;

	if (!CheckFunctionValidatorAccess(fcinfo->flinfo->fn_oid, funcoid))
		PG_RETURN_VOID();

	if (!check_function_bodies)
		PG_RETURN_VOID();

	/* Get the new function's pg_proc entry */
	tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcoid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for function %u", funcoid);
	procStruct = (Form_pg_proc) GETSTRUCT(tuple);

	is_trigger = PLy_procedure_is_trigger(procStruct);

	ReleaseSysCache(tuple);

	/*
	 * Set up a fake flinfo/fcinfo with just enough info to satisfy
	 * PLy_procedure_get().  That function derives the call context (plain
	 * function, DML trigger, or event trigger) from the fcinfo, so we have to
	 * construct matching context here.
	 */
	MemSet(fake_fcinfo, 0, SizeForFunctionCallInfo(0));
	MemSet(&flinfo, 0, sizeof(flinfo));
	fake_fcinfo->flinfo = &flinfo;
	flinfo.fn_oid = funcoid;
	flinfo.fn_mcxt = CurrentMemoryContext;

	if (is_trigger == PLPY_TRIGGER)
	{
		MemSet(&trigdata, 0, sizeof(trigdata));
		trigdata.type = T_TriggerData;
		/* We can't validate triggers against any particular table ... */
		fake_fcinfo->context = (Node *) &trigdata;
	}
	else if (is_trigger == PLPY_EVENT_TRIGGER)
	{
		MemSet(&etrigdata, 0, sizeof(etrigdata));
		etrigdata.type = T_EventTriggerData;
		fake_fcinfo->context = (Node *) &etrigdata;
	}

	{
		PyGILState_STATE gilstate = PyGILState_Ensure();

		PG_TRY();
		{
			pcache = PLy_procedure_get(fake_fcinfo, true);

			/*
			 * Release the reference count that PLy_procedure_get acquired; the
			 * PLyProcedure object remains valid for possible future use.  (We
			 * could leave this to be done when the calling memory context is
			 * cleaned up, but it seems neater to do it right away.  Note we
			 * mustn't release the pcache object, since the memory-context reset
			 * callback has a reference to it.)
			 */
			Assert(pcache->proc->cfunc.use_count > 0);
			pcache->proc->cfunc.use_count--;
			pcache->proc = NULL;
		}
		PG_FINALLY();
		{
			PyGILState_Release(gilstate);
		}
		PG_END_TRY();
	}

	PG_RETURN_VOID();
}

Datum
plpython3_call_handler(PG_FUNCTION_ARGS)
{
	bool		nonatomic;
	Datum		retval;
	PLyExecutionContext *exec_ctx;
	ErrorContextCallback plerrcontext;
	PyGILState_STATE gilstate;

	nonatomic = fcinfo->context &&
		IsA(fcinfo->context, CallContext) &&
		!castNode(CallContext, fcinfo->context)->atomic;

	/*
	 * Acquire the GIL outermost, enclosing SPI connect/finish and the
	 * exec-context push/pop.  Reference-counted per OS thread, so nested
	 * PL/Python via SPI just bumps the count; released on every exit path by the
	 * outer PG_FINALLY.  (Whole-call GIL hold: a plpython function blocking in
	 * SPI stalls Python on other carriers for that duration --
	 * ponytail: drop-GIL-around-SPI-wait is a throughput optimization for later,
	 * gated on wait-boundary scheduler integration, not a correctness need.)
	 */
	gilstate = PyGILState_Ensure();

	PG_TRY();
	{
	/* Note: SPI_finish() happens in plpy_exec.c, which is dubious design */
	SPI_connect_ext(nonatomic ? SPI_OPT_NONATOMIC : 0);

	/*
	 * Push execution context onto stack.  It is important that this get
	 * popped again, so avoid putting anything that could throw error between
	 * here and the PG_TRY.
	 */
	exec_ctx = PLy_push_execution_context(!nonatomic);

	PG_TRY();
	{
		PLyProcedureCache *pcache;

		/*
		 * Setup error traceback support for ereport().  Note that the PG_TRY
		 * structure pops this for us again at exit, so we needn't do that
		 * explicitly, nor do we risk the callback getting called after we've
		 * destroyed the exec_ctx.
		 */
		plerrcontext.callback = plpython_error_callback;
		plerrcontext.arg = exec_ctx;
		plerrcontext.previous = error_context_stack;
		error_context_stack = &plerrcontext;

		/*
		 * Look up (and if necessary compile) the procedure.  This can throw
		 * an error, so it must happen inside the PG_TRY so that the execution
		 * context gets popped on the way out.
		 */
		pcache = PLy_procedure_get(fcinfo, false);
		exec_ctx->curr_proc = pcache->proc;

		if (CALLED_AS_TRIGGER(fcinfo))
		{
			HeapTuple	trv;

			trv = PLy_exec_trigger(fcinfo, pcache->proc);
			retval = PointerGetDatum(trv);
		}
		else if (CALLED_AS_EVENT_TRIGGER(fcinfo))
		{
			PLy_exec_event_trigger(fcinfo, pcache->proc);
			retval = (Datum) 0;
		}
		else
			retval = PLy_exec_function(fcinfo, pcache);
	}
	PG_CATCH();
	{
		/* Destroy the execution context */
		PLy_pop_execution_context();
		PyErr_Clear();

		PG_RE_THROW();
	}
	PG_END_TRY();

	/* Destroy the execution context */
	PLy_pop_execution_context();
	}
	PG_FINALLY();
	{
		/* Release the GIL on every exit path (normal + rethrow). */
		PyGILState_Release(gilstate);
	}
	PG_END_TRY();

	return retval;
}

Datum
plpython3_inline_handler(PG_FUNCTION_ARGS)
{
	LOCAL_FCINFO(fake_fcinfo, 0);
	InlineCodeBlock *codeblock = (InlineCodeBlock *) DatumGetPointer(PG_GETARG_DATUM(0));
	FmgrInfo	flinfo;
	PLyProcedure proc;
	PLyProcedureCache pcache;
	PLyExecutionContext *exec_ctx;
	ErrorContextCallback plerrcontext;
	PyGILState_STATE gilstate;

	/* Acquire the GIL outermost (see plpython3_call_handler). */
	gilstate = PyGILState_Ensure();

	PG_TRY();
	{
	/* Note: SPI_finish() happens in plpy_exec.c, which is dubious design */
	SPI_connect_ext(codeblock->atomic ? 0 : SPI_OPT_NONATOMIC);

	MemSet(fcinfo, 0, SizeForFunctionCallInfo(0));
	MemSet(&flinfo, 0, sizeof(flinfo));
	fake_fcinfo->flinfo = &flinfo;
	flinfo.fn_oid = InvalidOid;
	flinfo.fn_mcxt = CurrentMemoryContext;

	MemSet(&proc, 0, sizeof(PLyProcedure));
	proc.mcxt = AllocSetContextCreate(
		PgRuntimeGetOwnedMemoryContextWithSizes(
			PgCurrentPLpythonMemoryContextRef(),
			"PL/Python session",
			ALLOCSET_DEFAULT_SIZES),
		"__plpython_inline_block",
		ALLOCSET_DEFAULT_SIZES);
	proc.pyname = MemoryContextStrdup(proc.mcxt, "__plpython_inline_block");
	proc.langid = codeblock->langOid;

	/*
	 * This is currently sufficient to get PLy_exec_function to work, but
	 * someday we might need to be honest and use PLy_output_setup_func.
	 */
	proc.result.typoid = VOIDOID;

	/* Set up a minimal PLyProcedureCache for the inline block */
	MemSet(&pcache, 0, sizeof(PLyProcedureCache));
	pcache.proc = &proc;
	pcache.fcontext = CurrentMemoryContext;

	/*
	 * Push execution context onto stack.  It is important that this get
	 * popped again, so avoid putting anything that could throw error between
	 * here and the PG_TRY.
	 */
	exec_ctx = PLy_push_execution_context(codeblock->atomic);

	PG_TRY();
	{
		/*
		 * Setup error traceback support for ereport().
		 * plpython_inline_error_callback doesn't currently need exec_ctx, but
		 * for consistency with plpython3_call_handler we do it the same way.
		 */
		plerrcontext.callback = plpython_inline_error_callback;
		plerrcontext.arg = exec_ctx;
		plerrcontext.previous = error_context_stack;
		error_context_stack = &plerrcontext;

		PLy_procedure_compile(&proc, codeblock->source_text);
		exec_ctx->curr_proc = &proc;
		PLy_exec_function(fake_fcinfo, &pcache);
	}
	PG_CATCH();
	{
		PLy_pop_execution_context();
		PLy_procedure_delete(&proc);
		PyErr_Clear();
		PG_RE_THROW();
	}
	PG_END_TRY();

	/* Destroy the execution context */
	PLy_pop_execution_context();

	/* Now clean up the transient procedure we made */
	PLy_procedure_delete(&proc);
	}
	PG_FINALLY();
	{
		PyGILState_Release(gilstate);
	}
	PG_END_TRY();

	PG_RETURN_VOID();
}

/*
 * Determine whether a function is a (DML or event) trigger from its pg_proc
 * result type.  This is used by the validator, which has no call context to
 * inspect; the call handler instead relies on the fcinfo's call context.
 */
static PLyTrigType
PLy_procedure_is_trigger(Form_pg_proc procStruct)
{
	PLyTrigType ret;

	switch (procStruct->prorettype)
	{
		case TRIGGEROID:
			ret = PLPY_TRIGGER;
			break;
		case EVENT_TRIGGEROID:
			ret = PLPY_EVENT_TRIGGER;
			break;
		default:
			ret = PLPY_NOT_TRIGGER;
			break;
	}

	return ret;
}

static void
plpython_error_callback(void *arg)
{
	PLyExecutionContext *exec_ctx = (PLyExecutionContext *) arg;

	if (exec_ctx->curr_proc)
	{
		if (exec_ctx->curr_proc->is_procedure)
			errcontext("PL/Python procedure \"%s\"",
					   PLy_procedure_name(exec_ctx->curr_proc));
		else
			errcontext("PL/Python function \"%s\"",
					   PLy_procedure_name(exec_ctx->curr_proc));
	}
}

static void
plpython_inline_error_callback(void *arg)
{
	errcontext("PL/Python anonymous code block");
}

PLyExecutionContext *
PLy_current_execution_context(void)
{
	Assert(PyGILState_Check());	/* a PL entry point must have taken the GIL */
	if (PLy_execution_contexts == NULL)
		elog(ERROR, "no Python function is currently executing");

	return PLy_execution_contexts;
}

MemoryContext
PLy_get_scratch_context(PLyExecutionContext *context)
{
	/*
	 * A scratch context might never be needed in a given plpython procedure,
	 * so allocate it on first request.
	 */
	if (context->scratch_ctx == NULL)
		context->scratch_ctx =
			AllocSetContextCreate(TopTransactionContext,
								  "PL/Python scratch context",
								  ALLOCSET_DEFAULT_SIZES);
	return context->scratch_ctx;
}

static PLyExecutionContext *
PLy_push_execution_context(bool atomic_context)
{
	PLyExecutionContext *context;

	/* Pick a memory context similar to what SPI uses. */
	context = (PLyExecutionContext *)
		MemoryContextAlloc(atomic_context ? TopTransactionContext : PortalContext,
						   sizeof(PLyExecutionContext));
	context->curr_proc = NULL;
	context->scratch_ctx = NULL;
	context->next = PLy_execution_contexts;
	PLy_execution_contexts = context;
	return context;
}

static void
PLy_pop_execution_context(void)
{
	PLyExecutionContext *context = PLy_execution_contexts;

	if (context == NULL)
		elog(ERROR, "no Python function is currently executing");

	PLy_execution_contexts = context->next;

	if (context->scratch_ctx)
		MemoryContextDelete(context->scratch_ctx);
	pfree(context);
}
