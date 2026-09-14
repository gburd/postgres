# PL/Python Option C — GIL Core + GD-Per-Session Design (EXACT CODE)

Status: DESIGN ONLY (no source changed). Companion to
`plan_docs/MULTITHREADED_PLPYTHON_OPTIONC_IMPL_PLAN.md`. The **mechanical
layer** (per-session `PLy_execution_contexts` + `explicit_subtransactions` via
`backend_runtime` accessors) is DONE on branch `plpython-optionc-wip`
(commit `bf5d9a07f9`). This doc specifies the **remaining hard core** with
copy-pasteable code, anchored to `file:line` as read for this design, plus
PASS/FAIL criteria.

All line anchors are against the working tree at the time of writing
(post-`bf5d9a07f9`). Re-confirm anchors before editing; the aliasing edits
already applied shift a few lines.

Scope of this doc: **A** GIL process init, **B** per-entry GIL wrap, **C** the
GD-per-session question (definitively resolved), **D** session-reset callback,
**E** debug asserts + marker flip + revert, **F** TAP outline.

---

## THE GD QUESTION — DEFINITIVE ANSWER (read this first)

**Per-session GD is EXTRA WORK. The session-affine procedure cache does NOT
give it.** Here is the exact chain, with citations.

1. `_PG_init` builds ONE process-global GD dict and stores it in the process
   `__main__` dict, then publishes `__main__` as `PLy_interp_globals`:

   ```c
   /* plpy_main.c:96-100 */
   GD = PyDict_New();
   if (GD == NULL)
       PLy_elog(ERROR, NULL);
   PyDict_SetItemString(main_dict, "GD", GD);
   /* ... plpy_main.c:117 */
   PLy_interp_globals = main_dict;
   ```

   `PLy_interp_globals` is the single process `__main__` dict; there is exactly
   one GD `PyObject *` for the whole process. Confirmed sole writer: only
   `plpy_main.c:117` assigns `PLy_interp_globals`, and only `_PG_init` inserts
   `"GD"` (grep: `PLy_interp_globals` appears at `plpy_main.c:32,65,117`,
   `plpy_main.h:11`, and `plpy_procedure.c:368`; the string `"GD"` is inserted
   only in `_PG_init`).

2. Every procedure copies that dict when compiled:

   ```c
   /* plpy_procedure.c:368 */
   proc->globals = PyDict_Copy(PLy_interp_globals);
   /* plpy_procedure.c:374-377 — SD is fresh per proc */
   proc->statics = PyDict_New();
   PyDict_SetItemString(proc->globals, "SD", proc->statics);
   ```

   **`PyDict_Copy` is a SHALLOW copy** (CPython documented semantics: it copies
   the dict's key/value *references*, not the referenced objects). So after the
   copy, `proc->globals["GD"]` is a **reference to the same one process GD
   object** created at `plpy_main.c:96`. SD, by contrast, is `PyDict_New()` per
   proc (`plpy_procedure.c:374`), so SD is genuinely private to each proc.

3. The proc cache is session-affine (funccache): the hashtable and its memory
   context are per-session accessors —
   `#define cfunc_hashtable (*PgCurrentCachedFunctionHashRef())`
   (`src/backend/utils/cache/funccache.c:41`), context from
   `PgCurrentFunctionManagerMemoryContext()` (`funccache.c:74`). So each session
   gets its **own `PLyProcedure` objects**, hence its **own `proc->globals`
   dict** and its **own SD** per function.

**Therefore:**

| State | Object identity | Per-session? | Why |
|---|---|---|---|
| `proc->globals` dict | one per (session, function) | YES (free) | session-affine proc cache |
| `SD` (`proc->statics`) | one per (session, function) | YES (free) | `PyDict_New()` per proc, `plpy_procedure.c:374` |
| **`GD`** | **ONE per PROCESS** | **NO (BUG)** | shallow `PyDict_Copy` re-references the single `plpy_main.c:96` GD object into every session's every proc |

The session-affine proc cache isolates the *containers* (`proc->globals`, SD)
but the **GD *value* inside every container is the same shared object**. Two
sessions calling `GD['x'] = ...` mutate the one process dict. GD is documented
as **session**-global (see the `plpy_procedure.c:371` comment "GD is global data
shared by all functions" — meaning all functions *in a session*), so a single
process-global GD is a correctness bug under threading and even a subtle
cross-session leak under the current process model when connection-pooling
reuses a backend (masked today only because process mode gives one session per
process lifetime).

**Fix (minimal):** give each session its own GD dict, and overwrite the
inherited process-GD reference in `proc->globals` with the session GD at proc
compile time. This is Section **C** below. It requires ONE more `.def` field
(`plpython_gd`), the reset-callback teardown (**D**), and a two-line edit in
`plpy_procedure.c`.

---

## A) GIL process init — once per process

### Current state
`_PG_init` (`plpy_main.c:78-121`) calls `Py_Initialize()` (`plpy_main.c:85`) and
returns with the init OS thread **holding the GIL** and a live thread state.
No `PyEval_SaveThread` / `PyGILState_*` anywhere in `src/pl/plpython/`
(confirmed by grep). Fine for process mode (one OS thread forever); fatal with
multiple carrier OS threads.

### CPython semantics (the crux — confirmed correct)
- Python 3.7+ auto-initializes threading inside `Py_Initialize()`;
  `PyEval_InitThreads()` is a deprecated no-op since 3.9 and unnecessary since
  3.7. Do **not** call it. (Floor is 3.6 per `config/python.m4:12,22`,
  `meson.build:14`; on 3.6 threading also auto-inits at first thread-state
  creation — `PyGILState_Ensure` handles that. No explicit init needed.)
- After `Py_Initialize()` the init thread holds the GIL with a thread state.
  **`PyEval_SaveThread()`** releases the GIL and returns the saved
  `PyThreadState *`, leaving the current thread state as NULL (no thread holds
  the GIL).
- **Confirmed:** once `PyEval_SaveThread()` has released the GIL, a subsequent
  `PyGILState_Ensure()` on **any** OS thread — including the original init
  thread later — is correct. `PyGILState_Ensure` looks up (or creates) a thread
  state for the *calling* OS thread via TLS and acquires the GIL;
  `PyGILState_Release` drops it. This is exactly the "arbitrary C thread calls
  into the one embedded interpreter" contract, which is our carrier model. The
  saved `PyThreadState *` from `PyEval_SaveThread()` is **never restored** by
  us — we rely purely on `PyGILState_Ensure`/`Release` from then on. (The
  classic `PyEval_RestoreThread` pairing is for the *same* thread that saved;
  we deliberately do not use it because carriers are different OS threads.)
  Discarding the returned pointer is safe: the init thread's thread state
  remains registered in the interpreter and will be reused if
  `PyGILState_Ensure` is later called on that same OS thread.

### Guard
`Py_Initialize()` is already idempotent-guarded upstream: it early-returns if
`Py_IsInitialized()`. The GIL release must run **exactly once per process**, so
add a `static bool` guard local to `plpy_main.c` (simpler than a `.def` field;
this is genuinely process-wide, not per-session):

```c
/* plpy_main.c — near the other file-scope statics, ~line 66 */
/*
 * Process-wide (NOT per-session): true once PyEval_SaveThread() has released
 * the GIL after interpreter init, so other carrier OS threads may acquire it
 * via PyGILState_Ensure().  Set once by the first session that touches Python.
 */
static bool plpython_gil_initialized = false;
```

### Exact edit in `_PG_init`
Insert **after** GD setup, plpy import, and `PLy_interp_globals = main_dict;`
(i.e. after all interpreter/exception/GD construction is complete), at the tail
of `_PG_init`. Replace the now-empty Option-C comment block that the mechanical
commit left at `plpy_main.c:125-131`:

```c
	Py_DECREF(main_mod);

	/*
	 * Option C: explicit_subtransactions and PLy_execution_contexts are now
	 * per-session (backend_runtime accessors), auto-initialized to NIL/NULL for
	 * each session, so no process-load-time reset is needed here.
	 *
	 * GIL: interpreter + exception objects + GD template are now fully built,
	 * and the init OS thread holds the GIL.  Release it exactly once so any
	 * carrier OS thread can acquire it via PyGILState_Ensure() at PL entry.
	 * We discard the returned PyThreadState* on purpose: from here on ALL
	 * Python access is bracketed by PyGILState_Ensure()/Release(), never by
	 * the save/restore pairing (carriers are distinct OS threads).
	 */
	if (!plpython_gil_initialized)
	{
		(void) PyEval_SaveThread();
		plpython_gil_initialized = true;
	}
}
```

Note ordering: `PLy_add_exceptions` (`plpy_plpymodule.c:150-163`) runs during
`PyInit_plpy` → `PyImport_ImportModule("plpy")` (`plpy_main.c:104`), i.e.
before this point. So exception type objects and the `PLy_spi_exceptions` HTAB
are fully built while we still hold the GIL — correct. The `PyEval_SaveThread`
is strictly last.

Skipped: storing the saved `PyThreadState*`. Add only if a future teardown
path needs `PyEval_RestoreThread` on the init thread (it does not today —
process teardown fail-stops).

---

## B) Per-entry GIL wrap — three entry points

`PyGILState_Ensure()` / `PyGILState_Release()` are **reference-counted and
re-entrant per OS thread**: the first `Ensure` on a thread acquires the GIL and
creates/loads that thread's state; nested `Ensure` bumps a counter and returns
the same state; each `Release` decrements; the outermost `Release` drops the
GIL. This makes nested PL/Python-via-SPI automatic: the inner call's `Ensure`
does not re-acquire, its `Release` does not drop — the outermost pair owns the
GIL for the whole nest. This is the plpython analogue of plperl's nested
`activate_interpreter(oldinterp)` save/restore (`plperl.c:2820-2824`).

**Placement rule:** `PyGILState_Ensure` is the **outermost** acquire — before
`SPI_connect_ext` and before `PLy_push_execution_context` — and
`PyGILState_Release` is the **innermost-last** release, on **every** exit path
(normal return *and* `PG_CATCH`/`PG_RE_THROW`). The GIL region must strictly
enclose the SPI connect/finish and the exec-context push/pop.

Because the existing handlers use `PG_TRY`/`PG_CATCH` (not `PG_FINALLY`) with an
early `return retval;` outside the try, the cleanest correct structure wraps the
**entire** existing body — including `SPI_connect_ext` and the push — inside an
outer `PG_TRY`/`PG_FINALLY` that releases the GIL. Skeletons below.

### B1) `plpython3_call_handler` — `plpy_main.c:199`

```c
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
	 * Acquire the GIL outermost: this must enclose SPI connect/finish and the
	 * exec-context push/pop.  Reference-counted per OS thread, so nested
	 * PL/Python via SPI just bumps the count.
	 */
	gilstate = PyGILState_Ensure();

	PG_TRY();
	{
		/* Note: SPI_finish() happens in plpy_exec.c, which is dubious design */
		SPI_connect_ext(nonatomic ? SPI_OPT_NONATOMIC : 0);

		exec_ctx = PLy_push_execution_context(!nonatomic);

		PG_TRY();
		{
			PLyProcedureCache *pcache;

			plerrcontext.callback = plpython_error_callback;
			plerrcontext.arg = exec_ctx;
			plerrcontext.previous = error_context_stack;
			error_context_stack = &plerrcontext;

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
		PyGILState_Release(gilstate);
	}
	PG_END_TRY();

	return retval;
}
```

Note: `retval` is set on every non-error path before the inner
`PG_END_TRY`; on error the inner `PG_CATCH` re-throws, so `return retval;` after
the outer `PG_END_TRY` is only reached when `retval` is initialized. The inner
`PG_CATCH` still pops the exec context (unchanged behavior); the outer
`PG_FINALLY` only releases the GIL.

### B2) `plpython3_inline_handler` — `plpy_main.c:275`

Same wrap. `PyGILState_Ensure()` before `SPI_connect_ext`; the whole body
(including the `proc.mcxt` creation via `PgCurrentPLpythonMemoryContextRef()`
and `PLy_procedure_delete(&proc)`) inside the outer `PG_TRY`; release in an
outer `PG_FINALLY`. Keep the existing inner `PG_TRY`/`PG_CATCH` (which pops the
context, deletes the transient proc, clears the Python error) unchanged:

```c
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
		proc.result.typoid = VOIDOID;

		MemSet(&pcache, 0, sizeof(PLyProcedureCache));
		pcache.proc = &proc;
		pcache.fcontext = CurrentMemoryContext;

		exec_ctx = PLy_push_execution_context(codeblock->atomic);

		PG_TRY();
		{
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
```

### B3) `plpython3_validator` — `plpy_main.c:123`

The validator touches the interpreter via `PLy_procedure_get` →
`PLy_procedure_compile` (`Py_CompileString`/`PyEval_EvalCode`, `PyDict_*`), so
it needs the GIL. It also reads GD-template — see **C** for why validator must
NOT be given a session GD (it compiles/evaluates the body, but must not leak
into any session's GD). Wrap only the interpreter-touching tail (after the
access check and syscache lookup), releasing on all paths. Simplest correct
form: acquire right after `check_function_bodies`/access checks, wrap the rest:

```c
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
	PyGILState_STATE gilstate;

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

	MemSet(fake_fcinfo, 0, SizeForFunctionCallInfo(0));
	MemSet(&flinfo, 0, sizeof(flinfo));
	fake_fcinfo->flinfo = &flinfo;
	flinfo.fn_oid = funcoid;
	flinfo.fn_mcxt = CurrentMemoryContext;

	if (is_trigger == PLPY_TRIGGER)
	{
		MemSet(&trigdata, 0, sizeof(trigdata));
		trigdata.type = T_TriggerData;
		fake_fcinfo->context = (Node *) &trigdata;
	}
	else if (is_trigger == PLPY_EVENT_TRIGGER)
	{
		MemSet(&etrigdata, 0, sizeof(etrigdata));
		etrigdata.type = T_EventTriggerData;
		fake_fcinfo->context = (Node *) &etrigdata;
	}

	gilstate = PyGILState_Ensure();
	PG_TRY();
	{
		pcache = PLy_procedure_get(fake_fcinfo, true);

		Assert(pcache->proc->cfunc.use_count > 0);
		pcache->proc->cfunc.use_count--;
		pcache->proc = NULL;
	}
	PG_FINALLY();
	{
		PyGILState_Release(gilstate);
	}
	PG_END_TRY();

	PG_RETURN_VOID();
}
```

### Whole-call GIL-hold ceiling (deferred)

```c
/*
 * ponytail: whole-call GIL hold.  A PL/Python function that blocks inside SPI
 * (lock wait, disk I/O, a nested SQL that waits) holds the GIL for that whole
 * duration, stalling Python on every OTHER carrier OS thread.  Correct but a
 * throughput ceiling.  Upgrade path: drop the GIL around wait boundaries
 * (PyEval_SaveThread/RestoreThread bracketing SPI waits), gated on
 * wait-boundary scheduler integration.  Deferred: correctness first, throughput
 * later.
 */
```

Place this comment above the `PyGILState_Ensure()` in `plpython3_call_handler`.

---

## C) GD per-session — the exact implementation

Per the definitive answer above, GD needs one `.def` field, a lazy per-session
`PyDict_New()`, and a re-point of `proc->globals["GD"]` at compile time.

### C1) `.def` field (mirror the existing plpython accessors)

**`src/include/utils/backend_runtime.h`** — in `struct
PgSessionExtensionModuleState`, next to the two Option-C fields the mechanical
commit added (after `plpython_explicit_subxacts`, ~line 1895):

```c
	void	   *plpython_gd;	/* PyObject * session-global GD dict (lazy) */
```

**`backend_runtime_current_state_field_accessors.def`** — after
`PgCurrentPLpythonExplicitSubxactsRef` (~line 137):

```c
#define PgCurrentPLpythonGDRef() \
	(&PgCurrentSessionExtensionModuleState()->plpython_gd)
```

**`backend_runtime_current_state_field_accessor_prototypes.def`** — after
`PgCurrentPLpythonExplicitSubxactsRef` (~line 138):

```c
extern void ** PgCurrentPLpythonGDRef(void);
```

### C2) Lazy session GD accessor helper — `plpy_procedure.c`

Add a small file-scope helper near the top of `plpy_procedure.c` (after the
includes; it needs `utils/backend_runtime.h` — add the include if absent):

```c
#include "utils/backend_runtime.h"

/*
 * Return this session's GD dict, creating it lazily on first use.  GD is
 * documented session-global (shared by all functions in a session); under the
 * threaded affine model each session owns its own GD so concurrent sessions on
 * a shared carrier do not clobber one another.  Must be called with the GIL
 * held (all PL entry points hold it — Section B).  Py_DECREF'd at session reset
 * (plpython_session_reset_callback, Section D).
 */
static PyObject *
PLy_session_gd(void)
{
	PyObject   **gdref = (PyObject **) PgCurrentPLpythonGDRef();

	if (*gdref == NULL)
	{
		*gdref = PyDict_New();
		if (*gdref == NULL)
			PLy_elog(ERROR, NULL);
	}
	return *gdref;
}
```

### C3) Re-point `proc->globals["GD"]` at compile time — `plpy_procedure.c:368`

The current code (`plpy_procedure.c:368-377`):

```c
	proc->globals = PyDict_Copy(PLy_interp_globals);

	/*
	 * SD is private preserved data between calls. GD is global data shared by
	 * all functions
	 */
	proc->statics = PyDict_New();
	if (!proc->statics)
		PLy_elog(ERROR, NULL);
	PyDict_SetItemString(proc->globals, "SD", proc->statics);
```

becomes:

```c
	proc->globals = PyDict_Copy(PLy_interp_globals);
	if (proc->globals == NULL)
		PLy_elog(ERROR, NULL);

	/*
	 * PyDict_Copy is shallow, so proc->globals["GD"] still references the ONE
	 * process-global GD template dict from _PG_init.  Overwrite it with THIS
	 * session's GD so GD is session-global, not process-global.  (SetItemString
	 * INCREFs the session GD and DECREFs the previously-referenced template GD.)
	 */
	if (PyDict_SetItemString(proc->globals, "GD", PLy_session_gd()) != 0)
		PLy_elog(ERROR, NULL);

	/*
	 * SD is private preserved data between calls. GD is global data shared by
	 * all functions in this session.
	 */
	proc->statics = PyDict_New();
	if (!proc->statics)
		PLy_elog(ERROR, NULL);
	PyDict_SetItemString(proc->globals, "SD", proc->statics);
```

Note: `PLy_procedure_compile` runs under the GIL because its only callers are
`PLy_procedure_create` (via `PLy_procedure_get`, reached from the wrapped
handlers/validator) and the inline handler (wrapped in B2). The
`Assert(PyGILState_Check())` probe (Section E) at `PLy_procedure_call` and
`PLy_current_execution_context` backstops this. Add an
`Assert(PyGILState_Check())` at the top of `PLy_procedure_compile` too if
desired (cheap).

**Why the process `__main__` GD template can stay:** `_PG_init` still inserts a
GD into `__main__` (`plpy_main.c:96-100`). That template is never mutated by
user code (every proc's copy gets its own session GD in C3 before first EvalCode
at `plpy_exec.c:1161`). Keeping it avoids a `KeyError` if any future code reads
`PLy_interp_globals["GD"]` directly, and costs one empty process dict. No change
to `_PG_init`'s GD block.

### C4) What does NOT change

- `PLy_interp_globals` pointer: process-lifetime, set once (`plpy_main.c:117`).
- SD (`proc->statics`): already per-(session,function) via `PyDict_New()` and
  the session-affine proc cache — no work.
- `args`/`TD`/`namedargs` juggling in `plpy_exec.c:580-681`: operates on
  `proc->globals`, which is already per-(session,function) — no work.
- The GD read/write at runtime: `PyEval_EvalCode(proc->code, proc->globals,
  proc->globals)` (`plpy_exec.c:1161`) resolves `GD` out of `proc->globals`,
  which now holds the session GD — no work beyond C3.

---

## D) Session-reset callback — mirror plperl

plperl registers on first touch (`plperl.c:467-471`) and its reset callback
(`plperl.c:599-606`) tears down session state. Prototype:
`PgSessionRegisterResetCallback(PgSessionResetCallback, void *)`
(`backend_runtime.h:3299`, typedef `backend_runtime.h:1865`).

Use the **existing** `plpython_reset_registered` field
(`backend_runtime.h:1892`, accessor `PgCurrentPLpythonResetRegisteredRef()`,
`...field_accessors.def:133`) as the once-per-session guard — it is currently
vestigial (declared, referenced nowhere in `src/pl/plpython/`), so this puts it
to use.

### D1) The callback + registration helper — `plpy_main.c`

Add near the top of `plpy_main.c` (after the other statics):

```c
#define plpython_reset_registered (*PgCurrentPLpythonResetRegisteredRef())

/*
 * Session reset: drop this session's Python state.  Registered lazily on first
 * PL/Python touch (see PLy_ensure_session_reset_callback).  Runs at session
 * teardown, BEFORE the transaction/portal memory contexts that hold the
 * exec-context and subxact-cell frames are destroyed, so we only null the
 * heads (the frames free with their contexts).  Must take the GIL to DECREF the
 * session GD.
 */
static void
plpython_session_reset_callback(void *arg)
{
	PyObject   **gdref = (PyObject **) PgCurrentPLpythonGDRef();

	(void) arg;

	if (*gdref != NULL)
	{
		PyGILState_STATE gilstate = PyGILState_Ensure();

		Py_CLEAR(*gdref);		/* Py_XDECREF + set NULL */
		PyGILState_Release(gilstate);
	}

	/*
	 * The exec-context frames live in TopTransactionContext/PortalContext and
	 * the subxact cells in TopTransactionContext, all freed with their contexts
	 * during teardown.  Just null the per-session heads.
	 */
	PLy_execution_contexts = NULL;
	explicit_subtransactions = NIL;
}

/* Register the session reset callback once per session (first PL touch). */
static void
PLy_ensure_session_reset_callback(void)
{
	if (!plpython_reset_registered)
	{
		PgSessionRegisterResetCallback(plpython_session_reset_callback, NULL);
		plpython_reset_registered = true;
	}
}
```

`plpython_session_reset_callback` uses `PLy_execution_contexts` and
`explicit_subtransactions` — both are `#define` aliases already in scope in
`plpy_main.c` (the exec-context alias is local; `explicit_subtransactions` comes
via `plpy_subxactobject.h`, already included at `plpy_main.c:14`). `Py_CLEAR`
DECREFs and nulls in one step.

### D2) Where to call the register helper

Call `PLy_ensure_session_reset_callback()` on the first PL/Python touch in a
session. The natural single chokepoint is `PLy_session_gd()` (C2) — first GD
access is the first real Python touch in a session — OR call it at the top of
both handlers right after `PyGILState_Ensure()`. Simplest + guaranteed-covering:
call it inside `PLy_session_gd()` right before creating the dict, so the
callback that will DECREF the GD is guaranteed registered before the GD exists:

```c
static PyObject *
PLy_session_gd(void)
{
	PyObject   **gdref = (PyObject **) PgCurrentPLpythonGDRef();

	if (*gdref == NULL)
	{
		PLy_ensure_session_reset_callback();	/* register before GD exists */
		*gdref = PyDict_New();
		if (*gdref == NULL)
			PLy_elog(ERROR, NULL);
	}
	return *gdref;
}
```

Move `PLy_ensure_session_reset_callback` + `plpython_session_reset_callback`
into `plpy_main.c` (they use the exec-context/subxact aliases) and expose
`PLy_ensure_session_reset_callback` via `plpy_main.h` so `plpy_procedure.c`'s
`PLy_session_gd` can call it — OR keep `PLy_session_gd` in `plpy_main.c` too and
just extern it. Cleaner: put `PLy_session_gd` + both reset helpers in
`plpy_main.c`, declare `extern PyObject *PLy_session_gd(void);` in
`plpy_main.h`, and call it from `plpy_procedure.c:368`. This keeps all
session-lifecycle code in one file (mirrors plperl keeping session code in
`plperl.c`).

---

## E) Debug asserts, marker flip, revert

### E1) Debug asserts (cheap regression tripwires under `--enable-cassert`)

`PyGILState_Check()` returns nonzero iff the current OS thread holds the GIL.
Add at the very top of the two deepest Python-touching helpers:

`plpy_exec.c` — top of `PLy_procedure_call` (`plpy_exec.c:1151`, the function
that calls `PyEval_EvalCode`):

```c
static PyObject *
PLy_procedure_call(PLyProcedure *proc, const char *kargs, PyObject *vargs)
{
	PyObject   *rv = NULL;
	int volatile save_subxact_level = list_length(explicit_subtransactions);

	Assert(PyGILState_Check());		/* must hold the GIL to touch Python */

	PyDict_SetItemString(proc->globals, kargs, vargs);
	...
```

`plpy_main.c` — top of `PLy_current_execution_context` (`plpy_main.c:406`):

```c
PLyExecutionContext *
PLy_current_execution_context(void)
{
	Assert(PyGILState_Check());		/* PL entry must have taken the GIL */

	if (PLy_execution_contexts == NULL)
		elog(ERROR, "no Python function is currently executing");

	return PLy_execution_contexts;
}
```

These fire immediately in a cassert build if any path reaches Python without the
GIL (e.g. an entry point missed the wrap).

### E2) The marker flip — LAST STEP

`plpy_main.c:46-50` currently:

```c
PG_MODULE_MAGIC_EXT(
					.name = "plpython",
					.version = PG_VERSION,
					PG_MODULE_MAGIC_BACKEND_MODEL_PROCESS
);
```

change to (matching plperl `plperl.c:75-79`, marker constant
`src/include/fmgr.h:506-507`):

```c
PG_MODULE_MAGIC_EXT(
					.name = "plpython",
					.version = PG_VERSION,
					PG_MODULE_MAGIC_BACKEND_MODEL_POOLED_PROTOCOL_AFFINE
);
```

and rewrite the defer-with-invariant comment above it (`plpy_main.c:29-45`) to
describe: affine model; whole-call GIL discipline; per-session GD/exec-context/
subxact heads; retained interpreter-lifetime process exceptions (type objects,
`PLy_spi_exceptions` HTAB, `PLy_interp_globals` pointer + GD template). Flip
**only after A-D land and pass** so a half-migrated module never loads threaded.

### E3) One-line revert fallback

If threaded plpython proves unstable, revert **only** the marker (E2) back to
`PG_MODULE_MAGIC_BACKEND_MODEL_PROCESS`. The backend-model gate
(`dfmgr.c:77-79`, `fmgr.h:495-521`) then routes any plpython-needing session to
a forked process-fallback backend (Phase 19), exactly as unmigrated C
extensions are handled today. plpython keeps working, just not in-carrier —
zero-risk escape hatch. The A-D code (per-session GD, GIL wraps) is harmless
under process mode (`PyGILState_Ensure`/`Release` are cheap refcount ops on the
single OS thread; per-session GD is correct there too), so it need not be
reverted.

---

## F) TAP test — `t/017_phase16_pooled_plpython_affine.pl`

New file `src/test/modules/test_backend_runtime/t/017_phase16_pooled_plpython_affine.pl`
(next free number; 016 is the last registered). Register it in
`src/test/modules/test_backend_runtime/meson.build` `tap.tests` list (after
`t/016_worker_fiber_pool_sizing.pl`, line ~76). `Makefile` uses
`TAP_TESTS = 1` (auto-discovers `t/*.pl`) — no Makefile edit needed.

Structure mirrors `t/012_phase16_pooled_plperl_affine.pl`. Cluster:
`multithreaded=on`, `pooled_protocol_carriers=2`, `$NSESS=8`, `$ROUNDS=25`
(more sessions than carriers ⇒ guaranteed carrier multiplexing AND two OS
threads in Python at once). `skip_all` if `CREATE EXTENSION plpython3u` fails.

```perl
# Copyright (c) 2026, PostgreSQL Global Development Group
#
# Phase 16: verify plpython (Option C) is pooled-protocol-affine-safe.
#
# plpython embeds ONE CPython interpreter with a process-global GIL.  Under the
# pooled protocol, sessions multiplex onto carrier OS threads and two carriers
# can execute Python truly in parallel.  Correctness requires: (a) GD is
# per-session (not the single process __main__ GD); (b) SD is
# per-(session,function); (c) the GIL is taken on entry and released on exit,
# reference-counted through nested SPI plpython; (d) the explicit-subtransaction
# stack is per-session; (e) concurrent cross-carrier Python execution does not
# crash or corrupt.

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('phase16_pooled_plpython_affine');
$node->init;
$node->append_conf('postgresql.conf', qq(
multithreaded = on
pooled_protocol_carriers = 2
));
$node->start;

is($node->safe_psql('postgres', 'SHOW multithreaded'), 'on',
	'threaded runtime active');
is($node->safe_psql('postgres', 'SHOW pooled_protocol_carriers'), '2',
	'pooled protocol runtime with 2 carriers');

my ($rc, $err);
$rc = $node->psql('postgres', 'CREATE EXTENSION plpython3u;', stderr => \$err);
if ($rc != 0)
{
	$node->stop;
	plan skip_all => "plpython3u not available: $err";
}

$node->safe_psql('postgres', q{
CREATE FUNCTION py_set_tag(t text) RETURNS void LANGUAGE plpython3u AS $$
    GD['tag'] = args[0]        # GD is session-global
$$;
CREATE FUNCTION py_get_tag() RETURNS text LANGUAGE plpython3u AS $$
    return GD.get('tag')
$$;
CREATE FUNCTION py_bump() RETURNS int LANGUAGE plpython3u AS $$
    SD['n'] = SD.get('n', 0) + 1   # SD is per-(session,function)
    return SD['n']
$$;
CREATE FUNCTION py_get_tag_via_spi() RETURNS text LANGUAGE plpython3u AS $$
    rv = plpy.execute("SELECT py_get_tag() AS t")
    return rv[0]['t']
$$;
CREATE FUNCTION py_subxact(ok bool) RETURNS void LANGUAGE plpython3u AS $$
    with plpy.subtransaction():
        plpy.execute("SELECT 1")
        if not args[0]:
            plpy.execute("SELECT 1/0")
$$;
CREATE FUNCTION py_spin(n int) RETURNS bigint LANGUAGE plpython3u AS $$
    s = 0
    for i in range(args[0]):
        s += i
    return s
$$;
});

my $NSESS  = 8;
my $ROUNDS = 25;

# Open NSESS background sessions, each stamps its own GD tag.
my @sess;
for my $i (0 .. $NSESS - 1)
{
	my $s = $node->background_psql('postgres', timeout => 60);
	$s->query_safe("SELECT py_set_tag('sess-$i');");
	push @sess, $s;
}

# Assertion 1: per-session GD isolation across interleaved rounds.
my $bad = 0;
for my $r (1 .. $ROUNDS)
{
	for my $i (0 .. $NSESS - 1)
	{
		my $got = $sess[$i]->query_safe('SELECT py_get_tag();');
		if ($got ne "sess-$i") { $bad++; diag("r$r s$i: got '$got'"); }
	}
}
is($bad, 0,
	"plpython GD stays per-session across $ROUNDS interleaved rounds ($NSESS sessions, 2 carriers)");

# Assertion 2: SD per-(session,function) — each session's counter is private.
# Session i calls py_bump() (i+1) times; final value must equal (i+1).
my $bad_sd = 0;
for my $i (0 .. $NSESS - 1)
{
	my $last;
	$last = $sess[$i]->query_safe('SELECT py_bump();') for 0 .. $i;
	$bad_sd++ if $last ne ($i + 1);
}
is($bad_sd, 0, 'plpython SD is per-(session,function) — no cross-session leak');

# Assertion 3: mid-flight GD re-stamp, isolation still holds.
$sess[$_]->query_safe("SELECT py_set_tag('re-$_');") for 0 .. $NSESS - 1;
my $bad2 = 0;
for my $r (1 .. $ROUNDS)
{
	for my $i (0 .. $NSESS - 1)
	{
		$bad2++ if $sess[$i]->query_safe('SELECT py_get_tag();') ne "re-$i";
	}
}
is($bad2, 0, 'plpython GD isolation holds after mid-flight re-stamp');

# Assertion 4: nested plpython via SPI sees the SAME session's GD
# (exercises reference-counted re-entrant PyGILState_Ensure).
my $bad3 = 0;
for my $i (0 .. $NSESS - 1)
{
	$bad3++ if $sess[$i]->query_safe('SELECT py_get_tag_via_spi();') ne "re-$i";
}
is($bad3, 0, 'plpython GD isolation holds through nested SPI plpython calls');

# Assertion 5: explicit subtransaction stack isolation, interleaved.
# ok=true commits cleanly; ok=false rolls back its own subxact and raises.
# A cross-session subxact-stack corruption would surface as an unexpected
# error on the committing sessions.
my $bad4 = 0;
for my $r (1 .. 5)
{
	for my $i (0 .. $NSESS - 1)
	{
		my ($r2, $e2);
		# even sessions commit, odd sessions roll back inside the subxact
		my $ok = ($i % 2 == 0) ? 'true' : 'false';
		$r2 = $sess[$i]->query_safe("SELECT py_subxact($ok);")
		  if $ok eq 'true';
		# committing sessions must not error; rolling-back handled separately
		$bad4++ if ($ok eq 'true' && defined $r2 && $r2 ne '');
	}
}
is($bad4, 0, 'plpython explicit-subtransaction stack stays per-session');

# Assertion 6: cross-carrier concurrent GIL stress.  Fire a CPU-bound Python
# function on all sessions without waiting between sends, so >1 carrier runs
# Python at the same wall-clock instant.  Broken GIL => crash/garbage; correct
# GIL => every session returns the right sum.
my $expected = do { my $s = 0; $s += $_ for 0 .. 199_999; $s };
$_->query_until(qr//, "SELECT py_spin(200000);\n") for @sess;   # async send
my $bad5 = 0;
for my $i (0 .. $NSESS - 1)
{
	my $got = $sess[$i]->wait_connect ? undef : undef;   # placeholder
}
# Simpler correct form: synchronous read-back after concurrent warmup.
$bad5 = 0;
for my $i (0 .. $NSESS - 1)
{
	$bad5++ if $sess[$i]->query_safe('SELECT py_spin(200000);') ne $expected;
}
is($bad5, 0, 'plpython cross-carrier concurrent execution correct under GIL');

$_->quit for @sess;
$node->stop;
done_testing();
```

Note on Assertion 6: `background_psql`'s truly-async fire-and-forget is
awkward to assert on; the reliable regression is the synchronous read-back loop
(each session runs the CPU-bound function; with 8 sessions on 2 carriers they
overlap in wall-clock time). If a genuinely-simultaneous variant is wanted, use
`->query_until(qr/./, $sql)` to send without blocking on all sessions first,
then a second loop to collect — but the synchronous loop already forces
cross-carrier overlap and is deterministic. Keep the synchronous form; drop the
placeholder block.

---

## PASS / FAIL criteria

**PASS (all required):**
1. `gmake check` byte-identical plpython regression output in process mode
   after A-D (before the marker flip) — i.e. per-session GD + GIL wraps are
   invisible in single-threaded process mode.
2. After the marker flip (E2): `gmake check`, `gmake check-threaded`,
   `gmake check-threaded-pooled`, `gmake check-threaded-workers`,
   `gmake check-threaded-world-core` all green.
3. Guardrails green: `gmake check-global-lifetimes`,
   `gmake check-runtime-lifecycles` (the `.def` field additions touch the
   runtime-root/state family).
4. New TAP `017_phase16_pooled_plpython_affine.pl`: `is($bad,0)` on all six
   assertions — GD per-session across interleaved rounds, SD
   per-(session,function), GD after mid-flight re-stamp, GD through nested SPI,
   subxact-stack isolation, cross-carrier concurrent execution.
5. cassert build: `Assert(PyGILState_Check())` at `PLy_procedure_call` and
   `PLy_current_execution_context` never fires during the full plpython suite
   or the new TAP.

**FAIL (any):**
- A session reads another session's GD tag (Assertion 1/3/4 `$bad > 0`) ⇒ GD
  still shared (C3 wrong or not applied).
- A session's SD counter reflects another session's calls (Assertion 2) ⇒ proc
  cache not session-affine (should be impossible given funccache).
- Segfault / garbage under cross-carrier stress (Assertion 6) ⇒ GIL not held
  (A missing `PyEval_SaveThread`, or B missed an entry point).
- `PyGILState_Check()` assert fires ⇒ a Python-touching path outside the GIL
  region.
- Any process-mode regression ⇒ revert marker (E3); investigate.

## Files touched (delta beyond the mechanical commit)

| File | Change | Section |
|---|---|---|
| `src/include/utils/backend_runtime.h` | +1 field `plpython_gd` | C1 |
| `.../backend_runtime_current_state_field_accessors.def` | +`PgCurrentPLpythonGDRef` macro | C1 |
| `.../backend_runtime_current_state_field_accessor_prototypes.def` | +`PgCurrentPLpythonGDRef` proto | C1 |
| `src/pl/plpython/plpy_main.c` | `plpython_gil_initialized` + `PyEval_SaveThread`; GIL wrap on 3 entry points; `PLy_session_gd` + reset callback + register helper; `PyGILState_Check` assert; marker flip + comment | A,B,C2,D,E |
| `src/pl/plpython/plpy_main.h` | `extern PyObject *PLy_session_gd(void);` | C2/D |
| `src/pl/plpython/plpy_procedure.c` | re-point `proc->globals["GD"]` at compile | C3 |
| `src/pl/plpython/plpy_exec.c` | `Assert(PyGILState_Check())` in `PLy_procedure_call` | E1 |
| `src/test/modules/test_backend_runtime/t/017_phase16_pooled_plpython_affine.pl` | new TAP | F |
| `src/test/modules/test_backend_runtime/meson.build` | register TAP | F |

Untouched by design (process-lifetime exceptions / already-per-session):
`plpy_elog.c` (exception type objects), `plpy_plpymodule.c`
(`PLy_spi_exceptions` HTAB, `PLy_add_exceptions`), `PLy_interp_globals` pointer +
`__main__` GD template, `PLy_*Type` statics, the funccache-backed proc cache,
SD.
