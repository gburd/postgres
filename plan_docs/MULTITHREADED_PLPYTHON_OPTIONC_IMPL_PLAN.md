# PL/Python "Option C" Threaded-Affine Implementation Plan

Status: PLAN ONLY (no source changed). Target: move plpython from
`PG_BACKEND_MODEL_PROCESS` to `PG_BACKEND_MODEL_POOLED_PROTOCOL_AFFINE`,
matching the plperl / pltcl affine pattern already validated in this fork.

Design decision (given): Option C -- keep the single embedded CPython
interpreter, move plpython's process-global mutable state to per-session
storage via the `backend_runtime_current` `.def` accessors, and make the GIL
handling correct across carriers. The GIL stays process-global (no fiber
parallelism gain; correct interleave because fibers are cooperative *within* a
carrier, and the GIL serializes *across* carriers).

All file:line citations are against the tree as read for this plan.

---

## 0. Ground truth discovered (why this plan differs from the naive brief)

Three findings change the shape of the work versus the task framing:

1. **plpython's procedure cache is already session-affine.** In this fork the
   PLyProcedure cache goes through the shared `funccache.c` machinery
   (`src/pl/plpython/plpy_procedure.c:113` `cached_function_compile`), and
   `funccache.c` already reads its hash and memory context from per-session
   accessors: `cfunc_hashtable` == `*PgCurrentCachedFunctionHashRef()`
   (`src/backend/utils/cache/funccache.c:41`) and
   `PgCurrentFunctionManagerMemoryContext()` (`funccache.c:74,199,645`). So the
   task's "the procedure cache" item is *already done for free*. The
   `plpython_procedure_cache` / `plpython_reset_registered` `.def` fields
   (`backend_runtime_current_state_field_accessors.def:129,133`;
   `backend_runtime.h:1890,1892`) are **vestigial scaffolding from prior partial
   work -- they are declared but referenced nowhere in `src/pl/plpython/`**
   (confirmed by grep). They can stay (harmless) or be removed; this plan leaves
   them.

2. **Only `PgCurrentPLpythonMemoryContextRef()` is actually wired today**
   (`backend_runtime.h:1891`), used at `plpy_cursorobject.c:115,220`,
   `plpy_spi.c:64`, `plpy_main.c:297`. That is the per-session owned memory
   context root for SPI plans / cursors / inline blocks. Good -- keep it.

3. **The real work is the GIL, not a PyThreadState re-stamp.** plpython today
   calls only `Py_Initialize()` (`plpy_main.c:85`) and uses **no**
   `PyThreadState`, `PyGILState`, `PyEval_SaveThread`, or `PyEval_InitThreads`
   anywhere (confirmed by grep across `src/pl/plpython/`). Under process mode
   that is fine: one OS thread owns the interpreter forever. Under
   `multithreaded=on` a **carrier is a real OS thread** and there are multiple
   carriers (`pooled_protocol_carriers`, see
   `backend_runtime.c:583-585,806,1045`: "many backend fibers time-share ONE OS
   thread" but there are N carrier OS threads). So two sessions pinned to two
   *different* carriers can enter CPython **truly in parallel on two OS
   threads**. Without correct GIL acquisition that is an immediate data race /
   crash. This is the crux of Option C and Section 6 treats it in full.

Net: the "move globals per-session" part is small and mechanical (5 globals).
The load-bearing part is GIL correctness across carriers.

---

## 1. Process-global mutable inventory (file:line + classification)

Legend:
- **PER-SESSION** = survives across calls within one session, must be isolated
  between sessions -> move to `PgSessionExtensionModuleState`.
- **PER-EXECUTION** = valid only during one call, lives on a stack -> per-session
  storage of the stack head is enough (the frames themselves are already in
  per-call memory contexts).
- **PROCESS-SAFE** = set once during `_PG_init` / first-touch, then read-only ->
  leave as process-global (a process-lifetime exception, like plperl's opmask).

### 1a. True file-scope mutable globals (the move list)

| Symbol | Decl | Extern in | Class | Disposition |
|---|---|---|---|---|
| `PLy_interp_globals` (main `__main__` dict, holds `GD`) | `plpy_main.c:65` | `plpy_main.h:11` | **PROCESS-SAFE** for the *pointer*, but see note | Set once in `_PG_init` (`plpy_main.c:113`). The pointer is set-once; **but the dict it points at is the shared GD carrier and is mutated per session** -- see Section 6d. Keep the pointer process-global; make GD per-session at the dict level. |
| `PLy_execution_contexts` (exec-context stack head) | `plpy_main.c:68` (`static`) | (file-local) | **PER-SESSION** (stack head) | Move head pointer to session state. Frames themselves already live in `TopTransactionContext`/`PortalContext` (`plpy_main.c:432`), which are per-session. Push/pop at `plpy_main.c:428,446`. |
| `explicit_subtransactions` (open explicit subxact stack) | `plpy_subxactobject.c:15` | `plpy_subxactobject.h:13` | **PER-SESSION** (stack head) | Move to session state. Read/written at `plpy_subxactobject.c:126,170,188,189` and `plpy_main.c:118` (reset in `_PG_init`). Cells are `TopTransactionContext`-allocated (`plpy_subxactobject.c:124` comment), i.e. per-session-per-xact. |
| `PLy_spi_exceptions` (sqlstate->exception-obj HTAB) | `plpy_plpymodule.c:20` | `plpy_plpymodule.h:12` | **PROCESS-SAFE** | Built once in `PLy_add_exceptions` (`plpy_plpymodule.c:163`) alongside the interpreter's exception type objects; read-only afterward (`plpy_plpymodule.c:236`). It holds `PyObject*` that belong to the single interpreter -> must stay process-global (same lifetime as the interpreter). Leave. |
| `PLy_exc_error`, `PLy_exc_fatal`, `PLy_exc_spi_error` (exception type objects) | `plpy_elog.c:15-17` | `plpy_elog.h:11-13` | **PROCESS-SAFE** | Created once in `PLy_add_exceptions` (`plpy_plpymodule.c:150-154`); they are interpreter-global type objects. Read at `plpy_elog.c:80-89`, `plpy_plpymodule.c:521`. Leave process-global (interpreter lifetime). |

### 1b. `PLy_curr_procedure`: does not exist as a global here

The task lists `PLy_curr_procedure`. In this tree there is **no such global**;
the "currently executing procedure" is `PLy_current_execution_context()->curr_proc`
(defined `plpy_main.c:406`, field set at `plpy_main.c:236` / `plpy_main.c:340`,
read across `plpy_typeio.c:799,980`, `plpy_elog.c`, etc.). It rides on the
exec-context stack, so once `PLy_execution_contexts` is per-session (1a),
`curr_proc` is automatically per-session. **No separate work.**

### 1c. Set-once-read-only statics (leave as PROCESS-SAFE exceptions)

These are file-scope `static` and initialized once during module/interpreter
init, then read-only. They are interpreter-global type objects and **must not**
be per-session (there is one interpreter):

- `PLy_CursorType` `plpy_cursorobject.c:67`
- `PLy_PlanType` `plpy_planobject.c:53`
- `PLy_ResultType` `plpy_resultobject.c:78`
- `PLy_SubtransactionType` `plpy_subxactobject.c:54`
- `PLy_exc_module` / method tables `plpy_plpymodule.c:103,114`

Treat exactly like plperl's `plperl_opmask` (`plperl.c:264`) -- a documented
process-lifetime exception.

### 1d. Already-per-session (no work; audit note only)

- Procedure cache -> `funccache.c` per-session (`funccache.c:41`), see Section 0.1.
- `PgCurrentPLpythonMemoryContextRef()` -> already wired (`backend_runtime.h:1891`).

### Summary of what actually moves

Exactly **two** stack heads become per-session:
`PLy_execution_contexts` and `explicit_subtransactions`. Everything else is
either already-per-session (procedure cache, session memory context) or a
legitimate interpreter-lifetime process global (type objects, exception HTAB,
the `PLy_interp_globals` pointer). The GD dict gets per-session treatment at
the dict level (Section 6d), not via a `.def` field.

---

## 2. PyThreadState re-stamp: is a per-session PyThreadState the right unit?

**Conclusion: NO per-session PyThreadState. The right unit is a per-carrier
(per-OS-thread) `PyThreadState`, acquired/released around each PL entry via the
GIL, on the single main interpreter.** Reasoning:

- There is one embedded interpreter (Option C keeps it). PyThreadState is a
  *(interpreter, OS-thread)* pairing, not a session pairing. Sessions are
  fibers; many fibers share one carrier OS thread and never run Python
  concurrently *within* that carrier (cooperative). So a per-session
  PyThreadState would be wrong-grained and pointless: two fibers on the same
  carrier can safely share one thread state because they never interleave
  mid-C-call.

- Across carriers (different OS threads) the danger is real, and the CPython
  primitive designed for exactly "arbitrary C thread wants to call into the one
  interpreter" is `PyGILState_Ensure()` / `PyGILState_Release()`. It maintains a
  per-OS-thread auto thread-state (keyed by TLS) and takes the GIL. That is the
  correct and minimal tool.

So there is **no `activate_plpython_interpreter()` analogous to plperl's
`activate_interpreter()` re-stamp**. plperl re-stamps because Perl keeps a
*thread-global* "current interpreter" (`my_perl` / `PERL_GET_CONTEXT`) that a
sibling on the same carrier can move (`plperl.c:743-767`, TAP-012 rationale).
CPython's equivalent (the current thread state) is *managed by
PyGILState_Ensure per OS thread*, not something a sibling fiber silently moves,
so the plperl-style re-stamp does not map. The plperl analogue in plpython is
"take the GIL on entry, drop it on exit," which is Section 6.

**Entry points that must be wrapped** (mirror plperl's `activate_interpreter`
call sites, but with GIL ensure/release):

- `plpython3_call_handler` -- `plpy_main.c:199`
- `plpython3_inline_handler` -- `plpy_main.c:268`
- `plpython3_validator` -- `plpy_main.c:123` (it calls `PLy_procedure_get` ->
  compiles Python -> touches the interpreter)

The wrap must be the **outermost** thing in each entry (before
`SPI_connect_ext` / `PLy_push_execution_context`), and released on every exit
path including the `PG_CATCH`. See Section 6b for placement detail and re-entrancy.

---

## 3. Per-session state: exact `.def` additions + accessor names

Follow the plperl mechanism exactly. Three coordinated files, plus the struct.

### 3a. Struct field additions -- `src/include/utils/backend_runtime.h`

In `struct PgSessionExtensionModuleState` (definition at
`backend_runtime.h:1887`), after the existing plpython fields
(`plpython_procedure_cache` / `plpython_memory_context` /
`plpython_reset_registered`, lines 1890-1892) add:

```c
	/* PL/Python per-session stack heads (Option C affine) */
	void	   *plpython_execution_contexts;	/* PLyExecutionContext * head */
	List	   *plpython_explicit_subxacts;		/* explicit subxact stack */
	bool		plpython_gil_initialized;		/* PyEval_SaveThread done? (process-wide guard, see 6a) */
```

Note: `plpython_gil_initialized` is logically process-wide (GIL init happens
once for the process, Section 6a), but storing it in session state is the
cheapest place that already has an accessor idiom; guard its use so only the
first session that touches Python performs `PyEval_SaveThread`. Alternatively
keep it as a genuine `static bool` in `plpy_main.c` -- either is fine since it
is set-once. This plan keeps it a `static bool` in `plpy_main.c` (simpler; no
`.def` churn) and **drops** the `.def` field for it. So only two new fields:
`plpython_execution_contexts`, `plpython_explicit_subxacts`.

Final struct addition (2 fields):

```c
	void	   *plpython_execution_contexts;
	List	   *plpython_explicit_subxacts;
```

### 3b. Accessor macros -- `backend_runtime_current_state_field_accessors.def`

Alongside the existing plpython block (`.def:129-133`) add:

```c
#define PgCurrentPLpythonExecutionContextsRef() \
	(&PgCurrentSessionExtensionModuleState()->plpython_execution_contexts)
#define PgCurrentPLpythonExplicitSubxactsRef() \
	(&PgCurrentSessionExtensionModuleState()->plpython_explicit_subxacts)
```

### 3c. Fallback prototypes -- `backend_runtime_current_state_field_accessor_prototypes.def`

Alongside the existing plpython protos (`.def:133-135`) add (keep the file's
alphabetical grouping):

```c
extern void ** PgCurrentPLpythonExecutionContextsRef(void);
extern List ** PgCurrentPLpythonExplicitSubxactsRef(void);
```

(These match the pattern of `PgCurrentPLperlCurrentCallDataRef` = `void **` and
of a `List *` field. Confirm the generator's type mapping for `List *` fields;
if the generator emits its own prototype, this fallback is only used when
generation is off -- same as every other entry here.)

### 3d. Any forward-decl `.def`?

`backend_runtime_current_state_forward_decls.def` carries struct forward
declarations, not per-field entries; `List` and `void` need none. No change.

---

## 4. `#define` aliasing (over accessors) -- the rewrites

Mirror plperl's `#define plperl_active_interp (*(...) PgCurrentPLperl...Ref())`
idiom (`plperl.c:246-269`) and auto_explain's `#define X (session_state->X)`.

### 4a. `plpy_main.c`

Replace the `static` global (`plpy_main.c:68`)

```c
static PLyExecutionContext *PLy_execution_contexts = NULL;
```

with an accessor alias:

```c
#include "utils/backend_runtime.h"   /* already included, line 20 */
#define PLy_execution_contexts \
	(*(PLyExecutionContext **) PgCurrentPLpythonExecutionContextsRef())
```

All existing uses (`plpy_main.c:408,428,431,432,444,446`) then compile
unchanged. Delete the `_PG_init` reset `PLy_execution_contexts = NULL;`
(`plpy_main.c:119`) -- per-session state is zero-initialized by the session
state allocator (verify: `PgSessionExtensionModuleState` is zeroed on session
setup, same as plperl relies on for `plperl_inited`).

### 4b. `plpy_subxactobject.c`

Replace the exported global (`plpy_subxactobject.c:15`)

```c
List	   *explicit_subtransactions = NIL;
```

with an accessor alias:

```c
#include "utils/backend_runtime.h"
#define explicit_subtransactions (*PgCurrentPLpythonExplicitSubxactsRef())
```

Uses at `plpy_subxactobject.c:126,170,188,189` compile unchanged. Remove the
`extern List *explicit_subtransactions;` from `plpy_subxactobject.h:13` and the
`explicit_subtransactions = NIL;` reset in `plpy_main.c:118` (session-zeroed).

Any other TU referencing the extern? Only `plpy_main.c:118`. So after removing
the header extern, only `plpy_subxactobject.c` sees the macro. Good.

### 4c. What does NOT get aliased

`PLy_interp_globals`, `PLy_spi_exceptions`, `PLy_exc_*` stay as-is (Section 1a:
interpreter-lifetime process globals). GD isolation is handled at the dict level
(Section 6d), not by aliasing the pointer.

---

## 5. Module magic change

`src/pl/plpython/plpy_main.c:46-50` currently:

```c
PG_MODULE_MAGIC_EXT(
					.name = "plpython",
					.version = PG_VERSION,
					PG_MODULE_MAGIC_BACKEND_MODEL_PROCESS
);
```

Change the marker to affine (matching plperl `plperl.c:75-79`, pltcl
`pltcl.c:59-62`):

```c
					PG_MODULE_MAGIC_BACKEND_MODEL_POOLED_PROTOCOL_AFFINE
```

Marker constant defined at `src/include/fmgr.h:506-507`. **This is the LAST
step** -- flipping it is what lets plpython load under `multithreaded=on`; do it
only after Sections 1-4 and 6 land, so a half-migrated module never loads
threaded.

Also update the large defer-with-invariant comment above the macro
(`plpy_main.c:29-45`) to describe the new affine model, the GIL discipline, and
the retained interpreter-lifetime process exceptions (type objects, exception
HTAB, GD-dict-per-session).

---

## 6. GIL / PyThreadState risk analysis + guard/fallback (the crux)

### 6a. Interpreter + GIL init (one-time, process-wide)

Today `_PG_init` (`plpy_main.c:78`) calls `Py_Initialize()` and leaves the GIL
**held** by whichever OS thread ran init, with a single thread state. That is
only safe if exactly one OS thread ever touches the interpreter -- true in
process mode, false with multiple carriers.

Required change in the init path (once per process, guarded by the `static bool
plpython_gil_initialized`):

- After `Py_Initialize()` (Py 3.7+ auto-inits threads; `PyEval_InitThreads` is a
  no-op/deprecated and not needed), the init thread holds the GIL. Release it so
  other carriers can acquire it: capture and drop with `PyEval_SaveThread()`
  once initialization + `PLy_add_exceptions` + GD setup are complete. Store
  nothing session-scoped; just flip `plpython_gil_initialized = true`.
- From then on, **no code assumes it holds the GIL** except between a
  `PyGILState_Ensure()` and its matching `PyGILState_Release()`.

Guard: `plpython_gil_initialized` ensures `PyEval_SaveThread()` runs exactly
once (first session to touch Python). `_PG_init` itself may run per-DSO-load;
Python init is already idempotent-guarded upstream via `Py_IsInitialized()` --
keep/verify that guard.

### 6b. Per-entry GIL acquire/release (mirrors plperl's activate call sites)

At each entry point (Section 2 list) wrap the body:

```c
	PyGILState_STATE gilstate = PyGILState_Ensure();
	PG_TRY();
	{
		... existing handler body ...
	}
	PG_FINALLY();       /* or duplicate in PG_CATCH + normal-exit */
	{
		PyGILState_Release(gilstate);
	}
	PG_END_TRY();
```

Placement notes:
- `PyGILState_Ensure` must be the **outermost** acquire, before
  `SPI_connect_ext` and `PLy_push_execution_context`, and `Release` the
  **innermost-last** on every path. Structure it so the existing exec-context
  push/pop and SPI connect/finish nest *inside* the GIL region.
- `PyGILState_Ensure/Release` are **reference-counted and re-entrant per OS
  thread**: nested PL/Python (plpython calling SQL that calls plpython via SPI)
  simply bumps the count. So the nested-SPI case (TAP-012's third assertion,
  ported below) is handled automatically -- inner `Ensure` doesn't re-take,
  inner `Release` doesn't drop, the outermost pair owns the GIL. This is the
  plpython analogue of plperl's nested `activate_interpreter(oldinterp)`
  save/restore (`plperl.c:2822-2824`).
- SPI calls out of plpython run other backend code (executor, maybe plperl).
  That code does not expect to hold the Python GIL, and holding it across a
  blocking wait would serialize *all* carriers' Python. **Decision for v1:**
  hold the GIL for the whole PL/Python call (simplest, correct). Ceiling: a
  plpython function that blocks in SPI (e.g. lock wait) stalls Python on every
  other carrier for that duration. Mark with a `ponytail:`-style note and defer
  the finer "drop GIL around SPI/wait boundaries" optimization to a later phase
  -- it is a throughput optimization, not a correctness requirement, and it
  interacts with the wait-boundary scheduler.

  ```
  // ponytail: whole-call GIL hold; drop-around-SPI-wait is a throughput
  // optimization for later, gated on wait-boundary scheduler integration.
  ```

### 6c. Sub-transaction stack integrity across interleave

`explicit_subtransactions` becomes per-session (Section 4b), so two sessions
cannot corrupt each other's subxact stack. Within a session, fibers are
cooperative and the GIL is held for the whole call, so no mid-stack interleave
occurs. The `PLySubtransactionData` cells are `TopTransactionContext`-allocated
(`plpy_subxactobject.c:124`), which is per-session -- consistent. No extra work
beyond the per-session move; the existing enter/exit balance logic
(`plpy_subxactobject.c:93-200`) is unchanged.

### 6d. SD / GD dictionary semantics

- **SD** (`proc->statics`, `plpy_procedure.c:374-377`) is per-PLyProcedure. The
  PLyProcedure cache is session-affine (funccache, Section 0.1), so SD is
  already **per-function-per-session**. Correct by construction. No work.
- **GD** is the shared dict living in `PLy_interp_globals` (`__main__` dict),
  set up once in `_PG_init` (`plpy_main.c:96-100,113`). Each procedure's globals
  are `PyDict_Copy(PLy_interp_globals)` (`plpy_procedure.c:368`), and GD is
  re-inserted by reference (`SD` is fresh per proc; `GD` rides in the copied
  main dict). **Problem:** a single process-global GD dict is shared across ALL
  sessions -> GD is documented as *session*-global, not process-global. Under
  Option C this must become per-session.
  - **Fix:** give each session its own GD dict. On first PL/Python touch in a
    session, create a fresh `GD = PyDict_New()` and stash it in session state
    (a third `.def` field `plpython_gd` of type `void *`/`PyObject *`, or store
    it inside the per-session memory context bookkeeping). When building a
    procedure's globals (`plpy_procedure.c:368` and the arg/TD setup in
    `plpy_exec.c`), insert the *session's* GD rather than the process
    `__main__` GD. Concretely: `proc->globals` copy currently pulls GD from the
    process main dict; instead set `PyDict_SetItemString(proc->globals, "GD",
    session_GD)` after the copy, overwriting the inherited process GD reference.
  - This DOES require one more `.def` field after all:
    `PgCurrentPLpythonGDRef()` -> `void *plpython_gd`. Add it to Section 3
    (struct + accessors + proto) exactly like `plpython_execution_contexts`.
    Session reset (`Py_DECREF` the GD) hooks the session-reset callback
    (Section 6f).
  - Ceiling/subtlety: GD holds arbitrary Python objects with refcounts; it must
    be `Py_DECREF`'d on session teardown while the GIL is held, from the
    session-reset callback (which must itself `PyGILState_Ensure`).

  Revised `.def` additions: **three** fields, not two --
  `plpython_execution_contexts`, `plpython_explicit_subxacts`, `plpython_gd`
  (accessors `PgCurrentPLpythonExecutionContextsRef`,
  `PgCurrentPLpythonExplicitSubxactsRef`, `PgCurrentPLpythonGDRef`).

### 6e. Error / exception globals

`PLy_exc_*` type objects and `PLy_spi_exceptions` HTAB are interpreter-global
(Section 1a). Comparisons (`PyErr_GivenExceptionMatches`, `plpy_elog.c:80-89`)
run under the held GIL, so no race. `PyErr_Occurred()` / `PyErr_Clear()`
(`plpy_main.c:255,362`) are per-OS-thread-current-exception, which is correct
because the current thread state is the one PyGILState just handed us. No work.

### 6f. Session-reset callback (mirror plperl)

plperl registers `plperl_session_reset_callback` via
`PgSessionRegisterResetCallback` (`plperl.c:469`, prototype
`backend_runtime.h:3299`). plpython should register an analogous
`plpython_session_reset_callback` on first PL/Python touch in a session,
guarded by a `static bool` (or the vestigial `plpython_reset_registered` field
-- reuse it, it exists at `backend_runtime.h:1892`). The callback must, **under
`PyGILState_Ensure`**:
- `Py_XDECREF` the session GD and clear `plpython_gd`;
- clear `PLy_execution_contexts` / `explicit_subtransactions` heads (memory is
  freed with the transaction/portal contexts already, so just null the heads);
- leave the interpreter, type objects, and `PLy_spi_exceptions` intact
  (process lifetime).

### 6g. Guard / fallback if PyThreadState corruption appears

- **Compile-time / config fallback:** the safety net is the module-magic marker
  itself. If threaded plpython proves unstable, revert `plpy_main.c` to
  `PG_MODULE_MAGIC_BACKEND_MODEL_PROCESS` (Section 5). The backend-model gate
  (`dfmgr.c:77-79`, `fmgr.h:495-521`) then routes any session needing plpython
  to a forked process-fallback backend (Phase 19), exactly as an
  unmigrated-C-extension session is today. plpython keeps working, just not
  in-carrier. This is a one-line revert -> zero-risk escape hatch.
- **Runtime assertion:** in debug builds, `Assert(PyGILState_Check())` at the
  top of the deepest Python-touching helpers (e.g. `PLy_procedure_call`
  `plpy_exec.c:1151`, `PLy_current_execution_context` `plpy_main.c:406`) catches
  "entered Python without the GIL" regressions cheaply.
- **Fail-stop:** genuine interpreter corruption in threaded mode fail-stops the
  whole process (fork-model exception per AGENTS.md), which is the intended
  behavior -- external supervision restarts. No half-alive interpreter.

---

## 7. Python version target

- Configure prefers `python3` and supports Python 3 only
  (`config/python.m4:12,22`); meson notes "python 3.6 compatibility on old
  platforms" (`meson.build:14`) and depends on `python3_dep`
  (`meson.build:1449-1481,4358`). Effective floor: **Python 3.6**.
- Option C needs nothing newer. The APIs used --
  `Py_Initialize`, `PyEval_SaveThread`, `PyGILState_Ensure/Release`,
  `PyDict_New`, `PyDict_SetItemString` -- are all pre-3.6 stable-ABI. **No 3.12+
  feature (PEP 684 per-interpreter GIL, sub-interpreters) is used or needed.**
  Explicitly NOT using sub-interpreters -- that is the rejected alternative to
  Option C.
- `PyEval_InitThreads` is deprecated/no-op since 3.9 and unnecessary since 3.7
  (threads auto-init in `Py_Initialize`); do not call it.

---

## 8. Phased implementation order (smallest safe steps)

Each step keeps process mode green (`gmake check`) and is independently
buildable. The module-magic flip is last.

1. **P0 -- `.def` scaffolding (no behavior change).** Add the three struct
   fields (`backend_runtime.h`), accessor macros
   (`...field_accessors.def`), and fallback prototypes
   (`...field_accessor_prototypes.def`) from Sections 3 + 6d. Build only; no
   plpython `.c` change yet. Validate: `gmake check-global-lifetimes`,
   `gmake check-runtime-lifecycles` (touches the runtime-root/state family).

2. **P1 -- move the two stack heads per-session.** Apply the `#define` aliases
   for `PLy_execution_contexts` (`plpy_main.c`) and `explicit_subtransactions`
   (`plpy_subxactobject.c`), remove the header extern + `_PG_init` resets
   (Section 4). Still `PG_MODULE_MAGIC_BACKEND_MODEL_PROCESS`. Validate: `gmake
   check` + plpython regression (`meson.build:74-99` suite) -- must be
   byte-identical output in process mode.

3. **P2 -- per-session GD.** Create/stash/teardown session GD (Section 6d),
   wire `plpy_procedure.c:368` globals build to use session GD, register the
   session-reset callback (Section 6f). Still PROCESS. Validate: `gmake check` +
   `plpython_global` regression test specifically (it exercises GD).

4. **P3 -- GIL discipline.** Add `PyEval_SaveThread` at init (guarded, Section
   6a) and `PyGILState_Ensure/Release` wrappers at the three entry points
   (Section 6b), plus the reset-callback `Ensure`. Still PROCESS (single OS
   thread -> Ensure/Release are cheap no-op-ish, refcount only). Validate: `gmake
   check` + full plpython regression; add `Assert(PyGILState_Check())` probes
   (Section 6g) under `--enable-cassert`.

5. **P4 -- flip the marker.** Change to
   `PG_MODULE_MAGIC_BACKEND_MODEL_POOLED_PROTOCOL_AFFINE` and rewrite the
   header comment (Section 5). Validate: `gmake check`, `gmake check-threaded`,
   `gmake check-threaded-pooled`, `gmake check-threaded-workers`,
   `gmake check-threaded-world-core`, plus the new TAP (Section 9).

6. **P5 -- docs + ledger.** Update `MULTITHREADED_PLAN.md:1004-1006` (flip
   plpython from PROCESS to POOLED_PROTOCOL_AFFINE with the same evidence
   sentence format used for plperl at `MULTITHREADED_PLAN.md:997-1003`), and
   record TAP evidence.

Commit granularity: P0+P1 can be one commit (scaffold + mechanical move); P2,
P3, P4 each their own commit (GD semantics / GIL / marker are distinct
conceptual changes and each is independently revertible -- important given P4 is
the risk gate). Push after each commit per AGENTS.md.

---

## 9. TAP test plan (mirror TAP 012)

New file: `src/test/modules/test_backend_runtime/t/0NN_phase16_pooled_plpython_affine.pl`
(next free number; 012 is plperl). Structure copied from
`t/012_phase16_pooled_plperl_affine.pl` (read in full for this plan), adapted to
plpython:

- **Cluster config:** `multithreaded = on`, `pooled_protocol_carriers = 2`,
  `$NSESS = 8`, `$ROUNDS = 25` -- i.e. **more sessions than carriers**, so
  sessions are guaranteed to (a) interleave on shared carrier OS threads and (b)
  run Python on two OS threads at once (the GIL cross-carrier test). Same
  skip_all guard if plpython3 is unavailable (`CREATE EXTENSION plpython3u`
  return-code check).

- **Assertion 1 -- per-session GD isolation across interleaved rounds.**
  ```sql
  CREATE FUNCTION py_set_tag(t text) RETURNS void LANGUAGE plpython3u AS $$
      GD['tag'] = args[0]        # GD is session-global
  $$;
  CREATE FUNCTION py_get_tag() RETURNS text LANGUAGE plpython3u AS $$
      return GD.get('tag')
  $$;
  ```
  Each of 8 background sessions sets `GD['tag'] = 'sess-N'`; round-robin read
  back over 25 rounds must always return this session's own tag. If GD were
  process-global (pre-P2) sessions would clobber each other -> `$bad > 0`.

- **Assertion 2 -- SD (per-function-per-session) isolation.** A function using
  a module-level `SD` counter incremented per call; each session calls it a
  distinct number of times, then reads its own count. Confirms SD does not leak
  across sessions on a shared carrier.
  ```sql
  CREATE FUNCTION py_bump() RETURNS int LANGUAGE plpython3u AS $$
      SD['n'] = SD.get('n', 0) + 1
      return SD['n']
  $$;
  ```
  Interleave calls; session i's returned count must equal its own call count,
  never another session's.

- **Assertion 3 -- mid-flight re-stamp.** Re-set GD to `'re-N'` mid-run, repeat
  the interleaved read loop, confirm isolation still holds (mirrors TAP-012's
  second block).

- **Assertion 4 -- nested plpython via SPI.** plpython function that runs
  `plpy.execute("SELECT py_get_tag()")` and returns the inner value; confirms
  the re-entrant `PyGILState_Ensure` refcount path (Section 6b) and that the
  inner call sees the *same* session's GD. Mirrors TAP-012's nested-SPI block.
  ```sql
  CREATE FUNCTION py_get_tag_via_spi() RETURNS text LANGUAGE plpython3u AS $$
      rv = plpy.execute("SELECT py_get_tag() AS t")
      return rv[0]['t']
  $$;
  ```

- **Assertion 5 -- explicit subtransaction stack isolation.** Each session opens
  and commits/rolls-back `plpy.subtransaction()` blocks interleaved with
  siblings; verify no cross-session subxact-stack corruption (a mismatched
  enter/exit would error). Exercises the per-session `explicit_subtransactions`
  move (Section 6c).
  ```sql
  CREATE FUNCTION py_subxact(ok bool) RETURNS void LANGUAGE plpython3u AS $$
      with plpy.subtransaction():
          plpy.execute("SELECT 1")
          if not args[0]:
              plpy.execute("SELECT 1/0")
  $$;
  ```

- **Assertion 6 (cross-carrier GIL stress, optional but recommended).** Fire all
  8 sessions at a CPU-bound plpython function *simultaneously* (async queries
  via `background_psql` without waiting between sends) so two carriers execute
  Python at the same wall-clock instant. Correct GIL handling -> all return
  correct results, no crash; broken GIL -> segfault/garbage. This is the direct
  regression test for Section 6a/6b.

Register the test in the module's `meson.build` / `Makefile` `TAP_TESTS` list
(same place TAP 012 is registered under `src/test/modules/test_backend_runtime/`).

Success criterion (copy TAP-012's phrasing): "plpython interpreter state stays
per-session across N interleaved rounds (M sessions, 2 carriers)" +
per-session GD/SD isolation after mid-flight re-stamp + through nested SPI +
subxact-stack isolation + cross-carrier concurrent execution, all with
`is($bad, 0, ...)`.

---

## 10. Files touched (summary map)

| File | Change | Section |
|---|---|---|
| `src/include/utils/backend_runtime.h` | +3 struct fields | 3a, 6d |
| `.../backend_runtime_current_state_field_accessors.def` | +3 accessor macros | 3b, 6d |
| `.../backend_runtime_current_state_field_accessor_prototypes.def` | +3 fallback protos | 3c, 6d |
| `src/pl/plpython/plpy_main.c` | GIL init+save; alias `PLy_execution_contexts`; entry-point `PyGILState` wraps; reset callback; magic flip; comment | 4a, 5, 6a, 6b, 6f |
| `src/pl/plpython/plpy_subxactobject.c` | alias `explicit_subtransactions` | 4b |
| `src/pl/plpython/plpy_subxactobject.h` | drop extern | 4b |
| `src/pl/plpython/plpy_procedure.c` | per-session GD into `proc->globals` | 6d |
| `src/pl/plpython/plpy_exec.c` | (only if GD wiring reaches arg/TD setup) | 6d |
| `src/test/modules/test_backend_runtime/t/0NN_phase16_pooled_plpython_affine.pl` | new TAP | 9 |
| `src/test/modules/test_backend_runtime/{meson.build,Makefile}` | register TAP | 9 |
| `plan_docs/MULTITHREADED_PLAN.md` | flip plpython entry to affine + evidence | 8.P5 |

Untouched by design (process-lifetime exceptions / already-per-session):
`plpy_elog.c` (exception type objects), `plpy_plpymodule.c`
(`PLy_spi_exceptions` HTAB), `PLy_interp_globals` pointer, `PLy_*Type` statics,
the funccache-backed procedure cache.
