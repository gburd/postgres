# libxslt Error Globals under the Threaded Runtime -- Audit

Status: analysis only (READ-ONLY audit). No code changed.
Scope: Phase 16 / Gate E2-Extensions -- sibling of the libxml structured-error
analysis (`LIBXML_ERROR_HANDLER_THREADED_DESIGN.md`), covering the *libxslt*
globals reached by `contrib/xml2`'s `xslt_process`.

Only consumer of libxslt in the tree: `contrib/xml2/xslt_proc.c`
(`xslt_process`). `contrib/xml2/xpath.c` uses libxml only (no libxslt symbols;
grep confirms). All libxml error routing for both is the shared
`pg_xml_init`/`pg_xml_done` path already analyzed in the sibling doc.

Runtime under audit: libxslt **1.1.45**
(`.../libxslt-1.1.45-dev/.../xsltconfig.h:30`, `LIBXSLT_VERSION 10145`),
libxml2 2.15.3.

Model facts (identical to sibling doc, from `MULTITHREADED_PLAN.md`):
cooperative scheduling, preemption OFF (`:4103-4105`), fibers PINNED
(`migratable=0`, `:3155-3169`). Sessions are libxtc fibers on shared carrier
OS threads; a process-global mutable word shared across concurrent carriers is
the hazard class.

---

## 1. Inventory of libxslt calls in `xslt_process` (file:line)

All in `contrib/xml2/xslt_proc.c`, inside one `xslt_process` invocation.

| # | Call | file:line | State touched | Class |
|---|------|-----------|---------------|-------|
| 1 | `xsltParseStylesheetDoc(ssdoc)` | `:129` | Builds a heap `xsltStylesheetPtr`; parsing/validation errors route through **libxml** (libxml thread-local handler, installed by `pg_xml_init`). Touches no libxslt error global. | per-object |
| 2 | `xsltNewTransformContext(stylesheet, doctree)` | `:135` | Heap-allocs `xsltTransformContextPtr`; its `error`/`errctx` fields (`xsltInternals.h:1750-1751`) are initialized to libxslt's **process-global** `xsltGenericError`/`xsltGenericErrorContext` defaults. Reads the global; does not write it. | per-object (reads global default) |
| 3 | `xsltNewSecurityPrefs()` | `:138` | Heap-allocs `xsltSecurityPrefsPtr`. No global. | per-object |
| 4 | `xsltSetSecurityPrefs(sec, PREF, forbid)` x6 | `:141,143,145,147,149,151` | Mutates fields of the *heap* `sec` object from call #3. **Not** `xsltSetDefaultSecurityPrefs` (which would be the process-global setter, `security.h:71`). No global. | per-object |
| 5 | `xsltSetCtxtSecurityPrefs(sec, xslt_ctxt)` | `:153` | Attaches the heap `sec` to the heap transform context. No global. | per-context |
| 6 | `xsltApplyStylesheetUser(stylesheet, doctree, params, NULL, NULL, xslt_ctxt)` | `:159` | Runs the transform. Transform-time errors go to `xslt_ctxt->error` (= the global default copied at #2, i.e. libxslt's `xsltGenericErrorDefault` -> stderr). CPU + memory only on already-parsed trees; no document loading. | per-context (error sink = global default) |
| 7 | `xsltSaveResultToString(&resstr, &reslen, restree, stylesheet)` | `:165` | Serializes result tree to string. No global error state. | per-object |
| 8 | `xsltFreeTransformContext` / `xsltFreeSecurityPrefs` / `xsltFreeStylesheet` | `:175-177`, `:191-194` | Frees the per-object state from #1-#5. No global. | per-object |
| 9 | `xsltCleanupGlobals()` | `:181` (catch), `:195` (normal) | **PROCESS-GLOBAL teardown.** Frees/resets libxslt's global extension-module registry and its global mutex. Called on *every* `xslt_process` invocation. See Section 4. | **process-global mutator** |

Not called anywhere in the tree (grep-confirmed, `-r` over `*.c`/`*.h`):
`xsltSetGenericErrorFunc`, `xsltSetGenericDebugFunc`,
`xsltSetTransformErrorFunc`, `exsltRegisterAll`, `xsltRegisterAllExtras`,
`xsltSetXIncludeDefault`, `xsltSetDefaultSecurityPrefs`, `xsltInit`.

### The load-bearing observation

`xslt_process` **never installs a libxslt error handler at all** -- neither the
process-global `xsltSetGenericErrorFunc` nor the per-context
`xsltSetTransformErrorFunc`. It relies entirely on:

- libxml's thread-local structured handler (installed by `pg_xml_init` via
  `xmlSetStructuredErrorFunc`, see sibling doc) to capture the *parsing* errors
  from `xmlReadMemory` (`:105,116`) and `xsltParseStylesheetDoc` (which parses
  via libxml), and
- `pg_xml_error_occurred(xmlerrcxt)` checks after each step
  (`:110,121,131,161`)

to detect failures. Genuine libxslt *transform-time* diagnostics (from
`xsltApplyStylesheetUser`) are emitted through the untouched libxslt global
handler -- libxslt's built-in `xsltGenericErrorDefault`, which writes to
`stderr`. They are not captured into `xmlerrcxt`; the code only notices
transform failure via `restree == NULL` / the libxml error flag (`:161`).

So from PG's side the libxslt error globals `xsltGenericError` /
`xsltGenericErrorContext` are **read once per transform-context creation and
never written**. There is no PG-driven clobber of a shared error-handler slot,
unlike the libxml `xmlSetStructuredErrorFunc` save/install/restore dance.

---

## 2. Thread-local vs process-global (header + symbol evidence)

### libxslt error globals are PROCESS-GLOBAL, not thread-local.

Declaration (`xsltutils.h:144-147`):

```c
XSLTPUBVAR xmlGenericErrorFunc xsltGenericError;
XSLTPUBVAR void *xsltGenericErrorContext;
XSLTPUBVAR xmlGenericErrorFunc xsltGenericDebug;
XSLTPUBVAR void *xsltGenericDebugContext;
```

`XSLTPUBVAR` expands to plain `extern` (`xsltexports.h:55`):

```c
#define XSLTPUBVAR XSLTPUBLIC extern      /* XSLTPUBLIC is empty */
```

No `__thread`, no accessor-function indirection. Contrast libxml2 2.15, whose
`xmlStructuredError` is `(*__xmlStructuredError())` reaching per-thread global
state (sibling doc Section 1). **libxslt never adopted libxml2's per-thread
global mechanism**: there are no `xsltThrDef*` symbols and no
`LIBXML_THREAD_ENABLED`-gated per-thread copies in the libxslt headers.

Dynamic-symbol confirmation (`nm -D libxslt.so`, v1.1.45):

```
000000000003e028 D xsltGenericError@@LIBXML2_1.0.24        # D = initialized .data
000000000003e090 B xsltGenericErrorContext@@LIBXML2_1.0.24 # B = .bss
```

Both are ordinary process-global storage (`D`/`B`), not TLS. So a write to
`xsltGenericError` (via `xsltSetGenericErrorFunc`, `T` in the .so) would mutate
one process-wide word shared by every carrier and every fiber.

### The clean per-context alternative exists in this runtime.

The transform context carries its own handler (`xsltInternals.h:1750-1751`):

```c
xmlGenericErrorFunc  error;   /* a specific error handler */
void              * errctx;   /* context for the error handler */
```

set by `xsltSetTransformErrorFunc(ctxt, ctx, handler)` (`xsltutils.h:164`,
present in the .so as `T xsltSetTransformErrorFunc@@LIBXML2_1.0.22`). This
writes NO global -- it stores the handler on the heap `xsltTransformContextPtr`
the backend already creates at `:135`. This is the exact analogue of libxml's
`xmlCtxtSetErrorHandler`, and it is available unconditionally in the linked
libxslt (has existed since libxslt 1.1.0; the `1.0.22` symbol version predates
any libxslt PG would build against).

---

## 3. Does `xslt_process` yield in the critical window?

No. Trace `xslt_process` start (`:47`) to finish (`:210`):

- `pgxml_parser_init` -> `pg_xml_init` + `xmlInitParser` (`xpath.c:73-78`).
  Comment at `xpath.c:79`: "we're assuming an elog cannot be thrown by the
  following calls".
- Body (`:88`-`:170`): `xmlReadMemory` x2, `xsltParseStylesheetDoc`,
  `xsltNewTransformContext`, security-prefs setup, `xsltApplyStylesheetUser`,
  `xsltSaveResultToString`, `cstring_to_text*`. **No SPI, no `table_open`, no
  lock acquisition, no `WaitLatch`/`WaitEventSetWait`, no protocol read/flush,
  no external I/O.**
- libxml's entity loader is defanged process-wide (`xml.c:2036-2052`,
  established in the sibling doc), and here security prefs additionally forbid
  `READ_FILE`/`READ_NETWORK`/etc on the transform context (`:141-153`) --
  the transform cannot fetch documents, so it cannot block on I/O.
- Inputs to `xsltApplyStylesheetUser` are already-parsed in-memory trees
  (`doctree`, `stylesheet`); the transform is CPU + `palloc`/`xmlMalloc` only.
- Teardown + `pg_xml_done` (`:172`-`:207`). `xsltCleanupGlobals` is CPU +
  free.

So the "no yield between `pg_xml_init` and `pg_xml_done`" invariant that makes
libxml safe today (sibling doc Section 4) **also holds for the libxslt window**.
The scheduler cannot switch fibers inside it (cooperative, preemption off), and
fibers are pinned, so no second fiber runs on the carrier and no migration
occurs while any libxslt state is live.

---

## 4. `xsltCleanupGlobals()` -- the one real process-global write

Unlike the error globals (never written by PG), `xsltCleanupGlobals()` (`:181`,
`:195`) IS a process-global mutation on every `xslt_process` call. It resets
libxslt's global extension-module registry and destroys/recreates the global
registry mutex. Assessment under the current model:

- It runs inside the no-yield window (Section 3), so no other fiber is
  executing libxslt concurrently on the same carrier, and pinning means no
  concurrent libxslt on another carrier can be reached mid-window by *this*
  fiber.
- BUT it is process-wide: if two carriers were *ever* to run `xslt_process`
  truly concurrently (which pinning + cooperative + no-yield prevents *today*
  for a single carrier, but does NOT prevent across *different* carriers each
  running their own fiber), carrier A's `xsltCleanupGlobals` frees/reinits the
  registry that carrier B's live transform may rely on.

This is the sharper hazard than the error globals: it is a genuine
cross-carrier process-global mutation with no PG guard. It is inherited
verbatim from upstream (upstream single-process backends serialize it by having
one backend per process), and it is arguably misplaced even upstream (cleaning
*global* state per-call is heavy-handed), but under threaded carriers it is a
correctness question, not just style. See verdict.

`xsltMaxDepth` (`xslt.h:62`) and `xsltSetDefaultSecurityPrefs`
(`security.h:71`) are other process-global libxslt knobs -- neither is touched
by `xslt_process`, so they stay at defaults and are not a hazard here.

---

## 5. Verdict

### Error globals: SAFE TODAY (safer than libxml, for a different reason).

libxml's handler is thread-local and *written* per call (install/restore),
making it safe only because of the no-yield window. libxslt's error globals are
process-global but **never written by PG** -- `xslt_process` installs no
libxslt handler and leaves `xsltGenericError` at its default. A word that is
only ever read cannot be clobbered by a concurrent carrier. So the
process-global-ness of the libxslt error handler is a latent trap (any future
call to `xsltSetGenericErrorFunc` would be an immediate cross-carrier hazard,
strictly worse than libxml because there is no thread-local cushion at all) but
is not exercised today.

### `xsltCleanupGlobals()`: the actual gap -- NOT-SAFE across concurrent carriers.

This is a per-call process-global mutation. Under strictly-one-carrier
execution (today's effective state for a single fiber, given pinning +
cooperative + no-yield) it does not corrupt *that* fiber. But it is
process-wide and unguarded, so two carriers each running `xslt_process`
concurrently can race on libxslt's global registry. Pinning + no-yield protect
a fiber from being interleaved *on its own carrier*; they do NOT serialize two
different carriers each executing their own `xslt_process`. This is the item
that blocks an unqualified affinity marking.

---

## 6. Durable fix (not implemented here)

Two independent items; both small, both local to `xslt_proc.c` (no core `xml.c`
change needed -- this is libxslt-specific, so it does not ride the sibling
doc's shared `pg_xml_init` fix).

### (a) Install a per-transform-context error handler (defense-in-depth).

Right after `xsltNewTransformContext` (`:135`), route libxslt transform
diagnostics into the existing `xmlerrcxt` instead of the process-global
stderr default:

```c
xsltSetTransformErrorFunc(xslt_ctxt, (void *) xmlerrcxt, <adapter>);
```

`xsltSetTransformErrorFunc` (`xsltutils.h:164`) writes only the heap
`xslt_ctxt->error`/`errctx` -- no global, migration-safe by construction. This
also *improves behavior* (transform errors become capturable PG errors instead
of stderr noise) and it never reads/writes `xsltGenericError`. It is
unconditionally available in any libxslt PG supports (symbol version 1.0.22).
Because it uses `xmlGenericErrorFunc` (printf-style), the adapter is a thin
`vsnprintf`-into-`xmlerrcxt` shim rather than reuse of the structured
`xml_errorHandler`; that is the only non-trivial line.

### (b) Remove per-call `xsltCleanupGlobals()`, or guard it.

The durable fix for the real hazard (Section 4): do NOT call
`xsltCleanupGlobals()` per invocation. libxslt global registry teardown belongs
at library shutdown, not per-transform. Options, laziest first:

1. **Drop the two `xsltCleanupGlobals()` calls** (`:181`, `:195`). PG never
   populates the libxslt global extension registry (no `exsltRegisterAll`),
   so there is nothing per-call to clean; the calls are cost + hazard with no
   benefit. This is the smallest, safest change and removes the cross-carrier
   race entirely.
2. If some libxslt build *does* accumulate global state, move a single
   `xsltCleanupGlobals()` to a process-exit / library-unload path, once.

Either way, the per-call process-global mutation disappears and the
cross-carrier race in Section 4 is gone.

---

## 7. Recommendation for the affinity marking

**`contrib/xml2` CANNOT be marked `POOLED_PROTOCOL_AFFINE` on the strength of
the error globals alone being fine.** The error-handler globals are safe today
(never written), but the module still performs an unguarded per-call
process-global mutation via `xsltCleanupGlobals()` (Section 4), which races
across concurrent carriers and is not protected by pinning/cooperative/no-yield
(those only serialize a *single* carrier's fibers, not two carriers).

Two acceptable paths:

- **Preferred: land durable fix (b) first** (drop per-call
  `xsltCleanupGlobals()`), optionally with (a). Then `contrib/xml2` can be
  marked `POOLED_PROTOCOL_AFFINE` with the documented invariant:
  *"xml2/xslt touches libxslt error globals read-only (never installs a libxslt
  handler) and performs no per-call libxslt global mutation; all error routing
  is per-context (libxml thread-local handler + optional
  `xsltSetTransformErrorFunc`), and the transform window does not yield."*
  Guard: a threaded regression running concurrent `xslt_process` across pooled
  carriers, asserting no crash / no `"libxml error handling state is out of
  sync"` WARNING (`xml.c:1356-1357`) and correct per-session results.

- **Defer-with-invariant (if marking now without (b)):** permissible ONLY if
  the pool guarantees `xslt_process` is never run on two carriers
  concurrently -- i.e. a documented invariant that libxslt work is serialized
  (single-carrier affinity, or an explicit process-wide lock around
  `xslt_process`). State the invariant explicitly, name the guard that catches
  a violation (the concurrent-carrier regression above; a genuine crash
  fail-stops the process), and record that Phase 16 / Gate E2-Extensions owns
  landing fix (b) to lift the serialization requirement. Without such a
  serialization invariant, the marking is unsound because of
  `xsltCleanupGlobals()`, not the error handler.

Bottom line: the *error-globals* sibling gap the libxml doc flagged turns out
benign (read-only usage), but auditing it surfaced a distinct, real hazard --
per-call `xsltCleanupGlobals()`. Fix (b) is a two-line deletion and should land
before an unqualified `POOLED_PROTOCOL_AFFINE` marking; fix (a) is optional
hardening + a behavior improvement.
