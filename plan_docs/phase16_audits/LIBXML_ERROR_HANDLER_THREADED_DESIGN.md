# libxml Structured Error Handler under the Threaded Runtime -- Design Analysis

Status: analysis only (READ-ONLY audit). No code changed.
Scope: Phase 16 / Gate E2-Extensions -- unblock `contrib/xml2` affinity marking
and harden the core `xml` data type on the same `pg_xml_init` path.

Owner path: `src/backend/utils/adt/xml.c` `pg_xml_init()` /
`pg_xml_done()` / `xml_errorHandler()`. Shared by core `xml` and by
`contrib/xml2` (`pgxml_parser_init` -> `pg_xml_init`,
`contrib/xml2/xpath.c:33,68`).

---

## 1. What `xmlSetStructuredErrorFunc` actually touches

`pg_xml_init()` saves and installs libxml's structured error handler
(`src/backend/utils/adt/xml.c:1276-1281`):

```c
errcxt->saved_errfunc = xmlStructuredError;
#ifdef HAVE_XMLSTRUCTUREDERRORCONTEXT
    errcxt->saved_errcxt = xmlStructuredErrorContext;
#endif
    xmlSetStructuredErrorFunc(errcxt, xml_errorHandler);
```

and `pg_xml_done()` restores them (`xml.c:1360`):

```c
xmlSetStructuredErrorFunc(errcxt->saved_errcxt, errcxt->saved_errfunc);
```

`xmlStructuredError` / `xmlStructuredErrorContext` are the storage that
`xmlSetStructuredErrorFunc` writes. In the libxml2 on this system (2.15.3,
`/nix/store/.../libxml2-2.15.3-dev/include/libxml2/libxml/xmlversion.h:19`,
`LIBXML_VERSION 21503`, `LIBXML_THREAD_ENABLED` defined at line 46) these are:

```c
/* xmlerror.h:960-961 */
XMLPUBFUN xmlStructuredErrorFunc *__xmlStructuredError(void);
XMLPUBFUN void **__xmlStructuredErrorContext(void);
/* xmlerror.h:990,997 */
#define xmlStructuredError        (*__xmlStructuredError())
#define xmlStructuredErrorContext (*__xmlStructuredErrorContext())
```

### CONFIRMED: thread-local, NOT process-global.

Header evidence, `xmlerror.h:985-997` (comments are libxml2's own):

> "Thread-local variable containing the structured error callback."
> "Thread-local variable containing user data for the structured error handler."

Since libxml2 >= 2.6 with `LIBXML_THREAD_ENABLED`, the error handler /
context / last-error live in libxml's per-thread global state
(`xmlGlobalState`, reached via the `__xmlStructuredError()` accessors). With
`LIBXML_THREAD_ENABLED` set (line 46) each OS thread has its own copy. So
`xmlSetStructuredErrorFunc` mutates **the calling OS thread's** handler slot,
not a single process-wide word.

This is strictly better than the worst case, but under the fiber model
"thread-local" means "carrier-OS-thread-local", which is NOT the same as
"backend-local" -- that is the whole hazard (Section 2).

### A non-global per-context handler exists (the clean fix)

libxml2 2.13+ added per-parser-context and per-xpath-context error handlers
that touch NO global at all:

```c
/* parser.h:1969 */
void xmlCtxtSetErrorHandler(xmlParserCtxt *ctxt,
                            xmlStructuredErrorFunc handler, void *data);
/* xpath.h:513 */
void xmlXPathSetErrorHandler(xmlXPathContext *ctxt,
                             xmlStructuredErrorFunc handler, void *context);
```

Both symbols are present in the linked runtime
(`nm -D libxml2.so.16`: `xmlCtxtSetErrorHandler`, `xmlXPathSetErrorHandler`,
`xmlSetStructuredErrorFunc` all `T`). These attach the handler to the
individual `xmlParserCtxtPtr` / `xmlXPathContextPtr` the backend already
creates (`xml.c:1872,4465,4474,4753,4811`), so no thread-local or global slot
is written and there is nothing to save/restore.

Version constraint: `xmlCtxtSetErrorHandler` / `xmlXPathSetErrorHandler`
landed in **libxml2 2.13.0**. PG's minimum supported libxml2 is **2.6.23**
(`configure.ac:1133,1458`: `libxml-2.0 >= 2.6.23`). So the clean API is NOT
unconditionally available; it needs a `LIBXML_VERSION >= 21300` gate with a
fallback for older libraries.

---

## 2. The actual hazard under cooperative-fiber + migration

Model facts (from the plan):

- Scheduling is **cooperative, preemption OFF**:
  `MULTITHREADED_PLAN.md:4103-4105` -- "preemption is off by default
  (preempt_interval_ns==0) and PG never calls xtc_exec_set_preempt". A fiber
  only yields at explicit wait boundaries (`xtc_pg_wait_fd`, `pg_fdatasync`,
  `WaitLatch` seam), never mid-CPU.
- Fibers are currently **PINNED** (`migratable=0`), so live carrier
  migration does not happen today; it is a deferred Phase-D obligation
  (`MULTITHREADED_PLAN.md:3155-3169,4103-4118`). Pinning exists precisely
  because PG per-backend state is carrier-`__thread`-bound and is only
  save/restored at specific wait boundaries, not on every switch.

Because the handler is carrier-thread-local, the hazard is entirely about
**whether a second fiber runs on the same carrier between `pg_xml_init` and
`pg_xml_done`** (same-carrier interleave) or **whether the fiber changes
carriers in that window** (migration).

### 2a. Same-carrier cooperative interleave

If session A does `pg_xml_init` (sets carrier C's TLS handler to A's errcxt),
then YIELDS before `pg_xml_done`, the scheduler can run session B on carrier
C. B's `pg_xml_init` overwrites C's TLS handler with B's errcxt and saves what
it thinks is the "previous" (actually A's) into B's save slots. When A
resumes, C's TLS now points at B (or, after B's `pg_xml_done`, at whatever B
restored) -> A's libxml errors route to the wrong context, and the
save/restore nesting is corrupted (B restored A's live handler as if it were
the pre-existing one). `pg_xml_done`'s own guard would fire:
`elog(WARNING, "libxml error handling state is out of sync with xml.c")`
(`xml.c:1356-1357`) -- but only as a symptom, after misrouting already
happened.

**This hazard requires a yield between init and done. Section 4 shows there is
none on any core or contrib path.** So under today's pinned + cooperative +
no-yield-in-window model, same-carrier interleave cannot occur: the window is
a single non-yielding CPU section, the scheduler cannot switch fibers inside
it, and A's `pg_xml_done` restores before any other fiber touches C's TLS.

### 2b. Carrier migration

If a fiber could migrate C1 -> C2 between init and done, it would set C1's TLS
in `pg_xml_init`, run libxml on C2 (whose TLS handler is stale/foreign ->
errors misrouted or dropped), and restore C1's TLS in `pg_xml_done` (leaving
C1 wrong and never fixing C2). This is the same class as the general
carrier-`__thread` migration hazard the plan pins against
(`MULTITHREADED_PLAN.md:3159-3164`). It is NOT reachable today (fibers pinned),
and even when migration is enabled it only bites if migration can occur in the
init/done window -- which requires a yield in that window (Section 4: none).

### Net

Under the current runtime (pinned, cooperative, no yield in the window) the
process-global-style clobber the concern describes **does not actually fire**:
the thread-local nature plus the no-yield init/done window makes it safe today.
BUT the safety is entirely *implicit* -- it rests on "no XML function ever
yields between init and done" and "fibers stay pinned", neither of which is
enforced by the code. That is exactly why it blocks a durable
`POOLED_PROTOCOL_AFFINE` marking: the invariant is real but undocumented and
unguarded, and it silently breaks the day someone adds a yield in an XML path
(e.g. a future streaming/large-doc path, an external-entity fetch, or a
CHECK_FOR_INTERRUPTS that becomes a yield) or the day migration is enabled.

---

## 3. Phase 8's prior conclusion (error handler vs allocation context)

Phase 8 classified ONLY the libxml **allocation** context, not the error
handler.

- `MULTITHREADED_PHASE8_THREAD_SAFETY.md:409-413`: "`LibxmlContext` is
  backend-local TLS for the optional `USE_LIBXMLCONTEXT` allocator hook path,
  where libxml callbacks allocate into the active backend's top memory
  context." That pointer is a *PG* global, so Phase 8 made it a per-backend
  accessor: `PgCurrentLibxmlContextRef()`
  (`src/backend/utils/misc/backend_runtime_utility.c:302-306`, used via
  `#define LibxmlContext (*PgCurrentLibxmlContextRef())` at `xml.c:142`).
- `MULTITHREADED_PHASE8_THREAD_SAFETY.md:1389-1394`: explicitly notes the
  local build "is not a libxml-enabled debug build, so the allocator-hook
  classification has compile/static coverage here rather than direct
  `USE_LIBXMLCONTEXT` runtime coverage" -- and `USE_LIBXMLCONTEXT` is OFF by
  default (`xml.c:44`).

Grep confirms Phase 8 and Phase 12 say **nothing** about
`xmlSetStructuredErrorFunc` / `xml_errorHandler` / `saved_errfunc` /
`saved_errcxt` (no matches in either doc). The distinction Phase 8 missed:

- `LibxmlContext` is a **PG-owned** pointer -> Phase 8 correctly made it
  per-backend. This is the pattern to reuse (`PgCurrentLibxmlContextRef`).
- `xmlStructuredError[Context]` is **libxml-owned** carrier-thread-local
  state, reached only through `xmlSetStructuredErrorFunc`. Phase 8 did not
  classify it because it is not a PG global -- the global-lifetime scanner
  cannot see inside libxml. It is a genuine, uncovered gap.

---

## 4. Do XML functions yield between `pg_xml_init` and `pg_xml_done`?

**No.** Every init/done window is pure libxml CPU work (parse / serialize /
xpath) with no SPI, no lock acquisition, no `WaitLatch`/`WaitEventSetWait`, no
protocol read/flush, and no external I/O.

- Entity loader is defanged: `xmlPgEntityLoader` returns an empty string input
  stream and performs no fetch (`xml.c:2036-2052`) -- so parsing never does
  network/file I/O.
- Core `xml.c` init/done pairs wrap tight parse/serialize regions:
  `536`, `739`, `951`, `1824`, `2666-2707`, `4455-4560` (XmlTable init),
  `4747-4763` (XmlTable fetch). The SPI / `table_open` calls in the
  `query_to_xml` / `cursor_to_xml` family (`xml.c:2834,2975,3058,3097,3117,
  3145,3175,3198`) build XML **text** via `StringInfo` and are NOT inside any
  `pg_xml_init` window.
- `contrib/xml2/xpath.c`: all init/done pairs are tight
  (`73/117-126`, `96/117-126`, `154/248-255`, ... `591/635`). The one function
  that mixes SPI and libxml, `xpath_table`, is explicit: it runs
  `SPI_connect`/`SPI_exec` first (`xpath.c:736-738`) and only THEN calls
  `pgxml_parser_init` (`xpath.c:762`), with the comment
  *"Setup the parser. This should happen after we are done evaluating the
  query, in case it calls functions that set up libxml differently."*
  `SPI_finish` is after `pg_xml_done` (`xpath.c:956-958`).
- `contrib/xml2/xslt_proc.c`: `xsltApplyStylesheetUser` (`143`) sits between
  init and `pg_xml_done` (`183/199`); libxslt transform is CPU-only here (no
  document loading -- inputs are already-parsed trees).

So the implicit invariant "no yield inside the init/done window" currently
holds everywhere. It is just not written down or enforced.

---

## 5. Fix options, ranked

The fix must cover BOTH core `xml` and `contrib/xml2`, because both go through
the same `pg_xml_init` -> `xmlSetStructuredErrorFunc` path. Any fix in
`pg_xml_init`/`pg_xml_done` automatically covers both; a per-call-site fix does
not and is rejected.

### (a) RECOMMENDED -- per-context error handlers, version-gated

Attach the handler to the parser/xpath context instead of the carrier-thread
global, so no global/TLS is ever written and there is nothing to save/restore.
This eliminates the hazard structurally rather than relying on the "no yield"
invariant, and it is migration-safe by construction.

Concrete shape (version-gated dual path, keyed on the same style as the
existing `HAVE_XMLSTRUCTUREDERRORCONTEXT` gate at `xml.c:67`):

```c
/* xml.c top, near line 74 */
#if LIBXML_VERSION >= 21300      /* xmlCtxtSetErrorHandler / xmlXPathSetErrorHandler */
#define HAVE_XML_PER_CTXT_ERRHANDLER 1
#endif
```

New helpers (thin, next to pg_xml_init):

```c
/* Call right after xmlNewParserCtxt()/xmlCtxtReadDoc setup */
void pg_xml_ctxt_seterror(PgXmlErrorContext *e, xmlParserCtxtPtr ctxt)
{
#ifdef HAVE_XML_PER_CTXT_ERRHANDLER
    xmlCtxtSetErrorHandler(ctxt, xml_errorHandler, e);
#endif
    /* else: rely on the global installed by pg_xml_init() (legacy path) */
}
void pg_xml_xpath_seterror(PgXmlErrorContext *e, xmlXPathContextPtr xp)
{
#ifdef HAVE_XML_PER_CTXT_ERRHANDLER
    xmlXPathSetErrorHandler(xp, xml_errorHandler, e);
#endif
}
```

Attach points already exist: `xmlNewParserCtxt` at `xml.c:1872,4465,4753`;
`xmlXPathNewContext` at `xml.c:4474,4811`; same in `contrib/xml2`. Paths that
use the context-less `xmlReadDoc`/`xmlReadMemory` (e.g. `xpath.c:524,793`)
must convert to `xmlNewParserCtxt()` + `xmlCtxtReadDoc()`/`xmlCtxtReadMemory()`
to have a context to attach to -- a mechanical change.

On the modern path (`>=2.13`), `pg_xml_init`/`pg_xml_done` still run but the
`xmlSetStructuredErrorFunc` global becomes a belt-and-suspenders fallback only
for libxml-internal call sites that don't route through our contexts; the
per-context handler wins for everything we drive. On the legacy path
(`<2.13`), behavior is exactly today's (global handler + the Section-4 no-yield
invariant, now documented and asserted -- see below).

Cost: one small `#if` block + a helper + a handful of call-site lines +
converting the two `xmlReadMemory` sites in xml2. Covers core and contrib in
one place.

### (b) FALLBACK for old libxml -- keep global, make the invariant explicit

For `LIBXML_VERSION < 21300` where option (a) is unavailable, keep the global
save/restore but stop relying on an unwritten invariant:

- Document in `pg_xml_init` that the caller MUST NOT yield (no SPI, no lock
  wait, no `WaitLatch`, no protocol I/O) before `pg_xml_done`.
- Add a cheap runtime guard: on the threaded runtime, mark the fiber
  non-yielding for the window (a "critical / no-wait" flag the wait-boundary
  seam already understands -- the plan's `xtc_proc_critical_enter/leave`
  family, `MULTITHREADED_PLAN.md:3564-3570`), so any accidental wait inside the
  window is caught (assert/error) rather than silently corrupting state. This
  is the lazy hardening: it does not serialize anything, it just enforces the
  invariant Section 4 shows already holds.

This is not a separate "option" so much as the correct legacy leg of (a): the
version gate picks (a) on new libxml, (b) on old.

### (c) NOT RECOMMENDED -- serialize all libxml under an LWLock

Wrap every `pg_xml_init`..`pg_xml_done` in a single process-wide LWLock so at
most one backend touches libxml globals at a time. Correct, trivially, but:

- It is a global serialization point on a data type people use heavily; kills
  concurrency for a hazard that (a) removes for free on modern libxml.
- Under cooperative fibers the lock must be fiber-aware/non-preemptible to be
  safe (`MULTITHREADED_PLAN.md:2301-2302`), adding complexity.
- It does nothing that (a)+(b) don't do better.

Keep (c) only as a described last resort if a target platform ships a libxml
`<2.13` AND we ever find a real yield in the window that we cannot remove.

### Recommendation

Ship **(a) with (b) as its version-gated fallback**. Rank: (a)+(b) >> (c).

---

## 6. What this unblocks / hardens

- **Unblocks `contrib/xml2` -> `POOLED_PROTOCOL_AFFINE`.** With (a), xml2's
  libxml error routing is per-context and migration-safe; with (b) the affine
  invariant is documented and guarded. Either way the marking rests on an
  enforced property, not an accident. (Note: xml2 also pulls in **libxslt**,
  which has its own error-handler globals -- `xsltSetGenericErrorFunc` etc.
  in `xslt_proc.c`. That is a sibling of this gap and should be audited on the
  same model before xml2 is finally marked; libxslt's per-transform-context
  error handler, if the linked version has one, is the analogous clean fix.
  Out of scope for this doc; flagged.)
- **Hardens the core `xml` type** in the same change, because the fix lives in
  the shared `pg_xml_init`/`pg_xml_done` + the shared attach helpers. No
  separate core work needed.

## 7. Concrete follow-ups (not done here)

1. Implement (a)+(b) in `xml.c` `pg_xml_init`/`pg_xml_done` + helpers; convert
   `xmlReadMemory`/`xmlReadDoc` call sites that lack a context.
2. Audit `contrib/xml2/xslt_proc.c` libxslt error-handler globals on the same
   model.
3. Add a threaded regression that runs concurrent XML parse/xpath across
   pooled carriers and asserts no `"libxml error handling state is out of
   sync"` WARNING (`xml.c:1356-1357`) and correct per-session error text.
