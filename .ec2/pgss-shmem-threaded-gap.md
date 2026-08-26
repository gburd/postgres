# pg_stat_statements view fails under multithreaded=on: shmem_startup state stranded in early_runtime_extension_modules

Status: root cause CONFIRMED (read-only investigation; no source modified).
Build: origin/xtc, branch `preload-custom-guc` @ 9793ba0024.

## Symptom (already measured)

With `shared_preload_libraries='pg_stat_statements'` + `compute_query_id=on`:
`SELECT * FROM pg_stat_statements` errors `pg_stat_statements must be loaded via
"shared_preload_libraries"` under `multithreaded=on`, but works under
`multithreaded=off` on the same build. GUCs (max/track/save) are all visible and
correct under mt=on (the just-landed per-session custom-GUC accessor fix). Only the
VIEW's shared-memory access fails: the SRF safety check
`if (!pgss || !pgss_hash ...)` (contrib/pg_stat_statements/pg_stat_statements.c:958,
also :866, :1407) sees `pgss == NULL`.

## Does shmem_startup run under mt=on? YES.

The init callback runs exactly once, in the postmaster, during startup — same as
process mode:

- postmaster.c:1097 `process_shared_preload_libraries()` -> pgss `_PG_init` ->
  `RegisterShmemCallbacks(&pgss_shmem_callbacks)` (pg_stat_statements.c:594). At this
  point `shmem_request_state != SRS_DONE`, so the callbacks are just appended to the
  `registered_shmem_callbacks` list (shmem.c:895). `registered_shmem_callbacks` and
  `shmem_request_state` are `PG_GLOBAL_RUNTIME` (shmem.c:157, :214) — one per OS
  process, shared by postmaster and all carrier threads.
- postmaster.c:1152 `ShmemCallRequestCallbacks()` -> `pgss_shmem_request`
  (pg_stat_statements.c:625): captures the target pointers
  `.ptr = (void **) &pgss` and `.ptr = &pgss_hash` for later fill-in.
- postmaster.c:1196 `CreateSharedMemoryAndSemaphores()` -> `ShmemInitRequested()` ->
  `foreach(... registered_shmem_callbacks) callbacks->init_fn(...)` (shmem.c:447-450)
  -> `pgss_shmem_init` (pg_stat_statements.c:648). This fills `pgss`/`pgss_hash`
  (`*(request->options->ptr) = index_entry->location`, shmem.c:557) and initializes
  the shared struct. `Assert(!IsUnderPostmaster)` holds because this is the postmaster
  itself, pre-fork/pre-carrier.

So init RUNS and writes real pointers. The bug is not "init didn't run" — it is
"init wrote into a runtime-state instance that threaded sessions never adopt."

## Which runtime-state does init WRITE vs sessions READ? THE BUG.

In this fork `pgss`/`pgss_hash` are macros (pg_stat_statements.c:388-389):
```
#define pgss      (pgss_runtime_state()->shared_state)
#define pgss_hash (pgss_runtime_state()->hash)
```
`pgss_runtime_state()` (pg_stat_statements.c:312) resolves via
`PgRuntimeEnsureExtensionPrivateState(PGSS_RUNTIME_STATE_KEY, ...)`
(backend_runtime_extension.c:122), which stores per **runtime**:
`PgCurrentRuntimeExtensionModuleState()` returns
`&CurrentPgRuntime->extension_modules` — or, when `CurrentPgRuntime == NULL`,
`&early_runtime_extension_modules` (backend_runtime_extension.c:85-91).

Key fact: **the postmaster never runs `BaseInit()`**, so it never runs
`InitializePgProcessRuntime()` and never installs a current runtime.
`CurrentPgRuntime` reads `PgRuntimeCurrentBridgeState.runtime`
(backend_runtime_current.h:224 -> :117), which is `{0}` (NULL) at process start and
is only set by `PgRuntimeSetCurrentWork()`. `BaseInit()` runs only in real process
children (postgres.c:7291, bootstrap.c:412, autovacuum.c, auxprocess.c, bgworker.c),
never in `PostmasterMain`.

Therefore, throughout postmaster shmem request+init, `CurrentPgRuntime == NULL`, and
pgss's `shared_state`/`hash` are stored in **`early_runtime_extension_modules`**
(a `PG_GLOBAL_RUNTIME` singleton, backend_runtime_extension.c:19), NOT in any real
runtime object.

The handoff that is supposed to move early state into a real runtime is
`PgRuntimeAdoptEarlyExtensionModuleState(runtime)`
(backend_runtime_extension.c:76): it does
`runtime->extension_modules = early_runtime_extension_modules;` then resets early to
empty. It is called from exactly ONE place: `InitializePgProcessRuntime()`
(backend_runtime.c:872) — i.e. only via `BaseInit()`, only in a process backend.

Under mt=on, threaded sessions run on `thread_runtime`
(backend_runtime.c:135). `thread_runtime.extension_modules` is set by a struct copy:
```
thread_runtime.extension_modules = process_runtime.extension_modules;   // backend_runtime.c:1108
```
in `InitializePgThreadRuntime()` (called in the postmaster at backend launch,
launch_backend.c:951 / :1171, after startup). But in the postmaster
`process_runtime` was only `MemSet`+`PgRuntimeInitializeRuntimeObject`'d (empty
`private_states`, NIL); it never adopted the early state (that adoption only happens
inside a process child's `BaseInit`). So the copy propagates an **empty**
extension-module list into `thread_runtime`.

Result: pgss's real `shared_state`/`hash` are stranded in
`early_runtime_extension_modules` and are adopted by NO threaded runtime. When a
threaded session evaluates the view, `pgss_runtime_state()` finds no entry on
`thread_runtime.extension_modules`, `PgRuntimeEnsureExtensionPrivateState`
`palloc0`s a fresh zeroed `PgStatStatementsRuntimeState`, and `shared_state == NULL`
-> the `!pgss` safety check trips -> the "must be loaded via shared_preload_libraries"
error.

Why the GUCs still work: track/track_utility/track_planning are **session**-scoped
(`PgSessionEnsureExtensionPrivateState`, backend_runtime_session.c:2525) and each
session seeds its own copy from `initialized=false`; the accessor-rebind fix binds
them to the live session cell. max/save are process-wide GUC storage. None of these
depend on the postmaster->thread_runtime extension-module handoff. Only the
runtime-scoped shmem pointers do, and that handoff is the missing link.

## Scope: pgss-specific exposure of a GENERAL extension-shmem-under-threading gap

The gap is general, but pgss is the only currently-exposed contrib case:

- Contrib/test modules using `RegisterShmemCallbacks`: pg_stat_statements,
  test_shmem, test_slru, test_aio, injection_points. Of these, ONLY pgss stashes its
  shmem pointer via `PgRuntimeEnsureExtensionPrivateState` (runtime-scoped). The
  others keep the pointer in a plain `static` process global (e.g. test_shmem.c:35
  `static TestShmemData *TestShmem;`). A plain static is shared across carrier
  threads because they share the postmaster's address space, so it "just works" under
  threading — the runtime-state indirection is what breaks pgss.
- pg_prewarm/autoprewarm does NOT use `RegisterShmemCallbacks`; it attaches via
  `GetNamedDSMSegment` per-backend (autoprewarm.c:965) and stores the pointer in
  **backend**-scoped state (`PgBackendEnsureExtensionPrivateState`,
  autoprewarm.c:152) which is rebuilt per backend — so it also sidesteps the gap.
- The general rule: any preloaded extension whose `RegisterShmemCallbacks` `init_fn`
  stashes a shmem pointer (or any postmaster-startup-time value) into
  **runtime-scoped** extension-private state (`PgRuntimeEnsureExtensionPrivateState`)
  will strand that value in `early_runtime_extension_modules` under mt=on, because the
  postmaster never adopts early runtime state into `process_runtime`/`thread_runtime`.

## Recommended fix

Two independently-correct options; (a) is the minimal, ship-now pgss change, (c) is
the core fix that makes the runtime-scoped extension-state pattern safe for every
future extension. They are not mutually exclusive.

### (a) pgss: keep the shmem pointers process-wide, not runtime-scoped  [SMALL, recommended to unblock AFFINE]

`shared_state` and `hash` are genuine process-wide singletons (one shmem area per
cluster; `pgss_shmem_init` asserts single-process init). They do NOT need per-runtime
scoping the way the session GUCs do. Move ONLY these two fields out of
`PgStatStatementsRuntimeState` back to plain process globals:

```
static PG_GLOBAL_SHMEM pgssSharedState *pgss = NULL;   /* set by shmem_init, .ptr target */
static PG_GLOBAL_SHMEM HTAB *pgss_hash = NULL;
```
and drop the `#define pgss (pgss_runtime_state()->shared_state)` / `pgss_hash`
indirection (pg_stat_statements.c:388-389). The `.ptr = (void**) &pgss` /
`&pgss_hash` request wiring (pg_stat_statements.c:632, :639) then targets the plain
globals, which carrier threads share directly (same address space) — exactly like
test_shmem. Everything else in the runtime-state struct (max/save/prev_* hooks) can
stay runtime-scoped or also move to plain statics; the prev_* hook pointers are also
process-wide (hooks are installed once at postmaster _PG_init), so they have the same
"safe as a plain static under threading" property. This is the smallest diff and
directly unblocks finishing the pgss AFFINE marker.

Ceiling / caveat: this relies on the mt-safe invariant that these are set once at
postmaster startup and never mutated per-session. That invariant already holds for
pgss (shmem_init runs once, hooks installed once). If a future field needs per-session
scoping it stays in the runtime/session struct.

### (b) run shmem_startup per-runtime-that-sessions-share  [WRONG shape here]

Not applicable/undesirable: the shmem area is a single cluster-wide segment created
once; re-running `init_fn` per runtime would try to re-init/double-allocate it
(`pgss_shmem_init` asserts `!IsUnderPostmaster` and single init). The pointer just
needs to reach the threaded runtime, not be re-created.

### (c) core: adopt early extension-module state into the threaded runtime  [CORRECT general fix]

Make the postmaster->thread_runtime handoff carry the early extension-module state, so
the runtime-scoped-extension-state pattern is safe for any extension. Concretely, in
`InitializePgThreadRuntime()` (backend_runtime.c:~1108), instead of copying the
(empty) `process_runtime.extension_modules`, adopt the early state the same way a
process backend does:

```
/* first-time thread_runtime init */
PgRuntimeAdoptEarlyExtensionModuleState(&thread_runtime);   /* moves early -> thread_runtime, resets early */
PgRuntimeEnsureExtensionModuleMemoryContext(&thread_runtime.extension_modules);
```
replacing `thread_runtime.extension_modules = process_runtime.extension_modules;`.
This transfers the pgss (and any other preload extension's) runtime-scoped shmem
pointers into the runtime that threaded sessions actually run on. Guard for the
already-adopted case: `PgRuntimeAdoptEarlyExtensionModuleState` resets `early` after
adopting, so it must run at most once and before any process backend also tries to
adopt. Under mt=on the postmaster is the sole adopter (process children are the
fallback path); confirm no process-fallback backend in the same postmaster expects to
re-adopt the same early state. If both `process_runtime` (fallback) and
`thread_runtime` must see it, the adoption should COPY early into both rather than
move-and-reset (i.e. change adopt semantics, or copy the list before reset).

Recommendation: land (a) now to finish the pgss AFFINE marker (smallest, matches how
every other in-tree shmem module already behaves under threading), and track (c) as
the core hardening so the next extension that stashes a shmem pointer in
runtime-scoped private state does not silently hit the same NULL under mt=on. Add a
regression: with `shared_preload_libraries='pg_stat_statements'`, `SELECT count(*)
FROM pg_stat_statements` must succeed under both `multithreaded=off` and
`multithreaded=on`.

## File:line index

- Symptom check: contrib/pg_stat_statements/pg_stat_statements.c:958 (also :866, :1407)
- pgss macros: pg_stat_statements.c:388-389 (`pgss`, `pgss_hash`)
- pgss runtime-state resolver: pg_stat_statements.c:312 `pgss_runtime_state()`
- pgss shmem callbacks: pg_stat_statements.c:263-266, request :625, init :648
- RegisterShmemCallbacks (deferred append): shmem.c:879, :895
- init_fn invocation (postmaster): shmem.c:447-450 via `ShmemInitRequested()` (:424);
  `.ptr` fill-in shmem.c:559
- postmaster order: postmaster.c:1097 preload, :1154 request cbs, :1196 create shmem
- `registered_shmem_callbacks`/`shmem_request_state` PG_GLOBAL_RUNTIME: shmem.c:157, :214
- extension-private resolver: backend_runtime_extension.c:122
  `PgRuntimeEnsureExtensionPrivateState`; NULL->early at :85-91
- early singleton: backend_runtime_extension.c:19 `early_runtime_extension_modules`
- adopt-early (only caller): backend_runtime_extension.c:76;
  called from backend_runtime.c:872 `InitializePgProcessRuntime` (via BaseInit only)
- thread_runtime extension_modules copy: backend_runtime.c:1108 in
  `InitializePgThreadRuntime`; launched from launch_backend.c:951, :1171
- CurrentPgRuntime: backend_runtime_current.h:224 (NULL until PgRuntimeSetCurrentWork)
- BaseInit not called in postmaster (only process children):
  postgres.c:7291, bootstrap.c:412, autovacuum.c:477/:1530, auxprocess.c:75, bgworker.c:888
- Contrib RegisterShmemCallbacks users: pg_stat_statements (runtime-scoped, BROKEN),
  test_shmem (static global, OK: test_shmem.c:35), test_slru, test_aio,
  injection_points (statics, OK); pg_prewarm uses GetNamedDSMSegment + backend-scoped
  (OK: autoprewarm.c:965, :152)
