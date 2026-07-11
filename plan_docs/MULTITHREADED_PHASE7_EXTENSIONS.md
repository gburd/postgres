# Phase 7: Extension Backend Model Gate

Phase 7 introduces a loader-level backend-model gate for C extension modules.
The gate keeps existing process-mode behavior working while making threaded
runtime compatibility an explicit opt-in.

## Implemented Contract

`Pg_magic_struct` now carries `backend_model` metadata.  The model values are
ordered from least to most reentrant:

- `PG_BACKEND_MODEL_PROCESS`: process-backend-only.  This is the default for
  `PG_MODULE_MAGIC` and for `PG_MODULE_MAGIC_EXT` callers that do not set a
  backend model.
- `PG_BACKEND_MODEL_THREAD_PER_SESSION`: may run with one logical backend per
  OS thread in one shared address space.
- `PG_BACKEND_MODEL_POOLED_SCHEDULER`: may run when logical sessions/executions
  can be scheduled onto a carrier pool.

The order is cumulative.  A pooled-scheduler-safe module may load in process or
thread-per-session mode, but a process-only module may not load in threaded
mode.

Extension authors can opt in with field-initializer macros inside
`PG_MODULE_MAGIC_EXT`:

```c
PG_MODULE_MAGIC_EXT(
					.name = "my_module",
					.version = PG_VERSION,
					PG_MODULE_MAGIC_BACKEND_MODEL_THREAD_PER_SESSION
);
```

The process-only default is deliberate.  Existing third-party extensions must
not become thread-compatible merely because they are rebuilt with a new header.

## Loader Behavior

`dfmgr.c` now performs backend-model validation after the normal magic-block ABI
check and before `_PG_init()` is called.  Rejection happens before any module
initialization hooks can mutate backend state.

Already-loaded libraries are rechecked against the active backend model on each
later `LOAD` or symbol lookup through a `DynamicFileList` handle.  Runtime
model changes also validate every already-loaded library before updating the
active model, so a process-only module loaded earlier prevents a later switch
to `thread-per-session` or `pooled-scheduler`.  This matters for Phase 7 tests
and for future runtimes that may switch from process mode to a stricter model
before loading more code.

If the runtime has not been initialized, the loader treats the active model as
`PG_BACKEND_MODEL_PROCESS`.  This preserves early process-mode behavior.

## Runtime Policy

`PgRuntime` carries the active extension backend model.  Process mode initializes
it to `PG_BACKEND_MODEL_PROCESS`.

Phase 7 adds internal getters and setters so tests can exercise the loader
policy before a threaded backend launcher exists:

- `PgRuntimeGetExtensionBackendModel()`
- `PgRuntimeSetExtensionBackendModel()`

These are not a user-facing compatibility switch.  Later threaded runtime code
should set the model according to the active carrier/session scheduler.  Callers
must set the stricter model before loading process-only modules, because the
setter rejects incompatible modules that are already present in the backend.

## Test Modules

`src/test/modules/test_extensions` now includes focused policy modules:

- `test_ext`: existing process-only module, used to prove default rejection in
  stricter models.
- `test_ext_threaded`: thread-per-session marker.
- `test_ext_backend_model`: pooled-scheduler marker plus C helpers for reading
  and setting the test runtime model and catching loader errors.
- `test_ext_bad_backend_model`: intentionally invalid metadata, used to prove
  the loader rejects malformed backend-model declarations independently of the
  active model.
- `test_ext_short_magic`: intentionally old-layout magic metadata, used to
  prove the loader rejects stale magic blocks without reading the new
  `backend_model` field past the module's static object.

The regression test verifies:

- process mode still loads process-only modules;
- thread-per-session mode loads thread-per-session and pooled-safe modules;
- thread-per-session mode rejects default process-only modules;
- pooled-scheduler mode rejects thread-per-session-only and process-only
  modules in a separate regression backend;
- invalid backend-model metadata is rejected in stricter models;
- invalid backend-model metadata is rejected even in process mode;
- old-layout magic metadata is rejected as a magic-block mismatch before the
  backend-model gate reads the new metadata field;
- active model changes are rejected when already-loaded modules are incompatible
  with the requested model;
- already-loaded modules are still rechecked on later load paths and
  handle-based symbol lookup;
- PL/pgSQL loads in thread-per-session mode after the Phase 10
  thread-local bridge migration, while default process-only modules remain
  rejected.

## PL/pgSQL Audit Result

PL/pgSQL was not marked thread-per-session safe in Phase 7.

The current implementation has mutable module-scope state that is described in
comments as session-wide but is process-wide in a multithreaded address space:

- `shared_simple_eval_estate`, `simple_econtext_stack`, and
  `shared_simple_eval_resowner` in `pl_exec.c`;
- `cast_expr_hash` and `shared_cast_hash` in `pl_exec.c`;
- compiler globals such as `plpgsql_curr_compile`,
  `plpgsql_compile_tmp_cxt`, `plpgsql_Datums`, `plpgsql_nDatums`,
  `datums_alloc`, and `datums_last` in `pl_comp.c`;
- namespace parser state `ns_top` in `pl_funcs.c`;
- the `PLpgSQL_plugin` rendezvous pointer, which can point at third-party
  plugin code and therefore needs its own compatibility policy.

The migration route is:

- add a PL/pgSQL-owned session state object reachable from `PgSession`;
- move simple-expression EState/resowner/econtext stack and shared cast caches
  into that session state;
- move compilation scratch state into an explicit compilation context object
  rather than module globals;
- make namespace parser state part of the compilation context;
- define backend-model metadata for PL/pgSQL plugins before allowing plugins in
  threaded mode;
- run PL/pgSQL process-mode regression and the future thread-per-session runtime
  gate before marking PL/pgSQL with
  `PG_MODULE_MAGIC_BACKEND_MODEL_THREAD_PER_SESSION`.

Phase 10 implemented the first thread-per-session bridge for this route:
the mutable PL/pgSQL state above is thread-local, PL/pgSQL performs
per-backend-thread session initialization even after the dynamic library is
already loaded process-wide, and the module now declares
`PG_MODULE_MAGIC_BACKEND_MODEL_THREAD_PER_SESSION`.  This is sufficient for
the thread-per-session runtime target.  A future pooled scheduler still needs
the explicit `PgSession`-owned state objects described above rather than
thread-local storage.

## In-Tree Allowlist Route

The Phase 7 allowlist is intentionally small: only the dedicated policy test
modules opt into threaded models.

The first real threaded-regression allowlist should be created when the
thread-per-session runtime exists and should include only audited modules:

- PL/pgSQL, now enabled for the thread-per-session runtime by Phase 10;
- required encoding conversion modules under
  `src/backend/utils/mb/conversion_procs`, if selected tests load them;
- regression helper modules such as `src/test/regress/regress.c` and selected
  `src/test/modules` helpers;
- any automatically loaded module required by the selected threaded test set.

Contrib-wide opt-in is deliberately later work.  Phase 16 remains the completion
point for making every contrib extension support threaded mode.

## Validation

The Phase 7 implementation has been validated with:

- `gmake -C src/backend/utils/fmgr dfmgr.o`
- `gmake -C src/backend/utils/init backend_runtime.o`
- `gmake -C src/backend all`
- `gmake all`
- `gmake -C src/backend/snowball clean all`
- `gmake -C src/pl/plpgsql/src all`
- `gmake -C src/test/modules/test_extensions clean all`
- equivalent `pg_regress` run for `test_extensions`, `test_extdepend`,
  `test_ext_backend_model`, and `test_ext_backend_model_pooled` after the
  local macOS temp-install `initdb` install-name workaround;
- `gmake -C src/pl/plpgsql check`
- clean-worktree Meson configure with optional features disabled:
  `/tmp/pg-phase7-meson-venv/bin/meson setup /tmp/pg-phase7-build-current /tmp/pg-phase7-meson-src-current --auto-features=disabled -Dssl=none -Dtap_tests=disabled`
- targeted Meson build of `test_ext.dylib`, `test_ext_backend_model.dylib`,
  `test_ext_threaded.dylib`, `test_ext_bad_backend_model.dylib`, and
  `test_ext_short_magic.dylib`

The forced clean rebuilds are important after changing `Pg_magic_struct`; stale
loadable modules built against the old magic-block size fail the normal ABI
check.
