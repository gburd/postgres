# Multithreaded PostgreSQL Agent Guide

This repository is an experimental branch for making PostgreSQL capable of
running backend sessions in a multithreaded runtime. The branch is allowed to
be ambitious and is not currently optimized for upstream patch shape.

Implementation is now underway. Keep the plan and architecture notes current as
the code evolves.

## Project Docs

- [MULTITHREADED_ARCHITECTURE.md](MULTITHREADED_ARCHITECTURE.md) describes the
  desired end-state architecture.
- [MULTITHREADED_PLAN.md](MULTITHREADED_PLAN.md) describes the staged
  implementation plan, validation strategy, and risk register.
- [MULTITHREADED_PHASE5_INTERRUPTS.md](MULTITHREADED_PHASE5_INTERRUPTS.md)
  records the logical interrupt boundary and recovery-conflict fixture
  decision.
- [MULTITHREADED_PHASE6_EXIT.md](MULTITHREADED_PHASE6_EXIT.md) records the
  current backend lifecycle/exit boundary and deferred thread-runtime proof.
- [MULTITHREADED_PHASE7_EXTENSIONS.md](MULTITHREADED_PHASE7_EXTENSIONS.md)
  records the extension backend-model gate and PL/pgSQL audit result.
- [MULTITHREADED_PHASE8_THREAD_SAFETY.md](MULTITHREADED_PHASE8_THREAD_SAFETY.md)
  records the first thread-local bridge for backend-local state and the
  remaining Phase 8 thread-safety floor.
- [MULTITHREADED_PHASE9_WAIT_BOUNDARY.md](MULTITHREADED_PHASE9_WAIT_BOUNDARY.md)
  records the current logical wait/suspend boundary work.
- [MULTITHREADED_PHASE10_THREAD_RUNTIME.md](MULTITHREADED_PHASE10_THREAD_RUNTIME.md)
  records the thread-per-session runtime work.
- [MULTITHREADED_PHASE11_WORKERS.md](MULTITHREADED_PHASE11_WORKERS.md)
  records the auxiliary worker thread-runtime work, starting with autovacuum
  workers.
- [MULTITHREADED_PHASE12_STATE.md](MULTITHREADED_PHASE12_STATE.md) records the
  state-migration bridge work that starts moving TLS/global state toward
  explicit runtime/session objects.
- [MULTITHREADED_RUNTIME_LIFECYCLE.tsv](MULTITHREADED_RUNTIME_LIFECYCLE.tsv)
  is the Gate E2 lifecycle manifest for runtime-root fields, currently
  including `PgCarrier`, `PgBackend`, `PgSession`, `PgConnection`, and
  `PgExecution`.
- [MULTITHREADED_RUNTIME_OWNERS.tsv](MULTITHREADED_RUNTIME_OWNERS.tsv) maps
  migrated legacy symbols to runtime object buckets, members, accessors, and
  owner source files. Extend it when moving globals behind runtime objects;
  `gmake check-runtime-lifecycles` validates that each owner-map bucket points
  at a checked lifecycle row, each owner source exists, and each accessor is
  present in the owner source or runtime header.
- [MULTITHREADED_THREADING_REVIEW.md](MULTITHREADED_THREADING_REVIEW.md)
  records the critical branch review findings and the Phase 12 exit-gate
  rationale.
- [refs/REFERENCES.md](refs/REFERENCES.md) lists external references.
- [refs/pgconf-2025-multithreading-transcript.md](refs/pgconf-2025-multithreading-transcript.md)
  is the local transcript of the PgConf.dev 2025 talk that motivates this work.

## Working Assumptions

- Use Heikki Linnakangas's multithreading branch as reference material, not as
  a base to merge wholesale.
- Preserve multiprocess PostgreSQL as a supported backend model.
- The first native threading target should be thread-per-session. The longer
  term target is an explicit scheduler that can map sessions/executions to a
  pool of carriers.
- Thread-per-session for regular client backends is not the final normal-mode
  target. Normal threaded server mode should eventually run in-tree
  server-owned workers, including autovacuum and auxiliary worker families, as
  threaded runtime-owned workers rather than forked subprocesses.
- Single-user mode, bootstrap mode, frontend command-line utilities,
  postmaster/control-plane process lifetime, and crash-escalation paths are
  deliberate process-lifetime exceptions.
- Do not overfit the design to WASM. Keep the main-loop and wait-boundary
  abstractions clean enough that a future host-driven runtime can use them.
- Existing third-party C extensions may be process-backend-only. That is an
  acceptable compatibility break for threaded mode.
- Existing third-party background workers may remain process-only or be
  rejected in threaded mode unless explicit worker-runtime metadata opts them
  in.
- In-tree modules and important bundled languages, especially PL/pgSQL, should
  have a plausible path to work in threaded mode.

## Source Orientation

Important current files:

- `src/backend/tcop/postgres.c`: `PostgresMain()`, the top-level backend loop,
  error recovery, command read, command dispatch, and `ProcessInterrupts()`.
- `src/include/access/session.h` and `src/backend/access/common/session.c`:
  existing `Session` abstraction for session-scoped DSM/DSA state. Treat this
  as a seed for the broader session object unless there is a strong reason not
  to.
- `src/backend/utils/cache/backend_runtime_cache.c`: fork-owned runtime bridge
  accessors for session-owned cache roots. Add future catalog/cache accessor
  shims here rather than growing `backend_runtime.c`.
- `src/backend/utils/init/backend_runtime_session.c`: fork-owned runtime
  bridge accessors for broad session-owned compatibility state that does not
  yet have a narrower owner file, including namespace, locale, database,
  tablespace, binary-upgrade, text-search, tcop, extension, invalidation, RI,
  relmap, prepared-statement, on-commit, and sequence shims. Keep
  fallback-aware current-bucket selectors in `backend_runtime.c` and expose
  them only through `backend_runtime_internal.h`.
- `src/backend/utils/activity/backend_runtime_pgstat.c`: fork-owned runtime
  bridge accessors for pgstat-owned backend/session state. Add future pgstat
  accessor shims here rather than growing `backend_runtime.c`.
- `src/backend/utils/misc/backend_runtime_guc.c`: fork-owned runtime bridge
  accessors for GUC compatibility state that lives in session/backend/runtime
  buckets, including server/runtime GUCs, connection GUCs, core GUC registry
  pointers/lists, miscellaneous GUCs, threaded GUC mutex depth, and GUC
  error-reporting state. Add future GUC backing-variable shims here rather
  than growing `backend_runtime.c` or `guc_tables.c`; keep only
  fallback-aware current-bucket selectors in `backend_runtime.c`.
- `src/backend/utils/misc/backend_runtime_utility.c`: fork-owned runtime
  bridge accessors for backend-local utility, formatting, sampling, superuser,
  and resource-owner callback state. Add small utility compatibility shims here
  rather than growing `backend_runtime.c`.
- `src/backend/access/transam/backend_runtime_parallel.c`: fork-owned runtime
  bridge accessors for backend-local parallel-query state. Add parallel-query
  compatibility shims here rather than growing `backend_runtime.c`.
- `src/backend/jit/backend_runtime_jit.c`: fork-owned runtime bridge accessors
  for provider-independent and LLVM-provider JIT session state. Keep
  LLVM-provider-private semantic lifecycle work under `src/backend/jit/llvm`
  when that state needs provider-specific cleanup.
- `src/backend/libpq/backend_runtime_connection.c`: fork-owned runtime bridge
  accessors for frontend/backend connection state. Add backend libpq,
  protocol, startup, and client-connection compatibility accessors here rather
  than growing `backend_runtime.c`.
- `src/backend/storage/buffer/backend_runtime_buffer.c`: fork-owned runtime
  bridge accessors for backend-local buffer state. Add buffer-manager
  compatibility accessors here rather than growing `backend_runtime.c`.
- `src/backend/storage/file/backend_runtime_file.c`: fork-owned runtime bridge
  accessors for backend-local fd/storage state. Add fd, smgr, and pending-fsync
  compatibility accessors here rather than growing `backend_runtime.c`.
- `src/backend/storage/lmgr/backend_runtime_lmgr.c`: fork-owned runtime bridge
  accessors for backend-local lock-manager state. Add lock, LWLock, predicate,
  and deadlock-detector compatibility accessors here rather than growing
  `backend_runtime.c`.
- `src/backend/storage/ipc/backend_runtime_ipc.c`: fork-owned runtime bridge
  accessors for backend-local IPC, sinval, DSM, and latch state. Add IPC
  compatibility accessors here rather than growing `backend_runtime.c`.
- `src/backend/utils/init/backend_runtime_internal.h`: backend-private runtime
  declarations shared by fork-owned runtime support files. Do not expose these
  helpers in installed headers unless an upstream-owned caller truly needs
  them.
- `src/test/modules/test_backend_runtime/test_backend_runtime.h`: shared
  declarations for the backend runtime test extension.
- `src/test/modules/test_backend_runtime/test_backend_runtime_backend.c`:
  broad backend bucket tests that have not yet earned a narrower owner file.
- `src/test/modules/test_backend_runtime/test_backend_runtime_backend_core.c`:
  core backend identity, command/log, expression-interpreter, and latch
  interrupt tests.
- `src/test/modules/test_backend_runtime/test_backend_runtime_backend_interrupt.c`:
  backend interrupt-holdoff, pending-interrupt, and exit-state tests.
- `src/test/modules/test_backend_runtime/test_backend_runtime_pmchild.c`:
  PMChild thread-backend signal and publication-race tests.
- `src/test/modules/test_backend_runtime/test_backend_runtime_session.c`:
  core session, cache, identity, and session-reset test functions.
- `src/test/modules/test_backend_runtime/test_backend_runtime_session_guc.c`:
  runtime/server GUC, session GUC, and generated GUC rebind test functions.
- `src/test/modules/test_backend_runtime/test_backend_runtime_connection.c`:
  connection, socket, protocol, startup, and security test functions.
- `src/test/modules/test_backend_runtime/test_backend_runtime_execution.c`:
  execution, transaction, snapshot, resource-owner, and query-work test
  functions.
- `src/test/modules/test_backend_runtime/test_backend_runtime.c`: keep this as
  the small module entry point and native-thread/runtime smoke holder; add new
  object-family tests to the split files above.
- `src/include/miscadmin.h`: widely visible process/session globals and the
  interrupt macros.
- `src/backend/storage/ipc/procsignal.c`: process-signal-style backend
  communication.
- `src/backend/storage/ipc/latch.c` and `src/backend/storage/ipc/waiteventset.c`:
  wait/wakeup infrastructure.
- `PgBackendRecoveryState` in `src/include/utils/backend_runtime.h` now owns
  backend-local recovery/startup/standby state bridged from `startup.c`,
  `standby.c`, and `xlogrecovery.c`. Use
  `PG_BACKEND_STANDBY_INITIAL_WAIT_US` for the standby conflict-wait default
  so early fallback and initialized thread backends stay aligned.
- `PgBackendMaintenanceWorkerState` in `src/include/utils/backend_runtime.h`
  now owns backend-local state bridged from archiver, checkpointer, bgwriter,
  WAL summarizer, and data-checksum workers. Avoid object-like compatibility
  macros named `arch_files` or `operation`: those collide with struct members
  in `pgarch.c` and `datachecksum_state.c`.
- `PgBackendRepackState` in `src/include/utils/backend_runtime.h` now owns
  backend-local repack leader/worker state bridged from
  `src/backend/commands/repack.c`, `src/backend/commands/repack_worker.c`,
  and `src/include/commands/repack.h`. Keep `DecodingWorker` private to
  `repack.c`; the runtime header should only forward-declare its struct tag.
- `PgBackendAioState` in `src/include/utils/backend_runtime.h` now owns
  backend-local AIO state bridged from `src/include/storage/aio_internal.h`,
  `src/backend/storage/aio/aio.c`,
  `src/backend/storage/aio/method_worker.c`, and
  `src/backend/storage/aio/method_io_uring.c`. Keep `PgAioUringContext`
  private to `method_io_uring.c`; the runtime header should only
  forward-declare its struct tag.
- `src/backend/postmaster/launch_backend.c` and
  `src/backend/postmaster/postmaster.c`: backend launch and supervision.
- `src/backend/postmaster/autovacuum.c`,
  `src/backend/postmaster/auxprocess.c`,
  `src/backend/postmaster/bgworker.c`, and the individual auxiliary worker
  files under `src/backend/postmaster/`: worker launch, supervision, and
  server-owned worker lifecycles.
  `PgBackendAutovacuumState` owns autovacuum launcher/worker backend-local
  state bridged from `autovacuum.c`; keep private `avl_dbase` and
  `WorkerInfoData` pointers typed through forward-declared struct tags.
- `src/backend/replication/walreceiver.c`,
  `src/backend/replication/logical/launcher.c`,
  `src/backend/replication/logical/worker.c`, and
  `src/backend/storage/aio/method_worker.c`: replication and AIO worker
  lifecycles that must eventually use the threaded worker runtime.
- `src/include/fmgr.h` and `src/backend/utils/fmgr/dfmgr.c`: extension module
  ABI checks.
- `src/pl/plpgsql`: PL/pgSQL implementation.

## Development Rules For This Branch

- Keep documentation and code commits coherent. Prefer one conceptual change
  per commit.
- After each commit, push the current branch immediately unless the user has
  explicitly asked not to push.
- Before editing core code, read the surrounding implementation and current
  comments. PostgreSQL has many invariants that are documented only locally.
- Keep process-mode behavior working after each implementation phase.
- Use static annotations and tools to classify globals before moving large
  amounts of state.
- For Phase 12 state migration, prefer larger coherent batches when the state
  has the same owner and validation surface. Avoid one-variable commits unless
  the variable sits on a fragile lifecycle path where a narrow proof is needed.
- Keep `src/backend/utils/init/backend_runtime.c` focused on root runtime
  construction, current-pointer installation, process/thread symmetry, and
  top-level adoption/reset orchestration. New domain-specific accessors and
  simple lifecycle helpers should live in fork-owned adjacent subsystem files,
  with `check-runtime-lifecycles` taught to scan those files.
- Before continuing with additional Gate E2 state migration or starting Phase
  13 scheduler-aware wait work, complete the documented maintainability
  refactor: split owner-specific runtime bridge code out of
  `backend_runtime.c` where practical, make every manifest-referenced split
  source part of the default lifecycle checker input, and split
  `src/test/modules/test_backend_runtime` into smaller
  object/lifetime-focused test sources while preserving the same extension,
  SQL, expected output, and TAP surface.
- The first Gate E2 maintainability split is in place. Keep adding
  owner-specific runtime accessors to adjacent `backend_runtime_*.c` files and
  backend-runtime tests to the object-family test files instead of rebuilding
  the old monoliths.
- Before pushing deeper into Gate E2 teardown, add a small lifecycle
  framework: root-object bucket definition files, X-macros, or an equivalent
  checked manifest for `PgBackend`, `PgSession`, `PgConnection`, and
  `PgExecution`. Use it as the single source of truth for constructor,
  early-adoption, and reset/destroy call lists. Keep semantic cleanup
  functions handwritten and owner-adjacent; generate only repetitive coverage
  and call-list mechanics. Extend `check_runtime_lifecycles.pl` to validate
  the bucket definitions against `MULTITHREADED_RUNTIME_LIFECYCLE.tsv` and to
  reject unintentional process/thread lifecycle asymmetry.
- Treat that lifecycle framework as the next Gate E2 implementation slice, not
  optional polish. Prefer checked `.def` bucket files included from the
  top-level runtime constructors/adoption/reset orchestration before adding
  more handwritten init/adopt/reset lists.
- Make the lifecycle framework reduce manual work. Add small macros,
  templates, or declarative rule columns for routine copied-scalar,
  zero-reset, whole-bucket copy/adopt, and destructor-call cases, so future
  agents can move larger batches without maintaining several call lists by
  hand. Keep exceptional ordering and semantic cleanup handwritten near the
  owning subsystem.
- Before each large Phase 12 migration or Gate E2 teardown batch, do a
  lifecycle-ergonomics preflight and record the result in
  `MULTITHREADED_PHASE12_STATE.md`. If the batch would add repetitive
  init/adopt/reset/destroy boilerplate, first add the small checked lifecycle
  action, `PG_RUNTIME_DEFINE_*` helper, bucket `.def` rule, or checker
  validation that makes the batch declarative. If the existing checked
  mechanism is sufficient, say which bucket rows/macros/checker rules are
  being reused before editing code.
- The first lifecycle framework slice uses
  `src/backend/utils/init/backend_runtime_*_buckets.def`. The checker validates
  one bucket-definition row for every `PgCarrier`, `PgBackend`, `PgSession`,
  `PgConnection`, and `PgExecution` field. Carrier, backend, connection, and
  execution constructor orchestration includes those rows directly. Backend,
  connection, and execution closed-reset orchestration also includes the rows;
  carrier has no closed-backend reset path yet because carrier lifetime is
  outside closed logical backend reset. Session constructor/adoption includes
  the rows. Session closed reset uses the separate ordered
  `backend_runtime_session_reset_buckets.def` because its teardown order is
  intentionally different from early-adoption order; keep semantic cleanup in
  handwritten helper functions and add ordered reset rows for new non-noop
  session reset buckets.
- For routine lifecycle helper functions in `backend_runtime.c`, use the
  local `PG_RUNTIME_DEFINE_*` helper macros where they fit. The lifecycle
  checker recognizes those macro-defined functions, so use them for ordinary
  zero-init, copy/adopt plus reinit, and copy/adopt plus zero-reset helpers.
  Do not hide exceptional destructor ordering or ownership semantics behind
  these macros.
- When a Phase 12 migration starts adding repeated lifecycle boilerplate,
  improve the checked lifecycle framework before continuing. Prefer adding a
  helper macro, `.def` bucket row, or declarative lifecycle rule over
  maintaining another handwritten constructor/adoption/reset list. This should
  make larger coherent migrations easier while keeping ownership and teardown
  semantics explicit.
- If lifecycle bookkeeping is slowing progress, treat that friction as a
  design signal. Batch related buckets by root object or subsystem, add the
  missing helper macro/table rule/checker validation first, and then migrate the
  batch through that mechanism instead of making several narrow one-off edits.
- Before taking another large Phase 12 migration batch, explicitly consider
  whether the lifecycle work can be simplified first. If the batch would add
  repetitive init/adopt/reset/destroy code, add or extend checked helper macros,
  `.def` rows, or declarative lifecycle rules before moving the state. The goal
  is faster large-batch migration with the same manifest-checked discipline,
  not more manual bookkeeping.
- Treat this as a required lifecycle-ergonomics checkpoint, not a preference:
  before coding a boilerplate-heavy Phase 12 batch, decide whether the existing
  `PG_RUNTIME_DEFINE_*` macros, bucket `.def` files, and checker rules are
  enough. If not, extend that framework first and record the chosen pattern in
  `MULTITHREADED_PHASE12_STATE.md` so future agents follow the same path.
- Keep improving lifecycle ergonomics when the pattern becomes repetitive. Good
  candidates are checked action names in the bucket `.def` files for common
  cases such as zero-init, zero-reset, copy/adopt, copy/adopt-with-reinit,
  reset-through-initializer, and memory-context/list/hash destruction; small
  `PG_RUNTIME_DEFINE_*` wrappers for those actions; and checker rules that
  reject unclassified `(void) 0` lifecycle cells on buckets with pointers,
  lists, memory contexts, or owned resources. Add these framework improvements
  before migrating another large batch that would otherwise duplicate the same
  helper code by hand.
- Use this concrete preflight before a large Phase 12 migration:
  identify the target root object and bucket rows, list the repeated lifecycle
  operations the batch would need, decide whether the existing macros/`.def`
  rows/checker rules cover them, add a reusable checked helper first if they do
  not, then migrate the batch through that mechanism. Record the decision in
  the Phase 12 state log even when the existing framework is sufficient.
- This lifecycle preflight is mandatory before the next code batch that moves
  object-owned globals or adds reset/destroy behavior. The preflight note must
  say one of: "existing lifecycle mechanism is sufficient" with the specific
  bucket rows/macros named, or "framework extended first" with the new macro,
  `.def` rule, or checker validation named. Do not start by writing another
  handwritten helper list if a small checked macro/table rule would cover the
  repeated pattern.
- When adding another runtime root object or moving more fields into an
  existing root, extend the checked lifecycle framework first if the existing
  macros and `.def` rows do not make the lifecycle obvious. The default should
  be one manifest row plus one checked bucket-definition row per field, with
  helper macros covering routine init/adopt/reset cases and handwritten code
  reserved for real ownership semantics.
- The next time a Phase 12 batch would add several similar lifecycle helpers,
  implement a checked lifecycle action vocabulary before moving the state. The
  intended direction is a small set of named actions in the bucket `.def` rows
  for zero-init, zero-reset, scalar copy/adopt, whole-bucket copy/adopt,
  copy/adopt-with-reinit, reset-through-initializer, and explicit
  owner-adjacent destroy calls. Teach `check_runtime_lifecycles.pl` to verify
  those actions against `MULTITHREADED_RUNTIME_LIFECYCLE.tsv` and reject
  unexplained no-op lifecycle cells for buckets that own pointers, lists,
  memory contexts, sockets, or other close-time resources.
- The first checked lifecycle action is `PG_RUNTIME_NOOP`. Use it in
  `backend_runtime_*_buckets.def` instead of bare `(void) 0`; the lifecycle
  checker rejects anonymous no-op cells and unknown `PG_RUNTIME_*` action
  names. Extend this vocabulary before adding another family of repetitive
  lifecycle helper bodies.
- The next lifecycle-framework simplification should cover the patterns now
  recurring in Gate E2: object-owned allocation contexts, delete-and-null
  memory-context teardown, free/reset list heads, clear-pointer-slot reset,
  copy/adopt-then-reset-fallback, and reset-through-initializer. If a planned
  Phase 12 batch needs two or more parallel helper bodies for these patterns,
  add the checked `PG_RUNTIME_*` action, `PG_RUNTIME_DEFINE_*` helper, bucket
  `.def` rule, and checker validation before moving the state.
- Treat the object-owned allocation-context pattern as the first concrete
  lifecycle-ergonomics target. The next time a batch repeats create-on-demand
  context accessors plus delete-and-null reset helpers, add a reusable checked
  primitive for that pattern before moving more state through one-off helpers.
- If the lifecycle process itself feels slow or repetitive, stop and improve
  the checked lifecycle vocabulary before continuing the migration. The
  preferred fix is a small named action, helper macro, table row, or checker
  rule that makes the next batch easier and keeps the manifest as the source
  of truth; do not paper over the friction with another manual helper list.
- Current Phase 12 standing instruction: before the next boilerplate-heavy
  migration batch, explicitly decide whether lifecycle helper macros,
  checked action names, or declarative bucket rules would make the batch
  simpler. If yes, land that lifecycle-framework improvement first, then move
  the globals through the checked path.
- Treat lifecycle-helper repetition as implementation work, not documentation
  debt. If a Phase 12/Gate E2 slice would add two or more similar
  init/adopt/reset/destroy helpers, first add or extend the checked
  lifecycle mechanism: a `PG_RUNTIME_DEFINE_*` helper, named `PG_RUNTIME_*`
  bucket action, `.def` row pattern, or `check_runtime_lifecycles.pl`
  validation. Only continue with handwritten helpers when the cleanup has
  real ordering or ownership semantics that need owner-adjacent code.
- Near-term lifecycle ergonomics TODO: when the next Phase 12 batch repeats an
  object-owned allocation-context, delete-and-null, list/hash reset, pointer
  clear, copy/adopt-then-reset, or reset-through-initializer pattern, stop and
  add the checked action/macro/checker support first. The intended deliverable
  is a named `PG_RUNTIME_*` action or `PG_RUNTIME_DEFINE_*` helper that lets
  future batches update the manifest and bucket `.def` row instead of copying
  another helper body.
- Do this lifecycle-ergonomics preflight before any further large Gate E2
  teardown or state-migration batch, including PMChild/thread-backend cleanup
  work if it starts adding repeated reset/destroy glue. The expected outcome is
  either a short state-log note naming the existing checked mechanism being
  reused, or a documentation/code slice that adds the missing macro, named
  action, `.def` row, or checker rule before the behavior change.
- Session GUC direct-variable rebinding in `src/backend/utils/misc/guc.c`
  is table-driven by `threaded_session_guc_rebinds[]`. Add new migrated
  built-in direct-pointer GUCs to that table instead of extending
  `RebindSessionGUCVariablePointers()` with handwritten `find_option()` blocks.
  `ValidateSessionGUCVariableRebinds()` and
  `test_session_guc_rebind_table_matches_registry()` verify that the table
  matches the live GUC registry. Keep custom/extension GUC semantics covered
  by tests when changing this path.
- Do not attempt thread launch until the thread-safety floor is in place:
  backend-local globals must not be shared plain process globals, backend exit
  must not terminate the whole runtime, and timeout/interrupt delivery must be
  per logical backend.
- Before leaving Phase 12 or starting scheduler-aware wait work, run
  `gmake check-global-lifetimes` as part of Gate E2. A new mutable global must
  be annotated with an explicit `PG_GLOBAL_*` owner or deliberately accepted in
  `src/tools/global_lifetime/global_lifetime_baseline.tsv`.
- Raw `PG_THREAD_LOCAL PG_GLOBAL_BACKEND`, `PG_GLOBAL_SESSION`,
  `PG_GLOBAL_CONNECTION`, and `PG_GLOBAL_EXECUTION` declarations should now be
  confined to `src/backend/utils/init/backend_runtime.c` early-fallback
  storage. Raw `PG_GLOBAL_CARRIER` declarations should be limited to the
  runtime current pointers and narrowly documented process-context flags such
  as `IsUnderPostmaster`; wait-event self-pipe/signalfd state,
  `stack_base_ptr`, backend-thread launch state, and threaded GUC mutex depth
  live in `PgCarrier`. If a new in-tree module needs cached shared registry
  data, prefer `PG_GLOBAL_RUNTIME`; if it needs backend/session/execution or
  carrier state, add an explicit runtime-object bucket instead.
- Windows-only Phase 12 edits made from this macOS checkout must be marked as
  best-effort until a Windows build validates them. The current
  `pgwin32_noblock` bridge is covered by shared connection-object tests here,
  but `src/backend/port/win32/socket.c` still needs Windows compile coverage.
- Before leaving Phase 12, perform the Gate E2 object-lifecycle audit. Every
  carrier/backend/session/connection/execution state bucket needs a documented
  initializer, early-adoption behavior or proof that early adoption is
  impossible, reset/destroy behavior, owner/lifetime, and copy/adoption rule
  for pointer, list, memory-context, socket, hash-table, and opaque-pointer
  fields. Keep `MULTITHREADED_RUNTIME_LIFECYCLE.tsv` synchronized with
  `src/include/utils/backend_runtime.h`, and run
  `gmake check-runtime-lifecycles` after adding, renaming, or removing
  `PgCarrier`, `PgBackend`, `PgSession`, `PgConnection`, or `PgExecution`
  fields. Manual process/thread init/adopt asymmetries must be centralized or
  explicitly justified. The checker also verifies the manifest's runtime
  lifecycle function references, owner-map references, and the required
  process/thread constructor and top-level adoption calls; update the checker
  deliberately if the object construction shape changes.
- When closing a lifecycle row for a memory-context or compatibility bridge,
  make the reset order explicit in code and docs. If cleanup only clears
  pointer slots while existing transaction/main-loop cleanup still owns the
  pointed-to contexts, say that in `MULTITHREADED_RUNTIME_LIFECYCLE.tsv`
  rather than implying the broader `TopMemoryContext` split is solved.
- Backend early fallback adoption is centralized in
  `PgBackendAdoptEarlyState()`. Do not add a backend bucket adoption helper to
  only the process or thread install path; add it to that shared helper or
  document why the asymmetry is intentional. Pointer/list-bearing buckets need
  an explicit copy/adopt rule, and copied empty list heads must be asserted and
  reinitialized in the destination object rather than preserving fallback
  self-pointers.
- Session, connection, and execution early fallback adoption are centralized in
  `PgSessionAdoptEarlyState()`, `PgConnectionAdoptEarlyState()`, and
  `PgExecutionAdoptEarlyState()`. Add newly migrated buckets to those helpers
  rather than directly to
  `InitializePgProcessRuntime()` or `InstallPgThreadBackendRuntimeState()`.
  Threaded connection adoption must preserve the live `Port` supplied during
  `InitializePgThreadBackendRuntimeState()` by passing it as
  `PgConnectionAdoptEarlyState()`'s `preserved_port`.
- Phase 12 miscellaneous execution scratch state now lives under
  `PgExecution`: `PgExecutionAnalyzeState.array_extra_data`,
  `PgExecutionRegexState`, `PgExecutionValgrindState`, and
  `PgExecutionSnapBuildState`. These buckets use whole-bucket copy/adopt plus
  zero reset; their pointer fields are borrowed or opaque and do not own lists,
  memory contexts, hash tables, sockets, or heap allocations. After changing
  these runtime structs or accessors, use the installed-header/layout clean
  rebuild path before trusting TAP or extension results.
- Catalog transaction/execution scratch state now lives under
  `PgExecutionCatalogState`: uncommitted enum type/value hash pointers,
  REINDEX suppression state, and pending storage delete/sync state. The
  catalog files keep their historic local variable names as macros over
  runtime accessors. If a local struct field has the same name as one of those
  macros, rename the field; this was required for `SerializedReindexState`.
  The actual hash/list storage is still owned by existing transaction cleanup
  paths such as enum, reindex, and smgr end-of-transaction cleanup.
- Catalog cache execution scratch state now lives under
  `PgExecutionCatalogCacheState`: catcache's create-in-progress stack pointer,
  relcache's `RelationBuildDesc()` in-progress list pointer/length/capacity,
  relcache's EOXact relation OID list/length/overflow flag, and relcache's
  EOXact tupledesc array pointer/index/capacity. The runtime object owns the
  slots and inline OID array; pointed-to catcache stack entries, relcache
  in-progress storage, and tupledesc arrays remain owned by existing stack,
  `CacheMemoryContext`, and relcache EOXact cleanup paths. After changing this
  bridge, rebuild `backend_runtime.o`, `catcache.o`, `relcache.o`, and
  `test_backend_runtime.o`, then run `gmake check-runtime-lifecycles` and
  `gmake check-global-lifetimes`.
- LISTEN/NOTIFY transaction scratch state now lives under
  `PgExecutionAsyncState`: pending LISTEN/UNLISTEN actions, pending NOTIFY
  lists, pending listen hash state, queue head snapshots used by
  `SignalBackends()`, and its preallocated workspace arrays. `async.c` keeps
  the historic local names as macros over runtime accessors. The pending lists
  remain owned by transaction memory contexts and async transaction cleanup;
  the signal workspace arrays are still allocated under `TopMemoryContext`
  until the broader backend destructor model is closed.
- Session teardown now explicitly drops prepared statements, destroys the
  prepared-query hash, frees leftover `ON COMMIT` actions, and destroys any
  remaining async local-channel hash. Keep that cleanup after `shmem_exit()`
  and `on_proc_exit` callbacks; async shared listener cleanup still belongs to
  the existing proc-exit callback path.
- Text-search parser, dictionary, and configuration caches now live under
  `PgSessionTextSearchState` with the `default_text_search_config` value and
  OID cache. The reset path owns parser/config hash destruction, dictionary
  private memory-context deletion, config map-array frees, and last-used
  pointer clearing. After changing this bridge, rebuild `backend_runtime.o`,
  `ts_cache.o`, and `test_backend_runtime.o`, then run
  `gmake check-runtime-lifecycles` and `gmake check-global-lifetimes`.
- ACL role-membership caches now live under `PgSessionUserIdentityState`.
  `acl.c` keeps `cached_role`, `cached_roles`, and `cached_db_hash` as
  file-local macros over the current session object. The copied membership
  lists are allocated in `TopMemoryContext` by existing ACL code and freed by
  `PgSessionResetClosedState()`. After changing this bridge, rebuild `acl.o`,
  `backend_runtime.o`, and `test_backend_runtime.o`, then run
  `gmake check-runtime-lifecycles` and `gmake check-global-lifetimes`.
- The fmgr external C-function lookup hash now lives under
  `PgSessionFunctionManagerState`. `fmgr.c` keeps `CFuncHash` as a file-local
  macro over `PgCurrentCFuncHashRef()`. `PgSessionResetClosedState()` destroys
  the hash; dynamic library handles and `Pg_finfo_record` metadata remain
  runtime/dynamic-loader owned. After changing this bridge, rebuild
  `fmgr.o`, `backend_runtime.o`, and `test_backend_runtime.o`, then run
  `gmake check-runtime-lifecycles` and `gmake check-global-lifetimes`.
- Syscache, relcache, and relsync invalidation callback registries now live
  under `PgSessionInvalidationCallbackState`. `inval.c` keeps the historic
  registry names as file-local macros over `PgCurrentInvalidationCallbackState()`.
  `PgSessionResetClosedState()` clears callback registrations after dependent
  session caches have been destroyed. After changing this bridge, rebuild
  `inval.o`, `backend_runtime.o`, and `test_backend_runtime.o`, then run
  `gmake check-runtime-lifecycles` and `gmake check-global-lifetimes`.
- Catalog lookup cache roots for attribute options, relfilenumber mapping,
  tablespace options, event triggers, ruleutils SPI plans, and the ICU
  converter now live under `PgSessionCatalogLookupState`. The bucket owns the
  root slots and reset closes/destroys the roots that can be safely reclaimed,
  but pointed allocations under `CacheMemoryContext` remain part of the
  broader memory-context ownership split. After changing this bridge, rebuild
  `backend_runtime.o`, `attoptcache.o`, `relfilenumbermap.o`, `spccache.o`,
  `evtcache.o`, `ruleutils.o`, `pg_locale_icu.o`, and
  `test_backend_runtime.o`, then run `gmake check-runtime-lifecycles` and
  `gmake check-global-lifetimes`.
- PL/pgSQL's custom-GUC, compile, namespace, plugin, simple-expression, and
  cast-cache session state now lives behind an opaque private pointer in
  `PgSessionExtensionModuleState`. PL/pgSQL registers a session reset callback
  so closed-session reset can release its private roots before
  `dynamic_library_context` is deleted. After changing this bridge, rebuild
  `backend_runtime.o`, PL/pgSQL objects, and `test_backend_runtime.o`; clean
  and reinstall PL/pgSQL into `tmp_install` before threaded TAP.
- Transaction cleanup slots now live under
  `PgExecutionTransactionCleanupState`: large-object descriptor cleanup slots,
  the transaction temporary-file cleanup flag, the pgstat subtransaction stack,
  and RI fast-path batch-cache state. The runtime object owns these slots and
  scalar flags, but the pointed-to storage remains owned by existing
  large-object, temporary-file, pgstat, and RI transaction/subtransaction
  cleanup paths. Add future execution cleanup buckets through
  `PgExecutionAdoptEarlyState()` and update
  `MULTITHREADED_RUNTIME_LIFECYCLE.tsv`; after changing this bridge, run
  touched-object builds plus `gmake check-runtime-lifecycles` and
  `gmake check-global-lifetimes` before trusting TAP.
- Execution error and replication/apply scratch state now lives under
  `PgExecution`: `PgExecutionErrorState` owns the `elog.c` error-data stack,
  recursion depth, saved timestamp cache, and formatted log-time buffer;
  `PgExecutionReplicationScratchState` owns the event-trigger query-state
  pointer, replication-origin transaction state, logical apply error-context
  stack, apply message context, and logical streaming context. The moved
  pointer slots are borrowed from existing error, event-trigger, and logical
  apply cleanup paths; replication-origin state is copied scalar state. After
  changing this bridge, rebuild touched logical replication, event-trigger, and
  error-reporting objects, then run `gmake check-runtime-lifecycles` and
  `gmake check-global-lifetimes` before trusting TAP.
- `AuxProcessResourceOwner` is now routed through `PgBackend` via
  `PgCurrentAuxProcessResourceOwnerRef()` and the `AuxProcessResourceOwner`
  lvalue macro. After changing `src/include/utils/resowner.h` or this backend
  runtime bridge, clean and rebuild backend objects before trusting link or TAP
  results; stale objects can still reference the removed
  `_AuxProcessResourceOwner` symbol.
- `MyProc` is now routed through `PgBackend` via `PgCurrentMyProcRef()` and
  the `MyProc` lvalue macro. After changing `src/include/storage/proc.h` or
  this backend runtime bridge, clean and rebuild backend objects and any
  extension modules under test before trusting link or TAP results; stale
  objects can still reference the removed `_MyProc` symbol. At minimum, clean
  and reinstall PL/pgSQL and `src/test/modules/test_backend_runtime` before
  rerunning their tests after a `MyProc` bridge change.
- `MyProcNumber` and `ParallelLeaderProcNumber` are now routed through
  `PgBackend` via `PgCurrentMyProcNumberRef()`,
  `PgCurrentParallelLeaderProcNumberRef()`, and the existing lvalue names in
  `src/include/storage/procnumber.h`. After changing that header or this
  backend runtime bridge, clean and rebuild backend objects and any extension
  modules under test before trusting link or TAP results; stale objects can
  still reference the removed `_MyProcNumber` or
  `_ParallelLeaderProcNumber` symbols, or miss the new accessor symbols. At
  minimum, clean and reinstall PL/pgSQL and
  `src/test/modules/test_backend_runtime` before testing.
- `MyBEEntry` is now routed through `PgBackend` via
  `PgCurrentMyBEEntryRef()` and the existing lvalue name in
  `src/include/utils/backend_status.h`. After changing that header or this
  backend runtime bridge, clean and rebuild backend objects and any extension
  modules under test before trusting link or TAP results; stale objects can
  still reference the removed `_MyBEEntry` symbol, or miss the new accessor
  symbol. At minimum, clean and reinstall
  `src/test/modules/test_backend_runtime` before testing.
- `MyBgworkerEntry` is now routed through `PgBackend` via
  `PgCurrentMyBgworkerEntryRef()` and the lvalue macro in
  `src/include/postmaster/bgworker.h`. After changing that header or this
  backend runtime bridge, clean and rebuild backend objects and any extension
  modules under test before trusting link or TAP results; stale objects can
  still reference the removed `_MyBgworkerEntry` symbol, or miss the new
  accessor symbol. At minimum, clean and reinstall
  `src/test/modules/test_backend_runtime`, `src/test/modules/worker_spi`,
  `src/test/modules/test_shm_mq`, and any worker modules under test.
- `ConfigReloadPending`, `ShutdownRequestPending`, `WakeupStopPending`,
  `AutoVacLauncherPending`, and `CheckpointerShutdownXLOGPending` are now
  fields in `PgBackendPendingInterruptState`, exposed through compatibility
  macros in `src/include/miscadmin.h`; their old exported TLS symbols were
  removed from `src/backend/postmaster/interrupt.c` and
  `src/backend/postmaster/checkpointer.c`. After changing this bridge, clean
  and rebuild backend objects and extension modules that include
  `postmaster/interrupt.h` or `miscadmin.h`; stale modules can still reference
  removed `_ConfigReloadPending`, `_ShutdownRequestPending`,
  `_WakeupStopPending`, `_AutoVacLauncherPending`, or
  `_CheckpointerShutdownXLOGPending` symbols. At minimum, clean and reinstall
  PL/pgSQL, `src/test/modules/test_backend_runtime`, worker modules, and
  contrib modules under test before validating.
- nbtree, GIN, GiST, and SP-GiST WAL redo `opCtx` memory contexts now live in
  `PgBackendXLogState` through source-local compatibility macros. After
  changing the XLog state bridge or these redo files, run touched-object
  builds for the affected AM redo objects and `backend_runtime.o`, then clean
  rebuild/install before trusting runtime tests.
- Allocation-set freelists and the memory-context logging reentrancy guard now
  live in `PgBackendMemoryManagerState`. After changing this bridge or
  `src/backend/utils/mmgr/aset.c`/`mcxt.c`, run touched-object builds for
  `backend_runtime.o`, `aset.o`, `mcxt.o`, and `test_backend_runtime.o`, then
  use a clean backend rebuild/install before runtime validation. The
  global-lifetime scan drops by one for this slice because two raw globals are
  replaced by one early-backend fallback bucket.
- Wait-event reporting storage now lives in `PgBackendWaitState`, and
  `my_wait_event_info` is a compatibility macro over the current backend wait
  state except in the standalone `S_LOCK_TEST` build. The shared-invalidation
  local transaction ID counter now lives in `PgBackendIPCState`. After changing
  this bridge or `src/include/utils/wait_event.h`, run touched-object builds
  for `backend_runtime.o`, `wait_event.o`, `sinvaladt.o`, and
  `test_backend_runtime.o`, then clean/rebuild backend and `src/common` before
  full runtime validation; stale `src/common` server objects can still
  reference removed wait-event symbols.
- `DoingCommandRead` now lives in `PgSessionLoopState`, while tcop command
  option/timing state lives in `PgBackendCommandState` and elog formatted
  start-time/line-number/PID cache state lives in `PgBackendLogState`. After
  changing this bridge, run touched-object builds for `backend_runtime.o`,
  `postgres.o`, `elog.o`, and `test_backend_runtime.o`, then use the normal
  backend plus `src/common` clean rebuild path before runtime validation.
- `pgStatLocal` now lives in `PgBackendPgStatPendingState.local`; the
  `pgStatLocal` identifier is a compatibility macro over
  `PgCurrentPgStatLocalState()`. This currently makes `backend_runtime.h`
  include `pgstat_internal.h` so the pgstat-local object stays embedded
  instead of being lazily allocated. After changing this bridge, run
  touched-object builds for `backend_runtime.o`, `pgstat.o`, representative
  `src/backend/utils/activity` objects, and `test_backend_runtime.o`, then use
  the normal backend plus `src/common` clean rebuild path before runtime
  validation. Include `gmake check-global-lifetimes`, contrib build, PL/pgSQL
  rebuild/install, `test_backend_runtime check`, and direct threaded TAP.
- Computed-goto expression interpreter lookup state now lives in
  `PgBackendExprInterpState`; `dispatch_table` and `reverse_dispatch_table`
  in `execExprInterp.c` are compatibility macros over the current backend.
  The reverse table stores integer opcodes in a fixed
  `PG_BACKEND_EXPR_INTERP_MAX_OPS` array and `execExprInterp.c` asserts that
  `EEOP_LAST` fits. After changing this bridge, run touched-object builds for
  `execExprInterp.o`, `backend_runtime.o`, and `test_backend_runtime.o`, then
  use the normal backend plus `src/common` clean rebuild path before runtime
  validation. Include `gmake check-global-lifetimes`, contrib build, PL/pgSQL
  rebuild/install, `test_backend_runtime check`, and direct threaded TAP.
- `proc_exit_inprogress` and `shmem_exit_inprogress` are now fields in
  `PgBackendExitState`, exposed through compatibility macros in
  `src/include/storage/ipc.h`; the old exported TLS definitions were removed
  from `src/backend/storage/ipc/ipc.c`. After changing this bridge, clean and
  rebuild backend objects and extension modules that include `storage/ipc.h`;
  stale modules can still reference the removed `_proc_exit_inprogress` or
  `_shmem_exit_inprogress` symbols, or miss the
  `PgCurrentBackendExitStateRef()` accessor.
- `PendingBgWriterStats`, `PendingCheckpointerStats`,
  `PendingIOStats`, `pending_SLRUStats`, `PendingLockStats`,
  `PendingBackendStats`, `pgStatXactCommit`, `pgStatXactRollback`,
  `pgStatBlockReadTime`, `pgStatBlockWriteTime`, `pgStatActiveTime`,
  `pgStatTransactionIdleTime`, `total_func_time`, `prevWalUsage`,
  `prevBackendWalUsage`, `pgstat_report_fixed`, `pgStatForceNextFlush`,
  `force_stats_snapshot_clear`, `pgstat_is_initialized`,
  `pgstat_is_shutdown`, `pgStatPendingContext`, `pgStatPending`, and the
  related `have_*stats`/`backend_has_iostats` booleans are now fields in
  `PgBackendPgStatPendingState`, exposed through compatibility macros in
  `src/include/pgstat.h` and private macros/accessors in
  `src/backend/utils/activity/pgstat.c` and
  `src/include/utils/pgstat_internal.h`; the old exported/static TLS
  definitions were removed from pgstat implementation files.
  `PGSTAT_SLRU_NUM_ELEMENTS` is public only to size the runtime SLRU
  pending-state array and is asserted against `slru_names[]` in
  `src/include/utils/pgstat_internal.h`. The pending-entry list bridge assumes
  no early pgstat pending entries exist before backend-runtime adoption; copied
  non-empty `dlist_head` values would still point at the old list head, so the
  adoption path asserts that invariant and reinitializes the adopted head.
  After changing this bridge, clean and rebuild backend objects and extension
  modules that include `pgstat.h`; stale objects can still reference removed
  pgstat symbols or miss the new accessor symbols. At minimum, clean and
  reinstall PL/pgSQL,
  `src/test/modules/test_backend_runtime`, and contrib/test modules under
  pgstat coverage before validating.
- `pgBufferUsage`, `save_pgBufferUsage`, `pgWalUsage`, and
  `save_pgWalUsage` are now fields in `PgBackendInstrumentationState`,
  exposed through compatibility macros in `src/include/executor/instrument.h`;
  the old exported/static TLS definitions were removed from
  `src/backend/executor/instrument.c`. After changing this bridge, clean and
  rebuild backend objects and extension modules that include `instrument.h`;
  stale objects can still reference removed `_pgBufferUsage` or
  `_pgWalUsage` symbols, or miss the new accessor symbols. At minimum, clean
  and reinstall PL/pgSQL, `src/test/modules/test_backend_runtime`, and contrib
  before validating.
- Pending file-sync state (`pendingOps`, `pendingUnlinks`,
  `pendingOpsCxt`, `sync_cycle_ctr`, `checkpoint_cycle_ctr`, and
  `sync_in_progress`), storage-manager relation state (`SMgrRelationHash` and
  `unpinned_relns`), magnetic-disk storage-manager context (`MdCxt`), and
  file-descriptor/VFD state (`VfdCache`, `SizeVfdCache`, `nfile`,
  `temporary_files_allowed`, `numAllocatedDescs`, `maxAllocatedDescs`,
  `allocatedDescs`, and `numExternalFDs`) are now fields in
  `PgBackendStorageState`, exposed through private compatibility macros in
  `src/backend/storage/sync/sync.c`, `src/backend/storage/smgr/smgr.c`,
  `src/backend/storage/smgr/md.c`, and `src/backend/storage/file/fd.c`.
  The smgr adoption path asserts that no early smgr relation hash/list exists
  before backend-runtime adoption; copied non-empty `dlist_head` values would
  still point at the old list head. Threaded backend startup can reserve file
  descriptors before installing the backend runtime, so
  `InstallPgThreadBackendRuntimeState()` must adopt early storage state into
  the thread-backed `PgBackend`; losing that fallback fd state can make the
  threaded TAP postmaster exit immediately after launching worker threads.
  After changing this bridge, clean and rebuild backend objects because
  `PgBackend` layout and installed runtime headers changed; at minimum rebuild
  and reinstall PL/pgSQL, `src/test/modules/test_backend_runtime`, and contrib
  before validating.
- Deadlock detector workspace state (`visitedProcs`, `nVisitedProcs`,
  `topoProcs`, `beforeConstraints`, `afterConstraints`, `waitOrders`,
  `nWaitOrders`, `waitOrderProcs`, `curConstraints`, `nCurConstraints`,
  `maxCurConstraints`, `possibleConstraints`, `nPossibleConstraints`,
  `maxPossibleConstraints`, `deadlockDetails`, `nDeadlockDetails`, and
  `blocking_autovacuum_proc`) is now owned by `PgBackendLockState`, exposed
  through private compatibility macros in `src/backend/storage/lmgr/deadlock.c`.
  `PgBackendLockState` intentionally uses opaque pointer fields so the private
  `deadlock.c` `EDGE`, `WAIT_ORDER`, and `DEADLOCK_INFO` types stay local to
  that source file. After changing this bridge, clean and rebuild backend
  objects because `PgBackend` layout and installed runtime headers changed; at
  minimum rebuild and reinstall PL/pgSQL, `src/test/modules/test_backend_runtime`,
  and contrib before validating.
- Local-buffer state (`NLocBuffer`, `LocalBufferDescriptors`,
  `LocalBufferBlockPointers`, `LocalRefCount`, `nextFreeLocalBufId`,
  `LocalBufHash`, `NLocalPinnedBuffers`, and the `GetLocalBufferStorage()`
  allocation cursor/context fields) is now owned by `PgBackendBufferState`.
  Exported local-buffer names are compatibility macros in `storage/bufmgr.h`
  and `storage/buf_internals.h`; private names remain compatibility macros in
  `src/backend/storage/buffer/localbuf.c`. Shared-buffer pin/writeback state
  (`BackendWritebackContext`, `PinCountWaitBuf`, the private refcount
  array/hash state, and `MaxProportionalPins`) is also owned by
  `PgBackendBufferState`; `BackendWritebackContext` remains object-like at call
  sites through a `storage/buf_internals.h` macro. After changing this bridge,
  clean and rebuild backend objects because `PgBackend` layout and installed
  buffer/runtime headers changed; at minimum rebuild and reinstall PL/pgSQL,
  `src/test/modules/test_backend_runtime`, and contrib before validating.
- IPC/sinval backend state (`MyProcSignalSlot`, `SharedInvalidMessageCounter`,
  `catchupInterruptPending`, and the recursive
  `ReceiveSharedInvalidMessages()` buffer/cursor state) is now owned by
  `PgBackendIPCState`. `procsignal.c` keeps `ProcSignalSlot` private through a
  file-local compatibility macro; `sinval.h` keeps the exported counter/flag
  names as compatibility macros. After changing this bridge, clean and rebuild
  backend objects because `PgBackend` layout and installed storage/runtime
  headers changed; at minimum rebuild and reinstall PL/pgSQL,
  `src/test/modules/test_backend_runtime`, and contrib before validating.
- Lock-manager backend-local state now also lives in `PgBackendLockState`:
  fast-path local-use counters, relation-extension lock ownership,
  `LockMethodLocalHash`, strong-lock progress, awaited-lock/owner state,
  `got_deadlock_timeout`, condition-variable sleep target state, and
  speculative insertion token state. `lock.c`, `proc.c`,
  `condition_variable.c`, and `lmgr.c` keep local compatibility macros. After
  changing this bridge, clean and rebuild backend objects because
  `PgBackend` layout and installed runtime headers changed; at minimum rebuild
  and reinstall PL/pgSQL, `src/test/modules/test_backend_runtime`, and contrib
  before validating.
- Always-built LWLock backend-local state now also lives in
  `PgBackendLockState`: `num_held_lwlocks`, the fixed `held_lwlocks` array,
  and `LocalNumUserDefinedTranches` are backed by runtime accessors while
  `lwlock.c` keeps the existing local source names. Optional `LWLOCK_STATS`
  debug state also lives in this bucket: the stats hash pointer, dummy stats
  entry, stats memory context pointer, and exit-registration flag are routed
  through backend-runtime accessors. Normal builds in this checkout do not
  compile the debug-only stats block, so pair static lifetime scan coverage
  with the backend-runtime accessor test unless using an `LWLOCK_STATS` build.
  After changing this bridge, clean and rebuild backend objects because
  `PgBackend` layout and installed runtime headers changed; at minimum rebuild
  and reinstall PL/pgSQL, `src/test/modules/test_backend_runtime`, and contrib
  before validating.
- Predicate-lock backend-local state now also lives in `PgBackendLockState`:
  `LocalPredicateLockHash`, `MySerializableXact`, `MyXactDidWrite`, and
  `SavedSerializableXact` are backed by runtime accessors while `predicate.c`
  keeps the existing local source names. Keep private `SERIALIZABLEXACT`
  layout out of `backend_runtime.h`; store those pointers as opaque `void *`
  fields and cast them in `predicate.c`. After changing this bridge, clean and
  rebuild backend objects because `PgBackend` layout and installed runtime
  headers changed; at minimum rebuild and reinstall PL/pgSQL,
  `src/test/modules/test_backend_runtime`, and contrib before validating.
  Direct threaded TAP should be run; a clean rerun is acceptable if the first
  run hits the known transient macOS child-count/shutdown race after SQL
  assertions finish.
- Transaction/access-manager backend-local state now lives in
  `PgBackendTransactionState`: transaction-status cache fields, two-phase
  locked-GXACT and exit-registration fields, the private `TwoPhaseGetGXact()`
  lookup cache, SLRU error-report fields, and multixact cache/debug-string
  state. This bridge deliberately includes function-local statics that do not
  appear in the raw `PG_THREAD_LOCAL` scan. The multixact list head must be
  initialized through the runtime state initializer, and early adoption asserts
  that any initialized early list is empty before copying. After changing this
  bridge, clean and rebuild backend objects because `PgBackend` layout and
  installed runtime headers changed; at minimum rebuild and reinstall
  PL/pgSQL, `src/test/modules/test_backend_runtime`, and contrib before
  validating.
- ProcArray backend-local visibility/cache state now also lives in
  `PgBackendTransactionState`: the `TransactionIdIsInProgress()` negative
  cache, `GlobalVisState` horizon caches, the
  `ComputeXidHorizonsResultLastXmin` throttle, and `XIDCACHE_DEBUG` counters.
  `GlobalVisState` is defined in `utils/backend_runtime.h` so the runtime can
  store it by value while existing snapshot/heapam headers keep using forward
  declarations. After changing this bridge, clean and rebuild backend objects
  because `PgBackend` layout and installed runtime headers changed; at minimum
  rebuild and reinstall PL/pgSQL, `src/test/modules/test_backend_runtime`, and
  contrib before validating.
- Snapshot-manager and combo-CID transaction visibility state now lives in
  `PgExecution`: `PgExecutionSnapshotState` owns `snapmgr.c` snapshot pointers,
  reusable `SnapshotData`, `TransactionXmin`, `RecentXmin`,
  `FirstSnapshotSet`, active/registered snapshot tracking, historic tuple-CID
  state, and exported-snapshot tracking; `PgExecutionComboCidState` owns the
  combo-CID hash, array pointer, and counters. `snapmgr.c` keeps its
  `ActiveSnapshotElt` type and registered-snapshot heap comparator private;
  the runtime bucket stores the heap and `snapmgr.c` lazily initializes the
  comparator. After changing this bridge, clean and rebuild backend objects
  because `PgExecution` layout and installed runtime/snapshot headers changed;
  at minimum rebuild and reinstall PL/pgSQL, `src/test/modules/test_backend_runtime`,
  and contrib before validating.
- WAL record-construction workspace now lives in `PgExecution`:
  `PgExecutionXLogInsertState` owns the `xloginsert.c` registered-buffer
  workspace, main-data `XLogRecData` chain state, insert flags, header
  record/scratch storage, registered-data array state, in-progress flag, and
  memory context. `registered_buffer` remains private to `xloginsert.c` behind
  an opaque runtime pointer. Early adoption asserts that no WAL insert is in
  progress and retargets the `mainrdata_last` self-pointer sentinel when early
  `InitXLogInsert()` has run before process/thread runtime installation. The
  hidden `XLogGetFakeLSN()` function-local statics are still a follow-up
  because they need a separate session/execution lifetime decision. After
  changing this bridge, clean and rebuild backend objects because
  `PgExecution` layout and installed runtime headers changed; at minimum
  rebuild and reinstall `src/test/modules/test_backend_runtime`, PL/pgSQL, and
  contrib before validating.
- Simple exported transaction execution state now lives in `PgExecution`:
  `PgExecutionXactState` owns `XactIsoLevel`, `XactReadOnly`,
  `XactDeferrable`, `xact_is_sampled`, `CheckXidAlive`, `bsysscan`, and
  `MyXactFlags`, plus the top full XID, parallel-current-XID count/borrowed
  pointer, inline unreported-XID array, subtransaction and command ID counters,
  transaction timestamps, prepare GID, force-sync flag, and transaction abort
  context pointer. `xact.h` keeps the public exported names as lvalue macros
  but must not include `backend_runtime.h`; it only declares accessor
  prototypes because `backend_runtime.h` already includes `xact.h`. The
  private transaction-state stack and transaction callback lists in `xact.c`
  remain a follow-up requiring a broader lifecycle split. After changing this
  bridge, clean and rebuild backend objects because `PgExecution` layout and
  installed runtime/xact headers changed; at minimum rebuild and reinstall
  `src/test/modules/test_backend_runtime`, PL/pgSQL, and contrib before
  validating. If `xact.c` defines compatibility macros for private names,
  rename local struct fields such as serialized transaction-state fields so
  macro expansion does not rewrite `tstate->field` references.
- GUC/error-report scratch state now lives in `PgExecution`:
  `PgExecutionGUCErrorState` owns the GUC check-hook error code and
  message/detail/hint strings, `pre_format_elog_string()` errno/domain state,
  and config-file scanner line/fatal-jump state. `guc.h` keeps public
  `GUC_check_errmsg_string`, `GUC_check_errdetail_string`, and
  `GUC_check_errhint_string` as lvalue macros. `guc.c`, `elog.c`, and
  `guc-file.l` keep private names through file-local compatibility macros.
  After changing this bridge, clean and rebuild backend objects because
  `PgExecution` layout and installed `guc.h` changed; stale backend objects
  can fail to link against removed `_GUC_check_*` symbols and stale PL/pgSQL
  can fail during `initdb` while loading removed `_GUC_check_*` symbols. At
  minimum rebuild and reinstall `src/test/modules/test_backend_runtime`,
  PL/pgSQL, and contrib before validating.
- Backend activity snapshot state now lives in `PgBackendActivityState`:
  `localBackendStatusTable`, `localNumBackends`, and
  `backendStatusSnapContext` are backed by runtime accessors while
  `backend_status.c` keeps the existing local source names. Pgstat
  shared-entry reference-cache state (`pgStatEntryRefHash`,
  `pgStatSharedRefAge`, `pgStatSharedRefContext`, and
  `pgStatEntryRefHashContext`) now lives in `PgBackendPgStatPendingState`
  behind private pgstat accessors and `pgstat_shmem.c` compatibility macros.
  The private simplehash type stays local to `pgstat_shmem.c` through an
  opaque runtime pointer. `pgStatLocal` remains standalone backend-local TLS
  for a later dedicated pgstat-local slice because its type depends on
  internal pgstat snapshot structures. After changing this bridge, clean and
  rebuild backend objects because `PgBackend` layout and installed runtime
  headers changed; at minimum rebuild and reinstall PL/pgSQL,
  `src/test/modules/test_backend_runtime`, and contrib before validating.
- Backend utility/support state now lives in `PgBackendUtilityState`:
  dynahash active sequential-scan tracking, the superuser one-entry cache,
  the resource-owner release callback pointer, and optional `RESOWNER_STATS`
  counters are backed by runtime accessors while `dynahash.c`,
  `superuser.c`, and `resowner.c` keep local source names. The private
  `ResourceReleaseCallbackItem` type stays local to `resowner.c`; the runtime
  stores the callback head as an opaque pointer and `resowner.c` casts it
  through a file-local typed helper. After changing this bridge, clean and
  rebuild backend objects because `PgBackend` layout and installed runtime
  headers changed; at minimum rebuild and reinstall PL/pgSQL,
  `src/test/modules/test_backend_runtime`, and contrib before validating.
- Utility cache/scratch state now also lives in `PgBackendUtilityState`:
  date/time token caches, degree-trig cached constants, date/time and numeric
  format-picture caches, the optional libxml allocation context, and the
  missing-attribute datum cache are backed by runtime accessors while
  `datetime.c`, `float.c`, `formatting.c`, `xml.c`, and `heaptuple.c` keep
  local source names. Private cache entry types stay private to their owning
  files through opaque runtime pointer arrays and local casts. After changing
  this bridge, clean and rebuild backend objects because `PgBackend` layout
  and installed runtime headers changed; at minimum rebuild and reinstall
  PL/pgSQL, `src/test/modules/test_backend_runtime`, and contrib before
  validating.
- Timezone-abbreviation session state now lives in `PgSessionDateTimeState`:
  `datetime.c` keeps local names for the active `TimeZoneAbbrevTable` pointer
  and recent abbreviation lookup cache through runtime accessors. The table
  pointer is borrowed from GUC extra storage; the inline cache is copied with
  the session bucket and reset by `InstallTimeZoneAbbrevs()` and
  `ClearTimeZoneAbbrevCache()`. After changing this bridge, rebuild
  `backend_runtime.o`, `datetime.o`, and `test_backend_runtime.o`, then run
  `gmake check-runtime-lifecycles` and `gmake check-global-lifetimes`.
- Logical replication session-cache roots now live in
  `PgSessionLogicalReplicationState`: `origin.c`, `relation.c`, `syncutils.c`,
  and `pgoutput.c` keep local names through runtime accessors. Relation-map
  contexts own their hashes and entries; `pgoutput` owns its relation sync
  hash; the replication-origin slot is a borrowed shared-memory pointer whose
  refcount is still released by `replorigin_session_reset()`/exit cleanup.
  After changing this bridge, rebuild `backend_runtime.o`, `origin.o`,
  `relation.o`, `syncutils.o`, `pgoutput.o`, and
  `test_backend_runtime.o`, then run the lifecycle/global scans and the
  `test_backend_runtime` regression.
- Utility command/cache state in `PgBackendUtilityState` now also covers
  async notify pending and exit-registration flags, the extension sibling cache
  head, the injection-point callback cache, and the legacy sampling reservoir
  state. `notifyInterruptPending` remains an exported source-compatible macro
  in `commands/async.h`; `ExtensionSiblingCache` stays private to
  `extension.c`; injection-point coverage requires an
  `--enable-injection-points` build for runtime tests. After changing this
  bridge, clean and rebuild backend objects because `PgBackend` layout and
  installed runtime headers changed; at minimum rebuild and reinstall
  PL/pgSQL, `src/test/modules/test_backend_runtime`, and contrib before
  validating.
- Parallel worker and pqmq backend-local state now lives in
  `PgBackendParallelState`: `ParallelWorkerNumber`,
  `ParallelMessagePending`, `InitializingParallelWorker`, private parallel
  context tracking, and shared-memory message queue redirection state are
  backed by runtime accessors while `parallel.c` and `pqmq.c` keep local
  source names. Private `FixedParallelState` and `shm_mq_handle` types remain
  opaque outside their owning files. The early fallback parallel state must
  keep the legacy `ParallelWorkerNumber = -1` sentinel as a static
  initializer; bootstrap reaches `IsParallelWorker()` before full backend
  runtime adoption, and a zero fallback makes `initdb` believe it is in a
  parallel worker. After changing this bridge, clean and rebuild backend
  objects because `PgBackend` layout and installed runtime headers changed; at
  minimum rebuild and reinstall PL/pgSQL, `src/test/modules/test_backend_runtime`,
  and contrib before validating.
- DSM/latch IPC backend-local state now also lives in `PgBackendIPCState`:
  `dsm_init_done`, `dsm_registry_dsa`, `dsm_registry_table`, `LatchWaitSet`,
  and `LocalLatchData` are backed by runtime accessors while `dsm.c`,
  `dsm_registry.c`, `latch.c`, and `miscinit.c` keep local compatibility
  names. Threaded backend startup initializes `MyLatch` and `LatchWaitSet`
  before installing the backend runtime object, so early IPC adoption must
  retarget adopted `backend->core.latch` and `backend->interrupt_latch`
  pointers from the early fallback latch to the backend-owned latch. If this
  is missed, direct threaded TAP fails during startup with
  `cannot wait on a latch owned by another process`. After changing this
  bridge, clean and rebuild backend objects because `PgBackend` layout and
  installed runtime headers changed; at minimum rebuild and reinstall
  PL/pgSQL, `src/test/modules/test_backend_runtime`, and contrib before
  validating.
- Timeout scheduler backend-local state now lives in `PgBackendTimeoutState`:
  registered timeout parameters, the active timeout queue, alarm/signal
  pending flags, firing-target pointers, and signal-vs-logical delivery mode
  are backed by runtime accessors while `timeout.c` keeps local compatibility
  names. `PgTimeoutParams` is defined in `utils/timeout.h` so `PgBackend` can
  own the fixed timeout arrays directly. After changing this bridge, clean and
  rebuild backend objects because `PgBackend` layout and installed runtime
  headers changed; at minimum rebuild and reinstall PL/pgSQL,
  `src/test/modules/test_backend_runtime`, and contrib before validating.
  Direct threaded TAP exercises logical timeout delivery and should be run.
- WAL sender backend-local state now lives in `PgBackendWalSenderState`.
  Public WAL sender flags and `MyWalSnd` are compatibility macros over
  `PgCurrentWalSenderState()`, while `walsender.c` uses private macros for the
  streaming cursor, timeline state, reply buffers, logical decoding context,
  replication command context, and lag tracker. Keep the local sent pointer
  named distinctly from `WalSnd.sentPtr` to avoid macro expansion inside
  shared-memory struct field references. After changing this bridge, clean and
  rebuild backend objects because `PgBackend` layout and installed runtime
  headers changed; at minimum rebuild and reinstall PL/pgSQL,
  `src/test/modules/test_backend_runtime`, and contrib before validating.
  Direct threaded TAP should be run.
- Replication receiver and slot backend-local state now lives in
  `PgBackendReplicationState`. `MyReplicationSlot` is a compatibility macro
  over `PgCurrentReplicationState()`, while `syncrep.c` and `walreceiver.c`
  keep local compatibility names for sync-rep wait mode and WAL receiver
  connection/file/logstream/wakeup/reply state. The runtime initializer sets
  non-zero sentinels: `sync_rep_wait_mode = SYNC_REP_NO_WAIT`,
  `walreceiver_recv_file = -1`, and
  `walreceiver_primary_has_standby_xmin = true`. Fake-backend tests that
  inspect untouched replication state must initialize those fields explicitly
  because raw `MemSet()` does not model `PgBackendInitializeReplicationState()`.
  After changing this bridge, clean and rebuild backend objects because
  `PgBackend` layout and installed runtime headers changed; at minimum rebuild
  and reinstall PL/pgSQL, `src/test/modules/test_backend_runtime`, and contrib
  before validating. Direct threaded TAP should be run.
- Logical replication worker backend-local state now lives in
  `PgBackendLogicalReplicationState`. Public logical replication headers keep
  the old names for `ApplyContext`, `MyParallelShared`,
  `ParallelApplyMessagePending`, `LogRepWorkerWalRcvConn`, `MySubscription`,
  `MyLogicalRepWorker`, `in_remote_transaction`, `InitializingApplyWorker`,
  `table_states_not_ready`, `SlotSyncShutdownPending`, and `XLogLogicalInfo`
  as compatibility macros over `PgCurrentLogicalReplicationState()`.
  Source-private launcher, apply-worker, parallel-apply, table-sync,
  sequence-sync, logical-info, and slot-sync fields use local macros in their
  owning files. The runtime initializer sets non-zero sentinels for
  `remote_final_lsn`, `stream_xid`, `skip_xact_finish_lsn`, and
  `last_flushpos`. Fake-backend tests that inspect untouched logical
  replication state must initialize those fields explicitly because raw
  `MemSet()` does not model `PgBackendInitializeLogicalReplicationState()`.
  The deeper logical replication internals `lsn_mapping`,
  `apply_error_callback_arg`, `subxact_data`, and slot-sync `sleep_ms` are
  also now stored in `PgBackendLogicalReplicationState`; the runtime
  initializer sets their non-zero sentinels, including `remote_attnum = -1`,
  invalid transaction/LSN values, invalid `subxact_last`, and
  `PG_BACKEND_SLOTSYNC_INITIAL_SLEEP_MS`. Do not include
  `logicalrelation.h` or `logicalproto.h` from `backend_runtime.h`; keep
  private logical-replication layouts opaque there by using `struct
  LogicalRepRelMapEntry *` and `int` storage for the relation pointer and
  message type. After changing this bridge, clean and rebuild backend objects
  because `PgBackend` layout and installed runtime headers changed; at minimum
  rebuild and reinstall PL/pgSQL, `src/test/modules/test_backend_runtime`, and
  contrib before validating. Direct threaded TAP should be run.
- Treat `PMChild.thread_backend` as private PMChild-owned publication state.
  Postmaster code should use PMChild helper APIs for threaded backend
  interrupt, wakeup, and thread-exit publication rather than dereferencing or
  clearing the raw pointer outside PMChild.
- Treat `PMChild.signal_pid` as live carrier-visible routing/logging state.
  Thread exit publication must capture the exited logical backend id in the
  PMChild exit payload before clearing `signal_pid`, so the postmaster can log
  the exited backend without advertising a dead thread as signalable.
- Thread-backed PMChild signal-id reads and thread-exit payload reads must use
  PMChild helper APIs. They are protected by the same PMChild mutex as
  `thread_backend` publication and clearing. Thread-carrier payload resets in
  `PostmasterChildSetProcess()`, `PostmasterChildSetThread()`, and
  `ReleasePostmasterChildSlot()` also belong under that mutex, so slot release
  and reuse cannot race with signal-id, interrupt, wakeup, or exit-payload
  readers.
- Use `PostmasterChildDetachThreadBackend()` when a thread carrier needs to
  stop advertising its live logical-backend pointer before final exit
  publication. It preserves the exited logical id for reaping/logging while
  preventing later signal routing from targeting a backend committed to
  teardown.
- `test_pmchild_thread_backend_publication_race()` in
  `src/test/modules/test_backend_runtime` is the focused C-level stress for
  the PMChild helper contract. Run the full `test_backend_runtime` regression
  after changing PMChild thread publication, detach, signal-id, interrupt,
  wakeup, or exit-payload behavior.
- For thread-backed PMChild reaping, successful `pg_thread_join()` is the
  boundary before child cleanup and slot release. If join fails, leave the
  PMChild active and re-publish the claimed thread-exit report for retry; do
  not release or reuse a slot whose native carrier was not joined.
- Threaded backend exit currently reports retained carrier `TopMemoryContext`
  bytes through PMChild exit accounting. Do not remove or bypass this
  accounting until thread-exit memory/resource cleanup has a stronger
  replacement. A direct attempt to reset the exiting carrier's top memory tree
  after `PgBackendExitCleanup()` crashed a parallel threaded reconnect smoke,
  so treat full `TopMemoryContext` reclamation as an unresolved Gate E2 blocker.
  The `TopMemoryContext` pointer slot itself now lives in
  `PgExecution.memory_contexts.top_context`; that is only a pointer-slot
  migration and must not be treated as proof that the top context tree can be
  deleted safely.
- `test_backend_runtime_emit_fatal()` in
  `test_backend_runtime_threaded` is the focused threaded backend `FATAL`
  fixture. Run it through
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` with the
  local TAP `PERL5LIB` paths documented below, so the check covers the
  expected `FATAL`, verifies the backend id leaves `pg_stat_activity`, and
  confirms the server remains usable.
- `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` contains
  the broader mixed teardown stress for Gate E2. It starts concurrent
  backend-local `FATAL`, `pg_terminate_backend()`, and abandoned-client
  sessions, then verifies logical backend ids leave `pg_stat_activity`,
  advisory locks are released, and the server remains usable. Keep this
  fixture current when changing PMChild exit publication, thread join/retry,
  backend teardown, or session resource cleanup.
- Threaded regular backend launch duplicates the accepted client socket into
  `BackendThreadStart.client_sock`. `pq_init()` marks that launch-time socket
  copy invalid only after `Port` owns the descriptor and `socket_close()` is
  registered. `backend_thread_finish()` is the backstop for closing a still
  valid copied socket if startup fails before that handoff.
- Backend libpq connection teardown is now part of the Gate E2 resource model:
  `socket_close()` frees the frontend/backend `WaitEventSet`, the dynamically
  sized send buffer, and the `PortContext` that owns `Port` plus most startup
  packet/remote-host/authentication strings and SSL/GSS connection identity
  structures before closing the accepted socket. Keep the threaded TAP teardown
  matrix current when changing backend libpq socket I/O or `Port` ownership
  state, because normal disconnect, abandoned clients, `FATAL`, and
  administrator termination all exercise this callback.
- `PgConnectionResetClosedState()` is the retained-object cleanup companion to
  `socket_close()`. `socket_close()` remains responsible for freeing the
  palloc-backed send buffer and `WaitEventSet`; the runtime helper scrubs the
  retained `PgConnection` socket/protocol/startup/security buckets, deletes
  any connection-owned warning context left by startup/authentication, and
  frees the malloc-backed GSS buffers. `StoreConnectionWarning()` delegates to
  the object-explicit `StoreConnectionWarningForConnection()`, which copies
  warning text into `PgConnection.startup.connection_warning_context`; do not
  reintroduce `TopMemoryContext` allocation for connection warning list cells
  or strings. Keep `test_connection_reset_closed_state()` and
  `test_connection_warning_state_is_connection_local()` current when changing
  connection teardown ownership.
- `PgSessionResetClosedState()` is the first retained-session cleanup
  companion. It deletes `PgSession.dynamic_library_context`, which owns the
  `dynamic_library_inits` list cells used for per-session dynamic-library
  `_PG_init()` replay, and clears the list pointer after `on_proc_exit`
  callbacks have run. Keep `test_session_reset_closed_state()` current when
  changing extension module replay or session teardown ownership.
- Thread-backed auxiliary workers receive postmaster `SIGQUIT`, `SIGKILL`,
  and `SIGABRT` as logical `PG_BACKEND_INTERRUPT_PROC_DIE` mailbox events, not
  as process signal handlers that can `_exit()`. Any custom auxiliary
  interrupt loop that calls `PgCurrentBackendApplyInterrupts()` must explicitly
  handle `ProcDiePending`, or immediate shutdown can leave thread carriers
  waiting for SIGKILL escalation.
- The temporary threaded startup serialization gate is helper-controlled, not
  unconditional. Do not reintroduce a broad `backend_thread_entry()` gate: it
  can block normal client startup behind long-running worker initialization or
  a worker path that has not reached `ThreadedBackendStartupComplete()`. Add a
  backend type to `backend_thread_requires_startup_gate()` only with a named
  shared-state dependency and a stress test that proves the gate releases.
- Thread-backed startup/exit publication must tolerate a missing postmaster
  latch during startup-era handoff. Startup carriers can finish before
  `ServerLoop()` has configured `postmaster_pmsignal_latch`; PMChild
  publication records the atomic state even with a NULL latch, and the
  postmaster drains thread startup/exit state before each blocking wait.
- Process-model background workers are still rejected in threaded mode.
  Thread-compatible dynamic background workers publish their shared bgworker
  started state only after the worker reaches
  `ThreadedBackendStartupComplete()`, so dynamic waiters cannot terminate the
  worker while `InitProcess()`, `BaseInit()`, or background-worker function
  lookup are still running. Background writer/checkpointer/WAL writer bypass
  was validated as a worker-specific narrowing because their common auxiliary
  startup does not run database/session bootstrap before entering the worker
  loop. The autovacuum launcher bypass is validated against the no-database
  launcher loop; autovacuum worker bypass is validated against a real
  database-connected autovacuum worker launch and table vacuum smoke. Startup
  process, archiver, WAL receiver, and WAL summarizer bypasses are validated
  separately because they use the same common auxiliary startup, publish
  wakeup/progress state through shared memory, and keep per-loop work state
  backend-local. WAL receiver's gate bypass covers
  `AuxiliaryProcessMainCommon()`; the later `libpqwalreceiver` load and
  streaming loop are validated by a threaded physical-replication smoke.
  Startup process bypass is validated by threaded normal-startup and
  crash-recovery smokes. Slot sync worker bypass is validated by a threaded
  physical standby smoke that synchronizes a failover logical slot from a
  primary and verifies standby catalog usability. Keep any future startup-gate
  reintroduction narrowly tied to a named shared-state dependency and covered
  by concurrent catalog-startup stress.
- Prefer introducing compatibility wrappers around current globals before
  changing all call sites.
- Be careful moving GUC backing variables behind dynamic lvalue macros. The
  generated GUC table stores direct pointers for many variables during
  `InitializeGUCVariablePointers()`. Variables written only by assign hooks,
  such as parsed `DateStyle`/`DateOrder`, can be moved independently, but
  direct-pointer GUCs need a GUC-table pointer rebind/adoption mechanism.
  Threaded startup now records the direct backing-variable pointers after
  `InitializeGUCVariablePointers()`, runs
  `RebindSessionGUCVariablePointers()`, and initializes every built-in GUC
  record whose backing pointer changed. Keep extending
  `RebindSessionGUCVariablePointers()` when moving more direct-pointer GUC
  backing variables under runtime/session/execution objects. Only the small
  TLS dummy startup compatibility list in
  `InitializeThreadedSessionCompatibilityGUCOptions()` should remain
  hand-curated until those dummy GUCs get explicit session accessors. When
  common GUC names become macros, local struct fields with the same names must
  be renamed because macro expansion also hits `object->field` expressions;
  this was observed for the local GIN build-state `work_mem` field and the
  `TableSpaceOpts` `seq_page_cost`/`random_page_cost` fields.
- Some string GUCs can still be unset after runtime installation because the
  generated GUC table may already point at early fallback accessors before the
  "changed pointer" pass runs. `InstallPgThreadBackendRuntimeState()` therefore
  calls `InitializeThreadedSessionRequiredGUCOptions()` after
  `PgSetCurrentSession()` and after installing `CurrentPgExecution`; the latter
  is required because GUC check hooks allocate through the current execution's
  memory context state. That pass now initializes any built-in string GUC whose
  backing pointer is owned by the installed `PgSession` and still has NULL
  string storage, so future session-owned string GUCs do not need to be added
  to a growing whitelist. `client_encoding` remains the only post-install
  compatibility exception because its authoritative state is the session
  encoding object rather than a direct `char *` field in `PgSession`.
- The central GUC registry is now `PgSession` state, not an independent
  process/thread-global bucket. `PgSessionGUCState` owns `GUCMemoryContext`,
  the copied GUC records, the GUC hash table, non-default/stack/report lists,
  reporting state, and `GUCNestLevel`. Any fake `PgSession` used by tests that
  call `SetConfigOption()`, `GetConfigOption()`, or `RebindSessionGUCVariablePointers()`
  needs a real per-session GUC table from `InitializeThreadedSessionGUCOptions()`;
  otherwise `guc_hashtab` will be NULL or a test sentinel and `find_option()`
  can crash. `test_backend_runtime` centralizes this in
  `test_copy_current_user_identity()`.
- Early GUC owner adoption must run before copying GUC-backed string buckets
  such as datetime, text search, and connection GUC state. The copied strings
  are owned by the transferred `GUCMemoryContext`; resetting the detached
  early datetime/text-search/connection string buckets leaves them
  uninitialized with NULL string pointers so partial runtime installation does
  not allocate new fallback-owned strings or free non-owned fallback defaults.
  Do not move `PgSessionAdoptEarlyGUCState()` later in
  `PgSessionAdoptEarlyState()`.
- Threaded GUC setup, mutation, and display currently use a temporary
  process-wide GUC critical section in `guc.c`. Treat it as a Gate E2
  correctness bridge while copied GUC metadata and check/assign/show hooks
  still have process-era assumptions. Narrowing it requires focused threaded
  smokes for concurrent GUC replay, `SET`, `SHOW`, and custom extension GUCs.
  The reentrancy depth for this bridge lives in `PgCarrier`, not standalone
  TLS; tests that swap fake carriers and touch `PgCurrentThreadedGUCMutexDepthRef()`
  must preserve and restore `CurrentPgCarrier`.
- Threaded `read_nondefault_variables()` skips `PGC_POSTMASTER` and
  `PGC_INTERNAL` records. Thread carriers share the postmaster address space,
  so process-global postmaster/internal GUCs are already present and must not
  be replayed through a session `GUCMemoryContext`.
- Runtime-global GUC metadata must not allocate from a session
  `GUCMemoryContext`. `reserved_class_prefix` is process/runtime metadata used
  by extension module initialization such as PL/pgSQL's GUC prefix
  reservation, so `MarkGUCPrefixReserved()` uses its own `TopMemoryContext`
  child and the temporary threaded GUC lock.
- Portal manager session state now lives in `PgSessionPortalManagerState`.
  `portalmem.c` keeps `TopPortalContext`, `PortalHashTable`, and the unnamed
  portal counter as local macros over runtime accessors. The lifecycle rule is
  destructive at session close: `PgSessionResetClosedState()` deletes
  `TopPortalContext`, which owns portal structs, portal contexts, hold
  contexts, and the portal hash table, then clears the counter.
- Regex session cache state now lives in `PgSessionRegexState`. `regexp.c`
  keeps the compiled-regexp context, fixed cached-entry array, cached-entry
  count, and ctype cache list behind runtime accessors. Session reset deletes
  the compiled-regexp cache context, clears the inline array/count, and frees
  the ctype cache list.
- Syscache and catcache session roots now live in
  `PgSessionCatalogLookupState`. `syscache.c` keeps `SysCache[]`, the
  initialization flag, and relation/supporting-relation OID arrays behind
  runtime accessors; `catcache.c` keeps `CacheHdr` behind a runtime accessor.
  Relcache root hashes, critical-cache flags, and the relcache invalidation
  counter also live in this bucket; `relcache.h` keeps the historical critical
  flag names as accessor macros for `relcache.c`, `catcache.c`, and
  `postinit.c`. Typcache root hashes, the domain list, in-progress stack
  pointer/counters, record-cache array/counters, and tupledesc ID counter also
  live in this bucket; `typcache.c` keeps the historical local names as
  accessor macros. Session reset clears those roots and scalars, while cache
  entry memory still belongs to the broader `CacheMemoryContext` ownership
  split. Do not move `funccache.c`'s hash root without adding a real
  iterator/destructor for copied tuple descriptors and language-specific
  cached-function state.
- After changing the relcache critical-cache flags from exported TLS variables
  to `relcache.h` accessor macros, stale objects may still reference the old
  linker symbols even when GNU make thinks they are up to date. If the backend
  link fails with unresolved `_criticalRelcachesBuilt` or
  `_criticalSharedRelcachesBuilt`, remove and rebuild at least
  `src/backend/utils/cache/catcache.o`, `src/backend/utils/init/postinit.o`,
  and `src/backend/commands/seclabel.o`, then rerun `gmake -j8`.
- Do not shallow-copy live `dlist_head` or `dclist_head` values when moving
  fallback state into a real runtime object. Use the runtime list-head move
  helpers so moved list nodes' back-links point at the destination head. This
  currently matters for the GUC non-default list and RI valid-entry dclist.
- Threaded backend cleanup currently retains each thread's `TopMemoryContext`
  tree for post-exit accounting. Do not free AllocSet context freelists during
  threaded `PgBackendResetClosedState()` until full `TopMemoryContext`
  reclamation is implemented; thread-mode reset clears the memory-manager
  freelist bucket, while process-mode reset still calls
  `AllocSetFreeContextFreelists()`.
- Threaded startup serialization is deliberately not a broad
  `backend_thread_entry()` gate. A broad gate can block normal threaded startup
  behind worker paths that have not reached `ThreadedBackendStartupComplete()`.
  Keep startup gating helper-controlled through
  `backend_thread_requires_startup_gate()`, and require a named shared-state
  dependency plus a release/stress test for any backend type that opts in.
- Custom extension GUCs in threaded sessions rely on per-session `_PG_init()`
  invocation for already-loaded dynamic libraries. `dfmgr.c` records loaded
  module init state in `PgSession.dynamic_library_inits`, with list storage
  allocated under `PgSession.dynamic_library_context`; when a second threaded
  session reuses a process-loaded module, `_PG_init()` must run again so that
  session's GUC table receives the custom GUC definitions. A focused custom-GUC
  smoke should use `LOAD 'test_backend_runtime_threaded'` plus `SHOW`, so it
  validates module/GUC behavior without depending on catalog writes.
- Threaded catalog-writing DDL previously crashed in `XLogInsert()` during
  `CREATE TABLE` because the derived `wal_consistency_checking` bool array was
  NULL in the installed `PgSession`. Keep the threaded
  `CREATE TABLE`/`INSERT`/`DROP TABLE` smoke in
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` when
  changing required GUC bootstrap or WAL GUC state.
- The same threaded runtime TAP fixture now covers database, role, and startup
  GUC adoption: `ALTER DATABASE postgres SET work_mem`, `ALTER ROLE ... SET
  statement_timeout`, `ALTER ROLE ... SET default_statistics_target`, and a
  startup-packet `options='-c lock_timeout=8s'` connection. Keep that matrix
  current when changing threaded GUC replay/adoption paths.
- Threaded backend startup must replay postmaster nondefault GUC state after
  `InitializeThreadedSessionGUCOptions()` and before
  `InstallPgThreadBackendRuntimeState()`. That ordering lets
  `read_nondefault_variables()` write configured built-in defaults into early
  fallback session/runtime GUC buckets, which runtime installation then adopts
  into the thread's `PgSession`/runtime objects. Moving runtime installation
  earlier can crash because some adoption paths allocate GUC strings before
  `GUCMemoryContext` exists.
  The replay depends on the postmaster write side too: non-`EXEC_BACKEND`
  postmasters must call `write_nondefault_variables()` when `multithreaded` is
  enabled, both after initial config load and after SIGHUP reloads. Without
  `global/config_exec_params`, threaded clients silently fall back to boot
  defaults such as `work_mem = 4MB`.
- Avoid broad mechanical churn unless it unlocks a specific migration step.
- Do not remove process isolation paths merely because threaded mode exists.

## Local Build And Test Notes

- This checkout is commonly built with GNU make on macOS. Use `gmake`, not the
  BSD `make`. In the Codex desktop shell, Homebrew's bin directory may be
  absent from `PATH`; if `gmake` is not found, use `/opt/homebrew/bin/gmake` or
  export `PATH="/opt/homebrew/bin:$PATH"` before building.
- Do not use parallel `gmake -B ...` as a shortcut to force touched-object
  compiles in this checkout. It can trigger concurrent `config.status
  --recheck` runs, which race through temporary `conftest` files and produce
  misleading configure failures. To force coverage for edited sources, remove
  the specific `.o` files and rebuild the ordinary targets serially, or use the
  documented clean rebuild path below when installed headers or runtime object
  layouts changed.
- After cleaning under `src/backend`, generated backend-side files can be
  missing while include-side `header-stamp` files and symlinks still exist. If
  the build fails with a missing header such as `utils/errcodes.h`, regenerate
  the backend-side utility outputs explicitly:

  ```sh
  gmake -C src/backend/utils fmgr-stamp errcodes.h probes.h guc_tables.inc.c pgstat_wait_event.c wait_event_funcs_data.c wait_event_types.h
  ```

  If include-side symlinks are also missing or stale, remove the stamp and
  rebuild the symlinks:

  ```sh
  rm -f src/include/utils/header-stamp
  gmake -C src/backend/utils generated-header-symlinks
  ```

  If the missing generated header is `nodes/nodetags.h`, use the equivalent
  node-header recovery:

  ```sh
  rm -f src/backend/nodes/node-support-stamp
  gmake -C src/backend/nodes node-support-stamp
  rm -f src/include/nodes/header-stamp
  gmake -C src/backend/nodes generated-header-symlinks
  ```

- After changing exported backend globals or their `PG_THREAD_LOCAL`
  declarations in installed headers, clean and rebuild any in-tree extension
  under test before trusting its regression result. At minimum, do this for
  PL/pgSQL when touching GUC backing variables used by PL/pgSQL:

  ```sh
  gmake -C src/pl/plpgsql/src clean
  gmake -C src/pl/plpgsql/src all
  gmake -C src/pl/plpgsql/src DESTDIR="$PWD/tmp_install" install
  ```

  A stale PL/pgSQL build after SPI exported-state changes can fail during
  `initdb` post-bootstrap initialization with `Symbol not found:
  _SPI_processed` while loading `plpgsql.dylib`. Treat that as a stale module
  build, not as a SQL regression: clean, rebuild, and reinstall PL/pgSQL into
  the current `tmp_install`.

  `pg_global_prng_state` is also exported through an installed common header
  and is referenced by some contrib/test modules, including `amcheck`,
  `auto_explain`, `tablefunc`, and several `src/test/modules` tests. Clean and
  reinstall any of those modules before testing them after PRNG TLS changes.

  After changing `src/include/utils/backend_runtime.h`, clean and relink
  `src/test/modules/test_backend_runtime` before running its regression check.
  The extension allocates runtime structs by value, so a stale
  `test_backend_runtime.dylib` can crash during early tests even when the
  backend itself rebuilt successfully:

  ```sh
  gmake -C src/test/modules/test_backend_runtime clean
  gmake -C src/test/modules/test_backend_runtime all
  gmake -C src/test/modules/test_backend_runtime check
  ```

  Pending interrupt globals such as `InterruptPending` can be referenced from
  server-side common objects and loadable modules. After converting one of
  these exported names to an object-backed compatibility macro, rebuild
  `src/common`, PL/pgSQL, `src/test/regress`, and `libpqwalreceiver` before
  trusting `initdb` or core regression results:

  ```sh
  gmake -C src/common clean all
  gmake -C src/pl/plpgsql/src clean all DESTDIR="$PWD/tmp_install" install
  gmake -C src/test/regress clean all
  gmake -C src/backend/replication/libpqwalreceiver clean all DESTDIR="$PWD/tmp_install" install
  ```

  Memory-context globals exported through `palloc.h` or `memutils.h` are also
  referenced by backend loadable modules needed during `initdb`
  post-bootstrap initialization. After converting one of these names to an
  object-backed compatibility macro, rebuild and reinstall `src/backend/snowball`
  before trusting temp-instance tests. A stale `dict_snowball.dylib` fails
  `initdb` with `Symbol not found: _CurrentMemoryContext`.

  ```sh
  gmake -C src/backend/snowball clean all DESTDIR="$PWD/tmp_install" install
  ```

  Direct-pointer GUC globals exported through `miscadmin.h` can be referenced
  by backend loadable modules too. After converting one of these names to an
  object-backed compatibility macro, force-clean and reinstall
  `libpqwalreceiver` before trusting subscription tests or the core
  `parallel_schedule`; a stale `libpqwalreceiver.dylib` failed to load with
  `Symbol not found: _work_mem` after the query-memory GUC migration.

  ```sh
  gmake -C src/backend/replication/libpqwalreceiver clean all
  gmake -C src/backend/replication/libpqwalreceiver DESTDIR="$PWD/tmp_install" install
  ```

  Logical-decoding output plugins can also keep stale references to moved
  backend globals. After moving memory-context or replication GUC globals,
  clean and reinstall `pgoutput` and `pgrepack` before trusting
  `contrib/test_decoding`; stale copies have failed with
  `Symbol not found: _CurrentMemoryContext`.

  ```sh
  gmake -C src/backend/replication/pgoutput clean all DESTDIR="$PWD/tmp_install" install
  gmake -C src/backend/replication/pgrepack clean all DESTDIR="$PWD/tmp_install" install
  ```

  Core backend globals such as `MyProcPid` can also be referenced from
  server-side port objects. If a clean backend link fails with a removed
  backend-global symbol from `libpgport_srv.a`, clean and rebuild `src/port`
  as well:

  ```sh
  gmake -C src/port clean all
  ```

  Server-side common objects can have the same stale-symbol problem. If a
  clean backend link fails with a removed backend-global symbol from
  `libpgcommon_srv.a`, clean and rebuild `src/common` as well:

  ```sh
  gmake -C src/common clean all
  ```

- After changing a contrib/test module header to expose `PG_THREAD_LOCAL`
  declarations, clean and rebuild every object in that module before running a
  threaded smoke. Stale objects can still see the old plain-global symbol while
  freshly compiled objects use TLS, producing crashes that look like missing
  initialization. This was observed in `pg_stash_advice` after changing its
  DSM attachment pointers in `pg_stash_advice.h`.

- If an installed header changes a global from plain storage to
  `PG_THREAD_LOCAL`, do not trust a purely incremental backend build. Stale
  backend objects can still compile and link but then crash during `initdb`
  post-bootstrap single-user startup. Use the backend clean plus generated-file
  recovery above, then rebuild with `gmake -j8`.

- If a shared-memory struct layout changes, especially `Latch`, `PGPROC`, or
  fields embedded immediately beside semaphores/latches, do not trust a purely
  incremental backend build. Stale objects can corrupt adjacent shared-memory
  fields; one observed failure after changing `Latch` was a bootstrap segfault
  in `PGSemaphoreReset()` because stale `proc.o` still used the old
  `PGPROC.procLatch` size. Use the backend clean plus generated-file recovery
  above, then rebuild with `gmake -j8`.

- If `src/include/utils/backend_runtime.h` changes the layout of embedded
  runtime structs such as `PgThreadBackendRuntimeState`, do not trust a purely
  incremental backend build. Stale objects can keep old field offsets while
  freshly compiled runtime code zeros or writes the new, larger struct. One
  observed failure after adding connection socket I/O state was threaded
  startup corrupting adjacent `BackendThreadStart` timezone fields and
  segfaulting in `StartupXLOG()` before readiness. Use the backend clean plus
  generated-file recovery above, then rebuild with `gmake -j8`.

  Another observed failure after expanding `PgThreadBackendRuntimeState` was
  `001_threaded_runtime.pl` failing to start a thread-model background worker
  with `could not access file "": No such file or directory`; stale
  `src/backend/postmaster/launch_backend.o` had allocated the old embedded
  runtime-state size, so initialization overwrote the background-worker
  startup data. At minimum remove and rebuild
  `src/backend/postmaster/launch_backend.o`, then rerun `gmake -j8`. Header
  changes that remove execution globals may also need stale users removed and
  rebuilt, including `src/backend/access/transam/xact.o`,
  `src/backend/access/transam/twophase.o`,
  `src/backend/access/transam/xloginsert.o`,
  `src/backend/replication/logical/applyparallelworker.o`, and
  `src/backend/replication/logical/tablesync.o`.

  Another observed failure after adding a field to `PgCarrier` was
  `001_threaded_runtime.pl` failing at `pg_ctl start`, with lldb showing
  `PgBackendGetSignalPid()` dereferencing address `0x1`. Stale
  `src/backend/postmaster/launch_backend.o` had passed a `PgBackend *` offset
  from the old embedded `PgThreadBackendRuntimeState` layout into
  `PostmasterChildSetThreadBackend()`. Force-rebuild that object before
  reinstalling and rerunning threaded TAP.

  The same rule applies when adding fields to `PgBackend` itself. A stale
  incremental build after adding AIO runtime state linked, but `initdb`
  post-bootstrap startup spun during shutdown cleanup in
  `dsm_backend_shutdown()`/`dsm_detach()` because stale backend objects still
  used the old `PgBackend` layout. Treat that as stale-object fallout: kill the
  stuck temp bootstrap, run the backend clean plus generated-file recovery,
  rebuild with `gmake -j8`, and reinstall before rerunning temp-instance tests.

  The same rule applies when adding fields to embedded `PgSession` state. One
  observed stale-object symptom after expanding `PgSessionCatalogLookupState`
  was threaded `CREATE EXTENSION test_backend_runtime_threaded` crashing in
  `list_member_ptr()` from `dfmgr.c` while recording `_PG_init()` session
  state. Rebuild at least `src/backend/utils/fmgr/dfmgr.o` and reinstall the
  temp tree; prefer a full backend clean plus generated-file recovery when the
  layout change is broad.

- When moving WAL/XLog state out of `src/backend/access/transam/xlog.c`, avoid
  object-like compatibility macros that reuse names of shared WAL struct
  fields. In particular, do not define a `RedoRecPtr` macro: it also expands
  inside `Insert->RedoRecPtr` and `XLogCtl->RedoRecPtr`. Use the existing
  `XLogLocalRedoRecPtr` compatibility name for the backend-local redo-pointer
  cache and leave shared struct member references untouched.

- If `src/include/replication/worker_internal.h` changes the layout of
  `LogicalRepWorker`, clean and rebuild the whole logical replication backend
  directory before running logical replication smokes. Incremental builds in
  this checkout have left objects such as `syncutils.o`, `tablesync.o`, and
  `applyparallelworker.o` built against the previous struct layout:

  ```sh
  gmake -C src/backend/replication/logical clean
  gmake -j8
  gmake -j8 install DESTDIR="$PWD/tmp_install"
  ```

  A stale `syncutils.o` can read `LogicalRepWorker.userid` from the old offset
  and make table-sync workers fail during startup with errors like
  `role with OID 119 does not exist`, where `119` is the ASCII value of a
  subscription relation-state byte.

- If `PMChild` layout changes in `src/include/postmaster/postmaster.h`, do not
  trust an incremental build of postmaster objects. Stale postmaster objects can
  corrupt the PMChild freelists or crash auxiliary children during temp-instance
  startup. Use:

  ```sh
  gmake -C src/backend/postmaster clean
  gmake -C src/backend -j8
  ```

- If a shared enum in an installed or widely included header changes numeric
  values, do not trust a purely incremental backend build. For example,
  inserting a new `PMSignalReason` before existing values can leave stale
  objects such as `checkpointer.o` signaling one numeric reason while
  `postmaster.o` interprets another, causing shutdown hangs. Prefer appending
  new signal reasons to preserve existing values, and force rebuild affected
  objects or use a clean backend rebuild before testing.

- This checkout is currently configured with `with_gssapi = no`. A direct
  `gmake -C src/backend/libpq be-secure-gssapi.o` can fail before reaching
  project changes because the GSSAPI types and functions are unavailable in
  this configuration. For GSSAPI-only source annotations, use static lifetime
  scan coverage plus a full non-GSS build here, and use a GSSAPI-enabled build
  when compile coverage for that file is required.

- This checkout is currently configured with `with_ssl = no`. A direct
  `gmake -C src/backend/libpq be-secure-openssl.o` can fail before reaching
  project changes because OpenSSL support macros are not enabled in
  `pg_config.h`. For OpenSSL-only source annotations, use static lifetime scan
  coverage plus a full non-SSL build here, and use an SSL-enabled build when
  compile coverage for that file is required.

- This checkout is currently validated with LLVM enabled for Phase 12 JIT
  provider work:

  ```sh
  ./configure --without-icu --disable-rpath --with-llvm LLVM_CONFIG=/opt/homebrew/opt/llvm@21/bin/llvm-config
  ```

  After switching LLVM configuration or changing installed runtime/JIT
  headers, do not trust stale incremental objects. Use the backend clean plus
  generated-file recovery above before a full `gmake -j8`; otherwise
  `llvmjit.dylib` can fail to link or runtime JIT smoke can crash against an
  older `postgres` binary missing new runtime accessors such as
  `PgCurrentLLVMJitState`.

  LLVM 21 headers collide with PostgreSQL's short historical macros. Keep the
  macro cleanup in `src/include/jit/llvmjit.h` for LLVM C headers and in
  `src/backend/jit/llvm/llvmjit_inline.cpp` for later LLVM C++ headers. If
  a clean LLVM provider build fails inside LLVM headers with names such as
  `Mode`, `PM`, `AM`, or `TZ`, fix the boundary cleanup rather than patching
  generated LLVM headers.

- This checkout is currently configured without `--enable-injection-points`.
  `src/test/modules/injection_points` intentionally skips checks in that
  configuration, and injection-point TAP/regression coverage requires a build
  configured with injection points enabled. For injection-point-only source
  annotations in this checkout, use object compile coverage where reachable,
  static lifetime scan coverage, and a full non-injection build/install.

- This checkout does not define `LWLOCK_STATS` in normal builds. Changes inside
  the optional LWLock stats debug block in `src/backend/storage/lmgr/lwlock.c`
  are therefore covered here by normal surrounding-object builds, runtime
  accessor tests, and `gmake check-global-lifetimes`; direct compile coverage
  for that debug block requires an `LWLOCK_STATS`-enabled build.

- For manual temp-cluster smokes, especially threaded-mode smokes launched from
  this deep checkout path, use a short Unix socket directory under `/tmp` with
  `pg_ctl -o "-k /tmp/..."` or an explicit `unix_socket_directories` setting.
  Nested workspace paths can exceed the platform Unix socket path length before
  SQL starts.

- Some `gmake ... check` runs fail on macOS because temporary-install binaries
  still refer to `/usr/local/pgsql/lib/libpq.5.dylib`. Patch the temp install
  before running direct `pg_regress` commands:

  ```sh
  install_name_tool -change /usr/local/pgsql/lib/libpq.5.dylib "$PWD/tmp_install/usr/local/pgsql/lib/libpq.5.dylib" "$PWD/tmp_install/usr/local/pgsql/bin/initdb" || true
  install_name_tool -change /usr/local/pgsql/lib/libpq.5.dylib "$PWD/tmp_install/usr/local/pgsql/lib/libpq.5.dylib" "$PWD/tmp_install/usr/local/pgsql/bin/psql"
  install_name_tool -change /usr/local/pgsql/lib/libpq.5.dylib "$PWD/tmp_install/usr/local/pgsql/lib/libpq.5.dylib" "$PWD/tmp_install/usr/local/pgsql/bin/pg_ctl" || true
  install_name_tool -change /usr/local/pgsql/lib/libpq.5.dylib "$PWD/tmp_install/usr/local/pgsql/lib/libpq.5.dylib" "$PWD/tmp_install/usr/local/pgsql/bin/pg_basebackup" || true
  install_name_tool -change /usr/local/pgsql/lib/libpq.5.dylib "$PWD/tmp_install/usr/local/pgsql/lib/libpq.5.dylib" "$PWD/tmp_install/usr/local/pgsql/lib/libpqwalreceiver.dylib" || true
  ```

  Tests that create subscriptions can reach `libpqwalreceiver.dylib`; patch it
  along with the frontend binaries after reinstalling or recreating
  `tmp_install`.

  After rebuilding backend/postmaster code, reinstall before direct TAP runs
  that use `tmp_install`; otherwise an old `postgres` binary can run with new
  test modules or headers. One observed stale-install symptom was
  `001_threaded_runtime.pl` failing at
  `test_backend_runtime_launch_thread_bgworker()` with
  `thread-model background worker did not start: status 2` and server log
  `could not access file ""`. Reinstall with
  `gmake -j8 install DESTDIR="$PWD/tmp_install"` and reinstall
  `src/test/modules/test_backend_runtime`, then patch install names again
  before rerunning TAP.

  Direct isolation runs can fail the same way from build-tree binaries. Patch
  `src/test/isolation/isolationtester` and
  `src/test/isolation/pg_isolation_regress` to the same temp-install
  `libpq.5.dylib` before rerunning them. Direct TAP runs that pass
  `PG_REGRESS="$PWD/src/test/regress/pg_regress"` can fail during
  `pg_regress --config-auth` with signal 6 for the same reason after
  rebuilding `src/test/regress`; patch `src/test/regress/pg_regress` too:

  ```sh
  install_name_tool -change /usr/local/pgsql/lib/libpq.5.dylib "$PWD/tmp_install/usr/local/pgsql/lib/libpq.5.dylib" src/test/regress/pg_regress
  ```

  `gmake -C src/test/regress check-tests` recreates `tmp_install`, so a
  previously patched `psql` can become unpatched again. If that target fails
  before SQL starts with a `dyld` `libpq.5.dylib` loader error, patch the new
  temp-install binaries and rerun the equivalent `pg_regress` command directly.

  Do not run two `gmake ... check` targets that create a temp install in
  parallel from the same checkout. They share `$PWD/tmp_install`, and parallel
  temp-install setup can fail before SQL starts with `rm: ... tmp_install:
  Directory not empty` or a missing `tmp_install/log/install.log`. Run those
  regression targets sequentially, or use a separate checkout/build directory
  for parallel test work.

  Top-level `gmake check-world` and recursive targets such as
  `gmake -C src/test check` also recreate `tmp_install` on this checkout. They
  can therefore fail before SQL starts even if a previous temp install was
  patched. If the failure is a `dyld` lookup for
  `/usr/local/pgsql/lib/libpq.5.dylib`, patch the recreated temp install and run
  the reached test driver directly. For example, after a `check-world` failure
  in `src/test/isolation`, patch `psql`, `pg_ctl`, `pg_isolation_regress`, and
  `isolationtester`, then rerun:

  ```sh
  cd src/test/isolation
  PATH="$PWD/../../../tmp_install/usr/local/pgsql/bin:$PWD:$PATH" \
  DYLD_LIBRARY_PATH="$PWD/../../../tmp_install/usr/local/pgsql/lib" \
  INITDB_TEMPLATE="$PWD/../../../tmp_install/initdb-template" \
  ./pg_isolation_regress --temp-instance=./tmp_check_iso --inputdir=. --outputdir=output_iso --bindir= --schedule=./isolation_schedule
  ```

  The core regression equivalent after patching is:

  ```sh
  cd src/test/regress
  PATH="$PWD/../../../tmp_install/usr/local/pgsql/bin:$PWD:$PATH" \
  DYLD_LIBRARY_PATH="$PWD/../../../tmp_install/usr/local/pgsql/lib" \
  INITDB_TEMPLATE="$PWD/../../../tmp_install/initdb-template" \
  ./pg_regress --temp-instance=./tmp_check --inputdir=. --bindir= --dlpath=. --schedule=./parallel_schedule
  ```

  If the same recursive target needs to be rerun, patch the build-tree binaries
  that are copied into each recreated temp install before rerunning. This has
  allowed recursive checks such as
  `gmake -C src/test/modules/test_extensions check` to run normally after an
  initial temp-install `initdb` loader failure:

  ```sh
  install_name_tool -change /usr/local/pgsql/lib/libpq.5.dylib "$PWD/tmp_install/usr/local/pgsql/lib/libpq.5.dylib" src/bin/initdb/initdb || true
  install_name_tool -change /usr/local/pgsql/lib/libpq.5.dylib "$PWD/tmp_install/usr/local/pgsql/lib/libpq.5.dylib" src/bin/psql/psql || true
  install_name_tool -change /usr/local/pgsql/lib/libpq.5.dylib "$PWD/tmp_install/usr/local/pgsql/lib/libpq.5.dylib" src/bin/pg_ctl/pg_ctl || true
  install_name_tool -change /usr/local/pgsql/lib/libpq.5.dylib "$PWD/tmp_install/usr/local/pgsql/lib/libpq.5.dylib" src/bin/pg_basebackup/pg_basebackup || true
  install_name_tool -change /usr/local/pgsql/lib/libpq.5.dylib "$PWD/tmp_install/usr/local/pgsql/lib/libpq.5.dylib" src/backend/replication/libpqwalreceiver/libpqwalreceiver.dylib || true
  ```

  `gmake check-world` also builds ECPG test executables that can record
  `/usr/local/pgsql/lib/libecpg.6.dylib`, `libpgtypes.3.dylib`, and
  `libecpg_compat.3.dylib` from build-tree library IDs. If all ECPG tests abort
  with signal 6 and stderr says `Library not loaded: /usr/local/pgsql/lib/...`,
  patch the build-tree dynamic-library IDs before rerunning:

  ```sh
  install_name_tool -id "$PWD/tmp_install/usr/local/pgsql/lib/libpq.5.dylib" src/interfaces/libpq/libpq.5.dylib
  install_name_tool -id "$PWD/tmp_install/usr/local/pgsql/lib/libecpg.6.dylib" src/interfaces/ecpg/ecpglib/libecpg.6.dylib
  install_name_tool -id "$PWD/tmp_install/usr/local/pgsql/lib/libpgtypes.3.dylib" src/interfaces/ecpg/pgtypeslib/libpgtypes.3.dylib
  install_name_tool -id "$PWD/tmp_install/usr/local/pgsql/lib/libecpg_compat.3.dylib" src/interfaces/ecpg/compatlib/libecpg_compat.3.dylib
  ```

  Also patch inter-library references in `src/interfaces/ecpg/ecpglib` and
  `src/interfaces/ecpg/compatlib`, and patch any already-built ECPG test
  executables if rerunning `src/interfaces/ecpg/test` without rebuilding them.

- Do not run temp-instance smokes that use `tmp_install` in parallel with
  recursive check targets that recreate `tmp_install`. In particular,
  `gmake -C src/test/modules/test_backend_runtime check` deletes and
  reinstalls `tmp_install`; any concurrent smoke using
  `$PWD/tmp_install/usr/local/pgsql/bin` can fail for environmental reasons
  before it reaches the code being tested.

- For focused process-mode regression checks, run the test driver directly with
  the temp install first on `PATH`, for example:

  ```sh
  cd src/test/regress
  PATH="$PWD/../../../tmp_install/usr/local/pgsql/bin:$PWD:$PATH" \
  DYLD_LIBRARY_PATH="$PWD/../../../tmp_install/usr/local/pgsql/lib" \
  INITDB_TEMPLATE="$PWD/../../../tmp_install/initdb-template" \
  ./pg_regress --temp-instance=./tmp_check --inputdir=. --bindir= --dlpath=. --dbname=regression guc
  ```

  If `$PWD/../../../tmp_install/initdb-template` or the equivalent relative
  path for the current test directory does not exist, omit `INITDB_TEMPLATE`
  and let `pg_regress` run a fresh `initdb`. A missing template fails before
  SQL starts with a `cp ... initdb-template: No such file or directory` error.

  On macOS, Unix-domain socket paths are limited. Live smokes from this long
  checkout path can fail before SQL starts with `Unix-domain socket path ... is
  too long (maximum 103 bytes)`. Use a short `mktemp -d /tmp/...` directory for
  ad hoc temp clusters that need Unix sockets.

  Threaded temp clusters currently require the database locale to match the
  postmaster process locale. In this checkout the shell commonly reports
  `LC_CTYPE=C.UTF-8`, so direct threaded smokes should initialize clusters with
  `initdb --locale=C.UTF-8` rather than `--no-locale`; otherwise threaded
  client backends fail before SQL starts with
  `database locale is incompatible with threaded backend mode`.

  If killed threaded temp clusters leave SysV shared-memory IDs behind,
  follow-up `initdb` runs can fail during bootstrap with
  `could not create shared memory segment: No space left on device` even when
  disk space is fine. First confirm no PostgreSQL server processes from this
  checkout are still running, then inspect and remove stale segments with
  `ipcs -m` and `ipcrm -m <id>`.

  Many individual regression tests assume fixture objects created by earlier
  `parallel_schedule` groups. If a direct focused run fails with missing tables
  such as `onek` or `tenk1`, rerun with the schedule prefix that builds the
  fixture state, for example:

  ```sh
  ./pg_regress --temp-instance=./tmp_check --inputdir=. --bindir= --dlpath=. --dbname=regression \
    test_setup copy copyselect copydml copyencoding insert insert_conflict \
    create_function_c create_misc create_operator create_procedure create_table create_type create_schema \
    create_index create_index_spgist create_view index_including index_including_gist \
    create_aggregate create_function_sql create_cast constraints triggers select vacuum sanity_check guc
  ```

  The `horology` test has its own date/time fixture dependencies. Run
  `date time timetz timestamp timestamptz interval` before `horology` in direct
  focused runs, matching `parallel_schedule`.

  The `select_parallel` test can produce plan-shape diffs if the direct run
  only includes `create_misc`; include the schedule prefix through
  `create_index`, `vacuum`, `guc`, and `sysviews` before `select_parallel`.

  The `privileges` test has an opening large-object cleanup query whose
  expected output assumes no matching leftover objects. In direct focused runs
  that include both files, run `privileges` before `largeobject`, or run them
  in separate temp instances.

  The `stats` test expects helper objects from `stats_ext`; include
  `stats_ext` before `stats` in direct focused runs.

  The `float8` test expects the permanent `FLOAT8_TBL` fixture from
  `test_setup` after it drops its temporary table. Direct focused runs should
  use at least:

  ```sh
  ./pg_regress --temp-instance=./tmp_check --inputdir=. --bindir= --dlpath=. --dbname=regression \
    test_setup float8
  ```

- `guc_privs` is not a core `src/test/regress` test. It lives under
  `src/test/modules/unsafe_tests`.
- `analyze` is not a core `src/test/regress` test file in this checkout. For
  focused sampling/ANALYZE validation, use a live temp-cluster smoke that
  creates a table, inserts enough rows, runs `ANALYZE`, and verifies visible
  `pg_stats` rows.
- `create_role` is not reliable as a standalone direct `pg_regress` test in
  this checkout. It appears late in `parallel_schedule` and assumes earlier
  fixture/public-schema state; for focused superuser/role-cache validation,
  prefer `roleattributes` plus a live temp-cluster role privilege smoke unless
  you are intentionally running the larger schedule prefix.
- The extension backend-model tests need the test extension module installed
  into the current temp install before direct `pg_regress` runs:

  ```sh
  gmake -C src/test/modules/test_extensions DESTDIR="$PWD/tmp_install" install
  ```

- The threaded backend-runtime TAP fixture uses
  `CREATE EXTENSION test_backend_runtime_threaded`. After changing
  `src/test/modules/test_backend_runtime/test_backend_runtime_threaded.c`, its
  extension control/SQL files, or the module Makefile/meson metadata, reinstall
  that module before manual threaded smokes:

  ```sh
  gmake -C src/test/modules/test_backend_runtime DESTDIR="$PWD/tmp_install" install
  ```

- The direct threaded backend-runtime TAP also installs representative contrib
  modules into `tmp_install` before starting its threaded node: `hstore`,
  `pg_trgm`, `btree_gist`, and `pageinspect`. Keep this as a Gate E2
  representative extension smoke. These modules opt in with
  thread-per-session backend-model metadata; `pg_trgm` also moves its custom
  GUC backing variables into `PgSession.extension_modules` before opting in.
  Future contrib opt-ins need the same mutable-state audit. Phase 16 still
  owns contrib-wide threaded regression.

- The backend-runtime state/PMChild regression is expected to be runnable as a
  focused process-mode control after the same module install. The fake
  thread-runtime helper tests should construct `PgThreadBackendRuntimeState`
  objects without installing them into the active SQL backend:

  ```sh
  cd src/test/modules/test_backend_runtime
  PATH="$PWD/../../../../tmp_install/usr/local/pgsql/bin:$PATH" \
  DYLD_LIBRARY_PATH="$PWD/../../../../tmp_install/usr/local/pgsql/lib" \
  ../../../../src/test/regress/pg_regress --temp-instance=./tmp_check \
    --inputdir=. --outputdir=output \
    --bindir="$PWD/../../../../tmp_install/usr/local/pgsql/bin" \
    --dlpath=. test_backend_runtime
  ```

- GUC custom-prefix smoke tests that preload `test_oat_hooks` need that module
  installed into the current temp install first:

  ```sh
  gmake -C src/test/modules/test_oat_hooks DESTDIR="$PWD/tmp_install" install
  ```

- Threaded runtime GUC stack coverage in
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` verifies
  built-in database/role/startup defaults, `SET LOCAL` rollback/commit
  behavior, `RESET` back to database and startup-packet sources, and custom
  extension GUC `SET LOCAL`/`RESET` semantics through the superuser `LOAD`
  path. Unprivileged `LOAD 'test_backend_runtime_threaded'` is expected to
  fail with the normal library-access policy error unless explicit load
  privileges are granted.

  Manual concurrent threaded GUC smokes should capture background client PIDs
  with `$!` and wait for that explicit list. Do not rely on `jobs -p` in the
  non-interactive zsh shell; it can produce an empty list, causing the harness
  to stop the temp postmaster before the background clients finish.

- Abandoned-client teardown smokes should leave the backend idle in
  transaction before killing the client, matching `background_psql` behavior.
  Do not use `SELECT pg_sleep(...)` as the wait point for that fixture: killing
  the frontend while the backend is inside `pg_sleep` can leave the advisory
  lock visible until the running query observes an interrupt or finishes,
  which tests a different path from idle-client abandonment.

- Direct logical replication parallel-apply smokes should use the upstream
  `src/test/subscription/t/015_stream.pl` interleaved transaction shape:
  start one large transaction, run and commit a second large transaction while
  the first remains open, then commit the first. A single large transaction
  followed by `pg_sleep()` can replicate successfully without proving the
  `STREAM_START`/parallel apply path. The parallel worker is pooled and can be
  hard to catch by polling `pg_stat_activity`; use the subscriber log marker
  for `logical replication parallel apply worker for subscription`, the final
  replicated row/default counts, and a postmaster child-process check as the
  primary smoke evidence.

- Manual threaded slot-sync smokes that use `pg_basebackup -R` should write
  the final `primary_conninfo` containing `dbname=postgres` into
  `postgresql.auto.conf`, not only `postgresql.conf`. The `-R` generated
  `primary_conninfo` in `postgresql.auto.conf` otherwise overrides the later
  config-file value and makes the slot sync worker restart with
  `replication slot synchronization requires "dbname" to be specified in
  "primary_conninfo"` before testing the intended threaded path.

- Threaded checkpointer/background-writer smokes should wait for the
  post-startup handoff. In threaded mode those workers intentionally start as
  processes before recovery forks the startup process, then exit and relaunch
  as thread carriers after `PM_RUN` and after another thread carrier exists.
  Good smoke evidence is one logical `checkpointer` and one logical
  `background writer` in `pg_stat_activity`, no OS child command containing
  `checkpointer` or `background writer` under the postmaster, a successful
  `CHECKPOINT`, and clean fast shutdown. In process-mode compatibility smokes,
  the same workers should still appear as OS child processes.

- Plain `multithreaded=on` temp clusters should complete
  `pg_ctl -m fast stop` cleanly. If a Phase 11 worker smoke hangs during fast
  stop, sample the postmaster before cleanup; a previous blocker was a
  thread-backed worker that consumed logical interrupts without routing
  shutdown requests through `ProcessMainLoopInterrupts()`.

- PostgreSQL TAP tests require the non-core Perl module `IPC::Run`. The system
  Perl on this checkout does not provide it. CPAN attempts to install through
  the normal system/local-lib path have stalled while installing `IO::Tty`, but
  the unpacked pure-Perl IPC::Run build is usable directly from:

  ```sh
  /Users/samwillis/.cpan/build/IPC-Run-20260402.0-5/blib/lib
  ```

  To retry a normal local install without relying on system Perl paths, use:

  ```sh
  PERL_MM_USE_DEFAULT=1 \
  PERL_MM_OPT="INSTALL_BASE=$HOME/perl5" \
  PERL_MB_OPT="--install_base $HOME/perl5" \
  cpan -T -i IPC::Run
  ```

  Until that succeeds, keep
  `PERL5LIB="/Users/samwillis/.cpan/build/IPC-Run-20260402.0-5/blib/lib:$PWD/src/test/perl"`
  in direct TAP commands. This checkout is still configured without
  `--enable-tap-tests`, so recursive `gmake ... check` targets report `TAP
  tests not enabled`. Do not treat that configure-time message as a reason to
  skip TAP coverage; run the direct `prove` command with the local `PERL5LIB`
  path. Direct `prove` runs also need the same harness environment that
  `gmake check` supplies, especially `PG_REGRESS`; if `PG_REGRESS` is missing,
  `PostgreSQL::Test::Cluster->init` can call `system_or_bail()` with an
  undefined command and `prove` may report an empty skip reason before the
  server starts. A minimal direct environment is:

  ```sh
  PERL5LIB="/Users/samwillis/.cpan/build/IPC-Run-20260402.0-5/blib/lib:$PWD/src/test/perl" \
  PATH="$PWD/tmp_install/usr/local/pgsql/bin:$PATH" \
  DYLD_LIBRARY_PATH="$PWD/tmp_install/usr/local/pgsql/lib" \
  INITDB_TEMPLATE="$PWD/tmp_install/initdb-template" \
  PG_REGRESS="$PWD/src/test/regress/pg_regress" \
  prove -I src/test/perl src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl
  ```

  Module TAP tests that normally run through `prove_check` may need the full
  makefile harness environment. For worker SPI, run from the module directory
  after refreshing `tmp_install` and patching the temp-install dynamic library
  names:

  ```sh
  cd src/test/modules/worker_spi
  rm -rf tmp_check && mkdir -p tmp_check/log
  PERL5LIB="/Users/samwillis/.cpan/build/IPC-Run-20260402.0-5/blib/lib:$OLDPWD/src/test/perl" \
  TESTLOGDIR="$PWD/tmp_check/log" \
  TESTDATADIR="$PWD/tmp_check" \
  PATH="$OLDPWD/tmp_install/usr/local/pgsql/bin:$PWD:$PATH" \
  DYLD_LIBRARY_PATH="$OLDPWD/tmp_install/usr/local/pgsql/lib" \
  INITDB_TEMPLATE="$OLDPWD/tmp_install/initdb-template" \
  PGPORT=65432 \
  top_builddir="$PWD/../../../.." \
  PG_REGRESS="$PWD/../../../../src/test/regress/pg_regress" \
  share_contrib_dir="$OLDPWD/tmp_install/usr/local/pgsql/share/extension" \
  enable_injection_points=no \
  prove -I "$OLDPWD/src/test/perl" -I . t/001_worker_spi.pl t/002_worker_terminate.pl
  ```

- In the managed Codex sandbox, PostgreSQL temp-instance tests can fail during
  `initdb` with `could not create shared memory segment: Operation not
  permitted` from `shmget()`. Treat that as a sandbox restriction, not a
  PostgreSQL regression. Rerun the same test outside the sandbox/with
  escalation, or force a POSIX DSM configuration when that is sufficient for
  the check.

- Repeated crash-debugging of threaded temp clusters on macOS can leave stale
  SysV shared-memory segments and semaphore sets even when no `postgres`
  process remains. If `initdb` fails with `shmget(...): No space left on
  device`, first confirm that no PostgreSQL server process is still running:

  ```sh
  ps -axo pid,ppid,stat,command | rg '[p]ostgres|[p]ostmaster|[i]nitdb' || true
  ```

  Prefer removing only detached shared-memory segments (`NATTCH` is zero in
  `ipcs -ma`). Do not remove attached segments from unrelated running
  PostgreSQL instances. Start with detached shared memory owned by the current
  user:

  ```sh
  for id in $(ipcs -ma | awk '$1 == "m" && $9 == 0 && $5 == "'$USER'" {print $2}'); do ipcrm -m "$id" || true; done
  ```

  Remove semaphore sets only after identifying them as stale test leftovers;
  they do not expose attachment counts in the same way as shared-memory
  segments.

- This shell is zsh. Cleanup commands with unmatched globs, such as
  `rm -rf tmp_check_*`, can fail with `no matches found` before the test command
  runs. Use a matched path, `find`, or enable null-glob behavior when cleaning
  optional TAP/regression scratch directories.

## Terminology

- Runtime: one server runtime inside an address space. In process mode, each
  backend process has its own private address space plus shared memory. In
  threaded mode, many backends share one address space.
- Carrier: the physical execution vehicle, such as an OS process, OS thread,
  or future host scheduler worker.
- Backend: a logical PostgreSQL backend identity visible to cancellation,
  statistics, lock ownership, and monitoring.
- Session: SQL session state for a client or pooled logical session.
- Execution: active transaction/query/portal work currently consuming backend
  resources.
- Connection: frontend/backend protocol transport. It is usually a socket in
  native PostgreSQL, but should not be architecturally identical to a session.
